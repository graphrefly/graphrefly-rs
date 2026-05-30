//! The node — the thinnest substrate primitive (D5 / R-node-thin).
//!
//! Holds a fn handle + deps + the wave state machine; **zero inspection cruft**
//! (naming/find/describe are the graph layer). Single-thread (D22) ⇒ the shared
//! state lives behind `Rc<RefCell<NodeInner>>` ([`Core`]), `!Send + !Sync`.
//!
//! Values cross the substrate **erased** as [`AnyValue`] (`Rc<dyn Any>`) — the
//! Rust analogue of TS's `unknown` (per-language impl, `CLEAN-SLATE.md`). A typed
//! [`Node<T>`] facade re-types the boundary (downcast at `cache`/`data`).
//!
//! ## Borrow discipline (the Rust-vs-TS divergence)
//!
//! TS has no runtime borrow check; here the wave engine must never hold a
//! `RefCell` borrow across a call that re-enters a node — `ctx.down` re-enters
//! the running node, and delivery re-enters downstream nodes. The rule throughout
//! is **clone the callable out, drop the borrow, then call** (subscribers,
//! dispatcher fns, dep handles). Short `borrow_mut` scopes mutate; calls happen
//! borrow-free.
//!
//! ## Slice scope (CSP-5 first cut)
//!
//! state / producer / derived nodes · two-phase DIRTY→DATA · diamond
//! pending-counter join (R-diamond/R-two-phase) · first-run gate
//! (R-first-run-gate) · every occurrence is DATA + substrate-synthesized undirty
//! RESOLVED for a no-emit fn (R-resolved-undirty / D49, supersedes D15 — no
//! equals-substitution) · push-on-subscribe (R-push-subscribe) · lazy activation ·
//! ROM/RAM (R-rom-ram). **Deferred:** INVALIDATE, COMPLETE/ERROR terminal,
//! TEARDOWN, PAUSE/RESUME, batch, LocalAsync, rewire, dynamicNode, replay-buffer.

use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::{Rc, Weak};

use crate::ctx::{Ctx, DepRecord};
use crate::dispatcher::{default_dispatcher, Dispatcher};
use crate::protocol::{AnyValue, Handle, Message};

/// Node lifecycle status (R-status-enum) — the source of truth for cache freshness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Status {
    /// No DATA yet — cache absent (SENTINEL = `None`, D16). Also the
    /// awaiting-first-run-gate state in this slice.
    Sentinel,
    /// Reserved by R-status-enum; not assigned in this slice (the awaiting-gate
    /// state is represented as `Sentinel`, matching the TS reference).
    Pending,
    /// Phase-1 DIRTY received — the prior cached value is stale.
    Dirty,
    /// Settled with a fresh value this wave.
    Settled,
    /// Undirty settle (R-resolved-undirty / D49): DIRTY'd but produced no new
    /// occurrence; a carried-over cached value stays fresh.
    Resolved,
    /// Terminal success — final (deferred slice).
    Completed,
    /// Terminal failure — final (deferred slice).
    Errored,
}

impl Status {
    /// Cached value is fresh (`settled` / `resolved`).
    pub fn is_fresh(self) -> bool {
        matches!(self, Status::Settled | Status::Resolved)
    }
    /// Terminal status (`completed` / `errored`) — D17 terminal-is-forever.
    pub fn is_terminal(self) -> bool {
        matches!(self, Status::Completed | Status::Errored)
    }
}

type Msg = Message<AnyValue>;
type Sink = Rc<dyn Fn(&Msg)>;
type Unsub = Box<dyn FnOnce()>;

/// The mutable state of a node. Pure fields + non-reentrant helpers only; the
/// reentrant engine lives on [`Core`].
struct NodeInner {
    deps: Vec<Core>,
    handle: Option<Handle>,
    dispatcher: Dispatcher,
    /// First-run gate off (fn fires as soon as dirty-count hits 0; fn body guards
    /// SENTINEL per dep). Default false (R-first-run-gate).
    partial: bool,

    // per-dep wave state (parallel arrays, indexed by dep position)
    dep_batch: Vec<Option<Vec<AnyValue>>>,
    dep_prev: Vec<Option<AnyValue>>,
    dep_has_data: Vec<bool>,
    dep_dirty: Vec<bool>,
    dep_tier: Vec<u8>,
    pending: i32,

    // own value + status
    cache: Option<AnyValue>,
    has_data: bool,
    status: Status,
    has_called_fn_once: bool,
    emitted_dirty_this_wave: bool,
    /// Did the fn emit a tier-3 (DATA/RESOLVED) this run? Drives the D49
    /// undirty-RESOLVED synthesis (R-resolved-undirty).
    emitted_tier3_this_wave: bool,
    inside_run_wave: bool,

    // subscribers + activation
    subscribers: Vec<(u64, Sink)>,
    next_sub_id: u64,
    activated: bool,
    dep_unsubs: Vec<Option<Unsub>>,

    // ctx.state + cleanup hooks
    state: Option<AnyValue>,
    state_persist: bool,
    on_deactivation: Vec<Box<dyn FnOnce()>>,
}

impl NodeInner {
    /// First-run gate predicate: every declared dep has delivered ≥1 DATA.
    fn all_deps_settled(&self) -> bool {
        self.dep_has_data.iter().all(|&h| h)
    }
}

/// Release a node's upstream subscriptions + external resources when its inner is
/// finally dropped (D1).
///
/// Rust has no GC, so a `Node<T>` dropped without an explicit unsubscribe (and
/// with no subscriber-driven deactivation) would otherwise leave its sink in each
/// dep's `subscribers` list forever — the dep would never deactivate and would do
/// wasted work emitting to a dead `Weak`. Running the remaining `dep_unsubs` here
/// removes those entries, and firing `on_deactivation` releases external resources
/// (mirroring what GC + finalizers do in TS). Safe: at drop the refcount is 0 (no
/// live borrow on `self`); each unsub touches a *dep* node (a different `RefCell`),
/// recursively deactivating up the DAG (terminates); idempotent with `deactivate`
/// (which already drained both vecs, so this is then a no-op).
impl Drop for NodeInner {
    fn drop(&mut self) {
        for u in self.dep_unsubs.drain(..).flatten() {
            u();
        }
        for h in std::mem::take(&mut self.on_deactivation) {
            h();
        }
    }
}

/// A cloneable handle over a node's shared inner state (`Rc<RefCell<NodeInner>>`).
/// The reentrant wave-engine methods live here; each manages its own short borrow
/// scopes so calls into other nodes (or back into this one via `ctx.down`) are
/// never made while a borrow is held.
#[derive(Clone)]
pub struct Core(Rc<RefCell<NodeInner>>);

/// Reset `inside_run_wave` on scope exit — including unwind, so the feedback-cycle
/// panic (D37) leaves the flag clean (mirrors TS's try/finally around
/// `dispatcher.invoke`). NOTE: full graph-wide panic→ERROR recovery (a panicking
/// user fn also unwinds through the source `down()` / `receive_from_dep`, leaving
/// THEIR wave flags stale) is deliberately NOT attempted here — it belongs with
/// the catch boundary introduced by the terminal/D30 slice (backlog B-rs-panic).
struct WaveGuard(Core);
impl Drop for WaveGuard {
    fn drop(&mut self) {
        self.0 .0.borrow_mut().inside_run_wave = false;
    }
}

impl Core {
    fn new(
        deps: Vec<Core>,
        handle: Option<Handle>,
        dispatcher: Dispatcher,
        initial: Option<AnyValue>,
    ) -> Core {
        let n = deps.len();
        let mut inner = NodeInner {
            deps,
            handle,
            dispatcher,
            partial: false,
            dep_batch: vec![None; n],
            dep_prev: vec![None; n],
            dep_has_data: vec![false; n],
            dep_dirty: vec![false; n],
            dep_tier: vec![0; n],
            pending: 0,
            cache: None,
            has_data: false,
            status: Status::Sentinel,
            has_called_fn_once: false,
            emitted_dirty_this_wave: false,
            emitted_tier3_this_wave: false,
            inside_run_wave: false,
            subscribers: Vec::new(),
            next_sub_id: 0,
            activated: false,
            dep_unsubs: Vec::new(),
            state: None,
            state_persist: false,
            on_deactivation: Vec::new(),
        };
        // R-initial: a provided initial pre-populates the cache.
        if let Some(v) = initial {
            inner.cache = Some(v);
            inner.has_data = true;
            inner.status = Status::Settled;
        }
        Core(Rc::new(RefCell::new(inner)))
    }

    // ── subscription (R-push-subscribe) ──

    /// A new sink receives START, then cached DATA (or DIRTY if dirty); the node
    /// lazily activates on its first subscriber.
    pub(crate) fn subscribe(&self, sink: Sink) -> Unsub {
        let (id, push) = {
            let mut n = self.0.borrow_mut();
            let id = n.next_sub_id;
            n.next_sub_id += 1;
            n.subscribers.push((id, sink.clone()));
            let push = if n.has_data {
                Some(Message::Data(
                    n.cache.clone().expect("has_data ⇒ cache present"),
                ))
            } else if n.status == Status::Dirty {
                Some(Message::Dirty)
            } else {
                None
            };
            (id, push)
        };
        sink(&Message::Start);
        if let Some(m) = push {
            sink(&m);
        }
        if !self.0.borrow().activated {
            self.activate();
        }
        let core = self.clone();
        Box::new(move || core.unsubscribe(id))
    }

    fn unsubscribe(&self, id: u64) {
        let became_empty = {
            let mut n = self.0.borrow_mut();
            let before = n.subscribers.len();
            n.subscribers.retain(|(sid, _)| *sid != id);
            n.subscribers.len() < before && n.subscribers.is_empty()
        };
        if became_empty {
            self.deactivate();
        }
    }

    // ── activation / deactivation (lazy; R-rom-ram) ──

    fn activate(&self) {
        let deps = {
            let mut n = self.0.borrow_mut();
            n.activated = true;
            n.dep_unsubs = (0..n.deps.len()).map(|_| None).collect();
            n.deps.clone()
        };
        for (i, dep) in deps.iter().enumerate() {
            self.subscribe_dep(i, dep);
        }
        // Depless producer (fn, no deps): run once on activation.
        let run = {
            let n = self.0.borrow();
            n.deps.is_empty() && n.handle.is_some() && !n.has_called_fn_once
        };
        if run {
            self.run_wave();
        }
    }

    fn subscribe_dep(&self, idx: usize, dep: &Core) {
        let weak: Weak<RefCell<NodeInner>> = Rc::downgrade(&self.0);
        let sink: Sink = Rc::new(move |msg: &Msg| {
            if let Some(rc) = weak.upgrade() {
                Core(rc).receive_from_dep(idx, msg);
            }
        });
        // dep.subscribe synchronously pushes START (+ cached DATA) into the sink,
        // re-entering receive_from_dep — done before we re-borrow self below.
        let unsub = dep.subscribe(sink);
        self.0.borrow_mut().dep_unsubs[idx] = Some(unsub);
    }

    fn deactivate(&self) {
        let (unsubs, hooks) = {
            let mut n = self.0.borrow_mut();
            n.activated = false;
            let unsubs: Vec<Unsub> = n.dep_unsubs.drain(..).flatten().collect();
            let hooks: Vec<Box<dyn FnOnce()>> = std::mem::take(&mut n.on_deactivation);
            // RAM: a compute node (fn and/or deps) clears its cache on deactivation;
            // a depless state node retains it (ROM).
            let is_compute = n.handle.is_some() || !n.deps.is_empty();
            if is_compute {
                n.cache = None;
                n.has_data = false;
                n.status = Status::Sentinel;
            }
            for i in 0..n.deps.len() {
                n.dep_batch[i] = None;
                n.dep_prev[i] = None;
                n.dep_has_data[i] = false;
                n.dep_dirty[i] = false;
                n.dep_tier[i] = 0;
            }
            n.pending = 0;
            n.emitted_dirty_this_wave = false;
            n.has_called_fn_once = false;
            if !n.state_persist {
                n.state = None;
            }
            (unsubs, hooks)
        };
        for u in unsubs {
            u();
        }
        for h in hooks {
            h();
        }
    }

    // ── upstream wave receive (two-phase + diamond) ──

    fn receive_from_dep(&self, idx: usize, msg: &Msg) {
        match msg {
            Message::Start => {}
            Message::Dirty => {
                let mark = {
                    let mut n = self.0.borrow_mut();
                    if !n.dep_dirty[idx] {
                        n.dep_dirty[idx] = true;
                        n.pending += 1;
                        n.dep_tier[idx] = 2;
                        true
                    } else {
                        false
                    }
                };
                if mark {
                    self.mark_dirty();
                }
            }
            Message::Data(v) => {
                {
                    let mut n = self.0.borrow_mut();
                    match &mut n.dep_batch[idx] {
                        Some(b) => b.push(v.clone()),
                        none => *none = Some(vec![v.clone()]),
                    }
                    n.dep_prev[idx] = Some(v.clone());
                    n.dep_has_data[idx] = true;
                    n.dep_tier[idx] = 3;
                    if n.dep_dirty[idx] {
                        n.dep_dirty[idx] = false;
                        n.pending -= 1;
                    }
                }
                self.maybe_run();
            }
            Message::Resolved => {
                {
                    let mut n = self.0.borrow_mut();
                    n.dep_tier[idx] = 3;
                    if n.dep_dirty[idx] {
                        n.dep_dirty[idx] = false;
                        n.pending -= 1;
                    }
                }
                self.maybe_run();
            }
            // Deferred slices: INVALIDATE / COMPLETE / ERROR / TEARDOWN / PAUSE / RESUME.
            _ => {}
        }
    }

    fn mark_dirty(&self) {
        let emit = {
            let mut n = self.0.borrow_mut();
            n.status = Status::Dirty;
            if !n.emitted_dirty_this_wave {
                n.emitted_dirty_this_wave = true;
                true
            } else {
                false
            }
        };
        if emit {
            self.emit_to_subs(&Message::Dirty);
        }
    }

    fn maybe_run(&self) {
        self.try_run();
    }

    fn try_run(&self) {
        let (pending, has_handle, has_called, partial, all_settled) = {
            let n = self.0.borrow();
            (
                n.pending,
                n.handle.is_some(),
                n.has_called_fn_once,
                n.partial,
                n.all_deps_settled(),
            )
        };
        if pending > 0 {
            return;
        }
        if !has_handle {
            self.passthrough_emit();
            return;
        }
        if !has_called {
            // first-run gate: hold the fn until every dep has settled (R-first-run-gate).
            if partial || all_settled {
                self.run_wave();
            }
            return;
        }
        self.run_wave();
    }

    fn passthrough_emit(&self) {
        // Single-dep wire (deps, no fn): relay dep 0's latest DATA downstream.
        let v = {
            let mut n = self.0.borrow_mut();
            let v = n.dep_batch[0].as_ref().and_then(|b| b.last().cloned());
            n.dep_batch[0] = None;
            v
        };
        if let Some(v) = v {
            self.down(vec![Message::Data(v)]);
        }
    }

    fn run_wave(&self) {
        if self.0.borrow().inside_run_wave {
            // R-reentrancy (D37): a fn re-driving its own dep mid-wave is the
            // synchronous feedback cycle. Proper [[ERROR,e]] graph-layer conversion
            // (D30) lands with the terminal slice / conformance C-6; for now surface
            // it loudly rather than recurse to a stack overflow.
            panic!(
                "synchronous feedback cycle: node fn re-entered its own wave (R-reentrancy / D37)"
            );
        }
        // build ctx (snapshot the per-dep wave view), reading under a short borrow.
        let (handle, dispatcher, ctx) = {
            let n = self.0.borrow();
            let dep_records: Vec<DepRecord> = (0..n.deps.len())
                .map(|i| {
                    let latest = n.dep_batch[i]
                        .as_ref()
                        .and_then(|b| b.last().cloned())
                        .or_else(|| n.dep_prev[i].clone());
                    DepRecord {
                        batch: n.dep_batch[i].clone(),
                        prev_data: n.dep_prev[i].clone(),
                        latest,
                        tier: n.dep_tier[i],
                    }
                })
                .collect();
            (
                n.handle.expect("run_wave ⇒ handle present"),
                n.dispatcher.clone(),
                Ctx::new(self.clone(), dep_records),
            )
        };
        {
            let mut n = self.0.borrow_mut();
            n.has_called_fn_once = true;
            n.inside_run_wave = true;
            n.emitted_tier3_this_wave = false;
        }
        let _guard = WaveGuard(self.clone());
        // borrow-free: the fn calls ctx.down → re-enters this node's _down.
        dispatcher.invoke(handle, &ctx);
        drop(_guard); // clears inside_run_wave
                      // R-resolved-undirty (D49): a node DIRTY'd downstream in phase 1
                      // that produced NO tier-3 value this run (filter-reject / no-emit
                      // fn) emits exactly one balancing RESOLVED to clear the downstream
                      // dirty — the substrate synthesizes it so the operator body stays
                      // clean (R-primary-api-clean).
        let undirty = {
            let n = self.0.borrow();
            n.emitted_dirty_this_wave && !n.emitted_tier3_this_wave
        };
        if undirty {
            {
                // status reflects cache freshness: a carried-over value → Resolved
                // (fresh, no new occurrence); never-valued → Sentinel. The RESOLVED
                // message clears the downstream dirty either way.
                let mut n = self.0.borrow_mut();
                n.status = if n.has_data {
                    Status::Resolved
                } else {
                    Status::Sentinel
                };
            }
            self.emit_to_subs(&Message::Resolved);
        }
        // roll wave-local state forward.
        let mut n = self.0.borrow_mut();
        for b in n.dep_batch.iter_mut() {
            *b = None;
        }
        n.emitted_dirty_this_wave = false;
        n.emitted_tier3_this_wave = false;
    }

    // ── downstream emission (the unified waist) ──

    /// Emit a wave toward sinks. Synthesizes the leading DIRTY for an external
    /// tier-3 emit (R-dirty-before-data), updates cache/status, and broadcasts.
    ///
    /// D49 (supersedes D15): every value-occurrence is emitted as **DATA** — the
    /// substrate does NOT substitute DATA→RESOLVED on value-equality and never
    /// inspects the per-wave DATA count for substitution (dedup is opt-in at the
    /// operator layer). A RESOLVED reaching here is an explicit operator escape
    /// hatch; the undirty RESOLVED is synthesized in [`Core::run_wave`].
    pub(crate) fn down(&self, msgs: Vec<Msg>) {
        let mut sorted = msgs;
        sorted.sort_by_key(|m| m.tier().as_u8()); // stable: preserves intra-tier order

        let mut data_count = 0usize;
        let mut has_resolved = false;
        let mut has_tier3 = false;
        for m in &sorted {
            match m {
                Message::Data(_) => {
                    data_count += 1;
                    has_tier3 = true;
                }
                Message::Resolved => {
                    has_resolved = true;
                    has_tier3 = true;
                }
                _ => {}
            }
        }
        // tier-3 exclusivity (R-resolved-undirty / D49): a wave's tier-3 slot is
        // ≥1 DATA XOR 1 RESOLVED (occurrence vs undirty), never mixed.
        assert!(
            !(data_count >= 1 && has_resolved),
            "down: a wave cannot mix DATA and RESOLVED (tier-3 exclusivity, R-resolved-undirty / D49)"
        );

        let inside = self.0.borrow().inside_run_wave;
        // Synthesize a leading DIRTY for an EXTERNAL tier-3 emit. Inside run_wave the
        // DIRTY already propagated in phase 1 (or the wave is activation-exempt).
        if has_tier3 && !inside {
            self.emit_dirty_once();
        }

        for m in sorted {
            match m {
                Message::Dirty => self.emit_dirty_once(),
                Message::Data(v) => {
                    // D49: every occurrence is DATA — no equals-substitution.
                    {
                        let mut n = self.0.borrow_mut();
                        n.cache = Some(v.clone());
                        n.has_data = true;
                        n.status = Status::Settled;
                        n.emitted_tier3_this_wave = true;
                    }
                    self.emit_to_subs(&Message::Data(v));
                }
                Message::Resolved => {
                    // Explicit operator escape hatch (D49); the substrate-synthesized
                    // undirty RESOLVED goes through run_wave, not here.
                    {
                        let mut n = self.0.borrow_mut();
                        n.status = Status::Resolved;
                        n.emitted_tier3_this_wave = true;
                    }
                    self.emit_to_subs(&Message::Resolved);
                }
                // Deferred slices: INVALIDATE / COMPLETE / ERROR / TEARDOWN / PAUSE / RESUME.
                _ => {}
            }
        }

        if !inside {
            let mut n = self.0.borrow_mut();
            n.emitted_dirty_this_wave = false;
            n.emitted_tier3_this_wave = false;
        }
    }

    fn emit_dirty_once(&self) {
        let emit = {
            let mut n = self.0.borrow_mut();
            if !n.emitted_dirty_this_wave {
                n.emitted_dirty_this_wave = true;
                n.status = Status::Dirty;
                true
            } else {
                false
            }
        };
        if emit {
            self.emit_to_subs(&Message::Dirty);
        }
    }

    fn emit_to_subs(&self, msg: &Msg) {
        // Clone the sink handles out, drop the borrow, then deliver — guards against
        // subscribe/unsubscribe (and any re-entry) during iteration.
        let subs: Vec<Sink> = self
            .0
            .borrow()
            .subscribers
            .iter()
            .map(|(_, s)| s.clone())
            .collect();
        for s in subs {
            s(msg);
        }
    }

    /// Emit upstream toward deps — control tiers only (R-ctx-up). Validates the
    /// kind; the terminus/forward actions (PAUSE lockset, INVALIDATE honor at a
    /// depless source, D38) are deferred to the control slice.
    pub(crate) fn up(&self, msgs: Vec<Msg>) {
        for m in &msgs {
            assert!(
                m.is_up_allowed(),
                "ctx.up: {m:?} is down-only; up carries control tiers only (R-ctx-up)"
            );
        }
        // Deferred: PAUSE/RESUME lockset, INVALIDATE terminus honor, intermediate forward.
    }

    // ── ctx.state + hooks (called from Ctx) ──

    pub(crate) fn get_state(&self) -> Option<AnyValue> {
        self.0.borrow().state.clone()
    }
    pub(crate) fn set_state(&self, v: AnyValue) {
        self.0.borrow_mut().state = Some(v);
    }
    pub(crate) fn set_state_persist(&self, on: bool) {
        self.0.borrow_mut().state_persist = on;
    }
    pub(crate) fn register_on_deactivation(&self, f: Box<dyn FnOnce()>) {
        self.0.borrow_mut().on_deactivation.push(f);
    }
}

/// The typed facade over the erased substrate node (D5). Re-types the boundary:
/// `cache()`/`set()` downcast to `T`; deps are erased via [`Node::erased`].
///
/// No `PartialEq` bound (D49: the substrate does no value-equality — dedup is
/// opt-in at the operator layer, e.g. `distinctUntilChanged`).
pub struct Node<T> {
    core: Core,
    _t: PhantomData<T>,
}

impl<T: 'static> Node<T> {
    /// A state node pre-populated with `initial`; a new subscriber gets `[DATA]`
    /// (R-initial). Manual source — push new values with [`Node::set`].
    pub fn state(initial: T) -> Node<T> {
        Node {
            core: Core::new(vec![], None, default_dispatcher(), Some(Rc::new(initial))),
            _t: PhantomData,
        }
    }

    /// A SENTINEL state node — no value until the first [`Node::set`].
    pub fn state_empty() -> Node<T> {
        Node {
            core: Core::new(vec![], None, default_dispatcher(), None),
            _t: PhantomData,
        }
    }

    /// A producer node (fn, no deps): runs once on activation, emits via `ctx.emit`.
    pub fn producer<F: Fn(&Ctx) + 'static>(f: F) -> Node<T> {
        let disp = default_dispatcher();
        let handle = disp.register(Rc::new(f));
        Node {
            core: Core::new(vec![], Some(handle), disp, None),
            _t: PhantomData,
        }
    }

    /// A derived node over erased deps. The fn reads deps positionally via
    /// `ctx.data::<U>(i)` and emits via `ctx.emit`. The first-run gate holds the fn
    /// until every dep has settled (R-first-run-gate). A fn that returns WITHOUT
    /// emitting (filter-reject) makes the substrate synthesize an undirty RESOLVED
    /// (D49 / R-resolved-undirty).
    pub fn derived<F: Fn(&Ctx) + 'static>(deps: Vec<Core>, f: F) -> Node<T> {
        let disp = default_dispatcher();
        let handle = disp.register(Rc::new(f));
        Node {
            core: Core::new(deps, Some(handle), disp, None),
            _t: PhantomData,
        }
    }

    /// Push a new value from a state node (one DATA wave). Always emits DATA — the
    /// substrate never absorbs to RESOLVED on value-equality (D49).
    pub fn set(&self, v: T) {
        self.core.down(vec![Message::Data(Rc::new(v))]);
    }

    /// The current cached value (downcast), or `None` for SENTINEL.
    pub fn cache(&self) -> Option<T>
    where
        T: Clone,
    {
        self.core
            .0
            .borrow()
            .cache
            .as_ref()
            .and_then(|a| a.downcast_ref::<T>().cloned())
    }

    /// The node's lifecycle status (R-status-enum).
    pub fn status(&self) -> Status {
        self.core.0.borrow().status
    }

    /// Subscribe a sink; returns an unsubscribe handle (call it to detach; the node
    /// deactivates when the last subscriber leaves, R-rom-ram).
    pub fn subscribe(&self, sink: impl Fn(&Msg) + 'static) -> Unsub {
        self.core.subscribe(Rc::new(sink))
    }

    /// The erased core, for wiring this node as a dep of a `derived` node.
    pub fn erased(&self) -> Core {
        self.core.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    /// Collect a subscriber's message-kind tags into a shared Vec for shape asserts.
    fn recorder() -> (Rc<RefCell<Vec<String>>>, impl Fn(&Msg) + 'static) {
        let log = Rc::new(RefCell::new(Vec::new()));
        let l2 = log.clone();
        (log, move |m: &Msg| l2.borrow_mut().push(format!("{m:?}")))
    }

    #[test]
    fn status_freshness_and_terminality() {
        assert!(Status::Settled.is_fresh());
        assert!(Status::Resolved.is_fresh());
        assert!(!Status::Dirty.is_fresh());
        assert!(Status::Completed.is_terminal());
        assert!(!Status::Settled.is_terminal());
    }

    #[test]
    fn state_node_push_on_subscribe_and_set() {
        let s = Node::<i32>::state(1);
        let (log, sink) = recorder();
        let _u = s.subscribe(sink);
        // push-on-subscribe: START then cached DATA (R-push-subscribe).
        assert_eq!(*log.borrow(), vec!["START", "DATA"]);
        assert_eq!(s.cache(), Some(1));

        s.set(2);
        // external tier-3 emit synthesizes a leading DIRTY (R-dirty-before-data).
        assert_eq!(*log.borrow(), vec!["START", "DATA", "DIRTY", "DATA"]);
        assert_eq!(s.cache(), Some(2));
        assert_eq!(s.status(), Status::Settled);
    }

    #[test]
    fn d49_repeated_equal_value_stays_data() {
        // D49 (supersedes D15): every occurrence is DATA — NO equals-substitution.
        // Probe: fromIter([1,1,1]) must be [DATA,DATA,DATA] so take/scan/count work.
        let s = Node::<i32>::state(5);
        let (log, sink) = recorder();
        let _u = s.subscribe(sink);
        s.set(5); // unchanged value → still DATA (never absorbed to RESOLVED)
        assert_eq!(*log.borrow(), vec!["START", "DATA", "DIRTY", "DATA"]);
        assert_eq!(s.status(), Status::Settled);
        s.set(5); // again → still DATA
        assert_eq!(
            *log.borrow(),
            vec!["START", "DATA", "DIRTY", "DATA", "DIRTY", "DATA"]
        );
        assert_eq!(s.status(), Status::Settled);
    }

    #[test]
    fn d49_filter_no_emit_synthesizes_undirty_resolved() {
        // R-resolved-undirty (D49): a derived fn DIRTY'd in phase 1 that returns
        // WITHOUT emitting (filter-reject) → the substrate synthesizes exactly one
        // RESOLVED to clear the downstream dirty (no wedge).
        let a = Node::<i32>::state_empty();
        let evens: Node<i32> = Node::derived(vec![a.erased()], |ctx| {
            let v = *ctx.data::<i32>(0).unwrap();
            if v % 2 == 0 {
                ctx.emit(v);
            } // odd → emit nothing (filter-reject)
        });
        let (log, sink) = recorder();
        let _u = evens.subscribe(sink);
        assert_eq!(*log.borrow(), vec!["START"]); // first-run gate holds (a SENTINEL)

        a.set(3); // odd → fn runs, no emit → substrate undirties with one RESOLVED
        assert_eq!(*log.borrow(), vec!["START", "DIRTY", "RESOLVED"]);
        assert_eq!(evens.cache(), None); // filtered → never valued
        assert_eq!(evens.status(), Status::Sentinel);

        a.set(4); // even → real DATA (downstream was NOT wedged by the prior RESOLVED)
        assert_eq!(
            *log.borrow(),
            vec!["START", "DIRTY", "RESOLVED", "DIRTY", "DATA"]
        );
        assert_eq!(evens.cache(), Some(4));
        assert_eq!(evens.status(), Status::Settled);

        a.set(5); // odd → filter; node KEEPS its value 4, status → Resolved (no new occurrence)
        assert_eq!(
            *log.borrow(),
            vec!["START", "DIRTY", "RESOLVED", "DIRTY", "DATA", "DIRTY", "RESOLVED"]
        );
        assert_eq!(evens.cache(), Some(4));
        assert_eq!(evens.status(), Status::Resolved);
    }

    #[test]
    fn d1_drop_releases_upstream_subscription_and_runs_cleanup() {
        // D1: dropping an active node (no explicit unsubscribe) must detach from its
        // deps and fire its cleanup hooks — Rust has no GC to reap the dead sink.
        let deactivated = Rc::new(Cell::new(false));
        let a = Node::<i32>::state(1);
        {
            let flag = deactivated.clone();
            let mid: Node<i32> = Node::derived(vec![a.erased()], move |ctx| {
                let f = flag.clone();
                ctx.on_deactivation(move || f.set(true));
                ctx.emit(*ctx.data::<i32>(0).unwrap() + 1);
            });
            let _sub = mid.subscribe(|_| {}); // activates mid: runs fn (registers hook) + subscribes to a
            assert!(!deactivated.get());
            // `mid` + `_sub` drop here → mid's Core refcount hits 0 → Drop runs cleanup.
        }
        assert!(
            deactivated.get(),
            "dropping an active node fires its on_deactivation + detaches from deps (D1)"
        );
        // `a` survived (still held), its sink was removed; a fresh subscribe still works.
        let (log, sink) = recorder();
        let _u = a.subscribe(sink);
        assert_eq!(*log.borrow(), vec!["START", "DATA"]);
    }

    #[test]
    fn producer_runs_once_on_activation() {
        let p = Node::<i32>::producer(|ctx| ctx.emit(42i32));
        let (log, sink) = recorder();
        let _u = p.subscribe(sink);
        // activation-exempt: the first run during subscribe needs no preceding DIRTY.
        assert_eq!(*log.borrow(), vec!["START", "DATA"]);
        assert_eq!(p.cache(), Some(42));
    }

    #[test]
    fn derived_first_run_gate_then_recompute() {
        let a = Node::<i32>::state_empty();
        let b = Node::<i32>::state_empty();
        let runs = Rc::new(Cell::new(0usize));
        let r2 = runs.clone();
        let sum: Node<i32> = Node::derived(vec![a.erased(), b.erased()], move |ctx| {
            r2.set(r2.get() + 1);
            let x = *ctx.data::<i32>(0).unwrap();
            let y = *ctx.data::<i32>(1).unwrap();
            ctx.emit(x + y);
        });
        let (log, sink) = recorder();
        let _u = sum.subscribe(sink);
        // first-run gate: neither dep has settled → fn has not fired.
        assert_eq!(runs.get(), 0);
        assert_eq!(*log.borrow(), vec!["START"]);

        a.set(3); // only one dep settled → gate still holds
        assert_eq!(runs.get(), 0);

        b.set(4); // both settled → first run, sum = 7
        assert_eq!(runs.get(), 1);
        assert_eq!(sum.cache(), Some(7));
        assert_eq!(*log.borrow(), vec!["START", "DIRTY", "DATA"]);

        a.set(10); // recompute (gate already passed), sum = 14
        assert_eq!(runs.get(), 2);
        assert_eq!(sum.cache(), Some(14));
    }

    #[test]
    fn diamond_recomputes_exactly_once_two_phase() {
        // A → B, A → C, B → D, C → D. D must join once per A update, glitch-free.
        let a = Node::<i32>::state_empty();
        let b: Node<i32> = Node::derived(vec![a.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap() + 1)
        });
        let c: Node<i32> = Node::derived(vec![a.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap() * 10)
        });
        let d_runs = Rc::new(Cell::new(0usize));
        let dr = d_runs.clone();
        let d: Node<i32> = Node::derived(vec![b.erased(), c.erased()], move |ctx| {
            dr.set(dr.get() + 1);
            ctx.emit(*ctx.data::<i32>(0).unwrap() + *ctx.data::<i32>(1).unwrap());
        });
        let (log, sink) = recorder();
        let _u = d.subscribe(sink);

        a.set(2); // B=3, C=20, D=23 — D fires EXACTLY once (R-diamond)
        assert_eq!(d_runs.get(), 1);
        assert_eq!(d.cache(), Some(23));
        // two-phase: DIRTY (phase 1, from the cascade) precedes DATA (phase 2). No glitch.
        assert_eq!(*log.borrow(), vec!["START", "DIRTY", "DATA"]);

        a.set(5); // B=6, C=50, D=56 — once more
        assert_eq!(d_runs.get(), 2);
        assert_eq!(d.cache(), Some(56));
    }

    #[test]
    fn rom_ram_deactivation_clears_compute_cache() {
        let a = Node::<i32>::state(7); // ROM: state node retains across disconnect
        let dbl: Node<i32> = Node::derived(vec![a.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap() * 2)
        });
        let (_log, sink) = recorder();
        let u = dbl.subscribe(sink);
        assert_eq!(dbl.cache(), Some(14));
        assert_eq!(dbl.status(), Status::Settled);

        u(); // last subscriber leaves → dbl deactivates
             // RAM: compute node cleared its cache; ROM: state node kept its value.
        assert_eq!(dbl.cache(), None);
        assert_eq!(dbl.status(), Status::Sentinel);
        assert_eq!(a.cache(), Some(7));
    }
}
