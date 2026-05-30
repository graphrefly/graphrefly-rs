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
//! ## Slice scope
//!
//! **CSP-5 first cut:** state / producer / derived nodes · two-phase DIRTY→DATA ·
//! diamond pending-counter join (R-diamond/R-two-phase) · first-run gate
//! (R-first-run-gate) · every occurrence is DATA + substrate-synthesized undirty
//! RESOLVED for a no-emit fn (R-resolved-undirty / D49, supersedes D15 — no
//! equals-substitution) · push-on-subscribe (R-push-subscribe) · lazy activation ·
//! ROM/RAM (R-rom-ram).
//!
//! **Control/terminal slice (this cut):** INVALIDATE + `ctx.on_invalidate`
//! (R-invalidate-idempotent / R-cleanup-hooks, C-3) · PAUSE/RESUME lockset +
//! default-mode coalesce (R-pause-lockset / R-pause-modes, C-5) · COMPLETE/ERROR
//! terminal (terminal-is-forever, D17) · the **D30 catch boundary** = a
//! `catch_unwind` at the wave-owner that converts a panic (the Rust analogue of a
//! value-level `throw`) into `[[ERROR,e]]` on a node ON the cycle (R-reentrancy /
//! D37, C-6), with whole-cascade wave-flag recovery (the touched-set reset — closes
//! **B25**). **Deferred:** TEARDOWN, batch, LocalAsync, rewire, dynamicNode,
//! replay-buffer, the resumeAll/false pause modes + async-paused buffering
//! (C-2/C-9/C-10), up-at-source INVALIDATE terminus (C-7).

use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::marker::PhantomData;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::{Rc, Weak};

use crate::ctx::{Ctx, DepRecord};
use crate::dispatcher::{default_dispatcher, Dispatcher};
use crate::protocol::{AnyValue, GraphError, Handle, LockId, Message, Wave};

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
    /// Flush-on-INVALIDATE hooks (R-cleanup-hooks / D28). Unlike `on_deactivation`
    /// these are **re-callable** (fire once per INVALIDATE wave, persist across
    /// invalidates), so `Rc<dyn Fn()>` not `Box<dyn FnOnce()>`. Cleared on deactivate.
    on_invalidate: Vec<Rc<dyn Fn()>>,

    // control + terminal
    /// PAUSE lockset (R-pause-lockset). Paused ⇔ non-empty; same-id PAUSE is
    /// idempotent (Set); RESUME of an unknown id is a no-op. Default ("true") mode:
    /// a dep wave that lands while paused is coalesced and fired once on final-lock
    /// RESUME (the resumeAll/false modes + async-paused buffering land with the
    /// async pool — C-2/C-9/C-10).
    pause_lockset: HashSet<LockId>,
    /// A dep wave was skipped while paused — re-run the fn once on final-lock RESUME.
    paused_dep_wave_occurred: bool,
    /// Terminal-is-forever (D17): once COMPLETE/ERROR has been emitted the node is
    /// final — it ignores further upstream messages and re-emits no terminal.
    terminal: bool,
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
        // B32: free this node's fn slot so the dispatcher pool no longer holds the fn —
        // and thus the upstream `Core`s the fn captured (e.g. a `Node: Clone` handle, the
        // idiomatic feedback pattern) — alive for the whole process. Safe: invoke clones
        // the fn out before calling (the pool is unborrowed during the fn run), so a node
        // dropped from inside a fn can re-borrow the pool here. A fn that captures its OWN
        // node is a genuine Rc cycle this cannot break (the pool keeps that node alive →
        // Drop never runs) — that is a user-level self-cycle (use a Weak self-handle).
        if let Some(handle) = self.handle {
            self.dispatcher.unregister(handle);
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
/// panic (D37) leaves the flag clean per frame (mirrors TS's try/finally around
/// `dispatcher.invoke`). The *other* stale wave-flags an unwinding panic leaves on
/// the source / intermediate frames (`emitted_dirty` / `dep_batch` / `dep_dirty` /
/// `pending`) are cleaned at the wave-owner catch via the [`WaveScope`] touched-set
/// (closes B25) — this guard only owns `inside_run_wave`.
struct WaveGuard(Core);
impl Drop for WaveGuard {
    fn drop(&mut self) {
        self.0 .0.borrow_mut().inside_run_wave = false;
    }
}

/// The D30 catch boundary + B25 whole-cascade recovery (per-language impl, D24 —
/// the Rust analogue of TS's graph-layer value-fn `try/catch`; Rust has no graph
/// layer yet, so the catch lives at the substrate wave-owner).
///
/// A `panic!` is the Rust analogue of a value-level `throw` (the fn signature is
/// `Fn(&Ctx) -> ()` — no `Result` channel; R-reentrancy mandates an *unwind*, not a
/// graceful return). The **outermost** public mutating entry (`subscribe` / `down` /
/// `up` / `set`) becomes the wave OWNER: it installs a scope, runs the cascade under
/// `catch_unwind`, and on a caught panic resets every participating node's transient
/// wave-flags ([`Core::reset_wave_flags`]) before emitting `[[ERROR,e]]` from the
/// `blamed` node. Nested re-entrant calls see the active scope and just run.
///
/// `blamed` is the innermost node whose fn actually ran ([`Core::run_wave`] records
/// it right before `invoke`), so a sync feedback cycle blames a node ON the cycle
/// (C-6) — the value-level catch "nearest the throw", impl-determined per
/// R-reentrancy. Falls back to the wave `owner` if no fn ran.
///
/// Forward-looking: this wave-owner boundary is the same "committed wave boundary"
/// that batch (D12) and the `ctx.rewire_next` deferred drain (D47) will reuse.
struct WaveScope {
    owner: Core,
    /// Every node that mutated a transient wave-flag this wave (may contain dups —
    /// [`Core::reset_wave_flags`] is idempotent; the set lives for one wave only).
    touched: Vec<Core>,
    /// Innermost node whose fn ran before the panic (the ERROR-bearing node).
    blamed: Option<Core>,
}

thread_local! {
    /// The current wave's scope, if a wave is in flight (single-thread, D22 ⇒
    /// thread-local, like the default dispatcher D26). `Some` ⇔ inside a wave.
    static WAVE: RefCell<Option<WaveScope>> = const { RefCell::new(None) };
}

/// `true` while a wave is in flight on this thread (a scope is installed).
fn wave_in_flight() -> bool {
    WAVE.with(|w| w.borrow().is_some())
}

/// Register a node as a wave participant (for the B25 touched-set reset). No-op if
/// no wave is in flight (e.g. a `cache` read outside any wave).
fn wave_register(core: &Core) {
    WAVE.with(|w| {
        if let Some(scope) = w.borrow_mut().as_mut() {
            scope.touched.push(core.clone());
        }
    });
}

/// Record the node whose fn is about to run — the ERROR-bearing node on a panic.
fn wave_set_blamed(core: &Core) {
    WAVE.with(|w| {
        if let Some(scope) = w.borrow_mut().as_mut() {
            scope.blamed = Some(core.clone());
        }
    });
}

/// Turn a caught panic payload into the untyped `GraphError` (D31).
fn panic_to_error(payload: Box<dyn std::any::Any + Send>) -> GraphError {
    let msg = payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "node fn panicked".to_owned());
    msg.into()
}

/// Run `body` as a wave, with `owner` the wave-owner if no wave is yet in flight.
/// The outermost caller installs a [`WaveScope`] + `catch_unwind`; on a caught
/// panic it resets every touched node's wave-flags (B25) and emits `[[ERROR,e]]`
/// from the blamed cycle node (D30), then returns `on_error()`. Nested calls (a
/// re-entrant `subscribe`/`down` during the cascade) just run `body` — the outer
/// owner owns the single catch.
fn with_wave_owner<R>(owner: &Core, body: impl FnOnce() -> R, on_error: impl FnOnce() -> R) -> R {
    if wave_in_flight() {
        return body();
    }
    WAVE.with(|w| {
        *w.borrow_mut() = Some(WaveScope {
            owner: owner.clone(),
            touched: Vec::new(),
            blamed: None,
        });
    });
    let result = catch_unwind(AssertUnwindSafe(body));
    let scope = WAVE.with(|w| w.borrow_mut().take());
    match result {
        Ok(r) => r,
        Err(payload) => {
            // The scope is taken (we are outside the wave again). Recover every node
            // the aborted wave corrupted (B25), then surface the failure as ERROR on a
            // node ON the cycle (D30) — a fresh terminal wave.
            let scope = scope.expect("wave owner installed a scope");
            for n in &scope.touched {
                n.reset_wave_flags();
            }
            let blamed = scope.blamed.unwrap_or(scope.owner);
            let err = panic_to_error(payload);
            // The recovery ERROR emit runs OUTSIDE any catch (the scope is taken). A
            // user subscriber sink that itself panics while handling this ERROR must not
            // escape the public API / fault the recovery — the node is going terminal
            // anyway. Swallow a sink-panic here (delivery may be partial; acceptable for
            // a node being torn down).
            let _ = catch_unwind(AssertUnwindSafe(move || {
                blamed.down(vec![Message::Error(err)]);
            }));
            on_error()
        }
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
            on_invalidate: Vec::new(),
            pause_lockset: HashSet::new(),
            paused_dep_wave_occurred: false,
            terminal: false,
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
        let dummy = Cell::new(None);
        self.subscribe_recording_id(sink, &dummy)
    }

    /// As [`Core::subscribe`], but records the new subscriber's id into `id_out`
    /// **immediately after registration** — before the push + activation cascade that
    /// may panic. The public [`Node::subscribe`] runs this under the wave-owner catch;
    /// if `activate()` panics (a feedback cycle), the sink is already registered, so the
    /// caller can still detach it (the error path reconstructs the real unsub from
    /// `id_out`) — no orphaned subscriber.
    fn subscribe_recording_id(&self, sink: Sink, id_out: &Cell<Option<u64>>) -> Unsub {
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
        id_out.set(Some(id));
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
            // Control + INVALIDATE hooks are fresh-lifecycle (R-cleanup-hooks / D28):
            // the next activation re-registers them from the fn body.
            n.on_invalidate.clear();
            n.pause_lockset.clear();
            n.paused_dep_wave_occurred = false;
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
        // Terminal-is-forever (D17): a terminated node ignores further upstream messages.
        if self.0.borrow().terminal {
            return;
        }
        // B25: enroll in the wave touched-set so a panic-abort can reset our flags.
        wave_register(self);
        match msg {
            Message::Start => {}
            Message::Invalidate => {
                // The dep's value is gone — drop our view of it (prev_data → SENTINEL so
                // the never-emitted detector reads correctly, C-3) and cascade idempotently.
                let (had_data, undirty_no_settle) = {
                    let mut n = self.0.borrow_mut();
                    n.dep_prev[idx] = None;
                    n.dep_has_data[idx] = false;
                    n.dep_batch[idx] = None;
                    // Un-wedge the dirty bookkeeping if this dep had gone DIRTY first, so an
                    // INVALIDATE-before-DATA doesn't strand `pending` / downstream forever
                    // (R-invalidate-idempotent — the wedged-DIRTY deadlock guard).
                    if n.dep_dirty[idx] {
                        n.dep_dirty[idx] = false;
                        n.pending -= 1;
                    }
                    // D50 / R-paused-invalidate: this INVALIDATE SUPERSEDES the dep's
                    // buffered paused dep-wave (dep_batch[idx] just cleared). Re-derive the
                    // paused-recompute flag — if no dep still carries a buffered DATA, CANCEL
                    // the paused recompute (the node has settled to SENTINEL via its own
                    // INVALIDATE; a RESUME must not recompute against a now-SENTINEL dep —
                    // attributed cancellation). A surviving dep keeps it set; a later DATA
                    // re-arms it ([DATA, INVALIDATE, DATA2] → recompute with DATA2).
                    if n.paused_dep_wave_occurred && n.dep_batch.iter().all(|b| b.is_none()) {
                        n.paused_dep_wave_occurred = false;
                    }
                    (n.has_data, n.pending == 0 && n.emitted_dirty_this_wave)
                };
                self.invalidate(); // cascades INVALIDATE iff populated; no-op otherwise
                                   // If we broadcast DIRTY this wave but produced no settle (never populated, so
                                   // the cascade was suppressed), un-dirty downstream with one RESOLVED.
                if undirty_no_settle {
                    let mut n = self.0.borrow_mut();
                    n.emitted_dirty_this_wave = false;
                    if !had_data {
                        n.status = Status::Sentinel;
                        drop(n);
                        self.emit_to_subs(&Message::Resolved);
                    }
                }
            }
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
            // Deferred: dep COMPLETE/ERROR/TEARDOWN propagation (operator-layer:
            // completeWhenDepsComplete / terminalAsRealInput) + PAUSE/RESUME are never
            // delivered to a dep-subscriber (a node is paused via its own up(), not by
            // an upstream dep).
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
        // R-pause-modes (default/"true" mode): while paused, skip dep-driven fn
        // re-execution and coalesce — fire once with the latest dep values on
        // final-lock RESUME. (resumeAll/false modes + async-paused buffering land
        // with the async pool — C-2/C-9/C-10.)
        if self.is_paused() {
            self.0.borrow_mut().paused_dep_wave_occurred = true;
            return;
        }
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
        // B25: enroll in the wave touched-set before any mutation so a panic-abort
        // can reset our flags.
        wave_register(self);
        if self.0.borrow().inside_run_wave {
            // R-reentrancy (D37): a fn re-driving its own dep mid-wave is the
            // synchronous feedback cycle. Reject by panicking (the Rust analogue of a
            // value-level throw) — the wave-owner's catch_unwind converts it to
            // [[ERROR,e]] on a node ON the cycle (D30 / C-6, see `with_wave_owner`),
            // rather than recurse to a stack overflow.
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
            // R-cleanup-hooks per-run lifecycle (D28 clarification / C-14): clear BOTH
            // hook lists before the fn runs; the fn body re-registers the current run's
            // hooks. Only the latest run's registrations are live — a re-run supersedes the
            // prior run's, discarded WITHOUT firing (no fire-on-rerun; onRerun stays cut).
            // Fixes the push-only accumulation (K stale hooks fired after K runs).
            n.on_invalidate.clear();
            n.on_deactivation.clear();
        }
        // D30: this is the innermost node whose fn runs — blame it if the fn (or a
        // re-entry it triggers) panics, so the ERROR lands "nearest the throw" (C-6).
        wave_set_blamed(self);
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
        // Terminal-is-forever (D17 / R-terminal): a terminated node emits nothing
        // further — including a self-emit DATA via `set()` / `ctx.down`. (The
        // COMPLETE/ERROR arms below also self-guard against a double terminal.)
        if self.0.borrow().terminal {
            return;
        }
        // B25: enroll in the wave touched-set before any mutation.
        wave_register(self);
        let mut sorted = msgs;
        sorted.sort_by_key(|m| m.tier().as_u8()); // stable: preserves intra-tier order

        // R-invalidate-idempotent: collapse repeated INVALIDATE in one wave so the
        // cleanup hook + downstream broadcast fire at most once.
        if sorted
            .iter()
            .filter(|m| matches!(m, Message::Invalidate))
            .count()
            > 1
        {
            let mut seen = false;
            sorted.retain(|m| {
                if matches!(m, Message::Invalidate) {
                    let keep = !seen;
                    seen = true;
                    keep
                } else {
                    true
                }
            });
        }

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
                Message::Invalidate => {
                    // Honor the invalidate-request: clear cache → SENTINEL, fire
                    // onInvalidate, broadcast downstream (idempotent, no-op if unpopulated).
                    self.invalidate();
                }
                Message::Complete => {
                    let go = {
                        let mut n = self.0.borrow_mut();
                        if n.terminal {
                            false
                        } else {
                            n.terminal = true;
                            n.status = Status::Completed;
                            true
                        }
                    };
                    if go {
                        self.emit_to_subs(&Message::Complete);
                    }
                }
                Message::Error(e) => {
                    let go = {
                        let mut n = self.0.borrow_mut();
                        if n.terminal {
                            false
                        } else {
                            n.terminal = true;
                            n.status = Status::Errored;
                            true
                        }
                    };
                    if go {
                        self.emit_to_subs(&Message::Error(e));
                    }
                }
                // Deferred slices: TEARDOWN / PAUSE / RESUME (delivered, not stored, here).
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

    /// Emit upstream toward deps — control tiers only (R-ctx-up). PAUSE/RESUME act on
    /// this node's own lockset (R-pause-lockset). The depless-source INVALIDATE
    /// terminus honor + intermediate forward (D38/R-up-at-source, C-7) is a later
    /// slice (Order 3) — non-PAUSE/RESUME control is dropped here for now.
    pub(crate) fn up(&self, msgs: Vec<Msg>) {
        for m in &msgs {
            assert!(
                m.is_up_allowed(),
                "ctx.up: {m:?} is down-only; up carries control tiers only (R-ctx-up)"
            );
        }
        for m in msgs {
            match m {
                Message::Pause(lock) => self.pause_acquire(lock),
                Message::Resume(lock) => self.pause_release(lock),
                _ => {} // INVALIDATE/DIRTY/TEARDOWN terminus → C-7 (deferred).
            }
        }
    }

    // ── PAUSE/RESUME lockset (R-pause-lockset) + default mode (R-pause-modes) ──

    fn is_paused(&self) -> bool {
        !self.0.borrow().pause_lockset.is_empty()
    }

    fn pause_acquire(&self, lock: LockId) {
        // Set ⇒ a same-id repeat PAUSE is idempotent.
        self.0.borrow_mut().pause_lockset.insert(lock);
    }

    fn pause_release(&self, lock: LockId) {
        let resumed = {
            let mut n = self.0.borrow_mut();
            if !n.pause_lockset.remove(&lock) {
                return; // unknown id ⇒ no-op
            }
            n.pause_lockset.is_empty() // another lock still held ⇒ stay paused
        };
        if resumed {
            self.on_resume();
        }
    }

    fn on_resume(&self) {
        // terminal-is-forever: a node that terminated while paused never recomputes.
        if self.0.borrow().terminal {
            self.0.borrow_mut().paused_dep_wave_occurred = false;
            return;
        }
        // Default mode: a dep wave skipped while paused fires the fn once now, with the
        // latest dep values. (The resumeAll/async pause-buffer drain lands with the
        // async pool — C-2/C-9/C-10.)
        let fire = {
            let mut n = self.0.borrow_mut();
            std::mem::take(&mut n.paused_dep_wave_occurred)
        };
        if fire {
            self.try_run();
        }
    }

    /// R-invalidate-idempotent: clear cache → SENTINEL, flush onInvalidate, broadcast
    /// INVALIDATE downstream. A no-op if nothing is cached (never-populated /
    /// already-reset are observationally equivalent — prevents double-cleanup).
    fn invalidate(&self) {
        let hooks = {
            let mut n = self.0.borrow_mut();
            if !n.has_data {
                return; // idempotent no-op
            }
            n.cache = None;
            n.has_data = false;
            n.status = Status::Sentinel;
            n.on_invalidate.clone() // re-callable; clone out so we fire borrow-free
        };
        for f in hooks {
            f();
        }
        self.emit_to_subs(&Message::Invalidate);
    }

    /// B25: reset transient wave-flags to their between-wave baseline after a
    /// panic-aborted wave (called from `with_wave_owner` on the touched-set). Leaves
    /// PERSISTED state (cache / dep_prev / dep_has_data / status / ctx.state) intact —
    /// only the wave-local bookkeeping a clean wave would have rolled forward itself.
    fn reset_wave_flags(&self) {
        let mut n = self.0.borrow_mut();
        n.inside_run_wave = false;
        n.emitted_dirty_this_wave = false;
        n.emitted_tier3_this_wave = false;
        n.pending = 0;
        for i in 0..n.deps.len() {
            n.dep_batch[i] = None;
            n.dep_dirty[i] = false;
        }
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
    pub(crate) fn register_on_invalidate(&self, f: Rc<dyn Fn()>) {
        self.0.borrow_mut().on_invalidate.push(f);
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
        self.down(vec![Message::Data(Rc::new(v))]);
    }

    /// Emit a raw wave downstream (R-node-iface). The wave-owner boundary for the D30
    /// catch (a panicking fn the cascade triggers becomes `[[ERROR,e]]`, not an escape).
    pub fn down(&self, msgs: Wave<AnyValue>) {
        let core = self.core.clone();
        with_wave_owner(&self.core, move || core.down(msgs), || {});
    }

    /// Emit a raw control wave upstream — control tiers only (R-ctx-up / R-node-iface).
    /// PAUSE/RESUME act on this node's lockset (R-pause-lockset).
    pub fn up(&self, msgs: Wave<AnyValue>) {
        let core = self.core.clone();
        with_wave_owner(&self.core, move || core.up(msgs), || {});
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
    ///
    /// This is also a wave-owner boundary: the activation cascade it drives runs under
    /// the D30 catch, so a synchronous feedback cycle surfaces as `[[ERROR,e]]` on a
    /// cycle node instead of escaping the subscribe call (C-6). Even on that error path
    /// the returned handle is a REAL unsubscribe (the sink is registered before the
    /// panicking activation, so the caller can always detach it).
    ///
    /// Panics (R-terminal / D17): subscribing to a non-resubscribable **terminal** node
    /// is rejected — the stream is permanently over. (Resubscribable opt-in reset is a
    /// later slice.)
    pub fn subscribe(&self, sink: impl Fn(&Msg) + 'static) -> Unsub {
        assert!(
            !self.core.0.borrow().terminal,
            "subscribe: node is terminal and non-resubscribable — the stream is permanently over (R-terminal / D17)"
        );
        let body_core = self.core.clone();
        let err_core = self.core.clone();
        let sink = Rc::new(sink);
        // Record the subscriber id before activation so the error path hands back a real
        // unsubscribe instead of leaking the just-registered sink (no orphaned sink).
        let id_cell: Rc<Cell<Option<u64>>> = Rc::new(Cell::new(None));
        let body_cell = id_cell.clone();
        with_wave_owner(
            &self.core,
            move || body_core.subscribe_recording_id(sink, &body_cell),
            move || match id_cell.get() {
                Some(id) => Box::new(move || err_core.unsubscribe(id)) as Unsub,
                None => Box::new(|| {}) as Unsub,
            },
        )
    }

    /// The erased core, for wiring this node as a dep of a `derived` node.
    pub fn erased(&self) -> Core {
        self.core.clone()
    }
}

/// A cheap handle clone — both `Node`s address the SAME underlying node (the shared
/// `Rc<RefCell<…>>`), like cloning a [`Core`]. Manual (not `#[derive]`) so it does
/// NOT require `T: Clone` — the value type need not be cloneable for the handle to be.
impl<T> Clone for Node<T> {
    fn clone(&self) -> Self {
        Node {
            core: self.core.clone(),
            _t: PhantomData,
        }
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

    #[test]
    #[should_panic(expected = "terminal")]
    fn subscribe_to_terminal_node_is_rejected() {
        // R-terminal (D17): a late subscribe to a non-resubscribable terminal node is
        // REJECTED (idiomatic error) — the stream is permanently over.
        let s = Node::<i32>::state(1);
        let _u = s.subscribe(|_| {}); // keep alive (so the node doesn't deactivate)
        s.down(vec![Message::Complete]); // S goes terminal
        assert_eq!(s.status(), Status::Completed);
        let _u2 = s.subscribe(|_| {}); // late subscribe to a terminal node → panics
    }

    #[test]
    fn set_on_terminal_node_is_a_noop() {
        // Terminal-is-forever (D17 / R-terminal): a self-emit (set/ctx.down) on a
        // terminated node emits nothing and does not resurrect its value.
        let s = Node::<i32>::state(1);
        let (log, sink) = recorder();
        let _u = s.subscribe(sink);
        assert_eq!(*log.borrow(), vec!["START", "DATA"]);

        s.down(vec![Message::Complete]); // S terminal
        assert_eq!(s.status(), Status::Completed);
        assert_eq!(*log.borrow(), vec!["START", "DATA", "COMPLETE"]);

        s.set(5); // self-emit after terminal → no-op (no DATA, value unchanged)
        assert_eq!(*log.borrow(), vec!["START", "DATA", "COMPLETE"]);
        assert_eq!(s.cache(), Some(1));
        assert_eq!(s.status(), Status::Completed);
    }

    #[test]
    fn b32_dropped_node_unregisters_its_fn_releasing_captures() {
        // B32: the dispatcher pool no longer holds a dropped node's fn forever. Before the
        // slotmap + unregister-on-Drop, a registered fn lived for the process, pinning the
        // upstream `Core`s it captured (the no-GC leak). Probe via a guard captured by the
        // fn: when the node drops, its fn slot is freed → the fn (and the guard) drop.
        struct DropFlag(Rc<Cell<bool>>);
        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }
        let fn_dropped = Rc::new(Cell::new(false));
        {
            let guard = DropFlag(fn_dropped.clone());
            let p = Node::<i32>::producer(move |ctx| {
                let _hold = &guard; // the fn captures the guard (stands in for a captured Core)
                ctx.emit(1i32);
            });
            let _u = p.subscribe(|_| {});
            assert!(
                !fn_dropped.get(),
                "the fn (and its capture) is live while the node is"
            );
            // `p` + `_u` drop here → p's Core refcount → 0 → NodeInner::Drop → dispatcher
            // .unregister → the pool drops the fn → the captured guard drops.
        }
        assert!(
            fn_dropped.get(),
            "a dropped node unregisters its fn from the pool, releasing the fn's captures (B32)"
        );
    }
}
