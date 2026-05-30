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

use crate::ctx::{Ctx, DepRecord, DepTerminal};
use crate::dispatcher::{default_dispatcher, Dispatcher, NodeFn, PoolKind};
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

/// PAUSE/RESUME backpressure mode (R-pause-modes). The OUTER gate over async-paused
/// buffering (D44).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Pausable {
    /// Default. Skip dep-driven fn recompute while paused, fire once on final-lock
    /// RESUME with the latest dep values. A leaf source's (depless) OWN production —
    /// sync or async — is NOT gated (delivered immediately); an async COMPUTE node's
    /// (deps>0) in-flight result buffers (R-async-paused / C-2).
    #[default]
    True,
    /// Production-gating: buffer the node's own tier-3/4 settle slice (sync OR async)
    /// while paused and replay it on final-lock RESUME. B9 pre-pause baseline-equals
    /// fidelity is deferred (same partial level as the TS arm).
    ResumeAll,
    /// Ignore PAUSE/RESUME ENTIRELY (timer/interval-class sources that must keep
    /// producing): the lockset is never consulted, nothing is gated or buffered (D44,
    /// resolves B20).
    False,
}

/// Node construction options (substrate-level). Sugar/operator opts are the graph layer
/// (D6/D24); this is the minimal substrate surface needed to select a pool (D20), a
/// pause mode (R-pause-modes), and the dep-terminal propagation policy (R-deps-terminal).
#[derive(Debug, Clone, Copy)]
pub struct NodeOpts {
    /// The dispatch pool (default LocalSync, R-sync-core).
    pub pool: PoolKind,
    /// The pause mode (default `true`, R-pause-modes).
    pub pausable: Pausable,
    /// Auto-emit COMPLETE when ALL deps complete (default `true`; combineLatest
    /// semantics — ALL not ANY). last/reduce/*Map set `false` to ABSORB inner
    /// terminals (R-deps-terminal).
    pub complete_when_deps_complete: bool,
    /// Auto-emit ERROR when ANY dep errors (default `true`). rescue/catch set `false`.
    pub error_when_deps_error: bool,
    /// A dep's terminal is a REAL INPUT the fn reads (rescue/reduce/*Map) rather than an
    /// absorbed settle — when set, the fn re-runs on a dep terminal and the first-run
    /// gate treats a terminated dep as settled (R-deps-terminal). Default `false`.
    pub terminal_as_real_input: bool,
}

/// Defaults: COMPLETE/ERROR auto-cascade ON (combineLatest-style), terminal-as-input OFF
/// — the plain derived/effect behavior. (A manual impl, not `#[derive(Default)]`, so the
/// two cascade flags default to `true`, not `false`.)
impl Default for NodeOpts {
    fn default() -> Self {
        Self {
            pool: PoolKind::default(),
            pausable: Pausable::default(),
            complete_when_deps_complete: true,
            error_when_deps_error: true,
            terminal_as_real_input: false,
        }
    }
}

type Msg = Message<AnyValue>;
type Sink = Rc<dyn Fn(&Msg)>;
type Unsub = Box<dyn FnOnce()>;

/// A deferred self-rewire request (R-rewire-deferred / D47), issued via `ctx.rewire_next`
/// and applied at the committed wave boundary. The new dep set is computed at APPLY time (so
/// multiple requests in one wave compose against the live deps), then the surgical R-rewire
/// (D42) runs as a fresh wave. The fn re-pairs the deps (SD-1 fn-deps pairing).
pub(crate) enum RewireRequest {
    /// Add a dep (idempotent if already present) + swap the fn.
    Add(Core, NodeFn),
    /// Remove a dep (idempotent if absent) + swap the fn.
    Remove(Core, NodeFn),
    /// Replace the whole dep set + swap the fn.
    Set(Vec<Core>, NodeFn),
}

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
    /// R-rewire (D42): a setDeps/addDep/removeDep is in flight. While set, an added
    /// cached dep's push-on-subscribe defers the fn-run (rewire_run_pending) so the
    /// settle is ONE atomic two-phase wave after every added dep is wired (P2 — never
    /// fire on a partially-populated added-dep view); also the reentrant-rewire guard.
    in_dep_mutation: bool,
    /// A rewire warrants exactly one post-mutation atomic settle (an added dep delivered
    /// data, or the sole dirty contributor was removed) — drained after the guard clears.
    rewire_run_pending: bool,

    // subscribers + activation
    subscribers: Vec<(u64, Sink)>,
    next_sub_id: u64,
    activated: bool,
    dep_unsubs: Vec<Option<Unsub>>,
    /// Per-dep CURRENT-index boxes (R-rewire Option-C / D42): each dep's subscribe
    /// callback reads its index from a shared `Cell` (O(1)), so a surgical rewire that
    /// reorders kept deps just updates the box — no re-subscribe, no per-message scan. A
    /// removed dep's box is set to -1 so a stale in-flight callback drops (drain).
    dep_idx_boxes: Vec<Rc<Cell<i64>>>,

    // ctx.state + cleanup hooks
    state: Option<AnyValue>,
    state_persist: bool,
    on_deactivation: Vec<Box<dyn FnOnce()>>,
    /// Flush-on-INVALIDATE hooks (R-cleanup-hooks / D28). Unlike `on_deactivation`
    /// these are **re-callable** (fire once per INVALIDATE wave, persist across
    /// invalidates), so `Rc<dyn Fn()>` not `Box<dyn FnOnce()>`. Cleared on deactivate.
    on_invalidate: Vec<Rc<dyn Fn()>>,

    // control + terminal
    /// Pause mode (R-pause-modes / D44). `true` coalesces dep-driven recompute; `false`
    /// ignores PAUSE entirely; `resumeAll` buffers+replays the own settle slice.
    pausable: Pausable,
    /// PAUSE lockset (R-pause-lockset). Paused ⇔ non-empty; same-id PAUSE is
    /// idempotent (Set); RESUME of an unknown id is a no-op. Default ("true") mode:
    /// a dep wave that lands while paused is coalesced and fired once on final-lock
    /// RESUME.
    pause_lockset: HashSet<LockId>,
    /// A dep wave was skipped while paused — re-run the fn once on final-lock RESUME
    /// (default "true" mode coalesce).
    paused_dep_wave_occurred: bool,
    /// Buffered tier-3/4 settle slices held while paused (resumeAll, and an async
    /// COMPUTE node's in-flight result in `true` mode — R-async-paused / D44). Replayed
    /// in arrival order on final-lock RESUME; discarded on terminal/deactivate (BH3).
    pause_buffer: Vec<Wave<AnyValue>>,
    /// Terminal-is-forever (D17): once COMPLETE/ERROR has been emitted the node is
    /// final — it ignores further upstream messages and re-emits no terminal.
    terminal: bool,

    // ── dep-terminal propagation (R-deps-terminal / R-terminal-settles-dirty, C-15) ──
    /// Per-dep terminal state (parallel array, indexed by dep position): `None` ⇔ the
    /// dep is live; `Some(..)` ⇔ it COMPLETEd/ERRORed. Read by the fn via
    /// `ctx.dep_records()[i].terminal` for `terminal_as_real_input` operators.
    dep_terminal: Vec<Option<DepTerminal>>,
    /// Auto-emit COMPLETE when ALL deps complete (default true). last/reduce/*Map set
    /// false to ABSORB inner terminals (R-deps-terminal).
    complete_when_deps_complete: bool,
    /// Auto-emit ERROR when ANY dep errors (default true). rescue/catch set false.
    error_when_deps_error: bool,
    /// A dep's terminal is a real input the fn reads (rescue/reduce/*Map), not an
    /// absorbed settle (R-deps-terminal).
    terminal_as_real_input: bool,
}

impl NodeInner {
    /// First-run gate predicate: every declared dep has delivered ≥1 DATA. A
    /// `terminal_as_real_input` node (rescue/reduce/*Map) also treats a TERMINATED dep
    /// as settled, so the fn may run when an inner terminates without ever delivering
    /// DATA (R-deps-terminal / R-first-run-gate).
    fn all_deps_settled(&self) -> bool {
        for i in 0..self.dep_has_data.len() {
            if self.dep_has_data[i] {
                continue;
            }
            if self.terminal_as_real_input && self.dep_terminal[i].is_some() {
                continue;
            }
            return false;
        }
        true
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

/// Clear `in_dep_mutation` on scope exit — including a panic unwind during a rewire
/// (QA-F1). A user fn can panic mid-rewire (an added dep's activation fn, or a removed
/// dep's onDeactivation hook); the wave-owner catch's [`Core::reset_wave_flags`] does NOT
/// cover `in_dep_mutation` (and may not include the rewiring node in its touched-set), so
/// without this RAII a caught rewire panic would wedge the node forever (every future
/// recompute deferred + every future rewire rejected as reentrant). Mirrors the TS arm's
/// `try { … } finally { _inDepMutation = false }`. `rewire_run_pending` needs no unwind
/// reset — it is reread/cleared at the start of the next `rewire` and has no other consumer.
struct DepMutationGuard(Core);
impl Drop for DepMutationGuard {
    fn drop(&mut self) {
        self.0 .0.borrow_mut().in_dep_mutation = false;
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
    let ret = match result {
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
    };
    // Committed wave boundary (R-rewire-deferred / D47): drain any `ctx.rewire_next` queued
    // during this wave, each applied as a FRESH wave. Runs on BOTH paths (like the TS
    // `exitWave` in a finally) so a queued self-rewire is not stranded by a caught panic.
    // The DRAINING guard makes a drained thunk's own wave-owner exit a no-op, so this
    // outermost exit owns the loop.
    drain_deferred_rewires();
    ret
}

thread_local! {
    /// FIFO of deferred self-rewires (`ctx.rewire_next`), drained at the committed wave
    /// boundary (R-rewire-deferred / D47). Single-thread (D22) ⇒ thread-local, like [`WAVE`].
    static DEFERRED_REWIRE: RefCell<Vec<Box<dyn FnOnce()>>> = const { RefCell::new(Vec::new()) };
    /// `true` while [`drain_deferred_rewires`] is running, so a drained thunk's own
    /// wave-owner exit does not re-enter the drain (the outermost drain owns the loop).
    static DRAINING_REWIRE: Cell<bool> = const { Cell::new(false) };
}

/// Queue a deferred self-rewire (R-rewire-deferred / D47). Applied at the committed wave
/// boundary by [`drain_deferred_rewires`], never in place.
fn defer_rewire(thunk: Box<dyn FnOnce()>) {
    DEFERRED_REWIRE.with(|q| q.borrow_mut().push(thunk));
}

/// Drain the deferred-rewire FIFO at the committed boundary. Each thunk applies one queued
/// self-rewire as a FRESH wave (its own wave-owner) which may enqueue MORE — appended and
/// drained by this same loop (DrainExactlyOnce). A single global FIFO yields the drain order
/// for free: issue order during a synchronous cascade IS causal order (a dep settles before
/// its dependent's fn runs), so global-FIFO == per-node FIFO + causal-node order. Per-thunk
/// isolation: a thunk that panics does NOT abandon the rest of the queue (which would strand
/// thunks to mis-fire at an unrelated later wave); the first escape re-surfaces once the
/// queue is empty. Process-global is correct per D22 (one sync cascade per causal domain;
/// cross-domain is the async wire bridge, which never shares this stack — same basis as the
/// module-global `batch.active`).
fn drain_deferred_rewires() {
    if DRAINING_REWIRE.with(|d| d.get()) {
        return; // a nested wave-owner exit during the drain — the outer loop owns draining
    }
    if DEFERRED_REWIRE.with(|q| q.borrow().is_empty()) {
        return; // F-PERF: one empty-queue check per outermost wave when unused (behavior-neutral)
    }
    DRAINING_REWIRE.with(|d| d.set(true));
    let mut escaped: Option<Box<dyn std::any::Any + Send>> = None;
    loop {
        let thunk = DEFERRED_REWIRE.with(|q| {
            let mut q = q.borrow_mut();
            if q.is_empty() {
                None
            } else {
                Some(q.remove(0))
            }
        });
        let Some(thunk) = thunk else { break };
        if let Err(e) = catch_unwind(AssertUnwindSafe(thunk)) {
            if escaped.is_none() {
                escaped = Some(e);
            }
        }
    }
    DRAINING_REWIRE.with(|d| d.set(false));
    if let Some(e) = escaped {
        std::panic::resume_unwind(e);
    }
}

impl Core {
    fn new(
        deps: Vec<Core>,
        handle: Option<Handle>,
        dispatcher: Dispatcher,
        initial: Option<AnyValue>,
        pausable: Pausable,
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
            in_dep_mutation: false,
            rewire_run_pending: false,
            subscribers: Vec::new(),
            next_sub_id: 0,
            activated: false,
            dep_unsubs: Vec::new(),
            dep_idx_boxes: Vec::new(),
            state: None,
            state_persist: false,
            on_deactivation: Vec::new(),
            on_invalidate: Vec::new(),
            pausable,
            pause_lockset: HashSet::new(),
            paused_dep_wave_occurred: false,
            pause_buffer: Vec::new(),
            terminal: false,
            dep_terminal: vec![None; n],
            // Defaults: plain derived/effect — auto-cascade COMPLETE/ERROR, terminal not
            // an input. derived_opts overrides from NodeOpts for last/reduce/rescue/*Map.
            complete_when_deps_complete: true,
            error_when_deps_error: true,
            terminal_as_real_input: false,
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
            // placeholder boxes (distinct Rcs); subscribe_dep overwrites each with the
            // real box its callback captures.
            n.dep_idx_boxes = (0..n.deps.len())
                .map(|_| Rc::new(Cell::new(-1i64)))
                .collect();
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
        // R-rewire Option-C: the callback reads the dep's CURRENT index from a shared box
        // so a surgical reorder reroutes in O(1); -1 means the dep was removed (drain).
        let idx_box: Rc<Cell<i64>> = Rc::new(Cell::new(idx as i64));
        let cb_box = idx_box.clone();
        let sink: Sink = Rc::new(move |msg: &Msg| {
            let i = cb_box.get();
            if i < 0 {
                return; // dep removed — stale in-flight callback drops (drain)
            }
            if let Some(rc) = weak.upgrade() {
                Core(rc).receive_from_dep(i as usize, msg);
            }
        });
        // dep.subscribe synchronously pushes START (+ cached DATA) into the sink,
        // re-entering receive_from_dep — done before we re-borrow self below.
        let unsub = dep.subscribe(sink);
        let mut n = self.0.borrow_mut();
        n.dep_unsubs[idx] = Some(unsub);
        n.dep_idx_boxes[idx] = idx_box;
    }

    fn deactivate(&self) {
        let (unsubs, hooks) = {
            let mut n = self.0.borrow_mut();
            n.activated = false;
            let unsubs: Vec<Unsub> = n.dep_unsubs.drain(..).flatten().collect();
            n.dep_idx_boxes.clear();
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
                n.dep_terminal[i] = None;
            }
            n.pending = 0;
            n.emitted_dirty_this_wave = false;
            n.has_called_fn_once = false;
            // Control + INVALIDATE hooks are fresh-lifecycle (R-cleanup-hooks / D28):
            // the next activation re-registers them from the fn body.
            n.on_invalidate.clear();
            n.pause_lockset.clear();
            n.paused_dep_wave_occurred = false;
            n.pause_buffer.clear();
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
            Message::Complete => {
                {
                    let mut n = self.0.borrow_mut();
                    n.dep_terminal[idx] = Some(DepTerminal::Complete);
                }
                // R-terminal-settles-dirty (B35): a terminal RELEASES this dep's outstanding
                // in-wave DIRTY contribution (the exactly-one-settle invariant) — exactly as
                // DATA/RESOLVED/INVALIDATE do; a dirty-then-COMPLETE-without-DATA dep would
                // otherwise strand `pending` and wedge the node (the INVALIDATE arm's guard,
                // generalized to terminals).
                self.release_dep_dirty(idx);
                let (auto_complete, tari) = {
                    let n = self.0.borrow();
                    (n.complete_when_deps_complete, n.terminal_as_real_input)
                };
                if auto_complete && self.all_deps_complete() {
                    // combineLatest semantics (R-deps-terminal): COMPLETE only when ALL deps
                    // complete (not ANY). The node itself goes terminal (pending moot).
                    self.down(vec![Message::Complete]);
                } else if tari {
                    // rescue/reduce/*Map: the fn reads ctx.dep_records()[idx].terminal.
                    self.maybe_run();
                } else {
                    // absorbed terminal, NOT an input: the dep's signalled change did not
                    // materialise (no DATA) → un-dirty downstream, keep cache.
                    self.settle_after_absorbed_terminal();
                }
            }
            Message::Error(e) => {
                {
                    let mut n = self.0.borrow_mut();
                    // Box<dyn Error> is not Clone and `msg` is a borrow, so store the error's
                    // Display message (the concrete type is not preserved — per-language, D31).
                    n.dep_terminal[idx] = Some(DepTerminal::Error(format!("{e}").into()));
                }
                self.release_dep_dirty(idx); // R-terminal-settles-dirty (B35), as COMPLETE
                let (auto_error, tari) = {
                    let n = self.0.borrow();
                    (n.error_when_deps_error, n.terminal_as_real_input)
                };
                if auto_error {
                    // Auto-cascade ERROR (R-deps-terminal): forward a FRESH Box rebuilt from the
                    // message (Box<dyn Error> isn't Clone). The node goes terminal.
                    self.down(vec![Message::Error(format!("{e}").into())]);
                } else if tari {
                    self.maybe_run(); // rescue/catch: the fn reads the dep's terminal (the error)
                } else {
                    self.settle_after_absorbed_terminal();
                }
            }
            // TEARDOWN propagation is a later slice (B33); PAUSE/RESUME are never delivered to
            // a dep-subscriber (a node is paused via its own up(), not by an upstream dep).
            _ => {}
        }
    }

    /// R-terminal-settles-dirty (B35): release a dep's outstanding in-wave DIRTY
    /// contribution. A settle-class event for that dep (DATA/RESOLVED inline above,
    /// INVALIDATE, and now COMPLETE/ERROR) clears its dirty flag + decrements `pending`.
    /// No-op if the dep already settled this wave — so the normal DATA-then-COMPLETE flow
    /// is unaffected. Makes the exactly-one-settle invariant a single shared step.
    fn release_dep_dirty(&self, idx: usize) {
        let mut n = self.0.borrow_mut();
        if n.dep_dirty[idx] {
            n.dep_dirty[idx] = false;
            n.pending -= 1;
        }
    }

    /// True iff EVERY dep has COMPLETEd (combineLatest semantics, R-deps-terminal). An
    /// ERRORed dep is NOT complete → false. A node with no deps is never auto-complete.
    fn all_deps_complete(&self) -> bool {
        let n = self.0.borrow();
        if n.deps.is_empty() {
            return false;
        }
        n.dep_terminal
            .iter()
            .all(|t| matches!(t, Some(DepTerminal::Complete)))
    }

    /// R-terminal-settles-dirty (B35): settle a node whose dirtied dep was released by an
    /// ABSORBED terminal that is NOT a real input (a plain derived/effect — one of several
    /// deps completing while others stay live). Runs only when the release drained `pending`
    /// while the node still owes a downstream settle (it broadcast DIRTY this wave):
    ///   - some OTHER dep delivered real DATA this wave → recompute (→ DATA);
    ///   - else no value materialised (or the recompute is gated) → one undirty RESOLVED
    ///     (R-resolved-undirty), keeping the cache (a terminal, unlike INVALIDATE, leaves it).
    fn settle_after_absorbed_terminal(&self) {
        {
            let n = self.0.borrow();
            if n.pending != 0 || !n.emitted_dirty_this_wave {
                return;
            }
        }
        // A real value occurred this wave (some OTHER dep delivered DATA) → recompute.
        // maybe_run runs the fn ONLY if ungated (gate open, not paused); it may emit DATA, a
        // fn-synthesized undirty RESOLVED, or nothing (gated / gate still holds).
        let saw_data = self
            .0
            .borrow()
            .dep_batch
            .iter()
            .any(|b| b.as_ref().is_some_and(|v| !v.is_empty()));
        if saw_data {
            self.maybe_run();
        }
        // If the node STILL owes a downstream settle (no DATA occurred, OR the recompute was
        // gated — e.g. the first-run gate holds because the terminated dep never delivered and
        // terminal_as_real_input is false), balance the broadcast DIRTY with one undirty
        // RESOLVED, keeping the cache. Without this fallback a DIRTY-then-terminal-without-DATA
        // dep on a pre-first-run multi-dep node strands the DIRTY → downstream wedged (the B35
        // gate-holds corner, C-15(d)). Bare emit mirrors the INVALIDATE receive-arm; the
        // terminal×pause/batch coalescing is backlog B39.
        let still_owes = self.0.borrow().emitted_dirty_this_wave;
        if still_owes {
            {
                let mut n = self.0.borrow_mut();
                n.emitted_dirty_this_wave = false;
                n.status = if n.has_data {
                    Status::Resolved
                } else {
                    Status::Sentinel
                };
            }
            self.emit_to_subs(&Message::Resolved);
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
        // R-rewire (D42): an added cached dep's push-on-subscribe lands here mid-mutation.
        // Defer the fn-run to ONE atomic settle after every added dep is wired (P2 — the
        // fn never fires on a partially-populated added-dep view).
        if self.0.borrow().in_dep_mutation {
            self.0.borrow_mut().rewire_run_pending = true;
            return;
        }
        // R-pause-modes: only the default "true" mode coalesces dep-driven recompute
        // (skip the fn while any lock is held → fire once with the latest dep values on
        // final-lock RESUME). `resumeAll` RUNS the fn while paused but buffers its output
        // (down → should_buffer_on_pause); `false` runs + emits immediately (ignores
        // PAUSE). D44: pause mode is the outer gate.
        if matches!(self.0.borrow().pausable, Pausable::True) && self.is_paused() {
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

    // ── runtime topology rewire (R-rewire / D42, C-8) ──

    /// Runtime topology rewire — the public substrate entry (called by `Node::set_deps`/
    /// `add_dep`/`remove_dep`). The validation REJECTS run first and panic: called
    /// EXTERNALLY (no wave in flight) the panic propagates to the caller (idiomatic
    /// error); called MID-FN (inside a wave) the panic is caught by the running wave-owner
    /// → `[[ERROR,e]]` (R-reentrancy/D37 — a fn mutating its own topology mid-wave is the
    /// feedback cycle). The surgical Option-C mutation + atomic settle then run under a
    /// FRESH wave-owner so a user-fn panic during dep-activation/settle becomes ERROR
    /// (D30). INTRA-graph only (D22). Requires an explicit fn (SD-1 fn-deps pairing).
    pub(crate) fn rewire(&self, new_deps: Vec<Core>, fn_: NodeFn) {
        let new_deps = dedup_cores(new_deps);
        {
            let n = self.0.borrow();
            assert!(
                !n.terminal,
                "rewire: node is terminal (completed/errored) — cannot rewire (R-rewire / D42)"
            );
            assert!(
                !n.inside_run_wave,
                "rewire: mid-fn topology mutation — a fn mutating its own deps mid-wave is the feedback cycle (R-rewire / D37)"
            );
            assert!(
                !n.in_dep_mutation,
                "rewire: reentrant dep mutation — another setDeps/addDep/removeDep is in flight (R-rewire)"
            );
        }
        assert!(
            !new_deps.iter().any(|d| Rc::ptr_eq(&d.0, &self.0)),
            "rewire: self-dependency rejected (R-rewire / D42)"
        );
        let old_deps: Vec<Core> = self.0.borrow().deps.clone();
        for d in new_deps
            .iter()
            .filter(|d| !old_deps.iter().any(|o| Rc::ptr_eq(&o.0, &d.0)))
        {
            assert!(
                !reachable_upstream(d, self),
                "rewire: would create a cycle — dep already transitively depends on this node (R-rewire / D42)"
            );
            // Rust has no resubscribable-terminal opt-in yet (R-terminal, later slice) ⇒
            // ALL terminal deps are non-resubscribable → adding any terminal dep is rejected.
            assert!(
                !d.0.borrow().terminal,
                "rewire: cannot add a non-resubscribable terminal dep — would wedge (R-rewire / D42)"
            );
        }
        let core = self.clone();
        with_wave_owner(self, move || core.rewire_apply(new_deps, fn_), || {});
    }

    // ── deferred self-rewire (R-rewire-deferred / D47): ctx.rewire_next ──

    /// Enqueue a deferred self-rewire (`ctx.rewire_next`). Applied at the committed wave
    /// boundary by the wave-owner drain, NEVER in place — the immediate `set_deps`/`add_dep`/
    /// `remove_dep` still panics mid-fn (D37/R-reentrancy). The drain runs each as a fresh
    /// wave; this is the substrate affordance the higher-order *Map operators wire inners with.
    pub(crate) fn request_rewire_next(&self, req: RewireRequest) {
        let node = self.clone();
        defer_rewire(Box::new(move || node.apply_rewire_next(req)));
    }

    /// Apply one queued self-rewire at the boundary (a drain thunk). A node that went TERMINAL
    /// during the wave DISCARDS its queued requests (R-rewire-deferred). The dep set is composed
    /// against the LIVE deps at apply time (so several requests in one wave compose). A rewire
    /// reject (cycle/self/non-resubscribable terminal dep) panics — caught here and surfaced as
    /// `[[ERROR,e]]` on this node (D30-consistent) so it does not strand the rest of the drain.
    fn apply_rewire_next(&self, req: RewireRequest) {
        if self.0.borrow().terminal {
            return; // terminal discards the queue
        }
        let (new_deps, fn_) = match req {
            RewireRequest::Set(deps, f) => (deps, f),
            RewireRequest::Add(dep, f) => {
                let mut next = self.0.borrow().deps.clone();
                if !next.iter().any(|d| Rc::ptr_eq(&d.0, &dep.0)) {
                    next.push(dep);
                }
                (next, f)
            }
            RewireRequest::Remove(dep, f) => {
                let next: Vec<Core> = self
                    .0
                    .borrow()
                    .deps
                    .iter()
                    .filter(|d| !Rc::ptr_eq(&d.0, &dep.0))
                    .cloned()
                    .collect();
                (next, f)
            }
        };
        // rewire() dedups + validates + applies under its own wave-owner (a fn-panic during the
        // apply becomes ERROR on the blamed node there). Only the PRE-apply validation rejects
        // panic OUT of rewire() — caught here → ERROR on this node, via owned_down (a fresh
        // wave-owner; the drain runs outside any live wave).
        let core = self.clone();
        let outcome = catch_unwind(AssertUnwindSafe(move || core.rewire(new_deps, fn_)));
        if let Err(payload) = outcome {
            let err = panic_to_error(payload);
            self.owned_down(vec![Message::Error(err)]);
        }
    }

    /// The surgical Option-C mutation + atomic settle (D42), run under a wave-owner. Kept
    /// deps keep their subscription + per-dep state + idx-box (rerouted O(1)); removed deps
    /// drain (box→-1, unsub) + drop their dirty contribution; added deps fresh-subscribe
    /// (push-on-subscribe). The first-run gate + cache are PRESERVED (Q2/Q7). One atomic
    /// settle if any added dep delivered data or the sole dirty contributor was removed.
    fn rewire_apply(&self, new_deps: Vec<Core>, fn_: NodeFn) {
        let old_deps: Vec<Core> = self.0.borrow().deps.clone();
        let n = new_deps.len();
        let added: Vec<Core> = new_deps
            .iter()
            .filter(|d| !old_deps.iter().any(|o| Rc::ptr_eq(&o.0, &d.0)))
            .cloned()
            .collect();
        let removed: Vec<Core> = old_deps
            .iter()
            .filter(|d| !new_deps.iter().any(|nd| Rc::ptr_eq(&nd.0, &d.0)))
            .cloned()
            .collect();

        {
            let mut nn = self.0.borrow_mut();
            nn.in_dep_mutation = true;
            nn.rewire_run_pending = false;
        }
        // QA-F1 (critical): clear `in_dep_mutation` on EVERY exit incl. a panic unwind during
        // the mutation. A user fn CAN panic in this window — an added dep's activation fn
        // (subscribe_dep → dep.activate → run_wave) or a removed dep's onDeactivation hook
        // (u() → deactivate → user closure). The wave-owner catch (with_wave_owner) then runs
        // reset_wave_flags, which does NOT cover in_dep_mutation and may not even include `self`
        // in `touched` — so without this guard a caught rewire panic leaves the node permanently
        // in_dep_mutation=true: every future maybe_run defers (never recomputes) + every future
        // rewire is rejected as reentrant. Mirrors the TS `finally { _inDepMutation = false }`.
        let _mut_guard = DepMutationGuard(self.clone());
        let activated = self.0.borrow().activated;
        let mut zero_dep_undirty = false;

        // fn swap (SD-1 + B32 GC): register the new fn in the SAME pool, then unregister the
        // old handle so the rewired-away closure (and its captures) is freed and its slot
        // reused. Register-first keeps `handle` never pointing at a freed slot.
        {
            let (old_handle, disp) = {
                let nn = self.0.borrow();
                (nn.handle, nn.dispatcher.clone())
            };
            let kind = old_handle
                .map(|h| disp.pool_kind(h.pool_id))
                .unwrap_or(PoolKind::Sync);
            let new_handle = register_with(&disp, kind, fn_);
            self.0.borrow_mut().handle = Some(new_handle);
            if let Some(oh) = old_handle {
                disp.unregister(oh);
            }
        }

        // removed deps: clear dirty contribution + drain (box→-1, unsubscribe the edge).
        let mut removed_dirty_contributor = false;
        for d in &removed {
            let old_idx = old_deps
                .iter()
                .position(|o| Rc::ptr_eq(&o.0, &d.0))
                .expect("removed dep was in old_deps");
            let unsub = {
                let mut nn = self.0.borrow_mut();
                if nn.dep_dirty[old_idx] {
                    removed_dirty_contributor = true;
                    nn.pending -= 1;
                }
                if let Some(b) = nn.dep_idx_boxes.get(old_idx) {
                    b.set(-1); // drain: any stale in-flight callback drops
                }
                nn.dep_unsubs.get_mut(old_idx).and_then(|u| u.take())
            };
            if let Some(u) = unsub {
                u(); // stops the removed dep's edge — no further delivery
            }
        }

        // rebuild the per-dep parallel arrays in new order; kept deps carry their state +
        // subscription + idx-box (rerouted to the new index), added deps start fresh.
        {
            let mut nn = self.0.borrow_mut();
            let mut new_batch: Vec<Option<Vec<AnyValue>>> = vec![None; n];
            let mut new_prev: Vec<Option<AnyValue>> = vec![None; n];
            let mut new_has = vec![false; n];
            let mut new_dirty = vec![false; n];
            let mut new_tier = vec![0u8; n];
            let mut new_terminal: Vec<Option<DepTerminal>> = vec![None; n];
            let mut new_unsubs: Vec<Option<Unsub>> = (0..n).map(|_| None).collect();
            let mut new_boxes: Vec<Rc<Cell<i64>>> =
                (0..n).map(|_| Rc::new(Cell::new(-1i64))).collect();
            for (j, dep) in new_deps.iter().enumerate() {
                if let Some(old_idx) = old_deps.iter().position(|o| Rc::ptr_eq(&o.0, &dep.0)) {
                    new_batch[j] = nn.dep_batch[old_idx].take();
                    new_prev[j] = nn.dep_prev[old_idx].clone();
                    new_has[j] = nn.dep_has_data[old_idx];
                    new_dirty[j] = nn.dep_dirty[old_idx];
                    new_tier[j] = nn.dep_tier[old_idx];
                    new_terminal[j] = nn.dep_terminal[old_idx].clone();
                    if let Some(slot) = nn.dep_unsubs.get_mut(old_idx) {
                        new_unsubs[j] = slot.take();
                    }
                    if let Some(b) = nn.dep_idx_boxes.get(old_idx) {
                        b.set(j as i64); // O(1) reroute of the kept dep's callback index
                        new_boxes[j] = b.clone();
                    }
                }
            }
            nn.deps = new_deps.clone();
            nn.dep_batch = new_batch;
            nn.dep_prev = new_prev;
            nn.dep_has_data = new_has;
            nn.dep_dirty = new_dirty;
            nn.dep_tier = new_tier;
            nn.dep_terminal = new_terminal;
            nn.dep_unsubs = new_unsubs;
            nn.dep_idx_boxes = new_boxes;
        }

        // subscribe added deps — push-on-subscribe delivers a cached dep's DATA (driving
        // maybe_run, which DEFERS to the atomic settle via in_dep_mutation); a SENTINEL dep
        // delivers START only. Only when activated (else activation will subscribe later).
        if activated {
            for d in &added {
                let idx = new_deps
                    .iter()
                    .position(|nd| Rc::ptr_eq(&nd.0, &d.0))
                    .expect("added dep present in new_deps");
                self.subscribe_dep(idx, d);
            }
        }

        // Q6 auto-settle: removing the sole dirty contributor closes the wave. With deps
        // remaining → request the atomic settle; with zero deps → just un-dirty downstream
        // (degenerate fn-no-deps). Cache is preserved either way (Q7).
        {
            let nn = self.0.borrow();
            if removed_dirty_contributor && nn.pending == 0 && nn.status == Status::Dirty {
                if new_deps.is_empty() {
                    zero_dep_undirty = true;
                } else {
                    drop(nn);
                    self.0.borrow_mut().rewire_run_pending = true;
                }
            }
        }

        self.0.borrow_mut().in_dep_mutation = false;

        // Atomic post-mutation settle (a fresh wave): ONE two-phase DIRTY→DATA if an added
        // dep delivered data or a sole-dirty dep was removed; else the zero-dep un-dirty.
        let run_pending = {
            let mut nn = self.0.borrow_mut();
            std::mem::take(&mut nn.rewire_run_pending)
        };
        if run_pending {
            self.settle_rewire();
        } else if zero_dep_undirty {
            let emitted = self.0.borrow().emitted_dirty_this_wave;
            if emitted {
                self.down(vec![Message::Resolved]); // un-dirty downstream (pause/batch-safe)
            } else {
                let mut nn = self.0.borrow_mut();
                nn.status = if nn.has_data {
                    Status::Settled
                } else {
                    Status::Sentinel
                };
            }
        }
    }

    /// R-rewire atomic settle (D42 + the D1 /qa fix): emit ONE proper two-phase
    /// DIRTY→DATA wave after a rewire that warrants a recompute. Mirrors `try_run`'s
    /// pause + gate + pending guards, then injects the phase-1 DIRTY the added dep's
    /// [START,DATA] handshake did not carry (R-dirty-before-data).
    fn settle_rewire(&self) {
        if matches!(self.0.borrow().pausable, Pausable::True) && self.is_paused() {
            self.0.borrow_mut().paused_dep_wave_occurred = true;
            return;
        }
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
        if !(has_called || partial || all_settled) {
            return; // first-run gate still holds
        }
        self.mark_dirty(); // phase 1 (no-op if already dirty, e.g. removeDep auto-settle)
        self.run_wave(); // phase 2: fn → DATA / undirty RESOLVED
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
                        terminal: n.dep_terminal[i].clone(),
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
                      // EXEMPT async-pool nodes: an async fn that returns WITHOUT emitting has DEFERRED
                      // its result (it emits later via a stashed DeferredCtx), NOT rejected — synthesizing
                      // an undirty RESOLVED here would prematurely settle a still-pending diamond leg
                      // (R-async-paused / C-4). The eventual deferred emit carries its own DIRTY balance.
        let undirty = {
            let n = self.0.borrow();
            // EXEMPT a TERMINAL wave (the fn emitted COMPLETE/ERROR): the terminal IS the settle,
            // and R-terminal-settles-dirty releases the downstream dirty — synthesizing an undirty
            // RESOLVED here would overwrite the terminal status + emit a spurious post-terminal
            // RESOLVED (mirrors the TS `_terminal === undefined` guard, node.ts). Pre-existing
            // parity gap surfaced by C-11 (a fn emitting a bare COMPLETE).
            n.emitted_dirty_this_wave
                && !n.emitted_tier3_this_wave
                && !n.terminal
                && !Self::inner_is_async(&n)
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

        // R-pause-modes / R-async-paused (D44): while paused, defer the tier-3/4 settle
        // slice (DATA/RESOLVED/INVALIDATE) into the pause buffer; tier<3 (DIRTY/PAUSE/
        // RESUME) and tier-5/6 (terminal/TEARDOWN) bypass so control + end-of-stream
        // always reach observers. Replayed in arrival order on final-lock RESUME
        // (on_resume). MUST run before the tier-3 counting + DIRTY synthesis below so a
        // buffered wave emits nothing while paused (matches the TS ordering).
        if self.should_buffer_on_pause() {
            let (buffered, rest): (Vec<Msg>, Vec<Msg>) = sorted
                .into_iter()
                .partition(|m| matches!(m.tier().as_u8(), 3 | 4));
            if !buffered.is_empty() {
                let mut n = self.0.borrow_mut();
                // A buffered tier-3 IS a produced settle — mark emitted_tier3 so run_wave
                // does NOT synthesize a spurious undirty RESOLVED for a fn whose DATA was
                // merely deferred into the buffer (per-language correctness, D24: TS sets
                // this flag only in the post-buffer emit loop, leaving a resumeAll recompute
                // prone to that spurious RESOLVED; recognizing the settle at buffer time is
                // the more-correct reading of R-resolved-undirty).
                n.emitted_tier3_this_wave = true;
                n.pause_buffer.push(buffered);
            }
            sorted = rest;
            if sorted.is_empty() {
                return;
            }
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

    /// External deferred emit — the async-pool late-emit path used by
    /// [`crate::ctx::DeferredCtx`] (R-sync-core: async lives in the fn body, the emit
    /// serializes back onto the single thread). Establishes a FRESH wave-owner boundary
    /// (like [`Node::down`]) so a panic in the cascade becomes `[[ERROR,e]]` (D30) and the
    /// leading DIRTY is synthesized (`inside_run_wave` is false here). Nested under a live
    /// wave-owner (e.g. a deferred emit fired during a RESUME cascade) it just runs.
    pub(crate) fn owned_down(&self, msgs: Wave<AnyValue>) {
        let core = self.clone();
        with_wave_owner(self, move || core.down(msgs), || {});
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
    /// this node's own lockset (R-pause-lockset). For the other up-allowed kinds
    /// (INVALIDATE/DIRTY/TEARDOWN) a node is either the TERMINUS (depless source) or a
    /// pass-through INTERMEDIATE (R-up-at-source / D38, C-7):
    /// - depless source = terminus: INVALIDATE → HONOR (route through `down` → clear
    ///   cache to SENTINEL, fire onInvalidate, broadcast INVALIDATE downstream); routing
    ///   through `down` (not a direct `invalidate()`) keeps it batch/pause-consistent with
    ///   a downstream-originated INVALIDATE. DIRTY/TEARDOWN → DROP (no coherent terminus
    ///   action: self-DIRTY would wedge downstream awaiting a settle that never comes;
    ///   source lifecycle is source/graph-owned).
    /// - dep-bearing intermediate: forward toward deps only, never self-act (the source
    ///   is the single actor, the down-cascade is the effect).
    pub(crate) fn up(&self, msgs: Vec<Msg>) {
        for m in &msgs {
            assert!(
                m.is_up_allowed(),
                "ctx.up: {m:?} is down-only; up carries control tiers only (R-ctx-up)"
            );
        }
        let is_depless = self.0.borrow().deps.is_empty();
        for m in msgs {
            match m {
                Message::Pause(lock) => self.pause_acquire(lock),
                Message::Resume(lock) => self.pause_release(lock),
                // R-up-at-source (D38): INVALIDATE/DIRTY/TEARDOWN.
                other if is_depless => {
                    // terminus: honor INVALIDATE, drop DIRTY/TEARDOWN.
                    if matches!(other, Message::Invalidate) {
                        self.down(vec![Message::Invalidate]);
                    }
                }
                other => {
                    // dep-bearing intermediate: forward toward deps (reconstruct the
                    // payload-free control kind — Msg is not Clone, but only the unit
                    // control kinds reach here per is_up_allowed minus PAUSE/RESUME).
                    let deps = self.0.borrow().deps.clone();
                    for dep in &deps {
                        dep.up(vec![dup_control(&other)]);
                    }
                }
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
        // terminal-is-forever: a node that terminated while paused discards its buffer
        // and never replays/recomputes (BH3).
        if self.0.borrow().terminal {
            let mut n = self.0.borrow_mut();
            n.paused_dep_wave_occurred = false;
            n.pause_buffer.clear();
            return;
        }
        // Drain buffered settle slices (resumeAll / async-at-paused, R-async-paused). The
        // lockset is already empty (pause_release removed the final lock before calling
        // us), so each replayed wave emits normally — DIRTY synth + DATA, no longer
        // buffered. Drain BEFORE the default-mode recompute (matches the TS arm order).
        let buf = {
            let mut n = self.0.borrow_mut();
            std::mem::take(&mut n.pause_buffer)
        };
        for wave in buf {
            self.down(wave);
        }
        // Default ("true") mode: a dep wave skipped while paused fires the fn once now,
        // with the latest dep values.
        let fire = {
            let mut n = self.0.borrow_mut();
            std::mem::take(&mut n.paused_dep_wave_occurred)
        };
        if fire {
            self.try_run();
        }
    }

    /// Async-pool check that takes an existing borrow (avoids a re-borrow when the caller
    /// already holds `NodeInner`). The pool kind is a dispatcher property of the handle.
    fn inner_is_async(n: &NodeInner) -> bool {
        match n.handle {
            Some(h) => n.dispatcher.pool_kind(h.pool_id) == PoolKind::Async,
            None => false,
        }
    }

    /// Should an outgoing settle slice be deferred into the pause buffer? `pausable` mode
    /// is the OUTER gate over R-async-paused buffering (D44).
    fn should_buffer_on_pause(&self) -> bool {
        let n = self.0.borrow();
        match n.pausable {
            // false: ignore PAUSE/RESUME ENTIRELY — never buffer, keep producing (B20).
            Pausable::False => false,
            _ if n.pause_lockset.is_empty() => false,
            // resumeAll: production-gating — buffer the own (sync/async) settle slice too.
            Pausable::ResumeAll => true,
            // true (default): PAUSE gates recomputation/propagation, NOT a leaf source's
            // own production. An async COMPUTE node's (deps>0) in-flight result buffers
            // (C-2); a depless async leaf source delivers immediately (C-10). The
            // leaf-vs-compute discriminator is deps.is_empty().
            Pausable::True => !n.inside_run_wave && Self::inner_is_async(&n) && !n.deps.is_empty(),
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
            // `dep_terminal` is intentionally NOT reset here: it is a PERSISTED cross-wave fact
            // (the dep really did COMPLETE/ERROR — terminal-is-forever, D17), not a wave-transient
            // like `dep_dirty`/`dep_batch`. `deactivate`/`rewire_apply` refresh it on a fresh
            // lifecycle; a panic-aborted wave must leave a terminated dep marked terminal.
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

/// Reconstruct a payload-free upstream control message for forwarding toward deps
/// (R-up-at-source / D38). [`Msg`] is not `Clone` (the `Error` variant carries a
/// non-clonable `GraphError`), but the only kinds that reach the intermediate-forward
/// path are the unit control kinds DIRTY/INVALIDATE/TEARDOWN (per `is_up_allowed`, minus
/// PAUSE/RESUME which self-handle), so a fresh copy is always constructible.
fn dup_control(m: &Msg) -> Msg {
    match m {
        Message::Dirty => Message::Dirty,
        Message::Invalidate => Message::Invalidate,
        Message::Teardown => Message::Teardown,
        other => unreachable!("up forwards only DIRTY/INVALIDATE/TEARDOWN, got {other:?}"),
    }
}

/// Is `target` reachable upstream from `from` (following deps)? The rewire cycle-
/// prevention DFS (R-rewire / D42). Node identity = `Rc::ptr_eq` on the inner.
fn reachable_upstream(from: &Core, target: &Core) -> bool {
    let mut seen: Vec<Core> = Vec::new();
    let mut stack: Vec<Core> = vec![from.clone()];
    while let Some(n) = stack.pop() {
        if Rc::ptr_eq(&n.0, &target.0) {
            return true;
        }
        if seen.iter().any(|s| Rc::ptr_eq(&s.0, &n.0)) {
            continue;
        }
        seen.push(n.clone());
        for d in n.0.borrow().deps.iter() {
            stack.push(d.clone());
        }
    }
    false
}

/// Dedup a dep list by node identity (`Rc::ptr_eq`), preserving first-seen order — a dep
/// appearing twice in a rewire collapses to one (Option-C; matches the TS `_dedupDeps`).
fn dedup_cores(deps: Vec<Core>) -> Vec<Core> {
    let mut out: Vec<Core> = Vec::with_capacity(deps.len());
    for d in deps {
        if !out.iter().any(|o| Rc::ptr_eq(&o.0, &d.0)) {
            out.push(d);
        }
    }
    out
}

/// Register a fn into the pool selected by `pool` (R-dispatch-all). The async pool's
/// invoke is still sync void (R-sync-core); the pool kind only labels the node's
/// deferred-emit / pause-buffering behavior.
fn register_with(disp: &Dispatcher, pool: PoolKind, f: NodeFn) -> Handle {
    match pool {
        PoolKind::Sync => disp.register(f),
        PoolKind::Async => disp.register_async(f),
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
            core: Core::new(
                vec![],
                None,
                default_dispatcher(),
                Some(Rc::new(initial)),
                Pausable::True,
            ),
            _t: PhantomData,
        }
    }

    /// A SENTINEL state node — no value until the first [`Node::set`].
    pub fn state_empty() -> Node<T> {
        Node {
            core: Core::new(vec![], None, default_dispatcher(), None, Pausable::True),
            _t: PhantomData,
        }
    }

    /// A producer node (fn, no deps): runs once on activation, emits via `ctx.emit`.
    pub fn producer<F: Fn(&Ctx) + 'static>(f: F) -> Node<T> {
        Self::producer_opts(NodeOpts::default(), f)
    }

    /// A producer on the LocalAsync pool (D20): the fn runs once on activation and may
    /// DEFER its emission — stash `ctx.defer()` and emit later via the [`DeferredCtx`]
    /// (the async source pattern, R-sync-core / R-no-raw-async). Default pause mode
    /// (`true`); a depless leaf source's own production is delivered immediately even
    /// while paused (R-pause-modes / C-10).
    ///
    /// [`DeferredCtx`]: crate::ctx::DeferredCtx
    pub fn producer_async<F: Fn(&Ctx) + 'static>(f: F) -> Node<T> {
        Self::producer_opts(
            NodeOpts {
                pool: PoolKind::Async,
                pausable: Pausable::True,
                ..NodeOpts::default()
            },
            f,
        )
    }

    /// A producer with explicit [`NodeOpts`] (pool + pause mode).
    pub fn producer_opts<F: Fn(&Ctx) + 'static>(opts: NodeOpts, f: F) -> Node<T> {
        let disp = default_dispatcher();
        let handle = register_with(&disp, opts.pool, Rc::new(f));
        Node {
            core: Core::new(vec![], Some(handle), disp, None, opts.pausable),
            _t: PhantomData,
        }
    }

    /// A derived node over erased deps. The fn reads deps positionally via
    /// `ctx.data::<U>(i)` and emits via `ctx.emit`. The first-run gate holds the fn
    /// until every dep has settled (R-first-run-gate). A fn that returns WITHOUT
    /// emitting (filter-reject) makes the substrate synthesize an undirty RESOLVED
    /// (D49 / R-resolved-undirty).
    pub fn derived<F: Fn(&Ctx) + 'static>(deps: Vec<Core>, f: F) -> Node<T> {
        Self::derived_opts(deps, NodeOpts::default(), f)
    }

    /// A derived node on the LocalAsync pool (D20): the fn may DEFER its emission (stash
    /// `ctx.defer()`, emit later). An async COMPUTE node (deps>0) that returns without
    /// emitting has DEFERRED (not rejected) — no undirty RESOLVED is synthesized, and its
    /// in-flight result buffers if the node is paused (R-async-paused / C-2/C-4). Default
    /// pause mode (`true`).
    pub fn derived_async<F: Fn(&Ctx) + 'static>(deps: Vec<Core>, f: F) -> Node<T> {
        Self::derived_opts(
            deps,
            NodeOpts {
                pool: PoolKind::Async,
                pausable: Pausable::True,
                ..NodeOpts::default()
            },
            f,
        )
    }

    /// A derived node with explicit [`NodeOpts`] (pool + pause mode + dep-terminal policy).
    pub fn derived_opts<F: Fn(&Ctx) + 'static>(deps: Vec<Core>, opts: NodeOpts, f: F) -> Node<T> {
        let disp = default_dispatcher();
        let handle = register_with(&disp, opts.pool, Rc::new(f));
        let node = Node {
            core: Core::new(deps, Some(handle), disp, None, opts.pausable),
            _t: PhantomData,
        };
        {
            // Thread the dep-terminal propagation policy (R-deps-terminal) into the inner —
            // `Core::new` defaults to the plain derived behavior (auto-cascade, not an input).
            let mut n = node.core.0.borrow_mut();
            n.complete_when_deps_complete = opts.complete_when_deps_complete;
            n.error_when_deps_error = opts.error_when_deps_error;
            n.terminal_as_real_input = opts.terminal_as_real_input;
        }
        node
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

    /// R-rewire (D42): replace this node's deps atomically (surgical Option-C). Kept deps
    /// keep their subscription + per-dep state; only removed deps unsubscribe + drain, only
    /// added deps fresh-subscribe (push-on-subscribe for an added cached dep). The first-run
    /// gate + cache are PRESERVED. Requires an explicit fn (SD-1 fn-deps pairing — user fns
    /// read deps positionally). INTRA-graph only (D22). Deps are erased ([`Core`]).
    ///
    /// Rejects (R-rewire): self-dep, a cycle, a terminal `self`, a (non-resubscribable)
    /// terminal added dep, a reentrant rewire — these panic (propagate to the caller). A
    /// mid-fn rewire (during this node's own fn run) is the D37 feedback cycle → caught by
    /// the running wave-owner as `[[ERROR,e]]`, not a propagated panic.
    pub fn set_deps<F: Fn(&Ctx) + 'static>(&self, new_deps: Vec<Core>, f: F) {
        self.core.rewire(new_deps, Rc::new(f));
    }

    /// Add one dep (special case of [`Node::set_deps`]); returns its index. fn required (SD-1).
    pub fn add_dep<F: Fn(&Ctx) + 'static>(&self, dep: Core, f: F) -> usize {
        let mut next = self.core.0.borrow().deps.clone();
        if !next.iter().any(|d| Rc::ptr_eq(&d.0, &dep.0)) {
            next.push(dep.clone());
        }
        self.core.rewire(next, Rc::new(f));
        self.core
            .0
            .borrow()
            .deps
            .iter()
            .position(|d| Rc::ptr_eq(&d.0, &dep.0))
            .expect("added dep present after rewire")
    }

    /// Remove one dep (special case of [`Node::set_deps`]); idempotent if absent (the fn
    /// swap still applies). fn required (SD-1).
    pub fn remove_dep<F: Fn(&Ctx) + 'static>(&self, dep: Core, f: F) {
        let next: Vec<Core> = self
            .core
            .0
            .borrow()
            .deps
            .iter()
            .filter(|d| !Rc::ptr_eq(&d.0, &dep.0))
            .cloned()
            .collect();
        self.core.rewire(next, Rc::new(f));
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

    #[test]
    fn resumeall_buffers_each_recompute_and_replays_all_on_resume() {
        // R-pause-modes resumeAll (D44): unlike `true` (coalesce → fire ONCE on resume),
        // resumeAll RUNS the fn on each dep wave while paused and BUFFERS the output,
        // replaying every buffered settle in arrival order on final-lock RESUME. No
        // conformance scenario covers resumeAll (TS is also only B9-partial — the pre-pause
        // baseline-equals fidelity is deferred); this pins the buffer+replay property.
        let runs = Rc::new(Cell::new(0usize));
        let src = Node::<i32>::state(1);
        let doubled = {
            let r = runs.clone();
            Node::<i32>::derived_opts(
                vec![src.erased()],
                NodeOpts {
                    pool: PoolKind::Sync,
                    pausable: Pausable::ResumeAll,
                    ..NodeOpts::default()
                },
                move |ctx| {
                    r.set(r.get() + 1);
                    ctx.emit(*ctx.data::<i32>(0).unwrap() * 2);
                },
            )
        };
        let (log, sink) = recorder();
        let _u = doubled.subscribe(sink); // activation (not paused): 1*2 = 2, delivered
        assert_eq!(doubled.cache(), Some(2));
        runs.set(0);

        let l = LockId::new("p");
        doubled.up(vec![Message::Pause(l.clone())]);
        log.borrow_mut().clear();

        src.set(5); // recompute (10) while paused → fn RUNS, output buffered
        src.set(6); // recompute (12) → buffered
        src.set(7); // recompute (14) → buffered
        let data_while_paused = log.borrow().iter().filter(|k| *k == "DATA").count();
        assert_eq!(
            runs.get(),
            3,
            "resumeAll runs the fn on each dep wave (NOT coalesced to one like `true`)"
        );
        assert_eq!(
            data_while_paused, 0,
            "no DATA delivered while paused (output buffered)"
        );
        assert_eq!(doubled.cache(), Some(2), "cache not advanced while paused");

        doubled.up(vec![Message::Resume(l)]); // replay all three buffered settles in order
        let data_after_resume = log.borrow().iter().filter(|k| *k == "DATA").count();
        assert_eq!(
            data_after_resume, 3,
            "all three buffered settles replay on final-lock RESUME (arrival order)"
        );
        assert_eq!(doubled.cache(), Some(14)); // last replayed value (7 * 2)
    }

    // ── rewire (R-rewire / D42, C-8) ──

    #[test]
    fn rewire_adddep_sentinel_dep_does_not_rearm_gate() {
        // Q2: adding a never-emitted (SENTINEL) dep delivers START only — no recompute — and
        // does NOT re-arm the first-run gate (a alone re-drives d, not waiting for b).
        let a = Node::<i32>::state(1);
        let b = Node::<i32>::state_empty(); // SENTINEL, never emits
        let runs = Rc::new(Cell::new(0usize));
        let d = {
            let r = runs.clone();
            Node::<i32>::derived(vec![a.erased()], move |ctx| {
                r.set(r.get() + 1);
                ctx.emit(*ctx.data::<i32>(0).unwrap() * 10);
            })
        };
        let (_log, sink) = recorder();
        let _u = d.subscribe(sink);
        assert_eq!(d.cache(), Some(10));
        runs.set(0);

        let r2 = runs.clone();
        d.add_dep(b.erased(), move |ctx| {
            r2.set(r2.get() + 1);
            let bv = ctx.data::<i32>(1).map(|v| *v).unwrap_or(0); // SENTINEL guard
            ctx.emit(*ctx.data::<i32>(0).unwrap() * 10 + bv);
        });
        assert_eq!(
            runs.get(),
            0,
            "a SENTINEL added dep delivers START only — no recompute"
        );

        a.set(2); // gate NOT re-armed: a alone re-drives d
        assert_eq!(runs.get(), 1);
        assert_eq!(d.cache(), Some(20)); // 2*10 + 0 (b still SENTINEL)
    }

    #[test]
    fn rewire_removedep_drains_and_preserves_cache() {
        // Q3/Q7: removeDep of a non-dirty dep preserves cache (no recompute) + drains the
        // removed edge (it no longer drives the node); the swapped fn runs on the next wave.
        let a = Node::<i32>::state(1);
        let b = Node::<i32>::state(2);
        let d: Node<i32> = Node::derived(vec![a.erased(), b.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap() + *ctx.data::<i32>(1).unwrap())
        });
        let (_log, sink) = recorder();
        let _u = d.subscribe(sink);
        assert_eq!(d.cache(), Some(3));

        d.remove_dep(b.erased(), |ctx| ctx.emit(*ctx.data::<i32>(0).unwrap()));
        assert_eq!(d.cache(), Some(3)); // preserved (removeDep of a non-dirty dep: no recompute)

        b.set(99); // drained — must NOT drive d
        assert_eq!(d.cache(), Some(3));

        a.set(5); // d recomputes with the swapped a-only fn
        assert_eq!(d.cache(), Some(5));
    }

    #[test]
    fn rewire_removedep_to_zero_deps_is_inert() {
        // SD-3: removeDep to zero deps → degenerate fn-no-deps, cache preserved, no auto-fire.
        let a = Node::<i32>::state(7);
        let runs = Rc::new(Cell::new(0usize));
        let d = {
            let r = runs.clone();
            Node::<i32>::derived(vec![a.erased()], move |ctx| {
                r.set(r.get() + 1);
                ctx.emit(*ctx.data::<i32>(0).unwrap());
            })
        };
        let (_log, sink) = recorder();
        let _u = d.subscribe(sink);
        assert_eq!(d.cache(), Some(7));
        runs.set(0);

        let r2 = runs.clone();
        d.remove_dep(a.erased(), move |ctx| {
            r2.set(r2.get() + 1);
            ctx.emit(-1i32);
        });
        assert_eq!(d.cache(), Some(7)); // preserved
        assert_eq!(runs.get(), 0); // inert — does not fire

        a.set(8); // a is no longer a dep
        assert_eq!(d.cache(), Some(7));
        assert_eq!(runs.get(), 0);
    }

    #[test]
    fn rewire_setdeps_reorders_kept_deps() {
        // Option-C / DepRecord-ref dispatch: reorder kept deps without losing state; a kept
        // dep's callback reroutes to its new index via the shared idx-box (O(1)).
        fn combine(ctx: &Ctx) {
            ctx.emit(*ctx.data::<i32>(0).unwrap() * 100 + *ctx.data::<i32>(1).unwrap());
        }
        let a = Node::<i32>::state(10);
        let b = Node::<i32>::state(20);
        let d: Node<i32> = Node::derived(vec![a.erased(), b.erased()], combine);
        let (_log, sink) = recorder();
        let _u = d.subscribe(sink);
        assert_eq!(d.cache(), Some(1020)); // dep0=a=10, dep1=b=20

        d.set_deps(vec![b.erased(), a.erased()], combine); // reorder; kept state preserved
        a.set(11); // a is now dep1; reroutes correctly
        assert_eq!(d.cache(), Some(2011)); // dep0=b=20, dep1=a=11 → 20*100+11
    }

    #[test]
    fn rewire_atomic_multi_add_settles_once() {
        // P2: adding ≥2 cached deps in ONE setDeps settles ATOMICALLY — the fn fires once,
        // never on a partial view (an added dep still SENTINEL at invocation = the bug).
        let a = Node::<i32>::state(1);
        let b = Node::<i32>::state(10);
        let c = Node::<i32>::state(100);
        let runs = Rc::new(Cell::new(0usize));
        let saw_partial = Rc::new(Cell::new(false));
        let d: Node<i32> = Node::derived(vec![a.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap())
        });
        let (_log, sink) = recorder();
        let _u = d.subscribe(sink);
        runs.set(0);

        let r = runs.clone();
        let sp = saw_partial.clone();
        d.set_deps(vec![a.erased(), b.erased(), c.erased()], move |ctx| {
            r.set(r.get() + 1);
            if ctx.data::<i32>(1).is_none() || ctx.data::<i32>(2).is_none() {
                sp.set(true);
            }
            ctx.emit(
                *ctx.data::<i32>(0).unwrap()
                    + *ctx.data::<i32>(1).unwrap()
                    + *ctx.data::<i32>(2).unwrap(),
            );
        });
        assert_eq!(
            runs.get(),
            1,
            "ONE atomic settle, not one fire per added dep"
        );
        assert!(
            !saw_partial.get(),
            "never fired with an added dep still SENTINEL"
        );
        assert_eq!(d.cache(), Some(111)); // 1 + 10 + 100
    }

    #[test]
    fn rewire_settle_emits_dirty_before_data() {
        // D1 / R-dirty-before-data: a rewire-triggered settle is a wave — DIRTY precedes DATA.
        let a = Node::<i32>::state(1);
        let b = Node::<i32>::state(100);
        let d: Node<i32> = Node::derived(vec![a.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap())
        });
        let (log, sink) = recorder();
        let _u = d.subscribe(sink);
        log.borrow_mut().clear(); // isolate the rewire wave

        d.add_dep(b.erased(), |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap() + *ctx.data::<i32>(1).unwrap())
        });
        assert_eq!(*log.borrow(), vec!["DIRTY", "DATA"]); // glitch-free two-phase
        assert_eq!(d.cache(), Some(101));
    }

    #[test]
    #[should_panic(expected = "self-dependency")]
    fn rewire_rejects_self_dep() {
        let a = Node::<i32>::state(1);
        let d: Node<i32> = Node::derived(vec![a.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap())
        });
        let _u = d.subscribe(|_| {});
        d.set_deps(vec![d.erased()], |_| {}); // self-dep → panic
    }

    #[test]
    #[should_panic(expected = "cycle")]
    fn rewire_rejects_cycle() {
        let a = Node::<i32>::state(1);
        let d: Node<i32> = Node::derived(vec![a.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap())
        });
        let e: Node<i32> = Node::derived(vec![d.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap())
        });
        let _u = e.subscribe(|_| {}); // e depends on d (→ a)
        d.add_dep(e.erased(), |ctx| ctx.emit(*ctx.data::<i32>(0).unwrap())); // would close d→e→d
    }

    #[test]
    #[should_panic(expected = "terminal dep")]
    fn rewire_rejects_adding_terminal_dep() {
        let term: Node<i32> = Node::producer(|ctx| ctx.down(vec![Message::Complete]));
        let _ut = term.subscribe(|_| {}); // activate → producer completes
        assert_eq!(term.status(), Status::Completed);
        let a = Node::<i32>::state(1);
        let d: Node<i32> = Node::derived(vec![a.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap())
        });
        let _u = d.subscribe(|_| {});
        d.add_dep(term.erased(), |ctx| ctx.emit(*ctx.data::<i32>(0).unwrap())); // terminal dep → panic
    }

    #[test]
    fn rewire_midfn_becomes_error_not_panic() {
        // A fn mutating its OWN deps mid-wave is the D37 feedback cycle. The Rust substrate
        // bundles the D30 catch at the wave-owner, so this surfaces as [[ERROR,e]] on the node
        // (R-reentrancy), NOT a propagated panic (the externally-called rejects DO panic).
        let a = Node::<i32>::state(1);
        let x = Node::<i32>::state(9);
        let slot: Rc<RefCell<Option<Node<i32>>>> = Rc::new(RefCell::new(None));
        let slot2 = slot.clone();
        let d: Node<i32> = Node::derived(vec![a.erased()], move |ctx| {
            if let Some(dh) = slot2.borrow().as_ref() {
                // mid-fn self-rewire — illegal (R-rewire / D37)
                dh.add_dep(x.erased(), |c| c.emit(*c.data::<i32>(0).unwrap()));
            }
            ctx.emit(*ctx.data::<i32>(0).unwrap());
        });
        *slot.borrow_mut() = Some(d.clone());
        let (log, sink) = recorder();
        let _u = d.subscribe(sink); // activate → fn → mid-fn add_dep → reject → wave-owner → ERROR
        assert_eq!(d.status(), Status::Errored);
        assert!(
            log.borrow().iter().any(|k| k == "ERROR"),
            "mid-fn rewire surfaces as ERROR (got {:?})",
            *log.borrow()
        );
    }

    #[test]
    fn rewire_fnswap_frees_old_fn_handle() {
        // B32 (rewire fn-swap GC): a rewire re-registers the fn → a new handle and
        // unregisters the OLD handle, freeing the rewired-away closure + its captures.
        struct DropFlag(Rc<Cell<bool>>);
        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }
        let old_fn_dropped = Rc::new(Cell::new(false));
        let a = Node::<i32>::state(1);
        let guard = DropFlag(old_fn_dropped.clone());
        let d: Node<i32> = Node::derived(vec![a.erased()], move |ctx| {
            let _hold = &guard; // the OLD fn captures the guard
            ctx.emit(*ctx.data::<i32>(0).unwrap());
        });
        let _u = d.subscribe(|_| {});
        assert!(!old_fn_dropped.get(), "old fn is live before the swap");

        d.set_deps(vec![a.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap())
        }); // fn swap
        assert!(
            old_fn_dropped.get(),
            "the rewired-away fn (and its capture) is freed on swap (B32)"
        );
    }

    #[test]
    fn rewire_panic_in_added_dep_activation_does_not_wedge_node() {
        // QA-F1 regression: adding a dep whose ACTIVATION fn panics. The wave-owner catch
        // blames the added producer (its fn ran) → ERROR lands on IT. Without the
        // DepMutationGuard, `d` would be left in_dep_mutation=true forever → every future
        // maybe_run defers (never recomputes) + every future rewire is rejected as reentrant.
        // With the guard `d` recovers cleanly.
        //
        // `d` ABSORBS a dep error (errorWhenDepsError:false) so this test isolates the QA-F1
        // GUARD concern (in_dep_mutation cleared on the unwind, observed via the recompute
        // below) from the orthogonal R-deps-terminal auto-error-cascade (C-15): under the
        // default errorWhenDepsError:true, boom's activation-panic→ERROR would correctly
        // cascade and TERMINATE d, making "d recovers" un-observable. The guard fires on the
        // unwind identically either way; absorbing just keeps d alive to prove it.
        let a = Node::<i32>::state(1);
        let d: Node<i32> = Node::derived_opts(
            vec![a.erased()],
            NodeOpts {
                error_when_deps_error: false,
                ..NodeOpts::default()
            },
            |ctx| ctx.emit(*ctx.data::<i32>(0).unwrap()),
        );
        let (_log, sink) = recorder();
        let _u = d.subscribe(sink);
        assert_eq!(d.cache(), Some(1));

        // boom: a producer whose activation fn panics — added as a dep of d.
        let boom = Node::<i32>::producer(|_ctx| panic!("boom on activation"));
        d.add_dep(boom.erased(), |ctx| ctx.emit(*ctx.data::<i32>(0).unwrap()));

        // d ABSORBED boom's error → d is NOT terminal; the QA-F1 guard cleared
        // in_dep_mutation on the unwind so d is not wedged.
        assert_ne!(
            d.status(),
            Status::Errored,
            "d absorbs the dep error (errorWhenDepsError:false) and survives the rewire panic"
        );

        // NOT WEDGED: d still recomputes from its live dep a (in_dep_mutation was cleared by
        // the guard on the unwind). Without the guard this stays Some(1) (deferred forever).
        a.set(5);
        assert_eq!(
            d.cache(),
            Some(5),
            "d recovers and recomputes — a stuck in_dep_mutation would have deferred this"
        );

        // And a follow-up rewire is NOT rejected as reentrant (the flag is clear).
        d.set_deps(vec![a.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap() * 2)
        });
        a.set(3);
        assert_eq!(
            d.cache(),
            Some(6),
            "a later rewire works (not 'reentrant'-rejected)"
        );
    }
}
