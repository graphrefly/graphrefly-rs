//! Producer-shape operator substrate (Slice D-ops, Commit 2).
//!
//! Producer ops (zip / concat / race / takeUntil) are nodes with no
//! declared deps that fire their fn ONCE on first activation. The fn
//! body subscribes to upstream sources via [`ProducerCtx::subscribe_to`]
//! and registers per-op state (queues, phase flags, winner index). When
//! upstream emits, the operator's sink closures re-enter Core via
//! `Core::emit` / `Core::complete` / `Core::error` on the producer node.
//!
//! On last-subscriber unsubscribe, Core invokes
//! [`BindingBoundary::producer_deactivate(node_id)`](graphrefly_core::BindingBoundary::producer_deactivate);
//! the binding's impl drops the per-node entry from its
//! `producer_states` map, which cascades:
//!
//! ```text
//! producer_states.remove(node_id)  →
//!   Vec<Subscription> drops          →
//!     each Subscription::Drop fires  →
//!       upstream sinks unsubscribe.
//! ```
//!
//! # Reference-cycle discipline (Slice Y, 2026-05-08)
//!
//! Build closures registered via
//! [`ProducerBinding::register_producer_build`] are stored long-term in
//! the binding's `producer_builds` registry. To avoid the strong-Arc
//! cycle `BenchBinding → registry → producer_builds[fn_id] → closure →
//! strong-Arc<dyn ProducerBinding> → BenchBinding`, factory bodies
//! (`zip` / `concat` / `race` / `take_until` in `ops_impl.rs` plus
//! `switch_map` / `exhaust_map` / `merge_map` / `concat_map` in
//! `higher_order.rs`) capture `WeakCore` and
//! `Weak<dyn ProducerBinding>` (and `Weak<dyn HigherOrderBinding>`
//! for the higher-order factories). The build closure upgrades both
//! on each invocation; if the host `Core` was already dropped, upgrade
//! returns `None` and the build closure no-ops cleanly.
//!
//! Sinks spawned by the build closure capture STRONG refs cloned from
//! the upgraded weaks. Their lifetime is tied to the producer's active
//! subscription — `producer_deactivate` on last-subscriber unsubscribe
//! clears `producer_storage[node_id]`, dropping the upstream
//! `Subscription`s, which drops the sinks, which drops the strong
//! captures. So the strong-ref window is bounded by producer-active
//! state, not by the long-lived `producer_builds` registry.

use std::any::Any;
use std::sync::Arc;

use ahash::AHashMap as HashMap;
use parking_lot::Mutex;

use graphrefly_core::{
    BindingBoundary, Core, CoreFull, FnId, HandleId, NodeId, Sink, SubscriptionId,
};

/// Outcome of [`ProducerCtx::subscribe_to`] — the producer-layer
/// translation of [`graphrefly_core::SubscribeError`] into a positive
/// outcome enum that operators (zip / concat / race / take_until /
/// merge_map / switch_map / exhaust_map / concat_map) can match on for
/// per-operator dead-source semantics.
///
/// Introduced /qa F2 (2026-05-10) to close the silent-wedge class of
/// bugs where operators previously couldn't tell that a `subscribe_to`
/// call had been rejected per R2.2.7.b (non-resubscribable terminal
/// source) — pre-F2 the rejection was logged-and-skipped silently,
/// which left zip waiting for a queue that would never fill, concat
/// stuck on a source that would never advance, etc.
///
/// Mirrors the per-domain status-string-union pattern used in TS
/// (`RefineStatus`, `AgentStatus`, process status: `"running" |
/// "completed" | "errored" | "cancelled"`) — each operator-layer
/// outcome lives in its own typed enum rather than sharing a global
/// `Outcome<T, E>` type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscribeOutcome {
    /// Subscription installed successfully. The
    /// [`ProducerNodeState`] holds the [`Subscription`]; no further
    /// operator action required.
    Live,
    /// Subscription was deferred to wave-end via the
    /// [`graphrefly_core::DeferredProducerOp::Callback`] queue (Phase
    /// H+ STRICT, D115). The deferred callback installs the
    /// subscription after wave_guards release. Operators MAY treat
    /// this as `Live` for lifecycle bookkeeping — the subscription
    /// WILL be installed; just not yet.
    Deferred,
    /// The target node is non-resubscribable AND has terminated
    /// (R2.2.7.b, D118). The sink will NOT be installed. Operators
    /// MUST handle this per their semantics:
    ///
    /// - **zip / take_until (source)**: self-Complete (tuple stream
    ///   can never form; take_until's source is gone).
    /// - **concat**: advance to the next source (treat as inner
    ///   Complete signal).
    /// - **race**: mark `completed[idx] = true`; if all sources are
    ///   Dead/Complete, self-Complete.
    /// - **take_until (notifier)**: ignore (notifier signal will
    ///   never fire; take_until reduces to a passthrough of source).
    /// - **switch_map / exhaust_map / concat_map / merge_map (inner)**:
    ///   treat as immediate `on_inner_complete` — decrement active,
    ///   advance to next, check self-Complete trigger.
    Dead {
        /// The dead node that rejected the subscribe.
        node: NodeId,
    },
}

/// Build closure type — the producer's fn body, called once on first
/// activation. The closure receives a [`ProducerCtx`] for setting up
/// upstream subscriptions; emissions on the producer come from sink
/// callbacks the closure registers.
pub type ProducerBuildFn = Box<dyn Fn(ProducerCtx<'_>) + Send + Sync>;

/// Per-producer-node state owned by the [`ProducerBinding`] impl.
///
/// Holds upstream `Subscription`s (auto-dropped on producer
/// deactivation) plus an optional `Box<dyn Any>` slot for op-specific
/// state shared across the build closure and its sink closures.
/// (Most ops capture state via `Arc<Mutex<...>>` directly in closure
/// captures; the `op_state` slot is reserved for ops that prefer
/// trait-object storage.)
#[derive(Default)]
pub struct ProducerNodeState {
    /// Recorded upstream `(source_node, sub_id)` pairs taken by
    /// [`ProducerCtx::subscribe_to`]. S2b/D229: core-level RAII
    /// `Subscription` is retired — these are explicitly unsubscribed by
    /// the binding's [`BindingBoundary::producer_deactivate`] impl via
    /// the owner-supplied `unsub` closure (see
    /// [`default_producer_deactivate`]), behaviour-identical to the old
    /// `Vec<Subscription>`-drop cascade.
    pub subs: Vec<(NodeId, SubscriptionId)>,
    /// Optional op-specific scratch (rarely used; most ops capture
    /// state via closure).
    pub op_state: Option<Box<dyn Any + Send + Sync>>,
}

/// Storage shared between the [`ProducerBinding`] impl and the
/// [`ProducerCtx`] passed to build closures. Keyed by producer NodeId.
///
/// Access via `Arc<Mutex<_>>` so the binding's `producer_deactivate`
/// hook can clear an entry while build/sink closures hold their own
/// per-op state via separate Arc captures.
pub type ProducerStorage = Arc<Mutex<HashMap<NodeId, ProducerNodeState>>>;

/// Closure-registration interface for producer-shape operators —
/// extends [`BindingBoundary`] with one method that bindings shipping
/// producers must implement.
///
/// Bindings that don't ship producers (e.g., minimal test bindings)
/// don't need to implement this trait. The operator factories below
/// (`zip`, `concat`, `race`, `take_until`) require it.
pub trait ProducerBinding: BindingBoundary {
    /// Register a producer build closure. The returned [`FnId`] is
    /// passed to [`Core::register_producer`]; on first activation,
    /// Core invokes [`BindingBoundary::invoke_fn`] which the binding
    /// dispatches to the registered build closure.
    fn register_producer_build(&self, build: ProducerBuildFn) -> FnId;

    /// Access the binding's producer-state storage. Used by
    /// [`ProducerCtx::subscribe_to`] to push subscriptions into the
    /// per-node entry, and by the binding's `producer_deactivate`
    /// impl to drop the entry on last unsubscribe.
    fn producer_storage(&self) -> &ProducerStorage;
}

/// Sink-side emit handle (S2b / D231 / D232-AMEND/A′).
///
/// Producer build closures spawn long-lived `Sink`s that fire on every
/// future upstream emit — long after the build closure's `&Core`
/// (`ctx`) is gone. Under the actor model the `Core` is owned by value
/// and relocates between workers, so sinks can no longer capture a
/// cloned `Core` / `WeakCore`. Instead they capture a `ProducerEmitter`
/// (cheap `Clone`: two `Arc`s) and post `MailboxOp`s to the
/// `Core`-owned [`graphrefly_core::CoreMailbox`]; the `BatchGuard`
/// drain-to-quiescence loop applies them **in-wave** via the sync
/// `Core::{emit,complete,error}` (immediate, cascade-ordering-preserving
/// — D232-AMEND).
///
/// Method names mirror the old `Core::{emit,complete,error}_or_defer`
/// so sink bodies are unchanged: only the captured handle's
/// construction differs (`em = ctx.emitter()` instead of
/// `core_s.clone()`).
/// The **`Send + Sync` cross-thread** producer emit handle (D249/S2c).
///
/// Holds only the id-only `Arc<CoreMailbox>` post side + the binding
/// (for the `Core`-gone handle-release branch). This is what an
/// autonomous timer task (`temporal.rs`, `tokio::spawn`-ed) captures —
/// it stays `Send` so the spawned future is `Send`. It deliberately
/// has **no `defer`** (that is the `!Send` owner-side path; see
/// [`ProducerEmitter`]).
#[derive(Clone)]
pub struct MailboxEmitter {
    mailbox: Arc<graphrefly_core::CoreMailbox>,
    /// For the `Core`-gone branch only: if the owning `Core` already
    /// dropped (mailbox closed), an `Emit`/`Error` payload handle would
    /// leak — release it (mirrors `timer.rs`'s post-`false` path).
    binding: Arc<dyn BindingBoundary>,
}

impl MailboxEmitter {
    /// Post an `Emit`. If the owning `Core` is gone, release `handle`
    /// (it held a retain for the would-be payload) — no leak.
    pub fn emit_or_defer(&self, node_id: NodeId, handle: HandleId) {
        if !self.mailbox.post_emit(node_id, handle) {
            self.binding.release_handle(handle);
        }
    }

    /// Post a `Complete`. No payload handle; `Core`-gone is a no-op.
    pub fn complete_or_defer(&self, node_id: NodeId) {
        let _ = self.mailbox.post_complete(node_id);
    }

    /// Post an `Error`. If the owning `Core` is gone, release the error
    /// payload `handle` — no leak.
    pub fn error_or_defer(&self, node_id: NodeId, handle: HandleId) {
        if !self.mailbox.post_error(node_id, handle) {
            self.binding.release_handle(handle);
        }
    }

    /// Post a **`Send`** cross-thread owner-side closure (D233/D249).
    /// For an autonomous timer task (`temporal.rs` `window_time`) doing
    /// task-side topology mutation that must run owner-side in FIFO
    /// order — the closure captures only `Send` state, so it rides the
    /// `Send + Sync` `CoreMailbox`. Returns `false` iff the `Core` is
    /// gone (closure dropped unrun; release any captured handles).
    #[must_use = "a `false` return means the Core is gone and the closure was dropped unrun; release any handles it captured"]
    pub fn defer(&self, f: impl FnOnce(&dyn graphrefly_core::CoreFull) + Send + 'static) -> bool {
        self.mailbox.post_defer(Box::new(f))
    }

    /// Whether the owning `Core` has dropped (mailbox closed) — for
    /// prompt timer-task shutdown (see [`ProducerEmitter::is_core_gone`]).
    #[must_use]
    pub fn is_core_gone(&self) -> bool {
        self.mailbox.is_closed()
    }
}

/// The owner-side producer handle (D249/S2c). `MailboxEmitter` (the
/// `Send` cross-thread emit side) **plus** the owner-only `!Send`
/// `Rc<DeferQueue>` for [`Self::defer`]. Captured into owner-side
/// `!Send` producer sinks (control/higher-order dynamic-inner); the
/// `Rc` makes it `!Send`, consistent with the D248 single-owner `Sink`
/// relaxation. A timer task that needs only the cross-thread emit side
/// takes [`Self::emitter`] (a `Send` [`MailboxEmitter`]) instead.
#[derive(Clone)]
pub struct ProducerEmitter {
    emitter: MailboxEmitter,
    /// Owner-side `!Send` `Defer` queue split off `CoreMailbox`
    /// (D249/S2c).
    deferred: std::rc::Rc<graphrefly_core::DeferQueue>,
}

impl ProducerEmitter {
    /// Construct directly from any `&Core` (S2b). Used by the
    /// **binding-layer RAII** convenience (D228-A): a test harness /
    /// napi `BenchCore` that co-owns the `Core` builds a [`SubGuard`]
    /// over `core.subscribe(...)`'s returned `SubscriptionId` so drop
    /// schedules the unsubscribe — the sanctioned replacement for the
    /// retired core-level RAII `Subscription`.
    #[must_use]
    pub fn for_core(core: &Core) -> Self {
        Self {
            emitter: MailboxEmitter {
                mailbox: core.mailbox(),
                binding: core.binding(),
            },
            deferred: core.defer_queue(),
        }
    }

    /// Construct from the object-safe [`CoreFull`] facade (D246 r5 /
    /// D245). Used by [`ProducerCtx::emitter`] now that the ctx holds
    /// `&dyn CoreFull` rather than a concrete `&Core`.
    #[must_use]
    pub fn from_corefull(core: &dyn CoreFull) -> Self {
        Self {
            emitter: MailboxEmitter {
                mailbox: core.mailbox(),
                binding: core.binding(),
            },
            deferred: core.defer_queue(),
        }
    }

    /// The `Send` cross-thread emit sub-handle — for autonomous timer
    /// tasks (`temporal.rs`, `tokio::spawn`) that only emit/complete/
    /// error and must keep their spawned future `Send` (D249/S2c).
    #[must_use]
    pub fn emitter(&self) -> MailboxEmitter {
        self.emitter.clone()
    }

    /// Post an `Emit`. If the owning `Core` is gone, release `handle`
    /// (it held a retain for the would-be payload) — no leak.
    pub fn emit_or_defer(&self, node_id: NodeId, handle: HandleId) {
        self.emitter.emit_or_defer(node_id, handle);
    }

    /// Post a `Complete`. No payload handle; `Core`-gone is a no-op.
    pub fn complete_or_defer(&self, node_id: NodeId) {
        self.emitter.complete_or_defer(node_id);
    }

    /// Post an `Error`. If the owning `Core` is gone, release the error
    /// payload `handle` — no leak.
    pub fn error_or_defer(&self, node_id: NodeId, handle: HandleId) {
        self.emitter.error_or_defer(node_id, handle);
    }

    /// Post an owner-side closure (D233) given the full object-safe
    /// `Core` surface — for sinks that must perform value-returning
    /// topology mutation (windowing `create_window_node`, higher-order
    /// dynamic-inner `subscribe`). Runs **in-wave** (the drain loop
    /// holds `&Core`); the closure consumes any returned
    /// `NodeId`/`SubscriptionId` to drive its captured op-state.
    ///
    /// Returns `false` iff the owning `Core` is already gone — the
    /// closure is dropped **unrun** (running `CoreFull` on a half-dropped
    /// `Core` is unsound; user-locked QA decision A). QA F2 (2026-05-18):
    /// this now surfaces the `Core`-gone signal (was a silent
    /// `let _ = …`) so a caller whose closure captured retained
    /// `HandleId`s can release them on `false` — mirroring the
    /// `emit_or_defer` / `error_or_defer` release-on-`false` contract.
    /// The not-yet-written windowing / higher-order callers MUST honour
    /// this (release captured payload handles when it returns `false`).
    #[must_use = "a `false` return means the Core is gone and the closure was dropped unrun; release any handles it captured"]
    pub fn defer(&self, f: impl FnOnce(&dyn graphrefly_core::CoreFull) + 'static) -> bool {
        self.deferred.post(Box::new(f))
    }

    /// Whether the owning `Core` has dropped (mailbox closed). Lets a
    /// long-lived task stop promptly + release any handle it holds
    /// (preserves the old `WeakCore::upgrade() == None` promptness).
    /// NOT required for leak-safety (`*_or_defer` already releases on a
    /// closed post) — only for prompt task shutdown.
    #[must_use]
    pub fn is_core_gone(&self) -> bool {
        self.emitter.is_core_gone()
    }
}

/// Binding-layer RAII subscription handle (S2b / D225 / D234). The
/// core-level RAII `Subscription` was retired (a parameterless `Drop`
/// can't reach a relocating owned `Core`); this wrapper IS the
/// sanctioned binding-layer replacement for *substrate operators* that
/// manage an inner subscription's lifetime by ownership (higher-order
/// `switch/exhaust/merge/concat_map` inner subs). It holds a
/// `ProducerEmitter` (an `Arc<CoreMailbox>` — `Send + Sync`, `'static`,
/// NOT the `Core`), so its `Drop` legitimately posts a deferred
/// `unsubscribe` via `em.defer` (owner-side, in-wave, FIFO-ordered —
/// D234). FIFO ordering gives the correct cancel-then-resubscribe
/// semantics: a `SubGuard` dropped before a new subscribe is posted is
/// drained (unsub) before the new subscribe. A `Core`-gone post is
/// dropped unrun (subscription moot at teardown — no leak).
#[must_use = "dropping a SubGuard schedules the inner unsubscribe"]
pub struct SubGuard {
    node: NodeId,
    sub: SubscriptionId,
    em: ProducerEmitter,
}

impl SubGuard {
    /// Track `sub` (returned by `CoreFull::try_subscribe` on `node`) so
    /// dropping this guard unsubscribes it.
    #[must_use]
    pub fn new(node: NodeId, sub: SubscriptionId, em: ProducerEmitter) -> Self {
        Self { node, sub, em }
    }
}

impl Drop for SubGuard {
    fn drop(&mut self) {
        let (n, s) = (self.node, self.sub);
        // D234: post the unsubscribe owner-side, in-wave, FIFO-ordered
        // (so a cancel-before-resubscribe drains in order). Dropped
        // unrun if the Core is already gone — the sub is moot then.
        let _ = self.em.defer(move |c| c.unsubscribe(n, s));
    }
}

/// Context handed to a producer's build closure on activation.
///
/// Provides:
/// - [`Self::node_id`] / [`Self::core`] — identity + Core access for
///   sink callbacks that re-enter Core.
/// - [`Self::subscribe_to`] — subscribe to an upstream Core node;
///   the resulting `Subscription` is auto-tracked under
///   `node_id` in the binding's producer storage and dropped on
///   producer deactivation.
pub struct ProducerCtx<'a> {
    node_id: NodeId,
    core: &'a dyn CoreFull,
    storage: &'a ProducerStorage,
}

impl<'a> ProducerCtx<'a> {
    /// Construct a new context for the binding's `invoke_fn` dispatch
    /// to call build closures. Internal — bindings call this; user
    /// code receives the constructed ctx via the build closure's arg.
    ///
    /// D246 r5 / D245: takes `&dyn CoreFull` — the one object-safe Core
    /// facade Core hands the binding via
    /// [`graphrefly_core::BindingBoundary::invoke_fn_with_core`]. A
    /// concrete `&Core` unsized-coerces to `&dyn CoreFull` at the call
    /// site, so existing `&Core`-holding call sites pass it directly
    /// (`ProducerCtx::new(node, &core, &storage)`). `ProducerCtx` only
    /// needs `subscribe`/`try_subscribe`/`register_*`/`emit`/`mailbox`/
    /// `binding`/`*_or_defer` — all on `CoreFull` — so no concrete
    /// `Core` / thread-local / stored back-reference is required.
    pub fn new(node_id: NodeId, core: &'a dyn CoreFull, storage: &'a ProducerStorage) -> Self {
        Self {
            node_id,
            core,
            storage,
        }
    }

    /// The producer node's id.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// The Core dispatcher, as the object-safe [`CoreFull`] facade
    /// (D246 r5 / D245). **Build-closure-side only** — valid only for
    /// the duration of the build call (the `Core` relocates; D231).
    /// Long-lived sinks must use [`Self::emitter`] instead. Carries
    /// everything a build closure uses (`subscribe`/`try_subscribe`/
    /// `register_*`/`emit`/`binding`/`*_or_defer`) without naming the
    /// concrete cell type.
    #[must_use]
    pub fn core(&self) -> &dyn CoreFull {
        self.core
    }

    /// Sink-side emit handle (D232-AMEND/A′). Cheap-`Clone`; capture it
    /// into spawned sink closures and call
    /// `emit_or_defer`/`complete_or_defer`/`error_or_defer` exactly as
    /// the old cloned-`Core` did — ops post to the `Core`-owned mailbox
    /// and are applied in-wave by the drain-to-quiescence loop.
    #[must_use]
    pub fn emitter(&self) -> ProducerEmitter {
        ProducerEmitter::from_corefull(self.core)
    }

    /// The binding's per-producer state storage (S2b). Replaces
    /// `binding.producer_storage()` for build closures / spawned sinks
    /// that track their own upstream subscriptions or per-op state:
    /// under D231 the build closure no longer holds a
    /// `Arc<dyn ProducerBinding>` (only `ctx`'s borrowed `&Core` +
    /// `&ProducerStorage`), and a sink can't reach `ProducerBinding`
    /// either. The returned `ProducerStorage` is
    /// `Arc<Mutex<…>>` — `'static` + cheap-`Clone`, so it can be
    /// captured into long-lived sink closures (exactly how the old code
    /// captured `binding.producer_storage().clone()`).
    #[must_use]
    pub fn storage(&self) -> ProducerStorage {
        self.storage.clone()
    }

    /// Subscribe `sink` to upstream `source`. The `Subscription` is
    /// auto-tracked under the producer's `node_id`; on producer
    /// deactivation, the binding drops the storage entry, which drops
    /// the Subscription, which unsubscribes the sink.
    ///
    /// **Phase H+ STRICT (D115, 2026-05-10):** uses `try_subscribe`
    /// to attempt the subscription. On partition order violation, the
    /// subscribe is deferred to wave-end via
    /// `DeferredProducerOp::Callback`. (S2c/D248 single-owner: the
    /// per-partition `wave_owner` `ReentrantMutex`es are deleted —
    /// there is no cross-thread interleaving wave to serialize — so
    /// the deferred callback simply runs owner-side at wave-end with
    /// no lock acquisition.)
    ///
    /// **R2.2.7.b (D118, 2026-05-10):** if the upstream is
    /// non-resubscribable AND already terminated, `try_subscribe`
    /// returns `Err(SubscribeError::TornDown)`. /qa F2 (2026-05-10):
    /// the rejection is now surfaced to the caller via
    /// [`SubscribeOutcome::Dead`] so the operator can apply its
    /// per-op dead-source semantics — pre-F2 the rejection was
    /// silently swallowed, leaving operators wedged (zip waiting on a
    /// queue that would never fill, concat stuck on a source that
    /// would never advance, etc.). See [`SubscribeOutcome::Dead`] for
    /// per-operator guidance.
    pub fn subscribe_to(&self, source: NodeId, sink: Sink) -> SubscribeOutcome {
        let sink_for_defer = sink.clone();
        match self.core.try_subscribe(source, sink) {
            Ok(sub) => {
                // S2b/D229: record `(source, sub_id)` for explicit
                // owner-driven unsubscribe at `producer_deactivate`.
                self.storage
                    .lock()
                    .entry(self.node_id)
                    .or_default()
                    .subs
                    .push((source, sub));
                SubscribeOutcome::Live
            }
            Err(graphrefly_core::SubscribeError::PartitionOrderViolation(_)) => {
                // S2b (D223/D231): the old code boxed a cloned-`Core`
                // `DeferredProducerOp::Callback` only for
                // `push_deferred_producer_op` to run it *immediately*
                // (the deferred queue is a deleted D211 no-op shim — see
                // `node::push_deferred_producer_op`). `Core` is no longer
                // `Clone`; an inline retry on `self.core` is
                // behaviour-identical (the prior path was already
                // immediate). F2 /qa: still `try_subscribe` (not the
                // panicking `subscribe`) so a source that raced to
                // non-resubscribable+terminal doesn't crash the boundary.
                match self.core.try_subscribe(source, sink_for_defer) {
                    Ok(sub) => {
                        self.storage
                            .lock()
                            .entry(self.node_id)
                            .or_default()
                            .subs
                            .push((source, sub));
                    }
                    Err(graphrefly_core::SubscribeError::TornDown { .. }) => {
                        // Source became Dead during the (now-immediate)
                        // retry — silently drop, as before.
                    }
                    Err(graphrefly_core::SubscribeError::PartitionOrderViolation(_)) => {
                        // The original deferral existed to retry with no
                        // partition held; the D211 shim already made it
                        // immediate, so a second order violation here is
                        // the same substrate-invariant break the old
                        // wave-end-drain panic guarded.
                        panic!(
                            "producer-op subscribe retry: partition-order violation — \
                             substrate invariant broken (wave_guards still held)"
                        );
                    }
                }
                SubscribeOutcome::Deferred
            }
            Err(graphrefly_core::SubscribeError::TornDown { node }) => {
                SubscribeOutcome::Dead { node }
            }
        }
    }
}

/// Default helper — explicitly unsubscribe the producer's recorded
/// upstream subs, then drop its storage entry, on deactivation.
///
/// S2b/D229: core-level RAII `Subscription` is retired, so the binding's
/// [`BindingBoundary::producer_deactivate`] impl receives a
/// `Core::unsubscribe`-capable `unsub` closure (the owner-driven chain
/// passes it the `&Core` it already holds). Looping it over the recorded
/// `(source, sub_id)` pairs is behaviour-identical to the old
/// `Vec<Subscription>`-drop cascade (same deregister + Phase-G chain,
/// lock-released so re-entrant producer cascades are safe).
///
/// Ordering (QA F3, 2026-05-18 — corrected from an earlier
/// remove-AFTER comment that contradicted the code): the entry is
/// **taken out under the `storage` lock FIRST**, then the `unsub`
/// cascade runs lock-released over the moved-out `subs`. This is
/// behaviour-identical to the retired path (old code did
/// `states.remove(&node_id)` and the dropped `Vec<Subscription>`'s
/// `Drop` ran the cascade — i.e. remove-then-cascade). Because the
/// entry is already gone before any re-entrant call, a re-entrant
/// `subscribe_to(node_id, …)` *during* the cascade `or_default()`s a
/// **fresh** entry that correctly survives this deactivation (a
/// genuine re-subscription) — there is never a half-cleared entry to
/// observe. Do NOT reorder to remove-after-unsub: that *would* expose
/// the live entry to the lock-released re-entrant cascade.
pub fn default_producer_deactivate(
    storage: &ProducerStorage,
    node_id: NodeId,
    unsub: &dyn Fn(NodeId, SubscriptionId),
) {
    // Take the entry out under the lock, then unsubscribe lock-released
    // (the `unsub` closure re-enters Core; holding `storage` across it
    // would risk a binding-vs-Core lock-order inversion).
    let removed = storage.lock().remove(&node_id);
    if let Some(state) = removed {
        for (source, sub_id) in state.subs {
            unsub(source, sub_id);
        }
    }
}

// =====================================================================
// Producer-shape operators (D-ops, Slice D Commit 2)
// =====================================================================
//
// All four producer ops follow the same shape:
//
// 1. Operator factory captures `Core::clone()` + sources + per-op state
//    (Arc<Mutex<...>>) into a build closure.
// 2. `register_producer_build` returns a FnId.
// 3. `Core::register_producer(fn_id)` creates the producer node.
// 4. On first subscribe, Core fires invoke_fn → binding dispatches to
//    the build closure → ProducerCtx is constructed.
// 5. Build closure subscribes to each upstream source, providing sink
//    closures that capture per-op state and the producer's NodeId.
// 6. Sink closures process upstream emissions and emit on the producer
//    node via `core.emit` / `core.complete` / `core.error`.
// 7. On last subscriber unsubscribe, Core fires producer_deactivate →
//    binding drops storage entry → Subscription Vec drops → sinks
//    unsub from upstream.
//
// The concrete operators (`zip` / `concat` / `race` / `take_until`)
// live in [`super::ops_impl`] (sibling module) and are re-exported
// from the crate root.
