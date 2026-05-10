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

use graphrefly_core::{BindingBoundary, Core, FnId, NodeId, Sink, Subscription};

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
    /// Subscriptions to upstream sources, taken by
    /// [`ProducerCtx::subscribe_to`]. Dropped on producer deactivation.
    pub subs: Vec<Subscription>,
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
    core: &'a Core,
    storage: &'a ProducerStorage,
}

impl<'a> ProducerCtx<'a> {
    /// Construct a new context for the binding's `invoke_fn` dispatch
    /// to call build closures. Internal — bindings call this; user
    /// code receives the constructed ctx via the build closure's arg.
    pub fn new(node_id: NodeId, core: &'a Core, storage: &'a ProducerStorage) -> Self {
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

    /// The Core dispatcher. Sink closures use this to re-enter Core —
    /// `core.emit(self.node_id(), h)` to emit a value, etc.
    #[must_use]
    pub fn core(&self) -> &Core {
        self.core
    }

    /// Subscribe `sink` to upstream `source`. The `Subscription` is
    /// auto-tracked under the producer's `node_id`; on producer
    /// deactivation, the binding drops the storage entry, which drops
    /// the Subscription, which unsubscribes the sink.
    ///
    /// **Phase H+ STRICT (D115, 2026-05-10):** uses `try_subscribe`
    /// to attempt the subscription. On partition order violation, the
    /// subscribe is deferred to wave-end via
    /// `DeferredProducerOp::Callback` — the deferred callback runs
    /// after all partition wave_owners are released (no partitions
    /// held → safe to acquire any partition).
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
                self.storage
                    .lock()
                    .entry(self.node_id)
                    .or_default()
                    .subs
                    .push(sub);
                SubscribeOutcome::Live
            }
            Err(graphrefly_core::SubscribeError::PartitionOrderViolation(_)) => {
                let core = self.core.clone();
                let core_for_callback = core.clone();
                let storage = self.storage.clone();
                let node_id = self.node_id;
                // F2 /qa (2026-05-10): deferred Callback uses
                // `try_subscribe` (not the panicking `subscribe`) so a
                // source that became non-resubscribable + terminal
                // between the original defer-queue push and the
                // wave-end drain doesn't crash the binding boundary.
                // On TornDown at drain time, the producer-layer's
                // SubscribeOutcome::Dead path was never reached at
                // subscribe-time (because we deferred); we silently
                // drop the deferred sub here. Per-operator dead-source
                // semantics rely on the subscribe-time outcome — if
                // the source went terminal during the defer window,
                // the operator's other inputs / lifecycle paths must
                // handle it (e.g., zip with one Live + one
                // raced-to-Dead source would still emit until the
                // dead one Complete-cascades to the operator via
                // other state, which it doesn't here — but the
                // condition requires concurrent termination during
                // wave-end drain which is a narrow window).
                core.push_deferred_producer_op(graphrefly_core::DeferredProducerOp::Callback(
                    Box::new(move || {
                        match core_for_callback.try_subscribe(source, sink_for_defer) {
                            Ok(sub) => {
                                storage.lock().entry(node_id).or_default().subs.push(sub);
                            }
                            Err(graphrefly_core::SubscribeError::TornDown { .. }) => {
                                // Source became Dead during the defer
                                // window — silently drop (see comment
                                // above).
                            }
                            Err(graphrefly_core::SubscribeError::PartitionOrderViolation(_)) => {
                                // Should never happen: the deferred
                                // Callback drains AFTER wave_guards
                                // release, so partition acquisition
                                // can't fail. Surface as a panic if
                                // it ever does.
                                panic!(
                                    "deferred producer-op Callback: partition-order \
                                     violation at wave-end drain — substrate invariant \
                                     broken (wave_guards still held during drain)"
                                );
                            }
                        }
                    }),
                ));
                SubscribeOutcome::Deferred
            }
            Err(graphrefly_core::SubscribeError::TornDown { node }) => {
                SubscribeOutcome::Dead { node }
            }
        }
    }
}

/// Default helper — drop the producer's storage entry on
/// deactivation. Bindings can call this from their
/// [`BindingBoundary::producer_deactivate`] impl to get the standard
/// auto-cleanup behavior.
pub fn default_producer_deactivate(storage: &ProducerStorage, node_id: NodeId) {
    let mut states = storage.lock();
    states.remove(&node_id);
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
