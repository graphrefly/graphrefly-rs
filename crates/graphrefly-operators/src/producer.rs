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
//! # Reference-cycle note (v1 limitation)
//!
//! The build closures registered via
//! [`ProducerBinding::register_producer_build`] capture `Core::clone()`
//! so sinks can re-enter Core. This creates a strong-Arc cycle: Core →
//! binding → build-closure → Core. In tests, drop ordering breaks the
//! cycle — `drop(runtime)` clears the binding's closure storage, which
//! drops the captured Core refs. In production bindings (napi-rs / pyo3
//! / wasm-bindgen), the host runtime owns Core's lifetime and ensures
//! cleanup at shutdown. v2 may switch to `Weak<Core>` upgrade-on-fire
//! (requires a `Core::weak_handle()` accessor); deferred to evidence
//! that the cycle causes user-visible leaks.

use std::any::Any;
use std::sync::Arc;

use ahash::AHashMap as HashMap;
use parking_lot::Mutex;

use graphrefly_core::{BindingBoundary, Core, FnId, NodeId, Sink, Subscription};

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
    pub fn subscribe_to(&self, source: NodeId, sink: Sink) {
        let sub = self.core.subscribe(source, sink);
        let mut states = self.storage.lock();
        states.entry(self.node_id).or_default().subs.push(sub);
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
