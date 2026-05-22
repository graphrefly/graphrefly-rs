//! Core-level topology-change notification primitive.
//!
//! The topology sink receives events when nodes are registered,
//! torn down, or have their deps mutated. This is the substrate
//! for reactive `describe()` and reactive `observe()` at the
//! graph layer.
//!
//! Topology sinks are NOT nodes — they sit outside the reactive
//! graph to avoid circularity (registering an observer node
//! would itself be a topology change).

use std::rc::Rc;

use crate::handle::NodeId;

/// What changed in the topology.
#[derive(Debug, Clone)]
pub enum TopologyEvent {
    /// A new node was registered (state, derived, or dynamic).
    NodeRegistered(NodeId),
    /// A node received TEARDOWN (terminal destruction).
    NodeTornDown(NodeId),
    /// A node's deps were atomically replaced via `set_deps`.
    DepsChanged {
        node: NodeId,
        old_deps: Vec<NodeId>,
        new_deps: Vec<NodeId>,
    },
}

/// Callback for topology changes. D246/S2c/D248: single-owner ⇒ no
/// `Send + Sync` (fires owner-side; the bound was shared-Core-era
/// legacy). D272 (2026-05-21): switched the outer container from
/// `Arc` to `Rc` to match the single-owner-thread shape and drop the
/// `clippy::arc_with_non_send_sync` lints. The `assert_not_impl_any!`
/// below locks D248 intent at the type system.
pub type TopologySink = Rc<dyn Fn(&TopologyEvent)>;

static_assertions::assert_not_impl_any!(TopologySink: Send, Sync);

/// Identifier for a topology subscription (S2b / D225). Returned by
/// [`super::node::Core::subscribe_topology`]; pass it to
/// [`super::node::Core::unsubscribe_topology`] to deregister. The
/// core-level RAII `TopologySubscription` is retired for the same
/// reason as [`crate::node::SubscriptionId`] (D223: owned relocatable
/// `Core`, no parameterless-`Drop` reach). Binding-layer RAII wraps
/// `unsubscribe_topology` where the holder co-owns the `Core`.
pub type TopologySubscriptionId = u64;

/// Deregister topology sink `id`. Shared body (D225 S2a) for the
/// synchronous owner-invoked [`super::node::Core::unsubscribe_topology`].
/// Operates on `&C` directly — it never needed a `Core`.
pub(crate) fn unsubscribe_topology_sink(core: &crate::node::Core, id: u64) {
    // D246/S2c: `topology_sinks` lives in the `CoreShared` region
    // (`St`'s `.shared` field).
    let mut s = crate::node::St::new(core);
    s.shared.topology_sinks.remove(&id);
}

impl super::node::Core {
    /// Subscribe to topology changes. The sink fires synchronously
    /// from the registration / teardown / `set_deps` call site, under
    /// no Core lock (the state lock is dropped before firing). Sinks
    /// MAY re-enter Core (`register_*`, `teardown`, `set_deps`, etc.)
    /// — the lock-released discipline (Slice A close) makes this safe.
    ///
    /// Returns a [`TopologySubscriptionId`]; pass it to
    /// [`Self::unsubscribe_topology`] to deregister (S2b / D225: core
    /// RAII retired — binding-layer RAII wraps `unsubscribe_topology`).
    ///
    /// # Event semantics
    ///
    /// - `NodeRegistered(id)` fires from `register_state` /
    ///   `register_computed`. The Core has finished installing the node
    ///   record but a Graph-layer namespace name (if any) is NOT yet in
    ///   place — the sink runs while the caller (`Graph::add`) is still
    ///   between Core insert and namespace insert. Sinks calling
    ///   `graph.name_of(id)` from this event will see `None`. Use the
    ///   Graph-level [`crate::node::Core`]-paired namespace-change hook
    ///   (graphrefly-graph) for namespace-aware reactivity.
    /// - `NodeTornDown(id)` fires for the root teardown AND for every
    ///   meta companion + downstream consumer that auto-cascades. One
    ///   `Core::teardown(root)` call may produce many events.
    /// - `DepsChanged { ... }` fires only when `set_deps` actually
    ///   rewires deps. The idempotent fast-path (deps unchanged as a
    ///   set) returns without firing.
    pub fn subscribe_topology(&self, sink: TopologySink) -> TopologySubscriptionId {
        let mut s = self.lock_state();
        let id = s.shared.next_topology_id;
        s.shared.next_topology_id += 1;
        s.shared.topology_sinks.insert(id, sink);
        id
    }

    /// Synchronous owner-invoked topology unsubscribe (D225 refined A2).
    /// Symmetric with `subscribe_topology`; the binding-layer / embedder
    /// RAII wrapper calls this on `Drop` (it holds the `Core` on its
    /// affinity worker). Idempotent — removing an absent id is a no-op.
    pub fn unsubscribe_topology(&self, id: TopologySubscriptionId) {
        unsubscribe_topology_sink(self, id);
    }

    /// Fire topology event to all registered sinks. Called from
    /// registration, teardown, and `set_deps` sites AFTER the state
    /// lock is dropped.
    pub(crate) fn fire_topology_event(&self, event: &TopologyEvent) {
        // Step 2a (D220-EXEC): `topology_sinks` is pure-shared — take
        // ONLY the `CoreShared` region (no shard lock needed).
        let sinks: Vec<TopologySink> =
            self.with_shared(|sh| sh.topology_sinks.values().cloned().collect());
        for sink in sinks {
            sink(event);
        }
    }
}
