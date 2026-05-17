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

use std::sync::Arc;

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

/// Callback for topology changes. `Send + Sync` for cross-thread Core usage.
pub type TopologySink = Arc<dyn Fn(&TopologyEvent) + Send + Sync>;

/// RAII handle for a topology subscription. Dropping it unregisters the sink.
#[must_use = "TopologySubscription holds the subscription; dropping it unregisters the sink"]
pub struct TopologySubscription<C: crate::state_cell::StateCell = crate::state_cell::LockedCell> {
    /// Index into `CoreShared.topology_sinks`. We use a generational
    /// approach: each subscription gets a unique id; unsubscribe
    /// marks the slot as `None`.
    id: u64,
    /// Weak ref to the state cell — silent no-op if Core is dropped first.
    state: std::sync::Weak<C>,
}

impl<C: crate::state_cell::StateCell> Drop for TopologySubscription<C> {
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade() {
            // Step 2a (D220-EXEC): combined guard — `topology_sinks`
            // lives in the `CoreShared` region (`St`'s `.shared` field).
            let mut s = crate::node::St::new(&*state);
            s.shared.topology_sinks.remove(&self.id);
        }
    }
}

// Send + Sync compile-time assertion.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<TopologySubscription>();
};

impl<C: crate::state_cell::StateCell> super::node::Core<C> {
    /// Subscribe to topology changes. The sink fires synchronously
    /// from the registration / teardown / `set_deps` call site, under
    /// no Core lock (the state lock is dropped before firing). Sinks
    /// MAY re-enter Core (`register_*`, `teardown`, `set_deps`, etc.)
    /// — the lock-released discipline (Slice A close) makes this safe.
    ///
    /// Returns a [`TopologySubscription`] — dropping it unregisters
    /// the sink.
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
    pub fn subscribe_topology(&self, sink: TopologySink) -> TopologySubscription<C> {
        let mut s = self.lock_state();
        let id = s.shared.next_topology_id;
        s.shared.next_topology_id += 1;
        s.shared.topology_sinks.insert(id, sink);
        TopologySubscription {
            id,
            state: Arc::downgrade(&self.state),
        }
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
