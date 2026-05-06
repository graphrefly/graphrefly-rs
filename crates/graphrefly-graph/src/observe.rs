//! `Graph::observe()` / `Graph::observe_all()` — default sink-style
//! message tap (canonical spec §3.6.2 default mode).
//!
//! Async-iterable / reactive (`Node<ObserveChangeset>`) / changeset
//! variants are deferred to a subsequent slice — they need either a
//! Core-level topology-change notification primitive or the M5
//! reactive structure layer to land first.

use std::sync::Arc;

use graphrefly_core::{LockId, Message, NodeId, PauseError, Sink, Subscription};

use crate::graph::Graph;

/// Single-node observe handle. `subscribe` taps downstream messages
/// from the observed node (same payload shape as a direct
/// [`graphrefly_core::Core::subscribe`], including the
/// `[Start, Data?]` handshake when a cache is present per R1.2.3
/// / R1.3.5.a).
///
/// `up` methods send tier-2 / tier-4 messages upstream
/// (`PAUSE` / `RESUME` / `INVALIDATE`) — the `up(messages)` API per
/// canonical §3.6.2 specialized to the supported message kinds.
/// Tier-3 / tier-5 / tier-6 messages have no upstream-injection
/// semantics.
#[must_use = "GraphObserveOne does nothing until you call subscribe()"]
pub struct GraphObserveOne {
    graph: Graph,
    node_id: NodeId,
}

impl GraphObserveOne {
    pub(crate) fn new(graph: Graph, node_id: NodeId) -> Self {
        Self { graph, node_id }
    }

    /// The observed `NodeId`.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Subscribe a sink. Drop the returned [`Subscription`] to detach.
    pub fn subscribe(&self, sink: Sink) -> Subscription {
        self.graph.subscribe(self.node_id, sink)
    }

    /// Send `[PAUSE, lock]` upstream (per canonical §3.6.2 `up(...)`).
    pub fn pause(&self, lock: LockId) -> Result<(), PauseError> {
        self.graph.pause(self.node_id, lock)
    }

    /// Send `[RESUME, lock]` upstream.
    pub fn resume(
        &self,
        lock: LockId,
    ) -> Result<Option<graphrefly_core::ResumeReport>, PauseError> {
        self.graph.resume(self.node_id, lock)
    }

    /// Send `[INVALIDATE]` upstream.
    pub fn invalidate(&self) {
        self.graph.invalidate(self.node_id);
    }
}

/// All-nodes observe handle. `subscribe` multiplexes across every
/// named node in the graph (NOT recursive into mounts — observers
/// wanting recursion compose with `child.observe_all()` per
/// subgraph).
///
/// The sink receives `(name, &[Message])` tuples; `name` is the
/// local namespace name. Subscriptions stay tied to the set of
/// nodes named at `observe_all()` call time — late-added nodes
/// are NOT auto-subscribed in this slice (lifts with the reactive
/// observe topology-change primitive in a later slice).
#[must_use = "GraphObserveAll holds Subscriptions; dropping it unsubscribes all sinks"]
pub struct GraphObserveAll {
    graph: Graph,
    /// One `Subscription` per named node at `observe_all()` call time.
    /// Held by the handle; dropping the handle unsubscribes every
    /// fan-out sink.
    subs: Vec<Subscription>,
}

impl GraphObserveAll {
    pub(crate) fn new(graph: Graph) -> Self {
        Self {
            graph,
            subs: Vec::new(),
        }
    }

    /// Multi-cast subscribe — registers `sink` against every named
    /// node at this exact moment. Each underlying subscription is
    /// kept alive in `self`; drop the handle to unsubscribe all.
    ///
    /// Returns the number of nodes the sink was registered against.
    pub fn subscribe<F>(&mut self, sink: F) -> usize
    where
        F: Fn(&str, &[Message]) + Send + Sync + 'static,
    {
        let names_to_ids: Vec<(String, NodeId)> = {
            let inner = self.graph.inner.lock();
            inner.names.iter().map(|(n, id)| (n.clone(), *id)).collect()
        };
        let sink_arc: Arc<F> = Arc::new(sink);
        let count = names_to_ids.len();
        for (name, id) in names_to_ids {
            let sink_clone = sink_arc.clone();
            let owned_name = name;
            let inner_sink: Sink = Arc::new(move |msgs: &[Message]| {
                sink_clone(&owned_name, msgs);
            });
            let sub = self.graph.subscribe(id, inner_sink);
            self.subs.push(sub);
        }
        count
    }
}

impl Graph {
    /// Tap a single node's downstream message stream.
    ///
    /// # Panics
    ///
    /// Panics if `path` doesn't resolve. Use [`Graph::try_resolve`]
    /// + a manual subscribe if non-panicking is required.
    pub fn observe(&self, path: &str) -> GraphObserveOne {
        let id = self.node(path);
        GraphObserveOne::new(self.clone(), id)
    }

    /// Tap every named node in this graph.
    ///
    /// # Snapshot semantics
    ///
    /// The returned handle subscribes against the namespace at
    /// the moment `subscribe()` is called. Nodes named AFTER that
    /// call are not auto-subscribed. Re-call `observe_all()` to
    /// pick them up; reactive variants in a later slice will
    /// handle dynamic membership.
    pub fn observe_all(&self) -> GraphObserveAll {
        GraphObserveAll::new(self.clone())
    }
}
