//! `Graph::observe()` / `Graph::observe_all()` — default sink-style
//! message tap (canonical spec §3.6.2 default mode).
//!
//! `observe_all_reactive()` auto-subscribes late-added nodes via
//! the Core topology-change notification primitive (Slice F+).
//!
//! Async-iterable / reactive (`Node<ObserveChangeset>`) / changeset
//! variants are deferred (Phase 14).

use std::collections::HashSet;
use std::sync::{Arc, Weak};

use graphrefly_core::{Core, LockId, Message, NodeId, PauseError, Sink, Subscription};
use parking_lot::Mutex;

use crate::graph::{Graph, GraphInner};

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
    /// call are not auto-subscribed. Use [`Self::observe_all_reactive`]
    /// for dynamic membership.
    pub fn observe_all(&self) -> GraphObserveAll {
        GraphObserveAll::new(self.clone())
    }

    /// Tap every named node AND auto-subscribe late-added nodes.
    ///
    /// Like [`Self::observe_all`] but subscribes to topology changes
    /// so that nodes registered AFTER the initial `subscribe()` call
    /// are automatically picked up. Dropping the returned handle
    /// unsubscribes all fan-out sinks AND the topology listener.
    pub fn observe_all_reactive(&self) -> GraphObserveAllReactive {
        GraphObserveAllReactive::new(self.clone())
    }
}

// -------------------------------------------------------------------
// Reactive observe_all — auto-subscribe late-added nodes
// -------------------------------------------------------------------

/// Shared state for the reactive `observe_all` subscription.
struct ObserveAllReactiveInner {
    /// Set of `NodeId`s we've already subscribed to.
    subscribed: HashSet<NodeId>,
    /// Live subscriptions — kept alive so dropping them unsubscribes.
    subs: Vec<Subscription>,
}

/// Reactive variant of [`GraphObserveAll`] that auto-subscribes
/// late-added named nodes via Graph namespace-change notifications.
///
/// Drop to unsubscribe everything.
#[must_use = "GraphObserveAllReactive holds Subscriptions; dropping it unsubscribes all sinks"]
pub struct GraphObserveAllReactive {
    graph: Graph,
    /// Namespace-change subscription id. Dropped BEFORE `inner` to
    /// avoid deadlock: unsubscribe removes the sink (which holds a
    /// `Weak<Mutex<GraphInner>>`); the closure's captured Arcs are
    /// not the last refs because `inner` is still owned by `Self`.
    ns_sink_id: Option<u64>,
    inner: Arc<Mutex<ObserveAllReactiveInner>>,
}

// Send + Sync compile-time assertion.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<GraphObserveAllReactive>();
};

impl Drop for GraphObserveAllReactive {
    fn drop(&mut self) {
        // Unsubscribe namespace sink BEFORE inner drops, to avoid
        // the deadlock described in the field comment.
        if let Some(id) = self.ns_sink_id.take() {
            self.graph.unsubscribe_namespace_change(id);
        }
    }
}

impl GraphObserveAllReactive {
    fn new(graph: Graph) -> Self {
        Self {
            graph,
            ns_sink_id: None,
            inner: Arc::new(Mutex::new(ObserveAllReactiveInner {
                subscribed: HashSet::new(),
                subs: Vec::new(),
            })),
        }
    }

    /// Subscribe a sink to all current AND future named nodes.
    ///
    /// The sink receives `(name, &[Message])` tuples, same as
    /// [`GraphObserveAll::subscribe`]. Returns the number of nodes
    /// subscribed at call time (future auto-subscriptions are not
    /// counted).
    ///
    /// # Panics
    ///
    /// Panics if called more than once on the same handle. The v1
    /// contract is "single-shot wiring"; rebuild the handle via
    /// [`Graph::observe_all_reactive`] to install another sink.
    pub fn subscribe<F>(&mut self, sink: F) -> usize
    where
        F: Fn(&str, &[Message]) + Send + Sync + 'static,
    {
        // P5 — subscribe-once contract. A second call would leak the
        // first namespace sink (we'd overwrite `ns_sink_id` without
        // unsubscribing the prior). Panic on misuse instead.
        assert!(
            self.ns_sink_id.is_none(),
            "GraphObserveAllReactive::subscribe is single-shot; called twice on the same handle"
        );

        let sink_arc: Arc<F> = Arc::new(sink);

        // P4 — install the namespace listener BEFORE the initial
        // snapshot. A node added concurrently between snapshot and
        // listener-install would otherwise be permanently missed.
        // The listener's `inner.subscribed.insert(id)` dedups against
        // the snapshot, so an idempotent overlap is harmless.
        //
        // P6 — capture Weak<inner> + Core (clone) instead of the full
        // Graph clone. This breaks the namespace_sinks → sink → Graph
        // → namespace_sinks Arc cycle so leaking the handle does not
        // leak the graph.
        let weak_graph_inner: Weak<Mutex<GraphInner>> = Arc::downgrade(&self.graph.inner);
        let core: Core = self.graph.core.clone();
        let inner_for_ns = self.inner.clone();
        let sink_for_ns = sink_arc.clone();
        let ns_sink = Arc::new(move || {
            let Some(arc_inner) = weak_graph_inner.upgrade() else {
                // Graph dropped; silent no-op.
                return;
            };
            let graph = Graph {
                core: core.clone(),
                inner: arc_inner,
            };
            // Scan for any newly-named nodes we haven't subscribed to.
            let new_nodes: Vec<(String, NodeId)> = {
                let graph_inner = graph.inner.lock();
                let state = inner_for_ns.lock();
                graph_inner
                    .names
                    .iter()
                    .filter(|(_name, id)| !state.subscribed.contains(id))
                    .map(|(n, id)| (n.clone(), *id))
                    .collect()
            };
            for (name, id) in new_nodes {
                let should_subscribe = {
                    let mut state = inner_for_ns.lock();
                    state.subscribed.insert(id)
                };
                if should_subscribe {
                    let sink_clone = sink_for_ns.clone();
                    let owned_name = name;
                    let msg_sink: Sink = Arc::new(move |msgs: &[Message]| {
                        sink_clone(&owned_name, msgs);
                    });
                    let sub = graph.subscribe(id, msg_sink);
                    inner_for_ns.lock().subs.push(sub);
                }
            }
        });
        self.ns_sink_id = Some(self.graph.subscribe_namespace_change(ns_sink));

        // Now take the initial snapshot. Any node added between
        // listener-install and snapshot will be picked up by the
        // listener's idempotent walk — `inner.subscribed.insert(id)`
        // returns false if we beat the listener to it.
        let names_to_ids: Vec<(String, NodeId)> = {
            let graph_inner = self.graph.inner.lock();
            graph_inner
                .names
                .iter()
                .map(|(n, id)| (n.clone(), *id))
                .collect()
        };
        let initial_count = names_to_ids.len();
        let to_subscribe: Vec<(String, NodeId)> = {
            let mut inner = self.inner.lock();
            names_to_ids
                .into_iter()
                .filter(|(_name, id)| inner.subscribed.insert(*id))
                .collect()
        };
        for (name, id) in to_subscribe {
            let sink_clone = sink_arc.clone();
            let msg_sink: Sink = Arc::new(move |msgs: &[Message]| {
                sink_clone(&name, msgs);
            });
            let sub = self.graph.subscribe(id, msg_sink);
            self.inner.lock().subs.push(sub);
        }

        initial_count
    }
}
