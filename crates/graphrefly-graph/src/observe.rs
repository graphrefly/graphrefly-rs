//! `Graph::observe()` / `observe_all()` / `observe_all_reactive()` —
//! default sink-style message tap (canonical §3.6.2 default mode).
//!
//! D246: observe handles carry a Core-free [`Graph`] (a cheap `Arc`
//! clone) + the embedder's `&Core` passed explicitly per call. There is
//! **no RAII `Drop`** below the binding (D246 rule 3 — this eliminates
//! the Blind #4 lock-across-unsubscribe-in-`Drop` deadlock class
//! entirely): `subscribe` returns ids; teardown is the owner-invoked
//! [`detach`](GraphObserveAllReactive::detach) (synchronous,
//! `&Core`-explicit). The embedder's [`graphrefly_core::OwnedCore`] is
//! the one RAII boundary.
//!
//! The reactive `observe_all` ns-listener fires owner-side with `&Core`
//! (D246 rule 2). The Core-topology prune of torn-down nodes fires
//! *inside* a wave, so its `unsubscribe` is routed through
//! `MailboxOp::Defer` (D246 rule 6 — sink-in-wave defers).

use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::{Rc, Weak};
use std::sync::Arc;

use graphrefly_core::{
    Core, CoreFull, LockId, Message, NodeId, PauseError, ResumeReport, Sink, SubscriptionId,
    TopologyEvent, TopologySubscriptionId,
};

use crate::graph::{register_ns_sink, Graph, GraphInner, NamespaceChangeSink};

/// Id pair for a single observe subscription. D246: a plain value (no
/// `Drop`); detach explicitly via [`Self::detach`] or let the
/// embedder's `OwnedCore` tear down on owner-thread drop.
#[derive(Debug, Clone, Copy)]
pub struct ObserveSub {
    node_id: NodeId,
    sub_id: SubscriptionId,
}

impl ObserveSub {
    /// The observed node.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// The subscription id.
    #[must_use]
    pub fn sub_id(&self) -> SubscriptionId {
        self.sub_id
    }

    /// Owner-invoked, synchronous detach (D246 rule 3).
    pub fn detach(&self, core: &Core) {
        core.unsubscribe(self.node_id, self.sub_id);
    }
}

/// Single-node observe handle (canonical §3.6.2). D246: holds a
/// Core-free [`Graph`]; `&Core` is passed per call.
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

    /// Subscribe a sink. Returns an [`ObserveSub`] id pair — detach
    /// owner-invoked (D246 rule 3).
    pub fn subscribe(&self, core: &Core, sink: Sink) -> ObserveSub {
        let sub_id = core.subscribe(self.node_id, sink);
        ObserveSub {
            node_id: self.node_id,
            sub_id,
        }
    }

    /// Send `[PAUSE, lock]` upstream.
    ///
    /// # Errors
    /// See [`PauseError`].
    pub fn pause(&self, core: &Core, lock: LockId) -> Result<(), PauseError> {
        core.pause(self.node_id, lock)
    }

    /// Send `[RESUME, lock]` upstream.
    ///
    /// # Errors
    /// See [`PauseError`].
    pub fn resume(&self, core: &Core, lock: LockId) -> Result<Option<ResumeReport>, PauseError> {
        core.resume(self.node_id, lock)
    }

    /// Send `[INVALIDATE]` upstream.
    pub fn invalidate(&self, core: &Core) {
        core.invalidate(self.node_id);
    }

    /// The backing graph handle.
    #[must_use]
    pub fn graph(&self) -> &Graph {
        &self.graph
    }
}

/// All-nodes observe handle. Subscriptions are tied to the set of
/// nodes named at `subscribe()` call time. D246: no RAII `Drop` —
/// You MUST call [`Self::detach`]`(core)` (owner-invoked) — these Core
/// subscriptions are opened via raw `core.subscribe` and are NOT
/// `OwnedCore`-tracked, so dropping the handle without `detach` leaks
/// them for the `Core` lifetime.
#[must_use = "GraphObserveAll holds Core subscriptions NOT tracked by OwnedCore; you MUST call detach(core) or they leak"]
pub struct GraphObserveAll {
    graph: Graph,
    subs: Vec<(NodeId, SubscriptionId)>,
}

impl GraphObserveAll {
    pub(crate) fn new(graph: Graph) -> Self {
        Self {
            graph,
            subs: Vec::new(),
        }
    }

    /// Multi-cast subscribe against every named node at this moment.
    /// Returns the node count.
    pub fn subscribe<F>(&mut self, core: &Core, sink: F) -> usize
    where
        F: Fn(&str, &[Message]) + 'static,
    {
        let names_to_ids: Vec<(String, NodeId)> = {
            let inner = self.graph.inner_arc().borrow_mut();
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
            let sub_id = core.subscribe(id, inner_sink);
            self.subs.push((id, sub_id));
        }
        count
    }

    /// Owner-invoked, synchronous detach of every fan-out sink
    /// (D246 rule 3 — no `Drop`, so no Blind #4 deadlock class).
    pub fn detach(&mut self, core: &Core) {
        for (node_id, sub_id) in self.subs.drain(..) {
            core.unsubscribe(node_id, sub_id);
        }
    }
}

// -------------------------------------------------------------------
// Reactive observe_all — auto-subscribe late-added nodes
// -------------------------------------------------------------------

struct ObserveAllReactiveInner {
    /// Set of `NodeId`s we've already subscribed to.
    subscribed: HashSet<NodeId>,
    /// Live `(node, sub)` pairs — unsubscribed on detach / prune.
    subs: Vec<(NodeId, SubscriptionId)>,
}

/// Reactive `observe_all` — auto-subscribes late-added named nodes via
/// the owner-side namespace-change listener, and prunes torn-down
/// nodes via the Core topology sub (the prune `unsubscribe` is
/// `MailboxOp::Defer`'d since `NodeTornDown` fires in-wave — D246 r6).
/// D246 rule 3: no RAII `Drop`; teardown is the owner-invoked
/// [`Self::detach`]`(core)` — owner-invoked, REQUIRED. The ns-sink is
/// collected by `graph.destroy(core)`; the Core topology sub + fan-out
/// subs are opened via raw `core.subscribe*` and are NOT
/// `OwnedCore`-tracked, so `detach(core)` is the only thing that
/// collects them (dropping the handle without it leaks them).
#[must_use = "GraphObserveAllReactive holds a Core topology sub + fan-out subs NOT tracked by OwnedCore; you MUST call detach(core) or they leak"]
pub struct GraphObserveAllReactive {
    graph: Graph,
    ns_sink_id: Option<u64>,
    topo_sub_id: Option<TopologySubscriptionId>,
    inner: Rc<RefCell<ObserveAllReactiveInner>>,
}

impl GraphObserveAllReactive {
    pub(crate) fn new(graph: Graph) -> Self {
        Self {
            graph,
            ns_sink_id: None,
            topo_sub_id: None,
            inner: Rc::new(RefCell::new(ObserveAllReactiveInner {
                subscribed: HashSet::new(),
                subs: Vec::new(),
            })),
        }
    }

    /// Subscribe a sink to all current AND future named nodes.
    ///
    /// # Panics
    ///
    /// Panics if called more than once on the same handle (single-shot
    /// wiring; rebuild via `observe_all_reactive`).
    pub fn subscribe<F>(&mut self, core: &Core, sink: F) -> usize
    where
        F: Fn(&str, &[Message]) + 'static,
    {
        assert!(
            self.ns_sink_id.is_none(),
            "GraphObserveAllReactive::subscribe is single-shot; called twice on the same handle"
        );

        let sink_arc: Arc<F> = Arc::new(sink);

        // P4: install the namespace listener BEFORE the initial
        // snapshot (the listener's `subscribed.insert` dedups).
        // D246 rule 2: the listener receives the owner's `&Core` at
        // fire-time (no stored/cloned Core); it subscribes new nodes
        // synchronously (fire_namespace_change is owner-side, not
        // in-wave).
        let weak_graph_inner: Weak<RefCell<GraphInner>> = Rc::downgrade(self.graph.inner_arc());
        let inner_for_ns = self.inner.clone();
        let sink_for_ns = sink_arc.clone();
        let ns_sink: NamespaceChangeSink = Arc::new(move |core: &Core| {
            let Some(arc_inner) = weak_graph_inner.upgrade() else {
                return;
            };
            let new_nodes: Vec<(String, NodeId)> = {
                let graph_inner = arc_inner.borrow_mut();
                let state = inner_for_ns.borrow_mut();
                graph_inner
                    .names
                    .iter()
                    .filter(|(_n, id)| !state.subscribed.contains(id))
                    .map(|(n, id)| (n.clone(), *id))
                    .collect()
            };
            for (name, id) in new_nodes {
                let should = {
                    let mut state = inner_for_ns.borrow_mut();
                    state.subscribed.insert(id)
                };
                if should {
                    let sink_clone = sink_for_ns.clone();
                    let owned_name = name;
                    let msg_sink: Sink = Arc::new(move |msgs: &[Message]| {
                        sink_clone(&owned_name, msgs);
                    });
                    let sub_id = core.subscribe(id, msg_sink);
                    inner_for_ns.borrow_mut().subs.push((id, sub_id));
                }
            }
        });
        self.ns_sink_id = Some(register_ns_sink(self.graph.inner_arc(), ns_sink));

        // Slice V3 D2: prune torn-down nodes. `NodeTornDown` fires
        // in-wave → the `unsubscribe` is `MailboxOp::Defer`'d (D246 r6:
        // no synchronous sink-side Core mutation).
        let inner_for_topo = self.inner.clone();
        // D249/S2c: post to the owner-side `!Send` `DeferQueue` (the
        // closure captures the `Rc<RefCell<ObserveAllReactiveInner>>`,
        // `!Send`); `Rc<DeferQueue>` is owner-thread-only — fine, this
        // topo sink is `!Send` (D248) and fires on the owner thread.
        let deferred = core.defer_queue();
        let topo_sink: Arc<dyn Fn(&TopologyEvent)> = Arc::new(move |event: &TopologyEvent| {
            if let TopologyEvent::NodeTornDown(id) = event {
                let id = *id;
                let inner_for_defer = inner_for_topo.clone();
                // No `HandleId` captured — Core-gone (`false`) just
                // skips the prune; nothing to release (D235 P8).
                let _ = deferred.post(Box::new(move |cf: &dyn CoreFull| {
                    let to_unsub: Vec<(NodeId, SubscriptionId)> = {
                        let mut state = inner_for_defer.borrow_mut();
                        if state.subscribed.remove(&id) {
                            let (keep, drop_): (Vec<_>, Vec<_>) =
                                state.subs.drain(..).partition(|(n, _)| *n != id);
                            state.subs = keep;
                            drop_
                        } else {
                            Vec::new()
                        }
                    };
                    for (n, s) in to_unsub {
                        cf.unsubscribe(n, s);
                    }
                }));
            }
        });
        self.topo_sub_id = Some(core.subscribe_topology(topo_sink));

        // Initial snapshot (listener's idempotent walk dedups overlap).
        let names_to_ids: Vec<(String, NodeId)> = {
            let graph_inner = self.graph.inner_arc().borrow_mut();
            graph_inner
                .names
                .iter()
                .map(|(n, id)| (n.clone(), *id))
                .collect()
        };
        let initial_count = names_to_ids.len();
        let to_subscribe: Vec<(String, NodeId)> = {
            let mut state = self.inner.borrow_mut();
            names_to_ids
                .into_iter()
                .filter(|(_n, id)| state.subscribed.insert(*id))
                .collect()
        };
        for (name, id) in to_subscribe {
            let sink_clone = sink_arc.clone();
            let msg_sink: Sink = Arc::new(move |msgs: &[Message]| {
                sink_clone(&name, msgs);
            });
            let sub_id = core.subscribe(id, msg_sink);
            self.inner.borrow_mut().subs.push((id, sub_id));
        }

        initial_count
    }

    /// Owner-invoked, synchronous teardown (D246 rule 3 — replaces the
    /// retired RAII `Drop`; eliminates the Blind #4 deadlock class).
    /// Topology sub first, then namespace sink, then drain the fan-out
    /// subs into a local `Vec` and release the `inner` lock BEFORE the
    /// `core.unsubscribe` cascade (`Core::unsubscribe` runs the full
    /// deactivation chain and can fire sinks synchronously; holding
    /// `inner` across it would self-deadlock — the pre-β invariant,
    /// preserved).
    pub fn detach(&mut self, core: &Core) {
        if let Some(id) = self.topo_sub_id.take() {
            core.unsubscribe_topology(id);
        }
        if let Some(id) = self.ns_sink_id.take() {
            crate::graph::unregister_ns_sink(self.graph.inner_arc(), id);
        }
        let drained: Vec<(NodeId, SubscriptionId)> = {
            let mut state = self.inner.borrow_mut();
            state.subs.drain(..).collect()
        };
        for (node_id, sub_id) in drained {
            core.unsubscribe(node_id, sub_id);
        }
    }

    /// The backing graph handle.
    #[must_use]
    pub fn graph(&self) -> &Graph {
        &self.graph
    }
}
