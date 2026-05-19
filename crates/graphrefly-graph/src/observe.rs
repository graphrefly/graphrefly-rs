//! `Graph::observe()` / `observe_all()` / `observe_all_reactive()` —
//! default sink-style message tap (canonical §3.6.2 default mode).
//!
//! S2b/β (D237/D240/D242/D241-AMEND): observe handles carry a borrowed
//! [`SubgraphRef`] (the root `&Core` + namespace handle). The `&Core`
//! borrow statically pins the Core alive for the handle's lifetime and
//! makes the handle `!Send` (owner-thread-bound), so RAII `Drop`
//! **synchronously unsubscribes** — sound, not the D225 relocating-Core
//! case. Subscribe returns ids (D241); the RAII guards wrap them.
//!
//! The reactive `observe_all` ns-listener fires owner-side with `&Core`
//! (D231). The Core-topology prune of torn-down nodes fires *inside* a
//! wave, so its `unsubscribe` is routed through `MailboxOp::Defer`
//! (D233 — sink-side Core mutation never runs synchronously in-wave).

use std::collections::HashSet;
use std::sync::{Arc, Weak};

use graphrefly_core::{
    Core, CoreFull, LockId, Message, NodeId, PauseError, ResumeReport, Sink, SubscriptionId,
    TopologyEvent, TopologySubscriptionId,
};
use parking_lot::Mutex;

use crate::graph::{register_ns_sink, GraphInner, GraphOps, NamespaceChangeSink, SubgraphRef};

/// RAII guard for a single observe subscription. Dropping it
/// synchronously unsubscribes (β/D241-AMEND: `&'g Core` borrow pins
/// the Core alive; `!Send` ⇒ owner-thread drop).
#[must_use = "ObserveSub unsubscribes on drop; bind it to keep the tap alive"]
pub struct ObserveSub<'g> {
    core: &'g Core,
    node_id: NodeId,
    sub_id: SubscriptionId,
}

impl ObserveSub<'_> {
    /// The observed node.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }
}

impl Drop for ObserveSub<'_> {
    fn drop(&mut self) {
        self.core.unsubscribe(self.node_id, self.sub_id);
    }
}

/// Single-node observe handle (canonical §3.6.2). β/D242: borrows the
/// root via [`SubgraphRef`].
#[must_use = "GraphObserveOne does nothing until you call subscribe()"]
pub struct GraphObserveOne<'g> {
    sub: SubgraphRef<'g>,
    node_id: NodeId,
}

impl<'g> GraphObserveOne<'g> {
    pub(crate) fn new(sub: SubgraphRef<'g>, node_id: NodeId) -> Self {
        Self { sub, node_id }
    }

    /// The observed `NodeId`.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Subscribe a sink. Returns an [`ObserveSub`] RAII guard — drop it
    /// to detach (β/D241-AMEND).
    pub fn subscribe(&self, sink: Sink) -> ObserveSub<'g> {
        let sub_id = self.sub.subscribe(self.node_id, sink);
        ObserveSub {
            core: self.sub.core,
            node_id: self.node_id,
            sub_id,
        }
    }

    /// Send `[PAUSE, lock]` upstream.
    pub fn pause(&self, lock: LockId) -> Result<(), PauseError> {
        self.sub.pause(self.node_id, lock)
    }

    /// Send `[RESUME, lock]` upstream.
    pub fn resume(&self, lock: LockId) -> Result<Option<ResumeReport>, PauseError> {
        self.sub.resume(self.node_id, lock)
    }

    /// Send `[INVALIDATE]` upstream.
    pub fn invalidate(&self) {
        self.sub.invalidate(self.node_id);
    }
}

/// All-nodes observe handle. Subscriptions are tied to the set of
/// nodes named at `subscribe()` call time. β/D241-AMEND: RAII `Drop`
/// unsubscribes every fan-out sink.
#[must_use = "GraphObserveAll holds subscriptions; dropping it unsubscribes all sinks"]
pub struct GraphObserveAll<'g> {
    sub: SubgraphRef<'g>,
    subs: Vec<(NodeId, SubscriptionId)>,
}

impl<'g> GraphObserveAll<'g> {
    pub(crate) fn new(sub: SubgraphRef<'g>) -> Self {
        Self {
            sub,
            subs: Vec::new(),
        }
    }

    /// Multi-cast subscribe against every named node at this moment.
    /// Returns the node count.
    pub fn subscribe<F>(&mut self, sink: F) -> usize
    where
        F: Fn(&str, &[Message]) + Send + Sync + 'static,
    {
        let names_to_ids: Vec<(String, NodeId)> = {
            let inner = self.sub.inner.lock();
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
            let sub_id = self.sub.subscribe(id, inner_sink);
            self.subs.push((id, sub_id));
        }
        count
    }
}

impl Drop for GraphObserveAll<'_> {
    fn drop(&mut self) {
        for (node_id, sub_id) in self.subs.drain(..) {
            self.sub.core.unsubscribe(node_id, sub_id);
        }
    }
}

// -------------------------------------------------------------------
// Reactive observe_all — auto-subscribe late-added nodes
// -------------------------------------------------------------------

struct ObserveAllReactiveInner {
    /// Set of `NodeId`s we've already subscribed to.
    subscribed: HashSet<NodeId>,
    /// Live `(node, sub)` pairs — unsubscribed on drop / prune.
    subs: Vec<(NodeId, SubscriptionId)>,
}

/// Reactive `observe_all` — auto-subscribes late-added named nodes via
/// the owner-side namespace-change listener, and prunes torn-down
/// nodes via the Core topology sub (the prune `unsubscribe` is
/// `MailboxOp::Defer`'d since `NodeTornDown` fires in-wave — D233).
/// β/D241-AMEND: RAII `Drop` unsubscribes everything.
#[must_use = "GraphObserveAllReactive holds subscriptions; dropping it unsubscribes all sinks"]
pub struct GraphObserveAllReactive<'g> {
    sub: SubgraphRef<'g>,
    ns_sink_id: Option<u64>,
    topo_sub_id: Option<TopologySubscriptionId>,
    inner: Arc<Mutex<ObserveAllReactiveInner>>,
}

impl Drop for GraphObserveAllReactive<'_> {
    fn drop(&mut self) {
        // Topology sub first, then namespace sink, then the fan-out
        // subs — all synchronous + owner-thread (β/D241-AMEND).
        if let Some(id) = self.topo_sub_id.take() {
            self.sub.core.unsubscribe_topology(id);
        }
        if let Some(id) = self.ns_sink_id.take() {
            crate::graph::unregister_ns_sink(&self.sub.inner, id);
        }
        // /qa-2026-05-18 Blind #4: drain the fan-out subs into a local
        // Vec and RELEASE the `inner` lock BEFORE the `core.unsubscribe`
        // cascade. `Core::unsubscribe` runs the full
        // OnDeactivation→producer_deactivate→wipe_ctx→cache-clear chain
        // and can fire teardown/topology sinks synchronously; any such
        // sink re-entering a path that locks this non-reentrant
        // `ObserveAllReactiveInner` mutex would self-deadlock. The
        // pre-β code avoided this for the same reason ("dropped BEFORE
        // inner to avoid deadlock").
        let drained: Vec<(NodeId, SubscriptionId)> = {
            let mut state = self.inner.lock();
            state.subs.drain(..).collect()
        };
        for (node_id, sub_id) in drained {
            self.sub.core.unsubscribe(node_id, sub_id);
        }
    }
}

impl<'g> GraphObserveAllReactive<'g> {
    pub(crate) fn new(sub: SubgraphRef<'g>) -> Self {
        Self {
            sub,
            ns_sink_id: None,
            topo_sub_id: None,
            inner: Arc::new(Mutex::new(ObserveAllReactiveInner {
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
    pub fn subscribe<F>(&mut self, sink: F) -> usize
    where
        F: Fn(&str, &[Message]) + Send + Sync + 'static,
    {
        assert!(
            self.ns_sink_id.is_none(),
            "GraphObserveAllReactive::subscribe is single-shot; called twice on the same handle"
        );

        let sink_arc: Arc<F> = Arc::new(sink);

        // P4: install the namespace listener BEFORE the initial
        // snapshot (the listener's `subscribed.insert` dedups).
        // β/D231: the listener receives the owner's `&Core` at
        // fire-time (no stored/cloned Core); it subscribes new nodes
        // synchronously (fire_namespace_change is owner-side, not
        // in-wave).
        let weak_graph_inner: Weak<Mutex<GraphInner>> = Arc::downgrade(&self.sub.inner);
        let inner_for_ns = self.inner.clone();
        let sink_for_ns = sink_arc.clone();
        let ns_sink: NamespaceChangeSink = Arc::new(move |core: &Core| {
            let Some(arc_inner) = weak_graph_inner.upgrade() else {
                return;
            };
            let new_nodes: Vec<(String, NodeId)> = {
                let graph_inner = arc_inner.lock();
                let state = inner_for_ns.lock();
                graph_inner
                    .names
                    .iter()
                    .filter(|(_n, id)| !state.subscribed.contains(id))
                    .map(|(n, id)| (n.clone(), *id))
                    .collect()
            };
            for (name, id) in new_nodes {
                let should = {
                    let mut state = inner_for_ns.lock();
                    state.subscribed.insert(id)
                };
                if should {
                    let sink_clone = sink_for_ns.clone();
                    let owned_name = name;
                    let msg_sink: Sink = Arc::new(move |msgs: &[Message]| {
                        sink_clone(&owned_name, msgs);
                    });
                    let sub_id = core.subscribe(id, msg_sink);
                    inner_for_ns.lock().subs.push((id, sub_id));
                }
            }
        });
        self.ns_sink_id = Some(register_ns_sink(&self.sub.inner, ns_sink));

        // Slice V3 D2: prune torn-down nodes. `NodeTornDown` fires
        // in-wave → the `unsubscribe` is `MailboxOp::Defer`'d (D233:
        // no synchronous sink-side Core mutation).
        let inner_for_topo = self.inner.clone();
        let mailbox = self.sub.core.mailbox();
        let topo_sink: Arc<dyn Fn(&TopologyEvent) + Send + Sync> =
            Arc::new(move |event: &TopologyEvent| {
                if let TopologyEvent::NodeTornDown(id) = event {
                    let id = *id;
                    let inner_for_defer = inner_for_topo.clone();
                    // No `HandleId` captured — Core-gone (`false`) just
                    // skips the prune; nothing to release (D235 P8).
                    let _ = mailbox.post_defer(Box::new(move |cf: &dyn CoreFull| {
                        let to_unsub: Vec<(NodeId, SubscriptionId)> = {
                            let mut state = inner_for_defer.lock();
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
        self.topo_sub_id = Some(self.sub.core.subscribe_topology(topo_sink));

        // Initial snapshot (listener's idempotent walk dedups overlap).
        let names_to_ids: Vec<(String, NodeId)> = {
            let graph_inner = self.sub.inner.lock();
            graph_inner
                .names
                .iter()
                .map(|(n, id)| (n.clone(), *id))
                .collect()
        };
        let initial_count = names_to_ids.len();
        let to_subscribe: Vec<(String, NodeId)> = {
            let mut state = self.inner.lock();
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
            let sub_id = self.sub.subscribe(id, msg_sink);
            self.inner.lock().subs.push((id, sub_id));
        }

        initial_count
    }
}
