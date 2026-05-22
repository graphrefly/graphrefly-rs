//! `Graph::describe()` — JSON form of canonical spec §3.6 + Appendix B.
//!
//! D246: describe logic is a free fn [`describe_of`] over
//! `(&dyn CoreFull, &Rc<RefCell<GraphInner>>)` so the one [`Graph`]
//! (`crate::Graph`) reuses it, AND so the in-wave reactive-describe
//! `MailboxOp::Defer` closure (D246 rule 6) can run it through the
//! `&dyn CoreFull` it is handed (the one facade carries read-only
//! inspection). `ReactiveDescribeHandle` holds ids only (Core-free,
//! `Send`); there is **no RAII `Drop`** (D246 rule 3) — teardown is the
//! owner-invoked [`ReactiveDescribeHandle::detach`].
//!
//! # Value rendering — raw vs. binding-rendered
//!
//! Canonical TS surfaces `value: T` directly. The Rust port preserves
//! the handle-protocol cleaving plane (`value: DescribeValue`):
//! `Handle(HandleId)` raw u64 (default) or `Rendered(serde_json::Value)`
//! via [`DebugBindingBoundary`].

use std::cell::RefCell;
use std::rc::{Rc, Weak};

use graphrefly_core::{
    Core, CoreFull, HandleId, NodeId, NodeKind, OperatorOp, TerminalKind, TopologyEvent,
    TopologySubscriptionId, NO_HANDLE,
};
use indexmap::IndexMap;
use serde::{Serialize, Serializer};

use crate::debug::DebugBindingBoundary;
use crate::graph::{register_ns_sink, unregister_ns_sink, GraphInner};

/// Top-level `describe()` output (canonical Appendix B JSON schema).
#[derive(Debug, Clone, Serialize)]
pub struct GraphDescribeOutput {
    /// Graph name as set at construction / mount.
    pub name: String,
    /// Local nodes by name.
    pub nodes: IndexMap<String, NodeDescribe>,
    /// Local edges (dep → consumer).
    pub edges: Vec<EdgeDescribe>,
    /// Mounted child names.
    pub subgraphs: Vec<String>,
}

/// Per-node descriptor.
#[derive(Debug, Clone, Serialize)]
pub struct NodeDescribe {
    /// `"state"` / `"derived"` / `"dynamic"` / `"producer"`.
    #[serde(rename = "type")]
    pub r#type: NodeTypeStr,
    /// Lifecycle status (canonical Appendix B enum).
    pub status: NodeStatus,
    /// Current cache value. `None` when sentinel (`NO_HANDLE`).
    pub value: Option<DescribeValue>,
    /// Dep names in declaration order (`_anon_<NodeId>` for unnamed).
    pub deps: Vec<String>,
    /// Operator discriminant (e.g. `"map"`); `None` for non-operators.
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "operator")]
    pub operator_kind: Option<String>,
    /// Free-form metadata per canonical Appendix B. Always `None` in
    /// this slice (metadata-storage primitive not yet shipped).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<serde_json::Value>,
}

/// Per-node cache value in `describe` output. Serialized uniformly
/// without an enum tag.
#[derive(Debug, Clone, PartialEq)]
pub enum DescribeValue {
    /// Raw handle view (default for [`crate::GraphOps::describe`]).
    Handle(HandleId),
    /// Binding-rendered view (from [`crate::GraphOps::describe_with_debug`]).
    Rendered(serde_json::Value),
}

impl Serialize for DescribeValue {
    fn serialize<S: Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        match self {
            DescribeValue::Handle(h) => ser.serialize_u64(h.raw()),
            DescribeValue::Rendered(v) => v.serialize(ser),
        }
    }
}

/// Edge between two named nodes (or a named node and `_anon_<NodeId>`).
#[derive(Debug, Clone, Serialize)]
pub struct EdgeDescribe {
    pub from: String,
    pub to: String,
}

/// Canonical Appendix B `type` enum.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeTypeStr {
    State,
    Derived,
    Dynamic,
    Producer,
    Effect,
    Operator,
}

/// Canonical Appendix B `status` enum.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NodeStatus {
    Sentinel,
    Pending,
    Dirty,
    Settled,
    Resolved,
    Completed,
    Errored,
}

/// β/D243: describe over the read-only-inspection `&dyn CoreFull` (so
/// the in-wave `MailboxOp::Defer` reactive-describe closure can run it)
/// + the namespace handle. Pure read; no `Core`-mutation.
pub(crate) fn describe_of(
    core: &dyn CoreFull,
    inner_arc: &Rc<RefCell<GraphInner>>,
    debug: Option<&dyn DebugBindingBoundary>,
) -> GraphDescribeOutput {
    let (graph_name, local_names, subgraphs, names_iter) = {
        let inner = inner_arc.borrow_mut();
        let graph_name = inner.name.clone();
        let local_names: IndexMap<NodeId, String> = inner
            .names
            .iter()
            .map(|(name, id)| (*id, name.clone()))
            .collect();
        let subgraphs: Vec<String> = inner.children.keys().cloned().collect();
        let names_iter: Vec<(String, NodeId)> =
            inner.names.iter().map(|(n, id)| (n.clone(), *id)).collect();
        (graph_name, local_names, subgraphs, names_iter)
    };

    let mut nodes: IndexMap<String, NodeDescribe> = IndexMap::new();
    let mut edges: Vec<EdgeDescribe> = Vec::new();

    for (name, id) in &names_iter {
        let kind = core.kind_of(*id).unwrap_or(NodeKind::State);
        let cache = core.cache_of(*id);
        let terminal = core.is_terminal(*id);
        let dirty = core.is_dirty(*id);
        let fired = core.has_fired_once(*id);

        let dep_ids = core.deps_of(*id);
        let dep_names: Vec<String> = dep_ids
            .iter()
            .map(|d| {
                local_names
                    .get(d)
                    .cloned()
                    .unwrap_or_else(|| format!("_anon_{}", d.raw()))
            })
            .collect();
        for dep_name in &dep_names {
            edges.push(EdgeDescribe {
                from: dep_name.clone(),
                to: name.clone(),
            });
        }

        let value = if cache == NO_HANDLE {
            None
        } else if let Some(debug) = debug {
            Some(DescribeValue::Rendered(debug.handle_to_debug(cache)))
        } else {
            Some(DescribeValue::Handle(cache))
        };

        let operator_kind = match kind {
            NodeKind::Operator(op) => Some(operator_op_name(op)),
            _ => None,
        };
        nodes.insert(
            name.clone(),
            NodeDescribe {
                r#type: type_str_of(kind),
                status: status_of(kind, cache, terminal, dirty, fired),
                value,
                deps: dep_names,
                operator_kind,
                meta: None,
            },
        );
    }

    GraphDescribeOutput {
        name: graph_name,
        nodes,
        edges,
        subgraphs,
    }
}

fn type_str_of(kind: NodeKind) -> NodeTypeStr {
    match kind {
        NodeKind::State => NodeTypeStr::State,
        NodeKind::Producer => NodeTypeStr::Producer,
        NodeKind::Derived => NodeTypeStr::Derived,
        NodeKind::Dynamic => NodeTypeStr::Dynamic,
        NodeKind::Operator(_) => NodeTypeStr::Operator,
    }
}

fn operator_op_name(op: OperatorOp) -> String {
    match op {
        OperatorOp::Map { .. } => "map",
        OperatorOp::Filter { .. } => "filter",
        OperatorOp::Scan { .. } => "scan",
        OperatorOp::Reduce { .. } => "reduce",
        OperatorOp::DistinctUntilChanged { .. } => "distinctUntilChanged",
        OperatorOp::Pairwise { .. } => "pairwise",
        OperatorOp::Combine { .. } => "combine",
        OperatorOp::WithLatestFrom { .. } => "withLatestFrom",
        OperatorOp::Merge => "merge",
        OperatorOp::Take { .. } => "take",
        OperatorOp::Skip { .. } => "skip",
        OperatorOp::TakeWhile { .. } => "takeWhile",
        OperatorOp::Last { .. } => "last",
        OperatorOp::Tap { .. } => "tap",
        OperatorOp::TapFirst { .. } => "tapFirst",
        OperatorOp::Valve => "valve",
        OperatorOp::Settle { .. } => "settle",
    }
    .to_owned()
}

/// Canonical-spec §3.6.1 status mapping. Precedence: errored >
/// completed > dirty > (cache-cleared) > settled > pending > sentinel.
/// R1.3.7.b: `cache == NO_HANDLE` discriminates Sentinel-vs-Settled
/// BEFORE the `fired` check (post-INVALIDATE fired compute → Sentinel).
fn status_of(
    kind: NodeKind,
    cache: HandleId,
    terminal: Option<TerminalKind>,
    dirty: bool,
    fired: bool,
) -> NodeStatus {
    match terminal {
        Some(TerminalKind::Error(_)) => return NodeStatus::Errored,
        Some(TerminalKind::Complete) => return NodeStatus::Completed,
        None => {}
    }
    if dirty {
        return NodeStatus::Dirty;
    }
    if cache == NO_HANDLE {
        return match kind {
            NodeKind::State => NodeStatus::Sentinel,
            NodeKind::Producer | NodeKind::Derived | NodeKind::Dynamic | NodeKind::Operator(_) => {
                if fired {
                    NodeStatus::Sentinel
                } else {
                    NodeStatus::Pending
                }
            }
        };
    }
    NodeStatus::Settled
}

// -------------------------------------------------------------------
// Reactive describe (canonical §3.6.1 `reactive: true` mode)
// -------------------------------------------------------------------

/// Sink type for reactive describe. D272 (2026-05-21): single-owner-
/// thread shape — `Rc<dyn Fn>` matches D248's `!Send + !Sync` Core. The
/// `assert_not_impl_any!` below locks D248 intent at the type system.
pub type DescribeSink = Rc<dyn Fn(&GraphDescribeOutput)>;

static_assertions::assert_not_impl_any!(DescribeSink: Send, Sync);

/// Id-bearing handle for a reactive describe subscription.
///
/// D246 rule 3: Core-free (`Send`), **no RAII `Drop`** — teardown is
/// the owner-invoked synchronous [`Self::detach`]. This eliminates the
/// "unsubscribe in `Drop`" deadlock class. The embedder's
/// Teardown is the owner-invoked [`Self::detach`]`(core)` — REQUIRED.
/// The ns-sink is also collected by `graph.destroy(core)`; the Core
/// topology sub is opened via raw `core.subscribe_topology` and is NOT
/// `OwnedCore`-tracked, so only `detach(core)` collects it.
#[must_use = "ReactiveDescribeHandle holds a Core topology sub NOT tracked by OwnedCore; you MUST call detach(core) or it leaks"]
pub struct ReactiveDescribeHandle {
    inner: Rc<RefCell<GraphInner>>,
    ns_sink_id: u64,
    /// Slice V3 D5: Core topology sub for `DepsChanged` (edges change
    /// without a namespace change). D246 r6: re-snapshot is in-wave
    /// `MailboxOp::Defer`'d (the topology event fires inside a Core
    /// wave; `describe_of` runs via the handed `&dyn CoreFull`).
    topo_sub_id: TopologySubscriptionId,
}

impl ReactiveDescribeHandle {
    /// Owner-invoked, synchronous detach (D246 rule 3). Topology sub
    /// first (so a topo fire mid-detach can't re-snapshot through a
    /// half-removed namespace sink), then the namespace sink.
    pub fn detach(&self, core: &Core) {
        core.unsubscribe_topology(self.topo_sub_id);
        unregister_ns_sink(&self.inner, self.ns_sink_id);
    }
}

/// Build a reactive-describe subscription. Push-on-subscribe fires
/// the current snapshot once, then re-fires on every namespace change
/// (owner-side `&Core`, D246 r2) and on `set_deps` `DepsChanged`
/// (in-wave `MailboxOp::Defer` → `&dyn CoreFull`, D246 r6).
pub(crate) fn describe_reactive_in(
    core: &Core,
    inner: &Rc<RefCell<GraphInner>>,
    sink: &DescribeSink,
) -> ReactiveDescribeHandle {
    // Push-on-subscribe (no lock held).
    sink(&describe_of(core, inner, None));

    // Namespace-change path (owner-side `&Core`, β/D231).
    let weak_inner: Weak<RefCell<GraphInner>> = Rc::downgrade(inner);
    let sink_ns = sink.clone();
    let ns_sink: crate::graph::NamespaceChangeSink = Rc::new(move |c: &Core| {
        let Some(arc_inner) = weak_inner.upgrade() else {
            return;
        };
        sink_ns(&describe_of(c, &arc_inner, None));
    });
    let ns_sink_id = register_ns_sink(inner, ns_sink);

    // Topology path (set_deps → `DepsChanged`, fired inside a Core
    // wave): re-snapshot via an in-wave `MailboxOp::Defer` so it runs
    // owner-side with a real `&dyn CoreFull` (D243/D233).
    let weak_inner_topo: Weak<RefCell<GraphInner>> = Rc::downgrade(inner);
    // D249/S2c: owner-side `!Send` `DeferQueue` (the closure captures a
    // `Weak<RefCell<GraphInner>>`, `!Send`). Owner-thread-only `Rc` —
    // fine: this topo sink is `!Send` (D248) and fires owner-side.
    let deferred = core.defer_queue();
    let sink_topo = sink.clone();
    // D246 rule 8 (S4): reusable coalescing slot. Re-snapshot is
    // idempotent at drain time (`describe_of` reads current state), so
    // N `DepsChanged` in one wave need only ONE deferred re-snapshot,
    // not N boxed closures. `scheduled` (owner-thread-only `Cell`) gates
    // a single `Box` post per drain; the closure clears it so the next
    // wave re-arms. Behaviour-equivalent (deferred-snapshot acceptable,
    // D243/D244) — one alloc + one snapshot per wave, not per emission.
    let scheduled = Rc::new(std::cell::Cell::new(false));
    let topo_sink: Rc<dyn Fn(&TopologyEvent)> = Rc::new(move |event: &TopologyEvent| {
        if matches!(event, TopologyEvent::DepsChanged { .. }) {
            if scheduled.get() {
                return; // already armed for this drain — coalesce.
            }
            // INVARIANT (QA, 2026-05-19): the `upgrade()` check runs
            // BEFORE `scheduled.set(true)`, so a graph-gone fire never
            // poisons the slot (`scheduled` stays `false`; a later
            // fire on the next wave re-tries the upgrade fresh).
            let Some(arc_inner) = weak_inner_topo.upgrade() else {
                return;
            };
            let s = sink_topo.clone();
            let sched = Rc::clone(&scheduled);
            sched.set(true);
            // The Defer closure captures no `HandleId` (only an
            // `Arc<sink>` + a `Weak`-upgraded inner) — if the Core
            // is gone (`false`) the snapshot simply won't fire;
            // nothing to release (D235 P8 pattern).
            let _ = deferred.post(Box::new(move |cf: &dyn CoreFull| {
                sched.set(false);
                s(&describe_of(cf, &arc_inner, None));
            }));
        }
    });
    let topo_sub_id = core.subscribe_topology(topo_sink);

    ReactiveDescribeHandle {
        inner: inner.clone(),
        ns_sink_id,
        topo_sub_id,
    }
}
