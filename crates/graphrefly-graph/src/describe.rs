//! `Graph::describe()` — JSON form of canonical spec §3.6 + Appendix B.
//!
//! S2b/β (D237/D240/D242/D243): describe logic is a free fn
//! [`describe_of`] over `(&dyn CoreFull, &Arc<Mutex<GraphInner>>)` so
//! both [`Graph`](crate::Graph) and
//! [`SubgraphRef`](crate::SubgraphRef) (via the
//! [`GraphOps`](crate::GraphOps) trait) reuse it, AND so the in-wave
//! reactive-describe `MailboxOp::Defer` closure (D233) can run it
//! through the `&dyn CoreFull` it is handed (D243 widened `CoreFull`
//! with read-only inspection). `ReactiveDescribeHandle<'g>` borrows the
//! root `&Core` (D242) so its RAII `Drop` synchronously unsubscribes
//! (D241-AMEND: the borrow statically pins the Core alive + the
//! handle is `!Send`, so this is sound — NOT the D225 relocating-Core
//! problem).
//!
//! # Value rendering — raw vs. binding-rendered
//!
//! Canonical TS surfaces `value: T` directly. The Rust port preserves
//! the handle-protocol cleaving plane (`value: DescribeValue`):
//! `Handle(HandleId)` raw u64 (default) or `Rendered(serde_json::Value)`
//! via [`DebugBindingBoundary`].

use std::sync::{Arc, Weak};

use graphrefly_core::{
    Core, CoreFull, HandleId, NodeId, NodeKind, OperatorOp, TerminalKind, TopologyEvent,
    TopologySubscriptionId, NO_HANDLE,
};
use indexmap::IndexMap;
use parking_lot::Mutex;
use serde::{Serialize, Serializer};

use crate::debug::DebugBindingBoundary;
use crate::graph::{register_ns_sink, GraphInner};

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
    inner_arc: &Arc<Mutex<GraphInner>>,
    debug: Option<&dyn DebugBindingBoundary>,
) -> GraphDescribeOutput {
    let (graph_name, local_names, subgraphs, names_iter) = {
        let inner = inner_arc.lock();
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

/// Sink type for reactive describe.
pub type DescribeSink = Arc<dyn Fn(&GraphDescribeOutput) + Send + Sync>;

/// RAII handle for a reactive describe subscription.
///
/// β/D242/D241-AMEND: carries the root `&'g Core` (borrow statically
/// pins the Core alive for `'g`; the handle is `!Send` so it drops on
/// the owner thread). `Drop` therefore **synchronously unsubscribes**
/// both the namespace sink (inner-only) and the Core topology
/// subscription — sound, not the D225 relocating-Core case.
#[must_use = "ReactiveDescribeHandle holds the subscription; dropping it unsubscribes"]
pub struct ReactiveDescribeHandle<'g> {
    core: &'g Core,
    inner: Arc<Mutex<GraphInner>>,
    ns_sink_id: u64,
    /// Slice V3 D5: Core topology sub for `DepsChanged` (edges change
    /// without a namespace change). D243: re-snapshot is in-wave
    /// `MailboxOp::Defer`'d (the topology event fires inside a Core
    /// wave; `describe_of` runs via the handed `&dyn CoreFull`).
    topo_sub_id: TopologySubscriptionId,
}

impl Drop for ReactiveDescribeHandle<'_> {
    fn drop(&mut self) {
        // Topology sub first (so a topo fire mid-unsubscribe can't
        // re-snapshot through a half-removed namespace sink), then the
        // namespace sink. Both synchronous + owner-thread (D241-AMEND).
        self.core.unsubscribe_topology(self.topo_sub_id);
        self.inner
            .lock()
            .namespace_sinks
            .shift_remove(&self.ns_sink_id);
    }
}

/// β: build a reactive-describe subscription. Push-on-subscribe fires
/// the current snapshot once, then re-fires on every namespace change
/// (owner-side `&Core`, D231) and on `set_deps` `DepsChanged`
/// (in-wave `MailboxOp::Defer` → `&dyn CoreFull`, D243).
pub(crate) fn describe_reactive_in<'g>(
    core: &'g Core,
    inner: &Arc<Mutex<GraphInner>>,
    sink: DescribeSink,
) -> ReactiveDescribeHandle<'g> {
    // Push-on-subscribe (no lock held).
    sink(&describe_of(core, inner, None));

    // Namespace-change path (owner-side `&Core`, β/D231).
    let weak_inner: Weak<Mutex<GraphInner>> = Arc::downgrade(inner);
    let sink_ns = sink.clone();
    let ns_sink: crate::graph::NamespaceChangeSink = Arc::new(move |c: &Core| {
        let Some(arc_inner) = weak_inner.upgrade() else {
            return;
        };
        sink_ns(&describe_of(c, &arc_inner, None));
    });
    let ns_sink_id = register_ns_sink(inner, ns_sink);

    // Topology path (set_deps → `DepsChanged`, fired inside a Core
    // wave): re-snapshot via an in-wave `MailboxOp::Defer` so it runs
    // owner-side with a real `&dyn CoreFull` (D243/D233).
    let weak_inner_topo: Weak<Mutex<GraphInner>> = Arc::downgrade(inner);
    let mailbox = core.mailbox();
    let sink_topo = sink.clone();
    let topo_sink: Arc<dyn Fn(&TopologyEvent) + Send + Sync> =
        Arc::new(move |event: &TopologyEvent| {
            if matches!(event, TopologyEvent::DepsChanged { .. }) {
                let Some(arc_inner) = weak_inner_topo.upgrade() else {
                    return;
                };
                let s = sink_topo.clone();
                // The Defer closure captures no `HandleId` (only an
                // `Arc<sink>` + a `Weak`-upgraded inner) — if the Core
                // is gone (`false`) the snapshot simply won't fire;
                // nothing to release (D235 P8 pattern).
                let _ = mailbox.post_defer(Box::new(move |cf: &dyn CoreFull| {
                    s(&describe_of(cf, &arc_inner, None));
                }));
            }
        });
    let topo_sub_id = core.subscribe_topology(topo_sink);

    ReactiveDescribeHandle {
        core,
        inner: inner.clone(),
        ns_sink_id,
        topo_sub_id,
    }
}
