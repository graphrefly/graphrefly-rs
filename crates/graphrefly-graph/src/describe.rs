//! `Graph::describe()` — JSON form of canonical spec §3.6 + Appendix B.
//!
//! Static JSON form (Slice E+) + reactive describe (Slice F+). Pretty
//! / mermaid / d2 / stage-log / explain / reachable variants are
//! deferred (subsequent slices).
//!
//! # Value rendering divergence (TS spec)
//!
//! Canonical TS surfaces `value: T` directly. The Rust port surfaces
//! `value: Option<HandleId>` — Core operates on opaque `HandleId`
//! integers, and the binding-side registry is the only place
//! `HandleId → T` resolution happens. Bindings (`graphrefly-bindings-js`,
//! `graphrefly-bindings-py`) provide a thin wrapper that swaps each
//! handle for the registered value before serializing for end-user
//! consumption. Documented divergence per §11 Implementation Deltas
//! (handle-protocol cleaving plane).

use std::sync::{Arc, Weak};

use graphrefly_core::{Core, HandleId, NodeId, NodeKind, TerminalKind, NO_HANDLE};
use indexmap::IndexMap;
use parking_lot::Mutex;
use serde::{Serialize, Serializer};

use crate::graph::{Graph, GraphInner};

/// Top-level `describe()` output (canonical Appendix B JSON schema).
///
/// `nodes` is insertion-ordered (matches namespace registration
/// order) — load-bearing for stable serialized output.
#[derive(Debug, Clone, Serialize)]
pub struct GraphDescribeOutput {
    /// Graph name as set at construction / mount.
    pub name: String,
    /// Local nodes by name.
    pub nodes: IndexMap<String, NodeDescribe>,
    /// Local edges (dep → consumer).
    pub edges: Vec<EdgeDescribe>,
    /// Mounted child names (recurse via `Graph::node(child).describe()`).
    pub subgraphs: Vec<String>,
}

/// Per-node descriptor.
#[derive(Debug, Clone, Serialize)]
pub struct NodeDescribe {
    /// `"state"` / `"derived"` / `"dynamic"` / `"producer"`.
    /// Producer-vs-state inference: a state node with no fn-id but
    /// `has_fired_once=true` may stem from a producer pattern; the
    /// rust-side classifier just reports `kind` directly. (Producer
    /// inference is a binding-side concern — see canonical §3.6.1.)
    #[serde(rename = "type")]
    pub r#type: NodeTypeStr,
    /// Lifecycle status (canonical Appendix B enum).
    pub status: NodeStatus,
    /// Raw handle of the node's current cache. `None` when the cache
    /// is sentinel (`NO_HANDLE`). Bindings render to `T` before
    /// surfacing to end users.
    #[serde(serialize_with = "ser_opt_handle")]
    pub value: Option<HandleId>,
    /// Dep names in declaration order. Unnamed deps surface as
    /// `_anon_<NodeId>` to keep the output lossless without
    /// elevating Core-only nodes into the namespace.
    pub deps: Vec<String>,
    /// Free-form metadata per canonical Appendix B (e.g. `{
    /// "description": "...", "type": "integer", "range": [1, 10] }`).
    /// Always `None` in this slice — the metadata-storage primitive
    /// on Core hasn't shipped yet. Reserved as `Option<serde_json::Value>`
    /// so the JSON shape stays forward-compatible (omitted via
    /// `skip_serializing_if` when None to keep current outputs slim).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<serde_json::Value>,
}

/// Edge between two named nodes (or a named node and an anonymous
/// dep, surfaced as `_anon_<NodeId>`).
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
    /// Reserved for future producer-pattern classification — the Rust
    /// port doesn't infer this kind today; emitted only when the
    /// binding side has annotated it.
    Producer,
    /// Reserved for future side-effect classification. Same caveat
    /// as `Producer`.
    Effect,
    /// Reserved for the operator catalog when M3 lands.
    Operator,
}

/// Canonical Appendix B `status` enum.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NodeStatus {
    /// State node with sentinel cache (never had a value).
    Sentinel,
    /// Compute node that has not yet fired (first-run gate not satisfied).
    Pending,
    /// DIRTY queued; tier-3 settle has not flushed yet.
    Dirty,
    /// Has a value, no terminal, no DIRTY pending.
    Settled,
    /// Same as `Settled` for static descriptors — wave-internal
    /// "resolved-this-wave" doesn't survive flush. Reserved for
    /// reactive-describe later.
    Resolved,
    /// Terminated via `[COMPLETE]`.
    Completed,
    /// Terminated via `[ERROR, h]`.
    Errored,
}

impl Graph {
    /// Snapshot the graph's topology + lifecycle state. JSON form only
    /// in this slice (see module docs).
    #[must_use]
    pub fn describe(&self) -> GraphDescribeOutput {
        let inner = self.inner.lock();
        let graph_name = inner.name.clone();
        let local_names: IndexMap<NodeId, String> = inner
            .names
            .iter()
            .map(|(name, id)| (*id, name.clone()))
            .collect();
        let subgraphs: Vec<String> = inner.children.keys().cloned().collect();
        let names_iter: Vec<(String, NodeId)> =
            inner.names.iter().map(|(n, id)| (n.clone(), *id)).collect();
        drop(inner);

        let mut nodes: IndexMap<String, NodeDescribe> = IndexMap::new();
        let mut edges: Vec<EdgeDescribe> = Vec::new();

        for (name, id) in &names_iter {
            let kind = self.core.kind_of(*id).unwrap_or(NodeKind::State);
            let cache = self.core.cache_of(*id);
            let terminal = self.core.is_terminal(*id);
            let dirty = self.core.is_dirty(*id);
            let fired = self.core.has_fired_once(*id);

            let dep_ids = self.core.deps_of(*id);
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

            nodes.insert(
                name.clone(),
                NodeDescribe {
                    r#type: type_str_of(kind),
                    status: status_of(kind, cache, terminal, dirty, fired),
                    value: if cache == NO_HANDLE {
                        None
                    } else {
                        Some(cache)
                    },
                    deps: dep_names,
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
}

/// Serialize `Option<HandleId>` as `null` or its raw u64.
///
/// Takes `&Option<T>` not `Option<&T>` because `serde`'s
/// `serialize_with` API mandates the former signature.
#[allow(clippy::ref_option)]
fn ser_opt_handle<S: Serializer>(value: &Option<HandleId>, ser: S) -> Result<S::Ok, S::Error> {
    match value {
        Some(h) => ser.serialize_some(&h.raw()),
        None => ser.serialize_none(),
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

/// Canonical-spec §3.6.1 status mapping.
///
/// Precedence (high to low): `errored` > `completed` > `dirty` >
/// (cache-cleared discriminator) > (`settled` if `cache != NO_HANDLE`)
/// > (`pending` for unfired compute) > (`sentinel` for state).
///
/// # R1.3.7.b post-INVALIDATE classification (Slice F, A8 — 2026-05-07)
///
/// Per canonical R1.3.7.b: "The emitting node's status transitions to
/// 'sentinel' (no value, nothing pending) — NOT 'dirty' (value about to
/// change) — because INVALIDATE has cleared the cache outright with no new
/// value pending."
///
/// Implementation: a *fired* compute node with `cache == NO_HANDLE` and no
/// terminal and no DIRTY pending has been `INVALIDATE`-d (the only path that
/// clears the cache without setting a terminal). Report `Sentinel`, NOT
/// `Settled` (the prior bug). State nodes use the same logic — `cache == NO_HANDLE`
/// always means `Sentinel` regardless of `fired`.
///
/// # Reactive-describe note
///
/// When both `terminal.is_some()` AND `dirty == true` (a wave that began
/// before the terminal was installed and still has unflushed tier-1 traffic),
/// this static classifier reports the terminal status. Reactive describe will
/// need a `terminating` substate to surface the unflushed wave — not modeled
/// here because the static walk happens between waves in practice.
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
    // R1.3.7.b: `cache == NO_HANDLE` discriminates Sentinel vs Settled
    // BEFORE the `fired` check, so post-INVALIDATE on fired compute nodes
    // correctly reports `Sentinel` (was incorrectly `Settled` pre-A8).
    if cache == NO_HANDLE {
        return match kind {
            NodeKind::State => NodeStatus::Sentinel,
            NodeKind::Producer | NodeKind::Derived | NodeKind::Dynamic | NodeKind::Operator(_) => {
                if fired {
                    // Compute node that previously fired but currently has
                    // sentinel cache → INVALIDATE wiped it. R1.3.7.b says
                    // status is `sentinel`, not `pending` (pending = first-fire
                    // gate not yet satisfied).
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

/// Sink type for reactive describe — receives a fresh `GraphDescribeOutput`
/// on every namespace change.
pub type DescribeSink = Arc<dyn Fn(&GraphDescribeOutput) + Send + Sync>;

/// RAII handle for a reactive describe subscription. Dropping it stops
/// the namespace listener and frees the describe-sink.
///
/// The reactive describe fires synchronously from Graph-level
/// namespace mutations (`add`, `remove`, `destroy`, `mount`,
/// `unmount`, and the cascaded teardowns of `core.teardown`). Each
/// fire re-snapshots the full `Graph::describe()` and delivers it
/// to the sink.
#[must_use = "ReactiveDescribeHandle holds the subscription; dropping it unsubscribes"]
pub struct ReactiveDescribeHandle {
    graph: Graph,
    ns_sink_id: u64,
}

impl Drop for ReactiveDescribeHandle {
    fn drop(&mut self) {
        self.graph.unsubscribe_namespace_change(self.ns_sink_id);
    }
}

// Send + Sync compile-time assertion.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<ReactiveDescribeHandle>();
};

impl Graph {
    /// Subscribe to live topology snapshots. The sink fires immediately
    /// with the current [`GraphDescribeOutput`] (push-on-subscribe per
    /// canonical §2.5.2 / R3.6.1) and then again with a fresh snapshot
    /// every time a node is added, removed, mounted, unmounted, or the
    /// graph is destroyed.
    ///
    /// Returns a [`ReactiveDescribeHandle`] — dropping it unsubscribes.
    ///
    /// This is the `reactive: true` mode from canonical §3.6.1. The
    /// `reactive: "diff"` (changeset) mode is deferred to Phase 14.
    ///
    /// Note: `set_deps` topology changes fire via Core's topology
    /// primitive, not this Graph-level namespace hook. If callers also
    /// need `set_deps` notifications, compose with
    /// [`graphrefly_core::Core::subscribe_topology`].
    ///
    /// The sink captures only a [`Weak`] reference to the graph's inner
    /// state, so the `namespace_sinks` → sink → Graph → `namespace_sinks`
    /// Arc cycle is broken at the sink edge (see P6 in the Slice F /qa
    /// closing notes).
    pub fn describe_reactive(&self, sink: DescribeSink) -> ReactiveDescribeHandle {
        // Push-on-subscribe: fire current snapshot once before installing
        // the listener. Sink runs without any Graph lock held.
        sink(&self.describe());

        // Capture Weak<inner> + Core (clone) to break the
        // namespace_sinks → sink → Graph → namespace_sinks Arc cycle.
        // If the user leaks the handle, the graph still drops cleanly
        // because the sink's Weak ref does not keep `inner` alive.
        let weak_inner: Weak<Mutex<GraphInner>> = Arc::downgrade(&self.inner);
        let core: Core = self.core.clone();
        let ns_sink = Arc::new(move || {
            let Some(arc_inner) = weak_inner.upgrade() else {
                // Graph dropped; silent no-op.
                return;
            };
            let graph = Graph {
                core: core.clone(),
                inner: arc_inner,
            };
            let snapshot = graph.describe();
            sink(&snapshot);
        });
        let ns_sink_id = self.subscribe_namespace_change(ns_sink);
        ReactiveDescribeHandle {
            graph: self.clone(),
            ns_sink_id,
        }
    }
}
