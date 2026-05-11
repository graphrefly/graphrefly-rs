//! `Graph::describe()` — JSON form of canonical spec §3.6 + Appendix B.
//!
//! Static JSON form (Slice E+) + reactive describe (Slice F+). Pretty
//! / mermaid / d2 / stage-log / explain / reachable variants are
//! deferred (subsequent slices).
//!
//! # Value rendering — raw vs. binding-rendered (F sub-slice, 2026-05-10)
//!
//! Canonical TS surfaces `value: T` directly. The Rust port preserves
//! the handle-protocol cleaving plane (Core operates on opaque
//! `HandleId` integers; binding-side owns `HandleId → T`) by surfacing
//! `value: DescribeValue`:
//!
//! - `DescribeValue::Handle(HandleId)` — raw u64 view, used by
//!   `Graph::describe()` (the default). Suitable for parity tests
//!   that compare against TS by mapping handles through the binding
//!   manually, and for debug contexts that don't have a debug
//!   binding wired up.
//! - `DescribeValue::Rendered(serde_json::Value)` — binding-rendered
//!   view, used by `Graph::describe_with_debug(debug)`. The caller
//!   passes a [`DebugBindingBoundary`] impl that knows how to
//!   project each registered value into a JSON form. This is the
//!   "developer-friendly" surface — looks just like TS's `value: T`
//!   in the serialized JSON because the rendering happens at the
//!   binding boundary, off the Core hot path.
//!
//! Each value field serializes uniformly: as a u64 number for
//! `Handle`, or as the binding's chosen JSON shape for `Rendered`.
//! `None` (sentinel cache) serializes as `null` in both modes.

use std::sync::{Arc, Weak};

use graphrefly_core::{Core, HandleId, NodeId, NodeKind, TerminalKind, NO_HANDLE};
use indexmap::IndexMap;
use parking_lot::Mutex;
use serde::{Serialize, Serializer};

use crate::debug::DebugBindingBoundary;
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
    /// Current cache value (F sub-slice, 2026-05-10). `None` when
    /// the cache is sentinel (`NO_HANDLE`). Otherwise:
    ///
    /// - `DescribeValue::Handle(HandleId)` — raw u64 (from
    ///   [`Graph::describe`]).
    /// - `DescribeValue::Rendered(serde_json::Value)` — binding-
    ///   rendered (from [`Graph::describe_with_debug`]).
    ///
    /// Serialization is uniform: the inner u64 or JSON value
    /// appears directly in the output (no enum tag).
    pub value: Option<DescribeValue>,
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

/// Per-node cache value in `describe` output. Surfaced as `value:
/// <u64>` when produced by [`Graph::describe`] (raw handle view), or
/// as `value: <T>` when produced by
/// [`Graph::describe_with_debug`] (binding-rendered view). Serialized
/// uniformly without an enum tag — consumers see either a number
/// or whatever JSON shape the binding emits.
#[derive(Debug, Clone, PartialEq)]
pub enum DescribeValue {
    /// Raw handle view. Default for [`Graph::describe`]. The
    /// serialized JSON is a `Number` (the u64 raw view of the
    /// handle).
    Handle(HandleId),
    /// Binding-rendered view. Produced by
    /// [`Graph::describe_with_debug`] via the supplied
    /// [`DebugBindingBoundary`]. The serialized JSON is whatever
    /// shape the binding's `handle_to_debug` returned (string,
    /// number, object — fully under binding control).
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
    ///
    /// `value` fields serialize as raw u64 handles. Pass a
    /// [`DebugBindingBoundary`] to
    /// [`Self::describe_with_debug`](Self::describe_with_debug)
    /// instead if you want `value: T`-shaped output.
    #[must_use]
    pub fn describe(&self) -> GraphDescribeOutput {
        self.describe_inner(None)
    }

    /// Variant of [`Self::describe`] that renders each node's
    /// `value` via the supplied [`DebugBindingBoundary`].
    ///
    /// Useful when consuming `describe()` output to display values
    /// to humans (e.g., debugging UIs, log scrapers) — the JSON
    /// surfaces the binding's `T` shape rather than opaque u64
    /// handles.
    ///
    /// The trait is intentionally outside
    /// [`graphrefly_core::BindingBoundary`] so the hot-path FFI
    /// surface stays narrow. Bindings opt in by implementing both.
    /// Pre-1.0: bindings that don't ship `DebugBindingBoundary`
    /// simply force callers to use raw [`Self::describe`] (no
    /// fallback). See [`crate::debug`] for the trait's contract.
    #[must_use]
    pub fn describe_with_debug(&self, debug: &dyn DebugBindingBoundary) -> GraphDescribeOutput {
        self.describe_inner(Some(debug))
    }

    fn describe_inner(&self, debug: Option<&dyn DebugBindingBoundary>) -> GraphDescribeOutput {
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

            // F sub-slice (2026-05-10): pick raw vs binding-rendered
            // value. Sentinel cache (NO_HANDLE) → None regardless of
            // mode. Real handle: route through debug binding when
            // supplied, else surface raw.
            let value = if cache == NO_HANDLE {
                None
            } else if let Some(debug) = debug {
                Some(DescribeValue::Rendered(debug.handle_to_debug(cache)))
            } else {
                Some(DescribeValue::Handle(cache))
            };

            nodes.insert(
                name.clone(),
                NodeDescribe {
                    r#type: type_str_of(kind),
                    status: status_of(kind, cache, terminal, dirty, fired),
                    value,
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
