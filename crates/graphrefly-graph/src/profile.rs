//! `Graph::resource_profile` — snapshot-based runtime profile per
//! canonical R3.6.3 (`docs/implementation-plan-13.6-canonical-spec.md:984`).
//!
//! D285 (2026-05-24, cross-track-ledger §1 D283 row, lifts D004 R3.6.3
//! deferral). Mirrors pure-ts `Graph.resourceProfile` /
//! `graphProfile()` at `packages/pure-ts/src/graph/profile.ts` —
//! **minus the value-size fields** (`valueSizeBytes` per node,
//! `totalValueSizeBytes` aggregate, `hotspots.byValueSize`) per the
//! D284 `Impl`-contract amendment. Rust cache is a `HandleId` (u64);
//! user values `T` live in the binding-side registry, so a true size
//! requires a per-node FFI crossing — the amendment closed that field
//! class instead of forcing the speculative substrate widening (D196,
//! value-#6 pre-design win). See parity `types.ts` `ImplNodeProfile`
//! docstring for the canonical rationale.
//!
//! # Layering
//!
//! Pure read over `&dyn CoreFull` + the `Graph` namespace state — no
//! Core mutation. Walks `describe_of(core, &inner, None)` for topology
//! shape (canonical Appendix B JSON form) + `CoreFull::sink_count_of`
//! (D285 widening) for per-node subscriber counts. Orphan
//! categorization mirrors pure-ts at `profile.ts:129-138`.

use std::cell::RefCell;
use std::rc::Rc;

use graphrefly_core::CoreFull;
use serde::Serialize;

use crate::describe::{describe_of, NodeStatus, NodeTypeStr};
use crate::graph::{Graph, GraphInner};

/// Orphan classification for a [`NodeProfile`]. `None` when the node
/// has at least one subscriber, or when it is a `state` node (state
/// has no fn to be "idle" about — excluded by construction at
/// `profile.ts:129-138`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum OrphanKind {
    /// Effect node with zero subscribers — classic leak pattern
    /// (pre-existing class in pure-ts before the broader `idle-*`
    /// categorization).
    OrphanEffect,
    /// Derived node with zero subscribers — wasted compute path if it
    /// ever activates; often indicates a factory forgot keepalive.
    IdleDerived,
    /// Producer node with zero subscribers — no external consumer;
    /// often an over-eager factory or forgotten cleanup.
    IdleProducer,
}

/// Per-node profile entry. Mirrors the post-D284 narrower
/// [`packages/parity-tests/impls/types.ts`](../../../../../../graphrefly-ts/packages/parity-tests/impls/types.ts)
/// `ImplNodeProfile` shape exactly (no `valueSizeBytes`).
///
/// JSON field names are camelCase to match the cross-arm parity wire
/// contract (`subscriberCount` / `depCount` / `isOrphanEffect` /
/// `orphanKind`) — the parity scenarios in
/// `scenarios/graph/resource-profile.test.ts` assert on those keys.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeProfile {
    /// Qualified path within the graph (local — recursive mount-walking
    /// is not in scope per R3.6.3's snapshot framing).
    pub path: String,
    /// Node type: `"state"` / `"derived"` / `"dynamic"` / `"producer"` /
    /// `"effect"`.
    #[serde(rename = "type")]
    pub r#type: String,
    /// Lifecycle status as a string (matches canonical Appendix B).
    pub status: String,
    /// Number of downstream external subscribers (sinks added via
    /// `Core::subscribe`). Computed via [`CoreFull::sink_count_of`].
    pub subscriber_count: usize,
    /// Number of upstream dependencies (length of the node's `_deps`
    /// array; recovered from `describe()` output).
    pub dep_count: usize,
    /// True if this is an `effect` node with no external subscribers —
    /// the classic leak pattern (pre-existing pure-ts class kept
    /// separately from the broader `orphan_kind` categorization for
    /// back-compat with `orphan_effects` consumers).
    pub is_orphan_effect: bool,
    /// Orphan category — `None` when the node has subscribers OR when
    /// it is a `state` node (state has no fn ⇒ can never be "idle").
    pub orphan_kind: Option<OrphanKind>,
}

/// Aggregate profile returned by [`Graph::resource_profile`]. Mirrors
/// the post-D284 narrower
/// [`packages/parity-tests/impls/types.ts`](../../../../../../graphrefly-ts/packages/parity-tests/impls/types.ts)
/// `ImplGraphProfileResult` shape (no `totalValueSizeBytes`, no
/// `hotspots.byValueSize`). JSON keys are camelCase to match the
/// cross-arm parity wire contract.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GraphProfileResult {
    /// Total local node count (NOT recursive into mounted children).
    pub node_count: usize,
    /// Total local edge count.
    pub edge_count: usize,
    /// Local mounted subgraph count.
    pub subgraph_count: usize,
    /// All local node profiles in insertion order.
    pub nodes: Vec<NodeProfile>,
    /// Top-N hotspots by dimension. Each list is sorted descending +
    /// truncated to `top_n` (default 10).
    pub hotspots: Hotspots,
    /// Every orphan across types — `effect`, `derived`, `producer`
    /// with zero subscribers. See [`NodeProfile::orphan_kind`].
    pub orphans: Vec<NodeProfile>,
    /// Effect nodes with no external subscribers (legacy field; subset
    /// of [`Self::orphans`] kept separately for back-compat with the
    /// pure-ts `orphanEffects` consumer surface).
    pub orphan_effects: Vec<NodeProfile>,
}

/// Top-N hotspots returned by [`GraphProfileResult::hotspots`]. D284
/// dropped the `by_value_size` ranking (see [`crate::profile`] module
/// docstring for the rationale). JSON keys are camelCase
/// (`bySubscriberCount` / `byDepCount`) to match the cross-arm parity
/// wire contract.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Hotspots {
    /// Top nodes by external subscriber count, descending.
    pub by_subscriber_count: Vec<NodeProfile>,
    /// Top nodes by dependency count, descending.
    pub by_dep_count: Vec<NodeProfile>,
}

/// Options for [`Graph::resource_profile`].
#[derive(Debug, Clone, Copy, Default)]
pub struct GraphProfileOptions {
    /// Hotspot list cap (default 10 — matches pure-ts).
    pub top_n: Option<usize>,
}

impl Graph {
    /// Snapshot-based runtime profile per canonical R3.6.3 (D285).
    ///
    /// Walks the local namespace + mount tree once via
    /// [`describe_of`], queries `CoreFull::sink_count_of` per local
    /// node, computes orphan categorization + hotspot rankings. Pure
    /// read — no Core mutation.
    ///
    /// Mirrors the post-D284 narrower [`GraphProfileResult`] shape that
    /// drops value-size fields (see [`crate::profile`] module
    /// docstring).
    #[must_use]
    pub fn resource_profile(
        &self,
        core: &dyn CoreFull,
        opts: Option<GraphProfileOptions>,
    ) -> GraphProfileResult {
        resource_profile_of(core, &self.inner, opts)
    }
}

/// Free fn form for the rare case where a caller has a bare
/// `Rc<RefCell<GraphInner>>` (e.g. a future in-wave defer closure)
/// rather than a full [`Graph`] handle.
fn resource_profile_of(
    core: &dyn CoreFull,
    inner_arc: &Rc<RefCell<GraphInner>>,
    opts: Option<GraphProfileOptions>,
) -> GraphProfileResult {
    let top_n = opts.and_then(|o| o.top_n).unwrap_or(10);

    // Describe once for topology — local nodes, edges, subgraph names.
    let desc = describe_of(core, inner_arc, None);

    // Build name → NodeId reverse lookup so we can query
    // `sink_count_of(id)` per local node. The `describe_of` output
    // is keyed by name; the NodeIds live in `GraphInner.names`.
    let names_to_id: Vec<(String, graphrefly_core::NodeId)> = {
        let inner = inner_arc.borrow_mut();
        inner.names.iter().map(|(n, id)| (n.clone(), *id)).collect()
    };

    let mut profiles: Vec<NodeProfile> = Vec::with_capacity(names_to_id.len());

    for (path, node_id) in &names_to_id {
        // describe()'s `_anon_<id>` fallback path: a name that exists
        // in `names` but somehow missed describe — skip. Invariant-
        // unreachable for the post-D279 describe shape.
        let Some(node_desc) = desc.nodes.get(path) else {
            continue;
        };

        let type_str = node_type_str(node_desc.r#type);
        let status_str = node_status_str(node_desc.status);
        let subscriber_count = core.sink_count_of(*node_id);
        let dep_count = node_desc.deps.len();

        let is_orphan_effect = type_str == "effect" && subscriber_count == 0;
        let orphan_kind = if subscriber_count == 0 {
            match type_str {
                "effect" => Some(OrphanKind::OrphanEffect),
                "derived" | "dynamic" => Some(OrphanKind::IdleDerived),
                "producer" => Some(OrphanKind::IdleProducer),
                // `state` is excluded by construction — no fn to be
                // idle about. Mirrors pure-ts `profile.ts:129-138`.
                _ => None,
            }
        } else {
            None
        };

        profiles.push(NodeProfile {
            path: path.clone(),
            r#type: type_str.to_string(),
            status: status_str.to_string(),
            subscriber_count,
            dep_count,
            is_orphan_effect,
            orphan_kind,
        });
    }

    let top_by = |key: fn(&NodeProfile) -> usize| -> Vec<NodeProfile> {
        let mut sorted: Vec<NodeProfile> = profiles.clone();
        // Descending sort via `Reverse(key(n))`.
        sorted.sort_by_key(|n| std::cmp::Reverse(key(n)));
        sorted.truncate(top_n);
        sorted
    };

    let orphans: Vec<NodeProfile> = profiles
        .iter()
        .filter(|p| p.orphan_kind.is_some())
        .cloned()
        .collect();
    let orphan_effects: Vec<NodeProfile> = profiles
        .iter()
        .filter(|p| p.is_orphan_effect)
        .cloned()
        .collect();

    GraphProfileResult {
        node_count: profiles.len(),
        edge_count: desc.edges.len(),
        subgraph_count: desc.subgraphs.len(),
        hotspots: Hotspots {
            by_subscriber_count: top_by(|p| p.subscriber_count),
            by_dep_count: top_by(|p| p.dep_count),
        },
        orphans,
        orphan_effects,
        nodes: profiles,
    }
}

fn node_type_str(t: NodeTypeStr) -> &'static str {
    match t {
        NodeTypeStr::State => "state",
        NodeTypeStr::Derived => "derived",
        NodeTypeStr::Dynamic => "dynamic",
        NodeTypeStr::Producer => "producer",
        NodeTypeStr::Effect => "effect",
        NodeTypeStr::Operator => "operator",
    }
}

fn node_status_str(s: NodeStatus) -> &'static str {
    match s {
        NodeStatus::Sentinel => "sentinel",
        NodeStatus::Pending => "pending",
        NodeStatus::Dirty => "dirty",
        NodeStatus::Settled => "settled",
        NodeStatus::Resolved => "resolved",
        NodeStatus::Completed => "completed",
        NodeStatus::Errored => "errored",
    }
}
