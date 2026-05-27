//! `snapshot()` / `restore()` / `Graph::from_snapshot()` — portable
//! serialization of graph state (M4.E1, R3.8).
//!
//! D246: `snapshot`/`restore`/`from_snapshot` are inherent [`Graph`]
//! methods over the Core-free namespace tree, taking the embedder's
//! `&Core` explicitly (D246 rule 2). `snapshot_of` is generic over
//! `&dyn CoreFull` (the one facade) so the storage in-wave
//! `MailboxOp::Defer` observe-sink can run it (read-only;
//! `serialize_handle` delegates to the binding). No `SubgraphRef`/
//! `GraphOps`/`SnapshotOps` — one `Graph`, plain free fns.
//!
//! # Handle-protocol boundary
//!
//! `snapshot()` calls `BindingBoundary::serialize_handle`; `restore()`
//! / `from_snapshot()` call `BindingBoundary::deserialize_value`.
//! Per D169 edges are omitted (derived from deps via `edges()`).

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use graphrefly_core::{BindingBoundary, Core, CoreFull, NodeId, NodeKind, TerminalKind, NO_HANDLE};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::graph::{resolve_checked, Graph, GraphInner, PATH_SEP};

/// Portable snapshot of a graph's state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphPersistSnapshot {
    /// Graph name as set at construction / mount.
    pub name: String,
    /// Per-node state by local name, in namespace insertion order.
    pub nodes: IndexMap<String, NodeSlice>,
    /// Mounted subgraph snapshots, keyed by mount name.
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub subgraphs: IndexMap<String, GraphPersistSnapshot>,
}

/// Per-node state within a snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSlice {
    /// `"state"` / `"derived"` / `"dynamic"` / `"producer"` / `"operator"`.
    #[serde(rename = "type")]
    pub node_type: String,
    /// Serialized cache value. `None` when sentinel.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>,
    /// Node lifecycle status.
    pub status: NodeSnapshotStatus,
    /// Dependency names in declaration order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deps: Vec<String>,
}

/// Lifecycle status stored in a snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum NodeSnapshotStatus {
    /// Never emitted DATA.
    Sentinel,
    /// Has emitted at least one DATA.
    Live,
    /// Terminal: COMPLETE.
    Completed,
    /// Terminal: ERROR (carries the serialized error value).
    Errored {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<serde_json::Value>,
    },
}

/// Errors from [`Graph::restore`] and [`Graph::from_snapshot`].
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("snapshot name `{expected}` does not match graph name `{actual}`")]
    NameMismatch { expected: String, actual: String },
    #[error("node `{0}` in snapshot not found in graph namespace")]
    UnknownNode(String),
    #[error("subgraph `{0}` in snapshot not found in graph mount tree")]
    UnknownSubgraph(String),
    #[error("auto-hydration: unresolvable deps for node `{0}` (deps: {1:?})")]
    UnresolvableDeps(String, Vec<String>),
    #[error("auto-hydration: no factory registered for node type `{0}` (node `{1}`)")]
    MissingFactory(String, String),
    /// D279 (2026-05-22, E-ii.1): a state node in the snapshot collides
    /// with an existing child mount name on the owner graph at decode
    /// time. Raised by Pass 1's pre-validation BEFORE any Core mutation
    /// — prevents the orphan-`NodeId` leak that pre-D279 occurred when
    /// `Graph::state` registered a `NodeId` before the namespace `add`
    /// returned `NameError::Collision`. `graph_path` is the owner
    /// graph's tree-relative path (empty string for the root).
    #[error("snapshot decode: state node `{name}` at graph `{graph_path}` collides with an existing child mount of the same name")]
    NameCollision { name: String, graph_path: String },
}

/// Factory for auto-hydration mode. D246: receives the embedder's
/// `&Core` + the Core-free [`Graph`] handle.
pub type NodeFactory =
    Box<dyn Fn(&Core, &Graph, &str, &NodeSlice, &[NodeId]) -> Result<NodeId, SnapshotError>>;

/// Builder function for `Graph::from_snapshot` builder mode (D246:
/// `&Core` + Core-free [`Graph`]).
pub type SnapshotBuilder = Box<dyn FnOnce(&Core, &Graph)>;

/// D246: recursive snapshot over `(&dyn CoreFull, &Rc<RefCell<GraphInner>>)`
/// — `&dyn CoreFull` (the one facade) so the storage in-wave
/// `MailboxOp::Defer` observe-sink can run it (read-only;
/// `serialize_handle` delegates to the binding).
///
/// **D276 (cross-mount deps):** the encoder pre-computes a tree-wide
/// `id_to_tree_path: HashMap<NodeId, String>` covering every named
/// node in the snapshot tree. For each dep the encoder emits an
/// **owner-relative path** using [`PATH_SEP`] (`"::"`) and `".."`
/// segments — the same syntax accepted by [`Graph::try_resolve`]:
///
/// - same-graph dep → bare local name (back-compat with snapshots
///   produced by callers that never crossed a mount boundary).
/// - cross-mount dep down → `"child::name"` / `"child::nested::name"`.
/// - cross-mount dep up → `"..::name"`, `"..::..::name"`, …
/// - cross-mount sibling → `"..::sibling::name"`.
///
/// Pre-D276 the encoder fell through to `"_anon_<rawid>"` for any
/// dep whose `NodeId` wasn't in the LOCAL graph's `names` map,
/// destroying every cross-mount reference at serialization time.
/// The new owner-relative encoding round-trips through
/// `Graph::from_snapshot`'s tree-wide hydration; the decoder
/// resolves dep names via [`Graph::try_resolve`] on the owner graph,
/// reusing Slice V3's cross-subgraph path machinery.
pub(crate) fn snapshot_of(
    core: &dyn CoreFull,
    inner_arc: &Rc<RefCell<GraphInner>>,
) -> GraphPersistSnapshot {
    let id_to_tree_path = build_id_to_tree_path(inner_arc);
    snapshot_of_with_tree_paths(core, inner_arc, &id_to_tree_path, "")
}

/// D276: walk the entire mount tree under `root` and build a
/// `NodeId → absolute_path` map. Absolute paths are **relative to
/// the snapshot root** (no leading root name) and use [`PATH_SEP`]
/// (`"::"`) — the same syntax accepted by [`Graph::try_resolve`].
fn build_id_to_tree_path(root: &Rc<RefCell<GraphInner>>) -> HashMap<NodeId, String> {
    let mut map = HashMap::new();
    walk_tree_paths(root, "", &mut map);
    map
}

fn walk_tree_paths(
    inner_arc: &Rc<RefCell<GraphInner>>,
    path_prefix: &str,
    out: &mut HashMap<NodeId, String>,
) {
    let inner = inner_arc.borrow_mut();
    let names: Vec<(String, NodeId)> = inner.names.iter().map(|(n, &id)| (n.clone(), id)).collect();
    let children: Vec<(String, Rc<RefCell<GraphInner>>)> = inner
        .children
        .iter()
        .map(|(n, g)| (n.clone(), g.clone()))
        .collect();
    drop(inner);
    for (name, id) in &names {
        let abs_path = if path_prefix.is_empty() {
            name.clone()
        } else {
            format!("{path_prefix}{PATH_SEP}{name}")
        };
        out.insert(*id, abs_path);
    }
    for (child_name, child_inner) in &children {
        let child_prefix = if path_prefix.is_empty() {
            child_name.clone()
        } else {
            format!("{path_prefix}{PATH_SEP}{child_name}")
        };
        walk_tree_paths(child_inner, &child_prefix, out);
    }
}

/// D276 helper — convert a dep's absolute path to a path relative
/// to the owner graph. Same-graph deps collapse to a bare local
/// name (back-compat); cross-mount deps use `".."`/`"name"`
/// segments separated by [`PATH_SEP`].
///
/// Examples (`owner_path` → `abs_path` → result):
///
/// - `""` → `"a"` → `"a"` (root local)
/// - `"child"` → `"child::b"` → `"b"` (same-graph local)
/// - `""` → `"child::b"` → `"child::b"` (descend)
/// - `"child"` → `"a"` → `"..::a"` (ascend to root)
/// - `"child::nested"` → `"a"` → `"..::..::a"` (ascend two)
/// - `"child::a"` → `"other::b"` → `"..::other::b"` (sibling)
fn absolute_to_owner_relative(owner_path: &str, abs_path: &str) -> String {
    // /qa G2.7 (2026-05-22): self-dep guard. graphrefly rejects self-deps at
    // registration (`SetDepsError::SelfDep`), so a healthy `Core::deps_of`
    // never yields the owning node's own id. If a snapshot is hand-
    // constructed pathologically OR an operator-internal NodeId is reused
    // structurally as both owner and dep, computing `owner_path == abs_path`
    // here would emit `""` — which `Graph::try_resolve("")` rejects with
    // `PathError::Empty`, eventually surfacing as `UnresolvableDeps` from
    // the decode retry loop. Catch it loudly at encode in debug builds.
    debug_assert_ne!(
        owner_path, abs_path,
        "D276 invariant: self-deps are rejected at registration; \
         encoding a dep whose absolute path equals the owner's would emit \
         an empty relative path"
    );
    let owner_segs: Vec<&str> = if owner_path.is_empty() {
        Vec::new()
    } else {
        owner_path.split(PATH_SEP).collect()
    };
    let abs_segs: Vec<&str> = if abs_path.is_empty() {
        Vec::new()
    } else {
        abs_path.split(PATH_SEP).collect()
    };
    let mut common = 0;
    while common < owner_segs.len()
        && common < abs_segs.len()
        && owner_segs[common] == abs_segs[common]
    {
        common += 1;
    }
    let up_count = owner_segs.len() - common;
    let down_segs = &abs_segs[common..];
    if up_count == 0 {
        return down_segs.join(PATH_SEP);
    }
    let mut parts: Vec<&str> = vec![".."; up_count];
    parts.extend(down_segs);
    parts.join(PATH_SEP)
}

fn snapshot_of_with_tree_paths(
    core: &dyn CoreFull,
    inner_arc: &Rc<RefCell<GraphInner>>,
    id_to_tree_path: &HashMap<NodeId, String>,
    owner_path: &str,
) -> GraphPersistSnapshot {
    let (name, node_entries, children, id_to_name) = {
        let inner = inner_arc.borrow_mut();
        let name = inner.name.clone();
        let node_entries: Vec<(String, NodeId)> =
            inner.names.iter().map(|(n, &id)| (n.clone(), id)).collect();
        let children: Vec<(String, Rc<RefCell<GraphInner>>)> = inner
            .children
            .iter()
            .map(|(n, g)| (n.clone(), g.clone()))
            .collect();
        let id_to_name: IndexMap<NodeId, String> =
            inner.names.iter().map(|(n, &id)| (id, n.clone())).collect();
        (name, node_entries, children, id_to_name)
    };

    let mut nodes = IndexMap::new();

    for (node_name, node_id) in &node_entries {
        let kind = core.kind_of(*node_id);
        let node_type = match kind {
            Some(NodeKind::State) => "state",
            Some(NodeKind::Derived) => "derived",
            Some(NodeKind::Dynamic) => "dynamic",
            Some(NodeKind::Producer) => "producer",
            Some(NodeKind::Operator(_)) => "operator",
            None => "unknown",
        };

        let cache = core.cache_of(*node_id);
        let value = if cache == NO_HANDLE {
            None
        } else {
            core.serialize_handle(cache)
        };

        let terminal = core.is_terminal(*node_id);
        let status = match terminal {
            Some(TerminalKind::Complete) => NodeSnapshotStatus::Completed,
            Some(TerminalKind::Error(err_handle)) => NodeSnapshotStatus::Errored {
                error: core.serialize_handle(err_handle),
            },
            None => {
                if core.has_fired_once(*node_id) || cache != NO_HANDLE {
                    NodeSnapshotStatus::Live
                } else {
                    NodeSnapshotStatus::Sentinel
                }
            }
        };

        // D276: dep-name encoding — 3 tiers, all owner-relative:
        // (1) same-graph dep → bare local name (back-compat — pre-D276
        //     snapshots used this shape exclusively).
        // (2) cross-mount dep that IS named somewhere in the snapshot
        //     tree → owner-relative path via [`PATH_SEP`] + `".."`
        //     segments (resolves via `Graph::try_resolve` on decode).
        // (3) anonymous dep (operator-internal NodeId with no name in
        //     ANY graph) → `_anon_<rawid>` fallback (pre-D276 behavior,
        //     unchanged; decode still fails with `UnresolvableDeps`
        //     for these — not in M4.E1 scope).
        //
        // D301 B.b (Q4 sub-decision, 2026-05-26): persistence-vs-
        // presentation distinction — snapshot encode KEEPS the
        // `_anon_<rawid>` marker while describe (`describe.rs:229`,
        // `graph.rs:1055`) converges to empty-string for TS parity.
        // Rationale: the marker carries real consumer value at decode
        // time. `SnapshotError::UnresolvableDeps` (snapshot.rs:84)
        // formats as `"unresolvable deps for node `{0}` (deps: {1:?})"`
        // — `{1:?}` is Debug-format of `Vec<String>`, preserving each
        // `_anon_<rawid>` verbatim. Converging snapshot to `""` would
        // degrade the diagnostic to `(deps: ["", ""])` — indistinguish-
        // able collisions for multiple unresolvable anon deps.
        // Describe is a presentation surface (cross-arm wire-shape
        // parity matters more than per-NodeId disambiguation); snapshot
        // is a persistence surface (decode-time diagnostic fidelity
        // matters more than wire-shape parity to TS — which has its
        // own analogous gap on the TS-snapshot side).
        let dep_ids = core.deps_of(*node_id);
        let deps: Vec<String> = dep_ids
            .iter()
            .map(|dep_id| {
                if let Some(local_name) = id_to_name.get(dep_id) {
                    local_name.clone()
                } else if let Some(tree_path) = id_to_tree_path.get(dep_id) {
                    absolute_to_owner_relative(owner_path, tree_path)
                } else {
                    format!("_anon_{}", dep_id.raw())
                }
            })
            .collect();

        nodes.insert(
            node_name.clone(),
            NodeSlice {
                node_type: node_type.to_owned(),
                value,
                status,
                deps,
            },
        );
    }

    let mut subgraphs = IndexMap::new();
    for (child_name, child_inner) in children {
        let child_owner_path = if owner_path.is_empty() {
            child_name.clone()
        } else {
            format!("{owner_path}{PATH_SEP}{child_name}")
        };
        subgraphs.insert(
            child_name,
            snapshot_of_with_tree_paths(core, &child_inner, id_to_tree_path, &child_owner_path),
        );
    }

    GraphPersistSnapshot {
        name,
        nodes,
        subgraphs,
    }
}

/// Recursive restore over `(&Core, &Rc<RefCell<GraphInner>>)`.
fn restore_into(
    core: &Core,
    inner_arc: &Rc<RefCell<GraphInner>>,
    snapshot: &GraphPersistSnapshot,
) -> Result<(), SnapshotError> {
    let graph_name = inner_arc.borrow_mut().name.clone();
    if snapshot.name != graph_name {
        return Err(SnapshotError::NameMismatch {
            expected: snapshot.name.clone(),
            actual: graph_name,
        });
    }

    let binding = core.binding_ptr();

    for (node_name, slice) in &snapshot.nodes {
        let node_id = resolve_checked(inner_arc, node_name)
            .ok()
            .flatten()
            .ok_or_else(|| SnapshotError::UnknownNode(node_name.clone()))?;

        if slice.node_type == "state" {
            if let Some(ref value) = slice.value {
                let handle = binding.deserialize_value(value.clone());
                core.emit(node_id, handle);
            }
        }

        match &slice.status {
            NodeSnapshotStatus::Completed => {
                core.complete(node_id);
            }
            NodeSnapshotStatus::Errored { error } => {
                if let Some(err_val) = error {
                    let err_handle = binding.deserialize_value(err_val.clone());
                    core.error(node_id, err_handle);
                }
            }
            NodeSnapshotStatus::Sentinel | NodeSnapshotStatus::Live => {}
        }
    }

    let child_pairs: Vec<(String, Rc<RefCell<GraphInner>>)> = {
        let inner = inner_arc.borrow_mut();
        snapshot
            .subgraphs
            .keys()
            .map(|name| {
                let child = inner
                    .children
                    .get(name)
                    .ok_or_else(|| SnapshotError::UnknownSubgraph(name.clone()))?;
                Ok((name.clone(), child.clone()))
            })
            .collect::<Result<Vec<_>, SnapshotError>>()?
    };
    for (child_name, child_inner) in child_pairs {
        restore_into(core, &child_inner, &snapshot.subgraphs[&child_name])?;
    }

    Ok(())
}

impl Graph {
    /// Serialize this graph's state into a portable snapshot.
    ///
    /// # Concurrent-mutation caveat (torn read; M4.E1 / D167)
    ///
    /// `snapshot()` is a **point-in-time best-effort capture**, not an
    /// isolated read. The implementation holds the graph's inner lock
    /// for the namespace walk (collect names + child mounts), then
    /// drops it before per-node `core.cache_of` / `core.is_terminal`
    /// queries. If another thread (or a re-entrant wave) mutates state
    /// during the post-walk phase, the snapshot may capture a mix of
    /// pre- and post-mutation values for different nodes — individual
    /// node slices are internally consistent, but the cross-node
    /// composition is not transaction-isolated.
    ///
    /// The TS impl has the same semantics. No user has requested
    /// snapshot-level isolation. If you need a consistent cross-node
    /// view, the supported pattern is:
    ///
    /// - Wrap the snapshot call in [`Core::batch`] (drains the wave
    ///   before `snapshot()` returns; subsequent emissions wait).
    /// - OR call `graph.signal(SignalKind::Pause(lock))` first, then
    ///   `snapshot()`, then `Resume(lock)` — explicitly freezes the
    ///   reactive layer for the duration.
    ///
    /// A future copy-on-write epoch / snapshot-under-lock would close
    /// the torn-read window at the cost of holding the Core lock for
    /// the full serialization walk; gated on D196 consumer-pressure
    /// (no scenario today justifies the lock-contention trade).
    #[must_use]
    pub fn snapshot(&self, core: &Core) -> GraphPersistSnapshot {
        snapshot_of(core, &self.inner)
    }

    /// [`Self::snapshot`] over the one object-safe facade (D246 rule 5)
    /// — for the storage in-wave `MailboxOp::Defer(|cf: &dyn CoreFull|)`
    /// path, which only has a `&dyn CoreFull` (not a concrete `&Core`).
    /// Read-only; `serialize_handle` delegates to the binding.
    ///
    /// Inherits the same concurrent-mutation caveat as [`Self::snapshot`].
    #[must_use]
    pub fn snapshot_full(&self, core: &dyn CoreFull) -> GraphPersistSnapshot {
        snapshot_of(core, &self.inner)
    }

    /// Restore state from a snapshot into this existing graph.
    ///
    /// # Errors
    /// `NameMismatch` if names differ; `UnknownNode`/`UnknownSubgraph`
    /// for snapshot entries absent from the graph.
    pub fn restore(
        &self,
        core: &Core,
        snapshot: &GraphPersistSnapshot,
    ) -> Result<(), SnapshotError> {
        restore_into(core, &self.inner, snapshot)
    }

    /// Reconstruct a graph from a snapshot. **Builder mode**
    /// (`builder = Some`): build topology then `restore()` values.
    /// **Auto-hydration** (`builder = None`): reconstruct topology +
    /// state from the snapshot via `factories` (state nodes need none).
    ///
    /// D246: the embedder owns the `Core` (see
    /// [`graphrefly_core::OwnedCore`]) and passes it in; the binding is
    /// `core.binding_ptr()`.
    ///
    /// # Errors
    /// `UnresolvableDeps` if auto-hydration can't resolve a node's
    /// deps; `MissingFactory` for a non-state node type with no factory.
    pub fn from_snapshot(
        core: &Core,
        snapshot: &GraphPersistSnapshot,
        builder: Option<SnapshotBuilder>,
        factories: Option<IndexMap<String, NodeFactory>>,
    ) -> Result<Self, SnapshotError> {
        let graph = Graph::new(&snapshot.name);
        let binding: Arc<dyn BindingBoundary> = core.binding();

        if let Some(build_fn) = builder {
            build_fn(core, &graph);
            graph.restore(core, snapshot)?;
            return Ok(graph);
        }

        let factories = factories.unwrap_or_default();

        // D276 tree-wide hydration — replaces the pre-D276 single-pass
        // per-graph `hydrate_subgraph` / `hydrate_nodes` recursion.
        // Four passes:
        //
        //   Pass 0 — mount tree: recursively `mount_new` every
        //            subgraph; record `(absolute_path → Graph)` so
        //            later passes can address each owning graph.
        //   Pass 1 — state-first: walk tree creating ALL state nodes
        //            (no deps to resolve). Each state node is
        //            registered in its owner graph's `names`, so
        //            subsequent passes can locate it via
        //            [`Graph::try_resolve`].
        //   Pass 2 — derived: collect ALL non-state nodes across the
        //            tree into one queue. Run ONE shared retry loop;
        //            dep names resolve via the owner graph's
        //            `try_resolve`, which natively handles owner-
        //            relative paths with `".."` and `"::"` segments
        //            (Slice V3 cross-subgraph path machinery).
        //   Pass 3 — status restore: walk tree, apply
        //            `Completed` / `Errored` via the same
        //            `try_resolve` lookup.
        //
        // Back-compat: snapshots produced by callers that never
        // crossed a mount boundary use bare local names only; those
        // resolve identically to the pre-D276 flat lookup (and
        // identically to a one-segment `try_resolve` on the owner).
        //
        // See `~/src/graphrefly-ts/docs/rust-port-decisions.md` D276
        // and the `docs/porting-deferred.md` M4.E1 closure block.

        // Pass 0 — mount tree.
        let mut graph_map: IndexMap<String, Graph> = IndexMap::new();
        graph_map.insert(String::new(), graph.clone());
        mount_subgraphs_recursive(core, &graph, snapshot, "", &mut graph_map)?;

        // Pass 1 — state nodes tree-wide.
        create_state_nodes_recursive(core, snapshot, "", &graph_map, &binding)?;

        // Pass 2 — derived nodes tree-wide with shared retry loop.
        let mut derived_queue: Vec<DerivedEntry> = Vec::new();
        collect_derived_recursive(snapshot, "", &graph_map, &mut derived_queue);
        create_derived_with_retry(core, &factories, derived_queue)?;

        // Pass 3 — status restore.
        apply_status_recursive(core, snapshot, "", &graph_map, &binding)?;

        Ok(graph)
    }
}

/// D276 auto-hydration: a non-state node awaiting derivation in
/// Pass 2's shared retry loop. The owner graph is captured so the
/// retry loop can call `owner_graph.try_resolve(dep_name)` against
/// the right namespace (owner-relative paths walk via the owner's
/// parent/child references in [`crate::graph::resolve_checked`]).
struct DerivedEntry {
    owner_graph: Graph,
    name: String,
    slice: NodeSlice,
}

/// D276 Pass 0 — mount every subgraph under `parent` and accumulate
/// each subgraph's absolute path → [`Graph`] handle into `graph_map`.
fn mount_subgraphs_recursive(
    core: &Core,
    parent: &Graph,
    snap: &GraphPersistSnapshot,
    parent_path: &str,
    graph_map: &mut IndexMap<String, Graph>,
) -> Result<(), SnapshotError> {
    for (child_name, child_snap) in &snap.subgraphs {
        let child_graph = parent
            .mount_new(core, child_name)
            .map_err(|_| SnapshotError::UnknownSubgraph(child_name.clone()))?;
        let child_path = if parent_path.is_empty() {
            child_name.clone()
        } else {
            format!("{parent_path}{PATH_SEP}{child_name}")
        };
        graph_map.insert(child_path.clone(), child_graph.clone());
        mount_subgraphs_recursive(core, &child_graph, child_snap, &child_path, graph_map)?;
    }
    Ok(())
}

/// D276 Pass 1 — recursively create every state node in the tree
/// (state nodes have no deps to resolve). Each new state node is
/// registered in its owner graph's `names`; Pass 2/3 locate it via
/// [`Graph::try_resolve`].
fn create_state_nodes_recursive(
    core: &Core,
    snap: &GraphPersistSnapshot,
    owner_path: &str,
    graph_map: &IndexMap<String, Graph>,
    binding: &Arc<dyn BindingBoundary>,
) -> Result<(), SnapshotError> {
    let owner_graph = graph_map
        .get(owner_path)
        .expect("D276 invariant: graph_map covers every subgraph mounted in Pass 0");

    // D279 (2026-05-22, E-ii.1): pre-validate every state name against the
    // owner graph's mount tree BEFORE any `register_state` call. Pre-D279,
    // `Graph::state` called `core.register_state(...)` first (allocating a
    // fresh NodeId + cache retention) THEN `add(name, ...)` — a name
    // collision against a child mount populated by Pass 0 left an orphan
    // NodeId in Core's registry with no path to teardown, and the surfaced
    // error (`UnknownNode` via `map_err(|_| ...)`) discarded the real
    // collision cause. Pre-validation closes both bugs at once: zero Core
    // mutation on a doomed restore, and a dedicated `NameCollision`
    // diagnostic.
    let child_mount_names: std::collections::HashSet<String> =
        owner_graph.child_names().into_iter().collect();
    for (name, slice) in &snap.nodes {
        if slice.node_type == "state" && child_mount_names.contains(name) {
            return Err(SnapshotError::NameCollision {
                name: name.clone(),
                graph_path: owner_path.to_owned(),
            });
        }
    }

    for (name, slice) in &snap.nodes {
        if slice.node_type == "state" {
            let initial = slice
                .value
                .as_ref()
                .map(|v| binding.deserialize_value(v.clone()));
            owner_graph
                .state(core, name, initial)
                .map_err(|_| SnapshotError::UnknownNode(name.clone()))?;
        }
    }
    for (child_name, child_snap) in &snap.subgraphs {
        let child_path = if owner_path.is_empty() {
            child_name.clone()
        } else {
            format!("{owner_path}{PATH_SEP}{child_name}")
        };
        create_state_nodes_recursive(core, child_snap, &child_path, graph_map, binding)?;
    }
    Ok(())
}

/// D276 Pass 2a — walk the entire tree collecting non-state nodes
/// into one flat queue tagged with their owner graph + path. The
/// queue is then run through [`create_derived_with_retry`] which
/// can resolve deps that cross mount boundaries.
fn collect_derived_recursive(
    snap: &GraphPersistSnapshot,
    owner_path: &str,
    graph_map: &IndexMap<String, Graph>,
    out: &mut Vec<DerivedEntry>,
) {
    let owner_graph = graph_map
        .get(owner_path)
        .expect("D276 invariant: graph_map covers every subgraph mounted in Pass 0");
    for (name, slice) in &snap.nodes {
        if slice.node_type != "state" {
            out.push(DerivedEntry {
                owner_graph: owner_graph.clone(),
                name: name.clone(),
                slice: slice.clone(),
            });
        }
    }
    for (child_name, child_snap) in &snap.subgraphs {
        let child_path = if owner_path.is_empty() {
            child_name.clone()
        } else {
            format!("{owner_path}{PATH_SEP}{child_name}")
        };
        collect_derived_recursive(child_snap, &child_path, graph_map, out);
    }
}

/// D276 Pass 2b — tree-wide retry loop for derived/dynamic/operator
/// nodes. Each iteration resolves an entry's deps via the owner
/// graph's [`Graph::try_resolve`], which natively understands the
/// owner-relative path syntax emitted by the D276 encoder:
///
/// - bare name → local lookup in owner's `names` (pre-D276 shape).
/// - `"child::name"` → descend into a mounted subgraph.
/// - `"..::name"` → walk to parent.
/// - `"..::sibling::name"` → walk to parent then descend into a sibling.
///
/// Loop terminates when (a) all entries created — success — or
/// (b) one pass made no progress — `UnresolvableDeps` on the first
/// stuck entry.
fn create_derived_with_retry(
    core: &Core,
    factories: &IndexMap<String, NodeFactory>,
    entries: Vec<DerivedEntry>,
) -> Result<(), SnapshotError> {
    let mut remaining = entries;
    loop {
        let before = remaining.len();
        let mut still_remaining = Vec::new();

        for entry in remaining {
            let mut resolved = Vec::with_capacity(entry.slice.deps.len());
            let mut all_ok = true;
            for dep_name in &entry.slice.deps {
                if let Some(dep_id) = entry.owner_graph.try_resolve(dep_name) {
                    resolved.push(dep_id);
                } else {
                    all_ok = false;
                    break;
                }
            }
            if all_ok {
                let factory = factories.get(&entry.slice.node_type).ok_or_else(|| {
                    SnapshotError::MissingFactory(entry.slice.node_type.clone(), entry.name.clone())
                })?;
                factory(
                    core,
                    &entry.owner_graph,
                    &entry.name,
                    &entry.slice,
                    &resolved,
                )?;
            } else {
                still_remaining.push(entry);
            }
        }

        remaining = still_remaining;
        if remaining.is_empty() {
            break;
        }
        if remaining.len() == before {
            let entry = &remaining[0];
            return Err(SnapshotError::UnresolvableDeps(
                entry.name.clone(),
                entry.slice.deps.clone(),
            ));
        }
    }
    Ok(())
}

/// D276 Pass 3 — recursively apply each node's snapshot status
/// (Completed / Errored) via the owner graph's [`Graph::try_resolve`].
/// Sentinel / Live are no-ops (state nodes already received their
/// cache during Pass 1's `Graph::state` initial-value path; derived
/// recompute on first subscribe).
fn apply_status_recursive(
    core: &Core,
    snap: &GraphPersistSnapshot,
    owner_path: &str,
    graph_map: &IndexMap<String, Graph>,
    binding: &Arc<dyn BindingBoundary>,
) -> Result<(), SnapshotError> {
    let owner_graph = graph_map
        .get(owner_path)
        .expect("D276 invariant: graph_map covers every subgraph mounted in Pass 0");
    for (name, slice) in &snap.nodes {
        // /qa G2.1 (2026-05-22): mirror `restore_into:339` — a node listed
        // in the snapshot that doesn't resolve via `try_resolve` is a
        // structural inconsistency (Pass 1/Pass 2 should have created it).
        // Silently dropping the status would mask a Pass-2 hydration bug
        // as "successful restore with corrupted lifecycle state."
        let node_id = owner_graph
            .try_resolve(name)
            .ok_or_else(|| SnapshotError::UnknownNode(name.clone()))?;
        match &slice.status {
            NodeSnapshotStatus::Completed => {
                owner_graph.complete(core, node_id);
            }
            NodeSnapshotStatus::Errored { error } => {
                if let Some(err_val) = error {
                    let err_handle = binding.deserialize_value(err_val.clone());
                    owner_graph.error(core, node_id, err_handle);
                }
            }
            NodeSnapshotStatus::Sentinel | NodeSnapshotStatus::Live => {}
        }
    }
    for (child_name, child_snap) in &snap.subgraphs {
        let child_path = if owner_path.is_empty() {
            child_name.clone()
        } else {
            format!("{owner_path}{PATH_SEP}{child_name}")
        };
        apply_status_recursive(core, child_snap, &child_path, graph_map, binding)?;
    }
    Ok(())
}
