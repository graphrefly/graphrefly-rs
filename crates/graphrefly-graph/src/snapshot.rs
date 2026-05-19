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

use std::sync::Arc;

use graphrefly_core::{BindingBoundary, Core, CoreFull, NodeId, NodeKind, TerminalKind, NO_HANDLE};
use indexmap::IndexMap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::graph::{resolve_checked, Graph, GraphInner};

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
}

/// Factory for auto-hydration mode. D246: receives the embedder's
/// `&Core` + the Core-free [`Graph`] handle.
pub type NodeFactory =
    Box<dyn Fn(&Core, &Graph, &str, &NodeSlice, &[NodeId]) -> Result<NodeId, SnapshotError>>;

/// Builder function for `Graph::from_snapshot` builder mode (D246:
/// `&Core` + Core-free [`Graph`]).
pub type SnapshotBuilder = Box<dyn FnOnce(&Core, &Graph)>;

/// D246: recursive snapshot over `(&dyn CoreFull, &Arc<Mutex<GraphInner>>)`
/// — `&dyn CoreFull` (the one facade) so the storage in-wave
/// `MailboxOp::Defer` observe-sink can run it (read-only;
/// `serialize_handle` delegates to the binding).
pub(crate) fn snapshot_of(
    core: &dyn CoreFull,
    inner_arc: &Arc<Mutex<GraphInner>>,
) -> GraphPersistSnapshot {
    let (name, node_entries, children, id_to_name) = {
        let inner = inner_arc.lock();
        let name = inner.name.clone();
        let node_entries: Vec<(String, NodeId)> =
            inner.names.iter().map(|(n, &id)| (n.clone(), id)).collect();
        let children: Vec<(String, Arc<Mutex<GraphInner>>)> = inner
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

        let dep_ids = core.deps_of(*node_id);
        let deps: Vec<String> = dep_ids
            .iter()
            .map(|dep_id| {
                id_to_name
                    .get(dep_id)
                    .cloned()
                    .unwrap_or_else(|| format!("_anon_{}", dep_id.raw()))
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
        subgraphs.insert(child_name, snapshot_of(core, &child_inner));
    }

    GraphPersistSnapshot {
        name,
        nodes,
        subgraphs,
    }
}

/// Recursive restore over `(&Core, &Arc<Mutex<GraphInner>>)`.
fn restore_into(
    core: &Core,
    inner_arc: &Arc<Mutex<GraphInner>>,
    snapshot: &GraphPersistSnapshot,
) -> Result<(), SnapshotError> {
    let graph_name = inner_arc.lock().name.clone();
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

    let child_pairs: Vec<(String, Arc<Mutex<GraphInner>>)> = {
        let inner = inner_arc.lock();
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
    #[must_use]
    pub fn snapshot(&self, core: &Core) -> GraphPersistSnapshot {
        snapshot_of(core, &self.inner)
    }

    /// [`Self::snapshot`] over the one object-safe facade (D246 rule 5)
    /// — for the storage in-wave `MailboxOp::Defer(|cf: &dyn CoreFull|)`
    /// path, which only has a `&dyn CoreFull` (not a concrete `&Core`).
    /// Read-only; `serialize_handle` delegates to the binding.
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
        for (child_name, child_snapshot) in &snapshot.subgraphs {
            let child = graph
                .mount_new(core, child_name)
                .map_err(|_| SnapshotError::UnknownSubgraph(child_name.clone()))?;
            hydrate_subgraph(core, &child, child_snapshot, &binding, &factories)?;
        }
        hydrate_nodes(core, &graph, snapshot, &binding, &factories)?;

        Ok(graph)
    }
}

fn hydrate_subgraph(
    core: &Core,
    g: &Graph,
    snapshot: &GraphPersistSnapshot,
    binding: &Arc<dyn BindingBoundary>,
    factories: &IndexMap<String, NodeFactory>,
) -> Result<(), SnapshotError> {
    for (child_name, child_snapshot) in &snapshot.subgraphs {
        let child = g
            .mount_new(core, child_name)
            .map_err(|_| SnapshotError::UnknownSubgraph(child_name.clone()))?;
        hydrate_subgraph(core, &child, child_snapshot, binding, factories)?;
    }
    hydrate_nodes(core, g, snapshot, binding, factories)
}

fn hydrate_nodes(
    core: &Core,
    g: &Graph,
    snapshot: &GraphPersistSnapshot,
    binding: &Arc<dyn BindingBoundary>,
    factories: &IndexMap<String, NodeFactory>,
) -> Result<(), SnapshotError> {
    let mut created: IndexMap<String, NodeId> = IndexMap::new();
    let mut remaining: Vec<(String, NodeSlice)> = snapshot
        .nodes
        .iter()
        .map(|(n, s)| (n.clone(), s.clone()))
        .collect();

    loop {
        let before = remaining.len();
        let mut still_remaining = Vec::new();

        for (name, slice) in remaining {
            let deps_resolved: Option<Vec<NodeId>> = if slice.deps.is_empty() {
                Some(Vec::new())
            } else {
                let mut resolved = Vec::with_capacity(slice.deps.len());
                let mut all_ok = true;
                for dep_name in &slice.deps {
                    if let Some(&dep_id) = created.get(dep_name) {
                        resolved.push(dep_id);
                    } else {
                        all_ok = false;
                        break;
                    }
                }
                if all_ok {
                    Some(resolved)
                } else {
                    None
                }
            };

            if let Some(dep_ids) = deps_resolved {
                let node_id = if slice.node_type == "state" {
                    let initial = slice
                        .value
                        .as_ref()
                        .map(|v| binding.deserialize_value(v.clone()));
                    g.state(core, &name, initial)
                        .map_err(|_| SnapshotError::UnknownNode(name.clone()))?
                } else {
                    let factory = factories.get(&slice.node_type).ok_or_else(|| {
                        SnapshotError::MissingFactory(slice.node_type.clone(), name.clone())
                    })?;
                    factory(core, g, &name, &slice, &dep_ids)?
                };
                created.insert(name, node_id);
            } else {
                still_remaining.push((name, slice));
            }
        }

        remaining = still_remaining;
        if remaining.is_empty() {
            break;
        }
        if remaining.len() == before {
            let (name, slice) = &remaining[0];
            return Err(SnapshotError::UnresolvableDeps(
                name.clone(),
                slice.deps.clone(),
            ));
        }
    }

    for (name, slice) in &snapshot.nodes {
        if let Some(&node_id) = created.get(name) {
            match &slice.status {
                NodeSnapshotStatus::Completed => {
                    g.complete(core, node_id);
                }
                NodeSnapshotStatus::Errored { error } => {
                    if let Some(err_val) = error {
                        let err_handle = binding.deserialize_value(err_val.clone());
                        g.error(core, node_id, err_handle);
                    }
                }
                NodeSnapshotStatus::Sentinel | NodeSnapshotStatus::Live => {}
            }
        }
    }

    Ok(())
}
