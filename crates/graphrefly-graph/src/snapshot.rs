//! `Graph::snapshot()` / `Graph::restore()` / `Graph::from_snapshot()`
//! — portable serialization of graph state (M4.E1, R3.8).
//!
//! # Handle-protocol boundary
//!
//! `snapshot()` calls `BindingBoundary::serialize_handle` to project
//! each node's cached `HandleId` into a `serde_json::Value`. `restore()`
//! / `from_snapshot()` call `BindingBoundary::deserialize_value` to
//! re-intern values from JSON back into handles.
//!
//! # Edges
//!
//! Per D169, edges are omitted from the snapshot (they're derived from
//! deps via `Graph::edges()`). This keeps the format lean; edges can be
//! added as an additive field later without breaking the format.

use std::sync::Arc;

use graphrefly_core::{BindingBoundary, NodeId, NodeKind, TerminalKind, NO_HANDLE};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::graph::Graph;

/// Portable snapshot of a graph's state — survives serialization
/// round-trips (JSON, CBOR, etc.).
///
/// Contains the graph name, per-node state slices, and mounted
/// subgraph names (recursive). Edges are omitted (derived from
/// deps per D169).
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
    /// Node kind: `"state"`, `"derived"`, `"dynamic"`, `"producer"`,
    /// `"operator"`.
    #[serde(rename = "type")]
    pub node_type: String,
    /// Serialized cache value. `None` when the cache is sentinel
    /// (node has never emitted DATA).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>,
    /// Node lifecycle status.
    pub status: NodeSnapshotStatus,
    /// Dependency names in declaration order (empty for state/producer).
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
    /// Terminal: ERROR. Carries the serialized error value.
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

/// Factory function for auto-hydration mode of `Graph::from_snapshot`.
/// Given a node name, its snapshot slice, and already-resolved dep
/// `NodeId`s, returns the `NodeId` of the reconstructed node registered
/// in the provided `Graph`.
///
/// The factory is responsible for calling `graph.state(...)`,
/// `graph.derived(...)`, etc. — it knows the node's type and how to
/// wire the fn/equals from the application's closure registry.
pub type NodeFactory =
    Box<dyn Fn(&Graph, &str, &NodeSlice, &[NodeId]) -> Result<NodeId, SnapshotError>>;

/// Builder function for `Graph::from_snapshot` builder mode.
pub type SnapshotBuilder = Box<dyn FnOnce(&Graph)>;

impl Graph {
    /// Serialize this graph's state into a portable snapshot.
    ///
    /// Walks the namespace and mounted subgraphs recursively. Each
    /// node's cache value is serialized via
    /// [`BindingBoundary::serialize_handle`]. Terminal error payloads
    /// are also serialized.
    ///
    /// The snapshot captures state-at-call-time; concurrent mutations
    /// during snapshot may produce a torn read. Use `Graph::batch` or
    /// `Graph::signal(Pause)` for consistency if needed.
    #[must_use]
    pub fn snapshot(&self) -> GraphPersistSnapshot {
        self.snapshot_inner()
    }

    fn snapshot_inner(&self) -> GraphPersistSnapshot {
        let inner = self.inner.lock();
        let name = inner.name.clone();

        // Build a name→NodeId map + collect dep NodeIds for each node,
        // all under the inner lock.
        let node_entries: Vec<(String, NodeId)> =
            inner.names.iter().map(|(n, &id)| (n.clone(), id)).collect();
        let children: Vec<(String, Graph)> = inner
            .children
            .iter()
            .map(|(n, g)| (n.clone(), g.clone()))
            .collect();

        // Build reverse map for dep name resolution.
        let id_to_name: IndexMap<NodeId, String> =
            inner.names.iter().map(|(n, &id)| (id, n.clone())).collect();
        drop(inner);

        let binding = self.core.binding_ptr();
        let mut nodes = IndexMap::new();

        for (node_name, node_id) in &node_entries {
            let kind = self.core.kind_of(*node_id);
            let node_type = match kind {
                Some(NodeKind::State) => "state",
                Some(NodeKind::Derived) => "derived",
                Some(NodeKind::Dynamic) => "dynamic",
                Some(NodeKind::Producer) => "producer",
                Some(NodeKind::Operator(_)) => "operator",
                None => "unknown",
            };

            // Serialize cache value.
            let cache = self.core.cache_of(*node_id);
            let value = if cache == NO_HANDLE {
                None
            } else {
                binding.serialize_handle(cache)
            };

            // Terminal status.
            let terminal = self.core.is_terminal(*node_id);
            let status = match terminal {
                Some(TerminalKind::Complete) => NodeSnapshotStatus::Completed,
                Some(TerminalKind::Error(err_handle)) => NodeSnapshotStatus::Errored {
                    error: binding.serialize_handle(err_handle),
                },
                None => {
                    if self.core.has_fired_once(*node_id) || cache != NO_HANDLE {
                        NodeSnapshotStatus::Live
                    } else {
                        NodeSnapshotStatus::Sentinel
                    }
                }
            };

            // Dep names in declaration order.
            let dep_ids = self.core.deps_of(*node_id);
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

        // Recurse into mounted subgraphs.
        let mut subgraphs = IndexMap::new();
        for (child_name, child_graph) in children {
            subgraphs.insert(child_name, child_graph.snapshot_inner());
        }

        GraphPersistSnapshot {
            name,
            nodes,
            subgraphs,
        }
    }

    /// Restore state from a snapshot into an existing graph.
    ///
    /// The graph must already have nodes registered with matching names.
    /// For each node in the snapshot:
    /// - State nodes: emits the serialized value via `Core::emit`.
    /// - Compute nodes (derived/dynamic/operator): skipped (their state
    ///   is derived from deps; they'll recompute when deps emit).
    /// - Terminal status is NOT restored (terminals require a separate
    ///   lifecycle mechanism — `complete()` / `error()` calls).
    ///
    /// Recurses into mounted subgraphs.
    ///
    /// # Errors
    ///
    /// Returns `SnapshotError::NameMismatch` if the snapshot name
    /// doesn't match the graph name. Returns `SnapshotError::UnknownNode`
    /// for nodes in the snapshot that aren't in the graph namespace.
    pub fn restore(&self, snapshot: &GraphPersistSnapshot) -> Result<(), SnapshotError> {
        let graph_name = self.name();
        if snapshot.name != graph_name {
            return Err(SnapshotError::NameMismatch {
                expected: snapshot.name.clone(),
                actual: graph_name,
            });
        }

        let binding = self.core.binding_ptr();

        for (node_name, slice) in &snapshot.nodes {
            let node_id = self
                .try_resolve(node_name)
                .ok_or_else(|| SnapshotError::UnknownNode(node_name.clone()))?;

            // Only restore values for state nodes — compute nodes derive
            // their state from deps and will recompute.
            if slice.node_type == "state" {
                if let Some(ref value) = slice.value {
                    let handle = binding.deserialize_value(value.clone());
                    self.core.emit(node_id, handle);
                }
            }

            // Restore terminal status.
            match &slice.status {
                NodeSnapshotStatus::Completed => {
                    self.core.complete(node_id);
                }
                NodeSnapshotStatus::Errored { error } => {
                    if let Some(err_val) = error {
                        let err_handle = binding.deserialize_value(err_val.clone());
                        self.core.error(node_id, err_handle);
                    }
                }
                NodeSnapshotStatus::Sentinel | NodeSnapshotStatus::Live => {}
            }
        }

        // Recurse into subgraphs. Collect under lock, restore after drop.
        let child_pairs: Vec<(String, Graph)> = {
            let inner = self.inner.lock();
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
        for (child_name, child) in child_pairs {
            child.restore(&snapshot.subgraphs[&child_name])?;
        }

        Ok(())
    }

    /// Reconstruct a graph from a snapshot.
    ///
    /// **Builder mode** (when `builder` is `Some`): creates an empty
    /// graph, runs the builder to register nodes, then calls `restore()`
    /// to populate state from the snapshot. The builder controls topology;
    /// the snapshot only provides values.
    ///
    /// **Auto-hydration mode** (when `builder` is `None`): reconstructs
    /// both topology and state from the snapshot. Requires `factories`
    /// for non-state nodes — a map from node type string (e.g.
    /// `"derived"`) to a factory function that registers the node in
    /// the graph given its name, snapshot slice, and resolved dep ids.
    ///
    /// State nodes are auto-created without a factory (they only need
    /// a name and optional initial value).
    ///
    /// # Errors
    ///
    /// Returns `SnapshotError::UnresolvableDeps` if auto-hydration
    /// can't resolve a node's dependencies after iterating all nodes.
    /// Returns `SnapshotError::MissingFactory` if a non-state node
    /// type has no registered factory.
    pub fn from_snapshot(
        snapshot: &GraphPersistSnapshot,
        binding: &Arc<dyn BindingBoundary>,
        builder: Option<SnapshotBuilder>,
        factories: Option<IndexMap<String, NodeFactory>>,
    ) -> Result<Self, SnapshotError> {
        let graph = Graph::new(&snapshot.name, Arc::clone(binding));

        if let Some(build_fn) = builder {
            // Builder mode: user registers nodes, snapshot restores state.
            build_fn(&graph);
            graph.restore(snapshot)?;
            return Ok(graph);
        }

        // Auto-hydration mode.
        let factories = factories.unwrap_or_default();

        // Phase 1: Mount hierarchy (recursive).
        for (child_name, child_snapshot) in &snapshot.subgraphs {
            let child = graph
                .mount_new(child_name)
                .map_err(|_| SnapshotError::UnknownSubgraph(child_name.clone()))?;
            Self::hydrate_subgraph(&child, child_snapshot, binding, &factories)?;
        }

        // Phase 2: Iterative node reconstruction.
        Self::hydrate_nodes(&graph, snapshot, binding, &factories)?;

        Ok(graph)
    }

    fn hydrate_subgraph(
        graph: &Graph,
        snapshot: &GraphPersistSnapshot,
        binding: &Arc<dyn BindingBoundary>,
        factories: &IndexMap<String, NodeFactory>,
    ) -> Result<(), SnapshotError> {
        // Mount children first.
        for (child_name, child_snapshot) in &snapshot.subgraphs {
            let child = graph
                .mount_new(child_name)
                .map_err(|_| SnapshotError::UnknownSubgraph(child_name.clone()))?;
            Self::hydrate_subgraph(&child, child_snapshot, binding, factories)?;
        }
        // Then hydrate nodes.
        Self::hydrate_nodes(graph, snapshot, binding, factories)
    }

    fn hydrate_nodes(
        graph: &Graph,
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

        // Iterative resolution: keep passing until all nodes are created
        // or no progress is made.
        loop {
            let before = remaining.len();
            let mut still_remaining = Vec::new();

            for (name, slice) in remaining {
                // Check if all deps are resolved.
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
                    // Create the node.
                    let node_id = if slice.node_type == "state" {
                        // State nodes: auto-create with initial value.
                        let initial = slice
                            .value
                            .as_ref()
                            .map(|v| binding.deserialize_value(v.clone()));
                        graph
                            .state(&name, initial)
                            .map_err(|_| SnapshotError::UnknownNode(name.clone()))?
                    } else {
                        // Non-state nodes: use factory.
                        let factory = factories.get(&slice.node_type).ok_or_else(|| {
                            SnapshotError::MissingFactory(slice.node_type.clone(), name.clone())
                        })?;
                        factory(graph, &name, &slice, &dep_ids)?
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
                // No progress — unresolvable deps.
                let (name, slice) = &remaining[0];
                return Err(SnapshotError::UnresolvableDeps(
                    name.clone(),
                    slice.deps.clone(),
                ));
            }
        }

        // Phase 3: Restore terminal status for all nodes.
        for (name, slice) in &snapshot.nodes {
            if let Some(&node_id) = created.get(name) {
                match &slice.status {
                    NodeSnapshotStatus::Completed => {
                        graph.complete(node_id);
                    }
                    NodeSnapshotStatus::Errored { error } => {
                        if let Some(err_val) = error {
                            let err_handle = binding.deserialize_value(err_val.clone());
                            graph.error(node_id, err_handle);
                        }
                    }
                    NodeSnapshotStatus::Sentinel | NodeSnapshotStatus::Live => {}
                }
            }
        }

        Ok(())
    }
}
