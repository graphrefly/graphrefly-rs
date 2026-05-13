//! `Graph` container — namespace, sugar constructors, and lifecycle
//! pass-throughs over `Arc<Core>`.
//!
//! `mount` / `describe` / `observe` live in sibling modules.

use std::sync::{Arc, Weak};

use graphrefly_core::{
    BindingBoundary, Core, EqualsMode, FnId, HandleId, LockId, NodeId, PauseError, ResumeReport,
    SetDepsError, Sink, Subscription,
};
use indexmap::IndexMap;
use parking_lot::Mutex;

use crate::mount::{GraphRemoveAudit, MountError};

/// Namespace path separator (canonical spec R3.5.1).
pub(crate) const PATH_SEP: &str = "::";

/// Errors from [`Graph::remove`].
#[derive(Debug, thiserror::Error)]
pub enum RemoveError {
    #[error("Graph::remove: name `{0}` not found (neither a node nor a mounted subgraph)")]
    NotFound(String),
    #[error("Graph::remove: graph has been destroyed")]
    Destroyed,
}

/// Signal kind for [`Graph::signal`] (canonical R3.7.1).
#[derive(Debug, Clone, Copy)]
pub enum SignalKind {
    /// Wipe caches (with meta filtering per R3.7.2).
    Invalidate,
    /// Pause every named node with the given lock.
    Pause(LockId),
    /// Resume every named node with the given lock.
    Resume(LockId),
    /// Mark every named node as terminal with `COMPLETE`.
    /// Slice V3: extends `SignalKind` per D3 in porting-deferred.md.
    Complete,
    /// Mark every named node as terminal with `ERROR` carrying the given handle.
    /// Slice V3: extends `SignalKind` per D3 in porting-deferred.md.
    Error(HandleId),
}

/// Path resolution errors returned by [`Graph::try_resolve_checked`].
/// Slice V3: introduces typed path errors per porting-deferred.md.
#[derive(Debug, thiserror::Error)]
pub enum PathError {
    /// Path is empty.
    #[error("Path is empty")]
    Empty,
    /// A `..` segment was used but the graph has no parent.
    #[error("Path segment `..` used on root graph (no parent)")]
    NoParent,
    /// A segment named a child graph that doesn't exist.
    #[error("Path segment `{0}` does not match any child graph")]
    ChildNotFound(String),
    /// The graph has been destroyed.
    #[error("Graph has been destroyed")]
    Destroyed,
}

/// Errors from name registration.
#[derive(Debug, thiserror::Error)]
pub enum NameError {
    #[error("Graph::add: name `{0}` already registered in this graph")]
    Collision(String),
    #[error("Graph: name `{0}` may not contain the `::` path separator")]
    InvalidName(String),
    #[error("Graph: name `{0}` uses the reserved `_anon_` prefix (collides with anonymous node describe format)")]
    ReservedPrefix(String),
    #[error("Graph: graph has been destroyed; further registration refused")]
    Destroyed,
}

/// Internal mutable state — namespace, mount tree, lifecycle bookkeeping.
///
/// Kept behind a single `Mutex<GraphInner>` because all mutations are
/// short structural updates (insert into `IndexMap`, insert child `Graph`,
/// pop name on unmount). The `Core` dispatcher behind `Arc<Core>` has its
/// own lock for the wave engine — these two locks never nest in the
/// same direction (`Graph` → `Core` only, never `Core` → `Graph`), avoiding
/// deadlock by acquisition order.
/// Callback type for graph-level namespace change notifications.
/// Used by reactive describe and reactive `observe_all` to react to
/// `add()`, `remove()`, mount/unmount, and `destroy()` — events that
/// change the graph's namespace AFTER Core registration completes.
pub(crate) type NamespaceChangeSink = Arc<dyn Fn() + Send + Sync>;

pub(crate) struct GraphInner {
    pub(crate) name: String,
    /// Local namespace: name → `NodeId`. Insertion order is load-bearing
    /// for `describe()` stability (canonical Appendix C "Git-versioned
    /// graphs" scenario).
    pub(crate) names: IndexMap<String, NodeId>,
    /// Reverse lookup. Single-owner (one node has at most one local
    /// name in this graph; if `add` is called twice with different
    /// names, the second errors via `NameError::Collision`).
    pub(crate) names_inverse: IndexMap<NodeId, String>,
    /// Mounted child subgraphs. Insertion order = describe `subgraphs`
    /// order. Each child shares this graph's `Arc<Core>` (cross-Core
    /// mount is post-M6 per session-doc Open Question 1).
    pub(crate) children: IndexMap<String, Graph>,
    /// Parent inner-state pointer (for `ancestors()`). Weak to break
    /// the strong cycle: parent owns child via `children`, so child's
    /// back-pointer must not pin the parent alive.
    pub(crate) parent: Option<Weak<Mutex<GraphInner>>>,
    /// True after `destroy()` completes — subsequent mutations refuse.
    pub(crate) destroyed: bool,
    /// Namespace-change sinks — fired from `add()`, `remove()`, etc.
    /// after the inner lock is dropped. Keyed by subscription id.
    pub(crate) namespace_sinks: IndexMap<u64, NamespaceChangeSink>,
    pub(crate) next_ns_sink_id: u64,
}

/// Graph container — Phase 1+ public API.
///
/// Holds an [`Arc<Core>`] internally plus an [`Arc<Mutex<GraphInner>>`]
/// for namespace + mount-tree state. Cloning is a cheap refcount bump
/// on both Arcs — pass `Graph` by value to threads (or share via
/// `Arc<Graph>` when an outer reference count is needed).
///
/// Two clones of the same `Graph` share BOTH the dispatcher state AND
/// the namespace. Mutations from one clone are visible to all clones.
#[derive(Clone)]
pub struct Graph {
    pub(crate) core: Core,
    pub(crate) inner: Arc<Mutex<GraphInner>>,
}

impl std::fmt::Debug for Graph {
    /// Compact `Debug` summary — `name` + counts. Avoids printing the
    /// full namespace map (which would lock + clone strings each call,
    /// surprising under `dbg!()`). Use [`Graph::describe`] for a full
    /// snapshot.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock();
        f.debug_struct("Graph")
            .field("name", &inner.name)
            .field("node_count", &inner.names.len())
            .field("subgraph_count", &inner.children.len())
            .field("destroyed", &inner.destroyed)
            .finish_non_exhaustive()
    }
}

impl Graph {
    /// Construct a named, empty root graph wired to the given binding.
    ///
    /// `name` becomes the graph's identity for `describe()` output and
    /// `ancestors()`. May contain colons (`":"`), but the double-colon
    /// path separator (`"::"`) is reserved; use [`Graph::is_valid_name`]
    /// to pre-check user input.
    #[must_use]
    pub fn new(name: impl Into<String>, binding: Arc<dyn BindingBoundary>) -> Self {
        Self::with_core(name.into(), Core::new(binding), None)
    }

    pub(crate) fn with_core(
        name: String,
        core: Core,
        parent: Option<Weak<Mutex<GraphInner>>>,
    ) -> Self {
        Self {
            core,
            inner: Arc::new(Mutex::new(GraphInner {
                name,
                names: IndexMap::new(),
                names_inverse: IndexMap::new(),
                children: IndexMap::new(),
                parent,
                destroyed: false,
                namespace_sinks: IndexMap::new(),
                next_ns_sink_id: 0,
            })),
        }
    }

    /// Construct a fresh root [`Graph`] wrapping an existing [`Core`].
    /// Used by binding crates (napi-rs, pyo3, wasm) where the same
    /// `Core` is shared between a `Graph` and direct binding-side
    /// dispatch (`BenchOperators::register_*`, etc.) so the namespace
    /// and the operator surface see the same node ids.
    ///
    /// Sister method to [`Self::new`]; the difference is that `new`
    /// constructs a fresh `Core` from a binding, while this one accepts
    /// a `Core` the caller already owns (typically cloned from
    /// `BenchCore`).
    #[must_use]
    pub fn with_existing_core(name: impl Into<String>, core: Core) -> Self {
        Self::with_core(name.into(), core, None)
    }

    /// The graph's name as set at construction (or via `mount` / `mount_new`).
    #[must_use]
    pub fn name(&self) -> String {
        self.inner.lock().name.clone()
    }

    /// Borrow the underlying [`Core`] — the M1 dispatcher. Useful when you
    /// need a Core-level method that hasn't yet been surfaced on `Graph`.
    /// Phase 4+ patterns commonly hold onto the `Core` directly.
    #[must_use]
    pub fn core(&self) -> &Core {
        &self.core
    }

    /// Whether `name` is a legal local node/subgraph name.
    /// Rejects names containing `::` (path separator) and names
    /// starting with `_anon_` (reserved for anonymous-node describe
    /// format, Slice V3 collision fix).
    #[must_use]
    pub fn is_valid_name(name: &str) -> bool {
        !name.contains(PATH_SEP) && !name.starts_with("_anon_")
    }

    /// Subscribe to namespace changes (add, remove, mount, unmount,
    /// destroy). The sink fires AFTER the inner lock is dropped, so
    /// it can safely call `describe()` or other Graph methods.
    ///
    /// Returns a subscription id for unsubscription.
    pub(crate) fn subscribe_namespace_change(&self, sink: NamespaceChangeSink) -> u64 {
        let mut inner = self.inner.lock();
        let id = inner.next_ns_sink_id;
        inner.next_ns_sink_id += 1;
        inner.namespace_sinks.insert(id, sink);
        id
    }

    /// Unsubscribe a namespace-change sink by id.
    pub(crate) fn unsubscribe_namespace_change(&self, id: u64) {
        self.inner.lock().namespace_sinks.shift_remove(&id);
    }

    /// Fire all namespace-change sinks. Called AFTER inner lock is
    /// dropped from `add()`, `remove()`, etc.
    pub(crate) fn fire_namespace_change(&self) {
        let sinks: Vec<NamespaceChangeSink> = {
            let inner = self.inner.lock();
            inner.namespace_sinks.values().cloned().collect()
        };
        for sink in sinks {
            sink();
        }
    }

    fn validate_name(name: &str) -> Result<(), NameError> {
        if name.contains(PATH_SEP) {
            Err(NameError::InvalidName(name.to_owned()))
        } else if name.starts_with("_anon_") {
            Err(NameError::ReservedPrefix(name.to_owned()))
        } else {
            Ok(())
        }
    }

    // -------------------------------------------------------------------
    // Namespace (canonical spec §3.5)
    // -------------------------------------------------------------------

    /// Register an existing `node_id` under `name` in this graph's
    /// namespace. The node is assumed already-registered with the
    /// underlying [`Core`] (via [`Graph::core`] or sugar constructors).
    ///
    /// Returns the same `node_id` for chaining. Returns
    /// [`NameError::Destroyed`] if the graph has been
    /// [`Self::destroy`]ed (symmetric with [`crate::MountError::Destroyed`]).
    pub fn add(&self, node_id: NodeId, name: impl Into<String>) -> Result<NodeId, NameError> {
        let name = name.into();
        Self::validate_name(&name)?;
        let mut inner = self.inner.lock();
        if inner.destroyed {
            return Err(NameError::Destroyed);
        }
        if inner.names.contains_key(&name) {
            return Err(NameError::Collision(name));
        }
        inner.names.insert(name.clone(), node_id);
        inner.names_inverse.insert(node_id, name);
        drop(inner);
        self.fire_namespace_change();
        Ok(node_id)
    }

    /// Resolve a path to a `NodeId`. Panics if missing — use
    /// [`Self::try_resolve`] for the non-panicking variant.
    ///
    /// Paths use `::` separators: `"validate"` (local), `"payment::validate"`
    /// (one mount level), `"system::payment::validate"` (two levels).
    ///
    /// # Panics
    ///
    /// Panics if any path segment is unknown.
    #[must_use]
    pub fn node(&self, path: &str) -> NodeId {
        self.try_resolve(path)
            .unwrap_or_else(|| panic!("Graph::node: no node at path `{path}`"))
    }

    /// Non-panicking variant of [`Self::node`]. Returns `None` for any
    /// resolution failure. For typed error reporting, use
    /// [`Self::try_resolve_checked`].
    #[must_use]
    pub fn try_resolve(&self, path: &str) -> Option<NodeId> {
        self.try_resolve_checked(path).ok().flatten()
    }

    /// Path resolution with typed errors. Returns:
    /// - `Ok(Some(id))` — path resolved to a node.
    /// - `Ok(None)` — path segments resolved but the final name wasn't found.
    /// - `Err(PathError)` — structural path error (empty, no parent for `..`,
    ///   child graph not found, destroyed).
    ///
    /// Supports `..` segments for parent traversal (R3.5.2):
    /// `"..::sibling::node"` walks up to parent, then down into `sibling`.
    ///
    /// Slice V3: adds typed errors + `..` cross-subgraph paths.
    pub fn try_resolve_checked(&self, path: &str) -> Result<Option<NodeId>, PathError> {
        if path.is_empty() {
            return Err(PathError::Empty);
        }
        let inner = self.inner.lock();
        if inner.destroyed {
            return Err(PathError::Destroyed);
        }

        let segments: Vec<&str> = path.split(PATH_SEP).collect();
        let first = segments[0];

        if first == ".." {
            // Parent traversal.
            let parent_weak = inner.parent.as_ref().ok_or(PathError::NoParent)?;
            let parent_inner = parent_weak.upgrade().ok_or(PathError::NoParent)?;
            // Reconstruct parent Graph from the Arc<Mutex<GraphInner>>.
            let parent = Graph {
                core: self.core.clone(),
                inner: parent_inner,
            };
            drop(inner);
            if segments.len() == 1 {
                // `..` alone — nonsensical (parent is a graph, not a node).
                return Ok(None);
            }
            let rest = segments[1..].join(PATH_SEP);
            parent.try_resolve_checked(&rest)
        } else if segments.len() > 1 {
            // Multi-segment: first is a child graph name.
            let child = inner
                .children
                .get(first)
                .cloned()
                .ok_or_else(|| PathError::ChildNotFound(first.to_string()))?;
            drop(inner);
            let rest = segments[1..].join(PATH_SEP);
            child.try_resolve_checked(&rest)
        } else {
            // Single segment — local lookup.
            Ok(inner.names.get(first).copied())
        }
    }

    /// Reverse lookup: returns the local name for a `node_id`, or `None`
    /// if the node is unnamed in this graph (it may still be registered
    /// in Core — `Graph::core().node_ids()` enumerates all of them).
    #[must_use]
    pub fn name_of(&self, node_id: NodeId) -> Option<String> {
        self.inner.lock().names_inverse.get(&node_id).cloned()
    }

    /// Number of named nodes in this graph (excludes mounted children's
    /// nodes, and excludes Core-only unnamed nodes). Recursive count is
    /// available by summing across [`Self::child_names`] + per-child
    /// `node_count()`.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.inner.lock().names.len()
    }

    /// Snapshot of local node names in insertion order.
    #[must_use]
    pub fn node_names(&self) -> Vec<String> {
        self.inner.lock().names.keys().cloned().collect()
    }

    /// Snapshot of mounted child names in insertion order.
    #[must_use]
    pub fn child_names(&self) -> Vec<String> {
        self.inner.lock().children.keys().cloned().collect()
    }

    /// Returns `true` after [`Self::destroy`] has been called.
    #[must_use]
    pub fn is_destroyed(&self) -> bool {
        self.inner.lock().destroyed
    }

    // -------------------------------------------------------------------
    // Sugar constructors (canonical spec §3.9)
    //
    // Each registers via Core THEN inserts into the local namespace.
    // The pair is not atomic in the Core sense (Core registration
    // happens before namespace insert), but Core's `register_state` is
    // synchronous and infallible, so the sequence is safe: a Name
    // collision rejects the whole call and leaks one Core node id (the
    // node lives but has no name). Per `feedback_no_backward_compat.md`
    // we don't catch + un-register; if a future caller needs that, the
    // Core would need a `unregister_node` API which is part of M2
    // mount/unmount cleanup — not yet wired.
    // -------------------------------------------------------------------

    /// Register a state node under `name`. `initial` of `None` starts
    /// sentinel; `Some(h)` pre-populates the cache. Returns the
    /// underlying `NodeId`.
    ///
    /// # Panics
    ///
    /// Structurally never panics — state-node registration has no
    /// reachable [`graphrefly_core::RegisterError`] variants for any
    /// caller-supplied input (state nodes have no deps and no operator
    /// scratch). The `.expect()` is present to satisfy the typed-error
    /// surface from Slice H; the `Result` return covers `NameError`
    /// (namespace conflicts) only.
    pub fn state(
        &self,
        name: impl Into<String>,
        initial: Option<HandleId>,
    ) -> Result<NodeId, NameError> {
        let id = self
            .core
            .register_state(initial.unwrap_or(graphrefly_core::NO_HANDLE), false)
            .expect("invariant: register_state has no error variants reachable for caller-controlled inputs");
        self.add(id, name)
    }

    /// Register a static-derived node — fn fires on every dep change
    /// with all deps tracked. Defaults to gated first-run (R2.5.3); use
    /// [`Self::derived_with_partial`] for the partial-mode variant
    /// (D011 / R5.4).
    ///
    /// # Panics
    ///
    /// Panics if any element of `deps` is not a registered node id, or
    /// if a dep is terminal and not resubscribable. The `Result` return
    /// covers `NameError` (namespace conflicts); construction-time
    /// errors at the Core layer ([`graphrefly_core::RegisterError`]) are
    /// caller-contract violations and surface as panics.
    pub fn derived(
        &self,
        name: impl Into<String>,
        deps: &[NodeId],
        fn_id: FnId,
        equals: EqualsMode,
    ) -> Result<NodeId, NameError> {
        let id = self
            .core
            .register_derived(deps, fn_id, equals, false)
            .expect("invariant: caller has validated dep ids before calling register_derived");
        self.add(id, name)
    }

    /// Register a dynamic-derived node — fn declares which dep indices
    /// it actually read this run. Defaults to gated first-run (R2.5.3).
    ///
    /// # Panics
    ///
    /// Panics if any element of `deps` is not a registered node id, or
    /// if a dep is terminal and not resubscribable. The `Result` return
    /// covers `NameError` (namespace conflicts); construction-time
    /// errors at the Core layer ([`graphrefly_core::RegisterError`]) are
    /// caller-contract violations and surface as panics.
    pub fn dynamic(
        &self,
        name: impl Into<String>,
        deps: &[NodeId],
        fn_id: FnId,
        equals: EqualsMode,
    ) -> Result<NodeId, NameError> {
        let id = self
            .core
            .register_dynamic(deps, fn_id, equals, false)
            .expect("invariant: caller has validated dep ids before calling register_dynamic");
        self.add(id, name)
    }

    // -------------------------------------------------------------------
    // Named-sugar wrappers (canonical spec §3.2.1)
    //
    // `set(name, h)` / `get(name)` / `invalidate(name)` / `complete(name)`
    // / `error(name, h)` — thin resolve-then-forward wrappers so callers
    // can operate by name instead of `NodeId`.
    // -------------------------------------------------------------------

    /// Emit a value on a named state node.
    ///
    /// # Panics
    ///
    /// Panics if `name` does not resolve to a node.
    pub fn set(&self, name: &str, handle: HandleId) {
        let id = self.node(name);
        self.core.emit(id, handle);
    }

    /// Read the cached value of a named node. Returns
    /// [`graphrefly_core::NO_HANDLE`] if sentinel.
    ///
    /// # Panics
    ///
    /// Panics if `name` does not resolve.
    #[must_use]
    pub fn get(&self, name: &str) -> HandleId {
        let id = self.node(name);
        self.core.cache_of(id)
    }

    /// Clear the cache of a named node and cascade `[INVALIDATE]`.
    ///
    /// # Panics
    ///
    /// Panics if `name` does not resolve.
    pub fn invalidate_by_name(&self, name: &str) {
        let id = self.node(name);
        self.core.invalidate(id);
    }

    /// Mark a named node terminal with COMPLETE.
    ///
    /// # Panics
    ///
    /// Panics if `name` does not resolve.
    pub fn complete_by_name(&self, name: &str) {
        let id = self.node(name);
        self.core.complete(id);
    }

    /// Mark a named node terminal with ERROR.
    ///
    /// # Panics
    ///
    /// Panics if `name` does not resolve.
    pub fn error_by_name(&self, name: &str, error_handle: HandleId) {
        let id = self.node(name);
        self.core.error(id, error_handle);
    }

    // -------------------------------------------------------------------
    // Remove (canonical spec §3.2.3)
    // -------------------------------------------------------------------

    /// Remove a named node OR mounted subgraph. Fires `[TEARDOWN]` on the
    /// node (which cascades to meta children per R1.3.9.d). For subgraphs,
    /// delegates to [`Self::unmount`]. Returns [`GraphRemoveAudit`]
    /// describing what was removed.
    ///
    /// Sinks observing TEARDOWN can resolve the node's name via
    /// [`Graph::name_of`] / [`Graph::try_resolve`] for the duration of
    /// the cascade — namespace clearing happens AFTER the teardown
    /// returns (R3.2.3 / R3.7.3 ordering, mirroring `destroy()`).
    ///
    /// Returns `Err(RemoveError::NotFound)` if `name` is unknown (neither
    /// a local node nor a mounted subgraph). Returns
    /// `Err(RemoveError::Destroyed)` if the graph (or, in the subgraph
    /// branch, an ancestor in the unmount path) has been destroyed.
    #[must_use = "remove returns Err on missing name; ignoring may hide bugs"]
    pub fn remove(&self, name: &str) -> Result<GraphRemoveAudit, RemoveError> {
        // Check if it's a mounted subgraph first.
        {
            let inner = self.inner.lock();
            if inner.destroyed {
                return Err(RemoveError::Destroyed);
            }
            if inner.children.contains_key(name) {
                drop(inner);
                return self.unmount(name).map_err(|e| match e {
                    crate::mount::MountError::Destroyed => RemoveError::Destroyed,
                    _ => RemoveError::NotFound(name.to_owned()),
                });
            }
        }
        // Otherwise resolve the named node WITHOUT clearing the
        // namespace yet — sinks observing TEARDOWN must be able to
        // call name_of / try_resolve mid-cascade (R3.7.3 discipline,
        // mirroring destroy() per Slice E+ /qa B3).
        let node_id = {
            let inner = self.inner.lock();
            if inner.destroyed {
                return Err(RemoveError::Destroyed);
            }
            *inner
                .names
                .get(name)
                .ok_or_else(|| RemoveError::NotFound(name.to_owned()))?
        };
        // R3.2.3: TEARDOWN cascades through metas + downstream consumers.
        // Namespace stays intact during the cascade so sinks can
        // resolve names.
        self.core.teardown(node_id);
        // Now clear the namespace entry.
        {
            let mut inner = self.inner.lock();
            inner.names.shift_remove(name);
            inner.names_inverse.shift_remove(&node_id);
        }
        self.fire_namespace_change();
        Ok(GraphRemoveAudit {
            node_count: 1,
            mount_count: 0,
        })
    }

    // -------------------------------------------------------------------
    // Edges (canonical spec §3.3.1)
    // -------------------------------------------------------------------

    /// Derive edges from the current topology. Returns `[from, to]`
    /// pairs where both names are local namespace entries (or
    /// `_anon_<id>` for unnamed Core-only deps — including deps
    /// living in sibling graphs that share this graph's Core but
    /// are not in this graph's local namespace).
    ///
    /// When `recursive` is true, recurses into mounted subgraphs and
    /// qualifies names with `::` path separators.
    #[must_use]
    pub fn edges(&self, recursive: bool) -> Vec<(String, String)> {
        // Pre-compute the qualified-names map across the entire mount
        // tree (when `recursive`) so a child node's cross-graph dep
        // (e.g., `sub::z` depending on root's `x`) can resolve to the
        // parent-namespace name `x` instead of falling through to
        // `sub::_anon_<id>`. Pre-Slice-Y bug: the per-level names_map
        // only contained the current graph's names, breaking cross-
        // graph edge tracing under recursive walking.
        let names_map = self.collect_qualified_names("", recursive);
        self.edges_inner("", recursive, &names_map)
    }

    /// Build an `id → qualified-name` map across this graph and (if
    /// `recursive`) its mount tree. Cross-mount lookups use the
    /// nearest registered name (insertion-order semantics via
    /// `IndexMap`); the same `NodeId` never appears under two different
    /// graphs in practice (Graph namespace integrity is enforced at
    /// `add` time).
    fn collect_qualified_names(&self, prefix: &str, recursive: bool) -> IndexMap<NodeId, String> {
        let inner = self.inner.lock();
        let mut map: IndexMap<NodeId, String> = inner
            .names
            .iter()
            .map(|(n, id)| (*id, format!("{prefix}{n}")))
            .collect();
        let children: Vec<(String, Graph)> = if recursive {
            inner
                .children
                .iter()
                .map(|(n, g)| (n.clone(), g.clone()))
                .collect()
        } else {
            Vec::new()
        };
        drop(inner);
        for (child_name, child_graph) in children {
            let child_prefix = format!("{prefix}{child_name}::");
            let child_map = child_graph.collect_qualified_names(&child_prefix, true);
            for (id, name) in child_map {
                map.entry(id).or_insert(name);
            }
        }
        map
    }

    fn edges_inner(
        &self,
        prefix: &str,
        recursive: bool,
        names_map: &IndexMap<NodeId, String>,
    ) -> Vec<(String, String)> {
        let inner = self.inner.lock();
        let qualified: Vec<(String, NodeId)> = inner
            .names
            .iter()
            .map(|(n, id)| (format!("{prefix}{n}"), *id))
            .collect();
        let children: Vec<(String, Graph)> = if recursive {
            inner
                .children
                .iter()
                .map(|(n, g)| (n.clone(), g.clone()))
                .collect()
        } else {
            Vec::new()
        };
        drop(inner);

        let mut result: Vec<(String, String)> = Vec::new();
        for (to_name, id) in &qualified {
            let dep_ids = self.core.deps_of(*id);
            for dep_id in dep_ids {
                let from_name = names_map
                    .get(&dep_id)
                    .cloned()
                    .unwrap_or_else(|| format!("{prefix}_anon_{}", dep_id.raw()));
                result.push((from_name, to_name.clone()));
            }
        }
        for (child_name, child_graph) in children {
            let child_prefix = format!("{prefix}{child_name}::");
            result.extend(child_graph.edges_inner(&child_prefix, true, names_map));
        }
        result
    }

    // -------------------------------------------------------------------
    // Lifecycle pass-throughs (canonical spec §3.7)
    // -------------------------------------------------------------------

    /// Subscribe a sink. Returns a [`Subscription`] handle — dropping it
    /// unsubscribes. See [`graphrefly_core::Core::subscribe`] for full
    /// handshake semantics.
    pub fn subscribe(&self, node_id: NodeId, sink: Sink) -> Subscription {
        self.core.subscribe(node_id, sink)
    }

    /// Emit a value on a state node.
    pub fn emit(&self, node_id: NodeId, new_handle: HandleId) {
        self.core.emit(node_id, new_handle);
    }

    /// Read a node's current cache. Returns
    /// [`graphrefly_core::NO_HANDLE`] if sentinel.
    #[must_use]
    pub fn cache_of(&self, node_id: NodeId) -> HandleId {
        self.core.cache_of(node_id)
    }

    /// Whether the node's fn has fired at least once (compute) OR it has
    /// had a non-sentinel value (state).
    #[must_use]
    pub fn has_fired_once(&self, node_id: NodeId) -> bool {
        self.core.has_fired_once(node_id)
    }

    /// Mark the node terminal with COMPLETE.
    pub fn complete(&self, node_id: NodeId) {
        self.core.complete(node_id);
    }

    /// Mark the node terminal with ERROR.
    pub fn error(&self, node_id: NodeId, error_handle: HandleId) {
        self.core.error(node_id, error_handle);
    }

    /// Tear the node down (R2.6.4).
    pub fn teardown(&self, node_id: NodeId) {
        self.core.teardown(node_id);
    }

    /// Clear the node's cache and cascade `[INVALIDATE]` to dependents.
    pub fn invalidate(&self, node_id: NodeId) {
        self.core.invalidate(node_id);
    }

    /// Acquire a pause lock.
    pub fn pause(&self, node_id: NodeId, lock_id: LockId) -> Result<(), PauseError> {
        self.core.pause(node_id, lock_id)
    }

    /// Release a pause lock.
    pub fn resume(
        &self,
        node_id: NodeId,
        lock_id: LockId,
    ) -> Result<Option<ResumeReport>, PauseError> {
        self.core.resume(node_id, lock_id)
    }

    /// Allocate a fresh `LockId`.
    #[must_use]
    pub fn alloc_lock_id(&self) -> LockId {
        self.core.alloc_lock_id()
    }

    /// Atomically rewire a node's deps.
    ///
    /// # Hazards
    ///
    /// **Re-entrant `set_deps` from inside the firing node's own fn
    /// corrupts Dynamic `tracked` indices** (D1 in
    /// `~/src/graphrefly-rs/docs/porting-deferred.md`). If a Dynamic
    /// node `n`'s fn captures a `Graph` clone and re-enters
    /// `Graph::set_deps(n, ...)` from inside its own
    /// `BindingBoundary::invoke_fn` call, the dep ordering is
    /// rewritten while the fn-result `tracked: Some([...])` is being
    /// staged against the OLD ordering. Phase 3 of `fire_fn` then
    /// stores those stale indices into `rec.tracked` against the NEW
    /// dep vector — pointing at different upstream nodes than the fn
    /// intended to track.
    ///
    /// Acceptable v1: most `set_deps` callers are external
    /// orchestrators, not the firing node itself. The structural
    /// fix (a thread-local "currently firing" stack with
    /// `SetDepsError::ReentrantOnFiringNode`) lifts in a later
    /// slice.
    pub fn set_deps(&self, n: NodeId, new_deps: &[NodeId]) -> Result<(), SetDepsError> {
        self.core.set_deps(n, new_deps)
    }

    /// Mark the node as resubscribable (R2.2.7).
    pub fn set_resubscribable(&self, node_id: NodeId, resubscribable: bool) {
        self.core.set_resubscribable(node_id, resubscribable);
    }

    /// Attach `companion` as a meta companion of `parent` (R1.3.9.d).
    pub fn add_meta_companion(&self, parent: NodeId, companion: NodeId) {
        self.core.add_meta_companion(parent, companion);
    }

    /// Coalesce multiple emissions into a single wave.
    pub fn batch<F: FnOnce()>(&self, f: F) {
        self.core.batch(f);
    }

    // -------------------------------------------------------------------
    // Graph-level lifecycle (canonical spec §3.7)
    // -------------------------------------------------------------------

    /// General broadcast (canonical R3.7.1). Sends `kind` to every
    /// named node in this graph plus recursively into mounted children.
    ///
    /// Supported kinds:
    /// - `SignalKind::Invalidate` — with meta filtering per R3.7.2.
    /// - `SignalKind::Pause(lock_id)` — pause every named node.
    /// - `SignalKind::Resume(lock_id)` — resume every named node.
    ///
    /// Idempotent on a destroyed graph (no-op).
    pub fn signal(&self, kind: SignalKind) {
        match kind {
            SignalKind::Invalidate => self.signal_invalidate(),
            SignalKind::Pause(lock_id) => self.signal_pause(lock_id),
            SignalKind::Resume(lock_id) => self.signal_resume(lock_id),
            SignalKind::Complete => self.signal_complete(),
            SignalKind::Error(h) => self.signal_error(h),
        }
    }

    /// Two-phase tree-wide gather for `signal_pause` / `signal_resume` /
    /// `signal_complete` / `signal_error`. Same iterative worklist pattern
    /// as `collect_signal_invalidate_ids` but with meta-companion
    /// filtering added per D4 (Slice V3). Returns the flat list of
    /// `NodeId`s to signal.
    fn collect_signal_ids_with_meta_filter(&self) -> Vec<NodeId> {
        let mut out: Vec<NodeId> = Vec::new();
        let mut worklist: Vec<Graph> = vec![self.clone()];
        while let Some(graph) = worklist.pop() {
            let (own_ids, meta_set, child_clones) = {
                let inner = graph.inner.lock();
                if inner.destroyed {
                    continue;
                }
                let mut meta_set: std::collections::HashSet<NodeId> =
                    std::collections::HashSet::new();
                for &parent_id in inner.names.values() {
                    for child_id in graph.core.meta_companions_of(parent_id) {
                        meta_set.insert(child_id);
                    }
                }
                (
                    inner.names.values().copied().collect::<Vec<_>>(),
                    meta_set,
                    inner.children.values().cloned().collect::<Vec<_>>(),
                )
            };
            for id in own_ids {
                if meta_set.contains(&id) {
                    continue;
                }
                out.push(id);
            }
            worklist.extend(child_clones);
        }
        out
    }

    fn signal_pause(&self, lock_id: LockId) {
        // Slice V3 D4: filter meta companions, same as signal_invalidate.
        let ids = self.collect_signal_ids_with_meta_filter();
        for id in ids {
            let _ = self.core.pause(id, lock_id);
        }
    }

    fn signal_resume(&self, lock_id: LockId) {
        // Slice V3 D4: filter meta companions, same as signal_invalidate.
        let ids = self.collect_signal_ids_with_meta_filter();
        for id in ids {
            let _ = self.core.resume(id, lock_id);
        }
    }

    /// Broadcast `[COMPLETE]` to every named node in this graph plus
    /// recursively into mounted children (meta-filtered per D4).
    /// Idempotent on destroyed graph.
    /// Slice V3: extends `SignalKind` per D3 in porting-deferred.md.
    fn signal_complete(&self) {
        let ids = self.collect_signal_ids_with_meta_filter();
        for id in ids {
            self.core.complete(id);
        }
    }

    /// Broadcast `[ERROR, handle]` to every named node in this graph plus
    /// recursively into mounted children (meta-filtered per D4). Each
    /// `Core::error` call consumes one caller refcount share (via
    /// `try_error`'s unconditional `release_handle`), so we pre-retain
    /// N−1 additional shares before the loop. Idempotent on destroyed
    /// graph.
    /// Slice V3: extends `SignalKind` per D3 in porting-deferred.md.
    fn signal_error(&self, error_handle: HandleId) {
        let ids = self.collect_signal_ids_with_meta_filter();
        // F1 /qa fix: each core.error() releases one caller share.
        // Pre-retain (N-1) so the handle survives all N releases.
        for _ in 1..ids.len() {
            self.core.binding_ptr().retain_handle(error_handle);
        }
        for id in ids {
            self.core.error(id, error_handle);
        }
    }

    /// Broadcast `[INVALIDATE]` to every named node in this graph plus
    /// recursively into mounted children. Meta companions (R1.3.9.d /
    /// R2.3.3) are filtered out per canonical R3.7.2: their cached
    /// values are preserved across graph-wide invalidation.
    ///
    /// Filter happens at the **graph layer** because the Core invalidate
    /// cascade does NOT skip meta children — it walks every consumer in
    /// `children`. The graph-layer filter walks the namespace, builds a
    /// "set of node ids that are meta-companion-of-some-other-named-node",
    /// and excludes them from the iterate-and-invalidate loop. Direct
    /// `Core::invalidate(meta_id)` still wipes a meta's cache — the
    /// filter applies only to this graph-layer broadcast.
    ///
    /// **Two-phase tree-wide gather (E sub-slice, 2026-05-10).** Phase 1
    /// recursively walks the mount tree under per-graph locks (each
    /// child locked briefly to extract its names + meta filter + children
    /// snapshot, then released), accumulating a flat list of `NodeId`s to
    /// invalidate. Phase 2 runs `Core::invalidate` on the flat list —
    /// no Graph locks held during the cascade. The split:
    ///
    /// - Tightens the snapshot to **whole-mount-tree** scope: an `add`
    ///   to a subtree-root graph AFTER its snapshot completes won't
    ///   cause a parent-graph invalidate to fire on the new node, but
    ///   it also won't sneak into an interleaved invalidate cascade
    ///   either. Mid-walk topology mutations are visible only to
    ///   not-yet-snapshotted subgraphs.
    /// - Preserves the lock-acquisition rule (R3.7.2 / Slice F /qa D2):
    ///   `Graph::inner` is dropped BEFORE any `Core::invalidate` call.
    ///   Core's invalidate cascade re-entering the Graph layer (for
    ///   describe-reactive notifications, etc.) cannot deadlock against
    ///   a Graph-held lock.
    /// - Makes invalidate ordering deterministic: DFS pre-order (parent
    ///   before children) across the entire mount tree.
    ///
    /// Idempotent on a destroyed graph (no-op).
    pub fn signal_invalidate(&self) {
        let mut to_invalidate: Vec<NodeId> = Vec::new();
        self.collect_signal_invalidate_ids(&mut to_invalidate);
        for id in to_invalidate {
            self.core.invalidate(id);
        }
    }

    /// Iterative gather pass for [`Self::signal_invalidate`]. Pushes
    /// every named node id (modulo meta-companion filter) across this
    /// graph and its mount subtree into `out`.
    ///
    /// Uses an explicit `Vec<Graph>` worklist instead of recursion to
    /// avoid stack overflow on deep mount trees (porting-deferred.md
    /// lift point). Each subgraph is locked briefly and independently
    /// — no nested locks. Skips destroyed subgraphs (idempotent).
    /// Order is unspecified (invalidate is order-independent).
    fn collect_signal_invalidate_ids(&self, out: &mut Vec<NodeId>) {
        let mut worklist: Vec<Graph> = vec![self.clone()];
        while let Some(graph) = worklist.pop() {
            let (own_ids, meta_set, child_clones) = {
                let inner = graph.inner.lock();
                if inner.destroyed {
                    continue;
                }
                // Build the set of ids that are meta-companions of any other
                // named node in this graph. Names that point at a meta of
                // some named parent get filtered.
                let mut meta_set: std::collections::HashSet<NodeId> =
                    std::collections::HashSet::new();
                for &parent_id in inner.names.values() {
                    for child_id in graph.core.meta_companions_of(parent_id) {
                        meta_set.insert(child_id);
                    }
                }
                (
                    inner.names.values().copied().collect::<Vec<_>>(),
                    meta_set,
                    inner.children.values().cloned().collect::<Vec<_>>(),
                )
            };
            for id in own_ids {
                if meta_set.contains(&id) {
                    continue;
                }
                out.push(id);
            }
            worklist.extend(child_clones);
        }
    }

    /// Tear down every named node in this graph plus recursively into
    /// mounted children, then clear namespace + mount-tree state.
    ///
    /// The underlying [`Core`] is NOT dropped — other clones of this
    /// graph (or the Core itself) may still be alive. Only namespace
    /// + mount tree are cleared.
    ///
    /// **Cascade order (canonical R3.7.3 — "After cascade, graph
    /// internal registries are cleared").** Mark `destroyed = true`
    /// to refuse new mutations, snapshot ids + children under the
    /// lock, drop the lock, recurse into children, fire
    /// `core.teardown(id)` per own id (sinks see TEARDOWN with the
    /// namespace still populated — `Graph::name_of` resolves), THEN
    /// reacquire the lock and clear the namespace. Without this
    /// order, sinks that look up names during the TEARDOWN cascade
    /// would see `None`.
    pub fn destroy(&self) {
        let (own_ids, child_clones) = {
            let mut inner = self.inner.lock();
            if inner.destroyed {
                return; // Idempotent.
            }
            inner.destroyed = true;
            let own = inner.names.values().copied().collect::<Vec<_>>();
            let kids = inner.children.values().cloned().collect::<Vec<_>>();
            (own, kids)
        };
        for child in child_clones {
            child.destroy();
        }
        for id in own_ids {
            self.core.teardown(id);
        }
        // Clear registries AFTER the teardown cascade fires so sinks
        // observing TEARDOWN can still resolve names via the graph
        // namespace. R3.7.3 ordering.
        {
            let mut inner = self.inner.lock();
            inner.names.clear();
            inner.names_inverse.clear();
            inner.children.clear();
        }
        self.fire_namespace_change();
    }

    // -------------------------------------------------------------------
    // Mount delegations (impl in `crate::mount`)
    // -------------------------------------------------------------------

    /// Embed an existing `child` graph as a subgraph under `name`.
    /// `child` must share this graph's `Core` (cross-Core mount is
    /// post-M6 per session-doc Open Question 1).
    ///
    /// Returns the registered child for chaining.
    pub fn mount(&self, name: impl Into<String>, child: Graph) -> Result<Graph, MountError> {
        crate::mount::mount(self, name.into(), child)
    }

    /// Create an empty subgraph sharing this graph's `Core`, mounted
    /// under `name`.
    pub fn mount_new(&self, name: impl Into<String>) -> Result<Graph, MountError> {
        crate::mount::mount_new(self, name.into())
    }

    /// Builder pattern: create an empty subgraph, run `builder` against
    /// it, then return the registered subgraph.
    pub fn mount_with<F: FnOnce(&Graph)>(
        &self,
        name: impl Into<String>,
        builder: F,
    ) -> Result<Graph, MountError> {
        let child = self.mount_new(name)?;
        builder(&child);
        Ok(child)
    }

    /// Detach a previously-mounted subgraph. Tears it down (TEARDOWN
    /// cascade across the child's nodes + recursively into the child's
    /// mounts) and returns a [`GraphRemoveAudit`] describing what was
    /// removed.
    pub fn unmount(&self, name: &str) -> Result<GraphRemoveAudit, MountError> {
        crate::mount::unmount(self, name)
    }

    /// Parent chain (root last). `include_self = true` prepends this
    /// graph; `false` returns ancestors only.
    #[must_use]
    pub fn ancestors(&self, include_self: bool) -> Vec<Graph> {
        crate::mount::ancestors(self, include_self)
    }
}
