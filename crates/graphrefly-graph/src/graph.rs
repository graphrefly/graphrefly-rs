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

use crate::mount::{MountError, RemoveAudit};

/// Namespace path separator (canonical spec R3.5.1).
pub(crate) const PATH_SEP: &str = "::";

/// Errors from name registration.
#[derive(Debug, thiserror::Error)]
pub enum NameError {
    #[error("Graph::add: name `{0}` already registered in this graph")]
    Collision(String),
    #[error("Graph: name `{0}` may not contain the `::` path separator")]
    InvalidName(String),
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
            })),
        }
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

    /// Whether `name` is a legal local node/subgraph name (no `::`).
    #[must_use]
    pub fn is_valid_name(name: &str) -> bool {
        !name.contains(PATH_SEP)
    }

    fn validate_name(name: &str) -> Result<(), NameError> {
        if name.contains(PATH_SEP) {
            Err(NameError::InvalidName(name.to_owned()))
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

    /// Non-panicking variant of [`Self::node`].
    #[must_use]
    pub fn try_resolve(&self, path: &str) -> Option<NodeId> {
        let mut segments = path.split(PATH_SEP);
        let first = segments.next()?;
        let inner = self.inner.lock();
        if let Some(rest) = path
            .strip_prefix(first)
            .and_then(|r| r.strip_prefix(PATH_SEP))
        {
            // first segment is a child graph name; recurse into it.
            let child = inner.children.get(first)?.clone();
            drop(inner);
            child.try_resolve(rest)
        } else {
            // single segment — local lookup.
            inner.names.get(first).copied()
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
    pub fn state(
        &self,
        name: impl Into<String>,
        initial: Option<HandleId>,
    ) -> Result<NodeId, NameError> {
        let id = self
            .core
            .register_state(initial.unwrap_or(graphrefly_core::NO_HANDLE));
        self.add(id, name)
    }

    /// Register a static-derived node — fn fires on every dep change
    /// with all deps tracked.
    pub fn derived(
        &self,
        name: impl Into<String>,
        deps: &[NodeId],
        fn_id: FnId,
        equals: EqualsMode,
    ) -> Result<NodeId, NameError> {
        let id = self.core.register_derived(deps, fn_id, equals);
        self.add(id, name)
    }

    /// Register a dynamic-derived node — fn declares which dep indices
    /// it actually read this run.
    pub fn dynamic(
        &self,
        name: impl Into<String>,
        deps: &[NodeId],
        fn_id: FnId,
        equals: EqualsMode,
    ) -> Result<NodeId, NameError> {
        let id = self.core.register_dynamic(deps, fn_id, equals);
        self.add(id, name)
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
    /// Idempotent on a destroyed graph (no-op).
    pub fn signal_invalidate(&self) {
        let (own_ids, meta_set, child_clones) = {
            let inner = self.inner.lock();
            if inner.destroyed {
                return;
            }
            // Build the set of ids that are meta-companions of any other
            // named node in this graph. Names that point at a meta of
            // some named parent get filtered.
            let mut meta_set: std::collections::HashSet<NodeId> = std::collections::HashSet::new();
            for &parent_id in inner.names.values() {
                for child_id in self.core.meta_companions_of(parent_id) {
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
            self.core.invalidate(id);
        }
        for child in child_clones {
            child.signal_invalidate();
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
        let mut inner = self.inner.lock();
        inner.names.clear();
        inner.names_inverse.clear();
        inner.children.clear();
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
    /// mounts) and returns a [`RemoveAudit`] describing what was
    /// removed.
    pub fn unmount(&self, name: &str) -> Result<RemoveAudit, MountError> {
        crate::mount::unmount(self, name)
    }

    /// Parent chain (root last). `include_self = true` prepends this
    /// graph; `false` returns ancestors only.
    #[must_use]
    pub fn ancestors(&self, include_self: bool) -> Vec<Graph> {
        crate::mount::ancestors(self, include_self)
    }
}
