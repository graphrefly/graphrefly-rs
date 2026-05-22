//! `Graph` container — a **Core-free** namespace + mount tree (D246).
//!
//! D246 β-simplification: the actor-model [`Core`] is move-only and
//! single-owner; the embedder owns it (via
//! [`graphrefly_core::OwnedCore`]). `Graph` carries **no `Core` and no
//! `&Core`** — it is purely the named namespace + mount tree. There is
//! **one** `Graph` type (no `SubgraphRef`/`GraphOps`/`NamespaceHandle`,
//! no `'g` lifetime): a subgraph is just another `Graph` handle into a
//! child node of the tree (a cheap `Arc` clone). Every Core-touching op
//! takes an explicit `&Core` first argument (D246 rule 2 — the owner
//! always has it; one arg, trivially-correct ownership). Pure-namespace
//! ops (`node`, `name_of`, `try_resolve`, …) take no `Core`.
//!
//! `Graph` is `Clone` (an `Rc` bump) and intentionally `!Send + !Sync`
//! (D246/S2c single-owner: `Rc<RefCell<GraphInner>>`). It lives on, and
//! is touched only by, the one thread that owns the `Core` — captured
//! into owner-side `Sink`/`MailboxOp::Defer` closures (also `!Send`
//! post-D248). It replaces the old `NamespaceHandle`.
//!
//! `mount` / `describe` / `observe` / `snapshot` live in sibling
//! modules and follow the same `&Core`-explicit convention.

use std::cell::RefCell;
use std::rc::{Rc, Weak};

use graphrefly_core::{
    Core, EqualsMode, FnId, HandleId, LockId, NodeId, PauseError, ResumeReport, SetDepsError, Sink,
    SubscriptionId, TopologySubscriptionId,
};
use indexmap::IndexMap;

use crate::debug::DebugBindingBoundary;
use crate::describe::{
    describe_of, describe_reactive_in, DescribeSink, GraphDescribeOutput, ReactiveDescribeHandle,
};
use crate::mount::{GraphRemoveAudit, MountError};
use crate::observe::{GraphObserveAll, GraphObserveAllReactive, GraphObserveOne};

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
    Complete,
    /// Mark every named node as terminal with `ERROR` carrying the handle.
    Error(HandleId),
}

/// Path resolution errors returned by [`Graph::try_resolve_checked`].
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

/// Callback type for graph-level namespace change notifications, used
/// by reactive describe and reactive `observe_all`.
///
/// D246 (rule 2/6): namespace changes (`add`/`remove`/`mount`/`unmount`/
/// `destroy`) are **owner-invoked** — the caller holds `&Core` — so the
/// sink receives that `&Core` and may re-snapshot / re-subscribe
/// synchronously owner-side. (The in-wave `DepsChanged`/`NodeTornDown`
/// re-entry path is the one that defers via `MailboxOp::Defer` — see
/// `describe.rs`/`observe.rs`.)
pub(crate) type NamespaceChangeSink = Rc<dyn Fn(&Core)>;

static_assertions::assert_not_impl_any!(NamespaceChangeSink: Send, Sync);

/// Inner namespace + mount-tree state for one graph level. D246: holds
/// **no `Core`** — purely the named namespace, the mounted children,
/// and the namespace-change sinks. D246/S2c/D247: single-owner ⇒
/// `Rc<RefCell<GraphInner>>` (the prior `Arc<Mutex<…>>` was
/// shared-Core-era legacy); the shareable-handle + parent-`Weak` cycle
/// stays, single-threaded.
pub struct GraphInner {
    pub(crate) name: String,
    /// Local namespace: name → `NodeId`. Insertion order is load-bearing
    /// for `describe()` stability.
    pub(crate) names: IndexMap<String, NodeId>,
    /// Reverse lookup.
    pub(crate) names_inverse: IndexMap<NodeId, String>,
    /// Mounted child subgraphs — each is just another level's inner
    /// state (no `Core`; the single owned `Core` is the embedder's, and
    /// is threaded as `&Core` through every Core-touching tree-walk).
    pub(crate) children: IndexMap<String, Rc<RefCell<GraphInner>>>,
    /// Parent inner-state pointer (for `ancestors()`). Weak to break
    /// the strong cycle.
    pub(crate) parent: Option<Weak<RefCell<GraphInner>>>,
    /// True after `destroy()` completes — subsequent mutations refuse.
    pub(crate) destroyed: bool,
    /// Namespace-change sinks — fired from `add()`, `remove()`, etc.
    /// after the inner lock is dropped. Keyed by subscription id.
    pub(crate) namespace_sinks: IndexMap<u64, NamespaceChangeSink>,
    pub(crate) next_ns_sink_id: u64,
}

/// Graph container — the one Core-free namespace + mount-tree handle
/// (canonical §3, D246).
///
/// `Clone` is a cheap `Rc` bump; a subgraph (from `mount*`/`ancestors`)
/// is just a `Graph` into a child level. Intentionally `!Send + !Sync`
/// (D246/S2c single-owner): no `Core`, no borrow — captured into
/// owner-side sinks on the one thread that owns the `Core`. Pass the
/// embedder's `&Core` (e.g. `owned.core()`) into every Core-touching
/// method.
#[derive(Clone)]
pub struct Graph {
    pub(crate) inner: Rc<RefCell<GraphInner>>,
}

impl std::fmt::Debug for Graph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.borrow_mut();
        f.debug_struct("Graph")
            .field("name", &inner.name)
            .field("node_count", &inner.names.len())
            .field("subgraph_count", &inner.children.len())
            .field("destroyed", &inner.destroyed)
            .finish_non_exhaustive()
    }
}

// `state`/`derived`/`dynamic` use `.expect()` on invariant-unreachable
// `register_*` error paths (caller-validated) — same documented stance
// the pre-D246 `GraphOps` trait carried via this exact allow.
#[allow(clippy::missing_panics_doc, clippy::must_use_candidate)]
impl Graph {
    /// Construct a named, empty root graph. D246: the graph is
    /// Core-free — the embedder owns the `Core` (see
    /// [`graphrefly_core::OwnedCore`]) and passes `&Core` into
    /// Core-touching ops.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self::with_parent(name.into(), None)
    }

    pub(crate) fn with_parent(name: String, parent: Option<Weak<RefCell<GraphInner>>>) -> Self {
        Self {
            inner: Rc::new(RefCell::new(GraphInner {
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

    /// Wrap an existing inner level as a `Graph` handle (mount/ancestors).
    pub(crate) fn from_inner(inner: Rc<RefCell<GraphInner>>) -> Self {
        Self { inner }
    }

    /// This level's namespace handle (the `Graph` ↔ tree-walk seam).
    #[inline]
    pub(crate) fn inner_arc(&self) -> &Rc<RefCell<GraphInner>> {
        &self.inner
    }

    /// Whether `name` is a legal local node/subgraph name.
    #[must_use]
    pub fn is_valid_name(name: &str) -> bool {
        !name.contains(PATH_SEP) && !name.starts_with("_anon_")
    }

    /// The graph's name as set at construction (or via `mount`).
    #[must_use]
    pub fn name(&self) -> String {
        self.inner.borrow_mut().name.clone()
    }

    // --- namespace-change sinks (used by observe / describe / mount) ---

    /// Subscribe to namespace changes (add, remove, mount, unmount,
    /// destroy). The sink fires AFTER the inner lock is dropped, with
    /// the owner's `&Core`. Returns a subscription id.
    pub fn subscribe_namespace_change(&self, sink: NamespaceChangeSink) -> u64 {
        register_ns_sink(&self.inner, sink)
    }

    /// Unsubscribe a namespace-change sink by id (owner-invoked,
    /// D246 rule 3 — no RAII below the binding).
    pub fn unsubscribe_namespace_change(&self, id: u64) {
        unregister_ns_sink(&self.inner, id);
    }

    // --------------------- describe / observe (§3.6) -------------------

    /// Snapshot the graph's topology + lifecycle state (JSON form).
    /// `value` fields are raw u64 handles.
    #[must_use]
    pub fn describe(&self, core: &Core) -> GraphDescribeOutput {
        describe_of(core, &self.inner, None)
    }

    /// [`Self::describe`] with each `value` rendered via `debug`.
    #[must_use]
    pub fn describe_with_debug(
        &self,
        core: &Core,
        debug: &dyn DebugBindingBoundary,
    ) -> GraphDescribeOutput {
        describe_of(core, &self.inner, Some(debug))
    }

    /// Subscribe to live topology snapshots (canonical §3.6.1
    /// `reactive: true`). Push-on-subscribe + re-fires on namespace
    /// changes and `set_deps`. D246 rule 3: returns an id-bearing
    /// handle with an explicit owner-invoked
    /// [`ReactiveDescribeHandle::detach`] — no RAII `Drop`.
    #[must_use = "dropping the handle without calling .detach(&core) leaks the topology sub"]
    pub fn describe_reactive(&self, core: &Core, sink: &DescribeSink) -> ReactiveDescribeHandle {
        describe_reactive_in(core, &self.inner, sink)
    }

    /// Tap a single node's downstream message stream. Pure-namespace
    /// resolution; pass `&Core` to the returned handle's `subscribe`.
    #[must_use = "GraphObserveOne is only useful via .subscribe(...) — dropping it silently no-ops"]
    pub fn observe(&self, path: &str) -> GraphObserveOne {
        let id = self.node(path);
        GraphObserveOne::new(self.clone(), id)
    }

    /// Tap every named node in this graph (snapshot at `subscribe()`).
    #[must_use = "GraphObserveAll is only useful via .subscribe(...) — dropping it silently no-ops"]
    pub fn observe_all(&self) -> GraphObserveAll {
        GraphObserveAll::new(self.clone())
    }

    /// Tap every named node AND auto-subscribe late-added nodes.
    #[must_use = "GraphObserveAllReactive is only useful via .subscribe(...) — dropping it silently no-ops"]
    pub fn observe_all_reactive(&self) -> GraphObserveAllReactive {
        GraphObserveAllReactive::new(self.clone())
    }

    /// Fire all namespace-change sinks owner-side (D246 rule 2 —
    /// caller holds `&Core`).
    pub fn fire_namespace_change(&self, core: &Core) {
        fire_ns(core, &self.inner);
    }

    // ------------------------- namespace (§3.5) -------------------------

    /// Register an existing `node_id` under `name` in this graph's
    /// namespace.
    ///
    /// # Errors
    /// See [`NameError`].
    pub fn add(
        &self,
        core: &Core,
        node_id: NodeId,
        name: impl Into<String>,
    ) -> Result<NodeId, NameError> {
        let name = name.into();
        validate_name(&name)?;
        {
            let mut inner = self.inner.borrow_mut();
            if inner.destroyed {
                return Err(NameError::Destroyed);
            }
            if inner.names.contains_key(&name) {
                return Err(NameError::Collision(name));
            }
            inner.names.insert(name.clone(), node_id);
            inner.names_inverse.insert(node_id, name);
        }
        self.fire_namespace_change(core);
        Ok(node_id)
    }

    /// Resolve a path to a `NodeId`. Panics if missing.
    #[must_use]
    pub fn node(&self, path: &str) -> NodeId {
        self.try_resolve(path)
            .unwrap_or_else(|| panic!("Graph::node: no node at path `{path}`"))
    }

    /// Non-panicking [`Self::node`].
    #[must_use]
    pub fn try_resolve(&self, path: &str) -> Option<NodeId> {
        self.try_resolve_checked(path).ok().flatten()
    }

    /// Path resolution with typed errors (pure-namespace; never touches
    /// `Core`).
    ///
    /// # Errors
    /// See [`PathError`].
    pub fn try_resolve_checked(&self, path: &str) -> Result<Option<NodeId>, PathError> {
        resolve_checked(&self.inner, path)
    }

    /// Reverse lookup: the local name for a `node_id`.
    #[must_use]
    pub fn name_of(&self, node_id: NodeId) -> Option<String> {
        self.inner.borrow_mut().names_inverse.get(&node_id).cloned()
    }

    /// Number of named nodes in this graph.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.inner.borrow_mut().names.len()
    }

    /// Snapshot of local node names in insertion order.
    #[must_use]
    pub fn node_names(&self) -> Vec<String> {
        self.inner.borrow_mut().names.keys().cloned().collect()
    }

    /// Snapshot of mounted child names in insertion order.
    #[must_use]
    pub fn child_names(&self) -> Vec<String> {
        self.inner.borrow_mut().children.keys().cloned().collect()
    }

    /// Returns `true` after [`Self::destroy`] has been called.
    #[must_use]
    pub fn is_destroyed(&self) -> bool {
        self.inner.borrow_mut().destroyed
    }

    // ---------------------- sugar constructors (§3.9) -------------------

    /// Register a state node under `name`.
    ///
    /// # Errors
    /// See [`NameError`].
    pub fn state(
        &self,
        core: &Core,
        name: impl Into<String>,
        initial: Option<HandleId>,
    ) -> Result<NodeId, NameError> {
        let id = core
            .register_state(initial.unwrap_or(graphrefly_core::NO_HANDLE), false)
            .expect("invariant: register_state has no error variants reachable for caller-controlled inputs");
        self.add(core, id, name)
    }

    /// Register a static-derived node (fn fires on every dep change).
    ///
    /// # Errors
    /// See [`NameError`].
    pub fn derived(
        &self,
        core: &Core,
        name: impl Into<String>,
        deps: &[NodeId],
        fn_id: FnId,
        equals: EqualsMode,
    ) -> Result<NodeId, NameError> {
        let id = core
            .register_derived(deps, fn_id, equals, false)
            .expect("invariant: caller has validated dep ids before calling register_derived");
        self.add(core, id, name)
    }

    /// Register a dynamic-derived node (fn declares read dep indices).
    ///
    /// # Errors
    /// See [`NameError`].
    pub fn dynamic(
        &self,
        core: &Core,
        name: impl Into<String>,
        deps: &[NodeId],
        fn_id: FnId,
        equals: EqualsMode,
    ) -> Result<NodeId, NameError> {
        let id = core
            .register_dynamic(deps, fn_id, equals, false)
            .expect("invariant: caller has validated dep ids before calling register_dynamic");
        self.add(core, id, name)
    }

    // -------------------- named-sugar wrappers (§3.2.1) -----------------

    /// Emit a value on a named state node.
    pub fn set(&self, core: &Core, name: &str, handle: HandleId) {
        let id = self.node(name);
        core.emit(id, handle);
    }

    /// Read the cached value of a named node.
    #[must_use]
    pub fn get(&self, core: &Core, name: &str) -> HandleId {
        let id = self.node(name);
        core.cache_of(id)
    }

    /// Clear the cache of a named node and cascade `[INVALIDATE]`.
    pub fn invalidate_by_name(&self, core: &Core, name: &str) {
        let id = self.node(name);
        core.invalidate(id);
    }

    /// Mark a named node terminal with COMPLETE.
    pub fn complete_by_name(&self, core: &Core, name: &str) {
        let id = self.node(name);
        core.complete(id);
    }

    /// Mark a named node terminal with ERROR.
    pub fn error_by_name(&self, core: &Core, name: &str, error_handle: HandleId) {
        let id = self.node(name);
        core.error(id, error_handle);
    }

    // --------------------------- remove (§3.2.3) ------------------------

    /// Remove a named node OR mounted subgraph (R3.2.3 / R3.7.3
    /// ordering — namespace cleared AFTER the TEARDOWN cascade).
    ///
    /// # Errors
    /// See [`RemoveError`].
    pub fn remove(&self, core: &Core, name: &str) -> Result<GraphRemoveAudit, RemoveError> {
        {
            let inner = self.inner.borrow_mut();
            if inner.destroyed {
                return Err(RemoveError::Destroyed);
            }
            if inner.children.contains_key(name) {
                drop(inner);
                return self.unmount(core, name).map_err(|e| match e {
                    MountError::Destroyed => RemoveError::Destroyed,
                    _ => RemoveError::NotFound(name.to_owned()),
                });
            }
        }
        let node_id = {
            let inner = self.inner.borrow_mut();
            if inner.destroyed {
                return Err(RemoveError::Destroyed);
            }
            *inner
                .names
                .get(name)
                .ok_or_else(|| RemoveError::NotFound(name.to_owned()))?
        };
        core.teardown(node_id);
        {
            let mut inner = self.inner.borrow_mut();
            inner.names.shift_remove(name);
            inner.names_inverse.shift_remove(&node_id);
        }
        self.fire_namespace_change(core);
        Ok(GraphRemoveAudit {
            node_count: 1,
            mount_count: 0,
        })
    }

    // --------------------------- edges (§3.3.1) -------------------------

    /// Derive `[from, to]` edge name pairs. `recursive` qualifies names
    /// across the mount tree with `::`.
    #[must_use]
    pub fn edges(&self, core: &Core, recursive: bool) -> Vec<(String, String)> {
        let names_map = collect_qualified_names_in(&self.inner, "", recursive);
        edges_in(core, &self.inner, "", recursive, &names_map)
    }

    // ------------------- lifecycle pass-throughs (§3.7) -----------------

    /// Subscribe a sink. Returns a [`SubscriptionId`]; pass it back to
    /// [`Self::unsubscribe`] (owner-invoked, synchronous — D246 rule 3;
    /// no RAII below the binding).
    #[must_use = "the SubscriptionId must be kept to later unsubscribe; dropping it leaks the sink"]
    pub fn subscribe(&self, core: &Core, node_id: NodeId, sink: Sink) -> SubscriptionId {
        core.subscribe(node_id, sink)
    }

    /// Detach a sink previously registered via [`Self::subscribe`].
    pub fn unsubscribe(&self, core: &Core, node_id: NodeId, sub_id: SubscriptionId) {
        core.unsubscribe(node_id, sub_id);
    }

    /// Detach a Core topology subscription by id.
    pub fn unsubscribe_topology(&self, core: &Core, id: TopologySubscriptionId) {
        core.unsubscribe_topology(id);
    }

    /// Emit a value on a state node.
    pub fn emit(&self, core: &Core, node_id: NodeId, new_handle: HandleId) {
        core.emit(node_id, new_handle);
    }

    /// Read a node's current cache.
    #[must_use]
    pub fn cache_of(&self, core: &Core, node_id: NodeId) -> HandleId {
        core.cache_of(node_id)
    }

    /// Whether the node's fn has fired at least once.
    #[must_use]
    pub fn has_fired_once(&self, core: &Core, node_id: NodeId) -> bool {
        core.has_fired_once(node_id)
    }

    /// Mark the node terminal with COMPLETE.
    pub fn complete(&self, core: &Core, node_id: NodeId) {
        core.complete(node_id);
    }

    /// Mark the node terminal with ERROR.
    pub fn error(&self, core: &Core, node_id: NodeId, error_handle: HandleId) {
        core.error(node_id, error_handle);
    }

    /// Tear the node down (R2.6.4).
    pub fn teardown(&self, core: &Core, node_id: NodeId) {
        core.teardown(node_id);
    }

    /// Clear the node's cache and cascade `[INVALIDATE]`.
    pub fn invalidate(&self, core: &Core, node_id: NodeId) {
        core.invalidate(node_id);
    }

    /// Acquire a pause lock.
    ///
    /// # Errors
    /// See [`PauseError`].
    pub fn pause(&self, core: &Core, node_id: NodeId, lock_id: LockId) -> Result<(), PauseError> {
        core.pause(node_id, lock_id)
    }

    /// Release a pause lock.
    ///
    /// # Errors
    /// See [`PauseError`].
    pub fn resume(
        &self,
        core: &Core,
        node_id: NodeId,
        lock_id: LockId,
    ) -> Result<Option<ResumeReport>, PauseError> {
        core.resume(node_id, lock_id)
    }

    /// Allocate a fresh `LockId`.
    #[must_use]
    pub fn alloc_lock_id(&self, core: &Core) -> LockId {
        core.alloc_lock_id()
    }

    /// Atomically rewire a node's deps.
    ///
    /// # Errors
    /// See [`SetDepsError`].
    ///
    /// # Hazards
    ///
    /// Re-entrant `set_deps` from inside the firing node's own fn
    /// corrupts Dynamic `tracked` indices (D1 in
    /// `~/src/graphrefly-rs/docs/porting-deferred.md`). Acceptable v1:
    /// most callers are external orchestrators, not the firing node.
    pub fn set_deps(
        &self,
        core: &Core,
        n: NodeId,
        new_deps: &[NodeId],
    ) -> Result<(), SetDepsError> {
        core.set_deps(n, new_deps)
    }

    /// Mark the node as resubscribable (R2.2.7).
    pub fn set_resubscribable(&self, core: &Core, node_id: NodeId, resubscribable: bool) {
        core.set_resubscribable(node_id, resubscribable);
    }

    /// Attach `companion` as a meta companion of `parent` (R1.3.9.d).
    pub fn add_meta_companion(&self, core: &Core, parent: NodeId, companion: NodeId) {
        core.add_meta_companion(parent, companion);
    }

    /// Coalesce multiple emissions into a single wave.
    pub fn batch<F: FnOnce()>(&self, core: &Core, f: F) {
        core.batch(f);
    }

    // --------------------- graph-level lifecycle (§3.7) -----------------

    /// General broadcast (canonical R3.7.1).
    pub fn signal(&self, core: &Core, kind: SignalKind) {
        match kind {
            SignalKind::Invalidate => self.signal_invalidate(core),
            SignalKind::Pause(lock_id) => {
                for id in collect_signal_ids_with_meta_filter(core, &self.inner) {
                    let _ = core.pause(id, lock_id);
                }
            }
            SignalKind::Resume(lock_id) => {
                for id in collect_signal_ids_with_meta_filter(core, &self.inner) {
                    let _ = core.resume(id, lock_id);
                }
            }
            SignalKind::Complete => {
                for id in collect_signal_ids_with_meta_filter(core, &self.inner) {
                    core.complete(id);
                }
            }
            SignalKind::Error(h) => {
                let ids = collect_signal_ids_with_meta_filter(core, &self.inner);
                // F1 /qa fix: each core.error() releases one caller share.
                // Pre-retain (N-1) so the handle survives all N releases.
                for _ in 1..ids.len() {
                    core.binding_ptr().retain_handle(h);
                }
                for id in ids {
                    core.error(id, h);
                }
            }
        }
    }

    /// Broadcast `[INVALIDATE]` across this graph + mount tree
    /// (meta-companion filtered per R3.7.2; Graph locks dropped before
    /// any `Core::invalidate`). Idempotent on a destroyed graph.
    pub fn signal_invalidate(&self, core: &Core) {
        let to_invalidate = collect_signal_invalidate_ids(core, &self.inner);
        for id in to_invalidate {
            core.invalidate(id);
        }
    }

    /// Tear down every named node + recursively into mounted children,
    /// then clear namespace + mount-tree state. R3.7.3 ordering
    /// (children-first, then own teardown, then clear, then fire
    /// ns-change) preserved verbatim.
    pub fn destroy(&self, core: &Core) {
        destroy_subtree(core, &self.inner);
    }

    // --------------------------- mount (§3.4) ---------------------------

    /// Embed an existing `child` subgraph under `name`. Fires ns-change
    /// sinks owner-side (P3) — hence `&Core` (D246 rule 2).
    ///
    /// # Errors
    /// See [`MountError`].
    pub fn mount(
        &self,
        core: &Core,
        name: impl Into<String>,
        child: &Graph,
    ) -> Result<Graph, MountError> {
        crate::mount::mount(core, &self.inner, name.into(), child)
    }

    /// Create an empty subgraph (shares the embedder's one `Core`).
    ///
    /// # Errors
    /// See [`MountError`].
    pub fn mount_new(&self, core: &Core, name: impl Into<String>) -> Result<Graph, MountError> {
        crate::mount::mount_new(core, &self.inner, name.into())
    }

    /// Builder pattern: create an empty subgraph, run `builder`, return it.
    ///
    /// # Errors
    /// See [`MountError`].
    pub fn mount_with<F: FnOnce(&Graph)>(
        &self,
        core: &Core,
        name: impl Into<String>,
        builder: F,
    ) -> Result<Graph, MountError> {
        let child = self.mount_new(core, name)?;
        builder(&child);
        Ok(child)
    }

    /// Detach a previously-mounted subgraph (TEARDOWN cascade).
    ///
    /// # Errors
    /// See [`MountError`].
    pub fn unmount(&self, core: &Core, name: &str) -> Result<GraphRemoveAudit, MountError> {
        crate::mount::unmount(core, &self.inner, name)
    }

    /// Parent chain (root last). `include_self = true` prepends this
    /// graph.
    #[must_use]
    pub fn ancestors(&self, include_self: bool) -> Vec<Graph> {
        crate::mount::ancestors(&self.inner, include_self)
    }
}

// =====================================================================
// D246/D237: free fns over (&Core, &Rc<RefCell<GraphInner>>)
// =====================================================================

fn validate_name(name: &str) -> Result<(), NameError> {
    if name.contains(PATH_SEP) {
        Err(NameError::InvalidName(name.to_owned()))
    } else if name.starts_with("_anon_") {
        Err(NameError::ReservedPrefix(name.to_owned()))
    } else {
        Ok(())
    }
}

/// Register a namespace-change sink on one graph level. Returns its id.
pub(crate) fn register_ns_sink(
    inner_arc: &Rc<RefCell<GraphInner>>,
    sink: NamespaceChangeSink,
) -> u64 {
    let mut inner = inner_arc.borrow_mut();
    let id = inner.next_ns_sink_id;
    inner.next_ns_sink_id += 1;
    inner.namespace_sinks.insert(id, sink);
    id
}

/// Remove a namespace-change sink by id (inner-only; no `Core`).
pub(crate) fn unregister_ns_sink(inner_arc: &Rc<RefCell<GraphInner>>, id: u64) {
    inner_arc.borrow_mut().namespace_sinks.shift_remove(&id);
}

/// Pure-namespace path resolution (R3.5.1/R3.5.2). Never touches `Core`.
pub(crate) fn resolve_checked(
    inner_arc: &Rc<RefCell<GraphInner>>,
    path: &str,
) -> Result<Option<NodeId>, PathError> {
    if path.is_empty() {
        return Err(PathError::Empty);
    }
    let inner = inner_arc.borrow_mut();
    if inner.destroyed {
        return Err(PathError::Destroyed);
    }
    let segments: Vec<&str> = path.split(PATH_SEP).collect();
    let first = segments[0];
    if first == ".." {
        let parent_weak = inner.parent.as_ref().ok_or(PathError::NoParent)?;
        let parent_inner = parent_weak.upgrade().ok_or(PathError::NoParent)?;
        drop(inner);
        if segments.len() == 1 {
            return Ok(None);
        }
        let rest = segments[1..].join(PATH_SEP);
        resolve_checked(&parent_inner, &rest)
    } else if segments.len() > 1 {
        let child = inner
            .children
            .get(first)
            .cloned()
            .ok_or_else(|| PathError::ChildNotFound(first.to_string()))?;
        drop(inner);
        let rest = segments[1..].join(PATH_SEP);
        resolve_checked(&child, &rest)
    } else {
        Ok(inner.names.get(first).copied())
    }
}

/// Fire one graph level's namespace-change sinks with the owner's
/// `&Core` (D246 rule 2 owner-side). Sinks run with no Graph lock held.
pub(crate) fn fire_ns(core: &Core, inner_arc: &Rc<RefCell<GraphInner>>) {
    let sinks: Vec<NamespaceChangeSink> = {
        let inner = inner_arc.borrow_mut();
        inner.namespace_sinks.values().cloned().collect()
    };
    for sink in sinks {
        sink(core);
    }
}

/// `destroy()` over `Rc<RefCell<GraphInner>>` + root `&Core`. Ordering
/// preserved verbatim (R3.7.3).
pub(crate) fn destroy_subtree(core: &Core, inner_arc: &Rc<RefCell<GraphInner>>) {
    let (own_ids, child_clones) = {
        let mut inner = inner_arc.borrow_mut();
        if inner.destroyed {
            return; // Idempotent.
        }
        inner.destroyed = true;
        let own = inner.names.values().copied().collect::<Vec<_>>();
        let kids = inner.children.values().cloned().collect::<Vec<_>>();
        (own, kids)
    };
    for child in &child_clones {
        destroy_subtree(core, child);
    }
    for id in own_ids {
        core.teardown(id);
    }
    {
        let mut inner = inner_arc.borrow_mut();
        inner.names.clear();
        inner.names_inverse.clear();
        inner.children.clear();
    }
    // Fire the final namespace change (reactive describe/observe see the
    // emptied graph) BEFORE dropping the sinks — so observers get the
    // destroy notification, then the sinks are released.
    fire_ns(core, inner_arc);
    // QA-A1: clear the namespace-change sinks on destroy. Without this a
    // reactive describe/observe ns-sink (registered via
    // `register_ns_sink`, NOT an `OwnedCore`-tracked Core sub) would
    // outlive `destroy()` for the whole `GraphInner` lifetime — the
    // documented `graph.destroy(core)` teardown fallback must actually
    // collect it. (The handle's Core *topology* sub is still owner-
    // detach-only — see the corrected `#[must_use]` text.)
    inner_arc.borrow_mut().namespace_sinks.clear();
}

/// Tree-wide gather for `signal_pause`/`resume`/`complete`/`error`
/// (meta-companion filtered per D4). `Rc<RefCell<GraphInner>>` worklist
/// + the single root `&Core`.
fn collect_signal_ids_with_meta_filter(core: &Core, root: &Rc<RefCell<GraphInner>>) -> Vec<NodeId> {
    let mut out: Vec<NodeId> = Vec::new();
    let mut worklist: Vec<Rc<RefCell<GraphInner>>> = vec![root.clone()];
    while let Some(inner_arc) = worklist.pop() {
        let (own_ids, meta_set, child_clones) = {
            let inner = inner_arc.borrow_mut();
            if inner.destroyed {
                continue;
            }
            let mut meta_set: std::collections::HashSet<NodeId> = std::collections::HashSet::new();
            for &parent_id in inner.names.values() {
                for child_id in core.meta_companions_of(parent_id) {
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

/// Iterative gather for `signal_invalidate` (DFS, meta-filtered).
fn collect_signal_invalidate_ids(core: &Core, root: &Rc<RefCell<GraphInner>>) -> Vec<NodeId> {
    let mut out: Vec<NodeId> = Vec::new();
    let mut worklist: Vec<Rc<RefCell<GraphInner>>> = vec![root.clone()];
    while let Some(inner_arc) = worklist.pop() {
        let (own_ids, meta_set, child_clones) = {
            let inner = inner_arc.borrow_mut();
            if inner.destroyed {
                continue;
            }
            let mut meta_set: std::collections::HashSet<NodeId> = std::collections::HashSet::new();
            for &parent_id in inner.names.values() {
                for child_id in core.meta_companions_of(parent_id) {
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

/// Build an `id → qualified-name` map across this graph + (if
/// `recursive`) its mount tree. Pure-namespace (no `Core`).
pub(crate) fn collect_qualified_names_in(
    inner_arc: &Rc<RefCell<GraphInner>>,
    prefix: &str,
    recursive: bool,
) -> IndexMap<NodeId, String> {
    let inner = inner_arc.borrow_mut();
    let mut map: IndexMap<NodeId, String> = inner
        .names
        .iter()
        .map(|(n, id)| (*id, format!("{prefix}{n}")))
        .collect();
    let children: Vec<(String, Rc<RefCell<GraphInner>>)> = if recursive {
        inner
            .children
            .iter()
            .map(|(n, g)| (n.clone(), g.clone()))
            .collect()
    } else {
        Vec::new()
    };
    drop(inner);
    for (child_name, child_inner) in children {
        let child_prefix = format!("{prefix}{child_name}::");
        let child_map = collect_qualified_names_in(&child_inner, &child_prefix, true);
        for (id, name) in child_map {
            map.entry(id).or_insert(name);
        }
    }
    map
}

/// Derive `[from, to]` edge name pairs. Needs the root `&Core`.
pub(crate) fn edges_in(
    core: &Core,
    inner_arc: &Rc<RefCell<GraphInner>>,
    prefix: &str,
    recursive: bool,
    names_map: &IndexMap<NodeId, String>,
) -> Vec<(String, String)> {
    let inner = inner_arc.borrow_mut();
    let qualified: Vec<(String, NodeId)> = inner
        .names
        .iter()
        .map(|(n, id)| (format!("{prefix}{n}"), *id))
        .collect();
    let children: Vec<(String, Rc<RefCell<GraphInner>>)> = if recursive {
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
        let dep_ids = core.deps_of(*id);
        for dep_id in dep_ids {
            let from_name = names_map
                .get(&dep_id)
                .cloned()
                .unwrap_or_else(|| format!("{prefix}_anon_{}", dep_id.raw()));
            result.push((from_name, to_name.clone()));
        }
    }
    for (child_name, child_inner) in children {
        let child_prefix = format!("{prefix}{child_name}::");
        result.extend(edges_in(core, &child_inner, &child_prefix, true, names_map));
    }
    result
}

// D247/D248: the QA-A4 `Graph: Send + Sync + 'static` assertion is
// **deleted**. Under D246/S2c single-owner the namespace tree is
// `Rc<RefCell<GraphInner>>` (the `Arc<Mutex<>>` + `Send+Sync` was
// shared-Core-era legacy), so `Graph` is intentionally `!Send + !Sync`
// — it lives on, and is touched only by, the one thread that owns the
// `Core`.
