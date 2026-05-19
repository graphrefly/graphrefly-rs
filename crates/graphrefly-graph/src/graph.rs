//! `Graph` container — namespace, sugar constructors, and lifecycle
//! pass-throughs over the single owned [`Core`].
//!
//! S2b/β (D237/D240/D242): the actor-model `Core` is move-only and
//! single-owner. The root [`Graph`] owns it by value (no `Clone`).
//! Subgraphs are borrowed [`SubgraphRef`]s carrying the root's `&Core`
//! + a namespace handle (`Arc<Mutex<GraphInner>>`). All Core-touching /
//! tree-walk logic lives in [`GraphOps`] trait defaults + free fns over
//! `(&Core, &Arc<Mutex<GraphInner>>)`, so `Graph` and `SubgraphRef`
//! share one implementation.
//!
//! `mount` / `describe` / `observe` live in sibling modules.

use std::sync::{Arc, Weak};

use graphrefly_core::{
    BindingBoundary, Core, CoreFull, EqualsMode, FnId, HandleId, LockId, NodeId, PauseError,
    ResumeReport, SetDepsError, Sink, SubscriptionId, TopologySubscriptionId,
};
use indexmap::IndexMap;
use parking_lot::Mutex;

use crate::debug::DebugBindingBoundary;
use crate::describe::{
    describe_of, describe_reactive_in, DescribeSink, GraphDescribeOutput, ReactiveDescribeHandle,
};
use crate::mount::{GraphRemoveAudit, MountError};
use crate::observe::{GraphObserveAll, GraphObserveAllReactive, GraphObserveOne};
use crate::snapshot::{snapshot_of, GraphPersistSnapshot};

/// Namespace path separator (canonical spec R3.5.1).
pub(crate) const PATH_SEP: &str = "::";

/// Errors from [`GraphOps::remove`].
#[derive(Debug, thiserror::Error)]
pub enum RemoveError {
    #[error("Graph::remove: name `{0}` not found (neither a node nor a mounted subgraph)")]
    NotFound(String),
    #[error("Graph::remove: graph has been destroyed")]
    Destroyed,
}

/// Signal kind for [`GraphOps::signal`] (canonical R3.7.1).
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

/// Path resolution errors returned by [`GraphOps::try_resolve_checked`].
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

/// Callback type for graph-level namespace change notifications.
/// Used by reactive describe and reactive `observe_all`.
///
/// S2b/β (D237/D240/D231): the sink receives the owner's `&Core`.
/// Namespace changes (`add`/`remove`/`mount`/`unmount`/`destroy`) fire
/// owner-side (the caller holds `&self.core`), so reactive
/// describe/observe sinks that re-snapshot or re-subscribe get the
/// relocatable owned `Core` by borrow at fire-time (NOT a stored/cloned
/// Core, NOT `em.defer`; the in-wave `DepsChanged`/`NodeTornDown`
/// re-entry path is the one that defers — see `describe.rs`/`observe.rs`).
pub(crate) type NamespaceChangeSink = Arc<dyn Fn(&Core) + Send + Sync>;

/// Opaque per-graph-level namespace + mount-tree handle. Public only
/// so the [`GraphOps`] `graph_inner` accessor (the `Graph` ↔
/// `SubgraphRef` unification seam) doesn't leak a more-private type;
/// it has no public fields or methods — not constructible or
/// inspectable by external callers.
pub struct GraphInner {
    pub(crate) name: String,
    /// Local namespace: name → `NodeId`. Insertion order is load-bearing
    /// for `describe()` stability.
    pub(crate) names: IndexMap<String, NodeId>,
    /// Reverse lookup.
    pub(crate) names_inverse: IndexMap<NodeId, String>,
    /// Mounted child subgraphs. S2b/β (D237/D240): a child is just its
    /// namespace handle `Arc<Mutex<GraphInner>>` — it does NOT carry a
    /// `Core`. The single owned `Core` lives on the root `Graph`; all
    /// Core-touching tree-walks thread the root's `&Core` (mount is
    /// shared-Core only ⇒ one Core serves the whole tree).
    pub(crate) children: IndexMap<String, Arc<Mutex<GraphInner>>>,
    /// Parent inner-state pointer (for `ancestors()`). Weak to break
    /// the strong cycle.
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
/// S2b/β (D237/D240): the root `Graph` **owns** the single relocatable
/// `Core` by value — it is **no longer `Clone`** (the actor-model Core
/// is move-only, `!Sync`). Subgraphs are [`SubgraphRef`] handles, not
/// standalone Core-carrying `Graph`s. The full operation surface is the
/// [`GraphOps`] trait (implemented for both `Graph` and `SubgraphRef`);
/// bring it into scope to call `state`/`derived`/`mount_new`/… on a
/// graph or subgraph.
pub struct Graph {
    pub(crate) core: Core,
    pub(crate) inner: Arc<Mutex<GraphInner>>,
}

impl std::fmt::Debug for Graph {
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

    /// Construct a fresh root [`Graph`] wrapping an existing [`Core`]
    /// the caller already owns (binding crates: napi-rs / pyo3 / wasm).
    #[must_use]
    pub fn with_existing_core(name: impl Into<String>, core: Core) -> Self {
        Self::with_core(name.into(), core, None)
    }

    /// Whether `name` is a legal local node/subgraph name.
    #[must_use]
    pub fn is_valid_name(name: &str) -> bool {
        !name.contains(PATH_SEP) && !name.starts_with("_anon_")
    }

    /// β/D242: a borrowed view of this root as a [`SubgraphRef`]
    /// (carries `&self.core`). The unifying primitive — `Graph`'s
    /// Core-touching methods and every subgraph op funnel through
    /// the [`GraphOps`] trait.
    #[must_use]
    pub fn view(&self) -> SubgraphRef<'_> {
        SubgraphRef {
            core: &self.core,
            inner: self.inner.clone(),
        }
    }
}

impl GraphOps for Graph {
    #[inline]
    fn graph_core(&self) -> &Core {
        &self.core
    }
    #[inline]
    fn graph_inner(&self) -> &Arc<Mutex<GraphInner>> {
        &self.inner
    }
}

// =====================================================================
// β/D242: SubgraphRef<'g> — borrowed subgraph handle
// =====================================================================

/// A borrowed handle to a (sub)graph: the root's `&'g Core` + the
/// node-namespace handle `Arc<Mutex<GraphInner>>`. Returned by
/// `mount`/`mount_new`/`mount_with`/`ancestors`/[`Graph::view`].
///
/// S2b/β (D237/D240/D242): the actor-model `Core` is move-only and
/// single-owner — only the root [`Graph`] owns it. A subgraph reaches
/// Core by **borrowing the owner's `&Core`** (D231: pass `&Core`
/// owner-side, never store/clone it), valid for as long as the root
/// `Graph` is alive (`'g`). Cloning a `SubgraphRef` is cheap (a
/// reference copy + an `Arc` bump); it carries no `Core`. The
/// operation surface is the [`GraphOps`] trait.
#[derive(Clone)]
pub struct SubgraphRef<'g> {
    pub(crate) core: &'g Core,
    pub(crate) inner: Arc<Mutex<GraphInner>>,
}

impl std::fmt::Debug for SubgraphRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock();
        f.debug_struct("SubgraphRef")
            .field("name", &inner.name)
            .field("node_count", &inner.names.len())
            .field("subgraph_count", &inner.children.len())
            .field("destroyed", &inner.destroyed)
            .finish_non_exhaustive()
    }
}

impl<'g> GraphOps for SubgraphRef<'g> {
    #[inline]
    fn graph_core(&self) -> &Core {
        self.core
    }
    #[inline]
    fn graph_inner(&self) -> &Arc<Mutex<GraphInner>> {
        &self.inner
    }
}

// =====================================================================
// β/D242: GraphOps — the unified operation surface
//
// One implementation, shared by `Graph` (root, owns Core) and
// `SubgraphRef<'g>` (borrowed). Required: the two accessors. Everything
// else is a default method over `self.graph_core()` / `self.graph_inner()`
// + the free fns at the end of this file.
// =====================================================================

#[allow(clippy::missing_panics_doc, clippy::must_use_candidate)]
pub trait GraphOps {
    /// The single owned dispatcher (borrowed). On a [`Graph`] this is
    /// the owned `Core`; on a [`SubgraphRef`] it is the root's `&Core`.
    fn graph_core(&self) -> &Core;
    /// This (sub)graph level's namespace handle.
    fn graph_inner(&self) -> &Arc<Mutex<GraphInner>>;

    /// Borrow the underlying [`Core`] — the M1 dispatcher.
    fn core(&self) -> &Core {
        self.graph_core()
    }

    /// A borrowed [`SubgraphRef`] view of this (sub)graph level.
    fn as_subgraph(&self) -> SubgraphRef<'_> {
        SubgraphRef {
            core: self.graph_core(),
            inner: self.graph_inner().clone(),
        }
    }

    /// The graph's name as set at construction (or via `mount`).
    fn name(&self) -> String {
        self.graph_inner().lock().name.clone()
    }

    // --- namespace-change sinks (used by observe / describe / mount) ---

    /// Subscribe to namespace changes (add, remove, mount, unmount,
    /// destroy). The sink fires AFTER the inner lock is dropped, with
    /// the owner's `&Core`. Returns a subscription id.
    fn subscribe_namespace_change(&self, sink: NamespaceChangeSink) -> u64 {
        register_ns_sink(self.graph_inner(), sink)
    }

    /// Unsubscribe a namespace-change sink by id.
    fn unsubscribe_namespace_change(&self, id: u64) {
        unregister_ns_sink(self.graph_inner(), id);
    }

    // --------------------- describe / observe (§3.6) -------------------

    /// Snapshot the graph's topology + lifecycle state (JSON form).
    /// `value` fields are raw u64 handles.
    fn describe(&self) -> GraphDescribeOutput {
        describe_of(self.graph_core(), self.graph_inner(), None)
    }

    /// [`Self::describe`] with each `value` rendered via `debug`.
    fn describe_with_debug(&self, debug: &dyn DebugBindingBoundary) -> GraphDescribeOutput {
        describe_of(self.graph_core(), self.graph_inner(), Some(debug))
    }

    /// Subscribe to live topology snapshots (canonical §3.6.1
    /// `reactive: true`). Push-on-subscribe + re-fires on namespace
    /// changes and `set_deps`. β/D242: the handle borrows the root
    /// `&Core`.
    fn describe_reactive(&self, sink: DescribeSink) -> ReactiveDescribeHandle<'_> {
        describe_reactive_in(self.graph_core(), self.graph_inner(), sink)
    }

    /// Tap a single node's downstream message stream.
    fn observe(&self, path: &str) -> GraphObserveOne<'_> {
        let id = self.node(path);
        GraphObserveOne::new(self.as_subgraph_for_observe(), id)
    }

    /// Tap every named node in this graph (snapshot at `subscribe()`).
    fn observe_all(&self) -> GraphObserveAll<'_> {
        GraphObserveAll::new(self.as_subgraph_for_observe())
    }

    /// Tap every named node AND auto-subscribe late-added nodes.
    fn observe_all_reactive(&self) -> GraphObserveAllReactive<'_> {
        GraphObserveAllReactive::new(self.as_subgraph_for_observe())
    }

    /// A `Send + Sync + 'static` **namespace-only** handle to this
    /// graph level (β/D242: namespace resolution is Core-free). Use
    /// this to capture name lookups into a `Sink`/closure that
    /// out-lives the borrowed [`SubgraphRef`] (which is `!Send` because
    /// it carries `&Core`). E.g. a teardown-observing sink that resolves
    /// a node's name mid-cascade.
    fn namespace(&self) -> NamespaceHandle {
        NamespaceHandle {
            inner: self.graph_inner().clone(),
        }
    }

    /// Internal: a borrowed [`SubgraphRef`] for observe handles to hold
    /// (carries `&Core` for owner-side subscribe + RAII unsubscribe).
    #[doc(hidden)]
    fn as_subgraph_for_observe(&self) -> SubgraphRef<'_> {
        self.as_subgraph()
    }

    /// Fire all namespace-change sinks (β/D231: owner-side `&Core`).
    fn fire_namespace_change(&self) {
        fire_ns(self.graph_core(), self.graph_inner());
    }

    // ------------------------- namespace (§3.5) -------------------------

    /// Register an existing `node_id` under `name` in this graph's
    /// namespace.
    fn add(&self, node_id: NodeId, name: impl Into<String>) -> Result<NodeId, NameError> {
        let name = name.into();
        validate_name(&name)?;
        {
            let mut inner = self.graph_inner().lock();
            if inner.destroyed {
                return Err(NameError::Destroyed);
            }
            if inner.names.contains_key(&name) {
                return Err(NameError::Collision(name));
            }
            inner.names.insert(name.clone(), node_id);
            inner.names_inverse.insert(node_id, name);
        }
        self.fire_namespace_change();
        Ok(node_id)
    }

    /// Resolve a path to a `NodeId`. Panics if missing.
    fn node(&self, path: &str) -> NodeId {
        self.try_resolve(path)
            .unwrap_or_else(|| panic!("Graph::node: no node at path `{path}`"))
    }

    /// Non-panicking [`Self::node`].
    fn try_resolve(&self, path: &str) -> Option<NodeId> {
        self.try_resolve_checked(path).ok().flatten()
    }

    /// Path resolution with typed errors (pure-namespace; β/D237 —
    /// never touches Core; walks `Arc<Mutex<GraphInner>>` handles).
    fn try_resolve_checked(&self, path: &str) -> Result<Option<NodeId>, PathError> {
        resolve_checked(self.graph_inner(), path)
    }

    /// Reverse lookup: the local name for a `node_id`.
    fn name_of(&self, node_id: NodeId) -> Option<String> {
        self.graph_inner()
            .lock()
            .names_inverse
            .get(&node_id)
            .cloned()
    }

    /// Number of named nodes in this graph.
    fn node_count(&self) -> usize {
        self.graph_inner().lock().names.len()
    }

    /// Snapshot of local node names in insertion order.
    fn node_names(&self) -> Vec<String> {
        self.graph_inner().lock().names.keys().cloned().collect()
    }

    /// Snapshot of mounted child names in insertion order.
    fn child_names(&self) -> Vec<String> {
        self.graph_inner().lock().children.keys().cloned().collect()
    }

    /// Returns `true` after [`Self::destroy`] has been called.
    fn is_destroyed(&self) -> bool {
        self.graph_inner().lock().destroyed
    }

    // ---------------------- sugar constructors (§3.9) -------------------

    /// Register a state node under `name`.
    fn state(
        &self,
        name: impl Into<String>,
        initial: Option<HandleId>,
    ) -> Result<NodeId, NameError> {
        let id = self
            .graph_core()
            .register_state(initial.unwrap_or(graphrefly_core::NO_HANDLE), false)
            .expect("invariant: register_state has no error variants reachable for caller-controlled inputs");
        self.add(id, name)
    }

    /// Register a static-derived node (fn fires on every dep change).
    fn derived(
        &self,
        name: impl Into<String>,
        deps: &[NodeId],
        fn_id: FnId,
        equals: EqualsMode,
    ) -> Result<NodeId, NameError> {
        let id = self
            .graph_core()
            .register_derived(deps, fn_id, equals, false)
            .expect("invariant: caller has validated dep ids before calling register_derived");
        self.add(id, name)
    }

    /// Register a dynamic-derived node (fn declares read dep indices).
    fn dynamic(
        &self,
        name: impl Into<String>,
        deps: &[NodeId],
        fn_id: FnId,
        equals: EqualsMode,
    ) -> Result<NodeId, NameError> {
        let id = self
            .graph_core()
            .register_dynamic(deps, fn_id, equals, false)
            .expect("invariant: caller has validated dep ids before calling register_dynamic");
        self.add(id, name)
    }

    // -------------------- named-sugar wrappers (§3.2.1) -----------------

    /// Emit a value on a named state node.
    fn set(&self, name: &str, handle: HandleId) {
        let id = self.node(name);
        self.graph_core().emit(id, handle);
    }

    /// Read the cached value of a named node.
    fn get(&self, name: &str) -> HandleId {
        let id = self.node(name);
        self.graph_core().cache_of(id)
    }

    /// Clear the cache of a named node and cascade `[INVALIDATE]`.
    fn invalidate_by_name(&self, name: &str) {
        let id = self.node(name);
        self.graph_core().invalidate(id);
    }

    /// Mark a named node terminal with COMPLETE.
    fn complete_by_name(&self, name: &str) {
        let id = self.node(name);
        self.graph_core().complete(id);
    }

    /// Mark a named node terminal with ERROR.
    fn error_by_name(&self, name: &str, error_handle: HandleId) {
        let id = self.node(name);
        self.graph_core().error(id, error_handle);
    }

    // --------------------------- remove (§3.2.3) ------------------------

    /// Remove a named node OR mounted subgraph (R3.2.3 / R3.7.3
    /// ordering — namespace cleared AFTER the TEARDOWN cascade).
    #[must_use = "remove returns Err on missing name; ignoring may hide bugs"]
    fn remove(&self, name: &str) -> Result<GraphRemoveAudit, RemoveError> {
        {
            let inner = self.graph_inner().lock();
            if inner.destroyed {
                return Err(RemoveError::Destroyed);
            }
            if inner.children.contains_key(name) {
                drop(inner);
                return self.unmount(name).map_err(|e| match e {
                    MountError::Destroyed => RemoveError::Destroyed,
                    _ => RemoveError::NotFound(name.to_owned()),
                });
            }
        }
        let node_id = {
            let inner = self.graph_inner().lock();
            if inner.destroyed {
                return Err(RemoveError::Destroyed);
            }
            *inner
                .names
                .get(name)
                .ok_or_else(|| RemoveError::NotFound(name.to_owned()))?
        };
        self.graph_core().teardown(node_id);
        {
            let mut inner = self.graph_inner().lock();
            inner.names.shift_remove(name);
            inner.names_inverse.shift_remove(&node_id);
        }
        self.fire_namespace_change();
        Ok(GraphRemoveAudit {
            node_count: 1,
            mount_count: 0,
        })
    }

    // --------------------------- edges (§3.3.1) -------------------------

    /// Derive `[from, to]` edge name pairs. `recursive` qualifies names
    /// across the mount tree with `::`.
    fn edges(&self, recursive: bool) -> Vec<(String, String)> {
        let names_map = collect_qualified_names_in(self.graph_inner(), "", recursive);
        edges_in(
            self.graph_core(),
            self.graph_inner(),
            "",
            recursive,
            &names_map,
        )
    }

    // ------------------- lifecycle pass-throughs (§3.7) -----------------

    /// Subscribe a sink. Returns a [`SubscriptionId`]; pass it back to
    /// [`Self::unsubscribe`] to detach. S2b/β/D241: core RAII
    /// `Subscription` is retired (parameterless `Drop` cannot reach a
    /// relocating owned `Core`); unsubscribe is explicit owner-invoked.
    #[must_use = "the SubscriptionId must be kept to later unsubscribe; dropping it leaks the sink"]
    fn subscribe(&self, node_id: NodeId, sink: Sink) -> SubscriptionId {
        self.graph_core().subscribe(node_id, sink)
    }

    /// Detach a sink previously registered via [`Self::subscribe`].
    /// Owner-invoked, synchronous (D225/D241).
    fn unsubscribe(&self, node_id: NodeId, sub_id: SubscriptionId) {
        self.graph_core().unsubscribe(node_id, sub_id);
    }

    /// Detach a Core topology subscription by id (D225/D241).
    fn unsubscribe_topology(&self, id: TopologySubscriptionId) {
        self.graph_core().unsubscribe_topology(id);
    }

    /// Emit a value on a state node.
    fn emit(&self, node_id: NodeId, new_handle: HandleId) {
        self.graph_core().emit(node_id, new_handle);
    }

    /// Read a node's current cache.
    fn cache_of(&self, node_id: NodeId) -> HandleId {
        self.graph_core().cache_of(node_id)
    }

    /// Whether the node's fn has fired at least once.
    fn has_fired_once(&self, node_id: NodeId) -> bool {
        self.graph_core().has_fired_once(node_id)
    }

    /// Mark the node terminal with COMPLETE.
    fn complete(&self, node_id: NodeId) {
        self.graph_core().complete(node_id);
    }

    /// Mark the node terminal with ERROR.
    fn error(&self, node_id: NodeId, error_handle: HandleId) {
        self.graph_core().error(node_id, error_handle);
    }

    /// Tear the node down (R2.6.4).
    fn teardown(&self, node_id: NodeId) {
        self.graph_core().teardown(node_id);
    }

    /// Clear the node's cache and cascade `[INVALIDATE]`.
    fn invalidate(&self, node_id: NodeId) {
        self.graph_core().invalidate(node_id);
    }

    /// Acquire a pause lock.
    fn pause(&self, node_id: NodeId, lock_id: LockId) -> Result<(), PauseError> {
        self.graph_core().pause(node_id, lock_id)
    }

    /// Release a pause lock.
    fn resume(&self, node_id: NodeId, lock_id: LockId) -> Result<Option<ResumeReport>, PauseError> {
        self.graph_core().resume(node_id, lock_id)
    }

    /// Allocate a fresh `LockId`.
    fn alloc_lock_id(&self) -> LockId {
        self.graph_core().alloc_lock_id()
    }

    /// Atomically rewire a node's deps.
    ///
    /// # Hazards
    ///
    /// Re-entrant `set_deps` from inside the firing node's own fn
    /// corrupts Dynamic `tracked` indices (D1 in
    /// `~/src/graphrefly-rs/docs/porting-deferred.md`). Acceptable v1:
    /// most callers are external orchestrators, not the firing node.
    fn set_deps(&self, n: NodeId, new_deps: &[NodeId]) -> Result<(), SetDepsError> {
        self.graph_core().set_deps(n, new_deps)
    }

    /// Mark the node as resubscribable (R2.2.7).
    fn set_resubscribable(&self, node_id: NodeId, resubscribable: bool) {
        self.graph_core()
            .set_resubscribable(node_id, resubscribable);
    }

    /// Attach `companion` as a meta companion of `parent` (R1.3.9.d).
    fn add_meta_companion(&self, parent: NodeId, companion: NodeId) {
        self.graph_core().add_meta_companion(parent, companion);
    }

    /// Coalesce multiple emissions into a single wave.
    fn batch<F: FnOnce()>(&self, f: F) {
        self.graph_core().batch(f);
    }

    // --------------------- graph-level lifecycle (§3.7) -----------------

    /// General broadcast (canonical R3.7.1).
    fn signal(&self, kind: SignalKind) {
        match kind {
            SignalKind::Invalidate => self.signal_invalidate(),
            SignalKind::Pause(lock_id) => {
                for id in collect_signal_ids_with_meta_filter(self.graph_core(), self.graph_inner())
                {
                    let _ = self.graph_core().pause(id, lock_id);
                }
            }
            SignalKind::Resume(lock_id) => {
                for id in collect_signal_ids_with_meta_filter(self.graph_core(), self.graph_inner())
                {
                    let _ = self.graph_core().resume(id, lock_id);
                }
            }
            SignalKind::Complete => {
                for id in collect_signal_ids_with_meta_filter(self.graph_core(), self.graph_inner())
                {
                    self.graph_core().complete(id);
                }
            }
            SignalKind::Error(h) => {
                let ids =
                    collect_signal_ids_with_meta_filter(self.graph_core(), self.graph_inner());
                // F1 /qa fix: each core.error() releases one caller share.
                // Pre-retain (N-1) so the handle survives all N releases.
                for _ in 1..ids.len() {
                    self.graph_core().binding_ptr().retain_handle(h);
                }
                for id in ids {
                    self.graph_core().error(id, h);
                }
            }
        }
    }

    /// Broadcast `[INVALIDATE]` across this graph + mount tree
    /// (meta-companion filtered per R3.7.2; Graph locks dropped before
    /// any `Core::invalidate`). Idempotent on a destroyed graph.
    fn signal_invalidate(&self) {
        let to_invalidate = collect_signal_invalidate_ids(self.graph_core(), self.graph_inner());
        for id in to_invalidate {
            self.graph_core().invalidate(id);
        }
    }

    /// Tear down every named node + recursively into mounted children,
    /// then clear namespace + mount-tree state. R3.7.3 ordering
    /// (children-first, then own teardown, then clear, then fire
    /// ns-change) preserved verbatim.
    fn destroy(&self) {
        destroy_subtree(self.graph_core(), self.graph_inner());
    }

    // --------------------------- mount (§3.4) ---------------------------

    /// Embed an existing `child` subgraph under `name`. `child` must
    /// share this graph's `Core` (cross-Core mount is post-M6).
    fn mount<'a>(
        &'a self,
        name: impl Into<String>,
        child: SubgraphRef<'_>,
    ) -> Result<SubgraphRef<'a>, MountError> {
        crate::mount::mount(self.graph_core(), self.graph_inner(), name.into(), child)
    }

    /// Create an empty subgraph sharing this graph's `Core`.
    fn mount_new<'a>(&'a self, name: impl Into<String>) -> Result<SubgraphRef<'a>, MountError> {
        crate::mount::mount_new(self.graph_core(), self.graph_inner(), name.into())
    }

    /// Builder pattern: create an empty subgraph, run `builder`, return it.
    fn mount_with<'a, F: FnOnce(&SubgraphRef<'_>)>(
        &'a self,
        name: impl Into<String>,
        builder: F,
    ) -> Result<SubgraphRef<'a>, MountError> {
        let child = self.mount_new(name)?;
        builder(&child);
        Ok(child)
    }

    /// Detach a previously-mounted subgraph (TEARDOWN cascade).
    fn unmount(&self, name: &str) -> Result<GraphRemoveAudit, MountError> {
        crate::mount::unmount(self.graph_core(), self.graph_inner(), name)
    }

    /// Parent chain (root last). `include_self = true` prepends this
    /// graph. β/D242: returns borrowed [`SubgraphRef`]s.
    fn ancestors(&self, include_self: bool) -> Vec<SubgraphRef<'_>> {
        crate::mount::ancestors(self.graph_core(), self.graph_inner(), include_self)
    }
}

/// A `Send + Sync + 'static` namespace-only view (no `Core`). Cloning
/// is an `Arc` bump. β/D242: the namespace half of the
/// root-owns-Core/namespace-is-separate split — safe to capture into
/// `Sink` closures that out-live a borrowed [`SubgraphRef`].
#[derive(Clone)]
pub struct NamespaceHandle {
    inner: Arc<Mutex<GraphInner>>,
}

impl NamespaceHandle {
    /// This graph level's name.
    #[must_use]
    pub fn name(&self) -> String {
        self.inner.lock().name.clone()
    }

    /// Local name for `node_id`, if any.
    #[must_use]
    pub fn name_of(&self, node_id: NodeId) -> Option<String> {
        self.inner.lock().names_inverse.get(&node_id).cloned()
    }

    /// Non-panicking path resolution (pure namespace).
    #[must_use]
    pub fn try_resolve(&self, path: &str) -> Option<NodeId> {
        resolve_checked(&self.inner, path).ok().flatten()
    }

    /// Path resolution with typed errors.
    ///
    /// # Errors
    /// See [`PathError`].
    pub fn try_resolve_checked(&self, path: &str) -> Result<Option<NodeId>, PathError> {
        resolve_checked(&self.inner, path)
    }

    /// Local node names in insertion order.
    #[must_use]
    pub fn node_names(&self) -> Vec<String> {
        self.inner.lock().names.keys().cloned().collect()
    }

    /// Mounted child names in insertion order.
    #[must_use]
    pub fn child_names(&self) -> Vec<String> {
        self.inner.lock().children.keys().cloned().collect()
    }

    /// Whether this graph level has been destroyed.
    #[must_use]
    pub fn is_destroyed(&self) -> bool {
        self.inner.lock().destroyed
    }

    /// β/D244: snapshot this (sub)graph given an in-wave `&dyn CoreFull`
    /// (the storage `observe`-sink `MailboxOp::Defer` path — the
    /// `NamespaceHandle` is the `Send` graph identity, `CoreFull` the
    /// owner-side reentry that carries `serialize_handle`).
    #[must_use]
    pub fn snapshot(&self, core: &dyn CoreFull) -> GraphPersistSnapshot {
        snapshot_of(core, &self.inner)
    }
}

// =====================================================================
// β/D237/D242: free fns over (&Core, &Arc<Mutex<GraphInner>>)
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
    inner_arc: &Arc<Mutex<GraphInner>>,
    sink: NamespaceChangeSink,
) -> u64 {
    let mut inner = inner_arc.lock();
    let id = inner.next_ns_sink_id;
    inner.next_ns_sink_id += 1;
    inner.namespace_sinks.insert(id, sink);
    id
}

/// Remove a namespace-change sink by id (inner-only; no `Core`).
pub(crate) fn unregister_ns_sink(inner_arc: &Arc<Mutex<GraphInner>>, id: u64) {
    inner_arc.lock().namespace_sinks.shift_remove(&id);
}

/// Pure-namespace path resolution (R3.5.1/R3.5.2). Never touches `Core`.
pub(crate) fn resolve_checked(
    inner_arc: &Arc<Mutex<GraphInner>>,
    path: &str,
) -> Result<Option<NodeId>, PathError> {
    if path.is_empty() {
        return Err(PathError::Empty);
    }
    let inner = inner_arc.lock();
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
/// `&Core` (β/D231 owner-side). Sinks run with no Graph lock held.
pub(crate) fn fire_ns(core: &Core, inner_arc: &Arc<Mutex<GraphInner>>) {
    let sinks: Vec<NamespaceChangeSink> = {
        let inner = inner_arc.lock();
        inner.namespace_sinks.values().cloned().collect()
    };
    for sink in sinks {
        sink(core);
    }
}

/// `destroy()` over `Arc<Mutex<GraphInner>>` + root `&Core`. Ordering
/// preserved verbatim (R3.7.3).
pub(crate) fn destroy_subtree(core: &Core, inner_arc: &Arc<Mutex<GraphInner>>) {
    let (own_ids, child_clones) = {
        let mut inner = inner_arc.lock();
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
        let mut inner = inner_arc.lock();
        inner.names.clear();
        inner.names_inverse.clear();
        inner.children.clear();
    }
    fire_ns(core, inner_arc);
}

/// Tree-wide gather for `signal_pause`/`resume`/`complete`/`error`
/// (meta-companion filtered per D4). `Arc<Mutex<GraphInner>>` worklist
/// + the single root `&Core`.
fn collect_signal_ids_with_meta_filter(core: &Core, root: &Arc<Mutex<GraphInner>>) -> Vec<NodeId> {
    let mut out: Vec<NodeId> = Vec::new();
    let mut worklist: Vec<Arc<Mutex<GraphInner>>> = vec![root.clone()];
    while let Some(inner_arc) = worklist.pop() {
        let (own_ids, meta_set, child_clones) = {
            let inner = inner_arc.lock();
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
fn collect_signal_invalidate_ids(core: &Core, root: &Arc<Mutex<GraphInner>>) -> Vec<NodeId> {
    let mut out: Vec<NodeId> = Vec::new();
    let mut worklist: Vec<Arc<Mutex<GraphInner>>> = vec![root.clone()];
    while let Some(inner_arc) = worklist.pop() {
        let (own_ids, meta_set, child_clones) = {
            let inner = inner_arc.lock();
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
    inner_arc: &Arc<Mutex<GraphInner>>,
    prefix: &str,
    recursive: bool,
) -> IndexMap<NodeId, String> {
    let inner = inner_arc.lock();
    let mut map: IndexMap<NodeId, String> = inner
        .names
        .iter()
        .map(|(n, id)| (*id, format!("{prefix}{n}")))
        .collect();
    let children: Vec<(String, Arc<Mutex<GraphInner>>)> = if recursive {
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
    inner_arc: &Arc<Mutex<GraphInner>>,
    prefix: &str,
    recursive: bool,
    names_map: &IndexMap<NodeId, String>,
) -> Vec<(String, String)> {
    let inner = inner_arc.lock();
    let qualified: Vec<(String, NodeId)> = inner
        .names
        .iter()
        .map(|(n, id)| (format!("{prefix}{n}"), *id))
        .collect();
    let children: Vec<(String, Arc<Mutex<GraphInner>>)> = if recursive {
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
