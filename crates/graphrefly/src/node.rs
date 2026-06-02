//! The node — the thinnest substrate primitive (D5 / R-node-thin).
//!
//! Holds a fn handle + deps + the wave state machine; **zero inspection cruft**
//! (naming/find/describe are the graph layer). Single-thread (D22) ⇒ the shared
//! state lives in a graph-local arena behind `Rc<RefCell<GraphCore>>` ([`Core`] =
//! graph handle + node id), `!Send + !Sync`.
//!
//! Values cross the substrate **erased** as [`AnyValue`] (`Rc<dyn Any>`) — the
//! Rust analogue of TS's `unknown` (per-language impl, `CLEAN-SLATE.md`). A typed
//! [`Node<T>`] facade re-types the boundary (downcast at `cache`/`data`).
//!
//! ## Borrow discipline (the Rust-vs-TS divergence)
//!
//! TS has no runtime borrow check; here the wave engine must never hold a
//! `RefCell` borrow across a call that re-enters a node — `ctx.down` re-enters
//! the running node, and delivery re-enters downstream nodes. The rule throughout
//! is **clone the callable out, drop the borrow, then call** (subscribers,
//! dispatcher fns, dep handles). Short `borrow_mut` scopes mutate; calls happen
//! borrow-free.
//!
//! ## Slice scope
//!
//! **CSP-5 first cut:** state / producer / derived nodes · two-phase DIRTY→DATA ·
//! diamond pending-counter join (R-diamond/R-two-phase) · first-run gate
//! (R-first-run-gate) · every occurrence is DATA + substrate-synthesized undirty
//! RESOLVED for a no-emit fn (R-resolved-undirty / D49, supersedes D15 — no
//! equals-substitution) · push-on-subscribe (R-push-subscribe) · lazy activation ·
//! ROM/RAM (R-rom-ram).
//!
//! **Control/terminal slice (this cut):** INVALIDATE + `ctx.on_invalidate`
//! (R-invalidate-idempotent / R-cleanup-hooks, C-3) · PAUSE/RESUME lockset +
//! default-mode coalesce (R-pause-lockset / R-pause-modes, C-5) · COMPLETE/ERROR
//! terminal (terminal-is-forever, D17) · the **D30 catch boundary** = a
//! `catch_unwind` at the wave-owner that converts a panic (the Rust analogue of a
//! value-level `throw`) into `[[ERROR,e]]` on a node ON the cycle (R-reentrancy /
//! D37, C-6), with whole-cascade wave-flag recovery (the touched-set reset — closes
//! **B25**). **Deferred:** TEARDOWN, batch, LocalAsync, rewire, dynamicNode,
//! replay-buffer, the resumeAll/false pause modes + async-paused buffering
//! (C-2/C-9/C-10), up-at-source INVALIDATE terminus (C-7).

use std::cell::{Cell, Ref, RefCell, RefMut};
use std::collections::{HashSet, VecDeque};
use std::marker::PhantomData;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::{Rc, Weak};

use crate::batch::{
    boundary_drains_blocked, collecting_batch, defer_after_batch_for_target, defer_to_batch,
};
use crate::ctx::{Ctx, DepRecord, DepTerminal, WaveData};
use crate::dispatcher::{default_dispatcher, Dispatcher, NodeFn, PoolKind};
use crate::protocol::{AnyValue, GraphError, Handle, LockId, Message, Tier, Wave};

/// Node lifecycle status (R-status-enum) — the source of truth for cache freshness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Status {
    /// No DATA yet — cache absent (SENTINEL = `None`, D16). Also the
    /// awaiting-first-run-gate state in this slice.
    Sentinel,
    /// Reserved by R-status-enum; not assigned in this slice (the awaiting-gate
    /// state is represented as `Sentinel`, matching the TS reference).
    Pending,
    /// Phase-1 DIRTY received — the prior cached value is stale.
    Dirty,
    /// Settled with a fresh value this wave.
    Settled,
    /// Undirty settle (R-resolved-undirty / D49): DIRTY'd but produced no new
    /// occurrence; a carried-over cached value stays fresh.
    Resolved,
    /// Terminal success — final (deferred slice).
    Completed,
    /// Terminal failure — final (deferred slice).
    Errored,
}

impl Status {
    /// Cached value is fresh (`settled` / `resolved`).
    pub fn is_fresh(self) -> bool {
        matches!(self, Status::Settled | Status::Resolved)
    }
    /// Terminal status (`completed` / `errored`) — D17 terminal-is-forever.
    pub fn is_terminal(self) -> bool {
        matches!(self, Status::Completed | Status::Errored)
    }
}

/// PAUSE/RESUME backpressure mode (R-pause-modes). The OUTER gate over async-paused
/// buffering (D44).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Pausable {
    /// Default. Skip dep-driven fn recompute while paused, fire once on final-lock
    /// RESUME with the latest dep values. A leaf source's (depless) OWN production —
    /// sync or async — is NOT gated (delivered immediately); an async COMPUTE node's
    /// (deps>0) in-flight result buffers (R-async-paused / C-2).
    #[default]
    True,
    /// Production-gating: buffer the node's own tier-3/4 settle slice (sync OR async)
    /// while paused and replay it on final-lock RESUME. B9 pre-pause baseline-equals
    /// fidelity is deferred (same partial level as the TS arm).
    ResumeAll,
    /// Ignore PAUSE/RESUME ENTIRELY (timer/interval-class sources that must keep
    /// producing): the lockset is never consulted, nothing is gated or buffered (D44,
    /// resolves B20).
    False,
}

/// Node construction options (substrate-level). Sugar/operator opts are the graph layer
/// (D6/D24); this is the minimal substrate surface needed to select a pool (D20), a
/// pause mode (R-pause-modes), and the dep-terminal propagation policy (R-deps-terminal).
#[derive(Debug, Clone)]
pub struct NodeOpts {
    /// Real factory name for graph-less nodes that `describe` auto-discovers (D43/D51).
    /// Registered graph entries still carry their own graph-layer factory string.
    pub factory: Option<String>,
    /// The dispatch pool (default LocalSync, R-sync-core).
    pub pool: PoolKind,
    /// The pause mode (default `true`, R-pause-modes).
    pub pausable: Pausable,
    /// First-run gate override: allow fn execution with SENTINEL deps (graph-layer
    /// partial combinators / pull consumers). Default false.
    pub partial: bool,
    /// Pull-mode node id (R-pull / D59). When present, the node starts quiet by
    /// self-holding this lock; a routed RESUME of the same id demands one delivery.
    pub pull_id: Option<LockId>,
    /// Auto-emit COMPLETE when ALL deps complete (default `true`; combineLatest
    /// semantics — ALL not ANY). last/reduce/*Map set `false` to ABSORB inner
    /// terminals (R-deps-terminal).
    pub complete_when_deps_complete: bool,
    /// Auto-emit ERROR when ANY dep errors (default `true`). rescue/catch set `false`.
    pub error_when_deps_error: bool,
    /// A dep's terminal is a REAL INPUT the fn reads (rescue/reduce/*Map) rather than an
    /// absorbed settle — when set, the fn re-runs on a dep terminal and the first-run
    /// gate treats a terminated dep as settled (R-deps-terminal). Default `false`.
    pub terminal_as_real_input: bool,
}

/// Defaults: COMPLETE/ERROR auto-cascade ON (combineLatest-style), terminal-as-input OFF
/// — the plain derived/effect behavior. (A manual impl, not `#[derive(Default)]`, so the
/// two cascade flags default to `true`, not `false`.)
impl Default for NodeOpts {
    fn default() -> Self {
        Self {
            factory: None,
            pool: PoolKind::default(),
            pausable: Pausable::default(),
            partial: false,
            pull_id: None,
            complete_when_deps_complete: true,
            error_when_deps_error: true,
            terminal_as_real_input: false,
        }
    }
}

type Msg = Message<AnyValue>;
type Sink = Rc<dyn Fn(&Msg)>;
type Unsub = Box<dyn FnOnce()>;

/// A deferred self-rewire request (R-rewire-deferred / D47), issued via `ctx.rewire_next`
/// and applied at the committed wave boundary. The new dep set is computed at APPLY time (so
/// multiple requests in one wave compose against the live deps), then the surgical R-rewire
/// (D42) runs as a fresh wave. The fn re-pairs the deps (SD-1 fn-deps pairing).
pub(crate) enum RewireRequest {
    /// Add a dep (idempotent if already present) + swap the fn.
    Add(Core, NodeFn),
    /// Remove a dep (idempotent if absent) + swap the fn.
    Remove(Core, NodeFn),
    /// Replace the whole dep set + swap the fn.
    Set(Vec<Core>, NodeFn),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct NodeId(usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct NodeKey {
    id: NodeId,
    generation: u64,
}

/// B49 / D71 first migration slice: graph-local arena for node bookkeeping. The
/// public [`Core`] is now a node id plus a shared graph arena, rather than a direct
/// `Rc<RefCell<NodeInner>>`. This keeps the public API stable while moving ownership
/// toward a GraphCore/PropagationEngine shape.
pub(crate) struct GraphArena(Rc<RefCell<GraphCore>>);

impl GraphArena {
    pub(crate) fn new() -> Self {
        Self(Rc::new(RefCell::new(GraphCore::new())))
    }

    pub(crate) fn default() -> Self {
        DEFAULT_GRAPH_CORE.with(|graph| Self(graph.clone()))
    }
}

struct GraphCore {
    slots: Vec<Option<NodeInner>>,
    edge_slots: Vec<Option<DepEdges>>,
    aux_slots: Vec<Option<NodeAux>>,
    generations: Vec<u64>,
    free: Vec<usize>,
    deferred_boundary: VecDeque<Box<dyn FnOnce()>>,
    draining_boundary: bool,
}

impl GraphCore {
    fn new() -> Self {
        Self {
            slots: Vec::new(),
            edge_slots: Vec::new(),
            aux_slots: Vec::new(),
            generations: Vec::new(),
            free: Vec::new(),
            deferred_boundary: VecDeque::new(),
            draining_boundary: false,
        }
    }

    fn alloc(&mut self, inner: NodeInner) -> NodeId {
        let edges = DepEdges::new(inner.deps.len());
        let aux = NodeAux::new();
        if let Some(id) = self.free.last().copied() {
            let next_generation = self.generations[id]
                .checked_add(1)
                .expect("GraphCore generation overflow on slot reuse");
            self.free.pop();
            self.slots[id] = Some(inner);
            self.edge_slots[id] = Some(edges);
            self.aux_slots[id] = Some(aux);
            self.generations[id] = next_generation;
            NodeId(id)
        } else {
            let id = self.slots.len();
            self.slots.push(Some(inner));
            self.edge_slots.push(Some(edges));
            self.aux_slots.push(Some(aux));
            self.generations.push(0);
            NodeId(id)
        }
    }

    fn key_for(&self, id: NodeId) -> NodeKey {
        NodeKey {
            id,
            generation: self.generations[id.0],
        }
    }

    fn get(&self, key: NodeKey) -> &NodeInner {
        assert!(
            self.is_live_key(key),
            "Core points at a stale or freed GraphCore slot"
        );
        self.slots
            .get(key.id.0)
            .and_then(Option::as_ref)
            .expect("Core points at a live GraphCore slot")
    }

    fn get_mut(&mut self, key: NodeKey) -> &mut NodeInner {
        assert!(
            self.is_live_key(key),
            "Core points at a stale or freed GraphCore slot"
        );
        self.slots
            .get_mut(key.id.0)
            .and_then(Option::as_mut)
            .expect("Core points at a live GraphCore slot")
    }

    fn get_node_and_edges_mut(&mut self, key: NodeKey) -> (&mut NodeInner, &mut DepEdges) {
        assert!(
            self.is_live_key(key),
            "Core points at a stale or freed GraphCore slot"
        );
        let n = self
            .slots
            .get_mut(key.id.0)
            .and_then(Option::as_mut)
            .expect("Core points at a live GraphCore slot");
        let e = self
            .edge_slots
            .get_mut(key.id.0)
            .and_then(Option::as_mut)
            .expect("Core points at a live GraphCore edge slot");
        (n, e)
    }

    fn get_node_edges_aux_mut(
        &mut self,
        key: NodeKey,
    ) -> (&mut NodeInner, &mut DepEdges, &mut NodeAux) {
        assert!(
            self.is_live_key(key),
            "Core points at a stale or freed GraphCore slot"
        );
        let slots = &mut self.slots;
        let edge_slots = &mut self.edge_slots;
        let aux_slots = &mut self.aux_slots;
        let n = slots
            .get_mut(key.id.0)
            .and_then(Option::as_mut)
            .expect("Core points at a live GraphCore slot");
        let e = edge_slots
            .get_mut(key.id.0)
            .and_then(Option::as_mut)
            .expect("Core points at a live GraphCore edge slot");
        let a = aux_slots
            .get_mut(key.id.0)
            .and_then(Option::as_mut)
            .expect("Core points at a live GraphCore aux slot");
        (n, e, a)
    }

    fn get_aux(&self, key: NodeKey) -> &NodeAux {
        assert!(
            self.is_live_key(key),
            "Core points at a stale or freed GraphCore slot"
        );
        self.aux_slots
            .get(key.id.0)
            .and_then(Option::as_ref)
            .expect("Core points at a live GraphCore aux slot")
    }

    fn is_live(&self, id: NodeId) -> bool {
        self.slots.get(id.0).is_some_and(Option::is_some)
            && self.edge_slots.get(id.0).is_some_and(Option::is_some)
            && self.aux_slots.get(id.0).is_some_and(Option::is_some)
    }

    fn is_live_key(&self, key: NodeKey) -> bool {
        self.generations.get(key.id.0) == Some(&key.generation) && self.is_live(key.id)
    }

    fn take_live(&mut self, key: NodeKey) -> Option<(NodeInner, DepEdges, NodeAux)> {
        if !self.is_live_key(key) {
            return None;
        }
        let inner = self.slots.get_mut(key.id.0)?.take()?;
        let edges = self.edge_slots.get_mut(key.id.0)?.take()?;
        let aux = self.aux_slots.get_mut(key.id.0)?.take()?;
        Some((inner, edges, aux))
    }
}

thread_local! {
    /// Until the Rust graph layer lands, public constructors allocate into the default
    /// graph-local arena, mirroring the existing default dispatcher shape (D26). A later
    /// graph-owned constructor can pass an explicit arena without changing [`Core`].
    static DEFAULT_GRAPH_CORE: Rc<RefCell<GraphCore>> = Rc::new(RefCell::new(GraphCore::new()));
}

/// Per-dependency wave bookkeeping for one node slot.
struct DepState {
    batch: Vec<Option<Vec<AnyValue>>>,
    batch_waves: Vec<Vec<Vec<WaveData>>>,
    batch_wave_id: Vec<Option<u64>>,
    prev: Vec<Option<AnyValue>>,
    has_data: Vec<bool>,
    dirty: Vec<bool>,
    tier: Vec<u8>,
    pending: i32,
    terminal: Vec<Option<DepTerminal>>,
    terminal_wave: Vec<Option<DepTerminal>>,
}

impl DepState {
    fn new(dep_count: usize) -> Self {
        Self {
            batch: vec![None; dep_count],
            batch_waves: vec![Vec::new(); dep_count],
            batch_wave_id: vec![None; dep_count],
            prev: vec![None; dep_count],
            has_data: vec![false; dep_count],
            dirty: vec![false; dep_count],
            tier: vec![0; dep_count],
            pending: 0,
            terminal: vec![None; dep_count],
            terminal_wave: vec![None; dep_count],
        }
    }

    fn all_settled(&self, terminal_as_real_input: bool) -> bool {
        for i in 0..self.has_data.len() {
            if self.has_data[i] {
                continue;
            }
            if terminal_as_real_input && self.terminal[i].is_some() {
                continue;
            }
            return false;
        }
        true
    }

    fn current_wave_mut(&mut self, idx: usize) -> &mut Vec<WaveData> {
        let delivery_id = current_delivery_id();
        if self.batch_wave_id[idx] != Some(delivery_id) {
            self.batch_waves[idx].push(Vec::new());
            self.batch_wave_id[idx] = Some(delivery_id);
        }
        self.batch_waves[idx]
            .last_mut()
            .expect("current_wave_mut just pushed an entry")
    }

    fn record_wave_data(&mut self, idx: usize, value: AnyValue) {
        self.current_wave_mut(idx).push(WaveData::Data(value));
    }

    fn record_wave_sentinel(&mut self, idx: usize) -> bool {
        let had_current_projection = self.batch_wave_id[idx] == Some(current_delivery_id());
        self.current_wave_mut(idx).push(WaveData::Sentinel);
        had_current_projection
    }

    fn has_run_triggering_projection(&self) -> bool {
        self.batch_waves.iter().any(|dep_waves| {
            dep_waves.iter().any(|wave| {
                wave.is_empty() || wave.iter().any(|item| matches!(item, WaveData::Data(_)))
            })
        })
    }

    fn clear_wave_projection(&mut self, idx: usize) {
        self.batch_waves[idx].clear();
        self.batch_wave_id[idx] = None;
    }

    fn record_resolved_wave(&mut self, idx: usize) {
        let _ = self.current_wave_mut(idx);
    }
}

/// Per-node transient wave/mutation flags.
struct NodeWaveState {
    emitted_dirty_this_wave: bool,
    emitted_tier3_this_wave: bool,
    inside_run_wave: bool,
    in_dep_mutation: bool,
    rewire_run_pending: bool,
    batch_dirty_owed: bool,
}

impl NodeWaveState {
    fn new() -> Self {
        Self {
            emitted_dirty_this_wave: false,
            emitted_tier3_this_wave: false,
            inside_run_wave: false,
            in_dep_mutation: false,
            rewire_run_pending: false,
            batch_dirty_owed: false,
        }
    }
}

struct NodeValueState {
    cache: Option<AnyValue>,
    has_data: bool,
    status: Status,
    terminal: bool,
}

impl NodeValueState {
    fn new() -> Self {
        Self {
            cache: None,
            has_data: false,
            status: Status::Sentinel,
            terminal: false,
        }
    }
}

/// Per-dependency edge bookkeeping (subscription ownership + callback index indirection).
/// Kept in GraphCore side-tables as part of the B49 migration path.
struct DepEdges {
    value: NodeValueState,
    wave: NodeWaveState,
    state: DepState,
    unsubs: Vec<Option<Unsub>>,
    idx_boxes: Vec<Rc<Cell<i64>>>,
}

impl DepEdges {
    fn new(dep_count: usize) -> Self {
        Self {
            value: NodeValueState::new(),
            wave: NodeWaveState::new(),
            state: DepState::new(dep_count),
            unsubs: (0..dep_count).map(|_| None).collect(),
            idx_boxes: (0..dep_count).map(|_| Rc::new(Cell::new(-1i64))).collect(),
        }
    }
}

/// Node-local sidecars that are NOT part of dep-slot wave bookkeeping. Keeping these
/// in a side-table continues the B49 thinning path without changing observable behavior.
struct NodeAux {
    subscribers: Vec<(u64, Sink)>,
    next_sub_id: u64,
    activated: bool,
    state: Option<AnyValue>,
    state_persist: bool,
    on_deactivation: Vec<Box<dyn FnOnce()>>,
    on_invalidate: Vec<Rc<dyn Fn()>>,
    /// A demand arrived but cannot yet fire because a dep is still pending or an
    /// external pause lock is held. Drained when the node is settle-ready.
    pull_demand_owed: bool,
    /// PAUSE lockset (R-pause-lockset). Paused iff non-empty.
    pause_lockset: HashSet<LockId>,
    /// A dep wave was skipped while paused; default true mode fires once on resume.
    paused_dep_wave_occurred: bool,
    /// Buffered tier-3/4 settle slices held while paused.
    pause_buffer: Vec<Wave<AnyValue>>,
}

impl NodeAux {
    fn new() -> Self {
        Self {
            subscribers: Vec::new(),
            next_sub_id: 0,
            activated: false,
            state: None,
            state_persist: false,
            on_deactivation: Vec::new(),
            on_invalidate: Vec::new(),
            pull_demand_owed: false,
            pause_lockset: HashSet::new(),
            paused_dep_wave_occurred: false,
            pause_buffer: Vec::new(),
        }
    }
}

/// The mutable state of a node. Pure fields + non-reentrant helpers only; the
/// reentrant engine lives on [`Core`].
struct NodeInner {
    deps: Vec<Core>,
    factory: Option<String>,
    handle: Option<Handle>,
    dispatcher: Dispatcher,
    /// First-run gate off (fn fires as soon as dirty-count hits 0; fn body guards
    /// SENTINEL per dep). Default false (R-first-run-gate).
    partial: bool,
    has_called_fn_once: bool,

    // control + terminal
    /// Pause mode (R-pause-modes / D44). `true` coalesces dep-driven recompute; `false`
    /// ignores PAUSE entirely; `resumeAll` buffers+replays the own settle slice.
    pausable: Pausable,
    /// Pull-mode id (R-pull / D59). `Some(id)` means quiet-by-default; demand is a
    /// cone-routed RESUME(id), matched by id equality in Rust's per-language representation.
    pull_id: Option<LockId>,

    /// Auto-emit COMPLETE when ALL deps complete (default true). last/reduce/*Map set
    /// false to ABSORB inner terminals (R-deps-terminal).
    complete_when_deps_complete: bool,
    /// Auto-emit ERROR when ANY dep errors (default true). rescue/catch set false.
    error_when_deps_error: bool,
    /// A dep's terminal is a real input the fn reads (rescue/reduce/*Map), not an
    /// absorbed settle (R-deps-terminal).
    terminal_as_real_input: bool,
}

impl NodeInner {
    /// First-run gate predicate: every declared dep has delivered ≥1 DATA. A
    /// `terminal_as_real_input` node (rescue/reduce/*Map) also treats a TERMINATED dep
    /// as settled, so the fn may run when an inner terminates without ever delivering
    /// DATA (R-deps-terminal / R-first-run-gate).
    fn all_deps_settled(&self, dep: &DepState) -> bool {
        dep.all_settled(self.terminal_as_real_input)
    }
}

/// Release a node's upstream subscriptions + external resources when its inner is
/// finally dropped (D1).
///
/// Rust has no GC, so a `Node<T>` dropped without an explicit unsubscribe (and
/// with no subscriber-driven deactivation) would otherwise leave its sink in each
/// dep's `subscribers` list forever — the dep would never deactivate and would do
/// wasted work emitting to a dead `Weak`. Running the remaining `dep_unsubs` here
/// removes those entries, and firing `on_deactivation` releases external resources
/// (mirroring what GC + finalizers do in TS). Safe: at drop the refcount is 0 (no
/// live borrow on `self`); each unsub touches a *dep* node (a different `RefCell`),
/// recursively deactivating up the DAG (terminates); idempotent with `deactivate`
/// (which already drained both vecs, so this is then a no-op).
impl NodeInner {
    fn cleanup_before_free(&mut self, edges: &mut DepEdges, aux: &mut NodeAux) {
        for u in edges.unsubs.drain(..).flatten() {
            u();
        }
        for h in std::mem::take(&mut aux.on_deactivation) {
            h();
        }
        // B32: free this node's fn slot so the dispatcher pool no longer holds the fn —
        // and thus the upstream `Core`s the fn captured (e.g. a `Node: Clone` handle, the
        // idiomatic feedback pattern) — alive for the whole process. Safe: invoke clones
        // the fn out before calling (the pool is unborrowed during the fn run), so a node
        // dropped from inside a fn can re-borrow the pool here. A fn that captures its OWN
        // node is a genuine Rc cycle this cannot break (the pool keeps that node alive →
        // Drop never runs) — that is a user-level self-cycle (use a Weak self-handle).
        if let Some(handle) = self.handle {
            self.dispatcher.unregister(handle);
        }
    }
}

/// A cloneable handle over a node slot in a shared graph arena. The reentrant
/// wave-engine methods live here; each manages its own short borrow scopes so calls
/// into other nodes (or back into this one via `ctx.down`) are never made while a
/// borrow is held.
pub struct Core {
    graph: Rc<RefCell<GraphCore>>,
    id: NodeId,
    generation: u64,
    /// Kept outside the arena so `Core::clone` can run while `GraphCore` is borrowed.
    refs: Rc<Cell<usize>>,
    /// Public/user handles are counted. Internal arena callbacks may construct an
    /// uncounted view for one synchronous dispatch to avoid refcount churn.
    counted: bool,
}

impl Core {
    fn from_parts(
        graph: Rc<RefCell<GraphCore>>,
        id: NodeId,
        generation: u64,
        refs: Rc<Cell<usize>>,
    ) -> Self {
        refs.set(refs.get().checked_add(1).expect("Core refcount overflow"));
        Self {
            graph,
            id,
            generation,
            refs,
            counted: true,
        }
    }

    fn from_borrowed_parts(
        graph: Rc<RefCell<GraphCore>>,
        id: NodeId,
        generation: u64,
        refs: Rc<Cell<usize>>,
    ) -> Self {
        Self {
            graph,
            id,
            generation,
            refs,
            counted: false,
        }
    }

    fn key(&self) -> NodeKey {
        NodeKey {
            id: self.id,
            generation: self.generation,
        }
    }

    fn borrow(&self) -> Ref<'_, NodeInner> {
        Ref::map(self.graph.borrow(), |g| g.get(self.key()))
    }

    fn borrow_mut(&self) -> RefMut<'_, NodeInner> {
        RefMut::map(self.graph.borrow_mut(), |g| g.get_mut(self.key()))
    }

    fn with_inner_edges_mut<R>(&self, f: impl FnOnce(&mut NodeInner, &mut DepEdges) -> R) -> R {
        let mut g = self.graph.borrow_mut();
        let (n, e) = g.get_node_and_edges_mut(self.key());
        f(n, e)
    }

    fn with_inner_edges_aux_mut<R>(
        &self,
        f: impl FnOnce(&mut NodeInner, &mut DepEdges, &mut NodeAux) -> R,
    ) -> R {
        let mut g = self.graph.borrow_mut();
        let (n, e, a) = g.get_node_edges_aux_mut(self.key());
        f(n, e, a)
    }

    fn with_inner_edges<R>(&self, f: impl FnOnce(&NodeInner, &DepEdges) -> R) -> R {
        let g = self.graph.borrow();
        let n = g.get(self.key());
        let e = g
            .edge_slots
            .get(self.id.0)
            .and_then(Option::as_ref)
            .expect("Core points at a live GraphCore edge slot");
        f(n, e)
    }

    fn with_aux<R>(&self, f: impl FnOnce(&NodeAux) -> R) -> R {
        let g = self.graph.borrow();
        f(g.get_aux(self.key()))
    }

    fn with_aux_mut<R>(&self, f: impl FnOnce(&mut NodeAux) -> R) -> R {
        let mut g = self.graph.borrow_mut();
        assert!(
            g.is_live_key(self.key()),
            "Core points at a stale or freed GraphCore slot"
        );
        let a = g
            .aux_slots
            .get_mut(self.id.0)
            .and_then(Option::as_mut)
            .expect("Core points at a live GraphCore aux slot");
        f(a)
    }

    fn try_with_inner_aux_mut<R>(
        &self,
        f: impl FnOnce(&mut NodeInner, &mut DepEdges, &mut NodeAux) -> R,
    ) -> Option<R> {
        let mut g = self.graph.try_borrow_mut().ok()?;
        let (n, e, a) = g.get_node_edges_aux_mut(self.key());
        Some(f(n, e, a))
    }

    fn downgrade(&self) -> CoreWeak {
        CoreWeak {
            graph: Rc::downgrade(&self.graph),
            key: self.key(),
            refs: Rc::downgrade(&self.refs),
        }
    }
}

impl Clone for Core {
    fn clone(&self) -> Self {
        Self::from_parts(
            self.graph.clone(),
            self.id,
            self.generation,
            self.refs.clone(),
        )
    }
}

impl Drop for Core {
    fn drop(&mut self) {
        if !self.counted {
            return;
        }
        let refs = self.refs.get();
        if refs > 1 {
            self.refs.set(refs - 1);
            return;
        }
        if refs == 0 {
            return;
        }
        self.refs.set(0);
        let graph_ref = self.graph.clone();
        let (mut inner, mut edges, mut aux) = {
            let mut graph = graph_ref.borrow_mut();
            let Some(pair) = graph.take_live(self.key()) else {
                return;
            };
            pair
        };
        struct FreeSlotOnDrop {
            graph: Rc<RefCell<GraphCore>>,
            id: usize,
            armed: bool,
        }
        impl Drop for FreeSlotOnDrop {
            fn drop(&mut self) {
                if self.armed {
                    self.graph.borrow_mut().free.push(self.id);
                }
            }
        }
        let mut free_slot = FreeSlotOnDrop {
            graph: graph_ref.clone(),
            id: self.id.0,
            armed: true,
        };
        inner.cleanup_before_free(&mut edges, &mut aux);
        free_slot.armed = false;
        graph_ref.borrow_mut().free.push(self.id.0);
    }
}

struct CoreWeak {
    graph: Weak<RefCell<GraphCore>>,
    key: NodeKey,
    refs: Weak<Cell<usize>>,
}

impl CoreWeak {
    fn counted_core(&self) -> Option<Core> {
        let graph = self.graph.upgrade()?;
        let refs = self.refs.upgrade()?;
        if refs.get() == 0 {
            return None;
        }
        if !graph.borrow().is_live_key(self.key) {
            return None;
        }
        Some(Core::from_parts(
            graph,
            self.key.id,
            self.key.generation,
            refs,
        ))
    }

    fn borrowed_core(&self) -> Option<Core> {
        let graph = self.graph.upgrade()?;
        let refs = self.refs.upgrade()?;
        if refs.get() == 0 {
            return None;
        }
        if !graph.borrow().is_live_key(self.key) {
            return None;
        }
        Some(Core::from_borrowed_parts(
            graph,
            self.key.id,
            self.key.generation,
            refs,
        ))
    }

    fn receive_from_dep(&self, idx: usize, msg: &Msg) {
        let core = if wave_in_flight() {
            self.borrowed_core()
        } else {
            self.counted_core()
        };
        if let Some(core) = core {
            core.receive_from_dep(idx, msg);
        }
    }
}

/// Reset `inside_run_wave` on scope exit — including unwind, so the feedback-cycle
/// panic (D37) leaves the flag clean per frame (mirrors TS's try/finally around
/// `dispatcher.invoke`). The *other* stale wave-flags an unwinding panic leaves on
/// the source / intermediate frames (`emitted_dirty` / `dep_batch` / `dep_dirty` /
/// `pending`) are cleaned at the wave-owner catch via the [`WaveScope`] touched-set
/// (closes B25) — this guard only owns `inside_run_wave`.
struct WaveGuard(Core);
impl Drop for WaveGuard {
    fn drop(&mut self) {
        self.0.with_inner_edges_mut(|_, e| {
            e.wave.inside_run_wave = false;
        });
    }
}

/// Clear `in_dep_mutation` on scope exit — including a panic unwind during a rewire
/// (QA-F1). A user fn can panic mid-rewire (an added dep's activation fn, or a removed
/// dep's onDeactivation hook); the wave-owner catch's [`Core::reset_wave_flags`] does NOT
/// cover `in_dep_mutation` (and may not include the rewiring node in its touched-set), so
/// without this RAII a caught rewire panic would wedge the node forever (every future
/// recompute deferred + every future rewire rejected as reentrant). Mirrors the TS arm's
/// `try { … } finally { _inDepMutation = false }`. `rewire_run_pending` needs no unwind
/// reset — it is reread/cleared at the start of the next `rewire` and has no other consumer.
struct DepMutationGuard(Core);
impl Drop for DepMutationGuard {
    fn drop(&mut self) {
        self.0.with_inner_edges_mut(|_, e| {
            e.wave.in_dep_mutation = false;
        });
    }
}

/// The D30 catch boundary + B25 whole-cascade recovery (per-language impl, D24 —
/// the Rust analogue of TS's graph-layer value-fn `try/catch`; Rust has no graph
/// layer yet, so the catch lives at the substrate wave-owner).
///
/// A `panic!` is the Rust analogue of a value-level `throw` (the fn signature is
/// `Fn(&Ctx) -> ()` — no `Result` channel; R-reentrancy mandates an *unwind*, not a
/// graceful return). The **outermost** public mutating entry (`subscribe` / `down` /
/// `up` / `set`) becomes the wave OWNER: it installs a scope, runs the cascade under
/// `catch_unwind`, and on a caught panic resets every participating node's transient
/// wave-flags ([`Core::reset_wave_flags`]) before emitting `[[ERROR,e]]` from the
/// `blamed` node. Nested re-entrant calls see the active scope and just run.
///
/// `blamed` is the innermost node whose fn actually ran ([`Core::run_wave`] records
/// it right before `invoke`), so a sync feedback cycle blames a node ON the cycle
/// (C-6) — the value-level catch "nearest the throw", impl-determined per
/// R-reentrancy. Falls back to the wave `owner` if no fn ran.
///
/// Forward-looking: this wave-owner boundary is the same "committed wave boundary"
/// that batch (D12) and the `ctx.rewire_next` deferred drain (D47) will reuse.
struct WaveScope {
    owner: Core,
    /// Every node that mutated a transient wave-flag this wave (may contain dups —
    /// [`Core::reset_wave_flags`] is idempotent; the set lives for one wave only).
    touched: Vec<Core>,
    /// Innermost node whose fn ran before the panic (the ERROR-bearing node).
    blamed: Option<Core>,
}

thread_local! {
    /// The current wave's scope, if a wave is in flight (single-thread, D22 ⇒
    /// thread-local, like the default dispatcher D26). `Some` ⇔ inside a wave.
    static WAVE: RefCell<Option<WaveScope>> = const { RefCell::new(None) };
    /// Downstream delivery depth for one `down(msgs)` wave. A dep may receive
    /// multiple DATA messages before its dirty contribution is cleared; the fn
    /// must see the full batch once, not run once per DATA occurrence.
    static DELIVERY_DEPTH: Cell<usize> = const { Cell::new(0) };
    static NEXT_DELIVERY_ID: Cell<u64> = const { Cell::new(1) };
    static CURRENT_DELIVERY_ID: Cell<u64> = const { Cell::new(0) };
    static DEFERRED_RUNS: RefCell<Vec<Core>> = const { RefCell::new(Vec::new()) };
    static DEFERRED_ABSORBED_SETTLES: RefCell<Vec<Core>> = const { RefCell::new(Vec::new()) };
}

/// `true` while a wave is in flight on this thread (a scope is installed).
fn wave_in_flight() -> bool {
    WAVE.with(|w| w.borrow().is_some())
}

/// Register a node as a wave participant (for the B25 touched-set reset). No-op if
/// no wave is in flight (e.g. a `cache` read outside any wave).
fn wave_register(core: &Core) {
    WAVE.with(|w| {
        if let Some(scope) = w.borrow_mut().as_mut() {
            scope.touched.push(core.clone());
        }
    });
}

fn delivery_in_flight() -> bool {
    DELIVERY_DEPTH.with(|depth| depth.get() > 0)
}

fn current_delivery_id() -> u64 {
    CURRENT_DELIVERY_ID.with(Cell::get)
}

fn defer_run_until_delivery_boundary(core: &Core) {
    DEFERRED_RUNS.with(|runs| {
        let mut runs = runs.borrow_mut();
        if !runs.iter().any(|n| n.ptr_eq(core)) {
            runs.push(core.clone());
        }
    });
}

fn defer_absorbed_settle_until_delivery_boundary(core: &Core) {
    DEFERRED_ABSORBED_SETTLES.with(|settles| {
        let mut settles = settles.borrow_mut();
        if !settles.iter().any(|n| n.ptr_eq(core)) {
            settles.push(core.clone());
        }
    });
}

struct DeliveryGuard {
    active: bool,
    prev_id: u64,
}

impl DeliveryGuard {
    fn enter() -> Self {
        let prev_id = CURRENT_DELIVERY_ID.with(Cell::get);
        let id = NEXT_DELIVERY_ID.with(|next| {
            let id = next.get();
            next.set(id.wrapping_add(1).max(1));
            id
        });
        CURRENT_DELIVERY_ID.with(|current| current.set(id));
        DELIVERY_DEPTH.with(|depth| depth.set(depth.get() + 1));
        Self {
            active: true,
            prev_id,
        }
    }

    fn finish(mut self) {
        self.active = false;
        CURRENT_DELIVERY_ID.with(|current| current.set(self.prev_id));
        let outer = DELIVERY_DEPTH.with(|depth| {
            let current = depth.get();
            debug_assert!(current > 0);
            depth.set(current - 1);
            current == 1
        });
        if outer {
            drain_deferred_runs();
        }
    }
}

impl Drop for DeliveryGuard {
    fn drop(&mut self) {
        if self.active {
            CURRENT_DELIVERY_ID.with(|current| current.set(self.prev_id));
            DELIVERY_DEPTH.with(|depth| {
                let current = depth.get();
                if current > 0 {
                    depth.set(current - 1);
                }
            });
            if !std::thread::panicking() && !delivery_in_flight() {
                drain_deferred_runs();
            }
        }
    }
}

fn with_delivery_scope(body: impl FnOnce()) {
    let guard = DeliveryGuard::enter();
    body();
    guard.finish();
}

fn drain_deferred_runs() {
    loop {
        let runs = DEFERRED_RUNS.with(|r| std::mem::take(&mut *r.borrow_mut()));
        let settles = DEFERRED_ABSORBED_SETTLES.with(|s| std::mem::take(&mut *s.borrow_mut()));
        if runs.is_empty() && settles.is_empty() {
            break;
        }
        for node in runs {
            if node.is_live() {
                node.maybe_run();
            }
        }
        for node in settles {
            if node.is_live() {
                node.settle_after_absorbed_terminal();
            }
        }
    }
}

/// Record the node whose fn is about to run — the ERROR-bearing node on a panic.
fn wave_set_blamed(core: &Core) {
    WAVE.with(|w| {
        if let Some(scope) = w.borrow_mut().as_mut() {
            scope.blamed = Some(core.clone());
        }
    });
}

/// Turn a caught panic payload into the untyped `GraphError` (D31).
fn panic_to_error(payload: Box<dyn std::any::Any + Send>) -> GraphError {
    let msg = payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "node fn panicked".to_owned());
    msg.into()
}

/// Run `body` as a wave, with `owner` the wave-owner if no wave is yet in flight.
/// The outermost caller installs a [`WaveScope`] + `catch_unwind`; on a caught
/// panic it resets every touched node's wave-flags (B25) and emits `[[ERROR,e]]`
/// from the blamed cycle node (D30), then returns `on_error()`. Nested calls (a
/// re-entrant `subscribe`/`down` during the cascade) just run `body` — the outer
/// owner owns the single catch.
fn with_wave_owner<R>(owner: &Core, body: impl FnOnce() -> R, on_error: impl FnOnce() -> R) -> R {
    if wave_in_flight() {
        return body();
    }
    WAVE.with(|w| {
        *w.borrow_mut() = Some(WaveScope {
            owner: owner.clone(),
            touched: Vec::new(),
            blamed: None,
        });
    });
    let result = catch_unwind(AssertUnwindSafe(body));
    let scope = WAVE.with(|w| w.borrow_mut().take());
    let ret = match result {
        Ok(r) => r,
        Err(payload) => {
            // The scope is taken (we are outside the wave again). Recover every node
            // the aborted wave corrupted (B25), then surface the failure as ERROR on a
            // node ON the cycle (D30) — a fresh terminal wave.
            let scope = scope.expect("wave owner installed a scope");
            for n in &scope.touched {
                n.reset_wave_flags();
            }
            let blamed = scope.blamed.unwrap_or(scope.owner);
            let err = panic_to_error(payload);
            // The recovery ERROR emit runs OUTSIDE any catch (the scope is taken). A
            // user subscriber sink that itself panics while handling this ERROR must not
            // escape the public API / fault the recovery — the node is going terminal
            // anyway. Swallow a sink-panic here (delivery may be partial; acceptable for
            // a node being torn down).
            let _ = catch_unwind(AssertUnwindSafe(move || {
                blamed.down(vec![Message::Error(err)]);
            }));
            on_error()
        }
    };
    // Committed wave boundary (R-rewire-deferred / D47): drain any `ctx.rewire_next` queued
    // during this wave, each applied as a FRESH wave. Batch holds this drain until AFTER
    // the outermost commit/rollback, so topology never mutates on an uncommitted view
    // (R-rewire-batch-boundary / D67).
    if !boundary_drains_blocked() {
        drain_deferred_rewires(owner);
    }
    ret
}

struct UpRouteState {
    demand_fired: Vec<(LockId, Core)>,
}

impl UpRouteState {
    fn new() -> Self {
        Self {
            demand_fired: Vec::new(),
        }
    }

    fn mark_demand(&mut self, lock: &LockId, holder: &Core) -> bool {
        if self
            .demand_fired
            .iter()
            .any(|(l, h)| l == lock && h.ptr_eq(holder))
        {
            return true;
        }
        self.demand_fired.push((lock.clone(), holder.clone()));
        false
    }
}

/// Queue a deferred self-rewire (R-rewire-deferred / D47). Applied at the committed wave
/// boundary by [`drain_deferred_rewires`], never in place.
fn defer_rewire(owner: &Core, thunk: Box<dyn FnOnce()>) {
    owner.graph.borrow_mut().deferred_boundary.push_back(thunk);
}

pub(crate) fn defer_boundary_task(target: &Core, thunk: Box<dyn FnOnce()>) {
    defer_rewire(target, thunk);
}

pub(crate) fn drain_committed_boundary(target: &Core) {
    drain_deferred_rewires(target);
}

pub(crate) fn clear_deferred_boundary_tasks(target: &Core) {
    target.graph.borrow_mut().deferred_boundary.clear();
}

/// Drain the deferred-rewire FIFO at the committed boundary. Each thunk applies one queued
/// self-rewire as a FRESH wave (its own wave-owner) which may enqueue MORE — appended and
/// drained by this same loop (DrainExactlyOnce). A single global FIFO yields the drain order
/// for free: issue order during a synchronous cascade IS causal order (a dep settles before
/// its dependent's fn runs), so global-FIFO == per-node FIFO + causal-node order. Per-thunk
/// isolation: a thunk that panics does NOT abandon the rest of the queue (which would strand
/// thunks to mis-fire at an unrelated later wave); the first escape re-surfaces once the
/// queue is empty. Process-global is correct per D22 (one sync cascade per causal domain;
/// cross-domain is the async wire bridge, which never shares this stack — same basis as the
/// module-global `batch.active`).
fn drain_deferred_rewires(owner: &Core) {
    if owner.graph.borrow().draining_boundary {
        return; // a nested wave-owner exit during the drain — the outer loop owns draining
    }
    if owner.graph.borrow().deferred_boundary.is_empty() {
        return; // F-PERF: one empty-queue check per outermost wave when unused (behavior-neutral)
    }
    owner.graph.borrow_mut().draining_boundary = true;
    let mut escaped: Option<Box<dyn std::any::Any + Send>> = None;
    loop {
        let thunk = {
            let mut g = owner.graph.borrow_mut();
            if g.deferred_boundary.is_empty() {
                None
            } else {
                g.deferred_boundary.pop_front()
            }
        };
        let Some(thunk) = thunk else { break };
        if let Err(e) = catch_unwind(AssertUnwindSafe(thunk)) {
            if escaped.is_none() {
                escaped = Some(e);
            }
        }
    }
    owner.graph.borrow_mut().draining_boundary = false;
    if let Some(e) = escaped {
        std::panic::resume_unwind(e);
    }
}

impl Core {
    pub(crate) fn ptr_eq(&self, other: &Core) -> bool {
        Rc::ptr_eq(&self.graph, &other.graph)
            && self.id == other.id
            && self.generation == other.generation
    }

    fn is_live(&self) -> bool {
        self.graph.borrow().is_live_key(self.key())
    }

    pub(crate) fn same_graph(&self, other: &Core) -> bool {
        Rc::ptr_eq(&self.graph, &other.graph)
    }

    pub(crate) fn same_graph_arena(&self, arena: &GraphArena) -> bool {
        Rc::ptr_eq(&self.graph, &arena.0)
    }

    pub(crate) fn deps(&self) -> Vec<Core> {
        self.borrow().deps.clone()
    }

    pub(crate) fn cache_any(&self) -> Option<AnyValue> {
        self.with_inner_edges(|_n, e| e.value.cache.clone())
    }

    pub(crate) fn status(&self) -> Status {
        self.with_inner_edges(|_n, e| e.value.status)
    }

    pub(crate) fn handle(&self) -> Option<Handle> {
        self.borrow().handle
    }

    pub(crate) fn factory(&self) -> Option<String> {
        self.borrow().factory.clone()
    }

    pub(crate) fn identity_key(&self) -> (usize, usize, u64) {
        (Rc::as_ptr(&self.graph) as usize, self.id.0, self.generation)
    }

    fn new_in_arena(
        arena: &GraphArena,
        deps: Vec<Core>,
        handle: Option<Handle>,
        dispatcher: Dispatcher,
        initial: Option<AnyValue>,
        pausable: Pausable,
    ) -> Core {
        for dep in &deps {
            assert!(
                dep.same_graph_arena(arena),
                "node construction: dep belongs to a different graph; cross-graph deps require a wire bridge (D22/R-graph-domain)"
            );
        }
        let inner = NodeInner {
            deps,
            factory: None,
            handle,
            dispatcher,
            partial: false,
            has_called_fn_once: false,
            pausable,
            pull_id: None,
            // Defaults: plain derived/effect — auto-cascade COMPLETE/ERROR, terminal not
            // an input. derived_opts overrides from NodeOpts for last/reduce/rescue/*Map.
            complete_when_deps_complete: true,
            error_when_deps_error: true,
            terminal_as_real_input: false,
        };
        {
            let mut g = arena.0.borrow_mut();
            let id = g.alloc(inner);
            let generation = g.key_for(id).generation;
            // R-initial: a provided initial pre-populates the cache.
            if let Some(v) = initial {
                let (_n, e) = g.get_node_and_edges_mut(NodeKey { id, generation });
                e.value.cache = Some(v);
                e.value.has_data = true;
                e.value.status = Status::Settled;
            }
            Core {
                graph: arena.0.clone(),
                id,
                generation,
                refs: Rc::new(Cell::new(1)),
                counted: true,
            }
        }
    }

    fn configure_pull(&self, pull_id: Option<LockId>) {
        if let Some(id) = pull_id {
            self.with_inner_edges_aux_mut(|n, _e, a| {
                a.pause_lockset.insert(id.clone());
                n.pull_id = Some(id);
            });
        }
    }

    // ── subscription (R-push-subscribe) ──

    /// Register a sink, push START/cached state, and lazily activate on first subscriber.
    /// Records the new subscriber's id into `id_out` **immediately after registration**
    /// — before the push + activation cascade that may panic. The public [`Node::subscribe`]
    /// runs this under the wave-owner catch; if `activate()` panics (a feedback cycle),
    /// the sink is already registered, so the caller can still detach it (the error path
    /// reconstructs the real unsub from `id_out`) — no orphaned subscriber.
    fn subscribe_recording_id(&self, sink: Sink, id_out: &Cell<Option<u64>>) -> Unsub {
        let (id, push) = {
            self.with_inner_edges_aux_mut(|n, e, a| {
                let id = a.next_sub_id;
                a.next_sub_id += 1;
                a.subscribers.push((id, sink.clone()));
                let push = if n.pull_id.is_some() {
                    None
                } else if e.value.has_data {
                    Some(Message::Data(
                        e.value.cache.clone().expect("has_data ⇒ cache present"),
                    ))
                } else if e.value.status == Status::Dirty {
                    Some(Message::Dirty)
                } else {
                    None
                };
                (id, push)
            })
        };
        id_out.set(Some(id));
        sink(&Message::Start);
        if let Some(m) = push {
            sink(&m);
        }
        if !self.with_aux(|a| a.activated) {
            self.activate();
        }
        let core = self.clone();
        Box::new(move || core.unsubscribe(id))
    }

    fn unsubscribe(&self, id: u64) {
        let became_empty = self.with_aux_mut(|a| {
            let before = a.subscribers.len();
            a.subscribers.retain(|(sid, _)| *sid != id);
            a.subscribers.len() < before && a.subscribers.is_empty()
        });
        if became_empty {
            self.deactivate();
        }
    }

    // ── activation / deactivation (lazy; R-rom-ram) ──

    fn activate(&self) {
        let deps = self.with_inner_edges_aux_mut(|n, e, a| {
            a.activated = true;
            if let Some(id) = n.pull_id.clone() {
                a.pause_lockset.insert(id);
            }
            e.unsubs = (0..n.deps.len()).map(|_| None).collect();
            // placeholder boxes (distinct Rcs); subscribe_dep overwrites each with the
            // real box its callback captures.
            e.idx_boxes = (0..n.deps.len())
                .map(|_| Rc::new(Cell::new(-1i64)))
                .collect();
            n.deps.clone()
        });
        for (i, dep) in deps.iter().enumerate() {
            self.subscribe_dep(i, dep);
        }
        // Depless producer (fn, no deps): run once on activation.
        let run = {
            let n = self.borrow();
            n.deps.is_empty() && n.handle.is_some() && !n.has_called_fn_once
        };
        if run {
            self.run_wave();
        }
    }

    fn subscribe_dep(&self, idx: usize, dep: &Core) {
        let weak = self.downgrade();
        // R-rewire Option-C: the callback reads the dep's CURRENT index from a shared box
        // so a surgical reorder reroutes in O(1); -1 means the dep was removed (drain).
        let idx_box: Rc<Cell<i64>> = Rc::new(Cell::new(idx as i64));
        let cb_box = idx_box.clone();
        let sink: Sink = Rc::new(move |msg: &Msg| {
            let i = cb_box.get();
            if i < 0 {
                return; // dep removed — stale in-flight callback drops (drain)
            }
            weak.receive_from_dep(i as usize, msg);
        });
        // dep.subscribe synchronously pushes START (+ cached DATA) into the sink,
        // re-entering receive_from_dep — done before we re-borrow self below.
        let dep_sub_id = Cell::new(None);
        let subscribed = catch_unwind(AssertUnwindSafe(|| {
            dep.subscribe_recording_id(sink, &dep_sub_id)
        }));
        let unsub = match subscribed {
            Ok(unsub) => unsub,
            Err(payload) => {
                if let Some(id) = dep_sub_id.get() {
                    if self.with_inner_edges(|_, e| e.wave.in_dep_mutation) {
                        let dep = dep.clone();
                        let _ = catch_unwind(AssertUnwindSafe(move || dep.unsubscribe(id)));
                    }
                }
                std::panic::resume_unwind(payload);
            }
        };
        self.with_inner_edges_mut(|_, e| {
            e.unsubs[idx] = Some(unsub);
            e.idx_boxes[idx] = idx_box;
        });
    }

    fn deactivate(&self) {
        let (unsubs, hooks) = self.with_inner_edges_aux_mut(|n, e, a| {
            a.activated = false;
            let unsubs: Vec<Unsub> = e.unsubs.drain(..).flatten().collect();
            e.idx_boxes.clear();
            let hooks: Vec<Box<dyn FnOnce()>> = std::mem::take(&mut a.on_deactivation);
            // RAM: a compute node (fn and/or deps) clears its cache on deactivation;
            // a depless state node retains it (ROM).
            let is_compute = n.handle.is_some() || !n.deps.is_empty();
            if is_compute {
                e.value.cache = None;
                e.value.has_data = false;
                e.value.status = Status::Sentinel;
            }
            for i in 0..n.deps.len() {
                e.state.batch[i] = None;
                e.state.batch_waves[i].clear();
                e.state.batch_wave_id[i] = None;
                e.state.prev[i] = None;
                e.state.has_data[i] = false;
                e.state.dirty[i] = false;
                e.state.tier[i] = 0;
                e.state.terminal[i] = None;
                e.state.terminal_wave[i] = None;
            }
            e.state.pending = 0;
            e.wave.emitted_dirty_this_wave = false;
            n.has_called_fn_once = false;
            // Control + INVALIDATE hooks are fresh-lifecycle (R-cleanup-hooks / D28):
            // the next activation re-registers them from the fn body.
            a.on_invalidate.clear();
            a.pause_lockset.clear();
            a.pull_demand_owed = false;
            a.paused_dep_wave_occurred = false;
            a.pause_buffer.clear();
            if !a.state_persist {
                a.state = None;
            }
            (unsubs, hooks)
        });
        for u in unsubs {
            u();
        }
        for h in hooks {
            h();
        }
    }

    // ── upstream wave receive (two-phase + diamond) ──

    fn receive_from_dep(&self, idx: usize, msg: &Msg) {
        // B25: enroll in the wave touched-set so a panic-abort can reset our flags.
        // DR-8/B54: the internal dep-callback entry may be an uncounted arena view;
        // under a real wave this counted touched-set clone also pins the node while
        // borrow-free subscriber callbacks run.
        wave_register(self);
        // Terminal-is-forever (D17): a terminated node ignores further upstream messages.
        if self.with_inner_edges(|_n, e| e.value.terminal) {
            if matches!(msg, Message::Teardown) {
                self.emit_to_subs(&Message::Teardown);
            }
            return;
        }
        match msg {
            Message::Start => {}
            Message::Invalidate => {
                // The dep's value is gone — drop our view of it (prev_data → SENTINEL so
                // the never-emitted detector reads correctly, C-3) and cascade idempotently.
                let (had_data, undirty_no_settle) = self.with_inner_edges_aux_mut(|_n, e, a| {
                    e.state.prev[idx] = None;
                    e.state.has_data[idx] = false;
                    e.state.batch[idx] = None;
                    let had_current_projection = e.state.record_wave_sentinel(idx);
                    if !had_current_projection && !e.state.has_run_triggering_projection() {
                        e.state.clear_wave_projection(idx);
                    }
                    // Un-wedge the dirty bookkeeping if this dep had gone DIRTY first, so an
                    // INVALIDATE-before-DATA doesn't strand `pending` / downstream forever
                    // (R-invalidate-idempotent — the wedged-DIRTY deadlock guard).
                    if e.state.dirty[idx] {
                        e.state.dirty[idx] = false;
                        e.state.pending -= 1;
                    }
                    // D50 / R-paused-invalidate: this INVALIDATE SUPERSEDES the dep's
                    // buffered paused dep-wave (dep_batch[idx] just cleared). Re-derive the
                    // paused-recompute flag — if no dep still carries a buffered DATA, CANCEL
                    // the paused recompute (the node has settled to SENTINEL via its own
                    // INVALIDATE; a RESUME must not recompute against a now-SENTINEL dep —
                    // attributed cancellation). A surviving dep keeps it set; a later DATA
                    // re-arms it ([DATA, INVALIDATE, DATA2] → recompute with DATA2).
                    if a.paused_dep_wave_occurred && e.state.batch.iter().all(|b| b.is_none()) {
                        a.paused_dep_wave_occurred = false;
                    }
                    (
                        e.value.has_data,
                        e.state.pending == 0 && e.wave.emitted_dirty_this_wave,
                    )
                });
                self.invalidate(); // cascades INVALIDATE iff populated; no-op otherwise
                                   // If we broadcast DIRTY this wave but produced no settle (never populated, so
                                   // the cascade was suppressed), un-dirty downstream with one RESOLVED.
                if undirty_no_settle {
                    if !had_data {
                        self.down(vec![Message::Resolved]);
                    } else {
                        self.with_inner_edges_mut(|_, e| {
                            e.wave.emitted_dirty_this_wave = false;
                        });
                    }
                }
                self.fire_owed_demand_if_ready();
            }
            Message::Dirty => {
                let mark = self.with_inner_edges_mut(|_n, e| {
                    if !e.state.dirty[idx] {
                        e.state.dirty[idx] = true;
                        e.state.pending += 1;
                        e.state.tier[idx] = 2;
                        true
                    } else {
                        false
                    }
                });
                if mark && !self.is_pull_quiet() {
                    self.mark_dirty();
                }
            }
            Message::Data(v) => {
                self.with_inner_edges_mut(|_n, e| {
                    match &mut e.state.batch[idx] {
                        Some(b) => b.push(v.clone()),
                        none => *none = Some(vec![v.clone()]),
                    }
                    e.state.record_wave_data(idx, v.clone());
                    e.state.has_data[idx] = true;
                    e.state.tier[idx] = 3;
                    if e.state.dirty[idx] {
                        e.state.dirty[idx] = false;
                        e.state.pending -= 1;
                    }
                });
                self.maybe_run();
                self.fire_owed_demand_if_ready();
            }
            Message::Resolved => {
                self.with_inner_edges_mut(|_n, e| {
                    e.state.record_resolved_wave(idx);
                    e.state.tier[idx] = 3;
                    if e.state.dirty[idx] {
                        e.state.dirty[idx] = false;
                        e.state.pending -= 1;
                    }
                });
                self.maybe_run();
                self.fire_owed_demand_if_ready();
            }
            // Tier 5 (D34): COMPLETE | ERROR — ONE arm routed by the CENTRAL tier
            // (`Message::tier() == Tier::Terminal`), not per-variant. The shared terminal
            // bookkeeping (record the terminal + release the in-wave DIRTY) runs for ANY tier-5;
            // only the COMPLETE-vs-ERROR cascade differs, discriminated by the type within.
            m if m.tier() == Tier::Terminal => {
                // Box<dyn Error> is not Clone and `m` is a borrow (D31): keep the error as its
                // Display message in the DepTerminal record (Rc<str>, which IS Clone).
                let dep_term = match m {
                    Message::Error(e) => DepTerminal::Error(format!("{e}").into()),
                    _ => DepTerminal::Complete,
                };
                let is_error = matches!(dep_term, DepTerminal::Error(_));
                self.with_inner_edges_mut(|_n, e| {
                    e.state.terminal[idx] = Some(dep_term.clone());
                    e.state.terminal_wave[idx] = Some(dep_term.clone());
                });
                // R-terminal-settles-dirty (B35): a terminal RELEASES this dep's outstanding in-wave
                // DIRTY contribution (the exactly-one-settle invariant) — exactly as DATA/RESOLVED/
                // INVALIDATE do; a dirty-then-terminal-without-DATA dep would otherwise strand
                // `pending` and wedge the node (the INVALIDATE arm's guard, generalized to terminals).
                self.release_dep_dirty(idx);
                let (auto_error, auto_complete, tari) = {
                    let n = self.borrow();
                    (
                        n.error_when_deps_error,
                        n.complete_when_deps_complete,
                        n.terminal_as_real_input,
                    )
                };
                if is_error && auto_error {
                    // Auto-cascade ERROR (R-deps-terminal): forward the error's message. Node terminal.
                    if let DepTerminal::Error(s) = &dep_term {
                        self.down(vec![Message::Error(s.to_string().into())]);
                    }
                } else if tari {
                    // rescue/reduce/catch/*Map: the fn reads ctx.terminal(idx).
                    self.maybe_run();
                } else if auto_complete && self.all_deps_terminal() {
                    // R-deps-terminal auto-COMPLETE + B42: COMPLETE once ALL deps are TERMINAL (each
                    // COMPLETE or an absorbed ERROR) — so an absorbed-error dep terminating LAST still
                    // fires the cascade. `tari` is checked FIRST so a rescue recovers via maybe_run
                    // rather than being preempted (no node sets both complete_when_deps_complete + tari).
                    self.down(vec![Message::Complete]);
                } else {
                    // absorbed terminal, NOT an input: the dep's signalled change did not materialise
                    // (no DATA) → un-dirty downstream, keep cache.
                    self.settle_after_absorbed_terminal();
                }
                self.fire_owed_demand_if_ready();
            }
            Message::Teardown => {
                self.down(vec![Message::Complete, Message::Teardown]);
            }
            // PAUSE/RESUME are never delivered to a dep-subscriber (a node is paused via its
            // own up(), not by an upstream dep).
            _ => {}
        }
    }

    /// R-terminal-settles-dirty (B35): release a dep's outstanding in-wave DIRTY
    /// contribution. A settle-class event for that dep (DATA/RESOLVED inline above,
    /// INVALIDATE, and now COMPLETE/ERROR) clears its dirty flag + decrements `pending`.
    /// No-op if the dep already settled this wave — so the normal DATA-then-COMPLETE flow
    /// is unaffected. Makes the exactly-one-settle invariant a single shared step.
    fn release_dep_dirty(&self, idx: usize) {
        self.with_inner_edges_mut(|_n, e| {
            if e.state.dirty[idx] {
                e.state.dirty[idx] = false;
                e.state.pending -= 1;
            }
        });
    }

    /// B42 (R-deps-terminal): true iff EVERY dep is TERMINAL — COMPLETE *or* an absorbed ERROR
    /// (errorWhenDepsError:false). Block only on a LIVE dep (`dep_terminal[i]` is None); an errored
    /// dep COUNTS as terminal-done (was `matches!(.., Complete)`, which wedged a node whose
    /// errorWhenDepsError:false dep ERRORed — it never auto-completed even after every other dep
    /// completed). A node with no deps never auto-completes. combineLatest semantics (all, not any).
    fn all_deps_terminal(&self) -> bool {
        self.with_inner_edges(|n, e| {
            if n.deps.is_empty() {
                return false;
            }
            e.state.terminal.iter().all(|t| t.is_some())
        })
    }

    /// R-terminal-settles-dirty (B35): settle a node whose dirtied dep was released by an
    /// ABSORBED terminal that is NOT a real input (a plain derived/effect — one of several
    /// deps completing while others stay live). Runs only when the release drained `pending`
    /// while the node still owes a downstream settle (it broadcast DIRTY this wave):
    ///   - some OTHER dep delivered real DATA this wave → recompute (→ DATA);
    ///   - else no value materialised (or the recompute is gated) → one undirty RESOLVED
    ///     (R-resolved-undirty), keeping the cache (a terminal, unlike INVALIDATE, leaves it).
    fn settle_after_absorbed_terminal(&self) {
        if delivery_in_flight() {
            defer_absorbed_settle_until_delivery_boundary(self);
            return;
        }
        if !self.with_inner_edges(|_, e| e.state.pending == 0 && e.wave.emitted_dirty_this_wave) {
            return;
        }
        // A real value occurred this wave (some OTHER dep delivered DATA) → recompute.
        // maybe_run runs the fn ONLY if ungated (gate open, not paused); it may emit DATA, a
        // fn-synthesized undirty RESOLVED, or nothing (gated / gate still holds).
        let saw_data = self.with_inner_edges(|_n, e| {
            e.state
                .batch
                .iter()
                .any(|b| b.as_ref().is_some_and(|v| !v.is_empty()))
        });
        if saw_data {
            self.maybe_run();
        }
        // If the node STILL owes a downstream settle (no DATA occurred, OR the recompute was
        // gated — e.g. the first-run gate holds because the terminated dep never delivered and
        // terminal_as_real_input is false), balance the broadcast DIRTY with one undirty
        // RESOLVED, keeping the cache. Without this fallback a DIRTY-then-terminal-without-DATA
        // dep on a pre-first-run multi-dep node strands the DIRTY → downstream wedged (the B35
        // gate-holds corner, C-15(d)). Route through `down` so D64/R-undirty-settle-timing
        // honors resumeAll and batch timing.
        let still_owes = self.with_inner_edges(|_, e| e.wave.emitted_dirty_this_wave);
        if still_owes {
            self.down(vec![Message::Resolved]);
        }
    }

    fn mark_dirty(&self) {
        let emit = self.with_inner_edges_mut(|_n, e| {
            e.value.status = Status::Dirty;
            if !e.wave.emitted_dirty_this_wave {
                e.wave.emitted_dirty_this_wave = true;
                true
            } else {
                false
            }
        });
        if emit {
            self.emit_to_subs(&Message::Dirty);
        }
    }

    fn maybe_run(&self) {
        if delivery_in_flight() {
            defer_run_until_delivery_boundary(self);
            return;
        }
        // R-rewire (D42): an added cached dep's push-on-subscribe lands here mid-mutation.
        // Defer the fn-run to ONE atomic settle after every added dep is wired (P2 — the
        // fn never fires on a partially-populated added-dep view).
        if self.with_inner_edges(|_, e| e.wave.in_dep_mutation) {
            self.with_inner_edges_mut(|_, e| {
                e.wave.rewire_run_pending = true;
            });
            return;
        }
        // R-pause-modes: only the default "true" mode coalesces dep-driven recompute
        // (skip the fn while any lock is held → fire once with the latest dep values on
        // final-lock RESUME). `resumeAll` RUNS the fn while paused but buffers its output
        // (down → should_buffer_on_pause); `false` runs + emits immediately (ignores
        // PAUSE). D44: pause mode is the outer gate.
        if matches!(self.borrow().pausable, Pausable::True) && self.is_paused() {
            self.with_aux_mut(|a| a.paused_dep_wave_occurred = true);
            return;
        }
        self.try_run();
    }

    fn try_run(&self) {
        let (pending, has_handle, has_called, partial, all_settled) =
            self.with_inner_edges(|n, e| {
                (
                    e.state.pending,
                    n.handle.is_some(),
                    n.has_called_fn_once,
                    n.partial,
                    n.all_deps_settled(&e.state),
                )
            });
        if pending > 0 {
            return;
        }
        if !has_handle {
            self.passthrough_emit();
            return;
        }
        if !has_called {
            // first-run gate: hold the fn until every dep has settled (R-first-run-gate).
            if partial || all_settled {
                self.run_wave();
            }
            return;
        }
        self.run_wave();
    }

    fn passthrough_emit(&self) {
        // Single-dep wire (deps, no fn): relay dep 0's latest DATA downstream.
        let v = self.with_inner_edges_mut(|_n, e| {
            let v = e.state.batch[0].as_ref().and_then(|b| b.last().cloned());
            if let Some(v) = &v {
                e.state.prev[0] = Some(v.clone());
            }
            e.state.batch[0] = None;
            e.state.batch_waves[0].clear();
            e.state.batch_wave_id[0] = None;
            v
        });
        if let Some(v) = v {
            self.down(vec![Message::Data(v)]);
        }
    }

    // ── runtime topology rewire (R-rewire / D42, C-8) ──

    /// Runtime topology rewire — the public substrate entry (called by `Node::set_deps`/
    /// `add_dep`/`remove_dep`). The validation REJECTS run first and panic: called
    /// EXTERNALLY (no wave in flight) the panic propagates to the caller (idiomatic
    /// error); called MID-FN (inside a wave) the panic is caught by the running wave-owner
    /// → `[[ERROR,e]]` (R-reentrancy/D37 — a fn mutating its own topology mid-wave is the
    /// feedback cycle). The surgical Option-C mutation + atomic settle then run under a
    /// FRESH wave-owner so a user-fn panic during dep-activation/settle becomes ERROR
    /// (D30). INTRA-graph only (D22). Requires an explicit fn (SD-1 fn-deps pairing).
    pub(crate) fn rewire(&self, new_deps: Vec<Core>, fn_: NodeFn) {
        self.rewire_inner(new_deps, fn_, false);
    }

    fn rewire_inner(&self, new_deps: Vec<Core>, fn_: NodeFn, allow_terminal_owner: bool) {
        let new_deps = dedup_cores(new_deps);
        for dep in &new_deps {
            assert!(
                dep.same_graph(self),
                "rewire: dep belongs to a different graph; cross-graph deps require a wire bridge (D22/R-graph-domain)"
            );
        }
        if !allow_terminal_owner {
            let core = self.clone();
            let deps = new_deps.clone();
            let f = fn_.clone();
            if defer_after_batch_for_target(
                self,
                Box::new(move || core.rewire_inner(deps, f, false)),
            ) {
                return;
            }
        }
        {
            let (terminal, inside_run_wave, in_dep_mutation) = self.with_inner_edges(|_n, e| {
                (
                    e.value.terminal,
                    e.wave.inside_run_wave,
                    e.wave.in_dep_mutation,
                )
            });
            assert!(
                allow_terminal_owner || !terminal,
                "rewire: node is terminal (completed/errored) — cannot rewire (R-rewire / D42)"
            );
            assert!(
                !inside_run_wave,
                "rewire: mid-fn topology mutation — a fn mutating its own deps mid-wave is the feedback cycle (R-rewire / D37)"
            );
            assert!(
                !in_dep_mutation,
                "rewire: reentrant dep mutation — another setDeps/addDep/removeDep is in flight (R-rewire)"
            );
        }
        assert!(
            !new_deps.iter().any(|d| d.ptr_eq(self)),
            "rewire: self-dependency rejected (R-rewire / D42)"
        );
        let old_deps: Vec<Core> = self.borrow().deps.clone();
        for d in new_deps
            .iter()
            .filter(|d| !old_deps.iter().any(|o| o.ptr_eq(d)))
        {
            assert!(
                !reachable_upstream(d, self),
                "rewire: would create a cycle — dep already transitively depends on this node (R-rewire / D42)"
            );
            // Rust has no resubscribable-terminal opt-in yet (R-terminal, later slice) ⇒
            // ALL terminal deps are non-resubscribable → adding any terminal dep is rejected.
            assert!(
                !d.with_inner_edges(|_n, e| e.value.terminal),
                "rewire: cannot add a non-resubscribable terminal dep — would wedge (R-rewire / D42)"
            );
        }
        let core = self.clone();
        with_wave_owner(self, move || core.rewire_apply(new_deps, fn_), || {});
    }

    // ── deferred self-rewire (R-rewire-deferred / D47): ctx.rewire_next ──

    /// Enqueue a deferred self-rewire (`ctx.rewire_next`). Applied at the committed wave
    /// boundary by the wave-owner drain, NEVER in place — the immediate `set_deps`/`add_dep`/
    /// `remove_dep` still panics mid-fn (D37/R-reentrancy). The drain runs each as a fresh
    /// wave; this is the substrate affordance the higher-order *Map operators wire inners with.
    pub(crate) fn request_rewire_next(&self, req: RewireRequest) {
        let node = self.clone();
        defer_rewire(self, Box::new(move || node.apply_rewire_next(req)));
    }

    pub(crate) fn request_up_next(&self, msgs: Wave<AnyValue>, toward_dep: Option<usize>) {
        let node = self.clone();
        defer_rewire(self, Box::new(move || node.owned_up(msgs, toward_dep)));
    }

    /// Apply one queued self-rewire at the boundary (a drain thunk). D62: terminal seals output
    /// but does NOT cancel queued topology intent. The dep set is composed
    /// against the LIVE deps at apply time (so several requests in one wave compose). A rewire
    /// reject (cycle/self/non-resubscribable terminal dep) panics — caught here and surfaced as
    /// `[[ERROR,e]]` on this node (D30-consistent) so it does not strand the rest of the drain.
    fn apply_rewire_next(&self, req: RewireRequest) {
        let (new_deps, fn_) = match req {
            RewireRequest::Set(deps, f) => (deps, f),
            RewireRequest::Add(dep, f) => {
                let mut next = self.borrow().deps.clone();
                if !next.iter().any(|d| d.ptr_eq(&dep)) {
                    next.push(dep);
                }
                (next, f)
            }
            RewireRequest::Remove(dep, f) => {
                let next: Vec<Core> = self
                    .borrow()
                    .deps
                    .iter()
                    .filter(|d| !d.ptr_eq(&dep))
                    .cloned()
                    .collect();
                (next, f)
            }
        };
        // rewire() dedups + validates + applies under its own wave-owner (a fn-panic during the
        // apply becomes ERROR on the blamed node there). Only the PRE-apply validation rejects
        // panic OUT of rewire() — caught here → ERROR on this node, via owned_down (a fresh
        // wave-owner; the drain runs outside any live wave).
        let core = self.clone();
        let outcome = catch_unwind(AssertUnwindSafe(move || {
            core.rewire_inner(new_deps, fn_, true)
        }));
        if let Err(payload) = outcome {
            let err = panic_to_error(payload);
            self.owned_down(vec![Message::Error(err)]);
        }
    }

    /// The surgical Option-C mutation + atomic settle (D42), run under a wave-owner. Kept
    /// deps keep their subscription + per-dep state + idx-box (rerouted O(1)); removed deps
    /// drain (box→-1, unsub) + drop their dirty contribution; added deps fresh-subscribe
    /// (push-on-subscribe). The first-run gate + cache are PRESERVED (Q2/Q7). One atomic
    /// settle if any added dep delivered data or the sole dirty contributor was removed.
    fn rewire_apply(&self, new_deps: Vec<Core>, fn_: NodeFn) {
        let old_deps: Vec<Core> = self.borrow().deps.clone();
        let n = new_deps.len();
        let added: Vec<Core> = new_deps
            .iter()
            .filter(|d| !old_deps.iter().any(|o| o.ptr_eq(d)))
            .cloned()
            .collect();
        let removed: Vec<Core> = old_deps
            .iter()
            .filter(|d| !new_deps.iter().any(|nd| nd.ptr_eq(d)))
            .cloned()
            .collect();

        self.with_inner_edges_mut(|_, e| {
            e.wave.in_dep_mutation = true;
            e.wave.rewire_run_pending = false;
        });
        // QA-F1 (critical): clear `in_dep_mutation` on EVERY exit incl. a panic unwind during
        // the mutation. A user fn CAN panic in this window — an added dep's activation fn
        // (subscribe_dep → dep.activate → run_wave) or a removed dep's onDeactivation hook
        // (u() → deactivate → user closure). The wave-owner catch (with_wave_owner) then runs
        // reset_wave_flags, which does NOT cover in_dep_mutation and may not even include `self`
        // in `touched` — so without this guard a caught rewire panic leaves the node permanently
        // in_dep_mutation=true: every future maybe_run defers (never recomputes) + every future
        // rewire is rejected as reentrant. Mirrors the TS `finally { _inDepMutation = false }`.
        let _mut_guard = DepMutationGuard(self.clone());
        let activated = self.with_aux(|a| a.activated);
        let mut zero_dep_undirty = false;

        // fn swap (SD-1 + B32 GC): register the new fn in the SAME pool, then unregister the
        // old handle so the rewired-away closure (and its captures) is freed and its slot
        // reused. Register-first keeps `handle` never pointing at a freed slot.
        {
            let (old_handle, disp) = {
                let nn = self.borrow();
                (nn.handle, nn.dispatcher.clone())
            };
            let kind = old_handle
                .map(|h| disp.pool_kind(h.pool_id))
                .unwrap_or(PoolKind::Sync);
            let new_handle = register_with(&disp, kind, fn_);
            self.borrow_mut().handle = Some(new_handle);
            if let Some(oh) = old_handle {
                disp.unregister(oh);
            }
        }

        // removed deps: clear dirty contribution + drain (box→-1, unsubscribe the edge).
        let mut removed_dirty_contributor = false;
        for d in &removed {
            let old_idx = old_deps
                .iter()
                .position(|o| o.ptr_eq(d))
                .expect("removed dep was in old_deps");
            let unsub = {
                let mut graph = self.graph.borrow_mut();
                let (_nn, edges) = graph.get_node_and_edges_mut(self.key());
                if edges.state.dirty[old_idx] {
                    removed_dirty_contributor = true;
                    edges.state.pending -= 1;
                }
                if let Some(b) = edges.idx_boxes.get(old_idx) {
                    b.set(-1); // drain: any stale in-flight callback drops
                }
                edges.unsubs.get_mut(old_idx).and_then(|u| u.take())
            };
            if let Some(u) = unsub {
                u(); // stops the removed dep's edge — no further delivery
            }
        }

        // rebuild the per-dep parallel arrays in new order; kept deps carry their state +
        // subscription + idx-box (rerouted to the new index), added deps start fresh.
        {
            let mut graph = self.graph.borrow_mut();
            let (nn, edges) = graph.get_node_and_edges_mut(self.key());
            let mut new_batch: Vec<Option<Vec<AnyValue>>> = vec![None; n];
            let mut new_batch_waves: Vec<Vec<Vec<WaveData>>> = vec![Vec::new(); n];
            let mut new_batch_wave_id: Vec<Option<u64>> = vec![None; n];
            let mut new_prev: Vec<Option<AnyValue>> = vec![None; n];
            let mut new_has = vec![false; n];
            let mut new_dirty = vec![false; n];
            let mut new_tier = vec![0u8; n];
            let mut new_terminal: Vec<Option<DepTerminal>> = vec![None; n];
            let mut new_terminal_wave: Vec<Option<DepTerminal>> = vec![None; n];
            let mut new_unsubs: Vec<Option<Unsub>> = (0..n).map(|_| None).collect();
            let mut new_boxes: Vec<Rc<Cell<i64>>> =
                (0..n).map(|_| Rc::new(Cell::new(-1i64))).collect();
            for (j, dep) in new_deps.iter().enumerate() {
                if let Some(old_idx) = old_deps.iter().position(|o| o.ptr_eq(dep)) {
                    new_batch[j] = edges.state.batch[old_idx].take();
                    new_batch_waves[j] = std::mem::take(&mut edges.state.batch_waves[old_idx]);
                    new_batch_wave_id[j] = edges.state.batch_wave_id[old_idx].take();
                    new_prev[j] = edges.state.prev[old_idx].clone();
                    new_has[j] = edges.state.has_data[old_idx];
                    new_dirty[j] = edges.state.dirty[old_idx];
                    new_tier[j] = edges.state.tier[old_idx];
                    new_terminal[j] = edges.state.terminal[old_idx].clone();
                    new_terminal_wave[j] = edges.state.terminal_wave[old_idx].clone();
                    if let Some(slot) = edges.unsubs.get_mut(old_idx) {
                        new_unsubs[j] = slot.take();
                    }
                    if let Some(b) = edges.idx_boxes.get(old_idx) {
                        b.set(j as i64); // O(1) reroute of the kept dep's callback index
                        new_boxes[j] = b.clone();
                    }
                }
            }
            nn.deps = new_deps.clone();
            edges.state.batch = new_batch;
            edges.state.batch_waves = new_batch_waves;
            edges.state.batch_wave_id = new_batch_wave_id;
            edges.state.prev = new_prev;
            edges.state.has_data = new_has;
            edges.state.dirty = new_dirty;
            edges.state.tier = new_tier;
            edges.state.terminal = new_terminal;
            edges.state.terminal_wave = new_terminal_wave;
            edges.unsubs = new_unsubs;
            edges.idx_boxes = new_boxes;
        }

        // subscribe added deps — push-on-subscribe delivers a cached dep's DATA (driving
        // maybe_run, which DEFERS to the atomic settle via in_dep_mutation); a SENTINEL dep
        // delivers START only. Only when activated (else activation will subscribe later).
        if activated {
            for d in &added {
                let idx = new_deps
                    .iter()
                    .position(|nd| nd.ptr_eq(d))
                    .expect("added dep present in new_deps");
                self.subscribe_dep(idx, d);
            }
        }

        // Q6 auto-settle: removing the sole dirty contributor closes the wave. With deps
        // remaining → request the atomic settle; with zero deps → just un-dirty downstream
        // (degenerate fn-no-deps). Cache is preserved either way (Q7).
        {
            let should_rewire_settle = self.with_inner_edges(|_n, e| {
                removed_dirty_contributor && e.state.pending == 0 && e.value.status == Status::Dirty
            });
            if should_rewire_settle {
                if new_deps.is_empty() {
                    zero_dep_undirty = true;
                } else {
                    self.with_inner_edges_mut(|_, e| {
                        e.wave.rewire_run_pending = true;
                    });
                }
            }
        }

        self.with_inner_edges_mut(|_, e| {
            e.wave.in_dep_mutation = false;
        });

        // Atomic post-mutation settle (a fresh wave): ONE two-phase DIRTY→DATA if an added
        // dep delivered data or a sole-dirty dep was removed; else the zero-dep un-dirty.
        let run_pending =
            self.with_inner_edges_mut(|_, e| std::mem::take(&mut e.wave.rewire_run_pending));
        if run_pending {
            self.settle_rewire();
        } else if zero_dep_undirty {
            let emitted = self.with_inner_edges(|_, e| e.wave.emitted_dirty_this_wave);
            if emitted {
                self.down(vec![Message::Resolved]); // un-dirty downstream (pause/batch-safe)
            } else {
                self.with_inner_edges_mut(|_n, e| {
                    e.value.status = if e.value.has_data {
                        Status::Settled
                    } else {
                        Status::Sentinel
                    };
                });
            }
        }
    }

    /// R-rewire atomic settle (D42 + the D1 /qa fix): emit ONE proper two-phase
    /// DIRTY→DATA wave after a rewire that warrants a recompute. Mirrors `try_run`'s
    /// pause + gate + pending guards, then injects the phase-1 DIRTY the added dep's
    /// [START,DATA] handshake did not carry (R-dirty-before-data).
    fn settle_rewire(&self) {
        if matches!(self.borrow().pausable, Pausable::True) && self.is_paused() {
            self.with_aux_mut(|a| a.paused_dep_wave_occurred = true);
            return;
        }
        let (pending, has_handle, has_called, partial, all_settled) =
            self.with_inner_edges(|n, e| {
                (
                    e.state.pending,
                    n.handle.is_some(),
                    n.has_called_fn_once,
                    n.partial,
                    n.all_deps_settled(&e.state),
                )
            });
        if pending > 0 {
            return;
        }
        if !has_handle {
            self.passthrough_emit();
            return;
        }
        if !(has_called || partial || all_settled) {
            return; // first-run gate still holds
        }
        self.mark_dirty(); // phase 1 (no-op if already dirty, e.g. removeDep auto-settle)
        self.run_wave(); // phase 2: fn → DATA / undirty RESOLVED
    }

    fn run_wave(&self) {
        // B25: enroll in the wave touched-set before any mutation so a panic-abort
        // can reset our flags.
        wave_register(self);
        if self.with_inner_edges(|_, e| e.wave.inside_run_wave) {
            // R-reentrancy (D37): a fn re-driving its own dep mid-wave is the
            // synchronous feedback cycle. Reject by panicking (the Rust analogue of a
            // value-level throw) — the wave-owner's catch_unwind converts it to
            // [[ERROR,e]] on a node ON the cycle (D30 / C-6, see `with_wave_owner`),
            // rather than recurse to a stack overflow.
            panic!(
                "synchronous feedback cycle: node fn re-entered its own wave (R-reentrancy / D37)"
            );
        }
        // build ctx (snapshot the per-dep wave view), reading under a short borrow.
        let (handle, dispatcher, ctx) = self.with_inner_edges(|n, e| {
            let dep_records: Vec<DepRecord> = (0..n.deps.len())
                .map(|i| {
                    let latest = e.state.batch[i]
                        .as_ref()
                        .and_then(|b| b.last().cloned())
                        .or_else(|| e.state.prev[i].clone());
                    DepRecord {
                        wave_data: e.state.batch_waves[i].clone(),
                        prev_data: e.state.prev[i].clone(),
                        latest,
                        terminal: e.state.terminal_wave[i].clone(),
                    }
                })
                .collect();
            (
                n.handle.expect("run_wave ⇒ handle present"),
                n.dispatcher.clone(),
                Ctx::new(self.clone(), dep_records),
            )
        });
        let (old_on_invalidate, old_on_deactivation) = self.with_inner_edges_aux_mut(|n, e, a| {
            n.has_called_fn_once = true;
            e.wave.inside_run_wave = true;
            e.wave.emitted_tier3_this_wave = false;
            // R-cleanup-hooks per-run lifecycle (D28 clarification / C-14): clear BOTH
            // hook lists before the fn runs; the fn body re-registers the current run's
            // hooks. Only the latest run's registrations are live — a re-run supersedes the
            // prior run's, discarded WITHOUT firing (no fire-on-rerun; onRerun stays cut).
            // Fixes the push-only accumulation (K stale hooks fired after K runs).
            (
                std::mem::take(&mut a.on_invalidate),
                std::mem::take(&mut a.on_deactivation),
            )
        });
        // Dropping user hooks may drop captured `Core`s. Do that after releasing the
        // graph-arena borrow, or the captured handle's `Drop` can re-enter `GraphCore`.
        drop(old_on_invalidate);
        drop(old_on_deactivation);
        // D30: this is the innermost node whose fn runs — blame it if the fn (or a
        // re-entry it triggers) panics, so the ERROR lands "nearest the throw" (C-6).
        wave_set_blamed(self);
        let _guard = WaveGuard(self.clone());
        // borrow-free: the fn calls ctx.down → re-enters this node's _down.
        dispatcher.invoke(handle, &ctx);
        drop(_guard); // clears inside_run_wave
                      // R-resolved-undirty (D49): a node DIRTY'd downstream in phase 1
                      // that produced NO tier-3 value this run (filter-reject / no-emit
                      // fn) emits exactly one balancing RESOLVED to clear the downstream
                      // dirty — the substrate synthesizes it so the operator body stays
                      // clean (R-primary-api-clean).
                      // EXEMPT async-pool nodes: an async fn that returns WITHOUT emitting has DEFERRED
                      // its result (it emits later via a stashed DeferredCtx), NOT rejected — synthesizing
                      // an undirty RESOLVED here would prematurely settle a still-pending diamond leg
                      // (R-async-paused / C-4). The eventual deferred emit carries its own DIRTY balance.
        let undirty = self.with_inner_edges(|n, e| {
            // EXEMPT a TERMINAL wave (the fn emitted COMPLETE/ERROR): the terminal IS the settle,
            // and R-terminal-settles-dirty releases the downstream dirty — synthesizing an undirty
            // RESOLVED here would overwrite the terminal status + emit a spurious post-terminal
            // RESOLVED (mirrors the TS `_terminal === undefined` guard, node.ts). Pre-existing
            // parity gap surfaced by C-11 (a fn emitting a bare COMPLETE).
            e.wave.emitted_dirty_this_wave
                && !e.wave.emitted_tier3_this_wave
                && !e.value.terminal
                && !Self::inner_is_async(n)
        });
        if undirty {
            {
                // status reflects cache freshness: a carried-over value → Resolved
                // (fresh, no new occurrence); never-valued → Sentinel. The RESOLVED
                // message clears the downstream dirty either way.
                self.with_inner_edges_mut(|_n, e| {
                    e.value.status = if e.value.has_data {
                        Status::Resolved
                    } else {
                        Status::Sentinel
                    };
                });
            }
            self.emit_to_subs(&Message::Resolved);
        }
        // roll wave-local state forward.
        self.with_inner_edges_mut(|_n, e| {
            for (i, b) in e.state.batch.iter_mut().enumerate() {
                if let Some(last) = b.as_ref().and_then(|batch| batch.last()).cloned() {
                    e.state.prev[i] = Some(last);
                }
                *b = None;
                e.state.batch_waves[i].clear();
                e.state.batch_wave_id[i] = None;
                e.state.terminal_wave[i] = None;
            }
            e.wave.emitted_dirty_this_wave = false;
            e.wave.emitted_tier3_this_wave = false;
        });
    }

    // ── downstream emission (the unified waist) ──

    /// Emit a wave toward sinks. Synthesizes the leading DIRTY for an external
    /// tier-3 emit (R-dirty-before-data), updates cache/status, and broadcasts.
    ///
    /// D49 (supersedes D15): every value-occurrence is emitted as **DATA** — the
    /// substrate does NOT substitute DATA→RESOLVED on value-equality and never
    /// inspects the per-wave DATA count for substitution (dedup is opt-in at the
    /// operator layer). A RESOLVED reaching here is an explicit operator escape
    /// hatch; the undirty RESOLVED is synthesized in [`Core::run_wave`].
    pub(crate) fn down(&self, msgs: Vec<Msg>) {
        // Terminal-is-forever (D17 / R-terminal): a terminated node emits nothing
        // further — including a self-emit DATA via `set()` / `ctx.down`. (The
        // COMPLETE/ERROR arms below also self-guard against a double terminal.)
        if self.with_inner_edges(|_n, e| e.value.terminal) {
            return;
        }
        // B25: enroll in the wave touched-set before any mutation.
        wave_register(self);
        let mut sorted = msgs;
        sorted.sort_by_key(|m| m.tier().as_u8()); // stable: preserves intra-tier order
        let inside = self.with_inner_edges(|_, e| e.wave.inside_run_wave);

        // R-invalidate-idempotent: collapse repeated INVALIDATE in one wave so the
        // cleanup hook + downstream broadcast fire at most once.
        if sorted
            .iter()
            .filter(|m| matches!(m, Message::Invalidate))
            .count()
            > 1
        {
            let mut seen = false;
            sorted.retain(|m| {
                if matches!(m, Message::Invalidate) {
                    let keep = !seen;
                    seen = true;
                    keep
                } else {
                    true
                }
            });
        }

        if !inside && collecting_batch() {
            let (deferred, rest): (Vec<Msg>, Vec<Msg>) = sorted
                .into_iter()
                .partition(|m| m.tier().is_batch_deferred());
            if !deferred.is_empty() && defer_to_batch(self, deferred) {
                if !self.with_inner_edges(|_, e| e.wave.emitted_dirty_this_wave) {
                    self.emit_dirty_once();
                }
                self.with_inner_edges_mut(|_, e| {
                    e.wave.batch_dirty_owed = true;
                });
                return;
            }
            sorted = rest;
            if sorted.is_empty() {
                return;
            }
        }

        // R-pause-modes / R-async-paused (D44): while paused, defer the tier-3/4 settle
        // slice (DATA/RESOLVED/INVALIDATE) into the pause buffer; tier<3 (DIRTY/PAUSE/
        // RESUME) and tier-5/6 (terminal/TEARDOWN) bypass so control + end-of-stream
        // always reach observers. Replayed in arrival order on final-lock RESUME
        // (on_resume). MUST run before the tier-3 counting + DIRTY synthesis below so a
        // buffered wave emits nothing while paused (matches the TS ordering).
        if self.should_buffer_on_pause() {
            let (buffered, rest): (Vec<Msg>, Vec<Msg>) = sorted
                .into_iter()
                .partition(|m| m.tier().is_pause_buffered());
            if !buffered.is_empty() {
                self.with_inner_edges_aux_mut(|_n, e, a| {
                    // A buffered tier-3 IS a produced settle — mark emitted_tier3 so run_wave
                    // does NOT synthesize a spurious undirty RESOLVED for a fn whose DATA was
                    // merely deferred into the buffer (per-language correctness, D24: TS sets
                    // this flag only in the post-buffer emit loop, leaving a resumeAll recompute
                    // prone to that spurious RESOLVED; recognizing the settle at buffer time is
                    // the more-correct reading of R-resolved-undirty).
                    e.wave.emitted_tier3_this_wave = true;
                    a.pause_buffer.push(buffered);
                });
            }
            sorted = rest;
            if sorted.is_empty() {
                return;
            }
        }

        let mut data_count = 0usize;
        let mut has_resolved = false;
        let mut has_tier3 = false;
        for m in &sorted {
            match m {
                Message::Data(_) => {
                    data_count += 1;
                    has_tier3 = true;
                }
                Message::Resolved => {
                    has_resolved = true;
                    has_tier3 = true;
                }
                _ => {}
            }
        }
        // tier-3 exclusivity (R-resolved-undirty / D49): a wave's tier-3 slot is
        // ≥1 DATA XOR 1 RESOLVED (occurrence vs undirty), never mixed.
        assert!(
            !(data_count >= 1 && has_resolved),
            "down: a wave cannot mix DATA and RESOLVED (tier-3 exclusivity, R-resolved-undirty / D49)"
        );

        // Synthesize a leading DIRTY for an EXTERNAL tier-3 emit. Inside run_wave the
        // DIRTY already propagated in phase 1 (or the wave is activation-exempt).
        if has_tier3 && !inside {
            self.emit_dirty_once();
        }

        with_delivery_scope(|| {
            for m in sorted {
                match m {
                    Message::Dirty => self.emit_dirty_once(),
                    Message::Data(v) => {
                        // D49: every occurrence is DATA — no equals-substitution.
                        {
                            let mut n = self.graph.borrow_mut();
                            let (_inner, e) = n.get_node_and_edges_mut(self.key());
                            e.value.cache = Some(v.clone());
                            e.value.has_data = true;
                            e.value.status = Status::Settled;
                            e.wave.emitted_tier3_this_wave = true;
                        }
                        self.emit_to_subs(&Message::Data(v));
                    }
                    Message::Resolved => {
                        // Explicit operator escape hatch (D49); the substrate-synthesized
                        // undirty RESOLVED usually goes through run_wave. Dirty-balance paths
                        // that must honor pause/batch timing also route through here, so keep
                        // the no-cache case at SENTINEL while still emitting RESOLVED.
                        {
                            let mut n = self.graph.borrow_mut();
                            let (_inner, e) = n.get_node_and_edges_mut(self.key());
                            e.value.status = if e.value.has_data {
                                Status::Resolved
                            } else {
                                Status::Sentinel
                            };
                            e.wave.emitted_tier3_this_wave = true;
                        }
                        self.emit_to_subs(&Message::Resolved);
                    }
                    Message::Invalidate => {
                        // Honor the invalidate-request: clear cache → SENTINEL, fire
                        // onInvalidate, broadcast downstream (idempotent, no-op if unpopulated).
                        self.invalidate();
                    }
                    Message::Complete => {
                        let go = {
                            self.with_inner_edges_aux_mut(|_n, e, a| {
                                if e.value.terminal {
                                    return false;
                                }
                                e.value.terminal = true;
                                e.value.status = Status::Completed;
                                a.pull_demand_owed = false;
                                a.paused_dep_wave_occurred = false;
                                a.pause_buffer.clear();
                                true
                            })
                        };
                        if go {
                            self.emit_to_subs(&Message::Complete);
                        }
                    }
                    Message::Error(e) => {
                        let go = {
                            self.with_inner_edges_aux_mut(|_n, edges, a| {
                                if edges.value.terminal {
                                    return false;
                                }
                                edges.value.terminal = true;
                                edges.value.status = Status::Errored;
                                a.pull_demand_owed = false;
                                a.paused_dep_wave_occurred = false;
                                a.pause_buffer.clear();
                                true
                            })
                        };
                        if go {
                            self.emit_to_subs(&Message::Error(e));
                        }
                    }
                    Message::Teardown => {
                        self.emit_to_subs(&Message::Teardown);
                    }
                    // PAUSE / RESUME are control-only and are delivered via up().
                    _ => {}
                }
            }
        });

        if !inside {
            self.with_inner_edges_mut(|_, e| {
                e.wave.emitted_dirty_this_wave = false;
                e.wave.emitted_tier3_this_wave = false;
            });
        }
    }

    /// External deferred emit — the async-pool late-emit path used by
    /// [`crate::ctx::DeferredCtx`] (R-sync-core: async lives in the fn body, the emit
    /// serializes back onto the single thread). Establishes a FRESH wave-owner boundary
    /// (like [`Node::down`]) so a panic in the cascade becomes `[[ERROR,e]]` (D30) and the
    /// leading DIRTY is synthesized (`inside_run_wave` is false here). Nested under a live
    /// wave-owner (e.g. a deferred emit fired during a RESUME cascade) it just runs.
    pub(crate) fn owned_down(&self, msgs: Wave<AnyValue>) {
        let core = self.clone();
        with_wave_owner(self, move || core.down(msgs), || {});
    }

    pub(crate) fn owned_up(&self, msgs: Wave<AnyValue>, toward_dep: Option<usize>) {
        let core = self.clone();
        with_wave_owner(self, move || core.up(msgs, toward_dep), || {});
    }

    pub(crate) fn commit_batched_wave(&self, wave: Wave<AnyValue>) {
        self.with_inner_edges_mut(|_, e| {
            e.wave.batch_dirty_owed = false;
        });
        self.owned_down(wave);
    }

    pub(crate) fn rollback_batched(&self) {
        let should_balance = self.with_inner_edges_mut(|_, e| {
            if e.wave.batch_dirty_owed {
                e.wave.batch_dirty_owed = false;
                true
            } else {
                false
            }
        });
        if should_balance {
            self.owned_down(vec![Message::Resolved]);
        }
    }

    fn emit_dirty_once(&self) {
        let emit = self.with_inner_edges_mut(|_n, e| {
            if !e.wave.emitted_dirty_this_wave {
                e.wave.emitted_dirty_this_wave = true;
                e.value.status = Status::Dirty;
                true
            } else {
                false
            }
        });
        if emit {
            self.emit_to_subs(&Message::Dirty);
        }
    }

    fn emit_to_subs(&self, msg: &Msg) {
        // Clone the sink handles out, drop the borrow, then deliver — guards against
        // subscribe/unsubscribe (and any re-entry) during iteration.
        let subs: Vec<Sink> =
            self.with_aux(|a| a.subscribers.iter().map(|(_, s)| s.clone()).collect());
        for s in subs {
            s(msg);
        }
    }

    /// Emit upstream toward deps — control tiers only (R-ctx-up). PAUSE/RESUME act on
    /// this node's own lockset (R-pause-lockset). For the other up-allowed kinds
    /// (INVALIDATE/DIRTY/TEARDOWN) a node is either the TERMINUS (depless source) or a
    /// pass-through INTERMEDIATE (R-up-at-source / D38, C-7):
    /// - depless source = terminus: INVALIDATE → HONOR (route through `down` → clear
    ///   cache to SENTINEL, fire onInvalidate, broadcast INVALIDATE downstream); routing
    ///   through `down` (not a direct `invalidate()`) keeps it batch/pause-consistent with
    ///   a downstream-originated INVALIDATE. DIRTY/TEARDOWN → DROP (no coherent terminus
    ///   action: self-DIRTY would wedge downstream awaiting a settle that never comes;
    ///   source lifecycle is source/graph-owned).
    /// - dep-bearing intermediate: forward toward deps only, never self-act (the source
    ///   is the single actor, the down-cascade is the effect).
    pub(crate) fn up(&self, msgs: Vec<Msg>, toward_dep: Option<usize>) {
        let mut route = UpRouteState::new();
        self.up_route(msgs, toward_dep, &mut route);
    }

    fn up_route(&self, msgs: Vec<Msg>, toward_dep: Option<usize>, route: &mut UpRouteState) {
        for m in &msgs {
            assert!(
                m.is_up_allowed(),
                "ctx.up: {m:?} is down-only; up carries control tiers only (R-ctx-up)"
            );
        }
        let is_depless = self.borrow().deps.is_empty();
        for m in msgs {
            match m {
                Message::Pause(lock) => self.pause_acquire(lock),
                Message::Resume(lock) => {
                    let is_pull_demand = self.borrow().pull_id.as_ref() == Some(&lock);
                    if is_pull_demand {
                        if !route.mark_demand(&lock, self) {
                            self.on_demand();
                        }
                    } else if self.with_aux(|a| a.pause_lockset.contains(&lock)) {
                        self.pause_release(lock);
                    } else {
                        self.forward_up(Message::Resume(lock), toward_dep, route);
                    }
                }
                // R-up-at-source (D38): INVALIDATE/DIRTY/TEARDOWN.
                other if is_depless => {
                    // terminus: honor INVALIDATE, drop DIRTY/TEARDOWN.
                    if matches!(other, Message::Invalidate) {
                        self.down(vec![Message::Invalidate]);
                    }
                }
                other => {
                    // dep-bearing intermediate: forward toward deps (reconstruct the
                    // payload-free control kind — Msg is not Clone, but only the unit
                    // control kinds reach here per is_up_allowed minus PAUSE/RESUME).
                    self.forward_up(other, toward_dep, route);
                }
            }
        }
    }

    fn forward_up(&self, m: Msg, toward_dep: Option<usize>, route: &mut UpRouteState) {
        let deps = self.borrow().deps.clone();
        if let Some(i) = toward_dep {
            if let Some(dep) = deps.get(i) {
                dep.up_route(vec![m], None, route);
            }
            return;
        }
        for dep in &deps {
            dep.up_route(vec![dup_control(&m)], None, route);
        }
    }

    // ── PAUSE/RESUME lockset (R-pause-lockset) + default mode (R-pause-modes) ──

    fn is_paused(&self) -> bool {
        !self.with_aux(|a| a.pause_lockset.is_empty())
    }

    fn is_pull_quiet(&self) -> bool {
        self.with_inner_edges_aux_mut(|n, _e, a| {
            n.pull_id
                .as_ref()
                .is_some_and(|id| a.pause_lockset.contains(id))
        })
    }

    fn pause_acquire(&self, lock: LockId) {
        // Set ⇒ a same-id repeat PAUSE is idempotent.
        self.with_aux_mut(|a| {
            a.pause_lockset.insert(lock);
        });
    }

    fn pause_release(&self, lock: LockId) {
        let Some(resumed) = self.with_aux_mut(|a| {
            if !a.pause_lockset.remove(&lock) {
                return None; // unknown id ⇒ no-op
            }
            Some(a.pause_lockset.is_empty()) // another lock still held ⇒ stay paused
        }) else {
            return;
        };
        if resumed {
            self.on_resume();
        }
        self.fire_owed_demand_if_ready();
    }

    fn on_demand(&self) {
        let has_pull = self.with_inner_edges_aux_mut(|n, _e, a| {
            if n.pull_id.is_none() {
                return false;
            }
            a.pull_demand_owed = true;
            true
        });
        if !has_pull {
            return;
        }
        self.fire_owed_demand_if_ready();
    }

    fn fire_owed_demand_if_ready(&self) {
        let Some((pull_id, should_mark_dirty)) = self.with_inner_edges_aux_mut(|n, e, a| {
            let id = n.pull_id.clone()?;
            if e.value.terminal || !a.pull_demand_owed || e.state.pending != 0 {
                return None;
            }
            if a.pause_lockset.iter().any(|held| held != &id) {
                return None;
            }
            if n.handle.is_some()
                && !n.has_called_fn_once
                && !n.partial
                && !n.all_deps_settled(&e.state)
            {
                return None;
            }
            Some((id, a.paused_dep_wave_occurred))
        }) else {
            return;
        };
        self.with_aux_mut(|a| {
            a.pull_demand_owed = false;
            a.pause_lockset.remove(&pull_id);
        });
        struct Requiet<'a> {
            core: &'a Core,
            id: Option<LockId>,
        }
        impl Drop for Requiet<'_> {
            fn drop(&mut self) {
                let Some(id) = self.id.take() else { return };
                let _ = self.core.try_with_inner_aux_mut(|n, e, a| {
                    if !e.value.terminal && n.pull_id.as_ref() == Some(&id) {
                        a.pause_lockset.insert(id);
                    }
                });
            }
        }
        let _requiet = Requiet {
            core: self,
            id: Some(pull_id),
        };
        if should_mark_dirty {
            self.mark_dirty();
        }
        self.on_resume();
    }

    fn on_resume(&self) {
        // terminal-is-forever: a node that terminated while paused discards its buffer
        // and never replays/recomputes (BH3).
        if self.with_inner_edges(|_n, e| e.value.terminal) {
            self.with_aux_mut(|a| {
                a.paused_dep_wave_occurred = false;
                a.pause_buffer.clear();
            });
            return;
        }
        // Drain buffered settle slices (resumeAll / async-at-paused, R-async-paused). The
        // lockset is already empty (pause_release removed the final lock before calling
        // us), so each replayed wave emits normally — DIRTY synth + DATA, no longer
        // buffered. Drain BEFORE the default-mode recompute (matches the TS arm order).
        let buf = { self.with_aux_mut(|a| std::mem::take(&mut a.pause_buffer)) };
        for wave in buf {
            self.down(wave);
        }
        // Default ("true") mode: a dep wave skipped while paused fires the fn once now,
        // with the latest dep values.
        let fire = { self.with_aux_mut(|a| std::mem::take(&mut a.paused_dep_wave_occurred)) };
        if fire {
            self.try_run();
        }
    }

    /// Async-pool check that takes an existing borrow (avoids a re-borrow when the caller
    /// already holds `NodeInner`). The pool kind is a dispatcher property of the handle.
    fn inner_is_async(n: &NodeInner) -> bool {
        match n.handle {
            Some(h) => n.dispatcher.pool_kind(h.pool_id) == PoolKind::Async,
            None => false,
        }
    }

    /// Should an outgoing settle slice be deferred into the pause buffer? `pausable` mode
    /// is the OUTER gate over R-async-paused buffering (D44).
    fn should_buffer_on_pause(&self) -> bool {
        self.with_inner_edges(|n, e| match n.pausable {
            // false: ignore PAUSE/RESUME ENTIRELY — never buffer, keep producing (B20).
            Pausable::False => false,
            _ if self.with_aux(|a| a.pause_lockset.is_empty()) => false,
            // resumeAll: production-gating — buffer the own (sync/async) settle slice too.
            Pausable::ResumeAll => true,
            // true (default): PAUSE gates recomputation/propagation, NOT a leaf source's
            // own production. An async COMPUTE node's (deps>0) in-flight result buffers
            // (C-2); a depless async leaf source delivers immediately (C-10). The
            // leaf-vs-compute discriminator is deps.is_empty().
            Pausable::True => {
                !e.wave.inside_run_wave && Self::inner_is_async(n) && !n.deps.is_empty()
            }
        })
    }

    /// R-invalidate-idempotent: clear cache → SENTINEL, flush onInvalidate, broadcast
    /// INVALIDATE downstream. A no-op if nothing is cached (never-populated /
    /// already-reset are observationally equivalent — prevents double-cleanup).
    fn invalidate(&self) {
        let hooks = {
            self.with_inner_edges_aux_mut(|_n, e, a| {
                if !e.value.has_data {
                    return None; // idempotent no-op
                }
                e.value.cache = None;
                e.value.has_data = false;
                e.value.status = Status::Sentinel;
                Some(a.on_invalidate.clone()) // re-callable; clone out so we fire borrow-free
            })
        };
        let Some(hooks) = hooks else {
            return;
        };
        for f in hooks {
            f();
        }
        self.emit_to_subs(&Message::Invalidate);
    }

    /// B25: reset transient wave-flags to their between-wave baseline after a
    /// panic-aborted wave (called from `with_wave_owner` on the touched-set). Leaves
    /// PERSISTED state (cache / dep_prev / dep_has_data / status / ctx.state) intact —
    /// only the wave-local bookkeeping a clean wave would have rolled forward itself.
    fn reset_wave_flags(&self) {
        self.with_inner_edges_mut(|n, e| {
            e.wave.inside_run_wave = false;
            e.wave.emitted_dirty_this_wave = false;
            e.wave.emitted_tier3_this_wave = false;
            e.state.pending = 0;
            for i in 0..n.deps.len() {
                e.state.batch[i] = None;
                e.state.batch_waves[i].clear();
                e.state.batch_wave_id[i] = None;
                e.state.terminal_wave[i] = None;
                e.state.dirty[i] = false;
            }
            // `dep_terminal` is intentionally NOT reset here: it is a PERSISTED cross-wave fact
            // (the dep really did COMPLETE/ERROR — terminal-is-forever, D17), not a wave-transient
            // like `dep_dirty`/`dep_batch`. `deactivate`/`rewire_apply` refresh it on a fresh
            // lifecycle; a panic-aborted wave must leave a terminated dep marked terminal.
        });
    }

    // ── ctx.state + hooks (called from Ctx) ──

    pub(crate) fn get_state(&self) -> Option<AnyValue> {
        self.with_aux(|a| a.state.clone())
    }
    pub(crate) fn set_state(&self, v: AnyValue) {
        self.with_aux_mut(|a| a.state = Some(v));
    }
    pub(crate) fn set_state_persist(&self, on: bool) {
        self.with_aux_mut(|a| a.state_persist = on);
    }
    pub(crate) fn register_on_deactivation(&self, f: Box<dyn FnOnce()>) {
        self.with_aux_mut(|a| a.on_deactivation.push(f));
    }
    pub(crate) fn register_on_invalidate(&self, f: Rc<dyn Fn()>) {
        self.with_aux_mut(|a| a.on_invalidate.push(f));
    }

    #[cfg(test)]
    fn subscriber_count(&self) -> usize {
        self.with_aux(|a| a.subscribers.len())
    }
}

/// Reconstruct a payload-free upstream control message for forwarding toward deps
/// (R-up-at-source / D38). [`Msg`] is not `Clone` (the `Error` variant carries a
/// non-clonable `GraphError`), but the only kinds that reach the intermediate-forward
/// path are the unit control kinds DIRTY/INVALIDATE/TEARDOWN (per `is_up_allowed`, minus
/// PAUSE/RESUME which self-handle), so a fresh copy is always constructible.
fn dup_control(m: &Msg) -> Msg {
    match m {
        Message::Pause(lock) => Message::Pause(lock.clone()),
        Message::Resume(lock) => Message::Resume(lock.clone()),
        Message::Dirty => Message::Dirty,
        Message::Invalidate => Message::Invalidate,
        Message::Teardown => Message::Teardown,
        other => unreachable!("up forwards only control messages, got {other:?}"),
    }
}

/// Is `target` reachable upstream from `from` (following deps)? The rewire cycle-
/// prevention DFS (R-rewire / D42). Node identity = (graph arena, node id).
fn reachable_upstream(from: &Core, target: &Core) -> bool {
    let mut seen: Vec<Core> = Vec::new();
    let mut stack: Vec<Core> = vec![from.clone()];
    while let Some(n) = stack.pop() {
        if n.ptr_eq(target) {
            return true;
        }
        if seen.iter().any(|s| s.ptr_eq(&n)) {
            continue;
        }
        seen.push(n.clone());
        for d in n.borrow().deps.iter() {
            stack.push(d.clone());
        }
    }
    false
}

/// Dedup a dep list by node identity, preserving first-seen order — a dep
/// appearing twice in a rewire collapses to one (Option-C; matches the TS `_dedupDeps`).
fn dedup_cores(deps: Vec<Core>) -> Vec<Core> {
    let mut out: Vec<Core> = Vec::with_capacity(deps.len());
    for d in deps {
        if !out.iter().any(|o| o.ptr_eq(&d)) {
            out.push(d);
        }
    }
    out
}

/// Register a fn into the pool selected by `pool` (R-dispatch-all). The async pool's
/// invoke is still sync void (R-sync-core); the pool kind only labels the node's
/// deferred-emit / pause-buffering behavior.
fn register_with(disp: &Dispatcher, pool: PoolKind, f: NodeFn) -> Handle {
    match pool {
        PoolKind::Sync => disp.register(f),
        PoolKind::Async => disp.register_async(f),
    }
}

/// The typed facade over the erased substrate node (D5). Re-types the boundary:
/// `cache()`/`set()` downcast to `T`; deps are erased via [`Node::erased`].
///
/// No `PartialEq` bound (D49: the substrate does no value-equality — dedup is
/// opt-in at the operator layer, e.g. `distinctUntilChanged`).
pub struct Node<T> {
    core: Core,
    _t: PhantomData<T>,
}

impl<T: 'static> Node<T> {
    /// A state node pre-populated with `initial`; a new subscriber gets `[DATA]`
    /// (R-initial). Manual source — push new values with [`Node::set`].
    pub fn state(initial: T) -> Node<T> {
        Self::state_in_arena(&GraphArena::default(), initial)
    }

    pub(crate) fn state_in_arena(arena: &GraphArena, initial: T) -> Node<T> {
        Self::state_in_arena_with_dispatcher(arena, default_dispatcher(), initial)
    }

    pub(crate) fn state_in_arena_with_dispatcher(
        arena: &GraphArena,
        dispatcher: Dispatcher,
        initial: T,
    ) -> Node<T> {
        Node {
            core: Core::new_in_arena(
                arena,
                vec![],
                None,
                dispatcher,
                Some(Rc::new(initial)),
                Pausable::True,
            ),
            _t: PhantomData,
        }
    }

    /// A SENTINEL state node — no value until the first [`Node::set`].
    pub fn state_empty() -> Node<T> {
        Self::state_empty_in_arena(&GraphArena::default())
    }

    pub(crate) fn state_empty_in_arena(arena: &GraphArena) -> Node<T> {
        Self::state_empty_in_arena_with_dispatcher(arena, default_dispatcher())
    }

    pub(crate) fn state_empty_in_arena_with_dispatcher(
        arena: &GraphArena,
        dispatcher: Dispatcher,
    ) -> Node<T> {
        Node {
            core: Core::new_in_arena(arena, vec![], None, dispatcher, None, Pausable::True),
            _t: PhantomData,
        }
    }

    /// A producer node (fn, no deps): runs once on activation, emits via `ctx.emit`.
    pub fn producer<F: Fn(&Ctx) + 'static>(f: F) -> Node<T> {
        Self::producer_opts(NodeOpts::default(), f)
    }

    /// A producer on the LocalAsync pool (D20): the fn runs once on activation and may
    /// DEFER its emission — stash `ctx.defer()` and emit later via the [`DeferredCtx`]
    /// (the async source pattern, R-sync-core / R-no-raw-async). Default pause mode
    /// (`true`); a depless leaf source's own production is delivered immediately even
    /// while paused (R-pause-modes / C-10).
    ///
    /// [`DeferredCtx`]: crate::ctx::DeferredCtx
    pub fn producer_async<F: Fn(&Ctx) + 'static>(f: F) -> Node<T> {
        Self::producer_opts(
            NodeOpts {
                factory: None,
                pool: PoolKind::Async,
                pausable: Pausable::True,
                ..NodeOpts::default()
            },
            f,
        )
    }

    /// A producer with explicit [`NodeOpts`] (pool + pause mode).
    pub fn producer_opts<F: Fn(&Ctx) + 'static>(opts: NodeOpts, f: F) -> Node<T> {
        Self::producer_opts_in_arena(&GraphArena::default(), opts, f)
    }

    pub(crate) fn producer_opts_in_arena<F: Fn(&Ctx) + 'static>(
        arena: &GraphArena,
        opts: NodeOpts,
        f: F,
    ) -> Node<T> {
        Self::producer_opts_in_arena_with_dispatcher(arena, default_dispatcher(), opts, f)
    }

    pub(crate) fn producer_opts_in_arena_with_dispatcher<F: Fn(&Ctx) + 'static>(
        arena: &GraphArena,
        dispatcher: Dispatcher,
        opts: NodeOpts,
        f: F,
    ) -> Node<T> {
        assert!(
            !(opts.pull_id.is_some() && matches!(opts.pausable, Pausable::False)),
            "pullId + pausable:false is invalid (R-pull / R-pause-modes)"
        );
        let factory = opts.factory.clone();
        let handle = register_with(&dispatcher, opts.pool, Rc::new(f));
        let node = Node {
            core: Core::new_in_arena(arena, vec![], Some(handle), dispatcher, None, opts.pausable),
            _t: PhantomData,
        };
        node.core.borrow_mut().factory = factory;
        node.core.configure_pull(opts.pull_id);
        node
    }

    /// A derived node over erased deps. The fn reads deps positionally via
    /// `ctx.data::<U>(i)` and emits via `ctx.emit`. The first-run gate holds the fn
    /// until every dep has settled (R-first-run-gate). A fn that returns WITHOUT
    /// emitting (filter-reject) makes the substrate synthesize an undirty RESOLVED
    /// (D49 / R-resolved-undirty).
    pub fn derived<F: Fn(&Ctx) + 'static>(deps: Vec<Core>, f: F) -> Node<T> {
        Self::derived_opts(deps, NodeOpts::default(), f)
    }

    /// A derived node on the LocalAsync pool (D20): the fn may DEFER its emission (stash
    /// `ctx.defer()`, emit later). An async COMPUTE node (deps>0) that returns without
    /// emitting has DEFERRED (not rejected) — no undirty RESOLVED is synthesized, and its
    /// in-flight result buffers if the node is paused (R-async-paused / C-2/C-4). Default
    /// pause mode (`true`).
    pub fn derived_async<F: Fn(&Ctx) + 'static>(deps: Vec<Core>, f: F) -> Node<T> {
        Self::derived_opts(
            deps,
            NodeOpts {
                factory: None,
                pool: PoolKind::Async,
                pausable: Pausable::True,
                ..NodeOpts::default()
            },
            f,
        )
    }

    /// A derived node with explicit [`NodeOpts`] (pool + pause mode + dep-terminal policy).
    pub fn derived_opts<F: Fn(&Ctx) + 'static>(deps: Vec<Core>, opts: NodeOpts, f: F) -> Node<T> {
        Self::derived_opts_in_arena(&GraphArena::default(), deps, opts, f)
    }

    pub(crate) fn derived_opts_in_arena<F: Fn(&Ctx) + 'static>(
        arena: &GraphArena,
        deps: Vec<Core>,
        opts: NodeOpts,
        f: F,
    ) -> Node<T> {
        Self::derived_opts_in_arena_with_dispatcher(arena, default_dispatcher(), deps, opts, f)
    }

    pub(crate) fn derived_opts_in_arena_with_dispatcher<F: Fn(&Ctx) + 'static>(
        arena: &GraphArena,
        dispatcher: Dispatcher,
        deps: Vec<Core>,
        opts: NodeOpts,
        f: F,
    ) -> Node<T> {
        assert!(
            !(opts.pull_id.is_some() && matches!(opts.pausable, Pausable::False)),
            "pullId + pausable:false is invalid (R-pull / R-pause-modes)"
        );
        let factory = opts.factory.clone();
        let handle = register_with(&dispatcher, opts.pool, Rc::new(f));
        let node = Node {
            core: Core::new_in_arena(arena, deps, Some(handle), dispatcher, None, opts.pausable),
            _t: PhantomData,
        };
        {
            // Thread the dep-terminal propagation policy (R-deps-terminal) into the inner —
            // `Core::new` defaults to the plain derived behavior (auto-cascade, not an input).
            let mut n = node.core.borrow_mut();
            n.partial = opts.partial;
            n.complete_when_deps_complete = opts.complete_when_deps_complete;
            n.error_when_deps_error = opts.error_when_deps_error;
            n.terminal_as_real_input = opts.terminal_as_real_input;
            n.factory = factory;
        }
        node.core.configure_pull(opts.pull_id);
        node
    }

    /// Push a new value from a state node (one DATA wave). Always emits DATA — the
    /// substrate never absorbs to RESOLVED on value-equality (D49).
    pub fn set(&self, v: T) {
        self.down(vec![Message::Data(Rc::new(v))]);
    }

    /// Emit a raw wave downstream (R-node-iface). The wave-owner boundary for the D30
    /// catch (a panicking fn the cascade triggers becomes `[[ERROR,e]]`, not an escape).
    pub fn down(&self, msgs: Wave<AnyValue>) {
        let core = self.core.clone();
        with_wave_owner(&self.core, move || core.down(msgs), || {});
    }

    /// Emit a raw control wave upstream — control tiers only (R-ctx-up / R-node-iface).
    /// PAUSE/RESUME act on this node's lockset (R-pause-lockset).
    pub fn up(&self, msgs: Wave<AnyValue>) {
        let core = self.core.clone();
        with_wave_owner(&self.core, move || core.up(msgs, None), || {});
    }

    /// Directed upstream control along one declared dep edge (R-up-routing).
    pub fn up_toward(&self, toward_dep: usize, msgs: Wave<AnyValue>) {
        let core = self.core.clone();
        with_wave_owner(&self.core, move || core.up(msgs, Some(toward_dep)), || {});
    }

    /// The current cached value (downcast), or `None` for SENTINEL.
    pub fn cache(&self) -> Option<T>
    where
        T: Clone,
    {
        self.core
            .with_inner_edges(|_n, e| e.value.cache.clone())
            .as_ref()
            .and_then(|a| a.downcast_ref::<T>().cloned())
    }

    /// The node's lifecycle status (R-status-enum).
    pub fn status(&self) -> Status {
        self.core.with_inner_edges(|_n, e| e.value.status)
    }

    pub fn pull_id(&self) -> Option<LockId> {
        self.core.borrow().pull_id.clone()
    }

    /// Subscribe a sink; returns an unsubscribe handle (call it to detach; the node
    /// deactivates when the last subscriber leaves, R-rom-ram).
    ///
    /// This is also a wave-owner boundary: the activation cascade it drives runs under
    /// the D30 catch, so a synchronous feedback cycle surfaces as `[[ERROR,e]]` on a
    /// cycle node instead of escaping the subscribe call (C-6). Even on that error path
    /// the returned handle is a REAL unsubscribe (the sink is registered before the
    /// panicking activation, so the caller can always detach it).
    ///
    /// Panics (R-terminal / D17): subscribing to a non-resubscribable **terminal** node
    /// is rejected — the stream is permanently over. (Resubscribable opt-in reset is a
    /// later slice.)
    pub fn subscribe(&self, sink: impl Fn(&Msg) + 'static) -> Unsub {
        assert!(
            !self.core.with_inner_edges(|_n, e| e.value.terminal),
            "subscribe: node is terminal and non-resubscribable — the stream is permanently over (R-terminal / D17)"
        );
        let body_core = self.core.clone();
        let err_core = self.core.clone();
        let sink = Rc::new(sink);
        // Record the subscriber id before activation so the error path hands back a real
        // unsubscribe instead of leaking the just-registered sink (no orphaned sink).
        let id_cell: Rc<Cell<Option<u64>>> = Rc::new(Cell::new(None));
        let body_cell = id_cell.clone();
        with_wave_owner(
            &self.core,
            move || body_core.subscribe_recording_id(sink, &body_cell),
            move || match id_cell.get() {
                Some(id) => Box::new(move || err_core.unsubscribe(id)) as Unsub,
                None => Box::new(|| {}) as Unsub,
            },
        )
    }

    /// R-rewire (D42): replace this node's deps atomically (surgical Option-C). Kept deps
    /// keep their subscription + per-dep state; only removed deps unsubscribe + drain, only
    /// added deps fresh-subscribe (push-on-subscribe for an added cached dep). The first-run
    /// gate + cache are PRESERVED. Requires an explicit fn (SD-1 fn-deps pairing — user fns
    /// read deps positionally). INTRA-graph only (D22). Deps are erased ([`Core`]).
    ///
    /// Rejects (R-rewire): self-dep, a cycle, a terminal `self`, a (non-resubscribable)
    /// terminal added dep, a reentrant rewire — these panic (propagate to the caller). A
    /// mid-fn rewire (during this node's own fn run) is the D37 feedback cycle → caught by
    /// the running wave-owner as `[[ERROR,e]]`, not a propagated panic.
    pub fn set_deps<F: Fn(&Ctx) + 'static>(&self, new_deps: Vec<Core>, f: F) {
        self.core.rewire(new_deps, Rc::new(f));
    }

    /// Add one dep (special case of [`Node::set_deps`]); returns its index. fn required (SD-1).
    pub fn add_dep<F: Fn(&Ctx) + 'static>(&self, dep: Core, f: F) -> usize {
        let mut next = self.core.borrow().deps.clone();
        if !next.iter().any(|d| d.ptr_eq(&dep)) {
            next.push(dep.clone());
        }
        self.core.rewire(next, Rc::new(f));
        self.core
            .borrow()
            .deps
            .iter()
            .position(|d| d.ptr_eq(&dep))
            .expect("added dep present after rewire")
    }

    /// Remove one dep (special case of [`Node::set_deps`]); idempotent if absent (the fn
    /// swap still applies). fn required (SD-1).
    pub fn remove_dep<F: Fn(&Ctx) + 'static>(&self, dep: Core, f: F) {
        let next: Vec<Core> = self
            .core
            .borrow()
            .deps
            .iter()
            .filter(|d| !d.ptr_eq(&dep))
            .cloned()
            .collect();
        self.core.rewire(next, Rc::new(f));
    }

    /// The erased core, for wiring this node as a dep of a `derived` node.
    pub fn erased(&self) -> Core {
        self.core.clone()
    }

    pub(crate) fn from_core(core: Core) -> Node<T> {
        Node {
            core,
            _t: PhantomData,
        }
    }
}

/// A cheap handle clone — both `Node`s address the SAME underlying node (the shared
/// `Rc<RefCell<…>>`), like cloning a [`Core`]. Manual (not `#[derive]`) so it does
/// NOT require `T: Clone` — the value type need not be cloneable for the handle to be.
impl<T> Clone for Node<T> {
    fn clone(&self) -> Self {
        Node {
            core: self.core.clone(),
            _t: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    /// Collect a subscriber's message-kind tags into a shared Vec for shape asserts.
    fn recorder() -> (Rc<RefCell<Vec<String>>>, impl Fn(&Msg) + 'static) {
        let log = Rc::new(RefCell::new(Vec::new()));
        let l2 = log.clone();
        (log, move |m: &Msg| l2.borrow_mut().push(format!("{m:?}")))
    }

    #[test]
    fn status_freshness_and_terminality() {
        assert!(Status::Settled.is_fresh());
        assert!(Status::Resolved.is_fresh());
        assert!(!Status::Dirty.is_fresh());
        assert!(Status::Completed.is_terminal());
        assert!(!Status::Settled.is_terminal());
    }

    #[test]
    fn state_node_push_on_subscribe_and_set() {
        let s = Node::<i32>::state(1);
        let (log, sink) = recorder();
        let _u = s.subscribe(sink);
        // push-on-subscribe: START then cached DATA (R-push-subscribe).
        assert_eq!(*log.borrow(), vec!["START", "DATA"]);
        assert_eq!(s.cache(), Some(1));

        s.set(2);
        // external tier-3 emit synthesizes a leading DIRTY (R-dirty-before-data).
        assert_eq!(*log.borrow(), vec!["START", "DATA", "DIRTY", "DATA"]);
        assert_eq!(s.cache(), Some(2));
        assert_eq!(s.status(), Status::Settled);
    }

    #[test]
    fn batch_commits_last_settle_and_rollback_balances_dirty() {
        let s = Node::<i32>::state(0);
        let (log, sink) = recorder();
        let _u = s.subscribe(sink);
        log.borrow_mut().clear();

        crate::batch::batch(|_| {
            s.set(1);
            s.set(2);
            assert_eq!(*log.borrow(), vec!["DIRTY"]);
            assert_eq!(s.cache(), Some(0));
        });
        assert_eq!(*log.borrow(), vec!["DIRTY", "DATA"]);
        assert_eq!(s.cache(), Some(2));

        log.borrow_mut().clear();
        crate::batch::batch(|bctx| {
            s.set(3);
            bctx.rollback();
            assert_eq!(*log.borrow(), vec!["DIRTY"]);
        });
        assert_eq!(*log.borrow(), vec!["DIRTY", "RESOLVED"]);
        assert_eq!(s.cache(), Some(2));
    }

    #[test]
    fn batch_rollback_resolved_respects_resumeall_pause() {
        let n = Node::<i32>::producer_opts(
            NodeOpts {
                pausable: Pausable::ResumeAll,
                ..NodeOpts::default()
            },
            |_| {},
        );
        let (log, sink) = recorder();
        let _u = n.subscribe(sink);
        log.borrow_mut().clear();

        let pause = LockId::new("rollback");
        n.up(vec![Message::Pause(pause.clone())]);
        crate::batch::batch(|bctx| {
            n.down(vec![Message::Data(Rc::new(1i32))]);
            bctx.rollback();
            assert_eq!(*log.borrow(), vec!["DIRTY"]);
        });

        assert_eq!(*log.borrow(), vec!["DIRTY"]);
        assert_eq!(n.cache(), None);
        n.up(vec![Message::Resume(pause)]);
        assert_eq!(*log.borrow(), vec!["DIRTY", "RESOLVED"]);
        assert_eq!(n.status(), Status::Sentinel);
    }

    #[test]
    fn d49_repeated_equal_value_stays_data() {
        // D49 (supersedes D15): every occurrence is DATA — NO equals-substitution.
        // Probe: fromIter([1,1,1]) must be [DATA,DATA,DATA] so take/scan/count work.
        let s = Node::<i32>::state(5);
        let (log, sink) = recorder();
        let _u = s.subscribe(sink);
        s.set(5); // unchanged value → still DATA (never absorbed to RESOLVED)
        assert_eq!(*log.borrow(), vec!["START", "DATA", "DIRTY", "DATA"]);
        assert_eq!(s.status(), Status::Settled);
        s.set(5); // again → still DATA
        assert_eq!(
            *log.borrow(),
            vec!["START", "DATA", "DIRTY", "DATA", "DIRTY", "DATA"]
        );
        assert_eq!(s.status(), Status::Settled);
    }

    #[test]
    fn d49_filter_no_emit_synthesizes_undirty_resolved() {
        // R-resolved-undirty (D49): a derived fn DIRTY'd in phase 1 that returns
        // WITHOUT emitting (filter-reject) → the substrate synthesizes exactly one
        // RESOLVED to clear the downstream dirty (no wedge).
        let a = Node::<i32>::state_empty();
        let evens: Node<i32> = Node::derived(vec![a.erased()], |ctx| {
            let v = *ctx.data::<i32>(0).unwrap();
            if v % 2 == 0 {
                ctx.emit(v);
            } // odd → emit nothing (filter-reject)
        });
        let (log, sink) = recorder();
        let _u = evens.subscribe(sink);
        assert_eq!(*log.borrow(), vec!["START"]); // first-run gate holds (a SENTINEL)

        a.set(3); // odd → fn runs, no emit → substrate undirties with one RESOLVED
        assert_eq!(*log.borrow(), vec!["START", "DIRTY", "RESOLVED"]);
        assert_eq!(evens.cache(), None); // filtered → never valued
        assert_eq!(evens.status(), Status::Sentinel);

        a.set(4); // even → real DATA (downstream was NOT wedged by the prior RESOLVED)
        assert_eq!(
            *log.borrow(),
            vec!["START", "DIRTY", "RESOLVED", "DIRTY", "DATA"]
        );
        assert_eq!(evens.cache(), Some(4));
        assert_eq!(evens.status(), Status::Settled);

        a.set(5); // odd → filter; node KEEPS its value 4, status → Resolved (no new occurrence)
        assert_eq!(
            *log.borrow(),
            vec!["START", "DIRTY", "RESOLVED", "DIRTY", "DATA", "DIRTY", "RESOLVED"]
        );
        assert_eq!(evens.cache(), Some(4));
        assert_eq!(evens.status(), Status::Resolved);
    }

    #[test]
    fn d1_drop_releases_upstream_subscription_and_runs_cleanup() {
        // D1: dropping an active node (no explicit unsubscribe) must detach from its
        // deps and fire its cleanup hooks — Rust has no GC to reap the dead sink.
        let deactivated = Rc::new(Cell::new(false));
        let a = Node::<i32>::state(1);
        {
            let flag = deactivated.clone();
            let mid: Node<i32> = Node::derived(vec![a.erased()], move |ctx| {
                let f = flag.clone();
                ctx.on_deactivation(move || f.set(true));
                ctx.emit(*ctx.data::<i32>(0).unwrap() + 1);
            });
            let _sub = mid.subscribe(|_| {}); // activates mid: runs fn (registers hook) + subscribes to a
            assert!(!deactivated.get());
            // `mid` + `_sub` drop here → mid's Core refcount hits 0 → Drop runs cleanup.
        }
        assert!(
            deactivated.get(),
            "dropping an active node fires its on_deactivation + detaches from deps (D1)"
        );
        // `a` survived (still held), its sink was removed; a fresh subscribe still works.
        let (log, sink) = recorder();
        let _u = a.subscribe(sink);
        assert_eq!(*log.borrow(), vec!["START", "DATA"]);
    }

    #[test]
    fn producer_runs_once_on_activation() {
        let p = Node::<i32>::producer(|ctx| ctx.emit(42i32));
        let (log, sink) = recorder();
        let _u = p.subscribe(sink);
        // activation-exempt: the first run during subscribe needs no preceding DIRTY.
        assert_eq!(*log.borrow(), vec!["START", "DATA"]);
        assert_eq!(p.cache(), Some(42));
    }

    #[test]
    fn derived_first_run_gate_then_recompute() {
        let a = Node::<i32>::state_empty();
        let b = Node::<i32>::state_empty();
        let runs = Rc::new(Cell::new(0usize));
        let r2 = runs.clone();
        let sum: Node<i32> = Node::derived(vec![a.erased(), b.erased()], move |ctx| {
            r2.set(r2.get() + 1);
            let x = *ctx.data::<i32>(0).unwrap();
            let y = *ctx.data::<i32>(1).unwrap();
            ctx.emit(x + y);
        });
        let (log, sink) = recorder();
        let _u = sum.subscribe(sink);
        // first-run gate: neither dep has settled → fn has not fired.
        assert_eq!(runs.get(), 0);
        assert_eq!(*log.borrow(), vec!["START"]);

        a.set(3); // only one dep settled → gate still holds
        assert_eq!(runs.get(), 0);

        b.set(4); // both settled → first run, sum = 7
        assert_eq!(runs.get(), 1);
        assert_eq!(sum.cache(), Some(7));
        assert_eq!(*log.borrow(), vec!["START", "DIRTY", "DATA"]);

        a.set(10); // recompute (gate already passed), sum = 14
        assert_eq!(runs.get(), 2);
        assert_eq!(sum.cache(), Some(14));
    }

    #[test]
    fn diamond_recomputes_exactly_once_two_phase() {
        // A → B, A → C, B → D, C → D. D must join once per A update, glitch-free.
        let a = Node::<i32>::state_empty();
        let b: Node<i32> = Node::derived(vec![a.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap() + 1)
        });
        let c: Node<i32> = Node::derived(vec![a.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap() * 10)
        });
        let d_runs = Rc::new(Cell::new(0usize));
        let dr = d_runs.clone();
        let d: Node<i32> = Node::derived(vec![b.erased(), c.erased()], move |ctx| {
            dr.set(dr.get() + 1);
            ctx.emit(*ctx.data::<i32>(0).unwrap() + *ctx.data::<i32>(1).unwrap());
        });
        let (log, sink) = recorder();
        let _u = d.subscribe(sink);

        a.set(2); // B=3, C=20, D=23 — D fires EXACTLY once (R-diamond)
        assert_eq!(d_runs.get(), 1);
        assert_eq!(d.cache(), Some(23));
        // two-phase: DIRTY (phase 1, from the cascade) precedes DATA (phase 2). No glitch.
        assert_eq!(*log.borrow(), vec!["START", "DIRTY", "DATA"]);

        a.set(5); // B=6, C=50, D=56 — once more
        assert_eq!(d_runs.get(), 2);
        assert_eq!(d.cache(), Some(56));
    }

    #[test]
    fn rom_ram_deactivation_clears_compute_cache() {
        let a = Node::<i32>::state(7); // ROM: state node retains across disconnect
        let dbl: Node<i32> = Node::derived(vec![a.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap() * 2)
        });
        let (_log, sink) = recorder();
        let u = dbl.subscribe(sink);
        assert_eq!(dbl.cache(), Some(14));
        assert_eq!(dbl.status(), Status::Settled);

        u(); // last subscriber leaves → dbl deactivates
             // RAM: compute node cleared its cache; ROM: state node kept its value.
        assert_eq!(dbl.cache(), None);
        assert_eq!(dbl.status(), Status::Sentinel);
        assert_eq!(a.cache(), Some(7));
    }

    #[test]
    #[should_panic(expected = "terminal")]
    fn subscribe_to_terminal_node_is_rejected() {
        // R-terminal (D17): a late subscribe to a non-resubscribable terminal node is
        // REJECTED (idiomatic error) — the stream is permanently over.
        let s = Node::<i32>::state(1);
        let _u = s.subscribe(|_| {}); // keep alive (so the node doesn't deactivate)
        s.down(vec![Message::Complete]); // S goes terminal
        assert_eq!(s.status(), Status::Completed);
        let _u2 = s.subscribe(|_| {}); // late subscribe to a terminal node → panics
    }

    #[test]
    fn set_on_terminal_node_is_a_noop() {
        // Terminal-is-forever (D17 / R-terminal): a self-emit (set/ctx.down) on a
        // terminated node emits nothing and does not resurrect its value.
        let s = Node::<i32>::state(1);
        let (log, sink) = recorder();
        let _u = s.subscribe(sink);
        assert_eq!(*log.borrow(), vec!["START", "DATA"]);

        s.down(vec![Message::Complete]); // S terminal
        assert_eq!(s.status(), Status::Completed);
        assert_eq!(*log.borrow(), vec!["START", "DATA", "COMPLETE"]);

        s.set(5); // self-emit after terminal → no-op (no DATA, value unchanged)
        assert_eq!(*log.borrow(), vec!["START", "DATA", "COMPLETE"]);
        assert_eq!(s.cache(), Some(1));
        assert_eq!(s.status(), Status::Completed);
    }

    #[test]
    fn b32_dropped_node_unregisters_its_fn_releasing_captures() {
        // B32: the dispatcher pool no longer holds a dropped node's fn forever. Before the
        // slotmap + unregister-on-Drop, a registered fn lived for the process, pinning the
        // upstream `Core`s it captured (the no-GC leak). Probe via a guard captured by the
        // fn: when the node drops, its fn slot is freed → the fn (and the guard) drop.
        struct DropFlag(Rc<Cell<bool>>);
        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }
        let fn_dropped = Rc::new(Cell::new(false));
        {
            let guard = DropFlag(fn_dropped.clone());
            let p = Node::<i32>::producer(move |ctx| {
                let _hold = &guard; // the fn captures the guard (stands in for a captured Core)
                ctx.emit(1i32);
            });
            let _u = p.subscribe(|_| {});
            assert!(
                !fn_dropped.get(),
                "the fn (and its capture) is live while the node is"
            );
            // `p` + `_u` drop here → p's Core refcount → 0 → NodeInner::Drop → dispatcher
            // .unregister → the pool drops the fn → the captured guard drops.
        }
        assert!(
            fn_dropped.get(),
            "a dropped node unregisters its fn from the pool, releasing the fn's captures (B32)"
        );
    }

    #[test]
    fn run_wave_hook_clear_drops_captured_core_borrow_free() {
        // B49/D71 regression: clearing per-run hooks must drop the old user closures
        // AFTER releasing the GraphCore borrow. A hook may capture the last handle to
        // another node; dropping that handle re-enters Core::drop on the same arena.
        let a = Node::<i32>::state(1);
        let d: Node<i32> = Node::derived(vec![a.erased()], |ctx| {
            let held = Node::<i32>::state(99);
            ctx.on_invalidate(move || {
                let _ = held.status();
            });
            ctx.emit(*ctx.data::<i32>(0).unwrap());
        });
        let _u = d.subscribe(|_| {});

        a.set(2);
        assert_eq!(d.cache(), Some(2));
    }

    #[test]
    fn drop_returns_arena_slot_when_cleanup_hook_panics() {
        // B49/D71 regression: Core::drop removes the NodeInner from the arena before
        // running cleanup. Even if a user cleanup hook panics, the slot id must be
        // returned to the free list.
        let free_before = DEFAULT_GRAPH_CORE.with(|g| g.borrow().free.len());
        let result = catch_unwind(AssertUnwindSafe(|| {
            let p = Node::<i32>::producer(|ctx| {
                ctx.on_deactivation(|| panic!("cleanup boom"));
                ctx.emit(1i32);
            });
            let _u = p.subscribe(|_| {});
        }));
        assert!(result.is_err());
        let free_after = DEFAULT_GRAPH_CORE.with(|g| g.borrow().free.len());
        assert!(
            free_after > free_before,
            "panicking cleanup still returns the arena slot to the free list"
        );
    }

    #[test]
    fn identity_key_changes_when_arena_slot_is_reused() {
        let first = Node::<i32>::producer_opts(
            NodeOpts {
                factory: Some("first".to_owned()),
                ..NodeOpts::default()
            },
            |_| {},
        );
        let first_key = first.erased().identity_key();
        drop(first);

        let second = Node::<i32>::producer_opts(
            NodeOpts {
                factory: Some("second".to_owned()),
                ..NodeOpts::default()
            },
            |_| {},
        );
        let second_key = second.erased().identity_key();

        assert_ne!(
            first_key, second_key,
            "D51 synthetic describe ids must not inherit across reused arena slots"
        );
    }

    #[test]
    fn arena_generation_key_rejects_reused_slot() {
        // DR-8/B54 owner-execution prep: arena entries are addressed by slot +
        // generation, so a stale key cannot hit a later node that reused the slot.
        let arena = GraphArena::new();
        let first = Node::<i32>::state_in_arena(&arena, 1);
        let old_key = first.core.key();
        let old_weak = first.core.downgrade();
        assert!(old_weak.borrowed_core().is_some());
        drop(first);

        let second = Node::<i32>::state_in_arena(&arena, 2);
        let new_key = second.core.key();
        assert_eq!(
            old_key.id, new_key.id,
            "isolated arena should reuse the just-freed slot"
        );
        assert_ne!(old_key.generation, new_key.generation);
        assert!(
            !arena.0.borrow().is_live_key(old_key),
            "the old generation key is stale after slot reuse"
        );

        // A stale weak dep-callback entry is a no-op and must not disturb the new node.
        old_weak.receive_from_dep(0, &Message::Start);
        assert_eq!(second.cache(), Some(2));
        assert_eq!(second.status(), Status::Settled);
    }

    #[test]
    fn internal_dep_callback_entry_does_not_increment_core_refcount() {
        // The B54 first slice keeps closure transport but avoids constructing a counted
        // Core for every internal dep message. START is enough to exercise the entry
        // without mutating dep-slot arrays. This path is only uncounted while a real
        // wave owner is active; outside a wave, the callback entry must pin the slot.
        let arena = GraphArena::new();
        let source = Node::<i32>::state_in_arena(&arena, 1);
        let derived: Node<i32> = Node::derived_opts_in_arena(
            &arena,
            vec![source.erased()],
            NodeOpts::default(),
            |ctx| ctx.emit(*ctx.data::<i32>(0).unwrap()),
        );
        let weak = derived.core.downgrade();
        let refs_before = derived.core.refs.get();

        let borrowed = with_wave_owner(&source.core, || weak.borrowed_core(), || None)
            .expect("borrowed core should exist while the node is live");
        assert_eq!(
            derived.core.refs.get(),
            refs_before,
            "borrowed weak dep entry should not increment Core refs while held"
        );
        drop(borrowed);

        assert_eq!(
            derived.core.refs.get(),
            refs_before,
            "internal weak dep entry should borrow by arena key, not create a counted Core"
        );
    }

    #[test]
    fn internal_dep_callback_entry_pins_core_outside_wave() {
        // Recovery ERROR delivery runs outside the original wave scope. In that path a
        // weak dep callback must hold a counted Core so subscriber callbacks cannot drop
        // the final public handle and free the slot mid-delivery.
        let arena = GraphArena::new();
        let source = Node::<i32>::state_in_arena(&arena, 1);
        let derived: Node<i32> = Node::derived_opts_in_arena(
            &arena,
            vec![source.erased()],
            NodeOpts::default(),
            |ctx| ctx.emit(*ctx.data::<i32>(0).unwrap()),
        );
        let weak = derived.core.downgrade();
        let refs_before = derived.core.refs.get();

        let counted = weak
            .counted_core()
            .expect("counted core should exist while the node is live");
        assert_eq!(
            derived.core.refs.get(),
            refs_before + 1,
            "outside a wave, weak dep entry pins the node with a counted Core"
        );
        drop(counted);
        assert_eq!(derived.core.refs.get(), refs_before);
    }

    #[test]
    #[should_panic(expected = "GraphCore generation overflow")]
    fn arena_generation_overflow_fails_closed() {
        // A stale NodeKey must never become live again via generation wraparound.
        let arena = GraphArena::new();
        let first = Node::<i32>::state_in_arena(&arena, 1);
        let slot = first.core.id.0;
        drop(first);
        arena.0.borrow_mut().generations[slot] = u64::MAX;

        let _second = Node::<i32>::state_in_arena(&arena, 2);
    }

    #[test]
    fn resumeall_buffers_each_recompute_and_replays_all_on_resume() {
        // R-pause-modes resumeAll (D44): unlike `true` (coalesce → fire ONCE on resume),
        // resumeAll RUNS the fn on each dep wave while paused and BUFFERS the output,
        // replaying every buffered settle in arrival order on final-lock RESUME. No
        // conformance scenario covers resumeAll (TS is also only B9-partial — the pre-pause
        // baseline-equals fidelity is deferred); this pins the buffer+replay property.
        let runs = Rc::new(Cell::new(0usize));
        let src = Node::<i32>::state(1);
        let doubled = {
            let r = runs.clone();
            Node::<i32>::derived_opts(
                vec![src.erased()],
                NodeOpts {
                    pool: PoolKind::Sync,
                    pausable: Pausable::ResumeAll,
                    ..NodeOpts::default()
                },
                move |ctx| {
                    r.set(r.get() + 1);
                    ctx.emit(*ctx.data::<i32>(0).unwrap() * 2);
                },
            )
        };
        let (log, sink) = recorder();
        let _u = doubled.subscribe(sink); // activation (not paused): 1*2 = 2, delivered
        assert_eq!(doubled.cache(), Some(2));
        runs.set(0);

        let l = LockId::new("p");
        doubled.up(vec![Message::Pause(l.clone())]);
        log.borrow_mut().clear();

        src.set(5); // recompute (10) while paused → fn RUNS, output buffered
        src.set(6); // recompute (12) → buffered
        src.set(7); // recompute (14) → buffered
        let data_while_paused = log.borrow().iter().filter(|k| *k == "DATA").count();
        assert_eq!(
            runs.get(),
            3,
            "resumeAll runs the fn on each dep wave (NOT coalesced to one like `true`)"
        );
        assert_eq!(
            data_while_paused, 0,
            "no DATA delivered while paused (output buffered)"
        );
        assert_eq!(doubled.cache(), Some(2), "cache not advanced while paused");

        doubled.up(vec![Message::Resume(l)]); // replay all three buffered settles in order
        let data_after_resume = log.borrow().iter().filter(|k| *k == "DATA").count();
        assert_eq!(
            data_after_resume, 3,
            "all three buffered settles replay on final-lock RESUME (arrival order)"
        );
        assert_eq!(doubled.cache(), Some(14)); // last replayed value (7 * 2)
    }

    // ── rewire (R-rewire / D42, C-8) ──

    #[test]
    fn rewire_adddep_sentinel_dep_does_not_rearm_gate() {
        // Q2: adding a never-emitted (SENTINEL) dep delivers START only — no recompute — and
        // does NOT re-arm the first-run gate (a alone re-drives d, not waiting for b).
        let a = Node::<i32>::state(1);
        let b = Node::<i32>::state_empty(); // SENTINEL, never emits
        let runs = Rc::new(Cell::new(0usize));
        let d = {
            let r = runs.clone();
            Node::<i32>::derived(vec![a.erased()], move |ctx| {
                r.set(r.get() + 1);
                ctx.emit(*ctx.data::<i32>(0).unwrap() * 10);
            })
        };
        let (_log, sink) = recorder();
        let _u = d.subscribe(sink);
        assert_eq!(d.cache(), Some(10));
        runs.set(0);

        let r2 = runs.clone();
        d.add_dep(b.erased(), move |ctx| {
            r2.set(r2.get() + 1);
            let bv = ctx.data::<i32>(1).map(|v| *v).unwrap_or(0); // SENTINEL guard
            ctx.emit(*ctx.data::<i32>(0).unwrap() * 10 + bv);
        });
        assert_eq!(
            runs.get(),
            0,
            "a SENTINEL added dep delivers START only — no recompute"
        );

        a.set(2); // gate NOT re-armed: a alone re-drives d
        assert_eq!(runs.get(), 1);
        assert_eq!(d.cache(), Some(20)); // 2*10 + 0 (b still SENTINEL)
    }

    #[test]
    fn rewire_removedep_drains_and_preserves_cache() {
        // Q3/Q7: removeDep of a non-dirty dep preserves cache (no recompute) + drains the
        // removed edge (it no longer drives the node); the swapped fn runs on the next wave.
        let a = Node::<i32>::state(1);
        let b = Node::<i32>::state(2);
        let d: Node<i32> = Node::derived(vec![a.erased(), b.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap() + *ctx.data::<i32>(1).unwrap())
        });
        let (_log, sink) = recorder();
        let _u = d.subscribe(sink);
        assert_eq!(d.cache(), Some(3));

        d.remove_dep(b.erased(), |ctx| ctx.emit(*ctx.data::<i32>(0).unwrap()));
        assert_eq!(d.cache(), Some(3)); // preserved (removeDep of a non-dirty dep: no recompute)

        b.set(99); // drained — must NOT drive d
        assert_eq!(d.cache(), Some(3));

        a.set(5); // d recomputes with the swapped a-only fn
        assert_eq!(d.cache(), Some(5));
    }

    #[test]
    fn rewire_removedep_to_zero_deps_is_inert() {
        // SD-3: removeDep to zero deps → degenerate fn-no-deps, cache preserved, no auto-fire.
        let a = Node::<i32>::state(7);
        let runs = Rc::new(Cell::new(0usize));
        let d = {
            let r = runs.clone();
            Node::<i32>::derived(vec![a.erased()], move |ctx| {
                r.set(r.get() + 1);
                ctx.emit(*ctx.data::<i32>(0).unwrap());
            })
        };
        let (_log, sink) = recorder();
        let _u = d.subscribe(sink);
        assert_eq!(d.cache(), Some(7));
        runs.set(0);

        let r2 = runs.clone();
        d.remove_dep(a.erased(), move |ctx| {
            r2.set(r2.get() + 1);
            ctx.emit(-1i32);
        });
        assert_eq!(d.cache(), Some(7)); // preserved
        assert_eq!(runs.get(), 0); // inert — does not fire

        a.set(8); // a is no longer a dep
        assert_eq!(d.cache(), Some(7));
        assert_eq!(runs.get(), 0);
    }

    #[test]
    fn rewire_setdeps_reorders_kept_deps() {
        // Option-C / DepRecord-ref dispatch: reorder kept deps without losing state; a kept
        // dep's callback reroutes to its new index via the shared idx-box (O(1)).
        fn combine(ctx: &Ctx) {
            ctx.emit(*ctx.data::<i32>(0).unwrap() * 100 + *ctx.data::<i32>(1).unwrap());
        }
        let a = Node::<i32>::state(10);
        let b = Node::<i32>::state(20);
        let d: Node<i32> = Node::derived(vec![a.erased(), b.erased()], combine);
        let (_log, sink) = recorder();
        let _u = d.subscribe(sink);
        assert_eq!(d.cache(), Some(1020)); // dep0=a=10, dep1=b=20

        d.set_deps(vec![b.erased(), a.erased()], combine); // reorder; kept state preserved
        a.set(11); // a is now dep1; reroutes correctly
        assert_eq!(d.cache(), Some(2011)); // dep0=b=20, dep1=a=11 → 20*100+11
    }

    #[test]
    fn rewire_atomic_multi_add_settles_once() {
        // P2: adding ≥2 cached deps in ONE setDeps settles ATOMICALLY — the fn fires once,
        // never on a partial view (an added dep still SENTINEL at invocation = the bug).
        let a = Node::<i32>::state(1);
        let b = Node::<i32>::state(10);
        let c = Node::<i32>::state(100);
        let runs = Rc::new(Cell::new(0usize));
        let saw_partial = Rc::new(Cell::new(false));
        let d: Node<i32> = Node::derived(vec![a.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap())
        });
        let (_log, sink) = recorder();
        let _u = d.subscribe(sink);
        runs.set(0);

        let r = runs.clone();
        let sp = saw_partial.clone();
        d.set_deps(vec![a.erased(), b.erased(), c.erased()], move |ctx| {
            r.set(r.get() + 1);
            if ctx.data::<i32>(1).is_none() || ctx.data::<i32>(2).is_none() {
                sp.set(true);
            }
            ctx.emit(
                *ctx.data::<i32>(0).unwrap()
                    + *ctx.data::<i32>(1).unwrap()
                    + *ctx.data::<i32>(2).unwrap(),
            );
        });
        assert_eq!(
            runs.get(),
            1,
            "ONE atomic settle, not one fire per added dep"
        );
        assert!(
            !saw_partial.get(),
            "never fired with an added dep still SENTINEL"
        );
        assert_eq!(d.cache(), Some(111)); // 1 + 10 + 100
    }

    #[test]
    fn rewire_settle_emits_dirty_before_data() {
        // D1 / R-dirty-before-data: a rewire-triggered settle is a wave — DIRTY precedes DATA.
        let a = Node::<i32>::state(1);
        let b = Node::<i32>::state(100);
        let d: Node<i32> = Node::derived(vec![a.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap())
        });
        let (log, sink) = recorder();
        let _u = d.subscribe(sink);
        log.borrow_mut().clear(); // isolate the rewire wave

        d.add_dep(b.erased(), |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap() + *ctx.data::<i32>(1).unwrap())
        });
        assert_eq!(*log.borrow(), vec!["DIRTY", "DATA"]); // glitch-free two-phase
        assert_eq!(d.cache(), Some(101));
    }

    #[test]
    #[should_panic(expected = "self-dependency")]
    fn rewire_rejects_self_dep() {
        let a = Node::<i32>::state(1);
        let d: Node<i32> = Node::derived(vec![a.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap())
        });
        let _u = d.subscribe(|_| {});
        d.set_deps(vec![d.erased()], |_| {}); // self-dep → panic
    }

    #[test]
    #[should_panic(expected = "cycle")]
    fn rewire_rejects_cycle() {
        let a = Node::<i32>::state(1);
        let d: Node<i32> = Node::derived(vec![a.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap())
        });
        let e: Node<i32> = Node::derived(vec![d.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap())
        });
        let _u = e.subscribe(|_| {}); // e depends on d (→ a)
        d.add_dep(e.erased(), |ctx| ctx.emit(*ctx.data::<i32>(0).unwrap())); // would close d→e→d
    }

    #[test]
    #[should_panic(expected = "terminal dep")]
    fn rewire_rejects_adding_terminal_dep() {
        let term: Node<i32> = Node::producer(|ctx| ctx.down(vec![Message::Complete]));
        let _ut = term.subscribe(|_| {}); // activate → producer completes
        assert_eq!(term.status(), Status::Completed);
        let a = Node::<i32>::state(1);
        let d: Node<i32> = Node::derived(vec![a.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap())
        });
        let _u = d.subscribe(|_| {});
        d.add_dep(term.erased(), |ctx| ctx.emit(*ctx.data::<i32>(0).unwrap())); // terminal dep → panic
    }

    #[test]
    fn rewire_midfn_becomes_error_not_panic() {
        // A fn mutating its OWN deps mid-wave is the D37 feedback cycle. The Rust substrate
        // bundles the D30 catch at the wave-owner, so this surfaces as [[ERROR,e]] on the node
        // (R-reentrancy), NOT a propagated panic (the externally-called rejects DO panic).
        let a = Node::<i32>::state(1);
        let x = Node::<i32>::state(9);
        let slot: Rc<RefCell<Option<Node<i32>>>> = Rc::new(RefCell::new(None));
        let slot2 = slot.clone();
        let d: Node<i32> = Node::derived(vec![a.erased()], move |ctx| {
            if let Some(dh) = slot2.borrow().as_ref() {
                // mid-fn self-rewire — illegal (R-rewire / D37)
                dh.add_dep(x.erased(), |c| c.emit(*c.data::<i32>(0).unwrap()));
            }
            ctx.emit(*ctx.data::<i32>(0).unwrap());
        });
        *slot.borrow_mut() = Some(d.clone());
        let (log, sink) = recorder();
        let _u = d.subscribe(sink); // activate → fn → mid-fn add_dep → reject → wave-owner → ERROR
        assert_eq!(d.status(), Status::Errored);
        assert!(
            log.borrow().iter().any(|k| k == "ERROR"),
            "mid-fn rewire surfaces as ERROR (got {:?})",
            *log.borrow()
        );
    }

    #[test]
    fn rewire_fnswap_frees_old_fn_handle() {
        // B32 (rewire fn-swap GC): a rewire re-registers the fn → a new handle and
        // unregisters the OLD handle, freeing the rewired-away closure + its captures.
        struct DropFlag(Rc<Cell<bool>>);
        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }
        let old_fn_dropped = Rc::new(Cell::new(false));
        let a = Node::<i32>::state(1);
        let guard = DropFlag(old_fn_dropped.clone());
        let d: Node<i32> = Node::derived(vec![a.erased()], move |ctx| {
            let _hold = &guard; // the OLD fn captures the guard
            ctx.emit(*ctx.data::<i32>(0).unwrap());
        });
        let _u = d.subscribe(|_| {});
        assert!(!old_fn_dropped.get(), "old fn is live before the swap");

        d.set_deps(vec![a.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap())
        }); // fn swap
        assert!(
            old_fn_dropped.get(),
            "the rewired-away fn (and its capture) is freed on swap (B32)"
        );
    }

    #[test]
    fn rewire_panic_in_added_dep_activation_does_not_wedge_node() {
        // QA-F1 regression: adding a dep whose ACTIVATION fn panics. The wave-owner catch
        // blames the added producer (its fn ran) → ERROR lands on IT. The partially
        // registered edge is rolled back so `boom` does not retain an orphaned subscriber.
        // Without the DepMutationGuard, `d` would be left in_dep_mutation=true forever
        // → every future maybe_run defers (never recomputes) + every future rewire is
        // rejected as reentrant. With the guard `d` recovers cleanly.
        //
        // `d` ABSORBS a dep error (errorWhenDepsError:false) so this test isolates the QA-F1
        // GUARD concern (in_dep_mutation cleared on the unwind, observed via the recompute
        // below) from the orthogonal R-deps-terminal auto-error-cascade (C-15): under the
        // default errorWhenDepsError:true, boom's activation-panic→ERROR would correctly
        // cascade and TERMINATE d, making "d recovers" un-observable. The guard fires on the
        // unwind identically either way; absorbing just keeps d alive to prove it.
        let a = Node::<i32>::state(1);
        let d: Node<i32> = Node::derived_opts(
            vec![a.erased()],
            NodeOpts {
                error_when_deps_error: false,
                ..NodeOpts::default()
            },
            |ctx| ctx.emit(*ctx.data::<i32>(0).unwrap()),
        );
        let (_log, sink) = recorder();
        let _u = d.subscribe(sink);
        assert_eq!(d.cache(), Some(1));

        // boom: a producer whose activation fn panics — added as a dep of d.
        let boom = Node::<i32>::producer(|_ctx| panic!("boom on activation"));
        d.add_dep(boom.erased(), |ctx| ctx.emit(*ctx.data::<i32>(0).unwrap()));

        assert_eq!(
            boom.core.subscriber_count(),
            0,
            "failed added-dep activation rolls back the partially registered subscriber"
        );

        // d is NOT terminal; the QA-F1 guard cleared in_dep_mutation on the unwind so
        // d is not wedged.
        assert_ne!(
            d.status(),
            Status::Errored,
            "d absorbs the dep error (errorWhenDepsError:false) and survives the rewire panic"
        );

        // NOT WEDGED: d still recomputes from its live dep a (in_dep_mutation was cleared by
        // the guard on the unwind). Without the guard this stays Some(1) (deferred forever).
        a.set(5);
        assert_eq!(
            d.cache(),
            Some(5),
            "d recovers and recomputes — a stuck in_dep_mutation would have deferred this"
        );

        // And a follow-up rewire is NOT rejected as reentrant (the flag is clear).
        d.set_deps(vec![a.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap() * 2)
        });
        a.set(3);
        assert_eq!(
            d.cache(),
            Some(6),
            "a later rewire works (not 'reentrant'-rejected)"
        );
    }
}
