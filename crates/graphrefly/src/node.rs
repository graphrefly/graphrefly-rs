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

use std::cell::{Cell, Ref, RefCell};
use std::collections::{HashMap, HashSet, VecDeque};
use std::marker::PhantomData;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::{Rc, Weak};

use crate::batch::{
    active_batch_committed_token, boundary_drains_blocked, collecting_batch,
    committed_after_batch_for_target, defer_to_batch, register_boundary_root,
};
use crate::ctx::{Ctx, DepRecord, DepTerminal, WaveData};
use crate::dispatcher::{default_dispatcher, Dispatcher, NodeFn, PoolKind};
use crate::environment::EnvironmentDrivers;
use crate::protocol::{AnyValue, GraphError, Handle, LockId, Message, Tier, Wave};
use crate::versioning::{
    advance_node_version, assert_node_version_data_compatible, create_node_version,
    resolve_node_versioning_policy, NodeVersion, NodeVersioningPolicy,
    ResolvedNodeVersioningPolicy, RestoredNodeVersion,
};

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
    /// D109 node runtime versioning policy. `None` means the package default V0; graph-owned
    /// nodes may inherit a graph default before construction.
    pub versioning: Option<NodeVersioningPolicy>,
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
            versioning: None,
        }
    }
}

type Msg = Message<AnyValue>;
type Sink = Rc<dyn Fn(&Msg)>;
type Unsub = Box<dyn FnOnce()>;

enum SubscriberSnapshot {
    Empty,
    One(Sink),
    Many(Vec<Sink>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum MaybeRunDecision {
    #[default]
    Skip,
    Passthrough,
    Run,
}

fn snapshot_subscribers(a: &NodeAux) -> SubscriberSnapshot {
    match a.subscribers.as_slice() {
        [] => SubscriberSnapshot::Empty,
        [(_, sink)] => SubscriberSnapshot::One(sink.clone()),
        many => SubscriberSnapshot::Many(many.iter().map(|(_, s)| s.clone()).collect()),
    }
}

fn deliver_subscriber_snapshot(subs: SubscriberSnapshot, msg: &Msg) {
    match subs {
        SubscriberSnapshot::Empty => {}
        SubscriberSnapshot::One(s) => s(msg),
        SubscriberSnapshot::Many(subs) => {
            for s in subs {
                s(msg);
            }
        }
    }
}

/// A deferred self-rewire request (R-rewire-deferred / D47), issued via `ctx.rewire_next`
/// and applied at the committed wave boundary. The new dep set is computed at APPLY time (so
/// multiple requests in one wave compose against the live deps), then the surgical R-rewire
/// (D42) runs as a fresh wave. The fn re-pairs the deps (SD-1 fn-deps pairing).
pub(crate) enum RewireRequest {
    /// Add a dep (idempotent if already present) + swap the fn.
    Add(Core, NodeFn),
    /// Remove a live dep identity + swap the fn. If that identity is already absent
    /// at boundary-apply time, the request is a full no-op, including no fn swap, so
    /// stale duplicate removes cannot desync the live dep/fn pairing.
    Remove(CoreIdentity, NodeFn),
    /// Replace the whole dep set + swap the fn.
    Set(Vec<Core>, NodeFn),
}

impl RewireRequest {
    pub(crate) fn remove(dep: &Core, f: NodeFn) -> Self {
        Self::Remove(CoreIdentity::from_core(dep), f)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct NodeId(usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct NodeKey {
    id: NodeId,
    generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ArenaNodeKey {
    graph: usize,
    node: NodeKey,
}

pub(crate) struct CoreIdentity {
    arena_key: ArenaNodeKey,
}

impl CoreIdentity {
    fn from_core(core: &Core) -> Self {
        Self {
            arena_key: core.arena_node_key(),
        }
    }

    fn matches(&self, core: &Core) -> bool {
        self.arena_key == core.arena_node_key()
    }
}

/// B49 / D71 first migration slice: graph-local arena for node bookkeeping. The
/// public [`Core`] is now a node id plus a shared graph arena, rather than a direct
/// `Rc<RefCell<NodeInner>>`. Topology is already graph-owned in `NodeTopologySlot`;
/// this keeps the public API stable while moving ownership toward arena-index execution.
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
    topology_slots: Vec<Option<NodeTopologySlot>>,
    call_slots: Vec<Option<NodeCallSlot>>,
    config_slots: Vec<Option<NodeConfigSlot>>,
    run_slots: Vec<Option<NodeRunState>>,
    edge_slots: Vec<Option<DepEdges>>,
    version_slots: Vec<Option<NodeVersionState>>,
    aux_slots: Vec<Option<NodeAux>>,
    generations: Vec<u64>,
    touched_waves: Vec<u64>,
    pins: Vec<usize>,
    free: Vec<usize>,
    deferred_boundary: VecDeque<BoundaryTask>,
    draining_boundary: bool,
}

type TakenNodeSlot = (
    NodeInner,
    NodeTopologySlot,
    NodeCallSlot,
    NodeConfigSlot,
    NodeRunState,
    DepEdges,
    NodeVersionState,
    NodeAux,
);

impl GraphCore {
    fn new() -> Self {
        Self {
            slots: Vec::new(),
            topology_slots: Vec::new(),
            call_slots: Vec::new(),
            config_slots: Vec::new(),
            run_slots: Vec::new(),
            edge_slots: Vec::new(),
            version_slots: Vec::new(),
            aux_slots: Vec::new(),
            generations: Vec::new(),
            touched_waves: Vec::new(),
            pins: Vec::new(),
            free: Vec::new(),
            deferred_boundary: VecDeque::new(),
            draining_boundary: false,
        }
    }

    fn alloc(
        &mut self,
        inner: NodeInner,
        topology: NodeTopologySlot,
        call: NodeCallSlot,
        config: NodeConfigSlot,
        version: NodeVersionState,
    ) -> NodeId {
        let edges = DepEdges::new(topology.deps.len());
        let aux = NodeAux::new();
        let run = NodeRunState::new();
        if let Some(id) = self.free.last().copied() {
            let next_generation = self.generations[id]
                .checked_add(1)
                .expect("GraphCore generation overflow on slot reuse");
            self.free.pop();
            self.slots[id] = Some(inner);
            self.topology_slots[id] = Some(topology);
            self.call_slots[id] = Some(call);
            self.config_slots[id] = Some(config);
            self.run_slots[id] = Some(run);
            self.edge_slots[id] = Some(edges);
            self.version_slots[id] = Some(version);
            self.aux_slots[id] = Some(aux);
            self.generations[id] = next_generation;
            self.touched_waves[id] = 0;
            self.pins[id] = 0;
            NodeId(id)
        } else {
            let id = self.slots.len();
            self.slots.push(Some(inner));
            self.topology_slots.push(Some(topology));
            self.call_slots.push(Some(call));
            self.config_slots.push(Some(config));
            self.run_slots.push(Some(run));
            self.edge_slots.push(Some(edges));
            self.version_slots.push(Some(version));
            self.aux_slots.push(Some(aux));
            self.generations.push(0);
            self.touched_waves.push(0);
            self.pins.push(0);
            NodeId(id)
        }
    }

    fn key_for(&self, id: NodeId) -> NodeKey {
        NodeKey {
            id,
            generation: self.generations[id.0],
        }
    }

    fn get(&self, key: NodeKey) -> &NodeTopologySlot {
        assert!(
            self.is_live_key(key),
            "Core points at a stale or freed GraphCore slot"
        );
        self.topology_slots
            .get(key.id.0)
            .and_then(Option::as_ref)
            .expect("Core points at a live GraphCore topology slot")
    }

    fn get_node_and_edges_mut(&mut self, key: NodeKey) -> (&mut NodeTopologySlot, &mut DepEdges) {
        assert!(
            self.is_live_key(key),
            "Core points at a stale or freed GraphCore slot"
        );
        let n = self
            .topology_slots
            .get_mut(key.id.0)
            .and_then(Option::as_mut)
            .expect("Core points at a live GraphCore topology slot");
        let e = self
            .edge_slots
            .get_mut(key.id.0)
            .and_then(Option::as_mut)
            .expect("Core points at a live GraphCore edge slot");
        (n, e)
    }

    fn get_node_call_config_run_edges(
        &self,
        key: NodeKey,
    ) -> (
        &NodeTopologySlot,
        &NodeCallSlot,
        &NodeConfigSlot,
        &NodeRunState,
        &DepEdges,
    ) {
        assert!(
            self.is_live_key(key),
            "Core points at a stale or freed GraphCore slot"
        );
        let n = self
            .topology_slots
            .get(key.id.0)
            .and_then(Option::as_ref)
            .expect("Core points at a live GraphCore topology slot");
        let c = self
            .call_slots
            .get(key.id.0)
            .and_then(Option::as_ref)
            .expect("Core points at a live GraphCore call slot");
        let cfg = self
            .config_slots
            .get(key.id.0)
            .and_then(Option::as_ref)
            .expect("Core points at a live GraphCore config slot");
        let r = self
            .run_slots
            .get(key.id.0)
            .and_then(Option::as_ref)
            .expect("Core points at a live GraphCore run slot");
        let e = self
            .edge_slots
            .get(key.id.0)
            .and_then(Option::as_ref)
            .expect("Core points at a live GraphCore edge slot");
        (n, c, cfg, r, e)
    }

    fn get_call(&self, key: NodeKey) -> &NodeCallSlot {
        assert!(
            self.is_live_key(key),
            "Core points at a stale or freed GraphCore slot"
        );
        self.call_slots
            .get(key.id.0)
            .and_then(Option::as_ref)
            .expect("Core points at a live GraphCore call slot")
    }

    fn get_call_mut(&mut self, key: NodeKey) -> &mut NodeCallSlot {
        assert!(
            self.is_live_key(key),
            "Core points at a stale or freed GraphCore slot"
        );
        self.call_slots
            .get_mut(key.id.0)
            .and_then(Option::as_mut)
            .expect("Core points at a live GraphCore call slot")
    }

    fn get_config(&self, key: NodeKey) -> &NodeConfigSlot {
        assert!(
            self.is_live_key(key),
            "Core points at a stale or freed GraphCore slot"
        );
        self.config_slots
            .get(key.id.0)
            .and_then(Option::as_ref)
            .expect("Core points at a live GraphCore config slot")
    }

    fn get_version(&self, key: NodeKey) -> &NodeVersionState {
        assert!(
            self.is_live_key(key),
            "Core points at a stale or freed GraphCore slot"
        );
        self.version_slots
            .get(key.id.0)
            .and_then(Option::as_ref)
            .expect("Core points at a live GraphCore version slot")
    }

    fn get_version_mut(&mut self, key: NodeKey) -> &mut NodeVersionState {
        assert!(
            self.is_live_key(key),
            "Core points at a stale or freed GraphCore slot"
        );
        self.version_slots
            .get_mut(key.id.0)
            .and_then(Option::as_mut)
            .expect("Core points at a live GraphCore version slot")
    }

    fn get_node_edges_aux_mut(
        &mut self,
        key: NodeKey,
    ) -> (&mut NodeTopologySlot, &mut DepEdges, &mut NodeAux) {
        assert!(
            self.is_live_key(key),
            "Core points at a stale or freed GraphCore slot"
        );
        let topology_slots = &mut self.topology_slots;
        let edge_slots = &mut self.edge_slots;
        let aux_slots = &mut self.aux_slots;
        let n = topology_slots
            .get_mut(key.id.0)
            .and_then(Option::as_mut)
            .expect("Core points at a live GraphCore topology slot");
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

    fn get_node_call_config_run_edges_mut(
        &mut self,
        key: NodeKey,
    ) -> (
        &mut NodeTopologySlot,
        &mut NodeCallSlot,
        &mut NodeConfigSlot,
        &mut NodeRunState,
        &mut DepEdges,
    ) {
        assert!(
            self.is_live_key(key),
            "Core points at a stale or freed GraphCore slot"
        );
        let topology_slots = &mut self.topology_slots;
        let call_slots = &mut self.call_slots;
        let config_slots = &mut self.config_slots;
        let run_slots = &mut self.run_slots;
        let edge_slots = &mut self.edge_slots;
        let n = topology_slots
            .get_mut(key.id.0)
            .and_then(Option::as_mut)
            .expect("Core points at a live GraphCore topology slot");
        let c = call_slots
            .get_mut(key.id.0)
            .and_then(Option::as_mut)
            .expect("Core points at a live GraphCore call slot");
        let cfg = config_slots
            .get_mut(key.id.0)
            .and_then(Option::as_mut)
            .expect("Core points at a live GraphCore config slot");
        let r = run_slots
            .get_mut(key.id.0)
            .and_then(Option::as_mut)
            .expect("Core points at a live GraphCore run slot");
        let e = edge_slots
            .get_mut(key.id.0)
            .and_then(Option::as_mut)
            .expect("Core points at a live GraphCore edge slot");
        (n, c, cfg, r, e)
    }

    fn get_node_call_config_run_edges_aux_mut(
        &mut self,
        key: NodeKey,
    ) -> (
        &mut NodeTopologySlot,
        &mut NodeCallSlot,
        &mut NodeConfigSlot,
        &mut NodeRunState,
        &mut DepEdges,
        &mut NodeAux,
    ) {
        assert!(
            self.is_live_key(key),
            "Core points at a stale or freed GraphCore slot"
        );
        let topology_slots = &mut self.topology_slots;
        let call_slots = &mut self.call_slots;
        let config_slots = &mut self.config_slots;
        let run_slots = &mut self.run_slots;
        let edge_slots = &mut self.edge_slots;
        let aux_slots = &mut self.aux_slots;
        let n = topology_slots
            .get_mut(key.id.0)
            .and_then(Option::as_mut)
            .expect("Core points at a live GraphCore topology slot");
        let c = call_slots
            .get_mut(key.id.0)
            .and_then(Option::as_mut)
            .expect("Core points at a live GraphCore call slot");
        let cfg = config_slots
            .get_mut(key.id.0)
            .and_then(Option::as_mut)
            .expect("Core points at a live GraphCore config slot");
        let r = run_slots
            .get_mut(key.id.0)
            .and_then(Option::as_mut)
            .expect("Core points at a live GraphCore run slot");
        let e = edge_slots
            .get_mut(key.id.0)
            .and_then(Option::as_mut)
            .expect("Core points at a live GraphCore edge slot");
        let a = aux_slots
            .get_mut(key.id.0)
            .and_then(Option::as_mut)
            .expect("Core points at a live GraphCore aux slot");
        (n, c, cfg, r, e, a)
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
        let live = self.slots.get(id.0).is_some_and(Option::is_some);
        if live {
            debug_assert!(self.topology_slots.get(id.0).is_some_and(Option::is_some));
            debug_assert!(self.call_slots.get(id.0).is_some_and(Option::is_some));
            debug_assert!(self.config_slots.get(id.0).is_some_and(Option::is_some));
            debug_assert!(self.run_slots.get(id.0).is_some_and(Option::is_some));
            debug_assert!(self.edge_slots.get(id.0).is_some_and(Option::is_some));
            debug_assert!(self.version_slots.get(id.0).is_some_and(Option::is_some));
            debug_assert!(self.aux_slots.get(id.0).is_some_and(Option::is_some));
        }
        live
    }

    fn is_live_key(&self, key: NodeKey) -> bool {
        self.generations.get(key.id.0) == Some(&key.generation) && self.is_live(key.id)
    }

    fn pin_live_key(&mut self, key: NodeKey) -> bool {
        if !self.is_live_key(key) {
            return false;
        }
        self.pins[key.id.0] = self.pins[key.id.0]
            .checked_add(1)
            .expect("GraphCore pin count overflow");
        true
    }

    fn release_pin(&mut self, key: NodeKey) {
        if self.generations.get(key.id.0) != Some(&key.generation) {
            return;
        }
        let Some(pin) = self.pins.get_mut(key.id.0) else {
            return;
        };
        if *pin > 0 {
            *pin -= 1;
        }
    }

    fn pin_count(&self, key: NodeKey) -> usize {
        if self.generations.get(key.id.0) != Some(&key.generation) {
            return 0;
        }
        self.pins.get(key.id.0).copied().unwrap_or(0)
    }

    fn take_live(&mut self, key: NodeKey) -> Option<TakenNodeSlot> {
        if !self.is_live_key(key) {
            return None;
        }
        let inner = self.slots.get_mut(key.id.0)?.take()?;
        let topology = self.topology_slots.get_mut(key.id.0)?.take()?;
        let call = self.call_slots.get_mut(key.id.0)?.take()?;
        let config = self.config_slots.get_mut(key.id.0)?.take()?;
        let run = self.run_slots.get_mut(key.id.0)?.take()?;
        let edges = self.edge_slots.get_mut(key.id.0)?.take()?;
        let version = self.version_slots.get_mut(key.id.0)?.take()?;
        let aux = self.aux_slots.get_mut(key.id.0)?.take()?;
        Some((inner, topology, call, config, run, edges, version, aux))
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
    restored_activation_handshake: Vec<bool>,
}

impl DepEdges {
    fn new(dep_count: usize) -> Self {
        Self {
            value: NodeValueState::new(),
            wave: NodeWaveState::new(),
            state: DepState::new(dep_count),
            unsubs: (0..dep_count).map(|_| None).collect(),
            idx_boxes: (0..dep_count).map(|_| Rc::new(Cell::new(-1i64))).collect(),
            restored_activation_handshake: vec![false; dep_count],
        }
    }
}

/// Dispatcher-owned callable metadata for one node. Keeping this in GraphCore
/// lets the hot wave path snapshot the call target by arena index, then invoke
/// borrow-free through the dispatcher (R-dispatch-all).
struct NodeCallSlot {
    factory: Option<String>,
    handle: Option<Handle>,
    dispatcher: Dispatcher,
    environment: EnvironmentDrivers,
}

/// Construction/configuration state for one node. This is side-table owned so the
/// public `Core` handle can keep thinning toward a graph/id/generation token.
struct NodeConfigSlot {
    partial: bool,
    pausable: Pausable,
    pull_id: Option<LockId>,
    complete_when_deps_complete: bool,
    error_when_deps_error: bool,
    terminal_as_real_input: bool,
    versioning: Option<NodeVersioningPolicy>,
}

impl NodeConfigSlot {
    fn from_opts(opts: &NodeOpts) -> Self {
        Self {
            partial: opts.partial,
            pausable: opts.pausable,
            pull_id: None,
            complete_when_deps_complete: opts.complete_when_deps_complete,
            error_when_deps_error: opts.error_when_deps_error,
            terminal_as_real_input: opts.terminal_as_real_input,
            versioning: opts.versioning.clone(),
        }
    }

    fn all_deps_settled(&self, dep: &DepState) -> bool {
        dep.all_settled(self.terminal_as_real_input)
    }
}

struct NodeVersionState {
    policy: ResolvedNodeVersioningPolicy,
    value: Option<NodeVersion>,
}

impl NodeVersionState {
    fn new(policy: Option<NodeVersioningPolicy>, initial: Option<&AnyValue>) -> Self {
        let policy = resolve_node_versioning_policy(policy);
        let value = create_node_version(&policy, initial)
            .unwrap_or_else(|err| panic!("node versioning: {err}"));
        Self { policy, value }
    }
}

/// Lifecycle/run mutable state. Transient wave flags stay in [`NodeWaveState`];
/// this side table holds state that persists across waves but resets on lifecycle
/// teardown.
struct NodeRunState {
    has_called_fn_once: bool,
}

impl NodeRunState {
    fn new() -> Self {
        Self {
            has_called_fn_once: false,
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

pub(crate) struct NodeCheckpointRuntime {
    pub cache: Option<AnyValue>,
    pub has_data: bool,
    pub version: Option<NodeVersion>,
    pub status: Status,
    pub terminal: bool,
    pub activated: bool,
    pub has_called_fn_once: bool,
    pub ctx_state: Option<AnyValue>,
    pub ctx_state_persist: bool,
}

pub(crate) struct NodeRestoreRuntime {
    pub cache: Option<AnyValue>,
    pub has_data: bool,
    pub version: RestoredNodeVersion,
    pub status: Status,
    pub terminal: bool,
    pub has_called_fn_once: bool,
    pub ctx_state: Option<AnyValue>,
    pub ctx_state_persist: bool,
}

/// The mutable state of a node. Pure fields + non-reentrant helpers only; the
/// reentrant engine lives on [`Core`].
type TopologyDepsChangedObserver = Rc<dyn Fn(&[Core], &[Core])>;

struct NodeTopologySlot {
    deps: Vec<Core>,
    topology_deps_changed: Option<TopologyDepsChangedObserver>,
}

struct NodeInner;

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
    fn cleanup_before_free(
        &mut self,
        call: &mut NodeCallSlot,
        edges: &mut DepEdges,
        aux: &mut NodeAux,
    ) {
        // B32: free this node's fn slot so the dispatcher pool no longer holds the fn —
        // and thus the upstream `Core`s the fn captured (e.g. a `Node: Clone` handle, the
        // idiomatic feedback pattern) — alive for the whole process. Safe: invoke clones
        // the fn out before calling (the pool is unborrowed during the fn run), so a node
        // dropped from inside a fn can re-borrow the pool here. A fn that captures its OWN
        // node is a genuine Rc cycle this cannot break (the pool keeps that node alive →
        // Drop never runs) — that is a user-level self-cycle (use a Weak self-handle).
        // Run this before user cleanup callbacks so a panicking cleanup cannot strand the
        // dispatcher slot and its captures.
        if let Some(handle) = call.handle {
            call.dispatcher.unregister(handle);
        }
        for u in edges.unsubs.drain(..).flatten() {
            u();
        }
        for h in std::mem::take(&mut aux.on_deactivation) {
            h();
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
        assert!(
            refs.get() != 0,
            "Core counted promotion rejected: node is externally dead"
        );
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

    fn borrowed_view(&self) -> Self {
        Self::from_borrowed_parts(
            self.graph.clone(),
            self.id,
            self.generation,
            self.refs.clone(),
        )
    }

    fn key(&self) -> NodeKey {
        NodeKey {
            id: self.id,
            generation: self.generation,
        }
    }

    fn arena_node_key(&self) -> ArenaNodeKey {
        ArenaNodeKey {
            graph: Rc::as_ptr(&self.graph) as usize,
            node: self.key(),
        }
    }

    fn borrow(&self) -> Ref<'_, NodeTopologySlot> {
        Ref::map(self.graph.borrow(), |g| g.get(self.key()))
    }

    fn with_inner_edges_mut<R>(
        &self,
        f: impl FnOnce(&mut NodeTopologySlot, &mut DepEdges) -> R,
    ) -> R {
        let mut g = self.graph.borrow_mut();
        let (n, e) = g.get_node_and_edges_mut(self.key());
        f(n, e)
    }

    fn with_inner_edges_aux_mut<R>(
        &self,
        f: impl FnOnce(&mut NodeTopologySlot, &mut DepEdges, &mut NodeAux) -> R,
    ) -> R {
        let mut g = self.graph.borrow_mut();
        let (n, e, a) = g.get_node_edges_aux_mut(self.key());
        f(n, e, a)
    }

    fn with_inner_edges<R>(&self, f: impl FnOnce(&NodeTopologySlot, &DepEdges) -> R) -> R {
        let g = self.graph.borrow();
        let n = g.get(self.key());
        let e = g
            .edge_slots
            .get(self.id.0)
            .and_then(Option::as_ref)
            .expect("Core points at a live GraphCore edge slot");
        f(n, e)
    }

    fn with_node_state<R>(
        &self,
        f: impl FnOnce(&NodeTopologySlot, &NodeCallSlot, &NodeConfigSlot, &NodeRunState, &DepEdges) -> R,
    ) -> R {
        let g = self.graph.borrow();
        let (n, c, cfg, r, e) = g.get_node_call_config_run_edges(self.key());
        f(n, c, cfg, r, e)
    }

    fn with_node_state_mut<R>(
        &self,
        f: impl FnOnce(
            &mut NodeTopologySlot,
            &mut NodeCallSlot,
            &mut NodeConfigSlot,
            &mut NodeRunState,
            &mut DepEdges,
        ) -> R,
    ) -> R {
        let mut g = self.graph.borrow_mut();
        let (n, c, cfg, r, e) = g.get_node_call_config_run_edges_mut(self.key());
        f(n, c, cfg, r, e)
    }

    fn with_node_state_aux_mut<R>(
        &self,
        f: impl FnOnce(
            &mut NodeTopologySlot,
            &mut NodeCallSlot,
            &mut NodeConfigSlot,
            &mut NodeRunState,
            &mut DepEdges,
            &mut NodeAux,
        ) -> R,
    ) -> R {
        let mut g = self.graph.borrow_mut();
        let (n, c, cfg, r, e, a) = g.get_node_call_config_run_edges_aux_mut(self.key());
        f(n, c, cfg, r, e, a)
    }

    fn with_call<R>(&self, f: impl FnOnce(&NodeCallSlot) -> R) -> R {
        let g = self.graph.borrow();
        f(g.get_call(self.key()))
    }

    fn with_call_mut<R>(&self, f: impl FnOnce(&mut NodeCallSlot) -> R) -> R {
        let mut g = self.graph.borrow_mut();
        f(g.get_call_mut(self.key()))
    }

    fn with_config<R>(&self, f: impl FnOnce(&NodeConfigSlot) -> R) -> R {
        let g = self.graph.borrow();
        f(g.get_config(self.key()))
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

    fn try_with_node_state_aux_mut<R>(
        &self,
        f: impl FnOnce(
            &mut NodeTopologySlot,
            &mut NodeCallSlot,
            &mut NodeConfigSlot,
            &mut NodeRunState,
            &mut DepEdges,
            &mut NodeAux,
        ) -> R,
    ) -> Option<R> {
        let mut g = self.graph.try_borrow_mut().ok()?;
        let (n, c, cfg, r, e, a) = g.get_node_call_config_run_edges_aux_mut(self.key());
        Some(f(n, c, cfg, r, e, a))
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
        free_slot_if_unreferenced(&self.graph, self.key(), &self.refs);
    }
}

fn free_slot_if_unreferenced(graph_ref: &Rc<RefCell<GraphCore>>, key: NodeKey, refs: &Cell<usize>) {
    if refs.get() != 0 {
        return;
    }
    let (mut inner, _topology, mut call, _config, _run, mut edges, _version, mut aux) = {
        let mut graph = graph_ref.borrow_mut();
        if graph.pin_count(key) != 0 {
            return;
        }
        let Some(pair) = graph.take_live(key) else {
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
        id: key.id.0,
        armed: true,
    };
    inner.cleanup_before_free(&mut call, &mut edges, &mut aux);
    free_slot.armed = false;
    graph_ref.borrow_mut().free.push(key.id.0);
}

struct ArenaNodePin {
    arena_key: ArenaNodeKey,
    graph: Rc<RefCell<GraphCore>>,
    refs: Rc<Cell<usize>>,
}

impl ArenaNodePin {
    pub(crate) fn from_core(core: &Core) -> Option<Self> {
        {
            let mut graph = core.graph.borrow_mut();
            if !graph.pin_live_key(core.key()) {
                return None;
            }
        }
        Some(Self {
            arena_key: core.arena_node_key(),
            graph: core.graph.clone(),
            refs: core.refs.clone(),
        })
    }

    fn borrowed_core(&self) -> Option<Core> {
        if !self.graph.borrow().is_live_key(self.arena_key.node) {
            return None;
        }
        Some(Core::from_borrowed_parts(
            self.graph.clone(),
            self.arena_key.node.id,
            self.arena_key.node.generation,
            self.refs.clone(),
        ))
    }

    fn reset_wave_flags(&self) {
        let mut g = self.graph.borrow_mut();
        if !g.is_live_key(self.arena_key.node) {
            return;
        }
        let (n, e) = g.get_node_and_edges_mut(self.arena_key.node);
        reset_wave_flags_inner(n, e);
    }

    fn boundary_root(&self) -> BoundaryRoot {
        BoundaryRoot {
            graph_id: self.arena_key.graph,
            graph: Rc::downgrade(&self.graph),
        }
    }
}

impl Drop for ArenaNodePin {
    fn drop(&mut self) {
        self.graph.borrow_mut().release_pin(self.arena_key.node);
        free_slot_if_unreferenced(&self.graph, self.arena_key.node, &self.refs);
    }
}

pub(crate) struct BatchTarget {
    pin: ArenaNodePin,
}

impl BatchTarget {
    pub(crate) fn from_core(core: &Core) -> Option<Self> {
        Some(Self {
            pin: ArenaNodePin::from_core(core)?,
        })
    }

    pub(crate) fn matches(&self, core: &Core) -> bool {
        self.pin.arena_key == core.arena_node_key()
    }

    pub(crate) fn boundary_root(&self) -> BoundaryRoot {
        self.pin.boundary_root()
    }

    pub(crate) fn commit_batched_wave(&self, wave: Wave<AnyValue>) {
        if let Some(core) = self.pin.borrowed_core() {
            core.commit_batched_wave(wave);
        }
    }

    pub(crate) fn rollback_batched(&self) {
        if let Some(core) = self.pin.borrowed_core() {
            core.rollback_batched();
        }
    }

    #[cfg(test)]
    fn borrowed_core(&self) -> Option<Core> {
        self.pin.borrowed_core()
    }
}

#[derive(Clone)]
struct CoreWeak {
    graph: Weak<RefCell<GraphCore>>,
    key: NodeKey,
    refs: Weak<Cell<usize>>,
}

#[derive(Default)]
struct InWaveDepReceiveAction {
    emit_teardown_subscribers: bool,
    emit_dirty: bool,
    do_invalidate: bool,
    invalidate_as_invalidate_msg: bool,
    had_node_data_before_invalidate: bool,
    clear_undirty_after_invalidate: bool,
    defer_maybe_run: bool,
    maybe_run_decision: MaybeRunDecision,
    down_msgs: Option<Vec<Msg>>,
    do_settle_after_absorbed_terminal: bool,
    fire_demand: bool,
}

impl InWaveDepReceiveAction {
    fn has_work(&self) -> bool {
        self.emit_teardown_subscribers
            || self.emit_dirty
            || self.do_invalidate
            || self.defer_maybe_run
            || !matches!(self.maybe_run_decision, MaybeRunDecision::Skip)
            || self.do_settle_after_absorbed_terminal
            || self.fire_demand
            || self.down_msgs.as_ref().is_some_and(|msgs| !msgs.is_empty())
    }

    fn apply(self, core: &Core) {
        if self.emit_teardown_subscribers {
            core.emit_to_subs(&Message::Teardown);
        }
        if self.emit_dirty {
            core.emit_to_subs(&Message::Dirty);
        }
        if self.do_invalidate {
            if self.invalidate_as_invalidate_msg && self.had_node_data_before_invalidate {
                core.down(vec![Message::Invalidate]);
            } else {
                core.invalidate();
            }
        }
        if self.clear_undirty_after_invalidate {
            if self.had_node_data_before_invalidate {
                core.with_inner_edges_mut(|_n, e| {
                    e.wave.emitted_dirty_this_wave = false;
                });
            } else {
                core.down(vec![Message::Resolved]);
            }
        }
        if self.defer_maybe_run {
            defer_run_until_delivery_boundary(core);
        }
        match self.maybe_run_decision {
            MaybeRunDecision::Skip => {}
            MaybeRunDecision::Passthrough => core.passthrough_emit(),
            MaybeRunDecision::Run => {
                core.run_wave();
            }
        }
        if let Some(msgs) = self.down_msgs {
            core.down(msgs);
        }
        if self.do_settle_after_absorbed_terminal {
            core.settle_after_absorbed_terminal();
        }
        if self.fire_demand {
            core.fire_owed_demand_if_ready();
        }
    }
}

enum DownAction {
    Emit { msg: Msg, subs: SubscriberSnapshot },
    Invalidate { hooks: Vec<Rc<dyn Fn()>> },
}

impl CoreWeak {
    #[cfg(test)]
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

    #[cfg(test)]
    fn borrowed_core(&self) -> Option<Core> {
        let graph = self.graph.upgrade()?;
        let refs = self.refs.upgrade()?;
        let can_borrow = {
            let g = graph.borrow();
            g.is_live_key(self.key) && (refs.get() != 0 || g.pin_count(self.key) != 0)
        };
        if !can_borrow {
            return None;
        }
        Some(Core::from_borrowed_parts(
            graph,
            self.key.id,
            self.key.generation,
            refs,
        ))
    }

    fn pinned_borrowed_core(&self) -> Option<(ArenaNodePin, Core)> {
        let graph = self.graph.upgrade()?;
        let refs = self.refs.upgrade()?;
        let arena_key = ArenaNodeKey {
            graph: Rc::as_ptr(&graph) as usize,
            node: self.key,
        };
        {
            let mut g = graph.borrow_mut();
            if !g.pin_live_key(self.key) {
                return None;
            }
        }
        let pin = ArenaNodePin {
            arena_key,
            graph,
            refs,
        };
        let core = pin.borrowed_core()?;
        Some((pin, core))
    }

    fn collect_in_wave_receive_from_dep_action(
        &self,
        idx: usize,
        msg: &Msg,
    ) -> Option<InWaveDepReceiveAction> {
        let graph = self.graph.upgrade()?;
        let refs = self.refs.upgrade()?;
        let arena_key = ArenaNodeKey {
            graph: Rc::as_ptr(&graph) as usize,
            node: self.key,
        };
        let registered = WAVE.with(|w| {
            let mut wave = w.borrow_mut();
            let Some(scope) = wave.as_mut() else {
                return false;
            };
            scope.pin_once(arena_key, &graph, &refs)
        });
        if !registered {
            return None;
        }
        let mut g = graph.borrow_mut();
        Core::collect_receive_from_dep_action_from_graph(&mut g, self.key, idx, msg)
    }

    fn apply_in_wave_receive_action(&self, action: InWaveDepReceiveAction) {
        let graph = match self.graph.upgrade() {
            Some(graph) => graph,
            None => return,
        };
        let refs = match self.refs.upgrade() {
            Some(refs) => refs,
            None => return,
        };
        let can_apply = {
            let g = graph.borrow();
            g.is_live_key(self.key) && (refs.get() != 0 || g.pin_count(self.key) != 0)
        };
        if !can_apply {
            return;
        }
        let core = Core::from_borrowed_parts(graph, self.key.id, self.key.generation, refs);
        core.apply_receive_from_dep_action(action);
    }

    fn receive_from_dep(&self, idx: usize, msg: &Msg) {
        // DR-8/B54 receive policy:
        // - active wave: use a borrowed arena view; `Core::receive_from_dep` immediately
        //   enrolls in the arena-pinned touched set before any callback-capable work;
        // - no active wave: pin first and run through a borrowed owner view, avoiding
        //   counted churn for ordinary callback delivery and for late cleanup/release
        //   windows.
        if wave_in_flight() {
            let Some(action) = self.collect_in_wave_receive_from_dep_action(idx, msg) else {
                return;
            };
            if action.has_work() {
                self.apply_in_wave_receive_action(action);
            }
            return;
        }
        if let Some((_pin, core)) = self.pinned_borrowed_core() {
            core.receive_from_dep_registered(idx, msg);
        }
    }
}

struct DeferredNodeAction {
    arena_key: ArenaNodeKey,
    weak: CoreWeak,
}

impl DeferredNodeAction {
    fn from_core(core: &Core) -> Self {
        Self {
            arena_key: core.arena_node_key(),
            weak: core.downgrade(),
        }
    }

    fn matches(&self, core: &Core) -> bool {
        self.arena_key == core.arena_node_key()
    }

    fn pinned_borrowed_core(&self) -> Option<(ArenaNodePin, Core)> {
        self.weak.pinned_borrowed_core()
    }

    fn with_live_edges_mut<R>(&self, f: impl FnOnce(&mut DepEdges) -> R) -> Option<R> {
        let graph = self.weak.graph.upgrade()?;
        let refs = self.weak.refs.upgrade()?;
        let mut g = graph.borrow_mut();
        if !g.is_live_key(self.weak.key) || (refs.get() == 0 && g.pin_count(self.weak.key) == 0) {
            return None;
        }
        let (_n, e) = g.get_node_and_edges_mut(self.weak.key);
        Some(f(e))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DeferredDeliveryKind {
    Run,
    AbsorbedSettle,
}

struct DeferredDeliveryAction {
    kind: DeferredDeliveryKind,
    target: DeferredNodeAction,
}

impl DeferredDeliveryAction {
    fn from_core(kind: DeferredDeliveryKind, core: &Core) -> Self {
        Self {
            kind,
            target: DeferredNodeAction::from_core(core),
        }
    }

    fn matches(&self, kind: DeferredDeliveryKind, core: &Core) -> bool {
        self.kind == kind && self.target.matches(core)
    }

    fn core_for_drain(&self) -> Option<(ArenaNodePin, Core)> {
        self.target.pinned_borrowed_core()
    }

    fn apply(&self) {
        let Some((_pin, node)) = self.core_for_drain() else {
            return;
        };
        if node.with_inner_edges(|_, e| e.value.terminal) {
            return;
        }
        match self.kind {
            DeferredDeliveryKind::Run => node.maybe_run(),
            DeferredDeliveryKind::AbsorbedSettle => node.settle_after_absorbed_terminal(),
        }
    }
}

#[derive(Clone)]
struct CoreToken {
    graph_id: usize,
    key: NodeKey,
    refs: Weak<Cell<usize>>,
}

impl CoreToken {
    fn from_core(core: &Core) -> Self {
        Self {
            graph_id: Rc::as_ptr(&core.graph) as usize,
            key: core.key(),
            refs: Rc::downgrade(&core.refs),
        }
    }

    fn pinned_borrowed_core(&self, graph: &Rc<RefCell<GraphCore>>) -> Option<PinnedBorrowedCore> {
        if Rc::as_ptr(graph) as usize != self.graph_id {
            return None;
        }
        let refs = self.refs.upgrade()?;
        {
            let mut g = graph.borrow_mut();
            if !(g.is_live_key(self.key) && (refs.get() != 0 || g.pin_count(self.key) != 0)) {
                return None;
            }
            if !g.pin_live_key(self.key) {
                return None;
            }
        }
        let arena_key = ArenaNodeKey {
            graph: Rc::as_ptr(graph) as usize,
            node: self.key,
        };
        Some(PinnedBorrowedCore {
            _pin: ArenaNodePin {
                arena_key,
                graph: graph.clone(),
                refs: refs.clone(),
            },
            core: Core::from_borrowed_parts(graph.clone(), self.key.id, self.key.generation, refs),
        })
    }
}

struct BoundaryDrainGuard {
    graph: Rc<RefCell<GraphCore>>,
    active: bool,
}

impl BoundaryDrainGuard {
    fn enter(graph: &Rc<RefCell<GraphCore>>) -> Self {
        graph.borrow_mut().draining_boundary = true;
        Self {
            graph: graph.clone(),
            active: true,
        }
    }

    fn finish(mut self) {
        self.active = false;
        self.graph.borrow_mut().draining_boundary = false;
    }
}

impl Drop for BoundaryDrainGuard {
    fn drop(&mut self) {
        if self.active {
            self.graph.borrow_mut().draining_boundary = false;
        }
    }
}

struct PinnedBorrowedCore {
    _pin: ArenaNodePin,
    core: Core,
}

enum BoundaryTask {
    Rewire {
        target: CoreToken,
        req: RewireRequest,
        committed: Rc<Cell<bool>>,
    },
    ExternalRewire {
        target: CoreToken,
        new_deps: Vec<Core>,
        fn_: NodeFn,
        committed: Rc<Cell<bool>>,
    },
    Up {
        target: CoreToken,
        msgs: Wave<AnyValue>,
        toward_dep: Option<usize>,
        committed: Rc<Cell<bool>>,
    },
}

impl BoundaryTask {
    fn committed(&self) -> bool {
        match self {
            BoundaryTask::Rewire { committed, .. }
            | BoundaryTask::ExternalRewire { committed, .. }
            | BoundaryTask::Up { committed, .. } => committed.get(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct BoundaryRoot {
    graph_id: usize,
    graph: Weak<RefCell<GraphCore>>,
}

impl BoundaryRoot {
    fn from_core(core: &Core) -> Self {
        Self {
            graph_id: Rc::as_ptr(&core.graph) as usize,
            graph: Rc::downgrade(&core.graph),
        }
    }

    pub(crate) fn same_graph(&self, core: &Core) -> bool {
        self.graph_id == Rc::as_ptr(&core.graph) as usize
    }

    pub(crate) fn same_root(&self, other: &Self) -> bool {
        self.graph_id == other.graph_id
    }

    fn upgrade(&self) -> Option<Rc<RefCell<GraphCore>>> {
        self.graph.upgrade()
    }
}

pub(crate) fn boundary_root_for(target: &Core) -> BoundaryRoot {
    BoundaryRoot::from_core(target)
}

/// Reset `inside_run_wave` on scope exit — including unwind, so the feedback-cycle
/// panic (D37) leaves the flag clean per frame (mirrors TS's try/finally around
/// `dispatcher.invoke`). The *other* stale wave-flags an unwinding panic leaves on
/// the source / intermediate frames (`emitted_dirty` / `dep_batch` / `dep_dirty` /
/// `pending`) are cleaned at the wave-owner catch via the [`WaveScope`] touched-set
/// (closes B25) — this guard only owns `inside_run_wave`.
struct WaveGuard(DeferredNodeAction);
impl Drop for WaveGuard {
    fn drop(&mut self) {
        let _ = self.0.with_live_edges_mut(|e| {
            e.wave.inside_run_wave = false;
        });
    }
}

/// Clear `in_dep_mutation` on scope exit — including a panic unwind during a rewire
/// (QA-F1). A user fn can panic mid-rewire (an added dep's activation fn, or a removed
/// dep's onDeactivation hook); the wave-owner catch's wave-flag reset does NOT
/// cover `in_dep_mutation` (and may not include the rewiring node in its touched-set), so
/// without this RAII a caught rewire panic would wedge the node forever (every future
/// recompute deferred + every future rewire rejected as reentrant). Mirrors the TS arm's
/// `try { … } finally { _inDepMutation = false }`. `rewire_run_pending` needs no unwind
/// reset — it is reread/cleared at the start of the next `rewire` and has no other consumer.
struct DepMutationGuard(DeferredNodeAction);
impl Drop for DepMutationGuard {
    fn drop(&mut self) {
        let _ = self.0.with_live_edges_mut(|e| {
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
/// wave-flags before emitting `[[ERROR,e]]` from the
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
    owner_graph_id: usize,
    /// Process-local wave id used to mark arena nodes once per wave without a
    /// per-touch hash lookup (DR-8/B54 owner-execution hot path).
    wave_id: u64,
    /// Graph roots that received committed-boundary tasks during this wave but are not
    /// necessarily the wave owner's graph (nested cross-arena public calls share WAVE).
    boundary_roots: Vec<BoundaryRoot>,
    /// Every node that mutated a transient wave-flag this wave. DR-8/B54: these are
    /// arena pins keyed by (graph, node id, generation), not counted Core clones.
    touched: Vec<ArenaNodePin>,
    /// Innermost node whose fn ran before the panic (the ERROR-bearing node).
    ///
    /// DR-8/B54: blame is a generation-keyed arena reference on the hot path. The
    /// node is already pinned by `touched` because `run_wave` registers before
    /// invoking the fn; the cold panic path reconstructs the counted handle from
    /// that pin when it needs to emit ERROR.
    blamed: Option<ArenaNodeKey>,
}

impl WaveScope {
    fn pin_once(
        &mut self,
        arena_key: ArenaNodeKey,
        graph: &Rc<RefCell<GraphCore>>,
        refs: &Rc<Cell<usize>>,
    ) -> bool {
        {
            let mut g = graph.borrow_mut();
            if !g.is_live_key(arena_key.node) {
                return false;
            }
            let id = arena_key.node.id.0;
            if g.touched_waves.get(id) == Some(&self.wave_id) {
                return true;
            }
            g.pins[id] = g.pins[id]
                .checked_add(1)
                .expect("GraphCore pin count overflow");
            let touched = g
                .touched_waves
                .get_mut(id)
                .expect("Core points at a live GraphCore touch slot");
            *touched = self.wave_id;
        }
        self.touched.push(ArenaNodePin {
            arena_key,
            graph: graph.clone(),
            refs: refs.clone(),
        });
        true
    }
}

thread_local! {
    /// The current wave's scope, if a wave is in flight (single-thread, D22 ⇒
    /// thread-local, like the default dispatcher D26). `Some` ⇔ inside a wave.
    static WAVE: RefCell<Option<WaveScope>> = const { RefCell::new(None) };
    static NEXT_WAVE_SCOPE_ID: Cell<u64> = const { Cell::new(1) };
    /// Downstream delivery depth for one `down(msgs)` wave. A dep may receive
    /// multiple DATA messages before its dirty contribution is cleared; the fn
    /// must see the full batch once, not run once per DATA occurrence.
    static DELIVERY_DEPTH: Cell<usize> = const { Cell::new(0) };
    static NEXT_DELIVERY_ID: Cell<u64> = const { Cell::new(1) };
    static CURRENT_DELIVERY_ID: Cell<u64> = const { Cell::new(0) };
    static DEFERRED_DELIVERY_ACTIONS: RefCell<Vec<DeferredDeliveryAction>> = const { RefCell::new(Vec::new()) };
}

/// `true` while a wave is in flight on this thread (a scope is installed).
fn wave_in_flight() -> bool {
    WAVE.with(|w| w.borrow().is_some())
}

fn next_wave_scope_id() -> u64 {
    NEXT_WAVE_SCOPE_ID.with(|next| {
        let id = next.get();
        next.set(id.checked_add(1).expect("wave scope id overflow"));
        id
    })
}

/// Register a node as a wave participant (for the B25 touched-set reset). No-op if
/// no wave is in flight (e.g. a `cache` read outside any wave).
fn wave_register(core: &Core) {
    WAVE.with(|w| {
        if let Some(scope) = w.borrow_mut().as_mut() {
            let _ = scope.pin_once(core.arena_node_key(), &core.graph, &core.refs);
        }
    });
}

fn wave_register_boundary_root(core: &Core) {
    WAVE.with(|w| {
        if let Some(scope) = w.borrow_mut().as_mut() {
            if scope.owner_graph_id == Rc::as_ptr(&core.graph) as usize
                || scope.boundary_roots.iter().any(|r| r.same_graph(core))
            {
                return;
            }
            scope.boundary_roots.push(BoundaryRoot::from_core(core));
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
    defer_delivery_action_until_boundary(core, DeferredDeliveryKind::Run);
}

fn defer_absorbed_settle_until_delivery_boundary(core: &Core) {
    defer_delivery_action_until_boundary(core, DeferredDeliveryKind::AbsorbedSettle);
}

fn defer_delivery_action_until_boundary(core: &Core, kind: DeferredDeliveryKind) {
    DEFERRED_DELIVERY_ACTIONS.with(|actions| {
        let mut actions = actions.borrow_mut();
        if !actions.iter().any(|action| action.matches(kind, core)) {
            actions.push(DeferredDeliveryAction::from_core(kind, core));
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
        let actions = DEFERRED_DELIVERY_ACTIONS.with(|r| std::mem::take(&mut *r.borrow_mut()));
        if actions.is_empty() {
            break;
        }
        for kind in [
            DeferredDeliveryKind::Run,
            DeferredDeliveryKind::AbsorbedSettle,
        ] {
            for action in &actions {
                if action.kind == kind {
                    action.apply();
                }
            }
        }
    }
}

fn clear_deferred_delivery_actions() {
    DEFERRED_DELIVERY_ACTIONS.with(|actions| actions.borrow_mut().clear());
}

/// Record the node whose fn is about to run — the ERROR-bearing node on a panic.
fn wave_set_blamed(core: &Core) {
    WAVE.with(|w| {
        if let Some(scope) = w.borrow_mut().as_mut() {
            scope.blamed = Some(core.arena_node_key());
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
            owner_graph_id: Rc::as_ptr(&owner.graph) as usize,
            wave_id: next_wave_scope_id(),
            boundary_roots: Vec::new(),
            touched: Vec::new(),
            blamed: None,
        });
    });
    let result = catch_unwind(AssertUnwindSafe(body));
    let mut scope = WAVE
        .with(|w| w.borrow_mut().take())
        .expect("wave owner installed a scope");
    let boundary_roots = std::mem::take(&mut scope.boundary_roots);
    let ret = match result {
        Ok(r) => r,
        Err(payload) => {
            // The scope is taken (we are outside the wave again). Recover every node
            // the aborted wave corrupted (B25), then surface the failure as ERROR on a
            // node ON the cycle (D30) — a fresh terminal wave.
            for n in &scope.touched {
                n.reset_wave_flags();
            }
            // Delivery-boundary run/settle queues are wave-local. If the delivery
            // aborted before its guard could drain them, carrying those actions into a
            // later unrelated wave would resurrect stale dep projections after B25 has
            // already reset the touched nodes.
            clear_deferred_delivery_actions();
            let blamed = scope
                .blamed
                .and_then(|key| {
                    scope
                        .touched
                        .iter()
                        .find(|n| n.arena_key == key)
                        .and_then(ArenaNodePin::borrowed_core)
                })
                .unwrap_or_else(|| owner.borrowed_view());
            let err = panic_to_error(payload);
            // The recovery ERROR emit runs OUTSIDE any catch (the scope is taken). A
            // user subscriber sink that itself panics while handling this ERROR must not
            // escape the public API / fault the recovery — the node is going terminal
            // anyway. Swallow a sink-panic here (delivery may be partial; acceptable for
            // a node being torn down).
            let _ = catch_unwind(AssertUnwindSafe(move || {
                blamed.down(vec![Message::Error(err)]);
            }));
            clear_deferred_delivery_actions();
            on_error()
        }
    };
    drop(std::mem::take(&mut scope.touched));
    // Committed wave boundary (R-rewire-deferred / D47): drain any `ctx.rewire_next` queued
    // during this wave, each applied as a FRESH wave. Batch holds this drain until AFTER
    // the outermost commit/rollback, so topology never mutates on an uncommitted view
    // (R-rewire-batch-boundary / D67).
    if !boundary_drains_blocked() {
        drain_committed_boundaries(std::slice::from_ref(owner), &boundary_roots);
    }
    ret
}

struct UpRouteState {
    demand_fired: Vec<(LockId, ArenaNodeKey)>,
}

impl UpRouteState {
    fn new() -> Self {
        Self {
            demand_fired: Vec::new(),
        }
    }

    fn mark_demand(&mut self, lock: &LockId, holder: &Core) -> bool {
        let holder_key = holder.arena_node_key();
        if self
            .demand_fired
            .iter()
            .any(|(l, h)| l == lock && *h == holder_key)
        {
            return true;
        }
        self.demand_fired.push((lock.clone(), holder_key));
        false
    }
}

/// Queue a deferred boundary task (R-rewire-deferred / D47). Applied at the committed
/// wave boundary by [`drain_deferred_rewires`], never in place.
fn defer_boundary(owner: &Core, task: BoundaryTask) {
    wave_register_boundary_root(owner);
    register_boundary_root(owner);
    owner.graph.borrow_mut().deferred_boundary.push_back(task);
}

pub(crate) fn drain_committed_boundary(target: &Core) {
    drain_deferred_rewires(target);
}

pub(crate) fn drain_committed_boundary_root(root: &BoundaryRoot) {
    if let Some(graph) = root.upgrade() {
        drain_deferred_rewires_for_graph(&graph);
    }
}

pub(crate) fn drain_committed_boundaries(core_roots: &[Core], task_roots: &[BoundaryRoot]) {
    let mut escaped: Option<Box<dyn std::any::Any + Send>> = None;
    for root in core_roots {
        remember_first_boundary_panic(
            &mut escaped,
            catch_unwind(AssertUnwindSafe(|| drain_committed_boundary(root))),
        );
    }
    for root in task_roots {
        remember_first_boundary_panic(
            &mut escaped,
            catch_unwind(AssertUnwindSafe(|| drain_committed_boundary_root(root))),
        );
    }
    if let Some(e) = escaped {
        std::panic::resume_unwind(e);
    }
}

fn remember_first_boundary_panic(
    escaped: &mut Option<Box<dyn std::any::Any + Send>>,
    result: std::thread::Result<()>,
) {
    if let Err(e) = result {
        if escaped.is_none() {
            *escaped = Some(e);
        }
    }
}

pub(crate) fn clear_deferred_boundary_root(root: &BoundaryRoot) {
    if let Some(graph) = root.upgrade() {
        graph
            .borrow_mut()
            .deferred_boundary
            .retain(BoundaryTask::committed);
    }
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
    drain_deferred_rewires_for_graph(&owner.graph);
}

fn drain_deferred_rewires_for_graph(graph: &Rc<RefCell<GraphCore>>) {
    if graph.borrow().draining_boundary {
        return; // a nested wave-owner exit during the drain — the outer loop owns draining
    }
    if graph.borrow().deferred_boundary.is_empty() {
        return; // F-PERF: one empty-queue check per outermost wave when unused (behavior-neutral)
    }
    let drain_guard = BoundaryDrainGuard::enter(graph);
    let mut escaped: Option<Box<dyn std::any::Any + Send>> = None;
    let mut blocked = 0usize;
    loop {
        let task = {
            let mut g = graph.borrow_mut();
            if g.deferred_boundary.is_empty() {
                None
            } else {
                g.deferred_boundary.pop_front()
            }
        };
        let Some(task) = task else { break };
        match catch_unwind(AssertUnwindSafe(|| run_boundary_task(graph, task))) {
            Ok(Some(task)) => {
                let queued = {
                    let mut g = graph.borrow_mut();
                    g.deferred_boundary.push_back(task);
                    g.deferred_boundary.len()
                };
                blocked += 1;
                if blocked >= queued {
                    break;
                }
            }
            Ok(None) => {
                blocked = 0;
            }
            Err(e) => {
                blocked = 0;
                if escaped.is_none() {
                    escaped = Some(e);
                }
            }
        }
    }
    drain_guard.finish();
    if let Some(e) = escaped {
        std::panic::resume_unwind(e);
    }
}

fn run_boundary_task(graph: &Rc<RefCell<GraphCore>>, task: BoundaryTask) -> Option<BoundaryTask> {
    match task {
        BoundaryTask::Rewire {
            target,
            req,
            committed,
        } => {
            if !committed.get() {
                return None;
            }
            if let Some(node) = target.pinned_borrowed_core(graph) {
                if node.core.is_paused() {
                    return Some(BoundaryTask::Rewire {
                        target,
                        req,
                        committed,
                    });
                }
                node.core.apply_rewire_next(req);
            }
        }
        BoundaryTask::ExternalRewire {
            target,
            new_deps,
            fn_,
            committed,
        } => {
            if committed.get() {
                if let Some(node) = target.pinned_borrowed_core(graph) {
                    node.core.rewire_inner(new_deps, fn_, false);
                }
            }
        }
        BoundaryTask::Up {
            target,
            msgs,
            toward_dep,
            committed,
        } => {
            if !committed.get() {
                return None;
            }
            if let Some(node) = target.pinned_borrowed_core(graph) {
                if node.core.is_paused() {
                    return Some(BoundaryTask::Up {
                        target,
                        msgs,
                        toward_dep,
                        committed,
                    });
                }
                node.core.owned_up(msgs, toward_dep);
            }
        }
    }
    None
}

impl Core {
    pub(crate) fn ptr_eq(&self, other: &Core) -> bool {
        Rc::ptr_eq(&self.graph, &other.graph)
            && self.id == other.id
            && self.generation == other.generation
    }

    pub(crate) fn same_graph(&self, other: &Core) -> bool {
        Rc::ptr_eq(&self.graph, &other.graph)
    }

    pub(crate) fn same_graph_arena(&self, arena: &GraphArena) -> bool {
        Rc::ptr_eq(&self.graph, &arena.0)
    }

    pub(crate) fn arena(&self) -> GraphArena {
        GraphArena(self.graph.clone())
    }

    pub(crate) fn dispatcher(&self) -> Dispatcher {
        self.with_call(|c| c.dispatcher.clone())
    }

    pub(crate) fn deps(&self) -> Vec<Core> {
        self.borrow().deps.clone()
    }

    pub(crate) fn set_topology_deps_changed_observer(&self, observer: TopologyDepsChangedObserver) {
        self.with_node_state_mut(|n, _c, _cfg, _r, _e| {
            n.topology_deps_changed = Some(observer);
        });
    }

    fn notify_topology_deps_changed(&self, old_deps: &[Core], new_deps: &[Core]) {
        let observer = self.with_inner_edges(|n, _e| n.topology_deps_changed.clone());
        if let Some(observer) = observer {
            observer(old_deps, new_deps);
        }
    }

    pub(crate) fn cache_any(&self) -> Option<AnyValue> {
        self.with_inner_edges(|_n, e| e.value.cache.clone())
    }

    pub(crate) fn status(&self) -> Status {
        self.with_inner_edges(|_n, e| e.value.status)
    }

    pub(crate) fn version(&self) -> Option<NodeVersion> {
        self.graph.borrow().get_version(self.key()).value.clone()
    }

    pub(crate) fn versioning_policy(&self) -> ResolvedNodeVersioningPolicy {
        self.graph.borrow().get_version(self.key()).policy.clone()
    }

    pub(crate) fn handle(&self) -> Option<Handle> {
        self.with_call(|c| c.handle)
    }

    pub(crate) fn runtime_is_quiescent_for_release(&self) -> bool {
        if wave_in_flight() || delivery_in_flight() {
            return false;
        }
        let g = self.graph.borrow();
        let key = self.key();
        if !g.is_live_key(key) {
            return true;
        }
        if g.pin_count(key) != 0 || g.draining_boundary || !g.deferred_boundary.is_empty() {
            return false;
        }
        let cfg = g.get_config(key);
        let e = g
            .edge_slots
            .get(key.id.0)
            .and_then(Option::as_ref)
            .expect("Core points at a live GraphCore edge slot");
        let a = g.get_aux(key);
        let allowed_pause_lock = cfg
            .pull_id
            .as_ref()
            .is_some_and(|id| a.pause_lockset.len() == 1 && a.pause_lockset.contains(id));
        e.state.pending == 0
            && !e.wave.inside_run_wave
            && !e.wave.in_dep_mutation
            && !e.wave.rewire_run_pending
            && !e.wave.batch_dirty_owed
            && !a.pull_demand_owed
            && a.pause_buffer.is_empty()
            && (a.pause_lockset.is_empty() || allowed_pause_lock)
    }

    pub(crate) fn is_quiescent_for_release(&self) -> bool {
        self.runtime_is_quiescent_for_release() && self.subscriber_count() == 0
    }

    pub(crate) fn release_runtime_for_graph(&self) -> bool {
        if !self.is_quiescent_for_release() {
            return false;
        }
        let key = self.key();
        let slot = {
            let mut graph = self.graph.borrow_mut();
            if graph.pin_count(key) != 0 {
                return false;
            }
            let Some(slot) = graph.take_live(key) else {
                self.refs.set(0);
                return true;
            };
            slot
        };
        self.refs.set(0);
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
            graph: self.graph.clone(),
            id: key.id.0,
            armed: true,
        };
        let (mut inner, _topology, mut call, _config, _run, mut edges, _version, mut aux) = slot;
        inner.cleanup_before_free(&mut call, &mut edges, &mut aux);
        free_slot.armed = false;
        self.graph.borrow_mut().free.push(key.id.0);
        true
    }

    pub(crate) fn checkpoint_runtime(&self) -> NodeCheckpointRuntime {
        let version = self.version();
        self.with_node_state_aux_mut(|_n, _c, _cfg, r, e, a| NodeCheckpointRuntime {
            cache: e.value.cache.clone(),
            has_data: e.value.has_data,
            version,
            status: e.value.status,
            terminal: e.value.terminal,
            activated: a.activated,
            has_called_fn_once: r.has_called_fn_once,
            ctx_state: a.state.clone(),
            ctx_state_persist: a.state_persist,
        })
    }

    pub(crate) fn restore_runtime(&self, state: NodeRestoreRuntime) {
        self.with_node_state_aux_mut(|_n, _c, cfg, r, e, a| {
            e.value.cache = state.cache;
            e.value.has_data = state.has_data;
            e.value.status = state.status;
            e.value.terminal = state.terminal;
            e.wave = NodeWaveState::new();
            e.state = DepState::new(e.unsubs.len());
            e.restored_activation_handshake =
                vec![state.has_data || state.has_called_fn_once; e.unsubs.len()];
            r.has_called_fn_once = state.has_called_fn_once;
            a.activated = false;
            a.state = state.ctx_state;
            a.state_persist = state.ctx_state_persist;
            a.on_deactivation.clear();
            a.on_invalidate.clear();
            a.pull_demand_owed = false;
            a.pause_lockset.clear();
            if let Some(lock) = cfg.pull_id.clone() {
                a.pause_lockset.insert(lock);
            }
            a.paused_dep_wave_occurred = false;
            a.pause_buffer.clear();
        });
        let mut graph = self.graph.borrow_mut();
        let version_state = graph.get_version_mut(self.key());
        match state.version {
            RestoredNodeVersion::Disabled => {
                version_state.policy = ResolvedNodeVersioningPolicy::Disabled;
                version_state.value = None;
            }
            RestoredNodeVersion::Version(version) => {
                if matches!(version, NodeVersion::V0 { .. }) {
                    version_state.policy = ResolvedNodeVersioningPolicy::Level0;
                }
                version_state.value = Some(version);
            }
        }
    }

    pub(crate) fn local_async_driver(
        &self,
    ) -> Option<Rc<dyn crate::async_driver::LocalAsyncDriver>> {
        self.with_call(|c| c.environment.local_async_driver())
    }

    pub(crate) fn environment(&self) -> EnvironmentDrivers {
        self.with_call(|c| c.environment.clone())
    }

    pub(crate) fn set_environment(&self, environment: EnvironmentDrivers) {
        self.with_call_mut(|c| {
            c.environment = environment;
        });
    }

    pub(crate) fn factory(&self) -> Option<String> {
        self.with_call(|c| c.factory.clone())
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
        opts: NodeOpts,
    ) -> Core {
        for dep in &deps {
            assert!(
                dep.same_graph_arena(arena),
                "node construction: dep belongs to a different graph; cross-graph deps require a wire bridge (D22/R-graph-domain)"
            );
        }
        let topology = NodeTopologySlot {
            deps,
            topology_deps_changed: None,
        };
        let inner = NodeInner;
        let mut environment = EnvironmentDrivers::default();
        environment.set_local_async_driver(dispatcher.local_async_driver());
        let call = NodeCallSlot {
            factory: None,
            handle,
            dispatcher,
            environment,
        };
        let config = NodeConfigSlot::from_opts(&opts);
        let version = NodeVersionState::new(config.versioning.clone(), initial.as_ref());
        {
            let mut g = arena.0.borrow_mut();
            let id = g.alloc(inner, topology, call, config, version);
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
            self.with_node_state_aux_mut(|_n, _c, cfg, _r, _e, a| {
                a.pause_lockset.insert(id.clone());
                cfg.pull_id = Some(id);
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
            self.with_node_state_aux_mut(|_n, _c, cfg, _r, e, a| {
                let id = a.next_sub_id;
                a.next_sub_id += 1;
                a.subscribers.push((id, sink.clone()));
                let push = if cfg.pull_id.is_some() {
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
        let deps = self.with_node_state_aux_mut(|n, _c, cfg, _r, e, a| {
            a.activated = true;
            if let Some(id) = cfg.pull_id.clone() {
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
        for (idx, dep) in deps.iter().enumerate() {
            self.subscribe_dep(idx, dep);
        }
        // Depless producer (fn, no deps): run once on activation.
        let run = self.with_node_state(|n, c, _cfg, r, _e| {
            n.deps.is_empty() && c.handle.is_some() && !r.has_called_fn_once
        });
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
            e.restored_activation_handshake[idx] = false;
        });
    }

    fn deactivate(&self) {
        let (unsubs, hooks) = self.with_node_state_aux_mut(|n, c, _cfg, r, e, a| {
            a.activated = false;
            let unsubs: Vec<Unsub> = e.unsubs.drain(..).flatten().collect();
            e.idx_boxes.clear();
            let hooks: Vec<Box<dyn FnOnce()>> = std::mem::take(&mut a.on_deactivation);
            // RAM: a compute node (fn and/or deps) clears its cache on deactivation;
            // a depless state node retains it (ROM).
            let is_compute = c.handle.is_some() || !n.deps.is_empty();
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
            e.restored_activation_handshake.fill(false);
            e.state.pending = 0;
            e.wave.emitted_dirty_this_wave = false;
            r.has_called_fn_once = false;
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

    #[cfg(test)]
    fn receive_from_dep(&self, idx: usize, msg: &Msg) {
        // B25: enroll in the wave touched-set so a panic-abort can reset our flags.
        // DR-8/B54: the internal dep-callback entry may be an uncounted arena view;
        // under a real wave this generation-keyed touched action pins the node in the
        // owner arena while borrow-free subscriber callbacks run.
        wave_register(self);
        self.receive_from_dep_registered(idx, msg);
    }

    fn receive_from_dep_registered(&self, idx: usize, msg: &Msg) {
        let Some(action) = self.collect_receive_from_dep_action(idx, msg) else {
            return;
        };
        if action.has_work() {
            self.apply_receive_from_dep_action(action);
        }
    }

    fn collect_receive_from_dep_action(
        &self,
        idx: usize,
        msg: &Msg,
    ) -> Option<InWaveDepReceiveAction> {
        let mut g = self.graph.borrow_mut();
        Self::collect_receive_from_dep_action_from_graph(&mut g, self.key(), idx, msg)
    }

    fn decide_maybe_run_from_graph(g: &mut GraphCore, key: NodeKey) -> MaybeRunDecision {
        let (_n, c, cfg, r, e, a) = g.get_node_call_config_run_edges_aux_mut(key);

        // R-rewire (D42): an added cached dep's push-on-subscribe lands here mid-mutation.
        // Defer fn-run to ONE atomic settle after every added dep is wired.
        if e.wave.in_dep_mutation {
            e.wave.rewire_run_pending = true;
            return MaybeRunDecision::Skip;
        }

        // R-pause-modes: only the default "true" mode coalesces dep-driven recompute
        // (skip the fn while any lock is held → fire once with latest dep values on
        // final-lock RESUME). `resumeAll` RUNS the fn while paused but buffers output
        // (`down` → `pause_buffer`). `false` runs + emits immediately.
        if matches!(cfg.pausable, Pausable::True) && !a.pause_lockset.is_empty() {
            a.paused_dep_wave_occurred = true;
            return MaybeRunDecision::Skip;
        }
        if e.state.pending > 0 {
            return MaybeRunDecision::Skip;
        }
        if c.handle.is_none() {
            return MaybeRunDecision::Passthrough;
        }
        if !(r.has_called_fn_once || cfg.partial || cfg.all_deps_settled(&e.state)) {
            return MaybeRunDecision::Skip;
        }

        MaybeRunDecision::Run
    }

    fn set_maybe_run_action(g: &mut GraphCore, key: NodeKey, action: &mut InWaveDepReceiveAction) {
        if delivery_in_flight() {
            action.defer_maybe_run = true;
        } else {
            action.maybe_run_decision = Self::decide_maybe_run_from_graph(g, key);
        }
    }

    fn apply_receive_from_dep_action(&self, action: InWaveDepReceiveAction) {
        action.apply(self);
    }

    fn collect_receive_from_dep_action_from_graph(
        g: &mut GraphCore,
        key: NodeKey,
        idx: usize,
        msg: &Msg,
    ) -> Option<InWaveDepReceiveAction> {
        let (pausable, pull_id, auto_error, auto_complete, terminal_as_real_input) = {
            let cfg = g.get_config(key);
            (
                cfg.pausable,
                cfg.pull_id.clone(),
                cfg.error_when_deps_error,
                cfg.complete_when_deps_complete,
                cfg.terminal_as_real_input,
            )
        };
        if g.get_node_and_edges_mut(key).1.value.terminal {
            if matches!(msg, Message::Teardown) {
                return Some(InWaveDepReceiveAction {
                    emit_teardown_subscribers: true,
                    ..Default::default()
                });
            }
            return None;
        }

        let mut action = InWaveDepReceiveAction::default();
        match msg {
            Message::Start => {}
            Message::Invalidate => {
                let (had_data, undirty_no_settle, paused) = {
                    let (_n, e, a) = g.get_node_edges_aux_mut(key);
                    e.state.prev[idx] = None;
                    e.state.has_data[idx] = false;
                    e.state.batch[idx] = None;
                    let had_current_projection = e.state.record_wave_sentinel(idx);
                    if !had_current_projection && !e.state.has_run_triggering_projection() {
                        e.state.clear_wave_projection(idx);
                    }
                    if e.state.dirty[idx] {
                        e.state.dirty[idx] = false;
                        e.state.pending -= 1;
                    }
                    if a.paused_dep_wave_occurred && e.state.batch.iter().all(|b| b.is_none()) {
                        a.paused_dep_wave_occurred = false;
                    }
                    (
                        e.value.has_data,
                        e.state.pending == 0 && e.wave.emitted_dirty_this_wave,
                        !a.pause_lockset.is_empty(),
                    )
                };
                let buffer_own_invalidate = paused && matches!(pausable, Pausable::ResumeAll);
                action.had_node_data_before_invalidate = had_data;
                action.do_invalidate = true;
                action.invalidate_as_invalidate_msg = buffer_own_invalidate && had_data;
                if undirty_no_settle {
                    if !had_data {
                        action.down_msgs = Some(vec![Message::Resolved]);
                    } else {
                        action.clear_undirty_after_invalidate = true;
                    }
                }
                action.fire_demand = true;
            }
            Message::Dirty => {
                action.emit_dirty = {
                    let pull_id = pull_id.clone();
                    let (_n, e, a) = g.get_node_edges_aux_mut(key);
                    if e.state.dirty[idx] {
                        false
                    } else {
                        e.state.dirty[idx] = true;
                        e.state.pending += 1;
                        e.state.tier[idx] = 2;
                        let pull_quiet = pull_id.is_some_and(|id| a.pause_lockset.contains(&id));
                        if pull_quiet || e.wave.emitted_dirty_this_wave {
                            false
                        } else {
                            e.wave.emitted_dirty_this_wave = true;
                            e.value.status = Status::Dirty;
                            true
                        }
                    }
                };
            }
            Message::Data(v) => {
                if g.get_node_and_edges_mut(key)
                    .1
                    .restored_activation_handshake[idx]
                {
                    let (_n, e) = g.get_node_and_edges_mut(key);
                    e.state.prev[idx] = Some(v.clone());
                    e.state.has_data[idx] = true;
                    e.state.tier[idx] = 3;
                    e.state.batch[idx] = None;
                    return None;
                }
                let pending_drained = {
                    let (_n, e, a) = g.get_node_edges_aux_mut(key);
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
                    let pending_drained = e.state.pending == 0;
                    if !pending_drained
                        && matches!(pausable, Pausable::True)
                        && !e.wave.in_dep_mutation
                        && !a.pause_lockset.is_empty()
                    {
                        a.paused_dep_wave_occurred = true;
                    }
                    pending_drained
                };
                if pending_drained {
                    Self::set_maybe_run_action(g, key, &mut action);
                    action.fire_demand = true;
                }
            }
            Message::Resolved => {
                let pending_drained = {
                    let (_n, e, a) = g.get_node_edges_aux_mut(key);
                    e.state.record_resolved_wave(idx);
                    e.state.tier[idx] = 3;
                    if e.state.dirty[idx] {
                        e.state.dirty[idx] = false;
                        e.state.pending -= 1;
                    }
                    let pending_drained = e.state.pending == 0;
                    if !pending_drained
                        && matches!(pausable, Pausable::True)
                        && !e.wave.in_dep_mutation
                        && !a.pause_lockset.is_empty()
                    {
                        a.paused_dep_wave_occurred = true;
                    }
                    pending_drained
                };
                if pending_drained {
                    Self::set_maybe_run_action(g, key, &mut action);
                    action.fire_demand = true;
                }
            }
            m if m.tier() == Tier::Terminal => {
                // Box<dyn Error> is not Clone and `m` is a borrow (D31): keep the error as its
                // Display message in the DepTerminal record (Rc<str>, which IS Clone).
                let dep_term = match m {
                    Message::Error(e) => DepTerminal::Error(format!("{e}").into()),
                    _ => DepTerminal::Complete,
                };
                let is_error = matches!(dep_term, DepTerminal::Error(_));
                let all_deps_terminal = {
                    let (n, e) = g.get_node_and_edges_mut(key);
                    e.state.terminal[idx] = Some(dep_term.clone());
                    e.state.terminal_wave[idx] = Some(dep_term.clone());
                    if e.state.dirty[idx] {
                        e.state.dirty[idx] = false;
                        e.state.pending -= 1;
                    }
                    !n.deps.is_empty() && e.state.terminal.iter().all(|t| t.is_some())
                };
                if is_error && auto_error {
                    if let DepTerminal::Error(s) = &dep_term {
                        action.down_msgs = Some(vec![Message::Error(s.to_string().into())]);
                    }
                } else if terminal_as_real_input {
                    Self::set_maybe_run_action(g, key, &mut action);
                } else if auto_complete && all_deps_terminal {
                    action.down_msgs = Some(vec![Message::Complete]);
                } else {
                    action.do_settle_after_absorbed_terminal = true;
                }
                action.fire_demand = true;
            }
            Message::Teardown => {
                action.down_msgs = Some(vec![Message::Complete, Message::Teardown]);
            }
            // PAUSE/RESUME are never delivered to a dep-subscriber (a node is paused via its
            // own up(), not by an upstream dep).
            _ => {}
        }
        if action.has_work() {
            Some(action)
        } else {
            None
        }
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
        let subs = self.with_inner_edges_aux_mut(|_n, e, a| {
            e.value.status = Status::Dirty;
            if !e.wave.emitted_dirty_this_wave {
                e.wave.emitted_dirty_this_wave = true;
                snapshot_subscribers(a)
            } else {
                SubscriberSnapshot::Empty
            }
        });
        deliver_subscriber_snapshot(subs, &Message::Dirty);
    }

    fn maybe_run(&self) {
        if delivery_in_flight() {
            defer_run_until_delivery_boundary(self);
            return;
        }
        let decision = {
            let mut g = self.graph.borrow_mut();
            Self::decide_maybe_run_from_graph(&mut g, self.key())
        };
        match decision {
            MaybeRunDecision::Skip => {}
            MaybeRunDecision::Passthrough => self.passthrough_emit(),
            MaybeRunDecision::Run => self.run_wave(),
        }
    }

    fn try_run(&self) {
        let (pending, has_handle, has_called, partial, all_settled) =
            self.with_node_state(|_n, c, cfg, r, e| {
                (
                    e.state.pending,
                    c.handle.is_some(),
                    r.has_called_fn_once,
                    cfg.partial,
                    cfg.all_deps_settled(&e.state),
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

    /// Runtime topology rewire — the public substrate entry (called by `Node::replace_deps`/
    /// `subscribe_dep`/`unsubscribe_dep`). The validation REJECTS run first and panic: called
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
            if let Some(committed) = committed_after_batch_for_target(self) {
                defer_boundary(
                    self,
                    BoundaryTask::ExternalRewire {
                        target: CoreToken::from_core(self),
                        new_deps,
                        fn_,
                        committed,
                    },
                );
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
                "rewire: reentrant dep mutation — another replace_deps/subscribe_dep/unsubscribe_dep is in flight (R-rewire)"
            );
        }
        assert!(
            !new_deps.iter().any(|d| d.ptr_eq(self)),
            "rewire: self-dependency rejected (R-rewire / D42)"
        );
        let old_deps = self.borrow().deps.clone();
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
        with_wave_owner(self, || self.rewire_apply(new_deps, fn_), || {});
    }

    // ── deferred self-rewire (R-rewire-deferred / D47): ctx.rewire_next ──

    /// Enqueue a deferred self-rewire (`ctx.rewire_next`). Applied at the committed wave
    /// boundary by the wave-owner drain, NEVER in place — the immediate `replace_deps`/`subscribe_dep`/
    /// `unsubscribe_dep` still panics mid-fn (D37/R-reentrancy). The drain runs each as a fresh
    /// wave; this is the substrate affordance the higher-order *Map operators wire inners with.
    pub(crate) fn request_rewire_next(&self, req: RewireRequest) {
        defer_boundary(
            self,
            BoundaryTask::Rewire {
                target: CoreToken::from_core(self),
                req,
                committed: active_batch_committed_token()
                    .unwrap_or_else(|| Rc::new(Cell::new(true))),
            },
        );
    }

    pub(crate) fn request_up_next(&self, msgs: Wave<AnyValue>, toward_dep: Option<usize>) {
        defer_boundary(
            self,
            BoundaryTask::Up {
                target: CoreToken::from_core(self),
                msgs,
                toward_dep,
                committed: active_batch_committed_token()
                    .unwrap_or_else(|| Rc::new(Cell::new(true))),
            },
        );
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
                let mut next = self.deps();
                if !next.iter().any(|d| d.ptr_eq(&dep)) {
                    next.push(dep);
                }
                (next, f)
            }
            RewireRequest::Remove(dep, f) => {
                let current = self.borrow().deps.clone();
                if !current.iter().any(|d| dep.matches(d)) {
                    return;
                }
                let next: Vec<Core> = current.into_iter().filter(|d| !dep.matches(d)).collect();
                (next, f)
            }
        };
        // rewire() dedups + validates + applies under its own wave-owner (a fn-panic during the
        // apply becomes ERROR on the blamed node there). Only the PRE-apply validation rejects
        // panic OUT of rewire() — caught here → ERROR on this node, via owned_down (a fresh
        // wave-owner; the drain runs outside any live wave).
        let outcome = catch_unwind(AssertUnwindSafe(|| self.rewire_inner(new_deps, fn_, true)));
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
        let old_deps = self.borrow().deps.clone();
        if same_ordered_deps(&old_deps, &new_deps) {
            self.with_inner_edges_mut(|_, e| {
                e.wave.in_dep_mutation = true;
                e.wave.rewire_run_pending = false;
            });
            let _mut_guard = DepMutationGuard(DeferredNodeAction::from_core(self));
            self.swap_fn_preserving_pool(fn_);
            self.with_inner_edges_mut(|_, e| {
                e.wave.in_dep_mutation = false;
            });
            return;
        }
        if old_deps.len() == new_deps.len() && Self::same_graph_no_overlap(&old_deps, &new_deps) {
            // DR-8/B54 no-kept fast path: full dep replacement (same arity, no shared
            // identity) keeps this in the same O(n) class as the old keep-path, but bypasses
            // full O(n²) matching when no dep survives.
            let n = new_deps.len();
            self.with_inner_edges_mut(|_, e| {
                e.wave.in_dep_mutation = true;
                e.wave.rewire_run_pending = false;
            });
            let _mut_guard = DepMutationGuard(DeferredNodeAction::from_core(self));
            let activated = self.with_aux(|a| a.activated);
            let mut removed_dirty_contributor = false;

            self.swap_fn_preserving_pool(fn_);

            for old_idx in 0..old_deps.len() {
                let unsub = {
                    let mut graph = self.graph.borrow_mut();
                    let (_nn, edges) = graph.get_node_and_edges_mut(self.key());
                    if edges.state.dirty[old_idx] {
                        removed_dirty_contributor = true;
                        edges.state.pending -= 1;
                    }
                    if let Some(box_ref) = edges.idx_boxes.get(old_idx) {
                        box_ref.set(-1);
                    }
                    edges.unsubs.get_mut(old_idx).and_then(|u| u.take())
                };
                if let Some(unsub) = unsub {
                    unsub();
                }
            }

            let new_batch: Vec<Option<Vec<AnyValue>>> = (0..n).map(|_| None).collect();
            let new_batch_waves: Vec<Vec<Vec<WaveData>>> = (0..n).map(|_| Vec::new()).collect();
            let new_batch_wave_id: Vec<Option<u64>> = (0..n).map(|_| None).collect();
            let new_prev: Vec<Option<AnyValue>> = (0..n).map(|_| None).collect();
            let new_tier: Vec<u8> = (0..n).map(|_| 0).collect();
            let new_terminal: Vec<Option<DepTerminal>> = (0..n).map(|_| None).collect();
            let new_terminal_wave: Vec<Option<DepTerminal>> = (0..n).map(|_| None).collect();
            let new_has: Vec<bool> = (0..n).map(|_| false).collect();
            let new_dirty: Vec<bool> = (0..n).map(|_| false).collect();
            let new_unsubs: Vec<Option<Unsub>> = (0..n).map(|_| None).collect();
            let new_boxes: Vec<Rc<Cell<i64>>> = (0..n).map(|_| Rc::new(Cell::new(-1i64))).collect();
            let new_restored_handshake: Vec<bool> = vec![false; n];
            self.with_inner_edges_mut(|nn, e| {
                nn.deps = new_deps.clone();
                e.state.batch = new_batch;
                e.state.batch_waves = new_batch_waves;
                e.state.batch_wave_id = new_batch_wave_id;
                e.state.prev = new_prev;
                e.state.has_data = new_has;
                e.state.dirty = new_dirty;
                e.state.tier = new_tier;
                e.state.terminal = new_terminal;
                e.state.terminal_wave = new_terminal_wave;
                e.unsubs = new_unsubs;
                e.idx_boxes = new_boxes;
                e.restored_activation_handshake = new_restored_handshake;
            });

            if activated {
                for (idx, dep) in new_deps.iter().enumerate() {
                    self.subscribe_dep(idx, dep);
                }
            }
            self.notify_topology_deps_changed(&old_deps, &new_deps);

            if self.with_inner_edges(|_, e| e.value.terminal) {
                return;
            }

            let should_rewire_settle = self.with_inner_edges(|_, e| {
                removed_dirty_contributor && e.state.pending == 0 && e.value.status == Status::Dirty
            });
            let mut zero_dep_undirty = false;
            if should_rewire_settle {
                if n == 0 {
                    zero_dep_undirty = true;
                } else {
                    self.with_inner_edges_mut(|_, e| {
                        e.wave.rewire_run_pending = true;
                    });
                }
            }

            self.with_inner_edges_mut(|_, e| {
                e.wave.in_dep_mutation = false;
            });

            let run_pending =
                self.with_inner_edges_mut(|_, e| std::mem::take(&mut e.wave.rewire_run_pending));
            if run_pending {
                self.settle_rewire();
            } else if zero_dep_undirty {
                let emitted = self.with_inner_edges(|_, e| e.wave.emitted_dirty_this_wave);
                if emitted {
                    self.down(vec![Message::Resolved]);
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
            return;
        }
        let n = new_deps.len();
        let mut old_to_new: Vec<Option<usize>> = vec![None; old_deps.len()];
        let mut new_to_old: Vec<Option<usize>> = vec![None; n];
        if old_deps.len().saturating_mul(n) <= 64 {
            for (old_idx, old_dep) in old_deps.iter().enumerate() {
                for (new_idx, new_dep) in new_deps.iter().enumerate() {
                    if old_dep.ptr_eq(new_dep) && new_to_old[new_idx].is_none() {
                        old_to_new[old_idx] = Some(new_idx);
                        new_to_old[new_idx] = Some(old_idx);
                        break;
                    }
                }
            }
        } else {
            let mut first_old_by_key: HashMap<ArenaNodeKey, usize> =
                HashMap::with_capacity(old_deps.len());
            for (old_idx, old_dep) in old_deps.iter().enumerate() {
                first_old_by_key
                    .entry(old_dep.arena_node_key())
                    .or_insert(old_idx);
            }
            for (new_idx, new_dep) in new_deps.iter().enumerate() {
                if let Some(old_idx) = first_old_by_key.remove(&new_dep.arena_node_key()) {
                    old_to_new[old_idx] = Some(new_idx);
                    new_to_old[new_idx] = Some(old_idx);
                }
            }
        }
        let added: Vec<(usize, Core)> = new_deps
            .iter()
            .enumerate()
            .filter(|(idx, _)| new_to_old[*idx].is_none())
            .map(|(idx, dep)| (idx, dep.clone()))
            .collect();
        let removed: Vec<usize> = old_deps
            .iter()
            .enumerate()
            .filter(|(idx, _)| old_to_new[*idx].is_none())
            .map(|(idx, _)| idx)
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
        let _mut_guard = DepMutationGuard(DeferredNodeAction::from_core(self));
        let activated = self.with_aux(|a| a.activated);
        let mut zero_dep_undirty = false;

        // fn swap (SD-1 + B32 GC): register the new fn in the SAME pool, then unregister the
        // old handle so the rewired-away closure (and its captures) is freed and its slot
        // reused. Register-first keeps `handle` never pointing at a freed slot.
        self.swap_fn_preserving_pool(fn_);

        // removed deps: clear dirty contribution + drain (box→-1, unsubscribe the edge).
        let mut removed_dirty_contributor = false;
        for old_idx in &removed {
            let old_idx = *old_idx;
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
            for (j, old_idx) in new_to_old.iter().enumerate() {
                if let Some(old_idx) = *old_idx {
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
            edges.restored_activation_handshake = vec![false; n];
        }

        // subscribe added deps — push-on-subscribe delivers a cached dep's DATA (driving
        // maybe_run, which DEFERS to the atomic settle via in_dep_mutation); a SENTINEL dep
        // delivers START only. Only when activated (else activation will subscribe later).
        if activated {
            for (idx, d) in &added {
                self.subscribe_dep(*idx, d);
            }
        }
        self.notify_topology_deps_changed(&old_deps, &new_deps);

        // D62 / R-rewire-deferred: a terminal owner still drains queued topology intent,
        // but terminal-is-forever remains an output guard. Apply the dep-set/fn cleanup
        // above, then skip every post-mutation settle path so no post-terminal DIRTY,
        // DATA, RESOLVED, INVALIDATE, COMPLETE, or ERROR can escape.
        if self.with_inner_edges(|_, e| e.value.terminal) {
            return;
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

    fn same_graph_no_overlap(old_deps: &[Core], new_deps: &[Core]) -> bool {
        let mut old_ids = HashSet::with_capacity(old_deps.len());
        for dep in old_deps {
            old_ids.insert(dep.arena_node_key());
        }
        !new_deps
            .iter()
            .any(|dep| old_ids.contains(&dep.arena_node_key()))
    }

    /// R-rewire atomic settle (D42 + the D1 /qa fix): emit ONE proper two-phase
    /// DIRTY→DATA wave after a rewire that warrants a recompute. Mirrors `try_run`'s
    /// pause + gate + pending guards, then injects the phase-1 DIRTY the added dep's
    /// [START,DATA] handshake did not carry (R-dirty-before-data).
    fn settle_rewire(&self) {
        if self.with_config(|cfg| matches!(cfg.pausable, Pausable::True)) && self.is_paused() {
            self.with_aux_mut(|a| a.paused_dep_wave_occurred = true);
            return;
        }
        let (pending, has_handle, has_called, partial, all_settled) =
            self.with_node_state(|_n, c, cfg, r, e| {
                (
                    e.state.pending,
                    c.handle.is_some(),
                    r.has_called_fn_once,
                    cfg.partial,
                    cfg.all_deps_settled(&e.state),
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
        self.mark_dirty(); // phase 1 (no-op if already dirty, e.g. unsubscribe_dep auto-settle)
        self.run_wave(); // phase 2: fn → DATA / undirty RESOLVED
    }

    fn swap_fn_preserving_pool(&self, fn_: NodeFn) {
        let (old_handle, disp) = { self.with_call(|call| (call.handle, call.dispatcher.clone())) };
        let kind = old_handle
            .map(|h| disp.pool_kind(h.pool_id))
            .unwrap_or(PoolKind::Sync);
        let new_handle = register_with(&disp, kind, fn_);
        self.with_node_state_mut(|_n, call, _cfg, _r, _e| {
            call.handle = Some(new_handle);
        });
        if let Some(oh) = old_handle {
            disp.unregister(oh);
        }
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
        // Build ctx + call snapshot under one short arena borrow, then invoke
        // borrow-free through the dispatcher (R-dispatch-all / R-sync-core).
        let (handle, dispatcher, is_async, ctx, old_on_invalidate, old_on_deactivation) = self
            .with_node_state_aux_mut(|n, c, _cfg, r, e, a| {
                let dep_records: Vec<DepRecord> = (0..n.deps.len())
                    .map(|i| {
                        let latest = e.state.batch[i]
                            .as_ref()
                            .and_then(|b| b.last().cloned())
                            .or_else(|| e.state.prev[i].clone());
                        let wave_data = std::mem::take(&mut e.state.batch_waves[i]);
                        e.state.batch_wave_id[i] = None;
                        DepRecord {
                            wave_data,
                            prev_data: e.state.prev[i].clone(),
                            latest,
                            terminal: e.state.terminal_wave[i].take(),
                        }
                    })
                    .collect();
                r.has_called_fn_once = true;
                e.wave.inside_run_wave = true;
                e.wave.emitted_tier3_this_wave = false;
                (
                    c.handle.expect("run_wave ⇒ handle present"),
                    c.dispatcher.clone(),
                    c.handle
                        .map(|h| c.dispatcher.pool_kind(h.pool_id) == PoolKind::Async)
                        .unwrap_or(false),
                    Ctx::new(self.borrowed_view(), dep_records),
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
        let _guard = WaveGuard(DeferredNodeAction::from_core(self));
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
        let undirty = self.with_inner_edges(|_n, e| {
            // EXEMPT a TERMINAL wave (the fn emitted COMPLETE/ERROR): the terminal IS the settle,
            // and R-terminal-settles-dirty releases the downstream dirty — synthesizing an undirty
            // RESOLVED here would overwrite the terminal status + emit a spurious post-terminal
            // RESOLVED (mirrors the TS `_terminal === undefined` guard, node.ts). Pre-existing
            // parity gap surfaced by C-11 (a fn emitting a bare COMPLETE).
            e.wave.emitted_dirty_this_wave
                && !e.wave.emitted_tier3_this_wave
                && !e.value.terminal
                && !is_async
        });
        if undirty {
            // Route the balancing RESOLVED through the normal delivery waist so
            // resumeAll and batch timing obey R-undirty-settle-timing.
            self.down(vec![Message::Resolved]);
        }
        let keep_dirty_for_deferred_undirty = undirty
            && self.with_inner_edges(|_n, e| {
                e.wave.batch_dirty_owed || e.wave.emitted_tier3_this_wave
            });

        // Roll wave-local state forward after the settle path sees this wave's
        // dirty/tier flags. Subscriber callbacks above run borrow-free.
        self.with_inner_edges_mut(|_n, e| {
            for (i, b) in e.state.batch.iter_mut().enumerate() {
                if let Some(last) = b.as_ref().and_then(|batch| batch.last()).cloned() {
                    e.state.prev[i] = Some(last);
                }
                *b = None;
            }
            if !keep_dirty_for_deferred_undirty {
                e.wave.emitted_dirty_this_wave = false;
                e.wave.emitted_tier3_this_wave = false;
            }
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
        let (terminal, inside) =
            self.with_inner_edges(|_n, e| (e.value.terminal, e.wave.inside_run_wave));
        if terminal {
            return;
        }
        // B25: enroll in the wave touched-set before any mutation.
        wave_register(self);
        let mut sorted = msgs;
        sorted.sort_by_key(|m| m.tier().as_u8()); // stable: preserves intra-tier order

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

        let versioning_policy = self.versioning_policy();
        for m in &sorted {
            if let Message::Data(value) = m {
                assert_node_version_data_compatible(&versioning_policy, value)
                    .unwrap_or_else(|err| panic!("{err}"));
            }
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
                if let Some(action) = self.collect_down_action(m) {
                    self.apply_down_action(action);
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

    fn collect_down_action(&self, msg: Msg) -> Option<DownAction> {
        let next_version = if let Message::Data(value) = &msg {
            let (policy, current) = {
                let graph = self.graph.borrow();
                let version = graph.get_version(self.key());
                (version.policy.clone(), version.value.clone())
            };
            Some(
                advance_node_version(current.as_ref(), &policy, value)
                    .unwrap_or_else(|err| panic!("{err}")),
            )
        } else {
            None
        };
        let mut g = self.graph.borrow_mut();
        Self::collect_down_action_from_graph(&mut g, self.key(), msg, next_version)
    }

    fn apply_down_action(&self, action: DownAction) {
        match action {
            DownAction::Emit { msg, subs } => deliver_subscriber_snapshot(subs, &msg),
            DownAction::Invalidate { hooks } => {
                // Invalidate hooks may unsubscribe current observers. Preserve the
                // existing hook-before-subscriber-snapshot order (R-cleanup-hooks).
                for f in hooks {
                    f();
                }
                self.emit_to_subs(&Message::Invalidate);
            }
        }
    }

    fn collect_down_action_from_graph(
        g: &mut GraphCore,
        key: NodeKey,
        msg: Msg,
        next_version: Option<Option<NodeVersion>>,
    ) -> Option<DownAction> {
        match msg {
            Message::Dirty => {
                let (_n, e, a) = g.get_node_edges_aux_mut(key);
                if e.wave.emitted_dirty_this_wave {
                    return None;
                }
                e.wave.emitted_dirty_this_wave = true;
                e.value.status = Status::Dirty;
                Some(DownAction::Emit {
                    msg: Message::Dirty,
                    subs: snapshot_subscribers(a),
                })
            }
            Message::Data(v) => {
                // D49: every occurrence is DATA — no equals-substitution.
                {
                    let version = g.get_version_mut(key);
                    version.value =
                        next_version.expect("DATA precomputed next node runtime version");
                }
                let (_n, e, a) = g.get_node_edges_aux_mut(key);
                e.value.cache = Some(v.clone());
                e.value.has_data = true;
                e.value.status = Status::Settled;
                e.wave.emitted_tier3_this_wave = true;
                Some(DownAction::Emit {
                    msg: Message::Data(v),
                    subs: snapshot_subscribers(a),
                })
            }
            Message::Resolved => {
                // Explicit operator escape hatch (D49); the substrate-synthesized
                // undirty RESOLVED usually goes through run_wave. Dirty-balance paths
                // that must honor pause/batch timing also route through here, so keep
                // the no-cache case at SENTINEL while still emitting RESOLVED.
                let (_n, e, a) = g.get_node_edges_aux_mut(key);
                e.value.status = if e.value.has_data {
                    Status::Resolved
                } else {
                    Status::Sentinel
                };
                e.wave.emitted_tier3_this_wave = true;
                Some(DownAction::Emit {
                    msg: Message::Resolved,
                    subs: snapshot_subscribers(a),
                })
            }
            Message::Invalidate => {
                // Honor the invalidate-request: clear cache → SENTINEL, fire
                // onInvalidate, broadcast downstream (idempotent, no-op if unpopulated).
                let (_n, e, a) = g.get_node_edges_aux_mut(key);
                if !e.value.has_data {
                    return None;
                }
                e.value.cache = None;
                e.value.has_data = false;
                e.value.status = Status::Sentinel;
                Some(DownAction::Invalidate {
                    hooks: a.on_invalidate.clone(),
                })
            }
            Message::Complete => {
                let (_n, e, a) = g.get_node_edges_aux_mut(key);
                if e.value.terminal {
                    return None;
                }
                e.value.terminal = true;
                e.value.status = Status::Completed;
                a.pull_demand_owed = false;
                a.paused_dep_wave_occurred = false;
                a.pause_buffer.clear();
                Some(DownAction::Emit {
                    msg: Message::Complete,
                    subs: snapshot_subscribers(a),
                })
            }
            Message::Error(e) => {
                let (_n, edges, a) = g.get_node_edges_aux_mut(key);
                if edges.value.terminal {
                    return None;
                }
                edges.value.terminal = true;
                edges.value.status = Status::Errored;
                a.pull_demand_owed = false;
                a.paused_dep_wave_occurred = false;
                a.pause_buffer.clear();
                Some(DownAction::Emit {
                    msg: Message::Error(e),
                    subs: snapshot_subscribers(a),
                })
            }
            Message::Teardown => Some(DownAction::Emit {
                msg: Message::Teardown,
                subs: snapshot_subscribers(g.get_aux(key)),
            }),
            // PAUSE / RESUME are control-only and are delivered via up().
            _ => None,
        }
    }

    /// External deferred emit — the async-pool late-emit path used by
    /// [`crate::ctx::DeferredCtx`] (R-sync-core: async lives in the fn body, the emit
    /// serializes back onto the single thread). Establishes a FRESH wave-owner boundary
    /// (like [`Node::down`]) so a panic in the cascade becomes `[[ERROR,e]]` (D30) and the
    /// leading DIRTY is synthesized (`inside_run_wave` is false here). Nested under a live
    /// wave-owner (e.g. a deferred emit fired during a RESUME cascade) it just runs.
    pub(crate) fn owned_down(&self, msgs: Wave<AnyValue>) {
        with_wave_owner(self, || self.down(msgs), || {});
    }

    pub(crate) fn owned_up(&self, msgs: Wave<AnyValue>, toward_dep: Option<usize>) {
        with_wave_owner(self, || self.up(msgs, toward_dep), || {});
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
        let subs = self.with_inner_edges_aux_mut(|_n, e, a| {
            if !e.wave.emitted_dirty_this_wave {
                e.wave.emitted_dirty_this_wave = true;
                e.value.status = Status::Dirty;
                snapshot_subscribers(a)
            } else {
                SubscriberSnapshot::Empty
            }
        });
        deliver_subscriber_snapshot(subs, &Message::Dirty);
    }

    fn emit_to_subs(&self, msg: &Msg) {
        // Clone the sink handles out, drop the borrow, then deliver — guards against
        // subscribe/unsubscribe (and any re-entry) during iteration. The 0/1-sink
        // cases avoid allocating a Vec while preserving the same snapshot boundary.
        let subs = self.with_aux(snapshot_subscribers);
        deliver_subscriber_snapshot(subs, msg);
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
        for m in msgs {
            self.up_msg(m, toward_dep, route);
        }
    }

    fn up_msg(&self, m: Msg, toward_dep: Option<usize>, route: &mut UpRouteState) {
        let is_depless = self.borrow().deps.is_empty();
        match m {
            Message::Pause(lock) => self.pause_acquire(lock),
            Message::Resume(lock) => {
                let is_pull_demand = self.with_config(|cfg| cfg.pull_id.as_ref() == Some(&lock));
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

    fn forward_up(&self, m: Msg, toward_dep: Option<usize>, route: &mut UpRouteState) {
        // DR-8/B54: route control fanout over borrowed dep keys under wave safety.
        // In-wave and out-of-wave share the same path: resolve dep projections as
        // arena keys and only materialize a borrowed Core when the target is live and
        // can be re-entered this slice (counted or pinned).
        let deps = self.with_inner_edges(|n, _| {
            n.deps
                .iter()
                .map(|dep| (dep.id, dep.generation, dep.refs.clone()))
                .collect::<Vec<_>>()
        });
        let mut to_target = |target: &(NodeId, u64, Rc<Cell<usize>>), msg: Msg| {
            let (id, generation, refs) = target;
            let key = NodeKey {
                id: *id,
                generation: *generation,
            };
            let arena_key = ArenaNodeKey {
                graph: Rc::as_ptr(&self.graph) as usize,
                node: key,
            };
            {
                let mut graph = self.graph.borrow_mut();
                if !graph.is_live_key(key) || (refs.get() == 0 && graph.pin_count(key) == 0) {
                    return;
                }
                if !graph.pin_live_key(key) {
                    return;
                }
            }
            let pin = ArenaNodePin {
                arena_key,
                graph: self.graph.clone(),
                refs: refs.clone(),
            };
            let Some(dep_core) = pin.borrowed_core() else {
                return;
            };
            dep_core.up_msg(msg, None, route);
        };
        if let Some(i) = toward_dep {
            if let Some(dep) = deps.get(i) {
                to_target(dep, m);
            }
            return;
        }
        for dep in deps {
            to_target(&dep, dup_control(&m));
        }
    }

    // ── PAUSE/RESUME lockset (R-pause-lockset) + default mode (R-pause-modes) ──

    fn is_paused(&self) -> bool {
        !self.with_aux(|a| a.pause_lockset.is_empty())
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
            if boundary_drains_blocked() {
                register_boundary_root(self);
            }
        }
        self.fire_owed_demand_if_ready();
    }

    fn on_demand(&self) {
        let has_pull = self.with_node_state_aux_mut(|_n, _c, cfg, _r, _e, a| {
            if cfg.pull_id.is_none() {
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
        let Some((pull_id, should_mark_dirty)) =
            self.with_node_state_aux_mut(|_n, c, cfg, r, e, a| {
                let id = cfg.pull_id.clone()?;
                if e.value.terminal || !a.pull_demand_owed || e.state.pending != 0 {
                    return None;
                }
                if a.pause_lockset.iter().any(|held| held != &id) {
                    return None;
                }
                if c.handle.is_some()
                    && !r.has_called_fn_once
                    && !cfg.partial
                    && !cfg.all_deps_settled(&e.state)
                {
                    return None;
                }
                Some((id, a.paused_dep_wave_occurred))
            })
        else {
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
                let _ = self
                    .core
                    .try_with_node_state_aux_mut(|_n, _c, cfg, _r, e, a| {
                        if !e.value.terminal && cfg.pull_id.as_ref() == Some(&id) {
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

    /// Async-pool check that takes an existing borrow. The pool kind is a dispatcher
    /// property of the handle.
    fn call_is_async(c: &NodeCallSlot) -> bool {
        c.handle
            .map(|h| c.dispatcher.pool_kind(h.pool_id) == PoolKind::Async)
            .unwrap_or(false)
    }

    /// Should an outgoing settle slice be deferred into the pause buffer? `pausable` mode
    /// is the OUTER gate over R-async-paused buffering (D44).
    fn should_buffer_on_pause(&self) -> bool {
        self.with_node_state(|n, c, cfg, _r, e| match cfg.pausable {
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
                !e.wave.inside_run_wave && Self::call_is_async(c) && !n.deps.is_empty()
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

    pub(crate) fn subscriber_count(&self) -> usize {
        self.with_aux(|a| a.subscribers.len())
    }

    pub(crate) fn is_active(&self) -> bool {
        self.with_aux(|a| a.activated)
    }
}

fn reset_wave_flags_inner(n: &mut NodeTopologySlot, e: &mut DepEdges) {
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
    if !from.same_graph(target) {
        return false;
    }
    let target_key = target.key();
    let mut seen: HashSet<NodeKey> = HashSet::new();
    let mut stack: Vec<NodeKey> = vec![from.key()];
    let graph = from.graph.borrow();
    if !graph.is_live_key(target_key) {
        return false;
    }
    while let Some(key) = stack.pop() {
        if !seen.insert(key) {
            continue;
        }
        if !graph.is_live_key(key) {
            continue;
        }
        if key == target_key {
            return true;
        }
        let topology = graph.get(key);
        for dep in &topology.deps {
            stack.push(dep.key());
        }
    }
    false
}

/// Dedup a dep list by node identity, preserving first-seen order — a dep
/// appearing twice in a rewire collapses to one (Option-C; matches the TS `_dedupDeps`).
fn dedup_cores(deps: Vec<Core>) -> Vec<Core> {
    let mut out: Vec<Core> = Vec::with_capacity(deps.len());
    let mut seen: HashSet<ArenaNodeKey> = HashSet::with_capacity(deps.len());
    for d in deps {
        if seen.insert(d.arena_node_key()) {
            out.push(d);
        }
    }
    out
}

fn same_ordered_deps(old_deps: &[Core], new_deps: &[Core]) -> bool {
    old_deps.len() == new_deps.len()
        && old_deps
            .iter()
            .zip(new_deps)
            .all(|(old_dep, new_dep)| old_dep.ptr_eq(new_dep))
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
        Self::state_opts_in_arena_with_dispatcher(arena, dispatcher, initial, NodeOpts::default())
    }

    pub(crate) fn state_opts_in_arena_with_dispatcher(
        arena: &GraphArena,
        dispatcher: Dispatcher,
        initial: T,
        opts: NodeOpts,
    ) -> Node<T> {
        Node {
            core: Core::new_in_arena(
                arena,
                vec![],
                None,
                dispatcher,
                Some(Rc::new(initial)),
                opts,
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
        Self::state_empty_opts_in_arena_with_dispatcher(arena, dispatcher, NodeOpts::default())
    }

    pub(crate) fn state_empty_opts_in_arena_with_dispatcher(
        arena: &GraphArena,
        dispatcher: Dispatcher,
        opts: NodeOpts,
    ) -> Node<T> {
        Node {
            core: Core::new_in_arena(arena, vec![], None, dispatcher, None, opts),
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
            core: Core::new_in_arena(arena, vec![], Some(handle), dispatcher, None, opts.clone()),
            _t: PhantomData,
        };
        node.core.with_node_state_mut(|_n, call, _cfg, _r, _e| {
            call.factory = factory;
        });
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
        Self::derived_opts_initial_in_arena_with_dispatcher(arena, dispatcher, deps, opts, None, f)
    }

    pub(crate) fn derived_opts_initial_in_arena_with_dispatcher<F: Fn(&Ctx) + 'static>(
        arena: &GraphArena,
        dispatcher: Dispatcher,
        deps: Vec<Core>,
        opts: NodeOpts,
        initial: Option<T>,
        f: F,
    ) -> Node<T> {
        assert!(
            !(opts.pull_id.is_some() && matches!(opts.pausable, Pausable::False)),
            "pullId + pausable:false is invalid (R-pull / R-pause-modes)"
        );
        let factory = opts.factory.clone();
        let handle = register_with(&dispatcher, opts.pool, Rc::new(f));
        let initial = initial.map(|value| Rc::new(value) as AnyValue);
        let node = Node {
            core: Core::new_in_arena(arena, deps, Some(handle), dispatcher, initial, opts.clone()),
            _t: PhantomData,
        };
        {
            // Thread the dep-terminal propagation policy (R-deps-terminal) into the inner —
            // `Core::new` defaults to the plain derived behavior (auto-cascade, not an input).
            node.core.with_node_state_mut(|_n, call, cfg, _r, _e| {
                cfg.partial = opts.partial;
                cfg.complete_when_deps_complete = opts.complete_when_deps_complete;
                cfg.error_when_deps_error = opts.error_when_deps_error;
                cfg.terminal_as_real_input = opts.terminal_as_real_input;
                cfg.versioning = opts.versioning;
                call.factory = factory;
            });
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
        with_wave_owner(&self.core, || self.core.down(msgs), || {});
    }

    /// Emit a raw control wave upstream — control tiers only (R-ctx-up / R-node-iface).
    /// PAUSE/RESUME act on this node's lockset (R-pause-lockset).
    pub fn up(&self, msgs: Wave<AnyValue>) {
        with_wave_owner(&self.core, || self.core.up(msgs, None), || {});
    }

    /// Directed upstream control along one declared dep edge (R-up-routing).
    pub fn up_toward(&self, toward_dep: usize, msgs: Wave<AnyValue>) {
        with_wave_owner(&self.core, || self.core.up(msgs, Some(toward_dep)), || {});
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

    /// Read-only node runtime version metadata (D109).
    pub fn version(&self) -> Option<NodeVersion> {
        self.core.version()
    }

    pub fn pull_id(&self) -> Option<LockId> {
        self.core.with_config(|cfg| cfg.pull_id.clone())
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
        let sink = Rc::new(sink);
        // Record the subscriber id before activation so the error path hands back a real
        // unsubscribe instead of leaking the just-registered sink (no orphaned sink).
        let id_cell: Rc<Cell<Option<u64>>> = Rc::new(Cell::new(None));
        let body_cell = id_cell.clone();
        with_wave_owner(
            &self.core,
            || self.core.subscribe_recording_id(sink, &body_cell),
            move || match id_cell.get() {
                Some(id) => {
                    let err_core = self.core.clone();
                    Box::new(move || err_core.unsubscribe(id)) as Unsub
                }
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
    pub fn replace_deps<F: Fn(&Ctx) + 'static>(&self, new_deps: Vec<Core>, f: F) {
        self.core.rewire(new_deps, Rc::new(f));
    }

    /// Subscribe to one dep (special case of [`Node::replace_deps`]); returns its index. fn required (SD-1).
    pub fn subscribe_dep<F: Fn(&Ctx) + 'static>(&self, dep: Core, f: F) -> usize {
        let mut next = self.core.deps();
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

    /// Unsubscribe from one dep (special case of [`Node::replace_deps`]); idempotent if absent (the fn
    /// swap still applies). fn required (SD-1).
    pub fn unsubscribe_dep<F: Fn(&Ctx) + 'static>(&self, dep: Core, f: F) {
        let next: Vec<Core> = self
            .core
            .deps()
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

    use serde_json::json;

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
        let seen_refs = Rc::new(RefCell::new(Vec::<usize>::new()));
        let evens_refs = evens.core.refs.clone();
        let seen_ref_count = seen_refs.clone();
        let _u = evens.subscribe({
            move |m| {
                if matches!(m, Message::Resolved) {
                    seen_ref_count.borrow_mut().push(evens_refs.get());
                }
                sink(m)
            }
        });
        let base_refs = evens.core.refs.get();
        assert_eq!(*log.borrow(), vec!["START"]); // first-run gate holds (a SENTINEL)

        a.set(3); // odd → fn runs, no emit → substrate undirties with one RESOLVED
        assert_eq!(*log.borrow(), vec!["START", "DIRTY", "RESOLVED"]);
        assert_eq!(evens.cache(), None); // filtered → never valued
        assert_eq!(evens.status(), Status::Sentinel);
        assert!(
            seen_refs.borrow().iter().all(|count| *count == base_refs),
            "post-run RESOLVED must not promote a counted owner Core"
        );
        let (prev, batch) = evens.core.with_inner_edges(|_n, e| {
            (
                e.state.prev[0]
                    .as_ref()
                    .and_then(|v| v.downcast_ref::<i32>().copied()),
                e.state.batch[0].is_none(),
            )
        });
        assert_eq!(prev, Some(3));
        assert!(batch);

        a.set(4); // even → real DATA (downstream was NOT wedged by the prior RESOLVED)
        assert_eq!(
            *log.borrow(),
            vec!["START", "DIRTY", "RESOLVED", "DIRTY", "DATA"]
        );
        assert_eq!(evens.cache(), Some(4));
        assert_eq!(evens.status(), Status::Settled);
        let prev = evens.core.with_inner_edges(|_n, e| {
            e.state.prev[0]
                .as_ref()
                .and_then(|v| v.downcast_ref::<i32>().copied())
        });
        assert_eq!(prev, Some(4));

        a.set(5); // odd → filter; node KEEPS its value 4, status → Resolved (no new occurrence)
        assert_eq!(
            *log.borrow(),
            vec!["START", "DIRTY", "RESOLVED", "DIRTY", "DATA", "DIRTY", "RESOLVED"]
        );
        assert_eq!(evens.cache(), Some(4));
        assert_eq!(evens.status(), Status::Resolved);
        let (seen_refs, prev_batch) = evens.core.with_inner_edges(|_n, e| {
            (
                seen_refs.borrow().clone(),
                e.state.prev[0]
                    .as_ref()
                    .and_then(|v| v.downcast_ref::<i32>().copied()),
            )
        });
        assert_eq!(
            seen_refs,
            vec![base_refs, base_refs],
            "RESOLVED callbacks should not increment counted Core refs"
        );
        assert_eq!(prev_batch, Some(5));
    }

    #[test]
    fn run_wave_post_dispatch_resolved_is_borrow_free_and_rolls_dep_state() {
        // DR-8/B54 owner-execution: post-dispatch run_wave cleanup should route the
        // synthesized undirty RESOLVED through the delivery waist, deliver callbacks
        // borrow-free, and then roll dep state without counted owner promotion.
        let arena = GraphArena::new();
        let source = Node::<i32>::state_in_arena(&arena, 2);
        type SeenRuns = Rc<RefCell<Vec<(Option<i32>, Vec<i32>)>>>;
        let seen: SeenRuns = Rc::new(RefCell::new(Vec::new()));
        let evens: Node<i32> =
            Node::derived_opts_in_arena(&arena, vec![source.erased()], NodeOpts::default(), {
                let seen = seen.clone();
                move |ctx| {
                    let prev = ctx.dep_records()[0]
                        .prev_data
                        .as_ref()
                        .and_then(|v| v.downcast_ref::<i32>().copied());
                    let batch = ctx
                        .batch::<i32>(0)
                        .into_iter()
                        .map(|v| *v)
                        .collect::<Vec<_>>();
                    seen.borrow_mut().push((prev, batch));
                    let v = *ctx.data::<i32>(0).unwrap();
                    if v % 2 == 0 {
                        ctx.emit(v);
                    }
                }
            });
        let held: Rc<RefCell<Option<Node<i32>>>> =
            Rc::new(RefCell::new(Some(Node::<i32>::state_in_arena(&arena, 99))));
        let refs_slot: Rc<RefCell<Option<Rc<Cell<usize>>>>> = Rc::new(RefCell::new(None));
        let resolved_refs = Rc::new(Cell::new(usize::MAX));
        let _u = evens.subscribe({
            let held = held.clone();
            let refs_slot = refs_slot.clone();
            let resolved_refs = resolved_refs.clone();
            move |msg| {
                if matches!(msg, Message::Resolved) {
                    if let Some(refs) = refs_slot.borrow().as_ref() {
                        resolved_refs.set(refs.get());
                    }
                    let _ = held.borrow_mut().take();
                }
            }
        });
        *refs_slot.borrow_mut() = Some(evens.core.refs.clone());
        let refs_before = evens.core.refs.get();

        assert_eq!(
            &*seen.borrow(),
            &[(None, vec![2])],
            "activation run sees the cached source wave once"
        );
        source.set(3);

        assert!(held.borrow().is_none(), "RESOLVED callback ran borrow-free");
        assert_eq!(
            resolved_refs.get(),
            refs_before,
            "post-run RESOLVED delivery must not temporarily promote the owner Core"
        );
        assert_eq!(evens.cache(), Some(2));
        assert_eq!(evens.status(), Status::Resolved);

        source.set(4);
        assert_eq!(
            &*seen.borrow(),
            &[(None, vec![2]), (Some(2), vec![3]), (Some(3), vec![4])],
            "the rejected odd wave must still roll dep prev_data before the next run"
        );
        assert_eq!(evens.cache(), Some(4));
        let (res_refs, rolled_prev) = evens.core.with_inner_edges(|_n, e| {
            (
                resolved_refs.get(),
                e.state.prev[0]
                    .as_ref()
                    .and_then(|v| v.downcast_ref::<i32>().copied()),
            )
        });
        assert_eq!(
            rolled_prev,
            Some(4),
            "run_wave should carry latest settled data forward in prev"
        );
        assert!(
            evens
                .core
                .with_inner_edges(|_n, e| e.state.batch[0].is_none()),
            "post-wave state must clear batch"
        );
        assert_eq!(
            res_refs, refs_before,
            "post-run RESOLVED delivery should remain borrow-free and ref-neutral"
        );
    }

    #[test]
    fn run_wave_post_dispatch_panic_keeps_wave_guard() {
        // DR-8/B54: if a synthesized RESOLVED callback panics, WaveGuard must
        // clear `inside_run_wave` on unwind so D30 recovery can proceed.
        let source = Node::<i32>::state_empty();
        let resolved = Rc::new(Cell::new(false));
        let resolved_flag = resolved.clone();
        let derived: Node<i32> = Node::derived(vec![source.erased()], |ctx| {
            let _ = ctx.data::<i32>(0).unwrap();
        });
        let _u = derived.subscribe(move |msg| {
            if matches!(msg, Message::Resolved) {
                resolved_flag.set(true);
                panic!("resolved callback panic");
            }
        });

        source.set(1);

        assert!(
            resolved.get(),
            "post-run RESOLVED should still be delivered before callback-panic recovery"
        );
        assert_eq!(
            derived.status(),
            Status::Errored,
            "panic during callback should land ERROR"
        );
        assert!(
            !derived.core.with_inner_edges(|_, e| e.wave.inside_run_wave),
            "WaveGuard must clear inside_run_wave during unwind"
        );
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
    fn data_waits_while_another_dep_is_pending() {
        let a = Node::<i32>::state_empty();
        let b = Node::<i32>::state_empty();
        let runs = Rc::new(Cell::new(0usize));
        let r2 = runs.clone();
        let sum: Node<i32> = Node::derived(vec![a.erased(), b.erased()], move |ctx| {
            r2.set(r2.get() + 1);
            ctx.emit(*ctx.data::<i32>(0).unwrap() + *ctx.data::<i32>(1).unwrap());
        });
        let (log, sink) = recorder();
        let _u = sum.subscribe(sink);
        assert_eq!(*log.borrow(), vec!["START"]);

        with_wave_owner(
            &a.core,
            || sum.core.receive_from_dep(0, &Message::Dirty),
            || {},
        );
        with_wave_owner(
            &b.core,
            || sum.core.receive_from_dep(1, &Message::Dirty),
            || {},
        );
        assert_eq!(*log.borrow(), vec!["START", "DIRTY"]);

        with_wave_owner(
            &a.core,
            || sum.core.receive_from_dep(0, &Message::Data(Rc::new(3i32))),
            || {},
        );
        assert_eq!(runs.get(), 0, "dep 1 is still pending");

        with_wave_owner(
            &b.core,
            || sum.core.receive_from_dep(1, &Message::Data(Rc::new(4i32))),
            || {},
        );
        assert_eq!(runs.get(), 1);
        assert_eq!(sum.cache(), Some(7));
        assert_eq!(*log.borrow(), vec!["START", "DIRTY", "DATA"]);
    }

    #[test]
    fn data_without_prior_dirty_still_satisfies_first_run_gate() {
        let a = Node::<i32>::state_empty();
        let b = Node::<i32>::state_empty();
        let runs = Rc::new(Cell::new(0usize));
        let r2 = runs.clone();
        let sum: Node<i32> = Node::derived(vec![a.erased(), b.erased()], move |ctx| {
            r2.set(r2.get() + 1);
            ctx.emit(*ctx.data::<i32>(0).unwrap() + *ctx.data::<i32>(1).unwrap());
        });
        let (log, sink) = recorder();
        let _u = sum.subscribe(sink);
        assert_eq!(*log.borrow(), vec!["START"]);

        with_wave_owner(
            &a.core,
            || sum.core.receive_from_dep(0, &Message::Data(Rc::new(3i32))),
            || {},
        );
        assert_eq!(runs.get(), 0, "first-run gate still waits for dep 1");

        with_wave_owner(
            &b.core,
            || sum.core.receive_from_dep(1, &Message::Data(Rc::new(4i32))),
            || {},
        );
        assert_eq!(runs.get(), 1);
        assert_eq!(sum.cache(), Some(7));
        assert_eq!(*log.borrow(), vec!["START", "DATA"]);
    }

    #[test]
    fn paused_data_survives_until_later_invalidate_drains_pending() {
        let a = Node::<i32>::state(1);
        let b = Node::<i32>::state(10);
        let runs = Rc::new(Cell::new(0usize));
        let r2 = runs.clone();
        let sum: Node<i32> = Node::derived(vec![a.erased(), b.erased()], move |ctx| {
            r2.set(r2.get() + 1);
            let x = ctx.data::<i32>(0).map(|v| *v).unwrap_or(0);
            let y = ctx.data::<i32>(1).map(|v| *v).unwrap_or(0);
            ctx.emit(x + y);
        });
        let (log, sink) = recorder();
        let _u = sum.subscribe(sink);
        assert_eq!(runs.get(), 1);
        assert_eq!(sum.cache(), Some(11));
        log.borrow_mut().clear();

        let pause = LockId::new("coalesce");
        sum.up(vec![Message::Pause(pause.clone())]);
        a.down(vec![Message::Dirty]);
        b.down(vec![Message::Dirty]);
        a.down(vec![Message::Data(Rc::new(2i32))]);
        assert_eq!(runs.get(), 1, "dep 1 is still pending while paused");

        b.down(vec![Message::Invalidate]);
        assert_eq!(runs.get(), 1, "still paused after pending drains");
        sum.up(vec![Message::Resume(pause)]);

        assert_eq!(runs.get(), 2);
        assert_eq!(sum.cache(), Some(2));
        assert_eq!(*log.borrow(), vec!["DIRTY", "INVALIDATE", "DATA"]);
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
    fn on_invalidate_unsubscribe_affects_current_broadcast_snapshot() {
        // R-cleanup-hooks / R-invalidate-idempotent: onInvalidate runs before the
        // downstream INVALIDATE broadcast. If the hook detaches a subscriber, the
        // current broadcast must snapshot after that detach rather than deliver to
        // a stale pre-hook subscriber list.
        let src = Node::<i32>::state(1);
        let victim_unsub: Rc<RefCell<Option<Unsub>>> = Rc::new(RefCell::new(None));
        let d: Node<i32> = {
            let victim_unsub = victim_unsub.clone();
            Node::derived(vec![src.erased()], move |ctx| {
                let victim_unsub = victim_unsub.clone();
                ctx.on_invalidate(move || {
                    if let Some(unsub) = victim_unsub.borrow_mut().take() {
                        unsub();
                    }
                });
                ctx.emit(*ctx.data::<i32>(0).unwrap());
            })
        };
        let _keepalive = d.subscribe(|_| {});
        let (log, sink) = recorder();
        let victim = d.subscribe(sink);
        *victim_unsub.borrow_mut() = Some(victim);
        log.borrow_mut().clear();

        src.down(vec![Message::Invalidate]);

        assert_eq!(
            log.borrow().iter().filter(|m| *m == "INVALIDATE").count(),
            0,
            "a subscriber detached by onInvalidate must not receive the same INVALIDATE"
        );
    }

    #[test]
    fn subscriber_callback_drops_handle_borrow_free() {
        // DR-8/B54 owner-execution invariant: callbacks run only after the owner arena
        // mutation borrow has been released. A subscriber may drop the last handle to
        // another same-arena node, which re-enters Core::drop; holding the owner borrow
        // across the callback would panic.
        let held: Rc<RefCell<Option<Node<i32>>>> =
            Rc::new(RefCell::new(Some(Node::<i32>::producer(|_| {}))));
        let source = Node::<i32>::state_empty();
        let h = held.clone();
        let _u = source.subscribe(move |m| {
            if matches!(m, Message::Data(_)) {
                let _ = h.borrow_mut().take();
            }
        });

        source.set(1);

        assert!(held.borrow().is_none());
        assert_eq!(source.cache(), Some(1));
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
    fn drop_unregisters_fn_even_when_cleanup_hook_panics() {
        // DR-8/B54 side-table cleanup regression: the call slot must unregister before
        // user cleanup hooks run, or a panicking onDeactivation strands the dispatcher
        // fn slot and every Core/resource captured by the fn.
        struct DropFlag(Rc<Cell<bool>>);
        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }

        let fn_dropped = Rc::new(Cell::new(false));
        let result = catch_unwind(AssertUnwindSafe({
            let fn_dropped = fn_dropped.clone();
            move || {
                let guard = DropFlag(fn_dropped);
                let p = Node::<i32>::producer(move |ctx| {
                    let _keep = &guard;
                    ctx.on_deactivation(|| panic!("cleanup boom"));
                    ctx.emit(1i32);
                });
                let _u = p.subscribe(|_| {});
            }
        }));

        assert!(result.is_err());
        assert!(
            fn_dropped.get(),
            "dispatcher unregister must drop fn captures even if user cleanup panics"
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
    fn deferred_delivery_actions_do_not_pin_core_while_queued() {
        // DR-8/B54: delivery-boundary queues carry generation-keyed weak arena actions,
        // not counted Core clones. The wave touched-set owns the in-wave pin; the queued
        // action should not add hot-path refcount churn while it waits for the boundary.
        let source = Node::<i32>::state(1);
        let derived: Node<i32> = Node::derived(vec![source.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap() + 1)
        });
        let _u = derived.subscribe(|_| {});
        let refs_before = derived.core.refs.get();

        with_delivery_scope(|| {
            defer_run_until_delivery_boundary(&derived.core);
            defer_run_until_delivery_boundary(&derived.core);
            defer_absorbed_settle_until_delivery_boundary(&derived.core);
            defer_absorbed_settle_until_delivery_boundary(&derived.core);

            assert_eq!(
                derived.core.refs.get(),
                refs_before,
                "queued delivery-boundary actions must not hold counted Core refs"
            );
            DEFERRED_DELIVERY_ACTIONS.with(|actions| {
                let actions = actions.borrow();
                assert_eq!(
                    actions.len(),
                    2,
                    "delivery-boundary actions dedupe by arena key and action kind"
                );
                assert_eq!(
                    actions
                        .iter()
                        .filter(|action| action.kind == DeferredDeliveryKind::Run)
                        .count(),
                    1,
                    "run actions dedupe by arena key"
                );
                assert_eq!(
                    actions
                        .iter()
                        .filter(|action| action.kind == DeferredDeliveryKind::AbsorbedSettle)
                        .count(),
                    1,
                    "absorbed-settle actions dedupe by arena key"
                );
            });
        });

        assert_eq!(
            derived.core.refs.get(),
            refs_before,
            "delivery-boundary drain does not retain queued counted Core refs"
        );
    }

    #[test]
    fn active_wave_deferred_delivery_drain_uses_borrowed_core() {
        // DR-8/B54: when a DATA delivery queues a fn run until the delivery boundary,
        // the owner wave's arena pin is still live while the drain executes. Rehydrate
        // that target as a borrowed Core, not a transient counted Core.
        let source = Node::<i32>::state_empty();
        let observed_refs = Rc::new(Cell::new(usize::MAX));
        let refs_slot: Rc<RefCell<Option<Rc<Cell<usize>>>>> = Rc::new(RefCell::new(None));
        let derived: Node<i32> = Node::derived(vec![source.erased()], {
            let observed_refs = observed_refs.clone();
            let refs_slot = refs_slot.clone();
            move |ctx| {
                if let Some(refs) = refs_slot.borrow().as_ref() {
                    observed_refs.set(refs.get());
                }
                ctx.emit(*ctx.data::<i32>(0).unwrap());
            }
        });
        *refs_slot.borrow_mut() = Some(derived.core.refs.clone());
        let _u = derived.subscribe(|_| {});
        let refs_before = derived.core.refs.get();

        source.set(1);

        assert_eq!(
            observed_refs.get(),
            refs_before,
            "active-wave delivery-boundary drain should run through a borrowed arena view"
        );
        assert_eq!(derived.core.refs.get(), refs_before);
    }

    #[test]
    fn deferred_delivery_drains_run_before_absorbed_settle() {
        // DR-8/B54: delivery boundary actions must process Run before AbsorbedSettle
        // so a deferred recompute is not starved behind its follow-up settle.
        let events = Rc::new(RefCell::new(Vec::<&'static str>::new()));
        let derived: Node<i32> = Node::derived(vec![], {
            let events = events.clone();
            move |_ctx| {
                events.borrow_mut().push("run");
            }
        });
        let _u = derived.subscribe({
            let events = events.clone();
            move |msg| {
                if let Message::Resolved = msg {
                    events.borrow_mut().push("resolved");
                }
            }
        });
        events.borrow_mut().clear();

        // Seed settle eligibility for the same wave as an absorbed terminal path would do.
        derived
            .core
            .with_inner_edges_mut(|_n, e| e.wave.emitted_dirty_this_wave = true);

        with_delivery_scope(|| {
            defer_run_until_delivery_boundary(&derived.core);
            defer_absorbed_settle_until_delivery_boundary(&derived.core);
        });

        assert_eq!(
            &*events.borrow(),
            &["run", "resolved"],
            "deferred Run must drain before AbsorbedSettle"
        );
    }

    #[test]
    fn deferred_delivery_outside_wave_run_does_not_increment_refs() {
        // DR-8/B54: when a queued delivery-boundary run drains outside a wave, the
        // borrowed pin is used for execution, so no temporary counted ref is added.
        let source = Node::<i32>::state(1);
        let refs_slot: Rc<RefCell<Option<Rc<Cell<usize>>>>> = Rc::new(RefCell::new(None));
        let observed_refs = Rc::new(Cell::new(usize::MAX));
        let run_count = Rc::new(Cell::new(0usize));

        let derived: Node<i32> = Node::derived_opts(
            vec![source.erased()],
            NodeOpts {
                partial: true,
                ..NodeOpts::default()
            },
            {
                let refs_slot = refs_slot.clone();
                let observed_refs = observed_refs.clone();
                let run_count = run_count.clone();
                move |_ctx| {
                    if let Some(refs) = refs_slot.borrow().as_ref() {
                        observed_refs.set(refs.get());
                    }
                    run_count.set(run_count.get() + 1);
                }
            },
        );

        *refs_slot.borrow_mut() = Some(derived.core.refs.clone());
        let before = derived.core.refs.get();

        with_delivery_scope(|| {
            defer_run_until_delivery_boundary(&derived.core);
        });

        assert_eq!(
            run_count.get(),
            1,
            "queued delivery-boundary run must execute"
        );
        assert_eq!(
            observed_refs.get(),
            before,
            "outside-wave delivery-boundary run must execute from a borrowed pin"
        );
    }

    #[test]
    fn stale_deferred_delivery_action_does_not_run_after_slot_free() {
        // DR-8/B54: once a node's slot is released, queued deferred actions must be
        // generation-checked and become inert, even before the slot is reused.
        DEFERRED_DELIVERY_ACTIONS.with(|actions| actions.borrow_mut().clear());

        let arena = GraphArena::new();
        let old = Node::<i32>::state_in_arena(&arena, 1);
        let old_key = old.core.key();
        let weak = old.core.downgrade();
        let stale_run = DeferredDeliveryAction::from_core(DeferredDeliveryKind::Run, &old.core);
        let stale_settle =
            DeferredDeliveryAction::from_core(DeferredDeliveryKind::AbsorbedSettle, &old.core);

        drop(old);

        assert!(
            weak.borrowed_core().is_none(),
            "old slot is not live after drop"
        );
        assert_eq!(weak.key.generation, old_key.generation);

        DEFERRED_DELIVERY_ACTIONS.with(|actions| {
            let mut actions = actions.borrow_mut();
            actions.push(stale_run);
            actions.push(stale_settle);
        });
        drain_deferred_runs();

        assert!(
            weak.borrowed_core().is_none(),
            "freed slot remains out-of-scope for stale deferred work"
        );
    }

    #[test]
    fn weak_borrowed_core_uses_arena_pin_after_external_death() {
        // DR-8/B54: a wave pin, not the counted Core refcount, owns liveness for
        // in-wave borrowed execution. This must fail closed once the pin releases.
        let arena = GraphArena::new();
        let graph = arena.0.clone();
        let victim = Node::<i32>::state_in_arena(&arena, 1);
        let key = victim.core.key();
        let weak = victim.core.downgrade();
        let refs = victim.core.refs.clone();
        let pin = ArenaNodePin::from_core(&victim.core).expect("live slot pins");
        drop(victim);

        assert_eq!(refs.get(), 0);
        assert!(graph.borrow().is_live_key(key));
        assert!(
            weak.borrowed_core().is_some(),
            "active arena pin should allow borrowed weak rehydration without reviving refs"
        );
        drop(pin);
        assert!(!graph.borrow().is_live_key(key));
        assert!(
            weak.borrowed_core().is_none(),
            "borrowed weak rehydration must fail once the pin releases"
        );
    }

    #[test]
    fn deferred_delivery_run_skips_same_wave_terminalized_target() {
        // R-terminal: a DATA may queue a delivery-boundary run, then a later terminal in
        // the same upstream wave can complete the target before that boundary drains.
        // The stale run must not invoke user code after terminal-is-forever seals output.
        let source = Node::<i32>::state_empty();
        let runs = Rc::new(Cell::new(0usize));
        let derived: Node<i32> = Node::derived(vec![source.erased()], {
            let runs = runs.clone();
            move |ctx| {
                runs.set(runs.get() + 1);
                ctx.emit(*ctx.data::<i32>(0).unwrap());
            }
        });
        let _u = derived.subscribe(|_| {});

        source.down(vec![Message::Data(Rc::new(1i32)), Message::Complete]);

        assert_eq!(
            runs.get(),
            0,
            "queued delivery-boundary run must not execute after same-wave terminal"
        );
        assert_eq!(derived.status(), Status::Completed);
    }

    #[test]
    fn stale_deferred_delivery_action_does_not_target_reused_slot() {
        // A queued weak arena action may outlive the original slot after an unwind/drop.
        // Rehydration must check the generation and old ref token, so a later occupant of
        // the same slot is a no-op target, not a run/settle target.
        DEFERRED_DELIVERY_ACTIONS.with(|actions| actions.borrow_mut().clear());

        let arena = GraphArena::new();
        let old = Node::<i32>::state_in_arena(&arena, 1);
        let stale_run = DeferredDeliveryAction::from_core(DeferredDeliveryKind::Run, &old.core);
        let stale_settle =
            DeferredDeliveryAction::from_core(DeferredDeliveryKind::AbsorbedSettle, &old.core);
        let old_key = old.core.key();
        drop(old);

        let reused = Node::<i32>::state_in_arena(&arena, 2);
        assert_eq!(old_key.id, reused.core.key().id);
        assert_ne!(old_key.generation, reused.core.key().generation);

        DEFERRED_DELIVERY_ACTIONS.with(|actions| {
            let mut actions = actions.borrow_mut();
            actions.push(stale_run);
            actions.push(stale_settle);
        });
        drain_deferred_runs();

        assert_eq!(reused.cache(), Some(2));
        assert_eq!(
            reused.status(),
            Status::Settled,
            "stale deferred generation must not disturb the reused slot occupant"
        );
    }

    #[test]
    fn panic_aborted_delivery_clears_deferred_delivery_actions() {
        // B25/DR-8: delivery-boundary run/settle queues are wave-local. If a subscriber
        // panics after a dep callback queued a run, the wave-owner catch resets touched
        // flags AND must discard those queued actions. A later unrelated delivery must
        // not run stale work against reset dep projections.
        DEFERRED_DELIVERY_ACTIONS.with(|actions| actions.borrow_mut().clear());

        let source = Node::<i32>::state_empty();
        let runs = Rc::new(Cell::new(0usize));
        let d: Node<i32> = Node::derived(vec![source.erased()], {
            let runs = runs.clone();
            move |_ctx| {
                runs.set(runs.get() + 1);
            }
        });
        let _d_sub = d.subscribe(|_| {});
        let _panic_sub = source.subscribe(|m| {
            if matches!(m, Message::Data(_)) {
                panic!("subscriber aborts delivery after dep callback queued run");
            }
        });

        source.set(1);

        assert_eq!(runs.get(), 0, "aborted delivery must not run the queued fn");
        DEFERRED_DELIVERY_ACTIONS.with(|queued| {
            assert!(
                queued.borrow().is_empty(),
                "aborted wave must clear queued deferred delivery actions"
            );
        });

        let unrelated = Node::<i32>::state_empty();
        let _u = unrelated.subscribe(|_| {});
        unrelated.set(1);
        assert_eq!(
            runs.get(),
            0,
            "later unrelated delivery must not drain stale work from the aborted wave"
        );
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
    fn internal_dep_callback_entry_uses_borrowed_pin_outside_wave() {
        // DR-8/B54: the off-wave weak dep callback path should also use a temporary
        // arena pin + borrowed Core, not a counted Core promotion.
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

        weak.receive_from_dep(0, &Message::Start);
        assert_eq!(
            derived.core.refs.get(),
            refs_before,
            "outside a wave, weak dep entry must not promote a counted Core"
        );
    }

    #[test]
    fn internal_dep_callback_entry_outside_wave_uses_pinned_borrow_when_refcount_dead() {
        // DR-8/B54: when a node is already arena-pinned (eg. by a temporary
        // batch/closure token) and `refs == 0`, we can process an off-wave dep callback
        // without creating a temporary counted Core clone.
        let arena = GraphArena::new();
        let source = Node::<i32>::state_in_arena(&arena, 1);
        let derived: Node<i32> = Node::derived_opts_in_arena(
            &arena,
            vec![source.erased()],
            NodeOpts::default(),
            |ctx| ctx.emit(*ctx.data::<i32>(0).unwrap()),
        );
        let weak = derived.core.downgrade();
        let borrowed = derived.core.borrowed_view();
        let pin = ArenaNodePin::from_core(&derived.core).expect("node must be pinnable while live");
        let refs = derived.core.refs.clone();

        drop(derived);
        assert_eq!(
            refs.get(),
            0,
            "setup drops the final public node handle so receive path is stress-tested with no counted refs"
        );

        weak.receive_from_dep(0, &Message::Data(Rc::new(4i32)));
        assert_eq!(
            refs.get(),
            0,
            "pinned receive should not promote a counted Core when the slot is already externally dead"
        );
        assert_eq!(
            borrowed
                .cache_any()
                .and_then(|v| v.downcast_ref::<i32>().copied()),
            Some(4),
            "borrowed owner execution should still run the dep callback while pinned"
        );

        with_wave_owner(
            &source.core,
            || weak.receive_from_dep(0, &Message::Data(Rc::new(5i32))),
            || {},
        );
        assert_eq!(
            refs.get(),
            0,
            "in-wave pinned receive should also avoid counted Core promotion"
        );
        assert_eq!(
            borrowed
                .cache_any()
                .and_then(|v| v.downcast_ref::<i32>().copied()),
            Some(5),
            "in-wave collect/apply must not half-apply dep bookkeeping then skip the action"
        );
        drop(pin);
    }

    #[test]
    fn internal_dep_callback_entry_run_decision_executes_from_borrowed_action() {
        // A precomputed in-wave Run decision must execute from the borrowed action
        // path and avoid perturbing the owner Core refcount during fn execution.
        let arena = GraphArena::new();
        let source = Node::<i32>::state_empty_in_arena(&arena);
        let refs_slot: Rc<RefCell<Option<Rc<Cell<usize>>>>> = Rc::new(RefCell::new(None));
        let observed_refs_in_fn = Rc::new(Cell::new(usize::MAX));
        let run_count = Rc::new(Cell::new(0usize));

        let derived = Node::<i32>::derived_opts_in_arena(
            &arena,
            vec![source.erased()],
            NodeOpts::default(),
            {
                let refs_slot = refs_slot.clone();
                let observed_refs_in_fn = observed_refs_in_fn.clone();
                let run_count = run_count.clone();
                move |ctx| {
                    run_count.set(run_count.get() + 1);
                    let refs = refs_slot
                        .borrow()
                        .as_ref()
                        .expect("test installed ref counter before run")
                        .clone();
                    observed_refs_in_fn.set(refs.get());
                    ctx.emit(*ctx.data::<i32>(0).unwrap());
                }
            },
        );
        *refs_slot.borrow_mut() = Some(derived.core.refs.clone());
        let _u = derived.subscribe(|_| {});
        let baseline_refs = derived.core.refs.get();
        assert_eq!(
            run_count.get(),
            0,
            "first-run gate should hold before source DATA"
        );

        let weak = derived.core.downgrade();
        let action = with_wave_owner(
            &source.core,
            || {
                weak.collect_in_wave_receive_from_dep_action(0, &Message::Data(Rc::new(4i32)))
                    .expect("in-wave receive should collect a runnable action")
            },
            InWaveDepReceiveAction::default,
        );

        assert!(
            matches!(action.maybe_run_decision, MaybeRunDecision::Run),
            "first in-wave settle for a ready dep should materialize as Run"
        );

        weak.apply_in_wave_receive_action(action);

        assert_eq!(
            run_count.get(),
            1,
            "borrowed in-wave action must still execute one fn run"
        );
        assert_eq!(
            observed_refs_in_fn.get(),
            baseline_refs,
            "fn execution from borrowed action must not re-count owner refs"
        );
        assert_eq!(derived.cache(), Some(4));
    }

    #[test]
    fn stale_in_wave_receive_action_is_generation_checked() {
        // A collected in-wave action is still keyed by the generation-scoped weak token.
        // Once the slot is reused, rehydrating the stale action must be a no-op.
        let arena = GraphArena::new();
        let source = Node::<i32>::state_in_arena(&arena, 1);
        let run_count = Rc::new(Cell::new(0usize));

        let (weak, old_key, action) = {
            let derived = Node::<i32>::derived_opts_in_arena(
                &arena,
                vec![source.erased()],
                NodeOpts::default(),
                {
                    let run_count = run_count.clone();
                    move |ctx| {
                        run_count.set(run_count.get() + 1);
                        ctx.emit(*ctx.data::<i32>(0).unwrap() + 1);
                    }
                },
            );
            let old_key = derived.core.key();
            let weak = derived.core.downgrade();
            let action = with_wave_owner(
                &source.core,
                || {
                    weak.collect_in_wave_receive_from_dep_action(0, &Message::Data(Rc::new(9i32)))
                        .expect("in-wave receive should collect a runnable action")
                },
                InWaveDepReceiveAction::default,
            );
            (weak, old_key, action)
        };

        let replacement = Node::<i32>::state_in_arena(&arena, 2);
        assert_eq!(old_key.id, replacement.core.key().id);
        assert_ne!(old_key.generation, replacement.core.key().generation);

        weak.apply_in_wave_receive_action(action);

        assert_eq!(
            run_count.get(),
            0,
            "stale in-wave action must not execute after generation drift"
        );
        assert_eq!(
            replacement.cache(),
            Some(2),
            "reused slot should keep its own initial cache when action is stale"
        );
    }

    #[test]
    fn in_flight_passthrough_decision_waits_for_delivery_boundary() {
        // R-diamond/D77: a single upstream msgs array may carry multiple DATA
        // occurrences. A no-fn passthrough must wait until the delivery boundary so
        // it relays the wave's latest DATA once instead of clearing its dep batch on
        // the first occurrence.
        let arena = GraphArena::new();
        let source = Node::<i32>::state_empty_in_arena(&arena);
        let wire = Node::<i32>::from_core(Core::new_in_arena(
            &arena,
            vec![source.erased()],
            None,
            default_dispatcher(),
            None,
            NodeOpts::default(),
        ));
        let seen = Rc::new(RefCell::new(Vec::<i32>::new()));
        let seen_sink = seen.clone();
        let _u = wire.subscribe(move |msg| {
            if let Message::Data(v) = msg {
                seen_sink
                    .borrow_mut()
                    .push(*v.downcast_ref::<i32>().expect("wire emits i32"));
            }
        });

        source.down(vec![
            Message::Data(Rc::new(1i32)),
            Message::Data(Rc::new(2i32)),
        ]);

        assert_eq!(
            seen.borrow().as_slice(),
            &[2],
            "passthrough must coalesce one upstream wave to its latest DATA"
        );
        assert_eq!(wire.cache(), Some(2));
    }

    #[test]
    fn in_flight_pause_resume_preserves_single_wave_projection() {
        // C-23/D77: deciding a paused skip during receive collection used to split
        // a multi-DATA upstream wave if another subscriber RESUMEd the node before
        // the delivery finished. The in-flight path must defer the whole maybe-run
        // decision until the delivery boundary so ctx.wave_data keeps [1, 2] in one
        // projection and the fn runs once.
        let arena = GraphArena::new();
        let source = Node::<i32>::state_empty_in_arena(&arena);
        let lock = LockId::from("qa-in-flight-resume");
        let batches_seen = Rc::new(RefCell::new(Vec::<Vec<i32>>::new()));
        let runs = Rc::new(Cell::new(0usize));
        let derived: Node<i32> =
            Node::derived_opts_in_arena(&arena, vec![source.erased()], NodeOpts::default(), {
                let batches_seen = batches_seen.clone();
                let runs = runs.clone();
                move |ctx| {
                    runs.set(runs.get() + 1);
                    batches_seen
                        .borrow_mut()
                        .push(ctx.batch::<i32>(0).into_iter().map(|v| *v).collect());
                    ctx.emit(*ctx.data::<i32>(0).expect("latest source DATA"));
                }
            });
        let _derived_sub = derived.subscribe(|_| {});
        derived.up(vec![Message::Pause(lock.clone())]);
        let derived_for_resume = derived.clone();
        let lock_for_resume = lock.clone();
        let _controller = source.subscribe(move |msg| {
            if matches!(msg, Message::Data(_)) {
                derived_for_resume.up(vec![Message::Resume(lock_for_resume.clone())]);
            }
        });

        source.down(vec![
            Message::Data(Rc::new(1i32)),
            Message::Data(Rc::new(2i32)),
        ]);

        assert_eq!(runs.get(), 1, "receiver fn should run once at boundary");
        assert_eq!(
            batches_seen.borrow().as_slice(),
            &[vec![1, 2]],
            "receiver must see one upstream wave projection, not two split runs"
        );
        assert_eq!(derived.cache(), Some(2));
    }

    #[test]
    fn internal_dep_callback_entry_out_of_wave_does_not_inflate_live_refcount() {
        // When the target is still publicly owned, a borrowed owner execution should be used
        // without increasing the temporary Core refcount visible to user callbacks.
        let arena = GraphArena::new();
        let source = Node::<i32>::state_empty_in_arena(&arena);
        let observed_refs = Rc::new(Cell::new(0usize));
        let derived: Node<i32> = Node::derived_opts_in_arena(
            &arena,
            vec![source.erased()],
            NodeOpts::default(),
            |ctx| {
                if let Some(v) = ctx.data::<i32>(0) {
                    ctx.emit(*v);
                }
            },
        );
        let weak = derived.core.downgrade();

        let _sub = derived.subscribe({
            let observed_refs = observed_refs.clone();
            let live_refs = derived.core.refs.clone();
            move |msg| {
                if matches!(msg, Message::Data(_)) {
                    observed_refs.set(live_refs.get());
                }
            }
        });
        let live_refs = derived.core.refs.clone();
        let baseline = live_refs.get();

        weak.receive_from_dep(0, &Message::Data(Rc::new(7i32)));

        assert_eq!(
            observed_refs.get(),
            baseline,
            "out-of-wave receive should not promote/ref-bump while still borrowed by arena pin"
        );
        assert_eq!(derived.cache(), Some(7));
        assert_eq!(
            derived.core.refs.get(),
            baseline,
            "out-of-wave borrowed execution should preserve live counted handle count"
        );
    }

    #[test]
    fn internal_dep_callback_entry_out_of_wave_refcount_dead_pin_stays_fail_closed() {
        // DR-8/B54 pin-first execution should work with refcount-dead live pins,
        // then fail closed once that temporary pin is released.
        let arena = GraphArena::new();
        let graph = arena.0.clone();
        let source = Node::<i32>::state_in_arena(&arena, 1);
        let derived: Node<i32> = Node::derived_opts_in_arena(
            &arena,
            vec![source.erased()],
            NodeOpts::default(),
            |ctx| ctx.emit(*ctx.data::<i32>(0).unwrap()),
        );
        let weak = derived.core.downgrade();
        let key = derived.core.key();
        let refs = derived.core.refs.clone();
        let borrowed = derived.core.borrowed_view();
        let pin = ArenaNodePin::from_core(&derived.core).expect("node must be pinnable while live");

        drop(derived);

        assert_eq!(
            refs.get(),
            0,
            "setup leaves no counted owner so this only exercises fail-closed pin behavior"
        );
        weak.receive_from_dep(0, &Message::Data(Rc::new(4i32)));
        assert_eq!(
            borrowed
                .cache_any()
                .and_then(|v| v.downcast_ref::<i32>().copied()),
            Some(4),
            "pinned borrowed receive should still execute while the slot is temporarily alive"
        );

        drop(pin);
        assert!(
            !graph.borrow().is_live_key(key),
            "pin release should allow slot cleanup"
        );
        assert!(
            weak.counted_core().is_none(),
            "fail-closed should not revive a dead counted handle after pin release"
        );
        assert!(
            weak.borrowed_core().is_none(),
            "fail-closed should not rehydrate from dead refs with no pin"
        );
        assert!(
            weak.pinned_borrowed_core().is_none(),
            "fail-closed should not construct a borrowed pin path after release"
        );
        weak.receive_from_dep(0, &Message::Data(Rc::new(5i32)));
        assert_eq!(
            refs.get(),
            0,
            "receive from a dead slot must not resurrect refs"
        );
    }

    #[test]
    fn internal_up_route_broadcast_uses_borrowed_deps() {
        // DR-8/B54: control fanout from an in-wave `up` path should traverse the
        // hot deps as borrowed arena views, so callback-visible refcounts must not
        // bump by one per forwarded target.
        let arena = GraphArena::new();
        let a = Node::<i32>::state_in_arena(&arena, 1);
        let b = Node::<i32>::state_in_arena(&arena, 2);
        let a_refs = a.core.refs.clone();
        let b_refs = b.core.refs.clone();
        let graph = arena.0.clone();
        let a_key = a.core.key();
        let b_key = b.core.key();
        let observed: Rc<RefCell<Vec<usize>>> = Rc::new(RefCell::new(Vec::new()));
        let observed_pins: Rc<RefCell<Vec<usize>>> = Rc::new(RefCell::new(Vec::new()));
        let _ua = a.subscribe({
            let observed = observed.clone();
            let observed_pins = observed_pins.clone();
            let a_refs = a_refs.clone();
            let graph = graph.clone();
            move |msg| {
                if matches!(msg, Message::Invalidate) {
                    observed.borrow_mut().push(a_refs.get());
                    observed_pins
                        .borrow_mut()
                        .push(graph.borrow().pin_count(a_key));
                }
            }
        });
        let _ub = b.subscribe({
            let observed = observed.clone();
            let observed_pins = observed_pins.clone();
            let b_refs = b_refs.clone();
            let graph = graph.clone();
            move |msg| {
                if matches!(msg, Message::Invalidate) {
                    observed.borrow_mut().push(b_refs.get());
                    observed_pins
                        .borrow_mut()
                        .push(graph.borrow().pin_count(b_key));
                }
            }
        });

        let parent = Node::<i32>::derived_opts_in_arena(
            &arena,
            vec![a.erased(), b.erased()],
            NodeOpts::default(),
            |_| {},
        );
        let baseline = (a_refs.get(), b_refs.get());
        parent.up(vec![Message::Invalidate]);

        let observed = observed.borrow();
        assert_eq!(
            observed.len(),
            2,
            "both deps should receive one in-wave INVALIDATE control wave"
        );
        assert_eq!(
            observed[0], baseline.0,
            "dep a must not pick up a temporary counted up-route ref bump"
        );
        assert_eq!(
            observed[1], baseline.1,
            "dep b must not pick up a temporary counted up-route ref bump"
        );
        assert_eq!(
            *observed_pins.borrow(),
            vec![2, 2],
            "in-wave borrowed up-route should keep the wave pin and add a scoped route pin around each dep callback"
        );
    }

    #[test]
    fn out_of_wave_up_route_broadcast_uses_borrowed_deps() {
        // DR-8/B54: direct `Core::up` control routing (no owning wave) should use
        // borrowed dep views rather than counted clones when forwarding out-of-wave.
        let arena = GraphArena::new();
        let a = Node::<i32>::state_in_arena(&arena, 1);
        let b = Node::<i32>::state_in_arena(&arena, 2);
        let a_refs = a.core.refs.clone();
        let b_refs = b.core.refs.clone();
        let graph = arena.0.clone();
        let a_key = a.core.key();
        let b_key = b.core.key();
        let observed: Rc<RefCell<Vec<usize>>> = Rc::new(RefCell::new(Vec::new()));
        let observed_pins: Rc<RefCell<Vec<usize>>> = Rc::new(RefCell::new(Vec::new()));
        let _ua = a.subscribe({
            let observed = observed.clone();
            let observed_pins = observed_pins.clone();
            let a_refs = a_refs.clone();
            let graph = graph.clone();
            move |msg| {
                if matches!(msg, Message::Invalidate) {
                    observed.borrow_mut().push(a_refs.get());
                    observed_pins
                        .borrow_mut()
                        .push(graph.borrow().pin_count(a_key));
                }
            }
        });
        let _ub = b.subscribe({
            let observed = observed.clone();
            let observed_pins = observed_pins.clone();
            let b_refs = b_refs.clone();
            let graph = graph.clone();
            move |msg| {
                if matches!(msg, Message::Invalidate) {
                    observed.borrow_mut().push(b_refs.get());
                    observed_pins
                        .borrow_mut()
                        .push(graph.borrow().pin_count(b_key));
                }
            }
        });

        let parent = Node::<i32>::derived_opts_in_arena(
            &arena,
            vec![a.erased(), b.erased()],
            NodeOpts::default(),
            |_| {},
        );
        let baseline = (a_refs.get(), b_refs.get());
        parent.core.up(vec![Message::Invalidate], None);

        let observed = observed.borrow();
        assert_eq!(
            observed.len(),
            2,
            "both deps should receive one out-of-wave INVALIDATE control wave"
        );
        assert_eq!(
            observed[0], baseline.0,
            "dep a must not pick up a temporary counted up-route ref bump"
        );
        assert_eq!(
            observed[1], baseline.1,
            "dep b must not pick up a temporary counted up-route ref bump"
        );
        assert_eq!(
            *observed_pins.borrow(),
            vec![1, 1],
            "out-of-wave borrowed up-route should hold an arena pin around each dep callback"
        );
    }

    #[test]
    fn public_wave_owner_entry_does_not_preclone_core() {
        // DR-8/B54 owner-execution prep: public down/up entry should enter the
        // wave-owner boundary with the existing owner handle instead of creating an
        // extra counted Core clone before the hot wave path runs. The scope itself
        // also stores only the owner's graph id; the touched-set remains the only
        // in-wave arena safety pin.
        let source = Node::<i32>::state_empty();
        let refs = source.core.refs.clone();
        let seen_refs = Rc::new(Cell::new(0usize));
        let seen = seen_refs.clone();
        let _u = source.subscribe(move |m| {
            if matches!(m, Message::Data(_)) {
                seen.set(refs.get());
            }
        });

        source.set(1);

        assert_eq!(
            seen_refs.get(),
            2,
            "expected source handle + unsubscribe handle; wave-owner/touched arena pins must not increment Core refs"
        );
    }

    #[test]
    fn touched_arena_pin_defers_slot_free_without_core_refcount_pin() {
        // DR-8/B54 owner-execution: a touched node is pinned by graph arena slot,
        // not by a counted Core clone. If the last public handle drops mid-wave,
        // cleanup/free waits until the arena pin releases at the wave boundary.
        let arena = GraphArena::new();
        let graph = arena.0.clone();
        let owner = Node::<i32>::state_in_arena(&arena, 0);
        let victim = Node::<i32>::state_in_arena(&arena, 1);
        let victim_key = victim.core.key();
        let victim_refs = victim.core.refs.clone();
        let graph_in_wave = graph.clone();

        with_wave_owner(
            &owner.core,
            move || {
                assert_eq!(victim_refs.get(), 1);
                wave_register(&victim.core);
                assert_eq!(
                    victim_refs.get(),
                    1,
                    "arena touched pin must not increment counted Core refs"
                );
                drop(victim);
                assert_eq!(
                    victim_refs.get(),
                    0,
                    "dropping the final public handle marks the slot externally dead"
                );
                assert!(
                    graph_in_wave.borrow().is_live_key(victim_key),
                    "arena pin keeps the touched slot live until wave exit"
                );
            },
            || {},
        );

        assert!(
            !graph.borrow().is_live_key(victim_key),
            "wave exit releases the arena pin and frees the externally-dead slot"
        );
        let reused = Node::<i32>::state_in_arena(&arena, 2);
        assert_eq!(victim_key.id, reused.core.key().id);
        assert_ne!(victim_key.generation, reused.core.key().generation);
    }

    #[test]
    fn touched_arena_pin_dedupes_same_node_within_wave() {
        // DR-8/B54 owner-execution: repeated callbacks to one node in a single wave
        // need one arena lifetime pin + one unwind-reset entry. DIRTY, DATA, and
        // run_wave may all touch the same node; duplicate pins only add hot-path tax.
        let arena = GraphArena::new();
        let graph = arena.0.clone();
        let owner = Node::<i32>::state_in_arena(&arena, 0);
        let victim = Node::<i32>::state_in_arena(&arena, 1);
        let victim_key = victim.core.key();
        let victim_refs = victim.core.refs.clone();
        let graph_in_wave = graph.clone();

        with_wave_owner(
            &owner.core,
            move || {
                wave_register(&victim.core);
                wave_register(&victim.core);
                wave_register(&victim.core);
                WAVE.with(|w| {
                    let wave = w.borrow();
                    let scope = wave.as_ref().expect("wave installed");
                    assert_eq!(scope.touched.len(), 1);
                });
                assert_eq!(
                    graph_in_wave.borrow().pin_count(victim_key),
                    1,
                    "duplicate touches should share one arena pin"
                );
                drop(victim);
                assert_eq!(
                    victim_refs.get(),
                    0,
                    "the deduped arena pin still allows external refs to reach zero"
                );
                assert!(
                    graph_in_wave.borrow().is_live_key(victim_key),
                    "one deduped pin must still keep the slot live through wave exit"
                );
            },
            || {},
        );

        assert!(
            !graph.borrow().is_live_key(victim_key),
            "deduped pin releases at wave exit and frees externally-dead slot"
        );
    }

    #[test]
    fn touched_arena_pins_release_before_committed_boundary_drain() {
        // R-rewire-deferred/D47 boundary tasks are fresh waves. A touched node whose
        // final public handle dropped during the prior wave must be cleaned up before
        // queued boundary tasks run, not kept alive until after the drain.
        let arena = GraphArena::new();
        let graph = arena.0.clone();
        let owner = Node::<i32>::state_in_arena(&arena, 1);
        let victim = Node::<i32>::state_in_arena(&arena, 2);
        let victim_key = victim.core.key();
        let saw_boundary = Rc::new(Cell::new(false));
        let saw = saw_boundary.clone();
        let graph_for_sink = graph.clone();
        let _u = owner.subscribe(move |m| {
            if matches!(m, Message::Invalidate) {
                saw.set(true);
                assert!(
                    !graph_for_sink.borrow().is_live_key(victim_key),
                    "touched pins must release before committed-boundary tasks run"
                );
            }
        });
        let owner_core = owner.core.clone();

        with_wave_owner(
            &owner.core,
            move || {
                wave_register(&victim.core);
                drop(victim);
                owner_core.request_up_next(vec![Message::Invalidate], None);
            },
            || {},
        );

        assert!(saw_boundary.get(), "queued boundary INVALIDATE should run");
        let reused = Node::<i32>::state_in_arena(&arena, 3);
        assert_eq!(victim_key.id, reused.core.key().id);
        assert_ne!(victim_key.generation, reused.core.key().generation);
    }

    #[test]
    fn run_wave_ctx_borrows_core_until_defer_promotes() {
        // DR-8/B54 delivery-waist follow-up: a synchronous fn-body Ctx is used only
        // while the wave touched-set already pins the owner in the arena, so it can be
        // an uncounted arena view. The async escape hatch ctx.defer() must promote to a
        // counted Core because DeferredCtx can outlive the wave.
        let refs_slot: Rc<RefCell<Option<Rc<Cell<usize>>>>> = Rc::new(RefCell::new(None));
        let sync_refs = Rc::new(Cell::new(0usize));
        let deferred_refs = Rc::new(Cell::new(0usize));
        let deferred_ctx: Rc<RefCell<Option<crate::ctx::DeferredCtx>>> =
            Rc::new(RefCell::new(None));

        let p = Node::<i32>::producer({
            let refs_slot = refs_slot.clone();
            let sync_refs = sync_refs.clone();
            let deferred_refs = deferred_refs.clone();
            let deferred_ctx = deferred_ctx.clone();
            move |ctx| {
                let refs = refs_slot
                    .borrow()
                    .as_ref()
                    .expect("test installed ref counter before activation")
                    .clone();
                sync_refs.set(refs.get());
                *deferred_ctx.borrow_mut() = Some(ctx.defer());
                deferred_refs.set(refs.get());
                ctx.emit(1i32);
            }
        });
        *refs_slot.borrow_mut() = Some(p.core.refs.clone());

        let _u = p.subscribe(|_| {});

        assert_eq!(
            sync_refs.get(),
            1,
            "expected only the public handle; subscribe owner execution, touched arena pin, and Ctx itself must not count"
        );
        assert_eq!(
            deferred_refs.get(),
            2,
            "ctx.defer() promotes the borrowed fn-body Ctx to a counted async handle"
        );
        assert_eq!(
            p.core.refs.get(),
            3,
            "after activation, public handle + unsubscribe handle + stashed DeferredCtx remain"
        );
        drop(deferred_ctx.borrow_mut().take());
        assert_eq!(
            p.core.refs.get(),
            2,
            "dropping the deferred async handle releases its counted Core pin"
        );
    }

    #[test]
    fn run_state_slot_resets_on_deactivation_while_call_slot_persists() {
        // DR-8/B54 next slice: first-run lifecycle state lives in GraphCore run_slots,
        // while the dispatcher handle lives in call_slots. Deactivation must re-arm the
        // producer without unregistering/re-registering its callable.
        let runs = Rc::new(Cell::new(0usize));
        let p = Node::<i32>::producer({
            let runs = runs.clone();
            move |ctx| {
                runs.set(runs.get() + 1);
                ctx.emit(runs.get() as i32);
            }
        });
        let handle_before = p.core.handle().expect("producer registered a call slot");

        let u = {
            let u = p.subscribe(|_| {});
            assert_eq!(runs.get(), 1);
            assert!(p
                .core
                .with_node_state(|_n, _c, _cfg, r, _e| r.has_called_fn_once));
            u
        };
        u();

        assert!(
            !p.core
                .with_node_state(|_n, _c, _cfg, r, _e| r.has_called_fn_once),
            "deactivation resets run_slots.has_called_fn_once"
        );
        assert_eq!(
            p.core.handle(),
            Some(handle_before),
            "deactivation keeps the call slot registered for the next lifecycle"
        );

        let _u2 = p.subscribe(|_| {});
        assert_eq!(
            runs.get(),
            2,
            "run state reset re-arms the depless producer on the next activation"
        );
        assert_eq!(p.cache(), Some(2));
    }

    #[test]
    fn config_slot_partial_gate_runs_with_sentinel_dep() {
        // DR-8/B54 next slice: first-run config moved out of NodeInner. The partial
        // flag still opens the gate for a SENTINEL dep once the node is asked to run;
        // without config-slot wiring this derived node would wait for source DATA.
        let source = Node::<i32>::state_empty();
        let d: Node<i32> = Node::derived_opts(
            vec![source.erased()],
            NodeOpts {
                partial: true,
                ..NodeOpts::default()
            },
            |ctx| {
                assert!(ctx.data::<i32>(0).is_none());
                ctx.emit(7i32);
            },
        );
        assert!(d.core.with_config(|cfg| cfg.partial));

        let _u = d.subscribe(|_| {});
        d.core.try_run();

        assert_eq!(d.cache(), Some(7));
        assert!(d
            .core
            .with_node_state(|_n, _c, _cfg, r, _e| r.has_called_fn_once));
    }

    #[test]
    fn reachable_upstream_uses_arena_keys_without_refcount_churn() {
        // DR-8/B54 retained-topology safe subset: the cycle-prevention DFS should walk
        // graph-owned generation keys from the topology side-table without allocating
        // extra counted Core clones for traversal.
        let a = Node::<i32>::state(1);
        let b: Node<i32> = Node::derived(vec![a.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap())
        });
        let c: Node<i32> = Node::derived(vec![b.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap())
        });
        let before = (a.core.refs.get(), b.core.refs.get(), c.core.refs.get());

        assert!(reachable_upstream(&c.core, &a.core));
        assert!(!reachable_upstream(&a.core, &c.core));

        assert_eq!(
            (a.core.refs.get(), b.core.refs.get(), c.core.refs.get()),
            before,
            "reachable_upstream must not allocate extra counted Core clones for traversal"
        );
    }

    #[test]
    fn reachable_upstream_stale_key_fails_closed() {
        // DR-8/B54 fail-closed safety: stale/liveness-checked keys in cycle-prevention DFS
        // should be inert and never follow dead arena slots.
        let arena = GraphArena::new();
        let graph = arena.0.clone();

        let victim = Node::<i32>::state_in_arena(&arena, 1);
        let victim_key = victim.core.key();
        drop(victim);

        let reused = Node::<i32>::state_in_arena(&arena, 2);
        assert_eq!(
            victim_key.id,
            reused.core.key().id,
            "slot should be reused so stale generation is visible"
        );
        assert_ne!(
            victim_key.generation,
            reused.core.key().generation,
            "reused slot must advance generation"
        );
        assert!(
            !graph.borrow().is_live_key(victim_key),
            "old generation key is dead after slot reuse"
        );

        let stale_source = Core::from_borrowed_parts(
            graph.clone(),
            victim_key.id,
            victim_key.generation,
            Rc::new(Cell::new(0)),
        );
        assert!(
            !reachable_upstream(&stale_source, &reused.core),
            "stale source key should never reach a live target"
        );
        assert!(
            !reachable_upstream(&reused.core, &stale_source),
            "stale target key should never be considered reachable"
        );
        assert!(
            !reachable_upstream(&stale_source, &stale_source),
            "stale source and target keys should still fail closed"
        );
    }

    #[test]
    fn topology_slot_retains_inactive_declared_deps() {
        // DR-8/B54 final topology contract: declared deps are graph-owned topology
        // until explicit rewire/remove/drop. An inactive derived node must not lose
        // an edge merely because the caller drops the original dep handle.
        let arena = GraphArena::new();
        let source = Node::<i32>::state_in_arena(&arena, 1);
        let source_key = source.core.key();
        let refs = source.core.refs.clone();
        let derived: Node<i32> = Node::derived_opts_in_arena(
            &arena,
            vec![source.erased()],
            NodeOpts::default(),
            |ctx| ctx.emit(*ctx.data::<i32>(0).unwrap()),
        );

        assert_eq!(
            refs.get(),
            2,
            "inactive topology retains the declared dep in addition to the caller handle"
        );
        assert_eq!(derived.core.borrow().deps.len(), 1);
        drop(source);

        assert!(
            arena.0.borrow().is_live_key(source_key),
            "declared topology keeps the dep live after the original handle is dropped"
        );
        assert_eq!(refs.get(), 1);
        assert_eq!(derived.core.deps().len(), 1);

        let u = derived.subscribe(|_| {});
        assert_eq!(
            derived.cache(),
            Some(1),
            "activation must still subscribe the retained declared dep"
        );
        u();
        drop(derived);
        assert!(
            !arena.0.borrow().is_live_key(source_key),
            "dropping the owner topology releases the last retained dep handle"
        );
        assert_eq!(refs.get(), 0);
    }

    #[test]
    fn activation_subscribes_retained_topology_deps() {
        // Once activated, the subscription edge owns an unsubscribe closure that keeps
        // the dep alive until deactivation. Topology itself also retains the declared edge.
        let arena = GraphArena::new();
        let source = Node::<i32>::state_in_arena(&arena, 1);
        let refs = source.core.refs.clone();
        let derived: Node<i32> = Node::derived_opts_in_arena(
            &arena,
            vec![source.erased()],
            NodeOpts::default(),
            |ctx| ctx.emit(*ctx.data::<i32>(0).unwrap() + 1),
        );
        assert_eq!(refs.get(), 2);

        let u = derived.subscribe(|_| {});
        assert!(
            refs.get() > 2,
            "activation should add a subscription-owned counted dep handle"
        );
        assert_eq!(derived.cache(), Some(2));
        u();
        assert_eq!(
            refs.get(),
            2,
            "deactivation releases only the subscription-owned dep handle"
        );
    }

    #[test]
    fn borrowed_core_cannot_promote_after_external_death() {
        // DR-8/B54 fail-closed lifetime rule: an arena pin may keep a slot live for
        // the current synchronous wave after the last public handle drops, but a
        // borrowed Core must not promote that externally-dead slot into a new counted
        // handle.
        let arena = GraphArena::new();
        let graph = arena.0.clone();
        let victim = Node::<i32>::state_in_arena(&arena, 1);
        let victim_key = victim.core.key();
        let victim_refs = victim.core.refs.clone();
        let pin = ArenaNodePin::from_core(&victim.core).expect("live slot pins");
        let borrowed = pin.borrowed_core().expect("pin can create borrowed view");
        drop(victim);

        assert_eq!(victim_refs.get(), 0);
        assert!(
            graph.borrow().is_live_key(victim_key),
            "arena pin keeps the externally-dead slot live only for the current wave"
        );
        let result = catch_unwind(AssertUnwindSafe(|| {
            let _revived = borrowed.clone();
        }));
        assert!(
            result.is_err(),
            "borrowed Core clone must fail closed instead of reviving refs from zero"
        );
        drop(borrowed);
        drop(pin);
        assert!(!graph.borrow().is_live_key(victim_key));
        let reused = Node::<i32>::state_in_arena(&arena, 2);
        assert_eq!(victim_key.id, reused.core.key().id);
        assert_ne!(victim_key.generation, reused.core.key().generation);
    }

    #[test]
    fn boundary_owner_actions_do_not_pin_core_while_queued() {
        // DR-8/B54: `ctx.up_next` / `ctx.rewire_next` boundary tasks store a
        // generation-checked owner token while queued, not a counted owner `Core`.
        // The drain upgrades to a counted handle only for execution, preserving
        // callback/drop safety without retaining the owner for the queue lifetime.
        let source = Node::<i32>::state(1);
        let refs = source.core.refs.clone();

        with_wave_owner(
            &source.core,
            || {
                let owner_refs = refs.get();
                source.core.request_up_next(vec![Message::Dirty], None);
                assert_eq!(
                    refs.get(),
                    owner_refs,
                    "queued up_next stores an owner token, not a counted Core"
                );

                source
                    .core
                    .request_rewire_next(RewireRequest::Set(vec![], Rc::new(|ctx| ctx.emit(2i32))));
                assert_eq!(
                    refs.get(),
                    owner_refs,
                    "queued rewire_next stores an owner token, not a counted Core"
                );
            },
            || {},
        );

        assert_eq!(
            refs.get(),
            1,
            "boundary execution releases its temporary counted handle"
        );
    }

    #[test]
    fn boundary_up_task_executes_with_borrowed_pinned_owner() {
        // DR-8/B54: committed-boundary owner actions execute via an arena pin +
        // borrowed Core view. The queue still does not retain the owner, but the drain
        // also avoids a temporary counted promotion while running the task.
        let source = Node::<i32>::state(1);
        let refs = source.core.refs.clone();
        let seen_refs = Rc::new(Cell::new(usize::MAX));
        let seen = seen_refs.clone();
        let refs_for_sink = refs.clone();
        let _u = source.subscribe(move |m| {
            if matches!(m, Message::Invalidate) {
                seen.set(refs_for_sink.get());
            }
        });
        let refs_before = refs.get();

        with_wave_owner(
            &source.core,
            || source.core.request_up_next(vec![Message::Invalidate], None),
            || {},
        );

        assert_eq!(
            seen_refs.get(),
            refs_before,
            "boundary up task should not add a counted owner Core while executing"
        );
        assert_eq!(refs.get(), refs_before);
    }

    #[test]
    fn boundary_rewire_task_executes_with_borrowed_pinned_owner() {
        // Same owner-execution invariant for the rewire boundary task. The added cached
        // dep drives the queued fn during boundary drain, where old code had both a
        // task-level counted promotion and a rewire_inner pre-clone.
        let a = Node::<i32>::state(1);
        let b = Node::<i32>::state(10);
        let observed_refs = Rc::new(Cell::new(usize::MAX));
        let d: Node<i32> = Node::derived(vec![a.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap())
        });
        let _u = d.subscribe(|_| {});
        let refs = d.core.refs.clone();
        let refs_before = refs.get();

        with_wave_owner(
            &d.core,
            || {
                let observed_refs = observed_refs.clone();
                let refs = refs.clone();
                d.core.request_rewire_next(RewireRequest::Set(
                    vec![b.erased()],
                    Rc::new(move |ctx| {
                        observed_refs.set(refs.get());
                        ctx.emit(*ctx.data::<i32>(0).unwrap());
                    }),
                ));
            },
            || {},
        );

        assert_eq!(
            observed_refs.get(),
            refs_before,
            "boundary rewire task should run without counted owner promotions"
        );
        assert_eq!(refs.get(), refs_before);
        assert_eq!(d.cache(), Some(10));
    }

    #[test]
    fn deferred_rewire_remove_does_not_pin_removed_dep_while_queued() {
        // DR-8/B54: a queued remove only needs dep identity. Add/Set must retain
        // runtime-created deps until the boundary, but Remove should not add another
        // counted dep pin while waiting to apply.
        let dep = Node::<i32>::state(1);
        let derived: Node<i32> = Node::derived(vec![dep.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap())
        });
        let _u = derived.subscribe(|_| {});
        let refs = dep.core.refs.clone();

        with_wave_owner(
            &derived.core,
            || {
                let before = refs.get();
                derived
                    .core
                    .request_rewire_next(RewireRequest::remove(&dep.core, Rc::new(|_| {})));
                assert_eq!(
                    refs.get(),
                    before,
                    "queued remove stores dep identity, not a counted dep Core"
                );
            },
            || {},
        );

        assert!(
            derived.core.deps().is_empty(),
            "queued remove still applies at the committed boundary"
        );
    }

    #[test]
    fn core_identity_does_not_match_reused_slot_generation() {
        // CoreIdentity is used by queued remove requests: it must be graph+generation
        // identity, not just a slot id, or a stale remove could target a later occupant.
        let arena = GraphArena::new();
        let old = Node::<i32>::state_in_arena(&arena, 1);
        let old_identity = CoreIdentity::from_core(&old.core);
        let old_key = old.core.key();
        drop(old);

        let reused = Node::<i32>::state_in_arena(&arena, 2);
        assert_eq!(old_key.id, reused.core.key().id);
        assert_ne!(old_key.generation, reused.core.key().generation);
        assert!(
            !old_identity.matches(&reused.core),
            "stale identity must not match a reused arena slot"
        );
    }

    #[test]
    fn queued_remove_identity_noops_after_prior_remove_and_preserves_later_add() {
        // Multiple ctx.rewire_next requests compose at apply time. A later remove of
        // an already-removed dep must be a no-op and must not remove a replacement dep
        // that was added in between.
        let old = Node::<i32>::state(1);
        let replacement = Node::<i32>::state(2);
        let derived: Node<i32> = Node::derived(vec![old.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap())
        });
        let _u = derived.subscribe(|_| {});

        with_wave_owner(
            &derived.core,
            || {
                derived
                    .core
                    .request_rewire_next(RewireRequest::remove(&old.core, Rc::new(|_| {})));
                derived.core.request_rewire_next(RewireRequest::Add(
                    replacement.erased(),
                    Rc::new(|ctx| {
                        if let Some(v) = ctx.data::<i32>(0) {
                            ctx.emit(*v);
                        }
                    }),
                ));
                derived
                    .core
                    .request_rewire_next(RewireRequest::remove(&old.core, Rc::new(|_| {})));
            },
            || {},
        );

        let deps = derived.core.deps();
        assert_eq!(deps.len(), 1);
        assert!(
            deps[0].ptr_eq(&replacement.core),
            "second stale remove must not disturb the replacement dep"
        );
        assert_eq!(
            derived.cache(),
            Some(2),
            "replacement add should install the replacement fn"
        );

        replacement.set(3);
        assert_eq!(
            derived.cache(),
            Some(3),
            "stale remove must not swap in its stale fn after the replacement add"
        );
    }

    #[test]
    fn stale_boundary_owner_action_does_not_target_reused_slot() {
        // A queued boundary action may outlive its owner if the owner is dropped before a
        // later explicit drain. The weak owner token + generation check must turn that
        // action into a no-op, never an operation on a later node that reused the slot.
        let arena = GraphArena::new();
        let old = Node::<i32>::state_in_arena(&arena, 1);
        let old_key = old.core.key();
        old.core.request_up_next(vec![Message::Invalidate], None);
        drop(old);

        let reused = Node::<i32>::state_in_arena(&arena, 2);
        assert_eq!(old_key.id, reused.core.key().id);
        assert_ne!(old_key.generation, reused.core.key().generation);

        drain_committed_boundary(&reused.core);

        assert_eq!(reused.status(), Status::Settled);
        assert_eq!(
            reused.cache(),
            Some(2),
            "stale queued owner action must not invalidate the reused slot occupant"
        );
    }

    #[test]
    fn nested_cross_arena_boundary_task_drains_at_outer_committed_boundary() {
        // Nested public calls into another GraphArena share the thread-local WAVE scope.
        // A boundary task queued by the inner arena must still drain at the outer
        // committed boundary, not wait for a later wave in that inner arena.
        let arena_a = GraphArena::new();
        let arena_b = GraphArena::new();
        let outer = Node::<i32>::state_empty_in_arena(&arena_a);
        let inner_src = Node::<i32>::state_empty_in_arena(&arena_b);
        let inner_dep = inner_src.erased();
        let replacement_dep = inner_dep.clone();
        let inner: Node<i32> =
            Node::derived_opts_in_arena(&arena_b, vec![inner_dep.clone()], NodeOpts::default(), {
                let replacement_dep = replacement_dep.clone();
                move |ctx| {
                    ctx.rewire_next_replace_deps(vec![replacement_dep.clone()], |next| {
                        next.emit(*next.data::<i32>(0).unwrap() * 10)
                    });
                    ctx.emit(*ctx.data::<i32>(0).unwrap());
                }
            });
        let _inner_sub = inner.subscribe(|_| {});
        let inner_src_for_outer = inner_src.clone();
        let _outer_sub = outer.subscribe(move |m| {
            if matches!(m, Message::Data(_)) {
                inner_src_for_outer.set(1);
            }
        });

        outer.set(1);
        assert_eq!(inner.cache(), Some(1));

        inner_src.set(2);
        assert_eq!(
            inner.cache(),
            Some(20),
            "inner arena rewire_next must have drained before the next inner wave"
        );
    }

    #[test]
    fn batch_boundary_root_tracking_does_not_pin_queued_owner() {
        // DR-8/B54: batch needs to remember which graph queues boundary work, but that
        // graph-root bookkeeping must not retain the node owner. Owner liveness remains
        // exclusively in the queued task's weak CoreToken.
        let source = Node::<i32>::state(1);
        let refs = source.core.refs.clone();

        crate::batch::batch(|_| {
            let before = refs.get();
            source.core.request_up_next(vec![Message::Dirty], None);
            assert_eq!(
                refs.get(),
                before,
                "batch boundary root is a weak graph root, not a counted Core"
            );
        });

        assert_eq!(refs.get(), 1);
    }

    #[test]
    fn batch_deferred_external_rewire_does_not_pin_owner_while_queued() {
        // D67 queues an external rewire until after the target's batched settle commits.
        // DR-8/B54: that queued owner action uses a weak generation token, not a counted
        // owner Core captured by a generic thunk.
        let a = Node::<i32>::state(1);
        let b = Node::<i32>::state(2);
        let d: Node<i32> = Node::derived(vec![a.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap())
        });
        let _u = d.subscribe(|_| {});

        crate::batch::batch(|_| {
            a.set(3);
            let refs_after_batched_settle = d.core.refs.get();
            d.replace_deps(vec![b.erased()], |ctx| {
                ctx.emit(*ctx.data::<i32>(0).unwrap() * 10)
            });
            assert_eq!(
                d.core.refs.get(),
                refs_after_batched_settle,
                "queued batch-deferred external rewire must not retain a counted owner Core"
            );
        });

        assert_eq!(
            d.cache(),
            Some(20),
            "batch-deferred external rewire still drains at the committed boundary"
        );
    }

    #[test]
    fn batch_boundary_task_executes_after_last_public_owner_drops() {
        // DR-8/B54: a queued boundary task can run while the public handle is gone,
        // as long as the batch target pin keeps the arena slot live during commit.
        let arena = GraphArena::new();
        let graph = arena.0.clone();
        let dep = Node::<i32>::state_in_arena(&arena, 10);
        let source = Node::<i32>::state_in_arena(&arena, 1);
        source.core.activate();
        let key = source.core.key();
        let refs = source.core.refs.clone();
        let seen_refs = Rc::new(Cell::new(usize::MAX));

        crate::batch::batch({
            let dep = dep.erased();
            let graph = graph.clone();
            let refs = refs.clone();
            let seen_refs = seen_refs.clone();
            move |_| {
                let owned = source;
                owned.set(2);
                owned.replace_deps(vec![dep], {
                    let refs = refs.clone();
                    let seen_refs = seen_refs.clone();
                    move |ctx| {
                        seen_refs.set(refs.get());
                        ctx.emit(*ctx.data::<i32>(0).expect("cached added dep data"));
                    }
                });
                drop(owned);
                assert_eq!(
                    refs.get(),
                    0,
                    "batch-deferred external rewire owner can drop to zero while queued"
                );
                assert!(
                    graph.borrow().pin_count(key) >= 1,
                    "batch target pin keeps the arena slot live during commit"
                );
                assert!(
                    graph.borrow().is_live_key(key),
                    "batch target pin keeps slot live during committed-boundary drain"
                );
            }
        });

        assert_eq!(
            seen_refs.get(),
            0,
            "batch-deferred external rewire executes from borrowed pin when refs reach zero"
        );
        assert!(
            !graph.borrow().is_live_key(key),
            "batch pin should release when commit finishes"
        );
    }

    #[test]
    fn defer_to_batch_does_not_increment_refs() {
        let source = Node::<i32>::state(1);
        let refs = source.core.refs.clone();
        let before = refs.get();

        crate::batch::batch(|_| {
            source.set(2);
            source.set(3);
            source.set(4);
            assert_eq!(
                refs.get(),
                before,
                "defer_to_batch should use generation-checked batch pins, not counted Core handles"
            );
        });

        assert_eq!(
            refs.get(),
            before,
            "collecting batch should not permanently retain extra refs"
        );
        assert_eq!(source.cache(), Some(4));
    }

    #[test]
    fn dropping_last_public_handle_inside_batch_keeps_slot_live_for_commit_and_rollback() {
        let arena = GraphArena::new();
        let graph = arena.0.clone();

        {
            let source = Node::<i32>::state_in_arena(&arena, 0);
            let refs = source.core.refs.clone();
            let key = source.core.key();
            let initial_refs = refs.get();
            assert_eq!(initial_refs, 1);

            crate::batch::batch({
                let graph = graph.clone();
                let refs = refs.clone();
                move |_| {
                    let owned = source;
                    owned.set(1);
                    drop(owned);

                    assert_eq!(
                        refs.get(),
                        0,
                        "explicitly dropping the public handle should leave no counted owners"
                    );
                    assert!(
                        graph.borrow().is_live_key(key),
                        "arena pin keeps the node slot live for open batch finish"
                    );
                }
            });

            assert_eq!(refs.get(), 0);
            assert!(
                !graph.borrow().is_live_key(key),
                "batch pin should release when commit finishes"
            );
        };

        let source = Node::<i32>::state_in_arena(&arena, 0);
        let refs = source.core.refs.clone();
        let key = source.core.key();
        crate::batch::batch({
            let graph = graph.clone();
            let refs = refs.clone();
            move |bctx| {
                let owned = source;
                owned.set(2);
                drop(owned);

                assert_eq!(
                    refs.get(),
                    0,
                    "explicit rollback should not restore a counted batch owner reference"
                );
                assert!(
                    graph.borrow().is_live_key(key),
                    "rollback also runs with a pinned slot while the batch remains open"
                );
                bctx.rollback();
            }
        });

        assert_eq!(refs.get(), 0);
        assert!(
            !graph.borrow().is_live_key(key),
            "batch pin should release after rollback too"
        );
    }

    #[test]
    fn batch_target_stale_generation_does_not_borrow_reused_slot() {
        let arena = GraphArena::new();
        let graph = arena.0.clone();
        let stale_target: BatchTarget;
        let old_key;
        {
            let node = Node::<i32>::state_in_arena(&arena, 1);
            old_key = node.core.key();
            stale_target =
                BatchTarget::from_core(&node.core).expect("pin tracks live batch target");
            let borrowed = stale_target
                .borrowed_core()
                .expect("pin may borrow while owner is alive");
            assert_eq!(
                borrowed
                    .cache_any()
                    .and_then(|value| value.downcast_ref::<i32>().copied()),
                Some(1)
            );
            drop(node);
            assert!(
                graph.borrow().is_live_key(old_key),
                "pin keeps dead owner slot alive while batch-target lives"
            );
        }

        drop(stale_target);
        assert!(!graph.borrow().is_live_key(old_key));

        let reused = Node::<i32>::state_in_arena(&arena, 2);
        assert_eq!(old_key.id, reused.core.key().id);
        assert_ne!(old_key.generation, reused.core.key().generation);
        assert!(
            graph.borrow().is_live_key(reused.core.key()),
            "reused slot is live with the new generation"
        );
    }

    #[test]
    fn stale_external_rewire_token_does_not_target_reused_slot() {
        // DR-8/B54 boundary freshness: ExternalRewire queues carry a weak generation
        // token for the owner. A stale queued owner must no-op after slot reuse, never
        // apply a topology/fn swap to the new occupant.
        let arena = GraphArena::new();
        let old = Node::<i32>::state_in_arena(&arena, 1);
        let old_key = old.core.key();
        let committed = Rc::new(Cell::new(true));
        defer_boundary(
            &old.core,
            BoundaryTask::ExternalRewire {
                target: CoreToken::from_core(&old.core),
                new_deps: vec![],
                fn_: Rc::new(|ctx| ctx.emit(999i32)),
                committed,
            },
        );
        drop(old);

        let reused = Node::<i32>::state_in_arena(&arena, 2);
        assert_eq!(old_key.id, reused.core.key().id);
        assert_ne!(old_key.generation, reused.core.key().generation);

        drain_committed_boundary(&reused.core);

        assert_eq!(reused.cache(), Some(2));
        assert_eq!(
            reused.status(),
            Status::Settled,
            "stale ExternalRewire token must not disturb the reused slot occupant"
        );
        assert!(
            reused.core.deps().is_empty(),
            "state node topology remains unchanged after stale external rewire drains"
        );
    }

    #[test]
    fn boundary_task_queued_during_batch_commit_drains_before_next_wave() {
        // A batch commit wave can synchronously enter another arena while ACTIVE_BATCH is
        // still installed (`collecting=false`). Roots registered during that commit must
        // be merged before clear/drain, or the inner queued rewire waits for a later wave.
        let arena_a = GraphArena::new();
        let arena_b = GraphArena::new();
        let outer = Node::<i32>::state_empty_in_arena(&arena_a);
        let inner_src = Node::<i32>::state_empty_in_arena(&arena_b);
        let inner_dep = inner_src.erased();
        let replacement_dep = inner_dep.clone();
        let inner: Node<i32> =
            Node::derived_opts_in_arena(&arena_b, vec![inner_dep.clone()], NodeOpts::default(), {
                let replacement_dep = replacement_dep.clone();
                move |ctx| {
                    ctx.rewire_next_replace_deps(vec![replacement_dep.clone()], |next| {
                        next.emit(*next.data::<i32>(0).unwrap() * 10)
                    });
                    ctx.emit(*ctx.data::<i32>(0).unwrap());
                }
            });
        let _inner_sub = inner.subscribe(|_| {});
        let inner_src_for_commit = inner_src.clone();
        let _outer_sub = outer.subscribe(move |m| {
            if matches!(m, Message::Data(_)) {
                inner_src_for_commit.set(1);
            }
        });

        crate::batch::batch(|_| {
            outer.set(1);
        });
        assert_eq!(inner.cache(), Some(1));

        inner_src.set(2);
        assert_eq!(
            inner.cache(),
            Some(20),
            "inner rewire queued during batch commit must drain before the next inner wave"
        );
    }

    #[test]
    fn wave_boundary_drains_later_roots_after_earlier_root_panics() {
        // Boundary drains isolate panics per queued task and per graph root: the first
        // panic still escapes, but not before every registered root reaches its boundary.
        let arena_a = GraphArena::new();
        let arena_b = GraphArena::new();
        let a = Node::<i32>::state_in_arena(&arena_a, 1);
        let b_src = Node::<i32>::state_in_arena(&arena_b, 2);
        let b: Node<i32> = Node::derived_opts_in_arena(
            &arena_b,
            vec![b_src.erased()],
            NodeOpts::default(),
            |ctx| ctx.emit(*ctx.data::<i32>(0).unwrap()),
        );
        let _u = b.subscribe(|_| {});
        let committed = Rc::new(Cell::new(true));

        let result = catch_unwind(AssertUnwindSafe(|| {
            with_wave_owner(
                &a.core,
                || {
                    defer_boundary(
                        &a.core,
                        BoundaryTask::ExternalRewire {
                            target: CoreToken::from_core(&a.core),
                            new_deps: vec![a.erased()],
                            fn_: Rc::new(|_| {}),
                            committed: committed.clone(),
                        },
                    );
                    defer_boundary(
                        &b.core,
                        BoundaryTask::ExternalRewire {
                            target: CoreToken::from_core(&b.core),
                            new_deps: vec![],
                            fn_: Rc::new(|ctx| ctx.emit(99i32)),
                            committed: committed.clone(),
                        },
                    );
                },
                || {},
            );
        }));

        assert!(result.is_err());
        assert!(
            b.core.borrow().deps.is_empty(),
            "a panic in the owner graph's boundary queue must not strand later graph roots"
        );
    }

    #[test]
    fn batch_boundary_drains_later_roots_after_earlier_root_panics() {
        // Same isolation as the wave boundary, but through the batch committed-boundary
        // root set. A panicking root must not skip later roots before the panic escapes.
        let arena_a = GraphArena::new();
        let arena_b = GraphArena::new();
        let a = Node::<i32>::state_in_arena(&arena_a, 1);
        let b_src = Node::<i32>::state_in_arena(&arena_b, 2);
        let b: Node<i32> = Node::derived_opts_in_arena(
            &arena_b,
            vec![b_src.erased()],
            NodeOpts::default(),
            |ctx| ctx.emit(*ctx.data::<i32>(0).unwrap()),
        );
        let _u = b.subscribe(|_| {});
        let committed = Rc::new(Cell::new(true));

        let result = catch_unwind(AssertUnwindSafe(|| {
            crate::batch::batch(|_| {
                defer_boundary(
                    &a.core,
                    BoundaryTask::ExternalRewire {
                        target: CoreToken::from_core(&a.core),
                        new_deps: vec![a.erased()],
                        fn_: Rc::new(|_| {}),
                        committed: committed.clone(),
                    },
                );
                defer_boundary(
                    &b.core,
                    BoundaryTask::ExternalRewire {
                        target: CoreToken::from_core(&b.core),
                        new_deps: vec![],
                        fn_: Rc::new(|ctx| ctx.emit(99i32)),
                        committed: committed.clone(),
                    },
                );
            });
        }));

        assert!(result.is_err());
        assert!(
            b.core.borrow().deps.is_empty(),
            "a panic in one batch boundary root must not strand later graph roots"
        );
    }

    #[test]
    fn terminal_owner_rewire_drains_topology_without_post_terminal_settle() {
        // D62: terminal drains queued topology but terminal-is-forever still seals output.
        // Force the old hazard shape: a terminal owner also has a stale dirty contribution
        // from a dep being removed. The rewire must apply cleanup but skip settle_rewire /
        // zero-dep undirty so no post-terminal DIRTY/RESOLVED/DATA escapes.
        let a = Node::<i32>::state(1);
        let d: Node<i32> = Node::derived(vec![a.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap())
        });
        let (log, sink) = recorder();
        let _u = d.subscribe(sink);
        log.borrow_mut().clear();

        d.core.with_inner_edges_mut(|_n, e| {
            e.value.terminal = true;
            e.value.status = Status::Completed;
            e.wave.emitted_dirty_this_wave = true;
            e.state.dirty[0] = true;
            e.state.pending = 1;
        });

        d.core
            .apply_rewire_next(RewireRequest::Set(vec![], Rc::new(|ctx| ctx.emit(999i32))));

        assert_eq!(d.status(), Status::Completed);
        assert!(
            log.borrow().is_empty(),
            "terminal-owner topology drain must not emit post-terminal messages: {:?}",
            *log.borrow()
        );
        assert!(
            d.core.borrow().deps.is_empty(),
            "queued topology still drains on the terminal owner"
        );
        assert!(
            !d.core.with_inner_edges(|_, e| e.wave.in_dep_mutation),
            "terminal-owner early return must still clear the dep-mutation guard"
        );
    }

    #[test]
    fn wave_scope_blamed_does_not_add_ref_pin() {
        // DR-8/B54 next slice: `blamed` is a generation-keyed arena reference rather
        // than another counted Core clone. The touched-set now pins live nodes in the
        // owner arena for drop/unwind safety; neither touch nor blame increments Core refs.
        let source = Node::<i32>::state(1);
        let refs = source.core.refs.clone();

        with_wave_owner(
            &source.core,
            || {
                let owner_pinned = refs.get();
                wave_register(&source.core);
                let touched_pinned = refs.get();
                assert_eq!(touched_pinned, owner_pinned);

                wave_set_blamed(&source.core);
                assert_eq!(
                    refs.get(),
                    touched_pinned,
                    "blame records a generation key, not a counted Core clone"
                );
            },
            || {},
        );

        assert_eq!(
            refs.get(),
            1,
            "all wave-owner/touched arena pins are released after the wave"
        );
    }

    #[test]
    fn generation_keyed_blame_recovers_after_callback_drop_and_fn_panic() {
        // DR-8/B54: the fn-running node is pinned by the touched set, while `blamed`
        // itself is only a generation key. If a subscriber callback drops another
        // same-arena node and the fn then panics, recovery still emits ERROR on the
        // blamed node without holding a GraphCore borrow across the callback.
        let held: Rc<RefCell<Option<Node<i32>>>> =
            Rc::new(RefCell::new(Some(Node::<i32>::producer(|_| {}))));
        let source = Node::<i32>::state_empty();
        let derived: Node<i32> = Node::derived(vec![source.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap());
            panic!("panic after subscriber callback");
        });
        let log = Rc::new(RefCell::new(Vec::<String>::new()));
        let h = held.clone();
        let l = log.clone();
        let _u = derived.subscribe(move |m| {
            l.borrow_mut().push(format!("{m:?}"));
            if matches!(m, Message::Data(_)) {
                let _ = h.borrow_mut().take();
            }
        });

        source.set(1);

        assert!(held.borrow().is_none());
        assert_eq!(derived.status(), Status::Errored);
        assert!(
            log.borrow().iter().any(|k| k == "ERROR"),
            "panic recovery should emit ERROR on the generation-keyed blamed node"
        );
    }

    #[test]
    fn stale_blamed_generation_does_not_target_reused_slot() {
        // A stale blamed key must not match a later node that reused the same arena slot.
        // This is an artificial cold-path probe for the panic recovery lookup: generation
        // is part of the key, so the old slot id cannot reset or ERROR the new occupant.
        let arena = GraphArena::new();
        let old = Node::<i32>::state_in_arena(&arena, 1);
        let old_key = old.core.key();
        let old_arena_key = old.core.arena_node_key();
        drop(old);

        let reused = Node::<i32>::state_in_arena(&arena, 2);
        assert_eq!(old_key.id, reused.core.key().id);
        assert_ne!(old_key.generation, reused.core.key().generation);

        let owner = Node::<i32>::state_in_arena(&arena, 3);
        with_wave_owner(
            &owner.core,
            || {
                wave_register(&reused.core);
                WAVE.with(|w| {
                    w.borrow_mut().as_mut().expect("wave installed").blamed = Some(old_arena_key);
                });
                panic!("stale blame");
            },
            || {},
        );

        assert_eq!(
            reused.status(),
            Status::Settled,
            "stale generation key must not ERROR/reset the reused slot occupant"
        );
        assert_eq!(
            owner.status(),
            Status::Errored,
            "stale blame falls back to the live wave owner"
        );
    }

    #[test]
    fn blame_key_is_arena_qualified_for_nested_cross_arena_panic() {
        // DR-8/B54: WAVE is thread-local, so a nested public call into another
        // GraphArena shares the outer wave-owner scope. Two independent arenas can
        // both have slot 0/generation 0; blame lookup must include arena identity or
        // the recovery ERROR can land on the wrong same-key node.
        let arena_a = GraphArena::new();
        let arena_b = GraphArena::new();
        let outer = Node::<i32>::state_empty_in_arena(&arena_a);
        let panicker = Node::<i32>::producer_opts_in_arena(&arena_b, NodeOpts::default(), |_| {
            panic!("nested cross-arena panic");
        });

        assert_eq!(outer.core.key(), panicker.core.key());
        assert_ne!(outer.core.arena_node_key(), panicker.core.arena_node_key());

        let p = panicker.clone();
        let _u = outer.subscribe(move |m| {
            if matches!(m, Message::Data(_)) {
                let _ = p.subscribe(|_| {});
            }
        });

        outer.set(1);

        assert_eq!(
            outer.status(),
            Status::Settled,
            "outer same-slot node must not receive the nested arena's recovery ERROR"
        );
        assert_eq!(
            panicker.status(),
            Status::Errored,
            "panic recovery should blame the nested arena node that ran the fn"
        );
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

    #[test]
    fn resumeall_buffers_dep_invalidate_until_resume() {
        // R-pause/R-undirty-settle timing: resumeAll buffers a node's own tier-4
        // INVALIDATE output while paused. Default pausable:true still propagates dep
        // INVALIDATE immediately (R-paused-invalidate); resumeAll replays it on RESUME.
        let src = Node::<i32>::state(1);
        let flushes = Rc::new(Cell::new(0usize));
        let d = {
            let flushes = flushes.clone();
            Node::<i32>::derived_opts(
                vec![src.erased()],
                NodeOpts {
                    pausable: Pausable::ResumeAll,
                    ..NodeOpts::default()
                },
                move |ctx| {
                    let f = flushes.clone();
                    ctx.on_invalidate(move || f.set(f.get() + 1));
                    ctx.emit(*ctx.data::<i32>(0).unwrap() + 1);
                },
            )
        };
        let (log, sink) = recorder();
        let _u = d.subscribe(sink);
        assert_eq!(d.cache(), Some(2));
        log.borrow_mut().clear();

        let lock = LockId::new("resumeall-invalidate");
        d.up(vec![Message::Pause(lock.clone())]);
        src.down(vec![Message::Invalidate]);

        assert_eq!(flushes.get(), 0, "onInvalidate waits for buffered replay");
        assert_eq!(
            d.cache(),
            Some(2),
            "cache is not cleared until RESUME replay"
        );
        assert_eq!(
            log.borrow().iter().filter(|k| *k == "INVALIDATE").count(),
            0,
            "INVALIDATE is not visible while resumeAll-paused"
        );

        d.up(vec![Message::Resume(lock)]);
        assert_eq!(flushes.get(), 1);
        assert_eq!(d.cache(), None);
        assert_eq!(
            log.borrow().iter().filter(|k| *k == "INVALIDATE").count(),
            1,
            "buffered INVALIDATE replays once on final RESUME"
        );
    }

    // ── rewire (R-rewire / D42, C-8) ──

    #[test]
    fn rewire_subscribe_dep_sentinel_dep_does_not_rearm_gate() {
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
        d.subscribe_dep(b.erased(), move |ctx| {
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
    fn rewire_unsubscribe_dep_drains_and_preserves_cache() {
        // Q3/Q7: unsubscribe_dep of a non-dirty dep preserves cache (no recompute) + drains the
        // removed edge (it no longer drives the node); the swapped fn runs on the next wave.
        let a = Node::<i32>::state(1);
        let b = Node::<i32>::state(2);
        let d: Node<i32> = Node::derived(vec![a.erased(), b.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap() + *ctx.data::<i32>(1).unwrap())
        });
        let (_log, sink) = recorder();
        let _u = d.subscribe(sink);
        assert_eq!(d.cache(), Some(3));

        d.unsubscribe_dep(b.erased(), |ctx| ctx.emit(*ctx.data::<i32>(0).unwrap()));
        assert_eq!(d.cache(), Some(3)); // preserved (unsubscribe_dep of a non-dirty dep: no recompute)

        b.set(99); // drained — must NOT drive d
        assert_eq!(d.cache(), Some(3));

        a.set(5); // d recomputes with the swapped a-only fn
        assert_eq!(d.cache(), Some(5));
    }

    #[test]
    fn rewire_unsubscribe_dep_to_zero_deps_is_inert() {
        // SD-3: unsubscribe_dep to zero deps → degenerate fn-no-deps, cache preserved, no auto-fire.
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
        d.unsubscribe_dep(a.erased(), move |ctx| {
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
    fn rewire_replace_deps_reorders_kept_deps() {
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

        d.replace_deps(vec![b.erased(), a.erased()], combine); // reorder; kept state preserved
        a.set(11); // a is now dep1; reroutes correctly
        assert_eq!(d.cache(), Some(2011)); // dep0=b=20, dep1=a=11 → 20*100+11
    }

    #[test]
    fn rewire_same_order_deps_swaps_fn_without_rebuilding_dep_state() {
        // DR-8/B54: same-ordered deps are a fn swap only. Option-C state and
        // subscriber transport stay untouched; the next dep wave uses the new fn.
        let a = Node::<i32>::state(1);
        let b = Node::<i32>::state(2);
        let d: Node<i32> = Node::derived(vec![a.erased(), b.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap() + *ctx.data::<i32>(1).unwrap())
        });
        let _u = d.subscribe(|_| {});
        assert_eq!(d.cache(), Some(3));
        assert_eq!(a.core.subscriber_count(), 1);
        assert_eq!(b.core.subscriber_count(), 1);

        d.core.with_inner_edges_mut(|_, e| {
            e.state.prev[0] = Some(Rc::new(11i32));
            e.state.prev[1] = Some(Rc::new(22i32));
        });

        d.replace_deps(vec![a.erased(), b.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap() * *ctx.data::<i32>(1).unwrap())
        });

        assert_eq!(a.core.subscriber_count(), 1);
        assert_eq!(b.core.subscriber_count(), 1);
        d.core.with_inner_edges(|_, e| {
            assert_eq!(
                e.state.prev[0]
                    .as_ref()
                    .and_then(|v| v.downcast_ref::<i32>())
                    .copied(),
                Some(11),
            );
            assert_eq!(
                e.state.prev[1]
                    .as_ref()
                    .and_then(|v| v.downcast_ref::<i32>())
                    .copied(),
                Some(22),
            );
        });

        a.set(3);
        assert_eq!(d.cache(), Some(66)); // new fn: dep0=3, dep1=22
    }

    #[test]
    fn rewire_same_order_fn_swap_rejects_reentrant_rewire_from_old_fn_drop() {
        struct ReenterOnDrop {
            target: Rc<RefCell<Option<Node<i32>>>>,
            dep: Core,
            saw_guard: Rc<Cell<bool>>,
        }

        impl Drop for ReenterOnDrop {
            fn drop(&mut self) {
                let Some(target) = self.target.borrow().as_ref().cloned() else {
                    return;
                };
                self.saw_guard
                    .set(target.core.with_inner_edges(|_, e| e.wave.in_dep_mutation));
                target.replace_deps(vec![self.dep.clone()], |_| {});
            }
        }

        let a = Node::<i32>::state(1);
        let holder: Rc<RefCell<Option<Node<i32>>>> = Rc::new(RefCell::new(None));
        let saw_guard = Rc::new(Cell::new(false));
        let drop_probe = ReenterOnDrop {
            target: holder.clone(),
            dep: a.erased(),
            saw_guard: saw_guard.clone(),
        };
        let d: Node<i32> = Node::derived(vec![a.erased()], move |ctx| {
            let _keep = &drop_probe;
            ctx.emit(*ctx.data::<i32>(0).unwrap());
        });
        *holder.borrow_mut() = Some(d.clone());
        let _u = d.subscribe(|_| {});

        let _ = catch_unwind(AssertUnwindSafe(|| {
            d.replace_deps(vec![a.erased()], |ctx| {
                ctx.emit(*ctx.data::<i32>(0).unwrap() + 1);
            });
        }));

        assert!(saw_guard.get(), "old fn Drop should see the rewire guard");
        assert!(
            !d.core.with_inner_edges(|_, e| e.wave.in_dep_mutation),
            "DepMutationGuard must clear the fast-path mutation guard after panic"
        );
        *holder.borrow_mut() = None;
    }

    #[test]
    fn rewire_precompute_keeps_first_duplicate_old_dep_slot() {
        // Public construction may still contain duplicate old deps while rewire
        // dedups the new dep list. The precomputed index map must preserve the
        // old `.position()` behavior: keep the first old slot's DepRecord/idx-box,
        // and drain the duplicate old edge.
        let a = Node::<i32>::state(1);
        let d: Node<i32> = Node::derived(vec![a.erased(), a.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap())
        });
        let _u = d.subscribe(|_| {});
        assert_eq!(
            a.core.subscriber_count(),
            2,
            "duplicate old deps install two internal edges before rewire"
        );

        d.core.with_inner_edges_mut(|_, e| {
            e.state.prev[0] = Some(Rc::new(11i32));
            e.state.prev[1] = Some(Rc::new(22i32));
            e.state.has_data[0] = true;
            e.state.has_data[1] = true;
        });

        d.replace_deps(vec![a.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap())
        });

        d.core.with_inner_edges(|n, e| {
            assert_eq!(n.deps.len(), 1);
            assert_eq!(e.state.prev.len(), 1);
            assert_eq!(
                e.state.prev[0]
                    .as_ref()
                    .and_then(|v| v.downcast_ref::<i32>())
                    .copied(),
                Some(11),
                "rewire should keep the first old duplicate dep slot"
            );
            assert_eq!(
                e.idx_boxes[0].get(),
                0,
                "kept first duplicate edge should reroute to new index 0"
            );
        });
        assert_eq!(
            a.core.subscriber_count(),
            1,
            "the duplicate old edge is drained during rewire"
        );
    }

    #[test]
    fn rewire_replace_deps_all_replaced_fast_path() {
        // DR-8/B54 no-kept fast path: same-length rewire where every old dep is dropped
        // and replaced by new deps should re-subscribe to the fresh edge set, drop the old
        // callback transport, and run once on the fresh push-on-subscribe wave.
        let a = Node::<i32>::state(1);
        let b = Node::<i32>::state(2);
        let c = Node::<i32>::state(10);
        let e = Node::<i32>::state(20);
        let runs = Rc::new(Cell::new(0usize));
        let d: Node<i32> = Node::derived(vec![a.erased(), b.erased()], {
            let runs = runs.clone();
            move |ctx| {
                runs.set(runs.get() + 1);
                ctx.emit(*ctx.data::<i32>(0).unwrap() + *ctx.data::<i32>(1).unwrap());
            }
        });
        let _u = d.subscribe(|_| {});
        assert_eq!(d.cache(), Some(3));
        runs.set(0);

        let runs2 = runs.clone();
        d.replace_deps(vec![c.erased(), e.erased()], move |ctx| {
            runs2.set(runs2.get() + 1);
            ctx.emit(*ctx.data::<i32>(0).unwrap() + *ctx.data::<i32>(1).unwrap() + 100);
        });
        assert_eq!(
            runs.get(),
            1,
            "one recompute after full no-kept replacement"
        );
        assert_eq!(d.cache(), Some(130));

        assert_eq!(
            a.core.subscriber_count(),
            0,
            "old dep A edge is fully drained"
        );
        assert_eq!(
            b.core.subscriber_count(),
            0,
            "old dep B edge is fully drained"
        );
        assert_eq!(c.core.subscriber_count(), 1, "new dep C edge is active");
        assert_eq!(e.core.subscriber_count(), 1, "new dep E edge is active");

        // Old deps no longer drive cache.
        a.set(99);
        b.set(99);
        assert_eq!(runs.get(), 1);
        assert_eq!(d.cache(), Some(130));

        // New deps continue to drive and preserve atomic settle.
        c.set(11);
        assert_eq!(runs.get(), 2);
        assert_eq!(d.cache(), Some(131));
    }

    #[test]
    fn rewire_no_kept_fast_path_preserves_remaining_unsubs_on_removed_cleanup_panic() {
        // QA regression for the DR-8/B54 no-kept fast path: a panicking removed-dep
        // deactivation hook must not drop later old-edge Unsub handles without running
        // them. The broader half-applied-rewire panic class is tracked by B16; this
        // test pins the new fast path to the slow path's cleanup ownership behavior.
        let b_deactivated = Rc::new(Cell::new(false));
        let a = Node::<i32>::producer(|ctx| {
            ctx.on_deactivation(|| panic!("removed cleanup boom"));
            ctx.emit(1i32);
        });
        let b = {
            let b_deactivated = b_deactivated.clone();
            Node::<i32>::producer(move |ctx| {
                let flag = b_deactivated.clone();
                ctx.on_deactivation(move || flag.set(true));
                ctx.emit(2i32);
            })
        };
        let c = Node::<i32>::state(10);
        let e = Node::<i32>::state(20);

        {
            let d: Node<i32> = Node::derived(vec![a.erased(), b.erased()], |ctx| {
                ctx.emit(*ctx.data::<i32>(0).unwrap() + *ctx.data::<i32>(1).unwrap());
            });
            let _u = d.subscribe(|_| {});
            assert_eq!(d.cache(), Some(3));
            assert_eq!(b.core.subscriber_count(), 1);

            d.replace_deps(vec![c.erased(), e.erased()], |ctx| {
                ctx.emit(*ctx.data::<i32>(0).unwrap() + *ctx.data::<i32>(1).unwrap());
            });
            assert_eq!(
                b.core.subscriber_count(),
                1,
                "later old edge remains owned by the rewiring node after the first cleanup panic"
            );
        }

        assert!(
            b_deactivated.get(),
            "dropping the rewiring node must still run the remaining old-edge unsubscribe"
        );
        assert_eq!(
            b.core.subscriber_count(),
            0,
            "remaining old edge is not leaked after owner drop"
        );
    }

    #[test]
    fn rewire_atomic_multi_add_settles_once() {
        // P2: adding ≥2 cached deps in ONE replace_deps settles ATOMICALLY — the fn fires once,
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
        d.replace_deps(vec![a.erased(), b.erased(), c.erased()], move |ctx| {
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

        d.subscribe_dep(b.erased(), |ctx| {
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
        d.replace_deps(vec![d.erased()], |_| {}); // self-dep → panic
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
        d.subscribe_dep(e.erased(), |ctx| ctx.emit(*ctx.data::<i32>(0).unwrap()));
        // would close d→e→d
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
        d.subscribe_dep(term.erased(), |ctx| ctx.emit(*ctx.data::<i32>(0).unwrap()));
        // terminal dep → panic
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
                dh.subscribe_dep(x.erased(), |c| c.emit(*c.data::<i32>(0).unwrap()));
            }
            ctx.emit(*ctx.data::<i32>(0).unwrap());
        });
        *slot.borrow_mut() = Some(d.clone());
        let (log, sink) = recorder();
        let _u = d.subscribe(sink); // activate → fn → mid-fn subscribe_dep → reject → wave-owner → ERROR
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

        d.replace_deps(vec![a.erased()], |ctx| {
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
        d.subscribe_dep(boom.erased(), |ctx| ctx.emit(*ctx.data::<i32>(0).unwrap()));

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
        d.replace_deps(vec![a.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap() * 2)
        });
        a.set(3);
        assert_eq!(
            d.cache(),
            Some(6),
            "a later rewire works (not 'reentrant'-rejected)"
        );
    }

    #[test]
    fn d109_default_v0_advances_only_on_data() {
        let s = Node::<i32>::state(1);
        assert_eq!(s.version(), Some(NodeVersion::V0 { counter: 0 }));

        s.down(vec![Message::Resolved]);
        assert_eq!(s.version(), Some(NodeVersion::V0 { counter: 0 }));

        s.set(2);
        assert_eq!(s.version(), Some(NodeVersion::V0 { counter: 1 }));

        s.down(vec![Message::Data(Rc::new(3)), Message::Data(Rc::new(4))]);
        assert_eq!(s.version(), Some(NodeVersion::V0 { counter: 3 }));

        s.down(vec![Message::Invalidate]);
        s.down(vec![Message::Complete]);
        assert_eq!(s.version(), Some(NodeVersion::V0 { counter: 3 }));
    }

    #[test]
    fn d112_v1_hash_receives_strict_canonical_json_bytes() {
        let calls = Rc::new(RefCell::new(Vec::<String>::new()));
        let calls_for_hash = calls.clone();
        let hash = Rc::new(move |bytes: &[u8]| {
            let text = std::str::from_utf8(bytes)
                .expect("strict canonical JSON bytes are UTF-8")
                .to_owned();
            calls_for_hash.borrow_mut().push(text.clone());
            format!("h:{text}")
        });
        let node = Node::<serde_json::Value>::derived_opts(
            vec![],
            NodeOpts {
                versioning: Some(NodeVersioningPolicy::Level1 { hash: Some(hash) }),
                ..NodeOpts::default()
            },
            |ctx| ctx.emit(json!({ "b": 2, "a": 1 })),
        );
        let _u = node.subscribe(|_| {});

        assert_eq!(
            calls.borrow().as_slice(),
            &[
                "{\"@graphrefly/node-version\":\"v1-absent\"}".to_owned(),
                "{\"a\":1,\"b\":2}".to_owned(),
            ]
        );
        assert_eq!(
            node.version(),
            Some(NodeVersion::V1 {
                counter: 1,
                cid: "h:{\"a\":1,\"b\":2}".to_owned(),
                prev: Some("h:{\"@graphrefly/node-version\":\"v1-absent\"}".to_owned()),
            })
        );
    }

    #[test]
    fn d112_v1_rejects_invalid_data_before_cache_or_version_mutation() {
        let node = Node::<serde_json::Value>::derived_opts(
            vec![],
            NodeOpts {
                versioning: Some(NodeVersioningPolicy::Level1 { hash: None }),
                ..NodeOpts::default()
            },
            |_ctx| {},
        );
        let before = node.version();
        node.down(vec![Message::Data(Rc::new(f64::NAN))]);

        assert_eq!(node.version(), before);
        assert_eq!(node.cache(), None);
        assert_eq!(node.status(), Status::Errored);
    }

    #[test]
    fn d112_v1_hash_runs_outside_graphcore_borrow() {
        let graph = crate::graph::graph();
        let probe = graph.state_opts(1i32, crate::graph::GraphNodeOpts::named("probe"));
        let probe_core = probe.erased();
        let calls = Rc::new(Cell::new(0usize));
        let calls_for_hash = calls.clone();
        let hash = Rc::new(move |bytes: &[u8]| {
            let _ = probe_core.version();
            calls_for_hash.set(calls_for_hash.get() + 1);
            format!(
                "h:{}",
                std::str::from_utf8(bytes).expect("strict canonical JSON bytes are UTF-8")
            )
        });
        let node = graph.state_opts(
            json!(1),
            crate::graph::GraphNodeOpts {
                name: Some("versioned".to_owned()),
                node: NodeOpts {
                    versioning: Some(NodeVersioningPolicy::Level1 { hash: Some(hash) }),
                    ..NodeOpts::default()
                },
                ..crate::graph::GraphNodeOpts::default()
            },
        );

        node.set(json!(2));

        assert_eq!(calls.get(), 2);
        assert_eq!(
            node.version(),
            Some(NodeVersion::V1 {
                counter: 1,
                cid: "h:2".to_owned(),
                prev: Some("h:1".to_owned()),
            })
        );
    }
}
