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
//! ## Current substrate scope
//!
//! The active substrate covers state / producer / derived nodes, two-phase
//! DIRTY-to-DATA delivery, diamond joins, first-run gating, push-on-subscribe,
//! lazy activation, ROM/RAM, INVALIDATE, PAUSE/RESUME locksets and pause modes,
//! COMPLETE/ERROR/TEARDOWN terminal behavior, batch commit/rollback, LocalAsync
//! boundaries, runtime rewire, pull/routed-up demand, `ctx.rewire_next`, and
//! panic-to-ERROR recovery at the wave boundary. Every occurrence remains DATA;
//! no-emit functions settle with substrate-synthesized undirty RESOLVED (D49).

use std::cell::{Cell, Ref, RefCell};
use std::collections::{HashSet, VecDeque};
use std::marker::PhantomData;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::{Rc, Weak};

use crate::batch::{
    active_batch_committed_token, boundary_drains_blocked, collecting_batch,
    committed_after_batch_for_target, defer_to_batch, register_boundary_root,
};
use crate::checkpoint::{
    unregister_backend_state_contributor, unregister_backend_state_contributor_key,
};
use crate::ctx::{Ctx, DepRecord, DepTerminal};
use crate::dispatcher::{default_dispatcher, Dispatcher, NodeFn, PoolKind};
use crate::environment::EnvironmentDrivers;
use crate::protocol::{reject_kernel_only_output, AnyValue, GraphError, Handle, Message, Wave};
use crate::versioning::{
    advance_node_version, assert_node_version_data_compatible, create_node_version,
    resolve_node_versioning_policy, NodeVersion, NodeVersioningPolicy,
    ResolvedNodeVersioningPolicy, RestoredNodeVersion,
};

/// A node's cache status (R-node): no retained value, a pending mark it received or
/// sent in the current wave is unsettled, or ready.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CacheStatus {
    /// No retained value.
    Empty,
    /// A pending mark of the current wave is unsettled.
    Stale,
    /// A retained value and no unsettled pending mark.
    Ready,
}

/// A node's output status (R-node).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OutputStatus {
    /// The node has not ended.
    Open,
    /// The node ended with COMPLETE.
    Completed,
    /// The node ended with ERROR.
    Errored,
}

/// A node's lifecycle status (R-node, R-lifetime).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Lifecycle {
    /// No open lifetime.
    Inactive,
    /// A lifetime is open.
    Active,
    /// The lifetime's cleanup is running.
    Closing,
}

/// A node's status, derived from what it holds and never stored (R-node).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Status {
    /// Empty, stale or ready.
    pub cache: CacheStatus,
    /// Open, completed or errored.
    pub output: OutputStatus,
    /// Inactive, active or closing.
    pub lifecycle: Lifecycle,
}

impl Status {
    /// The cache holds a value and no pending mark is unsettled.
    pub fn is_fresh(self) -> bool {
        self.cache == CacheStatus::Ready
    }
    /// The node has ended (COMPLETE or ERROR).
    pub fn is_terminal(self) -> bool {
        self.output != OutputStatus::Open
    }
    /// A short label: the output if ended, otherwise the cache (`empty`, `stale`,
    /// `ready`, `completed`, `errored`).
    pub fn label(self) -> &'static str {
        match (self.output, self.cache) {
            (OutputStatus::Completed, _) => "completed",
            (OutputStatus::Errored, _) => "errored",
            (OutputStatus::Open, CacheStatus::Empty) => "empty",
            (OutputStatus::Open, CacheStatus::Stale) => "stale",
            (OutputStatus::Open, CacheStatus::Ready) => "ready",
        }
    }
}

/// An auto or manual policy for COMPLETE, ERROR or INVALIDATE, per node (R-terminals,
/// R-invalidation). Both default to auto.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Policy {
    /// The kernel handles the message without running the function.
    #[default]
    Auto,
    /// The message is the function's input.
    Manual,
}

/// Node construction options (substrate-level). Sugar/operator opts are the graph layer
/// (D6/D24); this is the minimal substrate surface needed to select a pool (D20) and
/// the dep-terminal propagation policy (R-deps-terminal).
#[derive(Debug, Clone)]
pub struct NodeOpts {
    /// Real factory name for graph-less nodes that `describe` auto-discovers (D43/D51).
    /// Registered graph entries still carry their own graph-layer factory string.
    pub factory: Option<String>,
    /// The dispatch pool (default LocalSync, R-sync-core).
    pub pool: PoolKind,
    /// First-run gate override: allow fn execution with SENTINEL deps. Default
    /// false. The first-run gate is Rust sugar, not protocol (graphrefly:D797 (9)):
    /// the kernel runs a function on whatever its dependencies have delivered.
    pub partial: bool,
    /// A quiet node consumes its input waves without being marked pending or passing
    /// a mark on, and publishes its function's output as a later wave of its own
    /// (R-wave). Default false.
    pub quiet: bool,
    /// The COMPLETE policy (R-terminals). Auto: once every dependency has ended, the
    /// node completes after the run that sees the last terminal; a node with no
    /// dependencies never completes by itself. Manual: a dependency's COMPLETE is the
    /// function's input, and the node ends only when its function sends a terminal.
    pub complete: Policy,
    /// The ERROR policy (R-terminals). Auto: a dependency's ERROR ends the node with
    /// that ERROR without running its function. Manual: the ERROR is the function's
    /// input.
    pub error: Policy,
    /// The INVALIDATE policy (R-invalidation). Auto: an input's INVALIDATE invalidates
    /// the node without running its function. Manual: it is the function's input
    /// ([`Ctx::invalidated`]), and the node keeps its values until the function clears
    /// them or sends INVALIDATE itself.
    pub invalidate: Policy,
    /// D109 node runtime versioning policy. `None` means the package default V0; graph-owned
    /// nodes may inherit a graph default before construction.
    pub versioning: Option<NodeVersioningPolicy>,
    /// The instance's setup (R-lifetime@2): runs once per lifetime, before the
    /// function's first run in it, and may return the lifetime's cleanup.
    pub setup: Option<crate::lifetime::Setup>,
}

impl NodeOpts {
    /// Returns these options with `setup` as the node's setup (R-lifetime@2).
    pub fn with_setup(
        mut self,
        setup: impl Fn(&Ctx) -> Option<crate::lifetime::Cleanup> + 'static,
    ) -> Self {
        self.setup = Some(crate::lifetime::Setup::new(setup));
        self
    }
}

/// Defaults: COMPLETE/ERROR auto-cascade ON (combineLatest-style), terminal-as-input OFF
/// — the plain derived/effect behavior. (A manual impl, not `#[derive(Default)]`, so the
/// two cascade flags default to `true`, not `false`.)
impl Default for NodeOpts {
    fn default() -> Self {
        Self {
            factory: None,
            pool: PoolKind::default(),
            partial: false,
            quiet: false,
            complete: Policy::Auto,
            error: Policy::Auto,
            invalidate: Policy::Auto,
            versioning: None,
            setup: None,
        }
    }
}

type Msg = Message<AnyValue>;
type Sink = Rc<dyn Fn(&[Msg])>;
type Unsub = Box<dyn FnOnce()>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubscriberKind {
    External,
    GraphObserver,
    /// A dependent node's input edge. Only these receive the kernel's lone `[PENDING]`
    /// mark (R-wave).
    Dependent,
}

struct SubscriberEntry {
    id: u64,
    kind: SubscriberKind,
    sink: Sink,
}

enum SubscriberSnapshot {
    Empty,
    One(Sink),
    Many(Vec<Sink>),
}

fn snapshot_subscribers(a: &NodeAux) -> SubscriberSnapshot {
    match a.subscribers.as_slice() {
        [] => SubscriberSnapshot::Empty,
        [entry] => SubscriberSnapshot::One(entry.sink.clone()),
        many => SubscriberSnapshot::Many(many.iter().map(|entry| entry.sink.clone()).collect()),
    }
}

fn snapshot_dependents(a: &NodeAux) -> SubscriberSnapshot {
    let mut deps = a
        .subscribers
        .iter()
        .filter(|entry| entry.kind == SubscriberKind::Dependent)
        .map(|entry| entry.sink.clone());
    match (deps.next(), deps.next()) {
        (None, _) => SubscriberSnapshot::Empty,
        (Some(one), None) => SubscriberSnapshot::One(one),
        (Some(a), Some(b)) => {
            let mut many = vec![a, b];
            many.extend(deps);
            SubscriberSnapshot::Many(many)
        }
    }
}

/// Delivers arrays to a subscriber snapshot: every array to one subscriber, then the
/// next (R-wave).
fn deliver_subscriber_snapshot(subs: SubscriberSnapshot, frames: &[Wave<AnyValue>]) {
    match subs {
        SubscriberSnapshot::Empty => {}
        SubscriberSnapshot::One(s) => {
            for f in frames {
                s(f);
            }
        }
        SubscriberSnapshot::Many(subs) => {
            for s in subs {
                for f in frames {
                    s(f);
                }
            }
        }
    }
}

/// The first array a subscriber gets: START, the node's retained values, then its
/// terminal if it has ended (R-subscription).
fn snapshot_array(value: &NodeValueState) -> Wave<AnyValue> {
    let mut out = Vec::with_capacity(value.values.len() + 2);
    out.push(Message::Start);
    out.extend(value.values.iter().cloned().map(Message::Data));
    if let Some(ended) = &value.ended {
        out.push(terminal_message(ended));
    }
    out
}

fn terminal_message(ended: &DepTerminal) -> Msg {
    match ended {
        DepTerminal::Complete => Message::Complete,
        DepTerminal::Error(err) => Message::Error(err.to_string().into()),
    }
}

fn starts_with_invalidate(frame: &[InMsg]) -> bool {
    matches!(frame.first(), Some(InMsg::Invalidate))
}

/// Checks one submitted array and splits it into the arrays that are published
/// (R-wave). A terminal must be the last message; an INVALIDATE removes what came
/// before it and goes out as its own array, so `[DATA 1, INVALIDATE, DATA 2]` is
/// published as `[INVALIDATE]` then `[DATA 2]`. Rejections panic before anything is
/// applied (R-failure).
pub(crate) fn prepare_output(msgs: Wave<AnyValue>) -> Vec<Wave<AnyValue>> {
    check_output_shape(&msgs);
    let cut = msgs.iter().rposition(|m| matches!(m, Message::Invalidate));
    let mut frames = Vec::with_capacity(2);
    match cut {
        Some(at) => {
            let rest: Wave<AnyValue> = msgs.into_iter().skip(at + 1).collect();
            frames.push(vec![Message::Invalidate]);
            if !rest.is_empty() {
                frames.push(rest);
            }
        }
        None if !msgs.is_empty() => frames.push(msgs),
        None => {}
    }
    frames
}

/// Panics when an array breaks the written-order rules: a terminal must be last, and
/// START is not output (R-wave, R-messages).
fn check_output_shape(msgs: &[Msg]) {
    if let Some(end) = msgs.iter().position(Message::is_terminal) {
        assert!(
            end + 1 == msgs.len(),
            "a terminal must be the last message of its array (R-wave)"
        );
    }
    assert!(
        !msgs.iter().any(|m| matches!(m, Message::Start)),
        "START is the subscription handshake, not node output (R-messages)"
    );
}

/// What a node's settlement does (R-wave, R-terminals, R-invalidation).
enum SettlePlan {
    /// A dependency's ERROR ends the node under the auto policy.
    End {
        invalidated_first: bool,
        error: Rc<str>,
    },
    Run {
        invalidated: bool,
        runs: bool,
        auto_complete: bool,
        quiet: bool,
        passthrough: bool,
    },
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

    fn validate_deps_same_graph(&self, owner: &Core) {
        match self {
            RewireRequest::Set(deps, _) => {
                for dep in deps {
                    assert!(
                        dep.same_graph(owner),
                        "rewire: dep belongs to a different graph; cross-graph deps require a wire bridge (D22/R-graph-domain)"
                    );
                }
            }
            RewireRequest::Add(dep, _) => {
                assert!(
                    dep.same_graph(owner),
                    "rewire: dep belongs to a different graph; cross-graph deps require a wire bridge (D22/R-graph-domain)"
                );
            }
            RewireRequest::Remove(_, _) => {}
        }
    }

    fn project_deps(&self, current: Vec<Core>) -> Vec<Core> {
        match self {
            RewireRequest::Set(deps, _) => deps.clone(),
            RewireRequest::Add(dep, _) => {
                let mut next = current;
                if !next.iter().any(|d| d.ptr_eq(dep)) {
                    next.push(dep.clone());
                }
                next
            }
            RewireRequest::Remove(dep, _) => {
                current.into_iter().filter(|d| !dep.matches(d)).collect()
            }
        }
    }

    fn into_deps_and_fn(self, current: Vec<Core>) -> (Vec<Core>, NodeFn) {
        match self {
            RewireRequest::Set(deps, f) => (deps, f),
            RewireRequest::Add(dep, f) => {
                let mut next = current;
                if !next.iter().any(|d| d.ptr_eq(&dep)) {
                    next.push(dep);
                }
                (next, f)
            }
            RewireRequest::Remove(dep, f) => {
                let next = current.into_iter().filter(|d| !dep.matches(d)).collect();
                (next, f)
            }
        }
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

#[derive(Clone)]
pub(crate) struct CoreIdentity {
    arena_key: ArenaNodeKey,
}

impl CoreIdentity {
    pub(crate) fn from_core(core: &Core) -> Self {
        Self {
            arena_key: core.arena_node_key(),
        }
    }

    pub(crate) fn matches(&self, core: &Core) -> bool {
        self.arena_key == core.arena_node_key()
    }
}

/// B49 / D71 transition slice: graph-local arena for node bookkeeping. The
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

    #[cfg(test)]
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

/// One message as a dependent records it: PENDING and START stripped, the error kept
/// as its message (a dependency's error crosses a subscription as a borrow, D31).
#[derive(Clone)]
enum InMsg {
    Data(AnyValue),
    Resolved,
    Invalidate,
    Complete,
    Error(Rc<str>),
}

impl InMsg {
    fn from_message(msg: &Msg) -> Option<Self> {
        match msg {
            Message::Start | Message::Pending => None,
            Message::Data(v) => Some(Self::Data(v.clone())),
            Message::Resolved => Some(Self::Resolved),
            Message::Invalidate => Some(Self::Invalidate),
            Message::Complete => Some(Self::Complete),
            Message::Error(e) => Some(Self::Error(format!("{e}").into())),
        }
    }

    fn is_value(&self) -> bool {
        matches!(self, Self::Data(_) | Self::Resolved)
    }

    fn terminal(&self) -> Option<DepTerminal> {
        match self {
            Self::Complete => Some(DepTerminal::Complete),
            Self::Error(e) => Some(DepTerminal::Error(e.clone())),
            _ => None,
        }
    }

    /// DATA, RESOLVED, INVALIDATE, COMPLETE and ERROR settle a pending mark (R-wave).
    fn settles(&self) -> bool {
        true
    }
}

/// What a node holds about one dependency (R-invocation): the latest value received,
/// its terminal, and the arrays it delivered in the current wave.
#[derive(Clone, Default)]
struct DepInput {
    has: bool,
    value: Option<AnyValue>,
    /// The latest value as it was before this wave's arrays (the `Values::prev` sugar).
    value_before_wave: Option<AnyValue>,
    ended: Option<DepTerminal>,
    /// This wave's arrays from the dependency, each kept whole (R-wave).
    frames: Vec<Vec<InMsg>>,
    /// Marked pending in this wave and not settled yet.
    pending: bool,
    /// An INVALIDATE arrived on this input in this wave (shown to the function under
    /// the manual INVALIDATE policy, R-invalidation).
    invalidated: bool,
}

impl DepInput {
    /// Records one received array. Returns whether it settles this input's mark.
    fn record(&mut self, frame: Vec<InMsg>) -> bool {
        if self.frames.is_empty() {
            self.value_before_wave = self.value.clone();
        }
        for m in &frame {
            match m {
                InMsg::Data(v) => {
                    self.has = true;
                    self.value = Some(v.clone());
                }
                InMsg::Invalidate => {
                    self.has = false;
                    self.value = None;
                }
                InMsg::Complete | InMsg::Error(_) => self.ended = m.terminal(),
                InMsg::Resolved => {}
            }
        }
        let settles = frame.iter().any(InMsg::settles);
        if !frame.is_empty() {
            self.frames.push(frame);
        }
        settles
    }
}

/// Per-dependency wave bookkeeping for one node slot.
struct DepState {
    inputs: Vec<DepInput>,
    /// How many inputs are marked pending and not settled in this wave.
    pending: usize,
}

impl DepState {
    fn new(dep_count: usize) -> Self {
        Self {
            inputs: vec![DepInput::default(); dep_count],
            pending: 0,
        }
    }

    /// The first-run gate (Rust sugar, graphrefly:D797 (9)): every dependency has a
    /// value, or has ended when its terminal is the function's input.
    fn all_settled(&self, terminal_is_input: bool) -> bool {
        self.inputs
            .iter()
            .all(|input| input.has || (terminal_is_input && input.ended.is_some()))
    }

    fn has_frames(&self) -> bool {
        self.inputs.iter().any(|input| !input.frames.is_empty())
    }

    fn clear_frames(&mut self) {
        for input in &mut self.inputs {
            input.frames.clear();
            input.pending = false;
            input.invalidated = false;
        }
        self.pending = 0;
    }
}

/// Per-node transient wave/mutation flags.
struct NodeWaveState {
    /// Marked pending in the current wave and not yet published (R-wave).
    pending: bool,
    inside_run_wave: bool,
    /// Arrays the running function sent through `ctx.down`, published after it returns.
    run_out: Vec<Wave<AnyValue>>,
    /// The last array of `run_out` was opened by `ctx.emit` and takes further emits.
    emit_open: bool,
    /// Acquiring dependencies (activation or rewire): their snapshots are recorded as
    /// input and the node settles once afterwards.
    in_dep_mutation: bool,
}

impl NodeWaveState {
    fn new() -> Self {
        Self {
            pending: false,
            inside_run_wave: false,
            run_out: Vec::new(),
            emit_open: false,
            in_dep_mutation: false,
        }
    }
}

struct NodeValueState {
    cache: Option<AnyValue>,
    /// The DATA values of the last array that carried any (R-subscription).
    values: Vec<AnyValue>,
    has_data: bool,
    terminal: bool,
    /// The terminal this node ended with, carried by its snapshot (R-subscription).
    ended: Option<DepTerminal>,
}

impl NodeValueState {
    fn new() -> Self {
        Self {
            cache: None,
            values: Vec::new(),
            has_data: false,
            terminal: false,
            ended: None,
        }
    }

    fn clear_values(&mut self) {
        self.cache = None;
        self.values.clear();
        self.has_data = false;
    }
}

/// Per-dependency edge bookkeeping (subscription ownership + callback index indirection).
/// Kept in GraphCore side-tables as part of the B49 arena transition.
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
    quiet: bool,
    complete: Policy,
    error: Policy,
    invalidate: Policy,
    versioning: Option<NodeVersioningPolicy>,
    setup: Option<crate::lifetime::Setup>,
}

impl NodeConfigSlot {
    fn from_opts(opts: &NodeOpts) -> Self {
        Self {
            partial: opts.partial,
            quiet: opts.quiet,
            complete: opts.complete,
            error: opts.error,
            invalidate: opts.invalidate,
            versioning: opts.versioning.clone(),
            setup: opts.setup.clone(),
        }
    }

    /// Whether a dependency's terminal is the function's input under some policy.
    fn terminal_is_input(&self) -> bool {
        self.complete == Policy::Manual || self.error == Policy::Manual
    }

    fn all_deps_settled(&self, dep: &DepState) -> bool {
        dep.all_settled(self.terminal_is_input())
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
    subscribers: Vec<SubscriberEntry>,
    next_sub_id: u64,
    activated: bool,
    /// The current lifetime's private state (R-lifetime@2).
    state: Option<AnyValue>,
    /// The open lifetime, if any.
    lifetime: Option<crate::lifetime::Lifetime>,
    /// The closing lifetime's cleanup is running.
    closing: bool,
}

impl NodeAux {
    fn new() -> Self {
        Self {
            subscribers: Vec::new(),
            next_sub_id: 0,
            activated: false,
            state: None,
            lifetime: None,
            closing: false,
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
}

pub(crate) struct NodeRestoreRuntime {
    pub cache: Option<AnyValue>,
    pub has_data: bool,
    pub version: RestoredNodeVersion,
    pub terminal: bool,
    pub activated: bool,
    pub has_called_fn_once: bool,
    pub ctx_state: Option<AnyValue>,
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
/// removes those entries, and running the lifetime's cleanup releases external resources
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
        // The lifetime closes in R-lifetime@2 order: abort, release, cleanup, state.
        let lifetime = aux.lifetime.take();
        if let Some(lt) = &lifetime {
            lt.signal.abort();
        }
        for u in edges.unsubs.drain(..).flatten() {
            run_reported("release", u);
        }
        if let Some(cleanup) = lifetime.and_then(|lt| lt.cleanup) {
            let cctx = crate::lifetime::CleanupCtx {
                state: aux.state.clone(),
            };
            run_reported("cleanup", move || cleanup(&cctx));
        }
        aux.state = None;
    }
}

/// Runs `f`; a panic goes to the host reporter as `source` (R-failure@3).
fn run_reported(source: &str, f: impl FnOnce()) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(f)) {
        crate::report::report(source, payload.as_ref());
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

    #[cfg(test)]
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
    unregister_backend_state_contributor_key((
        Rc::as_ptr(graph_ref) as usize,
        key.id.0,
        key.generation,
    ));
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

    fn borrowed_core(&self) -> Option<Core> {
        self.pin.borrowed_core()
    }
}

/// Commits a batch as one wave (R-request-order): every staged node applies its one
/// accumulated array, every source's cone is marked pending, and only then is anything
/// delivered, so a fan-in over several staged sources runs once.
pub(crate) fn commit_batched_waves(entries: Vec<(&BatchTarget, Wave<AnyValue>)>) {
    let staged: Vec<(Core, Wave<AnyValue>)> = entries
        .into_iter()
        .filter_map(|(target, wave)| target.borrowed_core().map(|core| (core, wave)))
        .collect();
    // A borrowed view: the batch target pins keep the slots live even when every
    // public handle has been dropped (DR-8/B54).
    let Some(owner) = staged.first().map(|(core, _)| core.borrowed_view()) else {
        return;
    };
    with_wave_owner(
        &owner,
        || {
            let mut applied = Vec::with_capacity(staged.len());
            for (core, wave) in staged {
                if core.with_inner_edges(|_, e| e.value.terminal) {
                    continue;
                }
                wave_register(&core);
                let out = core.apply_out(prepare_output(wave));
                applied.push((core, out));
            }
            for (core, out) in &applied {
                if out.iter().flatten().any(Message::is_settling) {
                    core.mark_as_source();
                }
            }
            for (core, out) in applied {
                core.deliver_out(out);
            }
        },
        || {},
    );
}

#[derive(Clone)]
struct CoreWeak {
    graph: Weak<RefCell<GraphCore>>,
    key: NodeKey,
    refs: Weak<Cell<usize>>,
}

impl CoreWeak {
    #[cfg(test)]
    #[allow(dead_code)] // kept for pin/refcount probes
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

    fn receive_from_dep(&self, idx: usize, msgs: &[Msg]) {
        // DR-8/B54 receive policy: during a wave, pin through the wave's touched set and
        // run through a borrowed arena view; outside a wave, pin first.
        if wave_in_flight() {
            let Some(graph) = self.graph.upgrade() else {
                return;
            };
            let Some(refs) = self.refs.upgrade() else {
                return;
            };
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
                return;
            }
            let can_apply = {
                let g = graph.borrow();
                g.is_live_key(self.key) && (refs.get() != 0 || g.pin_count(self.key) != 0)
            };
            if !can_apply {
                return;
            }
            let core = Core::from_borrowed_parts(graph, self.key.id, self.key.generation, refs);
            core.receive_from_dep_registered(idx, msgs);
            return;
        }
        if let Some((_pin, core)) = self.pinned_borrowed_core() {
            core.receive_from_dep_registered(idx, msgs);
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

/// A node whose settlement waits until the current delivery has reached every
/// subscriber, so it settles once on everything its dependencies sent.
struct DeferredDeliveryAction {
    target: DeferredNodeAction,
}

impl DeferredDeliveryAction {
    fn from_core(core: &Core) -> Self {
        Self {
            target: DeferredNodeAction::from_core(core),
        }
    }

    fn matches(&self, core: &Core) -> bool {
        self.target.matches(core)
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
        node.settle();
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
        req: RewireRequest,
        committed: Rc<Cell<bool>>,
    },
    Down {
        target: CoreToken,
        frames: Vec<Wave<AnyValue>>,
        committed: Rc<Cell<bool>>,
    },
}

impl BoundaryTask {
    fn committed(&self) -> bool {
        match self {
            BoundaryTask::Rewire { committed, .. }
            | BoundaryTask::ExternalRewire { committed, .. }
            | BoundaryTask::Down { committed, .. } => committed.get(),
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
/// dep's activation); the wave-owner catch's wave-flag reset does NOT
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

fn defer_settle_until_delivery_boundary(core: &Core) {
    DEFERRED_DELIVERY_ACTIONS.with(|actions| {
        let mut actions = actions.borrow_mut();
        if !actions.iter().any(|action| action.matches(core)) {
            actions.push(DeferredDeliveryAction::from_core(core));
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
        for action in &actions {
            action.apply();
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
            // the aborted wave corrupted (B25).
            for n in &scope.touched {
                n.reset_wave_flags();
            }
            // Delivery-boundary run/settle queues are wave-local. If the delivery
            // aborted before its guard could drain them, carrying those actions into a
            // later unrelated wave would resurrect stale dep projections after B25 has
            // already reset the touched nodes.
            clear_deferred_delivery_actions();
            // A failure surfaces as ERROR on a node ON the failure path (D30) — a
            // fresh terminal wave.
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
        let result = catch_unwind(AssertUnwindSafe(|| drain_committed_boundary(root)));
        if let Err(payload) = result {
            remember_first_boundary_panic(&mut escaped, Err(payload));
        }
    }
    for root in task_roots {
        let result = catch_unwind(AssertUnwindSafe(|| drain_committed_boundary_root(root)));
        if let Err(payload) = result {
            remember_first_boundary_panic(&mut escaped, Err(payload));
        }
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
                node.core.apply_rewire_next(req);
            }
        }
        BoundaryTask::ExternalRewire {
            target,
            req,
            committed,
        } => {
            if committed.get() {
                if let Some(node) = target.pinned_borrowed_core(graph) {
                    node.core.apply_external_rewire(req);
                }
            }
        }
        BoundaryTask::Down {
            target,
            frames,
            committed,
        } => {
            if committed.get() {
                if let Some(node) = target.pinned_borrowed_core(graph) {
                    node.core.owned_publish(frames);
                }
            }
        }
    }
    None
}

impl Core {
    /// True when both handles name the same node (same graph arena, id and slot
    /// generation).
    #[must_use]
    pub fn ptr_eq(&self, other: &Core) -> bool {
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

    /// Derived from what the node holds (R-node); never stored.
    pub(crate) fn status(&self) -> Status {
        let (cache, output) = self.with_inner_edges(|_n, e| {
            let cache = if !e.value.has_data {
                CacheStatus::Empty
            } else if e.wave.pending || e.state.pending > 0 {
                CacheStatus::Stale
            } else {
                CacheStatus::Ready
            };
            let output = match &e.value.ended {
                None => OutputStatus::Open,
                Some(DepTerminal::Complete) => OutputStatus::Completed,
                Some(DepTerminal::Error(_)) => OutputStatus::Errored,
            };
            (cache, output)
        });
        let lifecycle = self.with_aux(|a| {
            if a.closing {
                Lifecycle::Closing
            } else if a.activated {
                Lifecycle::Active
            } else {
                Lifecycle::Inactive
            }
        });
        Status {
            cache,
            output,
            lifecycle,
        }
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
        let e = g
            .edge_slots
            .get(key.id.0)
            .and_then(Option::as_ref)
            .expect("Core points at a live GraphCore edge slot");
        e.state.pending == 0
            && !e.wave.inside_run_wave
            && !e.wave.in_dep_mutation
            && !e.wave.pending
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
        unregister_backend_state_contributor(self);
        let (mut inner, _topology, mut call, _config, _run, mut edges, _version, mut aux) = slot;
        inner.cleanup_before_free(&mut call, &mut edges, &mut aux);
        free_slot.armed = false;
        self.graph.borrow_mut().free.push(key.id.0);
        true
    }

    pub(crate) fn checkpoint_runtime(&self) -> NodeCheckpointRuntime {
        let version = self.version();
        let status = self.status();
        self.with_node_state_aux_mut(|_n, _c, _cfg, r, e, a| NodeCheckpointRuntime {
            cache: e.value.cache.clone(),
            has_data: e.value.has_data,
            version,
            status,
            terminal: e.value.terminal,
            activated: a.activated,
            has_called_fn_once: r.has_called_fn_once,
            ctx_state: a.state.clone(),
        })
    }

    pub(crate) fn restore_runtime(&self, state: NodeRestoreRuntime) {
        let should_activate = state.activated;
        let deps = self.with_node_state_aux_mut(|n, _c, _cfg, r, e, a| {
            e.value.cache = state.cache;
            e.value.has_data = state.has_data;
            e.value.terminal = state.terminal;
            e.value.values = e.value.cache.iter().cloned().collect();
            e.wave = NodeWaveState::new();
            e.state = DepState::new(e.unsubs.len());
            // Restored nodes take their dependencies' snapshots without running.
            e.wave.in_dep_mutation = true;
            r.has_called_fn_once = state.has_called_fn_once;
            a.activated = should_activate;
            a.state = state.ctx_state;
            a.lifetime = should_activate.then(crate::lifetime::Lifetime::open);
            n.deps.clone()
        });
        if should_activate {
            for (idx, dep) in deps.iter().enumerate() {
                self.subscribe_dep(idx, dep);
            }
        }
        self.with_inner_edges_mut(|_, e| {
            e.state.clear_frames();
            e.wave.in_dep_mutation = false;
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
                e.value.values = vec![v.clone()];
                e.value.cache = Some(v);
                e.value.has_data = true;
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

    // ── subscription (R-subscription) ──

    /// Register a sink and give it one first array: START, then the node's snapshot (the
    /// DATA values of the last array that carried DATA, then its terminal if it has
    /// ended). Subscribing activates an inactive node first (R-lifetime), so the snapshot
    /// carries what its first run produced.
    fn subscribe_recording_id_with_kind(
        &self,
        sink: Sink,
        kind: SubscriberKind,
        id_out: &Cell<Option<u64>>,
    ) -> Unsub {
        if !self.with_aux(|a| a.activated) {
            self.activate();
        }
        let (id, snapshot) = self.with_inner_edges_aux_mut(|_n, e, a| {
            let id = a.next_sub_id;
            a.next_sub_id += 1;
            a.subscribers.push(SubscriberEntry {
                id,
                kind,
                sink: sink.clone(),
            });
            (id, snapshot_array(&e.value))
        });
        id_out.set(Some(id));
        sink(&snapshot);
        let core = self.clone();
        Box::new(move || core.unsubscribe(id))
    }

    fn unsubscribe(&self, id: u64) {
        if !self.graph.borrow().is_live_key(self.key()) {
            return;
        }
        let became_empty = self.with_aux_mut(|a| {
            let before = a.subscribers.len();
            a.subscribers.retain(|entry| entry.id != id);
            a.subscribers.len() < before && a.subscribers.is_empty()
        });
        if became_empty {
            self.deactivate();
        }
    }

    pub(crate) fn external_subscriber_count_for_release(&self) -> usize {
        self.with_aux(|a| {
            a.subscribers
                .iter()
                .filter(|entry| entry.kind != SubscriberKind::GraphObserver)
                .count()
        })
    }

    pub(crate) fn detach_graph_observer_subscribers_for_release(&self) -> usize {
        if !self.graph.borrow().is_live_key(self.key()) {
            return 0;
        }
        self.with_aux_mut(|a| {
            let before = a.subscribers.len();
            a.subscribers
                .retain(|entry| entry.kind != SubscriberKind::GraphObserver);
            before - a.subscribers.len()
        })
    }

    // ── activation / deactivation (lazy; R-lifetime) ──

    /// Activating a computed node acquires its dependencies, activating them, then
    /// runs its function once on their snapshots (R-lifetime). Nothing subscribes to
    /// the node yet, so that first output only sets what the new subscriber's START
    /// snapshot carries.
    fn activate(&self) {
        if self.with_inner_edges(|_, e| e.value.terminal) {
            // An ended node is connected and gives its snapshot, but never runs again
            // and holds nothing upstream (R-terminals).
            self.with_aux_mut(|a| a.activated = true);
            return;
        }
        let deps = self.with_node_state_aux_mut(|n, _c, _cfg, _r, e, a| {
            a.activated = true;
            a.lifetime = Some(crate::lifetime::Lifetime::open());
            e.unsubs = (0..n.deps.len()).map(|_| None).collect();
            // placeholder boxes (distinct Rcs); subscribe_dep overwrites each with the
            // real box its callback captures.
            e.idx_boxes = (0..n.deps.len())
                .map(|_| Rc::new(Cell::new(-1i64)))
                .collect();
            e.state = DepState::new(n.deps.len());
            e.wave.in_dep_mutation = true;
            n.deps.clone()
        });
        {
            let _mut_guard = DepMutationGuard(DeferredNodeAction::from_core(self));
            for (idx, dep) in deps.iter().enumerate() {
                self.subscribe_dep(idx, dep);
            }
        }
        self.first_run();
    }

    fn subscribe_dep(&self, idx: usize, dep: &Core) {
        let weak = self.downgrade();
        // R-rewire Option-C: the callback reads the dep's CURRENT index from a shared box
        // so a surgical reorder reroutes in O(1); -1 means the dep was removed (drain).
        let idx_box: Rc<Cell<i64>> = Rc::new(Cell::new(idx as i64));
        let cb_box = idx_box.clone();
        let sink: Sink = Rc::new(move |msgs: &[Msg]| {
            let i = cb_box.get();
            if i < 0 {
                return; // dep removed — stale in-flight callback drops (drain)
            }
            weak.receive_from_dep(i as usize, msgs);
        });
        // dep.subscribe synchronously pushes its START snapshot into the sink, which
        // records it as this dependency's input — done before we re-borrow self below.
        let dep_sub_id = Cell::new(None);
        let subscribed = catch_unwind(AssertUnwindSafe(|| {
            dep.subscribe_recording_id_with_kind(sink, SubscriberKind::Dependent, &dep_sub_id)
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
        let (unsubs, lifetime) = self.with_node_state_aux_mut(|n, c, _cfg, r, e, a| {
            a.activated = false;
            let lifetime = a.lifetime.take();
            if let Some(lt) = &lifetime {
                lt.signal.abort();
            }
            let unsubs: Vec<Unsub> = e.unsubs.drain(..).flatten().collect();
            e.idx_boxes.clear();
            // A computed node clears its retained values on deactivation; a state node
            // keeps them, and so does an ended node (R-lifetime).
            let is_compute = c.handle.is_some() || !n.deps.is_empty();
            if is_compute && !e.value.terminal {
                e.value.clear_values();
            }
            e.state = DepState::new(n.deps.len());
            e.wave.pending = false;
            e.wave.run_out.clear();
            r.has_called_fn_once = false;
            (unsubs, lifetime)
        });
        self.finish_close(unsubs, lifetime);
    }

    /// The rest of closing a lifetime, after its signal was aborted (R-lifetime@2):
    /// release the dependencies, run the cleanup, which may still read the private
    /// state, then drop the private state. Each step's panic goes to the host
    /// reporter and the close completes.
    fn finish_close(&self, unsubs: Vec<Unsub>, lifetime: Option<crate::lifetime::Lifetime>) {
        for u in unsubs {
            run_reported("release", u);
        }
        if let Some(cleanup) = lifetime.and_then(|lt| lt.cleanup) {
            let cctx = crate::lifetime::CleanupCtx {
                state: self.get_state(),
            };
            self.with_aux_mut(|a| a.closing = true);
            run_reported("cleanup", move || cleanup(&cctx));
            self.with_aux_mut(|a| a.closing = false);
        }
        let state = self.with_aux_mut(|a| a.state.take());
        // Dropping private state may drop captured handles: after the arena borrow.
        drop(state);
    }

    /// Whether `id` is this node's open lifetime: output and requests through a ctx
    /// of a closed lifetime are dropped (R-lifetime@2).
    pub(crate) fn lifetime_open(&self, id: u64) -> bool {
        self.with_aux(|a| a.lifetime.as_ref().is_some_and(|lt| lt.id == id))
    }

    // ── upstream receive (R-wave) ──

    /// One array from dependency `idx`. A lone `[PENDING]` is the kernel's pending
    /// mark (it never reaches an external sink); any other array is that dependency's
    /// published output, recorded whole as this wave's input.
    fn receive_from_dep_registered(&self, idx: usize, msgs: &[Msg]) {
        if let [Message::Pending] = msgs {
            self.receive_mark(idx);
            return;
        }
        let frame: Vec<InMsg> = msgs.iter().filter_map(InMsg::from_message).collect();
        let ready = self.with_inner_edges_mut(|_, e| {
            if e.value.terminal {
                return false;
            }
            let Some(input) = e.state.inputs.get_mut(idx) else {
                return false;
            };
            let settles = input.record(frame);
            if settles && input.pending {
                input.pending = false;
                e.state.pending -= 1;
            }
            !e.wave.in_dep_mutation && e.state.pending == 0 && e.state.has_frames()
        });
        if ready {
            self.schedule_settle();
        }
    }

    /// The kernel marks a sender's dependents pending before it delivers (R-wave). A
    /// quiet node records its pending inputs but passes no mark on (R-wave).
    fn receive_mark(&self, idx: usize) {
        let quiet = self.with_config(|cfg| cfg.quiet);
        let propagate = self.with_inner_edges_mut(|_, e| {
            if e.value.terminal {
                return false;
            }
            let Some(input) = e.state.inputs.get_mut(idx) else {
                return false;
            };
            if !input.pending {
                input.pending = true;
                e.state.pending += 1;
            }
            if quiet || e.wave.pending {
                return false;
            }
            e.wave.pending = true;
            true
        });
        if propagate {
            self.mark_dependents();
        }
    }

    /// Sends the pending mark to this node's dependents.
    fn mark_dependents(&self) {
        let deps = self.with_aux(snapshot_dependents);
        deliver_subscriber_snapshot(deps, &[vec![Message::Pending]]);
    }

    /// Marks this node pending as the source of a wave, and its cone with it.
    fn mark_as_source(&self) {
        let fresh = self.with_inner_edges_mut(|_, e| {
            if e.wave.pending {
                false
            } else {
                e.wave.pending = true;
                true
            }
        });
        if fresh {
            self.mark_dependents();
        }
    }

    fn schedule_settle(&self) {
        if delivery_in_flight() {
            defer_settle_until_delivery_boundary(self);
        } else {
            self.settle();
        }
    }

    /// Settles a node once every dependency marked pending in this wave has settled
    /// (R-wave): a dependency's auto ERROR ends it, an INVALIDATE is applied under the
    /// auto policy, and the function runs at most once on everything the wave brought.
    fn settle(&self) {
        let plan = self.with_node_state_aux_mut(|n, c, cfg, r, e, _a| {
            if e.value.terminal
                || e.state.pending != 0
                || e.wave.in_dep_mutation
                || !e.state.has_frames()
            {
                return None;
            }
            let dep_error = e.state.inputs.iter().find_map(|input| {
                input.frames.iter().flatten().find_map(|m| match m {
                    InMsg::Error(err) => Some(err.clone()),
                    _ => None,
                })
            });
            if let (Some(err), Policy::Auto) = (dep_error, cfg.error) {
                // An INVALIDATE that reached the node before the ERROR takes effect
                // first, whatever the INVALIDATE policy (R-terminals).
                let invalidated_first = e
                    .state
                    .inputs
                    .iter()
                    .any(|input| input.frames.iter().any(|f| starts_with_invalidate(f)));
                e.state.clear_frames();
                return Some(SettlePlan::End {
                    invalidated_first,
                    error: err,
                });
            }
            // What came before an INVALIDATE on an input is gone; what came after it is
            // ordinary input (R-invalidation).
            let mut invalidated = false;
            for input in &mut e.state.inputs {
                if let Some(at) = input.frames.iter().rposition(|f| starts_with_invalidate(f)) {
                    input.frames.drain(..=at);
                    input.invalidated = true;
                    invalidated = true;
                }
            }
            let values = e
                .state
                .inputs
                .iter()
                .any(|input| input.frames.iter().flatten().any(InMsg::is_value));
            // A terminal is the function's input under its manual policy.
            let terminal_input = e.state.inputs.iter().any(|input| {
                input.frames.iter().flatten().any(|m| match m {
                    InMsg::Complete => cfg.complete == Policy::Manual,
                    InMsg::Error(_) => cfg.error == Policy::Manual,
                    _ => false,
                })
            });
            let invalidate_input = invalidated && cfg.invalidate == Policy::Manual;
            let gate_open = r.has_called_fn_once
                || cfg.partial
                || cfg.all_deps_settled(&e.state)
                || c.handle.is_none();
            let runs = (values || terminal_input || invalidate_input) && gate_open;
            let all_ended =
                !n.deps.is_empty() && e.state.inputs.iter().all(|input| input.ended.is_some());
            Some(SettlePlan::Run {
                invalidated: invalidated && cfg.invalidate == Policy::Auto,
                runs,
                auto_complete: cfg.complete == Policy::Auto && all_ended,
                quiet: cfg.quiet,
                passthrough: c.handle.is_none(),
            })
        });
        let Some(plan) = plan else {
            return;
        };
        match plan {
            SettlePlan::End {
                invalidated_first,
                error,
            } => {
                let mut end = Vec::new();
                if invalidated_first {
                    end.push(vec![Message::Invalidate]);
                }
                end.push(vec![Message::Error(error.to_string().into())]);
                self.publish(end);
            }
            SettlePlan::Run {
                invalidated,
                runs,
                auto_complete,
                quiet,
                passthrough,
            } => {
                let lead = if invalidated {
                    self.apply_out(vec![vec![Message::Invalidate]])
                } else {
                    Vec::new()
                };
                let out = if !runs {
                    Vec::new()
                } else if passthrough {
                    self.passthrough_frames()
                } else {
                    self.invoke()
                };
                self.with_inner_edges_mut(|_, e| e.state.clear_frames());
                let postlude = if auto_complete && !out.iter().flatten().any(Message::is_terminal) {
                    vec![vec![Message::Complete]]
                } else {
                    Vec::new()
                };
                if quiet {
                    // What a quiet node outputs is a later wave of its own; only an auto
                    // INVALIDATE goes on now (R-wave).
                    if !lead.is_empty() {
                        self.deliver_out(lead);
                    }
                    self.with_inner_edges_mut(|_, e| e.wave.pending = false);
                    let mut later = out;
                    later.extend(postlude);
                    if !later.is_empty() {
                        self.request_publish_next(later);
                    }
                    return;
                }
                let mut tail = out;
                tail.extend(postlude);
                let mut all = lead;
                all.extend(self.apply_out(tail));
                self.deliver_out(all);
            }
        }
    }

    /// A lifetime's first run, on the dependencies' snapshots (R-lifetime). Waiting for
    /// every dependency to have a value is the Rust first-run gate (sugar).
    fn first_run(&self) {
        let plan = self.with_node_state_aux_mut(|n, c, cfg, r, e, _a| {
            if e.value.terminal {
                return None;
            }
            let dep_error = e.state.inputs.iter().find_map(|input| match &input.ended {
                Some(DepTerminal::Error(err)) => Some(err.clone()),
                _ => None,
            });
            let all_ended =
                !n.deps.is_empty() && e.state.inputs.iter().all(|input| input.ended.is_some());
            let restored_producer = n.deps.is_empty() && r.has_called_fn_once;
            let gate_open = cfg.partial || cfg.all_deps_settled(&e.state);
            Some((
                dep_error.filter(|_| cfg.error == Policy::Auto),
                c.handle.is_some() && gate_open && !restored_producer,
                c.handle.is_none() && !n.deps.is_empty(),
                cfg.complete == Policy::Auto && all_ended,
            ))
        });
        let Some((dep_error, runs, passthrough, auto_complete)) = plan else {
            return;
        };
        let out = if let Some(err) = dep_error {
            vec![vec![Message::Error(err.to_string().into())]]
        } else if runs {
            self.invoke()
        } else if passthrough {
            self.passthrough_frames()
        } else {
            Vec::new()
        };
        self.with_inner_edges_mut(|_, e| e.state.clear_frames());
        let mut all = out;
        if auto_complete && !all.iter().flatten().any(Message::is_terminal) {
            all.push(vec![Message::Complete]);
        }
        if !all.is_empty() {
            self.publish(all);
        }
    }

    /// A node with dependencies and no function relays dependency 0's DATA, one array
    /// per array it received.
    fn passthrough_frames(&self) -> Vec<Wave<AnyValue>> {
        self.with_inner_edges(|_n, e| {
            e.state.inputs.first().map_or_else(Vec::new, |input| {
                input
                    .frames
                    .iter()
                    .map(|f| {
                        f.iter()
                            .filter_map(|m| match m {
                                InMsg::Data(v) => Some(Message::Data(v.clone())),
                                _ => None,
                            })
                            .collect::<Vec<_>>()
                    })
                    .filter(|f| !f.is_empty())
                    .collect()
            })
        })
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
        self.external_rewire(RewireRequest::Set(new_deps, fn_));
    }

    fn external_rewire(&self, req: RewireRequest) {
        req.validate_deps_same_graph(self);
        if let Some(committed) = committed_after_batch_for_target(self) {
            defer_boundary(
                self,
                BoundaryTask::ExternalRewire {
                    target: CoreToken::from_core(self),
                    req,
                    committed,
                },
            );
            return;
        }
        self.apply_external_rewire(req);
    }

    fn apply_external_rewire(&self, req: RewireRequest) {
        let current = self.deps();
        let (new_deps, fn_) = req.into_deps_and_fn(current);
        self.rewire_inner(new_deps, fn_, false);
    }

    fn project_pending_external_rewire_deps(&self) -> Vec<Core> {
        let target = CoreToken::from_core(self);
        let mut deps = self.deps();
        let graph = self.graph.borrow();
        for task in &graph.deferred_boundary {
            let BoundaryTask::ExternalRewire {
                target: queued_target,
                req,
                ..
            } = task
            else {
                continue;
            };
            if queued_target.graph_id == target.graph_id && queued_target.key == target.key {
                deps = req.project_deps(deps);
            }
        }
        deps
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
                        req: RewireRequest::Set(new_deps, fn_),
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

    pub(crate) fn request_down_next(&self, msgs: Wave<AnyValue>) {
        defer_boundary(
            self,
            BoundaryTask::Down {
                target: CoreToken::from_core(self),
                frames: prepare_output(msgs),
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

    /// The surgical Option-C mutation + settle (D42), run under a wave-owner. Kept deps
    /// keep their subscription, input record and idx-box (rerouted O(1)); removed deps
    /// drain (box→-1, unsub) and drop their pending mark; added deps are acquired, their
    /// snapshots recorded as one input wave, and the node settles on them once.
    fn rewire_apply(&self, new_deps: Vec<Core>, fn_: NodeFn) {
        let old_deps = self.borrow().deps.clone();
        self.with_inner_edges_mut(|_, e| e.wave.in_dep_mutation = true);
        // QA-F1: clear `in_dep_mutation` on EVERY exit, including a panic unwind during
        // the mutation (an added dep's activation or a removed dep's cleanup).
        let mut_guard = DepMutationGuard(DeferredNodeAction::from_core(self));
        // fn swap (SD-1 + B32 GC): register the new fn in the SAME pool, then unregister
        // the old handle so the rewired-away closure (and its captures) is freed.
        self.swap_fn_preserving_pool(fn_);
        if same_ordered_deps(&old_deps, &new_deps) {
            return;
        }
        let n = new_deps.len();
        let mut new_to_old: Vec<Option<usize>> = vec![None; n];
        let mut old_kept = vec![false; old_deps.len()];
        for (new_idx, new_dep) in new_deps.iter().enumerate() {
            if let Some(old_idx) = old_deps
                .iter()
                .enumerate()
                .position(|(old_idx, old_dep)| !old_kept[old_idx] && old_dep.ptr_eq(new_dep))
            {
                old_kept[old_idx] = true;
                new_to_old[new_idx] = Some(old_idx);
            }
        }
        let activated = self.with_aux(|a| a.activated);
        // removed deps: drop their pending mark, then drain (box→-1, unsubscribe).
        for (old_idx, _) in old_kept.iter().enumerate().filter(|(_, kept)| !**kept) {
            let unsub = {
                let mut graph = self.graph.borrow_mut();
                let (_nn, edges) = graph.get_node_and_edges_mut(self.key());
                if edges
                    .state
                    .inputs
                    .get(old_idx)
                    .is_some_and(|input| input.pending)
                {
                    edges.state.pending -= 1;
                }
                if let Some(b) = edges.idx_boxes.get(old_idx) {
                    b.set(-1);
                }
                edges.unsubs.get_mut(old_idx).and_then(Option::take)
            };
            if let Some(u) = unsub {
                u();
            }
        }
        // rebuild the per-dep records in new order; kept deps carry theirs.
        {
            let mut graph = self.graph.borrow_mut();
            let (nn, edges) = graph.get_node_and_edges_mut(self.key());
            let mut inputs = Vec::with_capacity(n);
            let mut unsubs: Vec<Option<Unsub>> = Vec::with_capacity(n);
            let mut boxes: Vec<Rc<Cell<i64>>> = Vec::with_capacity(n);
            for (j, old_idx) in new_to_old.iter().enumerate() {
                match *old_idx {
                    Some(old_idx) => {
                        inputs.push(std::mem::take(&mut edges.state.inputs[old_idx]));
                        unsubs.push(edges.unsubs.get_mut(old_idx).and_then(Option::take));
                        let b = edges.idx_boxes[old_idx].clone();
                        b.set(j as i64); // O(1) reroute of the kept dep's callback index
                        boxes.push(b);
                    }
                    None => {
                        inputs.push(DepInput::default());
                        unsubs.push(None);
                        boxes.push(Rc::new(Cell::new(-1i64)));
                    }
                }
            }
            nn.deps = new_deps.clone();
            edges.state.inputs = inputs;
            edges.unsubs = unsubs;
            edges.idx_boxes = boxes;
        }
        if activated {
            for (idx, dep) in new_deps.iter().enumerate() {
                if new_to_old[idx].is_none() {
                    self.subscribe_dep(idx, dep);
                }
            }
        }
        self.notify_topology_deps_changed(&old_deps, &new_deps);
        drop(mut_guard);
        // D62: a terminal owner still applies queued topology intent, but emits nothing.
        if self.with_inner_edges(|_, e| e.value.terminal) {
            return;
        }
        self.settle_rewire();
    }

    /// After a rewire: the added dependencies' snapshots are one input wave, so the
    /// node settles on them; a removed pending input that was the last to settle leaves
    /// a pending node that sent nothing, which gets the kernel's RESOLVED.
    fn settle_rewire(&self) {
        let (ready, owes_resolved) = self.with_inner_edges(|_, e| {
            (
                e.state.pending == 0 && e.state.has_frames(),
                e.state.pending == 0 && e.wave.pending,
            )
        });
        if ready {
            self.settle();
        } else if owes_resolved {
            self.deliver_out(Vec::new());
        }
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

    /// Runs the node's function once on this wave's input (R-invocation) and returns
    /// what it sent through `ctx.down`, each call one array, in order. A panic in the
    /// function is the node's ERROR, and what it sent before the panic is dropped
    /// (R-failure).
    fn invoke(&self) -> Vec<Wave<AnyValue>> {
        // B25: enroll in the wave touched-set before any mutation so a panic-abort
        // can reset our flags.
        wave_register(self);
        if self.with_inner_edges(|_, e| e.wave.inside_run_wave) {
            // R-reentrancy (D37): a fn re-driving its own dep mid-wave is the
            // synchronous feedback cycle. Reject by panicking — the enclosing run turns
            // it into ERROR on a node ON the cycle (C-6).
            panic!(
                "synchronous feedback cycle: node fn re-entered its own wave (R-reentrancy / D37)"
            );
        }
        // Build ctx + call snapshot under one short arena borrow, then invoke
        // borrow-free through the dispatcher (R-dispatch-all / R-sync-core).
        let (handle, dispatcher, ctx, setup) =
            self.with_node_state_aux_mut(|n, c, cfg, r, e, a| {
                let invalidate_manual = cfg.invalidate == Policy::Manual;
                let dep_records: Vec<DepRecord> = e
                    .state
                    .inputs
                    .iter()
                    .zip(&n.deps)
                    .map(|(input, dep)| DepRecord {
                        dep: CoreIdentity::from_core(dep),
                        wave_data: input
                            .frames
                            .iter()
                            .filter(|f| f.iter().any(InMsg::is_value))
                            .map(|f| {
                                f.iter()
                                    .filter_map(|m| match m {
                                        InMsg::Data(v) => Some(v.clone()),
                                        _ => None,
                                    })
                                    .collect()
                            })
                            .collect(),
                        prev_data: if input.frames.is_empty() {
                            input.value.clone()
                        } else {
                            input.value_before_wave.clone()
                        },
                        latest: input.value.clone(),
                        terminal: input.frames.iter().flatten().find_map(InMsg::terminal),
                        invalidated: input.invalidated && invalidate_manual,
                    })
                    .collect();
                r.has_called_fn_once = true;
                e.wave.inside_run_wave = true;
                e.wave.run_out.clear();
                e.wave.emit_open = false;
                // A run belongs to the open lifetime; the setup runs before the
                // lifetime's first run (R-lifetime@2).
                let lifetime = a
                    .lifetime
                    .get_or_insert_with(crate::lifetime::Lifetime::open);
                let setup = if lifetime.setup_done {
                    None
                } else {
                    lifetime.setup_done = true;
                    cfg.setup.clone()
                };
                (
                    c.handle.expect("invoke ⇒ handle present"),
                    c.dispatcher.clone(),
                    Ctx::new(
                        self.borrowed_view(),
                        dep_records,
                        lifetime.id,
                        lifetime.signal.clone(),
                    ),
                    setup,
                )
            });
        // D30: this is the innermost node whose fn runs.
        wave_set_blamed(self);
        let guard = WaveGuard(DeferredNodeAction::from_core(self));
        let result = catch_unwind(AssertUnwindSafe(|| {
            if let Some(setup) = setup {
                // What the setup sends is part of this run's output; its panic ends the
                // node as the function's does.
                let cleanup = (setup.0)(&ctx);
                let lifetime = ctx.lifetime_id();
                let unplaced = self.with_aux_mut(|a| match &mut a.lifetime {
                    Some(lt) if lt.id == lifetime => {
                        lt.cleanup = cleanup;
                        None
                    }
                    _ => cleanup,
                });
                // The lifetime closed while its setup ran: release what it opened.
                if let Some(cleanup) = unplaced {
                    let cctx = crate::lifetime::CleanupCtx { state: None };
                    run_reported("cleanup", move || cleanup(&cctx));
                }
            }
            dispatcher.invoke(handle, &ctx)
        }));
        drop(guard); // clears inside_run_wave
        let out = self.with_inner_edges_mut(|_, e| std::mem::take(&mut e.wave.run_out));
        match result {
            Ok(()) => out,
            Err(payload) => vec![vec![Message::Error(panic_to_error(payload))]],
        }
    }

    // ── downstream output (R-wave) ──

    /// One DATA value from the running function (`ctx.emit`). Consecutive emits of one
    /// run join one array; a `ctx.down` in between closes it. This is Rust sugar
    /// (graphrefly:D797 (9) leaves the combinators' array rule to each language): an
    /// operator that emits per value still sends what one run produced as one array.
    pub(crate) fn emit_value(&self, value: AnyValue) {
        let (terminal, inside) =
            self.with_inner_edges(|_n, e| (e.value.terminal, e.wave.inside_run_wave));
        if terminal {
            return;
        }
        if !inside {
            self.down(vec![Message::Data(value)]);
            return;
        }
        self.with_inner_edges_mut(|_, e| {
            if e.wave.emit_open {
                if let Some(last) = e.wave.run_out.last_mut() {
                    last.push(Message::Data(value));
                    return;
                }
            }
            e.wave.run_out.push(vec![Message::Data(value)]);
            e.wave.emit_open = true;
        });
    }

    /// One submitted array from this node: from its running function (`ctx.down`),
    /// from the host, or as later output. The array keeps its written order, a
    /// terminal must be last, and it is split at its last INVALIDATE (R-wave). Called
    /// while the function runs, it joins that run's output, published after the
    /// function returns; otherwise it is a wave of its own (or staged in a batch).
    pub(crate) fn down(&self, msgs: Vec<Msg>) {
        let (terminal, inside) =
            self.with_inner_edges(|_n, e| (e.value.terminal, e.wave.inside_run_wave));
        if terminal {
            return;
        }
        let frames = prepare_output(msgs);
        if inside {
            self.with_inner_edges_mut(|_, e| {
                e.wave.emit_open = false;
                e.wave.run_out.extend(frames);
            });
            return;
        }
        if frames.is_empty() {
            return;
        }
        let frames = if collecting_batch() {
            let flat: Wave<AnyValue> = frames.into_iter().flatten().collect();
            if defer_to_batch(self, flat) {
                return;
            }
            unreachable!("defer_to_batch accepts output while a batch is collecting")
        } else {
            frames
        };
        wave_register(self);
        self.publish(frames);
    }

    /// Applies arrays to this node, then delivers what remains (R-wave).
    fn publish(&self, frames: Vec<Wave<AnyValue>>) {
        let out = self.apply_out(frames);
        self.deliver_out(out);
    }

    /// Writes the node's own output into what it holds (values, invalidation,
    /// terminal) and returns the arrays that remain to be sent. An INVALIDATE at a node
    /// that retains no value is dropped (R-invalidation); nothing follows a terminal
    /// (R-terminals).
    fn apply_out(&self, frames: Vec<Wave<AnyValue>>) -> Vec<Wave<AnyValue>> {
        let mut out = Vec::new();
        for frame in frames {
            if self.with_inner_edges(|_, e| e.value.terminal) {
                break;
            }
            let mut kept = Vec::with_capacity(frame.len());
            let mut values = Vec::new();
            for m in frame {
                match &m {
                    Message::Data(value) => {
                        let next_version = self.next_version(value);
                        let mut g = self.graph.borrow_mut();
                        g.get_version_mut(self.key()).value = next_version;
                        let (_n, e) = g.get_node_and_edges_mut(self.key());
                        e.value.cache = Some(value.clone());
                        e.value.has_data = true;
                        values.push(value.clone());
                    }
                    Message::Resolved => {}
                    Message::Invalidate => {
                        // A node with no value drops an INVALIDATE (R-invalidation).
                        let had_value = self.with_inner_edges_mut(|_, e| {
                            let had = e.value.has_data;
                            e.value.clear_values();
                            had
                        });
                        if !had_value {
                            continue;
                        }
                        values.clear();
                    }
                    Message::Complete | Message::Error(_) => {
                        let ended = match &m {
                            Message::Error(err) => DepTerminal::Error(format!("{err}").into()),
                            _ => DepTerminal::Complete,
                        };
                        self.with_inner_edges_mut(|_, e| {
                            e.value.terminal = true;
                            e.value.ended = Some(ended);
                        });
                    }
                    Message::Start | Message::Pending => continue,
                }
                kept.push(m);
            }
            if !values.is_empty() {
                self.with_inner_edges_mut(|_, e| e.value.values = values);
            }
            if !kept.is_empty() {
                out.push(kept);
            }
        }
        out
    }

    fn next_version(&self, value: &AnyValue) -> Option<NodeVersion> {
        let (policy, current) = {
            let graph = self.graph.borrow();
            let version = graph.get_version(self.key());
            (version.policy.clone(), version.value.clone())
        };
        assert_node_version_data_compatible(&policy, value).unwrap_or_else(|err| panic!("{err}"));
        advance_node_version(current.as_ref(), &policy, value).unwrap_or_else(|err| panic!("{err}"))
    }

    /// Delivers arrays already applied to this node (R-wave). A pending node that ends
    /// up with nothing settling gets exactly one RESOLVED from the kernel. A node that
    /// sends without being pending marks its own cone first. Every array that carries
    /// DATA, RESOLVED, INVALIDATE, COMPLETE or ERROR gets the PENDING prefix. All arrays
    /// go to one subscriber before the next.
    fn deliver_out(&self, mut out: Vec<Wave<AnyValue>>) {
        let pending = self.with_inner_edges(|_, e| e.wave.pending);
        if pending && !out.iter().flatten().any(Message::is_settling) {
            out.extend(self.apply_out(vec![vec![Message::Resolved]]));
        }
        if out.is_empty() {
            return;
        }
        if !pending && out.iter().flatten().any(Message::is_settling) {
            self.mark_as_source();
        }
        self.with_inner_edges_mut(|_, e| e.wave.pending = false);
        let out: Vec<Wave<AnyValue>> = out
            .into_iter()
            .map(|f| {
                if f.iter().any(Message::is_settling) {
                    let mut prefixed = Vec::with_capacity(f.len() + 1);
                    prefixed.push(Message::Pending);
                    prefixed.extend(f);
                    prefixed
                } else {
                    f
                }
            })
            .collect();
        let subs = self.with_aux(snapshot_subscribers);
        with_delivery_scope(|| deliver_subscriber_snapshot(subs, &out));
        if self.with_inner_edges(|_, e| e.value.terminal) {
            self.close_ended_lifetime();
        }
    }

    /// A node that ends closes its lifetime at once (R-lifetime, R-terminals): it
    /// releases its dependencies, runs its cleanup and drops its private state, and
    /// keeps its values, terminal and subscribers.
    fn close_ended_lifetime(&self) {
        let (unsubs, lifetime) = self.with_inner_edges_aux_mut(|n, e, a| {
            let lifetime = a.lifetime.take();
            if let Some(lt) = &lifetime {
                lt.signal.abort();
            }
            for b in &e.idx_boxes {
                b.set(-1);
            }
            let unsubs: Vec<Unsub> = e.unsubs.iter_mut().filter_map(Option::take).collect();
            e.state = DepState::new(n.deps.len());
            (unsubs, lifetime)
        });
        self.finish_close(unsubs, lifetime);
    }

    /// External deferred emit — the async-pool late-emit path used by
    /// [`crate::ctx::DeferredCtx`]: a FRESH wave from this node, under its own
    /// wave-owner boundary (D30).
    pub(crate) fn owned_down(&self, msgs: Wave<AnyValue>) {
        with_wave_owner(self, || self.down(msgs), || {});
    }

    /// Publishes arrays as one wave of their own, under a fresh wave owner.
    fn owned_publish(&self, frames: Vec<Wave<AnyValue>>) {
        with_wave_owner(
            self,
            || {
                if !self.with_inner_edges(|_, e| e.value.terminal) {
                    wave_register(self);
                    self.publish(frames);
                }
            },
            || {},
        );
    }

    /// Queues arrays as a later wave from this node, after the current one.
    fn request_publish_next(&self, frames: Vec<Wave<AnyValue>>) {
        defer_boundary(
            self,
            BoundaryTask::Down {
                target: CoreToken::from_core(self),
                frames,
                committed: active_batch_committed_token()
                    .unwrap_or_else(|| Rc::new(Cell::new(true))),
            },
        );
    }

    // ── ctx.state + hooks (called from Ctx) ──

    pub(crate) fn get_state(&self) -> Option<AnyValue> {
        self.with_aux(|a| a.state.clone())
    }
    pub(crate) fn set_state(&self, v: AnyValue) {
        // The old value may hold the last handle to another node: drop it after the
        // arena borrow.
        let old = self.with_aux_mut(|a| a.state.replace(v));
        drop(old);
    }

    /// A state node: no function and no dependencies.
    pub(crate) fn is_state(&self) -> bool {
        self.with_call(|c| c.handle.is_none()) && self.borrow().deps.is_empty()
    }

    pub(crate) fn subscriber_count(&self) -> usize {
        self.with_aux(|a| a.subscribers.len())
    }

    pub(crate) fn is_active(&self) -> bool {
        self.with_aux(|a| a.activated)
    }
}

fn reset_wave_flags_inner(_n: &mut NodeTopologySlot, e: &mut DepEdges) {
    e.wave.inside_run_wave = false;
    e.wave.pending = false;
    e.wave.run_out.clear();
    e.state.clear_frames();
    // A dependency's recorded value and terminal are cross-wave facts (the dep really
    // did deliver or end), not wave transients; a panic-aborted wave keeps them.
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
    /// (R-initial). The host writes it through the returned [`State`].
    pub fn state(initial: T) -> State<T> {
        State::from_node(Self::state_in_arena(&GraphArena::default(), initial))
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

    /// A state node with no value until the first [`State::set`].
    pub fn state_empty() -> State<T> {
        State::from_node(Self::state_empty_in_arena(&GraphArena::default()))
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
        let factory = opts.factory.clone();
        let handle = register_with(&dispatcher, opts.pool, Rc::new(f));
        let node = Node {
            core: Core::new_in_arena(arena, vec![], Some(handle), dispatcher, None, opts.clone()),
            _t: PhantomData,
        };
        node.core.with_node_state_mut(|_n, call, _cfg, _r, _e| {
            call.factory = factory;
        });
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
                cfg.complete = opts.complete;
                cfg.error = opts.error;
                cfg.invalidate = opts.invalidate;
                cfg.versioning = opts.versioning;
                call.factory = factory;
            });
        }
        node
    }

    /// Writes a value as one DATA array: the crate-internal host write behind
    /// [`State::set`]. Every occurrence stays DATA; the kernel never absorbs an equal
    /// value (D49).
    pub(crate) fn set(&self, v: T) {
        self.down(vec![Message::Data(Rc::new(v))]);
    }

    /// Publishes one array from outside any function run, under its own wave owner: the
    /// crate-internal host input behind [`State`].
    ///
    /// # Panics
    ///
    /// Panics when `msgs` contains PENDING or RESOLVED, which only the kernel sends
    /// (R-wave); nothing of the array is applied.
    pub(crate) fn down(&self, msgs: Wave<AnyValue>) {
        // Rejections reach the caller, before the wave owner would turn them into the
        // node's ERROR (R-failure).
        reject_kernel_only_output(&msgs);
        check_output_shape(&msgs);
        crate::batch::assert_stageable(&self.core, &msgs);
        with_wave_owner(&self.core, || self.core.down(msgs), || {});
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
        self.core.status()
    }

    /// Read-only node runtime version metadata (D109).
    pub fn version(&self) -> Option<NodeVersion> {
        self.core.version()
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
    pub fn subscribe(&self, sink: impl Fn(&[Msg]) + 'static) -> Unsub {
        self.subscribe_with_kind(sink, SubscriberKind::External)
    }

    pub(crate) fn subscribe_graph_observer(&self, sink: impl Fn(&[Msg]) + 'static) -> Unsub {
        self.subscribe_with_kind(sink, SubscriberKind::GraphObserver)
    }

    fn subscribe_with_kind(&self, sink: impl Fn(&[Msg]) + 'static, kind: SubscriberKind) -> Unsub {
        // R-failure@3: a user sink's panic goes to the host reporter, not into a node.
        let sink = Rc::new(crate::report::guard_sink(sink));
        // Record the subscriber id before activation so the error path hands back a real
        // unsubscribe instead of leaking the just-registered sink (no orphaned sink).
        let id_cell: Rc<Cell<Option<u64>>> = Rc::new(Cell::new(None));
        let body_cell = id_cell.clone();
        with_wave_owner(
            &self.core,
            || {
                self.core
                    .subscribe_recording_id_with_kind(sink, kind, &body_cell)
            },
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
        let mut next = self.core.project_pending_external_rewire_deps();
        if !next.iter().any(|d| d.ptr_eq(&dep)) {
            next.push(dep.clone());
        }
        let index = next
            .iter()
            .position(|d| d.ptr_eq(&dep))
            .expect("subscribe_dep next shape contains dep");
        self.core
            .external_rewire(RewireRequest::Add(dep, Rc::new(f)));
        index
    }

    /// Unsubscribe from one dep (special case of [`Node::replace_deps`]); idempotent if absent (the fn
    /// swap still applies). fn required (SD-1).
    pub fn unsubscribe_dep<F: Fn(&Ctx) + 'static>(&self, dep: Core, f: F) {
        self.core
            .external_rewire(RewireRequest::remove(&dep, Rc::new(f)));
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
/// A state node's handle, the only host input (R-wave; graphrefly-rs:D3): a wave starts
/// from `set`, `invalidate`, `complete` or `error` on a state node. Dereferences to its
/// [`Node`] for subscribing, reading and use as a dependency.
pub struct State<T> {
    node: Node<T>,
}

impl<T: 'static> State<T> {
    pub(crate) fn from_node(node: Node<T>) -> Self {
        Self { node }
    }

    /// The state's node.
    #[must_use]
    pub fn node(&self) -> &Node<T> {
        &self.node
    }

    /// The state's node, by value.
    #[must_use]
    pub fn into_node(self) -> Node<T> {
        self.node
    }

    /// Writes `v` as one DATA wave. Every occurrence stays DATA, an equal value
    /// included (D49).
    ///
    /// # Panics
    ///
    /// Panics when the state has ended: host input to an ended node is refused
    /// (R-terminals). The same holds for every host input below.
    pub fn set(&self, v: T) {
        self.assert_open();
        self.node.set(v);
    }

    /// Removes the retained value, if any, as one INVALIDATE wave (R-invalidation).
    pub fn invalidate(&self) {
        self.assert_open();
        self.node.down(vec![Message::Invalidate]);
    }

    /// Ends the state with COMPLETE (R-terminals).
    pub fn complete(&self) {
        self.assert_open();
        self.node.down(vec![Message::Complete]);
    }

    /// Ends the state with ERROR carrying `error` (R-terminals).
    pub fn error(&self, error: impl Into<GraphError>) {
        self.assert_open();
        self.node.down(vec![Message::Error(error.into())]);
    }

    fn assert_open(&self) {
        assert!(
            !self.node.core.with_inner_edges(|_, e| e.value.terminal),
            "host input to an ended state is refused (R-terminals)"
        );
    }
}

impl<T> Clone for State<T> {
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
        }
    }
}

impl<T> From<State<T>> for Node<T> {
    fn from(state: State<T>) -> Self {
        state.node
    }
}

impl<T> std::ops::Deref for State<T> {
    type Target = Node<T>;

    fn deref(&self) -> &Node<T> {
        &self.node
    }
}

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
    type KindLog = Rc<RefCell<Vec<String>>>;

    /// Node options whose setup returns a cleanup that runs `f`.
    fn with_cleanup(f: impl Fn() + 'static) -> NodeOpts {
        let f = Rc::new(f);
        NodeOpts::default().with_setup(move |_| {
            let f = f.clone();
            crate::lifetime::cleanup(move |_| f())
        })
    }

    fn recorder() -> (KindLog, impl Fn(&[Msg]) + 'static) {
        let log = Rc::new(RefCell::new(Vec::new()));
        let l2 = log.clone();
        (log, move |msgs: &[Msg]| {
            l2.borrow_mut()
                .extend(msgs.iter().map(|m| format!("{m:?}")));
        })
    }

    #[test]
    fn status_freshness_and_terminality() {
        let status = |cache, output| Status {
            cache,
            output,
            lifecycle: Lifecycle::Active,
        };
        assert!(status(CacheStatus::Ready, OutputStatus::Open).is_fresh());
        assert!(!status(CacheStatus::Stale, OutputStatus::Open).is_fresh());
        assert!(!status(CacheStatus::Empty, OutputStatus::Open).is_fresh());
        assert!(status(CacheStatus::Ready, OutputStatus::Completed).is_terminal());
        assert!(!status(CacheStatus::Ready, OutputStatus::Open).is_terminal());
        assert_eq!(
            status(CacheStatus::Ready, OutputStatus::Errored).label(),
            "errored"
        );
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
        assert_eq!(*log.borrow(), vec!["START", "DATA", "PENDING", "DATA"]);
        assert_eq!(s.cache(), Some(2));
        assert_eq!(s.status().cache, crate::CacheStatus::Ready);
    }

    #[test]
    fn batch_commits_one_array_and_rollback_delivers_nothing() {
        // R-request-order: a batch delivers nothing before its commit, and its writes
        // to one node commit as one array; a rollback drops them with nothing to undo.
        let s = Node::<i32>::state(0);
        let (log, sink) = recorder();
        let _u = s.subscribe(sink);
        log.borrow_mut().clear();

        crate::batch::batch(|_| {
            s.set(1);
            s.set(2);
            assert!(log.borrow().is_empty());
            assert_eq!(s.cache(), Some(0));
        });
        assert_eq!(*log.borrow(), vec!["PENDING", "DATA", "DATA"]);
        assert_eq!(s.cache(), Some(2));

        log.borrow_mut().clear();
        crate::batch::batch(|bctx| {
            s.set(3);
            bctx.rollback();
            assert!(log.borrow().is_empty());
        });
        assert!(log.borrow().is_empty());
        assert_eq!(s.cache(), Some(2));
    }

    #[test]
    fn d49_repeated_equal_value_stays_data() {
        // D49 (supersedes D15): every occurrence is DATA — NO equals-substitution.
        // Probe: fromIter([1,1,1]) must be [DATA,DATA,DATA] so take/scan/count work.
        let s = Node::<i32>::state(5);
        let (log, sink) = recorder();
        let _u = s.subscribe(sink);
        s.set(5); // unchanged value → still DATA (never absorbed to RESOLVED)
        assert_eq!(*log.borrow(), vec!["START", "DATA", "PENDING", "DATA"]);
        assert_eq!(s.status().cache, crate::CacheStatus::Ready);
        s.set(5); // again → still DATA
        assert_eq!(
            *log.borrow(),
            vec!["START", "DATA", "PENDING", "DATA", "PENDING", "DATA"]
        );
        assert_eq!(s.status().cache, crate::CacheStatus::Ready);
    }

    #[test]
    fn sink_panic_goes_to_host_reporter_and_is_not_the_node_error() {
        // R-failure@3: a sink failure has no node to end. It goes to the host reporter,
        // the node keeps running, and the wave guard is clear afterwards.
        let reports = Rc::new(RefCell::new(Vec::<crate::HostFailure>::new()));
        let previous = crate::set_host_reporter(Some({
            let reports = reports.clone();
            Rc::new(move |f: &crate::HostFailure| reports.borrow_mut().push(f.clone()))
        }));
        let source = Node::<i32>::state_empty();
        let derived: Node<i32> = Node::derived(vec![source.erased()], |ctx| {
            let _ = ctx.data::<i32>(0).unwrap();
        });
        let _u = derived.subscribe(
            move |msgs: &[crate::protocol::Message<crate::protocol::AnyValue>]| {
                if msgs.iter().any(|m| matches!(m, Message::Resolved)) {
                    panic!("resolved callback panic");
                }
            },
        );

        source.set(1);
        source.set(2);
        crate::set_host_reporter(previous);

        assert_eq!(
            *reports.borrow(),
            vec![
                crate::HostFailure {
                    source: "sink".into(),
                    error: "resolved callback panic".into()
                };
                2
            ]
        );
        assert_ne!(derived.status().output, crate::OutputStatus::Errored);
        assert!(
            !derived.core.with_inner_edges(|_, e| e.wave.inside_run_wave),
            "the wave guard is clear after the wave"
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
            let mid: Node<i32> = Node::derived_opts(
                vec![a.erased()],
                with_cleanup(move || flag.set(true)),
                |ctx| ctx.emit(*ctx.data::<i32>(0).unwrap() + 1),
            );
            let _sub = mid.subscribe(|_| {}); // activates mid: setup + fn, subscribes to a
            assert!(!deactivated.get());
            // `mid` + `_sub` drop here → mid's Core refcount hits 0 → Drop runs cleanup.
        }
        assert!(
            deactivated.get(),
            "dropping an active node runs its lifetime cleanup + detaches from deps (D1)"
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
        // The first write marked `sum` pending while the gate (Rust sugar) held, so the
        // kernel settled it with RESOLVED (R-wave).
        assert_eq!(
            *log.borrow(),
            vec!["START", "PENDING", "RESOLVED", "PENDING", "DATA"]
        );

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
        assert_eq!(*log.borrow(), vec!["START", "PENDING", "DATA"]);

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
        assert_eq!(dbl.status().cache, crate::CacheStatus::Ready);

        u(); // last subscriber leaves → dbl deactivates
             // RAM: compute node cleared its cache; ROM: state node kept its value.
        assert_eq!(dbl.cache(), None);
        assert_eq!(dbl.status().cache, crate::CacheStatus::Empty);
        assert_eq!(a.cache(), Some(7));
    }

    #[test]
    fn subscribe_to_ended_node_gets_start_last_data_and_terminal() {
        // R-terminals / R-subscription@3: an ended node stays subscribable; a late
        // subscriber gets START, the last DATA array and the terminal.
        let s = Node::<i32>::state(1);
        let _u = s.subscribe(|_| {});
        s.down(vec![Message::Complete]);
        assert_eq!(s.status().output, crate::OutputStatus::Completed);
        let (log, sink) = recorder();
        let _u2 = s.subscribe(sink);
        assert_eq!(*log.borrow(), vec!["START", "DATA", "COMPLETE"]);
    }

    #[test]
    #[should_panic(expected = "host input to an ended state is refused")]
    fn set_on_ended_state_is_refused() {
        // R-terminals: host input to an ended node is refused.
        let s = Node::<i32>::state(1);
        let (log, sink) = recorder();
        let _u = s.subscribe(sink);
        s.down(vec![Message::Complete]);
        assert_eq!(*log.borrow(), vec!["START", "DATA", "PENDING", "COMPLETE"]);
        s.set(5);
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
    fn private_state_replace_drops_captured_core_borrow_free() {
        // B49/D71 regression: replacing private state must drop the old value AFTER
        // releasing the GraphCore borrow. The old value may hold the last handle to
        // another node; dropping that handle re-enters Core::drop on the same arena.
        let a = Node::<i32>::state(1);
        let d: Node<i32> = Node::derived(vec![a.erased()], |ctx| {
            ctx.state_set(Node::<i32>::state(99));
            ctx.emit(*ctx.data::<i32>(0).unwrap());
        });
        let _u = d.subscribe(|_| {});

        a.set(2);
        assert_eq!(d.cache(), Some(2));
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
        let _u = source.subscribe(
            move |msgs: &[crate::protocol::Message<crate::protocol::AnyValue>]| {
                let handle = |m: &crate::protocol::Message<crate::protocol::AnyValue>| {
                    if matches!(m, Message::Data(_)) {
                        let _ = h.borrow_mut().take();
                    }
                };
                for m in msgs {
                    handle(m);
                }
            },
        );

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
            let p = Node::<i32>::producer_opts(with_cleanup(|| panic!("cleanup boom")), |ctx| {
                ctx.emit(1i32);
            });
            let _u = p.subscribe(|_| {});
        }));
        // R-failure@3: the cleanup's panic goes to the host reporter.
        assert!(result.is_ok());
        let free_after = DEFAULT_GRAPH_CORE.with(|g| g.borrow().free.len());
        assert!(
            free_after > free_before,
            "panicking cleanup still returns the arena slot to the free list"
        );
    }

    #[test]
    fn drop_unregisters_fn_even_when_cleanup_hook_panics() {
        // DR-8/B54 side-table cleanup regression: the call slot must unregister before
        // the lifetime cleanup runs, or a panicking cleanup strands the dispatcher
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
                let p = Node::<i32>::producer_opts(
                    with_cleanup(|| panic!("cleanup boom")),
                    move |ctx| {
                        let _keep = &guard;
                        ctx.emit(1i32);
                    },
                );
                let _u = p.subscribe(|_| {});
            }
        }));

        assert!(
            result.is_ok(),
            "the cleanup's panic goes to the host reporter"
        );
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
        old_weak.receive_from_dep(0, &[Message::Start]);
        assert_eq!(second.cache(), Some(2));
        assert_eq!(second.status().cache, crate::CacheStatus::Ready);
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
    fn same_array_data_then_complete_runs_once_then_completes() {
        // R-terminals (auto COMPLETE): the node runs once on the DATA its dependency sent
        // with COMPLETE in one array, then completes after that run (C-36).
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

        assert_eq!(runs.get(), 1, "the run sees the DATA before the COMPLETE");
        assert_eq!(derived.status().output, crate::OutputStatus::Completed);
    }

    #[test]
    fn sink_panic_does_not_abort_the_wave() {
        // R-failure@3: a panicking sink on the source neither aborts the fanout nor
        // leaves queued delivery work behind; the dependent still runs once.
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
        let _panic_sub = source.subscribe(
            |msgs: &[crate::protocol::Message<crate::protocol::AnyValue>]| {
                if msgs.iter().any(|m| matches!(m, Message::Data(_))) {
                    panic!("subscriber panics during the fanout");
                }
            },
        );

        source.set(1);

        assert_eq!(runs.get(), 1, "the dependent still runs once");
        DEFERRED_DELIVERY_ACTIONS.with(|queued| {
            assert!(
                queued.borrow().is_empty(),
                "no delivery work is left queued"
            );
        });
        source.set(2);
        assert_eq!(runs.get(), 2);
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
    fn passthrough_relays_every_data_of_an_array_in_order() {
        // R-wave: the kernel delivers every DATA unchanged and in order and never
        // merges or drops it, so a no-fn passthrough relays the whole array.
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
        let _u = wire.subscribe(
            move |msgs: &[crate::protocol::Message<crate::protocol::AnyValue>]| {
                let handle = |msg: &crate::protocol::Message<crate::protocol::AnyValue>| {
                    if let Message::Data(v) = msg {
                        seen_sink
                            .borrow_mut()
                            .push(*v.downcast_ref::<i32>().expect("wire emits i32"));
                    }
                };
                for msg in msgs {
                    handle(msg);
                }
            },
        );

        source.down(vec![
            Message::Data(Rc::new(1i32)),
            Message::Data(Rc::new(2i32)),
        ]);

        assert_eq!(
            seen.borrow().as_slice(),
            &[1, 2],
            "passthrough relays both occurrences"
        );
        assert_eq!(wire.cache(), Some(2));
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
        let _u = source.subscribe(
            move |msgs: &[crate::protocol::Message<crate::protocol::AnyValue>]| {
                let handle = |m: &crate::protocol::Message<crate::protocol::AnyValue>| {
                    if matches!(m, Message::Data(_)) {
                        seen.set(refs.get());
                    }
                };
                for m in msgs {
                    handle(m);
                }
            },
        );

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
        let _u = owner.subscribe(
            move |msgs: &[crate::protocol::Message<crate::protocol::AnyValue>]| {
                let handle = |m: &crate::protocol::Message<crate::protocol::AnyValue>| {
                    if matches!(m, Message::Invalidate) {
                        saw.set(true);
                        assert!(
                            !graph_for_sink.borrow().is_live_key(victim_key),
                            "touched pins must release before committed-boundary tasks run"
                        );
                    }
                };
                for m in msgs {
                    handle(m);
                }
            },
        );
        let owner_core = owner.core.clone();

        with_wave_owner(
            &owner.core,
            move || {
                wave_register(&victim.core);
                drop(victim);
                owner_core.request_down_next(vec![Message::Invalidate]);
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
                source.core.request_down_next(vec![Message::Invalidate]);
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
        old.core.request_down_next(vec![Message::Invalidate]);
        drop(old);

        let reused = Node::<i32>::state_in_arena(&arena, 2);
        assert_eq!(old_key.id, reused.core.key().id);
        assert_ne!(old_key.generation, reused.core.key().generation);

        drain_committed_boundary(&reused.core);

        assert_eq!(reused.status().cache, crate::CacheStatus::Ready);
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
        let _outer_sub = outer.subscribe(
            move |msgs: &[crate::protocol::Message<crate::protocol::AnyValue>]| {
                let handle = |m: &crate::protocol::Message<crate::protocol::AnyValue>| {
                    if matches!(m, Message::Data(_)) {
                        inner_src_for_outer.set(1);
                    }
                };
                for m in msgs {
                    handle(m);
                }
            },
        );

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
            source.core.request_down_next(vec![Message::Invalidate]);
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
    fn batch_deferred_subscribe_dep_returns_intended_index_while_queued() {
        let a = Node::<i32>::state(1);
        let b = Node::<i32>::state(2);
        let c = Node::<i32>::state(3);
        let d: Node<i32> = Node::derived(vec![a.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap())
        });
        let _u = d.subscribe(|_| {});

        crate::batch::batch(|_| {
            d.down(vec![Message::Data(Rc::new(5i32))]);
            let index = d.subscribe_dep(b.erased(), |ctx| {
                ctx.emit(*ctx.data::<i32>(1).unwrap() * 10)
            });
            assert_eq!(
                index, 1,
                "queued subscribe_dep returns the requested next-shape index"
            );
            let c_index = d.subscribe_dep(c.erased(), |ctx| {
                ctx.emit(*ctx.data::<i32>(2).unwrap() * 100)
            });
            assert_eq!(
                c_index, 2,
                "queued subscribe_dep composes with earlier queued external rewire intents"
            );
            assert_eq!(
                d.core.deps().len(),
                1,
                "D67 keeps the live dep shape old until batch commit"
            );
        });

        assert_eq!(
            d.cache(),
            Some(300),
            "old-shape batch commit precedes fresh subscribeDep boundary waves in FIFO order"
        );
        assert_eq!(
            d.core.deps().len(),
            3,
            "queued subscribeDep requests compose instead of replacing one another"
        );
    }

    #[test]
    fn batch_deferred_subscribe_then_unsubscribe_composes_while_queued() {
        let a = Node::<i32>::state(1);
        let b = Node::<i32>::state(2);
        let d: Node<i32> = Node::derived(vec![a.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap())
        });
        let _u = d.subscribe(|_| {});

        crate::batch::batch(|_| {
            d.down(vec![Message::Data(Rc::new(5i32))]);
            d.subscribe_dep(b.erased(), |ctx| {
                ctx.emit(*ctx.data::<i32>(1).unwrap() * 10)
            });
            d.unsubscribe_dep(b.erased(), |ctx| ctx.emit(*ctx.data::<i32>(0).unwrap()));
            assert_eq!(
                d.core.deps().len(),
                1,
                "D67 keeps the live dep shape old until batch commit"
            );
        });

        assert_eq!(
            d.core.deps().len(),
            1,
            "queued unsubscribeDep composes with an earlier queued subscribeDep"
        );
        let before_removed_dep_set = d.cache();
        b.set(9);
        assert_eq!(
            d.cache(),
            before_removed_dep_set,
            "removed queued dep no longer drives"
        );
        a.set(6);
        assert_eq!(
            d.cache(),
            Some(6),
            "original dep remains live after composed drain"
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
                req: RewireRequest::Set(vec![], Rc::new(|ctx| ctx.emit(999i32))),
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
            reused.status().cache,
            crate::CacheStatus::Ready,
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
        let _outer_sub = outer.subscribe(
            move |msgs: &[crate::protocol::Message<crate::protocol::AnyValue>]| {
                let handle = |m: &crate::protocol::Message<crate::protocol::AnyValue>| {
                    if matches!(m, Message::Data(_)) {
                        inner_src_for_commit.set(1);
                    }
                };
                for m in msgs {
                    handle(m);
                }
            },
        );

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
                            req: RewireRequest::Set(vec![a.erased()], Rc::new(|_| {})),
                            committed: committed.clone(),
                        },
                    );
                    defer_boundary(
                        &b.core,
                        BoundaryTask::ExternalRewire {
                            target: CoreToken::from_core(&b.core),
                            req: RewireRequest::Set(vec![], Rc::new(|ctx| ctx.emit(99i32))),
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
                        req: RewireRequest::Set(vec![a.erased()], Rc::new(|_| {})),
                        committed: committed.clone(),
                    },
                );
                defer_boundary(
                    &b.core,
                    BoundaryTask::ExternalRewire {
                        target: CoreToken::from_core(&b.core),
                        req: RewireRequest::Set(vec![], Rc::new(|ctx| ctx.emit(99i32))),
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
        let _u = derived.subscribe(
            move |msgs: &[crate::protocol::Message<crate::protocol::AnyValue>]| {
                let handle = |m: &crate::protocol::Message<crate::protocol::AnyValue>| {
                    l.borrow_mut().push(format!("{m:?}"));
                    if matches!(m, Message::Data(_)) {
                        let _ = h.borrow_mut().take();
                    }
                };
                for m in msgs {
                    handle(m);
                }
            },
        );

        source.set(1);

        // R-failure: a panic in the function is the node's ERROR, and what it sent
        // before the panic is dropped, so the subscriber sees no DATA.
        assert!(held.borrow().is_some());
        assert_eq!(derived.status().output, crate::OutputStatus::Errored);
        assert!(
            log.borrow().iter().any(|k| k == "ERROR"),
            "a panicking function ends its node with ERROR"
        );
        assert!(!log.borrow().iter().any(|k| k == "DATA"));
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
            reused.status().cache,
            crate::CacheStatus::Ready,
            "stale generation key must not ERROR/reset the reused slot occupant"
        );
        assert_eq!(
            owner.status().output,
            crate::OutputStatus::Errored,
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
        let _u = outer.subscribe(
            move |msgs: &[crate::protocol::Message<crate::protocol::AnyValue>]| {
                let handle = |m: &crate::protocol::Message<crate::protocol::AnyValue>| {
                    if matches!(m, Message::Data(_)) {
                        let _ = p.subscribe(|_| {});
                    }
                };
                for m in msgs {
                    handle(m);
                }
            },
        );

        outer.set(1);

        assert_eq!(
            outer.status().cache,
            crate::CacheStatus::Ready,
            "outer same-slot node must not receive the nested arena's recovery ERROR"
        );
        assert_eq!(
            panicker.status().output,
            crate::OutputStatus::Errored,
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
    fn rewire_removed_cleanup_panic_is_reported_and_releases_remaining_edges() {
        // R-failure@3: a removed dependency's cleanup panic goes to the host reporter;
        // the rewire completes, the later old edge is released and the node stays open.
        let b_deactivated = Rc::new(Cell::new(false));
        let a =
            Node::<i32>::producer_opts(with_cleanup(|| panic!("removed cleanup boom")), |ctx| {
                ctx.emit(1i32);
            });
        let b = {
            let flag = b_deactivated.clone();
            Node::<i32>::producer_opts(with_cleanup(move || flag.set(true)), |ctx| {
                ctx.emit(2i32);
            })
        };
        let c = Node::<i32>::state(10);
        let e = Node::<i32>::state(20);

        let d: Node<i32> = Node::derived(vec![a.erased(), b.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap() + *ctx.data::<i32>(1).unwrap());
        });
        let _u = d.subscribe(|_| {});
        assert_eq!(d.cache(), Some(3));
        assert_eq!(b.core.subscriber_count(), 1);

        d.replace_deps(vec![c.erased(), e.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap() + *ctx.data::<i32>(1).unwrap());
        });
        assert_eq!(d.status().output, crate::OutputStatus::Open);
        assert_eq!(d.cache(), Some(30));
        assert!(
            b_deactivated.get(),
            "the later old edge's cleanup still runs"
        );
        assert_eq!(
            b.core.subscriber_count(),
            0,
            "the later old edge is released"
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
        assert_eq!(*log.borrow(), vec!["PENDING", "DATA"]); // glitch-free two-phase
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
    fn rewire_adding_ended_dep_delivers_its_terminal() {
        // R-terminals: an ended node stays subscribable, so adding it as a dep is allowed;
        // under the auto COMPLETE policy the node ends once every dep has completed.
        let term: Node<i32> = Node::producer(|ctx| ctx.down(vec![Message::Complete]));
        let _ut = term.subscribe(|_| {});
        assert_eq!(term.status().output, crate::OutputStatus::Completed);
        let a = Node::<i32>::state(1);
        let d: Node<i32> = Node::derived(vec![a.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap())
        });
        let _u = d.subscribe(|_| {});
        d.subscribe_dep(term.erased(), |ctx| ctx.emit(*ctx.data::<i32>(0).unwrap()));
        assert_ne!(
            d.status().output,
            crate::OutputStatus::Completed,
            "a live dep keeps the node open"
        );
        a.down(vec![Message::Complete]);
        assert_eq!(d.status().output, crate::OutputStatus::Completed);
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
        assert_eq!(d.status().output, crate::OutputStatus::Errored);
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
                error: Policy::Manual,
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

        // R-failure: the panic in boom's first run is boom's ERROR; nothing unwinds into
        // the rewire, and boom stays d's (ended) dependency.
        assert_eq!(boom.status().output, crate::OutputStatus::Errored);
        assert_eq!(boom.core.subscriber_count(), 1);

        // d is NOT terminal; the QA-F1 guard cleared in_dep_mutation on the unwind so
        // d is not wedged.
        assert_ne!(
            d.status().output,
            crate::OutputStatus::Errored,
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
        assert_eq!(node.status().output, crate::OutputStatus::Errored);
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
