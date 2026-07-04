//! Graph-layer Rust API: graph-owned factories, topology, and inspection.
//!
//! This module is per-language product surface (D24/D32), not behavioral
//! conformance. It keeps naming/find/describe/observe on the graph side
//! (R-node-thin / D39) while substrate nodes remain arena-slot handles.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::rc::Rc;

use crate::batch::{batch as run_batch, BatchCtx};
use crate::checkpoint::GraphCheckpointJson;
use crate::ctx::{Ctx, DepRecord, DepTerminal, WaveData};
use crate::dispatcher::{default_dispatcher, Dispatcher, NodeFn};
use crate::environment::EnvironmentDrivers;
use crate::node::{Core, GraphArena, Node, NodeOpts, Status};
use crate::operators::Operator;
use crate::protocol::{AnyValue, LockId, Message, PullDemand, Tier};
use crate::versioning::{NodeVersion, NodeVersioningPolicy};

/// Graph construction options.
#[derive(Clone, Default)]
pub struct GraphOptions {
    /// `name` field for name.
    pub name: Option<String>,
    /// `profile` field for profile.
    pub profile: bool,
    /// `dispatcher` field for dispatcher.
    pub dispatcher: Option<Dispatcher>,
    /// `environment` field for environment.
    pub environment: EnvironmentDrivers,
    /// `versioning` field for versioning.
    pub versioning: Option<NodeVersioningPolicy>,
}

impl fmt::Debug for GraphOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphOptions")
            .field("name", &self.name)
            .field("profile", &self.profile)
            .field("dispatcher", &self.dispatcher)
            .field("environment", &self.environment)
            .field("versioning", &self.versioning)
            .finish()
    }
}

impl GraphOptions {
    /// Creates or computes `named`.
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            profile: false,
            dispatcher: None,
            environment: EnvironmentDrivers::default(),
            versioning: None,
        }
    }
}

/// Graph-layer node options: naming/metadata live here, behavioral knobs are
/// threaded through to the substrate [`NodeOpts`].
#[derive(Debug, Clone, Default)]
pub struct GraphNodeOpts {
    /// `name` field for name.
    pub name: Option<String>,
    /// `meta` field for meta.
    pub meta: BTreeMap<String, String>,
    /// `node` field for node.
    pub node: NodeOpts,
    /// `restore` field for restore.
    pub restore: Option<RestoreFactoryMeta>,
}

impl GraphNodeOpts {
    /// Creates or computes `named`.
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            ..Self::default()
        }
    }
}

/// Graph-owned topology/release group options (D152).
#[derive(Debug, Clone, Default)]
pub struct TopologyGroupOptions {
    /// `name` field for name.
    pub name: Option<String>,
}

impl TopologyGroupOptions {
    /// Creates or computes `named`.
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
/// `RestoreFactoryMeta` data container.
pub struct RestoreFactoryMeta {
    /// `ref_` field for ref.
    pub ref_: String,
    /// `config` field for config.
    pub config: Option<GraphCheckpointJson>,
    /// `config_version` field for config version.
    pub config_version: Option<GraphCheckpointJson>,
}

impl RestoreFactoryMeta {
    /// Creates or computes `registry_ref`.
    pub fn registry_ref(ref_: impl Into<String>) -> Self {
        Self {
            ref_: ref_.into(),
            config: None,
            config_version: None,
        }
    }

    /// Updates or reads `with_config`.
    pub fn with_config(mut self, config: GraphCheckpointJson) -> Self {
        self.config = Some(config);
        self
    }

    /// Updates or reads `with_config_version`.
    pub fn with_config_version(mut self, config_version: GraphCheckpointJson) -> Self {
        self.config_version = Some(config_version);
        self
    }
}

#[derive(Clone)]
struct Entry {
    core: Core,
    id: String,
    name: Option<String>,
    factory: String,
    meta: BTreeMap<String, String>,
    restore: Option<RestoreFactoryMeta>,
}

struct Mount {
    at: String,
    graph: Graph,
    topology_observer: RefCell<Option<GraphTopologyObserver>>,
}

#[derive(Clone)]
struct TopologyObserverEntry {
    path: Option<String>,
    sink: Rc<dyn Fn(TopologyEvent)>,
}

struct GraphInner {
    name: Option<String>,
    arena: GraphArena,
    dispatcher: Dispatcher,
    environment: EnvironmentDrivers,
    versioning: Option<NodeVersioningPolicy>,
    profile_enabled: Cell<bool>,
    entries: RefCell<Vec<Entry>>,
    by_id: RefCell<HashMap<String, Core>>,
    retired_ids: RefCell<HashSet<String>>,
    mounts: RefCell<Vec<Mount>>,
    seq: Cell<usize>,
    synth_seq: Cell<usize>,
    synth_ids: RefCell<HashMap<(usize, usize, u64), String>>,
    clock: Rc<Cell<u64>>,
    topology_observers: RefCell<Vec<Option<TopologyObserverEntry>>>,
    topology_observer_active: Cell<usize>,
    topology_observer_free: RefCell<Vec<usize>>,
    topology_delivering: Cell<bool>,
    topology_queue: RefCell<VecDeque<TopologyEvent>>,
}

/// A single-thread graph/concurrency-domain product surface (D22/B53).
#[derive(Clone)]
pub struct Graph {
    inner: Rc<GraphInner>,
}

struct TopologyGroupInner {
    graph: Graph,
    name: Option<String>,
    members: RefCell<Vec<Core>>,
    released: Cell<bool>,
}

/// D152 graph-owned release group over ordinary graph-registered nodes.
///
/// The group is a label/release handle, not a registry: the graph registry remains
/// the source of truth for describe/find/profile/checkpoint and id retirement.
#[derive(Clone)]
pub struct TopologyGroup {
    inner: Rc<TopologyGroupInner>,
}

impl TopologyGroup {
    fn new(graph: Graph, opts: TopologyGroupOptions) -> Self {
        Self {
            inner: Rc::new(TopologyGroupInner {
                graph,
                name: opts.name,
                members: RefCell::new(Vec::new()),
                released: Cell::new(false),
            }),
        }
    }

    /// Updates or reads `name`.
    pub fn name(&self) -> Option<&str> {
        self.inner.name.as_deref()
    }

    /// Updates or reads `is_released`.
    pub fn is_released(&self) -> bool {
        self.inner.released.get()
    }

    /// Updates or reads `add`.
    pub fn add<T: 'static>(&self, node: &Node<T>) -> Node<T> {
        self.assert_live();
        self.inner.graph.assert_registered_core(
            &node.erased(),
            &format!("topology group '{}' member", self.name().unwrap_or("group")),
        );
        self.track(node.erased());
        node.clone()
    }

    /// Updates or reads `node`.
    pub fn node<T: 'static, F: Fn(&Ctx) + 'static>(&self, deps: Vec<Core>, f: F) -> Node<T> {
        self.node_opts(deps, f, GraphNodeOpts::default())
    }

    /// Updates or reads `node_opts`.
    pub fn node_opts<T: 'static, F: Fn(&Ctx) + 'static>(
        &self,
        deps: Vec<Core>,
        f: F,
        opts: GraphNodeOpts,
    ) -> Node<T> {
        self.assert_live();
        self.track_node(self.inner.graph.node_opts(deps, f, opts))
    }

    /// Updates or reads `state`.
    pub fn state<T: 'static>(&self, initial: T) -> Node<T> {
        self.state_opts(initial, GraphNodeOpts::default())
    }

    /// Updates or reads `state_opts`.
    pub fn state_opts<T: 'static>(&self, initial: T, opts: GraphNodeOpts) -> Node<T> {
        self.assert_live();
        self.track_node(self.inner.graph.state_opts(initial, opts))
    }

    /// Updates or reads `state_empty`.
    pub fn state_empty<T: 'static>(&self) -> Node<T> {
        self.state_empty_opts(GraphNodeOpts::default())
    }

    /// Updates or reads `state_empty_opts`.
    pub fn state_empty_opts<T: 'static>(&self, opts: GraphNodeOpts) -> Node<T> {
        self.assert_live();
        self.track_node(self.inner.graph.state_empty_opts(opts))
    }

    /// Updates or reads `producer`.
    pub fn producer<T: 'static, F: Fn(&Ctx) + 'static>(&self, f: F) -> Node<T> {
        self.producer_opts(f, GraphNodeOpts::default())
    }

    /// Updates or reads `producer_opts`.
    pub fn producer_opts<T: 'static, F: Fn(&Ctx) + 'static>(
        &self,
        f: F,
        opts: GraphNodeOpts,
    ) -> Node<T> {
        self.assert_live();
        self.track_node(self.inner.graph.producer_opts(f, opts))
    }

    /// Updates or reads `derived`.
    pub fn derived<T: 'static, F: Fn(&Values<'_>) -> Option<T> + 'static>(
        &self,
        deps: Vec<Core>,
        f: F,
    ) -> Node<T> {
        self.derived_opts(deps, f, GraphNodeOpts::default())
    }

    /// Updates or reads `derived_opts`.
    pub fn derived_opts<T: 'static, F: Fn(&Values<'_>) -> Option<T> + 'static>(
        &self,
        deps: Vec<Core>,
        f: F,
        opts: GraphNodeOpts,
    ) -> Node<T> {
        self.assert_live();
        self.track_node(self.inner.graph.derived_opts(deps, f, opts))
    }

    /// Updates or reads `effect`.
    pub fn effect<F: Fn(&Values<'_>) -> Option<Box<dyn FnOnce()>> + 'static>(
        &self,
        deps: Vec<Core>,
        f: F,
    ) -> Node<()> {
        self.effect_opts(deps, f, GraphNodeOpts::default())
    }

    /// Updates or reads `effect_opts`.
    pub fn effect_opts<F: Fn(&Values<'_>) -> Option<Box<dyn FnOnce()>> + 'static>(
        &self,
        deps: Vec<Core>,
        f: F,
        opts: GraphNodeOpts,
    ) -> Node<()> {
        self.assert_live();
        self.track_node(self.inner.graph.effect_opts(deps, f, opts))
    }

    /// Updates or reads `init_node`.
    pub fn init_node<T: 'static>(
        &self,
        op: Operator<T>,
        deps: Vec<Core>,
        opts: GraphNodeOpts,
    ) -> Node<T> {
        self.assert_live();
        self.track_node(self.inner.graph.init_node(op, deps, opts))
    }

    /// Updates or reads `release`.
    pub fn release(&self) {
        let reason = self
            .inner
            .name
            .clone()
            .unwrap_or_else(|| "topology group".to_owned());
        self.release_with_reason(&reason);
    }

    /// Updates or reads `release_with_reason`.
    pub fn release_with_reason(&self, reason: &str) {
        if self.inner.released.get() {
            return;
        }
        let members = self.inner.members.borrow().clone();
        self.inner.graph.release_nodes(&members, reason);
        self.inner.members.borrow_mut().clear();
        self.inner.released.set(true);
    }

    fn track_node<T: 'static>(&self, node: Node<T>) -> Node<T> {
        self.assert_live();
        self.track(node.erased());
        node
    }

    fn track(&self, core: Core) {
        let mut members = self.inner.members.borrow_mut();
        if !members.iter().any(|member| member.ptr_eq(&core)) {
            members.push(core);
        }
    }

    fn assert_live(&self) {
        assert!(
            !self.inner.released.get(),
            "topology group '{}' has been released (D152)",
            self.name().unwrap_or("group")
        );
    }
}

impl Graph {
    /// Creates or computes `new`.
    pub fn new(opts: GraphOptions) -> Self {
        let dispatcher = opts.dispatcher.unwrap_or_else(default_dispatcher);
        let mut environment = opts.environment;
        if environment.local_async_driver().is_none() {
            environment.set_local_async_driver(dispatcher.local_async_driver());
        }
        if opts.profile {
            dispatcher.set_recording(true);
        }
        Self {
            inner: Rc::new(GraphInner {
                name: opts.name,
                arena: GraphArena::new(),
                dispatcher,
                environment,
                versioning: opts.versioning,
                profile_enabled: Cell::new(opts.profile),
                entries: RefCell::new(Vec::new()),
                by_id: RefCell::new(HashMap::new()),
                retired_ids: RefCell::new(HashSet::new()),
                mounts: RefCell::new(Vec::new()),
                seq: Cell::new(0),
                synth_seq: Cell::new(0),
                synth_ids: RefCell::new(HashMap::new()),
                clock: Rc::new(Cell::new(0)),
                topology_observers: RefCell::new(Vec::new()),
                topology_observer_active: Cell::new(0),
                topology_observer_free: RefCell::new(Vec::new()),
                topology_delivering: Cell::new(false),
                topology_queue: RefCell::new(VecDeque::new()),
            }),
        }
    }

    /// Updates or reads `name`.
    pub fn name(&self) -> Option<&str> {
        self.inner.name.as_deref()
    }

    /// D152 graph-owned topology/release group. Members are ordinary registered
    /// graph nodes and release is quiescent-only; no protocol messages are synthesized.
    pub fn topology_group(&self) -> TopologyGroup {
        self.topology_group_opts(TopologyGroupOptions::default())
    }

    /// Updates or reads `topology_group_opts`.
    pub fn topology_group_opts(&self, opts: TopologyGroupOptions) -> TopologyGroup {
        TopologyGroup::new(self.clone(), opts)
    }

    /// Look up a registered graph node by stable id.
    pub fn find(&self, id: &str) -> Option<GraphNode> {
        self.inner
            .by_id
            .borrow()
            .get(id)
            .cloned()
            .map(GraphNode::new)
    }

    /// ctx-level power surface: the raw `(ctx)=>void` node factory.
    pub fn node<T: 'static, F: Fn(&Ctx) + 'static>(&self, deps: Vec<Core>, f: F) -> Node<T> {
        self.node_opts(deps, f, GraphNodeOpts::default())
    }

    /// Updates or reads `node_opts`.
    pub fn node_opts<T: 'static, F: Fn(&Ctx) + 'static>(
        &self,
        deps: Vec<Core>,
        f: F,
        opts: GraphNodeOpts,
    ) -> Node<T> {
        self.node_opts_initial(deps, f, opts, None)
    }

    pub(crate) fn node_opts_initial<T: 'static, F: Fn(&Ctx) + 'static>(
        &self,
        deps: Vec<Core>,
        f: F,
        opts: GraphNodeOpts,
        initial: Option<T>,
    ) -> Node<T> {
        self.assert_graph_local_deps(&deps, opts.name.as_deref().unwrap_or("node"));
        let node = Node::derived_opts_initial_in_arena_with_dispatcher(
            &self.inner.arena,
            self.inner.dispatcher.clone(),
            deps,
            self.apply_default_node_opts(opts.node.clone()),
            initial,
            f,
        );
        self.add(node, "node", opts)
    }

    /// Graph-owned state node with an initial value.
    pub fn state<T: 'static>(&self, initial: T) -> Node<T> {
        self.state_opts(initial, GraphNodeOpts::default())
    }

    /// Updates or reads `state_opts`.
    pub fn state_opts<T: 'static>(&self, initial: T, opts: GraphNodeOpts) -> Node<T> {
        let node = Node::state_opts_in_arena_with_dispatcher(
            &self.inner.arena,
            self.inner.dispatcher.clone(),
            initial,
            self.apply_default_node_opts(opts.node.clone()),
        );
        self.add(node, "state", opts)
    }

    /// Graph-owned SENTINEL state node.
    pub fn state_empty<T: 'static>(&self) -> Node<T> {
        self.state_empty_opts(GraphNodeOpts::default())
    }

    /// Updates or reads `state_empty_opts`.
    pub fn state_empty_opts<T: 'static>(&self, opts: GraphNodeOpts) -> Node<T> {
        let node = Node::state_empty_opts_in_arena_with_dispatcher(
            &self.inner.arena,
            self.inner.dispatcher.clone(),
            self.apply_default_node_opts(opts.node.clone()),
        );
        self.add(node, "state", opts)
    }

    /// ctx-level depless source; runs once on activation.
    pub fn producer<T: 'static, F: Fn(&Ctx) + 'static>(&self, f: F) -> Node<T> {
        self.producer_opts(f, GraphNodeOpts::default())
    }

    /// Updates or reads `producer_opts`.
    pub fn producer_opts<T: 'static, F: Fn(&Ctx) + 'static>(
        &self,
        f: F,
        opts: GraphNodeOpts,
    ) -> Node<T> {
        let node = Node::producer_opts_in_arena_with_dispatcher(
            &self.inner.arena,
            self.inner.dispatcher.clone(),
            self.apply_default_node_opts(opts.node.clone()),
            f,
        );
        self.add(node, "producer", opts)
    }

    /// Value-level derived sugar: return `Some(value)` to emit DATA, `None` for
    /// a no-emit/RESOLVED settle. Reads dep values through [`Values`], not raw `Ctx`.
    pub fn derived<T: 'static, F: Fn(&Values<'_>) -> Option<T> + 'static>(
        &self,
        deps: Vec<Core>,
        f: F,
    ) -> Node<T> {
        self.derived_opts(deps, f, GraphNodeOpts::default())
    }

    /// Updates or reads `derived_opts`.
    pub fn derived_opts<T: 'static, F: Fn(&Values<'_>) -> Option<T> + 'static>(
        &self,
        deps: Vec<Core>,
        f: F,
        opts: GraphNodeOpts,
    ) -> Node<T> {
        self.assert_graph_local_deps(&deps, opts.name.as_deref().unwrap_or("derived"));
        let node = Node::derived_opts_in_arena_with_dispatcher(
            &self.inner.arena,
            self.inner.dispatcher.clone(),
            deps,
            self.apply_default_node_opts(opts.node.clone()),
            move |ctx| {
                let values = Values::new(ctx.dep_records());
                if let Some(value) = f(&values) {
                    ctx.emit(value);
                }
            },
        );
        self.add(node, "derived", opts)
    }

    /// Value-level sink sugar. A returned cleanup is registered as onDeactivation.
    pub fn effect<F: Fn(&Values<'_>) -> Option<Box<dyn FnOnce()>> + 'static>(
        &self,
        deps: Vec<Core>,
        f: F,
    ) -> Node<()> {
        self.effect_opts(deps, f, GraphNodeOpts::default())
    }

    /// Updates or reads `effect_opts`.
    pub fn effect_opts<F: Fn(&Values<'_>) -> Option<Box<dyn FnOnce()>> + 'static>(
        &self,
        deps: Vec<Core>,
        f: F,
        opts: GraphNodeOpts,
    ) -> Node<()> {
        self.assert_graph_local_deps(&deps, opts.name.as_deref().unwrap_or("effect"));
        let node = Node::derived_opts_in_arena_with_dispatcher(
            &self.inner.arena,
            self.inner.dispatcher.clone(),
            deps,
            self.apply_default_node_opts(opts.node.clone()),
            move |ctx| {
                let values = Values::new(ctx.dep_records());
                if let Some(cleanup) = f(&values) {
                    ctx.on_deactivation(cleanup);
                }
            },
        );
        self.add(node, "effect", opts)
    }

    /// Declarative batch (D12): success commits, rollback/panic discards.
    pub fn batch<R>(&self, f: impl FnOnce(&BatchCtx) -> R) -> R {
        run_batch(f)
    }

    /// Instantiate a free-standing operator/source as a registered graph node (D43/D40).
    pub fn init_node<T: 'static>(
        &self,
        op: Operator<T>,
        deps: Vec<Core>,
        opts: GraphNodeOpts,
    ) -> Node<T> {
        self.assert_graph_local_deps(&deps, opts.name.as_deref().unwrap_or(op.factory));
        let node = crate::operators::init_node_in_arena_with_dispatcher(
            op.clone(),
            &self.inner.arena,
            self.inner.dispatcher.clone(),
            deps,
            self.apply_default_node_opts(opts.node.clone()),
        );
        self.add(node, op.factory, opts)
    }

    /// Mount a child graph under a stable `::` path prefix.
    pub fn mount(&self, child: Graph, at: impl Into<String>) {
        let at = at.into();
        assert!(!at.is_empty(), "mount path must not be empty");
        assert!(
            !child.contains_graph(self),
            "mount would create a graph cycle"
        );
        assert!(
            !self.inner.mounts.borrow().iter().any(|m| m.at == at),
            "duplicate mount path '{at}'"
        );
        if self.inner.profile_enabled.get() {
            child.enable_profile_recorder_recursive();
        }
        self.inner.mounts.borrow_mut().push(Mount {
            at: at.clone(),
            graph: child,
            topology_observer: RefCell::new(None),
        });
        if self.has_topology_observers() {
            self.ensure_mounted_topology_forwarders();
        }
        self.emit_topology_mount_changed(at);
    }

    /// Static point-in-time inspection snapshot (D39 first cut).
    pub fn describe(&self) -> DescribeSnapshot {
        self.describe_opts(DescribeOpts::default())
    }

    /// Updates or reads `describe_opts`.
    pub fn describe_opts(&self, opts: DescribeOpts) -> DescribeSnapshot {
        let snap = self.describe_with_prefix("");
        if let Some(explain) = opts.explain {
            explain_subset(snap, &explain)
        } else {
            snap
        }
    }

    /// Read-only observation egress for every currently registered node in this graph.
    pub fn observe(&self) -> ObserveStream {
        ObserveStream {
            graph: self.clone(),
            path: None,
        }
    }

    /// Read-only observation egress for a single registered id or a `::` subtree prefix.
    pub fn observe_path(&self, path: &str) -> ObserveStream {
        ObserveStream {
            graph: self.clone(),
            path: Some(path.to_owned()),
        }
    }

    /// D145 read-only topology lifecycle egress over the existing graph registry.
    ///
    /// This is not a graph node, does not subscribe to nodes, and does not publish DATA.
    pub fn observe_topology(&self) -> TopologyStream {
        TopologyStream {
            graph: self.clone(),
            path: None,
        }
    }

    /// Read-only topology lifecycle egress for one registered id or `::` subtree prefix.
    pub fn observe_topology_path(&self, path: &str) -> TopologyStream {
        TopologyStream {
            graph: self.clone(),
            path: Some(path.to_owned()),
        }
    }

    /// Opt-in accumulated-counter snapshot (D39/R-profile).
    pub fn profile(&self) -> Profile {
        let mut total_invokes = 0;
        let mut nodes = BTreeMap::new();
        self.collect_profile("", &mut nodes, &mut total_invokes);
        Profile {
            total_invokes,
            nodes,
        }
    }

    fn collect_profile(
        &self,
        prefix: &str,
        nodes: &mut BTreeMap<String, NodeProfile>,
        total_invokes: &mut u64,
    ) {
        for entry in self.inner.entries.borrow().iter() {
            let stat = entry
                .core
                .handle()
                .and_then(|handle| self.inner.dispatcher.stat_for(handle));
            let invokes = stat.map_or(0, |s| s.invokes);
            *total_invokes += invokes;
            nodes.insert(
                format!("{prefix}{}", entry.id),
                NodeProfile {
                    invokes,
                    total_duration_ns: stat.map_or(0, |s| s.total_duration_ns),
                    last_duration_ns: stat.map_or(0, |s| s.last_duration_ns),
                    status: entry.core.status(),
                },
            );
        }
        for mount in self.inner.mounts.borrow().iter() {
            mount
                .graph
                .collect_profile(&format!("{prefix}{}::", mount.at), nodes, total_invokes);
        }
    }

    fn enable_profile_recorder_recursive(&self) {
        self.inner.dispatcher.set_recording(true);
        self.inner.profile_enabled.set(true);
        for mount in self.inner.mounts.borrow().iter() {
            mount.graph.enable_profile_recorder_recursive();
        }
    }

    pub(crate) fn checkpoint_entries(&self) -> Vec<crate::checkpoint::CheckpointEntry> {
        self.inner
            .entries
            .borrow()
            .iter()
            .map(|entry| crate::checkpoint::CheckpointEntry {
                id: entry.id.clone(),
                name: entry.name.clone(),
                factory: entry.factory.clone(),
                meta: entry.meta.clone(),
                restore: entry.restore.clone(),
                core: entry.core.clone(),
                unregistered: false,
            })
            .collect()
    }

    pub(crate) fn checkpoint_mounts(&self) -> Vec<(String, Graph)> {
        self.inner
            .mounts
            .borrow()
            .iter()
            .map(|mount| (mount.at.clone(), mount.graph.clone()))
            .collect()
    }

    pub(crate) fn checkpoint_synthetic_id_for_core(&self, core: &Core) -> String {
        self.synthetic_id_for_core(core, "")
    }

    pub(crate) fn contains_core(&self, core: &Core) -> bool {
        core.same_graph_arena(&self.inner.arena)
            && self
                .inner
                .entries
                .borrow()
                .iter()
                .any(|entry| entry.core.ptr_eq(core))
    }

    pub(crate) fn restore_state_json_with_id(
        &self,
        id: String,
        opts: GraphNodeOpts,
    ) -> Node<GraphCheckpointJson> {
        let node = Node::state_empty_opts_in_arena_with_dispatcher(
            &self.inner.arena,
            self.inner.dispatcher.clone(),
            self.apply_default_node_opts(opts.node.clone()),
        );
        self.add_with_id(node, "state", id, opts)
    }

    pub(crate) fn restore_node_with_id(
        &self,
        id: String,
        factory: String,
        deps: Vec<Core>,
        f: NodeFn,
        opts: GraphNodeOpts,
    ) -> Node<GraphCheckpointJson> {
        self.assert_graph_local_deps(&deps, &id);
        let node = Node::derived_opts_in_arena_with_dispatcher(
            &self.inner.arena,
            self.inner.dispatcher.clone(),
            deps,
            self.apply_default_node_opts(opts.node.clone()),
            move |ctx| f(ctx),
        );
        self.add_with_id(node, &factory, id, opts)
    }

    pub(crate) fn mount_restored(&self, child: Graph, at: String) {
        self.mount(child, at);
    }

    fn add<T: 'static>(&self, node: Node<T>, factory: &str, opts: GraphNodeOpts) -> Node<T> {
        let id = opts.name.clone().unwrap_or_else(|| {
            let next = self.inner.seq.get();
            self.inner.seq.set(next + 1);
            format!("{factory}#{next}")
        });
        self.add_with_id(node, factory, id, opts)
    }

    fn apply_default_node_opts(&self, mut opts: NodeOpts) -> NodeOpts {
        if opts.versioning.is_none() {
            opts.versioning = self.inner.versioning.clone();
        }
        opts
    }

    pub(crate) fn empty_source<T: 'static>(&self, factory: &str, opts: GraphNodeOpts) -> Node<T> {
        let node = Node::state_empty_opts_in_arena_with_dispatcher(
            &self.inner.arena,
            self.inner.dispatcher.clone(),
            self.apply_default_node_opts(opts.node.clone()),
        );
        self.add(node, factory, opts)
    }

    fn add_with_id<T: 'static>(
        &self,
        node: Node<T>,
        factory: &str,
        id: String,
        opts: GraphNodeOpts,
    ) -> Node<T> {
        assert!(
            node.erased().same_graph_arena(&self.inner.arena),
            "graph node belongs to a different graph arena"
        );
        let core = node.erased();
        core.set_environment(self.inner.environment.clone());
        assert!(
            !self.inner.by_id.borrow().contains_key(&id),
            "duplicate graph node id '{id}'"
        );
        assert!(
            !self.inner.retired_ids.borrow().contains(&id),
            "graph node id '{id}' has been released from this graph lifecycle (D122)"
        );
        let entry = Entry {
            core: core.clone(),
            id: id.clone(),
            name: opts.name,
            factory: factory.to_owned(),
            meta: opts.meta,
            restore: opts.restore,
        };
        let weak_inner = Rc::downgrade(&self.inner);
        let topology_id = id.clone();
        core.set_topology_deps_changed_observer(Rc::new(move |old_deps, new_deps| {
            if let Some(inner) = weak_inner.upgrade() {
                Graph { inner }.emit_topology_deps_changed(&topology_id, old_deps, new_deps);
            }
        }));
        self.inner.by_id.borrow_mut().insert(id.clone(), core);
        self.inner.entries.borrow_mut().push(entry);
        if let Some(n) = id
            .rsplit_once('#')
            .and_then(|(_, n)| n.parse::<usize>().ok())
        {
            self.inner.seq.set(self.inner.seq.get().max(n + 1));
        }
        self.emit_topology_node_registered(&id);
        node
    }

    pub(crate) fn release_nodes(&self, nodes: &[Core], reason: &str) {
        let mut release_entries: Vec<(String, Core)> = Vec::new();
        for node in nodes {
            if release_entries.iter().any(|(_, core)| core.ptr_eq(node)) {
                continue;
            }
            let found = self
                .inner
                .entries
                .borrow()
                .iter()
                .find(|entry| entry.core.ptr_eq(node))
                .map(|entry| (entry.id.clone(), entry.core.clone()));
            if let Some(entry) = found {
                release_entries.push(entry);
            }
        }
        if release_entries.is_empty() {
            return;
        }
        let release_ids = release_entries
            .iter()
            .map(|(id, _)| id.clone())
            .collect::<HashSet<_>>();
        let release_cores = release_entries
            .iter()
            .map(|(_, core)| core.clone())
            .collect::<Vec<_>>();
        for entry in self.inner.entries.borrow().iter() {
            if release_ids.contains(&entry.id) {
                continue;
            }
            for dep in entry.core.deps() {
                if let Some((dep_id, _)) = release_entries
                    .iter()
                    .find(|(_, released)| released.ptr_eq(&dep))
                {
                    panic!(
                        "graph: cannot release node group for {reason}; '{}' still depends on '{}' (D122)",
                        entry.id, dep_id
                    );
                }
            }
        }
        for (id, core) in &release_entries {
            assert!(
                core.runtime_is_quiescent_for_release(),
                "graph: cannot release node group for {reason}; '{id}' is not runtime-quiescent (D124)"
            );
            let internal_subscribers = release_cores
                .iter()
                .filter(|dependent| dependent.is_active())
                .flat_map(|dependent| dependent.deps())
                .filter(|dep| dep.ptr_eq(core))
                .count();
            assert!(
                core.external_subscriber_count_for_release() <= internal_subscribers,
                "graph: cannot release node group for {reason}; '{id}' still has live subscribers (D124)"
            );
        }
        let release_events = if self.has_topology_observers() {
            release_entries
                .iter()
                .filter_map(|(id, core)| {
                    self.inner
                        .entries
                        .borrow()
                        .iter()
                        .find(|entry| entry.id == *id && entry.core.ptr_eq(core))
                        .map(|entry| {
                            (
                                entry.id.clone(),
                                entry.factory.clone(),
                                self.topology_deps(&entry.core.deps()),
                            )
                        })
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        {
            let mut entries = self.inner.entries.borrow_mut();
            entries.retain(|entry| !release_ids.contains(&entry.id));
        }
        {
            let mut by_id = self.inner.by_id.borrow_mut();
            let mut retired = self.inner.retired_ids.borrow_mut();
            for id in &release_ids {
                by_id.remove(id);
                retired.insert(id.clone());
            }
        }
        for core in &release_cores {
            core.detach_graph_observer_subscribers_for_release();
        }
        let mut pending = release_cores.iter().rev().cloned().collect::<Vec<_>>();
        let mut first_panic = None;
        while !pending.is_empty() {
            let before = pending.len();
            let mut index = 0;
            while index < pending.len() {
                if pending[index].subscriber_count() != 0 {
                    index += 1;
                    continue;
                }
                let core = pending.remove(index);
                let result = catch_unwind(AssertUnwindSafe(|| {
                    assert!(
                        core.release_runtime_for_graph(),
                        "graph: cannot release node group for {reason}; runtime became non-quiescent (D124)"
                    );
                }));
                if let Err(panic) = result {
                    if first_panic.is_none() {
                        first_panic = Some(panic);
                    }
                }
            }
            if pending.len() == before {
                break;
            }
        }
        if pending.is_empty() {
            for (id, factory, deps) in release_events {
                self.emit_topology_node_released(id, factory, deps);
            }
        }
        if let Some(panic) = first_panic {
            resume_unwind(panic);
        }
        assert!(
            pending.is_empty(),
            "graph: cannot release node group for {reason}; internal release order did not quiesce (D124)"
        );
    }

    pub(crate) fn retain<T: 'static>(&self, node: &Node<T>, reason: &str) -> Box<dyn FnOnce()> {
        assert!(
            node.erased().same_graph_arena(&self.inner.arena),
            "graph: cannot retain node for {reason}; node belongs to a different graph"
        );
        node.subscribe(|_| {})
    }

    fn assert_graph_local_deps(&self, deps: &[Core], label: &str) {
        for dep in deps {
            assert!(
                dep.same_graph_arena(&self.inner.arena),
                "{label} dep belongs to a different graph; cross-graph deps require a wire bridge"
            );
        }
    }

    fn assert_registered_core(&self, core: &Core, label: &str) {
        assert!(
            core.same_graph_arena(&self.inner.arena),
            "{label} belongs to a different graph; cross-graph deps require a wire bridge"
        );
        assert!(
            self.inner
                .entries
                .borrow()
                .iter()
                .any(|entry| entry.core.ptr_eq(core)),
            "{label} is not a registered graph node (D152)"
        );
    }

    fn registered_id_for_core(&self, core: &Core, prefix: &str) -> Option<String> {
        self.inner
            .entries
            .borrow()
            .iter()
            .find(|entry| entry.core.ptr_eq(core))
            .map(|entry| format!("{prefix}{}", entry.id))
    }

    fn synthetic_id_for_core(&self, core: &Core, prefix: &str) -> String {
        let key = core.identity_key();
        let id = {
            let mut synth = self.inner.synth_ids.borrow_mut();
            if let Some(id) = synth.get(&key) {
                id.clone()
            } else {
                let next = self.inner.synth_seq.get();
                self.inner.synth_seq.set(next + 1);
                let id = format!(
                    "~{}#{next}",
                    core.factory().unwrap_or_else(|| "?".to_owned())
                );
                synth.insert(key, id.clone());
                id
            }
        };
        format!("{prefix}{id}")
    }

    fn id_for_core(&self, core: &Core, prefix: &str) -> String {
        self.registered_id_for_core(core, prefix)
            .unwrap_or_else(|| self.synthetic_id_for_core(core, prefix))
    }

    fn topology_deps(&self, deps: &[Core]) -> Vec<String> {
        deps.iter().map(|dep| self.id_for_core(dep, "")).collect()
    }

    fn has_topology_observers(&self) -> bool {
        self.inner.topology_observer_active.get() > 0
    }

    fn emit_topology_node_registered(&self, id: &str) {
        if !self.has_topology_observers() {
            return;
        }
        let Some(entry) = self
            .inner
            .entries
            .borrow()
            .iter()
            .find(|entry| entry.id == id)
            .cloned()
        else {
            return;
        };
        let seq = self.inner.clock.get();
        self.inner.clock.set(seq + 1);
        self.emit_topology_event(TopologyEvent {
            kind: TopologyEventKind::NodeRegistered,
            path: entry.id,
            deps: self.topology_deps(&entry.core.deps()),
            prev_deps: None,
            factory: Some(entry.factory),
            seq,
        });
    }

    fn emit_topology_deps_changed(&self, id: &str, old_deps: &[Core], new_deps: &[Core]) {
        if !self.has_topology_observers() {
            return;
        }
        if self.find(id).is_none() {
            return;
        }
        let seq = self.inner.clock.get();
        self.inner.clock.set(seq + 1);
        self.emit_topology_event(TopologyEvent {
            kind: TopologyEventKind::DepsChanged,
            path: id.to_owned(),
            deps: self.topology_deps(new_deps),
            prev_deps: Some(self.topology_deps(old_deps)),
            factory: None,
            seq,
        });
    }

    fn emit_topology_node_released(&self, path: String, factory: String, deps: Vec<String>) {
        if !self.has_topology_observers() {
            return;
        }
        let seq = self.inner.clock.get();
        self.inner.clock.set(seq + 1);
        self.emit_topology_event(TopologyEvent {
            kind: TopologyEventKind::NodeReleased,
            path,
            deps,
            prev_deps: None,
            factory: Some(factory),
            seq,
        });
    }

    fn emit_topology_mount_changed(&self, path: String) {
        if !self.has_topology_observers() {
            return;
        }
        let seq = self.inner.clock.get();
        self.inner.clock.set(seq + 1);
        self.emit_topology_event(TopologyEvent {
            kind: TopologyEventKind::MountChanged,
            path,
            deps: Vec::new(),
            prev_deps: None,
            factory: Some("mount".to_owned()),
            seq,
        });
    }

    fn ensure_mounted_topology_forwarders(&self) {
        if !self.has_topology_observers() {
            return;
        }
        let parent = Rc::downgrade(&self.inner);
        for mount in self.inner.mounts.borrow().iter() {
            if mount.topology_observer.borrow().is_some() {
                continue;
            }
            let parent = parent.clone();
            let mount_path = mount.at.clone();
            let observer = mount.graph.observe_topology().subscribe(move |event| {
                if let Some(inner) = parent.upgrade() {
                    Graph { inner }.emit_mounted_topology_event(&mount_path, event);
                }
            });
            *mount.topology_observer.borrow_mut() = Some(observer);
        }
    }

    fn release_mounted_topology_forwarders(&self) {
        for mount in self.inner.mounts.borrow().iter() {
            mount.topology_observer.borrow_mut().take();
        }
    }

    fn emit_mounted_topology_event(&self, mount_path: &str, event: TopologyEvent) {
        if !self.has_topology_observers() {
            return;
        }
        let seq = self.inner.clock.get();
        self.inner.clock.set(seq + 1);
        self.emit_topology_event(TopologyEvent {
            kind: event.kind,
            path: prefix_topology_path(mount_path, &event.path),
            deps: event
                .deps
                .iter()
                .map(|dep| prefix_topology_path(mount_path, dep))
                .collect(),
            prev_deps: event.prev_deps.map(|deps| {
                deps.iter()
                    .map(|dep| prefix_topology_path(mount_path, dep))
                    .collect()
            }),
            factory: event.factory,
            seq,
        });
    }

    fn emit_topology_event(&self, event: TopologyEvent) {
        if self.inner.topology_delivering.get() {
            self.inner.topology_queue.borrow_mut().push_back(event);
            return;
        }
        self.inner.topology_delivering.set(true);
        let mut current = Some(event);
        while let Some(event) = current {
            let observers = self
                .inner
                .topology_observers
                .borrow()
                .iter()
                .enumerate()
                .filter_map(|(id, entry)| entry.clone().map(|entry| (id, entry)))
                .collect::<Vec<_>>();
            for (id, observer) in observers {
                let still_active = self
                    .inner
                    .topology_observers
                    .borrow()
                    .get(id)
                    .and_then(Option::as_ref)
                    .is_some_and(|current| Rc::ptr_eq(&current.sink, &observer.sink));
                if !still_active {
                    continue;
                }
                if observer
                    .path
                    .as_deref()
                    .is_some_and(|path| !topology_path_matches(&event.path, path))
                {
                    continue;
                }
                let _ = catch_unwind(AssertUnwindSafe(|| (observer.sink)(event.clone())));
            }
            current = self.inner.topology_queue.borrow_mut().pop_front();
        }
        self.inner.topology_delivering.set(false);
    }

    fn describe_with_prefix(&self, prefix: &str) -> DescribeSnapshot {
        let entries = self.inner.entries.borrow().clone();
        let mut nodes = Vec::with_capacity(entries.len());
        let mut edges = Vec::new();
        let mut discovered: Vec<Core> = Vec::new();
        for entry in &entries {
            let id = format!("{prefix}{}", entry.id);
            let deps: Vec<String> = entry
                .core
                .deps()
                .iter()
                .map(|dep| {
                    if self.registered_id_for_core(dep, prefix).is_none()
                        && !discovered.iter().any(|d| d.ptr_eq(dep))
                    {
                        discovered.push(dep.clone());
                    }
                    self.id_for_core(dep, prefix)
                })
                .collect();
            for dep_id in &deps {
                edges.push(DescribeEdge {
                    from: dep_id.clone(),
                    to: id.clone(),
                });
            }
            nodes.push(DescribeNode {
                id,
                name: entry.name.clone(),
                factory: entry.factory.clone(),
                status: entry.core.status(),
                value: entry.core.cache_any().as_ref().map(describe_value),
                version: entry.core.version(),
                deps,
                meta: if entry.meta.is_empty() {
                    None
                } else {
                    Some(entry.meta.clone())
                },
            });
        }
        let mut seen = HashSet::new();
        let mut i = 0;
        while i < discovered.len() {
            let core = discovered[i].clone();
            i += 1;
            let key = core.identity_key();
            if !seen.insert(key) {
                continue;
            }
            let id = self.synthetic_id_for_core(&core, prefix);
            let deps: Vec<String> = core
                .deps()
                .iter()
                .map(|dep| {
                    if self.registered_id_for_core(dep, prefix).is_none()
                        && !discovered.iter().any(|d| d.ptr_eq(dep))
                    {
                        discovered.push(dep.clone());
                    }
                    self.id_for_core(dep, prefix)
                })
                .collect();
            for dep_id in &deps {
                edges.push(DescribeEdge {
                    from: dep_id.clone(),
                    to: id.clone(),
                });
            }
            nodes.push(DescribeNode {
                id,
                name: None,
                factory: core.factory().unwrap_or_else(|| "?".to_owned()),
                status: core.status(),
                value: core.cache_any().as_ref().map(describe_value),
                version: core.version(),
                deps,
                meta: None,
            });
        }
        let subgraphs = self
            .inner
            .mounts
            .borrow()
            .iter()
            .map(|m| {
                let mount_path = format!("{prefix}{}", m.at);
                let mut child = m.graph.describe_with_prefix(&format!("{mount_path}::"));
                child.name = Some(mount_path);
                child
            })
            .collect::<Vec<_>>();
        DescribeSnapshot {
            name: self.inner.name.clone(),
            nodes,
            edges,
            subgraphs: if subgraphs.is_empty() {
                None
            } else {
                Some(subgraphs)
            },
        }
    }

    fn observe_targets(
        &self,
        path: Option<&str>,
        sink: impl Fn(ObserveEvent) + 'static,
    ) -> GraphObserver {
        let targets = self.observe_target_entries(path);
        let clock = self.inner.clock.clone();
        let sink = Rc::new(sink);
        let mut observer = GraphObserver {
            unsubs: Vec::with_capacity(targets.len()),
        };
        for (path, core) in targets {
            let sink = sink.clone();
            let clock = clock.clone();
            let subscribed = catch_unwind(AssertUnwindSafe(|| {
                Node::<AnyValue>::from_core(core).subscribe_graph_observer(move |msg| {
                    let seq = clock.get();
                    clock.set(seq + 1);
                    sink(ObserveEvent {
                        path: path.clone(),
                        msg: ObserveMessage::from_message(msg),
                        tier: msg.tier(),
                        seq,
                    });
                })
            }));
            let unsub = match subscribed {
                Ok(unsub) => unsub,
                Err(payload) => {
                    for unsub in observer.unsubs.drain(..).flatten() {
                        unsub();
                    }
                    resume_unwind(payload);
                }
            };
            observer.unsubs.push(Some(unsub));
        }
        observer
    }

    fn observe_target_entries(&self, path: Option<&str>) -> Vec<(String, Core)> {
        let mut all = Vec::new();
        self.collect_observe_entries("", &mut all);
        if let Some(path) = path {
            let prefix = format!("{path}::");
            all.into_iter()
                .filter(|(id, _)| id == path || id.starts_with(&prefix))
                .collect()
        } else {
            all
        }
    }

    fn collect_observe_entries(&self, prefix: &str, out: &mut Vec<(String, Core)>) {
        out.extend(
            self.inner
                .entries
                .borrow()
                .iter()
                .map(|entry| (format!("{prefix}{}", entry.id), entry.core.clone())),
        );
        for mount in self.inner.mounts.borrow().iter() {
            mount
                .graph
                .collect_observe_entries(&format!("{prefix}{}::", mount.at), out);
        }
    }

    fn contains_graph(&self, target: &Graph) -> bool {
        Rc::ptr_eq(&self.inner, &target.inner)
            || self
                .inner
                .mounts
                .borrow()
                .iter()
                .any(|m| m.graph.contains_graph(target))
    }
}

/// Construct a graph (D4 graph verb).
pub fn graph() -> Graph {
    Graph::new(GraphOptions::default())
}

/// Creates or computes `graph_opts`.
pub fn graph_opts(opts: GraphOptions) -> Graph {
    Graph::new(opts)
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
/// `DescribeOpts` data container.
pub struct DescribeOpts {
    /// `explain` field for explain.
    pub explain: Option<Explain>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `Explain` data container.
pub struct Explain {
    /// `from` field for from.
    pub from: String,
    /// `to` field for to.
    pub to: String,
}

/// Read-only value-level view over a node fn's dependency snapshot (D27/R-primary-api-clean).
///
/// `Values` exposes DATA-oriented dep state (`latest`, `prev`, `batch`, terminal state)
/// without the ctx-level power surface (`up`, `down`, `rewire_next`). Graph `derived` and
/// `effect` use this canonical Rust value API; `node`/`producer` remain the ctx-level
/// escape hatch.
pub struct Values<'a> {
    records: &'a [DepRecord],
}

impl<'a> Values<'a> {
    fn new(records: &'a [DepRecord]) -> Self {
        Self { records }
    }

    /// Updates or reads `len`.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Updates or reads `is_empty`.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Value-level derived helper: last committed DATA before dep `i`'s current batch.
    /// This is graph sugar, not part of the raw `Ctx` dep-value surface (D77).
    pub fn prev<T: 'static>(&self, i: usize) -> Option<Rc<T>> {
        self.records
            .get(i)
            .and_then(|r| r.prev_data.clone())
            .and_then(|a| a.downcast::<T>().ok())
    }

    /// DATA payloads dep `i` delivered before this run, grouped by upstream wave.
    pub fn batches<T: 'static>(&self, i: usize) -> Vec<Vec<Rc<T>>> {
        self.records
            .get(i)
            .map(|r| {
                r.wave_data
                    .iter()
                    .map(|wave| wave.iter().filter_map(WaveData::data::<T>).collect())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Terminal state for dep `i`, when terminal-as-real-input sugar needs it.
    pub fn terminal(&self, i: usize) -> Option<&DepTerminal> {
        self.records.get(i).and_then(|r| r.terminal.as_ref())
    }
}

/// A graph-layer erased node handle returned by [`Graph::find`].
///
/// This is intentionally not `Node<AnyValue>`: typed `Node<T>::cache()` downcasts to `T`,
/// while an erased cache should expose the raw `AnyValue` payload.
#[derive(Clone)]
pub struct GraphNode {
    core: Core,
}

impl GraphNode {
    fn new(core: Core) -> Self {
        Self { core }
    }

    /// Updates or reads `core`.
    pub fn core(&self) -> Core {
        self.core.clone()
    }

    /// Updates or reads `status`.
    pub fn status(&self) -> Status {
        self.core.status()
    }

    /// Updates or reads `version`.
    pub fn version(&self) -> Option<NodeVersion> {
        self.core.version()
    }

    /// Updates or reads `cache_any`.
    pub fn cache_any(&self) -> Option<AnyValue> {
        self.core.cache_any()
    }

    /// Updates or reads `deps`.
    pub fn deps(&self) -> Vec<Core> {
        self.core.deps()
    }

    /// Updates or reads `subscribe`.
    pub fn subscribe(&self, sink: impl Fn(&Message<AnyValue>) + 'static) -> Box<dyn FnOnce()> {
        Node::<AnyValue>::from_core(self.core.clone()).subscribe(sink)
    }

    /// Emit a raw erased wave at this graph-registered node boundary.
    ///
    /// This mirrors [`Node::down`] for nodes recovered through [`Graph::find`].
    pub fn down(&self, msgs: Vec<Message<AnyValue>>) {
        Node::<AnyValue>::from_core(self.core.clone()).down(msgs)
    }
}

/// D39 describe snapshot: flat nodes/edges plus mounted subgraphs.
#[derive(Debug, Clone, PartialEq)]
pub struct DescribeSnapshot {
    /// `name` field for name.
    pub name: Option<String>,
    /// `nodes` field for nodes.
    pub nodes: Vec<DescribeNode>,
    /// `edges` field for edges.
    pub edges: Vec<DescribeEdge>,
    /// `subgraphs` field for subgraphs.
    pub subgraphs: Option<Vec<DescribeSnapshot>>,
}

#[derive(Debug, Clone, PartialEq)]
/// `DescribeNode` data container.
pub struct DescribeNode {
    /// `id` field for id.
    pub id: String,
    /// `name` field for name.
    pub name: Option<String>,
    /// `factory` field for factory.
    pub factory: String,
    /// `status` field for status.
    pub status: Status,
    /// `value` field for value.
    pub value: Option<DescribeValue>,
    /// `version` field for version.
    pub version: Option<NodeVersion>,
    /// `deps` field for deps.
    pub deps: Vec<String>,
    /// `meta` field for meta.
    pub meta: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `DescribeEdge` data container.
pub struct DescribeEdge {
    /// `from` field for from.
    pub from: String,
    /// `to` field for to.
    pub to: String,
}

#[derive(Debug, Clone, PartialEq)]
/// `DescribeValue` variants.
pub enum DescribeValue {
    /// `Bool` variant.
    Bool(bool),
    /// `I64` variant.
    I64(i64),
    /// `U64` variant.
    U64(u64),
    /// `F64` variant.
    F64(f64),
    /// `String` variant.
    String(String),
    /// `Opaque` variant.
    Opaque,
}

#[derive(Clone)]
/// `ObserveEvent` data container.
pub struct ObserveEvent {
    /// `path` field for path.
    pub path: String,
    /// `msg` field for msg.
    pub msg: ObserveMessage,
    /// `tier` field for tier.
    pub tier: Tier,
    /// `seq` field for seq.
    pub seq: u64,
}

#[derive(Clone)]
/// `ObserveMessage` variants.
pub enum ObserveMessage {
    /// `Start` variant.
    Start,
    /// `Pause` variant.
    Pause(LockId),
    /// `Resume` variant.
    Resume(LockId),
    /// `Pull` variant.
    Pull(PullDemand),
    /// `Dirty` variant.
    Dirty,
    /// `Data` variant.
    Data(AnyValue),
    /// `Resolved` variant.
    Resolved,
    /// `Invalidate` variant.
    Invalidate,
    /// `Complete` variant.
    Complete,
    /// `Error` variant.
    Error(String),
    /// `Teardown` variant.
    Teardown,
}

impl ObserveMessage {
    fn from_message(msg: &Message<AnyValue>) -> Self {
        match msg {
            Message::Start => ObserveMessage::Start,
            Message::Pause(lock) => ObserveMessage::Pause(lock.clone()),
            Message::Resume(lock) => ObserveMessage::Resume(lock.clone()),
            Message::Pull(demand) => ObserveMessage::Pull(demand.clone()),
            Message::Dirty => ObserveMessage::Dirty,
            Message::Data(value) => ObserveMessage::Data(value.clone()),
            Message::Resolved => ObserveMessage::Resolved,
            Message::Invalidate => ObserveMessage::Invalidate,
            Message::Complete => ObserveMessage::Complete,
            Message::Error(error) => ObserveMessage::Error(format!("{error}")),
            Message::Teardown => ObserveMessage::Teardown,
        }
    }

    /// Updates or reads `kind`.
    pub fn kind(&self) -> &'static str {
        match self {
            ObserveMessage::Start => "START",
            ObserveMessage::Pause(_) => "PAUSE",
            ObserveMessage::Resume(_) => "RESUME",
            ObserveMessage::Pull(_) => "PULL",
            ObserveMessage::Dirty => "DIRTY",
            ObserveMessage::Data(_) => "DATA",
            ObserveMessage::Resolved => "RESOLVED",
            ObserveMessage::Invalidate => "INVALIDATE",
            ObserveMessage::Complete => "COMPLETE",
            ObserveMessage::Error(_) => "ERROR",
            ObserveMessage::Teardown => "TEARDOWN",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// `TopologyEventKind` variants.
pub enum TopologyEventKind {
    /// `NodeRegistered` variant.
    NodeRegistered,
    /// `DepsChanged` variant.
    DepsChanged,
    /// `NodeReleased` variant.
    NodeReleased,
    /// `MountChanged` variant.
    MountChanged,
}

#[derive(Clone)]
/// `TopologyEvent` data container.
pub struct TopologyEvent {
    /// `kind` field for kind.
    pub kind: TopologyEventKind,
    /// `path` field for path.
    pub path: String,
    /// `deps` field for deps.
    pub deps: Vec<String>,
    /// `prev_deps` field for prev deps.
    pub prev_deps: Option<Vec<String>>,
    /// `factory` field for factory.
    pub factory: Option<String>,
    /// `seq` field for seq.
    pub seq: u64,
}

/// `GraphObserver` data container.
pub struct GraphObserver {
    unsubs: Vec<Option<Box<dyn FnOnce()>>>,
}

/// `GraphTopologyObserver` data container.
pub struct GraphTopologyObserver {
    graph: Graph,
    id: usize,
    active: bool,
}

#[derive(Clone)]
/// `ObserveStream` data container.
pub struct ObserveStream {
    graph: Graph,
    path: Option<String>,
}

#[derive(Clone)]
/// `TopologyStream` data container.
pub struct TopologyStream {
    graph: Graph,
    path: Option<String>,
}

impl ObserveStream {
    /// Updates or reads `subscribe`.
    pub fn subscribe(&self, sink: impl Fn(ObserveEvent) + 'static) -> GraphObserver {
        self.graph.observe_targets(self.path.as_deref(), sink)
    }
}

impl TopologyStream {
    /// Updates or reads `subscribe`.
    pub fn subscribe(&self, sink: impl Fn(TopologyEvent) + 'static) -> GraphTopologyObserver {
        let id = self
            .graph
            .inner
            .topology_observer_free
            .borrow_mut()
            .pop()
            .unwrap_or_else(|| {
                let mut observers = self.graph.inner.topology_observers.borrow_mut();
                let id = observers.len();
                observers.push(None);
                id
            });
        let mut observers = self.graph.inner.topology_observers.borrow_mut();
        observers[id] = Some(TopologyObserverEntry {
            path: self.path.clone(),
            sink: Rc::new(sink),
        });
        self.graph
            .inner
            .topology_observer_active
            .set(self.graph.inner.topology_observer_active.get() + 1);
        self.graph.ensure_mounted_topology_forwarders();
        GraphTopologyObserver {
            graph: self.graph.clone(),
            id,
            active: true,
        }
    }
}

impl GraphObserver {
    /// Updates or reads `unsubscribe`.
    pub fn unsubscribe(mut self) {
        self.unsubscribe_all();
    }

    fn unsubscribe_all(&mut self) {
        for unsub in &mut self.unsubs {
            if let Some(unsub) = unsub.take() {
                unsub();
            }
        }
    }
}

impl GraphTopologyObserver {
    /// Updates or reads `unsubscribe`.
    pub fn unsubscribe(mut self) {
        self.unsubscribe_inner();
    }

    fn unsubscribe_inner(&mut self) {
        if !self.active {
            return;
        }
        self.active = false;
        let mut release_forwarders = false;
        if let Some(slot) = self
            .graph
            .inner
            .topology_observers
            .borrow_mut()
            .get_mut(self.id)
        {
            if slot.take().is_some() {
                let active = self
                    .graph
                    .inner
                    .topology_observer_active
                    .get()
                    .saturating_sub(1);
                self.graph.inner.topology_observer_active.set(active);
                release_forwarders = active == 0;
                self.graph
                    .inner
                    .topology_observer_free
                    .borrow_mut()
                    .push(self.id);
            }
        }
        if release_forwarders {
            self.graph.release_mounted_topology_forwarders();
        }
    }
}

impl Drop for GraphTopologyObserver {
    fn drop(&mut self) {
        self.unsubscribe_inner();
    }
}

impl Drop for GraphObserver {
    fn drop(&mut self) {
        self.unsubscribe_all();
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `NodeProfile` data container.
pub struct NodeProfile {
    /// `invokes` field for invokes.
    pub invokes: u64,
    /// `total_duration_ns` field for total duration ns.
    pub total_duration_ns: u128,
    /// `last_duration_ns` field for last duration ns.
    pub last_duration_ns: u128,
    /// `status` field for status.
    pub status: Status,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `Profile` data container.
pub struct Profile {
    /// `total_invokes` field for total invokes.
    pub total_invokes: u64,
    /// `nodes` field for nodes.
    pub nodes: BTreeMap<String, NodeProfile>,
}

fn explain_subset(snapshot: DescribeSnapshot, explain: &Explain) -> DescribeSnapshot {
    let snapshot = flatten_describe_snapshot(snapshot);
    let mut fwd: HashMap<String, Vec<String>> = HashMap::new();
    let mut rev: HashMap<String, Vec<String>> = HashMap::new();
    for edge in &snapshot.edges {
        fwd.entry(edge.from.clone())
            .or_default()
            .push(edge.to.clone());
        rev.entry(edge.to.clone())
            .or_default()
            .push(edge.from.clone());
    }
    let from_reach = reachable(&explain.from, &fwd);
    let to_reach = reachable(&explain.to, &rev);
    let on_path: HashSet<String> = from_reach.intersection(&to_reach).cloned().collect();
    DescribeSnapshot {
        name: snapshot.name,
        nodes: snapshot
            .nodes
            .into_iter()
            .filter(|node| on_path.contains(&node.id))
            .collect(),
        edges: snapshot
            .edges
            .into_iter()
            .filter(|edge| on_path.contains(&edge.from) && on_path.contains(&edge.to))
            .collect(),
        subgraphs: None,
    }
}

fn flatten_describe_snapshot(mut snapshot: DescribeSnapshot) -> DescribeSnapshot {
    if let Some(subgraphs) = snapshot.subgraphs.take() {
        for child in subgraphs {
            let mut child = flatten_describe_snapshot(child);
            snapshot.nodes.append(&mut child.nodes);
            snapshot.edges.append(&mut child.edges);
        }
    }
    snapshot
}

fn reachable(start: &str, adj: &HashMap<String, Vec<String>>) -> HashSet<String> {
    let mut seen = HashSet::from([start.to_owned()]);
    let mut stack = vec![start.to_owned()];
    while let Some(current) = stack.pop() {
        for next in adj.get(&current).into_iter().flatten() {
            if seen.insert(next.clone()) {
                stack.push(next.clone());
            }
        }
    }
    seen
}

fn topology_path_matches(event_path: &str, path: &str) -> bool {
    event_path == path || event_path.starts_with(&format!("{path}::"))
}

fn prefix_topology_path(prefix: &str, path: &str) -> String {
    format!("{prefix}::{path}")
}

fn describe_value(value: &AnyValue) -> DescribeValue {
    let value = value.as_ref();
    if let Some(v) = value.downcast_ref::<bool>() {
        DescribeValue::Bool(*v)
    } else if let Some(v) = value.downcast_ref::<i8>() {
        DescribeValue::I64(i64::from(*v))
    } else if let Some(v) = value.downcast_ref::<i16>() {
        DescribeValue::I64(i64::from(*v))
    } else if let Some(v) = value.downcast_ref::<i32>() {
        DescribeValue::I64(i64::from(*v))
    } else if let Some(v) = value.downcast_ref::<i64>() {
        DescribeValue::I64(*v)
    } else if let Some(v) = value.downcast_ref::<isize>() {
        DescribeValue::I64(*v as i64)
    } else if let Some(v) = value.downcast_ref::<u8>() {
        DescribeValue::U64(u64::from(*v))
    } else if let Some(v) = value.downcast_ref::<u16>() {
        DescribeValue::U64(u64::from(*v))
    } else if let Some(v) = value.downcast_ref::<u32>() {
        DescribeValue::U64(u64::from(*v))
    } else if let Some(v) = value.downcast_ref::<u64>() {
        DescribeValue::U64(*v)
    } else if let Some(v) = value.downcast_ref::<usize>() {
        DescribeValue::U64(*v as u64)
    } else if let Some(v) = value.downcast_ref::<f32>() {
        DescribeValue::F64(f64::from(*v))
    } else if let Some(v) = value.downcast_ref::<f64>() {
        DescribeValue::F64(*v)
    } else if let Some(v) = value.downcast_ref::<String>() {
        DescribeValue::String(v.clone())
    } else if let Some(v) = value.downcast_ref::<&'static str>() {
        DescribeValue::String((*v).to_owned())
    } else {
        DescribeValue::Opaque
    }
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::rc::Rc;

    use super::*;

    #[test]
    fn release_nodes_hides_registry_and_releases_remaining_after_cleanup_panic() {
        let g = graph();
        let panic_node = g.state_opts(1, GraphNodeOpts::named("panic"));
        let later_node = g.state_opts(2, GraphNodeOpts::named("later"));
        let later_released = Rc::new(Cell::new(false));
        let flag = later_released.clone();
        panic_node
            .erased()
            .register_on_deactivation(Box::new(|| panic!("cleanup boom")));
        later_node
            .erased()
            .register_on_deactivation(Box::new(move || flag.set(true)));

        let result = catch_unwind(AssertUnwindSafe(|| {
            g.release_nodes(&[later_node.erased(), panic_node.erased()], "test");
        }));

        assert!(result.is_err());
        assert!(
            g.find("panic").is_none() && g.find("later").is_none(),
            "D122: cleanup panic must not leave graph registry pointing at released slots"
        );
        assert!(
            later_released.get(),
            "a cleanup panic in one released node must not skip later nodes in the group"
        );
    }

    #[test]
    fn release_nodes_emits_node_released_even_when_cleanup_panics_after_commit() {
        let g = graph();
        let panic_node = g.state_opts(1, GraphNodeOpts::named("panic"));
        panic_node
            .erased()
            .register_on_deactivation(Box::new(|| panic!("cleanup boom")));
        let events = Rc::new(RefCell::new(Vec::new()));
        let event_sink = events.clone();
        let observer = g
            .observe_topology()
            .subscribe(move |event| event_sink.borrow_mut().push(event));

        let result = catch_unwind(AssertUnwindSafe(|| {
            g.release_nodes(&[panic_node.erased()], "test");
        }));

        observer.unsubscribe();
        assert!(result.is_err());
        let events = events.borrow();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, TopologyEventKind::NodeReleased);
        assert_eq!(events[0].path, "panic");
        assert_eq!(events[0].factory.as_deref(), Some("state"));
        assert_eq!(events[0].deps, Vec::<String>::new());
        assert_eq!(events[0].seq, 0);
        assert!(g.find("panic").is_none());
    }

    #[test]
    fn release_nodes_detaches_only_graph_observer_subscribers() {
        let g = graph();
        let group = g.topology_group_opts(TopologyGroupOptions::named("child"));
        let child = group.state_opts(1, GraphNodeOpts::named("child/value"));
        let unrelated = g.state_opts(10, GraphNodeOpts::named("unrelated"));
        let events = Rc::new(RefCell::new(Vec::new()));
        let event_sink = events.clone();
        let observer = g
            .observe()
            .subscribe(move |event| event_sink.borrow_mut().push(event));

        group.release();

        assert!(
            g.find("child/value").is_none(),
            "D122: released group nodes disappear from find"
        );
        assert!(
            !g.describe()
                .nodes
                .iter()
                .any(|entry| entry.id == "child/value"),
            "D122: released group nodes disappear from describe"
        );

        unrelated.set(11);
        observer.unsubscribe();

        let events = events.borrow();
        assert!(
            events
                .iter()
                .any(|event| event.path == "unrelated"
                    && matches!(event.msg, ObserveMessage::Data(_))),
            "D557: graph observer remains an ordinary observer and keeps unrelated subscriptions"
        );
        assert!(
            events.iter().all(|event| event.path != "child/value"
                || !matches!(
                    event.msg,
                    ObserveMessage::Complete | ObserveMessage::Error(_) | ObserveMessage::Teardown
                )),
            "D527: release does not synthesize protocol terminals"
        );
        drop(child);
    }

    #[test]
    fn release_nodes_still_rejects_public_subscribers() {
        let g = graph();
        let group = g.topology_group_opts(TopologyGroupOptions::named("child"));
        let child = group.state_opts(1, GraphNodeOpts::named("child/value"));
        let public_subscriber = child.subscribe(|_| {});
        let observer = g.observe().subscribe(|_| {});

        let result = catch_unwind(AssertUnwindSafe(|| group.release()));

        observer.unsubscribe();
        public_subscriber();
        group.release();
        assert!(result.is_err());
        assert!(
            g.find("child/value").is_none(),
            "after public subscriber detaches, release can proceed"
        );
    }

    #[test]
    fn graph_observer_unsubscribe_after_release_is_stale_safe() {
        let g = graph();
        let group = g.topology_group_opts(TopologyGroupOptions::named("child"));
        let _child = group.state_opts(1, GraphNodeOpts::named("child/value"));
        let observer = g.observe().subscribe(|_| {});

        group.release();
        observer.unsubscribe();
    }
}
