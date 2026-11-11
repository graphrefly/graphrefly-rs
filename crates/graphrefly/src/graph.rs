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

use crate::batch::BatchCtx;
use crate::checkpoint::GraphCheckpointJson;
use crate::ctx::{Ctx, DepRecord};
use crate::dispatcher::{default_dispatcher, Dispatcher, NodeFn};
use crate::environment::EnvironmentDrivers;
use crate::node::{Core, GraphArena, Node, NodeOpts, State, Status};
use crate::operators::Operator;
use crate::protocol::{AnyValue, Message};

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
    /// The host reporter (R-failure@3): failures in this graph with no caller and no
    /// node to end, such as a panicking sink or cleanup, go to it, and only to it.
    /// Without one they are written to standard error (review Q9); a panicking
    /// reporter is ignored (graphrefly-rs:D6).
    pub report: Option<crate::HostReporter>,
}

impl fmt::Debug for GraphOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphOptions")
            .field("name", &self.name)
            .field("profile", &self.profile)
            .field("dispatcher", &self.dispatcher)
            .field("environment", &self.environment)
            .field("report", &self.report.is_some())
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
            report: None,
        }
    }

    /// Returns these options with `report` as the graph's host reporter
    /// (R-failure@3).
    pub fn with_report(mut self, report: impl Fn(&crate::HostFailure) + 'static) -> Self {
        self.report = Some(Rc::new(report));
        self
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

    /// Returns these options with `node` as the substrate options.
    pub fn with_node(mut self, node: NodeOpts) -> Self {
        self.node = node;
        self
    }
}

/// An effect's latest per-run cleanup, kept in its private state.
struct EffectCleanup(RefCell<Option<Box<dyn FnOnce()>>>);

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
    /// The recipe JSON this factory reference stands for: `{ ref, config?,
    /// configVersion? }` (R-node, R-checkpoint).
    pub fn to_recipe(&self) -> GraphCheckpointJson {
        let mut recipe = serde_json::Map::new();
        recipe.insert(
            "ref".to_owned(),
            GraphCheckpointJson::String(self.ref_.clone()),
        );
        if let Some(config) = &self.config {
            recipe.insert("config".to_owned(), config.clone());
        }
        if let Some(config_version) = &self.config_version {
            recipe.insert("configVersion".to_owned(), config_version.clone());
        }
        GraphCheckpointJson::Object(recipe)
    }

    /// Reads a recipe JSON back as a factory reference; `None` if it has no `ref`.
    pub fn from_recipe(recipe: &GraphCheckpointJson) -> Option<Self> {
        let ref_ = recipe.get("ref")?.as_str()?.to_owned();
        Some(Self {
            ref_,
            config: recipe.get("config").cloned(),
            config_version: recipe.get("configVersion").cloned(),
        })
    }

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

    /// A group-owned state node with an initial value.
    pub fn state<T: 'static>(&self, initial: T) -> State<T> {
        self.state_opts(initial, GraphNodeOpts::default())
    }

    /// A group-owned state node with an initial value and options.
    pub fn state_opts<T: 'static>(&self, initial: T, opts: GraphNodeOpts) -> State<T> {
        self.assert_live();
        State::from_node(self.track_node(self.inner.graph.state_opts(initial, opts).into_node()))
    }

    /// A group-owned state node with no value yet.
    pub fn state_empty<T: 'static>(&self) -> State<T> {
        self.state_empty_opts(GraphNodeOpts::default())
    }

    /// A group-owned state node with no value yet, with options.
    pub fn state_empty_opts<T: 'static>(&self, opts: GraphNodeOpts) -> State<T> {
        self.assert_live();
        State::from_node(self.track_node(self.inner.graph.state_empty_opts(opts).into_node()))
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
                arena: GraphArena::with_reporter(opts.report),
                dispatcher,
                environment,
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
            opts.node.clone(),
            initial,
            f,
        );
        self.add(node, "node", opts)
    }

    /// Graph-owned state node with an initial value.
    pub fn state<T: 'static>(&self, initial: T) -> State<T> {
        self.state_opts(initial, GraphNodeOpts::default())
    }

    /// Graph-owned state node with an initial value and options.
    pub fn state_opts<T: 'static>(&self, initial: T, opts: GraphNodeOpts) -> State<T> {
        let node = Node::state_opts_in_arena_with_dispatcher(
            &self.inner.arena,
            self.inner.dispatcher.clone(),
            initial,
            opts.node.clone(),
        );
        State::from_node(self.add(node, "state", opts))
    }

    /// Graph-owned state node with no value yet.
    pub fn state_empty<T: 'static>(&self) -> State<T> {
        self.state_empty_opts(GraphNodeOpts::default())
    }

    /// Graph-owned state node with no value yet, with options.
    pub fn state_empty_opts<T: 'static>(&self, opts: GraphNodeOpts) -> State<T> {
        let node = Node::state_empty_opts_in_arena_with_dispatcher(
            &self.inner.arena,
            self.inner.dispatcher.clone(),
            opts.node.clone(),
        );
        State::from_node(self.add(node, "state", opts))
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
            opts.node.clone(),
            f,
        );
        self.add(node, "producer", opts)
    }

    /// Value-level derived sugar: return `Some(value)` to emit DATA, `None` for
    /// a no-emit/RESOLVED settle. Reads dep values through [`Values`], not raw `Ctx`.
    /// The callback runs once every dependency has a value, then whenever one sends
    /// DATA; the kernel itself has no such gate ([`Graph::node`] runs on whatever its
    /// dependencies hold).
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
            opts.node.clone(),
            move |ctx| {
                if !sugar_gate(ctx) {
                    return;
                }
                let values = Values::new(ctx.dep_records());
                if let Some(value) = f(&values) {
                    ctx.emit(value);
                }
            },
        );
        self.add(node, "derived", opts)
    }

    /// Value-level sink sugar, with derived's gate: the callback runs once every
    /// dependency has a value, then whenever one sends DATA. A run's returned cleanup
    /// replaces the previous run's and runs when the lifetime closes (R-lifetime@2).
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
        let mut node_opts = opts.node.clone();
        // The effect keeps its latest per-run cleanup in private state; the lifetime's
        // cleanup runs it (R-lifetime@2). A caller's own setup still runs first.
        let user_setup = node_opts.setup.take();
        let node_opts = node_opts.with_setup(move |ctx| {
            let user_cleanup = user_setup.as_ref().and_then(|setup| (setup.0)(ctx));
            crate::lifetime::cleanup(move |closing| {
                let latest = closing
                    .state_get::<EffectCleanup>()
                    .and_then(|slot| slot.0.borrow_mut().take());
                if let Some(latest) = latest {
                    latest();
                }
                if let Some(user_cleanup) = user_cleanup {
                    user_cleanup(closing);
                }
            })
        });
        let node = Node::derived_opts_in_arena_with_dispatcher(
            &self.inner.arena,
            self.inner.dispatcher.clone(),
            deps,
            node_opts,
            move |ctx| {
                if !sugar_gate(ctx) {
                    return;
                }
                let values = Values::new(ctx.dep_records());
                ctx.state_set(EffectCleanup(RefCell::new(f(&values))));
            },
        );
        self.add(node, "effect", opts)
    }

    /// Declarative batch (D12): success commits, rollback/panic discards.
    pub fn batch<R>(&self, f: impl FnOnce(&BatchCtx) -> R) -> R {
        // The graph's own batch (R-graph-domain@3).
        crate::batch::batch_in(&self.inner.arena.domain(), f)
    }

    /// The graph's concurrency domain.
    pub(crate) fn domain(&self) -> crate::node::DomainRef {
        self.inner.arena.domain()
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
            opts.node.clone(),
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

    /// The graph's topology as data (R-graph-surface@1, R-node@2): every node in
    /// creation order as `{ id, kind, deps, quiet }` and nothing else; `deps` are the
    /// current dependencies, a ctx.rewire's included. A value or a status is read from
    /// the node. It activates nothing and runs nothing.
    pub fn describe(&self) -> Vec<DescribeNode> {
        self.topology_with_prefix("")
            .nodes
            .into_iter()
            .map(|node| DescribeNode {
                id: node.id,
                kind: node.kind,
                deps: node.deps,
                quiet: node.quiet,
            })
            .collect()
    }

    /// This package's own topology view for rendering and diagnostics: describe's
    /// nodes with their factory, name and annotations, the edges and the mounted
    /// graphs (D39). Not part of R-graph-surface; values and status are read from the
    /// node.
    pub fn topology(&self) -> TopologySnapshot {
        self.topology_opts(TopologyOpts::default())
    }

    /// Updates or reads `topology_opts`.
    pub fn topology_opts(&self, opts: TopologyOpts) -> TopologySnapshot {
        let snap = self.topology_with_prefix("");
        if let Some(explain) = opts.explain {
            explain_subset(snap, &explain)
        } else {
            snap
        }
    }

    /// Subscribes `sink` to each node in `ids` as an ordinary subscriber
    /// (R-graph-surface@1): a cold node is activated and stays active while observed,
    /// and each array a node delivers reaches `sink` as `{ id, msgs }`, in delivery
    /// order, its START array first. Returns the stop; after it, nothing more reaches
    /// `sink`, also in the middle of a wave.
    ///
    /// # Panics
    ///
    /// Panics when an id names no node of this graph.
    pub fn observe(
        &self,
        ids: &[&str],
        sink: impl Fn(&ObserveEvent<'_>) + 'static,
    ) -> Box<dyn FnOnce()> {
        let cores: Vec<(String, Core)> = ids
            .iter()
            .map(|id| {
                let core = self.core_at(id);
                let core = core.unwrap_or_else(|| panic!("observe: no node '{id}'"));
                ((*id).to_owned(), core)
            })
            .collect();
        let stopped = Rc::new(Cell::new(false));
        let sink = Rc::new(sink);
        let mut stops = Vec::with_capacity(cores.len());
        for (id, core) in cores {
            let stopped = stopped.clone();
            let sink = sink.clone();
            stops.push(Node::<AnyValue>::from_core(core).subscribe(
                move |msgs: &[Message<AnyValue>]| {
                    if !stopped.get() {
                        sink(&ObserveEvent { id: &id, msgs });
                    }
                },
            ));
        }
        Box::new(move || {
            stopped.set(true);
            for stop in stops {
                stop();
            }
        })
    }

    /// Holds the node `id` active as a subscriber that ignores what it receives
    /// (R-graph-surface@1); returns the release. Each retain is its own subscription.
    ///
    /// # Panics
    ///
    /// Panics when `id` names no node of this graph.
    pub fn retain(&self, id: &str) -> Box<dyn FnOnce()> {
        let core = self.core_at(id);
        let core = core.unwrap_or_else(|| panic!("retain: no node '{id}'"));
        Node::<AnyValue>::from_core(core).subscribe(|_| {})
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
            opts.node.clone(),
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
            opts.node.clone(),
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

    pub(crate) fn empty_source<T: 'static>(&self, factory: &str, opts: GraphNodeOpts) -> Node<T> {
        let node = Node::state_empty_opts_in_arena_with_dispatcher(
            &self.inner.arena,
            self.inner.dispatcher.clone(),
            opts.node.clone(),
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
        // A registered factory reference is the instance's recipe (R-node,
        // R-checkpoint); an explicit NodeOpts.recipe wins.
        if let Some(restore) = &opts.restore {
            core.set_recipe_if_absent(restore.to_recipe());
        }
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

    /// The node registered as `id`, looking through a mounted graph for a
    /// `mount::id` path (mounting is this package's own).
    fn core_at(&self, id: &str) -> Option<Core> {
        if let Some(core) = self.inner.by_id.borrow().get(id).cloned() {
            return Some(core);
        }
        let (at, rest) = id.split_once("::")?;
        let child = self
            .inner
            .mounts
            .borrow()
            .iter()
            .find(|m| m.at == at)
            .map(|m| m.graph.clone())?;
        child.core_at(rest)
    }

    /// Holds `node` active, as [`Graph::retain`] does, for a bundle that has the
    /// handle rather than the id.
    pub(crate) fn hold<T: 'static>(&self, node: &Node<T>) -> Box<dyn FnOnce()> {
        let id = self
            .registered_id_for_core(&node.erased(), "")
            .expect("graph: cannot hold a node this graph has not registered");
        self.retain(&id)
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

    fn topology_with_prefix(&self, prefix: &str) -> TopologySnapshot {
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
                edges.push(TopologyEdge {
                    from: dep_id.clone(),
                    to: id.clone(),
                });
            }
            nodes.push(TopologyNode {
                id,
                name: entry.name.clone(),
                factory: entry.factory.clone(),
                kind: DescribeKind::of(&entry.core),
                deps,
                quiet: entry.core.is_quiet(),
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
                edges.push(TopologyEdge {
                    from: dep_id.clone(),
                    to: id.clone(),
                });
            }
            nodes.push(TopologyNode {
                id,
                name: None,
                factory: core.factory().unwrap_or_else(|| "?".to_owned()),
                kind: DescribeKind::of(&core),
                deps,
                quiet: core.is_quiet(),
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
                let mut child = m.graph.topology_with_prefix(&format!("{mount_path}::"));
                child.name = Some(mount_path);
                child
            })
            .collect::<Vec<_>>();
        TopologySnapshot {
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
/// `TopologyOpts` data container.
pub struct TopologyOpts {
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

/// The gate of `derived` and `effect` (sugar, not kernel: graphrefly:D797 (9), review
/// Q1): the callback runs once every dependency has a value, and after that when a
/// dependency sent DATA in this invocation. A dependency's latest value is kept while
/// the gate waits, so the first callback sees every value. A node with no
/// dependencies runs on its first run.
fn sugar_gate(ctx: &Ctx) -> bool {
    let mut fresh = ctx.dep_len() == 0;
    for dep in ctx.deps() {
        if !dep.has() {
            return false;
        }
        fresh |= dep.raw_waves().iter().any(|wave| !wave.is_empty());
    }
    fresh
}

/// Read-only value-level view over a node fn's dependency snapshot (D27/R-primary-api-clean).
///
/// Value-level sugar sees values only (R-primary-api-clean@2): each dependency's
/// latest value and the one before it. Arrays, terminals given as input, a manual
/// INVALIDATE and the policies belong to the ctx-level node ([`Graph::node`]).
///
/// ```compile_fail
/// let g = graphrefly::graph();
/// let s = g.state(1i32);
/// let _ = g.derived::<i32, _>(vec![s.erased()], |values| {
///     let _arrays = values.batches::<i32>(0); // arrays are ctx-level
///     None
/// });
/// ```
///
/// ```compile_fail
/// let g = graphrefly::graph();
/// let s = g.state(1i32);
/// let _ = g.derived::<i32, _>(vec![s.erased()], |values| {
///     let _ended = values.terminal(0); // a terminal given as input is ctx-level
///     None
/// });
/// ```
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

    /// Dep `i`'s latest value: the last DATA it delivered, in this run or before. Once
    /// the gate has opened every dependency has one (`None` only if it is not a `T`).
    pub fn value<T: 'static>(&self, i: usize) -> Option<Rc<T>> {
        self.records
            .get(i)
            .and_then(|r| r.latest.clone())
            .and_then(|a| a.downcast::<T>().ok())
    }

    /// Value-level derived helper: last committed DATA before dep `i`'s current batch.
    /// This is graph sugar, not part of the raw `Ctx` dep-value surface (D77).
    pub fn prev<T: 'static>(&self, i: usize) -> Option<Rc<T>> {
        self.records
            .get(i)
            .and_then(|r| r.prev_data.clone())
            .and_then(|a| a.downcast::<T>().ok())
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

    /// The state handle of a found state node, the host's way to write it
    /// (graphrefly-rs:D3); `None` for a computed node. The value type is the caller's
    /// to know, as with every erased read.
    #[must_use]
    pub fn as_state<T: 'static>(&self) -> Option<State<T>> {
        self.core
            .is_state()
            .then(|| State::from_node(Node::from_core(self.core.clone())))
    }

    /// The typed handle of a found node, for reading and subscribing; the value type
    /// is the caller's to know, as with every erased read.
    #[must_use]
    pub fn as_node<T: 'static>(&self) -> Node<T> {
        Node::from_core(self.core.clone())
    }

    /// Updates or reads `status`.
    pub fn status(&self) -> Status {
        self.core.status()
    }

    /// Updates or reads `cache_any`.
    pub fn cache_any(&self) -> Option<AnyValue> {
        self.core.cache_any()
    }

    /// Updates or reads `deps`.
    pub fn deps(&self) -> Vec<Core> {
        self.core.deps()
    }

    /// Subscribes a sink, called once per array (R-wave).
    pub fn subscribe(&self, sink: impl Fn(&[Message<AnyValue>]) + 'static) -> Box<dyn FnOnce()> {
        Node::<AnyValue>::from_core(self.core.clone()).subscribe(sink)
    }
}

/// D39 describe snapshot: flat nodes/edges plus mounted subgraphs.
#[derive(Debug, Clone, PartialEq)]
pub struct TopologySnapshot {
    /// `name` field for name.
    pub name: Option<String>,
    /// `nodes` field for nodes.
    pub nodes: Vec<TopologyNode>,
    /// `edges` field for edges.
    pub edges: Vec<TopologyEdge>,
    /// `subgraphs` field for subgraphs.
    pub subgraphs: Option<Vec<TopologySnapshot>>,
}

/// One node of [`Graph::describe`] (R-graph-surface@1): exactly its id, kind,
/// current dependencies and whether it is quiet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeNode {
    /// The node's id in the graph.
    pub id: String,
    /// A state or a computed node.
    pub kind: DescribeKind,
    /// The current dependencies' ids, in order.
    pub deps: Vec<String>,
    /// Whether the node is quiet (R-wave@2).
    pub quiet: bool,
}

#[derive(Debug, Clone, PartialEq)]
/// `TopologyNode` data container.
pub struct TopologyNode {
    /// `id` field for id.
    pub id: String,
    /// `name` field for name.
    pub name: Option<String>,
    /// `factory` field for factory.
    pub factory: String,
    /// Whether the node is a state or a computed node (R-node@2).
    pub kind: DescribeKind,
    /// The current dependencies, in order (R-node@2).
    pub deps: Vec<String>,
    /// Whether the node is quiet (R-node@2, R-wave@2).
    pub quiet: bool,
    /// `meta` field for meta.
    pub meta: Option<BTreeMap<String, String>>,
}

/// What kind of node a [`TopologyNode`] is (R-node@2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescribeKind {
    /// A state node: no function and no dependencies.
    State,
    /// A computed node.
    Node,
}

impl DescribeKind {
    fn of(core: &Core) -> Self {
        if core.is_state() {
            Self::State
        } else {
            Self::Node
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `TopologyEdge` data container.
pub struct TopologyEdge {
    /// `from` field for from.
    pub from: String,
    /// `to` field for to.
    pub to: String,
}

/// One array a node delivered to [`Graph::observe`] (R-graph-surface@1): no tier and
/// no sequence; events arrive in delivery order.
#[derive(Clone, Copy)]
pub struct ObserveEvent<'a> {
    /// The observed node's id.
    pub id: &'a str,
    /// The array, as the node delivered it.
    pub msgs: &'a [Message<AnyValue>],
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

/// `GraphTopologyObserver` data container.
pub struct GraphTopologyObserver {
    graph: Graph,
    id: usize,
    active: bool,
}

#[derive(Clone)]
/// `TopologyStream` data container.
pub struct TopologyStream {
    graph: Graph,
    path: Option<String>,
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

fn explain_subset(snapshot: TopologySnapshot, explain: &Explain) -> TopologySnapshot {
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
    TopologySnapshot {
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

fn flatten_describe_snapshot(mut snapshot: TopologySnapshot) -> TopologySnapshot {
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

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::rc::Rc;

    use super::*;

    /// A producer whose lifetime cleanup runs `on_close`.
    fn producer_with_cleanup(g: &Graph, name: &str, on_close: Rc<dyn Fn()>) -> Node<i32> {
        g.producer_opts(
            |_| {},
            GraphNodeOpts::named(name).with_node(NodeOpts::default().with_setup(move |_| {
                let on_close = on_close.clone();
                crate::lifetime::cleanup(move |_| on_close())
            })),
        )
    }

    #[test]
    fn release_nodes_after_a_cleanup_panic_releases_every_node() {
        // R-failure@3: a cleanup's panic goes to the host reporter and the close
        // completes, so a later node's cleanup still runs and the release succeeds.
        let reports = Rc::new(RefCell::new(Vec::new()));
        let g = graph_opts(GraphOptions::default().with_report({
            let reports = reports.clone();
            move |f: &crate::HostFailure| reports.borrow_mut().push(f.clone())
        }));
        let panic_node = producer_with_cleanup(&g, "panic", Rc::new(|| panic!("cleanup boom")));
        let later_released = Rc::new(Cell::new(false));
        let later_node = producer_with_cleanup(&g, "later", {
            let flag = later_released.clone();
            Rc::new(move || flag.set(true))
        });
        let u1 = panic_node.subscribe(|_| {});
        let u2 = later_node.subscribe(|_| {});
        u1();
        u2();
        g.release_nodes(&[later_node.erased(), panic_node.erased()], "test");

        assert!(g.find("panic").is_none() && g.find("later").is_none());
        assert!(later_released.get());
        assert_eq!(
            *reports.borrow(),
            vec![crate::HostFailure {
                source: "cleanup".into(),
                error: "cleanup boom".into()
            }]
        );
    }

    #[test]
    fn release_nodes_emits_node_released_after_a_cleanup_panic() {
        let g = graph();
        let panic_node = producer_with_cleanup(&g, "panic", Rc::new(|| panic!("cleanup boom")));
        let u = panic_node.subscribe(|_| {});
        u();
        let events = Rc::new(RefCell::new(Vec::new()));
        let event_sink = events.clone();
        let observer = g
            .observe_topology()
            .subscribe(move |event| event_sink.borrow_mut().push(event));

        g.release_nodes(&[panic_node.erased()], "test");

        observer.unsubscribe();
        let events = events.borrow();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, TopologyEventKind::NodeReleased);
        assert_eq!(events[0].path, "panic");
        assert_eq!(events[0].factory.as_deref(), Some("producer"));
        assert_eq!(events[0].deps, Vec::<String>::new());
        assert_eq!(events[0].seq, 0);
        assert!(g.find("panic").is_none());
    }

    #[test]
    fn release_nodes_still_rejects_public_subscribers() {
        let g = graph();
        let group = g.topology_group_opts(TopologyGroupOptions::named("child"));
        let child = group.state_opts(1, GraphNodeOpts::named("child/value"));
        let public_subscriber = child.subscribe(|_| {});
        let observer = g.observe(&["child/value"], |_| {});

        let result = catch_unwind(AssertUnwindSafe(|| group.release()));

        observer();
        public_subscriber();
        group.release();
        assert!(result.is_err());
        assert!(
            g.find("child/value").is_none(),
            "after public subscriber detaches, release can proceed"
        );
    }
}
