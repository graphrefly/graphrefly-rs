//! Graph layer MVP (B53): graph-owned factories + first inspection surface.
//!
//! This module is per-language product surface (D24/D32), not behavioral
//! conformance. It keeps naming/find/describe/observe on the graph side
//! (R-node-thin / D39) while substrate nodes remain arena-slot handles.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::rc::Rc;

use crate::async_driver::LocalAsyncDriver;
use crate::batch::{batch as run_batch, BatchCtx};
use crate::checkpoint::GraphCheckpointJson;
use crate::ctx::{Ctx, DepRecord, DepTerminal, WaveData};
use crate::dispatcher::{default_dispatcher, Dispatcher, NodeFn};
use crate::node::{Core, GraphArena, Node, NodeOpts, Status};
use crate::operators::Operator;
use crate::protocol::{AnyValue, LockId, Message, Tier};

/// Graph construction options.
#[derive(Clone, Default)]
pub struct GraphOptions {
    pub name: Option<String>,
    pub profile: bool,
    pub dispatcher: Option<Dispatcher>,
    pub local_async_driver: Option<Rc<dyn LocalAsyncDriver>>,
}

impl fmt::Debug for GraphOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphOptions")
            .field("name", &self.name)
            .field("profile", &self.profile)
            .field("dispatcher", &self.dispatcher)
            .field(
                "local_async_driver",
                &self.local_async_driver.as_ref().map(|_| "<installed>"),
            )
            .finish()
    }
}

impl GraphOptions {
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            profile: false,
            dispatcher: None,
            local_async_driver: None,
        }
    }
}

/// Graph-layer node options: naming/metadata live here, behavioral knobs are
/// threaded through to the substrate [`NodeOpts`].
#[derive(Debug, Clone, Default)]
pub struct GraphNodeOpts {
    pub name: Option<String>,
    pub meta: BTreeMap<String, String>,
    pub node: NodeOpts,
    pub restore: Option<RestoreFactoryMeta>,
}

impl GraphNodeOpts {
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RestoreFactoryMeta {
    pub ref_: String,
    pub config: Option<GraphCheckpointJson>,
    pub config_version: Option<GraphCheckpointJson>,
}

impl RestoreFactoryMeta {
    pub fn registry_ref(ref_: impl Into<String>) -> Self {
        Self {
            ref_: ref_.into(),
            config: None,
            config_version: None,
        }
    }

    pub fn with_config(mut self, config: GraphCheckpointJson) -> Self {
        self.config = Some(config);
        self
    }

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
}

struct GraphInner {
    name: Option<String>,
    arena: GraphArena,
    dispatcher: Dispatcher,
    local_async_driver: Option<Rc<dyn LocalAsyncDriver>>,
    profile_enabled: Cell<bool>,
    entries: RefCell<Vec<Entry>>,
    by_id: RefCell<HashMap<String, Core>>,
    mounts: RefCell<Vec<Mount>>,
    seq: Cell<usize>,
    synth_seq: Cell<usize>,
    synth_ids: RefCell<HashMap<(usize, usize, u64), String>>,
    clock: Rc<Cell<u64>>,
}

/// A single-thread graph/concurrency-domain product surface (D22/B53).
#[derive(Clone)]
pub struct Graph {
    inner: Rc<GraphInner>,
}

impl Graph {
    pub fn new(opts: GraphOptions) -> Self {
        let (dispatcher, local_async_driver) = match (opts.dispatcher, opts.local_async_driver) {
            (Some(dispatcher), Some(driver)) => (dispatcher, Some(driver)),
            (Some(dispatcher), None) => {
                let driver = dispatcher.local_async_driver();
                (dispatcher, driver)
            }
            (None, Some(driver)) => {
                let dispatcher = Dispatcher::new();
                (dispatcher, Some(driver))
            }
            (None, None) => {
                let dispatcher = default_dispatcher();
                let driver = dispatcher.local_async_driver();
                (dispatcher, driver)
            }
        };
        if opts.profile {
            dispatcher.set_recording(true);
        }
        Self {
            inner: Rc::new(GraphInner {
                name: opts.name,
                arena: GraphArena::new(),
                dispatcher,
                local_async_driver,
                profile_enabled: Cell::new(opts.profile),
                entries: RefCell::new(Vec::new()),
                by_id: RefCell::new(HashMap::new()),
                mounts: RefCell::new(Vec::new()),
                seq: Cell::new(0),
                synth_seq: Cell::new(0),
                synth_ids: RefCell::new(HashMap::new()),
                clock: Rc::new(Cell::new(0)),
            }),
        }
    }

    pub fn name(&self) -> Option<&str> {
        self.inner.name.as_deref()
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
    pub fn state<T: 'static>(&self, initial: T) -> Node<T> {
        self.state_opts(initial, GraphNodeOpts::default())
    }

    pub fn state_opts<T: 'static>(&self, initial: T, opts: GraphNodeOpts) -> Node<T> {
        let node = Node::state_in_arena_with_dispatcher(
            &self.inner.arena,
            self.inner.dispatcher.clone(),
            initial,
        );
        self.add(node, "state", opts)
    }

    /// Graph-owned SENTINEL state node.
    pub fn state_empty<T: 'static>(&self) -> Node<T> {
        self.state_empty_opts(GraphNodeOpts::default())
    }

    pub fn state_empty_opts<T: 'static>(&self, opts: GraphNodeOpts) -> Node<T> {
        let node = Node::state_empty_in_arena_with_dispatcher(
            &self.inner.arena,
            self.inner.dispatcher.clone(),
        );
        self.add(node, "state", opts)
    }

    /// ctx-level depless source; runs once on activation.
    pub fn producer<T: 'static, F: Fn(&Ctx) + 'static>(&self, f: F) -> Node<T> {
        self.producer_opts(f, GraphNodeOpts::default())
    }

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
    pub fn derived<T: 'static, F: Fn(&Values<'_>) -> Option<T> + 'static>(
        &self,
        deps: Vec<Core>,
        f: F,
    ) -> Node<T> {
        self.derived_opts(deps, f, GraphNodeOpts::default())
    }

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
            opts.node.clone(),
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
        self.inner
            .mounts
            .borrow_mut()
            .push(Mount { at, graph: child });
    }

    /// Static point-in-time inspection snapshot (D39 first cut).
    pub fn describe(&self) -> DescribeSnapshot {
        self.describe_opts(DescribeOpts::default())
    }

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

    pub(crate) fn restore_state_json_with_id(
        &self,
        id: String,
        opts: GraphNodeOpts,
    ) -> Node<GraphCheckpointJson> {
        let node = Node::state_empty_in_arena_with_dispatcher(
            &self.inner.arena,
            self.inner.dispatcher.clone(),
        );
        self.add_with_id(node, "state", id, opts)
    }

    pub(crate) fn restore_node_with_id(
        &self,
        id: String,
        factory: &'static str,
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
        self.add_with_id(node, factory, id, opts)
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
        let node = Node::state_empty_in_arena_with_dispatcher(
            &self.inner.arena,
            self.inner.dispatcher.clone(),
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
        core.set_local_async_driver(self.inner.local_async_driver.clone());
        assert!(
            !self.inner.by_id.borrow().contains_key(&id),
            "duplicate graph node id '{id}'"
        );
        let entry = Entry {
            core: core.clone(),
            id: id.clone(),
            name: opts.name,
            factory: factory.to_owned(),
            meta: opts.meta,
            restore: opts.restore,
        };
        self.inner.by_id.borrow_mut().insert(id.clone(), core);
        self.inner.entries.borrow_mut().push(entry);
        if let Some(n) = id
            .rsplit_once('#')
            .and_then(|(_, n)| n.parse::<usize>().ok())
        {
            self.inner.seq.set(self.inner.seq.get().max(n + 1));
        }
        node
    }

    fn assert_graph_local_deps(&self, deps: &[Core], label: &str) {
        for dep in deps {
            assert!(
                dep.same_graph_arena(&self.inner.arena),
                "{label} dep belongs to a different graph; cross-graph deps require a wire bridge"
            );
        }
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
                deps,
                meta: None,
            });
        }
        let subgraphs = self
            .inner
            .mounts
            .borrow()
            .iter()
            .map(|m| m.graph.describe_with_prefix(&format!("{prefix}{}::", m.at)))
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
            let unsub = Node::<AnyValue>::from_core(core).subscribe(move |msg| {
                let seq = clock.get();
                clock.set(seq + 1);
                sink(ObserveEvent {
                    path: path.clone(),
                    msg: ObserveMessage::from_message(msg),
                    tier: msg.tier(),
                    seq,
                });
            });
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

pub fn graph_opts(opts: GraphOptions) -> Graph {
    Graph::new(opts)
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DescribeOpts {
    pub explain: Option<Explain>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Explain {
    pub from: String,
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

    pub fn len(&self) -> usize {
        self.records.len()
    }

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

    pub fn core(&self) -> Core {
        self.core.clone()
    }

    pub fn status(&self) -> Status {
        self.core.status()
    }

    pub fn cache_any(&self) -> Option<AnyValue> {
        self.core.cache_any()
    }

    pub fn deps(&self) -> Vec<Core> {
        self.core.deps()
    }

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
    pub name: Option<String>,
    pub nodes: Vec<DescribeNode>,
    pub edges: Vec<DescribeEdge>,
    pub subgraphs: Option<Vec<DescribeSnapshot>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DescribeNode {
    pub id: String,
    pub name: Option<String>,
    pub factory: String,
    pub status: Status,
    pub value: Option<DescribeValue>,
    pub deps: Vec<String>,
    pub meta: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeEdge {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DescribeValue {
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(String),
    Opaque,
}

#[derive(Clone)]
pub struct ObserveEvent {
    pub path: String,
    pub msg: ObserveMessage,
    pub tier: Tier,
    pub seq: u64,
}

#[derive(Clone)]
pub enum ObserveMessage {
    Start,
    Pause(LockId),
    Resume(LockId),
    Dirty,
    Data(AnyValue),
    Resolved,
    Invalidate,
    Complete,
    Error(String),
    Teardown,
}

impl ObserveMessage {
    fn from_message(msg: &Message<AnyValue>) -> Self {
        match msg {
            Message::Start => ObserveMessage::Start,
            Message::Pause(lock) => ObserveMessage::Pause(lock.clone()),
            Message::Resume(lock) => ObserveMessage::Resume(lock.clone()),
            Message::Dirty => ObserveMessage::Dirty,
            Message::Data(value) => ObserveMessage::Data(value.clone()),
            Message::Resolved => ObserveMessage::Resolved,
            Message::Invalidate => ObserveMessage::Invalidate,
            Message::Complete => ObserveMessage::Complete,
            Message::Error(error) => ObserveMessage::Error(format!("{error}")),
            Message::Teardown => ObserveMessage::Teardown,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            ObserveMessage::Start => "START",
            ObserveMessage::Pause(_) => "PAUSE",
            ObserveMessage::Resume(_) => "RESUME",
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

pub struct GraphObserver {
    unsubs: Vec<Option<Box<dyn FnOnce()>>>,
}

#[derive(Clone)]
pub struct ObserveStream {
    graph: Graph,
    path: Option<String>,
}

impl ObserveStream {
    pub fn subscribe(&self, sink: impl Fn(ObserveEvent) + 'static) -> GraphObserver {
        self.graph.observe_targets(self.path.as_deref(), sink)
    }
}

impl GraphObserver {
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

impl Drop for GraphObserver {
    fn drop(&mut self) {
        self.unsubscribe_all();
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeProfile {
    pub invokes: u64,
    pub total_duration_ns: u128,
    pub last_duration_ns: u128,
    pub status: Status,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Profile {
    pub total_invokes: u64,
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
