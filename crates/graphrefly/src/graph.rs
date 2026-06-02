//! Graph layer MVP (B53): graph-owned factories + first inspection surface.
//!
//! This module is per-language product surface (D24/D32), not behavioral
//! conformance. It keeps naming/find/describe/observe on the graph side
//! (R-node-thin / D39) while substrate nodes remain arena-slot handles.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use crate::batch::{batch as run_batch, BatchCtx};
use crate::ctx::{Ctx, DepRecord, DepTerminal};
use crate::node::{Core, GraphArena, Node, NodeOpts, Status};
use crate::protocol::{AnyValue, LockId, Message, Tier};

/// Graph construction options.
#[derive(Debug, Clone, Default)]
pub struct GraphOptions {
    pub name: Option<String>,
}

impl GraphOptions {
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
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
}

impl GraphNodeOpts {
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            ..Self::default()
        }
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
}

struct GraphInner {
    name: Option<String>,
    arena: GraphArena,
    entries: RefCell<Vec<Entry>>,
    by_id: RefCell<HashMap<String, Core>>,
    mounts: RefCell<Vec<Mount>>,
    seq: Cell<usize>,
    clock: Rc<Cell<u64>>,
}

/// A single-thread graph/concurrency-domain product surface (D22/B53).
#[derive(Clone)]
pub struct Graph {
    inner: Rc<GraphInner>,
}

impl Graph {
    pub fn new(opts: GraphOptions) -> Self {
        Self {
            inner: Rc::new(GraphInner {
                name: opts.name,
                arena: GraphArena::new(),
                entries: RefCell::new(Vec::new()),
                by_id: RefCell::new(HashMap::new()),
                mounts: RefCell::new(Vec::new()),
                seq: Cell::new(0),
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
        self.assert_graph_local_deps(&deps, opts.name.as_deref().unwrap_or("node"));
        let node = Node::derived_opts_in_arena(&self.inner.arena, deps, opts.node.clone(), f);
        self.add(node, "node", opts)
    }

    /// Graph-owned state node with an initial value.
    pub fn state<T: 'static>(&self, initial: T) -> Node<T> {
        self.state_opts(initial, GraphNodeOpts::default())
    }

    pub fn state_opts<T: 'static>(&self, initial: T, opts: GraphNodeOpts) -> Node<T> {
        let node = Node::state_in_arena(&self.inner.arena, initial);
        self.add(node, "state", opts)
    }

    /// Graph-owned SENTINEL state node.
    pub fn state_empty<T: 'static>(&self) -> Node<T> {
        self.state_empty_opts(GraphNodeOpts::default())
    }

    pub fn state_empty_opts<T: 'static>(&self, opts: GraphNodeOpts) -> Node<T> {
        let node = Node::state_empty_in_arena(&self.inner.arena);
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
        let node = Node::producer_opts_in_arena(&self.inner.arena, opts.node.clone(), f);
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
        let node =
            Node::derived_opts_in_arena(&self.inner.arena, deps, opts.node.clone(), move |ctx| {
                let values = Values::new(ctx.dep_records());
                if let Some(value) = f(&values) {
                    ctx.emit(value);
                }
            });
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
        let node =
            Node::derived_opts_in_arena(&self.inner.arena, deps, opts.node.clone(), move |ctx| {
                let values = Values::new(ctx.dep_records());
                if let Some(cleanup) = f(&values) {
                    ctx.on_deactivation(cleanup);
                }
            });
        self.add(node, "effect", opts)
    }

    /// Declarative batch (D12): success commits, rollback/panic discards.
    pub fn batch<R>(&self, f: impl FnOnce(&BatchCtx) -> R) -> R {
        run_batch(f)
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
        self.inner
            .mounts
            .borrow_mut()
            .push(Mount { at, graph: child });
    }

    /// Static point-in-time inspection snapshot (D39 first cut).
    pub fn describe(&self) -> DescribeSnapshot {
        self.describe_with_prefix("")
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

    fn add<T: 'static>(&self, node: Node<T>, factory: &str, opts: GraphNodeOpts) -> Node<T> {
        assert!(
            node.erased().same_graph_arena(&self.inner.arena),
            "graph node belongs to a different graph arena"
        );
        let id = opts.name.clone().unwrap_or_else(|| {
            let next = self.inner.seq.get();
            self.inner.seq.set(next + 1);
            format!("{factory}#{next}")
        });
        assert!(
            !self.inner.by_id.borrow().contains_key(&id),
            "duplicate graph node id '{id}'"
        );
        let entry = Entry {
            core: node.erased(),
            id: id.clone(),
            name: opts.name,
            factory: factory.to_owned(),
            meta: opts.meta,
        };
        self.inner.by_id.borrow_mut().insert(id, node.erased());
        self.inner.entries.borrow_mut().push(entry);
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

    fn id_for_core(&self, core: &Core, prefix: &str) -> Option<String> {
        self.inner
            .entries
            .borrow()
            .iter()
            .find(|entry| entry.core.ptr_eq(core))
            .map(|entry| format!("{prefix}{}", entry.id))
    }

    fn describe_with_prefix(&self, prefix: &str) -> DescribeSnapshot {
        let entries = self.inner.entries.borrow().clone();
        let mut nodes = Vec::with_capacity(entries.len());
        let mut edges = Vec::new();
        for entry in &entries {
            let id = format!("{prefix}{}", entry.id);
            let deps: Vec<String> = entry
                .core
                .deps()
                .iter()
                .filter_map(|dep| self.id_for_core(dep, prefix))
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

    /// Last committed DATA before dep `i`'s current batch; `None` is the
    /// SENTINEL/never-emitted state.
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
                r.batches
                    .iter()
                    .map(|wave| {
                        wave.iter()
                            .filter_map(|a| a.clone().downcast::<T>().ok())
                            .collect()
                    })
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
