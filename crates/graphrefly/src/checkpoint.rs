//! Fresh graph checkpoint/restore (R-snapshot/R-restore, D83/D94/D116).
//!
//! Storage is deliberately absent here: callers load/decode checkpoint JSON outside
//! the sync core, then pass the already-loaded value to [`restore_graph`].

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::rc::Rc;

use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};

use crate::ctx::Ctx;
use crate::dispatcher::NodeFn;
use crate::graph::{Graph, GraphNodeOpts, GraphOptions, RestoreFactoryMeta};
use crate::node::{Core, NodeRestoreRuntime, Status};
use crate::protocol::AnyValue;

pub const GRAPH_CHECKPOINT_VERSION: &str = "graphrefly.checkpoint.v1";

pub type GraphCheckpointJson = Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum GraphCheckpointValue {
    #[serde(rename = "SENTINEL")]
    Sentinel,
    #[serde(rename = "DATA")]
    Data { data: GraphCheckpointJson },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum GraphCheckpointTerminal {
    #[serde(rename = "none")]
    None,
    #[serde(rename = "COMPLETE")]
    Complete,
    #[serde(rename = "ERROR")]
    Error { error: GraphCheckpointJson },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum GraphCheckpointFactory {
    #[serde(rename = "registry-ref")]
    RegistryRef {
        #[serde(rename = "ref")]
        ref_: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        config: Option<GraphCheckpointJson>,
        #[serde(rename = "configVersion", skip_serializing_if = "Option::is_none")]
        config_version: Option<GraphCheckpointJson>,
    },
    #[serde(rename = "local-only")]
    LocalOnly { name: String, reason: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphCheckpointNode {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub factory: GraphCheckpointFactory,
    pub status: String,
    pub deps: Vec<String>,
    pub value: GraphCheckpointValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<GraphCheckpointJson>,
    pub terminal: GraphCheckpointTerminal,
    pub lifecycle: GraphCheckpointLifecycle,
    #[serde(rename = "ctxState")]
    pub ctx_state: GraphCheckpointCtxState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<BTreeMap<String, GraphCheckpointJson>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphCheckpointLifecycle {
    pub activated: bool,
    #[serde(rename = "hasCalledFnOnce")]
    pub has_called_fn_once: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphCheckpointCtxState {
    pub persist: bool,
    pub value: GraphCheckpointValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphCheckpointEdge {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphCheckpointMount {
    pub at: String,
    pub checkpoint: GraphCheckpoint,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphCheckpoint {
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub nodes: Vec<GraphCheckpointNode>,
    pub edges: Vec<GraphCheckpointEdge>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mounts: Option<Vec<GraphCheckpointMount>>,
}

#[derive(Clone)]
pub(crate) struct CheckpointEntry {
    pub id: String,
    pub name: Option<String>,
    pub factory: String,
    pub meta: BTreeMap<String, String>,
    pub restore: Option<RestoreFactoryMeta>,
    pub core: Core,
    pub unregistered: bool,
}

#[derive(Debug, Clone)]
pub struct GraphRestoreError(String);

impl GraphRestoreError {
    fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

impl fmt::Display for GraphRestoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for GraphRestoreError {}

pub type GraphRestoreResult<T> = Result<T, GraphRestoreError>;
type RestoreResult<T> = GraphRestoreResult<T>;
type JsonDefinitionFn = dyn Fn(&GraphCheckpointJson) -> RestoreResult<GraphCheckpointJson>;

#[derive(Clone)]
pub struct GraphRestoreDefinition {
    pub ref_: String,
    f: Rc<JsonDefinitionFn>,
}

impl GraphRestoreDefinition {
    pub fn json(
        ref_: impl Into<String>,
        f: impl Fn(&GraphCheckpointJson) -> RestoreResult<GraphCheckpointJson> + 'static,
    ) -> Self {
        Self {
            ref_: ref_.into(),
            f: Rc::new(f),
        }
    }

    fn call(&self, value: &GraphCheckpointJson) -> RestoreResult<GraphCheckpointJson> {
        (self.f)(value)
    }
}

pub trait GraphRestoreDescriptor {
    fn ref_(&self) -> &'static str;
    fn define(&self, ctx: RestoreDefineCtx<'_>) -> RestoreResult<RestoreNodeDefinition>;
}

#[derive(Clone)]
pub enum GraphRestoreEntry {
    Descriptor(Rc<dyn GraphRestoreDescriptor>),
    Definition(GraphRestoreDefinition),
}

impl GraphRestoreEntry {
    pub fn descriptor(d: impl GraphRestoreDescriptor + 'static) -> Self {
        Self::Descriptor(Rc::new(d))
    }

    pub fn definition(d: GraphRestoreDefinition) -> Self {
        Self::Definition(d)
    }
}

#[derive(Clone, Default)]
pub struct GraphRestoreRegistry {
    entries: BTreeMap<String, GraphRestoreEntry>,
}

impl GraphRestoreRegistry {
    pub fn new(entries: impl IntoIterator<Item = GraphRestoreEntry>) -> Self {
        let mut out = Self::default();
        for entry in entries {
            let key = match &entry {
                GraphRestoreEntry::Descriptor(d) => d.ref_().to_owned(),
                GraphRestoreEntry::Definition(d) => d.ref_.clone(),
            };
            assert!(
                out.entries.insert(key.clone(), entry).is_none(),
                "duplicate restore registry ref '{key}'"
            );
        }
        out
    }

    fn descriptor(&self, ref_: &str) -> Option<Rc<dyn GraphRestoreDescriptor>> {
        match self.entries.get(ref_) {
            Some(GraphRestoreEntry::Descriptor(d)) => Some(d.clone()),
            _ => None,
        }
    }

    fn definition(&self, ref_: &str) -> Option<GraphRestoreDefinition> {
        match self.entries.get(ref_) {
            Some(GraphRestoreEntry::Definition(d)) => Some(d.clone()),
            _ => None,
        }
    }
}

pub fn restore_registry(
    entries: impl IntoIterator<Item = GraphRestoreEntry>,
) -> GraphRestoreRegistry {
    GraphRestoreRegistry::new(entries)
}

pub fn default_restore_registry() -> GraphRestoreRegistry {
    GraphRestoreRegistry::new([
        GraphRestoreEntry::descriptor(StateRestoreDescriptor),
        GraphRestoreEntry::descriptor(MapJsonRestoreDescriptor),
    ])
}

pub struct RestoreDefineCtx<'a> {
    pub id: &'a str,
    pub deps: &'a [String],
    pub config: Option<&'a GraphCheckpointJson>,
    pub config_version: Option<&'a GraphCheckpointJson>,
    pub checkpoint: &'a GraphCheckpointNode,
    registry: &'a GraphRestoreRegistry,
}

impl RestoreDefineCtx<'_> {
    pub fn resolve_definition(&self, ref_: &str) -> RestoreResult<GraphRestoreDefinition> {
        self.registry.definition(ref_).ok_or_else(|| {
            GraphRestoreError::new(format!(
                "restore_graph: missing function definition for '{ref_}' (node '{}')",
                self.id
            ))
        })
    }
}

pub enum RestoreNodeKind {
    StateJson,
    NodeJson(NodeFn),
}

pub struct RestoreNodeDefinition {
    pub factory: &'static str,
    pub kind: RestoreNodeKind,
    pub opts: GraphNodeOpts,
}

pub struct StateRestoreDescriptor;

impl GraphRestoreDescriptor for StateRestoreDescriptor {
    fn ref_(&self) -> &'static str {
        "state"
    }

    fn define(&self, ctx: RestoreDefineCtx<'_>) -> RestoreResult<RestoreNodeDefinition> {
        if !ctx.deps.is_empty() {
            return Err(GraphRestoreError::new(format!(
                "restore_graph: state node '{}' cannot restore deps",
                ctx.id
            )));
        }
        if ctx.config.is_some() {
            return Err(GraphRestoreError::new(
                "restore_graph: built-in state descriptor does not accept config",
            ));
        }
        if ctx.config_version.is_some() {
            return Err(GraphRestoreError::new(
                "restore_graph: built-in state descriptor does not accept configVersion",
            ));
        }
        Ok(RestoreNodeDefinition {
            factory: "state",
            kind: RestoreNodeKind::StateJson,
            opts: restored_opts(ctx.checkpoint)?,
        })
    }
}

pub struct MapJsonRestoreDescriptor;

impl GraphRestoreDescriptor for MapJsonRestoreDescriptor {
    fn ref_(&self) -> &'static str {
        "map"
    }

    fn define(&self, ctx: RestoreDefineCtx<'_>) -> RestoreResult<RestoreNodeDefinition> {
        if ctx.deps.len() != 1 {
            return Err(GraphRestoreError::new(format!(
                "restore_graph: map node '{}' requires exactly one dep",
                ctx.id
            )));
        }
        let config = ctx.config.and_then(Value::as_object).ok_or_else(|| {
            GraphRestoreError::new("restore_graph: 'map' descriptor requires object config")
        })?;
        if ctx.config_version.is_some() {
            return Err(GraphRestoreError::new(
                "restore_graph: built-in map descriptor does not accept configVersion",
            ));
        }
        let fn_ref = config.get("fn").and_then(Value::as_str).ok_or_else(|| {
            GraphRestoreError::new("restore_graph: 'map' config.fn must be a string definition ref")
        })?;
        let definition = ctx.resolve_definition(fn_ref)?;
        let body: NodeFn = Rc::new(move |ctx: &Ctx| {
            for value in ctx.batch::<GraphCheckpointJson>(0) {
                match definition.call(value.as_ref()) {
                    Ok(out) => ctx.emit(out),
                    Err(err) => panic!("{err}"),
                }
            }
        });
        Ok(RestoreNodeDefinition {
            factory: "map",
            kind: RestoreNodeKind::NodeJson(body),
            opts: restored_opts(ctx.checkpoint)?,
        })
    }
}

#[derive(Clone)]
pub struct RestoreGraphOptions {
    pub registry: GraphRestoreRegistry,
    pub graph: GraphOptions,
}

impl RestoreGraphOptions {
    pub fn new(registry: GraphRestoreRegistry) -> Self {
        Self {
            registry,
            graph: GraphOptions::default(),
        }
    }
}

impl Graph {
    pub fn checkpoint(&self) -> RestoreResult<GraphCheckpoint> {
        checkpoint_graph(self)
    }
}

pub fn restore_graph(
    checkpoint: GraphCheckpoint,
    options: RestoreGraphOptions,
) -> RestoreResult<Graph> {
    let prepared = prepare_checkpoint(&checkpoint, &options.registry, "checkpoint", None)?;
    construct_prepared(&prepared, &options)
}

fn checkpoint_graph(graph: &Graph) -> RestoreResult<GraphCheckpoint> {
    let mut entries = graph.checkpoint_entries();
    let mut i = 0;
    while i < entries.len() {
        let deps = entries[i].core.deps();
        for dep in deps {
            if entries.iter().any(|candidate| candidate.core.ptr_eq(&dep)) {
                continue;
            }
            entries.push(CheckpointEntry {
                id: graph.checkpoint_synthetic_id_for_core(&dep),
                name: None,
                factory: dep.factory().unwrap_or_else(|| "?".to_owned()),
                meta: BTreeMap::new(),
                restore: None,
                core: dep,
                unregistered: true,
            });
        }
        i += 1;
    }
    let mut nodes = Vec::with_capacity(entries.len());
    let mut edges = Vec::new();
    for entry in &entries {
        let runtime = entry.core.checkpoint_runtime();
        ensure_quiescent_status(runtime.status, &entry.id, "checkpoint")?;
        let deps = entry.core.deps();
        let dep_ids = deps
            .iter()
            .map(|dep| {
                entries
                    .iter()
                    .find(|candidate| candidate.core.ptr_eq(dep))
                    .map(|candidate| candidate.id.clone())
                    .ok_or_else(|| {
                        GraphRestoreError::new(format!(
                            "checkpoint: node '{}' has an unregistered dep; local-only auto-discovery restore is not implemented",
                            entry.id
                        ))
                    })
            })
            .collect::<RestoreResult<Vec<_>>>()?;
        edges.extend(dep_ids.iter().map(|dep| GraphCheckpointEdge {
            from: dep.clone(),
            to: entry.id.clone(),
        }));
        nodes.push(GraphCheckpointNode {
            id: entry.id.clone(),
            name: entry.name.clone(),
            factory: checkpoint_factory(entry),
            status: status_to_str(runtime.status).to_owned(),
            deps: dep_ids,
            value: checkpoint_value(runtime.cache.as_ref(), runtime.has_data, &entry.id)?,
            version: None,
            terminal: if runtime.terminal {
                match runtime.status {
                    Status::Completed => GraphCheckpointTerminal::Complete,
                    Status::Errored => {
                        return Err(GraphRestoreError::new(format!(
                            "checkpoint: ERROR payload for node '{}' is unavailable; cannot serialize a placeholder",
                            entry.id
                        )));
                    }
                    _ => GraphCheckpointTerminal::None,
                }
            } else {
                GraphCheckpointTerminal::None
            },
            lifecycle: GraphCheckpointLifecycle {
                activated: runtime.activated,
                has_called_fn_once: runtime.has_called_fn_once,
            },
            ctx_state: GraphCheckpointCtxState {
                persist: runtime.ctx_state_persist,
                value: checkpoint_value(
                    runtime.ctx_state.as_ref(),
                    runtime.ctx_state.is_some(),
                    &format!("{}.ctxState", entry.id),
                )?,
            },
            meta: if entry.meta.is_empty() {
                None
            } else {
                Some(
                    entry
                        .meta
                        .iter()
                        .map(|(k, v)| (k.clone(), GraphCheckpointJson::String(v.clone())))
                        .collect(),
                )
            },
        });
    }
    let mounts = graph
        .checkpoint_mounts()
        .into_iter()
        .map(|(at, child)| {
            Ok(GraphCheckpointMount {
                at,
                checkpoint: checkpoint_graph(&child)?,
            })
        })
        .collect::<RestoreResult<Vec<_>>>()?;
    Ok(GraphCheckpoint {
        version: GRAPH_CHECKPOINT_VERSION.to_owned(),
        name: graph.name().map(str::to_owned),
        nodes,
        edges,
        mounts: if mounts.is_empty() {
            None
        } else {
            Some(mounts)
        },
    })
}

fn checkpoint_factory(entry: &CheckpointEntry) -> GraphCheckpointFactory {
    if entry.unregistered {
        return GraphCheckpointFactory::LocalOnly {
            name: entry.factory.clone(),
            reason: "node is an unregistered live dependency auto-discovered from topology"
                .to_owned(),
        };
    }
    if let Some(restore) = &entry.restore {
        return GraphCheckpointFactory::RegistryRef {
            ref_: restore.ref_.clone(),
            config: restore.config.clone(),
            config_version: restore.config_version.clone(),
        };
    }
    if entry.factory == "state" {
        return GraphCheckpointFactory::RegistryRef {
            ref_: "state".to_owned(),
            config: None,
            config_version: None,
        };
    }
    GraphCheckpointFactory::LocalOnly {
        name: entry.factory.clone(),
        reason: "node was not constructed with restore metadata".to_owned(),
    }
}

fn checkpoint_value(
    value: Option<&AnyValue>,
    has_data: bool,
    path: &str,
) -> RestoreResult<GraphCheckpointValue> {
    if !has_data {
        return Ok(GraphCheckpointValue::Sentinel);
    }
    let value = value.ok_or_else(|| {
        GraphRestoreError::new(format!(
            "checkpoint: value at {path} is marked DATA but absent"
        ))
    })?;
    Ok(GraphCheckpointValue::Data {
        data: any_to_json(value, path)?,
    })
}

fn any_to_json(value: &AnyValue, path: &str) -> RestoreResult<GraphCheckpointJson> {
    if let Some(v) = value.downcast_ref::<GraphCheckpointJson>() {
        validate_checkpoint_json(v, path, 0)?;
        return Ok(v.clone());
    }
    if let Some(v) = value.downcast_ref::<String>() {
        return Ok(Value::String(v.clone()));
    }
    if let Some(v) = value.downcast_ref::<bool>() {
        return Ok(Value::Bool(*v));
    }
    if let Some(v) = value.downcast_ref::<i32>() {
        return Ok(Value::Number(Number::from(*v)));
    }
    if let Some(v) = value.downcast_ref::<i64>() {
        return Ok(Value::Number(Number::from(*v)));
    }
    if let Some(v) = value.downcast_ref::<u32>() {
        return Ok(Value::Number(Number::from(*v)));
    }
    if let Some(v) = value.downcast_ref::<u64>() {
        return Ok(Value::Number(Number::from(*v)));
    }
    if let Some(v) = value.downcast_ref::<usize>() {
        return Ok(Value::Number(Number::from(*v as u64)));
    }
    if let Some(v) = value.downcast_ref::<f64>() {
        if let Some(n) = Number::from_f64(*v) {
            let out = Value::Number(n);
            validate_checkpoint_json(&out, path, 0)?;
            return Ok(out);
        }
    }
    Err(GraphRestoreError::new(format!(
        "checkpoint: value at {path} is not strict JSON compatible"
    )))
}

fn validate_checkpoint_json(
    value: &GraphCheckpointJson,
    path: &str,
    depth: u32,
) -> RestoreResult<()> {
    if depth > 128 {
        return Err(GraphRestoreError::new(format!(
            "checkpoint: JSON value at {path} exceeds maximum depth 128"
        )));
    }
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                validate_checkpoint_json(value, &format!("{path}.{key}"), depth + 1)?;
            }
        }
        Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                validate_checkpoint_json(value, &format!("{path}[{index}]"), depth + 1)?;
            }
        }
        Value::Number(number) => {
            let text = number.to_string();
            if text == "-0.0" || text == "-0" {
                return Err(GraphRestoreError::new(format!(
                    "checkpoint: JSON number at {path} is not strict canonical JSON compatible"
                )));
            }
            if let Some(float) = number.as_f64() {
                let abs = float.abs();
                if abs > 0.0 && abs < f64::MIN_POSITIVE {
                    return Err(GraphRestoreError::new(format!(
                        "checkpoint: JSON number at {path} is subnormal and not strict canonical JSON compatible"
                    )));
                }
            }
        }
        Value::Null | Value::Bool(_) | Value::String(_) => {}
    }
    Ok(())
}

fn restored_opts(node: &GraphCheckpointNode) -> RestoreResult<GraphNodeOpts> {
    let meta = node
        .meta
        .clone()
        .unwrap_or_default()
        .into_iter()
        .map(|(k, v)| {
            let value = v.as_str().ok_or_else(|| {
                GraphRestoreError::new(format!(
                    "restore_graph: node '{}' meta field '{}' must be a string in Rust",
                    node.id, k
                ))
            })?;
            Ok((k, value.to_owned()))
        })
        .collect::<RestoreResult<BTreeMap<_, _>>>()?;
    let restore = match &node.factory {
        GraphCheckpointFactory::RegistryRef {
            ref_,
            config,
            config_version,
        } => Some(RestoreFactoryMeta {
            ref_: ref_.clone(),
            config: config.clone(),
            config_version: config_version.clone(),
        }),
        GraphCheckpointFactory::LocalOnly { .. } => None,
    };
    Ok(GraphNodeOpts {
        name: node.name.clone(),
        meta,
        restore,
        ..GraphNodeOpts::default()
    })
}

struct PreparedNode {
    checkpoint: GraphCheckpointNode,
    descriptor: Rc<dyn GraphRestoreDescriptor>,
    deps: Vec<String>,
}

struct PreparedCheckpoint {
    checkpoint: GraphCheckpoint,
    nodes: BTreeMap<String, PreparedNode>,
    mounts: Vec<(String, PreparedCheckpoint)>,
}

fn prepare_checkpoint(
    checkpoint: &GraphCheckpoint,
    registry: &GraphRestoreRegistry,
    path: &str,
    mount_at: Option<&str>,
) -> RestoreResult<PreparedCheckpoint> {
    if checkpoint.version != GRAPH_CHECKPOINT_VERSION {
        return Err(GraphRestoreError::new(format!(
            "restore_graph: unsupported checkpoint version at {path}"
        )));
    }
    let mut nodes = BTreeMap::new();
    for node in &checkpoint.nodes {
        if nodes.contains_key(&node.id) {
            return Err(GraphRestoreError::new(format!(
                "restore_graph: duplicate node id '{}'",
                node.id
            )));
        }
        if let Some(at) = mount_at {
            let prefix = format!("{at}::");
            if node.id.starts_with(&prefix) {
                return Err(GraphRestoreError::new(format!(
                    "restore_graph: mounted checkpoint at '{at}' must use child-local node id '{}', not '{}'",
                    node.id.trim_start_matches(&prefix),
                    node.id
                )));
            }
        }
        let descriptor = match &node.factory {
            GraphCheckpointFactory::LocalOnly { name, reason } => {
                return Err(GraphRestoreError::new(format!(
                    "restore_graph: node '{}' uses local-only factory '{}' ({reason})",
                    node.id, name
                )));
            }
            GraphCheckpointFactory::RegistryRef { ref_, .. } => {
                registry.descriptor(ref_).ok_or_else(|| {
                    GraphRestoreError::new(format!(
                        "restore_graph: missing registry descriptor for '{ref_}' (node '{}')",
                        node.id
                    ))
                })?
            }
        };
        let status = status_from_str(&node.status, &node.id)?;
        ensure_quiescent_status(status, &node.id, "restore_graph")?;
        validate_checkpoint_node_json(node)?;
        if node.version.is_some() {
            return Err(GraphRestoreError::new(format!(
                "restore_graph: node '{}' carries runtime version metadata, which Rust restore does not support yet",
                node.id
            )));
        }
        nodes.insert(
            node.id.clone(),
            PreparedNode {
                checkpoint: node.clone(),
                descriptor,
                deps: node.deps.clone(),
            },
        );
    }
    for node in nodes.values() {
        for dep in &node.deps {
            if !nodes.contains_key(dep) {
                return Err(GraphRestoreError::new(format!(
                    "restore_graph: node '{}' has missing dep '{dep}'",
                    node.checkpoint.id
                )));
            }
        }
    }
    let mut edge_keys = BTreeSet::new();
    for edge in &checkpoint.edges {
        if !edge_keys.insert((edge.from.clone(), edge.to.clone())) {
            return Err(GraphRestoreError::new(format!(
                "restore_graph: duplicate edge '{}' -> '{}'",
                edge.from, edge.to
            )));
        }
        let Some(target) = nodes.get(&edge.to) else {
            return Err(GraphRestoreError::new(format!(
                "restore_graph: edge '{}' -> '{}' references a missing node",
                edge.from, edge.to
            )));
        };
        if !nodes.contains_key(&edge.from) || !target.deps.contains(&edge.from) {
            return Err(GraphRestoreError::new(format!(
                "restore_graph: edge '{}' -> '{}' is not present in target deps",
                edge.from, edge.to
            )));
        }
    }
    for node in nodes.values() {
        for dep in &node.deps {
            if !edge_keys.contains(&(dep.clone(), node.checkpoint.id.clone())) {
                return Err(GraphRestoreError::new(format!(
                    "restore_graph: node '{}' dep '{dep}' is missing its edge",
                    node.checkpoint.id
                )));
            }
        }
    }
    let mut mount_paths = BTreeSet::new();
    let mut mounts = Vec::new();
    for mount in checkpoint.mounts.clone().unwrap_or_default() {
        if mount.at.is_empty() {
            return Err(GraphRestoreError::new(format!(
                "restore_graph: mount path at {path}.mounts[] must not be empty"
            )));
        }
        if !mount_paths.insert(mount.at.clone()) {
            return Err(GraphRestoreError::new(format!(
                "restore_graph: duplicate mount path '{}'",
                mount.at
            )));
        }
        mounts.push((
            mount.at.clone(),
            prepare_checkpoint(
                &mount.checkpoint,
                registry,
                &format!("{path}.mounts.{}", mount.at),
                Some(&mount.at),
            )?,
        ));
    }
    Ok(PreparedCheckpoint {
        checkpoint: checkpoint.clone(),
        nodes,
        mounts,
    })
}

fn validate_checkpoint_node_json(node: &GraphCheckpointNode) -> RestoreResult<()> {
    if let GraphCheckpointFactory::RegistryRef {
        config,
        config_version,
        ..
    } = &node.factory
    {
        if let Some(config) = config {
            validate_checkpoint_json(config, &format!("{}.factory.config", node.id), 0)?;
        }
        if let Some(config_version) = config_version {
            validate_checkpoint_json(
                config_version,
                &format!("{}.factory.configVersion", node.id),
                0,
            )?;
        }
    }
    if let GraphCheckpointValue::Data { data } = &node.value {
        validate_checkpoint_json(data, &format!("{}.value", node.id), 0)?;
    }
    if let GraphCheckpointValue::Data { data } = &node.ctx_state.value {
        validate_checkpoint_json(data, &format!("{}.ctxState", node.id), 0)?;
    }
    if let GraphCheckpointTerminal::Error { error } = &node.terminal {
        validate_checkpoint_json(error, &format!("{}.terminal.error", node.id), 0)?;
    }
    if let Some(meta) = &node.meta {
        for (key, value) in meta {
            validate_checkpoint_json(value, &format!("{}.meta.{key}", node.id), 0)?;
        }
    }
    Ok(())
}

fn construct_prepared(
    prepared: &PreparedCheckpoint,
    options: &RestoreGraphOptions,
) -> RestoreResult<Graph> {
    let mut opts = options.graph.clone();
    opts.name = prepared.checkpoint.name.clone();
    let graph = crate::graph::graph_opts(opts);
    let mut built = BTreeMap::<String, Core>::new();
    let mut visiting = BTreeSet::<String>::new();
    for id in prepared.nodes.keys() {
        build_node(
            id,
            prepared,
            &options.registry,
            &graph,
            &mut built,
            &mut visiting,
        )?;
    }
    for (at, child) in &prepared.mounts {
        graph.mount_restored(construct_prepared(child, options)?, at.clone());
    }
    for (id, core) in &built {
        let node = &prepared
            .nodes
            .get(id)
            .expect("built map only contains prepared nodes")
            .checkpoint;
        let (cache, has_data) = restore_value(&node.value);
        let (terminal, status) = restore_terminal(node)?;
        let (ctx_state, ctx_state_persist) = restore_ctx_state(&node.ctx_state);
        core.restore_runtime(NodeRestoreRuntime {
            cache,
            has_data,
            status,
            terminal,
            has_called_fn_once: node.lifecycle.has_called_fn_once,
            ctx_state,
            ctx_state_persist,
        });
    }
    Ok(graph)
}

fn build_node(
    id: &str,
    prepared: &PreparedCheckpoint,
    registry: &GraphRestoreRegistry,
    graph: &Graph,
    built: &mut BTreeMap<String, Core>,
    visiting: &mut BTreeSet<String>,
) -> RestoreResult<Core> {
    if let Some(core) = built.get(id) {
        return Ok(core.clone());
    }
    if !visiting.insert(id.to_owned()) {
        return Err(GraphRestoreError::new(format!(
            "restore_graph: dependency cycle at '{id}'"
        )));
    }
    let item = prepared.nodes.get(id).ok_or_else(|| {
        GraphRestoreError::new(format!("restore_graph: missing prepared node '{id}'"))
    })?;
    let deps = item
        .deps
        .iter()
        .map(|dep| build_node(dep, prepared, registry, graph, built, visiting))
        .collect::<RestoreResult<Vec<_>>>()?;
    let (config, config_version) = match &item.checkpoint.factory {
        GraphCheckpointFactory::RegistryRef {
            config,
            config_version,
            ..
        } => (config.as_ref(), config_version.as_ref()),
        GraphCheckpointFactory::LocalOnly { .. } => (None, None),
    };
    let definition = item.descriptor.define(RestoreDefineCtx {
        id,
        deps: &item.deps,
        config,
        config_version,
        checkpoint: &item.checkpoint,
        registry,
    })?;
    let core = match definition.kind {
        RestoreNodeKind::StateJson => graph
            .restore_state_json_with_id(id.to_owned(), definition.opts)
            .erased(),
        RestoreNodeKind::NodeJson(body) => graph
            .restore_node_with_id(
                id.to_owned(),
                definition.factory,
                deps.clone(),
                body,
                definition.opts,
            )
            .erased(),
    };
    let restored_deps = core.deps();
    if restored_deps.len() != deps.len()
        || restored_deps
            .iter()
            .zip(deps.iter())
            .any(|(restored, expected)| !restored.ptr_eq(expected))
    {
        return Err(GraphRestoreError::new(format!(
            "restore_graph: descriptor '{}' registered node '{}' with deps that do not match the checkpoint",
            item.descriptor.ref_(),
            id
        )));
    }
    built.insert(id.to_owned(), core.clone());
    visiting.remove(id);
    Ok(core)
}

fn restore_value(value: &GraphCheckpointValue) -> (Option<AnyValue>, bool) {
    match value {
        GraphCheckpointValue::Sentinel => (None, false),
        GraphCheckpointValue::Data { data } => (Some(Rc::new(data.clone()) as AnyValue), true),
    }
}

fn restore_ctx_state(state: &GraphCheckpointCtxState) -> (Option<AnyValue>, bool) {
    let (value, has_data) = restore_value(&state.value);
    (if has_data { value } else { None }, state.persist)
}

fn restore_terminal(node: &GraphCheckpointNode) -> RestoreResult<(bool, Status)> {
    let status = status_from_str(&node.status, &node.id)?;
    match &node.terminal {
        GraphCheckpointTerminal::None => {
            if status.is_terminal() {
                return Err(GraphRestoreError::new(format!(
                    "restore_graph: node '{}' terminal status requires terminal state",
                    node.id
                )));
            }
            Ok((false, status))
        }
        GraphCheckpointTerminal::Complete => {
            if status != Status::Completed {
                return Err(GraphRestoreError::new(format!(
                    "restore_graph: node '{}' COMPLETE terminal requires completed status",
                    node.id
                )));
            }
            Ok((true, status))
        }
        GraphCheckpointTerminal::Error { .. } => Err(GraphRestoreError::new(format!(
            "restore_graph: node '{}' carries ERROR terminal payload, which Rust restore cannot preserve yet",
            node.id
        ))),
    }
}

fn status_to_str(status: Status) -> &'static str {
    match status {
        Status::Sentinel => "sentinel",
        Status::Pending => "pending",
        Status::Dirty => "dirty",
        Status::Settled => "settled",
        Status::Resolved => "resolved",
        Status::Completed => "completed",
        Status::Errored => "errored",
    }
}

fn status_from_str(status: &str, id: &str) -> RestoreResult<Status> {
    match status {
        "sentinel" => Ok(Status::Sentinel),
        "pending" => Ok(Status::Pending),
        "dirty" => Ok(Status::Dirty),
        "settled" => Ok(Status::Settled),
        "resolved" => Ok(Status::Resolved),
        "completed" => Ok(Status::Completed),
        "errored" => Ok(Status::Errored),
        _ => Err(GraphRestoreError::new(format!(
            "restore_graph: node '{id}' has invalid status '{status}'"
        ))),
    }
}

fn ensure_quiescent_status(status: Status, id: &str, op: &str) -> RestoreResult<()> {
    if matches!(status, Status::Pending | Status::Dirty) {
        return Err(GraphRestoreError::new(format!(
            "{op}: node '{id}' has non-quiescent status '{status}' that cannot be checkpoint-restored yet",
            status = status_to_str(status)
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::RestoreFactoryMeta;
    use crate::node::Node;
    use crate::protocol::Message;
    use serde_json::json;

    struct StatefulJsonDescriptor;

    impl GraphRestoreDescriptor for StatefulJsonDescriptor {
        fn ref_(&self) -> &'static str {
            "stateful-json"
        }

        fn define(&self, ctx: RestoreDefineCtx<'_>) -> RestoreResult<RestoreNodeDefinition> {
            assert_eq!(ctx.deps.len(), 1);
            Ok(RestoreNodeDefinition {
                factory: "stateful-json",
                kind: RestoreNodeKind::NodeJson(Rc::new(|ctx: &Ctx| {
                    let mut acc = ctx
                        .state_get::<GraphCheckpointJson>()
                        .and_then(|v| v.as_i64())
                        .unwrap_or(40);
                    for _ in ctx.batch::<GraphCheckpointJson>(0) {
                        acc += 1;
                        ctx.emit(json!(acc));
                    }
                    ctx.state_set(json!(acc));
                })),
                opts: restored_opts(ctx.checkpoint)?,
            })
        }
    }

    #[test]
    fn restore_preserves_ctx_state_for_later_runs() {
        let g = crate::graph::graph();
        let source = g.state_opts(json!(1), GraphNodeOpts::named("source"));
        let mut opts = GraphNodeOpts::named("memo");
        opts.restore = Some(RestoreFactoryMeta::registry_ref("stateful-json"));
        let memo = g.node_opts::<GraphCheckpointJson, _>(
            vec![source.erased()],
            |ctx| {
                let mut acc = ctx
                    .state_get::<GraphCheckpointJson>()
                    .and_then(|v| v.as_i64())
                    .unwrap_or(40);
                for _ in ctx.batch::<GraphCheckpointJson>(0) {
                    acc += 1;
                    ctx.emit(json!(acc));
                }
                ctx.state_set(json!(acc));
            },
            opts,
        );
        let _keep_active = memo.subscribe(|_| {});
        assert_eq!(memo.cache(), Some(json!(41)));

        let checkpoint = g.checkpoint().expect("checkpoint succeeds");
        let registry = restore_registry([
            GraphRestoreEntry::descriptor(StateRestoreDescriptor),
            GraphRestoreEntry::descriptor(StatefulJsonDescriptor),
        ]);
        let restored = restore_graph(checkpoint, RestoreGraphOptions::new(registry))
            .expect("restore succeeds");
        let restored_memo = Node::<GraphCheckpointJson>::from_core(
            restored.find("memo").expect("memo restored").core(),
        );

        assert_eq!(restored_memo.cache(), Some(json!(41)));
        let runtime = restored_memo.erased().checkpoint_runtime();
        assert_eq!(
            runtime
                .ctx_state
                .and_then(|v| v.downcast::<GraphCheckpointJson>().ok())
                .as_deref(),
            Some(&json!(41))
        );
    }

    #[test]
    fn restore_rejects_unsupported_runtime_version_metadata() {
        let g = crate::graph::graph();
        g.state_opts(json!(1), GraphNodeOpts::named("source"));
        let mut checkpoint = g.checkpoint().expect("checkpoint succeeds");
        checkpoint.nodes[0].version = Some(json!({ "level": 0, "counter": 1 }));

        let err = match restore_graph(
            checkpoint,
            RestoreGraphOptions::new(default_restore_registry()),
        ) {
            Ok(_) => panic!("Rust does not silently drop version metadata"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("runtime version metadata"));
    }

    #[test]
    fn checkpoint_and_restore_reject_noncanonical_json_numbers() {
        let g = crate::graph::graph();
        let source = g.state_opts(-0.0_f64, GraphNodeOpts::named("source"));
        let _keep_active = source.subscribe(|_| {});
        let err = g
            .checkpoint()
            .expect_err("negative zero is not strict canonical JSON");
        assert!(err.to_string().contains("strict canonical JSON"));

        let g = crate::graph::graph();
        g.state_opts(json!(1), GraphNodeOpts::named("source"));
        let mut checkpoint = g.checkpoint().expect("checkpoint succeeds");
        checkpoint.nodes[0].value = GraphCheckpointValue::Data {
            data: Value::Number(Number::from_f64(f64::MIN_POSITIVE / 2.0).unwrap()),
        };
        let err = match restore_graph(
            checkpoint,
            RestoreGraphOptions::new(default_restore_registry()),
        ) {
            Ok(_) => panic!("subnormal checkpoint number must fail restore validation"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("subnormal"));
    }

    #[test]
    fn restore_rejects_error_terminal_payload_until_preservation_lands() {
        let g = crate::graph::graph();
        g.state_opts(json!(1), GraphNodeOpts::named("source"));
        let mut checkpoint = g.checkpoint().expect("checkpoint succeeds");
        checkpoint.nodes[0].status = "errored".to_owned();
        checkpoint.nodes[0].terminal = GraphCheckpointTerminal::Error {
            error: json!("boom"),
        };

        let err = match restore_graph(
            checkpoint,
            RestoreGraphOptions::new(default_restore_registry()),
        ) {
            Ok(_) => panic!("ERROR terminal payload restore must fail honestly"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("cannot preserve yet"));
    }

    #[test]
    fn restore_descriptors_reject_unsupported_config_version() {
        let g = crate::graph::graph();
        g.state_opts(json!(1), GraphNodeOpts::named("source"));
        let mut checkpoint = g.checkpoint().expect("checkpoint succeeds");
        if let GraphCheckpointFactory::RegistryRef { config_version, .. } =
            &mut checkpoint.nodes[0].factory
        {
            *config_version = Some(json!("v2"));
        }

        let err = match restore_graph(
            checkpoint,
            RestoreGraphOptions::new(default_restore_registry()),
        ) {
            Ok(_) => panic!("built-in state rejects configVersion"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("configVersion"));
    }

    #[test]
    fn restore_preserves_registry_metadata_for_later_checkpoints() {
        let g = crate::graph::graph();
        let source = g.state_opts(json!(3), GraphNodeOpts::named("source"));
        let mut opts = GraphNodeOpts::named("double");
        opts.restore = Some(
            RestoreFactoryMeta::registry_ref("map").with_config(json!({ "fn": "double-json" })),
        );
        let doubled = g.node_opts::<GraphCheckpointJson, _>(
            vec![source.erased()],
            |ctx| {
                for value in ctx.batch::<GraphCheckpointJson>(0) {
                    ctx.emit(json!(value.as_i64().expect("number input") * 2));
                }
            },
            opts,
        );
        let _keep_active = doubled.subscribe(|_| {});
        let checkpoint = g.checkpoint().expect("checkpoint succeeds");
        let registry = restore_registry([
            GraphRestoreEntry::descriptor(StateRestoreDescriptor),
            GraphRestoreEntry::descriptor(MapJsonRestoreDescriptor),
            GraphRestoreEntry::definition(GraphRestoreDefinition::json("double-json", |value| {
                Ok(json!(value.as_i64().expect("number input") * 2))
            })),
        ]);

        let restored = restore_graph(checkpoint, RestoreGraphOptions::new(registry))
            .expect("restore succeeds");
        let restored_checkpoint = restored.checkpoint().expect("re-checkpoint succeeds");
        let restored_node = restored_checkpoint
            .nodes
            .iter()
            .find(|node| node.id == "double")
            .expect("double checkpoint exists");

        assert_eq!(
            restored_node.factory,
            GraphCheckpointFactory::RegistryRef {
                ref_: "map".to_owned(),
                config: Some(json!({ "fn": "double-json" })),
                config_version: None,
            }
        );
    }

    #[test]
    fn restore_rejects_rust_unsupported_non_string_meta() {
        let g = crate::graph::graph();
        g.state_opts(json!(1), GraphNodeOpts::named("source"));
        let mut checkpoint = g.checkpoint().expect("checkpoint succeeds");
        checkpoint.nodes[0].meta = Some(BTreeMap::from([("json".to_owned(), json!({ "x": 1 }))]));

        let err = match restore_graph(
            checkpoint,
            RestoreGraphOptions::new(default_restore_registry()),
        ) {
            Ok(_) => panic!("Rust graph metadata is string-only"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("meta field 'json'"));
    }

    #[test]
    fn restore_rejects_duplicate_edges() {
        let g = crate::graph::graph();
        let source = g.state_opts(json!(1), GraphNodeOpts::named("source"));
        let mut opts = GraphNodeOpts::named("memo");
        opts.restore = Some(RestoreFactoryMeta::registry_ref("stateful-json"));
        let memo = g.node_opts::<GraphCheckpointJson, _>(
            vec![source.erased()],
            |ctx| {
                for value in ctx.batch::<GraphCheckpointJson>(0) {
                    ctx.emit(value.as_ref().clone());
                }
            },
            opts,
        );
        let _keep_active = memo.subscribe(|_| {});
        let mut checkpoint = g.checkpoint().expect("checkpoint succeeds");
        let edge = checkpoint.edges[0].clone();
        checkpoint.edges.push(edge);

        let registry = restore_registry([
            GraphRestoreEntry::descriptor(StateRestoreDescriptor),
            GraphRestoreEntry::descriptor(StatefulJsonDescriptor),
        ]);
        let err = match restore_graph(checkpoint, RestoreGraphOptions::new(registry)) {
            Ok(_) => panic!("duplicate checkpoint edges must fail"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("duplicate edge"));
    }

    #[test]
    fn checkpoint_and_restore_reject_non_quiescent_statuses() {
        let g = crate::graph::graph();
        let source = g.state_opts(json!(1), GraphNodeOpts::named("source"));
        source.down(vec![Message::Dirty]);

        let err = g
            .checkpoint()
            .expect_err("checkpoint cannot preserve in-flight wave bookkeeping yet");
        assert!(err.to_string().contains("non-quiescent status 'dirty'"));

        let g = crate::graph::graph();
        g.state_opts(json!(1), GraphNodeOpts::named("source"));
        let mut checkpoint = g.checkpoint().expect("checkpoint succeeds");
        checkpoint.nodes[0].status = "pending".to_owned();

        let err = match restore_graph(
            checkpoint,
            RestoreGraphOptions::new(default_restore_registry()),
        ) {
            Ok(_) => panic!("restore must reject non-quiescent checkpoint status"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("non-quiescent status 'pending'"));
    }

    #[test]
    fn restore_rejects_empty_mount_path_without_panicking() {
        let parent = crate::graph::graph();
        let child = crate::graph::graph();
        child.state_opts(json!(1), GraphNodeOpts::named("leaf"));
        parent.mount(child, "child");
        let mut checkpoint = parent.checkpoint().expect("checkpoint succeeds");
        checkpoint.mounts.as_mut().expect("mount exists")[0]
            .at
            .clear();

        let err = match restore_graph(
            checkpoint,
            RestoreGraphOptions::new(default_restore_registry()),
        ) {
            Ok(_) => panic!("empty mount path must fail validation"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("must not be empty"));
    }

    #[test]
    fn checkpoint_rejects_unavailable_error_payload_instead_of_inventing_one() {
        let g = crate::graph::graph();
        let source = g.state_opts(json!(1), GraphNodeOpts::named("source"));
        source.down(vec![Message::Error(Box::new(std::io::Error::other(
            "boom",
        )))]);

        let err = g
            .checkpoint()
            .expect_err("Rust cannot serialize the original GraphError payload yet");
        assert!(err.to_string().contains("ERROR payload"));
    }
}
