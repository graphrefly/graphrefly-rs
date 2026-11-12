//! Graph capture and restore (R-checkpoint@1, graphrefly:D802).
//!
//! [`Graph::checkpoint`] is taken only at a wave boundary and saves, for each node in
//! creation order, its declared dependencies, its current instance's recipe (a
//! registered factory reference and JSON configuration; a node whose instance has no
//! recipe is local-only), the values of its last array and its terminal; for a
//! computed node with a lifetime also its private state, what it received from each
//! dependency and its lifetime's dependencies when a ctx.rewire changed them. Every
//! saved value must be JSON. [`restore_graph`] builds a new graph with no subscribers,
//! each node after its declared dependencies, and runs no recipe function: each node
//! gets its saved state as a seed, and at its first activation its setup runs and its
//! function runs once only if a dependency no longer holds what it had received. The
//! layout is graphrefly-rs's own (graphrefly:D802), tagged
//! `graphrefly-rs/checkpoint@1`; it carries no version evidence, and a checkpoint
//! with any other tag, another package's included, is refused.
//!
//! Restored values and private state are `serde_json::Value`, whatever type the
//! saved graph used: the checkpoint carries JSON, not Rust types, and restore runs no
//! recipe that could decode it. Read a restored node as `serde_json::Value`
//! ([`crate::GraphNode::as_node::<serde_json::Value>()`](crate::GraphNode::as_node));
//! reading it as the original `T` gives `None`. A graph that is meant to round-trip
//! is therefore written over `serde_json::Value` (graphrefly-rs:D9).
//!
//! Checkpoint-owned storage integration is deliberately absent here: callers
//! load/decode checkpoint JSON outside the sync core, then pass the already-loaded
//! value to [`restore_graph`].

use std::any::{Any, TypeId};
use std::cell::RefCell;
use std::cmp::Ordering as CmpOrdering;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::error::Error;
use std::fmt;
use std::rc::Rc;
use std::sync::{Arc, Mutex, OnceLock};

use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};

use crate::ctx::Ctx;
use crate::ctx::DepTerminal;
use crate::dispatcher::NodeFn;
use crate::graph::{Graph, GraphNodeOpts, GraphOptions, RestoreFactoryMeta};
use crate::json::validate_strict_json_value;
use crate::node::{Core, NodeSeed, SeedInput};
use crate::protocol::AnyValue;

/// The tag of this package's checkpoint layout (R-checkpoint@1, graphrefly:D802; review
/// Q5): each package defines its own layout, so the tag names the package. The layout:
/// each node's last-array values, terminal (an ERROR payload included), private state,
/// received inputs and lifetime dependencies; no status, lifecycle or version
/// evidence. A checkpoint with any other tag is refused, the earlier
/// `graphrefly.checkpoint.v2` (a string TS used for a different layout) included.
pub const GRAPH_CHECKPOINT_VERSION: &str = "graphrefly-rs/checkpoint@1";

pub type GraphCheckpointJson = Value;

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
    },
    #[serde(rename = "local-only")]
    LocalOnly { name: String, reason: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// One node of a checkpoint (R-checkpoint@1).
pub struct GraphCheckpointNode {
    /// The node's id.
    pub id: String,
    /// The node's display name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// The current instance's recipe, or local-only.
    pub factory: GraphCheckpointFactory,
    /// The declared dependencies.
    pub deps: Vec<String>,
    /// The current lifetime's dependencies, when a ctx.rewire changed them.
    #[serde(rename = "lifetimeDeps", skip_serializing_if = "Option::is_none")]
    pub lifetime_deps: Option<Vec<String>>,
    /// The values of the node's last array.
    pub values: Vec<GraphCheckpointJson>,
    /// The node's terminal, with an ERROR payload.
    pub terminal: GraphCheckpointTerminal,
    /// A computed node's private state; for a collection's delta node, the
    /// collection's backend (a collection is a node whose backend is its private
    /// state, graphrefly:D802).
    #[serde(rename = "privateState", skip_serializing_if = "Option::is_none")]
    pub private_state: Option<GraphCheckpointJson>,
    /// What a computed node with a lifetime received from each current dependency.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub received: Option<Vec<GraphCheckpointInput>>,
    /// The node's graph metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub meta: Option<BTreeMap<String, GraphCheckpointJson>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// What a node received from one dependency: the values of the dependency's last
/// array and its terminal (R-checkpoint@1).
pub struct GraphCheckpointInput {
    /// The values of the dependency's last array.
    pub values: Vec<GraphCheckpointJson>,
    /// The dependency's terminal.
    pub terminal: GraphCheckpointTerminal,
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
    pub core: Core,
    pub unregistered: bool,
}

#[derive(Debug, Clone)]
pub struct GraphRestoreError(String);

impl GraphRestoreError {
    pub fn new(msg: impl Into<String>) -> Self {
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
type BackendStateContributor = dyn Fn(&str) -> Result<GraphCheckpointJson, String>;
type CustomRestoreFn = dyn Fn(&Graph) -> RestoreResult<Core>;
type CheckpointJsonEncoder =
    dyn Fn(&dyn Any, &str) -> RestoreResult<GraphCheckpointJson> + Send + Sync;

static CHECKPOINT_JSON_ENCODERS: OnceLock<Mutex<HashMap<TypeId, Arc<CheckpointJsonEncoder>>>> =
    OnceLock::new();

thread_local! {
    static BACKEND_STATE_CONTRIBUTORS: RefCell<HashMap<(usize, usize, u64), Rc<BackendStateContributor>>> =
        RefCell::new(HashMap::new());
}

/// Register a host-owned strict-JSON checkpoint encoder for an opaque DATA type.
///
/// This keeps host-language values out of the Rust core while letting a binding
/// such as PyO3 make its own payload wrapper checkpointable under D90.
#[doc(hidden)]
pub fn register_checkpoint_json_encoder<T: 'static>(
    encoder: impl Fn(&T, &str) -> RestoreResult<GraphCheckpointJson> + Send + Sync + 'static,
) {
    let encoders = CHECKPOINT_JSON_ENCODERS.get_or_init(|| Mutex::new(HashMap::new()));
    encoders
        .lock()
        .expect("checkpoint JSON encoder registry poisoned")
        .insert(
            TypeId::of::<T>(),
            Arc::new(move |value, path| {
                let typed = value.downcast_ref::<T>().ok_or_else(|| {
                    GraphRestoreError::new(format!(
                        "checkpoint: value at {path} did not match registered checkpoint encoder type"
                    ))
                })?;
                encoder(typed, path)
            }),
        );
}

/// D160 collection-owned backend checkpoint contributor for graph checkpoint.
pub(crate) fn register_backend_state_contributor(
    core: &Core,
    contributor: Rc<BackendStateContributor>,
) {
    BACKEND_STATE_CONTRIBUTORS.with(|contributors| {
        contributors
            .borrow_mut()
            .insert(core.identity_key(), contributor);
    });
}

pub(crate) fn unregister_backend_state_contributor_key(key: (usize, usize, u64)) {
    BACKEND_STATE_CONTRIBUTORS.with(|contributors| {
        contributors.borrow_mut().remove(&key);
    });
}

fn checkpoint_backend_state(core: &Core, path: &str) -> RestoreResult<Option<GraphCheckpointJson>> {
    let contributor = BACKEND_STATE_CONTRIBUTORS
        .with(|contributors| contributors.borrow().get(&core.identity_key()).cloned());
    contributor
        .map(|contributor| {
            let value = contributor(path).map_err(|err| {
                GraphRestoreError::new(format!("checkpoint: collection backend at {path}: {err}"))
            })?;
            validate_checkpoint_json(&value, path)?;
            Ok(value)
        })
        .transpose()
}

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

/// `GraphRestoreDescriptor` behavior contract.
pub trait GraphRestoreDescriptor {
    fn ref_(&self) -> &str;
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
    pub fn try_new(entries: impl IntoIterator<Item = GraphRestoreEntry>) -> RestoreResult<Self> {
        let mut out = Self::default();
        for entry in entries {
            let key = match &entry {
                GraphRestoreEntry::Descriptor(d) => d.ref_().to_owned(),
                GraphRestoreEntry::Definition(d) => d.ref_.clone(),
            };
            if out.entries.insert(key.clone(), entry).is_some() {
                return Err(GraphRestoreError::new(format!(
                    "duplicate restore registry ref '{key}'"
                )));
            }
        }
        Ok(out)
    }

    pub fn new(entries: impl IntoIterator<Item = GraphRestoreEntry>) -> Self {
        Self::try_new(entries).expect("restore registry must not contain duplicate refs")
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
        GraphRestoreEntry::descriptor(OperatorRestoreDescriptor("of")),
        GraphRestoreEntry::descriptor(OperatorRestoreDescriptor("fromIter")),
        GraphRestoreEntry::descriptor(OperatorRestoreDescriptor("empty")),
        GraphRestoreEntry::descriptor(OperatorRestoreDescriptor("never")),
        GraphRestoreEntry::descriptor(OperatorRestoreDescriptor("throwError")),
        GraphRestoreEntry::descriptor(OperatorRestoreDescriptor("take")),
        GraphRestoreEntry::descriptor(OperatorRestoreDescriptor("skip")),
        GraphRestoreEntry::descriptor(OperatorRestoreDescriptor("elementAt")),
        GraphRestoreEntry::descriptor(OperatorRestoreDescriptor("firstAny")),
        GraphRestoreEntry::descriptor(OperatorRestoreDescriptor("lastAny")),
        GraphRestoreEntry::descriptor(OperatorRestoreDescriptor("merge")),
        GraphRestoreEntry::descriptor(OperatorRestoreDescriptor("valve")),
        GraphRestoreEntry::descriptor(OperatorRestoreDescriptor("takeUntil")),
        GraphRestoreEntry::descriptor(ReactiveListDeltaRestoreDescriptor),
        GraphRestoreEntry::descriptor(ReactiveListSnapshotRestoreDescriptor),
        GraphRestoreEntry::descriptor(ReactiveLogDeltaRestoreDescriptor),
        GraphRestoreEntry::descriptor(ReactiveLogSnapshotRestoreDescriptor),
        GraphRestoreEntry::descriptor(ReactiveMapDeltaRestoreDescriptor),
        GraphRestoreEntry::descriptor(ReactiveMapSnapshotRestoreDescriptor),
        GraphRestoreEntry::descriptor(ReactiveIndexDeltaRestoreDescriptor),
        GraphRestoreEntry::descriptor(ReactiveIndexSnapshotRestoreDescriptor),
        GraphRestoreEntry::descriptor(CollectionSnapshotRequestRestoreDescriptor(
            "reactiveList.snapshotRequest",
        )),
        GraphRestoreEntry::descriptor(CollectionSnapshotRequestRestoreDescriptor(
            "reactiveLog.snapshotRequest",
        )),
        GraphRestoreEntry::descriptor(CollectionSnapshotRequestRestoreDescriptor(
            "reactiveMap.snapshotRequest",
        )),
        GraphRestoreEntry::descriptor(CollectionSnapshotRequestRestoreDescriptor(
            "reactiveIndex.snapshotRequest",
        )),
    ])
}

pub struct RestoreDefineCtx<'a> {
    pub id: &'a str,
    pub deps: &'a [String],
    pub config: Option<&'a GraphCheckpointJson>,
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
    Custom(Rc<CustomRestoreFn>),
    /// A `derived` node: the registry gives the value-level callback, and restore
    /// builds the node with the same sugar and gate as [`crate::Graph::derived`]. A
    /// derived definition brings no setup.
    Derived(Rc<DerivedRestoreFn>),
}

/// The value-level callback a [`RestoreNodeKind::Derived`] definition gives.
pub type DerivedRestoreFn = dyn Fn(&crate::graph::Values<'_>) -> Option<GraphCheckpointJson>;

pub struct RestoreNodeDefinition {
    pub factory: String,
    pub kind: RestoreNodeKind,
    pub opts: GraphNodeOpts,
}

pub struct StateRestoreDescriptor;

impl GraphRestoreDescriptor for StateRestoreDescriptor {
    fn ref_(&self) -> &str {
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
        Ok(RestoreNodeDefinition {
            factory: "state".to_owned(),
            kind: RestoreNodeKind::StateJson,
            opts: restored_opts(ctx.checkpoint)?,
        })
    }
}

/// A built-in operator or source, rebuilt over JSON values from its recipe config
/// (R-checkpoint@1): the operator's own policies and setup come with it.
pub struct OperatorRestoreDescriptor(pub &'static str);

impl OperatorRestoreDescriptor {
    fn operator(
        &self,
        ctx: &RestoreDefineCtx<'_>,
    ) -> RestoreResult<crate::operators::Operator<Value>> {
        let field = |name: &str| ctx.config.and_then(|config| config.get(name)).cloned();
        let count = |name: &str| {
            field(name)
                .and_then(|v| v.as_u64())
                .and_then(|v| usize::try_from(v).ok())
                .ok_or_else(|| not_restorable(ctx.id, &format!("its recipe has no '{name}' count")))
        };
        Ok(match self.0 {
            "of" => crate::sources::of(
                field("value").ok_or_else(|| not_restorable(ctx.id, "its recipe has no value"))?,
            ),
            "fromIter" => crate::sources::from_iter(
                field("items")
                    .and_then(|v| v.as_array().cloned())
                    .ok_or_else(|| not_restorable(ctx.id, "its recipe has no items"))?,
            ),
            "empty" => crate::sources::empty(),
            "never" => crate::sources::never(),
            "throwError" => crate::sources::throw_error(
                field("error")
                    .and_then(|v| v.as_str().map(str::to_owned))
                    .unwrap_or_default(),
            ),
            "take" => crate::operators::take(count("n")?),
            "skip" => crate::operators::skip(count("n")?),
            "elementAt" => crate::operators::element_at(count("index")?),
            "firstAny" => crate::operators::first_any(),
            "lastAny" => crate::operators::last_any(),
            "merge" => crate::operators::merge(),
            "valve" => crate::operators::valve(),
            "takeUntil" => crate::combinators::take_until(),
            other => {
                return Err(not_restorable(
                    ctx.id,
                    &format!("no built-in operator '{other}'"),
                ))
            }
        })
    }
}

impl GraphRestoreDescriptor for OperatorRestoreDescriptor {
    fn ref_(&self) -> &str {
        self.0
    }

    fn define(&self, ctx: RestoreDefineCtx<'_>) -> RestoreResult<RestoreNodeDefinition> {
        let op = self.operator(&ctx)?;
        let mut opts = restored_opts(ctx.checkpoint)?;
        opts.node = op.opts.clone();
        Ok(RestoreNodeDefinition {
            factory: op.factory.to_owned(),
            kind: RestoreNodeKind::NodeJson(op.body.clone()),
            opts,
        })
    }
}

pub struct MapJsonRestoreDescriptor;

impl GraphRestoreDescriptor for MapJsonRestoreDescriptor {
    fn ref_(&self) -> &str {
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
        let fn_ref = config.get("fn").and_then(Value::as_str).ok_or_else(|| {
            GraphRestoreError::new("restore_graph: 'map' config.fn must be a string definition ref")
        })?;
        let definition = ctx.resolve_definition(fn_ref)?;
        let body: NodeFn = Rc::new(move |ctx: &Ctx| {
            let mut emitted = Vec::new();
            for value in ctx.dep(0).values::<GraphCheckpointJson>() {
                match definition.call(value.as_ref()) {
                    Ok(out) => emitted.push(out),
                    Err(err) => panic!("{err}"),
                }
            }
            ctx.emit_all(emitted);
        });
        Ok(RestoreNodeDefinition {
            factory: "map".to_owned(),
            kind: RestoreNodeKind::NodeJson(body),
            opts: restored_opts(ctx.checkpoint)?,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
struct CheckpointJsonOrd(GraphCheckpointJson);

impl Ord for CheckpointJsonOrd {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        let left = strict_canonical_json_bytes_for_ord(&self.0);
        let right = strict_canonical_json_bytes_for_ord(&other.0);
        left.cmp(&right)
    }
}

impl PartialOrd for CheckpointJsonOrd {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

fn strict_canonical_json_bytes_for_ord(value: &GraphCheckpointJson) -> Vec<u8> {
    crate::json::strict_canonical_json_bytes(value)
        .expect("CheckpointJsonOrd is constructed only after strict JSON validation")
}

fn collection_base_id(id: &str, suffix: &str, ref_: &str) -> RestoreResult<String> {
    let Some(base) = id.strip_suffix(suffix) else {
        return Err(GraphRestoreError::new(format!(
            "restore_graph: '{ref_}' node '{id}' must end with '{suffix}'"
        )));
    };
    if base.is_empty() {
        return Err(GraphRestoreError::new(format!(
            "restore_graph: '{ref_}' node '{id}' has no collection name"
        )));
    }
    Ok(base.to_owned())
}

fn reject_collection_config(config: Option<&GraphCheckpointJson>, ref_: &str) -> RestoreResult<()> {
    if config.is_some() {
        return Err(GraphRestoreError::new(format!(
            "restore_graph: '{ref_}' descriptor does not accept config"
        )));
    }
    Ok(())
}

fn collection_log_max_size_config(
    config: Option<&GraphCheckpointJson>,
    ref_: &str,
) -> RestoreResult<Option<usize>> {
    let Some(config) = config else {
        return Ok(None);
    };
    let GraphCheckpointJson::Object(config) = config else {
        return Err(GraphRestoreError::new(format!(
            "restore_graph: '{ref_}' descriptor requires object config"
        )));
    };
    if config.is_empty() {
        return Ok(None);
    }
    if config.len() != 1 || !config.contains_key("maxSize") {
        return Err(GraphRestoreError::new(format!(
            "restore_graph: '{ref_}' config only accepts maxSize"
        )));
    }
    let max_size = config
        .get("maxSize")
        .expect("checked")
        .as_u64()
        .ok_or_else(|| {
            GraphRestoreError::new(format!(
                "restore_graph: '{ref_}' config.maxSize must be a positive integer"
            ))
        })?;
    if max_size == 0 {
        return Err(GraphRestoreError::new(format!(
            "restore_graph: '{ref_}' config.maxSize must be a positive integer"
        )));
    }
    usize::try_from(max_size).map(Some).map_err(|_| {
        GraphRestoreError::new(format!(
            "restore_graph: '{ref_}' config.maxSize exceeds usize"
        ))
    })
}

fn backend_array(
    node: &GraphCheckpointNode,
    ref_: &str,
) -> RestoreResult<Vec<GraphCheckpointJson>> {
    let Some(GraphCheckpointJson::Array(items)) = &node.private_state else {
        return Err(GraphRestoreError::new(format!(
            "restore_graph: '{ref_}' node '{}' requires an array backend as its privateState",
            node.id
        )));
    };
    Ok(items.clone())
}

fn map_entries(
    node: &GraphCheckpointNode,
    ref_: &str,
) -> RestoreResult<Vec<(CheckpointJsonOrd, GraphCheckpointJson)>> {
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for (i, entry) in backend_array(node, ref_)?.into_iter().enumerate() {
        let GraphCheckpointJson::Array(pair) = entry else {
            return Err(GraphRestoreError::new(format!(
                "restore_graph: {}.privateState[{i}] must be a [key,value] map entry",
                node.id
            )));
        };
        if pair.len() != 2 {
            return Err(GraphRestoreError::new(format!(
                "restore_graph: {}.privateState[{i}] must be a [key,value] map entry",
                node.id
            )));
        }
        let key = CheckpointJsonOrd(pair[0].clone());
        if !seen.insert(key.clone()) {
            return Err(GraphRestoreError::new(format!(
                "restore_graph: {}.privateState[{i}][0] duplicates an earlier map key",
                node.id
            )));
        }
        out.push((key, pair[1].clone()));
    }
    Ok(out)
}

fn index_rows(
    node: &GraphCheckpointNode,
    ref_: &str,
) -> RestoreResult<
    Vec<
        crate::data_structures::IndexRow<CheckpointJsonOrd, CheckpointJsonOrd, GraphCheckpointJson>,
    >,
> {
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    for (i, row) in backend_array(node, ref_)?.into_iter().enumerate() {
        let GraphCheckpointJson::Object(obj) = row else {
            return Err(GraphRestoreError::new(format!(
                "restore_graph: {}.privateState[{i}] must be an index row object",
                node.id
            )));
        };
        if obj.len() != 3
            || !obj.contains_key("primary")
            || !obj.contains_key("secondary")
            || !obj.contains_key("value")
        {
            return Err(GraphRestoreError::new(format!(
                "restore_graph: {}.privateState[{i}] must contain primary, secondary, and value",
                node.id
            )));
        }
        let primary = CheckpointJsonOrd(obj.get("primary").expect("checked").clone());
        if !seen.insert(primary.clone()) {
            return Err(GraphRestoreError::new(format!(
                "restore_graph: {}.privateState[{i}].primary duplicates an earlier index row",
                node.id
            )));
        }
        out.push(crate::data_structures::IndexRow {
            primary,
            secondary: CheckpointJsonOrd(obj.get("secondary").expect("checked").clone()),
            value: obj.get("value").expect("checked").clone(),
        });
    }
    Ok(out)
}

pub struct ReactiveListDeltaRestoreDescriptor;
pub struct ReactiveListSnapshotRestoreDescriptor;
pub struct ReactiveLogDeltaRestoreDescriptor;
pub struct ReactiveLogSnapshotRestoreDescriptor;
pub struct ReactiveMapDeltaRestoreDescriptor;
pub struct ReactiveMapSnapshotRestoreDescriptor;
pub struct ReactiveIndexDeltaRestoreDescriptor;
pub struct ReactiveIndexSnapshotRestoreDescriptor;

/// Restores a collection's snapshot request state, which the collection's delta
/// restore rebuilds with the collection (graphrefly-rs:D1 (1)).
struct CollectionSnapshotRequestRestoreDescriptor(&'static str);

impl GraphRestoreDescriptor for CollectionSnapshotRequestRestoreDescriptor {
    fn ref_(&self) -> &str {
        self.0
    }

    fn define(&self, ctx: RestoreDefineCtx<'_>) -> RestoreResult<RestoreNodeDefinition> {
        collection_existing_definition(ctx, self.0, ".snapshotRequest")
    }
}

impl GraphRestoreDescriptor for ReactiveListDeltaRestoreDescriptor {
    fn ref_(&self) -> &str {
        "reactiveList.delta"
    }

    fn define(&self, ctx: RestoreDefineCtx<'_>) -> RestoreResult<RestoreNodeDefinition> {
        let max_size = collection_log_max_size_config(ctx.config, self.ref_())?;
        collection_delta_definition(
            ctx,
            "reactiveList.delta",
            ".delta",
            true,
            move |graph: &Graph, base: String, node: &GraphCheckpointNode| {
                let state = backend_array(node, "reactiveList.delta")?;
                if max_size.is_some_and(|max_size| state.len() > max_size) {
                    return Err(not_restorable(
                        &node.id,
                        "its saved list exceeds its recipe's maxSize",
                    ));
                }
                let mut options = crate::data_structures::ReactiveListOptions::named(base);
                if let Some(max_size) = max_size {
                    options = options.max_size(max_size);
                }
                let collection = crate::data_structures::reactive_list::<GraphCheckpointJson>(
                    graph, state, options,
                );
                Ok(collection.delta.erased())
            },
        )
    }
}

impl GraphRestoreDescriptor for ReactiveListSnapshotRestoreDescriptor {
    fn ref_(&self) -> &str {
        "reactiveList.snapshot"
    }

    fn define(&self, ctx: RestoreDefineCtx<'_>) -> RestoreResult<RestoreNodeDefinition> {
        collection_existing_definition(ctx, "reactiveList.snapshot", ".snapshot")
    }
}

impl GraphRestoreDescriptor for ReactiveLogDeltaRestoreDescriptor {
    fn ref_(&self) -> &str {
        "reactiveLog.delta"
    }

    fn define(&self, ctx: RestoreDefineCtx<'_>) -> RestoreResult<RestoreNodeDefinition> {
        let max_size = collection_log_max_size_config(ctx.config, self.ref_())?;
        collection_delta_definition(
            ctx,
            "reactiveLog.delta",
            ".delta",
            true,
            move |graph: &Graph, base: String, node: &GraphCheckpointNode| {
                let state = backend_array(node, "reactiveLog.delta")?;
                if let Some(max_size) = max_size {
                    if state.len() > max_size {
                        return Err(GraphRestoreError::new(format!(
                        "restore_graph: 'reactiveLog.delta' node '{}' privateState backend exceeds config.maxSize",
                        node.id
                    )));
                    }
                }
                let mut options = crate::data_structures::ReactiveLogOptions::named(base);
                if let Some(max_size) = max_size {
                    options = options.max_size(max_size);
                }
                let collection = crate::data_structures::reactive_log::<GraphCheckpointJson>(
                    graph, state, options,
                );
                Ok(collection.delta.erased())
            },
        )
    }
}

impl GraphRestoreDescriptor for ReactiveLogSnapshotRestoreDescriptor {
    fn ref_(&self) -> &str {
        "reactiveLog.snapshot"
    }

    fn define(&self, ctx: RestoreDefineCtx<'_>) -> RestoreResult<RestoreNodeDefinition> {
        collection_existing_definition(ctx, "reactiveLog.snapshot", ".snapshot")
    }
}

impl GraphRestoreDescriptor for ReactiveMapDeltaRestoreDescriptor {
    fn ref_(&self) -> &str {
        "reactiveMap.delta"
    }

    fn define(&self, ctx: RestoreDefineCtx<'_>) -> RestoreResult<RestoreNodeDefinition> {
        collection_delta_definition(
            ctx,
            "reactiveMap.delta",
            ".delta",
            false,
            |graph: &Graph, base: String, node: &GraphCheckpointNode| {
                let collection =
                    crate::data_structures::reactive_map::<CheckpointJsonOrd, GraphCheckpointJson>(
                        graph,
                        map_entries(node, "reactiveMap.delta")?,
                        crate::data_structures::ReactiveMapOptions::named(base),
                    );
                Ok(collection.delta.erased())
            },
        )
    }
}

impl GraphRestoreDescriptor for ReactiveMapSnapshotRestoreDescriptor {
    fn ref_(&self) -> &str {
        "reactiveMap.snapshot"
    }

    fn define(&self, ctx: RestoreDefineCtx<'_>) -> RestoreResult<RestoreNodeDefinition> {
        collection_existing_definition(ctx, "reactiveMap.snapshot", ".snapshot")
    }
}

impl GraphRestoreDescriptor for ReactiveIndexDeltaRestoreDescriptor {
    fn ref_(&self) -> &str {
        "reactiveIndex.delta"
    }

    fn define(&self, ctx: RestoreDefineCtx<'_>) -> RestoreResult<RestoreNodeDefinition> {
        collection_delta_definition(
            ctx,
            "reactiveIndex.delta",
            ".delta",
            false,
            |graph: &Graph, base: String, node: &GraphCheckpointNode| {
                let collection = crate::data_structures::reactive_index::<
                    CheckpointJsonOrd,
                    CheckpointJsonOrd,
                    GraphCheckpointJson,
                >(
                    graph,
                    index_rows(node, "reactiveIndex.delta")?,
                    crate::data_structures::ReactiveIndexOptions::named(base),
                );
                Ok(collection.delta.erased())
            },
        )
    }
}

impl GraphRestoreDescriptor for ReactiveIndexSnapshotRestoreDescriptor {
    fn ref_(&self) -> &str {
        "reactiveIndex.snapshot"
    }

    fn define(&self, ctx: RestoreDefineCtx<'_>) -> RestoreResult<RestoreNodeDefinition> {
        collection_existing_definition(ctx, "reactiveIndex.snapshot", ".snapshot")
    }
}

fn collection_delta_definition(
    ctx: RestoreDefineCtx<'_>,
    ref_: &'static str,
    suffix: &str,
    allow_config: bool,
    restore: impl Fn(&Graph, String, &GraphCheckpointNode) -> RestoreResult<Core> + 'static,
) -> RestoreResult<RestoreNodeDefinition> {
    if !ctx.deps.is_empty() {
        return Err(GraphRestoreError::new(format!(
            "restore_graph: '{ref_}' node '{}' cannot restore deps",
            ctx.id
        )));
    }
    if !allow_config {
        reject_collection_config(ctx.config, ref_)?;
    }
    let base = collection_base_id(ctx.id, suffix, ref_)?;
    let checkpoint = ctx.checkpoint.clone();
    Ok(RestoreNodeDefinition {
        factory: ref_.to_owned(),
        kind: RestoreNodeKind::Custom(Rc::new(move |graph| {
            restore(graph, base.clone(), &checkpoint)
        })),
        opts: restored_opts(ctx.checkpoint)?,
    })
}

fn collection_existing_definition(
    ctx: RestoreDefineCtx<'_>,
    ref_: &'static str,
    suffix: &str,
) -> RestoreResult<RestoreNodeDefinition> {
    reject_collection_config(ctx.config, ref_)?;
    let id = ctx.id.to_owned();
    let _base = collection_base_id(ctx.id, suffix, ref_)?;
    Ok(RestoreNodeDefinition {
        factory: ref_.to_owned(),
        kind: RestoreNodeKind::Custom(Rc::new(move |graph| {
            graph.find(&id).map(|node| node.core()).ok_or_else(|| {
                GraphRestoreError::new(format!(
                    "restore_graph: '{ref_}' node '{id}' was not restored with its collection"
                ))
            })
        })),
        opts: restored_opts(ctx.checkpoint)?,
    })
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
    /// Captures the graph at a wave boundary (R-checkpoint@1); refused inside a wave or
    /// a batch body, and for values or private state that are not JSON.
    pub fn checkpoint(&self) -> RestoreResult<GraphCheckpoint> {
        checkpoint_graph(self)
    }
}

/// Builds a new graph from `checkpoint` (R-checkpoint@1). No recipe function runs;
/// each node gets its saved state as a seed. Restored values and private state are
/// `serde_json::Value`: a node saved as `State<i32>` reads back through
/// `as_node::<serde_json::Value>()`, and `as_state::<i32>()` gives no value (see the
/// module docs).
pub fn restore_graph(
    checkpoint: GraphCheckpoint,
    options: RestoreGraphOptions,
) -> RestoreResult<Graph> {
    let prepared = prepare_checkpoint(&checkpoint, &options.registry, "checkpoint", None)?;
    construct_prepared(&prepared, &options)
}

fn checkpoint_graph(graph: &Graph) -> RestoreResult<GraphCheckpoint> {
    // A checkpoint is taken only at a wave boundary (R-checkpoint@1).
    let domain = graph.domain();
    if crate::node::in_wave(&domain) {
        return Err(GraphRestoreError::new(
            "checkpoint: refused inside a wave (R-checkpoint)",
        ));
    }
    if crate::batch::collecting_batch(&domain) {
        return Err(GraphRestoreError::new(
            "checkpoint: refused inside a batch body (R-checkpoint)",
        ));
    }
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
                core: dep,
                unregistered: true,
            });
        }
        i += 1;
    }
    let mut nodes = Vec::with_capacity(entries.len());
    let mut edges = Vec::new();
    let id_of = |core: &Core, owner: &str| -> RestoreResult<String> {
        entries
            .iter()
            .find(|candidate| candidate.core.ptr_eq(core))
            .map(|candidate| candidate.id.clone())
            .ok_or_else(|| {
                GraphRestoreError::new(format!(
                    "checkpoint: node '{owner}' has an unregistered dep; local-only auto-discovery restore is not implemented"
                ))
            })
    };
    for entry in &entries {
        let runtime = entry.core.checkpoint_runtime();
        let dep_ids = runtime
            .declared
            .iter()
            .map(|dep| id_of(dep, &entry.id))
            .collect::<RestoreResult<Vec<_>>>()?;
        let lifetime_deps = runtime
            .lifetime_deps
            .as_ref()
            .map(|deps| {
                deps.iter()
                    .map(|dep| id_of(dep, &entry.id))
                    .collect::<RestoreResult<Vec<_>>>()
            })
            .transpose()?;
        edges.extend(dep_ids.iter().map(|dep| GraphCheckpointEdge {
            from: dep.clone(),
            to: entry.id.clone(),
        }));
        let backend = checkpoint_backend_state(&entry.core, &format!("{}.privateState", entry.id))
            .map_err(|_| not_restorable(&entry.id, "its private state is not JSON"))?;
        let helper = is_non_authoritative_helper(entry.meta.get("kind").map(String::as_str));
        let values = if helper {
            Vec::new()
        } else {
            values_to_json(&runtime.values, &format!("{}.values", entry.id))
                .map_err(|_| not_restorable(&entry.id, "its retained values are not JSON"))?
        };
        let private_state = match (backend, &runtime.private_state, helper) {
            (Some(backend), _, _) => Some(backend),
            (None, Some(state), false) => Some(
                any_to_json(state, &format!("{}.privateState", entry.id))
                    .map_err(|_| not_restorable(&entry.id, "its private state is not JSON"))?,
            ),
            _ => None,
        };
        let received = runtime
            .received
            .as_ref()
            .map(|inputs| {
                inputs
                    .iter()
                    .enumerate()
                    .map(|(i, input)| {
                        Ok(GraphCheckpointInput {
                            values: if helper {
                                Vec::new()
                            } else {
                                values_to_json(
                                    &input.values,
                                    &format!("{}.received[{i}]", entry.id),
                                )
                                .map_err(|_| {
                                    not_restorable(&entry.id, "the values it received are not JSON")
                                })?
                            },
                            terminal: terminal_to_json(input.ended.as_ref()),
                        })
                    })
                    .collect::<RestoreResult<Vec<_>>>()
            })
            .transpose()?;
        nodes.push(GraphCheckpointNode {
            id: entry.id.clone(),
            name: entry.name.clone(),
            factory: checkpoint_factory(entry, runtime.is_state),
            deps: dep_ids,
            lifetime_deps,
            values,
            terminal: terminal_to_json(runtime.ended.as_ref()),
            private_state,
            received,
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

/// Nodes whose value a checkpoint does not record: collection helpers rebuilt from
/// their backend.
fn is_non_authoritative_helper(kind: Option<&str>) -> bool {
    matches!(
        kind,
        Some(
            "collection_delta"
                | "collection_snapshot"
                | "collection_snapshot_request"
                | "collection_view_delta"
                | "collection_view_snapshot"
                | "collection_view_snapshot_request"
        )
    )
}

fn checkpoint_factory(entry: &CheckpointEntry, is_state: bool) -> GraphCheckpointFactory {
    if entry.unregistered {
        return GraphCheckpointFactory::LocalOnly {
            name: entry.factory.clone(),
            reason: "node is an unregistered live dependency auto-discovered from topology"
                .to_owned(),
        };
    }
    // The current instance's recipe (R-node): a replace without one makes the node
    // local-only.
    if let Some(recipe) = entry
        .core
        .recipe()
        .as_ref()
        .and_then(RestoreFactoryMeta::from_recipe)
    {
        return GraphCheckpointFactory::RegistryRef {
            ref_: recipe.ref_,
            config: recipe.config,
        };
    }
    if is_state && entry.factory == "state" {
        return GraphCheckpointFactory::RegistryRef {
            ref_: "state".to_owned(),
            config: None,
        };
    }
    GraphCheckpointFactory::LocalOnly {
        name: entry.factory.clone(),
        reason: "built or replaced without a recipe".to_owned(),
    }
}

/// The refusal of a node that a checkpoint cannot carry or a restore cannot rebuild
/// (R-checkpoint@1): `not restorable: <id>: <reason>`, the same text in every package.
fn not_restorable(id: &str, reason: &str) -> GraphRestoreError {
    GraphRestoreError::new(format!("not restorable: {id}: {reason}"))
}

fn values_to_json(values: &[AnyValue], path: &str) -> RestoreResult<Vec<GraphCheckpointJson>> {
    values
        .iter()
        .enumerate()
        .map(|(i, v)| any_to_json(v, &format!("{path}[{i}]")))
        .collect()
}

fn terminal_to_json(ended: Option<&DepTerminal>) -> GraphCheckpointTerminal {
    match ended {
        None => GraphCheckpointTerminal::None,
        Some(DepTerminal::Complete) => GraphCheckpointTerminal::Complete,
        Some(DepTerminal::Error(message)) => GraphCheckpointTerminal::Error {
            error: GraphCheckpointJson::String(message.to_string()),
        },
    }
}

fn terminal_from_json(terminal: &GraphCheckpointTerminal) -> Option<DepTerminal> {
    match terminal {
        GraphCheckpointTerminal::None => None,
        GraphCheckpointTerminal::Complete => Some(DepTerminal::Complete),
        GraphCheckpointTerminal::Error { error } => Some(DepTerminal::Error(
            match error {
                GraphCheckpointJson::String(message) => message.clone(),
                other => other.to_string(),
            }
            .into(),
        )),
    }
}

/// Whether two value lists hold the same JSON (R-checkpoint: a restored node compares
/// what it received with what its dependencies hold at its first activation).
pub(crate) fn same_values(a: &[AnyValue], b: &[AnyValue]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b).all(|(x, y)| {
            Rc::ptr_eq(x, y)
                || matches!(
                    (any_to_json(x, "compare"), any_to_json(y, "compare")),
                    (Ok(x), Ok(y)) if x == y
                )
        })
}

/// `value` as checkpoint JSON, if it is one of the types a checkpoint carries; an
/// operator's recipe config is only made from such a value.
pub(crate) fn json_of<T: 'static>(value: &T) -> Option<GraphCheckpointJson> {
    dyn_to_json(value, "recipe").ok()
}

fn any_to_json(value: &AnyValue, path: &str) -> RestoreResult<GraphCheckpointJson> {
    dyn_to_json(value.as_ref(), path)
}

fn dyn_to_json(value: &dyn std::any::Any, path: &str) -> RestoreResult<GraphCheckpointJson> {
    let encoder = CHECKPOINT_JSON_ENCODERS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|_| GraphRestoreError::new("checkpoint JSON encoder registry poisoned"))?
        .get(&value.type_id())
        .cloned();
    if let Some(encoder) = encoder {
        let out = encoder(value, path)?;
        validate_checkpoint_json(&out, path)?;
        return Ok(out);
    }
    if let Some(v) = value.downcast_ref::<GraphCheckpointJson>() {
        validate_checkpoint_json(v, path)?;
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
            validate_checkpoint_json(&out, path)?;
            return Ok(out);
        }
    }
    Err(GraphRestoreError::new(format!(
        "checkpoint: value at {path} is not strict JSON compatible"
    )))
}

fn validate_checkpoint_json(value: &GraphCheckpointJson, path: &str) -> RestoreResult<()> {
    validate_strict_json_value(value, path)
        .map_err(|err| GraphRestoreError::new(format!("checkpoint: {err}")))
}

pub fn restored_opts(node: &GraphCheckpointNode) -> RestoreResult<GraphNodeOpts> {
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
        GraphCheckpointFactory::RegistryRef { ref_, config } => Some(RestoreFactoryMeta {
            ref_: ref_.clone(),
            config: config.clone(),
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
            GraphCheckpointFactory::LocalOnly { .. } => {
                return Err(not_restorable(
                    &node.id,
                    "local-only, built or replaced without a recipe",
                ));
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
        validate_checkpoint_node_json(node)?;
        if let Some(received) = &node.received {
            let current = node.lifetime_deps.as_ref().unwrap_or(&node.deps);
            if received.len() != current.len() {
                return Err(GraphRestoreError::new(format!(
                    "restore_graph: node '{}' received records do not match its dependencies",
                    node.id
                )));
            }
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
        config: Some(config),
        ..
    } = &node.factory
    {
        validate_checkpoint_json(config, &format!("{}.factory.config", node.id))?;
    }
    for (i, value) in node.values.iter().enumerate() {
        validate_checkpoint_json(value, &format!("{}.values[{i}]", node.id))?;
    }
    if let Some(state) = &node.private_state {
        validate_checkpoint_json(state, &format!("{}.privateState", node.id))?;
    }
    for (i, input) in node.received.iter().flatten().enumerate() {
        for value in &input.values {
            validate_checkpoint_json(value, &format!("{}.received[{i}]", node.id))?;
        }
    }
    if let GraphCheckpointTerminal::Error { error } = &node.terminal {
        validate_checkpoint_json(error, &format!("{}.terminal.error", node.id))?;
    }
    if let Some(meta) = &node.meta {
        for (key, value) in meta {
            validate_checkpoint_json(value, &format!("{}.meta.{key}", node.id))?;
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
    // Each node after its declared dependencies, and otherwise in checkpoint order.
    for node in &prepared.checkpoint.nodes {
        build_node(
            &node.id,
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
    // No recipe function runs: each node gets its saved state as a seed.
    let json_values = |values: &[GraphCheckpointJson]| -> Vec<AnyValue> {
        values
            .iter()
            .map(|v| Rc::new(v.clone()) as AnyValue)
            .collect()
    };
    for (id, core) in &built {
        let node = &prepared
            .nodes
            .get(id)
            .expect("built map only contains prepared nodes")
            .checkpoint;
        let lifetime_deps = node
            .lifetime_deps
            .as_ref()
            .map(|ids| {
                ids.iter()
                    .map(|dep| {
                        built.get(dep).cloned().ok_or_else(|| {
                            GraphRestoreError::new(format!(
                                "restore_graph: node '{id}' lifetime dependency '{dep}' is missing"
                            ))
                        })
                    })
                    .collect::<RestoreResult<Vec<_>>>()
            })
            .transpose()?;
        // A computed node saved with a lifetime (private state, a lifetime's own
        // dependencies, or values while it has not ended) must carry what it received;
        // without it the checkpoint is refused, never completed from its dependencies'
        // values (R-checkpoint@1).
        let saved_lifetime = node.private_state.is_some()
            || node.lifetime_deps.is_some()
            || (matches!(node.terminal, GraphCheckpointTerminal::None) && !node.values.is_empty());
        if !core.is_state() && saved_lifetime && node.received.is_none() {
            return Err(GraphRestoreError::new(format!(
                "restore_graph: node '{id}' lacks the record of what it received (R-checkpoint)"
            )));
        }
        core.restore_seed(NodeSeed {
            values: json_values(&node.values),
            ended: terminal_from_json(&node.terminal),
            private_state: node
                .private_state
                .as_ref()
                .map(|state| Rc::new(state.clone()) as AnyValue),
            lifetime_deps,
            received: node.received.as_ref().map(|inputs| {
                inputs
                    .iter()
                    .map(|input| SeedInput {
                        values: json_values(&input.values),
                        ended: terminal_from_json(&input.terminal),
                    })
                    .collect()
            }),
            // The note holds what the node had received (R-checkpoint@1).
            note: node.received.as_ref().map(|inputs| {
                Rc::new(serde_json::to_value(inputs).unwrap_or(GraphCheckpointJson::Null))
                    as AnyValue
            }),
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
    let config = match &item.checkpoint.factory {
        GraphCheckpointFactory::RegistryRef { config, .. } => config.as_ref(),
        GraphCheckpointFactory::LocalOnly { .. } => None,
    };
    let definition = item.descriptor.define(RestoreDefineCtx {
        id,
        deps: &item.deps,
        config,
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
        RestoreNodeKind::Custom(restore) => restore(graph)?,
        RestoreNodeKind::Derived(callback) => {
            if definition.opts.node.setup.is_some() {
                return Err(not_restorable(
                    id,
                    "a derived definition gives a value-level callback and no setup",
                ));
            }
            let body: NodeFn = Rc::new(move |ctx: &Ctx| crate::graph::run_derived(ctx, &*callback));
            graph
                .restore_node_with_id(
                    id.to_owned(),
                    definition.factory,
                    deps.clone(),
                    body,
                    definition.opts,
                )
                .erased()
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::RestoreFactoryMeta;
    use crate::node::Node;
    use crate::protocol::Message;
    use serde_json::json;

    struct StatefulJsonDescriptor;

    impl GraphRestoreDescriptor for StatefulJsonDescriptor {
        fn ref_(&self) -> &str {
            "stateful-json"
        }

        fn define(&self, ctx: RestoreDefineCtx<'_>) -> RestoreResult<RestoreNodeDefinition> {
            assert_eq!(ctx.deps.len(), 1);
            Ok(RestoreNodeDefinition {
                factory: "stateful-json".to_owned(),
                kind: RestoreNodeKind::NodeJson(Rc::new(|ctx: &Ctx| {
                    let mut acc = ctx
                        .state_get::<GraphCheckpointJson>()
                        .and_then(|v| v.as_i64())
                        .unwrap_or(40);
                    let mut emitted = Vec::new();
                    for _ in ctx.dep(0).values::<GraphCheckpointJson>() {
                        acc += 1;
                        emitted.push(json!(acc));
                    }
                    ctx.emit_all(emitted);
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
                let mut emitted = Vec::new();
                for _ in ctx.dep(0).values::<GraphCheckpointJson>() {
                    acc += 1;
                    emitted.push(json!(acc));
                }
                ctx.emit_all(emitted);
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
        let restored_source = Node::<GraphCheckpointJson>::from_core(
            restored.find("source").expect("source restored").core(),
        );

        assert_eq!(restored_memo.cache(), Some(json!(41)));
        // Captured again before anything subscribes, the seed is the saved state.
        let restored_checkpoint = restored.checkpoint().expect("re-checkpoint succeeds");
        let memo_checkpoint = restored_checkpoint
            .nodes
            .iter()
            .find(|node| node.id == "memo")
            .expect("memo checkpoint exists");
        assert_eq!(memo_checkpoint.private_state, Some(json!(41)));
        // A restored node receives nothing until it is activated.
        let _sub = restored_memo.subscribe(|_| {});
        assert_eq!(
            restored_memo.cache(),
            Some(json!(41)),
            "unchanged source: no run"
        );
        restored_source.set(json!(2));
        assert_eq!(restored_memo.cache(), Some(json!(42)));
        let runtime = restored_memo.erased().checkpoint_runtime();
        assert_eq!(
            runtime
                .private_state
                .and_then(|v| v.downcast::<GraphCheckpointJson>().ok())
                .as_deref(),
            Some(&json!(42))
        );
    }

    #[test]
    fn checkpoint_and_restore_reject_noncanonical_json_numbers() {
        let g = crate::graph::graph();
        let source = g.state_opts(-0.0_f64, GraphNodeOpts::named("source"));
        let _keep_active = source.subscribe(|_| {});
        let err = g
            .checkpoint()
            .expect_err("negative zero is not strict canonical JSON");
        assert_eq!(
            err.to_string(),
            "not restorable: source: its retained values are not JSON"
        );

        let g = crate::graph::graph();
        g.state_opts(json!(1), GraphNodeOpts::named("source"));
        let mut checkpoint = g.checkpoint().expect("checkpoint succeeds");
        checkpoint.nodes[0].values = vec![Value::Number(
            Number::from_f64(f64::MIN_POSITIVE / 2.0).unwrap(),
        )];
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
                let mut emitted = Vec::new();
                for value in ctx.dep(0).values::<GraphCheckpointJson>() {
                    emitted.push(json!(value.as_i64().expect("number input") * 2));
                }
                ctx.emit_all(emitted);
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
                let mut emitted = Vec::new();
                for value in ctx.dep(0).values::<GraphCheckpointJson>() {
                    emitted.push(value.as_ref().clone());
                }
                ctx.emit_all(emitted);
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
    fn checkpoint_saves_and_restores_an_error_payload() {
        let g = crate::graph::graph();
        let source = g.state_opts(json!(1), GraphNodeOpts::named("source"));
        source.error("boom");
        let checkpoint = g.checkpoint().expect("checkpoint succeeds");
        assert_eq!(
            checkpoint.nodes[0].terminal,
            GraphCheckpointTerminal::Error {
                error: json!("boom")
            }
        );
        let restored = restore_graph(
            checkpoint,
            RestoreGraphOptions::new(default_restore_registry()),
        )
        .expect("restore succeeds");
        let node = restored.find("source").expect("source restored");
        assert_eq!(node.status().output, crate::OutputStatus::Errored);
    }

    #[test]
    fn checkpoint_is_refused_inside_a_wave_or_a_batch_body() {
        let g = crate::graph::graph();
        let source = g.state_opts(json!(1), GraphNodeOpts::named("source"));
        let in_batch = g.batch(|_| g.checkpoint().map(|_| ()));
        assert!(in_batch.unwrap_err().to_string().contains("batch body"));
        let in_wave = Rc::new(RefCell::new(None));
        let _sub = {
            let (g, in_wave) = (g.clone(), in_wave.clone());
            source.subscribe(move |msgs: &[Message<AnyValue>]| {
                if msgs.iter().any(|m| matches!(m, Message::Data(_))) {
                    *in_wave.borrow_mut() = Some(g.checkpoint().map(|_| ()));
                }
            })
        };
        source.set(json!(2));
        let refused = in_wave.borrow_mut().take().expect("callback ran");
        assert!(refused.unwrap_err().to_string().contains("inside a wave"));
    }
}
