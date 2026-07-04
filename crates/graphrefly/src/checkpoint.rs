//! Fresh graph checkpoint/restore (R-snapshot/R-restore, D83/D94/D116).
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
use crate::dispatcher::NodeFn;
use crate::graph::{Graph, GraphNodeOpts, GraphOptions, RestoreFactoryMeta};
use crate::json::validate_strict_json_value;
use crate::node::{Core, NodeRestoreRuntime, Status};
use crate::protocol::AnyValue;
use crate::versioning::{
    node_version_to_json, validate_node_version_json, verify_restored_node_version,
};

/// `constant` constant.
pub const GRAPH_CHECKPOINT_VERSION: &str = "graphrefly.checkpoint.v1";

/// `GraphCheckpointJson` type alias.
pub type GraphCheckpointJson = Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind")]
/// `GraphCheckpointValue` variants.
pub enum GraphCheckpointValue {
    #[serde(rename = "SENTINEL")]
    /// `Sentinel` variant.
    Sentinel,
    #[serde(rename = "DATA")]
    /// `Data` variant.
    Data {
        /// `data` field for `Data`.
        data: GraphCheckpointJson,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind")]
/// `GraphCheckpointTerminal` variants.
pub enum GraphCheckpointTerminal {
    #[serde(rename = "none")]
    /// `None` variant.
    None,
    #[serde(rename = "COMPLETE")]
    /// `Complete` variant.
    Complete,
    #[serde(rename = "ERROR")]
    /// `Error` variant.
    Error {
        /// `error` field for `Error`.
        error: GraphCheckpointJson,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind")]
/// `GraphCheckpointFactory` variants.
pub enum GraphCheckpointFactory {
    #[serde(rename = "registry-ref")]
    /// `RegistryRef` variant.
    RegistryRef {
        #[serde(rename = "ref")]
        /// `ref_` field for ref.
        ref_: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        /// `config` field for config.
        config: Option<GraphCheckpointJson>,
        #[serde(rename = "configVersion", skip_serializing_if = "Option::is_none")]
        /// `config_version` field for config version.
        config_version: Option<GraphCheckpointJson>,
    },
    #[serde(rename = "local-only")]
    /// `LocalOnly` variant.
    LocalOnly {
        /// `name` field for `LocalOnly`.
        name: String,
        /// `reason` field for `LocalOnly`.
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// `GraphCheckpointNode` data container.
pub struct GraphCheckpointNode {
    /// `id` field for id.
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// `name` field for name.
    pub name: Option<String>,
    /// `factory` field for factory.
    pub factory: GraphCheckpointFactory,
    /// `status` field for status.
    pub status: String,
    /// `deps` field for deps.
    pub deps: Vec<String>,
    /// `value` field for value.
    pub value: GraphCheckpointValue,
    #[serde(rename = "backendState", skip_serializing_if = "Option::is_none")]
    /// `backend_state` field for backend state.
    pub backend_state: Option<GraphCheckpointJson>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// `version` field for version.
    pub version: Option<GraphCheckpointJson>,
    /// `terminal` field for terminal.
    pub terminal: GraphCheckpointTerminal,
    /// `lifecycle` field for lifecycle.
    pub lifecycle: GraphCheckpointLifecycle,
    #[serde(rename = "ctxState")]
    /// `ctx_state` field for ctx state.
    pub ctx_state: GraphCheckpointCtxState,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// `meta` field for meta.
    pub meta: Option<BTreeMap<String, GraphCheckpointJson>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// `GraphCheckpointLifecycle` data container.
pub struct GraphCheckpointLifecycle {
    /// `activated` field for activated.
    pub activated: bool,
    #[serde(rename = "hasCalledFnOnce")]
    /// `has_called_fn_once` field for has called fn once.
    pub has_called_fn_once: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// `GraphCheckpointCtxState` data container.
pub struct GraphCheckpointCtxState {
    /// `persist` field for persist.
    pub persist: bool,
    /// `value` field for value.
    pub value: GraphCheckpointValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// `GraphCheckpointEdge` data container.
pub struct GraphCheckpointEdge {
    /// `from` field for from.
    pub from: String,
    /// `to` field for to.
    pub to: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// `GraphCheckpointMount` data container.
pub struct GraphCheckpointMount {
    /// `at` field for at.
    pub at: String,
    /// `checkpoint` field for checkpoint.
    pub checkpoint: GraphCheckpoint,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// `GraphCheckpoint` data container.
pub struct GraphCheckpoint {
    /// `version` field for version.
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// `name` field for name.
    pub name: Option<String>,
    /// `nodes` field for nodes.
    pub nodes: Vec<GraphCheckpointNode>,
    /// `edges` field for edges.
    pub edges: Vec<GraphCheckpointEdge>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// `mounts` field for mounts.
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
/// `GraphRestoreError` data container.
pub struct GraphRestoreError(String);

impl GraphRestoreError {
    /// Creates or computes `new`.
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

/// `GraphRestoreResult` type alias.
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

pub(crate) fn unregister_backend_state_contributor(core: &Core) {
    unregister_backend_state_contributor_key(core.identity_key());
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
                GraphRestoreError::new(format!("checkpoint: backendState at {path}: {err}"))
            })?;
            validate_checkpoint_json(&value, path)?;
            Ok(value)
        })
        .transpose()
}

#[derive(Clone)]
/// `GraphRestoreDefinition` data container.
pub struct GraphRestoreDefinition {
    /// `ref_` field for ref.
    pub ref_: String,
    f: Rc<JsonDefinitionFn>,
}

impl GraphRestoreDefinition {
    /// Creates or computes `json`.
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
    /// Updates or reads `ref_`.
    fn ref_(&self) -> &str;
    /// Updates or reads `define`.
    fn define(&self, ctx: RestoreDefineCtx<'_>) -> RestoreResult<RestoreNodeDefinition>;
}

#[derive(Clone)]
/// `GraphRestoreEntry` variants.
pub enum GraphRestoreEntry {
    /// `Descriptor` variant.
    Descriptor(Rc<dyn GraphRestoreDescriptor>),
    /// `Definition` variant.
    Definition(GraphRestoreDefinition),
}

impl GraphRestoreEntry {
    /// Creates or computes `descriptor`.
    pub fn descriptor(d: impl GraphRestoreDescriptor + 'static) -> Self {
        Self::Descriptor(Rc::new(d))
    }

    /// Creates or computes `definition`.
    pub fn definition(d: GraphRestoreDefinition) -> Self {
        Self::Definition(d)
    }
}

#[derive(Clone, Default)]
/// `GraphRestoreRegistry` data container.
pub struct GraphRestoreRegistry {
    entries: BTreeMap<String, GraphRestoreEntry>,
}

impl GraphRestoreRegistry {
    /// Creates or computes `try_new`.
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

    /// Creates or computes `new`.
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

/// Creates or computes `restore_registry`.
pub fn restore_registry(
    entries: impl IntoIterator<Item = GraphRestoreEntry>,
) -> GraphRestoreRegistry {
    GraphRestoreRegistry::new(entries)
}

/// Creates or computes `default_restore_registry`.
pub fn default_restore_registry() -> GraphRestoreRegistry {
    GraphRestoreRegistry::new([
        GraphRestoreEntry::descriptor(StateRestoreDescriptor),
        GraphRestoreEntry::descriptor(MapJsonRestoreDescriptor),
        GraphRestoreEntry::descriptor(ReactiveListDeltaRestoreDescriptor),
        GraphRestoreEntry::descriptor(ReactiveListSnapshotRestoreDescriptor),
        GraphRestoreEntry::descriptor(ReactiveLogDeltaRestoreDescriptor),
        GraphRestoreEntry::descriptor(ReactiveLogSnapshotRestoreDescriptor),
        GraphRestoreEntry::descriptor(ReactiveMapDeltaRestoreDescriptor),
        GraphRestoreEntry::descriptor(ReactiveMapSnapshotRestoreDescriptor),
        GraphRestoreEntry::descriptor(ReactiveIndexDeltaRestoreDescriptor),
        GraphRestoreEntry::descriptor(ReactiveIndexSnapshotRestoreDescriptor),
    ])
}

/// `RestoreDefineCtx` data container.
pub struct RestoreDefineCtx<'a> {
    /// `id` field for id.
    pub id: &'a str,
    /// `deps` field for deps.
    pub deps: &'a [String],
    /// `config` field for config.
    pub config: Option<&'a GraphCheckpointJson>,
    /// `config_version` field for config version.
    pub config_version: Option<&'a GraphCheckpointJson>,
    /// `checkpoint` field for checkpoint.
    pub checkpoint: &'a GraphCheckpointNode,
    registry: &'a GraphRestoreRegistry,
}

impl RestoreDefineCtx<'_> {
    /// Updates or reads `resolve_definition`.
    pub fn resolve_definition(&self, ref_: &str) -> RestoreResult<GraphRestoreDefinition> {
        self.registry.definition(ref_).ok_or_else(|| {
            GraphRestoreError::new(format!(
                "restore_graph: missing function definition for '{ref_}' (node '{}')",
                self.id
            ))
        })
    }
}

/// `RestoreNodeKind` variants.
pub enum RestoreNodeKind {
    /// `StateJson` variant.
    StateJson,
    /// `NodeJson` variant.
    NodeJson(NodeFn),
    /// `Custom` variant.
    Custom(Rc<CustomRestoreFn>),
}

/// `RestoreNodeDefinition` data container.
pub struct RestoreNodeDefinition {
    /// `factory` field for factory.
    pub factory: String,
    /// `kind` field for kind.
    pub kind: RestoreNodeKind,
    /// `opts` field for opts.
    pub opts: GraphNodeOpts,
}

/// `StateRestoreDescriptor` data container.
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
        if ctx.config_version.is_some() {
            return Err(GraphRestoreError::new(
                "restore_graph: built-in state descriptor does not accept configVersion",
            ));
        }
        Ok(RestoreNodeDefinition {
            factory: "state".to_owned(),
            kind: RestoreNodeKind::StateJson,
            opts: restored_opts(ctx.checkpoint)?,
        })
    }
}

/// `MapJsonRestoreDescriptor` data container.
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

fn reject_collection_config(
    config: Option<&GraphCheckpointJson>,
    config_version: Option<&GraphCheckpointJson>,
    ref_: &str,
) -> RestoreResult<()> {
    if config.is_some() || config_version.is_some() {
        return Err(GraphRestoreError::new(format!(
            "restore_graph: '{ref_}' descriptor does not accept config"
        )));
    }
    Ok(())
}

fn collection_log_max_size_config(
    config: Option<&GraphCheckpointJson>,
    config_version: Option<&GraphCheckpointJson>,
    ref_: &str,
) -> RestoreResult<Option<usize>> {
    if config_version.is_some() {
        return Err(GraphRestoreError::new(format!(
            "restore_graph: '{ref_}' descriptor does not accept configVersion"
        )));
    }
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
    let Some(GraphCheckpointJson::Array(items)) = &node.backend_state else {
        return Err(GraphRestoreError::new(format!(
            "restore_graph: '{ref_}' node '{}' requires array backendState",
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
                "restore_graph: {}.backendState[{i}] must be a [key,value] map entry",
                node.id
            )));
        };
        if pair.len() != 2 {
            return Err(GraphRestoreError::new(format!(
                "restore_graph: {}.backendState[{i}] must be a [key,value] map entry",
                node.id
            )));
        }
        let key = CheckpointJsonOrd(pair[0].clone());
        if !seen.insert(key.clone()) {
            return Err(GraphRestoreError::new(format!(
                "restore_graph: {}.backendState[{i}][0] duplicates an earlier map key",
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
                "restore_graph: {}.backendState[{i}] must be an index row object",
                node.id
            )));
        };
        if obj.len() != 3
            || !obj.contains_key("primary")
            || !obj.contains_key("secondary")
            || !obj.contains_key("value")
        {
            return Err(GraphRestoreError::new(format!(
                "restore_graph: {}.backendState[{i}] must contain primary, secondary, and value",
                node.id
            )));
        }
        let primary = CheckpointJsonOrd(obj.get("primary").expect("checked").clone());
        if !seen.insert(primary.clone()) {
            return Err(GraphRestoreError::new(format!(
                "restore_graph: {}.backendState[{i}].primary duplicates an earlier index row",
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

/// `ReactiveListDeltaRestoreDescriptor` data container.
pub struct ReactiveListDeltaRestoreDescriptor;
/// `ReactiveListSnapshotRestoreDescriptor` data container.
pub struct ReactiveListSnapshotRestoreDescriptor;
/// `ReactiveLogDeltaRestoreDescriptor` data container.
pub struct ReactiveLogDeltaRestoreDescriptor;
/// `ReactiveLogSnapshotRestoreDescriptor` data container.
pub struct ReactiveLogSnapshotRestoreDescriptor;
/// `ReactiveMapDeltaRestoreDescriptor` data container.
pub struct ReactiveMapDeltaRestoreDescriptor;
/// `ReactiveMapSnapshotRestoreDescriptor` data container.
pub struct ReactiveMapSnapshotRestoreDescriptor;
/// `ReactiveIndexDeltaRestoreDescriptor` data container.
pub struct ReactiveIndexDeltaRestoreDescriptor;
/// `ReactiveIndexSnapshotRestoreDescriptor` data container.
pub struct ReactiveIndexSnapshotRestoreDescriptor;

impl GraphRestoreDescriptor for ReactiveListDeltaRestoreDescriptor {
    fn ref_(&self) -> &str {
        "reactiveList.delta"
    }

    fn define(&self, ctx: RestoreDefineCtx<'_>) -> RestoreResult<RestoreNodeDefinition> {
        collection_delta_definition(
            ctx,
            "reactiveList.delta",
            ".delta",
            false,
            |graph: &Graph, base: String, node: &GraphCheckpointNode| {
                let collection = crate::data_structures::reactive_list::<GraphCheckpointJson>(
                    backend_array(node, "reactiveList.delta")?,
                    crate::data_structures::ReactiveListOptions::named(base).graph(graph.clone()),
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
        let max_size = collection_log_max_size_config(ctx.config, ctx.config_version, self.ref_())?;
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
                        "restore_graph: 'reactiveLog.delta' node '{}' backendState exceeds config.maxSize",
                        node.id
                    )));
                    }
                }
                let mut options =
                    crate::data_structures::ReactiveLogOptions::named(base).graph(graph.clone());
                if let Some(max_size) = max_size {
                    options = options.max_size(max_size);
                }
                let collection =
                    crate::data_structures::reactive_log::<GraphCheckpointJson>(state, options);
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
                        map_entries(node, "reactiveMap.delta")?,
                        crate::data_structures::ReactiveMapOptions::named(base)
                            .graph(graph.clone()),
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
                    index_rows(node, "reactiveIndex.delta")?,
                    crate::data_structures::ReactiveIndexOptions::named(base).graph(graph.clone()),
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
        reject_collection_config(ctx.config, ctx.config_version, ref_)?;
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
    reject_collection_config(ctx.config, ctx.config_version, ref_)?;
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
/// `RestoreGraphOptions` data container.
pub struct RestoreGraphOptions {
    /// `registry` field for registry.
    pub registry: GraphRestoreRegistry,
    /// `graph` field for graph.
    pub graph: GraphOptions,
}

impl RestoreGraphOptions {
    /// Creates or computes `new`.
    pub fn new(registry: GraphRestoreRegistry) -> Self {
        Self {
            registry,
            graph: GraphOptions::default(),
        }
    }
}

impl Graph {
    /// Updates or reads `checkpoint`.
    pub fn checkpoint(&self) -> RestoreResult<GraphCheckpoint> {
        checkpoint_graph(self)
    }
}

/// Creates or computes `restore_graph`.
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
        let backend_state =
            checkpoint_backend_state(&entry.core, &format!("{}.backendState", entry.id))?;
        let non_authoritative_collection_helper =
            is_non_authoritative_collection_helper(entry.meta.get("kind").map(String::as_str));
        nodes.push(GraphCheckpointNode {
            id: entry.id.clone(),
            name: entry.name.clone(),
            factory: checkpoint_factory(entry),
            status: status_to_str(runtime.status).to_owned(),
            deps: dep_ids,
            value: if non_authoritative_collection_helper {
                GraphCheckpointValue::Sentinel
            } else {
                checkpoint_value(runtime.cache.as_ref(), runtime.has_data, &entry.id)?
            },
            backend_state,
            version: runtime.version.as_ref().map(node_version_to_json),
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
                value: if non_authoritative_collection_helper {
                    GraphCheckpointValue::Sentinel
                } else {
                    checkpoint_value(
                        runtime.ctx_state.as_ref(),
                        runtime.ctx_state.is_some(),
                        &format!("{}.ctxState", entry.id),
                    )?
                },
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

fn is_non_authoritative_collection_helper(kind: Option<&str>) -> bool {
    matches!(
        kind,
        Some(
            "collection_delta"
                | "collection_snapshot"
                | "collection_view_delta"
                | "collection_view_snapshot"
        )
    )
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
    let encoder = CHECKPOINT_JSON_ENCODERS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|_| GraphRestoreError::new("checkpoint JSON encoder registry poisoned"))?
        .get(&value.as_ref().type_id())
        .cloned();
    if let Some(encoder) = encoder {
        let out = encoder(value.as_ref(), path)?;
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

/// Creates or computes `restored_opts`.
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
        if let Some(version) = &node.version {
            validate_node_version_json(version, &format!("{}.version", node.id))
                .map_err(|err| GraphRestoreError::new(err.to_string()))?;
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
            validate_checkpoint_json(config, &format!("{}.factory.config", node.id))?;
        }
        if let Some(config_version) = config_version {
            validate_checkpoint_json(
                config_version,
                &format!("{}.factory.configVersion", node.id),
            )?;
        }
    }
    if let GraphCheckpointValue::Data { data } = &node.value {
        validate_checkpoint_json(data, &format!("{}.value", node.id))?;
    }
    if let GraphCheckpointValue::Data { data } = &node.ctx_state.value {
        validate_checkpoint_json(data, &format!("{}.ctxState", node.id))?;
    }
    if let Some(backend_state) = &node.backend_state {
        validate_checkpoint_json(backend_state, &format!("{}.backendState", node.id))?;
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
        let restored_version = verify_restored_node_version(
            &core.versioning_policy(),
            node.version.as_ref(),
            has_data,
            cache.as_ref(),
            &format!("{}.version", node.id),
        )
        .map_err(|err| GraphRestoreError::new(err.to_string()))?;
        core.restore_runtime(NodeRestoreRuntime {
            cache,
            has_data,
            version: restored_version,
            status,
            terminal,
            activated: node.lifecycle.activated,
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
        RestoreNodeKind::Custom(restore) => restore(graph)?,
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
    use crate::versioning::{NodeVersion, NodeVersioningPolicy};
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
        let restored_source = Node::<GraphCheckpointJson>::from_core(
            restored.find("source").expect("source restored").core(),
        );

        assert_eq!(restored_memo.cache(), Some(json!(41)));
        let restored_checkpoint = restored.checkpoint().expect("re-checkpoint succeeds");
        let memo_checkpoint = restored_checkpoint
            .nodes
            .iter()
            .find(|node| node.id == "memo")
            .expect("memo checkpoint exists");
        assert_eq!(
            memo_checkpoint.lifecycle,
            GraphCheckpointLifecycle {
                activated: true,
                has_called_fn_once: true
            }
        );
        let runtime = restored_memo.erased().checkpoint_runtime();
        assert_eq!(
            runtime
                .ctx_state
                .and_then(|v| v.downcast::<GraphCheckpointJson>().ok())
                .as_deref(),
            Some(&json!(41))
        );
        restored_source.set(json!(2));
        assert_eq!(restored_memo.cache(), Some(json!(42)));
        let runtime = restored_memo.erased().checkpoint_runtime();
        assert_eq!(
            runtime
                .ctx_state
                .and_then(|v| v.downcast::<GraphCheckpointJson>().ok())
                .as_deref(),
            Some(&json!(42))
        );
    }

    #[test]
    fn checkpoints_and_restores_node_runtime_versions() {
        let g = crate::graph::graph();
        let source = g.state_opts(json!(1), GraphNodeOpts::named("source"));
        source.set(json!(2));
        let checkpoint = g.checkpoint().expect("checkpoint succeeds");
        assert_eq!(
            checkpoint.nodes[0].version,
            Some(json!({ "level": 0, "counter": 1 }))
        );

        let restored = restore_graph(
            checkpoint,
            RestoreGraphOptions::new(default_restore_registry()),
        )
        .expect("V0 restore succeeds");
        let restored_source = restored.find("source").expect("source restored");
        assert_eq!(
            restored_source.version(),
            Some(NodeVersion::V0 { counter: 1 })
        );

        restored_source.down(vec![Message::Data(Rc::new(json!(3)))]);
        assert_eq!(
            restored_source.version(),
            Some(NodeVersion::V0 { counter: 2 })
        );
    }

    #[test]
    fn restore_rejects_v0_metadata_when_selected_policy_is_not_v0() {
        let g = crate::graph::graph();
        let source = g.state_opts(json!(1), GraphNodeOpts::named("source"));
        source.set(json!(2));
        let checkpoint = g.checkpoint().expect("checkpoint succeeds");

        let disabled_err = match restore_graph(
            checkpoint.clone(),
            RestoreGraphOptions {
                registry: default_restore_registry(),
                graph: GraphOptions {
                    versioning: Some(NodeVersioningPolicy::Disabled),
                    ..GraphOptions::default()
                },
            },
        ) {
            Ok(_) => panic!("V0 metadata must not restore into disabled versioning"),
            Err(err) => err,
        };
        assert!(disabled_err.to_string().contains("versioning is disabled"));

        let level1_err = match restore_graph(
            checkpoint,
            RestoreGraphOptions {
                registry: default_restore_registry(),
                graph: GraphOptions {
                    versioning: Some(NodeVersioningPolicy::Level1 {
                        hash: Some(Rc::new(|bytes: &[u8]| {
                            format!(
                                "h:{}",
                                std::str::from_utf8(bytes).expect("canonical JSON is UTF-8")
                            )
                        })),
                    }),
                    ..GraphOptions::default()
                },
            },
        ) {
            Ok(_) => panic!("V0 metadata must not restore into a V1 lane"),
            Err(err) => err,
        };
        assert!(level1_err
            .to_string()
            .contains("level 0 requires matching node versioning policy"));
    }

    #[test]
    fn restores_v1_only_with_matching_hash_lane() {
        let hash = Rc::new(|bytes: &[u8]| {
            format!(
                "h:{}",
                std::str::from_utf8(bytes).expect("canonical JSON is UTF-8")
            )
        });
        let graph = crate::graph::graph_opts(GraphOptions {
            versioning: Some(NodeVersioningPolicy::Level1 {
                hash: Some(hash.clone()),
            }),
            ..GraphOptions::default()
        });
        let source = graph.state_opts(json!(1), GraphNodeOpts::named("source"));
        source.set(json!(2));
        let checkpoint = graph.checkpoint().expect("checkpoint succeeds");
        assert_eq!(
            checkpoint.nodes[0].version,
            Some(json!({ "level": 1, "counter": 1, "cid": "h:2", "prev": "h:1" }))
        );

        let err = match restore_graph(
            checkpoint.clone(),
            RestoreGraphOptions::new(default_restore_registry()),
        ) {
            Ok(_) => panic!("V1 restore without a matching lane must fail"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("matching node versioning policy"));

        let err = match restore_graph(
            checkpoint.clone(),
            RestoreGraphOptions {
                registry: default_restore_registry(),
                graph: GraphOptions {
                    versioning: Some(NodeVersioningPolicy::Level1 {
                        hash: Some(Rc::new(|bytes: &[u8]| {
                            format!(
                                "other:{}",
                                std::str::from_utf8(bytes).expect("canonical JSON is UTF-8")
                            )
                        })),
                    }),
                    ..GraphOptions::default()
                },
            },
        ) {
            Ok(_) => panic!("wrong hash lane must fail"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("hash policy"));

        let restored = restore_graph(
            checkpoint,
            RestoreGraphOptions {
                registry: default_restore_registry(),
                graph: GraphOptions {
                    versioning: Some(NodeVersioningPolicy::Level1 { hash: Some(hash) }),
                    ..GraphOptions::default()
                },
            },
        )
        .expect("matching V1 lane restores");
        let restored_source = restored.find("source").expect("source restored");
        assert_eq!(
            restored_source.version(),
            Some(NodeVersion::V1 {
                counter: 1,
                cid: "h:2".to_owned(),
                prev: Some("h:1".to_owned()),
            })
        );
        restored_source.down(vec![Message::Data(Rc::new(json!(3)))]);
        assert_eq!(
            restored_source.version(),
            Some(NodeVersion::V1 {
                counter: 2,
                cid: "h:3".to_owned(),
                prev: Some("h:2".to_owned()),
            })
        );
    }

    #[test]
    fn restore_requires_version_metadata_when_versioning_is_enabled() {
        let graph = crate::graph::graph();
        graph.state_opts(json!(1), GraphNodeOpts::named("source"));
        let mut checkpoint = graph.checkpoint().expect("checkpoint succeeds");
        checkpoint.nodes[0].version = None;

        let err = match restore_graph(
            checkpoint,
            RestoreGraphOptions::new(default_restore_registry()),
        ) {
            Ok(_) => panic!("missing version metadata must fail under default V0 versioning"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("version metadata is required"));
    }

    #[test]
    fn restore_rejects_invalid_node_version_metadata_shape() {
        let graph = crate::graph::graph();
        graph.state_opts(json!(1), GraphNodeOpts::named("source"));
        let checkpoint = graph.checkpoint().expect("checkpoint succeeds");

        let mut extra = checkpoint.clone();
        extra.nodes[0].version = Some(json!({ "level": 0, "counter": 1, "extra": true }));
        let err = match restore_graph(extra, RestoreGraphOptions::new(default_restore_registry())) {
            Ok(_) => panic!("extra node version fields must fail honestly"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("unexpected node version fields"));

        let mut unsafe_counter = checkpoint;
        unsafe_counter.nodes[0].version =
            Some(json!({ "level": 0, "counter": 9_007_199_254_740_992u64 }));
        let err = match restore_graph(
            unsafe_counter,
            RestoreGraphOptions::new(default_restore_registry()),
        ) {
            Ok(_) => panic!("unsafe node version counters must fail honestly"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("safe integer"));
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
