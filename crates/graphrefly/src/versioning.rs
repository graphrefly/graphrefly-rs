//! Node runtime versioning (D109/D112/D113).
//!
//! This is runtime metadata over DATA boundaries, not a protocol message, storage
//! generation, checkpoint envelope version, or public mutation surface.

use std::fmt;
use std::rc::Rc;

use serde_json::{Number, Value};

use crate::json::{strict_canonical_json_bytes, validate_strict_json_value};
use crate::protocol::AnyValue;

const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

/// `NodeVersionHashFn` type alias.
pub type NodeVersionHashFn = Rc<dyn Fn(&[u8]) -> String>;

#[derive(Clone)]
/// `NodeVersioningPolicy` variants.
pub enum NodeVersioningPolicy {
    /// `Disabled` variant.
    Disabled,
    /// `Level0` variant.
    Level0,
    /// `Level1` variant.
    Level1 {
        /// `hash` field for `Level1`.
        hash: Option<NodeVersionHashFn>,
    },
}

impl fmt::Debug for NodeVersioningPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disabled => f.write_str("Disabled"),
            Self::Level0 => f.write_str("Level0"),
            Self::Level1 { hash } => f
                .debug_struct("Level1")
                .field("hash", &hash.as_ref().map(|_| "<installed>"))
                .finish(),
        }
    }
}

#[derive(Clone)]
/// `ResolvedNodeVersioningPolicy` variants.
pub enum ResolvedNodeVersioningPolicy {
    /// `Disabled` variant.
    Disabled,
    /// `Level0` variant.
    Level0,
    /// `Level1` variant.
    Level1 {
        /// `hash` field for `Level1`.
        hash: NodeVersionHashFn,
    },
}

impl fmt::Debug for ResolvedNodeVersioningPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disabled => f.write_str("Disabled"),
            Self::Level0 => f.write_str("Level0"),
            Self::Level1 { .. } => f.write_str("Level1 { hash: <installed> }"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `NodeVersion` variants.
pub enum NodeVersion {
    /// `V0` variant.
    V0 {
        /// `counter` field for counter.
        counter: u64,
    },
    /// `V1` variant.
    V1 {
        /// `counter` field for counter.
        counter: u64,
        /// `cid` field for cid.
        cid: String,
        /// `prev` field for prev.
        prev: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub(crate) enum RestoredNodeVersion {
    Disabled,
    Version(NodeVersion),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NodeVersioningError {
    message: String,
}

impl NodeVersioningError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for NodeVersioningError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for NodeVersioningError {}

pub(crate) fn resolve_node_versioning_policy(
    policy: Option<NodeVersioningPolicy>,
) -> ResolvedNodeVersioningPolicy {
    match policy {
        Some(NodeVersioningPolicy::Disabled) => ResolvedNodeVersioningPolicy::Disabled,
        None | Some(NodeVersioningPolicy::Level0) => ResolvedNodeVersioningPolicy::Level0,
        Some(NodeVersioningPolicy::Level1 { hash }) => ResolvedNodeVersioningPolicy::Level1 {
            hash: hash.unwrap_or_else(default_node_version_hash_fn),
        },
    }
}

fn default_node_version_hash_fn() -> NodeVersionHashFn {
    Rc::new(default_node_version_hash)
}

/// Creates or computes `default_node_version_hash`.
pub fn default_node_version_hash(bytes: &[u8]) -> String {
    format!("fnv1a64:{}", fnv1a64(bytes))
}

fn fnv1a64(bytes: &[u8]) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

pub(crate) fn create_node_version(
    policy: &ResolvedNodeVersioningPolicy,
    initial: Option<&AnyValue>,
) -> Result<Option<NodeVersion>, NodeVersioningError> {
    match policy {
        ResolvedNodeVersioningPolicy::Disabled => Ok(None),
        ResolvedNodeVersioningPolicy::Level0 => Ok(Some(NodeVersion::V0 { counter: 0 })),
        ResolvedNodeVersioningPolicy::Level1 { hash } => {
            let cid = hash_node_data(hash, initial)?;
            Ok(Some(NodeVersion::V1 {
                counter: 0,
                cid,
                prev: None,
            }))
        }
    }
}

pub(crate) fn assert_node_version_data_compatible(
    policy: &ResolvedNodeVersioningPolicy,
    value: &AnyValue,
) -> Result<(), NodeVersioningError> {
    if matches!(policy, ResolvedNodeVersioningPolicy::Level1 { .. }) {
        let json = any_to_strict_json(value, "$")?;
        let _ = strict_canonical_json_bytes(&json).map_err(|err| {
            NodeVersioningError::new(format!(
                "node versioning: DATA is not strict canonical JSON compatible (D112): {err}"
            ))
        })?;
    }
    Ok(())
}

pub(crate) fn advance_node_version(
    current: Option<&NodeVersion>,
    policy: &ResolvedNodeVersioningPolicy,
    value: &AnyValue,
) -> Result<Option<NodeVersion>, NodeVersioningError> {
    match policy {
        ResolvedNodeVersioningPolicy::Disabled => Ok(None),
        ResolvedNodeVersioningPolicy::Level0 => Ok(Some(NodeVersion::V0 {
            counter: next_counter(current)?,
        })),
        ResolvedNodeVersioningPolicy::Level1 { hash } => {
            let previous = match current {
                Some(NodeVersion::V1 { cid, .. }) => Some(cid.clone()),
                _ => None,
            };
            Ok(Some(NodeVersion::V1 {
                counter: next_counter(current)?,
                cid: hash_node_data(hash, Some(value))?,
                prev: previous,
            }))
        }
    }
}

pub(crate) fn validate_node_version_json(
    value: &Value,
    path: &str,
) -> Result<NodeVersion, NodeVersioningError> {
    validate_strict_json_value(value, path).map_err(|err| {
        NodeVersioningError::new(format!(
            "restore_graph: node version metadata is not strict canonical JSON compatible (D112): {err}"
        ))
    })?;
    let Value::Object(record) = value else {
        return Err(NodeVersioningError::new(format!(
            "restore_graph: {path} must be an object"
        )));
    };
    match record.get("level").and_then(Value::as_u64) {
        Some(0) => {
            require_fields(record, &["level", "counter"], path)?;
            let counter = read_counter(record.get("counter"), path)?;
            Ok(NodeVersion::V0 { counter })
        }
        Some(1) => {
            require_fields(record, &["level", "counter", "cid", "prev"], path)?;
            let counter = read_counter(record.get("counter"), path)?;
            let cid = record
                .get("cid")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    NodeVersioningError::new(format!("restore_graph: {path}.cid must be a string"))
                })?
                .to_owned();
            let prev = match record.get("prev") {
                Some(Value::Null) => None,
                Some(Value::String(value)) => Some(value.clone()),
                _ => {
                    return Err(NodeVersioningError::new(format!(
                        "restore_graph: {path}.prev must be a string or null"
                    )));
                }
            };
            if counter == 0 && prev.is_some() {
                return Err(NodeVersioningError::new(format!(
                    "restore_graph: {path}.prev must be null when counter is 0"
                )));
            }
            if counter > 0 && prev.is_none() {
                return Err(NodeVersioningError::new(format!(
                    "restore_graph: {path}.prev must be a string when counter is > 0"
                )));
            }
            Ok(NodeVersion::V1 { counter, cid, prev })
        }
        _ => Err(NodeVersioningError::new(format!(
            "restore_graph: {path}.level must be 0 or 1"
        ))),
    }
}

pub(crate) fn node_version_to_json(version: &NodeVersion) -> Value {
    let mut record = serde_json::Map::new();
    match version {
        NodeVersion::V0 { counter } => {
            record.insert("level".to_owned(), Value::Number(Number::from(0)));
            record.insert("counter".to_owned(), Value::Number(Number::from(*counter)));
        }
        NodeVersion::V1 { counter, cid, prev } => {
            record.insert("level".to_owned(), Value::Number(Number::from(1)));
            record.insert("counter".to_owned(), Value::Number(Number::from(*counter)));
            record.insert("cid".to_owned(), Value::String(cid.clone()));
            record.insert(
                "prev".to_owned(),
                prev.clone().map_or(Value::Null, Value::String),
            );
        }
    }
    Value::Object(record)
}

pub(crate) fn verify_restored_node_version(
    policy: &ResolvedNodeVersioningPolicy,
    restored: Option<&Value>,
    has_data: bool,
    cache: Option<&AnyValue>,
    path: &str,
) -> Result<RestoredNodeVersion, NodeVersioningError> {
    let Some(restored) = restored else {
        return match policy {
            ResolvedNodeVersioningPolicy::Disabled => Ok(RestoredNodeVersion::Disabled),
            _ => Err(NodeVersioningError::new(
                "restore_graph: checkpoint node version metadata is required by the selected node versioning policy (D109)",
            )),
        };
    };
    let version = validate_node_version_json(restored, path)?;
    match (policy, &version) {
        (ResolvedNodeVersioningPolicy::Disabled, _) => Err(NodeVersioningError::new(
            "restore_graph: checkpoint node version metadata is present but node versioning is disabled",
        )),
        (ResolvedNodeVersioningPolicy::Level0, NodeVersion::V0 { .. }) => {
            Ok(RestoredNodeVersion::Version(version))
        }
        (ResolvedNodeVersioningPolicy::Level0, NodeVersion::V1 { .. }) => Err(
            NodeVersioningError::new(
                "restore_graph: checkpoint node version level 1 requires matching node versioning policy",
            ),
        ),
        (ResolvedNodeVersioningPolicy::Level1 { .. }, NodeVersion::V0 { .. }) => Err(
            NodeVersioningError::new(
                "restore_graph: checkpoint node version level 0 requires matching node versioning policy",
            ),
        ),
        (ResolvedNodeVersioningPolicy::Level1 { hash }, NodeVersion::V1 { counter, cid, .. }) => {
            if !has_data && *counter > 0 {
                return Err(NodeVersioningError::new(
                    "restore_graph: checkpoint node version cid cannot be verified without current DATA under V1 versioning (D109)",
                ));
            }
            let expected = hash_node_data(hash, if has_data { cache } else { None })?;
            if &expected != cid {
                return Err(NodeVersioningError::new(
                    "restore_graph: checkpoint node version cid does not match the selected node versioning hash policy (D109)",
                ));
            }
            Ok(RestoredNodeVersion::Version(version))
        }
    }
}

fn next_counter(current: Option<&NodeVersion>) -> Result<u64, NodeVersioningError> {
    let Some(current) = current else {
        return Ok(1);
    };
    current
        .counter()
        .checked_add(1)
        .ok_or_else(|| {
            NodeVersioningError::new(
                "node versioning: counter overflow while advancing node runtime version (D109)",
            )
        })
        .and_then(|counter| {
            if counter > MAX_SAFE_INTEGER {
                Err(NodeVersioningError::new(
                    "node versioning: counter exceeded the strict JSON safe-integer range (D109)",
                ))
            } else {
                Ok(counter)
            }
        })
}

fn require_fields(
    record: &serde_json::Map<String, Value>,
    fields: &[&str],
    path: &str,
) -> Result<(), NodeVersioningError> {
    if record.len() != fields.len() || fields.iter().any(|field| !record.contains_key(*field)) {
        return Err(NodeVersioningError::new(format!(
            "restore_graph: {path} has unexpected node version fields"
        )));
    }
    Ok(())
}

fn read_counter(value: Option<&Value>, path: &str) -> Result<u64, NodeVersioningError> {
    let counter = value.and_then(Value::as_u64).ok_or_else(|| {
        NodeVersioningError::new(format!(
            "restore_graph: {path}.counter must be a non-negative safe integer"
        ))
    })?;
    if counter > MAX_SAFE_INTEGER {
        return Err(NodeVersioningError::new(format!(
            "restore_graph: {path}.counter must be a non-negative safe integer"
        )));
    }
    Ok(counter)
}

fn hash_node_data(
    hash: &NodeVersionHashFn,
    value: Option<&AnyValue>,
) -> Result<String, NodeVersioningError> {
    let json = match value {
        Some(value) => any_to_strict_json(value, "$")?,
        None => absent_v1_seed(),
    };
    let bytes = strict_canonical_json_bytes(&json).map_err(|err| {
        NodeVersioningError::new(format!(
            "node versioning: DATA is not strict canonical JSON compatible (D112): {err}"
        ))
    })?;
    Ok(hash(&bytes))
}

fn absent_v1_seed() -> Value {
    let mut record = serde_json::Map::new();
    record.insert(
        "@graphrefly/node-version".to_owned(),
        Value::String("v1-absent".to_owned()),
    );
    Value::Object(record)
}

fn any_to_strict_json(value: &AnyValue, path: &str) -> Result<Value, NodeVersioningError> {
    let out = if let Some(v) = value.downcast_ref::<Value>() {
        v.clone()
    } else if let Some(v) = value.downcast_ref::<String>() {
        Value::String(v.clone())
    } else if let Some(v) = value.downcast_ref::<bool>() {
        Value::Bool(*v)
    } else if let Some(v) = value.downcast_ref::<i32>() {
        Value::Number(Number::from(*v))
    } else if let Some(v) = value.downcast_ref::<i64>() {
        Value::Number(Number::from(*v))
    } else if let Some(v) = value.downcast_ref::<u32>() {
        Value::Number(Number::from(*v))
    } else if let Some(v) = value.downcast_ref::<u64>() {
        Value::Number(Number::from(*v))
    } else if let Some(v) = value.downcast_ref::<usize>() {
        Value::Number(Number::from(*v as u64))
    } else if let Some(v) = value.downcast_ref::<f64>() {
        Number::from_f64(*v).map(Value::Number).ok_or_else(|| {
            NodeVersioningError::new(format!(
                "node versioning: DATA at {path} is not strict JSON compatible"
            ))
        })?
    } else {
        return Err(NodeVersioningError::new(format!(
            "node versioning: DATA at {path} is not strict JSON compatible"
        )));
    };
    validate_strict_json_value(&out, path).map_err(|err| {
        NodeVersioningError::new(format!(
            "node versioning: DATA is not strict canonical JSON compatible (D112): {err}"
        ))
    })?;
    Ok(out)
}

impl NodeVersion {
    /// Updates or reads `level`.
    pub fn level(&self) -> u8 {
        match self {
            Self::V0 { .. } => 0,
            Self::V1 { .. } => 1,
        }
    }

    /// Updates or reads `counter`.
    pub fn counter(&self) -> u64 {
        match self {
            Self::V0 { counter } | Self::V1 { counter, .. } => *counter,
        }
    }
}
