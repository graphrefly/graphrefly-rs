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
