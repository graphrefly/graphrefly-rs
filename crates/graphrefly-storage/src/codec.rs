//! Codec abstraction for tier serialization (Phase 14.6 — DS-14-storage Q4
//! lock, M4.B 2026-05-10).
//!
//! Tiers parameterize over a [`Codec<T>`] to encode values before
//! `backend.write` and decode bytes after `backend.read`. [`JsonCodec`] is the
//! default (zero-sized, parity-encoded to match TS `jsonCodec`). `DagCbor` /
//! `zstd` codecs land in later sub-slices when content-addressing scenarios
//! surface.
//!
//! # Parity with TS `jsonCodec`
//!
//! TS `jsonCodec.encode` runs values through `stableJsonString` — recursive
//! key-sort + `JSON.stringify(_, undefined, 0)`. [`JsonCodec`] mirrors via
//! [`serde_json::to_value`] (BTreeMap-backed `Map`, sorted iteration) →
//! [`serde_json::to_vec`]. Snapshot files written by the Rust impl are
//! byte-identical to TS for the value schemas Graph emits (ASCII keys,
//! integer numerics, no floats).

use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;

/// Codec encode / decode failures. Stringified internally — the underlying
/// `serde_json::Error` carries position info in its `Display` impl.
#[derive(Debug, Error)]
pub enum CodecError {
    #[error("codec encode failed: {0}")]
    Encode(String),

    #[error("codec decode failed: {0}")]
    Decode(String),
}

/// Codec for tier serialization. Tiers call `encode(value)` before
/// `backend.write` and `decode(bytes)` after `backend.read`. `name` +
/// `version` surface at the tier level for `format_version` migration (Q4).
pub trait Codec<T>: Send + Sync {
    /// Codec identifier (e.g. `"json"`, `"dag-cbor"`, `"dag-cbor-zstd"`).
    fn name(&self) -> &str;
    /// Codec version. Bumped when the on-wire format changes incompatibly.
    fn version(&self) -> u32;
    fn encode(&self, value: &T) -> Result<Vec<u8>, CodecError>;
    fn decode(&self, bytes: &[u8]) -> Result<T, CodecError>;
}

/// Zero-sized JSON codec — UTF-8 text, canonical (sorted-key) JSON. Matches
/// TS `jsonCodec` byte-for-byte on the value schemas Graph emits.
#[derive(Debug, Default, Clone, Copy)]
pub struct JsonCodec;

impl<T> Codec<T> for JsonCodec
where
    T: Serialize + DeserializeOwned + Send + Sync,
{
    fn name(&self) -> &'static str {
        "json"
    }
    fn version(&self) -> u32 {
        1
    }
    fn encode(&self, value: &T) -> Result<Vec<u8>, CodecError> {
        // Canonical (sorted-key) JSON via the BTreeMap-backed `Value` route —
        // matches TS `stableJsonString`.
        let v = serde_json::to_value(value).map_err(|e| CodecError::Encode(e.to_string()))?;
        serde_json::to_vec(&v).map_err(|e| CodecError::Encode(e.to_string()))
    }
    fn decode(&self, bytes: &[u8]) -> Result<T, CodecError> {
        serde_json::from_slice(bytes).map_err(|e| CodecError::Decode(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Counter {
        zebra: u32,
        apple: u32,
        monkey: u32,
    }

    #[test]
    fn json_codec_round_trip() {
        let codec = JsonCodec;
        let v = Counter {
            zebra: 1,
            apple: 2,
            monkey: 3,
        };
        let bytes = <JsonCodec as Codec<Counter>>::encode(&codec, &v).unwrap();
        let back: Counter = <JsonCodec as Codec<Counter>>::decode(&codec, &bytes).unwrap();
        assert_eq!(v, back);
    }

    #[test]
    fn json_codec_canonical_sorts_keys() {
        let codec = JsonCodec;
        let v = Counter {
            zebra: 1,
            apple: 2,
            monkey: 3,
        };
        let bytes = <JsonCodec as Codec<Counter>>::encode(&codec, &v).unwrap();
        let s = std::str::from_utf8(&bytes).unwrap();
        // Keys must appear in alphabetical order regardless of struct
        // declaration order.
        assert_eq!(s, r#"{"apple":2,"monkey":3,"zebra":1}"#);
    }

    #[test]
    fn json_codec_name_and_version() {
        let codec = JsonCodec;
        assert_eq!(<JsonCodec as Codec<Counter>>::name(&codec), "json");
        assert_eq!(<JsonCodec as Codec<Counter>>::version(&codec), 1);
    }

    #[test]
    fn json_codec_decode_rejects_invalid_bytes() {
        let codec = JsonCodec;
        let result: Result<Counter, _> = <JsonCodec as Codec<Counter>>::decode(&codec, b"not json");
        assert!(matches!(result, Err(CodecError::Decode(_))));
    }
}
