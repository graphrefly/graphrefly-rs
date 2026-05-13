//! WAL frame substrate (Phase 14.6 — DS-14-storage Q1+Q3+Q5 locks, M4.A
//! 2026-05-10).
//!
//! On-disk frame format consumed by `Graph::restore_snapshot({ mode:"diff" })`
//! (M4.E). Each frame decomposes a single graph diff into one DS-14
//! [`BaseChange<T>`] envelope per structural-or-value change, scoped by
//! [`Lifecycle`] so callers can narrow rewinds.
//!
//! The TS reference impl lives at
//! `packages/pure-ts/src/extra/storage/wal.ts`. Field names + checksum
//! algorithm are parity-locked — both impls produce byte-identical hex SHA-256
//! over the same canonical-JSON encoding of a frame body.
//!
//! # Canonical-JSON parity
//!
//! TS's `stableJsonString` (`packages/pure-ts/src/extra/storage/core.ts:22-39`)
//! is "recursively sort object keys, then `JSON.stringify(_, undefined, 0)`".
//! The Rust port mirrors this by routing the frame body through
//! [`serde_json::to_value`] (lands in `serde_json::Map` which is `BTreeMap` by
//! default — sorted iteration) then [`serde_json::to_string`]. Output is
//! byte-identical to TS for the WAL frame schema: ASCII keys, integer
//! numerics, no floats.
//!
//! **Parity caveats** (lift when a real consumer surfaces):
//! - String VALUES containing surrogate-pair code points (≥ U+10000): JS sorts
//!   keys by UTF-16 code-unit order; Rust `BTreeMap` sorts by UTF-8 byte order.
//!   For ASCII keys these agree; for non-BMP keys they don't. The frame
//!   schema's keys are ASCII so this can only bite if `path` or
//!   `change.structure` contains non-BMP code points — neither is expected
//!   for graph identifiers.
//! - Float-typed user payloads: JS `JSON.stringify` uses IEEE 754 with
//!   shortest-decimal-round-trip; Rust's `serde_json` uses `ryu` which agrees
//!   on finite f64 in safe range but may diverge on subnormals. WAL frames
//!   typically carry integer-only data; if a user puts a float in
//!   `change.change`, document the constraint.
//!
//! # Checksum
//!
//! SHA-256 over canonical-JSON of the frame body (everything except the
//! `checksum` field itself), encoded as a 64-char lowercase hex string.
//! Spec-locked at `GRAPHREFLY-SPEC.md:1201-1206` — original BLAKE3 lock was
//! revised to SHA-256 so the TS impl could stay zero-dep (no BLAKE3 in
//! `WebCrypto`). Rust matches via `sha2` + `hex`.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use graphrefly_structures::{BaseChange, Lifecycle};

// ── WAL frame envelope ─────────────────────────────────────────────────────

/// On-disk WAL frame (DS-14-storage Q1 lock).
///
/// Two seq fields and two timestamp fields are intentional:
/// - [`Self::frame_seq`] ≠ `change.seq`: latter is the bundle's `mutations`
///   cursor (DS-14 T1); former is the WAL tier's own cursor (this record's
///   position in the WAL stream). Replay uses `frame_seq` for ordering;
///   `change.seq` is only relevant for bundle-level cursor restoration.
/// - [`Self::frame_t_ns`] ≠ `change.t_ns`: latter is wall-clock at mutation
///   entry; former is wall-clock at WAL-write time. Under debounced tiers
///   they differ by `debounce_ms`.
///
/// The bridge wire format (DS-14 PART 5 worker bridge) is the schema-narrowed
/// subset `{ t, lifecycle, path, change }` — this struct is the
/// persistence-tier superset (DS-14-storage L3 lock).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WALFrame<T> {
    /// Bridge tag — discriminator shared with the DS-14 worker-bridge wire
    /// format. Always `"c"`; allocated as `String` for parity with the TS
    /// wire shape (TS uses a literal `"c"` value).
    pub t: WalTag,
    /// Lifecycle scope (DS-14 PART 4). Determines replay phase ordering.
    pub lifecycle: Lifecycle,
    /// Target node / bundle path (per-graph qualified path).
    pub path: String,
    /// DS-14 universal [`BaseChange<T>`] envelope — structure-tagged delta.
    pub change: BaseChange<T>,
    /// WAL-tier monotonic cursor (uniquely owned by the WAL tier writer).
    pub frame_seq: u64,
    /// Wall-clock at WAL-write time (matches `wall_clock_ns()`).
    pub frame_t_ns: u64,
    /// SHA-256 over the canonical-JSON of the frame body sans `checksum`,
    /// encoded as a 64-char lowercase hex string. Hex (vs raw bytes) keeps
    /// the wire format JSON-codec-friendly. M4.A parity-fixture asserts
    /// byte-equivalence against the TS impl.
    pub checksum: String,
    /// Codec version tag. All M4.A frames are implicitly version 1
    /// (JSON codec). Defaults to `1` for backward-compatible deserialization
    /// of frames written before this field was added.
    #[serde(default = "default_format_version")]
    pub format_version: u32,
}

fn default_format_version() -> u32 {
    1
}

/// Singleton-string discriminator for the bridge wire-format tag. Always
/// serializes / deserializes as `"c"`; rejects any other value at parse time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WalTag;

impl WalTag {
    pub const VALUE: &'static str = "c";
}

impl Serialize for WalTag {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(Self::VALUE)
    }
}

impl<'de> Deserialize<'de> for WalTag {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        if s == Self::VALUE {
            Ok(WalTag)
        } else {
            Err(serde::de::Error::custom(format!(
                "WALFrame.t must be {:?}, got {:?}",
                Self::VALUE,
                s
            )))
        }
    }
}

// ── Key format (Q5) ────────────────────────────────────────────────────────

/// Default WAL prefix segment relative to a `graph.name`. Frames land at
/// `${graph.name}/${WAL_KEY_SEGMENT}/${frame_seq:020}`.
pub const WAL_KEY_SEGMENT: &str = "wal";

/// Pad width for `frame_seq` in WAL keys. 20 digits keeps lex-ASC string sort
/// = numeric ASC up to `frame_seq < 10^20` (well past `u64::MAX`).
pub const WAL_FRAME_SEQ_PAD: usize = 20;

/// Build the canonical WAL frame key. `prefix` is the WAL-prefix portion (e.g.
/// `"my-graph/wal"`); `frame_seq` is the per-frame cursor. Zero-padded so
/// lex-ASC string sort equals numeric ASC sort.
#[must_use]
pub fn wal_frame_key(prefix: &str, frame_seq: u64) -> String {
    format!("{prefix}/{frame_seq:020}")
}

/// Default WAL key prefix for a graph by its `name`.
#[must_use]
pub fn graph_wal_prefix(graph_name: &str) -> String {
    format!("{graph_name}/{WAL_KEY_SEGMENT}")
}

// ── Replay order (Q2) ──────────────────────────────────────────────────────

/// Cross-scope replay order (DS-14 PART 4 lock — `Spec → Data → Ownership`).
/// Exported so the replay implementation and parity tests share one source of
/// truth.
pub const REPLAY_ORDER: [Lifecycle; 3] = [Lifecycle::Spec, Lifecycle::Data, Lifecycle::Ownership];

// ── Checksum ───────────────────────────────────────────────────────────────

/// Errors surfaced by checksum compute / verify.
#[derive(Debug, thiserror::Error)]
pub enum ChecksumError {
    /// `serde_json` rejected the frame body — typically a non-serializable
    /// payload (e.g. a `Map` with non-string keys, an `f64::NAN`).
    #[error("canonical JSON encoding failed: {0}")]
    CanonicalJsonFailed(#[from] serde_json::Error),
}

/// Body fields contributing to the checksum, in the shape TS computes over
/// (TS's `canonicalFrameBody` at `wal.ts:141`). The `checksum` field of the
/// outer [`WALFrame`] is deliberately excluded.
#[derive(Serialize)]
struct ChecksumBody<'a, T: Serialize> {
    t: &'static str,
    lifecycle: &'a Lifecycle,
    path: &'a str,
    change: &'a BaseChange<T>,
    frame_seq: u64,
    frame_t_ns: u64,
}

/// Encode a typed value to canonical JSON (sorted keys, no whitespace).
/// Routes through [`serde_json::Value`] so the resulting `serde_json::Map<
/// String, Value>` (BTreeMap-backed by default) iterates in sorted-key order
/// — byte-identical to TS `stableJsonString` on the WAL schema.
fn canonical_json<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
    let v = serde_json::to_value(value)?;
    serde_json::to_string(&v)
}

/// Compute the SHA-256 checksum over a frame's body (sans `checksum`),
/// returning a 64-char lowercase hex string. Parity-locked with TS
/// `walFrameChecksum`.
pub fn wal_frame_checksum<T: Serialize>(frame: &WALFrame<T>) -> Result<String, ChecksumError> {
    let body = ChecksumBody {
        t: WalTag::VALUE,
        lifecycle: &frame.lifecycle,
        path: frame.path.as_str(),
        change: &frame.change,
        frame_seq: frame.frame_seq,
        frame_t_ns: frame.frame_t_ns,
    };
    let canonical = canonical_json(&body)?;
    let digest = Sha256::digest(canonical.as_bytes());
    Ok(hex::encode(digest))
}

/// Verify a frame's `checksum` field matches its body. Replay invokes this at
/// the WAL tail (drop on mismatch by default) and mid-stream (abort on
/// mismatch by default) per Q3.
pub fn verify_wal_frame_checksum<T: Serialize>(frame: &WALFrame<T>) -> Result<bool, ChecksumError> {
    let expected = wal_frame_checksum(frame)?;
    Ok(frame.checksum == expected)
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use graphrefly_structures::Version;

    fn sample_frame() -> WALFrame<u64> {
        WALFrame {
            t: WalTag,
            lifecycle: Lifecycle::Data,
            path: "root/state".into(),
            change: BaseChange {
                structure: "graphValue".into(),
                version: Version::Counter(1),
                t_ns: 1_700_000_000_000,
                seq: Some(0),
                lifecycle: Lifecycle::Data,
                change: 42,
            },
            frame_seq: 17,
            frame_t_ns: 1_700_000_001_000,
            checksum: String::new(),
            format_version: 1,
        }
    }

    #[test]
    fn wal_frame_key_zero_pads_to_20_digits() {
        assert_eq!(wal_frame_key("g/wal", 0), "g/wal/00000000000000000000",);
        assert_eq!(wal_frame_key("g/wal", 17), "g/wal/00000000000000000017",);
        assert_eq!(
            wal_frame_key("g/wal", u64::MAX),
            format!("g/wal/{:020}", u64::MAX),
        );
    }

    #[test]
    fn wal_frame_key_lex_sort_equals_numeric_sort() {
        // Build keys for 0, 1, 10, 100, u64::MAX. Sort lex; assert numeric
        // order is preserved (the core invariant `frame_seq` ASC = lex ASC).
        let seqs = [0u64, 1, 10, 100, 1_000_000, u64::MAX];
        let mut keys: Vec<String> = seqs.iter().map(|s| wal_frame_key("g/wal", *s)).collect();
        keys.sort();
        for (k, expected) in keys.iter().zip(seqs.iter()) {
            assert!(
                k.ends_with(&format!("{expected:020}")),
                "lex-sort key {k} did not match numeric order for {expected}",
            );
        }
    }

    #[test]
    fn graph_wal_prefix_joins_with_segment() {
        assert_eq!(graph_wal_prefix("my-graph"), "my-graph/wal");
    }

    #[test]
    fn checksum_roundtrip_verifies() {
        let mut frame = sample_frame();
        frame.checksum = wal_frame_checksum(&frame).unwrap();
        assert!(verify_wal_frame_checksum(&frame).unwrap());
    }

    #[test]
    fn checksum_tamper_change_payload_fails_verify() {
        let mut frame = sample_frame();
        frame.checksum = wal_frame_checksum(&frame).unwrap();
        frame.change.change = 43; // tamper the user payload
        assert!(!verify_wal_frame_checksum(&frame).unwrap());
    }

    #[test]
    fn checksum_tamper_path_fails_verify() {
        let mut frame = sample_frame();
        frame.checksum = wal_frame_checksum(&frame).unwrap();
        frame.path = "different/path".into();
        assert!(!verify_wal_frame_checksum(&frame).unwrap());
    }

    #[test]
    fn checksum_tamper_frame_seq_fails_verify() {
        let mut frame = sample_frame();
        frame.checksum = wal_frame_checksum(&frame).unwrap();
        frame.frame_seq = 18;
        assert!(!verify_wal_frame_checksum(&frame).unwrap());
    }

    #[test]
    fn checksum_excludes_checksum_field_itself() {
        let mut frame = sample_frame();
        frame.checksum = "deadbeef".repeat(8);
        let first = wal_frame_checksum(&frame).unwrap();
        frame.checksum = "00".repeat(32);
        let second = wal_frame_checksum(&frame).unwrap();
        assert_eq!(
            first, second,
            "wal_frame_checksum must not depend on the existing checksum field",
        );
    }

    #[test]
    fn checksum_is_64_char_lowercase_hex() {
        let mut frame = sample_frame();
        frame.checksum = wal_frame_checksum(&frame).unwrap();
        assert_eq!(frame.checksum.len(), 64);
        assert!(
            frame
                .checksum
                .chars()
                .all(|c| matches!(c, '0'..='9' | 'a'..='f')),
            "checksum must be lowercase hex: {}",
            frame.checksum,
        );
    }

    #[test]
    fn wal_tag_serializes_as_string_c() {
        let s = serde_json::to_string(&WalTag).unwrap();
        assert_eq!(s, "\"c\"");
    }

    #[test]
    fn wal_tag_rejects_other_values() {
        let r: Result<WalTag, _> = serde_json::from_str("\"x\"");
        assert!(r.is_err(), "WalTag must reject non-c discriminators");
    }

    #[test]
    fn canonical_json_sorts_keys() {
        // Canonical-JSON sanity check on a struct with declaration order
        // OPPOSITE to alphabetical: `zebra, monkey, apple`. The emitted JSON
        // must list keys in alphabetical order regardless of declaration
        // order (mirrors TS `stableJsonString` recursive key sort). Single
        // level only so `find` matches unambiguously.
        #[derive(Serialize)]
        struct Flat {
            zebra: u32,
            monkey: u32,
            apple: u32,
        }
        let json = canonical_json(&Flat {
            zebra: 1,
            monkey: 2,
            apple: 3,
        })
        .unwrap();
        assert_eq!(json, "{\"apple\":3,\"monkey\":2,\"zebra\":1}");
    }

    /// Cross-impl parity fixture.
    ///
    /// This is the parity-or-bust check: a hand-computed canonical-JSON +
    /// SHA-256 fixture sourced from running TS's `walFrameChecksum` on the
    /// same input. If the Rust impl drifts from byte-identical TS output,
    /// this test fails loudly.
    ///
    /// Fixture inputs are deliberately minimal (single `u64` change payload)
    /// so the expected canonical bytes are auditable by hand.
    #[test]
    fn checksum_parity_fixture_minimal_frame() {
        // Frame body:
        //   { t:"c", lifecycle:"data", path:"p",
        //     change:{ change:0, lifecycle:"data", structure:"s", t_ns:0, version:0 },
        //     frame_seq:0, frame_t_ns:0 }
        //
        // Canonical (sorted-key, no whitespace) form:
        //   {"change":{"change":0,"lifecycle":"data","structure":"s","t_ns":0,"version":0},"frame_seq":0,"frame_t_ns":0,"lifecycle":"data","path":"p","t":"c"}
        //
        // SHA-256 over those bytes is checked below. Regenerate via:
        //   python3 -c 'import hashlib; print(hashlib.sha256(b\'{"change":...}\').hexdigest())'
        let frame: WALFrame<u64> = WALFrame {
            t: WalTag,
            lifecycle: Lifecycle::Data,
            path: "p".into(),
            change: BaseChange {
                structure: "s".into(),
                version: Version::Counter(0),
                t_ns: 0,
                seq: None,
                lifecycle: Lifecycle::Data,
                change: 0,
            },
            frame_seq: 0,
            frame_t_ns: 0,
            checksum: String::new(),
            format_version: 1,
        };
        let computed = wal_frame_checksum(&frame).unwrap();

        // Sanity: confirm the canonical body the Rust impl is hashing.
        let body = ChecksumBody {
            t: WalTag::VALUE,
            lifecycle: &frame.lifecycle,
            path: frame.path.as_str(),
            change: &frame.change,
            frame_seq: frame.frame_seq,
            frame_t_ns: frame.frame_t_ns,
        };
        let canonical = canonical_json(&body).unwrap();
        let expected_canonical = "{\"change\":{\"change\":0,\"lifecycle\":\"data\",\"structure\":\"s\",\"t_ns\":0,\"version\":0},\"frame_seq\":0,\"frame_t_ns\":0,\"lifecycle\":\"data\",\"path\":\"p\",\"t\":\"c\"}";
        assert_eq!(
            canonical, expected_canonical,
            "canonical JSON drifted from TS-side stableJsonString shape",
        );

        // SHA-256 hex of the canonical bytes above (computed via shell:
        // `printf '<canonical>' | shasum -a 256`).
        let expected_sha = "d00054d7886e1d73c07a0086e5cbccddf62de3c0cadae31e75d78215b3293ece";
        assert_eq!(
            computed, expected_sha,
            "SHA-256 hex drifted; canonical bytes were:\n  {canonical}",
        );
    }

    /// /qa A5 (2026-05-10): parity-fixture for `Lifecycle::Spec` — locks
    /// the canonical-JSON byte shape and SHA-256 for the `"spec"` discriminant.
    #[test]
    fn checksum_parity_fixture_lifecycle_spec() {
        let frame: WALFrame<u64> = WALFrame {
            t: WalTag,
            lifecycle: Lifecycle::Spec,
            path: "p".into(),
            change: BaseChange {
                structure: "s".into(),
                version: Version::Counter(0),
                t_ns: 0,
                seq: None,
                lifecycle: Lifecycle::Spec,
                change: 0,
            },
            frame_seq: 0,
            frame_t_ns: 0,
            checksum: String::new(),
            format_version: 1,
        };
        let expected_sha = "7e857f0862bd429d7d144980a2580da732e0d4b420a03d73d63462368f896c3b";
        assert_eq!(wal_frame_checksum(&frame).unwrap(), expected_sha);
    }

    /// /qa A5 (2026-05-10): parity-fixture for `Lifecycle::Ownership`.
    #[test]
    fn checksum_parity_fixture_lifecycle_ownership() {
        let frame: WALFrame<u64> = WALFrame {
            t: WalTag,
            lifecycle: Lifecycle::Ownership,
            path: "p".into(),
            change: BaseChange {
                structure: "s".into(),
                version: Version::Counter(0),
                t_ns: 0,
                seq: None,
                lifecycle: Lifecycle::Ownership,
                change: 0,
            },
            frame_seq: 0,
            frame_t_ns: 0,
            checksum: String::new(),
            format_version: 1,
        };
        let expected_sha = "901d3d70d38d954864243bdee5a88cb6d204e5e9823598606d38c10e604c3af4";
        assert_eq!(wal_frame_checksum(&frame).unwrap(), expected_sha);
    }

    /// /qa A6 (2026-05-10): parity-fixture for `seq: Some(0)`. The
    /// `skip_serializing_if` attribute means `None` omits the field; `Some(0)`
    /// emits `"seq":0`. Both round-trip cleanly. Distinct SHA from the
    /// `seq: None` fixture above proves the canonical body differs.
    #[test]
    fn checksum_parity_fixture_seq_some_zero() {
        let frame: WALFrame<u64> = WALFrame {
            t: WalTag,
            lifecycle: Lifecycle::Data,
            path: "p".into(),
            change: BaseChange {
                structure: "s".into(),
                version: Version::Counter(0),
                t_ns: 0,
                seq: Some(0),
                lifecycle: Lifecycle::Data,
                change: 0,
            },
            frame_seq: 0,
            frame_t_ns: 0,
            checksum: String::new(),
            format_version: 1,
        };
        let expected_sha = "da42bdfa3eff9dbb7ffc60b04c7478cbe7cbb7015ba48963b4ea4661f678c387";
        assert_eq!(wal_frame_checksum(&frame).unwrap(), expected_sha);
    }

    /// /qa A7 (2026-05-10): `WalTag` deserialization rejects non-string JSON
    /// tokens (null, number, array, object) with a clear error — not just
    /// other string values.
    #[test]
    fn wal_tag_rejects_non_string_tokens() {
        for bad in ["null", "42", "[]", "{}", "true"] {
            let r: Result<WalTag, _> = serde_json::from_str(bad);
            assert!(r.is_err(), "WalTag must reject {bad}");
        }
    }

    /// /qa A13 (2026-05-10): sanity-check the `WALFrame<T>` shape for two
    /// non-trivial payload types — unit `()` and `serde_json::Value` (the
    /// "any JSON" escape hatch). Both must round-trip with stable checksums.
    #[test]
    fn wal_frame_unit_payload_round_trips() {
        let frame: WALFrame<()> = WALFrame {
            t: WalTag,
            lifecycle: Lifecycle::Data,
            path: "p".into(),
            change: BaseChange {
                structure: "unit".into(),
                version: Version::Counter(0),
                t_ns: 0,
                seq: None,
                lifecycle: Lifecycle::Data,
                change: (),
            },
            frame_seq: 0,
            frame_t_ns: 0,
            checksum: String::new(),
            format_version: 1,
        };
        let mut f = frame.clone();
        f.checksum = wal_frame_checksum(&frame).unwrap();
        assert!(verify_wal_frame_checksum(&f).unwrap());
    }

    #[test]
    fn wal_frame_value_payload_round_trips() {
        use serde_json::json;
        let payload = json!({"kind": "set", "key": "k1", "value": [1, 2, 3]});
        let frame: WALFrame<serde_json::Value> = WALFrame {
            t: WalTag,
            lifecycle: Lifecycle::Data,
            path: "node/state".into(),
            change: BaseChange {
                structure: "graphValue".into(),
                version: Version::Counter(1),
                t_ns: 100,
                seq: Some(7),
                lifecycle: Lifecycle::Data,
                change: payload,
            },
            frame_seq: 17,
            frame_t_ns: 200,
            checksum: String::new(),
            format_version: 1,
        };
        let mut f = frame.clone();
        f.checksum = wal_frame_checksum(&frame).unwrap();
        assert!(verify_wal_frame_checksum(&f).unwrap());
    }

    /// /qa F5 (2026-05-12): backward-compatible deserialization of
    /// pre-`format_version` frames. Old frames serialized WITHOUT the
    /// `format_version` field must deserialize successfully with
    /// `format_version` defaulting to `1`.
    #[test]
    fn format_version_defaults_on_old_frame_json() {
        // JSON from a pre-format_version frame (no `format_version` key).
        let old_json = r#"{
            "t": "c",
            "lifecycle": "data",
            "path": "p",
            "change": {
                "structure": "s",
                "version": 0,
                "t_ns": 0,
                "lifecycle": "data",
                "change": 0
            },
            "frame_seq": 0,
            "frame_t_ns": 0,
            "checksum": ""
        }"#;
        let frame: WALFrame<u64> = serde_json::from_str(old_json).unwrap();
        assert_eq!(
            frame.format_version, 1,
            "missing format_version must default to 1"
        );
    }

    /// /qa F5 (2026-05-12): new frames with explicit `format_version`
    /// round-trip correctly.
    #[test]
    fn format_version_round_trips() {
        let frame = WALFrame {
            t: WalTag,
            lifecycle: Lifecycle::Data,
            path: "p".into(),
            change: BaseChange {
                structure: "s".into(),
                version: Version::Counter(0),
                t_ns: 0,
                seq: None,
                lifecycle: Lifecycle::Data,
                change: 0u64,
            },
            frame_seq: 0,
            frame_t_ns: 0,
            checksum: String::new(),
            format_version: 2,
        };
        let json = serde_json::to_string(&frame).unwrap();
        let deser: WALFrame<u64> = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.format_version, 2);
    }

    /// /qa A10 (2026-05-10): canary detecting `serde_json/preserve_order`
    /// feature unification. The canonical-JSON parity invariant requires
    /// `serde_json::Map<String, Value>` to be `BTreeMap`-backed (sorted on
    /// iter). If any workspace consumer enables `preserve_order` via Cargo
    /// feature unification, `Map` swaps to `IndexMap` (insertion-order) and
    /// this test fails loudly with a diff.
    #[test]
    fn preserve_order_feature_is_not_enabled() {
        // Build a Value::Object with INSERTION ORDER = reverse-alphabetical.
        // BTreeMap-backed Map iterates in alphabetical order on `to_string`.
        // IndexMap-backed Map preserves insertion order.
        let mut map = serde_json::Map::new();
        map.insert("z".into(), serde_json::json!(1));
        map.insert("a".into(), serde_json::json!(2));
        let serialized = serde_json::to_string(&serde_json::Value::Object(map)).unwrap();
        assert_eq!(
            serialized, r#"{"a":2,"z":1}"#,
            "serde_json `preserve_order` feature appears to be enabled \
             workspace-wide via Cargo feature unification — this BREAKS the \
             WAL checksum canonical-JSON parity invariant. Find the offending \
             dep with `cargo tree -e features | grep preserve_order` and \
             either disable it or pin to a non-preserve-order codec route.",
        );
    }
}
