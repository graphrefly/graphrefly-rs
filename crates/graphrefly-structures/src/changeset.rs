//! Universal change envelope (Phase 14 — DS-14 locked 2026-05-05; M4.A 2026-05-10).
//!
//! Mirrors the TS impl at
//! `packages/pure-ts/src/extra/data-structures/change.ts`. Every reactive
//! structure's delta log emits records conforming to [`BaseChange<T>`]; storage
//! WAL frames ([`crate`] consumer `graphrefly-storage::wal::WALFrame<T>`) and
//! worker-bridge wire frames both carry the same envelope.
//!
//! Two-level discriminant:
//! - Envelope-level [`Lifecycle`] (`spec` / `data` / `ownership`) for
//!   cross-scope replay-boundary safety (DS-14 PART 4).
//! - Payload-level `kind` discriminator (per-structure verb) inside the
//!   `change: T` slot — porting of the per-structure payload unions
//!   (`MapChangePayload`, `ListChangePayload`, etc.) lands with M5
//!   reactive-structure ports.
//!
//! The `serde-support` feature gates `Serialize` / `Deserialize` derives.
//! Storage / bridge consumers enable it; in-process structure consumers can
//! skip the codec footprint.

#[cfg(feature = "serde-support")]
use serde::{Deserialize, Serialize};

/// Cross-scope replay-boundary discriminant.
///
/// Replay order is `Spec → Data → Ownership` (canonical spec §b — fixed in
/// `graphrefly-storage::wal::REPLAY_ORDER`). Allows a `restoreSnapshot
/// mode:"diff"` caller to filter restore to a single lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde-support", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde-support", serde(rename_all = "lowercase"))]
pub enum Lifecycle {
    Spec,
    Data,
    Ownership,
}

/// Monotonic identity field for [`BaseChange::version`]. V0 is a counter
/// (`u64`); V1+ is a content-id (CID string). The TS impl uses `number |
/// string`; this enum is `#[serde(untagged)]` so the wire format is identical
/// — bare number for `Counter`, bare string for `Cid`. Mixed-type sequences
/// across versions are user-resolved per spec.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde-support", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde-support", serde(untagged))]
pub enum Version {
    Counter(u64),
    Cid(String),
}

/// Universal change envelope (DS-14 PART 4).
///
/// Field semantics:
/// - `structure` — open string namespace per structure variant
///   (`"reactiveMap"`, `"graphValue"`, `"ownership"`, …).
/// - `version` — monotonic identity ([`Version::Counter`] V0 or
///   [`Version::Cid`] V1+).
/// - `t_ns` — wall-clock at mutation entry (matches the TS `wallClockNs()` call
///   site).
/// - `seq` — optional cursor seq for joining with audit logs. **Distinct from**
///   `WALFrame::frame_seq` — `seq` is the bundle's `mutations` cursor (DS-14
///   T1); `frame_seq` is the WAL tier's own cursor.
/// - `lifecycle` — scope discriminant (see [`Lifecycle`]).
/// - `change` — structure-specific delta payload, discriminated internally
///   by `kind` (porting of per-structure payload unions arrives with M5).
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde-support", derive(Serialize, Deserialize))]
pub struct BaseChange<T> {
    pub structure: String,
    pub version: Version,
    pub t_ns: u64,
    #[cfg_attr(
        feature = "serde-support",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub seq: Option<u64>,
    pub lifecycle: Lifecycle,
    pub change: T,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "serde-support")]
    #[test]
    fn lifecycle_serde_lowercase() {
        assert_eq!(serde_json::to_string(&Lifecycle::Spec).unwrap(), "\"spec\"");
        assert_eq!(serde_json::to_string(&Lifecycle::Data).unwrap(), "\"data\"");
        assert_eq!(
            serde_json::to_string(&Lifecycle::Ownership).unwrap(),
            "\"ownership\""
        );
        let parsed: Lifecycle = serde_json::from_str("\"data\"").unwrap();
        assert_eq!(parsed, Lifecycle::Data);
    }

    #[cfg(feature = "serde-support")]
    #[test]
    fn version_serde_untagged_matches_ts_number_or_string() {
        // Counter serializes as a bare JSON number — wire-identical to TS V0.
        assert_eq!(serde_json::to_string(&Version::Counter(42)).unwrap(), "42");
        // Cid serializes as a bare JSON string — wire-identical to TS V1+.
        assert_eq!(
            serde_json::to_string(&Version::Cid("bafy123".into())).unwrap(),
            "\"bafy123\""
        );
        // Round-trip: number → Counter, string → Cid.
        let parsed_num: Version = serde_json::from_str("7").unwrap();
        assert_eq!(parsed_num, Version::Counter(7));
        let parsed_str: Version = serde_json::from_str("\"bafyabc\"").unwrap();
        assert_eq!(parsed_str, Version::Cid("bafyabc".into()));
    }

    #[cfg(feature = "serde-support")]
    #[test]
    fn base_change_skips_optional_seq() {
        let c: BaseChange<u64> = BaseChange {
            structure: "test".into(),
            version: Version::Counter(1),
            t_ns: 100,
            seq: None,
            lifecycle: Lifecycle::Data,
            change: 99,
        };
        let s = serde_json::to_string(&c).unwrap();
        assert!(
            !s.contains("seq"),
            "Option::None should not emit seq field: {s}"
        );
    }
}
