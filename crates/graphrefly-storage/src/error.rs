//! Storage tier + WAL replay error types (Phase 14.6 — DS-14-storage Q3/Q9).
//!
//! - [`StorageError`] — tier-level preconditions (backend lacks capability,
//!   codec mismatch, backend I/O failure). Surfaced by `BaseStorageTier`
//!   operations (M4.B+).
//! - [`RestoreError`] — replay-time failures from
//!   `Graph::restore_snapshot({ mode: "diff" })` (M4.E). Distinct from
//!   `StorageError` so callers can disambiguate "tier broken" from "replay
//!   couldn't complete".
//! - [`RestoreResult`] — telemetry returned on successful replay
//!   (inspection-as-test-harness shape per CLAUDE.md dry-run rule).
//!
//! Both error enums mirror the TS impl at
//! `packages/pure-ts/src/extra/storage/wal.ts:194-251`; variant names align
//! with the TS string discriminants for parity-test diffability.

use serde::Serialize;
use thiserror::Error;

use graphrefly_structures::Lifecycle;

use crate::codec::CodecError;
use crate::wal::ChecksumError;

/// Tier-level preconditions surfaced by `BaseStorageTier` operations.
#[derive(Debug, Error)]
pub enum StorageError {
    /// Caller invoked `list_by_prefix` on a backend that has no enumeration
    /// support. Lazy-thrown on first stream-yield, not at attach.
    #[error("storage tier {tier:?} does not support list_by_prefix; WAL replay requires it")]
    BackendNoListSupport { tier: String },

    /// Mixed codecs detected within a single WAL — replay refuses to proceed
    /// because frame deserialization would silently corrupt downstream state.
    #[error("codec mismatch: expected {expected:?}, found {found:?}")]
    CodecMismatch { expected: String, found: String },

    /// Codec encode / decode failed (typed values that don't round-trip
    /// through `serde_json`, version-mismatch decode, etc.).
    #[error("codec error: {0}")]
    Codec(#[from] CodecError),

    /// Wraps an underlying backend I/O failure (file system, redb transaction,
    /// network round-trip). The `source` chain preserves the original error.
    #[error("backend error: {message}")]
    BackendError {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    },
}

/// Convenience: `wal_frame_checksum` / `verify_wal_frame_checksum` failures
/// bubble through `?` at the tier-flush boundary without explicit mapping.
/// `ChecksumError::CanonicalJsonFailed` was a `serde_json::Error` at root,
/// which is a codec-encode failure — funnel through the `Codec` variant.
/// `ChecksumError::NonCanonicalContent` (B1, 2026-05-22) is a writer-side
/// rejection of cross-impl-divergent content (non-ASCII keys, subnormal
/// floats); also routed through `Codec::Encode` since it's an encode-time
/// failure surfaced from the same checksum codepath.
impl From<ChecksumError> for StorageError {
    fn from(e: ChecksumError) -> Self {
        match e {
            ChecksumError::CanonicalJsonFailed(err) => {
                StorageError::Codec(CodecError::Encode(err.to_string()))
            }
            ChecksumError::NonCanonicalContent { reason } => {
                StorageError::Codec(CodecError::Encode(reason))
            }
        }
    }
}

/// Replay-time failures from `Graph::restore_snapshot({ mode: "diff" })`.
#[derive(Debug, Error)]
pub enum RestoreError {
    /// A phase's `batch()` rejected. `lifecycle` and `frame_seq` identify the
    /// boundary; `message` carries the underlying cause. Earlier phases stay
    /// committed (Q2 partial-restore semantics).
    #[error("restore phase {lifecycle:?} failed at frame_seq={frame_seq}: {message}")]
    PhaseFailed {
        lifecycle: Lifecycle,
        frame_seq: u64,
        message: String,
    },

    /// A mid-stream frame's checksum mismatched and the
    /// `on_torn_write` policy resolved to "abort". The Q3 default is
    /// "abort" for mid-stream (vs "skip" for WAL tail).
    #[error("torn write mid-stream at frame_seq={frame_seq}: {reason}")]
    TornWriteMidStream { frame_seq: u64, reason: String },

    /// `restore_snapshot({ mode: "diff" })` ran with no `mode:"full"` baseline
    /// in the snapshot tier — replay needs a starting point.
    #[error("no mode=full baseline in snapshot tier; replay requires a baseline")]
    BaselineMissing,

    /// A frame's `format_version` doesn't match the tier's configured codec.
    /// Distinct from [`StorageError::CodecMismatch`] which is tier-level;
    /// this one is replay-time.
    #[error("codec mismatch at frame_seq={frame_seq}: expected {expected:?}, found {found:?}")]
    CodecMismatch {
        frame_seq: u64,
        expected: String,
        found: String,
    },

    /// Caller passed a `source: { tier }` shape without a `wal_tier` when the
    /// snapshot tier itself doesn't carry WAL frames. Distinct from
    /// [`StorageError::BackendNoListSupport`] because the diagnostic is
    /// "you forgot to wire the WAL tier", not "the backend can't enumerate".
    #[error("restore requires a wal_tier when the snapshot tier does not carry WAL frames")]
    WalTierRequired,
}

/// Per-lifecycle phase telemetry within a [`RestoreResult`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PhaseStat {
    pub lifecycle: Lifecycle,
    pub frames: u64,
}

/// Telemetry returned by a successful `restore_snapshot({ mode: "diff" })`
/// call. Every field is observable so tests + dry-run audit paths can pin
/// replay invariants (CLAUDE.md dry-run equivalence rule).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RestoreResult {
    /// Total frames applied across all phases.
    pub replayed_frames: u64,
    /// Frames dropped due to a tail torn-write under `on_torn_write: "skip"`.
    pub skipped_frames: u64,
    /// Highest `frame_seq` applied (zero if no frames replayed).
    pub final_seq: u64,
    /// Per-lifecycle phase breakdown in cross-scope replay order.
    pub phases: Vec<PhaseStat>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restore_result_discriminants_present() {
        let r = RestoreResult {
            replayed_frames: 3,
            skipped_frames: 0,
            final_seq: 42,
            phases: vec![
                PhaseStat {
                    lifecycle: Lifecycle::Spec,
                    frames: 1,
                },
                PhaseStat {
                    lifecycle: Lifecycle::Data,
                    frames: 2,
                },
            ],
        };
        assert_eq!(r.replayed_frames, 3);
        assert_eq!(r.phases.len(), 2);
        assert_eq!(r.phases[0].lifecycle, Lifecycle::Spec);
    }

    #[test]
    fn storage_error_display_carries_tier_name() {
        let e = StorageError::BackendNoListSupport {
            tier: "memory".into(),
        };
        assert!(e.to_string().contains("memory"));
    }

    #[test]
    fn restore_error_phase_failed_includes_seq() {
        let e = RestoreError::PhaseFailed {
            lifecycle: Lifecycle::Data,
            frame_seq: 17,
            message: "downstream invariant broken".into(),
        };
        let s = e.to_string();
        assert!(s.contains("17"), "frame_seq missing from display: {s}");
        assert!(s.contains("Data"), "lifecycle missing from display: {s}");
    }
}
