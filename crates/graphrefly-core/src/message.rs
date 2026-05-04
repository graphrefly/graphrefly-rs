//! Message protocol — the unit of communication between nodes.
//!
//! Mirrors `~/src/graphrefly-ts/src/core/messages.ts` and `GRAPHREFLY-SPEC.md` §1
//! under the handle-protocol cleaving plane: payload-bearing variants
//! (`Data`, `Error`) carry a [`HandleId`] rather than a raw user value.
//! The Core never sees `T`; the binding-side registry resolves handles to values.
//!
//! # Tier table (per canonical spec R1.3.7.b, post-13.6.A lock)
//!
//! | Tier | Variants                          | Purpose                |
//! |------|-----------------------------------|------------------------|
//! | 0    | [`Message::Start`]                | Subscribe handshake    |
//! | 1    | [`Message::Dirty`]                | Phase 1 control        |
//! | 2    | [`Message::Pause`], [`Message::Resume`] | Pause coord       |
//! | 3    | [`Message::Data`], [`Message::Resolved`] | Value delivery   |
//! | 4    | [`Message::Invalidate`]           | Cache clear            |
//! | 5    | [`Message::Complete`], [`Message::Error`] | Termination     |
//! | 6    | [`Message::Teardown`]             | Permanent destruction  |
//!
//! Tier ordering is enforced globally during dispatch (R1.3.1.b two-phase push,
//! R1.3.7 batch coalescing) — DIRTY drains system-wide before tier-3, etc.
//!
//! # Open type set (R1.2.2)
//!
//! The canonical spec allows custom message types. M1 first-slice ships only
//! the 10 built-ins; custom types arrive as a follow-up via a registry shape
//! mirroring `MessageTypeRegistration` in messages.ts.

use crate::handle::{HandleId, LockId};

/// A protocol message.
///
/// Payload-bearing variants carry [`HandleId`] (not user values) per the
/// handle-protocol cleaving plane. `Pause`/`Resume` carry [`LockId`]
/// (mandatory per R1.2.6; bare `[PAUSE]` / `[RESUME]` is a protocol violation).
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Message {
    /// Subscribe-time handshake. Per-subscription; not forwarded through
    /// intermediate nodes (R1.2.3). Tier 0.
    Start,

    /// Phase 1: value about to change. Tier 1 — immediate.
    Dirty,

    /// Phase 2: dirty pass complete, value unchanged (or equals-substituted).
    /// Tier 3 — deferred inside batch.
    Resolved,

    /// Value delivery. The handle never carries the sentinel — `[DATA, undefined]`
    /// / `[DATA, None]` is a protocol violation per R1.2.4. Tier 3.
    Data(HandleId),

    /// Cache clear; does not auto-emit. Tier 4 — deferred.
    Invalidate,

    /// Suspend activity. `lockId` mandatory (R1.2.6). Tier 2 — synchronous.
    Pause(LockId),

    /// Resume after pause. Unknown lockId is an idempotent no-op. Tier 2.
    Resume(LockId),

    /// Clean termination. Tier 5 — deferred.
    Complete,

    /// Error termination. Payload handle MUST resolve to a non-sentinel
    /// value per R1.2.5. Tier 5 — deferred.
    Error(HandleId),

    /// Permanent cleanup; auto-precedes [`Message::Complete`] when delivered
    /// to a non-terminal node (R2.6.4 / Lock 6.F). Tier 6 — deferred (drains last).
    Teardown,
}

impl Message {
    /// Per-message tier (0–6) per R1.3.7.b. Drives ordering in the dispatcher
    /// and gating thresholds (e.g. auto-checkpoint on `tier >= 3`).
    #[must_use]
    pub const fn tier(self) -> u8 {
        match self {
            Self::Start => 0,
            Self::Dirty => 1,
            Self::Pause(_) | Self::Resume(_) => 2,
            Self::Data(_) | Self::Resolved => 3,
            Self::Invalidate => 4,
            Self::Complete | Self::Error(_) => 5,
            Self::Teardown => 6,
        }
    }

    /// True for messages that carry a value handle (`Data`, `Error`).
    /// Useful for the auto-DIRTY-prefix logic (R1.3.1.a) and for the
    /// binding-layer refcount path: payload handles get a refcount bump
    /// at emit time, decremented when the message is consumed.
    #[must_use]
    pub const fn payload_handle(self) -> Option<HandleId> {
        match self {
            Self::Data(h) | Self::Error(h) => Some(h),
            _ => None,
        }
    }

    /// True for the "value already moved" terminal variants.
    /// `Complete` and `Error` are the lifecycle terminators per R1.3.4.a.
    /// `Teardown` is destruction, not termination of message flow per se.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Complete | Self::Error(_))
    }
}

/// A batch of messages delivered as one wire emission.
///
/// Per R1.1.1, all inter-node communication uses `[[Type, Data?], ...]` — the
/// outer batch is mandatory even for a single message. In Rust, we model a
/// batch as a slice; allocation policy is up to the caller (`Vec`, `SmallVec`,
/// stack arrays, or interned static slices for payload-free common cases).
pub type Messages<'a> = &'a [Message];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handle::{HandleId, LockId};

    const HANDLE_42: HandleId = HandleId::new(42);
    const LOCK_7: LockId = LockId::new(7);

    #[test]
    fn tier_table_matches_canonical_spec_r1_3_7b() {
        // Tier 0
        assert_eq!(Message::Start.tier(), 0);
        // Tier 1
        assert_eq!(Message::Dirty.tier(), 1);
        // Tier 2
        assert_eq!(Message::Pause(LOCK_7).tier(), 2);
        assert_eq!(Message::Resume(LOCK_7).tier(), 2);
        // Tier 3
        assert_eq!(Message::Data(HANDLE_42).tier(), 3);
        assert_eq!(Message::Resolved.tier(), 3);
        // Tier 4
        assert_eq!(Message::Invalidate.tier(), 4);
        // Tier 5
        assert_eq!(Message::Complete.tier(), 5);
        assert_eq!(Message::Error(HANDLE_42).tier(), 5);
        // Tier 6
        assert_eq!(Message::Teardown.tier(), 6);
    }

    #[test]
    fn payload_handle_only_for_data_and_error() {
        assert_eq!(Message::Data(HANDLE_42).payload_handle(), Some(HANDLE_42));
        assert_eq!(Message::Error(HANDLE_42).payload_handle(), Some(HANDLE_42));
        assert_eq!(Message::Start.payload_handle(), None);
        assert_eq!(Message::Dirty.payload_handle(), None);
        assert_eq!(Message::Resolved.payload_handle(), None);
        assert_eq!(Message::Invalidate.payload_handle(), None);
        assert_eq!(Message::Pause(LOCK_7).payload_handle(), None);
        assert_eq!(Message::Resume(LOCK_7).payload_handle(), None);
        assert_eq!(Message::Complete.payload_handle(), None);
        assert_eq!(Message::Teardown.payload_handle(), None);
    }

    #[test]
    fn is_terminal_only_complete_and_error() {
        assert!(Message::Complete.is_terminal());
        assert!(Message::Error(HANDLE_42).is_terminal());
        // Teardown is destruction, not lifecycle termination per R1.3.4.a.
        assert!(!Message::Teardown.is_terminal());
        assert!(!Message::Start.is_terminal());
        assert!(!Message::Data(HANDLE_42).is_terminal());
        assert!(!Message::Invalidate.is_terminal());
    }

    #[test]
    fn copy_and_eq_round_trip() {
        let m = Message::Data(HANDLE_42);
        let copy = m;
        assert_eq!(m, copy);
        assert_eq!(Message::Resolved, Message::Resolved);
    }
}
