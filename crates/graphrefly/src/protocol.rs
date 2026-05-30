//! Wave-protocol data types — the decision-locked, language-neutral core.
//!
//! These types are pinned by the spec and the D# log, so they are scaffolded
//! **concretely** (unlike the runtime modules, which are CSP-5 contract stubs).
//! Representation is per-language (D11: in-process tuple vs wire protobuf are
//! decoupled); the *semantics* below match `~/src/graphrefly/spec/rules.jsonl`.

use std::rc::Rc;

/// The erased in-process value type — the Rust analogue of TS's `unknown`
/// (value representation decision, per-language impl, see `CLEAN-SLATE.md`).
///
/// `Rc<dyn Any>` so one DATA payload fans out to N sinks + the node cache +
/// `prev_data` sharing a single allocation via refcount (single-thread D22 ⇒
/// `Rc`, not `Arc`; `dyn Any`, not `dyn Any + Send + Sync`). The substrate moves
/// values erased; the user fn downcasts. A typed `Node<T>` facade re-types the
/// boundary.
pub type AnyValue = Rc<dyn std::any::Any>;

/// Caller-chosen opaque pause-lock identifier (D10).
///
/// `[[PAUSE, lock_id]]` — the caller generates the id; the same id is
/// idempotent; `RESUME` of an unknown id is a no-op. Multiple independent pause
/// sources hold distinct ids in a node's lockset so they cannot fight each other
/// (R-pause-lockset). Opaque on purpose — the substrate never interprets it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LockId(pub String);

impl LockId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl From<String> for LockId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for LockId {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

/// A pool-relative pool callback handle — **pure data, no methods** (D7).
///
/// `(pool_id, handle_id)`. Serializable / snapshotable / wire-transferable. A
/// node is NOT a handle (a node carries `up`/`down`); a handle is inert routing
/// data the dispatcher resolves back to a pending pool callback.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Handle {
    pub pool_id: u32,
    pub handle_id: u32,
}

/// The cross-language "top" error type — error is **unknown** (D31).
///
/// `Node<T>` carries a single value generic; the error channel is untyped to
/// avoid a type-combinatorial explosion. The cross-language analogue is
/// `Box<dyn Error>` / `Exception`. Single-thread (D22) ⇒ no `Send + Sync` bound.
pub type GraphError = Box<dyn std::error::Error + 'static>;

/// The 7-tier const table (D34, amends R-tier numbering).
///
/// Ordering encodes **priority + batch timing**: `immediate` (`< Value`) flows
/// during the current wave; `batch-deferred` (`>= Value`) is held to the batch
/// commit / wave boundary. PAUSE/RESUME sit *below* DIRTY (control before
/// notification). `START` is the 10th handshake type; D9's 9 protocol message
/// types occupy tiers 1–6.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Tier {
    /// 0 — subscribe handshake (`START`).
    Start = 0,
    /// 1 — control (`PAUSE` / `RESUME`).
    Control = 1,
    /// 2 — notification (`DIRTY`).
    Notification = 2,
    /// 3 — value (`DATA` / `RESOLVED`).
    Value = 3,
    /// 4 — settle (`INVALIDATE`).
    Settle = 4,
    /// 5 — terminal (`COMPLETE` / `ERROR`).
    Terminal = 5,
    /// 6 — teardown (`TEARDOWN`).
    Teardown = 6,
}

impl Tier {
    #[inline]
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// Immediate tiers (`< Value`) propagate during the current wave (D34).
    #[inline]
    pub fn is_immediate(self) -> bool {
        (self as u8) < (Tier::Value as u8)
    }

    /// Batch-deferred tiers (`>= Value`) are held to the wave/batch boundary (D34).
    #[inline]
    pub fn is_batch_deferred(self) -> bool {
        !self.is_immediate()
    }
}

/// One protocol message. A `Vec<Message<T>>` ([`Wave`]) is one wave (D8) and may
/// mix tiers. The closed set is 9 protocol kinds (D9) + the `Start` handshake
/// (D34); adding a kind is a constitutional change (`/spec-amend`).
///
/// `Debug` is hand-written to print only the **kind tag** (not the payload), so
/// `Message<AnyValue>` — whose payload `Rc<dyn Any>` is not `Debug` — stays
/// assertable: tests compare wave *shapes* like `["DIRTY", "DATA"]`.
pub enum Message<T> {
    /// Subscribe handshake (substrate-internal, not a user `ctx.up` kind).
    Start,
    /// Acquire a pause lock (control, up-allowed).
    Pause(LockId),
    /// Release a pause lock (control, up-allowed).
    Resume(LockId),
    /// Dirty notification — phase 1 of the two-phase wave (notification, up-allowed).
    Dirty,
    /// A real value (value tier, **down-only**). Absence-of-DATA is the SENTINEL
    /// (`None` per D16), represented at the node's per-dep cache, not as a message.
    Data(T),
    /// Settle with no value change (value tier, **down-only**).
    Resolved,
    /// Invalidate-request — cache-drop, fire `onInvalidate`, cascade (settle,
    /// up-allowed: a depless source honors it at the terminus per D38/R-up-at-source).
    Invalidate,
    /// Terminal success (**down-only**).
    Complete,
    /// Terminal failure carrying the untyped error (**down-only**, D31).
    Error(GraphError),
    /// Teardown (up-allowed; a depless source drops it per D38).
    Teardown,
}

impl<T> Message<T> {
    /// The tier of this message per the D34 const table.
    pub fn tier(&self) -> Tier {
        match self {
            Message::Start => Tier::Start,
            Message::Pause(_) | Message::Resume(_) => Tier::Control,
            Message::Dirty => Tier::Notification,
            Message::Data(_) | Message::Resolved => Tier::Value,
            Message::Invalidate => Tier::Settle,
            Message::Complete | Message::Error(_) => Tier::Terminal,
            Message::Teardown => Tier::Teardown,
        }
    }

    /// Whether this kind may travel **upstream** via `ctx.up` (R-ctx-up).
    ///
    /// Control-tier only: `DIRTY` / `PAUSE` / `RESUME` / `INVALIDATE` / `TEARDOWN`.
    /// `DATA` / `RESOLVED` / `COMPLETE` / `ERROR` are down-only. `START` is a
    /// substrate handshake, not a user `ctx.up` kind.
    pub fn is_up_allowed(&self) -> bool {
        matches!(
            self,
            Message::Dirty
                | Message::Pause(_)
                | Message::Resume(_)
                | Message::Invalidate
                | Message::Teardown
        )
    }
}

impl<T> std::fmt::Debug for Message<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Message::Start => "START",
            Message::Pause(_) => "PAUSE",
            Message::Resume(_) => "RESUME",
            Message::Dirty => "DIRTY",
            Message::Data(_) => "DATA",
            Message::Resolved => "RESOLVED",
            Message::Invalidate => "INVALIDATE",
            Message::Complete => "COMPLETE",
            Message::Error(_) => "ERROR",
            Message::Teardown => "TEARDOWN",
        })
    }
}

/// One wave — a single `msgs` array, may mix tiers (D8).
pub type Wave<T> = Vec<Message<T>>;

#[cfg(test)]
mod tests {
    use super::*;

    // Pin the D34 tier table: ordering + the immediate/batch-deferred cut at Value.
    #[test]
    fn tier_table_numbering_and_cut() {
        assert_eq!(Tier::Start.as_u8(), 0);
        assert_eq!(Tier::Control.as_u8(), 1);
        assert_eq!(Tier::Notification.as_u8(), 2);
        assert_eq!(Tier::Value.as_u8(), 3);
        assert_eq!(Tier::Settle.as_u8(), 4);
        assert_eq!(Tier::Terminal.as_u8(), 5);
        assert_eq!(Tier::Teardown.as_u8(), 6);

        // immediate < Value; batch-deferred >= Value.
        for t in [Tier::Start, Tier::Control, Tier::Notification] {
            assert!(t.is_immediate(), "{t:?} should be immediate");
        }
        for t in [Tier::Value, Tier::Settle, Tier::Terminal, Tier::Teardown] {
            assert!(t.is_batch_deferred(), "{t:?} should be batch-deferred");
        }
    }

    #[test]
    fn message_tier_mapping() {
        assert_eq!(Message::<i32>::Dirty.tier(), Tier::Notification);
        assert_eq!(Message::Data(1).tier(), Tier::Value);
        assert_eq!(Message::<i32>::Resolved.tier(), Tier::Value);
        assert_eq!(
            Message::<i32>::Pause(LockId::new("a")).tier(),
            Tier::Control
        );
        assert_eq!(Message::<i32>::Teardown.tier(), Tier::Teardown);
    }

    // R-ctx-up: control-tier kinds are up-allowed; value/terminal are down-only.
    #[test]
    fn up_allowed_is_control_tier_only() {
        assert!(Message::<i32>::Dirty.is_up_allowed());
        assert!(Message::<i32>::Pause(LockId::new("l")).is_up_allowed());
        assert!(Message::<i32>::Resume(LockId::new("l")).is_up_allowed());
        assert!(Message::<i32>::Invalidate.is_up_allowed());
        assert!(Message::<i32>::Teardown.is_up_allowed());

        assert!(!Message::Data(1).is_up_allowed());
        assert!(!Message::<i32>::Resolved.is_up_allowed());
        assert!(!Message::<i32>::Complete.is_up_allowed());
        assert!(!Message::<i32>::Start.is_up_allowed());
    }
}
