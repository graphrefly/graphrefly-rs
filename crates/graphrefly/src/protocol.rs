//! Wave-protocol data types — the decision-locked, language-neutral core.
//!
//! These types are pinned by the spec and the D# log, so they are concrete Rust
//! representations of the current protocol surface.
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

/// A pool-relative pool callback handle — **pure data, no methods** (D7).
///
/// `(pool_id, handle_id)` per D7/DR-2; the Rust pool adds a local `generation`
/// (B32) so a recycled slotmap slot can be told apart from a stale handle (the
/// pool frees a dropped node's fn slot and reuses it). `generation` is a
/// PER-LANGUAGE impl detail (D24), NOT part of the protocol/IDL Handle — it is
/// wire-meaningless and is dropped at the wire boundary (a wired handle is
/// re-resolved against the remote pool). User-approved Handle-widening 2026-05-29
/// (see `CLEAN-SLATE.md`). A node is NOT a handle; a handle is inert routing data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Handle {
    /// `pool_id` field for pool id.
    pub pool_id: u32,
    /// `handle_id` field for handle id.
    pub handle_id: u32,
    /// Local slotmap generation (B32, per-language) — NOT in the protocol IDL.
    pub generation: u32,
}

/// The cross-language "top" error type — error is **unknown** (D31).
///
/// `Node<T>` carries a single value generic; the error channel is untyped to
/// avoid a type-combinatorial explosion. The cross-language analogue is
/// `Box<dyn Error>` / `Exception`. Single-thread (D22) ⇒ no `Send + Sync` bound.
pub type GraphError = Box<dyn std::error::Error + 'static>;

/// One protocol message (R-messages). The type set is closed: START, PENDING,
/// DATA, RESOLVED, INVALIDATE, COMPLETE and ERROR (graphrefly:D797, active by
/// graphrefly:D798). Messages travel only downstream; there is no upward channel
/// and no control message: pausing, demand or restart is a write to a state node
/// the target declares as a dependency (R-no-imperative). TEARDOWN, PAUSE, RESUME
/// and PULL are not representable.
///
/// `Debug` is hand-written to print only the **kind tag** (not the payload), so
/// `Message<AnyValue>` — whose payload `Rc<dyn Any>` is not `Debug` — stays
/// assertable: tests compare array *shapes* like `["PENDING", "DATA"]`.
pub enum Message<T> {
    /// Subscribe handshake, followed by the node's snapshot (R-subscription).
    Start,
    /// Kernel-only first message of each settling array (R-wave); formerly DIRTY.
    /// Node output containing PENDING is rejected.
    Pending,
    /// A real value. Absence of DATA is the SENTINEL (`None` per D16), represented
    /// at the node's per-dependency record, not as a message.
    Data(T),
    /// Settles a pending node that sent nothing (R-wave). Kernel-only: node output
    /// containing RESOLVED is rejected (graphrefly:D799).
    Resolved,
    /// Removes the retained value of a node that has one (R-invalidation).
    Invalidate,
    /// Clean end of the node's current instance (R-terminals).
    Complete,
    /// Error end of the node's current instance, carrying the untyped error (D31).
    Error(GraphError),
}

impl<T> Message<T> {
    /// The message's class rank in R-messages order: START, PENDING, value (DATA,
    /// RESOLVED), INVALIDATE, terminal (COMPLETE, ERROR).
    #[inline]
    pub(crate) fn class_rank(&self) -> u8 {
        match self {
            Message::Start => 0,
            Message::Pending => 1,
            Message::Data(_) | Message::Resolved => 2,
            Message::Invalidate => 3,
            Message::Complete | Message::Error(_) => 4,
        }
    }

    /// Whether this message is a value (DATA or RESOLVED) or a later class, the part
    /// of an array a batch holds until its commit.
    #[inline]
    pub(crate) fn is_settling(&self) -> bool {
        self.class_rank() >= 2
    }

    /// Whether this message is COMPLETE or ERROR.
    #[inline]
    pub fn is_terminal(&self) -> bool {
        matches!(self, Message::Complete | Message::Error(_))
    }

    /// Whether only the kernel may send this message (PENDING and RESOLVED, R-wave).
    #[inline]
    pub fn is_kernel_only(&self) -> bool {
        matches!(self, Message::Pending | Message::Resolved)
    }
}

impl<T> std::fmt::Debug for Message<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Message::Start => "START",
            Message::Pending => "PENDING",
            Message::Data(_) => "DATA",
            Message::Resolved => "RESOLVED",
            Message::Invalidate => "INVALIDATE",
            Message::Complete => "COMPLETE",
            Message::Error(_) => "ERROR",
        })
    }
}

/// One array of messages delivered by one call (R-wave).
pub type Wave<T> = Vec<Message<T>>;

/// Rejects node output that only the kernel may send (R-wave, R-failure).
///
/// PENDING and RESOLVED are the kernel's alone; an array carrying either is
/// rejected before anything of it is applied. The rejection panics, the Rust
/// form of a throw from `ctx.down`: uncaught, it becomes the node's ERROR at
/// the wave boundary.
pub(crate) fn reject_kernel_only_output<T>(msgs: &[Message<T>]) {
    if let Some(m) = msgs.iter().find(|m| m.is_kernel_only()) {
        panic!(
            "node output may not contain {m:?}: PENDING and RESOLVED are sent only by the kernel (R-wave)"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // R-messages: the class order of the closed set.
    #[test]
    fn class_rank_follows_message_class_order() {
        assert_eq!(Message::<i32>::Start.class_rank(), 0);
        assert_eq!(Message::<i32>::Pending.class_rank(), 1);
        assert_eq!(Message::Data(1).class_rank(), 2);
        assert_eq!(Message::<i32>::Resolved.class_rank(), 2);
        assert_eq!(Message::<i32>::Invalidate.class_rank(), 3);
        assert_eq!(Message::<i32>::Complete.class_rank(), 4);
    }

    // R-wave: PENDING and RESOLVED are kernel-only.
    #[test]
    fn kernel_only_messages_are_pending_and_resolved() {
        assert!(Message::<i32>::Pending.is_kernel_only());
        assert!(Message::<i32>::Resolved.is_kernel_only());
        assert!(!Message::Data(1).is_kernel_only());
        assert!(!Message::<i32>::Invalidate.is_kernel_only());
        assert!(!Message::<i32>::Complete.is_kernel_only());
    }
}
