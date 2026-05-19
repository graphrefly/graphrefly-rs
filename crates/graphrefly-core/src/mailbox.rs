//! `CoreMailbox` — the `Send + Sync` bridge between autonomous async
//! producers (timer tasks) and an owned, relocatable [`crate::node::Core`].
//!
//! # Why this exists (D223 / D225 / D227 / D230)
//!
//! Under the actor / work-stealing model (D221) a `Core` owns its state
//! cell **by value** and moves between workers — so a long-lived async
//! task (e.g. a `tokio::spawn`-ed timer loop) can no longer hold
//! `&Core` / `Arc<C>` / `Weak<C>` to call `Core::emit` directly (that
//! was the deleted `WeakCore` path; D223 forbids `Weak<C>` back-refs).
//!
//! Instead the task holds an `Arc<CoreMailbox>` (`Send + Sync`) and
//! **posts** a `(NodeId, HandleId)` emit request. The mailbox is drained
//! **owner-side** by the synchronous [`crate::node::Core::drain_mailbox`]
//! (applied via the existing sync `Core::emit` — *no async in Core*,
//! honoring the locked "Core never async" invariant). The drain point is
//! the embedder's existing advance/pump site (test harness `TestRuntime`
//! advance helper, napi pump): timer tasks already require the host
//! runtime to be advanced before they fire, so draining there is
//! **behaviour-identical** to the old autonomous `Weak::upgrade →
//! core.emit` (D230).
//!
//! # Shape (D227 full)
//!
//! - **Op queue** — FIFO of [`MailboxOp`]s, applied owner-side.
//! - **`closed`** — set when the owning `Core` drops; a timer task that
//!   observes it releases its pending handle and bails (mirrors the old
//!   `Weak::upgrade() == None` teardown path exactly).
//! - **`runnable`** — the per-group "this Core has work" wake bit. S2b
//!   lands the field + sets/clears it (D227 builds the *full* mailbox
//!   shape now); S4 wires it to wave drain-scoping + finalize, and the
//!   host-executor (M6) reads it to schedule the owning worker. The
//!   in-wave drain (`BatchGuard::drain_and_flush`) already gates on it
//!   so the no-producer §7 floor pays one atomic load, not a mutex.
//!
//! # This is ONE mechanism (not six)
//!
//! The S2b design history records six producer "forks" (D227–D233);
//! that is *design-question* count, not runtime surface. What actually
//! exists is **one** owner-side re-entry mechanism:
//!
//! > A `Send + Sync` FIFO of deferred [`MailboxOp`]s, drained on the
//! > owner thread — **in-wave to quiescence** for producer sinks
//! > (`BatchGuard::drain_and_flush`) and at the embedder pump point for
//! > autonomous timer tasks ([`crate::node::Core::drain_mailbox`]) —
//! > each op applied via the one object-safe owner re-entry surface
//! > [`crate::node::CoreFull`].
//!
//! `MailboxOp` keeps a **typed fast path** (`Emit`/`Complete`/`Error`:
//! zero-alloc, and a deterministic `Core`-gone handle-release contract —
//! see the per-kind `post_*`) plus a **`Defer` escape hatch** (a boxed
//! `FnOnce(&dyn CoreFull)` for value-returning topology mutation:
//! windowing / higher-order inner subscribe). Collapsing the enum to
//! pure `Defer` was considered and rejected — it would heap-allocate per
//! timer/producer emit AND lose the typed `Core`-gone release affordance
//! (a dropped `FnOnce` can't run an else-branch). `ProducerEmitter`
//! (`graphrefly-operators`) is thin sugar over `post_*`; the producer
//! *build* closure isn't a mechanism at all — it uses the `&Core` its
//! `ProducerCtx` already lends it (D231). One queue, one drain loop, one
//! `match`, one re-entry trait.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::handle::{HandleId, NodeId};

/// **`Send`** cross-thread deferred closure (D233; D249/S2c). Posted
/// by an autonomous timer task (`temporal.rs` `window_time`/etc., a
/// `tokio::spawn`ed cross-thread task whose defer closure captures only
/// `Send` state — `Arc<Mutex<NodeId>>` / `Arc<dyn BindingBoundary>` /
/// ids) and applied owner-side. Rides the `Send + Sync` [`CoreMailbox`]
/// (cross-thread post side, drained owner-side via `drain_mailbox`).
pub type SendDeferFn = Box<dyn FnOnce(&dyn crate::node::CoreFull) + Send>;

/// **`!Send`** owner-side deferred closure (D248/D249/S2c). Posted by
/// an owner-side in-wave producer/graph sink whose closure captures
/// `!Send` state (a relaxed `Sink` / `Rc<RefCell<GraphInner>>` —
/// D248). Lives in the owner-only [`DeferQueue`], **never** the
/// cross-thread [`CoreMailbox`].
pub type DeferFn = Box<dyn FnOnce(&dyn crate::node::CoreFull)>;

/// A re-entry request posted to the [`CoreMailbox`] by an autonomous
/// async producer (timer task → `Emit`) or by a producer-operator sink
/// (D232-AMEND/A′ → `Emit`/`Complete`/`Error`). Applied owner-side via
/// the sync `Core::{emit,complete,error}` by [`crate::node::Core::drain_mailbox`]
/// — drained **in-wave to quiescence** by the `BatchGuard` drain loop
/// for producer sinks (immediate, cascade-ordering-preserving), and at
/// the embedder pump point for timer tasks (D230).
pub enum MailboxOp {
    /// `Core::emit(node, handle)`. Posted by timer tasks + producer sinks.
    Emit(NodeId, HandleId),
    /// `Core::complete(node)`. Posted by producer sinks.
    Complete(NodeId),
    /// `Core::error(node, handle)`. Posted by producer sinks.
    Error(NodeId, HandleId),
    /// **`Send`** owner-side closure (D233; D249/S2c). Posted by a
    /// cross-thread timer task (`temporal.rs` `window_time`/etc.) whose
    /// closure captures only `Send` state; applied **in-wave** by the
    /// drain loop (the owner holds `&Core`). The `!Send` owner-side
    /// sink defers (graph describe/observe, control/higher-order
    /// dynamic-inner) go to the separate owner-only [`DeferQueue`]
    /// instead — D248/D249.
    Defer(SendDeferFn),
}

impl std::fmt::Debug for MailboxOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Emit(n, h) => write!(f, "Emit({n:?}, {h:?})"),
            Self::Complete(n) => write!(f, "Complete({n:?})"),
            Self::Error(n, h) => write!(f, "Error({n:?}, {h:?})"),
            Self::Defer(_) => write!(f, "Defer(<send closure>)"),
        }
    }
}

/// `Send + Sync` mailbox bridging autonomous async producers to an owned,
/// relocatable [`crate::node::Core`]. Held behind an `Arc`; the `Core`
/// owns one clone, each timer task another. See the module docs.
pub struct CoreMailbox {
    /// FIFO of [`MailboxOp`]s posted by timer tasks (`Emit`) and
    /// producer-operator sinks (`Emit`/`Complete`/`Error`), applied
    /// owner-side by [`crate::node::Core::drain_mailbox`] via the sync
    /// `Core::{emit,complete,error}`.
    ops: parking_lot::Mutex<VecDeque<MailboxOp>>,
    /// Set by [`Self::close`] when the owning `Core` drops. A timer task
    /// observing `true` from [`Self::post_emit`] is told to release its
    /// pending handle and bail (the old `Weak::upgrade() == None` path).
    closed: AtomicBool,
    /// Per-group "this Core has queued work" wake bit (D227 full shape).
    /// Set on every successful [`Self::post_emit`]; cleared by
    /// [`Self::drain_into`] once the queue empties. S4 wires it to wave
    /// drain-scoping + finalize; M6 reads it from the host executor.
    runnable: AtomicBool,
}

impl CoreMailbox {
    /// A fresh, open, empty mailbox.
    #[must_use]
    pub fn new() -> Self {
        Self {
            ops: parking_lot::Mutex::new(VecDeque::new()),
            closed: AtomicBool::new(false),
            runnable: AtomicBool::new(false),
        }
    }

    /// Post a timer-fired emit request. Returns `false` iff the owning
    /// `Core` has already dropped ([`Self::close`] was called) — the
    /// caller MUST then release `handle` and stop (mirrors the old
    /// `WeakCore::upgrade() == None` teardown branch in `timer.rs`).
    /// Returns `true` when queued (and sets the `runnable` wake bit).
    #[must_use]
    pub fn post_emit(&self, node_id: NodeId, handle: HandleId) -> bool {
        self.post_op(MailboxOp::Emit(node_id, handle))
    }

    /// Post a producer-sink `Complete` (D232-AMEND/A′). Returns `false`
    /// iff the owning `Core` is gone (caller stops; nothing to release).
    #[must_use]
    pub fn post_complete(&self, node_id: NodeId) -> bool {
        self.post_op(MailboxOp::Complete(node_id))
    }

    /// Post a producer-sink `Error` (D232-AMEND/A′). Returns `false` iff
    /// the owning `Core` is gone — the caller MUST then release
    /// `handle` (it owned a retain for the would-be `error` payload).
    #[must_use]
    pub fn post_error(&self, node_id: NodeId, handle: HandleId) -> bool {
        self.post_op(MailboxOp::Error(node_id, handle))
    }

    /// Post a **`Send`** cross-thread `Defer` (D249/S2c). For an
    /// autonomous timer task whose closure captures only `Send` state
    /// (`temporal.rs` `window_time`/etc.). Returns `false` iff the
    /// owning `Core` is gone — the closure is dropped unrun. The
    /// `!Send` owner-side sink defers use [`DeferQueue::post`] instead.
    #[must_use]
    pub fn post_defer(&self, f: SendDeferFn) -> bool {
        self.post_op(MailboxOp::Defer(f))
    }

    /// Post a [`MailboxOp`]. Returns `false` iff the owning `Core` has
    /// already dropped ([`Self::close`]) — see the per-kind wrappers for
    /// the caller's handle-release obligation.
    ///
    /// QA F-A (2026-05-18): the `closed` check and the `push_back` are
    /// performed **in one `ops`-lock critical section** so a concurrent
    /// owner-thread `close()` (which also takes `ops`) cannot interleave
    /// between "observed not-closed" and "enqueued" — that TOCTOU would
    /// strand the op (with its retained `HandleId`) in a queue
    /// `Drop for Core` already walked → leak. `close()` takes the same
    /// lock, so the two are mutually exclusive.
    #[must_use]
    pub fn post_op(&self, op: MailboxOp) -> bool {
        let mut q = self.ops.lock();
        if self.closed.load(Ordering::Acquire) {
            return false;
        }
        q.push_back(op);
        self.runnable.store(true, Ordering::Release);
        true
    }

    /// Owner-side drain. Pops every queued [`MailboxOp`] in FIFO order
    /// and hands each to `apply` (the caller passes a closure over the
    /// sync `Core::{emit,complete,error}`). Re-entrancy: `apply` may
    /// itself cascade and a concurrent timer task / re-entrant sink may
    /// post again — a fresh post re-sets `runnable`, so the enclosing
    /// drain-to-quiescence loop (or a later drain) picks it up.
    ///
    /// QA F-#4 (2026-05-18): the empty observation and the
    /// `runnable = false` store happen **in the same `ops`-lock critical
    /// section. Previously the empty `pop_front` released the lock before
    /// storing `false`, so a concurrent `post_op` (which takes `ops`)
    /// could push and set `runnable=true` in between, then our `false`
    /// store would clobber it — a lost wakeup invisible to the
    /// `is_runnable`-gated in-wave drain and the S4/M6 scheduler.
    /// Clearing `runnable` while still holding the lock every `post_op`
    /// must take orders them: a post is either popped here or runs
    /// strictly after the `false` store and re-sets `true`.
    /// `max_ops` bounds a single drain (QA P3, 2026-05-18): `apply` may
    /// re-post (a `Defer` that re-`defer`s, a producer re-subscribing),
    /// which this loop correctly drains in the *same* call — but a
    /// closure that re-posts itself on *every* application is an
    /// unbounded mailbox livelock (the producer-authoring analogue of a
    /// fn that emits to itself). That livelock lives HERE (the inner
    /// drain loop), not in `drain_and_flush`'s fire-cascade `cap`, so it
    /// is bounded + panics here — decoupled from the fire counter so a
    /// legitimately large finite producer drain never false-trips the
    /// fire `cap`.
    pub fn drain_into(&self, max_ops: u32, mut apply: impl FnMut(MailboxOp)) {
        let mut applied = 0u32;
        loop {
            let op = {
                let mut q = self.ops.lock();
                let Some(op) = q.pop_front() else {
                    self.runnable.store(false, Ordering::Release);
                    return;
                };
                op
            };
            applied += 1;
            assert!(
                applied < max_ops,
                "mailbox drain exceeded {max_ops} ops in one drain — a \
                 producer/Defer op is re-posting itself every application \
                 (mailbox livelock). Tune via \
                 Core::set_max_batch_drain_iterations only with concrete \
                 evidence the workload needs more."
            );
            apply(op);
        }
    }

    /// Whether the mailbox currently holds queued work (the wake bit).
    /// Advisory pre-M6 (the embedder pump drains unconditionally); S4/M6
    /// consumers gate scheduling on it.
    #[must_use]
    pub fn is_runnable(&self) -> bool {
        self.runnable.load(Ordering::Acquire)
    }

    /// Mark the owning `Core` gone. Idempotent. Takes the `ops` lock so
    /// it is mutually exclusive with [`Self::post_op`]'s under-lock
    /// `closed` check (QA F-A): after this returns, no further `post_op`
    /// can enqueue. Callers MUST then [`Self::take_all`] and release any
    /// `Emit`/`Error` payload handles still queued (a TOCTOU-enqueued op
    /// posted just before `close` won the lock).
    pub fn close(&self) {
        let _q = self.ops.lock();
        self.closed.store(true, Ordering::Release);
    }

    /// Drain and return every still-queued [`MailboxOp`] without
    /// applying it — for `Drop for Core` teardown (QA F-A / Blind #2).
    /// `Emit`/`Error` ops carry a retained `HandleId` the caller must
    /// release; `Defer` closures are dropped unrun (running `CoreFull`
    /// on a half-dropped `Core` is unsound — user-locked QA decision A,
    /// 2026-05-18). Clears `runnable` under the lock (same race
    /// discipline as `drain_into`).
    #[must_use]
    pub fn take_all(&self) -> VecDeque<MailboxOp> {
        let mut q = self.ops.lock();
        self.runnable.store(false, Ordering::Release);
        std::mem::take(&mut *q)
    }

    /// Whether [`Self::close`] has been called.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

impl Default for CoreMailbox {
    fn default() -> Self {
        Self::new()
    }
}

// `CoreMailbox` is `Send + Sync` by construction (parking_lot::Mutex +
// atomics over the id-only `MailboxOp`). Asserted so a future field
// that breaks it fails here, not at the `Arc<CoreMailbox>` share site
// in `timer.rs`. D249/S2c: this MUST hold — the `!Send` owner-side
// `Defer` payload now lives in [`DeferQueue`], NOT here.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<CoreMailbox>();
};

/// Owner-only deferred-closure queue (D249/S2c — the minimal Defer
/// split pulled out of [`CoreMailbox`]).
///
/// Under D248 full single-owner the substrate `Sink`/`TopologySink`
/// dropped `Send + Sync`, so a [`DeferFn`] (which captures relaxed
/// `Sink`s / `Rc<RefCell<GraphInner>>`) is `!Send`. It cannot ride the
/// `Send + Sync` [`CoreMailbox`] (that bridges the `timer.rs`
/// cross-thread `Arc<CoreMailbox>` post side). This queue is therefore
/// **owner-only and `!Send`**: held behind an `Rc` shared between
/// [`crate::node::Core`] and `graphrefly-operators`' `ProducerEmitter`
/// on the one owner thread. Drained owner-side by
/// [`crate::node::Core::drain_mailbox`] (after the id-mailbox) and the
/// in-wave `BatchGuard` drain.
///
/// S4 still does the per-group-wake + typed snapshot/prune `MailboxOp`
/// reshape (D246 rule 8) — D249 is the acknowledged minimal first
/// touch that lets the D248 single-owner Sink relaxation land in S2c.
pub struct DeferQueue {
    q: parking_lot::Mutex<VecDeque<DeferFn>>,
    closed: std::sync::atomic::AtomicBool,
    /// "Has queued work" bit — mirrors `CoreMailbox::runnable` so the
    /// in-wave `BatchGuard` drain gate (`is_runnable()`) also fires
    /// when only a `Defer` (no id-`MailboxOp`) was posted mid-wave
    /// (the reactive describe/observe `DepsChanged` path, D246 r6).
    runnable: std::sync::atomic::AtomicBool,
}

impl DeferQueue {
    /// A fresh, open, empty owner-side defer queue.
    #[must_use]
    pub fn new() -> Self {
        Self {
            q: parking_lot::Mutex::new(VecDeque::new()),
            closed: std::sync::atomic::AtomicBool::new(false),
            runnable: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Whether the queue currently holds work (the in-wave drain gate).
    #[must_use]
    pub fn is_runnable(&self) -> bool {
        self.runnable.load(Ordering::Acquire)
    }

    /// Enqueue an owner-side deferred closure. Returns `false` iff the
    /// owning `Core` has dropped ([`Self::close`]) — the caller's
    /// closure is dropped unrun (same contract as the old
    /// `CoreMailbox::post_defer`). Mirrors `CoreMailbox::post_op`'s
    /// under-lock `closed` check (TOCTOU-safe vs a concurrent `close`).
    #[must_use]
    pub fn post(&self, f: DeferFn) -> bool {
        let mut q = self.q.lock();
        if self.closed.load(Ordering::Acquire) {
            return false;
        }
        q.push_back(f);
        self.runnable.store(true, Ordering::Release);
        true
    }

    /// Owner-side drain. Pops every queued closure FIFO and applies it
    /// (re-entrancy-safe: a closure may re-`post`; a later drain picks
    /// it up). `max_ops` bounds one drain against a self-re-posting
    /// livelock (mirrors `CoreMailbox::drain_into`).
    ///
    /// # Panics
    /// Panics if a single drain applies `max_ops` closures — a `Defer`
    /// closure re-posting itself every application (owner-side
    /// livelock), the defer-queue analogue of a fn that emits to itself.
    pub fn drain_into(&self, max_ops: u32, mut apply: impl FnMut(DeferFn)) {
        let mut applied = 0u32;
        loop {
            let f = {
                let mut q = self.q.lock();
                let Some(f) = q.pop_front() else {
                    // Clear `runnable` under the lock (same race
                    // discipline as `CoreMailbox::drain_into` QA F-#4).
                    self.runnable.store(false, Ordering::Release);
                    return;
                };
                f
            };
            applied += 1;
            assert!(
                applied < max_ops,
                "defer-queue drain exceeded {max_ops} closures in one \
                 drain — a Defer closure is re-posting itself every \
                 application (owner-side livelock)."
            );
            apply(f);
        }
    }

    /// Mark the owning `Core` gone. Idempotent; mutually exclusive with
    /// [`Self::post`] (shared `q` lock). Queued closures are dropped
    /// unrun on `Core` drop (running `CoreFull` on a half-dropped
    /// `Core` is unsound — user-locked QA decision A, 2026-05-18).
    pub fn close(&self) {
        let _q = self.q.lock();
        self.closed.store(true, Ordering::Release);
    }
}

impl Default for DeferQueue {
    fn default() -> Self {
        Self::new()
    }
}
