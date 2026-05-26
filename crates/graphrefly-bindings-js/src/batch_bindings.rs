//! `Impl.batch` native arm — sync `BenchBatchContext` napi handle (D289).
//!
//! # Why
//!
//! D282 (`docs/rust-port-decisions.md:2341`) widened the parity `Impl`
//! contract with `batch(fn: (ctx) => void): Promise<void>` so the
//! `batch-throw-rollback` scenarios run cross-arm. The substrate is
//! already conformant via `BatchGuard::discard_wave_cleanup` +
//! `restore_wave_cache_snapshots` (R4.3.2). The napi binding could not
//! honor the contract without a new mechanism: post-D267 every
//! `BenchCore` mutation routes through `actor.run` (async), but the
//! locked `Impl.batch` shape passes a synchronous `fn` body that must
//! invoke `ctx.down(...)` synchronously.
//!
//! D288 (`docs/rust-port-decisions.md:2515`) locked Path D — a sync
//! `BenchBatchContext` napi handle that extends the α-shape's parked-
//! thread + sync-channel pattern to the per-batch-frame scope. D289 is
//! the paired `/porting-to-rs` slice implementing it.
//!
//! # Shape (Q1 lock — `SESSION-DS-D288-native-batch-path-D.md`)
//!
//! - JS calls `bench.openBatch()` (sync `#[napi] fn`). Caller creates a
//!   per-batch-frame `crossbeam_channel::unbounded::<BatchOp>()`. The
//!   `Sender<BatchOp>` is stored in the returned `BenchBatchContext`;
//!   the `Receiver<BatchOp>` is moved into a `dispatch_detached`
//!   closure on the actor.
//! - The actor closure opens a `BatchGuard`, then enters a
//!   `while let Ok(op) = op_rx.recv() { … }` loop — parking the actor
//!   thread inside the guard's lifetime. JS-visible `ctx.down*(...)` /
//!   `commit()` / `rollback()` napi methods push `BatchOp` envelopes
//!   onto the channel; each envelope carries its own per-op
//!   `std::sync::mpsc::sync_channel::<R>(1)` `SyncSender<R>` reply, so
//!   the JS thread blocks on `rx.recv()` until the actor acks. This
//!   mirrors `core_actor::CoreActor::run`'s pattern at
//!   [`core_actor.rs:437`](crate::core_actor) exactly.
//! - **Commit** drops the held `BatchGuard` (success path → drain +
//!   `fire_deferred` + redrain-to-quiescence), acks the reply, and
//!   breaks the loop.
//! - **Rollback** acks first (Q1 lock — JS sees rollback as resolved
//!   before substrate cleanup runs), then `std::panic::resume_unwind`s
//!   the (optional) payload; `BatchGuard::Drop` takes the
//!   `std::thread::panicking()` branch and runs
//!   `discard_wave_cleanup`. The panic is caught by `CoreActor`'s
//!   outer `catch_unwind` ([`core_actor.rs:356`](crate::core_actor)).
//!
//! # Q2 lock — "No sink fire during handle dispatch" invariant
//!
//! While a `BenchBatchContext` is open, the actor thread executes no
//! `bridge_sync*` / no Blocking-TSFN call — R4.3.1/R4.3.2 defer sink
//! fires to commit on the substrate, so the held-guard window is
//! TSFN-free. Enforced at runtime by:
//!
//! - [`DURING_BATCH_HANDLE`] — a thread-local `Cell<bool>` on the actor
//!   worker thread; set true by [`BatchHandleGuard::new`], cleared by
//!   its `Drop` (RAII covers success + panic paths).
//! - [`assert_no_batch_handle`] — called at every `bridge_sync*` entry
//!   point; panics fail-loud if the flag is set.
//!
//! **`Cell<bool>` NOT `AtomicBool`** (D288 Q2 lock): the actor's worker
//! thread is the sole writer (sets on `open_batch`, clears on
//! commit/rollback) AND the sole reader (every `bridge_sync*` call site
//! executes on the actor thread). Precedent: D248 single-owner →
//! D252's `IN_TICK_OWNED` `AHashSet`→`Cell<u64>` collapse →
//! D254 Tier A's `DeferQueue` `AtomicBool`→`Cell<bool>` collapse.
//!
//! # Q3 lock — Per-frame lifetime contract
//!
//! `ctx` is per-frame; **do NOT stash**. Once the JS-side `Impl.batch`
//! wrapper exits (`commit()` or `rollback()`), the actor exits the
//! inner loop; the channel `Receiver` is dropped; any subsequent
//! `ctx.down*(...)` call's `Sender::send` returns `SendError` →
//! mapped to a napi error `"BenchBatchContext used after batch
//! closed"`. Pinned by the `post_frame_ctx_throws` cargo regression
//! test below + the cross-arm regression added in the paired
//! `/dev-dispatch` slice.

// ---------------------------------------------------------------------------
// Imports
// ---------------------------------------------------------------------------

use std::any::Any;
use std::cell::Cell;
use std::sync::mpsc::{sync_channel, SyncSender};
use std::sync::Arc;

use crossbeam_channel::{unbounded, Receiver, Sender};
use graphrefly_core::{Core, HandleId, LockId, NodeId};
use napi::Error as NapiError;
use napi_derive::napi;
use parking_lot::Mutex;

use crate::core_actor::CoreActor;
use crate::core_bindings::{BenchBinding, BenchCore, BenchValue, ResumeReportJs};

// ---------------------------------------------------------------------------
// Thread-local tripwire — Q2 lock
// ---------------------------------------------------------------------------

thread_local! {
    /// Set true on the actor's worker thread while a `BenchBatchContext`
    /// is open; cleared on commit/rollback (success or panic) via
    /// [`BatchHandleGuard::drop`]. Every `bridge_sync*` site reads this
    /// via [`assert_no_batch_handle`] and panics fail-loud if armed.
    ///
    /// **`Cell<bool>` per D288 Q2:** single-owner worker thread (D248)
    /// makes `AtomicBool` redundant capacity (D252 / D254 Tier A
    /// precedent).
    pub(crate) static DURING_BATCH_HANDLE: Cell<bool> = const { Cell::new(false) };

    /// Test-only counter incremented at every `bridge_sync*` entry, AND
    /// at every [`assert_no_batch_handle`] call (so the
    /// `sink_fire_zero` test can assert "no TSFN dispatch happened
    /// during the held-guard window" without an actual TSFN — the
    /// counter is a stand-in for "any code path that would have
    /// dispatched to JS". Increments BEFORE the panic check so a
    /// would-have-fired call is visible in the counter even if the
    /// panic never runs (e.g. the test never crosses the
    /// `bridge_sync*` entry but does call `assert_no_batch_handle`
    /// directly via the test hook).
    #[cfg(test)]
    pub(crate) static BRIDGE_SYNC_INVOCATIONS: Cell<u64> = const { Cell::new(0) };
}

/// RAII setter for [`DURING_BATCH_HANDLE`]. Constructed at the top of
/// the actor's parked-batch closure, dropped when the closure unwinds
/// or returns. Drop is panic-safe so the rollback path also clears the
/// flag.
pub(crate) struct BatchHandleGuard;

impl BatchHandleGuard {
    #[must_use = "BatchHandleGuard is RAII — bind to a named local \
                  (`let _g = BatchHandleGuard::new();`) to keep \
                  DURING_BATCH_HANDLE armed across the parked-batch \
                  closure's lifetime. A discarded result drops \
                  immediately, clearing the flag before any \
                  bridge_sync* tripwire can fire."]
    pub(crate) fn new() -> Self {
        DURING_BATCH_HANDLE.with(|c| {
            debug_assert!(
                !c.get(),
                "D288/Q2: nested BatchHandleGuard observed on actor thread \
                 — a batch is already open. This is structurally impossible \
                 because the actor thread is the sole writer and a parked \
                 closure blocks all other dispatches; reaching this branch \
                 indicates a programming bug in the batch_bindings module."
            );
            c.set(true);
        });
        Self
    }
}

impl Drop for BatchHandleGuard {
    fn drop(&mut self) {
        DURING_BATCH_HANDLE.with(|c| c.set(false));
    }
}

/// Q2 tripwire — call at the top of every `bridge_sync*` function in
/// `operator_bindings`, `structures_bindings`, and the
/// `bridge_sync_unit` in `core_bindings`. Panics fail-loud if the
/// thread-local flag is set, which means a TSFN-bound JS callback is
/// being invoked while a `BenchBatchContext` is open — a 3-way
/// deadlock waiting to happen.
///
/// `callsite` is the static identifier (`module::fn`) for the panic
/// message; included so a stray fire is traceable to its origin.
#[inline]
pub(crate) fn assert_no_batch_handle(callsite: &'static str) {
    #[cfg(test)]
    BRIDGE_SYNC_INVOCATIONS.with(|c| c.set(c.get() + 1));
    DURING_BATCH_HANDLE.with(|c| {
        assert!(
            !c.get(),
            "D288/Q2 invariant violated — `bridge_sync*` called at {callsite} \
             while a BenchBatchContext is open. R4.3.1/R4.3.2 defer sink \
             fires to commit; a TSFN call during the held-guard window \
             3-way deadlocks (libuv busy on the JS thread driving the \
             handle, actor thread parked on sync_channel reply). \
             See archive/docs/SESSION-DS-D288-native-batch-path-D.md Q2."
        );
    });
}

// ---------------------------------------------------------------------------
// BatchMessage / BatchOp
// ---------------------------------------------------------------------------

/// Per-tier substrate-mutation message routed through [`BatchOp::Down`].
///
/// Maps 1:1 to a `Core` public-mutation call. PAUSE/RESUME are NOT
/// here — they need a `LockId` plus return `Result`, so they get their
/// own `BatchOp` variants.
///
/// **Not exposed to napi** — JS-side callers reach this surface via
/// per-tier `BenchBatchContext` methods (`down_int`, `down_complete`,
/// etc.) that intern values + construct the right variant.
#[derive(Debug, Clone, Copy)]
pub(crate) enum BatchMessage {
    /// DATA (tier 3) — `core.emit(node, handle)`.
    Data(HandleId),
    /// COMPLETE (tier 5) — `core.complete(node)`.
    Complete,
    /// ERROR (tier 5) — `core.error(node, error_handle)`.
    Error(HandleId),
    /// INVALIDATE (tier 4) — `core.invalidate(node)`.
    Invalidate,
    /// TEARDOWN (tier 6) — `core.teardown(node)`.
    Teardown,
}

/// One operation envelope on the per-batch-frame channel.
///
/// Each variant carries its own typed `SyncSender<R>` reply so the
/// caller blocks on the matching `rx.recv()` until the actor acks
/// (mirrors `core_actor.rs:437` exactly).
pub(crate) enum BatchOp {
    /// Apply a [`BatchMessage`] downward on a substrate node. Reply
    /// fires `()` after the substrate call returns — Down currently
    /// covers only infallible substrate ops (PAUSE/RESUME are separate
    /// variants below).
    Down {
        node: NodeId,
        msg: BatchMessage,
        reply: SyncSender<()>,
    },
    /// `core.pause(node, lock)` — returns `Result<(), PauseError>`.
    /// Reply carries the result as `Result<(), String>` (substrate
    /// `PauseError` Display-formatted for the napi boundary).
    Pause {
        node: NodeId,
        lock_id: LockId,
        reply: SyncSender<Result<(), String>>,
    },
    /// `core.resume(node, lock)` — returns
    /// `Result<Option<ResumeReport>, PauseError>`. Reply carries
    /// `Result<Option<(u32, u32)>, String>` (replayed/dropped) so the
    /// JS-side wrapper can surface report numbers without an extra
    /// napi struct.
    Resume {
        node: NodeId,
        lock_id: LockId,
        reply: SyncSender<Result<Option<(u32, u32)>, String>>,
    },
    /// Commit the batch — drop the held `BatchGuard` (success path:
    /// drain + `fire_deferred` + redrain-to-quiescence), ack, break
    /// the loop.
    Commit { reply: SyncSender<()> },
    /// Rollback — ack first (so JS sees rollback as resolved), then
    /// `std::panic::resume_unwind(panic_payload)` so the guard's Drop
    /// takes the `std::thread::panicking()` branch and runs
    /// `discard_wave_cleanup`.
    Rollback {
        panic_payload: Option<Box<dyn Any + Send>>,
        reply: SyncSender<()>,
    },
}

// `BatchOp` MUST be `Send` because it crosses the channel between the
// JS-side libuv thread and the actor worker thread. All variant fields
// are `Send` by construction (`NodeId`/`LockId`/`HandleId` are POD,
// `SyncSender<R: Send>` is `Send`, `Box<dyn Any + Send>` is `Send`).
const _: () = {
    const fn assert_send<T: Send>() {}
    assert_send::<BatchOp>();
};

// ---------------------------------------------------------------------------
// Parked-actor inner loop
// ---------------------------------------------------------------------------

/// The parked-batch closure body. Holds the `BatchGuard` for its
/// lifetime; reads `BatchOp` envelopes off `op_rx`; commits / rolls
/// back on the matching variants. Returns when the loop breaks
/// (Commit) or panics (Rollback).
///
/// `Err(RecvError)` (all senders dropped) is the "user forgot to
/// commit/rollback" safety net — the production JS wrapper always
/// calls one or the other before the `BenchBatchContext` napi class
/// drops; this branch handles the case where the napi class is
/// dropped without either (e.g., `BenchBatchContext::Drop` posts a
/// best-effort `Rollback` but the channel may have already been
/// dropped if the actor was shutting down concurrently).
// `op_rx` is taken by value so it drops on function exit — that's
// load-bearing: once the parked-batch closure returns, the Receiver
// drops and any subsequent `op_tx.send(...)` from JS fails (giving
// the "BenchBatchContext used after batch closed" error semantics).
// Passing by `&Receiver` would let the caller hold ownership past
// the closure, breaking the per-frame-lifetime contract (Q3 lock).
#[allow(clippy::needless_pass_by_value)]
fn run_batch_loop(core: &Core, op_rx: Receiver<BatchOp>) {
    // DECL ORDER LOAD-BEARING for the ROLLBACK path (D288 Q2 — closes
    // Edge-Case-Hunter F5): `handle_guard` MUST be declared BEFORE
    // `guard_holder` so that on a Rollback-path panic, Rust's
    // reverse-declaration drop order runs:
    //   1. `guard_holder` (substrate `BatchGuard`) drops first → takes
    //      the `std::thread::panicking()` branch of `BatchGuard::Drop`
    //      (`batch.rs:3832`) → `discard_wave_cleanup` (R4.3.2).
    //   2. Then `handle_guard` drops → clears `DURING_BATCH_HANDLE`.
    // Reversing the decl order would clear the tripwire BEFORE the
    // substrate's panic-cleanup runs, leaving a window where a sink hook
    // fired during `discard_wave_cleanup` could pass the tripwire check —
    // which is exactly the deadlock surface Q2 closes.
    //
    // COMMIT PATH does the OPPOSITE — D291 (closes D290 Case 15a). On a
    // successful commit, `BatchGuard::Drop`'s success path runs
    // `drain_and_flush` + `fire_deferred` → TSFN-backed sinks (built via
    // `subscribe_with_tsfn` → `bridge_sync_unit` at `core_bindings.rs:854`)
    // → `assert_no_batch_handle(...)` tripwire. The tripwire's
    // load-bearing surface is the ROLLBACK path's `discard_wave_cleanup`
    // (a sink hook fired there is the deadlock vector); on the COMMIT
    // path, sinks SHOULD fire (R4.3.5 coalescing) AND must not trip.
    // We resolve this by explicit-dropping `handle_guard` BEFORE
    // `guard_holder.take()` in `BatchOp::Commit` (see below) so
    // `DURING_BATCH_HANDLE` is clear when `BatchGuard::Drop`'s success
    // path fires sinks. The rollback path still relies on the decl-order
    // reverse-drop because rollback never reaches the explicit-drop site
    // (it `resume_unwind`s instead).
    let handle_guard = BatchHandleGuard::new();
    let mut guard_holder = Some(core.begin_batch());
    while let Ok(op) = op_rx.recv() {
        match op {
            BatchOp::Down { node, msg, reply } => {
                apply_batch_message(core, node, msg);
                let _ = reply.send(());
            }
            BatchOp::Pause {
                node,
                lock_id,
                reply,
            } => {
                let result = core.pause(node, lock_id).map_err(|e| format!("{e}"));
                let _ = reply.send(result);
            }
            BatchOp::Resume {
                node,
                lock_id,
                reply,
            } => {
                let result = core
                    .resume(node, lock_id)
                    .map(|opt| opt.map(|r| (r.replayed, r.dropped)))
                    .map_err(|e| format!("{e}"));
                let _ = reply.send(result);
            }
            BatchOp::Commit { reply } => {
                // D291 (closes D290 Case 15a): clear DURING_BATCH_HANDLE
                // BEFORE dropping the BatchGuard so the success-path
                // `fire_deferred` (R4.3.5 coalescing) doesn't trip the
                // tripwire on TSFN-backed sinks routed through
                // `bridge_sync_unit`. The tripwire's load-bearing surface
                // is the ROLLBACK path's `discard_wave_cleanup` (reached
                // via `resume_unwind` from `BatchOp::Rollback` or an
                // unhandled inner-op panic) — that path still gets the
                // tripwire armed because rollback never reaches this
                // explicit drop site (it `resume_unwind`s; the decl-order
                // reverse-drop in `run_batch_loop` ensures `guard_holder`
                // drops first under unwind, with `handle_guard` still
                // armed).
                drop(handle_guard);
                // D291 /qa A3 (2026-05-25): the parked-batch closure
                // owns `guard_holder` until exactly one terminal arm
                // (Commit or Rollback) consumes it; Rollback unwinds
                // via `resume_unwind` and never reaches this site, so
                // by construction `guard_holder.is_some()` here. A
                // refactor that double-consumes via another path would
                // silently no-op the drop → BatchGuard::Drop wouldn't
                // run → success-path drain + `fire_deferred` skipped →
                // silent data loss for the wave's pending DATA. Catch
                // in debug.
                debug_assert!(
                    guard_holder.is_some(),
                    "D288 Q3 / D291 /qa A3: BatchOp::Commit reached with \
                     `guard_holder` already taken — invariant violated. \
                     Each parked batch frame consumes its BatchGuard via \
                     exactly ONE of {{Commit, Rollback}}; another arm \
                     racing through (or a future refactor double-consuming) \
                     would skip BatchGuard::Drop and silently drop the \
                     wave's pending DATA."
                );
                // Now drop guard → BatchGuard::Drop runs the success path
                // (drain + fire_deferred + redrain-to-quiescence +
                // clear_in_tick) with DURING_BATCH_HANDLE clear.
                drop(guard_holder.take());
                let _ = reply.send(());
                break;
            }
            BatchOp::Rollback {
                panic_payload,
                reply,
            } => {
                // Ack FIRST so JS resolves before we unwind (Q1 lock).
                // The actor's outer `catch_unwind` (`core_actor.rs:356`)
                // catches the propagating panic; the JS-side
                // `await ctx.rollback()` already returned by then.
                let _ = reply.send(());
                let payload: Box<dyn Any + Send> = panic_payload.unwrap_or_else(|| {
                    Box::new("BenchBatchContext rolled back via ctx.rollback()")
                });
                std::panic::resume_unwind(payload);
            }
        }
    }
    // Loop exited without Commit (which `break`s with `guard_holder`
    // taken & dropped) or Rollback (which `resume_unwind`s and never
    // reaches this point). The only remaining exit is `RecvError`
    // (`while let Ok(op) = op_rx.recv()` returns None) which is
    // STRUCTURALLY UNREACHABLE: `BatchContextInner::Drop` posts a
    // `BatchOp::Rollback` BEFORE dropping `op_tx`, so the channel
    // never observes "empty + all senders gone" while a
    // `BenchBatchContext` is alive. Reaching here means a future
    // refactor broke the per-frame lifetime contract (D288 Q3).
    //
    // Fail loud rather than silently committing the still-pending
    // wave: panic so `BatchGuard::Drop` takes the
    // `std::thread::panicking()` branch and runs
    // `discard_wave_cleanup` (R4.3.2 — discard, not commit). The
    // actor's outer `catch_unwind` (`core_actor.rs:356`) catches.
    // Closes the Blind-Hunter #10 hazard (silent commit-on-channel-
    // close vs. rollback).
    assert!(
        guard_holder.is_none(),
        "run_batch_loop: RecvError exit path reached while \
         BatchGuard still held — D288 Q3 invariant violated. \
         `BatchContextInner::Drop` MUST post a Rollback before \
         dropping `op_tx`. If this fires, a refactor broke that \
         contract; the wave is panic-discarded via \
         BatchGuard::Drop (R4.3.2)."
    );
}

/// Apply a [`BatchMessage`] to the substrate. Infallible — PAUSE/RESUME
/// have their own [`BatchOp`] variants with `Result`-carrying replies.
fn apply_batch_message(core: &Core, node: NodeId, msg: BatchMessage) {
    match msg {
        BatchMessage::Data(handle) => core.emit(node, handle),
        BatchMessage::Complete => core.complete(node),
        BatchMessage::Error(handle) => core.error(node, handle),
        BatchMessage::Invalidate => core.invalidate(node),
        BatchMessage::Teardown => core.teardown(node),
    }
}

// ---------------------------------------------------------------------------
// BatchContextInner — pure-Rust mechanism, testable without napi env
// ---------------------------------------------------------------------------

/// The non-napi mechanism behind [`BenchBatchContext`]. Holds the
/// channel `Sender<BatchOp>` and a `closed` flag for idempotent
/// commit/rollback. Public-`pub(crate)` so the inline cargo tests can
/// drive it directly without instantiating a napi class.
pub(crate) struct BatchContextInner {
    op_tx: Sender<BatchOp>,
    /// `true` after the first `commit()` or `rollback()` call. Subsequent
    /// `down*` calls return [`crossbeam_channel::SendError`] because the receiver was dropped when
    /// the actor exited the inner loop; this flag short-circuits the
    /// would-be send with a deterministic error.
    closed: Cell<bool>,
}

// `BatchContextInner` is `!Sync` (Cell) — that's fine because the napi
// wrapper's `#[napi]` derive makes the outer class `Send + Sync`
// inheriting from its fields and we wrap this inner in a `Mutex` (see
// `BenchBatchContext::inner` field below). Cell<bool> is Send.
const _: () = {
    const fn assert_send<T: Send>() {}
    assert_send::<BatchContextInner>();
};

impl BatchContextInner {
    /// Open a new batch context against `actor`. Creates the per-frame
    /// crossbeam channel, dispatches the parked closure to the actor,
    /// returns the inner with the `Sender` retained.
    ///
    /// `dispatch_detached` returns `Err(())` only if the actor has
    /// already shut down — surfaced as a napi-mapped error in the
    /// outer wrapper.
    pub(crate) fn open(actor: &Arc<CoreActor>) -> Result<Self, ()> {
        let (op_tx, op_rx) = unbounded::<BatchOp>();
        actor.dispatch_detached(move |core: &Core| {
            run_batch_loop(core, op_rx);
        })?;
        Ok(Self {
            op_tx,
            closed: Cell::new(false),
        })
    }

    /// Apply a [`BatchMessage`] downward. Blocks until the actor acks.
    pub(crate) fn down(&self, node: NodeId, msg: BatchMessage) -> Result<(), String> {
        if self.closed.get() {
            return Err("BenchBatchContext used after batch closed".into());
        }
        let (tx, rx) = sync_channel::<()>(1);
        self.op_tx
            .send(BatchOp::Down {
                node,
                msg,
                reply: tx,
            })
            .map_err(|_| "BenchBatchContext used after batch closed".to_owned())?;
        rx.recv()
            .map_err(|_| "BenchBatchContext: actor reply channel closed (worker panicked OR commit/rollback completed concurrently with handle drop)".into())
    }

    /// `core.pause(node, lock)` inside the batch. Returns the
    /// substrate's `Result<(), PauseError>` as `Result<(), String>`.
    pub(crate) fn pause(
        &self,
        node: NodeId,
        lock_id: LockId,
    ) -> Result<Result<(), String>, String> {
        if self.closed.get() {
            return Err("BenchBatchContext used after batch closed".into());
        }
        let (tx, rx) = sync_channel::<Result<(), String>>(1);
        self.op_tx
            .send(BatchOp::Pause {
                node,
                lock_id,
                reply: tx,
            })
            .map_err(|_| "BenchBatchContext used after batch closed".to_owned())?;
        rx.recv()
            .map_err(|_| "BenchBatchContext: actor reply channel closed (worker panicked OR commit/rollback completed concurrently with handle drop)".into())
    }

    /// `core.resume(node, lock)` inside the batch. Returns the
    /// substrate's `Result<Option<ResumeReport>, PauseError>` as
    /// `Result<Option<(u32, u32)>, String>` — `Option::Some((replayed,
    /// dropped))` on the final-resume of a default-mode node.
    pub(crate) fn resume(
        &self,
        node: NodeId,
        lock_id: LockId,
    ) -> Result<Result<Option<(u32, u32)>, String>, String> {
        if self.closed.get() {
            return Err("BenchBatchContext used after batch closed".into());
        }
        let (tx, rx) = sync_channel::<Result<Option<(u32, u32)>, String>>(1);
        self.op_tx
            .send(BatchOp::Resume {
                node,
                lock_id,
                reply: tx,
            })
            .map_err(|_| "BenchBatchContext used after batch closed".to_owned())?;
        rx.recv()
            .map_err(|_| "BenchBatchContext: actor reply channel closed (worker panicked OR commit/rollback completed concurrently with handle drop)".into())
    }

    /// Commit the batch. Idempotent — second call returns
    /// "already closed" (the actor has already exited the inner loop).
    pub(crate) fn commit(&self) -> Result<(), String> {
        if self.closed.get() {
            return Err("BenchBatchContext used after batch closed".into());
        }
        let (tx, rx) = sync_channel::<()>(1);
        let send_result = self.op_tx.send(BatchOp::Commit { reply: tx });
        // Mark closed BEFORE awaiting reply — Sender::send only fails
        // if the receiver is already dropped, which means the loop
        // exited (commit/rollback raced through another path). In
        // either case the ctx is closed from the user's perspective.
        self.closed.set(true);
        send_result.map_err(|_| "BenchBatchContext used after batch closed".to_owned())?;
        rx.recv()
            .map_err(|_| "BenchBatchContext: actor reply channel closed (worker panicked OR commit/rollback completed concurrently with handle drop)".into())
    }

    /// Rollback the batch with an optional panic payload. The actor
    /// acks the reply BEFORE unwinding (Q1 lock — JS sees rollback
    /// resolved before substrate cleanup runs); the panic is caught
    /// by `CoreActor`'s outer `catch_unwind` so the worker is NOT
    /// bricked.
    pub(crate) fn rollback(
        &self,
        panic_payload: Option<Box<dyn Any + Send>>,
    ) -> Result<(), String> {
        if self.closed.get() {
            return Err("BenchBatchContext used after batch closed".into());
        }
        let (tx, rx) = sync_channel::<()>(1);
        let send_result = self.op_tx.send(BatchOp::Rollback {
            panic_payload,
            reply: tx,
        });
        self.closed.set(true);
        send_result.map_err(|_| "BenchBatchContext used after batch closed".to_owned())?;
        rx.recv()
            .map_err(|_| "BenchBatchContext: actor reply channel closed (worker panicked OR commit/rollback completed concurrently with handle drop)".into())
    }

    /// `true` after the first commit/rollback. Used by `Drop` to
    /// decide whether to fire the user-forgot Rollback safety net.
    fn is_closed(&self) -> bool {
        self.closed.get()
    }
}

impl Drop for BatchContextInner {
    fn drop(&mut self) {
        // Best-effort: if the user dropped the context without calling
        // commit or rollback (e.g., JS-side wrapper buggy, or a future
        // test probes raw behavior), fire a Rollback so the substrate
        // discards the wave cleanly. The reply channel's `_rx` drops
        // immediately — the actor's `let _ = reply.send(())` swallows
        // the resulting `SendError` (benign — Drop has no one to ack
        // back to).
        //
        // Q3 cross-frame leakage check: the `op_tx` we send through
        // belongs to THIS frame's `(op_tx, op_rx)` pair — each
        // `BatchContextInner::open` creates a fresh per-frame channel
        // and the `op_rx` is moved into THIS frame's actor closure.
        // Even if the actor is busy processing a NEXT frame by the
        // time this Rollback is consumed, the message only reaches
        // THIS frame's parked closure (which holds the matching
        // `op_rx`). Cross-frame leakage is impossible by per-frame
        // channel construction (D288 Q3 / per-frame lifetime).
        if !self.is_closed() {
            let (tx, _rx) = sync_channel::<()>(1);
            // Distinguish the Drop-driven Rollback from the explicit
            // `ctx.rollback()` path so panic-hook logs in the actor
            // thread (caught by core_actor's outer catch_unwind) name
            // the actual cause rather than misattributing to
            // `ctx.rollback()`. The user-visible diagnostic stays
            // accurate; the substrate still runs `discard_wave_cleanup`
            // via the `std::thread::panicking()` branch of
            // `BatchGuard::Drop` (R4.3.2 parity).
            let payload: Box<dyn Any + Send> = Box::new(
                "BenchBatchContext dropped without commit/rollback \
                 (safety-net rollback)",
            );
            let _ = self.op_tx.send(BatchOp::Rollback {
                panic_payload: Some(payload),
                reply: tx,
            });
            // Note: we don't block on rx here — Drop can't await, and
            // a blocking recv risks leaking the JS thread if the
            // actor is wedged. The actor will process the Rollback
            // op when it next pulls from op_rx; the wave is discarded
            // asynchronously.
        }
    }
}

// ---------------------------------------------------------------------------
// BenchBatchContext — the `#[napi]` class
// ---------------------------------------------------------------------------

/// JS-facing batch handle (D289 / D288 Path D). One-per-`Impl.batch`
/// frame; per-frame lifetime (do NOT stash — see Q3 lock).
///
/// Constructed via [`BenchCore::open_batch`]. Closed by `commit()` or
/// `rollback()`; subsequent method calls return
/// `"BenchBatchContext used after batch closed"`.
///
/// **Per-tier `down_*` methods** instead of a single tagged `down`
/// because each tier maps to a distinct substrate-mutation call with
/// distinct argument shapes — per-tier methods give a type-safe napi
/// boundary without per-call JSON encode/decode. Mirrors the existing
/// `BenchCore::emit_int` / `core.complete` style.
#[napi]
pub struct BenchBatchContext {
    // The inner mechanism. `Mutex` because the `#[napi]` derive
    // requires `Send + Sync` on the class and `BatchContextInner` is
    // `!Sync` (Cell). All napi methods take `&self`, lock briefly,
    // call through.
    inner: Mutex<BatchContextInner>,
    binding: Arc<BenchBinding>,
}

impl BenchBatchContext {
    pub(crate) fn open(actor: &Arc<CoreActor>, binding: Arc<BenchBinding>) -> napi::Result<Self> {
        let inner = BatchContextInner::open(actor).map_err(|()| {
            NapiError::from_reason("BenchCore::open_batch: actor shut down before batch could open")
        })?;
        Ok(Self {
            inner: Mutex::new(inner),
            binding,
        })
    }
}

#[napi]
impl BenchBatchContext {
    /// Emit a DATA message with an i32 payload on `node_id` (interns
    /// the value via the bench registry first).
    #[napi]
    pub fn down_int(&self, node_id: u32, value: i32) -> napi::Result<()> {
        let handle = self.binding.registry.lock().intern(BenchValue::Int(value));
        self.send_down(node_id, BatchMessage::Data(handle))
    }

    /// Emit a DATA message with a string payload.
    #[napi]
    pub fn down_str(&self, node_id: u32, value: String) -> napi::Result<()> {
        let handle = self.binding.registry.lock().intern(BenchValue::Str(value));
        self.send_down(node_id, BatchMessage::Data(handle))
    }

    /// Emit a DATA message that re-uses an already-interned [`HandleId`]
    /// (e.g., from `BenchCore::intern_int` / `alloc_external_handle`).
    /// No registry intern; passes the raw handle straight to the
    /// substrate. The substrate auto-retains on accept; binding-side
    /// retain discipline is the caller's responsibility.
    #[napi]
    pub fn down_handle(&self, node_id: u32, handle: u32) -> napi::Result<()> {
        self.send_down(
            node_id,
            BatchMessage::Data(HandleId::new(u64::from(handle))),
        )
    }

    /// Emit a COMPLETE message — irreversible terminate at tier 5.
    #[napi]
    pub fn down_complete(&self, node_id: u32) -> napi::Result<()> {
        self.send_down(node_id, BatchMessage::Complete)
    }

    /// Emit an ERROR message with an i32 payload.
    #[napi]
    pub fn down_error_int(&self, node_id: u32, value: i32) -> napi::Result<()> {
        let handle = self.binding.registry.lock().intern(BenchValue::Int(value));
        self.send_down(node_id, BatchMessage::Error(handle))
    }

    /// Emit an ERROR message with a string payload.
    #[napi]
    pub fn down_error_str(&self, node_id: u32, value: String) -> napi::Result<()> {
        let handle = self.binding.registry.lock().intern(BenchValue::Str(value));
        self.send_down(node_id, BatchMessage::Error(handle))
    }

    /// Emit an INVALIDATE message — tier 4.
    #[napi]
    pub fn down_invalidate(&self, node_id: u32) -> napi::Result<()> {
        self.send_down(node_id, BatchMessage::Invalidate)
    }

    /// Emit a TEARDOWN message — tier 6, deactivates the node.
    #[napi]
    pub fn down_teardown(&self, node_id: u32) -> napi::Result<()> {
        self.send_down(node_id, BatchMessage::Teardown)
    }

    /// `core.pause(node, lock_id)` inside the batch. Returns void on
    /// success; throws a napi error on substrate `PauseError`.
    #[napi]
    pub fn down_pause(&self, node_id: u32, lock_id: u32) -> napi::Result<()> {
        let outer = self.inner.lock().pause(
            NodeId::new(u64::from(node_id)),
            LockId::new(u64::from(lock_id)),
        );
        match outer {
            Ok(Ok(())) => Ok(()),
            Ok(Err(msg)) | Err(msg) => Err(NapiError::from_reason(msg)),
        }
    }

    /// `core.resume(node, lock_id)` inside the batch. Returns the
    /// substrate's resume report (`null` for non-final resumes, a
    /// `{ replayed, dropped }` object on the final-resume of a
    /// default-mode node — mirrors the `BenchCore::resume` napi
    /// shape).
    #[napi]
    pub fn down_resume(&self, node_id: u32, lock_id: u32) -> napi::Result<Option<ResumeReportJs>> {
        let outer = self.inner.lock().resume(
            NodeId::new(u64::from(node_id)),
            LockId::new(u64::from(lock_id)),
        );
        match outer {
            Ok(Ok(opt)) => Ok(opt.map(|(replayed, dropped)| ResumeReportJs { replayed, dropped })),
            Ok(Err(msg)) | Err(msg) => Err(NapiError::from_reason(msg)),
        }
    }

    /// Commit the batch — substrate drains + fires deferred sinks.
    /// Idempotent: a second call returns a napi error.
    #[napi]
    pub fn commit(&self) -> napi::Result<()> {
        let result = self.inner.lock().commit();
        result.map_err(NapiError::from_reason)
    }

    /// Rollback the batch — substrate runs `discard_wave_cleanup` +
    /// `restore_wave_cache_snapshots` (R4.3.2). Idempotent.
    #[napi]
    pub fn rollback(&self) -> napi::Result<()> {
        let result = self.inner.lock().rollback(None);
        result.map_err(NapiError::from_reason)
    }

    fn send_down(&self, node_id: u32, msg: BatchMessage) -> napi::Result<()> {
        let result = self.inner.lock().down(NodeId::new(u64::from(node_id)), msg);
        result.map_err(NapiError::from_reason)
    }
}

// ---------------------------------------------------------------------------
// BenchCore::open_batch (lives here to keep the napi class definition
// alongside its factory; the `BenchCore` `#[napi] impl` block can have
// multiple impl blocks per the napi-derive expansion model).
// ---------------------------------------------------------------------------

#[napi]
impl BenchCore {
    /// Open a synchronous batch context (D289 / D288 Path D).
    ///
    /// Returns a [`BenchBatchContext`] handle that JS uses to drive
    /// the batch synchronously: `ctx.down_*(...)`, then either
    /// `ctx.commit()` (R4.3.5 drain + `fire_deferred`) or
    /// `ctx.rollback()` (R4.3.2 `discard_wave_cleanup` +
    /// `restore_wave_cache_snapshots`).
    ///
    /// **Sync napi** — uses `actor.dispatch_detached(...)` to start
    /// the parked closure; the channel `Sender` is held by the
    /// returned context, so subsequent `ctx.*` calls block on the
    /// per-op `sync_channel<R>(1)` reply.
    ///
    /// **Lifetime contract (Q3 lock):** `ctx` is per-frame; do NOT
    /// stash. Once `commit()` / `rollback()` is called, subsequent
    /// `ctx.*` calls return "`BenchBatchContext` used after batch
    /// closed". Pinned by the `post_frame_ctx_throws` cargo
    /// regression test in [`batch_bindings::tests`].
    #[napi]
    pub fn open_batch(&self) -> napi::Result<BenchBatchContext> {
        BenchBatchContext::open(&self.actor, self.binding_arc())
    }
}

// ---------------------------------------------------------------------------
// Cargo regression tests (D289 acceptance bar — 4 tests)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use graphrefly_core::{NodeOpts, NodeRegistration, Sink};
    use std::rc::Rc;

    /// Helper — build a `BenchCore` with a single state-source node S
    /// seeded with `initial`. Returns `(bench, state_node_id)`.
    fn make_bench_with_state(initial: i32) -> (BenchCore, u32) {
        let bench = BenchCore::new();
        let binding = bench.binding_arc();
        let actor = bench.actor.clone();
        let node_id = actor
            .run_sync(move |core| {
                let h = binding.registry.lock().intern(BenchValue::Int(initial));
                let id = core
                    .register(NodeRegistration {
                        deps: vec![],
                        fn_or_op: None,
                        opts: NodeOpts {
                            initial: h,
                            ..Default::default()
                        },
                    })
                    .expect("register state");
                u32::try_from(id.raw()).expect("nid fits u32")
            })
            .expect("run_sync register");
        (bench, node_id)
    }

    /// Test-side mirror of the sink's observable state.
    ///
    /// - `fires`: number of sink invocations (one per `notify` call;
    ///   DIRTY and DATA tiers each produce their own invocation under
    ///   the substrate's two-phase wave engine — `R1.3.1.b`).
    /// - `data_msgs`: total count of DATA-tier messages the sink has
    ///   observed across all invocations (this is what R4.3.5
    ///   coalescing pins — N batched emissions deliver N DATA
    ///   messages, in ONE sink call).
    /// - `last_data_payload`: payload handle of the most recent DATA
    ///   message (sanity for "the last-emitted value reached the
    ///   sink").
    #[derive(Default)]
    struct SinkObservation {
        fires: u64,
        data_msgs: u64,
        last_data_payload: Option<HandleId>,
    }

    /// Helper — subscribe a counter-incrementing substrate Sink (no
    /// TSFN) directly via `Core::subscribe`. Returns the
    /// `Arc<Mutex<SinkObservation>>` the test thread can read.
    ///
    /// Sink is `Rc<dyn Fn(&[Message])>` (not `Send`), so the Sink is
    /// constructed inside the actor closure. We hand its observable
    /// state out as `Arc<Mutex<...>>` so the test thread can read it.
    fn subscribe_counting_sink(bench: &BenchCore, node_id: u32) -> Arc<Mutex<SinkObservation>> {
        let observed: Arc<Mutex<SinkObservation>> =
            Arc::new(Mutex::new(SinkObservation::default()));
        let observed_for_sink = Arc::clone(&observed);
        bench
            .actor
            .run_sync(move |core| {
                let sink: Sink = Rc::new(move |msgs: &[graphrefly_core::Message]| {
                    let mut state = observed_for_sink.lock();
                    state.fires = state.fires.saturating_add(1);
                    for msg in msgs {
                        // Substrate `Message` has tier accessors; we
                        // categorise via `payload_handle()` —
                        // present iff the message carries a DATA or
                        // ERROR payload. We treat ERROR as a
                        // distinct tier here; the parity scenarios
                        // emit ERROR via dedicated `ctx.down_error*`
                        // methods. For sink-fire-zero / commit-once
                        // tests we only emit DATA, so any
                        // `payload_handle()` we observe IS DATA.
                        if let Some(h) = msg.payload_handle() {
                            state.data_msgs = state.data_msgs.saturating_add(1);
                            state.last_data_payload = Some(h);
                        }
                    }
                });
                core.subscribe(NodeId::new(u64::from(node_id)), sink);
            })
            .expect("run_sync subscribe");
        observed
    }

    /// Helper — read the substrate `cache_of(node)` as the i32 stored
    /// behind it (or -1 if `NO_HANDLE` or non-Int).
    fn cache_int(bench: &BenchCore, node_id: u32) -> i32 {
        let binding = bench.binding_arc();
        bench
            .actor
            .run_sync(move |core| {
                let h = core.cache_of(NodeId::new(u64::from(node_id)));
                if h == graphrefly_core::NO_HANDLE {
                    return -1;
                }
                let reg = binding.registry.lock();
                match reg.deref(h) {
                    Some(BenchValue::Int(n)) => n,
                    _ => -1,
                }
            })
            .expect("run_sync cache_int")
    }

    // -----------------------------------------------------------------
    // Test 1 — sink-fire-zero (D289 acceptance #1, DS-D288 (a))
    //
    // Pin the "no sink fire during handle dispatch" invariant: while
    // a `BenchBatchContext` is open, the actor thread invokes no
    // sink calls (no `bridge_sync*` ≡ no TSFN dispatch in the parity
    // arm). The test uses a substrate-direct Sink (no TSFN — JS
    // env-free) and observes the sink's `fires` counter and
    // `data_msgs` accumulator across the held-guard window.
    //
    // Two complementary assertions:
    // 1. No NEW fires occur between `open_batch` and `commit` — the
    //    held window is sink-call-free (R4.3.1/R4.3.2 deferral).
    // 2. After commit, the sink has observed all 3 DATA messages
    //    we emitted (R4.3.5 coalescing: 3 same-node emissions ↦
    //    1 `downWithBatch` call carrying [Data, Data, Data]; the
    //    DIRTY-tier pre-pass may add a separate sink fire under
    //    R1.3.1.b two-phase push, which is correct substrate
    //    behavior and orthogonal to the held-window claim).
    // -----------------------------------------------------------------
    #[test]
    fn sink_fire_zero_during_handle_then_drained_on_commit() {
        let (bench, node) = make_bench_with_state(0);
        let observed = subscribe_counting_sink(&bench, node);

        let baseline_fires = observed.lock().fires;
        let baseline_data = observed.lock().data_msgs;

        let ctx = bench.open_batch().expect("open_batch");

        // Drive several Down ops. Each blocks until the actor acks;
        // the actor processes each inside the held-guard window. No
        // sink fires here because R4.3.5 coalesces emissions to the
        // batch-pending buffer, flushed at BatchGuard drop.
        ctx.down_int(node, 1).expect("down 1");
        ctx.down_int(node, 2).expect("down 2");
        ctx.down_int(node, 3).expect("down 3");

        // Held-window snapshot — no NEW sink fires; no NEW DATA
        // messages observed by the sink.
        {
            let mid = observed.lock();
            assert_eq!(
                mid.fires, baseline_fires,
                "R4.3.1/R4.3.2 violated: sink fired during held-guard window \
                 (baseline_fires {baseline_fires} -> mid {})",
                mid.fires,
            );
            assert_eq!(
                mid.data_msgs, baseline_data,
                "R4.3.1/R4.3.2 violated: sink observed DATA during held-guard \
                 window (baseline_data {baseline_data} -> mid {})",
                mid.data_msgs,
            );
        }

        ctx.commit().expect("commit");

        // After commit, BatchGuard::Drop ran drain + fire_deferred.
        // The sink observed exactly 3 NEW DATA messages (R4.3.5
        // coalescing — N emissions ↦ N DATA messages, delivered in
        // ONE `downWithBatch` call, but the DIRTY tier may add a
        // separate sink fire under R1.3.1.b which is correct).
        {
            let post = observed.lock();
            let new_data = post.data_msgs - baseline_data;
            assert_eq!(
                new_data, 3,
                "R4.3.5 violated: expected 3 new DATA messages after commit \
                 (baseline_data {baseline_data} -> final {})",
                post.data_msgs,
            );
            // At least one new fire — the DATA fire. May be more
            // (DIRTY tier) but never zero given new_data > 0.
            assert!(
                post.fires > baseline_fires,
                "commit must produce at least one new sink fire \
                 (baseline_fires {baseline_fires} -> final {})",
                post.fires,
            );
        }

        // Verify final cache reflects the last-emitted value.
        assert_eq!(cache_int(&bench, node), 3);
    }

    // -----------------------------------------------------------------
    // Test 2 — R4.3.2 panic-rollback (D289 acceptance #2, DS-D288 (b))
    //
    // Drive the binding path (`BenchBatchContext::down_int + rollback()`)
    // and verify the substrate's `BatchGuard::discard_wave_cleanup` +
    // `restore_wave_cache_snapshots` (R4.3.2) fires correctly via the
    // `std::panic::resume_unwind` in `BatchOp::Rollback`. Substrate-
    // direct R4.3.2 (`Core::begin_batch` + closure panic) is pinned
    // separately by `graphrefly-core`'s own cargo tests; this test
    // covers ONLY the binding-path triggering, not cross-path parity
    // (D196 honest pass — Edge-Case-Hunter F7).
    //
    // Companion: cross-IMPL parity (TS vs Rust) lives in the paired
    // `/dev-dispatch` slice's `batch-throw-rollback.test.ts` (12
    // scenarios, currently `runIf(impl.name === "pure-ts")`-gated).
    // -----------------------------------------------------------------
    #[test]
    fn rollback_parity_with_substrate_r4_3_2() {
        // Snapshot cache before; open batch, down(node, 99), rollback;
        // snapshot cache after. After-rollback cache must match
        // before-rollback (= 7) per R4.3.2.
        let (bench, node) = make_bench_with_state(7);
        // Force the initial cached DATA to land before we start the
        // batch — observable via cache_int.
        assert_eq!(cache_int(&bench, node), 7);
        let pre = cache_int(&bench, node);
        let ctx = bench.open_batch().expect("open_batch");
        ctx.down_int(node, 99).expect("down 99");
        // Mid-batch cache: emitted but not yet observable to next-
        // wave consumers because the wave is still held. The
        // substrate snapshot lives in `wave_cache_snapshots`;
        // `cache_of` reflects the in-wave update but the snapshot
        // pre-image is restored on discard. Either way, after
        // rollback the cache MUST equal `pre`.
        ctx.rollback().expect("rollback");
        // Give the actor a moment to finish the panic + catch_unwind +
        // cleanup before reading. The actor's outer loop catches the
        // panic synchronously on the actor thread; the next
        // `run_sync` call from `cache_int` queues behind that cleanup
        // and observes the post-rollback state.
        let post = cache_int(&bench, node);
        assert_eq!(
            post, pre,
            "R4.3.2 violated: post-rollback cache ({post}) != pre-batch cache ({pre})"
        );

        // Bench is still usable post-rollback — actor was not bricked
        // (`core_actor.rs:356` outer `catch_unwind` isolates the
        // panic). Drive a fresh batch to confirm.
        let ctx2 = bench.open_batch().expect("re-open after rollback");
        ctx2.down_int(node, 42).expect("down 42");
        ctx2.commit().expect("re-commit");
        assert_eq!(
            cache_int(&bench, node),
            42,
            "post-rollback bench must still accept new batches"
        );
    }

    // -----------------------------------------------------------------
    // Test 3 — post-frame-ctx-throws (D289 acceptance #3, Q3 lifetime
    // contract)
    //
    // Q3 lock: "post-frame `ctx.down(...)` rejects loudly (napi
    // handle's `Drop` has fired; the next sync call panics through
    // napi as 'BenchBatchContext used after batch closed')". Pin
    // via cargo: commit; verify subsequent `ctx.down_int` returns
    // SendError-mapped napi error.
    // -----------------------------------------------------------------
    #[test]
    fn post_frame_ctx_throws() {
        let (bench, node) = make_bench_with_state(0);
        let ctx = bench.open_batch().expect("open_batch");
        ctx.down_int(node, 1).expect("down 1");
        ctx.commit().expect("commit");

        // Post-commit `ctx.down_int` must fail with the "used after
        // batch closed" error.
        let err = ctx
            .down_int(node, 2)
            .expect_err("post-commit down must fail");
        let reason = format!("{err}");
        assert!(
            reason.contains("BenchBatchContext used after batch closed"),
            "expected closed-batch error, got: {reason}"
        );

        // Same for commit + rollback after close — both should fail.
        let err_commit = ctx.commit().expect_err("post-commit commit must fail");
        assert!(format!("{err_commit}").contains("BenchBatchContext used after batch closed"));
        let err_rb = ctx.rollback().expect_err("post-commit rollback must fail");
        assert!(format!("{err_rb}").contains("BenchBatchContext used after batch closed"));

        // Verify a separate context after rollback close too.
        let ctx2 = bench.open_batch().expect("re-open");
        ctx2.down_int(node, 5).expect("down 5");
        ctx2.rollback().expect("rollback");
        let err2 = ctx2
            .down_int(node, 6)
            .expect_err("post-rollback down must fail");
        assert!(format!("{err2}").contains("BenchBatchContext used after batch closed"));
    }

    // -----------------------------------------------------------------
    // Test 4 — commit fires pending_notify exactly once (D289
    // acceptance #4, DS-D288 (c))
    //
    // Pins R4.3.5 per-node coalescing: N down() calls on the same
    // node inside one batch deliver N DATA messages in ONE
    // `downWithBatch` sink call (NOT N separate sink calls — the
    // substrate accumulates into the batch-pending buffer and
    // flushes once on drain). The DIRTY tier may add a separate
    // sink fire under R1.3.1.b two-phase push; we measure the
    // R4.3.5 invariant via DATA-message count + new-fire-count
    // bound (≤ 2: one DIRTY call + one DATA call), not the raw
    // total.
    // -----------------------------------------------------------------
    #[test]
    fn commit_fires_pending_notify_exactly_once_per_tier() {
        let (bench, node) = make_bench_with_state(0);
        let observed = subscribe_counting_sink(&bench, node);
        let baseline_fires = observed.lock().fires;
        let baseline_data = observed.lock().data_msgs;

        let ctx = bench.open_batch().expect("open_batch");
        for v in 1..=5_i32 {
            ctx.down_int(node, v).expect("down v");
        }
        // No new fires + no new DATA messages during the held window
        // even for N=5 emissions.
        {
            let mid = observed.lock();
            assert_eq!(
                mid.fires, baseline_fires,
                "no fire during held window even for N=5 emissions"
            );
            assert_eq!(
                mid.data_msgs, baseline_data,
                "no DATA observed during held window"
            );
        }
        ctx.commit().expect("commit");
        {
            let post = observed.lock();
            let new_fires = post.fires - baseline_fires;
            let new_data = post.data_msgs - baseline_data;
            // R4.3.5 — exactly 5 DATA messages delivered (coalesced
            // into ONE DATA sink call, not 5 separate ones).
            assert_eq!(
                new_data, 5,
                "R4.3.5: 5 emissions must deliver 5 DATA messages \
                 (baseline_data {baseline_data} -> final {})",
                post.data_msgs,
            );
            // Total new sink invocations bounded: 1 DATA call + at
            // most 1 DIRTY call = 2. Never 5 (per-emission firing
            // would defeat R4.3.5).
            assert!(
                new_fires <= 2,
                "R4.3.5: too many sink fires for coalesced batch \
                 (new_fires {new_fires} > 2, baseline_fires {baseline_fires} -> final {})",
                post.fires,
            );
            assert!(
                new_fires >= 1,
                "commit must produce at least one new sink fire"
            );
        }
        // Final cache reflects the last-emitted value.
        assert_eq!(cache_int(&bench, node), 5);
    }

    // -----------------------------------------------------------------
    // Bonus tripwire test — `assert_no_batch_handle` panics when the
    // thread-local flag is set; passes when clear. Not part of the
    // 4-test acceptance bar but verifies the Q2 mechanism in
    // isolation (the 4 above test it via the sink-fire-zero
    // contract). Quick + cheap; included to make a stray flag-leak
    // trivially diagnosable.
    // -----------------------------------------------------------------
    #[test]
    fn assert_no_batch_handle_panics_when_flag_set() {
        let baseline = BRIDGE_SYNC_INVOCATIONS.with(Cell::get);

        // Flag clear — counter increments, no panic.
        assert_no_batch_handle("test:flag_clear");
        assert_eq!(
            BRIDGE_SYNC_INVOCATIONS.with(Cell::get),
            baseline + 1,
            "counter must increment regardless of flag state"
        );

        // Set the flag manually (RAII guard would block on actor
        // ownership in non-test paths) and expect panic. Suppress
        // the default panic hook around the catch_unwind so the test
        // doesn't pollute stderr with the expected panic message.
        let prior_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let result = std::panic::catch_unwind(|| {
            let _h = BatchHandleGuard::new();
            assert_no_batch_handle("test:flag_set");
        });
        std::panic::set_hook(prior_hook);
        assert!(
            result.is_err(),
            "Q2 tripwire MUST panic when DURING_BATCH_HANDLE is set; \
             a quiet catch_unwind here means the tripwire silently \
             stopped firing (regression hole)."
        );

        // After unwind, RAII Drop cleared the flag.
        assert!(
            !DURING_BATCH_HANDLE.with(Cell::get),
            "BatchHandleGuard::Drop must clear the flag on panic"
        );
    }

    // -----------------------------------------------------------------
    // Test 6 — D291 (closes D290 Case 15a): commit-path tripwire
    // ordering. Pre-fix: `run_batch_loop`'s `BatchOp::Commit` arm did
    // `drop(guard_holder.take())` with `_handle_guard` still alive →
    // `BatchGuard::Drop` success path fires sinks via `fire_deferred`
    // while `DURING_BATCH_HANDLE` is still set. The D289 acceptance
    // test `sink_fire_zero_during_handle_then_drained_on_commit`
    // PASSED because its Rust `Rc` sink does NOT route through
    // `bridge_sync_unit` (where `assert_no_batch_handle` lives); the
    // parity arm's TSFN-backed sinks exposed the divergence at
    // `scenarios/core/batch-throw-rollback.test.ts` Case 15a.
    //
    // D291 fix: explicit `drop(handle_guard)` BEFORE
    // `drop(guard_holder.take())` in the commit arm so the flag is
    // clear when `fire_deferred` runs. Rollback path still arms the
    // tripwire (decl-order reverse-drop on `resume_unwind`).
    //
    // This test pins the structural guarantee from the Rust side:
    // install a substrate `Rc` sink that READS `DURING_BATCH_HANDLE`
    // when fired and stashes the observed bool. After a clean commit,
    // the sink MUST have observed `DURING_BATCH_HANDLE == false`.
    // Pre-fix the flag would be `true` during the fire (and the parity
    // arm's TSFN sink would have panicked through
    // `assert_no_batch_handle`).
    // -----------------------------------------------------------------
    #[test]
    fn d291_commit_path_clears_during_batch_handle_before_sinks_fire() {
        let (bench, node) = make_bench_with_state(0);

        // Install a sink that snapshots DURING_BATCH_HANDLE on every
        // fire. The flag is a thread_local on the actor worker thread;
        // the sink runs on that same thread, so the read is
        // well-defined.
        let observed_flag: Arc<Mutex<Vec<bool>>> = Arc::new(Mutex::new(Vec::new()));
        let observed_for_sink = Arc::clone(&observed_flag);
        bench
            .actor
            .run_sync(move |core| {
                let sink: Sink = Rc::new(move |_msgs: &[graphrefly_core::Message]| {
                    let flag = DURING_BATCH_HANDLE.with(Cell::get);
                    observed_for_sink.lock().push(flag);
                });
                core.subscribe(NodeId::new(u64::from(node)), sink);
            })
            .expect("run_sync subscribe");
        // Drain handshake fire (the subscribe handshake fires the sink
        // once on the initial cached DATA pre-batch; that fire happens
        // OUTSIDE any batch handle, so the snapshot is `false` — which
        // is the post-fix invariant, but it's pre-batch noise we want
        // to drop from the assertion window).
        observed_flag.lock().clear();

        // Open + drive + commit. Pre-fix: `BatchGuard::Drop`'s success
        // path runs `fire_deferred` while `DURING_BATCH_HANDLE` is
        // still set → snapshot records `true`. Post-fix: explicit
        // `drop(handle_guard)` BEFORE `drop(guard_holder.take())` →
        // snapshot records `false`.
        let ctx = bench.open_batch().expect("open_batch");
        ctx.down_int(node, 1).expect("down 1");
        ctx.commit().expect("commit");

        let snapshots = observed_flag.lock().clone();
        assert!(
            !snapshots.is_empty(),
            "D291 sanity: sink must have fired at least once on the commit path"
        );
        for (i, flag) in snapshots.iter().enumerate() {
            assert!(
                !*flag,
                "D291 violated: commit-path sink fire #{i} observed \
                 DURING_BATCH_HANDLE == true. Pre-fix: `_handle_guard` \
                 dropped AFTER `guard_holder.take()` left the flag set \
                 across `BatchGuard::Drop`'s success-path `fire_deferred`, \
                 panicking through `bridge_sync_unit::assert_no_batch_handle` \
                 in the parity arm's TSFN-backed sinks. Snapshots: {snapshots:?}"
            );
        }

        // Sanity — the substrate state mutation landed.
        assert_eq!(cache_int(&bench, node), 1);
    }

    // NOTE — D291 rollback-path invariant ("DURING_BATCH_HANDLE stays
    // armed across `BatchGuard::Drop`'s panic branch") is NOT pinned by
    // its own cargo test. A direct test would need to observe the flag
    // from inside `BatchGuard::Drop`'s panic-discard work, but:
    //   - `std::panic::resume_unwind` (used by `BatchOp::Rollback`)
    //     does NOT invoke the panic hook (stdlib contract), so the
    //     hook-based snapshot mechanism that worked for Test 6 doesn't
    //     fit here.
    //   - `discard_wave_cleanup` deliberately does NOT fire sinks
    //     (that's the whole point), so a substrate `Sink` can't observe
    //     the rollback-path window.
    //   - `OnInvalidate` cleanup hooks are panic-discarded silently
    //     (D061), so a hook-side flag read never executes during
    //     rollback.
    // The invariant IS structurally pinned by:
    //   1. The `DECL ORDER LOAD-BEARING` doc-comment in
    //      `run_batch_loop` documenting why `handle_guard` is declared
    //      BEFORE `guard_holder` (decl-order reverse-drop).
    //   2. The commit-path explicit drop only clears the flag on the
    //      commit branch; rollback's `resume_unwind` doesn't reach the
    //      explicit drop.
    //   3. The `assert_no_batch_handle_panics_when_flag_set` mechanism
    //      test (above) verifies the Q2 tripwire's panic shape — so
    //      any future refactor that accidentally clears the flag on
    //      both branches would surface via the parity arm's TSFN sinks
    //      panicking through `bridge_sync_unit::assert_no_batch_handle`
    //      on the rollback-discard path (the exact deadlock surface Q2
    //      closes).

    // -----------------------------------------------------------------
    // D293 — `CoreActor::shutdown` idempotency + post-shutdown
    // channel-disconnect error mapping.
    //
    // Pins three invariants for the explicit-shutdown path that
    // closes the "process never exits" hang for napi consumers:
    //
    //  1. `shutdown()` is idempotent — second call is a no-op (flag
    //     swap returns true; JoinHandle is None; no panic).
    //  2. Post-shutdown `actor.run` returns the broadened napi error
    //     `"... (actor is shut down or shutting down)"` (D293 /qa
    //     refinement) — the JS-side surfaced shape that lets users
    //     recognize close-vs-other-failure without a separate
    //     `closed: AtomicBool` per-method guard (Q2a lock).
    //  3. `shutdown()` synchronously joins the worker thread, so by
    //     the time it returns the JoinHandle has been consumed
    //     exactly once.
    //
    // No cross-arm regression here — the JS-visible cleanup-test
    // scenario (`scenarios/usage/cleanup.test.ts`) covers the
    // observable Node-process-exits-fast invariant; this cargo test
    // is the substrate-level pin so the binding stays correct under
    // future refactors.
    // -----------------------------------------------------------------
    #[test]
    fn d293_shutdown_idempotent_and_post_close_errors_are_clear() {
        let (bench, node) = make_bench_with_state(0);
        let actor = bench.actor.clone();

        // Pre-shutdown: actor is responsive. The existing `cache_int`
        // helper exercises the actor.run_sync path end-to-end.
        let value_pre = cache_int(&bench, node);
        assert_eq!(
            value_pre, 0,
            "pre-shutdown cache_int should return the seeded value"
        );

        // Trigger explicit shutdown. The worker thread receives the
        // no-op wake closure, observes the flag, breaks; the join
        // completes before `shutdown()` returns.
        actor.shutdown();

        // Invariant 1 — idempotent. Second call must NOT panic
        // (would happen if the JoinHandle.take() returned the same
        // handle twice, or the flag-swap missed the prior `true`).
        actor.shutdown();

        // Invariant 2 — post-shutdown error shape. `actor.run` should
        // return Err with the broadened parenthetical so JS callers
        // (and any future error-classifying middleware) can recognize
        // shutdown-class failures uniformly across the original-race
        // path (Drop) AND the explicit-shutdown path (`shutdown()`).
        // The receiver was dropped when the worker exited the loop;
        // `sender.send(...)` returns `SendError`, which `actor.run`
        // maps to `napi::Error::from_reason`.
        let result_post = actor.run_sync(move |_core| 42_i32);
        let err = result_post
            .expect_err("post-shutdown actor.run_sync should error (worker thread dropped)");
        let err_text = err.to_string();
        assert!(
            err_text.contains("actor is shut down or shutting down"),
            "D293 /qa refinement: post-shutdown error MUST carry the broadened parenthetical \
             so consumers can recognize shutdown-class failures uniformly. Got: {err_text}"
        );

        // Invariant 3 — same post-shutdown shape for the async run.
        // (We can't actually .await here without an async runtime;
        // the sync run_sync exercise above gives identical evidence
        // because both share the `sender.send(...).map_err(...)`
        // branch.)
    }
}
