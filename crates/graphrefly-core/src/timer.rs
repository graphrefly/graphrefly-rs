//! Tokio-based timer substrate for deferred emissions.
//!
//! Feature-gated behind `tokio`. Core's dispatcher stays fully synchronous;
//! this module provides a minimal async boundary for timer scheduling.
//!
//! # Design (2026-05-11)
//!
//! Timer-dependent operators (debounce, throttle, delay, audit) and
//! infrastructure consumers (storage `debounce_ms`) need to schedule
//! deferred work — "emit this handle in 50ms" or "flush after 200ms of
//! quiet." These are async concerns that Core's sync dispatcher can't
//! own directly.
//!
//! The substrate provides:
//!
//! - [`TimerTaskHandle`] — command channel from sync code to a tokio task.
//!   Non-blocking send; drop-to-shutdown.
//! - [`spawn_timer_task`] — spawns a tokio task that manages tagged timer
//!   slots for one node, wires the channel, returns the handle.
//!
//! Operators send [`TimerCmd`] from their sync fn-fire to schedule,
//! cancel, or cancel-all timers. The task manages deadlines via
//! `tokio::time` and calls `Core::emit()` when a timer fires, which
//! enters the wave engine via the normal cross-thread emit path
//! (partition `wave_owner` serializes).
//!
//! Timer sources (fromTimer, interval) use the producer substrate +
//! this module: the producer's build closure spawns a timer task on
//! activation; deactivation drops the handle, shutting down the task.
//!
//! # Testing
//!
//! Use `tokio::time::pause()` + `tokio::time::advance()` for
//! deterministic timer control in tests. No mock infrastructure needed.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use crate::boundary::BindingBoundary;
use crate::handle::{HandleId, NodeId};
use crate::mailbox::CoreMailbox;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Callback invoked when a timer fires. Runs on the tokio task thread.
/// Must not block or hold locks across `Core::emit`.
pub type TimerCallback = Box<dyn Fn() + Send + Sync>;

/// Command sent from the operator fn (sync) to the timer task (async).
pub enum TimerCmd {
    /// Schedule a timer: after `delay`, emit `handle` on the node.
    /// If a timer with the same `tag` is already pending, it is cancelled
    /// and the old handle is released.
    Schedule {
        tag: u32,
        delay: Duration,
        handle: HandleId,
    },
    /// Schedule a callback-only timer: after `delay`, invoke `callback`.
    /// No handle emission — the callback manages state and may call
    /// `Core::emit` itself. Used by operators (debounce, throttle) that
    /// need finer control than the emit-on-fire pattern.
    ScheduleCallback {
        tag: u32,
        delay: Duration,
        callback: TimerCallback,
    },
    /// Cancel a pending timer by tag. The held handle (if any) is released.
    /// Callback-only timers have no handle to release.
    Cancel { tag: u32 },
    /// Cancel all pending timers and release all held handles.
    CancelAll,
}

impl std::fmt::Debug for TimerCmd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Schedule { tag, delay, handle } => f
                .debug_struct("Schedule")
                .field("tag", tag)
                .field("delay", delay)
                .field("handle", handle)
                .finish(),
            Self::ScheduleCallback { tag, delay, .. } => f
                .debug_struct("ScheduleCallback")
                .field("tag", tag)
                .field("delay", delay)
                .finish_non_exhaustive(),
            Self::Cancel { tag } => f.debug_struct("Cancel").field("tag", tag).finish(),
            Self::CancelAll => write!(f, "CancelAll"),
        }
    }
}

/// Sender half — stored in the operator's scratch for sync command dispatch.
pub type TimerSender = mpsc::UnboundedSender<TimerCmd>;

/// Handle to a running timer task. Dropping this shuts down the task.
#[must_use]
pub struct TimerTaskHandle {
    tx: Option<mpsc::UnboundedSender<TimerCmd>>,
}

impl TimerTaskHandle {
    /// Get a clone of the sender for use in operator scratch.
    ///
    /// # Panics
    ///
    /// Panics if called after `shutdown()`.
    #[must_use]
    pub fn sender(&self) -> TimerSender {
        self.tx
            .as_ref()
            .expect("TimerTaskHandle: sender already taken via shutdown")
            .clone()
    }

    /// Explicitly shut down the task. Sends `CancelAll` then drops the
    /// sender so the task exits cleanly.
    pub fn shutdown(&mut self) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(TimerCmd::CancelAll);
            // Drop tx -> channel closes -> task exits on next poll.
        }
    }
}

impl Drop for TimerTaskHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

// ---------------------------------------------------------------------------
// Task spawn
// ---------------------------------------------------------------------------

/// Spawn a timer task for a single operator/source node.
///
/// The task runs until the channel closes (sender dropped). It manages
/// pending timers tagged by `u32`. When a timer fires, it calls
/// `core.emit(node_id, handle)`.
///
/// # Panics
///
/// Must be called from within a tokio runtime context.
pub fn spawn_timer_task(
    mailbox: Arc<CoreMailbox>,
    node_id: NodeId,
    binding: Arc<dyn BindingBoundary>,
) -> TimerTaskHandle {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(timer_task_loop(rx, mailbox, node_id, binding));
    TimerTaskHandle { tx: Some(tx) }
}

// ---------------------------------------------------------------------------
// Task loop
// ---------------------------------------------------------------------------

/// Internal: the async task that manages timers for one node.
async fn timer_task_loop(
    mut rx: mpsc::UnboundedReceiver<TimerCmd>,
    mailbox: Arc<CoreMailbox>,
    node_id: NodeId,
    binding: Arc<dyn BindingBoundary>,
) {
    let mut slots: Vec<TimerSlot> = Vec::new();

    loop {
        // Find the nearest deadline among active slots.
        let next_fire = nearest_deadline(&slots);

        tokio::select! {
            biased; // Prefer commands over timer fires to process cancels first.

            cmd = rx.recv() => {
                match cmd {
                    Some(TimerCmd::Schedule { tag, delay, handle }) => {
                        cancel_slot(&mut slots, tag, &*binding);
                        let deadline = tokio::time::Instant::now() + delay;
                        slots.push(TimerSlot { tag, kind: TimerSlotKind::Emit(handle), deadline });
                    }
                    Some(TimerCmd::ScheduleCallback { tag, delay, callback }) => {
                        cancel_slot(&mut slots, tag, &*binding);
                        let deadline = tokio::time::Instant::now() + delay;
                        slots.push(TimerSlot { tag, kind: TimerSlotKind::Callback(callback), deadline });
                    }
                    Some(TimerCmd::Cancel { tag }) => {
                        cancel_slot(&mut slots, tag, &*binding);
                    }
                    Some(TimerCmd::CancelAll) => {
                        release_all_slots(&mut slots, &*binding);
                    }
                    None => {
                        // Channel closed — operator deactivated / torn down.
                        release_all_slots(&mut slots, &*binding);
                        return;
                    }
                }
            }

            () = sleep_until_or_forever(next_fire) => {
                // At least one timer fired. Drain all expired slots.
                let now = tokio::time::Instant::now();
                let mut i = 0;
                while i < slots.len() {
                    if slots[i].deadline <= now {
                        let slot = slots.swap_remove(i);
                        match slot.kind {
                            TimerSlotKind::Emit(handle) => {
                                // D227/D230: post to the `Send + Sync`
                                // mailbox instead of upgrading a `Weak<C>`
                                // (deleted in S2b — the `Core` relocates
                                // between workers). The owner applies it
                                // via `Core::drain_mailbox` → sync `emit`
                                // (no async in Core). `post_emit` returns
                                // `false` iff the owning `Core` already
                                // dropped — the exact teardown branch the
                                // old `WeakCore::upgrade() == None` took
                                // (release the handle, drain the rest,
                                // exit the task).
                                if !mailbox.post_emit(node_id, handle) {
                                    binding.release_handle(handle);
                                    release_all_slots(&mut slots, &*binding);
                                    return;
                                }
                            }
                            TimerSlotKind::Callback(cb) => {
                                cb();
                            }
                        }
                        // Don't increment i — swap_remove moved the last element here.
                    } else {
                        i += 1;
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

enum TimerSlotKind {
    /// Emit handle on fire; release on cancel.
    Emit(HandleId),
    /// Invoke callback on fire; nothing to release on cancel.
    Callback(TimerCallback),
}

struct TimerSlot {
    tag: u32,
    kind: TimerSlotKind,
    deadline: tokio::time::Instant,
}

fn cancel_slot(slots: &mut Vec<TimerSlot>, tag: u32, binding: &dyn BindingBoundary) {
    if let Some(pos) = slots.iter().position(|s| s.tag == tag) {
        let slot = slots.swap_remove(pos);
        if let TimerSlotKind::Emit(h) = slot.kind {
            binding.release_handle(h);
        }
    }
}

fn release_all_slots(slots: &mut Vec<TimerSlot>, binding: &dyn BindingBoundary) {
    for slot in slots.drain(..) {
        if let TimerSlotKind::Emit(h) = slot.kind {
            binding.release_handle(h);
        }
    }
}

fn nearest_deadline(slots: &[TimerSlot]) -> Option<tokio::time::Instant> {
    slots.iter().map(|s| s.deadline).min()
}

/// Sleep until the given instant, or sleep forever if `None`.
async fn sleep_until_or_forever(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(d) => tokio::time::sleep_until(d).await,
        None => std::future::pending::<()>().await,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::boundary::{DepBatch, FnResult};
    use crate::handle::FnId;

    /// Yield multiple times to let spawned tasks process commands, then
    /// drain the mailbox owner-side (D230). Timer tasks post `Emit`s to
    /// the `CoreMailbox`; `drain_mailbox` applies them via the sync
    /// `emit` → wave → sink. This is the embedder's pump point and is
    /// behaviour-identical to the deleted autonomous
    /// `WeakCore::upgrade → core.emit`: the test already had to advance
    /// the runtime for the timer task to fire at all. Draining an empty
    /// mailbox is an idempotent no-op (safe before any timer fires).
    async fn settle(core: &crate::node::Core) {
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        core.drain_mailbox();
    }

    /// Minimal binding for timer tests — tracks released handles.
    struct TimerTestBinding {
        released: Arc<parking_lot::Mutex<Vec<HandleId>>>,
    }

    impl TimerTestBinding {
        fn new(released: Arc<parking_lot::Mutex<Vec<HandleId>>>) -> Self {
            Self { released }
        }
    }

    impl BindingBoundary for TimerTestBinding {
        fn invoke_fn(&self, _node_id: NodeId, _fn_id: FnId, _dep_data: &[DepBatch]) -> FnResult {
            FnResult::Noop { tracked: None }
        }

        fn custom_equals(&self, _equals_handle: FnId, _a: HandleId, _b: HandleId) -> bool {
            false
        }

        fn release_handle(&self, h: HandleId) {
            self.released.lock().push(h);
        }
    }

    fn make_test_core(
        released: Arc<parking_lot::Mutex<Vec<HandleId>>>,
    ) -> (crate::node::Core, NodeId, Arc<dyn BindingBoundary>) {
        let binding: Arc<dyn BindingBoundary> = Arc::new(TimerTestBinding::new(released));
        let core = crate::node::Core::new(binding.clone());
        let s = core
            .register_state(crate::handle::NO_HANDLE, false)
            .unwrap();
        (core, s, binding)
    }

    #[tokio::test]
    async fn schedule_fires_after_delay() {
        tokio::time::pause();

        let released = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let (core, node, binding) = make_test_core(released.clone());
        let mailbox = core.mailbox();

        let emitted = Arc::new(parking_lot::Mutex::new(Vec::<HandleId>::new()));
        let em = emitted.clone();
        let _sub = core.subscribe(
            node,
            Arc::new(move |msgs| {
                for m in msgs {
                    if let crate::message::Message::Data(h) = m {
                        em.lock().push(*h);
                    }
                }
            }),
        );

        let task = spawn_timer_task(mailbox, node, binding.clone());
        let h1 = HandleId::new(42);
        binding.retain_handle(h1);

        task.sender()
            .send(TimerCmd::Schedule {
                tag: 0,
                delay: Duration::from_millis(50),
                handle: h1,
            })
            .unwrap();

        settle(&core).await; // task processes Schedule, enters sleep_until

        tokio::time::advance(Duration::from_millis(51)).await;
        settle(&core).await; // task fires timer, emit → wave → sink

        let got = emitted.lock().clone();
        assert_eq!(got, vec![h1], "timer should have emitted h1");
    }

    #[tokio::test]
    async fn cancel_releases_handle() {
        tokio::time::pause();

        let released = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let (core, node, binding) = make_test_core(released.clone());
        let mailbox = core.mailbox();

        let task = spawn_timer_task(mailbox, node, binding.clone());
        let h1 = HandleId::new(42);
        binding.retain_handle(h1);

        task.sender()
            .send(TimerCmd::Schedule {
                tag: 0,
                delay: Duration::from_millis(100),
                handle: h1,
            })
            .unwrap();

        task.sender().send(TimerCmd::Cancel { tag: 0 }).unwrap();
        settle(&core).await;

        assert!(
            released.lock().contains(&h1),
            "cancelled handle should be released"
        );
    }

    #[tokio::test]
    async fn reschedule_same_tag_cancels_previous() {
        tokio::time::pause();

        let released = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let (core, node, binding) = make_test_core(released.clone());
        let mailbox = core.mailbox();

        let emitted = Arc::new(parking_lot::Mutex::new(Vec::<HandleId>::new()));
        let em = emitted.clone();
        let _sub = core.subscribe(
            node,
            Arc::new(move |msgs| {
                for m in msgs {
                    if let crate::message::Message::Data(h) = m {
                        em.lock().push(*h);
                    }
                }
            }),
        );

        let task = spawn_timer_task(mailbox, node, binding.clone());
        let h1 = HandleId::new(10);
        let h2 = HandleId::new(20);
        binding.retain_handle(h1);
        binding.retain_handle(h2);

        // Schedule h1 at tag 0.
        task.sender()
            .send(TimerCmd::Schedule {
                tag: 0,
                delay: Duration::from_millis(100),
                handle: h1,
            })
            .unwrap();

        // Reschedule tag 0 with h2 — cancels h1.
        task.sender()
            .send(TimerCmd::Schedule {
                tag: 0,
                delay: Duration::from_millis(50),
                handle: h2,
            })
            .unwrap();

        settle(&core).await;

        // h1 released (cancelled).
        assert!(released.lock().contains(&h1));

        // Advance past h2's deadline.
        tokio::time::advance(Duration::from_millis(51)).await;
        settle(&core).await;

        let got = emitted.lock().clone();
        assert_eq!(got, vec![h2], "only h2 should fire");
    }

    #[tokio::test]
    async fn drop_handle_releases_pending() {
        tokio::time::pause();

        let released = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let (core, node, binding) = make_test_core(released.clone());
        let mailbox = core.mailbox();

        let mut task = spawn_timer_task(mailbox, node, binding.clone());
        let h1 = HandleId::new(42);
        binding.retain_handle(h1);

        task.sender()
            .send(TimerCmd::Schedule {
                tag: 0,
                delay: Duration::from_secs(1),
                handle: h1,
            })
            .unwrap();

        settle(&core).await;

        // Shutdown (simulates operator deactivation).
        task.shutdown();
        settle(&core).await;

        assert!(
            released.lock().contains(&h1),
            "pending handle should be released on shutdown"
        );
    }

    #[tokio::test]
    async fn multiple_tags_fire_independently() {
        tokio::time::pause();

        let released = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let (core, node, binding) = make_test_core(released.clone());
        let mailbox = core.mailbox();

        let emitted = Arc::new(parking_lot::Mutex::new(Vec::<HandleId>::new()));
        let em = emitted.clone();
        let _sub = core.subscribe(
            node,
            Arc::new(move |msgs| {
                for m in msgs {
                    if let crate::message::Message::Data(h) = m {
                        em.lock().push(*h);
                    }
                }
            }),
        );

        let task = spawn_timer_task(mailbox, node, binding.clone());
        let h1 = HandleId::new(10);
        let h2 = HandleId::new(20);
        binding.retain_handle(h1);
        binding.retain_handle(h2);

        // Tag 0 at 100ms, tag 1 at 50ms.
        task.sender()
            .send(TimerCmd::Schedule {
                tag: 0,
                delay: Duration::from_millis(100),
                handle: h1,
            })
            .unwrap();
        task.sender()
            .send(TimerCmd::Schedule {
                tag: 1,
                delay: Duration::from_millis(50),
                handle: h2,
            })
            .unwrap();

        // Yield so the task processes both Schedule commands.
        settle(&core).await; // task processes both Schedule commands

        // Advance to 51ms — h2 fires.
        tokio::time::advance(Duration::from_millis(51)).await;
        settle(&core).await;
        assert_eq!(*emitted.lock(), vec![h2]);

        // Advance to 101ms — h1 fires.
        tokio::time::advance(Duration::from_millis(50)).await;
        settle(&core).await;
        assert_eq!(*emitted.lock(), vec![h2, h1]);
    }
}
