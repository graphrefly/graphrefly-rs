//! Temporal operators — time-dependent transforms on reactive streams.
//!
//! # Operators
//!
//! - [`sample`] — pure reactive; emits source's latest value when notifier
//!   fires. No timer needed.
//! - [`debounce`] — emits after `ms` of quiet (no new upstream DATA).
//! - [`throttle`] — rate-limits: at most one emission per `ms` window.
//!   Configurable leading / trailing edge.
//! - [`delay`] — delays each upstream DATA by `ms`. Multiple in-flight.
//! - [`audit`] — on first DATA, starts a `ms` timer; when it fires, emits
//!   the latest value. Timer does NOT restart on subsequent DATA within
//!   the window.
//! - [`interval`] — source that emits a monotonic counter every `period_ms`.
//! - [`timeout`] — errors if no DATA arrives within `ms` after subscribe or
//!   after the previous DATA.
//! - [`buffer_time`] — collects upstream DATA into a buffer and flushes as
//!   a packed tuple every `ms` milliseconds.
//! - [`window_time`] — rotates inner sub-nodes every `ms` milliseconds;
//!   upstream DATA is forwarded to the current window node.
//!
//! # Architecture
//!
//! Timer operators (debounce, throttle, delay, audit) spawn a **per-operator
//! tokio task** that owns all pending state (handles, counters) exclusively.
//! The sync sink callback sends [`TemporalCmd`] commands; the async task
//! manages timers via `tokio::time` and calls `Core::emit_or_defer` /
//! `complete_or_defer` / `error_or_defer` when ready. This avoids the
//! double-ownership problem that would arise from tracking pending handles
//! in both the operator's state mutex and the generic timer substrate.
//!
//! `sample` is a pure-reactive producer-pattern node with two subscriptions
//! (source + notifier) and no timer dependency.
//!
//! All temporal operators require a **tokio runtime context** at activation
//! time (first subscriber triggers the build closure, which calls
//! `tokio::spawn`).

use std::collections::VecDeque;
use std::sync::{Arc, Weak};
use std::time::Duration;

use parking_lot::Mutex;

use graphrefly_core::{BindingBoundary, Core, FnId, HandleId, NodeId, Sink, WeakCore};
use smallvec::SmallVec;

use crate::producer::{ProducerBinding, ProducerBuildFn, ProducerCtx, SubscribeOutcome};

// =========================================================================
// Shared command type
// =========================================================================

/// Command sent from operator sink callbacks (sync) to the per-operator
/// async task. Channel close = shutdown (producer deactivated).
enum TemporalCmd {
    /// New DATA handle from upstream. Already retained by the sink.
    Value(HandleId),
    /// Upstream COMPLETE. Flush pending if applicable, then complete.
    Complete,
    /// Upstream ERROR. Cancel all, propagate. Handle already retained.
    Error(HandleId),
}

/// RAII wrapper for temporal operator tasks. On drop, closes the command
/// channel first (triggering the task's cleanup path which releases pending
/// handles), then aborts the task as a fallback if it hasn't exited.
struct TemporalTaskGuard {
    /// Dropping the sender closes the channel → task sees `None` on
    /// `rx.recv()` and releases all pending handles before returning.
    _tx: tokio::sync::mpsc::UnboundedSender<TemporalCmd>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for TemporalTaskGuard {
    fn drop(&mut self) {
        // _tx drops first (field order), closing the channel.
        // Abort as fallback in case the task is stuck on a non-channel await.
        self.task.abort();
    }
}

/// Simple abort-on-drop for tasks with no pending handle state (e.g. interval).
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

// =========================================================================
// sample(source, notifier) — pure reactive, no timer
// =========================================================================

/// Emits the source's latest DATA each time `notifier` delivers DATA.
///
/// - Source COMPLETE clears stored value; subsequent notifier DATAs no-op.
/// - Notifier COMPLETE terminates the sample node.
/// - Either dep ERROR terminates immediately.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn sample(
    core: &Core,
    binding: &Arc<dyn ProducerBinding>,
    source: NodeId,
    notifier: NodeId,
) -> NodeId {
    let core_weak = core.weak_handle();
    let binding_weak: Weak<dyn ProducerBinding> = Arc::downgrade(binding);

    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let (Some(core_s), Some(binding_s)) = (core_weak.upgrade(), binding_weak.upgrade()) else {
            return;
        };
        let pid = ctx.node_id();
        let state: Arc<Mutex<SampleState>> = Arc::new(Mutex::new(SampleState::default()));

        // --- source sink ---
        let st = state.clone();
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();
        let core_src = core_s.clone();
        let source_sink: Sink = Arc::new(move |msgs| {
            enum Act {
                Release(HandleId),
                Error(HandleId),
            }
            let mut actions: SmallVec<[Act; 2]> = SmallVec::new();
            {
                let mut s = st.lock();
                if s.terminated {
                    return;
                }
                for m in msgs {
                    match m.tier() {
                        3 => {
                            if let Some(h) = m.payload_handle() {
                                if let Some(old) = s.latest.replace(h) {
                                    actions.push(Act::Release(old));
                                }
                                bb.retain_handle(h);
                            }
                        }
                        5 => {
                            if let Some(h) = m.payload_handle() {
                                s.terminated = true;
                                if let Some(old) = s.latest.take() {
                                    actions.push(Act::Release(old));
                                }
                                bb.retain_handle(h);
                                actions.push(Act::Error(h));
                            } else {
                                s.source_completed = true;
                                if let Some(old) = s.latest.take() {
                                    actions.push(Act::Release(old));
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            for a in actions {
                match a {
                    Act::Release(h) => bb.release_handle(h),
                    Act::Error(h) => core_src.error_or_defer(pid, h),
                }
            }
        });

        let src_outcome = ctx.subscribe_to(source, source_sink);
        if matches!(src_outcome, SubscribeOutcome::Dead { .. }) {
            state.lock().source_completed = true;
        }

        // --- notifier sink ---
        let st2 = state.clone();
        let core_n = core_s.clone();
        let bb2: Arc<dyn BindingBoundary> = binding_s.clone();
        let notifier_sink: Sink = Arc::new(move |msgs| {
            let mut s = st2.lock();
            if s.terminated {
                return;
            }
            for m in msgs {
                if s.terminated {
                    return;
                }
                match m.tier() {
                    3 if m.payload_handle().is_some()
                        && !s.source_completed
                        && s.latest.is_some() =>
                    {
                        let h = s.latest.unwrap();
                        bb2.retain_handle(h);
                        drop(s);
                        core_n.emit_or_defer(pid, h);
                        // Re-acquire lock and continue processing remaining
                        // batch messages (e.g. a trailing Complete).
                        s = st2.lock();
                    }
                    5 => {
                        if let Some(h) = m.payload_handle() {
                            s.terminated = true;
                            if let Some(old) = s.latest.take() {
                                bb2.release_handle(old);
                            }
                            bb2.retain_handle(h);
                            drop(s);
                            core_n.error_or_defer(pid, h);
                            return;
                        }
                        // Notifier COMPLETE → self-complete.
                        s.terminated = true;
                        if let Some(old) = s.latest.take() {
                            bb2.release_handle(old);
                        }
                        drop(s);
                        core_n.complete_or_defer(pid);
                        return;
                    }
                    _ => {}
                }
            }
        });

        let not_outcome = ctx.subscribe_to(notifier, notifier_sink);
        if matches!(not_outcome, SubscribeOutcome::Dead { .. }) {
            let mut s = state.lock();
            if !s.terminated {
                s.terminated = true;
                if let Some(old) = s.latest.take() {
                    binding_s.release_handle(old);
                }
                drop(s);
                core_s.complete_or_defer(pid);
            }
        }
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
        .expect("sample: register_producer failed")
}

#[derive(Default)]
struct SampleState {
    latest: Option<HandleId>,
    source_completed: bool,
    terminated: bool,
}

// =========================================================================
// debounce(source, ms)
// =========================================================================

/// Emits after `delay` of quiet — each new upstream DATA resets the timer.
///
/// On upstream COMPLETE, flushes pending (if any) then completes.
/// On upstream ERROR, cancels and propagates immediately.
#[must_use]
pub fn debounce(
    core: &Core,
    binding: &Arc<dyn ProducerBinding>,
    source: NodeId,
    ms: u64,
) -> NodeId {
    let core_weak = core.weak_handle();
    let binding_weak: Weak<dyn ProducerBinding> = Arc::downgrade(binding);
    let delay = Duration::from_millis(ms);

    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let (Some(core_s), Some(binding_s)) = (core_weak.upgrade(), binding_weak.upgrade()) else {
            return;
        };
        let pid = ctx.node_id();
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let tx_sink = tx.clone();
        let tx_dead = tx.clone();
        let task = tokio::spawn(debounce_task(
            rx,
            core_s.weak_handle(),
            pid,
            bb.clone(),
            delay,
        ));

        // Store guard: drops tx (clean shutdown) then aborts task (fallback).
        {
            let mut storage = binding_s.producer_storage().lock();
            let entry = storage.entry(pid).or_default();
            entry.op_state = Some(Box::new(TemporalTaskGuard { _tx: tx, task }));
        }

        let bb_sink: Arc<dyn BindingBoundary> = binding_s.clone();
        let source_sink: Sink = Arc::new(move |msgs| {
            for m in msgs {
                match m.tier() {
                    3 => {
                        if let Some(h) = m.payload_handle() {
                            bb_sink.retain_handle(h);
                            if tx_sink.send(TemporalCmd::Value(h)).is_err() {
                                bb_sink.release_handle(h);
                            }
                        }
                    }
                    5 => {
                        if let Some(h) = m.payload_handle() {
                            bb_sink.retain_handle(h);
                            if tx_sink.send(TemporalCmd::Error(h)).is_err() {
                                bb_sink.release_handle(h);
                            }
                        } else {
                            let _ = tx_sink.send(TemporalCmd::Complete);
                        }
                    }
                    _ => {}
                }
            }
        });

        let outcome = ctx.subscribe_to(source, source_sink);
        if matches!(outcome, SubscribeOutcome::Dead { .. }) {
            let _ = tx_dead.send(TemporalCmd::Complete);
        }
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
        .expect("debounce: register_producer failed")
}

async fn debounce_task(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<TemporalCmd>,
    core: WeakCore,
    pid: NodeId,
    binding: Arc<dyn BindingBoundary>,
    delay: Duration,
) {
    let mut pending: Option<HandleId> = None;

    loop {
        if let Some(h) = pending {
            tokio::select! {
                biased;
                cmd = rx.recv() => {
                    match cmd {
                        Some(TemporalCmd::Value(new_h)) => {
                            binding.release_handle(h);
                            pending = Some(new_h);
                        }
                        Some(TemporalCmd::Complete) => {
                            // Flush pending then complete.
                            if let Some(c) = core.upgrade() {
                                c.emit_or_defer(pid, h);
                                c.complete_or_defer(pid);
                            } else {
                                binding.release_handle(h);
                            }
                            return;
                        }
                        Some(TemporalCmd::Error(err_h)) => {
                            binding.release_handle(h);
                            if let Some(c) = core.upgrade() {
                                c.error_or_defer(pid, err_h);
                            } else {
                                binding.release_handle(err_h);
                            }
                            return;
                        }
                        None => {
                            // Channel closed — deactivated.
                            binding.release_handle(h);
                            return;
                        }
                    }
                }
                () = tokio::time::sleep(delay) => {
                    // Timer fired — emit pending.
                    if let Some(c) = core.upgrade() {
                        c.emit_or_defer(pid, h);
                    } else {
                        binding.release_handle(h);
                        return;
                    }
                    pending = None;
                }
            }
        } else {
            // No pending — wait for command.
            match rx.recv().await {
                Some(TemporalCmd::Value(h)) => {
                    pending = Some(h);
                }
                Some(TemporalCmd::Complete) => {
                    if let Some(c) = core.upgrade() {
                        c.complete_or_defer(pid);
                    }
                    return;
                }
                Some(TemporalCmd::Error(err_h)) => {
                    if let Some(c) = core.upgrade() {
                        c.error_or_defer(pid, err_h);
                    } else {
                        binding.release_handle(err_h);
                    }
                    return;
                }
                None => return,
            }
        }
    }
}

// =========================================================================
// audit(source, ms)
// =========================================================================

/// On the first upstream DATA in each window, starts a `ms` timer. When
/// the timer fires, emits the **latest** value received during the window.
/// Subsequent DATA values within the window update the stored value but
/// do NOT restart the timer.
///
/// Differs from [`debounce`]: debounce resets on each DATA (quiet-time);
/// audit fires at a fixed interval after the first DATA in the window.
#[must_use]
pub fn audit(core: &Core, binding: &Arc<dyn ProducerBinding>, source: NodeId, ms: u64) -> NodeId {
    let core_weak = core.weak_handle();
    let binding_weak: Weak<dyn ProducerBinding> = Arc::downgrade(binding);
    let delay = Duration::from_millis(ms);

    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let (Some(core_s), Some(binding_s)) = (core_weak.upgrade(), binding_weak.upgrade()) else {
            return;
        };
        let pid = ctx.node_id();
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let tx_sink = tx.clone();
        let tx_dead = tx.clone();
        let task = tokio::spawn(audit_task(rx, core_s.weak_handle(), pid, bb.clone(), delay));

        {
            let mut storage = binding_s.producer_storage().lock();
            let entry = storage.entry(pid).or_default();
            entry.op_state = Some(Box::new(TemporalTaskGuard { _tx: tx, task }));
        }

        let bb_sink: Arc<dyn BindingBoundary> = binding_s.clone();
        let source_sink: Sink = Arc::new(move |msgs| {
            for m in msgs {
                match m.tier() {
                    3 => {
                        if let Some(h) = m.payload_handle() {
                            bb_sink.retain_handle(h);
                            if tx_sink.send(TemporalCmd::Value(h)).is_err() {
                                bb_sink.release_handle(h);
                            }
                        }
                    }
                    5 => {
                        if let Some(h) = m.payload_handle() {
                            bb_sink.retain_handle(h);
                            if tx_sink.send(TemporalCmd::Error(h)).is_err() {
                                bb_sink.release_handle(h);
                            }
                        } else {
                            let _ = tx_sink.send(TemporalCmd::Complete);
                        }
                    }
                    _ => {}
                }
            }
        });

        let outcome = ctx.subscribe_to(source, source_sink);
        if matches!(outcome, SubscribeOutcome::Dead { .. }) {
            let _ = tx_dead.send(TemporalCmd::Complete);
        }
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
        .expect("audit: register_producer failed")
}

async fn audit_task(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<TemporalCmd>,
    core: WeakCore,
    pid: NodeId,
    binding: Arc<dyn BindingBoundary>,
    delay: Duration,
) {
    // Outer loop: wait for first DATA to start a window.
    loop {
        match rx.recv().await {
            Some(TemporalCmd::Value(h)) => {
                // First value in window — start timer, collect updates.
                let mut latest = h;

                tokio::select! {
                    biased;
                    // Drain commands until timer fires.
                    () = async {
                        loop {
                            tokio::select! {
                                biased;
                                cmd = rx.recv() => {
                                    match cmd {
                                        Some(TemporalCmd::Value(new_h)) => {
                                            binding.release_handle(latest);
                                            latest = new_h;
                                        }
                                        Some(TemporalCmd::Complete) => {
                                            // Emit latest, then complete.
                                            if let Some(c) = core.upgrade() {
                                                c.emit_or_defer(pid, latest);
                                                c.complete_or_defer(pid);
                                            } else {
                                                binding.release_handle(latest);
                                            }
                                            return; // exits the async block
                                        }
                                        Some(TemporalCmd::Error(err_h)) => {
                                            binding.release_handle(latest);
                                            if let Some(c) = core.upgrade() {
                                                c.error_or_defer(pid, err_h);
                                            } else {
                                                binding.release_handle(err_h);
                                            }
                                            return;
                                        }
                                        None => {
                                            binding.release_handle(latest);
                                            return;
                                        }
                                    }
                                }
                            }
                        }
                    } => {
                        return; // Terminal command handled inside async block.
                    }
                    () = tokio::time::sleep(delay) => {
                        // Timer fired — emit latest, window closes.
                        if let Some(c) = core.upgrade() {
                            c.emit_or_defer(pid, latest);
                        } else {
                            binding.release_handle(latest);
                            return;
                        }
                        // Continue outer loop — wait for next window.
                    }
                }
            }
            Some(TemporalCmd::Complete) => {
                if let Some(c) = core.upgrade() {
                    c.complete_or_defer(pid);
                }
                return;
            }
            Some(TemporalCmd::Error(err_h)) => {
                if let Some(c) = core.upgrade() {
                    c.error_or_defer(pid, err_h);
                } else {
                    binding.release_handle(err_h);
                }
                return;
            }
            None => return,
        }
    }
}

// =========================================================================
// delay(source, ms)
// =========================================================================

/// Delays each upstream DATA by `ms` milliseconds. Multiple values can
/// be in-flight simultaneously, each with its own timer.
///
/// - COMPLETE waits for all pending delays, then completes.
/// - ERROR cancels all pending and propagates immediately.
#[must_use]
pub fn delay(core: &Core, binding: &Arc<dyn ProducerBinding>, source: NodeId, ms: u64) -> NodeId {
    let core_weak = core.weak_handle();
    let binding_weak: Weak<dyn ProducerBinding> = Arc::downgrade(binding);
    let delay_dur = Duration::from_millis(ms);

    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let (Some(core_s), Some(binding_s)) = (core_weak.upgrade(), binding_weak.upgrade()) else {
            return;
        };
        let pid = ctx.node_id();
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let tx_sink = tx.clone();
        let tx_dead = tx.clone();
        let task = tokio::spawn(delay_task(
            rx,
            core_s.weak_handle(),
            pid,
            bb.clone(),
            delay_dur,
        ));

        {
            let mut storage = binding_s.producer_storage().lock();
            let entry = storage.entry(pid).or_default();
            entry.op_state = Some(Box::new(TemporalTaskGuard { _tx: tx, task }));
        }

        let bb_sink: Arc<dyn BindingBoundary> = binding_s.clone();
        let source_sink: Sink = Arc::new(move |msgs| {
            for m in msgs {
                match m.tier() {
                    3 => {
                        if let Some(h) = m.payload_handle() {
                            bb_sink.retain_handle(h);
                            if tx_sink.send(TemporalCmd::Value(h)).is_err() {
                                bb_sink.release_handle(h);
                            }
                        }
                    }
                    5 => {
                        if let Some(h) = m.payload_handle() {
                            bb_sink.retain_handle(h);
                            if tx_sink.send(TemporalCmd::Error(h)).is_err() {
                                bb_sink.release_handle(h);
                            }
                        } else {
                            let _ = tx_sink.send(TemporalCmd::Complete);
                        }
                    }
                    _ => {}
                }
            }
        });

        let outcome = ctx.subscribe_to(source, source_sink);
        if matches!(outcome, SubscribeOutcome::Dead { .. }) {
            let _ = tx_dead.send(TemporalCmd::Complete);
        }
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
        .expect("delay: register_producer failed")
}

async fn delay_task(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<TemporalCmd>,
    core: WeakCore,
    pid: NodeId,
    binding: Arc<dyn BindingBoundary>,
    delay: Duration,
) {
    let mut queue: VecDeque<(tokio::time::Instant, HandleId)> = VecDeque::new();
    let mut complete_pending = false;

    loop {
        let next_fire = queue.front().map(|(deadline, _)| *deadline);

        tokio::select! {
            biased;
            cmd = rx.recv() => {
                match cmd {
                    Some(TemporalCmd::Value(h)) => {
                        queue.push_back((tokio::time::Instant::now() + delay, h));
                    }
                    Some(TemporalCmd::Complete) => {
                        complete_pending = true;
                        if queue.is_empty() {
                            if let Some(c) = core.upgrade() {
                                c.complete_or_defer(pid);
                            }
                            return;
                        }
                        // Wait for pending timers to drain.
                    }
                    Some(TemporalCmd::Error(err_h)) => {
                        // Release all pending.
                        for (_, h) in queue.drain(..) {
                            binding.release_handle(h);
                        }
                        if let Some(c) = core.upgrade() {
                            c.error_or_defer(pid, err_h);
                        } else {
                            binding.release_handle(err_h);
                        }
                        return;
                    }
                    None => {
                        for (_, h) in queue.drain(..) {
                            binding.release_handle(h);
                        }
                        return;
                    }
                }
            }
            () = sleep_until_or_forever(next_fire) => {
                // Fire all expired.
                let now = tokio::time::Instant::now();
                while let Some(&(deadline, _)) = queue.front() {
                    if deadline <= now {
                        let (_, h) = queue.pop_front().unwrap();
                        if let Some(c) = core.upgrade() {
                            c.emit_or_defer(pid, h);
                        } else {
                            binding.release_handle(h);
                            for (_, h2) in queue.drain(..) {
                                binding.release_handle(h2);
                            }
                            return;
                        }
                    } else {
                        break;
                    }
                }
                if complete_pending && queue.is_empty() {
                    if let Some(c) = core.upgrade() {
                        c.complete_or_defer(pid);
                    }
                    return;
                }
            }
        }
    }
}

// =========================================================================
// throttle(source, ms, opts)
// =========================================================================

/// Options for [`throttle`].
#[derive(Debug, Clone, Copy)]
pub struct ThrottleOpts {
    /// Emit immediately at the leading edge of each window (default: true).
    pub leading: bool,
    /// Emit the latest value at the trailing edge of each window (default: false).
    pub trailing: bool,
}

impl Default for ThrottleOpts {
    fn default() -> Self {
        Self {
            leading: true,
            trailing: false,
        }
    }
}

/// Rate-limits upstream emissions to at most one per `ms`-millisecond window.
///
/// With default options (`leading: true, trailing: false`), the first DATA
/// in each window is emitted immediately; subsequent DATA within the window
/// is dropped. With `trailing: true`, the latest DATA is emitted at window end.
///
/// On upstream COMPLETE, flushes trailing (if any) then completes.
#[must_use]
pub fn throttle(
    core: &Core,
    binding: &Arc<dyn ProducerBinding>,
    source: NodeId,
    ms: u64,
    opts: ThrottleOpts,
) -> NodeId {
    let core_weak = core.weak_handle();
    let binding_weak: Weak<dyn ProducerBinding> = Arc::downgrade(binding);
    let window = Duration::from_millis(ms);

    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let (Some(core_s), Some(binding_s)) = (core_weak.upgrade(), binding_weak.upgrade()) else {
            return;
        };
        let pid = ctx.node_id();
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let tx_sink = tx.clone();
        let tx_dead = tx.clone();
        let task = tokio::spawn(throttle_task(
            rx,
            core_s.weak_handle(),
            pid,
            bb.clone(),
            window,
            opts,
        ));

        {
            let mut storage = binding_s.producer_storage().lock();
            let entry = storage.entry(pid).or_default();
            entry.op_state = Some(Box::new(TemporalTaskGuard { _tx: tx, task }));
        }

        let bb_sink: Arc<dyn BindingBoundary> = binding_s.clone();
        let source_sink: Sink = Arc::new(move |msgs| {
            for m in msgs {
                match m.tier() {
                    3 => {
                        if let Some(h) = m.payload_handle() {
                            bb_sink.retain_handle(h);
                            if tx_sink.send(TemporalCmd::Value(h)).is_err() {
                                bb_sink.release_handle(h);
                            }
                        }
                    }
                    5 => {
                        if let Some(h) = m.payload_handle() {
                            bb_sink.retain_handle(h);
                            if tx_sink.send(TemporalCmd::Error(h)).is_err() {
                                bb_sink.release_handle(h);
                            }
                        } else {
                            let _ = tx_sink.send(TemporalCmd::Complete);
                        }
                    }
                    _ => {}
                }
            }
        });

        let outcome = ctx.subscribe_to(source, source_sink);
        if matches!(outcome, SubscribeOutcome::Dead { .. }) {
            let _ = tx_dead.send(TemporalCmd::Complete);
        }
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
        .expect("throttle: register_producer failed")
}

async fn throttle_task(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<TemporalCmd>,
    core: WeakCore,
    pid: NodeId,
    binding: Arc<dyn BindingBoundary>,
    window: Duration,
    opts: ThrottleOpts,
) {
    let mut trailing_pending: Option<HandleId> = None;
    let mut window_deadline: Option<tokio::time::Instant> = None;

    loop {
        let fire_at = if opts.trailing { window_deadline } else { None };

        tokio::select! {
            biased;
            cmd = rx.recv() => {
                match cmd {
                    Some(TemporalCmd::Value(h)) => {
                        let in_window = window_deadline
                            .is_some_and(|d| tokio::time::Instant::now() < d);

                        if !in_window {
                            // Window open — start new window.
                            window_deadline = Some(tokio::time::Instant::now() + window);
                            if opts.leading {
                                if let Some(c) = core.upgrade() {
                                    c.emit_or_defer(pid, h);
                                } else {
                                    binding.release_handle(h);
                                    release_opt(&mut trailing_pending, &*binding);
                                    return;
                                }
                            } else if opts.trailing {
                                if let Some(old) = trailing_pending.replace(h) {
                                    binding.release_handle(old);
                                }
                            } else {
                                binding.release_handle(h);
                            }
                        } else if opts.trailing {
                            // Inside window — store for trailing.
                            if let Some(old) = trailing_pending.replace(h) {
                                binding.release_handle(old);
                            }
                        } else {
                            binding.release_handle(h);
                        }
                    }
                    Some(TemporalCmd::Complete) => {
                        if let Some(h) = trailing_pending.take() {
                            if let Some(c) = core.upgrade() {
                                c.emit_or_defer(pid, h);
                                c.complete_or_defer(pid);
                            } else {
                                binding.release_handle(h);
                            }
                        } else if let Some(c) = core.upgrade() {
                            c.complete_or_defer(pid);
                        }
                        return;
                    }
                    Some(TemporalCmd::Error(err_h)) => {
                        release_opt(&mut trailing_pending, &*binding);
                        if let Some(c) = core.upgrade() {
                            c.error_or_defer(pid, err_h);
                        } else {
                            binding.release_handle(err_h);
                        }
                        return;
                    }
                    None => {
                        release_opt(&mut trailing_pending, &*binding);
                        return;
                    }
                }
            }
            () = sleep_until_or_forever(fire_at) => {
                // Window expired — emit trailing if any, reopen window.
                window_deadline = None;
                if let Some(h) = trailing_pending.take() {
                    if let Some(c) = core.upgrade() {
                        c.emit_or_defer(pid, h);
                        // Start new window for the trailing emission.
                        window_deadline = Some(tokio::time::Instant::now() + window);
                    } else {
                        binding.release_handle(h);
                        return;
                    }
                }
            }
        }
    }
}

// =========================================================================
// interval(period_ms) — timer source
// =========================================================================

/// Source that emits a monotonically increasing counter (`1, 2, 3, …`)
/// every `period_ms` milliseconds. Counter starts at 1 (not 0) because
/// `HandleId(0)` is the `NO_HANDLE` sentinel. Resubscribable: counter
/// resets on deactivation + reactivation.
///
/// The emitted values use raw `HandleId::new(counter)`. Bindings should
/// register these as integer handles.
#[must_use]
pub fn interval(core: &Core, binding: &Arc<dyn ProducerBinding>, period_ms: u64) -> NodeId {
    let core_weak = core.weak_handle();
    let binding_weak: Weak<dyn ProducerBinding> = Arc::downgrade(binding);
    let period = Duration::from_millis(period_ms);

    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let (Some(core_s), Some(binding_s)) = (core_weak.upgrade(), binding_weak.upgrade()) else {
            return;
        };
        let pid = ctx.node_id();
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();
        let weak = core_s.weak_handle();

        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(period);
            ticker.tick().await; // First tick is immediate — skip it.
            let mut counter: u64 = 1; // Start at 1: HandleId(0) = NO_HANDLE.
            loop {
                ticker.tick().await;
                if let Some(c) = weak.upgrade() {
                    let h = HandleId::new(counter);
                    bb.retain_handle(h);
                    c.emit_or_defer(pid, h);
                    counter += 1;
                } else {
                    break;
                }
            }
        });

        {
            let mut storage = binding_s.producer_storage().lock();
            let entry = storage.entry(pid).or_default();
            entry.op_state = Some(Box::new(AbortOnDrop(task)));
        }
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
        .expect("interval: register_producer failed")
}

// =========================================================================
// Helpers
// =========================================================================

/// Sleep until the given instant, or sleep forever if `None`.
async fn sleep_until_or_forever(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(d) => tokio::time::sleep_until(d).await,
        None => std::future::pending::<()>().await,
    }
}

/// Release an optional handle and set it to `None`.
fn release_opt(opt: &mut Option<HandleId>, binding: &dyn BindingBoundary) {
    if let Some(h) = opt.take() {
        binding.release_handle(h);
    }
}

// =========================================================================
// timeout(source, ms, error_handle)
// =========================================================================

/// Errors if no upstream DATA arrives within `ms` milliseconds after
/// subscribe or after the previous DATA. Each DATA resets the timer.
///
/// On timeout: retains `error_handle` and emits ERROR with it.
/// On upstream COMPLETE: cancels timer, forwards complete.
/// On upstream ERROR: cancels timer, forwards error.
#[must_use]
pub fn timeout(
    core: &Core,
    binding: &Arc<dyn ProducerBinding>,
    source: NodeId,
    ms: u64,
    error_handle: HandleId,
) -> NodeId {
    let core_weak = core.weak_handle();
    let binding_weak: Weak<dyn ProducerBinding> = Arc::downgrade(binding);
    let duration = Duration::from_millis(ms);

    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let (Some(core_s), Some(binding_s)) = (core_weak.upgrade(), binding_weak.upgrade()) else {
            return;
        };
        let pid = ctx.node_id();
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let tx_sink = tx.clone();
        let tx_dead = tx.clone();
        let task = tokio::spawn(timeout_task(
            rx,
            core_s.weak_handle(),
            pid,
            bb.clone(),
            duration,
            error_handle,
        ));

        {
            let mut storage = binding_s.producer_storage().lock();
            let entry = storage.entry(pid).or_default();
            entry.op_state = Some(Box::new(TemporalTaskGuard { _tx: tx, task }));
        }

        let bb_sink: Arc<dyn BindingBoundary> = binding_s.clone();
        let source_sink: Sink = Arc::new(move |msgs| {
            for m in msgs {
                match m.tier() {
                    3 => {
                        if let Some(h) = m.payload_handle() {
                            bb_sink.retain_handle(h);
                            if tx_sink.send(TemporalCmd::Value(h)).is_err() {
                                bb_sink.release_handle(h);
                            }
                        }
                    }
                    5 => {
                        if let Some(h) = m.payload_handle() {
                            bb_sink.retain_handle(h);
                            if tx_sink.send(TemporalCmd::Error(h)).is_err() {
                                bb_sink.release_handle(h);
                            }
                        } else {
                            let _ = tx_sink.send(TemporalCmd::Complete);
                        }
                    }
                    _ => {}
                }
            }
        });

        let outcome = ctx.subscribe_to(source, source_sink);
        if matches!(outcome, SubscribeOutcome::Dead { .. }) {
            let _ = tx_dead.send(TemporalCmd::Complete);
        }
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
        .expect("timeout: register_producer failed")
}

async fn timeout_task(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<TemporalCmd>,
    core: WeakCore,
    pid: NodeId,
    binding: Arc<dyn BindingBoundary>,
    duration: Duration,
    error_handle: HandleId,
) {
    // The timer starts immediately on subscribe — if no DATA arrives
    // within `duration`, we fire the timeout error.
    loop {
        tokio::select! {
            biased;
            cmd = rx.recv() => {
                match cmd {
                    Some(TemporalCmd::Value(h)) => {
                        // DATA arrived — forward it and reset the timer
                        // (the next loop iteration restarts the sleep).
                        if let Some(c) = core.upgrade() {
                            c.emit_or_defer(pid, h);
                        } else {
                            binding.release_handle(h);
                            return;
                        }
                        // Continue loop → resets the sleep timer.
                    }
                    Some(TemporalCmd::Complete) => {
                        if let Some(c) = core.upgrade() {
                            c.complete_or_defer(pid);
                        }
                        return;
                    }
                    Some(TemporalCmd::Error(err_h)) => {
                        if let Some(c) = core.upgrade() {
                            c.error_or_defer(pid, err_h);
                        } else {
                            binding.release_handle(err_h);
                        }
                        return;
                    }
                    None => return,
                }
            }
            () = tokio::time::sleep(duration) => {
                // Timeout fired — emit error.
                if let Some(c) = core.upgrade() {
                    binding.retain_handle(error_handle);
                    c.error_or_defer(pid, error_handle);
                }
                return;
            }
        }
    }
}

// =========================================================================
// buffer_time(source, ms, pack_fn_id)
// =========================================================================

/// Command sent from the `buffer_time` sink to its async task.
enum BufferTimeCmd {
    /// New DATA handle from upstream. Already retained by the sink.
    Value(HandleId),
    /// Upstream COMPLETE.
    Complete,
    /// Upstream ERROR. Handle already retained.
    Error(HandleId),
}

/// Collects upstream DATA handles into a buffer and flushes them as a
/// packed tuple every `ms` milliseconds.
///
/// - On interval tick: packs buffered handles via
///   `binding.pack_tuple(pack_fn_id, &buf)`, emits the result, releases
///   individual handles, clears buffer.
/// - On upstream COMPLETE: flushes remaining buffer, then completes.
/// - On upstream ERROR: releases buffered handles, forwards error.
#[must_use]
pub fn buffer_time(
    core: &Core,
    binding: &Arc<dyn ProducerBinding>,
    source: NodeId,
    ms: u64,
    pack_fn_id: FnId,
) -> NodeId {
    let core_weak = core.weak_handle();
    let binding_weak: Weak<dyn ProducerBinding> = Arc::downgrade(binding);
    let period = Duration::from_millis(ms);

    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let (Some(core_s), Some(binding_s)) = (core_weak.upgrade(), binding_weak.upgrade()) else {
            return;
        };
        let pid = ctx.node_id();
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let tx_sink = tx.clone();
        let tx_dead = tx.clone();
        let task = tokio::spawn(buffer_time_task(
            rx,
            core_s.weak_handle(),
            pid,
            bb.clone(),
            period,
            pack_fn_id,
        ));

        {
            let mut storage = binding_s.producer_storage().lock();
            let entry = storage.entry(pid).or_default();
            entry.op_state = Some(Box::new(BufferTimeTaskGuard { _tx: tx, task }));
        }

        let bb_sink: Arc<dyn BindingBoundary> = binding_s.clone();
        let source_sink: Sink = Arc::new(move |msgs| {
            for m in msgs {
                match m.tier() {
                    3 => {
                        if let Some(h) = m.payload_handle() {
                            bb_sink.retain_handle(h);
                            if tx_sink.send(BufferTimeCmd::Value(h)).is_err() {
                                bb_sink.release_handle(h);
                            }
                        }
                    }
                    5 => {
                        if let Some(h) = m.payload_handle() {
                            bb_sink.retain_handle(h);
                            if tx_sink.send(BufferTimeCmd::Error(h)).is_err() {
                                bb_sink.release_handle(h);
                            }
                        } else {
                            let _ = tx_sink.send(BufferTimeCmd::Complete);
                        }
                    }
                    _ => {}
                }
            }
        });

        let outcome = ctx.subscribe_to(source, source_sink);
        if matches!(outcome, SubscribeOutcome::Dead { .. }) {
            let _ = tx_dead.send(BufferTimeCmd::Complete);
        }
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
        .expect("buffer_time: register_producer failed")
}

/// RAII guard for `buffer_time` task — same pattern as [`TemporalTaskGuard`]
/// but with `BufferTimeCmd` channel.
struct BufferTimeTaskGuard {
    _tx: tokio::sync::mpsc::UnboundedSender<BufferTimeCmd>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for BufferTimeTaskGuard {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn buffer_time_task(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<BufferTimeCmd>,
    core: WeakCore,
    pid: NodeId,
    binding: Arc<dyn BindingBoundary>,
    period: Duration,
    pack_fn_id: FnId,
) {
    let mut buf: Vec<HandleId> = Vec::new();
    let mut ticker = tokio::time::interval(period);
    ticker.tick().await; // First tick is immediate — skip it.

    loop {
        tokio::select! {
            biased;
            cmd = rx.recv() => {
                match cmd {
                    Some(BufferTimeCmd::Value(h)) => {
                        buf.push(h);
                    }
                    Some(BufferTimeCmd::Complete) => {
                        // Flush remaining buffer then complete.
                        if !buf.is_empty() {
                            if let Some(c) = core.upgrade() {
                                let packed = binding.pack_tuple(pack_fn_id, &buf);
                                c.emit_or_defer(pid, packed);
                            }
                            for h in buf.drain(..) {
                                binding.release_handle(h);
                            }
                        }
                        if let Some(c) = core.upgrade() {
                            c.complete_or_defer(pid);
                        }
                        return;
                    }
                    Some(BufferTimeCmd::Error(err_h)) => {
                        for h in buf.drain(..) {
                            binding.release_handle(h);
                        }
                        if let Some(c) = core.upgrade() {
                            c.error_or_defer(pid, err_h);
                        } else {
                            binding.release_handle(err_h);
                        }
                        return;
                    }
                    None => {
                        // Channel closed — deactivated. Release buffered.
                        for h in buf.drain(..) {
                            binding.release_handle(h);
                        }
                        return;
                    }
                }
            }
            _ = ticker.tick() => {
                if !buf.is_empty() {
                    if let Some(c) = core.upgrade() {
                        let packed = binding.pack_tuple(pack_fn_id, &buf);
                        c.emit_or_defer(pid, packed);
                        for h in buf.drain(..) {
                            binding.release_handle(h);
                        }
                    } else {
                        for h in buf.drain(..) {
                            binding.release_handle(h);
                        }
                        return;
                    }
                }
            }
        }
    }
}

// =========================================================================
// window_time(source, ms)
// =========================================================================

/// Command sent from the `window_time` sink to its async task.
enum WindowTimeCmd {
    /// New DATA handle from upstream. Already retained by the sink.
    Value(HandleId),
    /// Upstream COMPLETE.
    Complete,
    /// Upstream ERROR. Handle already retained.
    Error(HandleId),
}

/// Rotates sub-node windows every `ms` milliseconds.
///
/// Creates a new inner state node each interval tick. Upstream DATA is
/// forwarded to the current inner window node. On tick, the current
/// window is completed, a new inner node is created, and its identity
/// is emitted as a DATA handle (via `binding.intern_node`).
///
/// - On upstream COMPLETE: completes current window, then completes self.
/// - On upstream ERROR: errors current window, then errors self.
#[must_use]
pub fn window_time(
    core: &Core,
    binding: &Arc<dyn ProducerBinding>,
    source: NodeId,
    ms: u64,
) -> NodeId {
    let core_weak = core.weak_handle();
    let binding_weak: Weak<dyn ProducerBinding> = Arc::downgrade(binding);
    let period = Duration::from_millis(ms);

    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let (Some(core_s), Some(binding_s)) = (core_weak.upgrade(), binding_weak.upgrade()) else {
            return;
        };
        let pid = ctx.node_id();
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();

        // Pre-register a reusable no-op FnId for creating inner window
        // nodes. The async task reuses this FnId for every window rotation
        // so it only needs Core (via WeakCore), not ProducerBinding.
        let noop_fn_id = binding_s.register_producer_build(Box::new(|_ctx: ProducerCtx<'_>| {
            // Inner window nodes are passive — all emissions are driven by
            // the parent window_time task via emit_or_defer / complete_or_defer.
        }));

        // Create the first inner window node and emit its handle.
        let first_inner = core_s
            .register_producer(noop_fn_id)
            .expect("window_time inner: register_producer failed");
        let first_handle = bb.intern_node(first_inner);
        core_s.emit_or_defer(pid, first_handle);

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let tx_sink = tx.clone();
        let tx_dead = tx.clone();
        let task = tokio::spawn(window_time_task(
            rx,
            core_s.weak_handle(),
            pid,
            first_inner,
            bb.clone(),
            period,
            noop_fn_id,
        ));

        {
            let mut storage = binding_s.producer_storage().lock();
            let entry = storage.entry(pid).or_default();
            entry.op_state = Some(Box::new(WindowTimeTaskGuard { _tx: tx, task }));
        }

        let bb_sink: Arc<dyn BindingBoundary> = binding_s.clone();
        let source_sink: Sink = Arc::new(move |msgs| {
            for m in msgs {
                match m.tier() {
                    3 => {
                        if let Some(h) = m.payload_handle() {
                            bb_sink.retain_handle(h);
                            if tx_sink.send(WindowTimeCmd::Value(h)).is_err() {
                                bb_sink.release_handle(h);
                            }
                        }
                    }
                    5 => {
                        if let Some(h) = m.payload_handle() {
                            bb_sink.retain_handle(h);
                            if tx_sink.send(WindowTimeCmd::Error(h)).is_err() {
                                bb_sink.release_handle(h);
                            }
                        } else {
                            let _ = tx_sink.send(WindowTimeCmd::Complete);
                        }
                    }
                    _ => {}
                }
            }
        });

        let outcome = ctx.subscribe_to(source, source_sink);
        if matches!(outcome, SubscribeOutcome::Dead { .. }) {
            let _ = tx_dead.send(WindowTimeCmd::Complete);
        }
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
        .expect("window_time: register_producer failed")
}

/// RAII guard for `window_time` task.
struct WindowTimeTaskGuard {
    _tx: tokio::sync::mpsc::UnboundedSender<WindowTimeCmd>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for WindowTimeTaskGuard {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn window_time_task(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<WindowTimeCmd>,
    core: WeakCore,
    pid: NodeId,
    initial_inner: NodeId,
    binding: Arc<dyn BindingBoundary>,
    period: Duration,
    noop_fn_id: FnId,
) {
    let mut current_inner = initial_inner;
    let mut ticker = tokio::time::interval(period);
    ticker.tick().await; // First tick is immediate — skip it.

    loop {
        tokio::select! {
            biased;
            cmd = rx.recv() => {
                match cmd {
                    Some(WindowTimeCmd::Value(h)) => {
                        // Forward DATA to current inner window.
                        if let Some(c) = core.upgrade() {
                            c.emit_or_defer(current_inner, h);
                        } else {
                            binding.release_handle(h);
                            return;
                        }
                    }
                    Some(WindowTimeCmd::Complete) => {
                        // Complete current inner window, then complete self.
                        if let Some(c) = core.upgrade() {
                            c.complete_or_defer(current_inner);
                            c.complete_or_defer(pid);
                        }
                        return;
                    }
                    Some(WindowTimeCmd::Error(err_h)) => {
                        // Error current inner window, then error self.
                        if let Some(c) = core.upgrade() {
                            binding.retain_handle(err_h);
                            c.error_or_defer(current_inner, err_h);
                            // err_h already retained once by sink + once above;
                            // the inner node consumes one retain, self consumes the other.
                            c.error_or_defer(pid, err_h);
                        } else {
                            binding.release_handle(err_h);
                        }
                        return;
                    }
                    None => {
                        // Channel closed — deactivated.
                        return;
                    }
                }
            }
            _ = ticker.tick() => {
                // Window rotation: complete current, create new, emit handle.
                if let Some(c) = core.upgrade() {
                    c.complete_or_defer(current_inner);

                    // Create a new inner window node using the pre-registered
                    // noop FnId. Each inner node is a passive producer whose
                    // emissions are driven externally by this task.
                    if let Ok(new_inner) = c.register_producer(noop_fn_id) {
                        current_inner = new_inner;
                        let h = binding.intern_node(new_inner);
                        c.emit_or_defer(pid, h);
                    } else {
                        // Registration failed — complete self and exit.
                        c.complete_or_defer(pid);
                        return;
                    }
                } else {
                    return;
                }
            }
        }
    }
}
