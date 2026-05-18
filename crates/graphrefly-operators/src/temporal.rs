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
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use graphrefly_core::{BindingBoundary, Core, FnId, HandleId, NodeId, Sink};
use smallvec::SmallVec;

use crate::producer::{
    ProducerBinding, ProducerBuildFn, ProducerCtx, ProducerEmitter, SubscribeOutcome,
};

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

    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let core_s = ctx.core();
        let binding_s = ctx.core().binding();
        let em = ctx.emitter();
        let pid = ctx.node_id();
        let state: Arc<Mutex<SampleState>> = Arc::new(Mutex::new(SampleState::default()));

        // --- source sink ---
        let st = state.clone();
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();
        let core_src = em.clone();
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
        let core_n = em.clone();
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
    let delay = Duration::from_millis(ms);

    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let binding_s = ctx.core().binding();
        let em = ctx.emitter();
        let pid = ctx.node_id();
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let tx_sink = tx.clone();
        let tx_dead = tx.clone();
        let task = tokio::spawn(debounce_task(
            rx,
            em.clone(),
            pid,
            bb.clone(),
            delay,
        ));

        // Store guard: drops tx (clean shutdown) then aborts task (fallback).
        {
            let st = ctx.storage();
            let mut storage = st.lock();
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

// S2b/D230/D232-AMEND: takes a `ProducerEmitter` (was `WeakCore`).
// `em.{emit,complete,error}_or_defer` post to the `Core`-owned mailbox
// and internally release the payload handle if the `Core` is gone (F2,
// QA-hardened) — so every old `if let Some(c)=core.upgrade(){ c.X }
// else { binding.release_handle(h) }` collapses to a direct `em.X`.
// `em.is_core_gone()` preserves the old teardown-promptness where the
// `else` branch also `return`ed (not leak-load-bearing — the task also
// exits on channel-close — only prompt shutdown).
async fn debounce_task(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<TemporalCmd>,
    em: ProducerEmitter,
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
                            // Flush pending then complete (em releases
                            // `h` itself if the Core is gone).
                            em.emit_or_defer(pid, h);
                            em.complete_or_defer(pid);
                            return;
                        }
                        Some(TemporalCmd::Error(err_h)) => {
                            binding.release_handle(h);
                            em.error_or_defer(pid, err_h);
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
                    em.emit_or_defer(pid, h);
                    if em.is_core_gone() {
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
                    em.complete_or_defer(pid);
                    return;
                }
                Some(TemporalCmd::Error(err_h)) => {
                    em.error_or_defer(pid, err_h);
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
    let delay = Duration::from_millis(ms);

    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let binding_s = ctx.core().binding();
        let em = ctx.emitter();
        let pid = ctx.node_id();
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let tx_sink = tx.clone();
        let tx_dead = tx.clone();
        let task = tokio::spawn(audit_task(rx, em.clone(), pid, bb.clone(), delay));

        {
            let st = ctx.storage();
            let mut storage = st.lock();
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

// S2b/D230/D232-AMEND: `core: WeakCore` → `em: ProducerEmitter` (same
// collapse rationale as `debounce_task`).
async fn audit_task(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<TemporalCmd>,
    em: ProducerEmitter,
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
                                            em.emit_or_defer(pid, latest);
                                            em.complete_or_defer(pid);
                                            return; // exits the async block
                                        }
                                        Some(TemporalCmd::Error(err_h)) => {
                                            binding.release_handle(latest);
                                            em.error_or_defer(pid, err_h);
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
                        em.emit_or_defer(pid, latest);
                        if em.is_core_gone() {
                            return;
                        }
                        // Continue outer loop — wait for next window.
                    }
                }
            }
            Some(TemporalCmd::Complete) => {
                em.complete_or_defer(pid);
                return;
            }
            Some(TemporalCmd::Error(err_h)) => {
                em.error_or_defer(pid, err_h);
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
    let delay_dur = Duration::from_millis(ms);

    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let binding_s = ctx.core().binding();
        let em = ctx.emitter();
        let pid = ctx.node_id();
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let tx_sink = tx.clone();
        let tx_dead = tx.clone();
        let task = tokio::spawn(delay_task(
            rx,
            em.clone(),
            pid,
            bb.clone(),
            delay_dur,
        ));

        {
            let st = ctx.storage();
            let mut storage = st.lock();
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

// S2b/D230/D232-AMEND: `core: WeakCore` → `em: ProducerEmitter`.
async fn delay_task(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<TemporalCmd>,
    em: ProducerEmitter,
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
                            em.complete_or_defer(pid);
                            return;
                        }
                        // Wait for pending timers to drain.
                    }
                    Some(TemporalCmd::Error(err_h)) => {
                        // Release all pending.
                        for (_, h) in queue.drain(..) {
                            binding.release_handle(h);
                        }
                        em.error_or_defer(pid, err_h);
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
                        em.emit_or_defer(pid, h);
                        if em.is_core_gone() {
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
                    em.complete_or_defer(pid);
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
    let window = Duration::from_millis(ms);

    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let binding_s = ctx.core().binding();
        let em = ctx.emitter();
        let pid = ctx.node_id();
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let tx_sink = tx.clone();
        let tx_dead = tx.clone();
        let task = tokio::spawn(throttle_task(
            rx,
            em.clone(),
            pid,
            bb.clone(),
            window,
            opts,
        ));

        {
            let st = ctx.storage();
            let mut storage = st.lock();
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
    // S2b/D230/D232-AMEND: `WeakCore` → `ProducerEmitter`.
    em: ProducerEmitter,
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
                                em.emit_or_defer(pid, h);
                                if em.is_core_gone() {
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
                            em.emit_or_defer(pid, h);
                        }
                        em.complete_or_defer(pid);
                        return;
                    }
                    Some(TemporalCmd::Error(err_h)) => {
                        release_opt(&mut trailing_pending, &*binding);
                        em.error_or_defer(pid, err_h);
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
                    em.emit_or_defer(pid, h);
                    if em.is_core_gone() {
                        return;
                    }
                    // Start new window for the trailing emission.
                    window_deadline = Some(tokio::time::Instant::now() + window);
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
    let period = Duration::from_millis(period_ms);

    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let binding_s = ctx.core().binding();
        let em = ctx.emitter();
        let pid = ctx.node_id();
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();
        let em_task = em.clone();

        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(period);
            ticker.tick().await; // First tick is immediate — skip it.
            let mut counter: u64 = 1; // Start at 1: HandleId(0) = NO_HANDLE.
            loop {
                ticker.tick().await;
                // S2b/D230: stop ticking once the Core is gone (was the
                // `weak.upgrade() == None` break); check BEFORE retaining
                // so a dead Core never leaks a fresh counter handle.
                if em_task.is_core_gone() {
                    break;
                }
                let h = HandleId::new(counter);
                bb.retain_handle(h);
                em_task.emit_or_defer(pid, h);
                counter += 1;
            }
        });

        {
            let st = ctx.storage();
            let mut storage = st.lock();
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
    let duration = Duration::from_millis(ms);

    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let binding_s = ctx.core().binding();
        let em = ctx.emitter();
        let pid = ctx.node_id();
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let tx_sink = tx.clone();
        let tx_dead = tx.clone();
        let task = tokio::spawn(timeout_task(
            rx,
            em.clone(),
            pid,
            bb.clone(),
            duration,
            error_handle,
        ));

        {
            let st = ctx.storage();
            let mut storage = st.lock();
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

// S2b/D230/D232-AMEND: `WeakCore` → `ProducerEmitter`.
async fn timeout_task(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<TemporalCmd>,
    em: ProducerEmitter,
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
                        em.emit_or_defer(pid, h);
                        if em.is_core_gone() {
                            return;
                        }
                        // Continue loop → resets the sleep timer.
                    }
                    Some(TemporalCmd::Complete) => {
                        em.complete_or_defer(pid);
                        return;
                    }
                    Some(TemporalCmd::Error(err_h)) => {
                        em.error_or_defer(pid, err_h);
                        return;
                    }
                    None => return,
                }
            }
            () = tokio::time::sleep(duration) => {
                // Timeout fired — emit error. Retain BEFORE the post
                // (skip if the Core is gone — nothing to error into).
                if !em.is_core_gone() {
                    binding.retain_handle(error_handle);
                    em.error_or_defer(pid, error_handle);
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
    let period = Duration::from_millis(ms);

    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let binding_s = ctx.core().binding();
        let em = ctx.emitter();
        let pid = ctx.node_id();
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let tx_sink = tx.clone();
        let tx_dead = tx.clone();
        let task = tokio::spawn(buffer_time_task(
            rx,
            em.clone(),
            pid,
            bb.clone(),
            period,
            pack_fn_id,
        ));

        {
            let st = ctx.storage();
            let mut storage = st.lock();
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

// S2b/D230/D232-AMEND: `WeakCore` → `ProducerEmitter`. Note: the
// emit-path now always `pack_tuple`s even on a gone Core (em releases
// the packed handle); the buffered component handles are drained-released
// exactly as before — behaviour-equivalent, just one extra teardown-only
// pack FFI.
async fn buffer_time_task(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<BufferTimeCmd>,
    em: ProducerEmitter,
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
                            let packed = binding.pack_tuple(pack_fn_id, &buf);
                            em.emit_or_defer(pid, packed);
                            for h in buf.drain(..) {
                                binding.release_handle(h);
                            }
                        }
                        em.complete_or_defer(pid);
                        return;
                    }
                    Some(BufferTimeCmd::Error(err_h)) => {
                        for h in buf.drain(..) {
                            binding.release_handle(h);
                        }
                        em.error_or_defer(pid, err_h);
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
                    let packed = binding.pack_tuple(pack_fn_id, &buf);
                    em.emit_or_defer(pid, packed);
                    for h in buf.drain(..) {
                        binding.release_handle(h);
                    }
                    if em.is_core_gone() {
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
    let period = Duration::from_millis(ms);

    // S2b/D231: register the reusable no-op inner-window build at FACTORY
    // scope (where `binding: &Arc<dyn ProducerBinding>` is in hand) — the
    // build closure no longer holds a `ProducerBinding`, only `ctx`.
    // `FnId` is `Copy`; capture it into the build/task.
    let noop_fn_id = binding.register_producer_build(Box::new(|_ctx: ProducerCtx<'_>| {
        // Inner window nodes are passive — all emissions are driven by
        // the parent window_time task via the mailbox.
    }));

    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let core_s = ctx.core();
        let binding_s = ctx.core().binding();
        let em = ctx.emitter();
        let pid = ctx.node_id();
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();

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
            em.clone(),
            pid,
            first_inner,
            bb.clone(),
            period,
            noop_fn_id,
        ));

        {
            let st = ctx.storage();
            let mut storage = st.lock();
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

// S2b/D230/D234: `WeakCore` → `ProducerEmitter`; the window rotation
// does task-side topology mutation (`register_producer`) so it routes
// through `em.defer` (`CoreFull`). `current_inner` (the routing
// selector) becomes a shared `Arc<Mutex<NodeId>>` read INSIDE the
// owner-serialized defer closures, so the FIFO mailbox order
// (arrival order) keeps every forward routed to the window live at the
// time it arrived — the D234 invariant (same as `window`).
async fn window_time_task(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<WindowTimeCmd>,
    em: ProducerEmitter,
    pid: NodeId,
    initial_inner: NodeId,
    binding: Arc<dyn BindingBoundary>,
    period: Duration,
    noop_fn_id: FnId,
) {
    let current_inner = Arc::new(Mutex::new(initial_inner));
    let mut ticker = tokio::time::interval(period);
    ticker.tick().await; // First tick is immediate — skip it.

    loop {
        tokio::select! {
            biased;
            cmd = rx.recv() => {
                match cmd {
                    Some(WindowTimeCmd::Value(h)) => {
                        // Forward DATA to whatever window is current when
                        // this defer runs (FIFO-ordered after any rotation
                        // posted earlier).
                        let cur = current_inner.clone();
                        let b = binding.clone();
                        if !em.defer(move |c| {
                            c.emit(*cur.lock(), h);
                        }) {
                            b.release_handle(h);
                        }
                    }
                    Some(WindowTimeCmd::Complete) => {
                        let cur = current_inner.clone();
                        let _ = em.defer(move |c| {
                            c.complete(*cur.lock());
                            c.complete(pid);
                        });
                        return;
                    }
                    Some(WindowTimeCmd::Error(err_h)) => {
                        // err_h retained once by the sink; the 2nd retain
                        // (for the self-error) is done INSIDE the closure
                        // so a Core-gone drop leaves exactly the sink's
                        // one retain to release (mirrors the old `else`).
                        let cur = current_inner.clone();
                        let b = binding.clone();
                        let b2 = binding.clone();
                        if !em.defer(move |c| {
                            b2.retain_handle(err_h);
                            c.error(*cur.lock(), err_h);
                            c.error(pid, err_h);
                        }) {
                            b.release_handle(err_h);
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
                // Window rotation: complete current, create new, emit
                // handle — ALL inside one owner-side closure so the
                // `register_producer` + `current_inner` swap is atomic
                // w.r.t. the FIFO drain (D234).
                let cur = current_inner.clone();
                let b = binding.clone();
                let _ = em.defer(move |c| {
                    let old = *cur.lock();
                    c.complete(old);
                    match c.register_producer(noop_fn_id) {
                        Ok(new_inner) => {
                            *cur.lock() = new_inner;
                            let h = b.intern_node(new_inner);
                            c.emit(pid, h);
                        }
                        Err(_) => {
                            // Registration failed — complete self.
                            c.complete(pid);
                        }
                    }
                });
                if em.is_core_gone() {
                    return;
                }
            }
        }
    }
}
