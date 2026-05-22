//! Control operators — side-effect, gating, error recovery, convergence.
//!
//! # Operators
//!
//! - [`tap`] / [`tap_observer`] — side-effect passthrough.
//! - [`on_first_data`] — one-shot side-effect on first DATA.
//! - [`rescue`] — error recovery via user callback.
//! - [`valve`] — boolean-gated passthrough with optional cancellation.
//! - [`settle`] — wave-count convergence detector.
//! - [`repeat`] — sequential resubscribe loop.

#![allow(clippy::too_many_lines, clippy::items_after_statements)]

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use graphrefly_core::{BindingBoundary, Core, FnId, HandleId, NodeId, Sink};
use smallvec::SmallVec;

use crate::producer::{ProducerBinding, ProducerBuildFn, ProducerCtx, SubscribeOutcome};

// =========================================================================
// tap(source, fn_id) — side-effect passthrough
// =========================================================================

/// Passthrough operator that forwards all DATA unchanged. On each DATA,
/// calls `binding.invoke_tap_fn(fn_id, handle)` as a side-effect.
/// Forwards COMPLETE and ERROR unchanged.
#[must_use]
pub fn tap(core: &Core, binding: &Arc<dyn ProducerBinding>, source: NodeId, fn_id: FnId) -> NodeId {
    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let core_s = ctx.core();
        let binding_s = ctx.core().binding();
        let em = ctx.emitter();
        let pid = ctx.node_id();
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();
        let core_sink = em.clone();

        let source_sink: Sink = Rc::new(move |msgs| {
            enum Act {
                EmitAndTap(HandleId),
                Complete,
                Error(HandleId),
            }
            let mut actions: SmallVec<[Act; 4]> = SmallVec::new();
            for m in msgs {
                match m.tier() {
                    3 => {
                        if let Some(h) = m.payload_handle() {
                            bb.retain_handle(h);
                            actions.push(Act::EmitAndTap(h));
                        }
                    }
                    5 => {
                        if let Some(h) = m.payload_handle() {
                            bb.retain_handle(h);
                            actions.push(Act::Error(h));
                        } else {
                            actions.push(Act::Complete);
                        }
                    }
                    _ => {}
                }
            }
            for a in actions {
                match a {
                    Act::EmitAndTap(h) => {
                        bb.invoke_tap_fn(fn_id, h);
                        core_sink.emit_or_defer(pid, h);
                    }
                    Act::Complete => core_sink.complete_or_defer(pid),
                    Act::Error(h) => core_sink.error_or_defer(pid, h),
                }
            }
        });

        let outcome = ctx.subscribe_to(source, source_sink);
        if matches!(outcome, SubscribeOutcome::Dead { .. }) {
            core_s.complete_or_defer(pid);
        }
    });

    let fn_id_reg = binding.register_producer_build(build);
    core.register_producer(fn_id_reg)
        .expect("tap: register_producer failed")
}

// =========================================================================
// tap_observer(source, data_fn, error_fn, complete_fn)
// =========================================================================

/// Like [`tap`] but with lifecycle observer callbacks. Each callback is
/// optional (`None` = skip that lifecycle event).
///
/// - `data_fn_id`: called on each DATA via `invoke_tap_fn`.
/// - `error_fn_id`: called on ERROR via `invoke_tap_error_fn`.
/// - `complete_fn_id`: called on COMPLETE via `invoke_tap_complete_fn`.
///
/// All messages are forwarded unchanged regardless of callback presence.
#[must_use]
pub fn tap_observer(
    core: &Core,
    binding: &Arc<dyn ProducerBinding>,
    source: NodeId,
    data_fn_id: Option<FnId>,
    error_fn_id: Option<FnId>,
    complete_fn_id: Option<FnId>,
) -> NodeId {
    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let core_s = ctx.core();
        let binding_s = ctx.core().binding();
        let em = ctx.emitter();
        let pid = ctx.node_id();
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();
        let core_sink = em.clone();

        let source_sink: Sink = Rc::new(move |msgs| {
            enum Act {
                Emit(HandleId),
                Complete,
                Error(HandleId),
            }
            let mut actions: SmallVec<[Act; 4]> = SmallVec::new();
            for m in msgs {
                match m.tier() {
                    3 => {
                        if let Some(h) = m.payload_handle() {
                            bb.retain_handle(h);
                            actions.push(Act::Emit(h));
                        }
                    }
                    5 => {
                        if let Some(h) = m.payload_handle() {
                            bb.retain_handle(h);
                            actions.push(Act::Error(h));
                        } else {
                            actions.push(Act::Complete);
                        }
                    }
                    _ => {}
                }
            }
            for a in actions {
                match a {
                    Act::Emit(h) => {
                        if let Some(fid) = data_fn_id {
                            bb.invoke_tap_fn(fid, h);
                        }
                        core_sink.emit_or_defer(pid, h);
                    }
                    Act::Complete => {
                        if let Some(fid) = complete_fn_id {
                            bb.invoke_tap_complete_fn(fid);
                        }
                        core_sink.complete_or_defer(pid);
                    }
                    Act::Error(h) => {
                        if let Some(fid) = error_fn_id {
                            bb.invoke_tap_error_fn(fid, h);
                        }
                        core_sink.error_or_defer(pid, h);
                    }
                }
            }
        });

        let outcome = ctx.subscribe_to(source, source_sink);
        if matches!(outcome, SubscribeOutcome::Dead { .. }) {
            if let Some(fid) = complete_fn_id {
                binding_s.invoke_tap_complete_fn(fid);
            }
            core_s.complete_or_defer(pid);
        }
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
        .expect("tap_observer: register_producer failed")
}

// =========================================================================
// on_first_data(source, fn_id) — one-shot tap
// =========================================================================

/// One-shot side-effect tap. Calls `invoke_tap_fn(fn_id, handle)` on
/// the first DATA only, then becomes a pure passthrough. All messages
/// are forwarded unchanged.
#[must_use]
pub fn on_first_data(
    core: &Core,
    binding: &Arc<dyn ProducerBinding>,
    source: NodeId,
    fn_id: FnId,
) -> NodeId {
    struct OnFirstState {
        fired: bool,
    }

    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let core_s = ctx.core();
        let binding_s = ctx.core().binding();
        let em = ctx.emitter();
        let pid = ctx.node_id();
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();
        let core_sink = em.clone();
        let state: Rc<RefCell<OnFirstState>> = Rc::new(RefCell::new(OnFirstState { fired: false }));

        let source_sink: Sink = Rc::new(move |msgs| {
            enum Act {
                EmitWithTap(HandleId),
                Emit(HandleId),
                Complete,
                Error(HandleId),
            }
            let mut actions: SmallVec<[Act; 4]> = SmallVec::new();
            {
                let mut s = state.borrow_mut();
                for m in msgs {
                    match m.tier() {
                        3 => {
                            if let Some(h) = m.payload_handle() {
                                bb.retain_handle(h);
                                if s.fired {
                                    actions.push(Act::Emit(h));
                                } else {
                                    s.fired = true;
                                    actions.push(Act::EmitWithTap(h));
                                }
                            }
                        }
                        5 => {
                            if let Some(h) = m.payload_handle() {
                                bb.retain_handle(h);
                                actions.push(Act::Error(h));
                            } else {
                                actions.push(Act::Complete);
                            }
                        }
                        _ => {}
                    }
                }
            }
            for a in actions {
                match a {
                    Act::EmitWithTap(h) => {
                        bb.invoke_tap_fn(fn_id, h);
                        core_sink.emit_or_defer(pid, h);
                    }
                    Act::Emit(h) => core_sink.emit_or_defer(pid, h),
                    Act::Complete => core_sink.complete_or_defer(pid),
                    Act::Error(h) => core_sink.error_or_defer(pid, h),
                }
            }
        });

        let outcome = ctx.subscribe_to(source, source_sink);
        if matches!(outcome, SubscribeOutcome::Dead { .. }) {
            core_s.complete_or_defer(pid);
        }
    });

    let fn_id_reg = binding.register_producer_build(build);
    core.register_producer(fn_id_reg)
        .expect("on_first_data: register_producer failed")
}

// =========================================================================
// rescue(source, fn_id) — error recovery
// =========================================================================

/// Error recovery operator. On ERROR, calls
/// `binding.invoke_rescue_fn(fn_id, error_handle)`:
///
/// - `Ok(recovered_handle)` — emit DATA with the recovered value.
/// - `Err(())` — forward the original ERROR unchanged.
///
/// DATA and COMPLETE pass through unchanged.
#[must_use]
pub fn rescue(
    core: &Core,
    binding: &Arc<dyn ProducerBinding>,
    source: NodeId,
    fn_id: FnId,
) -> NodeId {
    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let core_s = ctx.core();
        let binding_s = ctx.core().binding();
        let em = ctx.emitter();
        let pid = ctx.node_id();
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();
        let core_sink = em.clone();

        let source_sink: Sink = Rc::new(move |msgs| {
            enum Act {
                Emit(HandleId),
                Complete,
                TryRescue(HandleId),
            }
            let mut actions: SmallVec<[Act; 4]> = SmallVec::new();
            for m in msgs {
                match m.tier() {
                    3 => {
                        if let Some(h) = m.payload_handle() {
                            bb.retain_handle(h);
                            actions.push(Act::Emit(h));
                        }
                    }
                    5 => {
                        if let Some(h) = m.payload_handle() {
                            bb.retain_handle(h);
                            actions.push(Act::TryRescue(h));
                        } else {
                            actions.push(Act::Complete);
                        }
                    }
                    _ => {}
                }
            }
            for a in actions {
                match a {
                    Act::Emit(h) => core_sink.emit_or_defer(pid, h),
                    Act::Complete => core_sink.complete_or_defer(pid),
                    Act::TryRescue(err_h) => {
                        match bb.invoke_rescue_fn(fn_id, err_h) {
                            Ok(recovered_h) => {
                                // Recovery succeeded — release original error,
                                // emit recovered value as DATA.
                                bb.release_handle(err_h);
                                core_sink.emit_or_defer(pid, recovered_h);
                            }
                            Err(()) => {
                                // Recovery failed — forward original ERROR.
                                core_sink.error_or_defer(pid, err_h);
                            }
                        }
                    }
                }
            }
        });

        let outcome = ctx.subscribe_to(source, source_sink);
        if matches!(outcome, SubscribeOutcome::Dead { .. }) {
            core_s.complete_or_defer(pid);
        }
    });

    let fn_id_reg = binding.register_producer_build(build);
    core.register_producer(fn_id_reg)
        .expect("rescue: register_producer failed")
}

// =========================================================================
// valve(source, control, gate_fn_id, cancel) — boolean gate
// =========================================================================

/// Boolean-gated passthrough. Subscribes to both `source` and `control`.
///
/// - **Control DATA**: evaluates `binding.predicate_each(gate_fn_id,
///   &[handle])` to determine gate state. `true` = open, `false` = closed.
///   On transition from open to closed, if `cancel` is `Some`, invokes
///   `cancel.cancel()`.
/// - **Source DATA**: if gate is open, retains + emits. If closed, drops
///   the handle (source DATA while gate is closed is silently discarded).
/// - **Source COMPLETE/ERROR**: forwarded unchanged.
/// - **Control COMPLETE**: does NOT auto-complete the valve (per TS
///   `completeWhenDepsComplete: false` equivalent).
/// - **Control ERROR**: terminates the valve with ERROR.
#[must_use]
pub fn valve(
    core: &Core,
    binding: &Arc<dyn ProducerBinding>,
    source: NodeId,
    control: NodeId,
    gate_fn_id: FnId,
    cancel: Option<tokio_util::sync::CancellationToken>,
) -> NodeId {
    struct ValveState {
        open: bool,
        terminated: bool,
    }

    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let core_s = ctx.core();
        let binding_s = ctx.core().binding();
        let em = ctx.emitter();
        let pid = ctx.node_id();
        let state: Rc<RefCell<ValveState>> = Rc::new(RefCell::new(ValveState {
            open: false,
            terminated: false,
        }));

        // --- control sink ---
        let st_ctrl = state.clone();
        let bb_ctrl: Arc<dyn BindingBoundary> = binding_s.clone();
        let core_ctrl = em.clone();
        let cancel_ctrl = cancel.clone();
        let control_sink: Sink = Rc::new(move |msgs| {
            let mut should_cancel = false;
            let mut error_action: Option<HandleId> = None;
            {
                let mut s = st_ctrl.borrow_mut();
                if s.terminated {
                    return;
                }
                for m in msgs {
                    match m.tier() {
                        3 => {
                            if let Some(h) = m.payload_handle() {
                                let results = bb_ctrl.predicate_each(gate_fn_id, &[h]);
                                let new_open = results.first().copied().unwrap_or(false);
                                let was_open = s.open;
                                s.open = new_open;
                                // Transition open -> closed: trigger cancel.
                                if was_open && !new_open && cancel_ctrl.is_some() {
                                    should_cancel = true;
                                }
                            }
                        }
                        5 => {
                            if let Some(h) = m.payload_handle() {
                                // Control ERROR terminates valve.
                                if !s.terminated {
                                    s.terminated = true;
                                    bb_ctrl.retain_handle(h);
                                    error_action = Some(h);
                                }
                            }
                            // Control COMPLETE: do NOT auto-complete.
                        }
                        _ => {}
                    }
                }
            }
            if should_cancel {
                if let Some(ref ct) = cancel_ctrl {
                    ct.cancel();
                }
            }
            if let Some(h) = error_action {
                core_ctrl.error_or_defer(pid, h);
            }
        });

        let ctrl_outcome = ctx.subscribe_to(control, control_sink);
        if matches!(ctrl_outcome, SubscribeOutcome::Dead { .. }) {
            // Control is dead — gate state remains as-is (closed by default).
            // No auto-complete; source can still flow if gate was already opened.
        }

        // --- source sink ---
        let st_src = state.clone();
        let bb_src: Arc<dyn BindingBoundary> = binding_s.clone();
        let core_src = em.clone();
        let source_sink: Sink = Rc::new(move |msgs| {
            enum Act {
                Emit(HandleId),
                Complete,
                Error(HandleId),
            }
            let mut actions: SmallVec<[Act; 4]> = SmallVec::new();
            {
                let s = st_src.borrow_mut();
                if s.terminated {
                    return;
                }
                for m in msgs {
                    match m.tier() {
                        3 => {
                            if let Some(h) = m.payload_handle() {
                                if s.open {
                                    bb_src.retain_handle(h);
                                    actions.push(Act::Emit(h));
                                }
                                // Closed gate: silently discard.
                            }
                        }
                        5 => {
                            if let Some(h) = m.payload_handle() {
                                bb_src.retain_handle(h);
                                actions.push(Act::Error(h));
                            } else {
                                actions.push(Act::Complete);
                            }
                        }
                        _ => {}
                    }
                }
            }
            for a in actions {
                match a {
                    Act::Emit(h) => core_src.emit_or_defer(pid, h),
                    Act::Complete => core_src.complete_or_defer(pid),
                    Act::Error(h) => core_src.error_or_defer(pid, h),
                }
            }
        });

        let src_outcome = ctx.subscribe_to(source, source_sink);
        if matches!(src_outcome, SubscribeOutcome::Dead { .. }) {
            let mut s = state.borrow_mut();
            if !s.terminated {
                s.terminated = true;
                drop(s);
                core_s.complete_or_defer(pid);
            }
        }
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
        .expect("valve: register_producer failed")
}

// =========================================================================
// settle(source, quiet_waves, max_waves) — convergence detector
// =========================================================================

/// Wave-count convergence detector.
///
/// - On DATA: resets `quiet_count` to 0, increments `wave_count`. Forwards
///   DATA unchanged. If `max_waves` is set and `wave_count >= max_waves`,
///   completes.
/// - On RESOLVED (tier 3, no payload): increments `quiet_count`. If
///   `quiet_count >= quiet_waves`, completes.
/// - On COMPLETE/ERROR: forwarded unchanged.
#[must_use]
pub fn settle(
    core: &Core,
    binding: &Arc<dyn ProducerBinding>,
    source: NodeId,
    quiet_waves: u32,
    max_waves: Option<u32>,
) -> NodeId {
    struct SettleState {
        wave_count: u32,
        quiet_count: u32,
        completed: bool,
    }

    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let core_s = ctx.core();
        let binding_s = ctx.core().binding();
        let em = ctx.emitter();
        let pid = ctx.node_id();
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();
        let core_sink = em.clone();
        let state: Rc<RefCell<SettleState>> = Rc::new(RefCell::new(SettleState {
            wave_count: 0,
            quiet_count: 0,
            completed: false,
        }));

        let source_sink: Sink = Rc::new(move |msgs| {
            enum Act {
                Emit(HandleId),
                Complete,
                Error(HandleId),
                SelfComplete,
            }
            let mut actions: SmallVec<[Act; 4]> = SmallVec::new();
            {
                let mut s = state.borrow_mut();
                if s.completed {
                    return;
                }
                for m in msgs {
                    if s.completed {
                        break;
                    }
                    match m.tier() {
                        3 => {
                            if let Some(h) = m.payload_handle() {
                                // DATA: reset quiet, increment waves.
                                s.quiet_count = 0;
                                s.wave_count += 1;
                                bb.retain_handle(h);
                                actions.push(Act::Emit(h));
                                // Check max_waves.
                                if let Some(max) = max_waves {
                                    if s.wave_count >= max {
                                        s.completed = true;
                                        actions.push(Act::SelfComplete);
                                    }
                                }
                            } else {
                                // RESOLVED (tier 3, no payload): quiet wave.
                                s.quiet_count += 1;
                                if s.quiet_count >= quiet_waves {
                                    s.completed = true;
                                    actions.push(Act::SelfComplete);
                                }
                            }
                        }
                        5 => {
                            if let Some(h) = m.payload_handle() {
                                s.completed = true;
                                bb.retain_handle(h);
                                actions.push(Act::Error(h));
                            } else {
                                s.completed = true;
                                actions.push(Act::Complete);
                            }
                        }
                        _ => {}
                    }
                }
            }
            for a in actions {
                match a {
                    Act::Emit(h) => core_sink.emit_or_defer(pid, h),
                    Act::Complete | Act::SelfComplete => core_sink.complete_or_defer(pid),
                    Act::Error(h) => core_sink.error_or_defer(pid, h),
                }
            }
        });

        let outcome = ctx.subscribe_to(source, source_sink);
        if matches!(outcome, SubscribeOutcome::Dead { .. }) {
            core_s.complete_or_defer(pid);
        }
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
        .expect("settle: register_producer failed")
}

// =========================================================================
// repeat(source, count) — sequential resubscribe loop
// =========================================================================

/// Sequential resubscribe loop. Forwards all DATA from `source`. On
/// source COMPLETE, if `remaining > 0`, resubscribes to `source` and
/// decrements `remaining`. On ERROR, terminates immediately (no retry).
///
/// `count` is the number of ADDITIONAL subscriptions after the initial
/// one. `count = 0` is identity passthrough. `count = 2` means the
/// source will be subscribed up to 3 times total.
#[must_use]
pub fn repeat(
    core: &Core,
    binding: &Arc<dyn ProducerBinding>,
    source: NodeId,
    count: u32,
) -> NodeId {
    struct RepeatState {
        remaining: u32,
        terminated: bool,
    }

    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let core_s = ctx.core();
        let binding_s = ctx.core().binding();
        let em = ctx.emitter();
        let pid = ctx.node_id();
        let state: Rc<RefCell<RepeatState>> = Rc::new(RefCell::new(RepeatState {
            remaining: count,
            terminated: false,
        }));

        // We need to store the subscription in producer storage and be able
        // to replace it on resubscribe. S2b/D231: storage via the new
        // `ProducerCtx::storage()` accessor (the build closure no longer
        // holds a `ProducerBinding`). Resubscribe re-enters Core to
        // `try_subscribe`, which a long-lived sink can only do via
        // `em.defer` (D234) — see the `Act::Resubscribe` arm.
        let storage = ctx.storage();

        // Build the sink closure. It needs to reference itself for
        // resubscription, so we use a shared slot. Single-owner since
        // D248/D272 (`Sink = Rc<dyn Fn>`); the slot is owner-thread-only.
        let sink_slot: Rc<RefCell<Option<Sink>>> = Rc::new(RefCell::new(None));
        let sink_slot_inner = sink_slot.clone();
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();
        let core_sink = em.clone();
        let storage_inner = storage.clone();

        let source_sink: Sink = Rc::new(move |msgs| {
            enum Act {
                Emit(HandleId),
                Error(HandleId),
                Resubscribe,
                Complete,
            }
            let mut actions: SmallVec<[Act; 4]> = SmallVec::new();
            {
                let mut s = state.borrow_mut();
                if s.terminated {
                    return;
                }
                for m in msgs {
                    if s.terminated {
                        break;
                    }
                    match m.tier() {
                        3 => {
                            if let Some(h) = m.payload_handle() {
                                bb.retain_handle(h);
                                actions.push(Act::Emit(h));
                            }
                        }
                        5 => {
                            if let Some(h) = m.payload_handle() {
                                // ERROR — terminate immediately.
                                s.terminated = true;
                                bb.retain_handle(h);
                                actions.push(Act::Error(h));
                            } else {
                                // COMPLETE — resubscribe if remaining > 0.
                                if s.remaining > 0 {
                                    s.remaining -= 1;
                                    actions.push(Act::Resubscribe);
                                } else {
                                    s.terminated = true;
                                    actions.push(Act::Complete);
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            for a in actions {
                match a {
                    Act::Emit(h) => core_sink.emit_or_defer(pid, h),
                    Act::Error(h) => core_sink.error_or_defer(pid, h),
                    Act::Complete => core_sink.complete_or_defer(pid),
                    Act::Resubscribe => {
                        // Get our own sink from the shared slot.
                        let maybe_sink = sink_slot_inner.borrow_mut().clone();
                        if let Some(new_sink) = maybe_sink {
                            // D234: a long-lived sink can't hold `&Core`;
                            // route the re-subscribe through `em.defer`
                            // (owner-side, in-wave, FIFO-ordered). The
                            // returned `SubscriptionId` is recorded
                            // (D229 `(source, sub)` pair) inside the
                            // closure; the dead/violation path completes
                            // self there too.
                            let storage_d = storage_inner.clone();
                            let state_d = state.clone();
                            let _ = core_sink.defer(move |c| {
                                if let Ok(sub) = c.try_subscribe(source, new_sink) {
                                    storage_d
                                        .lock()
                                        .entry(pid)
                                        .or_default()
                                        .subs
                                        .push((source, sub));
                                } else {
                                    // Source dead / partition
                                    // violation — terminal complete.
                                    let mut s = state_d.borrow_mut();
                                    if !s.terminated {
                                        s.terminated = true;
                                        drop(s);
                                        c.complete(pid);
                                    }
                                }
                            });
                        }
                    }
                }
            }
        });

        // Store sink in the shared slot so resubscribe can access it.
        *sink_slot.borrow_mut() = Some(source_sink.clone());

        let outcome = ctx.subscribe_to(source, source_sink);
        if matches!(outcome, SubscribeOutcome::Dead { .. }) {
            // Dead source (non-resubscribable + terminated) will stay
            // dead — resubscribing won't help. Complete immediately.
            core_s.complete_or_defer(pid);
        }
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
        .expect("repeat: register_producer failed")
}
