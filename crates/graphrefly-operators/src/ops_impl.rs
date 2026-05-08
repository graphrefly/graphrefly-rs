//! Concrete implementations of the four subscription-managed combinators
//! (zip / concat / race / takeUntil). Built on the
//! [`super::producer::ProducerCtx`] substrate.

// Each sink closure runs a small phase-1/phase-2 dance (lock state,
// collect actions, drop lock, replay actions). The match-then-if
// structure inside phase 1 is intentional for readability; collapsing
// to `match { Some(...) if cond => ... }` obscures the lock-discipline.
#![allow(clippy::collapsible_if, clippy::collapsible_match)]
// `zip` is genuinely large (multi-arity tuple-pack with terminal-
// cascade handling). Splitting would obscure the concurrent state-
// machine logic across helpers without clear benefit.
#![allow(clippy::too_many_lines)]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Weak};

use graphrefly_core::{Core, HandleId, NodeId, Sink};
use smallvec::SmallVec;

use super::producer::{ProducerBinding, ProducerCtx};

// =====================================================================
// zip — pair handles N-wise across N sources
// =====================================================================

/// Per-zip-node state: one FIFO queue per source, plus a flag for each
/// source's terminal. Lives behind `Arc<Mutex<_>>` captured by the
/// build + sink closures.
struct ZipState {
    queues: Vec<VecDeque<HandleId>>,
    completed: Vec<bool>,
    errored: bool,
    terminated: bool,
}

impl ZipState {
    fn new(n: usize) -> Self {
        Self {
            queues: (0..n).map(|_| VecDeque::new()).collect(),
            completed: vec![false; n],
            errored: false,
            terminated: false,
        }
    }
}

/// `zip(s1, s2, ..., sN)` — collect one value from each source, emit a
/// tuple, repeat. Models RxJS / TS `zip`:
///
/// - Each upstream DATA pushes into that source's per-source queue.
/// - When **every** queue has at least one entry, pop one from each,
///   pack into a tuple via [`graphrefly_core::BindingBoundary::pack_tuple`],
///   and emit on the producer.
/// - On any source's COMPLETE: if its queue is empty, terminate the
///   producer with COMPLETE. Otherwise continue draining; terminate
///   when this source's queue becomes empty (zip can't produce a
///   tuple without input from every source).
/// - On any source's ERROR: terminate the producer with the same
///   ERROR (first error wins, like merge per Slice C-2 D022).
///
/// Empty source list (`n == 0`) emits a single empty-tuple event then
/// completes. Single source (`n == 1`) is identity-passthrough.
///
/// # Refcount discipline
///
/// Each upstream DATA handle is `retain_handle`-bumped before being
/// pushed onto a queue (the inbound message's payload retain belongs
/// to the wave-end-flush release path; we take our own share for the
/// queue). On pop, component handles are passed to `pack_tuple` which
/// must NOT consume or release them — the caller (zip) retains
/// ownership throughout the call and releases each component handle's
/// queue share after `pack_tuple` returns. The returned tuple handle
/// has a pre-bumped retain (binding convention per D020 doc on
/// [`BindingBoundary::pack_tuple`]).
#[must_use]
pub fn zip(
    core: &Core,
    binding: &Arc<dyn ProducerBinding>,
    sources: Vec<NodeId>,
    pack_fn_id: graphrefly_core::FnId,
) -> NodeId {
    let n = sources.len();
    // Weak-Arc captures break the BenchBinding → registry → producer_builds
    // → closure → strong-Arc<dyn ProducerBinding> cycle that would otherwise
    // pin the entire graph state when the host BenchCore drops with active
    // producer registrations. See `Core::weak_handle` doc + Slice Y close.
    let core_weak = core.weak_handle();
    let binding_weak: Weak<dyn ProducerBinding> = Arc::downgrade(binding);

    let build = Box::new(move |ctx: ProducerCtx<'_>| {
        let producer_id = ctx.node_id();
        let (Some(core_for_build), Some(binding_clone)) =
            (core_weak.upgrade(), binding_weak.upgrade())
        else {
            // Host Core / binding already dropped — no-op.
            return;
        };
        if n == 0 {
            // Empty zip emits an empty tuple immediately, then completes.
            let tuple_h = binding_clone.pack_tuple(pack_fn_id, &[]);
            core_for_build.emit(producer_id, tuple_h);
            core_for_build.complete(producer_id);
            return;
        }
        let state: Arc<Mutex<ZipState>> = Arc::new(Mutex::new(ZipState::new(n)));

        for (idx, &source) in sources.iter().enumerate() {
            let state_inner = state.clone();
            // Sinks live only while the producer is active (cleared via
            // producer_deactivate on last-subscriber unsubscribe), so they
            // can safely capture strong refs cloned from the upgraded weaks.
            let core_inner = core_for_build.clone();
            let binding_inner = binding_clone.clone();
            let sink: Sink = Arc::new(move |msgs| {
                // Phase 1 (lock held): mutate queues + collect actions.
                // Phase 2 (lock released): pack tuples + re-enter Core.
                enum PostLockAction {
                    /// Pack popped handles into a tuple, release components, emit.
                    PackAndEmit(Vec<HandleId>),
                    Complete,
                    Error(HandleId),
                }
                let mut post_actions: SmallVec<[PostLockAction; 4]> = SmallVec::new();
                // Handles to release after the lock drops (P2:
                // drain queues on terminate to avoid handle leaks).
                let mut to_release: SmallVec<[HandleId; 8]> = SmallVec::new();
                {
                    let mut s = state_inner.lock().unwrap();
                    if s.terminated {
                        return;
                    }
                    // Tier-based dispatch (canonical §4.2; see
                    // `feedback_use_tier_for_signal_routing.md`). Tier 3
                    // payload_handle.is_some() = DATA; tier 5
                    // payload_handle.is_some() = ERROR else COMPLETE.
                    for m in msgs {
                        match m.tier() {
                            3 => {
                                if let Some(h) = m.payload_handle() {
                                    binding_inner.retain_handle(h);
                                    s.queues[idx].push_back(h);
                                    // Collect complete tuples — pack_tuple runs
                                    // after the lock drops (P5).
                                    while s.queues.iter().all(|q| !q.is_empty()) {
                                        let popped: Vec<HandleId> = s
                                            .queues
                                            .iter_mut()
                                            .map(|q| q.pop_front().unwrap())
                                            .collect();
                                        post_actions.push(PostLockAction::PackAndEmit(popped));
                                    }
                                }
                                // else: Resolved on a source — no action.
                            }
                            5 => {
                                if let Some(h) = m.payload_handle() {
                                    // Error
                                    if !s.errored && !s.terminated {
                                        s.errored = true;
                                        s.terminated = true;
                                        binding_inner.retain_handle(h);
                                        // P2: release all remaining queued handles.
                                        for q in &mut s.queues {
                                            to_release.extend(q.drain(..));
                                        }
                                        post_actions.push(PostLockAction::Error(h));
                                    }
                                } else {
                                    // Complete
                                    s.completed[idx] = true;
                                    // If this source's queue is empty, no more
                                    // tuples from it — terminate.
                                    if s.queues[idx].is_empty() && !s.terminated {
                                        s.terminated = true;
                                        // P2: release all remaining queued handles.
                                        for q in &mut s.queues {
                                            to_release.extend(q.drain(..));
                                        }
                                        post_actions.push(PostLockAction::Complete);
                                    }
                                }
                            }
                            _ => {} // Tiers 0/1/2/4/6 — no action.
                        }
                    }
                }
                // Release leaked queue handles outside the lock.
                for h in to_release {
                    binding_inner.release_handle(h);
                }
                // Phase 2 (lock released): pack tuples + re-enter Core.
                // P5: pack_tuple runs outside the per-zip lock to avoid
                // deadlock if the binding's pack_tuple re-enters.
                for action in post_actions {
                    match action {
                        PostLockAction::PackAndEmit(popped) => {
                            let tuple_h = binding_inner.pack_tuple(pack_fn_id, &popped);
                            for h in &popped {
                                binding_inner.release_handle(*h);
                            }
                            core_inner.emit(producer_id, tuple_h);
                        }
                        PostLockAction::Complete => core_inner.complete(producer_id),
                        PostLockAction::Error(h) => core_inner.error(producer_id, h),
                    }
                }
            });
            ctx.subscribe_to(source, sink);
        }
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
        .expect("invariant: register_producer has no deps; no error variants reachable")
}

// =====================================================================
// concat — sequentially forward `first` then `second`
// =====================================================================

struct ConcatState {
    /// 0 = forwarding `first`; 1 = `first` complete, forwarding `second`.
    phase: u8,
    /// Buffered DATA from `second` that arrived during phase 0 (before
    /// `first` completed). Drained on phase transition.
    pending: VecDeque<HandleId>,
    /// Set to true if `second` completed during phase 0. On phase
    /// transition, after draining `pending`, concat self-completes
    /// because `second` won't emit Complete again (D041 / D-ops /qa D4).
    second_completed: bool,
    terminated: bool,
}

impl ConcatState {
    fn new() -> Self {
        Self {
            phase: 0,
            pending: VecDeque::new(),
            second_completed: false,
            terminated: false,
        }
    }
}

/// `concat(first, second)` — forward DATA from `first` until it
/// completes, then drain any DATA `second` emitted during phase 1
/// (buffered) and continue forwarding `second`. ERROR from either
/// source terminates the producer with the same ERROR.
///
/// Subscribes to BOTH sources at activation time (matches TS impl in
/// `extra/operators/combine.ts:332-379` so `second.subscribe` doesn't
/// race after `first` completes). DATA from `second` during phase 0
/// is buffered, not forwarded.
#[must_use]
pub fn concat(
    core: &Core,
    binding: &Arc<dyn ProducerBinding>,
    first: NodeId,
    second: NodeId,
) -> NodeId {
    // Weak captures break the producer-build Arc cycle (see `zip` doc).
    let core_weak = core.weak_handle();
    let binding_weak: Weak<dyn ProducerBinding> = Arc::downgrade(binding);

    let build = Box::new(move |ctx: ProducerCtx<'_>| {
        let producer_id = ctx.node_id();
        let (Some(core_clone), Some(binding_clone)) = (core_weak.upgrade(), binding_weak.upgrade())
        else {
            return;
        };
        let state: Arc<Mutex<ConcatState>> = Arc::new(Mutex::new(ConcatState::new()));

        // Subscribe to second FIRST so phase-0 DATA buffering catches
        // synchronous initial emissions. Sinks capture strong refs cloned
        // from the upgraded weaks; sink lifetime tied to producer activation.
        let state_for_second = state.clone();
        let core_for_second = core_clone.clone();
        let binding_for_second = binding_clone.clone();
        let second_sink: Sink = Arc::new(move |msgs| {
            enum Action {
                Emit(HandleId),
                Complete,
                Error(HandleId),
            }
            let mut actions: SmallVec<[Action; 4]> = SmallVec::new();
            let mut to_release: SmallVec<[HandleId; 4]> = SmallVec::new();
            {
                let mut s = state_for_second.lock().unwrap();
                if s.terminated {
                    return;
                }
                // Tier-based dispatch (canonical §4.2).
                for m in msgs {
                    match m.tier() {
                        3 => {
                            if let Some(h) = m.payload_handle() {
                                if s.phase == 0 {
                                    // Buffer for later drain.
                                    binding_for_second.retain_handle(h);
                                    s.pending.push_back(h);
                                } else {
                                    binding_for_second.retain_handle(h);
                                    actions.push(Action::Emit(h));
                                }
                            }
                            // else: Resolved on second source — no action.
                        }
                        5 => {
                            if let Some(h) = m.payload_handle() {
                                // Error
                                if !s.terminated {
                                    s.terminated = true;
                                    binding_for_second.retain_handle(h);
                                    // P2: release buffered pending handles.
                                    to_release.extend(s.pending.drain(..));
                                    actions.push(Action::Error(h));
                                }
                            } else {
                                // Complete
                                if s.phase == 1 && !s.terminated {
                                    s.terminated = true;
                                    actions.push(Action::Complete);
                                } else if s.phase == 0 {
                                    // D041 / D4 fix: remember that second
                                    // completed during phase 0. On phase
                                    // transition, after draining `pending`,
                                    // first_sink will self-complete
                                    // (second's Complete fires once and
                                    // won't be re-observed).
                                    s.second_completed = true;
                                }
                            }
                        }
                        _ => {} // Tiers 0/1/2/4/6 — no action.
                    }
                }
            }
            for h in to_release {
                binding_for_second.release_handle(h);
            }
            for action in actions {
                match action {
                    Action::Emit(h) => core_for_second.emit(producer_id, h),
                    Action::Complete => core_for_second.complete(producer_id),
                    Action::Error(h) => core_for_second.error(producer_id, h),
                }
            }
        });
        ctx.subscribe_to(second, second_sink);

        let state_for_first = state.clone();
        let core_for_first = core_clone.clone();
        let binding_for_first = binding_clone.clone();
        let first_sink: Sink = Arc::new(move |msgs| {
            // first.Complete triggers the phase transition (handled
            // via `s.phase = 1` + draining pending into `actions`),
            // and may also self-complete the producer if `second`
            // already completed during phase 0 (D041 / D4 fix).
            enum Action {
                Emit(HandleId),
                Complete,
                Error(HandleId),
            }
            let mut actions: SmallVec<[Action; 4]> = SmallVec::new();
            let mut to_release: SmallVec<[HandleId; 4]> = SmallVec::new();
            {
                let mut s = state_for_first.lock().unwrap();
                if s.terminated {
                    return;
                }
                if s.phase != 0 {
                    return; // first is done; ignore stale messages.
                }
                for m in msgs {
                    // Slice E /qa: defensive per-iteration `terminated`
                    // guard. The outer guard at the top of the lock
                    // block catches a `terminated` set BEFORE this
                    // batch arrived; this per-iteration check catches
                    // the case where an earlier message in the SAME
                    // batch transitioned us terminal (e.g., post-
                    // Complete the phase moved to 1, but a defensively-
                    // emitted stale `[Data]` later in the same batch
                    // would otherwise queue a useless retain that
                    // `core.emit` would discard on a terminal
                    // producer).
                    if s.terminated || s.phase != 0 {
                        break;
                    }
                    // Tier-based dispatch (canonical §4.2).
                    match m.tier() {
                        3 => {
                            if let Some(h) = m.payload_handle() {
                                binding_for_first.retain_handle(h);
                                actions.push(Action::Emit(h));
                            }
                            // else: Resolved on first source — no action.
                        }
                        5 => {
                            if let Some(h) = m.payload_handle() {
                                // Error
                                if !s.terminated {
                                    s.terminated = true;
                                    binding_for_first.retain_handle(h);
                                    // P2: release buffered pending handles.
                                    to_release.extend(s.pending.drain(..));
                                    actions.push(Action::Error(h));
                                }
                            } else {
                                // Complete — phase transition: drain pending
                                // second-data, then continue forwarding from
                                // second.
                                s.phase = 1;
                                // Pending handles already retained at buffer time.
                                for h in s.pending.drain(..) {
                                    actions.push(Action::Emit(h));
                                }
                                // D041 / D4 fix: if second already completed
                                // during phase 0, self-complete now (its
                                // Complete fired once and won't re-fire).
                                if s.second_completed && !s.terminated {
                                    s.terminated = true;
                                    actions.push(Action::Complete);
                                }
                            }
                        }
                        _ => {} // Tiers 0/1/2/4/6 — no action.
                    }
                }
            }
            for h in to_release {
                binding_for_first.release_handle(h);
            }
            for action in actions {
                match action {
                    Action::Emit(h) => core_for_first.emit(producer_id, h),
                    Action::Complete => core_for_first.complete(producer_id),
                    Action::Error(h) => core_for_first.error(producer_id, h),
                }
            }
        });
        ctx.subscribe_to(first, first_sink);
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
        .expect("invariant: register_producer has no deps; no error variants reachable")
}

// =====================================================================
// race — first source to emit DATA wins; losers are ignored
// =====================================================================

struct RaceState {
    /// Index of the winning source, or `None` if no winner yet.
    winner: Option<usize>,
    /// Per-source completed flags. When all complete without a winner,
    /// the producer completes (P4: no-winner all-complete termination).
    completed: Vec<bool>,
    terminated: bool,
}

impl RaceState {
    fn new(n: usize) -> Self {
        Self {
            winner: None,
            completed: vec![false; n],
            terminated: false,
        }
    }
}

/// `race(s1, s2, ..., sN)` — subscribes to all sources; the first to
/// emit DATA wins. Subsequent traffic from the winner is forwarded;
/// losers' messages are no-ops (per Q4=(b) — losers stay subscribed
/// but their sink callbacks short-circuit). Saves the dynamic
/// rewiring cost of explicitly unsubscribing losers.
///
/// Empty source list completes immediately. Single source is
/// identity-passthrough.
#[must_use]
pub fn race(core: &Core, binding: &Arc<dyn ProducerBinding>, sources: Vec<NodeId>) -> NodeId {
    let n = sources.len();
    // Weak captures break the producer-build Arc cycle (see `zip` doc).
    let core_weak = core.weak_handle();
    let binding_weak: Weak<dyn ProducerBinding> = Arc::downgrade(binding);

    let build = Box::new(move |ctx: ProducerCtx<'_>| {
        let producer_id = ctx.node_id();
        let (Some(core_clone), Some(binding_clone)) = (core_weak.upgrade(), binding_weak.upgrade())
        else {
            return;
        };
        if n == 0 {
            core_clone.complete(producer_id);
            return;
        }
        let state: Arc<Mutex<RaceState>> = Arc::new(Mutex::new(RaceState::new(n)));

        for (idx, &source) in sources.iter().enumerate() {
            let state_inner = state.clone();
            let core_inner = core_clone.clone();
            let binding_inner = binding_clone.clone();
            let sink: Sink = Arc::new(move |msgs| {
                enum Action {
                    Emit(HandleId),
                    Complete,
                    Error(HandleId),
                }
                let mut actions: SmallVec<[Action; 4]> = SmallVec::new();
                {
                    let mut s = state_inner.lock().unwrap();
                    if s.terminated {
                        return;
                    }
                    // Tier-based dispatch (canonical §4.2).
                    for m in msgs {
                        match m.tier() {
                            3 => {
                                if let Some(h) = m.payload_handle() {
                                    // P3: check s.winner live each iteration —
                                    // this source may have just become the winner
                                    // earlier in this batch.
                                    if s.winner.is_none() {
                                        s.winner = Some(idx);
                                        binding_inner.retain_handle(h);
                                        actions.push(Action::Emit(h));
                                    } else if s.winner == Some(idx) {
                                        binding_inner.retain_handle(h);
                                        actions.push(Action::Emit(h));
                                    }
                                    // else: loser DATA — ignore.
                                }
                                // else: Resolved on a source — no action.
                            }
                            5 => {
                                if let Some(h) = m.payload_handle() {
                                    // Error — from any source pre-winner OR from
                                    // the winner cascade; loser errors post-winner
                                    // are ignored.
                                    if (s.winner.is_none() || s.winner == Some(idx))
                                        && !s.terminated
                                    {
                                        s.terminated = true;
                                        binding_inner.retain_handle(h);
                                        actions.push(Action::Error(h));
                                    }
                                } else {
                                    // Complete
                                    s.completed[idx] = true;
                                    if s.winner == Some(idx) && !s.terminated {
                                        s.terminated = true;
                                        actions.push(Action::Complete);
                                    } else if s.winner.is_none()
                                        && s.completed.iter().all(|&c| c)
                                        && !s.terminated
                                    {
                                        // P4: all sources completed without a
                                        // winner — terminate the producer.
                                        s.terminated = true;
                                        actions.push(Action::Complete);
                                    }
                                    // else: loser complete — ignore.
                                }
                            }
                            _ => {} // Tiers 0/1/2/4/6 — no action.
                        }
                    }
                }
                for action in actions {
                    match action {
                        Action::Emit(h) => core_inner.emit(producer_id, h),
                        Action::Complete => core_inner.complete(producer_id),
                        Action::Error(h) => core_inner.error(producer_id, h),
                    }
                }
            });
            ctx.subscribe_to(source, sink);
        }
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
        .expect("invariant: register_producer has no deps; no error variants reachable")
}

// =====================================================================
// takeUntil — terminate when notifier emits DATA
// =====================================================================

struct TakeUntilState {
    terminated: bool,
}

impl TakeUntilState {
    fn new() -> Self {
        Self { terminated: false }
    }
}

/// `take_until(source, notifier)` — forward DATA from `source` until
/// `notifier` emits its first DATA, then terminate the producer with
/// COMPLETE. Errors from either source cascade. Source COMPLETE
/// terminates the producer.
///
/// Notifier DATA is consumed but never forwarded (zero-FFI on the
/// notifier path — we don't dereference its payload, just use the
/// emission as a signal).
#[must_use]
pub fn take_until(
    core: &Core,
    binding: &Arc<dyn ProducerBinding>,
    source: NodeId,
    notifier: NodeId,
) -> NodeId {
    // Weak captures break the producer-build Arc cycle (see `zip` doc).
    let core_weak = core.weak_handle();
    let binding_weak: Weak<dyn ProducerBinding> = Arc::downgrade(binding);

    let build = Box::new(move |ctx: ProducerCtx<'_>| {
        let producer_id = ctx.node_id();
        let (Some(core_clone), Some(binding_clone)) = (core_weak.upgrade(), binding_weak.upgrade())
        else {
            return;
        };
        let state: Arc<Mutex<TakeUntilState>> = Arc::new(Mutex::new(TakeUntilState::new()));

        // Source sink: forward DATA, propagate terminals.
        let state_for_source = state.clone();
        let core_for_source = core_clone.clone();
        let binding_for_source = binding_clone.clone();
        let source_sink: Sink = Arc::new(move |msgs| {
            enum Action {
                Emit(HandleId),
                Complete,
                Error(HandleId),
            }
            let mut actions: SmallVec<[Action; 4]> = SmallVec::new();
            {
                let mut s = state_for_source.lock().unwrap();
                if s.terminated {
                    return;
                }
                // Tier-based dispatch (canonical §4.2).
                for m in msgs {
                    match m.tier() {
                        3 => {
                            if let Some(h) = m.payload_handle() {
                                binding_for_source.retain_handle(h);
                                actions.push(Action::Emit(h));
                            }
                            // else: Resolved on source — no action.
                        }
                        5 => {
                            if let Some(h) = m.payload_handle() {
                                // Error
                                if !s.terminated {
                                    s.terminated = true;
                                    binding_for_source.retain_handle(h);
                                    actions.push(Action::Error(h));
                                }
                            } else {
                                // Complete
                                if !s.terminated {
                                    s.terminated = true;
                                    actions.push(Action::Complete);
                                }
                            }
                        }
                        _ => {} // Tiers 0/1/2/4/6 — no action.
                    }
                }
            }
            for action in actions {
                match action {
                    Action::Emit(h) => core_for_source.emit(producer_id, h),
                    Action::Complete => core_for_source.complete(producer_id),
                    Action::Error(h) => core_for_source.error(producer_id, h),
                }
            }
        });
        ctx.subscribe_to(source, source_sink);

        // Notifier sink: any DATA → terminate; ERROR → cascade.
        let state_for_notifier = state.clone();
        let core_for_notifier = core_clone.clone();
        let binding_for_notifier = binding_clone.clone();
        let notifier_sink: Sink = Arc::new(move |msgs| {
            enum Action {
                Complete,
                Error(HandleId),
            }
            let mut action: Option<Action> = None;
            {
                let mut s = state_for_notifier.lock().unwrap();
                if s.terminated {
                    return;
                }
                // Tier-based dispatch (canonical §4.2).
                for m in msgs {
                    match m.tier() {
                        3 => {
                            // Any DATA on notifier → complete the producer.
                            // (Resolved alone — no payload — no action.)
                            // We don't emit notifier DATA downstream, so we
                            // don't even need to extract the handle.
                            if m.payload_handle().is_some() && !s.terminated {
                                s.terminated = true;
                                action = Some(Action::Complete);
                                break;
                            }
                        }
                        5 => {
                            // Error: cascade. Complete (payload_handle.is_none())
                            // without prior DATA: nothing to do — source
                            // continues independently.
                            if let Some(h) = m.payload_handle() {
                                if !s.terminated {
                                    s.terminated = true;
                                    binding_for_notifier.retain_handle(h);
                                    action = Some(Action::Error(h));
                                    break;
                                }
                            }
                        }
                        _ => {} // Tiers 0/1/2/4/6 — no action.
                    }
                }
            }
            if let Some(a) = action {
                match a {
                    Action::Complete => core_for_notifier.complete(producer_id),
                    Action::Error(h) => core_for_notifier.error(producer_id, h),
                }
            }
        });
        ctx.subscribe_to(notifier, notifier_sink);
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
        .expect("invariant: register_producer has no deps; no error variants reachable")
}
