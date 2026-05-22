//! Higher-order operators (Slice E, D044) — operators whose project fn
//! returns an inner [`NodeId`] for each outer DATA. Mirrors TS legacy
//! `extra/operators/higher-order.ts` (`switchMap` / `exhaustMap` /
//! `concatMap` / `mergeMap`).
//!
//! All four are producer-pattern nodes (no declared deps; subscribe to
//! the outer source from inside the build closure; emit on themselves
//! via [`Core::emit`]). This mirrors the [`super::ops_impl`] family
//! (zip / concat / race / takeUntil); the producer substrate handles
//! auto-cleanup of upstream + inner subscriptions on producer
//! deactivation (D031–D038).
//!
//! The four flavors differ in how they handle a new outer DATA while a
//! prior inner is still active:
//!
//! - [`switch_map`] — cancel the prior inner (Rx-style `switchMap`).
//! - [`exhaust_map`] — drop the new value (Rx-style `exhaustMap`).
//! - [`concat_map`] — enqueue; process sequentially. (Equivalent to
//!   [`merge_map_with_concurrency`] with `Some(1)`.)
//! - [`merge_map`] / [`merge_map_with_concurrency`] — spawn in parallel
//!   up to `concurrency`. `None` = unbounded.
//!
//! # Inner-sub tracking (Slice E /qa refactor)
//!
//! Each operator owns its inner [`Subscription`]s **inside its state
//! `Mutex`** (not in [`super::producer::ProducerNodeState::subs`]).
//! `producer_storage[producer_id].subs` holds only the OUTER source
//! subscription (one entry, no positional concerns). switch_map /
//! exhaust_map keep `Option<Subscription>` (single active inner); merge
//! / concat keep `HashMap<u64, Subscription>` keyed by per-op
//! `next_inner_id`. This avoids two bugs the original positional design
//! exposed: (a) cached-outer source firing handshake before
//! `subscribe_to` pushed the outer sub, reordering `subs[0]`; (b)
//! merge/concat completed-inner subs accumulating in `subs` indefinitely.
//! Inner sub cleanup is per-op now: switch/exhaust take + drop on inner
//! Complete; merge/concat remove specific id on inner Complete.
//!
//! # Drain discipline (iterative spawn)
//!
//! `merge_map` could spawn the next buffered DATA from inside an
//! inner's `on_complete` callback. For pathological pre-completed
//! inners (synchronous Complete during the subscribe handshake),
//! recursive spawn would grow the stack proportionally to the buffer
//! depth. The thread-local [`MERGE_DRAIN_ACTIVE`] flag breaks the
//! recursion: the outermost drain owns the loop; nested `on_complete`
//! invocations only decrement state and return.
//!
//! # Project closure (D044)
//!
//! Each operator takes a `project: Fn(HandleId) -> NodeId` closure
//! registered through [`HigherOrderBinding::register_project`]. Bindings
//! (napi-rs / pyo3 / wasm-bindgen) marshal user-supplied JS / Python /
//! WASM callbacks into this Rust shape; Rust-side users register a
//! closure directly.

#![allow(clippy::collapsible_if, clippy::collapsible_match)]
#![allow(clippy::too_many_arguments, clippy::too_many_lines)]

use std::cell::Cell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::{Arc, Mutex, Weak};

use ahash::AHashMap;
use graphrefly_core::{Core, FnId, HandleId, Message, NodeId, Sink};
use smallvec::SmallVec;

use super::producer::{ProducerBinding, ProducerCtx, ProducerEmitter, SubGuard};

// =====================================================================
// HigherOrderBinding — closure registration for project: T -> Node<R>
// =====================================================================

/// Project closure: takes an outer DATA handle, returns the
/// [`NodeId`] of an inner node to subscribe to. Closure may register
/// new state/derived nodes on the fly via captured [`Core`].
pub type ProjectFn = Box<dyn Fn(HandleId) -> NodeId + Send + Sync>;

/// Closure-registration interface for higher-order operators.
///
/// Extends [`ProducerBinding`] with one method that bindings shipping
/// higher-order operators must implement.
pub trait HigherOrderBinding: ProducerBinding {
    /// Register a project closure. The returned [`FnId`] is captured by
    /// the operator's build closure and looked up via
    /// [`Self::invoke_project`] on each outer DATA fire.
    fn register_project(&self, project: ProjectFn) -> FnId;

    /// Invoke a registered project closure with the given outer DATA
    /// handle. Returns the inner node's [`NodeId`].
    ///
    /// # Panics
    ///
    /// Implementations panic if `fn_id` is not a registered project
    /// closure.
    fn invoke_project(&self, fn_id: FnId, value: HandleId) -> NodeId;
}

// =====================================================================
// build_inner_sink — shared inner-subscription handler
// =====================================================================
//
// Each higher-order op subscribes to the inner node returned by its
// project closure. The inner sink dispatches by `Message::tier()`
// (R1.3.7.b — central message-tier utility, never hardcode variant
// checks for forwarding gating per CLAUDE.md design invariant 4):
//
// - Tier 0 (Start) — drop.
// - Tier 1 (Dirty) — drop. (DIRTY forwarding from inner is an
//   acknowledged divergence from TS legacy; Rust producer relies on
//   Core's wave engine to generate DIRTY on the producer's own emits.)
// - Tier 2 (Pause/Resume) — drop. Backpressure is per-stream; not
//   propagated through higher-order ops by design.
// - Tier 3 (Data/Resolved) — `Data(h)` forwards via `Core::emit(producer_id, h)`;
//   `Resolved` drops (same wave-engine rationale as Dirty).
// - Tier 4 (Invalidate) — forwards via `Core::invalidate(producer_id)`
//   per R1.2.7. Inner cache invalidation surfaces to producer
//   subscribers.
// - Tier 5 (Complete/Error) — semantic dispatch via op-specific
//   callbacks (`on_inner_complete` / `on_inner_error`). Differs
//   per-op (switch_map clears active inner; merge_map decrements
//   active count; etc.).
// - Tier 6 (Teardown) — forwards via `Core::teardown(producer_id)`
//   per R2.6.4. Inner permanent destruction propagates.

// S2b/D230/D234: long-lived inner sink takes `em: ProducerEmitter`
// (was `core: Core`, no longer `Clone`). Emit is the mailbox fast path;
// the rare INVALIDATE/TEARDOWN terminal forwards route through
// `em.defer` (`CoreFull`).
fn build_inner_sink(
    em: ProducerEmitter,
    producer_binding: Arc<dyn ProducerBinding>,
    producer_id: NodeId,
    on_inner_complete: Rc<dyn Fn()>,
    on_inner_error: Rc<dyn Fn(HandleId)>,
) -> Sink {
    Rc::new(move |msgs: &[Message]| {
        enum Action {
            Emit(HandleId),
            Complete,
            Error(HandleId),
            Invalidate,
            Teardown,
        }
        let mut actions: SmallVec<[Action; 4]> = SmallVec::new();
        for m in msgs {
            match m.tier() {
                3 => {
                    // Tier 3: Data/Resolved. Only Data carries a payload
                    // to forward; Resolved is dropped per the
                    // wave-engine divergence above.
                    if let Some(h) = m.payload_handle() {
                        producer_binding.retain_handle(h);
                        actions.push(Action::Emit(h));
                    }
                }
                4 => {
                    // Tier 4: Invalidate. Forward to producer.
                    actions.push(Action::Invalidate);
                }
                5 => {
                    // Tier 5: Complete or Error. Semantic dispatch via
                    // op-specific callbacks. `payload_handle()` is the
                    // canonical Error-vs-Complete discriminator at this
                    // tier.
                    if let Some(h) = m.payload_handle() {
                        producer_binding.retain_handle(h);
                        actions.push(Action::Error(h));
                    } else {
                        actions.push(Action::Complete);
                    }
                }
                6 => {
                    // Tier 6: Teardown. Forward to producer.
                    actions.push(Action::Teardown);
                }
                // Tiers 0 (Start), 1 (Dirty), 2 (Pause/Resume): drop.
                _ => {}
            }
        }
        for action in actions {
            match action {
                Action::Emit(h) => em.emit_or_defer(producer_id, h),
                Action::Complete => on_inner_complete(),
                Action::Error(h) => on_inner_error(h),
                Action::Invalidate => {
                    let _ = em.defer(move |c| c.invalidate(producer_id));
                }
                Action::Teardown => {
                    let _ = em.defer(move |c| c.teardown(producer_id));
                }
            }
        }
    })
}

// =====================================================================
// switch_map — cancel previous inner on each new outer DATA
// =====================================================================

struct SwitchState {
    /// Currently-active inner subscription (if any). Cancelling the
    /// prior inner is `Option::take` + drop — S2b: `SubGuard::Drop`
    /// posts the `unsubscribe` via `em.defer` (owner-side, in-wave,
    /// FIFO-ordered so cancel drains before any resubscribe — D234).
    inner_sub: Option<SubGuard>,
    /// QA P2 (2026-05-18): monotonic switch epoch. Under D234 the
    /// cancel-prev is `inner_sub.take()` at outer-fire, but the prior
    /// subscribe is a still-queued `em.defer` that hasn't populated
    /// `inner_sub` yet — so a fast double-switch within one wave would
    /// `take()` an empty slot and leave BOTH inners live. Each accepted
    /// outer DATA bumps `epoch`; the subscribe-defer captures the value
    /// and, after `try_subscribe`, drops the new `SubGuard` immediately
    /// (deferred unsubscribe) instead of installing it if `epoch` has
    /// moved on — i.e. a newer outer DATA superseded it.
    epoch: u64,
    source_done: bool,
    terminated: bool,
}

impl SwitchState {
    fn new() -> Self {
        Self {
            inner_sub: None,
            epoch: 0,
            source_done: false,
            terminated: false,
        }
    }
}

/// `switch_map(source, project)` — for each outer DATA, cancel the
/// previous inner subscription and subscribe to the inner node returned
/// by `project(value)`. Inner DATA flows through to downstream; inner
/// COMPLETE clears the active slot; outer COMPLETE (with no active
/// inner) self-completes the operator.
#[must_use]
pub fn switch_map(
    core: &Core,
    binding: &Arc<dyn HigherOrderBinding>,
    source: NodeId,
    project: ProjectFn,
) -> NodeId {
    let project_fn_id = binding.register_project(project);
    // S2b/D223: the Core weak is GONE (Core relocates; long-lived sinks
    // reach it via `ProducerEmitter`/`em.defer`). The *binding* weaks
    // stay — they break the registry→build-closure→strong-binding cycle
    // (unrelated to Core ownership; D223 only forbids `Weak<C>`).
    let binding_weak: Weak<dyn HigherOrderBinding> = Arc::downgrade(binding);
    let producer_binding_weak: Weak<dyn ProducerBinding> =
        Arc::downgrade(&(binding.clone() as Arc<dyn ProducerBinding>));

    let build = Box::new(move |ctx: ProducerCtx<'_>| {
        let producer_id = ctx.node_id();
        let (Some(binding_clone), Some(producer_binding)) =
            (binding_weak.upgrade(), producer_binding_weak.upgrade())
        else {
            return;
        };
        let em = ctx.emitter();
        // D273 follow-on: Arc<Mutex<!Send>> → Rc<RefCell> in Family-2 sweep.
        #[allow(clippy::arc_with_non_send_sync)]
        let state: Arc<Mutex<SwitchState>> = Arc::new(Mutex::new(SwitchState::new()));

        let state_for_outer = state.clone();
        let em_for_outer = em.clone();
        let binding_for_outer = binding_clone.clone();
        let producer_binding_for_outer = producer_binding.clone();

        let outer_sink: Sink = Rc::new(move |msgs| {
            // Phase 1: classify under state lock. Track whether we
            // performed a retain so phase 2 can safely release it
            // without underflow on `[Data(_), Error(_)]` same-batch
            // (P1 /qa fix).
            #[derive(Default)]
            struct Plan {
                latest_outer_h: Option<HandleId>,
                latest_retained: bool,
                self_complete: bool,
                self_error: Option<HandleId>,
            }
            let mut plan = Plan::default();
            {
                let mut s = state_for_outer.lock().unwrap();
                if s.terminated {
                    return;
                }
                // Tier-based dispatch per `feedback_use_tier_for_signal_routing.md`
                // and canonical §4.2 ("Always use the provided tier utilities
                // rather than hardcoding type checks"). Tier 3 carries
                // DATA/RESOLVED — only DATA has a payload_handle, so the
                // `payload_handle().is_some()` test discriminates within
                // the tier without referring to specific variants. Tier 5
                // carries COMPLETE/ERROR — same shape (Error has handle,
                // Complete doesn't).
                for m in msgs {
                    match m.tier() {
                        3 => {
                            if let Some(h) = m.payload_handle() {
                                // switchMap: only the LATEST outer DATA in the
                                // batch matters (TS legacy "skip to last in
                                // the batch to avoid creating + immediately
                                // discarding N-1 inners"). Track the handle;
                                // we'll project + subscribe once after the
                                // lock drops. Skipped handles need no retain.
                                plan.latest_outer_h = Some(h);
                            }
                            // else: Resolved on outer source — no action.
                        }
                        5 => {
                            if let Some(h) = m.payload_handle() {
                                // Error
                                if !s.terminated {
                                    s.terminated = true;
                                    binding_for_outer.retain_handle(h);
                                    plan.self_error = Some(h);
                                }
                            } else {
                                // Complete
                                s.source_done = true;
                                if s.inner_sub.is_none()
                                    && plan.latest_outer_h.is_none()
                                    && !s.terminated
                                {
                                    s.terminated = true;
                                    plan.self_complete = true;
                                }
                            }
                        }
                        _ => {} // Tiers 0/1/2/4/6 — no action on outer source.
                    }
                }
                if let Some(h) = plan.latest_outer_h {
                    if !s.terminated {
                        // Retain ONE share for the chosen handle —
                        // released by phase 2 after invoke_project.
                        binding_for_outer.retain_handle(h);
                        plan.latest_retained = true;
                    }
                }
            }

            // Phase 2: cancel prior inner sub, project, subscribe.
            // Gated on `latest_retained` so that an `Error` arriving in
            // the same batch (which sets terminated and skips the
            // retain) does NOT trigger an unbalanced release.
            if plan.latest_retained {
                let outer_h = plan
                    .latest_outer_h
                    .expect("latest_retained implies latest_outer_h is Some");

                // Cancel prior inner sub (if any) + bump the switch
                // epoch (QA P2). `take()` only catches an *already
                // installed* prior inner; a prior subscribe still queued
                // as an `em.defer` hasn't populated `inner_sub` yet, so
                // bumping `epoch` here and re-checking it inside the
                // subscribe-defer is what actually invalidates a
                // superseded-but-still-queued prior subscribe.
                let my_epoch = {
                    let mut s = state_for_outer.lock().unwrap();
                    let prev = s.inner_sub.take();
                    s.epoch += 1;
                    let e = s.epoch;
                    drop(s);
                    drop(prev); // SubGuard::Drop → deferred unsubscribe.
                    e
                };

                // invoke_project lock-released (binding call, not Core).
                let inner_node = binding_for_outer.invoke_project(project_fn_id, outer_h);
                binding_for_outer.release_handle(outer_h);

                let on_complete = make_switch_on_complete(
                    state_for_outer.clone(),
                    em_for_outer.clone(),
                    producer_id,
                );
                let on_error = make_switch_on_error(
                    state_for_outer.clone(),
                    em_for_outer.clone(),
                    producer_id,
                );
                // F2 /qa: TornDown synthesizes inner-Complete so the
                // self-Complete trigger can fire (batched
                // [Data,Complete]→dead-inner wedge fix).
                let on_complete_for_dead = on_complete.clone();
                let inner_sink = build_inner_sink(
                    em_for_outer.clone(),
                    producer_binding_for_outer.clone(),
                    producer_id,
                    on_complete,
                    on_error,
                );
                // D234: the inner subscribe needs `CoreFull` from a
                // long-lived sink → `em.defer` (owner-side, in-wave,
                // FIFO after the cancel above). The returned
                // `SubscriptionId` is wrapped in a `SubGuard` (its Drop
                // schedules the eventual unsubscribe). QA P2: the
                // captured `my_epoch` invalidates this subscribe if a
                // newer outer DATA superseded it while it was queued.
                let state_sub = state_for_outer.clone();
                let em_guard = em_for_outer.clone();
                let _ = em_for_outer.defer(move |c| {
                    match c.try_subscribe(inner_node, inner_sink) {
                        Ok(sub) => {
                            let guard = SubGuard::new(inner_node, sub, em_guard);
                            let to_drop = {
                                let mut s = state_sub.lock().unwrap();
                                if s.terminated || s.epoch != my_epoch {
                                    Some(guard)
                                } else {
                                    s.inner_sub.replace(guard)
                                }
                            };
                            drop(to_drop);
                        }
                        Err(graphrefly_core::SubscribeError::TornDown { .. }) => {
                            // R2.2.7.b: inner dead — synthesize
                            // inner-Complete (clears inner_sub, checks
                            // self-Complete trigger).
                            on_complete_for_dead();
                        }
                        Err(graphrefly_core::SubscribeError::PartitionOrderViolation(_)) => {
                            // Already inside the in-wave drain (no
                            // partitions held the old way) — a violation
                            // here is the substrate-invariant break the
                            // old wave-end-drain guard caught.
                            panic!(
                                "switch_map inner subscribe: partition-order \
                                 violation inside em.defer — substrate invariant broken"
                            );
                        }
                    }
                });
            }

            if plan.self_complete {
                em_for_outer.complete_or_defer(producer_id);
            } else if let Some(h) = plan.self_error {
                em_for_outer.error_or_defer(producer_id, h);
            }
        });

        ctx.subscribe_to(source, outer_sink);
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
        .expect("invariant: register_producer has no deps; no error variants reachable")
}

fn make_switch_on_complete(
    state: Arc<Mutex<SwitchState>>,
    em: ProducerEmitter,
    producer_id: NodeId,
) -> Rc<dyn Fn()> {
    Rc::new(move || {
        let prev_inner;
        let mut should_complete = false;
        {
            let mut s = state.lock().unwrap();
            if s.terminated {
                return;
            }
            prev_inner = s.inner_sub.take();
            if s.source_done && !s.terminated {
                s.terminated = true;
                should_complete = true;
            }
        }
        drop(prev_inner); // SubGuard::Drop → deferred unsubscribe.
        if should_complete {
            em.complete_or_defer(producer_id);
        }
    })
}

fn make_switch_on_error(
    state: Arc<Mutex<SwitchState>>,
    em: ProducerEmitter,
    producer_id: NodeId,
) -> Rc<dyn Fn(HandleId)> {
    Rc::new(move |h| {
        let prev_inner;
        {
            let mut s = state.lock().unwrap();
            if s.terminated {
                return;
            }
            s.terminated = true;
            prev_inner = s.inner_sub.take();
        }
        drop(prev_inner);
        em.error_or_defer(producer_id, h);
    })
}

// =====================================================================
// exhaust_map — ignore outer DATA while inner is active
// =====================================================================

struct ExhaustState {
    /// Active inner sub. S2b: `Option<SubGuard>` — drop posts the
    /// deferred unsubscribe (D234).
    inner_sub: Option<SubGuard>,
    /// QA P2 (2026-05-18): exhaust is *older-wins* — while a projected
    /// inner is pending OR active, new outer DATA is dropped. Under
    /// D234 the subscribe is a queued `em.defer`, so `inner_sub` is
    /// `None` between accept and install; without this flag a 2nd
    /// outer DATA in that window would also pass `inner_sub.is_none()`
    /// and project a 2nd inner (exhaust contract violation). Set true
    /// when an outer DATA is accepted + its subscribe queued; cleared
    /// when the inner completes/errors or its subscribe finds the
    /// source dead.
    pending: bool,
    source_done: bool,
    terminated: bool,
}

impl ExhaustState {
    fn new() -> Self {
        Self {
            inner_sub: None,
            pending: false,
            source_done: false,
            terminated: false,
        }
    }
}

/// `exhaust_map(source, project)` — like [`switch_map`] but DROPS new
/// outer DATA while an inner subscription is active. First outer DATA
/// per "active window" wins; subsequent DATAs are discarded until the
/// inner completes.
#[must_use]
pub fn exhaust_map(
    core: &Core,
    binding: &Arc<dyn HigherOrderBinding>,
    source: NodeId,
    project: ProjectFn,
) -> NodeId {
    let project_fn_id = binding.register_project(project);
    // S2b/D223: binding weaks stay (registry-cycle break); Core weak
    // gone — sinks reach Core via `em`/`em.defer`.
    let binding_weak: Weak<dyn HigherOrderBinding> = Arc::downgrade(binding);
    let producer_binding_weak: Weak<dyn ProducerBinding> =
        Arc::downgrade(&(binding.clone() as Arc<dyn ProducerBinding>));

    let build = Box::new(move |ctx: ProducerCtx<'_>| {
        let producer_id = ctx.node_id();
        let (Some(binding_clone), Some(producer_binding)) =
            (binding_weak.upgrade(), producer_binding_weak.upgrade())
        else {
            return;
        };
        let em = ctx.emitter();
        // D273 follow-on: Arc<Mutex<!Send>> → Rc<RefCell> in Family-2 sweep.
        #[allow(clippy::arc_with_non_send_sync)]
        let state: Arc<Mutex<ExhaustState>> = Arc::new(Mutex::new(ExhaustState::new()));

        let state_for_outer = state.clone();
        let em_for_outer = em.clone();
        let binding_for_outer = binding_clone.clone();
        let producer_binding_for_outer = producer_binding.clone();

        let outer_sink: Sink = Rc::new(move |msgs| {
            #[derive(Default)]
            struct Plan {
                first_outer_h: Option<HandleId>,
                first_retained: bool,
                self_complete: bool,
                self_error: Option<HandleId>,
            }
            let mut plan = Plan::default();
            {
                let mut s = state_for_outer.lock().unwrap();
                if s.terminated {
                    return;
                }
                // Tier-based dispatch (canonical §4.2; see
                // `feedback_use_tier_for_signal_routing.md`).
                for m in msgs {
                    match m.tier() {
                        3 => {
                            if let Some(h) = m.payload_handle() {
                                // First DATA per active window wins.
                                // Remember the first one we accept; subsequent
                                // batch entries (or DATAs after) drop.
                                // QA P2: also gate on `!s.pending` — a
                                // prior accepted DATA's subscribe may be
                                // queued (inner_sub still None); exhaust
                                // must drop this one (older-wins).
                                if s.inner_sub.is_none()
                                    && !s.pending
                                    && plan.first_outer_h.is_none()
                                {
                                    binding_for_outer.retain_handle(h);
                                    plan.first_outer_h = Some(h);
                                    plan.first_retained = true;
                                    s.pending = true;
                                }
                            }
                            // else: Resolved on outer source — no action.
                        }
                        5 => {
                            if let Some(h) = m.payload_handle() {
                                // Error
                                if !s.terminated {
                                    s.terminated = true;
                                    // Release any retain we took for
                                    // first_outer_h — we won't be projecting it.
                                    if plan.first_retained {
                                        if let Some(h0) = plan.first_outer_h.take() {
                                            binding_for_outer.release_handle(h0);
                                            plan.first_retained = false;
                                        }
                                    }
                                    binding_for_outer.retain_handle(h);
                                    plan.self_error = Some(h);
                                }
                            } else {
                                // Complete
                                s.source_done = true;
                                if s.inner_sub.is_none()
                                    && plan.first_outer_h.is_none()
                                    && !s.terminated
                                {
                                    s.terminated = true;
                                    plan.self_complete = true;
                                }
                            }
                        }
                        _ => {} // Tiers 0/1/2/4/6 — no action.
                    }
                }
            }

            if plan.first_retained {
                let outer_h = plan
                    .first_outer_h
                    .expect("first_retained implies first_outer_h is Some");
                let inner_node = binding_for_outer.invoke_project(project_fn_id, outer_h);
                binding_for_outer.release_handle(outer_h);

                let on_complete = make_exhaust_on_complete(
                    state_for_outer.clone(),
                    em_for_outer.clone(),
                    producer_id,
                );
                let on_error = make_exhaust_on_error(
                    state_for_outer.clone(),
                    em_for_outer.clone(),
                    producer_id,
                );
                // F2 /qa: clone for TornDown synthesis (mirrors switch_map).
                let on_complete_for_dead = on_complete.clone();
                let inner_sink = build_inner_sink(
                    em_for_outer.clone(),
                    producer_binding_for_outer.clone(),
                    producer_id,
                    on_complete,
                    on_error,
                );
                // D234: inner subscribe via `em.defer` (long-lived sink
                // can't hold `&Core`). TornDown → synthesize
                // inner-Complete so `inner_sub` clears and the next
                // outer DATA can re-project (F2 /qa).
                let state_sub = state_for_outer.clone();
                let em_guard = em_for_outer.clone();
                let _ =
                    em_for_outer.defer(move |c| match c.try_subscribe(inner_node, inner_sink) {
                        Ok(sub) => {
                            let guard = SubGuard::new(inner_node, sub, em_guard);
                            let to_drop = {
                                let mut s = state_sub.lock().unwrap();
                                if s.terminated {
                                    Some(guard)
                                } else {
                                    s.inner_sub.replace(guard)
                                }
                            };
                            drop(to_drop);
                        }
                        Err(graphrefly_core::SubscribeError::TornDown { .. }) => {
                            on_complete_for_dead();
                        }
                        Err(graphrefly_core::SubscribeError::PartitionOrderViolation(_)) => {
                            panic!(
                                "exhaust_map inner subscribe: partition-order \
                                 violation inside em.defer — substrate invariant broken"
                            );
                        }
                    });
            }

            if plan.self_complete {
                em_for_outer.complete_or_defer(producer_id);
            } else if let Some(h) = plan.self_error {
                em_for_outer.error_or_defer(producer_id, h);
            }
        });

        ctx.subscribe_to(source, outer_sink);
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
        .expect("invariant: register_producer has no deps; no error variants reachable")
}

fn make_exhaust_on_complete(
    state: Arc<Mutex<ExhaustState>>,
    em: ProducerEmitter,
    producer_id: NodeId,
) -> Rc<dyn Fn()> {
    Rc::new(move || {
        let prev_inner;
        let mut should_complete = false;
        {
            let mut s = state.lock().unwrap();
            if s.terminated {
                return;
            }
            prev_inner = s.inner_sub.take();
            // QA P2: inner finished (or was dead) — exhaust window
            // re-opens; the next outer DATA may project again.
            s.pending = false;
            if s.source_done && !s.terminated {
                s.terminated = true;
                should_complete = true;
            }
        }
        drop(prev_inner);
        if should_complete {
            em.complete_or_defer(producer_id);
        }
    })
}

fn make_exhaust_on_error(
    state: Arc<Mutex<ExhaustState>>,
    em: ProducerEmitter,
    producer_id: NodeId,
) -> Rc<dyn Fn(HandleId)> {
    Rc::new(move |h| {
        let prev_inner;
        {
            let mut s = state.lock().unwrap();
            if s.terminated {
                return;
            }
            s.terminated = true;
            prev_inner = s.inner_sub.take();
        }
        drop(prev_inner);
        em.error_or_defer(producer_id, h);
    })
}

// =====================================================================
// merge_map — parallel inners up to `concurrency` cap
// concat_map — wrapper for concurrency = Some(1)
// =====================================================================

thread_local! {
    /// Per-thread guard preventing recursive drain of `MergeMapState`.
    /// When an `on_complete` fires synchronously inside a
    /// `Core::subscribe` handshake (pre-completed inner), it must not
    /// re-enter the drain loop — instead it just decrements + removes
    /// its sub and returns. The outermost drain owns the loop and
    /// observes the freed-up cap on its next iteration.
    static MERGE_DRAIN_ACTIVE: Cell<bool> = const { Cell::new(false) };
}

struct MergeMapState {
    /// Number of currently-active inner subscriptions (spawned but
    /// not yet completed/errored).
    active: u32,
    /// Outer DATAs waiting because `active >= concurrency`. Each
    /// handle has one retain share (taken on enqueue, released on
    /// dequeue + project).
    buffer: VecDeque<HandleId>,
    /// Per-inner `Subscription`s, keyed by `next_inner_id`. Each
    /// inner's `on_complete` removes its entry by id (lock-released
    /// drop).
    inner_subs: AHashMap<u64, SubGuard>,
    /// Pending inner ids (between `subscribe` call and
    /// `inner_subs.insert`). Used to detect synchronous-completion:
    /// if `on_complete` runs during `subscribe`, it removes from
    /// `pending_inner_ids`; the post-subscribe code checks the set
    /// and skips inserting the now-dead sub.
    pending_inner_ids: ahash::AHashSet<u64>,
    next_inner_id: u64,
    source_done: bool,
    terminated: bool,
}

impl MergeMapState {
    fn new() -> Self {
        Self {
            active: 0,
            buffer: VecDeque::new(),
            inner_subs: AHashMap::new(),
            pending_inner_ids: ahash::AHashSet::new(),
            next_inner_id: 0,
            source_done: false,
            terminated: false,
        }
    }
}

/// `merge_map(source, project)` — unbounded concurrency variant.
/// Equivalent to [`merge_map_with_concurrency`] with `None`.
#[must_use]
pub fn merge_map(
    core: &Core,
    binding: &Arc<dyn HigherOrderBinding>,
    source: NodeId,
    project: ProjectFn,
) -> NodeId {
    merge_map_with_concurrency(core, binding, source, project, None)
}

/// `concat_map(source, project)` — sequential queue variant.
/// Equivalent to [`merge_map_with_concurrency`] with `Some(1)`. Each
/// outer DATA is enqueued and processed one-at-a-time.
#[must_use]
pub fn concat_map(
    core: &Core,
    binding: &Arc<dyn HigherOrderBinding>,
    source: NodeId,
    project: ProjectFn,
) -> NodeId {
    merge_map_with_concurrency(core, binding, source, project, Some(1))
}

/// `merge_map_with_concurrency(source, project, concurrency)` — projects
/// each outer DATA to an inner Node and subscribes in parallel.
///
/// `concurrency`:
/// - `None` → unbounded (every outer DATA spawns immediately).
/// - `Some(n)` → at most `n` concurrent inners; excess outer DATAs
///   buffer until an active inner completes.
///
/// Per D043 / D040, this matches the
/// [`Core::set_pause_buffer_cap`](graphrefly_core::Core::set_pause_buffer_cap)
/// `Option<usize>` precedent (None = unbounded). `Some(0)` is degenerate
/// (would buffer everything indefinitely without ever spawning) but
/// accepted at the type level.
#[must_use]
pub fn merge_map_with_concurrency(
    core: &Core,
    binding: &Arc<dyn HigherOrderBinding>,
    source: NodeId,
    project: ProjectFn,
    concurrency: Option<u32>,
) -> NodeId {
    let project_fn_id = binding.register_project(project);
    // S2b/D223: binding weaks stay (registry-cycle break); Core weak
    // gone — sinks reach Core via `em`/`em.defer`.
    let binding_weak: Weak<dyn HigherOrderBinding> = Arc::downgrade(binding);
    let producer_binding_weak: Weak<dyn ProducerBinding> =
        Arc::downgrade(&(binding.clone() as Arc<dyn ProducerBinding>));

    let build = Box::new(move |ctx: ProducerCtx<'_>| {
        let producer_id = ctx.node_id();
        let (Some(binding_clone), Some(producer_binding)) =
            (binding_weak.upgrade(), producer_binding_weak.upgrade())
        else {
            return;
        };
        let em = ctx.emitter();
        // D273 follow-on: Arc<Mutex<!Send>> → Rc<RefCell> in Family-2 sweep.
        #[allow(clippy::arc_with_non_send_sync)]
        let state: Arc<Mutex<MergeMapState>> = Arc::new(Mutex::new(MergeMapState::new()));

        let state_for_outer = state.clone();
        let em_for_outer = em.clone();
        let binding_for_outer = binding_clone.clone();
        let producer_binding_for_outer = producer_binding.clone();

        let outer_sink: Sink = Rc::new(move |msgs| {
            // Phase 1: enqueue DATAs into the buffer (always — drain
            // loop dequeues + spawns up to cap), classify terminal
            // signals.
            let mut error_action: Option<HandleId> = None;
            let mut self_complete_now = false;
            {
                let mut s = state_for_outer.lock().unwrap();
                if s.terminated {
                    return;
                }
                // Tier-based dispatch (canonical §4.2; see
                // `feedback_use_tier_for_signal_routing.md`).
                for m in msgs {
                    match m.tier() {
                        3 => {
                            if let Some(h) = m.payload_handle() {
                                // Retain on enqueue — released by drain
                                // after invoke_project.
                                binding_for_outer.retain_handle(h);
                                s.buffer.push_back(h);
                            }
                            // else: Resolved on outer source — no action.
                        }
                        5 => {
                            if let Some(h) = m.payload_handle() {
                                // Error
                                if !s.terminated {
                                    s.terminated = true;
                                    binding_for_outer.retain_handle(h);
                                    while let Some(q) = s.buffer.pop_front() {
                                        binding_for_outer.release_handle(q);
                                    }
                                    error_action = Some(h);
                                }
                            } else {
                                // Complete
                                s.source_done = true;
                                if s.active == 0 && s.buffer.is_empty() && !s.terminated {
                                    s.terminated = true;
                                    self_complete_now = true;
                                }
                            }
                        }
                        _ => {} // Tiers 0/1/2/4/6 — no action.
                    }
                }
            }

            if let Some(h) = error_action {
                em_for_outer.error_or_defer(producer_id, h);
                return;
            }
            if self_complete_now {
                em_for_outer.complete_or_defer(producer_id);
                return;
            }

            // Phase 2: drain buffer iteratively up to concurrency cap.
            drain_merge_buffer(
                &state_for_outer,
                &em_for_outer,
                &binding_for_outer,
                &producer_binding_for_outer,
                producer_id,
                project_fn_id,
                concurrency,
            );
        });

        ctx.subscribe_to(source, outer_sink);
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
        .expect("invariant: register_producer has no deps; no error variants reachable")
}

/// Iteratively pop from `buffer` and spawn inners until cap is reached
/// or buffer is empty. Re-entrance from a nested `on_complete` is
/// short-circuited via [`MERGE_DRAIN_ACTIVE`]; the outermost call owns
/// the drain loop and picks up cap-frees on subsequent iterations.
// S2b/D234: `core: &Core` → `em: &ProducerEmitter`. The per-inner
// subscribe routes through `em.defer` (`CoreFull`); the returned
// `SubscriptionId` is wrapped in a `SubGuard` keyed in `inner_subs`
// (its Drop schedules the unsubscribe).
fn drain_merge_buffer(
    state: &Arc<Mutex<MergeMapState>>,
    em: &ProducerEmitter,
    binding: &Arc<dyn HigherOrderBinding>,
    producer_binding: &Arc<dyn ProducerBinding>,
    producer_id: NodeId,
    project_fn_id: FnId,
    concurrency: Option<u32>,
) {
    if MERGE_DRAIN_ACTIVE.with(|f| f.replace(true)) {
        // Already draining on this thread; outer loop will drain
        // remaining buffer.
        return;
    }

    loop {
        let h_and_id;
        let mut should_self_complete = false;
        {
            let mut s = state.lock().unwrap();
            if s.terminated {
                MERGE_DRAIN_ACTIVE.with(|f| f.set(false));
                return;
            }
            let allowed = match concurrency {
                None => true,
                Some(n) => s.active < n,
            };
            if !allowed {
                MERGE_DRAIN_ACTIVE.with(|f| f.set(false));
                return;
            }
            if let Some(h) = s.buffer.pop_front() {
                s.active += 1;
                let id = s.next_inner_id;
                s.next_inner_id += 1;
                s.pending_inner_ids.insert(id);
                h_and_id = Some((h, id));
            } else if s.source_done && s.active == 0 && !s.terminated {
                s.terminated = true;
                should_self_complete = true;
                h_and_id = None;
            } else {
                h_and_id = None;
            }
        }

        if should_self_complete {
            MERGE_DRAIN_ACTIVE.with(|f| f.set(false));
            em.complete_or_defer(producer_id);
            return;
        }

        let Some((outer_h, inner_id)) = h_and_id else {
            MERGE_DRAIN_ACTIVE.with(|f| f.set(false));
            return;
        };

        // Spawn lock-released.
        let inner_node = binding.invoke_project(project_fn_id, outer_h);
        binding.release_handle(outer_h);

        let on_complete = make_merge_on_complete(
            state.clone(),
            em.clone(),
            binding.clone(),
            producer_binding.clone(),
            producer_id,
            project_fn_id,
            inner_id,
            concurrency,
        );
        let on_error = make_merge_on_error(state.clone(), em.clone(), binding.clone(), producer_id);
        // F2 /qa: clone on_complete so the TornDown branch can
        // synthesize inner-Complete (closes the merge_map `s.active`
        // leak that left the producer never self-completing when a
        // projected inner was dead).
        let on_complete_for_dead = on_complete.clone();
        let inner_sink = build_inner_sink(
            em.clone(),
            producer_binding.clone(),
            producer_id,
            on_complete,
            on_error,
        );
        // D234: inner subscribe via `em.defer` (long-lived drain can't
        // hold `&Core`). The sync-completion detection still works: if
        // the inner pre-completes during `try_subscribe`'s handshake,
        // `on_complete` fires INSIDE this defer (owner-side) and removes
        // `inner_id` from `pending_inner_ids` before the post-subscribe
        // check below — so a now-dead sub is dropped, not installed.
        let state_sub = state.clone();
        let em_guard = em.clone();
        let _ = em.defer(move |c| {
            match c.try_subscribe(inner_node, inner_sink) {
                Ok(sub) => {
                    let guard = SubGuard::new(inner_node, sub, em_guard);
                    let to_drop = {
                        let mut s = state_sub.lock().unwrap();
                        if s.terminated || !s.pending_inner_ids.remove(&inner_id) {
                            Some(guard)
                        } else {
                            s.inner_subs.insert(inner_id, guard);
                            None
                        }
                    };
                    drop(to_drop);
                }
                Err(graphrefly_core::SubscribeError::TornDown { .. }) => {
                    // R2.2.7.b / F2 /qa: synthesize inner-Complete so
                    // `make_merge_on_complete` decrements `s.active`,
                    // clears pending, and checks the self-Complete
                    // trigger (Dead-inner `s.active` leak fix).
                    on_complete_for_dead();
                }
                Err(graphrefly_core::SubscribeError::PartitionOrderViolation(_)) => {
                    panic!(
                        "merge_map inner subscribe: partition-order \
                         violation inside em.defer — substrate invariant broken"
                    );
                }
            }
        });

        // Loop continues — pops next from buffer or returns.
    }
}

fn make_merge_on_complete(
    state: Arc<Mutex<MergeMapState>>,
    em: ProducerEmitter,
    binding: Arc<dyn HigherOrderBinding>,
    producer_binding: Arc<dyn ProducerBinding>,
    producer_id: NodeId,
    project_fn_id: FnId,
    this_inner_id: u64,
    concurrency: Option<u32>,
) -> Rc<dyn Fn()> {
    Rc::new(move || {
        let removed_sub;
        {
            let mut s = state.lock().unwrap();
            if s.terminated {
                return;
            }
            s.active -= 1;
            // Two cases:
            // (a) Sync-completion during subscribe: `pending_inner_ids`
            //     still contains us; `inner_subs` does not. Remove from
            //     pending so the post-subscribe insert sees we're done
            //     and skips installing the dead sub.
            // (b) Async completion: `inner_subs` contains us. Remove
            //     and drop lock-released.
            s.pending_inner_ids.remove(&this_inner_id);
            removed_sub = s.inner_subs.remove(&this_inner_id);
        }
        drop(removed_sub); // SubGuard::Drop → deferred unsubscribe.

        // Try to drain — if we're nested inside an outer drain loop
        // (sync-completion path), this is a no-op and the outer drain
        // continues.
        drain_merge_buffer(
            &state,
            &em,
            &binding,
            &producer_binding,
            producer_id,
            project_fn_id,
            concurrency,
        );
    })
}

/// Inner-error path for merge_map: terminates the producer + drains
/// all inner subs (lock-released) + releases buffered DATA handles.
/// Captures `binding` so we can release buffered handles' retains
/// (taken on enqueue in the outer_sink's Data branch); without this,
/// inner-error before all buffered DATAs project would leak refcount
/// shares.
fn make_merge_on_error(
    state: Arc<Mutex<MergeMapState>>,
    em: ProducerEmitter,
    binding: Arc<dyn HigherOrderBinding>,
    producer_id: NodeId,
) -> Rc<dyn Fn(HandleId)> {
    Rc::new(move |h| {
        let removed_subs;
        let buffered_to_release;
        {
            let mut s = state.lock().unwrap();
            if s.terminated {
                return;
            }
            s.terminated = true;
            removed_subs = s.inner_subs.drain().map(|(_, sub)| sub).collect::<Vec<_>>();
            s.pending_inner_ids.clear();
            buffered_to_release = s.buffer.drain(..).collect::<Vec<_>>();
        }
        drop(removed_subs); // each SubGuard::Drop → deferred unsubscribe.
        for h_b in buffered_to_release {
            binding.release_handle(h_b);
        }
        em.error_or_defer(producer_id, h);
    })
}

// =====================================================================
// Compile-time asserts (Slice E /qa; D248/D249-amended)
// =====================================================================
//
// D248/D249/S2c: `SwitchState`/`ExhaustState`/`MergeMapState` embed a
// `ProducerEmitter`, which now carries the owner-only `Rc<DeferQueue>`
// (the `!Send` `Defer` split off `CoreMailbox`). Under full
// single-owner these op-states are owner-thread-only and intentionally
// `!Send` — the prior `Send + Sync` assertions were shared-Core-era
// legacy and are deleted. `ProjectFn` is a pure projector (captures no
// `!Send`), so it stays `Send + Sync`.

const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<ProjectFn>();
};
