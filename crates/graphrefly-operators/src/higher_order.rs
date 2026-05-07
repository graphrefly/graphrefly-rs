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
use std::sync::{Arc, Mutex};

use ahash::AHashMap;
use graphrefly_core::{Core, FnId, HandleId, Message, NodeId, Sink, Subscription};
use smallvec::SmallVec;

use super::producer::{ProducerBinding, ProducerCtx};

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

fn build_inner_sink(
    core: Core,
    producer_binding: Arc<dyn ProducerBinding>,
    producer_id: NodeId,
    on_inner_complete: Arc<dyn Fn() + Send + Sync>,
    on_inner_error: Arc<dyn Fn(HandleId) + Send + Sync>,
) -> Sink {
    Arc::new(move |msgs: &[Message]| {
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
                Action::Emit(h) => core.emit(producer_id, h),
                Action::Complete => on_inner_complete(),
                Action::Error(h) => on_inner_error(h),
                Action::Invalidate => core.invalidate(producer_id),
                Action::Teardown => core.teardown(producer_id),
            }
        }
    })
}

// =====================================================================
// switch_map — cancel previous inner on each new outer DATA
// =====================================================================

struct SwitchState {
    /// Currently-active inner subscription (if any). Cancelling the
    /// prior inner is `Option::take` + lock-released drop.
    inner_sub: Option<Subscription>,
    source_done: bool,
    terminated: bool,
}

impl SwitchState {
    fn new() -> Self {
        Self {
            inner_sub: None,
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
    let core_clone = core.clone();
    let binding_clone: Arc<dyn HigherOrderBinding> = binding.clone();
    let producer_binding: Arc<dyn ProducerBinding> = binding.clone();

    let build = Box::new(move |ctx: ProducerCtx<'_>| {
        let producer_id = ctx.node_id();
        let state: Arc<Mutex<SwitchState>> = Arc::new(Mutex::new(SwitchState::new()));

        let state_for_outer = state.clone();
        let core_for_outer = core_clone.clone();
        let binding_for_outer = binding_clone.clone();
        let producer_binding_for_outer = producer_binding.clone();

        let outer_sink: Sink = Arc::new(move |msgs| {
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

                // Cancel prior inner sub (if any) lock-released.
                let prev_inner = {
                    let mut s = state_for_outer.lock().unwrap();
                    s.inner_sub.take()
                };
                drop(prev_inner);

                // invoke_project lock-released. Releases our retain after.
                let inner_node = binding_for_outer.invoke_project(project_fn_id, outer_h);
                binding_for_outer.release_handle(outer_h);

                let on_complete = make_switch_on_complete(
                    state_for_outer.clone(),
                    core_for_outer.clone(),
                    producer_id,
                );
                let on_error = make_switch_on_error(
                    state_for_outer.clone(),
                    core_for_outer.clone(),
                    producer_id,
                );
                let inner_sink = build_inner_sink(
                    core_for_outer.clone(),
                    producer_binding_for_outer.clone(),
                    producer_id,
                    on_complete,
                    on_error,
                );
                let inner_sub = core_for_outer.subscribe(inner_node, inner_sink);

                // Install (or drop, if terminated mid-project / inner
                // already completed and on_complete cleared the slot).
                // Prior slot is empty (we cleared above). If the inner
                // already self-completed during the handshake,
                // on_complete left `inner_sub: None`. Either way,
                // `replace` returns None and we install. Defensively
                // preserve any unexpected leftover for lock-released drop.
                let to_drop = {
                    let mut s = state_for_outer.lock().unwrap();
                    if s.terminated {
                        Some(inner_sub)
                    } else {
                        s.inner_sub.replace(inner_sub)
                    }
                };
                drop(to_drop);
            }

            if plan.self_complete {
                core_for_outer.complete(producer_id);
            } else if let Some(h) = plan.self_error {
                core_for_outer.error(producer_id, h);
            }
        });

        ctx.subscribe_to(source, outer_sink);
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
}

fn make_switch_on_complete(
    state: Arc<Mutex<SwitchState>>,
    core: Core,
    producer_id: NodeId,
) -> Arc<dyn Fn() + Send + Sync> {
    Arc::new(move || {
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
        drop(prev_inner);
        if should_complete {
            core.complete(producer_id);
        }
    })
}

fn make_switch_on_error(
    state: Arc<Mutex<SwitchState>>,
    core: Core,
    producer_id: NodeId,
) -> Arc<dyn Fn(HandleId) + Send + Sync> {
    Arc::new(move |h| {
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
        core.error(producer_id, h);
    })
}

// =====================================================================
// exhaust_map — ignore outer DATA while inner is active
// =====================================================================

struct ExhaustState {
    inner_sub: Option<Subscription>,
    source_done: bool,
    terminated: bool,
}

impl ExhaustState {
    fn new() -> Self {
        Self {
            inner_sub: None,
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
    let core_clone = core.clone();
    let binding_clone: Arc<dyn HigherOrderBinding> = binding.clone();
    let producer_binding: Arc<dyn ProducerBinding> = binding.clone();

    let build = Box::new(move |ctx: ProducerCtx<'_>| {
        let producer_id = ctx.node_id();
        let state: Arc<Mutex<ExhaustState>> = Arc::new(Mutex::new(ExhaustState::new()));

        let state_for_outer = state.clone();
        let core_for_outer = core_clone.clone();
        let binding_for_outer = binding_clone.clone();
        let producer_binding_for_outer = producer_binding.clone();

        let outer_sink: Sink = Arc::new(move |msgs| {
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
                                if s.inner_sub.is_none() && plan.first_outer_h.is_none() {
                                    binding_for_outer.retain_handle(h);
                                    plan.first_outer_h = Some(h);
                                    plan.first_retained = true;
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
                    core_for_outer.clone(),
                    producer_id,
                );
                let on_error = make_exhaust_on_error(
                    state_for_outer.clone(),
                    core_for_outer.clone(),
                    producer_id,
                );
                let inner_sink = build_inner_sink(
                    core_for_outer.clone(),
                    producer_binding_for_outer.clone(),
                    producer_id,
                    on_complete,
                    on_error,
                );
                // If inner already pre-completed during the handshake,
                // on_complete already cleared `inner_sub`. We `replace`
                // either way; in the synchronous-completion path,
                // `inner_sub` was None so our just-subscribed (and
                // already-dead) sub is dropped on the next iteration.
                let inner_sub = core_for_outer.subscribe(inner_node, inner_sink);
                let to_drop = {
                    let mut s = state_for_outer.lock().unwrap();
                    if s.terminated {
                        Some(inner_sub)
                    } else {
                        s.inner_sub.replace(inner_sub)
                    }
                };
                drop(to_drop);
            }

            if plan.self_complete {
                core_for_outer.complete(producer_id);
            } else if let Some(h) = plan.self_error {
                core_for_outer.error(producer_id, h);
            }
        });

        ctx.subscribe_to(source, outer_sink);
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
}

fn make_exhaust_on_complete(
    state: Arc<Mutex<ExhaustState>>,
    core: Core,
    producer_id: NodeId,
) -> Arc<dyn Fn() + Send + Sync> {
    Arc::new(move || {
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
        drop(prev_inner);
        if should_complete {
            core.complete(producer_id);
        }
    })
}

fn make_exhaust_on_error(
    state: Arc<Mutex<ExhaustState>>,
    core: Core,
    producer_id: NodeId,
) -> Arc<dyn Fn(HandleId) + Send + Sync> {
    Arc::new(move |h| {
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
        core.error(producer_id, h);
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
    inner_subs: AHashMap<u64, Subscription>,
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
    let core_clone = core.clone();
    let binding_clone: Arc<dyn HigherOrderBinding> = binding.clone();
    let producer_binding: Arc<dyn ProducerBinding> = binding.clone();

    let build = Box::new(move |ctx: ProducerCtx<'_>| {
        let producer_id = ctx.node_id();
        let state: Arc<Mutex<MergeMapState>> = Arc::new(Mutex::new(MergeMapState::new()));

        let state_for_outer = state.clone();
        let core_for_outer = core_clone.clone();
        let binding_for_outer = binding_clone.clone();
        let producer_binding_for_outer = producer_binding.clone();

        let outer_sink: Sink = Arc::new(move |msgs| {
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
                core_for_outer.error(producer_id, h);
                return;
            }
            if self_complete_now {
                core_for_outer.complete(producer_id);
                return;
            }

            // Phase 2: drain buffer iteratively up to concurrency cap.
            drain_merge_buffer(
                &state_for_outer,
                &core_for_outer,
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
}

/// Iteratively pop from `buffer` and spawn inners until cap is reached
/// or buffer is empty. Re-entrance from a nested `on_complete` is
/// short-circuited via [`MERGE_DRAIN_ACTIVE`]; the outermost call owns
/// the drain loop and picks up cap-frees on subsequent iterations.
fn drain_merge_buffer(
    state: &Arc<Mutex<MergeMapState>>,
    core: &Core,
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
            core.complete(producer_id);
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
            core.clone(),
            binding.clone(),
            producer_binding.clone(),
            producer_id,
            project_fn_id,
            inner_id,
            concurrency,
        );
        let on_error =
            make_merge_on_error(state.clone(), core.clone(), binding.clone(), producer_id);
        let inner_sink = build_inner_sink(
            core.clone(),
            producer_binding.clone(),
            producer_id,
            on_complete,
            on_error,
        );
        let inner_sub = core.subscribe(inner_node, inner_sink);

        // Decide whether to install the sub: if `on_complete` fired
        // synchronously inside `subscribe` (pre-completed inner), it
        // already removed `inner_id` from `pending_inner_ids`.
        let to_drop = {
            let mut s = state.lock().unwrap();
            if s.terminated || !s.pending_inner_ids.remove(&inner_id) {
                Some(inner_sub)
            } else {
                s.inner_subs.insert(inner_id, inner_sub);
                None
            }
        };
        drop(to_drop);

        // Loop continues — pops next from buffer or returns.
    }
}

fn make_merge_on_complete(
    state: Arc<Mutex<MergeMapState>>,
    core: Core,
    binding: Arc<dyn HigherOrderBinding>,
    producer_binding: Arc<dyn ProducerBinding>,
    producer_id: NodeId,
    project_fn_id: FnId,
    this_inner_id: u64,
    concurrency: Option<u32>,
) -> Arc<dyn Fn() + Send + Sync> {
    Arc::new(move || {
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
        drop(removed_sub);

        // Try to drain — if we're nested inside an outer drain loop
        // (sync-completion path), this is a no-op and the outer drain
        // continues.
        drain_merge_buffer(
            &state,
            &core,
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
    core: Core,
    binding: Arc<dyn HigherOrderBinding>,
    producer_id: NodeId,
) -> Arc<dyn Fn(HandleId) + Send + Sync> {
    Arc::new(move |h| {
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
        drop(removed_subs);
        for h_b in buffered_to_release {
            binding.release_handle(h_b);
        }
        core.error(producer_id, h);
    })
}

// =====================================================================
// Send + Sync compile-time asserts (Slice E /qa)
// =====================================================================

const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SwitchState>();
    assert_send_sync::<ExhaustState>();
    assert_send_sync::<MergeMapState>();
    assert_send_sync::<ProjectFn>();
};
