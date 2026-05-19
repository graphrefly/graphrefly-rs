//! Wave engine — drain loop, fire selection, emission commit, sink dispatch.
//!
//! Ports the wave-engine portion of the handle-protocol prototype
//! (`~/src/graphrefly-ts/src/__experiments__/handle-core/core.ts`).
//! Sibling to [`super::node`]; the dispatcher's other concerns
//! (registration, subscription, pause/resume, terminal cascade,
//! `set_deps`) live there.
//!
//! # Wave engine entry points
//!
//! - [`Core::run_wave`] — wave entry. Claims `in_tick` under the state lock,
//!   runs `op` lock-released, then drains all transitive fn-fires and
//!   flushes per-subscriber notifications. Each fn-fire iteration drops
//!   the state lock around `BindingBoundary::invoke_fn` so user fn callbacks
//!   can re-enter Core safely.
//! - [`Core::drain_and_flush`] — drain phase + flush phase. Acquires/drops
//!   the state lock per iteration around `invoke_fn`.
//! - [`Core::commit_emission`] — equals-substitution + DIRTY/DATA/RESOLVED
//!   queueing + child propagation. `&self`-only; bracket-fires
//!   `BindingBoundary::custom_equals` lock-released.
//! - [`Core::queue_notify`] — per-subscriber message queueing with
//!   pause-buffer routing. Snapshots the subscriber list at first-touch-
//!   per-wave so late subscribers (installed mid-wave between drain
//!   iterations) don't receive duplicate deliveries from messages already
//!   queued before they subscribed.
//! - [`Core::deliver_data_to_consumer`] — single-edge propagation; marks
//!   the consumer for fn-fire if its tracked-deps set is satisfied.
//!   Called from `commit_emission`, plus `activate_derived` and
//!   `set_deps` in [`super::node`].
//!
//! # Re-entrance discipline (Slice A close — M1 fully lock-released)
//!
//! - **Wave-end sink fires** drop the state lock first (Slice A-bigger
//!   discipline).
//! - **`BindingBoundary::invoke_fn`** in `fire_fn` fires lock-released —
//!   user fn callbacks may re-enter `Core::emit` / `pause` / `resume` /
//!   `invalidate` / `complete` / `error` / `teardown` and run a nested
//!   wave (the existing `in_tick` re-entrance gate composes
//!   transparently).
//! - **`BindingBoundary::custom_equals`** in `commit_emission`'s equals
//!   check fires lock-released.
//! - **Subscribe-time handshake** is the one remaining lock-held callback.
//!   It now fires per-tier (`[Start]`, `[Data(v)]`, `[Complete|Error]`,
//!   `[Teardown]`) as separate sink calls, matching the canonical R1.3.5.a
//!   tier-split. Re-entrance from a handshake sink callback panics with
//!   the [`reentrance_guard`] diagnostic.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use ahash::AHashSet;
use indexmap::map::Entry;
use indexmap::IndexMap;

use smallvec::SmallVec;

use crate::boundary::{DepBatch, FnEmission, FnResult};
use crate::handle::{FnId, HandleId, NodeId, NO_HANDLE};
use crate::message::Message;
use crate::node::{Core, CoreState, EqualsMode, OperatorOp, Sink, TerminalKind};

// Slice G (R1.3.2.d / R1.3.3.a) per-thread tier-3-emit tracker.
//
// **Wave scope = the owner thread (S4, D246/D248/D249).** `Core` is
// single-owner `!Send + !Sync`: every emit in a wave runs on the one
// owner thread, and a wave is one uninterrupted owner-side drain
// bounded above by the outermost `BatchGuard` drop. A per-thread
// `AHashSet<NodeId>` is therefore the natural placement for "has node
// X already emitted a tier-3 message in this wave?" — its lifetime
// matches the wave's, with no cross-thread or cross-wave contamination
// (there is no other thread driving this Core; the deleted
// `wave_owner` `ReentrantMutex` / cross-thread-BLOCK model is gone
// with S2c's §7 machinery).
//
// **History:** placed per-partition on `SubgraphLockBox::state` (Q3
// v1), moved to a per-thread thread-local by the D1 patch
// (2026-05-09) to survive mid-wave cross-thread `set_deps` partition
// splits — a hazard that only existed under the now-deleted
// shared-Core cross-thread model. Post-S4 the thread-local placement
// is simply the owner thread's wave-scoped set.
//
// **Lifecycle:** populated by `Core::commit_emission` /
// `Core::commit_emission_verbatim`; cleared at the OUTERMOST
// `BatchGuard` drop on the owner thread (both success and
// panic-discard paths). Re-entrant nested waves on the same Core+
// thread share the set — inner-wave emits add to the same set; the
// outermost drop is the canonical clear point.
thread_local! {
    static TIER3_EMITTED_THIS_WAVE: RefCell<AHashSet<NodeId>> = RefCell::new(AHashSet::new());
}

// Q-beyond Sub-slice 1 (D108 / 2026-05-09): per-thread wave-scoped state.
//
// **Design rationale (bench-driven, see `benches/lock_strategy.rs`):**
// - S1 showed parking_lot Mutex same-thread re-acquire is ~14 ns/op,
//   identical to thread_local borrow_mut. The "mutex hop is slow" intuition
//   is wrong UNCONTENDED.
// - S3 showed shared mutex on disjoint cross-thread keys is 2.7× slower
//   than per-partition mutex / thread_local (35.9 vs 13.0 ns/op) — pure
//   cache-line bouncing on the lock state itself.
// - Conclusion: the cost of the prior `Core::cross_partition` mutex was
//   dominated by cache-line bouncing across cores, NOT by single-thread
//   mutex acquire overhead. Moving the four wave-scoped fields to a
//   per-thread thread_local eliminates the bounce point entirely.
//
// **Wave scope = the owner thread (S4, D246/D248/D249):** `Core` is
// single-owner `!Send + !Sync` — exactly one owner thread drives a
// given `Core`, and a wave is **one uninterrupted owner-side drain**
// (the deleted cross-thread `wave_owner` `ReentrantMutex` / "cross-thread
// emits BLOCK" model is gone with S2c's §7 machinery). There is no
// cross-thread interleave to defend against; the only cross-`Core`
// concurrency is host-native via *independent* per-worker Cores
// (actor model), each with its own `WAVE_STATE`.
//
// **Lifecycle:** populated by `Core::commit_emission` /
// `Core::queue_notify` / etc.; mostly drained mid-wave by the auto-resolve
// sweep + cache snapshot commit/restore. Outermost `BatchGuard::drop`
// releases any retained handles still in `wave_cache_snapshots` /
// `deferred_handle_releases`. Defensive wave-start clear at outermost
// owning BatchGuard entry guards against cargo's thread-reuse propagating
// stale entries from a prior panicked-mid-wave test.
thread_local! {
    static WAVE_STATE: RefCell<WaveState> = RefCell::new(WaveState::new());
}

// Wave-ownership flag, keyed per-(Core, thread). Membership of a
// `Core::generation` value means "this thread is currently inside an
// OWNING wave on that Core" — i.e. the outermost `BatchGuard` whose drop
// must run the drain. Replaces the former Core-global `CoreState::in_tick`
// bool.
//
// **Why keyed by `Core::generation` (S4, D246/D248/D249).** Post-S2c
// `Core` is single-owner `!Send + !Sync`: one owner thread per Core, a
// wave is one uninterrupted owner-side drain. The set must still satisfy
// two LIVE constraints (the third — disjoint-partition cross-thread
// drain — is **deleted-model**: it required two threads driving disjoint
// partitions of ONE shared Core via separate `wave_owner`s, which D248
// structurally removed):
//   - **Cross-Core isolation (/qa F1, LIVE).** One owner thread may
//     hold a live `BatchGuard` on Core-A and, owner-side (e.g. a
//     `DeferQueue` closure or the embedder pump driving another Core),
//     enter a wave on Core-B. Core-B must not see Core-A's flag. Keying
//     by `Core::generation` (process-monotonic, never reused) gives each
//     Core a distinct slot — a single thread-local bool would leak
//     Core-A's `in_tick` to Core-B on the same owner thread (the /qa F1
//     bug). This is why it stays an `AHashSet<u64>` (≥2 entries during
//     owner-side cross-Core nesting), NOT a bool.
//   - **Nested same-(Core, thread) re-entry (/qa EC#3, LIVE).** A nested
//     `run_wave` (mailbox-applied `Emit`/`Complete`, the §7-floor
//     re-entry seam) on the same Core+thread MUST observe ownership so
//     its drop no-ops and the outer wave drains. A shared slot per
//     (Core, owner-thread) preserves this.
//
// **No lock required.** `in_tick` is only read/written by the one owner
// thread (single-owner `!Sync` Core ⇒ no cross-thread reader). Unlike
// `currently_firing` (kept on `CoreShared` for the D1 set_deps
// reentrancy guard — see `node.rs`), `in_tick` has no cross-thread read
// requirement and needs no shared-state lock.
//
// Stale slots: the owning `BatchGuard::drop` releases the generation on
// every exit path — normal return, the closure-body-panic branch, AND
// the drain-phase-panic `catch_unwind` arm (before `resume_unwind`). So
// a slot can only be left interned if `Drop` itself never runs:
// `std::mem::forget(guard)` or a process abort without unwinding — both
// out of contract (`BatchGuard` is `#[must_use]` + `!Send`). Such a
// leaked key is inert: generations are never reused, so it can never
// false-match a future Core (no correctness impact), and the leak is
// bounded by the number of distinct Cores a thread ever forgets a guard
// on — not an unbounded leak under any normal or panic-recovery path.
//
// History: this flag lived briefly per-thread (Q-beyond sub-slice 3),
// was reverted to Core-global (/qa F1+F2), keyed per-(Core, thread)
// for the deleted disjoint-cross-thread model (D047, 2026-05-15), and
// at S4 (D246/D248/D249) the disjoint-partition constraint was retired
// with the §7 cross-thread machinery — the `Core::generation` keying
// survives solely for the two LIVE constraints above (cross-Core
// owner-side nesting + nested same-Core re-entry). See
// `docs/rust-port-decisions.md` D047/D246 and `docs/migration-status.md`
// § "D246 S4".
thread_local! {
    static IN_TICK_OWNED: RefCell<AHashSet<u64>> = RefCell::new(AHashSet::new());
}

/// Wave-scoped state previously held under [`Core::cross_partition`]'s
/// `parking_lot::Mutex<CrossPartitionState>`. Now per-thread (Q-beyond
/// Sub-slice 1, 2026-05-09; Sub-slice 2 added `pending_fires` +
/// `pending_notify`, 2026-05-09; Sub-slice 3 added `currently_firing`,
/// `in_tick`, `deferred_flush_jobs`, `deferred_cleanup_hooks`,
/// `pending_wipes`, `invalidate_hooks_fired_this_wave`, 2026-05-09).
///
/// All fields are populated and drained within one wave on the one
/// owner thread. Cross-thread access is structurally impossible —
/// `Core` is single-owner `!Send + !Sync` (S4/D248); there is no
/// other thread driving this Core.
///
/// **Refcount discipline (load-bearing):** `wave_cache_snapshots`,
/// `deferred_handle_releases`, and `pending_notify` hold binding-side
/// handle retains. They MUST be drained (and released through
/// `Core::binding.release_handle`) by the outermost `BatchGuard::drop`
/// on success and panic paths. `pending_notify` holds one retain per
/// payload-bearing message (one per `Message::payload_handle()`); the
/// retains are taken in `Core::queue_notify` and balanced either by
/// `flush_notifications` (success path: pushed into
/// `deferred_handle_releases`) or directly in the panic-discard path of
/// `BatchGuard::drop` (taken from `pending_notify` and released).
///
/// The thread_local has no `Drop` hook with access to a binding — a
/// panic that bypasses `BatchGuard::drop` (e.g. panic OUTSIDE any batch)
/// would leak retains until the thread exits OR the next outermost
/// wave-start clear runs (which for safety we don't fire — clearing
/// without releasing would double-leak by losing the retain). The
/// defensive wave-start clear in `BatchGuard::begin_batch_with_guards`
/// clears `pending_auto_resolve` + `pending_pause_overflow` +
/// `pending_fires` (no retains) + `currently_firing` +
/// `invalidate_hooks_fired_this_wave` (also no retains) but NOT the
/// retain-holding fields — those must be empty by construction at
/// outermost wave start (a prior wave's panic-discard path drained them,
/// or a prior wave's success path drained them).
pub(crate) struct WaveState {
    /// Payload-handle releases owed for messages that landed in
    /// `pending_notify` during this wave (one per `payload_handle()`).
    /// `BatchGuard::drop` releases these after sinks fire and the lock
    /// is dropped, balancing the retain done in `queue_notify`.
    pub(crate) deferred_handle_releases: Vec<HandleId>,
    /// Pre-wave cache snapshots used to restore state if the wave aborts
    /// mid-flight (e.g., a `Core::batch` closure panics). Each entry is
    /// `(node_id → old_cache_handle)` — the handle the node held BEFORE
    /// the wave started writing to it. The snapshotted handle holds a
    /// retain (taken when the snapshot was inserted) so it stays alive
    /// for restoration. On wave success, snapshots are drained and their
    /// retains released. On wave abort, each cache slot is restored from
    /// the snapshot and the original retain transfers to the cache slot.
    pub(crate) wave_cache_snapshots: HashMap<NodeId, HandleId>,
    /// Nodes that need an auto-Resolved at wave end if they don't receive
    /// a tier-3+ message from their own commit_emission. Populated by
    /// the RESOLVED child propagation in `commit_emission`. Drained by
    /// the auto-resolve sweep in `drain_and_flush`.
    pub(crate) pending_auto_resolve: AHashSet<NodeId>,
    /// R1.3.8.c pause-overflow ERROR synthesis queue. Recorded by
    /// [`Core::queue_notify`] when the pause buffer first overflows;
    /// drained at wave-end after the lock-released call to
    /// `BindingBoundary::synthesize_pause_overflow_error`.
    pub(crate) pending_pause_overflow: Vec<crate::node::PendingPauseOverflow>,
    /// Nodes whose fn we owe a fire to — drained by [`Core::run_wave`].
    ///
    /// Q-beyond Sub-slice 2 (D108, 2026-05-09): moved from
    /// `CoreState::pending_fires` to per-thread `WaveState`. Wave-scoped
    /// — populated by `deliver_data_to_consumer`, `terminate_node`'s
    /// child-cascade `QueueFire` branch, `activate_derived`'s producer
    /// queueing, `resume`'s pending-wave consolidation, and operator
    /// re-arm paths; drained by `pick_next_fire` / `fire_fn` /
    /// `fire_regular` / `fire_operator` (each removes the firing node
    /// before invoking).
    pub(crate) pending_fires: AHashSet<NodeId>,
    /// Per-node outgoing message buffer; flushed at wave end. Insertion-
    /// ordered so flush order is deterministic — load-bearing for
    /// R1.3.9.d meta-TEARDOWN ordering: when a parent and its meta
    /// companion both have queued messages in the same wave, the meta
    /// (queued first via `teardown_inner`'s recursion order) flushes
    /// first.
    ///
    /// Each entry carries the per-wave subscriber snapshot taken at first
    /// touch (Slice A close, M1: lock-released drain). Late subscribers
    /// installed mid-wave between fn-fire iterations don't appear in
    /// already-snapshotted entries; this is the load-bearing fix that
    /// prevents duplicate-Data delivery when a handshake delivers the
    /// post-commit cache and the wave's flush would otherwise also fire
    /// to the same sink.
    ///
    /// Q-beyond Sub-slice 2 (D108, 2026-05-09): moved from
    /// `CoreState::pending_notify` to per-thread `WaveState`. The map
    /// holds a payload-handle retain per payload-bearing message
    /// (`Message::payload_handle()`); these MUST be released by the
    /// outermost `BatchGuard::drop` (success path: through
    /// `flush_notifications` → `deferred_handle_releases`; panic path:
    /// directly in `BatchGuard::drop`'s panic branch).
    pub(crate) pending_notify: IndexMap<NodeId, PendingPerNode>,
    /// D217-AMEND-2 (2026-05-16): persistent spare for `pending_notify`,
    /// ping-ponged with it at wave end so a fresh `IndexMap::default()`
    /// (a new `ahash::RandomState` via `gen_hasher_seed`/`from_keys`
    /// PLUS RawVec realloc churn on the next wave's `queue_notify`) is
    /// NEVER constructed after thread init. The empirical attribution
    /// (`examples/profile_st_emit.rs` + macOS `sample`) put the old
    /// per-wave `mem::take(&mut pending_notify)` at ~1250 of ~4767
    /// hot-path samples — the dominant §7 floor tax (D217 lever-1
    /// "slab store" falsified; the node store is minor). Holds NO
    /// retains between waves: it is always empty (cleared, capacity +
    /// hasher retained) outside `flush_notifications`.
    pub(crate) pending_notify_recycle: IndexMap<NodeId, PendingPerNode>,
    // Q-beyond Sub-slice 3 (D108, 2026-05-09) moved `in_tick` and
    // `currently_firing` from `CoreState` to per-thread `WaveState`;
    // /qa F1+F2 (2026-05-10) reverted both to `CoreState`; the in_tick
    // placement was finalized 2026-05-15 (see below). The two fields have
    // *different* scope requirements:
    //
    // - **`in_tick` — per-(Core, thread).** Pure thread-local broke
    //   cross-Core isolation (Core-A's flag leaked to Core-B on the same
    //   OS thread → Core-B non-owning → its writes drained by Core-A's
    //   binding, /qa F1). Pure Core-global broke disjoint-partition drain
    //   ownership (thread B's independent disjoint wave saw thread A's
    //   flag → non-owning → never drained). The (Core, thread) key —
    //   `crate::batch::IN_TICK_OWNED`, keyed by `Core::generation` —
    //   satisfies both, plus same-(Core, thread) nested re-entry
    //   (/qa EC#3). NOT a `CoreState` field.
    //
    // - **`currently_firing` — Core-global (stays on `CoreState`).**
    //   Per-thread placement silently bypassed the cross-thread P13
    //   partition-migration check in `Core::set_deps`: thread B's set_deps
    //   must observe thread A's firing pushes. Per-Core (cross-thread
    //   visible) placement restores the D091 safety check (/qa F2).
    //
    // The other 11 wave-scoped fields stay per-thread because they're
    // accessed only by the one owner thread (single-owner `!Send` Core,
    // S4/D248 — no cross-thread emitter exists).
    /// Slice E2 (R1.3.9.b strict per D057): per-wave-per-node dedup
    /// for `OnInvalidate` cleanup hook firing. A node already in this
    /// set this wave has already had its `OnInvalidate` queued into
    /// `deferred_cleanup_hooks` and MUST NOT queue again, even if
    /// `invalidate_inner` re-encounters it.
    ///
    /// Q-beyond Sub-slice 3 (D108, 2026-05-09): moved from
    /// `CoreState::invalidate_hooks_fired_this_wave` to per-thread
    /// `WaveState`. Wave-scoped — populated by `invalidate_inner` and
    /// cleared by `WaveState::clear_wave_state`.
    pub(crate) invalidate_hooks_fired_this_wave: AHashSet<NodeId>,
    /// Deferred sink-fire jobs collected by `flush_notifications`.
    /// `flush_notifications` populates this from `pending_notify`;
    /// `Core::drain_deferred` takes it and `Core::fire_deferred` fires
    /// each entry lock-released. Each tuple is
    /// `(sinks_for_one_node_one_phase, phase_messages)`. Empty between
    /// waves.
    ///
    /// Q-beyond Sub-slice 3 (D108, 2026-05-09): moved from
    /// `CoreState::deferred_flush_jobs` to per-thread `WaveState`. No
    /// retains held — the `Vec<Sink>` clones own Arcs that drop
    /// naturally; the `Vec<Message>` payload retains were already moved
    /// into `deferred_handle_releases` by `flush_notifications`.
    pub(crate) deferred_flush_jobs: DeferredJobs,
    /// Slice E2 (per D060/D061): lock-released drain queue for
    /// `OnInvalidate` cleanup hooks. Populated by `Core::invalidate_inner`
    /// when a node's cache transitions `!= NO_HANDLE → NO_HANDLE`;
    /// drained after the lock drops at wave boundary by
    /// `Core::fire_deferred` (each call wrapped in `catch_unwind` per
    /// D060). Panic-discarded silently per D061.
    ///
    /// Q-beyond Sub-slice 3 (D108, 2026-05-09): moved from
    /// `CoreState::deferred_cleanup_hooks` to per-thread `WaveState`.
    pub(crate) deferred_cleanup_hooks: Vec<(NodeId, crate::boundary::CleanupTrigger)>,
    /// Slice E2 /qa Q2(b) (D069): lock-released drain queue for
    /// `BindingBoundary::wipe_ctx` calls fired eagerly from
    /// `Core::terminate_node` when a resubscribable node terminates with
    /// no live subscribers. Drained alongside `deferred_cleanup_hooks`
    /// at wave boundary; same `catch_unwind` discipline. Panic-discarded
    /// silently.
    ///
    /// Q-beyond Sub-slice 3 (D108, 2026-05-09): moved from
    /// `CoreState::pending_wipes` to per-thread `WaveState`.
    pub(crate) pending_wipes: Vec<NodeId>,
}

impl WaveState {
    fn new() -> Self {
        Self {
            deferred_handle_releases: Vec::new(),
            wave_cache_snapshots: HashMap::new(),
            pending_auto_resolve: AHashSet::new(),
            pending_pause_overflow: Vec::new(),
            pending_fires: AHashSet::new(),
            pending_notify: IndexMap::new(),
            // D217-AMEND-2: one `IndexMap::default()` for the thread's
            // life — its ahash seed + capacity are recycled forever.
            pending_notify_recycle: IndexMap::new(),
            invalidate_hooks_fired_this_wave: AHashSet::new(),
            deferred_flush_jobs: Vec::new(),
            deferred_cleanup_hooks: Vec::new(),
            pending_wipes: Vec::new(),
        }
    }

    /// Wave-end clear of the non-retain-holding fields. Called from
    /// [`Core::drain_and_flush`]'s wave-end path. Fields holding retains
    /// (`wave_cache_snapshots`, `deferred_handle_releases`,
    /// `pending_notify`) are NOT cleared here — they follow the
    /// success/panic paths' explicit drain discipline in
    /// `BatchGuard::drop`.
    pub(crate) fn clear_wave_state(&mut self) {
        self.pending_auto_resolve.clear();
        // pending_pause_overflow is normally drained by drain_and_flush
        // via the synthesis loop. If a wave is panic-discarded BEFORE
        // synthesis runs, BatchGuard::drop's panic path also clears it
        // explicitly. Pre-wave defensive clear in
        // `begin_batch_with_guards` makes this idempotent.
        self.pending_pause_overflow.clear();
        // Sub-slice 2: pending_fires is intentionally NOT cleared
        // here. Two reasons:
        //   1. Wave-success drain empties it by construction: every
        //      `pick_next_fire` selection is removed by
        //      `fire_regular` / `fire_operator` before invocation,
        //      and `drain_and_flush` only exits when the set is empty.
        //   2. The `Core::resume` default-mode consolidated-fire
        //      pattern stages an entry OUTSIDE any in-tick wave and
        //      then enters a new wave to drain it; clearing here
        //      would erase that pre-staged entry. The panic-discard
        //      path in `BatchGuard::drop` clears it explicitly.

        // /qa F2 reverted (2026-05-10): currently_firing moved BACK to
        // CoreState::currently_firing — defensive clear there.
        // Slice E2 (D057): per-wave-per-node OnInvalidate dedup is
        // wave-scoped — cleared so the next wave can fire cleanups
        // again.
        self.invalidate_hooks_fired_this_wave.clear();
        // `deferred_flush_jobs`, `deferred_cleanup_hooks`, and
        // `pending_wipes` are intentionally NOT cleared here. They
        // follow the same discipline as `deferred_handle_releases` /
        // `pending_notify`:
        //   - SUCCESS path (`BatchGuard::drop` non-panic): drained by
        //     `Core::drain_deferred` AFTER `clear_wave_state` runs,
        //     then fired lock-released by `Core::fire_deferred`.
        //   - PANIC-DISCARD path (`BatchGuard::drop` panic): explicitly
        //     `std::mem::take`-and-dropped AFTER `clear_wave_state`
        //     runs (silently per D061 / D069).
        // Clearing here would race the success path: queued sink fires
        // / cleanup hooks / wipes would be erased BEFORE
        // `drain_deferred` could take them.
    }
}

/// Run a closure with mutable access to this thread's [`WaveState`].
///
/// Convention: prefer this helper over inline `WAVE_STATE.with(...)`
/// for sites that touch ONE field. For sites that interleave state lock
/// access with wave-state mutation, inline `WAVE_STATE.with(...)` keeps
/// the lock-acquire / wave-state-borrow scopes visible (mirrors the
/// pre-Q-beyond `let mut s = self.lock_state(); let mut cps = self.lock_cross_partition();`
/// pattern).
///
/// **Re-entrance:** the closure MUST NOT re-enter Core in a way that
/// would call back into `with_wave_state` — `RefCell::borrow_mut` panics
/// on nested borrow. The same discipline that the prior
/// `parking_lot::Mutex<CrossPartitionState>` enforced (no re-entry
/// holding cross_partition) carries over.
pub(crate) fn with_wave_state<R>(f: impl FnOnce(&mut WaveState) -> R) -> R {
    WAVE_STATE.with(|cell| f(&mut cell.borrow_mut()))
}

/// Outermost-wave defensive clear of [`WaveState`]'s non-retain-holding
/// fields. Called from [`BatchGuard::begin_batch_with_guards`] on
/// outermost owning entry. Mirrors the pre-existing tier3 defensive
/// clear (D1 patch, 2026-05-09) — guards against cargo's thread-reuse
/// propagating stale entries from a prior panicked-mid-wave test.
///
/// The retain-holding fields (`wave_cache_snapshots` /
/// `deferred_handle_releases`) MUST already be empty by construction at
/// outermost wave entry — outermost `BatchGuard::drop` always drains
/// them on both success and panic paths. If they're non-empty here it
/// indicates a prior wave bypassed `BatchGuard::drop`; in that case
/// the next BatchGuard's outermost drop will eventually drain them.
fn wave_state_clear_outermost() {
    with_wave_state(|ws| {
        // /qa F4 (2026-05-10): debug_assert that retain-holding fields
        // are empty at outermost wave start. The invariant claim is
        // "outermost BatchGuard::drop drains them on both success and
        // panic paths, so they're empty before the next wave starts."
        // If a panic path EVER bypasses the drain (today: not reachable
        // because BatchGuard::drop is robust against panicking sinks via
        // catch_unwind), this assert catches it in tests immediately
        // rather than letting stale entries leak into the next wave's
        // drain (which would release Core-A's HandleIds via Core-B's
        // binding under cross-Core same-thread sequential use).
        debug_assert!(
            ws.wave_cache_snapshots.is_empty(),
            "wave_state_clear_outermost: wave_cache_snapshots non-empty at \
             outermost wave start ({} entries) — prior BatchGuard::drop \
             bypassed the drain (would leak retains into next wave's \
             binding). See /qa F4 (2026-05-10).",
            ws.wave_cache_snapshots.len()
        );
        debug_assert!(
            ws.deferred_handle_releases.is_empty(),
            "wave_state_clear_outermost: deferred_handle_releases non-empty \
             at outermost wave start ({} entries) — prior BatchGuard::drop \
             bypassed the drain. See /qa F4 (2026-05-10).",
            ws.deferred_handle_releases.len()
        );
        debug_assert!(
            ws.pending_notify.is_empty(),
            "wave_state_clear_outermost: pending_notify non-empty at \
             outermost wave start ({} entries) — prior BatchGuard::drop \
             bypassed the drain. See /qa F4 (2026-05-10).",
            ws.pending_notify.len()
        );
        // D217-AMEND-2 / QA: enforce the invariant the field's own doc
        // claims ("always empty outside `flush_notifications`"). The
        // recycle slot is cleared at the end of every `flush_notifications`
        // and drained on the panic-discard path (`discard_wave_cleanup`);
        // a non-empty slot here means a panic bypassed both — surface it
        // loudly in tests rather than silently injecting a prior wave's
        // stale entries into the next wave's `mem::swap`.
        debug_assert!(
            ws.pending_notify_recycle.is_empty(),
            "wave_state_clear_outermost: pending_notify_recycle non-empty \
             at outermost wave start ({} entries) — a panic bypassed both \
             flush_notifications' clear AND discard_wave_cleanup's drain. \
             See D217-AMEND-2 / QA (2026-05-16).",
            ws.pending_notify_recycle.len()
        );
        ws.pending_auto_resolve.clear();
        ws.pending_pause_overflow.clear();
        // Sub-slice 2: pending_fires is intentionally NOT cleared here.
        // Pre-Sub-slice-2 it lived on CoreState and survived between
        // waves; load-bearing for `Core::resume`'s default-mode
        // consolidated-fire pattern, which inserts into pending_fires
        // OUTSIDE any in-tick wave (Phase 1, lock-held but `in_tick`
        // false at that moment) and then calls `run_wave_for(node_id)`
        // — `run_wave_for` enters a NEW outermost wave whose drain must
        // pick up that pre-staged pending_fires entry. Clearing here
        // would erase it.
        //
        // pending_fires holds no retains, so a stale entry from a
        // prior panicked-mid-wave test that bypassed BatchGuard::drop
        // would leak as a spurious fire on the next wave on the same
        // thread (no refcount damage). The panic-discard path in
        // BatchGuard::drop and the wave-success drain together
        // guarantee pending_fires is empty by wave end; relying on
        // that invariant matches the pre-refactor lifecycle.
        //
        // Intentionally NOT clearing wave_cache_snapshots /
        // deferred_handle_releases / pending_notify here — those hold
        // retains and need a binding to release. Documented invariant:
        // they're empty by outermost wave start.

        // Sub-slice 3 (2026-05-09; /qa F2 partially reverted 2026-05-10):
        // defensively clear the OnInvalidate dedup set on outermost-wave
        // entry. Holds no retains; a stale entry from a prior
        // panicked-mid-wave test that bypassed BatchGuard::drop would
        // only suppress the OnInvalidate cleanup hook for that node on
        // the next wave (no refcount damage). Clearing matches the
        // tier3 defensive-clear precedent.
        //
        // `currently_firing` was reverted to CoreState (per /qa F2 — the
        // per-thread placement silently bypassed the cross-thread P13
        // partition-migration check); its defensive clear lives in
        // `CoreState::clear_wave_state` (which BatchGuard::drop runs
        // wave-end on both success and panic paths).
        ws.invalidate_hooks_fired_this_wave.clear();
        // Intentionally NOT clearing deferred_flush_jobs /
        // deferred_cleanup_hooks / pending_wipes here — by invariant
        // they're empty at outermost wave start (drained on success
        // by drain_deferred → fire_deferred; drained on panic by
        // BatchGuard::drop's panic branch). Pre-clearing would race a
        // hypothetical wave that staged into them OUTSIDE in_tick
        // (none does today, but matching the deferred_handle_releases
        // / pending_notify discipline keeps the invariant uniform).
    });
}

// §7 (D208–D211): the per-thread `PARTITION_CACHE` is DELETED. It existed
// to amortize the union-find `compute_touched_partitions` BFS + registry
// epoch round-trips across repeated same-seed emits. With static
// user-declared scheduling groups there is no epoch and the
// touched-group walk resolves group `Arc`s under a single uncontended
// `group_locks` lock (and is entirely skipped for the all-`None` floor),
// so the cache (and its ABA-avoidance generation keying) is unnecessary.

/// Has `node` emitted a tier-3 (DATA / RESOLVED) message in the current
/// wave on this thread? See [`TIER3_EMITTED_THIS_WAVE`] for the per-thread
/// wave-scope rationale.
fn tier3_check(node: NodeId) -> bool {
    TIER3_EMITTED_THIS_WAVE.with(|s| s.borrow().contains(&node))
}

/// Mark `node` as having emitted a tier-3 message in the current wave on
/// this thread. Idempotent. See [`TIER3_EMITTED_THIS_WAVE`].
fn tier3_mark(node: NodeId) {
    TIER3_EMITTED_THIS_WAVE.with(|s| {
        s.borrow_mut().insert(node);
    });
}

/// Wave-end clear of the per-thread tier3 tracker. Called from the
/// OUTERMOST [`BatchGuard::drop`] on this thread (both success and
/// panic-discard paths). Inner non-owning BatchGuard drops MUST NOT
/// invoke this — the outer wave is still in flight and inner-wave marks
/// are part of the outer wave's Slice G coalescing state.
fn tier3_clear() {
    TIER3_EMITTED_THIS_WAVE.with(|s| {
        s.borrow_mut().clear();
    });
}

/// Deferred sink-fire jobs collected during `flush_notifications`. Each
/// entry pairs a snapshot of the sink Arcs to fire with the messages to
/// deliver to them — one entry per (node × phase) cell with non-empty
/// content. Drained from `CoreState` and fired lock-released.
pub(crate) type DeferredJobs = Vec<(Vec<Sink>, Vec<Message>)>;

/// Lock-released drain payload of the wave's BatchGuard:
/// `(sink_jobs, handle_releases, OnInvalidate cleanup hooks, pending wipe_ctx fires)`.
/// Returned by [`Core::drain_deferred`], consumed by [`Core::fire_deferred`].
/// Sliced into a type alias to satisfy `clippy::type_complexity`.
pub(crate) type WaveDeferred = (
    DeferredJobs,
    Vec<HandleId>,
    Vec<(crate::handle::NodeId, crate::boundary::CleanupTrigger)>,
    Vec<crate::handle::NodeId>,
);

/// One subscriber-snapshot epoch within a node's wave-end notification
/// queue. A `PendingBatch` is opened the first time `queue_notify` runs
/// for the node in a wave, and a fresh batch is opened whenever the node's
/// `subscribers_revision` advances mid-wave (a new sink subscribes, an
/// existing sink unsubscribes, or a handshake-time panic evicts an
/// orphaned sink). All messages within one batch flush to the same sink
/// list — the snapshot taken when the batch opened, frozen against
/// subsequent revision bumps.
pub(crate) struct PendingBatch {
    /// `NodeRecord::subscribers_revision` value at the moment this batch
    /// opened. Used by `queue_notify` to decide append-to-last-batch vs
    /// open-fresh-batch on every push.
    pub(crate) snapshot_revision: u64,
    /// Subscriber snapshot frozen at batch-open time. SmallVec<[_; 1]>
    /// inlines the common single-subscriber case (avoids heap alloc for
    /// the dominant 1-sink-per-node pattern in most reactive graphs).
    pub(crate) sinks: SmallVec<[Sink; 1]>,
    /// Messages queued to this batch. SmallVec<[_; 3]> inlines the
    /// common per-node-per-wave message set (DIRTY + DATA + optional
    /// RESOLVED) without heap allocation.
    pub(crate) messages: SmallVec<[Message; 3]>,
}

/// Per-node wave-end notification queue, structured as one or more
/// subscriber-snapshot epochs (`batches`). The common case (no
/// mid-wave subscribe / unsubscribe at this node) keeps a single
/// inline batch — `SmallVec<[_; 1]>` keeps that allocation-free.
///
/// **Slice X4 / D2 (2026-05-08):** the prior shape was a single
/// `(sinks, messages)` pair per node — the snapshot froze on first
/// `queue_notify` and was reused for every subsequent emit to the same
/// node in the wave. That caused the documented late-subscriber +
/// multi-emit-per-wave gap (R1.3.5.a divergence): a sub installed
/// between two emits to the same node was invisible to the second
/// emit's flush slice. The revision-tracked batch list resolves it —
/// late subs land in a fresh batch that frozenly carries them, while
/// pre-subscribe batches retain their original snapshot so the new
/// sub doesn't double-receive earlier emits via flush AND handshake.
pub(crate) struct PendingPerNode {
    pub(crate) batches: SmallVec<[PendingBatch; 1]>,
}

impl PendingPerNode {
    /// Iterate every queued message for this node across all batches in
    /// arrival order. Used by R1.3.3.a invariant assertions and the
    /// auto-resolve / Slice-G coalescing tier-3-presence checks, which
    /// reason about wave-content per node, not per batch.
    pub(crate) fn iter_messages(&self) -> impl Iterator<Item = &Message> + '_ {
        self.batches.iter().flat_map(|b| b.messages.iter())
    }

    /// Mutable counterpart for `iter_messages`. Used by
    /// `rewrite_prior_resolved_to_data` to in-place rewrite Resolved
    /// entries to Data when a wave detects a multi-emit case after the
    /// fact.
    pub(crate) fn iter_messages_mut(&mut self) -> impl Iterator<Item = &mut Message> + '_ {
        self.batches.iter_mut().flat_map(|b| b.messages.iter_mut())
    }
}

/// RAII helper for the A6 reentrancy guard (Slice F, 2026-05-07).
///
/// Pushes `node_id` onto [`WaveState::currently_firing`] on construction,
/// pops it on Drop. [`Core::set_deps`] consults the stack and rejects
/// `set_deps(N, ...)` from inside N's own fn-fire with
/// [`crate::node::SetDepsError::ReentrantOnFiringNode`] — closing the
/// D1 hazard where Phase-1's snapshot of `dep_handles` would refer to
/// a different dep ordering than Phase-3's `tracked` storage.
///
/// Wraps the lock-released `invoke_fn` (and operator-equivalent FFI
/// callbacks like `project_each` / `predicate_each`). Drop fires even
/// on panic, so the stack stays balanced under user-fn unwinds.
///
/// Membership semantics (NOT strict LIFO): the only consumer of
/// `currently_firing` is `Core::set_deps`'s reentrancy check, which uses
/// `contains(&n)` — a set-membership test. Drop pops the right-most
/// matching `node_id` via `rposition` + `swap_remove`. For a stack like
/// `[A, B, A]` (A's fn re-enters B, B's fn re-enters A), B's drop pops
/// the SECOND A (index 1) via swap_remove, leaving `[A, A]` — the
/// physical order of the remaining As may not match construction order,
/// but membership is preserved. If a future call site needs strict LIFO
/// (e.g. "pop the most recently fired node"), switch to `pop()` + assert
/// the popped value equals `self.node_id`. (QA A6, 2026-05-07)
// D221 (F-b) floor-hardening, 2026-05-17: holds `&'a Core`, NOT an
// owned `core.clone()` (Core is no longer `Clone`). D246/S2c: the
// `group_locks`/`global_wave` Arcs are deleted; the lock-free
// single-owner `RefCell` floor pays zero per-fn-fire Arc tax. Both
// construction sites
// (`fire_regular` batch.rs:~1148, `fire_operator` batch.rs:~1719) are
// locals in a `&self` method, so the `self: &Core` borrow strictly
// outlives the guard — the lifetime is sound by construction (no
// escape, dropped within the same method). Removing the clone here also
// stops S2's `LockedCell` deletion from being able to reintroduce the
// tax.
pub(crate) struct FiringGuard<'a> {
    core: &'a Core,
    node_id: NodeId,
}

impl<'a> FiringGuard<'a> {
    pub(crate) fn new(core: &'a Core, node_id: NodeId) -> Self {
        // /qa F2 reverted (2026-05-10): currently_firing moved BACK to
        // CoreState (cross-thread visible, restoring the D091 P13 check).
        // Push under the state lock scope.
        {
            let mut s = core.lock_state();
            s.shared.currently_firing.push(node_id);
        }
        Self { core, node_id }
    }
}

impl Drop for FiringGuard<'_> {
    fn drop(&mut self) {
        // /qa F2 reverted (2026-05-10): currently_firing moved BACK to
        // CoreState. Pop under state lock.
        {
            let mut s = self.core.lock_state();
            // Pop the right-most matching node_id (membership semantics —
            // not strict LIFO). If absent, an external rebalance already
            // popped — silent no-op (panic-in-Drop is poison).
            if let Some(pos) = s
                .shared
                .currently_firing
                .iter()
                .rposition(|n| *n == self.node_id)
            {
                s.shared.currently_firing.swap_remove(pos);
            }
        }
    }
}

/// Borrow the per-operator scratch slot as `&T`. Panics if the slot is
/// uninitialized or the contained type doesn't match `T` — both are
/// invariant violations for any `fire_op_*` helper that should only be
/// called from `fire_operator`'s match arm for the matching variant.
fn scratch_ref<T: crate::op_state::OperatorScratch>(s: &CoreState, node_id: NodeId) -> &T {
    s.require_node(node_id)
        .op_scratch
        .as_ref()
        .expect("op_scratch slot uninitialized for operator node")
        .as_any_ref()
        .downcast_ref::<T>()
        .expect("op_scratch type mismatch")
}

/// Mutable borrow of the per-operator scratch slot. Same invariants as
/// [`scratch_ref`].
fn scratch_mut<T: crate::op_state::OperatorScratch>(s: &mut CoreState, node_id: NodeId) -> &mut T {
    s.require_node_mut(node_id)
        .op_scratch
        .as_mut()
        .expect("op_scratch slot uninitialized for operator node")
        .as_any_mut()
        .downcast_mut::<T>()
        .expect("op_scratch type mismatch")
}

impl Core {
    // -------------------------------------------------------------------
    // Wave entry + drain
    // -------------------------------------------------------------------

    /// Wave entry. The caller passes a closure that performs the wave's
    /// triggering operation (`commit_emission`, `terminate_node`, etc.).
    /// The closure runs lock-released; closure-internal Core methods
    /// acquire the state lock as they go.
    ///
    /// **Implementation:** delegates to [`Self::begin_batch`] for the
    /// wave's RAII lifecycle. The returned `BatchGuard` claims `in_tick`
    /// (`Core::generation`-keyed) and on drop runs the drain + flush +
    /// sink-fire phases — OR, if the closure panicked, the
    /// panic-discard path that restores cache snapshots and clears
    /// in_tick. (S4/D248: the `wave_owner` re-entrant mutex is deleted —
    /// single-owner `!Send` Core, one uninterrupted owner-side drain.)
    /// This unification gives `run_wave` the same panic-safety
    /// guarantee as the user-facing `Core::batch`.
    ///
    /// **Re-entrance:** a closure invoked from inside another wave — the
    /// inner `run_wave`'s `begin_batch` observes `in_tick=true`, the
    /// returned guard is non-owning (`owns_tick=false`), drop is a no-op.
    /// The outer wave's drain picks up the inner closure's queued work.
    ///
    /// **Lock-release discipline (Slice A close, M1):** all binding-side
    /// callbacks except the subscribe-time handshake fire lock-released.
    /// Sinks / user fns / custom-equals oracles that re-enter Core via
    /// the owner-side mailbox/`DeferQueue` seam run a nested wave. The
    /// one owner thread runs the in-flight drain to quiescence before
    /// `emit` returns — preserving the user-facing "emit returning means
    /// subscribers have observed" contract (no cross-thread lock needed;
    /// there is no cross-thread emitter).
    /// Wave entry with a known `seed` node. Acquires only the partitions
    /// transitively touched from `seed` (downstream cascade via
    /// `s.children` + R1.3.9.d meta-companion cascade) instead of every
    /// current partition. The canonical Y1 parallelism win for per-seed
    /// entry points (`Core::emit`, `Core::subscribe`'s activation,
    /// `Core::pause` / `Core::resume` / `Core::invalidate` / `Core::complete`
    /// / `Core::error` / `Core::teardown` / `Core::set_deps`'s
    /// push-on-subscribe).
    ///
    /// S4/D248: single-owner `!Send + !Sync` Core — one uninterrupted
    /// owner-side drain per wave; the deleted per-partition `wave_owner`
    /// `ReentrantMutex` parallelism is replaced by host-native
    /// concurrency across *independent per-worker Cores* (actor model).
    /// The "emit returning means subscribers have observed" contract
    /// holds because the one owner thread drains to quiescence before
    /// returning.
    ///
    /// Slice Y1 / Phase E (2026-05-08).
    pub(crate) fn run_wave_for<F>(&self, seed: crate::handle::NodeId, op: F)
    where
        F: FnOnce(&Self),
    {
        let _guard = self.begin_batch_for(seed);
        op(self);
    }

    /// Fallible wave entry. Returns `Err` if partition acquire violates
    /// ascending order (Phase H+ STRICT, D115). Used by `try_emit` /
    /// `try_complete` / `try_error`; the public `run_wave_for` calls
    /// `begin_batch_for` which panics on violation.
    pub(crate) fn try_run_wave_for<F>(
        &self,
        seed: crate::handle::NodeId,
        op: F,
    ) -> Result<(), crate::node::PartitionOrderViolation>
    where
        F: FnOnce(&Self),
    {
        let _guard = self.try_begin_batch_for(seed)?;
        op(self);
        Ok(())
    }

    /// Drain retains held by `wave_cache_snapshots` and return them so
    /// the caller can release them lock-released. Called from the
    /// wave-success path in [`BatchGuard::drop`].
    ///
    /// Q-beyond Sub-slice 1 (D108, 2026-05-09): the snapshots map moved
    /// to per-thread `WaveState`; signature takes `&mut WaveState`. The
    /// drain-and-release-lock-released discipline (introduced as /qa A1
    /// fix 2026-05-09 against the prior cross_partition mutex) carries
    /// over: caller drains under WaveState borrow + state lock, releases
    /// after both are dropped — `release_handle` may re-enter Core via
    /// finalizers and re-entry under either guard would deadlock /
    /// double-borrow.
    #[must_use]
    pub(crate) fn drain_wave_cache_snapshots(ws: &mut WaveState) -> Vec<HandleId> {
        if ws.wave_cache_snapshots.is_empty() {
            return Vec::new();
        }
        std::mem::take(&mut ws.wave_cache_snapshots)
            .into_values()
            .collect()
    }

    /// Restore cache slots from `wave_cache_snapshots` and clear the map.
    /// Called from the wave-abort path in `BatchGuard::drop` (panic).
    ///
    /// For each snapshotted node:
    ///
    /// 1. Read the current cache (the in-flight new value).
    /// 2. Set `cache = old_handle` (the snapshot's retained value).
    /// 3. Release the now-unowned current cache handle.
    ///
    /// Returns the list of "current" handles to release outside the lock.
    /// Q-beyond Sub-slice 1 (D108, 2026-05-09): the snapshots map moved
    /// to per-thread `WaveState`; signature takes both `s` (for cache
    /// slots) and `ws` (for the snapshots map).
    pub(crate) fn restore_wave_cache_snapshots(
        &self,
        s: &mut CoreState,
        ws: &mut WaveState,
    ) -> Vec<HandleId> {
        if ws.wave_cache_snapshots.is_empty() {
            return Vec::new();
        }
        let snapshots = std::mem::take(&mut ws.wave_cache_snapshots);
        let mut releases = Vec::with_capacity(snapshots.len());
        for (node_id, old_handle) in snapshots {
            let Some(rec) = s.nodes.get_mut(&node_id) else {
                releases.push(old_handle);
                continue;
            };
            let current = std::mem::replace(&mut rec.cache, old_handle);
            if current != NO_HANDLE {
                releases.push(current);
            }
        }
        releases
    }

    /// Drain pending fires until quiescent, then flush wave-end notifications
    /// to subscribers. Each fire iteration drops the state lock around the
    /// binding's `invoke_fn` callback so user fns may re-enter Core safely.
    ///
    /// `&self`-only — manages its own locking. Called from [`Self::run_wave`]
    /// and [`super::node::Core::activate_derived`] (via `run_wave`).
    pub(crate) fn drain_and_flush(&self) {
        let mut guard = 0u32;
        loop {
            // A′ (D232-AMEND): apply any producer-sink / timer
            // `MailboxOp`s **in-wave** at the top of every drain
            // iteration. Each applied op re-enters `Core::{emit,
            // complete,error}` with `in_tick = true` (non-owning batch
            // guard → it does NOT start its own drain; it queues into
            // this wave's `pending_fires`), so a sink that posted during
            // the previous `fire_fn` is picked up on the very next
            // iteration — immediate, in-wave, cascade-ordering-preserving
            // (consistent with the pre-S2b deferred-producer-op
            // drained-within-the-wave behaviour). Quiescence requires
            // BOTH `pending_fires` AND the mailbox empty.
            //
            // §7-floor guard (D-reflect, 2026-05-17): gate on the cheap
            // `runnable` atomic (one `Acquire` load) BEFORE touching the
            // mailbox `parking_lot::Mutex`. A no-producer / no-timer wave
            // (the `identity_dedup` ≈508 ns floor path) never posts, so
            // `runnable` stays `false` and this costs one relaxed-ish
            // atomic load per drain iteration — NOT a mutex lock + deque
            // pop. **Sink-side** posts are same-thread, in-wave: the
            // `runnable` Release precedes this Acquire on the same
            // thread, so no in-wave op is missed. **Task-side** posts
            // (timer/temporal `tokio::spawn` tasks) are cross-thread:
            // their happens-before is the `ops` `parking_lot::Mutex`
            // (acquire/release), NOT this atomic — `runnable` is only an
            // advisory fast-path bit there, and a racing task post is
            // caught by the embedder pump's *unconditional*
            // `drain_mailbox()` (timer tasks drain at the pump, not this
            // in-wave gate). Do NOT remove the unconditional pump drain.
            // This is exactly the per-group wake bit S4 wires to the
            // host executor — doing it here now is coherent, not
            // throwaway.
            if self.mailbox.is_runnable() || self.deferred.is_runnable() {
                self.drain_mailbox();
            }

            // R1.3.8.c (Slice F, A3): if no fires are pending but there are
            // queued pause-overflow ERRORs, synthesize them now. The
            // resulting ERROR cascade may add to pending_fires (children
            // settling their terminal state), so we loop back to drain.
            //
            // Q-beyond Sub-slice 1 + 2 (D108, 2026-05-09): pending_fires
            // and pending_pause_overflow both live on per-thread
            // WaveState. State lock no longer required for either read.
            let synth_pending = with_wave_state(|ws| {
                if ws.pending_fires.is_empty() && !ws.pending_pause_overflow.is_empty() {
                    std::mem::take(&mut ws.pending_pause_overflow)
                } else {
                    Vec::new()
                }
            });
            for entry in synth_pending {
                // Lock-released call to the binding hook. Default impl
                // returns None — the binding has opted out of R1.3.8.c
                // and we fall back to silent-drop + ResumeReport.dropped.
                let handle = self.binding.synthesize_pause_overflow_error(
                    entry.node_id,
                    entry.dropped_count,
                    entry.configured_max,
                    entry.lock_held_ns / 1_000_000,
                );
                if let Some(h) = handle {
                    // Re-enter Core::error to terminate the node and
                    // cascade. We're inside a wave (`in_tick = true`),
                    // so error() gets a non-owning batch guard — it
                    // doesn't try to start its own drain. The cascade
                    // queues into our outer drain via pending_fires
                    // and pending_notify.
                    self.error(entry.node_id, h);
                }
            }

            // Pick next fire under a short lock. Also re-read the configured
            // drain cap so callers can tune via `Core::set_max_batch_drain_iterations`
            // without restarting waves mid-flight.
            //
            // Q-beyond Sub-slice 2 (D108, 2026-05-09): pending_fires lives
            // on per-thread WaveState; pick_next_fire takes both state and
            // WaveState. The pending_size diagnostic and emptiness check
            // also read WaveState. Borrow scopes are split: WaveState
            // borrow drops before fire_fn runs (which re-borrows WaveState
            // via fire_regular / fire_operator).
            let (next, cap, pending_size) = {
                let s = self.lock_state();
                let cap = s.shared.max_batch_drain_iterations;
                let (next, pending_size) = with_wave_state(|ws| {
                    if ws.pending_fires.is_empty() {
                        return (None, 0);
                    }
                    let size = ws.pending_fires.len();
                    let next = Self::pick_next_fire(&s, ws);
                    (next, size)
                });
                (next, cap, pending_size)
            };
            if pending_size == 0 {
                // QA F1 (2026-05-18): quiescence requires BOTH
                // `pending_fires` AND the mailbox empty — the asserted
                // invariant the old `if pending_size == 0 { break }`
                // did NOT enforce. If the mailbox still holds work,
                // `continue` so the next iteration's top-of-loop
                // `is_runnable()`-gated `drain_mailbox()` applies it
                // (an applied op either cascades into `pending_fires`
                // or, if terminal/no-op, leaves the mailbox empty so
                // the *next* check breaks). §7 floor: no producer/timer
                // ⇒ never posted ⇒ `is_runnable()` false ⇒ breaks on the
                // first empty `pending_fires` (one atomic load — already
                // in the floor budget).
                // QA P3 (2026-05-18): the mailbox-continue does NOT
                // count against the fire-cascade `cap`. `drain_mailbox`
                // already drains the FIFO to quiescence in ONE call
                // (re-posts during `apply` are popped by the same
                // `drain_into` loop), and the self-reposting-`Defer`
                // livelock is bounded INSIDE `drain_into` (its own
                // `max_ops`). Reaching here `is_runnable()` again means
                // genuinely-new cross-thread (timer) work — bounded by
                // external input, not a fire cycle — so counting it
                // against the fire `cap` would false-trip a production
                // panic on heavy producer/timer graphs.
                if self.mailbox.is_runnable() || self.deferred.is_runnable() {
                    continue;
                }
                break;
            }
            guard += 1;
            assert!(
                guard < cap,
                "wave drain exceeded {cap} iterations \
                 (pending_fires={pending_size}). Most likely cause: a runtime \
                 cycle introduced by an operator that re-arms its own pending_fires \
                 slot from inside `invoke_fn` (e.g. a producer that subscribes to \
                 itself, or a fn that calls Core::emit on a node whose fn fires \
                 the original node again). Structural cycles via set_deps are \
                 rejected at edge-mutation time. Tune via Core::set_max_batch_drain_iterations \
                 only with concrete evidence the workload needs more iterations."
            );
            let Some(next) = next else { break };
            // fire_fn manages its own locking around invoke_fn.
            self.fire_fn(next);
        }
        // Auto-resolve sweep: nodes registered in pending_auto_resolve
        // by the RESOLVED child propagation need a Resolved if they didn't
        // fire and settle via their own commit_emission. Check pending_notify
        // for each candidate — if it has Dirty but no tier-3+ message, the
        // node never settled and needs auto-Resolved. Route through
        // queue_notify so paused nodes get the Resolved into their pause
        // buffer.
        let mut s = self.lock_state();
        // Q-beyond Sub-slice 1 + 2 (D108, 2026-05-09): pending_auto_resolve
        // and pending_notify both live on per-thread WaveState. /qa A5
        // fix (2026-05-09): explicit scope for the WaveState borrow so
        // it drops BEFORE the for-loop. Inside the loop, `queue_notify`
        // re-borrows WaveState for `pending_pause_overflow.push` /
        // `pending_notify` writes — re-entrance on RefCell::borrow_mut
        // would panic. Explicit scope makes the lifetime load-bearing.
        let candidates = with_wave_state(|ws| std::mem::take(&mut ws.pending_auto_resolve));
        for node_id in candidates {
            let needs_resolve = with_wave_state(|ws| {
                ws.pending_notify
                    .get(&node_id)
                    .is_some_and(|entry| !entry.iter_messages().any(|m| m.tier() >= 3))
            });
            if needs_resolve {
                self.queue_notify(&mut s, node_id, Message::Resolved);
            }
        }
        // Final flush phase — populates deferred_flush_jobs
        // from pending_notify (already carries per-node sink snapshots).
        self.flush_notifications(&mut s);
    }

    /// Pick the pending node with the lowest topological rank.
    ///
    /// Nodes with lower `topo_rank` have no transitive upstream in
    /// `pending_fires` (by construction — `topo_rank = 1 + max dep rank`).
    /// This is O(|pending_fires|) instead of the prior O(N·V) BFS.
    /// §10 perf optimization (D047, Slice U).
    ///
    /// Q-beyond Sub-slice 2 (D108, 2026-05-09): `pending_fires` lives on
    /// per-thread `WaveState`. Caller passes `&WaveState` alongside
    /// `&CoreState` so the borrow scopes stay disjoint and visible.
    fn pick_next_fire(s: &CoreState, ws: &WaveState) -> Option<NodeId> {
        ws.pending_fires
            .iter()
            .copied()
            .min_by_key(|&id| s.nodes.get(&id).map_or(0, |r| r.topo_rank))
    }

    /// Wave drain entry point. Dispatches via `rec.op` to either the
    /// regular fn-fire path ([`Self::fire_regular`]) or the operator
    /// dispatch ([`Self::fire_operator`]).
    pub(crate) fn fire_fn(&self, node_id: NodeId) {
        let op = {
            let s = self.lock_state();
            s.nodes.get(&node_id).and_then(|r| r.op)
        };
        match op {
            Some(operator_op) => self.fire_operator(node_id, operator_op),
            None => {
                // State / Derived / Dynamic / Producer all dispatch via fn_id.
                self.fire_regular(node_id);
            }
        }
    }

    /// Fire a node's fn lock-released around `invoke_fn`.
    ///
    /// Phase 1 (lock-held): remove from pending_fires, snapshot fn_id +
    /// dep_records → DepBatch + kind. Skip if terminal, first-run-gate-closed,
    /// or stateless.
    ///
    /// Phase 2 (lock-released): call `BindingBoundary::invoke_fn`. User fn
    /// callbacks may re-enter Core (`emit`, `pause`, etc.) and run a nested
    /// wave — the in_tick gate composes naturally because nested calls
    /// observe `in_tick = true` and skip their own drain.
    ///
    /// Phase 3 (lock-held): mark `has_fired_once`, store dynamic-tracked,
    /// decide between Noop+RESOLVED, single Data, or Batch.
    ///
    /// Phase 4: commit emissions. Single Data goes through
    /// `commit_emission` (with equals substitution). Batch emissions are
    /// processed in sequence — Data via `commit_emission_verbatim` (no
    /// equals substitution per R1.3.2.d / R1.3.3.c), Complete/Error via
    /// terminal cascade.
    #[allow(clippy::too_many_lines)] // Slice G added Noop / Batch tier-3 guards
    fn fire_regular(&self, node_id: NodeId) {
        enum FireAction {
            None,
            SingleData(HandleId),
            Batch(SmallVec<[FnEmission; 2]>),
        }

        // Phase 1: snapshot inputs — build DepBatch per dep from dep_records.
        // `has_fired_once` is captured here for the Slice E2 OnRerun gate
        // (Phase 1.5 below): the cleanup hook only fires when the fn has
        // run at least once already in this activation cycle.
        let prep: Option<(crate::handle::FnId, Vec<DepBatch>, bool, bool, bool)> = {
            let s = self.lock_state();
            // Q-beyond Sub-slice 2 (D108, 2026-05-09): pending_fires lives
            // on per-thread WaveState. Removed via with_wave_state — no
            // re-entry concern because only the immediate remove happens
            // under the borrow.
            with_wave_state(|ws| {
                ws.pending_fires.remove(&node_id);
            });
            let rec = s.require_node(node_id);
            // Skip: terminal, first-run-gate-closed (R2.5.3 / R5.4 — partial
            // mode opts out of the gate per D011), or stateless.
            if rec.terminal.is_some() || (!rec.partial && rec.has_sentinel_deps()) {
                None
            } else {
                rec.fn_id.map(|fn_id| {
                    let use_mask = rec.dep_records.len() <= 64;
                    let mask = rec.involved_mask;
                    let dep_batches: Vec<DepBatch> = rec
                        .dep_records
                        .iter()
                        .enumerate()
                        .map(|(i, dr)| DepBatch {
                            data: dr.data_batch.clone(),
                            prev_data: dr.prev_data,
                            // §10.3 perf (Slice V1): derive from bitmask
                            // for ≤64 deps; fall back to per-dep field.
                            involved: if use_mask {
                                (mask >> i) & 1 != 0
                            } else {
                                dr.involved_this_wave
                            },
                        })
                        .collect();
                    (
                        fn_id,
                        dep_batches,
                        rec.is_dynamic,
                        rec.has_fired_once,
                        rec.is_producer(),
                    )
                })
            }
        };
        let Some((fn_id, dep_batches, is_dynamic, has_fired_once, is_producer)) = prep else {
            return;
        };

        // Phase 1.5 (Slice E2 — R2.4.5 OnRerun, lock-released per D045): if
        // the fn has fired at least once in this activation cycle, fire its
        // OnRerun cleanup hook BEFORE the next invoke_fn re-allocates fn-
        // local resources. First-fire is intentionally skipped — there is
        // no prior run to clean up. Fires OUTSIDE `FiringGuard` because
        // cleanup re-entrance is not the A6 reentrancy concern (which
        // protects against `set_deps(self, ...)` from inside the in-flight
        // invoke_fn). Operator nodes never reach this path (`fire_regular`
        // is the fn-id branch of `fire_fn`; operators dispatch via
        // `fire_operator`), so cleanup hooks correctly only fire for fn-
        // shaped nodes (state / derived / dynamic / producer).
        if has_fired_once {
            self.binding
                .cleanup_for(node_id, crate::boundary::CleanupTrigger::OnRerun);
        }

        // Phase 2: invoke fn lock-released. A6 reentrancy guard is scoped to
        // the FFI call only — Phase 3's lock-held state mutation is not part
        // of "currently firing" because set_deps would already block on the
        // state lock by then. Drop on the guard pops the stack even if
        // invoke_fn panics, keeping `currently_firing` balanced.
        //
        // D246 rule 5 / D245 (QA D1) — the owner-side full-`Core` facade
        // hand-off is needed ONLY by producer-building bindings (to
        // construct `ProducerCtx` from a real Core surface here without a
        // thread-local / `Core` clone / stored back-ref — all β-invalid
        // under the actor model). Branch on node kind so the hot
        // derived/dynamic/state path keeps the single parameterless
        // `invoke_fn` virtual call it always had (no `&dyn CoreFull`
        // fat-pointer coercion, no default-body re-dispatch) — byte-
        // identical to pre-D246, zero §7-floor regression. Only the rare
        // producer-build fire pays the facade hand-off. `self: &Core`
        // unsized-coerces to `&dyn CoreFull` (`Core: CoreFull`).
        let result = {
            let _firing = FiringGuard::new(self, node_id);
            if is_producer {
                self.binding
                    .invoke_fn_with_core(node_id, fn_id, &dep_batches, self)
            } else {
                self.binding.invoke_fn(node_id, fn_id, &dep_batches)
            }
        };

        // Phase 3: apply result under the lock — defensive terminal check
        // (a sibling cascade may have terminated this node during phase 2).
        let action: FireAction = {
            let mut s = self.lock_state();
            // Defensive: node may have terminated mid-phase-2 via a sibling
            // cascade (a fn that re-entered `Core::error` on a path that
            // cascaded here). If so, release any payload handles and no-op.
            if s.require_node(node_id).terminal.is_some() {
                match &result {
                    FnResult::Data { handle, .. } => {
                        self.binding.release_handle(*handle);
                    }
                    FnResult::Batch { emissions, .. } => {
                        for em in emissions {
                            match em {
                                FnEmission::Data(h) | FnEmission::Error(h) => {
                                    self.binding.release_handle(*h);
                                }
                                FnEmission::Complete => {}
                            }
                        }
                    }
                    FnResult::Noop { .. } => {}
                }
                return;
            }
            let rec = s.require_node_mut(node_id);
            rec.has_fired_once = true;
            if is_dynamic {
                let tracked = match &result {
                    FnResult::Data { tracked, .. }
                    | FnResult::Noop { tracked }
                    | FnResult::Batch { tracked, .. } => tracked.clone(),
                };
                if let Some(t) = tracked {
                    rec.tracked = t.into_iter().collect();
                }
            }
            match result {
                FnResult::Noop { .. } => {
                    // Slice G: skip Resolved if a prior emission in the same
                    // wave already queued tier-3 (would violate R1.3.3.a).
                    // Q-beyond Sub-slice 2 (D108, 2026-05-09): pending_notify
                    // lives on per-thread WaveState. Borrow scoped to the
                    // tier3 read so queue_notify (which re-borrows
                    // WaveState) doesn't double-borrow.
                    let already_dirty = s.require_node(node_id).dirty;
                    let already_tier3 = with_wave_state(|ws| {
                        ws.pending_notify
                            .get(&node_id)
                            .is_some_and(|entry| entry.iter_messages().any(|m| m.tier() == 3))
                    });
                    if already_dirty && !already_tier3 {
                        self.queue_notify(&mut s, node_id, Message::Resolved);
                    }
                    FireAction::None
                }
                FnResult::Data { handle, .. } => FireAction::SingleData(handle),
                FnResult::Batch { emissions, .. } if emissions.is_empty() => {
                    // Empty Batch is equivalent to Noop — settle with
                    // RESOLVED if the node was dirty (R1.3.1.a). Slice G:
                    // skip if a prior emission already queued tier-3.
                    // Q-beyond Sub-slice 2 (D108, 2026-05-09): see Noop
                    // arm above for the WaveState borrow scope rationale.
                    let already_dirty = s.require_node(node_id).dirty;
                    let already_tier3 = with_wave_state(|ws| {
                        ws.pending_notify
                            .get(&node_id)
                            .is_some_and(|entry| entry.iter_messages().any(|m| m.tier() == 3))
                    });
                    if already_dirty && !already_tier3 {
                        self.queue_notify(&mut s, node_id, Message::Resolved);
                    }
                    FireAction::None
                }
                FnResult::Batch { emissions, .. } => FireAction::Batch(emissions),
            }
        };

        // Phase 4: commit emissions.
        match action {
            FireAction::None => {}
            // Single Data — equals substitution applies (R1.3.2).
            FireAction::SingleData(handle) => {
                self.commit_emission(node_id, handle);
            }
            // Batch — process in sequence. No equals substitution
            // (R1.3.2.d / R1.3.3.c: multi-message waves pass verbatim).
            FireAction::Batch(emissions) => {
                self.commit_batch(node_id, emissions);
            }
        }
    }

    /// Process a `FnResult::Batch` emissions sequence. Each `Data` goes
    /// through `commit_emission_verbatim` (no equals substitution per
    /// R1.3.2.d / R1.3.3.c). Terminal emissions (`Complete` / `Error`)
    /// cascade per R1.3.4; processing stops at the first terminal and
    /// remaining handles are released (R1.3.4.a: no further messages
    /// after terminal).
    fn commit_batch(&self, node_id: NodeId, emissions: SmallVec<[FnEmission; 2]>) {
        let mut iter = emissions.into_iter();
        for em in iter.by_ref() {
            match em {
                FnEmission::Data(handle) => {
                    self.commit_emission_verbatim(node_id, handle);
                }
                FnEmission::Complete => {
                    self.complete(node_id);
                    break;
                }
                FnEmission::Error(handle) => {
                    self.error(node_id, handle);
                    break;
                }
            }
        }
        // Release handles from any emissions after the terminal break.
        for em in iter {
            match em {
                FnEmission::Data(h) | FnEmission::Error(h) => {
                    self.binding.release_handle(h);
                }
                FnEmission::Complete => {}
            }
        }
    }

    // -------------------------------------------------------------------
    // Emission commit — equals-substitution lives here
    // -------------------------------------------------------------------

    /// Apply a node's emission. `&self`-only; brackets the equals check
    /// around a lock release so `BindingBoundary::custom_equals` can re-enter
    /// Core safely.
    ///
    /// Phase 1 (lock-held): defensive terminal short-circuit; snapshot
    /// equals_mode + old cache handle.
    ///
    /// Phase 2 (lock-released): call `handles_equal` — `EqualsMode::Identity`
    /// is a pure `u64` compare with no boundary call; `EqualsMode::Custom`
    /// crosses to the binding's `custom_equals` oracle, which may re-enter
    /// Core.
    ///
    /// Phase 3 (lock-held): set cache, queue Dirty + Data/Resolved into
    /// pending_notify (which snapshots subscribers on first touch),
    /// propagate to children.
    // Q2 / Q3 (2026-05-09) tipped past clippy's 100-line threshold; the
    // function is already a multi-phase wave-engine routine and breaking
    // out the four phases would obscure the lock-discipline.
    #[allow(clippy::too_many_lines)]
    pub(crate) fn commit_emission(&self, node_id: NodeId, new_handle: HandleId) {
        assert!(
            new_handle != NO_HANDLE,
            "NO_HANDLE is not a valid DATA payload (R1.2.4) for node {node_id:?}",
        );

        // Phase 1: terminal short-circuit + snapshot equals/cache.
        let snapshot = {
            let s = self.lock_state();
            let rec = s.require_node(node_id);
            // (§7-D: the throwaway `bench_state_collapse` relocated
            // is_state/producer assert was removed — the normal-path
            // validation in `Core::emit` is retained; the
            // commit_emission single-pass collapse is deferred, §7-A.)
            if rec.terminal.is_some() {
                drop(s);
                self.binding.release_handle(new_handle);
                return;
            }
            (rec.cache, rec.equals)
        };
        let (old_handle, equals_mode) = snapshot;

        // Slice G (2026-05-07): R1.3.2.d says equals substitution only
        // fires for SINGLE-DATA waves at one node. Detect "this is a
        // subsequent emit in the same wave at this node" via the
        // per-thread `TIER3_EMITTED_THIS_WAVE` thread-local
        // (D1 patch, 2026-05-09 — moved off per-partition state to be
        // robust against mid-wave cross-thread `set_deps` partition
        // splits). If set → multi-emit wave: skip equals, queue Data
        // verbatim, retroactively rewrite any prior Resolved (queued by
        // an earlier same-value emit's equals match) to Data using the
        // wave-start cache snapshot. Outside batch / first emit:
        // standard per-emit equals path. Thread-local lookup is
        // ~5ns and lock-free.
        let is_subsequent_emit_in_wave = tier3_check(node_id);

        if is_subsequent_emit_in_wave {
            // Multi-emit wave detected. Skip equals, queue Data verbatim.
            // Also rewrite any prior Resolved entries to Data using the
            // wave-start cache snapshot.
            self.rewrite_prior_resolved_to_data(node_id);
            self.commit_emission_verbatim(node_id, new_handle);
            return;
        }

        // Phase 2: equals check (lock-released for Custom).
        let is_data = !self.handles_equal_lock_released(equals_mode, old_handle, new_handle);

        // Phase 3: apply emission under the lock. Defensive terminal
        // re-check — a concurrent cascade between phase 2 and phase 3
        // could have terminated the node.
        let mut s = self.lock_state();
        if s.require_node(node_id).terminal.is_some() {
            drop(s);
            self.binding.release_handle(new_handle);
            return;
        }

        // R1.3.1.a condition (b): synthesize DIRTY only if node not already
        // dirty from an earlier emission in the same wave.
        let already_dirty = s.require_node(node_id).dirty;
        s.require_node_mut(node_id).dirty = true;
        if !already_dirty {
            self.queue_notify(&mut s, node_id, Message::Dirty);
        }

        if is_data {
            // P3 (Slice A close /qa): re-read CURRENT cache. Same-thread
            // re-entry from a `custom_equals` oracle that called back into
            // `Core::emit` on this same node during phase 2's lock-released
            // equals check could have advanced the cache between phase 1's
            // snapshot (`old_handle`) and this point.
            let current_cache = s.require_node(node_id).cache;
            // Q-beyond Sub-slice 1 (D108, 2026-05-09): wave_cache_snapshots
            // lives on per-thread WaveState. `in_tick` is per-(Core,
            // thread) (`IN_TICK_OWNED`); this read is on the wave-owner
            // thread, so it observes this thread's own ownership.
            let in_tick = self.in_tick();
            let snapshot_taken = if in_tick && current_cache != NO_HANDLE {
                use std::collections::hash_map::Entry;
                with_wave_state(|ws| match ws.wave_cache_snapshots.entry(node_id) {
                    Entry::Vacant(slot) => {
                        slot.insert(current_cache);
                        true
                    }
                    Entry::Occupied(_) => false,
                })
            } else {
                false
            };
            s.require_node_mut(node_id).cache = new_handle;
            if current_cache != NO_HANDLE && !snapshot_taken {
                self.binding.release_handle(current_cache);
            }
            // Slice E1 (R2.6.5 / Lock 6.G): push DATA into the replay
            // buffer if the node opted in. RESOLVED entries are NOT
            // buffered (canonical "DATA only").
            self.push_replay_buffer(&mut s, node_id, new_handle);
            // Slice G (D1 patch, 2026-05-09): mark this node as having
            // emitted tier-3 in this wave on the per-thread tracker.
            tier3_mark(node_id);
            self.queue_notify(&mut s, node_id, Message::Data(new_handle));
            // Propagate to children
            let child_ids: Vec<NodeId> = s
                .children
                .get(&node_id)
                .map(|c| c.iter().copied().collect())
                .unwrap_or_default();
            for child_id in child_ids {
                let dep_idx = s.require_node(child_id).dep_index_of(node_id);
                if let Some(idx) = dep_idx {
                    self.deliver_data_to_consumer(&mut s, child_id, idx, new_handle);
                }
            }
        } else {
            // RESOLVED: handle unchanged. Don't release; old still in use.
            // Slice G: snapshot cache so a subsequent same-wave emit can
            // rewrite this Resolved to Data using the snapshot.
            // Q-beyond Sub-slice 1 (D108, 2026-05-09): wave_cache_snapshots
            // lives on per-thread WaveState. /qa F1 reverted (2026-05-10):
            // `in_tick` is per-(Core, thread); read on the wave-owner
            // thread (observes this thread's own ownership).
            let current_cache = s.require_node(node_id).cache;
            if self.in_tick() && current_cache != NO_HANDLE {
                use std::collections::hash_map::Entry;
                with_wave_state(|ws| {
                    if let Entry::Vacant(slot) = ws.wave_cache_snapshots.entry(node_id) {
                        self.binding.retain_handle(current_cache);
                        slot.insert(current_cache);
                    }
                });
            }
            // Slice G (D1 patch, 2026-05-09): mark this node as having
            // emitted tier-3 in this wave on the per-thread tracker.
            tier3_mark(node_id);
            self.queue_notify(&mut s, node_id, Message::Resolved);
            let child_ids: Vec<NodeId> = s
                .children
                .get(&node_id)
                .map(|c| c.iter().copied().collect())
                .unwrap_or_default();
            // /qa A7 fix (2026-05-09): collect auto-resolve inserts
            // during the loop and bulk-insert into pending_auto_resolve
            // under a SINGLE cross_partition acquire after the loop.
            // Pre-fix the loop acquired `cross_partition` once per
            // child via `self.lock_cross_partition().pending_auto_resolve.insert(...)`,
            // which is N mutex hops for an N-child cascade. Cannot
            // hoist to acquire-cps-before-loop because `queue_notify`
            // (called inside the loop) also acquires cross_partition
            // for `pending_pause_overflow.push` in the rare overflow
            // case — re-entrance on the non-reentrant Mutex would
            // self-deadlock.
            let mut auto_resolve_inserts: SmallVec<[NodeId; 4]> = SmallVec::new();
            for child_id in child_ids {
                let already_involved = s.require_node(child_id).involved_this_wave;
                if !already_involved {
                    {
                        let child = s.require_node_mut(child_id);
                        child.involved_this_wave = true;
                        child.dirty = true;
                    }
                    self.queue_notify(&mut s, child_id, Message::Dirty);
                    // Q2 (2026-05-09): pending_auto_resolve lives on
                    // CrossPartitionState. Deferred to after-loop
                    // bulk insert per the /qa A7 fix above.
                    auto_resolve_inserts.push(child_id);
                }
            }
            // /qa A7 (2026-05-09) — preserved post-Sub-slice-1 (D108):
            // single WaveState borrow for the bulk-insert. queue_notify
            // above no longer holds the WaveState borrow by the time we
            // reach here, so this borrow is uncontested.
            if !auto_resolve_inserts.is_empty() {
                with_wave_state(|ws| ws.pending_auto_resolve.extend(auto_resolve_inserts));
            }
        }
    }

    /// Slice G: when a multi-emit wave is detected at `node_id` (a second
    /// emit arrives while a prior tier-3 message is still pending), rewrite
    /// any `Resolved` entries from earlier emits to `Data(snapshot_cache)`
    /// so the wave conforms to R1.3.3.a (≥1 DATA OR exactly 1 RESOLVED).
    /// Touches both `pending_notify` (immediate-flush path) and the per-node
    /// pause buffer (paused path).
    fn rewrite_prior_resolved_to_data(&self, node_id: NodeId) {
        let mut s = self.lock_state();
        // Q-beyond Sub-slice 1 + 2 (D108, 2026-05-09): wave_cache_snapshots
        // and pending_notify both live on per-thread WaveState. Single
        // WaveState borrow handles both the snapshot lookup and the
        // pending_notify rewrite; the pause-buffer path uses the state
        // lock and is independent of WaveState.
        let snapshot = match with_wave_state(|ws| ws.wave_cache_snapshots.get(&node_id).copied()) {
            Some(h) if h != NO_HANDLE => h,
            // No snapshot available — the prior Resolved was queued without
            // a cache (sentinel pre-emit). Nothing to rewrite to; the
            // multi-emit case from sentinel is fine (verbatim Data path).
            _ => return,
        };
        let mut retains_needed = 0u32;
        // Pending_notify path. Walk all batches' messages — Slice-G
        // coalescing reasons about wave-content per node, not per-batch.
        with_wave_state(|ws| {
            if let Some(entry) = ws.pending_notify.get_mut(&node_id) {
                for msg in entry.iter_messages_mut() {
                    if matches!(msg, Message::Resolved) {
                        *msg = Message::Data(snapshot);
                        retains_needed += 1;
                    }
                }
            }
        });
        // Pause-buffer path.
        if let Some(rec) = s.nodes.get_mut(&node_id) {
            if let crate::node::PauseState::Paused { buffer, .. } = &mut rec.pause_state {
                for msg in &mut *buffer {
                    if matches!(msg, Message::Resolved) {
                        *msg = Message::Data(snapshot);
                        retains_needed += 1;
                    }
                }
            }
        }
        drop(s);
        // Each rewritten Resolved → Data adds a payload retain that
        // queue_notify would otherwise have taken at emit time. The
        // snapshot already owns one retain (taken when cache was
        // snapshotted); we need one fresh retain per rewrite.
        for _ in 0..retains_needed {
            self.binding.retain_handle(snapshot);
        }
    }

    /// Equals check that crosses the binding boundary lock-released for
    /// `EqualsMode::Custom`. Caller must NOT hold the state lock.
    fn handles_equal_lock_released(&self, mode: EqualsMode, a: HandleId, b: HandleId) -> bool {
        if a == b {
            return true; // identity-on-handles always sufficient
        }
        if a == NO_HANDLE || b == NO_HANDLE {
            return false;
        }
        match mode {
            EqualsMode::Identity => false,
            EqualsMode::Custom(handle) => self.binding.custom_equals(handle, a, b),
        }
    }

    /// Commit a DATA emission **without** equals substitution — used by
    /// `FnResult::Batch` processing where multi-message waves pass through
    /// verbatim per R1.3.2.d / R1.3.3.c. DIRTY auto-prefix respects
    /// R1.3.1.a condition (b): only queued if node not already dirty.
    ///
    /// Structurally identical to the DATA branch of [`Self::commit_emission`]
    /// but skips the Phase 2 equals check entirely.
    fn commit_emission_verbatim(&self, node_id: NodeId, new_handle: HandleId) {
        assert!(
            new_handle != NO_HANDLE,
            "NO_HANDLE is not a valid DATA payload (R1.2.4) for node {node_id:?}",
        );

        let mut s = self.lock_state();
        let rec = s.require_node(node_id);
        if rec.terminal.is_some() {
            drop(s);
            self.binding.release_handle(new_handle);
            return;
        }

        // R1.3.1.a condition (b): DIRTY only if not already dirty.
        let already_dirty = s.require_node(node_id).dirty;
        s.require_node_mut(node_id).dirty = true;
        if !already_dirty {
            self.queue_notify(&mut s, node_id, Message::Dirty);
        }

        // Always DATA — no equals substitution for Batch emissions.
        // Q-beyond Sub-slice 1 (D108, 2026-05-09): wave_cache_snapshots
        // lives on per-thread WaveState. /qa F1 reverted (2026-05-10):
        // `in_tick` is per-(Core, thread); read on the wave-owner thread.
        let current_cache = s.require_node(node_id).cache;
        let snapshot_taken = if self.in_tick() && current_cache != NO_HANDLE {
            use std::collections::hash_map::Entry;
            with_wave_state(|ws| match ws.wave_cache_snapshots.entry(node_id) {
                Entry::Vacant(slot) => {
                    slot.insert(current_cache);
                    true
                }
                Entry::Occupied(_) => false,
            })
        } else {
            false
        };
        s.require_node_mut(node_id).cache = new_handle;
        if current_cache != NO_HANDLE && !snapshot_taken {
            self.binding.release_handle(current_cache);
        }
        // Slice E1: replay buffer push (R2.6.5 / Lock 6.G).
        self.push_replay_buffer(&mut s, node_id, new_handle);
        // Slice G QA fix (A2, 2026-05-07) / D1 patch (2026-05-09): mark
        // tier3_emitted_this_wave on the per-thread tracker even on the
        // verbatim path. A subsequent commit_emission at the same node
        // in the same wave needs this flag to detect multi-emit and
        // skip equals substitution; without it, a Batch-then-standard
        // sequence would queue Resolved into a wave that already has
        // Data — violating R1.3.3.a. The Batch path itself still
        // passes verbatim per R1.3.3.c (we don't re-run equals here);
        // we just record that "this node has emitted tier-3 in this
        // wave."
        tier3_mark(node_id);
        self.queue_notify(&mut s, node_id, Message::Data(new_handle));
        // Propagate to children
        let child_ids: Vec<NodeId> = s
            .children
            .get(&node_id)
            .map(|c| c.iter().copied().collect())
            .unwrap_or_default();
        for child_id in child_ids {
            let dep_idx = s.require_node(child_id).dep_index_of(node_id);
            if let Some(idx) = dep_idx {
                self.deliver_data_to_consumer(&mut s, child_id, idx, new_handle);
            }
        }
    }

    /// Slice E1 (R2.6.5 / Lock 6.G): push a DATA handle into the node's
    /// replay buffer if opted in. Evicts oldest if cap exceeded; takes a
    /// fresh retain on push. RESOLVED is NOT buffered per canonical
    /// "DATA only" — call sites only invoke this for Data emissions.
    ///
    /// Evicted handle is queued into `cps.deferred_handle_releases`
    /// (released lock-released at flush time) per the binding-boundary
    /// lock-release discipline — `release_handle` may re-enter Core via
    /// finalizers and must not run while the state lock is held
    /// (QA A3, 2026-05-07). Q2 (2026-05-09): the queue moved to
    /// CrossPartitionState; this fn acquires `cross_partition` only
    /// when an eviction actually happens (the common case is no
    /// eviction → no second-mutex acquire).
    fn push_replay_buffer(&self, s: &mut CoreState, node_id: NodeId, new_handle: HandleId) {
        let rec = s.require_node_mut(node_id);
        let cap = match rec.replay_buffer_cap {
            Some(c) if c > 0 => c,
            _ => return,
        };
        self.binding.retain_handle(new_handle);
        rec.replay_buffer.push_back(new_handle);
        let evicted = if rec.replay_buffer.len() > cap {
            rec.replay_buffer.pop_front()
        } else {
            None
        };
        if let Some(h) = evicted {
            with_wave_state(|ws| ws.deferred_handle_releases.push(h));
        }
    }

    // ===================================================================
    // Operator dispatch (Slice C-1, D009).
    //
    // `fire_operator` is the entry point for nodes whose `kind` is
    // `NodeKind::Operator(_)`. It branches on the `OperatorOp` discriminant
    // to per-operator helpers that snapshot inputs under the lock, drop the
    // lock to call the binding's bulk projection FFI, and reacquire to
    // apply emissions via `commit_emission_verbatim` (no per-item equals
    // dedup at the wire — operator output passes verbatim per the same
    // R1.3.2.d / R1.3.3.c rule that governs `FnResult::Batch`).
    //
    // **Refcount discipline:** inputs sourced from `dep_records[i].data_batch`
    // share retains owned by the wave's data-batch slot (released at
    // wave-end rotation in `clear_wave_state`). Operators that emit those
    // handles unchanged (`Filter`, `DistinctUntilChanged`, `Pairwise`'s
    // `prev` carry-over) take an additional retain via `retain_handle`
    // before passing to `commit_emission_verbatim` — the cache slot owns
    // its own share, independent of the data-batch slot's. Operators that
    // produce fresh handles (`Map` / `Scan` / `Reduce` / `Pairwise`'s
    // packed tuples) receive retains pre-bumped by the binding's bulk-
    // projection method.
    // ===================================================================

    /// Operator dispatch entry. Pre-checks (terminal short-circuit, first-
    /// run gate accounting for `partial`, terminal-aware fire for `Reduce`)
    /// happen here; per-operator behavior lives in the `fire_op_*` helpers.
    fn fire_operator(&self, node_id: NodeId, op: OperatorOp) {
        // Phase 1 (lock-held): remove from pending_fires, evaluate skip.
        // Q-beyond Sub-slice 2 (D108, 2026-05-09): pending_fires lives on
        // per-thread WaveState; state lock + WaveState borrow are
        // independent.
        let proceed = {
            let s = self.lock_state();
            with_wave_state(|ws| {
                ws.pending_fires.remove(&node_id);
            });
            let rec = s.require_node(node_id);
            if rec.terminal.is_some() {
                false
            } else {
                // First-run gate (R2.5.3 / R5.4). Partial-mode operators
                // (D011) opt out of the gate; otherwise we wait for every
                // dep to have delivered at least one real handle. Terminal-
                // aware operators (currently `Reduce`) additionally count a
                // dep terminal as "real input" so they can fire on
                // upstream COMPLETE-without-DATA and emit the seed.
                let has_real_input = !rec.has_sentinel_deps()
                    || rec.dep_records.iter().any(|dr| dr.terminal.is_some());
                rec.partial || has_real_input
            }
        };
        if !proceed {
            return;
        }

        // A6 (Slice F, 2026-05-07): track operator fire on the
        // `currently_firing` stack so a binding-side project/predicate/fold
        // FFI callback that re-enters `Core::set_deps(node_id, ...)` is
        // rejected with `SetDepsError::ReentrantOnFiringNode`. Drop pops
        // the stack on panic too.
        let _firing = FiringGuard::new(self, node_id);

        match op {
            OperatorOp::Map { fn_id } => self.fire_op_map(node_id, fn_id),
            OperatorOp::Filter { fn_id } => self.fire_op_filter(node_id, fn_id),
            OperatorOp::Scan { fn_id, .. } => self.fire_op_scan(node_id, fn_id),
            OperatorOp::Reduce { fn_id, .. } => self.fire_op_reduce(node_id, fn_id),
            OperatorOp::DistinctUntilChanged { equals_fn_id } => {
                self.fire_op_distinct(node_id, equals_fn_id);
            }
            OperatorOp::Pairwise { fn_id } => self.fire_op_pairwise(node_id, fn_id),
            OperatorOp::Combine { pack_fn } => self.fire_op_combine(node_id, pack_fn),
            OperatorOp::WithLatestFrom { pack_fn } => {
                self.fire_op_with_latest_from(node_id, pack_fn);
            }
            OperatorOp::Merge => self.fire_op_merge(node_id),
            OperatorOp::Take { count } => self.fire_op_take(node_id, count),
            OperatorOp::Skip { count } => self.fire_op_skip(node_id, count),
            OperatorOp::TakeWhile { fn_id } => self.fire_op_take_while(node_id, fn_id),
            // The variant carries `default` for `register_operator`'s
            // `make_op_scratch` path; once registered, the live default
            // is read from `LastState::default` inside `fire_op_last`.
            OperatorOp::Last { .. } => self.fire_op_last(node_id),
            OperatorOp::Tap { fn_id } => self.fire_op_tap(node_id, fn_id),
            OperatorOp::TapFirst { fn_id } => self.fire_op_tap_first(node_id, fn_id),
            OperatorOp::Valve => self.fire_op_valve(node_id),
            OperatorOp::Settle {
                quiet_waves,
                max_waves,
            } => self.fire_op_settle(node_id, quiet_waves, max_waves),
        }
    }

    /// Snapshot the operator's single dep batch (transform constraint —
    /// R5.7 single-dep). Returns `(inputs, terminal)` where `inputs` is a
    /// fresh `Vec<HandleId>` (no retains) and `terminal` reflects
    /// `dep_records[0].terminal` at snapshot time.
    fn snapshot_op_dep0(&self, node_id: NodeId) -> (Vec<HandleId>, Option<TerminalKind>) {
        let s = self.lock_state();
        let rec = s.require_node(node_id);
        debug_assert!(
            !rec.dep_records.is_empty(),
            "transform operator must have ≥1 dep"
        );
        let dr = &rec.dep_records[0];
        (dr.data_batch.iter().copied().collect(), dr.terminal)
    }

    /// Emit DIRTY (if not already dirty) followed by RESOLVED. Used by
    /// silent-drop operators (Filter / DistinctUntilChanged / Pairwise)
    /// when a wave's inputs all suppress and the operator needs to settle
    /// the wave for its subscribers (D018 — let DIRTY ride; queue RESOLVED
    /// on full-reject).
    fn settle_dirty_resolved(&self, node_id: NodeId) {
        let mut s = self.lock_state();
        if s.require_node(node_id).terminal.is_some() {
            return;
        }
        let already_dirty = s.require_node(node_id).dirty;
        s.require_node_mut(node_id).dirty = true;
        if !already_dirty {
            self.queue_notify(&mut s, node_id, Message::Dirty);
        }
        // Slice G: skip Resolved if pending_notify already has a tier-3
        // message — adding Resolved would violate R1.3.3.a.
        // Q-beyond Sub-slice 2 (D108, 2026-05-09): pending_notify lives
        // on per-thread WaveState; borrow scoped so queue_notify can
        // re-borrow.
        let already_tier3 = with_wave_state(|ws| {
            ws.pending_notify
                .get(&node_id)
                .is_some_and(|entry| entry.iter_messages().any(|m| m.tier() == 3))
        });
        if !already_tier3 {
            self.queue_notify(&mut s, node_id, Message::Resolved);
        }
    }

    /// `OperatorOp::Map` dispatch.
    fn fire_op_map(&self, node_id: NodeId, fn_id: crate::handle::FnId) {
        let (inputs, _terminal) = self.snapshot_op_dep0(node_id);
        // Mark fired regardless of input count (activation gate already
        // satisfied or partial-mode).
        {
            let mut s = self.lock_state();
            s.require_node_mut(node_id).has_fired_once = true;
        }
        if inputs.is_empty() {
            return;
        }
        // Phase 2 (lock-released): bulk project. Binding returns one
        // handle per input, each with a retain share already taken.
        let outputs = self.binding.project_each(fn_id, &inputs);
        // Phase 3: emit each output. `commit_emission_verbatim` consumes
        // the retain into the cache slot (and releases the prior cache
        // handle internally).
        for h in outputs {
            self.commit_emission_verbatim(node_id, h);
        }
    }

    /// `OperatorOp::Filter` dispatch (D012/D018).
    fn fire_op_filter(&self, node_id: NodeId, fn_id: crate::handle::FnId) {
        let (inputs, _terminal) = self.snapshot_op_dep0(node_id);
        {
            let mut s = self.lock_state();
            s.require_node_mut(node_id).has_fired_once = true;
        }
        if inputs.is_empty() {
            return;
        }
        // Phase 2: predicate per input.
        let pass = self.binding.predicate_each(fn_id, &inputs);
        // Slice V2: promoted from debug_assert! — binding contract violation
        // should fail loud in release builds too.
        assert!(
            pass.len() == inputs.len(),
            "predicate_each returned {} bools for {} inputs",
            pass.len(),
            inputs.len()
        );
        // Phase 3: emit passing items verbatim. Take a fresh retain for
        // each — the data_batch slot still owns its retain (released at
        // wave-end rotation), and the cache slot needs its own.
        let mut emitted = 0usize;
        for (i, &h) in inputs.iter().enumerate() {
            if pass.get(i).copied().unwrap_or(false) {
                self.binding.retain_handle(h);
                self.commit_emission_verbatim(node_id, h);
                emitted += 1;
            }
        }
        // D018: full-reject settles with DIRTY+RESOLVED.
        if emitted == 0 {
            self.settle_dirty_resolved(node_id);
        }
    }

    /// `OperatorOp::Scan` dispatch — left-fold emitting each new acc.
    fn fire_op_scan(&self, node_id: NodeId, fn_id: crate::handle::FnId) {
        use crate::op_state::ScanState;
        let (inputs, _terminal) = self.snapshot_op_dep0(node_id);
        let acc = {
            let s = self.lock_state();
            scratch_ref::<ScanState>(&s, node_id).acc
        };
        {
            let mut s = self.lock_state();
            s.require_node_mut(node_id).has_fired_once = true;
        }
        if inputs.is_empty() {
            return;
        }
        // Phase 2: fold each input through. Returns N new handles, each
        // with a fresh retain.
        let new_states = self.binding.fold_each(fn_id, acc, &inputs);
        // Slice V2: promoted from debug_assert! — binding contract violation.
        assert!(
            new_states.len() == inputs.len(),
            "fold_each returned {} accs for {} inputs",
            new_states.len(),
            inputs.len()
        );
        // Phase 3a: update ScanState.acc to the LAST new acc. Take an
        // extra retain for the slot; release the prior acc's slot retain.
        let last_acc = new_states.last().copied();
        if let Some(last) = last_acc {
            let prev_acc = {
                let mut s = self.lock_state();
                let scratch = scratch_mut::<ScanState>(&mut s, node_id);
                let prev = scratch.acc;
                scratch.acc = last;
                prev
            };
            // Take the slot's retain on the new acc.
            self.binding.retain_handle(last);
            // Release the prior slot's retain (post-lock to keep binding
            // free to re-enter Core safely).
            if prev_acc != crate::handle::NO_HANDLE {
                self.binding.release_handle(prev_acc);
            }
        }
        // Phase 3b: emit each intermediate acc verbatim. `new_states`
        // entries each carry one retain from `fold_each`; that retain is
        // consumed by `commit_emission_verbatim` into the cache slot.
        for h in new_states {
            self.commit_emission_verbatim(node_id, h);
        }
    }

    /// `OperatorOp::Reduce` dispatch — accumulates silently; emits acc on
    /// upstream COMPLETE (cascades ERROR verbatim).
    fn fire_op_reduce(&self, node_id: NodeId, fn_id: crate::handle::FnId) {
        use crate::op_state::ReduceState;
        let (inputs, terminal) = self.snapshot_op_dep0(node_id);
        let acc = {
            let s = self.lock_state();
            scratch_ref::<ReduceState>(&s, node_id).acc
        };
        {
            let mut s = self.lock_state();
            s.require_node_mut(node_id).has_fired_once = true;
        }
        // Phase 2: accumulate (silent — no per-input emit).
        let new_states = if inputs.is_empty() {
            SmallVec::<[HandleId; 1]>::new()
        } else {
            self.binding.fold_each(fn_id, acc, &inputs)
        };
        // Slice V2: promoted from debug_assert! — binding contract violation.
        assert!(
            new_states.len() == inputs.len(),
            "fold_each returned {} accs for {} inputs",
            new_states.len(),
            inputs.len()
        );
        // Update ReduceState.acc to last new acc; release intermediate
        // states (we don't emit them) and the prior acc's slot retain.
        let last_acc = new_states.last().copied();
        let intermediates_to_release: Vec<HandleId> = if new_states.len() > 1 {
            new_states[..new_states.len() - 1].to_vec()
        } else {
            Vec::new()
        };
        let prev_acc_to_release = if let Some(last) = last_acc {
            let prev_acc = {
                let mut s = self.lock_state();
                let scratch = scratch_mut::<ReduceState>(&mut s, node_id);
                let prev = scratch.acc;
                scratch.acc = last;
                prev
            };
            self.binding.retain_handle(last);
            if prev_acc == crate::handle::NO_HANDLE {
                None
            } else {
                Some(prev_acc)
            }
        } else {
            None
        };
        // Release intermediate fold results (Reduce only emits the LAST,
        // but only on terminal). Each was retained by `fold_each`.
        for h in intermediates_to_release {
            self.binding.release_handle(h);
        }
        if let Some(h) = prev_acc_to_release {
            self.binding.release_handle(h);
        }

        // Phase 3: emit on terminal.
        match terminal {
            None => {
                // Still accumulating; no emit. Subscribers see no message
                // for this wave (silent accumulation). The first wave that
                // pushes Reduce to fire produces a Dirty entry on the
                // upstream's commit, but Reduce itself doesn't queue any
                // tier-3 since R5 silently absorbs. v1: leave the
                // post-drain auto-resolve sweep to settle nothing —
                // pending_notify has no entry for Reduce so the sweep is
                // a no-op.
            }
            Some(TerminalKind::Complete) => {
                // Read the live acc (may be the seed if no DATA arrived)
                // and emit Data(acc) + Complete.
                let final_acc = {
                    let s = self.lock_state();
                    scratch_ref::<ReduceState>(&s, node_id).acc
                };
                if final_acc != crate::handle::NO_HANDLE {
                    // Emission needs its own retain (slot's retain is
                    // owned by ReduceState.acc until reset/Drop).
                    self.binding.retain_handle(final_acc);
                    self.commit_emission_verbatim(node_id, final_acc);
                }
                self.complete(node_id);
            }
            Some(TerminalKind::Error(h)) => {
                // Core::error transfers the caller's share into the
                // cascade (node.terminal + per-child dep_terminal slots);
                // no release at the error() boundary. Take a fresh share
                // here so the cascade owns it independently of the
                // dep_records[0].terminal slot's share.
                self.binding.retain_handle(h);
                self.error(node_id, h);
            }
        }
    }

    /// `OperatorOp::DistinctUntilChanged` dispatch.
    fn fire_op_distinct(&self, node_id: NodeId, equals_fn_id: crate::handle::FnId) {
        use crate::op_state::DistinctState;
        let (inputs, _terminal) = self.snapshot_op_dep0(node_id);
        let mut prev = {
            let s = self.lock_state();
            scratch_ref::<DistinctState>(&s, node_id).prev
        };
        {
            let mut s = self.lock_state();
            s.require_node_mut(node_id).has_fired_once = true;
        }
        if inputs.is_empty() {
            return;
        }
        // Take a working-copy retain on the initial prev so both the loop
        // (which releases old_prev on each non-equal item) and phase 3
        // (which releases the slot's original handle) each have their own
        // share. Without this, the loop's release of old_prev (== original
        // DistinctState.prev) double-releases against phase 3's stale_slot
        // release.
        if prev != crate::handle::NO_HANDLE {
            self.binding.retain_handle(prev);
        }
        // Phase 2: per-input equals(prev, current). Each non-equal input
        // is emitted and becomes the new prev. Equals fn_id reuses
        // `BindingBoundary::custom_equals`.
        let mut emitted = 0usize;
        for &h in &inputs {
            let equal = if prev == crate::handle::NO_HANDLE {
                false
            } else if prev == h {
                true
            } else {
                self.binding.custom_equals(equals_fn_id, prev, h)
            };
            if !equal {
                // Emit this input verbatim.
                self.binding.retain_handle(h);
                self.commit_emission_verbatim(node_id, h);
                // Update prev: take retain on new prev, release old
                // (working-copy retain from above or from prior iteration).
                self.binding.retain_handle(h);
                let old_prev = prev;
                prev = h;
                if old_prev != crate::handle::NO_HANDLE {
                    self.binding.release_handle(old_prev);
                }
                emitted += 1;
            }
        }
        // Phase 3: persist prev into DistinctState.prev slot. Release the
        // slot's original retain (stale_slot) — this is the slot-owned
        // share, independent of the working-copy share released in the
        // loop above.
        {
            let mut s = self.lock_state();
            let scratch = scratch_mut::<DistinctState>(&mut s, node_id);
            let stale_slot = scratch.prev;
            scratch.prev = prev;
            if stale_slot != prev && stale_slot != crate::handle::NO_HANDLE {
                drop(s);
                self.binding.release_handle(stale_slot);
            }
        }
        // Release the working-copy retain on the final prev if it was
        // never released in the loop (i.e. no non-equal items passed,
        // prev == original). In that case stale_slot == prev, so phase 3
        // didn't release it either — but the working-copy retain is still
        // outstanding. Release it now.
        if emitted == 0 && prev != crate::handle::NO_HANDLE {
            self.binding.release_handle(prev);
        }
        if emitted == 0 {
            self.settle_dirty_resolved(node_id);
        }
    }

    /// `OperatorOp::Pairwise` dispatch — emits `(prev, current)` tuples
    /// starting after the second value (first input swallowed, sets `prev`).
    fn fire_op_pairwise(&self, node_id: NodeId, fn_id: crate::handle::FnId) {
        use crate::op_state::PairwiseState;
        let (inputs, _terminal) = self.snapshot_op_dep0(node_id);
        let mut prev = {
            let s = self.lock_state();
            scratch_ref::<PairwiseState>(&s, node_id).prev
        };
        {
            let mut s = self.lock_state();
            s.require_node_mut(node_id).has_fired_once = true;
        }
        if inputs.is_empty() {
            return;
        }
        let mut emitted = 0usize;
        for &h in &inputs {
            if prev == crate::handle::NO_HANDLE {
                // First-ever value — swallow, set prev. Retain for the
                // PairwiseState.prev slot (persisted in phase 3 below).
                self.binding.retain_handle(h);
                prev = h;
                continue;
            }
            // Pack (prev, current) into a tuple handle. Binding returns a
            // fresh retain on the packed handle.
            let packed = self.binding.pairwise_pack(fn_id, prev, h);
            self.commit_emission_verbatim(node_id, packed);
            // Advance prev: take retain on h, release old prev.
            self.binding.retain_handle(h);
            let old_prev = prev;
            prev = h;
            self.binding.release_handle(old_prev);
            emitted += 1;
        }
        // Persist prev into PairwiseState.prev slot.
        {
            let mut s = self.lock_state();
            let scratch = scratch_mut::<PairwiseState>(&mut s, node_id);
            let stale_slot = scratch.prev;
            scratch.prev = prev;
            if stale_slot != prev && stale_slot != crate::handle::NO_HANDLE {
                drop(s);
                self.binding.release_handle(stale_slot);
            }
        }
        if emitted == 0 {
            self.settle_dirty_resolved(node_id);
        }
    }

    // =================================================================
    // Slice C-2: multi-dep combinator operators (D020)
    // =================================================================

    /// Snapshot all deps' "latest" handle for multi-dep combinators.
    /// For each dep: returns `data_batch.last()` if non-empty (dep fired
    /// this wave), else `prev_data` (last handle from previous wave).
    /// Also returns whether dep[0] (primary) had DATA this wave —
    /// needed by `fire_op_with_latest_from`.
    fn snapshot_op_all_latest(&self, node_id: NodeId) -> (SmallVec<[HandleId; 4]>, bool) {
        let s = self.lock_state();
        let rec = s.require_node(node_id);
        let primary_fired = rec
            .dep_records
            .first()
            .is_some_and(|dr| !dr.data_batch.is_empty());
        let latest: SmallVec<[HandleId; 4]> = rec
            .dep_records
            .iter()
            .map(|dr| dr.data_batch.last().copied().unwrap_or(dr.prev_data))
            .collect();
        (latest, primary_fired)
    }

    /// `OperatorOp::Combine` dispatch — N-dep combineLatest. Packs the
    /// latest handle per dep into a tuple via `pack_tuple`, emits on
    /// any dep fire. First-run gate (R2.5.3, partial: false) guarantees
    /// all deps have a real handle on first fire. Post-warmup INVALIDATE
    /// guard: if any dep's prev_data was cleared, settles with RESOLVED
    /// instead of packing a NO_HANDLE into the tuple.
    fn fire_op_combine(&self, node_id: NodeId, pack_fn: crate::handle::FnId) {
        let (latest, _primary_fired) = self.snapshot_op_all_latest(node_id);
        {
            let mut s = self.lock_state();
            s.require_node_mut(node_id).has_fired_once = true;
        }
        // Post-warmup INVALIDATE guard: a dep may have been invalidated
        // (prev_data cleared to NO_HANDLE) and not yet re-delivered.
        if latest.contains(&crate::handle::NO_HANDLE) {
            self.settle_dirty_resolved(node_id);
            return;
        }
        let tuple_handle = self.binding.pack_tuple(pack_fn, &latest);
        self.commit_emission_verbatim(node_id, tuple_handle);
    }

    /// `OperatorOp::WithLatestFrom` dispatch — 2-dep, fire-on-primary-only
    /// (D021 / Phase 10.5). Emits `[primary, secondary]` pair only when
    /// dep[0] (primary) has DATA in the wave. If only dep[1] fires →
    /// RESOLVED. Post-warmup INVALIDATE guard: if secondary latest is
    /// `NO_HANDLE` (INVALIDATE cleared it), settles with RESOLVED.
    fn fire_op_with_latest_from(&self, node_id: NodeId, pack_fn: crate::handle::FnId) {
        let (latest, primary_fired) = self.snapshot_op_all_latest(node_id);
        let first_fire = {
            let mut s = self.lock_state();
            let rec = s.require_node_mut(node_id);
            let was_first = !rec.has_fired_once;
            rec.has_fired_once = true;
            was_first
        };
        // On first fire (gate release), always emit — the first-run gate
        // guarantees both deps have values (via prev_data fallback in
        // snapshot). On subsequent fires, only emit when primary fires.
        if !first_fire && !primary_fired {
            // Secondary-only update — no downstream DATA.
            self.settle_dirty_resolved(node_id);
            return;
        }
        // Post-warmup INVALIDATE guard: secondary may have been invalidated
        // (prev_data cleared to NO_HANDLE) and not yet re-delivered.
        debug_assert!(latest.len() == 2, "withLatestFrom requires exactly 2 deps");
        if latest[1] == crate::handle::NO_HANDLE {
            self.settle_dirty_resolved(node_id);
            return;
        }
        let tuple_handle = self.binding.pack_tuple(pack_fn, &latest);
        self.commit_emission_verbatim(node_id, tuple_handle);
    }

    /// `OperatorOp::Merge` dispatch — N-dep, forward all DATA handles
    /// verbatim (D022). Zero FFI on fire: no transformation. Each dep's
    /// batch handles are collected, retained, and emitted individually.
    fn fire_op_merge(&self, node_id: NodeId) {
        // Collect all batch handles from all deps (flat).
        let all_handles: Vec<HandleId> = {
            let s = self.lock_state();
            let rec = s.require_node(node_id);
            rec.dep_records
                .iter()
                .flat_map(|dr| dr.data_batch.iter().copied())
                .collect()
        };
        {
            let mut s = self.lock_state();
            s.require_node_mut(node_id).has_fired_once = true;
        }
        if all_handles.is_empty() {
            // All deps settled RESOLVED this wave — no DATA to forward.
            self.settle_dirty_resolved(node_id);
            return;
        }
        // Emit each handle verbatim. Take a fresh retain per handle
        // (independent of the dep batch's retain which gets released at
        // wave-end). Matches Filter's discipline for passing inputs.
        for &h in &all_handles {
            self.binding.retain_handle(h);
            self.commit_emission_verbatim(node_id, h);
        }
    }

    // =================================================================
    // Slice C-3: flow operators (D024)
    // =================================================================

    /// `OperatorOp::Take` dispatch — emits the first `count` DATA values
    /// then self-completes via `Core::complete`. When `count == 0`, the
    /// first fire emits zero items then immediately self-completes
    /// (D027). Cross-wave counter lives in
    /// [`TakeState::count_emitted`](super::op_state::TakeState::count_emitted).
    fn fire_op_take(&self, node_id: NodeId, count: u32) {
        use crate::op_state::TakeState;
        let (inputs, terminal) = self.snapshot_op_dep0(node_id);
        // Snapshot current counter; mark fired regardless of input count
        // (activation gate already satisfied or partial-mode).
        let mut count_emitted = {
            let s = self.lock_state();
            scratch_ref::<TakeState>(&s, node_id).count_emitted
        };
        {
            let mut s = self.lock_state();
            s.require_node_mut(node_id).has_fired_once = true;
        }
        // Already at quota before any input this wave — self-complete
        // immediately. Covers `count == 0` (first-fire short-circuit) and
        // any defensive re-entry after the terminal-skip in `fire_operator`
        // already guards against double-complete.
        if count_emitted >= count {
            self.complete(node_id);
            return;
        }
        // Per-input emission loop. Each pass takes a fresh retain for the
        // cache slot; data_batch slot's retain is released at wave-end
        // rotation independently.
        for &h in &inputs {
            self.binding.retain_handle(h);
            self.commit_emission_verbatim(node_id, h);
            count_emitted = count_emitted.saturating_add(1);
            if count_emitted >= count {
                break;
            }
        }
        // Persist the updated counter.
        {
            let mut s = self.lock_state();
            scratch_mut::<TakeState>(&mut s, node_id).count_emitted = count_emitted;
        }
        // Self-complete if we hit the quota this wave. Upstream COMPLETE
        // (terminal == Some(Complete)) without us hitting the count
        // propagates via the standard auto-cascade — we don't intercept it.
        if count_emitted >= count {
            self.complete(node_id);
            return;
        }
        // If upstream is already Errored and we haven't hit count, the
        // standard cascade will propagate it. If the wave delivered no
        // inputs (e.g. RESOLVED from upstream), settle DIRTY+RESOLVED so
        // subscribers see the wave close.
        if inputs.is_empty() && terminal.is_none() {
            self.settle_dirty_resolved(node_id);
        }
    }

    /// `OperatorOp::Skip` dispatch — drops the first `count` DATA values,
    /// then forwards the rest. Cross-wave counter lives in
    /// [`SkipState::count_skipped`](super::op_state::SkipState::count_skipped).
    /// On a wave where every input is still in the skip window, settles
    /// DIRTY+RESOLVED (D018 pattern) so subscribers see the wave close.
    fn fire_op_skip(&self, node_id: NodeId, count: u32) {
        use crate::op_state::SkipState;
        let (inputs, _terminal) = self.snapshot_op_dep0(node_id);
        let mut count_skipped = {
            let s = self.lock_state();
            scratch_ref::<SkipState>(&s, node_id).count_skipped
        };
        {
            let mut s = self.lock_state();
            s.require_node_mut(node_id).has_fired_once = true;
        }
        // No early-return on empty inputs: the post-loop `emitted == 0`
        // settle handles the empty-inputs case identically to the
        // all-swallowed-by-skip-window case (Slice C-3 /qa P6 — symmetry
        // with `fire_op_take`).
        let mut emitted = 0usize;
        for &h in &inputs {
            if count_skipped < count {
                count_skipped = count_skipped.saturating_add(1);
                // Drop this input — the data_batch slot still owns its
                // retain (released at wave-end rotation). No emission.
                continue;
            }
            // Past the skip window — emit verbatim. Take a fresh retain
            // for the cache slot.
            self.binding.retain_handle(h);
            self.commit_emission_verbatim(node_id, h);
            emitted += 1;
        }
        // Persist the updated counter.
        {
            let mut s = self.lock_state();
            scratch_mut::<SkipState>(&mut s, node_id).count_skipped = count_skipped;
        }
        if emitted == 0 {
            self.settle_dirty_resolved(node_id);
        }
    }

    /// `OperatorOp::TakeWhile` dispatch — emits while the predicate
    /// holds; on the first `false`, emits any preceding passes from the
    /// same batch then self-completes via `Core::complete`. Reuses
    /// [`BindingBoundary::predicate_each`] (D029).
    fn fire_op_take_while(&self, node_id: NodeId, fn_id: crate::handle::FnId) {
        let (inputs, _terminal) = self.snapshot_op_dep0(node_id);
        {
            let mut s = self.lock_state();
            s.require_node_mut(node_id).has_fired_once = true;
        }
        if inputs.is_empty() {
            return;
        }
        // Phase 2: predicate per input.
        let pass = self.binding.predicate_each(fn_id, &inputs);
        // Slice V2: promoted from debug_assert! — binding contract violation
        // should fail loud in release builds too.
        assert!(
            pass.len() == inputs.len(),
            "predicate_each returned {} bools for {} inputs",
            pass.len(),
            inputs.len()
        );
        // Phase 3: emit each input until the first false; then
        // self-complete. `fire_operator`'s `terminal.is_some()`
        // short-circuit gates re-entry after the self-complete cascade
        // installs the terminal slot — no extra `done` flag needed.
        let mut emitted = 0usize;
        let mut first_false_seen = false;
        for (i, &h) in inputs.iter().enumerate() {
            if pass.get(i).copied().unwrap_or(false) {
                self.binding.retain_handle(h);
                self.commit_emission_verbatim(node_id, h);
                emitted += 1;
            } else {
                first_false_seen = true;
                break;
            }
        }
        if first_false_seen {
            self.complete(node_id);
            return;
        }
        if emitted == 0 {
            // Whole batch passed but was empty (impossible here since
            // inputs.is_empty() returned early above) — defensive only.
            self.settle_dirty_resolved(node_id);
        }
    }

    /// `OperatorOp::Last` dispatch — buffers the latest DATA; emits
    /// `Data(latest)` (or `Data(default)` if no DATA arrived and a
    /// default was registered) then `Complete` on upstream COMPLETE.
    /// On upstream ERROR, propagates verbatim. Storage:
    /// [`LastState`](super::op_state::LastState).
    ///
    /// **Silent-buffer semantics (mirrors Reduce):** on a non-terminal
    /// wave (`terminal == None`), `fire_op_last` updates the buffered
    /// `latest` handle but produces NO downstream wire message —
    /// subscribers observe the operator only when upstream
    /// COMPLETE/ERROR triggers the terminal branch. Intermediate
    /// inputs from the dep's batch are dropped on the floor (their
    /// `data_batch` retains release at wave-end rotation
    /// independently). Per-wave settlement on intermediate waves is
    /// the canonical behavior for terminal-aware operators.
    fn fire_op_last(&self, node_id: NodeId) {
        use crate::op_state::LastState;
        let (inputs, terminal) = self.snapshot_op_dep0(node_id);
        {
            let mut s = self.lock_state();
            s.require_node_mut(node_id).has_fired_once = true;
        }

        // Phase 2: buffer the latest input handle (if any). Retain new,
        // release old. data_batch slot's retain is released at wave-end
        // rotation independently — the LastState slot keeps its own
        // share so the value survives across waves.
        if let Some(&new_latest) = inputs.last() {
            let prev_latest = {
                let mut s = self.lock_state();
                let scratch = scratch_mut::<LastState>(&mut s, node_id);
                let prev = scratch.latest;
                scratch.latest = new_latest;
                prev
            };
            self.binding.retain_handle(new_latest);
            if prev_latest != crate::handle::NO_HANDLE {
                self.binding.release_handle(prev_latest);
            }
        }

        // Phase 3: emit on terminal. Buffer-only fires (no terminal yet)
        // produce no downstream message — Reduce-style silent
        // accumulation. The post-drain auto-resolve sweep is a no-op
        // because pending_notify has no entry for Last.
        match terminal {
            None => {}
            Some(TerminalKind::Complete) => {
                // Read the live latest + default. If latest != NO_HANDLE,
                // emit it. Otherwise, if default != NO_HANDLE, emit default.
                // Otherwise, emit only Complete (empty stream, no default).
                let (latest, default) = {
                    let s = self.lock_state();
                    let scratch = scratch_ref::<LastState>(&s, node_id);
                    (scratch.latest, scratch.default)
                };
                let to_emit = if latest != crate::handle::NO_HANDLE {
                    Some(latest)
                } else if default != crate::handle::NO_HANDLE {
                    Some(default)
                } else {
                    None
                };
                if let Some(h) = to_emit {
                    // Emission needs its own retain — the LastState slot
                    // keeps its share until reset/Drop.
                    self.binding.retain_handle(h);
                    self.commit_emission_verbatim(node_id, h);
                }
                self.complete(node_id);
            }
            Some(TerminalKind::Error(h)) => {
                // Take a fresh share for the error cascade — the
                // dep_records[0].terminal slot keeps its own share
                // (released by reset_for_fresh_lifecycle / Drop).
                self.binding.retain_handle(h);
                self.error(node_id, h);
            }
        }
    }

    // -----------------------------------------------------------------
    // Slice U: control operators — fire_op impls
    // -----------------------------------------------------------------

    /// Tap — side-effect passthrough. Invoke tap fn on each DATA, then
    /// emit each input handle unchanged (zero allocation).
    fn fire_op_tap(&self, node_id: NodeId, fn_id: FnId) {
        let (inputs, terminal) = self.snapshot_op_dep0(node_id);
        {
            let mut s = self.lock_state();
            s.require_node_mut(node_id).has_fired_once = true;
        }
        if inputs.is_empty() {
            if terminal.is_none() {
                self.settle_dirty_resolved(node_id);
            }
        } else {
            for &h in &inputs {
                self.binding.invoke_tap_fn(fn_id, h);
                self.binding.retain_handle(h);
                self.commit_emission_verbatim(node_id, h);
            }
        }
        // Terminal forwarding.
        match terminal {
            None => {}
            Some(TerminalKind::Complete) => {
                self.binding.invoke_tap_complete_fn(fn_id);
                self.complete(node_id);
            }
            Some(TerminalKind::Error(h)) => {
                self.binding.invoke_tap_error_fn(fn_id, h);
                self.binding.retain_handle(h);
                self.error(node_id, h);
            }
        }
    }

    /// TapFirst — one-shot side-effect on first DATA. After the first
    /// qualifying DATA, acts as pure passthrough.
    fn fire_op_tap_first(&self, node_id: NodeId, fn_id: FnId) {
        use crate::op_state::TapFirstState;
        let (inputs, terminal) = self.snapshot_op_dep0(node_id);
        {
            let mut s = self.lock_state();
            s.require_node_mut(node_id).has_fired_once = true;
        }
        if inputs.is_empty() {
            if terminal.is_none() {
                self.settle_dirty_resolved(node_id);
            }
        } else {
            let fired = {
                let s = self.lock_state();
                scratch_ref::<TapFirstState>(&s, node_id).fired
            };
            for &h in &inputs {
                if !fired {
                    self.binding.invoke_tap_fn(fn_id, h);
                    let mut s = self.lock_state();
                    scratch_mut::<TapFirstState>(&mut s, node_id).fired = true;
                }
                self.binding.retain_handle(h);
                self.commit_emission_verbatim(node_id, h);
            }
        }
        if let Some(TerminalKind::Complete) = terminal {
            self.complete(node_id);
        } else if let Some(TerminalKind::Error(h)) = terminal {
            self.binding.retain_handle(h);
            self.error(node_id, h);
        }
    }

    /// Valve — conditional forward. dep[0]=source, dep[1]=control.
    /// When control is truthy, forwards source DATA; else RESOLVED.
    fn fire_op_valve(&self, node_id: NodeId) {
        // Snapshot both deps.
        let (src_inputs, src_terminal, ctrl_latest) = {
            let s = self.lock_state();
            let rec = s.require_node(node_id);
            debug_assert!(rec.dep_records.len() == 2, "valve must have exactly 2 deps");
            let dr0 = &rec.dep_records[0];
            let dr1 = &rec.dep_records[1];
            let src_inputs: Vec<HandleId> = dr0.data_batch.iter().copied().collect();
            let src_term = dr0.terminal;
            // Latest control: last of this wave's batch, or prev_data.
            let ctrl = dr1.data_batch.last().copied().unwrap_or(dr1.prev_data);
            (src_inputs, src_term, ctrl)
        };
        {
            let mut s = self.lock_state();
            s.require_node_mut(node_id).has_fired_once = true;
        }

        // Source terminal forwarding (D3).
        if let Some(TerminalKind::Complete) = src_terminal {
            self.complete(node_id);
            return;
        }
        if let Some(TerminalKind::Error(h)) = src_terminal {
            self.binding.retain_handle(h);
            self.error(node_id, h);
            return;
        }

        // Gate: NO_HANDLE means "gate closed" (control never sent DATA);
        // any real handle means "gate open". Proper value-level truthiness
        // would require BindingBoundary::is_truthy (deferred — D048).
        let gate_open = ctrl_latest != crate::handle::NO_HANDLE;

        if !gate_open {
            self.settle_dirty_resolved(node_id);
            return;
        }

        if src_inputs.is_empty() {
            // Control opened but no source DATA this wave. Re-emit
            // prev source value if available.
            let prev_src = {
                let s = self.lock_state();
                s.require_node(node_id).dep_records[0].prev_data
            };
            if prev_src == crate::handle::NO_HANDLE {
                self.settle_dirty_resolved(node_id);
            } else {
                self.binding.retain_handle(prev_src);
                self.commit_emission_verbatim(node_id, prev_src);
            }
        } else {
            for &h in &src_inputs {
                self.binding.retain_handle(h);
                self.commit_emission_verbatim(node_id, h);
            }
        }
    }

    /// Settle — convergence detector. Forwards DATA, counts quiet waves,
    /// self-completes when converged.
    fn fire_op_settle(&self, node_id: NodeId, quiet_waves: u32, max_waves: Option<u32>) {
        use crate::op_state::SettleState;
        let (inputs, terminal) = self.snapshot_op_dep0(node_id);
        {
            let mut s = self.lock_state();
            s.require_node_mut(node_id).has_fired_once = true;
        }

        // Terminal forwarding.
        if let Some(TerminalKind::Complete) = terminal {
            self.complete(node_id);
            return;
        }
        if let Some(TerminalKind::Error(h)) = terminal {
            self.binding.retain_handle(h);
            self.error(node_id, h);
            return;
        }

        let saw_data = !inputs.is_empty();

        // Forward all DATA.
        for &h in &inputs {
            self.binding.retain_handle(h);
            self.commit_emission_verbatim(node_id, h);
        }

        // Update counters.
        let should_complete = {
            let mut s = self.lock_state();
            let scratch = scratch_mut::<SettleState>(&mut s, node_id);
            scratch.wave_count += 1;
            if saw_data {
                scratch.has_value = true;
                scratch.quiet_count = 0;
            } else {
                scratch.quiet_count += 1;
            }
            let settled = scratch.has_value && scratch.quiet_count >= quiet_waves;
            let exhausted = max_waves.is_some_and(|max| scratch.wave_count >= max);
            settled || exhausted
        };

        if should_complete {
            self.complete(node_id);
        } else if !saw_data {
            self.settle_dirty_resolved(node_id);
        }
    }

    pub(crate) fn deliver_data_to_consumer(
        &self,
        s: &mut CoreState,
        consumer_id: NodeId,
        dep_idx: usize,
        handle: HandleId,
    ) {
        // Retain the handle for the batch accumulation slot — each DATA
        // handle in `data_batch` owns a retain share, released at wave-end
        // rotation in `clear_wave_state`.
        self.binding.retain_handle(handle);

        let is_dynamic;
        let is_state;
        let tracked_or_first_fire;
        // Slice F audit close (2026-05-07): default-mode pause suppression.
        // If the consumer is paused with `PausableMode::Default`, the
        // canonical-spec §2.6 behavior is to suppress fn-fire and consolidate
        // pause-window dep deliveries into one fn execution on RESUME.
        // Mark `pending_wave` on the pause state instead of adding to
        // `pending_fires`. The dep state still advances (the data_batch push
        // above is unchanged), and clear_wave_state still rotates the latest
        // dep DATA into prev_data — so when the fn ultimately fires on
        // RESUME, it sees the consolidated post-pause state.
        let suppressed_for_default_pause;
        {
            let consumer = s.require_node_mut(consumer_id);
            consumer.dep_records[dep_idx].data_batch.push(handle);
            consumer.dep_records[dep_idx].involved_this_wave = true;
            consumer.involved_this_wave = true;
            // §10.13 perf (D047): set received_mask bit on first DATA
            // delivery for this dep.
            if dep_idx < 64 {
                consumer.received_mask |= 1u64 << dep_idx;
                // §10.3 perf (Slice V1): set involved_mask bit for
                // O(1) per-dep involvement query during fire.
                consumer.involved_mask |= 1u64 << dep_idx;
            }
            is_dynamic = consumer.is_dynamic;
            is_state = consumer.is_state();
            tracked_or_first_fire = !consumer.has_fired_once || consumer.tracked.contains(&dep_idx);
            suppressed_for_default_pause = consumer.pause_state.is_paused()
                && consumer.pausable == crate::node::PausableMode::Default;
            if suppressed_for_default_pause {
                consumer.pause_state.mark_pending_wave();
            }
        }
        if suppressed_for_default_pause {
            // Default-mode pause: don't add to pending_fires; RESUME will
            // schedule one consolidated fire.
            return;
        }
        // Q-beyond Sub-slice 2 (D108, 2026-05-09): pending_fires lives on
        // per-thread WaveState. State lock + WaveState borrow are
        // independent.
        if is_state {
            // State nodes don't have deps; unreachable in practice.
        } else if is_dynamic {
            if tracked_or_first_fire {
                with_wave_state(|ws| {
                    ws.pending_fires.insert(consumer_id);
                });
            }
        } else {
            // Derived / Operator / Producer (Producer has no deps so won't
            // reach here, but the predicate-based dispatch handles it
            // uniformly).
            with_wave_state(|ws| {
                ws.pending_fires.insert(consumer_id);
            });
        }
    }

    // -------------------------------------------------------------------
    // Subscriber notification
    // -------------------------------------------------------------------

    /// Queue a wave-end message for `node_id`'s subscribers.
    ///
    /// **Revision-tracked sink-snapshot batches (Slice X4 / D2,
    /// 2026-05-08):** each push for a given node either appends the
    /// message to the open batch (if `NodeRecord::subscribers_revision`
    /// hasn't advanced since that batch opened — the common case — no
    /// extra allocation), or opens a fresh batch with a current sink
    /// snapshot frozen at the new revision. A sub installed mid-wave
    /// bumps `subscribers_revision`; the next `queue_notify` for the
    /// same node observes the bump and starts a new batch that includes
    /// the new sub. Pre-subscribe batches retain their original snapshot,
    /// so earlier emits flush to their original sink list — the new sub
    /// does NOT double-receive them via flush AND handshake replay,
    /// closing the late-subscriber + multi-emit-per-wave R1.3.5.a gap.
    ///
    /// Pause routing decision (R1.3.7.b tier table, §10.2 buffering):
    ///   Tier 3 (DATA / RESOLVED) and Tier 4 (INVALIDATE) buffer while
    ///   paused; all other tiers (DIRTY tier 1, PAUSE/RESUME tier 2,
    ///   COMPLETE/ERROR tier 5, TEARDOWN tier 6) bypass the buffer and
    ///   flush immediately. START (tier 0) is per-subscription and never
    ///   transits queue_notify.
    pub(crate) fn queue_notify(&self, s: &mut crate::node::St<'_>, node_id: NodeId, msg: Message) {
        // R1.3.3.a / R1.3.3.d (Slice G — re-added 2026-05-07): dev-mode
        // wave-content invariant assertion. The tier-3 slot at one node in
        // one wave is either ≥1 DATA or exactly 1 RESOLVED — never mixed,
        // never multiple RESOLVED. Slice G moved equals substitution from
        // per-emit to wave-end coalescing; this assert pins that the
        // dispatcher itself never queues a violating combination at the
        // queue_notify granularity. Resolved arrivals come from:
        //   1. The auto-resolve sweep in `drain_and_flush` (gates on
        //      `!any tier-3` so it can't add to a wave with Data).
        //   2. The wave-end equals-substitution pass (rewrites in place,
        //      doesn't go through queue_notify).
        // Both honor R1.3.3.a by construction post-Slice-G.
        // Q-beyond Sub-slice 2 (D108, 2026-05-09): pending_notify lives
        // on per-thread WaveState. The dev-mode invariant assertion
        // borrows WaveState briefly and drops before the rest of
        // queue_notify proceeds.
        #[cfg(debug_assertions)]
        if matches!(msg.tier(), 3) {
            with_wave_state(|ws| {
                if let Some(entry) = ws.pending_notify.get(&node_id) {
                    // Walk all batches' messages — R1.3.3.a is a per-node
                    // wave-content invariant, not per-batch (the X4 batches
                    // are subscriber-snapshot epochs; the protocol-level
                    // tier-3 invariant spans the whole wave for the node).
                    let has_data = entry.iter_messages().any(|m| matches!(m, Message::Data(_)));
                    let resolved_count = entry
                        .iter_messages()
                        .filter(|m| matches!(m, Message::Resolved))
                        .count();
                    let incoming_is_data = matches!(msg, Message::Data(_));
                    if incoming_is_data {
                        debug_assert!(
                            resolved_count == 0,
                            "R1.3.3.a violation at {node_id:?}: queueing Data into a \
                             wave that already contains Resolved — Slice G should have \
                             prevented this via wave-end coalescing"
                        );
                    } else {
                        debug_assert!(
                            !has_data,
                            "R1.3.3.a violation at {node_id:?}: queueing Resolved into a \
                             wave that already contains Data"
                        );
                        debug_assert!(
                            resolved_count == 0,
                            "R1.3.3.a violation at {node_id:?}: multiple Resolved in one \
                             wave at one node"
                        );
                    }
                }
            });
        }

        let buffered_tier = matches!(msg.tier(), 3 | 4);
        let cap = s.shared.pause_buffer_cap;

        // Pause-routing branch — handles its own retain/release and returns
        // before we touch `pending_notify`, so the rec borrow is contained.
        {
            let rec = s.require_node_mut(node_id);
            if rec.subscribers.is_empty() {
                return;
            }
            // Slice F audit close (2026-05-07); amended 2026-05-17 for
            // canonical §2.6 R2.6.0 ("Option A"). Pause routing depends on
            // mode:
            //   - `ResumeAll`: buffer tier-3/4 for verbatim replay on RESUME.
            //   - `Default` + STATE node (leaf source — no deps): a state
            //     node's value is intrinsic, NOT produced by an fn/dep
            //     settle pipeline. PAUSE/RESUME gating is fn/dep-pipeline-
            //     scoped only (R2.6.0). A leaf source that holds its own
            //     pause lock and then self-emits via a direct external
            //     `down([[DATA, v]])` is pushing OUTSIDE that pipeline, so
            //     the DATA MUST flush immediately (cache advances now, no
            //     PAUSE synthesized, nothing replayed on RESUME). Therefore
            //     Default-mode state nodes do NOT buffer — they fall
            //     through to the immediate queue path, matching the
            //     `@graphrefly/pure-ts` reference (only `pausable:
            //     "resumeAll"` buffers a leaf source's direct `down()`).
            //   - `Default` + COMPUTE node: suppression happens upstream at
            //     fn-fire scheduling (see `deliver_data_to_consumer`); no
            //     outgoing tier-3 is produced from this node while paused,
            //     so this branch is unreachable for compute-default-paused.
            //     Fallthrough to the non-paused queue path is fine.
            //   - `Off`: pause is ignored entirely — tier-3 flushes
            //     immediately. Fallthrough.
            let mode_buffers_tier3 = match rec.pausable {
                crate::node::PausableMode::ResumeAll => true,
                crate::node::PausableMode::Default => false,
                crate::node::PausableMode::Off => false,
            };
            if buffered_tier && mode_buffers_tier3 && rec.pause_state.is_paused() {
                if let Some(h) = msg.payload_handle() {
                    self.binding.retain_handle(h);
                }
                let push_result = rec.pause_state.push_buffered(msg, cap);
                for dm in push_result.dropped_msgs {
                    if let Some(h) = dm.payload_handle() {
                        self.binding.release_handle(h);
                    }
                }
                // R1.3.8.c (Slice F, A3): on first overflow this cycle,
                // schedule a synthesized ERROR for wave-end emission.
                // `cap` is `Some` here (an overflow can only happen with a
                // configured cap), so `unwrap` is safe.
                if push_result.first_overflow_this_cycle {
                    if let Some((dropped_count, lock_held_ns)) =
                        rec.pause_state.overflow_diagnostic()
                    {
                        // Q-beyond Sub-slice 1 (D108, 2026-05-09):
                        // pending_pause_overflow lives on per-thread WaveState.
                        with_wave_state(|ws| {
                            ws.pending_pause_overflow
                                .push(crate::node::PendingPauseOverflow {
                                    node_id,
                                    dropped_count,
                                    configured_max: cap.unwrap_or(0),
                                    lock_held_ns,
                                });
                        });
                    }
                }
                return;
            }
        }

        // Non-paused queue path: retain payload handle and queue into
        // pending_notify. Released in `flush_notifications` after sinks
        // fire.
        if let Some(h) = msg.payload_handle() {
            self.binding.retain_handle(h);
        }
        Self::push_into_pending_notify(s, node_id, msg);
    }

    /// Slice X4 / D2: revision-tracked batch decision for `queue_notify`'s
    /// non-paused path. Either appends `msg` to the open batch (if
    /// `subscribers_revision` hasn't advanced since it opened — common
    /// case, no extra allocation) or opens a fresh batch with a current
    /// sink snapshot frozen at the new revision.
    ///
    /// Borrow discipline: reads `subscribers_revision` and the snapshot
    /// from `s.nodes` BEFORE borrowing WaveState's `pending_notify` to
    /// keep the two scopes disjoint.
    ///
    /// Q-beyond Sub-slice 2 (D108, 2026-05-09): `pending_notify` moved
    /// to per-thread WaveState. The state-side read of
    /// `subscribers_revision` / `subscribers` happens before the
    /// `with_wave_state` block opens, then the WaveState borrow
    /// performs the entry insertion / append. State lock + WaveState
    /// borrow remain independent.
    ///
    /// Lock-discipline assumption: this read of `subscribers_revision`
    /// is safe because both the subscribe install path
    /// ([`crate::node::Core::subscribe`]) and `queue_notify` hold
    /// `CoreState`'s mutex when they bump / read the revision —
    /// concurrent subscribe/unsubscribe cannot interleave. **If
    /// `Core::subscribe` ever moves the sink-install lock-released
    /// (mirroring the lock-released drain refactor), the revision read
    /// here must re-validate post-borrow — otherwise a fresh batch
    /// could open with a stale snapshot.**
    fn push_into_pending_notify(s: &mut CoreState, node_id: NodeId, msg: Message) {
        let current_rev = s.require_node(node_id).subscribers_revision;
        let needs_new_batch = with_wave_state(|ws| {
            ws.pending_notify.get(&node_id).is_none_or(|entry| {
                entry
                    .batches
                    .last()
                    .is_none_or(|b| b.snapshot_revision != current_rev)
            })
        });
        let sinks_snapshot: SmallVec<[Sink; 1]> = if needs_new_batch {
            s.require_node(node_id)
                .subscribers
                .values()
                .cloned()
                .collect()
        } else {
            SmallVec::new()
        };
        with_wave_state(|ws| match ws.pending_notify.entry(node_id) {
            Entry::Vacant(slot) => {
                let mut batches: SmallVec<[PendingBatch; 1]> = SmallVec::new();
                batches.push(PendingBatch {
                    snapshot_revision: current_rev,
                    sinks: sinks_snapshot,
                    messages: smallvec::smallvec![msg],
                });
                slot.insert(PendingPerNode { batches });
            }
            Entry::Occupied(mut slot) => {
                let entry = slot.get_mut();
                if needs_new_batch {
                    entry.batches.push(PendingBatch {
                        snapshot_revision: current_rev,
                        sinks: sinks_snapshot,
                        messages: smallvec::smallvec![msg],
                    });
                } else {
                    entry
                        .batches
                        .last_mut()
                        .expect("non-empty by construction (entry exists implies batch exists)")
                        .messages
                        .push(msg);
                }
            }
        });
    }

    /// Collect wave-end sink-fire jobs into `ws.deferred_flush_jobs` and the
    /// payload-handle releases owed for `pending_notify` into
    /// `ws.deferred_handle_releases`. The actual sink fires + handle releases
    /// run **after** the state lock is dropped — see [`Core::run_wave`].
    ///
    /// R1.3.1.b two-phase propagation: phase 1 (DIRTY) propagates through
    /// the entire graph before phase 2 (DATA / RESOLVED) begins. Implemented
    /// here as cross-node tier-then-node collect — phase 1's jobs sit before
    /// phase 2's in `deferred_flush_jobs`, so when `run_wave` drains the
    /// queue lock-released, multi-node subscribers see all DIRTYs before any
    /// settle. Matches TS's drainPhase model without the per-tier queue
    /// indirection.
    ///
    /// Phase ordering:
    ///   1 → tier 1   (DIRTY)
    ///   2 → tier 3+4 (DATA/RESOLVED + INVALIDATE — the "settle slice")
    ///   3 → tier 5   (COMPLETE/ERROR)
    ///   4 → tier 6   (TEARDOWN)
    ///
    /// Tier 0 (START) is per-subscription (never enters pending_notify) and
    /// tier 2 (PAUSE/RESUME) is delivered through dedicated paths, also
    /// bypassing pending_notify; both are absent from this enumeration.
    ///
    /// Within a single phase, per-node insertion order (IndexMap iteration)
    /// is preserved — an emit on A before B → A's phase-2 messages flush
    /// before B's. Within a single node, message order is preserved.
    fn flush_notifications(&self, s: &mut CoreState) {
        const PHASES: &[&[u8]] = &[
            &[1],    // DIRTY
            &[3, 4], // DATA/RESOLVED + INVALIDATE
            &[5],    // COMPLETE/ERROR
            &[6],    // TEARDOWN
        ];
        // Q-beyond Sub-slice 1 + 2 + 3 (D108, 2026-05-09): pending_notify,
        // deferred_handle_releases, and deferred_flush_jobs all live on
        // per-thread WaveState. Take pending_notify under the WaveState
        // borrow, drop the borrow, run the per-phase loop (no WaveState
        // access in the loop body), then re-borrow WaveState at the end
        // to push the collected jobs and payload-handle releases.
        //
        // /qa F7 (2026-05-10): the `s: &mut CoreState` parameter is
        // currently unused inside the per-phase loop — `pending` was
        // moved off `s` to WaveState by sub-slice 2, and the per-batch
        // sink snapshot is already on the PendingBatch. Kept as a
        // parameter to preserve the caller's `let mut s = lock_state();
        // self.flush_notifications(&mut s);` invocation shape (caller
        // holds the state lock around this call — load-bearing for
        // R1.3.5.a per-tier handshake-vs-flush ordering). NOT a "lock
        // released" marker; the lock guard belongs to the caller and
        // is held throughout this function. A future change that adds
        // an in-loop state read should remove the discard below;
        // removing the parameter would break the caller's ability to
        // express the lock-discipline contract at the call site.
        let _ = &*s; // explicit no-op acknowledgement; lock held by caller.
                     // D217-AMEND-2 (2026-05-16): recycle the per-wave `pending_notify`
                     // map instead of `mem::take`-ing it. `mem::take` installed a
                     // fresh `IndexMap::default()` every wave → a new `ahash::
                     // RandomState` (entropy via `gen_hasher_seed`/`from_keys`) PLUS
                     // RawVec realloc churn (`finish_grow`/`grow_one`) on the next
                     // wave's `queue_notify`. Empirical attribution
                     // (`examples/profile_st_emit.rs` + macOS `sample`): ~1250 of
                     // ~4767 hot-path samples — the dominant §7 floor tax (D217
                     // lever-1 "slab store" falsified; `require_node` was minor).
                     // Fix: swap the live map with a persistent spare so the next
                     // wave fills a capacity-retained, fixed-seed map; process this
                     // wave's full map from the spare slot and `.clear()` it (retain
                     // capacity + hasher; no drop, no realloc, no reseed). Zero
                     // `IndexMap::default()` after thread init.
                     //
                     // The jobs-building loop now runs INSIDE the single WaveState
                     // borrow. This is sound: the loop is pure (iteration +
                     // `Arc::clone` + `Vec` collect; no re-entrant `with_wave_state`
                     // / `lock_state`), and the caller holds the CoreState lock
                     // throughout regardless — so R1.3.5.a per-tier
                     // handshake-vs-flush ordering is unchanged. Refcount discipline
                     // is unchanged: payload-handle releases are still collected into
                     // `deferred_handle_releases` (released post-lock-drop by
                     // `BatchGuard::drop`); `.clear()` runs the same element drops as
                     // the old map-drop and the `PendingPerNode`/`Message` payloads
                     // carry no refcount-releasing `Drop`.
        with_wave_state(|ws| {
            core::mem::swap(&mut ws.pending_notify, &mut ws.pending_notify_recycle);
            // ws.pending_notify         = empty spare (next wave fills it)
            // ws.pending_notify_recycle = THIS wave's full map (below)
            let mut jobs: DeferredJobs = Vec::new();
            let mut releases: Vec<HandleId> = Vec::new();
            {
                let full = &ws.pending_notify_recycle;
                for &phase_tiers in PHASES {
                    for (_node_id, entry) in full {
                        // Slice X4 / D2: iterate batches in arrival
                        // order. Each batch carries its own sink
                        // snapshot frozen at open-time; a batch's
                        // messages flush to ITS sinks only. Within a
                        // single (phase, node), batches stay in arrival
                        // order so emit-order semantics are preserved.
                        for batch in &entry.batches {
                            if batch.sinks.is_empty() {
                                continue;
                            }
                            let phase_msgs: Vec<Message> = batch
                                .messages
                                .iter()
                                .copied()
                                .filter(|m| phase_tiers.contains(&m.tier()))
                                .collect();
                            if phase_msgs.is_empty() {
                                continue;
                            }
                            let sinks_clone: Vec<Sink> =
                                batch.sinks.iter().map(Arc::clone).collect();
                            jobs.push((sinks_clone, phase_msgs));
                        }
                    }
                }
                // Refcount release balances the retain done in
                // `queue_notify` for every payload-bearing message that
                // landed in pending_notify (across ALL batches per
                // node); deferred to post-lock-drop so the binding's
                // release path can't re-enter Core under our lock.
                for entry in full.values() {
                    for msg in entry.iter_messages() {
                        if let Some(h) = msg.payload_handle() {
                            releases.push(h);
                        }
                    }
                }
            }
            ws.deferred_flush_jobs.append(&mut jobs);
            ws.deferred_handle_releases.append(&mut releases);
            // Retain capacity + the existing ahash seed for next wave's
            // swap-in. No drop, no realloc, no reseed.
            ws.pending_notify_recycle.clear();
        });
    }

    /// Take the deferred sink-fire jobs, payload-handle releases,
    /// cleanup-hook fire queue, and pending-wipe queue from `WaveState`.
    /// Callers pair this with `drop(state_guard)` and a subsequent
    /// [`Self::fire_deferred`] call to deliver the wave's sinks, handle
    /// releases, Slice E2 OnInvalidate cleanup hooks, and Slice E2 /qa
    /// Q2(b) eager wipe_ctx fires lock-released.
    ///
    /// Q-beyond Sub-slice 1 (D108, 2026-05-09): `deferred_handle_releases`
    /// source moved to per-thread WaveState — signature takes `&mut WaveState`.
    /// Q-beyond Sub-slice 3 (D108, 2026-05-09): `deferred_flush_jobs`,
    /// `deferred_cleanup_hooks`, and `pending_wipes` all moved to
    /// WaveState. The `_s: &mut CoreState` parameter is now unused but
    /// kept to preserve the call-site lock-discipline ordering (caller
    /// holds the state lock around this call to interleave with prior
    /// `clear_wave_state` per-NodeRecord work).
    pub(crate) fn drain_deferred(_s: &mut CoreState, ws: &mut WaveState) -> WaveDeferred {
        (
            std::mem::take(&mut ws.deferred_flush_jobs),
            std::mem::take(&mut ws.deferred_handle_releases),
            std::mem::take(&mut ws.deferred_cleanup_hooks),
            std::mem::take(&mut ws.pending_wipes),
        )
    }

    /// Fire deferred sink-fire jobs in collected order, then release the
    /// payload handles owed for messages that landed in `pending_notify`
    /// during the wave, then fire any queued Slice E2 OnInvalidate cleanup
    /// hooks. All three phases run lock-released so:
    /// - Sinks that call back into Core (emit, pause, etc.) re-acquire the
    ///   state lock cleanly and run their own nested wave.
    /// - The binding's `release_handle` path can't deadlock against a
    ///   binding-side mutex held by Core.
    /// - User cleanup closures (invoked via `BindingBoundary::cleanup_for`)
    ///   may safely re-enter Core for unrelated nodes.
    ///
    /// **Cleanup-drain panic discipline (D060):** each `cleanup_for` call
    /// is wrapped in `catch_unwind` so a single binding panic doesn't
    /// short-circuit the per-wave drain. All queued cleanup attempts run;
    /// if any panicked, the LAST panic re-raises after the loop completes
    /// (preserving wave-end discipline while still surfacing failures).
    /// Per D060, Core stays panic-naive about user code — bindings own
    /// their host-language panic policy inside `cleanup_for`; this
    /// `catch_unwind` is purely about drain-don't-short-circuit.
    pub(crate) fn fire_deferred(
        &self,
        jobs: DeferredJobs,
        releases: Vec<HandleId>,
        cleanup_hooks: Vec<(crate::handle::NodeId, crate::boundary::CleanupTrigger)>,
        pending_wipes: Vec<crate::handle::NodeId>,
    ) {
        // Slice E2 /qa P1 (2026-05-07): wrap each sink-fire in
        // `catch_unwind` so a panicking sink doesn't unwind out of
        // `fire_deferred` and drop the queued `releases` +
        // `cleanup_hooks`. Mirrors Slice F audit fix A7's per-tier
        // handshake-fire discipline. Without this guard, a sink panic
        // here would silently leak handle retains AND silently drop
        // OnInvalidate cleanup hooks. AssertUnwindSafe is safe because
        // we re-raise the last panic at the end after running every
        // queued fire — drain ordering is preserved.
        let mut last_panic: Option<Box<dyn std::any::Any + Send>> = None;
        for (sinks, msgs) in jobs {
            for sink in &sinks {
                let sink = sink.clone();
                let msgs_ref = &msgs;
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                    sink(msgs_ref);
                }));
                if let Err(payload) = result {
                    last_panic = Some(payload);
                }
            }
        }
        for h in releases {
            self.binding.release_handle(h);
        }
        // Slice E2 (D060): drain cleanup hooks with per-item panic
        // isolation so the loop always completes. AssertUnwindSafe is
        // safe here because we don't rely on logical state being valid
        // post-panic — the panic propagates anyway after the drain ends.
        for (node_id, trigger) in cleanup_hooks {
            let binding = &self.binding;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                binding.cleanup_for(node_id, trigger);
            }));
            if let Err(payload) = result {
                last_panic = Some(payload);
            }
        }
        // Slice E2 /qa Q2(b) (D069): drain eager wipe_ctx queue with the
        // same per-item panic isolation. Fires AFTER cleanup hooks so a
        // resubscribable node's OnInvalidate (or any tier-3+ cleanup that
        // fires in the same wave) sees pre-wipe binding state if it
        // landed in the same wave as the terminal cascade. Mutually
        // exclusive with `Subscription::Drop`'s direct-fire site, but
        // even concurrent fires are idempotent (binding's `wipe_ctx`
        // calls `HashMap::remove` which is a no-op on absent keys).
        for node_id in pending_wipes {
            let binding = &self.binding;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                binding.wipe_ctx(node_id);
            }));
            if let Err(payload) = result {
                last_panic = Some(payload);
            }
        }
        if let Some(payload) = last_panic {
            std::panic::resume_unwind(payload);
        }
    }

    // -------------------------------------------------------------------
    // User-facing batch — coalesce multiple emits into one wave
    // -------------------------------------------------------------------

    /// Coalesce multiple emissions into a single wave. Every `emit` /
    /// `complete` / `error` / `teardown` / `invalidate` call inside `f`
    /// queues its downstream work; the wave drains when `f` returns.
    ///
    /// **R1.3.6.a** — DIRTY still propagates immediately (tier 1 isn't
    /// deferred); only tier-3+ delivery is held until scope exit. **R1.3.6.b**
    /// — repeated emits on the same node coalesce into a single multi-message
    /// delivery (one [`Message::Dirty`] for the wave + one [`Message::Data`]
    /// per emit, all delivered together in the per-node phase-2 pass).
    ///
    /// Nested `batch()` calls share the outer wave; only the outermost call
    /// drives the drain. Re-entrant calls from inside an `emit`/fn (the wave
    /// engine's own `in_tick` re-entrance) compose with this method
    /// transparently — they observe `in_tick = true` and skip drain just
    /// like nested `batch()`.
    ///
    /// On panic inside `f`, the `BatchGuard` returned by the internal
    /// `begin_batch` call drops normally and discards pending tier-3+ work
    /// (subscribers do not observe the half-built wave). See
    /// [`Core::begin_batch`] for the RAII variant if you need explicit control
    /// over the scope boundary.
    pub fn batch<F>(&self, f: F)
    where
        F: FnOnce(),
    {
        let _guard = self.begin_batch();
        f();
    }

    /// RAII batch handle — opens a wave when constructed, drains on drop.
    ///
    /// Mirrors the closure-based [`Self::batch`] but exposes the scope
    /// boundary so callers can compose batches with non-`FnOnce` control
    /// flow (e.g. async-state-machine code paths, or splitting setup and
    /// drain across helper functions).
    ///
    /// ```
    /// use graphrefly_core::{Core, BindingBoundary, NodeRegistration, NodeOpts,
    ///     HandleId, NodeId, FnId, FnResult, DepBatch};
    /// use std::sync::Arc;
    ///
    /// struct Stub;
    /// impl BindingBoundary for Stub {
    ///     fn invoke_fn(&self, _: NodeId, _: FnId, _: &[DepBatch]) -> FnResult {
    ///         FnResult::Noop { tracked: None }
    ///     }
    ///     fn custom_equals(&self, _: FnId, _: HandleId, _: HandleId) -> bool { false }
    ///     fn release_handle(&self, _: HandleId) {}
    /// }
    ///
    /// let core = Core::new(Arc::new(Stub) as Arc<dyn BindingBoundary>);
    /// let state_a = core.register(NodeRegistration {
    ///     deps: vec![], fn_or_op: None,
    ///     opts: NodeOpts { initial: HandleId::new(1), ..Default::default() },
    /// }).unwrap();
    /// let state_b = core.register(NodeRegistration {
    ///     deps: vec![], fn_or_op: None,
    ///     opts: NodeOpts { initial: HandleId::new(2), ..Default::default() },
    /// }).unwrap();
    ///
    /// let g = core.begin_batch();
    /// core.emit(state_a, HandleId::new(10));
    /// core.emit(state_b, HandleId::new(20));
    /// drop(g); // wave drains here
    /// ```
    ///
    /// Like the closure form, nested `begin_batch` calls share the outer
    /// wave (only the outermost guard drains).
    ///
    #[must_use = "BatchGuard drains the wave on drop; assign to a named binding"]
    pub fn begin_batch(&self) -> BatchGuard<'_> {
        // D246/S2c: single-owner ⇒ a `Core` is driven by exactly one
        // thread, so there is no cross-thread wave serialization to
        // acquire (the §7 group/global wave-locks are deleted) and no
        // shard to route to (single shard always).
        self.begin_batch_with_guards()
    }

    /// Begin a batch for a wave seeded at `seed`. Historically this
    /// acquired the per-partition `wave_owner` `ReentrantMutex`es for
    /// every partition transitively touched from `seed` (the Slice Y1
    /// parallelism win). **S2c/D248 deleted that machinery:** `Core` is
    /// single-owner `!Send + !Sync`, so a wave is one uninterrupted
    /// owner-side drain with nothing to lock. This now delegates to the
    /// infallible [`Self::try_begin_batch_for`], which acquires nothing
    /// (the all-`None` single-owner floor); the `seed` parameter is
    /// retained for the declared-group identity routing + D211
    /// minimal-churn (callers compile unchanged). Cross-`Core`
    /// parallelism is host-native via independent per-worker Cores
    /// (actor model), not per-partition locks.
    ///
    /// # Panics
    ///
    /// Panics only if [`Self::try_begin_batch_for`] returns `Err` —
    /// **unreachable**: it is now always `Ok` (scheduling groups are
    /// static identity; no `PartitionOrderViolation`, no epoch retry).
    ///
    /// Slice Y1 / Phase E (2026-05-08); infallible-since S2c (D246).
    #[must_use = "BatchGuard drains the wave on drop; assign to a named binding"]
    pub fn begin_batch_for(&self, seed: crate::handle::NodeId) -> BatchGuard<'_> {
        match self.try_begin_batch_for(seed) {
            Ok(guard) => guard,
            Err(e) => panic!("{e}"),
        }
    }

    /// §7: now-infallible variant of [`Self::begin_batch_for`]. The
    /// `Result` is retained so existing callers (`try_run_wave_for`,
    /// the `?`-using wave entry points) compile unchanged (D211); it is
    /// **always `Ok`** — scheduling groups are static + user-declared
    /// and the touched-group set is acquired sorted upfront, so there is
    /// no `PartitionOrderViolation` and no epoch retry. For an
    /// all-`None` cascade this acquires nothing (single-threaded floor).
    #[allow(clippy::unnecessary_wraps)]
    pub(crate) fn try_begin_batch_for(
        &self,
        seed: crate::handle::NodeId,
    ) -> Result<BatchGuard<'_>, crate::node::PartitionOrderViolation> {
        // D246/S2c: single-owner ⇒ no cross-thread wave serialization
        // and no shard routing. The `Result`/`seed` are retained so
        // callers (`try_run_wave_for`, the `?`-using wave entry points)
        // compile unchanged (D211); always `Ok`. `seed` is unused now —
        // the §7 touched-group walk is deleted.
        let _ = seed;
        Ok(self.begin_batch_with_guards())
    }

    /// Is this thread currently inside an owning wave on this Core?
    /// Per-(Core, thread) — see [`IN_TICK_OWNED`]. Read on the wave-owner
    /// thread (e.g. by `commit_emission` to decide cache-snapshot taking).
    /// `#[must_use]`: a discarded result silently loses the
    /// ownership/nesting decision (a classic predicate-misuse bug).
    #[must_use]
    fn in_tick(&self) -> bool {
        IN_TICK_OWNED.with(|s| s.borrow().contains(&self.generation))
    }

    /// Claim wave ownership for this (Core, thread). Returns `true` iff
    /// this call is the outermost entry (slot was absent) — i.e.
    /// `owns_tick`; `false` for nested same-(Core, thread) re-entry.
    /// `AHashSet::insert` returns `true` exactly when the value was newly
    /// inserted, which is precisely the `owns_tick` semantics.
    fn claim_in_tick(&self) -> bool {
        IN_TICK_OWNED.with(|s| s.borrow_mut().insert(self.generation))
    }

    /// Release wave ownership for this (Core, thread). Called by the
    /// owning [`BatchGuard::drop`] only — after the `!owns_tick`
    /// early-return, so a nested guard never releases — explicitly at
    /// each of the three exit points, always AFTER the wave drain +
    /// WaveState cleanup and BEFORE `fire_deferred` (so a re-entrant sink
    /// emit runs as a fresh owning wave): (1) the closure-body-panic
    /// branch, (2) the drain-phase-panic `catch_unwind` arm (before
    /// `resume_unwind`), (3) the success path's locked cleanup block.
    /// Released exactly once per (Core, thread, wave); idempotent
    /// regardless (`AHashSet::remove` of an absent key is a no-op).
    fn clear_in_tick(&self) {
        IN_TICK_OWNED.with(|s| {
            s.borrow_mut().remove(&self.generation);
        });
    }

    /// Internal helper: claim `in_tick` and assemble a [`BatchGuard`].
    /// D246/S2c: single-owner ⇒ no wave-owner / shard guards to carry.
    fn begin_batch_with_guards(&self) -> BatchGuard<'_> {
        // Claim wave ownership for this (Core, thread). Keyed per-(Core,
        // thread) in the `IN_TICK_OWNED` thread_local (see its doc for
        // the cross-Core / disjoint-partition / nested-re-entry rationale)
        // — no state lock needed, since `in_tick` has no cross-thread read
        // requirement.
        let owns_tick = self.claim_in_tick();
        // D1 patch (2026-05-09): defensive wave-start clear of the
        // per-thread Slice G tier3 tracker on outermost owning entry.
        // The thread-local is cleared at outermost BatchGuard drop on
        // both success + panic paths; this start-clear is belt-and-
        // suspenders against panic paths that bypass Drop (catch_unwind
        // can interleave with thread reuse — e.g. cargo's test-runner
        // thread pool — and propagate stale entries from a prior
        // panicked test's wave that didn't fully unwind through
        // BatchGuard::drop).
        if owns_tick {
            tier3_clear();
            // Q-beyond Sub-slice 1 (D108, 2026-05-09): defensive wave-start
            // clear of WaveState's non-retain-holding fields. Mirrors the
            // tier3 defensive-clear above. Retain-holding fields
            // (wave_cache_snapshots / deferred_handle_releases) MUST be
            // empty here — outermost BatchGuard::drop drains them on both
            // success + panic paths.
            wave_state_clear_outermost();
        }
        BatchGuard {
            core: self,
            owns_tick,
            _not_send: std::marker::PhantomData,
        }
    }
}

/// RAII guard returned by [`Core::begin_batch`].
///
/// While alive, suppresses per-emit wave drains — multiple `emit` /
/// `complete` / `error` / `teardown` / `invalidate` calls coalesce into one
/// wave. On drop:
/// - Outermost guard: drains the wave (fires sinks, runs cleanup, clears
///   in-tick).
/// - Nested guard (an outer `BatchGuard` or an in-progress wave already owns
///   the in-tick flag): silently no-ops.
///
/// On thread panic during the closure body, the drop path discards pending
/// tier-3+ delivery rather than firing sinks (avoids cascading panics).
/// Subscribers observe **no tier-3+ delivery for the panicked wave**.
/// State-node cache writes that already executed inside the closure are
/// rolled back via wave-cache snapshots — `cache_of(s)` returns the pre-
/// panic value. The atomicity guarantee covers both sink-observability and
/// cache state.
///
/// # Thread safety
///
/// `BatchGuard` is **`!Send`** by design. `begin_batch` claims the
/// per-(Core, thread) `in_tick` ownership slot on the calling thread;
/// sending the guard to another thread and dropping it there would
/// clear `in_tick` against the wrong thread's slot, breaking the
/// per-(Core, thread) "I own the wave scope" semantic. D246/S2c:
/// single-owner ⇒ the §7 per-partition `wave_owner` re-entrant
/// mutex(es) are deleted; `!Send` is now enforced solely by the
/// `PhantomData<*const ()>` marker.
///
/// ```compile_fail
/// use graphrefly_core::{BatchGuard, BindingBoundary, Core, DepBatch, FnId, FnResult, HandleId, NodeId};
/// use std::sync::Arc;
///
/// struct Stub;
/// impl BindingBoundary for Stub {
///     fn invoke_fn(&self, _: NodeId, _: FnId, _: &[DepBatch]) -> FnResult {
///         FnResult::Noop { tracked: None }
///     }
///     fn custom_equals(&self, _: FnId, _: HandleId, _: HandleId) -> bool { false }
///     fn release_handle(&self, _: HandleId) {}
/// }
/// fn requires_send<T: Send>(_: T) {}
/// let core = Core::new(Arc::new(Stub) as Arc<dyn BindingBoundary>);
/// let guard = core.begin_batch();
/// requires_send(guard); // <- compile_fail: BatchGuard is !Send.
/// ```
#[must_use = "BatchGuard drains the wave on drop; assign to a named binding"]
pub struct BatchGuard<'a> {
    // S2b (D223): borrows `&'a Core` — `Core` is no longer `Clone`
    // (owned-by-value, relocates between workers). The guard lives
    // entirely within the wave scope of the `&self` that produced it
    // (`begin_batch*` takes `&self`), so `self` strictly outlives the
    // guard — identical to S1's `FiringGuard<'a>`. `!Send` via
    // `_not_send`.
    core: &'a Core,
    owns_tick: bool,
    /// D246/S2c: single-owner ⇒ no per-partition wave-owner guards and
    /// no ambient shard-key guard (both were shared-Core-era §7
    /// machinery, now deleted). `BatchGuard` stays `!Send` purely via
    /// this `PhantomData<*const ()>` — a wave guard is wave-scoped to
    /// the one owner thread and must not cross threads.
    _not_send: std::marker::PhantomData<*const ()>,
}

impl BatchGuard<'_> {
    /// Panic-discard cleanup for the owning guard: drop pending wave
    /// work, release queued payload + handle retains lock-released,
    /// restore pre-wave cache snapshots, clear per-thread `WaveState` +
    /// the Slice-G tier3 tracker, and discard deferred producer ops.
    ///
    /// Shared by BOTH panic origins so a drain-phase fn/sink panic gets
    /// the identical `BatchGuard` atomicity guarantee as a closure-body
    /// panic: (1) the `std::thread::panicking()` branch (panic propagated
    /// from the wave's *closure body* — drop runs during that unwind),
    /// and (2) the success-path `catch_unwind` around `drain_and_flush()`
    /// (a fn/sink panic that escaped the inner per-call `catch_unwind`
    /// isolation while drop was NOT already unwinding). /qa D047.
    ///
    /// Does NOT release `in_tick` — each `BatchGuard::drop` exit path
    /// calls `clear_in_tick()` explicitly, after this cleanup and before
    /// `fire_deferred` (so a re-entrant sink emit runs as a fresh owning
    /// wave).
    fn discard_wave_cleanup(&self) {
        let (pending, pending_recycle, deferred_releases, restored_releases) = {
            let mut s = self.core.lock_state();
            // WaveState borrowed alongside state for panic-discard
            // cleanup. The WaveState borrow is per-thread, independent of
            // state. `in_tick` is per-(Core, thread) (`IN_TICK_OWNED`),
            // released separately by the explicit `clear_in_tick` on each
            // exit path; this method only drains/cleans the per-thread
            // WaveState retain-fields.
            with_wave_state(|ws| {
                let pending = std::mem::take(&mut ws.pending_notify);
                // D217-AMEND-2 / QA: `pending_notify_recycle` is the
                // ONE retain-capable WaveState field whose retains live
                // in a *persistent* slot (not an owned local that drops
                // on unwind). On the success path it is empty here
                // (`flush_notifications` cleared it). But if a wave
                // panic-discards mid-`flush_notifications` AFTER the
                // swap but before `.clear()`, this slot holds the full
                // wave's payload-retaining map while `pending_notify` is
                // the empty spare. Drain it symmetrically so the same
                // "every retain field released on the panic path"
                // invariant the sibling fields uphold also covers
                // recycle (closes the leak + the stale-entry-injected-
                // into-next-wave hazard; success path: this is a no-op
                // take of an already-empty map).
                let pending_recycle = std::mem::take(&mut ws.pending_notify_recycle);
                let _: DeferredJobs = std::mem::take(&mut ws.deferred_flush_jobs);
                ws.pending_fires.clear();
                let restored = self.core.restore_wave_cache_snapshots(&mut s, ws);
                // clear_wave_state pushes batch-handle releases into
                // ws.deferred_handle_releases, so take ws's queue AFTER
                // the clear.
                s.clear_wave_state(ws);
                // Step 2a (D220-EXEC): defensive `currently_firing`
                // clear, relocated here from `CoreState::clear_wave_state`
                // (the field moved to the separate `CoreShared` region;
                // `St`'s `.shared` reaches it, a `&mut CoreState` can't).
                // Same wave-end point; `FiringGuard` RAII already
                // balances push/pop — this is the belt-and-suspenders
                // net for a future guard-bypassing path.
                s.shared.currently_firing.clear();
                ws.clear_wave_state();
                let deferred_releases = std::mem::take(&mut ws.deferred_handle_releases);
                // Slice E2 (D061): panic-discard wave drops queued
                // OnInvalidate cleanup hooks SILENTLY. Bindings using
                // OnInvalidate for external-resource cleanup MUST
                // idempotent-cleanup at process exit / next successful
                // invalidate. Mirrors A3 `pending_pause_overflow`
                // panic-discard precedent.
                let _: Vec<(crate::handle::NodeId, crate::boundary::CleanupTrigger)> =
                    std::mem::take(&mut ws.deferred_cleanup_hooks);
                // Slice E2 /qa Q2(b) (D069): same panic-discard discipline
                // for the eager-wipe queue. A panic-discarded wave drops
                // queued `wipe_ctx` fires silently; the binding-side
                // `NodeCtxState` entry remains until the next successful
                // terminate-with-no-subs cycle (or until `Core` drops).
                // This mirrors D061's external-resource-cleanup gap and
                // is documented similarly.
                let _: Vec<crate::handle::NodeId> = std::mem::take(&mut ws.pending_wipes);
                (pending, pending_recycle, deferred_releases, restored)
            })
        };
        // Lock dropped — release retains lock-released so the binding
        // can't deadlock against an internal binding mutex.
        for entry in pending.values() {
            for msg in entry.iter_messages() {
                if let Some(h) = msg.payload_handle() {
                    self.core.binding.release_handle(h);
                }
            }
        }
        // Symmetric with the `pending` loop above (D217-AMEND-2 / QA):
        // releases the full-map retains stranded in the recycle slot by
        // a panic between `flush_notifications`'s swap and clear. Empty
        // (no-op) on every non-panic path.
        for entry in pending_recycle.values() {
            for msg in entry.iter_messages() {
                if let Some(h) = msg.payload_handle() {
                    self.core.binding.release_handle(h);
                }
            }
        }
        for h in deferred_releases {
            self.core.binding.release_handle(h);
        }
        for h in restored_releases {
            self.core.binding.release_handle(h);
        }
        // D1 patch (2026-05-09): clear the per-thread Slice G tier3
        // tracker on outermost wave-end (panic-discard path). The
        // thread-local outlives the BatchGuard otherwise — cargo's
        // thread reuse across tests would propagate stale entries.
        tier3_clear();
        // §7 (D208–D211): the panic-path "discard deferred producer
        // ops" block is DELETED. There is no `deferred_producer_ops`
        // queue — producer ops execute immediately
        // (`Core::push_deferred_producer_op`), so on a panic-discard
        // there is nothing queued to release/drop.
    }
}

impl Drop for BatchGuard<'_> {
    fn drop(&mut self) {
        if !self.owns_tick {
            // Nested / non-owning guard: never claimed ownership, so it
            // must never release it. The owning guard's RAII releaser
            // (below) is the single clear site.
            return;
        }
        // Wave-ownership (`in_tick`) release discipline. `clear_in_tick`
        // must run AFTER the wave's drain + WaveState cleanup but BEFORE
        // `fire_deferred` (sinks), on every exit path:
        //
        // - **Before `fire_deferred` (load-bearing):** a sink re-entering
        //   `Core::emit` / `complete` from a flush callback must run as a
        //   fresh OWNING wave (so its own emissions drain + deliver). If
        //   `in_tick` were still owned during `fire_deferred`, that
        //   re-entrant emit would be a non-owning no-op and its data
        //   silently lost (regression caught by
        //   `lock_discipline::sink_can_reenter_core_via_emit`). This is
        //   why each path clears explicitly at the right point — NOT via
        //   an end-of-`drop` RAII guard (which would clear *after*
        //   `fire_deferred`).
        // - **/qa hardening (D047):** a fn/sink panic in the drain phase
        //   can escape the per-call `catch_unwind` isolation (e.g. a
        //   derived fn panicking when fired). Drop is NOT already
        //   unwinding, so it would otherwise skip BOTH the WaveState
        //   drain (→ next owning wave trips `wave_state_clear_outermost`)
        //   AND the `in_tick` clear (pre-D047 the explicit clear had this
        //   same window). Catching the drain panic, running the shared
        //   discard cleanup + `clear_in_tick`, then `resume_unwind` gives
        //   a drain-phase panic the identical atomicity as a
        //   closure-body panic.
        if std::thread::panicking() {
            // Closure-body panic — drop runs during that unwind. Discard
            // pending wave work (don't fire sinks mid-unwind — a sink
            // panic then aborts the process), release queued retains,
            // restore caches, then release ownership.
            self.discard_wave_cleanup();
            self.core.clear_in_tick();
            return;
        }
        if let Err(payload) =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.core.drain_and_flush()))
        {
            self.discard_wave_cleanup();
            self.core.clear_in_tick();
            std::panic::resume_unwind(payload);
        }
        // Wave cleanup + extract deferred jobs under the lock.
        let (jobs, releases, cleanup_hooks, pending_wipes, snapshot_releases) = {
            let mut s = self.core.lock_state();
            // Q-beyond Sub-slice 1 + 3 (D108, 2026-05-09): WaveState
            // borrowed alongside state for wave-end cleanup. Per-thread;
            // independent of state. Sub-slice 3 moved deferred_* drains
            // into WaveState. /qa F1+F2 (2026-05-10) reverted in_tick +
            // currently_firing back to CoreState — clear via
            // CoreState::clear_wave_state under the held state lock.
            let result = with_wave_state(|ws| {
                s.clear_wave_state(ws);
                // Step 2a (D220-EXEC): defensive `currently_firing`
                // clear, relocated here from `CoreState::clear_wave_state`
                // (the field moved to the separate `CoreShared` region;
                // `St`'s `.shared` reaches it, a `&mut CoreState` can't).
                // Same wave-end point; `FiringGuard` RAII already
                // balances push/pop — this is the belt-and-suspenders
                // net for a future guard-bypassing path.
                s.shared.currently_firing.clear();
                ws.clear_wave_state();
                // /qa A1 (2026-05-09) discipline preserved: drain snapshot
                // retains under lock, release lock-released below to avoid
                // binding re-entrance under held mutex / borrow.
                let snapshot_releases = Core::drain_wave_cache_snapshots(ws);
                // `drain_deferred` takes `deferred_flush_jobs` +
                // `deferred_handle_releases` (incl. rotation releases pushed
                // by `clear_wave_state` above) + Slice E2
                // `deferred_cleanup_hooks` + Slice E2 /qa Q2(b)
                // `pending_wipes` — all from WaveState post-Sub-slice-3.
                let (jobs, releases, hooks, wipes) = Core::drain_deferred(&mut s, ws);
                (jobs, releases, hooks, wipes, snapshot_releases)
            });
            // Release wave ownership now — AFTER drain + WaveState
            // cleanup, BEFORE `fire_deferred` below. Load-bearing: a sink
            // re-entering Core from a flush callback must observe
            // `in_tick` clear so its emit runs as a fresh owning wave.
            // (Mirrors the placement of the pre-D047 `s.in_tick = false`;
            // the drain-phase-panic window that placement had is closed
            // by the `catch_unwind` above.)
            self.core.clear_in_tick();
            result
        };
        // Lock dropped — fire deferred sinks + release retains + fire
        // cleanup hooks (Slice E2 OnInvalidate, D060 catch_unwind drain)
        // + fire eager wipes (D069).
        self.core
            .fire_deferred(jobs, releases, cleanup_hooks, pending_wipes);
        // /qa A1 fix (2026-05-09): release wave_cache_snapshots retains
        // lock-released. Pre-A1 these were released inside the held
        // state + cross_partition locks; binding finalizers re-entering
        // Core would deadlock against either mutex. Drained earlier
        // under the lock; released here after both mutexes dropped and
        // sinks have fired.
        for h in snapshot_releases {
            self.core.binding.release_handle(h);
        }
        // D1 patch (2026-05-09): clear the per-thread Slice G tier3
        // tracker at outermost wave-end (success path). Mirrors the
        // panic-discard branch above. Thread-local outlives BatchGuard
        // by default; cargo's thread-reuse across tests would propagate
        // stale entries. Cleared after sinks fire (sink callbacks may
        // re-enter Core via emit and could read the tier3 set
        // mid-wave; the wave is over here so clearing is safe).
        tier3_clear();
        // D246/S2c: no per-partition wave-owner guards to release
        // (single-owner ⇒ the §7 wave-locks are deleted).
        // Phase H+ STRICT (D115): drain deferred producer ops at
        // wave-end (now a no-op shim — §7 deleted the deferred-producer
        // queue; retained so call sites compile unchanged, D211).
        self.core.drain_deferred_producer_ops();
    }
}
