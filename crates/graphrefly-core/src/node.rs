//! The dispatcher — node registration, subscription, wave engine.
//!
//! Mirrors `~/src/graphrefly-ts/src/__experiments__/handle-core/core.ts`
//! (the Phase 13.6 brainstorm prototype, ~370 lines, 22 invariant tests).
//!
//! # Scope (M1 dispatcher + Slice A+B parity, closed 2026-05-05)
//!
//! - State + derived + dynamic node registration.
//! - Subscribe / unsubscribe with push-on-subscribe (R1.2.3).
//! - RAII [`Subscription`] with Drop-based deregister (§10.12).
//! - DIRTY → DATA / RESOLVED ordering (R1.3.1.b two-phase push).
//! - Equals-substitution (R1.3.2): identity is zero-FFI; custom crosses boundary.
//! - First-run gate (R2.5.3) — fn does not fire until every dep has a handle.
//! - Diamond resolution — one fn fire per wave even with shared upstream.
//! - `set_deps()` atomic dep mutation with cycle detection + Phase 13.8 Q1
//!   terminal-rejection policy (R3.3.1).
//! - PAUSE / RESUME with lockId set + replay buffer (R1.2.6, R2.6, §10.2).
//! - INVALIDATE broadcast + cascade with R1.4 idempotency.
//! - COMPLETE / ERROR cascade + Lock 2.B auto-cascade gating
//!   (ERROR dominates COMPLETE; first error wins).
//! - TEARDOWN auto-precedes COMPLETE (R2.6.4 / Lock 6.F) +
//!   `has_received_teardown` idempotency.
//! - Meta TEARDOWN ordering (R1.3.9.d) — companions tear down before parent.
//! - Resubscribable terminal lifecycle (R2.2.7, R2.5.3) — late subscribe to a
//!   resubscribable terminal node resets lifecycle, except after TEARDOWN
//!   (per F3 audit guard: TEARDOWN is permanent).
//!
//! # Module split (Slice C-1, 2026-05-05)
//!
//! Wave-engine internals (drain loop, fire selection, emission commit, sink
//! dispatch) live in [`crate::batch`]. The split is purely organizational —
//! the methods are still on `Core`. See `batch.rs` for the wave-engine
//! entry points (`run_wave`, `drain_and_flush`, `commit_emission`,
//! `queue_notify`, `deliver_data_to_consumer`).
//!
//! # Out of scope (later slices / milestones)
//!
//! - Deactivation cleanup (RAM nodes clear cache when sink count → 0) — M2.
//!
//! See [`migration-status.md`](../../../docs/migration-status.md) for the
//! milestone tracker and [`porting-deferred.md`](../../../docs/porting-deferred.md)
//! for surfaced concerns deferred to evidence-driven slices.
//!
//! # Re-entrance discipline (Slice A close, M1: fully lock-released)
//!
//! - **Wave-end sink fires** drop the state lock first. A subscriber's sink
//!   that calls back into `Core::emit` / `pause` / `resume` / `invalidate` /
//!   `complete` / `error` / `teardown` re-acquires the lock cleanly and runs
//!   a nested wave (`WaveState::in_tick` is cleared before the
//!   deferred-fire phase).
//! - **`BindingBoundary::invoke_fn`** fires lock-released. The wave engine
//!   acquires + drops the state lock per fn-fire iteration around the
//!   `invoke_fn` callback. User fns may re-enter `Core::emit` / `pause` /
//!   etc. and run a nested wave.
//! - **`BindingBoundary::custom_equals`** fires lock-released.
//!   `commit_emission` brackets the equals check around a lock release;
//!   custom equals oracles may re-enter Core safely.
//! - **Subscribe-time handshake** also fires lock-released. [`Core::subscribe`]
//!   acquires the [`Core::wave_owner`] re-entrant mutex first (cross-thread
//!   serialization), installs the sink under the state lock, drops the state
//!   lock, then fires the per-tier handshake (`[Start]` / `[Data(cache)]?` /
//!   `[Complete]?` / `[Error(h)]?` / `[Teardown]?` per R1.3.5.a) lock-released.
//!   A handshake-time sink callback may re-enter Core (`emit` / `complete` /
//!   `error` / `subscribe`); same-thread re-entry passes through `wave_owner`
//!   transparently. Cross-thread emits block on `wave_owner` until the
//!   subscribe path drops it, preserving R1.3.5.a happens-after ordering.

use std::collections::VecDeque;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use parking_lot::{ArcReentrantMutexGuard, Mutex, MutexGuard};

/// Held guard from `parking_lot::ReentrantMutex::lock_arc()` on a
/// partition's `wave_owner`. `!Send` per `parking_lot::ReentrantMutex`'s
/// thread-affinity contract (the inner guard is `!Send`; the wrapper
/// inherits) — the type-level `!Send` flows into
/// [`crate::batch::BatchGuard::_wave_guards`] so any attempt to send
/// the batch guard across threads fails to compile.
///
/// **Phase H+ option (d) limited variant (2026-05-09):** the guard's
/// [`Drop`] pops `sid` from the [`held_partitions`] thread-local so
/// the ascending-order check on the next acquire sees the correct
/// "currently held" set. Re-entrant acquires (same thread, same
/// partition) increment a refcount in the thread-local; final drop
/// removes the entry.
///
/// Slice Y1 / Phase E (2026-05-08); Phase H+ wrapper (2026-05-09).
pub(crate) struct WaveOwnerGuard {
    /// Drop order: the wrapper's Drop runs FIRST (pops `held_partitions`),
    /// then `inner` drops automatically (releases the parking_lot lock).
    /// Field-declaration order matters in Rust: the wrapper drops top-
    /// down by default, so `inner` is listed AFTER `sid` so the wrapper's
    /// custom Drop runs on the whole struct first, releasing the
    /// thread-local entry under our control before the inner guard
    /// hits parking_lot's release path.
    sid: crate::subgraph::SubgraphId,
    /// `#[allow(dead_code)]`: the inner guard is held to keep the
    /// parking_lot::ReentrantMutex acquired for the wave's duration;
    /// it's never read, only its `Drop` matters.
    #[allow(dead_code)]
    inner: ArcReentrantMutexGuard<parking_lot::RawMutex, parking_lot::RawThreadId, ()>,
}

impl Drop for WaveOwnerGuard {
    fn drop(&mut self) {
        // `held_partitions::release` returns `bool was_outermost`. The
        // outermost-release signal is consumed by [`crate::batch::BatchGuard::drop`]
        // for the per-thread `TIER3_EMITTED_THIS_WAVE` clear (D1 patch,
        // 2026-05-09 — Slice G coalescing tracker is keyed by thread,
        // not by partition, so the clear lives on `BatchGuard` not here).
        // We discard the bool — no per-guard cleanup remains.
        let _ = held_partitions::release(self.sid);
        // `inner` drops automatically after this — releases the
        // parking_lot::ReentrantMutex (decrementing parking_lot's
        // own internal re-entry counter; only the FINAL release
        // unparks waiters).
    }
}

/// Phase H+ STRICT variant — thread-local infrastructure for
/// cross-partition acquire-during-fire / cross-partition
/// acquire-during-wave deadlock detection (D115, 2026-05-10).
///
/// **Protocol invariant enforced:** whenever this thread already
/// holds at least one partition wave_owner (HELD non-empty), every
/// NEW partition this thread tries to acquire must have an id
/// strictly greater than every partition currently held. Re-entrant
/// acquires (same thread, same partition that's already held) bypass
/// the check (they're fine — `parking_lot::ReentrantMutex` allows
/// same-thread re-entry transparently).
///
/// Without this check, two threads each doing nested cross-partition
/// acquires within an active wave could form an AB/BA cycle: thread A
/// holds X, attempts Y (Y < X); concurrently thread B holds Y,
/// attempts X (X > Y, ascending-OK from B's POV). A's acquire on Y
/// blocks behind B; B's acquire on X blocks behind A. Cycle.
///
/// **No producer carve-out:** the prior limited variant suppressed
/// the check during producer build/sink closures via an
/// `IN_PRODUCER_BUILD` refcount. The STRICT variant (D115) removes
/// this carve-out entirely. Instead, `check_and_acquire` returns a
/// typed `Result<(), PartitionOrderViolation>` error, and callers
/// that would violate ascending order (producer-pattern operator
/// sinks) defer the operation to wave-end via the per-Core
/// `deferred_producer_ops` queue. The defer-and-retry approach
/// preserves deadlock-freedom without suppressing the check.
mod held_partitions {
    use crate::subgraph::SubgraphId;
    use smallvec::SmallVec;
    use std::cell::RefCell;

    /// Inline-storage capacity for the per-thread held-partitions set.
    /// 4 mirrors the same inline limit used elsewhere in the codebase
    /// (`compute_touched_partitions` returns `SmallVec<[SubgraphId; 4]>`,
    /// `BatchGuard::_wave_guards` is `SmallVec<[WaveOwnerGuard; 4]>`).
    /// In the typical wave a single thread holds 1–3 partitions; spillover
    /// to the heap costs allocation but is correct.
    const HELD_INLINE: usize = 4;

    thread_local! {
        /// Currently-held partitions on this thread, as `(SubgraphId, refcount)`
        /// pairs in arbitrary order. The refcount mirrors
        /// `parking_lot::ReentrantMutex`'s internal counter (we can't query
        /// parking_lot's directly).
        ///
        /// `SmallVec<[_; HELD_INLINE]>` over `BTreeMap<_, _>`: under the
        /// expected workload (≤4 partitions held simultaneously per wave) the
        /// inline-storage SmallVec keeps the entire set in stack memory with
        /// no allocation, no Box-per-node, and contiguous cache layout. Linear
        /// scans are branch-predictable and faster than BTreeMap's logn
        /// pointer-chasing for tiny N. Phase J post-widening bench
        /// (`migration-status.md` 2026-05-09) reported 14–25% overhead vs the
        /// pre-widening baseline, attributed in part to BTreeMap allocation
        /// costs on the hot path. This swap recovers part of that overhead.
        ///
        /// Bookkeeping is unconditional — every acquire bumps the
        /// refcount, every release decrements. The CHECK gate
        /// (`!held.is_empty() && !already_held`) is what
        /// distinguishes "first-time acquire on a fresh thread"
        /// (allowed, held empty) from "nested acquire while we
        /// already hold something" (must be ascending).
        static HELD: RefCell<SmallVec<[(SubgraphId, u32); HELD_INLINE]>>
            = const { RefCell::new(SmallVec::new_const()) };
    }

    /// Phase H+ STRICT check + bookkeeping. Called BEFORE acquiring the
    /// partition's parking_lot::ReentrantMutex.
    ///
    /// Panics with a clear diagnostic if:
    /// - HELD is non-empty (this thread already holds ≥1 partition),
    /// - AND we're NOT inside a producer build closure,
    /// - AND `sid` is NOT already held by this thread,
    /// - AND `sid <= max(currently held)`.
    ///
    /// Otherwise: increments the refcount for `sid` (creating the
    /// entry if needed) and returns. The caller MUST pair every
    /// call with a [`release`] when the guard drops.
    ///
    /// **Important note on cross-thread vs same-thread:** this check
    /// is a SAME-THREAD invariant — it catches a thread acquiring
    /// out of order from itself. Cross-thread AB/BA cycles between
    /// threads with disjoint same-thread acquisition orders are
    /// prevented at a different layer (the
    /// `compute_touched_partitions` upfront-acquire-all-ascending
    /// rule in `Core::begin_batch_for`). This thread-local check
    /// adds the layer that prevents a same-thread descending acquire
    /// from creating the FIRST half of a cross-thread cycle.
    pub(crate) fn check_and_acquire(sid: SubgraphId) -> Result<(), super::PartitionOrderViolation> {
        HELD.with(|h| {
            let mut held = h.borrow_mut();
            // Gate: held non-empty (we're nested). First-time acquires
            // on a fresh thread (held empty) skip the check — there's
            // nothing to compare against.
            let already_held = held.iter().any(|(s, _)| *s == sid);
            if !held.is_empty() && !already_held {
                // Linear-scan max over the inline storage (typical N ≤ 4).
                // Branch-predictable and cache-local; no allocation.
                if let Some(max_held) = held.iter().map(|(s, _)| *s).max() {
                    if sid <= max_held {
                        // Drop the borrow before returning Err so
                        // the caller doesn't see a still-borrowed RefCell.
                        let new_id = sid;
                        drop(held);
                        return Err(super::PartitionOrderViolation {
                            attempted: new_id,
                            max_held,
                        });
                    }
                }
            }
            // Bookkeeping: increment refcount. `checked_add(1)` so
            // overflow surfaces (would indicate an unbounded
            // re-entrance — a real bug). Linear find-or-push.
            if let Some((_, count)) = held.iter_mut().find(|(s, _)| *s == sid) {
                *count = count.checked_add(1).expect(
                    "held_partitions refcount overflow — unbounded \
                     same-partition re-entrance. Should be bounded by the \
                     protocol's MAX_BATCH_DRAIN_ITERATIONS cap.",
                );
            } else {
                held.push((sid, 1));
            }
            Ok(())
        })
    }

    /// Decrement the refcount for `sid`; remove the entry if it
    /// hits zero. Called from [`super::WaveOwnerGuard::drop`] AND
    /// from the retry / panic paths in
    /// [`super::Core::partition_wave_owner_lock_arc`] to ensure the
    /// refcount stays balanced under all unwind / retry / exhaust
    /// paths.
    ///
    /// Returns `true` iff this release brought the partition's
    /// refcount on this thread to zero — i.e. this was the OUTERMOST
    /// guard for `sid` on this thread. [`super::WaveOwnerGuard::drop`]
    /// uses this signal to clear per-partition wave state (Q3) on
    /// outermost release only; inner re-entrant guard drops must NOT
    /// clear (a containing wave is still active and still holds
    /// in-flight wave state). The `partition_wave_owner_lock_arc`
    /// retry / panic paths ignore the return value because the
    /// partition state hadn't been touched yet on those paths
    /// (clearing would be a no-op anyway).
    pub(crate) fn release(sid: SubgraphId) -> bool {
        HELD.with(|h| {
            let mut held = h.borrow_mut();
            if let Some(idx) = held.iter().position(|(s, _)| *s == sid) {
                let count = &mut held[idx].1;
                // /qa A3 fix (2026-05-09): debug_assert the refcount is
                // non-zero before decrement. A `release(sid)` on an
                // entry with `count == 0` indicates a bookkeeping bug
                // (a release without a matching `check_and_acquire`,
                // or a logic error in caller), but the legacy
                // saturating_sub silently returned `was_outermost=true`
                // and removed the entry — masking the bug. Surface
                // in dev/test builds; release builds preserve the
                // saturating behavior.
                debug_assert!(
                    *count > 0,
                    "held_partitions::release({sid:?}): refcount underflow — \
                     release without matching check_and_acquire (caller bug)"
                );
                *count = count.saturating_sub(1);
                if *count == 0 {
                    // `swap_remove` is O(1) and order-irrelevant: the
                    // CHECK gate computes max via linear scan and does
                    // not depend on iteration order.
                    held.swap_remove(idx);
                    return true;
                }
            } else {
                // /qa A3 fix (2026-05-09): same intent — release on a
                // sid that's not in HELD is a bookkeeping bug. Surface
                // in dev/test builds.
                debug_assert!(
                    false,
                    "held_partitions::release({sid:?}): sid not in HELD — \
                     double-drop or stray release (caller bug)"
                );
            }
            false
        })
    }

    /// Returns `true` if this thread currently holds any partition
    /// wave_owner. Used by `BatchGuard::drop` to skip the deferred-ops
    /// drain when we're still nested inside an outer wave_guard scope
    /// (Phase H+ STRICT, D115).
    pub(crate) fn any_held() -> bool {
        HELD.with(|h| !h.borrow().is_empty())
    }

    /// Test-only: read the current thread's held-partitions snapshot.
    /// Used by post-panic regression assertions to verify the
    /// thread-local stays clean even when the H+ check unwinds the
    /// stack (so cargo's thread-reuse doesn't propagate corrupted
    /// state to subsequent tests). `pub` (gated by
    /// `cfg(any(test, debug_assertions))`) so integration tests
    /// outside the crate can read it.
    #[cfg(any(test, debug_assertions))]
    #[must_use]
    pub fn held_snapshot_for_tests() -> Vec<(SubgraphId, u32)> {
        // /qa A2 fix (2026-05-09): sort by SubgraphId so the snapshot
        // returns ascending order — matches the BTreeMap-iteration
        // contract that pre-/qa SmallVec swap consumers might rely on.
        // Test consumers currently only assert `is_empty()`, but the
        // ordered shape is the safer default for future tests that
        // assert specific entries.
        let mut v: Vec<(SubgraphId, u32)> = HELD.with(|h| h.borrow().to_vec());
        v.sort_unstable_by_key(|(s, _)| *s);
        v
    }
}

/// Test-only re-exports for integration tests under
/// `crates/graphrefly-core/tests/`. Gated `#[cfg(any(test, debug_assertions))]`
/// so they don't leak into release builds. Public visibility is
/// required because integration tests live outside the crate.
#[cfg(any(test, debug_assertions))]
pub use held_partitions::held_snapshot_for_tests;
use smallvec::SmallVec;
use thiserror::Error;

use crate::boundary::{BindingBoundary, CleanupTrigger};
use crate::clock::monotonic_ns;
use crate::handle::{FnId, HandleId, LockId, NodeId, NO_HANDLE};
use crate::message::Message;

/// Terminal-lifecycle state — once set on a node, the node will not emit
/// further DATA; per-dep slots on consumers also use this to track which
/// upstreams have terminated (R1.3.4 / Lock 2.B).
///
/// `Error` carries a [`HandleId`] resolving to the error value. Refcount is
/// retained when the variant is stored in a node's `terminal` slot or any
/// consumer's `dep_terminals` slot; v1 does not release these (terminal
/// state is one-shot at this layer; release happens on resubscribable
/// terminal-lifecycle reset, a separate slice).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum TerminalKind {
    Complete,
    Error(HandleId),
}

/// Node kind discriminant — **derived metadata** computed from
/// [`NodeRecord`]'s field shape (D030 unification, Slice D).
///
/// Core no longer stores `kind` as a field; it's computed on demand from
/// `(deps.is_empty(), fn_id.is_some(), op.is_some(), is_dynamic)`,
/// mirroring TS's data model where `NodeImpl` has no `_kind` field. The
/// shape uniquely identifies the kind:
///
/// | deps      | fn_id | op   | is_dynamic | kind     |
/// |-----------|-------|------|-----------|----------|
/// | empty     | None  | None | -         | State    |
/// | empty     | Some  | None | -         | Producer |
/// | non-empty | Some  | None | false     | Derived  |
/// | non-empty | Some  | None | true      | Dynamic  |
/// | non-empty | None  | Some | -         | Operator |
///
/// Public API ([`Core::kind_of`]) derives this enum on each call. State
/// nodes are ROM (cache survives deactivation); compute nodes
/// (Derived / Dynamic / Operator) and producers are RAM.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum NodeKind {
    /// Source node: cache is intrinsic, no fn, no deps. Mutated via [`Core::emit`].
    State,
    /// Producer node: fn fires once on first subscribe. No deps;
    /// emissions arrive via sinks the fn subscribes to (zip / concat /
    /// race / takeUntil pattern). Slice D / D031.
    Producer,
    /// Derived node: fn fires on every dep change; all deps tracked.
    Derived,
    /// Dynamic node: fn declares which dep indices it actually read this run.
    /// Untracked dep updates flow through cache but do NOT re-fire fn.
    Dynamic,
    /// Operator node: built-in dispatch path for transform / combine /
    /// flow / resilience operators. The `OperatorOp` discriminant selects
    /// the per-operator FFI path ([`BindingBoundary::project_each`] etc.);
    /// Core manages per-operator state via the generic `op_scratch` slot
    /// on `NodeRecord` (D026). Per Slice C-1 (D009) / Slice C-3 (D026).
    Operator(OperatorOp),
}

impl NodeKind {
    /// True if this kind opts OUT of Lock 2.B auto-cascade. Operator(Reduce)
    /// and Operator(Last) must intercept upstream COMPLETE so they can emit
    /// their accumulator / buffered value before the cascade terminates them;
    /// instead of cascading, terminate_node queues such children for fn-fire
    /// so `fire_operator` can handle the terminal.
    pub(crate) fn skips_auto_cascade(self) -> bool {
        matches!(
            self,
            NodeKind::Operator(
                OperatorOp::Reduce { .. } | OperatorOp::Last { .. } | OperatorOp::Valve
            )
        )
    }
}

/// Built-in operator discriminant. Selects the per-operator dispatch path
/// in `fire_operator` (`crates/graphrefly-core/src/batch.rs`). Each variant
/// carries the binding-side closure ids (and seed handle for stateful
/// folders) needed for the wave-execution path; Core stores no user values
/// itself per the handle-protocol cleaving plane.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum OperatorOp {
    /// `map(source, project)` — element-wise transform. Calls
    /// `BindingBoundary::project_each(fn_id, &inputs)` per fire; emits each
    /// returned handle via `commit_emission_verbatim` (R1.3.2.d batch
    /// semantics — no equals substitution between batch entries).
    Map { fn_id: FnId },
    /// `filter(source, predicate)` — silent-drop selection (D012/D018).
    /// Calls `BindingBoundary::predicate_each(fn_id, &inputs)`; emits each
    /// passing input verbatim. If zero pass on a wave that dirtied the
    /// node, queues a single `RESOLVED` to settle (D018).
    Filter { fn_id: FnId },
    /// `scan(source, fold, seed)` — left-fold emitting each new accumulator.
    /// `seed` is captured at registration; `acc` lives in
    /// [`ScanState`](super::op_state::ScanState) inside
    /// [`NodeRecord::op_scratch`] and persists across waves until
    /// resubscribable reset. Calls `BindingBoundary::fold_each(fn_id, acc,
    /// &inputs) -> SmallVec<HandleId>` per fire.
    Scan { fn_id: FnId, seed: HandleId },
    /// `reduce(source, fold, seed)` — left-fold emitting once on upstream
    /// COMPLETE. Accumulates silently while source DATA flows; on
    /// dep[0].terminal == Some(Complete), emits `[Data(acc), Complete]`.
    /// On `Error(h)`, propagates the error verbatim. Opts out of Lock 2.B
    /// auto-cascade (see `NodeKind::skips_auto_cascade`).
    Reduce { fn_id: FnId, seed: HandleId },
    /// `distinctUntilChanged(source, equals)` — suppresses adjacent
    /// duplicates. Calls `BindingBoundary::custom_equals(equals_fn_id,
    /// prev, current)` per input; emits non-equal items verbatim and
    /// updates `prev`. If zero items pass on a wave that dirtied the node,
    /// queues `RESOLVED` (matches Filter discipline).
    DistinctUntilChanged { equals_fn_id: FnId },
    /// `pairwise(source)` — emits `(prev, current)` pairs starting after
    /// the second value. First value swallowed (sets `prev`). Calls
    /// `BindingBoundary::pairwise_pack(fn_id, prev, current)` per pair to
    /// produce the binding-side tuple handle.
    Pairwise { fn_id: FnId },

    // ----- Slice C-2: multi-dep combinators (D020) -----
    /// `combine(...sources)` — N-dep combineLatest. On any dep fire, packs
    /// the latest handle per dep into a single tuple handle via
    /// `BindingBoundary::pack_tuple(pack_fn, &handles)`. First-run gate
    /// (`partial: false` default) holds until all deps deliver real DATA
    /// (R2.5.3). COMPLETE cascades when all deps complete (R1.3.4.b).
    Combine { pack_fn: FnId },

    /// `withLatestFrom(primary, secondary)` — 2-dep, fire-on-primary-only
    /// (D021, Phase 10.5). Packs `[primary, secondary]` via
    /// `BindingBoundary::pack_tuple(pack_fn, &handles)` when dep[0]
    /// (primary) has DATA in the wave. If only dep[1] (secondary) fires,
    /// settles with RESOLVED (D018 pattern). First-run gate holds until
    /// both deps deliver (R2.5.3 `partial: false`). Post-warmup INVALIDATE
    /// guard: if secondary `prev_data == NO_HANDLE` and batch empty after
    /// warmup, settles with RESOLVED (no stale pair).
    WithLatestFrom { pack_fn: FnId },

    /// `merge(...sources)` — N-dep, forward all DATA handles verbatim
    /// (D022). Zero FFI on fire: no transformation, no binding call.
    /// Each dep's batch handles are retained and emitted individually.
    /// COMPLETE cascades when all deps complete (R1.3.4.b).
    Merge,

    // ----- Slice C-3: flow operators (D024) -----
    /// `take(source, count)` — emits the first `count` DATA values then
    /// self-completes via `Core::complete`. Tracks `count_emitted` in
    /// [`TakeState`](super::op_state::TakeState). When upstream completes
    /// before `count` is reached, the standard auto-cascade propagates
    /// COMPLETE. `count == 0` is allowed: the first fire emits zero
    /// items then immediately self-completes (D027).
    Take { count: u32 },

    /// `skip(source, count)` — drops the first `count` DATA values; once
    /// the threshold is crossed, subsequent DATAs pass through verbatim.
    /// Tracks `count_skipped` in [`SkipState`](super::op_state::SkipState).
    /// On a wave where every input is still in the skip window, queues
    /// DIRTY+RESOLVED to settle (D018 pattern).
    Skip { count: u32 },

    /// `takeWhile(source, predicate)` — emits while `predicate(input)`
    /// holds; on the first `false`, emits any preceding passes then
    /// self-completes via `Core::complete`. Reuses
    /// [`BindingBoundary::predicate_each`] (D029); after the first
    /// `false`, subsequent inputs in the same batch are dropped.
    TakeWhile { fn_id: FnId },

    /// `last(source)` / `last_with_default(source, default)` — buffers
    /// the latest DATA; on upstream COMPLETE, emits `Data(latest)` then
    /// `Complete`. The `default` field is `NO_HANDLE` for the no-default
    /// factory (emits only `Complete` on empty stream), or a registered
    /// default handle (emits `Data(default)` + `Complete` on empty
    /// stream). Storage: [`LastState`](super::op_state::LastState) holds
    /// `latest` (live buffer) and `default` (registration-time, stable).
    /// Opts out of Lock 2.B auto-cascade so it can intercept upstream
    /// COMPLETE.
    Last { default: HandleId },

    // ----- Slice U: control operators (D047) -----
    /// `tap(source, fn)` — side-effect passthrough. Calls
    /// `BindingBoundary::invoke_tap_fn(fn_id, handle)` on each input DATA,
    /// then emits the input handle unchanged. Zero-transform: output
    /// handles are the inputs verbatim (no equals substitution, no
    /// allocation).
    Tap { fn_id: FnId },

    /// `tapFirst(source, fn)` — one-shot side-effect on first DATA. Same
    /// as [`Tap`](Self::Tap) but fires `invoke_tap_fn` only once; after
    /// the first fire, subsequent DATA passes through without a callback.
    /// State: [`TapFirstState`](super::op_state::TapFirstState) tracks
    /// `fired: bool`.
    TapFirst { fn_id: FnId },

    /// `valve(source, control)` — conditional forward. 2-dep: dep[0] is
    /// source, dep[1] is boolean control. When the latest control value
    /// is truthy (non-zero handle), forwards source DATA; when falsy,
    /// settles with RESOLVED. Partial mode so it fires on control-alone
    /// before source has delivered. Does NOT auto-complete on control
    /// terminal (`completeWhenDepsComplete: false` equivalent).
    Valve,

    /// `settle(source, quietWaves, maxWaves)` — convergence detector.
    /// Forwards each upstream DATA, counts consecutive no-change waves,
    /// and self-completes when `quiet_count >= quiet_waves` (or
    /// `wave_count >= max_waves` if set). State:
    /// [`SettleState`](super::op_state::SettleState).
    Settle {
        quiet_waves: u32,
        max_waves: Option<u32>,
    },
}

/// Registration options for [`Core::register_operator`].
///
/// `equals` controls operator output dedup (R5.7 — defaults to identity).
/// `partial` controls the R2.5.3 first-run gate (R5.4 — operator dispatch
/// fires on first DATA from any dep when `true`; default `false` matches
/// the gated derived discipline).
#[derive(Copy, Clone, Debug)]
pub struct OperatorOpts {
    pub equals: EqualsMode,
    pub partial: bool,
}

impl Default for OperatorOpts {
    fn default() -> Self {
        Self {
            equals: EqualsMode::Identity,
            partial: false,
        }
    }
}

/// Closure-form fn id OR typed operator discriminant — the two dispatch
/// paths a node can use. State / passthrough nodes pass `None` to
/// [`Core::register`] (no fn at all).
#[derive(Copy, Clone, Debug)]
pub enum NodeFnOrOp {
    /// Closure-form: invokes [`BindingBoundary::invoke_fn`] per fire.
    /// Used for Derived / Dynamic / Producer.
    Fn(FnId),
    /// Typed-op: routes to a `fire_op_*` helper that calls per-operator
    /// FFI methods (`project_each` / `predicate_each` / `fold_each` /
    /// `pairwise_pack` / `pack_tuple`). Used for Operator nodes.
    Op(OperatorOp),
}

/// Pause behavior mode (canonical-spec §2.6 — three modes shipped in TS;
/// Slice F audit, 2026-05-07 — closed the Rust port gap).
///
/// | Mode | Outgoing tier-3 routing while paused | RESUME behavior |
/// |---|---|---|
/// | [`PausableMode::Default`] | suppress fn-fire upstream (no DIRTY emitted) | fire fn ONCE on RESUME if any dep delivered DATA during pause; collapses N pause-window writes into one settle |
/// | [`PausableMode::ResumeAll`] | buffer outgoing tier-3 / tier-4 messages per-wave | replay each buffered wave verbatim on RESUME |
/// | [`PausableMode::Off`] | dispatcher ignores PAUSE; tier-3 flushes immediately | no-op (no buffer to drain) |
///
/// Default is [`PausableMode::Default`] per canonical §2.6 — every untagged
/// source picks it up. Memory profile is O(1) per node (no buffer); the
/// trade-off is "subscribers see one consolidated DATA on RESUME" rather
/// than the K mid-pause emissions verbatim.
///
/// Note: tier-1 (DIRTY) / tier-2 (PAUSE/RESUME) / tier-5 (COMPLETE/ERROR) /
/// tier-6 (TEARDOWN) bypass pause regardless of mode — they remain
/// observable so leaked pause-controllers cannot strand subscribers.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum PausableMode {
    /// Suppress fn-fire while paused; fire once on RESUME if any dep
    /// delivered DATA during the pause window. Canonical default.
    #[default]
    Default,
    /// Buffer outgoing tier-3 / tier-4 messages per-wave; replay on
    /// RESUME. Use when subscribers need verbatim emit history (e.g. an
    /// audit log, replay-on-reconnect bridge).
    ResumeAll,
    /// Dispatcher ignores PAUSE for this node — tier-3 flushes
    /// immediately even while a lock is held. Use for nodes whose value
    /// production is intrinsically pause-immune (telemetry counters,
    /// monotonic timers).
    Off,
}

/// Per-kind opts for [`Core::register`]. Cross-kind config knobs live
/// here; per-kind specifics (deps, fn_or_op) live on
/// [`NodeRegistration`].
#[derive(Copy, Clone, Debug)]
pub struct NodeOpts {
    /// Initial cached value. Only valid for state nodes (no deps + no
    /// fn + no op). [`NO_HANDLE`] starts the node sentinel.
    pub initial: HandleId,
    /// Equality mode for outgoing emissions (R1.3.2). Defaults to
    /// [`EqualsMode::Identity`].
    pub equals: EqualsMode,
    /// First-run gate (R2.5.3 / D011). When `true`, the node fires as
    /// soon as ANY dep delivers a real handle; when `false` (default),
    /// the node holds until every dep has delivered.
    pub partial: bool,
    /// Dynamic flag (R2.5.3) — fn declares actually-tracked dep indices
    /// per fire. Only meaningful when `fn_or_op == Some(Fn(_))` AND
    /// deps non-empty.
    pub is_dynamic: bool,
    /// Pause behavior mode (canonical §2.6). Default is
    /// [`PausableMode::Default`]. See [`PausableMode`] for the trade-offs.
    pub pausable: PausableMode,
    /// Replay buffer cap (canonical R2.6.5 / Lock 6.G — Slice E1, 2026-05-07).
    /// `None` (default) disables; `Some(N)` keeps a circular buffer of the
    /// last N DATA emissions and replays them to late subscribers as part
    /// of the per-tier handshake (between [`Message::Start`] and any
    /// terminal slice). Only DATA is buffered; RESOLVED entries are NOT
    /// (R2.6.5 explicit "DATA only").
    pub replay_buffer: Option<usize>,
}

impl Default for NodeOpts {
    fn default() -> Self {
        Self {
            initial: NO_HANDLE,
            equals: EqualsMode::Identity,
            partial: false,
            is_dynamic: false,
            pausable: PausableMode::Default,
            replay_buffer: None,
        }
    }
}

/// Unified node-registration descriptor (D030, Slice D).
///
/// All node kinds (State / Producer / Derived / Dynamic / Operator)
/// register through [`Core::register`] with a `NodeRegistration`. The
/// kind is **derived from the field shape** of the registration —
/// `(deps.is_empty(), fn_or_op variant)`:
///
/// | deps      | fn_or_op   | is_dynamic | resulting kind |
/// |-----------|-----------|-----------|----------------|
/// | empty     | None      | -         | State          |
/// | empty     | Some(Fn)  | -         | Producer       |
/// | non-empty | Some(Fn)  | false     | Derived        |
/// | non-empty | Some(Fn)  | true      | Dynamic        |
/// | non-empty | Some(Op)  | -         | Operator       |
///
/// The sugar wrappers ([`Core::register_state`], [`Core::register_producer`],
/// etc.) build a `NodeRegistration` and delegate.
#[derive(Clone, Debug)]
pub struct NodeRegistration {
    /// Upstream deps in declaration order. Empty for state / producer.
    pub deps: Vec<NodeId>,
    /// Closure-form fn id or typed-op discriminant. `None` for state /
    /// passthrough.
    pub fn_or_op: Option<NodeFnOrOp>,
    /// Cross-kind config knobs.
    pub opts: NodeOpts,
}

/// Equality mode for a node's outgoing emissions.
///
/// `Identity` is the default: cache vs. new handle compare is a `u64` equal —
/// zero FFI. `Custom` invokes [`BindingBoundary::custom_equals`] every check
/// (R1.3.2.b two-arg call when both sides are non-sentinel).
#[derive(Copy, Clone, Debug)]
pub enum EqualsMode {
    Identity,
    Custom(FnId),
}

/// Internal identifier for a single subscription. Allocated per
/// [`Core::subscribe`] call. Wrapped by [`Subscription`] for the public API;
/// consumed directly only by Core internals and the [`Subscription::Drop`]
/// path.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub(crate) struct SubscriptionId(u64);

/// RAII subscription handle.
///
/// Returned by [`Core::subscribe`]. While the handle is held, the sink stays
/// registered against its node. Dropping the handle (explicitly via
/// `drop(sub)` or implicitly at scope exit) unsubscribes the sink — no manual
/// `unsubscribe()` call is needed. Per §10.12 of the rust-port session doc.
///
/// # Lifetime semantics
///
/// The subscription holds a [`Weak`] reference back to the Core's state. If
/// the Core is dropped before the subscription, the Drop impl is a silent
/// no-op (the sink has nowhere to deregister from anyway). This avoids a
/// reference cycle when subscribers capture an `Arc<Core>` in their closure.
///
/// # Thread safety
///
/// `Send + Sync`. The handle can be moved across threads or dropped from
/// any thread.
///
/// # Not Clone
///
/// `Subscription` owns the unsubscribe action exclusively. Cloning would
/// require either "first drop wins" or "last drop wins" semantics, both
/// of which surprise. If a binding needs multiple deregistration handles,
/// it should subscribe multiple times (each producing a fresh handle) or
/// wrap the single `Subscription` in `Arc<Mutex<Option<Subscription>>>`.
#[must_use = "dropping a Subscription unsubscribes its sink immediately"]
pub struct Subscription {
    state: Weak<Mutex<CoreState>>,
    node_id: NodeId,
    sub_id: SubscriptionId,
}

impl Subscription {
    /// The node this subscription is attached to.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }
}

impl Drop for Subscription {
    #[allow(clippy::too_many_lines)] // Phase G is one continuous lifecycle hook chain (user cleanup → producer_deactivate → wipe_ctx → Core cache-clear); splitting it would obscure the ordering invariant.
    fn drop(&mut self) {
        // Silent no-op if Core is gone. This keeps Drop infallible (no panics
        // from a dropped subscription racing a dropped Core) and avoids
        // surprising users with errors on shutdown.
        //
        // Producer deactivation (Slice D, D031): if removing this sub
        // empties the subscribers map AND the node is a producer, fire
        // `BindingBoundary::producer_deactivate(node_id)` AFTER releasing
        // the state lock. The binding then drops its per-node state
        // (subscriptions to upstream sources, captured closure state),
        // which transitively unsubs from upstreams via their own
        // `Subscription::Drop`. Re-entrance into Core from the deactivate
        // hook is permitted since the lock is released first.
        let Some(state) = self.state.upgrade() else {
            return;
        };
        // Slice E2 (D056): when the last subscriber drops, fire the
        // node's OnDeactivation cleanup hook BEFORE producer_deactivate
        // (cleanup may release handles the producer subscription owns;
        // reverse order would let producer_deactivate drop subs that user
        // cleanup expected to be live). Both calls are lock-released per
        // D045.
        //
        // OnDeactivation gating (D068, QA Q3 fix): fires only when the
        // node has fired its fn at least once AND has a fn (`fn_id`
        // populated). State nodes have no fn — they cannot register a
        // cleanup spec via the production fn-return path (R2.4.5), so
        // firing `cleanup_for` on them is wasted FFI; the binding's
        // lookup is guaranteed to find no `current_cleanup`. Skipping
        // here saves the FFI hop and matches the design-doc wording
        // ("never-fired state nodes" — state-with-initial-value satisfies
        // `has_fired_once = true` but still has no fn).
        //
        // Slice E2 /qa Q2(b) (D069): if the node is a resubscribable
        // node that's ALREADY terminal (terminate fired BEFORE this last
        // sub drop), fire `wipe_ctx` lock-released AFTER OnDeactivation
        // + producer_deactivate. Mutually exclusive with `terminate_node`'s
        // queue-wipe site: terminate-with-empty-subs goes through
        // `pending_wipes`; terminate-with-live-subs routes here when
        // those subs eventually drop. Either path fires exactly one
        // wipe per terminal lifecycle.
        let (was_last_sub, is_producer, has_user_cleanup, fire_wipe, binding) = {
            let mut s = state.lock();
            let Some(rec) = s.nodes.get_mut(&self.node_id) else {
                return;
            };
            rec.subscribers.remove(&self.sub_id);
            // Slice X4 / D2: bump revision so any pending_notify entry for
            // this node opened earlier in the wave starts a fresh batch on
            // the next queue_notify, dropping the now-departed sink from
            // the snapshot.
            rec.subscribers_revision = rec.subscribers_revision.wrapping_add(1);
            let last = rec.subscribers.is_empty();
            let producer = rec.is_producer();
            // OnDeactivation gate: must have run a fn at least once
            // (has_fired_once) AND have a fn registered (fn_id.is_some()).
            // The fn_id check excludes state nodes whose has_fired_once
            // tracks initial-value status, not "user fn ran."
            let user_cleanup = rec.has_fired_once && rec.fn_id.is_some();
            let fire_wipe = last && rec.resubscribable && rec.terminal.is_some();
            // Phase G (D119/D120/D121, 2026-05-10): always clone the
            // binding when last sub leaves so we can run the Core
            // cache-clear after the existing hooks. The Arc::clone is
            // cheap and dwarfed by the cost of the hooks themselves.
            let binding = if last { Some(s.binding.clone()) } else { None };
            (last, producer, user_cleanup, fire_wipe, binding)
        };
        if was_last_sub {
            if let Some(binding) = binding {
                if has_user_cleanup {
                    binding.cleanup_for(self.node_id, CleanupTrigger::OnDeactivation);
                }
                if is_producer {
                    binding.producer_deactivate(self.node_id);
                }
                // D069: eager wipe — fires AFTER OnDeactivation so the
                // user closure observes pre-wipe `store` (matches the
                // existing "OnDeactivation runs before wipe on terminal
                // reset" invariant covered by test 10). Idempotent —
                // `HashMap::remove` on absent key is a no-op, so even
                // if the wave already drained `pending_wipes` earlier,
                // this fire is benign.
                if fire_wipe {
                    binding.wipe_ctx(self.node_id);
                }

                // Phase G (D118/D119/D120/D121, 2026-05-10) — Core
                // cache-clear on deactivation, mirroring TS `_deactivate`
                // (`pure-ts/src/core/node.ts:2185-2297`):
                //
                //   1. user `cleanup_for(OnDeactivation)`  ← above
                //   2. `producer_deactivate`                ← above
                //   3. `wipe_ctx` (resubscribable+terminal) ← above
                //   4. **NEW: Core cache-clear**            ← here
                //
                // Releases per-dep `prev_data` + `data_batch` retains +
                // `dep_terminals` Error retains (the latter closes the
                // long-standing "Non-resubscribable terminal Error
                // handles leak via diamond cascade" porting-deferred
                // entry — D121). Clears pause/replay buffers. Releases
                // `cached` for compute nodes (R2.2.7 / R2.2.8 ROM:
                // state nodes preserve cache; compute nodes clear).
                // Keeps the per-node `terminal` slot intact (D121:
                // producer-side terminal stays for late-subscriber
                // R2.2.7.a reset or R2.2.7.b rejection).
                //
                // Lock-released release discipline: collect handles
                // under the lock, drop the lock, fire `release_handle`
                // outside (mirrors `Core::resume` Phase 2 + the
                // existing F1 `set_deps` removed-handles release at
                // node.rs:5003).
                //
                // Re-entrance safety (F1 / D123, /qa 2026-05-10): if the
                // user `cleanup_for` / `producer_deactivate` / `wipe_ctx`
                // hook re-subscribed via some path, `subscribers` is now
                // non-empty. **Skip Phase G entirely in that case** —
                // the new subscriber's handshake delivered the live
                // `cache` handle to its sink and is holding a refcount
                // share through `pending_notify`; clearing cache here
                // would race with the new subscriber's wave and cause
                // a use-after-release in bindings that reap registry
                // slots at refcount-zero.
                //
                // Why this diverges from TS `_deactivate` (which clears
                // unconditionally): TS runs the cleanup hook + cache
                // clear as ONE sync block under the (implicit) JS
                // single-thread mutex; there's no released-lock window
                // for a re-subscribe to install a sub before the
                // cache-clear runs. Rust's lock-released hook discipline
                // (D045) opens that window, so the re-check is
                // necessary to preserve refcount soundness.
                //
                // Re-acquire state lock atomically with the recheck so
                // a concurrent thread cannot install a sub between the
                // emptiness check and the cache mutation.
                // F8 (/qa 2026-05-10): also take `op_scratch` so its
                // retains release lock-released after the state-lock
                // scope. Pre-F8 Phase G only released per-edge handles
                // + the compute `cache`, leaving operator-internal
                // retains (Last.latest, Scan.acc, Reduce.acc, etc.)
                // in-place. For non-resubscribable nodes that never
                // re-subscribe, this was a permanent leak —
                // asymmetric with the per-edge cleanup. F8 closes that
                // by taking the scratch alongside per-edge handles
                // and calling `release_handles` lock-released.
                //
                // D-α (D028 full close, 2026-05-10): for resubscribable
                // operator nodes, take the OLD scratch AND build a
                // FRESH scratch via `make_op_scratch` (lock-held, but
                // `binding.retain_handle` is a leaf operation per
                // op_state.rs:69-80 docs), install the fresh scratch
                // on `rec.op_scratch`, and push the old scratch to
                // `pending_scratch_release` for deferred release. The
                // queue drains on the next `reset_for_fresh_lifecycle`
                // (after Phase 2 takes fresh retains — preserves the
                // C-3 /qa P1 seed-aliasing-acc invariant) or on
                // `Drop for CoreState`. The fresh install gives
                // re-activation correct counter / acc state (Take.taken
                // back to 0, Scan.acc back to seed, etc.) matching TS
                // Lock 6.D ("resets on every deactivation").
                let (to_release, scratch_to_release): (
                    Vec<HandleId>,
                    Option<Box<dyn crate::op_state::OperatorScratch>>,
                ) = {
                    let mut s = state.lock();
                    if let Some(rec) = s.nodes.get_mut(&self.node_id) {
                        // F1 re-entrance check.
                        if !rec.subscribers.is_empty() {
                            // A user hook re-subscribed during the
                            // lock-released window; the new lifecycle
                            // owns this node now. Phase G is a no-op.
                            return;
                        }
                        let mut handles: Vec<HandleId> = Vec::new();
                        // Per-dep state. Empty for state nodes.
                        for dr in &mut rec.dep_records {
                            if dr.prev_data != NO_HANDLE {
                                handles.push(dr.prev_data);
                                dr.prev_data = NO_HANDLE;
                            }
                            for h in dr.data_batch.drain(..) {
                                handles.push(h);
                            }
                            // D121: per-edge terminal-Error retain
                            // released here. Closes the cascade leak.
                            // The producer's own `rec.terminal` slot
                            // stays intact (preserved below).
                            if let Some(TerminalKind::Error(h)) = dr.terminal {
                                handles.push(h);
                            }
                            dr.terminal = None;
                            dr.dirty = false;
                            dr.involved_this_wave = false;
                        }
                        // Pause buffer DATA / replay buffer.
                        if let PauseState::Paused { ref mut buffer, .. } = rec.pause_state {
                            for msg in buffer.drain(..) {
                                if let Some(h) = msg.payload_handle() {
                                    handles.push(h);
                                }
                            }
                        }
                        for h in rec.replay_buffer.drain(..) {
                            handles.push(h);
                        }
                        // Pause locks drained → node back to Active.
                        rec.pause_state = PauseState::Active;
                        // R2.2.7 / R2.2.8 ROM: state nodes preserve
                        // cache; compute (fn or op) nodes clear.
                        // D119: state nodes keep `cached` because the
                        // value is intrinsic and non-volatile;
                        // resubscribe sees the same value.
                        let is_compute = rec.fn_id.is_some() || rec.op.is_some();
                        if is_compute && rec.cache != NO_HANDLE {
                            handles.push(rec.cache);
                            rec.cache = NO_HANDLE;
                        }
                        // Reset wave + lifecycle state so reactivation
                        // begins fresh. `terminal` STAYS (D121).
                        rec.has_fired_once = false;
                        rec.dirty = false;
                        rec.involved_this_wave = false;
                        // §10.13 perf (D047): reset received_mask — all deps back
                        // to sentinel on resubscribe.
                        rec.received_mask = 0;
                        if rec.is_dynamic {
                            rec.tracked.clear();
                        }
                        // F8 + D-α: op_scratch handling forks by
                        // `resubscribable`. Non-resubscribable: eager
                        // release (F8 path — there's no future reset
                        // so the leak ends here). Resubscribable +
                        // has-op: take old AND install fresh; defer
                        // old release to the queue.
                        let scratch = if !rec.resubscribable {
                            std::mem::take(&mut rec.op_scratch)
                        } else if let Some(op) = rec.op {
                            // Slice C-3 /qa P1 (retain-before-release):
                            // build fresh scratch FIRST (this calls
                            // `binding.retain_handle` for any seed /
                            // default the op carries), THEN swap. The
                            // old scratch's release is deferred to the
                            // `pending_scratch_release` queue (drained
                            // on next reset_for_fresh_lifecycle or
                            // Drop for CoreState).
                            //
                            // make_op_scratch is fallible only for
                            // OperatorSeedSentinel; the OperatorOp
                            // stored on NodeRecord passed validation
                            // at registration time, so the unwrap
                            // here is structurally guaranteed (mirrors
                            // reset_for_fresh_lifecycle).
                            //
                            // Uses the binding-explicit static variant
                            // because we have only `&dyn BindingBoundary`
                            // here (Subscription::Drop holds no Core).
                            let new_scratch = Core::make_op_scratch_with_binding(&*binding, op)
                                .expect("invariant: stored OperatorOp passed make_op_scratch validation at registration time");
                            let old = std::mem::replace(&mut rec.op_scratch, new_scratch);
                            if let Some(old_box) = old {
                                s.pending_scratch_release.push(old_box);
                            }
                            // The "scratch_to_release" for lock-released
                            // release stays None here — the resubscribable
                            // case routes through the queue.
                            None
                        } else {
                            // Resubscribable but no op (e.g. derived
                            // compute / dynamic / state). Nothing to
                            // do for op_scratch.
                            None
                        };
                        (handles, scratch)
                    } else {
                        // Node destroyed between lock-released hooks
                        // and this re-acquire (terminate cascade or
                        // graph removal) — nothing to clear.
                        (Vec::new(), None)
                    }
                };
                // Release handles lock-released. Binding may re-enter
                // Core during `release_handle` (final-Drop callbacks
                // are user code).
                for h in to_release {
                    binding.release_handle(h);
                }
                // F8: release operator-scratch handles lock-released
                // (mirrors `ScratchReleaseGuard::drop` ordering).
                if let Some(mut scratch) = scratch_to_release {
                    scratch.release_handles(&*binding);
                }
            }
        }
    }
}

// Compile-time assertion that Subscription is Send + Sync. If a future field
// breaks this, the build fails here rather than downstream at the binding
// site.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Subscription>();
};

/// A subscriber callback. `Send + Sync` so the Core can fire it from any
/// thread; `Fn` (not `FnMut`) so multiple references coexist — capture
/// mutable state in `Mutex<T>` or atomics on the binding side.
pub type Sink = Arc<dyn Fn(&[Message]) + Send + Sync>;

// ---------------------------------------------------------------------------
// PAUSE/RESUME state — §10.2 of the rust-port session doc
// ---------------------------------------------------------------------------

/// Per-node pause state.
///
/// Replaces the four TS fields (`_pauseLocks`, `_pauseBuffer`,
/// `_pauseDroppedCount`, `_pauseStartNs`) with a single enum where
/// the buffered fields are unreachable in the [`Self::Active`] variant —
/// the compiler refuses access. Per §10.2 simplification.
///
/// # Invariants
///
/// - `Active` ⇔ no lockId held.
/// - `Paused { locks, .. }` ⇔ `!locks.is_empty()`.
/// - Buffered messages are tier 3 (DATA/RESOLVED) and tier 4 (INVALIDATE)
///   only. Other tiers pass through immediately even while paused.
/// - `dropped` counts messages that fell out the front of `buffer` due to
///   the Core-global `pause_buffer_cap`; it is reported on resume so callers
///   can detect overflow without re-tracking it externally.
#[derive(Debug)]
pub(crate) enum PauseState {
    Active,
    Paused {
        /// Active lock holders. `SmallVec` keeps the common 1–2 lock case
        /// stack-allocated. Replaces `Set<unknown>` from TS.
        locks: SmallVec<[LockId; 2]>,
        /// Buffered tier-3/tier-4 outgoing messages, in arrival order.
        /// Replayed on the final RESUME.
        buffer: VecDeque<Message>,
        /// Count of messages dropped from the front when `buffer.len()` would
        /// exceed `pause_buffer_cap`. Cleared on final RESUME (next pause
        /// cycle starts fresh).
        dropped: u32,
        /// Wall-clock-monotonic ns when the lock first transitioned this node
        /// from `Active` to `Paused`. Used by R1.3.8.c overflow ERROR
        /// synthesis to compute `lock_held_duration_ms` in the diagnostic
        /// payload (Slice F, A3 — 2026-05-07).
        started_at_ns: u64,
        /// True after the first overflow event in this pause cycle has been
        /// reported via [`crate::boundary::BindingBoundary::synthesize_pause_overflow_error`].
        /// Subsequent overflows in the same cycle don't re-emit ERROR
        /// (canonical R1.3.8.c: "once per overflow event"). Cleared on
        /// final RESUME (next pause cycle starts fresh).
        overflow_reported: bool,
        /// Default-mode bookkeeping (Slice F audit close, 2026-05-07).
        /// Set to `true` when an upstream dep delivery arrives while this
        /// node is paused with [`PausableMode::Default`]. On final RESUME,
        /// if `true`, the node is added back to `pending_fires` so the fn
        /// fires once with the consolidated dep state. Always `false` for
        /// `ResumeAll` mode (the buffered messages are the consolidation
        /// mechanism there). Cleared on final RESUME.
        pending_wave: bool,
    },
}

impl PauseState {
    pub(crate) fn is_paused(&self) -> bool {
        matches!(self, Self::Paused { .. })
    }

    fn lock_count(&self) -> usize {
        match self {
            Self::Active => 0,
            Self::Paused { locks, .. } => locks.len(),
        }
    }

    fn contains_lock(&self, lock_id: LockId) -> bool {
        match self {
            Self::Active => false,
            Self::Paused { locks, .. } => locks.contains(&lock_id),
        }
    }

    /// Add a lock; transitions Active → Paused on first lock. Idempotent on
    /// duplicate lock_id (matches TS convention; spec is silent on the case).
    fn add_lock(&mut self, lock_id: LockId) {
        match self {
            Self::Active => {
                let mut locks = SmallVec::new();
                locks.push(lock_id);
                *self = Self::Paused {
                    locks,
                    buffer: VecDeque::new(),
                    dropped: 0,
                    started_at_ns: monotonic_ns(),
                    overflow_reported: false,
                    pending_wave: false,
                };
            }
            Self::Paused { locks, .. } => {
                if !locks.contains(&lock_id) {
                    locks.push(lock_id);
                }
            }
        }
    }

    /// Mark that an upstream dep delivered DATA to a node paused with
    /// [`PausableMode::Default`]. The node will re-enter `pending_fires`
    /// on final RESUME via [`Self::take_pending_wave`].
    pub(crate) fn mark_pending_wave(&mut self) {
        if let Self::Paused { pending_wave, .. } = self {
            *pending_wave = true;
        }
    }

    /// Read and clear the `pending_wave` flag. Called from
    /// [`Core::resume`] when transitioning Paused → Active. Returns `true`
    /// only if the node was paused with `pending_wave` set.
    pub(crate) fn take_pending_wave(&mut self) -> bool {
        if let Self::Paused { pending_wave, .. } = self {
            std::mem::replace(pending_wave, false)
        } else {
            false
        }
    }

    /// Remove a lock; if the lockset becomes empty, transition Paused →
    /// Active and return the buffered messages for replay (along with the
    /// dropped count for diagnostics). Unknown lock_id is an idempotent
    /// no-op (matches TS, R1.2.6 implicit).
    fn remove_lock(&mut self, lock_id: LockId) -> Option<(VecDeque<Message>, u32)> {
        match self {
            Self::Active => None,
            Self::Paused { locks, .. } => {
                if let Some(idx) = locks.iter().position(|l| *l == lock_id) {
                    locks.swap_remove(idx);
                }
                if locks.is_empty() {
                    let prev = std::mem::replace(self, Self::Active);
                    if let Self::Paused {
                        buffer, dropped, ..
                    } = prev
                    {
                        return Some((buffer, dropped));
                    }
                }
                None
            }
        }
    }

    /// Append a message to the buffer; if the buffer would exceed `cap`,
    /// pop from the front (oldest-first), increment `dropped`, and return
    /// the dropped messages so the caller can release any payload handles
    /// they reference. `cap` of `None` means unbounded.
    ///
    /// Returns [`PushBufferedResult`] carrying both the dropped messages
    /// (for refcount release) and whether this push triggered the FIRST
    /// overflow event in the current pause cycle (for R1.3.8.c ERROR
    /// synthesis — the caller schedules a single ERROR per cycle).
    ///
    /// Note: refcount management for the message's payload handle is the
    /// caller's responsibility — see [`Core::queue_notify`] for the
    /// retain/release discipline. The buffer itself is just a message
    /// container; refcounts cross the binding boundary.
    pub(crate) fn push_buffered(&mut self, msg: Message, cap: Option<usize>) -> PushBufferedResult {
        let mut result = PushBufferedResult::default();
        if let Self::Paused {
            buffer,
            dropped,
            overflow_reported,
            ..
        } = self
        {
            buffer.push_back(msg);
            if let Some(c) = cap {
                while buffer.len() > c {
                    if let Some(dropped_msg) = buffer.pop_front() {
                        result.dropped_msgs.push(dropped_msg);
                    }
                    *dropped = dropped.saturating_add(1);
                }
            }
            // R1.3.8.c (Slice F, A3): flag first overflow this cycle.
            if !result.dropped_msgs.is_empty() && !*overflow_reported {
                *overflow_reported = true;
                result.first_overflow_this_cycle = true;
            }
        }
        result
    }

    /// Snapshot the diagnostic for an R1.3.8.c overflow ERROR synthesis.
    /// Returns `(dropped_count, lock_held_ns)`. Caller must already know
    /// the configured cap (it's a Core-global value, not per-PauseState).
    pub(crate) fn overflow_diagnostic(&self) -> Option<(u32, u64)> {
        match self {
            Self::Active => None,
            Self::Paused {
                dropped,
                started_at_ns,
                ..
            } => {
                let lock_held_ns = monotonic_ns().saturating_sub(*started_at_ns);
                Some((*dropped, lock_held_ns))
            }
        }
    }
}

/// Return shape for [`PauseState::push_buffered`]. Carries both the dropped
/// messages (for refcount release) and an "is this the first overflow this
/// cycle" flag (for R1.3.8.c ERROR synthesis scheduling).
#[derive(Default)]
pub(crate) struct PushBufferedResult {
    pub(crate) dropped_msgs: Vec<Message>,
    pub(crate) first_overflow_this_cycle: bool,
}

/// Pending R1.3.8.c overflow ERROR synthesis entry. Recorded by
/// [`Core::queue_notify`] when the pause buffer first overflows in a cycle;
/// drained at wave-end after the lock-released call to
/// [`crate::boundary::BindingBoundary::synthesize_pause_overflow_error`].
///
/// `configured_max` is captured at scheduling time rather than read at
/// drain — the user could change `pause_buffer_cap` between schedule and
/// drain, and the diagnostic reads "the cap that was in effect when the
/// overflow happened."
#[derive(Debug, Clone)]
pub(crate) struct PendingPauseOverflow {
    pub(crate) node_id: NodeId,
    pub(crate) dropped_count: u32,
    pub(crate) configured_max: usize,
    pub(crate) lock_held_ns: u64,
}

/// Error returned when a same-thread partition acquire violates
/// ascending order. Phase H+ STRICT variant (D115).
///
/// The ascending-order protocol prevents AB/BA deadlocks between
/// threads. When this error surfaces, the caller should defer the
/// operation to wave-end (when no partitions are held) and retry.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
#[error(
    "Phase H+ ascending-order violation: attempted partition {attempted:?} \
     while already holding partition {max_held:?} — defer to wave-end"
)]
pub struct PartitionOrderViolation {
    /// The partition the caller tried to acquire.
    pub attempted: crate::subgraph::SubgraphId,
    /// The highest-id partition currently held by this thread.
    pub max_held: crate::subgraph::SubgraphId,
}

/// Errors returnable by [`Core::try_subscribe`].
///
/// `Core::subscribe` (the panic-on-error variant) panics on either
/// case; `try_subscribe` returns these so operators (zip / concat /
/// race / take_until / merge / switch_map / etc.) can match on the
/// variant — defer for [`Self::PartitionOrderViolation`], skip the
/// source for [`Self::TornDown`].
///
/// Per canonical spec R2.2.7.a / R2.2.7.b (D118, 2026-05-10).
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum SubscribeError {
    /// Phase H+ STRICT (D115): partition acquisition would violate the
    /// ascending-order protocol. Caller should defer the subscribe to
    /// wave-end via the producer-pattern deferred-op queue.
    #[error(transparent)]
    PartitionOrderViolation(#[from] PartitionOrderViolation),

    /// R2.2.7.b (D118, 2026-05-10): the node is non-resubscribable AND
    /// has terminated (`[COMPLETE]` or `[ERROR, h]` was delivered).
    /// The stream is permanently over; subscribe is rejected.
    /// Resubscribable terminal nodes do NOT surface this error — they
    /// reset to a fresh lifecycle on subscribe per R2.2.7.a, regardless
    /// of TEARDOWN state.
    #[error(
        "subscribe({node:?}): node is non-resubscribable and has terminated; \
         the stream is permanently over (R2.2.7.b)"
    )]
    TornDown {
        /// The non-resubscribable terminal node that rejected the subscribe.
        node: NodeId,
    },
}

/// A producer-pattern operation deferred because it would have
/// violated the ascending partition-order protocol (Phase H+ STRICT,
/// D115). Drained by `BatchGuard::drop` after wave_guards are
/// released (no partitions held → safe to acquire any partition).
///
/// Variants with `HandleId` fields hold a binding-side retain taken
/// at defer time. The drain path releases this retain after the
/// operation fires; the panic-discard path releases it without firing.
pub enum DeferredProducerOp {
    /// Deferred `Core::emit`. Retain held on `handle`.
    Emit { node_id: NodeId, handle: HandleId },
    /// Deferred `Core::complete`. No handle.
    Complete { node_id: NodeId },
    /// Deferred `Core::error`. Retain held on `handle`.
    Error { node_id: NodeId, handle: HandleId },
    /// Generic deferred callback (e.g., deferred subscribe from
    /// producer build closure). The closure captures everything it
    /// needs; `graphrefly-core` doesn't depend on operator-specific
    /// types. The closure is responsible for its own retain discipline.
    Callback(Box<dyn FnOnce() + Send>),
}

/// Errors returnable by [`Core::pause`] and [`Core::resume`].
#[derive(Error, Debug, Clone, PartialEq)]
pub enum PauseError {
    #[error("pause/resume: unknown node {0:?}")]
    UnknownNode(NodeId),
}

/// Errors returnable by [`Core::up`] (canonical R1.4.1).
#[derive(Error, Debug, Clone, PartialEq)]
pub enum UpError {
    /// Node id is not registered.
    #[error("up: unknown node {0:?}")]
    UnknownNode(NodeId),
    /// Tier-3 (DATA / RESOLVED) and tier-5 (COMPLETE / ERROR) are
    /// downstream-only per R1.4.1; rejected at the boundary.
    #[error(
        "up: tier {tier} is forbidden upstream — value (tier 3) and \
         terminal-lifecycle (tier 5) planes are downstream-only per R1.4.1"
    )]
    TierForbidden { tier: u8 },
}

/// Errors returnable by [`Core::register`] and its sugar wrappers
/// ([`Core::register_state`], [`Core::register_producer`],
/// [`Core::register_derived`], [`Core::register_dynamic`],
/// [`Core::register_operator`]).
///
/// Slice H (2026-05-07) promoted these from `assert!`/`panic!` to typed
/// errors so that callers can recover from contract violations without
/// process abort. Every variant corresponds to a construction-time
/// invariant that the caller is responsible for upholding; the dispatcher
/// rejects the registration before any reactive state is created (so
/// there is no `Message::Error` channel through which to surface the
/// failure — these are imperative-layer errors, not reactive ones).
///
/// All variants are zero-side-effect: when [`Core::register`] returns
/// `Err`, no node has been added to the graph and any handle retains
/// taken on the way in (e.g. operator scratch seed retains via
/// [`BindingBoundary::retain_handle`]) have been released.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum RegisterError {
    /// One of the supplied dep ids is not a registered node.
    #[error("register: unknown dep {0:?}")]
    UnknownDep(NodeId),

    /// `op` was supplied (operator node) but `deps` was empty. Operator
    /// nodes need at least one dep — for subscription-managed combinators
    /// with no declared deps, use [`Core::register_producer`] instead.
    #[error(
        "register: operator nodes require at least one dep — \
         use register_producer for subscription-managed combinators"
    )]
    OperatorWithoutDeps,

    /// [`NodeOpts::initial`] was set to a real handle but the registration
    /// shape is not a state node (state nodes are `deps.is_empty() &&
    /// fn_id.is_none() && op.is_none()`). Initial cache only makes sense
    /// for state nodes.
    #[error("register: NodeOpts::initial only valid for state nodes (no deps + no fn + no op)")]
    InitialOnlyForStateNodes,

    /// A supplied dep is terminal (COMPLETE / ERROR) AND not
    /// resubscribable. Adding it would create a permanent wedge — the dep
    /// will never re-emit, so the registered node would be stuck.
    /// Mirrors [`SetDepsError::TerminalDep`] at registration time.
    #[error(
        "register: dep {0:?} is terminal and not resubscribable; \
         mark it resubscribable before terminating, or remove it from the dep list"
    )]
    TerminalDep(NodeId),

    /// A stateful operator ([`OperatorOp::Scan`] / [`OperatorOp::Reduce`])
    /// was registered with `seed = NO_HANDLE`. R2.5.3 first-run gate
    /// requires the seed to be a real handle so that the operator can
    /// emit on its first fire.
    #[error("register: operator seed must be a real handle (R2.5.3); got NO_HANDLE")]
    OperatorSeedSentinel,
}

/// Errors returnable by [`Core::set_pausable_mode`].
///
/// Slice H (2026-05-07) promoted these from `assert!`/`panic!` to typed
/// errors. Same imperative-layer error model as [`RegisterError`].
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum SetPausableModeError {
    /// `node_id` is not a registered node.
    #[error("set_pausable_mode: unknown node {0:?}")]
    UnknownNode(NodeId),
    /// The node currently holds at least one pause lock. Changing pausable
    /// mode mid-pause would lose buffered content or strand a
    /// `pending_wave` flag — resume all locks first.
    #[error(
        "set_pausable_mode: cannot change pausable mode while paused; \
         resume all locks first"
    )]
    WhilePaused,
}

/// Per-dep record. Replaces the parallel `deps` / `dep_handles` /
/// `dep_terminals` vectors from v1. Canonical spec R2.9.b alignment.
///
/// Each entry tracks one dep's lifecycle state, wave-scoped batch data,
/// and cross-wave `prev_data` for `ctx.prevData` access.
pub(crate) struct DepRecord {
    /// The dep node this record tracks.
    pub(crate) node: NodeId,
    /// Last DATA handle from the end of the previous wave. [`NO_HANDLE`]
    /// means the dep has never emitted DATA.
    pub(crate) prev_data: HandleId,
    /// Per-dep dirty flag — awaiting DATA/RESOLVED for current wave.
    pub(crate) dirty: bool,
    /// Per-dep involved-this-wave flag. Distinguishes:
    /// - `involved && data_batch.is_empty()` → dep settled RESOLVED
    /// - `!involved && data_batch.is_empty()` → dep was not in this wave
    pub(crate) involved_this_wave: bool,
    /// DATA handles accumulated this wave. Outside `batch()` scope, at most
    /// 1 element. Inside `batch()`, K emits on the source produce K entries
    /// per R1.3.6.b coalescing. Each handle holds a `retain_handle` share
    /// taken at `deliver_data_to_consumer` time; released at wave-end
    /// rotation in `clear_wave_state`.
    pub(crate) data_batch: SmallVec<[HandleId; 1]>,
    /// Terminal state for this dep. `None` = dep is live.
    /// `Some` = dep emitted COMPLETE/ERROR. When ALL entries are Some,
    /// the node auto-cascades per Lock 2.B (ERROR dominates COMPLETE).
    pub(crate) terminal: Option<TerminalKind>,
}

impl DepRecord {
    fn new(node: NodeId) -> Self {
        Self {
            node,
            prev_data: NO_HANDLE,
            dirty: false,
            involved_this_wave: false,
            data_batch: SmallVec::new(),
            terminal: None,
        }
    }
}

/// Internal node record. Mirrors `core.ts:132–154` post-D030 unification.
///
/// **Kind is derived, not stored** (D030, Slice D). `(dep_records.is_empty(),
/// fn_id, op, is_dynamic)` uniquely identifies the kind — see [`NodeKind`].
/// Helper methods (`is_state()`, `is_producer()`, `is_compute()`,
/// `is_operator()`, `skips_auto_cascade()`, `kind()`) cover the common
/// predicates without unpacking via [`Core::kind_of`].
///
/// The 5 bool fields (`has_fired_once`, `dirty`, `involved_this_wave`,
/// `has_received_teardown`, `resubscribable`, `is_dynamic`) each represent
/// an orthogonal concern. `is_dynamic` is constant per node (set at
/// register time); the others are mutable lifecycle state. Collapsing
/// them into a bitfield would obscure intent.
#[allow(clippy::struct_excessive_bools)]
pub(crate) struct NodeRecord {
    /// Per-dep records. Replaces the old parallel `deps` / `dep_handles` /
    /// `dep_terminals` vecs. Dep NodeIds derived via `dep_ids()`.
    pub(crate) dep_records: Vec<DepRecord>,
    /// User-fn id for closure-form dispatch. `Some` for Derived / Dynamic /
    /// Producer; `None` for State / Operator. (Operator dispatch goes via
    /// [`Self::op`] instead.)
    pub(crate) fn_id: Option<FnId>,
    /// Operator discriminant for typed-op dispatch. `Some` for Operator
    /// nodes; `None` otherwise. Mutually exclusive with `fn_id` (a node is
    /// either closure-form OR typed-op, never both).
    pub(crate) op: Option<OperatorOp>,
    /// True for Dynamic nodes (R2.5.3 — fn declares actually-tracked dep
    /// indices per fire). False for everything else. Only meaningful when
    /// `fn_id.is_some()` AND `!dep_records.is_empty()`.
    pub(crate) is_dynamic: bool,
    pub(crate) equals: EqualsMode,

    // Mutable state
    pub(crate) cache: HandleId,
    pub(crate) has_fired_once: bool,
    pub(crate) subscribers: HashMap<SubscriptionId, Sink>,
    /// Monotonic counter bumped on every mutation of [`Self::subscribers`]
    /// (insert on subscribe, remove on `Subscription::Drop`, remove on
    /// handshake-panic cleanup). Used by
    /// [`crate::batch::Core::queue_notify`] to detect mid-wave subscriber-
    /// set changes and start a fresh `PendingBatch` with an updated sink
    /// snapshot — closes D2 (Slice X4, 2026-05-08): the late-subscriber
    /// and multi-emit-per-wave gap where the pre-fix per-node single
    /// snapshot meant a sub installed between two emits to the same node
    /// in one wave was invisible to the second emit's flush.
    ///
    /// Per-node (not per-Core) so that a subscribe to node A doesn't
    /// invalidate snapshot reuse for node B's pending batch in the same
    /// wave.
    pub(crate) subscribers_revision: u64,
    /// For dynamic nodes: which dep indices fn actually tracks.
    /// For static derived: all indices, populated at construction.
    pub(crate) tracked: HashSet<usize>,

    // Wave-scoped state — cleared at wave end.
    pub(crate) dirty: bool,
    pub(crate) involved_this_wave: bool,

    /// Per-node pause state. Default `Active`. See [`PauseState`].
    pub(crate) pause_state: PauseState,
    /// Pause behavior mode (canonical-spec §2.6). Set at registration via
    /// [`NodeOpts::pausable`]. Default [`PausableMode::Default`] suppresses
    /// fn-fire while paused and consolidates N pause-window dep deliveries
    /// into one fn-fire on RESUME; `ResumeAll` buffers tier-3/4 outgoing
    /// for verbatim replay; `Off` ignores PAUSE entirely. See
    /// [`PausableMode`].
    pub(crate) pausable: PausableMode,
    /// Replay buffer cap (R2.6.5 / Lock 6.G — Slice E1, 2026-05-07).
    /// `None` disables; `Some(N)` keeps a circular VecDeque of the last N
    /// DATA-handle emissions for late-subscriber replay. Each handle in
    /// the buffer owns one binding-side retain share, released on evict
    /// (cap exceeded) or in `Drop for CoreState`.
    pub(crate) replay_buffer_cap: Option<usize>,
    pub(crate) replay_buffer: VecDeque<HandleId>,

    /// Terminal lifecycle state for THIS node's outgoing stream. Once set,
    /// further `emit` calls are silent no-ops, fn no longer fires, and only
    /// the terminal message has been queued downstream.
    pub(crate) terminal: Option<TerminalKind>,
    /// True after the first TEARDOWN has been processed for this node
    /// (R2.6.4 / Lock 6.F). Subsequent TEARDOWN deliveries are idempotent
    /// — the auto-prepended COMPLETE only fires on the first one. Without
    /// this flag, a redundant TEARDOWN delivered via the cascade plus an
    /// explicit `core.teardown(node)` would re-emit `[COMPLETE, TEARDOWN]`
    /// to subscribers per delivery, which is incorrect.
    pub(crate) has_received_teardown: bool,
    /// Per R2.2.7 / R2.5.3 — resubscribable terminal lifecycle.
    /// When `true` AND `terminal == Some(...)`, a fresh subscribe call
    /// will reset the node: clear `terminal`, `has_fired_once`,
    /// `has_received_teardown`, all dep_records to sentinel, and drain the
    /// pause lockset. Default `false`.
    pub(crate) resubscribable: bool,
    /// Meta companion nodes attached to this node per R1.3.9.d. When this
    /// node tears down, its meta companions are torn down FIRST (before
    /// the main node's auto-COMPLETE + TEARDOWN wire emission), so
    /// observers see companions terminate before the parent. The ordering
    /// is load-bearing — meta nodes typically subscribe to parent state
    /// that becomes inconsistent during the parent's destruction phase.
    pub(crate) meta_companions: Vec<NodeId>,
    /// R5.4 / D011 partial-mode: when `true`, fire_fn skips the R2.5.3
    /// first-run gate — the node fires as soon as ANY dep delivers a
    /// real handle, even if other deps remain sentinel. Defaults to
    /// `false` (gated). Lifted into Core for operator support; for
    /// State/Derived/Dynamic nodes the field is settable but the gated
    /// path remains the typical caller default.
    pub(crate) partial: bool,
    /// Topological rank — 1 + max dep rank. Nodes with no deps have rank 0.
    /// Used by `pick_next_fire` for O(|pending_fires|) scheduling instead
    /// of O(V) transitive BFS. Computed at registration; recomputed on
    /// `set_deps` for the modified node (consumer propagation deferred —
    /// see `porting-deferred.md`). §10 perf optimization (D047, Slice U).
    pub(crate) topo_rank: u32,
    /// Bitmask for first-run gate — bit i set when dep i delivers first
    /// DATA. `has_sentinel_deps()` becomes a single integer compare for
    /// ≤64 deps. Falls back to `iter().any()` when `dep_count > 64`.
    /// Reset on resubscribe. §10.13 perf optimization (D047, Slice U).
    pub(crate) received_mask: u64,
    /// Generic per-operator scratch slot (Slice C-3, D026). Replaces
    /// the typed `operator_state: HandleId` field used by Slices C-1 / C-2.
    /// `None` for non-operator kinds and operators with no cross-wave
    /// state (Map / Filter / Combine / WithLatestFrom / Merge); `Some`
    /// for stateful operators ([`OperatorOp::Scan`] / [`Reduce`] /
    /// [`DistinctUntilChanged`] / [`Pairwise`] / [`Take`] / [`Skip`] /
    /// [`TakeWhile`] / [`Last`]).
    ///
    /// The boxed value implements
    /// [`OperatorScratch`](crate::op_state::OperatorScratch); its
    /// `release_handles` method is called from
    /// [`reset_for_fresh_lifecycle`] (resubscribable terminal cycle) and
    /// from [`Drop for CoreState`].
    ///
    /// **Refcount discipline:** the state struct owns whatever handle
    /// shares it stores (e.g., [`ScanState::acc`](crate::op_state::ScanState::acc),
    /// [`LastState::latest`](crate::op_state::LastState::latest)).
    /// Per-fire helpers retain the new value before releasing the old;
    /// `release_handles` releases the current shares at end-of-life.
    pub(crate) op_scratch: Option<Box<dyn crate::op_state::OperatorScratch>>,
}

impl NodeRecord {
    // ---- Kind predicates (D030 — derived from field shape) ----

    /// True iff this is a state node (no deps, no fn, no op).
    pub(crate) fn is_state(&self) -> bool {
        self.dep_records.is_empty() && self.fn_id.is_none() && self.op.is_none()
    }

    /// True iff this is a producer node (no deps + has fn + no op).
    /// Producers fire fn once on first subscribe; cleanup fires via
    /// [`BindingBoundary::producer_deactivate`] (D031, Slice D).
    pub(crate) fn is_producer(&self) -> bool {
        self.dep_records.is_empty() && self.fn_id.is_some() && self.op.is_none()
    }

    /// True iff this is a compute node (Derived / Dynamic / Operator) —
    /// has at least one dep AND either a fn or an op.
    #[allow(dead_code)] // Convenience predicate; callers may use is_state/is_producer instead.
    pub(crate) fn is_compute(&self) -> bool {
        !self.dep_records.is_empty() && (self.fn_id.is_some() || self.op.is_some())
    }

    /// True iff this is an Operator node (has op set).
    #[allow(dead_code)] // Direct `op.is_some()` is more common; this is a readability sugar.
    pub(crate) fn is_operator(&self) -> bool {
        self.op.is_some()
    }

    /// True iff this node opts OUT of Lock 2.B auto-cascade —
    /// Operator(Reduce) / Operator(Last) intercept upstream COMPLETE.
    pub(crate) fn skips_auto_cascade(&self) -> bool {
        match self.op {
            Some(op) => NodeKind::Operator(op).skips_auto_cascade(),
            None => false,
        }
    }

    /// Compute the public-API [`NodeKind`] from the field shape (D030).
    /// Used by [`Core::kind_of`] and rare internal sites that need the
    /// enum (most use the predicate methods above).
    pub(crate) fn kind(&self) -> NodeKind {
        if let Some(op) = self.op {
            NodeKind::Operator(op)
        } else if self.dep_records.is_empty() {
            if self.fn_id.is_some() {
                NodeKind::Producer
            } else {
                NodeKind::State
            }
        } else if self.is_dynamic {
            NodeKind::Dynamic
        } else {
            NodeKind::Derived
        }
    }

    // ---- Existing accessors ----

    /// Iterator over dep NodeIds in declaration order.
    pub(crate) fn dep_ids(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.dep_records.iter().map(|r| r.node)
    }

    /// Collected dep NodeIds — for call sites that need a `Vec<NodeId>`.
    pub(crate) fn dep_ids_vec(&self) -> Vec<NodeId> {
        self.dep_ids().collect()
    }

    /// True if any dep is in sentinel state (never emitted DATA and no
    /// data this wave). Replaces the old `dep_handles.contains(&NO_HANDLE)`.
    pub(crate) fn has_sentinel_deps(&self) -> bool {
        let n = self.dep_records.len();
        if n == 0 {
            return false;
        }
        if n <= 64 {
            // O(1): check if all bits [0..n) are set.
            let full_mask = if n == 64 { u64::MAX } else { (1u64 << n) - 1 };
            self.received_mask != full_mask
        } else {
            // Fallback for >64 deps (extremely rare).
            self.dep_records
                .iter()
                .any(|r| r.prev_data == NO_HANDLE && r.data_batch.is_empty())
        }
    }

    /// Find the index of a dep by NodeId.
    pub(crate) fn dep_index_of(&self, dep_id: NodeId) -> Option<usize> {
        self.dep_records.iter().position(|r| r.node == dep_id)
    }

    /// True if ALL dep terminal slots are populated (Lock 2.B cascade check).
    pub(crate) fn all_deps_terminal(&self) -> bool {
        !self.dep_records.is_empty() && self.dep_records.iter().all(|r| r.terminal.is_some())
    }
}

// Q-beyond Sub-slice 1 (D108, 2026-05-09): `CrossPartitionState` removed.
//
// The four wave-scoped fields previously held under
// `Core::cross_partition: Arc<parking_lot::Mutex<CrossPartitionState>>`
// (Q2, 2026-05-09) moved to a per-thread `WaveState` thread_local in
// [`crate::batch`]. The bench-driven rationale is documented at
// [`crate::batch::WaveState`]: cross-thread cache-line bouncing on the
// `cross_partition` mutex was the dominant cost in Phase J's regression,
// not single-thread mutex hop count. Per-thread placement eliminates the
// bounce point entirely.
//
// **Refcount discipline preserved.** `wave_cache_snapshots` and
// `deferred_handle_releases` still hold binding-side handle retains;
// outermost `BatchGuard::drop` drains them via `Core::binding` on both
// success and panic paths (the binding ref the prior
// `CrossPartitionState` held for its `Drop` impl is no longer needed —
// `BatchGuard` already holds a `Core` clone with the binding).

/// All mutable Core state, behind one [`parking_lot::Mutex`].
///
/// **Architecture history.** Q2 (2026-05-09) split four wave-scoped
/// cross-partition aggregation fields out into a separate
/// `parking_lot::Mutex<CrossPartitionState>` on [`Core`]. Q-beyond
/// Sub-slice 1 (D108, 2026-05-09) eliminated `CrossPartitionState`
/// entirely — its four fields moved to per-thread `WaveState`
/// thread_local in `crate::batch`. Sub-slice 2 + 3 moved 8 more
/// wave-scoped fields the same way. /qa F1+F2 (2026-05-10) reverted
/// `in_tick` and `currently_firing` BACK to CoreState (per-Core,
/// cross-thread visible — load-bearing for cross-Core same-thread
/// isolation and cross-thread P13 set_deps check respectively).
///
/// The D1 patch (2026-05-09) moved Slice G's `tier3_emitted_this_wave`
/// set to a per-thread thread-local in `crate::batch` (was briefly
/// per-partition under Q3 v1; that placement was vulnerable to mid-wave
/// cross-thread `set_deps` partition splits — see
/// `docs/porting-deferred.md` "Per-partition state-shard refactor"
/// closing summary). Q-beyond
/// will continue the shape decomposition by sharding most of the
/// remaining fields per-partition.
pub(crate) struct CoreState {
    pub(crate) next_node_id: u64,
    pub(crate) next_subscription_id: u64,
    pub(crate) next_lock_id: u64,
    pub(crate) nodes: HashMap<NodeId, NodeRecord>,
    /// Inverted adjacency: `parent → children`. Updated on registration.
    pub(crate) children: HashMap<NodeId, HashSet<NodeId>>,
    // Q-beyond Sub-slice 2 (D108, 2026-05-09): `pending_fires` and
    // `pending_notify` moved to per-thread
    // [`crate::batch::WaveState`]. Both fields are wave-scoped — emit
    // populates them on the same thread that drains them at wave end.
    // Cross-thread emits block on partition `wave_owner` and land in
    // the OTHER thread's wave context, so the per-thread placement is
    // safe by construction.
    //
    // Q-beyond Sub-slice 3 (D108, 2026-05-09): `in_tick`,
    // `currently_firing`, `deferred_flush_jobs`, `deferred_cleanup_hooks`,
    // `pending_wipes`, and `invalidate_hooks_fired_this_wave` likewise
    // moved to [`crate::batch::WaveState`]. Same wave-scoped per-thread
    // rationale.
    /// Core-global cap on per-node pause replay buffer length. `None` means
    /// unbounded. Per the user direction (Q1, 2026-05-05): start core-global;
    /// per-node override can be added later as a pure addition without API
    /// breakage. Default `None`.
    pub(crate) pause_buffer_cap: Option<usize>,
    /// Core-global cap on wave-drain iterations before
    /// [`crate::batch::Core::drain_and_flush`] aborts with a diagnostic panic.
    /// Replaces the prior `MAX_DRAIN_ITERATIONS` hard-coded constant
    /// (R4.3 / Lock 2.F′). Default `10_000`.
    ///
    /// The drain loop bound exists to surface runtime cycles
    /// (e.g. an operator that re-arms its own `pending_fires` slot during
    /// `invoke_fn`) as a panic with context, rather than letting Core
    /// spin forever. Structural cycles via [`Core::set_deps`] are
    /// rejected at edge-mutation time (`SetDepsError::WouldCreateCycle`);
    /// registration is structurally cycle-safe by construction (the new
    /// node's id is not allocated until AFTER deps are validated, so deps
    /// cannot transitively reach the new node). The drain bound is the
    /// safety net for runtime cycles that bypass both static checks.
    pub(crate) max_batch_drain_iterations: u32,
    /// Wave-active flag — true between outermost `BatchGuard` entry and
    /// the matching outermost drop. Per-Core (cross-thread visible) so
    /// cross-Core same-thread BatchGuard nesting is correctly isolated:
    /// Core-A's `in_tick=true` does NOT cause Core-B's `begin_batch` to
    /// see itself as nested.
    ///
    /// /qa F1 reverted (2026-05-10): briefly placed on per-thread
    /// `WaveState` in Sub-slice 3, then moved BACK to `CoreState` after
    /// /qa F1 surfaced the cross-Core same-thread isolation regression
    /// (a thread holding a live BatchGuard on Core-A and starting a wave
    /// on Core-B would observe `ws.in_tick=true` set by Core-A → Core-B
    /// becomes non-owning → Core-B's writes drained by Core-A's binding
    /// → silent corruption). The other 11 wave-scoped fields stay on
    /// per-thread `WaveState` because they're accessed only by the
    /// wave-owner thread under `wave_owner` discipline.
    pub(crate) in_tick: bool,
    /// A6 reentrancy guard stack (Slice F, 2026-05-07): the stack of
    /// NodeIds whose fn is currently being invoked. Pushed at the top of
    /// `fire_fn` (just before the lock-released `invoke_fn` call) and
    /// popped on return / unwind via the [`crate::batch::FiringGuard`]
    /// RAII helper. [`Core::set_deps`] consults this set and rejects
    /// with [`SetDepsError::ReentrantOnFiringNode`] if `n` is currently
    /// firing — preventing the D1 `tracked` index corruption. Read by
    /// the P13 partition-migration check (D091) to reject mid-fire
    /// `set_deps` that would migrate a firing node's partition.
    ///
    /// **Per-Core (cross-thread visible).** /qa F2 reverted (2026-05-10):
    /// briefly placed on per-thread `WaveState` in Sub-slice 3, then
    /// moved BACK to `CoreState` after /qa F2 surfaced the cross-thread
    /// P13 bypass (per-thread placement made Thread B's `set_deps` read
    /// its own empty stack → P13 silently bypassed for cross-thread
    /// `set_deps` calls during Thread A's lock-released `invoke_fn`).
    /// Cross-thread visibility on shared `CoreState` is the load-bearing
    /// property the D091 check requires.
    ///
    /// Membership semantics (NOT strict LIFO): consumed via
    /// `contains(&n)` membership test. `FiringGuard::drop` pops the
    /// right-most matching `node_id` via `rposition` + `swap_remove`;
    /// physical order of remaining entries may not match construction
    /// order, but membership is preserved.
    pub(crate) currently_firing: Vec<NodeId>,
    // Q-beyond Sub-slice 3 (D108, 2026-05-09): `deferred_flush_jobs`
    // moved to [`crate::batch::WaveState`].
    /// Binding-boundary handle for `Drop`-time refcount balancing.
    /// `Core` also holds a clone of this Arc; storing it here lets
    /// `Drop for CoreState` walk every retained slot and release the
    /// binding-side share when the last `Core` clone drops. Without this,
    /// `cache` / `terminal` / `dep_terminals` Error / pause-buffer payload
    /// handle refs leak in the binding registry until process exit.
    pub(crate) binding: Arc<dyn BindingBoundary>,
    // Q-beyond Sub-slice 1 (D108, 2026-05-09): `wave_cache_snapshots`
    // moved to [`crate::batch::WaveState::wave_cache_snapshots`].
    // Q-beyond Sub-slice 1 (D108, 2026-05-09): `pending_auto_resolve`
    // moved to [`crate::batch::WaveState::pending_auto_resolve`].
    /// Topology-change sinks. Keyed by subscription id for O(1) removal.
    pub(crate) topology_sinks: HashMap<u64, crate::topology::TopologySink>,
    pub(crate) next_topology_id: u64,
    /// D-α (D028 full close, 2026-05-10): per-Core defer queue for old
    /// operator-scratch boxes pushed by Phase G on resubscribable nodes.
    /// Phase G builds a fresh scratch via [`Core::make_op_scratch`] and
    /// installs it on the node's `op_scratch` slot (so re-activation
    /// sees fresh counters / a fresh seed-share); the OLD scratch's
    /// handle retains are deferred to one of two drain points to
    /// preserve the Slice C-3 /qa P1 retain-before-release invariant
    /// (the C-3 test
    /// `scan_resubscribable_reset_with_seed_aliasing_acc_does_not_collapse_registry`
    /// fails if the old `acc` share is released before the fresh seed
    /// share is taken — when `acc == seed` interns to the same registry
    /// slot, eager release drops the slot to zero before the fresh
    /// retain bumps it back up).
    ///
    /// Drain points:
    /// 1. [`Core::reset_for_fresh_lifecycle`] — after Phase 2 takes
    ///    fresh retains on the new seed/default, the queue drain
    ///    releases queued boxes whose handles may have aliased.
    /// 2. [`Drop for CoreState`] — catch-all on Core shutdown.
    ///
    /// Note that the queue lives on `CoreState` (not on `Core`) so
    /// `Drop for CoreState` has access to both the binding and the
    /// queue under the single state lock — no separate mutex needed.
    ///
    /// **Growth bound (/qa m17, 2026-05-10):** size is bounded by the
    /// number of non-terminal deactivate-reactivate cycles since the
    /// last terminal-then-resubscribe reset on any resubscribable +
    /// has-op node in this Core. Each entry is a `Box<dyn OperatorScratch>`
    /// holding O(1) handles (Scan/Reduce/Last: 1 handle; Take/Skip/
    /// TakeWhile/DistinctUntilChanged/Pairwise: 0 or 1 handle). Typical
    /// workloads: O(few KB). Degenerate workloads (long-lived Cores
    /// with frequent deactivate-reactivate cycles and no terminal
    /// resets) should call `core.complete()` / `core.error()` on the
    /// op node periodically to trigger queue drain via
    /// `reset_for_fresh_lifecycle`. The release happens unconditionally
    /// on Core drop, so this is not a leak — just a deferred-release
    /// growth concern under unusual workloads.
    pub(crate) pending_scratch_release: Vec<Box<dyn crate::op_state::OperatorScratch>>,
    // /qa F2 reverted (2026-05-10): `currently_firing` field is declared
    // EARLIER in this struct (above `pause_buffer_cap`). Sub-slice 3
    // briefly moved it to `crate::batch::WaveState::currently_firing` on
    // the per-thread thread_local, but per-thread placement silently
    // bypassed the cross-thread P13 partition-migration check. /qa F2
    // (2026-05-10) moved it BACK to CoreState (cross-thread visible).
    // Q-beyond Sub-slice 1 (D108, 2026-05-09): `pending_pause_overflow`
    // moved to [`crate::batch::WaveState::pending_pause_overflow`].
    // Slice G (R1.3.2.d / R1.3.3.a — 2026-05-07): tier3-emitted-this-wave
    // tracker MOVED to a per-thread thread-local in `crate::batch`
    // (D1 patch, 2026-05-09 — was briefly per-partition under Q3 v1
    // 2026-05-09 morning). Wave-scope = thread; per-thread placement
    // is robust to mid-wave cross-thread `set_deps` partition splits
    // because thread B's split doesn't touch thread A's thread-local.
    // See [`crate::batch::TIER3_EMITTED_THIS_WAVE`] for the per-thread
    // wave-scope rationale and lifecycle (cleared at outermost
    // [`crate::batch::BatchGuard`] drop, both success + panic).
    //
    // Q-beyond Sub-slice 3 (D108, 2026-05-09):
    // `invalidate_hooks_fired_this_wave`, `deferred_cleanup_hooks`,
    // and `pending_wipes` moved to [`crate::batch::WaveState`].
    // Wave-scoped per-thread; same rationale as the other Sub-slice 3
    // migrations.
}

/// The handle-protocol Core dispatcher.
///
/// Holds an [`Arc`] to the [`BindingBoundary`] and all dispatch state. Cheap
/// to clone (the inner `Arc<Mutex<CoreState>>` is shared); pass `Core` by
/// value to threads.
///
/// # Wave-owner re-entrant mutex (Slice A close /qa, M1)
///
/// The state lock (`state: Mutex<CoreState>`) is **dropped** around binding
/// callbacks (`invoke_fn`, `custom_equals`) so user fns may re-enter Core.
/// To preserve serializability of WAVE EXECUTION across threads — without
/// re-introducing the lock-held-during-fn-fire deadlock the Slice A close
/// refactor lifted — the wave engine acquires `wave_owner` (a
/// [`parking_lot::ReentrantMutex`]) for the lifetime of each wave.
///
/// Properties:
///
/// - **Same-thread re-entrance is free.** A user fn that calls back into
///   `Core::emit` / `Core::pause` / etc. mid-fire re-acquires `wave_owner`
///   on the same thread and runs as a nested wave (the inner `run_wave`
///   sees `in_tick=true` and skips drain — outer drain picks up).
/// - **Cross-thread emits BLOCK** at `wave_owner.lock_arc()` until the
///   in-flight wave completes (drain + flush + sink fire all done). This
///   serializes wave OWNERSHIP across threads, while still allowing the
///   state lock to drop inside the wave for binding callbacks.
///
/// Without this, Slice A close's lock-released drain let cross-thread
/// emits absorb into the in-flight wave's `pending_notify` and return
/// before subscribers fire — breaking the user-facing happens-after
/// contract that `emit` returning means subscribers have observed.
/// Monotonic generation counter for `Core` instances. Used by the
/// per-thread `PARTITION_CACHE` in `batch.rs` to distinguish Core
/// instances without relying on `Arc::as_ptr` (which can be reused by
/// the allocator after a Core is dropped — ABA hazard). One atomic
/// increment per `Core::new`; negligible cost.
static CORE_GENERATION: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub struct Core {
    pub(crate) state: Arc<Mutex<CoreState>>,
    pub(crate) binding: Arc<dyn BindingBoundary>,
    /// Deferred producer-pattern operations. Per-Core (not per-thread)
    /// to avoid the cross-Core contamination discovered in D114 F1/F2.
    /// Drained after wave_guards release in `BatchGuard::drop`.
    pub(crate) deferred_producer_ops: Arc<parking_lot::Mutex<Vec<DeferredProducerOp>>>,
    /// Slice X5 (D3 substrate, 2026-05-08) + Slice Y1 / Phase E
    /// (wave-engine migration, 2026-05-08): per-subgraph union-find
    /// registry. Tracks each registered node's connected-component
    /// membership (a "subgraph"). Each component's root carries an
    /// `Arc<SubgraphLockBox>` whose `wave_owner: ReentrantMutex<()>`
    /// is the per-partition wave-serialization lock — acquired by
    /// [`Self::partition_wave_owner_lock_arc`] under the retry-validate
    /// loop. Cross-thread emits to disjoint partitions run truly
    /// parallel; same-thread re-entry passes through reentrantly.
    ///
    /// Direct port of [`graphrefly-py`'s
    /// `subgraph_locks.py`](https://github.com/graphrefly/graphrefly-py/blob/main/src/graphrefly/core/subgraph_locks.py)
    /// design (locked in [`SESSION-rust-port-d3-per-subgraph-parallelism.md`](https://github.com/graphrefly/graphrefly-ts/blob/main/archive/docs/SESSION-rust-port-d3-per-subgraph-parallelism.md)).
    pub(crate) registry: Arc<parking_lot::Mutex<crate::subgraph::SubgraphRegistry>>,
    /// Unique generation ID for this Core instance. Assigned from
    /// [`CORE_GENERATION`] at construction. Used by `PARTITION_CACHE`
    /// to avoid ABA false-hits after Core drop + allocator reuse.
    pub(crate) generation: u64,
}

/// Weak handle to a [`Core`] — does not contribute to strong refcount.
///
/// Constructed via [`Core::weak_handle`]; upgraded back to a strong
/// [`Core`] via [`WeakCore::upgrade`]. Used by long-lived binding-stored
/// closures (notably `ProducerBuildFn`s registered via
/// [`graphrefly_operators::ProducerBinding::register_producer_build`])
/// to break the BenchBinding → registry → closure → strong-Core cycle
/// that would otherwise leak the entire graph state when a `BenchCore`
/// drops with active producer registrations.
///
/// Upgrade on each invocation; if the host `Core` was already dropped,
/// `upgrade()` returns `None` and the closure should no-op (the host
/// is being torn down, no work to do).
#[derive(Clone)]
pub struct WeakCore {
    state: Weak<Mutex<CoreState>>,
    binding: Weak<dyn BindingBoundary>,
    deferred_producer_ops: Weak<parking_lot::Mutex<Vec<DeferredProducerOp>>>,
    registry: Weak<parking_lot::Mutex<crate::subgraph::SubgraphRegistry>>,
    generation: u64,
}

impl WeakCore {
    /// Try to upgrade back to a strong [`Core`]. Returns `None` if the
    /// host `Core`'s strong count has reached zero (i.e. the host
    /// `BenchCore` / equivalent owner was dropped).
    #[must_use]
    pub fn upgrade(&self) -> Option<Core> {
        Some(Core {
            state: self.state.upgrade()?,
            binding: self.binding.upgrade()?,
            deferred_producer_ops: self.deferred_producer_ops.upgrade()?,
            registry: self.registry.upgrade()?,
            generation: self.generation,
        })
    }
}

/// RAII guard that owns an [`OperatorScratch`] until either (a) the
/// caller `take()`s it for installation, or (b) the guard drops on an
/// early return / unwind, in which case the scratch's handle retains
/// are released via [`OperatorScratch::release_handles`].
///
/// Slice H /qa F1 + F2 (2026-05-07): closes two related correctness
/// gaps in `Core::register`:
///
/// 1. **TOCTOU window** — the original three-phase split called
///    `lock_state()` twice (once for validation, once for insertion),
///    so a concurrent `Core::complete(dep)` on a non-resubscribable
///    dep could slip in between the two acquisitions and re-create
///    the wedge `RegisterError::TerminalDep` was designed to prevent.
///    The guard plus a single locked region for both phases closes
///    this gap (release runs lock-released because guard variables
///    drop in reverse declaration order — guard declared BEFORE
///    `lock_state()`, so the lock guard drops first).
///
/// 2. **Panic-unsafe scratch leak** — without an RAII drop, a panic
///    between `make_op_scratch` (Phase 2) and the explicit
///    `if let Err(e)` cleanup branch (e.g., `lock_state()` reentrance
///    assert, OOM-as-panic on Vec growth in dep iteration) would
///    drop the `Box<dyn OperatorScratch>` without releasing the
///    seed/default retain. The guard's `Drop` impl releases on any
///    unwind path.
///
/// Lock-discipline: the guard holds `&dyn BindingBoundary` (through
/// the `Arc<dyn BindingBoundary>` it borrows from). On `Drop`, it
/// invokes `release_handles` lock-released — fires AFTER any
/// `MutexGuard<CoreState>` declared later in the same scope drops
/// (LIFO destruction order). Mirrors `Core::resume` Phase 2 release
/// pattern.
struct ScratchReleaseGuard<'a> {
    scratch: Option<Box<dyn crate::op_state::OperatorScratch>>,
    binding: &'a dyn BindingBoundary,
}

impl<'a> ScratchReleaseGuard<'a> {
    fn new(
        scratch: Option<Box<dyn crate::op_state::OperatorScratch>>,
        binding: &'a dyn BindingBoundary,
    ) -> Self {
        Self { scratch, binding }
    }

    /// Take ownership of the scratch — disarms the release-on-drop
    /// behavior. Used on the success path to install the scratch on
    /// `NodeRecord.op_scratch`.
    fn take(mut self) -> Option<Box<dyn crate::op_state::OperatorScratch>> {
        self.scratch.take()
    }
}

impl Drop for ScratchReleaseGuard<'_> {
    fn drop(&mut self) {
        if let Some(mut scratch) = self.scratch.take() {
            scratch.release_handles(self.binding);
        }
    }
}

impl Core {
    /// Construct a fresh Core wired to the given binding. Pause buffer cap
    /// defaults to unbounded; set via [`Self::set_pause_buffer_cap`].
    #[must_use]
    pub fn new(binding: Arc<dyn BindingBoundary>) -> Self {
        Self {
            state: Arc::new(Mutex::new(CoreState {
                next_node_id: 1,
                next_subscription_id: 1,
                // A4 (Slice F, 2026-05-07): start `next_lock_id` in the high
                // half of the u32 range so `alloc_lock_id` can't collide with
                // user-supplied `LockId::new(N)` constructors (which the
                // napi-rs binding marshals from `u32` and tests typically use
                // in the low range, 1..1024). Phase E /qa F1 (2026-05-08):
                // lowered from `1u64 << 32` to `1u64 << 31` so the value
                // round-trips through `u32::try_from(...)` at the napi
                // boundary — the previous seed errored every napi
                // `alloc_lock_id` call. Anti-collision intent (high range vs
                // low user range) preserved at half the prior ceiling
                // (2^31 ≈ 2 billion allocations per Core, ample for parity
                // tests). Lift the floor when the deferred BigInt-narrowing
                // migration extends `LockId` to `u64` at the FFI layer
                // (porting-deferred "BigInt migration for u32-narrowed napi
                // types" entry).
                next_lock_id: 1u64 << 31,
                nodes: HashMap::new(),
                children: HashMap::new(),
                // Q-beyond Sub-slice 2 + 3 (D108, 2026-05-09): pending_fires,
                // pending_notify, deferred_flush_jobs,
                // deferred_cleanup_hooks, pending_wipes, and
                // invalidate_hooks_fired_this_wave all live on per-thread
                // [`crate::batch::WaveState`].
                // /qa F1+F2 reverted (2026-05-10): in_tick + currently_firing
                // stay on CoreState (per-Core, cross-thread visible) for
                // cross-Core same-thread isolation (F1) and cross-thread
                // P13 set_deps check (F2).
                in_tick: false,
                currently_firing: Vec::new(),
                pause_buffer_cap: None,
                max_batch_drain_iterations: 10_000,
                binding: binding.clone(),
                topology_sinks: HashMap::new(),
                next_topology_id: 1,
                pending_scratch_release: Vec::new(),
            })),
            binding,
            deferred_producer_ops: Arc::new(parking_lot::Mutex::new(Vec::new())),
            registry: Arc::new(parking_lot::Mutex::new(
                crate::subgraph::SubgraphRegistry::new(),
            )),
            generation: CORE_GENERATION.fetch_add(1, Ordering::Relaxed),
        }
    }

    /// Acquire the state lock.
    ///
    /// Post-Slice-E: `Core::subscribe` fires the per-tier handshake
    /// LOCK-RELEASED with `wave_owner` held; sink callbacks may freely
    /// re-enter Core (`emit` / `complete` / `error` / nested `subscribe`).
    /// Same-thread re-entry passes through `wave_owner`'s `ReentrantMutex`
    /// transparently; cross-thread emits block on `wave_owner` until the
    /// outer subscribe completes, preserving R1.3.5.a happens-after
    /// ordering. The previous `IN_HANDSHAKE_FIRE` panic-diagnostic is no
    /// longer needed.
    pub(crate) fn lock_state(&self) -> MutexGuard<'_, CoreState> {
        self.state.lock()
    }

    /// Whether `self` and `other` point to the same dispatcher state.
    /// True when one was produced by `Clone`-ing the other (or they
    /// were both cloned from a common ancestor); false for two
    /// independently `Core::new`-constructed instances even with the
    /// same binding.
    ///
    /// Used by `graphrefly-graph`'s `mount` to enforce the "shared-Core
    /// only" v1 invariant — cross-Core mount is post-M6.
    #[must_use]
    pub fn same_dispatcher(&self, other: &Core) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }

    /// Downgrade to a [`WeakCore`] handle that doesn't contribute to
    /// strong refcount of the underlying state / binding / wave_owner.
    ///
    /// Used by binding-stored long-lived closures (e.g.
    /// `register_producer_build`-stored `ProducerBuildFn`s) to avoid the
    /// Arc cycle:
    ///
    /// ```text
    /// BenchBinding → registry → producer_builds[fn_id]
    ///   → closure → strong Arc<dyn _Binding> → BenchBinding
    /// ```
    ///
    /// Closures hold `WeakCore` and `Weak<dyn _Binding>` instead, then
    /// upgrade-on-fire (returning early if either weak is dangling —
    /// indicating the host BenchCore was already dropped). Upgraded
    /// strong refs live only for the build closure's invocation; sinks
    /// the build closure spawns close over those upgraded strongs and
    /// stay alive only while the producer is active (cleared via
    /// `producer_deactivate` on last-subscriber unsubscribe).
    #[must_use]
    pub fn weak_handle(&self) -> WeakCore {
        WeakCore {
            state: Arc::downgrade(&self.state),
            binding: Arc::downgrade(&self.binding),
            deferred_producer_ops: Arc::downgrade(&self.deferred_producer_ops),
            registry: Arc::downgrade(&self.registry),
            generation: self.generation,
        }
    }

    /// Number of distinct connected-component partitions tracked by
    /// the per-subgraph union-find registry (Slice X5 substrate).
    /// Two threads emitting into nodes with distinct partitions will
    /// run truly parallel once Y1 wires the wave engine through the
    /// registry; X5 reports the partition count for inspection
    /// (acceptance bar + debugging) but the wave engine still uses
    /// the legacy Core-level `wave_owner`.
    #[must_use]
    pub fn partition_count(&self) -> usize {
        self.registry.lock().component_count()
    }

    /// Resolve `node`'s partition identity per the per-subgraph
    /// union-find registry (Slice X5 substrate). Two nodes with the
    /// same `SubgraphId` are connected via dep edges (transitively)
    /// and share a partition lock under Y1+; nodes in different
    /// partitions can run truly parallel.
    ///
    /// Returns `None` for unregistered nodes.
    #[must_use]
    pub fn partition_of(&self, node: NodeId) -> Option<crate::subgraph::SubgraphId> {
        self.registry.lock().partition_of(node)
    }

    /// Push a deferred producer operation. Called by operator sinks
    /// when a Core method returns `PartitionOrderViolation`.
    ///
    /// For `Emit` and `Error` variants, the caller MUST retain the
    /// handle before pushing (the drain releases it after firing).
    /// `Complete` and `Callback` variants have no handle to retain.
    pub fn push_deferred_producer_op(&self, op: DeferredProducerOp) {
        self.deferred_producer_ops.lock().push(op);
    }

    /// Drain deferred producer ops when no partitions are held on
    /// the current thread. Each op may itself produce new deferred
    /// ops (e.g., a deferred subscribe activates a producer whose
    /// build defers further subscribes), so drain in a loop until
    /// the queue is empty.
    ///
    /// Called from `try_subscribe` after its `wave_guard` drops,
    /// and from `BatchGuard::drop` after releasing all wave_guards.
    /// Skips silently if partitions are still held (nested context).
    /// Maximum number of drain iterations before panicking with a
    /// diagnostic. Prevents unbounded loops from buggy callbacks that
    /// keep pushing more deferred ops indefinitely.
    const MAX_DEFERRED_DRAIN_ITERATIONS: u32 = 1000;

    pub(crate) fn drain_deferred_producer_ops(&self) {
        if held_partitions::any_held() {
            return;
        }
        let mut iterations = 0u32;
        loop {
            let deferred_ops: Vec<DeferredProducerOp> = {
                let mut ops = self.deferred_producer_ops.lock();
                if ops.is_empty() {
                    break;
                }
                std::mem::take(&mut *ops)
            };
            iterations += 1;
            assert!(
                iterations <= Self::MAX_DEFERRED_DRAIN_ITERATIONS,
                "drain_deferred_producer_ops exceeded {} iterations — \
                 a deferred callback is likely pushing unbounded ops. \
                 Iteration {iterations}, batch size {}.",
                Self::MAX_DEFERRED_DRAIN_ITERATIONS,
                deferred_ops.len(),
            );
            for op in deferred_ops {
                match op {
                    DeferredProducerOp::Emit { node_id, handle } => {
                        self.emit(node_id, handle);
                        self.binding.release_handle(handle);
                    }
                    DeferredProducerOp::Complete { node_id } => {
                        self.complete(node_id);
                    }
                    DeferredProducerOp::Error { node_id, handle } => {
                        self.error(node_id, handle);
                        self.binding.release_handle(handle);
                    }
                    DeferredProducerOp::Callback(f) => {
                        f();
                    }
                }
            }
        }
    }

    // Q3 (2026-05-09) introduced `Core::partition_box_of(node)` to
    // resolve a partition's `Arc<SubgraphLockBox>` for per-partition
    // state access. The D1 patch (2026-05-09) moved Slice G's
    // `tier3_emitted_this_wave` set off `SubgraphLockBox::state` to a
    // per-thread thread-local in `crate::batch`, eliminating
    // `partition_box_of`'s only callers (`commit_emission` /
    // `commit_emission_verbatim`). The helper is REMOVED rather than
    // kept dead — Q-beyond will resurrect a similar shape when the
    // CoreState shard layout actually needs per-partition lookups.

    /// Acquire `seed`'s partition `wave_owner` re-entrant mutex with
    /// retry-validate against concurrent union/split. Mirrors
    /// graphrefly-py's `subgraph_locks.py::lock_for` retry pattern
    /// (lines 154–178): a concurrent `union_nodes` may redirect
    /// `seed`'s partition root between our `lock_for` resolve and
    /// our `lock_arc` call; if so, the held guard is on a stale
    /// (but still valid) `SubgraphLockBox` whose `Arc` no longer
    /// matches the registry's canonical box for `seed`'s current
    /// root. Release + retry up to [`crate::subgraph::MAX_LOCK_RETRIES`].
    ///
    /// Returns the held guard. Caller holds it for the wave's
    /// duration; drop releases.
    ///
    /// **Panics** if `seed` is not registered (caller violation —
    /// every wave entry takes a `NodeId` already in `s.nodes`, and
    /// the P12-fixed lock-discipline guarantees registry membership
    /// is published atomically with state). **Panics** on exceeding
    /// `MAX_LOCK_RETRIES` — pathological union activity.
    ///
    /// Slice Y1 / Phase E (2026-05-08).
    pub(crate) fn partition_wave_owner_lock_arc(
        &self,
        seed: NodeId,
    ) -> Result<WaveOwnerGuard, PartitionOrderViolation> {
        /// Scope-guard for the H+ thread-local refcount entry. Released on
        /// Drop unless `into_consumed()` is called (the success path).
        /// Ensures balance even on panic between `check_and_acquire` and
        /// successful `WaveOwnerGuard` construction (`lock_arc()` /
        /// `lock_for_validate()` could in principle panic; defensive).
        struct AcquireGuard {
            sid: crate::subgraph::SubgraphId,
            consumed: bool,
        }
        impl AcquireGuard {
            fn into_consumed(mut self) {
                self.consumed = true;
            }
        }
        impl Drop for AcquireGuard {
            fn drop(&mut self) {
                if !self.consumed {
                    held_partitions::release(self.sid);
                }
            }
        }

        for _ in 0..crate::subgraph::MAX_LOCK_RETRIES {
            let (sid, lock_box) = {
                let mut reg = self.registry.lock();
                reg.lock_for(seed).expect(
                    "partition_wave_owner_lock_arc: seed must be registered \
                     (P12-fix invariant: registry membership is published \
                     atomically with `s.nodes`)",
                )
            };
            // Phase H+ option (d) /qa N1(a) widened variant: BEFORE
            // acquiring the parking_lot lock, check ascending-order if
            // this thread already holds at least one partition AND we're
            // not in a producer build closure. Panics on violation.
            // Also increments the thread-local refcount for `sid`. The
            // `AcquireGuard` ensures the refcount is released on EVERY
            // exit path — successful return (via `into_consumed()`),
            // retry-validate failure (Drop fires), retry-exhaustion panic
            // (Drop fires before unwind), or a panic in `lock_arc()` /
            // `lock_for_validate()` (Drop fires during unwind).
            held_partitions::check_and_acquire(sid)?;
            let acquire_guard = AcquireGuard {
                sid,
                consumed: false,
            };
            let inner = lock_box.wave_owner.lock_arc();
            // Re-validate post-acquire. If a concurrent `union` redirected
            // `seed`'s root between our `lock_for` and `lock_arc`, the
            // registry's current box for `seed` differs from what we hold.
            let still_valid = self.registry.lock().lock_for_validate(seed, &lock_box);
            if still_valid {
                acquire_guard.into_consumed();
                // `lock_box` is unused after this point — the D1 patch
                // moved Slice G tier3 tracking off the per-partition
                // `SubgraphLockBox::state` to a per-thread thread-local,
                // so the guard no longer carries the box reference.
                drop(lock_box);
                return Ok(WaveOwnerGuard { sid, inner });
            }
            // Stale — drop the parking_lot guard. The AcquireGuard's
            // Drop releases the held_partitions refcount automatically.
            // Yield to give the contending writer a chance to make
            // forward progress before re-resolving (QA-fix group 2 —
            // earlier tight-spin could monopolize a CPU under sustained
            // pathological union/split activity).
            drop(inner);
            drop(acquire_guard);
            std::thread::yield_now();
        }
        panic!(
            "partition_wave_owner_lock_arc: exceeded {} retries for seed {:?} \
             — pathological concurrent union/split activity. Mirrors py \
             `_MAX_LOCK_RETRIES`.",
            crate::subgraph::MAX_LOCK_RETRIES,
            seed
        );
    }

    /// BFS from `seed` along `s.children` (downstream consumer cascade
    /// for DATA / RESOLVED / INVALIDATE / COMPLETE / ERROR / TEARDOWN)
    /// and `meta_companions` (R1.3.9.d TEARDOWN cascade). Collects
    /// every partition reachable from `seed`, returning the unique
    /// `SubgraphId`s sorted ascending — the canonical lock-acquisition
    /// order per session-doc Q7 / decision D092 that guarantees
    /// deadlock-freedom across cross-partition waves.
    ///
    /// Holds the state lock + registry lock for the BFS duration
    /// (lock order `state → registry` per the P12-fix invariant).
    /// Bounded by the cascade graph reachable from `seed`; for typical
    /// apps the partition count is small (1–3) and the BFS is
    /// negligible relative to wave drain.
    ///
    /// Used by [`Core::begin_batch_for`] to compute the upfront-
    /// acquired partition set for per-seed waves. Closure-form
    /// [`Core::batch`] doesn't have a seed and uses
    /// [`Core::all_partitions_lock_boxes`] instead.
    ///
    /// Slice Y1 / Phase E (2026-05-08).
    pub(crate) fn compute_touched_partitions(
        &self,
        seed: NodeId,
    ) -> SmallVec<[crate::subgraph::SubgraphId; 4]> {
        let s = self.lock_state();
        let mut reg = self.registry.lock();
        let mut partitions: SmallVec<[crate::subgraph::SubgraphId; 4]> = SmallVec::new();
        let mut visited: HashSet<NodeId> = HashSet::default();
        let mut stack: SmallVec<[NodeId; 16]> = SmallVec::new();
        stack.push(seed);
        while let Some(n) = stack.pop() {
            if !visited.insert(n) {
                continue;
            }
            if let Some(p) = reg.partition_of(n) {
                if !partitions.contains(&p) {
                    partitions.push(p);
                }
            }
            if let Some(children) = s.children.get(&n) {
                stack.extend(children.iter().copied());
            }
            if let Some(rec) = s.nodes.get(&n) {
                stack.extend(rec.meta_companions.iter().copied());
            }
        }
        partitions.sort_unstable_by_key(|sid| sid.raw());
        partitions
    }

    /// Snapshot of every currently-existing partition's lock box, in
    /// ascending [`crate::subgraph::SubgraphId`] order (canonical
    /// lock-acquisition order per session-doc Q7 / D092). Used by
    /// closure-form [`Core::batch`] / [`Core::begin_batch`] which
    /// don't have a known seed and must serialize against every
    /// existing partition.
    ///
    /// Slice Y1 / Phase E (2026-05-08).
    pub(crate) fn all_partitions_lock_boxes(
        &self,
    ) -> Vec<(
        crate::subgraph::SubgraphId,
        Arc<crate::subgraph::SubgraphLockBox>,
    )> {
        self.registry.lock().all_partitions()
    }
}

/// Walk the undirected dep-edge graph from `start`, optionally
/// skipping ONE edge in both directions, and optionally treating
/// additional edges as if present. Returns every reachable
/// [`NodeId`].
///
/// Implementation note: uses a stack (`pop()` on a `SmallVec`) — i.e.
/// DFS traversal order. For pure reachability the order doesn't
/// matter (the visited set is identical to BFS); the function is
/// named "walk" rather than "BFS" to avoid implying that traversal
/// distance is meaningful (QA-fix group 2 — earlier name
/// `bfs_undirected_dep_graph` was misleading).
///
/// **Edge convention:** the dep edge `parent → child` represents
/// data flow from `parent` (a dep) to `child` (the consumer). It
/// appears in `s.children[parent]` as `child`, and in
/// `s.nodes[child].dep_records` as `parent`. `skip_edge =
/// Some((parent, child))` skips both forward (`parent → child`) and
/// backward (`child → parent`) traversals of that edge. Each
/// `(p, c)` pair in `extra_edges` is treated as if `c ∈
/// s.children[p]` and `p ∈ s.nodes[c].dep_records` — used for
/// "what would connectivity look like if THESE edges were also
/// present?" lookahead.
///
/// Used by Slice Y1 / Phase F (D3 split-eager, 2026-05-09):
/// - **P13 widening (pre-removal connectivity):** call with
///   `skip_edge = Some((removed_parent, removed_child))` AND
///   `extra_edges = added_edges_in_set_deps_call` so a `set_deps`
///   that simultaneously removes one edge AND adds another path
///   isn't falsely flagged as disconnecting (QA-fix #4 2026-05-09 —
///   without `extra_edges`, the pre-mutation BFS doesn't see the
///   would-be-added edges and rejects the conservative case).
/// - **Actual split execution (post-removal):** call with
///   `skip_edge = None` and `extra_edges = &[]`; the visited set is
///   the keep-side of the split (the side containing `start`).
pub(crate) fn walk_undirected_dep_graph(
    s: &CoreState,
    start: NodeId,
    skip_edge: Option<(NodeId, NodeId)>,
    extra_edges: &[(NodeId, NodeId)],
) -> HashSet<NodeId> {
    let mut visited: HashSet<NodeId> = HashSet::default();
    let mut queue: SmallVec<[NodeId; 32]> = SmallVec::new();
    queue.push(start);
    while let Some(cur) = queue.pop() {
        if !visited.insert(cur) {
            continue;
        }
        if let Some(consumers) = s.children.get(&cur) {
            for &c in consumers {
                let is_skipped = skip_edge.is_some_and(|(sp, sc)| cur == sp && c == sc);
                if !is_skipped && !visited.contains(&c) {
                    queue.push(c);
                }
            }
        }
        if let Some(rec) = s.nodes.get(&cur) {
            for d in rec.dep_records.iter().map(|r| r.node) {
                let is_skipped = skip_edge.is_some_and(|(sp, sc)| cur == sc && d == sp);
                if !is_skipped && !visited.contains(&d) {
                    queue.push(d);
                }
            }
        }
        // Virtual extra edges (e.g. would-be-added edges in
        // pre-mutation BFS).
        for &(ep, ec) in extra_edges {
            if cur == ep && !visited.contains(&ec) {
                queue.push(ec);
            }
            if cur == ec && !visited.contains(&ep) {
                queue.push(ep);
            }
        }
    }
    visited
}

impl Core {
    /// Test-only inspection: number of `PendingBatch`es queued for
    /// `node` in the current wave. Used by Slice X4 D2 regression
    /// tests to pin the "common case = single batch, no SmallVec
    /// spill" perf invariant.
    ///
    /// Returns `None` if no `pending_notify` entry exists for `node`
    /// (no tier-1+ message has been queued for this node yet in this
    /// wave). `Some(0)` is unreachable by construction (a vacant
    /// entry implies no batches; an occupied entry has at least one).
    #[cfg(any(test, debug_assertions))]
    #[must_use]
    pub fn pending_batch_count(&self, node: NodeId) -> Option<usize> {
        // Q-beyond Sub-slice 2 (D108, 2026-05-09): pending_notify lives
        // on per-thread `WaveState`. Test callers run on the same
        // thread that ran the wave, so the per-thread placement is
        // observable here.
        crate::batch::with_wave_state(|ws| {
            ws.pending_notify
                .get(&node)
                .map(|entry| entry.batches.len())
        })
    }

    /// Configure the Core-global cap on pause replay buffer length. When set,
    /// any per-node pause buffer that would exceed `cap` drops the oldest
    /// message(s) from the front; the dropped count is reported back via the
    /// resume callback (see [`ResumeReport`]). `None` (default) means
    /// unbounded; messages buffer indefinitely until the lockset clears.
    pub fn set_pause_buffer_cap(&self, cap: Option<usize>) {
        self.lock_state().pause_buffer_cap = cap;
    }

    /// Configure the replay buffer cap on `node_id` (R2.6.5 / Lock 6.G —
    /// Slice E1, 2026-05-07). `None` disables the buffer. `Some(N)` keeps
    /// the last `N` DATA emissions in a circular buffer; late subscribers
    /// receive them as part of the per-tier handshake (between START and
    /// any terminal). Switching from a larger cap to a smaller cap evicts
    /// the front of the buffer to fit; switching to `None` drains the
    /// buffer entirely. Each evicted/drained handle's retain is released
    /// back to the binding.
    ///
    /// # Panics
    ///
    /// Panics if `node_id` is not registered.
    pub fn set_replay_buffer_cap(&self, node_id: NodeId, cap: Option<usize>) {
        // QA A7 (2026-05-07): normalize `Some(0)` to `None`. Two ways to
        // express "disabled" is confusing: `push_replay_buffer` already
        // treats `Some(0)` as no-op, so persisting it adds nothing.
        let cap = match cap {
            Some(0) => None,
            other => other,
        };
        let to_release: Vec<HandleId> = {
            let mut s = self.lock_state();
            let rec = s.require_node_mut(node_id);
            rec.replay_buffer_cap = cap;
            match cap {
                None => rec.replay_buffer.drain(..).collect(),
                Some(c) => {
                    let mut drained = Vec::new();
                    while rec.replay_buffer.len() > c {
                        if let Some(h) = rec.replay_buffer.pop_front() {
                            drained.push(h);
                        }
                    }
                    drained
                }
            }
        };
        for h in to_release {
            self.binding.release_handle(h);
        }
    }

    /// Reconfigure the pause mode for `node_id` (canonical §2.6 — Slice F
    /// audit close, 2026-05-07). Default for new nodes is
    /// [`PausableMode::Default`]; switch to [`PausableMode::ResumeAll`]
    /// for nodes whose pause-window emit history must be observable
    /// verbatim, or [`PausableMode::Off`] for nodes intrinsically
    /// pause-immune.
    ///
    /// # Errors
    ///
    /// - [`SetPausableModeError::UnknownNode`] — `node_id` is not
    ///   registered.
    /// - [`SetPausableModeError::WhilePaused`] — the node currently
    ///   holds at least one pause lock. Changing mode mid-pause would
    ///   lose buffered content or strand a `pending_wave` flag — resume
    ///   all locks first.
    pub fn set_pausable_mode(
        &self,
        node_id: NodeId,
        mode: PausableMode,
    ) -> Result<(), SetPausableModeError> {
        let mut s = self.lock_state();
        let rec = s
            .nodes
            .get_mut(&node_id)
            .ok_or(SetPausableModeError::UnknownNode(node_id))?;
        if rec.pause_state.is_paused() {
            return Err(SetPausableModeError::WhilePaused);
        }
        rec.pausable = mode;
        Ok(())
    }

    /// Configure the wave-drain iteration cap (R4.3 / Lock 2.F′). The wave
    /// engine aborts a drain after `cap` iterations with a diagnostic panic.
    /// Default is `10_000` — high enough to avoid false positives on legitimate
    /// fan-in cascades, low enough to surface runtime cycles within seconds.
    ///
    /// Lower this only when running adversarial / property-based tests that
    /// want fast cycle detection. Raise it only with concrete evidence that a
    /// legitimate workload needs more iterations than the default — and even
    /// then, prefer to tune the workload (per-subgraph batching, etc.) over
    /// raising the cap.
    ///
    /// # Panics
    ///
    /// Panics if `cap == 0` — a zero cap would abort every wave on the very
    /// first iteration, deadlocking any subsequent dispatcher work.
    pub fn set_max_batch_drain_iterations(&self, cap: u32) {
        assert!(cap > 0, "max_batch_drain_iterations must be > 0");
        self.lock_state().max_batch_drain_iterations = cap;
    }

    /// Send a message UPSTREAM from `node_id` to each of its declared deps
    /// (canonical R1.4.1 — Slice F audit, F2 / 2026-05-07).
    ///
    /// The dispatcher rejects tier-3 (DATA / RESOLVED) and tier-5
    /// (COMPLETE / ERROR) per R1.4.1: value and terminal-lifecycle planes
    /// are downstream-only. All other tiers (0 START, 1 DIRTY, 2 PAUSE /
    /// RESUME, 4 INVALIDATE, 6 TEARDOWN) pass.
    ///
    /// # Routing per tier
    ///
    /// - **Tier 0 ([`Message::Start`]):** no-op. START is a per-subscription
    ///   handshake, not a routable wire signal — sending it upstream has no
    ///   well-defined target.
    /// - **Tier 1 ([`Message::Dirty`]):** no-op. The dep's "something
    ///   changed" notification is its own [`Self::emit`] / commit
    ///   responsibility; ignoring upstream DIRTY hints is safe.
    /// - **Tier 2 ([`Message::Pause`] / [`Message::Resume`]):** translates
    ///   to [`Self::pause`] / [`Self::resume`] on each dep. Lock id is
    ///   forwarded verbatim. Errors from individual deps are accumulated
    ///   in the `dep_errors` field of the returned report.
    /// - **Tier 4 ([`Message::Invalidate`]):** translates to
    ///   [`Self::invalidate`] on each dep. Note: canonical R1.4.2
    ///   distinguishes "downstream INVALIDATE" (cache clear + cascade) from
    ///   "upstream INVALIDATE" (plain forward, no self-process). The Rust
    ///   port v1 SIMPLIFICATION delegates to the same `Core::invalidate`
    ///   path — upstream INVALIDATE here DOES clear dep caches and cascade.
    ///   If a "plain forward" mode surfaces as a real consumer need, add
    ///   `up_with_options`.
    /// - **Tier 6 ([`Message::Teardown`]):** translates to
    ///   [`Self::teardown`] on each dep. Cascades per the standard
    ///   teardown path.
    ///
    /// # Errors
    ///
    /// - [`UpError::UnknownNode`] — `node_id` is not registered.
    /// - [`UpError::TierForbidden`] — tier 3 or tier 5.
    pub fn up(&self, node_id: NodeId, message: Message) -> Result<(), UpError> {
        // QA A10 (2026-05-07): check unknown node BEFORE tier rejection
        // for consistent error UX — `up(unknown, Data)` and
        // `up(unknown, Pause)` both report `UnknownNode` rather than
        // splitting on the tier.
        let dep_ids: Vec<NodeId> = {
            let s = self.lock_state();
            let rec = s.nodes.get(&node_id).ok_or(UpError::UnknownNode(node_id))?;
            rec.dep_ids_vec()
        };
        let tier = message.tier();
        if tier == 3 || tier == 5 {
            return Err(UpError::TierForbidden { tier });
        }
        for dep_id in dep_ids {
            match message {
                Message::Pause(lock) => {
                    let _ = self.pause(dep_id, lock);
                }
                Message::Resume(lock) => {
                    let _ = self.resume(dep_id, lock);
                }
                Message::Invalidate => {
                    self.invalidate(dep_id);
                }
                Message::Teardown => {
                    self.teardown(dep_id);
                }
                // Tier 0 START + tier 1 DIRTY: no-op upstream per the
                // routing table above.
                _ => {}
            }
        }
        Ok(())
    }

    /// Allocate a unique [`LockId`] for use with [`Self::pause`] /
    /// [`Self::resume`]. Convenience for callers that don't already have an
    /// id-allocation scheme; user-supplied ids work too.
    #[must_use]
    pub fn alloc_lock_id(&self) -> LockId {
        let mut s = self.lock_state();
        let id = LockId::new(s.next_lock_id);
        s.next_lock_id += 1;
        id
    }

    /// Access the binding boundary for this Core.
    ///
    /// Used by `graphrefly-graph` for snapshot serialization (M4.E1 / D166):
    /// `Graph::snapshot()` calls `binding.serialize_handle(cache)` to
    /// project each node's cached value into portable JSON.
    #[must_use]
    pub fn binding_ptr(&self) -> &Arc<dyn BindingBoundary> {
        &self.binding
    }

    // -------------------------------------------------------------------
    // Registration — unified `register()` (D030, Slice D)
    //
    // All node kinds (State / Producer / Derived / Dynamic / Operator)
    // funnel through `Core::register(NodeRegistration) -> NodeId`. Sugar
    // wrappers (`register_state` / `register_producer` / `register_derived`
    // / `register_dynamic` / `register_operator`) build a `NodeRegistration`
    // and delegate. There is no parallel registration path internally.
    // -------------------------------------------------------------------

    /// Unified node registration (D030).
    ///
    /// `reg` describes the node's identity (deps + closure-form fn id OR
    /// typed-op + per-kind opts). The kind is **derived from the field
    /// shape**, not stored — see [`NodeKind`].
    ///
    /// Sugar wrappers below ([`Self::register_state`],
    /// [`Self::register_producer`], [`Self::register_derived`],
    /// [`Self::register_dynamic`], [`Self::register_operator`]) build the
    /// registration for the common kinds and delegate here. Direct callers
    /// that need uncommon combinations (e.g., a partial-true derived) can
    /// invoke this method directly.
    ///
    /// # Errors
    ///
    /// Errors are returned in evaluation order — earlier phases short-circuit
    /// later ones, so a single registration produces at most one variant.
    ///
    /// **Phase 1 — lock-released, side-effect-free validation:**
    /// - [`RegisterError::OperatorWithoutDeps`] — `reg` carries an op but
    ///   `deps` is empty. Operator nodes need at least one dep — for
    ///   subscription-managed combinators with no declared deps, use
    ///   [`Self::register_producer`] instead.
    /// - [`RegisterError::InitialOnlyForStateNodes`] — `reg.opts.initial`
    ///   is non-sentinel for a non-state shape (deps non-empty, or
    ///   fn_or_op present). State nodes are the only kind with an initial
    ///   cache.
    ///
    /// **Phase 2 — operator scratch construction (lock-released):**
    /// - [`RegisterError::OperatorSeedSentinel`] — `reg` carries `Op(Scan)`
    ///   / `Op(Reduce)` with a `NO_HANDLE` seed. R2.5.3 — stateful folders
    ///   must have a real seed.
    ///
    /// **Phase 3 — state-lock validation (folded with insertion under a
    /// single lock acquisition per /qa F1 to prevent TOCTOU between
    /// validation and `nodes.insert`):**
    /// - [`RegisterError::UnknownDep`] — any element of `reg.deps` is not
    ///   a registered node id.
    /// - [`RegisterError::TerminalDep`] — a dep is terminal (COMPLETE /
    ///   ERROR) AND not resubscribable — would create a permanent wedge.
    ///
    /// All errors are construction-time invariants — the dispatcher
    /// rejects the registration before any reactive state is created.
    /// On `Err`, no node has been added and any handle retains taken on
    /// the way in (operator scratch seed retains via
    /// [`BindingBoundary::retain_handle`]) have been released
    /// lock-released — see [`ScratchReleaseGuard`] for the RAII
    /// discipline that covers both early-return AND unwind paths.
    /// `Last { default }` retains its `default` handle on the same
    /// release path.
    #[allow(clippy::too_many_lines)]
    pub fn register(&self, reg: NodeRegistration) -> Result<NodeId, RegisterError> {
        let NodeRegistration {
            deps,
            fn_or_op,
            opts,
        } = reg;
        let NodeOpts {
            initial,
            equals,
            partial,
            is_dynamic,
            pausable,
            replay_buffer,
        } = opts;

        // Derive the field shape from fn_or_op + deps.
        let (fn_id, op) = match fn_or_op {
            Some(NodeFnOrOp::Fn(f)) => (Some(f), None),
            Some(NodeFnOrOp::Op(o)) => (None, Some(o)),
            None => (None, None),
        };

        // Phase 1 — lock-released, side-effect-free validation. Errors
        // here return BEFORE any handle retain is taken.
        //
        //   - State (no deps + no fn + no op) is the only kind with `initial`.
        //   - Dynamic flag only meaningful when fn + non-empty deps.
        //   - Operator (op present) must have deps (P9: operator without deps
        //     would skip activation — use a producer instead).
        let is_state_shape = deps.is_empty() && fn_id.is_none() && op.is_none();
        if op.is_some() && deps.is_empty() {
            return Err(RegisterError::OperatorWithoutDeps);
        }
        if initial != NO_HANDLE && !is_state_shape {
            return Err(RegisterError::InitialOnlyForStateNodes);
        }

        // Phase 2 — build per-operator scratch struct (may take handle
        // retains via `binding.retain_handle` for Scan/Reduce/Last seed).
        // Lock-released per Slice E (D045) handshake discipline. Returns
        // `OperatorSeedSentinel` BEFORE retain so an Err leaves no
        // dangling handles.
        let scratch = match op {
            Some(operator_op) => self.make_op_scratch(operator_op)?,
            None => None,
        };

        // Wrap scratch in an RAII guard immediately after Phase 2. From
        // here on, ANY early return / unwind path correctly releases the
        // scratch's handle retains via `OperatorScratch::release_handles`
        // (Slice H /qa F2 — defense against panics between Phase 2 and
        // Phase 3 cleanup branch). Lock-released because the guard is
        // declared BEFORE `lock_state()` below — variable destruction
        // order is reverse declaration order, so the `MutexGuard` drops
        // first on any return path.
        let scratch_guard = ScratchReleaseGuard::new(scratch, &*self.binding);

        // Phase 3 — state-lock-required validation, FOLDED with insertion
        // under a single `lock_state()` acquisition per /qa F1. The
        // pre-/qa version split this into two acquisitions (one for
        // validation, one for `alloc_node_id` + `nodes.insert`), opening
        // a TOCTOU window where a concurrent `Core::complete(dep)` on a
        // non-resubscribable dep could slip in and recreate the wedge
        // `TerminalDep` was designed to prevent. Single locked region
        // closes the gap.
        let mut s = self.lock_state();

        for &dep in &deps {
            if !s.nodes.contains_key(&dep) {
                return Err(RegisterError::UnknownDep(dep));
            }
        }
        // Slice F audit (2026-05-07): mirror `set_deps`'s `TerminalDep`
        // rejection at registration time. Adding a non-resubscribable
        // terminal node as a dep at registration creates a permanent wedge.
        for &dep in &deps {
            let dep_rec = s.require_node(dep);
            if dep_rec.terminal.is_some() && !dep_rec.resubscribable {
                return Err(RegisterError::TerminalDep(dep));
            }
        }

        // Validation passed — install. Take scratch out of the guard
        // (disarms the release-on-drop) and continue using `s`.
        let installed_scratch = scratch_guard.take();

        let id = s.alloc_node_id();

        // `tracked`: Static derived + Operator track all deps; Dynamic
        // starts empty and fills via fn return; State / Producer have no
        // deps so tracked is empty.
        let tracked: HashSet<usize> = if op.is_some() {
            (0..deps.len()).collect()
        } else if is_dynamic {
            HashSet::new()
        } else if fn_id.is_some() && !deps.is_empty() {
            // Static derived
            (0..deps.len()).collect()
        } else {
            HashSet::new()
        };

        let dep_records: Vec<DepRecord> = deps.iter().map(|&d| DepRecord::new(d)).collect();

        // §10 perf (D047): compute topo_rank = 1 + max(dep ranks).
        let topo_rank = if deps.is_empty() {
            0
        } else {
            deps.iter()
                .filter_map(|&d| s.nodes.get(&d).map(|r| r.topo_rank))
                .max()
                .unwrap_or(0)
                .saturating_add(1)
        };

        let rec = NodeRecord {
            dep_records,
            fn_id,
            op,
            is_dynamic,
            equals,
            cache: initial,
            has_fired_once: initial != NO_HANDLE,
            subscribers: HashMap::new(),
            subscribers_revision: 0,
            tracked,
            dirty: false,
            involved_this_wave: false,
            pause_state: PauseState::Active,
            pausable,
            replay_buffer_cap: replay_buffer,
            replay_buffer: VecDeque::new(),
            terminal: None,
            has_received_teardown: false,
            resubscribable: false,
            meta_companions: Vec::new(),
            partial,
            topo_rank,
            received_mask: 0,
            op_scratch: installed_scratch,
        };
        s.nodes.insert(id, rec);
        s.children.insert(id, HashSet::new());
        for &dep in &deps {
            s.children.entry(dep).or_default().insert(id);
        }
        // Slice Y1 (D3 / D090 — P12 fix, 2026-05-08): maintain partition
        // membership BEFORE dropping the state lock. Closes the
        // eventual-consistency window where a concurrent thread observed
        // the new node in `s.nodes` / new edges in `s.children` but the
        // registry hadn't unioned the partition yet. Today benign
        // (`partition_of` is debug-only); under Y1's wave engine
        // migration `lock_for(node)` consumes registry state on the hot
        // path, and the window means `lock_for` could resolve to a
        // partition that's been topologically unioned in `s.children`
        // but not yet in `registry`.
        //
        // **Lock-discipline invariant:** `state lock → registry mutex`
        // (one-way; never registry → state). Registry mutex is
        // uncontended in the X5 substrate — the only acquisition sites
        // are this one + `Core::set_deps` + the read-only accessors
        // `partition_count`/`partition_of` and (Y1+) `lock_for` — none
        // of which take the state lock — so the inner critical section
        // adds negligible latency.
        {
            let mut reg = self.registry.lock();
            reg.ensure_registered(id);
            for &dep in &deps {
                reg.union_nodes(id, dep);
            }
        }
        drop(s);
        self.fire_topology_event(&crate::topology::TopologyEvent::NodeRegistered(id));
        Ok(id)
    }

    /// Sugar over [`Self::register`] — register a state node. `initial`
    /// may be [`NO_HANDLE`] to start sentinel.
    ///
    /// `partial` is accepted for surface consistency (D019); for state
    /// nodes it has no effect (state nodes don't fire fn).
    ///
    /// # Errors
    ///
    /// State registration is structurally simple — no deps, no op — so
    /// the only reachable variant is none in practice. Returns
    /// [`Result`] for surface consistency with [`Self::register`].
    pub fn register_state(
        &self,
        initial: HandleId,
        partial: bool,
    ) -> Result<NodeId, RegisterError> {
        self.register(NodeRegistration {
            deps: Vec::new(),
            fn_or_op: None,
            opts: NodeOpts {
                initial,
                partial,
                ..NodeOpts::default()
            },
        })
    }

    /// Sugar over [`Self::register`] — register a producer node (D031,
    /// Slice D). No deps; fn fires once on first subscribe; cleanup runs
    /// via [`BindingBoundary::producer_deactivate`] when the last
    /// subscriber unsubscribes.
    ///
    /// The fn body uses the binding's `ProducerCtx`-equivalent helper
    /// (see `graphrefly-operators::producer`) to subscribe to other Core
    /// nodes — the zip / concat / race / takeUntil pattern.
    ///
    /// # Errors
    ///
    /// Producer registration has no user-supplied deps, so structurally
    /// none of [`RegisterError`]'s variants are reachable. Returns
    /// [`Result`] for surface consistency with [`Self::register`].
    pub fn register_producer(&self, fn_id: FnId) -> Result<NodeId, RegisterError> {
        self.register(NodeRegistration {
            deps: Vec::new(),
            fn_or_op: Some(NodeFnOrOp::Fn(fn_id)),
            opts: NodeOpts {
                // Producers have no deps — the first-run gate is degenerate.
                partial: true,
                ..NodeOpts::default()
            },
        })
    }

    /// Sugar over [`Self::register`] — register a derived (static) node.
    /// `partial` controls the R2.5.3 first-run gate (D011).
    ///
    /// # Errors
    ///
    /// - [`RegisterError::UnknownDep`] — any element of `deps` is not
    ///   registered.
    /// - [`RegisterError::TerminalDep`] — a dep is terminal and not
    ///   resubscribable.
    pub fn register_derived(
        &self,
        deps: &[NodeId],
        fn_id: FnId,
        equals: EqualsMode,
        partial: bool,
    ) -> Result<NodeId, RegisterError> {
        self.register(NodeRegistration {
            deps: deps.to_vec(),
            fn_or_op: Some(NodeFnOrOp::Fn(fn_id)),
            opts: NodeOpts {
                equals,
                partial,
                ..NodeOpts::default()
            },
        })
    }

    /// Sugar over [`Self::register`] — register a dynamic node (fn
    /// declares its actually-tracked dep indices per fire). `partial`
    /// controls the R2.5.3 first-run gate (D011).
    ///
    /// # Errors
    ///
    /// - [`RegisterError::UnknownDep`] — any element of `deps` is not
    ///   registered.
    /// - [`RegisterError::TerminalDep`] — a dep is terminal and not
    ///   resubscribable.
    pub fn register_dynamic(
        &self,
        deps: &[NodeId],
        fn_id: FnId,
        equals: EqualsMode,
        partial: bool,
    ) -> Result<NodeId, RegisterError> {
        self.register(NodeRegistration {
            deps: deps.to_vec(),
            fn_or_op: Some(NodeFnOrOp::Fn(fn_id)),
            opts: NodeOpts {
                equals,
                partial,
                is_dynamic: true,
                ..NodeOpts::default()
            },
        })
    }

    /// Build a fresh [`OperatorScratch`](crate::op_state::OperatorScratch)
    /// box for an operator variant, taking any required handle retains.
    /// Shared between `register_operator` (initial install),
    /// `reset_for_fresh_lifecycle` (resubscribable terminal-cycle
    /// re-install), and Phase G of `Subscription::Drop` (D-α
    /// resubscribable + non-terminal deactivate re-install).
    ///
    /// # Errors
    ///
    /// Returns [`RegisterError::OperatorSeedSentinel`] if `op` is `Scan`
    /// / `Reduce` with a [`NO_HANDLE`] seed (R2.5.3 — stateful folders
    /// must have a real seed). Refcount discipline: the seed-sentinel
    /// check happens BEFORE [`BindingBoundary::retain_handle`], so an
    /// `Err` leaves no handles dangling.
    fn make_op_scratch(
        &self,
        op: OperatorOp,
    ) -> Result<Option<Box<dyn crate::op_state::OperatorScratch>>, RegisterError> {
        Self::make_op_scratch_with_binding(&*self.binding, op)
    }

    /// Associated-function variant of [`Self::make_op_scratch`] that
    /// takes the binding explicitly. Used by call sites that have a
    /// `&dyn BindingBoundary` but not a `&Core` (notably
    /// [`Subscription::Drop`]'s Phase G, which holds only a
    /// `Weak<Mutex<CoreState>>` and operates on `s.binding`).
    pub(crate) fn make_op_scratch_with_binding(
        binding: &dyn BindingBoundary,
        op: OperatorOp,
    ) -> Result<Option<Box<dyn crate::op_state::OperatorScratch>>, RegisterError> {
        use crate::op_state::{
            DistinctState, LastState, PairwiseState, ReduceState, ScanState, SkipState, TakeState,
            TakeWhileState,
        };
        // Slice H (2026-05-07): Scan/Reduce seed-sentinel checks happen
        // BEFORE retain_handle so an Err return leaves no handles dangling.
        //
        // Slice H /qa F13 (2026-05-07): for retaining variants, allocate
        // the `Box<State>` BEFORE calling `binding.retain_handle`. If
        // `Box::new` panics (e.g., OOM-as-panic), no retain has happened
        // yet — no leak. If `retain_handle` panics after Box succeeds,
        // the `Box<State>` is dropped on unwind; State has no handle yet
        // (we haven't touched the registry refcount), so still no leak.
        // Caller wraps the returned scratch in `ScratchReleaseGuard` to
        // cover panics AFTER make_op_scratch returns.
        match op {
            OperatorOp::Scan { seed, .. } => {
                if seed == NO_HANDLE {
                    return Err(RegisterError::OperatorSeedSentinel);
                }
                let state = Box::new(ScanState { acc: seed });
                binding.retain_handle(seed);
                Ok(Some(state))
            }
            OperatorOp::Reduce { seed, .. } => {
                if seed == NO_HANDLE {
                    return Err(RegisterError::OperatorSeedSentinel);
                }
                let state = Box::new(ReduceState { acc: seed });
                binding.retain_handle(seed);
                Ok(Some(state))
            }
            OperatorOp::DistinctUntilChanged { .. } => Ok(Some(Box::new(DistinctState::default()))),
            OperatorOp::Pairwise { .. } => Ok(Some(Box::new(PairwiseState::default()))),
            OperatorOp::Take { .. } => Ok(Some(Box::new(TakeState::default()))),
            OperatorOp::Skip { .. } => Ok(Some(Box::new(SkipState::default()))),
            OperatorOp::TakeWhile { .. } => Ok(Some(Box::new(TakeWhileState))),
            OperatorOp::Last { default } => {
                let state = Box::new(LastState {
                    latest: NO_HANDLE,
                    default,
                });
                if default != NO_HANDLE {
                    binding.retain_handle(default);
                }
                Ok(Some(state))
            }
            OperatorOp::TapFirst { .. } => {
                Ok(Some(Box::new(crate::op_state::TapFirstState::default())))
            }
            OperatorOp::Settle { .. } => {
                Ok(Some(Box::new(crate::op_state::SettleState::default())))
            }
            OperatorOp::Map { .. }
            | OperatorOp::Filter { .. }
            | OperatorOp::Combine { .. }
            | OperatorOp::WithLatestFrom { .. }
            | OperatorOp::Merge
            | OperatorOp::Tap { .. }
            | OperatorOp::Valve => Ok(None),
        }
    }

    /// Sugar over [`Self::register`] — register a built-in operator node
    /// (Slice C-1, D009; D026 generic scratch). The operator dispatch path
    /// lives in `fire_operator`; `op` selects which per-operator FFI
    /// method on [`BindingBoundary`] gets called per fire.
    ///
    /// For stateful operators ([`OperatorOp::Scan`] / [`Reduce`] /
    /// [`Last`] with a default), the seed/default handle is captured
    /// into the appropriate
    /// [`OperatorScratch`](crate::op_state::OperatorScratch) struct
    /// stored at [`NodeRecord::op_scratch`], and Core takes one retain
    /// share via [`BindingBoundary::retain_handle`].
    ///
    /// # Errors
    ///
    /// - [`RegisterError::OperatorWithoutDeps`] — `deps` is empty (use
    ///   [`Self::register_producer`] instead).
    /// - [`RegisterError::OperatorSeedSentinel`] — `op` is
    ///   [`OperatorOp::Scan`] / [`OperatorOp::Reduce`] with a
    ///   [`NO_HANDLE`] seed.
    /// - [`RegisterError::UnknownDep`] — any element of `deps` is not
    ///   registered.
    /// - [`RegisterError::TerminalDep`] — a dep is terminal and not
    ///   resubscribable.
    pub fn register_operator(
        &self,
        deps: &[NodeId],
        op: OperatorOp,
        opts: OperatorOpts,
    ) -> Result<NodeId, RegisterError> {
        self.register(NodeRegistration {
            deps: deps.to_vec(),
            fn_or_op: Some(NodeFnOrOp::Op(op)),
            opts: NodeOpts {
                equals: opts.equals,
                partial: opts.partial,
                ..NodeOpts::default()
            },
        })
    }

    // -------------------------------------------------------------------
    // Subscription
    // -------------------------------------------------------------------

    /// Subscribe a sink to a node. Returns a [`Subscription`] handle —
    /// dropping the handle unsubscribes the sink. Per §10.12, no manual
    /// `unsubscribe(node, id)` call is required.
    ///
    /// Push-on-subscribe (R1.2.3, R2.2.3 step 4): the sink is registered AFTER
    /// the START handshake fires. The handshake contents depend on node
    /// state:
    /// - Sentinel cache + live (non-terminal): `[START]`
    /// - Cached + live: `[START, DATA(handle)]`
    ///
    /// Subscribe-after-terminal semantics (canonical R2.2.7.a / R2.2.7.b,
    /// D118 2026-05-10):
    /// - **Resubscribable + terminal** (any TEARDOWN state): the subscribe
    ///   call first **resets** the node — clears `terminal`,
    ///   `has_fired_once`, `has_received_teardown`, all `dep_handles` to
    ///   `NO_HANDLE`, all `dep_terminals` to `None`, drains the pause
    ///   lockset, clears the replay buffer. The new subscriber receives a
    ///   fresh `[START]` (cache survives for state nodes per R2.2.8;
    ///   sentinel for compute). The `wipe_ctx` cleanup hook fires
    ///   lock-released so binding-side `ctx.store` starts fresh.
    /// - **Non-resubscribable + terminal** (any TEARDOWN state): the
    ///   subscribe is rejected — `try_subscribe` returns
    ///   [`SubscribeError::TornDown`]; this method (the panic-on-error
    ///   variant) panics with the diagnostic.
    ///
    /// Activation (R2.2.3 step 5): if this is the first subscriber and the
    /// node is a derived/dynamic compute, recursively activate deps so their
    /// cached handles fill our `dep_handles`.
    ///
    /// # Panics
    ///
    /// Panics if:
    /// - Subscribing would violate the Phase H+ ascending partition-order
    ///   invariant ([`SubscribeError::PartitionOrderViolation`]).
    /// - The node is non-resubscribable AND has terminated
    ///   ([`SubscribeError::TornDown`], R2.2.7.b).
    #[allow(clippy::needless_pass_by_value)] // Sink is `Arc<dyn Fn>`; we clone for the subscribers map and call it directly. Taking by value matches the ergonomics callers expect.
    pub fn subscribe(&self, node_id: NodeId, sink: Sink) -> Subscription {
        match self.try_subscribe(node_id, sink) {
            Ok(sub) => sub,
            Err(e) => panic!("{e}"),
        }
    }

    /// Fallible subscribe. Returns `Err` on:
    /// - Partition order violation (Phase H+ STRICT, D115) — caller defers.
    /// - Non-resubscribable terminal node (R2.2.7.b, D118) — caller skips.
    ///
    /// Used by `subscribe` (unwraps both errors as panics) and producer-
    /// pattern operator sinks (match on variant).
    #[allow(clippy::needless_pass_by_value)]
    pub fn try_subscribe(
        &self,
        node_id: NodeId,
        sink: Sink,
    ) -> Result<Subscription, SubscribeError> {
        // Subscribe protocol (Slice E rework, post-handshake-reentry-lift):
        //
        // 1. Acquire `wave_owner` first (re-entrant; same-thread passes
        //    through, cross-thread blocks). This is the cross-thread
        //    serialization point that preserves R1.3.5.a happens-after
        //    ordering across the lock-released handshake fire.
        // 2. Acquire state lock briefly: alloc sub_id, run resubscribable
        //    reset if applicable, snapshot handshake state, install sink
        //    in `subscribers`. Drop state lock.
        // 3. Fire handshake LOCK-RELEASED. Per-tier slices (R1.3.5.a):
        //    `[Start]` / `[Data(cache)]?` / `[Complete]?` / `[Error(h)]?`
        //    / `[Teardown]?`. Empty tiers are skipped. Sink callbacks
        //    may re-enter Core freely — same-thread re-entry passes
        //    through `wave_owner` reentrantly.
        // 4. Run activation under `run_wave` if needed (first subscriber
        //    on a non-state node).
        // 5. Drop `wave_owner`.
        //
        // Race-fix discipline: the sink is installed in `subscribers`
        // BEFORE the state lock drops, so concurrent threads that
        // acquire `wave_owner` after our scope sees the sink already
        // registered. Cross-thread emits block on `wave_owner` until
        // we drop it, ensuring all our handshake calls land before
        // any concurrent wave's flush observes the sink.

        // Acquire the partition's `wave_owner` first — cross-thread
        // serialization point. Per Slice Y1 / Phase E (2026-05-08),
        // subscribe routes through the per-partition lock instead of
        // a Core-global one. Subscribe touches only `node_id`'s
        // partition (activation cascade stays within the partition
        // because dep edges are unioned). `partition_wave_owner_lock_arc`
        // does retry-validate against concurrent union/split.
        // `lock_arc()` is `!Send`; same-thread reentrant.
        let wave_guard = self.partition_wave_owner_lock_arc(node_id)?;

        let (sub_id, tier_slices, needs_activation, did_reset) = {
            let mut s = self.lock_state();

            // R2.2.7.b (D118, 2026-05-10): non-resubscribable + terminal →
            // reject. The stream is permanently over; subscribe gets a
            // clean error rather than a confusing replay of past events.
            // Operators (zip / concat / race / ...) match on the variant
            // and skip the source. Drop the wave_owner before returning so
            // a concurrent waiter can proceed.
            //
            // The `has_received_teardown` flag is irrelevant here —
            // `terminal.is_some()` alone gates rejection. The auto-TEARDOWN
            // cascade in R2.6.4 / Lock 6.F means torn_down lags terminal
            // by at most one wave anyway; a brief mid-wave window where
            // `terminal.is_some() && !torn_down` is reachable but the
            // rejection decision doesn't depend on which side of that
            // window we're in.
            let should_reject = {
                let rec = s.require_node(node_id);
                !rec.resubscribable && rec.terminal.is_some()
            };
            if should_reject {
                drop(s);
                drop(wave_guard);
                return Err(SubscribeError::TornDown { node: node_id });
            }

            let sub_id = s.alloc_sub_id();

            // R2.2.7.a (D118, 2026-05-10): resubscribable + terminal → reset
            // to fresh lifecycle, regardless of TEARDOWN state. The prior
            // `!has_received_teardown` guard (Slice A+B F3) conflated
            // "TEARDOWN is the cleanup signal of the previous activation
            // cycle" with "permanent destruction" — corrected per the
            // canonical-spec amendment. `reset_for_fresh_lifecycle` clears
            // `has_received_teardown` along with `terminal`,
            // `has_fired_once`, dep_records sentinels, pause lockset, and
            // replay buffer. `wipe_ctx` fires lock-released after the
            // state lock drops so the binding's `ctx.store` starts fresh.
            let needs_reset = {
                let rec = s.require_node(node_id);
                rec.resubscribable && rec.terminal.is_some()
            };
            if needs_reset {
                self.reset_for_fresh_lifecycle(&mut s, node_id);
            }

            // Snapshot handshake state under lock.
            //
            // F5 (/qa 2026-05-10): post-D118 R2.2.7.a/b, the snapshot
            // ALWAYS sees a non-terminal node here — `should_reject`
            // already rejected non-resubscribable terminal above, and
            // `needs_reset` cleared resubscribable terminal back to
            // `terminal = None` / `has_received_teardown = false`.
            // The pre-D118 terminal-replay + teardown-replay branches
            // were dead code in this post-D118 sequence and are
            // removed.
            let (cache, is_state, first_subscriber) = {
                let rec = s.require_node(node_id);
                debug_assert!(
                    rec.terminal.is_none(),
                    "R2.2.7.a/b invariant: post-reject/reset, terminal must be None"
                );
                debug_assert!(
                    !rec.has_received_teardown,
                    "R2.2.7.a invariant: reset clears has_received_teardown"
                );
                (rec.cache, rec.is_state(), rec.subscribers.is_empty())
            };

            // Build per-tier handshake slices. Each non-empty slice is
            // fired as a separate sink call (R1.3.5.a tier-split).
            let mut tier_slices: SmallVec<[Vec<Message>; 4]> = SmallVec::new();
            tier_slices.push(vec![Message::Start]);
            if cache != NO_HANDLE {
                tier_slices.push(vec![Message::Data(cache)]);
            }
            // Slice E1 (R2.6.5 / Lock 6.G): replay buffered DATA between
            // [Start] (and the cache slice, if present) and any terminal.
            // Each buffered handle becomes a separate per-tier slice so
            // late subscribers see the historical Data sequence as
            // distinct sink calls.
            //
            // Dedupe: when a cache slice is present and the buffer's last
            // entry is the same handle (the typical case — cache always
            // tracks the last DATA emitted, and the buffer's tail entry
            // is that same DATA), skip the last buffer entry to avoid
            // delivering Data(cache) twice. For state nodes whose cache
            // survives unsubscribe, the buffer may have older entries
            // the cache doesn't reflect; the dedupe only drops the
            // single trailing entry that equals cache. (QA A1, 2026-05-07)
            let replay_handles: Vec<HandleId> = {
                let rec = s.require_node(node_id);
                let cap = rec.replay_buffer_cap.unwrap_or(0);
                if cap == 0 {
                    Vec::new()
                } else {
                    let mut v: Vec<HandleId> = rec.replay_buffer.iter().copied().collect();
                    if cache != NO_HANDLE && v.last() == Some(&cache) {
                        v.pop();
                    }
                    v
                }
            };
            for h in &replay_handles {
                tier_slices.push(vec![Message::Data(*h)]);
            }

            // Install sink BEFORE dropping state lock so any thread that
            // subsequently acquires `wave_owner` (after our scope ends)
            // sees the sink already registered.
            //
            // Slice X4 / D2: bump `subscribers_revision` alongside the
            // insert so a pending_notify entry opened earlier in the same
            // wave (e.g. inside `batch(|| { emit(s, h1); subscribe(s,
            // late); emit(s, h2); })`) starts a fresh `PendingBatch` on
            // its next `queue_notify` push — making the new sink visible
            // to subsequent emits' flush slices, while the pre-subscribe
            // batch's snapshot stays frozen so we don't double-deliver
            // earlier emits via the wave's flush AND the new sub's
            // handshake replay.
            {
                let rec = s.require_node_mut(node_id);
                rec.subscribers.insert(sub_id, sink.clone());
                rec.subscribers_revision = rec.subscribers_revision.wrapping_add(1);
            }

            let needs_activation = first_subscriber && !is_state;
            (sub_id, tier_slices, needs_activation, needs_reset)
            // state lock drops here
        };

        // Slice E2 (R2.4.6 / D055): on resubscribable terminal reset, fire
        // `wipe_ctx` LOCK-RELEASED so the binding drops its `NodeCtxState`
        // entry (clearing both `store` and any residual `current_cleanup`).
        // The new subscriber's first invoke_fn sees a fresh empty store.
        // Fires AFTER the state lock drops so the binding's
        // `node_ctx.lock()` can't deadlock against Core's state lock — and
        // BEFORE the handshake so the wipe is observable before any
        // user-visible interaction with the new lifecycle.
        if did_reset {
            self.binding.wipe_ctx(node_id);
        }

        // Fire handshake LOCK-RELEASED. Sink may re-enter Core; same-
        // thread re-entry passes through `wave_owner` reentrantly.
        // Cross-thread emits block at `wave_owner` until our scope ends.
        //
        // A7 (Slice F, 2026-05-07): per-tier slice fire is wrapped in
        // `catch_unwind`. The sink is installed in `subscribers` BEFORE
        // the handshake fires (load-bearing — concurrent threads observe
        // the sink immediately). If a sink panics on tier N, the panic
        // would otherwise unwind out of `subscribe` BEFORE the
        // `Subscription` handle is constructed, leaving the sink
        // registered in `subscribers` with no user-held handle to drop.
        // Subsequent waves' `flush_notifications` would re-fire the
        // panicking sink forever.
        //
        // On panic: remove the sink from `subscribers` (via the
        // already-allocated `sub_id`), drop `_wave_guard` cleanly via
        // RAII, and resume the unwind so the user observes the panic at
        // the call site. Same effect as the user dropping the
        // `Subscription` immediately, but pre-emptive.
        for slice in &tier_slices {
            let sink_clone = sink.clone();
            let slice_ref: &[Message] = slice;
            let result = catch_unwind(AssertUnwindSafe(|| sink_clone(slice_ref)));
            if let Err(panic_payload) = result {
                // Remove the orphaned sink. Best-effort: if the node was
                // since torn down (e.g., the sink itself called teardown
                // before panicking), the entry may already be gone.
                {
                    let mut s = self.lock_state();
                    if let Some(rec) = s.nodes.get_mut(&node_id) {
                        rec.subscribers.remove(&sub_id);
                        // Slice X4 / D2: keep revision-tracked snapshot
                        // discipline consistent with the install site —
                        // any pending_notify entry that already absorbed
                        // the panicking sink under the post-install
                        // revision should start a fresh batch on its
                        // next queue_notify push.
                        rec.subscribers_revision = rec.subscribers_revision.wrapping_add(1);
                    }
                }
                std::panic::resume_unwind(panic_payload);
            }
        }

        // Run activation if needed. `run_wave_for(node_id)` acquires
        // only the partitions transitively touched from `node_id`
        // (downstream cascade + meta-companion teardown reach) — same-
        // partition activation re-enters reentrantly. Slice Y1 / Phase E.
        if needs_activation {
            self.run_wave_for(node_id, |this| {
                let mut s = this.lock_state();
                this.activate_derived(&mut s, node_id);
            });
        }

        // Phase H+ STRICT (D115): drop the wave_guard BEFORE draining
        // deferred producer ops. The deferred ops may need to subscribe
        // to sources in lower-numbered partitions — if wave_guard is
        // still held, the ascending-order check would reject them.
        drop(wave_guard);

        // Drain deferred producer ops now that no partitions are held
        // on this thread. The drain is a loop because each deferred op
        // may itself produce new deferred ops.
        self.drain_deferred_producer_ops();

        Ok(Subscription {
            state: Arc::downgrade(&self.state),
            node_id,
            sub_id,
        })
    }

    /// Mark `node_id` as resubscribable per R2.2.7. Resubscribable nodes
    /// reset their terminal-lifecycle state on a fresh subscribe — see
    /// [`Self::subscribe`].
    ///
    /// Configuration call — must be made before the node has any active
    /// subscribers, since changing the policy mid-flight would surprise
    /// existing observers.
    ///
    /// # Panics
    ///
    /// Panics if the node has subscribers (the policy is observable
    /// behavior; changing it after the fact would change semantics for
    /// existing sinks).
    pub fn set_resubscribable(&self, node_id: NodeId, resubscribable: bool) {
        let mut s = self.lock_state();
        let rec = s.require_node_mut(node_id);
        assert!(
            rec.subscribers.is_empty(),
            "set_resubscribable: node already has subscribers; \
             configure resubscribable before any subscribe call"
        );
        rec.resubscribable = resubscribable;
    }

    /// Reset a resubscribable node's terminal-lifecycle state. Called from
    /// `subscribe` when a late subscriber arrives at a flagged node.
    ///
    /// Released: terminal-slot retain (Error handle), all per-dep terminal
    /// retains (Error handles), all data_batch retains.
    /// Cleared: `terminal`, `has_fired_once`, `has_received_teardown`, all
    /// dep_records to sentinel, the pause lockset (any held locks are
    /// released — replay buffer drops silently because there are no
    /// subscribers to flush to).
    fn reset_for_fresh_lifecycle(&self, s: &mut CoreState, node_id: NodeId) {
        // Phase 1: collect wave-state handle releases + take the old
        // op_scratch + reset other state. Take all mutations under one
        // borrow so the post-borrow phases don't re-walk dep_records.
        let (prev_op, mut old_scratch, handles_to_release, pause_buffer_payloads) = {
            let rec = s.require_node_mut(node_id);
            let mut hs = Vec::new();
            if let Some(TerminalKind::Error(h)) = rec.terminal {
                hs.push(h);
            }
            for dr in &rec.dep_records {
                if let Some(TerminalKind::Error(h)) = dr.terminal {
                    hs.push(h);
                }
                for &h in &dr.data_batch {
                    hs.push(h);
                }
                // Slice C-3 /qa: also release `prev_data`. Prior to this
                // collection, `reset_for_fresh_lifecycle` overwrote
                // `dr.prev_data = NO_HANDLE` without releasing the old
                // handle, leaking one share per dep per resubscribable
                // cycle. The leak was masked because no test exercised
                // the per-dep `prev_data` retain across a lifecycle
                // reset; surfaced by the T1 tightening of
                // `last_releases_buffered_latest_on_lifecycle_reset`.
                if dr.prev_data != NO_HANDLE {
                    hs.push(dr.prev_data);
                }
            }
            // Take pause_state's buffer; collect its payload handles for
            // release (they were retained at queue_notify time; buffer
            // drops because the new subscriber starts fresh).
            let mut pulled = Vec::new();
            if let PauseState::Paused { ref mut buffer, .. } = rec.pause_state {
                for msg in buffer.drain(..) {
                    if let Some(h) = msg.payload_handle() {
                        pulled.push(h);
                    }
                }
            }
            // Slice E1: drain the replay buffer too — the new subscriber
            // gets a fresh lifecycle and shouldn't see prior emissions.
            for h in rec.replay_buffer.drain(..) {
                pulled.push(h);
            }
            // Reset wave / lifecycle state.
            rec.terminal = None;
            rec.has_fired_once = rec.cache != NO_HANDLE && rec.is_state();
            rec.has_received_teardown = false;
            for dr in &mut rec.dep_records {
                dr.prev_data = NO_HANDLE;
                dr.data_batch.clear();
                dr.terminal = None;
                dr.dirty = false;
                dr.involved_this_wave = false;
            }
            rec.pause_state = PauseState::Active;
            rec.involved_this_wave = false;
            rec.dirty = false;
            // §10.13 perf (D047): reset received_mask — fresh lifecycle
            // means all deps are sentinel again.
            rec.received_mask = 0;
            // P7 (Slice A close /qa): Dynamic nodes clear `tracked` so
            // the post-reset first fire repopulates from the fn's
            // returned tracked-deps set.
            if rec.is_dynamic {
                rec.tracked.clear();
            }
            // Take the old scratch out so we can release its handles and
            // install a fresh one. Operator op is copied for the
            // rebuild step below.
            let prev_op = rec.op;
            let old = std::mem::take(&mut rec.op_scratch);
            (prev_op, old, hs, pulled)
        };

        // Phase 2 (Slice C-3 /qa P1 — RETAIN-BEFORE-RELEASE ordering):
        // build the fresh scratch FIRST, taking new retains on any
        // seed/default handles. This must run BEFORE Phase 3 releases
        // the old scratch's shares — if old `acc` (Scan/Reduce) or old
        // `latest` (Last) aliases the new `seed`/`default` (common:
        // `fold(seed, x) == seed` interns to the same registry entry),
        // releasing the old share first could collapse the binding's
        // registry slot to zero (production bindings remove the value
        // entry on refcount-zero — see `tests/common/mod.rs:191-204`),
        // and a subsequent `retain_handle` on the new seed would bump a
        // refcount on a slot whose value has been removed. By taking
        // the new retains first, we floor the refcount at ≥1 before
        // any release happens.
        let new_scratch = match prev_op {
            // Slice H: the OperatorOp stored on NodeRecord previously
            // passed `make_op_scratch` validation at registration time
            // (RegisterError::OperatorSeedSentinel for Scan/Reduce
            // sentinel seeds; Last { default: NO_HANDLE } is accepted
            // and never errors). Re-running it here on the same op
            // value is structurally guaranteed to succeed.
            Some(op) => self
                .make_op_scratch(op)
                .expect("invariant: stored OperatorOp passed make_op_scratch validation at registration time"),
            None => None,
        };

        // Phase 3: NOW release handles owned by the old op_scratch
        // (Scan/Reduce acc, Distinct/Pairwise prev, Last latest +
        // default). Safe per Phase 2's retain-first floor. The boxed
        // value is consumed and dropped after.
        if let Some(scratch) = old_scratch.as_mut() {
            scratch.release_handles(&*self.binding);
        }
        drop(old_scratch);

        // Phase 3b (D-α, D028 full close, 2026-05-10): drain the
        // per-Core `pending_scratch_release` queue populated by Phase G
        // on prior resubscribable + non-terminal deactivate cycles.
        // Queued boxes' shares may alias the seed/default we just
        // retained in Phase 2 (e.g., when scan's `acc` interns to the
        // same registry slot as `seed`). Releasing AFTER Phase 2's
        // floor keeps the registry slot at refcount ≥1 throughout —
        // mirrors the Phase 2/3 retain-before-release ordering. Safe
        // even when the queue is empty (no prior deactivations
        // happened).
        let queued: Vec<Box<dyn crate::op_state::OperatorScratch>> =
            std::mem::take(&mut s.pending_scratch_release);
        for mut scratch in queued {
            scratch.release_handles(&*self.binding);
        }

        // Phase 4: install the fresh scratch.
        {
            let rec = s.require_node_mut(node_id);
            rec.op_scratch = new_scratch;
        }

        // Phase 5: release wave-state handles collected in phase 1.
        for h in handles_to_release {
            self.binding.release_handle(h);
        }
        for h in pause_buffer_payloads {
            self.binding.release_handle(h);
        }
    }

    /// Activate `root` and any transitive uncached compute deps so their
    /// caches fill our dep_handles slots.
    ///
    /// Slice A close (M1): pure dep-walk + dep_handles population +
    /// pending_fires queueing. No `in_tick` management or `drain_and_flush`
    /// call — the outer caller (typically `Core::subscribe` via
    /// [`Core::run_wave`]) owns the wave lifecycle and drains lock-released
    /// around `invoke_fn`.
    ///
    /// Walk shape:
    ///   1. **Discover phase (DFS via Vec stack):** starting at `root`,
    ///      walk transitively-needing-activation deps via the `deps`
    ///      chain. Build an ordering where each node appears AFTER all
    ///      of its uncached compute deps — i.e., reverse topological
    ///      among the visited subgraph.
    ///   2. **Deliver phase (forward iteration):** for each visited
    ///      node in dep-first order, push deps' caches into the node's
    ///      `dep_handles` slots. Caches that were sentinel pre-walk are
    ///      filled because their parent's fn fires later in the wave's
    ///      drain loop and `commit_emission` propagates new caches forward
    ///      via `deliver_data_to_consumer` — the same path this method
    ///      uses for the initial seed. Adds the node to `pending_fires`
    ///      if its tracked-deps gate is satisfied; the wave-engine drain
    ///      fires the fn lock-released around `invoke_fn`.
    pub(crate) fn activate_derived(&self, s: &mut CoreState, root: NodeId) {
        // Phase 1: discover. DFS to collect every compute node reachable
        // via deps that doesn't yet have a cache and hasn't fired.
        // Record them in dep-first (post-order) so phase 2 can deliver
        // caches forward. Frame is `(node_id, finalize)` — `finalize=false`
        // means "first visit: push deps then re-push self with finalize=true";
        // `finalize=true` means "deps have been expanded, append self to
        // `order`."
        let mut visited: HashSet<NodeId> = HashSet::new();
        let mut order: Vec<NodeId> = Vec::new();
        let mut stack: Vec<(NodeId, bool)> = vec![(root, false)];
        while let Some((id, finalize)) = stack.pop() {
            if finalize {
                order.push(id);
                continue;
            }
            if !visited.insert(id) {
                continue;
            }
            stack.push((id, true));
            let dep_ids: Vec<NodeId> = s.require_node(id).dep_ids_vec();
            for dep_id in dep_ids {
                let (dep_is_state, dep_cache, dep_has_fired) = {
                    let dep_rec = s.require_node(dep_id);
                    (dep_rec.is_state(), dep_rec.cache, dep_rec.has_fired_once)
                };
                if !dep_is_state
                    && dep_cache == NO_HANDLE
                    && !dep_has_fired
                    && !visited.contains(&dep_id)
                {
                    stack.push((dep_id, false));
                }
            }
        }

        // Phase 2: deliver caches in dep-first order. For each node, walk
        // its deps and call `deliver_data_to_consumer` for any with caches.
        // Producer nodes (no deps + has fn — Slice D, D031) have no deps
        // to walk; queue them directly into `pending_fires` so the wave
        // engine fires their fn once on activation.
        //
        // Q-beyond Sub-slice 2 (D108, 2026-05-09): pending_fires lives on
        // per-thread WaveState. State lock + thread_local borrow are
        // independent; deliver_data_to_consumer also writes pending_fires
        // via WaveState (no nested with_wave_state borrows here).
        for &id in &order {
            let (dep_ids, is_producer) = {
                let rec = s.require_node(id);
                (rec.dep_ids_vec(), rec.is_producer())
            };
            if is_producer {
                crate::batch::with_wave_state(|ws| {
                    ws.pending_fires.insert(id);
                });
                continue;
            }
            for (i, dep_id) in dep_ids.iter().copied().enumerate() {
                let dep_cache = s.require_node(dep_id).cache;
                if dep_cache != NO_HANDLE {
                    self.deliver_data_to_consumer(s, id, i, dep_cache);
                }
            }
        }
    }

    // -------------------------------------------------------------------
    // Emission entry point
    // -------------------------------------------------------------------

    /// Set a state node's value. Triggers a wave (DIRTY → DATA/RESOLVED →
    /// fn fires for downstream).
    ///
    /// Silent no-op if the node has already terminated (R1.3.4). The handle
    /// passed in is still released by the caller's binding-side intern path
    /// — no implicit retain is consumed when the call short-circuits.
    ///
    /// # Panics
    ///
    /// Panics if `node_id` is not a state node, or if `new_handle` is
    /// [`NO_HANDLE`] (per R1.2.4, sentinel is not a valid DATA payload).
    pub fn emit(&self, node_id: NodeId, new_handle: HandleId) {
        match self.try_emit(node_id, new_handle) {
            Ok(()) => {}
            Err(e) => panic!("{e}"),
        }
    }

    /// Fallible emit. Returns `Err` on partition order violation
    /// (Phase H+ STRICT, D115). The public `emit` calls this and
    /// unwraps; `emit_or_defer` calls this and defers on Err.
    pub(crate) fn try_emit(
        &self,
        node_id: NodeId,
        new_handle: HandleId,
    ) -> Result<(), PartitionOrderViolation> {
        assert!(
            new_handle != NO_HANDLE,
            "NO_HANDLE is not a valid DATA payload (R1.2.4)"
        );
        // Validate + terminal short-circuit under a brief lock.
        //
        // emit() is valid for State and Producer nodes — both are
        // intrinsic sources whose values are not derived from declared
        // deps. State nodes get emit() from user code; Producer nodes
        // get emit() from sink callbacks the producer's build closure
        // registered (sink fires → re-enter Core → emit on self).
        // Derived / Dynamic / Operator nodes emit via their fn return
        // value through fire_fn / fire_operator, NOT via emit().
        {
            let s = self.lock_state();
            let rec = s.require_node(node_id);
            assert!(
                rec.is_state() || rec.is_producer(),
                "emit() is for state or producer nodes only; \
                 derived/dynamic/operator emit via their fn return value"
            );
            if rec.terminal.is_some() {
                drop(s);
                // Caller's intern share would otherwise leak; cache slot
                // ownership doesn't transfer because we're not advancing
                // cache. Released lock-released so the binding can't
                // deadlock against an internal binding mutex.
                self.binding.release_handle(new_handle);
                return Ok(());
            }
        }
        // Run wave on `node_id`'s touched partitions. Slice Y1 / Phase E:
        // emit cascades only via `s.children`, all unioned with `node_id`'s
        // partition by construction (dep edges = union edges). Common case
        // is a single-partition acquire — disjoint-partition emits run
        // truly parallel under per-partition `wave_owner`.
        self.try_run_wave_for(node_id, |this| {
            this.commit_emission(node_id, new_handle);
        })?;
        Ok(())
    }

    /// Emit or defer to wave-end on partition order violation.
    /// For producer-pattern operator sinks. Retains `handle` on defer;
    /// the drain releases it after firing (or on discard).
    pub fn emit_or_defer(&self, node_id: NodeId, new_handle: HandleId) {
        if self.try_emit(node_id, new_handle).is_err() {
            self.binding.retain_handle(new_handle);
            self.push_deferred_producer_op(DeferredProducerOp::Emit {
                node_id,
                handle: new_handle,
            });
        }
    }

    /// Read a node's current cache. Returns [`NO_HANDLE`] if sentinel.
    #[must_use]
    pub fn cache_of(&self, node_id: NodeId) -> HandleId {
        self.lock_state().require_node(node_id).cache
    }

    /// Whether the node's fn has fired at least once (compute) OR it has had
    /// a non-sentinel value (state).
    #[must_use]
    pub fn has_fired_once(&self, node_id: NodeId) -> bool {
        self.lock_state().require_node(node_id).has_fired_once
    }

    // -------------------------------------------------------------------
    // Read-side inspection helpers (Slice E+, M2)
    //
    // Non-panicking accessors for graph-layer introspection (`describe()`,
    // `observe()`, `node_count()`). All five return Option/empty for
    // unknown ids — they're meant to back walks over `node_ids()` where
    // the caller already knows the ids are valid, plus debugging /
    // dry-run probes that prefer "absence" over "panic".
    //
    // Keep these strictly read-only: no wave entry, no binding callbacks,
    // no lock release. Each takes the state lock once, copies a small
    // value, and drops the lock.
    // -------------------------------------------------------------------

    /// Snapshot of every registered `NodeId` in unspecified order. The
    /// order matches `HashMap` iteration over the internal node table —
    /// callers that need stable ordering should track names at the
    /// `Graph` layer (canonical spec §3.5 namespace).
    #[must_use]
    pub fn node_ids(&self) -> Vec<NodeId> {
        self.lock_state().nodes.keys().copied().collect()
    }

    /// Total number of nodes registered in this Core.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.lock_state().nodes.len()
    }

    /// Returns `Some(kind)` for known nodes, `None` for unknown. Kind is
    /// **derived** from the field shape per D030 — see [`NodeKind`].
    #[must_use]
    pub fn kind_of(&self, node_id: NodeId) -> Option<NodeKind> {
        self.lock_state().nodes.get(&node_id).map(NodeRecord::kind)
    }

    /// Snapshot of the node's deps in declaration order. Empty for
    /// unknown nodes or for state nodes (which have no deps).
    #[must_use]
    pub fn deps_of(&self, node_id: NodeId) -> Vec<NodeId> {
        self.lock_state()
            .nodes
            .get(&node_id)
            .map(NodeRecord::dep_ids_vec)
            .unwrap_or_default()
    }

    /// Returns `Some(kind)` if the node has terminated (R1.3.4) — the
    /// pair `Some(Complete)` / `Some(Error(h))` mirrors the wire message
    /// the node emitted. `None` for live nodes or unknown ids.
    #[must_use]
    pub fn is_terminal(&self, node_id: NodeId) -> Option<TerminalKind> {
        self.lock_state()
            .nodes
            .get(&node_id)
            .and_then(|r| r.terminal)
    }

    /// Whether the node has wave-scoped DIRTY pending (a tier-1 message
    /// queued but the matching tier-3 settle has not yet flushed).
    /// `false` for unknown ids. Mostly useful for `describe()` status
    /// classification (R3.6.1 `"dirty"`).
    #[must_use]
    pub fn is_dirty(&self, node_id: NodeId) -> bool {
        self.lock_state()
            .nodes
            .get(&node_id)
            .is_some_and(|r| r.dirty)
    }

    /// Snapshot of `parent`'s meta companion list (R1.3.9.d / R2.3.3 —
    /// the companions added via [`Self::add_meta_companion`]). Empty
    /// for unknown ids or for nodes with no companions registered.
    ///
    /// Used by the graph layer's `signal_invalidate` to filter meta
    /// children out of the broadcast (canonical R3.7.2 — meta caches
    /// are preserved across graph-wide INVALIDATE).
    #[must_use]
    pub fn meta_companions_of(&self, parent: NodeId) -> Vec<NodeId> {
        self.lock_state()
            .nodes
            .get(&parent)
            .map(|r| r.meta_companions.clone())
            .unwrap_or_default()
    }

    // -------------------------------------------------------------------
    // Wave engine — lives in `crate::batch` (Slice C-1 module split;
    // Slice A close M1 refactor lifted the binding-callback re-entrance
    // restrictions). The methods are still on `Core`; see `batch.rs` for:
    //
    //   - `run_wave` — wave entry, manages own locking.
    //   - `drain_and_flush` — drain phase, lock-released around invoke_fn.
    //   - `commit_emission` — lock-released around custom_equals.
    //   - `pick_next_fire`, `deliver_data_to_consumer`, `queue_notify`,
    //     `flush_notifications` — wave-engine helpers.
    // -------------------------------------------------------------------
}

// -----------------------------------------------------------------------
// COMPLETE / ERROR — terminal lifecycle + auto-cascade gating
// -----------------------------------------------------------------------

impl Core {
    /// Emit `[COMPLETE]` (R1.3.4) on `node_id`, marking it terminal. After
    /// this call:
    ///
    /// - Subsequent `Core::emit` on this node is a silent no-op (idempotent
    ///   termination).
    /// - The node's fn no longer fires.
    /// - The node's cache is preserved (last value still observable via
    ///   `cache_of`).
    /// - Children receive `[COMPLETE]` (tier 5 — bypasses pause buffer).
    /// - Auto-cascade gating (Lock 2.B): each child that has all of its
    ///   deps in a terminal state auto-emits its own `[COMPLETE]`. ERROR
    ///   dominates COMPLETE — if any of a child's deps emitted ERROR, the
    ///   child auto-cascades that ERROR instead.
    ///
    /// Idempotent: calling `complete` on an already-terminal node is a no-op.
    ///
    /// # Panics
    ///
    /// Panics if `node_id` is unknown.
    pub fn complete(&self, node_id: NodeId) {
        match self.try_complete(node_id) {
            Ok(()) => {}
            Err(e) => panic!("{e}"),
        }
    }

    /// Fallible complete. Returns `Err` on partition order violation.
    pub(crate) fn try_complete(&self, node_id: NodeId) -> Result<(), PartitionOrderViolation> {
        self.try_emit_terminal(node_id, TerminalKind::Complete)
    }

    /// Complete or defer to wave-end on partition order violation.
    /// For producer-pattern operator sinks.
    pub fn complete_or_defer(&self, node_id: NodeId) {
        match self.try_complete(node_id) {
            Ok(()) => {}
            Err(_) => {
                self.push_deferred_producer_op(DeferredProducerOp::Complete { node_id });
            }
        }
    }

    /// Emit `[ERROR, error_handle]` (R1.3.4) on `node_id`. `error_handle`
    /// must resolve to a non-sentinel value (R1.2.5) — the binding side has
    /// already interned the error value before this call. Same lifecycle
    /// effects as [`Self::complete`]; ERROR dominates COMPLETE in auto-
    /// cascade gating.
    ///
    /// # Panics
    ///
    /// Panics if `node_id` is unknown or `error_handle == NO_HANDLE`.
    pub fn error(&self, node_id: NodeId, error_handle: HandleId) {
        match self.try_error(node_id, error_handle) {
            Ok(()) => {}
            Err(e) => panic!("{e}"),
        }
    }

    /// Fallible error. Returns `Err` on partition order violation.
    pub(crate) fn try_error(
        &self,
        node_id: NodeId,
        error_handle: HandleId,
    ) -> Result<(), PartitionOrderViolation> {
        assert!(
            error_handle != NO_HANDLE,
            "NO_HANDLE is not a valid ERROR payload (R1.2.5)"
        );
        self.try_emit_terminal(node_id, TerminalKind::Error(error_handle))?;
        // The caller's intern share for `error_handle` is NOT transferred
        // to any slot — `terminate_node` takes its OWN retain for every
        // populated `terminal` and `dep_terminals` slot. Release the
        // caller's share here (mirrors `Core::emit`'s short-circuit
        // release on terminal). Without this, every `error()` call leaks
        // one binding-side handle ref. Slice A-bigger /qa item D fix.
        self.binding.release_handle(error_handle);
        Ok(())
    }

    /// Error or defer to wave-end on partition order violation.
    /// For producer-pattern operator sinks. Retains `handle` on defer;
    /// the drain releases it after firing (or on discard).
    pub fn error_or_defer(&self, node_id: NodeId, error_handle: HandleId) {
        if self.try_error(node_id, error_handle).is_err() {
            self.binding.retain_handle(error_handle);
            self.push_deferred_producer_op(DeferredProducerOp::Error {
                node_id,
                handle: error_handle,
            });
        }
    }

    fn try_emit_terminal(
        &self,
        node_id: NodeId,
        terminal: TerminalKind,
    ) -> Result<(), PartitionOrderViolation> {
        {
            let s = self.lock_state();
            assert!(s.nodes.contains_key(&node_id), "unknown node {node_id:?}");
        }
        // Wave on `node_id`'s touched partitions (Slice Y1 / Phase E).
        // COMPLETE / ERROR cascade follows `s.children` (in-partition by
        // union-find construction). The thunk acquires its own state lock
        // to queue the cascade.
        self.try_run_wave_for(node_id, |this| {
            let mut s = this.lock_state();
            this.terminate_node(&mut s, node_id, terminal);
        })
    }

    /// Set the node's terminal slot, queue the wire message, and cascade to
    /// children. Idempotent on already-terminal node (no-op).
    ///
    /// Iterative implementation (Slice A-bigger, M1-close): a work-queue
    /// drives the cascade so deep linear chains don't overflow the OS
    /// thread stack. Mirrors `path_from_to`'s explicit-stack pattern.
    fn terminate_node(&self, s: &mut CoreState, node_id: NodeId, terminal: TerminalKind) {
        let mut work: Vec<(NodeId, TerminalKind)> = vec![(node_id, terminal)];
        while let Some((id, t)) = work.pop() {
            if s.require_node(id).terminal.is_some() {
                continue; // Idempotent — already terminal.
            }
            // Take a refcount share for the terminal slot so the error
            // handle outlives the binding-side intern's transient share.
            if let TerminalKind::Error(h) = t {
                self.binding.retain_handle(h);
            }
            // Slice E2 /qa Q2(b) (D069): if a resubscribable node is
            // terminating with no live subscribers, queue eager
            // `wipe_ctx` for the wave's lock-released drain. This is the
            // mutually-exclusive complement of the `Subscription::Drop`
            // wipe site: when the LAST sub drops first then terminate
            // fires, subs are empty here and we queue; when terminate
            // fires WITH subs still alive, we DON'T queue (subs not
            // empty), and `Subscription::Drop` will fire wipe directly
            // when those subs eventually drop. Either way, exactly one
            // wipe fires per terminal lifecycle.
            let queue_wipe = {
                let rec = s.require_node(id);
                rec.resubscribable && rec.subscribers.is_empty()
            };
            s.require_node_mut(id).terminal = Some(t);
            // Q-beyond Sub-slice 2 + 3 (D108, 2026-05-09): pending_fires
            // and pending_wipes both live on per-thread WaveState. Single
            // borrow handles the queue-wipe push and the pending_fires
            // remove.
            crate::batch::with_wave_state(|ws| {
                if queue_wipe {
                    ws.pending_wipes.push(id);
                }
                // Drain pending fires for this node — fn won't fire on a
                // terminal node.
                ws.pending_fires.remove(&id);
            });
            // R1.3.8.b / Slice F (A3, 2026-05-07): if this node was paused
            // when terminating (the canonical case is the R1.3.8.c overflow
            // ERROR synthesis path), drain the pause buffer and release
            // each payload's queue_notify-time retain. Without this, the
            // buffer leaks one share per buffered DATA/RESOLVED/INVALIDATE.
            // Subscribers receive the terminal directly via the cascade
            // below (tier-5 bypasses the pause buffer); the buffered
            // content is moot post-terminal.
            let drained: Vec<HandleId> = {
                let rec = s.require_node_mut(id);
                let mut drained: Vec<HandleId> = Vec::new();
                if rec.pause_state.is_paused() {
                    // Take the buffered messages out, then collapse the
                    // pause state to Active so subsequent code observes a
                    // clean lifecycle. Idempotent on Active (no-op).
                    let prev = std::mem::replace(&mut rec.pause_state, PauseState::Active);
                    if let PauseState::Paused { buffer, .. } = prev {
                        drained.extend(buffer.into_iter().filter_map(Message::payload_handle));
                    }
                }
                // QA A4 (2026-05-07): drain replay buffer on terminate. A
                // non-resubscribable terminal ends the lifecycle; without
                // this drain the buffer's retains leak until `Drop for
                // CoreState`. Resubscribable nodes' replay buffers are
                // also drained (when they're hit by a terminal cascade);
                // a fresh subscribe rebuilds the buffer from scratch as
                // part of `reset_for_fresh_lifecycle`.
                drained.extend(rec.replay_buffer.drain(..));
                drained
            };
            for h in drained {
                self.binding.release_handle(h);
            }
            // Queue the wire message (tier 5 — bypasses pause buffer).
            let msg = match t {
                TerminalKind::Complete => Message::Complete,
                TerminalKind::Error(h) => Message::Error(h),
            };
            self.queue_notify(s, id, msg);
            // Cascade to children.
            let child_ids: Vec<NodeId> = s
                .children
                .get(&id)
                .map(|c| c.iter().copied().collect())
                .unwrap_or_default();
            for child_id in child_ids {
                let dep_idx = s.require_node(child_id).dep_index_of(id);
                let Some(idx) = dep_idx else { continue };
                // Mark this child's per-dep terminal slot. Take a retain on
                // the error handle for the slot share.
                {
                    let child = s.require_node_mut(child_id);
                    if child.dep_records[idx].terminal.is_some() {
                        // Idempotent — child already saw this dep terminate.
                        continue;
                    }
                    child.dep_records[idx].terminal = Some(t);
                }
                if let TerminalKind::Error(h) = t {
                    self.binding.retain_handle(h);
                }
                // Auto-cascade gating: if all deps now terminal, push child
                // onto the work queue with the chosen terminal.
                //
                // Slice C-1: kinds that opt out of Lock 2.B (currently
                // `Operator(Reduce)`) intercept upstream COMPLETE so they
                // can emit their accumulator before terminating. Instead of
                // cascading, queue the child for fn-fire — `fire_operator`
                // sees `dep_records[0].terminal` set and emits the
                // appropriate batch (Data(acc) + Complete on COMPLETE,
                // Error(h) on ERROR).
                let action = {
                    let child = s.require_node(child_id);
                    if child.terminal.is_some() {
                        ChildAction::None // Already terminated — no-op.
                    } else if child.all_deps_terminal() {
                        if child.skips_auto_cascade() {
                            ChildAction::QueueFire
                        } else {
                            ChildAction::Cascade(pick_cascade_terminal(&child.dep_records))
                        }
                    } else {
                        ChildAction::None
                    }
                };
                match action {
                    ChildAction::None => {}
                    ChildAction::Cascade(t_child) => {
                        work.push((child_id, t_child));
                    }
                    ChildAction::QueueFire => {
                        // Q-beyond Sub-slice 2 (D108, 2026-05-09):
                        // pending_fires lives on per-thread WaveState.
                        crate::batch::with_wave_state(|ws| {
                            ws.pending_fires.insert(child_id);
                        });
                    }
                }
            }
        }
    }
}

/// Outcome of Lock 2.B child gating in `terminate_node`'s cascade walk.
enum ChildAction {
    /// No cascade; child is already terminal or not yet all-deps-terminal.
    None,
    /// Auto-cascade with the picked terminal kind (ERROR dominates COMPLETE).
    Cascade(TerminalKind),
    /// Queue child for fn-fire instead of cascading. Used by operator
    /// kinds that intercept upstream terminal (Operator(Reduce)).
    QueueFire,
}

/// Lock 2.B cascade-terminal selection: ERROR dominates COMPLETE; first
/// ERROR seen wins. Caller has already verified all deps are terminal.
fn pick_cascade_terminal(dep_records: &[DepRecord]) -> TerminalKind {
    for dr in dep_records {
        if let Some(TerminalKind::Error(h)) = dr.terminal {
            return TerminalKind::Error(h);
        }
    }
    TerminalKind::Complete
}

// -----------------------------------------------------------------------
// TEARDOWN — destruction, with auto-COMPLETE prepend (R2.6.4 / Lock 6.F)
// -----------------------------------------------------------------------

impl Core {
    /// Tear `node_id` down. Per R2.6.4 / Lock 6.F:
    ///
    /// - **Auto-prepend COMPLETE.** If the node has not yet emitted a
    ///   terminal (`COMPLETE` / `ERROR`), `terminate_node` is called with
    ///   `Complete` first so subscribers see `[COMPLETE, TEARDOWN]`, not
    ///   bare `[TEARDOWN]`. This guarantees a clean end-of-stream signal
    ///   to async iterators and other consumers that wait on terminal
    ///   delivery.
    /// - **Idempotent on duplicate delivery.** The per-node
    ///   `has_received_teardown` flag is set on the first call; subsequent
    ///   `teardown` calls (or cascade visits from other paths) are silent
    ///   no-ops — no second `[COMPLETE, TEARDOWN]` pair to subscribers.
    /// - **Cascade downstream.** Each child is recursively torn down. The
    ///   child's own COMPLETE auto-cascades from `terminate_node`'s logic
    ///   (Lock 2.B); its TEARDOWN comes from this cascade.
    ///
    /// # Panics
    ///
    /// Panics if `node_id` is unknown.
    pub fn teardown(&self, node_id: NodeId) {
        match self.try_teardown(node_id) {
            Ok(()) => {}
            Err(e) => panic!("{e}"),
        }
    }

    /// Teardown or defer to wave-end on partition order violation.
    /// For producer-pattern operator sinks.
    pub fn teardown_or_defer(&self, node_id: NodeId) {
        match self.try_teardown(node_id) {
            Ok(()) => {}
            Err(_) => {
                self.push_deferred_producer_op(DeferredProducerOp::Callback(Box::new({
                    let core = self.clone();
                    move || {
                        core.teardown(node_id);
                    }
                })));
            }
        }
    }

    fn try_teardown(&self, node_id: NodeId) -> Result<(), PartitionOrderViolation> {
        {
            let s = self.lock_state();
            assert!(s.nodes.contains_key(&node_id), "unknown node {node_id:?}");
        }
        let torn_down: Arc<Mutex<Vec<NodeId>>> = Arc::new(Mutex::new(Vec::new()));
        let torn_down_for_wave = torn_down.clone();
        // TEARDOWN cascade follows `s.children` AND `meta_companions`
        // (R1.3.9.d) — meta-companions can cross partitions. Slice Y1 /
        // Phase E `compute_touched_partitions(node_id)` (called by
        // `run_wave_for`) walks both axes so the wave acquires every
        // partition reachable via the cascade.
        self.try_run_wave_for(node_id, move |this| {
            let mut s = this.lock_state();
            let collected = this.teardown_inner(&mut s, node_id);
            torn_down_for_wave.lock().extend(collected);
        })?;
        // Fire NodeTornDown for every cascaded id (root + metas +
        // downstream consumers that auto-cascaded). Outside the state
        // lock, matching fire_topology_event discipline.
        let ids = std::mem::take(&mut *torn_down.lock());
        for id in ids {
            self.fire_topology_event(&crate::topology::TopologyEvent::NodeTornDown(id));
        }
        Ok(())
    }

    /// Iterative teardown walk (Slice A-bigger, M1-close).
    ///
    /// The recursive shape was:
    ///   ```text
    ///   teardown(n):
    ///     if torn_down: return
    ///     mark torn_down
    ///     for meta in metas: teardown(meta)
    ///     terminate_node + queue Teardown
    ///     for child in children: teardown(child)
    ///   ```
    /// Deep linear chains (~10k nodes) overflowed the OS thread stack.
    ///
    /// The iterative shape uses a `Vec<Action>` stack with `Visit` and
    /// `EmitTeardown` actions. `Visit(n)` marks `n` torn-down (or no-ops
    /// if already), then pushes (in reverse order so LIFO pops in forward
    /// order) `Visit(child_K), …, Visit(child_1), EmitTeardown(n),
    /// Visit(meta_M), …, Visit(meta_1)`. The R1.3.9.d "metas first, then
    /// self, then children" ordering is preserved by the push order:
    /// metas pop first, recursively expand and emit; then `EmitTeardown(n)`
    /// pops and runs `terminate_node` + queue `Teardown`; then children
    /// pop. Idempotency via `has_received_teardown` keeps each node
    /// visited at most once even when multi-parent diamonds re-enter via
    /// a sibling path.
    fn teardown_inner(&self, s: &mut CoreState, root: NodeId) -> Vec<NodeId> {
        enum Action {
            Visit(NodeId),
            EmitTeardown(NodeId),
        }
        let mut stack: Vec<Action> = vec![Action::Visit(root)];
        // Topology accumulator: every node that actually emits TEARDOWN
        // (i.e. each `EmitTeardown(id)` site, NOT each `Visit` — visits
        // for already-torn-down nodes short-circuit on idempotency).
        let mut torn_down: Vec<NodeId> = Vec::new();
        while let Some(action) = stack.pop() {
            match action {
                Action::Visit(id) => {
                    if s.require_node(id).has_received_teardown {
                        continue; // Idempotent (R2.6.4).
                    }
                    s.require_node_mut(id).has_received_teardown = true;
                    // Push order: children first (pop LAST), then
                    // EmitTeardown(id), then metas (pop FIRST). Reverse
                    // each list so within-group order matches the original
                    // recursive iteration.
                    let children: Vec<NodeId> = s
                        .children
                        .get(&id)
                        .map(|c| c.iter().copied().collect())
                        .unwrap_or_default();
                    for &child in children.iter().rev() {
                        stack.push(Action::Visit(child));
                    }
                    stack.push(Action::EmitTeardown(id));
                    let metas: Vec<NodeId> = s.require_node(id).meta_companions.clone();
                    for &meta in metas.iter().rev() {
                        stack.push(Action::Visit(meta));
                    }
                }
                Action::EmitTeardown(id) => {
                    // Auto-prepend COMPLETE if not yet terminal. The (now
                    // iterative) terminate_node handles auto-cascade to
                    // children's own terminal slots per Lock 2.B.
                    let already_terminal = s.require_node(id).terminal.is_some();
                    if !already_terminal {
                        self.terminate_node(s, id, TerminalKind::Complete);
                    }
                    // Wire emission of the TEARDOWN itself (tier 6).
                    self.queue_notify(s, id, Message::Teardown);
                    torn_down.push(id);
                }
            }
        }
        torn_down
    }

    /// Attach `companion` as a meta companion of `parent` per R1.3.9.d.
    /// Meta companions are nodes whose lifecycle is bound to the parent's
    /// in TEARDOWN ordering: when `parent` tears down, `companion` tears
    /// down first.
    ///
    /// Use this for inspection / audit / sidecar nodes that subscribe to
    /// parent state — without the ordering, the companion could observe
    /// the parent mid-destruction and emit garbage.
    ///
    /// Idempotent on duplicate registration of the same companion.
    ///
    /// # Lifecycle constraint
    ///
    /// Intended for **setup-time** wiring — call this before `parent` or
    /// `companion` enters a wave. Mid-wave registration (especially during
    /// a teardown cascade in flight) is implementation-defined: the new
    /// edge takes effect on the *next* wave. Adding a companion to a
    /// torn-down parent silently no-ops (the parent will not tear down
    /// again). For dynamic companion attachment with deterministic
    /// ordering, prefer constructing the wiring before subscribers exist.
    ///
    /// # Panics
    ///
    /// Panics if either node id is unknown, or if `parent == companion`
    /// (a node cannot be its own meta companion — would loop on TEARDOWN).
    pub fn add_meta_companion(&self, parent: NodeId, companion: NodeId) {
        assert!(parent != companion, "node cannot be its own meta companion");
        let mut s = self.lock_state();
        assert!(s.nodes.contains_key(&parent), "unknown parent {parent:?}");
        assert!(
            s.nodes.contains_key(&companion),
            "unknown companion {companion:?}"
        );
        let metas = &mut s.require_node_mut(parent).meta_companions;
        if !metas.contains(&companion) {
            metas.push(companion);
        }
    }
}

// -----------------------------------------------------------------------
// INVALIDATE — cache clear + downstream cascade
// -----------------------------------------------------------------------

impl Core {
    /// Clear `node_id`'s cache and cascade `[INVALIDATE]` to downstream
    /// dependents per canonical spec §1.4.
    ///
    /// Semantics:
    /// - **Never-populated case (R1.4 line 197):** if `cache == NO_HANDLE`,
    ///   the call is a no-op — no cache to clear, no INVALIDATE emitted.
    ///   This naturally provides idempotency within a wave: once a node has
    ///   been invalidated this wave (cache = NO_HANDLE), a second invalidate
    ///   on the same node does nothing.
    /// - **Cache clear (immediate):** the node's cached handle is dropped
    ///   (refcount released), `cache` becomes `NO_HANDLE`. State nodes
    ///   keep `has_fired_once` per spec — INVALIDATE is not a re-gating
    ///   event (the next emission to a previously-fired state still does
    ///   not re-trigger the first-run gate; that's a resubscribable-terminal
    ///   lifecycle concern, separate slice).
    /// - **Wire emission (tier 4):** `[INVALIDATE]` is queued via the
    ///   normal pause-aware notify path. Buffers while paused, flushes
    ///   immediately otherwise.
    /// - **Downstream cascade:** for each child of this node, the child's
    ///   `dep_handles[idx_of_node]` is reset to `NO_HANDLE` (its previous
    ///   value referenced a now-released handle). The child is then
    ///   recursively invalidated (no-op if its cache was already
    ///   `NO_HANDLE`). This re-closes the child's first-run gate — fn
    ///   won't fire again until the upstream re-emits a value.
    ///
    /// Wraps in a fresh wave when called from outside a wave, so
    /// notifications flush at the natural wave boundary.
    ///
    /// # Panics
    ///
    /// Panics if `node_id` is unknown, consistent with `emit` / `pause`.
    pub fn invalidate(&self, node_id: NodeId) {
        {
            let s = self.lock_state();
            assert!(s.nodes.contains_key(&node_id), "unknown node {node_id:?}");
        }
        // INVALIDATE cascade follows `s.children` (in-partition by union-
        // find construction). Slice Y1 / Phase E.
        self.run_wave_for(node_id, |this| {
            let mut s = this.lock_state();
            this.invalidate_inner(&mut s, node_id);
        });
    }

    /// Invalidate or defer to wave-end on partition order violation.
    /// For producer-pattern operator sinks.
    ///
    /// # Panics
    ///
    /// Panics if `node_id` is not registered in this Core.
    pub fn invalidate_or_defer(&self, node_id: NodeId) {
        {
            let s = self.lock_state();
            assert!(s.nodes.contains_key(&node_id), "unknown node {node_id:?}");
        }
        let result = self.try_run_wave_for(node_id, |this| {
            let mut s = this.lock_state();
            this.invalidate_inner(&mut s, node_id);
        });
        if result.is_err() {
            self.push_deferred_producer_op(DeferredProducerOp::Callback(Box::new({
                let core = self.clone();
                move || {
                    core.invalidate(node_id);
                }
            })));
        }
    }

    /// Iterative invalidate cascade (Slice A-bigger, M1-close).
    ///
    /// The recursive shape was a depth-first cache-clear walk:
    ///   ```text
    ///   invalidate(n):
    ///     if cache(n) == NO_HANDLE: return  // already-invalidated guard
    ///     cache(n) = NO_HANDLE; release handle
    ///     queue Invalidate(n)
    ///     for child in children:
    ///       child.dep_handles[idx] = NO_HANDLE
    ///       invalidate(child)
    ///   ```
    /// Deep linear chains overflowed the OS thread stack. The work-queue
    /// rewrite has no ordering subtleties (unlike teardown's R1.3.9.d
    /// metas-first constraint) — Invalidate is a tier-4 broadcast where
    /// the never-populated / already-invalidated guard provides natural
    /// idempotency for diamond fan-in.
    fn invalidate_inner(&self, s: &mut CoreState, root: NodeId) {
        let mut work: Vec<NodeId> = vec![root];
        while let Some(node_id) = work.pop() {
            // Never-populated / already-invalidated: no-op (R1.4 idempotency).
            // Per R1.3.9.c never-populated case, OnInvalidate cleanup hook
            // also does NOT fire — natural fallout of skipping via the
            // cache==NO_HANDLE guard (we never reach the queue-push below).
            let old_handle = s.require_node(node_id).cache;
            if old_handle == NO_HANDLE {
                continue;
            }
            // Clear cache + release the handle's slot ownership.
            s.require_node_mut(node_id).cache = NO_HANDLE;
            self.binding.release_handle(old_handle);
            // Slice E2 (R1.3.9.b strict per D057 + D058 fire-at-cache-clear):
            // queue OnInvalidate cleanup hook for lock-released drain at
            // wave-end. The dedup set guarantees at-most-once-per-wave-per-
            // node firing even if a node re-populates mid-wave (via fn-fire
            // emit) and gets re-invalidated through a separate path. Pure
            // cache==NO_HANDLE idempotency (above) catches "still at
            // sentinel" only; the explicit set is the strict R1.3.9.b
            // reading.
            // Q-beyond Sub-slice 3 (D108, 2026-05-09):
            // `invalidate_hooks_fired_this_wave` and
            // `deferred_cleanup_hooks` both live on per-thread WaveState.
            // Single borrow handles the dedup-insert and (on first
            // insertion) the cleanup-hook push.
            crate::batch::with_wave_state(|ws| {
                if ws.invalidate_hooks_fired_this_wave.insert(node_id) {
                    ws.deferred_cleanup_hooks
                        .push((node_id, CleanupTrigger::OnInvalidate));
                }
            });
            // Wire emission. Pause-aware via queue_notify.
            self.queue_notify(s, node_id, Message::Invalidate);
            // Cascade: for each child, clear the dep record's prev_data
            // referencing this node and push child onto the work queue.
            let child_ids: Vec<NodeId> = s
                .children
                .get(&node_id)
                .map(|c| c.iter().copied().collect())
                .unwrap_or_default();
            for child_id in child_ids {
                let dep_idx = s.require_node(child_id).dep_index_of(node_id);
                if let Some(idx) = dep_idx {
                    // Reset the child's dep record — the handle was just
                    // released. Subsequent first-run-gate checks see
                    // sentinel and re-close.
                    //
                    // Snapshot prev_data + data_batch retains for deferred
                    // release, then clear the record. Two-phase to satisfy
                    // the borrow checker (nodes + deferred_handle_releases
                    // are separate CoreState fields).
                    let (old_prev, batch_hs): (HandleId, SmallVec<[HandleId; 1]>) = {
                        let dr = &s.require_node(child_id).dep_records[idx];
                        (dr.prev_data, dr.data_batch.clone())
                    };
                    {
                        // Q-beyond Sub-slice 1 (D108, 2026-05-09):
                        // deferred_handle_releases moved to per-thread
                        // WaveState thread_local. State lock held; the
                        // thread_local borrow is independent.
                        crate::batch::with_wave_state(|ws| {
                            if old_prev != NO_HANDLE {
                                ws.deferred_handle_releases.push(old_prev);
                            }
                            for h in batch_hs {
                                ws.deferred_handle_releases.push(h);
                            }
                        });
                    }
                    let child_rec = s.require_node_mut(child_id);
                    child_rec.dep_records[idx].prev_data = NO_HANDLE;
                    child_rec.dep_records[idx].data_batch.clear();
                    // §10.13 perf (D047): clear received_mask bit so
                    // has_sentinel_deps() re-closes the first-run gate.
                    if idx < 64 {
                        child_rec.received_mask &= !(1u64 << idx);
                    }
                    work.push(child_id);
                }
            }
        }
    }
}

// -----------------------------------------------------------------------
// PAUSE / RESUME — multi-pauser lockset + replay buffer
// -----------------------------------------------------------------------

/// Reported back from [`Core::resume`] when the final lock releases.
///
/// `replayed` is the number of tier-3/tier-4 messages dispatched to
/// subscribers as part of the drain. `dropped` is the number of messages
/// that fell out the front of the buffer due to the Core-global
/// `pause_buffer_cap` while this pause cycle was active. A non-zero
/// `dropped` indicates a controller held the lock long enough to overflow
/// the cap; the binding may want to surface a warning or error.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ResumeReport {
    pub replayed: u32,
    pub dropped: u32,
}

impl Core {
    /// Acquire a pause lock on `node_id`. The first lock transitions the
    /// node from `Active` to `Paused`; further locks add to the lockset.
    /// While paused, tier-3 (DATA/RESOLVED) and tier-4 (INVALIDATE) outgoing
    /// messages buffer in the node's pause buffer; other tiers flush
    /// immediately.
    ///
    /// Re-acquiring the same `lock_id` is an idempotent no-op (matches TS
    /// convention, R1.2.6 silent on the case).
    pub fn pause(&self, node_id: NodeId, lock_id: LockId) -> Result<(), PauseError> {
        let mut s = self.lock_state();
        let rec = s
            .nodes
            .get_mut(&node_id)
            .ok_or(PauseError::UnknownNode(node_id))?;
        // QA A5 (2026-05-07): terminated nodes can't be re-paused. Without
        // this check, a stale pause-controller calling pause() on an
        // already-terminated node would re-arm `pause_state` to Paused.
        // The terminate_node path collapses pause_state → Active and
        // drains the buffer (A3-related), but doesn't gate subsequent
        // pause() calls. Treat as idempotent no-op (consistent with how
        // emit/complete/error early-return on terminal).
        if rec.terminal.is_some() {
            return Ok(());
        }
        // Slice F audit close (2026-05-07): `PausableMode::Off` means the
        // dispatcher ignores PAUSE for this node — tier-3 flushes
        // immediately, fn fires immediately. Treat the call as a successful
        // no-op so callers don't need to special-case.
        if rec.pausable == PausableMode::Off {
            return Ok(());
        }
        rec.pause_state.add_lock(lock_id);
        Ok(())
    }

    /// Release a pause lock on `node_id`. If the lockset becomes empty, the
    /// node transitions back to `Active` and the buffered messages are
    /// dispatched to subscribers in arrival order. Returns a [`ResumeReport`]
    /// when the final lock released; `None` if the lockset is still
    /// non-empty (further locks held).
    ///
    /// Releasing an unknown `lock_id` (or releasing on an already-Active
    /// node) is an idempotent no-op returning `None`.
    pub fn resume(
        &self,
        node_id: NodeId,
        lock_id: LockId,
    ) -> Result<Option<ResumeReport>, PauseError> {
        // Phase 1 (lock-held): collect drained buffer + pending-wave flag +
        // sink Arcs. For default-mode nodes whose `pending_wave` was set
        // during pause, schedule a single fn-fire by adding to
        // `pending_fires` BEFORE we exit the lock — the wave engine picks
        // it up on the next drain tick.
        let (sinks, messages, dropped, pending_wave_for_default) = {
            let mut s = self.lock_state();
            let rec = s
                .nodes
                .get_mut(&node_id)
                .ok_or(PauseError::UnknownNode(node_id))?;
            // For Off mode, pause/resume are no-ops by construction.
            if rec.pausable == PausableMode::Off {
                return Ok(None);
            }
            let was_default_mode = rec.pausable == PausableMode::Default;
            // Capture pending_wave BEFORE remove_lock collapses the state.
            let pending_wave = if was_default_mode {
                rec.pause_state.take_pending_wave()
            } else {
                false
            };
            let Some((buffer, dropped)) = rec.pause_state.remove_lock(lock_id) else {
                // Not the final-resume — restore the pending_wave flag we
                // tentatively cleared, since we're not transitioning to
                // Active yet.
                if pending_wave {
                    rec.pause_state.mark_pending_wave();
                }
                return Ok(None);
            };
            let sinks: Vec<Sink> = rec.subscribers.values().cloned().collect();
            let messages: Vec<Message> = buffer.into_iter().collect();
            // Default-mode pending-wave handling: schedule the fn-fire so
            // the wave engine consolidates the pause-window dep deliveries
            // into one fn execution. State nodes don't fire fn (no
            // `pending_fires` membership has effect for them).
            //
            // Q-beyond Sub-slice 2 (D108, 2026-05-09): pending_fires lives
            // on per-thread WaveState.
            if pending_wave && was_default_mode {
                crate::batch::with_wave_state(|ws| {
                    ws.pending_fires.insert(node_id);
                });
            }
            (sinks, messages, dropped, pending_wave && was_default_mode)
        };
        let replayed = u32::try_from(messages.len()).unwrap_or(u32::MAX);

        // Phase 2 (lock-released): fire sinks for ResumeAll-buffered
        // messages. Default-mode resume produces no buffered replay (the
        // consolidated fn-fire produces fresh wave traffic via the standard
        // commit_emission path).
        if !messages.is_empty() {
            for sink in &sinks {
                sink(&messages);
            }
            // Phase 3: balance the retain_handle calls done at buffer-push
            // time — sinks observe values but don't own refcount shares.
            for msg in &messages {
                if let Some(h) = msg.payload_handle() {
                    self.binding.release_handle(h);
                }
            }
        }

        // Phase 4 (default-mode): drain the consolidated fn-fire scheduled
        // in Phase 1. `run_wave_for(node_id)` acquires the partitions
        // touched from `node_id` (Slice Y1 / Phase E) and runs the standard
        // drain pipeline; the new fn-fire emerges as a normal wave's worth
        // of messages to subscribers.
        if pending_wave_for_default {
            self.run_wave_for(node_id, |_this| {
                // The pending_fires entry was pushed in Phase 1 under the
                // lock. run_wave's drain picks it up.
            });
        }
        Ok(Some(ResumeReport { replayed, dropped }))
    }

    /// True if the node currently holds at least one pause lock.
    #[must_use]
    pub fn is_paused(&self, node_id: NodeId) -> bool {
        self.state
            .lock()
            .require_node(node_id)
            .pause_state
            .is_paused()
    }

    /// Number of pause locks currently held on `node_id`. `0` if Active.
    #[must_use]
    pub fn pause_lock_count(&self, node_id: NodeId) -> usize {
        self.state
            .lock()
            .require_node(node_id)
            .pause_state
            .lock_count()
    }

    /// Test helper: whether `node_id` currently holds the given `lock_id`.
    #[must_use]
    pub fn holds_pause_lock(&self, node_id: NodeId, lock_id: LockId) -> bool {
        self.state
            .lock()
            .require_node(node_id)
            .pause_state
            .contains_lock(lock_id)
    }
}

// -----------------------------------------------------------------------
// set_deps — atomic dep mutation
// -----------------------------------------------------------------------

/// Errors returnable by [`Core::set_deps`].
///
/// Per `~/src/graphrefly-ts/docs/research/rewire-design-notes.md` and the
/// Phase 13.8 Q1 lock:
/// - `SelfDependency` — `n in newDeps` (self-loops are pathological without
///   explicit fixed-point semantics, which GraphReFly does not provide).
/// - `WouldCreateCycle { path }` — adding the new edge would create a cycle.
///   The `path` field reports the offending dep chain for debuggability.
/// - `UnknownNode` / `NotComputeNode` — invariant violations from the caller.
/// - `TerminalNode` — `n` itself has emitted COMPLETE/ERROR; rewiring a
///   terminal stream is a category error (terminal is one-shot at this
///   layer; recovery is the resubscribable path on a fresh subscribe).
/// - `TerminalDep` — a newly-added dep is terminal AND not resubscribable.
///   Resubscribable terminal deps are accepted because the subscribe path
///   resets their lifecycle. Non-resubscribable terminal deps would deliver
///   their already-emitted terminal directly to `n`'s `dep_terminals` slot,
///   which is rarely intended.
#[derive(Error, Debug, Clone, PartialEq)]
pub enum SetDepsError {
    /// `n` appeared in `new_deps` (self-loop rejection).
    #[error("set_deps({n:?}, ...): self-dependency rejected (n appeared in new_deps)")]
    SelfDependency { n: NodeId },

    /// Adding the new dep would create a cycle. `path` is the chain
    /// `[added_dep, ..., n]` reachable via existing deps.
    #[error(
        "set_deps({n:?}, ...): cycle would form via path {path:?} \
         (adding {added_dep:?} → {n:?} closes the loop)"
    )]
    WouldCreateCycle {
        n: NodeId,
        added_dep: NodeId,
        path: Vec<NodeId>,
    },

    #[error("set_deps: unknown node {0:?}")]
    UnknownNode(NodeId),

    #[error("set_deps: node {0:?} is not a compute node (state nodes have no deps)")]
    NotComputeNode(NodeId),

    /// `n` itself has terminated (COMPLETE / ERROR). Rewiring a terminal node
    /// is rejected — the stream has ended at this layer. To recover, mark
    /// the node resubscribable before terminate; a fresh subscribe will then
    /// reset its lifecycle.
    #[error("set_deps({n:?}, ...): node has already terminated; cannot rewire a terminal node")]
    TerminalNode { n: NodeId },

    /// A newly-added dep is terminal AND non-resubscribable. Per Phase 13.8
    /// Q1, this is rejected; resubscribable terminal deps are allowed
    /// because the subscribe path resets them when activated. Already-present
    /// terminal deps are unaffected (their terminal status was accepted at
    /// the time they terminated).
    #[error(
        "set_deps({n:?}, ...): added dep {dep:?} is terminal and not resubscribable; \
         either mark it resubscribable before terminate, or remove the dep from new_deps"
    )]
    TerminalDep { n: NodeId, dep: NodeId },

    /// `n` itself is currently mid-fire — a user fn for `n` re-entered Core
    /// via `set_deps(n, ...)` from inside `n`'s own `invoke_fn` /
    /// `project_each` / `predicate_each` / etc. Phase 1 of the dispatcher
    /// snapshotted `dep_handles` BEFORE the lock-released callback; the
    /// callback returning a `tracked` set indexed against THAT ordering
    /// would corrupt indices if the rewire re-orders deps mid-fire.
    /// Rejected to preserve the dynamic-tracked-indices invariant (D1).
    ///
    /// Workaround: schedule the rewire from a different node's fn (via
    /// `Core::emit` on a state node and observing the emit downstream),
    /// or perform the rewire after the wave completes (e.g. from a sink
    /// callback that is itself outside any fn-fire scope).
    ///
    /// Slice F (2026-05-07) — A6.
    #[error(
        "set_deps({n:?}, ...): rejected — node {n:?} is currently mid-fire \
         (set_deps from inside the firing node's own fn would corrupt the \
         Dynamic `tracked` indices snapshot taken before invoke_fn). \
         Schedule the rewire outside this fire scope."
    )]
    ReentrantOnFiringNode { n: NodeId },

    /// `set_deps(n, ...)` would trigger a partition migration (union or
    /// split in the per-subgraph union-find registry) that affects the
    /// partition of a node currently mid-fire on this thread. Distinct
    /// from [`Self::ReentrantOnFiringNode`]: that variant rejects
    /// `set_deps(n, ...)` where `n` itself is firing; this variant
    /// rejects `set_deps(n, ...)` on some OTHER node whose union/split
    /// shifts a firing node's partition root mid-wave.
    ///
    /// Why this matters: Y1's wave engine holds an
    /// [`Arc<crate::subgraph::SubgraphLockBox>`] for the firing node's
    /// partition for the wave's duration. A union mid-wave swaps the
    /// box-identity for one of the two affected partitions; a split
    /// (Y1+ post-Phase-F) extracts a fresh box for the orphan side.
    /// Either way the held Arc would diverge from the registry's
    /// current root for that partition, so the wave would lose
    /// serialization against the box's true partition mid-flight.
    ///
    /// Per [`SESSION-rust-port-d3-per-subgraph-parallelism.md`](https://github.com/graphrefly/graphrefly-ts/blob/main/archive/docs/SESSION-rust-port-d3-per-subgraph-parallelism.md)
    /// Q3 = (a-strict): mid-wave migration is rejected at edge-mutation
    /// time. If a real consumer surfaces pressure to support mid-wave
    /// migration, lift via state-migration logic in a follow-up — but
    /// the v1 contract is "the partition a wave runs in cannot change
    /// shape mid-flight."
    ///
    /// `n` is the node whose `set_deps` was rejected; `firing` is the
    /// concretely-identified firing node whose partition would be
    /// migrated. Workaround: schedule the rewire outside the wave
    /// (e.g. emit a state-change that triggers `set_deps` from a sink
    /// callback running post-flush).
    ///
    /// Slice Y1 (D3 / D091, 2026-05-08).
    #[error(
        "set_deps({n:?}, ...): rejected — would migrate the partition of \
         currently-firing node {firing:?} mid-wave (union/split during \
         fire would invalidate the held wave_owner Arc). Schedule the \
         rewire outside the wave."
    )]
    PartitionMigrationDuringFire { n: NodeId, firing: NodeId },
}

impl Core {
    /// Atomic dep mutation — change a node's upstream deps without TEARDOWN
    /// cascading and without losing cache.
    ///
    /// Per the TLA+-verified design at
    /// `~/src/graphrefly-ts/docs/research/wave_protocol_rewire.tla`
    /// (35,950 distinct states, all 7 invariants clean):
    ///
    /// - Removed deps: clear dirtyMask bit, drain pending queue, drop DepRecord.
    /// - Added deps: SENTINEL prevData; push-on-subscribe if added dep has cached DATA.
    /// - Preserved: `firstRunPassed`, `pauseLocks`, `pauseBuffer`, `cache` (ROM/RAM).
    /// - Status auto-settles if dirtyMask becomes empty.
    /// - Idempotent on `new_deps == current deps`.
    /// - Self-rewire `n ∈ new_deps` rejected (`SelfDependency`).
    /// - Cycles rejected (`WouldCreateCycle`).
    /// - Allowed mid-wave + while paused.
    /// - Phase 13.8 Q1: terminal `n` rejected (`TerminalNode`); newly-added
    ///   terminal non-resubscribable deps rejected (`TerminalDep`).
    ///
    /// The body is a single atomic dep-mutation transaction with several
    /// discrete validation stages. Splitting would require passing a
    /// partially-mutable CoreState across helpers, and the transaction's
    /// locality is what makes the F1 refcount-leak collection work.
    #[allow(clippy::too_many_lines)]
    pub fn set_deps(&self, n: NodeId, new_deps: &[NodeId]) -> Result<(), SetDepsError> {
        let mut s = self.lock_state();
        // Validate node exists and is compute. Read-once via the helper so
        // subsequent code can use `require_node(n)` without re-checking.
        let (is_state, is_producer, is_terminal) = {
            let rec = s.nodes.get(&n).ok_or(SetDepsError::UnknownNode(n))?;
            (rec.is_state(), rec.is_producer(), rec.terminal.is_some())
        };
        if is_state || is_producer {
            // State and Producer nodes have no declared deps — set_deps
            // is meaningless. Producer nodes manage their own subscriptions
            // through the binding's ProducerCtx; mutating their (empty)
            // dep set would not affect that.
            return Err(SetDepsError::NotComputeNode(n));
        }
        // Reject if `n` itself is terminal (Phase 13.8 Q1: terminal nodes
        // cannot be rewired; recovery is via resubscribable subscribe).
        if is_terminal {
            return Err(SetDepsError::TerminalNode { n });
        }
        // A6 reentrancy guard (Slice F, 2026-05-07): reject if `n` is
        // currently mid-fire on the wave-owner thread. Closes the D1 hazard
        // where `Phase 1` snapshotted `dep_handles` against pre-rewire dep
        // ordering and `Phase 3` would store the returned `tracked` indices
        // against post-rewire ordering. Same-thread re-entry is the only
        // path that matters — cross-thread emits already block on
        // `wave_owner` per the M1 design.
        // /qa F2 reverted (2026-05-10): currently_firing lives on
        // CoreState (per-Core, cross-thread visible). The D1 reentrance
        // check requires the same-thread visibility (a fn re-entering
        // set_deps on its own firing node), and the P13 cross-thread
        // check requires cross-thread visibility (Thread B's set_deps
        // observing Thread A's firing pushes during A's lock-released
        // invoke_fn). Per-Core placement on shared CoreState delivers
        // both. Read under the already-held state lock.
        if s.currently_firing.contains(&n) {
            return Err(SetDepsError::ReentrantOnFiringNode { n });
        }
        // Self-rewire rejection.
        if new_deps.contains(&n) {
            return Err(SetDepsError::SelfDependency { n });
        }
        // Validate all new deps exist.
        for &d in new_deps {
            if !s.nodes.contains_key(&d) {
                return Err(SetDepsError::UnknownNode(d));
            }
        }
        // Cycle detection: data flows parent → child via the `children` map.
        // Adding edge `d → n` (d becomes a dep of n) creates a cycle iff
        // `d` is already reachable from `n` via existing data-flow edges
        // (so `n → ... → d` exists, and the new `d → n` closes the loop).
        // DFS from `n` along `children` edges, looking for each added dep.
        let current_deps: HashSet<NodeId> = s.require_node(n).dep_ids().collect();
        let new_deps_set: HashSet<NodeId> = new_deps.iter().copied().collect();
        let added: HashSet<NodeId> = new_deps_set.difference(&current_deps).copied().collect();
        for &d in &added {
            if let Some(path) = self.path_from_to(&s, n, d) {
                return Err(SetDepsError::WouldCreateCycle {
                    n,
                    added_dep: d,
                    path,
                });
            }
        }
        // Phase 13.8 Q1: reject newly-added deps that are terminal AND not
        // resubscribable. Resubscribable terminal deps are allowed — the
        // subscribe path resets their lifecycle when something activates
        // them. Already-present (kept) deps are unaffected; their terminal
        // status was accepted at the time they terminated.
        for &d in &added {
            let dep_rec = s.require_node(d);
            if dep_rec.terminal.is_some() && !dep_rec.resubscribable {
                return Err(SetDepsError::TerminalDep { n, dep: d });
            }
        }
        // Compute `removed` early (Phase F: needs to be available for P13
        // split-case widening below). Idempotent fast-path moved below the
        // P13 check accordingly.
        let removed: HashSet<NodeId> = current_deps.difference(&new_deps_set).copied().collect();

        // Slice Y1 (D3 / D091 — P13, 2026-05-08): reject mid-wave set_deps
        // that would shift a currently-firing node's partition root.
        // Distinct from `ReentrantOnFiringNode` (same-node case, line above).
        // Holds the registry lock briefly under the state lock per the
        // P12-fix lock-discipline invariant `state lock → registry mutex`.
        //
        // **Two cases:**
        // 1. **Union (Phase D)** — adding a cross-partition dep merges two
        //    components. Both pre-merge components are affected (the
        //    smaller-rank loser's box is dropped, its members migrate to
        //    the winner's root).
        // 2. **Split (Phase F, 2026-05-09)** — removing an edge whose
        //    removal disconnects the dep graph splits one component into
        //    two. The pre-split component is affected (every member
        //    re-unions; the orphan side gets a fresh `SubgraphLockBox`
        //    while the keep side preserves the original Arc).
        //
        // Either case migrates the partition root (and box-identity) for
        // affected nodes mid-wave; if any node currently firing on this
        // thread is in an affected partition, the wave's held
        // `Arc<SubgraphLockBox>` would diverge from the registry's new
        // canonical box. Q3 = (a-strict) per the D3 design lock rejects
        // both cases at edge-mutation time.
        // /qa F2 reverted (2026-05-10): currently_firing lives on
        // CoreState (cross-thread visible — load-bearing for this P13
        // partition-migration check, which detects cross-thread set_deps
        // calls during another thread's lock-released invoke_fn).
        // Snapshot under the already-held state lock so the registry
        // mutex acquire below doesn't need to also hold the snapshot
        // borrow.
        let currently_firing_snapshot: Vec<NodeId> = s.currently_firing.clone();
        if !currently_firing_snapshot.is_empty() && (!added.is_empty() || !removed.is_empty()) {
            let mut reg = self.registry.lock();
            // Snapshot firing nodes' partitions. `partition_of` is mutating
            // (path compression) but partition IDENTITY is stable across
            // reads (only `union_nodes` / `split_partition` mutate roots).
            let firing_with_partition: Vec<(NodeId, crate::subgraph::SubgraphId)> =
                currently_firing_snapshot
                    .iter()
                    .filter_map(|&f| reg.partition_of(f).map(|p| (f, p)))
                    .collect();
            if !firing_with_partition.is_empty() {
                let part_n = reg.partition_of(n);
                // Case 1 (union): for each added dep, check cross-partition merge.
                for &added_dep in &added {
                    let part_added = reg.partition_of(added_dep);
                    if part_n == part_added {
                        continue; // same-partition add is a no-op in union-find
                    }
                    let affected = [part_n, part_added];
                    if let Some(&(firing, _)) = firing_with_partition
                        .iter()
                        .find(|(_, p)| affected.contains(&Some(*p)))
                    {
                        return Err(SetDepsError::PartitionMigrationDuringFire { n, firing });
                    }
                }
                // Case 2 (split): for each removed dep, simulate undirected
                // walk from `removed_dep` skipping the would-be-removed edge
                // (`removed_dep → n`); if `n` is unreachable, removal would
                // disconnect — split — affecting all nodes in that
                // partition. Since dep edges are within a single partition
                // by construction (union-find merges on edge add), every
                // node currently in the partition is affected.
                //
                // QA-fix #4 (2026-05-09): pass `added_edges` as `extra_edges`
                // so a `set_deps` that simultaneously REMOVES one edge AND
                // ADDS another path isn't falsely rejected. Without this,
                // the pre-mutation walk doesn't see the would-be-added
                // edges and reports disconnect even when the net change
                // preserves connectivity.
                let added_edges: Vec<(NodeId, NodeId)> = added.iter().map(|&a| (a, n)).collect();
                for &removed_dep in &removed {
                    let part_removed = reg.partition_of(removed_dep);
                    let visited = walk_undirected_dep_graph(
                        &s,
                        removed_dep,
                        Some((removed_dep, n)),
                        &added_edges,
                    );
                    let would_disconnect = !visited.contains(&n);
                    if would_disconnect {
                        if let Some(&(firing, _)) = firing_with_partition
                            .iter()
                            .find(|(_, p)| Some(*p) == part_removed)
                        {
                            return Err(SetDepsError::PartitionMigrationDuringFire { n, firing });
                        }
                    }
                }
            }
        }
        // Idempotent fast-path. Now safe to short-circuit since the P13
        // check above already considered both `added` and `removed`.
        if added.is_empty() && removed.is_empty() {
            return Ok(());
        }

        // Snapshot old deps (ordered) for topology event, before mutation.
        let old_deps_vec: Vec<NodeId> = s.require_node(n).dep_ids_vec();

        // Carry out the rewire atomically.
        // 1. Build new dep_records, preserving DepRecord state for kept deps.
        let new_deps_vec: Vec<NodeId> = new_deps.to_vec();
        //
        // Refcount discipline (F1 audit fix): each `Some(TerminalKind::Error(h))`
        // slot owns a refcount share retained at `terminate_node` time. When a
        // dep is REMOVED, its slot is dropped — the corresponding handle's
        // share must be released here, otherwise it leaks until Core drop.
        // Also release data_batch retains for removed deps.
        let (new_dep_records, removed_handles): (Vec<DepRecord>, Vec<HandleId>) = {
            let rec = s.require_node(n);
            // Index old dep_records by NodeId for O(1) lookup of kept deps.
            let old_by_node: HashMap<NodeId, &DepRecord> =
                rec.dep_records.iter().map(|dr| (dr.node, dr)).collect();
            let new_records: Vec<DepRecord> = new_deps_vec
                .iter()
                .map(|&d| {
                    if let Some(old) = old_by_node.get(&d) {
                        // Kept dep: preserve all state (prev_data, data_batch,
                        // terminal, wave flags). Subscriptions stay live.
                        DepRecord {
                            node: d,
                            prev_data: old.prev_data,
                            dirty: old.dirty,
                            involved_this_wave: old.involved_this_wave,
                            data_batch: old.data_batch.clone(),
                            terminal: old.terminal,
                        }
                    } else {
                        // Added dep: fresh sentinel record.
                        DepRecord::new(d)
                    }
                })
                .collect();
            // Collect handles to release from REMOVED dep records.
            let mut to_release: Vec<HandleId> = Vec::new();
            for d in &removed {
                if let Some(old) = old_by_node.get(d) {
                    if let Some(TerminalKind::Error(h)) = old.terminal {
                        to_release.push(h);
                    }
                    // Release data_batch retains for removed deps.
                    for &h in &old.data_batch {
                        to_release.push(h);
                    }
                }
            }
            (new_records, to_release)
        };
        // Clear dirtyMask bit by re-emitting the wave-bookkeeping: we don't
        // currently model a per-dep dirtyMask explicitly (we use the boolean
        // `dirty` flag at node level). Removing a dep's entry from the implicit
        // mask is therefore implicit — by removing the dep, future emissions
        // from it can't re-arm the bit. The per-dep `involved_this_wave` flag
        // stays wave-scoped and gets cleared at wave end. The setDeps action
        // itself does NOT change the dirty boolean unless all deps are cleared;
        // in that case we settle.
        // Slice E2 (D067): on a dynamic node that had previously fired its
        // fn, capture `has_fired_once` BEFORE the reset so we can fire
        // `OnRerun` cleanup lock-released after `drop(s)` below. Without
        // this, the next `fire_regular` Phase 1 would capture
        // `has_fired_once = false`, causing Phase 1.5 to skip OnRerun —
        // silently dropping the prior activation's cleanup closure when
        // the next `invoke_fn` overwrites `current_cleanup`. Per spec
        // R2.4.5, `set_deps` does NOT end the activation cycle
        // (subscribe→unsubscribe is the cycle boundary), so OnRerun must
        // fire on every re-fire including post-set_deps.
        // §10 perf (D047): compute new topo_rank before the mutable
        // borrow on rec, since we need to read other nodes' depths.
        let new_topo_rank = if new_deps_vec.is_empty() {
            0
        } else {
            new_deps_vec
                .iter()
                .filter(|&&d| d != n)
                .filter_map(|&d| s.nodes.get(&d).map(|r| r.topo_rank))
                .max()
                .unwrap_or(0)
                .saturating_add(1)
        };
        let fire_set_deps_on_rerun;
        {
            let rec = s.require_node_mut(n);
            fire_set_deps_on_rerun = rec.is_dynamic && rec.has_fired_once;
            rec.dep_records = new_dep_records;
            rec.topo_rank = new_topo_rank;
            // §10.13 perf (D047): recompute received_mask from new dep_records.
            rec.received_mask = 0;
            for (i, dr) in rec.dep_records.iter().enumerate() {
                if i < 64 && (dr.prev_data != NO_HANDLE || !dr.data_batch.is_empty()) {
                    rec.received_mask |= 1u64 << i;
                }
            }
            // Re-derive `tracked` for static derived: all indices.
            // For dynamic: clear `tracked` AND reset `has_fired_once` so the
            // next dep delivery satisfies the first-fire branch in
            // `deliver_data_to_consumer` (`!has_fired_once || tracked.contains(...)`).
            // Without resetting `has_fired_once`, the cleared `tracked` blocks
            // every future fire — fn never re-runs and the dynamic node sits
            // on stale cache derived from the old dep set. The next fire
            // re-runs fn unconditionally; fn's returned `tracked` then
            // repopulates `rec.tracked` and normal selective-deps semantics
            // resume from the next dep update onward.
            if rec.is_dynamic {
                rec.tracked.clear();
                rec.has_fired_once = false;
            } else {
                // Derived (static) and Operator track all deps.
                rec.tracked = (0..new_deps_vec.len()).collect();
            }
        }

        // 2. Update inverted-edge map (children).
        for &removed_dep in &removed {
            if let Some(set) = s.children.get_mut(&removed_dep) {
                set.remove(&n);
            }
        }
        for &added_dep in &added {
            s.children.entry(added_dep).or_default().insert(n);
        }

        // 3. Push-on-subscribe for added deps with cached DATA. Wraps in a
        // wave so any downstream propagation runs cleanly. We capture only
        // the LIST of added deps (not their cache values) because the cache
        // can change between releasing the validation lock and the wave's
        // re-acquisition — see the P2 race fix below.
        //
        // P2 (Slice A close /qa) — between `drop(s)` and `run_wave`'s
        // closure re-acquiring the lock, a concurrent thread could
        // invalidate one of the added deps, releasing its cache handle. A
        // pre-snapshot of `(added_dep, cache)` pairs would then carry a
        // dangling HandleId into `deliver_data_to_consumer`. The fix is to
        // re-read each added dep's `cache` INSIDE the closure (under the
        // freshly re-acquired state lock). The wave-owner re-entrant mutex
        // (Q2) blocks concurrent waves once we enter `run_wave`, so the
        // re-read sees a coherent post-validation state.
        let added_for_wave: Vec<NodeId> = added.iter().copied().collect();
        // Slice Y1 (D3 / D090 — P12 fix, 2026-05-08): maintain partition
        // membership BEFORE dropping the state lock so the registry can
        // never lag behind topology mutations as observed by concurrent
        // readers. Lock-order invariant `state lock → registry mutex`
        // (one-way; never registry → state) — see the matching block in
        // `Core::register` for the full rationale.
        //   - For each new edge: union the partitions of `n` and `added_dep`.
        //   - For each removed edge (Slice Y1 / Phase F, 2026-05-09):
        //     run undirected-dep-graph BFS from `removed_dep` over the
        //     POST-removal `s.children` + `dep_records`. If `n` is
        //     unreachable, the partition has split — gather the affected
        //     component nodes + intra-component edges, then call
        //     [`SubgraphRegistry::split_partition`] to migrate the orphan
        //     side onto a fresh `SubgraphLockBox`. Mid-fire splits would
        //     have been rejected at the P13 check above (Q3 = (a-strict)).
        {
            let mut reg = self.registry.lock();
            for &added_dep in &added {
                reg.union_nodes(n, added_dep);
            }
            for &removed_dep in &removed {
                // Post-removal walk — `s.children[removed_dep]` no longer
                // contains `n`, and `s.nodes[n].dep_records` no longer
                // contains `removed_dep`. No skip needed; no extra edges
                // (added edges are already applied to `s.children` and
                // `dep_records` by the time we reach this block).
                let visited = walk_undirected_dep_graph(&s, removed_dep, None, &[]);
                if visited.contains(&n) {
                    // Still connected via other dep edges — no split.
                    continue;
                }
                // Disconnected. `visited` is the keep-side (containing
                // `removed_dep`). Identify the original component, the
                // intra-component dep edges, and split.
                let original_root = reg.find(removed_dep);
                // Snapshot keys before iterating — `find` mutates via
                // path compression; iterating + mutating concurrently
                // would alias-borrow.
                let snapshot_keys: Vec<NodeId> = reg.registered_nodes();
                let component_nodes: Vec<NodeId> = snapshot_keys
                    .into_iter()
                    .filter(|&node| reg.find(node) == original_root)
                    .collect();
                let component_set: HashSet<NodeId> = component_nodes.iter().copied().collect();
                // Collect dep edges within the component (post-removal).
                // Edge convention: `(parent, child)` data-flow direction.
                let mut edges_in_component: Vec<(NodeId, NodeId)> = Vec::new();
                for &node in &component_nodes {
                    if let Some(rec) = s.nodes.get(&node) {
                        for d in rec.dep_records.iter().map(|r| r.node) {
                            if component_set.contains(&d) {
                                edges_in_component.push((d, node));
                            }
                        }
                    }
                }
                let keep_side_nodes: Vec<NodeId> = visited.iter().copied().collect();
                reg.split_partition(&component_nodes, &keep_side_nodes, &edges_in_component);
                // Marker call kept for symmetry with `union_nodes` — the
                // registry's `on_edge_removed` is itself a no-op (Phase F
                // moved the actual work into Core where the dep-graph
                // view is available).
                reg.on_edge_removed(n, removed_dep);
            }
        }
        // B3 (D117, 2026-05-10): when set_deps clears ALL deps mid-wave AND
        // n has a tier-1 (DIRTY) message already queued this wave with no
        // settle yet, push n to `pending_auto_resolve` so the drain-end
        // sweep emits a paired Resolved. Without this, subscribers observe
        // an unpaired DIRTY, violating R1.3.1.b two-phase push pairing.
        //
        // Outside any active wave the per-thread `pending_notify` is empty
        // (cleared at wave-end), so the predicate short-circuits and the
        // insert is a no-op. Inside a wave, the `pending_auto_resolve`
        // sweep at `drain_and_flush` end re-checks pending_notify and
        // routes through `queue_notify` (which handles paused-children
        // pause-buffer placement automatically).
        if new_deps_vec.is_empty() {
            // F6 (/qa 2026-05-10): walk pending_notify in arrival order
            // counting unpaired DIRTYs. Tier 4 INVALIDATE is NOT a
            // settle for two-phase pairing — it clears cache but does
            // not pair with a DIRTY. Pairs are DIRTY ↔ DATA / RESOLVED
            // (tier 3 value-class) or DIRTY ↔ COMPLETE / ERROR (tier 5
            // terminal). Multi-emit waves like `[DIRTY, RESOLVED, DIRTY]`
            // leave one trailing unpaired DIRTY that needs auto-Resolved.
            crate::batch::with_wave_state(|ws| {
                let needs_auto_resolve = ws.pending_notify.get(&n).is_some_and(|entry| {
                    let mut unpaired: i32 = 0;
                    for m in entry.iter_messages() {
                        match m {
                            crate::message::Message::Dirty => unpaired += 1,
                            crate::message::Message::Data(_)
                            | crate::message::Message::Resolved
                            | crate::message::Message::Complete
                            | crate::message::Message::Error(_)
                                if unpaired > 0 =>
                            {
                                unpaired -= 1;
                            }
                            // INVALIDATE / PAUSE / RESUME / TEARDOWN /
                            // START — not settles for two-phase pairing.
                            _ => {}
                        }
                    }
                    unpaired > 0
                });
                if needs_auto_resolve {
                    ws.pending_auto_resolve.insert(n);
                }
            });
        }
        // Drop the state lock before run_wave (which acquires its own) and
        // before crossing the binding boundary for the F1 refcount-fix
        // releases. Keeps the lock-discipline split (binding calls outside
        // the state lock) consistent with the rest of the dispatcher.
        drop(s);
        // Slice E2 (D067): fire OnRerun lock-released for dynamic nodes
        // that had previously fired. The cleanup closure cleans up
        // resources tied to the old dep shape before the next fn-fire
        // (triggered by added-dep push-on-subscribe below) registers a
        // fresh cleanup spec. Direct fire (NOT via deferred_cleanup_hooks)
        // because set_deps may NOT enter a wave (no added deps → no
        // run_wave below) — queueing the hook would orphan it until the
        // next unrelated wave drains.
        if fire_set_deps_on_rerun {
            self.binding.cleanup_for(n, CleanupTrigger::OnRerun);
        }
        // Fire topology event after lock is dropped.
        self.fire_topology_event(&crate::topology::TopologyEvent::DepsChanged {
            node: n,
            old_deps: old_deps_vec,
            new_deps: new_deps_vec.clone(),
        });
        if !added_for_wave.is_empty() {
            // Slice Y1 / Phase E: push-on-subscribe wave runs on `n`'s
            // touched partitions. Added deps are now unioned with `n`
            // (Phase C P12 fix moved registry mutation inside the state
            // lock), so any cascade through them stays in `n`'s partition
            // set as walked by `compute_touched_partitions`.
            self.run_wave_for(n, |this| {
                let mut s = this.lock_state();
                // Defensive: re-validate `n` still exists and isn't terminal.
                // A concurrent path could have terminated it between
                // validation and run_wave_for's partition-lock acquisition.
                if !s.nodes.contains_key(&n) || s.require_node(n).terminal.is_some() {
                    return;
                }
                for added_dep in &added_for_wave {
                    // Re-read cache under the wave-owner-held lock — this
                    // is the post-validation, post-concurrent-action
                    // snapshot. NO_HANDLE means the dep was invalidated
                    // concurrently; skip (no data to push).
                    let cache = match s.nodes.get(added_dep) {
                        Some(rec) => rec.cache,
                        None => continue, // dep deleted concurrently
                    };
                    if cache == NO_HANDLE {
                        continue;
                    }
                    let dep_idx = s.require_node(n).dep_index_of(*added_dep);
                    if let Some(idx) = dep_idx {
                        this.deliver_data_to_consumer(&mut s, n, idx, cache);
                    }
                }
            });
        }
        for h in removed_handles {
            self.binding.release_handle(h);
        }
        Ok(())
    }

    /// DFS from `from` along data-flow edges (children map) looking for `to`.
    /// Returns the path including endpoints, or `None` if unreachable. Used
    /// for cycle detection in [`Self::set_deps`].
    fn path_from_to(&self, s: &CoreState, from: NodeId, to: NodeId) -> Option<Vec<NodeId>> {
        if from == to {
            return Some(vec![from]);
        }
        let mut stack: Vec<(NodeId, Vec<NodeId>)> = vec![(from, vec![from])];
        let mut visited: HashSet<NodeId> = HashSet::new();
        while let Some((cur, path)) = stack.pop() {
            if !visited.insert(cur) {
                continue;
            }
            if cur == to {
                return Some(path);
            }
            if let Some(children) = s.children.get(&cur) {
                for &child in children {
                    let mut new_path = path.clone();
                    new_path.push(child);
                    stack.push((child, new_path));
                }
            }
        }
        None
    }
}

// CoreState helpers — kept on the inner struct so they're naturally scoped
// to the lock guard.
impl CoreState {
    fn alloc_node_id(&mut self) -> NodeId {
        let id = NodeId::new(self.next_node_id);
        self.next_node_id += 1;
        id
    }

    fn alloc_sub_id(&mut self) -> SubscriptionId {
        let id = SubscriptionId(self.next_subscription_id);
        self.next_subscription_id += 1;
        id
    }

    /// Clear wave-scoped flags and rotate per-dep batch data on every
    /// node. Run at the end of every wave (regular drain via `run_wave`,
    /// activation drain via `activate_derived`, and `BatchGuard::drop`'s
    /// drain). Centralized so a future wave-state field can't be missed
    /// at one of the cleanup sites.
    ///
    /// Per-dep rotation (R2.9.b / R1.3.6.b):
    /// - `prev_data` ← last element of `data_batch` (or unchanged if empty).
    ///   The last batch entry's retain transfers to `prev_data`; the old
    ///   `prev_data`'s retain is released. All earlier batch entries are
    ///   released.
    /// - `data_batch` cleared.
    /// - Per-dep `dirty` and `involved_this_wave` cleared.
    ///
    /// Handle releases are pushed to `deferred_handle_releases` for
    /// post-lock-drop release by the caller.
    pub(crate) fn clear_wave_state(&mut self, ws: &mut crate::batch::WaveState) {
        // Q-beyond Sub-slice 1 (D108, 2026-05-09): `pending_auto_resolve`
        // + `pending_pause_overflow` clears moved to
        // [`crate::batch::WaveState::clear_wave_state`]. The per-NodeRecord
        // rotation below pushes batch-handle and prev_data releases into
        // `ws.deferred_handle_releases` (was `cps.deferred_handle_releases`
        // pre-sub-slice-1, was `s.deferred_handle_releases` pre-Q2). Caller
        // borrows the WaveState thread_local; no lock-discipline rule
        // applies (state lock + thread_local borrow are independent).
        //
        // Q-beyond Sub-slice 3 (D108, 2026-05-09):
        // `invalidate_hooks_fired_this_wave` clear moved to
        // [`crate::batch::WaveState::clear_wave_state`]. The
        // `deferred_cleanup_hooks` invariant (NOT cleared here, drained
        // explicitly on success/panic paths) likewise moves with the
        // field.
        //
        // /qa F2 reverted (2026-05-10): `currently_firing` stays on
        // CoreState (per-Core, cross-thread visible — load-bearing for
        // P13). Defensive clear here mirrors the pre-sub-slice-3 safety
        // net (`FiringGuard`'s RAII push/pop is balanced even on panic;
        // a future code path that bypasses the guard would otherwise
        // leak a stale entry into the next wave).
        self.currently_firing.clear();
        //
        // Slice G tier3 emit tracking moved to per-partition state (Q3,
        // 2026-05-09); cleared by [`super::WaveOwnerGuard::drop`] on
        // outermost release for each partition the wave touched.
        for rec in self.nodes.values_mut() {
            rec.dirty = false;
            rec.involved_this_wave = false;
            for dr in &mut rec.dep_records {
                let batch_len = dr.data_batch.len();
                if batch_len > 0 {
                    // Release all batch entries EXCEPT the last — the last
                    // entry's retain transfers to prev_data.
                    for &h in &dr.data_batch[..batch_len - 1] {
                        ws.deferred_handle_releases.push(h);
                    }
                    // Release the OLD prev_data (its retain was from the
                    // previous wave's rotation or from initial delivery).
                    if dr.prev_data != NO_HANDLE {
                        ws.deferred_handle_releases.push(dr.prev_data);
                    }
                    // Rotate: last batch entry becomes new prev_data.
                    // Its retain carries over — no extra retain needed.
                    dr.prev_data = dr.data_batch[batch_len - 1];
                    dr.data_batch.clear();
                }
                dr.dirty = false;
                dr.involved_this_wave = false;
            }
        }
    }

    pub(crate) fn require_node(&self, id: NodeId) -> &NodeRecord {
        self.nodes
            .get(&id)
            .unwrap_or_else(|| panic!("unknown node {id:?}"))
    }

    pub(crate) fn require_node_mut(&mut self, id: NodeId) -> &mut NodeRecord {
        self.nodes
            .get_mut(&id)
            .unwrap_or_else(|| panic!("unknown node {id:?}"))
    }
}

/// Release every binding-side refcount share owned by this `CoreState`
/// when the last `Core` clone drops the inner Mutex.
///
/// Without this, every retained handle in `cache` / `terminal` Error /
/// `dep_terminals` Error / pause-buffer-payload would leak in the binding
/// registry until process exit. Production bindings (napi-rs, pyo3,
/// wasm-bindgen) all maintain handle-ref maps that grow unbounded without
/// this cleanup.
///
/// Safe to call during panic unwinding — `BindingBoundary::release_handle`
/// is the only call, and a panicking binding during cleanup would already
/// have been a problem in normal operation.
impl Drop for CoreState {
    fn drop(&mut self) {
        // Q-beyond Sub-slice 3 (D108, 2026-05-09): `deferred_flush_jobs`
        // moved to [`crate::batch::WaveState`]. The `Vec<Sink>` clones
        // drop naturally with the per-thread WaveState's lifetime; no
        // CoreState-side cleanup needed.
        // Q-beyond Sub-slice 1 (D108, 2026-05-09): `deferred_handle_releases`
        // and `wave_cache_snapshots` moved to per-thread WaveState
        // thread_local. By outermost-BatchGuard-drop discipline both fields
        // are empty by the time CoreState drops (BatchGuard owns a Core
        // clone, so Core can't drop while a BatchGuard is in flight). Any
        // thread that ran a wave on this Core drained on its own outermost
        // BatchGuard; cross-Core thread_local sharing is fine because each
        // wave drains its own retains.
        //
        // Q-beyond Sub-slice 2 (D108, 2026-05-09): `pending_fires` and
        // `pending_notify` likewise moved to per-thread WaveState. The
        // pre-Sub-slice-2 `pending_notify` walk here (drain + release each
        // payload_handle) is no longer reachable from `Drop for CoreState`:
        // by invariant, no wave is in flight when CoreState drops (BatchGuard
        // holds a Core clone), so the originating thread's WaveState
        // pending_notify is empty by then. Other threads' WaveStates are
        // unreachable from CoreState::drop anyway — they're per-thread
        // thread_locals scoped to whichever thread ran the wave. The
        // outermost `BatchGuard::drop` is the canonical drain point on both
        // success and panic paths; Drop for CoreState relies on that
        // discipline holding rather than re-implementing it.

        // Per-node retained handles:
        //   - `cache` (1 retain per non-NO_HANDLE state cache or
        //     populated compute cache).
        //   - `terminal == Some(Error(h))` (1 retain on the terminal slot).
        //   - `dep_terminals[i] == Some(Error(h))` (1 retain per consumer's
        //     terminated-dep slot).
        //   - `pause_state` paused buffer messages with payload handles
        //     (1 retain per buffered Data/Error).
        for rec in self.nodes.values_mut() {
            if rec.cache != NO_HANDLE {
                self.binding.release_handle(rec.cache);
            }
            if let Some(TerminalKind::Error(h)) = rec.terminal {
                self.binding.release_handle(h);
            }
            for dr in &rec.dep_records {
                if let Some(TerminalKind::Error(h)) = dr.terminal {
                    self.binding.release_handle(h);
                }
                // Release data_batch retains (in-flight wave data).
                for &h in &dr.data_batch {
                    self.binding.release_handle(h);
                }
                // Release prev_data retain (cross-wave persistence).
                if dr.prev_data != NO_HANDLE {
                    self.binding.release_handle(dr.prev_data);
                }
            }
            if let PauseState::Paused { buffer, .. } = &rec.pause_state {
                for msg in buffer {
                    if let Some(h) = msg.payload_handle() {
                        self.binding.release_handle(h);
                    }
                }
            }
            // Slice E1: release replay-buffer retains.
            for &h in &rec.replay_buffer {
                self.binding.release_handle(h);
            }
            // Operator scratch (Slice C-3, D026): generic per-operator
            // state struct. Each variant's release_handles releases the
            // shares it owns (Scan/Reduce acc, Distinct/Pairwise prev,
            // Last latest + default; Take/Skip/TakeWhile own no handles).
            if let Some(scratch) = rec.op_scratch.as_mut() {
                scratch.release_handles(&*self.binding);
            }
        }

        // D-α (D028 full close, 2026-05-10): drain the
        // `pending_scratch_release` queue (Phase G of
        // `Subscription::Drop` pushes old operator-scratch boxes here
        // on resubscribable + non-terminal deactivate). Catch-all for
        // Core shutdown — anything still queued never made it through
        // a `reset_for_fresh_lifecycle` drain. Release happens BEFORE
        // the queue's `Vec<Box<dyn OperatorScratch>>` drops, because
        // each box's `Drop` impl is a plain `mem::drop` of the state
        // struct fields (HandleIds are raw u64s; the boxes don't
        // re-enter the binding on drop). Without this explicit drain
        // the binding-side refcount is left bumped on Core shutdown.
        let queued: Vec<Box<dyn crate::op_state::OperatorScratch>> =
            std::mem::take(&mut self.pending_scratch_release);
        for mut scratch in queued {
            scratch.release_handles(&*self.binding);
        }

        // Q-beyond Sub-slice 1 + 2 (D108, 2026-05-09): WaveState's
        // retain-holding fields (`wave_cache_snapshots`,
        // `deferred_handle_releases`, `pending_notify`) are drained by
        // outermost BatchGuard::drop (success + panic paths). See
        // comment above for the invariant.
    }
}
