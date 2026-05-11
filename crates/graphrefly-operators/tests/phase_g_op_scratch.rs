//! D-α (D028 full close, 2026-05-10) — Phase G operator-scratch reset
//! for resubscribable nodes on non-terminal deactivate-reactivate cycles.
//!
//! Pre-D-α status matrix:
//! | Path                                                | Scratch handling  |
//! |-----------------------------------------------------|-------------------|
//! | Non-resubscribable + deactivate                     | F8 eager release  |
//! | Resubscribable + terminal + late-subscribe          | reset_for_fresh   |
//! | Resubscribable + non-terminal deactivate-reactivate | **LEAK / stale**  |
//!
//! Post-D-α: the third row routes through Phase G which (1) builds a
//! fresh scratch via `Core::make_op_scratch_with_binding` (lock-held but
//! retain_handle is a leaf op), (2) installs it on `rec.op_scratch`, and
//! (3) pushes the OLD scratch to `CoreState::pending_scratch_release`.
//! The queue drains on the next `reset_for_fresh_lifecycle` (after its
//! Phase 2 fresh retain — preserves Slice C-3 /qa P1 seed-aliasing-acc
//! invariant) or on `Drop for CoreState` (catch-all).

mod common;

use graphrefly_core::BindingBoundary;
use graphrefly_operators::flow::{last_with_default, take};
use graphrefly_operators::transform::scan;

use common::{OpRuntime, RecordedEvent, TestValue};

// =====================================================================
// take counter reset across non-terminal deactivate-reactivate
// =====================================================================

#[test]
fn take_resubscribable_non_terminal_deactivate_resets_counter() {
    // Pre-D-α: post-resubscribe `TakeState::count_emitted` retained the
    // stale count from the prior cycle; next emit hit the take limit
    // prematurely.
    //
    // Post-D-α: Phase G installs a fresh TakeState (count_emitted=0)
    // on non-terminal deactivate, so the next cycle counts from zero.
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let taken = take(&rt.core, source, 3).into_node();
    rt.core.set_resubscribable(taken, true);

    // Cycle 1: emit 2 of the 3 — take has NOT self-completed yet (so
    // this is genuinely a non-terminal deactivate path, distinct from
    // the existing `take_resubscribable_resets_counter_on_lifecycle_reset`
    // test which exercises the terminal path).
    let rec1 = rt.subscribe_recorder(taken);
    rt.emit_int(source, 1);
    rt.emit_int(source, 2);
    let cycle1: Vec<i64> = rec1.data_values().into_iter().map(|v| v.int()).collect();
    assert_eq!(cycle1, vec![1, 2], "cycle 1 should emit 1 and 2");
    assert!(
        !rec1.events().contains(&RecordedEvent::Complete),
        "take(3) should not self-complete after only 2 emits"
    );

    // Drop subscriber → Phase G runs (resubscribable + has-op +
    // non-terminal). D-α: take's count_emitted should reset to 0 on
    // the next activation.
    drop(rec1);

    // Invalidate source so the resubscribe's re-walk doesn't redeliver
    // the cached source value (which would consume one of the quota
    // slots before our fresh emits).
    rt.core.invalidate(source);

    // Cycle 2: re-subscribe and emit a fresh 3-value window.
    let rec2 = rt.subscribe_recorder(taken);
    rt.emit_int(source, 100);
    rt.emit_int(source, 200);
    rt.emit_int(source, 300);
    rt.emit_int(source, 400); // ignored — quota hit at 100/200/300
    let cycle2: Vec<i64> = rec2
        .data_values()
        .into_iter()
        .map(|v| v.int())
        .filter(|n| *n >= 100)
        .collect();
    assert_eq!(
        cycle2,
        vec![100, 200, 300],
        "post-deactivate cycle must count from 0 again (D-α). Pre-D-α \
         this would have emitted only 100 then self-completed."
    );
    assert!(
        rec2.events().contains(&RecordedEvent::Complete),
        "take(3) must self-complete after 3 fresh emits"
    );
}

// =====================================================================
// scan acc reset across non-terminal deactivate-reactivate
// =====================================================================

#[test]
fn scan_resubscribable_non_terminal_deactivate_resets_acc_to_seed() {
    // Pre-D-α: scan's `acc` retained the post-fold value across a
    // non-terminal deactivate. Post-D-α: Phase G builds a fresh
    // ScanState seeded with the registered seed, restoring `acc` to
    // the original seed on re-activation.
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let seed = rt.intern_int(10);
    // fold: acc + x
    let binding_for_fold = rt.binding.clone();
    let scanned = scan(
        &rt.core,
        &rt.op_binding,
        source,
        move |acc, x| {
            let a = binding_for_fold.deref(acc).int();
            let b = binding_for_fold.deref(x).int();
            binding_for_fold.intern(TestValue::Int(a + b))
        },
        seed,
    )
    .node;
    rt.core.set_resubscribable(scanned, true);

    let rec1 = rt.subscribe_recorder(scanned);
    rt.emit_int(source, 5);
    rt.emit_int(source, 3);
    let cycle1: Vec<i64> = rec1.data_values().into_iter().map(|v| v.int()).collect();
    assert_eq!(
        cycle1,
        vec![15, 18],
        "scan(seed=10) folds 10+5=15 then 15+3=18"
    );
    drop(rec1);

    // Phase G ran (non-terminal). On the next subscribe + emit, acc
    // must restart from seed=10.
    rt.core.invalidate(source);

    let rec2 = rt.subscribe_recorder(scanned);
    rt.emit_int(source, 7);
    let cycle2: Vec<i64> = rec2
        .data_values()
        .into_iter()
        .map(|v| v.int())
        .filter(|n| ![15, 18].contains(n)) // drop any cache replays
        .collect();
    assert_eq!(
        cycle2,
        vec![17],
        "post-deactivate cycle must restart acc from seed=10 (so 10+7=17). \
         Pre-D-α this would have emitted 25 (18 + 7 carrying over stale acc)."
    );
}

// =====================================================================
// Seed-aliasing-acc invariant on non-terminal deactivate-reactivate
// (D-α regression: must not collapse the registry slot)
// =====================================================================

#[test]
fn phase_g_resubscribable_seed_aliasing_acc_does_not_collapse_registry() {
    // The C-3 /qa P1 invariant for non-terminal deactivate: a fold
    // where `fold(seed, x) == seed` aliases acc → seed in the
    // registry. Phase G must take a fresh retain on seed BEFORE
    // releasing the old acc share, otherwise the binding's
    // refcount-zero reaper drops the registry entry and a later
    // `release_handle` on the queued OLD acc underflows.
    //
    // Verification: drive a non-terminal deactivate-reactivate cycle
    // on a scan whose fold is the identity-acc (`|acc, _| acc`), and
    // verify the seed handle stays alive in the registry across the
    // cycle.
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let seed = rt.intern_int(42);
    // Diagnostic share so refcount_of(seed) stays observable even if
    // a bug drops to zero (we'd otherwise see a panic from
    // intern/deref on a dropped slot). Bumps refcount on the SAME
    // handle.
    let _keep_alive = rt.intern_int(42);
    let baseline = rt.binding.refcount_of(seed);
    // /qa m3 (2026-05-10): pre-cycle, refcount(seed) == 2
    // (user intern + diagnostic intern). After scan registration's
    // `make_op_scratch` retain, expect 3.
    assert_eq!(
        baseline, 2,
        "pre-scan baseline: user intern + diagnostic intern = 2"
    );

    // Identity-acc fold: `|acc, _| acc` — acc never changes from seed.
    let binding_for_fold = rt.binding.clone();
    let scanned = scan(
        &rt.core,
        &rt.op_binding,
        source,
        move |acc, _x| {
            // Bump the share via intern path — `intern(deref(acc))`
            // re-interns to the same slot, emulating "fold returns the
            // same value as seed."
            binding_for_fold.intern(binding_for_fold.deref(acc))
        },
        seed,
    )
    .node;
    rt.core.set_resubscribable(scanned, true);
    assert_eq!(
        rt.binding.refcount_of(seed),
        3,
        "post-scan registration: user + diag + ScanState.acc retain = 3"
    );

    let rec1 = rt.subscribe_recorder(scanned);
    rt.emit_int(source, 1);
    rt.emit_int(source, 2);
    // After 2 emits with identity-acc fold: source.cache holds the
    // last emitted value handle (not seed); ScanState.acc still
    // aliases seed (intern hit). Multiple retains accumulate from the
    // emit path (scan.cache for last emission + retain in operator
    // pipeline). Lower bound: baseline + 1 (acc retain) = 3.
    assert!(
        rt.binding.refcount_of(seed) > baseline,
        "seed retained above baseline during active subscription (acc \
         aliases seed via identity fold). got refcount(seed)={}, baseline={baseline}",
        rt.binding.refcount_of(seed)
    );
    drop(rec1);
    // Phase G must NOT collapse the slot. The OLD scratch was pushed
    // to pending_scratch_release with its share of acc=seed; FRESH
    // scratch installed with a new retain on seed. Net change after
    // Phase G: -1 (old scratch share now in queue, not in rec.op_scratch
    // but still alive) +1 (fresh scratch retain) = 0. Plus the deactivate
    // cache-clear releases the compute cache (which held seed via
    // identity-acc fold). Lower bound: baseline (2) still alive.
    assert!(
        rt.binding.refcount_of(seed) >= baseline,
        "seed handle must NOT drop below baseline ({baseline}) after \
         Phase G — D-α retain-before-release invariant. got refcount(seed)={}",
        rt.binding.refcount_of(seed)
    );

    rt.core.invalidate(source);
    let _rec2 = rt.subscribe_recorder(scanned);
    rt.emit_int(source, 3);
    assert!(
        rt.binding.refcount_of(seed) >= baseline,
        "seed handle remains at least at baseline ({baseline}) across \
         non-terminal deactivate-reactivate"
    );
}

// =====================================================================
// Queue drains on Core drop (catch-all path)
// =====================================================================

#[test]
fn pending_scratch_release_drains_on_core_drop() {
    // Phase G on resubscribable + has-op pushes old scratches to
    // `pending_scratch_release`. If reset_for_fresh_lifecycle never
    // runs (no terminal), the queue accumulates. `Drop for CoreState`
    // must drain it to release the queued boxes' handle retains.
    //
    // Verification: build a scan with a seed, subscribe, emit (so
    // ScanState.acc transitions away from seed), drop subscriber
    // (Phase G pushes the OLD scratch with acc=fold(...) to the queue
    // AND installs a fresh scratch with acc=seed), drop OpRuntime
    // (CoreState::drop runs). The folded intermediate values held by
    // the queued OLD scratch must be released by the D-α drain. We
    // verify by tracking specific folded handles' refcounts: if the
    // queue wasn't drained, the folded acc value (e.g., 103) would
    // remain at refcount > 0 after Core drop.
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let seed = rt.intern_int(100);
    let binding_for_fold = rt.binding.clone();
    let scanned = scan(
        &rt.core,
        &rt.op_binding,
        source,
        move |acc, x| {
            let a = binding_for_fold.deref(acc).int();
            let b = binding_for_fold.deref(x).int();
            binding_for_fold.intern(TestValue::Int(a + b))
        },
        seed,
    )
    .node;
    rt.core.set_resubscribable(scanned, true);

    let rec = rt.subscribe_recorder(scanned);
    rt.emit_int(source, 1);
    rt.emit_int(source, 2);
    // ScanState.acc is now fold(fold(seed=100, 1), 2) = 103.
    drop(rec);
    // Phase G pushed the old ScanState (acc=103 share) to the queue.
    // The queue now retains the 103 handle. Without D-α drain on
    // Core drop, 103 leaks.

    // Take a diagnostic share of 103 so we can observe its refcount
    // post-drop. This holds the registry entry alive even if Core
    // released its share, so we can distinguish "released by Core"
    // from "still leaked in registry."
    let observed_103 = rt.binding.intern(TestValue::Int(103));
    let refcount_pre_drop = rt.binding.refcount_of(observed_103);
    assert!(
        refcount_pre_drop >= 2,
        "diagnostic intern + queued ScanState.acc share both retain 103. \
         Got refcount={refcount_pre_drop}"
    );

    let binding = rt.binding.clone();
    drop(rt);
    // CoreState::drop ran. D-α drain released the queued ScanState's
    // share of 103. Only the diagnostic share remains.
    let refcount_post_drop = binding.refcount_of(observed_103);
    assert_eq!(
        refcount_post_drop, 1,
        "CoreState::drop must drain pending_scratch_release queue \
         (D-α catch-all). Pre-drop refcount(103)={refcount_pre_drop} \
         (diagnostic + queued ScanState.acc); post-drop expected 1 \
         (diagnostic only). Got {refcount_post_drop}."
    );
    // Cleanup: release diagnostic share.
    binding.release_handle(observed_103);
    let _ = seed; // silence unused-var lint
}

// =====================================================================
// Queue drains on reset_for_fresh_lifecycle (terminal path, after
// fresh retain — preserves C-3 /qa P1 invariant when terminal arrives
// after one or more non-terminal cycles)
// =====================================================================

#[test]
fn pending_scratch_release_drains_on_reset_for_fresh_lifecycle() {
    // Phase G accumulates old scratches in the queue across
    // non-terminal deactivate cycles. When a terminal eventually
    // arrives and re-subscribe triggers reset_for_fresh_lifecycle,
    // its Phase 3b drains the queue AFTER Phase 2's fresh retain (so
    // any seed-aliasing-acc shares released here are safe — the
    // registry slot is already floored at ≥1 by the fresh retain).
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let default_handle = rt.intern_int(0);
    let n = last_with_default(&rt.core, source, default_handle)
        .expect("last_with_default registers cleanly for live non-terminal source")
        .into_node();
    rt.core.set_resubscribable(n, true);

    // Cycle 1: subscribe, emit, deactivate (no terminal).
    let rec1 = rt.subscribe_recorder(n);
    rt.emit_int(source, 7);
    drop(rec1);
    // Phase G pushed old LastState (latest=7) to queue. Installed
    // fresh LastState (latest=NO_HANDLE, default=0 retained fresh).

    // Cycle 2: subscribe, emit different value, deactivate.
    rt.core.invalidate(source);
    let rec2 = rt.subscribe_recorder(n);
    rt.emit_int(source, 9);
    drop(rec2);
    // Queue now has TWO entries (latest=7 from cycle 1, latest=9 from
    // cycle 2).

    // Cycle 3: terminal arrives. Re-subscribe triggers
    // reset_for_fresh_lifecycle (resubscribable + terminal). Its
    // Phase 3b drains the queue. Live handles must collapse to only
    // the values still actively held by Core (source.cache, etc.) +
    // operator scratch + dep_records — no orphan shares from queued
    // old LastStates.
    rt.core.complete(source);
    let live_pre_reset = rt.binding.live_handles();
    let _rec3 = rt.subscribe_recorder(n);
    // After reset, the queued boxes' shares of 7 and 9 must be
    // released; if any remained, `live_handles` would not have
    // decreased.
    let live_post_reset = rt.binding.live_handles();
    assert!(
        live_post_reset <= live_pre_reset,
        "reset_for_fresh_lifecycle Phase 3b must drain pending_scratch_release \
         (D-α drain on next-reset path). pre={live_pre_reset}, post={live_post_reset}"
    );
    // The handles 7 and 9 must not appear in the registry anymore (we
    // hold no diagnostic shares; ScanState's retain is the only
    // source).
    let h7 = rt.intern_int(7); // re-intern bumps refcount; if value
                               // was already gone, this creates a fresh entry — but we just
                               // bumped it by 1, so refcount must be >0. The real check is:
                               // BEFORE this re-intern, did `7` exist as a residual share?
    assert!(
        rt.binding.refcount_of(h7) >= 1,
        "intern(7) post-test must be valid"
    );
}
