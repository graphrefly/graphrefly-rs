//! D291 — terminal-status rollback on `BatchGuard` panic-discard
//! (R4.3.2 status-snapshot completeness for COMPLETE/ERROR tiers).
//!
//! Closes the cross-track-ledger §1 D282 row's D290 follow-on Case 5:
//! pre-D291 a `core.complete(node)` inside `core.batch(...)` + throw
//! would clear the wave's pending tier-5 message via
//! `BatchGuard::discard_wave_cleanup` but leave `rec.terminal = Some(_)`,
//! causing the substrate's terminal-state guard to silently reject any
//! post-rollback emit on the same node.
//!
//! D291 adds `wave_terminal_snapshots` + `wave_dep_terminal_snapshots`
//! to `WaveState` and `Core::restore_wave_terminal_snapshots` to reset
//! the slots on panic-discard.
//!
//! Source: `~/src/graphrefly-ts/docs/cross-track-ledger.md` §1 D282 row;
//! `~/src/graphrefly-ts/docs/rust-port-decisions.md` D291.

use std::panic;

mod common;
use common::{TestRuntime, TestValue};

// ---------------------------------------------------------------------------
// Case 5 minimum — leaf source COMPLETE inside batch + throw, then assert
// the source is NOT in terminal state and is re-emittable post-rollback.
// ---------------------------------------------------------------------------

#[test]
fn d291_terminal_rollback_leaf_source_complete() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(5)));
    let s_id = s.id;

    // Pre-batch: source is non-terminal.
    assert!(
        rt.core().is_terminal(s_id).is_none(),
        "source must start non-terminal"
    );
    assert_eq!(rt.cache_value(s_id), Some(TestValue::Int(5)));

    let core = rt.core();
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        core.batch(|| {
            core.complete(s_id);
            // At this point rec.terminal = Some(Complete) (visible
            // mid-batch). Throwing triggers BatchGuard::Drop's
            // panic-discard path → discard_wave_cleanup →
            // restore_wave_terminal_snapshots resets it to None.
            panic!("user threw mid-batch");
        });
    }));
    assert!(result.is_err(), "panic must propagate out of batch()");

    // Post-rollback: terminal slot restored, source re-emittable.
    assert!(
        rt.core().is_terminal(s_id).is_none(),
        "D291: panic-discard must restore rec.terminal to None (was Some(Complete) mid-batch)"
    );

    // Verify re-emit succeeds: the substrate's terminal-state guard
    // would silently reject a `.emit` on a terminal node pre-D291.
    let binding = rt.binding.clone();
    let h_6 = binding.intern(TestValue::Int(6));
    rt.core().emit(s_id, h_6);
    assert_eq!(
        rt.cache_value(s_id),
        Some(TestValue::Int(6)),
        "post-rollback emit must land; pre-D291 was silently rejected because rec.terminal stayed Some(Complete)"
    );
}

// ---------------------------------------------------------------------------
// Cascade — state → derived: COMPLETE the state inside batch + throw,
// then assert (a) the source restores, (b) the derived's per-dep terminal
// slot also restored to None (cascade rollback).
// ---------------------------------------------------------------------------

#[test]
fn d291_terminal_rollback_cascade_clears_child_dep_slot() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let s_id = s.id;
    let derived = rt.derived(&[s_id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(n + 1)),
        _ => None,
    });
    // Subscribe derived to instantiate it.
    let _rec = rt.subscribe_recorder(derived);
    // Settle the initial fire wave.
    assert_eq!(rt.cache_value(derived), Some(TestValue::Int(1)));

    let core = rt.core();
    let _ = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        core.batch(|| {
            // COMPLETE the source. terminate_node's cascade walks
            // children → sets derived.dep_records[0].terminal = Some(Complete).
            // Since derived has only one dep, all_deps_terminal → derived
            // itself gets queued for cascade (terminate_node pushed onto
            // the work-queue). So BOTH `s` and `derived` end up with
            // `rec.terminal = Some(Complete)` AND `derived.dep_records[0]
            // .terminal = Some(Complete)`.
            core.complete(s_id);
            panic!("rollback me");
        });
    }));

    // Source restored.
    assert!(
        rt.core().is_terminal(s_id).is_none(),
        "D291: source's rec.terminal must restore to None"
    );
    // Derived restored.
    assert!(
        rt.core().is_terminal(derived).is_none(),
        "D291: derived's rec.terminal (set via cascade) must restore to None"
    );
    // D291 /qa A7 (2026-05-25): pin the per-dep cascade slot DIRECTLY
    // (not just observe the downstream re-fire effect). Pre-/qa, the
    // test relied solely on `derived.cache == 43` post-re-emit — that
    // could pass for reasons OTHER than per-dep slot restoration (e.g.
    // top-level slot restored + cascade re-evaluates from scratch on
    // next emit), giving a false-green on a real per-dep regression.
    // `Core::dep_terminal_of(child, dep_node_id)` reads
    // `rec.dep_records[idx].terminal` directly via dep NodeId (D291
    // /qa D2 re-keying — index resolved via `dep_index_of` at read time).
    assert!(
        rt.core().dep_terminal_of(derived, s_id).is_none(),
        "D291 /qa A7: per-dep cascade slot derived.dep_records[s].terminal \
         must restore to None directly (independent of downstream re-fire)"
    );
    // Per-dep cascade slot restored — verify by re-emitting upstream and
    // observing the derived fires again (would not fire if the per-dep
    // terminal slot were stuck).
    let binding = rt.binding.clone();
    let h_42 = binding.intern(TestValue::Int(42));
    rt.core().emit(s_id, h_42);
    assert_eq!(
        rt.cache_value(derived),
        Some(TestValue::Int(43)),
        "D291: derived must re-fire post-rollback; per-dep terminal slot \
         was stuck pre-fix → first-run gate or terminal short-circuit \
         would suppress the fire"
    );
}

// ---------------------------------------------------------------------------
// Success-path sanity — a clean COMPLETE inside batch (no throw) MUST
// commit normally (rec.terminal stays Some(Complete)). Pin so D291's
// snapshot/restore plumbing can't accidentally trigger on the commit
// path.
// ---------------------------------------------------------------------------

#[test]
fn d291_terminal_commit_does_not_revert() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let s_id = s.id;

    rt.core().batch(|| {
        rt.core().complete(s_id);
    });

    assert!(
        rt.core().is_terminal(s_id).is_some(),
        "D291 success path: rec.terminal must stay Some(Complete) on commit"
    );
}
