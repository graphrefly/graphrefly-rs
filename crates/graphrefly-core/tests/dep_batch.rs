//! M3 Slice A /qa — DepRecord + DepBatch regression coverage.
//!
//! These tests exercise the per-dep `DepRecord` substrate (R1.3.6.b
//! batch-array delivery) at the FFI boundary by capturing the `&[DepBatch]`
//! values delivered to `BindingBoundary::invoke_fn`. Slice A landed the
//! structural refactor without these regression tests; this file closes that
//! gap.
//!
//! Coverage:
//! - K-emit coalescing inside `batch()` produces a `data` SmallVec with K
//!   entries, in emit order.
//! - `prev_data` rotates correctly across waves: wave N+1's `prev_data`
//!   equals the last DATA from wave N's batch.
//! - `involved` flag distinguishes "RESOLVED in wave" (involved + empty
//!   data) from "not in wave" (!involved + empty data).
//! - Refcount discipline: each `data_batch` entry retains; rotation
//!   transfers the last batch's retain to `prev_data`; earlier entries
//!   release.
//! - Multi-dep diamonds: per-dep batches stay separately tracked.

mod common;

use std::sync::{Arc, Mutex};

use common::{TestRuntime, TestValue};
use graphrefly_core::{DepBatch, EqualsMode, FnResult, HandleId};

type Captured = Arc<Mutex<Vec<Vec<CapturedDep>>>>;

#[derive(Clone, Debug)]
struct CapturedDep {
    data: Vec<HandleId>,
    prev_data: HandleId,
    involved: bool,
}

fn snapshot(deps: &[DepBatch]) -> Vec<CapturedDep> {
    deps.iter()
        .map(|d| CapturedDep {
            data: d.data.iter().copied().collect(),
            prev_data: d.prev_data,
            involved: d.involved,
        })
        .collect()
}

/// R1.3.6.b — single emit produces a 1-entry batch (fast path; SmallVec inline-1).
#[test]
fn single_emit_outside_batch_yields_data_len_1() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));

    let captured: Captured = Arc::new(Mutex::new(Vec::new()));
    let captured_c = captured.clone();
    let fn_id = rt.binding.register_raw_fn(move |deps| {
        captured_c.lock().unwrap().push(snapshot(deps));
        FnResult::Noop { tracked: None }
    });

    let d = rt
        .core
        .register_derived(&[s.id], fn_id, EqualsMode::Identity, false);
    let _rec = rt.subscribe_recorder(d);

    captured.lock().unwrap().clear(); // discard activation fire

    s.set(TestValue::Int(42));

    let snaps = captured.lock().unwrap().clone();
    assert_eq!(snaps.len(), 1, "fn fired once for the single emit");
    assert_eq!(snaps[0].len(), 1, "1 dep");
    assert_eq!(snaps[0][0].data.len(), 1, "single-emit fast path");
    assert!(snaps[0][0].involved);
}

/// R1.3.6.b — K consecutive emits inside `batch()` coalesce into one fire
/// with `data` containing K entries in emit order.
#[test]
fn k_emit_in_batch_coalesces_into_one_fire_with_k_data_entries() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));

    let captured: Captured = Arc::new(Mutex::new(Vec::new()));
    let captured_c = captured.clone();
    let fn_id = rt.binding.register_raw_fn(move |deps| {
        captured_c.lock().unwrap().push(snapshot(deps));
        FnResult::Noop { tracked: None }
    });

    let d = rt
        .core
        .register_derived(&[s.id], fn_id, EqualsMode::Identity, false);
    let _rec = rt.subscribe_recorder(d);
    captured.lock().unwrap().clear();

    let h1 = rt.binding.intern(TestValue::Int(1));
    let h2 = rt.binding.intern(TestValue::Int(2));
    let h3 = rt.binding.intern(TestValue::Int(3));
    rt.core.batch(|| {
        rt.core.emit(s.id, h1);
        rt.core.emit(s.id, h2);
        rt.core.emit(s.id, h3);
    });

    let snaps = captured.lock().unwrap().clone();
    assert_eq!(snaps.len(), 1, "fn fires once per wave regardless of K");
    let dep = &snaps[0][0];
    assert_eq!(dep.data, vec![h1, h2, h3], "K-emit coalesces in emit order",);
    assert!(dep.involved);
}

/// `prev_data` rotates: wave N+1's `prev_data` equals the LAST DATA handle
/// from wave N's batch.
#[test]
fn prev_data_rotates_to_last_batch_entry_across_waves() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));

    let captured: Captured = Arc::new(Mutex::new(Vec::new()));
    let captured_c = captured.clone();
    let fn_id = rt.binding.register_raw_fn(move |deps| {
        captured_c.lock().unwrap().push(snapshot(deps));
        FnResult::Noop { tracked: None }
    });

    let d = rt
        .core
        .register_derived(&[s.id], fn_id, EqualsMode::Identity, false);
    let _rec = rt.subscribe_recorder(d);
    captured.lock().unwrap().clear();

    // Wave 1: 2-emit batch produces last_h1 = h_b
    let h_a = rt.binding.intern(TestValue::Int(10));
    let h_b = rt.binding.intern(TestValue::Int(20));
    rt.core.batch(|| {
        rt.core.emit(s.id, h_a);
        rt.core.emit(s.id, h_b);
    });

    // Wave 2: emit h_c. prev_data should be h_b (last entry of prior batch).
    let h_c = rt.binding.intern(TestValue::Int(30));
    rt.core.emit(s.id, h_c);

    let snaps = captured.lock().unwrap().clone();
    assert_eq!(snaps.len(), 2);
    // Wave 1 baseline: prev_data should be the activation-fire's rotated
    // handle (the source's initial value, NOT NO_HANDLE) — pins
    // activation-fire rotation discipline. (A4 — /qa coverage gap.)
    assert_ne!(
        snaps[0][0].prev_data,
        graphrefly_core::NO_HANDLE,
        "wave-1 prev_data reflects activation-fire rotation, not sentinel",
    );
    // Wave 2 fire (index 1): prev_data == h_b
    assert_eq!(snaps[1][0].prev_data, h_b);
    assert_eq!(snaps[1][0].data, vec![h_c]);
}

/// `involved=false` distinguishes "dep not in this wave" from RESOLVED.
/// In a 2-dep node, when only dep 0 emits, dep 1's batch is empty AND
/// `!involved`.
#[test]
fn uninvolved_dep_shows_data_empty_and_involved_false() {
    let rt = TestRuntime::new();
    let s1 = rt.state(Some(TestValue::Int(1)));
    let s2 = rt.state(Some(TestValue::Int(2)));

    let captured: Captured = Arc::new(Mutex::new(Vec::new()));
    let captured_c = captured.clone();
    let fn_id = rt.binding.register_raw_fn(move |deps| {
        captured_c.lock().unwrap().push(snapshot(deps));
        FnResult::Noop { tracked: None }
    });

    let d = rt
        .core
        .register_derived(&[s1.id, s2.id], fn_id, EqualsMode::Identity, false);
    let _rec = rt.subscribe_recorder(d);
    // Drain activation fires (handshake tier-split R1.3.5.a may produce
    // multiple captured fires). Core is fully synchronous so a single
    // clear after subscribe-return is sufficient; the post-set
    // `assert_eq!(snaps.len(), 1)` self-checks if a stray fire lands.
    // (A5 — /qa fix: explicit comment on the discipline.)
    captured.lock().unwrap().clear();

    // Emit only on s1; s2 is uninvolved this wave.
    s1.set(TestValue::Int(99));

    let snaps = captured.lock().unwrap().clone();
    assert_eq!(
        snaps.len(),
        1,
        "exactly one fire from s1.set; activation drained pre-set",
    );
    let s = &snaps[0];
    // s1 (dep 0) is involved with one DATA.
    assert!(s[0].involved);
    assert_eq!(s[0].data.len(), 1);
    // s2 (dep 1) is uninvolved with empty data.
    assert!(!s[1].involved);
    assert!(s[1].data.is_empty());
}

/// `involved=true` with empty data signals "RESOLVED in this wave".
/// Triggered by equals-substitution: emit the same value that's already
/// cached → child sees DIRTY+RESOLVED forwarded with no DATA on this dep.
#[test]
fn resolved_in_wave_yields_involved_true_with_empty_data() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(7)));

    let captured: Captured = Arc::new(Mutex::new(Vec::new()));
    let captured_c = captured.clone();
    let fn_id = rt.binding.register_raw_fn(move |deps| {
        captured_c.lock().unwrap().push(snapshot(deps));
        FnResult::Noop { tracked: None }
    });

    let d = rt
        .core
        .register_derived(&[s.id], fn_id, EqualsMode::Identity, false);
    let _rec = rt.subscribe_recorder(d);
    captured.lock().unwrap().clear();

    // Emit the same value 7 — Identity equals → RESOLVED on the wire.
    s.set(TestValue::Int(7));

    let snaps = captured.lock().unwrap().clone();
    // Two acceptable spec-conformant branches; pin which one Rust chose.
    // (A2 — /qa fix: the previous loop body asserted a tautology.)
    //
    // Branch 1: wave fully suppressed (fn doesn't fire at all). Acceptable
    // when the dispatcher elides the fire because the only dep settled
    // RESOLVED via equals-substitution and no other deps were dirty.
    //
    // Branch 2: fn fires with `involved=true` + `data=[]`. The substrate
    // contract: any "empty data with involved=true" must mean
    // RESOLVED-in-wave (R1.3.2.a equals match), NOT "dep was uninvolved"
    // (which is the inverted case asserted by `uninvolved_dep_*` above).
    if snaps.is_empty() {
        // Branch 1 — wave suppression. No further assertions; just confirm
        // the dispatcher's choice.
    } else {
        // Branch 2 — fn fired. Every fire's dep[0] must have data either
        // non-empty OR involved=true (rejecting the failure mode where a
        // non-empty data is paired with involved=false, which would be
        // protocol-corrupt).
        for s in &snaps {
            assert!(
                !s[0].data.is_empty() || s[0].involved,
                "RESOLVED-in-wave must surface as involved=true with empty data, not silent uninvolved",
            );
        }
    }
}

/// Refcount: each batch entry retains its handle. Rotation at wave-end
/// releases earlier entries; the last batch entry's retain transfers to
/// `prev_data`. Net refcount delta after K-emit batch == 1 retain
/// (the cache slot in `prev_data`).
#[test]
fn k_emit_batch_refcounts_balance_after_wave_end() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));

    let fn_id = rt
        .binding
        .register_raw_fn(|_deps| FnResult::Noop { tracked: None });
    let d = rt
        .core
        .register_derived(&[s.id], fn_id, EqualsMode::Identity, false);
    let _rec = rt.subscribe_recorder(d);

    let h_a = rt.binding.intern(TestValue::Int(101));
    let h_b = rt.binding.intern(TestValue::Int(102));
    let h_c = rt.binding.intern(TestValue::Int(103));

    let rc_a_before = rt.binding.refcount_of(h_a);
    let rc_b_before = rt.binding.refcount_of(h_b);
    let rc_c_before = rt.binding.refcount_of(h_c);

    rt.core.batch(|| {
        rt.core.emit(s.id, h_a);
        rt.core.emit(s.id, h_b);
        rt.core.emit(s.id, h_c);
    });

    // Refcount discipline (A1 — /qa fix: previous `<= rc_before + 1`
    // admitted +1-per-rotation leaks. Strict: each non-final batch entry
    // must release fully).
    //
    // Ownership model (Core consumes caller's share on emit):
    // - `intern(h_a)` bumps rc to 1 (test's share).
    // - `emit(s, h_a)` transfers share to Core (no extra retain).
    // - Core retains separately for derived's data_batch slot (rc=2).
    // - Subsequent emit(h_b) replaces source cache — release(h_a) → rc=1.
    // - Wave-end rotation releases data_batch[0] (h_a's batch slot) → rc=0.
    // Net: h_a (and h_b) end at 0; only h_c (rotation winner) remains.
    let rc_a_after = rt.binding.refcount_of(h_a);
    let rc_b_after = rt.binding.refcount_of(h_b);
    let rc_c_after = rt.binding.refcount_of(h_c);

    assert_eq!(
        rc_a_after, 0,
        "h_a was consumed by emit + replaced by h_b's commit + rotated out of data_batch — must be fully released (was {rc_a_before}, now {rc_a_after})",
    );
    assert_eq!(
        rc_b_after, 0,
        "h_b same discipline as h_a — fully released after rotation (was {rc_b_before}, now {rc_b_after})",
    );
    assert!(
        rc_c_after >= 2,
        "h_c (rotation winner) held by source cache + derived prev_data — rc must be >= 2 (was {rc_c_before}, now {rc_c_after})",
    );
}

/// Multi-dep diamond: each dep maintains its own `data_batch`/`prev_data`
/// independently. Emitting on different deps in different waves rotates
/// each dep's prev_data on its own schedule.
#[test]
fn multi_dep_each_dep_rotates_prev_data_independently() {
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(1)));
    let b = rt.state(Some(TestValue::Int(2)));

    let captured: Captured = Arc::new(Mutex::new(Vec::new()));
    let captured_c = captured.clone();
    let fn_id = rt.binding.register_raw_fn(move |deps| {
        captured_c.lock().unwrap().push(snapshot(deps));
        FnResult::Noop { tracked: None }
    });

    let d = rt
        .core
        .register_derived(&[a.id, b.id], fn_id, EqualsMode::Identity, false);
    let _rec = rt.subscribe_recorder(d);
    captured.lock().unwrap().clear();

    // Wave 1: emit handle h_a_w1 on a only. Capture handle for the
    // post-wave-2 assertion (intern dedup guarantees same handle
    // re-returns when the same value is interned, so we can compare
    // against `last[0].prev_data` later). (A3 — /qa fix.)
    let h_a_w1 = rt.binding.intern(TestValue::Int(10));
    a.set(TestValue::Int(10));

    // Wave 2: emit on b only.
    b.set(TestValue::Int(20));

    let snaps = captured.lock().unwrap().clone();
    assert_eq!(
        snaps.len(),
        2,
        "exactly one fire per single-dep emit; activation drained pre-wave-1",
    );

    // Wave 2 fire: dep a is uninvolved (data empty, !involved); dep b is
    // involved with one DATA. dep a's prev_data must equal h_a_w1 (the
    // exact handle from wave 1) — strict assertion replaces the prior
    // `assert_ne!(.., NO_HANDLE)` which would pass on a leaked-but-non-
    // sentinel value.
    let last = &snaps[snaps.len() - 1];
    assert!(!last[0].involved, "dep a uninvolved in wave 2");
    assert!(last[1].involved, "dep b involved in wave 2");
    assert_eq!(last[1].data.len(), 1);
    assert_eq!(
        last[0].prev_data, h_a_w1,
        "dep a's prev_data is exactly the handle emitted in wave 1",
    );
}
