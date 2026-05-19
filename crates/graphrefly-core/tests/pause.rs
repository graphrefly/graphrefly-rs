//! PAUSE / RESUME — multi-pauser lockset + replay buffer.
//!
//! Maps to the canonical spec §1.2.6 (lockId mandatory) + §2.6 (multi-pauser
//! lockset), the §10.2 simplification (PauseState enum + VecDeque), and the
//! TLA+ scenario MC `handle_pause_MC` from `~/src/graphrefly-ts/docs/research/`.
//!
//! Tier routing:
//! - Tier 3 (DATA / RESOLVED) buffers while paused.
//! - Tier 4 (INVALIDATE) buffers while paused.
//! - Tier 1 (DIRTY), tier 2 (PAUSE / RESUME), tier 5 (COMPLETE / ERROR),
//!   tier 6 (TEARDOWN) bypass the buffer and flush immediately.

use std::sync::Arc;

mod common;
use common::{RecordedEvent, TestRuntime, TestValue};
use graphrefly_core::PausableMode;

#[test]
fn pause_then_resume_with_no_emissions_drains_empty_buffer() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let _rec = rt.subscribe_recorder(s.id);
    let lock = rt.core().alloc_lock_id();
    rt.core().pause(s.id, lock).expect("pause ok");
    assert!(rt.core().is_paused(s.id));
    let report = rt
        .core()
        .resume(s.id, lock)
        .expect("resume ok")
        .expect("final lock release should yield a ResumeReport");
    assert_eq!(report.replayed, 0);
    assert_eq!(report.dropped, 0);
    assert!(!rt.core().is_paused(s.id));
}

#[test]
fn single_pauser_buffers_data_and_resolved_then_replays_on_resume() {
    // R2.6.0 (canonical §2.6 "Option A", pinned 2026-05-17): a Default-mode
    // leaf source delivers its OWN direct emit immediately while self-paused
    // (verified by `r2_6_0_*` in slice_f_corrections.rs). The verbatim
    // buffer-then-replay contract this test exercises is the `ResumeAll`
    // contract — opt in explicitly (the established Slice-F pattern, cf.
    // `pause_buffers_derived_data_through_diamond`).
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    rt.core()
        .set_pausable_mode(s.id, PausableMode::ResumeAll)
        .unwrap();
    let rec = rt.subscribe_recorder(s.id);

    // Activate: push-on-subscribe delivers START + DATA(0).
    let baseline = rec.snapshot();
    let lock = rt.core().alloc_lock_id();

    // Pause; subsequent emissions buffer.
    rt.core().pause(s.id, lock).expect("pause ok");
    s.set(TestValue::Int(1)); // DIRTY (immediate) + DATA(1) (buffered)
    s.set(TestValue::Int(2)); // DIRTY (immediate) + DATA(2) (buffered)

    // Subscriber observed DIRTY but no DATA yet (DATAs are buffered).
    let mid_pause = rec.snapshot();
    let dirty_count = mid_pause[baseline.len()..]
        .iter()
        .filter(|e| matches!(e, RecordedEvent::Dirty))
        .count();
    let data_count = mid_pause[baseline.len()..]
        .iter()
        .filter(|e| matches!(e, RecordedEvent::Data(_)))
        .count();
    assert_eq!(dirty_count, 2, "DIRTY should pass through pause");
    assert_eq!(data_count, 0, "DATA should buffer while paused");

    // Resume and verify replay.
    let report = rt
        .core()
        .resume(s.id, lock)
        .expect("resume ok")
        .expect("final lock release should yield a ResumeReport");
    assert_eq!(report.replayed, 2, "two DATAs replayed");
    assert_eq!(report.dropped, 0);

    let post_resume = rec.snapshot();
    let post_data_values: Vec<i64> = post_resume[mid_pause.len()..]
        .iter()
        .filter_map(|e| match e {
            RecordedEvent::Data(TestValue::Int(n)) => Some(*n),
            _ => None,
        })
        .collect();
    assert_eq!(
        post_data_values,
        vec![1, 2],
        "DATAs replayed in arrival order"
    );
}

#[test]
fn multi_pauser_remains_paused_until_final_release() {
    // R2.6.0: verbatim buffer-replay is the `ResumeAll` contract — opt in.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    rt.core()
        .set_pausable_mode(s.id, PausableMode::ResumeAll)
        .unwrap();
    let rec = rt.subscribe_recorder(s.id);
    let baseline = rec.snapshot().len();

    let lock_a = rt.core().alloc_lock_id();
    let lock_b = rt.core().alloc_lock_id();
    let lock_c = rt.core().alloc_lock_id();

    rt.core().pause(s.id, lock_a).expect("pause a");
    rt.core().pause(s.id, lock_b).expect("pause b");
    rt.core().pause(s.id, lock_c).expect("pause c");
    assert_eq!(rt.core().pause_lock_count(s.id), 3);

    s.set(TestValue::Int(1));
    s.set(TestValue::Int(2));
    s.set(TestValue::Int(3));

    // Release two of three locks — still paused.
    let no_drain_b = rt.core().resume(s.id, lock_b).expect("resume b");
    assert!(no_drain_b.is_none(), "non-final resume returns None");
    let no_drain_c = rt.core().resume(s.id, lock_c).expect("resume c");
    assert!(no_drain_c.is_none());
    assert!(rt.core().is_paused(s.id));
    assert_eq!(rt.core().pause_lock_count(s.id), 1);

    // No DATA delivered yet during multi-pause.
    let mid_count_data = rec
        .snapshot()
        .iter()
        .skip(baseline)
        .filter(|e| matches!(e, RecordedEvent::Data(_)))
        .count();
    assert_eq!(mid_count_data, 0);

    // Final release drains the buffer.
    let report = rt
        .core()
        .resume(s.id, lock_a)
        .expect("resume a")
        .expect("final lock release yields a ResumeReport");
    assert_eq!(report.replayed, 3);
    assert_eq!(report.dropped, 0);

    let final_data: Vec<i64> = rec
        .snapshot()
        .iter()
        .skip(baseline)
        .filter_map(|e| match e {
            RecordedEvent::Data(TestValue::Int(n)) => Some(*n),
            _ => None,
        })
        .collect();
    assert_eq!(final_data, vec![1, 2, 3]);
}

#[test]
fn duplicate_pause_lockid_is_idempotent() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let lock = rt.core().alloc_lock_id();
    rt.core().pause(s.id, lock).expect("pause");
    rt.core().pause(s.id, lock).expect("pause again");
    rt.core().pause(s.id, lock).expect("pause yet again");
    assert_eq!(rt.core().pause_lock_count(s.id), 1, "duplicate ids dedupe");
    rt.core().resume(s.id, lock).expect("resume");
    assert!(!rt.core().is_paused(s.id));
}

#[test]
fn unknown_resume_lockid_is_noop_and_returns_none() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));

    // Resume on Active node with bogus lock — silent no-op.
    let bogus = graphrefly_core::LockId::new(99_999);
    let result = rt.core().resume(s.id, bogus).expect("resume ok");
    assert!(result.is_none());
    assert!(!rt.core().is_paused(s.id));

    // Same when actually paused with a different lock.
    let lock = rt.core().alloc_lock_id();
    rt.core().pause(s.id, lock).expect("pause");
    let result = rt.core().resume(s.id, bogus).expect("resume bogus");
    assert!(result.is_none());
    assert!(
        rt.core().is_paused(s.id),
        "still paused — bogus didn't release"
    );
    assert!(rt.core().holds_pause_lock(s.id, lock));
}

#[test]
fn pause_buffer_cap_drops_oldest_and_reports_dropped() {
    // R2.6.0: buffer-cap eviction is part of the `ResumeAll` buffer
    // machinery — opt in (a Default leaf source has no buffer to cap).
    let rt = TestRuntime::new();
    rt.core().set_pause_buffer_cap(Some(2));
    let s = rt.state(Some(TestValue::Int(0)));
    rt.core()
        .set_pausable_mode(s.id, PausableMode::ResumeAll)
        .unwrap();
    let _rec = rt.subscribe_recorder(s.id);
    let lock = rt.core().alloc_lock_id();
    rt.core().pause(s.id, lock).expect("pause");

    s.set(TestValue::Int(1));
    s.set(TestValue::Int(2));
    s.set(TestValue::Int(3));
    s.set(TestValue::Int(4));
    s.set(TestValue::Int(5));
    // 5 DATAs emitted; cap=2, so 3 dropped from the front.

    let report = rt
        .core()
        .resume(s.id, lock)
        .expect("resume")
        .expect("final lock release yields a ResumeReport");
    assert_eq!(report.replayed, 2, "buffer holds at most cap=2");
    assert_eq!(report.dropped, 3, "3 oldest DATAs dropped");
    // Cache reflects the latest emission regardless (commit_emission updates
    // cache before queueing notification).
    assert_eq!(rt.cache_value(s.id), Some(TestValue::Int(5)));
}

#[test]
fn pause_does_not_buffer_unrelated_node() {
    // R2.6.0: s_a's buffered-while-paused expectation is the `ResumeAll`
    // contract — opt in. The "unrelated node not buffered" property under
    // test is mode-independent.
    let rt = TestRuntime::new();
    let s_a = rt.state(Some(TestValue::Int(0)));
    let s_b = rt.state(Some(TestValue::Int(0)));
    rt.core()
        .set_pausable_mode(s_a.id, PausableMode::ResumeAll)
        .unwrap();
    let rec_a = rt.subscribe_recorder(s_a.id);
    let rec_b = rt.subscribe_recorder(s_b.id);
    let baseline_b = rec_b.snapshot().len();

    let lock = rt.core().alloc_lock_id();
    rt.core().pause(s_a.id, lock).expect("pause a only");
    s_a.set(TestValue::Int(1));
    s_b.set(TestValue::Int(99));

    // s_b's DATA delivered immediately (s_b not paused).
    let b_data: Vec<i64> = rec_b
        .snapshot()
        .iter()
        .skip(baseline_b)
        .filter_map(|e| match e {
            RecordedEvent::Data(TestValue::Int(n)) => Some(*n),
            _ => None,
        })
        .collect();
    assert_eq!(b_data, vec![99]);

    // s_a's DATA still buffered.
    let a_data_count = rec_a
        .snapshot()
        .iter()
        .filter(|e| matches!(e, RecordedEvent::Data(TestValue::Int(1))))
        .count();
    assert_eq!(a_data_count, 0, "s_a DATA(1) should still be buffered");

    rt.core().resume(s_a.id, lock).expect("resume a");
}

// ---------------------------------------------------------------------
// R2.6.0 (QA B1) — the multi-pauser lock-set arithmetic and cross-node
// pause isolation are MODE-INDEPENDENT properties (`pause_state`
// add/remove + per-node buffer keying do not branch on `PausableMode`).
// The verbatim-buffer variants above were converted to `ResumeAll`
// (that IS the buffer contract), which would have left these
// mode-independent properties asserted ONLY under `ResumeAll`. These
// two tests restore Default-mode (the post-R2.6.0 common path)
// coverage: under Default a leaf source's self-emit delivers
// immediately (R2.6.0), so the lock arithmetic / isolation is verified
// against immediate delivery rather than buffered replay.
// ---------------------------------------------------------------------

#[test]
fn multi_pauser_lock_arithmetic_default_mode() {
    // Default mode (no set_pausable_mode). Lock-set arithmetic — count,
    // partial-release-stays-paused, final-release-yields-report — is
    // mode-independent. Under R2.6.0 a Default leaf source's self-emit
    // is delivered immediately while paused (NOT buffered), so the
    // final ResumeReport replays 0.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let rec = rt.subscribe_recorder(s.id);
    let baseline = rec.snapshot().len();

    let lock_a = rt.core().alloc_lock_id();
    let lock_b = rt.core().alloc_lock_id();
    let lock_c = rt.core().alloc_lock_id();

    rt.core().pause(s.id, lock_a).expect("pause a");
    rt.core().pause(s.id, lock_b).expect("pause b");
    rt.core().pause(s.id, lock_c).expect("pause c");
    assert_eq!(rt.core().pause_lock_count(s.id), 3);
    assert!(rt.core().is_paused(s.id));

    s.set(TestValue::Int(1));
    s.set(TestValue::Int(2));
    s.set(TestValue::Int(3));

    // R2.6.0: Default-mode leaf source self-emits deliver IMMEDIATELY
    // while self-paused — not buffered behind the lockset.
    let mid_data: Vec<i64> = rec
        .snapshot()
        .iter()
        .skip(baseline)
        .filter_map(|e| match e {
            RecordedEvent::Data(TestValue::Int(n)) => Some(*n),
            _ => None,
        })
        .collect();
    assert_eq!(
        mid_data,
        vec![1, 2, 3],
        "Default-mode self-emit delivered immediately while multi-paused (R2.6.0)"
    );

    // Lock arithmetic (mode-independent): partial release stays paused.
    let no_drain_b = rt.core().resume(s.id, lock_b).expect("resume b");
    assert!(no_drain_b.is_none(), "non-final resume returns None");
    let no_drain_c = rt.core().resume(s.id, lock_c).expect("resume c");
    assert!(no_drain_c.is_none());
    assert!(rt.core().is_paused(s.id), "still paused — lock_a held");
    assert_eq!(rt.core().pause_lock_count(s.id), 1);

    // Final release yields a ResumeReport; nothing was buffered (Default).
    let report = rt
        .core()
        .resume(s.id, lock_a)
        .expect("resume a")
        .expect("final lock release yields a ResumeReport");
    assert_eq!(
        report.replayed, 0,
        "Default mode buffers nothing — RESUME replays 0 (R2.6.0)"
    );
    assert_eq!(report.dropped, 0);
    assert!(!rt.core().is_paused(s.id));

    // No duplicate/replayed DATA after the final release.
    let final_data: Vec<i64> = rec
        .snapshot()
        .iter()
        .skip(baseline)
        .filter_map(|e| match e {
            RecordedEvent::Data(TestValue::Int(n)) => Some(*n),
            _ => None,
        })
        .collect();
    assert_eq!(
        final_data,
        vec![1, 2, 3],
        "no replay on final release in Default mode (R2.6.0)"
    );
    assert_eq!(rt.cache_value(s.id), Some(TestValue::Int(3)));
}

#[test]
fn pause_does_not_leak_to_unrelated_node_default_mode() {
    // Default mode (no set_pausable_mode). Pause isolation is per-node
    // and mode-independent: pausing s_a must not block/buffer s_b. Under
    // R2.6.0, Default-mode s_a's OWN self-emit also delivers immediately
    // (s_a is a depless leaf source) — so neither node is gated.
    let rt = TestRuntime::new();
    let s_a = rt.state(Some(TestValue::Int(0)));
    let s_b = rt.state(Some(TestValue::Int(0)));
    let rec_a = rt.subscribe_recorder(s_a.id);
    let rec_b = rt.subscribe_recorder(s_b.id);
    let baseline_a = rec_a.snapshot().len();
    let baseline_b = rec_b.snapshot().len();

    let lock = rt.core().alloc_lock_id();
    rt.core().pause(s_a.id, lock).expect("pause a only");
    assert!(rt.core().is_paused(s_a.id));
    assert!(!rt.core().is_paused(s_b.id), "s_b never paused");

    s_a.set(TestValue::Int(1));
    s_b.set(TestValue::Int(99));

    // s_b (unrelated, unpaused) delivers immediately — isolation holds.
    let b_data: Vec<i64> = rec_b
        .snapshot()
        .iter()
        .skip(baseline_b)
        .filter_map(|e| match e {
            RecordedEvent::Data(TestValue::Int(n)) => Some(*n),
            _ => None,
        })
        .collect();
    assert_eq!(b_data, vec![99], "unrelated node not blocked by s_a pause");

    // R2.6.0: s_a's OWN self-emit also delivered immediately under
    // Default mode (depless leaf source — no dep wave, no buffer).
    let a_data: Vec<i64> = rec_a
        .snapshot()
        .iter()
        .skip(baseline_a)
        .filter_map(|e| match e {
            RecordedEvent::Data(TestValue::Int(n)) => Some(*n),
            _ => None,
        })
        .collect();
    assert_eq!(
        a_data,
        vec![1],
        "Default-mode self-paused leaf source self-emit delivered immediately (R2.6.0)"
    );

    let report = rt.core().resume(s_a.id, lock).expect("resume a");
    assert!(
        report.is_none() || report.unwrap().replayed == 0,
        "Default mode replays nothing on resume (R2.6.0)"
    );
}

#[test]
fn equals_substituted_resolved_buffers_too() {
    // R2.6.0: RESOLVED buffering alongside DATA is the `ResumeAll`
    // contract — opt in.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(7)));
    rt.core()
        .set_pausable_mode(s.id, PausableMode::ResumeAll)
        .unwrap();
    let rec = rt.subscribe_recorder(s.id);
    let baseline = rec.snapshot().len();
    let lock = rt.core().alloc_lock_id();

    rt.core().pause(s.id, lock).expect("pause");
    // Same value as cache → equals-substitution → RESOLVED. Tier 3, buffered.
    s.set(TestValue::Int(7));
    s.set(TestValue::Int(7));

    let mid_resolved = rec
        .snapshot()
        .iter()
        .skip(baseline)
        .filter(|e| matches!(e, RecordedEvent::Resolved))
        .count();
    assert_eq!(mid_resolved, 0, "RESOLVED should buffer alongside DATA");

    let report = rt
        .core()
        .resume(s.id, lock)
        .expect("resume")
        .expect("final");
    assert_eq!(report.replayed, 2);

    let post_resolved = rec
        .snapshot()
        .iter()
        .skip(baseline)
        .filter(|e| matches!(e, RecordedEvent::Resolved))
        .count();
    assert_eq!(post_resolved, 2);
}

#[test]
fn unbounded_buffer_holds_many_emissions() {
    // R2.6.0: the unbounded buffer is the `ResumeAll` buffer — opt in.
    let rt = TestRuntime::new();
    // Default cap = None (unbounded).
    let s = rt.state(Some(TestValue::Int(0)));
    rt.core()
        .set_pausable_mode(s.id, PausableMode::ResumeAll)
        .unwrap();
    let _rec = rt.subscribe_recorder(s.id);
    let lock = rt.core().alloc_lock_id();
    rt.core().pause(s.id, lock).expect("pause");
    for i in 1..=1_000_i64 {
        s.set(TestValue::Int(i));
    }
    let report = rt
        .core()
        .resume(s.id, lock)
        .expect("resume")
        .expect("final");
    assert_eq!(report.replayed, 1_000);
    assert_eq!(report.dropped, 0);
}

#[test]
fn pause_unknown_node_returns_error() {
    let rt = TestRuntime::new();
    let bogus = graphrefly_core::NodeId::new(99_999);
    let lock = rt.core().alloc_lock_id();
    let result = rt.core().pause(bogus, lock);
    assert!(matches!(
        result,
        Err(graphrefly_core::PauseError::UnknownNode(_))
    ));
    let result = rt.core().resume(bogus, lock);
    assert!(matches!(
        result,
        Err(graphrefly_core::PauseError::UnknownNode(_))
    ));
}

#[test]
fn lock_ids_are_unique() {
    let rt = TestRuntime::new();
    let mut ids = Vec::new();
    for _ in 0..16 {
        ids.push(rt.core().alloc_lock_id());
    }
    let mut sorted = ids.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        ids.len(),
        "alloc_lock_id should produce unique ids"
    );
}

#[test]
fn pause_buffers_derived_data_through_diamond() {
    // Slice F audit close (2026-05-07): rewritten for `PausableMode::ResumeAll`.
    // Pre-rewrite this asserted ResumeAll semantics under what was then the
    // (incorrectly chosen) sole pause mode. With canonical §2.6 default mode
    // now the default, ResumeAll must be opted into. See the parallel
    // `pause_default_mode_consolidates_to_one_fn_fire_on_resume` for the
    // canonical-default case.
    use graphrefly_core::PausableMode;

    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(1)));
    let b = rt.derived(&[a.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(n * 2)),
        _ => panic!("type"),
    });
    let c = rt.derived(&[a.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(n * 3)),
        _ => panic!("type"),
    });
    let d_calls = Arc::new(std::sync::Mutex::new(0u32));
    let d_calls_inner = d_calls.clone();
    let d = rt.derived(&[b, c], move |deps| {
        *d_calls_inner.lock().unwrap() += 1;
        match (&deps[0], &deps[1]) {
            (TestValue::Int(bv), TestValue::Int(cv)) => Some(TestValue::Int(bv + cv)),
            _ => panic!("type"),
        }
    });
    // Opt into ResumeAll BEFORE subscribe so the activation wave runs
    // under the same semantics the rest of the test expects.
    rt.core()
        .set_pausable_mode(d, PausableMode::ResumeAll)
        .unwrap();
    let rec = rt.subscribe_recorder(d);

    // Initial activation: a=1 → b=2, c=3, d=5 (one fire).
    assert_eq!(*d_calls.lock().unwrap(), 1);
    assert_eq!(rt.cache_value(d), Some(TestValue::Int(5)));
    let baseline = rec.snapshot().len();

    let lock = rt.core().alloc_lock_id();
    rt.core().pause(d, lock).expect("pause d");

    a.set(TestValue::Int(10)); // → b=20, c=30, d=50 (one fire)
    a.set(TestValue::Int(100)); // → b=200, c=300, d=500 (one fire)

    // d's fn fires happened (cache advances), but d's outgoing DATA is buffered.
    assert_eq!(*d_calls.lock().unwrap(), 3, "fn fires happen mid-pause");
    let mid_data_count = rec
        .snapshot()
        .iter()
        .skip(baseline)
        .filter(|e| matches!(e, RecordedEvent::Data(_)))
        .count();
    assert_eq!(
        mid_data_count, 0,
        "d's outgoing DATA buffered while d is paused (ResumeAll mode)"
    );

    let report = rt.core().resume(d, lock).expect("resume").expect("final");
    assert_eq!(report.replayed, 2, "two waves replayed");
    assert_eq!(rt.cache_value(d), Some(TestValue::Int(500)));
}
