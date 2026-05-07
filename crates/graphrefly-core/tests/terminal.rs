//! COMPLETE / ERROR — terminal lifecycle + Lock 2.B auto-cascade gating.
//!
//! Maps to canonical spec R1.3.4 (terminal lifecycle) and Lock 2.B
//! (auto-cascade COMPLETE/ERROR when all deps terminate; ERROR dominates
//! COMPLETE).

mod common;
use common::{RecordedEvent, TestRuntime, TestValue};

#[test]
fn complete_emits_complete_message() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(7)));
    let rec = rt.subscribe_recorder(s.id);
    let baseline = rec.snapshot().len();

    rt.core.complete(s.id);

    let post = rec.snapshot();
    let complete_count = post[baseline..]
        .iter()
        .filter(|e| matches!(e, RecordedEvent::Complete))
        .count();
    assert_eq!(complete_count, 1);
}

#[test]
fn error_emits_error_message_with_payload() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(7)));
    let rec = rt.subscribe_recorder(s.id);
    let baseline = rec.snapshot().len();

    let err_handle = rt.binding.intern(TestValue::Str("boom".into()));
    rt.core.error(s.id, err_handle);

    let post = rec.snapshot();
    let error_payloads: Vec<&TestValue> = post[baseline..]
        .iter()
        .filter_map(|e| match e {
            RecordedEvent::Error(v) => Some(v),
            _ => None,
        })
        .collect();
    assert_eq!(error_payloads.len(), 1);
    assert_eq!(error_payloads[0], &TestValue::Str("boom".into()));
}

#[test]
fn complete_is_idempotent() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(7)));
    let rec = rt.subscribe_recorder(s.id);
    let baseline = rec.snapshot().len();

    rt.core.complete(s.id);
    rt.core.complete(s.id);
    rt.core.complete(s.id);

    let count = rec.snapshot()[baseline..]
        .iter()
        .filter(|e| matches!(e, RecordedEvent::Complete))
        .count();
    assert_eq!(count, 1);
}

#[test]
fn emit_after_complete_is_silent_noop() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(7)));
    let _rec = rt.subscribe_recorder(s.id);

    rt.core.complete(s.id);
    let cache_after_complete = rt.cache_value(s.id);

    s.set(TestValue::Int(99));
    assert_eq!(
        rt.cache_value(s.id),
        cache_after_complete,
        "post-complete emit must not advance cache"
    );
}

#[test]
fn complete_cascades_to_single_dep_derived() {
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(10)));
    let b = rt.derived(&[a.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(n + 1)),
        _ => panic!("type"),
    });
    let rec_b = rt.subscribe_recorder(b);
    assert_eq!(rt.cache_value(b), Some(TestValue::Int(11)));
    let baseline = rec_b.snapshot().len();

    rt.core.complete(a.id);

    // Auto-cascade: B's only dep terminated → B auto-completes.
    let b_completes = rec_b.snapshot()[baseline..]
        .iter()
        .filter(|e| matches!(e, RecordedEvent::Complete))
        .count();
    assert_eq!(b_completes, 1, "B should auto-cascade COMPLETE");
}

#[test]
fn complete_does_not_cascade_with_partial_terminal_deps() {
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(10)));
    let c = rt.state(Some(TestValue::Int(100)));
    let b = rt.derived(&[a.id, c.id], |deps| match (&deps[0], &deps[1]) {
        (TestValue::Int(av), TestValue::Int(cv)) => Some(TestValue::Int(av + cv)),
        _ => panic!("type"),
    });
    let rec_b = rt.subscribe_recorder(b);
    let baseline = rec_b.snapshot().len();

    // Only A completes — C still live → B does NOT auto-cascade.
    rt.core.complete(a.id);
    let b_completes = rec_b.snapshot()[baseline..]
        .iter()
        .filter(|e| matches!(e, RecordedEvent::Complete))
        .count();
    assert_eq!(b_completes, 0);

    // C completes too → all B deps terminal → B auto-cascades.
    rt.core.complete(c.id);
    let b_completes = rec_b.snapshot()[baseline..]
        .iter()
        .filter(|e| matches!(e, RecordedEvent::Complete))
        .count();
    assert_eq!(b_completes, 1);
}

#[test]
fn error_dominates_complete_in_cascade_lock_2b() {
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(10)));
    let c = rt.state(Some(TestValue::Int(100)));
    let b = rt.derived(&[a.id, c.id], |deps| match (&deps[0], &deps[1]) {
        (TestValue::Int(av), TestValue::Int(cv)) => Some(TestValue::Int(av + cv)),
        _ => panic!("type"),
    });
    let rec_b = rt.subscribe_recorder(b);
    let baseline = rec_b.snapshot().len();

    rt.core.complete(a.id);
    let err = rt.binding.intern(TestValue::Str("boom".into()));
    rt.core.error(c.id, err);

    // B's deps: A=Complete, C=Error("boom") → ERROR dominates → B emits Error("boom").
    let post = rec_b.snapshot();
    let b_errors: Vec<&TestValue> = post[baseline..]
        .iter()
        .filter_map(|e| match e {
            RecordedEvent::Error(v) => Some(v),
            _ => None,
        })
        .collect();
    let b_completes = post[baseline..]
        .iter()
        .filter(|e| matches!(e, RecordedEvent::Complete))
        .count();
    assert_eq!(b_errors.len(), 1, "B should auto-cascade ERROR");
    assert_eq!(b_errors[0], &TestValue::Str("boom".into()));
    assert_eq!(
        b_completes, 0,
        "no COMPLETE when an ERROR is in the dep mix"
    );
}

#[test]
fn complete_cascades_through_diamond() {
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(1)));
    let b = rt.derived(&[a.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(n + 100)),
        _ => panic!("type"),
    });
    let c = rt.derived(&[a.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(n + 200)),
        _ => panic!("type"),
    });
    let d = rt.derived(&[b, c], |deps| match (&deps[0], &deps[1]) {
        (TestValue::Int(bv), TestValue::Int(cv)) => Some(TestValue::Int(bv + cv)),
        _ => panic!("type"),
    });
    let rec_d = rt.subscribe_recorder(d);
    let baseline = rec_d.snapshot().len();

    rt.core.complete(a.id);

    // B and C both auto-complete (their only dep, A, terminated). D's deps
    // (B, C) both terminal → D auto-completes.
    let d_completes = rec_d.snapshot()[baseline..]
        .iter()
        .filter(|e| matches!(e, RecordedEvent::Complete))
        .count();
    // D may receive multiple COMPLETE notifications — once via the
    // B-cascade path, once via the C-cascade path. Per-dep idempotency
    // gate (already-terminal early return) prevents the second visit from
    // re-emitting; the wire emission is exactly one.
    assert_eq!(d_completes, 1);
}

#[test]
fn complete_buffers_through_pause_only_for_buffered_tiers() {
    // Tier 5 (COMPLETE) is NOT buffered while paused — it bypasses pause
    // per spec § 1.3.7.b. Verify this end-to-end.
    //
    // Slice F (A3, 2026-05-07) tightening: when a paused node is terminated
    // (COMPLETE / ERROR), the dispatcher now drains the pause buffer and
    // releases payload retains as part of `terminate_node` — without this,
    // buffered DATA would (a) leak refcount shares forever, and (b) be
    // observable via resume AFTER the terminal had already fired, violating
    // the natural lifecycle order (no DATA after COMPLETE). The previous
    // version of this test asserted `replayed == 1` post-complete, which
    // exercised the bug; the new assertion is `replayed == 0` because the
    // buffer is drained by the terminal cascade.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let rec = rt.subscribe_recorder(s.id);
    let baseline = rec.snapshot().len();

    let lock = rt.core.alloc_lock_id();
    rt.core.pause(s.id, lock).expect("pause");

    s.set(TestValue::Int(1)); // tier 3 — buffered
    rt.core.complete(s.id); // tier 5 — bypasses buffer; ALSO drains buffer

    // Subscriber should have seen COMPLETE immediately even while paused.
    let mid_complete = rec.snapshot()[baseline..]
        .iter()
        .filter(|e| matches!(e, RecordedEvent::Complete))
        .count();
    assert_eq!(mid_complete, 1, "COMPLETE bypasses pause buffer");

    // After complete drained the buffer, the resume call sees the node is
    // no longer paused (terminate_node collapsed pause_state to Active).
    // Per `Core::resume` semantics, an unknown lockId on an unpaused node
    // returns `Ok(None)` — the lockset is empty, no final-resume report.
    let report = rt.core.resume(s.id, lock).expect("resume");
    assert!(
        report.is_none(),
        "node was unpaused by complete; resume is a no-op"
    );

    // No further DATA emissions after COMPLETE.
    let post_complete_data = rec.snapshot()[baseline..]
        .iter()
        .filter(|e| matches!(e, RecordedEvent::Data(_)))
        .count();
    assert_eq!(
        post_complete_data, 0,
        "no DATA can flow after the terminal cascade"
    );
}

#[test]
fn complete_drops_pending_fires() {
    // If a derived has a pending fire queued (e.g., dep DATA arrived but fn
    // didn't fire yet) and the derived terminates, fn must not fire after.
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(10)));
    let calls = std::sync::Arc::new(std::sync::Mutex::new(0u32));
    let calls_inner = calls.clone();
    let b = rt.derived(&[a.id], move |deps| {
        *calls_inner.lock().unwrap() += 1;
        match &deps[0] {
            TestValue::Int(n) => Some(TestValue::Int(*n)),
            _ => None,
        }
    });
    let _rec = rt.subscribe_recorder(b);
    let initial = *calls.lock().unwrap();

    rt.core.complete(b);
    a.set(TestValue::Int(99));
    assert_eq!(
        *calls.lock().unwrap(),
        initial,
        "post-complete dep update must not re-fire fn"
    );
}

#[test]
fn error_handle_retained_across_subscribers() {
    // Multiple subscribers should see the same error payload; the binding's
    // refcount discipline keeps the error value alive.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let rec1 = rt.subscribe_recorder(s.id);
    let rec2 = rt.subscribe_recorder(s.id);
    let baseline1 = rec1.snapshot().len();
    let baseline2 = rec2.snapshot().len();

    let err = rt.binding.intern(TestValue::Str("oops".into()));
    rt.core.error(s.id, err);

    let r1_err = rec1.snapshot()[baseline1..].iter().find_map(|e| match e {
        RecordedEvent::Error(v) => Some(v.clone()),
        _ => None,
    });
    let r2_err = rec2.snapshot()[baseline2..].iter().find_map(|e| match e {
        RecordedEvent::Error(v) => Some(v.clone()),
        _ => None,
    });
    assert_eq!(r1_err, Some(TestValue::Str("oops".into())));
    assert_eq!(r2_err, Some(TestValue::Str("oops".into())));
}

#[test]
#[should_panic(expected = "NO_HANDLE is not a valid ERROR payload")]
fn error_with_no_handle_panics() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    rt.core.error(s.id, graphrefly_core::NO_HANDLE);
}
