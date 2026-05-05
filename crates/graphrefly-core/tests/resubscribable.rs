//! Resubscribable terminal lifecycle (R2.2.7 / R2.5.3).
//!
//! Two policies side-by-side:
//! - **Non-resubscribable (default).** Terminal node + late subscriber →
//!   handshake includes the terminal so the subscriber sees `[START,
//!   DATA?, COMPLETE|ERROR]`. Stream stays terminated.
//! - **Resubscribable.** Terminal node + late subscriber → node resets:
//!   terminal cleared, has_fired_once cleared, dep_handles cleared,
//!   pause locks drained. Subscriber gets a fresh `[START]`.

mod common;
use common::{RecordedEvent, TestRuntime, TestValue};

#[test]
fn non_resubscribable_late_subscriber_sees_complete_in_handshake() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(42)));
    rt.core.complete(s.id);

    // Late subscribe — node already terminal.
    let rec = rt.subscribe_recorder(s.id);
    let snap = rec.snapshot();

    let has_start = snap.iter().any(|e| matches!(e, RecordedEvent::Start));
    let has_data_42 = snap
        .iter()
        .any(|e| matches!(e, RecordedEvent::Data(TestValue::Int(42))));
    let has_complete = snap.iter().any(|e| matches!(e, RecordedEvent::Complete));
    assert!(has_start);
    assert!(
        has_data_42,
        "non-resubscribable terminal node still pushes cache"
    );
    assert!(has_complete, "terminal replays in handshake");
}

#[test]
fn non_resubscribable_late_subscriber_sees_error_in_handshake() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(42)));
    let err = rt.binding.intern(TestValue::Str("boom".into()));
    rt.core.error(s.id, err);

    let rec = rt.subscribe_recorder(s.id);
    let snap = rec.snapshot();

    let error_payloads: Vec<&TestValue> = snap
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
fn resubscribable_late_subscriber_resets_terminal_state() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(42)));
    rt.core.set_resubscribable(s.id, true);
    rt.core.complete(s.id);
    assert!(!rt
        .core
        .holds_pause_lock(s.id, graphrefly_core::LockId::new(0)));

    // Late subscribe to resubscribable terminal node — should reset.
    let rec = rt.subscribe_recorder(s.id);
    let snap = rec.snapshot();

    // Should NOT see COMPLETE in the handshake — terminal was cleared.
    let has_complete = snap.iter().any(|e| matches!(e, RecordedEvent::Complete));
    assert!(
        !has_complete,
        "resubscribable: terminal cleared on resubscribe"
    );

    // Cache survives for state nodes — subscriber sees DATA(42).
    let has_data = snap
        .iter()
        .any(|e| matches!(e, RecordedEvent::Data(TestValue::Int(42))));
    assert!(has_data, "state cache survives reset");
}

#[test]
fn resubscribable_resumes_after_reset_accepts_new_emits() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(42)));
    rt.core.set_resubscribable(s.id, true);
    rt.core.complete(s.id);

    // First lifecycle dies; resubscribe → reset.
    let rec = rt.subscribe_recorder(s.id);
    let baseline = rec.snapshot().len();

    // Now emit again — should propagate.
    s.set(TestValue::Int(100));
    let snap = rec.snapshot();
    let has_new_data = snap[baseline..]
        .iter()
        .any(|e| matches!(e, RecordedEvent::Data(TestValue::Int(100))));
    assert!(has_new_data, "post-reset emit should propagate");
}

#[test]
fn resubscribable_clears_dep_handles_for_compute_nodes() {
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
    rt.core.set_resubscribable(b, true);
    let rec1 = rt.subscribe_recorder(b);
    assert_eq!(*calls.lock().unwrap(), 1);
    drop(rec1); // release first subscription

    rt.core.complete(b);

    // Resubscribe — reset clears dep_handles and has_fired_once. Compute
    // node's first-run gate re-closes; activation walks deps again; fn
    // re-fires.
    let _rec2 = rt.subscribe_recorder(b);
    assert!(
        *calls.lock().unwrap() >= 2,
        "fn re-fires post-resubscribe-reset on compute node"
    );
}

#[test]
fn set_resubscribable_after_subscribe_panics() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let _rec = rt.subscribe_recorder(s.id);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rt.core.set_resubscribable(s.id, true);
    }));
    assert!(
        result.is_err(),
        "set_resubscribable after first subscribe should panic"
    );
}

#[test]
fn resubscribable_resets_pause_state_drops_buffer() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    rt.core.set_resubscribable(s.id, true);
    let rec = rt.subscribe_recorder(s.id);

    // Pause and emit some buffered DATAs.
    let lock = rt.core.alloc_lock_id();
    rt.core.pause(s.id, lock).expect("pause");
    s.set(TestValue::Int(1));
    s.set(TestValue::Int(2));
    // Terminate while paused; buffered messages won't replay.
    rt.core.complete(s.id);
    drop(rec);

    // Resubscribe — reset drains pause locks and the buffer (no replay
    // because the buffer is cleared along with the lifecycle).
    let rec2 = rt.subscribe_recorder(s.id);
    let snap = rec2.snapshot();
    let buffered_data_post_reset = snap
        .iter()
        .filter(|e| matches!(e, RecordedEvent::Data(_)))
        .count();
    // Snapshot has START + DATA(cache) for state nodes.
    assert_eq!(
        buffered_data_post_reset, 1,
        "exactly one DATA from cache; buffered DATAs not replayed post-reset"
    );
    assert!(!rt.core.is_paused(s.id), "pause state cleared on reset");
}

#[test]
fn resubscribable_does_not_resurrect_after_teardown() {
    // F3 audit guard: TEARDOWN is permanent destruction (R2.6.4). Even on a
    // resubscribable node, a late subscribe after teardown does NOT reset
    // the lifecycle — the subscriber sees the full
    // [START, DATA?, COMPLETE, TEARDOWN] replay.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(42)));
    rt.core.set_resubscribable(s.id, true);
    rt.core.teardown(s.id);

    // Late subscribe — should NOT reset because the node was torn down.
    let rec = rt.subscribe_recorder(s.id);
    let snap = rec.snapshot();

    let has_complete = snap.iter().any(|e| matches!(e, RecordedEvent::Complete));
    let has_teardown = snap.iter().any(|e| matches!(e, RecordedEvent::Teardown));
    assert!(
        has_complete,
        "torn-down resubscribable node still replays COMPLETE in handshake"
    );
    assert!(
        has_teardown,
        "torn-down resubscribable node replays TEARDOWN in handshake"
    );

    // Verify post-teardown emits are still no-ops (node is permanently terminal).
    s.set(TestValue::Int(999));
    assert_eq!(
        rt.cache_value(s.id),
        Some(TestValue::Int(42)),
        "post-teardown emit must not advance cache"
    );
}

#[test]
fn resubscribable_resets_after_complete_but_not_after_teardown() {
    // Side-by-side: complete-then-resubscribe DOES reset; teardown-then-
    // resubscribe DOES NOT reset.
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(1)));
    rt.core.set_resubscribable(a.id, true);
    rt.core.complete(a.id);
    let rec_a = rt.subscribe_recorder(a.id);
    let snap_a = rec_a.snapshot();
    let a_has_complete = snap_a.iter().any(|e| matches!(e, RecordedEvent::Complete));
    assert!(!a_has_complete, "complete-only: reset clears terminal");

    let b = rt.state(Some(TestValue::Int(1)));
    rt.core.set_resubscribable(b.id, true);
    rt.core.teardown(b.id);
    let rec_b = rt.subscribe_recorder(b.id);
    let snap_b = rec_b.snapshot();
    let b_has_complete = snap_b.iter().any(|e| matches!(e, RecordedEvent::Complete));
    let b_has_teardown = snap_b.iter().any(|e| matches!(e, RecordedEvent::Teardown));
    assert!(
        b_has_complete,
        "teardown blocks reset; COMPLETE still replayed"
    );
    assert!(
        b_has_teardown,
        "teardown blocks reset; TEARDOWN still replayed"
    );
}

#[test]
fn non_resubscribable_late_subscriber_sees_only_terminal_when_cache_was_cleared() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(42)));
    rt.core.invalidate(s.id); // clears cache to NO_HANDLE
    rt.core.complete(s.id);

    let rec = rt.subscribe_recorder(s.id);
    let snap = rec.snapshot();

    let has_data = snap.iter().any(|e| matches!(e, RecordedEvent::Data(_)));
    let has_complete = snap.iter().any(|e| matches!(e, RecordedEvent::Complete));
    assert!(!has_data, "no DATA — cache was invalidated");
    assert!(has_complete, "still see terminal in handshake");
}
