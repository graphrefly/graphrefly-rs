//! Resubscribable terminal lifecycle (canonical R2.2.7.a / R2.2.7.b, D118).
//!
//! Two policies side-by-side:
//! - **Non-resubscribable (default).** Terminal node + late subscriber →
//!   `try_subscribe` returns `Err(SubscribeError::TornDown)`; public
//!   `subscribe` panics with TornDown diagnostic. The stream is permanently
//!   over; the late subscriber is rejected rather than receiving a confusing
//!   replay of past lifecycle events.
//! - **Resubscribable.** Terminal node + late subscriber → node resets:
//!   `terminal` cleared, `has_fired_once` cleared, `has_received_teardown`
//!   cleared, dep_handles cleared, pause locks drained, replay buffer
//!   cleared. Subscriber gets a fresh `[START]`. **TEARDOWN does NOT
//!   block reset (D118)** — TEARDOWN is the cleanup signal of the prior
//!   activation cycle, not permanent destruction.

mod common;
use common::{RecordedEvent, TestRuntime, TestValue};
use graphrefly_core::SubscribeError;

#[test]
fn subscribe_to_non_resubscribable_completed_returns_torn_down_error() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(42)));
    rt.core().complete(s.id);

    // try_subscribe returns Err(TornDown).
    let sink: graphrefly_core::Sink = std::rc::Rc::new(|_msgs| {});
    match rt.core().try_subscribe(s.id, sink) {
        Err(SubscribeError::TornDown { node }) => assert_eq!(node, s.id),
        Ok(_) => panic!("expected TornDown, got Ok(_)"),
    }
}

#[test]
fn subscribe_to_non_resubscribable_completed_panics() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(42)));
    rt.core().complete(s.id);

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _rec = rt.subscribe_recorder(s.id);
    }));
    assert!(
        result.is_err(),
        "subscribe to non-resubscribable completed node must panic (R2.2.7.b)"
    );
}

#[test]
fn subscribe_to_non_resubscribable_errored_returns_torn_down_error() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(42)));
    let err = rt.binding.intern(TestValue::Str("boom".into()));
    rt.core().error(s.id, err);

    let sink: graphrefly_core::Sink = std::rc::Rc::new(|_msgs| {});
    match rt.core().try_subscribe(s.id, sink) {
        Err(SubscribeError::TornDown { node }) => assert_eq!(node, s.id),
        Ok(_) => panic!("expected TornDown, got Ok(_)"),
    }
}

#[test]
fn subscribe_to_non_resubscribable_torndown_returns_torn_down_error() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(5)));
    rt.core().teardown(s.id);

    let sink: graphrefly_core::Sink = std::rc::Rc::new(|_msgs| {});
    match rt.core().try_subscribe(s.id, sink) {
        Err(SubscribeError::TornDown { node }) => assert_eq!(node, s.id),
        Ok(_) => panic!("expected TornDown, got Ok(_)"),
    }
}

#[test]
fn resubscribable_late_subscriber_resets_terminal_state() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(42)));
    rt.core().set_resubscribable(s.id, true);
    rt.core().complete(s.id);
    assert!(!rt
        .core()
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
    rt.core().set_resubscribable(s.id, true);
    rt.core().complete(s.id);

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
    rt.core().set_resubscribable(b, true);
    let rec1 = rt.subscribe_recorder(b);
    assert_eq!(*calls.lock().unwrap(), 1);
    rt.unsub_recorder(&rec1); // release first subscription

    rt.core().complete(b);

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
        rt.core().set_resubscribable(s.id, true);
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
    rt.core().set_resubscribable(s.id, true);
    let rec = rt.subscribe_recorder(s.id);

    // Pause and emit some buffered DATAs.
    let lock = rt.core().alloc_lock_id();
    rt.core().pause(s.id, lock).expect("pause");
    s.set(TestValue::Int(1));
    s.set(TestValue::Int(2));
    // Terminate while paused; buffered messages won't replay.
    rt.core().complete(s.id);
    rt.unsub_recorder(&rec);

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
    assert!(!rt.core().is_paused(s.id), "pause state cleared on reset");
}

#[test]
fn resubscribable_resets_after_teardown() {
    // R2.2.7.a (D118, 2026-05-10): TEARDOWN is the cleanup signal of the
    // PREVIOUS activation cycle, not permanent destruction. A late
    // subscribe to a resubscribable + torn-down node DOES reset the
    // lifecycle — `terminal` cleared, `has_received_teardown` cleared,
    // pause/replay buffers cleared. The subscriber sees a clean fresh
    // `[Start]` (state cache survives per R2.2.8 ROM rule). The previous
    // implementation (Slice A+B F3) blocked reset on torn-down state;
    // that was over-defensive and is corrected by D118.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(42)));
    rt.core().set_resubscribable(s.id, true);
    rt.core().teardown(s.id);

    // Late subscribe — should reset because resubscribable.
    let rec = rt.subscribe_recorder(s.id);
    let snap = rec.snapshot();

    let has_complete = snap.iter().any(|e| matches!(e, RecordedEvent::Complete));
    let has_teardown = snap.iter().any(|e| matches!(e, RecordedEvent::Teardown));
    assert!(
        !has_complete,
        "resubscribable + torn-down: reset clears COMPLETE; subscriber sees fresh lifecycle"
    );
    assert!(
        !has_teardown,
        "resubscribable + torn-down: reset clears TEARDOWN; subscriber sees fresh lifecycle"
    );

    // State cache survives reset per R2.2.8 ROM rule.
    let has_data_42 = snap
        .iter()
        .any(|e| matches!(e, RecordedEvent::Data(TestValue::Int(42))));
    assert!(has_data_42, "state cache survives resubscribable reset");

    // Post-reset, the node accepts new emits in the fresh lifecycle.
    s.set(TestValue::Int(999));
    assert_eq!(
        rt.cache_value(s.id),
        Some(TestValue::Int(999)),
        "post-reset emit advances cache in the fresh lifecycle"
    );
}

#[test]
fn resubscribable_resets_after_any_terminal_including_teardown() {
    // Side-by-side: resubscribable resets after both COMPLETE and TEARDOWN.
    // R2.2.7.a (D118): the prior `!has_received_teardown` guard is removed.
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(1)));
    rt.core().set_resubscribable(a.id, true);
    rt.core().complete(a.id);
    let rec_a = rt.subscribe_recorder(a.id);
    let snap_a = rec_a.snapshot();
    let a_has_complete = snap_a.iter().any(|e| matches!(e, RecordedEvent::Complete));
    assert!(!a_has_complete, "complete: reset clears terminal");

    let b = rt.state(Some(TestValue::Int(1)));
    rt.core().set_resubscribable(b.id, true);
    rt.core().teardown(b.id);
    let rec_b = rt.subscribe_recorder(b.id);
    let snap_b = rec_b.snapshot();
    let b_has_complete = snap_b.iter().any(|e| matches!(e, RecordedEvent::Complete));
    let b_has_teardown = snap_b.iter().any(|e| matches!(e, RecordedEvent::Teardown));
    assert!(
        !b_has_complete,
        "teardown: reset clears COMPLETE (D118 — TEARDOWN no longer blocks reset)"
    );
    assert!(
        !b_has_teardown,
        "teardown: reset clears TEARDOWN (D118 — TEARDOWN no longer blocks reset)"
    );
}

#[test]
fn subscribe_to_non_resubscribable_terminated_with_invalidated_cache_returns_torn_down() {
    // R2.2.7.b: rejection applies regardless of cache state. A
    // non-resubscribable node that was invalidated then completed still
    // refuses subscribe — the stream is over.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(42)));
    rt.core().invalidate(s.id); // clears cache to NO_HANDLE
    rt.core().complete(s.id);

    let sink: graphrefly_core::Sink = std::rc::Rc::new(|_msgs| {});
    match rt.core().try_subscribe(s.id, sink) {
        Err(SubscribeError::TornDown { node }) => assert_eq!(node, s.id),
        Ok(_) => panic!("expected TornDown, got Ok(_)"),
    }
}
