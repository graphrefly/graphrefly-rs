//! Dispatcher integration tests — port of
//! `~/src/graphrefly-ts/src/__experiments__/handle-core/core.test.ts`.
//!
//! Each test maps to one or more invariants from the Phase 13.6.A canonical
//! spec / handle-protocol audit input. The names cite spec rules so a failing
//! test points at a concrete spec section.

use std::sync::Arc;

mod common;
use common::{RecordedEvent, TestObject, TestRuntime, TestValue};

// ---------------------------------------------------------------------------
// Identity dedup (Rule 1.3 — equals-substitution)
// ---------------------------------------------------------------------------

#[test]
fn emitting_same_primitive_twice_yields_one_data_one_resolved() {
    let rt = TestRuntime::new();
    let s = rt.state(None);
    let rec = rt.subscribe_recorder(s.id);
    s.set(TestValue::Int(42));
    s.set(TestValue::Int(42)); // same handle (interned) → RESOLVED
    let data = rec.data_values();
    assert_eq!(data, vec![TestValue::Int(42)]);
    // Sequence: START, DIRTY+DATA, DIRTY+RESOLVED.
    let snap = rec.snapshot();
    assert_eq!(
        snap,
        vec![
            RecordedEvent::Start,
            RecordedEvent::Dirty,
            RecordedEvent::Data(TestValue::Int(42)),
            RecordedEvent::Dirty,
            RecordedEvent::Resolved,
        ]
    );
}

#[test]
fn emitting_same_object_reference_twice_yields_one_data_one_resolved() {
    let rt = TestRuntime::new();
    let s = rt.state(None);
    let rec = rt.subscribe_recorder(s.id);
    let obj = Arc::new(TestObject {
        label: "x".into(),
        x: 1,
    });
    s.set(TestValue::Object(obj.clone()));
    s.set(TestValue::Object(obj.clone())); // same Arc::ptr_eq → same handle
    assert_eq!(rec.count(|e| matches!(e, RecordedEvent::Data(_))), 1);
    assert_eq!(rec.count(|e| matches!(e, RecordedEvent::Resolved)), 1);
}

#[test]
fn structurally_equal_distinct_objects_produce_two_data() {
    let rt = TestRuntime::new();
    let s = rt.state(None);
    let rec = rt.subscribe_recorder(s.id);
    s.set(TestValue::Object(Arc::new(TestObject {
        label: "x".into(),
        x: 1,
    })));
    s.set(TestValue::Object(Arc::new(TestObject {
        label: "x".into(),
        x: 1,
    })));
    // Distinct Arc → distinct handle → DATA both times (no deep equals by default).
    assert_eq!(rec.count(|e| matches!(e, RecordedEvent::Data(_))), 2);
    assert_eq!(rec.count(|e| matches!(e, RecordedEvent::Resolved)), 0);
}

// ---------------------------------------------------------------------------
// First-run gate (Rule 2.3 / P.1)
// ---------------------------------------------------------------------------

#[test]
fn derived_with_two_state_deps_does_not_fire_until_both_emit() {
    let rt = TestRuntime::new();
    let a = rt.state(None);
    let b = rt.state(None);
    let calls = Arc::new(std::sync::Mutex::new(0u32));
    let calls_inner = calls.clone();
    let sum = rt.derived(&[a.id, b.id], move |deps| {
        *calls_inner.lock().unwrap() += 1;
        let av = match &deps[0] {
            TestValue::Int(n) => *n,
            other => panic!("expected Int, got {other:?}"),
        };
        let bv = match &deps[1] {
            TestValue::Int(n) => *n,
            other => panic!("expected Int, got {other:?}"),
        };
        Some(TestValue::Int(av + bv))
    });
    let _rec = rt.subscribe_recorder(sum);
    assert_eq!(*calls.lock().unwrap(), 0);
    assert_eq!(rt.cache_value(sum), None);

    // Emit on a — gate still closed (b is sentinel).
    a.set(TestValue::Int(10));
    assert_eq!(*calls.lock().unwrap(), 0);

    // Emit on b — gate releases, fn fires.
    b.set(TestValue::Int(20));
    assert_eq!(*calls.lock().unwrap(), 1);
    assert_eq!(rt.cache_value(sum), Some(TestValue::Int(30)));
}

#[test]
fn derived_with_pre_initialized_state_deps_fires_on_first_subscribe() {
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(10)));
    let b = rt.state(Some(TestValue::Int(20)));
    let calls = Arc::new(std::sync::Mutex::new(0u32));
    let calls_inner = calls.clone();
    let sum = rt.derived(&[a.id, b.id], move |deps| {
        *calls_inner.lock().unwrap() += 1;
        match (&deps[0], &deps[1]) {
            (TestValue::Int(av), TestValue::Int(bv)) => Some(TestValue::Int(av + bv)),
            _ => panic!("type mismatch"),
        }
    });
    let rec = rt.subscribe_recorder(sum);
    // Both deps cached → push-on-subscribe → fn fires.
    assert_eq!(*calls.lock().unwrap(), 1);
    assert_eq!(rt.cache_value(sum), Some(TestValue::Int(30)));
    assert_eq!(rec.data_values(), vec![TestValue::Int(30)]);
}

// ---------------------------------------------------------------------------
// Diamond resolution (Spec §1.3.3)
// ---------------------------------------------------------------------------

#[test]
fn diamond_one_update_at_root_fires_d_once() {
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
    let _rec = rt.subscribe_recorder(d);
    // Initial: a=1 → b=2, c=3, d=5. d fires once.
    assert_eq!(*d_calls.lock().unwrap(), 1);
    assert_eq!(rt.cache_value(d), Some(TestValue::Int(5)));

    let before = *d_calls.lock().unwrap();
    a.set(TestValue::Int(10));
    // One update at root → ONE d fire (not two).
    assert_eq!(*d_calls.lock().unwrap() - before, 1);
    assert_eq!(rt.cache_value(d), Some(TestValue::Int(50))); // 20 + 30
}

#[test]
fn diamond_emitted_data_at_root_yields_one_data_at_sink() {
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
    let sum = rt.derived(&[b, c], |deps| match (&deps[0], &deps[1]) {
        (TestValue::Int(bv), TestValue::Int(cv)) => Some(TestValue::Int(bv + cv)),
        _ => panic!("type"),
    });
    let rec = rt.subscribe_recorder(sum);
    let initial_data = rec.count(|e| matches!(e, RecordedEvent::Data(_)));
    a.set(TestValue::Int(5));
    let final_data = rec.count(|e| matches!(e, RecordedEvent::Data(_)));
    // Exactly one new DATA after initial activation.
    assert_eq!(final_data - initial_data, 1);
    assert_eq!(rt.cache_value(sum), Some(TestValue::Int(310))); // (5+100)+(5+200)
}

// ---------------------------------------------------------------------------
// Push-on-subscribe (Rule 1.2 / 2.1)
// ---------------------------------------------------------------------------

#[test]
fn late_subscriber_to_state_with_cached_value_receives_data() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(99)));
    let rec = rt.subscribe_recorder(s.id);
    assert_eq!(rec.data_values(), vec![TestValue::Int(99)]);
}

#[test]
fn late_subscriber_to_sentinel_state_receives_only_start() {
    let rt = TestRuntime::new();
    let s = rt.state(None);
    let rec = rt.subscribe_recorder(s.id);
    assert_eq!(rec.data_values(), Vec::<TestValue>::new());
    assert_eq!(rec.snapshot(), vec![RecordedEvent::Start]);
}

// ---------------------------------------------------------------------------
// Custom equals (FFI-only when opted in)
// ---------------------------------------------------------------------------

#[test]
fn derived_with_custom_deep_equals_dedups_structurally_equal_outputs() {
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(1)));
    // Wrapper returns "small"/"big" Str depending on threshold.
    let wrapper = rt.derived_with_equals(
        &[a.id],
        |deps| match &deps[0] {
            TestValue::Int(n) => Some(TestValue::Str(if *n < 10 { "small" } else { "big" }.into())),
            _ => panic!("type"),
        },
        |x, y| match (x, y) {
            (TestValue::Str(a), TestValue::Str(b)) => a == b,
            _ => false,
        },
    );
    let rec = rt.subscribe_recorder(wrapper);
    let initial_data = rec.count(|e| matches!(e, RecordedEvent::Data(_)));
    assert_eq!(initial_data, 1); // initial activation

    a.set(TestValue::Int(2)); // still "small" — RESOLVED downstream
    a.set(TestValue::Int(3));
    let mid_data = rec.count(|e| matches!(e, RecordedEvent::Data(_)));
    assert_eq!(mid_data, initial_data); // no new DATA

    a.set(TestValue::Int(20)); // crosses threshold → "big" → DATA
    let final_data = rec.count(|e| matches!(e, RecordedEvent::Data(_)));
    assert_eq!(final_data, initial_data + 1);
}

// ---------------------------------------------------------------------------
// SENTINEL discipline (Rule M.4 / R1.2.4)
// ---------------------------------------------------------------------------

#[test]
#[should_panic(expected = "NO_HANDLE is not a valid DATA payload")]
fn emitting_no_handle_panics() {
    use graphrefly_core::NO_HANDLE;
    let rt = TestRuntime::new();
    let s = rt.state(None);
    // Bypass the binding's intern path to construct a NO_HANDLE emission directly.
    rt.core.emit(s.id, NO_HANDLE);
}

#[test]
fn null_is_a_valid_data_payload() {
    let rt = TestRuntime::new();
    let s = rt.state(None);
    let rec = rt.subscribe_recorder(s.id);
    s.set(TestValue::Null);
    assert_eq!(rec.data_values(), vec![TestValue::Null]);
}

// ---------------------------------------------------------------------------
// Handle release / refcount (memory hygiene)
// ---------------------------------------------------------------------------

#[test]
fn replacing_state_value_releases_old_handle() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));
    let before = rt.binding.live_handles();
    s.set(TestValue::Int(2));
    let after = rt.binding.live_handles();
    // Old handle for `1` released; new handle for `2` added. Net change: 0.
    assert_eq!(after, before);
}
