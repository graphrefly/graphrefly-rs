//! Tests for cold synchronous sources (from_iter, of, empty, never,
//! throw_error). Verifies the producer-pattern lifecycle: build closure
//! fires on first subscribe, emits synchronously, deactivation cleans up.

mod common;

use common::{OpRuntime, RecordedEvent, TestValue};
use graphrefly_operators::{empty, from_iter, never, of, throw_error};

// =====================================================================
// from_iter / of
// =====================================================================

#[test]
fn from_iter_emits_each_handle_then_completes() {
    let rt = OpRuntime::new();
    let h1 = rt.intern_int(10);
    let h2 = rt.intern_int(20);
    let h3 = rt.intern_int(30);

    let node = from_iter(rt.core(), &rt.producer_binding, vec![h1, h2, h3]);
    let rec = rt.subscribe_recorder(node);

    // Multi-emit produces Dirty before Data (R1.3.1.b two-phase push).
    let events = rec.events();
    assert_eq!(
        events,
        vec![
            RecordedEvent::Start,
            RecordedEvent::Dirty,
            RecordedEvent::Data(TestValue::Int(10)),
            RecordedEvent::Data(TestValue::Int(20)),
            RecordedEvent::Data(TestValue::Int(30)),
            RecordedEvent::Complete,
        ]
    );
}

#[test]
fn of_emits_values_then_completes() {
    let rt = OpRuntime::new();
    let h1 = rt.intern_int(42);

    let node = of(rt.core(), &rt.producer_binding, vec![h1]);
    let rec = rt.subscribe_recorder(node);

    let events = rec.events();
    assert_eq!(
        events,
        vec![
            RecordedEvent::Start,
            RecordedEvent::Dirty,
            RecordedEvent::Data(TestValue::Int(42)),
            RecordedEvent::Complete,
        ]
    );
}

#[test]
fn from_iter_empty_vec_behaves_like_empty() {
    let rt = OpRuntime::new();

    let node = from_iter(rt.core(), &rt.producer_binding, vec![]);
    let rec = rt.subscribe_recorder(node);

    let events = rec.events();
    assert_eq!(events, vec![RecordedEvent::Start, RecordedEvent::Complete]);
}

// =====================================================================
// empty
// =====================================================================

#[test]
fn empty_completes_immediately() {
    let rt = OpRuntime::new();

    let node = empty(rt.core(), &rt.producer_binding);
    let rec = rt.subscribe_recorder(node);

    let events = rec.events();
    assert_eq!(events, vec![RecordedEvent::Start, RecordedEvent::Complete]);
}

// =====================================================================
// never
// =====================================================================

#[test]
fn never_emits_start_only() {
    let rt = OpRuntime::new();

    let node = never(rt.core(), &rt.producer_binding);
    let rec = rt.subscribe_recorder(node);

    let events = rec.events();
    assert_eq!(events, vec![RecordedEvent::Start]);
}

#[test]
fn never_cleanup_on_last_unsub() {
    let rt = OpRuntime::new();

    let node = never(rt.core(), &rt.producer_binding);
    let rec = rt.subscribe_recorder(node);

    // Only Start — no DATA, no terminal.
    assert_eq!(rec.events(), vec![RecordedEvent::Start]);

    // Drop the subscription — triggers producer_deactivate.
    drop(rec);

    // Producer storage should be cleaned up.
    let storage = rt.binding.producer_storage();
    let guard = storage.lock();
    assert!(
        !guard.contains_key(&node),
        "producer storage should be cleaned up after last unsub"
    );
}

// =====================================================================
// throw_error
// =====================================================================

#[test]
fn throw_error_emits_error_immediately() {
    let rt = OpRuntime::new();
    let err_handle = rt.intern_int(999);

    let node = throw_error(rt.core(), &rt.producer_binding, err_handle);
    let rec = rt.subscribe_recorder(node);

    let events = rec.events();
    assert_eq!(
        events,
        vec![
            RecordedEvent::Start,
            RecordedEvent::Error(TestValue::Int(999)),
        ]
    );
}

// =====================================================================
// Data extraction helpers
// =====================================================================

#[test]
fn from_iter_data_values_only() {
    let rt = OpRuntime::new();
    let h1 = rt.intern_int(1);
    let h2 = rt.intern_int(2);
    let h3 = rt.intern_int(3);

    let node = from_iter(rt.core(), &rt.producer_binding, vec![h1, h2, h3]);
    let rec = rt.subscribe_recorder(node);

    // data_values() strips lifecycle events.
    let data = rec.data_values();
    assert_eq!(
        data,
        vec![TestValue::Int(1), TestValue::Int(2), TestValue::Int(3)]
    );
}

#[test]
fn from_iter_single_element() {
    let rt = OpRuntime::new();
    let h = rt.intern_int(7);

    let node = from_iter(rt.core(), &rt.producer_binding, vec![h]);
    let rec = rt.subscribe_recorder(node);

    let data = rec.data_values();
    assert_eq!(data, vec![TestValue::Int(7)]);
}

// =====================================================================
// Composition with downstream operators
// =====================================================================

#[test]
fn empty_into_take_completes_with_no_data() {
    let rt = OpRuntime::new();

    let source = empty(rt.core(), &rt.producer_binding);
    let take_reg = graphrefly_operators::take(rt.core(), source, 5);
    let rec = rt.subscribe_recorder(take_reg.node);

    let events = rec.events();
    assert_eq!(events, vec![RecordedEvent::Start, RecordedEvent::Complete]);
}
