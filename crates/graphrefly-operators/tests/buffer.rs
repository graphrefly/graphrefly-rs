//! Integration tests for `buffer` operators (Slice U).
//!
//! Operators: buffer, buffer_count, window, window_count.

mod common;

use graphrefly_operators::buffer::{buffer, buffer_count, window, window_count};

use common::{OpRuntime, RecordedEvent, TestValue};

// =====================================================================
// buffer_count — fixed-size buffer
// =====================================================================

#[test]
fn buffer_count_emits_packed_array_at_count() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let pack_fn = rt.register_tuple_packer();

    let buffered = buffer_count(&rt.core, &rt.producer_binding, source, 3, pack_fn);
    let rec = rt.subscribe_recorder(buffered);

    rt.emit_int(source, 1);
    rt.emit_int(source, 2);
    rt.emit_int(source, 3);

    let values = rec.data_values();
    assert_eq!(values.len(), 1);
    assert_eq!(
        values[0].clone().tuple(),
        vec![TestValue::Int(1), TestValue::Int(2), TestValue::Int(3)]
    );
}

#[test]
fn buffer_count_flushes_remainder_on_complete() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let pack_fn = rt.register_tuple_packer();

    let buffered = buffer_count(&rt.core, &rt.producer_binding, source, 5, pack_fn);
    let rec = rt.subscribe_recorder(buffered);

    rt.emit_int(source, 10);
    rt.emit_int(source, 20);
    rt.core.complete(source);

    let values = rec.data_values();
    assert_eq!(values.len(), 1, "should flush remainder: {:?}", values);
    assert_eq!(
        values[0].clone().tuple(),
        vec![TestValue::Int(10), TestValue::Int(20)]
    );
    assert!(rec.events().contains(&RecordedEvent::Complete));
}

#[test]
fn buffer_count_multiple_flushes() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let pack_fn = rt.register_tuple_packer();

    let buffered = buffer_count(&rt.core, &rt.producer_binding, source, 2, pack_fn);
    let rec = rt.subscribe_recorder(buffered);

    rt.emit_int(source, 1);
    rt.emit_int(source, 2);
    rt.emit_int(source, 3);
    rt.emit_int(source, 4);

    let values = rec.data_values();
    assert_eq!(values.len(), 2);
    assert_eq!(
        values[0].clone().tuple(),
        vec![TestValue::Int(1), TestValue::Int(2)]
    );
    assert_eq!(
        values[1].clone().tuple(),
        vec![TestValue::Int(3), TestValue::Int(4)]
    );
}

#[test]
fn buffer_count_error_releases_and_terminates() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let pack_fn = rt.register_tuple_packer();

    let buffered = buffer_count(&rt.core, &rt.producer_binding, source, 5, pack_fn);
    let rec = rt.subscribe_recorder(buffered);

    rt.emit_int(source, 1);
    rt.emit_int(source, 2);
    let err_h = rt.intern(TestValue::Str("buf_err".into()));
    rt.core.error(source, err_h);

    // No data flushed (error discards buffer)
    assert!(rec.data_values().is_empty());
    assert!(rec
        .events()
        .contains(&RecordedEvent::Error(TestValue::Str("buf_err".into()))));
}

#[test]
fn buffer_count_empty_on_complete_no_flush() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let pack_fn = rt.register_tuple_packer();

    let buffered = buffer_count(&rt.core, &rt.producer_binding, source, 5, pack_fn);
    let rec = rt.subscribe_recorder(buffered);

    rt.core.complete(source);

    // No data buffered, so no flush — just complete
    assert!(rec.data_values().is_empty());
    assert!(rec.events().contains(&RecordedEvent::Complete));
}

// =====================================================================
// buffer — notifier-triggered flush
// =====================================================================

#[test]
fn buffer_flushes_on_notifier() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let notifier = rt.state_int(None);
    let pack_fn = rt.register_tuple_packer();

    let buffered = buffer(&rt.core, &rt.producer_binding, source, notifier, pack_fn);
    let rec = rt.subscribe_recorder(buffered);

    rt.emit_int(source, 1);
    rt.emit_int(source, 2);
    // Trigger flush
    rt.emit_int(notifier, 0);

    let values = rec.data_values();
    assert_eq!(values.len(), 1);
    assert_eq!(
        values[0].clone().tuple(),
        vec![TestValue::Int(1), TestValue::Int(2)]
    );
}

#[test]
fn buffer_empty_notifier_noop() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let notifier = rt.state_int(None);
    let pack_fn = rt.register_tuple_packer();

    let buffered = buffer(&rt.core, &rt.producer_binding, source, notifier, pack_fn);
    let rec = rt.subscribe_recorder(buffered);

    // Notifier fires with empty buffer — should be no-op
    rt.emit_int(notifier, 0);

    assert!(rec.data_values().is_empty());
}

#[test]
fn buffer_source_complete_flushes_remainder() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let notifier = rt.state_int(None);
    let pack_fn = rt.register_tuple_packer();

    let buffered = buffer(&rt.core, &rt.producer_binding, source, notifier, pack_fn);
    let rec = rt.subscribe_recorder(buffered);

    rt.emit_int(source, 10);
    rt.emit_int(source, 20);
    rt.core.complete(source);

    let values = rec.data_values();
    assert_eq!(values.len(), 1, "should flush on complete: {:?}", values);
    assert_eq!(
        values[0].clone().tuple(),
        vec![TestValue::Int(10), TestValue::Int(20)]
    );
    assert!(rec.events().contains(&RecordedEvent::Complete));
}

#[test]
fn buffer_notifier_error_terminates() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let notifier = rt.state_int(None);
    let pack_fn = rt.register_tuple_packer();

    let buffered = buffer(&rt.core, &rt.producer_binding, source, notifier, pack_fn);
    let rec = rt.subscribe_recorder(buffered);

    rt.emit_int(source, 1);
    let err_h = rt.intern(TestValue::Str("not_err".into()));
    rt.core.error(notifier, err_h);

    // Buffer discarded on notifier error
    assert!(rec.data_values().is_empty());
    assert!(rec
        .events()
        .contains(&RecordedEvent::Error(TestValue::Str("not_err".into()))));
}

// =====================================================================
// window_count — count-based sub-node splitting
// =====================================================================

#[test]
fn window_count_emits_inner_nodes() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);

    let windowed = window_count(&rt.core, &rt.producer_binding, source, 2);
    let rec = rt.subscribe_recorder(windowed);

    // First window emitted at activation
    rt.emit_int(source, 1);
    rt.emit_int(source, 2); // window full → complete old, emit new

    let values = rec.data_values();
    // Should have at least 2 window handles (first at activation + second after count)
    assert!(
        values.len() >= 2,
        "expected >=2 window handles, got {:?}",
        values
    );
}

#[test]
fn window_count_complete_closes_current_window() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);

    let windowed = window_count(&rt.core, &rt.producer_binding, source, 5);
    let rec = rt.subscribe_recorder(windowed);

    rt.emit_int(source, 1);
    rt.core.complete(source);

    // Should see at least 1 window handle + Complete
    assert!(!rec.data_values().is_empty());
    assert!(rec.events().contains(&RecordedEvent::Complete));
}

// =====================================================================
// window — notifier-triggered sub-node splitting
// =====================================================================

#[test]
fn window_emits_new_inner_on_notifier() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let notifier = rt.state_int(None);

    let windowed = window(&rt.core, &rt.producer_binding, source, notifier);
    let rec = rt.subscribe_recorder(windowed);

    // First window emitted at activation
    rt.emit_int(source, 1);

    // Notifier triggers new window
    rt.emit_int(notifier, 0);

    let values = rec.data_values();
    assert!(
        values.len() >= 2,
        "expected >=2 window handles, got {:?}",
        values
    );
}

#[test]
fn window_source_complete_closes_and_completes() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let notifier = rt.state_int(None);

    let windowed = window(&rt.core, &rt.producer_binding, source, notifier);
    let rec = rt.subscribe_recorder(windowed);

    rt.emit_int(source, 1);
    rt.core.complete(source);

    assert!(!rec.data_values().is_empty());
    assert!(rec.events().contains(&RecordedEvent::Complete));
}
