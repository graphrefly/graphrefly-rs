//! Integration tests for `control` operators (Slice U).
//!
//! Operators: tap, tap_observer, on_first_data, rescue, valve, settle, repeat.

mod common;

use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::sync::Arc;

use graphrefly_operators::control::{
    on_first_data, repeat, rescue, settle, tap, tap_observer, valve,
};

use common::{OpRuntime, RecordedEvent, TestValue};

// =====================================================================
// tap — side-effect passthrough
// =====================================================================

#[test]
fn tap_forwards_data_and_calls_side_effect() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);

    let counter = Arc::new(AtomicI64::new(0));
    let c = counter.clone();
    let b = rt.binding.clone();
    let fn_id = rt.binding.register_tap(Box::new(move |h| {
        let v = b.deref(h);
        c.fetch_add(v.int(), Ordering::SeqCst);
    }));

    let tapped = tap(rt.core(), &rt.producer_binding, source, fn_id);
    let rec = rt.subscribe_recorder(tapped);

    rt.emit_int(source, 10);
    rt.emit_int(source, 20);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    assert_eq!(
        rec.data_values(),
        vec![TestValue::Int(10), TestValue::Int(20)]
    );
    assert_eq!(counter.load(Ordering::SeqCst), 30);
}

#[test]
fn tap_forwards_complete() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);

    let fn_id = rt.binding.register_tap(Box::new(|_| {}));
    let tapped = tap(rt.core(), &rt.producer_binding, source, fn_id);
    let rec = rt.subscribe_recorder(tapped);

    rt.emit_int(source, 1);
    rt.core().complete(source);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    assert_eq!(rec.data_values(), vec![TestValue::Int(1)]);
    assert!(rec.events().contains(&RecordedEvent::Complete));
}

#[test]
fn tap_forwards_error() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);

    let fn_id = rt.binding.register_tap(Box::new(|_| {}));
    let tapped = tap(rt.core(), &rt.producer_binding, source, fn_id);
    let rec = rt.subscribe_recorder(tapped);

    let err_h = rt.intern(TestValue::Str("boom".into()));
    rt.core().error(source, err_h);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    assert!(rec
        .events()
        .contains(&RecordedEvent::Error(TestValue::Str("boom".into()))));
}

// =====================================================================
// tap_observer — lifecycle observer callbacks
// =====================================================================

#[test]
fn tap_observer_calls_data_error_complete_callbacks() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);

    let data_count = Arc::new(AtomicU32::new(0));
    let complete_count = Arc::new(AtomicU32::new(0));

    let dc = data_count.clone();
    let data_fn = rt.binding.register_tap(Box::new(move |_| {
        dc.fetch_add(1, Ordering::SeqCst);
    }));
    let cc = complete_count.clone();
    let complete_fn = rt.binding.register_tap_complete(Box::new(move || {
        cc.fetch_add(1, Ordering::SeqCst);
    }));

    let observed = tap_observer(
        rt.core(),
        &rt.producer_binding,
        source,
        Some(data_fn),
        None,
        Some(complete_fn),
    );
    let rec = rt.subscribe_recorder(observed);

    rt.emit_int(source, 42);
    rt.core().complete(source);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    assert_eq!(rec.data_values(), vec![TestValue::Int(42)]);
    assert!(rec.events().contains(&RecordedEvent::Complete));
    assert_eq!(data_count.load(Ordering::SeqCst), 1);
    assert_eq!(complete_count.load(Ordering::SeqCst), 1);
}

#[test]
fn tap_observer_none_callbacks_still_forwards() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);

    let observed = tap_observer(rt.core(), &rt.producer_binding, source, None, None, None);
    let rec = rt.subscribe_recorder(observed);

    rt.emit_int(source, 5);
    rt.core().complete(source);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    assert_eq!(rec.data_values(), vec![TestValue::Int(5)]);
    assert!(rec.events().contains(&RecordedEvent::Complete));
}

// =====================================================================
// on_first_data — one-shot tap
// =====================================================================

#[test]
fn on_first_data_calls_tap_only_once() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);

    let counter = Arc::new(AtomicU32::new(0));
    let c = counter.clone();
    let fn_id = rt.binding.register_tap(Box::new(move |_| {
        c.fetch_add(1, Ordering::SeqCst);
    }));

    let node = on_first_data(rt.core(), &rt.producer_binding, source, fn_id);
    let rec = rt.subscribe_recorder(node);

    rt.emit_int(source, 1);
    rt.emit_int(source, 2);
    rt.emit_int(source, 3);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    // All data forwarded
    assert_eq!(
        rec.data_values(),
        vec![TestValue::Int(1), TestValue::Int(2), TestValue::Int(3)]
    );
    // Tap called only once
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[test]
fn on_first_data_forwards_complete_and_error() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);

    let fn_id = rt.binding.register_tap(Box::new(|_| {}));
    let node = on_first_data(rt.core(), &rt.producer_binding, source, fn_id);
    let rec = rt.subscribe_recorder(node);

    rt.core().complete(source);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    assert!(rec.events().contains(&RecordedEvent::Complete));
    assert!(rec.data_values().is_empty());
}

// =====================================================================
// rescue — error recovery
// =====================================================================

#[test]
fn rescue_recovers_from_error() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);

    let b = rt.binding.clone();
    let fn_id = rt.binding.register_rescue(Box::new(move |_err_h| {
        // Always recover with 999
        Ok(b.intern(TestValue::Int(999)))
    }));

    let rescued = rescue(rt.core(), &rt.producer_binding, source, fn_id);
    let rec = rt.subscribe_recorder(rescued);

    rt.emit_int(source, 1);
    let err_h = rt.intern(TestValue::Str("fail".into()));
    rt.core().error(source, err_h);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    assert_eq!(
        rec.data_values(),
        vec![TestValue::Int(1), TestValue::Int(999)]
    );
    // Should NOT have Error event since we recovered
    assert!(
        !rec.events()
            .iter()
            .any(|e| matches!(e, RecordedEvent::Error(_))),
        "should not see Error after recovery: {:?}",
        rec.events()
    );
}

#[test]
fn rescue_propagates_error_when_recovery_fails() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);

    let fn_id = rt.binding.register_rescue(Box::new(move |_| Err(())));

    let rescued = rescue(rt.core(), &rt.producer_binding, source, fn_id);
    let rec = rt.subscribe_recorder(rescued);

    let err_h = rt.intern(TestValue::Str("unrecoverable".into()));
    rt.core().error(source, err_h);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    assert!(rec.events().contains(&RecordedEvent::Error(TestValue::Str(
        "unrecoverable".into()
    ))));
}

#[test]
fn rescue_passes_data_through() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);

    let fn_id = rt.binding.register_rescue(Box::new(move |_| Err(())));
    let rescued = rescue(rt.core(), &rt.producer_binding, source, fn_id);
    let rec = rt.subscribe_recorder(rescued);

    rt.emit_int(source, 10);
    rt.emit_int(source, 20);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    assert_eq!(
        rec.data_values(),
        vec![TestValue::Int(10), TestValue::Int(20)]
    );
}

// =====================================================================
// valve — boolean gate
// =====================================================================

#[test]
fn valve_gates_data_by_control_signal() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let control = rt.state_int(None);

    // Gate predicate: value > 0 = open
    let b = rt.binding.clone();
    let pred_fn = rt
        .op_binding
        .register_predicate(Box::new(move |h| b.deref(h).int() > 0));

    let gated = valve(
        rt.core(),
        &rt.producer_binding,
        source,
        control,
        pred_fn,
        None,
    );
    let rec = rt.subscribe_recorder(gated);

    // Gate is closed by default
    rt.emit_int(source, 1); // dropped

    // Open gate
    rt.emit_int(control, 1);
    rt.emit_int(source, 2); // passes
    rt.emit_int(source, 3); // passes

    // Close gate
    rt.emit_int(control, 0);
    rt.emit_int(source, 4); // dropped

    // Re-open
    rt.emit_int(control, 1);
    rt.emit_int(source, 5); // passes
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    assert_eq!(
        rec.data_values(),
        vec![TestValue::Int(2), TestValue::Int(3), TestValue::Int(5)]
    );
}

#[test]
fn valve_forwards_source_complete() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let control = rt.state_int(None);

    let b = rt.binding.clone();
    let pred_fn = rt
        .op_binding
        .register_predicate(Box::new(move |h| b.deref(h).int() > 0));

    let gated = valve(
        rt.core(),
        &rt.producer_binding,
        source,
        control,
        pred_fn,
        None,
    );
    let rec = rt.subscribe_recorder(gated);

    rt.emit_int(control, 1); // open
    rt.emit_int(source, 10);
    rt.core().complete(source);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    assert_eq!(rec.data_values(), vec![TestValue::Int(10)]);
    assert!(rec.events().contains(&RecordedEvent::Complete));
}

#[test]
fn valve_control_error_terminates() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let control = rt.state_int(None);

    let b = rt.binding.clone();
    let pred_fn = rt
        .op_binding
        .register_predicate(Box::new(move |h| b.deref(h).int() > 0));

    let gated = valve(
        rt.core(),
        &rt.producer_binding,
        source,
        control,
        pred_fn,
        None,
    );
    let rec = rt.subscribe_recorder(gated);

    let err_h = rt.intern(TestValue::Str("ctrl_err".into()));
    rt.core().error(control, err_h);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    assert!(rec
        .events()
        .contains(&RecordedEvent::Error(TestValue::Str("ctrl_err".into()))));
}

#[test]
fn valve_cancellation_token_fires_on_close() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let control = rt.state_int(None);

    let b = rt.binding.clone();
    let pred_fn = rt
        .op_binding
        .register_predicate(Box::new(move |h| b.deref(h).int() > 0));

    let token = tokio_util::sync::CancellationToken::new();
    let gated = valve(
        rt.core(),
        &rt.producer_binding,
        source,
        control,
        pred_fn,
        Some(token.clone()),
    );
    let _rec = rt.subscribe_recorder(gated);

    // Open gate
    rt.emit_int(control, 1);
    assert!(!token.is_cancelled());

    // Close gate — should trigger cancel
    rt.emit_int(control, 0);
    assert!(token.is_cancelled());
}

// =====================================================================
// settle — wave convergence
// =====================================================================

#[test]
fn settle_completes_after_max_waves() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);

    let settled = settle(rt.core(), &rt.producer_binding, source, 10, Some(3));
    let rec = rt.subscribe_recorder(settled);

    rt.emit_int(source, 1);
    rt.emit_int(source, 2);
    rt.emit_int(source, 3);
    rt.emit_int(source, 4); // post-complete, should not appear

    assert_eq!(
        rec.data_values(),
        vec![TestValue::Int(1), TestValue::Int(2), TestValue::Int(3)]
    );
    assert!(rec.events().contains(&RecordedEvent::Complete));
}

#[test]
fn settle_forwards_complete_from_source() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);

    let settled = settle(rt.core(), &rt.producer_binding, source, 5, None);
    let rec = rt.subscribe_recorder(settled);

    rt.emit_int(source, 1);
    rt.core().complete(source);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    assert_eq!(rec.data_values(), vec![TestValue::Int(1)]);
    assert!(rec.events().contains(&RecordedEvent::Complete));
}

#[test]
fn settle_forwards_error() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);

    let settled = settle(rt.core(), &rt.producer_binding, source, 5, None);
    let rec = rt.subscribe_recorder(settled);

    let err_h = rt.intern(TestValue::Str("settle_err".into()));
    rt.core().error(source, err_h);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    assert!(rec
        .events()
        .contains(&RecordedEvent::Error(TestValue::Str("settle_err".into()))));
}

// =====================================================================
// repeat — sequential resubscribe
// =====================================================================

#[test]
fn repeat_zero_is_identity() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);

    let repeated = repeat(rt.core(), &rt.producer_binding, source, 0);
    let rec = rt.subscribe_recorder(repeated);

    rt.emit_int(source, 1);
    rt.core().complete(source);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    assert_eq!(rec.data_values(), vec![TestValue::Int(1)]);
    assert!(rec.events().contains(&RecordedEvent::Complete));
}

#[test]
fn repeat_forwards_error_immediately() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);

    let repeated = repeat(rt.core(), &rt.producer_binding, source, 5);
    let rec = rt.subscribe_recorder(repeated);

    let err_h = rt.intern(TestValue::Str("repeat_err".into()));
    rt.core().error(source, err_h);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    assert!(rec
        .events()
        .contains(&RecordedEvent::Error(TestValue::Str("repeat_err".into()))));
}

#[test]
fn repeat_data_passthrough() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);

    let repeated = repeat(rt.core(), &rt.producer_binding, source, 0);
    let rec = rt.subscribe_recorder(repeated);

    rt.emit_int(source, 10);
    rt.emit_int(source, 20);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    assert_eq!(
        rec.data_values(),
        vec![TestValue::Int(10), TestValue::Int(20)]
    );
}
