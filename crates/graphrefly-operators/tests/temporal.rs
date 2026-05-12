//! Integration tests for temporal operators (sample, debounce, throttle,
//! delay, audit, interval).
//!
//! Timer-dependent tests use `tokio::time::pause()` + `advance()` for
//! deterministic control. Multiple `yield_now()` calls ensure spawned
//! tasks get enough poll cycles.

mod common;

use std::time::Duration;

use common::{OpRuntime, RecordedEvent, TestValue};
use graphrefly_operators::temporal::{self, ThrottleOpts};

/// Yield multiple times to let spawned tasks process commands and timers.
async fn multi_yield(n: usize) {
    for _ in 0..n {
        tokio::task::yield_now().await;
    }
}

// =========================================================================
// sample
// =========================================================================

#[tokio::test]
async fn sample_emits_source_latest_on_notifier_data() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let notifier = rt.state_int(None);

    let sampled = temporal::sample(&rt.core, &rt.producer_binding, source, notifier);
    let rec = rt.subscribe_recorder(sampled);

    // Emit source values.
    rt.emit_int(source, 10);
    rt.emit_int(source, 20);

    // Notifier fires → should emit 20 (latest source value).
    rt.emit_int(notifier, 1);

    let data = rec.data_values();
    assert_eq!(data, vec![TestValue::Int(20)]);
}

#[tokio::test]
async fn sample_no_emit_if_source_completed() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let notifier = rt.state_int(None);

    let sampled = temporal::sample(&rt.core, &rt.producer_binding, source, notifier);
    let rec = rt.subscribe_recorder(sampled);

    rt.emit_int(source, 10);
    rt.core.complete(source);

    // Notifier fires after source complete → no emission.
    rt.emit_int(notifier, 1);

    let data = rec.data_values();
    assert!(data.is_empty(), "no data after source complete");
}

#[tokio::test]
async fn sample_completes_on_notifier_complete() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let notifier = rt.state_int(None);

    let sampled = temporal::sample(&rt.core, &rt.producer_binding, source, notifier);
    let rec = rt.subscribe_recorder(sampled);

    rt.emit_int(source, 10);
    rt.core.complete(notifier);

    assert!(
        rec.events().contains(&RecordedEvent::Complete),
        "sample should complete when notifier completes"
    );
}

// =========================================================================
// debounce
// =========================================================================

#[tokio::test]
async fn debounce_emits_after_quiet() {
    tokio::time::pause();

    let rt = OpRuntime::new();
    let source = rt.state_int(None);

    let debounced = temporal::debounce(&rt.core, &rt.producer_binding, source, 50);
    let rec = rt.subscribe_recorder(debounced);

    rt.emit_int(source, 10);
    multi_yield(5).await;

    // Before deadline — nothing emitted.
    tokio::time::advance(Duration::from_millis(30)).await;
    multi_yield(5).await;
    assert!(rec.data_values().is_empty(), "nothing before deadline");

    // Past deadline — should emit.
    tokio::time::advance(Duration::from_millis(25)).await;
    multi_yield(10).await;

    assert_eq!(rec.data_values(), vec![TestValue::Int(10)]);
}

#[tokio::test]
async fn debounce_resets_on_new_data() {
    tokio::time::pause();

    let rt = OpRuntime::new();
    let source = rt.state_int(None);

    let debounced = temporal::debounce(&rt.core, &rt.producer_binding, source, 50);
    let rec = rt.subscribe_recorder(debounced);

    rt.emit_int(source, 10);
    multi_yield(5).await;

    // Advance 30ms, send new data (resets timer).
    tokio::time::advance(Duration::from_millis(30)).await;
    multi_yield(5).await;
    rt.emit_int(source, 20);
    multi_yield(5).await;

    // 30ms more (60ms total, but only 30ms since last data).
    tokio::time::advance(Duration::from_millis(30)).await;
    multi_yield(5).await;
    assert!(rec.data_values().is_empty(), "timer reset, not yet fired");

    // 25ms more (55ms since second data).
    tokio::time::advance(Duration::from_millis(25)).await;
    multi_yield(10).await;

    assert_eq!(rec.data_values(), vec![TestValue::Int(20)], "emits latest");
}

#[tokio::test]
async fn debounce_flushes_on_complete() {
    tokio::time::pause();

    let rt = OpRuntime::new();
    let source = rt.state_int(None);

    let debounced = temporal::debounce(&rt.core, &rt.producer_binding, source, 50);
    let rec = rt.subscribe_recorder(debounced);

    rt.emit_int(source, 10);
    multi_yield(5).await;

    // Complete before timer fires — should flush pending.
    rt.core.complete(source);
    multi_yield(10).await;

    let events = rec.events();
    assert!(
        events.contains(&RecordedEvent::Data(TestValue::Int(10))),
        "pending value flushed on complete"
    );
    assert!(
        events.contains(&RecordedEvent::Complete),
        "complete propagated"
    );
}

// =========================================================================
// delay
// =========================================================================

#[tokio::test]
async fn delay_emits_after_duration() {
    tokio::time::pause();

    let rt = OpRuntime::new();
    let source = rt.state_int(None);

    let delayed = temporal::delay(&rt.core, &rt.producer_binding, source, 100);
    let rec = rt.subscribe_recorder(delayed);

    rt.emit_int(source, 42);
    multi_yield(5).await;

    // Not yet.
    tokio::time::advance(Duration::from_millis(50)).await;
    multi_yield(5).await;
    assert!(rec.data_values().is_empty());

    // Now.
    tokio::time::advance(Duration::from_millis(55)).await;
    multi_yield(10).await;

    assert_eq!(rec.data_values(), vec![TestValue::Int(42)]);
}

#[tokio::test]
async fn delay_multiple_in_flight() {
    tokio::time::pause();

    let rt = OpRuntime::new();
    let source = rt.state_int(None);

    let delayed = temporal::delay(&rt.core, &rt.producer_binding, source, 100);
    let rec = rt.subscribe_recorder(delayed);

    rt.emit_int(source, 1);
    multi_yield(5).await;

    tokio::time::advance(Duration::from_millis(30)).await;
    multi_yield(5).await;
    rt.emit_int(source, 2);
    multi_yield(5).await;

    // At 105ms: first fires.
    tokio::time::advance(Duration::from_millis(75)).await;
    multi_yield(10).await;
    assert_eq!(rec.data_values(), vec![TestValue::Int(1)]);

    // At 135ms: second fires.
    tokio::time::advance(Duration::from_millis(30)).await;
    multi_yield(10).await;
    assert_eq!(
        rec.data_values(),
        vec![TestValue::Int(1), TestValue::Int(2)]
    );
}

// =========================================================================
// audit
// =========================================================================

#[tokio::test]
async fn audit_emits_latest_after_window() {
    tokio::time::pause();

    let rt = OpRuntime::new();
    let source = rt.state_int(None);

    let audited = temporal::audit(&rt.core, &rt.producer_binding, source, 50);
    let rec = rt.subscribe_recorder(audited);

    // First DATA starts window.
    rt.emit_int(source, 10);
    multi_yield(5).await;

    // More data during window — updates latest, doesn't restart timer.
    tokio::time::advance(Duration::from_millis(20)).await;
    multi_yield(5).await;
    rt.emit_int(source, 20);
    multi_yield(5).await;

    // At 30ms since first DATA — still within 50ms window.
    tokio::time::advance(Duration::from_millis(10)).await;
    multi_yield(5).await;
    assert!(rec.data_values().is_empty(), "window hasn't closed yet");

    // At 55ms — window fires. Should emit 20 (latest).
    tokio::time::advance(Duration::from_millis(25)).await;
    multi_yield(10).await;

    assert_eq!(rec.data_values(), vec![TestValue::Int(20)]);
}

// =========================================================================
// throttle
// =========================================================================

#[tokio::test]
async fn throttle_leading_emits_first_then_drops() {
    tokio::time::pause();

    let rt = OpRuntime::new();
    let source = rt.state_int(None);

    let throttled = temporal::throttle(
        &rt.core,
        &rt.producer_binding,
        source,
        100,
        ThrottleOpts::default(), // leading: true, trailing: false
    );
    let rec = rt.subscribe_recorder(throttled);

    // First DATA — emitted immediately (leading edge).
    rt.emit_int(source, 1);
    multi_yield(5).await;
    assert_eq!(rec.data_values(), vec![TestValue::Int(1)]);

    // Second DATA within window — dropped (no trailing).
    rt.emit_int(source, 2);
    multi_yield(5).await;
    assert_eq!(
        rec.data_values(),
        vec![TestValue::Int(1)],
        "second value dropped"
    );

    // Window expires.
    tokio::time::advance(Duration::from_millis(105)).await;
    multi_yield(10).await;

    // Third DATA — new window, emitted immediately.
    rt.emit_int(source, 3);
    multi_yield(5).await;
    assert_eq!(
        rec.data_values(),
        vec![TestValue::Int(1), TestValue::Int(3)]
    );
}

#[tokio::test]
async fn throttle_trailing_emits_at_window_end() {
    tokio::time::pause();

    let rt = OpRuntime::new();
    let source = rt.state_int(None);

    let throttled = temporal::throttle(
        &rt.core,
        &rt.producer_binding,
        source,
        100,
        ThrottleOpts {
            leading: true,
            trailing: true,
        },
    );
    let rec = rt.subscribe_recorder(throttled);

    // First DATA — leading.
    rt.emit_int(source, 1);
    multi_yield(5).await;
    assert_eq!(rec.data_values(), vec![TestValue::Int(1)]);

    // More data during window — stored for trailing.
    rt.emit_int(source, 2);
    multi_yield(5).await;
    rt.emit_int(source, 3);
    multi_yield(5).await;

    // Window expires — trailing emits latest (3).
    tokio::time::advance(Duration::from_millis(105)).await;
    multi_yield(10).await;

    assert_eq!(
        rec.data_values(),
        vec![TestValue::Int(1), TestValue::Int(3)]
    );
}

// =========================================================================
// interval
// =========================================================================

#[tokio::test]
async fn interval_emits_incrementing_counter() {
    tokio::time::pause();

    let rt = OpRuntime::new();

    let iv = temporal::interval(&rt.core, &rt.producer_binding, 50);

    // Use a raw sink because interval emits HandleId::new(counter)
    // which isn't in the test binding's value registry.
    let emitted = std::sync::Arc::new(std::sync::Mutex::new(
        Vec::<graphrefly_core::HandleId>::new(),
    ));
    let em = emitted.clone();
    let _sub = rt.core.subscribe(
        iv,
        std::sync::Arc::new(move |msgs: &[graphrefly_core::Message]| {
            for &m in msgs {
                if let graphrefly_core::Message::Data(h) = m {
                    em.lock().unwrap().push(h);
                }
            }
        }),
    );

    // Let the interval task start and process the immediate first tick (skipped).
    multi_yield(10).await;

    // First real tick at 50ms.
    tokio::time::advance(Duration::from_millis(55)).await;
    multi_yield(20).await;

    // Second real tick at 100ms.
    tokio::time::advance(Duration::from_millis(55)).await;
    multi_yield(20).await;

    // Third real tick at 150ms.
    tokio::time::advance(Duration::from_millis(55)).await;
    multi_yield(20).await;

    let handles = emitted.lock().unwrap().clone();
    assert!(
        handles.len() >= 2,
        "expected at least 2 interval ticks, got {}",
        handles.len()
    );
    // Counter should be incrementing (starting at 1 to avoid NO_HANDLE).
    if handles.len() >= 2 {
        assert_ne!(handles[0], handles[1], "handles should be distinct");
    }
}

// =========================================================================
// Error propagation tests (/qa F6)
// =========================================================================

#[tokio::test]
async fn debounce_error_releases_pending() {
    tokio::time::pause();

    let rt = OpRuntime::new();
    let source = rt.state_int(None);

    let debounced = temporal::debounce(&rt.core, &rt.producer_binding, source, 50);
    let rec = rt.subscribe_recorder(debounced);

    rt.emit_int(source, 10);
    multi_yield(5).await;

    // Error before timer fires — pending should be released, error propagated.
    rt.core.error(source, rt.intern_int(99));
    multi_yield(10).await;

    let events = rec.events();
    assert!(
        events.contains(&RecordedEvent::Error(TestValue::Int(99))),
        "error should propagate"
    );
    // Should NOT have emitted the pending value 10.
    assert!(
        !events.contains(&RecordedEvent::Data(TestValue::Int(10))),
        "pending should not emit on error"
    );
}

#[tokio::test]
async fn delay_error_releases_all_pending() {
    tokio::time::pause();

    let rt = OpRuntime::new();
    let source = rt.state_int(None);

    let delayed = temporal::delay(&rt.core, &rt.producer_binding, source, 100);
    let rec = rt.subscribe_recorder(delayed);

    rt.emit_int(source, 1);
    multi_yield(5).await;
    rt.emit_int(source, 2);
    multi_yield(5).await;

    // Error cancels all pending delays.
    rt.core.error(source, rt.intern_int(99));
    multi_yield(10).await;

    let events = rec.events();
    assert!(
        events.contains(&RecordedEvent::Error(TestValue::Int(99))),
        "error should propagate"
    );
    assert!(
        events
            .iter()
            .filter(|e| matches!(e, RecordedEvent::Data(_)))
            .count()
            == 0,
        "no data should emit after error"
    );
}

#[tokio::test]
async fn sample_notifier_data_then_complete_in_batch() {
    // Regression test for /qa F4: notifier [Data, Complete] in single batch.
    // The sample must both emit the source value AND complete.
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let notifier = rt.state_int(None);

    let sampled = temporal::sample(&rt.core, &rt.producer_binding, source, notifier);
    let rec = rt.subscribe_recorder(sampled);

    rt.emit_int(source, 42);

    // Emit then complete on notifier in one batch.
    rt.core.batch(|| {
        rt.emit_int(notifier, 1);
        rt.core.complete(notifier);
    });

    let events = rec.events();
    assert!(
        events.contains(&RecordedEvent::Data(TestValue::Int(42))),
        "should emit source latest on notifier DATA"
    );
    assert!(
        events.contains(&RecordedEvent::Complete),
        "should complete after notifier completes"
    );
}
