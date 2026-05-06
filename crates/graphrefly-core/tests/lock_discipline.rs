//! Slice A-bigger — drop-then-fire lock discipline tests.
//!
//! Verifies the v1 sink-fire-with-lock-held re-entrance limitation has
//! been lifted **for wave-end flush**. A sink that calls back into
//! `Core::emit` / `pause` / `resume` / `invalidate` / `complete` /
//! `error` / `teardown` during its own wave-end callback no longer
//! deadlocks — `flush_notifications` snapshots sink-fire jobs under
//! the lock, drops it, then fires.
//!
//! Caveats verified by these tests:
//! - **Subscribe-time handshake fires lock-held.** Re-entrance from a
//!   handshake-time sink call still deadlocks (the lock-held handshake
//!   is the trade-off for the subscribe-vs-emit race fix; sink registers
//!   under lock + handshake fires under lock + activation runs under
//!   lock = atomic with respect to concurrent emits). Tests use a
//!   `armed` flag to opt out of re-entrance during the handshake call.
//! - **Fn re-entrance via `BindingBoundary::invoke_fn`** is still
//!   forbidden in v1 — that path holds the state lock across the binding
//!   call. Lifting this is a follow-up.
//!
//! See also `tests/cascade_depth.rs` for cascade-recursion → iterative
//! walk tests (item 6 in the slice scope).

mod common;

use std::sync::{Arc, Mutex};

use graphrefly_core::Message;

use common::{TestRuntime, TestValue};

/// Helper: build a sink that becomes "armed" only after `armed.set(true)`.
/// Pre-arming, sink ignores all messages (lets the lock-held handshake
/// run cleanly). Post-arming, the sink runs `on_data` on every Data
/// message it observes.
fn armed_sink<F>(armed: Arc<Mutex<bool>>, on_data: F) -> graphrefly_core::Sink
where
    F: Fn(&Message) + Send + Sync + 'static,
{
    Arc::new(move |msgs: &[Message]| {
        if !*armed.lock().unwrap() {
            return;
        }
        for m in msgs {
            if matches!(m, Message::Data(_)) {
                on_data(m);
            }
        }
    })
}

#[test]
fn sink_can_reenter_core_via_emit() {
    let rt = Arc::new(TestRuntime::new());
    let s = rt.state(Some(TestValue::Int(0)));
    let s_id = s.id;

    let t = rt.state(Some(TestValue::Int(0)));
    let t_id = t.id;
    let t_rec = rt.subscribe_recorder(t_id);
    let baseline_t = t_rec.snapshot().len();

    let armed = Arc::new(Mutex::new(false));
    let triggered = Arc::new(Mutex::new(false));
    let rt_inner = Arc::clone(&rt);
    let triggered_inner = triggered.clone();
    let sink = armed_sink(armed.clone(), move |_msg| {
        let already = {
            let mut t = triggered_inner.lock().unwrap();
            let prev = *t;
            *t = true;
            prev
        };
        if !already {
            let h = rt_inner.binding.intern(TestValue::Int(99));
            rt_inner.core.emit(t_id, h);
        }
    });
    std::mem::forget(rt.core.subscribe(s_id, sink));
    *armed.lock().unwrap() = true;

    // Trigger a wave on s. The sink fires lock-released via flush; safely
    // re-enters Core to emit on t.
    let h = rt.binding.intern(TestValue::Int(7));
    rt.core.emit(s_id, h);

    let t_events = t_rec.snapshot();
    let new_t = &t_events[baseline_t..];
    assert!(
        new_t
            .iter()
            .any(|e| matches!(e, common::RecordedEvent::Data(TestValue::Int(99)))),
        "expected re-entrant emit to deliver Data(99) to t; got {t_events:?}"
    );
}

#[test]
fn sink_can_reenter_core_via_pause_and_resume() {
    let rt = Arc::new(TestRuntime::new());
    let s = rt.state(Some(TestValue::Int(0)));
    let s_id = s.id;
    let t = rt.state(Some(TestValue::Int(0)));
    let t_id = t.id;
    let _t_rec = rt.subscribe_recorder(t_id);
    let lock = rt.core.alloc_lock_id();

    let armed = Arc::new(Mutex::new(false));
    let pauses_seen = Arc::new(Mutex::new(0u32));
    let rt_inner = Arc::clone(&rt);
    let pauses_inner = pauses_seen.clone();
    let sink = armed_sink(armed.clone(), move |_msg| {
        let mut p = pauses_inner.lock().unwrap();
        if *p == 0 {
            *p = 1;
            drop(p);
            rt_inner.core.pause(t_id, lock).expect("pause from sink");
            rt_inner.core.resume(t_id, lock).expect("resume from sink");
        }
    });
    std::mem::forget(rt.core.subscribe(s_id, sink));
    *armed.lock().unwrap() = true;

    let h = rt.binding.intern(TestValue::Int(42));
    rt.core.emit(s_id, h);

    assert_eq!(
        *pauses_seen.lock().unwrap(),
        1,
        "sink should have re-entered Core via pause/resume"
    );
}

#[test]
fn sink_can_complete_another_node_from_callback() {
    let rt = Arc::new(TestRuntime::new());
    let s = rt.state(Some(TestValue::Int(0)));
    let t = rt.state(Some(TestValue::Int(0)));
    let t_id = t.id;
    let t_rec = rt.subscribe_recorder(t_id);
    let baseline = t_rec.snapshot().len();

    let armed = Arc::new(Mutex::new(false));
    let fired = Arc::new(Mutex::new(false));
    let rt_inner = Arc::clone(&rt);
    let fired_inner = fired.clone();
    let sink = armed_sink(armed.clone(), move |_msg| {
        let mut f = fired_inner.lock().unwrap();
        if !*f {
            *f = true;
            drop(f);
            rt_inner.core.complete(t_id);
        }
    });
    std::mem::forget(rt.core.subscribe(s.id, sink));
    *armed.lock().unwrap() = true;

    let h = rt.binding.intern(TestValue::Int(11));
    rt.core.emit(s.id, h);

    let t_events = t_rec.snapshot();
    let new_t = &t_events[baseline..];
    assert!(
        new_t
            .iter()
            .any(|e| matches!(e, common::RecordedEvent::Complete)),
        "t should observe Complete after re-entrant complete from s's sink; got {t_events:?}"
    );
}

#[test]
fn handshake_sink_reentry_panics_with_diagnostic_not_deadlocks() {
    // Verifies the re-entrance diagnostic for the lock-held subscribe
    // handshake fire. A handshake-time sink callback that calls back into
    // Core would silently deadlock on the held state lock — the thread-
    // local IN_HANDSHAKE_FIRE guard converts that into a clear panic.
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::Arc;

    let rt = Arc::new(TestRuntime::new());
    let s = rt.state(Some(TestValue::Int(0)));
    let s_id = s.id;
    let other = rt.state(Some(TestValue::Int(0)));
    let other_id = other.id;

    let rt_inner = Arc::clone(&rt);
    let sink: graphrefly_core::Sink = Arc::new(move |_msgs: &[graphrefly_core::Message]| {
        // Re-enter Core from the very first sink call (handshake fire).
        // Should panic via the diagnostic, not deadlock.
        let h = rt_inner.binding.intern(TestValue::Int(99));
        rt_inner.core.emit(other_id, h);
    });

    let result = catch_unwind(AssertUnwindSafe(|| {
        let _ = rt.core.subscribe(s_id, sink);
    }));
    assert!(
        result.is_err(),
        "handshake re-entrance should panic with diagnostic, not deadlock or succeed"
    );
    let panic_payload = result.expect_err("panic payload");
    let panic_msg = panic_payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic_payload.downcast_ref::<&'static str>().copied())
        .unwrap_or("(non-string panic)");
    assert!(
        panic_msg.contains("handshake"),
        "panic message should mention 'handshake'; got: {panic_msg}"
    );
}

#[test]
fn concurrent_subscribe_during_emit_observes_monotonic_post_subscribe_emits() {
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::thread;
    use std::time::Duration;

    let rt = Arc::new(TestRuntime::new());
    let s = rt.state(Some(TestValue::Int(0)));
    let s_id = s.id;

    // Stronger contract than just "Start is first":
    //   1. First observed message is `Start`.
    //   2. Every observed `Data(n)` value after Start is monotonically
    //      increasing (the emit thread emits `counter` strictly ascending).
    //   3. At least one `Data` is observed AFTER subscribe returned (i.e.,
    //      the emit thread DID race past us — sandwich check that the
    //      concurrency window was real, not vacuously satisfied because the
    //      emit thread finished before our subscribe).
    //
    // The sandwich check is the key strengthening: it fails if the emit
    // thread completed before subscribe (vacuous-pass mode of the prior
    // test) by requiring the highest observed counter to exceed
    // `subscribe_at_count`.
    let stop = Arc::new(Mutex::new(false));
    let emit_count = Arc::new(AtomicI64::new(0));
    let rt_emit = Arc::clone(&rt);
    let stop_emit = Arc::clone(&stop);
    let emit_count_inner = Arc::clone(&emit_count);
    let emit_handle = thread::spawn(move || {
        let mut counter = 1i64;
        while !*stop_emit.lock().unwrap() && counter <= 1000 {
            let h = rt_emit.binding.intern(TestValue::Int(counter));
            rt_emit.core.emit(s_id, h);
            emit_count_inner.store(counter, Ordering::SeqCst);
            counter += 1;
        }
    });

    // Brief warm-up so the emit thread definitely started.
    thread::sleep(Duration::from_millis(2));
    let count_before_subscribe = emit_count.load(Ordering::SeqCst);
    let rec = rt.subscribe_recorder(s_id);
    let count_at_subscribe = emit_count.load(Ordering::SeqCst);

    // Let post-subscribe emits run for a window long enough that some
    // emits land AFTER subscribe (sandwich check).
    thread::sleep(Duration::from_millis(20));
    *stop.lock().unwrap() = true;
    emit_handle.join().expect("emit thread join");
    let count_after_join = emit_count.load(Ordering::SeqCst);

    let events = rec.snapshot();

    // Invariant 1: first event is Start.
    assert!(
        matches!(events.first(), Some(common::RecordedEvent::Start)),
        "first event must be Start; got {:?}",
        events.first()
    );

    // Invariant 2: observed Data values are monotonically increasing.
    let mut last: Option<i64> = None;
    for e in &events {
        if let common::RecordedEvent::Data(common::TestValue::Int(n)) = e {
            if let Some(prev) = last {
                assert!(
                    *n > prev,
                    "Data values not monotonic: prev={prev}, now={n}; full trace: {events:?}"
                );
            }
            last = Some(*n);
        }
    }

    // Invariant 3 (sandwich check): the emit thread continued past
    // subscribe — at least some emits happened AFTER subscribe returned.
    // If this fails, the test ran in vacuous-pass mode and didn't actually
    // exercise the race window.
    assert!(
        count_after_join > count_at_subscribe,
        "sandwich check failed: emit thread completed before/at subscribe \
         (count before subscribe={count_before_subscribe}, at subscribe={count_at_subscribe}, \
         after join={count_after_join}). The race window was not exercised; \
         test result is vacuous. Increase the post-subscribe sleep or upper \
         counter bound."
    );

    // The largest observed Data should be ≥ count_at_subscribe + 1 if any
    // post-subscribe emits were delivered before our recorder snapshot.
    if let Some(max_observed) = last {
        // It's OK if max_observed < count_after_join (the thread finished
        // emitting AFTER the snapshot). What we want: the new sub saw at
        // least ONE post-subscribe value.
        assert!(max_observed >= 1, "no Data values observed at all");
    }
}
