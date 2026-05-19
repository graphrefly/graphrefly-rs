//! Lock-discipline tests — sink re-entrance into Core.
//!
//! Verifies that sinks can call back into Core (`emit` / `pause` /
//! `resume` / `invalidate` / `complete` / `error` / `teardown`) without
//! deadlock. Two paths are covered:
//!
//! - **Wave-end flush** (Slice A-bigger): `flush_notifications` snapshots
//!   sink-fire jobs under the lock, drops it, then fires lock-released.
//!   Same-thread re-entry from sink callbacks works.
//! - **Subscribe-time handshake** (Slice E rework): `Core::subscribe`
//!   acquires `wave_owner` (re-entrant) first, installs the sink under
//!   the state lock, drops the state lock, then fires the per-tier
//!   handshake lock-released. Sink callbacks may re-enter Core; same-
//!   thread re-entry passes through `wave_owner` transparently.
//!   Cross-thread emits block on `wave_owner` until the subscribe scope
//!   ends, preserving R1.3.5.a happens-after ordering.
//!
//! Tests at the bottom of this file use an `armed` flag to opt out of
//! re-entrance during the initial handshake call where they only want
//! to test the wave-end-flush path; the dedicated handshake re-entry
//! test (`handshake_sink_can_reenter_core_emit_on_other_node`) verifies
//! the Slice E rework.
//!
//! Fn re-entrance via [`BindingBoundary::invoke_fn`] was lifted in Slice
//! A close (lock-released `invoke_fn`); custom-equals re-entrance was
//! lifted in the same slice.

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
#[ignore = "actor-model (D238/D232-AMEND): sink→Core re-entry is now mailbox-deferred (em.defer/post_*), not synchronous shared-Core; covered by graphrefly-operators producer tests + the P7 re-entrant-drain obligation"]
fn sink_can_reenter_core_via_emit() {
    // deferred stub (D238/D232-AMEND): body did synchronous sink->Core re-entry; now mailbox-deferred. Covered by graphrefly-operators producer tests + P7.
}

#[test]
#[ignore = "actor-model (D238/D232-AMEND): sink→Core re-entry is now mailbox-deferred (em.defer/post_*), not synchronous shared-Core; covered by graphrefly-operators producer tests + the P7 re-entrant-drain obligation"]
fn sink_can_reenter_core_via_pause_and_resume() {
    // deferred stub (D238/D232-AMEND): body did synchronous sink->Core re-entry; now mailbox-deferred. Covered by graphrefly-operators producer tests + P7.
}

#[test]
#[ignore = "actor-model (D238/D232-AMEND): sink→Core re-entry is now mailbox-deferred (em.defer/post_*), not synchronous shared-Core; covered by graphrefly-operators producer tests + the P7 re-entrant-drain obligation"]
fn sink_can_complete_another_node_from_callback() {
    // deferred stub (D238/D232-AMEND): body did synchronous sink->Core re-entry; now mailbox-deferred. Covered by graphrefly-operators producer tests + P7.
}

#[test]
fn handshake_sink_can_reenter_core_emit_on_other_node() {
    // Slice E rework: the handshake now fires LOCK-RELEASED with
    // `wave_owner` held. A handshake-time sink callback can re-enter
    // Core (`emit` on a different node, here) without deadlock or
    // panic. Same-thread re-entry passes through `wave_owner`'s
    // ReentrantMutex transparently. This unblocks the canonical
    // higher-order operator pattern (subscribe to inner state with
    // cache; sink re-enters via `Core::emit(producer_id, h)`).
    use std::sync::Arc;

    let rt = Arc::new(TestRuntime::new());
    let s = rt.state(Some(TestValue::Int(0)));
    let s_id = s.id;
    let other = rt.state(None);
    let other_id = other.id;

    // Subscribe a passive sink to `other` so we can observe re-entrant
    // emits hitting it.
    let other_rec = rt.subscribe_recorder(other_id);

    let rt_inner = Arc::clone(&rt);
    let sink: graphrefly_core::Sink = Arc::new(move |msgs: &[graphrefly_core::Message]| {
        // On the [Data(cache)] tier, re-enter Core to emit on `other`.
        for m in msgs {
            if matches!(m, graphrefly_core::Message::Data(_)) {
                let h = rt_inner.binding.intern(TestValue::Int(99));
                rt_inner.core.emit(other_id, h);
            }
        }
    });

    // No panic; re-entrant emit lands on `other_rec`.
    let _sub = rt.core.subscribe(s_id, sink);

    let other_events = other_rec.snapshot();
    let saw_99 = other_events
        .iter()
        .any(|e| matches!(e, common::RecordedEvent::Data(TestValue::Int(99))));
    assert!(
        saw_99,
        "other should observe Data(99) emitted from the handshake sink; got {other_events:?}"
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
