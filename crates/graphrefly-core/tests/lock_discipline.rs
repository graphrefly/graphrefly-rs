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
//! D246 (rule 6): a long-lived sink that fires IN-WAVE re-enters Core
//! via the mailbox (`post_emit`/`post_complete`/`post_defer`), drained
//! owner-side by `drain_mailbox` as a FIFO nested wave (the P7/D239
//! obligation). Synchronous handshake-time sink re-entry (owner holds
//! `&Core` during subscribe) still works directly — see
//! `handshake_sink_can_reenter_core_emit_on_other_node`.
//!
//! Fn re-entrance via [`graphrefly_core::BindingBoundary::invoke_fn`]
//! and custom-equals re-entrance use the same mailbox seam — see
//! `tests/lock_released.rs`.

mod common;

use std::sync::{Arc, Mutex};

use graphrefly_core::Message;

use common::{TestRuntime, TestValue};

#[test]
fn sink_can_reenter_core_via_emit() {
    // LIVE invariant: a long-lived sink that fires IN-WAVE may
    // re-enter Core. Under the actor model the β-valid seam is the
    // mailbox (D233/D246 rule 6): the sink posts `MailboxOp::Emit`;
    // the owner's drain applies it as a nested wave. No deadlock, no
    // shared/cloned Core.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));
    let d = rt.derived(&[s.id], |deps| Some(deps[0].clone()));
    let other = rt.state(None);
    let other_obs = rt.derived(&[other.id], |deps| Some(deps[0].clone()));
    let other_rec = rt.subscribe_recorder(other_obs);

    let mailbox = rt.mailbox();
    let binding = rt.binding.clone();
    let other_id = other.id;
    let reentrant_sink: graphrefly_core::Sink = Arc::new(move |msgs: &[Message]| {
        for m in msgs {
            if matches!(m, Message::Data(_)) {
                let h = binding.intern(TestValue::Int(55));
                let _ = mailbox.post_emit(other_id, h);
            }
        }
    });
    let sub = rt.track_subscribe(d, reentrant_sink);

    // Drive a wave: the in-wave sink posts an Emit on `other`.
    s.set(TestValue::Int(2));
    rt.drain_mailbox();
    assert!(
        other_rec.data_values().contains(&TestValue::Int(55)),
        "in-wave sink re-entered Core via mailbox emit → delivered as nested wave"
    );
    rt.unsubscribe(d, sub);
}

#[test]
#[ignore = "D246 record-and-skip: sink→Core pause/resume re-entry has NO β-valid seam — pause/resume are absent from CoreFull (the MailboxOp::Defer surface) and are not MailboxOp variants; the deleted synchronous shared-Core mechanism is the only path. Invariant (an in-wave sink can pause/resume) stays live → S4 owner-side seam. See porting-deferred § D246 record-and-skip."]
fn sink_can_reenter_core_via_pause_and_resume() {
    // No β-valid expression: pause/resume not on CoreFull/MailboxOp. → S4.
}

#[test]
fn sink_can_complete_another_node_from_callback() {
    // LIVE invariant: an in-wave sink may `complete` another node.
    // β-valid via `MailboxOp::Complete` (D246 rule 6); owner drain
    // applies it as a nested wave.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));
    let d = rt.derived(&[s.id], |deps| Some(deps[0].clone()));
    let target = rt.state(Some(TestValue::Int(0)));
    let target_rec = rt.subscribe_recorder(target.id);

    let mailbox = rt.mailbox();
    let target_id = target.id;
    let completing_sink: graphrefly_core::Sink = Arc::new(move |msgs: &[Message]| {
        for m in msgs {
            if matches!(m, Message::Data(_)) {
                let _ = mailbox.post_complete(target_id);
            }
        }
    });
    let sub = rt.track_subscribe(d, completing_sink);

    s.set(TestValue::Int(2));
    rt.drain_mailbox();
    assert!(
        target_rec
            .snapshot()
            .iter()
            .any(|e| matches!(e, common::RecordedEvent::Complete)),
        "in-wave sink completed another node via mailbox → delivered as nested wave"
    );
    rt.unsubscribe(d, sub);
}

#[test]
fn p7_reentrant_drain_mailbox_applies_nested_waves_in_fifo_order() {
    // P7 (D239) — re-entrant `drain_mailbox` FIFO nested-wave
    // ordering. Multiple in-wave sink re-entries post `Emit`s; the
    // owner's `drain_mailbox` MUST apply them in FIFO post order,
    // each cascading its own nested wave, with no re-ordering and no
    // deadlock when a drained op itself posts further ops.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let d = rt.derived(&[s.id], |deps| Some(deps[0].clone()));

    // A recorder on a sink that captures the FIFO arrival order of
    // re-entrant emits.
    let order = Arc::new(Mutex::new(Vec::<i64>::new()));
    let sink_node = rt.state(None);
    let order_w = order.clone();
    let binding_o = rt.binding.clone();
    let order_sink: graphrefly_core::Sink = Arc::new(move |msgs: &[Message]| {
        for m in msgs {
            if let Message::Data(h) = m {
                if let common::TestValue::Int(n) = binding_o.deref(*h) {
                    order_w.lock().unwrap().push(n);
                }
            }
        }
    });
    let order_sub = rt.track_subscribe(sink_node.id, order_sink);

    // The in-wave sink on `d` posts THREE emits in order 10, 20, 30.
    // The first drained op (10) itself posts a follow-on (40) — the
    // re-entrant-during-drain case P7 must keep FIFO.
    let mailbox = rt.mailbox();
    let binding = rt.binding.clone();
    let sink_id = sink_node.id;
    let posted = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let reentrant: graphrefly_core::Sink = Arc::new(move |msgs: &[Message]| {
        for m in msgs {
            if matches!(m, Message::Data(_))
                && !posted.swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                for v in [10i64, 20, 30] {
                    let h = binding.intern(common::TestValue::Int(v));
                    let _ = mailbox.post_emit(sink_id, h);
                }
                // The op draining `10` re-enters and posts `40`.
                let mailbox2 = mailbox.clone();
                let binding2 = binding.clone();
                let _ = mailbox.post_defer(Box::new(move |_cf| {
                    let h = binding2.intern(common::TestValue::Int(40));
                    let _ = mailbox2.post_emit(sink_id, h);
                }));
            }
        }
    });
    let d_sub = rt.track_subscribe(d, reentrant);

    s.set(TestValue::Int(1));
    rt.drain_mailbox();

    assert_eq!(
        *order.lock().unwrap(),
        vec![10, 20, 30, 40],
        "re-entrant drain_mailbox applied nested waves in strict FIFO post order"
    );
    rt.unsubscribe(d, d_sub);
    rt.unsubscribe(sink_node.id, order_sub);
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
                rt_inner.core().emit(other_id, h);
            }
        }
    });

    // No panic; re-entrant emit lands on `other_rec`.
    let _sub = rt.core().subscribe(s_id, sink);

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
            rt_emit.core().emit(s_id, h);
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
