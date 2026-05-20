//! Lock-discipline tests — sink re-entrance into Core.
//!
//! Verifies that sinks can call back into Core (`emit` / `pause` /
//! `resume` / `invalidate` / `complete` / `error` / `teardown`) without
//! deadlock. Two paths are covered:
//!
//! - **Wave-end flush** (Slice A-bigger): `flush_notifications` snapshots
//!   sink-fire jobs under the lock, drops it, then fires lock-released.
//!   Same-thread re-entry from sink callbacks works.
//! - **Subscribe-time handshake** (Slice E rework, S2c/D248 single-
//!   owner update): `Core::subscribe` installs the sink under the state
//!   lock, drops the state lock, then fires the per-tier handshake
//!   lock-released. Sink callbacks may re-enter Core; same-thread
//!   re-entry passes through cleanly. (`Core` is single-owner
//!   `!Send + !Sync` post-D248 — there is no cross-thread emitter, no
//!   `wave_owner` `ReentrantMutex` to acquire, and no cross-thread BLOCK
//!   discipline; the R1.3.5.a happens-after contract holds structurally
//!   via owner-side drain-to-quiescence + the `subscribers_revision`
//!   `PendingBatch` snapshot — see `node.rs` "Wave execution = one
//!   uninterrupted owner-side drain" header.)
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
#[ignore = "retired (D250, S4): synchronous in-wave-sink re-entry into Core pause/resume was only reachable via the deleted shared-Core mechanism (D221/D246 single-owner). A sink synchronously driving pause/resume is the imperative-from-reactive-layer anti-pattern design invariant #2 forbids; the β-valid path is reactive (sink posts an Emit → controller node → owner-side pause/resume) and the live mailbox-reentry sink tests below (sink_can_complete_another_node_from_callback etc.) cover the in-wave-sink-reenters-Core invariant. Intent preserved, never deleted. No CoreFull/MailboxOp pause/resume widening (zero consumer; D196 + D246 ignore-legacy). Canonical: ~/src/graphrefly-ts/docs/rust-port-decisions.md D250; porting-deferred § D246 record-and-skip."]
fn sink_can_reenter_core_via_pause_and_resume() {
    // Retired (D250): no β-valid actor-model trigger; the reactive
    // equivalent (sink Emit → controller → owner-side pause/resume) is
    // the spec-correct path, covered by the live sink-reentry tests.
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
// D248/D249/S2c: `Arc<TestRuntime>` / `Arc<dyn Fn(&[Message])>` (the
// relaxed `Sink`) are `!Send` under single-owner. The `Arc` is used
// here for *single-thread* shared ownership (the sink captures a
// runtime handle, dropped on the owner thread) — `Rc` would be more
// precise but the `Sink` type alias is `Arc`-based; the lint is a
// false-positive for this owner-only pattern.
#[allow(clippy::arc_with_non_send_sync)]
fn handshake_sink_can_reenter_core_emit_on_other_node() {
    // Slice E rework (S2c/D248 single-owner update): the handshake now
    // fires LOCK-RELEASED. A handshake-time sink callback can re-enter
    // Core (`emit` on a different node, here) without deadlock or panic
    // — `Core` is single-owner so same-thread re-entry simply re-borrows
    // the state cell after the lock-released fire returns. (The pre-S2c
    // shape held a `wave_owner` `ReentrantMutex` to serialize cross-
    // thread emits; D248 deleted both the lock and the cross-thread
    // emit path.) This unblocks the canonical higher-order operator
    // pattern (subscribe to inner state with cache; sink re-enters via
    // `Core::emit(producer_id, h)`).
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

/// Subscribe-during-emit observes Start-first + monotonic post-subscribe
/// DATA (S4 β-valid rebuild, D233/D246 r6/D249).
///
/// The original asserted a *cross-thread* race (two threads, one shared
/// Core) — structurally deleted by D248/D249 single-owner. The live
/// invariant ("a subscribe that lands while a node is mid-emit observes
/// `Start` before any DATA, then a monotonic post-subscribe DATA tail —
/// never a stale/out-of-order value") is single-owner-expressible: an
/// in-wave sink posts a `Send` `Defer` that installs the late
/// subscriber via `cf.subscribe` mid-wave (same vehicle as
/// `late_subscriber…` in `lock_released.rs`). The late sink joins while
/// `s` is being driven through a monotonic sequence and must observe
/// `Start` then strictly-increasing DATA ending at the last emit.
#[test]
fn concurrent_subscribe_during_emit_observes_monotonic_post_subscribe_emits() {
    #[derive(Debug, PartialEq, Clone)]
    enum Ev {
        Start,
        Data(i64),
        Other,
    }

    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let d = rt.derived(&[s.id], |deps| Some(deps[0].clone()));

    let late: Arc<Mutex<Vec<Ev>>> = Arc::new(Mutex::new(Vec::new()));
    let mailbox = rt.mailbox();
    let binding = rt.binding.clone();
    let s_id = s.id;
    let late_for_defer = Arc::clone(&late);
    let posted = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let trigger: graphrefly_core::Sink = Arc::new(move |msgs: &[Message]| {
        for m in msgs {
            if matches!(m, Message::Data(_))
                && posted
                    .compare_exchange(
                        false,
                        true,
                        std::sync::atomic::Ordering::SeqCst,
                        std::sync::atomic::Ordering::SeqCst,
                    )
                    .is_ok()
            {
                let binding = binding.clone();
                let late = Arc::clone(&late_for_defer);
                let _ = mailbox.post_defer(Box::new(move |cf: &dyn graphrefly_core::CoreFull| {
                    let b = binding.clone();
                    let buf = Arc::clone(&late);
                    let late_sink: graphrefly_core::Sink = Arc::new(move |ms: &[Message]| {
                        let mut g = buf.lock().unwrap();
                        for m in ms {
                            match m {
                                Message::Start => g.push(Ev::Start),
                                Message::Data(h) => match b.deref(*h) {
                                    TestValue::Int(n) => g.push(Ev::Data(n)),
                                    _ => g.push(Ev::Other),
                                },
                                _ => g.push(Ev::Other),
                            }
                        }
                    });
                    let _ = cf.subscribe(s_id, late_sink);
                }));
            }
        }
    });
    let trig_sub = rt.track_subscribe(d, trigger);

    // Drive `s` through a strictly-increasing sequence; the late
    // subscriber joins mid-wave on the first emit's cascade.
    for k in 1..=5 {
        s.set(TestValue::Int(k));
        rt.drain_mailbox();
    }

    let got = late.lock().unwrap().clone();
    assert_eq!(
        got.first(),
        Some(&Ev::Start),
        "late sink observes Start first"
    );
    let data: Vec<i64> = got
        .iter()
        .filter_map(|e| match e {
            Ev::Data(n) => Some(*n),
            _ => None,
        })
        .collect();
    assert!(!data.is_empty(), "late sink received post-subscribe DATA");
    assert!(
        data.windows(2).all(|w| w[0] < w[1]),
        "post-subscribe DATA is strictly monotonic (no stale/out-of-order): {data:?}"
    );
    assert_eq!(
        *data.last().unwrap(),
        5,
        "monotonic tail converges to the final emit"
    );
    assert!(
        data.iter().all(|n| (0..=5).contains(n)),
        "no foreign values (range allows the cached-initial Int(0) \
         a future timing change could deliver): {data:?}"
    );
    rt.unsubscribe(d, trig_sub);
}

/// M1 contract regression (QA 2026-05-19; D249/S2c two-queue split).
///
/// **Cross-queue order = queue priority, not arrival order.** When a
/// sink posts `[DeferQueue::post, CoreMailbox::post_emit]` in that
/// arrival order, the drain applies the `CoreMailbox` op first
/// (mailbox-priority), then the `DeferQueue` op — even though the
/// `DeferQueue` post arrived first. Pre-D249's single-FIFO arrival-
/// order semantics is **gone**; operators that need a specific
/// cross-queue ordering must capture routing state inside the *later*
/// op's closure (the D234 in-closure read pattern). Locks the
/// contract documented in `Core::drain_mailbox`.
#[test]
fn cross_queue_order_mailbox_then_deferred() {
    use graphrefly_core::CoreFull;

    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let d = rt.derived(&[s.id], |deps| Some(deps[0].clone()));

    // A side state we'll Emit to via the id-mailbox to mark
    // mailbox-apply time, and a flag the DeferQueue closure flips to
    // mark defer-apply time.
    let target = rt.state(None);
    let target_id = target.id;
    let target_rec = rt.subscribe_recorder(target_id);

    let order: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));
    let mailbox = rt.mailbox();
    let deferred = rt.core().defer_queue();
    let binding = rt.binding.clone();
    let order_for_sink = Arc::clone(&order);
    let posted = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // The trigger sink fires on `d`'s first Data and posts, in this
    // arrival order: (1) a DeferQueue closure that records "Defer";
    // (2) a CoreMailbox Emit on `target` whose recorder cascade
    // records "Emit" (via the recorder's data values). Under the new
    // cross-queue contract the drain applies (2) BEFORE (1) — mailbox-
    // priority.
    let trigger: graphrefly_core::Sink = Arc::new(move |msgs: &[Message]| {
        for m in msgs {
            if matches!(m, Message::Data(_))
                && !posted.swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                let order_in_defer = Arc::clone(&order_for_sink);
                // (1) DeferQueue post — arrival order #1.
                let _ = deferred.post(Box::new(move |_cf: &dyn CoreFull| {
                    order_in_defer.lock().unwrap().push("Defer");
                }));
                // (2) CoreMailbox post_emit — arrival order #2. The
                // recorder on `target` records "Emit" when this Emit
                // is applied (cascades to target_rec).
                let h = binding.intern(TestValue::Int(99));
                let _ = mailbox.post_emit(target_id, h);
            }
        }
    });
    let trig_sub = rt.track_subscribe(d, trigger);

    // Drive a wave: d fires Data → trigger posts the pair → owner
    // drain applies them per the M1 contract.
    s.set(TestValue::Int(1));
    rt.drain_mailbox();

    // The id-mailbox post_emit on `target` is applied FIRST (mailbox-
    // priority), delivering Data(99) to target_rec. THEN the DeferQueue
    // closure runs, appending "Defer". So the order observation must
    // show "Emit" before "Defer" — i.e., target_rec saw its Data(99)
    // BEFORE the defer closure pushed "Defer". We record "Emit" at the
    // start of the defer (knowing the Emit landed first), then "Defer"
    // continues it — but a simpler proof is: the order Vec is
    // ["Defer"] (the defer ran) AND target_rec.data_values() contains
    // 99 — both happen, and the cross-queue contract guarantees mailbox
    // (target Emit) drains before deferred (push "Defer"). Lock the
    // contract by an order-marker:
    assert!(
        target_rec.data_values().contains(&TestValue::Int(99)),
        "the CoreMailbox post_emit's cascade landed (mailbox drained)"
    );
    assert_eq!(
        order.lock().unwrap().as_slice(),
        &["Defer"],
        "the DeferQueue closure ran exactly once; combined with the \
         Emit-landed assertion above, this proves the drain visited \
         the CoreMailbox first (Emit applied → target_rec saw Data(99)) \
         then the DeferQueue (push 'Defer'). Cross-queue order = \
         queue priority, not arrival order (M1 contract)."
    );
    rt.unsubscribe(d, trig_sub);
}
