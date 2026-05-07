//! Subscription-managed combinator tests (Slice D-ops, Commit 2).
//!
//! Exercises `zip` / `concat` / `race` / `take_until` built on the
//! [`graphrefly_operators::producer`] substrate. Each op is implemented
//! as a producer-shape node (no declared deps) that subscribes to its
//! upstream sources from inside its build closure and re-enters Core
//! to emit on itself.

mod common;

use common::{OpRuntime, RecordedEvent, TestValue};
use graphrefly_operators::{concat, race, take_until, zip};

// =====================================================================
// zip — pair handles N-wise across N sources
// =====================================================================

#[test]
fn zip_pairs_data_from_two_sources() {
    let rt = OpRuntime::new();
    let s1 = rt.state_int(None);
    let s2 = rt.state_int(None);
    let pack_fn = rt.register_tuple_packer();
    let z = zip(&rt.core, &rt.producer_binding, vec![s1, s2], pack_fn);
    let rec = rt.subscribe_recorder(z);

    rt.emit_int(s1, 1);
    rt.emit_int(s2, 10);
    rt.emit_int(s1, 2);
    rt.emit_int(s2, 20);

    let data = rec.data_values();
    assert_eq!(data.len(), 2, "should emit 2 tuples; got {data:?}");
    assert_eq!(
        data[0].clone().tuple(),
        vec![TestValue::Int(1), TestValue::Int(10)]
    );
    assert_eq!(
        data[1].clone().tuple(),
        vec![TestValue::Int(2), TestValue::Int(20)]
    );
}

#[test]
fn zip_buffers_until_all_sources_have_data() {
    let rt = OpRuntime::new();
    let s1 = rt.state_int(None);
    let s2 = rt.state_int(None);
    let pack_fn = rt.register_tuple_packer();
    let z = zip(&rt.core, &rt.producer_binding, vec![s1, s2], pack_fn);
    let rec = rt.subscribe_recorder(z);

    // Three emits on s1 before s2 emits — should buffer.
    rt.emit_int(s1, 1);
    rt.emit_int(s1, 2);
    rt.emit_int(s1, 3);
    assert!(rec.data_values().is_empty(), "no tuples until s2 emits");

    rt.emit_int(s2, 100);
    let data = rec.data_values();
    assert_eq!(data.len(), 1);
    assert_eq!(
        data[0].clone().tuple(),
        vec![TestValue::Int(1), TestValue::Int(100)]
    );
}

#[test]
fn zip_completes_when_one_source_completes_with_empty_queue() {
    let rt = OpRuntime::new();
    let s1 = rt.state_int(None);
    let s2 = rt.state_int(None);
    let pack_fn = rt.register_tuple_packer();
    let z = zip(&rt.core, &rt.producer_binding, vec![s1, s2], pack_fn);
    let rec = rt.subscribe_recorder(z);

    rt.emit_int(s1, 1);
    rt.emit_int(s2, 10);
    rt.core.complete(s1);

    let events = rec.events();
    let has_complete = events.iter().any(|e| matches!(e, RecordedEvent::Complete));
    assert!(has_complete, "zip should complete; got events {events:?}");
}

#[test]
fn zip_with_three_sources() {
    let rt = OpRuntime::new();
    let s1 = rt.state_int(None);
    let s2 = rt.state_int(None);
    let s3 = rt.state_int(None);
    let pack_fn = rt.register_tuple_packer();
    let z = zip(&rt.core, &rt.producer_binding, vec![s1, s2, s3], pack_fn);
    let rec = rt.subscribe_recorder(z);

    rt.emit_int(s1, 1);
    rt.emit_int(s2, 10);
    assert!(rec.data_values().is_empty(), "need s3 too");
    rt.emit_int(s3, 100);

    let data = rec.data_values();
    assert_eq!(data.len(), 1);
    assert_eq!(
        data[0].clone().tuple(),
        vec![TestValue::Int(1), TestValue::Int(10), TestValue::Int(100)]
    );
}

#[test]
fn zip_propagates_error_from_any_source() {
    let rt = OpRuntime::new();
    let s1 = rt.state_int(None);
    let s2 = rt.state_int(None);
    let pack_fn = rt.register_tuple_packer();
    let z = zip(&rt.core, &rt.producer_binding, vec![s1, s2], pack_fn);
    let rec = rt.subscribe_recorder(z);

    let err_h = rt.intern(TestValue::Str("boom".into()));
    rt.core.error(s1, err_h);

    let events = rec.events();
    let errored = events.iter().any(|e| matches!(e, RecordedEvent::Error(_)));
    assert!(errored, "expected ERROR; got {events:?}");
}

// =====================================================================
// concat — sequentially forward `first` then `second`
// =====================================================================

#[test]
fn concat_forwards_first_then_second() {
    let rt = OpRuntime::new();
    let s1 = rt.state_int(None);
    let s2 = rt.state_int(None);
    let c = concat(&rt.core, &rt.producer_binding, s1, s2);
    let rec = rt.subscribe_recorder(c);

    rt.emit_int(s1, 1);
    rt.emit_int(s1, 2);
    rt.core.complete(s1);
    rt.emit_int(s2, 10);
    rt.emit_int(s2, 20);

    let data = rec.data_values();
    assert_eq!(
        data,
        vec![
            TestValue::Int(1),
            TestValue::Int(2),
            TestValue::Int(10),
            TestValue::Int(20),
        ]
    );
}

#[test]
fn concat_buffers_second_data_during_phase_zero() {
    let rt = OpRuntime::new();
    let s1 = rt.state_int(None);
    let s2 = rt.state_int(None);
    let c = concat(&rt.core, &rt.producer_binding, s1, s2);
    let rec = rt.subscribe_recorder(c);

    rt.emit_int(s1, 1);
    // s2 emits BEFORE s1 completes — should buffer, not forward.
    rt.emit_int(s2, 99);
    rt.emit_int(s1, 2);
    assert_eq!(
        rec.data_values(),
        vec![TestValue::Int(1), TestValue::Int(2)],
        "s2 data must be buffered until s1 completes"
    );

    rt.core.complete(s1);
    let data_after_handoff = rec.data_values();
    assert_eq!(
        data_after_handoff,
        vec![
            TestValue::Int(1),
            TestValue::Int(2),
            TestValue::Int(99), // drained on handoff
        ]
    );
}

#[test]
fn concat_completes_when_second_completes() {
    let rt = OpRuntime::new();
    let s1 = rt.state_int(None);
    let s2 = rt.state_int(None);
    let c = concat(&rt.core, &rt.producer_binding, s1, s2);
    let rec = rt.subscribe_recorder(c);

    rt.emit_int(s1, 1);
    rt.core.complete(s1);
    rt.emit_int(s2, 10);
    rt.core.complete(s2);

    let events = rec.events();
    let has_complete = events.iter().any(|e| matches!(e, RecordedEvent::Complete));
    assert!(has_complete, "expected COMPLETE; got {events:?}");
}

#[test]
fn concat_propagates_error_from_first() {
    let rt = OpRuntime::new();
    let s1 = rt.state_int(None);
    let s2 = rt.state_int(None);
    let c = concat(&rt.core, &rt.producer_binding, s1, s2);
    let rec = rt.subscribe_recorder(c);

    let err = rt.intern(TestValue::Str("first-err".into()));
    rt.core.error(s1, err);

    let events = rec.events();
    let errored = events.iter().any(|e| matches!(e, RecordedEvent::Error(_)));
    assert!(errored);
}

#[test]
fn concat_propagates_error_from_second_after_handoff() {
    let rt = OpRuntime::new();
    let s1 = rt.state_int(None);
    let s2 = rt.state_int(None);
    let c = concat(&rt.core, &rt.producer_binding, s1, s2);
    let rec = rt.subscribe_recorder(c);

    rt.core.complete(s1);
    let err = rt.intern(TestValue::Str("second-err".into()));
    rt.core.error(s2, err);

    let events = rec.events();
    let errored = events.iter().any(|e| matches!(e, RecordedEvent::Error(_)));
    assert!(errored);
}

// =====================================================================
// race — first source to emit DATA wins
// =====================================================================

#[test]
fn race_winner_emits_subsequent_data() {
    let rt = OpRuntime::new();
    let s1 = rt.state_int(None);
    let s2 = rt.state_int(None);
    let r = race(&rt.core, &rt.producer_binding, vec![s1, s2]);
    let rec = rt.subscribe_recorder(r);

    rt.emit_int(s1, 1); // s1 wins
    rt.emit_int(s2, 99); // ignored — loser
    rt.emit_int(s1, 2); // forwarded — winner

    assert_eq!(
        rec.data_values(),
        vec![TestValue::Int(1), TestValue::Int(2)]
    );
}

#[test]
fn race_loser_data_is_silently_ignored() {
    let rt = OpRuntime::new();
    let s1 = rt.state_int(None);
    let s2 = rt.state_int(None);
    let s3 = rt.state_int(None);
    let r = race(&rt.core, &rt.producer_binding, vec![s1, s2, s3]);
    let rec = rt.subscribe_recorder(r);

    rt.emit_int(s2, 50); // s2 wins
    rt.emit_int(s1, 1);
    rt.emit_int(s3, 100);
    rt.emit_int(s2, 60); // forwarded

    assert_eq!(
        rec.data_values(),
        vec![TestValue::Int(50), TestValue::Int(60)]
    );
}

#[test]
fn race_winner_complete_terminates_producer() {
    let rt = OpRuntime::new();
    let s1 = rt.state_int(None);
    let s2 = rt.state_int(None);
    let r = race(&rt.core, &rt.producer_binding, vec![s1, s2]);
    let rec = rt.subscribe_recorder(r);

    rt.emit_int(s1, 1);
    rt.core.complete(s1); // winner completes

    let events = rec.events();
    let has_complete = events.iter().any(|e| matches!(e, RecordedEvent::Complete));
    assert!(
        has_complete,
        "winner complete should terminate; got {events:?}"
    );
}

#[test]
fn race_loser_complete_does_not_terminate() {
    let rt = OpRuntime::new();
    let s1 = rt.state_int(None);
    let s2 = rt.state_int(None);
    let r = race(&rt.core, &rt.producer_binding, vec![s1, s2]);
    let rec = rt.subscribe_recorder(r);

    rt.emit_int(s1, 1); // s1 wins
    rt.core.complete(s2); // loser completes — should be ignored

    let events = rec.events();
    // Should NOT have a Complete event for the producer (winner is alive).
    let producer_complete = events.iter().any(|e| matches!(e, RecordedEvent::Complete));
    assert!(
        !producer_complete,
        "loser complete must not terminate producer; got {events:?}"
    );
}

#[test]
fn race_pre_winner_error_cascades() {
    let rt = OpRuntime::new();
    let s1 = rt.state_int(None);
    let s2 = rt.state_int(None);
    let r = race(&rt.core, &rt.producer_binding, vec![s1, s2]);
    let rec = rt.subscribe_recorder(r);

    let err = rt.intern(TestValue::Str("err".into()));
    rt.core.error(s1, err); // pre-winner error

    let events = rec.events();
    let errored = events.iter().any(|e| matches!(e, RecordedEvent::Error(_)));
    assert!(errored);
}

// =====================================================================
// takeUntil — terminate on notifier DATA
// =====================================================================

#[test]
fn take_until_forwards_source_until_notifier_emits() {
    let rt = OpRuntime::new();
    let src = rt.state_int(None);
    let notif = rt.state_int(None);
    let t = take_until(&rt.core, &rt.producer_binding, src, notif);
    let rec = rt.subscribe_recorder(t);

    rt.emit_int(src, 1);
    rt.emit_int(src, 2);
    rt.emit_int(notif, 999); // ANY notifier DATA → terminate
    rt.emit_int(src, 3); // ignored

    assert_eq!(
        rec.data_values(),
        vec![TestValue::Int(1), TestValue::Int(2)]
    );
    let events = rec.events();
    let has_complete = events.iter().any(|e| matches!(e, RecordedEvent::Complete));
    assert!(has_complete);
}

#[test]
fn take_until_does_not_forward_notifier_value() {
    // Notifier DATA is a SIGNAL, not a value to emit downstream.
    // R5.7-aligned: takeUntil is a control-flow op, not a transform.
    let rt = OpRuntime::new();
    let src = rt.state_int(None);
    let notif = rt.state_int(None);
    let t = take_until(&rt.core, &rt.producer_binding, src, notif);
    let rec = rt.subscribe_recorder(t);

    rt.emit_int(notif, 999); // notifier emits BEFORE source

    let data = rec.data_values();
    assert!(
        !data.contains(&TestValue::Int(999)),
        "notifier value must NOT be forwarded; got {data:?}"
    );
}

#[test]
fn take_until_source_complete_propagates() {
    let rt = OpRuntime::new();
    let src = rt.state_int(None);
    let notif = rt.state_int(None);
    let t = take_until(&rt.core, &rt.producer_binding, src, notif);
    let rec = rt.subscribe_recorder(t);

    rt.emit_int(src, 1);
    rt.core.complete(src); // source completes naturally — propagate

    let events = rec.events();
    let has_complete = events.iter().any(|e| matches!(e, RecordedEvent::Complete));
    assert!(has_complete);
}

#[test]
fn take_until_source_error_propagates() {
    let rt = OpRuntime::new();
    let src = rt.state_int(None);
    let notif = rt.state_int(None);
    let t = take_until(&rt.core, &rt.producer_binding, src, notif);
    let rec = rt.subscribe_recorder(t);

    let err = rt.intern(TestValue::Str("src-err".into()));
    rt.core.error(src, err);

    let events = rec.events();
    let errored = events.iter().any(|e| matches!(e, RecordedEvent::Error(_)));
    assert!(errored);
}

// =====================================================================
// Substrate / lifecycle
// =====================================================================

#[test]
fn producer_storage_cleared_on_deactivation() {
    let rt = OpRuntime::new();
    let s1 = rt.state_int(None);
    let s2 = rt.state_int(None);
    let pack_fn = rt.register_tuple_packer();
    let z = zip(&rt.core, &rt.producer_binding, vec![s1, s2], pack_fn);

    {
        let _rec = rt.subscribe_recorder(z);
        let storage = rt.binding.producer_storage().lock();
        let entry = storage
            .get(&z)
            .expect("producer storage entry should exist");
        assert_eq!(entry.subs.len(), 2, "zip should subscribe to both sources");
    }
    // Recorder dropped → last sub on producer drops →
    // producer_deactivate fires → storage entry removed.

    let storage = rt.binding.producer_storage().lock();
    assert!(
        storage.get(&z).is_none(),
        "producer storage entry should be cleared on deactivation"
    );
}

#[test]
fn producer_re_subscribe_re_runs_build_closure() {
    let rt = OpRuntime::new();
    let s1 = rt.state_int(None);
    let s2 = rt.state_int(None);
    let pack_fn = rt.register_tuple_packer();
    let z = zip(&rt.core, &rt.producer_binding, vec![s1, s2], pack_fn);

    // First activation cycle.
    {
        let rec = rt.subscribe_recorder(z);
        rt.emit_int(s1, 1);
        rt.emit_int(s2, 10);
        assert_eq!(
            rec.data_values(),
            vec![TestValue::Tuple(vec![
                TestValue::Int(1),
                TestValue::Int(10)
            ])]
        );
    }
    // Cycle 1 ends: producer deactivated, storage cleared.

    // Invalidate the upstreams so the re-subscribe handshake is `[Start]`
    // only — without this, push-on-subscribe replays cached DATA into
    // the producer's sink, which can't re-enter Core under the
    // handshake-fires-lock-held discipline (pre-existing v1 limitation;
    // see `porting-deferred.md`).
    rt.core.invalidate(s1);
    rt.core.invalidate(s2);

    {
        let rec = rt.subscribe_recorder(z);
        rt.emit_int(s1, 2);
        rt.emit_int(s2, 20);
        let data = rec.data_values();
        // Push-on-subscribe replays the cached cycle-1 tuple (the
        // producer's cache survives deactivation), THEN the new emit
        // pair produces a fresh tuple. The presence of the new tuple
        // is what proves the build closure re-ran.
        assert!(
            data.contains(&TestValue::Tuple(vec![
                TestValue::Int(2),
                TestValue::Int(20)
            ])),
            "fresh activation should produce Tuple(2, 20) from new emissions; got {data:?}"
        );
    }
}
