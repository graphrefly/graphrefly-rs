//! TEARDOWN — destruction with auto-COMPLETE prepend (R2.6.4 / Lock 6.F).

mod common;
use common::{RecordedEvent, TestRuntime, TestValue};

#[test]
fn teardown_on_live_node_emits_complete_then_teardown() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(42)));
    let rec = rt.subscribe_recorder(s.id);
    let baseline = rec.snapshot().len();

    rt.core.teardown(s.id);

    let snap = rec.snapshot();
    let post: Vec<&RecordedEvent> = snap[baseline..].iter().collect();
    let complete_idx = post
        .iter()
        .position(|e| matches!(e, RecordedEvent::Complete));
    let teardown_idx = post
        .iter()
        .position(|e| matches!(e, RecordedEvent::Teardown));
    assert!(complete_idx.is_some(), "COMPLETE auto-prepended");
    assert!(teardown_idx.is_some(), "TEARDOWN delivered");
    assert!(
        complete_idx.unwrap() < teardown_idx.unwrap(),
        "COMPLETE precedes TEARDOWN"
    );
}

#[test]
fn teardown_on_already_terminal_node_does_not_duplicate_complete() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(42)));
    let rec = rt.subscribe_recorder(s.id);
    let baseline = rec.snapshot().len();

    rt.core.complete(s.id);
    rt.core.teardown(s.id);

    let snap = rec.snapshot();
    let complete_count = snap[baseline..]
        .iter()
        .filter(|e| matches!(e, RecordedEvent::Complete))
        .count();
    let teardown_count = snap[baseline..]
        .iter()
        .filter(|e| matches!(e, RecordedEvent::Teardown))
        .count();
    assert_eq!(
        complete_count, 1,
        "exactly one COMPLETE (from explicit complete())"
    );
    assert_eq!(teardown_count, 1);
}

#[test]
fn duplicate_teardown_is_idempotent() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(42)));
    let rec = rt.subscribe_recorder(s.id);
    let baseline = rec.snapshot().len();

    rt.core.teardown(s.id);
    rt.core.teardown(s.id);
    rt.core.teardown(s.id);

    let snap = rec.snapshot();
    let complete_count = snap[baseline..]
        .iter()
        .filter(|e| matches!(e, RecordedEvent::Complete))
        .count();
    let teardown_count = snap[baseline..]
        .iter()
        .filter(|e| matches!(e, RecordedEvent::Teardown))
        .count();
    assert_eq!(complete_count, 1, "no second auto-prepended COMPLETE");
    assert_eq!(teardown_count, 1, "exactly one TEARDOWN delivered");
}

#[test]
fn teardown_cascades_through_chain() {
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(1)));
    let b = rt.derived(&[a.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(n + 10)),
        _ => panic!("type"),
    });
    let c = rt.derived(&[b], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(n + 100)),
        _ => panic!("type"),
    });
    let rec_b = rt.subscribe_recorder(b);
    let rec_c = rt.subscribe_recorder(c);
    let baseline_b = rec_b.snapshot().len();
    let baseline_c = rec_c.snapshot().len();

    rt.core.teardown(a.id);

    for (name, snap, baseline) in [
        ("B", rec_b.snapshot(), baseline_b),
        ("C", rec_c.snapshot(), baseline_c),
    ] {
        let post: Vec<&RecordedEvent> = snap[baseline..].iter().collect();
        let c_idx = post
            .iter()
            .position(|e| matches!(e, RecordedEvent::Complete));
        let t_idx = post
            .iter()
            .position(|e| matches!(e, RecordedEvent::Teardown));
        assert!(c_idx.is_some(), "{name} should see COMPLETE in cascade");
        assert!(t_idx.is_some(), "{name} should see TEARDOWN in cascade");
        assert!(
            c_idx.unwrap() < t_idx.unwrap(),
            "{name} COMPLETE precedes TEARDOWN"
        );
    }
}

#[test]
fn teardown_diamond_each_node_sees_one_pair() {
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
    let d = rt.derived(&[b, c], |deps| match (&deps[0], &deps[1]) {
        (TestValue::Int(bv), TestValue::Int(cv)) => Some(TestValue::Int(bv + cv)),
        _ => panic!("type"),
    });
    let rec_d = rt.subscribe_recorder(d);
    let baseline = rec_d.snapshot().len();

    rt.core.teardown(a.id);

    let snap = rec_d.snapshot();
    let complete_count = snap[baseline..]
        .iter()
        .filter(|e| matches!(e, RecordedEvent::Complete))
        .count();
    let teardown_count = snap[baseline..]
        .iter()
        .filter(|e| matches!(e, RecordedEvent::Teardown))
        .count();
    assert_eq!(
        complete_count, 1,
        "D's COMPLETE is single (idempotent terminal cascade)"
    );
    assert_eq!(
        teardown_count, 1,
        "D's TEARDOWN is single (idempotent teardown cascade)"
    );
}

#[test]
fn teardown_after_error_does_not_re_emit_complete() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(42)));
    let rec = rt.subscribe_recorder(s.id);
    let baseline = rec.snapshot().len();

    let err = rt.binding.intern(TestValue::Str("bad".into()));
    rt.core.error(s.id, err);
    rt.core.teardown(s.id);

    let snap = rec.snapshot();
    let complete_count = snap[baseline..]
        .iter()
        .filter(|e| matches!(e, RecordedEvent::Complete))
        .count();
    let error_count = snap[baseline..]
        .iter()
        .filter(|e| matches!(e, RecordedEvent::Error(_)))
        .count();
    let teardown_count = snap[baseline..]
        .iter()
        .filter(|e| matches!(e, RecordedEvent::Teardown))
        .count();
    assert_eq!(
        complete_count, 0,
        "no auto-COMPLETE — already terminal via ERROR"
    );
    assert_eq!(error_count, 1);
    assert_eq!(teardown_count, 1);
}

#[test]
#[should_panic(expected = "unknown node")]
fn teardown_unknown_node_panics() {
    let rt = TestRuntime::new();
    let bogus = graphrefly_core::NodeId::new(99_999);
    rt.core.teardown(bogus);
}
