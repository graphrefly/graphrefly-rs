//! Integration tests for multi-dep combinator operators (Slice C-2, D020).
//!
//! Covers: `combine` (combineLatest), `with_latest_from`, `merge`.

mod common;

use common::{OpRuntime, RecordedEvent, TestValue};
use graphrefly_operators::combine::{combine, merge, with_latest_from};

// =====================================================================
// combine (combineLatest)
// =====================================================================

#[test]
fn combine_emits_tuple_on_any_dep_fire() {
    let rt = OpRuntime::new();
    let a = rt.state_int(Some(1));
    let b = rt.state_int(Some(2));

    let c = combine(rt.core(), &rt.op_binding, &[a, b], rt.make_packer()).unwrap();
    let rec = rt.subscribe_recorder(c.node);

    // Both deps have initial values → first-run gate satisfied at subscribe →
    // push-on-subscribe delivers combined tuple.
    let data = rec.data_values();
    assert_eq!(data.len(), 1);
    assert_eq!(
        data[0],
        TestValue::Tuple(vec![TestValue::Int(1), TestValue::Int(2)])
    );

    // Emit on dep a → new tuple.
    rec.clear();
    rt.emit_int(a, 10);
    let data = rec.data_values();
    assert_eq!(data.len(), 1);
    assert_eq!(
        data[0],
        TestValue::Tuple(vec![TestValue::Int(10), TestValue::Int(2)])
    );

    // Emit on dep b → new tuple with latest a.
    rec.clear();
    rt.emit_int(b, 20);
    let data = rec.data_values();
    assert_eq!(data.len(), 1);
    assert_eq!(
        data[0],
        TestValue::Tuple(vec![TestValue::Int(10), TestValue::Int(20)])
    );
}

#[test]
fn combine_first_run_gate_holds_until_all_deps_fire() {
    let rt = OpRuntime::new();
    // Create sources without initial values (sentinel).
    let a = rt.state_int(None);
    let b = rt.state_int(None);

    let c = combine(rt.core(), &rt.op_binding, &[a, b], rt.make_packer()).unwrap();
    let rec = rt.subscribe_recorder(c.node);

    // No data yet — gate holds.
    assert!(rec.data_values().is_empty());

    // Emit only a — gate still holds (b is sentinel).
    rt.emit_int(a, 1);
    assert!(rec.data_values().is_empty());

    // Emit b — gate releases, tuple emitted.
    rt.emit_int(b, 2);
    let data = rec.data_values();
    assert_eq!(data.len(), 1);
    assert_eq!(
        data[0],
        TestValue::Tuple(vec![TestValue::Int(1), TestValue::Int(2)])
    );
}

#[test]
fn combine_3_deps() {
    let rt = OpRuntime::new();
    let a = rt.state_int(Some(1));
    let b = rt.state_int(Some(2));
    let c = rt.state_int(Some(3));

    let combined = combine(rt.core(), &rt.op_binding, &[a, b, c], rt.make_packer()).unwrap();
    let rec = rt.subscribe_recorder(combined.node);

    let data = rec.data_values();
    assert_eq!(data.len(), 1);
    assert_eq!(
        data[0],
        TestValue::Tuple(vec![
            TestValue::Int(1),
            TestValue::Int(2),
            TestValue::Int(3)
        ])
    );

    rec.clear();
    rt.emit_int(b, 20);
    let data = rec.data_values();
    assert_eq!(
        data[0],
        TestValue::Tuple(vec![
            TestValue::Int(1),
            TestValue::Int(20),
            TestValue::Int(3)
        ])
    );
}

#[test]
fn combine_complete_when_all_deps_complete() {
    let rt = OpRuntime::new();
    let a = rt.state_int(Some(1));
    let b = rt.state_int(Some(2));

    let c = combine(rt.core(), &rt.op_binding, &[a, b], rt.make_packer()).unwrap();
    let rec = rt.subscribe_recorder(c.node);
    rec.clear();

    // Complete a — combine doesn't complete yet.
    rt.core().complete(a);
    assert!(!rec.events().contains(&RecordedEvent::Complete));

    // Complete b — now combine completes (R1.3.4.b).
    rt.core().complete(b);
    assert!(rec.events().contains(&RecordedEvent::Complete));
}

// =====================================================================
// with_latest_from
// =====================================================================

#[test]
fn with_latest_from_emits_only_on_primary() {
    let rt = OpRuntime::new();
    let primary = rt.state_int(Some(1));
    let secondary = rt.state_int(Some(2));

    let wlf = with_latest_from(
        rt.core(),
        &rt.op_binding,
        primary,
        secondary,
        rt.make_packer(),
    );
    let rec = rt.subscribe_recorder(wlf.node);

    // Push-on-subscribe: both have values, gate satisfied → pair emitted.
    let data = rec.data_values();
    assert_eq!(data.len(), 1);
    assert_eq!(
        data[0],
        TestValue::Tuple(vec![TestValue::Int(1), TestValue::Int(2)])
    );

    // Emit on secondary only → RESOLVED, no new data.
    rec.clear();
    rt.emit_int(secondary, 20);
    assert!(rec.data_values().is_empty());
    assert!(rec.events().contains(&RecordedEvent::Resolved));

    // Emit on primary → pair with latest secondary.
    rec.clear();
    rt.emit_int(primary, 10);
    let data = rec.data_values();
    assert_eq!(data.len(), 1);
    assert_eq!(
        data[0],
        TestValue::Tuple(vec![TestValue::Int(10), TestValue::Int(20)])
    );
}

#[test]
fn with_latest_from_gate_holds_until_both_deliver() {
    let rt = OpRuntime::new();
    let primary = rt.state_int(None);
    let secondary = rt.state_int(None);

    let wlf = with_latest_from(
        rt.core(),
        &rt.op_binding,
        primary,
        secondary,
        rt.make_packer(),
    );
    let rec = rt.subscribe_recorder(wlf.node);

    // No data — gate holds.
    assert!(rec.data_values().is_empty());

    // Emit primary only — gate holds (secondary sentinel).
    rt.emit_int(primary, 1);
    assert!(rec.data_values().is_empty());

    // Emit secondary — gate releases (both deps now have values).
    // First-fire special case: gate release always emits regardless of
    // which dep triggered the wave, since the gate guarantees both have
    // real values.
    rt.emit_int(secondary, 2);
    let data = rec.data_values();
    assert_eq!(data.len(), 1);
    assert_eq!(
        data[0],
        TestValue::Tuple(vec![TestValue::Int(1), TestValue::Int(2)])
    );

    // Subsequent secondary-only fire → no emission (fire-on-primary-only).
    rec.clear();
    rt.emit_int(secondary, 20);
    assert!(rec.data_values().is_empty());

    // Primary fires → pair with latest secondary.
    rt.emit_int(primary, 10);
    let data = rec.data_values();
    assert_eq!(data.len(), 1);
    assert_eq!(
        data[0],
        TestValue::Tuple(vec![TestValue::Int(10), TestValue::Int(20)])
    );
}

#[test]
fn with_latest_from_secondary_update_samples_latest() {
    let rt = OpRuntime::new();
    let primary = rt.state_int(Some(1));
    let secondary = rt.state_int(Some(100));

    let wlf = with_latest_from(
        rt.core(),
        &rt.op_binding,
        primary,
        secondary,
        rt.make_packer(),
    );
    let rec = rt.subscribe_recorder(wlf.node);
    rec.clear();

    // Update secondary multiple times.
    rt.emit_int(secondary, 200);
    rt.emit_int(secondary, 300);

    // Now fire primary — should sample latest secondary (300).
    rt.emit_int(primary, 5);
    let data = rec.data_values();
    assert_eq!(data.len(), 1);
    assert_eq!(
        data[0],
        TestValue::Tuple(vec![TestValue::Int(5), TestValue::Int(300)])
    );
}

#[test]
fn with_latest_from_complete_when_all_deps_complete() {
    let rt = OpRuntime::new();
    let primary = rt.state_int(Some(1));
    let secondary = rt.state_int(Some(2));

    let wlf = with_latest_from(
        rt.core(),
        &rt.op_binding,
        primary,
        secondary,
        rt.make_packer(),
    );
    let rec = rt.subscribe_recorder(wlf.node);
    rec.clear();

    rt.core().complete(primary);
    assert!(!rec.events().contains(&RecordedEvent::Complete));

    rt.core().complete(secondary);
    assert!(rec.events().contains(&RecordedEvent::Complete));
}

// =====================================================================
// merge
// =====================================================================

#[test]
fn merge_forwards_all_dep_data_verbatim() {
    let rt = OpRuntime::new();
    let a = rt.state_int(Some(1));
    let b = rt.state_int(Some(2));

    let m = merge(rt.core(), &[a, b]).unwrap();
    let rec = rt.subscribe_recorder(m.node);

    // Push-on-subscribe: merge is partial:true, so fires on first dep.
    // Both deps deliver sequentially on subscribe (N separate waves per
    // R2.7.5). Merge forwards each.
    let data = rec.data_values();
    assert_eq!(data.len(), 2);
    assert_eq!(data[0], TestValue::Int(1));
    assert_eq!(data[1], TestValue::Int(2));

    // Emit on a → forwarded.
    rec.clear();
    rt.emit_int(a, 10);
    let data = rec.data_values();
    assert_eq!(data.len(), 1);
    assert_eq!(data[0], TestValue::Int(10));

    // Emit on b → forwarded.
    rec.clear();
    rt.emit_int(b, 20);
    let data = rec.data_values();
    assert_eq!(data.len(), 1);
    assert_eq!(data[0], TestValue::Int(20));
}

#[test]
fn merge_zero_ffi_no_binding_call() {
    // Merge doesn't call any FFI method (project/predicate/fold/pack) —
    // it's pure handle forwarding. We verify by checking that the binding
    // doesn't panic (since the default impls panic for unregistered ops).
    let rt = OpRuntime::new();
    let a = rt.state_int(Some(1));
    let b = rt.state_int(Some(2));

    let m = merge(rt.core(), &[a, b]).unwrap();
    let _rec = rt.subscribe_recorder(m.node);

    // If any FFI method were called, InnerBinding would unreachable! on
    // invoke_fn. Reaching here proves zero FFI on the fire path.
    rt.emit_int(a, 100);
    rt.emit_int(b, 200);
}

#[test]
fn merge_complete_when_all_deps_complete() {
    let rt = OpRuntime::new();
    let a = rt.state_int(Some(1));
    let b = rt.state_int(Some(2));
    let c = rt.state_int(Some(3));

    let m = merge(rt.core(), &[a, b, c]).unwrap();
    let rec = rt.subscribe_recorder(m.node);
    rec.clear();

    rt.core().complete(a);
    assert!(!rec.events().contains(&RecordedEvent::Complete));

    rt.core().complete(b);
    assert!(!rec.events().contains(&RecordedEvent::Complete));

    rt.core().complete(c);
    assert!(rec.events().contains(&RecordedEvent::Complete));
}

#[test]
fn merge_error_cascades_when_all_deps_terminal() {
    // Rust merge uses Core's standard dep-terminal cascade (R1.3.4.b):
    // error propagates when ALL deps are terminal, ERROR dominates.
    // This differs from TS merge (first-error-terminates via producer
    // pattern). Documented in porting-deferred.md.
    let rt = OpRuntime::new();
    let a = rt.state_int(Some(1));
    let b = rt.state_int(Some(2));

    let m = merge(rt.core(), &[a, b]).unwrap();
    let rec = rt.subscribe_recorder(m.node);
    rec.clear();

    let err_h = rt.intern(TestValue::Str("boom".into()));
    rt.core().error(a, err_h);
    // Only dep a is terminal — merge doesn't cascade yet.
    assert!(!rec
        .events()
        .iter()
        .any(|e| matches!(e, RecordedEvent::Error(_))));

    // Complete dep b → all deps terminal, ERROR dominates → merge errors.
    rt.core().complete(b);
    assert!(rec
        .events()
        .iter()
        .any(|e| matches!(e, RecordedEvent::Error(_))));
}

#[test]
fn merge_partial_mode_fires_on_first_dep() {
    // Merge uses partial:true — fires as soon as any dep delivers.
    let rt = OpRuntime::new();
    let a = rt.state_int(None); // sentinel
    let b = rt.state_int(None); // sentinel

    let m = merge(rt.core(), &[a, b]).unwrap();
    let rec = rt.subscribe_recorder(m.node);

    // No data yet — both sentinel.
    assert!(rec.data_values().is_empty());

    // Emit on b only → merge fires immediately (partial:true).
    rt.emit_int(b, 42);
    let data = rec.data_values();
    assert_eq!(data.len(), 1);
    assert_eq!(data[0], TestValue::Int(42));
}

#[test]
fn merge_many_sources() {
    let rt = OpRuntime::new();
    let sources: Vec<_> = (0..5).map(|i| rt.state_int(Some(i))).collect();

    let m = merge(rt.core(), &sources).unwrap();
    let rec = rt.subscribe_recorder(m.node);

    // Push-on-subscribe delivers each source's initial value.
    let data = rec.data_values();
    assert_eq!(data.len(), 5);
    for (i, datum) in data.iter().enumerate() {
        assert_eq!(*datum, TestValue::Int(i as i64));
    }
}
