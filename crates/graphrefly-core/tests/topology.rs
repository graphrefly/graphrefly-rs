//! Tests for Core topology-change notification primitive.

mod common;

use common::{TestRuntime, TestValue};
use graphrefly_core::TopologyEvent;
use std::sync::{Arc, Mutex};

/// Helper: collect topology events into a shared vec.
fn event_log() -> (
    graphrefly_core::TopologySink,
    Arc<Mutex<Vec<TopologyEvent>>>,
) {
    let log: Arc<Mutex<Vec<TopologyEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let log_clone = log.clone();
    let sink: graphrefly_core::TopologySink = std::rc::Rc::new(move |event: &TopologyEvent| {
        log_clone.lock().unwrap().push(event.clone());
    });
    (sink, log)
}

#[test]
fn register_state_fires_node_registered() {
    let rt = TestRuntime::new();
    let (sink, log) = event_log();
    let _sub = rt.core().subscribe_topology(sink);

    let s = rt.state(Some(TestValue::Int(1)));
    let events = log.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert!(matches!(&events[0], TopologyEvent::NodeRegistered(id) if *id == s.id));
}

#[test]
fn register_derived_fires_node_registered() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));

    let (sink, log) = event_log();
    let _sub = rt.core().subscribe_topology(sink);

    let d = rt.derived(&[s.id], |vals| Some(vals[0].clone()));
    let events = log.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert!(matches!(&events[0], TopologyEvent::NodeRegistered(id) if *id == d));
}

#[test]
fn register_dynamic_fires_node_registered() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));

    let (sink, log) = event_log();
    let _sub = rt.core().subscribe_topology(sink);

    let d = rt.dynamic(&[s.id], |vals| (Some(vals[0].clone()), None));
    let events = log.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert!(matches!(&events[0], TopologyEvent::NodeRegistered(id) if *id == d));
}

#[test]
fn teardown_fires_node_torn_down() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));

    let (sink, log) = event_log();
    let _sub = rt.core().subscribe_topology(sink);

    rt.core().teardown(s.id);
    let events = log.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert!(matches!(&events[0], TopologyEvent::NodeTornDown(id) if *id == s.id));
}

#[test]
fn set_deps_fires_deps_changed() {
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(1)));
    let b = rt.state(Some(TestValue::Int(2)));
    let d = rt.derived(&[a.id], |vals| Some(vals[0].clone()));

    let (sink, log) = event_log();
    let _sub = rt.core().subscribe_topology(sink);

    rt.core().set_deps(d, &[b.id]).unwrap();

    let events = log.lock().unwrap();
    assert_eq!(events.len(), 1);
    match &events[0] {
        TopologyEvent::DepsChanged {
            node,
            old_deps,
            new_deps,
        } => {
            assert_eq!(*node, d);
            assert_eq!(old_deps, &[a.id]);
            assert_eq!(new_deps, &[b.id]);
        }
        other => panic!("expected DepsChanged, got {other:?}"),
    }
}

#[test]
fn set_deps_idempotent_no_event() {
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(1)));
    let d = rt.derived(&[a.id], |vals| Some(vals[0].clone()));

    let (sink, log) = event_log();
    let _sub = rt.core().subscribe_topology(sink);

    // Same deps — idempotent fast-path, no topology event.
    rt.core().set_deps(d, &[a.id]).unwrap();
    let events = log.lock().unwrap();
    assert!(events.is_empty());
}

#[test]
fn explicit_unsubscribe_topology_stops_events() {
    // D246 rule 3: no core RAII below the binding — `subscribe_topology`
    // returns a Copy `TopologySubscriptionId`; teardown is owner-invoked
    // via `unsubscribe_topology` (the old `drop(sub)` RAII is gone).
    let rt = TestRuntime::new();
    let (sink, log) = event_log();
    let sub = rt.core().subscribe_topology(sink);

    let _ = rt.state(Some(TestValue::Int(1)));
    assert_eq!(log.lock().unwrap().len(), 1);

    rt.core().unsubscribe_topology(sub);

    let _ = rt.state(Some(TestValue::Int(2)));
    // No new event after unsubscribe.
    assert_eq!(log.lock().unwrap().len(), 1);
}

#[test]
fn multiple_sinks_all_fire() {
    let rt = TestRuntime::new();
    let (sink1, log1) = event_log();
    let (sink2, log2) = event_log();
    let _sub1 = rt.core().subscribe_topology(sink1);
    let _sub2 = rt.core().subscribe_topology(sink2);

    let _ = rt.state(Some(TestValue::Int(1)));
    assert_eq!(log1.lock().unwrap().len(), 1);
    assert_eq!(log2.lock().unwrap().len(), 1);
}

#[test]
fn teardown_cascade_fires_node_torn_down_for_each_node() {
    // P2 — cascaded teardown (root + downstream consumers) must fire
    // NodeTornDown for every node that emits Teardown, not just the
    // root.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));
    let d = rt.derived(&[s.id], |vals| Some(vals[0].clone()));

    let (sink, log) = event_log();
    let _sub = rt.core().subscribe_topology(sink);

    rt.core().teardown(s.id);

    let events = log.lock().unwrap();
    let torn: Vec<graphrefly_core::NodeId> = events
        .iter()
        .filter_map(|e| match e {
            TopologyEvent::NodeTornDown(id) => Some(*id),
            _ => None,
        })
        .collect();
    // Both the root state and the auto-cascaded derived must fire.
    assert!(torn.contains(&s.id), "root NodeTornDown missing");
    assert!(torn.contains(&d), "cascaded derived NodeTornDown missing");
}

#[test]
fn teardown_cascade_fires_for_meta_companions() {
    let rt = TestRuntime::new();
    let parent = rt.state(Some(TestValue::Int(1)));
    let meta = rt.state(Some(TestValue::Int(99)));
    rt.core().add_meta_companion(parent.id, meta.id);

    let (sink, log) = event_log();
    let _sub = rt.core().subscribe_topology(sink);

    rt.core().teardown(parent.id);

    let events = log.lock().unwrap();
    let torn: Vec<graphrefly_core::NodeId> = events
        .iter()
        .filter_map(|e| match e {
            TopologyEvent::NodeTornDown(id) => Some(*id),
            _ => None,
        })
        .collect();
    assert!(torn.contains(&parent.id), "parent NodeTornDown missing");
    assert!(
        torn.contains(&meta.id),
        "meta companion NodeTornDown missing"
    );
}
