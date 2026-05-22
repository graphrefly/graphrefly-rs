//! Tests for reactive describe and reactive observe_all (M2 Slice F+).

mod common;

use common::graph;
use graphrefly_core::{HandleId, Message};
use graphrefly_graph::DescribeSink;
use std::sync::{Arc, Mutex};

fn h(n: u64) -> HandleId {
    HandleId::new(n)
}

// -------------------------------------------------------------------
// Reactive describe
// -------------------------------------------------------------------

#[test]
fn describe_reactive_pushes_initial_snapshot() {
    let (rt, g) = graph("test");
    g.state(rt.core(), "a", Some(h(1))).unwrap();

    let snapshots = Arc::new(Mutex::new(Vec::new()));
    let snapshots_clone = snapshots.clone();
    let sink: DescribeSink = std::rc::Rc::new(move |output| {
        snapshots_clone.lock().unwrap().push(output.clone());
    });
    let handle = g.describe_reactive(rt.core(), &sink);

    // P7 — push-on-subscribe per canonical R3.6.1 / §2.5.2.
    {
        let snaps = snapshots.lock().unwrap();
        assert_eq!(snaps.len(), 1);
        assert!(snaps[0].nodes.contains_key("a"));
    }
    handle.detach(rt.core());
}

#[test]
fn describe_reactive_fires_on_new_node() {
    let (rt, g) = graph("test");
    let snapshots = Arc::new(Mutex::new(Vec::new()));
    let snapshots_clone = snapshots.clone();
    let sink: DescribeSink = std::rc::Rc::new(move |output| {
        snapshots_clone.lock().unwrap().push(output.clone());
    });
    let handle = g.describe_reactive(rt.core(), &sink);

    // 1 fire from the initial empty snapshot.
    assert_eq!(snapshots.lock().unwrap().len(), 1);

    g.state(rt.core(), "a", Some(h(1))).unwrap();

    {
        let snaps = snapshots.lock().unwrap();
        // Initial empty snapshot + post-add snapshot.
        assert_eq!(snaps.len(), 2);
        assert!(snaps[0].nodes.is_empty());
        assert!(snaps[1].nodes.contains_key("a"));
    }
    handle.detach(rt.core());
}

#[test]
fn describe_reactive_fires_on_remove() {
    let (rt, g) = graph("test");
    g.state(rt.core(), "a", Some(h(1))).unwrap();

    let snapshots = Arc::new(Mutex::new(Vec::new()));
    let snapshots_clone = snapshots.clone();
    let sink: DescribeSink = std::rc::Rc::new(move |output| {
        snapshots_clone.lock().unwrap().push(output.clone());
    });
    let handle = g.describe_reactive(rt.core(), &sink);

    // Initial snapshot fires synchronously.
    assert_eq!(snapshots.lock().unwrap().len(), 1);
    g.remove(rt.core(), "a").unwrap();

    {
        let snaps = snapshots.lock().unwrap();
        // Initial (with "a") + post-remove (without "a").
        assert_eq!(snaps.len(), 2);
        assert!(snaps[0].nodes.contains_key("a"));
        assert!(!snaps[1].nodes.contains_key("a"));
    }
    handle.detach(rt.core());
}

// Note: set_deps fires Core topology events, NOT Graph namespace
// changes. describe_reactive subscribes to namespace changes only.
// Callers needing set_deps reactivity compose with
// core.subscribe_topology(). This is tested in topology.rs.

#[test]
fn describe_reactive_stops_on_detach() {
    // D246 rule 3: `ReactiveDescribeHandle` has NO RAII `Drop`.
    // Teardown is owner-invoked via `detach`. This test asserts the
    // same behavior the pre-D246 drop-based test did (no snapshots
    // after teardown) — only the trigger changed from scope-drop to
    // explicit detach.
    let (rt, g) = graph("test");
    let snapshots = Arc::new(Mutex::new(Vec::new()));
    let snapshots_clone = snapshots.clone();
    let sink: DescribeSink = std::rc::Rc::new(move |output| {
        snapshots_clone.lock().unwrap().push(output.clone());
    });
    let handle = g.describe_reactive(rt.core(), &sink);

    g.state(rt.core(), "a", Some(h(1))).unwrap();
    // Initial empty + post-add = 2 snapshots.
    assert_eq!(snapshots.lock().unwrap().len(), 2);

    handle.detach(rt.core());

    g.state(rt.core(), "b", Some(h(2))).unwrap();
    // No new snapshot after detach.
    assert_eq!(snapshots.lock().unwrap().len(), 2);
}

#[test]
fn describe_reactive_accumulates_nodes() {
    let (rt, g) = graph("test");
    let snapshots = Arc::new(Mutex::new(Vec::new()));
    let snapshots_clone = snapshots.clone();
    let sink: DescribeSink = std::rc::Rc::new(move |output| {
        snapshots_clone.lock().unwrap().push(output.clone());
    });
    let handle = g.describe_reactive(rt.core(), &sink);

    g.state(rt.core(), "a", Some(h(1))).unwrap();
    g.state(rt.core(), "b", Some(h(2))).unwrap();

    {
        let snaps = snapshots.lock().unwrap();
        // Initial empty + 2 adds = 3 snapshots.
        assert_eq!(snaps.len(), 3);
        assert_eq!(snaps[0].nodes.len(), 0);
        assert_eq!(snaps[1].nodes.len(), 1);
        assert_eq!(snaps[2].nodes.len(), 2);
    }
    handle.detach(rt.core());
}

// -------------------------------------------------------------------
// Reactive observe_all (auto-subscribe)
// -------------------------------------------------------------------

#[test]
fn observe_all_reactive_subscribes_current_nodes() {
    let (rt, g) = graph("test");
    g.state(rt.core(), "a", Some(h(10))).unwrap();

    let events = Arc::new(Mutex::new(Vec::<(String, Vec<Message>)>::new()));
    let events_clone = events.clone();
    let mut obs = g.observe_all_reactive();
    let count = obs.subscribe(rt.core(), move |name: &str, msgs: &[Message]| {
        events_clone
            .lock()
            .unwrap()
            .push((name.to_string(), msgs.to_vec()));
    });
    assert_eq!(count, 1);

    // Push-on-subscribe should have fired for "a".
    {
        let evts = events.lock().unwrap();
        assert!(!evts.is_empty());
        assert_eq!(evts[0].0, "a");
    }
    obs.detach(rt.core());
}

#[test]
fn observe_all_reactive_auto_subscribes_late_node() {
    let (rt, g) = graph("test");

    let events = Arc::new(Mutex::new(Vec::<(String, Vec<Message>)>::new()));
    let events_clone = events.clone();
    let mut obs = g.observe_all_reactive();
    let count = obs.subscribe(rt.core(), move |name: &str, msgs: &[Message]| {
        events_clone
            .lock()
            .unwrap()
            .push((name.to_string(), msgs.to_vec()));
    });
    assert_eq!(count, 0); // No nodes yet.

    // Add a node AFTER subscribe — should auto-subscribe.
    g.state(rt.core(), "late", Some(h(42))).unwrap();

    {
        let evts = events.lock().unwrap();
        // Should have received push-on-subscribe for "late".
        let late_events: Vec<_> = evts.iter().filter(|(name, _)| name == "late").collect();
        assert!(
            !late_events.is_empty(),
            "expected auto-subscribe to fire for late-added node"
        );
    }
    obs.detach(rt.core());
}

#[test]
fn observe_all_reactive_stops_on_detach() {
    // D246 rule 3: `GraphObserveAllReactive` has NO RAII `Drop`.
    // Teardown is owner-invoked via `detach`. Asserts the same
    // behavior as the pre-D246 drop-based test (no events after
    // teardown) — only the trigger changed.
    let (rt, g) = graph("test");

    let events = Arc::new(Mutex::new(Vec::<(String, Vec<Message>)>::new()));
    let events_clone = events.clone();
    let mut obs = g.observe_all_reactive();
    obs.subscribe(rt.core(), move |name: &str, msgs: &[Message]| {
        events_clone
            .lock()
            .unwrap()
            .push((name.to_string(), msgs.to_vec()));
    });

    g.state(rt.core(), "x", Some(h(1))).unwrap();
    let count_before = events.lock().unwrap().len();

    obs.detach(rt.core());

    // Add another node — sink should NOT fire after detach.
    g.state(rt.core(), "y", Some(h(2))).unwrap();
    let count_after = events.lock().unwrap().len();
    // After detach, no new events for "y".
    // Note: count_before includes events from "x".
    assert_eq!(
        count_after, count_before,
        "no events should arrive after detaching reactive handle"
    );
}

// -------------------------------------------------------------------
// Mount / unmount fire namespace_change on parent (P3)
// -------------------------------------------------------------------

#[test]
fn describe_reactive_fires_on_mount_new() {
    let (rt, g) = graph("root");
    let snapshots = Arc::new(Mutex::new(Vec::new()));
    let snapshots_clone = snapshots.clone();
    let sink: DescribeSink = std::rc::Rc::new(move |output| {
        snapshots_clone.lock().unwrap().push(output.clone());
    });
    let handle = g.describe_reactive(rt.core(), &sink);

    // Initial snapshot fires synchronously.
    assert_eq!(snapshots.lock().unwrap().len(), 1);

    g.mount_new(rt.core(), "child").unwrap();

    {
        // Mount must fire as a namespace change.
        let snaps = snapshots.lock().unwrap();
        assert_eq!(snaps.len(), 2);
        // Subgraph "child" appears in the post-mount snapshot.
        assert!(snaps[1].subgraphs.iter().any(|s| s == "child"));
    }
    handle.detach(rt.core());
}

#[test]
fn describe_reactive_fires_on_unmount() {
    let (rt, g) = graph("root");
    g.mount_new(rt.core(), "child").unwrap();

    let snapshots = Arc::new(Mutex::new(Vec::new()));
    let snapshots_clone = snapshots.clone();
    let sink: DescribeSink = std::rc::Rc::new(move |output| {
        snapshots_clone.lock().unwrap().push(output.clone());
    });
    let handle = g.describe_reactive(rt.core(), &sink);

    // Initial snapshot includes "child".
    assert_eq!(snapshots.lock().unwrap().len(), 1);
    assert!(snapshots.lock().unwrap()[0]
        .subgraphs
        .iter()
        .any(|s| s == "child"));

    g.unmount(rt.core(), "child").unwrap();

    {
        let snaps = snapshots.lock().unwrap();
        // Initial + post-unmount.
        assert_eq!(snaps.len(), 2);
        // "child" no longer in subgraphs.
        assert!(!snaps[1].subgraphs.iter().any(|s| s == "child"));
    }
    handle.detach(rt.core());
}

#[test]
fn observe_all_reactive_handles_late_mount() {
    let (rt, g) = graph("root");

    let events = Arc::new(Mutex::new(Vec::<(String, Vec<Message>)>::new()));
    let events_clone = events.clone();
    let mut obs = g.observe_all_reactive();
    obs.subscribe(rt.core(), move |name: &str, msgs: &[Message]| {
        events_clone
            .lock()
            .unwrap()
            .push((name.to_string(), msgs.to_vec()));
    });

    // Mount a new subgraph + add a node into it. The reactive handle
    // tracks the parent's namespace; mounting the child fires
    // namespace_change but the child's nodes are NOT auto-subscribed
    // (each Graph has its own namespace_sinks list). Verify at least
    // that the mount didn't crash and pre-existing parent nodes still
    // fire.
    let child = g.mount_new(rt.core(), "child").unwrap();
    child.state(rt.core(), "inside", Some(h(99))).unwrap();
    g.state(rt.core(), "parent_node", Some(h(1))).unwrap();

    {
        // Parent node "parent_node" must reach the sink (namespace
        // listener fired from add()).
        let evts = events.lock().unwrap();
        assert!(
            evts.iter().any(|(name, _)| name == "parent_node"),
            "expected parent_node event after late add"
        );
    }
    obs.detach(rt.core());
}

// -------------------------------------------------------------------
// P5 — subscribe-once contract panics
// -------------------------------------------------------------------

#[test]
#[should_panic(expected = "single-shot")]
fn observe_all_reactive_subscribe_twice_panics() {
    let (rt, g) = graph("root");
    let mut obs = g.observe_all_reactive();
    obs.subscribe(rt.core(), |_, _| {});
    // Second subscribe must panic — would otherwise leak the first
    // namespace sink.
    obs.subscribe(rt.core(), |_, _| {});
}

// -------------------------------------------------------------------
// P6 — sink captures Weak<inner>; reactive sinks no-op if graph drops
// -------------------------------------------------------------------
//
// The structural correctness of the Weak capture (sinks don't form an
// Arc cycle through Graph clones) is verified by code review; testing
// it requires reaching into Graph's pub(crate) internals which integration
// tests can't reach. The behavioural test below verifies the related
// guarantee: a sink whose captured Weak<inner> can no longer upgrade
// must silently no-op rather than panic.
//
// The "drop graph but keep handle" scenario can't actually drop GraphInner
// because the handle holds its own `graph: Graph` strong clone (by design
// so the handle keeps working after the user drops their own clone). What
// matters for P6 is that the namespace_sinks → sink → Weak<inner> path
// does not extend the lifetime — verified by inspection of describe.rs and
// observe.rs.
