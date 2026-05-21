//! Tests for M2 Slice F port-coverage gap fills:
//! R3.2.1 named-sugar wrappers, R3.7.1 signal(), R3.2.3 remove(), R3.3.1 edges().

// D248: substrate is structurally `!Send + !Sync` post-S2c.
#![allow(clippy::arc_with_non_send_sync)]

mod common;

use common::graph;
use graphrefly_core::{EqualsMode, FnId, HandleId, Message, NO_HANDLE};
use graphrefly_graph::{GraphRemoveAudit, RemoveError, SignalKind};
use std::sync::{Arc, Mutex};

fn h(n: u64) -> HandleId {
    HandleId::new(n)
}

// -------------------------------------------------------------------
// R3.2.1 — Named-sugar wrappers
// -------------------------------------------------------------------

#[test]
fn set_and_get_by_name() {
    let (rt, g) = graph("test");
    let s = g.state(rt.core(), "count", Some(h(10))).unwrap();
    assert_eq!(g.get(rt.core(), "count"), h(10));

    g.set(rt.core(), "count", h(20));
    assert_eq!(g.get(rt.core(), "count"), h(20));
    assert_eq!(g.cache_of(rt.core(), s), h(20));
}

#[test]
#[should_panic(expected = "no node at path")]
fn get_panics_on_missing_name() {
    let (rt, g) = graph("test");
    let _ = g.get(rt.core(), "nonexistent");
}

#[test]
#[should_panic(expected = "no node at path")]
fn set_panics_on_missing_name() {
    let (rt, g) = graph("test");
    g.set(rt.core(), "nonexistent", h(1));
}

#[test]
fn invalidate_by_name_clears_cache() {
    let (rt, g) = graph("test");
    let s = g.state(rt.core(), "x", Some(h(5))).unwrap();
    assert_eq!(g.cache_of(rt.core(), s), h(5));
    g.invalidate_by_name(rt.core(), "x");
    assert_eq!(g.cache_of(rt.core(), s), NO_HANDLE);
}

#[test]
fn complete_by_name_terminates() {
    let (rt, g) = graph("test");
    let s = g.state(rt.core(), "x", Some(h(1))).unwrap();

    let events = Arc::new(Mutex::new(Vec::new()));
    let events_clone = events.clone();
    let sub = g.subscribe(
        rt.core(),
        s,
        Arc::new(move |msgs: &[Message]| {
            events_clone.lock().unwrap().extend_from_slice(msgs);
        }),
    );

    g.complete_by_name(rt.core(), "x");
    let evts = events.lock().unwrap();
    assert!(evts.iter().any(|m| matches!(m, Message::Complete)));
    drop(evts);
    g.unsubscribe(rt.core(), s, sub);
}

#[test]
fn error_by_name_terminates_with_handle() {
    let (rt, g) = graph("test");
    let s = g.state(rt.core(), "x", Some(h(1))).unwrap();

    let events = Arc::new(Mutex::new(Vec::new()));
    let events_clone = events.clone();
    let sub = g.subscribe(
        rt.core(),
        s,
        Arc::new(move |msgs: &[Message]| {
            events_clone.lock().unwrap().extend_from_slice(msgs);
        }),
    );

    g.error_by_name(rt.core(), "x", h(99));
    let evts = events.lock().unwrap();
    assert!(evts.iter().any(|m| matches!(m, Message::Error(_))));
    drop(evts);
    g.unsubscribe(rt.core(), s, sub);
}

// -------------------------------------------------------------------
// R3.2.3 — remove(name)
// -------------------------------------------------------------------

#[test]
fn remove_node_sends_teardown_and_clears_namespace() {
    let (rt, g) = graph("test");
    let s = g.state(rt.core(), "temp", Some(h(1))).unwrap();

    let events = Arc::new(Mutex::new(Vec::new()));
    let events_clone = events.clone();
    let sub = g.subscribe(
        rt.core(),
        s,
        Arc::new(move |msgs: &[Message]| {
            events_clone.lock().unwrap().extend_from_slice(msgs);
        }),
    );

    let audit = g.remove(rt.core(), "temp").unwrap();
    assert_eq!(
        audit,
        GraphRemoveAudit {
            node_count: 1,
            mount_count: 0
        }
    );
    // Name is gone from namespace.
    assert!(g.try_resolve("temp").is_none());
    // TEARDOWN was delivered.
    let evts = events.lock().unwrap();
    assert!(evts.iter().any(|m| matches!(m, Message::Teardown)));
    drop(evts);
    g.unsubscribe(rt.core(), s, sub);
}

#[test]
fn remove_mounted_subgraph_delegates_to_unmount() {
    let (rt, g) = graph("root");
    let child = g.mount_new(rt.core(), "child").unwrap();
    child.state(rt.core(), "a", Some(h(1))).unwrap();
    child.state(rt.core(), "b", Some(h(2))).unwrap();

    let audit = g.remove(rt.core(), "child").unwrap();
    assert_eq!(audit.node_count, 2);
    assert_eq!(audit.mount_count, 0); // child itself had 0 sub-mounts
    assert!(g.try_resolve("child::a").is_none());
}

#[test]
fn remove_not_found_returns_error() {
    let (rt, g) = graph("test");
    let err = g.remove(rt.core(), "ghost").unwrap_err();
    assert!(matches!(err, RemoveError::NotFound(_)));
}

#[test]
fn remove_preserves_namespace_during_teardown_cascade() {
    // P1 — sinks observing TEARDOWN must be able to resolve the name
    // mid-cascade. R3.2.3 / R3.7.3 ordering: clear namespace AFTER the
    // teardown returns, mirroring destroy()'s Slice E+ /qa B3 fix.
    let (rt, g) = graph("test");
    let s = g.state(rt.core(), "temp", Some(h(1))).unwrap();

    let g_clone = g.clone();
    let observed_name = Arc::new(Mutex::new(None::<String>));
    let observed_clone = observed_name.clone();
    let sub = g.subscribe(
        rt.core(),
        s,
        Arc::new(move |msgs: &[Message]| {
            for m in msgs {
                if matches!(m, Message::Teardown) {
                    // Try to resolve "temp" mid-cascade. Pre-fix this
                    // returned None because the name was already cleared.
                    *observed_clone.lock().unwrap() = g_clone.name_of(s);
                }
            }
        }),
    );

    g.remove(rt.core(), "temp").unwrap();
    // Mid-cascade lookup observed the name.
    assert_eq!(
        observed_name.lock().unwrap().as_deref(),
        Some("temp"),
        "sink observing TEARDOWN must see namespace name"
    );
    // After remove returns, the name is gone.
    assert_eq!(g.name_of(s), None);
    g.unsubscribe(rt.core(), s, sub);
}

#[test]
fn remove_on_destroyed_graph() {
    let (rt, g) = graph("test");
    g.state(rt.core(), "x", None).unwrap();
    g.destroy(rt.core());
    let err = g.remove(rt.core(), "x").unwrap_err();
    assert!(matches!(err, RemoveError::Destroyed));
}

// -------------------------------------------------------------------
// R3.7.1 — signal(kind)
// -------------------------------------------------------------------

#[test]
fn signal_invalidate_clears_all_named_caches() {
    let (rt, g) = graph("test");
    let a = g.state(rt.core(), "a", Some(h(1))).unwrap();
    let b = g.state(rt.core(), "b", Some(h(2))).unwrap();

    g.signal(rt.core(), SignalKind::Invalidate);
    assert_eq!(g.cache_of(rt.core(), a), NO_HANDLE);
    assert_eq!(g.cache_of(rt.core(), b), NO_HANDLE);
}

#[test]
fn signal_pause_and_resume_round_trip() {
    let (rt, g) = graph("test");
    let s = g.state(rt.core(), "x", Some(h(1))).unwrap();

    let lock = g.alloc_lock_id(rt.core());
    g.signal(rt.core(), SignalKind::Pause(lock));
    assert!(rt.core().is_paused(s));

    g.signal(rt.core(), SignalKind::Resume(lock));
    assert!(!rt.core().is_paused(s));
}

#[test]
fn signal_invalidate_recurses_into_mounts() {
    let (rt, g) = graph("root");
    let child = g.mount_new(rt.core(), "child").unwrap();
    let c = child.state(rt.core(), "x", Some(h(5))).unwrap();

    g.signal(rt.core(), SignalKind::Invalidate);
    assert_eq!(g.cache_of(rt.core(), c), NO_HANDLE);
}

#[test]
fn signal_on_destroyed_graph_is_noop() {
    let (rt, g) = graph("test");
    g.state(rt.core(), "x", Some(h(1))).unwrap();
    g.destroy(rt.core());
    // Should not panic.
    g.signal(rt.core(), SignalKind::Invalidate);
}

// -------------------------------------------------------------------
// R3.3.1 — edges(opts)
// -------------------------------------------------------------------

#[test]
fn edges_local_only() {
    let (rt, g) = graph("test");
    let a = g.state(rt.core(), "a", None).unwrap();
    g.derived(rt.core(), "b", &[a], FnId::new(1), EqualsMode::Identity)
        .unwrap();

    let edges = g.edges(rt.core(), false);
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0], ("a".to_string(), "b".to_string()));
}

#[test]
fn edges_recursive_into_mounts() {
    let (rt, g) = graph("root");
    let a = g.state(rt.core(), "a", None).unwrap();
    g.derived(rt.core(), "b", &[a], FnId::new(1), EqualsMode::Identity)
        .unwrap();

    let child = g.mount_new(rt.core(), "child").unwrap();
    let c = child.state(rt.core(), "c", None).unwrap();
    child
        .derived(rt.core(), "d", &[c], FnId::new(2), EqualsMode::Identity)
        .unwrap();

    let edges = g.edges(rt.core(), true);
    assert_eq!(edges.len(), 2);
    assert!(edges.contains(&("a".to_string(), "b".to_string())));
    assert!(edges.contains(&("child::c".to_string(), "child::d".to_string())));
}

#[test]
fn edges_cross_graph_dep_shows_as_anon() {
    let (rt, g) = graph("test");
    // Register a node through Core without naming it.
    let anon = rt.core().register_state(h(1), false).unwrap();
    g.derived(rt.core(), "d", &[anon], FnId::new(1), EqualsMode::Identity)
        .unwrap();

    let edges = g.edges(rt.core(), false);
    assert_eq!(edges.len(), 1);
    assert!(edges[0].0.starts_with("_anon_"));
    assert_eq!(edges[0].1, "d");
}

#[test]
fn edges_empty_graph() {
    let (rt, g) = graph("empty");
    assert!(g.edges(rt.core(), false).is_empty());
    assert!(g.edges(rt.core(), true).is_empty());
}
