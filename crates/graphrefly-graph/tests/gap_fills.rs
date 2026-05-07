//! Tests for M2 Slice F port-coverage gap fills:
//! R3.2.1 named-sugar wrappers, R3.7.1 signal(), R3.2.3 remove(), R3.3.1 edges().

mod common;

use graphrefly_core::{EqualsMode, FnId, HandleId, Message, NO_HANDLE};
use graphrefly_graph::{Graph, GraphRemoveAudit, RemoveError, SignalKind};
use std::sync::{Arc, Mutex};

fn h(n: u64) -> HandleId {
    HandleId::new(n)
}

// -------------------------------------------------------------------
// R3.2.1 — Named-sugar wrappers
// -------------------------------------------------------------------

#[test]
fn set_and_get_by_name() {
    let g = Graph::new("test", common::binding());
    let s = g.state("count", Some(h(10))).unwrap();
    assert_eq!(g.get("count"), h(10));

    g.set("count", h(20));
    assert_eq!(g.get("count"), h(20));
    assert_eq!(g.cache_of(s), h(20));
}

#[test]
#[should_panic(expected = "no node at path")]
fn get_panics_on_missing_name() {
    let g = Graph::new("test", common::binding());
    let _ = g.get("nonexistent");
}

#[test]
#[should_panic(expected = "no node at path")]
fn set_panics_on_missing_name() {
    let g = Graph::new("test", common::binding());
    g.set("nonexistent", h(1));
}

#[test]
fn invalidate_by_name_clears_cache() {
    let g = Graph::new("test", common::binding());
    let s = g.state("x", Some(h(5))).unwrap();
    assert_eq!(g.cache_of(s), h(5));
    g.invalidate_by_name("x");
    assert_eq!(g.cache_of(s), NO_HANDLE);
}

#[test]
fn complete_by_name_terminates() {
    let g = Graph::new("test", common::binding());
    let s = g.state("x", Some(h(1))).unwrap();

    let events = Arc::new(Mutex::new(Vec::new()));
    let events_clone = events.clone();
    let _sub = g.subscribe(
        s,
        Arc::new(move |msgs: &[Message]| {
            events_clone.lock().unwrap().extend_from_slice(msgs);
        }),
    );

    g.complete_by_name("x");
    let evts = events.lock().unwrap();
    assert!(evts.iter().any(|m| matches!(m, Message::Complete)));
}

#[test]
fn error_by_name_terminates_with_handle() {
    let g = Graph::new("test", common::binding());
    let s = g.state("x", Some(h(1))).unwrap();

    let events = Arc::new(Mutex::new(Vec::new()));
    let events_clone = events.clone();
    let _sub = g.subscribe(
        s,
        Arc::new(move |msgs: &[Message]| {
            events_clone.lock().unwrap().extend_from_slice(msgs);
        }),
    );

    g.error_by_name("x", h(99));
    let evts = events.lock().unwrap();
    assert!(evts.iter().any(|m| matches!(m, Message::Error(_))));
}

// -------------------------------------------------------------------
// R3.2.3 — remove(name)
// -------------------------------------------------------------------

#[test]
fn remove_node_sends_teardown_and_clears_namespace() {
    let g = Graph::new("test", common::binding());
    let s = g.state("temp", Some(h(1))).unwrap();

    let events = Arc::new(Mutex::new(Vec::new()));
    let events_clone = events.clone();
    let _sub = g.subscribe(
        s,
        Arc::new(move |msgs: &[Message]| {
            events_clone.lock().unwrap().extend_from_slice(msgs);
        }),
    );

    let audit = g.remove("temp").unwrap();
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
}

#[test]
fn remove_mounted_subgraph_delegates_to_unmount() {
    let g = Graph::new("root", common::binding());
    let child = g.mount_new("child").unwrap();
    child.state("a", Some(h(1))).unwrap();
    child.state("b", Some(h(2))).unwrap();

    let audit = g.remove("child").unwrap();
    assert_eq!(audit.node_count, 2);
    assert_eq!(audit.mount_count, 0); // child itself had 0 sub-mounts
    assert!(g.try_resolve("child::a").is_none());
}

#[test]
fn remove_not_found_returns_error() {
    let g = Graph::new("test", common::binding());
    let err = g.remove("ghost").unwrap_err();
    assert!(matches!(err, RemoveError::NotFound(_)));
}

#[test]
fn remove_preserves_namespace_during_teardown_cascade() {
    // P1 — sinks observing TEARDOWN must be able to resolve the name
    // mid-cascade. R3.2.3 / R3.7.3 ordering: clear namespace AFTER the
    // teardown returns, mirroring destroy()'s Slice E+ /qa B3 fix.
    let g = Graph::new("test", common::binding());
    let s = g.state("temp", Some(h(1))).unwrap();

    let g_clone = g.clone();
    let observed_name = Arc::new(Mutex::new(None::<String>));
    let observed_clone = observed_name.clone();
    let _sub = g.subscribe(
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

    g.remove("temp").unwrap();
    // Mid-cascade lookup observed the name.
    assert_eq!(
        observed_name.lock().unwrap().as_deref(),
        Some("temp"),
        "sink observing TEARDOWN must see namespace name"
    );
    // After remove returns, the name is gone.
    assert_eq!(g.name_of(s), None);
}

#[test]
fn remove_on_destroyed_graph() {
    let g = Graph::new("test", common::binding());
    g.state("x", None).unwrap();
    g.destroy();
    let err = g.remove("x").unwrap_err();
    assert!(matches!(err, RemoveError::Destroyed));
}

// -------------------------------------------------------------------
// R3.7.1 — signal(kind)
// -------------------------------------------------------------------

#[test]
fn signal_invalidate_clears_all_named_caches() {
    let g = Graph::new("test", common::binding());
    let a = g.state("a", Some(h(1))).unwrap();
    let b = g.state("b", Some(h(2))).unwrap();

    g.signal(SignalKind::Invalidate);
    assert_eq!(g.cache_of(a), NO_HANDLE);
    assert_eq!(g.cache_of(b), NO_HANDLE);
}

#[test]
fn signal_pause_and_resume_round_trip() {
    let g = Graph::new("test", common::binding());
    let s = g.state("x", Some(h(1))).unwrap();

    let lock = g.alloc_lock_id();
    g.signal(SignalKind::Pause(lock));
    assert!(g.core().is_paused(s));

    g.signal(SignalKind::Resume(lock));
    assert!(!g.core().is_paused(s));
}

#[test]
fn signal_invalidate_recurses_into_mounts() {
    let g = Graph::new("root", common::binding());
    let child = g.mount_new("child").unwrap();
    let c = child.state("x", Some(h(5))).unwrap();

    g.signal(SignalKind::Invalidate);
    assert_eq!(g.cache_of(c), NO_HANDLE);
}

#[test]
fn signal_on_destroyed_graph_is_noop() {
    let g = Graph::new("test", common::binding());
    g.state("x", Some(h(1))).unwrap();
    g.destroy();
    // Should not panic.
    g.signal(SignalKind::Invalidate);
}

// -------------------------------------------------------------------
// R3.3.1 — edges(opts)
// -------------------------------------------------------------------

#[test]
fn edges_local_only() {
    let g = Graph::new("test", common::binding());
    let a = g.state("a", None).unwrap();
    g.derived("b", &[a], FnId::new(1), EqualsMode::Identity)
        .unwrap();

    let edges = g.edges(false);
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0], ("a".to_string(), "b".to_string()));
}

#[test]
fn edges_recursive_into_mounts() {
    let g = Graph::new("root", common::binding());
    let a = g.state("a", None).unwrap();
    g.derived("b", &[a], FnId::new(1), EqualsMode::Identity)
        .unwrap();

    let child = g.mount_new("child").unwrap();
    let c = child.state("c", None).unwrap();
    child
        .derived("d", &[c], FnId::new(2), EqualsMode::Identity)
        .unwrap();

    let edges = g.edges(true);
    assert_eq!(edges.len(), 2);
    assert!(edges.contains(&("a".to_string(), "b".to_string())));
    assert!(edges.contains(&("child::c".to_string(), "child::d".to_string())));
}

#[test]
fn edges_cross_graph_dep_shows_as_anon() {
    let g = Graph::new("test", common::binding());
    // Register a node through Core without naming it.
    let anon = g.core().register_state(h(1), false).unwrap();
    g.derived("d", &[anon], FnId::new(1), EqualsMode::Identity)
        .unwrap();

    let edges = g.edges(false);
    assert_eq!(edges.len(), 1);
    assert!(edges[0].0.starts_with("_anon_"));
    assert_eq!(edges[0].1, "d");
}

#[test]
fn edges_empty_graph() {
    let g = Graph::new("empty", common::binding());
    assert!(g.edges(false).is_empty());
    assert!(g.edges(true).is_empty());
}
