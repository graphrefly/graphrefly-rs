//! Namespace + sugar API + add/node/try_resolve/name_of (canonical
//! spec §3.5 / §3.9).

mod common;

use common::graph;
use graphrefly_core::{EqualsMode, FnId, HandleId, NO_HANDLE};
use graphrefly_graph::NameError;

#[test]
fn state_registers_under_name_and_resolves() {
    let (rt, g) = graph("system");
    let id = g
        .state(rt.core(), "retry_limit", Some(HandleId::new(7)))
        .unwrap();
    assert_eq!(g.try_resolve("retry_limit"), Some(id));
    assert_eq!(g.node("retry_limit"), id);
    assert_eq!(g.cache_of(rt.core(), id), HandleId::new(7));
}

#[test]
fn state_with_none_initial_starts_sentinel() {
    let (rt, g) = graph("system");
    let id = g.state(rt.core(), "knob", None).unwrap();
    assert_eq!(g.cache_of(rt.core(), id), NO_HANDLE);
}

#[test]
fn name_of_round_trips_after_state_register() {
    let (rt, g) = graph("system");
    let id = g.state(rt.core(), "x", None).unwrap();
    assert_eq!(g.name_of(id).as_deref(), Some("x"));
}

#[test]
fn duplicate_state_name_returns_collision_error() {
    let (rt, g) = graph("system");
    g.state(rt.core(), "x", None).unwrap();
    let err = g.state(rt.core(), "x", None).unwrap_err();
    assert!(matches!(err, NameError::Collision(ref n) if n == "x"));
}

#[test]
fn name_with_path_separator_is_rejected() {
    let (rt, g) = graph("system");
    let err = g.state(rt.core(), "a::b", None).unwrap_err();
    assert!(matches!(err, NameError::InvalidName(ref n) if n == "a::b"));
    // single colons are allowed (R3.5.1)
    g.state(rt.core(), "a:b", None).unwrap();
}

#[test]
#[should_panic(expected = "Graph::node: no node at path `nope`")]
fn node_panics_on_missing_path() {
    let (_rt, g) = graph("system");
    let _ = g.node("nope");
}

#[test]
fn try_resolve_returns_none_on_missing() {
    let (_rt, g) = graph("system");
    assert_eq!(g.try_resolve("nope"), None);
}

#[test]
fn derived_registers_named_with_dep_lookup() {
    let (rt, g) = graph("system");
    let s = g.state(rt.core(), "input", Some(HandleId::new(1))).unwrap();
    let d = g
        .derived(
            rt.core(),
            "validate",
            &[s],
            FnId::new(1),
            EqualsMode::Identity,
        )
        .unwrap();
    assert_eq!(g.node("validate"), d);
    assert_eq!(rt.core().deps_of(d), vec![s]);
}

#[test]
fn add_registers_pre_existing_core_node() {
    let (rt, g) = graph("system");
    // bypass sugar: register raw via Core, then bring into namespace.
    let raw_id = rt.core().register_state(HandleId::new(42), false).unwrap();
    let returned = g.add(rt.core(), raw_id, "raw").unwrap();
    assert_eq!(returned, raw_id);
    assert_eq!(g.node("raw"), raw_id);
}

#[test]
fn node_count_matches_namespace_size() {
    let (rt, g) = graph("system");
    g.state(rt.core(), "a", None).unwrap();
    g.state(rt.core(), "b", None).unwrap();
    g.state(rt.core(), "c", None).unwrap();
    // unnamed Core node — does NOT count in namespace.
    let _hidden = rt.core().register_state(HandleId::new(9), false).unwrap();
    assert_eq!(g.node_count(), 3);
    assert_eq!(rt.core().node_count(), 4);
}

#[test]
fn node_names_preserves_insertion_order() {
    let (rt, g) = graph("system");
    g.state(rt.core(), "z", None).unwrap();
    g.state(rt.core(), "a", None).unwrap();
    g.state(rt.core(), "m", None).unwrap();
    assert_eq!(g.node_names(), vec!["z", "a", "m"]);
}

#[test]
fn graph_clone_shares_namespace() {
    // D246: `Graph` is `Clone` (a cheap `Arc` bump). Clones share the
    // same inner namespace/mount-tree state — the namespace-sharing
    // intent that the pre-D246 `SubgraphRef`/`view()` carried.
    let (rt, g1) = graph("system");
    let g2 = g1.clone();
    let id = g1.state(rt.core(), "x", Some(HandleId::new(5))).unwrap();
    // Visible via the clone.
    assert_eq!(g2.try_resolve("x"), Some(id));
    assert_eq!(g2.cache_of(rt.core(), id), HandleId::new(5));
}

#[test]
fn dynamic_registers_named() {
    let (rt, g) = graph("system");
    let s = g.state(rt.core(), "a", Some(HandleId::new(1))).unwrap();
    let d = g
        .dynamic(rt.core(), "dyn", &[s], FnId::new(1), EqualsMode::Identity)
        .unwrap();
    assert_eq!(g.node("dyn"), d);
}
