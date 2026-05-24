//! D285 — `Graph::resource_profile` substrate (R3.6.3). Mirrors the
//! parity scenarios in `graphrefly-ts`
//! `packages/parity-tests/scenarios/graph/resource-profile.test.ts`
//! that the napi binding will expose cross-arm once D286 lands.
//!
//! D284 amendment scope: the Rust `GraphProfileResult` matches the
//! narrower `ImplGraphProfileResult` — no `valueSizeBytes` per node,
//! no `totalValueSizeBytes` aggregate, no `hotspots.byValueSize`.
//!
//! Spec: `docs/implementation-plan-13.6-canonical-spec.md:984`.

mod common;

use common::graph;
use graphrefly_core::{EqualsMode, FnId, HandleId};
use graphrefly_graph::{GraphProfileOptions, OrphanKind};

#[test]
fn nodecount_edgecount_subgraphcount_match_topology() {
    // Pure-ts test #1 parity: a + b + sum (3 nodes), a→sum + b→sum
    // (2 edges), one mount (1 subgraph).
    let (rt, g) = graph("root");
    let a = g.state(rt.core(), "a", Some(HandleId::new(1))).unwrap();
    let b = g.state(rt.core(), "b", Some(HandleId::new(2))).unwrap();
    g.derived(
        rt.core(),
        "sum",
        &[a, b],
        FnId::new(1),
        EqualsMode::Identity,
    )
    .unwrap();
    let _child = g
        .mount_new(rt.core(), "child".to_string())
        .expect("mount_new");

    let profile = g.resource_profile(rt.core(), None);
    assert_eq!(profile.node_count, 3, "a + b + sum");
    assert_eq!(profile.edge_count, 2, "a→sum + b→sum");
    assert_eq!(profile.subgraph_count, 1, "one mount");
}

#[test]
fn per_node_subscriber_count_reflects_active_subscriptions() {
    // Pure-ts test #2 parity: baseline = 0; +1 after subscribe; back
    // to 0 after unsubscribe.
    use std::rc::Rc;
    let (rt, g) = graph("root");
    let a = g.state(rt.core(), "a", Some(HandleId::new(1))).unwrap();

    // Baseline.
    let p0 = g.resource_profile(rt.core(), None);
    let a0 = p0.nodes.iter().find(|n| n.path == "a").expect("a in nodes");
    assert_eq!(a0.subscriber_count, 0);

    // Subscribe — sink_count_of should increment.
    let sink: graphrefly_core::Sink = Rc::new(|_msgs: &[_]| {});
    let sub_id = rt.core().subscribe(a, sink);
    let p1 = g.resource_profile(rt.core(), None);
    let a1 = p1.nodes.iter().find(|n| n.path == "a").unwrap();
    assert!(
        a1.subscriber_count >= 1,
        "expected >= 1 subscriber, got {}",
        a1.subscriber_count
    );

    // Unsubscribe — count recovers.
    rt.core().unsubscribe(a, sub_id);
    let p2 = g.resource_profile(rt.core(), None);
    let a2 = p2.nodes.iter().find(|n| n.path == "a").unwrap();
    assert_eq!(a2.subscriber_count, 0);
}

#[test]
fn orphan_detection_categorizes_idle_derived_and_excludes_state() {
    // Pure-ts test #3 parity: idle derived nodes appear with
    // OrphanKind::IdleDerived; state nodes are excluded from
    // orphan_kind by construction (pure-ts `profile.ts:129-138`).
    let (rt, g) = graph("root");
    let src = g
        .state(rt.core(), "source", Some(HandleId::new(0)))
        .unwrap();
    g.derived(
        rt.core(),
        "idle_d",
        &[src],
        FnId::new(1),
        EqualsMode::Identity,
    )
    .unwrap();
    g.derived(
        rt.core(),
        "idle_d2",
        &[src],
        FnId::new(2),
        EqualsMode::Identity,
    )
    .unwrap();

    let profile = g.resource_profile(rt.core(), None);

    let orphan_paths: std::collections::HashSet<&str> =
        profile.orphans.iter().map(|p| p.path.as_str()).collect();
    assert!(orphan_paths.contains("idle_d"), "idle_d in orphans");
    assert!(orphan_paths.contains("idle_d2"), "idle_d2 in orphans");

    let idle_d = profile.orphans.iter().find(|p| p.path == "idle_d").unwrap();
    assert_eq!(idle_d.orphan_kind, Some(OrphanKind::IdleDerived));
    assert!(!idle_d.is_orphan_effect, "derived is not an effect");

    // State node MUST be excluded from orphan_kind (parity test #3
    // QA-A3 invariant).
    assert!(
        !orphan_paths.contains("source"),
        "state node should not be in orphans"
    );
    let source = profile
        .nodes
        .iter()
        .find(|n| n.path == "source")
        .expect("source in nodes");
    assert_eq!(source.r#type, "state");
    assert_eq!(source.subscriber_count, 0, "R2.2.6 lazy activation");
    assert!(
        source.orphan_kind.is_none(),
        "state is never categorized as orphan"
    );
}

#[test]
fn hotspots_sorted_descending_and_capped_by_top_n() {
    // Pure-ts test #4 parity: 5 state nodes with varying subscriber
    // counts; top_n=2 cap honored; sorted descending; n0 is #1.
    use std::rc::Rc;
    let (rt, g) = graph("root");
    let mut nodes = Vec::new();
    for i in 0..5 {
        nodes.push(
            g.state(rt.core(), format!("n{i}"), Some(HandleId::new(i)))
                .unwrap(),
        );
    }

    // n0 gets 3 sinks, n1 gets 2, rest get 0.
    let mut subs = Vec::new();
    for _ in 0..3 {
        let sink: graphrefly_core::Sink = Rc::new(|_msgs: &[_]| {});
        subs.push((nodes[0], rt.core().subscribe(nodes[0], sink)));
    }
    for _ in 0..2 {
        let sink: graphrefly_core::Sink = Rc::new(|_msgs: &[_]| {});
        subs.push((nodes[1], rt.core().subscribe(nodes[1], sink)));
    }

    let profile = g.resource_profile(rt.core(), Some(GraphProfileOptions { top_n: Some(2) }));

    // top_n cap honored.
    assert!(profile.hotspots.by_subscriber_count.len() <= 2);
    // Sorted descending.
    let top = &profile.hotspots.by_subscriber_count;
    assert!(top[0].subscriber_count >= top[1].subscriber_count);
    // n0 should be #1 (3 subscribers).
    assert_eq!(top[0].path, "n0");
    assert_eq!(top[0].subscriber_count, 3);

    for (id, sub_id) in subs {
        rt.core().unsubscribe(id, sub_id);
    }
}

#[test]
fn default_top_n_is_ten() {
    // Mirrors pure-ts `topN ?? 10` default.
    let (rt, g) = graph("root");
    // Create 12 state nodes to exceed the default cap.
    for i in 0..12 {
        g.state(rt.core(), format!("n{i}"), Some(HandleId::new(i)))
            .unwrap();
    }

    let profile = g.resource_profile(rt.core(), None);
    assert_eq!(profile.node_count, 12);
    assert_eq!(
        profile.hotspots.by_subscriber_count.len(),
        10,
        "default top_n = 10"
    );
    assert_eq!(profile.hotspots.by_dep_count.len(), 10);
}

#[test]
fn d284_amendment_no_value_size_fields_in_serde_output() {
    // D284 amendment: the Rust GraphProfileResult MUST NOT carry
    // `value_size_bytes` per node, `total_value_size_bytes` aggregate,
    // or `hotspots.by_value_size`. Pinning the shape via JSON
    // serialization — a future refactor that re-introduces these
    // fields will fail this regression.
    let (rt, g) = graph("root");
    g.state(rt.core(), "a", Some(HandleId::new(1))).unwrap();
    let profile = g.resource_profile(rt.core(), None);
    let json_str = serde_json::to_string(&profile).unwrap();

    assert!(
        !json_str.contains("value_size") && !json_str.contains("valueSize"),
        "D284 amendment: no value-size fields, got: {json_str}"
    );
    assert!(
        !json_str.contains("by_value_size") && !json_str.contains("byValueSize"),
        "D284 amendment: no by_value_size hotspot, got: {json_str}"
    );
}

#[test]
fn d286_napi_camel_case_json_keys() {
    // D286 napi: the JSON wire shape MUST use camelCase keys to match
    // the ImplGraphProfileResult contract (cross-arm parity scenarios
    // assert on `subscriberCount` / `depCount` / `isOrphanEffect` /
    // `orphanKind` / `nodeCount` / `edgeCount` / `subgraphCount` /
    // `bySubscriberCount` / `byDepCount` / `orphanEffects`). Snake-case
    // keys would silently false-pass the Rust cargo tests but break
    // cross-arm parity. Pin the positive shape here.
    let (rt, g) = graph("root");
    let a = g.state(rt.core(), "a", Some(HandleId::new(1))).unwrap();
    g.derived(rt.core(), "d", &[a], FnId::new(1), EqualsMode::Identity)
        .unwrap();

    let profile = g.resource_profile(rt.core(), None);
    let json_str = serde_json::to_string(&profile).unwrap();

    // Aggregate keys.
    assert!(json_str.contains("\"nodeCount\""), "got: {json_str}");
    assert!(json_str.contains("\"edgeCount\""), "got: {json_str}");
    assert!(json_str.contains("\"subgraphCount\""), "got: {json_str}");
    assert!(json_str.contains("\"orphanEffects\""), "got: {json_str}");
    // Hotspot dimension keys.
    assert!(
        json_str.contains("\"bySubscriberCount\""),
        "got: {json_str}"
    );
    assert!(json_str.contains("\"byDepCount\""), "got: {json_str}");
    // Per-node keys (idle_d is an orphan → appears in orphans + nodes).
    assert!(json_str.contains("\"subscriberCount\""), "got: {json_str}");
    assert!(json_str.contains("\"depCount\""), "got: {json_str}");
    assert!(json_str.contains("\"isOrphanEffect\""), "got: {json_str}");
    assert!(json_str.contains("\"orphanKind\""), "got: {json_str}");
    // OrphanKind variant is kebab-case (matches Impl
    // `"orphan-effect" | "idle-derived" | "idle-producer"`).
    assert!(json_str.contains("\"idle-derived\""), "got: {json_str}");

    // Negative: no snake_case keys leaked through.
    for snake in [
        "\"node_count\"",
        "\"edge_count\"",
        "\"subgraph_count\"",
        "\"subscriber_count\"",
        "\"dep_count\"",
        "\"is_orphan_effect\"",
        "\"orphan_kind\"",
        "\"by_subscriber_count\"",
        "\"by_dep_count\"",
        "\"orphan_effects\"",
    ] {
        assert!(
            !json_str.contains(snake),
            "snake_case key {snake} leaked: {json_str}"
        );
    }
}
