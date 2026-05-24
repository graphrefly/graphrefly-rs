//! D285 — `Graph::tag_factory` substrate (R3.1.2). Mirrors the parity
//! scenarios in `graphrefly-ts` `packages/parity-tests/scenarios/graph/
//! tag-factory.test.ts` that the napi binding will expose cross-arm
//! once D286 lands.
//!
//! Spec: `docs/implementation-plan-13.6-canonical-spec.md:768`.

mod common;

use common::graph;
use graphrefly_core::HandleId;
use serde_json::json;

#[test]
fn tag_factory_surfaces_factory_and_factory_args_at_top_of_describe() {
    // R3.1.2 — tagFactory(factory, args) populates desc.factory +
    // desc.factory_args at the TOP-LEVEL of the describe output
    // (NOT under per-node meta).
    let (rt, g) = graph("root");
    g.state(rt.core(), "a", Some(HandleId::new(1))).unwrap();

    g.tag_factory(
        "rateLimiter",
        Some(json!({ "maxBuffer": 10, "mode": "shrink" })),
    );

    let d = g.describe(rt.core());
    assert_eq!(d.factory.as_deref(), Some("rateLimiter"));
    assert_eq!(
        d.factory_args,
        Some(json!({ "maxBuffer": 10, "mode": "shrink" }))
    );
}

#[test]
fn second_call_without_args_clears_stale_args() {
    // QA F8 invariant (pure-ts `graph.ts:1686-1689`): a second
    // tag_factory call WITHOUT args MUST clear stale args (re-assign
    // to None) — otherwise `tag_factory("a", {x:1})` then
    // `tag_factory("b", None)` would keep `{x:1}` paired with `"b"`,
    // which is mismatched provenance.
    let (rt, g) = graph("root");
    g.state(rt.core(), "a", Some(HandleId::new(1))).unwrap();

    g.tag_factory("rateLimiter", Some(json!({ "x": 1 })));
    let d1 = g.describe(rt.core());
    assert_eq!(d1.factory.as_deref(), Some("rateLimiter"));
    assert_eq!(d1.factory_args, Some(json!({ "x": 1 })));

    // Second call: only factory name — args MUST clear.
    g.tag_factory("circuitBreaker", None);
    let d2 = g.describe(rt.core());
    assert_eq!(d2.factory.as_deref(), Some("circuitBreaker"));
    assert_eq!(d2.factory_args, None);
}

#[test]
fn cold_call_without_args_omits_factory_args_key_from_json() {
    // Cold tag_factory(name) with no args — `factoryArgs` MUST be
    // omitted entirely from JSON (not serialized as `null`). Mirrors
    // the pure-ts spread-conditional at `graph.ts:3509` pinned by
    // parity test `"factoryArgs" in desc` assertion (QA-A2).
    let (rt, g) = graph("root");
    g.state(rt.core(), "a", Some(HandleId::new(1))).unwrap();

    g.tag_factory("compileSpec", None);
    let d = g.describe(rt.core());
    assert_eq!(d.factory.as_deref(), Some("compileSpec"));
    assert_eq!(d.factory_args, None);

    // Pin the JSON key-omission semantic — the camelCase `factoryArgs`
    // key MUST be absent from the serialized output.
    let json_str = serde_json::to_string(&d).unwrap();
    assert!(
        !json_str.contains("\"factoryArgs\""),
        "factoryArgs key should be omitted (skip_serializing_if), got: {json_str}"
    );
    // `factory` IS present.
    assert!(
        json_str.contains("\"factory\":\"compileSpec\""),
        "factory key should be present, got: {json_str}"
    );
}

#[test]
fn cold_describe_before_any_tag_factory_omits_both_keys() {
    // Untouched describe — both `factory` AND `factoryArgs` keys MUST
    // be omitted (default `None` round-trip).
    let (rt, g) = graph("root");
    g.state(rt.core(), "a", Some(HandleId::new(1))).unwrap();

    let d = g.describe(rt.core());
    assert!(d.factory.is_none());
    assert!(d.factory_args.is_none());

    let json_str = serde_json::to_string(&d).unwrap();
    assert!(!json_str.contains("\"factory\""));
    assert!(!json_str.contains("\"factoryArgs\""));
}

#[test]
fn json_uses_camel_case_factory_args_key() {
    // The Rust field is snake_case `factory_args`, but the JSON wire
    // shape MUST be camelCase `factoryArgs` to match the pure-ts arm
    // byte-for-byte (cross-arm describe consumers depend on this).
    let (rt, g) = graph("root");
    g.tag_factory("foo", Some(json!({ "k": "v" })));
    let d = g.describe(rt.core());
    let json_str = serde_json::to_string(&d).unwrap();
    assert!(
        json_str.contains("\"factoryArgs\""),
        "JSON should use camelCase `factoryArgs`, got: {json_str}"
    );
    assert!(
        !json_str.contains("\"factory_args\""),
        "JSON should NOT use snake_case `factory_args`, got: {json_str}"
    );
}
