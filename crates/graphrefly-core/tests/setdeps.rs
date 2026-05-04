//! `set_deps` integration tests — atomic dep mutation per
//! `~/src/graphrefly-ts/docs/research/rewire-design-notes.md`.
//!
//! Mirrors the scenarios from `wave_protocol_rewire_MC.tla`:
//! - Straight rewire {A} → {B}
//! - Additive rewire {A} → {A, B}
//! - Full removal {A} → {}
//! - Idempotent {A} → {A}
//! - Self-rewire rejection
//! - Cycle rejection
//! - Push-on-subscribe for added dep with cached DATA
//! - Cache preservation (ROM/RAM)

use std::sync::Arc;

mod common;
use common::{TestRuntime, TestValue};
use graphrefly_core::{NodeId, SetDepsError};

#[test]
fn straight_rewire_a_to_b() {
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(10)));
    let b = rt.state(Some(TestValue::Int(20)));
    let calls = Arc::new(std::sync::Mutex::new(0u32));
    let calls_inner = calls.clone();
    let c = rt.derived(&[a.id], move |deps| {
        *calls_inner.lock().unwrap() += 1;
        match &deps[0] {
            TestValue::Int(n) => Some(TestValue::Int(*n)),
            _ => panic!("type"),
        }
    });
    let (_rec, _) = rt.subscribe_recorder(c);
    assert_eq!(rt.cache_value(c), Some(TestValue::Int(10)));
    let calls_before = *calls.lock().unwrap();

    // Rewire C from {A} to {B}.
    rt.core.set_deps(c, &[b.id]).expect("rewire ok");
    // Push-on-subscribe should have fired fn with B's cached value.
    assert_eq!(rt.cache_value(c), Some(TestValue::Int(20)));
    assert!(*calls.lock().unwrap() > calls_before);

    // Now A's emissions should NOT trigger C's fn.
    let calls_after_rewire = *calls.lock().unwrap();
    a.set(TestValue::Int(99));
    assert_eq!(*calls.lock().unwrap(), calls_after_rewire);
    // C should still be 20 (B unchanged).
    assert_eq!(rt.cache_value(c), Some(TestValue::Int(20)));

    // B's emissions DO trigger.
    b.set(TestValue::Int(50));
    assert_eq!(rt.cache_value(c), Some(TestValue::Int(50)));
}

#[test]
fn additive_rewire_a_to_a_b() {
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(10)));
    let b = rt.state(Some(TestValue::Int(20)));
    let c = rt.derived(&[a.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(*n)),
        _ => panic!("type"),
    });
    let (_rec, _) = rt.subscribe_recorder(c);

    // Add B as a dep. C's fn now receives [a_value, b_value].
    // We need a fn that handles both 1-dep and 2-dep cases — but the closure
    // is fixed at registration. So C's fn is registered for 1 dep; rewiring
    // to 2 deps means the SAME closure will receive a 2-element slice.
    // For this test, we rewire and verify the deps[0] still points to A by
    // index (since A was retained at index 0). Ideally tests would update
    // the fn too; production graph.rewire might re-register fn. For v1,
    // we just verify the mechanical behavior.
    //
    // Reusing the same fn that ignores deps[1]:
    rt.core.set_deps(c, &[a.id, b.id]).expect("rewire ok");
    // C should still derive from A's cache (=10).
    assert_eq!(rt.cache_value(c), Some(TestValue::Int(10)));
    // B's emission triggers C — fn re-runs and returns deps[0] (=10).
    b.set(TestValue::Int(30));
    assert_eq!(rt.cache_value(c), Some(TestValue::Int(10)));
}

#[test]
fn full_removal_to_empty_deps() {
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(10)));
    let c = rt.derived(&[a.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(*n)),
        _ => panic!("type"),
    });
    let (_rec, _) = rt.subscribe_recorder(c);
    assert_eq!(rt.cache_value(c), Some(TestValue::Int(10)));

    rt.core.set_deps(c, &[]).expect("rewire to empty ok");
    // Cache preserved per ROM/RAM (active compute node, so cache stays).
    assert_eq!(rt.cache_value(c), Some(TestValue::Int(10)));
    // Future emissions on A no longer affect C.
    a.set(TestValue::Int(99));
    assert_eq!(rt.cache_value(c), Some(TestValue::Int(10)));
}

#[test]
fn idempotent_no_change() {
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(10)));
    let calls = Arc::new(std::sync::Mutex::new(0u32));
    let calls_inner = calls.clone();
    let c = rt.derived(&[a.id], move |deps| {
        *calls_inner.lock().unwrap() += 1;
        match &deps[0] {
            TestValue::Int(n) => Some(TestValue::Int(*n)),
            _ => panic!("type"),
        }
    });
    let (_rec, _) = rt.subscribe_recorder(c);
    let calls_before = *calls.lock().unwrap();

    // Idempotent rewire: same deps.
    rt.core.set_deps(c, &[a.id]).expect("idempotent ok");
    // No additional fires.
    assert_eq!(*calls.lock().unwrap(), calls_before);
}

#[test]
fn self_rewire_rejected() {
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(10)));
    let c = rt.derived(&[a.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(*n)),
        _ => panic!("type"),
    });
    let result = rt.core.set_deps(c, &[c]);
    assert!(matches!(result, Err(SetDepsError::SelfDependency { .. })));
}

#[test]
fn cycle_rejected() {
    let rt = TestRuntime::new();
    // Build chain: A → B → C → D. Rewiring A's deps to include D would create
    // a cycle since D is downstream of A.
    let a = rt.state(Some(TestValue::Int(1)));
    let b = rt.derived(&[a.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(*n + 1)),
        _ => panic!("type"),
    });
    let c = rt.derived(&[b], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(*n + 1)),
        _ => panic!("type"),
    });
    let d = rt.derived(&[c], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(*n + 1)),
        _ => panic!("type"),
    });
    let (_rec, _) = rt.subscribe_recorder(d);

    // Try to rewire B to depend on D. Existing chain: B → C → D. Adding the
    // edge D → B would close a cycle (B → C → D → B).
    let result = rt.core.set_deps(b, &[d]);
    match result {
        Err(SetDepsError::WouldCreateCycle {
            n: cycle_n,
            added_dep,
            path,
        }) => {
            assert_eq!(cycle_n, b);
            assert_eq!(added_dep, d);
            // Path is from n (b) along children to added_dep (d): b → c → d.
            assert_eq!(path.first(), Some(&b));
            assert_eq!(path.last(), Some(&d));
            assert!(
                path.len() >= 2,
                "path should include at least endpoints, got {path:?}"
            );
        }
        other => panic!("expected WouldCreateCycle, got {other:?}"),
    }

    // Sanity: a non-cyclic rewire still works.
    let unrelated = rt.state(Some(TestValue::Int(7)));
    rt.core
        .set_deps(b, &[unrelated.id])
        .expect("unrelated rewire ok");
}

#[test]
fn unknown_node_rejected() {
    let rt = TestRuntime::new();
    let bogus = NodeId::new(99999);
    let result = rt.core.set_deps(bogus, &[]);
    assert!(matches!(result, Err(SetDepsError::UnknownNode(_))));
}

#[test]
fn rewire_state_node_rejected() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));
    let result = rt.core.set_deps(s.id, &[]);
    assert!(matches!(result, Err(SetDepsError::NotComputeNode(_))));
}

#[test]
fn cache_preserved_across_rewire() {
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(100)));
    let b = rt.state(None);
    let c = rt.derived(&[a.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(*n)),
        _ => panic!("type"),
    });
    let (_rec, _) = rt.subscribe_recorder(c);
    let cache_before = rt.cache_value(c);
    assert_eq!(cache_before, Some(TestValue::Int(100)));

    // Rewire to B (which is sentinel — no cached DATA → no push-on-subscribe).
    rt.core.set_deps(c, &[b.id]).expect("rewire ok");
    // Cache preserved per ROM/RAM (Q7).
    assert_eq!(rt.cache_value(c), cache_before);
}
