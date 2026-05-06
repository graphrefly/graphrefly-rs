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
    let _rec = rt.subscribe_recorder(c);
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
    let _rec = rt.subscribe_recorder(c);

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
    let _rec = rt.subscribe_recorder(c);
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
    let _rec = rt.subscribe_recorder(c);
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
    let _rec = rt.subscribe_recorder(d);

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
    let _rec = rt.subscribe_recorder(c);
    let cache_before = rt.cache_value(c);
    assert_eq!(cache_before, Some(TestValue::Int(100)));

    // Rewire to B (which is sentinel — no cached DATA → no push-on-subscribe).
    rt.core.set_deps(c, &[b.id]).expect("rewire ok");
    // Cache preserved per ROM/RAM (Q7).
    assert_eq!(rt.cache_value(c), cache_before);
}

// ---------------------------------------------------------------------------
// Dynamic-node rewire (regression: 2026-05-05 audit fix)
// ---------------------------------------------------------------------------
// Pre-fix bug: after `set_deps` on a dynamic, `tracked` was cleared but
// `has_fired_once` stayed true. The deliver gate
// `!has_fired_once || tracked.contains(&dep_idx)` then resolved to false for
// every new dep, so fn never re-fired and the node sat on stale cache
// derived from the old dep set. The fix in `Core::set_deps` resets
// `has_fired_once = false` for Dynamic so the next dep delivery satisfies
// the first-fire branch.

#[test]
fn dynamic_rewire_refires_fn_on_new_deps() {
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(10)));
    let b = rt.state(Some(TestValue::Int(20)));
    let calls = Arc::new(std::sync::Mutex::new(0u32));
    let calls_inner = calls.clone();
    // Dynamic node: returns deps[0] verbatim and tracks all deps it received.
    let n = rt.dynamic(&[a.id], move |deps| {
        *calls_inner.lock().unwrap() += 1;
        let value = match &deps[0] {
            TestValue::Int(v) => Some(TestValue::Int(*v)),
            _ => None,
        };
        let tracked: Vec<usize> = (0..deps.len()).collect();
        (value, Some(tracked))
    });
    let _rec = rt.subscribe_recorder(n);
    // Dynamic fired once on first activation with [10].
    assert_eq!(*calls.lock().unwrap(), 1);
    assert_eq!(rt.cache_value(n), Some(TestValue::Int(10)));

    // Rewire to [b.id] (cached value 20).
    rt.core.set_deps(n, &[b.id]).expect("rewire ok");
    // Pre-fix: this assertion fails — fn never re-fired so cache stuck at 10.
    assert_eq!(
        rt.cache_value(n),
        Some(TestValue::Int(20)),
        "dynamic must re-fire on rewire to a dep with cached DATA"
    );
    assert!(
        *calls.lock().unwrap() >= 2,
        "fn should have fired at least once post-rewire"
    );

    // Subsequent updates on the new dep continue to fire fn (tracked
    // repopulated by the fn on its post-rewire fire).
    let calls_after_rewire = *calls.lock().unwrap();
    b.set(TestValue::Int(99));
    assert_eq!(rt.cache_value(n), Some(TestValue::Int(99)));
    assert!(*calls.lock().unwrap() > calls_after_rewire);

    // Old dep no longer triggers fires.
    let calls_before_old = *calls.lock().unwrap();
    a.set(TestValue::Int(7));
    assert_eq!(*calls.lock().unwrap(), calls_before_old);
}

// ---------------------------------------------------------------------------
// QA pass — Phase 13.8 Q1 terminal-rejection policy + F1 refcount leak fix
// ---------------------------------------------------------------------------

#[test]
fn rewire_terminal_node_rejected() {
    // Per Phase 13.8 Q1: a terminal node cannot be rewired. Recovery is via
    // the resubscribable subscribe path on a fresh subscriber.
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(10)));
    let b = rt.state(Some(TestValue::Int(20)));
    let c = rt.derived(&[a.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(*n)),
        _ => panic!("type"),
    });
    let _rec = rt.subscribe_recorder(c);
    rt.core.complete(c);
    let result = rt.core.set_deps(c, &[b.id]);
    assert!(matches!(result, Err(SetDepsError::TerminalNode { .. })));
}

#[test]
fn rewire_to_terminal_non_resubscribable_dep_rejected() {
    // Phase 13.8 Q1: terminal non-resubscribable dep is rejected. Adds-only
    // — kept deps are unaffected.
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(10)));
    let b = rt.state(Some(TestValue::Int(20)));
    let c = rt.derived(&[a.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(*n)),
        _ => panic!("type"),
    });
    let _rec = rt.subscribe_recorder(c);
    // Terminate b — it's not resubscribable.
    rt.core.complete(b.id);
    let result = rt.core.set_deps(c, &[b.id]);
    match result {
        Err(SetDepsError::TerminalDep { dep, .. }) => assert_eq!(dep, b.id),
        other => panic!("expected TerminalDep, got {other:?}"),
    }
}

#[test]
fn rewire_to_terminal_resubscribable_dep_accepted() {
    // Phase 13.8 Q1: resubscribable terminal deps are allowed — subscribing
    // / activating them resets their lifecycle.
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(10)));
    let b = rt.state(Some(TestValue::Int(20)));
    rt.core.set_resubscribable(b.id, true);
    let c = rt.derived(&[a.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(*n)),
        _ => panic!("type"),
    });
    let _rec = rt.subscribe_recorder(c);
    // Terminate b but it's flagged resubscribable.
    rt.core.complete(b.id);
    let result = rt.core.set_deps(c, &[b.id]);
    assert!(
        result.is_ok(),
        "resubscribable terminal dep should be accepted"
    );
}

#[test]
fn rewire_with_kept_terminal_dep_does_not_re_check() {
    // Already-present terminal deps (kept across rewire) are not re-checked.
    // Their terminal status was accepted at the time they terminated.
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(10)));
    let b = rt.state(Some(TestValue::Int(20)));
    let c = rt.derived(&[a.id, b.id], |deps| match (&deps[0], &deps[1]) {
        (TestValue::Int(av), TestValue::Int(bv)) => Some(TestValue::Int(av + bv)),
        _ => panic!("type"),
    });
    let _rec = rt.subscribe_recorder(c);
    // Terminate a — c's auto-cascade gating doesn't fire because b is still
    // live. a stays in c's deps as a terminal-but-kept slot.
    rt.core.complete(a.id);
    // Rewire c, keeping a (a stays terminal but is not newly-added → no re-check).
    let result = rt.core.set_deps(c, &[a.id, b.id]);
    assert!(result.is_ok(), "kept terminal dep is not re-validated");
}

#[test]
fn rewire_releases_error_handles_in_removed_dep_slots() {
    // F1 audit fix regression: when a removed dep had `Error(h)` in its
    // dep_terminals slot (retained by terminate_node), set_deps must release
    // the handle to avoid a refcount leak.
    let rt = TestRuntime::new();
    let p = rt.state(Some(TestValue::Int(1)));
    let q = rt.state(Some(TestValue::Int(2)));
    let consumer = rt.derived(&[p.id, q.id], |deps| match (&deps[0], &deps[1]) {
        (TestValue::Int(pv), TestValue::Int(qv)) => Some(TestValue::Int(pv + qv)),
        _ => panic!("type"),
    });
    let _rec = rt.subscribe_recorder(consumer);

    // Trigger ERROR on p. p has only one child (consumer); consumer has one
    // other live dep (q), so consumer does NOT auto-cascade. The retains
    // (after Slice A-bigger /qa item D fix to `Core::error`):
    //   p.terminal Error(err)              → refcount = 1 (terminate_node retain)
    //   consumer.dep_terminals[idx_p] Error(err) → refcount = 2 (cascade retain)
    // (The intern share is released by `Core::error` itself, since
    // `terminate_node` takes its own slot retain.)
    let err = rt.binding.intern(TestValue::Str("p-err".into()));
    rt.core.error(p.id, err);
    let count_after_error = rt.binding.refcount_of(err);
    assert_eq!(
        count_after_error, 2,
        "err refcount = p.terminal(1) + consumer.dep_terminals(1) \
         (intern share released by Core::error)"
    );

    // Rewire consumer to drop p. F1 fix releases the consumer.dep_terminals
    // slot retain for err.
    rt.core.set_deps(consumer, &[q.id]).expect("rewire ok");
    let count_after_rewire = rt.binding.refcount_of(err);
    assert_eq!(
        count_after_rewire, 1,
        "set_deps must release the per-slot Error retain when removing the dep \
         (refcount 2 → 1: p.terminal(1) preserved)"
    );
}

#[test]
fn dynamic_rewire_to_sentinel_dep_holds_until_dep_emits() {
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(10)));
    let b = rt.state(None); // sentinel
    let calls = Arc::new(std::sync::Mutex::new(0u32));
    let calls_inner = calls.clone();
    let n = rt.dynamic(&[a.id], move |deps| {
        *calls_inner.lock().unwrap() += 1;
        let value = match &deps[0] {
            TestValue::Int(v) => Some(TestValue::Int(*v)),
            _ => None,
        };
        (value, Some((0..deps.len()).collect()))
    });
    let _rec = rt.subscribe_recorder(n);
    let calls_at_setup = *calls.lock().unwrap();

    // Rewire to sentinel — push-on-subscribe finds no cache → no delivery →
    // fn doesn't fire.
    rt.core.set_deps(n, &[b.id]).expect("rewire ok");
    assert_eq!(
        *calls.lock().unwrap(),
        calls_at_setup,
        "no fire while new dep is sentinel"
    );

    // Now b emits — first-run gate releases, fn fires.
    b.set(TestValue::Int(50));
    assert!(*calls.lock().unwrap() > calls_at_setup);
    assert_eq!(rt.cache_value(n), Some(TestValue::Int(50)));
}
