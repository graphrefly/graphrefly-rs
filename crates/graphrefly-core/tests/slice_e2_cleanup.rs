//! Slice E2 — fn ctx + cleanup hooks (R2.4.5 / R2.4.6 / Lock 4.A / Lock 6.D).
//!
//! Locks D054 / D055 / D056 / D057 / D058 / D059 / D060 / D061 in
//! `~/src/graphrefly-ts/docs/rust-port-decisions.md`. Full design lives in
//! `~/src/graphrefly-ts/archive/docs/SESSION-rust-port-fn-ctx-cleanup.md`.
//!
//! Test scenarios mirror session doc §9 (15 mandatory tests).

mod common;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use common::{CleanupClosure, TestNodeFnCleanup, TestRuntime, TestValue};
use graphrefly_core::CleanupTrigger;

/// Helper: build a CleanupClosure that bumps an AtomicU64 each fire.
fn counter_hook(counter: Arc<AtomicU64>) -> CleanupClosure {
    Arc::new(move || {
        counter.fetch_add(1, Ordering::SeqCst);
    })
}

// =====================================================================
// Test 1 — R2.4.5 OnRerun fires before second invoke_fn
// =====================================================================

#[test]
fn r2_4_5_on_rerun_fires_before_second_fn() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));
    let derived_node = rt.derived(&[s.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(n * 10)),
        _ => panic!("type"),
    });
    let _rec = rt.subscribe_recorder(derived_node);

    let on_rerun_count = Arc::new(AtomicU64::new(0));
    rt.binding.register_cleanup(
        derived_node,
        TestNodeFnCleanup {
            on_rerun: Some(counter_hook(on_rerun_count.clone())),
            ..Default::default()
        },
    );

    // First fn run already happened during subscribe activation. OnRerun
    // should NOT have fired yet (no prior fn run before this one).
    assert_eq!(
        on_rerun_count.load(Ordering::SeqCst),
        0,
        "OnRerun must not fire before any prior fn run"
    );

    // Second emit → second fn fire → OnRerun fires once before invoke_fn.
    s.set(TestValue::Int(2));

    assert_eq!(
        on_rerun_count.load(Ordering::SeqCst),
        1,
        "OnRerun fires before second invoke_fn"
    );
    let calls = rt.binding.cleanup_calls_for(CleanupTrigger::OnRerun);
    assert_eq!(calls, vec![derived_node]);
}

// =====================================================================
// Test 2 — R2.4.5 OnRerun is skipped on first fire
// =====================================================================

#[test]
fn r2_4_5_on_rerun_skipped_on_first_fire() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(7)));
    // Pre-register cleanup BEFORE the derived node is subscribed (and thus
    // before its first fire). When the first fire happens, has_fired_once
    // is false → OnRerun should not fire even though current_cleanup is
    // populated.
    let derived_node = rt.derived(&[s.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(n + 1)),
        _ => panic!("type"),
    });
    let on_rerun_count = Arc::new(AtomicU64::new(0));
    rt.binding.register_cleanup(
        derived_node,
        TestNodeFnCleanup {
            on_rerun: Some(counter_hook(on_rerun_count.clone())),
            ..Default::default()
        },
    );

    let _rec = rt.subscribe_recorder(derived_node);

    assert_eq!(
        on_rerun_count.load(Ordering::SeqCst),
        0,
        "first fire skips OnRerun (no prior run to clean up)"
    );
    // No `cleanup_for(OnRerun)` call should have been recorded.
    assert!(rt
        .binding
        .cleanup_calls_for(CleanupTrigger::OnRerun)
        .is_empty());
}

// =====================================================================
// Test 3 — R2.4.5 OnDeactivation fires when last sub leaves
// =====================================================================

#[test]
fn r2_4_5_on_deactivation_fires_on_last_unsub() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));
    let derived_node = rt.derived(&[s.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(*n)),
        _ => panic!("type"),
    });
    let on_deact_count = Arc::new(AtomicU64::new(0));
    rt.binding.register_cleanup(
        derived_node,
        TestNodeFnCleanup {
            on_deactivation: Some(counter_hook(on_deact_count.clone())),
            ..Default::default()
        },
    );

    let rec = rt.subscribe_recorder(derived_node);
    drop(rec); // last subscriber leaves

    assert_eq!(
        on_deact_count.load(Ordering::SeqCst),
        1,
        "OnDeactivation fires once on last-sub-drop"
    );
}

// =====================================================================
// Test 4 — D056 OnDeactivation fires BEFORE producer_deactivate
// =====================================================================

#[test]
fn r2_4_5_on_deactivation_precedes_producer_deactivate() {
    use graphrefly_core::{BindingBoundary, CleanupTrigger as CT, NodeId};
    use std::sync::Mutex;

    // Custom binding that records the order of cleanup_for + producer_deactivate
    // calls so we can verify cleanup-first ordering per D056.
    struct OrderingBinding {
        inner: Arc<common::TestBinding>,
        events: Mutex<Vec<String>>,
    }

    impl BindingBoundary for OrderingBinding {
        fn invoke_fn(
            &self,
            n: NodeId,
            f: graphrefly_core::FnId,
            d: &[graphrefly_core::DepBatch],
        ) -> graphrefly_core::FnResult {
            self.inner.invoke_fn(n, f, d)
        }
        fn custom_equals(
            &self,
            f: graphrefly_core::FnId,
            a: graphrefly_core::HandleId,
            b: graphrefly_core::HandleId,
        ) -> bool {
            self.inner.custom_equals(f, a, b)
        }
        fn release_handle(&self, h: graphrefly_core::HandleId) {
            self.inner.release_handle(h);
        }
        fn retain_handle(&self, h: graphrefly_core::HandleId) {
            self.inner.retain_handle(h);
        }
        fn cleanup_for(&self, n: NodeId, t: CT) {
            self.events
                .lock()
                .unwrap()
                .push(format!("cleanup_for({n:?}, {t:?})"));
            self.inner.cleanup_for(n, t);
        }
        fn producer_deactivate(&self, n: NodeId) {
            self.events
                .lock()
                .unwrap()
                .push(format!("producer_deactivate({n:?})"));
            self.inner.producer_deactivate(n);
        }
        fn wipe_ctx(&self, n: NodeId) {
            self.inner.wipe_ctx(n);
        }
    }

    let test_binding = common::TestBinding::new();
    let ordering = Arc::new(OrderingBinding {
        inner: test_binding.clone(),
        events: Mutex::new(Vec::new()),
    });
    let core = graphrefly_core::Core::new(ordering.clone() as Arc<dyn BindingBoundary>);

    // Register a producer fn that emits one DATA on invoke_fn.
    let producer_fn = test_binding.register_fn(|_| Some(TestValue::Int(42)));
    let prod_id = core.register_producer(producer_fn).unwrap();

    let on_deact_count = Arc::new(AtomicU64::new(0));
    test_binding.register_cleanup(
        prod_id,
        TestNodeFnCleanup {
            on_deactivation: Some(counter_hook(on_deact_count.clone())),
            ..Default::default()
        },
    );

    let sink: graphrefly_core::Sink = Arc::new(|_| {});
    let sub = core.subscribe(prod_id, sink);
    drop(sub);

    let events = ordering.events.lock().unwrap().clone();
    let cleanup_idx = events
        .iter()
        .position(|s| s.starts_with("cleanup_for"))
        .expect("cleanup_for fired");
    let prod_deact_idx = events
        .iter()
        .position(|s| s.starts_with("producer_deactivate"))
        .expect("producer_deactivate fired");
    assert!(
        cleanup_idx < prod_deact_idx,
        "OnDeactivation must fire BEFORE producer_deactivate (D056); got {events:?}"
    );
    assert_eq!(on_deact_count.load(Ordering::SeqCst), 1);
}

// =====================================================================
// Test 5 — OnDeactivation gated on has_fired_once (never-fired skipped)
// =====================================================================

#[test]
fn r2_4_5_on_deactivation_skipped_on_never_fired() {
    let rt = TestRuntime::new();
    // Sentinel-initialized state node never fires its fn (state nodes
    // don't have fns at all, so has_fired_once stays false unless they
    // were initialized with a value or received an emit).
    let s = rt.state(None);

    let on_deact_count = Arc::new(AtomicU64::new(0));
    rt.binding.register_cleanup(
        s.id,
        TestNodeFnCleanup {
            on_deactivation: Some(counter_hook(on_deact_count.clone())),
            ..Default::default()
        },
    );

    let rec = rt.subscribe_recorder(s.id);
    drop(rec);

    assert_eq!(
        on_deact_count.load(Ordering::SeqCst),
        0,
        "never-fired node skips OnDeactivation"
    );
    assert!(rt
        .binding
        .cleanup_calls_for(CleanupTrigger::OnDeactivation)
        .is_empty());
}

// =====================================================================
// Test 6 — R1.3.9.b dedup: diamond fan-in fires OnInvalidate once per node
// =====================================================================

#[test]
fn r1_3_9_b_on_invalidate_dedup_diamond() {
    // A → B; A → C; (B, C) → D. Invalidate A — D's onInvalidate must
    // fire exactly once (not once per fan-in path).
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(10)));
    let b = rt.derived(&[a.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(n + 1)),
        _ => panic!("type"),
    });
    let c = rt.derived(&[a.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(n + 100)),
        _ => panic!("type"),
    });
    let d = rt.derived(&[b, c], |deps| {
        if let (TestValue::Int(x), TestValue::Int(y)) = (&deps[0], &deps[1]) {
            Some(TestValue::Int(x + y))
        } else {
            panic!("type");
        }
    });
    let _rec = rt.subscribe_recorder(d);
    // Sanity: D fires once on activation.

    let d_invalidate_count = Arc::new(AtomicU64::new(0));
    rt.binding.register_cleanup(
        d,
        TestNodeFnCleanup {
            on_invalidate: Some(counter_hook(d_invalidate_count.clone())),
            ..Default::default()
        },
    );

    rt.core.invalidate(a.id);

    assert_eq!(
        d_invalidate_count.load(Ordering::SeqCst),
        1,
        "OnInvalidate fires exactly once on D despite diamond fan-in (R1.3.9.b)"
    );
    let invalidate_targets = rt.binding.cleanup_calls_for(CleanupTrigger::OnInvalidate);
    let d_count = invalidate_targets.iter().filter(|n| **n == d).count();
    assert_eq!(d_count, 1, "Core fired cleanup_for(d, OnInvalidate) once");
}

// =====================================================================
// Test 7 — R1.3.9.c never-populated case: OnInvalidate skipped
// =====================================================================

#[test]
fn r1_3_9_c_on_invalidate_skipped_on_never_populated() {
    let rt = TestRuntime::new();
    let s = rt.state(None);
    let _rec = rt.subscribe_recorder(s.id);

    let on_inv_count = Arc::new(AtomicU64::new(0));
    rt.binding.register_cleanup(
        s.id,
        TestNodeFnCleanup {
            on_invalidate: Some(counter_hook(on_inv_count.clone())),
            ..Default::default()
        },
    );

    rt.core.invalidate(s.id);

    assert_eq!(
        on_inv_count.load(Ordering::SeqCst),
        0,
        "OnInvalidate skipped when cache is never-populated sentinel (R1.3.9.c)"
    );
    assert!(rt
        .binding
        .cleanup_calls_for(CleanupTrigger::OnInvalidate)
        .is_empty());
}

// =====================================================================
// Test 8 — R2.4.6 ctx.store persists across deactivation
// =====================================================================

#[test]
fn r2_4_6_store_persists_across_deactivation() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let derived_node = rt.derived(&[s.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(*n)),
        _ => panic!("type"),
    });

    // First subscribe → fn fires → write to store.
    {
        let _rec = rt.subscribe_recorder(derived_node);
        rt.binding
            .store_set(derived_node, "counter", TestValue::Int(7));
        // _rec drops at scope exit → onDeactivation fires (none registered)
        // and store does NOT get wiped (R2.4.6 default: persists across
        // deactivation).
    }

    // Verify store survived deactivation.
    assert_eq!(
        rt.binding.store_get(derived_node, "counter"),
        Some(TestValue::Int(7)),
        "store persists across deactivation by default per R2.4.6"
    );
    assert!(
        rt.binding.has_ctx(derived_node),
        "NodeCtxState entry remains after deactivation"
    );

    // Re-subscribe → first fire of new activation cycle → store still has
    // prior value.
    {
        let _rec2 = rt.subscribe_recorder(derived_node);
        assert_eq!(
            rt.binding.store_get(derived_node, "counter"),
            Some(TestValue::Int(7)),
            "second activation reads prior store value"
        );
    }
}

// =====================================================================
// Test 9 — R2.4.6 store wiped on resubscribable terminal reset
// =====================================================================

#[test]
fn r2_4_6_store_wiped_on_resubscribable_reset() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    rt.core.set_resubscribable(s.id, true);

    let _rec = rt.subscribe_recorder(s.id);
    rt.binding.store_set(s.id, "scratch", TestValue::Int(99));
    assert_eq!(
        rt.binding.store_get(s.id, "scratch"),
        Some(TestValue::Int(99))
    );

    // Drop subscriber, complete the resubscribable node, then re-subscribe.
    drop(_rec);
    rt.core.complete(s.id);

    // Re-subscribe triggers reset_for_fresh_lifecycle → wipe_ctx.
    let _rec2 = rt.subscribe_recorder(s.id);

    assert_eq!(
        rt.binding.store_get(s.id, "scratch"),
        None,
        "store wiped on resubscribable terminal reset per R2.4.6"
    );
    assert!(
        !rt.binding.has_ctx(s.id),
        "NodeCtxState entry removed entirely on wipe_ctx"
    );
    // D069 (Slice E2 /qa Q2(b)) interaction: when subs are empty at
    // terminate AND a re-subscribe later arrives, wipe fires TWICE —
    // once via `terminate_node`'s eager `pending_wipes` drain, once via
    // `subscribe`'s `reset_for_fresh_lifecycle` defensive safety-net.
    // The second is idempotent (`HashMap::remove` on absent key is a
    // no-op) — the safety net covers the rare edge case where subs are
    // still alive at terminate AND a new subscribe arrives before the
    // old subs drop (in which case D069 didn't fire eager and only the
    // reset path wipes). Both wipes target this same node.
    let wipes = rt.binding.wipe_calls();
    assert!(
        wipes == vec![s.id] || wipes == vec![s.id, s.id],
        "wipe_ctx fired for the resubscribable node (eagerly via D069 \
         and/or via subscribe reset path); got: {wipes:?}"
    );
}

// =====================================================================
// Test 10 — OnDeactivation runs BEFORE wipe on resubscribable reset
// =====================================================================

#[test]
fn on_deactivation_runs_before_wipe_on_terminal_reset() {
    use std::sync::Mutex;

    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    rt.core.set_resubscribable(s.id, true);
    let derived_node = rt.derived(&[s.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(*n)),
        _ => panic!("type"),
    });
    rt.core.set_resubscribable(derived_node, true);

    // OnDeactivation hook records what it sees in store at fire-time —
    // proving it ran BEFORE wipe.
    let observed_store_at_deact: Arc<Mutex<Option<TestValue>>> = Arc::new(Mutex::new(None));
    let binding = rt.binding.clone();
    let observed = observed_store_at_deact.clone();
    let on_deact: CleanupClosure = Arc::new(move || {
        let v = binding.store_get(derived_node, "k");
        *observed.lock().unwrap() = v;
    });
    rt.binding.register_cleanup(
        derived_node,
        TestNodeFnCleanup {
            on_deactivation: Some(on_deact),
            ..Default::default()
        },
    );

    // First lifecycle: subscribe, write store, complete, drop sub.
    let rec = rt.subscribe_recorder(derived_node);
    rt.binding
        .store_set(derived_node, "k", TestValue::Int(2026));
    drop(rec);
    rt.core.complete(derived_node);

    // OnDeactivation fired during drop(rec) — should have seen pre-wipe value.
    assert_eq!(
        *observed_store_at_deact.lock().unwrap(),
        Some(TestValue::Int(2026)),
        "OnDeactivation fires BEFORE wipe; sees pre-wipe store"
    );

    // Re-subscribe → triggers wipe.
    let _rec2 = rt.subscribe_recorder(derived_node);
    assert!(
        !rt.binding.has_ctx(derived_node),
        "store wiped after re-subscribe (post-OnDeactivation)"
    );
}

// =====================================================================
// Test 11 — D045 / D060 cleanup re-entrance into Core (lock-released)
// =====================================================================

#[test]
fn cleanup_can_reenter_core_lock_released() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));
    let other = rt.state(Some(TestValue::Int(0)));
    let derived_node = rt.derived(&[s.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(*n)),
        _ => panic!("type"),
    });

    let other_id = other.id;
    let other_binding = rt.binding.clone();
    let other_core = rt.core.clone();
    // OnDeactivation re-enters Core to emit on `other`. If lock-released
    // discipline is correct, this composes naturally; if Core's state
    // lock were held, this would deadlock.
    let on_deact: CleanupClosure = Arc::new(move || {
        let h = other_binding.intern(TestValue::Int(42));
        other_core.emit(other_id, h);
    });
    rt.binding.register_cleanup(
        derived_node,
        TestNodeFnCleanup {
            on_deactivation: Some(on_deact),
            ..Default::default()
        },
    );

    let rec = rt.subscribe_recorder(derived_node);
    drop(rec);

    // The re-entrant emit should have updated `other`'s cache.
    assert_eq!(
        rt.cache_value(other_id),
        Some(TestValue::Int(42)),
        "cleanup re-entered Core::emit lock-released; other's cache updated"
    );
}

// =====================================================================
// Test 12 — D060 OnRerun panic does not corrupt wave / next emit fires fn
// =====================================================================

#[test]
fn on_rerun_panic_isolated_does_not_corrupt_wave() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));
    let derived_node = rt.derived(&[s.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(n * 2)),
        _ => panic!("type"),
    });
    let _rec = rt.subscribe_recorder(derived_node);
    // Sanity: first fire happened; cache = 2.
    assert_eq!(rt.cache_value(derived_node), Some(TestValue::Int(2)));

    // Register OnRerun that panics. Binding catches per D060 default
    // (propagate_cleanup_panics = false); the panic is recorded but
    // doesn't propagate to Core.
    let panicking: CleanupClosure = Arc::new(|| panic!("test panic in OnRerun"));
    rt.binding.register_cleanup(
        derived_node,
        TestNodeFnCleanup {
            on_rerun: Some(panicking),
            ..Default::default()
        },
    );

    // Trigger second fire — OnRerun panics but binding catches.
    s.set(TestValue::Int(3));

    // Verify: panic was caught binding-side; wave still completed.
    assert_eq!(
        rt.binding.cleanup_panics(),
        vec![(derived_node, CleanupTrigger::OnRerun)],
        "panicking OnRerun was caught + recorded"
    );
    // Wave proceeded past the panic — fn still fired and updated cache.
    assert_eq!(
        rt.cache_value(derived_node),
        Some(TestValue::Int(6)),
        "wave completed despite OnRerun panic; second fn fire ran"
    );

    // A third emit should still work (currently_firing stack must be
    // balanced — OnRerun fires OUTSIDE FiringGuard so this would only
    // fail if Phase 1.5 corrupted state, which the wave-engine teardown
    // would have papered over).
    s.set(TestValue::Int(5));
    assert_eq!(rt.cache_value(derived_node), Some(TestValue::Int(10)));
}

// =====================================================================
// Test 13 — OnInvalidate fires at cache-clear, not at wire-delivery (D058)
// =====================================================================

#[test]
fn on_invalidate_fires_at_cache_clear_not_replay() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));
    let derived_node = rt.derived(&[s.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(*n)),
        _ => panic!("type"),
    });
    let _rec = rt.subscribe_recorder(derived_node);

    let on_inv_count = Arc::new(AtomicU64::new(0));
    rt.binding.register_cleanup(
        derived_node,
        TestNodeFnCleanup {
            on_invalidate: Some(counter_hook(on_inv_count.clone())),
            ..Default::default()
        },
    );

    // Pause the derived node, then invalidate. Cache should clear
    // immediately and OnInvalidate should fire at the wave end (not
    // deferred to a future resume).
    let lock_id = rt.core.alloc_lock_id();
    rt.core.pause(derived_node, lock_id).unwrap();
    rt.core.invalidate(s.id);

    assert_eq!(
        on_inv_count.load(Ordering::SeqCst),
        1,
        "OnInvalidate fires at cache-clear time (D058); pause buffering doesn't defer the hook"
    );
    // Resume — the buffered INVALIDATE wire delivery happens, but
    // OnInvalidate count must NOT increment again.
    rt.core.resume(derived_node, lock_id).unwrap();
    assert_eq!(
        on_inv_count.load(Ordering::SeqCst),
        1,
        "OnInvalidate did not re-fire on wire replay"
    );
}

// =====================================================================
// Test 14 — State nodes (no fn_id) skip OnRerun via the fire_regular gate
// =====================================================================
//
// State nodes have no fn (`rec.fn_id == None`), so `fire_regular`'s
// Phase 1 snapshot returns `None` and Phase 1.5 OnRerun is never reached
// for them. This test exercises only the state-node case. Operator nodes
// dispatch via `fire_operator` (a separate path from `fire_regular`); the
// canonical operator binding lives in `graphrefly-operators` and uses
// the `project_each` / `predicate_each` / etc. FFI surface that
// `TestBinding` deliberately doesn't implement, so operator-side
// OnRerun-skip coverage lands with that crate's own tests rather than
// here. NOTE: state nodes still receive `cleanup_for` for OnInvalidate
// when their cache transitions to `NO_HANDLE` — that's a separate
// dispatch path (`invalidate_inner`); only the OnRerun gate is verified
// by this test.

#[test]
fn non_fn_nodes_skip_on_rerun() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));

    let on_rerun_count = Arc::new(AtomicU64::new(0));
    rt.binding.register_cleanup(
        s.id,
        TestNodeFnCleanup {
            on_rerun: Some(counter_hook(on_rerun_count.clone())),
            ..Default::default()
        },
    );

    let _rec = rt.subscribe_recorder(s.id);
    s.set(TestValue::Int(2));
    s.set(TestValue::Int(3));

    assert_eq!(
        on_rerun_count.load(Ordering::SeqCst),
        0,
        "state nodes (no fn_id) never reach fire_regular Phase 1.5 OnRerun"
    );
    assert!(rt
        .binding
        .cleanup_calls_for(CleanupTrigger::OnRerun)
        .is_empty());
}

// =====================================================================
// Test 15 — R1.3.9.b strict per-wave dedup across re-populate (D057)
// =====================================================================

#[test]
fn r1_3_9_b_strict_dedup_across_repopulate() {
    // Within one wave: invalidate → fn re-emits self via Core::batch
    // (re-populates cache) → another invalidate hits the same node.
    // Per D057 strict reading, OnInvalidate fires AT MOST ONCE this wave
    // even though two cache-clear events occurred.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));
    let derived_node = rt.derived(&[s.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(*n)),
        _ => panic!("type"),
    });
    let _rec = rt.subscribe_recorder(derived_node);

    let on_inv_count = Arc::new(AtomicU64::new(0));
    rt.binding.register_cleanup(
        derived_node,
        TestNodeFnCleanup {
            on_invalidate: Some(counter_hook(on_inv_count.clone())),
            ..Default::default()
        },
    );

    // Compose into one wave: invalidate, then emit (re-populates cache),
    // then invalidate again. All in one batch → one wave.
    let s_id = s.id;
    let derived_node_clone = derived_node;
    let core = rt.core.clone();
    rt.core.batch(|| {
        core.invalidate(s_id); // cascades to derived_node — first OnInvalidate
        let h = rt.binding.intern(TestValue::Int(99));
        core.emit(s_id, h); // s reemits → derived_node re-fires + repopulates
        core.invalidate(s_id); // cascades again — should NOT re-fire OnInvalidate
        let _ = derived_node_clone;
    });

    assert_eq!(
        on_inv_count.load(Ordering::SeqCst),
        1,
        "OnInvalidate fires AT MOST ONCE per wave per node despite re-populate (D057)"
    );

    // Second wave: another invalidate on a populated cache should fire
    // OnInvalidate again (set is wave-scoped).
    let h = rt.binding.intern(TestValue::Int(7));
    rt.core.emit(s_id, h);
    rt.core.invalidate(s_id);
    assert_eq!(
        on_inv_count.load(Ordering::SeqCst),
        2,
        "next wave re-arms the dedup set; new OnInvalidate fires"
    );
}

// =====================================================================
// Test 16 — Q1(b) / D067: set_deps on dynamic fires OnRerun before reset
// =====================================================================

#[test]
fn d067_set_deps_fires_on_rerun_for_dynamic() {
    let rt = TestRuntime::new();
    let s1 = rt.state(Some(TestValue::Int(1)));
    let s2 = rt.state(Some(TestValue::Int(2)));
    // Dynamic node initially depending only on s1.
    let dyn_id = rt.dynamic(&[s1.id], |deps| match &deps[0] {
        TestValue::Int(n) => (Some(TestValue::Int(*n)), Some(vec![0])),
        _ => panic!("type"),
    });
    let _rec = rt.subscribe_recorder(dyn_id);
    // Sanity: dynamic fired at least once on activation.
    assert_eq!(rt.cache_value(dyn_id), Some(TestValue::Int(1)));

    let on_rerun_count = Arc::new(AtomicU64::new(0));
    rt.binding.register_cleanup(
        dyn_id,
        TestNodeFnCleanup {
            on_rerun: Some(counter_hook(on_rerun_count.clone())),
            ..Default::default()
        },
    );

    // set_deps on the dynamic node — change its deps from [s1] to [s2].
    // Per D067, OnRerun must fire because set_deps doesn't end the
    // activation cycle; the prior fn-fire's cleanup spec must run before
    // any subsequent invoke_fn overwrites it.
    rt.core.set_deps(dyn_id, &[s2.id]).unwrap();

    assert_eq!(
        on_rerun_count.load(Ordering::SeqCst),
        1,
        "OnRerun fires from set_deps on dynamic-with-prior-fire (D067)"
    );
    let calls = rt.binding.cleanup_calls_for(CleanupTrigger::OnRerun);
    assert_eq!(
        calls,
        vec![dyn_id],
        "Core fired cleanup_for(dyn, OnRerun) once during set_deps"
    );
}

// =====================================================================
// Test 17 — Coverage: batch() multi-emit fires OnRerun once per wave
// =====================================================================

#[test]
fn batch_multi_emit_fires_on_rerun_once_per_wave() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));
    let derived_node = rt.derived(&[s.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(*n)),
        _ => panic!("type"),
    });
    let _rec = rt.subscribe_recorder(derived_node);

    let on_rerun_count = Arc::new(AtomicU64::new(0));
    rt.binding.register_cleanup(
        derived_node,
        TestNodeFnCleanup {
            on_rerun: Some(counter_hook(on_rerun_count.clone())),
            ..Default::default()
        },
    );

    // batch() with 3 emits on s — the wave engine coalesces the deliveries
    // and `derived_node`'s fn fires ONCE for the wave (R1.3.6.b). OnRerun
    // must fire ONCE before that single re-fire (not 3 times).
    let s_id = s.id;
    let core = rt.core.clone();
    let binding = rt.binding.clone();
    rt.core.batch(|| {
        for v in [10, 20, 30] {
            let h = binding.intern(TestValue::Int(v));
            core.emit(s_id, h);
        }
    });

    assert_eq!(
        on_rerun_count.load(Ordering::SeqCst),
        1,
        "OnRerun fires once per wave despite 3 emits (R1.3.6.b coalescing)"
    );
}

// =====================================================================
// Test 18 — Coverage: OnDeactivation fires when last sub leaves a paused node
// =====================================================================

#[test]
fn on_deactivation_fires_on_paused_node() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));
    let derived_node = rt.derived(&[s.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(*n)),
        _ => panic!("type"),
    });
    let on_deact_count = Arc::new(AtomicU64::new(0));
    rt.binding.register_cleanup(
        derived_node,
        TestNodeFnCleanup {
            on_deactivation: Some(counter_hook(on_deact_count.clone())),
            ..Default::default()
        },
    );

    let rec = rt.subscribe_recorder(derived_node);
    // Pause the node — wire delivery buffers, but lifecycle continues.
    let lock_id = rt.core.alloc_lock_id();
    rt.core.pause(derived_node, lock_id).unwrap();
    drop(rec);

    assert_eq!(
        on_deact_count.load(Ordering::SeqCst),
        1,
        "OnDeactivation fires when last sub leaves even on paused node (pause is wire-side, not lifecycle)"
    );
}

// =====================================================================
// Test 19 — Coverage: Core::teardown does NOT fire any cleanup_for slot
// =====================================================================

#[test]
fn teardown_does_not_fire_any_cleanup_slot() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));
    let derived_node = rt.derived(&[s.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(*n)),
        _ => panic!("type"),
    });
    let _rec = rt.subscribe_recorder(derived_node);

    let r = Arc::new(AtomicU64::new(0));
    let d = Arc::new(AtomicU64::new(0));
    let i = Arc::new(AtomicU64::new(0));
    rt.binding.register_cleanup(
        derived_node,
        TestNodeFnCleanup {
            on_rerun: Some(counter_hook(r.clone())),
            on_deactivation: Some(counter_hook(d.clone())),
            on_invalidate: Some(counter_hook(i.clone())),
        },
    );

    // teardown(node) — should NOT fire any cleanup slot. Per spec R2.4.5
    // there is no `onTeardown`. The cleanup spec persists; nothing fires
    // until a separate trigger (OnDeactivation on last-sub-drop, or
    // OnInvalidate on a cache-clearing path).
    rt.core.teardown(derived_node);

    assert_eq!(
        r.load(Ordering::SeqCst),
        0,
        "teardown does not fire OnRerun"
    );
    assert_eq!(
        d.load(Ordering::SeqCst),
        0,
        "teardown does not fire OnDeactivation"
    );
    assert_eq!(
        i.load(Ordering::SeqCst),
        0,
        "teardown does not fire OnInvalidate"
    );
    // Verify Core sent NO cleanup_for at all for derived_node during the
    // teardown call.
    let calls: Vec<_> = rt
        .binding
        .cleanup_calls()
        .into_iter()
        .filter(|(n, _)| *n == derived_node)
        .collect();
    assert!(
        calls.is_empty(),
        "Core fired no cleanup_for for derived_node during teardown; got {calls:?}"
    );
}

// =====================================================================
// Test 20 — Q3(a) / D068: state node with initial value skips OnDeactivation
// =====================================================================

#[test]
fn d068_state_with_initial_value_skips_on_deactivation() {
    let rt = TestRuntime::new();
    // state(Some(v)) sets has_fired_once=true at registration time, but
    // state nodes have no fn_id — they cannot register a cleanup spec
    // via the production fn-return path. Per D068, Subscription::Drop
    // skips cleanup_for on state nodes (gate: has_fired_once && fn_id.is_some()).
    let s = rt.state(Some(TestValue::Int(7)));

    let on_deact_count = Arc::new(AtomicU64::new(0));
    // Register a cleanup spec via the test ergonomic — this bypasses the
    // spec's fn-return-only path, but verifies that Core's gate truly
    // skips state nodes regardless of binding-side state.
    rt.binding.register_cleanup(
        s.id,
        TestNodeFnCleanup {
            on_deactivation: Some(counter_hook(on_deact_count.clone())),
            ..Default::default()
        },
    );

    let rec = rt.subscribe_recorder(s.id);
    drop(rec);

    assert_eq!(
        on_deact_count.load(Ordering::SeqCst),
        0,
        "state node skips OnDeactivation per D068 (no fn_id)"
    );
    assert!(rt
        .binding
        .cleanup_calls_for(CleanupTrigger::OnDeactivation)
        .is_empty());
}

// =====================================================================
// Test 21 — D069: terminate-with-no-subs fires eager wipe via terminate_node
// =====================================================================

#[test]
fn d069_terminate_with_no_subs_fires_eager_wipe() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    rt.core.set_resubscribable(s.id, true);

    // First lifecycle: subscribe → write store → drop sub. Store
    // persists across deactivation (R2.4.6). Then complete the node
    // with no subscribers active — eager wipe path.
    {
        let _rec = rt.subscribe_recorder(s.id);
        rt.binding.store_set(s.id, "k", TestValue::Int(99));
    } // sub drops here; node still NOT terminal yet

    assert!(
        rt.binding.has_ctx(s.id),
        "store still present after deactivation (R2.4.6)"
    );

    // Now complete with subscribers.is_empty() — D069 path: terminate_node
    // queues `pending_wipes`, wave drains, `fire_deferred` fires wipe_ctx.
    rt.core.complete(s.id);

    assert!(
        !rt.binding.has_ctx(s.id),
        "D069: NodeCtxState wiped eagerly when terminate fires with no subs"
    );
    assert_eq!(
        rt.binding.wipe_calls(),
        vec![s.id],
        "Core fired wipe_ctx exactly once via terminate_node's pending_wipes drain"
    );
}

// =====================================================================
// Test 22 — D069: terminate-then-last-sub-drops fires wipe via Subscription::Drop
// =====================================================================

#[test]
fn d069_terminate_then_last_sub_drops_fires_wipe_via_subscription_drop() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    rt.core.set_resubscribable(s.id, true);

    let rec = rt.subscribe_recorder(s.id);
    rt.binding.store_set(s.id, "k", TestValue::Int(2026));

    // Complete WHILE subscribers exist. D069: terminate_node sees
    // subscribers.is_empty() == false → pending_wipes NOT pushed.
    // Subscription::Drop will fire wipe directly when the last sub drops.
    rt.core.complete(s.id);
    assert!(
        rt.binding.has_ctx(s.id),
        "store still present while sub is alive (terminate didn't queue wipe)"
    );
    assert!(
        rt.binding.wipe_calls().is_empty(),
        "no wipe fired yet — terminate_node skipped because subs were live"
    );

    // Now drop the sub. Subscription::Drop sees terminal.is_some() &&
    // resubscribable → fires wipe_ctx directly.
    drop(rec);

    assert!(
        !rt.binding.has_ctx(s.id),
        "D069: Subscription::Drop fired wipe when last sub dropped on terminal-resubscribable node"
    );
    assert_eq!(
        rt.binding.wipe_calls(),
        vec![s.id],
        "exactly one wipe fired via Subscription::Drop direct-fire path"
    );
}

// =====================================================================
// Bonus: D061 panic-discard wave drops queued OnInvalidate silently
// =====================================================================

#[test]
fn on_invalidate_dropped_silently_on_panic_discard_wave() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));
    let derived_node = rt.derived(&[s.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(*n)),
        _ => panic!("type"),
    });
    let _rec = rt.subscribe_recorder(derived_node);

    let on_inv_count = Arc::new(AtomicU64::new(0));
    rt.binding.register_cleanup(
        derived_node,
        TestNodeFnCleanup {
            on_invalidate: Some(counter_hook(on_inv_count.clone())),
            ..Default::default()
        },
    );

    // Panic mid-wave AFTER queueing an OnInvalidate. The wave is
    // panic-discarded → cleanup queue dropped silently per D061.
    let s_id = s.id;
    let core = rt.core.clone();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        core.batch(|| {
            core.invalidate(s_id); // queues OnInvalidate for derived_node
            panic!("test panic mid-wave");
        });
    }));
    assert!(result.is_err(), "panic propagated out of batch closure");

    // OnInvalidate must NOT have fired (panic-discard silently drops
    // the queue per D061).
    assert_eq!(
        on_inv_count.load(Ordering::SeqCst),
        0,
        "panic-discard drops queued OnInvalidate silently per D061"
    );

    // CoreState should be clean — next wave should still work.
    let h = rt.binding.intern(TestValue::Int(42));
    rt.core.emit(s_id, h);
    rt.core.invalidate(s_id);
    assert_eq!(
        on_inv_count.load(Ordering::SeqCst),
        1,
        "subsequent wave fires OnInvalidate normally (no leftover state)"
    );
}
