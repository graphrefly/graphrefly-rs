//! Slice X5 (D3 substrate, 2026-05-08) — verify the per-subgraph
//! union-find registry tracks correctly through `Core::register` and
//! `Core::set_deps`'s edge add path. The registry's view of
//! connected components must match the actual graph topology after
//! every topology mutation.
//!
//! These tests inspect the registry via the public `Core::partition_count`
//! / `Core::partition_of` accessors. The wave engine still uses
//! Core-level `wave_owner` in X5 (no parallelism gain yet) — Y1
//! wires the wave engine through the registry's per-partition
//! `wave_owner`.

mod common;

use std::sync::{Arc, Mutex};

use common::{TestBinding, TestRuntime, TestValue};
use graphrefly_core::{
    BindingBoundary, Core, EqualsMode, FnId, FnResult, HandleId, NodeId, SetDepsError,
};

// Slice X5 /qa P10: belt-and-suspenders verification that the new
// `SubgraphRegistry` field on `Core` and the `SubgraphLockBox` it
// wraps don't accidentally regress `Send + Sync` discipline. CLAUDE.md
// Rust invariant 2 ("Compiler-enforced thread safety. `Send + Sync`
// discipline applies to every public type.") makes this a load-bearing
// guarantee — if a future refactor stuffs an `Rc` or `Cell` into
// `SubgraphRegistry`, this static assertion fails to compile and
// surfaces the violation at the right layer.
const _: fn() = || {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}
    assert_send::<graphrefly_core::Core>();
    assert_sync::<graphrefly_core::Core>();
    assert_send::<graphrefly_core::SubgraphId>();
    assert_sync::<graphrefly_core::SubgraphId>();
};

#[test]
fn singleton_registration_creates_partition() {
    let rt = TestRuntime::new();
    let _s = rt.state(Some(TestValue::Int(0)));
    assert_eq!(rt.core.partition_count(), 1);
}

#[test]
fn derived_unions_with_dep() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let d = rt.derived(&[s.id], |_| Some(TestValue::Int(0)));
    // s and d are connected via the dep edge → share one partition.
    assert_eq!(rt.core.partition_count(), 1);
    assert_eq!(rt.core.partition_of(s.id), rt.core.partition_of(d));
}

#[test]
fn disjoint_state_nodes_have_distinct_partitions() {
    let rt = TestRuntime::new();
    let s1 = rt.state(Some(TestValue::Int(1)));
    let s2 = rt.state(Some(TestValue::Int(2)));
    assert_eq!(rt.core.partition_count(), 2);
    assert_ne!(rt.core.partition_of(s1.id), rt.core.partition_of(s2.id));
}

#[test]
fn diamond_topology_collapses_to_one_partition() {
    let rt = TestRuntime::new();
    // s1, s2 → d (combine); two state sources feed one derived.
    // After d registers with both deps, the union-find collapses both
    // partitions into one.
    let s1 = rt.state(Some(TestValue::Int(1)));
    let s2 = rt.state(Some(TestValue::Int(2)));
    let _d = rt.derived(&[s1.id, s2.id], |_| Some(TestValue::Int(0)));
    assert_eq!(rt.core.partition_count(), 1);
    assert_eq!(
        rt.core.partition_of(s1.id),
        rt.core.partition_of(s2.id),
        "s1 and s2 connected via shared derived consumer"
    );
}

#[test]
fn set_deps_add_edge_unions_partitions() {
    let rt = TestRuntime::new();
    let s1 = rt.state(Some(TestValue::Int(1)));
    let s2 = rt.state(Some(TestValue::Int(2)));
    // Register a dynamic node with only s1 as dep — d and s1 share a
    // partition; s2 is in its own partition.
    let d = rt.dynamic(&[s1.id], |_| (Some(TestValue::Int(0)), None));
    assert_eq!(rt.core.partition_count(), 2);
    assert_eq!(rt.core.partition_of(d), rt.core.partition_of(s1.id));
    assert_ne!(rt.core.partition_of(d), rt.core.partition_of(s2.id));

    // Now add s2 as a dep — set_deps should union s2's partition into d's.
    rt.core.set_deps(d, &[s1.id, s2.id]).expect("set_deps");
    assert_eq!(rt.core.partition_count(), 1);
    assert_eq!(
        rt.core.partition_of(d),
        rt.core.partition_of(s2.id),
        "s2 unioned in via set_deps add-edge"
    );
}

#[test]
fn set_deps_remove_edge_splits_partition_when_disconnects() {
    // Slice Y1 / Phase F (D3 split-eager, 2026-05-09) — INVERTED from
    // the legacy X5 monotonic-merge test. Removing the only edge
    // connecting two halves of a component now SPLITS the partition
    // (per Q1 = (c-uf split-eager) lock).
    let rt = TestRuntime::new();
    let s1 = rt.state(Some(TestValue::Int(1)));
    let s2 = rt.state(Some(TestValue::Int(2)));
    let d = rt.dynamic(&[s1.id, s2.id], |_| (Some(TestValue::Int(0)), None));
    assert_eq!(rt.core.partition_count(), 1, "{{d, s1, s2}} merged");

    // Remove s2 from d's deps — s2 is now disconnected from {d, s1}.
    // Phase F BFS detects the disconnection and splits.
    rt.core.set_deps(d, &[s1.id]).expect("set_deps");
    assert_eq!(
        rt.core.partition_count(),
        2,
        "Phase F split-eager: edge removal disconnects → split into 2 components"
    );
    assert_eq!(
        rt.core.partition_of(d),
        rt.core.partition_of(s1.id),
        "d and s1 stay together (still connected via the s1 → d edge)"
    );
    assert_ne!(
        rt.core.partition_of(d),
        rt.core.partition_of(s2.id),
        "s2 lives in its own (orphan) partition after split"
    );
}

#[test]
fn set_deps_remove_edge_keeps_partition_when_still_connected() {
    // Phase F split-eager only splits when removal DISCONNECTS the
    // component. If alternative dep paths keep the two endpoints
    // connected, no split occurs.
    let rt = TestRuntime::new();
    // Build:  s1 → d1 → d2  AND  s1 → d2  (parallel path)
    // Partition: {s1, d1, d2}.
    let s1 = rt.state(Some(TestValue::Int(1)));
    let d1 = rt.dynamic(&[s1.id], |_| (Some(TestValue::Int(0)), None));
    let d2 = rt.dynamic(&[s1.id, d1], |_| (Some(TestValue::Int(0)), None));
    assert_eq!(rt.core.partition_count(), 1);

    // Remove the s1 → d2 edge. d2 still reaches s1 transitively via
    // d1 (s1 → d1 → d2). BFS finds the alternative path; no split.
    rt.core.set_deps(d2, &[d1]).expect("set_deps");
    assert_eq!(
        rt.core.partition_count(),
        1,
        "alternative dep path keeps the component connected — no split"
    );
}

#[test]
fn set_deps_mixed_add_and_remove_in_one_call() {
    // Slice X5 /qa P5: pin the union-before-remove ordering as the
    // X5/Y1 contract. `Core::set_deps` runs `union_nodes` for ALL
    // added edges THEN walks split-detection for each removed edge.
    // Slice Y1 / Phase F (2026-05-09): the order matters under
    // split-eager — adding s3 first puts it in {d, s1, s2}'s
    // partition; removing s2 then walks BFS and finds s2
    // disconnected → splits.
    let rt = TestRuntime::new();
    let s1 = rt.state(Some(TestValue::Int(1)));
    let s2 = rt.state(Some(TestValue::Int(2)));
    let s3 = rt.state(Some(TestValue::Int(3)));
    // d initially depends on s1 + s2; partition collapses to {d, s1, s2}.
    let d = rt.dynamic(&[s1.id, s2.id], |_| (Some(TestValue::Int(0)), None));
    assert_eq!(rt.core.partition_count(), 2); // {d, s1, s2} + {s3}
    assert_ne!(rt.core.partition_of(d), rt.core.partition_of(s3.id));

    // set_deps to {s1, s3}: order is `union_nodes(d, s3)` first, then
    // split-detection on `removed = {s2}`. Post-add, the component is
    // {d, s1, s2, s3} (still merged). Then s2 removal: BFS from s2
    // finds {s2} only — disconnected from d/s1/s3. Split into
    // {d, s1, s3} (keep) + {s2} (orphan).
    rt.core.set_deps(d, &[s1.id, s3.id]).expect("set_deps");
    assert_eq!(
        rt.core.partition_count(),
        2,
        "Phase F split-eager: post-mixed-set_deps, s2 splits off into \
         its own partition while {{d, s1, s3}} stays merged via the \
         remaining dep edges"
    );
    assert_eq!(rt.core.partition_of(d), rt.core.partition_of(s1.id));
    assert_eq!(rt.core.partition_of(d), rt.core.partition_of(s3.id));
    assert_ne!(
        rt.core.partition_of(d),
        rt.core.partition_of(s2.id),
        "s2 is the orphan side of the split"
    );
}

#[test]
fn unrelated_subgraphs_stay_disjoint_through_topology_mutation() {
    let rt = TestRuntime::new();
    // Two disjoint subgraphs:
    //   A:  s1 → d1
    //   B:  s2 → d2
    let s1 = rt.state(Some(TestValue::Int(1)));
    let d1 = rt.derived(&[s1.id], |_| Some(TestValue::Int(0)));
    let s2 = rt.state(Some(TestValue::Int(2)));
    let d2 = rt.derived(&[s2.id], |_| Some(TestValue::Int(0)));
    assert_eq!(rt.core.partition_count(), 2);
    let p_a = rt.core.partition_of(d1).expect("registered");
    let p_b = rt.core.partition_of(d2).expect("registered");
    assert_ne!(p_a, p_b);
    // s1 and d1 share partition A; s2 and d2 share partition B.
    assert_eq!(rt.core.partition_of(s1.id), Some(p_a));
    assert_eq!(rt.core.partition_of(s2.id), Some(p_b));
}

// =====================================================================
// Slice Y1 (D3 / D090 — P12 fix, 2026-05-08): registry mutation
// happens INSIDE the state-lock scope. After `register` / `set_deps`
// returns, no observer can see a topology mutation in `s.nodes` /
// `s.children` without the matching registry update — closing the
// eventual-consistency window that would invalidate Y1's `lock_for`
// hot-path resolution.
//
// The window is hard to provoke deterministically in single-threaded
// tests (it'd require interrupting between state-lock-drop and
// registry-acquire on the X5 commit-1 code path, which no longer
// exists). Y1's `lock_for` will exercise the post-mutation state
// directly under the new ordering, so any regression that re-opens
// the window would surface as a `lock_for` mismatch in
// `tests/per_subgraph_parallelism.rs` (Phase H).
//
// These regression tests document the post-mutation invariants that
// must hold and pin the existing partition-of-immediately-after-
// register / partition-of-immediately-after-set_deps semantics.
// =====================================================================

#[test]
fn p12_register_partition_visible_immediately() {
    // Post-register, `partition_of` MUST return `Some(_)` for every node
    // that's just been registered. With the X5 commit-1 ordering
    // (registry mutated AFTER state-lock-drop), there was a window where
    // a thread could observe the new node in `s.nodes` but `partition_of`
    // would return `None`. The P12 fix moves registry mutation inside
    // the state-lock scope, closing the window.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));
    assert!(
        rt.core.partition_of(s.id).is_some(),
        "register MUST publish partition membership atomically with the \
         node's appearance in s.nodes — the P12-fixed lock-discipline \
         invariant `state lock → registry mutex` underwrites Y1's \
         lock_for resolution"
    );
}

#[test]
fn p12_set_deps_partition_visible_immediately() {
    let rt = TestRuntime::new();
    let s1 = rt.state(Some(TestValue::Int(1)));
    let s2 = rt.state(Some(TestValue::Int(2)));
    let d = rt.dynamic(&[s1.id], |_| (Some(TestValue::Int(0)), None));
    let p_before = rt.core.partition_of(d).expect("registered");
    let p_s2_before = rt.core.partition_of(s2.id).expect("registered");
    assert_ne!(p_before, p_s2_before, "pre-set_deps disjoint");

    // After set_deps adds s2, the union MUST be visible immediately —
    // not eventually-consistent with respect to the state mutation.
    rt.core.set_deps(d, &[s1.id, s2.id]).expect("set_deps");
    assert_eq!(
        rt.core.partition_of(d),
        rt.core.partition_of(s2.id),
        "set_deps add-edge MUST publish partition union atomically with \
         the children-map mutation in s.nodes — P12 fix invariant"
    );
}

// =====================================================================
// Slice Y1 (D3 / D091 — P13, 2026-05-08): mid-wave set_deps that
// triggers partition migration is rejected with
// `SetDepsError::PartitionMigrationDuringFire`. Distinct from
// `ReentrantOnFiringNode` (same-node reentrance, A6 in slice_f_corrections.rs).
//
// Y1's wave engine holds Arc<SubgraphLockBox> for the firing node's
// partition. A union mid-wave changes the partition's box-identity;
// a split (Phase F) extracts a fresh box for the orphan side. Either
// way the held Arc would diverge from the registry's current root.
// Q3 = (a-strict) per the D3 design lock rejects this at edge-mutation
// time so the partition shape is stable for the wave's lifetime.
// =====================================================================

/// Helper binding that re-enters `Core::set_deps(target, new_deps)` from
/// inside `firing_node`'s fn-fire. Adapted from the A6 D1Binding pattern
/// in `slice_f_corrections.rs`, generalized so target can differ from
/// the firing node.
struct CrossPartitionRewireBinding {
    inner: Arc<TestBinding>,
    firing_node: Mutex<Option<NodeId>>,
    target: Mutex<Option<NodeId>>,
    new_deps: Mutex<Vec<NodeId>>,
    core: Mutex<Option<Core>>,
    captured_result: Mutex<Option<Result<(), SetDepsError>>>,
}

impl BindingBoundary for CrossPartitionRewireBinding {
    fn invoke_fn(
        &self,
        node_id: NodeId,
        fn_id: FnId,
        dep_data: &[graphrefly_core::DepBatch],
    ) -> FnResult {
        // When the firing node is the configured trigger, attempt
        // set_deps(target, new_deps).
        if Some(node_id) == *self.firing_node.lock().unwrap() {
            let target = *self.target.lock().unwrap();
            let new_deps = self.new_deps.lock().unwrap().clone();
            let core = self.core.lock().unwrap().clone();
            if let (Some(t), Some(c)) = (target, core) {
                let result = c.set_deps(t, &new_deps);
                *self.captured_result.lock().unwrap() = Some(result);
            }
        }
        self.inner.invoke_fn(node_id, fn_id, dep_data)
    }

    fn release_handle(&self, handle: HandleId) {
        self.inner.release_handle(handle);
    }

    fn retain_handle(&self, handle: HandleId) {
        self.inner.retain_handle(handle);
    }

    fn custom_equals(&self, fn_id: FnId, a: HandleId, b: HandleId) -> bool {
        self.inner.custom_equals(fn_id, a, b)
    }
}

#[test]
fn p13_cross_partition_set_deps_during_fire_rejected() {
    // Build two disjoint partitions:
    //   Partition A: s1 → n  (fire-trigger: n's fn re-enters Core::set_deps)
    //   Partition B: s2,    m  (state-only target; m is a dynamic node)
    //
    // From inside n's fn-fire, attempt set_deps(m, [s1]) — that union
    // would merge partitions A and B. Both partitions are "affected"
    // (the union changes the box-identity for one of them); n is in A,
    // so the migration shifts n's wave_owner mid-wave. Q3=(a-strict)
    // rejects with PartitionMigrationDuringFire.
    let inner = TestBinding::new();
    let bind = Arc::new(CrossPartitionRewireBinding {
        inner: inner.clone(),
        firing_node: Mutex::new(None),
        target: Mutex::new(None),
        new_deps: Mutex::new(Vec::new()),
        core: Mutex::new(None),
        captured_result: Mutex::new(None),
    });
    let core = Core::new(bind.clone() as Arc<dyn BindingBoundary>);
    *bind.core.lock().unwrap() = Some(core.clone());

    let s1_init = inner.intern(TestValue::Int(10));
    let s2_init = inner.intern(TestValue::Int(20));
    let s1 = core.register_state(s1_init, false).unwrap();
    let s2 = core.register_state(s2_init, false).unwrap();
    let fn_id = inner.register_fn(|deps: &[TestValue]| match deps[0] {
        TestValue::Int(v) => Some(TestValue::Int(v * 2)),
        _ => None,
    });
    let n = core
        .register_derived(&[s1], fn_id, EqualsMode::Identity, false)
        .unwrap();
    // m is dynamic so its deps can be mutated; starts in s2's partition.
    let m_fn = inner.register_fn(|deps: &[TestValue]| match deps[0] {
        TestValue::Int(v) => Some(TestValue::Int(v + 1)),
        _ => None,
    });
    let m = core
        .register_dynamic(&[s2], m_fn, EqualsMode::Identity, false)
        .unwrap();

    // Sanity: A and B are disjoint pre-fire.
    let p_a = core.partition_of(n).expect("registered");
    let p_b = core.partition_of(m).expect("registered");
    assert_ne!(p_a, p_b, "n and m start in disjoint partitions");

    // Configure binding: when n fires, attempt set_deps(m, [s1, s2]) —
    // union(m, s1) merges A and B; n is currently firing in A.
    *bind.firing_node.lock().unwrap() = Some(n);
    *bind.target.lock().unwrap() = Some(m);
    *bind.new_deps.lock().unwrap() = vec![s2, s1];

    // Subscribe to drive n's first fn-fire (handshake → activation → fire).
    let _sub = core.subscribe(n, Arc::new(|_msgs: &[graphrefly_core::Message]| {}));

    // The captured set_deps result MUST be PartitionMigrationDuringFire.
    let result = bind
        .captured_result
        .lock()
        .unwrap()
        .take()
        .expect("set_deps was attempted from inside invoke_fn");
    assert!(
        matches!(
            result,
            Err(SetDepsError::PartitionMigrationDuringFire { n: rejected_n, firing })
                if rejected_n == m && firing == n
        ),
        "expected PartitionMigrationDuringFire {{ n: {m:?}, firing: {n:?} }}; got {result:?}"
    );

    // Topology unchanged: m still has only s2 as dep, partitions still disjoint.
    assert_eq!(
        core.deps_of(m),
        vec![s2],
        "m's deps survived the rejected rewire"
    );
    assert_ne!(
        core.partition_of(n),
        core.partition_of(m),
        "partitions remained disjoint — no spurious union"
    );
}

#[test]
fn p13_same_partition_set_deps_during_fire_allowed() {
    // Counter-test: set_deps that does NOT trigger partition migration
    // MUST be allowed even from inside another node's fire. P13 only
    // gates union-cross-partition AND split-disconnects cases.
    //
    // Build a topology where removing one edge does NOT disconnect the
    // component (alternative dep path keeps connectivity):
    //   s1 → n_fire (firing target)
    //   s1 → bridge
    //   s2 → bridge        (so s2 is in s1's component via bridge)
    //   s1 → m
    //   s2 → m             (the edge to remove)
    // Component pre-removal: {s1, s2, bridge, n_fire, m}.
    // set_deps(m, [s1]) removes the s2 → m edge. Post-removal, s2
    // remains connected to s1 via the s2 → bridge ← s1 path. No split.
    let inner = TestBinding::new();
    let bind = Arc::new(CrossPartitionRewireBinding {
        inner: inner.clone(),
        firing_node: Mutex::new(None),
        target: Mutex::new(None),
        new_deps: Mutex::new(Vec::new()),
        core: Mutex::new(None),
        captured_result: Mutex::new(None),
    });
    let core = Core::new(bind.clone() as Arc<dyn BindingBoundary>);
    *bind.core.lock().unwrap() = Some(core.clone());

    let s1_init = inner.intern(TestValue::Int(1));
    let s2_init = inner.intern(TestValue::Int(2));
    let s1 = core.register_state(s1_init, false).unwrap();
    let s2 = core.register_state(s2_init, false).unwrap();
    let fn_id = inner.register_fn(|deps: &[TestValue]| match deps[0] {
        TestValue::Int(v) => Some(TestValue::Int(v * 2)),
        _ => None,
    });
    let n_fire = core
        .register_derived(&[s1], fn_id, EqualsMode::Identity, false)
        .unwrap();
    let bridge_fn = inner.register_fn(|_: &[TestValue]| Some(TestValue::Int(0)));
    // bridge depends on both s1 and s2 — keeps them connected even if
    // m's dep on s2 is removed.
    let _bridge = core
        .register_derived(&[s1, s2], bridge_fn, EqualsMode::Identity, false)
        .unwrap();
    let m_fn = inner.register_fn(|_: &[TestValue]| Some(TestValue::Int(0)));
    let m = core
        .register_dynamic(&[s1, s2], m_fn, EqualsMode::Identity, false)
        .unwrap();

    // Sanity: all nodes share one component (via dep edges → bridge).
    assert_eq!(
        core.partition_count(),
        1,
        "s1, s2, bridge, n_fire, m all unioned in one partition"
    );

    *bind.firing_node.lock().unwrap() = Some(n_fire);
    *bind.target.lock().unwrap() = Some(m);
    // Remove s2 from m's deps; new_deps = [s1]. Post-removal, s2 still
    // reaches m via the s2 → bridge ← s1 → m path, so no split.
    *bind.new_deps.lock().unwrap() = vec![s1];

    let _sub = core.subscribe(n_fire, Arc::new(|_msgs: &[graphrefly_core::Message]| {}));

    let result = bind
        .captured_result
        .lock()
        .unwrap()
        .take()
        .expect("set_deps was attempted from inside invoke_fn");
    assert!(
        result.is_ok(),
        "no-migration set_deps from inside another node's fire MUST pass \
         — alternative dep path via `bridge` keeps the component \
         connected, so removal triggers no split; got {result:?}"
    );
    // Component still merged.
    assert_eq!(
        core.partition_count(),
        1,
        "no split — alternative dep path preserves connectivity"
    );
}

#[test]
fn p13_split_during_fire_rejected() {
    // Slice Y1 / Phase F (D3 split-eager, 2026-05-09): removing an
    // edge that DISCONNECTS the partition triggers a split, which
    // migrates the partition root + box-identity for affected nodes.
    // P13 widening rejects this when a firing node lives in the
    // affected partition.
    //
    // Build:  s1 → n_fire (firing) AND s1 → m (target) AND s2 → m.
    // Component: {s1, s2, n_fire, m}. There's no bridge node, so
    // removing s2's edge to m disconnects s2 from the rest.
    let inner = TestBinding::new();
    let bind = Arc::new(CrossPartitionRewireBinding {
        inner: inner.clone(),
        firing_node: Mutex::new(None),
        target: Mutex::new(None),
        new_deps: Mutex::new(Vec::new()),
        core: Mutex::new(None),
        captured_result: Mutex::new(None),
    });
    let core = Core::new(bind.clone() as Arc<dyn BindingBoundary>);
    *bind.core.lock().unwrap() = Some(core.clone());

    let s1_init = inner.intern(TestValue::Int(1));
    let s2_init = inner.intern(TestValue::Int(2));
    let s1 = core.register_state(s1_init, false).unwrap();
    let s2 = core.register_state(s2_init, false).unwrap();
    let fn_id = inner.register_fn(|deps: &[TestValue]| match deps[0] {
        TestValue::Int(v) => Some(TestValue::Int(v * 2)),
        _ => None,
    });
    let n_fire = core
        .register_derived(&[s1], fn_id, EqualsMode::Identity, false)
        .unwrap();
    let m_fn = inner.register_fn(|_: &[TestValue]| Some(TestValue::Int(0)));
    let m = core
        .register_dynamic(&[s1, s2], m_fn, EqualsMode::Identity, false)
        .unwrap();
    assert_eq!(
        core.partition_count(),
        1,
        "single component pre-set_deps: s2's only edge is to m"
    );

    *bind.firing_node.lock().unwrap() = Some(n_fire);
    *bind.target.lock().unwrap() = Some(m);
    *bind.new_deps.lock().unwrap() = vec![s1]; // remove s2 — disconnects s2

    let _sub = core.subscribe(n_fire, Arc::new(|_msgs: &[graphrefly_core::Message]| {}));

    let result = bind
        .captured_result
        .lock()
        .unwrap()
        .take()
        .expect("set_deps was attempted from inside invoke_fn");
    assert!(
        matches!(
            result,
            Err(SetDepsError::PartitionMigrationDuringFire { n: rejected_n, firing })
                if rejected_n == m && firing == n_fire
        ),
        "expected PartitionMigrationDuringFire {{ n: {m:?}, firing: {n_fire:?} }}; got {result:?}"
    );
    // Topology unchanged: m still has both deps; component still merged.
    assert_eq!(
        core.deps_of(m),
        vec![s1, s2],
        "m's deps survived the rejected rewire"
    );
    assert_eq!(core.partition_count(), 1, "rejected — no split occurred");
}
