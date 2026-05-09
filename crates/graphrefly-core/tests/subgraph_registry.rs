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

use common::{TestRuntime, TestValue};

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
fn set_deps_remove_edge_keeps_partition_x5_monotonic() {
    // X5 is monotonic-merge — `on_edge_removed` is a no-op. The
    // partition stays merged even when the only connecting edge is
    // removed. Y1 (split-eager) will change this to split when
    // disconnected.
    let rt = TestRuntime::new();
    let s1 = rt.state(Some(TestValue::Int(1)));
    let s2 = rt.state(Some(TestValue::Int(2)));
    let d = rt.dynamic(&[s1.id, s2.id], |_| (Some(TestValue::Int(0)), None));
    assert_eq!(rt.core.partition_count(), 1);

    // Remove s2 from d's deps — the only edge connecting s2 to the
    // rest. Under split-eager (Y1) this would split into 2 components.
    // Under X5 monotonic-merge, partitions stay merged.
    rt.core.set_deps(d, &[s1.id]).expect("set_deps");
    assert_eq!(
        rt.core.partition_count(),
        1,
        "X5 monotonic-merge: edge removal does not split"
    );
}

#[test]
fn set_deps_mixed_add_and_remove_in_one_call() {
    // Slice X5 /qa P5: pin the union-before-remove ordering as the
    // X5/Y1 contract. `Core::set_deps` runs `union_nodes` for ALL
    // added edges THEN `on_edge_removed` for ALL removed edges. In
    // X5 monotonic-merge `on_edge_removed` is a no-op, so the order
    // doesn't matter; under Y1 split-eager the order is semantically
    // meaningful (removing-then-unioning vs unioning-then-removing
    // produces different intermediate component states). Pinning
    // the ordering now prevents a Y1 regression from silently
    // flipping it.
    let rt = TestRuntime::new();
    let s1 = rt.state(Some(TestValue::Int(1)));
    let s2 = rt.state(Some(TestValue::Int(2)));
    let s3 = rt.state(Some(TestValue::Int(3)));
    // d initially depends on s1 + s2; partition collapses to {d, s1, s2}.
    let d = rt.dynamic(&[s1.id, s2.id], |_| (Some(TestValue::Int(0)), None));
    assert_eq!(rt.core.partition_count(), 2); // {d, s1, s2} + {s3}
    assert_ne!(rt.core.partition_of(d), rt.core.partition_of(s3.id));

    // set_deps to {s1, s3}: removes s2 (no-op in X5 — partition stays
    // merged) AND adds s3 (union — s3's partition merges in).
    rt.core.set_deps(d, &[s1.id, s3.id]).expect("set_deps");
    assert_eq!(
        rt.core.partition_count(),
        1,
        "all four nodes share a single partition after mixed set_deps \
         (s2 stays via X5 monotonic-merge; s3 unioned in via add-edge)"
    );
    assert_eq!(rt.core.partition_of(d), rt.core.partition_of(s1.id));
    assert_eq!(rt.core.partition_of(d), rt.core.partition_of(s2.id));
    assert_eq!(rt.core.partition_of(d), rt.core.partition_of(s3.id));
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
