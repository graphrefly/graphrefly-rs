//! Slice B-2 Step 2b-ii (D220-EXEC) — **cross-shard routing delivery**
//! regression tests.
//!
//! `serialization_groups.rs` asserts the `partition_of` *contract* but
//! not that a grouped node's waves actually DELIVER through a
//! non-`DEFAULT_SHARD` shard (the exact coverage gap 2b-ii introduces:
//! a grouped node's `CoreState` record lives in shard `g`, NOT
//! `DEFAULT_SHARD`, so register-with-group placement, the
//! `set_serialization_group` cross-shard component migration, the
//! `begin_batch_for`→`group_of(seed)` ambient wave routing, and the
//! `node_group` index must all be coherent or emits silently no-op on
//! the wrong shard). These tests drive real emits + recorders across
//! the shard boundary so the gate verifies grouped correctness, not
//! just the metadata.

mod common;

use common::{TestRuntime, TestValue};
use graphrefly_core::{NodeOpts, NodeRegistration, SerializationGroupId};

const G1: SerializationGroupId = SerializationGroupId::new(1);
const G2: SerializationGroupId = SerializationGroupId::new(2);

/// /qa C: exact-count, not membership. The cross-shard migration's
/// risk is double / extra delivery (a record re-running activation on
/// both `src` and `dst` shard, or replay after a regroup) — a
/// `.contains()` assertion would green-light that regression. Assert
/// each expected value is delivered EXACTLY once.
fn count(rec: &common::Recorder, v: i64) -> usize {
    rec.data_values()
        .into_iter()
        .filter(|x| *x == TestValue::Int(v))
        .count()
}

/// `set_serialization_group` migrates a single-node component
/// `DEFAULT_SHARD → g`; subsequent emits route to shard `g` (ambient
/// set from `group_of(seed)` by `begin_batch_for`) and still deliver.
#[test]
fn grouped_state_round_trip_delivers() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    rt.core()
        .set_serialization_group(s.id, Some(G1))
        .expect("migrate to G1 shard");
    assert_eq!(rt.core().partition_of(s.id), Some(G1));
    let rec = rt.subscribe_recorder(s.id);
    s.set(TestValue::Int(7));
    assert_eq!(
        count(&rec, 7),
        1,
        "grouped (shard-G1) state emit must deliver EXACTLY once (no \
         migration double-delivery) — got {:?}",
        rec.data_values()
    );
}

/// A multi-node component (`s → d`) migrates wholesale to shard `g`
/// (children-adjacency moves too); the derived recomputes + delivers
/// from the non-default shard.
#[test]
fn grouped_derived_component_migrates_and_delivers() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));
    let d = rt.derived(&[s.id], |vals| match &vals[0] {
        TestValue::Int(n) => Some(TestValue::Int(n * 10)),
        _ => None,
    });
    rt.core()
        .set_serialization_group(s.id, Some(G1))
        .expect("migrate component {s,d} to G1");
    // Whole dep-connected component moved (homogeneity).
    assert_eq!(rt.core().partition_of(s.id), Some(G1));
    assert_eq!(rt.core().partition_of(d), Some(G1));
    let rec = rt.subscribe_recorder(d);
    s.set(TestValue::Int(4));
    assert_eq!(
        count(&rec, 40),
        1,
        "derived in migrated shard-G1 component must recompute+deliver \
         EXACTLY once — got {:?}",
        rec.data_values()
    );
}

/// Register directly with a group (the `group_parallelism.rs` path):
/// the record is placed in shard `g` + indexed at register time;
/// emits deliver.
#[test]
fn register_with_group_places_and_delivers() {
    let rt = TestRuntime::new();
    let init = rt.binding.intern(TestValue::Int(0));
    let s = rt
        .core()
        .register(NodeRegistration {
            deps: vec![],
            fn_or_op: None,
            opts: NodeOpts {
                initial: init,
                serialization_group: Some(G1),
                ..Default::default()
            },
        })
        .expect("register grouped state");
    assert_eq!(rt.core().partition_of(s), Some(G1));
    let rec = rt.subscribe_recorder(s);
    let h = rt.binding.intern(TestValue::Int(99));
    rt.core().emit(s, h);
    assert_eq!(
        count(&rec, 99),
        1,
        "register-with-group node must deliver from its shard EXACTLY \
         once — got {:?}",
        rec.data_values()
    );
}

/// Disjoint groups occupy disjoint shards; each delivers only its own
/// value (no cross-shard leakage / mis-route).
#[test]
fn disjoint_groups_deliver_independently() {
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(0)));
    let b = rt.state(Some(TestValue::Int(0)));
    rt.core().set_serialization_group(a.id, Some(G1)).unwrap();
    rt.core().set_serialization_group(b.id, Some(G2)).unwrap();
    assert_eq!(rt.core().partition_of(a.id), Some(G1));
    assert_eq!(rt.core().partition_of(b.id), Some(G2));
    let ra = rt.subscribe_recorder(a.id);
    let rb = rt.subscribe_recorder(b.id);
    a.set(TestValue::Int(11));
    b.set(TestValue::Int(22));
    assert_eq!(count(&ra, 11), 1, "G1 delivers its own value exactly once");
    assert_eq!(count(&rb, 22), 1, "G2 delivers its own value exactly once");
    assert_eq!(
        count(&ra, 22),
        0,
        "G1 recorder must NOT see G2's value (no cross-shard mis-route/leak)"
    );
}

/// Repeated regrouping (`None → G1 → None → G2`) round-trips the
/// cross-shard migration + the `node_group` index (None ⇒ absent ⇒
/// `DEFAULT_SHARD`); delivery holds at every step.
#[test]
fn repeated_regroup_round_trips_delivery_and_index() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let rec = rt.subscribe_recorder(s.id);

    s.set(TestValue::Int(1));
    assert_eq!(rt.core().partition_of(s.id), None);

    rt.core().set_serialization_group(s.id, Some(G1)).unwrap();
    assert_eq!(rt.core().partition_of(s.id), Some(G1));
    s.set(TestValue::Int(2));

    rt.core().set_serialization_group(s.id, None).unwrap();
    assert_eq!(rt.core().partition_of(s.id), None);
    s.set(TestValue::Int(3));

    rt.core().set_serialization_group(s.id, Some(G2)).unwrap();
    assert_eq!(rt.core().partition_of(s.id), Some(G2));
    s.set(TestValue::Int(4));

    let got = rec.data_values();
    for n in [1, 2, 3, 4] {
        assert_eq!(
            count(&rec, n),
            1,
            "value {n} (across None/G1/None/G2 migrations) must deliver \
             EXACTLY once — a regroup must NOT replay prior values \
             (cross-shard double-delivery) — got {got:?}"
        );
    }
}

/// Behaviour-identical floor regression: an all-`None` graph still
/// delivers through the new ambient/index code path (index empty,
/// ambient `None` ⇒ `DEFAULT_SHARD`).
#[test]
fn ungrouped_floor_still_delivers() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let d = rt.derived(&[s.id], |vals| match &vals[0] {
        TestValue::Int(n) => Some(TestValue::Int(n + 1)),
        _ => None,
    });
    assert_eq!(rt.core().partition_of(s.id), None);
    assert_eq!(rt.core().partition_of(d), None);
    let rec = rt.subscribe_recorder(d);
    s.set(TestValue::Int(41));
    assert_eq!(
        count(&rec, 42),
        1,
        "all-None floor must still deliver EXACTLY once — got {:?}",
        rec.data_values()
    );
}
