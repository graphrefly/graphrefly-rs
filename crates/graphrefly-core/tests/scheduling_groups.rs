//! §7 single-threaded substrate + `SchedulingGroupId` contract
//! (D208–D211, 2026-05-16). Replaces the deleted `subgraph_registry.rs`
//! (which tested the now-removed D3 union-find connected-component
//! semantics).
//!
//! Covers: default-`None` floor (`partition_of` == `None`,
//! `partition_count` == 0); group assignment via `NodeOpts` +
//! `Core::set_scheduling_group`; and the **strict
//! dep-component-consistency invariant** (a component must be uniformly
//! all-`None` or all-`Some`) at every topology-mutation entry point
//! (`register` / `set_deps` / `set_scheduling_group`).

mod common;

use std::sync::Arc;

use common::{TestRuntime, TestValue};
use graphrefly_core::{
    BindingBoundary, Core, EqualsMode, NodeOpts, NodeRegistration, RegisterError,
    SchedulingGroupId, SetDepsError, SetGroupError,
};

// D248/D249/S2c: `Core` is now **`!Send + !Sync`** (full single-owner).
// The substrate `Sink`/`TopologySink` dropped `Send + Sync`
// (shared-Core-era legacy); `Core` owns the subscriber map + the
// owner-only `Rc<DeferQueue>`, so it is owned, driven, and dropped on
// exactly one thread (the only cross-thread bridge is the id-only
// `Arc<CoreMailbox>`). This **reverses** the prior "CLAUDE.md Rust
// invariant 2: Core stays Send+Sync" + the S2b `Core: Send` premise —
// a documented consequence of D248 (user-locked). The
// `Core<SingleThreadCell>` / `Core<LockedCell>` cell-generic split is
// deleted (D246/S2c). `SchedulingGroupId` is a plain newtype and
// stays `Send + Sync` (S3 renames it to `SchedulingGroupId`).
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SchedulingGroupId>();

    // `Core` must be `!Send` (it holds `!Send` sinks + `Rc`): the
    // canonical no-dep ambiguity trick — if `Core` impl'd `Send`,
    // method resolution is ambiguous → compile error; `!Send` leaves
    // only the blanket `()` impl → unambiguous → compiles.
    trait AmbiguousIfSend<A> {
        fn probe() {}
    }
    impl<T: ?Sized> AmbiguousIfSend<()> for T {}
    impl<T: ?Sized + Send> AmbiguousIfSend<u16> for T {}
    let _ = <graphrefly_core::Core as AmbiguousIfSend<_>>::probe;
};

const G1: SchedulingGroupId = SchedulingGroupId::new(1);
const G2: SchedulingGroupId = SchedulingGroupId::new(2);

// ---------------------------------------------------------------------
// Default-None floor
// ---------------------------------------------------------------------

#[test]
fn default_none_graph_has_no_groups() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let d = rt.derived(&[s.id], |_| Some(TestValue::Int(1)));
    // No node carries a group → the §7 single-threaded lock-free floor.
    assert_eq!(rt.core().partition_of(s.id), None);
    assert_eq!(rt.core().partition_of(d), None);
    assert_eq!(
        rt.core().partition_count(),
        0,
        "all-None graph touches no group lock — the floor"
    );
}

#[test]
fn partition_of_unregistered_is_none() {
    let rt = TestRuntime::new();
    assert_eq!(
        rt.core().partition_of(graphrefly_core::NodeId::new(9999)),
        None
    );
}

// ---------------------------------------------------------------------
// Group assignment
// ---------------------------------------------------------------------

#[test]
fn register_with_group_then_partition_of_reports_it() {
    let rt = TestRuntime::new();
    let init = rt.binding.intern(TestValue::Int(0));
    let s = rt
        .core()
        .register(NodeRegistration {
            deps: vec![],
            fn_or_op: None,
            opts: NodeOpts {
                initial: init,
                scheduling_group: Some(G1),
                ..Default::default()
            },
        })
        .expect("register grouped state");
    assert_eq!(rt.core().partition_of(s), Some(G1));
}

#[test]
fn set_scheduling_group_on_isolated_node_ok() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    // Single-node component — setting a group keeps it uniformly all-Some.
    rt.core()
        .set_scheduling_group(s.id, Some(G1))
        .expect("isolated node group assignment");
    assert_eq!(rt.core().partition_of(s.id), Some(G1));
    // Clearing back to None is also consistent (single-node, all-None).
    rt.core()
        .set_scheduling_group(s.id, None)
        .expect("clear group");
    assert_eq!(rt.core().partition_of(s.id), None);
}

#[test]
fn set_scheduling_group_unknown_node() {
    let rt = TestRuntime::new();
    let err = rt
        .core()
        .set_scheduling_group(graphrefly_core::NodeId::new(42), Some(G1))
        .unwrap_err();
    assert!(matches!(err, SetGroupError::UnknownNode(_)));
}

// ---------------------------------------------------------------------
// Strict dep-component consistency — register
// ---------------------------------------------------------------------

#[test]
fn register_all_none_chain_ok() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    // None derived on None state — uniformly all-None.
    let _d = rt.derived(&[s.id], |_| Some(TestValue::Int(1)));
}

#[test]
fn register_all_some_chain_ok_even_with_distinct_groups() {
    let rt = TestRuntime::new();
    let init = rt.binding.intern(TestValue::Int(0));
    let s = rt
        .core()
        .register(NodeRegistration {
            deps: vec![],
            fn_or_op: None,
            opts: NodeOpts {
                initial: init,
                scheduling_group: Some(G1),
                ..Default::default()
            },
        })
        .unwrap();
    let f = rt
        .binding
        .register_fn(|_: &[TestValue]| Some(TestValue::Int(0)));
    // Distinct group on a dep-connected node is allowed — the invariant
    // is "all-None OR all-Some", not "all-same-group".
    let d = rt
        .core()
        .register(NodeRegistration {
            deps: vec![s],
            fn_or_op: Some(graphrefly_core::NodeFnOrOp::Fn(f)),
            opts: NodeOpts {
                scheduling_group: Some(G2),
                ..Default::default()
            },
        })
        .expect("all-Some component with distinct groups is consistent");
    assert_eq!(rt.core().partition_of(s), Some(G1));
    assert_eq!(rt.core().partition_of(d), Some(G2));
}

#[test]
fn register_mixed_component_rejected() {
    let rt = TestRuntime::new();
    let init = rt.binding.intern(TestValue::Int(0));
    // Grouped state ...
    let s = rt
        .core()
        .register(NodeRegistration {
            deps: vec![],
            fn_or_op: None,
            opts: NodeOpts {
                initial: init,
                scheduling_group: Some(G1),
                ..Default::default()
            },
        })
        .unwrap();
    let f = rt
        .binding
        .register_fn(|_: &[TestValue]| Some(TestValue::Int(0)));
    // ... ungrouped derived depending on it → mixed component → reject.
    let err = rt
        .core()
        .register(NodeRegistration {
            deps: vec![s],
            fn_or_op: Some(graphrefly_core::NodeFnOrOp::Fn(f)),
            opts: NodeOpts::default(), // scheduling_group: None
        })
        .unwrap_err();
    assert!(
        matches!(err, RegisterError::GroupInconsistent),
        "mixing a grouped dep with an ungrouped consumer must be rejected; got {err:?}"
    );
}

// ---------------------------------------------------------------------
// Strict dep-component consistency — set_deps
// ---------------------------------------------------------------------

#[test]
fn set_deps_into_mixed_component_rejected() {
    let rt = TestRuntime::new();
    // Ungrouped dynamic on ungrouped s1.
    let s1 = rt.state(Some(TestValue::Int(1)));
    let d = rt.dynamic(&[s1.id], |_| (Some(TestValue::Int(0)), None));
    // Grouped s2 (isolated, all-Some single-node — fine).
    let init2 = rt.binding.intern(TestValue::Int(2));
    let s2 = rt
        .core()
        .register(NodeRegistration {
            deps: vec![],
            fn_or_op: None,
            opts: NodeOpts {
                initial: init2,
                scheduling_group: Some(G1),
                ..Default::default()
            },
        })
        .unwrap();
    // Rewiring d to also depend on grouped s2 mixes None (d, s1) with
    // Some (s2) in one component → reject; topology unchanged.
    let err = rt.core().set_deps(d, &[s1.id, s2]).unwrap_err();
    assert!(
        matches!(err, SetDepsError::GroupInconsistent { n } if n == d),
        "set_deps creating a mixed component must be rejected; got {err:?}"
    );
    assert_eq!(
        rt.core().deps_of(d),
        vec![s1.id],
        "rejected — deps unchanged"
    );
}

#[test]
fn set_deps_consistent_rewire_ok() {
    let rt = TestRuntime::new();
    let s1 = rt.state(Some(TestValue::Int(1)));
    let s2 = rt.state(Some(TestValue::Int(2)));
    let d = rt.dynamic(&[s1.id], |_| (Some(TestValue::Int(0)), None));
    // All-None rewire — consistent.
    rt.core()
        .set_deps(d, &[s1.id, s2.id])
        .expect("all-None rewire");
    assert_eq!(rt.core().deps_of(d), vec![s1.id, s2.id]);
}

// ---------------------------------------------------------------------
// QA point-3 (2026-05-16): set_scheduling_group is COMPONENT-WIDE —
// the retroactive-regrouping primitive. Assigning to any member assigns
// the whole dep+children+meta component atomically (consistent by
// construction; never the chicken-and-egg per-node rejection).
// ---------------------------------------------------------------------

#[test]
fn set_scheduling_group_is_component_wide_retroactive_regroup() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let d = rt.derived(&[s.id], |_| Some(TestValue::Int(1)));
    // {s, d} is a 2-node all-None component. Grouping `s` now groups the
    // ENTIRE component (s AND d) → uniformly G1, invariant holds.
    rt.core()
        .set_scheduling_group(s.id, Some(G1))
        .expect("component-wide assign succeeds (consistent by construction)");
    assert_eq!(rt.core().partition_of(s.id), Some(G1));
    assert_eq!(
        rt.core().partition_of(d),
        Some(G1),
        "the connected derived node was regrouped too (component-wide)"
    );
    // Retroactive clear back to the all-None floor, also component-wide.
    rt.core()
        .set_scheduling_group(d, None)
        .expect("component-wide clear succeeds");
    assert_eq!(rt.core().partition_of(s.id), None);
    assert_eq!(rt.core().partition_of(d), None);
}

// ---------------------------------------------------------------------
// QA F2 (2026-05-16): meta-companion edges are component-joining edges,
// guarded by the SAME unified deps+children+meta consistency check
// (closing the prior hole where add_meta_companion did no group check
// and the walk ignored meta).
// ---------------------------------------------------------------------

fn grouped_state(rt: &TestRuntime, g: Option<SchedulingGroupId>) -> graphrefly_core::NodeId {
    let init = rt.binding.intern(TestValue::Int(0));
    rt.core()
        .register(NodeRegistration {
            deps: vec![],
            fn_or_op: None,
            opts: NodeOpts {
                initial: init,
                scheduling_group: g,
                ..Default::default()
            },
        })
        .expect("register isolated state")
}

#[test]
fn add_meta_companion_mixed_component_panics() {
    let rt = TestRuntime::new();
    let parent = grouped_state(&rt, Some(G1)); // grouped
    let companion = grouped_state(&rt, None); // ungrouped
                                              // Meta edge would join a Some component with a None one → §7 strict
                                              // invariant violation → panic (this API's assert-based contract).
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rt.core().add_meta_companion(parent, companion);
    }));
    assert!(
        res.is_err(),
        "add_meta_companion joining grouped+ungrouped must panic (mixed component)"
    );
    // Rolled back: the meta edge must NOT have been retained.
    assert!(
        rt.core().meta_companions_of(parent).is_empty(),
        "meta edge rolled back on the panic path"
    );
}

#[test]
fn add_meta_companion_consistent_and_walk_includes_meta() {
    let rt = TestRuntime::new();
    let parent = grouped_state(&rt, Some(G1));
    let companion = grouped_state(&rt, Some(G1));
    // Both Some(G1): consistent → ok.
    rt.core().add_meta_companion(parent, companion);
    // Proof the unified walk includes meta: a component-wide clear seeded
    // at `parent` must reach `companion` ONLY via the meta edge (there is
    // no dep/child edge between them).
    rt.core()
        .set_scheduling_group(parent, None)
        .expect("component-wide clear");
    assert_eq!(rt.core().partition_of(parent), None);
    assert_eq!(
        rt.core().partition_of(companion),
        None,
        "meta companion regrouped via the unified deps+children+meta walk"
    );
}

// ---------------------------------------------------------------------
// Groups are static: set_deps during fire does NOT migrate a group
// (the deleted union-find `PartitionMigrationDuringFire` is vestigial /
// never returned post-§7).
// ---------------------------------------------------------------------

#[test]
fn grouped_emit_drives_wave_and_touches_group_lock() {
    // A grouped state + grouped derived (same group, consistent). A
    // wave touching them resolves the group's wave-lock, so
    // `partition_count` (distinct touched groups) becomes 1.
    let rt = TestRuntime::new();
    let init = rt.binding.intern(TestValue::Int(1));
    let s = rt
        .core()
        .register(NodeRegistration {
            deps: vec![],
            fn_or_op: None,
            opts: NodeOpts {
                initial: init,
                scheduling_group: Some(G1),
                ..Default::default()
            },
        })
        .unwrap();
    let f = rt
        .binding
        .register_fn(|deps: &[TestValue]| match deps.first() {
            Some(TestValue::Int(v)) => Some(TestValue::Int(v + 1)),
            _ => None,
        });
    let d = rt
        .core()
        .register(NodeRegistration {
            deps: vec![s],
            fn_or_op: Some(graphrefly_core::NodeFnOrOp::Fn(f)),
            opts: NodeOpts {
                scheduling_group: Some(G1),
                ..Default::default()
            },
        })
        .unwrap();
    let _sub = rt
        .core()
        .subscribe(d, Arc::new(|_msgs: &[graphrefly_core::Message]| {}));
    let v = rt.binding.intern(TestValue::Int(41));
    rt.core().emit(s, v);
    assert_eq!(
        rt.core().partition_count(),
        1,
        "the wave touched scheduling group G1 → one group lock resolved"
    );
}

// ---------------------------------------------------------------------
// Single-owner floor: the one (non-generic, `RefCell`-backed) `Core` is
// a working lock-free substrate. D246/S2c: the `SingleThreadCell` /
// `LockedCell` cell-generic split is deleted — there is one `Core` and
// `Core::new` IS the single-owner lock-free path.
// ---------------------------------------------------------------------

#[test]
fn single_owner_core_is_functional() {
    let binding = common::TestBinding::new();
    let core: Core = Core::new(binding.clone() as Arc<dyn BindingBoundary>);
    let init = binding.intern(TestValue::Int(10));
    let s = core.register_state(init, false).unwrap();
    let f = binding.register_fn(|deps: &[TestValue]| match deps.first() {
        Some(TestValue::Int(v)) => Some(TestValue::Int(v * 2)),
        _ => None,
    });
    let d = core
        .register_derived(&[s], f, EqualsMode::Identity, false)
        .unwrap();
    // D248/S2c: the `Sink` type is `Arc<dyn Fn(..)>` (no `Send + Sync`
    // — single-owner). A plain `Vec` cell would suffice, but a `Mutex`
    // collector still works and keeps the assertion shape unchanged.
    let seen = Arc::new(std::sync::Mutex::new(Vec::<TestValue>::new()));
    let seen2 = seen.clone();
    let bclone = binding.clone();
    let _sub = core.subscribe(
        d,
        Arc::new(move |msgs: &[graphrefly_core::Message]| {
            for m in msgs {
                if let graphrefly_core::Message::Data(h) = m {
                    seen2.lock().unwrap().push(bclone.deref(*h));
                }
            }
        }),
    );
    // Subscribing to `d` activates it → it fires once on `s`'s initial
    // value (10 → 20, push-on-subscribe), then the explicit emit (21 →
    // 42). Both must be observed in order on the lock-free path.
    let v = binding.intern(TestValue::Int(21));
    core.emit(s, v);
    assert_eq!(
        *seen.lock().unwrap(),
        vec![TestValue::Int(20), TestValue::Int(42)],
        "single-owner lock-free Core dispatches activation + emit correctly"
    );
    // No node carries a group on the floor path.
    assert_eq!(core.partition_of(d), None);
}
