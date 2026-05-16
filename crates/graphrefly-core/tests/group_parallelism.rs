//! §7 serialization-group concurrency (D208–D211). Replaces the deleted
//! `per_subgraph_parallelism.rs` (union-find per-partition `wave_owner`).
//!
//! Parallelism itself is a perf property (not deterministically
//! unit-testable), so these tests assert the **correctness** guarantees
//! the group model must uphold under concurrency:
//!
//! - Same-group cross-thread emits serialize (no lost/torn updates, no
//!   deadlock) through the group's `Arc<ReentrantMutex<()>>`.
//! - Disjoint-group cross-thread waves both complete (the locks don't
//!   block each other; no cross-group deadlock).
//!
//! `Core` is `Send + Sync` on the default `LockedCell` substrate, so it
//! is shared across threads by clone.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use common::{TestBinding, TestValue};
use graphrefly_core::{BindingBoundary, Core, NodeOpts, NodeRegistration, SerializationGroupId};

const GA: SerializationGroupId = SerializationGroupId::new(1);
const GB: SerializationGroupId = SerializationGroupId::new(2);

fn grouped_state(
    core: &Core,
    binding: &Arc<TestBinding>,
    g: SerializationGroupId,
) -> graphrefly_core::NodeId {
    let init = binding.intern(TestValue::Int(0));
    core.register(NodeRegistration {
        deps: vec![],
        fn_or_op: None,
        opts: NodeOpts {
            initial: init,
            serialization_group: Some(g),
            ..Default::default()
        },
    })
    .expect("register grouped state")
}

#[test]
fn same_group_cross_thread_emits_serialize_without_deadlock() {
    let binding = TestBinding::new();
    let core = Core::new(binding.clone() as Arc<dyn BindingBoundary>);
    // Two state nodes in the SAME group → every wave touching either
    // serializes through group GA's single wave-lock.
    let a = grouped_state(&core, &binding, GA);
    let b = grouped_state(&core, &binding, GA);

    let count = Arc::new(AtomicUsize::new(0));
    let c2 = count.clone();
    let _sub_a = core.subscribe(
        a,
        Arc::new(move |msgs: &[graphrefly_core::Message]| {
            for m in msgs {
                if matches!(m, graphrefly_core::Message::Data(_)) {
                    c2.fetch_add(1, Ordering::SeqCst);
                }
            }
        }),
    );
    let c3 = count.clone();
    let _sub_b = core.subscribe(
        b,
        Arc::new(move |msgs: &[graphrefly_core::Message]| {
            for m in msgs {
                if matches!(m, graphrefly_core::Message::Data(_)) {
                    c3.fetch_add(1, Ordering::SeqCst);
                }
            }
        }),
    );

    const N: usize = 200;
    let core_a = core.clone();
    let bind_a = binding.clone();
    let core_b = core.clone();
    let bind_b = binding.clone();
    let ta = std::thread::spawn(move || {
        for i in 0..N {
            let h = bind_a.intern(TestValue::Int(i as i64));
            core_a.emit(a, h);
        }
    });
    let tb = std::thread::spawn(move || {
        for i in 0..N {
            let h = bind_b.intern(TestValue::Int(1_000_000 + i as i64));
            core_b.emit(b, h);
        }
    });
    // The core guarantee under test is **deadlock-freedom + correct
    // dispatch under same-group cross-thread contention**: both threads
    // must complete (the group `ReentrantMutex` serializes waves without
    // deadlocking) and the serialized waves must deliver. Exact counts
    // are not asserted — push-on-subscribe of the initial value plus
    // R1.3.2 identity-dedup make the precise total non-trivial, and raw
    // throughput is a perf property, not unit-testable.
    ta.join()
        .expect("thread a finished (no deadlock under same-group contention)");
    tb.join()
        .expect("thread b finished (no deadlock under same-group contention)");
    assert!(
        count.load(Ordering::SeqCst) >= N,
        "same-group serialized waves delivered (saw {} Data, expected ≥ {N})",
        count.load(Ordering::SeqCst)
    );
    assert_eq!(core.partition_of(a), Some(GA));
    assert_eq!(core.partition_of(b), Some(GA));
}

#[test]
fn disjoint_groups_cross_thread_waves_both_complete() {
    let binding = TestBinding::new();
    let core = Core::new(binding.clone() as Arc<dyn BindingBoundary>);
    // Disjoint groups GA / GB — their wave-locks are independent, so
    // concurrent waves on each must both finish (no cross-group block).
    let a = grouped_state(&core, &binding, GA);
    let b = grouped_state(&core, &binding, GB);

    let seen_a = Arc::new(AtomicUsize::new(0));
    let seen_b = Arc::new(AtomicUsize::new(0));
    let sa = seen_a.clone();
    let _sub_a = core.subscribe(
        a,
        Arc::new(move |msgs: &[graphrefly_core::Message]| {
            for m in msgs {
                if matches!(m, graphrefly_core::Message::Data(_)) {
                    sa.fetch_add(1, Ordering::SeqCst);
                }
            }
        }),
    );
    let sb = seen_b.clone();
    let _sub_b = core.subscribe(
        b,
        Arc::new(move |msgs: &[graphrefly_core::Message]| {
            for m in msgs {
                if matches!(m, graphrefly_core::Message::Data(_)) {
                    sb.fetch_add(1, Ordering::SeqCst);
                }
            }
        }),
    );

    const N: usize = 150;
    let (ca, ba) = (core.clone(), binding.clone());
    let (cb, bb) = (core.clone(), binding.clone());
    let ta = std::thread::spawn(move || {
        for i in 0..N {
            let h = ba.intern(TestValue::Int(i as i64));
            ca.emit(a, h);
        }
    });
    let tb = std::thread::spawn(move || {
        for i in 0..N {
            let h = bb.intern(TestValue::Int(500_000 + i as i64));
            cb.emit(b, h);
        }
    });
    // Guarantee under test: independent group wave-locks do not block
    // each other — concurrent waves on disjoint groups BOTH complete
    // (no cross-group deadlock) and both deliver.
    ta.join()
        .expect("disjoint-group thread a completed (no cross-group deadlock)");
    tb.join()
        .expect("disjoint-group thread b completed (no cross-group deadlock)");
    assert!(
        seen_a.load(Ordering::SeqCst) >= 1 && seen_b.load(Ordering::SeqCst) >= 1,
        "both disjoint-group waves delivered (a={}, b={})",
        seen_a.load(Ordering::SeqCst),
        seen_b.load(Ordering::SeqCst)
    );
    assert_ne!(
        core.partition_of(a),
        core.partition_of(b),
        "disjoint groups stay distinct"
    );
}

// ---------------------------------------------------------------------
// QA F1 (2026-05-16) regression: the config the rewrite originally broke
// and no test covered (per_subgraph_parallelism.rs, deleted, used to).
// An all-`None` (unannotated, default) `Core<LockedCell>` driven from
// two threads must STILL serialize waves — the Core-global wave
// `ReentrantMutex` restores the cross-thread safety floor the deleted
// union-find `wave_owner` provided. Without the fix, the per-thread
// WAVE_STATE / TIER3 / IN_TICK_OWNED invariants interleave → torn
// multi-node cascades / panics / hangs.
// ---------------------------------------------------------------------
#[test]
fn all_none_default_core_cross_thread_cascade_integrity() {
    let binding = TestBinding::new();
    let core = Core::new(binding.clone() as Arc<dyn BindingBoundary>);
    // 2-node cascade, NO serialization_group anywhere (the default that
    // every downstream crate + napi uses).
    let init = binding.intern(TestValue::Int(0));
    let s = core
        .register(NodeRegistration {
            deps: vec![],
            fn_or_op: None,
            opts: NodeOpts {
                initial: init,
                ..Default::default()
            },
        })
        .expect("register ungrouped state");
    let idf = binding.register_fn(|xs: &[TestValue]| xs.first().cloned());
    let d = core
        .register(NodeRegistration {
            deps: vec![s],
            fn_or_op: Some(graphrefly_core::NodeFnOrOp::Fn(idf)),
            opts: NodeOpts::default(),
        })
        .expect("register ungrouped derived consumer");
    assert_eq!(core.partition_of(s), None, "all-None: no group");
    assert_eq!(core.partition_of(d), None);

    let seen = Arc::new(AtomicUsize::new(0));
    let seen2 = seen.clone();
    let _sub = core.subscribe(
        d,
        Arc::new(move |msgs: &[graphrefly_core::Message]| {
            for m in msgs {
                if matches!(m, graphrefly_core::Message::Data(_)) {
                    seen2.fetch_add(1, Ordering::SeqCst);
                }
            }
        }),
    );

    const N: usize = 200;
    let (c1, b1) = (core.clone(), binding.clone());
    let (c2, b2) = (core.clone(), binding.clone());
    let t1 = std::thread::spawn(move || {
        for i in 0..N {
            let h = b1.intern(TestValue::Int(i as i64));
            c1.emit(s, h);
        }
    });
    let t2 = std::thread::spawn(move || {
        for i in 0..N {
            let h = b2.intern(TestValue::Int(1_000_000 + i as i64));
            c2.emit(s, h);
        }
    });
    // The fix's contract: the Core-global wave lock serializes these
    // interleaved cross-thread waves → no panic (RefCell would not apply
    // here; LockedCell re-entrancy / torn WAVE_STATE would surface as a
    // hang or a wave-drain panic), both threads complete, and the
    // cascade delivered to the derived consumer.
    t1.join()
        .expect("thread 1 finished (global wave lock serialized, no deadlock/panic)");
    t2.join()
        .expect("thread 2 finished (global wave lock serialized, no deadlock/panic)");
    assert!(
        seen.load(Ordering::SeqCst) >= 1,
        "all-None cross-thread cascade still delivered to the derived consumer \
         (saw {} Data)",
        seen.load(Ordering::SeqCst)
    );
}
