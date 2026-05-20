//! §7 scheduling-group concurrency — **actor-model rebuild (S4,
//! D238/D246/D250) + D253 (S5) `SchedulingGroupId`-surface deletion**.
//!
//! The pre-actor-model versions of these tests drove N threads on ONE
//! shared `Core` (cloned across threads), asserting the deleted
//! per-group `Arc<ReentrantMutex<()>>` wave-lock serialized/parallelized
//! cross-thread emits. D221/D246 made `Core` move-only `!Send + !Sync`
//! and S2c deleted the §7 group-lock machinery, so a single Core can no
//! longer be shared across threads — the original premise is structurally
//! gone (not a re-entry-seam issue; the *model* changed).
//!
//! Under the actor model, cross-`Core` parallelism is **host-native via
//! independent per-worker `Core`s**: each worker thread constructs and
//! owns its OWN `TestRuntime` (the `!Send` Core is built *inside* the
//! spawned closure — nothing crosses the thread boundary). The
//! correctness guarantees the model must still uphold are:
//!
//! - Independent Cores driven concurrently each serialize their own wave
//!   (in-order, exactly-once delivery) and never deadlock (the `join()`
//!   returning is the no-hang proof).
//! - Independent Cores do not block each other.
//! - Independent all-`None` (default lock-free floor) Cores keep cascade
//!   integrity (no torn multi-node cascade) under real concurrency.
//!
//! D253 (S5) further deletes the `SchedulingGroupId` declared-identity
//! surface: the cascades are now plain `state → derived` chains. The
//! "independent Cores serialize their own wave" invariant is independent
//! of group identity — group ids only ever supplied a wake-bit key, and
//! per the actor model the bit is per-Core (not per-group).
//!
//! Parallelism itself is a perf property, re-measured by
//! `benches/group_scaling.rs` with the same independent-Core shape.

mod common;

use std::thread;

use common::{TestRuntime, TestValue};
use graphrefly_core::{NodeFnOrOp, NodeOpts, NodeRegistration};

const WORKERS: usize = 4;
const EMITS: i64 = 20;

/// Build a plain `state → derived` cascade, subscribe a recorder on the
/// derived tail, then emit `1..=EMITS` and return the values the tail
/// delivered. Runs entirely on the calling thread's own `Core`. D253
/// (S5) dropped the `g: SchedulingGroupId` parameter — the post-S4
/// actor-model rebuild tests characterise independent-Core serialisation
/// and never needed the group id.
fn run_cascade(rt: &TestRuntime) -> Vec<TestValue> {
    let init = rt.binding.intern(TestValue::Int(0));
    let s = rt
        .core()
        .register(NodeRegistration {
            deps: vec![],
            fn_or_op: None,
            opts: NodeOpts {
                initial: init,
                ..Default::default()
            },
        })
        .expect("register state");
    let fid = rt
        .binding
        .register_fn(|vals: &[TestValue]| Some(vals[0].clone()));
    let d = rt
        .core()
        .register(NodeRegistration {
            deps: vec![s],
            fn_or_op: Some(NodeFnOrOp::Fn(fid)),
            opts: NodeOpts::default(),
        })
        .expect("register derived");
    let rec = rt.subscribe_recorder(d);
    for k in 1..=EMITS {
        let h = rt.binding.intern(TestValue::Int(k));
        rt.core().emit(s, h);
    }
    rt.drain_mailbox();
    rec.data_values()
}

/// Independent Cores, one per worker thread, driven concurrently. Each
/// worker constructs its OWN `!Send` `Core` inside the spawned closure
/// — **the workers share no state** (no shared Core, no shared binding,
/// no shared sink, no atomic counter); the deleted "two threads on one
/// shared Core serialize via the §7 group lock" contract is structurally
/// gone (D246/D248), and this test now characterises only the post-S4
/// invariant that each *independent* Core serialises its own wave
/// in-order (single-thread trivially true) and the `join()` succeeds
/// (no hang in the independent-Core path). (Replaces the deleted shared-
/// Core `same_group_cross_thread_emits_serialize_without_deadlock`.)
#[test]
fn independent_cores_each_serialize_without_deadlock() {
    let handles: Vec<_> = (0..WORKERS)
        .map(|_| {
            thread::spawn(|| {
                // `!Send` Core constructed INSIDE the worker (actor model).
                let rt = TestRuntime::new();
                run_cascade(&rt)
            })
        })
        .collect();
    // Push-on-subscribe replays the cached initial (state init = 0,
    // identity derived ⇒ d cache 0) as the first Data, then the 1..=EMITS
    // emits — in order, exactly once.
    let expected: Vec<TestValue> = std::iter::once(TestValue::Int(0))
        .chain((1..=EMITS).map(TestValue::Int))
        .collect();
    for h in handles {
        let delivered = h
            .join()
            .expect("worker Core completed without panic/deadlock");
        assert_eq!(
            delivered, expected,
            "each independent same-group Core serializes its own wave \
             in-order, exactly once (cached initial replayed first)"
        );
    }
}

/// Two independent Cores run concurrently and both complete — they
/// share no Core and no lock, so neither blocks the other. (Replaces
/// the deleted `disjoint_groups_cross_thread_waves_both_complete`; with
/// D253 the cores are simply two independent Cores, no group identity.)
#[test]
fn independent_cores_both_complete() {
    let a = thread::spawn(|| {
        let rt = TestRuntime::new();
        run_cascade(&rt)
    });
    let b = thread::spawn(|| {
        let rt = TestRuntime::new();
        run_cascade(&rt)
    });
    let expected: Vec<TestValue> = std::iter::once(TestValue::Int(0))
        .chain((1..=EMITS).map(TestValue::Int))
        .collect();
    let av = a.join().expect("Core A completed");
    let bv = b.join().expect("Core B completed");
    assert_eq!(av, expected, "independent Core A delivered fully");
    assert_eq!(bv, expected, "independent Core B delivered fully");
}

/// Independent all-`None` (default lock-free floor) Cores, one per
/// worker, each driving a diamond cascade `s → {a,b} → c=a+b`
/// concurrently. Each Core must keep cascade integrity — no torn
/// intermediate (a updated, b stale) ever delivered as a settled `c`;
/// every settled `c` is `2*k`. (Replaces the deleted
/// `all_none_default_core_cross_thread_cascade_integrity`.)
#[test]
fn all_none_default_independent_cores_cascade_integrity() {
    let handles: Vec<_> = (0..WORKERS)
        .map(|_| {
            thread::spawn(|| {
                let rt = TestRuntime::new();
                let s = rt.state(Some(TestValue::Int(0)));
                let a = rt.derived(&[s.id], |v| Some(v[0].clone()));
                let b = rt.derived(&[s.id], |v| Some(v[0].clone()));
                let c = rt.derived(&[a, b], |v| {
                    let int = |t: &TestValue| match t {
                        TestValue::Int(n) => *n,
                        other => panic!("diamond expected Int, got {other:?}"),
                    };
                    Some(TestValue::Int(int(&v[0]) + int(&v[1])))
                });
                let rec = rt.subscribe_recorder(c);
                for k in 1..=EMITS {
                    s.set(TestValue::Int(k));
                }
                rt.drain_mailbox();
                rec.data_values()
            })
        })
        .collect();
    // Push-on-subscribe replays the cached initial c (a=b=0 ⇒ c=0)
    // first; then each wave settles to c = 2*k (identity dedup).
    let expected: Vec<TestValue> = std::iter::once(TestValue::Int(0))
        .chain((1..=EMITS).map(|k| TestValue::Int(2 * k)))
        .collect();
    for h in handles {
        let delivered = h.join().expect("worker diamond Core completed");
        assert_eq!(
            delivered, expected,
            "all-None diamond keeps cascade integrity per independent Core \
             (no torn a/b → every settled c is 2*k, in order)"
        );
    }
}
