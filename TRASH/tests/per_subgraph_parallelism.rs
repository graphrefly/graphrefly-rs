//! Phase H of D3 closure (Slice Y1+Y2) — comprehensive cross-partition
//! parallelism scenarios beyond the 2-partition acceptance tests in
//! [`tests/lock_released.rs`] (`concurrent_emit_on_disjoint_partitions_runs_truly_parallel`
//! and `concurrent_emit_on_same_partition_serializes`).
//!
//! These exercise the per-partition `wave_owner` substrate landed in
//! Phase E (D092) + the split-eager protocol from Phase F (D086), pinning:
//!
//! 1. **3+ disjoint partitions concurrently emit** — `wave_owner`
//!    granularity is per-partition, not per-Core. Property tested: a
//!    cross-thread emit on a disjoint partition does NOT block at
//!    `partition_wave_owner_lock_arc`.
//! 2. **Cross-partition cascade via meta-companion** — `compute_touched_partitions`
//!    walks `meta_companions` and acquires every reachable partition in
//!    ascending `SubgraphId` order (Q7). Two assertions: a disjoint
//!    partition's emit is NOT blocked, AND a held meta-target partition's
//!    emit IS blocked (directly pins the meta-walk property).
//! 3. **Reciprocal cross-partition cascades are deadlock-free** — two
//!    threads doing meta-companion-driven cross-partition emits both
//!    complete within the deadlock-detection timeout AND each completes
//!    its full N-emit loop (atomic per-thread liveness counter detects
//!    partial-deadlock that a wall-clock timeout alone would mask).
//! 4. **Loom-checked reciprocal arg-order acquire** — abstract model
//!    verifying that `acquire_two_in_ascending_order` produces the
//!    same lock acquisition order regardless of caller arg-order. Two
//!    threads call with reciprocal arg orders; sort makes them both
//!    acquire `{X, Y}` in low-then-high order; AB/BA cycle is
//!    structurally ruled out (Q4 + Q6 item 4). **The loom model is
//!    intentionally narrow** — it abstracts `parking_lot::ReentrantMutex`
//!    as `loom::sync::Mutex` (non-reentrant), so same-thread
//!    re-entrancy is NOT verified at the model level (parking_lot's
//!    library guarantees + the std-thread tests cover that).
//!    `Subscription::Drop` in the current Rust impl does NOT acquire
//!    a partition `wave_owner` — the loom test is forward-compat
//!    infra for the Q5 scope item (drop-cascade-with-partition-acquire)
//!    if/when that lift lands.
//! 5. **Cross-partition acquire-during-fire (Phase H+ hazard)** —
//!    `#[ignore]`-gated regression test asset for the deferred Phase H+
//!    bundle (porting-deferred entry; see Q-E in the slice HALT).
//!    Three trigger surfaces share the same protocol gap: producer-
//!    pattern subscribe-during-fire, lifecycle re-entry on a different
//!    partition mid-fire, and `Subscription::Drop` from inside a fire
//!    (the latter activates only if Q5 lift makes Drop acquire
//!    wave_owners). The structural fix is out of scope for D3 closure.

#![forbid(unsafe_code)]
#![allow(clippy::too_many_lines)]

mod common;

#[cfg(not(loom))]
mod std_thread_tests {
    use super::common::{Recorder, TestRuntime, TestValue};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};

    /// Helper: spin-wait on a flag with a deadlock-detection timeout.
    fn wait_until(flag: &AtomicU64, target: u64, secs: u64, what: &str) {
        let start = Instant::now();
        while flag.load(Ordering::SeqCst) != target {
            assert!(
                start.elapsed() < Duration::from_secs(secs),
                "timed out waiting for {what} to reach {target} (current = {})",
                flag.load(Ordering::SeqCst),
            );
            thread::sleep(Duration::from_millis(1));
        }
    }

    /// Helper: join a thread with a deadlock-detection timeout.
    fn join_with_timeout(handle: thread::JoinHandle<()>, secs: u64, what: &str) {
        let start = Instant::now();
        loop {
            if handle.is_finished() {
                handle.join().expect("thread panicked");
                return;
            }
            assert!(
                start.elapsed() < Duration::from_secs(secs),
                "{what} did not finish within {secs}s — likely deadlock"
            );
            thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn three_disjoint_partitions_emit_returns_concurrently_while_one_blocked() {
        // Extends the 2-partition acceptance test
        // `concurrent_emit_on_disjoint_partitions_runs_truly_parallel`
        // to 3 disjoint partitions. The parallelism property tested
        // is narrow: cross-thread emits on disjoint partitions do NOT
        // block at `partition_wave_owner_lock_arc`. State mutex
        // contention (Regime A — see Phase J bench) and the
        // Core-global `in_tick` flag still serialize portions of the
        // wave path; this test is NOT a parallel-drain test.
        //
        // Test shape:
        //   - s1 has a derived d1 whose fn blocks on rx (drives in_tick;
        //     holds partition(s1) wave_owner for the wave's duration).
        //   - s2 and s3 are state-only (no consumers), in disjoint partitions.
        //   - Thread 1 emits on s1; fn enters and blocks on rx.
        //   - Threads 2 and 3 emit on s2 and s3 in parallel.
        //
        // What happens under per-partition wave_owner:
        //   - Thread 2 acquires partition(s2)'s wave_owner — disjoint
        //     from partition(s1), no contention.
        //   - Thread 2 acquires `lock_state()` (Core-global state mutex)
        //     briefly — may serialize against Thread 3 (Regime A
        //     state-mutex contention) but NOT against Thread 1's
        //     blocked fn (Thread 1 dropped state mutex around
        //     lock-released invoke_fn).
        //   - Thread 2's `commit_emission` writes the cache. s2 has no
        //     consumers, so no children are queued into pending_fires.
        //   - Thread 2's BatchGuard sees in_tick=true (Thread 1's wave
        //     still owns it) → drops as non-owning no-op. Returns.
        //   - Thread 3 same.
        //
        // Under the legacy whole-Core wave_owner (pre-Phase-E), Threads
        // 2 and 3 would both block at the Core-global mutex acquired
        // by Thread 1 — serializing 3 ways and timing out on Thread 1's
        // release. Under per-partition, both return BEFORE Thread 1's
        // release.
        let rt = TestRuntime::new();
        let s1 = rt.state(None);
        let s2 = rt.state(Some(TestValue::Int(0)));
        let s3 = rt.state(Some(TestValue::Int(0)));

        let p1 = rt.core.partition_of(s1.id).expect("registered");
        let p2 = rt.core.partition_of(s2.id).expect("registered");
        let p3 = rt.core.partition_of(s3.id).expect("registered");
        assert_ne!(p1, p2);
        assert_ne!(p1, p3);
        assert_ne!(p2, p3);

        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let rx = Arc::new(Mutex::new(Some(rx)));
        let rx_for_fn = rx.clone();
        let entered = Arc::new(AtomicU64::new(0));
        let entered_for_fn = entered.clone();

        let d1 = rt.derived(&[s1.id], move |deps| {
            entered_for_fn.fetch_add(1, Ordering::SeqCst);
            let recv = rx_for_fn.lock().unwrap().take();
            if let Some(rx) = recv {
                let _ = rx.recv();
            }
            Some(deps[0].clone())
        });
        let _r1: Recorder = rt.subscribe_recorder(d1);

        // Thread 1: emit on s1 → drives the wave that blocks in d1's fn.
        let core_1 = rt.core.clone();
        let s1_id = s1.id;
        let binding_1 = rt.binding.clone();
        let thread_1 = thread::spawn(move || {
            let h = binding_1.intern(TestValue::Int(1));
            core_1.emit(s1_id, h);
        });
        wait_until(&entered, 1, 5, "Thread 1 fn entry");

        // Threads 2 and 3: both emit on disjoint state nodes. Both must
        // return BEFORE Thread 1's release. Under per-partition
        // wave_owner, partition(s2) and partition(s3) are unrelated to
        // partition(s1)'s held mutex. Both become non-owning batches
        // (in_tick=true set by Thread 1) and return quickly.
        let t2_done = Arc::new(AtomicU64::new(0));
        let t3_done = Arc::new(AtomicU64::new(0));
        let done_2 = t2_done.clone();
        let done_3 = t3_done.clone();
        let core_2 = rt.core.clone();
        let core_3 = rt.core.clone();
        let s2_id = s2.id;
        let s3_id = s3.id;
        let binding_2 = rt.binding.clone();
        let binding_3 = rt.binding.clone();
        let thread_2 = thread::spawn(move || {
            let h = binding_2.intern(TestValue::Int(20));
            core_2.emit(s2_id, h);
            done_2.store(1, Ordering::SeqCst);
        });
        let thread_3 = thread::spawn(move || {
            let h = binding_3.intern(TestValue::Int(30));
            core_3.emit(s3_id, h);
            done_3.store(1, Ordering::SeqCst);
        });

        // Both should complete BEFORE tx.send (Thread 1 still blocked).
        wait_until(&t2_done, 1, 10, "Thread 2 emit");
        wait_until(&t3_done, 1, 10, "Thread 3 emit");

        tx.send(()).expect("release Thread 1");
        join_with_timeout(thread_1, 5, "thread 1");
        join_with_timeout(thread_2, 5, "thread 2");
        join_with_timeout(thread_3, 5, "thread 3");
    }

    #[test]
    fn cross_partition_cascade_does_not_block_disjoint_third_partition() {
        // Setup:
        //   - A in partition P_A, B in partition P_B (disjoint by construction).
        //   - `add_meta_companion(A, B)` — `compute_touched_partitions(A)`
        //     now walks {A, B} and returns {P_A, P_B}.
        //   - C in partition P_C, fully disjoint from both.
        //
        // Thread 1: emits on `_d_a` (subscribes to A) — wave acquires
        // {P_A, P_B} in ascending order, fn blocks on rx.
        // Thread 2: emits on C — should acquire only P_C and finish
        // BEFORE Thread 1's fn is released (pins: meta-companion
        // cascade does NOT over-serialize against unrelated partitions).
        // Thread 3: emits on B (the meta-target) — MUST block until
        // Thread 1 releases (pins: meta-walk DID acquire P_B; this
        // catches a regression where `compute_touched_partitions`
        // skips meta-edges and Thread 1 only acquires P_A).
        let rt = TestRuntime::new();
        let s_a = rt.state(None);
        let s_b = rt.state(Some(TestValue::Int(0)));
        let s_c = rt.state(Some(TestValue::Int(0)));

        let p_a = rt.core.partition_of(s_a.id).expect("registered");
        let p_b = rt.core.partition_of(s_b.id).expect("registered");
        let p_c = rt.core.partition_of(s_c.id).expect("registered");
        assert_ne!(p_a, p_b);
        assert_ne!(p_a, p_c);
        assert_ne!(p_b, p_c);

        // Meta-companion edge: A → B. Adding it does NOT union
        // partitions (per `add_meta_companion` semantics — it's a
        // teardown-cascade edge), but it does cause
        // `compute_touched_partitions(s_a)` to return {P_A, P_B}.
        rt.core.add_meta_companion(s_a.id, s_b.id);
        // Confirm the partitions are STILL disjoint after adding the
        // meta-companion (the registry should not have unioned them).
        assert_eq!(rt.core.partition_of(s_a.id), Some(p_a));
        assert_eq!(rt.core.partition_of(s_b.id), Some(p_b));

        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let rx = Arc::new(Mutex::new(Some(rx)));
        let rx_for_fn = rx.clone();
        let entered = Arc::new(AtomicU64::new(0));
        let entered_for_fn = entered.clone();

        let d_a = rt.derived(&[s_a.id], move |deps| {
            entered_for_fn.fetch_add(1, Ordering::SeqCst);
            let recv = rx_for_fn.lock().unwrap().take();
            if let Some(rx) = recv {
                let _ = rx.recv();
            }
            Some(deps[0].clone())
        });
        let _rec_a: Recorder = rt.subscribe_recorder(d_a);

        let core_a = rt.core.clone();
        let s_a_id = s_a.id;
        let binding_a = rt.binding.clone();
        let thread_1 = thread::spawn(move || {
            let h = binding_a.intern(TestValue::Int(1));
            core_a.emit(s_a_id, h);
        });

        // Wait for Thread 1 to be inside its blocked fn — partitions
        // {P_A, P_B} are now held.
        wait_until(&entered, 1, 5, "Thread 1 fn entry");

        // Thread 2 emits on C. P_C is disjoint from {P_A, P_B}. This
        // emit must finish without waiting for Thread 1's release.
        let thread_2_done = Arc::new(AtomicU64::new(0));
        let done_for_t2 = thread_2_done.clone();
        let core_c = rt.core.clone();
        let s_c_id = s_c.id;
        let binding_c = rt.binding.clone();
        let thread_2 = thread::spawn(move || {
            let h = binding_c.intern(TestValue::Int(99));
            core_c.emit(s_c_id, h);
            done_for_t2.store(1, Ordering::SeqCst);
        });

        // Thread 3 emits on B — the meta-target. Under correct meta-walk,
        // P_B IS held by Thread 1, so Thread 3 BLOCKS on
        // partition(s_b)'s wave_owner. Under a regression that strips
        // meta-edge walking from `compute_touched_partitions`, Thread 1
        // would hold only P_A and Thread 3 would race through (no block).
        // We use entry/exit atomic flags — same pattern as the
        // `concurrent_emit_on_same_partition_serializes` test in
        // `lock_released.rs` — to deterministically distinguish "Thread 3
        // started the emit and is blocked" from "Thread 3 raced through
        // (regression)".
        let t3_entered = Arc::new(AtomicU64::new(0));
        let t3_exited = Arc::new(AtomicU64::new(0));
        let entered_for_t3 = t3_entered.clone();
        let exited_for_t3 = t3_exited.clone();
        let core_b = rt.core.clone();
        let s_b_id = s_b.id;
        let binding_b = rt.binding.clone();
        let thread_3 = thread::spawn(move || {
            let h = binding_b.intern(TestValue::Int(77));
            entered_for_t3.store(1, Ordering::SeqCst);
            core_b.emit(s_b_id, h);
            exited_for_t3.store(1, Ordering::SeqCst);
        });

        // Deterministic event-ordering: Thread 2 must complete BEFORE
        // tx.send (Thread 1's release). Thread 3 must STILL be blocked
        // (entered but not exited) at the same point.
        wait_until(&thread_2_done, 1, 10, "Thread 2 emit");
        wait_until(&t3_entered, 1, 5, "Thread 3 emit entry");
        // Give Thread 3 a wall-clock window to be sure its emit attempt
        // has been made + blocked. Same 100ms pattern as
        // `concurrent_emit_on_same_partition_serializes` (lock_released.rs).
        thread::sleep(Duration::from_millis(100));
        assert_eq!(
            t3_exited.load(Ordering::SeqCst),
            0,
            "Thread 3's emit on s_b must block on partition(s_b)'s wave_owner \
             held by Thread 1's meta-companion-driven cascade. The exit flag \
             being set early would mean `compute_touched_partitions` did NOT \
             walk the meta-edge — meta-walk regression."
        );

        tx.send(()).expect("release Thread 1");
        join_with_timeout(thread_1, 5, "Thread 1");
        join_with_timeout(thread_2, 5, "Thread 2");
        join_with_timeout(thread_3, 5, "Thread 3");
    }

    #[test]
    fn reciprocal_cross_partition_cascades_complete_without_deadlock() {
        // The acid test for Q4 + Q7 ascending-`SubgraphId` acquisition
        // ordering: two threads each driving cross-partition cascades
        // that collectively touch the SAME pair of partitions but in
        // logically reciprocal directions (A's meta-companion points to
        // B; A2's meta-companion points to a third node in B's
        // partition). Both `compute_touched_partitions` calls return
        // {P_X, P_Y} and acquire in ASCENDING order — no thread can
        // beat the other to the partitions in opposite order. AB/BA
        // deadlock is structurally ruled out.
        //
        // Without ascending-order acquisition this test would deadlock
        // ~half the time under load (depending on schedule).
        let rt = TestRuntime::new();
        let a = rt.state(None);
        let b = rt.state(None);
        // Anchor nodes pin the partitions: union via dep edges to keep
        // a/a2 in P_X and b/b2 in P_Y after meta-companion edges are
        // added (meta-companion does NOT union partitions; dep edges do).
        let a2 = rt.state(None);
        let b2 = rt.state(None);
        // Union: a and a2 share P_X.
        let _join_x = rt.derived(&[a.id, a2.id], |_deps| Some(TestValue::Int(0)));
        // Union: b and b2 share P_Y.
        let _join_y = rt.derived(&[b.id, b2.id], |_deps| Some(TestValue::Int(0)));

        let p_a = rt.core.partition_of(a.id).expect("registered");
        let p_b = rt.core.partition_of(b.id).expect("registered");
        assert_ne!(p_a, p_b);
        assert_eq!(rt.core.partition_of(a2.id), Some(p_a));
        assert_eq!(rt.core.partition_of(b2.id), Some(p_b));

        // Reciprocal cross-partition meta-companions: emits on a or b2
        // each touch BOTH partitions.
        rt.core.add_meta_companion(a.id, b.id); // a → b
        rt.core.add_meta_companion(b2.id, a2.id); // b2 → a2
                                                  // Partitions stay disjoint.
        assert_ne!(rt.core.partition_of(a.id), rt.core.partition_of(b.id));

        // Wire derived consumers so emits actually drive a wave (not
        // just cache an unobserved value).
        let d_a = rt.derived(&[a.id], |deps| Some(deps[0].clone()));
        let d_b2 = rt.derived(&[b2.id], |deps| Some(deps[0].clone()));
        let _rec_a: Recorder = rt.subscribe_recorder(d_a);
        let _rec_b2: Recorder = rt.subscribe_recorder(d_b2);

        // Each thread fires N emits in a tight loop. Under any AB/BA
        // window, the loop maximizes the chance of hitting the cycle.
        // Per-thread atomic counter increments PER EMIT — this is the
        // liveness signal. A wall-clock timeout alone can't distinguish
        // "running, just slow CI" from "one thread made 1 iter then
        // stuck" (partial-deadlock — the canonical regression a 30s
        // timeout can mask). Asserting `t1_progress == N` AND
        // `t2_progress == N` post-join catches that explicitly.
        const N: usize = 200;
        let t1_progress = Arc::new(AtomicU64::new(0));
        let t2_progress = Arc::new(AtomicU64::new(0));
        let progress_1 = t1_progress.clone();
        let progress_2 = t2_progress.clone();
        let core1 = rt.core.clone();
        let a_id = a.id;
        let binding1 = rt.binding.clone();
        let thread_1 = thread::spawn(move || {
            for i in 0..N {
                let h = binding1.intern(TestValue::Int(i as i64));
                core1.emit(a_id, h);
                progress_1.fetch_add(1, Ordering::SeqCst);
            }
        });
        let core2 = rt.core.clone();
        let b2_id = b2.id;
        let binding2 = rt.binding.clone();
        let thread_2 = thread::spawn(move || {
            for i in 0..N {
                let h = binding2.intern(TestValue::Int((1_000 + i) as i64));
                core2.emit(b2_id, h);
                progress_2.fetch_add(1, Ordering::SeqCst);
            }
        });

        // Generous deadlock-detection budget: 30s tolerates very slow
        // CI but is still a hard signal that something is blocked
        // permanently. The progress assertions below catch a
        // partial-deadlock where one thread stalls but the timeout
        // doesn't fire.
        join_with_timeout(thread_1, 30, "Thread 1 reciprocal cascade");
        join_with_timeout(thread_2, 30, "Thread 2 reciprocal cascade");

        // Liveness assertions: both threads must have completed all N
        // iterations. A "stuck after iter k < N" partial-deadlock fails
        // here even if the wall-clock timeout was generous enough to
        // hide it.
        assert_eq!(
            t1_progress.load(Ordering::SeqCst),
            N as u64,
            "Thread 1 did not complete all {N} emits — partial deadlock \
             or stall (counter shows iter count reached, not full N)"
        );
        assert_eq!(
            t2_progress.load(Ordering::SeqCst),
            N as u64,
            "Thread 2 did not complete all {N} emits — partial deadlock \
             or stall"
        );

        // Sanity: caches are non-zero (something was committed). Per-emit
        // ordering is non-deterministic across threads, so we don't
        // assert specific values — the liveness counters above are the
        // load-bearing assertions.
        let cache_a = rt.core.cache_of(a.id);
        let cache_b2 = rt.core.cache_of(b2.id);
        assert_ne!(cache_a, graphrefly_core::HandleId::new(0));
        assert_ne!(cache_b2, graphrefly_core::HandleId::new(0));
    }

    // =================================================================
    // Phase H+ STRICT deferred-producer-op regression tests (D115)
    // =================================================================
    //
    // These tests pin the defer-and-succeed behavior that replaced the
    // `IN_PRODUCER_BUILD` carve-out. Producer-pattern operators that
    // hit a partition-order violation now defer to wave-end instead of
    // bypassing the check.

    #[test]
    fn producer_cross_partition_emit_defers_and_succeeds() {
        // A producer-pattern node subscribes to `a` (higher-id partition)
        // and on receiving DATA, calls `emit_or_defer` on `b` (lower-id
        // partition). Under Phase H+ STRICT this defers (not panics) and
        // eventually delivers the emission to downstream subscribers of b.
        let rt = TestRuntime::new();
        let s_b = rt.state(Some(TestValue::Int(0)));
        let s_a = rt.state(None);

        // Confirm partition ordering: s_a's derived will have a HIGHER
        // partition id than s_b's standalone partition (s_b was registered
        // first → lower id).
        let p_b = rt.core.partition_of(s_b.id).expect("registered");
        let p_a = rt.core.partition_of(s_a.id).expect("registered");
        assert!(
            p_a.raw() > p_b.raw(),
            "fixture invariant: partition(s_a) must be > partition(s_b) \
             for the cross-partition emit to trigger descending-order deferral. \
             Got p_a={p_a:?}, p_b={p_b:?}"
        );

        // The "producer sink" pattern: a derived node that, during its fn
        // callback (holding partition(s_a)), calls emit_or_defer on s_b
        // (lower partition). This simulates what a producer-pattern operator
        // sink does when it forwards data.
        let core_for_fn = rt.core.clone();
        let binding_for_fn = rt.binding.clone();
        let s_b_id = s_b.id;
        let d_producer = rt.derived(&[s_a.id], move |deps| {
            if let TestValue::Int(n) = deps[0] {
                let h = binding_for_fn.intern(TestValue::Int(n * 100));
                // This would have panicked pre-D115 (or been suppressed
                // by the IN_PRODUCER_BUILD carve-out). Now it defers.
                core_for_fn.emit_or_defer(s_b_id, h);
            }
            Some(deps[0].clone())
        });

        // Subscribe to both the producer-derived and to s_b to observe
        // the deferred emission landing.
        let rec_d: Recorder = rt.subscribe_recorder(d_producer);
        let rec_b: Recorder = rt.subscribe_recorder(s_b.id);

        // Emit on s_a → fires d_producer's fn → emit_or_defer on s_b
        // → deferred → drains at wave-end → s_b gets the value.
        let h = rt.binding.intern(TestValue::Int(7));
        rt.core.emit(s_a.id, h);

        // d_producer should have received the value from s_a.
        let d_data = rec_d.data_values();
        assert!(
            d_data.contains(&TestValue::Int(7)),
            "d_producer should see s_a's emission; got: {d_data:?}"
        );

        // s_b should have received the deferred emission (7 * 100 = 700).
        let b_data = rec_b.data_values();
        assert!(
            b_data.contains(&TestValue::Int(700)),
            "s_b should receive the deferred emission (700); got: {b_data:?}"
        );
    }

    #[test]
    fn producer_cross_partition_subscribe_defers_and_succeeds() {
        // A producer whose build closure (simulated via a derived fn)
        // pushes a DeferredProducerOp::Callback that subscribes to a
        // source in a LOWER-id partition than the producer's own. The
        // subscription is deferred to wave-end and succeeds.
        let rt = TestRuntime::new();
        let s_low = rt.state(Some(TestValue::Int(42)));
        let s_trigger = rt.state(None);

        let p_low = rt.core.partition_of(s_low.id).expect("registered");
        let p_trigger = rt.core.partition_of(s_trigger.id).expect("registered");
        assert!(
            p_trigger.raw() > p_low.raw(),
            "fixture invariant: partition(s_trigger) > partition(s_low). \
             Got p_trigger={p_trigger:?}, p_low={p_low:?}"
        );

        // We'll use a shared recorder that the deferred subscribe
        // wires up. Once the deferred subscribe fires, data from s_low
        // should reach this recorder.
        let deferred_events: Arc<Mutex<Vec<TestValue>>> = Arc::new(Mutex::new(Vec::new()));
        let events_for_sub = deferred_events.clone();
        let binding_for_sub = rt.binding.clone();

        // The "producer build" pattern: a derived node that, during its fn,
        // pushes a DeferredProducerOp::Callback to subscribe to s_low.
        let core_for_fn = rt.core.clone();
        let s_low_id = s_low.id;
        let d_build = rt.derived(&[s_trigger.id], move |deps| {
            if let TestValue::Int(_n) = deps[0] {
                let events_c = events_for_sub.clone();
                let binding_c = binding_for_sub.clone();
                let core_c = core_for_fn.clone();
                let sink: graphrefly_core::Sink =
                    Arc::new(move |msgs: &[graphrefly_core::Message]| {
                        for msg in msgs {
                            if let graphrefly_core::Message::Data(h) = msg {
                                let v = binding_c.deref(*h);
                                events_c.lock().unwrap().push(v);
                            }
                        }
                    });
                // Push a deferred callback that subscribes to s_low.
                // During this fn, partition(s_trigger) is held — subscribing
                // to s_low (lower partition) would violate ascending order.
                // The Callback variant runs at wave-end when no partitions
                // are held.
                let core_for_cb = core_c.clone();
                core_c.push_deferred_producer_op(graphrefly_core::DeferredProducerOp::Callback(
                    Box::new(move || {
                        let sub = core_for_cb.subscribe(s_low_id, sink);
                        // Keep the subscription alive without dropping it —
                        // ManuallyDrop prevents unsubscribe while avoiding
                        // the unsound `mem::forget` pattern.
                        let _ = std::mem::ManuallyDrop::new(sub);
                    }),
                ));
            }
            Some(deps[0].clone())
        });

        let _rec_d: Recorder = rt.subscribe_recorder(d_build);

        // Trigger the build: emit on s_trigger fires d_build's fn
        // which pushes the deferred subscribe callback.
        let h = rt.binding.intern(TestValue::Int(1));
        rt.core.emit(s_trigger.id, h);

        // After the wave drains, the deferred callback ran and
        // subscribed to s_low. s_low has cache = 42, so the handshake
        // should have delivered Data(42) to our deferred_events.
        let events = deferred_events.lock().unwrap();
        assert!(
            events.contains(&TestValue::Int(42)),
            "deferred subscribe should receive s_low's cached value (42); \
             got: {events:?}"
        );
    }

    #[test]
    fn deferred_emit_ordering_preserved() {
        // A producer-pattern node defers multiple emissions in sequence
        // (h1, h2, h3). All three must be delivered in FIFO order when
        // the deferred queue drains.
        let rt = TestRuntime::new();
        let s_target = rt.state(Some(TestValue::Int(0)));
        let s_source = rt.state(None);

        let p_target = rt.core.partition_of(s_target.id).expect("registered");
        let p_source = rt.core.partition_of(s_source.id).expect("registered");
        assert!(
            p_source.raw() > p_target.raw(),
            "fixture invariant: partition(s_source) > partition(s_target). \
             Got p_source={p_source:?}, p_target={p_target:?}"
        );

        let core_for_fn = rt.core.clone();
        let binding_for_fn = rt.binding.clone();
        let s_target_id = s_target.id;
        let d_multi = rt.derived(&[s_source.id], move |deps| {
            if let TestValue::Int(_n) = deps[0] {
                // Defer 3 emissions in sequence on s_target (lower partition).
                for i in 1..=3i64 {
                    let h = binding_for_fn.intern(TestValue::Int(i * 10));
                    core_for_fn.emit_or_defer(s_target_id, h);
                }
            }
            Some(deps[0].clone())
        });

        let _rec_d: Recorder = rt.subscribe_recorder(d_multi);
        let rec_target: Recorder = rt.subscribe_recorder(s_target.id);

        // Trigger: emit on s_source fires d_multi's fn which defers 3
        // emissions on s_target.
        let h = rt.binding.intern(TestValue::Int(1));
        rt.core.emit(s_source.id, h);

        // All three deferred emissions should have landed on s_target
        // in order: 10, 20, 30.
        let target_data = rec_target.data_values();
        // Filter out the initial cache push (0) from subscribe handshake.
        let deferred_values: Vec<i64> = target_data
            .iter()
            .filter_map(|v| match v {
                TestValue::Int(n) if *n > 0 => Some(*n),
                _ => None,
            })
            .collect();
        assert_eq!(
            deferred_values,
            vec![10, 20, 30],
            "deferred emissions must arrive in FIFO order; got: {deferred_values:?}"
        );
    }

    #[test]
    fn user_fn_cross_partition_emit_during_fire_panics_with_h_plus_diagnostic() {
        // Phase H+ option (d) /qa N1(a) widened variant —
        // ACTIVATED 2026-05-09.
        //
        // The shipped Rust impl enforces the same-thread
        // ascending-`SubgraphId` rule for cross-partition acquires
        // whenever this thread already holds at least one partition
        // wave_owner AND we're not inside a producer build closure.
        // The previous gate ("during fn-fire only" via fire_depth)
        // missed sink-callback / handshake / drop-cleanup re-entry
        // paths; the widened gate ("held non-empty + !in_producer_build")
        // closes those gaps.
        //
        // This test pins the panic-on-violation behavior directly:
        // build a topology where a derived's user fn re-enters
        // `Core::emit` on a partition with a SMALLER id than the
        // firing node's partition (descending order — the canonical
        // hazard). The H+ check fires and panics with a clear
        // diagnostic naming the canonical fix patterns.
        //
        // The complementary SAFE pattern is in
        // `tests/lock_released.rs::fn_can_reenter_core_emit_during_invoke_fn_runs_nested_wave`
        // — same topology shape but with `add_meta_companion` on the
        // outer wave entry so the cross-partition is acquired ascending
        // upfront and the inner emit is re-entrant (no panic).
        //
        // **POST-PANIC ASSERTIONS (per /qa A3/A10):** verify the H+
        // thread-local `HELD` is CLEAN after the panic unwinds. Cargo's
        // default test runner reuses threads — a leak would corrupt
        // subsequent tests on the same thread (spurious H+ panics).
        // The scope-guard pattern in `partition_wave_owner_lock_arc`
        // (per /qa A1) is what makes this safe; this assertion is the
        // regression test for that guard.
        //
        // **NOTE**: this test asserts the impl REJECTS the unsafe
        // pattern. If the broader fix lands (option (b) defer-acquire-
        // to-post-flush, or option (d) typed-error variant), update
        // this test to assert the new safe behavior instead.

        // /qa A9: AssertUnwindSafe rationale — `rt: TestRuntime`
        // captures `parking_lot::Mutex` instances which, unlike
        // `std::sync::Mutex`, do NOT poison on panic. After
        // catch_unwind returns, `rt` is dropped but never accessed
        // by this test's body. If a future contributor swaps the
        // dispatcher's mutex type to `std::sync::Mutex`, that swap
        // would silently break the test (poisoning during rt's
        // Drop). The choice is sound under the parking_lot impl;
        // re-evaluate if the dispatcher mutex type changes.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let rt = TestRuntime::new();
            let s_in = rt.state(Some(TestValue::Int(0)));
            let s_side = rt.state(Some(TestValue::Int(0)));
            // Intentionally NO `add_meta_companion` — the descending
            // cross-partition emit during d's fire is what we WANT
            // the H+ check to catch.

            let core = rt.core.clone();
            let s_side_id = s_side.id;
            let binding = rt.binding.clone();
            let d = rt.derived(&[s_in.id], move |deps| {
                if let TestValue::Int(n) = deps[0] {
                    let h = binding.intern(TestValue::Int(n * 10));
                    // Cross-partition emit during fn-fire. d's
                    // partition includes s_in and was unioned to a
                    // higher SubgraphId (after register_derived);
                    // s_side's partition is a smaller SubgraphId.
                    // The H+ check should panic here.
                    core.emit(s_side_id, h);
                }
                Some(deps[0].clone())
            });

            // /qa A6: fixture invariant — pin the descending-order
            // assumption explicitly. If a future change to
            // `union_nodes`'s tiebreak swaps the partition-root
            // selection (e.g., smaller-id-wins instead of
            // larger-id-wins), the test would otherwise pass for the
            // wrong reason (the cross-partition emit becomes
            // ASCENDING, no panic, `expect_err` below fails with
            // "H+ enforcement regression" message — but the actual
            // root cause is fixture topology drift, not H+
            // regression). This assertion catches the topology drift
            // immediately with a clear diagnostic.
            let p_d = rt.core.partition_of(d).expect("d registered");
            let p_side = rt.core.partition_of(s_side.id).expect("s_side registered");
            assert!(
                p_d.raw() > p_side.raw(),
                "fixture invariant violated: this test requires partition(d) \
                 > partition(s_side) so the inner emit is descending order. \
                 Got partition(d)={p_d:?}, partition(s_side)={p_side:?}. \
                 If union_nodes' tiebreak was changed (e.g., to pick \
                 smaller-id-as-root on equal rank), reconstruct the topology \
                 to force descending order — DO NOT assume the test still \
                 covers the H+ panic path."
            );

            // Subscribe-time activation drives d's first fire.
            // The user fn re-enters Core::emit on s_side mid-fire.
            // H+ check: descending order → PANIC.
            let _rec_d = rt.subscribe_recorder(d);
        }));

        // Capture the panic. Verify the diagnostic text is the H+
        // ascending-order message (not some other panic).
        let payload = result.expect_err(
            "expected the H+ ascending-order check to panic when a user fn \
             does a cross-partition emit DESCENDING during fire — instead the \
             closure completed without panic. H+ enforcement regression?",
        );
        let msg: &str = payload
            .downcast_ref::<&'static str>()
            .copied()
            .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("(non-string panic payload)");
        assert!(
            msg.contains("Phase H+ ascending-order violation"),
            "expected H+ ascending-order panic; got: {msg}"
        );

        // /qa A3: post-panic thread-local cleanup verification.
        // Cargo reuses worker threads across tests; a leak here
        // would propagate to the next test on this thread.
        let held = graphrefly_core::held_snapshot_for_tests();
        assert!(
            held.is_empty(),
            "Phase H+ thread-local `HELD` is dirty post-panic: {held:?}. \
             The scope-guard in `partition_wave_owner_lock_arc` (/qa A1) \
             must release the refcount on every exit path including \
             panic unwinds; a non-empty held set here means a leak."
        );
    }

    // -------------------------------------------------------------------
    // Finding 10 (HIGH): panic-path handle discard
    // -------------------------------------------------------------------

    #[test]
    fn panic_in_derived_discards_deferred_handles_without_leaking() {
        // A derived fn defers an emit via `emit_or_defer`, then panics.
        // BatchGuard::drop's panic-discard path must release the deferred
        // handle (not fire it) and leave the held_partitions thread-local
        // clean.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let rt = TestRuntime::new();
            let s_target = rt.state(Some(TestValue::Int(0)));
            let s_trigger = rt.state(None);

            let p_target = rt.core.partition_of(s_target.id).expect("registered");
            let p_trigger = rt.core.partition_of(s_trigger.id).expect("registered");
            assert!(
                p_trigger.raw() > p_target.raw(),
                "fixture invariant: partition(s_trigger) > partition(s_target). \
                 Got p_trigger={p_trigger:?}, p_target={p_target:?}"
            );

            let core_for_fn = rt.core.clone();
            let binding_for_fn = rt.binding.clone();
            let s_target_id = s_target.id;
            let _d = rt.derived(&[s_trigger.id], move |deps| {
                if let TestValue::Int(_n) = deps[0] {
                    // Defer an emit on s_target (lower partition).
                    let h = binding_for_fn.intern(TestValue::Int(999));
                    core_for_fn.emit_or_defer(s_target_id, h);
                    // Now panic — the deferred emit handle must be released
                    // without firing during BatchGuard's panic-discard.
                    panic!("intentional panic for discard test");
                }
                Some(deps[0].clone())
            });

            let _rec = rt.subscribe_recorder(_d);

            // Trigger: this will fire d's fn which defers then panics.
            let h = rt.binding.intern(TestValue::Int(1));
            rt.core.emit(s_trigger.id, h);
        }));

        // The panic from the derived fn should propagate.
        assert!(
            result.is_err(),
            "expected panic from derived fn, but closure completed normally"
        );

        // Post-panic: held_partitions must be clean (no leaked partition
        // refcounts). Cargo reuses threads — a leak would corrupt the
        // next test on this thread.
        let held = graphrefly_core::held_snapshot_for_tests();
        assert!(
            held.is_empty(),
            "held_partitions dirty after panic-discard: {held:?}"
        );

        // The deferred handle (value 999) was released without firing.
        // s_target's cache should still be the initial value (0), not 999.
        // We can't inspect s_target after catch_unwind (rt was dropped
        // inside), so the held_partitions check + no-panic on this thread
        // is the primary assertion. The handle release is exercised by
        // BatchGuard::drop's explicit release_handle loop on discarded ops.
    }

    // -------------------------------------------------------------------
    // Finding 11 (MEDIUM): nested/chained deferred ops
    // -------------------------------------------------------------------

    #[test]
    fn chained_deferred_ops_drain_multi_iteration() {
        // When processing one deferred op queues another, the drain loop
        // must iterate more than once to process all of them. This test
        // sets up a chain: deferred callback A pushes deferred emit B.
        let rt = TestRuntime::new();
        let s_target = rt.state(Some(TestValue::Int(0)));
        let s_trigger = rt.state(None);

        let p_target = rt.core.partition_of(s_target.id).expect("registered");
        let p_trigger = rt.core.partition_of(s_trigger.id).expect("registered");
        assert!(
            p_trigger.raw() > p_target.raw(),
            "fixture invariant: partition(s_trigger) > partition(s_target)"
        );

        let core_for_fn = rt.core.clone();
        let binding_for_fn = rt.binding.clone();
        let s_target_id = s_target.id;
        let d = rt.derived(&[s_trigger.id], move |deps| {
            if let TestValue::Int(_n) = deps[0] {
                // Push a Callback that itself pushes a deferred Emit.
                // This exercises the multi-iteration drain loop.
                let core_cb = core_for_fn.clone();
                let binding_cb = binding_for_fn.clone();
                core_for_fn.push_deferred_producer_op(
                    graphrefly_core::DeferredProducerOp::Callback(Box::new(move || {
                        // This callback runs during drain iteration 1.
                        // It pushes a new deferred emit — which must be
                        // picked up in drain iteration 2.
                        let h = binding_cb.intern(TestValue::Int(777));
                        core_cb.emit_or_defer(s_target_id, h);
                    })),
                );
            }
            Some(deps[0].clone())
        });

        let _rec_d = rt.subscribe_recorder(d);
        let rec_target = rt.subscribe_recorder(s_target.id);

        // Trigger the chain.
        let h = rt.binding.intern(TestValue::Int(1));
        rt.core.emit(s_trigger.id, h);

        // The chained deferred emit (777) should have landed on s_target.
        let target_data = rec_target.data_values();
        assert!(
            target_data.contains(&TestValue::Int(777)),
            "chained deferred op (emit 777 from callback) should drain; \
             got target data: {target_data:?}"
        );
    }

    // -------------------------------------------------------------------
    // Finding 12 (MEDIUM): drain from try_subscribe path
    // -------------------------------------------------------------------

    #[test]
    fn subscribe_triggered_drain_of_deferred_ops() {
        // The try_subscribe path has its own drain call site (after
        // dropping wave_guard). This test exercises it by having a
        // derived node whose subscribe-time activation pushes a deferred
        // op, which is then drained by the subscribe path (not batch).
        let rt = TestRuntime::new();
        let s_low = rt.state(Some(TestValue::Int(100)));
        let s_trigger = rt.state(Some(TestValue::Int(0)));

        let p_low = rt.core.partition_of(s_low.id).expect("registered");
        let p_trigger = rt.core.partition_of(s_trigger.id).expect("registered");
        assert!(
            p_trigger.raw() > p_low.raw(),
            "fixture invariant: partition(s_trigger) > partition(s_low)"
        );

        let core_for_fn = rt.core.clone();
        let binding_for_fn = rt.binding.clone();
        let s_low_id = s_low.id;
        // d depends on s_trigger. When d activates (via subscribe), it
        // fires its fn which pushes a deferred emit on s_low.
        let d = rt.derived(&[s_trigger.id], move |deps| {
            if let TestValue::Int(n) = deps[0] {
                // Defer an emit on s_low (lower partition).
                let h = binding_for_fn.intern(TestValue::Int(n + 500));
                core_for_fn.emit_or_defer(s_low_id, h);
            }
            Some(deps[0].clone())
        });

        let rec_low = rt.subscribe_recorder(s_low.id);

        // Subscribe to d — this is the subscribe path (not batch).
        // d's fn fires during subscribe activation, pushes a deferred
        // emit on s_low. The subscribe path's drain_deferred_producer_ops
        // should fire it.
        let _rec_d = rt.subscribe_recorder(d);

        // The deferred emit (0 + 500 = 500) should have landed on s_low.
        let low_data = rec_low.data_values();
        assert!(
            low_data.contains(&TestValue::Int(500)),
            "subscribe-path drain should fire deferred emit (500); \
             got s_low data: {low_data:?}"
        );
    }

    // -------------------------------------------------------------------
    // Finding 13 (LOW): mixed deferred op types FIFO order
    // -------------------------------------------------------------------

    #[test]
    fn mixed_deferred_op_types_preserve_fifo_order() {
        // Verifies FIFO ordering across mixed deferred op types:
        // emit, then complete. The emit must land before the complete.
        let rt = TestRuntime::new();
        let s_target = rt.state(Some(TestValue::Int(0)));
        let s_trigger = rt.state(None);

        let p_target = rt.core.partition_of(s_target.id).expect("registered");
        let p_trigger = rt.core.partition_of(s_trigger.id).expect("registered");
        assert!(
            p_trigger.raw() > p_target.raw(),
            "fixture invariant: partition(s_trigger) > partition(s_target)"
        );

        let core_for_fn = rt.core.clone();
        let binding_for_fn = rt.binding.clone();
        let s_target_id = s_target.id;
        let d = rt.derived(&[s_trigger.id], move |deps| {
            if let TestValue::Int(_n) = deps[0] {
                // Defer an emit followed by a complete on s_target.
                // FIFO order: emit must fire before complete.
                let h = binding_for_fn.intern(TestValue::Int(42));
                core_for_fn.emit_or_defer(s_target_id, h);
                core_for_fn.complete_or_defer(s_target_id);
            }
            Some(deps[0].clone())
        });

        let _rec_d = rt.subscribe_recorder(d);
        let rec_target = rt.subscribe_recorder(s_target.id);

        // Trigger: fires d's fn which defers emit + complete on s_target.
        let h = rt.binding.intern(TestValue::Int(1));
        rt.core.emit(s_trigger.id, h);

        // Verify: s_target received Data(42) THEN Complete, in that order.
        let events = rec_target.snapshot();
        let data_idx = events
            .iter()
            .position(|e| matches!(e, super::common::RecordedEvent::Data(TestValue::Int(42))));
        let complete_idx = events
            .iter()
            .position(|e| matches!(e, super::common::RecordedEvent::Complete));

        assert!(
            data_idx.is_some(),
            "s_target should receive Data(42) from deferred emit; events: {events:?}"
        );
        assert!(
            complete_idx.is_some(),
            "s_target should receive Complete from deferred complete; events: {events:?}"
        );
        assert!(
            data_idx.unwrap() < complete_idx.unwrap(),
            "FIFO order violation: Data(42) at index {:?} should precede \
             Complete at index {:?}; events: {events:?}",
            data_idx,
            complete_idx
        );
    }

    /// Regression: two threads emitting on DISJOINT partitions of one
    /// Core concurrently must EACH fully drain their own wave — every
    /// emitted value is delivered to that partition's subscriber, and no
    /// payload retains leak.
    ///
    /// Pins the `in_tick` re-keying (Core-global → per-(Core, thread)).
    /// Under the old Core-global flag, whichever thread entered second
    /// observed the first thread's `in_tick=true`, wrongly treated its
    /// independent disjoint wave as nested, and no-op'd its drop — so its
    /// subscriber silently missed Data and its per-emit payload retains
    /// leaked (caught late by `wave_state_clear_outermost`). This asserts
    /// the *observable* correctness directly (delivery + bounded handle
    /// count), independent of the debug-assert.
    #[test]
    fn disjoint_partition_concurrent_emits_each_drain_and_deliver() {
        const N: i64 = 300;
        let rt = TestRuntime::new();
        // Sentinel initial ⇒ no handshake Data; the derived's first-run
        // gate opens on the first real emit, so the delivered sequence is
        // exactly the emitted values 0..N (no initial-value prefix).
        let s_a = rt.state(None);
        let s_b = rt.state(None);

        // No deps between them ⇒ disjoint partitions (pins the premise).
        let p_a = rt.core.partition_of(s_a.id).expect("registered");
        let p_b = rt.core.partition_of(s_b.id).expect("registered");
        assert_ne!(p_a, p_b, "s_a and s_b must be in disjoint partitions");

        // A subscribed derived per state node ⇒ each wave has real drain
        // work (a sink to deliver to); identity passthrough of the value.
        let d_a = rt.derived(&[s_a.id], |deps| Some(deps[0].clone()));
        let d_b = rt.derived(&[s_b.id], |deps| Some(deps[0].clone()));
        let rec_a = rt.subscribe_recorder(d_a);
        let rec_b = rt.subscribe_recorder(d_b);

        let baseline_handles = rt.binding.live_handles();

        let start = Arc::new(std::sync::Barrier::new(2));
        let mk = |sh: super::common::StateHandle, b: Arc<std::sync::Barrier>| {
            thread::spawn(move || {
                b.wait();
                for i in 0..N {
                    sh.set(TestValue::Int(i));
                }
            })
        };
        let t_a = mk(s_a, start.clone());
        let t_b = mk(s_b, start.clone());
        join_with_timeout(t_a, 10, "thread A");
        join_with_timeout(t_b, 10, "thread B");

        let want: Vec<TestValue> = (0..N).map(TestValue::Int).collect();
        assert_eq!(
            rec_a.data_values(),
            want,
            "thread A's disjoint wave must deliver every value (got {} of {N})",
            rec_a.data_values().len()
        );
        assert_eq!(
            rec_b.data_values(),
            want,
            "thread B's disjoint wave must deliver every value (got {} of {N})",
            rec_b.data_values().len()
        );

        // Leak guard: per-emit payload retains are released by each
        // wave's drain. Live handle count must NOT scale with N — under
        // the bug the skipped drains leaked ~N retains per leaked thread.
        let after = rt.binding.live_handles();
        assert!(
            after <= baseline_handles + 8,
            "payload retains leaked: live_handles {after} vs baseline \
             {baseline_handles} after {N} emits/thread (skipped drain?)"
        );
    }
}

// ---------------------------------------------------------------------------
// Loom — abstract model of the cross-partition lock-acquisition protocol
// ---------------------------------------------------------------------------
//
// Per D096 the loom test uses default model parameters; users wanting
// exhaustive local runs can override via the standard loom env vars
// (`LOOM_MAX_THREADS`, `LOOM_MAX_BRANCHES`, `LOOM_MAX_PREEMPTIONS`).
// CI invokes:
//
//   RUSTFLAGS="--cfg loom" cargo test --test per_subgraph_parallelism
//
// Outside `--cfg loom` the bodies compile to nothing (gated below).
//
// Modeling discipline mirrors `tests/loom_subscription.rs` (D042): we
// model the protocol-relevant slice (per-partition lock + ascending-
// `SubgraphId` acquisition) directly with `loom::sync::Mutex`, not the
// production dispatcher's mutex/Arc primitives. Keeps loom's blast
// radius minimal and exercises the protocol invariant exhaustively
// across all interleavings.
//
// **Abstract-model boundary** (intentional limitations):
//
// 1. `loom::sync::Mutex` is non-reentrant; the real production type is
//    `parking_lot::ReentrantMutex<()>`. Same-thread re-entry into a
//    held partition (which the real impl supports for nested wave
//    re-entry) is NOT verified at the model level. parking_lot's
//    library guarantees + the `tests/lock_released.rs` std-thread
//    tests cover that surface.
//
// 2. `Subscription::Drop` in the current Rust impl does NOT acquire
//    a partition `wave_owner` (`crates/graphrefly-core/src/node.rs`
//    `impl Drop for Subscription` only acquires the state mutex).
//    Earlier drafts of these tests modeled a hypothetical
//    drop-cascade-acquires path; we do not — the loom tests verify
//    cross-partition `begin_batch_for`-shape acquires only. If the
//    Q5 scope item lands (Drop walks upstream and acquires
//    per-upstream-partition wave_owners), this section will need a
//    new test for that path.

#[cfg(loom)]
mod loom_tests {
    use loom::sync::atomic::{AtomicUsize, Ordering};
    use loom::sync::{Arc, Mutex};

    /// Abstract per-partition lock box. The real `SubgraphLockBox`
    /// holds a `parking_lot::ReentrantMutex<()>`; here we use
    /// `loom::sync::Mutex<()>` so loom can permute interleavings
    /// across all schedules. See module-level comment for the
    /// reentrance-modeling boundary.
    struct PartitionLock {
        /// Identifier used for ascending-order acquisition. The real
        /// type is `SubgraphId(u64)`; usize is sufficient for the model.
        id: usize,
        guard: Mutex<()>,
    }

    /// Acquire two partition locks in ascending-id order. Mirrors
    /// `Core::begin_batch_for`'s acquire-all-upfront contract (Q7 + D092):
    /// regardless of which arg-order the caller passed, the function
    /// sorts by id and acquires low-then-high. This invariant is what
    /// rules out AB/BA cycles when reciprocal callers exist.
    ///
    /// Returns owned guards — caller drops them to release.
    fn acquire_two_in_ascending_order<'a>(
        a: &'a Arc<PartitionLock>,
        b: &'a Arc<PartitionLock>,
    ) -> (
        loom::sync::MutexGuard<'a, ()>,
        loom::sync::MutexGuard<'a, ()>,
    ) {
        // Sort by id; acquire low first, then high.
        let (low, high) = if a.id <= b.id { (a, b) } else { (b, a) };
        let g_low = low.guard.lock().unwrap();
        let g_high = high.guard.lock().unwrap();
        (g_low, g_high)
    }

    #[test]
    fn cross_partition_reciprocal_arg_order_acquire_no_deadlock() {
        // Phase H Q4 + Q6 item 4 — verifies that
        // `acquire_two_in_ascending_order` produces the same
        // acquisition order regardless of caller arg order. Two
        // threads call with reciprocal arg orders:
        //   - Thread A: `acquire_two(&x, &y)` — `x.id < y.id`, so the
        //     sort branch `a.id <= b.id` taken; acquires `x` then `y`.
        //   - Thread B: `acquire_two(&y, &x)` — `y.id > x.id`, so the
        //     sort branch `a.id > b.id` taken; STILL acquires `x` then
        //     `y` (the OTHER branch of the sort).
        //
        // Both threads end up acquiring `{X, Y}` in low-then-high
        // order. AB/BA cycle is structurally ruled out — there's no
        // schedule in which Thread A holds X waiting for Y while
        // Thread B holds Y waiting for X, because both sort first.
        //
        // Without the sort, the caller arg order would propagate to
        // the lock acquisition order; loom would find a deadlock
        // schedule on roughly half the interleavings.
        //
        // This is the SMALL-IDS variant (1, 2). The companion test
        // `reciprocal_cross_partition_acquisitions_no_deadlock` runs
        // the same shape with non-adjacent ids (7, 13) to exercise
        // the sort against larger gaps.
        loom::model(|| {
            let part_x = Arc::new(PartitionLock {
                id: 1,
                guard: Mutex::new(()),
            });
            let part_y = Arc::new(PartitionLock {
                id: 2,
                guard: Mutex::new(()),
            });

            let acquired_x = Arc::new(AtomicUsize::new(0));
            let acquired_y = Arc::new(AtomicUsize::new(0));

            let x_for_a = part_x.clone();
            let y_for_a = part_y.clone();
            let acq_x_a = acquired_x.clone();
            let acq_y_a = acquired_y.clone();
            // Thread A: normal arg order (x first, y second).
            // The sort takes the `a.id <= b.id` branch.
            let h1 = loom::thread::spawn(move || {
                let (gx, gy) = acquire_two_in_ascending_order(&x_for_a, &y_for_a);
                acq_x_a.fetch_add(1, Ordering::SeqCst);
                acq_y_a.fetch_add(1, Ordering::SeqCst);
                // Drop in reverse acquisition order (LIFO — high first).
                drop(gy);
                drop(gx);
            });

            let x_for_b = part_x.clone();
            let y_for_b = part_y.clone();
            let acq_x_b = acquired_x.clone();
            let acq_y_b = acquired_y.clone();
            // Thread B: REVERSE arg order (y first, x second). The sort
            // must take the `a.id > b.id` branch. Verifies the
            // OTHER branch of the sort is reachable AND produces the
            // same lock acquisition order.
            let h2 = loom::thread::spawn(move || {
                let (gx, gy) = acquire_two_in_ascending_order(&y_for_b, &x_for_b);
                acq_x_b.fetch_add(1, Ordering::SeqCst);
                acq_y_b.fetch_add(1, Ordering::SeqCst);
                drop(gy);
                drop(gx);
            });

            h1.join().unwrap();
            h2.join().unwrap();

            // Both threads acquired both partitions on every schedule.
            let x_count = acquired_x.load(Ordering::SeqCst);
            let y_count = acquired_y.load(Ordering::SeqCst);
            assert_eq!(x_count, 2, "partition X acquired by both threads");
            assert_eq!(y_count, 2, "partition Y acquired by both threads");
        });
    }

    #[test]
    fn reciprocal_cross_partition_acquisitions_no_deadlock() {
        // Companion to `cross_partition_reciprocal_arg_order_...`
        // above, with non-adjacent ids (7, 13) to exercise the sort
        // against gaps. Same reciprocal-arg-order setup: Thread A
        // calls with `(x, y)`, Thread B calls with `(y, x)`. Both
        // sort to acquire X then Y; no AB/BA cycle.
        //
        // Compare to a buggy implementation that locked from-the-seed-
        // outward (without sorting): it would cycle here on roughly
        // half the interleavings.
        loom::model(|| {
            let part_x = Arc::new(PartitionLock {
                id: 7,
                guard: Mutex::new(()),
            });
            let part_y = Arc::new(PartitionLock {
                id: 13,
                guard: Mutex::new(()),
            });

            let x_a = part_x.clone();
            let y_a = part_y.clone();
            // Thread A: normal arg order.
            let h1 = loom::thread::spawn(move || {
                let (gl, gh) = acquire_two_in_ascending_order(&x_a, &y_a);
                // LIFO: drop high (acquired second) first, then low.
                drop(gh);
                drop(gl);
            });

            let x_b = part_x.clone();
            let y_b = part_y.clone();
            // Thread B: REVERSE arg order — exercises the OTHER branch
            // of the sort. The `compute_touched_partitions` sort makes
            // this transparent at the protocol level.
            let h2 = loom::thread::spawn(move || {
                let (gl, gh) = acquire_two_in_ascending_order(&y_b, &x_b);
                drop(gh);
                drop(gl);
            });

            h1.join().unwrap();
            h2.join().unwrap();
            // Reaching this line on every schedule = deadlock-free.
        });
    }
}
