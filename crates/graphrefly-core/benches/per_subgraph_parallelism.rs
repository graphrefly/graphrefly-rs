//! Phase J — multi-thread parallel-emit microbenchmarks for the
//! Slice Y1 / D3 per-subgraph parallelism win.
//!
//! Quantifies "by how much" the per-partition `wave_owner` mutex
//! delivers wall-clock parallelism for cross-thread emits on
//! DISJOINT partitions, vs the same-partition counter-case where
//! emits SHOULD serialize.
//!
//! # Bench shapes
//!
//! - `serial_baseline_2N_emits` — single thread emits `2 * M` times
//!   on a single state node. Establishes a "what does this much
//!   wave-engine work cost serially?" baseline.
//! - `parallel_2t_disjoint_M_each` — 2 threads, each emits `M`
//!   times on its OWN state node in a disjoint partition. Total
//!   work `= 2 * M` emits, same as serial baseline. Wall-clock
//!   should approach `serial / 2` if per-partition `wave_owner`
//!   delivers real parallelism (modulo spawn overhead +
//!   `state` mutex contention during `pending_notify` queueing).
//! - `parallel_2t_same_partition_M_each` — 2 threads, each emits
//!   `M` times on the SAME state node. The threads serialize on
//!   the partition's `wave_owner.lock_arc()`. Wall-clock should
//!   approach `serial` (no parallelism gain — confirms the
//!   per-partition mutex actually serializes same-partition work).
//!
//! # Hypothesis
//!
//! `parallel_2t_disjoint_M_each` ≈ `serial / 2` (with overhead);
//! `parallel_2t_same_partition_M_each` ≈ `serial` (no gain). The
//! ratio `serial / disjoint` quantifies the per-partition
//! parallelism speedup at 2 threads — Y1's user-facing win.
//!
//! Run via:
//!   `cargo bench -p graphrefly-core --bench per_subgraph_parallelism`

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use graphrefly_core::{
    BindingBoundary, Core, DepBatch, EqualsMode, FnId, FnResult, HandleId, Message, NodeId, Sink,
};

/// Atomics-only bench binding — avoids the `Mutex` contention that
/// the dispatcher.rs `BenchBinding` introduces (its `next_handle`
/// and `invoke_count` are `Mutex<u64>`s, which serialize cross-
/// thread emits at the binding layer and would mask Core's
/// per-partition parallelism). Here `next_handle` is atomic and
/// `release_handle` / `retain_handle` are no-ops; `invoke_fn` is
/// not exercised by these state-node benches.
struct ParallelBenchBinding {
    next_handle: AtomicU64,
}

impl ParallelBenchBinding {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            next_handle: AtomicU64::new(1),
        })
    }
}

impl BindingBoundary for ParallelBenchBinding {
    fn invoke_fn(&self, _: NodeId, _: FnId, _dep_data: &[DepBatch]) -> FnResult {
        // Not exercised by state-node-only benches; defensive default.
        FnResult::Data {
            handle: HandleId::new(self.next_handle.fetch_add(1, Ordering::Relaxed)),
            tracked: None,
        }
    }

    fn custom_equals(&self, _: FnId, a: HandleId, b: HandleId) -> bool {
        a == b
    }

    fn release_handle(&self, _: HandleId) {}

    fn retain_handle(&self, _: HandleId) {}
}

fn noop_sink() -> Sink {
    Arc::new(|_msgs: &[Message]| {})
}

/// Emits per thread per bench iteration. Tuned to amortize thread-
/// spawn overhead (~10–50µs/thread on Linux/macOS) while keeping
/// total iteration time around 1–10ms so criterion can collect
/// adequate samples without excess wall-clock.
const M_EMITS_PER_THREAD: usize = 4_000;

// ---------------------------------------------------------------------------
// 1) Serial baseline: single thread emits 2 * M times on a single state node.
// ---------------------------------------------------------------------------

fn bench_serial_baseline(c: &mut Criterion) {
    let mut group = c.benchmark_group("y1_per_subgraph_parallelism");
    group.throughput(Throughput::Elements((2 * M_EMITS_PER_THREAD) as u64));

    let binding = ParallelBenchBinding::new();
    let core = Core::new(binding.clone());
    let s = core.register_state(HandleId::new(1), false).unwrap();
    std::mem::forget(core.subscribe(s, noop_sink()));
    // Pre-allocate the handle to emit (same handle each iteration —
    // exercises the equals-substitution dedup hot path; no per-emit
    // binding allocation).
    let h = HandleId::new(1);

    group.bench_function(
        BenchmarkId::new("serial_baseline_2N_emits", 2 * M_EMITS_PER_THREAD),
        |b| {
            b.iter(|| {
                for _ in 0..(2 * M_EMITS_PER_THREAD) {
                    core.emit(s, h);
                }
            });
        },
    );

    group.finish();
}

// ---------------------------------------------------------------------------
// 2) Parallel disjoint-partition: 2 threads, each on its own state node in a
//    disjoint partition. Wall-clock should approach serial / 2 if per-partition
//    `wave_owner` delivers real parallelism.
// ---------------------------------------------------------------------------

fn bench_parallel_disjoint(c: &mut Criterion) {
    let mut group = c.benchmark_group("y1_per_subgraph_parallelism");
    group.throughput(Throughput::Elements((2 * M_EMITS_PER_THREAD) as u64));

    let binding = ParallelBenchBinding::new();
    let core = Core::new(binding.clone());
    // Two disjoint state nodes — no dep edges between them, so union-find
    // keeps them in distinct partitions.
    let s_a = core.register_state(HandleId::new(1), false).unwrap();
    let s_b = core.register_state(HandleId::new(2), false).unwrap();
    std::mem::forget(core.subscribe(s_a, noop_sink()));
    std::mem::forget(core.subscribe(s_b, noop_sink()));

    // Sanity: pin the parallelism premise — disjoint partitions.
    assert_ne!(
        core.partition_of(s_a),
        core.partition_of(s_b),
        "bench premise: s_a and s_b must be in disjoint partitions"
    );

    let h_a = HandleId::new(1);
    let h_b = HandleId::new(2);

    group.bench_function(
        BenchmarkId::new("parallel_2t_disjoint_M_each", M_EMITS_PER_THREAD),
        |b| {
            b.iter(|| {
                let core_a = core.clone();
                let core_b = core.clone();
                let t_a = thread::spawn(move || {
                    for _ in 0..M_EMITS_PER_THREAD {
                        core_a.emit(s_a, h_a);
                    }
                });
                let t_b = thread::spawn(move || {
                    for _ in 0..M_EMITS_PER_THREAD {
                        core_b.emit(s_b, h_b);
                    }
                });
                t_a.join().unwrap();
                t_b.join().unwrap();
            });
        },
    );

    group.finish();
}

// ---------------------------------------------------------------------------
// 3) Parallel same-partition: 2 threads, each on the SAME state node.
//    Both acquire the partition's `wave_owner` — they serialize on the lock.
//    Wall-clock should approach serial baseline (no parallelism gain).
// ---------------------------------------------------------------------------

fn bench_parallel_same_partition(c: &mut Criterion) {
    let mut group = c.benchmark_group("y1_per_subgraph_parallelism");
    group.throughput(Throughput::Elements((2 * M_EMITS_PER_THREAD) as u64));

    let binding = ParallelBenchBinding::new();
    let core = Core::new(binding.clone());
    let s = core.register_state(HandleId::new(1), false).unwrap();
    std::mem::forget(core.subscribe(s, noop_sink()));

    let h = HandleId::new(1);

    group.bench_function(
        BenchmarkId::new("parallel_2t_same_partition_M_each", M_EMITS_PER_THREAD),
        |b| {
            b.iter(|| {
                let core_a = core.clone();
                let core_b = core.clone();
                let t_a = thread::spawn(move || {
                    for _ in 0..M_EMITS_PER_THREAD {
                        core_a.emit(s, h);
                    }
                });
                let t_b = thread::spawn(move || {
                    for _ in 0..M_EMITS_PER_THREAD {
                        core_b.emit(s, h);
                    }
                });
                t_a.join().unwrap();
                t_b.join().unwrap();
            });
        },
    );

    group.finish();
}

// ---------------------------------------------------------------------------
// 4) Stress: 4 disjoint partitions, 4 threads. Speedup ceiling on a
//    multi-core machine; on a 2-core machine bound by physical cores.
// ---------------------------------------------------------------------------

fn bench_parallel_4t_disjoint(c: &mut Criterion) {
    let mut group = c.benchmark_group("y1_per_subgraph_parallelism");
    group.throughput(Throughput::Elements((4 * M_EMITS_PER_THREAD) as u64));

    let binding = ParallelBenchBinding::new();
    let core = Core::new(binding.clone());
    let states: Vec<NodeId> = (0..4)
        .map(|i| {
            let s = core
                .register_state(HandleId::new((i + 10) as u64), false)
                .unwrap();
            // Subscribe so emits actually drive a flush.
            std::mem::forget(core.subscribe(s, noop_sink()));
            s
        })
        .collect();

    // Sanity: 4 disjoint partitions.
    let p0 = core.partition_of(states[0]).unwrap();
    let p1 = core.partition_of(states[1]).unwrap();
    let p2 = core.partition_of(states[2]).unwrap();
    let p3 = core.partition_of(states[3]).unwrap();
    assert!(
        p0 != p1 && p0 != p2 && p0 != p3 && p1 != p2 && p1 != p3 && p2 != p3,
        "bench premise: 4 disjoint partitions"
    );

    let handles: Vec<HandleId> = (0..4).map(|i| HandleId::new((i + 10) as u64)).collect();

    group.bench_function(
        BenchmarkId::new("parallel_4t_disjoint_M_each", M_EMITS_PER_THREAD),
        |b| {
            b.iter(|| {
                let mut threads = Vec::with_capacity(4);
                for i in 0..4 {
                    let core_i = core.clone();
                    let s_i = states[i];
                    let h_i = handles[i];
                    threads.push(thread::spawn(move || {
                        for _ in 0..M_EMITS_PER_THREAD {
                            core_i.emit(s_i, h_i);
                        }
                    }));
                }
                for t in threads {
                    t.join().unwrap();
                }
            });
        },
    );

    group.finish();
}

// ---------------------------------------------------------------------------
// 5) Serial baseline at 4N — establishes the comparison baseline for the 4t
//    bench. Wall-clock at 4N emits, single-threaded.
// ---------------------------------------------------------------------------

fn bench_serial_baseline_4n(c: &mut Criterion) {
    let mut group = c.benchmark_group("y1_per_subgraph_parallelism");
    group.throughput(Throughput::Elements((4 * M_EMITS_PER_THREAD) as u64));

    let binding = ParallelBenchBinding::new();
    let core = Core::new(binding.clone());
    let s = core.register_state(HandleId::new(1), false).unwrap();
    std::mem::forget(core.subscribe(s, noop_sink()));
    let h = HandleId::new(1);

    group.bench_function(
        BenchmarkId::new("serial_baseline_4N_emits", 4 * M_EMITS_PER_THREAD),
        |b| {
            b.iter(|| {
                for _ in 0..(4 * M_EMITS_PER_THREAD) {
                    core.emit(s, h);
                }
            });
        },
    );

    group.finish();
}

// ---------------------------------------------------------------------------
// 6) FN-FIRE regime: each emit triggers a derived's fn fire that does
//    a fixed-iteration CPU spin (deterministic, no clock syscalls per
//    loop). Since `BindingBoundary::invoke_fn` fires LOCK-RELEASED
//    (Slice A close M1), the state mutex is dropped around the binding
//    callback — this is where per-partition `wave_owner` parallelism
//    SHOULD be observable, because cross-thread waves on disjoint
//    partitions can fire fn concurrently with state lock free.
//
//    **CRITICAL — handle freshness (QA-fix #1, 2026-05-09):** every emit
//    MUST produce a NEW state cache to actually reach the derived's fn.
//    Emitting the SAME handle twice triggers equals-substitution
//    (R1.3.2.d) at the state node — the wave converts to RESOLVED and
//    the derived's `invoke_fn` is NOT re-fired. The earlier
//    `WorkBinding` impl returned `HandleId::new(1)` on every fire and
//    the bench emitted `HandleId::new(1)` on every iteration; both
//    layers dedup'd → only ONE fn fire per `b.iter` despite the loop
//    iterations claiming N. The resulting numbers measured the
//    equals-coalesce hot path, not the fn-fire workload — i.e., they
//    were the same workload as the tight-state-emit benches with extra
//    cascade overhead. The fix: `binding.fresh()` per emit + per fn
//    return → cache always changes → DATA cascade fires the fn.
// ---------------------------------------------------------------------------

/// Fixed-iteration spin counter approximating ~3–6 µs of CPU work on
/// modern x86_64 / Apple-silicon. Calibrated coarsely; bench numbers
/// are relative comparisons across scenarios on the same machine, so
/// exact wall-clock per fire matters less than cross-bench parity.
/// Replaces an earlier `Instant::elapsed()`-driven spin (QA finding 9 —
/// per-iteration `clock_gettime` overhead dominated the spin and added
/// cross-thread `vDSO` clock-read contention).
const SPIN_ITERS_PER_FIRE: u32 = 2_000;

struct WorkBinding {
    next_handle: AtomicU64,
}

impl WorkBinding {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            // Start at 100 — leaves room for state-init handles (1, 2, …)
            // without colliding with the per-emit fresh-handle stream.
            next_handle: AtomicU64::new(100),
        })
    }

    #[inline]
    fn fresh(&self) -> HandleId {
        HandleId::new(self.next_handle.fetch_add(1, Ordering::Relaxed))
    }
}

impl BindingBoundary for WorkBinding {
    fn invoke_fn(&self, _: NodeId, _: FnId, _dep_data: &[DepBatch]) -> FnResult {
        // Calibrated counted spin — no `Instant::elapsed()` per iteration
        // (would dominate at sub-µs spin targets and add cross-thread
        // clock-syscall variance per QA finding 9). `black_box` prevents
        // the loop being optimized to a no-op.
        let mut x: u64 = 0;
        for _ in 0..SPIN_ITERS_PER_FIRE {
            x = std::hint::black_box(x.wrapping_add(1));
        }
        let _ = std::hint::black_box(x);
        // FRESH handle every fire — derived's downstream cache always
        // changes, so consumer-side equals-coalesce can't suppress the
        // fn re-fire either.
        FnResult::Data {
            handle: self.fresh(),
            tracked: None,
        }
    }

    fn custom_equals(&self, _: FnId, a: HandleId, b: HandleId) -> bool {
        a == b
    }

    fn release_handle(&self, _: HandleId) {}
    fn retain_handle(&self, _: HandleId) {}
}

const M_EMITS_FNFIRE: usize = 1_000;

fn bench_fnfire_serial(c: &mut Criterion) {
    let mut group = c.benchmark_group("y1_per_subgraph_parallelism_fnfire");
    group.throughput(Throughput::Elements((2 * M_EMITS_FNFIRE) as u64));

    let binding = WorkBinding::new();
    let core = Core::new(binding.clone());
    let s = core.register_state(HandleId::new(1), false).unwrap();
    let dummy_fn = FnId::new(0);
    let d = core
        .register_derived(&[s], dummy_fn, EqualsMode::Identity, false)
        .unwrap();
    std::mem::forget(core.subscribe(d, noop_sink()));

    group.bench_function(
        BenchmarkId::new("fnfire_serial_2N", 2 * M_EMITS_FNFIRE),
        |b| {
            b.iter(|| {
                for _ in 0..(2 * M_EMITS_FNFIRE) {
                    // Fresh handle per emit (QA-fix #1) — state cache
                    // changes → DATA cascade → derived's `invoke_fn`
                    // actually runs (the work we're measuring).
                    core.emit(s, binding.fresh());
                }
            });
        },
    );
    group.finish();
}

fn bench_fnfire_parallel_2t_disjoint(c: &mut Criterion) {
    let mut group = c.benchmark_group("y1_per_subgraph_parallelism_fnfire");
    group.throughput(Throughput::Elements((2 * M_EMITS_FNFIRE) as u64));

    let binding = WorkBinding::new();
    let core = Core::new(binding.clone());
    let s_a = core.register_state(HandleId::new(1), false).unwrap();
    let s_b = core.register_state(HandleId::new(2), false).unwrap();
    let dummy_fn = FnId::new(0);
    // Two derived nodes, each in its own partition (no dep edges between them).
    let d_a = core
        .register_derived(&[s_a], dummy_fn, EqualsMode::Identity, false)
        .unwrap();
    let d_b = core
        .register_derived(&[s_b], dummy_fn, EqualsMode::Identity, false)
        .unwrap();
    std::mem::forget(core.subscribe(d_a, noop_sink()));
    std::mem::forget(core.subscribe(d_b, noop_sink()));
    assert_ne!(
        core.partition_of(s_a),
        core.partition_of(s_b),
        "fnfire bench premise: disjoint partitions"
    );

    group.bench_function(
        BenchmarkId::new("fnfire_parallel_2t_disjoint", M_EMITS_FNFIRE),
        |b| {
            b.iter(|| {
                let core_a = core.clone();
                let core_b = core.clone();
                let binding_a = binding.clone();
                let binding_b = binding.clone();
                let t_a = thread::spawn(move || {
                    for _ in 0..M_EMITS_FNFIRE {
                        core_a.emit(s_a, binding_a.fresh());
                    }
                });
                let t_b = thread::spawn(move || {
                    for _ in 0..M_EMITS_FNFIRE {
                        core_b.emit(s_b, binding_b.fresh());
                    }
                });
                t_a.join().unwrap();
                t_b.join().unwrap();
            });
        },
    );
    group.finish();
}

fn bench_fnfire_parallel_2t_same_partition(c: &mut Criterion) {
    let mut group = c.benchmark_group("y1_per_subgraph_parallelism_fnfire");
    group.throughput(Throughput::Elements((2 * M_EMITS_FNFIRE) as u64));

    let binding = WorkBinding::new();
    let core = Core::new(binding.clone());
    let s = core.register_state(HandleId::new(1), false).unwrap();
    let dummy_fn = FnId::new(0);
    let d = core
        .register_derived(&[s], dummy_fn, EqualsMode::Identity, false)
        .unwrap();
    std::mem::forget(core.subscribe(d, noop_sink()));

    group.bench_function(
        BenchmarkId::new("fnfire_parallel_2t_same_partition", M_EMITS_FNFIRE),
        |b| {
            b.iter(|| {
                let core_a = core.clone();
                let core_b = core.clone();
                let binding_a = binding.clone();
                let binding_b = binding.clone();
                let t_a = thread::spawn(move || {
                    for _ in 0..M_EMITS_FNFIRE {
                        core_a.emit(s, binding_a.fresh());
                    }
                });
                let t_b = thread::spawn(move || {
                    for _ in 0..M_EMITS_FNFIRE {
                        core_b.emit(s, binding_b.fresh());
                    }
                });
                t_a.join().unwrap();
                t_b.join().unwrap();
            });
        },
    );
    group.finish();
}

criterion_group!(
    benches,
    bench_serial_baseline,
    bench_parallel_disjoint,
    bench_parallel_same_partition,
    bench_serial_baseline_4n,
    bench_parallel_4t_disjoint,
    bench_fnfire_serial,
    bench_fnfire_parallel_2t_disjoint,
    bench_fnfire_parallel_2t_same_partition,
);
criterion_main!(benches);
