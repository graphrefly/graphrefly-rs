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
use std::time::Duration;

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
    let _sub = core.subscribe(s, noop_sink());
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
    let _sub_a = core.subscribe(s_a, noop_sink());
    let _sub_b = core.subscribe(s_b, noop_sink());

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
    let _sub = core.subscribe(s, noop_sink());

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
    let _sub = core.subscribe(s, noop_sink());
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
//    `WORK_NS` of CPU work. Since `BindingBoundary::invoke_fn` fires
//    LOCK-RELEASED (Slice A close M1), the state mutex is dropped around
//    the binding callback — this is where per-partition `wave_owner`
//    parallelism SHOULD be observable, because cross-thread waves on
//    disjoint partitions can fire fn concurrently with state lock free.
//
//    Compare to the cheap state-emit benches above where state mutex
//    contention dominates and parallelism is invisible.
// ---------------------------------------------------------------------------

/// Spin-loop work per fn fire — simulates a non-trivial dependent
/// computation (lock-released; doesn't hold state). Tuned so single-
/// thread total is ~10ms at the M_EMITS_PER_THREAD scale.
const WORK_NS_PER_FIRE: u64 = 4_000;

struct WorkBinding {
    next_handle: AtomicU64,
}

impl WorkBinding {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            next_handle: AtomicU64::new(1),
        })
    }
}

impl BindingBoundary for WorkBinding {
    fn invoke_fn(&self, _: NodeId, _: FnId, _dep_data: &[DepBatch]) -> FnResult {
        // Spin-busy loop for WORK_NS_PER_FIRE nanoseconds. This stands
        // in for "real" CPU work that a user fn might do — pure-Rust
        // code that holds no Core lock (lock-released per Slice A).
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_nanos(WORK_NS_PER_FIRE) {
            std::hint::spin_loop();
        }
        // Same-handle return → equals-substitution dedup → RESOLVED tier.
        FnResult::Data {
            handle: HandleId::new(1),
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
    let _sub = core.subscribe(d, noop_sink());
    let h = HandleId::new(1);

    group.bench_function(
        BenchmarkId::new("fnfire_serial_2N", 2 * M_EMITS_FNFIRE),
        |b| {
            b.iter(|| {
                for _ in 0..(2 * M_EMITS_FNFIRE) {
                    core.emit(s, h);
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
    let _sub_a = core.subscribe(d_a, noop_sink());
    let _sub_b = core.subscribe(d_b, noop_sink());
    assert_ne!(
        core.partition_of(s_a),
        core.partition_of(s_b),
        "fnfire bench premise: disjoint partitions"
    );
    let h_a = HandleId::new(1);
    let h_b = HandleId::new(2);

    group.bench_function(
        BenchmarkId::new("fnfire_parallel_2t_disjoint", M_EMITS_FNFIRE),
        |b| {
            b.iter(|| {
                let core_a = core.clone();
                let core_b = core.clone();
                let t_a = thread::spawn(move || {
                    for _ in 0..M_EMITS_FNFIRE {
                        core_a.emit(s_a, h_a);
                    }
                });
                let t_b = thread::spawn(move || {
                    for _ in 0..M_EMITS_FNFIRE {
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
    let _sub = core.subscribe(d, noop_sink());
    let h = HandleId::new(1);

    group.bench_function(
        BenchmarkId::new("fnfire_parallel_2t_same_partition", M_EMITS_FNFIRE),
        |b| {
            b.iter(|| {
                let core_a = core.clone();
                let core_b = core.clone();
                let t_a = thread::spawn(move || {
                    for _ in 0..M_EMITS_FNFIRE {
                        core_a.emit(s, h);
                    }
                });
                let t_b = thread::spawn(move || {
                    for _ in 0..M_EMITS_FNFIRE {
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
