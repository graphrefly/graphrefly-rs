//! §7 parallel-scaling bench (D216) — does a disjoint
//! `SerializationGroupId` set actually run in parallel?
//!
//! `tests/group_parallelism.rs` only asserts no-deadlock + delivery — it
//! would pass even if disjoint groups fully serialized. This bench is the
//! throughput instrument: N threads, each driving its OWN per-thread
//! graph on a SHARED `Core<LockedCell>` (the `Send + Sync` production
//! default, D211), measured as aggregate emits/sec while N scales.
//!
//! Three arms, same workload, differing only in group assignment:
//!
//! - `disjoint`   — thread `i`'s component gets `SerializationGroupId(i+1)`.
//!   Distinct wave-locks ⇒ should scale up with N (the parallelism the
//!   §7 machinery exists to buy).
//! - `same_group` — every thread's component shares `SerializationGroupId(1)`.
//!   One `ReentrantMutex` ⇒ should NOT scale (serialized control).
//! - `all_none`   — no groups. The QA-F1 (D213) Core-global wave
//!   `ReentrantMutex` serializes every wave on `LockedCell` ⇒ should NOT
//!   scale (the "unannotated production default" control).
//!
//! Reading the result: `disjoint` elements/sec should rise with N while
//! `same_group` / `all_none` stay flat (or regress on contention). If
//! `disjoint` is also flat, disjoint groups are NOT actually parallel and
//! the §7 parallelism thesis is unverified.
//!
//! Run:   `cargo bench -p graphrefly-core --bench group_scaling`
//! Quick: append `-- --quick`.

use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use graphrefly_core::{
    BindingBoundary, Core, DepBatch, EqualsMode, FnId, FnResult, HandleId, Message, NodeId,
    SerializationGroupId, Sink,
};
use parking_lot::Mutex;

struct BenchBinding {
    next_handle: Mutex<u64>,
}

impl BenchBinding {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            next_handle: Mutex::new(1),
        })
    }
    fn fresh_handle(&self) -> HandleId {
        let mut g = self.next_handle.lock();
        let h = HandleId::new(*g);
        *g += 1;
        h
    }
}

impl BindingBoundary for BenchBinding {
    fn invoke_fn(&self, _: NodeId, _: FnId, _: &[DepBatch]) -> FnResult {
        FnResult::Data {
            handle: self.fresh_handle(),
            tracked: None,
        }
    }
    fn custom_equals(&self, _: FnId, a: HandleId, b: HandleId) -> bool {
        a == b
    }
    fn release_handle(&self, _: HandleId) {}
}

fn noop_sink() -> Sink {
    Arc::new(|_m: &[Message]| {})
}

#[derive(Clone, Copy, PartialEq)]
enum Grouping {
    /// thread `i` → `SerializationGroupId(i + 1)` — disjoint locks.
    Disjoint,
    /// every thread → `SerializationGroupId(1)` — one shared lock.
    Same,
    /// no group — QA-F1 Core-global wave lock serializes all waves.
    None,
}

/// Build one per-thread graph on `core`: `s → d → noop sink`, with `s`'s
/// component assigned the grouping policy. Registration is
/// topology-mutation (serialized) so it runs on the main thread before
/// the workers spawn. Returns the seed state node + the keep-alive sub.
fn build_thread_graph(
    core: &Core,
    grouping: Grouping,
    thread_idx: usize,
) -> (NodeId, graphrefly_core::Subscription) {
    let s = core.register_state(HandleId::new(1), false).unwrap();
    let d = core
        .register_derived(&[s], FnId::new(0), EqualsMode::Identity, false)
        .unwrap();
    let sub = core.subscribe(d, noop_sink());
    match grouping {
        Grouping::Disjoint => core
            .set_serialization_group(s, Some(SerializationGroupId::new(thread_idx as u64 + 1)))
            .unwrap(),
        Grouping::Same => core
            .set_serialization_group(s, Some(SerializationGroupId::new(1)))
            .unwrap(),
        Grouping::None => {}
    }
    (s, sub)
}

/// One arm: scale N ∈ {1,2,4,8}; report aggregate emits/sec.
fn scaling_arm(c: &mut Criterion, arm: &str, grouping: Grouping) {
    let mut group = c.benchmark_group(format!("group_scaling/{arm}"));
    for &n in &[1_usize, 2, 4, 8] {
        // `Throughput::Elements(n)` ⇒ each criterion "iteration" accounts
        // for `n` parallel emits (one per thread); criterion's
        // elements/sec is then the aggregate cross-thread throughput, so
        // the figure to compare across N is "elem/s", and across arms at
        // the same N is the parallel-vs-serialized delta.
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n_threads| {
            b.iter_custom(|iters| {
                // Fresh Core + per-thread graphs per measurement call.
                // Build cost is O(n_threads) and amortized over
                // `iters * n_threads` emits (iters is large in the
                // measured phase).
                let binding = BenchBinding::new();
                let core = Core::new(binding.clone() as Arc<dyn BindingBoundary>);
                let mut seeds = Vec::with_capacity(n_threads);
                let mut subs = Vec::with_capacity(n_threads);
                for t in 0..n_threads {
                    let (s, sub) = build_thread_graph(&core, grouping, t);
                    seeds.push(s);
                    subs.push(sub);
                }

                let barrier = Arc::new(Barrier::new(n_threads + 1));
                let mut handles = Vec::with_capacity(n_threads);
                for &s in &seeds {
                    let core = core.clone();
                    let binding = binding.clone();
                    let barrier = barrier.clone();
                    handles.push(std::thread::spawn(move || {
                        barrier.wait();
                        for _ in 0..iters {
                            let h = binding.fresh_handle();
                            core.emit(black_box(s), black_box(h));
                        }
                    }));
                }

                barrier.wait();
                let start = Instant::now();
                for h in handles {
                    h.join().unwrap();
                }
                let elapsed = start.elapsed();
                // Keep subscriptions alive until after the workers join.
                drop(subs);
                // criterion divides by `iters`; we charged `n_threads`
                // elements per iter, so elem/s == aggregate throughput.
                elapsed.max(Duration::from_nanos(1))
            });
        });
    }
    group.finish();
}

fn group_scaling(c: &mut Criterion) {
    scaling_arm(c, "disjoint", Grouping::Disjoint);
    scaling_arm(c, "same_group", Grouping::Same);
    scaling_arm(c, "all_none", Grouping::None);
}

criterion_group!(benches, group_scaling);
criterion_main!(benches);
