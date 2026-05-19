//! Actor-model scaling bench (S4 rebuild — D238/D246).
//!
//! The pre-actor-model `group_scaling` drove N threads on ONE shared
//! `Core` (cloned across threads) with disjoint `SchedulingGroupId`s,
//! asking "do disjoint groups run in parallel?". D221/D246 made `Core`
//! move-only `!Send + !Sync` and S2c deleted the §7 group-lock
//! machinery, so that premise is structurally gone.
//!
//! Under the actor model, cross-`Core` parallelism is **host-native via
//! independent per-worker `Core`s**. This bench measures exactly that:
//! the same fixed per-worker emit+cascade workload, run on `W ∈
//! {1,2,4}` worker threads each owning its OWN `Core` (constructed
//! inside the worker — nothing crosses the thread boundary). Ideal
//! scaling is near-flat wall-time as `W` grows (the work is fully
//! independent — no shared Core, no shared lock), which is the
//! actor-model thesis the deleted shared-Core bench could never show
//! honestly. The shipped per-binding group executor (M6) is what wires
//! these independent Cores to a host pool; this bench is the substrate
//! ceiling it targets.

use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use graphrefly_core::{
    BindingBoundary, Core, DepBatch, FnId, FnResult, HandleId, Message, NodeId, Sink,
};

const EMITS_PER_WORKER: u64 = 50_000;

/// Minimal monotonic-handle binding (no dedup — every fire is a new
/// handle, so the cascade actually re-fires each emit).
struct WorkBinding {
    next: AtomicU64,
}
impl WorkBinding {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            next: AtomicU64::new(1),
        })
    }
}
impl BindingBoundary for WorkBinding {
    fn invoke_fn(&self, _: NodeId, _: FnId, _: &[DepBatch]) -> FnResult {
        FnResult::Data {
            handle: HandleId::new(self.next.fetch_add(1, Ordering::Relaxed)),
            tracked: None,
        }
    }
    fn custom_equals(&self, _: FnId, _: HandleId, _: HandleId) -> bool {
        false
    }
    fn release_handle(&self, _: HandleId) {}
    fn retain_handle(&self, _: HandleId) {}
}

fn noop_sink() -> Sink {
    Arc::new(|_: &[Message]| {})
}

/// One worker: build its OWN `Core`, a `state → derived` cascade, then
/// emit `EMITS_PER_WORKER` distinct handles. Returns nothing — the
/// point is the wall time under independent concurrent Cores.
fn worker() {
    let binding = WorkBinding::new();
    let core = Core::new(binding.clone() as Arc<dyn BindingBoundary>);
    let s = core.register_state(HandleId::new(0), false).unwrap();
    // A trivial derived so each emit drives a real 2-node wave. The
    // minimal binding's `invoke_fn` ignores `fn_id`, so any id works.
    let d = core
        .register_derived(
            &[s],
            FnId::new(1),
            graphrefly_core::EqualsMode::Identity,
            false,
        )
        .unwrap();
    let _sub = core.subscribe(d, noop_sink());
    for k in 1..=EMITS_PER_WORKER {
        core.emit(black_box(s), black_box(HandleId::new(k + 1)));
    }
    core.drain_mailbox();
}

fn group_scaling(c: &mut Criterion) {
    let mut g = c.benchmark_group("group_scaling/independent_cores");
    for workers in [1usize, 2, 4] {
        g.throughput(criterion::Throughput::Elements(
            EMITS_PER_WORKER * workers as u64,
        ));
        g.bench_with_input(
            BenchmarkId::from_parameter(workers),
            &workers,
            |b, &workers| {
                b.iter(|| {
                    let handles: Vec<_> = (0..workers).map(|_| thread::spawn(worker)).collect();
                    for h in handles {
                        h.join().expect("worker Core completed");
                    }
                });
            },
        );
    }
    g.finish();
}

criterion_group!(benches, group_scaling);
criterion_main!(benches);
