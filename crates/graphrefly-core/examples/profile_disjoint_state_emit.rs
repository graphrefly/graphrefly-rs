//! Profile target — 4-thread disjoint **state-emit** (Regime A): the
//! contention regime where `per_subgraph_parallelism` shows a 2.46x
//! REGRESSION vs serial (not a speedup). Mirrors
//! `benches/per_subgraph_parallelism.rs::bench_parallel_4t_disjoint`:
//! 4 state nodes in disjoint partitions, each with a direct noop sink,
//! 4 threads re-emitting a constant handle. No derived fn / no spin —
//! so `wave_owner` is NOT released around an `invoke_fn`, and the
//! global Core mutexes are on the hot path the whole wave.
//!
//! Run:
//!   cargo build --profile profiling --example profile_disjoint_state_emit -p graphrefly-core
//!   samply record --save-only -o /tmp/p.json.gz -- \
//!     target/profiling/examples/profile_disjoint_state_emit

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use graphrefly_core::{
    BindingBoundary, Core, DepBatch, FnId, FnResult, HandleId, Message, NodeId, Sink,
};

const EMITS_PER_THREAD: usize = 1_500_000;
const THREADS: usize = 4;

struct WorkBinding {
    next_handle: AtomicU64,
}
impl WorkBinding {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            next_handle: AtomicU64::new(100),
        })
    }
}
impl BindingBoundary for WorkBinding {
    fn invoke_fn(&self, _: NodeId, _: FnId, _: &[DepBatch]) -> FnResult {
        FnResult::Data {
            handle: HandleId::new(self.next_handle.fetch_add(1, Ordering::Relaxed)),
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

fn main() {
    let binding = WorkBinding::new();
    let core = Core::new(binding.clone());

    let states: Vec<NodeId> = (0..THREADS)
        .map(|i| {
            let s = core
                .register_state(HandleId::new((i + 10) as u64), false)
                .unwrap();
            std::mem::forget(core.subscribe(s, noop_sink()));
            s
        })
        .collect();

    // Premise: all disjoint partitions.
    for i in 0..THREADS {
        for j in (i + 1)..THREADS {
            assert_ne!(
                core.partition_of(states[i]),
                core.partition_of(states[j]),
                "premise: disjoint partitions"
            );
        }
    }

    let handles: Vec<HandleId> = (0..THREADS)
        .map(|i| HandleId::new((i + 10) as u64))
        .collect();

    let start = std::time::Instant::now();
    let mut ts = Vec::with_capacity(THREADS);
    for i in 0..THREADS {
        let core_i = core.clone();
        let s_i = states[i];
        let h_i = handles[i];
        ts.push(thread::spawn(move || {
            for _ in 0..EMITS_PER_THREAD {
                core_i.emit(s_i, h_i);
            }
        }));
    }
    for t in ts {
        t.join().unwrap();
    }
    let elapsed = start.elapsed();
    eprintln!(
        "state-emit profile complete: {} total emits, {:?} wall, {:.1} ns/emit",
        THREADS * EMITS_PER_THREAD,
        elapsed,
        elapsed.as_nanos() as f64 / (THREADS * EMITS_PER_THREAD) as f64
    );
}
