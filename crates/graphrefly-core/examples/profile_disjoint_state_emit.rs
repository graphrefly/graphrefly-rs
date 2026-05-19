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

use graphrefly_core::{BindingBoundary, DepBatch, FnId, FnResult, HandleId, Message, NodeId, Sink};

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

// S2b/β /qa-2026-05-18 Blind #2: this profiler's premise — ONE
// shared `Core` cloned across N threads emitting on "disjoint
// partitions" (`core.partition_of`, the §7 union-find) — is exactly
// the model the actor / work-stealing rewrite deletes (S2c removes
// `partition_of`/`LockedCell`; `Core` is move-only `!Sync`). Disjoint
// parallelism is re-measured at S4 via `benches/group_scaling.rs`
// with the actor model (independent Cores per worker), not a
// cloned-shared Core. Gutted to a deferred note so the example target
// compiles; do NOT resurrect the cross-thread-shared-Core shape.
#[allow(dead_code)]
fn _superseded_uses(_b: fn() -> Arc<WorkBinding>, _s: fn() -> Sink) {}

fn main() {
    let _ = (WorkBinding::new, noop_sink, THREADS, EMITS_PER_THREAD);
    eprintln!(
        "profile_disjoint_state_emit: superseded by the actor model \
         (S2c deletes §7 partitioning; S4 re-measures disjoint \
         parallelism via benches/group_scaling.rs with independent \
         per-worker Cores). See porting-deferred § /qa-2026-05-18."
    );
}
