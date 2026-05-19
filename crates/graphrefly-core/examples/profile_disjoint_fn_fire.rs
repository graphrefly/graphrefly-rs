//! Profile target — disjoint **fn-fire** parallelism (2 threads, each
//! emitting on its own state node in a disjoint partition, cascading
//! through a derived node with calibrated CPU spin). Mirrored
//! `benches/per_subgraph_parallelism.rs::fnfire_parallel_2t_disjoint`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use graphrefly_core::{BindingBoundary, DepBatch, FnId, FnResult, HandleId, Message, NodeId, Sink};

const EMITS_PER_THREAD: usize = 2_000_000;
const SPIN_ITERS_PER_FIRE: u32 = 1_500;

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
    fn invoke_fn(&self, _: NodeId, _: FnId, _dep_data: &[DepBatch]) -> FnResult {
        // Calibrated counted spin (~3µs on modern hw).
        for _ in 0..SPIN_ITERS_PER_FIRE {
            std::hint::black_box(0u64);
        }
        FnResult::Data {
            handle: HandleId::new(self.next_handle.fetch_add(1, Ordering::Relaxed)),
            tracked: None,
        }
    }

    fn custom_equals(&self, _: FnId, _a: HandleId, _b: HandleId) -> bool {
        false
    }

    fn release_handle(&self, _: HandleId) {}

    fn retain_handle(&self, _: HandleId) {}
}

fn noop_sink() -> Sink {
    Arc::new(|_msgs: &[Message]| {})
}

// D246/S2b Blind #2: this profiler's premise — ONE shared `Core`
// cloned across 2 threads emitting on "disjoint partitions"
// (`core.partition_of`, the §7 union-find) — is exactly the model the
// actor / work-stealing rewrite deletes (`Core` is move-only `!Sync`,
// `Core::clone` removed by D246; S2c removes `partition_of`). Its
// mirror bench `benches/per_subgraph_parallelism.rs` was trashed for
// the same invalid `assert_ne!(partition_of(s_a), partition_of(s_b))`
// premise (B-3 / D220-EXEC; both `None` post-§7). Disjoint
// parallelism is re-measured at S4 via `benches/group_scaling.rs`
// with the actor model (independent Cores per worker), not a
// cloned-shared Core. Gutted to a deferred note so the example target
// compiles; do NOT resurrect the cross-thread-shared-Core shape. This
// mirrors the sibling `profile_disjoint_state_emit.rs` migration.
#[allow(dead_code)]
fn _superseded_uses(_b: fn() -> Arc<WorkBinding>, _s: fn() -> Sink) {}

fn main() {
    let _ = (WorkBinding::new, noop_sink, EMITS_PER_THREAD);
    eprintln!(
        "profile_disjoint_fn_fire: superseded by the actor model \
         (D246 makes Core move-only `!Sync`; S2c deletes §7 \
         partitioning; S4 re-measures disjoint parallelism via \
         benches/group_scaling.rs with independent per-worker Cores). \
         See porting-deferred § /qa-2026-05-18."
    );
}
