//! Profile target — actor-model disjoint **fn-fire** scaling (S4
//! rebuild). `THREADS` workers, each owning its OWN single-owner
//! `Core` (built inside the worker — `Core` is `!Send`), each emitting
//! on a state node cascading through a derived node with calibrated
//! CPU spin. The deleted version cloned ONE shared `Core` across
//! threads on "disjoint §7 partitions" (`partition_of` — removed by
//! S2c; `Core` is move-only `!Sync`). Independent per-worker Cores is
//! the actor-model substrate `benches/group_scaling.rs` benchmarks;
//! this is its profileable sibling for the fn-fire (cascade) regime.
//!
//! Run:
//!   cargo build --profile profiling --example profile_disjoint_fn_fire -p graphrefly-core
//!   samply record --save-only -o /tmp/p.json.gz -- \
//!     target/profiling/examples/profile_disjoint_fn_fire

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use graphrefly_core::{
    BindingBoundary, Core, DepBatch, EqualsMode, FnId, FnResult, HandleId, Message, NodeId, Sink,
};

const EMITS_PER_THREAD: u64 = 2_000_000;
const SPIN_ITERS_PER_FIRE: u32 = 1_500;
const THREADS: usize = 2;

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
        // Calibrated counted spin (~3µs on modern hw).
        for _ in 0..SPIN_ITERS_PER_FIRE {
            std::hint::black_box(0u64);
        }
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
    std::rc::Rc::new(|_: &[Message]| {})
}

/// One worker owns its own `Core`; emits on a state node cascading
/// through a spinning derived.
fn worker() {
    let binding = WorkBinding::new();
    let core = Core::new(binding as Arc<dyn BindingBoundary>);
    let s = core.register_state(HandleId::new(1), false).unwrap();
    let d = core
        .register_derived(&[s], FnId::new(1), EqualsMode::Identity, false)
        .unwrap();
    let sub = core.subscribe(d, noop_sink());
    for k in 0..EMITS_PER_THREAD {
        core.emit(std::hint::black_box(s), HandleId::new(2 + k));
    }
    core.drain_mailbox();
    core.unsubscribe(d, sub);
}

fn main() {
    let start = std::time::Instant::now();
    let handles: Vec<_> = (0..THREADS).map(|_| thread::spawn(worker)).collect();
    for h in handles {
        h.join().expect("worker Core completed");
    }
    let dt = start.elapsed();
    let total = EMITS_PER_THREAD * THREADS as u64;
    eprintln!(
        "profile_disjoint_fn_fire: {THREADS}×{EMITS_PER_THREAD} fn-fire \
         emits (independent per-worker Cores) in {dt:?} = {:.1} ns/emit \
         aggregate",
        dt.as_nanos() as f64 / total as f64
    );
}
