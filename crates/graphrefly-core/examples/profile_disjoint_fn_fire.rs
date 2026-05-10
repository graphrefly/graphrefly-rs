//! Profile target — runs the disjoint fn-fire workload long enough for
//! samply to sample meaningfully. Used to identify hot paths in the
//! post-sub-slice-3 dispatcher.
//!
//! Run via:
//!   `samply record --rate 999 -- cargo run --release --example profile_disjoint_fn_fire`
//!
//! Or for a quick view:
//!   `cargo run --release --example profile_disjoint_fn_fire`
//!
//! The workload mirrors `crates/graphrefly-core/benches/per_subgraph_parallelism.rs::fnfire_parallel_2t_disjoint`:
//! 2 threads, each emits `EMITS_PER_THREAD` times on its own state node
//! in a disjoint partition. Each emit cascades through a derived node
//! whose fn does `SPIN_ITERS_PER_FIRE` iterations of `black_box(0u64)`
//! as calibrated CPU work (lock-released invoke_fn).
//!
//! At `EMITS_PER_THREAD = 2_000_000` and `SPIN_ITERS_PER_FIRE = 1_500`,
//! total wall-clock is ~8s on Apple Silicon (~2.1µs/emit; the spin is
//! ~450ns of empty black_box, the rest is dispatcher overhead) — long
//! enough for samply at `--rate 1999` to collect ~16k samples per
//! worker thread. Tune `EMITS_PER_THREAD` if your hardware needs a
//! shorter run.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use graphrefly_core::{
    BindingBoundary, Core, DepBatch, EqualsMode, FnId, FnResult, HandleId, Message, NodeId, Sink,
};

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

    fn fresh(&self) -> HandleId {
        HandleId::new(self.next_handle.fetch_add(1, Ordering::Relaxed))
    }
}

impl BindingBoundary for WorkBinding {
    fn invoke_fn(&self, _: NodeId, _: FnId, _dep_data: &[DepBatch]) -> FnResult {
        // Calibrated counted spin (~3µs on modern hw).
        for _ in 0..SPIN_ITERS_PER_FIRE {
            std::hint::black_box(0u64);
        }
        FnResult::Data {
            handle: self.fresh(),
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

fn main() {
    let binding = WorkBinding::new();
    let core = Core::new(binding.clone());

    // Two disjoint state nodes.
    let s_a = core.register_state(HandleId::new(1), false).unwrap();
    let s_b = core.register_state(HandleId::new(2), false).unwrap();

    // Each state node has a derived consumer (lock-released fn fire).
    let fn_id = FnId::new(1);
    let d_a = core
        .register_derived(&[s_a], fn_id, EqualsMode::Identity, false)
        .unwrap();
    let d_b = core
        .register_derived(&[s_b], fn_id, EqualsMode::Identity, false)
        .unwrap();

    std::mem::forget(core.subscribe(d_a, noop_sink()));
    std::mem::forget(core.subscribe(d_b, noop_sink()));

    assert_ne!(
        core.partition_of(s_a),
        core.partition_of(s_b),
        "premise: s_a and s_b in disjoint partitions"
    );

    let core_a = core.clone();
    let core_b = core.clone();
    let binding_a = binding.clone();
    let binding_b = binding.clone();

    let start = std::time::Instant::now();

    let t_a = thread::spawn(move || {
        for _ in 0..EMITS_PER_THREAD {
            let h = binding_a.fresh();
            core_a.emit(s_a, h);
        }
    });
    let t_b = thread::spawn(move || {
        for _ in 0..EMITS_PER_THREAD {
            let h = binding_b.fresh();
            core_b.emit(s_b, h);
        }
    });
    t_a.join().unwrap();
    t_b.join().unwrap();

    let elapsed = start.elapsed();
    eprintln!(
        "Profile workload complete: {} total emits, {:?} wall clock, {:.2} ns/emit",
        2 * EMITS_PER_THREAD,
        elapsed,
        elapsed.as_nanos() as f64 / (2 * EMITS_PER_THREAD) as f64
    );
}
