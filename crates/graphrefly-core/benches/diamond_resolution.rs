//! §10.3 diamond resolution bookkeeping microbench.
//!
//! Isolates the `pending_fires: AHashSet<NodeId>` hot-path on
//! per-thread `WaveState` (`crates/graphrefly-core/src/batch.rs`) — the
//! data structure §10.3 of `docs/porting-deferred.md` originally proposed
//! replacing with a `WaveTracker { settled: u64, all_deps_mask: u64 }`
//! bitmask for O(1) "is_complete" vs O(n) walks.
//!
//! # Why this bench exists
//!
//! The original §10.3 entry framed the gap as `pending_fires:
//! HashSet<NodeId>` + per-node `involved_this_wave`. The per-node field
//! is gone (eliminated when the wave engine was rewritten); the only
//! remaining diamond bookkeeping is the wave-scoped
//! `pending_fires: AHashSet<NodeId>` set + the per-node `received_mask:
//! u64` (Slice U 2026-05-12 — first-run gate's `has_sentinel_deps()` is
//! ALREADY an O(1) bitmask compare). The pick-next-fire scan also runs
//! O(|pending_fires|) by `topo_rank` per fire (post-Slice U).
//!
//! After both, the §10.3 bitmask proposal has only one remaining
//! candidate target: replacing the `AHashSet<NodeId>` itself with a
//! per-wave `u64` bitmask. That pays only when fan-in stays under 64 deps
//! AND the AHashSet pressure is measurable. This bench measures the
//! per-N-fan-in cost to either validate a remaining gap (≥5% projected
//! improvement at fanout=32) or close §10.3 as "validated not a win"
//! with explicit numbers.
//!
//! # Workload
//!
//! `diamond_fan_in_per_wave` constructs the classic `dispatcher.rs`
//! diamond topology: one source state node fans out to N intermediate
//! derived nodes, all N intermediates feed back into a single sink
//! derived node (fan-in N). Emitting once to the source produces a wave
//! where N intermediates fire (each inserts itself into `pending_fires`)
//! and the sink fires after the N-th intermediate settles (the
//! "all deps settled" check at sink-fire time is the §10.3 target).
//!
//! The benchmark sweeps N = 1, 4, 8, 16, 32, 64 to surface any
//! super-linear component: linear/sublinear scaling = AHashSet is fine;
//! super-linear scaling > N = the bitmask proposal would pay.

use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use graphrefly_core::{
    BindingBoundary, Core, DepBatch, EqualsMode, FnId, FnResult, HandleId, Message, NodeId, Sink,
};
use parking_lot::Mutex;

// ---------------------------------------------------------------------------
// Bench-specific lightweight binding (mirrors dispatcher.rs `BenchBinding`).
// ---------------------------------------------------------------------------

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
    fn invoke_fn(&self, _: NodeId, _: FnId, _dep_data: &[DepBatch]) -> FnResult {
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

/// Noop sink — present to keep the sink derived node activated so the
/// full diamond resolution path runs each wave.
fn noop_sink() -> Sink {
    std::rc::Rc::new(|_msgs: &[Message]| {})
}

/// `s` → `{d1..dN}` → `sink`. Mirrors `dispatcher.rs::build_diamond`.
fn build_diamond(core: &Core, fanout: usize) -> (NodeId, NodeId) {
    let s = core.register_state(HandleId::new(1), false).unwrap();
    let dummy_fn = FnId::new(0);
    let inner: Vec<NodeId> = (0..fanout)
        .map(|_| {
            core.register_derived(&[s], dummy_fn, EqualsMode::Identity, false)
                .unwrap()
        })
        .collect();
    let sink = core
        .register_derived(&inner, dummy_fn, EqualsMode::Identity, false)
        .unwrap();
    (s, sink)
}

// ---------------------------------------------------------------------------
// Bench
// ---------------------------------------------------------------------------

fn bench_diamond_fan_in_per_wave(c: &mut Criterion) {
    let mut group = c.benchmark_group("diamond_resolution_fan_in");
    for &n in &[1_usize, 4, 8, 16, 32, 64] {
        let binding = BenchBinding::new();
        let core = Core::new(binding.clone());
        let (s, sink) = build_diamond(&core, n);
        // Hold the sink subscription for the duration of the bench so the
        // diamond stays activated; RAII-bound to a held variable.
        let _sub = core.subscribe(sink, noop_sink());

        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let h = binding.fresh_handle();
                core.emit(black_box(s), black_box(h));
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_diamond_fan_in_per_wave);
criterion_main!(benches);
