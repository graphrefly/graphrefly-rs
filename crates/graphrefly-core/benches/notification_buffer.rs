//! §10.5 wave-end notification buffer microbench.
//!
//! Isolates the `pending_notify: IndexMap<NodeId, PendingPerNode>` hot-path
//! (`crates/graphrefly-core/src/batch.rs`) — the data structure §10.5 of
//! `docs/porting-deferred.md` originally proposed replacing with a flat
//! `BatchFrame { pending: Vec<(NodeId, ...)> }` arena.
//!
//! # Why this bench exists
//!
//! Since §10.5 was first surfaced (Slice A+B audit, 2026-05-05) the structure
//! has been hardened twice:
//!
//! 1. **Per-node `Vec<Message>` → `SmallVec<[Message; 3]>`** (resolved profile
//!    item (2), 2026-05-10): inlines the common DIRTY+DATA+RESOLVED set,
//!    eliminating heap allocation for the dominant per-node case.
//! 2. **`IndexMap::default()` per wave → ping-pong with
//!    `pending_notify_recycle`** (D217-AMEND-2, 2026-05-16): the persistent
//!    spare swaps with `pending_notify` at wave end so a fresh `IndexMap`
//!    (new `ahash::RandomState` + RawVec realloc) is NEVER constructed after
//!    thread init. Empirically attributed ~1250 of ~4767 hot-path samples to
//!    the old per-wave `mem::take` — eliminated.
//!
//! After both, the §10.5 arena proposal has no obvious remaining target. This
//! bench measures the current per-N-active-nodes-per-wave cost to either
//! validate a remaining gap (≥5% projected improvement at fanout=32) or close
//! §10.5 as "validated not a win" with explicit numbers.
//!
//! # Workload
//!
//! `n_independent_emits_per_wave` constructs N independent state nodes, each
//! with a single subscriber, and emits to all N inside one `Core::batch`.
//! This produces exactly N `pending_notify` entries per wave; the wave-end
//! drain + recycle is exercised in isolation from upstream propagation work.
//!
//! The benchmark sweeps N = 1, 8, 32, 128, 256 to surface any super-linear
//! component (would indicate IndexMap hashing or `PendingPerNode.batches`
//! SmallVec spill becoming load-bearing at scale).

use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use graphrefly_core::{
    BindingBoundary, Core, DepBatch, FnId, FnResult, HandleId, Message, NodeId, Sink,
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
        // Not used — this bench has no derived nodes.
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

/// Noop sink — present to keep nodes activated so `pending_notify` actually
/// receives entries during dispatch.
fn noop_sink() -> Sink {
    std::rc::Rc::new(|_msgs: &[Message]| {})
}

// ---------------------------------------------------------------------------
// Bench
// ---------------------------------------------------------------------------

fn bench_n_independent_emits_per_wave(c: &mut Criterion) {
    let mut group = c.benchmark_group("notification_buffer_per_wave");
    for &n in &[1_usize, 8, 32, 128, 256] {
        let binding = BenchBinding::new();
        let core = Core::new(binding.clone());
        let states: Vec<NodeId> = (0..n)
            .map(|_| core.register_state(HandleId::new(1), false).unwrap())
            .collect();
        // Hold subscriptions for the duration of the bench. RAII-bound to
        // a Vec so they live across `b.iter`; bare `_` would unsub immediately.
        let _subs: Vec<_> = states
            .iter()
            .map(|&s| core.subscribe(s, noop_sink()))
            .collect();

        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                core.batch(|| {
                    for &s in &states {
                        let h = binding.fresh_handle();
                        core.emit(black_box(s), black_box(h));
                    }
                });
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_n_independent_emits_per_wave);
criterion_main!(benches);
