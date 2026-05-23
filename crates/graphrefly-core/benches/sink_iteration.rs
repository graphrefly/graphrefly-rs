//! §10.6 sink iteration microbench.
//!
//! Isolates the per-node `subscribers: HashMap<SubscriptionId, Sink>`
//! flush hot-path (`crates/graphrefly-core/src/node.rs` line ~1263) — the
//! data structure §10.6 of `docs/porting-deferred.md` originally
//! proposed replacing with epoch-iteration / snapshot-spread (the TS
//! pattern). The §10.6 entry already notes "Rust avoids snapshot cost
//! because sink re-entrance discipline differs" — this bench measures
//! whether the current HashMap iteration scales linearly with subscriber
//! count (fine) or super-linearly (would indicate hashing/locking is a
//! hot spot worth addressing).
//!
//! # Why this bench exists
//!
//! `flush_notifications` iterates `subscribers` under the per-Core state
//! lock and calls each `Sink: Rc<dyn Fn(&[Message])>` once per pending
//! batch. The proposed swap is to an `Arc<[Sink]>` snapshot taken on
//! subscribe-set mutation (epoch tracking) so the iteration runs
//! lock-released. The Rust dispatcher's re-entrance discipline (D248
//! single-owner Core, `!Send + !Sync` post-S6) already prevents the
//! re-entrance scenarios that motivated the TS snapshot pattern, so the
//! benefit reduces to "shorter critical section" — bench-evidence-gated.
//!
//! # Workload
//!
//! `state_emit_with_S_subscribers` constructs ONE state node with S
//! independent noop sinks attached, then `core.emit(s, fresh_handle)`
//! per sample. Each emission triggers a per-wave flush that iterates all
//! S subscribers. The wave is single-node (no fan-out / fan-in), so the
//! flush_notifications cost dominates and the §10.6 hot path is
//! isolated.
//!
//! The benchmark sweeps S = 1, 8, 32, 128, 512 to surface any
//! super-linear component (would indicate HashMap hashing pressure or
//! per-call closure-dispatch cache pressure becoming load-bearing at
//! scale).

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

/// Noop sink — present to keep the state node activated and exercise the
/// per-batch flush iteration.
fn noop_sink() -> Sink {
    std::rc::Rc::new(|_msgs: &[Message]| {})
}

// ---------------------------------------------------------------------------
// Bench
// ---------------------------------------------------------------------------

fn bench_state_emit_with_s_subscribers(c: &mut Criterion) {
    let mut group = c.benchmark_group("sink_iteration_per_emit");
    for &s_count in &[1_usize, 8, 32, 128, 512] {
        let binding = BenchBinding::new();
        let core = Core::new(binding.clone());
        let s = core.register_state(HandleId::new(1), false).unwrap();
        // Hold all S subscriptions for the duration of the bench. RAII-bound
        // to a Vec so they live across `b.iter`; bare `_` would unsub
        // immediately and the iteration would always be empty.
        let _subs: Vec<_> = (0..s_count)
            .map(|_| core.subscribe(s, noop_sink()))
            .collect();

        // Throughput: 1 emission per iter, S subscriber-callbacks fanned out.
        // We report per-emit so the y-axis is ns/op; the per-subscriber slope
        // is computed downstream from the sweep.
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::from_parameter(s_count), &s_count, |b, _| {
            b.iter(|| {
                let h = binding.fresh_handle();
                core.emit(black_box(s), black_box(h));
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_state_emit_with_s_subscribers);
criterion_main!(benches);
