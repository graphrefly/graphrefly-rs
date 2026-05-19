//! Single-owner dispatcher **floor** bench (S4 rebuild — D246/S2c).
//!
//! The original `floor_compare` was a side-by-side of the two
//! `StateCell` monomorphizations (`Core<SingleThreadCell>` vs
//! `Core<LockedCell>`). D246/S2c collapsed the `StateCell` generic:
//! there is now exactly ONE (non-generic, `RefCell`-backed,
//! single-owner `!Send + !Sync`) `Core` and the cross-thread wave-lock
//! machinery is deleted, so the comparison no longer has two arms — the
//! single-owner `Core` *is* the floor.
//!
//! Rebuilt (per the S4 plan: "rebuild the floor/actor-model perf bench
//! with independent per-worker Cores alongside `group_scaling.rs`") as
//! the honest single-arm floor: the zero-FFI equals-substitution
//! identity-dedup hot path (emit the SAME handle on one state node with
//! a noop sink — no `invoke_fn`/`custom_equals` beyond the equals
//! check). `benches/group_scaling.rs` carries the actor-model
//! independent-per-worker-Core scaling re-measure; this carries the
//! per-emit ns floor that scaling is measured against.

use std::hint::black_box;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use graphrefly_core::{
    BindingBoundary, Core, DepBatch, FnId, FnResult, HandleId, Message, NodeId, Sink,
};

/// Minimal binding shape-matched to the historical `floor_compare`
/// `BenchBinding` so the number stays comparable across the rebuild.
struct FloorBinding;
impl BindingBoundary for FloorBinding {
    fn invoke_fn(&self, _: NodeId, _: FnId, _: &[DepBatch]) -> FnResult {
        FnResult::Data {
            handle: HandleId::new(1),
            tracked: None,
        }
    }
    fn custom_equals(&self, _: FnId, a: HandleId, b: HandleId) -> bool {
        a == b
    }
    fn release_handle(&self, _: HandleId) {}
    // Explicit no-op (QA, 2026-05-19): pin the bench number against
    // any future trait-default change for `retain_handle`.
    fn retain_handle(&self, _: HandleId) {}
}

fn noop_sink() -> Sink {
    Arc::new(|_: &[Message]| {})
}

fn floor(c: &mut Criterion) {
    let mut g = c.benchmark_group("floor_compare");
    g.throughput(Throughput::Elements(1));
    g.bench_function("identity_dedup/single_owner", |b| {
        let binding: Arc<dyn BindingBoundary> = Arc::new(FloorBinding);
        let core = Core::new(binding);
        let s = core.register_state(HandleId::new(1), false).unwrap();
        let _sub = core.subscribe(s, noop_sink());
        let same = HandleId::new(1);
        b.iter(|| {
            // Same handle ⇒ equals-substitution dedup: the lock-free
            // single-owner floor (no fn fire, no cascade).
            core.emit(black_box(s), black_box(same));
        });
    });
    g.finish();
}

criterion_group!(benches, floor);
criterion_main!(benches);
