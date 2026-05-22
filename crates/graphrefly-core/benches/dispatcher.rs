//! Dispatcher hot-path microbenchmarks.
//!
//! Pairs with `~/src/graphrefly-ts/bench/dispatcher.bench.ts` — the same
//! workload patterns measured against the current pure-TS implementation.
//! Side-by-side numbers answer Phase 13.7's "is the Rust dispatcher
//! fundamentally faster?" question (FFI overhead measured separately).
//!
//! # Workloads
//!
//! - `state_emit_identity_dedup` — tight loop emitting same value on a state
//!   node; equals-substitution under identity is the zero-FFI hot path.
//! - `state_emit_changing_value` — tight loop emitting distinct values;
//!   exercises full DATA dispatch + cache update + refcount paths.
//! - `chain_propagation_n` — N-deep chain `s → d1 → d2 → ... → dN`;
//!   measures one update-at-source → wave-close-at-sink latency × propagation
//!   per node.
//! - `diamond_fanout_n` — N-fanout diamond: `s → {d1, d2, ..., dN} → sink`;
//!   measures one update-at-source → wave-coalescing → sink fires once.
//! - `large_fanout_n` — `s → {leaf1, leaf2, ..., leafN}` (no inner derived);
//!   measures pure dispatch / propagation cost without fn-fire overhead.

use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use graphrefly_core::{
    BindingBoundary, Core, DepBatch, EqualsMode, FnId, FnResult, HandleId, Message, NodeId, Sink,
};
use parking_lot::Mutex;

// ---------------------------------------------------------------------------
// Bench-specific lightweight binding. The TestBinding from tests/common is
// designed for correctness (interning, deref, registry); this one is shaped
// for speed (handle = u64 from a counter; deref returns canned value).
// ---------------------------------------------------------------------------

struct BenchBinding {
    next_handle: Mutex<u64>,
    /// fn_id → closure that consumes dep handles (count) and returns a new handle.
    /// We use a static "increment counter" pattern to avoid per-fire allocation.
    invoke_count: Mutex<u64>,
}

impl BenchBinding {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            next_handle: Mutex::new(1),
            invoke_count: Mutex::new(0),
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
        *self.invoke_count.lock() += 1;
        // Always return a fresh handle — exercises the full DATA path.
        // For identity-equals dedup tests we use a different fn that returns
        // the same handle each time (pre-allocated below).
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

/// A noop sink — present to keep nodes activated; doesn't accumulate.
fn noop_sink() -> Sink {
    std::rc::Rc::new(|_msgs: &[Message]| {})
}

// ---------------------------------------------------------------------------
// Workload setup helpers
// ---------------------------------------------------------------------------

fn build_chain(core: &Core, length: usize) -> (NodeId, NodeId) {
    // s = state; chain of `length` derived nodes each reading from previous.
    let s = core.register_state(HandleId::new(1), false).unwrap();
    let mut prev = s;
    let dummy_fn = FnId::new(0);
    for _ in 0..length {
        prev = core
            .register_derived(&[prev], dummy_fn, EqualsMode::Identity, false)
            .unwrap();
    }
    (s, prev)
}

fn build_diamond(core: &Core, fanout: usize) -> (NodeId, NodeId) {
    // s → {d1..dN} → sink; sink has all dN as deps.
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

fn build_fanout(core: &Core, fanout: usize) -> (NodeId, Vec<NodeId>) {
    // s → {leaf1..leafN}, no further reduction.
    let s = core.register_state(HandleId::new(1), false).unwrap();
    let dummy_fn = FnId::new(0);
    let leaves: Vec<NodeId> = (0..fanout)
        .map(|_| {
            core.register_derived(&[s], dummy_fn, EqualsMode::Identity, false)
                .unwrap()
        })
        .collect();
    (s, leaves)
}

// ---------------------------------------------------------------------------
// Benches
// ---------------------------------------------------------------------------

fn bench_state_emit_identity_dedup(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_emit_identity_dedup");
    group.throughput(Throughput::Elements(1));

    let binding = BenchBinding::new();
    let core = Core::new(binding.clone());
    let s = core.register_state(HandleId::new(1), false).unwrap();
    let _sub = core.subscribe(s, noop_sink());
    let same = HandleId::new(1);

    group.bench_function("emit_same_handle", |b| {
        b.iter(|| {
            core.emit(black_box(s), black_box(same));
        });
    });
    group.finish();
}

fn bench_state_emit_changing(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_emit_changing_value");
    group.throughput(Throughput::Elements(1));

    let binding = BenchBinding::new();
    let core = Core::new(binding.clone());
    let s = core.register_state(HandleId::new(1), false).unwrap();
    let _sub = core.subscribe(s, noop_sink());

    group.bench_function("emit_fresh_handle_each", |b| {
        b.iter(|| {
            let h = binding.fresh_handle();
            core.emit(black_box(s), black_box(h));
        });
    });
    group.finish();
}

fn bench_chain_propagation(c: &mut Criterion) {
    let mut group = c.benchmark_group("chain_propagation");
    for &n in &[1_usize, 4, 16, 64] {
        let binding = BenchBinding::new();
        let core = Core::new(binding.clone());
        let (s, sink) = build_chain(&core, n);
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

fn bench_diamond(c: &mut Criterion) {
    let mut group = c.benchmark_group("diamond_fanout");
    for &n in &[2_usize, 8, 32] {
        let binding = BenchBinding::new();
        let core = Core::new(binding.clone());
        let (s, sink) = build_diamond(&core, n);
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

fn bench_large_fanout(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_fanout");
    for &n in &[10_usize, 100, 1000] {
        let binding = BenchBinding::new();
        let core = Core::new(binding.clone());
        let (s, leaves) = build_fanout(&core, n);
        // Subscribe to all leaves so they activate and propagation is exercised.
        // Per §10.12 RAII: bind to a Vec so subscriptions live for the whole
        // bench iteration block (binding to bare `_` would unsub immediately).
        let _subs: Vec<_> = leaves
            .iter()
            .map(|&leaf| core.subscribe(leaf, noop_sink()))
            .collect();

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

criterion_group!(
    benches,
    bench_state_emit_identity_dedup,
    bench_state_emit_changing,
    bench_chain_propagation,
    bench_diamond,
    bench_large_fanout,
);
criterion_main!(benches);
