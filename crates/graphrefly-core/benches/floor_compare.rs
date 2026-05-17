//! §7 floor-verification bench (D216) — the **real shipped** dispatcher,
//! both `StateCell` monomorphizations, side by side.
//!
//! The §7 perf thesis ("~83 ns / ~7.6× floor"; `migration-status.md`
//! § "Concurrency-model rewrite — §7") was only ever measured on
//! `benches/minimal_handle_core.rs` — a hand-written lean *mirror* of the
//! protocol with ZERO of the production machinery (no `CoreState`
//! HashMap, no `Arc<C>`, no refcount discipline, no `BatchGuard`, no
//! `SERIALIZE_WAVES` branch). That number is the *floor of the floor*.
//!
//! This bench measures the genuine `Core<C>` we ship (D216, user:
//! "I want the real one we use in production — that's accurate"):
//!
//! - `Core<SingleThreadCell>` — the §7 lock-free floor path
//!   (`RefCell`, `!Sync`, group machinery monomorphizes out).
//! - `Core<LockedCell>` — the production *default* (D211: every
//!   downstream crate + napi). All-`None` here pays the QA-F1 (D213)
//!   Core-global wave `ReentrantMutex` (~1 uncontended acquire/wave).
//!
//! Same workloads as `benches/dispatcher.rs` (chain/diamond/fanout +
//! the identity-dedup hot path) so the two arms' ratio is the honest
//! "what did deleting union-find + the `SingleThreadCell` path actually
//! buy on the real dispatcher" answer, and each arm cross-references the
//! `minimal_handle_core` mirror (lean Rust) and the §4c pure-ts numbers.
//!
//! Run: `cargo bench -p graphrefly-core --bench floor_compare`
//! Quick: append `-- --quick` (criterion one-shot, no statistics).

use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use graphrefly_core::{
    BindingBoundary, Core, DepBatch, EqualsMode, FnId, FnResult, HandleId, Message, NodeId, Sink,
    StateCell,
};
use parking_lot::Mutex;

// ---------------------------------------------------------------------------
// Bench-specific lightweight binding — identical shape to dispatcher.rs's
// `BenchBinding` so the two harnesses' numbers are directly comparable.
// `BindingBoundary: Send + Sync` (boundary.rs), so this works for both the
// `!Send` `SingleThreadCell` arm (binding never crosses a thread here) and
// the `Send + Sync` `LockedCell` arm.
// ---------------------------------------------------------------------------

struct BenchBinding {
    next_handle: Mutex<u64>,
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

fn noop_sink() -> Sink {
    Arc::new(|_msgs: &[Message]| {})
}

// ---------------------------------------------------------------------------
// Workload setup — generic over the `StateCell` so both real
// monomorphizations run the *same* topology. No `serialization_group` is
// ever assigned: the floor question is "all-`None` real Core, both cells".
// ---------------------------------------------------------------------------

fn build_chain<C: StateCell>(core: &Core<C>, length: usize) -> (NodeId, NodeId) {
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

fn build_diamond<C: StateCell>(core: &Core<C>, fanout: usize) -> (NodeId, NodeId) {
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

fn build_fanout<C: StateCell>(core: &Core<C>, fanout: usize) -> (NodeId, Vec<NodeId>) {
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
// The two real cells. `LockedCell` is the production default; the generic
// `Core::<SingleThreadCell>::new_with_cell` reaches the §7 floor path.
// `CELL` names the arm in the criterion id (`floor/<cell>/<workload>/<n>`).
// ---------------------------------------------------------------------------

trait FloorCell: StateCell + Sized {
    const CELL: &'static str;
    fn make(binding: Arc<dyn BindingBoundary>) -> Core<Self>;
}

impl FloorCell for graphrefly_core::SingleThreadCell {
    const CELL: &'static str = "SingleThreadCell";
    fn make(binding: Arc<dyn BindingBoundary>) -> Core<Self> {
        Core::<graphrefly_core::SingleThreadCell>::new_with_cell(binding)
    }
}

impl FloorCell for graphrefly_core::LockedCell {
    const CELL: &'static str = "LockedCell";
    fn make(binding: Arc<dyn BindingBoundary>) -> Core<Self> {
        Core::new(binding)
    }
}

// ---------------------------------------------------------------------------
// Benches — one criterion group per workload, one arm per real cell.
// ---------------------------------------------------------------------------

fn bench_identity_dedup<C: FloorCell>(c: &mut Criterion) {
    let mut group = c.benchmark_group("floor/identity_dedup");
    group.throughput(Throughput::Elements(1));

    let binding = BenchBinding::new();
    let core = C::make(binding.clone());
    let s = core.register_state(HandleId::new(1), false).unwrap();
    let _sub = core.subscribe(s, noop_sink());
    let same = HandleId::new(1);

    group.bench_function(BenchmarkId::from_parameter(C::CELL), |b| {
        b.iter(|| {
            core.emit(black_box(s), black_box(same));
        });
    });
    group.finish();
}

fn bench_chain<C: FloorCell>(c: &mut Criterion) {
    let mut group = c.benchmark_group("floor/chain");
    for &n in &[1_usize, 4, 16, 64] {
        let binding = BenchBinding::new();
        let core = C::make(binding.clone());
        let (s, sink) = build_chain(&core, n);
        let _sub = core.subscribe(sink, noop_sink());

        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new(C::CELL, n), &n, |b, _| {
            b.iter(|| {
                let h = binding.fresh_handle();
                core.emit(black_box(s), black_box(h));
            });
        });
    }
    group.finish();
}

fn bench_diamond<C: FloorCell>(c: &mut Criterion) {
    let mut group = c.benchmark_group("floor/diamond");
    for &n in &[2_usize, 8, 32] {
        let binding = BenchBinding::new();
        let core = C::make(binding.clone());
        let (s, sink) = build_diamond(&core, n);
        let _sub = core.subscribe(sink, noop_sink());

        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new(C::CELL, n), &n, |b, _| {
            b.iter(|| {
                let h = binding.fresh_handle();
                core.emit(black_box(s), black_box(h));
            });
        });
    }
    group.finish();
}

fn bench_fanout<C: FloorCell>(c: &mut Criterion) {
    let mut group = c.benchmark_group("floor/fanout");
    for &n in &[10_usize, 100, 1000] {
        let binding = BenchBinding::new();
        let core = C::make(binding.clone());
        let (s, leaves) = build_fanout(&core, n);
        // RAII: keep all subscriptions alive for the whole bench block.
        let _subs: Vec<_> = leaves
            .iter()
            .map(|&leaf| core.subscribe(leaf, noop_sink()))
            .collect();

        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new(C::CELL, n), &n, |b, _| {
            b.iter(|| {
                let h = binding.fresh_handle();
                core.emit(black_box(s), black_box(h));
            });
        });
    }
    group.finish();
}

fn floor(c: &mut Criterion) {
    // §7 floor path first, production default second, so the criterion
    // report lists them in "floor → default" order per workload.
    bench_identity_dedup::<graphrefly_core::SingleThreadCell>(c);
    bench_identity_dedup::<graphrefly_core::LockedCell>(c);
    bench_chain::<graphrefly_core::SingleThreadCell>(c);
    bench_chain::<graphrefly_core::LockedCell>(c);
    bench_diamond::<graphrefly_core::SingleThreadCell>(c);
    bench_diamond::<graphrefly_core::LockedCell>(c);
    bench_fanout::<graphrefly_core::SingleThreadCell>(c);
    bench_fanout::<graphrefly_core::LockedCell>(c);
}

criterion_group!(benches, floor);
criterion_main!(benches);
