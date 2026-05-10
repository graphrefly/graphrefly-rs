//! Lock-strategy microbench — compares sync primitives under realistic
//! dispatcher access patterns to inform Q-beyond architecture decisions.
//!
//! # What this benches
//!
//! Microbenchmarks of the SYNC PRIMITIVES THEMSELVES, simulating the
//! access patterns of `Core::commit_emission` / `Core::queue_notify` /
//! `Core::pick_next_fire`. NOT a full prototype dispatcher — too expensive
//! to refactor 3x. Instead we measure the cost of:
//!
//! - Single mutex acquire on a HashSet (current `pending_fires` shape).
//! - thread_local borrow_mut on a HashSet (per-thread alternative).
//! - Multiple mutex hops in sequence (current `commit_emission` shape).
//! - HashMap reads under Mutex / RwLock / DashMap (registry alternative).
//! - MPSC channel push (Tokio-style work-stealing alternative).
//!
//! # Scenarios
//!
//! 1. `single_thread_insert_remove` — uncontended cost per primitive.
//! 2. `multi_mutex_hop` — single thread, simulates `commit_emission`'s
//!    state + cross_partition + registry mutex hops.
//! 3. `cross_thread_disjoint_keys` — 2 threads, each insert/remove
//!    distinct keys. Measures cache-line bouncing on the mutex itself
//!    even when data is disjoint.
//! 4. `cross_thread_same_key` — 2 threads, same key. Measures true
//!    contention.
//! 5. `registry_read_heavy` — 80% reads, 20% writes on a HashMap.
//!    Compares Mutex / RwLock / DashMap.
//! 6. `mpsc_push_vs_mutex_insert` — Alt-D simulation: pushing a NodeId
//!    onto an MPSC channel vs inserting into a Mutex<HashSet>.
//!
//! # Output
//!
//! Run via:
//!   `cargo bench -p graphrefly-core --bench lock_strategy`
//!
//! Compare ns/op across primitives within each scenario; the winner
//! tells us which architecture to commit to.

use std::cell::RefCell;
use std::sync::Arc;
use std::thread;

use ahash::AHashSet;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use crossbeam_channel::{unbounded, Receiver, Sender};
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Tuning constants
// ---------------------------------------------------------------------------

/// Operations per benchmark iteration. Tuned to amortize criterion overhead
/// while keeping iteration time around 1–10ms.
const OPS_PER_ITER: usize = 4_000;

/// Pre-built key set so insert/remove operations don't measure key
/// generation. Use `i % KEYSET_SIZE` to vary keys without expanding state.
const KEYSET_SIZE: usize = 64;

// ---------------------------------------------------------------------------
// Scenario 1: single-thread insert + remove
//   Simulates the dispatcher's `pending_fires.insert(x)` followed later by
//   `pending_fires.remove(x)` in fire_regular Phase 1. NO cross-thread
//   contention — just the raw cost of the sync primitive on the local
//   thread.
// ---------------------------------------------------------------------------

fn bench_s1_single_thread(c: &mut Criterion) {
    let mut group = c.benchmark_group("s1_single_thread_insert_remove");
    group.throughput(Throughput::Elements(OPS_PER_ITER as u64));

    // Variant A: parking_lot::Mutex<HashSet>  (current)
    {
        let m: Arc<Mutex<AHashSet<u64>>> = Arc::new(Mutex::new(AHashSet::new()));
        group.bench_function(BenchmarkId::new("parking_lot_mutex", OPS_PER_ITER), |b| {
            b.iter(|| {
                for i in 0..OPS_PER_ITER {
                    let k = (i % KEYSET_SIZE) as u64;
                    let mut g = m.lock();
                    g.insert(black_box(k));
                    g.remove(&k);
                }
            });
        });
    }

    // Variant B: std::sync::Mutex<HashSet>  (baseline for comparison)
    {
        let m: Arc<std::sync::Mutex<AHashSet<u64>>> =
            Arc::new(std::sync::Mutex::new(AHashSet::new()));
        group.bench_function(BenchmarkId::new("std_mutex", OPS_PER_ITER), |b| {
            b.iter(|| {
                for i in 0..OPS_PER_ITER {
                    let k = (i % KEYSET_SIZE) as u64;
                    let mut g = m.lock().unwrap();
                    g.insert(black_box(k));
                    g.remove(&k);
                }
            });
        });
    }

    // Variant C: thread_local RefCell<HashSet>  (per-thread alternative)
    thread_local! {
        static TL_S1: RefCell<AHashSet<u64>> = RefCell::new(AHashSet::new());
    }
    group.bench_function(
        BenchmarkId::new("thread_local_refcell", OPS_PER_ITER),
        |b| {
            b.iter(|| {
                TL_S1.with(|cell| {
                    let mut s = cell.borrow_mut();
                    for i in 0..OPS_PER_ITER {
                        let k = (i % KEYSET_SIZE) as u64;
                        s.insert(black_box(k));
                        s.remove(&k);
                    }
                });
            });
        },
    );

    // Variant D: thread_local with per-op borrow_mut (NOT batched).
    //   This is the "realistic" thread_local cost when many call sites each
    //   borrow independently — the dispatcher's pending_fires.insert is
    //   called from many places, not under one outer borrow.
    group.bench_function(
        BenchmarkId::new("thread_local_refcell_per_op", OPS_PER_ITER),
        |b| {
            b.iter(|| {
                for i in 0..OPS_PER_ITER {
                    let k = (i % KEYSET_SIZE) as u64;
                    TL_S1.with(|cell| {
                        let mut s = cell.borrow_mut();
                        s.insert(black_box(k));
                        s.remove(&k);
                    });
                }
            });
        },
    );

    group.finish();
}

// ---------------------------------------------------------------------------
// Scenario 2: multi-mutex hop — single thread
//   Simulates `Core::commit_emission`'s mutex hop sequence: state.lock()
//   + cross_partition.lock() + registry.lock() in one fire iteration.
//   All under no contention. Measures just the mutex acquire/release
//   overhead per emission.
//
// Variant A: 3 separate mutexes — but the loop body does FIVE acquires
//            per iteration (read nodes + insert fires + insert notify +
//            remove fires + remove notify cleanup). The label
//            `3_separate_mutexes` refers to the storage shape (3 distinct
//            Mutex<T> instances), not the per-iter acquire count.
//            Variant B's single-mutex loop body does 1 acquire with all
//            5 ops inside — so the A/B Δ also includes 4 extra acquires
//            on top of the storage difference. /qa F5 (2026-05-10):
//            relabel BenchmarkId to make the asymmetry explicit.
// Variant B: 1 single mutex (pre-Q2 shape) — 1 acquire/iter, 5 ops.
// Variant C: 1 mutex + 2 thread-local — 1 acquire/iter on the mutex,
//            4 thread-local borrows.
// ---------------------------------------------------------------------------

struct ThreeFields {
    nodes: HashMap<u64, u64>,
    pending_fires: AHashSet<u64>,
    pending_notify: HashMap<u64, u64>,
}

fn bench_s2_multi_mutex_hop(c: &mut Criterion) {
    let mut group = c.benchmark_group("s2_multi_mutex_hop");
    group.throughput(Throughput::Elements(OPS_PER_ITER as u64));

    // Variant A: 3 separate parking_lot::Mutex<...>  (current Q3+Q2 shape)
    {
        let nodes: Arc<Mutex<HashMap<u64, u64>>> = Arc::new(Mutex::new(HashMap::new()));
        let fires: Arc<Mutex<AHashSet<u64>>> = Arc::new(Mutex::new(AHashSet::new()));
        let notify: Arc<Mutex<HashMap<u64, u64>>> = Arc::new(Mutex::new(HashMap::new()));
        // Pre-populate nodes so reads succeed.
        {
            let mut g = nodes.lock();
            for i in 0..KEYSET_SIZE {
                g.insert(i as u64, i as u64);
            }
        }

        group.bench_function(
            BenchmarkId::new("3_separate_mutexes_5_acquires_per_iter", OPS_PER_ITER),
            |b| {
                b.iter(|| {
                    for i in 0..OPS_PER_ITER {
                        let k = (i % KEYSET_SIZE) as u64;
                        // Acquire 1: read nodes.
                        let _v = {
                            let g = nodes.lock();
                            *g.get(&k).unwrap_or(&0)
                        };
                        // Acquire 2: insert into pending_fires.
                        {
                            let mut g = fires.lock();
                            g.insert(black_box(k));
                        }
                        // Acquire 3: insert into pending_notify.
                        {
                            let mut g = notify.lock();
                            g.insert(black_box(k), black_box(k));
                        }
                        // Acquire 4: cleanup fires.
                        fires.lock().remove(&k);
                        // Acquire 5: cleanup notify.
                        notify.lock().remove(&k);
                    }
                });
            },
        );
    }

    // Variant B: 1 single parking_lot::Mutex<ThreeFields>  (pre-Q2 shape)
    {
        let combined: Arc<Mutex<ThreeFields>> = Arc::new(Mutex::new(ThreeFields {
            nodes: HashMap::new(),
            pending_fires: AHashSet::new(),
            pending_notify: HashMap::new(),
        }));
        {
            let mut g = combined.lock();
            for i in 0..KEYSET_SIZE {
                g.nodes.insert(i as u64, i as u64);
            }
        }

        group.bench_function(BenchmarkId::new("1_combined_mutex", OPS_PER_ITER), |b| {
            b.iter(|| {
                for i in 0..OPS_PER_ITER {
                    let k = (i % KEYSET_SIZE) as u64;
                    let mut g = combined.lock();
                    let _v = *g.nodes.get(&k).unwrap_or(&0);
                    g.pending_fires.insert(black_box(k));
                    g.pending_notify.insert(black_box(k), black_box(k));
                    g.pending_fires.remove(&k);
                    g.pending_notify.remove(&k);
                }
            });
        });
    }

    // Variant C: nodes in mutex, fires + notify in thread_local (Alt-A).
    {
        let nodes: Arc<Mutex<HashMap<u64, u64>>> = Arc::new(Mutex::new(HashMap::new()));
        {
            let mut g = nodes.lock();
            for i in 0..KEYSET_SIZE {
                g.insert(i as u64, i as u64);
            }
        }
        thread_local! {
            static TL_FIRES: RefCell<AHashSet<u64>> = RefCell::new(AHashSet::new());
            static TL_NOTIFY: RefCell<HashMap<u64, u64>> = RefCell::new(HashMap::new());
        }

        group.bench_function(
            BenchmarkId::new("1_mutex_2_thread_locals", OPS_PER_ITER),
            |b| {
                b.iter(|| {
                    for i in 0..OPS_PER_ITER {
                        let k = (i % KEYSET_SIZE) as u64;
                        let _v = {
                            let g = nodes.lock();
                            *g.get(&k).unwrap_or(&0)
                        };
                        TL_FIRES.with(|c| c.borrow_mut().insert(black_box(k)));
                        TL_NOTIFY.with(|c| c.borrow_mut().insert(black_box(k), black_box(k)));
                        TL_FIRES.with(|c| c.borrow_mut().remove(&k));
                        TL_NOTIFY.with(|c| c.borrow_mut().remove(&k));
                    }
                });
            },
        );
    }

    // Variant D: DashMap for nodes, fires + notify in thread_local (Alt-B).
    {
        let nodes: Arc<DashMap<u64, u64>> = Arc::new(DashMap::new());
        for i in 0..KEYSET_SIZE {
            nodes.insert(i as u64, i as u64);
        }
        thread_local! {
            static TL_FIRES_B: RefCell<AHashSet<u64>> = RefCell::new(AHashSet::new());
            static TL_NOTIFY_B: RefCell<HashMap<u64, u64>> = RefCell::new(HashMap::new());
        }

        group.bench_function(
            BenchmarkId::new("dashmap_2_thread_locals", OPS_PER_ITER),
            |b| {
                b.iter(|| {
                    for i in 0..OPS_PER_ITER {
                        let k = (i % KEYSET_SIZE) as u64;
                        let _v = nodes.get(&k).map(|r| *r).unwrap_or(0);
                        TL_FIRES_B.with(|c| c.borrow_mut().insert(black_box(k)));
                        TL_NOTIFY_B.with(|c| c.borrow_mut().insert(black_box(k), black_box(k)));
                        TL_FIRES_B.with(|c| c.borrow_mut().remove(&k));
                        TL_NOTIFY_B.with(|c| c.borrow_mut().remove(&k));
                    }
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Scenario 3: cross-thread disjoint keys
//   2 threads, each insert/remove distinct keys (no overlapping data).
//   Measures cache-line bouncing on the mutex itself even when data is
//   disjoint — the "would per-partition shards help?" test.
// ---------------------------------------------------------------------------

fn bench_s3_cross_thread_disjoint(c: &mut Criterion) {
    let mut group = c.benchmark_group("s3_cross_thread_disjoint_keys");
    group.throughput(Throughput::Elements((2 * OPS_PER_ITER) as u64));

    // Variant A: Single shared parking_lot::Mutex<HashSet>.
    {
        let m: Arc<Mutex<AHashSet<u64>>> = Arc::new(Mutex::new(AHashSet::new()));
        group.bench_function(BenchmarkId::new("shared_mutex", OPS_PER_ITER), |b| {
            b.iter(|| {
                let m_a = m.clone();
                let m_b = m.clone();
                let t_a = thread::spawn(move || {
                    for i in 0..OPS_PER_ITER {
                        let k = (i % KEYSET_SIZE) as u64; // 0..63
                        let mut g = m_a.lock();
                        g.insert(black_box(k));
                        g.remove(&k);
                    }
                });
                let t_b = thread::spawn(move || {
                    for i in 0..OPS_PER_ITER {
                        let k = ((i % KEYSET_SIZE) + 1000) as u64; // 1000..1063
                        let mut g = m_b.lock();
                        g.insert(black_box(k));
                        g.remove(&k);
                    }
                });
                t_a.join().unwrap();
                t_b.join().unwrap();
            });
        });
    }

    // Variant B: Two separate parking_lot::Mutex<HashSet> (per-partition shard).
    {
        let m_a: Arc<Mutex<AHashSet<u64>>> = Arc::new(Mutex::new(AHashSet::new()));
        let m_b: Arc<Mutex<AHashSet<u64>>> = Arc::new(Mutex::new(AHashSet::new()));
        group.bench_function(BenchmarkId::new("per_partition_mutex", OPS_PER_ITER), |b| {
            b.iter(|| {
                let m_a_c = m_a.clone();
                let m_b_c = m_b.clone();
                let t_a = thread::spawn(move || {
                    for i in 0..OPS_PER_ITER {
                        let k = (i % KEYSET_SIZE) as u64;
                        let mut g = m_a_c.lock();
                        g.insert(black_box(k));
                        g.remove(&k);
                    }
                });
                let t_b = thread::spawn(move || {
                    for i in 0..OPS_PER_ITER {
                        let k = ((i % KEYSET_SIZE) + 1000) as u64;
                        let mut g = m_b_c.lock();
                        g.insert(black_box(k));
                        g.remove(&k);
                    }
                });
                t_a.join().unwrap();
                t_b.join().unwrap();
            });
        });
    }

    // Variant C: thread_local on each thread (no shared state at all).
    thread_local! {
        static TL_S3: RefCell<AHashSet<u64>> = RefCell::new(AHashSet::new());
    }
    group.bench_function(BenchmarkId::new("thread_local", OPS_PER_ITER), |b| {
        b.iter(|| {
            let t_a = thread::spawn(|| {
                TL_S3.with(|cell| {
                    let mut s = cell.borrow_mut();
                    for i in 0..OPS_PER_ITER {
                        let k = (i % KEYSET_SIZE) as u64;
                        s.insert(black_box(k));
                        s.remove(&k);
                    }
                });
            });
            let t_b = thread::spawn(|| {
                TL_S3.with(|cell| {
                    let mut s = cell.borrow_mut();
                    for i in 0..OPS_PER_ITER {
                        let k = ((i % KEYSET_SIZE) + 1000) as u64;
                        s.insert(black_box(k));
                        s.remove(&k);
                    }
                });
            });
            t_a.join().unwrap();
            t_b.join().unwrap();
        });
    });

    // Variant D: DashMap (sharded HashMap, but here we use it like a HashSet
    //   via DashMap<u64, ()>).
    {
        let m: Arc<DashMap<u64, ()>> = Arc::new(DashMap::new());
        group.bench_function(BenchmarkId::new("dashmap", OPS_PER_ITER), |b| {
            b.iter(|| {
                let m_a = m.clone();
                let m_b = m.clone();
                let t_a = thread::spawn(move || {
                    for i in 0..OPS_PER_ITER {
                        let k = (i % KEYSET_SIZE) as u64;
                        m_a.insert(black_box(k), ());
                        m_a.remove(&k);
                    }
                });
                let t_b = thread::spawn(move || {
                    for i in 0..OPS_PER_ITER {
                        let k = ((i % KEYSET_SIZE) + 1000) as u64;
                        m_b.insert(black_box(k), ());
                        m_b.remove(&k);
                    }
                });
                t_a.join().unwrap();
                t_b.join().unwrap();
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Scenario 4: cross-thread same key
//   2 threads, same key. Measures TRUE contention — the worst case for any
//   shared mutex. The thread_local variant is omitted here because it
//   doesn't share state across threads (would be measuring something
//   different).
// ---------------------------------------------------------------------------

fn bench_s4_cross_thread_same_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("s4_cross_thread_same_key");
    group.throughput(Throughput::Elements((2 * OPS_PER_ITER) as u64));

    {
        let m: Arc<Mutex<AHashSet<u64>>> = Arc::new(Mutex::new(AHashSet::new()));
        group.bench_function(BenchmarkId::new("shared_mutex", OPS_PER_ITER), |b| {
            b.iter(|| {
                let m_a = m.clone();
                let m_b = m.clone();
                let t_a = thread::spawn(move || {
                    for i in 0..OPS_PER_ITER {
                        let k = (i % KEYSET_SIZE) as u64;
                        let mut g = m_a.lock();
                        g.insert(black_box(k));
                        g.remove(&k);
                    }
                });
                let t_b = thread::spawn(move || {
                    for i in 0..OPS_PER_ITER {
                        let k = (i % KEYSET_SIZE) as u64;
                        let mut g = m_b.lock();
                        g.insert(black_box(k));
                        g.remove(&k);
                    }
                });
                t_a.join().unwrap();
                t_b.join().unwrap();
            });
        });
    }

    {
        let m: Arc<DashMap<u64, ()>> = Arc::new(DashMap::new());
        group.bench_function(BenchmarkId::new("dashmap", OPS_PER_ITER), |b| {
            b.iter(|| {
                let m_a = m.clone();
                let m_b = m.clone();
                let t_a = thread::spawn(move || {
                    for i in 0..OPS_PER_ITER {
                        let k = (i % KEYSET_SIZE) as u64;
                        m_a.insert(black_box(k), ());
                        m_a.remove(&k);
                    }
                });
                let t_b = thread::spawn(move || {
                    for i in 0..OPS_PER_ITER {
                        let k = (i % KEYSET_SIZE) as u64;
                        m_b.insert(black_box(k), ());
                        m_b.remove(&k);
                    }
                });
                t_a.join().unwrap();
                t_b.join().unwrap();
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Scenario 5: registry read-heavy (80% reads, 20% writes)
//   Simulates `nodes.get(node_id)` (the hot read path during cascade) +
//   occasional `nodes.insert(...)` (register) on a HashMap.
//
// Variants:
//   A: Mutex<HashMap>
//   B: RwLock<HashMap>
//   C: DashMap
//
// Single-thread variant first (baseline cost), then 2-thread.
// ---------------------------------------------------------------------------

fn bench_s5_registry_read_heavy(c: &mut Criterion) {
    let mut group = c.benchmark_group("s5_registry_read_heavy");
    group.throughput(Throughput::Elements(OPS_PER_ITER as u64));

    let prepopulate = |k| (k as u64, k as u64 * 100);

    // Single-thread baseline.
    {
        let m: Arc<Mutex<HashMap<u64, u64>>> = Arc::new(Mutex::new(HashMap::new()));
        for k in 0..KEYSET_SIZE {
            let (k, v) = prepopulate(k);
            m.lock().insert(k, v);
        }
        group.bench_function(
            BenchmarkId::new("1t_mutex_hashmap_80r20w", OPS_PER_ITER),
            |b| {
                b.iter(|| {
                    for i in 0..OPS_PER_ITER {
                        let k = (i % KEYSET_SIZE) as u64;
                        if i % 5 == 0 {
                            // Write
                            m.lock().insert(black_box(k), black_box(k * 100));
                        } else {
                            // Read
                            let _v = m.lock().get(&k).copied().unwrap_or(0);
                        }
                    }
                });
            },
        );
    }
    {
        let m: Arc<RwLock<HashMap<u64, u64>>> = Arc::new(RwLock::new(HashMap::new()));
        for k in 0..KEYSET_SIZE {
            let (k, v) = prepopulate(k);
            m.write().insert(k, v);
        }
        group.bench_function(
            BenchmarkId::new("1t_rwlock_hashmap_80r20w", OPS_PER_ITER),
            |b| {
                b.iter(|| {
                    for i in 0..OPS_PER_ITER {
                        let k = (i % KEYSET_SIZE) as u64;
                        if i % 5 == 0 {
                            m.write().insert(black_box(k), black_box(k * 100));
                        } else {
                            let _v = m.read().get(&k).copied().unwrap_or(0);
                        }
                    }
                });
            },
        );
    }
    {
        let m: Arc<DashMap<u64, u64>> = Arc::new(DashMap::new());
        for k in 0..KEYSET_SIZE {
            let (k, v) = prepopulate(k);
            m.insert(k, v);
        }
        group.bench_function(BenchmarkId::new("1t_dashmap_80r20w", OPS_PER_ITER), |b| {
            b.iter(|| {
                for i in 0..OPS_PER_ITER {
                    let k = (i % KEYSET_SIZE) as u64;
                    if i % 5 == 0 {
                        m.insert(black_box(k), black_box(k * 100));
                    } else {
                        let _v = m.get(&k).map(|r| *r).unwrap_or(0);
                    }
                }
            });
        });
    }

    // 2-thread (disjoint key ranges).
    {
        let m: Arc<Mutex<HashMap<u64, u64>>> = Arc::new(Mutex::new(HashMap::new()));
        for k in 0..KEYSET_SIZE * 2 {
            let (k, v) = prepopulate(k);
            m.lock().insert(k, v);
        }
        group.bench_function(
            BenchmarkId::new("2t_mutex_hashmap_80r20w", OPS_PER_ITER),
            |b| {
                b.iter(|| {
                    let m_a = m.clone();
                    let m_b = m.clone();
                    let t_a = thread::spawn(move || {
                        for i in 0..OPS_PER_ITER {
                            let k = (i % KEYSET_SIZE) as u64;
                            if i % 5 == 0 {
                                m_a.lock().insert(black_box(k), black_box(k * 100));
                            } else {
                                let _v = m_a.lock().get(&k).copied().unwrap_or(0);
                            }
                        }
                    });
                    let t_b = thread::spawn(move || {
                        for i in 0..OPS_PER_ITER {
                            let k = ((i % KEYSET_SIZE) + KEYSET_SIZE) as u64;
                            if i % 5 == 0 {
                                m_b.lock().insert(black_box(k), black_box(k * 100));
                            } else {
                                let _v = m_b.lock().get(&k).copied().unwrap_or(0);
                            }
                        }
                    });
                    t_a.join().unwrap();
                    t_b.join().unwrap();
                });
            },
        );
    }
    {
        let m: Arc<RwLock<HashMap<u64, u64>>> = Arc::new(RwLock::new(HashMap::new()));
        for k in 0..KEYSET_SIZE * 2 {
            let (k, v) = prepopulate(k);
            m.write().insert(k, v);
        }
        group.bench_function(
            BenchmarkId::new("2t_rwlock_hashmap_80r20w", OPS_PER_ITER),
            |b| {
                b.iter(|| {
                    let m_a = m.clone();
                    let m_b = m.clone();
                    let t_a = thread::spawn(move || {
                        for i in 0..OPS_PER_ITER {
                            let k = (i % KEYSET_SIZE) as u64;
                            if i % 5 == 0 {
                                m_a.write().insert(black_box(k), black_box(k * 100));
                            } else {
                                let _v = m_a.read().get(&k).copied().unwrap_or(0);
                            }
                        }
                    });
                    let t_b = thread::spawn(move || {
                        for i in 0..OPS_PER_ITER {
                            let k = ((i % KEYSET_SIZE) + KEYSET_SIZE) as u64;
                            if i % 5 == 0 {
                                m_b.write().insert(black_box(k), black_box(k * 100));
                            } else {
                                let _v = m_b.read().get(&k).copied().unwrap_or(0);
                            }
                        }
                    });
                    t_a.join().unwrap();
                    t_b.join().unwrap();
                });
            },
        );
    }
    {
        let m: Arc<DashMap<u64, u64>> = Arc::new(DashMap::new());
        for k in 0..KEYSET_SIZE * 2 {
            let (k, v) = prepopulate(k);
            m.insert(k, v);
        }
        group.bench_function(BenchmarkId::new("2t_dashmap_80r20w", OPS_PER_ITER), |b| {
            b.iter(|| {
                let m_a = m.clone();
                let m_b = m.clone();
                let t_a = thread::spawn(move || {
                    for i in 0..OPS_PER_ITER {
                        let k = (i % KEYSET_SIZE) as u64;
                        if i % 5 == 0 {
                            m_a.insert(black_box(k), black_box(k * 100));
                        } else {
                            let _v = m_a.get(&k).map(|r| *r).unwrap_or(0);
                        }
                    }
                });
                let t_b = thread::spawn(move || {
                    for i in 0..OPS_PER_ITER {
                        let k = ((i % KEYSET_SIZE) + KEYSET_SIZE) as u64;
                        if i % 5 == 0 {
                            m_b.insert(black_box(k), black_box(k * 100));
                        } else {
                            let _v = m_b.get(&k).map(|r| *r).unwrap_or(0);
                        }
                    }
                });
                t_a.join().unwrap();
                t_b.join().unwrap();
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Scenario 6: MPSC push vs Mutex insert (Alt-D simulation)
//   Simulates the cost of "post a fire to a worker's queue" (Tokio-style)
//   vs "insert into a shared pending_fires mutex" (current).
//   Single-thread baseline then 2-producer / 1-consumer.
// ---------------------------------------------------------------------------

fn bench_s6_mpsc_vs_mutex(c: &mut Criterion) {
    let mut group = c.benchmark_group("s6_mpsc_push_vs_mutex_insert");
    group.throughput(Throughput::Elements(OPS_PER_ITER as u64));

    // Variant A: Single-thread MPSC push (cost of crossbeam_channel send).
    {
        let (tx, rx): (Sender<u64>, Receiver<u64>) = unbounded();
        group.bench_function(
            BenchmarkId::new("1t_crossbeam_unbounded", OPS_PER_ITER),
            |b| {
                b.iter(|| {
                    for i in 0..OPS_PER_ITER {
                        let k = (i % KEYSET_SIZE) as u64;
                        tx.send(black_box(k)).unwrap();
                    }
                    // Drain so the queue doesn't grow unbounded across iters.
                    while rx.try_recv().is_ok() {}
                });
            },
        );
    }

    // Variant B: Single-thread Mutex<HashSet> insert.
    {
        let m: Arc<Mutex<AHashSet<u64>>> = Arc::new(Mutex::new(AHashSet::new()));
        group.bench_function(
            BenchmarkId::new("1t_mutex_insert_only", OPS_PER_ITER),
            |b| {
                b.iter(|| {
                    for i in 0..OPS_PER_ITER {
                        let k = (i % KEYSET_SIZE) as u64;
                        m.lock().insert(black_box(k));
                    }
                    m.lock().clear();
                });
            },
        );
    }

    // Variant C: 2 producers MPSC into 1 channel.
    {
        let (tx, rx): (Sender<u64>, Receiver<u64>) = unbounded();
        group.bench_function(
            BenchmarkId::new("2p_crossbeam_unbounded", OPS_PER_ITER),
            |b| {
                b.iter(|| {
                    let tx_a = tx.clone();
                    let tx_b = tx.clone();
                    let t_a = thread::spawn(move || {
                        for i in 0..OPS_PER_ITER {
                            tx_a.send(black_box(i as u64)).unwrap();
                        }
                    });
                    let t_b = thread::spawn(move || {
                        for i in 0..OPS_PER_ITER {
                            tx_b.send(black_box(i as u64)).unwrap();
                        }
                    });
                    t_a.join().unwrap();
                    t_b.join().unwrap();
                    while rx.try_recv().is_ok() {}
                });
            },
        );
    }

    // Variant D: 2 producers Mutex<HashSet> insert.
    {
        let m: Arc<Mutex<AHashSet<u64>>> = Arc::new(Mutex::new(AHashSet::new()));
        group.bench_function(
            BenchmarkId::new("2p_mutex_insert_only", OPS_PER_ITER),
            |b| {
                b.iter(|| {
                    let m_a = m.clone();
                    let m_b = m.clone();
                    let t_a = thread::spawn(move || {
                        for i in 0..OPS_PER_ITER {
                            m_a.lock().insert(black_box(i as u64));
                        }
                    });
                    let t_b = thread::spawn(move || {
                        for i in 0..OPS_PER_ITER {
                            m_b.lock().insert(black_box((OPS_PER_ITER + i) as u64));
                        }
                    });
                    t_a.join().unwrap();
                    t_b.join().unwrap();
                    m.lock().clear();
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Scenario 7: dynamic topology — interleave registry insert + node read
//   Simulates Core::register (insert into nodes) interleaved with cascade
//   reads (get from nodes). Tests how each registry primitive handles
//   write contention during a workload that's mostly reads.
// ---------------------------------------------------------------------------

fn bench_s7_dynamic_topology(c: &mut Criterion) {
    let mut group = c.benchmark_group("s7_dynamic_topology_register_plus_read");
    group.throughput(Throughput::Elements(OPS_PER_ITER as u64));

    // 90% reads, 10% inserts of NEW keys (simulating register).
    // 2 threads.
    {
        let m: Arc<Mutex<HashMap<u64, u64>>> = Arc::new(Mutex::new(HashMap::new()));
        // Pre-populate so reads succeed.
        for k in 0..KEYSET_SIZE {
            m.lock().insert(k as u64, k as u64);
        }
        let next_id = Arc::new(std::sync::atomic::AtomicU64::new(1000));
        group.bench_function(
            BenchmarkId::new("2t_mutex_register_read", OPS_PER_ITER),
            |b| {
                b.iter(|| {
                    let m_a = m.clone();
                    let m_b = m.clone();
                    let n_a = next_id.clone();
                    let n_b = next_id.clone();
                    let t_a = thread::spawn(move || {
                        for i in 0..OPS_PER_ITER {
                            if i % 10 == 0 {
                                let k = n_a.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                m_a.lock().insert(black_box(k), black_box(k));
                            } else {
                                let k = (i % KEYSET_SIZE) as u64;
                                let _v = m_a.lock().get(&k).copied().unwrap_or(0);
                            }
                        }
                    });
                    let t_b = thread::spawn(move || {
                        for i in 0..OPS_PER_ITER {
                            if i % 10 == 0 {
                                let k = n_b.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                m_b.lock().insert(black_box(k), black_box(k));
                            } else {
                                let k = (i % KEYSET_SIZE) as u64;
                                let _v = m_b.lock().get(&k).copied().unwrap_or(0);
                            }
                        }
                    });
                    t_a.join().unwrap();
                    t_b.join().unwrap();
                });
            },
        );
    }

    {
        let m: Arc<RwLock<HashMap<u64, u64>>> = Arc::new(RwLock::new(HashMap::new()));
        for k in 0..KEYSET_SIZE {
            m.write().insert(k as u64, k as u64);
        }
        let next_id = Arc::new(std::sync::atomic::AtomicU64::new(1000));
        group.bench_function(
            BenchmarkId::new("2t_rwlock_register_read", OPS_PER_ITER),
            |b| {
                b.iter(|| {
                    let m_a = m.clone();
                    let m_b = m.clone();
                    let n_a = next_id.clone();
                    let n_b = next_id.clone();
                    let t_a = thread::spawn(move || {
                        for i in 0..OPS_PER_ITER {
                            if i % 10 == 0 {
                                let k = n_a.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                m_a.write().insert(black_box(k), black_box(k));
                            } else {
                                let k = (i % KEYSET_SIZE) as u64;
                                let _v = m_a.read().get(&k).copied().unwrap_or(0);
                            }
                        }
                    });
                    let t_b = thread::spawn(move || {
                        for i in 0..OPS_PER_ITER {
                            if i % 10 == 0 {
                                let k = n_b.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                m_b.write().insert(black_box(k), black_box(k));
                            } else {
                                let k = (i % KEYSET_SIZE) as u64;
                                let _v = m_b.read().get(&k).copied().unwrap_or(0);
                            }
                        }
                    });
                    t_a.join().unwrap();
                    t_b.join().unwrap();
                });
            },
        );
    }

    {
        let m: Arc<DashMap<u64, u64>> = Arc::new(DashMap::new());
        for k in 0..KEYSET_SIZE {
            m.insert(k as u64, k as u64);
        }
        let next_id = Arc::new(std::sync::atomic::AtomicU64::new(1000));
        group.bench_function(
            BenchmarkId::new("2t_dashmap_register_read", OPS_PER_ITER),
            |b| {
                b.iter(|| {
                    let m_a = m.clone();
                    let m_b = m.clone();
                    let n_a = next_id.clone();
                    let n_b = next_id.clone();
                    let t_a = thread::spawn(move || {
                        for i in 0..OPS_PER_ITER {
                            if i % 10 == 0 {
                                let k = n_a.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                m_a.insert(black_box(k), black_box(k));
                            } else {
                                let k = (i % KEYSET_SIZE) as u64;
                                let _v = m_a.get(&k).map(|r| *r).unwrap_or(0);
                            }
                        }
                    });
                    let t_b = thread::spawn(move || {
                        for i in 0..OPS_PER_ITER {
                            if i % 10 == 0 {
                                let k = n_b.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                m_b.insert(black_box(k), black_box(k));
                            } else {
                                let k = (i % KEYSET_SIZE) as u64;
                                let _v = m_b.get(&k).map(|r| *r).unwrap_or(0);
                            }
                        }
                    });
                    t_a.join().unwrap();
                    t_b.join().unwrap();
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Scenario 8: dispatcher-faithful fire_regular simulation
//
// Simulates the actual Core::fire_regular hot path more faithfully than
// the isolated-primitive scenarios. Each "fire" iteration:
//   1. Acquire state lock (or shard lock).
//   2. Phase 1: read 5 fields from the NodeRecord (snapshot inputs).
//   3. Drop lock (lock-released invoke_fn).
//   4. Simulate ~3µs of CPU work (calibrated counted spin).
//   5. Re-acquire lock.
//   6. Phase 3: write 2 fields (cache + dirty).
//   7. Drop lock.
//
// 2 threads on disjoint state nodes (disjoint partitions).
//
// Variants:
//   A:  Shared Mutex<CoreStateApprox>           — current architecture post-sub-slices-1-3.
//   B:  Per-partition Mutex<ShardApprox> ALONE  — MISLEADING (eliminates state mutex; not what slice 4 does).
//   B': HONEST state.lock() + shard.lock()      — what a structural-only sub-slice 4 actually looks like.
//   C:  Single-thread serial baseline.
//
// **WARNING — read this before drawing conclusions from this bench.**
//
// Variant B (`2t_disjoint_per_partition_shards`) eliminates the state
// mutex ENTIRELY — only the per-partition shard is acquired. That's
// NOT what a structural sub-slice 4 migration would do. The dispatcher's
// fire paths still need state.lock() because state holds `s.children`,
// ID counters, topology_sinks, config (these stay on CoreState in any
// minimum-viable shard migration).
//
// Variant B' (`2t_disjoint_HONEST_state_plus_shard`) is the apples-to-
// apples comparison: state.lock() acquired briefly for non-nodes fields,
// shard.lock() acquired separately for nodes access. Both held. This is
// what sub-slice 4 actually delivers structurally.
//
// **Bench results (2026-05-10):**
//   A:  2.74 ms  (baseline — current architecture)
//   B': 2.50 ms  (~9% improvement — what sub-slice 4 would actually deliver)
//   B:  2.21 ms  (~19% improvement — misleading; only achievable via hot-path
//                 lock-model refactor that drops state.lock() before shard.lock())
//   C:  4.22 ms  (serial baseline)
//
// **For sub-slice 4 evaluation: USE B'.** D113 (2026-05-10) tightened the
// lift-trigger for sub-slice 4 to require profiler-identified evidence
// of state-mutex contention on the real dispatcher, not bench projections
// alone. See `docs/porting-deferred.md` "Per-partition `nodes`/`children`
// shards (Q-beyond Sub-slice 4)" for the full analysis.
//
// **/qa F9+F10 caveats (2026-05-10) — bench under-counts in BOTH directions:**
//
// - **Variant B' under-counts state-mutex contention.** `StateApproxNoNodes`
//   has `_children` + `_next_id` + `_padding` but neither thread *touches*
//   them — state.lock() is acquired and released around no work. In the
//   real dispatcher, state.lock() co-occurs with substantive `s.children`
//   walks (in `compute_touched_partitions`), ID counter bumps, and
//   topology_sinks reads. Real cross-thread bouncing on state would be
//   higher than this bench measures → real Variant B' gain could exceed
//   9% (more state-mutex contention to escape). Could go the other way too.
// - **Variant A under-counts the per-emit thread_local borrow.** Variant A
//   models the current architecture's state mutex but doesn't simulate the
//   per-emit `WAVE_STATE.borrow_mut()` calls that sub-slices 1-3 added.
//   Per S1, those borrows are ~14ns/op uncontended (negligible) but they
//   ARE additional work the real dispatcher pays. Variant A's measured
//   time is artificially low → A vs B/B' ratio is slightly inflated.
// - The two under-counts are SYMMETRIC across A/B/B' (all three are bench
//   approximations of the real dispatcher), so the **decision based on the
//   bench (defer per D113) is robust either way**. The 9% projection is a
//   directional estimate, not a tight upper bound.
// ---------------------------------------------------------------------------

const SPIN_ITERS_PER_FIRE: u32 = 1_500;

struct CoreStateApprox {
    nodes: HashMap<u64, NodeRecordApprox>,
    // Approximate the rest of CoreState's fields with some bytes to make the
    // mutex's cache-line footprint realistic. CoreState in production is a
    // large struct; this stub is small but the mutex itself is what dominates
    // the bouncing.
    _padding: [u64; 4],
}

struct NodeRecordApprox {
    fn_id: u64,
    kind_byte: u8,
    cache: u64,
    dirty: bool,
    has_fired_once: bool,
    // `dep_count` is bench-fixture padding to make NodeRecordApprox a
    // realistic size — the bench reads 5 fields from the record (fn_id,
    // kind_byte, cache, dirty, has_fired_once) but doesn't read this one.
    // Kept as a struct member so the cache footprint matches the real
    // NodeRecord more faithfully.
    #[allow(dead_code)]
    dep_count: u32,
}

#[inline(never)]
fn calibrated_work() {
    // Counted spin — replaces Instant::elapsed() to avoid clock_gettime
    // overhead dominating the work simulation (per Phase J QA learning).
    for _ in 0..SPIN_ITERS_PER_FIRE {
        std::hint::black_box(0u64);
    }
}

fn bench_s8_fire_regular_simulation(c: &mut Criterion) {
    let mut group = c.benchmark_group("s8_fire_regular_simulation");
    group.throughput(Throughput::Elements((2 * OPS_PER_ITER) as u64));

    // Pre-populate node records for both threads.
    let init_nodes = |start: u64| -> HashMap<u64, NodeRecordApprox> {
        let mut nodes = HashMap::new();
        for k in start..start + KEYSET_SIZE as u64 {
            nodes.insert(
                k,
                NodeRecordApprox {
                    fn_id: k * 7,
                    kind_byte: 1,
                    cache: k,
                    dirty: false,
                    has_fired_once: true,
                    dep_count: 2,
                },
            );
        }
        nodes
    };

    // ---------------------------------------------------------------------
    // Variant A: Shared Mutex<CoreStateApprox>  (current architecture)
    // ---------------------------------------------------------------------
    {
        let mut all_nodes = init_nodes(0);
        all_nodes.extend(init_nodes(1000));
        let state: Arc<Mutex<CoreStateApprox>> = Arc::new(Mutex::new(CoreStateApprox {
            nodes: all_nodes,
            _padding: [0; 4],
        }));

        group.bench_function(
            BenchmarkId::new("2t_disjoint_shared_state_mutex", OPS_PER_ITER),
            |b| {
                b.iter(|| {
                    let s_a = state.clone();
                    let s_b = state.clone();
                    let t_a = thread::spawn(move || {
                        for i in 0..OPS_PER_ITER {
                            let k = (i % KEYSET_SIZE) as u64;
                            // Phase 1: lock + read 5 fields.
                            let (fn_id, _kind, _cache, _dirty, _has_fired) = {
                                let g = s_a.lock();
                                let r = g.nodes.get(&k).unwrap();
                                (r.fn_id, r.kind_byte, r.cache, r.dirty, r.has_fired_once)
                            };
                            black_box(fn_id);
                            // Phase 2: lock-released work (binding callback simulation).
                            calibrated_work();
                            // Phase 3: lock + write 2 fields.
                            {
                                let mut g = s_a.lock();
                                let r = g.nodes.get_mut(&k).unwrap();
                                r.cache = black_box(k + 1);
                                r.dirty = true;
                            }
                        }
                    });
                    let t_b = thread::spawn(move || {
                        for i in 0..OPS_PER_ITER {
                            let k = ((i % KEYSET_SIZE) as u64) + 1000;
                            let (fn_id, _kind, _cache, _dirty, _has_fired) = {
                                let g = s_b.lock();
                                let r = g.nodes.get(&k).unwrap();
                                (r.fn_id, r.kind_byte, r.cache, r.dirty, r.has_fired_once)
                            };
                            black_box(fn_id);
                            calibrated_work();
                            {
                                let mut g = s_b.lock();
                                let r = g.nodes.get_mut(&k).unwrap();
                                r.cache = black_box(k + 1);
                                r.dirty = true;
                            }
                        }
                    });
                    t_a.join().unwrap();
                    t_b.join().unwrap();
                });
            },
        );
    }

    // ---------------------------------------------------------------------
    // Variant B: Per-partition Mutex<ShardApprox>  (sub-slice 4 architecture)
    // ---------------------------------------------------------------------
    {
        let shard_a: Arc<Mutex<HashMap<u64, NodeRecordApprox>>> =
            Arc::new(Mutex::new(init_nodes(0)));
        let shard_b: Arc<Mutex<HashMap<u64, NodeRecordApprox>>> =
            Arc::new(Mutex::new(init_nodes(1000)));

        group.bench_function(
            BenchmarkId::new("2t_disjoint_per_partition_shards", OPS_PER_ITER),
            |b| {
                b.iter(|| {
                    let s_a = shard_a.clone();
                    let s_b = shard_b.clone();
                    let t_a = thread::spawn(move || {
                        for i in 0..OPS_PER_ITER {
                            let k = (i % KEYSET_SIZE) as u64;
                            let (fn_id, _kind, _cache, _dirty, _has_fired) = {
                                let g = s_a.lock();
                                let r = g.get(&k).unwrap();
                                (r.fn_id, r.kind_byte, r.cache, r.dirty, r.has_fired_once)
                            };
                            black_box(fn_id);
                            calibrated_work();
                            {
                                let mut g = s_a.lock();
                                let r = g.get_mut(&k).unwrap();
                                r.cache = black_box(k + 1);
                                r.dirty = true;
                            }
                        }
                    });
                    let t_b = thread::spawn(move || {
                        for i in 0..OPS_PER_ITER {
                            let k = ((i % KEYSET_SIZE) as u64) + 1000;
                            let (fn_id, _kind, _cache, _dirty, _has_fired) = {
                                let g = s_b.lock();
                                let r = g.get(&k).unwrap();
                                (r.fn_id, r.kind_byte, r.cache, r.dirty, r.has_fired_once)
                            };
                            black_box(fn_id);
                            calibrated_work();
                            {
                                let mut g = s_b.lock();
                                let r = g.get_mut(&k).unwrap();
                                r.cache = black_box(k + 1);
                                r.dirty = true;
                            }
                        }
                    });
                    t_a.join().unwrap();
                    t_b.join().unwrap();
                });
            },
        );
    }

    // ---------------------------------------------------------------------
    // Variant B': HONEST per-partition shards — keeps state mutex for the
    // non-nodes fields (children, topology_sinks, ID counters, config),
    // adds per-partition shard for nodes. This is what a STRUCTURAL-ONLY
    // sub-slice 4 would actually look like — state.lock() is still acquired
    // because the dispatcher's fire paths read s.children + ID counters etc.
    // alongside nodes.
    //
    // The original Variant B was misleading because it eliminated state
    // mutex entirely. That speedup is only achievable via a HOT-PATH
    // LOCK-MODEL REFACTOR (drop state.lock() before acquiring shard.lock())
    // — outside the "structural-only" envelope.
    // ---------------------------------------------------------------------
    {
        struct StateApproxNoNodes {
            // Keeps the "rest of CoreState" — what stays after sub-slice 4.
            _children: HashMap<u64, Vec<u64>>,
            _next_id: u64,
            _padding: [u64; 4],
        }
        let state: Arc<Mutex<StateApproxNoNodes>> = Arc::new(Mutex::new(StateApproxNoNodes {
            _children: HashMap::new(),
            _next_id: 1,
            _padding: [0; 4],
        }));
        let shard_a: Arc<Mutex<HashMap<u64, NodeRecordApprox>>> =
            Arc::new(Mutex::new(init_nodes(0)));
        let shard_b: Arc<Mutex<HashMap<u64, NodeRecordApprox>>> =
            Arc::new(Mutex::new(init_nodes(1000)));

        group.bench_function(
            BenchmarkId::new("2t_disjoint_HONEST_state_plus_shard", OPS_PER_ITER),
            |b| {
                b.iter(|| {
                    let st_a = state.clone();
                    let st_b = state.clone();
                    let sh_a = shard_a.clone();
                    let sh_b = shard_b.clone();
                    let t_a = thread::spawn(move || {
                        for i in 0..OPS_PER_ITER {
                            let k = (i % KEYSET_SIZE) as u64;
                            // Phase 1: lock state (for children/ID reads)
                            // + lock shard (for node access). Both held
                            // during the snapshot — same as the dispatcher
                            // would do today if structurally migrated.
                            let (fn_id, _kind, _cache, _dirty, _has_fired) = {
                                let st = st_a.lock();
                                let sh = sh_a.lock();
                                let r = sh.get(&k).unwrap();
                                let v = (r.fn_id, r.kind_byte, r.cache, r.dirty, r.has_fired_once);
                                drop(st);
                                v
                            };
                            black_box(fn_id);
                            calibrated_work();
                            // Phase 3: same pattern.
                            {
                                let st = st_a.lock();
                                let mut sh = sh_a.lock();
                                let r = sh.get_mut(&k).unwrap();
                                r.cache = black_box(k + 1);
                                r.dirty = true;
                                drop(st);
                            }
                        }
                    });
                    let t_b = thread::spawn(move || {
                        for i in 0..OPS_PER_ITER {
                            let k = ((i % KEYSET_SIZE) as u64) + 1000;
                            let (fn_id, _kind, _cache, _dirty, _has_fired) = {
                                let st = st_b.lock();
                                let sh = sh_b.lock();
                                let r = sh.get(&k).unwrap();
                                let v = (r.fn_id, r.kind_byte, r.cache, r.dirty, r.has_fired_once);
                                drop(st);
                                v
                            };
                            black_box(fn_id);
                            calibrated_work();
                            {
                                let st = st_b.lock();
                                let mut sh = sh_b.lock();
                                let r = sh.get_mut(&k).unwrap();
                                r.cache = black_box(k + 1);
                                r.dirty = true;
                                drop(st);
                            }
                        }
                    });
                    t_a.join().unwrap();
                    t_b.join().unwrap();
                });
            },
        );
    }

    // ---------------------------------------------------------------------
    // Variant C: Single-thread baseline for serial divisor.
    // ---------------------------------------------------------------------
    {
        let state: Arc<Mutex<CoreStateApprox>> = Arc::new(Mutex::new(CoreStateApprox {
            nodes: init_nodes(0),
            _padding: [0; 4],
        }));

        group.bench_function(BenchmarkId::new("serial_baseline_2N", OPS_PER_ITER), |b| {
            b.iter(|| {
                for i in 0..(2 * OPS_PER_ITER) {
                    let k = (i % KEYSET_SIZE) as u64;
                    let (fn_id, _kind, _cache, _dirty, _has_fired) = {
                        let g = state.lock();
                        let r = g.nodes.get(&k).unwrap();
                        (r.fn_id, r.kind_byte, r.cache, r.dirty, r.has_fired_once)
                    };
                    black_box(fn_id);
                    calibrated_work();
                    {
                        let mut g = state.lock();
                        let r = g.nodes.get_mut(&k).unwrap();
                        r.cache = black_box(k + 1);
                        r.dirty = true;
                    }
                }
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_s1_single_thread,
    bench_s2_multi_mutex_hop,
    bench_s3_cross_thread_disjoint,
    bench_s4_cross_thread_same_key,
    bench_s5_registry_read_heavy,
    bench_s6_mpsc_vs_mutex,
    bench_s7_dynamic_topology,
    bench_s8_fire_regular_simulation,
);
criterion_main!(benches);
