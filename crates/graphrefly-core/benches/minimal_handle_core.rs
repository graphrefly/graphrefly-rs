//! THROWAWAY (2026-05-16) — minimal SINGLE-THREADED handle-core, a
//! faithful Rust mirror of the pure-ts prototype
//! `packages/pure-ts/src/__experiments__/handle-core/{core,bindings}.ts`.
//!
//! Purpose: isolate "Rust the language" from "the production dispatcher's
//! thread-safety + D3 partition machinery tax". Full protocol (intern +
//! refcount, registerDerived, children, subscribe+recursive-activate,
//! deliverDataToConsumer, pendingFires drain w/ topo gate, fireFn
//! first-run gate, queueNotify/flush, per-wave reset) — but ZERO locks,
//! ZERO Arc, ZERO union-find registry, ZERO BatchGuard. Plain `&mut self`.
//!
//! Scenarios mirror `handle-core.bench.ts` 1:1 so numbers compare to the
//! pure-ts prototype AND to production `graphrefly-core`.
//!
//! Run: `cargo bench -p graphrefly-core --bench minimal_handle_core`

// §7 floor-regression harness (D208–D211): a hand-written single-threaded
// mirror, not production code — dead-field / unused-method / manual-contains
// lints are expected and intentional in a measurement scaffold.
#![allow(dead_code, clippy::manual_contains, clippy::pedantic)]

use std::collections::{HashMap, HashSet};

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

type NodeId = u32;
type HandleId = u64;
const NO_HANDLE: HandleId = 0;

#[derive(Clone, Copy)]
enum Msg {
    Dirty,
    Data(HandleId),
    Resolved,
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    State,
    Derived,
}

#[derive(Clone, Copy)]
enum FnKind {
    Passthrough, // v => v   (identity passthrough; chain/fanout/diamond-inner)
    Sum,         // (...vs) => sum  (diamond sink)
}

struct Rec {
    id: NodeId,
    deps: Vec<NodeId>,
    kind: Kind,
    fnk: FnKind,
    cache: HandleId,
    dep_handles: Vec<HandleId>,
    has_fired_once: bool,
    sub_count: usize,
    dirty: bool,
    involved: bool,
}

/// Mirrors bindings.ts intern (primitive dedup + refcount) + a value
/// table so invokeFn can deref→fn→intern exactly like HandleRuntime.
struct Registry {
    prim: HashMap<i64, HandleId>,
    val: HashMap<HandleId, i64>,
    refc: HashMap<HandleId, u32>,
    next: HandleId,
}
impl Registry {
    fn new() -> Self {
        Self {
            prim: HashMap::new(),
            val: HashMap::new(),
            refc: HashMap::new(),
            next: 1,
        }
    }
    fn intern(&mut self, v: i64) -> HandleId {
        if let Some(&h) = self.prim.get(&v) {
            *self.refc.entry(h).or_insert(0) += 1;
            return h;
        }
        let id = self.next;
        self.next += 1;
        self.prim.insert(v, id);
        self.val.insert(id, v);
        self.refc.insert(id, 1);
        id
    }
    fn deref(&self, h: HandleId) -> i64 {
        *self.val.get(&h).unwrap()
    }
}

struct Core {
    nodes: HashMap<NodeId, Rec>,
    children: HashMap<NodeId, HashSet<NodeId>>,
    next_id: NodeId,
    in_tick: bool,
    pending_fires: HashSet<NodeId>,
    pending_notify: Vec<(NodeId, Msg)>,
    sink_calls: u64,
}
impl Core {
    fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            children: HashMap::new(),
            next_id: 1,
            in_tick: false,
            pending_fires: HashSet::new(),
            pending_notify: Vec::new(),
            sink_calls: 0,
        }
    }

    fn register_state(&mut self, init: HandleId) -> NodeId {
        let id = self.next_id;
        self.next_id += 1;
        self.nodes.insert(
            id,
            Rec {
                id,
                deps: vec![],
                kind: Kind::State,
                fnk: FnKind::Passthrough,
                cache: init,
                dep_handles: vec![],
                has_fired_once: init != NO_HANDLE,
                sub_count: 0,
                dirty: false,
                involved: false,
            },
        );
        self.children.insert(id, HashSet::new());
        id
    }

    fn register_derived(&mut self, deps: &[NodeId], fnk: FnKind) -> NodeId {
        let id = self.next_id;
        self.next_id += 1;
        self.nodes.insert(
            id,
            Rec {
                id,
                deps: deps.to_vec(),
                kind: Kind::Derived,
                fnk,
                cache: NO_HANDLE,
                dep_handles: vec![NO_HANDLE; deps.len()],
                has_fired_once: false,
                sub_count: 0,
                dirty: false,
                involved: false,
            },
        );
        self.children.insert(id, HashSet::new());
        for &d in deps {
            self.children.get_mut(&d).unwrap().insert(id);
        }
        id
    }

    // Setup-time only (not in the measured hot loop) — recursive activate.
    fn subscribe(&mut self, reg: &Registry, id: NodeId) {
        self.nodes.get_mut(&id).unwrap().sub_count += 1;
        let (kind, subs) = {
            let r = &self.nodes[&id];
            (r.kind, r.sub_count)
        };
        if kind != Kind::State && subs == 1 {
            self.activate(reg, id);
        }
    }

    fn activate(&mut self, reg: &Registry, id: NodeId) {
        let outer = !self.in_tick;
        if outer {
            self.in_tick = true;
        }
        let deps = self.nodes[&id].deps.clone();
        for (i, dep) in deps.iter().enumerate() {
            let (dkind, dcache, dfired, dsubs) = {
                let d = &self.nodes[dep];
                (d.kind, d.cache, d.has_fired_once, d.sub_count)
            };
            if dkind != Kind::State && dcache == NO_HANDLE && !dfired {
                self.nodes.get_mut(dep).unwrap().sub_count += 1;
                if dsubs + 1 == 1 {
                    self.activate(reg, *dep);
                }
            }
            let dcache = self.nodes[dep].cache;
            if dcache != NO_HANDLE {
                self.deliver(*dep, id, i, dcache);
            }
        }
        if outer {
            self.drain(reg);
            self.flush();
            for r in self.nodes.values_mut() {
                r.dirty = false;
                r.involved = false;
            }
            self.in_tick = false;
        }
    }

    #[inline]
    fn require_cache_equals(&self, id: NodeId) -> HandleId {
        self.nodes[&id].cache
    }

    fn emit(&mut self, reg: &Registry, id: NodeId, h: HandleId) {
        assert!(h != NO_HANDLE);
        if self.in_tick {
            self.commit(id, h);
            return;
        }
        self.in_tick = true;
        self.commit(id, h);
        self.drain(reg);
        self.flush();
        for r in self.nodes.values_mut() {
            r.dirty = false;
            r.involved = false;
        }
        self.in_tick = false;
    }

    fn pick_next_fire(&self) -> Option<NodeId> {
        for &id in &self.pending_fires {
            let r = &self.nodes[&id];
            if r.deps.iter().all(|d| !self.pending_fires.contains(d)) {
                return Some(id);
            }
        }
        self.pending_fires.iter().next().copied()
    }

    fn drain(&mut self, reg: &Registry) {
        let mut guard = 0;
        while !self.pending_fires.is_empty() {
            guard += 1;
            assert!(guard <= 100_000, "wave drain cycle");
            let Some(next) = self.pick_next_fire() else {
                break;
            };
            self.fire_fn(reg, next);
        }
    }

    fn fire_fn(&mut self, reg: &Registry, id: NodeId) {
        self.pending_fires.remove(&id);
        let (fnk, dep_handles) = {
            let r = &self.nodes[&id];
            // first-run gate: every dep must have a handle.
            if r.dep_handles.iter().any(|&h| h == NO_HANDLE) {
                return;
            }
            (r.fnk, r.dep_handles.clone())
        };
        // invokeFn: deref → fn → intern, exactly like HandleRuntime.
        let result: HandleId = match fnk {
            FnKind::Passthrough => dep_handles[0], // v=>v interns back to same handle
            FnKind::Sum => {
                // fresh value each wave → fresh interned handle
                let s: i64 = dep_handles.iter().map(|&h| reg.deref(h)).sum();
                // mirror intern() read path without &mut: precomputed table
                PRECOMPUTED_SUM_HANDLE.with(|c| {
                    let mut m = c.borrow_mut();
                    let n = m.len() as HandleId + 1_000_000;
                    *m.entry(s).or_insert(n)
                })
            }
        };
        self.nodes.get_mut(&id).unwrap().has_fired_once = true;
        self.commit(id, result);
    }

    fn commit(&mut self, id: NodeId, new: HandleId) {
        let old = self.nodes[&id].cache;
        let is_data = old != new; // identity equals-substitution
        {
            let r = self.nodes.get_mut(&id).unwrap();
            if !r.dirty {
                r.dirty = true;
                if r.sub_count > 0 {
                    self.pending_notify.push((id, Msg::Dirty));
                }
            }
        }
        if is_data {
            self.nodes.get_mut(&id).unwrap().cache = new;
            if self.nodes[&id].sub_count > 0 {
                self.pending_notify.push((id, Msg::Data(new)));
            }
            let kids: Vec<NodeId> = self.children[&id].iter().copied().collect();
            for c in kids {
                let idx = self.nodes[&c].deps.iter().position(|&d| d == id);
                if let Some(i) = idx {
                    self.deliver(id, c, i, new);
                }
            }
        } else {
            if self.nodes[&id].sub_count > 0 {
                self.pending_notify.push((id, Msg::Resolved));
            }
            let kids: Vec<NodeId> = self.children[&id].iter().copied().collect();
            for c in kids {
                if !self.nodes[&c].involved {
                    let r = self.nodes.get_mut(&c).unwrap();
                    r.involved = true;
                    r.dirty = true;
                    if r.sub_count > 0 {
                        self.pending_notify.push((c, Msg::Dirty));
                        self.pending_notify.push((c, Msg::Resolved));
                    }
                }
            }
        }
    }

    fn deliver(&mut self, _dep: NodeId, consumer: NodeId, dep_idx: usize, handle: HandleId) {
        let r = self.nodes.get_mut(&consumer).unwrap();
        r.dep_handles[dep_idx] = handle;
        r.involved = true;
        if r.kind == Kind::Derived {
            self.pending_fires.insert(consumer);
        }
    }

    fn flush(&mut self) {
        let drained = std::mem::take(&mut self.pending_notify);
        for (nid, msg) in &drained {
            let subs = self.nodes.get(nid).map(|r| r.sub_count).unwrap_or(0);
            for _ in 0..subs {
                self.sink_calls += match msg {
                    Msg::Dirty => 1,
                    Msg::Data(_) => 2,
                    Msg::Resolved => 3,
                };
            }
        }
        self.pending_notify = drained;
        self.pending_notify.clear();
    }
}

thread_local! {
    static PRECOMPUTED_SUM_HANDLE: std::cell::RefCell<HashMap<i64, HandleId>> =
        std::cell::RefCell::new(HashMap::new());
}

const OPS: usize = 1_000;

fn bench_minimal(c: &mut Criterion) {
    let mut g = c.benchmark_group("minimal_handle_core");
    g.throughput(Throughput::Elements(OPS as u64));

    // state_emit_identity_dedup
    {
        let mut reg = Registry::new();
        let mut core = Core::new();
        let h0 = reg.intern(1);
        let s = core.register_state(h0);
        core.subscribe(&reg, s);
        g.bench_function(BenchmarkId::new("emit_same_handle", OPS), |b| {
            b.iter(|| {
                for _ in 0..OPS {
                    let h = reg.intern(black_box(1));
                    core.emit(&reg, black_box(s), h);
                }
            });
            black_box(core.sink_calls);
        });
    }

    // state_emit_changing_value
    {
        let mut reg = Registry::new();
        let mut core = Core::new();
        let s = core.register_state(NO_HANDLE);
        core.subscribe(&reg, s);
        let mut v: i64 = 0;
        g.bench_function(BenchmarkId::new("emit_fresh_handle_each", OPS), |b| {
            b.iter(|| {
                for _ in 0..OPS {
                    v += 1;
                    let h = reg.intern(black_box(v));
                    core.emit(&reg, black_box(s), h);
                }
            });
            black_box(core.sink_calls);
        });
    }

    // chain_propagation/N
    for n in [1usize, 4, 16, 64] {
        let mut reg = Registry::new();
        let mut core = Core::new();
        let s = core.register_state(reg.intern(0));
        let mut prev = s;
        for _ in 0..n {
            prev = core.register_derived(&[prev], FnKind::Passthrough);
        }
        core.subscribe(&reg, prev);
        let mut i: i64 = 0;
        g.bench_function(BenchmarkId::new("chain", n), |b| {
            b.iter(|| {
                for _ in 0..OPS {
                    i += 1;
                    let h = reg.intern(black_box(i));
                    core.emit(&reg, s, h);
                }
            });
            black_box(core.sink_calls);
        });
    }

    // diamond_fanout/N
    for n in [2usize, 8, 32] {
        let mut reg = Registry::new();
        let mut core = Core::new();
        let s = core.register_state(reg.intern(0));
        let inner: Vec<NodeId> = (0..n)
            .map(|_| core.register_derived(&[s], FnKind::Passthrough))
            .collect();
        let sink = core.register_derived(&inner, FnKind::Sum);
        core.subscribe(&reg, sink);
        let mut i: i64 = 0;
        g.bench_function(BenchmarkId::new("diamond", n), |b| {
            b.iter(|| {
                for _ in 0..OPS {
                    i += 1;
                    let h = reg.intern(black_box(i));
                    core.emit(&reg, s, h);
                }
            });
            black_box(core.sink_calls);
        });
    }

    // large_fanout/N
    for n in [10usize, 100, 1000] {
        let mut reg = Registry::new();
        let mut core = Core::new();
        let s = core.register_state(reg.intern(0));
        for _ in 0..n {
            let leaf = core.register_derived(&[s], FnKind::Passthrough);
            core.subscribe(&reg, leaf);
        }
        let mut j: i64 = 0;
        g.bench_function(BenchmarkId::new("fanout", n), |b| {
            b.iter(|| {
                for _ in 0..OPS {
                    j += 1;
                    let h = reg.intern(black_box(j));
                    core.emit(&reg, s, h);
                }
            });
            black_box(core.sink_calls);
        });
    }

    g.finish();
}

criterion_group!(benches, bench_minimal);
criterion_main!(benches);
