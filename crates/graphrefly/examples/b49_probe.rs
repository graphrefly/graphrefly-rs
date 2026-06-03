//! B49 implementation-shape probe (Rust, clean-slate).
//!
//! Focus: bookkeeping/frontier-adjacent costs while preserving protocol behavior.
//! This is a runtime probe (not a conformance test).
//!
//! Run:
//!   mise exec -- cargo run --release --example b49_probe
//! or fallback:
//!   ~/.cargo/bin/cargo run --release --example b49_probe

use std::cell::Cell;
use std::collections::VecDeque;
use std::hint::black_box;
use std::rc::Rc;
use std::time::Instant;

use graphrefly::{AnyValue, Ctx, Message, Node};

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    v[v.len() / 2]
}

fn time_ms(runs: usize, mut f: impl FnMut()) -> f64 {
    f(); // warmup
    let mut s = Vec::with_capacity(runs);
    for _ in 0..runs {
        let t = Instant::now();
        f();
        s.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    median(s)
}

fn ns_per_op(ms: f64, ops: usize) -> f64 {
    (ms * 1_000_000.0) / (ops.max(1) as f64)
}

fn typed_vs_erased_probe(hops: usize, waves: usize, protocol_waves: usize, runs: usize) {
    println!(
        "--- Probe E: typed vs erased value path ({hops} hops x {waves} waves; protocol {protocol_waves} waves) ---"
    );
    let typed_values: Vec<Rc<f64>> = (0..hops).map(|i| Rc::new(i as f64 + 1.0)).collect();
    let erased_values: Vec<AnyValue> = typed_values
        .iter()
        .map(|v| Rc::new(**v) as AnyValue)
        .collect();

    let typed_read_ms = time_ms(runs, || {
        let mut acc = 0.0;
        for _ in 0..waves {
            for v in &typed_values {
                let cloned = black_box(v.clone());
                acc += *cloned;
            }
        }
        black_box(acc);
    });

    let erased_read_ms = time_ms(runs, || {
        let mut acc = 0.0;
        for _ in 0..waves {
            for v in &erased_values {
                let cloned = black_box(v.clone());
                acc += *cloned
                    .downcast::<f64>()
                    .expect("probe stores only f64 payloads");
            }
        }
        black_box(acc);
    });

    let typed_emit_ms = time_ms(runs, || {
        let mut acc = 0.0;
        for i in 0..(waves * hops) {
            let produced: Rc<f64> = black_box(Rc::new(i as f64));
            acc += *produced;
        }
        black_box(acc);
    });

    let erased_emit_ms = time_ms(runs, || {
        let mut acc = 0.0;
        for i in 0..(waves * hops) {
            let produced: AnyValue = black_box(Rc::new(i as f64));
            acc += *produced
                .downcast::<f64>()
                .expect("probe stores only f64 payloads");
        }
        black_box(acc);
    });

    let source = Node::<f64>::state(0.0);
    let derived = Node::<f64>::derived(vec![source.erased()], |ctx: &Ctx| {
        ctx.emit(*ctx.data::<f64>(0).unwrap() + 1.0);
    });
    let sink_count = Rc::new(Cell::new(0u64));
    let sc = sink_count.clone();
    let _u = derived.subscribe(move |m| {
        if matches!(m, Message::Data(_)) {
            sc.set(sc.get().wrapping_add(1));
        }
    });
    let graph_ms = time_ms(runs, || {
        for i in 0..protocol_waves {
            source.set(i as f64);
        }
        black_box(sink_count.get());
        black_box(derived.cache());
    });

    let value_ops = waves * hops;
    println!(
        "typed read Rc<T>    : {:8.3} ms ({:8.2} ns/op)",
        typed_read_ms,
        ns_per_op(typed_read_ms, value_ops)
    );
    println!(
        "erased read+cast    : {:8.3} ms ({:8.2} ns/op, {:5.2}x typed)",
        erased_read_ms,
        ns_per_op(erased_read_ms, value_ops),
        erased_read_ms / typed_read_ms
    );
    println!(
        "typed emit alloc    : {:8.3} ms ({:8.2} ns/op)",
        typed_emit_ms,
        ns_per_op(typed_emit_ms, value_ops)
    );
    println!(
        "erased emit+cast    : {:8.3} ms ({:8.2} ns/op, {:5.2}x typed)",
        erased_emit_ms,
        ns_per_op(erased_emit_ms, value_ops),
        erased_emit_ms / typed_emit_ms
    );
    println!(
        "graph ctx data+emit : {:8.3} ms ({:8.2} ns/source.set)\n",
        graph_ms,
        ns_per_op(graph_ms, protocol_waves)
    );
}

fn queue_probe(queue_n: usize, runs: usize) {
    println!("--- Probe Q: boundary FIFO queue ({queue_n} tasks) ---");

    let vec_ms = time_ms(runs, || {
        let acc = Rc::new(Cell::new(0u64));
        let mut q: Vec<Box<dyn FnOnce()>> = Vec::with_capacity(queue_n);
        for _ in 0..queue_n {
            let a = acc.clone();
            q.push(Box::new(move || a.set(a.get().wrapping_add(1))));
        }
        while !q.is_empty() {
            let thunk = q.remove(0);
            thunk();
        }
        black_box(acc.get());
    });

    let deque_ms = time_ms(runs, || {
        let acc = Rc::new(Cell::new(0u64));
        let mut q: VecDeque<Box<dyn FnOnce()>> = VecDeque::with_capacity(queue_n);
        for _ in 0..queue_n {
            let a = acc.clone();
            q.push_back(Box::new(move || a.set(a.get().wrapping_add(1))));
        }
        while let Some(thunk) = q.pop_front() {
            thunk();
        }
        black_box(acc.get());
    });

    println!("Vec/remove(0)       : {:8.3} ms", vec_ms);
    println!("VecDeque/pop_front  : {:8.3} ms", deque_ms);
    println!("speedup             : {:8.2}x\n", vec_ms / deque_ms);
}

fn fanout_probe(fanout: usize, waves: usize, runs: usize) {
    println!("--- Probe F: dirty/data fanout ({fanout} subscribers x {waves} waves) ---");
    let source = Node::<f64>::state(0.0);
    let sink_count = Rc::new(Cell::new(0u64));

    let mut derived_nodes = Vec::with_capacity(fanout);
    let mut unsubs: Vec<Box<dyn FnOnce()>> = Vec::with_capacity(fanout);
    for _ in 0..fanout {
        let d = Node::<f64>::derived(vec![source.erased()], |ctx: &Ctx| {
            ctx.emit(*ctx.data::<f64>(0).unwrap() + 1.0);
        });
        let c = sink_count.clone();
        let u = d.subscribe(move |_| c.set(c.get().wrapping_add(1)));
        derived_nodes.push(d);
        unsubs.push(u);
    }

    let ms = time_ms(runs, || {
        for i in 0..waves {
            source.set(i as f64);
        }
        black_box(sink_count.get());
    });

    let mut tail = 0.0_f64;
    for d in &derived_nodes {
        tail += d.cache().unwrap_or(0.0);
    }
    black_box(tail);
    black_box(unsubs.len());
    println!("fanout push median  : {:8.3} ms", ms);
    println!(
        "ns per source.set   : {:8.2} ns\n",
        ns_per_op(ms, waves.max(1))
    );
}

fn invalidate_probe(fanout: usize, waves: usize, runs: usize) {
    println!("--- Probe I: invalidate traversal ({fanout} fanout x {waves} waves) ---");
    let source = Node::<f64>::state(0.0);
    let inv_count = Rc::new(Cell::new(0usize));
    let mut leaves = Vec::with_capacity(fanout);
    let mut unsubs: Vec<Box<dyn FnOnce()>> = Vec::with_capacity(fanout);
    for _ in 0..fanout {
        let d = Node::<f64>::derived(vec![source.erased()], |ctx: &Ctx| {
            ctx.emit(*ctx.data::<f64>(0).unwrap() + 1.0);
        });
        let c = inv_count.clone();
        let u = d.subscribe(move |m: &Message<AnyValue>| {
            if matches!(m, Message::Invalidate) {
                c.set(c.get() + 1);
            }
        });
        leaves.push(d);
        unsubs.push(u);
    }

    let set_only_ms = time_ms(runs, || {
        for i in 0..waves {
            source.set(i as f64);
        }
        black_box(inv_count.get());
    });

    let set_plus_invalidate_ms = time_ms(runs, || {
        for i in 0..waves {
            source.set(i as f64);
            source.down(vec![Message::Invalidate]);
        }
        black_box(inv_count.get());
    });

    let expected = fanout * waves * (runs + 1); // warmup + measured runs
    let invalidate_only_ms = (set_plus_invalidate_ms - set_only_ms).max(0.0);
    println!("set-only median     : {:8.3} ms", set_only_ms);
    println!("set+invalidate med  : {:8.3} ms", set_plus_invalidate_ms);
    println!("invalidate-only est : {:8.3} ms", invalidate_only_ms);
    println!(
        "ns per inv wave est : {:8.2} ns",
        ns_per_op(invalidate_only_ms, waves.max(1))
    );
    println!(
        "invalidate deliveries observed: {} (expected ~{})\n",
        inv_count.get(),
        expected
    );
    black_box(leaves.len());
    black_box(unsubs.len());
}

fn diamond_probe(legs: usize, waves: usize, runs: usize) {
    println!("--- Probe D: diamond pending join ({legs} legs x {waves} waves) ---");
    let source = Node::<f64>::state_empty();
    let mut mids = Vec::with_capacity(legs);
    for _ in 0..legs {
        let m = Node::<f64>::derived(vec![source.erased()], |ctx: &Ctx| {
            ctx.emit(*ctx.data::<f64>(0).unwrap() + 1.0);
        });
        mids.push(m);
    }
    let deps = mids.iter().map(Node::erased).collect::<Vec<_>>();
    let join_runs = Rc::new(Cell::new(0usize));
    let jr = join_runs.clone();
    let join = Node::<f64>::derived(deps, move |ctx: &Ctx| {
        jr.set(jr.get() + 1);
        let mut acc = 0.0;
        for i in 0..ctx.dep_len() {
            acc += *ctx.data::<f64>(i).unwrap();
        }
        ctx.emit(acc);
    });
    let sink_count = Rc::new(Cell::new(0usize));
    let sc = sink_count.clone();
    let _u = join.subscribe(move |m| {
        if matches!(m, Message::Data(_)) {
            sc.set(sc.get() + 1);
        }
    });

    let ms = time_ms(runs, || {
        for i in 0..waves {
            source.set(i as f64);
        }
        black_box(join_runs.get());
        black_box(sink_count.get());
    });

    let warmup_data = sink_count.get().saturating_sub(waves * runs);
    println!("diamond median      : {:8.3} ms", ms);
    println!(
        "ns per source.set   : {:8.2} ns",
        ns_per_op(ms, waves.max(1))
    );
    println!(
        "join DATA count     : {} (expected measured={} + warmup≈{})\n",
        sink_count.get(),
        waves * runs,
        warmup_data
    );
}

fn frontier_probe(sources_n: usize, waves: usize, runs: usize) {
    println!("--- Probe C: sparse vs dense frontier ({sources_n} sources, {waves} waves) ---");
    assert!(sources_n > 0, "frontier_probe requires sources_n > 0");
    let mut sources = Vec::with_capacity(sources_n);
    let mut unsubs: Vec<Box<dyn FnOnce()>> = Vec::with_capacity(sources_n);
    for _ in 0..sources_n {
        let s = Node::<f64>::state(0.0);
        let d = Node::<f64>::derived(vec![s.erased()], |ctx: &Ctx| {
            ctx.emit(*ctx.data::<f64>(0).unwrap() + 1.0);
        });
        let u = d.subscribe(|_| {});
        sources.push(s);
        unsubs.push(u);
    }

    let sparse_ms = time_ms(runs, || {
        for i in 0..waves {
            let idx = i % sources_n;
            sources[idx].set(i as f64);
        }
    });
    let dense_ms = time_ms(runs, || {
        for i in 0..waves {
            for s in &sources {
                s.set(i as f64);
            }
        }
    });

    let sparse_ops = waves;
    let dense_ops = waves * sources_n;
    println!("sparse frontier     : {:8.3} ms", sparse_ms);
    println!("dense frontier      : {:8.3} ms", dense_ms);
    println!(
        "ns per sparse set   : {:8.2} ns",
        ns_per_op(sparse_ms, sparse_ops.max(1))
    );
    println!(
        "ns per dense set    : {:8.2} ns\n",
        ns_per_op(dense_ms, dense_ops.max(1))
    );
    black_box(unsubs.len());
}

struct DropToken {
    dropped: Rc<Cell<usize>>,
}

impl Drop for DropToken {
    fn drop(&mut self) {
        self.dropped.set(self.dropped.get() + 1);
    }
}

fn retained_heap_probe(rounds: usize, nodes_per_round: usize, payload_bytes: usize) {
    println!(
        "--- Probe M: retained-heap proxy (rounds={rounds}, nodes/round={nodes_per_round}, payload={}B) ---",
        payload_bytes
    );
    let dropped = Rc::new(Cell::new(0usize));
    let source = Node::<usize>::state(1);
    let ms = {
        let mut per_round = Vec::with_capacity(rounds);
        for r in 0..rounds {
            let t0 = Instant::now();
            let mut nodes = Vec::with_capacity(nodes_per_round);
            let mut unsubs: Vec<Box<dyn FnOnce()>> = Vec::with_capacity(nodes_per_round);
            for _ in 0..nodes_per_round {
                let token = DropToken {
                    dropped: dropped.clone(),
                };
                let payload = vec![0u8; payload_bytes];
                let n = Node::<usize>::derived(vec![source.erased()], move |ctx: &Ctx| {
                    black_box(payload.len());
                    black_box(&token);
                    ctx.emit(*ctx.data::<usize>(0).unwrap() + 1);
                });
                // Activate once so retention checks include active dep/subscription lifecycle.
                let u = n.subscribe(|_| {});
                unsubs.push(u);
                nodes.push(n);
            }
            // Drive one active wave across the round.
            source.set(r + 2);
            for u in unsubs.drain(..) {
                u();
            }
            drop(nodes);
            let got = dropped.get();
            let want = (r + 1) * nodes_per_round;
            if got != want {
                println!(
                    "WARN retained payloads after round {}: dropped={} expected={}",
                    r + 1,
                    got,
                    want
                );
            }
            per_round.push(t0.elapsed().as_secs_f64() * 1000.0);
        }
        median(per_round)
    };
    println!("alloc+drop median   : {:8.3} ms/round", ms);
    println!(
        "drop completeness   : {}/{} (1.0 means no retained captures)\n",
        dropped.get(),
        rounds * nodes_per_round
    );
}

fn rewire_probe(iterations: usize, runs: usize) {
    println!("--- Probe R: rewire churn ({iterations} alternating set_deps) ---");
    let a = Node::<f64>::state(1.0);
    let b = Node::<f64>::state(2.0);
    let out = Node::<f64>::derived(vec![a.erased()], |ctx: &Ctx| {
        ctx.emit(*ctx.data::<f64>(0).unwrap() + 1.0);
    });
    let counter = Rc::new(Cell::new(0u64));
    let c = counter.clone();
    let _u = out.subscribe(move |_| c.set(c.get().wrapping_add(1)));

    let ms = time_ms(runs, || {
        for i in 0..iterations {
            if (i & 1) == 0 {
                out.set_deps(vec![a.erased()], |ctx: &Ctx| {
                    ctx.emit(*ctx.data::<f64>(0).unwrap() + 1.0);
                });
                a.set(i as f64);
            } else {
                out.set_deps(vec![b.erased()], |ctx: &Ctx| {
                    ctx.emit(*ctx.data::<f64>(0).unwrap() + 1.0);
                });
                b.set(i as f64);
            }
        }
        black_box(counter.get());
    });

    black_box(out.cache());
    println!("rewire churn median : {:8.3} ms\n", ms);
}

fn main() {
    // Tuned to stay practical on laptops while still showing trend lines.
    let runs = 5;
    let queue_n = 20_000;
    let fanout = 512;
    let inv_fanout = 256;
    let diamond_legs = 128;
    let frontier_sources = 256;
    let waves = 2_000;
    let rewires = 10_000;
    let value_hops = 8;
    let value_waves = 1_000_000;
    let protocol_value_waves = 50_000;
    let heap_rounds = 6;
    let heap_nodes_per_round = 2_500;
    let heap_payload_bytes = 4 * 1024;

    println!("== B49 probe (Rust) ==");
    typed_vs_erased_probe(value_hops, value_waves, protocol_value_waves, runs);
    queue_probe(queue_n, runs);
    fanout_probe(fanout, waves, runs);
    invalidate_probe(inv_fanout, waves, runs);
    diamond_probe(diamond_legs, waves, runs);
    frontier_probe(frontier_sources, 800, runs);
    retained_heap_probe(heap_rounds, heap_nodes_per_round, heap_payload_bytes);
    rewire_probe(rewires, runs);
}
