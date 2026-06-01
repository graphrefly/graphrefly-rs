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

use graphrefly::{Ctx, Node};

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
    println!("fanout push median  : {:8.3} ms\n", ms);
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
    let waves = 2_000;
    let rewires = 10_000;

    println!("== B49 probe (Rust) ==");
    queue_probe(queue_n, runs);
    fanout_probe(fanout, waves, runs);
    rewire_probe(rewires, runs);
}
