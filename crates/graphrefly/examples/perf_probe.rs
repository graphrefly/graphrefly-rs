//! DR-7 perf probes (Stage-0 ceiling + typed-vs-erased). STANDALONE — measures the
//! CEILING (raw threads / raw value representation), NOT the graph engine (Stage-0 =
//! "don't carry the graph"). Run:  cargo run --release --example perf_probe
//!
//! Probe A (typed-vs-erased): the hot-path tax of `AnyValue = Rc<dyn Any>` — per value
//! hop = box on emit + downcast on read. De-gates DR-7's build GO.
//! Probe B (ceiling): N independent CPU-bound units, 1-thread vs all-core scaling + the
//! single-thread ns/iter (the cross-language constant-factor baseline).

use std::any::Any;
use std::hint::black_box;
use std::rc::Rc;
use std::thread;
use std::time::Instant;

/// A fair, branchless, bounded, CPU-bound f64 kernel — byte-identical arithmetic across
/// Rust / TS / Python (IEEE-754 double mul-add + floor-fract). No transcendentals (those
/// differ per libm), no integer width games (TS has only f64).
#[inline(never)]
fn kernel(w: u64) -> f64 {
    let mut acc = 0.123_456_789_f64;
    for i in 0..w {
        acc = acc * 1.000_001 + 0.5 + ((i & 1) as f64) * 1e-9;
        acc -= acc.floor(); // keep in [0,1), branchless
    }
    acc
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

/// Median wall-ms over `runs` timed reps after one warmup.
fn time_ms(runs: usize, mut f: impl FnMut()) -> f64 {
    f(); // warmup
    let mut s = Vec::new();
    for _ in 0..runs {
        let t = Instant::now();
        f();
        s.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    median(s)
}

fn main() {
    let cores = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    println!("== DR-7 Rust perf probe ==  cores(available_parallelism)={cores}\n");

    // ===== Probe A: typed vs erased value representation =====
    // Model the AnyValue hot-path tax: per hop = produce (box into Rc<dyn Any>) + consume
    // (downcast back). HOPS hops/wave x WAVES waves. The ONLY delta vs the typed path is the
    // box+downcast+refcount; the arithmetic (black_box(x+1.0)) is identical.
    let hops = 8usize;
    let waves = 2_000_000u64;
    let ops = hops as u64 * waves; // value box+downcast operations
    println!("--- Probe A: typed vs erased ({hops} hops x {waves} waves = {ops} value ops) ---");

    let typed = time_ms(5, || {
        let mut x = black_box(1.0_f64);
        for _ in 0..waves {
            for _ in 0..hops {
                x = black_box(x + 1.0);
            }
            if x > 1e12 {
                x = black_box(1.0);
            }
        }
        black_box(x);
    });

    let erased = time_ms(5, || {
        let mut x = black_box(1.0_f64);
        for _ in 0..waves {
            for _ in 0..hops {
                let produced: Rc<dyn Any> = Rc::new(black_box(x + 1.0)); // emit: box
                x = *produced.downcast_ref::<f64>().unwrap(); // read: downcast
            }
            if x > 1e12 {
                x = black_box(1.0);
            }
        }
        black_box(x);
    });
    println!(
        "typed       : {typed:8.1} ms   ({:.2} ns/op)",
        typed * 1e6 / ops as f64
    );
    println!(
        "erased      : {erased:8.1} ms   ({:.2} ns/op)",
        erased * 1e6 / ops as f64
    );
    println!(
        "ERASURE TAX : {:.2}x   (+{:.0}% over typed) <- de-gates DR-7 GO\n",
        erased / typed,
        (erased / typed - 1.0) * 100.0
    );

    // ===== Probe B: ceiling — parallel scaling (raw std::thread) =====
    let n_units = (cores * 4) as u64;
    let w = 10_000_000u64;
    let total_iters = n_units * w;
    println!("--- Probe B: ceiling ({n_units} units x W={w}, 1-thread vs {cores}-thread) ---");

    let seq = time_ms(5, || {
        let mut s = 0.0;
        for u in 0..n_units {
            s += kernel(w + u);
        }
        black_box(s);
    });

    let par = time_ms(5, || {
        let chunk = n_units.div_ceil(cores as u64);
        let handles: Vec<_> = (0..cores as u64)
            .map(|c| {
                let start = c * chunk;
                let end = ((c + 1) * chunk).min(n_units);
                thread::spawn(move || {
                    let mut s = 0.0;
                    for u in start..end {
                        s += kernel(w + u);
                    }
                    s
                })
            })
            .collect();
        let mut s = 0.0;
        for h in handles {
            s += h.join().unwrap();
        }
        black_box(s);
    });
    println!("1-thread    : {seq:8.1} ms");
    println!("{cores}-thread    : {par:8.1} ms");
    println!(
        "SPEEDUP     : {:.2}x   (efficiency {:.0}% of {cores}x linear)",
        seq / par,
        (seq / par) / cores as f64 * 100.0
    );
    println!(
        "single-thread CONSTANT-FACTOR baseline = {:.3} ns/iter   [N={n_units} W={w}]",
        seq * 1e6 / total_iters as f64
    );
}
