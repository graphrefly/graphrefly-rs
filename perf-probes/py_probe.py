#!/usr/bin/env python3
"""DR-7 Python ceiling probe (Stage-0). Threading (GIL) vs multiprocessing + ns/iter.

Run:  python3 perf-probes/py_probe.py
Same branchless f64 kernel as the Rust/TS probes. Pure Python is ~50x slower, so W is
smaller here -- the cross-language comparison is the NORMALIZED ns/iter, not raw ms.
The threading-vs-multiprocessing gap IS the GIL kill-switch for "Python beats TS on CPU".
"""
import math
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor


def kernel(w):
    acc = 0.123456789
    for i in range(w):
        acc = acc * 1.000001 + 0.5 + (i & 1) * 1e-9
        acc -= math.floor(acc)
    return acc


def run_units(args):
    start, end, w = args
    s = 0.0
    for u in range(start, end):
        s += kernel(w + u)
    return s


def median(a):
    a = sorted(a)
    return a[len(a) // 2]


def time_ms(fn, runs):
    fn()  # warmup
    samples = []
    for _ in range(runs):
        t = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - t) * 1000)
    return median(samples)


def main():
    cores = os.cpu_count() or 1
    n_units = cores * 4
    w = 2_000_000  # ~5x smaller than Rust/TS; compare via ns/iter, not raw ms
    runs = 3
    total_iters = n_units * w

    def seq():
        s = 0.0
        for u in range(n_units):
            s += kernel(w + u)
        return s

    t_seq = time_ms(seq, runs)

    chunk = (n_units + cores - 1) // cores
    tasks = [(c * chunk, min((c + 1) * chunk, n_units), w) for c in range(cores)]

    def threaded():
        with ThreadPoolExecutor(max_workers=cores) as ex:
            list(ex.map(run_units, tasks))

    t_thr = time_ms(threaded, runs)

    def procd():
        with ProcessPoolExecutor(max_workers=cores) as ex:
            list(ex.map(run_units, tasks))

    t_proc = time_ms(procd, runs)

    # free-threaded build detection (PEP 703, 3.13t+)
    is_gil = getattr(sys, "_is_gil_enabled", None)
    gil = "?" if is_gil is None else ("ON" if is_gil() else "OFF (free-threaded)")

    print(f"== DR-7 Python probe ==  Python {sys.version.split()[0]}  cores={cores}  nUnits={n_units}  W={w}  GIL={gil}")
    print(f"1-thread        : {t_seq:8.1f} ms")
    print(f"{cores}-thread (GIL)  : {t_thr:8.1f} ms   speedup {t_seq / t_thr:.2f}x  <- GIL kill-switch")
    print(f"{cores}-process      : {t_proc:8.1f} ms   speedup {t_seq / t_proc:.2f}x")
    print(f"single-thread CONSTANT-FACTOR baseline = {t_seq * 1e6 / total_iters:.3f} ns/iter   [N={n_units} W={w}]")


if __name__ == "__main__":
    main()
