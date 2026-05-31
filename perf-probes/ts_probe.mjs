// DR-7 TS ceiling probe (Stage-0). Raw worker_threads scaling + single-thread ns/iter.
// Run:  node perf-probes/ts_probe.mjs
// Same branchless f64 kernel as the Rust/Python probes (apples-to-apples, normalized ns/iter).
import { Worker, isMainThread, parentPort, workerData } from "node:worker_threads";
import { performance } from "node:perf_hooks";
import os from "node:os";

function kernel(w) {
	let acc = 0.123456789;
	for (let i = 0; i < w; i++) {
		acc = acc * 1.000001 + 0.5 + (i & 1) * 1e-9;
		acc -= Math.floor(acc);
	}
	return acc;
}

if (!isMainThread) {
	const { start, end, w } = workerData;
	let s = 0;
	for (let u = start; u < end; u++) s += kernel(w + u);
	parentPort.postMessage(s);
} else {
	const cores = os.cpus().length;
	const nUnits = cores * 4;
	const w = 10_000_000;
	const runs = 5;
	const totalIters = nUnits * w;
	const median = (a) => {
		a.sort((x, y) => x - y);
		return a[a.length >> 1];
	};

	function seqOnce() {
		let s = 0;
		for (let u = 0; u < nUnits; u++) s += kernel(w + u);
		return s;
	}
	function timeMs(fn) {
		fn();
		const s = [];
		for (let r = 0; r < runs; r++) {
			const t = performance.now();
			fn();
			s.push(performance.now() - t);
		}
		return median(s);
	}
	const seq = timeMs(seqOnce);

	// parallel via workers, re-spawned each rep — the HONEST cost of TS worker parallelism
	// (workers are heavy + have no shared memory; this includes spawn/teardown).
	function parOnce() {
		return new Promise((resolve) => {
			const chunk = Math.ceil(nUnits / cores);
			let done = 0;
			let s = 0;
			for (let c = 0; c < cores; c++) {
				const start = c * chunk;
				const end = Math.min((c + 1) * chunk, nUnits);
				if (start >= end) {
					if (++done === cores) resolve(s);
					continue;
				}
				const wk = new Worker(new URL(import.meta.url), { workerData: { start, end, w } });
				wk.on("message", (m) => {
					s += m;
					wk.terminate();
					if (++done >= cores) resolve(s);
				});
			}
		});
	}
	async function timeMsAsync(fn) {
		await fn();
		const s = [];
		for (let r = 0; r < runs; r++) {
			const t = performance.now();
			await fn();
			s.push(performance.now() - t);
		}
		return median(s);
	}
	const par = await timeMsAsync(parOnce);

	console.log(`== DR-7 TS probe ==  node ${process.version}  cores=${cores}  nUnits=${nUnits}  W=${w}`);
	console.log(`1-thread    : ${seq.toFixed(1)} ms`);
	console.log(`${cores}-worker    : ${par.toFixed(1)} ms`);
	console.log(
		`SPEEDUP     : ${(seq / par).toFixed(2)}x   (efficiency ${(((seq / par) / cores) * 100).toFixed(0)}% of ${cores}x)`,
	);
	console.log(
		`single-thread CONSTANT-FACTOR baseline = ${((seq * 1e6) / totalIters).toFixed(3)} ns/iter   [N=${nUnits} W=${w}]`,
	);
}
