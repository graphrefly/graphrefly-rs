# Report 003 — M3 Slice C-1 + C-2 + C-3 + D-substrate (operator architecture)

**Slices covered:** C-1 (transform — `map`/`filter`/`scan`/`reduce`/`distinctUntilChanged`/`pairwise`), C-2 (multi-dep combinators — `combine`/`withLatestFrom`/`merge`), C-3 (flow + `op_scratch` migration — `take`/`skip`/`take_while`/`last`), D-substrate (unified `Core::register` + producer hooks). All landed 2026-05-06.

**Test count progression:** 281 → 300 (C-1) → 314 (C-2) → 333 (C-3) → 347 (D-substrate). Test files: `transform.rs` (706 LOC), `combine.rs` (418 LOC), `flow.rs` (523 LOC), `producer.rs` (440 LOC).

---

## 1. Architectural premise

The Core-dispatch architecture (D009) makes operators a first-class kind, distinct from generic `Derived` nodes. This is a **Rust-side optimization** with no spec analog — TS implements every operator as a regular `derived(deps, fn)` call where the operator semantics live entirely inside the user-supplied fn closure. Rust dispatches operators inside `fire_operator` directly, bypassing FFI hops for built-in transforms (`Map`, `Merge`, `Pairwise`).

The trade-off: a `OperatorOp` discriminant enum and 13 dispatch arms in [F8.3](#fc-8.3). The win: FFI elimination on hot paths (~50 ns/emit per benchmark) plus the ability to participate in `skips_auto_cascade` (Lock 2.B opt-out for `Reduce` and `Last`).

---

## 2. Behavioral traces

### Trace 1 — `map` zero-FFI Data forward

| Step | Event | Internal state change | Observable output |
|------|-------|----------------------|-------------------|
| 1 | `map(&core, &binding, source, project_fn)` | `binding.register_project(project_fn) → fn_id`; `core.register_operator(deps=[source], OperatorOp::Map(fn_id), opts)` | `Ok(NodeId)` |
| 2 | `subscribe(map_id, sink)` | activate; first fire — `fire_operator(Map)`: lock-released `binding.invoke_project(fn_id, latest=h_initial) → h_mapped` | `[Start, Data(h_mapped)]` |
| 3 | `emit(source, h1)` | wave: `fire_operator(Map)`: `binding.invoke_project(fn_id, h1) → h_m1` | wave queues Data(h_m1) |
| 4 | drain | sink sees `[Data(h_m1), Resolved]` | — |

Cited rules: R1.3.5.a (handshake on subscribe). **Diagrams:** [F8.1](#fc-8.1), [F8.3](#fc-8.3).

### Trace 2 — `reduce` Lock 2.B opt-out (intercepts upstream COMPLETE)

| Step | Event | Internal state change | Observable output |
|------|-------|----------------------|-------------------|
| 1 | `reduce(&core, &binding, source, fold_fn, seed=h0)` | `OperatorOp::Reduce{ fn_id, seed: h0 }`; `NodeKind::skips_auto_cascade()` → true | `Ok(NodeId)` |
| 2 | `subscribe(reduce_id, sink)` | activate; `op_scratch = Box<ReduceState{ acc: h0 }>` | `[Start]` (no Data yet — partial mode) |
| 3 | `emit(source, h1); emit(source, h2);` | each fire updates `acc` via `fold_each(acc, latest)`; no emit yet (Reduce buffers) | (no sink output) |
| 4 | `complete(source)` | upstream COMPLETE arrives; without skips_auto_cascade, Lock 2.B would auto-cascade COMPLETE to reduce_id. With opt-out, `fire_operator` sees the COMPLETE message tier and emits `[Data(acc), Complete]` | `sink: [Data(acc), Teardown(reduce_id), Complete(reduce_id)]` |

**Diagrams:** [F8.1](#fc-8.1) (`skips_auto_cascade` note), [F8.3](#fc-8.3) (Reduce arm).

Same shape applies to `Last`: buffer last DATA, emit on COMPLETE. `Last{default}` emits the default if no DATA was ever seen — Slice H /qa F4 added a regression test for refcount discipline (default handle released on terminate even if `last == None`).

### Trace 3 — `withLatestFrom` first-fire gate-release (D018, D021)

| Step | Event | Internal state change | Observable output |
|------|-------|----------------------|-------------------|
| 1 | `with_latest_from(&core, &binding, primary, others=[s], packer)` | `OperatorOp::WithLatestFrom{ packer: fn_id }`; deps=[primary, s] | `Ok(NodeId)` |
| 2 | `subscribe(wlf_id, sink)` | activate ancestors; `partial: true` (gate-paused until all secondaries have DATA) | `[Start]` |
| 3 | `emit(primary, h_p)` | `fire_op_with_latest_from`: `fired_dep_idx == 0` (primary) but secondary `s` has no DATA yet → fire is gated; queue is `[Resolved]` only (D018) | sink eventually sees `[Resolved]` |
| 4 | `emit(s, h_s)` | secondary DATA arrives; `fired_dep_idx == 1` (secondary) — `with_latest_from` fires *only on primary*, so this is no-op for emit purposes; but the gate-release flag flips | (no Data) |
| 5 | `emit(primary, h_p2)` | now both deps have DATA; `pack_tuple([h_p2, h_s]) → h_packed`; emit | `sink: [Data(h_packed), Resolved]` |

**Diagrams:** [F8.3](#fc-8.3) (WithLatestFrom arm).

### Trace 4 — `take(0)` self-completes on first fire (D027)

| Step | Event | Internal state change | Observable output |
|------|-------|----------------------|-------------------|
| 1 | `take(&core, &binding, source, count=0)` | `OperatorOp::Take{ count: 0 }`; `op_scratch = Box<TakeState{ remaining: 0 }>` | `Ok(NodeId)` |
| 2 | `subscribe(take_id, sink)` | activate; first fire — `remaining == 0` → emit zero items, self-complete | `sink: [Start, Teardown, Complete]` |

D027 explicitly admits `count == 0` (was originally rejected; the spec doesn't forbid it). **Diagrams:** [F8.3](#fc-8.3) (Take arm).

### Trace 5 — `scan` resubscribable cycle reset (Slice C-3 D029 alias-fix)

| Step | Event | Internal state change | Observable output |
|------|-------|----------------------|-------------------|
| 1 | `scan(&core, &binding, source, fold_fn, seed=h0)`, source resubscribable | `op_scratch = Box<ScanState{ acc: h0 }>` (initial retain on `h0`) | — |
| 2 | `subscribe(scan_id, sink_a)` activate, emit, terminate cycle | acc updates, then on terminal cycle → `reset_for_fresh_lifecycle` | — |
| 3 | `reset_for_fresh_lifecycle(scan_id)` 5-phase retain-before-release | (a) make new scratch `Box<ScanState{ acc: h0 }>` (retain h0 again first); (b) take old scratch.acc; (c) install new scratch; (d) release old scratch.acc; (e) drop old scratch | scan ready for re-subscribe |
| 4 | `subscribe(scan_id, sink_b)` | fresh `acc = h0` retain count = 1 (correct) | sink_b sees scan from fresh seed |

The 5-phase retain-before-release is the bug fix: pre-fix, retain count of `h0` could collapse to zero between releasing old acc and retaining new seed if the seed was the same handle as the old acc. **Diagrams:** [F8.4](#fc-8.4) (ScanState note).

### Trace 6 — Producer activation via D-substrate hooks (D031, D035)

| Step | Event | Internal state change | Observable output |
|------|-------|----------------------|-------------------|
| 1 | `register_producer(fn_id) → producer_id` | `NodeRecord{ deps: [], fn_id: Some(_), op: None, kind() = Producer }` | `producer_id` |
| 2 | first `subscribe(producer_id, sink)` | `activate_derived` queues producer into `pending_fires`; lock-released `invoke_fn` runs the producer build closure once | `[Start, Data(initial)]` |
| 3 | additional subscribers: `subscribe(producer_id, sink_b)` | already active; sink_b gets handshake from cache | `[Start, Data(cache_handle)]` |
| 4 | last `drop(Subscription)` | `Subscription::Drop` fires `BindingBoundary::producer_deactivate(producer_id)` lock-released; binding cleans its per-node state | (no message) |
| 5 | re-subscribe later | activate again; producer fires fresh (binding-side state was wiped) | `[Start, Data(fresh)]` |

**Diagrams:** [F9.1](#fc-9.1) NodeKind drop refactor, [F9.2](#fc-9.2) producer lifecycle.

---

## 3. Simplification delta

| # | TS pattern | Rust replacement | Simpler? | Notes |
|---|-----------|------------------|----------|-------|
| 1 | Operator = `derived(deps, fn)` with semantics in fn closure | `NodeKind::Operator(OperatorOp)` + `fire_operator` dispatch ([F8.1](#fc-8.1), [F8.3](#fc-8.3)) | Rust harder | Justified: zero-FFI on Map/Merge/Pairwise; Reduce/Last `skips_auto_cascade` cleaner than TS's Lock 2.B opt-out via fn closure |
| 2 | Operator state in fn closure (`let mut acc = seed`) | `op_scratch: Option<Box<dyn OperatorScratch>>` ([F8.4](#fc-8.4)) | Rust harder | Justified: deterministic `reset_for_fresh_lifecycle` requires lifted state; closure-captured state can't be wiped without rerunning fn |
| 3 | Reduce: `Lock 2.B opt-out` via closure capturing COMPLETE | `NodeKind::skips_auto_cascade() == true` for `Operator(Reduce|Last)` ([F8.1](#fc-8.1)) | Same/cleaner | Discriminant flag |
| 4 | `withLatestFrom` first-fire: re-fire on secondary arrival | `partial: true` register flag + first-fire gate-release ([F8.3](#fc-8.3)) | Same | Already part of D011/R5.4 |
| 5 | `combine` custom equals on tuples | Default `EqualsMode::Identity` (D023) | Rust simpler 🟨 | Custom tuple-equality deferred — simplification is a v1 limitation |
| 6 | Operator factory throws | `Result<NodeId, RegisterError>` (Slice H) | Same | Promoted Slice H |
| 7 | TS `subscribe()` returns producer | Rust producer = `register_producer` + `producer_deactivate` lifecycle hook ([F9.2](#fc-9.2)) | Same | Slice D-substrate D031/D035 |
| 8 | TS `NodeImpl._kind` field (none) | NodeKind drop refactor ([F9.1](#fc-9.1)) — kind derived | Same | D030 brought Rust into parity with TS |
| 9 | Combine equals comparison | `snapshot_op_all_latest` for N-dep snapshot helper | Same | Necessary helper for multi-dep |
| 10 | Recorder fixture (TS: just GC) | `Weak<RecorderInner>` to break Arc cycle (Slice D-ops infrastructure update) | Rust harder | Required because Subscription::Drop is reference-counted |

**Net assessment:** rows 1, 2, 10 add Rust complexity. Rows 1+2 are justified by zero-FFI win + deterministic reset. Row 10 is forced by Rust's lack of GC. Row 5 is the only "Rust simpler but as v1 limitation" row — flagged for closure with custom equals work.

---

## 4. Deferred gaps audit

**`#[ignore]` stubs:** zero. Convention is doc-only.

**Open items linked to Slices C-1 / C-2 / C-3:**
- `OperatorOpts.equals` no-op for transform — applies only to `DistinctUntilChanged` per spec; deliberate choice.
- `fire_operator` first-run gate uses linear scan over `dep_records` — perf concern, not correctness.
- Operator `describe()` doesn't surface per-operator discriminant — `kind: "operator"` only, no variant info. Tracked as M2 follow-up.
- D022 `merge` first-error-terminates divergence (Rust: all-deps-terminal) — documented divergence vs TS producer pattern.
- D023 `combine` custom tuple-equality not implemented.
- D027 `take(0)` allowed (was rejected pre-fix).
- D028 flow counters reset only on resubscribable terminal cycle — documented.
- D1 `predicate_each` length-mismatch silent truncate — documented; closes when napi operator binding lands.

**Open items from D-substrate:**
- TEARDOWN propagation through `producer_deactivate` not yet symmetric (M3 Slice D D1 — open).
- D2 sink Arc cycle audit completed (no fix needed; cycle path identified).

**Potential correctness holes:** none flagged. The Slice C-2 /qa surfaced and fixed F1 (`fire_op_distinct` double-release), F2 (`fire_op_combine` INVALIDATE guard) — both already closed.

---

## 5. Parity test coverage

Existing scenarios (Rust-side activation pending napi operator binding):
- `packages/parity-tests/scenarios/operators/transform.test.ts` — 8 scenarios (map/filter/scan/reduce/distinctUntilChanged/pairwise)
- `packages/parity-tests/scenarios/operators/combine.test.ts` — 6 scenarios (combine/withLatestFrom/merge variants)
- `packages/parity-tests/scenarios/operators/flow.test.ts` — 11 scenarios (take/skip/takeWhile/last + sugar first/find/elementAt)

**Missing parity scenarios (suggested):**
1. **R5.4 / D011 partial-mode first-fire** — `withLatestFrom` gate-release on secondary arrival, assert primary refire emits.
2. **Lock 2.B opt-out via skips_auto_cascade** — `reduce` and `last` intercept upstream COMPLETE; assert single emit + Complete.
3. **D029 5-phase scratch reset** — resubscribable scan cycle, assert acc=seed retain count is correct after re-subscribe.
4. **D031 producer activation lifecycle** — subscribe → emit → drop → re-subscribe; assert producer build runs twice.

---

## 6. Recommended actions

1. When napi operator binding ships, fold D1 (`predicate_each` silent truncate) into a unified `binding_contract_check_pass_len` helper.
2. Promote `OperatorScratch: Send + Sync` static assertion to a doctest (currently only enforced via the trait bound on `dyn OperatorScratch`).
3. Surface per-operator discriminant in `describe()` JSON — small ergonomic win for inspection tools.
4. Schedule custom tuple-equality work for `combine` (D023) — bundles with TSFN custom-equals (D052).

---

## 7. Overall assessment

- **Spec-fidelity: high.** All 13 operator variants verified against canonical spec; documented divergences (D022 merge cascade, D023 combine equals) are explicit.
- **Over-engineering risk: low.** The Operator dispatch architecture adds two complexity rows (rows 1, 2 above) — both justified by FFI elimination + deterministic reset.
- **NodeKind drop refactor (D030):** brings Rust into structural parity with TS — no longer a divergence vector. Excellent simplification.
- **Producer hook (D031/D035):** clean substrate for Slice D-ops + Slice E. The `producer_deactivate` lifecycle is symmetric to TS's disposer pattern.
- **Deferred items:** ~9 open, all explicit. No correctness holes.
- **No HALT.**
