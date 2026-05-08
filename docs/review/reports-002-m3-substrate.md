# Report 002 — M3 Slice A + B (DepRecord + FnResult::Batch substrate)

**Slices covered:** M3 Slice A (per-dep `DepRecord` + `DepBatch` FFI + R1.3.6.b wave-end rotation) and Slice B (`FnResult::Batch` + `commit_emission_verbatim` + `pending_auto_resolve` + R1.3.1.a fix). Both landed 2026-05-06; /qa rounds applied 2026-05-06.

**Test count:** 281 → after both /qa rounds.

---

## 1. Why this slice exists

Pre-M3 the dispatcher fed firing fns a single `dep_handles: Vec<HandleId>` (one slot per dep — last-write-wins). That's wrong by R1.3.6.b: when a dep emits multiple Data messages in a single wave (e.g. two emits inside a `batch(|| {})`), the firing fn must see *both* the prefix and the latest, not just the latest. Slice A introduces per-dep `DepRecord` carrying both `latest_handle` and a `data_batch: VecDeque<HandleId>` to hold the prefix.

Slice B then fixes the symmetric bug on the *output* side: a fn must be able to emit a heterogeneous wave from a single fire (e.g. `[Data, Data, Complete]`) without being re-invoked between emissions. `FnResult::Batch(Vec<FnEmission>)` is the new return shape; `commit_emission_verbatim` is the per-element committer.

---

## 2. Behavioral traces

### Trace 1 — Single-emit `DepBatch` rotation (R1.3.6.b basic case)

| Step | Event | Internal state change | Observable output |
|------|-------|----------------------|-------------------|
| 1 | `register_derived(deps=[s], fn_id)` | `DepRecord{ s, latest=h_initial, data_batch=[], involved=false }` | `NodeId d` |
| 2 | `subscribe(d, sink)` | activate; first fire | `[Start, Data(initial_d)]` |
| 3 | `emit(s, h1)` | `s`'s `DepRecord` (in `d`'s record list): `data_batch.push_back(h1)`, `involved = true` | (mid-wave) |
| 4 | wave drain — rotate dep records | `latest = h1`, `data_batch = []`, `involved = false` | — |
| 5 | `invoke_fn(d, &[DepBatch{ data: [], latest: h1 }])` | fn sees prefix=empty + latest=h1 | — |
| 6 | fn returns `FnResult::Single(Data(h_d))` | commit, queue notify | `sink: [Data(h_d)]` |

**Diagrams:** [F7.1](#fc-7.1) NodeRecord shape, [F7.2](#fc-7.2) wave-end rotation.

### Trace 2 — Multi-emit DepBatch coalescing (R1.3.6.b multi-emit + R1.3.1.a fix)

| Step | Event | Internal state change | Observable output |
|------|-------|----------------------|-------------------|
| 1 | `batch(|| { emit(s, h1); emit(s, h2); })` opens batch | `BatchGuard` armed | — |
| 2 | first `emit(s, h1)` | DepRecord(s in d): `data_batch = [h1]`, `tier3_emitted_this_wave.insert(s)` | (mid-wave) |
| 3 | second `emit(s, h2)` | DepRecord(s in d): `data_batch = [h1, h2]` (per Slice G: skip equals on subsequent emit; same wave) | (mid-wave) |
| 4 | batch drain | rotate: `latest = h2`, `data_batch = [h1]` | — |
| 5 | `invoke_fn(d, &[DepBatch{ data: [h1], latest: h2 }])` | fn sees full history | — |
| 6 | fn returns `FnResult::Batch([Data(h_d1), Data(h_d2)])` | commit each verbatim | `sink: [Data(h_d1), Data(h_d2)]` |

**Diagrams:** [F7.2](#fc-7.2), [F7.3](#fc-7.3) `FnResult::Batch` dispatch + `commit_emission_verbatim`.

### Trace 3 — `FnResult::Batch [Data, Complete]` with terminal-break-in-batch (R1.3.4.a)

| Step | Event | Internal state change | Observable output |
|------|-------|----------------------|-------------------|
| 1 | `register_derived(deps=[s], fn_id)`; `subscribe(d, sink)` | — | `[Start, Data(initial_d)]` |
| 2 | `emit(s, h1)` triggers fn fire | — | (mid-wave) |
| 3 | fn returns `FnResult::Batch([Data(h_d), Complete])` | commit `Data(h_d)` verbatim; *terminal break* — subsequent emissions in the same FnResult::Batch are dropped per R1.3.4.a; queue Complete | (mid-wave) |
| 4 | wave drain | sink sees `[Data(h_d), Teardown(d), Complete(d)]` (R2.6.4 auto-Teardown precedes Complete) | — |

**Diagrams:** [F7.3](#fc-7.3) (terminal-break box).

### Trace 4 — Empty Batch settles RESOLVED (Slice B /qa F1)

| Step | Event | Internal state change | Observable output |
|------|-------|----------------------|-------------------|
| 1 | `emit(s, h1)` triggers fn fire on `d` | — | — |
| 2 | fn returns `FnResult::Batch([])` (empty) | `settle_dirty_resolved` path: promote DIRTY → RESOLVED for `d` (R1.3.3.a discharge) | — |
| 3 | wave drain | sink sees `[Resolved(d)]` (no Data) | — |

This case was DIRTY-stuck pre-/qa. Diagram: [F7.3](#fc-7.3) (empty-Batch box).

### Trace 5 — `pending_auto_resolve` diamond fix (R1.3.3.b)

| Step | Event | Internal state change | Observable output |
|------|-------|----------------------|-------------------|
| 1 | Topology: `s → l, r → d` (d depends on l and r); both subscribed | — | — |
| 2 | `emit(s, h1)` | wave: `pending_fires = {l, r, d}` | (mid-wave) |
| 3 | `l` fires → `Data(h_l) + Resolved(l)` | wait — `d` not yet fired | (mid-wave) |
| 4 | `r` fires → `Data(h_r) + Resolved(r)` | now `d` fires | (mid-wave) |
| 5 | `d` fires → `FnResult::Single(Data(h_d))` | commit; `pending_auto_resolve.set(d)` (Slice B addition) | (mid-wave) |
| 6 | wave drain | `queue_auto_resolved` discharges: `Resolved(d)` queued *once* (not duplicated by both `l` and `r` settle paths) | sink sees `[Data(h_l), Data(h_r), Data(h_d), Resolved(l), Resolved(r), Resolved(d)]` |

Pre-Slice-B `d` would have settled Resolved twice (once via each parent). **Diagram:** [F7.3](#fc-7.3) (`pending_auto_resolve` box).

---

## 3. Simplification delta

| # | TS pattern | Rust replacement | Simpler? | Notes |
|---|-----------|------------------|----------|-------|
| 1 | Per-dep `latestHandle`, `prevDataBuffer` parallel in fn closure | Single `DepRecord` struct ([F7.1](#fc-7.1)) | Same | Required for lock-acquired access |
| 2 | `EmitMessage` discriminated union returned from fn | `FnResult` enum + `FnEmission` ([F7.3](#fc-7.3)) | Same | Same shape, typed |
| 3 | `commit_emission` with inline equals path | Two methods: `commit_emission` + `commit_emission_verbatim` ([F7.3](#fc-7.3)) | Rust harder | Justified: R1.3.2.d per-wave coalescing (Slice G) requires verbatim emits to participate in `tier3_emitted_this_wave`; cleaner with a dedicated method |
| 4 | `currentWaveAutoResolve` Set | `pending_auto_resolve: Option<PendingAutoResolve>` per node ([F1.1](#fc-1.1) + [F7.1](#fc-7.1)) | Same | Per-node makes diamond fix more localized than wave-global |
| 5 | (no analog) `FnResult::Batch([])` empty-batch handling | Explicit `settle_dirty_resolved` path | Rust adds — required | Slice B /qa F1 — was DIRTY-stuck without it |
| 6 | DepBatchJs napi wrapper | Dropped (D3 in /qa) | Rust simpler | No FFI consumer yet; add when napi operator binding lands |

**Net assessment:** 1 row adds Rust complexity (row 3); justified by Slice G's invariant. Row 5 is required correctness; row 6 is a correct simplification of removing speculative FFI.

---

## 4. Deferred gaps audit

**`#[ignore]` stubs in Slice A/B test files:** zero. `tests/dep_batch.rs` (7 tests) and `tests/batch.rs` (21 tests, includes Slice B regressions) all live tests.

**Open items linked to this slice:**
- D1 napi-rs builtin batch fns (`MapAddOneBatch`, `MulTenThenComplete`) — silent-skip on non-Int handle. /qa A6 tightened to panic. ✅
- D3 dropped speculative `DepBatchJs` napi wrapper. ✅ (correct simplification — re-adds when napi operator binding ships)
- Slice B /qa A8 — hardcoded `EqualsMode::Identity` in `register_batch_derived` for now; widen to user-configurable when M3 napi binding parity lands.

**Potential correctness holes:** none.

---

## 5. Parity test coverage

Direct parity scenarios for Slice A + B substrate are pending napi binding activation. Once the operator parity slice lands (D053 — activate-and-triage), the existing `core` integration tests get parity coverage automatically. Suggested additions when activated:
1. **R1.3.6.b coalescing** — emit twice in `batch(||)`, assert fn sees `data_batch=[h1]` + `latest=h2`.
2. **R1.3.1.a empty Batch** — fn returns `FnResult::Batch([])`, assert `[Resolved]` only.
3. **R1.3.4.a terminal-break-in-batch** — fn returns `[Data, Complete, Data]`, assert third Data dropped.
4. **R1.3.3.b diamond auto-resolve** — same node settles Resolved exactly once across diamond reconvergence.

---

## 6. Recommended actions

1. When napi binding ships, add the four parity scenarios above.
2. Re-introduce `DepBatchJs` napi wrapper alongside operator binding (D3 was correctly dropped pre-consumer; will need it once operators consume `&[DepBatch]` across FFI).
3. Widen `register_batch_derived` napi method to take `EqualsMode` parameter (currently hardcoded Identity).

---

## 7. Overall assessment

- **Spec-fidelity: high.** Both R1.3.6.b (input batch) and R1.3.1.a (output batch) correctly modeled. `pending_auto_resolve` is a faithful R1.3.3.b implementation.
- **Over-engineering risk: low.** Two-method `commit_emission` split is the only Rust-adds row; justified by Slice G's per-wave coalescing.
- **Deferred items:** 0 open; 3 closed by /qa.
- **No HALT.**
