# Rust Port Review — Overview & Current State

**Review date:** 2026-05-07
**Port HEAD:** `b5349d6 feat: slice H, type error refactoring` + 23 modified files / 3 untracked (Slice H tests)
**Total tests passing:** 438 workspace-wide
**Lint state:** `cargo clippy --all-targets -D warnings` clean, `cargo fmt --check` clean, `#![forbid(unsafe_code)]` preserved

---

## Why this report exists

This is a one-shot incremental review covering **every slice of the Rust port from the very first scaffold to the current HEAD.** It supersedes the two prior `/rust-review` runs (sessions `ec73a18b` 2026-04-02 and `2d79c5da` 2026-04-02) by re-deriving traces against current source, updating the flowcharts to reflect 16+ slices that have landed since those sessions, and laying down a navigable record across six chronological reports plus this overview.

The companion artifact is [`docs/flowcharts.md`](../../../graphrefly-rs/docs/flowcharts.md) in the Rust repo — **canonical** Mermaid diagrams of the port's distinctive shape (locks, RAII, refcounts, ownership). Every report below cites flowcharts via `[F<batch>.<n>]` markers; on the review website those markers open the diagram in a draggable modal so you can read the report and the diagram side-by-side.

---

## Current state at a glance

| Group | Status | Tests | Notes |
|-------|--------|-------|-------|
| **Pre-M1 scaffold** | ✅ closed 2026-05-03 | 13 | 8 crates, message/handle/clock/boundary, `forbid(unsafe_code)` |
| **M1 dispatcher** | ✅ closed 2026-05-05 | 174 | Slices A+B / A-bigger / A close / B / C / C-1 / C-1.5 / C-2 |
| **M2 graph** | ✅ closed 2026-05-06 | 227 | Slices D / E+ / F + /qa rounds |
| **M3 Slice A + B** (substrate) | ✅ landed 2026-05-06 | 281 | DepRecord + FnResult::Batch + napi parity |
| **M3 Slice C-1 / C-2 / C-3** (operators) | ✅ landed 2026-05-06 | 333 | 13 OperatorOp variants, op_scratch trait |
| **M3 Slice D-substrate + D-ops + D /qa** | ✅ landed 2026-05-06–07 | 384 | Unified `register`, ProducerCtx, zip/concat/race/take_until, loom verification |
| **M3 Slice E** (higher-order + lock-released handshake + /qa) | ✅ landed 2026-05-07 | 388 | switchMap/exhaustMap/concatMap/mergeMap, D045 lifts subscribe-time handshake panic |
| **M3 Slice F** (canonical correctness pass + audit follow-on + /qa) | ✅ landed 2026-05-07 | 411–417 | 8 audit-driven fixes; `Core::up` + `UpError`; `PausableMode { Default, ResumeAll, Off }` |
| **M3 Slice G** (R1.3.2.d per-wave equals coalescing) | ✅ landed 2026-05-07 | 411 | Closes the bug surfaced by Slice F audit's debug_assert |
| **M3 Slice E1** (replay buffer, R2.6.5 / Lock 6.G) | ✅ landed 2026-05-07 | (with F) | Per-node `replay_buffer` + handshake replay |
| **M3 Slice H** (typed-error promotion + /qa) | ✅ landed 2026-05-07 | 438 | `RegisterError`, `SetPausableModeError`, `OperatorFactoryError`, `ScratchReleaseGuard` RAII |
| **M3 Slice E2** (fn ctx + cleanup hooks) | ⏸ scheduled | — | D054/D055 design locked 2026-05-07; lift scheduled |
| **M3 napi-rs operator parity** | ⏸ deferred | — | TSFN plumbing; activates ~25 existing parity scenarios when shipped |
| **M4 / M5 / M6 / wasm bindings** | ⏸ blocked | — | Sequenced behind DS-14 / M2 / M5 |

---

## Reports in this set

The reports walk forward through history. Read in order if you want the full arc; jump if you only need a particular slice.

| File | Scope |
|------|-------|
| [`reports-001-m1-and-m2.md`](reports-001-m1-and-m2.md) | M1 dispatcher (Slices A through C-2) + M2 (Slice D / E+ / F) — closed milestones |
| [`reports-002-m3-substrate.md`](reports-002-m3-substrate.md) | M3 Slice A (`DepRecord` + `DepBatch` FFI + R1.3.6.b) + Slice B (`FnResult::Batch` + `commit_emission_verbatim`) |
| [`reports-003-m3-operators.md`](reports-003-m3-operators.md) | M3 Slice C-1 (transform) + C-2 (combine/withLatestFrom/merge) + C-3 (flow + `op_scratch` trait) + D-substrate (NodeKind drop + Producer hook) |
| [`reports-004-m3-combinators-and-higher-order.md`](reports-004-m3-combinators-and-higher-order.md) | M3 Slice D-ops (zip/concat/race/take_until + `ProducerCtx` + loom verification) + Slice E (switchMap/exhaustMap/concatMap/mergeMap + D045 lock-released handshake + /qa) |
| [`reports-005-m3-correctness-and-typed-errors.md`](reports-005-m3-correctness-and-typed-errors.md) | M3 Slice F canonical correctness pass + audit follow-on + /qa + Slice G (per-wave equals coalescing) + Slice E1 (replay buffer) + Slice H (typed-error promotion + `ScratchReleaseGuard` RAII) |

---

## How to read the flowcharts

Diagrams in [`docs/flowcharts.md`](../../../graphrefly-rs/docs/flowcharts.md) cover the **Rust port's shape** — locks, RAII, refcounts, ownership — not protocol semantics. For the protocol semantics see [`docs/implementation-plan-13.6-flowcharts.md`](../implementation-plan-13.6-flowcharts.md) which covers the TS-side spec invariants. Together they form the spec ↔ implementation bridge.

Diagram conventions:
- 🟨 **YELLOW** flags v1 limitations / documented divergences from the canonical spec.
- 🟦 **BLUE** flags Rust-specific simplifications versus TS (no protocol meaning, just port shape).
- Solid arrow → control flow. Dashed arrow → data/message flow. Dotted arrow → optional/conditional path.

Each report references diagrams using `[F<batch>.<n>]` markers (e.g. [F7.2](#fc-7.2)). On the website these open as draggable modals next to the report; in raw markdown they read as anchors that GitHub silently ignores.

---

## Top-line assessment (scoped detail in each report)

- **Spec-fidelity: high.** Across all 16+ slices, no contradictions with the canonical spec went unflagged. ~92% rule conformance after the Slice F audit pass; the residual 8% is documented divergences in [`porting-deferred.md`](../../../graphrefly-rs/docs/porting-deferred.md), every one with a justification or scheduled fix.
- **Over-engineering risk: low.** Of ~50 simplification-delta rows examined across reports 001–005, four flag Rust as more complex than TS (`OperatorScratch` trait substrate, transitive `pick_next_fire` BFS, `ScratchReleaseGuard` RAII, op-state Mutex for higher-order subs). Each is justified by a property TS doesn't need (multi-thread state lock, deterministic resubscribable reset, panic-safe registration).
- **Correctness holes flagged for fix:** 0 critical. Several documented divergences (D1, D2, D4 from Slice F /qa; D028 from C-3; concat_map=mergeMap(.., Some(1)) from D040) are explicit and tracked.
- **No HALT** required across this review. The Slice F audit's R1.3.2.d violation was already fixed in Slice G before this review fired.

The detailed traces, simplification deltas, gap audits, parity-test coverage, and recommended actions live in the per-slice reports.

---

## What this review did NOT cover

- **napi-rs operator binding parity** (~25 parity scenarios live in `packages/parity-tests/scenarios/operators/` but only run against `pureTsImpl`; `rustImpl` activation gates on the deferred binding work). When that lands, those scenarios become this review's evidence layer.
- **pyo3 / wasm bindings** — both stubs only, deferred to M6 / post-M5.
- **`graphrefly-storage` / `graphrefly-structures`** — both stubs only, blocked behind DS-14 op-log changesets. Their flowchart batches will land alongside their first feature slice.
- **Bench numbers** — Phase 13.7 v0 bench landed pre-M1 and is reported in [`docs/research/rust-bench-v0-results.md`](../research/rust-bench-v0-results.md). No new bench passes since.
