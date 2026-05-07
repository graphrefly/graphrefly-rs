# Migration status

Live tracker for the 6-milestone Rust port. Update after each milestone closes. The full migration plan lives in `~/src/graphrefly-ts/archive/docs/SESSION-rust-port-architecture.md`.

## Current state (2026-05-06)

**M1 closed; M2 closed; M3 Slice A + B + C-1 + C-2 landed (2026-05-06).** Slice C-2 (combinator operators — `combine` / `withLatestFrom` / `merge`) extends the operator substrate with multi-dep dispatch: `OperatorOp::Combine { pack_fn }` / `WithLatestFrom { pack_fn }` / `Merge` enum variants, `BindingBoundary::pack_tuple` extension method (D020), `snapshot_op_all_latest` helper for N-dep latest-handle collection, first-fire gate-release semantics on `withLatestFrom` (D021), zero-FFI merge forwarding (D022), `OperatorBinding::register_packer` closure registration. Parity-tested: 6 scenarios across combine/withLatestFrom/merge in `packages/parity-tests/scenarios/operators/combine.test.ts`. Slice C-1 (transform operators — `map` / `filter` / `scan` / `reduce` / `distinctUntilChanged` / `pairwise`) lands the first operators against the Core-dispatch architecture (D009): `NodeKind::Operator(OperatorOp)` enum variant, `OperatorOp` discriminant, `partial: bool` register option (D011 / R5.4), `BindingBoundary` extension methods (`project_each` / `predicate_each` / `fold_each` / `pairwise_pack`), `Core::register_operator(deps, op, opts)`, per-operator dispatch in `fire_operator` with operator-specific state (`operator_state: HandleId` slot for Scan/Reduce acc and Distinct/Pairwise prev), Reduce's Lock 2.B opt-out (`NodeKind::skips_auto_cascade`) so Reduce can intercept upstream COMPLETE to emit acc + Complete. New crate `graphrefly-operators` exposes `OperatorBinding` super-trait of `BindingBoundary` (D015) plus the six transform factories taking `&Core` + `&Arc<dyn OperatorBinding>` (D010 helper-trait shape). `partial` flag plumbed through all three legacy register methods (`register_state` / `register_derived` / `register_dynamic`) per D019. Substrate is now end-to-end parity-tested across Rust core ↔ JS bindings ↔ TS legacy oracle. M2 (Slice F — port-coverage gap fills + topology primitive + reactive describe/observe; Slice F /qa applied 2026-05-06). Slice A close (lock-released `invoke_fn` + `custom_equals` + R1.3.5.a handshake tier-split + sink-snapshot-on-first-touch race fix) + Slice B (napi-rs binding parity for the new Core methods) + Slice C (CI workflow with rust + clippy + fmt + cargo-deny + TLC steps) + Slice D (M2 starter — `graphrefly-graph` Graph container with sugar constructors + lifecycle pass-throughs) + Slice E+ (M2 read-side: Core inspection + Graph namespace + mount/unmount + describe() JSON + observe() default sink-style) all landed 2026-05-05.

**DS-14 locked + Phase 14 landed in TS** ([archive/docs/SESSION-DS-14-changesets-design.md](https://github.com/graphrefly/graphrefly-ts/blob/main/archive/docs/SESSION-DS-14-changesets-design.md)). Rust port greenlit per [SESSION-rust-port-architecture.md](https://github.com/graphrefly/graphrefly-ts/blob/main/archive/docs/SESSION-rust-port-architecture.md) Part 10 lock. M1 parity work (Slice A+B 2026-05-05 + Slice C-1/C-1.5/C-2 + A-bigger + Slice A close 2026-05-05) is now feature-complete: PAUSE/RESUME, INVALIDATE, COMPLETE/ERROR cascade, TEARDOWN-precedes-COMPLETE, resubscribable terminals, meta-TEARDOWN ordering, §10.12 RAII Subscription, dynamic-rewire audit fix, drop-then-fire wave-end sinks, iterative cascades for deep chains, two-phase tier-then-node flush, RAII batch + cache-snapshot rollback, lock-released `invoke_fn` + `custom_equals`, R1.3.5.a per-tier handshake delivery.

Audit-surfaced concerns deferred to evidence-driven slices live in [`porting-deferred.md`](porting-deferred.md).

**Phase 13.7 v0 bench complete (6 passes); all three axes confirmed in Rust's favor.**

- **Pass 2 — Pure dispatcher:** Rust 2.3×–3.4× faster than TS.
- **Pass 3 — Through napi-rs FFI:** Rust 1.24×–2.03× faster end-to-end; FFI overhead ~51 ns/call.
- **Pass 4 — Cross-Worker shared state:** uniquely possible in Rust; TS literally cannot do it without `SharedArrayBuffer` gymnastics. 4-Worker contention is ~2× slower than single-Worker but the capability is the win.
- **Pass 5 — Tuning attempts:** SmallVec + ChildEdge dep_idx caching both regressed. Pass 2 (ahash) is a local optimum at the current architectural shape; further wins need structural changes (per-subgraph locks, lock-free reads, PGO).
- **Pass 6 — Memory:** 1000-node chain × 1M emits, TOTAL process memory (RSS — Rust+JS combined vs pure JS): TS peaks at 214.59 MiB; Rust+JS at 976 KiB (**TS 225× more at peak**, **97× more post-GC**). JS-heap-only ratio is 3,157× (V8 pressure isolated). Rust eliminates GC pressure on dispatcher hot path.

See [`rust-bench-v0-results.md`](https://github.com/graphrefly/graphrefly-ts/blob/main/docs/research/rust-bench-v0-results.md) for the full numbers.

DS-14 locked 2026-05-05; full M1 parity unblocked.

| Milestone | Crate | Status | Notes |
|-----------|-------|--------|-------|
| Pre-M1 scaffold | (workspace) | ✅ done | 8 crates with stubs; cargo check + test + clippy + fmt all clean |
| M1 (warm-up) | `graphrefly-core` | ✅ landed | `message`/`handle`/`clock`/`boundary` 2026-05-03; 13 unit tests green; `#![forbid(unsafe_code)]` enforced |
| M1 (dispatcher) | `graphrefly-core` | ✅ landed | `node.rs` 2026-05-04: dispatch + equals-sub + first-run gate + setDeps + cycle detection + 22 invariant tests |
| M1 (bench v0) | `graphrefly-core` + `graphrefly-bindings-js` | ✅ landed | criterion bench + vitest bench + napi-rs FFI bench + numbers ([rust-bench-v0-results.md](https://github.com/graphrefly/graphrefly-ts/blob/main/docs/research/rust-bench-v0-results.md)) |
| M1 (parity features) | `graphrefly-core` | ✅ landed | Slice A+B 2026-05-05: PAUSE/RESUME with `PauseState` enum (§10.2), INVALIDATE broadcast, COMPLETE/ERROR cascade + Lock 2.B auto-cascade, TEARDOWN auto-precedes COMPLETE, resubscribable terminal lifecycle, meta-TEARDOWN ordering, §10.12 RAII Subscription, dynamic-rewire fn-fire fix. 102 tests green; clippy + fmt clean |
| M1 (Slice C + Slice A-bigger) | `graphrefly-core` | ✅ landed | 2026-05-05: `batch.rs` extraction, user-facing `Core::batch`/`begin_batch`, R1.3.1.b two-pass cross-node DIRTY-first delivery, transitive `pick_next_fire` (diamond glitch fix), `proptest` framework + 7 protocol-layer invariants, drop-then-fire lock discipline (sinks re-enter Core safely), subscribe-vs-emit race fix, iterative cascades for 5000-node chains. 139 tests green; clippy + fmt clean. |
| M1 (Slice A close — fn + equals re-entrance lift) | `graphrefly-core` | ✅ landed | 2026-05-05: lock-released `BindingBoundary::invoke_fn` (per-iteration drop around fn fire), lock-released `BindingBoundary::custom_equals` (bracket around `commit_emission` equals check), R1.3.5.a per-tier handshake delivery (`[Start]`/`[Data]`/`[Complete\|Error]`/`[Teardown]` as separate sink calls), sink-snapshot-on-first-touch race fix in `pending_notify` (late subscriber installed mid-wave doesn't double-receive already-queued messages). 174 tests green (161 core + 13 new lock-released regression + 10 graph). |
| M1 (napi-rs parity) | `graphrefly-bindings-js` | ✅ landed | 2026-05-05 (Slice B): pause / resume / alloc_lock_id / invalidate / complete / error_int / teardown / add_meta_companion / set_resubscribable / batch_emit_ints / set_deps / has_fired_once / set_pause_buffer_cap / is_paused / pause_lock_count / holds_pause_lock instance methods on `BenchCore`. `ResumeReportJs` napi object. Builds clean. |
| CI scaffold | (workspace) | ✅ landed | 2026-05-05 (Slice C): `.github/workflows/ci.yml` — `cargo fmt --check` / `cargo check` / `cargo clippy --all-targets -D warnings` / `cargo test --all-targets` / `cargo doc` against Rust 1.95 toolchain matrix; cargo-deny job (license + advisory check); bindings-js cdylib build job; TLA+ TLC step runs `handle_protocol_MC` + `wave_protocol_rewire_MC` against the canonical scenarios fetched from `graphrefly-ts`. |
| M2 (starter slice) | `graphrefly-graph` | ✅ landed | 2026-05-05 (Slice D): `Graph` container holding `Arc<Core>`; sugar constructors (`state` / `derived` / `dynamic`); lifecycle pass-throughs (`subscribe` / `emit` / `cache_of` / `complete` / `error` / `teardown` / `invalidate` / `pause` / `resume` / `set_deps` / `set_resubscribable` / `add_meta_companion` / `batch`); `Graph::clone` is cheap Arc bump. 10 tests. Superseded by Slice E+ (the sugar API was hard-broken to take `name` per canonical §3.9). |
| M2 (Slice F — gap fills + reactive) | `graphrefly-graph` + `graphrefly-core` | ✅ landed | 2026-05-06 (Slice F): R3.2.1 named-sugar wrappers, R3.2.3 remove(name), R3.3.1 edges(opts), R3.7.1 signal(kind), Core topology-change notification primitive, reactive describe, reactive observe_all auto-subscribe, GraphRemoveAudit rename. 33 new tests (18 gap_fills + 8 topology + 7 reactive). |
| M2 (read-side + composition) | `graphrefly-graph` + `graphrefly-core` | ✅ landed | 2026-05-05 (Slice E+): Core inspection helpers (`node_ids` / `node_count` / `kind_of` / `deps_of` / `is_terminal` / `is_dirty` / `same_dispatcher`); `TerminalKind` made public (renamed from `DepTerminal`); Graph namespace (canonical §3.5: `add` / `node` / `try_resolve` / `name_of` / `node_count` / `node_names`); canonical §3.9 sugar API (`state(name, initial)` / `derived(name, deps, fn_id, equals)` / `dynamic(name, deps, fn_id, equals)`); mount/unmount (canonical §3.4: `mount` / `mount_new` / `mount_with` / `unmount` / `ancestors`, shared-Core only with `RemoveAudit`); lifecycle (canonical §3.7: `destroy` cascade, `signal_invalidate`); `Graph::describe()` JSON form (canonical §3.6 + Appendix B); `Graph::observe()` / `Graph::observe_all()` default sink-style (canonical §3.6.2 default mode only). **Out of scope** (subsequent slices): reactive describe / observe (`Node<...>` returns), async-iterable observe, non-JSON describe formats (pretty/mermaid/d2/stage-log), explain/reachable, snapshot/CIDs, DS-14 `mutate()`, cross-Core mount, meta annotations, versioning fields. 227 tests workspace-wide post-/qa (was 174 pre-Slice-E+). |
| M3 (Slice A — DepRecord + batch-array) | `graphrefly-core` | ✅ landed | 2026-05-06: Per-dep `DepRecord` struct replaces parallel vecs, `DepBatch` public FFI type, `invoke_fn(&[DepBatch])` signature, R1.3.6.b batch accumulation + wave-end rotation, retain/release discipline on `data_batch`. 267 tests green. |
| M3 (Slice B — FnResult::Batch + R1.3.1.a fix) | `graphrefly-core` | ✅ landed | 2026-05-06: `FnEmission` enum, `FnResult::Batch` variant, `commit_emission_verbatim`, conditional DIRTY (R1.3.1.a), RESOLVED double-settlement fix via `pending_auto_resolve`. 273 tests green; /qa F1–F4 applied (empty Batch settles RESOLVED, dedup, terminal break, must_use). |
| M3 (Slice A /qa — DepBatch tests) | `graphrefly-core` | ✅ landed (post-/qa) | 2026-05-06: `tests/dep_batch.rs` adds 7 regression tests covering K-emit coalescing, `prev_data` rotation, `involved` flag distinction, refcount discipline, multi-dep diamonds. Post-/qa hardening: A1 tightened refcount assertions to exact equality (was lenient `<=+1`); A2 fixed tautological assertion in resolved-in-wave; A3 strengthened multi-dep assertion to exact-handle equality (was `assert_ne! NO_HANDLE`); A4 added wave-1 `prev_data` activation-baseline assertion; A5 documented activation drain discipline. 281 tests green. |
| M3 (napi-rs Batch parity) | `graphrefly-bindings-js` | ✅ landed (post-/qa) | 2026-05-06: `BuiltinBatchFn` enum (`MapAddOneBatch` / `MulTenThenComplete`); `BatchEmissionJs` napi struct (`kind` / `value`); `register_batch_derived(deps, builtin)` exposes `FnResult::Batch` substrate; `batch_emit_messages(node, msgs)` exposes user-facing heterogeneous wave (data/complete/error). Post-/qa hardening: D1 reject `None` value for data/error kinds (R1.2.5 intent); D2 return `napi::Result` with up-front msgs validation before `Core::batch` opens (no panic-mid-batch); D3 dropped speculative `DepBatchJs` (no consumer yet); D4 single registry-lock acquisition across batch-fn fire (was 3 acquisitions); A6 panic-on-non-Int in builtin batch fns (was silent skip); A7 closure-form `Core::batch(\|\|...)`; A8 added R1.3.2.d comment justifying hardcoded `EqualsMode::Identity`. cdylib builds clean. |
| M2 parity-tests widened (D6) | `graphrefly-ts/packages/parity-tests` | ✅ landed | 2026-05-06: `Impl` interface widened with `Graph` + tier-symbols; 6 new scenario files under `scenarios/graph/` (sugar / remove / edges / signal / describe-reactive / observe-all-reactive). 18 passing + 2 skipped (skipped: TS-side R3.7.3 ordering + auto-subscribe-late, both Rust-port-only enhancements not yet backported). 4 fragility items deferred to follow-up — see `porting-deferred.md` "M2 parity-tests fragility (post-/qa)" section. |
| M3 (operators — Slice C-1 transform) | `graphrefly-operators` | ✅ landed | 2026-05-06 (Slice C-1): Core-dispatch architecture (D009–D019). `NodeKind::Operator(OperatorOp)` + `OperatorOpts { equals, partial }` + `Core::register_operator` + `BindingBoundary` extensions (`project_each` / `predicate_each` / `fold_each` / `pairwise_pack` — default unimplemented). Six transform factories: `map` / `filter` / `scan` / `reduce` / `distinctUntilChanged` / `pairwise`. `OperatorBinding` super-trait. 20 operator regression tests + 8 parity scenarios under `packages/parity-tests/scenarios/operators/transform.test.ts`. 300 tests workspace-wide (was 273 post-Slice-B). |
| M3 (operators — Slice C-2 combinators) | `graphrefly-operators` + `graphrefly-core` | ✅ landed | 2026-05-06 (Slice C-2): Multi-dep combinator operators (D020–D023). 3 new `OperatorOp` variants (`Combine { pack_fn }` / `WithLatestFrom { pack_fn }` / `Merge`). `BindingBoundary::pack_tuple` + `OperatorBinding::register_packer`. `snapshot_op_all_latest` multi-dep snapshot helper. `fire_op_combine` (any-dep-fire tuple emission + post-warmup INVALIDATE NO_HANDLE guard), `fire_op_with_latest_from` (fire-on-primary-only + first-fire gate-release), `fire_op_merge` (zero-FFI handle forwarding). 3 factory functions in `graphrefly-operators::combine`. 14 Rust tests + 6 parity scenarios. Post-/qa: F1 `fire_op_distinct` double-release fix (working-copy retain discipline), F2 `fire_op_combine` INVALIDATE guard, A1 withLatestFrom `debug_assert` corrected to `== 2`, A3 Reduce error-retain comment accuracy, A4 pairwise first-value comment. Known v1 divergence: merge error cascade is all-deps-terminal (Rust Core standard) vs first-error-terminates (TS producer pattern). Combine custom tuple-equality deferred (D023). |
| M4 | `graphrefly-storage` | ⏸ blocked | After M2 + DS-14 (`restoreSnapshot mode: "diff"` WAL replay shape) |
| M5 | `graphrefly-structures` | ⏸ blocked | After M2 + DS-14 (op-log changesets) — STRONG DEFER per Phase 14 guardrail |
| M6 | `graphrefly-bindings-py` | ⏸ blocked | After M5; closes graphrefly-py G.6 parity gap |
| any | `graphrefly-bindings-wasm` | ⏸ blocked | Lands alongside napi-rs progression |

## Phase 13.7 bench study — re-decision gate

Per [implementation-plan.md Phase 13.7](https://github.com/graphrefly/graphrefly-ts/blob/main/docs/implementation-plan.md#phase-137):

After M1 dispatcher + bench data lands, **PAUSE** before extending to M2/M3/M4/M5. DS-14 must lock before any further Rust work — confirmed user direction 2026-05-03.

## Pre-M1 dependencies (in graphrefly-ts)

- [x] **Phase 13.6.A — rules/invariants audit + locking** ✅ locked 2026-05-03 ([implementation-plan-13.6-canonical-spec.md](https://github.com/graphrefly/graphrefly-ts/blob/main/docs/implementation-plan-13.6-canonical-spec.md), 24 invariant locks).
- [x] **`setDeps()` design + TLA+ verification** ✅ verified 2026-05-03 ([wave_protocol_rewire.tla](https://github.com/graphrefly/graphrefly-ts/blob/main/docs/research/wave_protocol_rewire.tla), 35,950 distinct states clean; [rewire-design-notes.md](https://github.com/graphrefly/graphrefly-ts/blob/main/docs/research/rewire-design-notes.md)).
- [ ] **Phase 14 design lock — DS-14.** Gates M2/M4/M5 *and* full M1 commit (the Phase 13.7 bench study runs ahead of this gate).

## M1 entry checklist

- [x] Add CI: `.github/workflows/ci.yml` with `cargo check`, `cargo test`, `cargo clippy`, `cargo fmt --check`, `cargo deny check` against the Rust 1.95 matrix. ✅ Slice C (2026-05-05).
- [x] Port `src/core/messages.ts` → `crates/graphrefly-core/src/message.rs` ✅ 2026-05-03 (warm-up; 4 tests).
- [x] Port `src/core/clock.ts` → `crates/graphrefly-core/src/clock.rs` ✅ 2026-05-03 (4 tests).
- [x] Set up newtypes: `NodeId(u64)`, `HandleId(u64)`, `FnId(u64)`, `LockId(u64)` in `crates/graphrefly-core/src/handle.rs` ✅ 2026-05-03 (3 tests).
- [x] Define `BindingBoundary` trait in `crates/graphrefly-core/src/boundary.rs` mirroring the TS prototype ✅ 2026-05-03 (2 tests, including `Send + Sync` compile-time check).
- [x] Crate-root `#![forbid(unsafe_code)]` per CLAUDE.md Rust invariant 1 ✅ 2026-05-03.
- [x] Port `core.ts` dispatcher: registration, subscribe, push-on-subscribe, wave engine, equals-substitution, first-run gate ✅ 2026-05-04 (`crates/graphrefly-core/src/node.rs`).
- [x] `set_deps()` per [rewire-design-notes.md](https://github.com/graphrefly/graphrefly-ts/blob/main/docs/research/rewire-design-notes.md) — substrate primitive ✅ 2026-05-04 (self-rewire + cycle rejection enforced).
- [x] 22 invariant tests ported from `core.test.ts` ✅ 2026-05-04 (`crates/graphrefly-core/tests/dispatcher.rs` + `setdeps.rs`).
- [x] Bench harness (criterion) — dispatcher hot path, large fanout ✅ 2026-05-04 (`crates/graphrefly-core/benches/dispatcher.rs`).
- [x] napi-rs binding stub for end-to-end FFI bench ✅ 2026-05-04 (`crates/graphrefly-bindings-js/src/core_bindings.rs`).
- [x] Port `_invariants.ts` property-test fixtures to `proptest`. ✅ Slice C-2 (2026-05-05) — 7 protocol-layer invariants ported (#1, #2, #3, #4, #5, #7×3 sub-tests, #11×2 sub-tests = 10 proptest cases). Operator-layer (#12+) and re-entrance-dependent (#8, #9) invariants deferred.
- [x] Port `batch.ts`: wave coalescing extracted to its own module. ✅ Slice C-1 (2026-05-05) — `crates/graphrefly-core/src/batch.rs`; sibling-module `impl Core` split, no semantic change, 109 tests still green.
- [x] User-facing `batch()` + RAII `BatchGuard` per-Core scoping. ✅ Slice C-1.5 (2026-05-05) — `Core::batch(|| {...})` and `Core::begin_batch() -> BatchGuard`. Per-Core in_tick scoping (not TS's process-global). Panic-discard: pending tier-3+ work cleared, refcount retains released, no half-fired sinks during unwind.
- [x] R1.3.1.b cross-node two-phase delivery. ✅ Slice C-1.5 (2026-05-05) — `flush_notifications` now two-pass tier-then-node: phase 1 (DIRTY) flushes across all nodes before phase 2 (DATA/RESOLVED + INVALIDATE), then phase 3 (COMPLETE/ERROR), then phase 4 (TEARDOWN). Multi-node subscribers observe "all involved nodes dirty" before any settle. Latent gap closed; `Recorder` flat-event tests unaffected.
- [x] PAUSE/RESUME with lockId set (R1.2.6, R2.6). ✅ Slice A+B (2026-05-05).
- [x] INVALIDATE broadcast (R1.2 `Message::Invalidate`). ✅ Slice A+B (2026-05-05).
- [x] COMPLETE/ERROR cascade + auto-cascade gating (Lock 2.B). ✅ Slice A+B (2026-05-05).
- [x] TEARDOWN auto-precedes COMPLETE (R2.6.4 / Lock 6.F). ✅ Slice A+B (2026-05-05).
- [x] Meta TEARDOWN ordering (R1.3.9.d). ✅ Slice A+B (2026-05-05).
- [x] Resubscribable terminal lifecycle (R2.2.7, R2.5.3). ✅ Slice A+B (2026-05-05).
- [x] §10.12 RAII `Subscription` (public API). ✅ Slice A+B (2026-05-05).
- [x] Audit fix: dynamic-node rewire fn-fire reset. ✅ Slice A+B (2026-05-05).
- [x] Cross-Worker bench (process-global `static OnceLock<Arc<BenchCore>>`; verified shared state across Workers; Rust 4-Worker shared 1.72 M/s vs TS 4-Worker isolated 4.72 M/s — capability over throughput) ✅ 2026-05-04.
- [x] Memory bench: 1000-node chain × 1M emits — TOTAL process memory (RSS): TS 214.59 MiB peak, Rust+JS 976 KiB peak (**225× ratio at peak, 97× post-GC**) ✅ 2026-05-04.
- [x] Tuning Pass 5: SmallVec + ChildEdge dep_idx caching both regressed; Pass 2 (ahash) is a local optimum. Further wins need per-subgraph locks / lock-free reads / PGO ✅ 2026-05-04.
- [x] Stand up TLC-runs-in-CI step using existing `wave_protocol_*_MC.tla` + the new `wave_protocol_rewire_MC.tla` against the handle interpretation. ✅ Slice C (2026-05-05) — `tlc` job in `.github/workflows/ci.yml` runs `handle_protocol_MC` + `wave_protocol_rewire_MC`.

## Reference impl

The TS prototype at `~/src/graphrefly-ts/src/__experiments__/handle-core/` is the reference implementation for the handle protocol. Port it module-for-module:

| TS file | Rust target |
|---------|-------------|
| `core.ts` (~370 lines, all Core dispatch) | `crates/graphrefly-core/src/{message,handle,boundary,node,batch}.rs` |
| `bindings.ts` (~370 lines, value registry + FFI counters + dynamic builder) | `crates/graphrefly-bindings-js/src/{value_registry,counters,*}.rs` |
| `core.test.ts` (13 tests) | `crates/graphrefly-core/tests/` (integration tests) |
| `extensions.test.ts` (9 tests, FFI counters + dynamic + refcount) | same |

The Rust impl should pass the same 22 invariant tests plus additional Rust-specific tests (Send/Sync discipline, panic safety, leak detection under loom).

## Open architectural questions

Moved to [`porting-deferred.md`](porting-deferred.md) under the "Open questions from SESSION-rust-port-architecture.md Part 6" section. That file is the single home for surfaced-but-deferred concerns; this status file tracks landed-vs-pending milestones.

## M2 — closed 2026-05-06

M2 is formally closed. All canonical §3 surfaces scoped for the Rust port's M2 milestone have landed across Slice D (starter), Slice E+ (read-side + composition), Slice E+ /qa (adversarial review), and Slice F (gap fills + reactive).

### What landed in Slice F (2026-05-06)

**Port-coverage gap fills (R3.2.1, R3.2.3, R3.3.1, R3.7.1):**
- `Graph::set(name, handle)` / `get(name)` / `invalidate_by_name(name)` / `complete_by_name(name)` / `error_by_name(name, handle)` — named-sugar wrappers (canonical R3.2.1).
- `Graph::remove(name) -> Result<GraphRemoveAudit, RemoveError>` — removes individual named nodes (sends TEARDOWN) or delegates to `unmount()` for mounted subgraphs (canonical R3.2.3).
- `Graph::edges(recursive: bool) -> Vec<(String, String)>` — direct edge accessor with recursive mount-walking and `_anon_<id>` for unnamed deps (canonical R3.3.1).
- `Graph::signal(kind: SignalKind)` with `SignalKind::Invalidate` / `Pause(lock)` / `Resume(lock)` — general broadcast (canonical R3.7.1).
- `RemoveAudit` renamed to `GraphRemoveAudit` — avoids name collision with future Core-level `RemoveAudit`.

**Core topology-change notification primitive:**
- `TopologyEvent` enum: `NodeRegistered(NodeId)`, `NodeTornDown(NodeId)`, `DepsChanged { node, old_deps, new_deps }`.
- `Core::subscribe_topology(sink) -> TopologySubscription` — RAII subscription, fires synchronously after state lock drops from registration / teardown / set_deps call sites.
- `TopologySubscription` drops cleanly (even if Core is already dropped, via `Weak` ref).
- Sinks are NOT nodes — they sit outside the reactive graph to avoid circularity.

**Reactive describe:**
- `Graph::describe_reactive(sink) -> ReactiveDescribeHandle` — sink fires with a fresh `GraphDescribeOutput` on every namespace mutation (add, remove, destroy). RAII handle; dropping unsubscribes.
- Uses Graph-level namespace-change sinks (not Core topology events) to avoid the ordering gap where Core fires `NodeRegistered` before `Graph::add()` inserts the name.

**Reactive observe_all (auto-subscribe):**
- `Graph::observe_all_reactive() -> GraphObserveAllReactive` — subscribes to all current named nodes AND auto-subscribes late-added nodes via namespace-change notifications.
- Drop-order discipline: namespace sink unsubscribes BEFORE inner `Subscription`s drop, preventing a deadlock where the sink closure's `Arc<inner>` would be the last reference and try to drop `Subscription`s under `CoreState` lock.

### What did NOT land (deferred from M2 scope)

- **`tagFactory` / `resourceProfile` / `setVersioning`** — deferred per user direction; tracked in `porting-deferred.md`.
- **napi-rs `BenchGraph` class** — deferred; needs napi-rs dev environment and is a binding-layer concern, not a correctness surface.
- **Reactive `describe({ reactive: "diff" })` (changeset mode)** — deferred to Phase 14 op-log changeset protocol.
- **`describe({ explain })` / `describe({ reachable })`** — deferred (causal chain + reachability walks).
- **Non-JSON describe formats** — pretty / mermaid / d2 / stage-log.
- **Async-iterable observe modes** — structured / timeline / causal / derived.

### Test count post-M2-close

268 tests green workspace-wide (was 260 pre-/qa, 227 post-Slice-E+ /qa). New tests: 18 gap_fills + 10 topology + 11 reactive = 39 new (post-/qa: +1 gap_fills regression for P1, +2 topology for P2 cascade, +5 reactive net for P3/P5/P7 push-on-subscribe). `cargo clippy --all-targets -D warnings` clean. `cargo fmt --check` clean. `#![forbid(unsafe_code)]` preserved across all crates that had it pre-/qa.

### M3 blockers identified

- ✅ **R1.3.6.b batched delivery** — **RESOLVED** in M3 Slice A (2026-05-06). `DepRecord.data_batch: SmallVec<[HandleId; 1]>` accumulates per-dep DATA handles per wave. `DepBatch` FFI type exposes `data`/`prev_data`/`involved` to `invoke_fn`. Wave-end rotation in `clear_wave_state` handles retain/release discipline. SmallVec inline-1 avoids heap alloc for the common single-emit case.

## M3 Slice A — Per-dep DepRecord + R1.3.6.b batch-array delivery — landed 2026-05-06

Core-level structural refactor. Prerequisite for M3 operator ports (operators need full per-dep batch arrays per wave, not just the latest handle).

### What landed

- **`DepRecord` struct** — replaces parallel `deps: Vec<NodeId>` / `dep_handles: Vec<HandleId>` / `dep_terminals: Vec<Option<TerminalKind>>` in `NodeRecord` with a unified `dep_records: Vec<DepRecord>`. Each record: `node`, `prev_data`, `dirty`, `involved_this_wave`, `data_batch: SmallVec<[HandleId; 1]>`, `terminal`.
- **`DepBatch` public FFI type** — passed to `BindingBoundary::invoke_fn(&[DepBatch])`. Fields: `data` (batch array), `prev_data` (last wave's final DATA), `involved` (dep dirtied→settled this wave). Helper methods: `latest()`, `is_sentinel()`.
- **`deliver_data_to_consumer` batch accumulation** — pushes to `data_batch` with `retain_handle` instead of overwriting a single slot. Each DATA handle in the batch owns its own retain share.
- **Wave-end rotation in `clear_wave_state`** — last batch entry's retain transfers to `prev_data`; old `prev_data` released; all earlier batch entries released via `deferred_handle_releases`.
- **SmallVec<[HandleId; 1]>** — inline storage for the common single-emit case; heap alloc only for multi-emit batch inside `batch()` scope.
- **Helper methods on `NodeRecord`** — `dep_ids()`, `dep_ids_vec()`, `dep_count()`, `has_sentinel_deps()`, `dep_index_of()`, `all_deps_terminal()`.
- **All `BindingBoundary` implementations updated** — `TestBinding`, `StubBinding`, `BenchBinding`, `ReentrantBinding`, boundary.rs `TestBinding`, bench `BenchBinding` — all use `&[DepBatch]` signature with `db.latest()` for backward-compatible value resolution.

### Files touched

| File | Change |
|------|--------|
| `crates/graphrefly-core/src/boundary.rs` | `DepBatch` struct + `invoke_fn` signature |
| `crates/graphrefly-core/src/node.rs` | `DepRecord` struct, `NodeRecord` refactor, all lifecycle methods |
| `crates/graphrefly-core/src/batch.rs` | `fire_fn`, `deliver_data_to_consumer`, `commit_emission`, `transitive_upstream_settled` |
| `crates/graphrefly-core/src/lib.rs` | `DepBatch` re-export |
| `crates/graphrefly-core/tests/common/mod.rs` | `TestBinding::invoke_fn` |
| `crates/graphrefly-core/tests/lock_released.rs` | `ReentrantBinding::invoke_fn` |
| `crates/graphrefly-core/benches/dispatcher.rs` | `BenchBinding::invoke_fn` |
| `crates/graphrefly-graph/tests/common/mod.rs` | `StubBinding::invoke_fn` |
| `crates/graphrefly-bindings-js/src/core_bindings.rs` | `BenchBinding::invoke_fn` |

### Test count

267 tests green workspace-wide (no new tests in this slice — structural refactor only; all 267 existing tests pass against the new `DepRecord`/`DepBatch` substrate). `cargo clippy --all-targets -D warnings` clean. `cargo fmt --check` clean. `#![forbid(unsafe_code)]` preserved.

### What did NOT land in M3 Slice A

- ~~**New batch-accumulation tests**~~ — **RESOLVED** by `tests/dep_batch.rs` (2026-05-06): 7 regression tests covering K-emit coalescing, `prev_data` rotation, per-dep `involved` flags, refcount discipline, multi-dep diamonds.
- **Reusable scratch buffer** — Q2 decision was C (reusable per-Core), but the current impl allocates a fresh `Vec<DepBatch>` per fire. Deferred to bench-driven optimization when the allocation shows up in profiling.
- ~~**napi-rs binding parity for DepBatch**~~ — **RESOLVED** by `BuiltinBatchFn` + `BatchEmissionJs` + `DepBatchJs` + `register_batch_derived` + `batch_emit_messages` (2026-05-06). `FnResult::Batch` substrate reachable from JS via the builtin-fn registry; user-facing heterogeneous waves (data/complete/error) flow through `batch_emit_messages`.

---

## M3 Slice B — FnResult::Batch + commit_emission DIRTY fix — landed 2026-05-06

Core-level extension adding multi-emission support to `invoke_fn` return type. Enables operators to emit multiple DATA values and terminal messages within a single wave (modeling TS `actions.down(msgs)`). Also fixes R1.3.1.a DIRTY synthesis to be conditional on not-already-dirty, and fixes a latent double-settlement bug in RESOLVED child propagation.

### What landed

- **`FnEmission` enum** — `Data(HandleId)`, `Complete`, `Error(HandleId)`. Elements of a multi-message wave, passed inside `FnResult::Batch`.
- **`FnResult::Batch` variant** — `{ emissions: SmallVec<[FnEmission; 2]>, tracked: Option<Vec<usize>> }`. Models `actions.down(msgs)` from the canonical spec. Processed in sequence within the same wave. No equals substitution on any Data emission (R1.3.2.d / R1.3.3.c).
- **`commit_emission_verbatim`** — Same as `commit_emission`'s DATA branch but skips Phase 2 (equals check). Used by `FnResult::Batch` processing where multi-message waves pass through verbatim.
- **`fire_fn` Phase 3–4 rewrite** — `FireAction` enum (`None`, `SingleData`, `Batch`) dispatches: single Data → `commit_emission` (equals substitution applies), Batch → iterate emissions through `commit_emission_verbatim` / `complete()` / `error()`.
- **R1.3.1.a conditional DIRTY** — `commit_emission` and `commit_emission_verbatim` only queue Dirty if `!already_dirty`. Fixes spec violation where multiple DATA emissions in the same wave produced spurious Dirty messages.
- **RESOLVED child propagation fix** — Removed eager Resolved queueing from the RESOLVED branch's child propagation. Replaced with `pending_auto_resolve` tracking + post-drain sweep in `drain_and_flush`. Fixes double-settlement bug (1 Dirty balanced by 2 settlements) exposed by the R1.3.1.a conditional change, where a child could receive Resolved from propagation AND Data from its own commit_emission in the same wave. The sweep routes through `queue_notify` so paused nodes get the auto-Resolved into their pause buffer.
- **`RawFnImpl` test infrastructure** — `register_raw_fn` on `TestBinding` allows tests to return arbitrary `FnResult` variants (including Batch) from fn callbacks.

### Files touched

| File | Change |
|------|--------|
| `crates/graphrefly-core/src/boundary.rs` | `FnEmission` enum, `FnResult::Batch` variant, test match arm |
| `crates/graphrefly-core/src/batch.rs` | `fire_fn` rewrite, `commit_emission_verbatim`, R1.3.1.a conditional DIRTY, RESOLVED child propagation fix, auto-resolve sweep |
| `crates/graphrefly-core/src/node.rs` | `pending_auto_resolve` field on `CoreState`, `clear_wave_state` update |
| `crates/graphrefly-core/src/lib.rs` | `FnEmission` re-export |
| `crates/graphrefly-core/tests/common/mod.rs` | `RawFnImpl`, `register_raw_fn`, `invoke_fn` dispatch |
| `crates/graphrefly-core/tests/batch.rs` | 6 new tests |

### New tests (6)

1. `batch_fn_result_multi_data_delivers_all_values` — Batch with 2 Data emissions, both delivered to subscriber + propagated to grandchild
2. `batch_fn_result_data_then_complete` — Data + Complete in Batch, terminal cascade
3. `batch_fn_result_data_then_error` — Data + Error in Batch, error cascade
4. `batch_fn_result_dirty_queued_once_per_wave` — R1.3.1.a: only 1 Dirty per wave with 3 Data Batch
5. `batch_fn_result_no_equals_substitution` — R1.3.2.d: Batch skips equals even when handle equals cache
6. `batch_fn_result_propagates_to_grandchild` — children accumulate batch entries from Batch parent

### Test count

273 tests green workspace-wide (was 267 pre-slice). `cargo clippy --all-targets -D warnings` clean. `cargo fmt --check` clean. `#![forbid(unsafe_code)]` preserved.

### /qa fixes (4)

**F1 — Empty `FnResult::Batch` settlement (R1.3.1.a).** An empty emissions vec left the node dirty with no tier-3 settlement. Now treated as equivalent to `FnResult::Noop` — queues RESOLVED if node was dirty.

**F2 — `pending_auto_resolve` dedup.** Changed from `Vec<NodeId>` to `AHashSet<NodeId>`. Diamond fan-in where multiple parents settle RESOLVED no longer produces duplicate entries (the post-drain sweep was already idempotent via the tier-3 check, but the `Vec` grew unboundedly on wide diamonds).

**F3 — Break after terminal in Batch loop.** `commit_batch` now stops processing after the first `Complete` or `Error` emission (R1.3.4.a). Remaining handles are released. Extracted Batch processing into `commit_batch` helper (clippy `too_many_lines` fix).

**F4 — `#[must_use]` on `FnEmission`.** Added `#[must_use = "FnEmission may contain handles that must be processed or released"]` to prevent silent handle leaks from discarded emissions.

---

## M3 Slice C-1 — transform operators (Core-dispatch architecture) — landed 2026-05-06

First operators slice. Lands the substrate (`NodeKind::Operator`, `OperatorOp` discriminant, `partial` flag, `BindingBoundary` extension methods, `Core::register_operator`, per-operator dispatch in `fire_operator`) plus the six transform operators (`map` / `filter` / `scan` / `reduce` / `distinctUntilChanged` / `pairwise`) in one bundled slice (Q1=A per `docs/rust-port-decisions.md` D014).

### What landed

- **`NodeKind::Operator(OperatorOp)` enum variant** in [`crates/graphrefly-core/src/node.rs`](../crates/graphrefly-core/src/node.rs). `OperatorOp` discriminant carries per-operator `FnId`s and seeds: `Map { fn_id }`, `Filter { fn_id }`, `Scan { fn_id, seed }`, `Reduce { fn_id, seed }`, `DistinctUntilChanged { equals_fn_id }`, `Pairwise { fn_id }`. `NodeKind::skips_auto_cascade()` accessor flags Reduce so it intercepts upstream COMPLETE.
- **`OperatorOpts { equals, partial }`** registration struct (D017). `Core::register_operator(deps, op, opts) -> NodeId` validates seeds (Scan/Reduce reject `NO_HANDLE`), retains the seed handle for the `operator_state` slot's lifetime, registers the inverted edge map.
- **`partial: bool`** field on `NodeRecord` (D011 / R5.4 / D019) — plumbed through `register_state` / `register_derived` / `register_dynamic` as a positional arg. Operators may opt out of the R2.5.3 first-run gate via `OperatorOpts::partial = true`.
- **`operator_state: HandleId`** slot on `NodeRecord` for stateful operators (Scan/Reduce acc, Distinct/Pairwise prev). `NO_HANDLE` for stateless operators / non-operator kinds. Released by `Drop for CoreState` and re-seeded on `reset_for_fresh_lifecycle` (resubscribable lifecycle reset).
- **`BindingBoundary` extension** in [`crates/graphrefly-core/src/boundary.rs`](../crates/graphrefly-core/src/boundary.rs): `project_each` / `predicate_each` / `fold_each` / `pairwise_pack` with default `unimplemented!()` so non-operator bindings don't pay. Each method takes a flattened `&[HandleId]` slice and returns the per-input result; one FFI per fire (R5.7 batch-mapping shape).
- **`fire_operator` dispatch** in [`crates/graphrefly-core/src/batch.rs`](../crates/graphrefly-core/src/batch.rs). Branches on `OperatorOp` to per-variant helpers (`fire_op_map` / `fire_op_filter` / `fire_op_scan` / `fire_op_reduce` / `fire_op_distinct` / `fire_op_pairwise`). Each follows the existing three-phase shape (snapshot inputs → drop lock → call binding FFI → reacquire). Filter D018: full-reject queues `[Dirty, Resolved]` to settle. Reduce: terminal-aware (emits `Data(acc)` + `Complete` on upstream COMPLETE; cascades ERROR verbatim).
- **Reduce auto-cascade opt-out.** `terminate_node`'s child propagation uses a new `ChildAction { None | Cascade(t) | QueueFire }` enum: when a child opts out via `kind.skips_auto_cascade()`, push to `pending_fires` instead of cascading so the operator's `fire_operator` sees the dep terminal and emits the seed/acc.
- **`graphrefly-operators` crate** ([`crates/graphrefly-operators/`](../crates/graphrefly-operators/)). New `OperatorBinding` super-trait of `BindingBoundary` (D015) for closure registration: `register_projector` / `register_predicate` / `register_folder` / `register_equals` / `register_pairwise_packer`, each taking `Box<dyn Fn(...) + Send + Sync>` (D016). Six transform factories accept `&Core` + `&Arc<dyn OperatorBinding>` + `source: NodeId` + user closure (+ seed for folders) and return `OperatorRegistration { node, fn_id }`. `*_with` variants accept explicit `OperatorOpts`.

### Files touched

| Path | Change |
|------|--------|
| `crates/graphrefly-core/src/node.rs` | `OperatorOp` + `OperatorOpts` + `NodeKind::Operator` + `partial`/`operator_state` fields + `register_operator` + `register_state/derived/dynamic` partial arg + `ChildAction` cascade-gating + reset_for_fresh_lifecycle re-seed + Drop release |
| `crates/graphrefly-core/src/boundary.rs` | 4 new `BindingBoundary` methods (default unimplemented) |
| `crates/graphrefly-core/src/batch.rs` | `fire_fn` dispatch fork → `fire_operator` + 6 per-variant helpers + `settle_dirty_resolved` + `snapshot_op_dep0` + partial-flag check in `fire_regular`'s skip gate |
| `crates/graphrefly-core/src/lib.rs` | re-export `OperatorOp` + `OperatorOpts` |
| `crates/graphrefly-operators/Cargo.toml` | dev-dep ahash; deps parking_lot + smallvec |
| `crates/graphrefly-operators/src/lib.rs` | module wiring + re-exports |
| `crates/graphrefly-operators/src/binding.rs` | `OperatorBinding` super-trait |
| `crates/graphrefly-operators/src/transform.rs` | 6 factories + 6 `_with` variants + `OperatorRegistration` |
| `crates/graphrefly-operators/tests/common/mod.rs` | `InnerBinding` (`BindingBoundary` + `OperatorBinding`) + `OpRuntime` + `Recorder` |
| `crates/graphrefly-operators/tests/transform.rs` | 20 regression tests |
| `crates/graphrefly-graph/src/graph.rs` | pass `false` for partial through 3 sugar wrappers |
| `crates/graphrefly-graph/src/describe.rs` | `NodeKind::Operator(_)` arms in `type_str_of` and `status_of` |
| `crates/graphrefly-bindings-js/src/core_bindings.rs` | call-site updates for `partial` arg |
| `crates/graphrefly-core/tests/{common/mod.rs, batch.rs, dep_batch.rs, lock_released.rs}` | call-site updates |
| `crates/graphrefly-core/benches/dispatcher.rs` | call-site updates |
| `crates/graphrefly-graph/tests/{describe.rs, gap_fills.rs, namespace.rs}` | call-site updates |

### Tests added (Rust)

20 new tests in [`crates/graphrefly-operators/tests/transform.rs`](../crates/graphrefly-operators/tests/transform.rs):

- `map_per_value_projection_in_single_emit_wave`
- `map_batch_emits_one_dirty_per_wave_per_r1_3_1_a` (R1.3.1.a)
- `filter_passes_only_matching_items`
- `filter_full_reject_emits_dirty_resolved_per_d018` (D012/D018)
- `filter_mixed_wave_no_resolved_when_at_least_one_passes`
- `scan_emits_running_accumulator_per_input`
- `scan_persists_acc_across_waves`
- `reduce_emits_acc_on_upstream_complete`
- `reduce_no_data_emits_seed_on_complete`
- `reduce_propagates_upstream_error`
- `distinct_until_changed_suppresses_consecutive_duplicates`
- `distinct_emits_on_first_value_always`
- `pairwise_emits_pairs_starting_from_second_value`
- `pairwise_first_value_alone_emits_nothing`
- `map_does_not_leak_handles_after_drop`
- `scan_seed_retain_balances_on_core_drop` (refcount discipline)
- `map_does_not_fire_on_sentinel_source_until_first_emit` (R2.5.3)
- `operator_opts_default_is_identity_and_gated`
- `no_handle_const_is_recognized`
- `op_binding_is_send_and_sync` (CLAUDE.md Rust invariant 2)

### Tests added (TS-side parity scenarios)

8 new scenarios in [`packages/parity-tests/scenarios/operators/transform.test.ts`](https://github.com/graphrefly/graphrefly-ts/blob/main/packages/parity-tests/scenarios/operators/transform.test.ts) parameterized via `describe.each(impls)`:

- map: per-value projection
- filter: passes matching + full-reject settles
- scan: running-accumulator emission
- reduce: emit-on-COMPLETE (no DATA → seed; with DATA → final acc)
- distinctUntilChanged: adjacent-dup suppression
- pairwise: (prev, current) emission

`Impl` interface widened with `map` / `filter` / `scan` / `reduce` / `distinctUntilChanged` / `pairwise` plus `RESOLVED` + `DIRTY` tier symbols. All scenarios pass against `legacyImpl`; `rustImpl` activates with `@graphrefly/native` publication.

### Test count

300 tests green workspace-wide (was 273 post-Slice-B; +20 operator tests + 7 indirect via the partial-flag plumbing). `cargo clippy --all-targets -D warnings` clean. `cargo fmt --check` clean. `#![forbid(unsafe_code)]` preserved across all crates.

### Decisions logged

D014–D019 in [`docs/rust-port-decisions.md`](rust-port-decisions.md): one-slice scope (A), `OperatorBinding` lives in operators crate, `Fn(HandleId) -> HandleId` projector signature, equals defaults to Identity, filter silent-drop with self-RESOLVED on full-reject, `partial: bool` added to all three legacy register methods.

### What did NOT land in Slice C-1

- **napi-rs operator binding parity** — `BenchCore` currently has no `register_map` / `register_filter` / etc. surface. Operator FFI for JS callbacks via napi tsfn is post-bench-study work; tracked as M3 close-gate residual (see `porting-deferred.md`).
- **pyo3 operator binding** — same deferral, lands with M6.
- **Operator describe/observe enrichment** — `describe()` exposes `Operator` as `NodeTypeStr::Operator` but doesn't surface the per-operator discriminant (Map vs Filter etc.) yet. Belongs with the future canonical describe extension that surfaces operator catalog metadata.
- **`partial`-mode operators** — no transform operator uses partial=true. Plumbing is in place for combine/withLatestFrom in future slices.
- **Operator `OperatorOpts.equals` for wire-level dedup** — currently a no-op for transform operators (which use `commit_emission_verbatim`). Reserved for future operators that DO want wire-level equals substitution.
- **`fire_operator` first-run gate fast-path** — gate check uses linear-scan `has_sentinel_deps()`. Same shape as the §10.13 deferred concern; bench-driven re-look.

---

## Slice F /qa — adversarial review fixes — landed 2026-05-06

QA pass on Slice F surfaced ~25 active findings via two adversarial subagents (Blind Hunter, Edge Case Hunter); ~10 rejected (matched existing deferred entries or false alarms). Applied 7 priority patches (P1–P7) + 7 auto-applicable polish (A1–A7); 6 new deferral entries (D1–D6) added to `porting-deferred.md`.

### What landed

**P1 — `Graph::remove()` namespace-clear-after-teardown reorder.** Pre-fix: `shift_remove(name)` ran inside the lock block before `core.teardown(node_id)` fired; sinks observing TEARDOWN would call `name_of(id)` mid-cascade and see `None`. Now mirrors `destroy()`'s Slice E+ /qa B3 ordering: snapshot id under lock, drop lock, teardown cascades with namespace intact, then reacquire lock and clear name+inverse. Regression test `remove_preserves_namespace_during_teardown_cascade` confirms via a sink observing Teardown and resolving `name_of(s)` → `Some("temp")`. R3.2.3 / R3.7.3 ordering.

**P2 — Cascaded teardown fires `NodeTornDown` for every cascaded id.** Pre-fix: `Core::teardown(root)` fired `NodeTornDown(root)` only, leaving meta companions and downstream auto-cascaded consumers invisible to topology subscribers. Reactive describe / observe_all silently retained ghost nodes. Now `teardown_inner` returns `Vec<NodeId>` of every node that emits a TEARDOWN; `Core::teardown` iterates the collection and fires `NodeTornDown` for each AFTER `run_wave` returns and the lock drops. Regression tests `teardown_cascade_fires_node_torn_down_for_each_node` (root + auto-cascaded derived) and `teardown_cascade_fires_for_meta_companions` (root + meta) confirm.

**P3 — `mount` / `mount_new` / `unmount` fire `fire_namespace_change` on the parent.** Pre-fix: only `add()` / `remove()` / `destroy()` fired the namespace-change hook; mount/unmount were namespace-changing operations that reactive describe / observe_all missed silently. Now mount-path operations call `parent.fire_namespace_change()` AFTER the parent inner lock drops (and AFTER the child's destroy completes for unmount). Regression tests `describe_reactive_fires_on_mount_new`, `describe_reactive_fires_on_unmount`, `observe_all_reactive_handles_late_mount`.

**P4 — `GraphObserveAllReactive::subscribe()` race fix: install ns listener BEFORE initial snapshot.** Pre-fix: snapshot taken first → namespace listener installed last. A node added between the two was missed (snapshot didn't see it; listener wasn't yet installed when add fired). Now listener registers first, then the snapshot pass walks current names; `inner.subscribed.insert(id)` dedups against the listener's first fire so an idempotent overlap is harmless.

**P5 — `GraphObserveAllReactive::subscribe()` panics on second call (subscribe-once contract).** Pre-fix: a second `subscribe(...)` call overwrote `self.ns_sink_id` without unsubscribing the prior, leaking the first namespace sink permanently (until graph drop). Now `assert!(self.ns_sink_id.is_none(), …)` enforces the v1 single-shot contract. Rebuild a fresh `observe_all_reactive()` handle to install another sink. Regression test `observe_all_reactive_subscribe_twice_panics` (`#[should_panic(expected = "single-shot")]`).

**P6 — Reactive sinks capture `Weak<Mutex<GraphInner>>` + `Core` (clone) instead of strong `Graph`.** Pre-fix: sink closures held `graph.clone()` (strong Arc<inner>); the closure was stored INSIDE `inner.namespace_sinks`, forming a cycle. Normal RAII (drop handle → unsubscribe → break cycle) worked, but `mem::forget(handle)` would extend the Arc cycle, leaking GraphInner via the namespace_sinks → sink → Graph → namespace_sinks path. Now sinks capture a `Weak<Mutex<GraphInner>>` + `Core` clone; on each fire they `weak.upgrade()` and reconstruct `Graph { core, inner }` for the `describe()` / namespace walk. The cycle is broken at the namespace_sinks → sink edge. (Note: the handle's own `graph: Graph` strong field still keeps GraphInner alive while the handle exists — by design.)

**P7 — `Graph::describe_reactive(sink)` fires the initial snapshot synchronously before installing the listener.** Pre-fix: sink fired only on later changes; the current state was silently dropped. Per R3.6.1 / spec §2.5.2 push-on-subscribe, reactive observers must receive the cached state. Now `sink(&self.describe())` runs once before `subscribe_namespace_change`, so consumers always start from a known baseline (matches dynamic graph visualization expectations). Existing tests `describe_reactive_fires_on_new_node` / `describe_reactive_fires_on_remove` / `describe_reactive_stops_on_drop` / `describe_reactive_accumulates_nodes` updated to expect initial+post snapshots; new test `describe_reactive_pushes_initial_snapshot` covers the contract directly.

**A1 — `Graph::remove`'s mount-branch maps `MountError::Destroyed` → `RemoveError::Destroyed`** instead of collapsing every `MountError` variant into `NotFound`. Other variants (NotMounted, etc.) remain `NotFound`.

**A2 — `signal_pause` / `signal_resume` comment fix:** "Ignore UnknownNode from concurrent removal between snapshot and call" replaces the misleading "already paused with this lock" reference (multi-pauser pause is idempotent on duplicate locks; the only failure mode is a teardown race).

**A3 — `#[must_use]` on `Graph::remove`, `TopologySubscription`, `ReactiveDescribeHandle`.** All three are RAII-bearing; without `must_use`, `let _ = g.describe_reactive(sink);` silently drops the handle and the sink never fires.

**A4 — Send + Sync compile-time asserts** for `ReactiveDescribeHandle` and `GraphObserveAllReactive` (mirror the existing assert on `TopologySubscription`).

**A5 — `Core::subscribe_topology` rustdoc updated** to document: (a) sinks fire OUTSIDE the state lock and MAY re-enter Core (lock-released discipline); (b) `NodeRegistered(id)` fires before `Graph::add()` inserts the name → namespace lookups from inside the sink see `None`; (c) `NodeTornDown(id)` fires for the root AND every cascaded meta + downstream consumer; (d) idempotent `set_deps` (deps unchanged as a set) does not fire `DepsChanged`.

**A6 — `edges_inner` single-pass refactor.** Pre-fix: built `names_iter` and `names_map` via two separate walks of `inner.names`. Now one walk produces both via `Vec<(qualified_name, NodeId)>` and `IndexMap<NodeId, qualified_name>` derived from the same pairs. Edge case docs updated to mention sibling-graph `_anon_<id>` resolution.

**A7 — 5 clippy `doc_markdown` nits** resolved: `observe_all`, `NodeIds`, `CoreState`, `set_deps`, `namespace_sinks` properly backticked across `graph.rs`, `observe.rs`, `describe.rs`, `topology.rs`.

### Group 3 deferrals (added to porting-deferred.md as D1–D6)

- **D1** — `edges()` cross-graph deps surface as `_anon_<id>` for sibling-graph nodes (sibling-resolution gap pattern).
- **D2** — `subscribed: HashSet<NodeId>` in `GraphObserveAllReactive` grows unboundedly across torn-down nodes.
- **D3** — `signal()`'s `SignalKind` enum is narrower than canonical's open `Messages` (no `[[ERROR]]` / `[[COMPLETE]]` broadcast).
- **D4** — `signal(Pause/Resume)` does not filter meta companions; R3.7.2 explicit only for INVALIDATE — spec ambiguity for non-invalidate.
- **D5** — `describe_reactive` does not fire on `set_deps` topology changes (documented divergence; consumer composes with `core.subscribe_topology()`).
- **D6** — `packages/parity-tests/scenarios/graph/` not yet widened for the M2 surface (Slice F deliberate cut; tracked as M2-close-gate residual).

### Test count post-/qa

268 tests green workspace-wide (was 260 pre-/qa, 227 post-Slice-E+ /qa). 8 net-new regression tests: +1 in `gap_fills.rs` (`remove_preserves_namespace_during_teardown_cascade`); +2 in `topology.rs` (`teardown_cascade_fires_node_torn_down_for_each_node`, `teardown_cascade_fires_for_meta_companions`); +5 in `reactive.rs` (`describe_reactive_pushes_initial_snapshot`, `describe_reactive_fires_on_mount_new`, `describe_reactive_fires_on_unmount`, `observe_all_reactive_handles_late_mount`, `observe_all_reactive_subscribe_twice_panics`). 4 existing reactive tests updated to expect initial+post snapshots. `cargo clippy --all-targets -D warnings` clean. `cargo fmt --check` clean. `#![forbid(unsafe_code)]` preserved on `graphrefly-core` and `graphrefly-graph`.

---

## Slice E+ /qa — adversarial review fixes — landed 2026-05-05

QA pass on Slice E+ surfaced ~16 active findings via two adversarial subagents (Blind Hunter, Edge Case Hunter). Applied 11 patches; deferred 8 items (added entries to `porting-deferred.md`).

### What landed

**Auto-applicable (6):**
- **A1** — `assert!(matches!(...))` wraps for test variant checks. Pre-fix several mount/namespace tests had bare `matches!()` at expression position — pass regardless of error variant.
- **A2** — `NameError::Destroyed` variant added; `Graph::add` (and the `state` / `derived` / `dynamic` sugar) now returns `Err(NameError::Destroyed)` on a destroyed graph instead of `assert!`-panicking. Symmetric with `MountError::Destroyed`. `From<NameError> for MountError` updated. Test `add_after_destroy_panics` rewritten to `add_after_destroy_returns_destroyed_error`.
- **A3** — `#[must_use]` on `GraphObserveOne` (was already on `GraphObserveAll`).
- **A4** — `Graph::signal_invalidate` short-circuits on destroyed graph.
- **A5** — `status_of` rustdoc note about the future `terminating` substate when `terminal && dirty` for reactive describe.
- **A6** — regression test verifying `parent.destroy()` flips `destroyed` on user-held child clones via the shared `Arc<Mutex<GraphInner>>`.

**Patches (5):**
- **B1 — Mount/mount_new TOCTOU.** `mount` and `mount_new` now hold `parent.inner.lock()` across validation + child construction + insert. Two concurrent `mount_new("foo")` calls can no longer both pass validation and overwrite each other's `IndexMap::insert`. Lock ordering is parent → child (mount path locks the child's inner under the parent's inner; no Graph code acquires a parent lock from inside a child lock).
- **B2 — Graph-layer meta filter (R3.7.2).** Pre-fix: `Graph::signal_invalidate`'s doc-comment claimed Core's invalidate cascade skipped meta children — verified false (Core's `invalidate_inner` walks every consumer). Now `Graph::signal_invalidate` builds a `HashSet<NodeId>` of all meta companions of any named node (via the new `Core::meta_companions_of` accessor) and excludes them from the broadcast. Direct `Core::invalidate(meta_id)` still wipes a meta cache — the filter applies only to the graph-level broadcast. Regression test `signal_invalidate_skips_meta_companions` confirms.
- **B3 — `destroy()` reorder per R3.7.3.** Pre-fix order was: mark destroyed → clear `names`/`names_inverse`/`children` → drop lock → recurse into children → `core.teardown(id)` for own ids. Sinks observing TEARDOWN saw an empty namespace via `Graph::name_of`. Now: snapshot ids + children → drop lock → recurse into children → `core.teardown(id)` per own id (sinks still resolve names) → THEN clear registries. Regression test `destroy_preserves_namespace_during_teardown_cascade` subscribes a sink, observes TEARDOWN, looks up `name_of(node)` mid-cascade — passes Some, then None after destroy returns.
- **B4 — `describe()` `meta` field added per Appendix B.** `NodeDescribe` gains `meta: Option<serde_json::Value>` with `#[serde(skip_serializing_if = "Option::is_none")]` so current outputs (always None) don't carry the key. The binding-side wrapper that resolves `HandleId → T` will populate `meta` from the binding-side metadata-storage primitive (post-Core slice). `serde_json` moved from optional dep to non-optional; `json` feature flag retired (the JSON shape is core to describe). Two regression tests: `describe_meta_field_omitted_when_none`, `describe_meta_field_round_trips_via_json`.
- **B8 — `Graph::set_deps` rustdoc `# Hazards` block.** Cross-references the D1 entry in `porting-deferred.md` (Dynamic `tracked` corruption from re-entrant `set_deps`). Wider exposure than pre-Slice-E+ (Core was internal-only); doc warning is the v1 mitigation until the structural fix lands.

### Deferred (8 entries added to `porting-deferred.md`)

- **B5** — `try_resolve` no `..::sibling::node` (R3.5.2 cross-subgraph paths).
- **B6** — `audit_of` racy snapshot under concurrent mutation.
- **B7** — `try_resolve` silent None on malformed paths.
- **F6** — Unmount-vs-destroy not distinguishable on the reactive stream.
- **F12** — `GraphObserveOne::up()` decomposed into `pause`/`resume`/`invalidate` methods (R3.6.2 divergence).
- **F13** — `GraphObserveAll::subscribe` accumulates Subscriptions into one shared vec (no per-call disposal).
- **`_anon_<id>` deps could collide** with a user-named `_anon_<id>` node — format ambiguity.
- **`ancestors()` cycle insurance** — current API can't construct a cycle; future regressions could.

Plus a port-coverage gap audit catalogued 6 unimplemented canonical §3 surfaces (`graph.signal(messages)` general, name-based sugar wrappers `set`/`get`/`invalidate`/`complete`/`error`, `tagFactory`, `resourceProfile`, `setVersioning`, `remove(name)` for individual nodes, `edges(opts)` direct accessor) — non-blocking M2 close.

### Rejected (false positives)

- BlindHunter "GraphObserveAll deadlock from sink callback" — retracted by author (Rust borrowck).
- BlindHunter `mount` allowing same child different names — `AlreadyMounted` covers.
- BlindHunter `name()` perf clones — declined as nit.
- BlindHunter `Resolved` status unreachable — already commented as reactive-describe placeholder.

### Test count post-/qa

227 tests green workspace-wide (was 220 pre-/qa). 7 new regression tests added (1 in `tests/inspection.rs`, 4 in `tests/mount.rs`, 2 in `tests/describe.rs`); 1 test rewritten (`add_after_destroy_*`). `cargo clippy --all-targets -D warnings` clean. `cargo fmt --check` clean. `#![forbid(unsafe_code)]` preserved.

## Slice E+ — M2 read-side introspection + composition — landed 2026-05-05

Layered on top of Slice D (M2 starter). Lands the canonical-spec read-side surface in the Rust port: Core-level node enumeration, Graph namespace + canonical §3.9 sugar API, mount/unmount composition, `describe()` JSON form, `observe()` default sink-style.

### What landed

- **Core inspection helpers** ([crates/graphrefly-core/src/node.rs](../crates/graphrefly-core/src/node.rs)): `Core::node_ids()` / `node_count()` / `kind_of()` / `deps_of()` / `is_terminal()` / `is_dirty()` — five non-panicking accessors backing the graph-layer introspection. All return `Option`/empty for unknown ids; pure read paths with single state-lock acquisition. `Core::same_dispatcher(&Core)` for the cross-Core mount-rejection check via `Arc::ptr_eq`. `DepTerminal` enum renamed to `TerminalKind` and made `pub` (it carries the public `is_terminal()` return shape).

- **Graph namespace** (canonical §3.5) at [crates/graphrefly-graph/src/graph.rs](../crates/graphrefly-graph/src/graph.rs): `IndexMap<String, NodeId>` + reverse-map for insertion-ordered + reverse-lookup-stable namespace. New `Graph::add(node_id, name)`, `Graph::node(path)` (panics on missing per Q1 lock), `Graph::try_resolve(path)` (non-panicking), `Graph::name_of(id)`, `Graph::node_count()` (proper impl, replaces Slice D's `unimplemented!()`), `Graph::node_names()`, `Graph::child_names()`. Path resolution recurses through `::`-delimited paths into mounted children.

- **Canonical §3.9 sugar API** (hard-break per Q2 lock): `state(name, initial)` / `derived(name, deps, fn_id, equals)` / `dynamic(name, deps, fn_id, equals)` replace Slice D's nameless variants. `Graph::new(name, binding)` now takes a graph name. Pre-1.0 — no compat shims.

- **Mount/unmount** (canonical §3.4) at [crates/graphrefly-graph/src/mount.rs](../crates/graphrefly-graph/src/mount.rs):
  - `Graph::mount(name, child)` — embed an existing graph (rejects with `MountError::CoreMismatch` if the child has a different `Core`; cross-Core multi-binding mount is post-M6 per session-doc Open Question 1).
  - `Graph::mount_new(name)` — create empty subgraph sharing this graph's `Core`.
  - `Graph::mount_with(name, builder)` — builder pattern.
  - `Graph::unmount(name)` — detach + destroy + return `RemoveAudit { node_count, mount_count }`.
  - `Graph::ancestors(include_self)` — parent chain via `Weak<GraphInner>` (no strong cycle).

- **Graph-level lifecycle** (canonical §3.7): `Graph::destroy()` recursive TEARDOWN cascade across own nodes + mounts (idempotent; subsequent registration calls panic). `Graph::signal_invalidate()` invalidate broadcast across own nodes + recursive into mounts. Meta filtering (R3.7.2) is implicit — Core's invalidate cascade already skips meta-companion children of registered parents.

- **`Graph::describe()`** (canonical §3.6 + Appendix B JSON schema) at [crates/graphrefly-graph/src/describe.rs](../crates/graphrefly-graph/src/describe.rs): JSON form only. Returns `GraphDescribeOutput { name, nodes, edges, subgraphs }` with `serde::Serialize` derived; consumer round-trips to JSON via `serde_json::to_string`. Status mapping per canonical §3.6.1 precedence (`errored` > `completed` > `dirty` > `settled` if fired else `pending` > `sentinel`). Unnamed Core-only nodes referenced as deps surface as `_anon_<NodeId>` (per Q3 lock — they are NOT enumerated as top-level nodes). Mounted children listed by name in `subgraphs[]`; their internal nodes are NOT inlined (recurse via `child.describe()`). Edges in insertion order (per Q4 lock — no sorting).

- **`Graph::observe()` / `Graph::observe_all()`** (canonical §3.6.2 default mode only) at [crates/graphrefly-graph/src/observe.rs](../crates/graphrefly-graph/src/observe.rs):
  - `GraphObserveOne::subscribe(sink) -> Subscription` (taps a single named node — same handshake semantics as `Core::subscribe`).
  - `GraphObserveOne::pause(lock)` / `resume(lock)` / `invalidate()` — the `up(...)` API per canonical §3.6.2 specialized to tier-2 / tier-4 messages.
  - `GraphObserveAll::subscribe(sink: Fn(&str, &[Message]))` — multicasts across every named node at observe-time. Late-added nodes are NOT auto-subscribed (snapshot-at-subscribe-time semantics — reactive variants in a later slice will handle dynamic membership).
  - `GraphObserveAll` holds the `Subscription`s; dropping the handle unsubscribes all sinks.

- **Module split** in `graphrefly-graph` from a single `lib.rs` to `lib.rs` (re-exports) + `graph.rs` + `mount.rs` + `describe.rs` + `observe.rs`. Roughly 1100 LOC of impl + 600 LOC of tests across 4 integration test files (`namespace.rs`, `mount.rs`, `describe.rs`, `observe.rs`) plus a new `tests/inspection.rs` in `graphrefly-core` for the helpers.

### Test count

220 tests green workspace-wide (was 174 post-Slice-D). New tests: 8 inspection (Core helpers) + 13 namespace + 16 mount + 12 describe + 7 observe = 56 new (-10 from Slice D's old `lib.rs` unit tests that moved to integration). `cargo clippy --all-targets -D warnings` clean. `cargo fmt --check` clean. `#![forbid(unsafe_code)]` preserved across all crates.

### What did NOT land in Slice E+

- **Reactive describe / observe** — `describe({ reactive: true })` returning `Node<GraphDescribeOutput>` and `observe({ reactive: true })` / `observe({ changeset: true })` returning `Node<...>`. Need a Core-level topology-change notification primitive.
- **Async-iterable observe modes** — `structured` / `timeline` / `causal` / `derived` opts.
- **Non-JSON describe formats** — pretty / mermaid / d2 / stage-log.
- **`describe({ explain })` / `describe({ reachable })`** — causal chain + reachability walks.
- **Snapshot / restore + content addressing** — V1 lazy CID / V2 schema validation / V3 caps + cross-graph refs (canonical §3.8 + Phase 6). Brings in `serde_ipld_dagcbor`, `cid`, `multihash`, `blake3`.
- **DS-14 `mutate()` factory** — RAII guard + batch + audit append substrate. Waits for Phase 14 Core-substrate work.
- **Cross-Core (multi-binding) mount** — post-M6 per session-doc Open Question 1.
- **Meta annotations + versioning fields** in describe output — needs a metadata-storage primitive on Core. Documented divergence: `value: Option<HandleId>` (raw handle) instead of `value: T` (canonical TS); binding-side renders to user values.
- **napi-rs binding parity for the new Graph methods** — bindings still expose `Core` only. Wire-up follows when graph-layer is consumer-driven.

### Carried forward to porting-deferred.md

- Late-added node not auto-subscribed in `observe_all()` (v1 snapshot-at-subscribe-time). Lifts with reactive observe.
- Mid-wave `signal_invalidate()` doesn't snapshot the namespace under a single lock — concurrent mutations during the broadcast are observable. Acceptable v1 (signal_invalidate is typically setup/teardown-time).
- `value` field surfaces raw `HandleId` rather than user-rendered `T` — documented spec divergence, lifted at the binding layer (`describe_with_values`).
- Anonymous Core nodes referenced as deps surface in describe edges as `_anon_<id>` strings; they are not first-class nodes in the namespace. Acceptable v1.

## Slice A close — fn + custom_equals re-entrance lift + R1.3.5.a tier-split — landed 2026-05-05

The largest deliberately-deferred M1-close concern (per the Slice A-bigger /qa scoping note: "MutexGuard-ownership refactor — would have lifted both `BindingBoundary::invoke_fn` and `BindingBoundary::custom_equals` lock-released … 400–500 LOC of churn"). Closed as part of the M1-close batch. Bundled with the R1.3.5.a handshake tier-split per the same lift point.

### What landed

- **Lock-released `BindingBoundary::invoke_fn`.** `Core::fire_fn` rewritten as three phases: (1) snapshot inputs under the state lock, (2) drop the lock and call `binding.invoke_fn(...)`, (3) reacquire the lock to apply the result. Defensive terminal re-check in phase 3 catches the case where a sibling cascade terminated the node mid-phase-2. User fns may now re-enter `Core::emit` / `Core::pause` / `Core::resume` / `Core::invalidate` / `Core::complete` / `Core::error` / `Core::teardown` from inside `invoke_fn` and run a nested wave; the existing `s.in_tick` re-entrance gate composes transparently.
- **Lock-released `BindingBoundary::custom_equals`.** `Core::commit_emission` rewritten with the same three-phase shape: snapshot equals_mode + cache, drop lock for the equals check, reacquire to apply DATA/RESOLVED. The new `handles_equal_lock_released` helper is the only call site for the binding callback. Custom equals oracles may now read from Core (e.g. `Core::cache_of`) mid-evaluation without deadlocking.
- **R1.3.5.a per-tier handshake delivery.** `Core::subscribe` builds the handshake as per-tier slices (`[Start]`, `[Data(cache)]`, `[Complete]`/`[Error(h)]`, `[Teardown]`) and fires each slice as a separate sink call. Cached state subscribes now produce 2 sink calls (was 1 bundled); torn-down state produces 4 (was 1).
- **Sink-snapshot-on-first-touch race fix.** `CoreState.pending_notify` changed from `IndexMap<NodeId, Vec<Message>>` to `IndexMap<NodeId, PendingPerNode { sinks, messages }>`. The first `queue_notify` for a node within a wave snapshots the current subscriber list; subsequent pushes append messages but reuse the snapshot. This makes late subscribers (installed mid-wave between fire_fn iterations now that the state lock drops per-iteration) invisible to messages already queued before they subscribed. Without the snapshot fix, the lock-released drain would deliver duplicate Data: handshake `[Start, Data(post)]` plus wave `[Dirty, Data(post)]` to the same late sink. Closed for the canonical race scenario by the snapshot discipline.
- **`run_wave` and `drain_and_flush` are now `&self`-only.** Both manage their own locking; binding callbacks happen between lock acquisitions. `BatchGuard::drop` updated to match. `activate_derived` simplified to a pure dep-walk + `pending_fires` queueing — `run_wave` (called from `Core::subscribe`) handles the in_tick management and drain.
- **All wave-firing public methods updated** (`Core::emit`, `Core::error`, `Core::complete`, `Core::teardown`, `Core::invalidate`, `Core::set_deps`'s push-on-add path, `Core::subscribe`'s activation path) to use the new `&self`-only `run_wave`. No semantic change for existing callers.

### Test count

174 tests green: 13 unit + 15 batch + 10 proptest + 5 lock_discipline + 4 cascade_depth + 4 drop_refcount + **13 lock_released (NEW Slice A close)** + 13 dispatcher + 16 setdeps + 12 pause + 11 invalidate + 12 terminal + 7 teardown + 10 resubscribable + 8 meta_teardown + 7 subscription + **10 graph (NEW Slice D)** + 4 scaffold (graph + operators + storage + structures). `cargo clippy --all-targets -D warnings` clean. `cargo fmt --check` clean. `#![forbid(unsafe_code)]` preserved.

### What did NOT land in Slice A close

- **Subscribe-time handshake re-entrance lift.** The handshake fires per-tier under the state lock (per the race-fix discipline carried from Slice A-bigger). A handshake-time sink callback that re-enters Core still hits the `IN_HANDSHAKE_FIRE` thread-local diagnostic and panics. Lifting requires per-sink "handshake pending" staging-buffer machinery (queue concurrent wave messages destined for this sink until handshake completes, then drain). Tracked in `porting-deferred.md` as M1-close+ work; not blocking M2.
- **Pause-buffer overflow → ERROR synthesis (Lock 6.A).** Kept deferred per the slice-scope discussion. Separate Lock 6.A divergence — the `started_at_ns` field is reserved for future use.

### Carried forward to porting-deferred.md

- Subscribe-time handshake re-entrance from a callback still panics via the `IN_HANDSHAKE_FIRE` diagnostic. Acceptable v1 trade-off for the subscribe-vs-emit race fix; lifts with per-sink staging-buffer work.

### Closed in porting-deferred.md

- ✅ **Fn re-entrance via `BindingBoundary::invoke_fn` and `custom_equals` still forbidden** — both fire lock-released after Slice A close.
- ✅ **Subscribe handshake delivered as single sink call (R1.3.5.a divergence)** — handshake now fires per-tier as separate sink calls.

## Slice A close /qa — adversarial review fixes — landed 2026-05-05

QA pass on Slice A close + B + C + D landed via two adversarial subagents (Blind Hunter, Edge Case Hunter). ~40 findings surfaced; this layer applied 9 patches (P1–P9) and 5 deferred entries (D1–D5) per user direction Q1=(a) defer, Q2=(b) wave-owner mutex.

### What landed

- **Q2 + P1 — wave-owner re-entrant mutex + RAII run_wave.** `Core` gains a `wave_owner: Arc<parking_lot::ReentrantMutex<()>>`. `Core::begin_batch` acquires it via `lock_arc()` (returning an `ArcReentrantMutexGuard` stored in `BatchGuard`) BEFORE the state lock acquisition for `in_tick`. `Core::run_wave` now delegates to `begin_batch` for the entire wave lifecycle — same panic-discard / drain semantics. **Cross-thread emits BLOCK** at `wave_owner.lock_arc()` until the in-flight wave's drain + flush + sink-fire completes, preserving the user-facing "emit returning means subscribers have observed" contract that the Slice A close lock-released drain would otherwise silently break. Same-thread re-entry (a fn calling back into `Core::emit` mid-fire) passes through transparently. Closes Blind Hunter #4 (panic-in-op poisons in_tick) and #5 (cross-thread emit returns before delivery) simultaneously — the RAII via `BatchGuard::drop` handles the panic-cleanup that ad-hoc `run_wave` lacked.
- **P2 — `set_deps` re-reads cache inside the run_wave closure.** Pre-fix: `additions_with_cache` snapshot taken in the validation phase; lock dropped; `run_wave` re-acquires. Concurrent invalidate could release a snapshotted cache handle, leaving a dangling HandleId that `deliver_data_to_consumer` would write into the consumer's `dep_handles[idx]`. Now: snapshot only the LIST of added deps; re-read each dep's cache INSIDE the closure under the wave-owner-held lock. Defensive `n` re-validation also added (concurrent termination).
- **P3 — `commit_emission` phase 3 re-reads current cache.** Pre-fix: phase 1 snapshotted `old_handle = rec.cache`; phase 3 released `old_handle` without checking that the cache actually still held it. Same-thread nested `commit_emission` (e.g. from a `custom_equals` oracle that re-entered `Core::emit` on the same node during phase 2's lock-released equals check) could have advanced the cache to `mid_handle`, releasing `old_handle`. Phase 3's release of `old_handle` would underflow the binding refcount AND leak `mid_handle`. Now: phase 3 re-reads `current_cache` and releases that. The `is_data` decision is still computed against `(old_handle, new_handle)` — staleness manifests as a benign redundant `Data` emit in the rare race; documented in D5.
- **P4 — `Graph::node_count` panics with `unimplemented!()`.** Replaced the misleading `return 0` placeholder with an explicit panic; updated the test to `#[should_panic]`.
- **P5 — `BenchCore::alloc_lock_id` returns `Result<u32, NapiError>`.** Pre-fix: `unwrap_or(u32::MAX)` silently clamped after ~2³² allocs; every subsequent call returned the same `u32::MAX`, aliasing every "fresh" lock id and causing cross-controller pause leak in long-lived BenchCores. Now surfaces an error.
- **P6 — `pick_next_fire` visibility reverted.** Was accidentally `pub(crate)` during the wave-engine refactor; same-impl-block call site doesn't need it.
- **P7 — Resubscribable reset clears `rec.tracked` for Dynamic nodes.** Parity with `set_deps`'s Dynamic-rewire path. Latent foot-gun until the first-fire fallback (which currently masks it) ever changes.
- **P8 + P9 — CI workflow tightening.** TLA fetch reads `TLA_REF` env var (currently `main` for pre-1.0 spec-tracking; pin to a release SHA post-1.0). All TLC steps use `shell: bash -euo pipefail {0}` so a failed `java` invocation propagates through `tee` instead of being masked.

### Test count post-/qa

174 tests green: 13 unit + 15 batch + 10 proptest + 5 lock_discipline + 4 cascade_depth + 4 drop_refcount + 13 lock_released + 13 dispatcher + 16 setdeps + 12 pause + 11 invalidate + 12 terminal + 7 teardown + 10 resubscribable + 8 meta_teardown + 7 subscription + **11 graph** (added P4 `#[should_panic]` test) + 3 scaffold (operators + storage + structures). `cargo clippy --all-targets -D warnings` clean. `cargo fmt --check` clean. `#![forbid(unsafe_code)]` preserved across all crates.

### Carried forward to porting-deferred.md

5 new deferred entries (D1–D5):

- **D1.** Set_deps from inside firing node's fn corrupts Dynamic `tracked` indices.
- **D2.** Late subscriber + multi-emit-per-wave snapshot gap (user direction: Q1=(a) defer, will rethink alongside DS-14).
- **D3.** Cross-thread emit blocks until in-flight wave completes (documented v1 contract; per-subgraph parallelism is the planned parallel-throughput axis post-M2).
- **D4.** Per-tier handshake panic on tier-N leaves sink registered (lifts with the staging-buffer machinery for handshake re-entrance).
- **D5.** `commit_emission` cache-race documentation: `is_data` staleness in the rare race window is a benign redundant `Data` emit for Identity, undefined for Custom oracles racing the same-node commit.
- **CI hardening (TLA SHA pin)** — pin `TLA_REF` to a release SHA post-1.0 graphrefly-ts release.

### Rejected (non-issues)

~25 findings rejected as documentation nits without correctness impact, perf concerns without bench evidence (deferred per Pass 5 directive), false positives, or pre-existing patterns not exacerbated by this slice. See the Q1/Q2 user-decision summary in the closing section above.

## Slice B — napi-rs binding parity for new Core methods — landed 2026-05-05

Closes the M1-close binding-side parity gap. Each new Core method that landed in Slice A+B and Slice A close is now callable from JavaScript via the `BenchCore` napi class.

### What landed

New `BenchCore` instance methods in `crates/graphrefly-bindings-js/src/core_bindings.rs`:

- `alloc_lock_id() -> u32` — returns a fresh `LockId` (collision-free across calls).
- `set_pause_buffer_cap(cap: Option<u32>)` — Core-global pause replay buffer cap.
- `pause(node_id, lock_id) -> Result<()>` / `resume(node_id, lock_id) -> Result<Option<ResumeReportJs>>` — multi-pauser pause/resume; `ResumeReportJs` is a napi-object wrapper for `ResumeReport` (`replayed`, `dropped`).
- `is_paused(node_id) -> bool` / `pause_lock_count(node_id) -> u32` / `holds_pause_lock(node_id, lock_id) -> bool` — pause introspection.
- `invalidate(node_id)` — cache clear + cascade.
- `complete(node_id)` — terminal COMPLETE.
- `error_int(node_id, err_code: i32)` — terminal ERROR with primitive i32 payload (bench-binding limitation: real bindings need arbitrary error payloads via TSFN).
- `teardown(node_id)` — TEARDOWN with auto-prepended COMPLETE.
- `add_meta_companion(parent: u32, companion: u32)` — meta TEARDOWN ordering.
- `set_resubscribable(node_id: u32, resubscribable: bool)` — resubscribable terminal lifecycle config.
- `batch_emit_ints(node_id: u32, values: Vec<i32>)` — RAII batch demo (closures don't cross FFI cleanly; bench surface uses a value-list shape).
- `set_deps(node_id: u32, new_dep_ids: Vec<u32>) -> Result<()>` — atomic dep mutation; `SetDepsError` stringifies into the `napi::Error` body.
- `has_fired_once(node_id: u32) -> bool` — read-side activation introspection.

### What did NOT land in Slice B

- **Global cross-Worker variants** of the new methods. The existing `global_*` functions cover the throughput-bench surface (state register, derived register, subscribe noop, emit, cache, rust_emit_loop). Lifecycle methods (pause/resume/invalidate/complete/teardown) don't map directly to the cross-Worker bench scenarios; they can be added later if needed.
- **TSFN-based JS-callback fns / TSFN custom-equals.** The bench binding still uses pre-baked Rust closures (`Identity`, `AddOne`). Real JS callbacks via TSFN are post-bench-study work.
- **JS-side smoke tests.** The cdylib build verifies the binding compiles; behavior tests live in graphrefly-ts's bench harness.

## Slice C — CI scaffold + TLC integration — landed 2026-05-05

Closes two M1-entry-checklist infrastructure items in one workflow file.

### What landed

`.github/workflows/ci.yml` with five jobs:

1. **`rust`** — `cargo fmt --check` / `cargo check` / `cargo clippy --all-targets -D warnings` / `cargo test --all-targets` / `cargo doc --no-deps -D warnings` against the pinned Rust 1.95 toolchain. Targets `default-members` (binding crates excluded; they have their own jobs).
2. **`deny`** — supply-chain audit via `EmbarkStudios/cargo-deny-action@v2`. License allowlist + RustSec advisory database + version-unification warnings per the existing workspace `deny.toml`.
3. **`bindings-js`** — napi-rs binding crate cdylib build (Rust 1.95 + Node 24). Clippy is informational (continue-on-error) because napi-rs macro-generated FFI items trip pedantic-tier lints that don't apply to FFI surfaces.
4. **`tlc`** — TLA+ model checks. Sets up Java 21 (Temurin), downloads `tla2tools.jar` v1.8.0, fetches the canonical MC harnesses (`handle_protocol_MC`, `wave_protocol_rewire_MC`) from `graphrefly-ts/main` via raw GitHub URLs, runs each MC with `-workers auto`, and asserts `Model checking completed` + no `is violated` / `Error:` output.

### What did NOT land in Slice C

- **CI workflow for the python and wasm bindings** — pyo3 needs maturin, wasm-bindgen needs wasm-pack. They're scaffold-only crates today; CI lands when they have content.
- **Cargo bench in CI** — criterion benches run locally; no CI step yet (would need a baseline comparison framework).

## Slice D — M2 starter — `graphrefly-graph` Graph container — landed 2026-05-05

First slice of M2. Stands up the `Graph` container as a thin sugar layer over `Core`, unblocking subsequent M2 work (mount/unmount, describe, observe, snapshot).

### What landed

`crates/graphrefly-graph/src/lib.rs` (~280 lines incl. tests):

- **`Graph`** struct holding an `Arc<Core>`; `Clone` is a cheap refcount bump.
- **Sugar constructors** (canonical spec §3.9): `state(initial: Option<HandleId>)`, `derived(deps, fn_id, equals)`, `dynamic(deps, fn_id, equals)`.
- **Lifecycle pass-throughs**: `subscribe`, `emit`, `cache_of`, `has_fired_once`, `complete`, `error`, `teardown`, `invalidate`, `pause`, `resume`, `alloc_lock_id`, `set_deps`, `set_resubscribable`, `add_meta_companion`, `batch`.
- **`Graph::core() -> &Core`** escape hatch for Core-only methods not yet surfaced.
- **10 unit tests** covering: empty graph construction, state register (with both `Some(h)` and `None` initial), emit propagation through derived, pause/resume round-trip, complete terminating subsequent emits, set_deps validating cycle/state-node rejection, alloc_lock_id distinctness, `Graph::clone` shares state, batch coalescing emits.

### What did NOT land in Slice D (M2 follow-ups)

- **`mount` / `unmount` / subgraph composition** (canonical spec §3.4 / §3.7).
- **`describe()`** topology export — pretty / mermaid / d2 / json (§3.6, Appendix B JSON schema).
- **`observe()`** message tap.
- **Content-addressed snapshots** — V1 lazy CID / V2 schema validation / V3 caps + cross-graph refs (§3.8 + roadmap Phase 6).
- **Namespace** (§3.5) — node naming and lookup by name.
- **DS-14 `mutate()` factory** — RAII guard + batch + audit append substrate.
- **`Graph::node_count`** is currently a placeholder returning 0 (pending describe() implementation that surfaces a structured node-iteration inspector at the Core level).
- **Persistence integration** with `graphrefly-storage` (M4).

These are tracked at the M2 milestone level rather than as porting-deferred entries (they're explicit follow-ups, not surfaced concerns).

## Slice A-bigger /qa — adversarial review fixes — landed 2026-05-05

QA pass on the bundled C-1 / C-1.5 / C-2 / A-bigger slice. Two adversarial subagents (Blind Hunter, Edge Case Hunter) surfaced ~25 active findings; this layer applied the auto-approved fixes plus 5 user-decided items.

### What landed

- **Auto-applicable (9):** node.rs module-doc cleanup (stale "out of scope" entries removed); defensive `terminal.is_some()` guard in `commit_emission`; `# Panics` blocks on `register_derived` / `register_dynamic`; richer panic message for `commit_emission`'s `NO_HANDLE` assert (includes `node_id`); `set_deps` style consistency (single early-out validation); dead `a_handle` field removed from Diamond test fixture; `inv11_pause_multi_pauser_only_final_release_replays` strategy `1usize..=4` → `2usize..=4` to actually exercise multi-pauser; `CoreState::clear_wave_state` helper centralizes wave cleanup across 4 sites; `add_meta_companion` doc clarifies setup-time vs mid-wave registration semantics.
- **`BatchGuard: !Send`** via `PhantomData<*const ()>` marker + `compile_fail` doctest. Sending the guard across threads and dropping it on a different thread would clear `in_tick` from a thread that didn't set it. Verified at the type level.
- **`impl Drop for CoreState`** releases every retained handle on the last `Core` clone drop — `cache`, `terminal` Error, `dep_terminals` Error, pause-buffer payloads, `pending_notify` queue retains, `deferred_handle_releases`, `wave_cache_snapshots`. Adds 4 `tests/drop_refcount.rs` regressions verifying `live_handles() == 0` after drop. **Also fixed a pre-existing leak** in `Core::error` — the caller's intern share for `error_handle` was never released after `terminate_node` took its own slot retain, leaking one handle ref per `error()` call. The QA-surfaced regression test caught it.
- **Concurrent-subscribe test strengthened** — was vacuous (only `events.first() == Start`); now asserts (1) Start is first, (2) observed `Data` values are monotonically increasing (no out-of-order delivery), (3) sandwich check that emit thread continued past subscribe (race window was real).
- **Re-entrance diagnostic** via `IN_HANDSHAKE_FIRE` thread-local + `Core::lock_state` helper that all public Core methods route through. A handshake-time sink callback that calls back into Core now panics with a clear message instead of silently deadlocking on the held state lock. Regression test `handshake_sink_reentry_panics_with_diagnostic_not_deadlocks` confirms.
- **State-cache snapshot+restore on batch panic** — `CoreState::wave_cache_snapshots` records pre-wave state-node caches; `BatchGuard::drop`'s panic-discard path restores them. `cache_of(s)` now returns the pre-panic value, not the partially-committed mid-closure value. Atomicity guarantee covers BOTH sink-observability AND cache state. Adds 3 `tests/batch.rs` regressions.

### What did NOT land

- **`MutexGuard`-ownership refactor (item E option 2)** — would have lifted both `BindingBoundary::invoke_fn` and `BindingBoundary::custom_equals` lock-released. The refactor requires changing `run_wave` / `drain_and_flush` / `fire_fn` signatures to `&Core` (no `&mut CoreState`), restructuring lock acquisition per-iteration in the drain loop, and updating all 5 `run_wave` callers + `activate_derived` + `BatchGuard::drop`. Estimated 400–500 LOC of churn with attendant test updates. Scoped out of this `/qa` cycle to keep the touch surface manageable; tracked as a follow-up. The `IN_HANDSHAKE_FIRE` diagnostic from item A guards the most common fail-mode (handshake re-entrance). Fn re-entrance via `invoke_fn` and `custom_equals` re-entrance during `commit_emission` remain forbidden in v1.

### Test count

154 tests green: 13 unit + 15 batch (Slice C-1.5 + 3 new panic-restore) + 10 proptest + 5 lock_discipline (Slice A-bigger + 1 handshake-reentry diagnostic) + 4 cascade_depth + 4 drop_refcount (NEW) + 13 dispatcher + 16 setdeps + 12 pause + 11 invalidate + 12 terminal + 7 teardown + 10 resubscribable + 8 meta_teardown + 7 subscription. `cargo clippy -p graphrefly-core --all-targets` clean. `cargo fmt --check` clean. `#![forbid(unsafe_code)]` preserved.

## Slice A-bigger — drop-then-fire lock discipline + iterative cascades — landed 2026-05-05

M1-close hardening. Lifts the v1 sink-fire-with-lock-held re-entrance limitation, fixes the subscribe-vs-emit race, and converts every recursive cascade walk in the dispatcher to iterative work-queues so deep linear chains don't overflow the OS thread stack.

### What landed

- **Drop-then-fire `flush_notifications`.** Wave-end sink delivery now snapshots `(Vec<Sink>, Vec<Message>)` jobs into `CoreState::deferred_flush_jobs` under the state lock, then `run_wave` (and every direct caller) drops the lock before invoking sinks. A sink that calls back into `Core::emit` / `pause` / `resume` / `invalidate` / `complete` / `error` / `teardown` re-acquires the state lock cleanly and runs a nested wave — the existing `s.in_tick` re-entrance gate composes with the new shape because the cleanup loop clears `in_tick` before the lock is dropped. Payload-handle releases owed for `pending_notify` are also deferred to post-lock-drop via `CoreState::deferred_handle_releases` so the binding's `release_handle` can't deadlock against a binding-side mutex held by Core.
- **Subscribe-vs-emit race fix.** `Core::subscribe` now registers the sink **first** under the lock, then builds and fires the handshake with the lock still held, then runs activation, then drops and fires deferred work. Concurrent emits on other threads block on the state lock until subscribe releases — by that time the new sink is in `subscribers` and observes the post-handshake wave in normal happens-after order. Trade-off: a handshake-time sink callback that calls back into Core deadlocks (the lock is held during handshake fire); flushed sinks (post-handshake) re-enter freely.
- **Cascade recursion → iterative.** `terminate_node`, `teardown_inner`, `invalidate_inner`, and `activate_derived` now use `Vec`-based work queues instead of recursing into the OS thread stack. `teardown_inner` preserves R1.3.9.d meta-TEARDOWN ordering via a two-action stack (`Visit` / `EmitTeardown`) so the original "metas first, then self, then children" order survives the rewrite. `activate_derived` uses a two-pass DFS: phase 1 discovers compute deps in dep-first order, phase 2 delivers caches forward. Linear chains of 5000 nodes now cascade COMPLETE / TEARDOWN / INVALIDATE / ERROR without overflow.

### Tests

- `tests/lock_discipline.rs` (4 tests) — sink re-entrance via `emit`, `pause`/`resume`, `complete`; concurrent subscribe-during-emit returns Start as the first message.
- `tests/cascade_depth.rs` (4 tests) — 5000-node chain stack-overflow stress for COMPLETE / TEARDOWN / INVALIDATE / ERROR cascades.

### Test count

139 tests green: 13 unit + 12 batch + 10 proptest + 4 lock_discipline + 4 cascade_depth + 13 dispatcher + 16 setdeps + 12 pause + 11 invalidate + 12 terminal + 7 teardown + 10 resubscribable + 8 meta_teardown + 7 subscription. `cargo clippy -p graphrefly-core --all-targets` clean. `cargo fmt --check` clean. `#![forbid(unsafe_code)]` preserved.

### What did NOT land in Slice A-bigger

- **Fn re-entrance via `BindingBoundary::invoke_fn`** — the `fire_fn` path still holds the state lock across the binding `invoke_fn` call. Lifting this requires refactoring `run_wave` / `drain_and_flush` to take ownership of the `MutexGuard` (so the lock can be dropped per-iteration around `invoke_fn`), which is a larger surgical change than the wave-end-flush case. Sink re-entrance — the more common pattern — is fully supported. Tracked in [`porting-deferred.md`](porting-deferred.md) as an M1-close-bigger follow-up; not blocking M1.
- **R1.3.5.a handshake tier-split** — the handshake fires as a single sink call (e.g. `[Start, Data(v)]` together) rather than per-tier. Documented divergence from TS routing through `downWithBatch`. Same lift point as fn re-entrance — the lock-discipline rework would also let the handshake split into separate tier-grouped calls.

### Carried forward to porting-deferred.md

- Fn re-entrance via `invoke_fn` (above).
- Handshake tier-split (above).
- Subscribe handshake lock-held during fire — re-entrance from a handshake-time sink callback deadlocks. Acceptable v1 trade-off for the race fix; lifts together with fn re-entrance.

### Closed in porting-deferred.md

- ✅ Re-entrance forbidden during sink fire — sinks now fire lock-released for wave-end flush.
- ✅ Inconsistent sink-fire lock discipline (`flush_notifications` vs `Core::resume`) — both now drop-then-fire.
- ✅ `subscribe` drop-then-reacquire race against concurrent emit — register-first-then-fire-under-lock fixes the window.
- ✅ Cascade recursion can stack-overflow on deep chains — iterative for `terminate_node`, `teardown_inner`, `invalidate_inner`, `activate_derived`.
- ✅ Activation walk uses recursion — same fix as cascade.

## Slice C-2 — `proptest` framework + protocol-layer invariants — landed 2026-05-05

### What landed

- **`tests/proptest_invariants.rs`** (~430 lines, 10 proptest cases). Mirrors `_invariants.ts`'s shared event vocabulary (`Emit` / `Batch` / `Complete`) + `apply_event` driver + `capture_trace` helper, then asserts 7 protocol-layer invariants:

| # | Name                  | Spec ref           |
|---|-----------------------|--------------------|
| 1 | no-data-without-dirty | R1.3.1.a           |
| 2 | resolved-per-wave     | R1.3.1.a (balance) |
| 3 | terminal-monotonicity | R1.3.4 (terminal)  |
| 4 | diamond-resolution    | R2.7.1 / R1.3.1.b  |
| 5 | equals-substitution   | R1.3.2             |
| 7 | start-handshake       | R1.2.3, R2.2.3     |
|11 | pause-multi-pauser    | R1.2.6, R2.6, §10.2|

- **R1.3.6.b alignment fix in `commit_emission`** — caught by inv #2 on a multi-emit batch with one equals-match emit + one value-changing emit. The pre-fix code skipped queueing `Message::Dirty` when `was_dirty=true`, leaving subscribers with 1 DIRTY + N settlements (R1.3.1.a balance violation). Now each `commit_emission` invocation queues its own DIRTY/settlement pair, so K state-emit calls in one wave produce K DIRTYs + K settlements.

### Out of scope (deferred to later slices/milestones)

- **#6 version-counter-per-emit** — handle-protocol Core has no versioning (production-only concern). Lands when versioning ports.
- **#8 throw-recovery-consistency**, **#9 subscribe-unsubscribe-reentry** — both need sink re-entrance, which Slice A-bigger lifts (drop-then-fire).
- **#10 multi-dep-initial-pair-clean** — covered structurally by #7's three sub-tests; redundant.
- All operator-layer invariants (#12+ in TS) — wait for M3 (`graphrefly-operators`).

### Test count

131 tests green: 13 unit + 12 batch (Slice C-1.5) + 10 proptest (Slice C-2) + 13 dispatcher + 16 setdeps + 12 pause + 11 invalidate + 12 terminal + 7 teardown + 10 resubscribable + 8 meta_teardown + 7 subscription. `cargo clippy -p graphrefly-core --all-targets` clean. `cargo fmt --check` clean. `#![forbid(unsafe_code)]` preserved.

### Carried forward to porting-deferred.md

- **R1.3.5.a handshake tier-split divergence** — Rust's `subscribe` synthesizes the handshake under the lock and fires it as a single sink call (e.g. `[Start, Data(v)]` as one batch). TS's R1.3.5.a routes `Start` through the same `downWithBatch` path as other messages, so it splits into separate tier calls. Acceptable v1 simplification (subscribe is out-of-wave; lock-held handshake fires once); revisit alongside Slice A-bigger lock-discipline work since handshake delivery is one of the lock-held sink-fire sites.

## Slice C-1.5 — user-facing batch + R1.3.1.b two-phase delivery + diamond bug fixes — landed 2026-05-05

Layered on top of Slice C-1 (batch.rs sibling-module split). Adds the user-facing batch API, fixes the latent R1.3.1.b cross-node delivery gap, and fixes three diamond-correctness bugs the strict tests surfaced.

### What landed

- **User-facing `Core::batch<F: FnOnce()>(&self, f: F)`** + RAII `Core::begin_batch() -> BatchGuard`. Closure form delegates to RAII; `BatchGuard` Drop drains the wave or (on panic) discards pending tier-3+ work without firing sinks during unwind. Refcount retains in `pending_notify` are released on the discard path.
- **Two-pass `flush_notifications`** — tier 1 (DIRTY) → tier 3+4 (DATA/RESOLVED + INVALIDATE) → tier 5 (COMPLETE/ERROR) → tier 6 (TEARDOWN). Cross-node ordering preserves R1.3.1.b "phase 1 propagates through the entire graph before phase 2 begins" at sink delivery time.
- **`queue_notify` refcount fix** — payload-bearing messages (`Data` / `Error`) queued for the wave-end flush now get a `retain_handle` on enqueue and `release_handle` after sink fire (or in the panic-discard path). Without this, a coalesced multi-emit (two `emit(s, h)` calls in one wave via `batch`) would underflow the prior cache handle's refcount when the second `commit_emission` released it before the wave drained.
- **`activate_derived` wave-state cleanup** — added the `dirty = false` / `involved_this_wave = false` reset loop after the activation drain. Pre-fix: derived nodes stayed `dirty = true` after subscribe-time activation; the next user emit then saw `was_dirty = true` and skipped queueing the wave's `Message::Dirty` for those nodes, silently violating R1.3.1.a (DIRTY precedes DATA in the same wave). The two-pass flush surfaced the gap.
- **Transitive `pick_next_fire` readiness** — `pick_next_fire` now walks transitive upstream via `deps` (BFS, visited set) and only fires a node when no transitive ancestor is in `pending_fires`. Pre-fix: only direct deps were checked, which let a downstream join-node fire prematurely on stale upstream when the wave ordering put a sibling-branch ahead of the join's other branch (concretely: in `A → {B, C, E}; D = combine(B, C); F = combine(D, E)`, firing `E` first added `F` to `pending_fires` before `D` was added; the picker then mistakenly fired `F` against the activation-time `D` cache, then again when `D` actually settled). Strict diamond test caught this.
- **Strict diamond + cross-node tests** — `tests/batch.rs` (~500 lines, 12 tests):
  - 3 R1.3.1.b cross-node DIRTY-first tests (4-node diamond, multi-emit outside batch, 6-node two-stage diamond)
  - 1 diamond glitch-free settle-once test
  - 7 batch API tests (closure coalesce, RAII guard drain-on-drop, nested batch, batch+complete, batch+pause, batch panic-discards-pending, per-node insertion-order preservation through two-pass)
  - 1 batch-coalesces-source-multi-emit test (verifies the queue_notify refcount fix)

### Test count

121 tests green in `graphrefly-core` (13 unit + 12 batch + 13 dispatcher + 16 setdeps + 12 pause + 11 invalidate + 12 terminal + 7 teardown + 10 resubscribable + 8 meta_teardown + 7 subscription). `cargo clippy -p graphrefly-core --all-targets` clean. `cargo fmt --check` clean. `#![forbid(unsafe_code)]` preserved.

### What did NOT land in Slice C-1.5

- TS-style drainPhase queues (separate per-tier deferral). Rust uses the existing `pending_notify: IndexMap` + tier-filter at flush time — adequate for R1.3.1.b's sink-observability invariant. Pre-wave DIRTY broadcast walk (Option B-2 from the design discussion) deferred — flush-time tier-filter (Option B-1) is sufficient.
- `pick_next_fire` performance — the new transitive walk is O(V) per candidate, worst-case O(N·V) per pick. The pre-existing perf flag in [porting-deferred.md](porting-deferred.md) is now updated to reflect this. Per-node `unresolved_dep_count` counter (the proper fix) lives there, deferred to bench-driven evidence.

### Carried forward to porting-deferred.md

- `pick_next_fire` perf — O(N·V) per pick after the transitive-walk fix. Replace with per-node `unresolved_dep_count` counter when bench surfaces the cost.

## M1 parity (Slice A+B) — closed 2026-05-05

### What landed

- **§10.2 — PAUSE/RESUME with `PauseState` enum.** `Active | Paused { locks: SmallVec<[LockId;2]>, buffer: VecDeque<Message>, dropped: u32, started_at_ns: u64 }`. Multi-pauser lockset (idempotent on duplicate lock_id). Tier-3/tier-4 messages buffer; tier-0/1/2/5/6 bypass. Core-global cap (`Core::set_pause_buffer_cap(Option<usize>)`). Drop-on-overflow with reported count. `ResumeReport { replayed, dropped }` returned on final lock release. Refcount discipline: `BindingBoundary` gained `retain_handle(handle)` (default no-op); buffered Data/Error retain on push, release on drain or overflow-drop.
- **INVALIDATE broadcast.** `Core::invalidate(node_id)`. R1.4 idempotency-within-wave via cache-already-`NO_HANDLE` check (never-populated case is a no-op). Cache clear + handle release; cascade to children with `dep_handles` reset (re-closes first-run gate). Tier-4 buffers under pause.
- **COMPLETE/ERROR cascade + Lock 2.B auto-cascade gating.** `Core::complete(node)` / `Core::error(node, handle)`. Per-node `terminal: Option<DepTerminal>` + per-dep `dep_terminals: Vec<Option<DepTerminal>>`. Child auto-cascades when all `dep_terminals` are `Some`; `pick_cascade_terminal` picks ERROR over COMPLETE. Terminal nodes silently no-op subsequent `emit` (with explicit handle-release for the rejected emit) and do not fire fn. Tier-5 bypasses pause buffer.
- **TEARDOWN auto-precedes COMPLETE (R2.6.4 / Lock 6.F).** `Core::teardown(node)`. Per-node `has_received_teardown: bool` provides idempotency. First call auto-prepends `Complete` for not-yet-terminal nodes; emits `Teardown`; cascades. Subsequent calls or cascade re-entries are no-ops.
- **Meta TEARDOWN ordering (R1.3.9.d).** `Core::add_meta_companion(parent, companion)`. Companions tear down before the parent's own terminate + Teardown wire emission, recursively (meta-of-meta first). Switched `pending_notify` from `HashMap` to `IndexMap` so flush order is deterministic — the cascade's queue order is the flush order.
- **Resubscribable terminal lifecycle (R2.2.7 / R2.5.3).** `Core::set_resubscribable(node, bool)` (config-only; panics if called after first subscribe). Late subscribe to terminal+resubscribable triggers reset: `terminal`, `has_fired_once`, `has_received_teardown`, all `dep_handles` to `NO_HANDLE`, all `dep_terminals` to `None`, pause locks drained (any retained payload handles released). Non-resubscribable terminal nodes now replay the terminal in the START handshake so late subscribers see `[START, DATA(cache)?, COMPLETE|ERROR]` — closes the late-subscriber-to-terminal-node-delivers-nothing foot-gun for this slice (resubscribable case is the alternative path).
- **§10.12 RAII `Subscription`.** `Core::subscribe` returns `Subscription` instead of `SubscriptionId`. `Drop` deregisters via `Weak<Mutex<CoreState>>`; silent no-op if Core is gone. `Send + Sync`, not `Clone`. `SubscriptionId` is now `pub(crate)`.
- **Audit fix: dynamic-node rewire fn-fire.** `Core::set_deps` on a Dynamic node now also resets `has_fired_once = false`. Pre-fix: `tracked.clear()` + `has_fired_once == true` collapsed the deliver gate to false → fn never re-fired post-rewire, leaving stale cache. Regression tests added.
- **Test infrastructure.** `TestRuntime::dynamic` helper with side-channel `tracked` reporting; `BenchBinding::retain_handle` mirrored for the napi-rs FFI bench; `Recorder` extended for all 10 message variants. The Recorder now owns its `Subscription` so dropping the recorder unsubscribes the sink.

### Test count

109 tests green in `graphrefly-core` (13 unit + 13 dispatcher + 16 setdeps + 12 pause + 11 invalidate + 12 terminal + 7 teardown + 10 resubscribable + 8 meta_teardown + 7 subscription); 113 across the workspace default members. `cargo clippy -p graphrefly-core --all-targets` clean. `cargo fmt --check` clean. `#![forbid(unsafe_code)]` preserved.

Counts include the QA pass regression tests added for **F1** (set_deps refcount leak fix on removed Error dep_terminals slots — verified via new `TestBinding::refcount_of` inspector), **F2** (Phase 13.8 Q1 lock: reject `set_deps` on terminal `n`; reject newly-added terminal-non-resubscribable deps; allow resubscribable terminal deps), and **F3** (resubscribable reset refuses after TEARDOWN; handshake replays full `[..., COMPLETE|ERROR, TEARDOWN]` lifecycle for torn-down nodes).

### What did NOT land in Slice A+B

- §10.3 / 10.4 / 10.5 / 10.6 / 10.13 perf simplifications — see [`porting-deferred.md`](porting-deferred.md). Bench-driven re-look.
- `proptest` fixtures port — Slice C-2 (test infra). `batch.rs` extraction landed in Slice C-1 (refactor; sibling-module `impl Core` split, no semantic change).
- ReentrantMutex hardening for the v1 sink-fire / `invoke_fn`-with-lock-held re-entrance limitation — M1-close hardening.
- napi-rs binding parity for the new public methods (`pause` / `resume` / `invalidate` / `complete` / `error` / `teardown` / `add_meta_companion` / `set_resubscribable`) — small follow-up; not blocking M1 close.

### Carried forward to porting-deferred.md

- Error-handle refcount cleanup beyond the resubscribable-reset and F1 set_deps-removed-dep paths — `terminal` slot retains for non-resubscribable nodes still leak until Core drop. Acceptable for v1 (terminal nodes are one-shot at this layer); future M2 graph-layer node-deletion will release on full deletion.
- The `started_at_ns` field on `PauseState::Paused` is reserved for future overflow-error reporting (TS Lock 6.A `pauseStartNs` parity); currently `#[allow(dead_code)]`.
- QA-surfaced concerns now cataloged in [`porting-deferred.md`](porting-deferred.md):
  cascade recursion stack-overflow on deep chains; `pick_next_fire` cycle-fallback busy-loop; `commit_emission` no-op-equals propagation noise to uninvolved children; pause-buffer overflow doesn't synthesize ERROR (TS Lock 6.A divergence); `alloc_lock_id` collision risk with user-supplied LockIds; DIRTY-without-DATA-across-pause-resume divergence; mid-wave `set_deps` full-removal stuck-DIRTY; `IndexMap` pending_notify mid-wave-teardown ordering edge case; non-resubscribable terminal Error handle leak via diamond cascade; sink-fire lock discipline asymmetry between `flush_notifications` (lock-held) and `Core::resume` (lock-released); `subscribe` drop-then-reacquire race against concurrent emit.

## Closing a milestone

When a milestone closes:

1. Update this file's status table.
2. Add a `## Mn — closed YYYY-MM-DD` section below documenting what landed, what was deferred, and how the milestone's deliverables map back to the migration plan.
3. Cross-reference any spec amendments (issues filed against `graphrefly/graphrefly`) and any TS-side changes (PRs against `graphrefly-ts`).
4. Tag a release: `git tag -a vM<n>.0.0 -m "Mn complete"` and bump `[workspace.package].version` in the workspace `Cargo.toml`.

## Post-1.0 backlog

Per the deferral guardrails in `~/src/graphrefly-ts/docs/implementation-plan.md` Phase 14:

- CRDT-backed `reactiveMap` / `reactiveLog` variants (`yrs`, `automerge`, `loro`, `diamond-types`)
- `peerGraph(transport)` multi-replica sync via libp2p
- Cross-replica changeset merging with CRDT semantics
- Tighter cross-tier WAL atomicity beyond best-effort

These are post-1.0 by deliberate design — they need the substrate that M5 establishes, and the libraries Rust offers (vs. what TS would have to re-implement). Don't land them until M1–M6 close.
