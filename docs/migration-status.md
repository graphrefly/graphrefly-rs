# Migration status

Live tracker for the 6-milestone Rust port. Update after each milestone closes. The full migration plan lives in `~/src/graphrefly-ts/archive/docs/SESSION-rust-port-architecture.md`.

## Current state (2026-05-05)

**DS-14 locked + Phase 14 landed in TS** ([archive/docs/SESSION-DS-14-changesets-design.md](https://github.com/graphrefly/graphrefly-ts/blob/main/archive/docs/SESSION-DS-14-changesets-design.md)). Rust port greenlit per [SESSION-rust-port-architecture.md](https://github.com/graphrefly/graphrefly-ts/blob/main/archive/docs/SESSION-rust-port-architecture.md) Part 10 lock. **Slice A+B landed 2026-05-05** — full M1 parity (PAUSE/RESUME, INVALIDATE, COMPLETE/ERROR cascade, TEARDOWN-precedes-COMPLETE, resubscribable terminals, meta-TEARDOWN ordering) + §10.12 RAII Subscription + dynamic-rewire audit fix.

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
| M2 | `graphrefly-graph` | ⏸ blocked | After M1 close + DS-14 lock |
| M3 | `graphrefly-operators` | ⏸ blocked | After M1 close |
| M4 | `graphrefly-storage` | ⏸ blocked | After M2 + DS-14 (`restoreSnapshot mode: "diff"` WAL replay shape) |
| M5 | `graphrefly-structures` | ⏸ blocked | After M2 + DS-14 (op-log changesets) — STRONG DEFER per Phase 14 guardrail |
| M1+ | `graphrefly-bindings-js` | ⏸ blocked | Bench-study slice lands alongside M1 dispatcher |
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

- [ ] Add CI: `.github/workflows/ci.yml` with `cargo check`, `cargo test`, `cargo clippy`, `cargo fmt --check`, `cargo deny check` against the Rust 1.95 matrix.
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
- [ ] Port `_invariants.ts` property-test fixtures to `proptest`. *(Slice C — test infra, after parity)*
- [ ] Port `batch.ts`: wave coalescing extracted to its own module. *(Slice C — refactor; no semantic change)*
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
- [ ] Stand up TLC-runs-in-CI step using existing `wave_protocol_*_MC.tla` + the new `wave_protocol_rewire_MC.tla` against the handle interpretation.

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
- `proptest` fixtures port and `batch.ts` extraction to its own module — Slice C (test infra + refactor).
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
