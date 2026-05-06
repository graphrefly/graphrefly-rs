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
| M1 (Slice C + Slice A-bigger) | `graphrefly-core` | ✅ landed | 2026-05-05: `batch.rs` extraction, user-facing `Core::batch`/`begin_batch`, R1.3.1.b two-pass cross-node DIRTY-first delivery, transitive `pick_next_fire` (diamond glitch fix), `proptest` framework + 7 protocol-layer invariants, drop-then-fire lock discipline (sinks re-enter Core safely), subscribe-vs-emit race fix, iterative cascades for 5000-node chains. 139 tests green; clippy + fmt clean. |
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
