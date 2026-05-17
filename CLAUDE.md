# graphrefly-rs — Claude orientation

**graphrefly-rs** is the Rust implementation of the [GraphReFly](https://graphrefly.dev) reactive graph protocol. This file orients new Claude sessions to the cross-repo context. **This is a pointer file. Do not duplicate canonical content; reference it.** (See "Single source of truth" memory below.)

## Cross-repo layout

| Repo | Path | Role |
|------|------|------|
| **graphrefly-rs** | this repo | **Future canonical implementation.** Cargo workspace + per-language bindings (napi-rs, pyo3, wasm-bindgen). Currently scaffold only. |
| **graphrefly** (spec) | `~/src/graphrefly` | `GRAPHREFLY-SPEC.md`, `COMPOSITION-GUIDE-*.md`, `formal/wave_protocol.tla`. **Behavior + protocol authority.** |
| **graphrefly-ts** | `~/src/graphrefly-ts` | **Currently canonical TS impl.** Operational docs, roadmap, optimizations, skills, audit prep all live here. Migrates to a thin shim over napi-rs binding once Rust port reaches parity. |
| **graphrefly-py** | `~/src/graphrefly-py` | Currently canonical PY impl. Migrates to a thin shim over pyo3 binding once Rust port reaches parity. |

## Canonical references (read these — DO NOT duplicate them here)

| Doc | Lives in | Role for graphrefly-rs |
|-----|----------|------------------------|
| `GRAPHREFLY-SPEC.md` | `~/src/graphrefly/` | **Behavior spec.** Every public type and message-handling path in this repo MUST honor it. |
| `COMPOSITION-GUIDE-{PROTOCOL,GRAPH,PATTERNS,SOLUTIONS}.md` | `~/src/graphrefly/` | Composition patterns. Apply when porting `extra/` and `patterns/` analogs. |
| `formal/wave_protocol.tla` | `~/src/graphrefly/formal/` | TLA+ spec + 16 scenario MCs. The Rust impl must verify against the same model. |
| `archive/docs/SESSION-rust-port-architecture.md` | `~/src/graphrefly-ts/` | **The migration plan.** 6-milestone phasing, layer-by-layer port recommendation, deferral guardrails. Read FIRST when picking up port work. |
| `docs/research/handle-protocol.tla` + `handle_protocol_MC.tla` | `~/src/graphrefly-ts/` | Handle-protocol refinement of wave_protocol. The architectural cleaving plane that makes this Rust port viable. |
| `docs/research/handle-protocol-audit-input.md` | `~/src/graphrefly-ts/` | Per-rule classification: which 13.6 invariants become Core-internal vs binding-layer. Use as the layer-classification key during M1–M5. |
| `src/__experiments__/handle-core/` | `~/src/graphrefly-ts/` | **TS prototype** of the handle-protocol Core (~370 lines `core.ts` + ~370 lines `bindings.ts` + 22 vitest tests). Use as the reference impl when porting M1. |
| `docs/implementation-plan.md` | `~/src/graphrefly-ts/` | Phase 11–16 sequencer. Phase 13.6 / Phase 14 deferral guardrails (DON'T DEFER / STRONG DEFER) define what landed-in-TS vs what waits-for-Rust. |

## Commands

```bash
# Toolchain — pinned in rust-toolchain.toml + .mise.toml (Rust 1.95)
mise trust && mise install         # one-time, after cloning
eval "$(mise env)"                  # or just exec a shell that sources mise

# Build / test / lint
cargo check --workspace             # fast type-check, all crates

# Test loop — PREFER nextest. `cargo test` runs the ~27 graphrefly-core
# integration-test binaries one-at-a-time (only parallelising within a
# binary); nextest runs ALL tests across ALL binaries in one global thread
# pool, and its slow-timeout (.config/nextest.toml) KILLS a deadlocked
# test after 3min instead of wedging the suite + the cargo build lock.
#
# The default profile QUARANTINES the 4 cascade_depth stack-safety stress
# tests (~95% of wall time, ~13.6s each) → inner loop ≈ 0.5s for 304 tests
# vs ~14s. They are NOT benchmarks — they guard the iterative-cascade /
# no-stack-overflow invariant — so they STILL run under the ci/stress
# profiles. Run the full suite (incl. cascade_depth) before merge / after
# touching cascade/terminate/teardown/invalidate code.
scripts/dev-test.sh                 # 304 tests, isolated, ~0.5s (no cascade_depth)
scripts/dev-test.sh -p graphrefly-core            # one crate
scripts/dev-test.sh -p graphrefly-core <filter>   # filtered
cargo t                             # = nextest run (default; shared target)
cargo tc                            # = nextest run --profile ci (FULL, incl. cascade_depth)
cargo nextest run --profile stress  # FULL on dev box (incl. cascade_depth)
cargo test --workspace              # legacy fallback (slow; runs cascade_depth too)
cargo test -p graphrefly-core       # one crate (legacy)

# ⚠ Parallel sessions: `cargo test`/`cargo t` share `target/` and its
# build lock — a second run BLOCKS behind the first (and is wedged forever
# if the first deadlocks). Use `scripts/dev-test.sh`: it sets a
# per-worktree CARGO_TARGET_DIR so concurrent sessions never share a lock.

cargo clippy --workspace --all-targets   # pedantic-tier lints
cargo fmt --all                     # rustfmt
cargo deny check                    # supply-chain audit (after `cargo install cargo-deny`)

# Concurrency permutation testing (loom)
cargo test -p graphrefly-core --features loom-checked

# Bindings (each requires its own toolchain)
cd crates/graphrefly-bindings-js && pnpm build         # napi-rs (needs Node + pnpm)
cd crates/graphrefly-bindings-py && maturin develop    # pyo3 (needs maturin)
cd crates/graphrefly-bindings-wasm && wasm-pack build  # wasm-bindgen (needs wasm-pack)
```

## Layout

```
crates/
├── graphrefly-core/              # M1: dispatcher, message tiers, batch, wave engine
├── graphrefly-graph/             # M2: Graph container, snapshot, content addressing (CIDs)
├── graphrefly-operators/         # M3: built-in operator node types
├── graphrefly-storage/           # M4: tier dispatch + Node-side persistence (redb)
├── graphrefly-structures/        # M5: reactiveMap/List/Log/Index (imbl-backed)
├── graphrefly-bindings-js/       # M1+: napi-rs JS bindings (cdylib)
├── graphrefly-bindings-py/       # M6: pyo3 Python bindings (cdylib, abi3)
└── graphrefly-bindings-wasm/     # WASM target (browser, edge runtimes)
```

Default `cargo build` excludes the bindings crates (they need their own toolchains). See `Cargo.toml` workspace `default-members`.

## Migration status

Currently: **scaffold complete**, awaiting Phase 13.6 + Phase 14 lock-down in graphrefly-ts before M1 begins. See `docs/migration-status.md` for milestone tracking.

The agreed sequence:
1. Lock 13.6.A (spec amendments) in graphrefly-ts
2. Lock Phase 14 design (DS-14 audit) in graphrefly-ts
3. Skip 13.6.B + Phase 14 implementation in TS — fold into Rust port
4. **Start M1 here** in graphrefly-rs

## Design invariants (cross-language, non-negotiable)

These are the spec-level invariants every implementation honors. Canonical text in `~/src/graphrefly/GRAPHREFLY-SPEC.md` §5.8–5.12; do NOT copy that text here.

1. **Handle-protocol cleaving plane.** Core operates on opaque `HandleId` integers. User values `T` live in the binding-side registry; they never enter Core types. See `~/src/graphrefly-ts/docs/research/handle-protocol.tla` for the formalization.
2. **No polling.** State changes propagate via messages. Use reactive timer sources, not `std::thread::sleep` loops or `tokio::interval` busy-checks against state.
3. **No imperative triggers in public API.** All coordination via message flow. Imperative methods only on the L2.35 controller-with-audit primitives (jobQueue, cqrs, saga, processManager) — see `archive/docs/SESSION-rust-port-architecture.md` for the rubric.
4. **First-run gate.** A compute node does NOT fire fn until every declared dep has delivered at least one real value. Encoded in Core via `Pending | Has(HandleId)` per dep.
5. **Equals-substitution under `equals: 'identity'` is zero-FFI.** It's a `HandleId` u64 compare in pure Rust. Custom equals crosses the binding boundary; opt-in only.
6. **DIRTY before DATA/RESOLVED in the same wave.** Wave engine must enforce.
7. **`actions.up` rejects tier-3/4.** Make this a compile error via separate `UpMessage` / `DownMessage` enums.

## Layering predicate — substrate vs presentation (locked 2026-05-14, D193)

**The single rule** that decides whether a new TS/PY symbol belongs in `graphrefly-rs` (substrate) or stays binding-side (presentation):

> **Substrate** = the symbol manages state, dispatch, mutation, or persistence — i.e., the mechanism. Port to Rust.
> **Presentation** = the symbol composes substrate into domain shapes (graph-level sugar, observability surfaces, user-facing factories, runtime-idiomatic glue). Stay binding-side.
>
> **Hot-path tie-breaker:** when (a) is ambiguous, ask "does this execute on every emission / per-wave?" Yes → substrate; one-shot/wiring-only/audit-only → presentation.
> **Cross-language sharability cross-check:** if JS and PY both need the same shape with the same semantics, that's evidence for substrate; language-idiomatic shapes (Promise, async iter, NestJS, Pythonic context manager, decorators) are evidence for presentation.

Source: `~/src/graphrefly-ts/archive/docs/SESSION-rust-port-layer-boundary.md` Unit 1 (Q9.1 = C, user-locked 2026-05-14).

**Three-package install-time model** (locked 2026-05-14, D198):

```
@graphrefly/graphrefly  ← presentation only (patterns + binding-only extras + compat)
       │  peerDependency: pick ONE substrate provider
       ▼
@graphrefly/pure-ts   OR   @graphrefly/native
  (TS substrate)            (Rust substrate via napi)
```

Both substrate packages MUST satisfy the same `packages/parity-tests/impls/types.ts` `Impl` interface. `@graphrefly/wasm` and the facade-with-fallback are explicitly **canceled** in favor of this model (supersedes Phase 13 Deferred 1).

### Rust crate ↔ TS folder mapping

| Rust crate | TS folder (in `@graphrefly/pure-ts`) | Substrate? |
|---|---|---|
| `graphrefly-core` | `core/` | substrate (always) |
| `graphrefly-graph` | `graph/` | substrate (always) |
| `graphrefly-operators` | `extra/operators/` + `extra/composition/stratify` | substrate |
| `graphrefly-storage` | `extra/storage/` (Node tiers only) | substrate (Node-only path) |
| `graphrefly-structures` | `extra/data-structures/` | substrate |
| `graphrefly-bindings-js` | (consumed by `@graphrefly/native`) | binding glue |
| `graphrefly-bindings-py` | (consumed by `graphrefly-py`) | binding glue |
| `graphrefly-bindings-wasm` | (consumed by `@graphrefly/wasm`, deferred) | binding glue |

### `extra/` row classification (locked 2026-05-14, D197)

Each `extra/<row>/` sub-folder either ports (substrate) or stays binding-side (presentation). New `extra/` subfolders MUST be classified at introduction:

| `extra/` sub-folder | Classification | Rust home | Notes |
|---|---|---|---|
| `operators/` | substrate | `graphrefly-operators` | Already ported (Slice C-1, C-2, U). |
| `data-structures/` | substrate | `graphrefly-structures` | Already ported (M5.A/M5.B). |
| `storage/` (Node tiers) | substrate | `graphrefly-storage` | Already ported (M4). |
| `composition/stratify` | substrate | `graphrefly-operators::stratify` | ✅ Landed 2026-05-14 (D199, Layer-boundary slice). `stratify_branch` operator (~370 LOC incl. two-dep DIRTY gating per N1, `Drop` impl, `cache_of` pre-seed per N2) + `BindingBoundary::invoke_stratify_classifier_fn` + `OperatorBinding::register_stratify_classifier` + napi `register_stratify_branch`. |
| `composition/{verifiable, distill, pubsub, backpressure, externalProducer}` | presentation | — | Composable from `derived` + `filter` + `state`; stay binding-side. |
| `mutation/{lightMutation, wrapMutation, createAuditLog, tryIncrementBounded}` | presentation | — | Decorators over `reactiveLog` (already substrate); HOF in TS, context-manager in PY. |
| `sources/sync` (`of`, `empty`, `fromIter`, `never`, `throwError`) | substrate | `graphrefly-operators::source` | Already ported (Slice 3e/3f). |
| `sources/event/fromTimer` | substrate | `graphrefly-operators::interval` | Already ported (Slice T) — surface binding-side as `fromTimer`. |
| `sources/event/fromCron` | substrate (timer) + presentation (parser) | `graphrefly-operators::interval` | Cron parser stays binding-side; consumes Rust `interval` for the timer. |
| `sources/event/{fromEvent, fromRaf}` | presentation | — | DOM/runtime APIs are binding-language idiomatic. |
| `io/{http, ws, sse, webhook, reactiveSink}` | presentation | — | Network IO uses language-idiomatic libs per binding. |
| `data-structures` Graph sugar (`graph.log/list/map/index`) | presentation | — | Ergonomic facade over substrate; stays binding-side (confirmed Slice U-napi S3). |

**Anti-pattern caught in honest review (2026-05-14):** A TS surface that looks like substrate may actually be a thin **decorator** over already-existing substrate. If true, classify as presentation. Example: `mutation/createAuditLog` looked like substrate; the audit log itself IS `reactiveLog` (already substrate), and the decorator is presentation.

### napi widening policy — consumer-pressure-driven, parity-test gated (locked 2026-05-14, D196)

A Rust-core surface gets a napi binding when EITHER:

1. A non-pattern consumer materializes (concrete JS user reaches for it), OR
2. A parity scenario in `packages/parity-tests/scenarios/<layer>/` exercises it cross-impl.

Presentation symbols (per the table above) **never** enter `Impl` (`packages/parity-tests/impls/types.ts`) and never bind to napi. Substrate symbols not yet exercised by a parity scenario STAY deferred — recorded in `docs/porting-deferred.md`. "Parity scenarios are the consumer pressure signal" — the scenario is itself the receipt that justifies the binding work.

**EXEMPTION — native-publish milestone (D203, user-locked 2026-05-15).** D196 governs *incremental* one-off widening; it is carved out for the bounded `@graphrefly/native` publish milestone. That milestone (napi parity for newer methods, F18, F20, the TS wrapper layer, the publish matrix, full `rustImpl` parity arm) is scheduled as the **next `/porting-to-rs` batch regardless of consumer-pressure demand** — the gating parity scenarios are authored *as outputs of* the slice, not preconditions. See `docs/migration-status.md` § "NEXT BATCH — Ship `@graphrefly/native`". Rationale: D196's trigger is unreachable for the native-publish milestone (no scenario → no binding → no publish → no consumer → no scenario); the exemption breaks the self-perpetuating deferral while keeping D196 intact for everything outside the milestone.

Source: `~/src/graphrefly-ts/archive/docs/SESSION-rust-port-layer-boundary.md` Unit 4 (Q9.1 = B, user-locked 2026-05-14); D203 exemption user-locked 2026-05-15.

## Rust-specific invariants

Beyond the cross-language spec:

1. **No `unsafe`. Anywhere. In any crate of this workspace.** Enforced by `#![forbid(unsafe_code)]` at every crate root. If a feature seems to need unsafe (FFI hot paths, raw pointer tricks, manual memory layout), the answer is to find a safe abstraction (parking_lot, dashmap, imbl, redb, napi-rs / pyo3 wrappers) — not to allow the lint. The Rust port's value over TS / PY is *compiler-enforced safety*; bypassing it forfeits the win. If you genuinely cannot find a safe path, escalate to spec-level discussion before adding unsafe.
2. **Compiler-enforced thread safety.** `Send` + `Sync` discipline applies to every public type. No `Rc<T>` or `RefCell<T>` in shared state — use `Arc<T>` + `Mutex<T>` / `parking_lot::ReentrantMutex<T>`.
3. **Hybrid concurrency: per-thread `WaveState` thread_local for wave-scoped state + per-partition `parking_lot::ReentrantMutex` for cross-thread serialization.** Wave-scoped state (`pending_fires`, `pending_notify`, `deferred_*`, `wave_cache_snapshots`, `pending_auto_resolve`, etc.) lives in a per-thread thread_local in `crate::batch::WaveState`. **Two ownership-gate fields are NOT in `WaveState`** (D047, 2026-05-15): `in_tick` is per-(Core, thread) in `crate::batch::IN_TICK_OWNED` (keyed by `Core::generation` — Core-global broke disjoint-partition drain ownership; pure thread-local broke cross-Core isolation, /qa F1); `currently_firing` is Core-global on `CoreState` (cross-thread P13 set_deps check, /qa F2). Cross-thread emits on the same partition BLOCK on the partition `wave_owner` `ReentrantMutex`; the receiving thread takes ownership and runs the wave with its own `WaveState`. Cross-partition cascades acquire all touched partitions upfront in ascending `SubgraphId` order (Q7 from `archive/docs/SESSION-rust-port-d3-per-subgraph-parallelism.md`) — deadlock-free under any interleaving (verified by `wave_protocol_partitioned_MC` TLA+ harness in graphrefly-ts). **Phase J bench post-Q-beyond (2026-05-10):** disjoint state-emit 2t = −17.9% vs prior baseline; fn-fire serial −20.9%; parallelism property preserved (1.84× separation between disjoint and same-partition fn-fire). **⚠️ D047 CORRECTION (2026-05-15): the Phase J *disjoint-regime* speedup figures were inflated by an `in_tick` drain-skip bug (thread B's wave never drained). Post-fix `fnfire_parallel_2t_disjoint` ≈2.8× SLOWER than serial (not 1.23× faster); the disjoint per-subgraph-parallelism value proposition is materially weaker and is flagged for spec re-validation — see `docs/porting-deferred.md` Phase J CORRECTION + `docs/rust-port-decisions.md` D047.** The architecture decision was bench-driven (D108–D110) — see `crates/graphrefly-core/benches/lock_strategy.rs` for the canonical comparison harness across `parking_lot::Mutex` / `RwLock` / `DashMap` / `crossbeam-channel` / thread_local. Future maintainers: resist the urge to "shard everything" without bench evidence; the bench shows uncontended same-thread mutex is identical cost to thread_local borrow, while shared-mutex cross-thread cache-line bouncing is 2.7× slower than per-partition mutex / thread_local. Per-partition `nodes`/`children` shards (Q-beyond sub-slice 4) are DEFERRED evidence-gated — no current workload shows state-mutex contention on node lookups; lifts when one does. See `docs/porting-deferred.md` "Per-partition `nodes`/`children` shards (Q-beyond Sub-slice 4)" entry.
4. **No async runtime in Core.** The Core dispatcher is sync. `tokio` only enters the picture in `graphrefly-storage` (for blocking I/O off-thread via napi-rs / pyo3) and in bindings; never in `graphrefly-core`.
5. **No `unwrap()` / `expect()` on user-facing paths.** Domain errors via `thiserror`-derived enums. `unwrap` only in tests, build scripts, or genuinely-impossible-by-construction paths (with a comment explaining why).
6. **`#[must_use]` on every public fn that returns a value.** Errors and unused emissions are the most common bug class; the lint catches them.
7. **`clippy::pedantic` + `rust_2018_idioms` warn-by-default** in every crate's `lib.rs`. Allow on a per-need basis with a comment, never silently.
8. **Public types live behind newtype wrappers** (`NodeId(u64)`, `HandleId(u64)`, etc.). Don't expose raw integers — they collide structurally with each other and with user counters.
9. **Snapshot serialization uses `serde_ipld_dagcbor`** for content-addressed paths and `ciborium` for non-content-addressed snapshots. **Never** mix codec choice with content-addressing semantics.

## Memory principles (inherited from graphrefly-ts)

These principles guide all sessions on this repo. They are **not** duplicated as files in this repo — they live in `~/.claude/projects/-Users-davidchenallio-src-graphrefly-ts/memory/`. Read them via that path when needed.

- **Single source of truth.** `feedback_single_source_of_truth.md` — every doc points to canonical, never duplicates. This file is itself an example.
- **No autonomous decisions.** `feedback_no_autonomous_decisions.md` — surface spec ↔ code conflicts, don't silently pick. File-by-file review cadence for multi-file rewrites.
- **No implementation without explicit approval.** `feedback_no_implement_without_approval.md` — decisions locked ≠ implementation approved. Wait for explicit "implement."
- **No backward compat (pre-1.0).** `feedback_no_backward_compat.md` — free to refactor / rename / break APIs. No legacy shims; update all call sites in same pass.
- **No imperative in public API.** `feedback_no_imperative.md` — reactive `NodeInput` signals over imperative trigger methods. Actively remove unused imperative paths.
- **Don't wrap imperative helpers as primitives.** `feedback_no_imperative_wrap_as_primitive.md` — widen the helper's args to `T | Node<T>` instead. In Rust: `impl Into<NodeOr<T>>`.
- **Read guides before implementing.** `feedback_read_guides_before_implementing.md` — read COMPOSITION-GUIDE before composition/operator work. Skipping leads to null-guard violations and layer-breaking optimizations.
- **Debug process.** `feedback_debug_process.md` — read COMPOSITION-GUIDE first, use graphProfile / harnessProfile to inspect, isolate test before fixing.

## Working with this repo

- **Phase 13.6 audit** is in flight in graphrefly-ts. Do NOT pre-empt it by feeding handle-protocol artifacts into the audit session. After the audit closes, use the audit-input doc as a second-pass comparison.
- **Spec amendments** land in `~/src/graphrefly/GRAPHREFLY-SPEC.md` and `COMPOSITION-GUIDE-*.md` — never in this repo. Reference them; don't fork them.
- **Optimization decisions** land in `~/src/graphrefly-ts/docs/optimizations.md` and `archive/optimizations/*.jsonl`. Cross-reference; don't duplicate.
- **Session docs** for design discussions land in `~/src/graphrefly-ts/archive/docs/SESSION-*.md` with an entry in `archive/docs/design-archive-index.jsonl`. Same convention applies if a discussion solely concerns this repo's internals — but most strategic discussions span both repos and belong in graphrefly-ts.
