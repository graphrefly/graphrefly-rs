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
cargo test --workspace              # all tests across crates
cargo test -p graphrefly-core       # one crate
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

## Rust-specific invariants

Beyond the cross-language spec:

1. **No `unsafe`. Anywhere. In any crate of this workspace.** Enforced by `#![forbid(unsafe_code)]` at every crate root. If a feature seems to need unsafe (FFI hot paths, raw pointer tricks, manual memory layout), the answer is to find a safe abstraction (parking_lot, dashmap, imbl, redb, napi-rs / pyo3 wrappers) — not to allow the lint. The Rust port's value over TS / PY is *compiler-enforced safety*; bypassing it forfeits the win. If you genuinely cannot find a safe path, escalate to spec-level discussion before adding unsafe.
2. **Compiler-enforced thread safety.** `Send` + `Sync` discipline applies to every public type. No `Rc<T>` or `RefCell<T>` in shared state — use `Arc<T>` + `Mutex<T>` / `parking_lot::ReentrantMutex<T>`.
3. **Per-subgraph `parking_lot::ReentrantMutex`.** Mirrors the graphrefly-py per-subgraph RLock parity goal. Two threads operating on different subgraphs run truly parallel; same subgraph serializes via the lock.
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
