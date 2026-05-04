# Migration status

Live tracker for the 6-milestone Rust port. Update after each milestone closes. The full migration plan lives in `~/src/graphrefly-ts/archive/docs/SESSION-rust-port-architecture.md`.

## Current state (2026-05-03)

**Scaffold complete. M1 not yet started.** Awaiting Phase 13.6.A (rules audit lock) and Phase 14 design lock-down (DS-14) in graphrefly-ts.

| Milestone | Crate | Status | Notes |
|-----------|-------|--------|-------|
| Pre-M1 scaffold | (workspace) | ✅ done | 8 crates with stubs; cargo check + test + clippy + fmt all clean |
| M1 | `graphrefly-core` | ⏸ blocked | Awaiting 13.6.A lock |
| M2 | `graphrefly-graph` | ⏸ blocked | After M1 |
| M3 | `graphrefly-operators` | ⏸ blocked | After M1 |
| M4 | `graphrefly-storage` | ⏸ blocked | After M2 (codec dependency) |
| M5 | `graphrefly-structures` | ⏸ blocked | After M2; lands Phase 14 op-log delta protocol |
| M1+ | `graphrefly-bindings-js` | ⏸ blocked | Grows alongside M1–M5 |
| M6 | `graphrefly-bindings-py` | ⏸ blocked | After M5; closes graphrefly-py G.6 parity gap |
| any | `graphrefly-bindings-wasm` | ⏸ blocked | Lands alongside napi-rs progression |

## Pre-M1 dependencies (in graphrefly-ts)

These must close before M1 begins:

- [ ] **Phase 13.6.A — rules/invariants audit + locking.** Substantial 9Q audit producing spec/composition-guide amendments. See [`~/src/graphrefly-ts/docs/implementation-plan.md`](https://github.com/graphrefly/graphrefly-ts/blob/main/docs/implementation-plan.md) Phase 13.6.
- [ ] **Phase 14 design lock — DS-14.** Co-design 5 threads (op-log changesets, worker bridge wire-protocol, `lens.flow` delta companion, `reactiveLog.scan`, `restoreSnapshot mode: "diff"` WAL replay).

Once both close, **skip 13.6.B + Phase 14 implementation in TS** per the deferral guardrails added to `implementation-plan.md`. Start M1 here.

## M1 entry checklist

When M1 begins, work to do FIRST in this repo:

- [ ] Add CI: `.github/workflows/ci.yml` with `cargo check`, `cargo test`, `cargo clippy`, `cargo fmt --check`, `cargo deny check` against the Rust 1.95 matrix.
- [ ] Port `src/core/messages.ts` → `crates/graphrefly-core/src/message.rs` (smallest, simplest port; warm-up).
- [ ] Port `src/core/clock.ts` → `crates/graphrefly-core/src/clock.rs` (tiny; uses `std::time::Instant` + `std::time::SystemTime`).
- [ ] Set up newtypes: `NodeId(u64)`, `HandleId(u64)`, `FnId(u64)` in `crates/graphrefly-core/src/handle.rs`.
- [ ] Define `BindingBoundary` trait in `crates/graphrefly-core/src/boundary.rs` mirroring the TS prototype's `BindingBoundary` interface.
- [ ] Verify the Rust impl passes the same property-test fixtures as the TS implementation (port `~/src/graphrefly-ts/src/__tests__/properties/_invariants.ts` to `proptest`).
- [ ] Stand up a TLC-runs-in-CI step using `~/src/graphrefly/formal/wave_protocol.tla` against an MC harness updated for the handle interpretation.

## Reference impl

The TS prototype at `~/src/graphrefly-ts/src/__experiments__/handle-core/` is the reference implementation for the handle protocol. Port it module-for-module:

| TS file | Rust target |
|---------|-------------|
| `core.ts` (~370 lines, all Core dispatch) | `crates/graphrefly-core/src/{message,handle,boundary,node,batch}.rs` |
| `bindings.ts` (~370 lines, value registry + FFI counters + dynamic builder) | `crates/graphrefly-bindings-js/src/{value_registry,counters,*}.rs` |
| `core.test.ts` (13 tests) | `crates/graphrefly-core/tests/` (integration tests) |
| `extensions.test.ts` (9 tests, FFI counters + dynamic + refcount) | same |

The Rust impl should pass the same 22 invariant tests plus additional Rust-specific tests (Send/Sync discipline, panic safety, leak detection under loom).

## Open architectural questions (not blocking M1, but worth re-validating during port)

From `SESSION-rust-port-architecture.md` Open Questions section:

1. **Cross-language handle-space composition.** TS-frontend mounting Python-frontend subgraph — disjoint registries; explicit serialize bridge or process boundary? Out of scope until post-M6.
2. **Async fn boundary modeling in TLA+.** Current spec is sync-only; pyo3 async path needs additional MC.
3. **Refcount soundness companion module.** Handle leak detection in production; smoke-tested in TS prototype, needs production-grade Rust impl.
4. **Custom-equals oracle correctness beyond identity-extension** (symmetry/transitivity).
5. **Phase 14 missing `### Phase 14 — …` header** in `~/src/graphrefly-ts/docs/implementation-plan.md` (line ~1207). Trivial fix when next touching that file.

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
