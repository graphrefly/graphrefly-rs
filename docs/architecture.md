# Architecture pointer doc

Where to find the truth. **This file does not duplicate spec content** — it is a map. Per the project's [single-source-of-truth principle](../CLAUDE.md#memory-principles-inherited-from-graphrefly-ts), every architectural decision lives in exactly one canonical location, and this doc points to that location.

If something here disagrees with the canonical source, the canonical source wins. File a PR to fix this map.

## The cleaving plane

graphrefly-rs is structured around the **handle protocol**: the Core operates entirely on opaque `HandleId` integers, never on user values `T`. Per-language SDK harnesses (napi-rs, pyo3, wasm-bindgen) own the value-to-handle registry. This split is what makes a Rust port viable without losing language ergonomics.

The cleaving plane is documented end-to-end in:

- **`~/src/graphrefly-ts/archive/docs/SESSION-rust-port-architecture.md`** — strategic discussion: timing, layer-by-layer recommendation, 6-milestone migration plan, threading and concurrency wins, multi-distribution model.
- **`~/src/graphrefly-ts/docs/research/handle-protocol.tla`** — TLA+ refinement of `wave_protocol.tla` formalizing the handle interpretation. Verified across 5 scenario MCs (39,331 distinct states).
- **`~/src/graphrefly-ts/docs/research/handle-protocol-audit-input.md`** — per-rule classification of the 247-rule 13.6 inventory: which rules collapse to Core-internal vs which survive at the binding-layer API. This is the layer-classification key to use during implementation.
- **`~/src/graphrefly-ts/src/__experiments__/handle-core/`** — TS prototype (`core.ts`, `bindings.ts`, 22 vitest tests). Reference impl when porting M1.

## Spec sources

The behavior protocol is canonical in `~/src/graphrefly`:

| File | What it covers |
|------|----------------|
| `GRAPHREFLY-SPEC.md` | Message protocol, node primitive, lifecycle, equals-substitution, PAUSE/RESUME, INVALIDATE, versioning. **The behavior authority.** |
| `COMPOSITION-GUIDE.md` | Index for the four-level composition guide |
| `COMPOSITION-GUIDE-PROTOCOL.md` | L0 — message tuples, dispatcher, batch coalescing |
| `COMPOSITION-GUIDE-GRAPH.md` | L1 — Graph container, sentinel/null guards, feedback cycles |
| `COMPOSITION-GUIDE-PATTERNS.md` | L2 — `promptNode`, `dynamicNode`, controller-with-audit, `frozenContext`, criteria-grid, etc. |
| `COMPOSITION-GUIDE-SOLUTIONS.md` | L3 — harness 7-stage loop, `agentMemory`, `resilientPipeline`, multi-agent shapes |
| `formal/wave_protocol.tla` | TLA+ spec; ~35 invariants. **The Rust impl must verify against the same model.** |
| `formal/wave_protocol_*_MC.tla` | 16 scenario harnesses (pause, custom_equals, multisink, invalidate, etc.) |

Read the relevant L<n> guide before touching the corresponding crate. Per project memory: **skipping leads to null-guard violations and layer-breaking optimizations.**

## Canonical-but-mutable references in graphrefly-ts

These are the operational docs that evolve over time (as opposed to the spec, which evolves more deliberately):

| File | Role |
|------|------|
| `~/src/graphrefly-ts/docs/implementation-plan.md` | Phase 11–16 sequencer. Phase 13.6 / Phase 14 deferral guardrails (DON'T DEFER / STRONG DEFER) define what's TS-implemented vs Rust-deferred. |
| `~/src/graphrefly-ts/docs/optimizations.md` | Open backlog, anti-patterns, deferred follow-ups. |
| `~/src/graphrefly-ts/archive/optimizations/*.jsonl` | Resolved decisions log. |
| `~/src/graphrefly-ts/archive/docs/SESSION-*.md` | Design discussions; index in `archive/docs/design-archive-index.jsonl`. |

## Crate map

Each crate's `src/lib.rs` has a doc comment listing its planned modules. The mapping to TS-canonical sources:

| Crate | TS source it ports | Spec sections |
|-------|-------------------|---------------|
| `graphrefly-core` | `~/src/graphrefly-ts/src/core/` (~4,600 lines, dominated by `node.ts`) | §1 protocol, §2 node, §6 dep records, §7 versioning |
| `graphrefly-graph` | `~/src/graphrefly-ts/src/graph/` | §3 graph, §4 utilities, Phase 6 content addressing (CIDs) |
| `graphrefly-operators` | `~/src/graphrefly-ts/src/extra/operators/` + parts of `extra/sources/` | COMPOSITION-GUIDE-PROTOCOL §1, §9, §19, §41 |
| `graphrefly-storage` | `~/src/graphrefly-ts/src/extra/{node-only storage}/` | §2.5 storage, §G.27 tier rules |
| `graphrefly-structures` | `~/src/graphrefly-ts/src/extra/structures/` (`reactiveMap`, etc.) | Phase 14 op-log changesets |
| `graphrefly-bindings-{js,py,wasm}` | new — no TS analog | SDK harness contract |

Crates `patterns/`, `compat/`, browser-only sources, and user-facing fns deliberately stay in the binding language (TS or Python). See `SESSION-rust-port-architecture.md` Part 5 for the layer-by-layer rationale.

## Design invariants

The non-negotiable cross-language invariants (no polling, no imperative triggers, no raw async primitives in reactive layer, central timer, first-run gate, etc.) are listed in [`../CLAUDE.md`](../CLAUDE.md) → "Design invariants" with a pointer to the canonical text in `GRAPHREFLY-SPEC.md` §5.8–5.12.

The Rust-specific additions (Send/Sync discipline, no async runtime in Core, newtype wrappers, `clippy::pedantic` warn-by-default, etc.) live in the same `CLAUDE.md` section.

## Why this doc exists

Without this map, future contributors and AI sessions would re-derive the architecture from scratch by reading the SPEC, then the composition guides, then the migration plan, then the prototype, then the audit input — in some unspecified order. This doc is the entry point that orders the reading and points at the right canonical source for each concern.

If you find yourself wanting to add architectural content **here** instead of pointing to graphrefly-ts or graphrefly: stop. Add it to the canonical location and point this file at it. Architectural drift between this repo and graphrefly-ts is the failure mode this doc guards against.
