# Architecture Pointer

This file is a map, not a spec. The language-neutral authority for protocol
rules, decisions, conformance scenarios, formal models, and sequencing lives in
`~/src/graphrefly`. If anything here disagrees with that repo, that repo wins.

Package-local documentation policy lives in [`docs.jsonl`](docs.jsonl). In
short: graphrefly-rs owns Rust rustdoc, examples, crate docs, development docs,
release notes, docs.rs output, and Rust package-local checks. Shared
graphrefly.dev docs, guide records, public blog architecture, protocol authority,
and dashboard/control views live in `~/src/graphrefly` under D563.

## Current Rust Shape

The active workspace members are:

| Path | Role |
|---|---|
| `crates/graphrefly` | The clean-slate Rust package, with lib crate name `graphrefly` and Cargo package `graphrefly-rs`. |
| `crates/graphrefly-bindings-py` | Native binding foundation for Python host integration. |

The old port-model crate directories and old JS/WASM binding directories have
been deleted from the active tree. They are historical material only; use git
history for archaeology.

The current Rust crate is self-contained per D32. It implements the sync
wave-protocol substrate, graph-layer Rust API, per-language operators/sources,
composition and diagnostics helpers, reactive data structures, app-infra
bundles, environment/resilience adapters, and wire-bridge helpers without
depending on a TypeScript substrate or reviving structural `Impl` parity.

## Authority Order

| Concern | Authority |
|---|---|
| Protocol rules | `~/src/graphrefly/spec/rules.jsonl` |
| Decisions | `~/src/graphrefly/decisions/decisions.jsonl` |
| Sequencing | `~/src/graphrefly/plan/*.jsonl` |
| Conformance | `~/src/graphrefly/spec/conformance.jsonl` |
| Formal model | `~/src/graphrefly/formal/*.tla` |
| Rust docs policy | `docs/docs.jsonl` |
| Public Rust API reference | rustdoc generated from exported crate items |

Legacy markdown such as `GRAPHREFLY-SPEC.md` or `COMPOSITION-GUIDE*.md` in the
authority repo is migration/reference material unless extracted into the current
JSONL authority records.

## Rust Floor

- A graph is a single-thread causal/concurrency domain (D22). The actor model is
  historical; the current core is sync and single-thread.
- Cross-language/package structure follows D32: each package is self-contained;
  cross-runtime collaboration goes through a coarse wire bridge.
- Cross-language parity is behavioral conformance (D24), not structural `Impl`
  surface mirroring.
- Operators, sources, inspection helpers, and package ergonomics are
  per-language surfaces unless the protocol/conformance records say otherwise.
- No `unsafe`; the crate root forbids unsafe code.

## Historical Port Material

Old port-era migration notes, frozen API snapshots, audit reports, flowcharts,
and review reports are no longer kept in the active docs tree. Use git history
for archaeology; current guidance lives in this file, [`development.md`](development.md),
[`docs.jsonl`](docs.jsonl), rustdoc, and the language-neutral authority repo.
