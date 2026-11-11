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

`crates/graphrefly-bindings-py` is not a workspace member: under graphrefly:D800
Python writes its own kernel and, until that branch merges, builds against
graphrefly-rs `cf02fc9`. The crate gets no new work and is removed once Python no
longer imports `graphrefly._native`.

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
| Decisions | `~/src/graphrefly/authority/ledgers.jsonl` → uniquely resolved owner ledger |
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

## Package capability catalog

The following package descriptions moved from README under B142. For delivery
state and evidence, use the map and owning work instead of inferring current
verification from this catalog.


The active crate is `crates/graphrefly` (`lib` name `graphrefly`, package
`graphrefly-rs`). The old port-model crate directories and old JS/WASM binding
directories have been deleted from the active tree; use git history for
archaeology.

The clean-slate crate includes:

- Sync wave-protocol substrate: protocol, node, dispatcher, ctx, batch, rewire.
- Graph-layer Rust API: `graph`, state/producer/derived/effect/mount helpers,
  describe/observe/profile, operators, sources, and passive storage helpers.
- Composition and diagnostics helpers such as `pipe`, `stratify`, and
  `topology_diff`, all derived from declared graph topology or `describe()`
  snapshots.
- Reactive data structures and views: `reactive_list`, `reactive_log`,
  `reactive_index`, `reactive_map`, pull-driven snapshots, and graph-visible
  `ReactiveView` helper nodes.
- CSP-10 baseline app-infra: messaging, work queue, scheduled readiness,
  CQRS/process recipes, graph-visible environment adapters, and resilience
  policy/status bundles.

### CSP-10 package surface

The baseline app-infra surface is intentionally reusable and graph-visible. It
does not add protocol tiers/messages, hidden schedulers, provider runtimes, or
product-specific WorkItem/Canvas/agent semantics.

Useful entry points:

- `graphrefly::pipe`, `graphrefly::stratify`, and
  `graphrefly::topology_diff` for Rust-native composition and snapshot
  diagnostics.
- `graphrefly::reactive_list`, `graphrefly::reactive_log`,
  `graphrefly::reactive_index`, and `graphrefly::reactive_map` for collection
  deltas, lazy snapshots, and retained views.
- `graphrefly::first_sync_value_from`, `graphrefly::single_sync_value_from`,
  `graphrefly::from_http`, `graphrefly::from_sse`, and the graph-owned
  `LocalHttpStreamDriver` boundary for source/IO ergonomics without a hidden
  runtime.
- `graphrefly::message_bus`, `graphrefly::to_topic`, topic/catalog/cursor facts.
- `graphrefly::TopicMessage`, `graphrefly::JsonSchema`,
  `graphrefly::STANDARD_TOPICS`, and explicit
  `graphrefly::validate_topic_message_payload` helpers for passive topic
  vocabulary. The bus does not auto-validate schemas or own topic auth,
  expiration, compatibility, registry, storage, or provider behavior.
- `graphrefly::work_queue` plus `graphrefly::work_queue_scheduled_readiness_projector`,
  `graphrefly::work_queue_readiness_handoff_projector`, and
  `graphrefly::work_queue_lease_expiration_command_projector`.
- `graphrefly::scheduled_readiness_projector` with canonical
  `subject_refs` + `ready_at_ms` schedule facts.
- Focused recipe modules:
  `graphrefly::cqrs::messaging`, `graphrefly::cqrs::work_queue`,
  `graphrefly::process::messaging`, and `graphrefly::process::work_queue`.
- `graphrefly::EnvironmentDrivers`, `graphrefly::to_http`,
  `graphrefly::to_process`, `graphrefly::to_websocket`, and
  `graphrefly::websocket_session` for graph-local environment boundaries.
- `graphrefly::BackoffPolicy`, `graphrefly::RetryPolicy`,
  `graphrefly::retry_status_node`, `graphrefly::breaker_status_node`,
  `graphrefly::rate_limit_bundle`, and `graphrefly::timeout_bundle`.

Run the app-infra example:

```bash
mise exec -- cargo run -p graphrefly-rs --example csp10_baseline_app_infra
```

The example wires messaging -> work queue -> scheduled readiness -> CQRS queue
disposition, and includes focused process work-queue recipe composition without
making queue completion domain truth.

Semantic and agentic-memory utilities are also available as passive/domain
facts and graph-visible bundles: lower memory fragments, retrieval, KG reducers,
agentic records, retention, consolidation, context packing, and the D172
persistence sidecar. Real LLM/vector/provider runtimes, ontology policy,
alias/provenance repair, multi-writer conflict repair, provider-specific context
adapters, and vertical solution runtimes remain explicit follow-ups, not hidden
crate behavior.
