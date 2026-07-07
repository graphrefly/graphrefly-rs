# graphrefly-rs

Rust implementation of GraphReFly's clean-slate reactive graph engine and reusable
application-infrastructure surface.

GraphReFly is a reactive universal reduction layer: high fan-in/fan-out,
information reduction, then push. The language-neutral authority for protocol,
decisions, conformance, formal models, and phase sequencing lives in
`~/src/graphrefly`; this repository implements the Rust package.

## Current Shape

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
- Native host binding foundation for Python under `crates/graphrefly-bindings-py`.

## Documentation Boundary

This repo owns Rust package-local documentation: rustdoc on exported crate
items, examples, crate README material, development docs, crate release notes,
docs.rs output, and Rust package-local docs checks. The package-local policy is
recorded in [`docs/docs.jsonl`](docs/docs.jsonl), with D32 and D563 as the
governing decisions.

Shared public docs for graphrefly.dev, shared guide records, public blog
storage/rendering, protocol authority, and maintainer dashboard/control views
belong to `~/src/graphrefly`. Those shared docs may link to Rust package docs,
but this repo must not hand-maintain mirrors of shared docs, TypeScript docs,
Python docs, public blog posts, or generated dashboard state.

## CSP-10 App-Infra Baseline

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

## Setup

Toolchains are managed with `mise`:

```bash
mise trust
mise install
```

Rust is also pinned by `rust-toolchain.toml` for direct `rustup` users.

## Build And Test

Preferred agent/developer commands:

```bash
mise exec -- cargo test -p graphrefly-rs
mise exec -- cargo clippy -p graphrefly-rs --all-targets
mise exec -- cargo build -p graphrefly-rs
```

Focused CSP-10 checks:

```bash
mise exec -- cargo test -p graphrefly-rs --test acceptance public_crate_root_d566
mise exec -- cargo test -p graphrefly-rs --test csp10_recipes
mise exec -- cargo run -p graphrefly-rs --example csp10_baseline_app_infra
```

Optional runtime-driver features:

```bash
mise exec -- cargo test -p graphrefly-rs --features tokio-http,tokio-websocket
mise exec -- cargo test -p graphrefly-rs --features tokio-worker
```

## API Docs And Release Notes

Rust API docs are package-local rustdoc. The public docs gate is:

```bash
RUSTDOCFLAGS="-D warnings" mise exec -- cargo doc -p graphrefly-rs --all-features --no-deps
```

If the public surface grows and rustdoc reports missing docs, use the mechanical
comment generator instead of hand-editing large comment batches:

```bash
scripts/generate-rustdoc-comments.py --apply
mise exec -- cargo fmt --all
```

The generator is idempotent and uses rustdoc's own `missing_docs` diagnostics as
the source of truth; when the crate is already covered it exits without editing.

Docs.rs builds use the crate metadata in `crates/graphrefly/Cargo.toml`. GitHub
Pages builds the package-local Astro/Starlight site in `website/`, then copies
generated rustdoc into `/api/rustdoc/` and publishes the artifact to
`https://rs.graphrefly.dev/`. Package release notes and crate-specific
development notes stay in this repo, while shared docs/blog content stays in
`~/src/graphrefly`.

## Authority And Guardrails

When code and docs disagree with `~/src/graphrefly`, the authority repo wins.
Important clean-slate guardrails:

- The wave-protocol core is synchronous; async lives only at source, pool, driver,
  or wire-bridge boundaries.
- All node functions go through the dispatcher.
- A graph is a single-thread causal/concurrency domain.
- Cross-runtime parity is behavioral conformance, not structural symbol parity.
- Environment/resilience adapters expose graph-visible attempts, status, errors,
  lifecycle, and bounded retry material.
- Scheduled readiness projects eligibility only; domain projectors own claiming,
  materialization, cancellation, and execution.

## License

Dual-licensed under MIT OR Apache-2.0, at your option.
