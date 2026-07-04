# `graphrefly` Current Shape

This file is a package-local orientation note for the active Rust crate. It is
not a protocol spec and does not preserve the retired port-era migration trail.
The language-neutral authority lives in `~/src/graphrefly`; when this file and
that repo disagree, that repo wins.

## Package Boundary

`crates/graphrefly` is the publishable Cargo package `graphrefly-rs` and the lib
crate `graphrefly`. It is self-contained per D32 and does not depend on a
TypeScript substrate.

Rust package-local material lives here:

- rustdoc comments on exported Rust APIs
- crate examples and package-local development docs
- docs.rs and GitHub Pages rustdoc output
- Rust release and verification notes

Shared public concepts, package delegation routes, protocol rules, decisions,
conformance scenarios, and the public website shell live in `~/src/graphrefly`.

## Active Surface

The active crate includes:

- sync wave-protocol substrate: protocol, node, dispatcher, ctx, batch, rewire,
  pull/routed-up, terminal and async-boundary behavior
- graph-layer Rust API: `Graph`, `graph`, graph-owned node construction helpers,
  describe, observe, profile, restore, and topology helpers
- per-language operators and sources, implemented as graph-layer node sugar
- composition and diagnostics helpers
- reactive collections and storage helpers
- app-infra helpers for messaging, work queues, scheduled readiness, CQRS,
  process recipes, environment adapters, resilience, and wire-bridge helpers
- native binding foundation for Python host integration

Operators, sources, inspection helpers, and package ergonomics are Rust package
APIs. They are not protocol verbs and are not structural parity obligations.
Cross-runtime consistency is behavioral conformance from `~/src/graphrefly`.

## Guardrails

- No `unsafe`; the crate root uses `#![forbid(unsafe_code)]`.
- Public Rust APIs are covered by `#![deny(missing_docs)]`.
- The wave core remains synchronous; async work enters through source, driver,
  pool, or bridge boundaries.
- A graph is a single-thread concurrency domain. The actor model is retired.
- Data moves through protocol messages; package docs must not describe hidden
  polling, imperative triggers, or structural `Impl` parity as current behavior.

## Verification

Preferred package checks:

```bash
mise exec -- cargo fmt --all --check
RUSTDOCFLAGS="-D warnings" mise exec -- cargo doc -p graphrefly-rs --all-features --no-deps
mise exec -- cargo clippy -p graphrefly-rs --all-targets --all-features -- -D warnings
mise exec -- cargo test -p graphrefly-rs --all-features
```

For broad public rustdoc coverage, use:

```bash
scripts/generate-rustdoc-comments.py --apply
mise exec -- cargo fmt --all
```

The generator is conservative: rustdoc missing-doc diagnostics remain the source
of truth, existing docs are preserved, and generated comments are neutral
templates.
