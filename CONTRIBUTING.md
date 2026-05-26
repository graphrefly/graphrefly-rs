# Contributing to graphrefly-rs

Contributions welcome. This file is a brief pointer; the substantive setup and authoring conventions live in [`docs/development.md`](docs/development.md), and the spec sources you must honor when contributing live in [`docs/architecture.md`](docs/architecture.md).

## Status before contributing

The **M1–M5 substrate** is implemented; [`@graphrefly/native`](crates/graphrefly-bindings-js/) is the published napi-rs package. **pyo3** (`graphrefly-bindings-py`) and **wasm-bindgen** (`graphrefly-bindings-wasm`) remain scaffolds until M6 / consumer pressure. See [`docs/migration-status.md`](docs/migration-status.md) for the live tracker and [`docs/porting-deferred.md`](docs/porting-deferred.md) for open follow-ons.

High-leverage contribution paths today:
- Closing a [`porting-deferred.md`](docs/porting-deferred.md) item with a parity scenario + napi binding (D196)
- Cross-arm fixes surfaced by `pnpm test:parity` in [graphrefly-ts](https://github.com/graphrefly/graphrefly-ts)
- Audit-dashboard findings in [`docs/audit/`](docs/audit/) (see migration-status open-findings roll-up)

## Quick start

```bash
git clone https://github.com/graphrefly/graphrefly-rs
cd graphrefly-rs

# Install the toolchain (mise reads .mise.toml; rustup honors rust-toolchain.toml)
mise trust && mise install

# Verify the workspace compiles
cargo check --workspace
cargo nextest run --profile ci     # full suite incl. cascade_depth guards
cargo clippy --workspace --all-targets
cargo fmt --all --check
```

For the full developer guide (toolchain alternatives, binding-specific build flows, pre-commit checklist), see [`docs/development.md`](docs/development.md).

## Where the spec lives

The behavior protocol is defined in `~/src/graphrefly` (or `https://github.com/graphrefly/graphrefly`). This repo implements that spec; it does not redefine it. See [`docs/architecture.md`](docs/architecture.md) for the canonical-references map.

Per the project's [single-source-of-truth principle](CLAUDE.md#memory-principles-inherited-from-graphrefly-ts): **never duplicate spec text into this repo.** If spec language is unclear or wrong, file an issue against `graphrefly/graphrefly` and amend it there.

## PR conventions

- Branch from `main`. Open PRs against `main`.
- One logical change per PR. Multi-crate refactors are fine; mixing unrelated concerns is not.
- Commits should pass: `cargo check --workspace`, `cargo nextest run --profile ci` (full suite incl. cascade_depth stack-safety guards), `cargo clippy --workspace --all-targets`, `cargo fmt --all --check`. CI enforces.
- Pre-1.0: **no backward-compatibility shims.** Rename freely; update all call sites in the same PR. See [`CLAUDE.md`](CLAUDE.md) → "Memory principles" for the full discipline.
- Conventional commits style optional but appreciated (`feat:`, `fix:`, `refactor:`, `docs:`, `chore:`).

## Code review pace

This repo's owner reviews on a personal cadence. For non-trivial PRs, please link the relevant SESSION doc or design discussion in graphrefly-ts so context is preserved.

## License

By contributing, you agree your contributions are licensed under MIT OR Apache-2.0 at the discretion of the consumer (Rust crate convention). See [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE).
