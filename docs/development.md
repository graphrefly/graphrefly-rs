# Development Guide

How to set up the toolchain, build, test, lint, and maintain Rust package-local
documentation for graphrefly-rs.

For the authority map, see [`architecture.md`](architecture.md). For the docs
ownership boundary, see [`docs.jsonl`](docs.jsonl). For agent/session
orientation, see [`../AGENTS.md`](../AGENTS.md).

## Toolchain

Rust is pinned in two places:

- [`rust-toolchain.toml`](../rust-toolchain.toml) for rustup.
- [`.mise.toml`](../.mise.toml) for mise.

Use mise in agent shells unless you have a reason not to:

```bash
mise trust
mise install
mise exec -- cargo --version
```

Direct rustup also works from the repo root:

```bash
rustc --version
cargo --version
```

## Build, Test, Lint

Preferred commands:

```bash
mise exec -- cargo test -p graphrefly-rs
mise exec -- cargo build -p graphrefly-rs
mise exec -- cargo clippy -p graphrefly-rs --all-targets
mise exec -- cargo fmt --all --check
```

Optional feature checks:

```bash
mise exec -- cargo test -p graphrefly-rs --features tokio-http,tokio-websocket
mise exec -- cargo test -p graphrefly-rs --features tokio-worker
```

Focused CSP-10 checks:

```bash
mise exec -- cargo test -p graphrefly-rs --test acceptance public_crate_root_d566
mise exec -- cargo test -p graphrefly-rs --test csp10_recipes
mise exec -- cargo run -p graphrefly-rs --example csp10_baseline_app_infra
```

## Documentation Work

Rust package docs live here:

- Rustdoc comments on exported crate items.
- `README.md`.
- Rust examples and package-local development docs under `docs/`.
- Crate release notes and docs.rs/rustdoc generation material.
- Package-local docs checks recorded in `docs/docs.jsonl`.

Shared graphrefly.dev pages, shared guide records, public blog storage/rendering,
protocol authority, and dashboard/control views live in `~/src/graphrefly`
under D563. Do not copy rustdoc output into the shared authority repo by hand,
and do not copy shared public docs back into this repo as mirrors.

When editing rustdoc examples, prefer examples that compile or are covered by
tests. If an example is illustrative only, mark it as such in the rustdoc block.
Run `mise exec -- cargo fmt --all --check` after touching Rust files, including
rustdoc comments.

For broad missing-doc coverage, use the mechanical generator instead of
hand-editing large comment batches:

```bash
scripts/generate-rustdoc-comments.py --apply
mise exec -- cargo fmt --all
```

The generator runs rustdoc with `-D missing_docs`, parses the resulting
diagnostics, and inserts neutral template comments only for items rustdoc reports.
It preserves existing docs and is a no-op when the public surface is already
covered. Use `scripts/generate-rustdoc-comments.py --check-stale` to combine the
missing-doc gate with the active-source stale-phrase scan used in CI.

## Docs.rs And Releases

The publishable crate is `crates/graphrefly` (`package = "graphrefly-rs"`,
`lib = "graphrefly"`). Its `package.metadata.docs.rs` enables all features so
feature-gated public APIs appear in generated docs.rs/rustdoc output.

The first crates.io release is a local bootstrap because crates.io trusted
publishing must be configured after the crate exists:

```bash
mise exec -- cargo publish -p graphrefly-rs --dry-run
mise exec -- cargo publish -p graphrefly-rs
```

After that, configure crates.io Trusted Publishing for
`graphrefly/graphrefly-rs` and `.github/workflows/release.yml`. Future releases
are driven by Conventional Commits: push `feat:`, `fix:`, `perf:`, or breaking
change commits to `main`; release-plz opens/updates a release PR, and merging
that PR publishes after CI succeeds.

Before a crate release:

1. Confirm package-local release notes or changelog material is current.
2. Run `mise exec -- cargo fmt --all --check`.
3. Run `mise exec -- cargo test -p graphrefly-rs`.
4. Run `mise exec -- cargo build -p graphrefly-rs`.
5. Run `mise exec -- cargo clippy -p graphrefly-rs --all-targets`.
6. For docs-affecting feature work, check docs with all features:
   `mise exec -- cargo doc -p graphrefly-rs --all-features --no-deps`.
7. For package-site changes, build the Starlight artifact:
   `pnpm --dir website build`.

The GitHub Pages docs deployment workflow lives at
`.github/workflows/pages.yml`. It builds the package-local Astro/Starlight site,
generates rustdoc with `RUSTDOCFLAGS="-D warnings" cargo doc -p graphrefly-rs
--all-features --no-deps`, copies rustdoc into `/api/rustdoc/`, writes the
`rs.graphrefly.dev` CNAME, and uploads `website/dist`.

## Adding A Dependency

1. Add the dependency to `[workspace.dependencies]` in the root
   [`Cargo.toml`](../Cargo.toml).
2. Reference it from the member crate with `workspace = true`.
3. Add or adjust feature flags if the dependency is optional.
4. Run the focused build/test/lint commands above.

## When Tests Break

First determine whether the failure is a Rust implementation bug or a protocol
authority question:

1. Check the governing rule in `~/src/graphrefly/spec/rules.jsonl`.
2. Check the governing decision in
   `~/src/graphrefly/decisions/decisions.jsonl`.
3. If the rule is clear, fix the Rust implementation or docs to match it.
4. If the rule is silent or contradictory, stop and route through the
   spec/decision process instead of choosing silently.
