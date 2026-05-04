# Development guide

How to set up the toolchain, build, test, lint, and ship contributions to graphrefly-rs.

This guide is the substantive companion to [`../CONTRIBUTING.md`](../CONTRIBUTING.md). For the spec sources you must honor while developing, see [`architecture.md`](architecture.md). For Claude session orientation, see [`../CLAUDE.md`](../CLAUDE.md).

## Toolchain

The Rust version is pinned in two places:
- [`rust-toolchain.toml`](../rust-toolchain.toml) — honored automatically by [rustup](https://rustup.rs)
- [`.mise.toml`](../.mise.toml) — honored by [mise](https://mise.jdx.dev)

Both pin the same version. Pick whichever toolchain manager you already use.

### Option 1: mise (recommended)

```bash
mise trust          # one-time, after cloning (mise refuses untrusted configs)
mise install        # installs the pinned Rust version
eval "$(mise env)"  # adds cargo/rustc to PATH for this shell
```

### Option 2: rustup

```bash
# One-time install of rustup itself
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# rustup automatically reads rust-toolchain.toml when you cd into the repo
cd ~/src/graphrefly-rs
rustc --version  # should match the pinned version
```

### Option 3: Homebrew / system package manager

Discouraged. System Rust is often outdated or version-mismatched against the pin. Use mise or rustup.

### Verify

```bash
rustc --version    # must match rust-toolchain.toml
cargo --version
```

## Build, test, lint

The default-members of the workspace exclude binding crates so daily commands don't pull in napi-rs, pyo3, or wasm-bindgen toolchains.

```bash
# Type-check (fast; no codegen)
cargo check --workspace

# Build everything (default-members; bindings excluded)
cargo build

# Run all tests
cargo test --workspace

# Run tests for one crate
cargo test -p graphrefly-core

# Lint (clippy::pedantic warn-by-default — see CLAUDE.md "Rust-specific invariants")
cargo clippy --workspace --all-targets

# Format (rustfmt; CI fails if --check would diff)
cargo fmt --all --check     # check only
cargo fmt --all             # apply

# Apply clippy auto-fixes (review the diff before committing)
cargo clippy --workspace --all-targets --fix
```

### Concurrency permutation testing (loom)

The Core dispatcher's per-subgraph `parking_lot::ReentrantMutex` and the version-counter atomics warrant model-checked concurrency tests. Loom permutes thread interleavings to catch races the runtime might miss.

```bash
cargo test -p graphrefly-core --features loom-checked
```

Loom tests are slower than regular tests; CI runs them on every PR but locally you can skip unless touching dispatch internals.

### Benchmarks

```bash
cargo bench -p graphrefly-core   # criterion benches
```

Benches land alongside M1 implementation (not in the scaffold).

### Supply-chain audit

Once-per-machine: `cargo install cargo-deny`. Then:

```bash
cargo deny check          # license + advisory + version-unification audit
cargo deny check licenses
cargo deny check advisories
```

Config lives in [`../deny.toml`](../deny.toml). CI runs this on every PR.

## Bindings — separate toolchains

The three binding crates (`graphrefly-bindings-{js,py,wasm}`) require their own non-Cargo build tools. They are NOT compiled by `cargo build` or `cargo test --workspace`. Build them via their toolchains:

### JavaScript (napi-rs)

```bash
# One-time toolchain setup
mise install node@22
mise install pnpm

cd crates/graphrefly-bindings-js
pnpm install
pnpm build              # produces .node binary in ./
pnpm test               # JS-side smoke tests
```

CI publishes per-platform binaries to npm under `@graphrefly/lite`, `@graphrefly/standard`, `@graphrefly/full`. See [`SESSION-rust-port-architecture.md`](https://github.com/graphrefly/graphrefly-ts/blob/main/archive/docs/SESSION-rust-port-architecture.md) Part 9 for the multi-distribution model.

### Python (pyo3 + maturin)

```bash
# One-time toolchain setup
mise install python@3.13
mise install uv
uv tool install maturin

cd crates/graphrefly-bindings-py
maturin develop --release   # builds + installs into the active Python env
pytest                      # PY-side smoke tests
```

CI publishes to PyPI as `graphrefly` with extras (`pip install graphrefly[full]`).

### WASM (wasm-bindgen + wasm-pack)

```bash
# One-time toolchain setup
cargo install wasm-pack

cd crates/graphrefly-bindings-wasm
wasm-pack build --target web      # for browser
wasm-pack build --target nodejs   # for Node / edge runtimes
wasm-pack test --node             # WASM-side smoke tests
```

CI publishes to npm as `@graphrefly/lite-wasm`, `@graphrefly/standard-wasm`.

## Pre-commit checklist

Before opening a PR, run all of:

```bash
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets   # zero warnings
cargo fmt --all --check                  # zero diffs
cargo deny check                         # if cargo-deny installed
```

CI enforces all five. Failing locally is faster to fix than failing in CI.

## Adding a new dependency

1. Add to `[workspace.dependencies]` in the root [`Cargo.toml`](../Cargo.toml) — never directly in a member crate's `Cargo.toml`. This is how the workspace keeps versions unified.
2. In the member crate's `Cargo.toml`, reference via `workspace = true`:
   ```toml
   [dependencies]
   new_crate = { workspace = true, features = ["specific-feature"] }
   ```
3. If the crate has multiple feature-gated paths, add a feature to the member crate's `[features]` block:
   ```toml
   [features]
   default = ["..."]
   new-capability = ["dep:new_crate"]
   ```
4. Run `cargo deny check licenses` to confirm the new transitive deps are allowed.

## Bumping a dep version

1. Edit `[workspace.dependencies]` in the root `Cargo.toml`.
2. `cargo update` to refresh `Cargo.lock`.
3. `cargo check --workspace` to confirm no API breaks.
4. `cargo test --workspace` to confirm behavior intact.
5. Commit `Cargo.toml` + `Cargo.lock` together.

For the Rust toolchain itself, edit BOTH `rust-toolchain.toml` AND `.mise.toml` (and the `rust-version` field in `Cargo.toml`'s `[workspace.package]`). Three files, same version.

## When tests break

The Core implementation must verify against `~/src/graphrefly/formal/wave_protocol.tla` (TLA+ spec) and the [property tests in graphrefly-ts](https://github.com/graphrefly/graphrefly-ts/tree/main/src/__tests__/properties) (fast-check). If a Rust test fails:

1. **Read [COMPOSITION-GUIDE-PROTOCOL.md](https://github.com/graphrefly/graphrefly/blob/main/COMPOSITION-GUIDE-PROTOCOL.md) FIRST** before improvising fixes. Per the project memory rule: skipping leads to null-guard violations and layer-breaking optimizations.
2. Isolate the failing test (`cargo test -p <crate> --lib <test_name> -- --exact --nocapture`).
3. Run the equivalent TLC scenario MC against `wave_protocol.tla` to see if the bug exists in the spec interpretation.
4. Decide: protocol bug (file against `graphrefly/graphrefly`) vs Rust impl bug (fix here).

## Working with multiple crates

When changing the Core protocol surface, multiple crates often need coordinated updates:

```bash
# Find every crate that depends on the changed type
cargo tree -i graphrefly-core

# Make a single PR that touches all affected crates
```

Per the project's "no autonomous decisions" memory rule: surface conflicts file-by-file, don't silently rewrite multiple files in one pass without raising flags.
