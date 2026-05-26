# graphrefly-rs

Rust implementation of the [GraphReFly](https://graphrefly.dev) reactive graph protocol.

**Status:** Substrate-complete (2026-05-25). M1–M5 crates are implemented and gate-green; [`@graphrefly/native`](crates/graphrefly-bindings-js/) ships the async JS substrate (`createNativeImpl()`). Milestone tracker: [`docs/migration-status.md`](docs/migration-status.md). Migration plan: [`SESSION-rust-port-architecture.md`](https://github.com/graphrefly/graphrefly-ts/blob/main/archive/docs/SESSION-rust-port-architecture.md) in graphrefly-ts.

## Architectural premise

The Core operates entirely on opaque `HandleId` integers. Per-language SDK harnesses (napi-rs for JS, pyo3 for Python, wasm-bindgen for browser/edge) own the value-to-handle registry. Equals-substitution under `equals: 'identity'` is a u64 compare with zero FFI; user-fn invocation is the only mandatory boundary crossing per fn fire.

This split is what makes the Rust port viable without losing language ergonomics. See `~/src/graphrefly/formal/wave_protocol.tla` (canonical TLA+ spec) and `~/src/graphrefly-ts/docs/research/handle-protocol-audit-input.md` for the validated cleaving plane.

## Workspace layout

```
crates/
├── graphrefly-core/              # M1: dispatcher, message tiers, batch, wave engine
├── graphrefly-graph/             # M2: Graph container, snapshot, content addressing (CIDs)
├── graphrefly-operators/         # M3: built-in operator node types
├── graphrefly-storage/           # M4: tiers + Node-side persistence (redb-backed)
├── graphrefly-structures/        # M5: reactiveMap, reactiveList, reactiveLog, reactiveIndex
├── graphrefly-bindings-js/       # M1+: napi-rs JS bindings (cdylib)
├── graphrefly-bindings-py/       # M6: pyo3 Python bindings (cdylib, abi3)
└── graphrefly-bindings-wasm/     # WASM target (browser, Cloudflare Workers, Deno, Bun)
```

## Distribution

**Published today:** `@graphrefly/native` (single Node-only package; per-platform `.node` binaries via napi-rs OIDC trusted publishing). All M1–M5 substrate features (core, graph, operators, storage, structures) are baked into one binary; the cargo `lite` / `standard` / `full` feature gates in `crates/graphrefly-bindings-js/Cargo.toml` exist for future split-bundle builds but no `@graphrefly/lite|standard|full` package ships yet.

**Future bundle splits (deferred per D196 consumer-pressure gate):**

| Variant | npm package | Approx size | Use case |
|---|---|---:|---|
| lite | `@graphrefly/lite` *(not yet published)* | ~400 KB / platform | Tracing injection, instrumentation, edge runtimes (via WASM) |
| standard | `@graphrefly/standard` *(not yet published)* | ~1.4 MB / platform | Typical agent harnesses |
| full | `@graphrefly/full` *(not yet published)* | ~3.5 MB / platform | Heavy server workloads with persistence + structures |
| WASM | `@graphrefly/wasm` *(deferred until a browser-Rust consumer surfaces)* | ~250–900 KB | Edge runtimes, browser |

Python bindings (`graphrefly-bindings-py`, pyo3) are scaffolded but post-1.0 per the migration plan; no PyPI artifact yet.

## Setup

Install the toolchain via [mise](https://mise.jdx.dev) (recommended — pinned in `.mise.toml`):

```bash
mise trust
mise install
```

Or via [rustup](https://rustup.rs) directly (honors `rust-toolchain.toml`):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Verify:

```bash
cargo --version
cargo check --workspace
```

## Build / test commands

```bash
# Build core crates (default-members; bindings excluded)
cargo build

# Test loop (cargo-nextest — install: `cargo install cargo-nextest --locked`
# or prebuilt: `curl -LsSf https://get.nexte.st/latest/mac | tar zxf - -C ~/.cargo/bin`)
cargo nextest run                       # default-members; fast inner loop
cargo nextest run -p graphrefly-core    # only one crate

# Full suite incl. the cascade_depth stack-safety stress tests
# (quarantined from the default loop — see .config/nextest.toml). Run
# before merge / after touching cascade/terminate/teardown/invalidate:
cargo nextest run --profile ci          # == `cargo tc`

# Parallel sessions: use the isolated wrapper so a second run never
# blocks on the shared `target/` build lock:
scripts/dev-test.sh                     # default loop, per-worktree target

# Property-test with concurrency permutations (loom — stays on `cargo
# test`: loom needs the `--cfg loom` build, not a nextest run):
cargo test -p graphrefly-core --features loom-checked

# Lint
cargo clippy --workspace --all-targets

# Format
cargo fmt --all

# Supply-chain audit (requires `cargo install cargo-deny`)
cargo deny check
```

Bindings have their own toolchains (not built by `cargo build`):

```bash
# JS bindings (requires Node 22+ and pnpm)
cd crates/graphrefly-bindings-js && pnpm build

# Python bindings (requires maturin)
cd crates/graphrefly-bindings-py && maturin develop --release

# WASM bindings (requires wasm-pack)
cd crates/graphrefly-bindings-wasm && wasm-pack build
```

## Spec sources

- `~/src/graphrefly/GRAPHREFLY-SPEC.md` — protocol spec (canonical)
- `~/src/graphrefly/COMPOSITION-GUIDE.md` + `COMPOSITION-GUIDE-{PROTOCOL,GRAPH,PATTERNS,SOLUTIONS}.md` — composition guides (single + per-layer splits)
- `~/src/graphrefly/formal/wave_protocol.tla` + `wave_protocol_MC.tla` (+ `_bufferall`, `_custom_equals`, `_equals_false` variants) — TLA+ spec; this workspace's invariants must verify against the same model
- `~/src/graphrefly-ts/docs/research/handle-protocol-audit-input.md` — handle-protocol cleaving rationale (companion to the canonical TLA+ spec)
- `~/src/graphrefly-ts/docs/cross-track-ledger.md` — single source of truth for any `Impl`-contract widening that couples this workspace to graphrefly-ts

## License

Dual-licensed under [MIT](LICENSE-MIT) OR [Apache-2.0](LICENSE-APACHE), at your option.
