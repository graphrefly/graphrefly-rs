# graphrefly-rs

Rust implementation of the [GraphReFly](https://graphrefly.dev) reactive graph protocol.

**Status:** Scaffold (2026-05-03). Active implementation is pending the close of Phase 13.6 (rules audit) and Phase 14 (op-log changesets) decision lock-down in [graphrefly-ts](https://github.com/graphrefly/graphrefly-ts). See `archive/docs/SESSION-rust-port-architecture.md` in graphrefly-ts for the full migration plan.

## Architectural premise

The Core operates entirely on opaque `HandleId` integers. Per-language SDK harnesses (napi-rs for JS, pyo3 for Python, wasm-bindgen for browser/edge) own the value-to-handle registry. Equals-substitution under `equals: 'identity'` is a u64 compare with zero FFI; user-fn invocation is the only mandatory boundary crossing per fn fire.

This split is what makes the Rust port viable without losing language ergonomics. See `~/src/graphrefly-ts/docs/research/handle-protocol.tla` and the companion TS prototype at `src/__experiments__/handle-core/` for the validated cleaving plane.

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

## Distribution variants (planned)

Single source tree, multiple build profiles via Cargo features:

| Variant | npm package | Approx size | Use case |
|---|---|---:|---|
| lite | `@graphrefly/lite` | ~400 KB / platform | Tracing injection, instrumentation, edge runtimes (via WASM) |
| standard | `@graphrefly/standard` | ~1.4 MB / platform | Typical agent harnesses |
| full | `@graphrefly/full` | ~3.5 MB / platform | Heavy server workloads with persistence + structures |
| WASM lite/standard | `@graphrefly/lite-wasm`, `@graphrefly/standard-wasm` | ~250 KB / ~900 KB | Edge runtimes, browser |

PyPI uses extras syntax: `pip install graphrefly[full]`.

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

# Test everything
cargo test --workspace

# Test only one crate
cargo test -p graphrefly-core

# Property-test with concurrency permutations
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
- `~/src/graphrefly/COMPOSITION-GUIDE-{PROTOCOL,GRAPH,PATTERNS,SOLUTIONS}.md` — composition guides
- `~/src/graphrefly/formal/wave_protocol.tla` — TLA+ spec; this workspace's invariants must verify against the same model
- `~/src/graphrefly-ts/docs/research/handle-protocol.tla` — handle-protocol refinement of wave_protocol

## License

Dual-licensed under [MIT](LICENSE-MIT) OR [Apache-2.0](LICENSE-APACHE), at your option.
