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

Public items carry docs where the docs say something; there is no generator of
placeholder comments and no `missing_docs` lint (review §4.2 of the D797
migration removed 3,600 template lines). `scripts/check-stale-phrases.py`, run in
CI, fails when active source names a concept the D797 kernel retired.

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
2. Resolve the governing origin-qualified decision through
   `~/src/graphrefly/authority/ledgers.jsonl` and read its unique owner ledger.
3. If the rule is clear, fix the Rust implementation or docs to match it.
4. If the rule is silent or contradictory, stop and route through the
   spec/decision process instead of choosing silently.

## Rust contributor constraints

Relocated from AGENTS under the user-approved B142 editorial batch. These retain
existing constraints; root rules and owner decisions still govern meaning.


1. **Single-thread = `Rc<RefCell<…>>`, NOT `Arc<Mutex<…>>` (D22).** A graph is one
   causal/concurrency domain; the substrate is `!Send + !Sync`. The actor model and
   per-partition `ReentrantMutex` machinery of the old port are **gone** — they were
   the ~9.5× concurrency tax (memory `project_rust_perf_value_investigation`). Lock the
   `!Send`/`!Sync` boundary with `static_assertions` once types stabilize.
2. **No async runtime in the core (R-invocation@2).** `dispatcher.invoke` is sync. `tokio`
   enters only behind the local async driver and the wire bridge.
3. **No `unsafe`. Anywhere.** `#![forbid(unsafe_code)]` at the crate root. Find a safe
   abstraction; if you truly cannot, escalate to spec-level discussion first.
4. **Error = unknown, single value generic (D31).** `Node<T>` carries one value
   generic; the error channel is `Box<dyn Error>` (`GraphError`). No typed-error
   combinatorial explosion.
5. **No `unwrap()`/`expect()` on user-facing paths.** `thiserror` enums for domain
   errors; `unwrap` only in tests or genuinely-impossible-by-construction paths (with a
   comment).
6. **`#[must_use]` on value-returning public fns; `clippy::pedantic` warn-by-default.**
   Allow per-need with a comment, never silently.
7. **Public ids behind newtypes** (`Handle`, future `NodeId`) — never raw ints.


## Workflow and command observation

Follow [root contributor guidance](../../graphrefly/guide/contribute.jsonl),
[the authority contract](../../graphrefly/authority/README.md), personal governance
and the [local workflow routing](../AGENTS.md). Keep package-local decisions local;
root-origin history is relocation-only. Do not restore retired port models or
structural parity. Surface spec/code conflicts; do not pick new semantics.

Long commands use `scripts/run-logged.sh` and its `<<<RUN-LOGGED:DONE>>>` sentinel.
Run the actual single-crate command through it; do not use the old port-era gate
sequence, background cargo, buffered output pipelines or sleep-based completion
inference. Use mise in agent shells; `~/.cargo/bin/cargo` is the PATH fallback.

[Documentation categories](README.md) locate work, drafts and dated evidence.

## D797 rereview API checks

`Graph::batch` takes `|| -> Result<_, E>`. Returning `Err` from the outer body
aborts all staged work. A nested batch shares that staging; catching its `Err`
inside an outer body that returns `Ok` keeps the accumulated writes.

The minimal graph view is `graph.describe()`; use `graph.find(id)` for status and
values. Optional diagnostics are free functions: `graphrefly::topology::snapshot(&graph)`,
`snapshot_opts`, `observe`, `observe_at`, `profile`, `group`, `group_opts`, `mount`
and `name`. Snapshot edges are derived with `snapshot.edges()` from node dependencies.
The operator names are `combine`, `on_first_data`, `rescue`, `debounce` and `throttle`;
the wire cause type is `WireEdgeCause`.

The shared D808 trace runs with:

```sh
GRAPHREFLY_SPEC_ROOT=../graphrefly/spec cargo run -p graphrefly-rs --example d797_rereview_probe
```

It compares the actual trace against the root `d797-rereview.v1.json` fixture.
CI checks out authority commit `4771691460538cd387197571d930128abdac90a2`
and runs both the behavior probe and its `--reasons` mode. A missing fixture is a
hard error. This shared probe does not replace D687 qualification.

Checkpoint records now require the actual kernel `kind`, the complete `policies`
tuple, and explicit nullable `privateState` / `received` fields. Older incomplete
records are refused; no defaults reconstruct omitted data. Saved policies override
recipe defaults, including custom constructors. Checkpoint dependencies live only
in each node's `deps` / `lifetimeDeps`; there is no separate `edges` field. The
`graphrefly-rs/checkpoint@1` tag stays unchanged, and records carrying old `edges`
or other unknown graph-level fields are refused, including mounted checkpoints.

Restore creates a cold graph; explicitly retain the required runtime nodes before
sending commands. Dependency views are borrowed only for setup/function invocation;
`Ctx::defer()` keeps output, state, rewire and signal capabilities for later work.
