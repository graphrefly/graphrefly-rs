# GraphReFly Rust

The Rust graph engine and native foundation used by Python host bindings.
Cargo package `graphrefly-rs`, library crate `graphrefly`.

## Where things are

| Path | Contents |
|---|---|
| [crates/graphrefly/](crates/graphrefly/) | Rust package, source and examples |
| [crates/graphrefly-bindings-py/](crates/graphrefly-bindings-py/) | Python native binding foundation |
| [docs/](docs/README.md) | Documentation map, architecture and development |
| [website/](website/) | Package-local site and rustdoc integration |

## Where things stand

- **Work:** [local work ledger](plan/work.jsonl) and [root locator](../graphrefly/authority/work-ledgers.jsonl).
- **Rules, drafts and decisions:** [documentation map](docs/README.md).
- **Package/module map:** [CLEAN-SLATE.md](crates/graphrefly/CLEAN-SLATE.md).
- **API:** [package docs](https://rs.graphrefly.dev/) and [development/release guidance](docs/development.md).

## Start locally

```sh
mise trust
mise install
mise exec -- cargo build -p graphrefly-rs
```

| Command | What it does |
|---|---|
| `mise exec -- cargo test -p graphrefly-rs` | Rust package tests |
| `mise exec -- cargo build -p graphrefly-rs` | Package build |
| `mise exec -- cargo clippy -p graphrefly-rs --all-targets` | Rust lint |
| `mise exec -- cargo fmt --all --check` | Formatting check |

[Development](docs/development.md) owns optional features, docs and release checks.
[Agent entrypoint](AGENTS.md). Dual-licensed under MIT OR Apache-2.0.
