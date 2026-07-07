---
title: "Release"
description: "GraphReFly Rust package release and docs publishing policy."
---

GraphReFly Rust publishes the `graphrefly-rs` Cargo package whose library crate
is `graphrefly`.

## Version

The crate version lives in `crates/graphrefly/Cargo.toml`.

## API docs

Rust API docs are package-local rustdoc:

```bash
RUSTDOCFLAGS="-D warnings" mise exec -- cargo doc -p graphrefly-rs --all-features --no-deps
```

The Starlight site build generates the same rustdoc and copies it to
`/api/rustdoc/`.

## Publish

Before publishing, run:

```bash
mise exec -- cargo fmt --all --check
RUSTDOCFLAGS="-D warnings" mise exec -- cargo doc -p graphrefly-rs --all-features --no-deps
mise exec -- cargo clippy -p graphrefly-rs --all-targets --all-features -- -D warnings
mise exec -- cargo test -p graphrefly-rs --all-features
pnpm --dir website build
```

Shared website and blog publishing remains owned by `~/src/graphrefly`. This
package deploys its own Starlight output to `https://rs.graphrefly.dev/`.
