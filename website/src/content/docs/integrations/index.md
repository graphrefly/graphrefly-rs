---
title: "Integrations"
description: "Rust native host, IO, and bridge integration surfaces."
---

Rust is the native shared engine and reusable graph-infrastructure library for
Python and future non-TypeScript host packages. Host-language packages own their
idiomatic public facade, value and lifetime mapping, exception policy, and
runtime adapters.

## Python native binding foundation

The active binding crate is `crates/graphrefly-bindings-py`. It exposes an
opaque native foundation over the Rust graph engine while Python keeps ownership
of the public host facade.

## Optional runtime features

```bash
mise exec -- cargo test -p graphrefly-rs --features tokio-http,tokio-websocket
mise exec -- cargo test -p graphrefly-rs --features tokio-worker
```

## Cross-graph transport

Wire bridge helpers keep cross-runtime graph collaboration coarse-grained. Remote
nodes, ordinary same-wave deps, and host callbacks do not cross the boundary.
