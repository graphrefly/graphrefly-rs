---
title: "Integrations"
description: "Rust native host, IO, and bridge integration surfaces."
---

Rust is a self-contained GraphReFly package. Other languages have their own
kernels and talk to a Rust graph through the wire bridge, never in process.

## Python

Python owns its independent kernel (graphrefly:D800).

## Optional runtime features

```bash
mise exec -- cargo test -p graphrefly-rs --features tokio-http,tokio-websocket
mise exec -- cargo test -p graphrefly-rs --features tokio-worker
```

## Cross-graph transport

Wire bridge helpers keep cross-runtime graph collaboration coarse-grained. Remote
nodes, ordinary same-wave deps, and host callbacks do not cross the boundary.
