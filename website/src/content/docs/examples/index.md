---
title: "Examples"
description: "Small Rust entry points for graphrefly-rs."
---

Examples stay package-local so runnable Rust code can track the exact crate API.

## CSP-10 baseline app infrastructure

```bash
mise exec -- cargo run -p graphrefly-rs --example csp10_baseline_app_infra
```

The example wires messaging, work queue, scheduled readiness, CQRS queue
disposition, and process work-queue recipe composition without making queue
completion domain truth.

## Focused checks

```bash
mise exec -- cargo test -p graphrefly-rs --test acceptance public_crate_root_d566
mise exec -- cargo test -p graphrefly-rs --test csp10_recipes
```

Exact public signatures live in the generated API Reference.
