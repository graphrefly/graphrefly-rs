---
title: "Recipes"
description: "Rust package recipes for graph and runtime boundaries."
---

Recipes describe Rust-specific composition around the clean-slate graph engine.
Cross-language protocol guarantees stay in the shared docs; exact Rust syntax
stays here.

## Keep IO at graph-visible boundaries

Use explicit driver surfaces for host-owned work:

- `EnvironmentDrivers`
- `LocalHttpDriver`
- `LocalHttpStreamDriver`
- `LocalProcessDriver`
- `LocalWebSocketDriver`

These adapters expose attempts, status, lifecycle, errors, and retry facts as
ordinary graph-visible data rather than hidden runtime state.

## Prefer graph-owned composition

Use `pipe`, `stratify`, `topology_diff`, `reactive_list`, `reactive_map`, and the
recipe modules under `cqrs` and `process` when a workflow should remain visible
to `describe()` and diagnostic tools.
