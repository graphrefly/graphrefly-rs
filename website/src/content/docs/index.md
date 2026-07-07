---
title: "Rust Docs"
description: "Package-local documentation for graphrefly-rs."
tableOfContents: false
slug: /
---

Rust APIs, examples, recipes, integrations, and generated rustdoc for building
graph-driven reactive systems with the `graphrefly` crate.

## Start here

- **[Quick Start](/quickstart/)** — build and observe a tiny graph in Rust.
- **[API Reference](/api/)** — generated rustdoc for the public crate surface.
- **[Examples](/examples/)** — package-local Rust entry points.
- **[Recipes](/recipes/)** — composition and runtime-boundary notes.
- **[Integrations](/integrations/)** — native host, IO, and wire bridge surfaces.
- **[Release](/release/)** — crates.io, docs.rs, and Pages publishing notes.

## Install

```toml
[dependencies]
graphrefly-rs = "0.0.1"
```

```rust
use std::cell::RefCell;
use std::rc::Rc;

use graphrefly::{graph, Message, Values};

fn last_or_prev(values: &Values<'_>, i: usize) -> Option<Rc<i32>> {
    values
        .batches::<i32>(i)
        .last()
        .and_then(|wave| wave.last().cloned())
        .or_else(|| values.prev::<i32>(i))
}

let graph = graph();
let count = graph.state(1);
let doubled = graph.derived(vec![count.erased()], |values| {
    Some(*last_or_prev(values, 0)? * 2)
});

let seen = Rc::new(RefCell::new(Vec::new()));
let observed = seen.clone();
let unsubscribe = doubled.subscribe(move |msg| {
    if let Message::Data(value) = msg {
        observed.borrow_mut().push(*value);
    }
});

count.set(2);
unsubscribe();
```

## Ownership

This site is generated in `graphrefly-rs` and deployed to
`https://rs.graphrefly.dev/`. The shared `graphrefly.dev` site links here for
Rust-specific API details and keeps cross-language concepts in the shared docs.
