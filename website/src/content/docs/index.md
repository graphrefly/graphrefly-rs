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

use graphrefly::{graph, AnyValue, Message};

let graph = graph();
let count = graph.state(0i32);
let doubled = graph.derived::<i32, _>(vec![count.erased()], |values| {
    Some(*values.value::<i32>(0)? * 2)
});

let seen = Rc::new(RefCell::new(Vec::new()));
let observed = seen.clone();
let unsubscribe = doubled.subscribe(move |msgs: &[Message<AnyValue>]| {
    for msg in msgs {
        if let Message::Data(value) = msg {
            observed.borrow_mut().push(*value.downcast_ref::<i32>().unwrap());
        }
    }
});

count.set(1);
count.set(2);
assert_eq!(*seen.borrow(), vec![0, 2, 4]);
assert_eq!(doubled.cache(), Some(4));
unsubscribe();
```

## Ownership

This site is generated in `graphrefly-rs` and deployed to
`https://rs.graphrefly.dev/`. The shared `graphrefly.dev` site links here for
Rust-specific API details and keeps cross-language concepts in the shared docs.
