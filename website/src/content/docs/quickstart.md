---
title: "Quick Start"
description: "Build and observe a small GraphReFly graph from Rust."
---

Install the Rust crate:

```toml
[dependencies]
graphrefly-rs = "0.0.1"
```

The library crate name is `graphrefly`:

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
let count = graph.state(0);
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

count.set(1);
count.set(2);
unsubscribe();

assert_eq!(seen.borrow().last().copied(), Some(4));
assert_eq!(doubled.cache(), Some(4));
```

## Graph inspection

GraphReFly keeps topology visible. Name graph nodes when you want stable
inspection output:

```rust
use graphrefly::{graph_opts, GraphNodeOpts, GraphOptions};

let graph = graph_opts(GraphOptions::named("counter"));
let count = graph.state_opts(1, GraphNodeOpts::named("count"));
let doubled = graph.derived_opts(
    vec![count.erased()],
    |values| values.prev::<i32>(0).map(|value| *value * 2),
    GraphNodeOpts::named("doubled"),
);

let snapshot = graph.describe();
assert!(snapshot.nodes.iter().any(|node| node.name.as_deref() == Some("doubled")));
drop(doubled);
```

## Async boundary

The wave-protocol core is synchronous. Async work enters through explicit source,
driver, pool, or wire-bridge boundaries rather than a hidden runtime.
