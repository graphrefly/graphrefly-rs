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

## Graph inspection

GraphReFly keeps topology visible. `describe` gives each node's id, kind,
current dependencies and whether it is quiet; values are read from the nodes.
Name graph nodes when you want stable ids:

```rust
use graphrefly::{graph_opts, DescribeKind, GraphNodeOpts, GraphOptions};

let graph = graph_opts(GraphOptions::named("counter"));
let count = graph.state_opts(1i32, GraphNodeOpts::named("count"));
let doubled = graph.derived_opts::<i32, _>(
    vec![count.erased()],
    |values| Some(*values.value::<i32>(0)? * 2),
    GraphNodeOpts::named("doubled"),
);

// describe gives exactly { id, kind, deps, quiet } per node, in creation order.
let nodes = graph.describe();
assert_eq!(nodes[1].id, "doubled");
assert_eq!(nodes[1].kind, DescribeKind::Node);
assert_eq!(nodes[1].deps, vec!["count".to_owned()]);

// observe hands each delivered array to the sink as { id, msgs }; it returns the stop.
let stop = graph.observe(&["doubled"], |event| {
    assert_eq!(event.id, "doubled");
});
count.set(2);
assert_eq!(doubled.cache(), Some(4));
stop();
```

## Async boundary

The wave-protocol core is synchronous. Async work enters through explicit source,
driver, pool, or wire-bridge boundaries rather than a hidden runtime.
