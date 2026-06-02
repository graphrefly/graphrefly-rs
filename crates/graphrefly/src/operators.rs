//! Free-standing operator factories (D6/D24/D40).
//!
//! Operators are graph-layer node sugar, not verbs and not conformance parity.
//! Each factory is a Rust-native ctx-body plus the real factory name used by
//! `describe`; graph-bound construction goes through `Graph::init_node`.

use std::marker::PhantomData;
use std::rc::Rc;

use crate::ctx::Ctx;
use crate::node::{Core, GraphArena, Node, NodeOpts, Pausable};
use crate::protocol::{AnyValue, Message};

/// A free-standing operator/source definition.
pub struct Operator<T> {
    pub factory: &'static str,
    pub body: Rc<dyn Fn(&Ctx)>,
    pub opts: NodeOpts,
    _t: PhantomData<fn() -> T>,
}

impl<T> Clone for Operator<T> {
    fn clone(&self) -> Self {
        Self {
            factory: self.factory,
            body: self.body.clone(),
            opts: self.opts.clone(),
            _t: PhantomData,
        }
    }
}

impl<T> Operator<T> {
    pub fn new(factory: &'static str, body: impl Fn(&Ctx) + 'static) -> Self {
        Self::with_opts(factory, NodeOpts::default(), body)
    }

    pub fn with_opts(
        factory: &'static str,
        mut opts: NodeOpts,
        body: impl Fn(&Ctx) + 'static,
    ) -> Self {
        if opts.factory.is_none() {
            opts.factory = Some(factory.to_owned());
        }
        Self {
            factory,
            body: Rc::new(body),
            opts,
            _t: PhantomData,
        }
    }
}

/// Instantiate an operator as a bare node; graph-bound callers should prefer
/// `Graph::init_node` so the node is registered for inspection.
pub fn init_node<T: 'static>(op: Operator<T>, deps: Vec<Core>, caller_opts: NodeOpts) -> Node<T> {
    init_node_in_arena(op, &GraphArena::default(), deps, caller_opts)
}

pub(crate) fn init_node_in_arena<T: 'static>(
    op: Operator<T>,
    arena: &GraphArena,
    deps: Vec<Core>,
    caller_opts: NodeOpts,
) -> Node<T> {
    let mut opts = merge_node_opts(&op.opts, caller_opts);
    if opts.factory.is_none() {
        opts.factory = Some(op.factory.to_owned());
    }
    Node::derived_opts_in_arena(arena, deps, opts, move |ctx| (op.body)(ctx))
}

/// map: emit `fn(value)` for every upstream DATA occurrence.
pub fn map<S: 'static, T: 'static>(f: impl Fn(&S) -> T + 'static) -> Operator<T> {
    Operator::new("map", move |ctx| {
        for value in ctx.batch::<S>(0) {
            ctx.emit(f(value.as_ref()));
        }
    })
}

/// filter: forward values matching `pred`; skipped waves settle as undirty RESOLVED.
pub fn filter<S: Clone + 'static>(pred: impl Fn(&S) -> bool + 'static) -> Operator<S> {
    Operator::new("filter", move |ctx| {
        for value in ctx.batch::<S>(0) {
            if pred(value.as_ref()) {
                ctx.emit((*value).clone());
            }
        }
    })
}

/// scan: stateful accumulator, emitting each intermediate accumulator value.
pub fn scan<S: 'static, T: Clone + 'static>(
    reducer: impl Fn(T, &S) -> T + 'static,
    seed: T,
) -> Operator<T> {
    Operator::new("scan", move |ctx| {
        let mut acc = ctx
            .state_get::<T>()
            .map(|v| (*v).clone())
            .unwrap_or_else(|| seed.clone());
        for value in ctx.batch::<S>(0) {
            acc = reducer(acc, value.as_ref());
            ctx.emit(acc.clone());
        }
        ctx.state_set(acc);
    })
}

/// take: emit the first `n` DATA values, then COMPLETE.
pub fn take<S: Clone + 'static>(n: usize) -> Operator<S> {
    Operator::new("take", move |ctx| {
        if n == 0 {
            ctx.down(vec![Message::Complete]);
            return;
        }
        let mut count = ctx.state_get::<usize>().map_or(0, |v| *v);
        if count >= n {
            return;
        }
        for value in ctx.batch::<S>(0) {
            if count >= n {
                break;
            }
            count += 1;
            let out: AnyValue = Rc::new((*value).clone());
            if count >= n {
                ctx.down(vec![Message::Data(out), Message::Complete]);
            } else {
                ctx.down(vec![Message::Data(out)]);
            }
        }
        ctx.state_set(count);
    })
}

/// distinct_until_changed: opt-in dedup at the operator layer (D49).
pub fn distinct_until_changed<S: Clone + 'static>(
    eq: impl Fn(&S, &S) -> bool + 'static,
) -> Operator<S> {
    Operator::new("distinctUntilChanged", move |ctx| {
        let mut last = ctx.state_get::<S>().map(|v| (*v).clone());
        for value in ctx.batch::<S>(0) {
            if last.as_ref().is_some_and(|prev| eq(prev, value.as_ref())) {
                continue;
            }
            last = Some((*value).clone());
            ctx.emit((*value).clone());
        }
        if let Some(value) = last {
            ctx.state_set(value);
        }
    })
}

/// merge: interleave DATA from any dep; first-run gate is partial.
pub fn merge<T: Clone + 'static>() -> Operator<T> {
    Operator::with_opts(
        "merge",
        NodeOpts {
            partial: true,
            ..NodeOpts::default()
        },
        move |ctx| {
            for i in 0..ctx.dep_len() {
                for value in ctx.batch::<T>(i) {
                    ctx.emit((*value).clone());
                }
            }
        },
    )
}

fn is_default_node_opts(opts: &NodeOpts) -> bool {
    opts.factory.is_none()
        && opts.pool == Default::default()
        && opts.pausable == Pausable::True
        && !opts.partial
        && opts.pull_id.is_none()
        && opts.complete_when_deps_complete
        && opts.error_when_deps_error
        && !opts.terminal_as_real_input
}

fn merge_node_opts(base: &NodeOpts, caller: NodeOpts) -> NodeOpts {
    if is_default_node_opts(&caller) {
        return base.clone();
    }
    let default = NodeOpts::default();
    let mut opts = base.clone();
    if caller.factory.is_some() {
        opts.factory = caller.factory;
    }
    if caller.pool != default.pool {
        opts.pool = caller.pool;
    }
    if caller.pausable != default.pausable {
        opts.pausable = caller.pausable;
    }
    if caller.partial {
        opts.partial = true;
    }
    if caller.pull_id.is_some() {
        opts.pull_id = caller.pull_id;
    }
    if caller.complete_when_deps_complete != default.complete_when_deps_complete {
        opts.complete_when_deps_complete = caller.complete_when_deps_complete;
    }
    if caller.error_when_deps_error != default.error_when_deps_error {
        opts.error_when_deps_error = caller.error_when_deps_error;
    }
    if caller.terminal_as_real_input {
        opts.terminal_as_real_input = true;
    }
    opts
}
