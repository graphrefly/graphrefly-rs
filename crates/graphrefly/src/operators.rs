//! Free-standing operator factories (D6/D24/D40).
//!
//! Operators are graph-layer node sugar, not verbs and not conformance parity.
//! Each factory is a Rust-native ctx-body plus the real factory name used by
//! `describe`; graph-bound construction goes through `Graph::init_node`.

use std::marker::PhantomData;
use std::rc::Rc;

use crate::ctx::{Ctx, DepTerminal};
use crate::dispatcher::Dispatcher;
use crate::node::{Core, GraphArena, Node, NodeOpts, Pausable};
use crate::protocol::{AnyValue, Message};

/// A free-standing operator/source definition.
pub struct Operator<T> {
    /// `factory` field for factory.
    pub factory: &'static str,
    /// `body` field for body.
    pub body: Rc<dyn Fn(&Ctx)>,
    /// `opts` field for opts.
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
    /// Creates or computes `new`.
    pub fn new(factory: &'static str, body: impl Fn(&Ctx) + 'static) -> Self {
        Self::with_opts(factory, NodeOpts::default(), body)
    }

    /// Creates or computes `with_opts`.
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
    init_node_in_arena_with_dispatcher(
        op,
        arena,
        crate::dispatcher::default_dispatcher(),
        deps,
        caller_opts,
    )
}

pub(crate) fn init_node_in_arena_with_dispatcher<T: 'static>(
    op: Operator<T>,
    arena: &GraphArena,
    dispatcher: Dispatcher,
    deps: Vec<Core>,
    caller_opts: NodeOpts,
) -> Node<T> {
    let mut opts = merge_node_opts(&op.opts, caller_opts);
    if opts.factory.is_none() {
        opts.factory = Some(op.factory.to_owned());
    }
    Node::derived_opts_in_arena_with_dispatcher(arena, dispatcher, deps, opts, move |ctx| {
        (op.body)(ctx)
    })
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

/// reduce: accumulate the whole source and emit one final value on source COMPLETE.
pub fn reduce<S: 'static, T: Clone + 'static>(
    reducer: impl Fn(T, &S) -> T + 'static,
    seed: T,
) -> Operator<T> {
    Operator::with_opts(
        "reduce",
        NodeOpts {
            complete_when_deps_complete: false,
            terminal_as_real_input: true,
            ..NodeOpts::default()
        },
        move |ctx| {
            let mut acc = ctx
                .state_get::<T>()
                .map(|v| (*v).clone())
                .unwrap_or_else(|| seed.clone());
            for value in ctx.batch::<S>(0) {
                acc = reducer(acc, value.as_ref());
            }
            ctx.state_set(acc.clone());
            if is_complete(ctx.terminal(0)) {
                ctx.down(vec![Message::Data(Rc::new(acc)), Message::Complete]);
            }
        },
    )
}

/// pairwise: emit `(previous, current)` for each consecutive DATA occurrence.
pub fn pairwise<S: Clone + 'static>() -> Operator<(S, S)> {
    Operator::new("pairwise", move |ctx| {
        let mut prev = ctx.state_get::<S>().map(|v| (*v).clone());
        for value in ctx.batch::<S>(0) {
            if let Some(p) = prev.clone() {
                ctx.emit((p, (*value).clone()));
            }
            prev = Some((*value).clone());
        }
        if let Some(value) = prev {
            ctx.state_set(value);
        }
    })
}

/// skip: drop the first `n` DATA occurrences, then pass through.
pub fn skip<S: Clone + 'static>(n: usize) -> Operator<S> {
    Operator::new("skip", move |ctx| {
        let mut count = ctx.state_get::<usize>().map_or(0, |v| *v);
        for value in ctx.batch::<S>(0) {
            if count < n {
                count += 1;
            } else {
                ctx.emit((*value).clone());
            }
        }
        ctx.state_set(count);
    })
}

/// take_while: emit while `pred` holds; first failed value completes without emission.
pub fn take_while<S: Clone + 'static>(pred: impl Fn(&S) -> bool + 'static) -> Operator<S> {
    Operator::new("takeWhile", move |ctx| {
        for value in ctx.batch::<S>(0) {
            if pred(value.as_ref()) {
                ctx.emit((*value).clone());
            } else {
                ctx.down(vec![Message::Complete]);
                return;
            }
        }
    })
}

/// first: emit the first value matching `pred` (or the first value), then COMPLETE.
pub fn first<S: Clone + 'static>(pred: impl Fn(&S) -> bool + 'static) -> Operator<S> {
    Operator::new("first", move |ctx| {
        for value in ctx.batch::<S>(0) {
            if pred(value.as_ref()) {
                ctx.down(vec![
                    Message::Data(Rc::new((*value).clone())),
                    Message::Complete,
                ]);
                return;
            }
        }
    })
}

/// first_any: emit the first value, then COMPLETE.
pub fn first_any<S: Clone + 'static>() -> Operator<S> {
    first(|_: &S| true)
}

/// last: emit the last matching value on source COMPLETE; no match -> bare COMPLETE.
pub fn last<S: Clone + 'static>(pred: impl Fn(&S) -> bool + 'static) -> Operator<S> {
    Operator::with_opts(
        "last",
        NodeOpts {
            complete_when_deps_complete: false,
            terminal_as_real_input: true,
            ..NodeOpts::default()
        },
        move |ctx| {
            for value in ctx.batch::<S>(0) {
                if pred(value.as_ref()) {
                    ctx.state_set((*value).clone());
                }
            }
            if is_complete(ctx.terminal(0)) {
                if let Some(value) = ctx.state_get::<S>() {
                    ctx.down(vec![
                        Message::Data(Rc::new((*value).clone())),
                        Message::Complete,
                    ]);
                } else {
                    ctx.down(vec![Message::Complete]);
                }
            }
        },
    )
}

/// last_any: emit the last value on source COMPLETE.
pub fn last_any<S: Clone + 'static>() -> Operator<S> {
    last(|_: &S| true)
}

/// find: emit the first matching value, then COMPLETE; no match -> bare COMPLETE.
pub fn find<S: Clone + 'static>(pred: impl Fn(&S) -> bool + 'static) -> Operator<S> {
    Operator::with_opts(
        "find",
        NodeOpts {
            complete_when_deps_complete: false,
            terminal_as_real_input: true,
            ..NodeOpts::default()
        },
        move |ctx| {
            for value in ctx.batch::<S>(0) {
                if pred(value.as_ref()) {
                    ctx.down(vec![
                        Message::Data(Rc::new((*value).clone())),
                        Message::Complete,
                    ]);
                    return;
                }
            }
            if is_complete(ctx.terminal(0)) {
                ctx.down(vec![Message::Complete]);
            }
        },
    )
}

/// element_at: emit the zero-based `index` value, then COMPLETE; out of range -> bare COMPLETE.
pub fn element_at<S: Clone + 'static>(index: usize) -> Operator<S> {
    Operator::with_opts(
        "elementAt",
        NodeOpts {
            complete_when_deps_complete: false,
            terminal_as_real_input: true,
            ..NodeOpts::default()
        },
        move |ctx| {
            let mut count = ctx.state_get::<usize>().map_or(0, |v| *v);
            for value in ctx.batch::<S>(0) {
                if count == index {
                    ctx.down(vec![
                        Message::Data(Rc::new((*value).clone())),
                        Message::Complete,
                    ]);
                    return;
                }
                count += 1;
            }
            ctx.state_set(count);
            if is_complete(ctx.terminal(0)) {
                ctx.down(vec![Message::Complete]);
            }
        },
    )
}

/// tap: run a side-effect for each DATA occurrence and pass values through unchanged.
pub fn tap<S: Clone + 'static>(f: impl Fn(&S) + 'static) -> Operator<S> {
    Operator::new("tap", move |ctx| {
        for value in ctx.batch::<S>(0) {
            f(value.as_ref());
            ctx.emit((*value).clone());
        }
    })
}

/// on_first_data: run a side-effect exactly once, then pass all values through.
pub fn on_first_data<S: Clone + 'static>(f: impl Fn(&S) + 'static) -> Operator<S> {
    on_first_data_where(f, |_| true)
}

/// on_first_data_where: run a side-effect once on the first value satisfying `where_pred`.
pub fn on_first_data_where<S: Clone + 'static>(
    f: impl Fn(&S) + 'static,
    where_pred: impl Fn(&S) -> bool + 'static,
) -> Operator<S> {
    Operator::new("onFirstData", move |ctx| {
        let mut fired = ctx.state_get::<bool>().is_some_and(|v| *v);
        for value in ctx.batch::<S>(0) {
            if !fired && where_pred(value.as_ref()) {
                fired = true;
                f(value.as_ref());
            }
            ctx.emit((*value).clone());
        }
        ctx.state_set(fired);
    })
}

/// tap_first: alias-shaped Rust helper for [`on_first_data`].
pub fn tap_first<S: Clone + 'static>(f: impl Fn(&S) + 'static) -> Operator<S> {
    on_first_data(f)
}

#[derive(Clone)]
struct SettleState<S> {
    last: Option<S>,
    quiet: usize,
    waves: usize,
    done: bool,
}

/// settle_by: forward DATA and COMPLETE after `quiet_waves` quiet waves or `max_waves`.
pub fn settle_by<S: Clone + 'static>(
    quiet_waves: usize,
    max_waves: Option<usize>,
    equals: impl Fn(&S, &S) -> bool + 'static,
) -> Operator<S> {
    assert!(quiet_waves > 0, "settle: quiet_waves must be positive");
    if let Some(max) = max_waves {
        assert!(max > 0, "settle: max_waves must be positive when set");
    }
    Operator::new("settle", move |ctx| {
        let mut st = ctx
            .state_get::<SettleState<S>>()
            .map(|v| (*v).clone())
            .unwrap_or(SettleState {
                last: None,
                quiet: 0,
                waves: 0,
                done: false,
            });
        if st.done {
            return;
        }
        st.waves += 1;
        let mut saw_change = false;
        for value in ctx.batch::<S>(0) {
            let next = (*value).clone();
            if st.last.as_ref().is_none_or(|prev| !equals(prev, &next)) {
                saw_change = true;
            }
            st.last = Some(next.clone());
            ctx.emit(next);
        }
        st.quiet = if saw_change { 0 } else { st.quiet + 1 };
        let settled = st.last.is_some() && st.quiet >= quiet_waves;
        let exhausted = max_waves.is_some_and(|max| st.waves >= max);
        if settled || exhausted {
            st.done = true;
            ctx.state_set(st);
            ctx.down(vec![Message::Complete]);
        } else {
            ctx.state_set(st);
        }
    })
}

/// settle: [`settle_by`] using `PartialEq`.
pub fn settle<S: Clone + PartialEq + 'static>(
    quiet_waves: usize,
    max_waves: Option<usize>,
) -> Operator<S> {
    settle_by(quiet_waves, max_waves, |a, b| a == b)
}

/// rescue: absorb upstream ERROR and replace it with `recover(error_message)`.
pub fn rescue<S: Clone + 'static>(recover: impl Fn(&str) -> S + 'static) -> Operator<S> {
    Operator::with_opts(
        "rescue",
        NodeOpts {
            error_when_deps_error: false,
            complete_when_deps_complete: false,
            terminal_as_real_input: true,
            ..NodeOpts::default()
        },
        move |ctx| {
            for value in ctx.batch::<S>(0) {
                ctx.emit((*value).clone());
            }
            match ctx.terminal(0) {
                Some(DepTerminal::Complete) => ctx.down(vec![Message::Complete]),
                Some(DepTerminal::Error(error)) => ctx.emit(recover(error.as_ref())),
                None => {}
            }
        },
    )
}

/// catch_error: alias-shaped Rust helper for [`rescue`].
pub fn catch_error<S: Clone + 'static>(recover: impl Fn(&str) -> S + 'static) -> Operator<S> {
    rescue(recover)
}

/// valve: forward source DATA while boolean control dep is true.
pub fn valve<S: Clone + 'static>() -> Operator<S> {
    Operator::with_opts(
        "valve",
        NodeOpts {
            partial: true,
            complete_when_deps_complete: false,
            terminal_as_real_input: true,
            ..NodeOpts::default()
        },
        move |ctx| {
            match ctx.terminal(0) {
                Some(DepTerminal::Complete) => {
                    ctx.down(vec![Message::Complete]);
                    return;
                }
                Some(DepTerminal::Error(error)) => {
                    ctx.down(vec![Message::Error(error.to_string().into())]);
                    return;
                }
                None => {}
            }

            let control = ctx.data::<bool>(1).is_some_and(|v| *v);
            if !control {
                return;
            }

            let source_batch = ctx.batch::<S>(0);
            if source_batch.is_empty() {
                let control_fired = !ctx.batch::<bool>(1).is_empty();
                if control_fired {
                    if let Some(latest) = ctx.data::<S>(0) {
                        ctx.emit((*latest).clone());
                    }
                }
            } else {
                for value in source_batch {
                    ctx.emit((*value).clone());
                }
            }
        },
    )
}

fn is_complete(terminal: Option<&DepTerminal>) -> bool {
    matches!(terminal, Some(DepTerminal::Complete))
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
        && opts.versioning.is_none()
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
    if caller.versioning.is_some() {
        opts.versioning = caller.versioning;
    }
    opts
}
