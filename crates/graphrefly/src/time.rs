//! Wall-clock time operator factories (D52/D111/B53).
//!
//! Operators stay graph-layer sugar over visible helper deps. Raw time work lives
//! only in [`crate::sources::timer`]; helper timer nodes are created in the owning
//! node's graph arena/dispatcher through a crate-private `Ctx` initializer.

use std::cell::RefCell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;

use crate::ctx::{Ctx, DepTerminal, WaveData};
use crate::higher_order::{merge_map_with_ctx, switch_map_with_ctx};
use crate::node::{Core, Node, NodeOpts};
use crate::operators::Operator;
use crate::protocol::Message;
use crate::sources::{interval, timer};

type Body = Rc<dyn Fn(&Ctx)>;
type BodyCell = Rc<RefCell<Option<Body>>>;
type AuditProject<S> = Rc<dyn Fn(&Ctx, &S) -> Core>;

fn delayed_value<S: Clone + 'static>(ctx: &Ctx, value: S, ms: u64) -> Node<S> {
    let tick = ctx.init_node_in_scope(timer(ms), vec![]);
    ctx.init_node_in_scope(
        Operator::with_opts(
            "delayedValue",
            crate::node::NodeOpts {
                partial: true,
                error_when_deps_error: false,
                complete_when_deps_complete: false,
                terminal_as_real_input: true,
                ..crate::node::NodeOpts::default()
            },
            move |ctx| {
                if let Some(DepTerminal::Error(error)) = ctx.terminal(0) {
                    ctx.down(vec![Message::Error(error.to_string().into())]);
                } else if dep_has_data(ctx, 0) || is_complete(ctx.terminal(0)) {
                    ctx.down(vec![
                        Message::Data(Rc::new(value.clone())),
                        Message::Complete,
                    ]);
                }
            },
        ),
        vec![tick.erased()],
    )
}

/// delay: shift every DATA value by `ms`, preserving every occurrence.
pub fn delay<S: Clone + 'static>(ms: u64) -> Operator<S> {
    merge_map_with_ctx("delay", move |ctx, value: &S| {
        delayed_value(ctx, value.clone(), ms)
    })
}

/// debounce: emit the latest value after `ms` of quiet.
pub fn debounce<S: Clone + 'static>(ms: u64) -> Operator<S> {
    switch_map_with_ctx("debounce", move |ctx, value: &S| {
        delayed_value(ctx, value.clone(), ms)
    })
}

/// debounce_time: RxJS-shaped alias of [`debounce`] with its own factory name.
pub fn debounce_time<S: Clone + 'static>(ms: u64) -> Operator<S> {
    switch_map_with_ctx("debounceTime", move |ctx, value: &S| {
        delayed_value(ctx, value.clone(), ms)
    })
}

/// throttle: leading-edge throttle. Emit immediately, then ignore source DATA for `ms`.
pub fn throttle<S: Clone + 'static>(ms: u64) -> Operator<S> {
    throttle_with_factory("throttle", ms)
}

/// throttle_time: RxJS-shaped alias of [`throttle`] with its own factory name.
pub fn throttle_time<S: Clone + 'static>(ms: u64) -> Operator<S> {
    throttle_with_factory("throttleTime", ms)
}

#[derive(Clone)]
struct AuditState<S> {
    window_open: bool,
    latest: Option<S>,
    notifier: Option<Core>,
    suppress_next_notifier: bool,
}

#[derive(Clone)]
struct TimeoutState {
    timer: Option<Core>,
}

#[derive(Clone)]
struct BufferTimeState<S> {
    buffer: Vec<S>,
    interval: Option<Core>,
}

#[derive(Clone)]
struct ThrottleState {
    timer: Option<Core>,
    source_done: bool,
}

fn throttle_with_factory<S: Clone + 'static>(factory: &'static str, ms: u64) -> Operator<S> {
    let body_cell: BodyCell = Rc::new(RefCell::new(None));
    let body_cell_for_body = body_cell.clone();
    let body: Body = Rc::new(move |ctx| {
        run_throttle_body::<S>(ctx, ms, &body_cell_for_body);
    });
    *body_cell.borrow_mut() = Some(body.clone());

    Operator::with_opts(
        factory,
        crate::node::NodeOpts {
            partial: true,
            error_when_deps_error: false,
            complete_when_deps_complete: false,
            terminal_as_real_input: true,
            ..crate::node::NodeOpts::default()
        },
        move |ctx| body(ctx),
    )
}

fn run_throttle_body<S: Clone + 'static>(ctx: &Ctx, ms: u64, body_cell: &BodyCell) {
    ctx.state_persist(true);
    let mut st = ctx
        .state_get::<ThrottleState>()
        .map(|v| (*v).clone())
        .unwrap_or(ThrottleState {
            timer: None,
            source_done: false,
        });

    let timer_dep = if st.source_done { 0 } else { 1 };
    let mut to_remove = Vec::new();
    let mut to_add = None;
    let mut set_to_timer_only = false;
    let mut complete = false;

    if st.timer.is_some() && (dep_has_data(ctx, timer_dep) || is_complete(ctx.terminal(timer_dep)))
    {
        if let Some(timer) = st.timer.take() {
            to_remove.push(timer);
        }
        if st.source_done {
            complete = true;
        }
    }

    if let Some(error) = first_error(ctx) {
        if let Some(timer) = st.timer.take() {
            to_remove.push(timer);
        }
        ctx.state_set(ThrottleState {
            timer: None,
            source_done: true,
        });
        for timer in to_remove {
            ctx.rewire_next_unsubscribe_dep(timer, rewire_body(body_cell));
        }
        ctx.down(vec![Message::Error(error.into())]);
        return;
    }

    if !st.source_done {
        let source_batch = ctx.batch::<S>(0);
        if st.timer.is_none() {
            if let Some(value) = source_batch.first() {
                ctx.down(vec![Message::Data(Rc::new((**value).clone()))]);
                let timer = ctx.init_node_in_scope(timer(ms), vec![]).erased();
                st.timer = Some(timer.clone());
                to_add = Some(timer);
            }
        }

        if is_complete(ctx.terminal(0)) {
            st.source_done = true;
            if st.timer.is_some() {
                set_to_timer_only = true;
            } else {
                complete = true;
            }
        }
    }

    ctx.state_set(st.clone());
    for timer in to_remove {
        ctx.rewire_next_unsubscribe_dep(timer, rewire_body(body_cell));
    }
    if set_to_timer_only {
        let timer = st
            .timer
            .clone()
            .expect("source_done with live throttle timer");
        ctx.rewire_next_replace_deps(vec![timer], rewire_body(body_cell));
    } else if let Some(timer) = to_add {
        ctx.rewire_next_subscribe_dep(timer, rewire_body(body_cell));
    }
    if complete {
        ctx.down(vec![Message::Complete]);
    }
}

/// audit: value-triggered trailing throttle. The selector returns the duration notifier node.
pub fn audit<S: Clone + 'static, N: 'static>(
    duration_selector: impl Fn(&S) -> Node<N> + 'static,
) -> Operator<S> {
    let project: AuditProject<S> = Rc::new(move |_ctx, value| duration_selector(value).erased());
    audit_with_ctx("audit", project)
}

/// audit_time: `audit` specialized to a graph-scoped `timer(ms)` duration.
pub fn audit_time<S: Clone + 'static>(ms: u64) -> Operator<S> {
    let project: AuditProject<S> =
        Rc::new(move |ctx, _value| ctx.init_node_in_scope(timer(ms), vec![]).erased());
    audit_with_ctx("auditTime", project)
}

fn audit_with_ctx<S: Clone + 'static>(
    factory: &'static str,
    duration_selector: AuditProject<S>,
) -> Operator<S> {
    let body_cell: BodyCell = Rc::new(RefCell::new(None));
    let body_cell_for_body = body_cell.clone();
    let body: Body = Rc::new(move |ctx| {
        run_audit_body(ctx, &duration_selector, &body_cell_for_body);
    });
    *body_cell.borrow_mut() = Some(body.clone());

    Operator::with_opts(
        factory,
        crate::node::NodeOpts {
            partial: true,
            error_when_deps_error: false,
            complete_when_deps_complete: false,
            terminal_as_real_input: true,
            ..crate::node::NodeOpts::default()
        },
        move |ctx| body(ctx),
    )
}

fn run_audit_body<S: Clone + 'static>(
    ctx: &Ctx,
    duration_selector: &AuditProject<S>,
    body_cell: &BodyCell,
) {
    let mut st = ctx
        .state_get::<AuditState<S>>()
        .map(|v| (*v).clone())
        .unwrap_or(AuditState {
            window_open: false,
            latest: None,
            notifier: None,
            suppress_next_notifier: false,
        });

    let source_batch = ctx.batch::<S>(0);
    let notifier_signaled = dep_has_data(ctx, 1) || is_complete(ctx.terminal(1));
    let notifier_fired = st.window_open && notifier_signaled && !st.suppress_next_notifier;
    let fired_notifier = notifier_fired.then(|| st.notifier.clone()).flatten();
    if st.window_open && notifier_signaled && st.suppress_next_notifier {
        st.suppress_next_notifier = false;
    }
    if notifier_fired {
        if let Some(value) = st.latest.clone() {
            ctx.down(vec![Message::Data(Rc::new(value))]);
        }
        let old = st.notifier.take();
        st.window_open = false;
        st.latest = None;
        st.suppress_next_notifier = false;
        if let Some(old) = old {
            ctx.rewire_next_unsubscribe_dep(old, rewire_body(body_cell));
        }
    }

    if let Some(error) = first_error(ctx) {
        if let Some(old) = st.notifier.take() {
            ctx.rewire_next_unsubscribe_dep(old, rewire_body(body_cell));
        }
        ctx.state_set(AuditState::<S> {
            window_open: false,
            latest: None,
            notifier: None,
            suppress_next_notifier: false,
        });
        ctx.down(vec![Message::Error(error.into())]);
        return;
    }

    for value in &source_batch {
        st.latest = Some((**value).clone());
    }

    if is_complete(ctx.terminal(0)) {
        if let Some(value) = st.latest.clone() {
            ctx.down(vec![Message::Data(Rc::new(value))]);
        }
        if let Some(old) = st.notifier.take() {
            ctx.rewire_next_unsubscribe_dep(old, rewire_body(body_cell));
        }
        ctx.state_set(AuditState::<S> {
            window_open: false,
            latest: None,
            notifier: None,
            suppress_next_notifier: false,
        });
        ctx.down(vec![Message::Complete]);
        return;
    }

    if !st.window_open && !source_batch.is_empty() {
        if let Some(value) = st.latest.as_ref() {
            match catch_unwind(AssertUnwindSafe(|| duration_selector(ctx, value))) {
                Ok(notifier) => {
                    st.window_open = true;
                    st.suppress_next_notifier = fired_notifier
                        .as_ref()
                        .is_some_and(|old| old.ptr_eq(&notifier));
                    st.notifier = Some(notifier.clone());
                    ctx.rewire_next_subscribe_dep(notifier, rewire_body(body_cell));
                }
                Err(payload) => {
                    ctx.state_set(AuditState::<S> {
                        window_open: false,
                        latest: None,
                        notifier: None,
                        suppress_next_notifier: false,
                    });
                    ctx.down(vec![Message::Error(panic_payload(payload).into())]);
                    return;
                }
            }
        }
    }

    ctx.state_set(st);
}

/// timeout: subscribe-armed idle watchdog. The helper is a free node constructor,
/// not an operator factory or graph method (D114).
pub fn timeout<S: Clone + 'static>(source: &Node<S>, ms: u64) -> Node<S> {
    let source_core = source.erased();
    let initial_timer = scoped_timer(&source_core, ms).erased();
    let body_cell: BodyCell = Rc::new(RefCell::new(None));
    let body_cell_for_body = body_cell.clone();
    let initial_for_body = initial_timer.clone();
    let source_for_body = source_core.clone();
    let body: Body = Rc::new(move |ctx| {
        run_timeout_body::<S>(
            ctx,
            ms,
            &initial_for_body,
            &source_for_body,
            &body_cell_for_body,
        );
    });
    *body_cell.borrow_mut() = Some(body.clone());

    let node = crate::operators::init_node_in_arena_with_dispatcher(
        Operator::with_opts("timeout", time_helper_opts(), move |ctx| {
            body(ctx);
        }),
        &source_core.arena(),
        source_core.dispatcher(),
        vec![initial_timer, source_core.clone()],
        NodeOpts::default(),
    );
    node.erased()
        .set_local_async_driver(source_core.local_async_driver());
    node
}

fn run_timeout_body<S: Clone + 'static>(
    ctx: &Ctx,
    ms: u64,
    initial_timer: &Core,
    source: &Core,
    body_cell: &BodyCell,
) {
    let mut st = ctx
        .state_get::<TimeoutState>()
        .map(|v| (*v).clone())
        .unwrap_or_else(|| TimeoutState {
            timer: Some(initial_timer.clone()),
        });

    let source_batch = ctx.batch::<S>(1);
    for value in &source_batch {
        ctx.down(vec![Message::Data(Rc::new((**value).clone()))]);
    }

    if is_complete(ctx.terminal(1)) {
        if let Some(timer) = st.timer.take() {
            ctx.rewire_next_unsubscribe_dep(timer, rewire_body(body_cell));
        }
        ctx.state_set(TimeoutState { timer: None });
        ctx.down(vec![Message::Complete]);
        return;
    }

    if let Some(DepTerminal::Error(error)) = ctx.terminal(1) {
        if let Some(timer) = st.timer.take() {
            ctx.rewire_next_unsubscribe_dep(timer, rewire_body(body_cell));
        }
        ctx.state_set(TimeoutState { timer: None });
        ctx.down(vec![Message::Error(error.to_string().into())]);
        return;
    }

    if !source_batch.is_empty() {
        let old = st.timer.take();
        let next = ctx.init_node_in_scope(timer(ms), vec![]).erased();
        st.timer = Some(next.clone());
        ctx.state_set(st);
        if old.is_some() {
            ctx.rewire_next_replace_deps(vec![next, source.clone()], rewire_body(body_cell));
        } else {
            ctx.rewire_next_subscribe_dep(next, rewire_body(body_cell));
        }
        return;
    }

    if let Some(DepTerminal::Error(error)) = ctx.terminal(0) {
        ctx.state_set(TimeoutState { timer: None });
        ctx.down(vec![Message::Error(error.to_string().into())]);
        return;
    }

    if dep_has_data(ctx, 0) || is_complete(ctx.terminal(0)) {
        ctx.state_set(TimeoutState { timer: None });
        ctx.down(vec![Message::Error(
            format!("timeout: no value within {ms}ms").into(),
        )]);
        return;
    }

    ctx.state_set(st);
}

/// buffer_time: subscribe-armed interval buffer helper (D114).
pub fn buffer_time<S: Clone + 'static>(source: &Node<S>, ms: u64) -> Node<Vec<S>> {
    let source_core = source.erased();
    let interval_node = scoped_interval(&source_core, ms).erased();
    let body_cell: BodyCell = Rc::new(RefCell::new(None));
    let body_cell_for_body = body_cell.clone();
    let interval_for_body = interval_node.clone();
    let body: Body = Rc::new(move |ctx| {
        run_buffer_time_body::<S>(ctx, &interval_for_body, &body_cell_for_body);
    });
    *body_cell.borrow_mut() = Some(body.clone());

    let node = crate::operators::init_node_in_arena_with_dispatcher(
        Operator::with_opts("bufferTime", time_helper_opts(), move |ctx| {
            body(ctx);
        }),
        &source_core.arena(),
        source_core.dispatcher(),
        vec![interval_node, source_core.clone()],
        NodeOpts::default(),
    );
    node.erased()
        .set_local_async_driver(source_core.local_async_driver());
    node
}

fn run_buffer_time_body<S: Clone + 'static>(
    ctx: &Ctx,
    initial_interval: &Core,
    body_cell: &BodyCell,
) {
    let mut st = ctx
        .state_get::<BufferTimeState<S>>()
        .map(|v| (*v).clone())
        .unwrap_or_else(|| BufferTimeState {
            buffer: Vec::new(),
            interval: Some(initial_interval.clone()),
        });

    for value in ctx.batch::<S>(1) {
        st.buffer.push((*value).clone());
    }

    if is_complete(ctx.terminal(1)) {
        if !st.buffer.is_empty() {
            ctx.down(vec![Message::Data(Rc::new(st.buffer.clone()))]);
        }
        if let Some(interval) = st.interval.take() {
            ctx.rewire_next_unsubscribe_dep(interval, rewire_body(body_cell));
        }
        ctx.state_set(BufferTimeState::<S> {
            buffer: Vec::new(),
            interval: None,
        });
        ctx.down(vec![Message::Complete]);
        return;
    }

    if let Some(DepTerminal::Error(error)) = ctx.terminal(1) {
        if let Some(interval) = st.interval.take() {
            ctx.rewire_next_unsubscribe_dep(interval, rewire_body(body_cell));
        }
        ctx.state_set(BufferTimeState::<S> {
            buffer: Vec::new(),
            interval: None,
        });
        ctx.down(vec![Message::Error(error.to_string().into())]);
        return;
    }

    if let Some(DepTerminal::Error(error)) = ctx.terminal(0) {
        ctx.state_set(BufferTimeState::<S> {
            buffer: Vec::new(),
            interval: None,
        });
        ctx.down(vec![Message::Error(error.to_string().into())]);
        return;
    }

    if dep_has_data(ctx, 0) {
        ctx.down(vec![Message::Data(Rc::new(st.buffer.clone()))]);
        st.buffer.clear();
    }

    ctx.state_set(st);
}

fn scoped_timer(anchor: &Core, ms: u64) -> Node<u64> {
    scoped_time_source(anchor, timer(ms))
}

fn scoped_interval(anchor: &Core, ms: u64) -> Node<u64> {
    scoped_time_source(anchor, interval(ms))
}

fn scoped_time_source(anchor: &Core, op: Operator<u64>) -> Node<u64> {
    let node = crate::operators::init_node_in_arena_with_dispatcher(
        op,
        &anchor.arena(),
        anchor.dispatcher(),
        vec![],
        NodeOpts::default(),
    );
    node.erased()
        .set_local_async_driver(anchor.local_async_driver());
    node
}

fn time_helper_opts() -> NodeOpts {
    NodeOpts {
        partial: true,
        complete_when_deps_complete: false,
        error_when_deps_error: false,
        terminal_as_real_input: true,
        ..NodeOpts::default()
    }
}

fn rewire_body(body_cell: &BodyCell) -> impl Fn(&Ctx) + 'static {
    let body_cell = body_cell.clone();
    move |ctx| {
        let body = body_cell
            .borrow()
            .as_ref()
            .expect("audit body initialized")
            .clone();
        body(ctx);
    }
}

fn dep_has_data(ctx: &Ctx, dep: usize) -> bool {
    ctx.wave_data().get(dep).is_some_and(|waves| {
        waves
            .iter()
            .flatten()
            .any(|v| matches!(v, WaveData::Data(_)))
    })
}

fn is_complete(term: Option<&DepTerminal>) -> bool {
    matches!(term, Some(DepTerminal::Complete))
}

fn first_error(ctx: &Ctx) -> Option<String> {
    (0..ctx.dep_len()).find_map(|i| match ctx.terminal(i) {
        Some(DepTerminal::Error(error)) => Some(error.to_string()),
        _ => None,
    })
}

fn panic_payload(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_owned()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "time operator panicked".to_owned()
    }
}
