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
use crate::node::{Core, Node};
use crate::operators::Operator;
use crate::protocol::Message;
use crate::sources::timer;

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
}

#[derive(Clone)]
struct ThrottleState {
    timer: Option<Core>,
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
    let mut st = ctx
        .state_get::<ThrottleState>()
        .map(|v| (*v).clone())
        .unwrap_or(ThrottleState { timer: None });

    if st.timer.is_some() && (dep_has_data(ctx, 1) || is_complete(ctx.terminal(1))) {
        if let Some(timer) = st.timer.take() {
            ctx.rewire_next_remove(timer, rewire_body(body_cell));
        }
    }

    if let Some(error) = first_error(ctx) {
        if let Some(timer) = st.timer.take() {
            ctx.rewire_next_remove(timer, rewire_body(body_cell));
        }
        ctx.state_set(ThrottleState { timer: None });
        ctx.down(vec![Message::Error(error.into())]);
        return;
    }

    let source_batch = ctx.batch::<S>(0);
    if st.timer.is_none() {
        if let Some(value) = source_batch.first() {
            ctx.down(vec![Message::Data(Rc::new((**value).clone()))]);
            let timer = ctx.init_node_in_scope(timer(ms), vec![]).erased();
            st.timer = Some(timer.clone());
            ctx.rewire_next_add(timer, rewire_body(body_cell));
        }
    }

    if is_complete(ctx.terminal(0)) {
        if let Some(timer) = st.timer.take() {
            ctx.rewire_next_remove(timer, rewire_body(body_cell));
        }
        ctx.state_set(ThrottleState { timer: None });
        ctx.down(vec![Message::Complete]);
        return;
    }

    ctx.state_set(st);
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
        });

    for value in ctx.batch::<S>(0) {
        st.latest = Some((*value).clone());
    }

    let notifier_fired = st.window_open && (dep_has_data(ctx, 1) || is_complete(ctx.terminal(1)));
    if notifier_fired {
        if let Some(value) = st.latest.clone() {
            ctx.down(vec![Message::Data(Rc::new(value))]);
        }
        let old = st.notifier.take();
        st.window_open = false;
        st.latest = None;
        if let Some(old) = old {
            ctx.rewire_next_remove(old, rewire_body(body_cell));
        }
    }

    if let Some(error) = first_error(ctx) {
        if let Some(old) = st.notifier.take() {
            ctx.rewire_next_remove(old, rewire_body(body_cell));
        }
        ctx.state_set(AuditState::<S> {
            window_open: false,
            latest: None,
            notifier: None,
        });
        ctx.down(vec![Message::Error(error.into())]);
        return;
    }

    if is_complete(ctx.terminal(0)) {
        if st.window_open {
            if let Some(value) = st.latest.clone() {
                ctx.down(vec![Message::Data(Rc::new(value))]);
            }
        }
        if let Some(old) = st.notifier.take() {
            ctx.rewire_next_remove(old, rewire_body(body_cell));
        }
        ctx.state_set(AuditState::<S> {
            window_open: false,
            latest: None,
            notifier: None,
        });
        ctx.down(vec![Message::Complete]);
        return;
    }

    if !st.window_open && !ctx.batch::<S>(0).is_empty() {
        if let Some(value) = st.latest.as_ref() {
            match catch_unwind(AssertUnwindSafe(|| duration_selector(ctx, value))) {
                Ok(notifier) => {
                    st.window_open = true;
                    st.notifier = Some(notifier.clone());
                    ctx.rewire_next_add(notifier, rewire_body(body_cell));
                }
                Err(payload) => {
                    ctx.state_set(AuditState::<S> {
                        window_open: false,
                        latest: None,
                        notifier: None,
                    });
                    ctx.down(vec![Message::Error(panic_payload(payload).into())]);
                    return;
                }
            }
        }
    }

    ctx.state_set(st);
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
