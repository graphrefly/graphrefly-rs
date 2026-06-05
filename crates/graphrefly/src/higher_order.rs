//! Higher-order operator factories (D6/D24/B53).
//!
//! These are graph-layer sugar over the existing R-rewire-deferred substrate:
//! inners are runtime deps added/removed with `ctx.rewire_next_*`, not hidden
//! subscriptions. Projectors must return graph-local nodes.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;

use crate::ctx::{Ctx, DepTerminal, WaveData};
use crate::node::{Core, Node, NodeOpts};
use crate::operators::Operator;
use crate::protocol::Message;

type Body = Rc<dyn Fn(&Ctx)>;
type BodyCell = Rc<RefCell<Option<Body>>>;
type Project<TIn, TOut> = Rc<dyn Fn(&Ctx, &TIn) -> Node<TOut>>;

#[derive(Clone, Copy)]
enum Mode {
    Merge,
    Switch,
    Concat,
    Exhaust,
}

#[derive(Clone)]
struct MapState<TIn> {
    inners: Vec<Core>,
    queue: VecDeque<TIn>,
    source_done: bool,
}

/// switch_map: project each source DATA to an inner node, cancelling the prior live inner.
pub fn switch_map<TIn: Clone + 'static, TOut: 'static>(
    project: impl Fn(&TIn) -> Node<TOut> + 'static,
) -> Operator<TOut> {
    map_operator("switchMap", move |_ctx, value| project(value), Mode::Switch)
}

/// merge_map: project every source DATA to an inner node and merge all live inners.
pub fn merge_map<TIn: Clone + 'static, TOut: 'static>(
    project: impl Fn(&TIn) -> Node<TOut> + 'static,
) -> Operator<TOut> {
    map_operator("mergeMap", move |_ctx, value| project(value), Mode::Merge)
}

/// flat_map: alias-shaped Rust helper for [`merge_map`].
pub fn flat_map<TIn: Clone + 'static, TOut: 'static>(
    project: impl Fn(&TIn) -> Node<TOut> + 'static,
) -> Operator<TOut> {
    map_operator("flatMap", move |_ctx, value| project(value), Mode::Merge)
}

/// concat_map: queue source values and run one projected inner at a time.
pub fn concat_map<TIn: Clone + 'static, TOut: 'static>(
    project: impl Fn(&TIn) -> Node<TOut> + 'static,
) -> Operator<TOut> {
    map_operator("concatMap", move |_ctx, value| project(value), Mode::Concat)
}

/// exhaust_map: project the first source DATA while no inner is live; ignore source DATA while busy.
pub fn exhaust_map<TIn: Clone + 'static, TOut: 'static>(
    project: impl Fn(&TIn) -> Node<TOut> + 'static,
) -> Operator<TOut> {
    map_operator(
        "exhaustMap",
        move |_ctx, value| project(value),
        Mode::Exhaust,
    )
}

pub(crate) fn switch_map_with_ctx<TIn: Clone + 'static, TOut: 'static>(
    factory: &'static str,
    project: impl Fn(&Ctx, &TIn) -> Node<TOut> + 'static,
) -> Operator<TOut> {
    map_operator(factory, project, Mode::Switch)
}

pub(crate) fn merge_map_with_ctx<TIn: Clone + 'static, TOut: 'static>(
    factory: &'static str,
    project: impl Fn(&Ctx, &TIn) -> Node<TOut> + 'static,
) -> Operator<TOut> {
    map_operator(factory, project, Mode::Merge)
}

#[derive(Clone)]
struct RepeatState {
    started: bool,
    round: usize,
    inner: Option<Core>,
}

/// repeat: run a fresh source from `factory` `count` times in sequence.
///
/// The factory must return a fresh node for each round. Reusing the same node is
/// not a repeat in the clean-slate substrate: same-boundary
/// unsubscribe_dep+subscribe_dep is a no-op under D47. D115 keeps the model to
/// ordinary unsubscribe plus a later subscribe, so same-node repeat needs a
/// separate future design.
pub fn repeat<T: 'static>(factory: impl Fn() -> Node<T> + 'static, count: usize) -> Operator<T> {
    assert!(count > 0, "repeat: count must be positive");

    let factory: Rc<dyn Fn() -> Node<T>> = Rc::new(factory);
    let body_cell: BodyCell = Rc::new(RefCell::new(None));
    let body_cell_for_body = body_cell.clone();
    let body: Body = Rc::new(move |ctx| {
        run_repeat_body(ctx, &factory, count, &body_cell_for_body);
    });
    *body_cell.borrow_mut() = Some(body.clone());

    Operator::with_opts(
        "repeat",
        NodeOpts {
            error_when_deps_error: false,
            complete_when_deps_complete: false,
            terminal_as_real_input: true,
            ..NodeOpts::default()
        },
        move |ctx| body(ctx),
    )
}

fn map_operator<TIn: Clone + 'static, TOut: 'static>(
    factory: &'static str,
    project: impl Fn(&Ctx, &TIn) -> Node<TOut> + 'static,
    mode: Mode,
) -> Operator<TOut> {
    let project: Project<TIn, TOut> = Rc::new(project);
    let body_cell: BodyCell = Rc::new(RefCell::new(None));
    let body_cell_for_body = body_cell.clone();
    let body: Body = Rc::new(move |ctx| {
        run_map_body(ctx, &project, mode, &body_cell_for_body);
    });
    *body_cell.borrow_mut() = Some(body.clone());
    Operator::with_opts(
        factory,
        NodeOpts {
            partial: true,
            error_when_deps_error: false,
            complete_when_deps_complete: false,
            terminal_as_real_input: true,
            ..NodeOpts::default()
        },
        move |ctx| body(ctx),
    )
}

fn run_map_body<TIn: Clone + 'static, TOut: 'static>(
    ctx: &Ctx,
    project: &Project<TIn, TOut>,
    mode: Mode,
    body_cell: &BodyCell,
) {
    let mut st = ctx
        .state_get::<MapState<TIn>>()
        .map(|v| (*v).clone())
        .unwrap_or(MapState {
            inners: Vec::new(),
            queue: VecDeque::new(),
            source_done: false,
        });

    for i in 1..ctx.dep_len() {
        forward_data(ctx, i);
    }

    if let Some(error) = first_error_terminal(ctx) {
        cleanup_all_inners(ctx, &mut st, body_cell);
        ctx.state_set(st);
        ctx.down(vec![Message::Error(error.into())]);
        return;
    }

    if is_complete(ctx.terminal(0)) {
        st.source_done = true;
    }

    let mut to_remove = Vec::new();
    let mut survivors = Vec::new();
    for (i, inner) in st.inners.iter().enumerate() {
        if is_complete(ctx.terminal(i + 1)) {
            push_unique(&mut to_remove, inner.clone());
        } else {
            survivors.push(inner.clone());
        }
    }
    st.inners = survivors;

    let mut to_add = Vec::new();
    let source_batch = ctx.batch::<TIn>(0);
    if !source_batch.is_empty() {
        match mode {
            Mode::Switch => {
                let latest = source_batch
                    .last()
                    .expect("source_batch is not empty")
                    .as_ref()
                    .clone();
                let Some(inner) = project_inner(ctx, project, &latest, &mut st, body_cell) else {
                    return;
                };
                for live in &st.inners {
                    if !live.ptr_eq(&inner) {
                        push_unique(&mut to_remove, live.clone());
                    }
                }
                if !contains_core(&st.inners, &inner) {
                    to_add.push(inner.clone());
                }
                st.inners = vec![inner];
            }
            Mode::Merge => {
                for value in source_batch {
                    let Some(inner) =
                        project_inner(ctx, project, value.as_ref(), &mut st, body_cell)
                    else {
                        return;
                    };
                    if !contains_core(&st.inners, &inner) {
                        st.inners.push(inner.clone());
                        to_add.push(inner);
                    }
                }
            }
            Mode::Concat => {
                for value in source_batch {
                    st.queue.push_back(value.as_ref().clone());
                }
            }
            Mode::Exhaust => {
                if st.inners.is_empty() {
                    let first = source_batch
                        .first()
                        .expect("source_batch is not empty")
                        .as_ref()
                        .clone();
                    let Some(inner) = project_inner(ctx, project, &first, &mut st, body_cell)
                    else {
                        return;
                    };
                    st.inners.push(inner.clone());
                    to_add.push(inner);
                }
            }
        }
    }

    if matches!(mode, Mode::Concat) && st.inners.is_empty() && to_add.is_empty() {
        if let Some(value) = st.queue.pop_front() {
            let Some(inner) = project_inner(ctx, project, &value, &mut st, body_cell) else {
                return;
            };
            st.inners.push(inner.clone());
            to_add.push(inner);
        }
    }

    ctx.state_set(st.clone());
    for dep in to_remove {
        ctx.rewire_next_unsubscribe_dep(dep, rewire_body(body_cell));
    }
    for dep in to_add {
        ctx.rewire_next_subscribe_dep(dep, rewire_body(body_cell));
    }

    if st.source_done && st.inners.is_empty() && st.queue.is_empty() {
        ctx.down(vec![Message::Complete]);
    }
}

fn run_repeat_body<T: 'static>(
    ctx: &Ctx,
    factory: &Rc<dyn Fn() -> Node<T>>,
    count: usize,
    body_cell: &BodyCell,
) {
    let mut st = ctx
        .state_get::<RepeatState>()
        .map(|v| (*v).clone())
        .unwrap_or(RepeatState {
            started: false,
            round: 0,
            inner: None,
        });

    forward_data(ctx, 0);

    if let Some(error) = first_error_terminal(ctx) {
        if let Some(inner) = st.inner.take() {
            ctx.rewire_next_unsubscribe_dep(inner, rewire_body(body_cell));
        }
        ctx.state_set(st);
        ctx.down(vec![Message::Error(error.into())]);
        return;
    }

    if !st.started {
        let Some(inner) = make_repeat_inner(ctx, factory) else {
            return;
        };
        st.started = true;
        st.round = 0;
        st.inner = Some(inner.clone());
        ctx.state_set(st);
        ctx.rewire_next_subscribe_dep(inner, rewire_body(body_cell));
        return;
    }

    if st.inner.is_some() && is_complete(ctx.terminal(0)) {
        let old = st.inner.take().expect("repeat inner was checked as Some");
        ctx.rewire_next_unsubscribe_dep(old, rewire_body(body_cell));
        if st.round + 1 < count {
            st.round += 1;
            let Some(next) = make_repeat_inner(ctx, factory) else {
                ctx.state_set(RepeatState {
                    started: true,
                    round: st.round,
                    inner: None,
                });
                return;
            };
            st.inner = Some(next.clone());
            ctx.state_set(st);
            ctx.rewire_next_subscribe_dep(next, rewire_body(body_cell));
        } else {
            ctx.state_set(RepeatState {
                started: true,
                round: st.round,
                inner: None,
            });
            ctx.down(vec![Message::Complete]);
        }
    }
}

fn make_repeat_inner<T: 'static>(ctx: &Ctx, factory: &Rc<dyn Fn() -> Node<T>>) -> Option<Core> {
    match catch_unwind(AssertUnwindSafe(|| factory().erased())) {
        Ok(core) => Some(core),
        Err(payload) => {
            ctx.down(vec![Message::Error(panic_payload(payload).into())]);
            None
        }
    }
}

fn project_inner<TIn: Clone + 'static, TOut: 'static>(
    ctx: &Ctx,
    project: &Project<TIn, TOut>,
    value: &TIn,
    st: &mut MapState<TIn>,
    body_cell: &BodyCell,
) -> Option<Core> {
    match catch_unwind(AssertUnwindSafe(|| project(ctx, value).erased())) {
        Ok(core) => Some(core),
        Err(payload) => {
            cleanup_all_inners(ctx, st, body_cell);
            ctx.state_set(st.clone());
            ctx.down(vec![Message::Error(panic_payload(payload).into())]);
            None
        }
    }
}

fn cleanup_all_inners<TIn: Clone + 'static>(
    ctx: &Ctx,
    st: &mut MapState<TIn>,
    body_cell: &BodyCell,
) {
    let mut seen = Vec::new();
    for inner in st.inners.drain(..) {
        if !contains_core(&seen, &inner) {
            seen.push(inner);
        }
    }
    st.queue.clear();
    st.source_done = true;
    for inner in seen {
        ctx.rewire_next_unsubscribe_dep(inner, rewire_body(body_cell));
    }
}

fn rewire_body(body_cell: &BodyCell) -> impl Fn(&Ctx) + 'static {
    let body_cell = body_cell.clone();
    move |ctx| {
        let body = body_cell
            .borrow()
            .as_ref()
            .expect("higher-order body initialized")
            .clone();
        body(ctx);
    }
}

fn forward_data(ctx: &Ctx, dep: usize) {
    for wave in ctx
        .wave_data()
        .get(dep)
        .into_iter()
        .flat_map(|waves| waves.iter())
    {
        for item in wave.iter() {
            if let WaveData::Data(value) = item {
                ctx.down(vec![Message::Data(value.clone())]);
            }
        }
    }
}

fn first_error_terminal(ctx: &Ctx) -> Option<String> {
    for i in 0..ctx.dep_len() {
        if let Some(DepTerminal::Error(error)) = ctx.terminal(i) {
            return Some(error.to_string());
        }
    }
    None
}

fn is_complete(terminal: Option<&DepTerminal>) -> bool {
    matches!(terminal, Some(DepTerminal::Complete))
}

fn push_unique(out: &mut Vec<Core>, core: Core) {
    if !contains_core(out, &core) {
        out.push(core);
    }
}

fn contains_core(haystack: &[Core], needle: &Core) -> bool {
    haystack.iter().any(|core| core.ptr_eq(needle))
}

fn panic_payload(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_owned()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "higher-order projector panicked".to_owned()
    }
}
