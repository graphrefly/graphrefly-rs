//! Static-dep combinators (D6/D24/B53).
//!
//! These are Rust-native graph-visible operator factories. They use declared deps
//! plus ctx state only; no internal subscribe islands (D45). Heterogeneous TS
//! tuple helpers are represented here as homogeneous `Vec<T>` or fixed tuple
//! helpers where Rust can type them cleanly.

use std::collections::VecDeque;

use crate::ctx::{Ctx, DepTerminal};
use crate::node::{NodeOpts, Policy};
use crate::operators::Operator;
use crate::protocol::Message;

/// combine: emit latest values from all deps once every dep has delivered.
pub fn combine<T: Clone + 'static>() -> Operator<Vec<T>> {
    Operator::with_opts(
        "combine",
        NodeOpts {
            ..NodeOpts::default()
        },
        move |ctx| {
            let mut values = Vec::with_capacity(ctx.dep_len());
            for i in 0..ctx.dep_len() {
                let Some(value) = ctx.dep(i).value::<T>() else {
                    return;
                };
                values.push((*value).clone());
            }
            ctx.emit(values);
        },
    )
}

/// combine_latest: alias-shaped Rust helper for [`combine`].
pub fn combine_latest<T: Clone + 'static>() -> Operator<Vec<T>> {
    combine()
}

/// with_latest_from: dep 0 drives `(primary, latest_secondary)` emissions.
pub fn with_latest_from<A: Clone + 'static, B: Clone + 'static>() -> Operator<(A, B)> {
    Operator::with_opts(
        "withLatestFrom",
        NodeOpts {
            complete: Policy::Manual,
            ..NodeOpts::default()
        },
        move |ctx| {
            if is_complete(ctx.dep(0).terminal()) {
                ctx.down(vec![Message::Complete]);
                return;
            }
            let Some(secondary) = ctx.dep(1).value::<B>() else {
                return;
            };
            for wave in ctx.dep(0).waves::<A>() {
                ctx.down(crate::protocol::data_wave(
                    wave.iter()
                        .map(|primary| ((**primary).clone(), (*secondary).clone())),
                ));
            }
        },
    )
}

#[derive(Clone)]
struct ZipState<T> {
    queues: Vec<VecDeque<T>>,
    complete: Vec<bool>,
}

/// zip: buffer per-dep queues and emit one vector when every dep has one value.
pub fn zip<T: Clone + 'static>() -> Operator<Vec<T>> {
    Operator::with_opts(
        "zip",
        NodeOpts {
            complete: Policy::Manual,
            ..NodeOpts::default()
        },
        move |ctx| {
            let n = ctx.dep_len();
            if n == 0 {
                ctx.down(vec![Message::Complete]);
                return;
            }
            let mut st = ctx
                .state_get::<ZipState<T>>()
                .map(|v| (*v).clone())
                .unwrap_or_else(|| ZipState {
                    queues: vec![VecDeque::new(); n],
                    complete: vec![false; n],
                });
            if st.queues.len() != n {
                st.queues.resize_with(n, VecDeque::new);
                st.complete.resize(n, false);
            }

            for i in 0..n {
                for value in ctx.dep(i).values::<T>() {
                    st.queues[i].push_back((*value).clone());
                }
                if is_complete(ctx.dep(i).terminal()) {
                    st.complete[i] = true;
                }
            }

            let mut emitted = Vec::new();
            while st.queues.iter().all(|q| !q.is_empty()) {
                let tuple = st
                    .queues
                    .iter_mut()
                    .map(|q| q.pop_front().expect("zip queue is non-empty"))
                    .collect::<Vec<_>>();
                emitted.push(tuple);
            }
            ctx.down(crate::protocol::data_wave(emitted));

            let should_complete = st
                .complete
                .iter()
                .enumerate()
                .any(|(i, done)| *done && st.queues[i].is_empty());
            ctx.state_set(st);
            if should_complete {
                ctx.down(vec![Message::Complete]);
            }
        },
    )
}

#[derive(Clone)]
struct ConcatState<T> {
    phase: u8,
    pending: Vec<T>,
    second_done: bool,
}

/// concat: forward dep 0, then dep 1; dep-1 early values are buffered.
pub fn concat<T: Clone + 'static>() -> Operator<T> {
    Operator::with_opts(
        "concat",
        NodeOpts {
            complete: Policy::Manual,
            ..NodeOpts::default()
        },
        move |ctx| {
            let mut st = ctx
                .state_get::<ConcatState<T>>()
                .map(|v| (*v).clone())
                .unwrap_or(ConcatState {
                    phase: 0,
                    pending: Vec::new(),
                    second_done: false,
                });

            // What one run forwards goes out as one array.
            if st.phase == 0 {
                let mut emitted: Vec<T> = ctx
                    .dep(0)
                    .values::<T>()
                    .iter()
                    .map(|value| (**value).clone())
                    .collect();
                for value in ctx.dep(1).values::<T>() {
                    st.pending.push((*value).clone());
                }
                if is_complete(ctx.dep(1).terminal()) {
                    st.second_done = true;
                }
                if is_complete(ctx.dep(0).terminal()) {
                    st.phase = 1;
                    emitted.append(&mut st.pending);
                    ctx.down(crate::protocol::data_wave(emitted));
                    if st.second_done {
                        ctx.down(vec![Message::Complete]);
                    }
                } else {
                    ctx.down(crate::protocol::data_wave(emitted));
                }
            } else {
                ctx.down(crate::protocol::data_wave(
                    ctx.dep(1)
                        .values::<T>()
                        .iter()
                        .map(|value| (**value).clone()),
                ));
                if is_complete(ctx.dep(1).terminal()) {
                    ctx.down(vec![Message::Complete]);
                }
            }
            ctx.state_set(st);
        },
    )
}

#[derive(Clone)]
struct RaceState {
    winner: Option<usize>,
    terminals: Vec<bool>,
}

/// race: first dep to deliver DATA wins; loser terminals are ignored.
pub fn race<T: Clone + 'static>() -> Operator<T> {
    Operator::with_opts(
        "race",
        NodeOpts {
            error: Policy::Manual,
            complete: Policy::Manual,
            ..NodeOpts::default()
        },
        move |ctx| {
            let n = ctx.dep_len();
            let mut st = ctx
                .state_get::<RaceState>()
                .map(|v| (*v).clone())
                .unwrap_or_else(|| RaceState {
                    winner: None,
                    terminals: vec![false; n],
                });
            st.terminals.resize(n, false);

            for i in 0..n {
                if ctx.dep(i).terminal().is_some() {
                    st.terminals[i] = true;
                }
            }

            if let Some(winner) = st.winner {
                for wave in ctx.dep(winner).waves::<T>() {
                    ctx.down(crate::protocol::data_wave(
                        wave.iter().map(|value| (**value).clone()),
                    ));
                }
                match ctx.dep(winner).terminal() {
                    Some(DepTerminal::Complete) => {
                        ctx.state_set(st);
                        ctx.down(vec![Message::Complete]);
                        return;
                    }
                    Some(DepTerminal::Error(error)) => {
                        ctx.state_set(st);
                        ctx.down(vec![Message::Error(error.to_string().into())]);
                        return;
                    }
                    None => {}
                }
            } else {
                let mut emitted = Vec::new();
                for i in 0..n {
                    let batch = ctx.dep(i).values::<T>();
                    if !batch.is_empty() {
                        st.winner = Some(i);
                        for value in batch {
                            emitted.push((*value).clone());
                        }
                        break;
                    }
                }
                ctx.down(crate::protocol::data_wave(emitted));
                if st.winner.is_none() && st.terminals.iter().all(|t| *t) {
                    ctx.state_set(st);
                    ctx.down(vec![Message::Complete]);
                    return;
                }
            }

            ctx.state_set(st);
        },
    )
}

/// buffer: collect dep 0 values, flush a Vec on each dep 1 notifier DATA.
pub fn buffer<T: Clone + 'static>() -> Operator<Vec<T>> {
    Operator::with_opts(
        "buffer",
        NodeOpts {
            complete: Policy::Manual,
            ..NodeOpts::default()
        },
        move |ctx| {
            let mut buf = ctx
                .state_get::<Vec<T>>()
                .map(|v| (*v).clone())
                .unwrap_or_default();
            for value in ctx.dep(0).values::<T>() {
                buf.push((*value).clone());
            }
            if is_complete(ctx.dep(0).terminal()) {
                if !buf.is_empty() {
                    ctx.emit(buf.clone());
                }
                ctx.state_set(Vec::<T>::new());
                ctx.down(vec![Message::Complete]);
                return;
            }
            if dep_has_data(ctx, 1) {
                ctx.emit(std::mem::take(&mut buf));
                ctx.state_set(Vec::<T>::new());
            } else {
                ctx.state_set(buf);
            }
        },
    )
}

/// buffer_count: emit chunks of `count`, flushing the remainder on source COMPLETE.
pub fn buffer_count<T: Clone + 'static>(count: usize) -> Operator<Vec<T>> {
    assert!(count > 0, "buffer_count: count must be positive");
    Operator::with_opts(
        "bufferCount",
        NodeOpts {
            complete: Policy::Manual,
            ..NodeOpts::default()
        },
        move |ctx| {
            let mut buf = ctx
                .state_get::<Vec<T>>()
                .map(|v| (*v).clone())
                .unwrap_or_default();
            let mut emitted = Vec::new();
            for value in ctx.dep(0).values::<T>() {
                buf.push((*value).clone());
                if buf.len() >= count {
                    emitted.push(std::mem::take(&mut buf));
                }
            }
            if is_complete(ctx.dep(0).terminal()) {
                if !buf.is_empty() {
                    emitted.push(std::mem::take(&mut buf));
                }
                ctx.down(crate::protocol::data_wave(emitted));
                ctx.state_set(Vec::<T>::new());
                ctx.down(vec![Message::Complete]);
            } else {
                ctx.down(crate::protocol::data_wave(emitted));
                ctx.state_set(buf);
            }
        },
    )
}

#[derive(Clone)]
struct SampleState<T> {
    last: Option<T>,
    source_done: bool,
}

/// sample: emit dep 0's latest value whenever dep 1 notifier delivers DATA.
pub fn sample<T: Clone + 'static>() -> Operator<T> {
    Operator::with_opts(
        "sample",
        NodeOpts {
            error: Policy::Manual,
            complete: Policy::Manual,
            ..NodeOpts::default()
        },
        move |ctx| {
            for dep in [0, 1] {
                if let Some(DepTerminal::Error(error)) = ctx.dep(dep).terminal() {
                    ctx.down(vec![Message::Error(error.to_string().into())]);
                    return;
                }
            }

            let mut st = ctx
                .state_get::<SampleState<T>>()
                .map(|v| (*v).clone())
                .unwrap_or(SampleState {
                    last: None,
                    source_done: false,
                });

            for value in ctx.dep(0).values::<T>() {
                st.last = Some((*value).clone());
            }
            if is_complete(ctx.dep(0).terminal()) {
                st.source_done = true;
                st.last = None;
            }
            if is_complete(ctx.dep(1).terminal()) {
                ctx.state_set(st);
                ctx.down(vec![Message::Complete]);
                return;
            }
            if dep_has_data(ctx, 1) && !st.source_done {
                if let Some(value) = st.last.clone() {
                    ctx.emit(value);
                }
            }
            ctx.state_set(st);
        },
    )
}

/// take_until: forward source values until notifier DATA arrives, then COMPLETE.
pub fn take_until<T: Clone + 'static>() -> Operator<T> {
    take_until_op::<T>().with_recipe("takeUntil", None)
}

fn take_until_op<T: Clone + 'static>() -> Operator<T> {
    Operator::with_opts(
        "takeUntil",
        NodeOpts {
            complete: Policy::Manual,
            ..NodeOpts::default()
        },
        move |ctx: &Ctx| {
            if dep_has_data(ctx, 1) {
                ctx.down(vec![Message::Complete]);
                return;
            }
            let mut emitted = Vec::new();
            for value in ctx.dep(0).values::<T>() {
                emitted.push((*value).clone());
            }
            ctx.down(crate::protocol::data_wave(emitted));
            if is_complete(ctx.dep(0).terminal()) {
                ctx.down(vec![Message::Complete]);
            }
        },
    )
}

fn is_complete(terminal: Option<&DepTerminal>) -> bool {
    matches!(terminal, Some(DepTerminal::Complete))
}

fn dep_has_data(ctx: &Ctx, i: usize) -> bool {
    i < ctx.dep_len() && ctx.dep(i).raw_waves().iter().any(|wave| !wave.is_empty())
}
