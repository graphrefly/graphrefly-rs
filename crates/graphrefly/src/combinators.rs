//! Static-dep combinators (D6/D24/B53).
//!
//! These are Rust-native graph-visible operator factories. They use declared deps
//! plus ctx state only; no internal subscribe islands (D45). Heterogeneous TS
//! tuple helpers are represented here as homogeneous `Vec<T>` or fixed tuple
//! helpers where Rust can type them cleanly.

use std::collections::VecDeque;

use crate::ctx::{Ctx, DepTerminal, WaveData};
use crate::node::NodeOpts;
use crate::operators::Operator;
use crate::protocol::Message;

/// combine: emit latest values from all deps once every dep has delivered.
pub fn combine<T: Clone + 'static>() -> Operator<Vec<T>> {
    Operator::with_opts(
        "combine",
        NodeOpts {
            partial: true,
            ..NodeOpts::default()
        },
        move |ctx| {
            let mut values = Vec::with_capacity(ctx.dep_len());
            for i in 0..ctx.dep_len() {
                let Some(value) = ctx.data::<T>(i) else {
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
            complete_when_deps_complete: false,
            terminal_as_real_input: true,
            ..NodeOpts::default()
        },
        move |ctx| {
            if is_complete(ctx.terminal(0)) {
                ctx.down(vec![Message::Complete]);
                return;
            }
            let Some(secondary) = ctx.data::<B>(1) else {
                return;
            };
            for primary in ctx.batch::<A>(0) {
                ctx.emit(((*primary).clone(), (*secondary).clone()));
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
            partial: true,
            complete_when_deps_complete: false,
            terminal_as_real_input: true,
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
                for value in ctx.batch::<T>(i) {
                    st.queues[i].push_back((*value).clone());
                }
                if is_complete(ctx.terminal(i)) {
                    st.complete[i] = true;
                }
            }

            while st.queues.iter().all(|q| !q.is_empty()) {
                let tuple = st
                    .queues
                    .iter_mut()
                    .map(|q| q.pop_front().expect("zip queue is non-empty"))
                    .collect::<Vec<_>>();
                ctx.emit(tuple);
            }

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
            partial: true,
            complete_when_deps_complete: false,
            terminal_as_real_input: true,
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

            if st.phase == 0 {
                for value in ctx.batch::<T>(0) {
                    ctx.emit((*value).clone());
                }
                for value in ctx.batch::<T>(1) {
                    st.pending.push((*value).clone());
                }
                if is_complete(ctx.terminal(1)) {
                    st.second_done = true;
                }
                if is_complete(ctx.terminal(0)) {
                    st.phase = 1;
                    for value in st.pending.drain(..) {
                        ctx.emit(value);
                    }
                    if st.second_done {
                        ctx.down(vec![Message::Complete]);
                    }
                }
            } else {
                for value in ctx.batch::<T>(1) {
                    ctx.emit((*value).clone());
                }
                if is_complete(ctx.terminal(1)) {
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
            partial: true,
            error_when_deps_error: false,
            complete_when_deps_complete: false,
            terminal_as_real_input: true,
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
                if ctx.terminal(i).is_some() {
                    st.terminals[i] = true;
                }
            }

            if let Some(winner) = st.winner {
                for value in ctx.batch::<T>(winner) {
                    ctx.emit((*value).clone());
                }
                match ctx.terminal(winner) {
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
                for i in 0..n {
                    let batch = ctx.batch::<T>(i);
                    if !batch.is_empty() {
                        st.winner = Some(i);
                        for value in batch {
                            ctx.emit((*value).clone());
                        }
                        break;
                    }
                }
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
            partial: true,
            complete_when_deps_complete: false,
            terminal_as_real_input: true,
            ..NodeOpts::default()
        },
        move |ctx| {
            let mut buf = ctx
                .state_get::<Vec<T>>()
                .map(|v| (*v).clone())
                .unwrap_or_default();
            for value in ctx.batch::<T>(0) {
                buf.push((*value).clone());
            }
            if is_complete(ctx.terminal(0)) {
                if !buf.is_empty() {
                    ctx.emit(buf.clone());
                }
                ctx.state_set(Vec::<T>::new());
                ctx.down(vec![Message::Complete]);
                return;
            }
            if dep_has_data(ctx, 1) {
                ctx.emit(buf.clone());
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
            complete_when_deps_complete: false,
            terminal_as_real_input: true,
            ..NodeOpts::default()
        },
        move |ctx| {
            let mut buf = ctx
                .state_get::<Vec<T>>()
                .map(|v| (*v).clone())
                .unwrap_or_default();
            for value in ctx.batch::<T>(0) {
                buf.push((*value).clone());
                if buf.len() >= count {
                    ctx.emit(std::mem::take(&mut buf));
                }
            }
            if is_complete(ctx.terminal(0)) {
                if !buf.is_empty() {
                    ctx.emit(buf.clone());
                }
                ctx.state_set(Vec::<T>::new());
                ctx.down(vec![Message::Complete]);
            } else {
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
            partial: true,
            error_when_deps_error: false,
            complete_when_deps_complete: false,
            terminal_as_real_input: true,
            ..NodeOpts::default()
        },
        move |ctx| {
            for dep in [0, 1] {
                if let Some(DepTerminal::Error(error)) = ctx.terminal(dep) {
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

            for value in ctx.batch::<T>(0) {
                st.last = Some((*value).clone());
            }
            if is_complete(ctx.terminal(0)) {
                st.source_done = true;
                st.last = None;
            }
            if is_complete(ctx.terminal(1)) {
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
    Operator::with_opts(
        "takeUntil",
        NodeOpts {
            partial: true,
            complete_when_deps_complete: false,
            terminal_as_real_input: true,
            ..NodeOpts::default()
        },
        move |ctx: &Ctx| {
            if dep_has_data(ctx, 1) {
                ctx.down(vec![Message::Complete]);
                return;
            }
            for value in ctx.batch::<T>(0) {
                ctx.emit((*value).clone());
            }
            if is_complete(ctx.terminal(0)) {
                ctx.down(vec![Message::Complete]);
            }
        },
    )
}

fn is_complete(terminal: Option<&DepTerminal>) -> bool {
    matches!(terminal, Some(DepTerminal::Complete))
}

fn dep_has_data(ctx: &Ctx, i: usize) -> bool {
    ctx.wave_data()
        .get(i)
        .into_iter()
        .flat_map(|waves| waves.iter())
        .flat_map(|wave| wave.iter())
        .any(|item| matches!(item, WaveData::Data(_)))
}
