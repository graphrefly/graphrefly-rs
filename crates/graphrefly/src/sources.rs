//! Source factories (D43/D40/D111).
//!
//! Sync sources run directly in the source body. Async/time sources stay at the
//! source/driver boundary: they schedule work on the graph-local driver and emit
//! later through `DeferredCtx`, preserving the sync wave core.

use std::cell::Cell;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::time::Duration;

use futures_core::Stream;

use crate::node::{NodeOpts, Pausable};
use crate::operators::Operator;
use crate::protocol::{AnyValue, Message};

/// of: emit one value and COMPLETE on activation.
pub fn of<T: Clone + 'static>(value: T) -> Operator<T> {
    Operator::new("of", move |ctx| {
        let out: AnyValue = Rc::new(value.clone());
        ctx.down(vec![Message::Data(out), Message::Complete]);
    })
}

/// from_iter: emit every item in order, then COMPLETE, on activation.
pub fn from_iter<T: Clone + 'static>(items: impl IntoIterator<Item = T>) -> Operator<T> {
    let values: Vec<T> = items.into_iter().collect();
    Operator::new("fromIter", move |ctx| {
        for value in &values {
            let out: AnyValue = Rc::new(value.clone());
            ctx.down(vec![Message::Data(out)]);
        }
        ctx.down(vec![Message::Complete]);
    })
}

/// empty: COMPLETE immediately with no DATA.
pub fn empty<T: 'static>() -> Operator<T> {
    Operator::new("empty", |ctx| {
        ctx.down(vec![Message::Complete]);
    })
}

/// never: activate and remain silent until deactivation.
pub fn never<T: 'static>() -> Operator<T> {
    Operator::new("never", |_| {})
}

/// throw_error: terminate with ERROR on activation.
pub fn throw_error<T: 'static>(err: impl Into<String>) -> Operator<T> {
    let err = err.into();
    Operator::new("throwError", move |ctx| {
        ctx.down(vec![Message::Error(err.clone().into())]);
    })
}

fn timer_source(factory: &'static str, ms: u64) -> Operator<u64> {
    let duration = Duration::from_millis(ms);
    Operator::with_opts(
        factory,
        NodeOpts {
            pausable: Pausable::False,
            ..NodeOpts::default()
        },
        move |ctx| {
            let Some(driver) = ctx.local_async_driver() else {
                ctx.down(vec![Message::Error(
                    format!("{factory}: missing local async driver").into(),
                )]);
                return;
            };
            let out = ctx.defer();
            let cancel = driver.sleep(
                duration,
                Box::new(move || {
                    out.down(vec![Message::Data(Rc::new(0u64)), Message::Complete]);
                }),
            );
            ctx.on_deactivation(cancel);
        },
    )
}

/// timer: one tick (`0`) after `ms`, then COMPLETE.
///
/// Requires a graph-local driver (D111); missing driver reports ERROR on activation.
pub fn timer(ms: u64) -> Operator<u64> {
    timer_source("timer", ms)
}

/// from_timer: frozen source-name alias for [`timer`].
///
/// Preserves the real factory name (`fromTimer`) in describe/render output.
pub fn from_timer(ms: u64) -> Operator<u64> {
    timer_source("fromTimer", ms)
}

/// interval: ticks `0, 1, 2, ...` every `ms` until deactivation.
///
/// Requires a graph-local driver (D111); missing driver reports ERROR on activation.
pub fn interval(ms: u64) -> Operator<u64> {
    let period = Duration::from_millis(ms);
    Operator::with_opts(
        "interval",
        NodeOpts {
            pausable: Pausable::False,
            ..NodeOpts::default()
        },
        move |ctx| {
            let Some(driver) = ctx.local_async_driver() else {
                ctx.down(vec![Message::Error(
                    "interval: missing local async driver".into(),
                )]);
                return;
            };
            let out = ctx.defer();
            let count = Rc::new(Cell::new(0u64));
            let tick = Rc::new(move || {
                let next = count.get();
                count.set(next + 1);
                out.down(vec![Message::Data(Rc::new(next))]);
            });
            let cancel = driver.interval(period, tick);
            ctx.on_deactivation(cancel);
        },
    )
}

/// future_local: run a fresh single-thread local fallible future on activation.
///
/// `Ok(value)` emits DATA then COMPLETE; `Err(error)` emits ERROR. A plain Rust
/// `Future<Output = T>` has no rejection channel, so Rust async sources use the
/// fallible `Result` shape as the protocol error bridge.
pub fn future_local<T, E, Fut>(make: impl Fn() -> Fut + 'static) -> Operator<T>
where
    T: 'static,
    E: Error + 'static,
    Fut: Future<Output = Result<T, E>> + 'static,
{
    Operator::with_opts(
        "futureLocal",
        NodeOpts {
            pool: crate::dispatcher::PoolKind::Async,
            ..NodeOpts::default()
        },
        move |ctx| {
            let Some(driver) = ctx.local_async_driver() else {
                ctx.down(vec![Message::Error(
                    "futureLocal: missing local async driver".into(),
                )]);
                return;
            };
            let future = make();
            let out = ctx.defer();
            let cancel = driver.spawn_local(Box::pin(async move {
                match future.await {
                    Ok(value) => out.down(vec![Message::Data(Rc::new(value)), Message::Complete]),
                    Err(error) => out.down(vec![Message::Error(error.into())]),
                }
            }));
            ctx.on_deactivation(cancel);
        },
    )
}

/// stream_local: pump a fresh single-thread local fallible stream through the
/// graph-local driver. Every `Ok(item)` becomes DATA; stream exhaustion emits
/// COMPLETE; the first `Err(error)` emits ERROR and terminates the source.
pub fn stream_local<T, E, S>(make: impl Fn() -> S + 'static) -> Operator<T>
where
    T: 'static,
    E: Error + 'static,
    S: Stream<Item = Result<T, E>> + 'static,
{
    Operator::with_opts(
        "streamLocal",
        NodeOpts {
            pool: crate::dispatcher::PoolKind::Async,
            ..NodeOpts::default()
        },
        move |ctx| {
            let Some(driver) = ctx.local_async_driver() else {
                ctx.down(vec![Message::Error(
                    "streamLocal: missing local async driver".into(),
                )]);
                return;
            };
            let mut stream = Box::pin(make()) as Pin<Box<dyn Stream<Item = Result<T, E>>>>;
            let out = ctx.defer();
            let cancel = driver.spawn_local(Box::pin(async move {
                loop {
                    let next = std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
                    match next {
                        Some(Ok(value)) => out.down(vec![Message::Data(Rc::new(value))]),
                        Some(Err(error)) => {
                            out.down(vec![Message::Error(error.into())]);
                            break;
                        }
                        None => {
                            out.down(vec![Message::Complete]);
                            break;
                        }
                    }
                }
            }));
            ctx.on_deactivation(cancel);
        },
    )
}
