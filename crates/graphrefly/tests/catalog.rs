use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use futures_core::Stream;
use graphrefly::{
    batch, buffer, buffer_count, catch_error, combine, combine_latest, concat, concat_map,
    distinct_until_changed, element_at, empty, exhaust_map, filter, find, first, first_any,
    flat_map, from_iter, future_local, graph, interval, last, last_any, map, merge_map, never,
    on_first_data, on_first_data_where, pairwise, race, reduce, rescue, sample, settle, settle_by,
    skip, stream_local, switch_map, take, take_until, take_while, tap, tap_first, throw_error,
    timer, valve, with_latest_from, zip, Dispatcher, GraphNodeOpts, GraphOptions, LocalAsyncDriver,
    Message, Node,
};

fn collect_data<T: Clone + 'static>(node: &graphrefly::Node<T>) -> Rc<RefCell<Vec<T>>> {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let seen_sink = seen.clone();
    let _keep = node.subscribe(move |msg| {
        if let Message::Data(value) = msg {
            if let Some(v) = value.as_ref().downcast_ref::<T>() {
                seen_sink.borrow_mut().push(v.clone());
            }
        }
    });
    seen
}

fn collect_shapes<T: Clone + 'static>(node: &graphrefly::Node<T>) -> Rc<RefCell<Vec<String>>> {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let seen_sink = seen.clone();
    let _keep = node.subscribe(move |msg| match msg {
        Message::Data(value) => {
            if value.as_ref().downcast_ref::<T>().is_some() {
                seen_sink.borrow_mut().push("DATA".to_owned());
            }
        }
        Message::Complete => seen_sink.borrow_mut().push("COMPLETE".to_owned()),
        Message::Error(_) => seen_sink.borrow_mut().push("ERROR".to_owned()),
        Message::Start | Message::Dirty | Message::Resolved | Message::Invalidate => {}
        Message::Pause(_) | Message::Resume(_) | Message::Teardown => {}
    });
    seen
}

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

struct Sleeper {
    active: Rc<Cell<bool>>,
    callback: Option<Box<dyn FnOnce()>>,
}

struct IntervalTick {
    active: Rc<Cell<bool>>,
    callback: Rc<dyn Fn()>,
}

type PendingFuture = (Rc<Cell<bool>>, Pin<Box<dyn Future<Output = ()>>>);

#[derive(Default)]
struct ManualDriver {
    sleepers: RefCell<Vec<Sleeper>>,
    intervals: RefCell<Vec<IntervalTick>>,
    futures: RefCell<Vec<PendingFuture>>,
}

impl ManualDriver {
    fn fire_sleepers(&self) {
        let sleepers = std::mem::take(&mut *self.sleepers.borrow_mut());
        for mut sleeper in sleepers {
            if sleeper.active.get() {
                if let Some(callback) = sleeper.callback.take() {
                    callback();
                }
            }
        }
    }

    fn tick_intervals(&self) {
        for tick in self.intervals.borrow().iter() {
            if tick.active.get() {
                (tick.callback)();
            }
        }
    }

    fn poll_futures(&self) {
        let waker = Waker::from(Arc::new(NoopWake));
        let mut cx = Context::from_waker(&waker);
        let mut remaining = Vec::new();
        for (active, mut fut) in std::mem::take(&mut *self.futures.borrow_mut()) {
            if !active.get() {
                continue;
            }
            if fut.as_mut().poll(&mut cx).is_pending() {
                remaining.push((active, fut));
            }
        }
        *self.futures.borrow_mut() = remaining;
    }
}

impl LocalAsyncDriver for ManualDriver {
    fn sleep(&self, _duration: Duration, callback: Box<dyn FnOnce()>) -> graphrefly::DriverCancel {
        let active = Rc::new(Cell::new(true));
        self.sleepers.borrow_mut().push(Sleeper {
            active: active.clone(),
            callback: Some(callback),
        });
        Box::new(move || active.set(false))
    }

    fn interval(&self, _period: Duration, callback: Rc<dyn Fn()>) -> graphrefly::DriverCancel {
        let active = Rc::new(Cell::new(true));
        self.intervals.borrow_mut().push(IntervalTick {
            active: active.clone(),
            callback,
        });
        Box::new(move || active.set(false))
    }

    fn spawn_local(
        &self,
        fut: Pin<Box<dyn Future<Output = ()> + 'static>>,
    ) -> graphrefly::DriverCancel {
        let active = Rc::new(Cell::new(true));
        self.futures.borrow_mut().push((active.clone(), fut));
        Box::new(move || active.set(false))
    }
}

struct VecStream<T> {
    values: VecDeque<T>,
}

impl<T> VecStream<T> {
    fn new(values: impl IntoIterator<Item = T>) -> Self {
        Self {
            values: values.into_iter().collect(),
        }
    }
}

impl<T: Unpin> Stream for VecStream<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.get_mut().values.pop_front())
    }
}

#[test]
fn source_primitives_cover_empty_never_and_throw_error() {
    let g = graph();

    let done = g.init_node(empty::<i32>(), vec![], GraphNodeOpts::named("empty"));
    assert_eq!(*collect_shapes::<i32>(&done).borrow(), vec!["COMPLETE"]);
    assert_eq!(done.status(), graphrefly::Status::Completed);

    let silent = g.init_node(never::<i32>(), vec![], GraphNodeOpts::named("never"));
    assert!(collect_shapes::<i32>(&silent).borrow().is_empty());
    assert_eq!(silent.status(), graphrefly::Status::Sentinel);

    let failed = g.init_node(
        throw_error::<i32>("boom"),
        vec![],
        GraphNodeOpts::named("throw"),
    );
    assert_eq!(*collect_shapes::<i32>(&failed).borrow(), vec!["ERROR"]);
    assert_eq!(failed.status(), graphrefly::Status::Errored);
}

#[test]
fn timer_and_interval_use_injected_driver_and_deactivation_cleanup() {
    let driver = Rc::new(ManualDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        local_async_driver: Some(driver.clone()),
        ..GraphOptions::default()
    });

    let once = g.init_node(timer(10), vec![], GraphNodeOpts::named("timer"));
    let once_seen = collect_shapes::<u64>(&once);
    assert!(once_seen.borrow().is_empty());
    driver.fire_sleepers();
    assert_eq!(*once_seen.borrow(), vec!["DATA", "COMPLETE"]);
    assert_eq!(once.cache(), Some(0));

    let ticks = g.init_node(interval(10), vec![], GraphNodeOpts::named("interval"));
    let tick_seen = Rc::new(RefCell::new(Vec::new()));
    let tick_sink = tick_seen.clone();
    let unsubscribe = ticks.subscribe(move |msg| {
        if let Message::Data(value) = msg {
            if let Some(v) = value.as_ref().downcast_ref::<u64>() {
                tick_sink.borrow_mut().push(*v);
            }
        }
    });
    driver.tick_intervals();
    driver.tick_intervals();
    assert_eq!(*tick_seen.borrow(), vec![0, 1]);
    unsubscribe();
    driver.tick_intervals();
    assert_eq!(
        *tick_seen.borrow(),
        vec![0, 1],
        "interval cancel should stop future driver ticks after deactivation"
    );
}

#[test]
fn missing_driver_reports_source_activation_error() {
    let g = graph();
    let timed = g.init_node(timer(1), vec![], GraphNodeOpts::named("timer_missing"));
    assert_eq!(*collect_shapes::<u64>(&timed).borrow(), vec!["ERROR"]);
    assert_eq!(timed.status(), graphrefly::Status::Errored);

    let future = g.init_node(
        future_local(|| async { Ok::<_, io::Error>(7i32) }),
        vec![],
        GraphNodeOpts::named("future_missing"),
    );
    assert_eq!(*collect_shapes::<i32>(&future).borrow(), vec!["ERROR"]);

    let interval_node = g.init_node(
        interval(1),
        vec![],
        GraphNodeOpts::named("interval_missing"),
    );
    assert_eq!(
        *collect_shapes::<u64>(&interval_node).borrow(),
        vec!["ERROR"]
    );

    let stream = g.init_node(
        stream_local(|| VecStream::new([Ok::<_, io::Error>(1i32)])),
        vec![],
        GraphNodeOpts::named("stream_missing"),
    );
    assert_eq!(*collect_shapes::<i32>(&stream).borrow(), vec!["ERROR"]);
}

#[test]
fn graph_with_explicit_dispatcher_uses_preinstalled_driver() {
    let driver = Rc::new(ManualDriver::default());
    let dispatcher = Dispatcher::new();
    dispatcher.set_local_async_driver(Some(driver.clone()));
    let g = graphrefly::graph_opts(GraphOptions {
        dispatcher: Some(dispatcher),
        ..GraphOptions::default()
    });

    let once = g.init_node(
        timer(5),
        vec![],
        GraphNodeOpts::named("explicit_dispatcher_timer"),
    );
    let seen = collect_data(&once);
    driver.fire_sleepers();
    assert_eq!(*seen.borrow(), vec![0]);
}

#[test]
fn graph_local_driver_does_not_mutate_shared_dispatcher_scope() {
    let dispatcher = Dispatcher::new();
    let first_driver = Rc::new(ManualDriver::default());
    let second_driver = Rc::new(ManualDriver::default());

    let first = graphrefly::graph_opts(GraphOptions {
        dispatcher: Some(dispatcher.clone()),
        local_async_driver: Some(first_driver.clone()),
        ..GraphOptions::default()
    });
    let second = graphrefly::graph_opts(GraphOptions {
        dispatcher: Some(dispatcher.clone()),
        local_async_driver: Some(second_driver.clone()),
        ..GraphOptions::default()
    });
    let no_driver = graphrefly::graph_opts(GraphOptions {
        dispatcher: Some(dispatcher),
        ..GraphOptions::default()
    });

    let first_timer = first.init_node(timer(1), vec![], GraphNodeOpts::named("first_timer"));
    let second_timer = second.init_node(timer(1), vec![], GraphNodeOpts::named("second_timer"));
    let missing_timer =
        no_driver.init_node(timer(1), vec![], GraphNodeOpts::named("missing_timer"));

    let first_seen = collect_shapes::<u64>(&first_timer);
    let second_seen = collect_shapes::<u64>(&second_timer);
    let missing_seen = collect_shapes::<u64>(&missing_timer);

    second_driver.fire_sleepers();
    assert_eq!(*first_seen.borrow(), Vec::<String>::new());
    assert_eq!(*second_seen.borrow(), vec!["DATA", "COMPLETE"]);
    assert_eq!(*missing_seen.borrow(), vec!["ERROR"]);

    first_driver.fire_sleepers();
    assert_eq!(*first_seen.borrow(), vec!["DATA", "COMPLETE"]);
}

#[test]
fn local_future_and_stream_sources_emit_via_driver_boundary() {
    let driver = Rc::new(ManualDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        local_async_driver: Some(driver.clone()),
        ..GraphOptions::default()
    });

    let future = g.init_node(
        future_local(|| async { Ok::<_, io::Error>(42i32) }),
        vec![],
        GraphNodeOpts::named("future"),
    );
    let future_seen = collect_shapes::<i32>(&future);
    assert!(future_seen.borrow().is_empty());
    driver.poll_futures();
    assert_eq!(*future_seen.borrow(), vec!["DATA", "COMPLETE"]);
    assert_eq!(future.cache(), Some(42));

    let stream = g.init_node(
        stream_local(|| {
            VecStream::new([
                Ok::<_, io::Error>(1i32),
                Ok::<_, io::Error>(2),
                Ok::<_, io::Error>(3),
            ])
        }),
        vec![],
        GraphNodeOpts::named("stream"),
    );
    let stream_seen = collect_data(&stream);
    driver.poll_futures();
    assert_eq!(*stream_seen.borrow(), vec![1, 2, 3]);
    assert_eq!(stream.status(), graphrefly::Status::Completed);
}

#[test]
fn local_future_and_stream_sources_route_errors_into_protocol() {
    let driver = Rc::new(ManualDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        local_async_driver: Some(driver.clone()),
        ..GraphOptions::default()
    });

    let future = g.init_node(
        future_local(|| async { Err::<i32, _>(io::Error::other("future failed")) }),
        vec![],
        GraphNodeOpts::named("future_error"),
    );
    let future_seen = collect_shapes::<i32>(&future);
    driver.poll_futures();
    assert_eq!(*future_seen.borrow(), vec!["ERROR"]);
    assert_eq!(future.status(), graphrefly::Status::Errored);

    let stream = g.init_node(
        stream_local(|| {
            VecStream::new([
                Ok::<_, io::Error>(1i32),
                Err(io::Error::other("stream failed")),
                Ok::<_, io::Error>(2),
            ])
        }),
        vec![],
        GraphNodeOpts::named("stream_error"),
    );
    let stream_shapes = collect_shapes::<i32>(&stream);
    let stream_data = collect_data(&stream);
    driver.poll_futures();
    assert_eq!(*stream_shapes.borrow(), vec!["DATA", "ERROR"]);
    assert_eq!(*stream_data.borrow(), vec![1]);
    assert_eq!(stream.status(), graphrefly::Status::Errored);
}

#[test]
fn single_dep_catalog_preserves_occurrences_and_terminal_inputs() {
    let g = graph();

    let src = g.init_node(
        from_iter(vec![1i32, 1, 2, 3]),
        vec![],
        GraphNodeOpts::named("src"),
    );
    let sum = g.init_node(
        reduce::<i32, i32>(|acc, v| acc + *v, 0),
        vec![src.erased()],
        GraphNodeOpts::named("sum"),
    );
    assert_eq!(*collect_data(&sum).borrow(), vec![7]);
    assert_eq!(sum.status(), graphrefly::Status::Completed);

    let src = g.init_node(
        from_iter(vec![1i32, 2, 3]),
        vec![],
        GraphNodeOpts::named("src2"),
    );
    let pairs = g.init_node(
        pairwise::<i32>(),
        vec![src.erased()],
        GraphNodeOpts::named("pairs"),
    );
    assert_eq!(*collect_data(&pairs).borrow(), vec![(1, 2), (2, 3)]);

    let src = g.init_node(
        from_iter(vec![1i32, 2, 3, 4, 5]),
        vec![],
        GraphNodeOpts::named("src3"),
    );
    let skipped = g.init_node(
        skip::<i32>(1),
        vec![src.erased()],
        GraphNodeOpts::named("skip"),
    );
    let until = g.init_node(
        take_while::<i32>(|v| *v < 4),
        vec![skipped.erased()],
        GraphNodeOpts::named("take_while"),
    );
    assert_eq!(*collect_data(&until).borrow(), vec![2, 3]);
    assert_eq!(until.status(), graphrefly::Status::Completed);

    let src = g.init_node(
        from_iter(vec![9i32, 8, 7]),
        vec![],
        GraphNodeOpts::named("src4"),
    );
    let first = g.init_node(
        first_any::<i32>(),
        vec![src.erased()],
        GraphNodeOpts::named("first"),
    );
    assert_eq!(*collect_data(&first).borrow(), vec![9]);
    assert_eq!(first.status(), graphrefly::Status::Completed);

    let src = g.init_node(
        from_iter(vec![1i32, 2, 3]),
        vec![],
        GraphNodeOpts::named("src5"),
    );
    let found = g.init_node(
        find::<i32>(|v| *v == 2),
        vec![src.erased()],
        GraphNodeOpts::named("find"),
    );
    assert_eq!(*collect_data(&found).borrow(), vec![2]);
    assert_eq!(found.status(), graphrefly::Status::Completed);

    let src = g.init_node(
        from_iter(vec![4i32, 5, 6]),
        vec![],
        GraphNodeOpts::named("src6"),
    );
    let at = g.init_node(
        element_at::<i32>(1),
        vec![src.erased()],
        GraphNodeOpts::named("element_at"),
    );
    assert_eq!(*collect_data(&at).borrow(), vec![5]);

    let src = g.init_node(
        from_iter(vec![4i32, 5, 6]),
        vec![],
        GraphNodeOpts::named("src7"),
    );
    let last = g.init_node(
        last_any::<i32>(),
        vec![src.erased()],
        GraphNodeOpts::named("last"),
    );
    assert_eq!(*collect_data(&last).borrow(), vec![6]);
}

#[test]
fn catalog_aliases_predicates_and_empty_terminals_are_pinned() {
    let g = graph();

    let src = g.init_node(
        from_iter(vec![1i32, 2, 3]),
        vec![],
        GraphNodeOpts::named("first_predicate_src"),
    );
    let first_even = g.init_node(
        first::<i32>(|v| *v % 2 == 0),
        vec![src.erased()],
        GraphNodeOpts::named("first_predicate"),
    );
    assert_eq!(*collect_data(&first_even).borrow(), vec![2]);
    assert_eq!(first_even.status(), graphrefly::Status::Completed);

    let src = g.init_node(
        from_iter(vec![1i32, 2, 3]),
        vec![],
        GraphNodeOpts::named("last_predicate_src"),
    );
    let last_even = g.init_node(
        last::<i32>(|v| *v % 2 == 0),
        vec![src.erased()],
        GraphNodeOpts::named("last_predicate"),
    );
    assert_eq!(*collect_data(&last_even).borrow(), vec![2]);
    assert_eq!(last_even.status(), graphrefly::Status::Completed);

    let src = g.init_node(
        from_iter(vec![1i32, 2]),
        vec![],
        GraphNodeOpts::named("find_empty_src"),
    );
    let not_found = g.init_node(
        find::<i32>(|v| *v > 9),
        vec![src.erased()],
        GraphNodeOpts::named("find_empty"),
    );
    assert_eq!(
        *collect_shapes::<i32>(&not_found).borrow(),
        vec!["COMPLETE"]
    );

    let src = g.init_node(
        from_iter(vec![1i32, 2]),
        vec![],
        GraphNodeOpts::named("last_empty_src"),
    );
    let no_last = g.init_node(
        last::<i32>(|v| *v > 9),
        vec![src.erased()],
        GraphNodeOpts::named("last_empty"),
    );
    assert_eq!(*collect_shapes::<i32>(&no_last).borrow(), vec!["COMPLETE"]);

    let src = g.init_node(
        from_iter(vec![1i32, 2]),
        vec![],
        GraphNodeOpts::named("element_empty_src"),
    );
    let out_of_range = g.init_node(
        element_at::<i32>(5),
        vec![src.erased()],
        GraphNodeOpts::named("element_empty"),
    );
    assert_eq!(
        *collect_shapes::<i32>(&out_of_range).borrow(),
        vec!["COMPLETE"]
    );

    let empty = g.init_node(
        from_iter(Vec::<i32>::new()),
        vec![],
        GraphNodeOpts::named("reduce_empty_src"),
    );
    let reduced = g.init_node(
        reduce::<i32, i32>(|acc, v| acc + *v, 10),
        vec![empty.erased()],
        GraphNodeOpts::named("reduce_empty"),
    );
    assert_eq!(*collect_data(&reduced).borrow(), vec![10]);

    let source = g.state(1i32);
    let recovered = g.init_node(
        catch_error::<i32>(|_| 42),
        vec![source.erased()],
        GraphNodeOpts::named("catch_error"),
    );
    let recovered_seen = collect_data(&recovered);
    source.down(vec![Message::Error("boom".into())]);
    assert_eq!(*recovered_seen.borrow(), vec![1, 42]);
    assert_ne!(recovered.status(), graphrefly::Status::Completed);
    assert_ne!(recovered.status(), graphrefly::Status::Errored);

    let first_seen = Rc::new(Cell::new(0));
    let first_seen_sink = first_seen.clone();
    let src = g.init_node(
        from_iter(vec![1i32, 2, 3]),
        vec![],
        GraphNodeOpts::named("on_first_where_src"),
    );
    let first_where = g.init_node(
        on_first_data_where::<i32>(move |v| first_seen_sink.set(*v), |v| *v > 1),
        vec![src.erased()],
        GraphNodeOpts::named("on_first_where"),
    );
    assert_eq!(*collect_data(&first_where).borrow(), vec![1, 2, 3]);
    assert_eq!(first_seen.get(), 2);

    let tapped_values = Rc::new(RefCell::new(Vec::new()));
    let tapped_sink = tapped_values.clone();
    let src = g.init_node(
        from_iter(vec![8i32, 9]),
        vec![],
        GraphNodeOpts::named("tap_first_src"),
    );
    let tapped = g.init_node(
        tap_first::<i32>(move |v| tapped_sink.borrow_mut().push(*v)),
        vec![src.erased()],
        GraphNodeOpts::named("tap_first"),
    );
    assert_eq!(*collect_data(&tapped).borrow(), vec![8, 9]);
    assert_eq!(*tapped_values.borrow(), vec![8]);
}

#[test]
fn side_effect_error_and_gate_operators_are_graph_visible() {
    let g = graph();

    let tapped_values = Rc::new(RefCell::new(Vec::new()));
    let tapped_sink = tapped_values.clone();
    let src = g.init_node(
        from_iter(vec![1i32, 2]),
        vec![],
        GraphNodeOpts::named("tap_src"),
    );
    let tapped = g.init_node(
        tap::<i32>(move |v| tapped_sink.borrow_mut().push(*v)),
        vec![src.erased()],
        GraphNodeOpts::named("tap"),
    );
    assert_eq!(*collect_data(&tapped).borrow(), vec![1, 2]);
    assert_eq!(*tapped_values.borrow(), vec![1, 2]);

    let first_seen = Rc::new(Cell::new(0));
    let first_seen_sink = first_seen.clone();
    let src = g.init_node(
        from_iter(vec![3i32, 4, 5]),
        vec![],
        GraphNodeOpts::named("first_data_src"),
    );
    let first_data = g.init_node(
        on_first_data::<i32>(move |v| first_seen_sink.set(*v)),
        vec![src.erased()],
        GraphNodeOpts::named("first_data"),
    );
    assert_eq!(*collect_data(&first_data).borrow(), vec![3, 4, 5]);
    assert_eq!(first_seen.get(), 3);

    let source = g.state(1i32);
    let recovered = g.init_node(
        rescue::<i32>(|_| 99),
        vec![source.erased()],
        GraphNodeOpts::named("rescue"),
    );
    let recovered_seen = collect_data(&recovered);
    source.down(vec![Message::Error("boom".into())]);
    assert_eq!(*recovered_seen.borrow(), vec![1, 99]);
    assert_ne!(recovered.status(), graphrefly::Status::Completed);
    assert_ne!(recovered.status(), graphrefly::Status::Errored);

    let source = g.state(10i32);
    let control = g.state_empty::<bool>();
    let gated = g.init_node(
        valve::<i32>(),
        vec![source.erased(), control.erased()],
        GraphNodeOpts::named("valve"),
    );
    let gated_seen = collect_data(&gated);
    control.set(true);
    source.set(11);
    control.set(false);
    source.set(12);
    control.set(true);
    assert_eq!(*gated_seen.borrow(), vec![10, 11, 12]);

    let snap = g.describe();
    let factories = snap
        .nodes
        .iter()
        .map(|n| (n.id.as_str(), n.factory.as_str()))
        .collect::<Vec<_>>();
    assert!(factories.contains(&("tap", "tap")));
    assert!(factories.contains(&("first_data", "onFirstData")));
    assert!(factories.contains(&("rescue", "rescue")));
    assert!(factories.contains(&("valve", "valve")));
}

#[test]
fn static_combinators_use_declared_deps_and_state() {
    let g = graph();

    let left = g.state_empty::<i32>();
    let right = g.state_empty::<i32>();
    let combined = g.init_node(
        combine::<i32>(),
        vec![left.erased(), right.erased()],
        GraphNodeOpts::named("combine"),
    );
    let combined_seen = collect_data(&combined);
    left.set(1);
    assert!(combined_seen.borrow().is_empty());
    right.set(2);
    left.set(3);
    assert_eq!(*combined_seen.borrow(), vec![vec![1, 2], vec![3, 2]]);

    let left = g.state_empty::<i32>();
    let right = g.state_empty::<i32>();
    let combined_latest = g.init_node(
        combine_latest::<i32>(),
        vec![left.erased(), right.erased()],
        GraphNodeOpts::named("combine_latest"),
    );
    let combined_latest_seen = collect_data(&combined_latest);
    left.set(4);
    right.set(5);
    assert_eq!(*combined_latest_seen.borrow(), vec![vec![4, 5]]);

    let primary = g.state(1i32);
    let secondary = g.state(10i32);
    let with_latest = g.init_node(
        with_latest_from::<i32, i32>(),
        vec![primary.erased(), secondary.erased()],
        GraphNodeOpts::named("with_latest"),
    );
    let with_latest_seen = collect_data(&with_latest);
    secondary.set(20);
    primary.set(2);
    assert_eq!(*with_latest_seen.borrow(), vec![(1, 10), (2, 20)]);

    let a = g.init_node(
        from_iter(vec![1i32, 2]),
        vec![],
        GraphNodeOpts::named("zip_a"),
    );
    let b = g.init_node(
        from_iter(vec![10i32, 20, 30]),
        vec![],
        GraphNodeOpts::named("zip_b"),
    );
    let zipped = g.init_node(
        zip::<i32>(),
        vec![a.erased(), b.erased()],
        GraphNodeOpts::named("zip"),
    );
    assert_eq!(
        *collect_data(&zipped).borrow(),
        vec![vec![1, 10], vec![2, 20]]
    );

    let empty_zip = g.init_node(zip::<i32>(), vec![], GraphNodeOpts::named("zip_empty"));
    assert_eq!(
        *collect_shapes::<Vec<i32>>(&empty_zip).borrow(),
        vec!["COMPLETE"]
    );

    let a = g.init_node(
        from_iter(vec![1i32, 2]),
        vec![],
        GraphNodeOpts::named("concat_a"),
    );
    let b = g.init_node(
        from_iter(vec![3i32, 4]),
        vec![],
        GraphNodeOpts::named("concat_b"),
    );
    let concatted = g.init_node(
        concat::<i32>(),
        vec![a.erased(), b.erased()],
        GraphNodeOpts::named("concat"),
    );
    assert_eq!(*collect_data(&concatted).borrow(), vec![1, 2, 3, 4]);
}

#[test]
fn notifier_combinators_and_race_follow_static_edges() {
    let g = graph();

    let left = g.state_empty::<i32>();
    let right = g.state_empty::<i32>();
    let raced = g.init_node(
        race::<i32>(),
        vec![left.erased(), right.erased()],
        GraphNodeOpts::named("race"),
    );
    let race_seen = collect_data(&raced);
    right.set(9);
    left.set(1);
    right.set(10);
    assert_eq!(*race_seen.borrow(), vec![9, 10]);

    let source = g.state(1i32);
    let notifier = g.state_empty::<()>();
    let buffered = g.init_node(
        buffer::<i32>(),
        vec![source.erased(), notifier.erased()],
        GraphNodeOpts::named("buffer"),
    );
    let buffer_seen = collect_data(&buffered);
    source.set(2);
    notifier.set(());
    source.set(3);
    source.down(vec![Message::Complete]);
    assert_eq!(*buffer_seen.borrow(), vec![vec![1, 2], vec![3]]);

    let src = g.init_node(
        from_iter(vec![1i32, 2, 3]),
        vec![],
        GraphNodeOpts::named("buffer_count_src"),
    );
    let counted = g.init_node(
        buffer_count::<i32>(2),
        vec![src.erased()],
        GraphNodeOpts::named("buffer_count"),
    );
    assert_eq!(*collect_data(&counted).borrow(), vec![vec![1, 2], vec![3]]);

    let source = g.state(5i32);
    let notifier = g.state_empty::<()>();
    let sampled = g.init_node(
        sample::<i32>(),
        vec![source.erased(), notifier.erased()],
        GraphNodeOpts::named("sample"),
    );
    let sample_seen = collect_data(&sampled);
    notifier.set(());
    source.set(6);
    notifier.set(());
    assert_eq!(*sample_seen.borrow(), vec![5, 6]);

    let source = g.state(5i32);
    let notifier = g.state_empty::<()>();
    let sampled = g.init_node(
        sample::<i32>(),
        vec![source.erased(), notifier.erased()],
        GraphNodeOpts::named("sample_error"),
    );
    let sample_shapes = collect_shapes::<i32>(&sampled);
    batch(|_| {
        source.down(vec![Message::Complete]);
        notifier.down(vec![Message::Error("sample notifier failed".into())]);
    });
    assert_eq!(*sample_shapes.borrow(), vec!["ERROR"]);

    let source = g.state(7i32);
    let notifier = g.state_empty::<()>();
    let until = g.init_node(
        take_until::<i32>(),
        vec![source.erased(), notifier.erased()],
        GraphNodeOpts::named("take_until"),
    );
    let until_seen = collect_shapes::<i32>(&until);
    source.set(8);
    notifier.set(());
    source.set(9);
    assert_eq!(*until_seen.borrow(), vec!["DATA", "DATA", "COMPLETE"]);
}

#[test]
fn batched_notifier_and_control_ordering_is_pinned() {
    let g = graph();

    let source = g.state(1i32);
    let notifier = g.state_empty::<()>();
    let buffered = g.init_node(
        buffer::<i32>(),
        vec![source.erased(), notifier.erased()],
        GraphNodeOpts::named("buffer_same_wave"),
    );
    let buffer_seen = collect_data(&buffered);
    batch(|_| {
        source.set(2);
        notifier.set(());
    });
    assert_eq!(*buffer_seen.borrow(), vec![vec![1, 2]]);

    let source = g.state(5i32);
    let notifier = g.state_empty::<()>();
    let sampled = g.init_node(
        sample::<i32>(),
        vec![source.erased(), notifier.erased()],
        GraphNodeOpts::named("sample_same_wave"),
    );
    let sample_seen = collect_data(&sampled);
    batch(|_| {
        source.set(6);
        notifier.set(());
    });
    assert_eq!(*sample_seen.borrow(), vec![6]);

    let source = g.state_empty::<i32>();
    let notifier = g.state_empty::<()>();
    let until = g.init_node(
        take_until::<i32>(),
        vec![source.erased(), notifier.erased()],
        GraphNodeOpts::named("take_until_same_wave"),
    );
    let until_seen = collect_shapes::<i32>(&until);
    batch(|_| {
        source.set(8);
        notifier.set(());
    });
    assert_eq!(*until_seen.borrow(), vec!["COMPLETE"]);

    let source = g.state_empty::<i32>();
    let notifier = g.state_empty::<()>();
    let until = g.init_node(
        take_until::<i32>(),
        vec![source.erased(), notifier.erased()],
        GraphNodeOpts::named("take_until_notifier_first"),
    );
    let until_seen = collect_shapes::<i32>(&until);
    batch(|_| {
        notifier.set(());
        source.set(21);
    });
    assert_eq!(*until_seen.borrow(), vec!["COMPLETE"]);

    let source = g.state(10i32);
    let control = g.state_empty::<bool>();
    let gated = g.init_node(
        valve::<i32>(),
        vec![source.erased(), control.erased()],
        GraphNodeOpts::named("valve_same_wave"),
    );
    let gated_seen = collect_data(&gated);
    batch(|_| {
        control.set(true);
        source.set(11);
    });
    batch(|_| {
        control.set(false);
        source.set(12);
    });
    assert_eq!(*gated_seen.borrow(), vec![11]);
}

#[test]
fn existing_core_catalog_still_composes_after_widening() {
    let g = graph();
    let source = g.init_node(
        from_iter(vec![1i32, 1, 2, 2, 3]),
        vec![],
        GraphNodeOpts::named("source"),
    );
    let mapped = g.init_node(
        map::<i32, i32>(|v| v * 2),
        vec![source.erased()],
        GraphNodeOpts::named("map"),
    );
    let filtered = g.init_node(
        filter::<i32>(|v| *v >= 4),
        vec![mapped.erased()],
        GraphNodeOpts::named("filter"),
    );
    let distinct = g.init_node(
        distinct_until_changed::<i32>(|a, b| a == b),
        vec![filtered.erased()],
        GraphNodeOpts::named("distinct"),
    );
    let first_two = g.init_node(
        take::<i32>(2),
        vec![distinct.erased()],
        GraphNodeOpts::named("take"),
    );

    assert_eq!(*collect_data(&first_two).borrow(), vec![4, 6]);

    let src = g.init_node(
        from_iter(vec![1i32, 1, 1]),
        vec![],
        GraphNodeOpts::named("settle_src"),
    );
    let settled = g.init_node(
        settle::<i32>(1, Some(3)),
        vec![src.erased()],
        GraphNodeOpts::named("settle"),
    );
    assert_eq!(
        *collect_shapes::<i32>(&settled).borrow(),
        vec!["DATA", "DATA", "COMPLETE"]
    );

    let src = g.init_node(
        from_iter(vec![1i32, 2, 2]),
        vec![],
        GraphNodeOpts::named("settle_by_src"),
    );
    let settled = g.init_node(
        settle_by::<i32>(1, Some(3), |a, b| a == b),
        vec![src.erased()],
        GraphNodeOpts::named("settle_by"),
    );
    assert_eq!(
        *collect_shapes::<i32>(&settled).borrow(),
        vec!["DATA", "DATA", "DATA", "COMPLETE"]
    );
}

#[test]
fn higher_order_switch_and_exhaust_use_visible_rewire_deps() {
    let g = graph();

    let source = g.state(1i32);
    let switch_cleanups = Rc::new(Cell::new(0usize));
    let switched = g.init_node(
        switch_map::<i32, i32>({
            let g = g.clone();
            let switch_cleanups = switch_cleanups.clone();
            move |v| {
                let value = *v * 10;
                let switch_cleanups = switch_cleanups.clone();
                g.producer(move |ctx| {
                    ctx.on_deactivation({
                        let switch_cleanups = switch_cleanups.clone();
                        move || switch_cleanups.set(switch_cleanups.get() + 1)
                    });
                    ctx.emit(value);
                })
            }
        }),
        vec![source.erased()],
        GraphNodeOpts::named("switch_map"),
    );
    let switch_seen = collect_data(&switched);
    assert_eq!(*switch_seen.borrow(), vec![10]);
    source.set(2);
    assert_eq!(*switch_seen.borrow(), vec![10, 20]);
    assert_eq!(
        switch_cleanups.get(),
        1,
        "switch_map should remove/deactivate the superseded inner"
    );

    let exhaust_source = g.state(1i32);
    let exhaust_inners = Rc::new(RefCell::new(Vec::<Node<i32>>::new()));
    let exhausted = g.init_node(
        exhaust_map::<i32, i32>({
            let g = g.clone();
            let exhaust_inners = exhaust_inners.clone();
            move |v| {
                let inner = g.state(*v * 10);
                exhaust_inners.borrow_mut().push(inner.clone());
                inner
            }
        }),
        vec![exhaust_source.erased()],
        GraphNodeOpts::named("exhaust_map"),
    );
    let exhaust_seen = collect_data(&exhausted);
    assert_eq!(*exhaust_seen.borrow(), vec![10]);
    exhaust_source.set(2);
    assert_eq!(
        exhaust_inners.borrow().len(),
        1,
        "exhaust_map ignores source DATA while an inner is live"
    );
    assert_eq!(*exhaust_seen.borrow(), vec![10]);
    exhaust_inners.borrow()[0].down(vec![Message::Complete]);
    exhaust_source.set(3);
    assert_eq!(*exhaust_seen.borrow(), vec![10, 30]);

    let snap = g.describe();
    let factories = snap
        .nodes
        .iter()
        .map(|n| (n.id.as_str(), n.factory.as_str()))
        .collect::<Vec<_>>();
    assert!(factories.contains(&("switch_map", "switchMap")));
    assert!(factories.contains(&("exhaust_map", "exhaustMap")));
}

#[test]
fn higher_order_merge_concat_and_flatten_lifecycle_are_pinned() {
    let g = graph();

    let merge_source = g.init_node(
        from_iter(vec![1i32, 2]),
        vec![],
        GraphNodeOpts::named("merge_outer"),
    );
    let merge_inners = Rc::new(RefCell::new(Vec::<Node<i32>>::new()));
    let merged = g.init_node(
        merge_map::<i32, i32>({
            let g = g.clone();
            let merge_inners = merge_inners.clone();
            move |v| {
                let inner = g.state(*v * 10);
                merge_inners.borrow_mut().push(inner.clone());
                inner
            }
        }),
        vec![merge_source.erased()],
        GraphNodeOpts::named("merge_map"),
    );
    let merge_seen = collect_data(&merged);
    assert_eq!(*merge_seen.borrow(), vec![10, 20]);
    let merge_inner_0 = merge_inners.borrow()[0].clone();
    let merge_inner_1 = merge_inners.borrow()[1].clone();
    merge_inner_0.set(11);
    merge_inner_1.set(22);
    assert_eq!(*merge_seen.borrow(), vec![10, 20, 11, 22]);

    let concat_source = g.init_node(
        from_iter(vec![1i32, 2]),
        vec![],
        GraphNodeOpts::named("concat_outer"),
    );
    let concat_inners = Rc::new(RefCell::new(Vec::<Node<i32>>::new()));
    let concatted = g.init_node(
        concat_map::<i32, i32>({
            let g = g.clone();
            let concat_inners = concat_inners.clone();
            move |v| {
                let inner = g.state(*v * 10);
                concat_inners.borrow_mut().push(inner.clone());
                inner
            }
        }),
        vec![concat_source.erased()],
        GraphNodeOpts::named("concat_map"),
    );
    let concat_shapes = collect_shapes::<i32>(&concatted);
    assert_eq!(*concat_shapes.borrow(), vec!["DATA"]);
    assert_eq!(
        concat_inners.borrow().len(),
        1,
        "concat_map projects the next queued inner only after the live one completes"
    );
    let concat_inner_0 = concat_inners.borrow()[0].clone();
    concat_inner_0.down(vec![Message::Complete]);
    assert_eq!(*concat_shapes.borrow(), vec!["DATA", "DATA"]);
    let concat_inner_1 = concat_inners.borrow()[1].clone();
    concat_inner_1.down(vec![Message::Complete]);
    assert_eq!(*concat_shapes.borrow(), vec!["DATA", "DATA", "COMPLETE"]);

    let flat_source = g.init_node(
        from_iter(vec![3i32]),
        vec![],
        GraphNodeOpts::named("flat_outer"),
    );
    let flattened = g.init_node(
        flat_map::<i32, i32>({
            let g = g.clone();
            move |v| g.state(*v * 10)
        }),
        vec![flat_source.erased()],
        GraphNodeOpts::named("flat_map"),
    );
    assert_eq!(*collect_data(&flattened).borrow(), vec![30]);

    let snap = g.describe();
    let factories = snap
        .nodes
        .iter()
        .map(|n| (n.id.as_str(), n.factory.as_str()))
        .collect::<Vec<_>>();
    assert!(factories.contains(&("merge_map", "mergeMap")));
    assert!(factories.contains(&("concat_map", "concatMap")));
    assert!(factories.contains(&("flat_map", "flatMap")));
}

#[test]
fn higher_order_error_paths_detach_live_inners_and_seal_output() {
    let g = graph();

    let source = g.state_empty::<i32>();
    let cleanup_a = Rc::new(Cell::new(0usize));
    let cleanup_b = Rc::new(Cell::new(0usize));
    let inner_a = g.producer({
        let cleanup_a = cleanup_a.clone();
        move |ctx| {
            ctx.on_deactivation({
                let cleanup_a = cleanup_a.clone();
                move || cleanup_a.set(cleanup_a.get() + 1)
            });
        }
    });
    let inner_b = g.producer({
        let cleanup_b = cleanup_b.clone();
        move |ctx| {
            ctx.on_deactivation({
                let cleanup_b = cleanup_b.clone();
                move || cleanup_b.set(cleanup_b.get() + 1)
            });
        }
    });
    let merged = g.init_node(
        merge_map::<i32, i32>({
            let inner_a = inner_a.clone();
            let inner_b = inner_b.clone();
            move |v| {
                if *v == 1 {
                    inner_a.clone()
                } else {
                    inner_b.clone()
                }
            }
        }),
        vec![source.erased()],
        GraphNodeOpts::named("merge_error"),
    );
    let shapes = collect_shapes::<i32>(&merged);

    source.down(vec![Message::Data(Rc::new(1i32))]);
    source.down(vec![Message::Data(Rc::new(2i32))]);
    inner_a.down(vec![Message::Data(Rc::new(10i32))]);
    source.down(vec![Message::Error("source boom".into())]);

    assert_eq!(merged.status(), graphrefly::Status::Errored);
    assert_eq!(cleanup_a.get(), 1, "source ERROR should detach inner A");
    assert_eq!(cleanup_b.get(), 1, "source ERROR should detach inner B");
    assert_eq!(*shapes.borrow(), vec!["DATA", "ERROR"]);

    let after_error = shapes.borrow().len();
    inner_a.down(vec![Message::Data(Rc::new(11i32))]);
    inner_b.down(vec![Message::Data(Rc::new(20i32))]);
    source.down(vec![Message::Data(Rc::new(3i32))]);
    assert_eq!(
        shapes.borrow().len(),
        after_error,
        "terminal higher-order output should be sealed after ERROR"
    );
}

#[test]
fn higher_order_projector_panic_cleans_live_inners() {
    let g = graph();

    let source = g.state_empty::<i32>();
    let cleanup = Rc::new(Cell::new(0usize));
    let inner = g.producer({
        let cleanup = cleanup.clone();
        move |ctx| {
            ctx.on_deactivation({
                let cleanup = cleanup.clone();
                move || cleanup.set(cleanup.get() + 1)
            });
        }
    });
    let merged = g.init_node(
        merge_map::<i32, i32>({
            let inner = inner.clone();
            move |v| {
                assert!(*v != 2, "projector boom");
                inner.clone()
            }
        }),
        vec![source.erased()],
        GraphNodeOpts::named("merge_panic"),
    );
    let shapes = collect_shapes::<i32>(&merged);

    source.down(vec![Message::Data(Rc::new(1i32))]);
    inner.down(vec![Message::Data(Rc::new(10i32))]);
    source.down(vec![Message::Data(Rc::new(2i32))]);

    assert_eq!(merged.status(), graphrefly::Status::Errored);
    assert_eq!(cleanup.get(), 1, "projector panic should detach live inner");
    assert_eq!(*shapes.borrow(), vec!["DATA", "ERROR"]);

    let after_error = shapes.borrow().len();
    inner.down(vec![Message::Data(Rc::new(11i32))]);
    source.down(vec![Message::Data(Rc::new(3i32))]);
    assert_eq!(
        shapes.borrow().len(),
        after_error,
        "terminal higher-order output should be sealed after projector panic"
    );
}
