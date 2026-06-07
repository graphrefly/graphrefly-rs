use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::fs;
use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::process::Command;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_core::Stream;
use graphrefly::{
    audit, audit_time, batch, buffer, buffer_count, buffer_time, catch_error, combine,
    combine_latest, concat, concat_map, debounce, debounce_time, delay, distinct_until_changed,
    element_at, empty, exhaust_map, filter, find, first, first_any, flat_map, from_cron,
    from_cron_with_options, from_fs_watch, from_fs_watch_with_options, from_git_hook,
    from_git_hook_with_options, from_http, from_iter, from_process, from_sse, from_timer,
    from_webhook, from_webhook_with_options, from_websocket, future_local, graph, interval, last,
    last_any, map, matches_cron, merge_map, merge_map_with_options, never, of, on_first_data,
    on_first_data_where, pairwise, parse_cron, race, reduce, repeat, rescue, run_process, sample,
    scan, settle, settle_by, skip, stream_local, switch_map, take, take_until, take_while, tap,
    tap_first, throttle, throttle_time, throw_error, timeout, timer, valve, with_latest_from, zip,
    CronInstant, CronTick, Dispatcher, EnvironmentDrivers, FromCronOptions, FromFsWatchOptions,
    FromGitHookOptions, FsEvent, FsEventKind, GitEvent, GraphNodeOpts, GraphOptions, HttpRequest,
    HttpResponse, LocalAsyncDriver, LocalHttpDriver, LocalProcessDriver, LocalSseDriver,
    LocalWebSocketDriver, LocalWebhookDriver, MergeMapOptions, Message, Node, ProcessCommand,
    ProcessResult, SseDriverEvent, SseEvent, WebSocketDriverEvent, WebSocketEvent,
    WebhookDriverEvent, WebhookEvent, WebhookRegistration,
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

fn collect_errors<T: Clone + 'static>(node: &graphrefly::Node<T>) -> Rc<RefCell<Vec<String>>> {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let seen_sink = seen.clone();
    let _keep = node.subscribe(move |msg| {
        if let Message::Error(error) = msg {
            seen_sink.borrow_mut().push(error.to_string());
        }
    });
    seen
}

fn temp_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is after epoch")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "graphrefly-rs-{label}-{}-{nanos}",
        std::process::id()
    ));
    fs::create_dir_all(&dir).expect("temp dir can be created");
    dir
}

fn git(dir: &PathBuf, args: &[&str]) {
    let output = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .expect("git command can run");
    assert!(
        output.status.success(),
        "git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
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
type ProcessCallback = Box<dyn FnOnce(Result<ProcessResult, graphrefly::GraphError>)>;
type PendingProcess = (ProcessCommand, Rc<Cell<bool>>, Option<ProcessCallback>);
type HttpCallback = Box<dyn FnOnce(Result<HttpResponse, graphrefly::GraphError>)>;
type PendingHttp = (HttpRequest, Rc<Cell<bool>>, Option<HttpCallback>);
type SseCallback = Rc<dyn Fn(SseDriverEvent)>;
type PendingSse = (String, Rc<Cell<bool>>, SseCallback);
type WebSocketCallback = Rc<dyn Fn(WebSocketDriverEvent)>;
type PendingWebSocket = (String, Rc<Cell<bool>>, WebSocketCallback);
type WebhookCallback = Rc<dyn Fn(WebhookDriverEvent)>;
type PendingWebhook = (WebhookRegistration, Rc<Cell<bool>>, WebhookCallback);

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

#[derive(Default)]
struct ManualProcessDriver {
    processes: RefCell<Vec<PendingProcess>>,
}

impl ManualProcessDriver {
    fn commands(&self) -> Vec<ProcessCommand> {
        self.processes
            .borrow()
            .iter()
            .map(|(command, _, _)| command.clone())
            .collect()
    }

    fn finish_next(&self, result: Result<ProcessResult, graphrefly::GraphError>) {
        let (_, active, callback) = self.processes.borrow_mut().remove(0);
        if active.get() {
            callback.expect("process callback is live")(result);
        }
    }

    fn finish_next_ignoring_cancel(&self, result: Result<ProcessResult, graphrefly::GraphError>) {
        let (_, _, callback) = self.processes.borrow_mut().remove(0);
        callback.expect("process callback is live")(result);
    }

    fn active_count(&self) -> usize {
        self.processes
            .borrow()
            .iter()
            .filter(|(_, active, _)| active.get())
            .count()
    }
}

impl LocalProcessDriver for ManualProcessDriver {
    fn run(&self, command: ProcessCommand, callback: ProcessCallback) -> graphrefly::DriverCancel {
        let active = Rc::new(Cell::new(true));
        self.processes
            .borrow_mut()
            .push((command, active.clone(), Some(callback)));
        Box::new(move || active.set(false))
    }
}

#[derive(Default)]
struct ManualHttpDriver {
    requests: RefCell<Vec<PendingHttp>>,
}

impl ManualHttpDriver {
    fn requests(&self) -> Vec<HttpRequest> {
        self.requests
            .borrow()
            .iter()
            .map(|(request, _, _)| request.clone())
            .collect()
    }

    fn finish_next(&self, result: Result<HttpResponse, graphrefly::GraphError>) {
        let (_, active, callback) = self.requests.borrow_mut().remove(0);
        if active.get() {
            callback.expect("http callback is live")(result);
        }
    }

    fn finish_next_ignoring_cancel(&self, result: Result<HttpResponse, graphrefly::GraphError>) {
        let (_, _, callback) = self.requests.borrow_mut().remove(0);
        callback.expect("http callback is live")(result);
    }
}

impl LocalHttpDriver for ManualHttpDriver {
    fn request(&self, request: HttpRequest, callback: HttpCallback) -> graphrefly::DriverCancel {
        let active = Rc::new(Cell::new(true));
        self.requests
            .borrow_mut()
            .push((request, active.clone(), Some(callback)));
        Box::new(move || active.set(false))
    }
}

struct EagerHttpDriver {
    canceled: Rc<Cell<usize>>,
}

impl LocalHttpDriver for EagerHttpDriver {
    fn request(&self, request: HttpRequest, callback: HttpCallback) -> graphrefly::DriverCancel {
        callback(Ok(HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: request.url.into_bytes(),
        }));
        let canceled = self.canceled.clone();
        Box::new(move || canceled.set(canceled.get() + 1))
    }
}

#[derive(Default)]
struct ManualSseDriver {
    connections: RefCell<Vec<PendingSse>>,
}

impl ManualSseDriver {
    fn emit(&self, event: SseDriverEvent) {
        let (_, active, callback) = &self.connections.borrow()[0];
        if active.get() {
            callback(event);
        }
    }

    fn emit_ignoring_cancel(&self, event: SseDriverEvent) {
        let (_, _, callback) = &self.connections.borrow()[0];
        callback(event);
    }

    fn active_count(&self) -> usize {
        self.connections
            .borrow()
            .iter()
            .filter(|(_, active, _)| active.get())
            .count()
    }
}

impl LocalSseDriver for ManualSseDriver {
    fn connect(
        &self,
        request: graphrefly::SseRequest,
        callback: Rc<dyn Fn(SseDriverEvent)>,
    ) -> graphrefly::DriverCancel {
        let active = Rc::new(Cell::new(true));
        self.connections
            .borrow_mut()
            .push((request.url, active.clone(), callback));
        Box::new(move || active.set(false))
    }
}

#[derive(Default)]
struct ManualWebSocketDriver {
    connections: RefCell<Vec<PendingWebSocket>>,
}

impl ManualWebSocketDriver {
    fn emit(&self, event: WebSocketDriverEvent) {
        let (_, active, callback) = &self.connections.borrow()[0];
        if active.get() {
            callback(event);
        }
    }

    fn emit_ignoring_cancel(&self, event: WebSocketDriverEvent) {
        let (_, _, callback) = &self.connections.borrow()[0];
        callback(event);
    }

    fn active_count(&self) -> usize {
        self.connections
            .borrow()
            .iter()
            .filter(|(_, active, _)| active.get())
            .count()
    }
}

impl LocalWebSocketDriver for ManualWebSocketDriver {
    fn connect(
        &self,
        request: graphrefly::WebSocketRequest,
        callback: Rc<dyn Fn(WebSocketDriverEvent)>,
    ) -> graphrefly::DriverCancel {
        let active = Rc::new(Cell::new(true));
        self.connections
            .borrow_mut()
            .push((request.url, active.clone(), callback));
        Box::new(move || active.set(false))
    }
}

#[derive(Default)]
struct ManualWebhookDriver {
    registrations: RefCell<Vec<PendingWebhook>>,
}

impl ManualWebhookDriver {
    fn registrations(&self) -> Vec<WebhookRegistration> {
        self.registrations
            .borrow()
            .iter()
            .map(|(registration, _, _)| registration.clone())
            .collect()
    }

    fn emit(&self, event: WebhookDriverEvent) {
        let (_, active, callback) = &self.registrations.borrow()[0];
        if active.get() {
            callback(event);
        }
    }

    fn emit_ignoring_cancel(&self, event: WebhookDriverEvent) {
        let (_, _, callback) = &self.registrations.borrow()[0];
        callback(event);
    }

    fn active_count(&self) -> usize {
        self.registrations
            .borrow()
            .iter()
            .filter(|(_, active, _)| active.get())
            .count()
    }
}

impl LocalWebhookDriver for ManualWebhookDriver {
    fn register(
        &self,
        registration: WebhookRegistration,
        callback: Rc<dyn Fn(WebhookDriverEvent)>,
    ) -> graphrefly::DriverCancel {
        let active = Rc::new(Cell::new(true));
        self.registrations
            .borrow_mut()
            .push((registration, active.clone(), callback));
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

    let single = g.init_node(of(7i32), vec![], GraphNodeOpts::named("of"));
    let single_events = Rc::new(RefCell::new(Vec::<String>::new()));
    let single_sink = single_events.clone();
    let _single_sub = single.subscribe(move |msg| match msg {
        Message::Data(value) => single_sink.borrow_mut().push(format!(
            "DATA:{}",
            value.as_ref().downcast_ref::<i32>().expect("of emits i32")
        )),
        Message::Complete => single_sink.borrow_mut().push("COMPLETE".to_owned()),
        Message::Start | Message::Dirty | Message::Resolved | Message::Invalidate => {}
        Message::Error(_) | Message::Pause(_) | Message::Resume(_) | Message::Teardown => {}
    });
    assert_eq!(*single_events.borrow(), vec!["DATA:7", "COMPLETE"]);
    assert_eq!(single.status(), graphrefly::Status::Completed);

    let iter = g.init_node(
        from_iter(vec![1i32, 2, 3]),
        vec![],
        GraphNodeOpts::named("from_iter"),
    );
    let iter_events = Rc::new(RefCell::new(Vec::<String>::new()));
    let iter_sink = iter_events.clone();
    let _iter_sub = iter.subscribe(move |msg| match msg {
        Message::Data(value) => iter_sink.borrow_mut().push(format!(
            "DATA:{}",
            value
                .as_ref()
                .downcast_ref::<i32>()
                .expect("from_iter emits i32")
        )),
        Message::Complete => iter_sink.borrow_mut().push("COMPLETE".to_owned()),
        Message::Start | Message::Dirty | Message::Resolved | Message::Invalidate => {}
        Message::Error(_) | Message::Pause(_) | Message::Resume(_) | Message::Teardown => {}
    });
    assert_eq!(
        *iter_events.borrow(),
        vec!["DATA:1", "DATA:2", "DATA:3", "COMPLETE"]
    );
    assert_eq!(iter.status(), graphrefly::Status::Completed);

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
    let failed_events = Rc::new(RefCell::new(Vec::<String>::new()));
    let failed_sink = failed_events.clone();
    let _failed_sub = failed.subscribe(move |msg| {
        if let Message::Error(error) = msg {
            failed_sink.borrow_mut().push(format!("ERROR:{error}"));
        }
    });
    assert_eq!(*failed_events.borrow(), vec!["ERROR:boom"]);
    assert_eq!(failed.status(), graphrefly::Status::Errored);
}

#[test]
fn source_factory_names_are_stable_in_describe() {
    let g = graph();
    let dir = temp_dir("factory");
    g.init_node(of(1i32), vec![], GraphNodeOpts::named("of"));
    g.init_node(
        from_iter(vec![1i32, 2]),
        vec![],
        GraphNodeOpts::named("from_iter"),
    );
    g.init_node(
        throw_error::<i32>("boom"),
        vec![],
        GraphNodeOpts::named("throw_error"),
    );
    g.init_node(
        future_local(|| async { Ok::<_, io::Error>(1i32) }),
        vec![],
        GraphNodeOpts::named("future_local"),
    );
    g.init_node(
        stream_local(|| VecStream::new([Ok::<_, io::Error>(1i32)])),
        vec![],
        GraphNodeOpts::named("stream_local"),
    );
    g.init_node(from_timer(10), vec![], GraphNodeOpts::named("from_timer"));
    g.init_node(
        from_cron("* * * * *"),
        vec![],
        GraphNodeOpts::named("from_cron"),
    );
    g.init_node(
        from_fs_watch([dir.clone()]),
        vec![],
        GraphNodeOpts::named("from_fs_watch"),
    );
    g.init_node(
        from_git_hook(dir.clone()),
        vec![],
        GraphNodeOpts::named("from_git_hook"),
    );
    g.init_node(
        run_process("echo", ["ok"]),
        vec![],
        GraphNodeOpts::named("run_process"),
    );
    g.init_node(
        from_process("echo", ["ok"]),
        vec![],
        GraphNodeOpts::named("from_process"),
    );
    g.init_node(
        from_http("https://example.invalid"),
        vec![],
        GraphNodeOpts::named("from_http"),
    );
    g.init_node(
        from_sse("https://example.invalid/events"),
        vec![],
        GraphNodeOpts::named("from_sse"),
    );
    g.init_node(
        from_websocket("wss://example.invalid/socket"),
        vec![],
        GraphNodeOpts::named("from_websocket"),
    );
    g.init_node(
        from_webhook("stripe"),
        vec![],
        GraphNodeOpts::named("from_webhook"),
    );

    let snap = g.describe();
    let factory = |id: &str| {
        snap.nodes
            .iter()
            .find(|node| node.id == id)
            .map(|node| node.factory.as_str())
            .unwrap()
            .to_owned()
    };
    assert_eq!(factory("of"), "of");
    assert_eq!(factory("from_iter"), "fromIter");
    assert_eq!(factory("throw_error"), "throwError");
    assert_eq!(factory("future_local"), "futureLocal");
    assert_eq!(factory("stream_local"), "streamLocal");
    assert_eq!(
        factory("from_timer"),
        "fromTimer",
        "the Rust alias should preserve TS's frozen source factory name in describe"
    );
    assert_eq!(factory("from_cron"), "fromCron");
    assert_eq!(factory("from_fs_watch"), "fromFSWatch");
    assert_eq!(factory("from_git_hook"), "fromGitHook");
    assert_eq!(factory("run_process"), "runProcess");
    assert_eq!(factory("from_process"), "fromProcess");
    assert_eq!(factory("from_http"), "fromHttp");
    assert_eq!(factory("from_sse"), "fromSSE");
    assert_eq!(factory("from_websocket"), "fromWebSocket");
    assert_eq!(factory("from_webhook"), "fromWebhook");
    fs::remove_dir_all(dir).ok();
}

#[test]
fn cron_parser_supports_lists_ranges_steps_and_matching() {
    let schedule = parse_cron("0,30 9-17/2 * 1-3 1-5").expect("cron parses");

    assert_eq!(
        schedule.minutes.into_iter().collect::<Vec<_>>(),
        vec![0, 30]
    );
    assert_eq!(
        schedule.hours.into_iter().collect::<Vec<_>>(),
        vec![9, 11, 13, 15, 17]
    );
    assert!(parse_cron("60 * * * *").is_err());
    assert!(parse_cron("* * * *").is_err());
    assert!(parse_cron("*/5foo * * * *").is_err());
    assert!(parse_cron("1/2/3 * * * *").is_err());
    assert!(parse_cron("8-12bar * * * *").is_err());

    let schedule = parse_cron("30 8 * * 1").expect("cron parses");
    assert!(matches_cron(
        &schedule,
        CronInstant::new(2026, 3, 30, 8, 30, 1)
    ));
    assert!(!matches_cron(
        &schedule,
        CronInstant::new(2026, 3, 30, 8, 31, 1)
    ));

    let schedule = parse_cron("0 9 1 * 1").expect("cron parses");
    assert!(
        matches_cron(&schedule, CronInstant::new(2026, 7, 1, 9, 0, 3)),
        "standard cron matches when day-of-month matches"
    );
    assert!(
        matches_cron(&schedule, CronInstant::new(2026, 6, 8, 9, 0, 1)),
        "standard cron matches when day-of-week matches"
    );
    assert!(!matches_cron(
        &schedule,
        CronInstant::new(2026, 6, 2, 9, 0, 2)
    ));

    let schedule = parse_cron("0 9 * * 1").expect("cron parses");
    assert!(matches_cron(
        &schedule,
        CronInstant::new(2026, 6, 8, 9, 0, 1)
    ));
    assert!(!matches_cron(
        &schedule,
        CronInstant::new(2026, 6, 9, 9, 0, 2)
    ));

    let schedule = parse_cron("0 9 1 * *").expect("cron parses");
    assert!(matches_cron(
        &schedule,
        CronInstant::new(2026, 7, 1, 9, 0, 3)
    ));
    assert!(!matches_cron(
        &schedule,
        CronInstant::new(2026, 7, 2, 9, 0, 4)
    ));
}

#[test]
fn from_cron_emits_once_per_matching_minute_and_cleans_up_driver_interval() {
    let driver = Rc::new(ManualDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        environment: EnvironmentDrivers::new().with_local_async(driver.clone()),
        ..GraphOptions::default()
    });
    let ticks = Rc::new(RefCell::new(VecDeque::from([
        CronTick::new(CronInstant::new(2026, 3, 30, 8, 30, 1), "100"),
        CronTick::new(CronInstant::new(2026, 3, 30, 8, 30, 1), "101"),
        CronTick::new(CronInstant::new(2026, 3, 30, 8, 31, 1), "102"),
        CronTick::new(CronInstant::new(2026, 4, 6, 8, 30, 1), "103"),
        CronTick::new(CronInstant::new(2026, 4, 13, 8, 30, 1), "104"),
    ])));
    let now_ticks = ticks.clone();
    let cron = g.init_node(
        from_cron_with_options(
            "30 8 * * 1",
            FromCronOptions {
                tick_ms: 1_000,
                now: Some(Rc::new(move || {
                    now_ticks.borrow_mut().pop_front().expect("test cron tick")
                })),
            },
        ),
        vec![],
        GraphNodeOpts::named("cron"),
    );
    let seen = Rc::new(RefCell::new(Vec::<String>::new()));
    let seen_sink = seen.clone();
    let unsubscribe = cron.subscribe(move |msg| {
        if let Message::Data(value) = msg {
            if let Some(tick) = value.as_ref().downcast_ref::<CronTick>() {
                seen_sink.borrow_mut().push(tick.timestamp_ns.clone());
            }
        }
    });

    assert_eq!(*seen.borrow(), vec!["100".to_owned()]);
    driver.tick_intervals();
    driver.tick_intervals();
    driver.tick_intervals();
    assert_eq!(*seen.borrow(), vec!["100".to_owned(), "103".to_owned()]);
    assert_eq!(
        driver
            .intervals
            .borrow()
            .iter()
            .filter(|tick| tick.active.get())
            .count(),
        1
    );
    assert_eq!(
        g.describe()
            .nodes
            .iter()
            .find(|node| node.id == "cron")
            .map(|node| node.factory.as_str()),
        Some("fromCron")
    );

    unsubscribe();
    assert_eq!(
        driver
            .intervals
            .borrow()
            .iter()
            .filter(|tick| tick.active.get())
            .count(),
        0
    );
    driver.tick_intervals();
    assert_eq!(*seen.borrow(), vec!["100".to_owned(), "103".to_owned()]);
}

#[test]
fn from_git_hook_records_baseline_then_emits_filtered_commit_events() {
    let dir = temp_dir("git-hook");
    git(&dir, &["init"]);
    git(&dir, &["config", "user.email", "test@example.com"]);
    git(&dir, &["config", "user.name", "GraphReFly Test"]);
    fs::write(dir.join("a.txt"), "a").expect("write initial file");
    git(&dir, &["add", "a.txt"]);
    git(&dir, &["commit", "-m", "initial"]);

    let driver = Rc::new(ManualDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        environment: EnvironmentDrivers::new().with_local_async(driver.clone()),
        ..GraphOptions::default()
    });
    let hook = g.init_node(
        from_git_hook_with_options(
            dir.clone(),
            FromGitHookOptions {
                poll_ms: 1,
                include: vec!["*.txt".to_owned()],
                exclude: vec!["skip*".to_owned()],
                ..FromGitHookOptions::default()
            },
        ),
        vec![],
        GraphNodeOpts::named("git_hook"),
    );
    let seen = collect_data::<GitEvent>(&hook);
    assert!(seen.borrow().is_empty(), "first poll establishes baseline");

    fs::write(dir.join(" spaced .txt"), "kept").expect("write kept file");
    fs::write(dir.join("skip.log"), "ignored").expect("write skipped file");
    git(&dir, &["add", "--", " spaced .txt", "skip.log"]);
    git(&dir, &["commit", "-m", "second"]);
    driver.tick_intervals();

    let events = seen.borrow();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].hook, graphrefly::GitHookType::PostCommit);
    assert_eq!(events[0].message, "second");
    assert_eq!(events[0].author, "GraphReFly Test");
    assert_eq!(events[0].files, vec![" spaced .txt".to_owned()]);
    assert!(!events[0].commit.is_empty());
    assert!(!events[0].timestamp_ns.is_empty());
    assert_eq!(
        g.describe()
            .nodes
            .iter()
            .find(|node| node.id == "git_hook")
            .map(|node| node.factory.as_str()),
        Some("fromGitHook")
    );
    fs::remove_dir_all(dir).ok();
}

#[test]
fn from_git_hook_initial_errors_respect_threshold_and_cancel_interval() {
    let dir = temp_dir("git-hook-errors");
    let driver = Rc::new(ManualDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        environment: EnvironmentDrivers::new().with_local_async(driver.clone()),
        ..GraphOptions::default()
    });
    let hook = g.init_node(
        from_git_hook_with_options(
            dir.clone(),
            FromGitHookOptions {
                poll_ms: 1,
                max_consecutive_errors: 2,
                ..FromGitHookOptions::default()
            },
        ),
        vec![],
        GraphNodeOpts::named("git_hook_errors"),
    );
    let errors = collect_errors::<GitEvent>(&hook);

    assert!(
        errors.borrow().is_empty(),
        "first activation error is below the configured threshold"
    );
    assert_eq!(
        driver
            .intervals
            .borrow()
            .iter()
            .filter(|tick| tick.active.get())
            .count(),
        1
    );

    driver.tick_intervals();
    assert_eq!(errors.borrow().len(), 1);
    assert!(
        errors.borrow()[0].contains("rev-parse"),
        "git diagnostic should preserve the failing command"
    );
    assert_eq!(
        driver
            .intervals
            .borrow()
            .iter()
            .filter(|tick| tick.active.get())
            .count(),
        0,
        "terminal source error should clean up the driver interval"
    );

    driver.tick_intervals();
    assert_eq!(errors.borrow().len(), 1);
    fs::remove_dir_all(dir).ok();
}

#[test]
fn fs_watch_source_initial_scan_is_inspectable_and_filtered() {
    let dir = temp_dir("fs-initial");
    fs::write(dir.join("a.txt"), "a").expect("write txt file");
    fs::write(dir.join("skip.log"), "skip").expect("write log file");
    let driver = Rc::new(ManualDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        environment: EnvironmentDrivers::new().with_local_async(driver),
        ..GraphOptions::default()
    });

    let watched = g.init_node(
        from_fs_watch_with_options(
            [dir.clone()],
            FromFsWatchOptions {
                debounce_ms: 1,
                initial_scan: true,
                include: vec!["*.txt".to_owned()],
                ..FromFsWatchOptions::default()
            },
        ),
        vec![],
        GraphNodeOpts::named("fs"),
    );
    let seen = collect_data(&watched);

    assert_eq!(seen.borrow().len(), 1);
    let event = seen.borrow()[0].clone();
    assert_eq!(event.kind, FsEventKind::Create);
    assert_eq!(event.relative_path, PathBuf::from("a.txt"));
    assert!(event.path.ends_with("a.txt"));
    assert_eq!(
        g.describe()
            .nodes
            .iter()
            .find(|node| node.id == "fs")
            .map(|node| node.factory.as_str()),
        Some("fromFSWatch")
    );
    fs::remove_dir_all(dir).ok();
}

#[test]
fn fs_watch_source_rejects_empty_paths_and_reports_missing_driver() {
    let empty = std::panic::catch_unwind(|| from_fs_watch(Vec::<PathBuf>::new()));
    assert!(empty.is_err());

    let dir = temp_dir("fs-missing-driver");
    let g = graph();
    let watched = g.init_node(
        from_fs_watch([dir.clone()]),
        vec![],
        GraphNodeOpts::named("fs_missing_driver"),
    );
    assert_eq!(
        *collect_errors::<FsEvent>(&watched).borrow(),
        vec!["fromFSWatch: missing local async driver".to_owned()]
    );
    assert_eq!(watched.status(), graphrefly::Status::Errored);
    fs::remove_dir_all(dir).ok();
}

#[test]
fn fs_watch_source_deactivation_cancels_poll_driver() {
    let dir = temp_dir("fs-cleanup");
    let driver = Rc::new(ManualDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        environment: EnvironmentDrivers::new().with_local_async(driver.clone()),
        ..GraphOptions::default()
    });
    let watched = g.init_node(
        from_fs_watch([dir.clone()]),
        vec![],
        GraphNodeOpts::named("fs_cleanup"),
    );
    let unsubscribe = watched.subscribe(|_| {});
    assert_eq!(
        driver
            .intervals
            .borrow()
            .iter()
            .filter(|tick| tick.active.get())
            .count(),
        1
    );
    unsubscribe();
    assert_eq!(
        driver
            .intervals
            .borrow()
            .iter()
            .filter(|tick| tick.active.get())
            .count(),
        0
    );
    fs::remove_dir_all(dir).ok();
}

#[test]
fn timer_and_interval_use_injected_driver_and_deactivation_cleanup() {
    let driver = Rc::new(ManualDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        environment: EnvironmentDrivers::new().with_local_async(driver.clone()),
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
fn time_helpers_cancel_armed_clock_on_unsubscribe() {
    let driver = Rc::new(ManualDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        environment: EnvironmentDrivers::new().with_local_async(driver.clone()),
        ..GraphOptions::default()
    });

    let src = g.state_empty::<i32>();
    let timed = timeout(&src, 10);
    let timeout_seen = Rc::new(RefCell::new(Vec::<String>::new()));
    let timeout_sink = timeout_seen.clone();
    let unsubscribe = timed.subscribe(move |msg| match msg {
        Message::Data(_) => timeout_sink.borrow_mut().push("DATA".to_owned()),
        Message::Complete => timeout_sink.borrow_mut().push("COMPLETE".to_owned()),
        Message::Error(_) => timeout_sink.borrow_mut().push("ERROR".to_owned()),
        Message::Start | Message::Dirty | Message::Resolved | Message::Invalidate => {}
        Message::Pause(_) | Message::Resume(_) | Message::Teardown => {}
    });
    assert_eq!(
        driver
            .sleepers
            .borrow()
            .iter()
            .filter(|s| s.active.get())
            .count(),
        1
    );
    unsubscribe();
    assert_eq!(
        driver
            .sleepers
            .borrow()
            .iter()
            .filter(|s| s.active.get())
            .count(),
        0,
        "timeout unsubscribe should deactivate the armed helper timer"
    );
    driver.fire_sleepers();
    assert!(timeout_seen.borrow().is_empty());

    let src = g.state_empty::<i32>();
    let buffered = buffer_time(&src, 10);
    let buffer_seen = Rc::new(RefCell::new(Vec::<String>::new()));
    let buffer_sink = buffer_seen.clone();
    let unsubscribe = buffered.subscribe(move |msg| match msg {
        Message::Data(_) => buffer_sink.borrow_mut().push("DATA".to_owned()),
        Message::Complete => buffer_sink.borrow_mut().push("COMPLETE".to_owned()),
        Message::Error(_) => buffer_sink.borrow_mut().push("ERROR".to_owned()),
        Message::Start | Message::Dirty | Message::Resolved | Message::Invalidate => {}
        Message::Pause(_) | Message::Resume(_) | Message::Teardown => {}
    });
    assert_eq!(
        driver
            .intervals
            .borrow()
            .iter()
            .filter(|tick| tick.active.get())
            .count(),
        1
    );
    unsubscribe();
    assert_eq!(
        driver
            .intervals
            .borrow()
            .iter()
            .filter(|tick| tick.active.get())
            .count(),
        0,
        "buffer_time unsubscribe should deactivate the armed helper interval"
    );
    driver.tick_intervals();
    assert!(buffer_seen.borrow().is_empty());
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
    assert_eq!(
        *collect_errors::<i32>(&future).borrow(),
        vec!["futureLocal: missing local async driver"]
    );

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
    assert_eq!(
        *collect_errors::<i32>(&stream).borrow(),
        vec!["streamLocal: missing local async driver"]
    );

    let from_timer_node = g.init_node(
        from_timer(1),
        vec![],
        GraphNodeOpts::named("from_timer_missing"),
    );
    assert_eq!(
        *collect_errors::<u64>(&from_timer_node).borrow(),
        vec!["fromTimer: missing local async driver".to_owned()],
        "alias diagnostics should match the visible source factory name"
    );

    let cron_node = g.init_node(
        from_cron("* * * * *"),
        vec![],
        GraphNodeOpts::named("cron_missing"),
    );
    assert_eq!(
        *collect_errors::<CronTick>(&cron_node).borrow(),
        vec!["fromCron: missing local async driver".to_owned()]
    );

    let git_node = g.init_node(
        from_git_hook("."),
        vec![],
        GraphNodeOpts::named("git_hook_missing"),
    );
    assert_eq!(
        *collect_errors::<GitEvent>(&git_node).borrow(),
        vec!["fromGitHook: missing local async driver".to_owned()]
    );

    let process = g.init_node(
        run_process("cargo", ["--version"]),
        vec![],
        GraphNodeOpts::named("process_missing"),
    );
    assert_eq!(
        *collect_errors::<ProcessResult>(&process).borrow(),
        vec!["runProcess: missing process driver".to_owned()]
    );

    let http = g.init_node(
        from_http("https://example.invalid"),
        vec![],
        GraphNodeOpts::named("http_missing"),
    );
    assert_eq!(
        *collect_errors::<HttpResponse>(&http).borrow(),
        vec!["fromHttp: missing http driver".to_owned()]
    );

    let sse = g.init_node(
        from_sse("https://example.invalid/events"),
        vec![],
        GraphNodeOpts::named("sse_missing"),
    );
    assert_eq!(
        *collect_errors::<SseEvent>(&sse).borrow(),
        vec!["fromSSE: missing sse driver".to_owned()]
    );

    let websocket = g.init_node(
        from_websocket("wss://example.invalid/socket"),
        vec![],
        GraphNodeOpts::named("websocket_missing"),
    );
    assert_eq!(
        *collect_errors::<WebSocketEvent>(&websocket).borrow(),
        vec!["fromWebSocket: missing websocket driver".to_owned()]
    );

    let webhook = g.init_node(
        from_webhook("stripe"),
        vec![],
        GraphNodeOpts::named("webhook_missing"),
    );
    assert_eq!(
        *collect_errors::<WebhookEvent>(&webhook).borrow(),
        vec!["fromWebhook: missing webhook driver".to_owned()]
    );
}

#[test]
fn run_process_uses_graph_environment_process_driver() {
    let driver = Rc::new(ManualProcessDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        environment: EnvironmentDrivers::new().with_process(driver.clone()),
        ..GraphOptions::default()
    });
    let process = g.init_node(
        run_process("cargo", ["--version"]),
        vec![],
        GraphNodeOpts::named("process"),
    );
    let seen = collect_data::<ProcessResult>(&process);
    let shapes = collect_shapes::<ProcessResult>(&process);

    assert_eq!(
        driver.commands(),
        vec![ProcessCommand::new("cargo").args(["--version"])]
    );
    driver.finish_next(Ok(ProcessResult {
        stdout: "cargo 1.0\n".to_owned(),
        stderr: "warning\n".to_owned(),
        exit_code: Some(7),
        signal: None,
    }));

    assert_eq!(*shapes.borrow(), vec!["DATA", "COMPLETE"]);
    assert_eq!(
        *seen.borrow(),
        vec![ProcessResult {
            stdout: "cargo 1.0\n".to_owned(),
            stderr: "warning\n".to_owned(),
            exit_code: Some(7),
            signal: None,
        }]
    );
}

#[test]
fn process_driver_cancel_suppresses_late_completion() {
    let driver = Rc::new(ManualProcessDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        environment: EnvironmentDrivers::new().with_process(driver.clone()),
        ..GraphOptions::default()
    });
    let process = g.init_node(
        from_process("cargo", ["--version"]),
        vec![],
        GraphNodeOpts::named("cancel_process"),
    );
    let shapes = Rc::new(RefCell::new(Vec::<String>::new()));
    let shapes_sink = shapes.clone();
    let unsubscribe = process.subscribe(move |msg| match msg {
        Message::Data(_) => shapes_sink.borrow_mut().push("DATA".to_owned()),
        Message::Complete => shapes_sink.borrow_mut().push("COMPLETE".to_owned()),
        Message::Error(_) => shapes_sink.borrow_mut().push("ERROR".to_owned()),
        Message::Start | Message::Dirty | Message::Resolved | Message::Invalidate => {}
        Message::Pause(_) | Message::Resume(_) | Message::Teardown => {}
    });

    unsubscribe();
    assert_eq!(driver.active_count(), 0);
    driver.finish_next_ignoring_cancel(Ok(ProcessResult {
        stdout: "late\n".to_owned(),
        stderr: String::new(),
        exit_code: Some(0),
        signal: None,
    }));

    assert!(shapes.borrow().is_empty());
}

#[test]
fn from_http_uses_graph_environment_http_driver() {
    let driver = Rc::new(ManualHttpDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        environment: EnvironmentDrivers::new().with_http(driver.clone()),
        ..GraphOptions::default()
    });
    let http = g.init_node(
        from_http("https://example.test/resource"),
        vec![],
        GraphNodeOpts::named("http"),
    );
    let seen = collect_data::<HttpResponse>(&http);
    let shapes = collect_shapes::<HttpResponse>(&http);

    assert_eq!(
        driver.requests(),
        vec![HttpRequest::get("https://example.test/resource")]
    );
    driver.finish_next(Ok(HttpResponse {
        status: 503,
        headers: vec![("retry-after".to_owned(), "1".to_owned())],
        body: b"busy".to_vec(),
    }));

    assert_eq!(*shapes.borrow(), vec!["DATA", "COMPLETE"]);
    assert_eq!(
        *seen.borrow(),
        vec![HttpResponse {
            status: 503,
            headers: vec![("retry-after".to_owned(), "1".to_owned())],
            body: b"busy".to_vec(),
        }]
    );
}

#[test]
fn http_driver_cancel_suppresses_late_completion() {
    let driver = Rc::new(ManualHttpDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        environment: EnvironmentDrivers::new().with_http(driver.clone()),
        ..GraphOptions::default()
    });
    let http = g.init_node(
        from_http("https://example.test/slow"),
        vec![],
        GraphNodeOpts::named("cancel_http"),
    );
    let shapes = Rc::new(RefCell::new(Vec::<String>::new()));
    let shapes_sink = shapes.clone();
    let unsubscribe = http.subscribe(move |msg| match msg {
        Message::Data(_) => shapes_sink.borrow_mut().push("DATA".to_owned()),
        Message::Complete => shapes_sink.borrow_mut().push("COMPLETE".to_owned()),
        Message::Error(_) => shapes_sink.borrow_mut().push("ERROR".to_owned()),
        Message::Start | Message::Dirty | Message::Resolved | Message::Invalidate => {}
        Message::Pause(_) | Message::Resume(_) | Message::Teardown => {}
    });

    unsubscribe();
    driver.finish_next_ignoring_cancel(Ok(HttpResponse {
        status: 200,
        headers: Vec::new(),
        body: b"late".to_vec(),
    }));

    assert!(shapes.borrow().is_empty());
}

#[test]
fn eager_http_driver_cleanup_installs_returned_cancel_after_sync_completion() {
    let canceled = Rc::new(Cell::new(0usize));
    let driver = Rc::new(EagerHttpDriver {
        canceled: canceled.clone(),
    });
    let g = graphrefly::graph_opts(GraphOptions {
        environment: EnvironmentDrivers::new().with_http(driver),
        ..GraphOptions::default()
    });
    let http = g.init_node(
        from_http("https://example.test/eager"),
        vec![],
        GraphNodeOpts::named("eager_http"),
    );
    let _subscription = http.subscribe(|_| {});

    assert_eq!(
        canceled.get(),
        1,
        "sync driver completion must still release the returned cancel handle"
    );
    assert_eq!(http.status(), graphrefly::Status::Completed);
    assert_eq!(
        http.cache().expect("http response is cached").body,
        b"https://example.test/eager".to_vec()
    );
}

#[test]
fn from_sse_and_websocket_emit_driver_events() {
    let sse_driver = Rc::new(ManualSseDriver::default());
    let websocket_driver = Rc::new(ManualWebSocketDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        environment: EnvironmentDrivers::new()
            .with_sse(sse_driver.clone())
            .with_websocket(websocket_driver.clone()),
        ..GraphOptions::default()
    });
    let sse = g.init_node(
        from_sse("https://example.test/events"),
        vec![],
        GraphNodeOpts::named("sse"),
    );
    let websocket = g.init_node(
        from_websocket("wss://example.test/socket"),
        vec![],
        GraphNodeOpts::named("websocket"),
    );
    let sse_seen = collect_data::<SseEvent>(&sse);
    let sse_shapes = collect_shapes::<SseEvent>(&sse);
    let websocket_seen = collect_data::<WebSocketEvent>(&websocket);
    let websocket_shapes = collect_shapes::<WebSocketEvent>(&websocket);

    sse_driver.emit(SseDriverEvent::Event(SseEvent {
        event: Some("message".to_owned()),
        data: "hello".to_owned(),
        id: Some("42".to_owned()),
        retry_ms: Some(500),
    }));
    sse_driver.emit(SseDriverEvent::Complete);
    assert_eq!(sse_driver.active_count(), 0);
    sse_driver.emit_ignoring_cancel(SseDriverEvent::Event(SseEvent {
        event: Some("message".to_owned()),
        data: "late".to_owned(),
        id: None,
        retry_ms: None,
    }));

    websocket_driver.emit(WebSocketDriverEvent::Event(WebSocketEvent::Open));
    websocket_driver.emit(WebSocketDriverEvent::Event(WebSocketEvent::Text(
        "hello".to_owned(),
    )));
    websocket_driver.emit(WebSocketDriverEvent::Complete);
    assert_eq!(websocket_driver.active_count(), 0);
    websocket_driver.emit_ignoring_cancel(WebSocketDriverEvent::Event(WebSocketEvent::Text(
        "late".to_owned(),
    )));

    assert_eq!(
        *sse_seen.borrow(),
        vec![SseEvent {
            event: Some("message".to_owned()),
            data: "hello".to_owned(),
            id: Some("42".to_owned()),
            retry_ms: Some(500),
        }]
    );
    assert_eq!(*sse_shapes.borrow(), vec!["DATA", "COMPLETE"]);
    assert_eq!(
        *websocket_seen.borrow(),
        vec![
            WebSocketEvent::Open,
            WebSocketEvent::Text("hello".to_owned())
        ]
    );
    assert_eq!(*websocket_shapes.borrow(), vec!["DATA", "DATA", "COMPLETE"]);
}

#[test]
fn from_webhook_registers_environment_bridge_and_emits_events() {
    let driver = Rc::new(ManualWebhookDriver::default());
    let registration = WebhookRegistration::new("stripe")
        .method("POST")
        .path("/hooks/stripe");
    let g = graphrefly::graph_opts(GraphOptions {
        environment: EnvironmentDrivers::new().with_webhook(driver.clone()),
        ..GraphOptions::default()
    });
    let webhook = g.init_node(
        from_webhook_with_options(registration.clone()),
        vec![],
        GraphNodeOpts::named("webhook"),
    );
    let seen = collect_data::<WebhookEvent>(&webhook);
    let shapes = collect_shapes::<WebhookEvent>(&webhook);

    assert_eq!(driver.registrations(), vec![registration]);
    driver.emit(WebhookDriverEvent::Event(WebhookEvent {
        registration_id: "stripe".to_owned(),
        method: "POST".to_owned(),
        path: "/hooks/stripe".to_owned(),
        headers: vec![("content-type".to_owned(), "application/json".to_owned())],
        query: vec![("source".to_owned(), "test".to_owned())],
        body: br#"{"ok":true}"#.to_vec(),
    }));
    driver.emit(WebhookDriverEvent::Complete);
    assert_eq!(driver.active_count(), 0);
    driver.emit_ignoring_cancel(WebhookDriverEvent::Event(WebhookEvent {
        registration_id: "stripe".to_owned(),
        method: "POST".to_owned(),
        path: "/hooks/stripe".to_owned(),
        headers: Vec::new(),
        query: Vec::new(),
        body: b"late".to_vec(),
    }));

    assert_eq!(
        *seen.borrow(),
        vec![WebhookEvent {
            registration_id: "stripe".to_owned(),
            method: "POST".to_owned(),
            path: "/hooks/stripe".to_owned(),
            headers: vec![("content-type".to_owned(), "application/json".to_owned())],
            query: vec![("source".to_owned(), "test".to_owned())],
            body: br#"{"ok":true}"#.to_vec(),
        }]
    );
    assert_eq!(*shapes.borrow(), vec!["DATA", "COMPLETE"]);
}

#[test]
fn from_webhook_error_releases_driver_and_suppresses_late_events() {
    let driver = Rc::new(ManualWebhookDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        environment: EnvironmentDrivers::new().with_webhook(driver.clone()),
        ..GraphOptions::default()
    });
    let webhook = g.init_node(
        from_webhook("github"),
        vec![],
        GraphNodeOpts::named("webhook_error"),
    );
    let shapes = collect_shapes::<WebhookEvent>(&webhook);

    driver.emit(WebhookDriverEvent::Error("webhook failed".into()));
    assert_eq!(driver.active_count(), 0);
    driver.emit_ignoring_cancel(WebhookDriverEvent::Event(WebhookEvent {
        registration_id: "github".to_owned(),
        method: "POST".to_owned(),
        path: "/hooks/github".to_owned(),
        headers: Vec::new(),
        query: Vec::new(),
        body: b"late".to_vec(),
    }));

    assert_eq!(*shapes.borrow(), vec!["ERROR"]);
}

#[test]
fn from_webhook_unsubscribe_cancels_registration_and_suppresses_late_events() {
    let driver = Rc::new(ManualWebhookDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        environment: EnvironmentDrivers::new().with_webhook(driver.clone()),
        ..GraphOptions::default()
    });
    let webhook = g.init_node(
        from_webhook("github"),
        vec![],
        GraphNodeOpts::named("webhook_cancel"),
    );
    let shapes = Rc::new(RefCell::new(Vec::<String>::new()));
    let shapes_sink = shapes.clone();
    let unsubscribe = webhook.subscribe(move |msg| match msg {
        Message::Data(_) => shapes_sink.borrow_mut().push("DATA".to_owned()),
        Message::Complete => shapes_sink.borrow_mut().push("COMPLETE".to_owned()),
        Message::Error(_) => shapes_sink.borrow_mut().push("ERROR".to_owned()),
        Message::Start | Message::Dirty | Message::Resolved | Message::Invalidate => {}
        Message::Pause(_) | Message::Resume(_) | Message::Teardown => {}
    });

    unsubscribe();
    assert_eq!(driver.active_count(), 0);
    driver.emit_ignoring_cancel(WebhookDriverEvent::Event(WebhookEvent {
        registration_id: "github".to_owned(),
        method: "POST".to_owned(),
        path: "/hooks/github".to_owned(),
        headers: Vec::new(),
        query: Vec::new(),
        body: b"late".to_vec(),
    }));

    assert!(shapes.borrow().is_empty());
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
        environment: EnvironmentDrivers::new().with_local_async(first_driver.clone()),
        ..GraphOptions::default()
    });
    let second = graphrefly::graph_opts(GraphOptions {
        dispatcher: Some(dispatcher.clone()),
        environment: EnvironmentDrivers::new().with_local_async(second_driver.clone()),
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
        environment: EnvironmentDrivers::new().with_local_async(driver.clone()),
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
fn local_future_and_stream_sources_cancel_spawned_work_on_unsubscribe() {
    let driver = Rc::new(ManualDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        environment: EnvironmentDrivers::new().with_local_async(driver.clone()),
        ..GraphOptions::default()
    });

    let future = g.init_node(
        future_local(|| async { Ok::<_, io::Error>(42i32) }),
        vec![],
        GraphNodeOpts::named("future_cancel"),
    );
    let future_shapes = Rc::new(RefCell::new(Vec::<String>::new()));
    let future_sink = future_shapes.clone();
    let unsubscribe = future.subscribe(move |msg| match msg {
        Message::Data(_) => future_sink.borrow_mut().push("DATA".to_owned()),
        Message::Complete => future_sink.borrow_mut().push("COMPLETE".to_owned()),
        Message::Error(_) => future_sink.borrow_mut().push("ERROR".to_owned()),
        Message::Start | Message::Dirty | Message::Resolved | Message::Invalidate => {}
        Message::Pause(_) | Message::Resume(_) | Message::Teardown => {}
    });
    assert_eq!(
        driver
            .futures
            .borrow()
            .iter()
            .filter(|(active, _)| active.get())
            .count(),
        1
    );
    unsubscribe();
    assert_eq!(
        driver
            .futures
            .borrow()
            .iter()
            .filter(|(active, _)| active.get())
            .count(),
        0,
        "future_local deactivation should cancel spawned source work"
    );
    driver.poll_futures();
    assert!(future_shapes.borrow().is_empty());

    let stream = g.init_node(
        stream_local(|| VecStream::new([Ok::<_, io::Error>(1i32), Ok::<_, io::Error>(2)])),
        vec![],
        GraphNodeOpts::named("stream_cancel"),
    );
    let stream_shapes = Rc::new(RefCell::new(Vec::<String>::new()));
    let stream_sink = stream_shapes.clone();
    let unsubscribe = stream.subscribe(move |msg| match msg {
        Message::Data(_) => stream_sink.borrow_mut().push("DATA".to_owned()),
        Message::Complete => stream_sink.borrow_mut().push("COMPLETE".to_owned()),
        Message::Error(_) => stream_sink.borrow_mut().push("ERROR".to_owned()),
        Message::Start | Message::Dirty | Message::Resolved | Message::Invalidate => {}
        Message::Pause(_) | Message::Resume(_) | Message::Teardown => {}
    });
    assert_eq!(
        driver
            .futures
            .borrow()
            .iter()
            .filter(|(active, _)| active.get())
            .count(),
        1
    );
    unsubscribe();
    assert_eq!(
        driver
            .futures
            .borrow()
            .iter()
            .filter(|(active, _)| active.get())
            .count(),
        0,
        "stream_local deactivation should cancel spawned source work"
    );
    driver.poll_futures();
    assert!(stream_shapes.borrow().is_empty());
}

#[test]
fn local_future_and_stream_sources_route_errors_into_protocol() {
    let driver = Rc::new(ManualDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        environment: EnvironmentDrivers::new().with_local_async(driver.clone()),
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
fn time_operators_compose_over_graph_scoped_timer_helpers() {
    let driver = Rc::new(ManualDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        environment: EnvironmentDrivers::new().with_local_async(driver.clone()),
        ..GraphOptions::default()
    });

    let delayed_src = g.state_empty::<i32>();
    let delayed = g.init_node(
        delay::<i32>(10),
        vec![delayed_src.erased()],
        GraphNodeOpts::named("delay"),
    );
    let delayed_seen = collect_data(&delayed);
    let delayed_shapes = collect_shapes::<i32>(&delayed);
    delayed_src.set(1);
    delayed_src.set(2);
    assert!(delayed_seen.borrow().is_empty());
    assert_eq!(driver.sleepers.borrow().len(), 2);
    assert!(
        g.describe().nodes.iter().any(|n| n.factory == "timer"),
        "time operators should expose pending helper timer nodes through describe discovery"
    );
    driver.fire_sleepers();
    assert_eq!(
        *delayed_shapes.borrow(),
        vec!["DATA", "DATA"],
        "delay helper timers should settle through DATA, not ERROR or silent COMPLETE"
    );
    assert_eq!(*delayed_seen.borrow(), vec![1, 2]);

    let debounced_src = g.state_empty::<i32>();
    let debounced = g.init_node(
        debounce_time::<i32>(10),
        vec![debounced_src.erased()],
        GraphNodeOpts::named("debounce_time"),
    );
    let debounced_seen = collect_data(&debounced);
    debounced_src.set(1);
    debounced_src.set(2);
    driver.fire_sleepers();
    assert_eq!(
        *debounced_seen.borrow(),
        vec![2],
        "debounce_time should cancel the superseded timer via unsubscribe_dep"
    );

    let throttled_src = g.state_empty::<i32>();
    let throttled = g.init_node(
        throttle_time::<i32>(10),
        vec![throttled_src.erased()],
        GraphNodeOpts::named("throttle_time"),
    );
    let throttled_seen = collect_data(&throttled);
    throttled_src.set(1);
    throttled_src.set(2);
    assert_eq!(
        *throttled_seen.borrow(),
        vec![1],
        "throttle_time is leading-edge and ignores values while the timer window is live"
    );
    driver.fire_sleepers();
    throttled_src.set(3);
    assert_eq!(*throttled_seen.borrow(), vec![1, 3]);

    let audited_src = g.state_empty::<i32>();
    let audited = g.init_node(
        audit_time::<i32>(10),
        vec![audited_src.erased()],
        GraphNodeOpts::named("audit_time"),
    );
    let audited_seen = collect_data(&audited);
    audited_src.set(1);
    audited_src.set(2);
    assert!(audited_seen.borrow().is_empty());
    driver.fire_sleepers();
    assert_eq!(
        *audited_seen.borrow(),
        vec![2],
        "audit_time should emit the latest value when the visible timer dep closes"
    );

    let factories = g
        .describe()
        .nodes
        .into_iter()
        .map(|n| n.factory)
        .collect::<Vec<_>>();
    assert!(factories.contains(&"delay".to_owned()));
    assert!(factories.contains(&"debounceTime".to_owned()));
    assert!(factories.contains(&"throttleTime".to_owned()));
    assert!(factories.contains(&"auditTime".to_owned()));

    let alias_src = g.state_empty::<i32>();
    let _debounce = g.init_node(
        debounce::<i32>(10),
        vec![alias_src.erased()],
        GraphNodeOpts::named("debounce"),
    );
    let _throttle = g.init_node(
        throttle::<i32>(10),
        vec![alias_src.erased()],
        GraphNodeOpts::named("throttle"),
    );
    let notifier = g.state_empty::<()>();
    let _audit = g.init_node(
        audit::<i32, ()>({
            let notifier = notifier.clone();
            move |_| notifier.clone()
        }),
        vec![alias_src.erased()],
        GraphNodeOpts::named("audit"),
    );
    let alias_factories = g
        .describe()
        .nodes
        .into_iter()
        .map(|n| n.factory)
        .collect::<Vec<_>>();
    assert!(alias_factories.contains(&"debounce".to_owned()));
    assert!(alias_factories.contains(&"throttle".to_owned()));
    assert!(alias_factories.contains(&"audit".to_owned()));
}

#[test]
fn throttle_waits_for_open_window_before_source_complete() {
    let driver = Rc::new(ManualDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        environment: EnvironmentDrivers::new().with_local_async(driver.clone()),
        ..GraphOptions::default()
    });
    let src = g.state_empty::<i32>();
    let throttled = g.init_node(
        throttle_time::<i32>(10),
        vec![src.erased()],
        GraphNodeOpts::named("throttle_complete_window"),
    );
    let seen = collect_shapes::<i32>(&throttled);
    let data = collect_data(&throttled);

    src.down(vec![Message::Data(Rc::new(1i32)), Message::Complete]);

    assert_eq!(*data.borrow(), vec![1]);
    assert_eq!(
        *seen.borrow(),
        vec!["DATA"],
        "throttle is exhaustMap-shaped: source COMPLETE waits for the live window dep"
    );
    assert_eq!(throttled.status(), graphrefly::Status::Settled);

    driver.fire_sleepers();

    assert_eq!(*seen.borrow(), vec!["DATA", "COMPLETE"]);
    assert_eq!(throttled.status(), graphrefly::Status::Completed);
}

#[test]
fn throttle_completion_window_survives_deactivation_without_resubscribing_source() {
    let driver = Rc::new(ManualDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        environment: EnvironmentDrivers::new().with_local_async(driver.clone()),
        ..GraphOptions::default()
    });
    let src = g.state_empty::<i32>();
    let throttled = g.init_node(
        throttle_time::<i32>(10),
        vec![src.erased()],
        GraphNodeOpts::named("throttle_deactivate_window"),
    );

    let first_events = Rc::new(RefCell::new(Vec::<String>::new()));
    let first_sink = first_events.clone();
    let unsubscribe = throttled.subscribe(move |msg| match msg {
        Message::Data(value) => first_sink.borrow_mut().push(format!(
            "DATA:{}",
            value
                .as_ref()
                .downcast_ref::<i32>()
                .expect("throttle emits i32")
        )),
        Message::Complete => first_sink.borrow_mut().push("COMPLETE".to_owned()),
        Message::Error(error) => first_sink.borrow_mut().push(format!("ERROR:{error}")),
        Message::Start | Message::Dirty | Message::Resolved | Message::Invalidate => {}
        Message::Pause(_) | Message::Resume(_) | Message::Teardown => {}
    });

    src.down(vec![Message::Data(Rc::new(1i32)), Message::Complete]);
    assert_eq!(*first_events.borrow(), vec!["DATA:1"]);
    unsubscribe();

    driver.fire_sleepers();
    assert_eq!(
        *first_events.borrow(),
        vec!["DATA:1"],
        "deactivation should cancel the in-flight helper timer while no subscriber is present"
    );

    let second_events = Rc::new(RefCell::new(Vec::<String>::new()));
    let second_sink = second_events.clone();
    let _second = throttled.subscribe(move |msg| match msg {
        Message::Data(value) => second_sink.borrow_mut().push(format!(
            "DATA:{}",
            value
                .as_ref()
                .downcast_ref::<i32>()
                .expect("throttle emits i32")
        )),
        Message::Complete => second_sink.borrow_mut().push("COMPLETE".to_owned()),
        Message::Error(error) => second_sink.borrow_mut().push(format!("ERROR:{error}")),
        Message::Start | Message::Dirty | Message::Resolved | Message::Invalidate => {}
        Message::Pause(_) | Message::Resume(_) | Message::Teardown => {}
    });

    assert!(
        second_events.borrow().is_empty(),
        "reactivation should not duplicate the leading DATA from the completed source"
    );
    driver.fire_sleepers();
    assert_eq!(
        *second_events.borrow(),
        vec!["COMPLETE"],
        "the retained helper timer should close the pending completion window"
    );
    assert_eq!(throttled.status(), graphrefly::Status::Completed);
}

#[test]
fn audit_time_flushes_pending_value_on_source_complete() {
    let driver = Rc::new(ManualDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        environment: EnvironmentDrivers::new().with_local_async(driver),
        ..GraphOptions::default()
    });
    let src = g.state_empty::<i32>();
    let audited = g.init_node(
        audit_time::<i32>(10),
        vec![src.erased()],
        GraphNodeOpts::named("audit_time_complete"),
    );
    let seen = collect_shapes::<i32>(&audited);
    let data = collect_data(&audited);

    src.set(7);
    src.down(vec![Message::Complete]);

    assert_eq!(*data.borrow(), vec![7]);
    assert_eq!(*seen.borrow(), vec!["DATA", "COMPLETE"]);
    assert_eq!(audited.status(), graphrefly::Status::Completed);
}

#[test]
fn audit_flushes_same_wave_source_data_before_complete() {
    let driver = Rc::new(ManualDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        environment: EnvironmentDrivers::new().with_local_async(driver),
        ..GraphOptions::default()
    });
    let src = g.state_empty::<i32>();
    let audited = g.init_node(
        audit_time::<i32>(10),
        vec![src.erased()],
        GraphNodeOpts::named("audit_same_wave_complete"),
    );
    let seen = collect_shapes::<i32>(&audited);
    let data = collect_data(&audited);

    src.down(vec![Message::Data(Rc::new(9i32)), Message::Complete]);

    assert_eq!(*data.borrow(), vec![9]);
    assert_eq!(*seen.borrow(), vec!["DATA", "COMPLETE"]);
}

#[test]
fn audit_same_wave_notifier_close_uses_pre_wave_latest() {
    let g = graph();
    let src = g.state_empty::<i32>();
    let notifier = g.state_empty::<()>();
    let audited = g.init_node(
        audit::<i32, ()>({
            let notifier = notifier.clone();
            move |_| notifier.clone()
        }),
        vec![src.erased()],
        GraphNodeOpts::named("audit_same_wave_close"),
    );
    let data = collect_data(&audited);

    src.set(1);
    batch(|_| {
        notifier.set(());
        src.set(2);
    });
    assert_eq!(
        *data.borrow(),
        vec![1],
        "a notifier close should flush the old window before same-wave source DATA opens a new one"
    );

    notifier.set(());
    assert_eq!(*data.borrow(), vec![1, 2]);
}

#[test]
fn audit_same_wave_reopen_only_suppresses_the_closed_notifier() {
    let g = graph();
    let src = g.state_empty::<i32>();
    let old_notifier = g.state_empty::<()>();
    let ready_notifier = g.state(());
    let audited = g.init_node(
        audit::<i32, ()>({
            let old_notifier = old_notifier.clone();
            let ready_notifier = ready_notifier.clone();
            move |v| {
                if *v == 1 {
                    old_notifier.clone()
                } else {
                    ready_notifier.clone()
                }
            }
        }),
        vec![src.erased()],
        GraphNodeOpts::named("audit_reopen_ready_notifier"),
    );
    let data = collect_data(&audited);

    src.set(1);
    batch(|_| {
        old_notifier.set(());
        src.set(2);
    });

    assert_eq!(
        *data.borrow(),
        vec![1, 2],
        "only the old notifier's cached close is suppressed; a different ready notifier closes immediately"
    );
}

#[test]
fn audit_selector_panic_errors_and_seals_output() {
    let g = graph();
    let src = g.state_empty::<i32>();
    let notifier = g.state_empty::<()>();
    let audited = g.init_node(
        audit::<i32, ()>({
            let notifier = notifier.clone();
            move |v| {
                assert!(*v != 2, "audit selector boom");
                notifier.clone()
            }
        }),
        vec![src.erased()],
        GraphNodeOpts::named("audit_selector_panic"),
    );
    let shapes = collect_shapes::<i32>(&audited);
    let data = collect_data(&audited);

    src.set(1);
    notifier.set(());
    src.set(2);

    assert_eq!(audited.status(), graphrefly::Status::Errored);
    assert_eq!(*data.borrow(), vec![1]);
    assert_eq!(*shapes.borrow(), vec!["DATA", "ERROR"]);

    let after_error = shapes.borrow().len();
    src.set(3);
    notifier.set(());
    assert_eq!(
        shapes.borrow().len(),
        after_error,
        "terminal audit output should be sealed after selector panic"
    );
}

#[test]
fn timeout_arms_on_subscribe_resets_and_cleans_up() {
    let driver = Rc::new(ManualDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        environment: EnvironmentDrivers::new().with_local_async(driver.clone()),
        ..GraphOptions::default()
    });
    let src = g.state_empty::<i32>();
    let timed = timeout(&src, 10);
    let seen = collect_shapes::<i32>(&timed);
    let data = collect_data(&timed);

    assert_eq!(driver.sleepers.borrow().len(), 1);
    src.set(1);
    src.set(2);
    assert_eq!(*data.borrow(), vec![1, 2]);
    assert_eq!(
        driver
            .sleepers
            .borrow()
            .iter()
            .filter(|s| s.active.get())
            .count(),
        1,
        "each source value should cancel and replace the idle timer"
    );

    driver.fire_sleepers();
    assert_eq!(*seen.borrow(), vec!["DATA", "DATA", "ERROR"]);

    let src = g.state_empty::<i32>();
    let completing = timeout(&src, 10);
    let completing_seen = collect_shapes::<i32>(&completing);
    src.down(vec![Message::Data(Rc::new(3i32)), Message::Complete]);
    assert_eq!(*completing_seen.borrow(), vec!["DATA", "COMPLETE"]);
    driver.fire_sleepers();
    assert_eq!(
        *completing_seen.borrow(),
        vec!["DATA", "COMPLETE"],
        "source COMPLETE should remove the live idle timer"
    );
}

#[test]
fn timeout_cached_source_forwards_cached_value_then_times_out_after_deadline() {
    let driver = Rc::new(ManualDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        environment: EnvironmentDrivers::new().with_local_async(driver.clone()),
        ..GraphOptions::default()
    });
    let src = g.state(5i32);
    let timed = timeout(&src, 10);
    let seen = collect_shapes::<i32>(&timed);
    let data = collect_data(&timed);

    assert_eq!(*data.borrow(), vec![5]);
    assert_eq!(*seen.borrow(), vec!["DATA"]);
    assert_eq!(
        driver
            .sleepers
            .borrow()
            .iter()
            .filter(|s| s.active.get())
            .count(),
        1,
        "cached source activation should replace the initial idle timer with exactly one live timer"
    );

    driver.fire_sleepers();

    assert_eq!(*seen.borrow(), vec!["DATA", "ERROR"]);
    assert_eq!(
        timed.status(),
        graphrefly::Status::Errored,
        "cached source DATA should not satisfy future idle deadlines forever"
    );
}

#[test]
fn timeout_propagates_source_error_and_missing_driver_error() {
    let driver = Rc::new(ManualDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        environment: EnvironmentDrivers::new().with_local_async(driver.clone()),
        ..GraphOptions::default()
    });
    let src = g.state_empty::<i32>();
    let timed = timeout(&src, 10);
    let seen = collect_shapes::<i32>(&timed);

    src.down(vec![Message::Error("source failed".into())]);
    assert_eq!(*seen.borrow(), vec!["ERROR"]);
    driver.fire_sleepers();
    assert_eq!(
        *seen.borrow(),
        vec!["ERROR"],
        "source ERROR should cancel the helper timer"
    );

    let g = graph();
    let src = g.state_empty::<i32>();
    let missing = timeout(&src, 10);
    assert_eq!(*collect_shapes::<i32>(&missing).borrow(), vec!["ERROR"]);

    let cached_src = g.state(5i32);
    let missing = timeout(&cached_src, 10);
    assert_eq!(
        *collect_shapes::<i32>(&missing).borrow(),
        vec!["ERROR"],
        "D114 subscribe-armed timeout must activate the clock before cached source DATA"
    );
}

#[test]
fn timeout_same_wave_data_then_error_forwards_data_then_error_and_clears_timer() {
    let driver = Rc::new(ManualDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        environment: EnvironmentDrivers::new().with_local_async(driver.clone()),
        ..GraphOptions::default()
    });
    let src = g.state_empty::<i32>();
    let timed = timeout(&src, 10);
    let seen = collect_shapes::<i32>(&timed);
    let data = collect_data(&timed);

    src.down(vec![
        Message::Data(Rc::new(7i32)),
        Message::Error("source failed".into()),
    ]);

    assert_eq!(*data.borrow(), vec![7]);
    assert_eq!(*seen.borrow(), vec!["DATA", "ERROR"]);
    assert_eq!(
        driver
            .sleepers
            .borrow()
            .iter()
            .filter(|s| s.active.get())
            .count(),
        0,
        "source ERROR should clear the helper timer after same-wave DATA is forwarded"
    );
    driver.fire_sleepers();
    assert_eq!(*seen.borrow(), vec!["DATA", "ERROR"]);
}

#[test]
fn buffer_time_flushes_empty_windows_values_and_terminal_remainder() {
    let driver = Rc::new(ManualDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        environment: EnvironmentDrivers::new().with_local_async(driver.clone()),
        ..GraphOptions::default()
    });
    let src = g.state_empty::<i32>();
    let buffered = buffer_time(&src, 10);
    let data = collect_data(&buffered);
    let seen = collect_shapes::<Vec<i32>>(&buffered);

    assert_eq!(driver.intervals.borrow().len(), 1);
    driver.tick_intervals();
    assert_eq!(*data.borrow(), vec![Vec::<i32>::new()]);

    src.set(1);
    src.set(2);
    driver.tick_intervals();
    assert_eq!(*data.borrow(), vec![Vec::<i32>::new(), vec![1, 2]]);

    src.down(vec![Message::Data(Rc::new(3i32)), Message::Complete]);
    assert_eq!(*data.borrow(), vec![Vec::<i32>::new(), vec![1, 2], vec![3]]);
    assert_eq!(*seen.borrow(), vec!["DATA", "DATA", "DATA", "COMPLETE"]);
    driver.tick_intervals();
    assert_eq!(
        *seen.borrow(),
        vec!["DATA", "DATA", "DATA", "COMPLETE"],
        "source COMPLETE should remove the helper interval"
    );
}

#[test]
fn buffer_time_cached_source_buffers_initial_value_and_flushes_on_first_tick() {
    let driver = Rc::new(ManualDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        environment: EnvironmentDrivers::new().with_local_async(driver.clone()),
        ..GraphOptions::default()
    });
    let src = g.state(5i32);
    let buffered = buffer_time(&src, 10);
    let data = collect_data(&buffered);

    assert!(data.borrow().is_empty());
    assert_eq!(
        driver
            .intervals
            .borrow()
            .iter()
            .filter(|tick| tick.active.get())
            .count(),
        1,
        "cached source activation should keep one live flushing interval"
    );

    driver.tick_intervals();
    assert_eq!(*data.borrow(), vec![vec![5]]);
    driver.tick_intervals();
    assert_eq!(
        *data.borrow(),
        vec![vec![5], Vec::<i32>::new()],
        "cached source DATA should not be replayed into later windows"
    );
}

#[test]
fn buffer_time_propagates_source_error_missing_driver_and_is_described() {
    let driver = Rc::new(ManualDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        environment: EnvironmentDrivers::new().with_local_async(driver.clone()),
        ..GraphOptions::default()
    });
    let src = g.state_empty::<i32>();
    let buffered = buffer_time(&src, 10);
    let seen = collect_shapes::<Vec<i32>>(&buffered);

    src.down(vec![Message::Error("source failed".into())]);
    assert_eq!(*seen.borrow(), vec!["ERROR"]);
    driver.tick_intervals();
    assert_eq!(*seen.borrow(), vec!["ERROR"]);

    let src = g.state_empty::<i32>();
    let timed = timeout(&src, 10);
    let buffered = buffer_time(&src, 10);
    let _timed_sink = g.init_node(
        map::<i32, i32>(|v| *v),
        vec![timed.erased()],
        GraphNodeOpts::named("timeout_sink"),
    );
    let _buffered_sink = g.init_node(
        map::<Vec<i32>, Vec<i32>>(|v| v.clone()),
        vec![buffered.erased()],
        GraphNodeOpts::named("buffer_time_sink"),
    );
    let factories = g
        .describe()
        .nodes
        .into_iter()
        .map(|n| n.factory)
        .collect::<Vec<_>>();
    assert!(factories.contains(&"timeout".to_owned()));
    assert!(factories.contains(&"bufferTime".to_owned()));
    assert!(factories.contains(&"timer".to_owned()));
    assert!(factories.contains(&"interval".to_owned()));

    let g = graph();
    let src = g.state_empty::<i32>();
    let missing = buffer_time(&src, 10);
    assert_eq!(
        *collect_shapes::<Vec<i32>>(&missing).borrow(),
        vec!["ERROR"]
    );

    let cached_src = g.state(5i32);
    let missing = buffer_time(&cached_src, 10);
    assert_eq!(
        *collect_shapes::<Vec<i32>>(&missing).borrow(),
        vec!["ERROR"],
        "D114 subscribe-armed buffer_time must activate the interval before cached source DATA"
    );
}

#[test]
fn delay_and_buffer_time_handle_same_wave_data_terminal_edges() {
    let driver = Rc::new(ManualDriver::default());
    let g = graphrefly::graph_opts(GraphOptions {
        environment: EnvironmentDrivers::new().with_local_async(driver.clone()),
        ..GraphOptions::default()
    });
    let src = g.state_empty::<i32>();
    let delayed = g.init_node(
        delay::<i32>(10),
        vec![src.erased()],
        GraphNodeOpts::named("delay_same_wave_terminal"),
    );
    let delayed_seen = collect_shapes::<i32>(&delayed);
    let delayed_data = collect_data(&delayed);

    src.down(vec![Message::Data(Rc::new(7i32)), Message::Complete]);
    assert!(delayed_seen.borrow().is_empty());
    driver.fire_sleepers();
    assert_eq!(*delayed_data.borrow(), vec![7]);
    assert_eq!(*delayed_seen.borrow(), vec!["DATA", "COMPLETE"]);

    let src = g.state_empty::<i32>();
    let buffered = buffer_time(&src, 10);
    let buffered_seen = collect_shapes::<Vec<i32>>(&buffered);
    let buffered_data = collect_data(&buffered);

    src.down(vec![
        Message::Data(Rc::new(9i32)),
        Message::Error("source failed".into()),
    ]);

    assert_eq!(
        *buffered_data.borrow(),
        Vec::<Vec<i32>>::new(),
        "same-wave DATA+ERROR should drop the partial buffer instead of flushing it"
    );
    assert_eq!(*buffered_seen.borrow(), vec!["ERROR"]);
    assert_eq!(
        driver
            .intervals
            .borrow()
            .iter()
            .filter(|tick| tick.active.get())
            .count(),
        0,
        "source ERROR should deactivate the helper interval, not just seal the output"
    );
    driver.tick_intervals();
    assert_eq!(
        *buffered_seen.borrow(),
        vec!["ERROR"],
        "source ERROR should remove the interval helper before future ticks"
    );
}

#[test]
fn time_operator_missing_driver_routes_timer_error() {
    let g = graph();
    let src = g.state_empty::<i32>();
    let delayed = g.init_node(
        delay::<i32>(10),
        vec![src.erased()],
        GraphNodeOpts::named("delay_missing_driver"),
    );
    let seen = collect_shapes::<i32>(&delayed);

    src.set(1);

    assert_eq!(*seen.borrow(), vec!["ERROR"]);
    assert_eq!(delayed.status(), graphrefly::Status::Errored);

    let src = g.state_empty::<i32>();
    let debounced = g.init_node(
        debounce_time::<i32>(10),
        vec![src.erased()],
        GraphNodeOpts::named("debounce_time_missing_driver"),
    );
    let seen = collect_shapes::<i32>(&debounced);
    src.set(1);
    assert_eq!(*seen.borrow(), vec!["ERROR"]);
    assert_eq!(debounced.status(), graphrefly::Status::Errored);

    let src = g.state_empty::<i32>();
    let throttled = g.init_node(
        throttle_time::<i32>(10),
        vec![src.erased()],
        GraphNodeOpts::named("throttle_time_missing_driver"),
    );
    let seen = collect_shapes::<i32>(&throttled);
    src.set(1);
    assert_eq!(*seen.borrow(), vec!["DATA", "ERROR"]);
    assert_eq!(
        throttled.status(),
        graphrefly::Status::Errored,
        "throttle emits the leading value, then the helper timer reports the missing driver"
    );

    let src = g.state_empty::<i32>();
    let audited = g.init_node(
        audit_time::<i32>(10),
        vec![src.erased()],
        GraphNodeOpts::named("audit_time_missing_driver"),
    );
    let seen = collect_shapes::<i32>(&audited);
    src.set(1);
    assert_eq!(*seen.borrow(), vec!["ERROR"]);
    assert_eq!(audited.status(), graphrefly::Status::Errored);
}

#[test]
fn tap_callback_panic_becomes_error_and_seals_output() {
    let g = graph();
    let src = g.init_node(
        from_iter(vec![1i32, 2, 3]),
        vec![],
        GraphNodeOpts::named("tap_panic_src"),
    );
    let tapped = g.init_node(
        tap::<i32>(|v| assert!(*v != 2, "tap callback boom")),
        vec![src.erased()],
        GraphNodeOpts::named("tap_panic"),
    );
    let events = Rc::new(RefCell::new(Vec::<String>::new()));
    let sink = events.clone();
    let _sub = tapped.subscribe(move |msg| match msg {
        Message::Data(value) => sink.borrow_mut().push(format!(
            "DATA:{}",
            value.as_ref().downcast_ref::<i32>().expect("tap emits i32")
        )),
        Message::Error(error) => sink.borrow_mut().push(format!("ERROR:{error}")),
        Message::Complete => sink.borrow_mut().push("COMPLETE".to_owned()),
        Message::Start | Message::Dirty | Message::Resolved | Message::Invalidate => {}
        Message::Pause(_) | Message::Resume(_) | Message::Teardown => {}
    });

    assert_eq!(
        events
            .borrow()
            .iter()
            .map(|event| event.split(':').next().unwrap().to_owned())
            .collect::<Vec<_>>(),
        vec!["DATA", "ERROR"],
        "D30 graph-layer catch should convert tap callback panic into terminal ERROR"
    );
    assert_eq!(tapped.status(), graphrefly::Status::Errored);
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
fn scan_and_merge_catalog_symbols_are_directly_pinned() {
    let g = graph();

    let src = g.init_node(
        from_iter(vec![1i32, 2, 3]),
        vec![],
        GraphNodeOpts::named("scan_src"),
    );
    let scanned = g.init_node(
        scan::<i32, i32>(|acc, v| acc + *v, 0),
        vec![src.erased()],
        GraphNodeOpts::named("scan"),
    );
    let scan_data = Rc::new(RefCell::new(Vec::<i32>::new()));
    let scan_shapes = Rc::new(RefCell::new(Vec::<String>::new()));
    let scan_data_sink = scan_data.clone();
    let scan_shapes_sink = scan_shapes.clone();
    let _scan_keep = scanned.subscribe(move |msg| match msg {
        Message::Data(value) => {
            if let Some(v) = value.as_ref().downcast_ref::<i32>() {
                scan_data_sink.borrow_mut().push(*v);
                scan_shapes_sink.borrow_mut().push("DATA".to_owned());
            }
        }
        Message::Complete => scan_shapes_sink.borrow_mut().push("COMPLETE".to_owned()),
        Message::Error(_) => scan_shapes_sink.borrow_mut().push("ERROR".to_owned()),
        Message::Start | Message::Dirty | Message::Resolved | Message::Invalidate => {}
        Message::Pause(_) | Message::Resume(_) | Message::Teardown => {}
    });
    assert_eq!(*scan_data.borrow(), vec![1, 3, 6]);
    assert_eq!(
        *scan_shapes.borrow(),
        vec!["DATA", "DATA", "DATA", "COMPLETE"]
    );
    assert_eq!(scanned.status(), graphrefly::Status::Completed);

    let left = g.state_empty::<i32>();
    let right = g.state_empty::<i32>();
    let merged = g.init_node(
        graphrefly::merge::<i32>(),
        vec![left.erased(), right.erased()],
        GraphNodeOpts::named("merge"),
    );
    let merged_seen = collect_data(&merged);
    batch(|_| {
        left.set(1);
        right.set(10);
    });
    right.set(11);
    assert_eq!(*merged_seen.borrow(), vec![1, 10, 11]);

    let factories = g
        .describe()
        .nodes
        .into_iter()
        .map(|n| n.factory)
        .collect::<Vec<_>>();
    assert!(factories.contains(&"scan".to_owned()));
    assert!(factories.contains(&"merge".to_owned()));
}

#[test]
fn rescue_edges_absorb_same_wave_error_and_recover_panic_becomes_error() {
    let g = graph();

    let source = g.state_empty::<i32>();
    let recovered = g.init_node(
        rescue::<i32>(|err| {
            assert_eq!(err, "boom");
            42
        }),
        vec![source.erased()],
        GraphNodeOpts::named("rescue_same_wave_error"),
    );
    let recovered_seen = collect_data(&recovered);
    let recovered_shapes = collect_shapes::<i32>(&recovered);

    source.down(vec![
        Message::Data(Rc::new(7i32)),
        Message::Error("boom".into()),
    ]);

    assert_eq!(*recovered_seen.borrow(), vec![7, 42]);
    assert_eq!(*recovered_shapes.borrow(), vec!["DATA", "DATA"]);
    assert_ne!(recovered.status(), graphrefly::Status::Completed);
    assert_ne!(recovered.status(), graphrefly::Status::Errored);

    let source = g.state_empty::<i32>();
    let recovered = g.init_node(
        catch_error::<i32>(|_| panic!("recover callback boom")),
        vec![source.erased()],
        GraphNodeOpts::named("catch_error_recover_panic"),
    );
    let shapes = collect_shapes::<i32>(&recovered);

    source.down(vec![Message::Error("boom".into())]);

    assert_eq!(*shapes.borrow(), vec!["ERROR"]);
    assert_eq!(recovered.status(), graphrefly::Status::Errored);
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

    let empty_source = g.state_empty::<i32>();
    let empty_notifier = g.state_empty::<()>();
    let empty_buffered = g.init_node(
        buffer::<i32>(),
        vec![empty_source.erased(), empty_notifier.erased()],
        GraphNodeOpts::named("buffer_empty_flush"),
    );
    let empty_buffer_seen = collect_data(&empty_buffered);
    empty_notifier.set(());
    assert_eq!(
        *empty_buffer_seen.borrow(),
        vec![Vec::<i32>::new()],
        "buffer should flush an empty Vec on notifier DATA, like the TS catalog"
    );

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

    let source = g.state(3i32);
    let notifier = g.state_empty::<()>();
    let buffered = g.init_node(
        buffer::<i32>(),
        vec![source.erased(), notifier.erased()],
        GraphNodeOpts::named("buffer_multi_notifier_wave"),
    );
    let buffer_seen = collect_data(&buffered);
    batch(|_| {
        source.set(4);
        notifier.down(vec![Message::Data(Rc::new(())), Message::Data(Rc::new(()))]);
    });
    assert_eq!(
        *buffer_seen.borrow(),
        vec![vec![3, 4]],
        "a notifier wave with one or more DATA occurrences flushes the current window once"
    );

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
fn higher_order_repeat_replays_fresh_factory_rounds_and_completes() {
    let g = graph();
    let round = Rc::new(Cell::new(0i32));
    let repeated = g.init_node(
        repeat::<i32>(
            {
                let g = g.clone();
                let round = round.clone();
                move || {
                    let next = round.get() + 1;
                    round.set(next);
                    g.init_node(
                        from_iter(vec![next * 10, next * 10 + 1]),
                        vec![],
                        GraphNodeOpts::named(format!("repeat_inner_{next}")),
                    )
                }
            },
            3,
        ),
        vec![],
        GraphNodeOpts::named("repeat"),
    );
    let seen = Rc::new(RefCell::new(Vec::<String>::new()));
    let data = Rc::new(RefCell::new(Vec::<i32>::new()));
    let seen_sink = seen.clone();
    let data_sink = data.clone();
    let _keep = repeated.subscribe(move |msg| match msg {
        Message::Data(value) => {
            if let Some(v) = value.as_ref().downcast_ref::<i32>() {
                data_sink.borrow_mut().push(*v);
                seen_sink.borrow_mut().push("DATA".to_owned());
            }
        }
        Message::Complete => seen_sink.borrow_mut().push("COMPLETE".to_owned()),
        Message::Error(_) => seen_sink.borrow_mut().push("ERROR".to_owned()),
        Message::Start | Message::Dirty | Message::Resolved | Message::Invalidate => {}
        Message::Pause(_) | Message::Resume(_) | Message::Teardown => {}
    });

    assert_eq!(*data.borrow(), vec![10, 11, 20, 21, 30, 31]);
    assert_eq!(
        *seen.borrow(),
        vec!["DATA", "DATA", "DATA", "DATA", "DATA", "DATA", "COMPLETE"],
        "same-wave inner DATA must forward before the final repeat COMPLETE"
    );
    assert_eq!(
        round.get(),
        3,
        "repeat must call the factory once per round"
    );
    assert_eq!(repeated.status(), graphrefly::Status::Completed);

    let factories = g
        .describe()
        .nodes
        .into_iter()
        .map(|n| n.factory)
        .collect::<Vec<_>>();
    assert!(factories.contains(&"repeat".to_owned()));
}

#[test]
#[should_panic(expected = "repeat: count must be positive")]
fn higher_order_repeat_rejects_zero_count() {
    let _ = repeat::<i32>(|| Node::producer(|_| {}), 0);
}

#[test]
fn higher_order_repeat_inner_error_detaches_and_seals_output() {
    let g = graph();
    let cleanup = Rc::new(Cell::new(0usize));
    let inners = Rc::new(RefCell::new(Vec::<Node<i32>>::new()));
    let repeated = g.init_node(
        repeat::<i32>(
            {
                let g = g.clone();
                let cleanup = cleanup.clone();
                let inners = inners.clone();
                move || {
                    let cleanup = cleanup.clone();
                    let inner = g.producer(move |ctx| {
                        ctx.on_deactivation({
                            let cleanup = cleanup.clone();
                            move || cleanup.set(cleanup.get() + 1)
                        });
                    });
                    inners.borrow_mut().push(inner.clone());
                    inner
                }
            },
            2,
        ),
        vec![],
        GraphNodeOpts::named("repeat_error"),
    );
    let shapes = collect_shapes::<i32>(&repeated);

    assert_eq!(inners.borrow().len(), 1);
    inners.borrow()[0].down(vec![Message::Data(Rc::new(1i32))]);
    inners.borrow()[0].down(vec![Message::Error("inner boom".into())]);

    assert_eq!(*shapes.borrow(), vec!["DATA", "ERROR"]);
    assert_eq!(cleanup.get(), 1, "inner ERROR should detach the live round");
    assert_eq!(repeated.status(), graphrefly::Status::Errored);

    let after_error = shapes.borrow().len();
    inners.borrow()[0].down(vec![Message::Data(Rc::new(2i32))]);
    assert_eq!(
        shapes.borrow().len(),
        after_error,
        "repeat output should be sealed after inner ERROR"
    );
}

#[test]
fn higher_order_repeat_factory_panic_errors_after_cleaning_completed_round() {
    let g = graph();
    let calls = Rc::new(Cell::new(0usize));
    let repeated = g.init_node(
        repeat::<i32>(
            {
                let g = g.clone();
                let calls = calls.clone();
                move || {
                    let next = calls.get() + 1;
                    calls.set(next);
                    assert!(next != 2, "repeat factory boom");
                    g.init_node(
                        from_iter(vec![7i32]),
                        vec![],
                        GraphNodeOpts::named(format!("repeat_panic_inner_{next}")),
                    )
                }
            },
            2,
        ),
        vec![],
        GraphNodeOpts::named("repeat_panic"),
    );
    let shapes = collect_shapes::<i32>(&repeated);

    assert_eq!(*shapes.borrow(), vec!["DATA", "ERROR"]);
    assert_eq!(calls.get(), 2);
    assert_eq!(repeated.status(), graphrefly::Status::Errored);
}

#[test]
fn higher_order_repeat_live_inner_is_described_and_replaced_per_round() {
    let g = graph();
    let inners = Rc::new(RefCell::new(Vec::<Node<i32>>::new()));
    let repeated = g.init_node(
        repeat::<i32>(
            {
                let g = g.clone();
                let inners = inners.clone();
                move || {
                    let next = inners.borrow().len() + 1;
                    let inner = g
                        .producer_opts(|_| {}, GraphNodeOpts::named(format!("repeat_live_{next}")));
                    inners.borrow_mut().push(inner.clone());
                    inner
                }
            },
            2,
        ),
        vec![],
        GraphNodeOpts::named("repeat_live"),
    );
    let _seen = collect_shapes::<i32>(&repeated);

    let snap = g.describe();
    assert!(snap.nodes.iter().any(|n| n.id == "repeat_live_1"));
    assert!(snap
        .edges
        .iter()
        .any(|e| e.from == "repeat_live_1" && e.to == "repeat_live"));

    let first_inner = inners.borrow()[0].clone();
    first_inner.down(vec![Message::Complete]);
    let snap = g.describe();
    assert!(snap.nodes.iter().any(|n| n.id == "repeat_live_2"));
    assert!(snap
        .edges
        .iter()
        .any(|e| e.from == "repeat_live_2" && e.to == "repeat_live"));
    assert!(!snap
        .edges
        .iter()
        .any(|e| e.from == "repeat_live_1" && e.to == "repeat_live"));
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
fn higher_order_merge_map_with_options_limits_live_inners() {
    let g = graph();
    let source = g.state_empty::<i32>();
    let inners = Rc::new(RefCell::new(Vec::<Node<i32>>::new()));
    let merged = g.init_node(
        merge_map_with_options::<i32, i32>(
            {
                let g = g.clone();
                let inners = inners.clone();
                move |_v| {
                    let inner = g.state_empty::<i32>();
                    inners.borrow_mut().push(inner.clone());
                    inner
                }
            },
            MergeMapOptions {
                concurrent: Some(2),
            },
        ),
        vec![source.erased()],
        GraphNodeOpts::named("merge_map_bounded"),
    );
    let seen = collect_data(&merged);

    source.down(vec![
        Message::Data(Rc::new(1i32)),
        Message::Data(Rc::new(2i32)),
        Message::Data(Rc::new(3i32)),
        Message::Complete,
    ]);
    assert_eq!(
        inners.borrow().len(),
        2,
        "bounded merge_map should not project queued work until an inner completes"
    );
    let inner_0 = inners.borrow()[0].clone();
    let inner_1 = inners.borrow()[1].clone();
    inner_0.set(10);
    inner_1.set(20);
    assert_eq!(*seen.borrow(), vec![10, 20]);
    assert_ne!(merged.status(), graphrefly::Status::Completed);

    inner_0.down(vec![Message::Complete]);
    assert_eq!(
        inners.borrow().len(),
        3,
        "one completed inner frees exactly one bounded merge_map slot"
    );
    let inner_2 = inners.borrow()[2].clone();
    inner_2.set(30);
    assert_eq!(*seen.borrow(), vec![10, 20, 30]);
    inner_1.down(vec![Message::Complete]);
    inner_2.down(vec![Message::Complete]);
    assert_eq!(merged.status(), graphrefly::Status::Completed);
}

#[test]
fn higher_order_merge_map_with_options_skips_just_completed_reused_inner() {
    let g = graph();
    let source = g.state_empty::<i32>();
    let inner = g.state_empty::<i32>();
    let merged = g.init_node(
        merge_map_with_options::<i32, i32>(
            {
                let inner = inner.clone();
                move |_v| inner.clone()
            },
            MergeMapOptions {
                concurrent: Some(1),
            },
        ),
        vec![source.erased()],
        GraphNodeOpts::named("merge_map_bounded_reused_inner"),
    );
    let seen = collect_data(&merged);

    source.down(vec![
        Message::Data(Rc::new(1i32)),
        Message::Data(Rc::new(2i32)),
        Message::Complete,
    ]);
    inner.set(10);
    assert_eq!(*seen.borrow(), vec![10]);
    assert_ne!(merged.status(), graphrefly::Status::Completed);

    inner.down(vec![Message::Complete]);
    assert_eq!(merged.status(), graphrefly::Status::Completed);
}

#[test]
fn higher_order_merge_map_dedupes_reused_live_inner() {
    let g = graph();
    let source = g.state_empty::<i32>();
    let shared = g.state_empty::<i32>();
    let merged = g.init_node(
        merge_map::<i32, i32>({
            let shared = shared.clone();
            move |_| shared.clone()
        }),
        vec![source.erased()],
        GraphNodeOpts::named("merge_reused_inner"),
    );
    let shapes = collect_shapes::<i32>(&merged);
    let data = collect_data(&merged);

    source.down(vec![
        Message::Data(Rc::new(1i32)),
        Message::Data(Rc::new(2i32)),
        Message::Complete,
    ]);
    shared.set(10);
    shared.down(vec![Message::Complete]);

    assert_eq!(*data.borrow(), vec![10]);
    assert_eq!(
        *shapes.borrow(),
        vec!["DATA", "COMPLETE"],
        "a projector returning an already-live inner should not leave a duplicate tracked inner"
    );
}

#[test]
fn higher_order_switch_map_dedupes_reused_live_inner() {
    let g = graph();
    let source = g.state_empty::<i32>();
    let cleanup = Rc::new(Cell::new(0usize));
    let shared = g.producer({
        let cleanup = cleanup.clone();
        move |ctx| {
            ctx.on_deactivation({
                let cleanup = cleanup.clone();
                move || cleanup.set(cleanup.get() + 1)
            });
        }
    });
    let switched = g.init_node(
        switch_map::<i32, i32>({
            let shared = shared.clone();
            move |_| shared.clone()
        }),
        vec![source.erased()],
        GraphNodeOpts::named("switch_reused_inner"),
    );
    let shapes = collect_shapes::<i32>(&switched);
    let data = collect_data(&switched);

    source.down(vec![Message::Data(Rc::new(1i32))]);
    source.down(vec![Message::Data(Rc::new(2i32)), Message::Complete]);
    shared.set(10);
    shared.down(vec![Message::Complete]);

    assert_eq!(*data.borrow(), vec![10]);
    assert_eq!(
        cleanup.get(),
        1,
        "the shared inner should deactivate only when it completes, not on the second source wave"
    );
    assert_eq!(
        *shapes.borrow(),
        vec!["DATA", "COMPLETE"],
        "switch_map should not remove and re-add the same already-live projected inner"
    );
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

#[test]
fn higher_order_switch_projector_panic_detaches_previous_inner() {
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
    let switched = g.init_node(
        switch_map::<i32, i32>({
            let inner = inner.clone();
            move |v| {
                assert!(*v != 2, "switch projector boom");
                inner.clone()
            }
        }),
        vec![source.erased()],
        GraphNodeOpts::named("switch_panic"),
    );
    let shapes = collect_shapes::<i32>(&switched);

    source.down(vec![Message::Data(Rc::new(1i32))]);
    inner.down(vec![Message::Data(Rc::new(10i32))]);
    source.down(vec![Message::Data(Rc::new(2i32))]);

    assert_eq!(switched.status(), graphrefly::Status::Errored);
    assert_eq!(
        cleanup.get(),
        1,
        "switch projector panic should detach the previous live inner"
    );
    assert_eq!(*shapes.borrow(), vec!["DATA", "ERROR"]);

    let after_error = shapes.borrow().len();
    inner.down(vec![Message::Data(Rc::new(11i32))]);
    source.down(vec![Message::Data(Rc::new(3i32))]);
    assert_eq!(
        shapes.borrow().len(),
        after_error,
        "terminal switch_map output should be sealed after projector panic"
    );
}
