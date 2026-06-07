//! Source factories (D43/D40/D111).
//!
//! Sync sources run directly in the source body. Async/time sources stay at the
//! source/driver boundary: they schedule work on the graph-local driver and emit
//! later through `DeferredCtx`, preserving the sync wave core.

use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;
use std::error::Error;
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::Command;
use std::rc::Rc;
use std::sync::mpsc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_core::Stream;
use notify::event::ModifyKind;
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};

use crate::async_driver::DriverCancel;
use crate::environment::{
    HttpRequest, HttpResponse, ProcessCommand, ProcessResult, SseDriverEvent, SseEvent, SseRequest,
    WebSocketDriverEvent, WebSocketEvent, WebSocketRequest,
};
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

/// run_process: execute one process via the graph-local environment process driver.
///
/// Completion emits one [`ProcessResult`] DATA and COMPLETE. Non-zero process
/// exits remain DATA. Driver/spawn failures become protocol ERROR.
pub fn run_process<I, S>(program: impl Into<String>, args: I) -> Operator<ProcessResult>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    run_process_with_options(ProcessCommand::new(program).args(args))
}

/// from_process: source-name alias for [`run_process`].
pub fn from_process<I, S>(program: impl Into<String>, args: I) -> Operator<ProcessResult>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let command = ProcessCommand::new(program).args(args);
    process_source("fromProcess", command)
}

/// Configurable form of [`run_process`].
pub fn run_process_with_options(command: ProcessCommand) -> Operator<ProcessResult> {
    process_source("runProcess", command)
}

fn process_source(factory: &'static str, command: ProcessCommand) -> Operator<ProcessResult> {
    assert!(
        !command.program.is_empty(),
        "{factory}: program must be a non-empty string"
    );
    Operator::with_opts(
        factory,
        NodeOpts {
            pool: crate::dispatcher::PoolKind::Async,
            pausable: Pausable::False,
            ..NodeOpts::default()
        },
        move |ctx| {
            let Some(driver) = ctx.environment().process_driver() else {
                ctx.down(vec![Message::Error(
                    format!("{factory}: missing process driver").into(),
                )]);
                return;
            };
            let active = Rc::new(Cell::new(true));
            let cancel_slot: Rc<RefCell<Option<DriverCancel>>> = Rc::new(RefCell::new(None));
            let cleanup_active = active.clone();
            let cleanup_cancel = cancel_slot.clone();
            ctx.on_deactivation(move || {
                cleanup_driver_work(&cleanup_active, &cleanup_cancel);
            });
            let out = ctx.defer();
            let callback_active = active.clone();
            let callback_cancel = cancel_slot.clone();
            let cancel = driver.run(
                command.clone(),
                Box::new(move |result| {
                    if !callback_active.get() {
                        return;
                    }
                    cleanup_driver_work(&callback_active, &callback_cancel);
                    match result {
                        Ok(result) => {
                            out.down(vec![Message::Data(Rc::new(result)), Message::Complete]);
                        }
                        Err(error) => {
                            out.down(vec![Message::Error(error)]);
                        }
                    }
                }),
            );
            install_driver_cancel(&active, &cancel_slot, cancel);
        },
    )
}

pub fn from_http(url: impl Into<String>) -> Operator<HttpResponse> {
    from_http_with_options(HttpRequest::get(url))
}

pub fn from_http_with_options(request: HttpRequest) -> Operator<HttpResponse> {
    assert!(!request.url.is_empty(), "fromHttp: url must be non-empty");
    assert!(
        !request.method.is_empty(),
        "fromHttp: method must be non-empty"
    );
    Operator::with_opts(
        "fromHttp",
        NodeOpts {
            pool: crate::dispatcher::PoolKind::Async,
            pausable: Pausable::False,
            ..NodeOpts::default()
        },
        move |ctx| {
            let Some(driver) = ctx.environment().http_driver() else {
                ctx.down(vec![Message::Error("fromHttp: missing http driver".into())]);
                return;
            };
            let active = Rc::new(Cell::new(true));
            let cancel_slot: Rc<RefCell<Option<DriverCancel>>> = Rc::new(RefCell::new(None));
            let cleanup_active = active.clone();
            let cleanup_cancel = cancel_slot.clone();
            ctx.on_deactivation(move || {
                cleanup_driver_work(&cleanup_active, &cleanup_cancel);
            });
            let out = ctx.defer();
            let callback_active = active.clone();
            let callback_cancel = cancel_slot.clone();
            let cancel = driver.request(
                request.clone(),
                Box::new(move |result| {
                    if !callback_active.get() {
                        return;
                    }
                    cleanup_driver_work(&callback_active, &callback_cancel);
                    match result {
                        Ok(response) => {
                            out.down(vec![Message::Data(Rc::new(response)), Message::Complete]);
                        }
                        Err(error) => out.down(vec![Message::Error(error)]),
                    }
                }),
            );
            install_driver_cancel(&active, &cancel_slot, cancel);
        },
    )
}

pub fn from_sse(url: impl Into<String>) -> Operator<SseEvent> {
    from_sse_with_options(SseRequest::new(url))
}

pub fn from_sse_with_options(request: SseRequest) -> Operator<SseEvent> {
    assert!(!request.url.is_empty(), "fromSSE: url must be non-empty");
    Operator::with_opts(
        "fromSSE",
        NodeOpts {
            pool: crate::dispatcher::PoolKind::Async,
            pausable: Pausable::False,
            ..NodeOpts::default()
        },
        move |ctx| {
            let Some(driver) = ctx.environment().sse_driver() else {
                ctx.down(vec![Message::Error("fromSSE: missing sse driver".into())]);
                return;
            };
            let active = Rc::new(Cell::new(true));
            let cancel_slot: Rc<RefCell<Option<DriverCancel>>> = Rc::new(RefCell::new(None));
            let cleanup_active = active.clone();
            let cleanup_cancel = cancel_slot.clone();
            ctx.on_deactivation(move || {
                cleanup_driver_work(&cleanup_active, &cleanup_cancel);
            });
            let out = ctx.defer();
            let callback_active = active.clone();
            let callback_cancel = cancel_slot.clone();
            let callback = Rc::new(move |event| match event {
                SseDriverEvent::Event(event) => {
                    if callback_active.get() {
                        out.down(vec![Message::Data(Rc::new(event))]);
                    }
                }
                SseDriverEvent::Error(error) => {
                    if callback_active.get() {
                        cleanup_driver_work(&callback_active, &callback_cancel);
                        out.down(vec![Message::Error(error)]);
                    }
                }
                SseDriverEvent::Complete => {
                    if callback_active.get() {
                        cleanup_driver_work(&callback_active, &callback_cancel);
                        out.down(vec![Message::Complete]);
                    }
                }
            });
            let cancel = driver.connect(request.clone(), callback);
            install_driver_cancel(&active, &cancel_slot, cancel);
        },
    )
}

pub fn from_websocket(url: impl Into<String>) -> Operator<WebSocketEvent> {
    from_websocket_with_options(WebSocketRequest::new(url))
}

pub fn from_websocket_with_options(request: WebSocketRequest) -> Operator<WebSocketEvent> {
    assert!(
        !request.url.is_empty(),
        "fromWebSocket: url must be non-empty"
    );
    Operator::with_opts(
        "fromWebSocket",
        NodeOpts {
            pool: crate::dispatcher::PoolKind::Async,
            pausable: Pausable::False,
            ..NodeOpts::default()
        },
        move |ctx| {
            let Some(driver) = ctx.environment().websocket_driver() else {
                ctx.down(vec![Message::Error(
                    "fromWebSocket: missing websocket driver".into(),
                )]);
                return;
            };
            let active = Rc::new(Cell::new(true));
            let cancel_slot: Rc<RefCell<Option<DriverCancel>>> = Rc::new(RefCell::new(None));
            let cleanup_active = active.clone();
            let cleanup_cancel = cancel_slot.clone();
            ctx.on_deactivation(move || {
                cleanup_driver_work(&cleanup_active, &cleanup_cancel);
            });
            let out = ctx.defer();
            let callback_active = active.clone();
            let callback_cancel = cancel_slot.clone();
            let callback = Rc::new(move |event| match event {
                WebSocketDriverEvent::Event(event) => {
                    if callback_active.get() {
                        out.down(vec![Message::Data(Rc::new(event))]);
                    }
                }
                WebSocketDriverEvent::Error(error) => {
                    if callback_active.get() {
                        cleanup_driver_work(&callback_active, &callback_cancel);
                        out.down(vec![Message::Error(error)]);
                    }
                }
                WebSocketDriverEvent::Complete => {
                    if callback_active.get() {
                        cleanup_driver_work(&callback_active, &callback_cancel);
                        out.down(vec![Message::Complete]);
                    }
                }
            });
            let cancel = driver.connect(request.clone(), callback);
            install_driver_cancel(&active, &cancel_slot, cancel);
        },
    )
}

/// Filesystem event kind emitted by [`from_fs_watch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FsEventKind {
    Change,
    Rename,
    Create,
    Delete,
}

/// Filesystem event emitted by [`from_fs_watch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsEvent {
    pub kind: FsEventKind,
    pub path: PathBuf,
    pub root: PathBuf,
    pub relative_path: PathBuf,
}

/// Options for [`from_fs_watch_with_options`].
#[derive(Debug, Clone)]
pub struct FromFsWatchOptions {
    pub recursive: bool,
    pub debounce_ms: u64,
    pub initial_scan: bool,
    pub include: Vec<String>,
    pub exclude: Vec<String>,
}

/// Minimal five-field cron schedule: minute hour day-of-month month day-of-week.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronSchedule {
    pub minutes: BTreeSet<u8>,
    pub hours: BTreeSet<u8>,
    pub days_of_month: BTreeSet<u8>,
    pub months: BTreeSet<u8>,
    /// Sunday = 0, matching common five-field cron and JavaScript Date#getDay.
    pub days_of_week: BTreeSet<u8>,
}

/// Fieldized instant used by [`matches_cron`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CronInstant {
    pub year: i32,
    pub month: u8,
    pub day_of_month: u8,
    pub hour: u8,
    pub minute: u8,
    /// Sunday = 0.
    pub day_of_week: u8,
}

impl CronInstant {
    #[must_use]
    pub fn new(
        year: i32,
        month: u8,
        day_of_month: u8,
        hour: u8,
        minute: u8,
        day_of_week: u8,
    ) -> Self {
        Self {
            year,
            month,
            day_of_month,
            hour,
            minute,
            day_of_week,
        }
    }
}

/// Value emitted by [`from_cron`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronTick {
    pub instant: CronInstant,
    /// Decimal nanoseconds since the Unix epoch.
    pub timestamp_ns: String,
}

impl CronTick {
    #[must_use]
    pub fn new(instant: CronInstant, timestamp_ns: impl Into<String>) -> Self {
        Self {
            instant,
            timestamp_ns: timestamp_ns.into(),
        }
    }
}

#[derive(Clone)]
pub struct FromCronOptions {
    pub tick_ms: u64,
    pub now: Option<Rc<dyn Fn() -> CronTick>>,
}

impl Default for FromCronOptions {
    fn default() -> Self {
        Self {
            tick_ms: 60_000,
            now: None,
        }
    }
}

/// Error returned by [`parse_cron`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronParseError {
    message: String,
}

impl CronParseError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for CronParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for CronParseError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GitHookType {
    PostCommit,
}

/// Git event emitted by [`from_git_hook`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitEvent {
    pub hook: GitHookType,
    pub commit: String,
    pub files: Vec<String>,
    pub message: String,
    pub author: String,
    pub timestamp_ns: String,
}

#[derive(Debug, Clone)]
pub struct FromGitHookOptions {
    pub poll_ms: u64,
    pub include: Vec<String>,
    pub exclude: Vec<String>,
    pub max_consecutive_errors: usize,
}

impl Default for FromGitHookOptions {
    fn default() -> Self {
        Self {
            poll_ms: 5_000,
            include: Vec::new(),
            exclude: Vec::new(),
            max_consecutive_errors: 1,
        }
    }
}

#[derive(Debug, Clone)]
struct GitPollResult {
    head: String,
    files: Vec<String>,
    message: String,
    author: String,
}

impl Default for FromFsWatchOptions {
    fn default() -> Self {
        Self {
            recursive: false,
            debounce_ms: 100,
            initial_scan: false,
            include: Vec::new(),
            exclude: vec![
                "**/node_modules/**".to_owned(),
                "**/.git/**".to_owned(),
                "**/dist/**".to_owned(),
            ],
        }
    }
}

#[derive(Debug, Clone)]
struct WatchRoot {
    root: PathBuf,
    rel_base: PathBuf,
}

/// from_fs_watch: filesystem watcher source.
///
/// Filesystem callbacks enqueue host events; the graph-local async driver drains
/// that queue back through `DeferredCtx`, keeping graph state on the local
/// source boundary. The visible factory name is `fromFSWatch`, matching the
/// clean-slate TS source catalog without making this a parity surface.
pub fn from_fs_watch<P>(paths: impl IntoIterator<Item = P>) -> Operator<FsEvent>
where
    P: Into<PathBuf>,
{
    from_fs_watch_with_options(paths, FromFsWatchOptions::default())
}

/// Configurable form of [`from_fs_watch`].
pub fn from_fs_watch_with_options<P>(
    paths: impl IntoIterator<Item = P>,
    opts: FromFsWatchOptions,
) -> Operator<FsEvent>
where
    P: Into<PathBuf>,
{
    let roots: Vec<WatchRoot> = paths
        .into_iter()
        .map(|path| watch_root(absolutize(path.into())))
        .collect();
    assert!(!roots.is_empty(), "from_fs_watch: paths must not be empty");
    Operator::with_opts(
        "fromFSWatch",
        NodeOpts {
            pausable: Pausable::False,
            ..NodeOpts::default()
        },
        move |ctx| {
            let Some(driver) = ctx.local_async_driver() else {
                ctx.down(vec![Message::Error(
                    "fromFSWatch: missing local async driver".into(),
                )]);
                return;
            };

            let (tx, rx) = mpsc::channel::<notify::Result<notify::Event>>();
            let mut watcher = match notify::recommended_watcher(tx) {
                Ok(watcher) => watcher,
                Err(error) => {
                    ctx.down(vec![Message::Error(error.into())]);
                    return;
                }
            };
            let mode = if opts.recursive {
                RecursiveMode::Recursive
            } else {
                RecursiveMode::NonRecursive
            };
            for root in &roots {
                if let Err(error) = watcher.watch(&root.root, mode) {
                    ctx.down(vec![Message::Error(error.into())]);
                    return;
                }
            }

            let out = ctx.defer();
            let active = Rc::new(Cell::new(true));
            let active_tick = active.clone();
            let roots_tick = roots.clone();
            let opts_tick = opts.clone();
            let rx = Rc::new(RefCell::new(rx));
            let watcher_slot = Rc::new(RefCell::new(Some(watcher)));
            let cancel_slot: Rc<RefCell<Option<DriverCancel>>> = Rc::new(RefCell::new(None));
            let watcher_tick = watcher_slot.clone();
            let cancel_tick = cancel_slot.clone();
            let period = Duration::from_millis(opts.debounce_ms.max(1));
            let cancel_poll = driver.interval(
                period,
                Rc::new(move || {
                    if !active_tick.get() {
                        return;
                    }
                    let mut messages: Vec<Message<AnyValue>> = Vec::new();
                    loop {
                        match rx.borrow_mut().try_recv() {
                            Ok(Ok(event)) => {
                                for fs_event in event_to_fs_events(event, &roots_tick, &opts_tick) {
                                    let out: AnyValue = Rc::new(fs_event);
                                    messages.push(Message::Data(out));
                                }
                            }
                            Ok(Err(error)) => {
                                cleanup_fs_watch(&active_tick, &cancel_tick, &watcher_tick);
                                out.down(vec![Message::Error(error.into())]);
                                return;
                            }
                            Err(mpsc::TryRecvError::Empty) => break,
                            Err(mpsc::TryRecvError::Disconnected) => {
                                active_tick.set(false);
                                break;
                            }
                        }
                    }
                    if !messages.is_empty() {
                        out.down(messages);
                    }
                }),
            );
            *cancel_slot.borrow_mut() = Some(cancel_poll);
            let cleanup_active = active.clone();
            let cleanup_cancel = cancel_slot.clone();
            let cleanup_watcher = watcher_slot.clone();
            ctx.on_deactivation(move || {
                cleanup_fs_watch(&cleanup_active, &cleanup_cancel, &cleanup_watcher);
            });

            if opts.initial_scan {
                let initial = initial_scan_events(&roots, &opts);
                if !initial.is_empty() {
                    ctx.down(
                        initial
                            .into_iter()
                            .map(|event| {
                                let out: AnyValue = Rc::new(event);
                                Message::Data(out)
                            })
                            .collect(),
                    );
                }
            }
        },
    )
}

fn initial_scan_events(roots: &[WatchRoot], opts: &FromFsWatchOptions) -> Vec<FsEvent> {
    let mut out = Vec::new();
    for root in roots {
        scan_root(root, opts.recursive, opts, &mut |path| {
            if let Some(event) = fs_event_for(FsEventKind::Create, path, root, opts) {
                out.push(event);
            }
        });
    }
    out
}

fn scan_root(
    root: &WatchRoot,
    recursive: bool,
    opts: &FromFsWatchOptions,
    visit: &mut impl FnMut(&Path),
) {
    let Ok(meta) = fs::symlink_metadata(&root.root) else {
        return;
    };
    if meta.file_type().is_symlink() {
        return;
    }
    if meta.is_file() {
        visit(&root.root);
        return;
    }
    if !meta.is_dir() {
        return;
    }
    let Ok(entries) = fs::read_dir(&root.root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(meta) = fs::symlink_metadata(&path) else {
            continue;
        };
        if meta.file_type().is_symlink() {
            continue;
        }
        if meta.is_file() {
            visit(&path);
        } else if recursive && meta.is_dir() {
            let relative = path
                .strip_prefix(&root.rel_base)
                .map_or_else(|_| path.clone(), Path::to_path_buf);
            if is_excluded_path(&path, &relative, opts) {
                continue;
            }
            scan_root(
                &watch_root_with_base(path, root.rel_base.clone()),
                recursive,
                opts,
                visit,
            );
        }
    }
}

fn event_to_fs_events(
    event: notify::Event,
    roots: &[WatchRoot],
    opts: &FromFsWatchOptions,
) -> Vec<FsEvent> {
    let kind = match event.kind {
        EventKind::Create(_) => Some(FsEventKind::Create),
        EventKind::Remove(_) => Some(FsEventKind::Delete),
        EventKind::Modify(ModifyKind::Name(_)) => Some(FsEventKind::Rename),
        EventKind::Modify(_) | EventKind::Any | EventKind::Other => Some(FsEventKind::Change),
        EventKind::Access(_) => None,
    };
    let Some(kind) = kind else {
        return Vec::new();
    };
    event
        .paths
        .iter()
        .filter_map(|path| {
            let abs = absolutize(path.clone());
            let root = roots
                .iter()
                .filter(|root| abs.starts_with(root.root.as_path()))
                .max_by_key(|root| root.root.as_os_str().len())?;
            fs_event_for(kind.clone(), &abs, root, opts)
        })
        .collect()
}

fn fs_event_for(
    kind: FsEventKind,
    path: &Path,
    root: &WatchRoot,
    opts: &FromFsWatchOptions,
) -> Option<FsEvent> {
    let relative = path
        .strip_prefix(&root.rel_base)
        .map_or_else(|_| path.to_path_buf(), Path::to_path_buf);
    if !accepts_path(path, &relative, opts) {
        return None;
    }
    Some(FsEvent {
        kind,
        path: path.to_path_buf(),
        root: root.root.clone(),
        relative_path: relative,
    })
}

fn accepts_path(path: &Path, relative: &Path, opts: &FromFsWatchOptions) -> bool {
    if is_excluded_path(path, relative, opts) {
        return false;
    }
    let abs = normalize_path(path);
    let rel = normalize_path(relative);
    opts.include.is_empty()
        || opts
            .include
            .iter()
            .any(|pattern| wildcard_match(pattern, &abs) || wildcard_match(pattern, &rel))
}

fn is_excluded_path(path: &Path, relative: &Path, opts: &FromFsWatchOptions) -> bool {
    let abs = normalize_path(path);
    let rel = normalize_path(relative);
    opts.exclude.iter().any(|pattern| {
        wildcard_match(pattern, &abs)
            || wildcard_match(pattern, &rel)
            || wildcard_match(pattern, &format!("{abs}/"))
            || (!rel.is_empty() && wildcard_match(pattern, &format!("{rel}/")))
    })
}

fn watch_root(root: PathBuf) -> WatchRoot {
    let rel_base = if root.is_file() {
        root.parent().unwrap_or(root.as_path()).to_path_buf()
    } else {
        root.clone()
    };
    WatchRoot { root, rel_base }
}

fn watch_root_with_base(root: PathBuf, rel_base: PathBuf) -> WatchRoot {
    WatchRoot { root, rel_base }
}

fn cleanup_fs_watch(
    active: &Rc<Cell<bool>>,
    cancel_slot: &Rc<RefCell<Option<DriverCancel>>>,
    watcher_slot: &Rc<RefCell<Option<RecommendedWatcher>>>,
) {
    active.set(false);
    if let Some(cancel) = cancel_slot.borrow_mut().take() {
        cancel();
    }
    watcher_slot.borrow_mut().take();
}

fn cleanup_driver_interval(
    active: &Rc<Cell<bool>>,
    cancel_slot: &Rc<RefCell<Option<DriverCancel>>>,
) {
    active.set(false);
    if let Some(cancel) = cancel_slot.borrow_mut().take() {
        cancel();
    }
}

fn cleanup_driver_work(active: &Rc<Cell<bool>>, cancel_slot: &Rc<RefCell<Option<DriverCancel>>>) {
    active.set(false);
    if let Some(cancel) = cancel_slot.borrow_mut().take() {
        cancel();
    }
}

fn install_driver_cancel(
    active: &Rc<Cell<bool>>,
    cancel_slot: &Rc<RefCell<Option<DriverCancel>>>,
    cancel: DriverCancel,
) {
    if active.get() {
        *cancel_slot.borrow_mut() = Some(cancel);
    } else {
        cancel();
    }
}

fn absolutize(path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    }
}

fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn wildcard_match(pattern: &str, value: &str) -> bool {
    let (mut p, mut v) = (0usize, 0usize);
    let (mut star, mut star_v) = (None, 0usize);
    let p_bytes = pattern.as_bytes();
    let v_bytes = value.as_bytes();
    while v < v_bytes.len() {
        if p < p_bytes.len() && p_bytes[p] == b'*' {
            star = Some(p);
            p += 1;
            star_v = v;
        } else if p < p_bytes.len() && p_bytes[p] == v_bytes[v] {
            p += 1;
            v += 1;
        } else if let Some(star_p) = star {
            p = star_p + 1;
            star_v += 1;
            v = star_v;
        } else {
            return false;
        }
    }
    while p < p_bytes.len() && p_bytes[p] == b'*' {
        p += 1;
    }
    p == p_bytes.len()
}

fn parse_cron_field(field: &str, min: u8, max: u8) -> Result<BTreeSet<u8>, CronParseError> {
    if field.is_empty() {
        return Err(CronParseError::new("Invalid cron field: empty"));
    }
    let mut out = BTreeSet::new();
    for part in field.split(',') {
        if part.is_empty() {
            return Err(CronParseError::new(format!("Invalid cron field: {field}")));
        }
        let step_parts: Vec<_> = part.split('/').collect();
        if step_parts.len() > 2 || step_parts[0].is_empty() {
            return Err(CronParseError::new(format!("Invalid cron step: {part}")));
        }
        let step = if step_parts.len() == 2 {
            if step_parts[1].is_empty() {
                return Err(CronParseError::new(format!("Invalid cron step: {part}")));
            }
            parse_cron_int(step_parts[1], format!("Invalid cron step: {part}"))?
        } else {
            1
        };
        if step < 1 {
            return Err(CronParseError::new(format!("Invalid cron step: {part}")));
        }

        let range = step_parts[0];
        let (start, end) = if range == "*" {
            (min, max)
        } else if range.contains('-') {
            let pieces: Vec<_> = range.split('-').collect();
            if pieces.len() != 2 || pieces[0].is_empty() || pieces[1].is_empty() {
                return Err(CronParseError::new(format!("Invalid cron field: {field}")));
            }
            (
                parse_cron_int(pieces[0], format!("Invalid cron field: {field}"))?,
                parse_cron_int(pieces[1], format!("Invalid cron field: {field}"))?,
            )
        } else {
            let value = parse_cron_int(range, format!("Invalid cron field: {field}"))?;
            (value, value)
        };

        if start < min || end > max {
            return Err(CronParseError::new(format!(
                "Cron field out of range: {field} ({min}-{max})"
            )));
        }
        if start > end {
            return Err(CronParseError::new(format!(
                "Invalid cron range: {start}-{end} in {field}"
            )));
        }
        let mut value = start;
        while value <= end {
            out.insert(value);
            match value.checked_add(step) {
                Some(next) => value = next,
                None => break,
            }
        }
    }
    Ok(out)
}

fn parse_cron_int(text: &str, message: String) -> Result<u8, CronParseError> {
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        return Err(CronParseError::new(message));
    }
    text.parse::<u8>().map_err(|_| CronParseError::new(message))
}

/// Parse a standard five-field cron expression.
pub fn parse_cron(expr: &str) -> Result<CronSchedule, CronParseError> {
    let parts: Vec<_> = expr.split_whitespace().collect();
    if parts.len() != 5 {
        return Err(CronParseError::new(format!(
            "Invalid cron: expected 5 fields, got {}",
            parts.len()
        )));
    }
    Ok(CronSchedule {
        minutes: parse_cron_field(parts[0], 0, 59)?,
        hours: parse_cron_field(parts[1], 0, 23)?,
        days_of_month: parse_cron_field(parts[2], 1, 31)?,
        months: parse_cron_field(parts[3], 1, 12)?,
        days_of_week: parse_cron_field(parts[4], 0, 6)?,
    })
}

/// Test whether an instant matches a parsed five-field cron schedule.
#[must_use]
pub fn matches_cron(schedule: &CronSchedule, instant: CronInstant) -> bool {
    let day_of_month_matches = schedule.days_of_month.contains(&instant.day_of_month);
    let day_of_week_matches = schedule.days_of_week.contains(&instant.day_of_week);
    let day_of_month_restricted = !is_full_range(&schedule.days_of_month, 1, 31);
    let day_of_week_restricted = !is_full_range(&schedule.days_of_week, 0, 6);
    let day_matches = if day_of_month_restricted && day_of_week_restricted {
        day_of_month_matches || day_of_week_matches
    } else {
        day_of_month_matches && day_of_week_matches
    };

    schedule.minutes.contains(&instant.minute)
        && schedule.hours.contains(&instant.hour)
        && schedule.months.contains(&instant.month)
        && day_matches
}

fn is_full_range(values: &BTreeSet<u8>, min: u8, max: u8) -> bool {
    values.len() == usize::from(max - min + 1)
        && values.first().copied() == Some(min)
        && values.last().copied() == Some(max)
}

fn cron_minute_key(instant: CronInstant) -> (i32, u8, u8, u8, u8) {
    (
        instant.year,
        instant.month,
        instant.day_of_month,
        instant.hour,
        instant.minute,
    )
}

fn default_cron_tick_utc() -> CronTick {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    let secs = i64::try_from(duration.as_secs()).unwrap_or(i64::MAX);
    let nanos = duration.as_nanos();
    let days = secs.div_euclid(86_400);
    let seconds_of_day = secs.rem_euclid(86_400);
    let (year, month, day_of_month) = civil_from_days(days);
    let hour = u8::try_from(seconds_of_day / 3_600).unwrap_or(23);
    let minute = u8::try_from((seconds_of_day % 3_600) / 60).unwrap_or(59);
    let day_of_week = u8::try_from((days + 4).rem_euclid(7)).unwrap_or(0);
    CronTick::new(
        CronInstant::new(year, month, day_of_month, hour, minute, day_of_week),
        nanos.to_string(),
    )
}

fn civil_from_days(days_since_unix_epoch: i64) -> (i32, u8, u8) {
    let z = days_since_unix_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = year + i64::from(month <= 2);
    (
        i32::try_from(year).unwrap_or(i32::MAX),
        u8::try_from(month).unwrap_or(12),
        u8::try_from(day).unwrap_or(31),
    )
}

/// from_cron: emit a [`CronTick`] when the schedule matches the current minute.
///
/// The default clock uses UTC `SystemTime`; pass [`FromCronOptions::now`] for a
/// domain-local clock or deterministic tests. Each matching minute emits at most
/// once. Requires a graph-local driver (D111); missing driver reports ERROR on
/// activation.
pub fn from_cron(expr: &str) -> Operator<CronTick> {
    from_cron_with_options(expr, FromCronOptions::default())
}

/// Configurable form of [`from_cron`].
pub fn from_cron_with_options(expr: &str, opts: FromCronOptions) -> Operator<CronTick> {
    assert!(opts.tick_ms > 0, "from_cron: tick_ms must be positive");
    let schedule = parse_cron(expr).expect("from_cron: invalid cron expression");
    let tick_ms = opts.tick_ms;
    let now = opts
        .now
        .unwrap_or_else(|| Rc::new(default_cron_tick_utc) as Rc<dyn Fn() -> CronTick>);
    Operator::with_opts(
        "fromCron",
        NodeOpts {
            pausable: Pausable::False,
            ..NodeOpts::default()
        },
        move |ctx| {
            let Some(driver) = ctx.local_async_driver() else {
                ctx.down(vec![Message::Error(
                    "fromCron: missing local async driver".into(),
                )]);
                return;
            };
            let schedule = schedule.clone();
            let now = now.clone();
            let last_fired = Rc::new(RefCell::new(None::<(i32, u8, u8, u8, u8)>));
            let active = Rc::new(Cell::new(true));
            let cancel_slot: Rc<RefCell<Option<DriverCancel>>> = Rc::new(RefCell::new(None));
            let out = ctx.defer();
            let last_fired_interval = last_fired.clone();
            let schedule_interval = schedule.clone();
            let now_interval = now.clone();
            let active_interval = active.clone();
            let cancel = driver.interval(
                Duration::from_millis(tick_ms),
                Rc::new(move || {
                    if !active_interval.get() {
                        return;
                    }
                    let tick = now_interval();
                    let key = cron_minute_key(tick.instant);
                    if matches_cron(&schedule_interval, tick.instant)
                        && *last_fired_interval.borrow() != Some(key)
                    {
                        *last_fired_interval.borrow_mut() = Some(key);
                        out.down(vec![Message::Data(Rc::new(tick))]);
                    }
                }),
            );
            *cancel_slot.borrow_mut() = Some(cancel);
            let cleanup_active = active.clone();
            let cleanup_cancel = cancel_slot.clone();
            ctx.on_deactivation(move || {
                cleanup_driver_interval(&cleanup_active, &cleanup_cancel);
            });

            let tick = now();
            let key = cron_minute_key(tick.instant);
            if active.get()
                && matches_cron(&schedule, tick.instant)
                && *last_fired.borrow() != Some(key)
            {
                *last_fired.borrow_mut() = Some(key);
                ctx.down(vec![Message::Data(Rc::new(tick))]);
            }
        },
    )
}

fn git_text(repo_path: &Path, args: &[&str]) -> Result<String, String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo_path)
        .args(args)
        .output()
        .map_err(|error| format!("git: {error}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("git {:?}: {}", args, stderr.trim()));
    }
    Ok(strip_final_line_break(
        String::from_utf8_lossy(&output.stdout).into_owned(),
    ))
}

fn git_path_list(repo_path: &Path, args: &[&str]) -> Result<Vec<String>, String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo_path)
        .args(args)
        .arg("-z")
        .output()
        .map_err(|error| format!("git: {error}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("git {:?}: {}", args, stderr.trim()));
    }
    let text = String::from_utf8_lossy(&output.stdout);
    Ok(text
        .split('\0')
        .filter(|part| !part.is_empty())
        .map(ToOwned::to_owned)
        .collect())
}

fn strip_final_line_break(value: String) -> String {
    value
        .strip_suffix("\r\n")
        .or_else(|| value.strip_suffix('\n'))
        .unwrap_or(&value)
        .to_owned()
}

fn read_git_head(
    repo_path: &Path,
    previous_head: Option<&str>,
) -> Result<Option<GitPollResult>, String> {
    let head = git_text(repo_path, &["rev-parse", "HEAD"])?;
    if head.is_empty() || Some(head.as_str()) == previous_head {
        return Ok(None);
    }
    let files = if let Some(previous) = previous_head {
        git_path_list(
            repo_path,
            &["diff", "--name-only", &format!("{previous}..{head}")],
        )?
    } else {
        Vec::new()
    };
    let message = git_text(repo_path, &["log", "-1", "--format=%s", &head])?;
    let author = git_text(repo_path, &["log", "-1", "--format=%an", &head])?;
    Ok(Some(GitPollResult {
        head,
        files,
        message,
        author,
    }))
}

fn timestamp_ns_now() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_nanos()
        .to_string()
}

fn accepts_git_file(file: &str, opts: &FromGitHookOptions) -> bool {
    let normalized = file.replace('\\', "/");
    let included = opts.include.is_empty()
        || opts
            .include
            .iter()
            .any(|pattern| wildcard_match(pattern, &normalized));
    included
        && !opts
            .exclude
            .iter()
            .any(|pattern| wildcard_match(pattern, &normalized))
}

fn poll_git_hook(
    repo_path: &Path,
    opts: &FromGitHookOptions,
    last_seen: &Rc<RefCell<Option<String>>>,
) -> Result<Option<GitEvent>, String> {
    let previous = last_seen.borrow().clone();
    let Some(result) = read_git_head(repo_path, previous.as_deref())? else {
        return Ok(None);
    };
    let is_baseline = previous.is_none();
    *last_seen.borrow_mut() = Some(result.head.clone());
    if is_baseline {
        return Ok(None);
    }
    Ok(Some(GitEvent {
        hook: GitHookType::PostCommit,
        commit: result.head,
        files: result
            .files
            .into_iter()
            .filter(|file| accepts_git_file(file, opts))
            .collect(),
        message: result.message,
        author: result.author,
        timestamp_ns: timestamp_ns_now(),
    }))
}

/// from_git_hook: poll a local Git repository and emit post-commit events.
///
/// The first successful poll records a baseline and emits nothing; later HEAD
/// changes emit [`GitEvent`]. Git process calls are confined to this source
/// boundary and its graph-local driver callback (B72/D111).
pub fn from_git_hook<P>(repo_path: P) -> Operator<GitEvent>
where
    P: Into<PathBuf>,
{
    from_git_hook_with_options(repo_path, FromGitHookOptions::default())
}

/// Configurable form of [`from_git_hook`].
pub fn from_git_hook_with_options<P>(repo_path: P, opts: FromGitHookOptions) -> Operator<GitEvent>
where
    P: Into<PathBuf>,
{
    assert!(opts.poll_ms > 0, "from_git_hook: poll_ms must be positive");
    assert!(
        opts.max_consecutive_errors > 0,
        "from_git_hook: max_consecutive_errors must be positive"
    );
    let repo_path = absolutize(repo_path.into());
    Operator::with_opts(
        "fromGitHook",
        NodeOpts {
            pausable: Pausable::False,
            ..NodeOpts::default()
        },
        move |ctx| {
            let Some(driver) = ctx.local_async_driver() else {
                ctx.down(vec![Message::Error(
                    "fromGitHook: missing local async driver".into(),
                )]);
                return;
            };
            let repo_path = repo_path.clone();
            let opts = opts.clone();
            let last_seen = Rc::new(RefCell::new(None::<String>));
            let consecutive_errors = Rc::new(Cell::new(0usize));
            let active = Rc::new(Cell::new(true));
            let cancel_slot: Rc<RefCell<Option<DriverCancel>>> = Rc::new(RefCell::new(None));

            let out = ctx.defer();
            let repo_interval = repo_path.clone();
            let opts_interval = opts.clone();
            let last_seen_interval = last_seen.clone();
            let consecutive_errors_interval = consecutive_errors.clone();
            let active_interval = active.clone();
            let cancel_interval = cancel_slot.clone();
            let cancel = driver.interval(
                Duration::from_millis(opts.poll_ms),
                Rc::new(move || {
                    if !active_interval.get() {
                        return;
                    }
                    match poll_git_hook(&repo_interval, &opts_interval, &last_seen_interval) {
                        Ok(Some(event)) => {
                            consecutive_errors_interval.set(0);
                            out.down(vec![Message::Data(Rc::new(event))]);
                        }
                        Ok(None) => {
                            consecutive_errors_interval.set(0);
                        }
                        Err(error) => {
                            let next = consecutive_errors_interval.get() + 1;
                            consecutive_errors_interval.set(next);
                            if next >= opts_interval.max_consecutive_errors {
                                cleanup_driver_interval(&active_interval, &cancel_interval);
                                out.down(vec![Message::Error(error.into())]);
                            }
                        }
                    }
                }),
            );
            *cancel_slot.borrow_mut() = Some(cancel);
            let active_cleanup = active.clone();
            let cancel_cleanup = cancel_slot.clone();
            ctx.on_deactivation(move || {
                cleanup_driver_interval(&active_cleanup, &cancel_cleanup);
            });

            match poll_git_hook(&repo_path, &opts, &last_seen) {
                Ok(Some(event)) => {
                    consecutive_errors.set(0);
                    ctx.down(vec![Message::Data(Rc::new(event))]);
                }
                Ok(None) => {
                    consecutive_errors.set(0);
                }
                Err(error) => {
                    let next = consecutive_errors.get() + 1;
                    consecutive_errors.set(next);
                    if next >= opts.max_consecutive_errors {
                        cleanup_driver_interval(&active, &cancel_slot);
                        ctx.down(vec![Message::Error(error.into())]);
                    }
                }
            }
        },
    )
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

#[cfg(test)]
mod tests {
    use super::*;
    use notify::event::AccessKind;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time is after epoch")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "graphrefly-rs-source-{label}-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("temp dir can be created");
        dir
    }

    #[test]
    fn fs_event_conversion_drops_access_and_out_of_root_paths() {
        let root = temp_dir("convert-root");
        let outside = temp_dir("convert-outside");
        let opts = FromFsWatchOptions::default();
        let roots = vec![watch_root(root.clone())];

        let access =
            notify::Event::new(EventKind::Access(AccessKind::Any)).add_path(root.join("a.txt"));
        assert!(event_to_fs_events(access, &roots, &opts).is_empty());

        let outside_event =
            notify::Event::new(EventKind::Modify(ModifyKind::Any)).add_path(outside.join("b.txt"));
        assert!(event_to_fs_events(outside_event, &roots, &opts).is_empty());

        fs::remove_dir_all(root).ok();
        fs::remove_dir_all(outside).ok();
    }

    #[test]
    fn fs_event_conversion_uses_longest_root_and_file_relative_base() {
        let root = temp_dir("convert-longest");
        let nested = root.join("nested");
        fs::create_dir_all(&nested).expect("nested dir can be created");
        let file = nested.join("a.txt");
        fs::write(&file, "a").expect("file can be written");
        let opts = FromFsWatchOptions::default();

        let roots = vec![watch_root(root.clone()), watch_root(nested.clone())];
        let nested_event =
            notify::Event::new(EventKind::Modify(ModifyKind::Any)).add_path(file.clone());
        let converted = event_to_fs_events(nested_event, &roots, &opts);
        assert_eq!(converted.len(), 1);
        assert_eq!(converted[0].root, nested);
        assert_eq!(converted[0].relative_path, PathBuf::from("a.txt"));

        let roots = vec![watch_root(file.clone())];
        let file_event = notify::Event::new(EventKind::Modify(ModifyKind::Any)).add_path(file);
        let converted = event_to_fs_events(file_event, &roots, &opts);
        assert_eq!(converted.len(), 1);
        assert_eq!(converted[0].relative_path, PathBuf::from("a.txt"));

        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn fs_initial_scan_skips_excluded_dirs() {
        let root = temp_dir("scan-exclude");
        fs::create_dir_all(root.join("dist")).expect("dist dir can be created");
        fs::write(root.join("dist").join("ignored.txt"), "ignored").expect("ignored file");
        fs::write(root.join("kept.txt"), "kept").expect("kept file");
        let opts = FromFsWatchOptions {
            recursive: true,
            initial_scan: true,
            include: vec!["*.txt".to_owned()],
            ..FromFsWatchOptions::default()
        };
        let events = initial_scan_events(&[watch_root(root.clone())], &opts);

        let rels: Vec<_> = events
            .into_iter()
            .map(|event| event.relative_path)
            .collect();
        assert_eq!(rels, vec![PathBuf::from("kept.txt")]);
        fs::remove_dir_all(root).ok();
    }

    #[cfg(unix)]
    #[test]
    fn fs_initial_scan_skips_symlinked_dirs() {
        let root = temp_dir("scan-symlink");
        let real = root.join("real");
        fs::create_dir_all(&real).expect("real dir can be created");
        fs::write(real.join("kept.txt"), "kept").expect("kept file");
        std::os::unix::fs::symlink(&root, root.join("loop")).expect("symlink can be created");
        let opts = FromFsWatchOptions {
            recursive: true,
            initial_scan: true,
            include: vec!["*.txt".to_owned()],
            ..FromFsWatchOptions::default()
        };
        let events = initial_scan_events(&[watch_root(root.clone())], &opts);

        let rels: Vec<_> = events
            .into_iter()
            .map(|event| event.relative_path)
            .collect();
        assert_eq!(rels, vec![PathBuf::from("real").join("kept.txt")]);
        fs::remove_dir_all(root).ok();
    }

    #[test]
    fn fs_watch_cleanup_is_one_shot_and_cancels_driver_work() {
        let active = Rc::new(Cell::new(true));
        let canceled = Rc::new(Cell::new(0usize));
        let canceled_once = canceled.clone();
        let cancel_slot: Rc<RefCell<Option<DriverCancel>>> =
            Rc::new(RefCell::new(Some(Box::new(move || {
                canceled_once.set(canceled_once.get() + 1)
            }))));
        let watcher_slot: Rc<RefCell<Option<RecommendedWatcher>>> = Rc::new(RefCell::new(None));

        cleanup_fs_watch(&active, &cancel_slot, &watcher_slot);
        cleanup_fs_watch(&active, &cancel_slot, &watcher_slot);

        assert!(!active.get());
        assert_eq!(canceled.get(), 1);
        assert!(cancel_slot.borrow().is_none());
    }
}
