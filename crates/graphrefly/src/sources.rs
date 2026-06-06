//! Source factories (D43/D40/D111).
//!
//! Sync sources run directly in the source body. Async/time sources stay at the
//! source/driver boundary: they schedule work on the graph-local driver and emit
//! later through `DeferredCtx`, preserving the sync wave core.

use std::cell::{Cell, RefCell};
use std::error::Error;
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::mpsc;
use std::time::Duration;

use futures_core::Stream;
use notify::event::ModifyKind;
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};

use crate::async_driver::DriverCancel;
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
