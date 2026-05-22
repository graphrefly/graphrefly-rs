//! Reactive data structures — Core-level integration (M5.A — D177/D179).
//!
//! Each structure wraps a backend trait behind a `parking_lot::Mutex`, owns a
//! state `NodeId` registered with Core, and emits DIRTY->DATA snapshots on
//! every mutation. Structures are standalone — no Graph dependency required.
//!
//! **Lock discipline (P1 fix):** Mutation methods hold the inner lock only
//! during backend mutation and snapshot collection. The lock is dropped before
//! `Core::emit()` is called, so subscribers are free to read (but not mutate)
//! the structure during message delivery. The `EmitHandle` lives outside the
//! inner `Mutex` and uses `AtomicU64` for thread-safe version counting.

use std::collections::HashMap;
use std::hash::Hash;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use graphrefly_core::{
    monotonic_ns, wall_clock_ns, Core, CoreMailbox, HandleId, Message, NodeId, SubscriptionId,
};

use crate::backend::{
    HashMapBackend, IndexBackend, IndexRow, ListBackend, LogBackend, MapBackend, VecIndexBackend,
    VecListBackend, VecLogBackend,
};
use crate::changeset::{
    BaseChange, DeleteReason, IndexChange, Lifecycle, ListChange, LogChange, MapChange, Version,
};

// ---------------------------------------------------------------------------
// Intern function type — user-supplied value->handle conversion
// ---------------------------------------------------------------------------

/// User-supplied function that converts a snapshot value `Vec<T>` (or similar)
/// into a `HandleId` for Core emission. The binding owns the value registry;
/// this closure captures `Arc<Binding>` and calls `binding.intern(snapshot)`.
pub type InternFn<S> = Arc<dyn Fn(S) -> HandleId + Send + Sync>;

// ---------------------------------------------------------------------------
// EmitHandle — outside Mutex, thread-safe emission
// ---------------------------------------------------------------------------

/// Emission handle stored outside the inner Mutex. Uses `AtomicU64` for the
/// version counter so no lock is needed during the emit.
///
/// D246 rule 6: structure mutation is owner-synchronous. The handle no
/// longer holds the `Arc<CoreMailbox>`; the owner threads its `&Core`
/// into each mutation method and `emit` calls `core.emit(..)` directly.
struct EmitHandle<S> {
    node_id: NodeId,
    intern: InternFn<S>,
    version: AtomicU64,
}

impl<S> EmitHandle<S> {
    /// Bump version, intern snapshot, emit synchronously owner-side.
    /// Returns the new version.
    fn emit(&self, core: &Core, snapshot: S) -> Version {
        let ver = self.version.fetch_add(1, Ordering::Relaxed) + 1;
        let handle = (self.intern)(snapshot);
        core.emit(self.node_id, handle);
        Version::Counter(ver)
    }
}

/// In-wave sink emitter. Captured into long-lived `core.subscribe(..)`
/// sink closures (`view`/`scan`) that fire IN-WAVE — re-entering Core
/// synchronously from inside a wave is unsound, so it posts
/// `MailboxOp::Emit` (deferred, drained owner-side / at the embedder
/// pump). This is genuine deferred sink re-entry, correct under D246
/// rule 6 (same mechanism `attach`/`attach_storage` use).
struct SinkEmitter<S> {
    mailbox: Arc<CoreMailbox>,
    node_id: NodeId,
    intern: InternFn<S>,
    version: AtomicU64,
}

impl<S> SinkEmitter<S> {
    /// Bump version, intern snapshot, post the deferred emit. Returns
    /// the new version.
    fn emit(&self, snapshot: S) -> Version {
        let ver = self.version.fetch_add(1, Ordering::Relaxed) + 1;
        let handle = (self.intern)(snapshot);
        // Core-gone (`false`) ⇒ the op is dropped unrun — identical to
        // the pre-S2b `WeakCore::upgrade()` → `None` no-op (the
        // structure is being torn down; the binding owns intern/release
        // so there is no handle leak the old path didn't also have).
        let _ = self.mailbox.post_emit(self.node_id, handle);
        Version::Counter(ver)
    }
}

// ---------------------------------------------------------------------------
// IndexOutOfBounds error
// ---------------------------------------------------------------------------

/// Error returned when a list `insert` or `insert_many` index is out of bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexOutOfBounds;

impl std::fmt::Display for IndexOutOfBounds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("index out of bounds")
    }
}

impl std::error::Error for IndexOutOfBounds {}

// ---------------------------------------------------------------------------
// ReactiveLog
// ---------------------------------------------------------------------------

/// Reactive append-only log with optional ring-buffer cap.
///
/// Emits `Vec<T>` snapshots via a Core state node on every mutation.
/// Optionally records `BaseChange<LogChange<T>>` deltas to a companion
/// mutation log.
pub struct ReactiveLog<T: Clone + Send + Sync + 'static> {
    inner: Arc<Mutex<LogInner<T>>>,
    emitter: EmitHandle<Vec<T>>,
    /// The Core state node backing this structure.
    pub node_id: NodeId,
}

struct LogInner<T: Clone + Send + Sync + 'static> {
    backend: Box<dyn LogBackend<T>>,
    mutation_log: Option<Vec<BaseChange<LogChange<T>>>>,
    structure_name: String,
}

impl<T: Clone + Send + Sync + 'static> LogInner<T> {
    fn record(&mut self, change: LogChange<T>, version: Version) {
        if let Some(log) = &mut self.mutation_log {
            log.push(BaseChange {
                structure: self.structure_name.clone(),
                version,
                t_ns: wall_clock_ns(),
                seq: None,
                lifecycle: Lifecycle::Data,
                change,
            });
        }
    }
}

/// Options for constructing a [`ReactiveLog`].
pub struct ReactiveLogOptions<T: Clone + Send + Sync + 'static> {
    pub name: String,
    pub max_size: Option<usize>,
    pub backend: Option<Box<dyn LogBackend<T>>>,
    pub mutation_log: bool,
}

impl<T: Clone + Send + Sync + 'static> Default for ReactiveLogOptions<T> {
    fn default() -> Self {
        Self {
            name: "reactiveLog".into(),
            max_size: None,
            backend: None,
            mutation_log: false,
        }
    }
}

impl<T: Clone + Send + Sync + 'static> ReactiveLog<T> {
    /// Create a new reactive log registered with the given Core.
    ///
    /// `intern` converts a `Vec<T>` snapshot to a `HandleId` for Core emission.
    #[must_use]
    pub fn new(core: &Core, intern: InternFn<Vec<T>>, opts: ReactiveLogOptions<T>) -> Self {
        let node_id = core
            .register_state(HandleId::new(0), false)
            .expect("register_state for ReactiveLog");
        let backend: Box<dyn LogBackend<T>> = opts
            .backend
            .unwrap_or_else(|| Box::new(VecLogBackend::new(opts.max_size)));
        let mutation_log = if opts.mutation_log {
            Some(Vec::new())
        } else {
            None
        };
        let inner = LogInner {
            backend,
            mutation_log,
            structure_name: opts.name,
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
            emitter: EmitHandle {
                node_id,
                intern,
                version: AtomicU64::new(0),
            },
            node_id,
        }
    }

    #[must_use]
    pub fn size(&self) -> usize {
        self.inner.lock().backend.size()
    }

    #[must_use]
    pub fn at(&self, index: i64) -> Option<T> {
        self.inner.lock().backend.at(index)
    }

    pub fn append(&self, core: &Core, value: T) {
        let (snapshot, change) = {
            let mut inner = self.inner.lock();
            let change = inner.mutation_log.is_some().then(|| LogChange::Append {
                value: value.clone(),
            });
            inner.backend.append(value);
            (inner.backend.to_vec(), change)
        };
        let version = self.emitter.emit(core, snapshot);
        if let Some(change) = change {
            self.inner.lock().record(change, version);
        }
    }

    pub fn append_many(&self, core: &Core, values: Vec<T>) {
        if values.is_empty() {
            return;
        }
        let (snapshot, change) = {
            let mut inner = self.inner.lock();
            let change = inner.mutation_log.is_some().then(|| LogChange::AppendMany {
                values: values.clone(),
            });
            inner.backend.append_many(values);
            (inner.backend.to_vec(), change)
        };
        let version = self.emitter.emit(core, snapshot);
        if let Some(change) = change {
            self.inner.lock().record(change, version);
        }
    }

    pub fn clear(&self, core: &Core) {
        let (snapshot, count) = {
            let mut inner = self.inner.lock();
            let count = inner.backend.clear();
            if count == 0 {
                return;
            }
            (inner.backend.to_vec(), count)
        };
        let version = self.emitter.emit(core, snapshot);
        self.inner
            .lock()
            .record(LogChange::Clear { count }, version);
    }

    pub fn trim_head(&self, core: &Core, n: usize) {
        if n == 0 {
            return;
        }
        let (snapshot, actual) = {
            let mut inner = self.inner.lock();
            let actual = inner.backend.trim_head(n);
            if actual == 0 {
                return;
            }
            (inner.backend.to_vec(), actual)
        };
        let version = self.emitter.emit(core, snapshot);
        self.inner
            .lock()
            .record(LogChange::TrimHead { n: actual }, version);
    }

    #[must_use]
    pub fn to_vec(&self) -> Vec<T> {
        self.inner.lock().backend.to_vec()
    }

    /// Snapshot the mutation log (if enabled). Returns `None` if mutation log
    /// was not enabled at construction.
    #[must_use]
    pub fn mutation_log_snapshot(&self) -> Option<Vec<BaseChange<LogChange<T>>>> {
        self.inner.lock().mutation_log.clone()
    }
}

// ---------------------------------------------------------------------------
// ReactiveLog — views, scan, attach, attachStorage (M5.B)
// ---------------------------------------------------------------------------

/// View specification for [`ReactiveLog::view`].
pub enum ViewSpec {
    /// Last `n` entries.
    Tail { n: usize },
    /// Range `[start..stop)`. `stop` defaults to log length.
    Slice { start: usize, stop: Option<usize> },
    /// Entries from cursor position onward. The cursor node provides a `usize`
    /// position via `read_cursor(handle) -> usize`.
    FromCursor {
        cursor_node: NodeId,
        read_cursor: Arc<dyn Fn(HandleId) -> usize + Send + Sync>,
    },
}

/// D246 rule 3: NO RAII unsubscribe below the binding. A subscription
/// bundle that tracks its `(NodeId, SubscriptionId)` subs and is torn
/// down by the **owner-invoked, synchronous** [`ReactiveSub::detach`] —
/// not `Drop`. Carries no `&Core` (the `'c` lifetime is gone); the
/// owner passes its `&Core` into `detach`.
pub struct ReactiveSub {
    subs: Vec<(NodeId, SubscriptionId)>,
}

impl ReactiveSub {
    /// Synchronously unsubscribe every tracked sub. Owner-invoked
    /// (D246 r3). Idempotent: subs are drained, so a second call is a
    /// no-op.
    pub fn detach(&mut self, core: &Core) {
        for (node_id, sub_id) in self.subs.drain(..) {
            core.unsubscribe(node_id, sub_id);
        }
    }
}

/// Handle to a log view. Call [`LogView::detach`] to dispose the view
/// subscription (no RAII — D246 r3).
pub struct LogView {
    /// The state node backing this view. Subscribe to receive view snapshots.
    pub node_id: NodeId,
    sub: ReactiveSub,
}

impl LogView {
    /// Synchronously dispose the view subscription. Owner-invoked.
    pub fn detach(&mut self, core: &Core) {
        self.sub.detach(core);
    }
}

/// Handle to a log scan. Call [`ScanHandle::detach`] to dispose the
/// scan subscription (no RAII — D246 r3).
pub struct ScanHandle {
    /// The state node backing this scan. Subscribe to receive accumulator snapshots.
    pub node_id: NodeId,
    sub: ReactiveSub,
}

impl ScanHandle {
    /// Synchronously dispose the scan subscription. Owner-invoked.
    pub fn detach(&mut self, core: &Core) {
        self.sub.detach(core);
    }
}

/// **Local mirror of `graphrefly_storage::AppendLogMode`** (next batch
/// 2026-05-21). `graphrefly-storage` already depends on
/// `graphrefly-structures` (for `BaseChange<T>`), so we can't forward
/// the enum the other direction without creating a dep cycle. The two
/// declarations are intentionally identical and pre-1.0 stable; if a
/// third mode is ever added, both must move together (`Append` /
/// `Overwrite` are the locked D269 vocabulary).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AppendLogMode {
    /// Read existing bucket, merge new entries, write back. M4.B default.
    #[default]
    Append,
    /// Replace bucket contents with the current batch (no read-merge).
    /// `ReactiveLog::attach_storage` rejects sinks declaring this mode —
    /// delta-shipping into an overwrite sink silently truncates the log
    /// to the last batch (memo:Re P1 hazard class).
    Overwrite,
}

/// **Error returned by [`ReactiveLog::attach_storage`]** (next batch
/// 2026-05-21 — D269 part-2 parity with TS `attachStorage` throw).
#[derive(Debug)]
pub enum AttachStorageError {
    /// A sink declared `AppendLogMode::Overwrite`. Delta-shipping into
    /// an overwrite sink silently truncates the log to the last batch.
    OverwriteSinkRejected { sink_index: usize },
}

impl std::fmt::Display for AttachStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OverwriteSinkRejected { sink_index } => write!(
                f,
                "attach_storage: sink[{sink_index}] declares AppendLogMode::Overwrite — \
                 delta-shipping into an overwrite sink truncates; use an Append-mode sink",
            ),
        }
    }
}

impl std::error::Error for AttachStorageError {}

/// Trait for append-log storage sinks used by [`ReactiveLog::attach_storage`].
///
/// Implementors receive batches of entries and optionally support pre-loading.
/// `append_entries` / `load_entries` / `flush` errors are swallowed by
/// `attach_storage`'s in-wave delivery sink (matches TS try/catch semantics) so
/// a failing tier doesn't break the reactive graph. `mode` is consulted at
/// attach time only.
pub trait AppendLogSink<T>: Send + Sync {
    /// Append entries to persistent storage.
    fn append_entries(&self, entries: &[T])
        -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    /// Load previously stored entries (for pre-loading on startup).
    /// Returns an empty vec if no entries are stored.
    fn load_entries(&self) -> Result<Vec<T>, Box<dyn std::error::Error + Send + Sync>>;
    /// **D269 part-2 parity** — sink's persistence mode. `attach_storage`
    /// rejects `Overwrite` sinks at attach time. Default `Append` so
    /// existing impls work unchanged.
    fn mode(&self) -> AppendLogMode {
        AppendLogMode::Append
    }
    /// **F9 parity (Option B per D171 precedent)** — flush any buffered
    /// writes synchronously. Drives [`AttachStorageHandle::flush_all`],
    /// which callers wire from a reactive timer subgraph
    /// (`graphrefly_operators::temporal::interval`). Default no-op so
    /// non-buffering sinks need no implementation.
    fn flush(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

/// Handle to an attach-storage subscription. Call
/// [`AttachStorageHandle::detach`] to dispose it (no RAII — D246 r3).
///
/// **Generic over `T`** (next batch 2026-05-21) — the handle retains
/// shared references to the sinks so [`AttachStorageHandle::flush_all`]
/// can drive their `flush()` methods. Callers wire
/// `interval(debounce_ms) → handle.flush_all()` (Option B per D171
/// precedent — mirrors `graphrefly_storage::StorageHandle::flush_all`).
pub struct AttachStorageHandle<T> {
    sub: ReactiveSub,
    sinks: Vec<Arc<dyn AppendLogSink<T>>>,
}

impl<T> AttachStorageHandle<T> {
    /// Synchronously dispose the attach-storage subscription.
    /// Owner-invoked.
    pub fn detach(&mut self, core: &Core) {
        self.sub.detach(core);
    }
    /// **F9 parity** — drive every sink's `flush()` synchronously.
    /// Returns the first error encountered (with a `sink[N] flush:`
    /// prefix for diagnostics); subsequent sinks are NOT attempted
    /// after a failure (matches `graphrefly_storage::StorageHandle::flush_all`).
    /// Callers wire this from a reactive timer subgraph; the handle
    /// owns no internal timer (no tokio in `graphrefly-structures`).
    ///
    /// **Caller obligation (D271 QA EC-2):** callers MUST NOT hold a
    /// lock that any sink's `flush()` impl might try to acquire.
    /// `flush_all` calls each sink synchronously and holds no internal
    /// lock during the call, so a sink that re-enters a caller-held
    /// lock would deadlock. Real-world sinks (`graphrefly-storage`'s
    /// `AppendLogStorage::flush` is the canonical example) take only
    /// their own internal locks — wiring `flush_all` from a reactive
    /// timer subgraph is therefore deadlock-free by construction.
    #[must_use = "ignoring AttachStorageHandle::flush_all's Result swallows write errors"]
    pub fn flush_all(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for (i, sink) in self.sinks.iter().enumerate() {
            if let Err(e) = sink.flush() {
                return Err(format!("sink[{i}] flush: {e}").into());
            }
        }
        Ok(())
    }
}

/// **D270 — `AttachOptions { skip_cached_replay }` (memo:Re P2 parity).**
/// Mirrors TS `ReactiveLogBundle.attach(upstream, opts?)`: when
/// `skip_cached_replay = true` AND the upstream has a cached value
/// (`Core::cache_of(upstream)` non-sentinel), drop the FIRST DATA-
/// bearing batch the attach sink receives — i.e. the subscribe-
/// handshake replay. Subsequent live emissions still land. A cold
/// upstream's first live emit is NOT dropped (the `cache_of` check
/// gates the suppression). The flag has no effect when the upstream's
/// cache is sentinel (no replay to skip).
#[derive(Debug, Clone, Copy, Default)]
pub struct AttachOptions {
    pub skip_cached_replay: bool,
}

impl<T: Clone + Send + Sync + 'static> ReactiveLog<T> {
    /// Create a reactive view of this log. Returns a [`LogView`] whose
    /// `node_id` emits `Vec<T>` snapshots on every log mutation.
    ///
    /// `intern` converts the view `Vec<T>` into a `HandleId` for Core emission.
    /// β/D231/D246: takes the owner's `&Core` (no stored Core under the
    /// actor model). The returned [`LogView`] carries no Core borrow;
    /// tear it down via [`LogView::detach`] (D246 r3 — no RAII).
    #[allow(clippy::too_many_lines)]
    pub fn view(&self, core: &Core, spec: ViewSpec, intern: InternFn<Vec<T>>) -> LogView {
        let view_node = core
            .register_state(HandleId::new(0), false)
            .expect("register_state for LogView");
        // In-wave sink emitter: the `core.subscribe(..)` closures below
        // fire IN-WAVE, so emission must be deferred (D246 r6).
        let view_emitter = Arc::new(SinkEmitter {
            mailbox: core.mailbox(),
            node_id: view_node,
            intern,
            version: AtomicU64::new(0),
        });

        let inner = Arc::clone(&self.inner);
        let mut subscriptions: Vec<(NodeId, SubscriptionId)> = Vec::new();

        match spec {
            ViewSpec::Tail { n } => {
                let inner_c = Arc::clone(&inner);
                let emitter_c = Arc::clone(&view_emitter);
                let sub = core.subscribe(
                    self.node_id,
                    Rc::new(move |msgs| {
                        if msgs.iter().any(|m| matches!(m, Message::Data(_))) {
                            let guard = inner_c.lock();
                            let data = guard.backend.to_vec();
                            let start = data.len().saturating_sub(n);
                            let view = data[start..].to_vec();
                            drop(guard);
                            emitter_c.emit(view);
                        }
                    }),
                );
                subscriptions.push((self.node_id, sub));
            }
            ViewSpec::Slice { start, stop } => {
                let inner_c = Arc::clone(&inner);
                let emitter_c = Arc::clone(&view_emitter);
                let sub = core.subscribe(
                    self.node_id,
                    Rc::new(move |msgs| {
                        if msgs.iter().any(|m| matches!(m, Message::Data(_))) {
                            let guard = inner_c.lock();
                            let data = guard.backend.to_vec();
                            let end = stop.unwrap_or(data.len()).min(data.len());
                            let s = start.min(end);
                            let view = data[s..end].to_vec();
                            drop(guard);
                            emitter_c.emit(view);
                        }
                    }),
                );
                subscriptions.push((self.node_id, sub));
            }
            ViewSpec::FromCursor {
                cursor_node,
                read_cursor,
            } => {
                let cursor_pos = Arc::new(Mutex::new(0usize));

                // Cursor subscription: update position and recompute.
                let cursor_pos_c = Arc::clone(&cursor_pos);
                let inner_c = Arc::clone(&inner);
                let emitter_c = Arc::clone(&view_emitter);
                let read_cursor_c = Arc::clone(&read_cursor);
                let sub_cursor = core.subscribe(
                    cursor_node,
                    Rc::new(move |msgs| {
                        for m in msgs {
                            if let Message::Data(h) = m {
                                let pos = read_cursor_c(*h);
                                *cursor_pos_c.lock() = pos;
                                let guard = inner_c.lock();
                                let data = guard.backend.to_vec();
                                let s = pos.min(data.len());
                                let view = data[s..].to_vec();
                                drop(guard);
                                emitter_c.emit(view);
                            }
                        }
                    }),
                );
                subscriptions.push((cursor_node, sub_cursor));

                // Log subscription: recompute with current cursor.
                let cursor_pos_c2 = Arc::clone(&cursor_pos);
                let inner_c2 = Arc::clone(&inner);
                let emitter_c2 = view_emitter;
                let sub_log = core.subscribe(
                    self.node_id,
                    Rc::new(move |msgs| {
                        if msgs.iter().any(|m| matches!(m, Message::Data(_))) {
                            let pos = *cursor_pos_c2.lock();
                            let guard = inner_c2.lock();
                            let data = guard.backend.to_vec();
                            let s = pos.min(data.len());
                            let view = data[s..].to_vec();
                            drop(guard);
                            emitter_c2.emit(view);
                        }
                    }),
                );
                subscriptions.push((self.node_id, sub_log));
            }
        }

        LogView {
            node_id: view_node,
            sub: ReactiveSub {
                subs: subscriptions,
            },
        }
    }

    /// Incremental running aggregate over the log.
    ///
    /// Returns a [`ScanHandle`] whose `node_id` emits the current accumulator
    /// on every log mutation. Appends are O(1) (only new entries processed);
    /// `trimHead`/`clear` trigger a full rescan.
    pub fn scan<TAcc: Clone + Send + Sync + 'static>(
        &self,
        core: &Core,
        initial: TAcc,
        step: Arc<dyn Fn(&TAcc, &T) -> TAcc + Send + Sync>,
        intern: InternFn<TAcc>,
    ) -> ScanHandle {
        struct ScanState<T, TAcc> {
            acc: TAcc,
            processed: usize,
            initial: TAcc,
            step: Arc<dyn Fn(&TAcc, &T) -> TAcc + Send + Sync>,
        }

        let scan_node = core
            .register_state(HandleId::new(0), false)
            .expect("register_state for Scan");

        let state = Arc::new(Mutex::new(ScanState {
            acc: initial.clone(),
            processed: 0,
            initial,
            step,
        }));
        let inner = Arc::clone(&self.inner);
        // In-wave sink emitter (D246 r6): the `core.subscribe(..)`
        // closure below fires IN-WAVE, so emission is deferred.
        let scan_emitter = Arc::new(SinkEmitter {
            mailbox: core.mailbox(),
            node_id: scan_node,
            intern,
            version: AtomicU64::new(0),
        });

        let sub = core.subscribe(
            self.node_id,
            Rc::new(move |msgs| {
                if msgs.iter().any(|m| matches!(m, Message::Data(_))) {
                    let mut ss = state.lock();
                    let guard = inner.lock();
                    let data = guard.backend.to_vec();
                    drop(guard);

                    if data.len() < ss.processed {
                        // Length decreased (clear/trim) — full rescan.
                        ss.acc = ss.initial.clone();
                        ss.processed = 0;
                    }
                    for item in &data[ss.processed..] {
                        ss.acc = (ss.step)(&ss.acc, item);
                    }
                    ss.processed = data.len();
                    let acc = ss.acc.clone();
                    drop(ss);
                    scan_emitter.emit(acc);
                }
            }),
        );

        ScanHandle {
            node_id: scan_node,
            sub: ReactiveSub {
                subs: vec![(self.node_id, sub)],
            },
        }
    }

    /// Subscribe to an upstream node and append every DATA value into this log.
    ///
    /// `read_value` converts the upstream `HandleId` to a `T` for appending.
    /// Returns a [`ReactiveSub`] — call [`ReactiveSub::detach`] to stop
    /// the attachment (D246 r3 — no RAII).
    /// β/D231: takes the owner's `&Core`.
    pub fn attach(
        &self,
        core: &Core,
        upstream: NodeId,
        read_value: Arc<dyn Fn(HandleId) -> T + Send + Sync>,
    ) -> ReactiveSub {
        self.attach_with_options(core, upstream, read_value, AttachOptions::default())
    }

    /// D270 — variant of [`Self::attach`] accepting [`AttachOptions`].
    /// Use when you need `skip_cached_replay` or future attach knobs;
    /// the no-option `attach` delegates here with `AttachOptions::default()`
    /// for back-compat callers.
    pub fn attach_with_options(
        &self,
        core: &Core,
        upstream: NodeId,
        read_value: Arc<dyn Fn(HandleId) -> T + Send + Sync>,
        opts: AttachOptions,
    ) -> ReactiveSub {
        let inner = Arc::clone(&self.inner);
        // β/D232-AMEND: the long-lived attach sink re-enters Core to
        // emit; it captures the `Send + Sync` `Arc<CoreMailbox>` and
        // posts `MailboxOp::Emit` (drained owner-side in-wave / at the
        // pump) — it can no longer hold a relocatable `WeakCore`.
        let mailbox = core.mailbox();
        let node_id = self.node_id;
        let intern = Arc::clone(&self.emitter.intern);

        // D270: gate the "skip first DATA-bearing batch" decision on
        // the upstream having a cached value at attach time. The flag
        // is one-shot — once a DATA batch arrives and is dropped, the
        // sink delivers normally for all subsequent batches.
        let suppress = if opts.skip_cached_replay {
            let cache = core.cache_of(upstream);
            cache != graphrefly_core::NO_HANDLE
        } else {
            false
        };
        let skip_state = Arc::new(parking_lot::Mutex::new(suppress));

        let sub = core.subscribe(
            upstream,
            Rc::new(move |msgs| {
                // D270: check-and-clear the skip flag if THIS batch
                // carries DATA. Locks-then-clears in one atomic step
                // so the next batch (live emission) is not skipped.
                let should_skip = {
                    let mut s = skip_state.lock();
                    let has_data = msgs.iter().any(|m| matches!(m, Message::Data(_)));
                    let do_skip = *s && has_data;
                    if do_skip {
                        *s = false;
                    }
                    do_skip
                };
                if should_skip {
                    return;
                }
                for m in msgs {
                    if let Message::Data(h) = m {
                        let value = read_value(*h);
                        let snapshot = {
                            let mut guard = inner.lock();
                            guard.backend.append(value);
                            guard.backend.to_vec()
                        };
                        let handle = (intern)(snapshot);
                        // Core-gone ⇒ drop-unrun, same as the pre-S2b
                        // `weak_core.upgrade()` → `None` no-op.
                        let _ = mailbox.post_emit(node_id, handle);
                    }
                }
            }),
        );
        ReactiveSub {
            subs: vec![(upstream, sub)],
        }
    }

    /// Wire append-log storage sinks to this log.
    ///
    /// If `preload` is true, attempts `load_entries()` on the first sink that
    /// returns data and appends to this log before subscribing. After
    /// subscription, deltas are forwarded to ALL sinks. On trim/clear, the
    /// full snapshot is re-shipped. Sink errors are swallowed (logged via
    /// `eprintln!`) so a failing tier doesn't break the reactive graph.
    pub fn attach_storage(
        &self,
        core: &Core,
        sinks: Vec<Arc<dyn AppendLogSink<T>>>,
        preload: bool,
    ) -> Result<AttachStorageHandle<T>, AttachStorageError> {
        // **D269 part-2 parity (next batch 2026-05-21)** — reject
        // `Overwrite` sinks loud (TS `attachStorage` throws on the
        // same shape). Delta-shipping into overwrite silently
        // truncates the log to the last batch — the memo:Re P1
        // hazard class.
        for (sink_index, sink) in sinks.iter().enumerate() {
            if sink.mode() == AppendLogMode::Overwrite {
                return Err(AttachStorageError::OverwriteSinkRejected { sink_index });
            }
        }

        if sinks.is_empty() {
            let sub = core.subscribe(self.node_id, Rc::new(|_| {}));
            return Ok(AttachStorageHandle {
                sub: ReactiveSub {
                    subs: vec![(self.node_id, sub)],
                },
                sinks,
            });
        }

        if preload {
            for sink in &sinks {
                if let Ok(entries) = sink.load_entries() {
                    if !entries.is_empty() {
                        self.append_many(core, entries);
                        break;
                    }
                }
            }
        }

        let current_size = self.size();
        // All sinks start delivered at current_size (post-preload snapshot).
        // The subscriber only sees future emissions, so no double-write.
        let delivered: Vec<Arc<Mutex<usize>>> = sinks
            .iter()
            .map(|_| Arc::new(Mutex::new(current_size)))
            .collect();

        let inner = Arc::clone(&self.inner);
        let sinks_for_sink = sinks.clone();
        let delivered_arc = delivered;

        let sub = core.subscribe(
            self.node_id,
            Rc::new(move |msgs| {
                if msgs.iter().any(|m| matches!(m, Message::Data(_))) {
                    let guard = inner.lock();
                    let data = guard.backend.to_vec();
                    drop(guard);

                    for (i, sink) in sinks_for_sink.iter().enumerate() {
                        let mut del = delivered_arc[i].lock();
                        let result = match data.len().cmp(&*del) {
                            std::cmp::Ordering::Greater => sink.append_entries(&data[*del..]),
                            std::cmp::Ordering::Less => sink.append_entries(&data),
                            std::cmp::Ordering::Equal => continue,
                        };
                        match result {
                            Ok(()) => *del = data.len(),
                            Err(e) => eprintln!("attach_storage sink[{i}] error: {e}"),
                        }
                    }
                }
            }),
        );

        Ok(AttachStorageHandle {
            sub: ReactiveSub {
                subs: vec![(self.node_id, sub)],
            },
            sinks,
        })
    }
}

// ---------------------------------------------------------------------------
// ReactiveList
// ---------------------------------------------------------------------------

/// Reactive ordered list.
///
/// Emits `Vec<T>` snapshots via a Core state node on every mutation.
pub struct ReactiveList<T: Clone + Send + Sync + 'static> {
    inner: Mutex<ListInner<T>>,
    emitter: EmitHandle<Vec<T>>,
    pub node_id: NodeId,
}

struct ListInner<T: Clone + Send + Sync + 'static> {
    backend: Box<dyn ListBackend<T>>,
    mutation_log: Option<Vec<BaseChange<ListChange<T>>>>,
    structure_name: String,
}

impl<T: Clone + Send + Sync + 'static> ListInner<T> {
    fn record(&mut self, change: ListChange<T>, version: Version) {
        if let Some(log) = &mut self.mutation_log {
            log.push(BaseChange {
                structure: self.structure_name.clone(),
                version,
                t_ns: wall_clock_ns(),
                seq: None,
                lifecycle: Lifecycle::Data,
                change,
            });
        }
    }
}

/// Options for constructing a [`ReactiveList`].
pub struct ReactiveListOptions<T: Clone + Send + Sync + 'static> {
    pub name: String,
    pub backend: Option<Box<dyn ListBackend<T>>>,
    pub mutation_log: bool,
}

impl<T: Clone + Send + Sync + 'static> Default for ReactiveListOptions<T> {
    fn default() -> Self {
        Self {
            name: "reactiveList".into(),
            backend: None,
            mutation_log: false,
        }
    }
}

impl<T: Clone + Send + Sync + 'static> ReactiveList<T> {
    #[must_use]
    pub fn new(core: &Core, intern: InternFn<Vec<T>>, opts: ReactiveListOptions<T>) -> Self {
        let node_id = core
            .register_state(HandleId::new(0), false)
            .expect("register_state for ReactiveList");
        let backend: Box<dyn ListBackend<T>> = opts
            .backend
            .unwrap_or_else(|| Box::new(VecListBackend::new()));
        let mutation_log = if opts.mutation_log {
            Some(Vec::new())
        } else {
            None
        };
        let inner = ListInner {
            backend,
            mutation_log,
            structure_name: opts.name,
        };
        Self {
            inner: Mutex::new(inner),
            emitter: EmitHandle {
                node_id,
                intern,
                version: AtomicU64::new(0),
            },
            node_id,
        }
    }

    #[must_use]
    pub fn size(&self) -> usize {
        self.inner.lock().backend.size()
    }

    #[must_use]
    pub fn at(&self, index: i64) -> Option<T> {
        self.inner.lock().backend.at(index)
    }

    pub fn append(&self, core: &Core, value: T) {
        let (snapshot, change) = {
            let mut inner = self.inner.lock();
            let change = inner.mutation_log.is_some().then(|| ListChange::Append {
                value: value.clone(),
            });
            inner.backend.append(value);
            (inner.backend.to_vec(), change)
        };
        let version = self.emitter.emit(core, snapshot);
        if let Some(change) = change {
            self.inner.lock().record(change, version);
        }
    }

    pub fn append_many(&self, core: &Core, values: Vec<T>) {
        if values.is_empty() {
            return;
        }
        let (snapshot, change) = {
            let mut inner = self.inner.lock();
            let change = inner
                .mutation_log
                .is_some()
                .then(|| ListChange::AppendMany {
                    values: values.clone(),
                });
            inner.backend.append_many(values);
            (inner.backend.to_vec(), change)
        };
        let version = self.emitter.emit(core, snapshot);
        if let Some(change) = change {
            self.inner.lock().record(change, version);
        }
    }

    /// Insert `value` at `index`. Returns `Err(IndexOutOfBounds)` if
    /// `index > self.size()`.
    pub fn insert(&self, core: &Core, index: usize, value: T) -> Result<(), IndexOutOfBounds> {
        let (snapshot, change) = {
            let mut inner = self.inner.lock();
            if index > inner.backend.size() {
                return Err(IndexOutOfBounds);
            }
            let change = inner.mutation_log.is_some().then(|| ListChange::Insert {
                index,
                value: value.clone(),
            });
            inner.backend.insert(index, value);
            (inner.backend.to_vec(), change)
        };
        let version = self.emitter.emit(core, snapshot);
        if let Some(change) = change {
            self.inner.lock().record(change, version);
        }
        Ok(())
    }

    /// Insert `values` starting at `index`. Returns `Err(IndexOutOfBounds)` if
    /// `index > self.size()`.
    pub fn insert_many(
        &self,
        core: &Core,
        index: usize,
        values: Vec<T>,
    ) -> Result<(), IndexOutOfBounds> {
        if values.is_empty() {
            return Ok(());
        }
        let (snapshot, change) = {
            let mut inner = self.inner.lock();
            if index > inner.backend.size() {
                return Err(IndexOutOfBounds);
            }
            let change = inner
                .mutation_log
                .is_some()
                .then(|| ListChange::InsertMany {
                    index,
                    values: values.clone(),
                });
            inner.backend.insert_many(index, values);
            (inner.backend.to_vec(), change)
        };
        let version = self.emitter.emit(core, snapshot);
        if let Some(change) = change {
            self.inner.lock().record(change, version);
        }
        Ok(())
    }

    /// Remove and return element at `index`. Negative indices count from end.
    /// Returns `None` if index is out of range.
    pub fn pop(&self, core: &Core, index: i64) -> Option<T> {
        let (value, snapshot, change) = {
            let mut inner = self.inner.lock();
            let value = inner.backend.pop(index)?;
            let change = inner.mutation_log.is_some().then(|| ListChange::Pop {
                index,
                value: value.clone(),
            });
            let snapshot = inner.backend.to_vec();
            (value, snapshot, change)
        };
        let version = self.emitter.emit(core, snapshot);
        if let Some(change) = change {
            self.inner.lock().record(change, version);
        }
        Some(value)
    }

    pub fn clear(&self, core: &Core) {
        let (snapshot, count) = {
            let mut inner = self.inner.lock();
            let count = inner.backend.clear();
            if count == 0 {
                return;
            }
            (inner.backend.to_vec(), count)
        };
        let version = self.emitter.emit(core, snapshot);
        self.inner
            .lock()
            .record(ListChange::Clear { count }, version);
    }

    #[must_use]
    pub fn to_vec(&self) -> Vec<T> {
        self.inner.lock().backend.to_vec()
    }

    #[must_use]
    pub fn mutation_log_snapshot(&self) -> Option<Vec<BaseChange<ListChange<T>>>> {
        self.inner.lock().mutation_log.clone()
    }
}

// ---------------------------------------------------------------------------
// ReactiveMap
// ---------------------------------------------------------------------------

/// Reactive key-value map.
///
/// Emits `Vec<(K, V)>` snapshots via a Core state node on every mutation.
/// (Vec of pairs rather than HashMap for serialization stability.)
pub struct ReactiveMap<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    inner: Mutex<MapInner<K, V>>,
    emitter: EmitHandle<Vec<(K, V)>>,
    pub node_id: NodeId,
}

/// Score-based retention policy for [`ReactiveMap`].
///
/// After every mutation, entries are scored and those below `archive_threshold`
/// (or exceeding `max_size` by ascending score) are archived (deleted).
/// Mutually exclusive with top-level `max_size` (LRU).
pub struct RetentionPolicy<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Score function — higher is kept.
    pub score: Arc<dyn Fn(&K, &V) -> f64 + Send + Sync>,
    /// Entries scoring below this threshold are archived.
    pub archive_threshold: Option<f64>,
    /// Maximum number of live entries. Lowest-scored entries archived first.
    pub max_size: Option<usize>,
    /// Called for each archived entry.
    pub on_archive: Option<Arc<dyn Fn(&K, &V, f64) + Send + Sync>>,
}

struct MapInner<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    backend: Box<dyn MapBackend<K, V>>,
    mutation_log: Option<Vec<BaseChange<MapChange<K, V>>>>,
    structure_name: String,
    /// Per-key expiry timestamps (monotonic ns). Present only when TTL is enabled.
    ttl_expiry: HashMap<K, u64>,
    /// Default TTL in nanoseconds. None = no TTL.
    default_ttl_ns: Option<u64>,
    /// LRU access order (most recently used at end). Present only when LRU is enabled.
    lru_order: Vec<K>,
    /// Maximum entries for LRU eviction. None = no LRU cap.
    lru_max_size: Option<usize>,
    /// Score-based retention policy.
    retention: Option<RetentionPolicy<K, V>>,
}

impl<K, V> MapInner<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn record(&mut self, change: MapChange<K, V>, version: Version) {
        if let Some(log) = &mut self.mutation_log {
            log.push(BaseChange {
                structure: self.structure_name.clone(),
                version,
                t_ns: wall_clock_ns(),
                seq: None,
                lifecycle: Lifecycle::Data,
                change,
            });
        }
    }

    /// Prune expired keys. Returns deleted (key, previous_value) pairs.
    fn prune_expired_inner(&mut self) -> Vec<(K, V)> {
        if self.ttl_expiry.is_empty() {
            return vec![];
        }
        let now = monotonic_ns();
        let expired_keys: Vec<K> = self
            .ttl_expiry
            .iter()
            .filter(|(_, &exp)| now >= exp)
            .map(|(k, _)| k.clone())
            .collect();
        let mut expired = Vec::new();
        for k in expired_keys {
            if let Some(prev) = self.backend.get(&k) {
                self.backend.delete(&k);
                self.ttl_expiry.remove(&k);
                self.lru_remove(&k);
                expired.push((k, prev));
            }
        }
        expired
    }

    /// Apply retention policy. Returns archived keys with scores.
    fn apply_retention_inner(&mut self) -> Vec<(K, V, f64)> {
        // Extract retention config values to avoid borrow conflict.
        let (score_fn, archive_threshold, max_size) = match &self.retention {
            Some(r) => (Arc::clone(&r.score), r.archive_threshold, r.max_size),
            None => return vec![],
        };
        let entries = self.backend.to_vec();
        if entries.is_empty() {
            return vec![];
        }
        let mut scored: Vec<(K, V, f64)> = entries
            .into_iter()
            .map(|(k, v)| {
                let s = (score_fn)(&k, &v);
                (k, v, s)
            })
            .collect();
        scored.sort_by(|a, b| a.2.total_cmp(&b.2));

        let mut archived = Vec::new();
        if let Some(threshold) = archive_threshold {
            while let Some(entry) = scored.first() {
                if entry.2 < threshold {
                    let (k, v, s) = scored.remove(0);
                    self.backend.delete(&k);
                    self.ttl_expiry.remove(&k);
                    self.lru_remove(&k);
                    archived.push((k, v, s));
                } else {
                    break;
                }
            }
        }
        if let Some(max) = max_size {
            while scored.len() > max {
                let (k, v, s) = scored.remove(0);
                self.backend.delete(&k);
                self.ttl_expiry.remove(&k);
                self.lru_remove(&k);
                archived.push((k, v, s));
            }
        }
        archived
    }

    /// Touch key in LRU order (move to end). Does NOT emit.
    fn lru_touch(&mut self, key: &K) {
        if self.lru_max_size.is_none() {
            return;
        }
        if let Some(pos) = self.lru_order.iter().position(|k| k == key) {
            self.lru_order.remove(pos);
            self.lru_order.push(key.clone());
        }
    }

    /// Remove key from LRU order.
    fn lru_remove(&mut self, key: &K) {
        if self.lru_max_size.is_none() {
            return;
        }
        if let Some(pos) = self.lru_order.iter().position(|k| k == key) {
            self.lru_order.remove(pos);
        }
    }

    /// Evict LRU entries exceeding max_size. Returns evicted (key, previous_value) pairs.
    fn lru_evict(&mut self) -> Vec<(K, V)> {
        let Some(max) = self.lru_max_size else {
            return vec![];
        };
        let mut evicted = Vec::new();
        while self.backend.size() > max && !self.lru_order.is_empty() {
            let victim = self.lru_order.remove(0);
            if let Some(prev) = self.backend.get(&victim) {
                self.backend.delete(&victim);
                self.ttl_expiry.remove(&victim);
                evicted.push((victim, prev));
            }
        }
        evicted
    }

    /// Record TTL for a key. Per-call `ttl` (seconds) overrides `default_ttl_ns`.
    fn set_ttl_with(&mut self, key: &K, ttl: Option<f64>) {
        let ttl_ns = match ttl {
            Some(secs) => Some((secs * 1_000_000_000.0) as u64),
            None => self.default_ttl_ns,
        };
        if let Some(ns) = ttl_ns {
            self.ttl_expiry.insert(key.clone(), monotonic_ns() + ns);
        }
    }
}

/// Options for constructing a [`ReactiveMap`].
pub struct ReactiveMapOptions<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub name: String,
    pub backend: Option<Box<dyn MapBackend<K, V>>>,
    pub mutation_log: bool,
    /// Default TTL in seconds. Applied to all `set`/`set_many` calls.
    /// Must be > 0 if set.
    pub default_ttl: Option<f64>,
    /// LRU cap. Mutually exclusive with `retention`.
    pub max_size: Option<usize>,
    /// Score-based retention policy. Mutually exclusive with `max_size`.
    pub retention: Option<RetentionPolicy<K, V>>,
}

impl<K, V> Default for ReactiveMapOptions<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self {
            name: "reactiveMap".into(),
            backend: None,
            mutation_log: false,
            default_ttl: None,
            max_size: None,
            retention: None,
        }
    }
}

/// Configuration validation error for [`ReactiveMap`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapConfigError(pub String);

impl std::fmt::Display for MapConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for MapConfigError {}

impl<K, V> ReactiveMap<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Create a new reactive map.
    ///
    /// # Errors
    /// Returns `MapConfigError` if both `max_size` (LRU) and `retention` are set.
    pub fn new(
        core: &Core,
        intern: InternFn<Vec<(K, V)>>,
        opts: ReactiveMapOptions<K, V>,
    ) -> Result<Self, MapConfigError> {
        if opts.max_size.is_some() && opts.retention.is_some() {
            return Err(MapConfigError(
                "max_size (LRU) and retention are mutually exclusive".into(),
            ));
        }
        if let Some(ref r) = opts.retention {
            if r.archive_threshold.is_none() && r.max_size.is_none() {
                return Err(MapConfigError(
                    "retention requires at least one of archive_threshold or max_size".into(),
                ));
            }
        }
        if let Some(ttl) = opts.default_ttl {
            if ttl <= 0.0 {
                return Err(MapConfigError("default_ttl must be > 0".into()));
            }
        }
        let node_id = core
            .register_state(HandleId::new(0), false)
            .expect("register_state for ReactiveMap");
        let backend: Box<dyn MapBackend<K, V>> = opts
            .backend
            .unwrap_or_else(|| Box::new(HashMapBackend::new()));
        let mutation_log = if opts.mutation_log {
            Some(Vec::new())
        } else {
            None
        };
        let default_ttl_ns = opts.default_ttl.map(|secs| (secs * 1_000_000_000.0) as u64);
        let inner = MapInner {
            backend,
            mutation_log,
            structure_name: opts.name,
            ttl_expiry: HashMap::new(),
            default_ttl_ns,
            lru_order: Vec::new(),
            lru_max_size: opts.max_size,
            retention: opts.retention,
        };
        Ok(Self {
            inner: Mutex::new(inner),
            emitter: EmitHandle {
                node_id,
                intern,
                version: AtomicU64::new(0),
            },
            node_id,
        })
    }

    #[must_use]
    pub fn size(&self) -> usize {
        self.inner.lock().backend.size()
    }

    /// Check if key exists. Expired keys are pruned (observable side-effect).
    /// LRU touch: live key is marked as most-recently-used (no emission).
    pub fn has(&self, core: &Core, key: &K) -> bool {
        let (has, expired) = {
            let mut inner = self.inner.lock();
            let mut target_expired = false;
            // Check TTL expiry first.
            if inner.default_ttl_ns.is_some() {
                if let Some(&exp) = inner.ttl_expiry.get(key) {
                    if monotonic_ns() >= exp {
                        target_expired = true;
                    }
                }
            }
            // Prune all expired keys (including the target if expired).
            let mut expired = inner.prune_expired_inner();
            if target_expired && !expired.iter().any(|(k, _)| k == key) {
                // Target was expired but prune_expired_inner didn't catch it
                // (race with monotonic_ns). Delete it explicitly.
                if let Some(prev) = inner.backend.get(key) {
                    inner.backend.delete(key);
                    inner.ttl_expiry.remove(key);
                    inner.lru_remove(key);
                    expired.push((key.clone(), prev));
                }
            }
            let has = if target_expired {
                false
            } else {
                let h = inner.backend.has(key);
                if h {
                    inner.lru_touch(key);
                }
                h
            };
            (has, expired)
        };
        if !expired.is_empty() {
            let snapshot = self.inner.lock().backend.to_vec();
            let version = self.emitter.emit(core, snapshot);
            let mut inner = self.inner.lock();
            for (k, prev) in expired {
                inner.record(
                    MapChange::Delete {
                        key: k,
                        previous: prev,
                        reason: DeleteReason::Expired,
                    },
                    version.clone(),
                );
            }
        }
        has
    }

    /// Get value by key. Expired keys return `None` (observable side-effect).
    /// LRU touch: live key is marked as most-recently-used (no emission).
    pub fn get(&self, core: &Core, key: &K) -> Option<V> {
        let (value, expired) = {
            let mut inner = self.inner.lock();
            let mut target_expired = false;
            if inner.default_ttl_ns.is_some() {
                if let Some(&exp) = inner.ttl_expiry.get(key) {
                    if monotonic_ns() >= exp {
                        target_expired = true;
                    }
                }
            }
            // Prune all expired keys (including the target if expired).
            let mut expired = inner.prune_expired_inner();
            if target_expired && !expired.iter().any(|(k, _)| k == key) {
                // Target was expired but prune_expired_inner didn't catch it.
                if let Some(prev) = inner.backend.get(key) {
                    inner.backend.delete(key);
                    inner.ttl_expiry.remove(key);
                    inner.lru_remove(key);
                    expired.push((key.clone(), prev));
                }
            }
            let value = if target_expired {
                None
            } else {
                let v = inner.backend.get(key);
                if v.is_some() {
                    inner.lru_touch(key);
                }
                v
            };
            (value, expired)
        };
        if !expired.is_empty() {
            let snapshot = self.inner.lock().backend.to_vec();
            let version = self.emitter.emit(core, snapshot);
            let mut inner = self.inner.lock();
            for (k, prev) in expired {
                inner.record(
                    MapChange::Delete {
                        key: k,
                        previous: prev,
                        reason: DeleteReason::Expired,
                    },
                    version.clone(),
                );
            }
        }
        value
    }

    pub fn set(&self, core: &Core, key: K, value: V) {
        self.set_with_ttl(core, key, value, None);
    }

    /// Set a key with an optional per-call TTL override (seconds).
    ///
    /// # Panics
    /// Panics if `ttl` is `Some` with a non-positive or non-finite value.
    pub fn set_with_ttl(&self, core: &Core, key: K, value: V, ttl: Option<f64>) {
        if let Some(t) = ttl {
            assert!(
                t > 0.0 && t.is_finite(),
                "per-call ttl must be positive and finite"
            );
        }
        let (snapshot, change, eviction_changes) = {
            let mut inner = self.inner.lock();
            let expired = inner.prune_expired_inner();
            let change = inner.mutation_log.is_some().then(|| MapChange::Set {
                key: key.clone(),
                value: value.clone(),
            });
            inner.set_ttl_with(&key, ttl);
            inner.lru_remove(&key);
            if inner.lru_max_size.is_some() {
                inner.lru_order.push(key.clone());
            }
            inner.backend.set(key, value);
            let evicted = inner.lru_evict();
            let archived = inner.apply_retention_inner();
            let mut eviction_changes: Vec<(K, V, DeleteReason)> = Vec::new();
            for (k, prev) in expired {
                eviction_changes.push((k, prev, DeleteReason::Expired));
            }
            for (k, prev) in evicted {
                eviction_changes.push((k, prev, DeleteReason::LruEvict));
            }
            for (k, v, s) in &archived {
                if let Some(on_archive) =
                    &inner.retention.as_ref().and_then(|r| r.on_archive.clone())
                {
                    on_archive(k, v, *s);
                }
                eviction_changes.push((k.clone(), v.clone(), DeleteReason::Archived));
            }
            (inner.backend.to_vec(), change, eviction_changes)
        };
        let version = self.emitter.emit(core, snapshot);
        if change.is_some() || !eviction_changes.is_empty() {
            let mut inner = self.inner.lock();
            if let Some(change) = change {
                inner.record(change, version.clone());
            }
            for (k, prev, reason) in eviction_changes {
                inner.record(
                    MapChange::Delete {
                        key: k,
                        previous: prev,
                        reason,
                    },
                    version.clone(),
                );
            }
        }
    }

    pub fn set_many(&self, core: &Core, entries: Vec<(K, V)>) {
        self.set_many_with_ttl(core, entries, None);
    }

    /// Batch set with optional per-call TTL override (seconds).
    ///
    /// # Panics
    /// Panics if `ttl` is `Some` with a non-positive or non-finite value.
    pub fn set_many_with_ttl(&self, core: &Core, entries: Vec<(K, V)>, ttl: Option<f64>) {
        if let Some(t) = ttl {
            assert!(
                t > 0.0 && t.is_finite(),
                "per-call ttl must be positive and finite"
            );
        }
        if entries.is_empty() {
            return;
        }
        let (snapshot, changes, eviction_changes) = {
            let mut inner = self.inner.lock();
            let expired = inner.prune_expired_inner();
            let changes: Option<Vec<MapChange<K, V>>> = inner.mutation_log.is_some().then(|| {
                entries
                    .iter()
                    .map(|(k, v)| MapChange::Set {
                        key: k.clone(),
                        value: v.clone(),
                    })
                    .collect()
            });
            for (k, _) in &entries {
                inner.set_ttl_with(k, ttl);
                inner.lru_remove(k);
                if inner.lru_max_size.is_some() {
                    inner.lru_order.push(k.clone());
                }
            }
            inner.backend.set_many(entries);
            let evicted = inner.lru_evict();
            let archived = inner.apply_retention_inner();
            let mut eviction_changes: Vec<(K, V, DeleteReason)> = Vec::new();
            for (k, prev) in expired {
                eviction_changes.push((k, prev, DeleteReason::Expired));
            }
            for (k, prev) in evicted {
                eviction_changes.push((k, prev, DeleteReason::LruEvict));
            }
            for (k, v, s) in &archived {
                if let Some(on_archive) =
                    &inner.retention.as_ref().and_then(|r| r.on_archive.clone())
                {
                    on_archive(k, v, *s);
                }
                eviction_changes.push((k.clone(), v.clone(), DeleteReason::Archived));
            }
            (inner.backend.to_vec(), changes, eviction_changes)
        };
        let version = self.emitter.emit(core, snapshot);
        if changes.is_some() || !eviction_changes.is_empty() {
            let mut inner = self.inner.lock();
            if let Some(changes) = changes {
                for change in changes {
                    inner.record(change, version.clone());
                }
            }
            for (k, prev, reason) in eviction_changes {
                inner.record(
                    MapChange::Delete {
                        key: k,
                        previous: prev,
                        reason,
                    },
                    version.clone(),
                );
            }
        }
    }

    pub fn delete(&self, core: &Core, key: &K) {
        let (snapshot, previous) = {
            let mut inner = self.inner.lock();
            let previous = inner.backend.get(key);
            if !inner.backend.delete(key) {
                return;
            }
            inner.ttl_expiry.remove(key);
            inner.lru_remove(key);
            (inner.backend.to_vec(), previous)
        };
        let version = self.emitter.emit(core, snapshot);
        if let Some(prev) = previous {
            self.inner.lock().record(
                MapChange::Delete {
                    key: key.clone(),
                    previous: prev,
                    reason: DeleteReason::Explicit,
                },
                version,
            );
        }
    }

    pub fn delete_many(&self, core: &Core, keys: &[K]) {
        let (snapshot, actually_deleted) = {
            let mut inner = self.inner.lock();
            let actually_deleted: Vec<(K, V)> = keys
                .iter()
                .filter_map(|k| inner.backend.get(k).map(|v| (k.clone(), v)))
                .collect();
            let removed = inner.backend.delete_many(keys);
            if removed == 0 {
                return;
            }
            for k in keys {
                inner.ttl_expiry.remove(k);
                inner.lru_remove(k);
            }
            (inner.backend.to_vec(), actually_deleted)
        };
        let version = self.emitter.emit(core, snapshot);
        if !actually_deleted.is_empty() {
            let mut inner = self.inner.lock();
            for (k, prev) in actually_deleted {
                inner.record(
                    MapChange::Delete {
                        key: k,
                        previous: prev,
                        reason: DeleteReason::Explicit,
                    },
                    version.clone(),
                );
            }
        }
    }

    pub fn clear(&self, core: &Core) {
        let (snapshot, count) = {
            let mut inner = self.inner.lock();
            let count = inner.backend.clear();
            if count == 0 {
                return;
            }
            inner.ttl_expiry.clear();
            inner.lru_order.clear();
            (inner.backend.to_vec(), count)
        };
        let version = self.emitter.emit(core, snapshot);
        self.inner
            .lock()
            .record(MapChange::Clear { count }, version);
    }

    /// Explicitly prune all expired keys. Returns the number of keys removed.
    pub fn prune_expired(&self, core: &Core) -> usize {
        let expired = {
            let mut inner = self.inner.lock();
            inner.prune_expired_inner()
        };
        if expired.is_empty() {
            return 0;
        }
        let count = expired.len();
        let snapshot = self.inner.lock().backend.to_vec();
        let version = self.emitter.emit(core, snapshot);
        let mut inner = self.inner.lock();
        for (k, prev) in expired {
            inner.record(
                MapChange::Delete {
                    key: k,
                    previous: prev,
                    reason: DeleteReason::Expired,
                },
                version.clone(),
            );
        }
        count
    }

    #[must_use]
    pub fn to_vec(&self) -> Vec<(K, V)> {
        self.inner.lock().backend.to_vec()
    }

    #[must_use]
    pub fn mutation_log_snapshot(&self) -> Option<Vec<BaseChange<MapChange<K, V>>>> {
        self.inner.lock().mutation_log.clone()
    }
}

// ---------------------------------------------------------------------------
// ReactiveIndex
// ---------------------------------------------------------------------------

/// Reactive sorted index with primary key lookup and secondary sort order.
///
/// Emits `Vec<IndexRow<K, V>>` snapshots via a Core state node on every mutation.
pub struct ReactiveIndex<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + ToString + 'static,
    V: Clone + Send + Sync + 'static,
{
    inner: Mutex<IndexInner<K, V>>,
    emitter: EmitHandle<Vec<IndexRow<K, V>>>,
    pub node_id: NodeId,
}

/// User-supplied equality function for upsert idempotency.
pub type IndexEqualsFn<K, V> = Arc<dyn Fn(&IndexRow<K, V>, &IndexRow<K, V>) -> bool + Send + Sync>;

struct IndexInner<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + ToString + 'static,
    V: Clone + Send + Sync + 'static,
{
    backend: Box<dyn IndexBackend<K, V>>,
    mutation_log: Option<Vec<BaseChange<IndexChange<K, V>>>>,
    structure_name: String,
    /// Factory-level equals for upsert idempotency. When set, `upsert` on an
    /// existing key compares the existing row with the proposed row; if
    /// `equals(existing, proposed)` returns `true`, the upsert is a no-op
    /// (no version bump, no emission).
    equals: Option<IndexEqualsFn<K, V>>,
}

impl<K, V> IndexInner<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + ToString + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn record(&mut self, change: IndexChange<K, V>, version: Version) {
        if let Some(log) = &mut self.mutation_log {
            log.push(BaseChange {
                structure: self.structure_name.clone(),
                version,
                t_ns: wall_clock_ns(),
                seq: None,
                lifecycle: Lifecycle::Data,
                change,
            });
        }
    }
}

/// Per-call upsert options for [`ReactiveIndex::upsert`].
pub struct UpsertOptions<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + ToString + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Per-call equals override. Takes precedence over the factory-level equals.
    pub equals: Option<IndexEqualsFn<K, V>>,
}

impl<K, V> Default for UpsertOptions<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + ToString + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self { equals: None }
    }
}

/// Options for constructing a [`ReactiveIndex`].
pub struct ReactiveIndexOptions<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + ToString + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub name: String,
    pub backend: Option<Box<dyn IndexBackend<K, V>>>,
    pub mutation_log: bool,
    /// Factory-level equals for upsert idempotency.
    pub equals: Option<IndexEqualsFn<K, V>>,
}

impl<K, V> Default for ReactiveIndexOptions<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + ToString + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self {
            name: "reactiveIndex".into(),
            backend: None,
            mutation_log: false,
            equals: None,
        }
    }
}

impl<K, V> ReactiveIndex<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + ToString + 'static,
    V: Clone + Send + Sync + 'static,
{
    #[must_use]
    pub fn new(
        core: &Core,
        intern: InternFn<Vec<IndexRow<K, V>>>,
        opts: ReactiveIndexOptions<K, V>,
    ) -> Self {
        let node_id = core
            .register_state(HandleId::new(0), false)
            .expect("register_state for ReactiveIndex");
        let backend: Box<dyn IndexBackend<K, V>> = opts
            .backend
            .unwrap_or_else(|| Box::new(VecIndexBackend::new()));
        let mutation_log = if opts.mutation_log {
            Some(Vec::new())
        } else {
            None
        };
        let inner = IndexInner {
            backend,
            mutation_log,
            structure_name: opts.name,
            equals: opts.equals,
        };
        Self {
            inner: Mutex::new(inner),
            emitter: EmitHandle {
                node_id,
                intern,
                version: AtomicU64::new(0),
            },
            node_id,
        }
    }

    #[must_use]
    pub fn size(&self) -> usize {
        self.inner.lock().backend.size()
    }

    #[must_use]
    pub fn has(&self, primary: &K) -> bool {
        self.inner.lock().backend.has(primary)
    }

    #[must_use]
    pub fn get(&self, primary: &K) -> Option<V> {
        self.inner.lock().backend.get(primary)
    }

    /// Insert or update a row. Returns `true` if inserted (new primary key),
    /// `false` if updated or skipped by equals idempotency.
    pub fn upsert(&self, core: &Core, primary: K, secondary: String, value: V) -> bool {
        self.upsert_with(core, primary, secondary, value, &UpsertOptions::default())
    }

    /// Insert or update a row with per-call options.
    ///
    /// If `opts.equals` (or the factory-level equals) returns `true` for the
    /// existing row vs the proposed row, the upsert is a no-op (no emission).
    pub fn upsert_with(
        &self,
        core: &Core,
        primary: K,
        secondary: String,
        value: V,
        opts: &UpsertOptions<K, V>,
    ) -> bool {
        let (is_new, snapshot, change) = {
            let mut inner = self.inner.lock();
            // Check equals idempotency: per-call overrides factory-level.
            let eq_fn = opts.equals.as_ref().or(inner.equals.as_ref());
            if let Some(eq) = eq_fn {
                if let Some(existing_row) = inner.backend.get_row(&primary) {
                    let proposed = IndexRow {
                        primary: primary.clone(),
                        secondary: secondary.clone(),
                        value: value.clone(),
                    };
                    if eq(&existing_row, &proposed) {
                        return false;
                    }
                }
            }
            let change = inner.mutation_log.is_some().then(|| IndexChange::Upsert {
                primary: primary.clone(),
                secondary: secondary.clone(),
                value: value.clone(),
            });
            let is_new = inner.backend.upsert(primary, secondary, value);
            (is_new, inner.backend.to_ordered(), change)
        };
        let version = self.emitter.emit(core, snapshot);
        if let Some(change) = change {
            self.inner.lock().record(change, version);
        }
        is_new
    }

    /// Batch upsert. Rows matching the factory-level equals are skipped.
    /// If ALL rows are skipped, no emission occurs.
    pub fn upsert_many(&self, core: &Core, rows: Vec<(K, String, V)>) {
        if rows.is_empty() {
            return;
        }
        let (snapshot, changes) = {
            let mut inner = self.inner.lock();
            // Filter rows through equals idempotency.
            let effective_rows: Vec<(K, String, V)> = if let Some(eq) = &inner.equals {
                rows.into_iter()
                    .filter(|(pk, sec, val)| {
                        if let Some(existing) = inner.backend.get_row(pk) {
                            let proposed = IndexRow {
                                primary: pk.clone(),
                                secondary: sec.clone(),
                                value: val.clone(),
                            };
                            !eq(&existing, &proposed)
                        } else {
                            true
                        }
                    })
                    .collect()
            } else {
                rows
            };
            if effective_rows.is_empty() {
                return;
            }
            let changes: Option<Vec<IndexChange<K, V>>> = inner.mutation_log.is_some().then(|| {
                effective_rows
                    .iter()
                    .map(|(k, s, v)| IndexChange::Upsert {
                        primary: k.clone(),
                        secondary: s.clone(),
                        value: v.clone(),
                    })
                    .collect()
            });
            inner.backend.upsert_many(effective_rows);
            (inner.backend.to_ordered(), changes)
        };
        let version = self.emitter.emit(core, snapshot);
        if let Some(changes) = changes {
            let mut inner = self.inner.lock();
            for change in changes {
                inner.record(change, version.clone());
            }
        }
    }

    pub fn delete(&self, core: &Core, primary: &K) {
        let snapshot = {
            let mut inner = self.inner.lock();
            if !inner.backend.delete(primary) {
                return;
            }
            inner.backend.to_ordered()
        };
        let version = self.emitter.emit(core, snapshot);
        self.inner.lock().record(
            IndexChange::Delete {
                primary: primary.clone(),
            },
            version,
        );
    }

    pub fn delete_many(&self, core: &Core, primaries: &[K]) {
        let (snapshot, actually_deleted) = {
            let mut inner = self.inner.lock();
            // Pre-filter to keys that actually exist (P4 fix).
            let actually_deleted: Vec<K> = if inner.mutation_log.is_some() {
                primaries
                    .iter()
                    .filter(|k| inner.backend.has(k))
                    .cloned()
                    .collect()
            } else {
                vec![]
            };
            let removed = inner.backend.delete_many(primaries);
            if removed == 0 {
                return;
            }
            (inner.backend.to_ordered(), actually_deleted)
        };
        let version = self.emitter.emit(core, snapshot);
        if !actually_deleted.is_empty() {
            self.inner.lock().record(
                IndexChange::DeleteMany {
                    primaries: actually_deleted,
                },
                version,
            );
        }
    }

    pub fn clear(&self, core: &Core) {
        let (snapshot, count) = {
            let mut inner = self.inner.lock();
            let count = inner.backend.clear();
            if count == 0 {
                return;
            }
            (inner.backend.to_ordered(), count)
        };
        let version = self.emitter.emit(core, snapshot);
        self.inner
            .lock()
            .record(IndexChange::Clear { count }, version);
    }

    #[must_use]
    pub fn to_ordered(&self) -> Vec<IndexRow<K, V>> {
        self.inner.lock().backend.to_ordered()
    }

    #[must_use]
    pub fn to_primary_map(&self) -> Vec<(K, V)> {
        self.inner.lock().backend.to_primary_map()
    }

    /// Values of all rows whose **primary key** sorts within `[start, end)`
    /// (inclusive start, exclusive end), in **ascending primary-key order**
    /// (D205). `start >= end` or no matches → empty vec. The `K: Ord` bound
    /// is method-scoped so it does not constrain `ReactiveIndex<K, V>` itself.
    #[must_use]
    pub fn range_by_primary(&self, start: &K, end: &K) -> Vec<V>
    where
        K: Ord,
    {
        let mut rows: Vec<(K, V)> = self
            .inner
            .lock()
            .backend
            .to_primary_map()
            .into_iter()
            .filter(|(k, _)| k >= start && k < end)
            .collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        rows.into_iter().map(|(_, v)| v).collect()
    }

    #[must_use]
    pub fn mutation_log_snapshot(&self) -> Option<Vec<BaseChange<IndexChange<K, V>>>> {
        self.inner.lock().mutation_log.clone()
    }
}
