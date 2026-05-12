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

use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use graphrefly_core::{wall_clock_ns, Core, HandleId, NodeId, WeakCore};

use crate::backend::{
    HashMapBackend, IndexBackend, IndexRow, ListBackend, LogBackend, MapBackend, VecIndexBackend,
    VecListBackend, VecLogBackend,
};
use crate::changeset::{
    BaseChange, IndexChange, Lifecycle, ListChange, LogChange, MapChange, Version,
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
/// version counter so no lock is needed during `Core::emit()`.
struct EmitHandle<S> {
    core: WeakCore,
    node_id: NodeId,
    intern: InternFn<S>,
    version: AtomicU64,
}

impl<S> EmitHandle<S> {
    /// Bump version, intern snapshot, emit via Core. Returns the new version.
    fn emit(&self, snapshot: S) -> Version {
        let ver = self.version.fetch_add(1, Ordering::Relaxed) + 1;
        let handle = (self.intern)(snapshot);
        if let Some(core) = self.core.upgrade() {
            core.emit(self.node_id, handle);
        }
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
    inner: Mutex<LogInner<T>>,
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
            inner: Mutex::new(inner),
            emitter: EmitHandle {
                core: core.weak_handle(),
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

    pub fn append(&self, value: T) {
        let (snapshot, change) = {
            let mut inner = self.inner.lock();
            let change = inner.mutation_log.is_some().then(|| LogChange::Append {
                value: value.clone(),
            });
            inner.backend.append(value);
            (inner.backend.to_vec(), change)
        };
        let version = self.emitter.emit(snapshot);
        if let Some(change) = change {
            self.inner.lock().record(change, version);
        }
    }

    pub fn append_many(&self, values: Vec<T>) {
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
        let version = self.emitter.emit(snapshot);
        if let Some(change) = change {
            self.inner.lock().record(change, version);
        }
    }

    pub fn clear(&self) {
        let (snapshot, count) = {
            let mut inner = self.inner.lock();
            let count = inner.backend.clear();
            if count == 0 {
                return;
            }
            (inner.backend.to_vec(), count)
        };
        let version = self.emitter.emit(snapshot);
        self.inner
            .lock()
            .record(LogChange::Clear { count }, version);
    }

    pub fn trim_head(&self, n: usize) {
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
        let version = self.emitter.emit(snapshot);
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
                core: core.weak_handle(),
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

    pub fn append(&self, value: T) {
        let (snapshot, change) = {
            let mut inner = self.inner.lock();
            let change = inner.mutation_log.is_some().then(|| ListChange::Append {
                value: value.clone(),
            });
            inner.backend.append(value);
            (inner.backend.to_vec(), change)
        };
        let version = self.emitter.emit(snapshot);
        if let Some(change) = change {
            self.inner.lock().record(change, version);
        }
    }

    pub fn append_many(&self, values: Vec<T>) {
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
        let version = self.emitter.emit(snapshot);
        if let Some(change) = change {
            self.inner.lock().record(change, version);
        }
    }

    /// Insert `value` at `index`. Returns `Err(IndexOutOfBounds)` if
    /// `index > self.size()`.
    pub fn insert(&self, index: usize, value: T) -> Result<(), IndexOutOfBounds> {
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
        let version = self.emitter.emit(snapshot);
        if let Some(change) = change {
            self.inner.lock().record(change, version);
        }
        Ok(())
    }

    /// Insert `values` starting at `index`. Returns `Err(IndexOutOfBounds)` if
    /// `index > self.size()`.
    pub fn insert_many(&self, index: usize, values: Vec<T>) -> Result<(), IndexOutOfBounds> {
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
        let version = self.emitter.emit(snapshot);
        if let Some(change) = change {
            self.inner.lock().record(change, version);
        }
        Ok(())
    }

    /// Remove and return element at `index`. Negative indices count from end.
    /// Returns `None` if index is out of range.
    pub fn pop(&self, index: i64) -> Option<T> {
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
        let version = self.emitter.emit(snapshot);
        if let Some(change) = change {
            self.inner.lock().record(change, version);
        }
        Some(value)
    }

    pub fn clear(&self) {
        let (snapshot, count) = {
            let mut inner = self.inner.lock();
            let count = inner.backend.clear();
            if count == 0 {
                return;
            }
            (inner.backend.to_vec(), count)
        };
        let version = self.emitter.emit(snapshot);
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

struct MapInner<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    backend: Box<dyn MapBackend<K, V>>,
    mutation_log: Option<Vec<BaseChange<MapChange<K, V>>>>,
    structure_name: String,
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
        }
    }
}

impl<K, V> ReactiveMap<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    #[must_use]
    pub fn new(core: &Core, intern: InternFn<Vec<(K, V)>>, opts: ReactiveMapOptions<K, V>) -> Self {
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
        let inner = MapInner {
            backend,
            mutation_log,
            structure_name: opts.name,
        };
        Self {
            inner: Mutex::new(inner),
            emitter: EmitHandle {
                core: core.weak_handle(),
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
    pub fn has(&self, key: &K) -> bool {
        self.inner.lock().backend.has(key)
    }

    #[must_use]
    pub fn get(&self, key: &K) -> Option<V> {
        self.inner.lock().backend.get(key)
    }

    pub fn set(&self, key: K, value: V) {
        let (snapshot, change) = {
            let mut inner = self.inner.lock();
            let change = inner.mutation_log.is_some().then(|| MapChange::Set {
                key: key.clone(),
                value: value.clone(),
            });
            inner.backend.set(key, value);
            (inner.backend.to_vec(), change)
        };
        let version = self.emitter.emit(snapshot);
        if let Some(change) = change {
            self.inner.lock().record(change, version);
        }
    }

    pub fn set_many(&self, entries: Vec<(K, V)>) {
        if entries.is_empty() {
            return;
        }
        let (snapshot, changes) = {
            let mut inner = self.inner.lock();
            let changes: Option<Vec<MapChange<K, V>>> = inner.mutation_log.is_some().then(|| {
                entries
                    .iter()
                    .map(|(k, v)| MapChange::Set {
                        key: k.clone(),
                        value: v.clone(),
                    })
                    .collect()
            });
            inner.backend.set_many(entries);
            (inner.backend.to_vec(), changes)
        };
        let version = self.emitter.emit(snapshot);
        if let Some(changes) = changes {
            let mut inner = self.inner.lock();
            for change in changes {
                inner.record(change, version.clone());
            }
        }
    }

    pub fn delete(&self, key: &K) {
        let snapshot = {
            let mut inner = self.inner.lock();
            if !inner.backend.delete(key) {
                return;
            }
            inner.backend.to_vec()
        };
        let version = self.emitter.emit(snapshot);
        self.inner
            .lock()
            .record(MapChange::Delete { key: key.clone() }, version);
    }

    pub fn delete_many(&self, keys: &[K]) {
        let (snapshot, actually_deleted) = {
            let mut inner = self.inner.lock();
            // Pre-filter to keys that actually exist (P4 fix).
            let actually_deleted: Vec<K> = if inner.mutation_log.is_some() {
                keys.iter()
                    .filter(|k| inner.backend.has(k))
                    .cloned()
                    .collect()
            } else {
                vec![]
            };
            let removed = inner.backend.delete_many(keys);
            if removed == 0 {
                return;
            }
            (inner.backend.to_vec(), actually_deleted)
        };
        let version = self.emitter.emit(snapshot);
        if !actually_deleted.is_empty() {
            let mut inner = self.inner.lock();
            for k in actually_deleted {
                inner.record(MapChange::Delete { key: k }, version.clone());
            }
        }
    }

    pub fn clear(&self) {
        let (snapshot, count) = {
            let mut inner = self.inner.lock();
            let count = inner.backend.clear();
            if count == 0 {
                return;
            }
            (inner.backend.to_vec(), count)
        };
        let version = self.emitter.emit(snapshot);
        self.inner
            .lock()
            .record(MapChange::Clear { count }, version);
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

struct IndexInner<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + ToString + 'static,
    V: Clone + Send + Sync + 'static,
{
    backend: Box<dyn IndexBackend<K, V>>,
    mutation_log: Option<Vec<BaseChange<IndexChange<K, V>>>>,
    structure_name: String,
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

/// Options for constructing a [`ReactiveIndex`].
pub struct ReactiveIndexOptions<K, V>
where
    K: Clone + Eq + Hash + Send + Sync + ToString + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub name: String,
    pub backend: Option<Box<dyn IndexBackend<K, V>>>,
    pub mutation_log: bool,
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
        };
        Self {
            inner: Mutex::new(inner),
            emitter: EmitHandle {
                core: core.weak_handle(),
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

    /// Insert or update a row. Returns `true` if inserted (new primary key).
    pub fn upsert(&self, primary: K, secondary: String, value: V) -> bool {
        let (is_new, snapshot, change) = {
            let mut inner = self.inner.lock();
            let change = inner.mutation_log.is_some().then(|| IndexChange::Upsert {
                primary: primary.clone(),
                secondary: secondary.clone(),
                value: value.clone(),
            });
            let is_new = inner.backend.upsert(primary, secondary, value);
            (is_new, inner.backend.to_ordered(), change)
        };
        let version = self.emitter.emit(snapshot);
        if let Some(change) = change {
            self.inner.lock().record(change, version);
        }
        is_new
    }

    pub fn upsert_many(&self, rows: Vec<(K, String, V)>) {
        if rows.is_empty() {
            return;
        }
        let (snapshot, changes) = {
            let mut inner = self.inner.lock();
            let changes: Option<Vec<IndexChange<K, V>>> = inner.mutation_log.is_some().then(|| {
                rows.iter()
                    .map(|(k, s, v)| IndexChange::Upsert {
                        primary: k.clone(),
                        secondary: s.clone(),
                        value: v.clone(),
                    })
                    .collect()
            });
            inner.backend.upsert_many(rows);
            (inner.backend.to_ordered(), changes)
        };
        let version = self.emitter.emit(snapshot);
        if let Some(changes) = changes {
            let mut inner = self.inner.lock();
            for change in changes {
                inner.record(change, version.clone());
            }
        }
    }

    pub fn delete(&self, primary: &K) {
        let snapshot = {
            let mut inner = self.inner.lock();
            if !inner.backend.delete(primary) {
                return;
            }
            inner.backend.to_ordered()
        };
        let version = self.emitter.emit(snapshot);
        self.inner.lock().record(
            IndexChange::Delete {
                primary: primary.clone(),
            },
            version,
        );
    }

    pub fn delete_many(&self, primaries: &[K]) {
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
        let version = self.emitter.emit(snapshot);
        if !actually_deleted.is_empty() {
            self.inner.lock().record(
                IndexChange::DeleteMany {
                    primaries: actually_deleted,
                },
                version,
            );
        }
    }

    pub fn clear(&self) {
        let (snapshot, count) = {
            let mut inner = self.inner.lock();
            let count = inner.backend.clear();
            if count == 0 {
                return;
            }
            (inner.backend.to_ordered(), count)
        };
        let version = self.emitter.emit(snapshot);
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

    #[must_use]
    pub fn mutation_log_snapshot(&self) -> Option<Vec<BaseChange<IndexChange<K, V>>>> {
        self.inner.lock().mutation_log.clone()
    }
}
