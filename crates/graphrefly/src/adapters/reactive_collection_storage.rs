//! Graph-bound persistence sidecars for reactive collections (D161).
//!
//! Storage remains passive: load helpers live in `storage`, restore helpers live
//! with the data structures, and this adapter only composes collection deltas
//! into strict JSON storage frames plus observable persistence facts.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Value};

use crate::data_structures::{
    restore_reactive_index, restore_reactive_list, restore_reactive_log, restore_reactive_map,
    IndexChange, IndexRow, ListChange, LogChange, MapChange, ReactiveIndex, ReactiveIndexOptions,
    ReactiveList, ReactiveListOptions, ReactiveLog, ReactiveLogOptions, ReactiveMap,
    ReactiveMapOptions,
};
use crate::graph::{Graph, GraphNodeOpts};
use crate::node::Node;
use crate::protocol::Message;
use crate::storage::{
    load_reactive_index_state, load_reactive_list_state, load_reactive_log_state,
    load_reactive_map_state, reactive_collection_change_frame, reactive_collection_snapshot_frame,
    reactive_collection_snapshot_key, AppendLogStorageTier, KvStorageTier,
    LoadReactiveCollectionStateOptions, ReactiveCollectionChangeFrame, ReactiveCollectionKind,
    ReactiveCollectionSnapshotFrame, StorageError, StorageResult,
};

type Disposer = Box<dyn FnOnce()>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReactiveCollectionPersistenceStatus {
    Starting,
    Ready,
    Flushing,
    Errored,
    Disposed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReactiveCollectionPersistenceCursor {
    pub collection: ReactiveCollectionKind,
    pub change_seq: Option<u64>,
    pub snapshot_writes: usize,
    pub change_writes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReactiveCollectionPersistenceStatusFact {
    pub state: ReactiveCollectionPersistenceStatus,
    pub pending: usize,
    pub writes: usize,
    pub errors: usize,
    pub cursor: ReactiveCollectionPersistenceCursor,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReactiveCollectionPersistenceErrorFact {
    pub phase: String,
    pub message: String,
    pub cursor: ReactiveCollectionPersistenceCursor,
}

#[derive(Clone)]
pub struct PersistReactiveCollectionOptions {
    pub graph: Option<Graph>,
    pub name: Option<String>,
    pub storage_prefix: Option<String>,
    pub snapshot_key: Option<String>,
    pub snapshot_store: Rc<dyn KvStorageTier<ReactiveCollectionSnapshotFrame>>,
    pub change_log: Option<Rc<dyn AppendLogStorageTier<ReactiveCollectionChangeFrame>>>,
    pub snapshot_on_attach: bool,
    pub snapshot_every_changes: Option<usize>,
}

impl PersistReactiveCollectionOptions {
    pub fn new(
        snapshot_store: Rc<dyn KvStorageTier<ReactiveCollectionSnapshotFrame>>,
        storage_prefix: impl Into<String>,
    ) -> Self {
        Self {
            graph: None,
            name: None,
            storage_prefix: Some(storage_prefix.into()),
            snapshot_key: None,
            snapshot_store,
            change_log: None,
            snapshot_on_attach: true,
            snapshot_every_changes: None,
        }
    }
}

pub struct ReactiveCollectionPersistence {
    pub ready: Node<bool>,
    pub status: Node<Value>,
    pub error: Node<Value>,
    pub cursor: Node<Value>,
    flush: Rc<dyn Fn() -> StorageResult<()>>,
    snapshot: Rc<dyn Fn() -> StorageResult<()>>,
    dispose: RefCell<Option<Disposer>>,
}

impl ReactiveCollectionPersistence {
    pub fn flush(&self) -> StorageResult<()> {
        (self.flush)()
    }

    pub fn snapshot(&self) -> StorageResult<()> {
        (self.snapshot)()
    }

    pub fn dispose(&self) {
        if let Some(dispose) = self.dispose.borrow_mut().take() {
            dispose();
        }
    }

    pub fn status_fact(&self) -> StorageResult<ReactiveCollectionPersistenceStatusFact> {
        let value = self
            .status
            .cache()
            .ok_or_else(|| StorageError::backend("reactiveCollection status fact is absent"))?;
        parse_status_fact(&value)
    }

    pub fn cursor_fact(&self) -> StorageResult<ReactiveCollectionPersistenceCursor> {
        let value = self
            .cursor
            .cache()
            .ok_or_else(|| StorageError::backend("reactiveCollection cursor fact is absent"))?;
        parse_cursor_fact(&value)
    }

    pub fn error_fact(&self) -> StorageResult<Option<ReactiveCollectionPersistenceErrorFact>> {
        let Some(value) = self.error.cache() else {
            return Ok(None);
        };
        if value.is_null() {
            return Ok(None);
        }
        parse_error_fact(&value).map(Some)
    }
}

impl Drop for ReactiveCollectionPersistence {
    fn drop(&mut self) {
        if let Some(dispose) = self.dispose.borrow_mut().take() {
            dispose();
        }
    }
}

pub struct OpenPersistentReactiveList<T> {
    pub collection: ReactiveList<T>,
    pub persistence: ReactiveCollectionPersistence,
}

pub struct OpenPersistentReactiveLog<T> {
    pub collection: ReactiveLog<T>,
    pub persistence: ReactiveCollectionPersistence,
}

pub struct OpenPersistentReactiveMap<K, V> {
    pub collection: ReactiveMap<K, V>,
    pub persistence: ReactiveCollectionPersistence,
}

pub struct OpenPersistentReactiveIndex<K, S, V> {
    pub collection: ReactiveIndex<K, S, V>,
    pub persistence: ReactiveCollectionPersistence,
}

pub struct OpenPersistentReactiveListOptions<T> {
    pub initial: Vec<T>,
    pub collection: ReactiveListOptions,
    pub persistence: PersistReactiveCollectionOptions,
}

pub struct OpenPersistentReactiveLogOptions<T> {
    pub initial: Vec<T>,
    pub collection: ReactiveLogOptions,
    pub persistence: PersistReactiveCollectionOptions,
}

pub struct OpenPersistentReactiveMapOptions<K, V> {
    pub initial: Vec<(K, V)>,
    pub collection: ReactiveMapOptions,
    pub persistence: PersistReactiveCollectionOptions,
}

pub struct OpenPersistentReactiveIndexOptions<K, S, V> {
    pub initial: Vec<IndexRow<K, S, V>>,
    pub collection: ReactiveIndexOptions,
    pub persistence: PersistReactiveCollectionOptions,
}

pub fn persist_reactive_list<T>(
    collection: &ReactiveList<T>,
    options: PersistReactiveCollectionOptions,
) -> StorageResult<ReactiveCollectionPersistence>
where
    T: Clone + Serialize + 'static,
{
    persist_collection(
        ReactiveCollectionKind::ReactiveList,
        collection.to_vec(),
        &collection.delta,
        options,
        None,
        |change| serde_json::to_value(change).map_err(storage_json_error),
        apply_list_snapshot_change::<T>,
    )
}

pub fn persist_reactive_log<T>(
    collection: &ReactiveLog<T>,
    options: PersistReactiveCollectionOptions,
) -> StorageResult<ReactiveCollectionPersistence>
where
    T: Clone + Serialize + 'static,
{
    persist_collection(
        ReactiveCollectionKind::ReactiveLog,
        collection.to_vec(),
        &collection.delta,
        options,
        None,
        |change| serde_json::to_value(change).map_err(storage_json_error),
        apply_log_snapshot_change::<T>,
    )
}

pub fn persist_reactive_map<K, V>(
    collection: &ReactiveMap<K, V>,
    options: PersistReactiveCollectionOptions,
) -> StorageResult<ReactiveCollectionPersistence>
where
    K: Clone + Ord + std::fmt::Debug + Serialize + 'static,
    V: Clone + Serialize + 'static,
{
    persist_collection(
        ReactiveCollectionKind::ReactiveMap,
        collection.to_map().into_iter().collect::<Vec<_>>(),
        &collection.delta,
        options,
        None,
        |change| serde_json::to_value(change).map_err(storage_json_error),
        apply_map_snapshot_change::<K, V>,
    )
}

pub fn persist_reactive_index<K, S, V>(
    collection: &ReactiveIndex<K, S, V>,
    options: PersistReactiveCollectionOptions,
) -> StorageResult<ReactiveCollectionPersistence>
where
    K: Clone + Ord + std::fmt::Debug + Serialize + 'static,
    S: Clone + Ord + Serialize + 'static,
    V: Clone + Serialize + 'static,
{
    persist_collection(
        ReactiveCollectionKind::ReactiveIndex,
        collection.to_vec(),
        &collection.delta,
        options,
        None,
        |change| serde_json::to_value(change).map_err(storage_json_error),
        apply_index_snapshot_change::<K, S, V>,
    )
}

fn persist_reactive_list_with_cursor<T>(
    collection: &ReactiveList<T>,
    options: PersistReactiveCollectionOptions,
    initial_cursor: Option<u64>,
) -> StorageResult<ReactiveCollectionPersistence>
where
    T: Clone + Serialize + 'static,
{
    persist_collection(
        ReactiveCollectionKind::ReactiveList,
        collection.to_vec(),
        &collection.delta,
        options,
        initial_cursor,
        |change| serde_json::to_value(change).map_err(storage_json_error),
        apply_list_snapshot_change::<T>,
    )
}

fn persist_reactive_log_with_cursor<T>(
    collection: &ReactiveLog<T>,
    options: PersistReactiveCollectionOptions,
    initial_cursor: Option<u64>,
) -> StorageResult<ReactiveCollectionPersistence>
where
    T: Clone + Serialize + 'static,
{
    persist_collection(
        ReactiveCollectionKind::ReactiveLog,
        collection.to_vec(),
        &collection.delta,
        options,
        initial_cursor,
        |change| serde_json::to_value(change).map_err(storage_json_error),
        apply_log_snapshot_change::<T>,
    )
}

fn persist_reactive_map_with_cursor<K, V>(
    collection: &ReactiveMap<K, V>,
    options: PersistReactiveCollectionOptions,
    initial_cursor: Option<u64>,
) -> StorageResult<ReactiveCollectionPersistence>
where
    K: Clone + Ord + std::fmt::Debug + Serialize + 'static,
    V: Clone + Serialize + 'static,
{
    persist_collection(
        ReactiveCollectionKind::ReactiveMap,
        collection.to_map().into_iter().collect::<Vec<_>>(),
        &collection.delta,
        options,
        initial_cursor,
        |change| serde_json::to_value(change).map_err(storage_json_error),
        apply_map_snapshot_change::<K, V>,
    )
}

fn persist_reactive_index_with_cursor<K, S, V>(
    collection: &ReactiveIndex<K, S, V>,
    options: PersistReactiveCollectionOptions,
    initial_cursor: Option<u64>,
) -> StorageResult<ReactiveCollectionPersistence>
where
    K: Clone + Ord + std::fmt::Debug + Serialize + 'static,
    S: Clone + Ord + Serialize + 'static,
    V: Clone + Serialize + 'static,
{
    persist_collection(
        ReactiveCollectionKind::ReactiveIndex,
        collection.to_vec(),
        &collection.delta,
        options,
        initial_cursor,
        |change| serde_json::to_value(change).map_err(storage_json_error),
        apply_index_snapshot_change::<K, S, V>,
    )
}

pub fn open_persistent_reactive_list<T>(
    options: OpenPersistentReactiveListOptions<T>,
) -> StorageResult<OpenPersistentReactiveList<T>>
where
    T: Clone + Serialize + DeserializeOwned + 'static,
{
    let mut state = load_reactive_list_state(
        options.persistence.snapshot_store.as_ref(),
        LoadReactiveCollectionStateOptions {
            storage_prefix: options.persistence.storage_prefix.as_deref(),
            snapshot_key: options.persistence.snapshot_key.as_deref(),
            change_log: options.persistence.change_log.as_deref(),
        },
    )?;
    if !state.snapshot_found && state.changes_applied == 0 {
        state.state = options.initial;
    }
    let initial_cursor = state.cursor;
    let collection = restore_reactive_list(state, options.collection)?;
    let persistence =
        persist_reactive_list_with_cursor(&collection, options.persistence, initial_cursor)?;
    Ok(OpenPersistentReactiveList {
        collection,
        persistence,
    })
}

pub fn open_persistent_reactive_log<T>(
    options: OpenPersistentReactiveLogOptions<T>,
) -> StorageResult<OpenPersistentReactiveLog<T>>
where
    T: Clone + Serialize + DeserializeOwned + 'static,
{
    let mut state = load_reactive_log_state(
        options.persistence.snapshot_store.as_ref(),
        LoadReactiveCollectionStateOptions {
            storage_prefix: options.persistence.storage_prefix.as_deref(),
            snapshot_key: options.persistence.snapshot_key.as_deref(),
            change_log: options.persistence.change_log.as_deref(),
        },
    )?;
    if !state.snapshot_found && state.changes_applied == 0 {
        state.state = options.initial;
    }
    let initial_cursor = state.cursor;
    let collection = restore_reactive_log(state, options.collection)?;
    let persistence =
        persist_reactive_log_with_cursor(&collection, options.persistence, initial_cursor)?;
    Ok(OpenPersistentReactiveLog {
        collection,
        persistence,
    })
}

pub fn open_persistent_reactive_map<K, V>(
    options: OpenPersistentReactiveMapOptions<K, V>,
) -> StorageResult<OpenPersistentReactiveMap<K, V>>
where
    K: Clone + Ord + std::fmt::Debug + Serialize + DeserializeOwned + 'static,
    V: Clone + Serialize + DeserializeOwned + 'static,
{
    let mut state = load_reactive_map_state(
        options.persistence.snapshot_store.as_ref(),
        LoadReactiveCollectionStateOptions {
            storage_prefix: options.persistence.storage_prefix.as_deref(),
            snapshot_key: options.persistence.snapshot_key.as_deref(),
            change_log: options.persistence.change_log.as_deref(),
        },
    )?;
    if matches!(
        state.source,
        crate::storage::ReactiveCollectionRestoreSource::Empty
    ) {
        state.state = options.initial;
    }
    let initial_cursor = state.cursor;
    let collection = restore_reactive_map(state, options.collection)?;
    let persistence =
        persist_reactive_map_with_cursor(&collection, options.persistence, initial_cursor)?;
    Ok(OpenPersistentReactiveMap {
        collection,
        persistence,
    })
}

pub fn open_persistent_reactive_index<K, S, V>(
    options: OpenPersistentReactiveIndexOptions<K, S, V>,
) -> StorageResult<OpenPersistentReactiveIndex<K, S, V>>
where
    K: Clone + Ord + std::fmt::Debug + Serialize + DeserializeOwned + 'static,
    S: Clone + Ord + Serialize + DeserializeOwned + 'static,
    V: Clone + Serialize + DeserializeOwned + 'static,
{
    let mut state = load_reactive_index_state(
        options.persistence.snapshot_store.as_ref(),
        LoadReactiveCollectionStateOptions {
            storage_prefix: options.persistence.storage_prefix.as_deref(),
            snapshot_key: options.persistence.snapshot_key.as_deref(),
            change_log: options.persistence.change_log.as_deref(),
        },
    )?;
    if matches!(
        state.source,
        crate::storage::ReactiveCollectionRestoreSource::Empty
    ) {
        state.state = options.initial;
    }
    let initial_cursor = state.cursor;
    let collection = restore_reactive_index(state, options.collection)?;
    let persistence =
        persist_reactive_index_with_cursor(&collection, options.persistence, initial_cursor)?;
    Ok(OpenPersistentReactiveIndex {
        collection,
        persistence,
    })
}

fn persist_collection<C, S, F, A>(
    kind: ReactiveCollectionKind,
    snapshot_state: S,
    delta: &Node<C>,
    options: PersistReactiveCollectionOptions,
    initial_cursor: Option<u64>,
    encode_change: F,
    apply_change: A,
) -> StorageResult<ReactiveCollectionPersistence>
where
    C: Clone + 'static,
    S: Serialize + Clone + 'static,
    F: Fn(&C) -> StorageResult<Value> + 'static,
    A: Fn(&mut S, &C) -> StorageResult<()> + 'static,
{
    let snapshot_key = resolve_snapshot_key(&options)?;
    if let Some(every) = options.snapshot_every_changes {
        if every == 0 {
            return Err(StorageError::backend(
                "persistReactiveCollection: snapshot_every_changes must be positive",
            ));
        }
    }
    let graph = options.graph.as_ref().ok_or_else(|| {
        StorageError::backend(
            "persistReactiveCollection: graph is required for graph-visible sidecar facts",
        )
    })?;
    if !graph.contains_core(&delta.erased()) {
        return Err(StorageError::backend(
            "persistReactiveCollection: collection delta belongs to a different graph or is not graph-registered",
        ));
    }
    let fact_prefix = options
        .name
        .clone()
        .or_else(|| options.storage_prefix.clone())
        .unwrap_or_else(|| "reactiveCollection.persistence".to_owned());
    let ready = persistence_fact(graph, format!("{fact_prefix}.ready"), false);
    let status = persistence_fact(
        graph,
        format!("{fact_prefix}.status"),
        status_json(
            ReactiveCollectionPersistenceStatus::Starting,
            0,
            0,
            cursor_json(kind, initial_cursor, 0, 0),
        ),
    );
    let error = persistence_fact(graph, format!("{fact_prefix}.error"), Value::Null);
    let cursor = persistence_fact(
        graph,
        format!("{fact_prefix}.cursor"),
        cursor_json(kind, initial_cursor, 0, 0),
    );

    let disposed = Rc::new(Cell::new(false));
    let errored = Rc::new(Cell::new(false));
    let cursor_cell = Rc::new(Cell::new(initial_cursor));
    let snapshot_writes = Rc::new(Cell::new(0usize));
    let change_writes = Rc::new(Cell::new(0usize));
    let error_count = Rc::new(Cell::new(0usize));
    let snapshot_state = Rc::new(RefCell::new(snapshot_state));
    let pending_changes = Rc::new(RefCell::new(Vec::<ReactiveCollectionChangeFrame>::new()));
    let snapshot_dirty = Rc::new(Cell::new(false));
    let cadence_snapshot_due = Rc::new(Cell::new(false));
    let snapshot_store = options.snapshot_store.clone();
    let change_log = options.change_log.clone();
    let encode_change = Rc::new(encode_change);
    let apply_change = Rc::new(apply_change);
    let snapshot_every_changes = options.snapshot_every_changes;
    let changes_since_snapshot = Rc::new(Cell::new(0usize));

    let write_snapshot = {
        let ready = ready.clone();
        let status = status.clone();
        let error = error.clone();
        let cursor = cursor.clone();
        let snapshot_key = snapshot_key.clone();
        let snapshot_store = snapshot_store.clone();
        let snapshot_state = snapshot_state.clone();
        let cursor_cell = cursor_cell.clone();
        let snapshot_writes = snapshot_writes.clone();
        let change_writes = change_writes.clone();
        let error_count = error_count.clone();
        let pending_changes = pending_changes.clone();
        let snapshot_dirty = snapshot_dirty.clone();
        let cadence_snapshot_due = cadence_snapshot_due.clone();
        let change_log = change_log.clone();
        let errored = errored.clone();
        move || {
            drain_pending_changes(
                pending_changes.clone(),
                change_log.clone(),
                cursor_cell.clone(),
                cursor.clone(),
                kind,
                snapshot_writes.clone(),
                change_writes.clone(),
            )?;
            let cursor_value = cursor_cell
                .get()
                .map(seq_to_snapshot_cursor)
                .transpose()?
                .unwrap_or(-1);
            let snapshot_value = serde_json::to_value(snapshot_state.borrow().clone())
                .map_err(storage_json_error)?;
            let frame = reactive_collection_snapshot_frame(kind, cursor_value, snapshot_value)?;
            snapshot_store.set(&snapshot_key, frame)?;
            snapshot_writes.set(snapshot_writes.get().saturating_add(1));
            let cursor_value = cursor_json(
                kind,
                cursor_cell.get(),
                snapshot_writes.get(),
                change_writes.get(),
            );
            if !errored.get() {
                ready.set(true);
                snapshot_dirty.set(false);
                cadence_snapshot_due.set(false);
                ready.set(true);
                status.set(status_json(
                    ReactiveCollectionPersistenceStatus::Ready,
                    pending_changes.borrow().len(),
                    error_count.get(),
                    cursor_value.clone(),
                ));
                error.set(Value::Null);
                cursor.set(cursor_value);
            }
            Ok(())
        }
    };

    if options.snapshot_on_attach {
        if let Err(err) = write_snapshot() {
            record_persistence_error(
                PersistenceErrorTargets {
                    ready: &ready,
                    status: &status,
                    error: &error,
                    errored: &errored,
                    error_count: &error_count,
                },
                PersistenceErrorFacts {
                    kind,
                    cursor: cursor_cell.get(),
                    snapshot_writes: snapshot_writes.get(),
                    change_writes: change_writes.get(),
                    pending: pending_changes.borrow().len(),
                },
                "snapshot",
                err,
            );
        }
    } else {
        ready.set(true);
        status.set(status_json(
            ReactiveCollectionPersistenceStatus::Ready,
            pending_changes.borrow().len(),
            error_count.get(),
            cursor_json(
                kind,
                cursor_cell.get(),
                snapshot_writes.get(),
                change_writes.get(),
            ),
        ));
    }

    let write_snapshot_for_control = Rc::new(write_snapshot);
    let flush = {
        let disposed = disposed.clone();
        let errored = errored.clone();
        let ready = ready.clone();
        let status = status.clone();
        let error = error.clone();
        let cursor = cursor.clone();
        let cursor_cell = cursor_cell.clone();
        let snapshot_writes = snapshot_writes.clone();
        let change_writes = change_writes.clone();
        let error_count = error_count.clone();
        let pending_changes = pending_changes.clone();
        let change_log = change_log.clone();
        let snapshot_dirty = snapshot_dirty.clone();
        let cadence_snapshot_due = cadence_snapshot_due.clone();
        let write_snapshot = write_snapshot_for_control.clone();
        move || {
            if disposed.get() {
                return Ok(());
            }
            if errored.get() {
                return Err(StorageError::backend(
                    "reactiveCollection persistence is errored; inspect error fact",
                ));
            }
            let writes_snapshot =
                (change_log.is_none() && snapshot_dirty.get()) || cadence_snapshot_due.get();
            let result = if writes_snapshot {
                write_snapshot()
            } else {
                match drain_pending_changes(
                    pending_changes.clone(),
                    change_log.clone(),
                    cursor_cell.clone(),
                    cursor.clone(),
                    kind,
                    snapshot_writes.clone(),
                    change_writes.clone(),
                ) {
                    Ok(()) => {
                        ready.set(true);
                        status.set(status_json(
                            ReactiveCollectionPersistenceStatus::Ready,
                            pending_changes.borrow().len(),
                            error_count.get(),
                            cursor_json(
                                kind,
                                cursor_cell.get(),
                                snapshot_writes.get(),
                                change_writes.get(),
                            ),
                        ));
                        error.set(Value::Null);
                        Ok(())
                    }
                    Err(err) => Err(err),
                }
            };
            if let Err(err) = result {
                record_persistence_error(
                    PersistenceErrorTargets {
                        ready: &ready,
                        status: &status,
                        error: &error,
                        errored: &errored,
                        error_count: &error_count,
                    },
                    PersistenceErrorFacts {
                        kind,
                        cursor: cursor_cell.get(),
                        snapshot_writes: snapshot_writes.get(),
                        change_writes: change_writes.get(),
                        pending: pending_changes.borrow().len(),
                    },
                    if writes_snapshot {
                        "snapshot"
                    } else {
                        "change"
                    },
                    err.clone(),
                );
                return Err(err);
            }
            Ok(())
        }
    };
    let snapshot = {
        let disposed = disposed.clone();
        let write_snapshot = write_snapshot_for_control.clone();
        let ready = ready.clone();
        let status = status.clone();
        let error = error.clone();
        let errored = errored.clone();
        let cursor_cell = cursor_cell.clone();
        let snapshot_writes = snapshot_writes.clone();
        let change_writes = change_writes.clone();
        let error_count = error_count.clone();
        let pending_changes = pending_changes.clone();
        move || {
            if disposed.get() {
                return Err(StorageError::backend(
                    "reactiveCollection persistence is disposed",
                ));
            }
            match write_snapshot() {
                Ok(()) => Ok(()),
                Err(err) => {
                    record_persistence_error(
                        PersistenceErrorTargets {
                            ready: &ready,
                            status: &status,
                            error: &error,
                            errored: &errored,
                            error_count: &error_count,
                        },
                        PersistenceErrorFacts {
                            kind,
                            cursor: cursor_cell.get(),
                            snapshot_writes: snapshot_writes.get(),
                            change_writes: change_writes.get(),
                            pending: pending_changes.borrow().len(),
                        },
                        "snapshot",
                        err.clone(),
                    );
                    Err(err)
                }
            }
        }
    };

    let ready_for_sub = ready.clone();
    let status_for_sub = status.clone();
    let error_for_sub = error.clone();
    let snapshot_state_for_sub = snapshot_state.clone();
    let disposed_for_sub = disposed.clone();
    let errored_for_sub = errored.clone();
    let cursor_cell_for_sub = cursor_cell.clone();
    let snapshot_writes_for_sub = snapshot_writes.clone();
    let change_writes_for_sub = change_writes.clone();
    let error_count_for_sub = error_count.clone();
    let change_log_for_sub = change_log.clone();
    let apply_change_for_sub = apply_change.clone();
    let pending_changes_for_sub = pending_changes.clone();
    let changes_since_snapshot_for_sub = changes_since_snapshot.clone();
    let snapshot_dirty_for_sub = snapshot_dirty.clone();
    let cadence_snapshot_due_for_sub = cadence_snapshot_due.clone();
    let armed = Rc::new(Cell::new(false));
    let armed_for_sub = armed.clone();
    let unsub = delta.subscribe(move |message| {
        if disposed_for_sub.get() || errored_for_sub.get() {
            return;
        }
        let Message::Data(value) = message else {
            return;
        };
        if !armed_for_sub.get() {
            return;
        }
        let Some(change) = value.as_ref().downcast_ref::<C>() else {
            return;
        };
        ready_for_sub.set(false);
        status_for_sub.set(status_json(
            ReactiveCollectionPersistenceStatus::Flushing,
            pending_changes_for_sub.borrow().len(),
            error_count_for_sub.get(),
            cursor_json(
                kind,
                cursor_cell_for_sub.get(),
                snapshot_writes_for_sub.get(),
                change_writes_for_sub.get(),
            ),
        ));
        let result = (|| {
            apply_change_for_sub(&mut snapshot_state_for_sub.borrow_mut(), change)?;
            let change_value = encode_change(change)?;
            let frame = reactive_collection_change_frame(kind, change_value)?;
            if change_log_for_sub.is_some() {
                pending_changes_for_sub.borrow_mut().push(frame);
            } else {
                snapshot_dirty_for_sub.set(true);
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                error_for_sub.set(Value::Null);
                let next = changes_since_snapshot_for_sub.get().saturating_add(1);
                changes_since_snapshot_for_sub.set(next);
                if snapshot_every_changes.is_some_and(|every| next >= every) {
                    changes_since_snapshot_for_sub.set(0);
                    cadence_snapshot_due_for_sub.set(true);
                }
            }
            Err(err) => {
                record_persistence_error(
                    PersistenceErrorTargets {
                        ready: &ready_for_sub,
                        status: &status_for_sub,
                        error: &error_for_sub,
                        errored: &errored_for_sub,
                        error_count: &error_count_for_sub,
                    },
                    PersistenceErrorFacts {
                        kind,
                        cursor: cursor_cell_for_sub.get(),
                        snapshot_writes: snapshot_writes_for_sub.get(),
                        change_writes: change_writes_for_sub.get(),
                        pending: pending_changes_for_sub.borrow().len(),
                    },
                    "change",
                    err,
                );
            }
        }
    });
    armed.set(true);

    let dispose = {
        let ready = ready.clone();
        let status = status.clone();
        let disposed = disposed.clone();
        let cursor_cell = cursor_cell.clone();
        let snapshot_writes = snapshot_writes.clone();
        let change_writes = change_writes.clone();
        let error_count = error_count.clone();
        let pending_changes = pending_changes.clone();
        let cadence_snapshot_due = cadence_snapshot_due.clone();
        Box::new(move || {
            disposed.set(true);
            pending_changes.borrow_mut().clear();
            cadence_snapshot_due.set(false);
            ready.set(false);
            status.set(status_json(
                ReactiveCollectionPersistenceStatus::Disposed,
                pending_changes.borrow().len(),
                error_count.get(),
                cursor_json(
                    kind,
                    cursor_cell.get(),
                    snapshot_writes.get(),
                    change_writes.get(),
                ),
            ));
            unsub();
        }) as Disposer
    };

    Ok(ReactiveCollectionPersistence {
        ready,
        status,
        error,
        cursor,
        flush: Rc::new(flush),
        snapshot: Rc::new(snapshot),
        dispose: RefCell::new(Some(dispose)),
    })
}

fn drain_pending_changes(
    pending_changes: Rc<RefCell<Vec<ReactiveCollectionChangeFrame>>>,
    change_log: Option<Rc<dyn AppendLogStorageTier<ReactiveCollectionChangeFrame>>>,
    cursor_cell: Rc<Cell<Option<u64>>>,
    cursor: Node<Value>,
    kind: ReactiveCollectionKind,
    snapshot_writes: Rc<Cell<usize>>,
    change_writes: Rc<Cell<usize>>,
) -> StorageResult<()> {
    let Some(log) = change_log else {
        pending_changes.borrow_mut().clear();
        return Ok(());
    };
    loop {
        let frame = { pending_changes.borrow().first().cloned() };
        let Some(frame) = frame else {
            break;
        };
        let entry = log.append(frame)?;
        cursor_cell.set(Some(entry.seq));
        change_writes.set(change_writes.get().saturating_add(1));
        cursor.set(cursor_json(
            kind,
            Some(entry.seq),
            snapshot_writes.get(),
            change_writes.get(),
        ));
        pending_changes.borrow_mut().remove(0);
    }
    Ok(())
}

fn persistence_fact<T: 'static>(graph: &Graph, name: String, initial: T) -> Node<T> {
    graph.state_opts(initial, GraphNodeOpts::named(name))
}

fn resolve_snapshot_key(options: &PersistReactiveCollectionOptions) -> StorageResult<String> {
    if let Some(key) = options.snapshot_key.as_ref() {
        if key.is_empty() {
            return Err(StorageError::backend(
                "persistReactiveCollection: snapshot_key must be non-empty",
            ));
        }
        return Ok(key.clone());
    }
    let prefix = options.storage_prefix.as_ref().ok_or_else(|| {
        StorageError::backend(
            "persistReactiveCollection: storage_prefix or snapshot_key is required",
        )
    })?;
    reactive_collection_snapshot_key(prefix)
}

fn seq_to_snapshot_cursor(seq: u64) -> StorageResult<i64> {
    i64::try_from(seq)
        .map_err(|_| StorageError::backend("reactiveCollection cursor exceeds i64 range"))
}

fn storage_json_error(error: serde_json::Error) -> StorageError {
    StorageError::backend(format!("reactiveCollection JSON error: {error}"))
}

struct PersistenceErrorTargets<'a> {
    ready: &'a Node<bool>,
    status: &'a Node<Value>,
    error: &'a Node<Value>,
    errored: &'a Cell<bool>,
    error_count: &'a Cell<usize>,
}

struct PersistenceErrorFacts {
    kind: ReactiveCollectionKind,
    cursor: Option<u64>,
    snapshot_writes: usize,
    change_writes: usize,
    pending: usize,
}

fn record_persistence_error(
    targets: PersistenceErrorTargets<'_>,
    facts: PersistenceErrorFacts,
    phase: &str,
    err: StorageError,
) {
    targets.errored.set(true);
    targets
        .error_count
        .set(targets.error_count.get().saturating_add(1));
    let cursor = cursor_json(
        facts.kind,
        facts.cursor,
        facts.snapshot_writes,
        facts.change_writes,
    );
    targets.ready.set(false);
    targets.status.set(status_json(
        ReactiveCollectionPersistenceStatus::Errored,
        facts.pending,
        targets.error_count.get(),
        cursor.clone(),
    ));
    targets.error.set(json!({
        "phase": phase,
        "message": err.to_string(),
        "cursor": cursor
    }));
}

fn cursor_json(
    collection: ReactiveCollectionKind,
    cursor: Option<u64>,
    snapshot_writes: usize,
    change_writes: usize,
) -> Value {
    json!({
        "kind": "persistence.cursor",
        "collection": collection_name(collection),
        "changeSeq": cursor.map_or(-1_i64, |seq| i64::try_from(seq).unwrap_or(i64::MAX)),
        "snapshotWrites": snapshot_writes,
        "changeWrites": change_writes
    })
}

fn status_json(
    state: ReactiveCollectionPersistenceStatus,
    pending: usize,
    errors: usize,
    cursor: Value,
) -> Value {
    json!({
        "state": status_name(&state),
        "pending": pending,
        "writes": cursor
            .get("snapshotWrites")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            .saturating_add(cursor.get("changeWrites").and_then(Value::as_u64).unwrap_or(0)),
        "errors": errors,
        "cursor": cursor
    })
}

fn collection_name(kind: ReactiveCollectionKind) -> &'static str {
    match kind {
        ReactiveCollectionKind::ReactiveList => "reactiveList",
        ReactiveCollectionKind::ReactiveLog => "reactiveLog",
        ReactiveCollectionKind::ReactiveMap => "reactiveMap",
        ReactiveCollectionKind::ReactiveIndex => "reactiveIndex",
    }
}

fn status_name(status: &ReactiveCollectionPersistenceStatus) -> &'static str {
    match status {
        ReactiveCollectionPersistenceStatus::Starting => "starting",
        ReactiveCollectionPersistenceStatus::Ready => "ready",
        ReactiveCollectionPersistenceStatus::Flushing => "flushing",
        ReactiveCollectionPersistenceStatus::Errored => "errored",
        ReactiveCollectionPersistenceStatus::Disposed => "disposed",
    }
}

fn parse_status_fact(value: &Value) -> StorageResult<ReactiveCollectionPersistenceStatusFact> {
    let object = value
        .as_object()
        .ok_or_else(|| StorageError::backend("reactiveCollection status fact must be an object"))?;
    let state = parse_status(
        object
            .get("state")
            .and_then(Value::as_str)
            .ok_or_else(|| StorageError::backend("reactiveCollection status.state is missing"))?,
    )?;
    let pending = parse_usize_field(object.get("pending"), "reactiveCollection status.pending")?;
    let writes = parse_usize_field(object.get("writes"), "reactiveCollection status.writes")?;
    let errors = parse_usize_field(object.get("errors"), "reactiveCollection status.errors")?;
    let cursor =
        parse_cursor_fact(object.get("cursor").ok_or_else(|| {
            StorageError::backend("reactiveCollection status.cursor is missing")
        })?)?;
    Ok(ReactiveCollectionPersistenceStatusFact {
        state,
        pending,
        writes,
        errors,
        cursor,
    })
}

fn parse_error_fact(value: &Value) -> StorageResult<ReactiveCollectionPersistenceErrorFact> {
    let object = value
        .as_object()
        .ok_or_else(|| StorageError::backend("reactiveCollection error fact must be an object"))?;
    let phase = object
        .get("phase")
        .and_then(Value::as_str)
        .ok_or_else(|| StorageError::backend("reactiveCollection error.phase is missing"))?
        .to_owned();
    let message = object
        .get("message")
        .and_then(Value::as_str)
        .ok_or_else(|| StorageError::backend("reactiveCollection error.message is missing"))?
        .to_owned();
    let cursor = parse_cursor_fact(
        object
            .get("cursor")
            .ok_or_else(|| StorageError::backend("reactiveCollection error.cursor is missing"))?,
    )?;
    Ok(ReactiveCollectionPersistenceErrorFact {
        phase,
        message,
        cursor,
    })
}

fn parse_cursor_fact(value: &Value) -> StorageResult<ReactiveCollectionPersistenceCursor> {
    let object = value
        .as_object()
        .ok_or_else(|| StorageError::backend("reactiveCollection cursor fact must be an object"))?;
    if object.get("kind").and_then(Value::as_str) != Some("persistence.cursor") {
        return Err(StorageError::backend(
            "reactiveCollection cursor.kind must be persistence.cursor",
        ));
    }
    let collection = parse_collection_kind(
        object
            .get("collection")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                StorageError::backend("reactiveCollection cursor.collection is missing")
            })?,
    )?;
    let change_seq = object
        .get("changeSeq")
        .and_then(Value::as_i64)
        .ok_or_else(|| StorageError::backend("reactiveCollection cursor.changeSeq is missing"))?;
    let change_seq = if change_seq < 0 {
        None
    } else {
        Some(change_seq as u64)
    };
    Ok(ReactiveCollectionPersistenceCursor {
        collection,
        change_seq,
        snapshot_writes: parse_usize_field(
            object.get("snapshotWrites"),
            "reactiveCollection cursor.snapshotWrites",
        )?,
        change_writes: parse_usize_field(
            object.get("changeWrites"),
            "reactiveCollection cursor.changeWrites",
        )?,
    })
}

fn parse_status(value: &str) -> StorageResult<ReactiveCollectionPersistenceStatus> {
    match value {
        "starting" => Ok(ReactiveCollectionPersistenceStatus::Starting),
        "ready" => Ok(ReactiveCollectionPersistenceStatus::Ready),
        "flushing" => Ok(ReactiveCollectionPersistenceStatus::Flushing),
        "errored" => Ok(ReactiveCollectionPersistenceStatus::Errored),
        "disposed" => Ok(ReactiveCollectionPersistenceStatus::Disposed),
        _ => Err(StorageError::backend(format!(
            "reactiveCollection status.state is unsupported: {value}"
        ))),
    }
}

fn parse_collection_kind(value: &str) -> StorageResult<ReactiveCollectionKind> {
    match value {
        "reactiveList" => Ok(ReactiveCollectionKind::ReactiveList),
        "reactiveLog" => Ok(ReactiveCollectionKind::ReactiveLog),
        "reactiveMap" => Ok(ReactiveCollectionKind::ReactiveMap),
        "reactiveIndex" => Ok(ReactiveCollectionKind::ReactiveIndex),
        _ => Err(StorageError::backend(format!(
            "reactiveCollection cursor.collection is unsupported: {value}"
        ))),
    }
}

fn parse_usize_field(value: Option<&Value>, label: &str) -> StorageResult<usize> {
    let value = value
        .and_then(Value::as_u64)
        .ok_or_else(|| StorageError::backend(format!("{label} must be a non-negative integer")))?;
    usize::try_from(value).map_err(|_| StorageError::backend(format!("{label} exceeds usize")))
}

fn apply_list_snapshot_change<T>(snapshot: &mut Vec<T>, change: &ListChange<T>) -> StorageResult<()>
where
    T: Clone + Serialize,
{
    match change {
        ListChange::Append { value } => snapshot.push(value.clone()),
        ListChange::AppendMany { values } => snapshot.extend(values.clone()),
        ListChange::Insert { index, value } => {
            if *index > snapshot.len() {
                return Err(StorageError::backend(
                    "reactiveList persistence mirror: insert index out of bounds",
                ));
            }
            snapshot.insert(*index, value.clone());
        }
        ListChange::InsertMany { index, values } => {
            if *index > snapshot.len() {
                return Err(StorageError::backend(
                    "reactiveList persistence mirror: insertMany index out of bounds",
                ));
            }
            snapshot.splice(*index..*index, values.clone());
        }
        ListChange::Pop { index, value } => {
            if *index >= snapshot.len() {
                return Err(StorageError::backend(
                    "reactiveList persistence mirror: pop index out of bounds",
                ));
            }
            let actual = snapshot.remove(*index);
            if !strict_json_equal(&actual, value)? {
                return Err(StorageError::backend(
                    "reactiveList persistence mirror: pop value does not match stored state",
                ));
            }
        }
        ListChange::TrimHead { n } => {
            if *n > snapshot.len() {
                return Err(StorageError::backend(
                    "reactiveList persistence mirror: trimHead out of bounds",
                ));
            }
            snapshot.drain(0..*n);
        }
        ListChange::Clear { count } => {
            if *count != snapshot.len() {
                return Err(StorageError::backend(
                    "reactiveList persistence mirror: clear count does not match stored state",
                ));
            }
            snapshot.clear();
        }
    }
    Ok(())
}

fn apply_log_snapshot_change<T>(snapshot: &mut Vec<T>, change: &LogChange<T>) -> StorageResult<()>
where
    T: Clone,
{
    match change {
        LogChange::Append { value } => snapshot.push(value.clone()),
        LogChange::AppendMany { values } => snapshot.extend(values.clone()),
        LogChange::TrimHead { n } => {
            if *n > snapshot.len() {
                return Err(StorageError::backend(
                    "reactiveLog persistence mirror: trimHead out of bounds",
                ));
            }
            snapshot.drain(0..*n);
        }
        LogChange::Clear { count } => {
            if *count != snapshot.len() {
                return Err(StorageError::backend(
                    "reactiveLog persistence mirror: clear count does not match stored state",
                ));
            }
            snapshot.clear();
        }
    }
    Ok(())
}

fn apply_map_snapshot_change<K, V>(
    snapshot: &mut Vec<(K, V)>,
    change: &MapChange<K, V>,
) -> StorageResult<()>
where
    K: Clone + Serialize,
    V: Clone + Serialize,
{
    match change {
        MapChange::Set { key, value } => match find_map_key(snapshot, key)? {
            Some(index) => snapshot[index] = (key.clone(), value.clone()),
            None => snapshot.push((key.clone(), value.clone())),
        },
        MapChange::Delete { key, previous } => {
            let Some(index) = find_map_key(snapshot, key)? else {
                return Err(StorageError::backend(
                    "reactiveMap persistence mirror: delete key is missing",
                ));
            };
            if !strict_json_equal(&snapshot[index].1, previous)? {
                return Err(StorageError::backend(
                    "reactiveMap persistence mirror: delete previous value does not match stored state",
                ));
            }
            snapshot.remove(index);
        }
        MapChange::Clear { count } => {
            if *count != snapshot.len() {
                return Err(StorageError::backend(
                    "reactiveMap persistence mirror: clear count does not match stored state",
                ));
            }
            snapshot.clear();
        }
    }
    Ok(())
}

fn apply_index_snapshot_change<K, S, V>(
    snapshot: &mut Vec<IndexRow<K, S, V>>,
    change: &IndexChange<K, S, V>,
) -> StorageResult<()>
where
    K: Clone + Serialize,
    S: Clone + Serialize,
    V: Clone + Serialize,
{
    match change {
        IndexChange::Upsert {
            primary,
            secondary,
            value,
        } => {
            let row = IndexRow {
                primary: primary.clone(),
                secondary: secondary.clone(),
                value: value.clone(),
            };
            match find_index_primary(snapshot, primary)? {
                Some(index) => snapshot[index] = row,
                None => snapshot.push(row),
            }
        }
        IndexChange::Delete { primary } => {
            let Some(index) = find_index_primary(snapshot, primary)? else {
                return Err(StorageError::backend(
                    "reactiveIndex persistence mirror: delete primary is missing",
                ));
            };
            snapshot.remove(index);
        }
        IndexChange::DeleteMany { primaries } => {
            remove_index_primaries(
                snapshot,
                primaries,
                "reactiveIndex persistence mirror: deleteMany primary",
            )?;
        }
        IndexChange::Clear { count } => {
            if *count != snapshot.len() {
                return Err(StorageError::backend(
                    "reactiveIndex persistence mirror: clear count does not match stored state",
                ));
            }
            snapshot.clear();
        }
    }
    Ok(())
}

fn find_map_key<K, V>(entries: &[(K, V)], key: &K) -> StorageResult<Option<usize>>
where
    K: Serialize,
{
    let target = strict_json_identity(key)?;
    for (index, (candidate, _)) in entries.iter().enumerate() {
        if strict_json_identity(candidate)? == target {
            return Ok(Some(index));
        }
    }
    Ok(None)
}

fn find_index_primary<K, S, V>(
    rows: &[IndexRow<K, S, V>],
    primary: &K,
) -> StorageResult<Option<usize>>
where
    K: Serialize,
{
    let target = strict_json_identity(primary)?;
    for (index, row) in rows.iter().enumerate() {
        if strict_json_identity(&row.primary)? == target {
            return Ok(Some(index));
        }
    }
    Ok(None)
}

fn remove_index_primaries<K, S, V>(
    rows: &mut Vec<IndexRow<K, S, V>>,
    primaries: &[K],
    label: &str,
) -> StorageResult<()>
where
    K: Serialize,
{
    let mut seen = Vec::<Vec<u8>>::new();
    let mut indexes = Vec::<usize>::new();
    for (index, primary) in primaries.iter().enumerate() {
        let id = strict_json_identity(primary)?;
        if seen.iter().any(|existing| existing == &id) {
            return Err(StorageError::backend(format!(
                "{label} {index} duplicates an earlier primary"
            )));
        }
        seen.push(id);
        let Some(row_index) = find_index_primary(rows, primary)? else {
            return Err(StorageError::backend(format!("{label} {index} is missing")));
        };
        indexes.push(row_index);
    }
    indexes.sort_unstable_by(|a, b| b.cmp(a));
    for index in indexes {
        rows.remove(index);
    }
    Ok(())
}

fn strict_json_equal<T: Serialize>(left: &T, right: &T) -> StorageResult<bool> {
    let left = serde_json::to_value(left).map_err(storage_json_error)?;
    let right = serde_json::to_value(right).map_err(storage_json_error)?;
    let left = crate::strict_canonical_json_bytes(&left)
        .map_err(|err| StorageError::backend(format!("reactiveCollection JSON error: {err}")))?;
    let right = crate::strict_canonical_json_bytes(&right)
        .map_err(|err| StorageError::backend(format!("reactiveCollection JSON error: {err}")))?;
    Ok(left == right)
}

fn strict_json_identity<T: Serialize>(value: &T) -> StorageResult<Vec<u8>> {
    let value = serde_json::to_value(value).map_err(storage_json_error)?;
    crate::strict_canonical_json_bytes(&value)
        .map_err(|err| StorageError::backend(format!("reactiveCollection JSON error: {err}")))
}
