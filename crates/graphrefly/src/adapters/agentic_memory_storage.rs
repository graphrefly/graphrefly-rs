//! Graph-bound persistence sidecar for agentic-memory records (D172).
//!
//! This adapter composes D166 strict record frames with passive storage tiers.
//! It never restores a live graph and never adds storage fields to records.
//! The v1 sidecar assumes a single writer per chosen storage prefix/change log;
//! corrupt, sparse, or non-contiguous durable input fails honestly during load.

use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::rc::Rc;

use serde_json::{json, Value};

use crate::graph::{Graph, GraphNodeOpts};
use crate::json::{strict_canonical_json_bytes, strict_json_decode, Codec, JsonValue};
use crate::node::Node;
use crate::protocol::Message;
use crate::solutions::{
    agentic_memory_record_frame, agentic_memory_record_frame_codec, AgenticMemoryRecord,
    AgenticMemoryRecordFrame,
};
use crate::storage::{
    AppendLogReadOptions, AppendLogStorageTier, KvStorageTier, StorageError, StorageResult,
};

type Disposer = Box<dyn FnOnce()>;

pub const AGENTIC_MEMORY_RECORD_SNAPSHOT_FORMAT: &str = "graphrefly.agenticMemory.records.snapshot";
pub const AGENTIC_MEMORY_RECORD_CHANGE_FORMAT: &str = "graphrefly.agenticMemory.records.change";
pub const AGENTIC_MEMORY_RECORD_STORAGE_FRAME_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AgenticMemoryRecordsPersistenceStatus {
    Starting,
    Ready,
    Flushing,
    Errored,
    Disposed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgenticMemoryRecordsPersistenceCursor {
    pub change_seq: Option<u64>,
    pub snapshot_writes: usize,
    pub change_writes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgenticMemoryRecordsPersistenceStatusFact {
    pub state: AgenticMemoryRecordsPersistenceStatus,
    pub pending: usize,
    pub writes: usize,
    pub errors: usize,
    pub cursor: AgenticMemoryRecordsPersistenceCursor,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgenticMemoryRecordsPersistenceErrorFact {
    pub phase: String,
    pub message: String,
    pub cursor: AgenticMemoryRecordsPersistenceCursor,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AgenticMemoryRecordsRestoreState {
    pub records: Vec<AgenticMemoryRecord<JsonValue>>,
    pub snapshot_found: bool,
    pub changes_applied: usize,
    pub cursor: Option<u64>,
    pub source: String,
}

#[derive(Clone, Default)]
pub struct LoadAgenticMemoryRecordsStateOptions<'a> {
    pub storage_prefix: Option<&'a str>,
    pub snapshot_key: Option<&'a str>,
    pub change_log: Option<&'a dyn AppendLogStorageTier<Value>>,
}

#[derive(Clone)]
pub struct PersistAgenticMemoryRecordsOptions {
    pub graph: Option<Graph>,
    pub name: Option<String>,
    pub storage_prefix: Option<String>,
    pub snapshot_key: Option<String>,
    pub snapshot_store: Rc<dyn KvStorageTier<Value>>,
    /// Optional append-only change log.
    ///
    /// D172 v1 assumes one writer per selected log/prefix. Multi-writer merge,
    /// fencing, and conflict repair belong in a future adapter decision; this
    /// sidecar loads only contiguous frames and reports gaps as corruption.
    pub change_log: Option<Rc<dyn AppendLogStorageTier<Value>>>,
    pub snapshot_on_attach: bool,
}

impl PersistAgenticMemoryRecordsOptions {
    pub fn new(
        snapshot_store: Rc<dyn KvStorageTier<Value>>,
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
        }
    }
}

pub struct AgenticMemoryRecordsPersistence {
    pub ready: Node<bool>,
    pub status: Node<Value>,
    pub error: Node<Value>,
    pub cursor: Node<Value>,
    flush: Rc<dyn Fn() -> StorageResult<()>>,
    snapshot: Rc<dyn Fn() -> StorageResult<()>>,
    dispose: RefCell<Option<Disposer>>,
}

impl AgenticMemoryRecordsPersistence {
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

    pub fn cursor_fact(&self) -> StorageResult<AgenticMemoryRecordsPersistenceCursor> {
        parse_cursor_fact(
            &self.cursor.cache().ok_or_else(|| {
                StorageError::backend("agenticMemoryRecords cursor fact is absent")
            })?,
        )
    }
}

impl Drop for AgenticMemoryRecordsPersistence {
    fn drop(&mut self) {
        if let Some(dispose) = self.dispose.borrow_mut().take() {
            dispose();
        }
    }
}

pub struct OpenPersistentAgenticMemoryRecords {
    pub records: Node<Vec<AgenticMemoryRecord<JsonValue>>>,
    pub persistence: AgenticMemoryRecordsPersistence,
    pub loaded: AgenticMemoryRecordsRestoreState,
}

pub struct OpenPersistentAgenticMemoryRecordsOptions {
    pub graph: Graph,
    pub name: Option<String>,
    pub initial: Vec<AgenticMemoryRecord<JsonValue>>,
    pub persistence: PersistAgenticMemoryRecordsOptions,
}

pub fn agentic_memory_records_snapshot_key(storage_prefix: &str) -> StorageResult<String> {
    if storage_prefix.is_empty() {
        return Err(StorageError::backend("storage_prefix must be non-empty"));
    }
    Ok(format!("{storage_prefix}/records.snapshot"))
}

pub fn agentic_memory_record_snapshot_frame(
    records: &[AgenticMemoryRecord<JsonValue>],
    change_cursor: Option<u64>,
) -> StorageResult<Value> {
    let cursor = change_cursor
        .map(|seq| i64::try_from(seq).map_err(|_| StorageError::backend("change cursor overflow")))
        .transpose()?
        .unwrap_or(-1);
    let records = records
        .iter()
        .map(record_frame_json)
        .collect::<StorageResult<Vec<_>>>()?;
    Ok(json!({
        "format": AGENTIC_MEMORY_RECORD_SNAPSHOT_FORMAT,
        "version": AGENTIC_MEMORY_RECORD_STORAGE_FRAME_VERSION,
        "changeCursor": cursor,
        "records": records,
    }))
}

pub fn agentic_memory_record_change_frame(
    records: &[AgenticMemoryRecord<JsonValue>],
) -> StorageResult<Value> {
    let records = records
        .iter()
        .map(record_frame_json)
        .collect::<StorageResult<Vec<_>>>()?;
    Ok(json!({
        "format": AGENTIC_MEMORY_RECORD_CHANGE_FORMAT,
        "version": AGENTIC_MEMORY_RECORD_STORAGE_FRAME_VERSION,
        "change": { "kind": "replaceAll", "records": records },
    }))
}

pub fn load_agentic_memory_records_state(
    snapshot_store: &dyn KvStorageTier<Value>,
    options: LoadAgenticMemoryRecordsStateOptions<'_>,
) -> StorageResult<AgenticMemoryRecordsRestoreState> {
    let storage_prefix = options.storage_prefix.unwrap_or("agentic-memory");
    let snapshot_key = options
        .snapshot_key
        .map(ToOwned::to_owned)
        .unwrap_or(agentic_memory_records_snapshot_key(storage_prefix)?);
    let snapshot = snapshot_store.get(&snapshot_key)?;
    let mut records = Vec::new();
    let mut snapshot_found = false;
    let mut cursor = None;
    if let Some(snapshot) = snapshot {
        let parsed = parse_snapshot_frame(&snapshot)?;
        records = parsed.records;
        snapshot_found = true;
        cursor = parsed.change_cursor;
    }
    let mut changes_applied = 0usize;
    if let Some(change_log) = options.change_log {
        let entries = change_log.read(AppendLogReadOptions::default())?;
        let mut expected = cursor.map(|seq| seq.saturating_add(1)).unwrap_or(0);
        for entry in entries {
            if let Some(snapshot_cursor) = cursor {
                if entry.seq <= snapshot_cursor {
                    continue;
                }
            }
            if entry.seq != expected {
                return Err(StorageError::backend(
                    "agentic memory records change log is non-contiguous",
                ));
            }
            let change = parse_change_frame(&entry.value)?;
            records = change;
            cursor = Some(entry.seq);
            expected = entry.seq.saturating_add(1);
            changes_applied += 1;
        }
    }
    Ok(AgenticMemoryRecordsRestoreState {
        records,
        snapshot_found,
        changes_applied,
        cursor,
        source: match (snapshot_found, changes_applied > 0) {
            (true, true) => "snapshot+changes",
            (true, false) => "snapshot",
            (false, true) => "changes",
            (false, false) => "empty",
        }
        .to_owned(),
    })
}

pub fn persist_agentic_memory_records(
    records: &Node<Vec<AgenticMemoryRecord<JsonValue>>>,
    options: PersistAgenticMemoryRecordsOptions,
) -> StorageResult<AgenticMemoryRecordsPersistence> {
    persist_agentic_memory_records_with_cursor(records, options, None)
}

fn persist_agentic_memory_records_with_cursor(
    records: &Node<Vec<AgenticMemoryRecord<JsonValue>>>,
    options: PersistAgenticMemoryRecordsOptions,
    initial_change_seq: Option<u64>,
) -> StorageResult<AgenticMemoryRecordsPersistence> {
    let graph = options.graph.as_ref().ok_or_else(|| {
        StorageError::backend(
            "persistAgenticMemoryRecords: graph is required for graph-visible sidecar facts",
        )
    })?;
    if !graph.contains_core(&records.erased()) {
        return Err(StorageError::backend(
            "persistAgenticMemoryRecords: records node belongs to a different graph or is not graph-registered",
        ));
    }
    let prefix = options
        .name
        .clone()
        .or_else(|| options.storage_prefix.clone())
        .unwrap_or_else(|| "agenticMemoryRecords.persistence".to_owned());
    let snapshot_key = options
        .snapshot_key
        .clone()
        .unwrap_or(agentic_memory_records_snapshot_key(
            options.storage_prefix.as_deref().unwrap_or(&prefix),
        )?);
    let ready = graph.state_opts(false, GraphNodeOpts::named(format!("{prefix}.ready")));
    let status = graph.state_opts(
        status_json(
            AgenticMemoryRecordsPersistenceStatus::Starting,
            0,
            0,
            cursor_json(initial_change_seq, 0, 0),
        ),
        GraphNodeOpts::named(format!("{prefix}.status")),
    );
    let error = graph.state_opts(Value::Null, GraphNodeOpts::named(format!("{prefix}.error")));
    let cursor = graph.state_opts(
        cursor_json(initial_change_seq, 0, 0),
        GraphNodeOpts::named(format!("{prefix}.cursor")),
    );
    let latest = Rc::new(RefCell::new(records.cache().unwrap_or_default()));
    let change_seq = Rc::new(Cell::new(initial_change_seq));
    let snapshot_writes = Rc::new(Cell::new(0usize));
    let change_writes = Rc::new(Cell::new(0usize));
    let error_count = Rc::new(Cell::new(0usize));
    let disposed = Rc::new(Cell::new(false));
    let failed_write = Rc::new(RefCell::new(None::<StorageError>));
    let snapshot_store = options.snapshot_store.clone();
    let change_log = options.change_log.clone();

    let write_snapshot = {
        let latest = latest.clone();
        let snapshot_store = snapshot_store.clone();
        let snapshot_key = snapshot_key.clone();
        let change_seq = change_seq.clone();
        let snapshot_writes = snapshot_writes.clone();
        let change_writes = change_writes.clone();
        let ready = ready.clone();
        let status = status.clone();
        let error = error.clone();
        let cursor = cursor.clone();
        let failed_write = failed_write.clone();
        move || {
            let frame = agentic_memory_record_snapshot_frame(&latest.borrow(), change_seq.get())?;
            snapshot_store.set(&snapshot_key, frame)?;
            failed_write.replace(None);
            snapshot_writes.set(snapshot_writes.get().saturating_add(1));
            let cursor_value =
                cursor_json(change_seq.get(), snapshot_writes.get(), change_writes.get());
            ready.set(true);
            status.set(status_json(
                AgenticMemoryRecordsPersistenceStatus::Ready,
                0,
                0,
                cursor_value.clone(),
            ));
            error.set(Value::Null);
            cursor.set(cursor_value);
            Ok(())
        }
    };

    if options.snapshot_on_attach {
        write_snapshot()?;
    } else {
        ready.set(true);
        status.set(status_json(
            AgenticMemoryRecordsPersistenceStatus::Ready,
            0,
            0,
            cursor_json(initial_change_seq, 0, 0),
        ));
    }

    let write_snapshot_for_control = Rc::new(write_snapshot);
    let subscribing = Rc::new(Cell::new(true));
    let unsubscribe = {
        let latest = latest.clone();
        let change_log = change_log.clone();
        let change_seq = change_seq.clone();
        let snapshot_writes = snapshot_writes.clone();
        let change_writes = change_writes.clone();
        let ready = ready.clone();
        let status = status.clone();
        let error = error.clone();
        let cursor = cursor.clone();
        let error_count = error_count.clone();
        let failed_write = failed_write.clone();
        let subscribing = subscribing.clone();
        records.subscribe(move |message| {
            let Message::Data(next) = message else {
                return;
            };
            let Some(next) = next
                .as_ref()
                .downcast_ref::<Vec<AgenticMemoryRecord<JsonValue>>>()
            else {
                return;
            };
            latest.replace(next.clone());
            if subscribing.get() {
                subscribing.set(false);
                return;
            }
            let Some(change_log) = &change_log else {
                return;
            };
            match agentic_memory_record_change_frame(next)
                .and_then(|frame| change_log.append(frame).map(|entry| entry.seq))
            {
                Ok(seq) => {
                    failed_write.replace(None);
                    change_seq.set(Some(seq));
                    change_writes.set(change_writes.get().saturating_add(1));
                    let cursor_value =
                        cursor_json(change_seq.get(), snapshot_writes.get(), change_writes.get());
                    ready.set(true);
                    status.set(status_json(
                        AgenticMemoryRecordsPersistenceStatus::Ready,
                        0,
                        error_count.get(),
                        cursor_value.clone(),
                    ));
                    error.set(Value::Null);
                    cursor.set(cursor_value);
                }
                Err(err) => {
                    failed_write.replace(Some(err.clone()));
                    error_count.set(error_count.get().saturating_add(1));
                    ready.set(false);
                    let cursor_value =
                        cursor_json(change_seq.get(), snapshot_writes.get(), change_writes.get());
                    status.set(status_json(
                        AgenticMemoryRecordsPersistenceStatus::Errored,
                        0,
                        error_count.get(),
                        cursor_value.clone(),
                    ));
                    error.set(error_json("change", &err, cursor_value));
                }
            }
        })
    };
    subscribing.set(false);
    let flush = {
        let status = status.clone();
        let cursor = cursor.clone();
        let ready = ready.clone();
        let change_seq = change_seq.clone();
        let snapshot_writes = snapshot_writes.clone();
        let change_writes = change_writes.clone();
        let error_count = error_count.clone();
        let disposed = disposed.clone();
        let error = error.clone();
        let failed_write = failed_write.clone();
        Rc::new(move || {
            if disposed.get() {
                return Ok(());
            }
            let cursor_value =
                cursor_json(change_seq.get(), snapshot_writes.get(), change_writes.get());
            if let Some(err) = failed_write.borrow().clone() {
                ready.set(false);
                status.set(status_json(
                    AgenticMemoryRecordsPersistenceStatus::Errored,
                    0,
                    error_count.get(),
                    cursor_value.clone(),
                ));
                error.set(error_json("flush", &err, cursor_value));
                return Err(err);
            }
            ready.set(true);
            status.set(status_json(
                AgenticMemoryRecordsPersistenceStatus::Ready,
                0,
                error_count.get(),
                cursor_value.clone(),
            ));
            cursor.set(cursor_value);
            Ok(())
        })
    };
    let snapshot = {
        let write_snapshot = write_snapshot_for_control.clone();
        let disposed = disposed.clone();
        Rc::new(move || {
            if disposed.get() {
                return Err(StorageError::backend(
                    "agenticMemoryRecords persistence is disposed",
                ));
            }
            write_snapshot()
        })
    };
    let dispose = {
        let disposed = disposed.clone();
        let ready = ready.clone();
        let status = status.clone();
        let cursor = cursor.clone();
        let change_seq = change_seq.clone();
        let snapshot_writes = snapshot_writes.clone();
        let change_writes = change_writes.clone();
        Box::new(move || {
            disposed.set(true);
            unsubscribe();
            ready.set(false);
            status.set(status_json(
                AgenticMemoryRecordsPersistenceStatus::Disposed,
                0,
                0,
                cursor_json(change_seq.get(), snapshot_writes.get(), change_writes.get()),
            ));
            cursor.set(cursor_json(
                change_seq.get(),
                snapshot_writes.get(),
                change_writes.get(),
            ));
        }) as Disposer
    };

    Ok(AgenticMemoryRecordsPersistence {
        ready,
        status,
        error,
        cursor,
        flush,
        snapshot,
        dispose: RefCell::new(Some(dispose)),
    })
}

pub fn open_persistent_agentic_memory_records(
    options: OpenPersistentAgenticMemoryRecordsOptions,
) -> StorageResult<OpenPersistentAgenticMemoryRecords> {
    let snapshot_key =
        options
            .persistence
            .snapshot_key
            .clone()
            .or(agentic_memory_records_snapshot_key(
                options
                    .persistence
                    .storage_prefix
                    .as_deref()
                    .unwrap_or("agentic-memory"),
            )
            .ok());
    let mut loaded = load_agentic_memory_records_state(
        options.persistence.snapshot_store.as_ref(),
        LoadAgenticMemoryRecordsStateOptions {
            storage_prefix: options.persistence.storage_prefix.as_deref(),
            snapshot_key: snapshot_key.as_deref(),
            change_log: options.persistence.change_log.as_deref(),
        },
    )?;
    if loaded.source == "empty" {
        loaded.records = options.initial;
    }
    let records = options.graph.state_opts(
        loaded.records.clone(),
        GraphNodeOpts::named(
            options
                .name
                .clone()
                .unwrap_or_else(|| "agenticMemoryRecords".to_owned()),
        ),
    );
    let mut persistence_options = options.persistence;
    if persistence_options.graph.is_none() {
        persistence_options.graph = Some(options.graph.clone());
    }
    let persistence =
        persist_agentic_memory_records_with_cursor(&records, persistence_options, loaded.cursor)?;
    Ok(OpenPersistentAgenticMemoryRecords {
        records,
        persistence,
        loaded,
    })
}

struct ParsedSnapshot {
    records: Vec<AgenticMemoryRecord<JsonValue>>,
    change_cursor: Option<u64>,
}

fn parse_snapshot_frame(value: &Value) -> StorageResult<ParsedSnapshot> {
    let object = value
        .as_object()
        .ok_or_else(|| StorageError::backend("agentic memory snapshot frame must be an object"))?;
    assert_keys(
        object.keys().map(String::as_str),
        ["changeCursor", "format", "records", "version"],
    )?;
    if object.get("format")
        != Some(&Value::String(
            AGENTIC_MEMORY_RECORD_SNAPSHOT_FORMAT.to_owned(),
        ))
    {
        return Err(StorageError::backend(
            "agentic memory snapshot frame: invalid format",
        ));
    }
    if object.get("version").and_then(Value::as_u64)
        != Some(AGENTIC_MEMORY_RECORD_STORAGE_FRAME_VERSION as u64)
    {
        return Err(StorageError::backend(
            "agentic memory snapshot frame: invalid version",
        ));
    }
    let change_cursor = cursor_from_json(object.get("changeCursor"))?;
    let records = object
        .get("records")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            StorageError::backend("agentic memory snapshot frame: records must be an array")
        })?
        .iter()
        .map(record_from_frame_json)
        .collect::<StorageResult<Vec<_>>>()?;
    validate_unique_record_set(&records, "agentic memory snapshot frame")?;
    Ok(ParsedSnapshot {
        records,
        change_cursor,
    })
}

fn parse_change_frame(value: &Value) -> StorageResult<Vec<AgenticMemoryRecord<JsonValue>>> {
    let object = value
        .as_object()
        .ok_or_else(|| StorageError::backend("agentic memory change frame must be an object"))?;
    assert_keys(
        object.keys().map(String::as_str),
        ["change", "format", "version"],
    )?;
    if object.get("format")
        != Some(&Value::String(
            AGENTIC_MEMORY_RECORD_CHANGE_FORMAT.to_owned(),
        ))
    {
        return Err(StorageError::backend(
            "agentic memory change frame: invalid format",
        ));
    }
    if object.get("version").and_then(Value::as_u64)
        != Some(AGENTIC_MEMORY_RECORD_STORAGE_FRAME_VERSION as u64)
    {
        return Err(StorageError::backend(
            "agentic memory change frame: invalid version",
        ));
    }
    let change = object
        .get("change")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            StorageError::backend("agentic memory change frame: change must be an object")
        })?;
    assert_keys(change.keys().map(String::as_str), ["kind", "records"])?;
    if change.get("kind") != Some(&Value::String("replaceAll".to_owned())) {
        return Err(StorageError::backend(
            "agentic memory change frame: change.kind must be replaceAll",
        ));
    }
    let records = change
        .get("records")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            StorageError::backend("agentic memory change frame: records must be an array")
        })?
        .iter()
        .map(record_from_frame_json)
        .collect::<StorageResult<Vec<_>>>()?;
    validate_unique_record_set(&records, "agentic memory change frame")?;
    Ok(records)
}

fn validate_unique_record_set(
    records: &[AgenticMemoryRecord<JsonValue>],
    context: &str,
) -> StorageResult<()> {
    let mut record_ids = HashSet::new();
    let mut fragment_ids = HashSet::new();
    for record in records {
        if !record_ids.insert(record.id.as_str()) {
            return Err(StorageError::backend(format!(
                "{context}: duplicate record id '{}'",
                record.id
            )));
        }
        if !fragment_ids.insert(record.fragment.id.as_str()) {
            return Err(StorageError::backend(format!(
                "{context}: duplicate fragment id '{}'",
                record.fragment.id
            )));
        }
    }
    Ok(())
}

fn record_frame_json(record: &AgenticMemoryRecord<JsonValue>) -> StorageResult<Value> {
    let codec = agentic_memory_record_frame_codec();
    let bytes = codec
        .encode(&agentic_memory_record_frame(record.clone()))
        .map_err(storage_json_error)?;
    strict_json_decode(&bytes).map_err(storage_json_error)
}

fn record_from_frame_json(value: &Value) -> StorageResult<AgenticMemoryRecord<JsonValue>> {
    strict_canonical_json_bytes(value)
        .and_then(|bytes| agentic_memory_record_frame_codec().decode(&bytes))
        .map(|frame: AgenticMemoryRecordFrame| frame.record)
        .map_err(storage_json_error)
}

fn cursor_from_json(value: Option<&Value>) -> StorageResult<Option<u64>> {
    let raw = value
        .and_then(Value::as_i64)
        .ok_or_else(|| StorageError::backend("changeCursor must be an integer"))?;
    if raw < -1 {
        return Err(StorageError::backend("changeCursor must be >= -1"));
    }
    Ok(if raw == -1 { None } else { Some(raw as u64) })
}

fn cursor_json(change_seq: Option<u64>, snapshot_writes: usize, change_writes: usize) -> Value {
    json!({
        "kind": "persistence.cursor",
        "changeSeq": change_seq,
        "snapshotWrites": snapshot_writes,
        "changeWrites": change_writes,
    })
}

fn status_json(
    state: AgenticMemoryRecordsPersistenceStatus,
    pending: usize,
    errors: usize,
    cursor: Value,
) -> Value {
    json!({
        "state": match state {
            AgenticMemoryRecordsPersistenceStatus::Starting => "starting",
            AgenticMemoryRecordsPersistenceStatus::Ready => "ready",
            AgenticMemoryRecordsPersistenceStatus::Flushing => "flushing",
            AgenticMemoryRecordsPersistenceStatus::Errored => "errored",
            AgenticMemoryRecordsPersistenceStatus::Disposed => "disposed",
        },
        "pending": pending,
        "writes": cursor.get("snapshotWrites").and_then(Value::as_u64).unwrap_or(0)
            + cursor.get("changeWrites").and_then(Value::as_u64).unwrap_or(0),
        "errors": errors,
        "cursor": cursor,
    })
}

fn error_json(phase: &str, err: &StorageError, cursor: Value) -> Value {
    json!({
        "phase": phase,
        "message": err.to_string(),
        "cursor": cursor,
    })
}

fn parse_cursor_fact(value: &Value) -> StorageResult<AgenticMemoryRecordsPersistenceCursor> {
    Ok(AgenticMemoryRecordsPersistenceCursor {
        change_seq: value.get("changeSeq").and_then(Value::as_u64),
        snapshot_writes: value
            .get("snapshotWrites")
            .and_then(Value::as_u64)
            .unwrap_or(0) as usize,
        change_writes: value
            .get("changeWrites")
            .and_then(Value::as_u64)
            .unwrap_or(0) as usize,
    })
}

fn assert_keys<'a>(
    actual: impl Iterator<Item = &'a str>,
    expected: impl IntoIterator<Item = &'a str>,
) -> StorageResult<()> {
    let mut actual = actual.collect::<Vec<_>>();
    actual.sort();
    let mut expected = expected.into_iter().collect::<Vec<_>>();
    expected.sort();
    if actual != expected {
        return Err(StorageError::backend(format!(
            "unexpected frame fields {}",
            actual.join(",")
        )));
    }
    Ok(())
}

fn storage_json_error(error: impl std::fmt::Display) -> StorageError {
    StorageError::backend(error.to_string())
}
