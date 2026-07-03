use std::cell::{Cell, RefCell};
use std::fs;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use graphrefly::adapters::observe_storage::{
    attach_observe_event_log, AttachObserveEventLogOptions,
};
use graphrefly::{
    append_log_key, append_log_storage, assert_decimal_integer_string,
    assert_non_negative_decimal_integer_string, assert_wal_frame, change_envelope_codec,
    content_addressed_kv, content_addressed_storage, decimal_string_to_i128,
    default_restore_registry, dict_kv, envelope_change, file_append_log, file_backend, file_kv,
    graph, i128_to_decimal_string, is_decimal_integer_string,
    is_non_negative_decimal_integer_string, json_codec_for, load_reactive_index_state,
    load_reactive_list_state, load_reactive_log_state, load_reactive_map_state, memory_append_log,
    memory_kv, memory_multi_writer_append_log, multi_writer_append_log_storage,
    non_negative_decimal_string_to_u128, observe_event_frame, observe_event_frame_codec,
    open_persistent_reactive_index, open_persistent_reactive_list, open_persistent_reactive_log,
    open_persistent_reactive_map, persist_reactive_list, reactive_cascading_cache,
    reactive_collection_change_frame, reactive_collection_snapshot_frame,
    reactive_collection_snapshot_key, reactive_list, reactive_log, reactive_map,
    read_append_log_page, read_observe_event_log_page, restore_graph, restore_reactive_index,
    restore_reactive_list, restore_reactive_log, restore_reactive_map, stable_json_string,
    strict_canonical_json_bytes, strict_json_codec_for, strict_json_decode, tiered_read_through,
    u128_to_non_negative_decimal_string, verify_wal_frame_checksum, wal_frame, wal_frame_checksum,
    wal_frame_codec, wal_frame_key, wal_frame_prefix, AppendLogEntry, AppendLogReadOptions,
    AppendLogStorageTier, ByteStorageBackend, CascadingCacheEvent, CascadingCachePolicy,
    CascadingCacheStatus, ChangeEnvelopeOptions, ChangeLifecycle, Codec, ContentAddressedKvOptions,
    ContentAddressedMode, FileAppendLogOptions, FileBackendOptions, GraphCheckpointFactory,
    GraphCheckpointValue, JsonCodec, KvStorageTier, KvVersionedRead, Message, Node,
    ObserveEventFrame, ObserveEventFrameOptions, ObserveMessage,
    OpenPersistentReactiveIndexOptions, OpenPersistentReactiveListOptions,
    OpenPersistentReactiveLogOptions, OpenPersistentReactiveMapOptions,
    PersistReactiveCollectionOptions, PromotionPolicy, PullDemand, ReactiveCascadingCacheOptions,
    ReactiveCollectionChangeFrame, ReactiveCollectionKind, ReactiveCollectionPersistenceStatus,
    ReactiveCollectionRestoreSource, ReactiveCollectionSnapshotFrame, ReactiveIndexOptions,
    ReactiveListOptions, ReactiveLogOptions, ReactiveMapOptions, ReadThroughErrorStage,
    ReadThroughOutcome, RestoreGraphOptions, StorageError, StorageResult, TieredReadThroughOptions,
    TieredReadThroughStatus, WalFrame, WalFrameBody, WalFrameOptions, WAL_FORMAT_VERSION,
};
use serde_json::json;

#[derive(Clone)]
struct PlainTier {
    label: &'static str,
    read: StorageResult<Option<i32>>,
    calls: Rc<RefCell<Vec<String>>>,
}

impl PlainTier {
    fn new(
        label: &'static str,
        read: StorageResult<Option<i32>>,
        calls: Rc<RefCell<Vec<String>>>,
    ) -> Self {
        Self { label, read, calls }
    }
}

impl KvStorageTier<i32> for PlainTier {
    fn get(&self, key: &str) -> StorageResult<Option<i32>> {
        self.calls
            .borrow_mut()
            .push(format!("{}:{key}", self.label));
        self.read.clone()
    }

    fn set(&self, key: &str, value: i32) -> StorageResult<()> {
        self.calls
            .borrow_mut()
            .push(format!("{}:set:{key}:{value}", self.label));
        Ok(())
    }

    fn delete(&self, key: &str) -> StorageResult<()> {
        self.calls
            .borrow_mut()
            .push(format!("{}:delete:{key}", self.label));
        Ok(())
    }

    fn list(&self, _prefix: &str) -> StorageResult<Vec<String>> {
        Ok(Vec::new())
    }
}

type Collected<T> = (Rc<RefCell<Vec<T>>>, Box<dyn FnOnce()>);
type KindLog = (Rc<RefCell<Vec<&'static str>>>, Box<dyn FnOnce()>);

fn data_of<T: Clone + 'static>(node: &Node<T>) -> Collected<T> {
    let out = Rc::new(RefCell::new(Vec::new()));
    let sink = out.clone();
    let unsub = node.subscribe(move |msg| {
        if let Message::Data(value) = msg {
            if let Some(value) = value.as_ref().downcast_ref::<T>() {
                sink.borrow_mut().push(value.clone());
            }
        }
    });
    (out, unsub)
}

fn message_kinds<T: 'static>(node: &Node<T>) -> KindLog {
    let out = Rc::new(RefCell::new(Vec::new()));
    let sink = out.clone();
    let unsub = node.subscribe(move |msg| {
        let kind = match msg {
            Message::Start => "START",
            Message::Pause(_) => "PAUSE",
            Message::Resume(_) => "RESUME",
            Message::Pull(_) => "PULL",
            Message::Dirty => "DIRTY",
            Message::Data(_) => "DATA",
            Message::Resolved => "RESOLVED",
            Message::Invalidate => "INVALIDATE",
            Message::Complete => "COMPLETE",
            Message::Error(_) => "ERROR",
            Message::Teardown => "TEARDOWN",
        };
        sink.borrow_mut().push(kind);
    });
    (out, unsub)
}

fn event_kind<V>(event: &CascadingCacheEvent<V>) -> &'static str {
    match event {
        CascadingCacheEvent::Request { .. } => "request",
        CascadingCacheEvent::Invalidate { .. } => "invalidate",
        CascadingCacheEvent::Lookup { .. } => "lookup",
        CascadingCacheEvent::Promotion { .. } => "promotion",
        CascadingCacheEvent::Fill { .. } => "fill",
        CascadingCacheEvent::Error { .. } => "error",
    }
}

static NEXT_TMP_DIR: AtomicU64 = AtomicU64::new(1);

fn temp_storage_dir(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let path = std::env::temp_dir().join(format!(
        "graphrefly-rs-{label}-{}-{nanos}-{}",
        std::process::id(),
        NEXT_TMP_DIR.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&path);
    path
}

#[test]
fn stable_and_strict_json_helpers_use_canonical_bytes() {
    let value = json!({ "b": 2, "a": { "d": 4, "c": 3 } });

    assert_eq!(
        stable_json_string(&value).unwrap(),
        r#"{"a":{"c":3,"d":4},"b":2}"#
    );
    assert_eq!(
        String::from_utf8(strict_canonical_json_bytes(&value).unwrap()).unwrap(),
        r#"{"a":{"c":3,"d":4},"b":2}"#
    );
    assert_eq!(
        String::from_utf8(strict_canonical_json_bytes(&json!(1.0)).unwrap()).unwrap(),
        "1",
        "D112: integral floats use the same canonical number bytes as TS/Py JSON"
    );
    assert_eq!(
        strict_json_decode(br#"{"a":{"c":3,"d":4},"b":2}"#).unwrap(),
        json!({ "a": { "c": 3, "d": 4 }, "b": 2 })
    );
    assert!(strict_json_decode(b"1.0")
        .unwrap_err()
        .to_string()
        .contains("canonical"));
    assert!(strict_json_decode(b"-0")
        .unwrap_err()
        .to_string()
        .contains("canonical"));
    assert!(
        strict_canonical_json_bytes(&json!(9_007_199_254_740_992_u64))
            .unwrap_err()
            .to_string()
            .contains("safe integer")
    );
    assert!(
        strict_canonical_json_bytes(&json!(-9_007_199_254_740_992_i64))
            .unwrap_err()
            .to_string()
            .contains("safe integer")
    );
    assert!(strict_json_decode(b"9007199254740992")
        .unwrap_err()
        .to_string()
        .contains("safe integer"));
    assert!(strict_json_decode(b"1e20")
        .unwrap_err()
        .to_string()
        .contains("safe integer"));

    assert!(strict_json_decode(br#"{"b":2,"a":1}"#)
        .unwrap_err()
        .to_string()
        .contains("canonical"));
    assert!(strict_json_decode(br#"{ "a": 1 }"#)
        .unwrap_err()
        .to_string()
        .contains("canonical"));
}

#[test]
fn strict_json_decode_rejects_duplicate_keys_and_malformed_utf8() {
    for raw in [
        br#"{"a":1,"a":2}"#.as_slice(),
        br#"{"a":1,"\u0061":2}"#,
        br#"{"a":{"b":1,"b":2}}"#,
        br#"{"a":[{"b":1,"b":2}]}"#,
    ] {
        assert!(strict_json_decode(raw)
            .unwrap_err()
            .to_string()
            .contains("duplicate object key"));
    }

    assert!(strict_json_decode(&[0xff]).is_err());
    assert!(strict_json_decode(br#""\ud800""#).is_err());
}

#[test]
fn reactive_collection_load_folds_list_snapshot_and_changes() {
    let snapshots = memory_kv::<ReactiveCollectionSnapshotFrame>();
    let changes = memory_append_log::<ReactiveCollectionChangeFrame>("reactive-list/changes");
    let snapshot_key = reactive_collection_snapshot_key("reactive-list").unwrap();
    snapshots
        .set(
            &snapshot_key,
            reactive_collection_snapshot_frame(
                ReactiveCollectionKind::ReactiveList,
                -1,
                json!([1, 2]),
            )
            .unwrap(),
        )
        .unwrap();
    changes
        .append(
            reactive_collection_change_frame(
                ReactiveCollectionKind::ReactiveList,
                json!({ "kind": "append", "value": 3 }),
            )
            .unwrap(),
        )
        .unwrap();
    changes
        .append(
            reactive_collection_change_frame(
                ReactiveCollectionKind::ReactiveList,
                json!({ "kind": "pop", "index": 1, "value": 2 }),
            )
            .unwrap(),
        )
        .unwrap();

    let restored = load_reactive_list_state::<i32>(
        &snapshots,
        graphrefly::LoadReactiveCollectionStateOptions {
            storage_prefix: Some("reactive-list"),
            snapshot_key: None,
            change_log: Some(&changes),
        },
    )
    .unwrap();

    assert_eq!(restored.state, vec![1, 3]);
    assert_eq!(restored.cursor, Some(1));
    assert!(restored.snapshot_found);
    assert_eq!(restored.changes_applied, 2);
}

#[test]
fn reactive_collection_load_accepts_empty_and_changes_only_from_seq_zero() {
    let snapshots = memory_kv::<ReactiveCollectionSnapshotFrame>();
    let empty = load_reactive_list_state::<i32>(
        &snapshots,
        graphrefly::LoadReactiveCollectionStateOptions {
            storage_prefix: Some("empty-list"),
            snapshot_key: None,
            change_log: None,
        },
    )
    .unwrap();
    assert_eq!(empty.source, ReactiveCollectionRestoreSource::Empty);
    assert_eq!(empty.state, Vec::<i32>::new());

    let empty_changes = memory_append_log::<ReactiveCollectionChangeFrame>("empty-list/changes");
    let empty_with_log = load_reactive_list_state::<i32>(
        &snapshots,
        graphrefly::LoadReactiveCollectionStateOptions {
            storage_prefix: Some("empty-list"),
            snapshot_key: None,
            change_log: Some(&empty_changes),
        },
    )
    .unwrap();
    assert_eq!(
        empty_with_log.source,
        ReactiveCollectionRestoreSource::Empty
    );
    assert_eq!(empty_with_log.state, Vec::<i32>::new());

    let changes = memory_append_log::<ReactiveCollectionChangeFrame>("orphan-list/changes");
    changes
        .append(
            reactive_collection_change_frame(
                ReactiveCollectionKind::ReactiveList,
                json!({ "kind": "append", "value": 1 }),
            )
            .unwrap(),
        )
        .unwrap();

    let restored = load_reactive_list_state::<i32>(
        &snapshots,
        graphrefly::LoadReactiveCollectionStateOptions {
            storage_prefix: Some("orphan-list"),
            snapshot_key: None,
            change_log: Some(&changes),
        },
    )
    .unwrap();

    assert_eq!(restored.source, ReactiveCollectionRestoreSource::Changes);
    assert_eq!(restored.state, vec![1]);
    assert_eq!(restored.cursor, Some(0));
}

#[derive(Clone)]
struct GapChangeLog {
    entries: Vec<AppendLogEntry<ReactiveCollectionChangeFrame>>,
}

impl AppendLogStorageTier<ReactiveCollectionChangeFrame> for GapChangeLog {
    fn append(
        &self,
        _value: ReactiveCollectionChangeFrame,
    ) -> StorageResult<AppendLogEntry<ReactiveCollectionChangeFrame>> {
        Err(StorageError::backend("gap log is read-only"))
    }

    fn read(
        &self,
        _opts: AppendLogReadOptions,
    ) -> StorageResult<Vec<AppendLogEntry<ReactiveCollectionChangeFrame>>> {
        Ok(self.entries.clone())
    }

    fn truncate_after(&self, _seq: u64) -> StorageResult<()> {
        Ok(())
    }

    fn size(&self) -> StorageResult<usize> {
        Ok(self.entries.len())
    }
}

#[derive(Clone)]
struct FailOnceChangeLog {
    inner: graphrefly::AppendLogStorage<ReactiveCollectionChangeFrame>,
    fail_next: Rc<Cell<bool>>,
}

impl FailOnceChangeLog {
    fn new(prefix: &str) -> Self {
        Self {
            inner: memory_append_log(prefix),
            fail_next: Rc::new(Cell::new(true)),
        }
    }
}

impl AppendLogStorageTier<ReactiveCollectionChangeFrame> for FailOnceChangeLog {
    fn append(
        &self,
        value: ReactiveCollectionChangeFrame,
    ) -> StorageResult<AppendLogEntry<ReactiveCollectionChangeFrame>> {
        if self.fail_next.replace(false) {
            return Err(StorageError::backend("injected append failure"));
        }
        self.inner.append(value)
    }

    fn read(
        &self,
        opts: AppendLogReadOptions,
    ) -> StorageResult<Vec<AppendLogEntry<ReactiveCollectionChangeFrame>>> {
        self.inner.read(opts)
    }

    fn truncate_after(&self, seq: u64) -> StorageResult<()> {
        self.inner.truncate_after(seq)
    }

    fn size(&self) -> StorageResult<usize> {
        self.inner.size()
    }
}

#[derive(Clone)]
struct FailOnceSnapshotStore {
    entries: Rc<RefCell<std::collections::BTreeMap<String, ReactiveCollectionSnapshotFrame>>>,
    fail_next: Rc<Cell<bool>>,
}

impl FailOnceSnapshotStore {
    fn new() -> Self {
        Self {
            entries: Rc::new(RefCell::new(std::collections::BTreeMap::new())),
            fail_next: Rc::new(Cell::new(true)),
        }
    }
}

impl KvStorageTier<ReactiveCollectionSnapshotFrame> for FailOnceSnapshotStore {
    fn get(&self, key: &str) -> StorageResult<Option<ReactiveCollectionSnapshotFrame>> {
        Ok(self.entries.borrow().get(key).cloned())
    }

    fn set(&self, key: &str, value: ReactiveCollectionSnapshotFrame) -> StorageResult<()> {
        if self.fail_next.replace(false) {
            return Err(StorageError::backend("injected snapshot failure"));
        }
        self.entries.borrow_mut().insert(key.to_owned(), value);
        Ok(())
    }

    fn delete(&self, key: &str) -> StorageResult<()> {
        self.entries.borrow_mut().remove(key);
        Ok(())
    }

    fn list(&self, prefix: &str) -> StorageResult<Vec<String>> {
        Ok(self
            .entries
            .borrow()
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect())
    }
}

#[test]
fn reactive_collection_load_rejects_seq_holes_and_duplicate_keys() {
    let snapshots = memory_kv::<ReactiveCollectionSnapshotFrame>();
    let gap = GapChangeLog {
        entries: vec![AppendLogEntry {
            key: "gap/0000000000000001".to_owned(),
            seq: 1,
            value: reactive_collection_change_frame(
                ReactiveCollectionKind::ReactiveList,
                json!({ "kind": "append", "value": 1 }),
            )
            .unwrap(),
        }],
    };

    let err = load_reactive_list_state::<i32>(
        &snapshots,
        graphrefly::LoadReactiveCollectionStateOptions {
            storage_prefix: Some("gap-list"),
            snapshot_key: None,
            change_log: Some(&gap),
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("non-contiguous"));

    let map_snapshots = memory_kv::<ReactiveCollectionSnapshotFrame>();
    map_snapshots
        .set(
            "map/snapshot",
            reactive_collection_snapshot_frame(
                ReactiveCollectionKind::ReactiveMap,
                -1,
                json!([["a", 1], ["a", 2]]),
            )
            .unwrap(),
        )
        .unwrap();
    let err = load_reactive_map_state::<String, i32>(
        &map_snapshots,
        graphrefly::LoadReactiveCollectionStateOptions {
            storage_prefix: None,
            snapshot_key: Some("map/snapshot"),
            change_log: None,
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("duplicates"));

    let index_snapshots = memory_kv::<ReactiveCollectionSnapshotFrame>();
    index_snapshots
        .set(
            "index/snapshot",
            reactive_collection_snapshot_frame(
                ReactiveCollectionKind::ReactiveIndex,
                -1,
                json!([
                    { "primary": "p", "secondary": 1, "value": 10 },
                    { "primary": "p", "secondary": 2, "value": 20 }
                ]),
            )
            .unwrap(),
        )
        .unwrap();
    let err = load_reactive_index_state::<String, i32, i32>(
        &index_snapshots,
        graphrefly::LoadReactiveCollectionStateOptions {
            storage_prefix: None,
            snapshot_key: Some("index/snapshot"),
            change_log: None,
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("duplicates"));
}

#[test]
fn reactive_collection_load_rejects_corrupt_frames_and_impossible_folds() {
    let wrong_kind_snapshots = memory_kv::<ReactiveCollectionSnapshotFrame>();
    wrong_kind_snapshots
        .set(
            "wrong/snapshot",
            reactive_collection_snapshot_frame(ReactiveCollectionKind::ReactiveLog, -1, json!([]))
                .unwrap(),
        )
        .unwrap();
    let err = load_reactive_list_state::<i32>(
        &wrong_kind_snapshots,
        graphrefly::LoadReactiveCollectionStateOptions {
            storage_prefix: None,
            snapshot_key: Some("wrong/snapshot"),
            change_log: None,
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("expected ReactiveList"));

    let bad_version = GapChangeLog {
        entries: vec![AppendLogEntry {
            key: "bad-version/0000000000000000".to_owned(),
            seq: 0,
            value: ReactiveCollectionChangeFrame {
                format: graphrefly::REACTIVE_COLLECTION_CHANGE_FORMAT.to_owned(),
                version: 2,
                kind: ReactiveCollectionKind::ReactiveList,
                change: json!({ "kind": "append", "value": 1 }),
            },
        }],
    };
    let err = load_reactive_list_state::<i32>(
        &memory_kv::<ReactiveCollectionSnapshotFrame>(),
        graphrefly::LoadReactiveCollectionStateOptions {
            storage_prefix: Some("bad-version"),
            snapshot_key: None,
            change_log: Some(&bad_version),
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("unsupported version"));

    let bad_shape = GapChangeLog {
        entries: vec![AppendLogEntry {
            key: "bad-shape/0000000000000000".to_owned(),
            seq: 0,
            value: ReactiveCollectionChangeFrame {
                format: graphrefly::REACTIVE_COLLECTION_CHANGE_FORMAT.to_owned(),
                version: 1,
                kind: ReactiveCollectionKind::ReactiveList,
                change: json!({ "value": 1 }),
            },
        }],
    };
    let err = load_reactive_list_state::<i32>(
        &memory_kv::<ReactiveCollectionSnapshotFrame>(),
        graphrefly::LoadReactiveCollectionStateOptions {
            storage_prefix: Some("bad-shape"),
            snapshot_key: None,
            change_log: Some(&bad_shape),
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("missing field"));

    let list_snapshots = memory_kv::<ReactiveCollectionSnapshotFrame>();
    list_snapshots
        .set(
            "list/impossible",
            reactive_collection_snapshot_frame(
                ReactiveCollectionKind::ReactiveList,
                -1,
                json!([1, 2]),
            )
            .unwrap(),
        )
        .unwrap();
    let list_changes = memory_append_log::<ReactiveCollectionChangeFrame>("list/impossible/log");
    list_changes
        .append(
            reactive_collection_change_frame(
                ReactiveCollectionKind::ReactiveList,
                json!({ "kind": "pop", "index": 1, "value": 99 }),
            )
            .unwrap(),
        )
        .unwrap();
    let err = load_reactive_list_state::<i32>(
        &list_snapshots,
        graphrefly::LoadReactiveCollectionStateOptions {
            storage_prefix: None,
            snapshot_key: Some("list/impossible"),
            change_log: Some(&list_changes),
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("pop value"));

    let map_snapshots = memory_kv::<ReactiveCollectionSnapshotFrame>();
    map_snapshots
        .set(
            "map/impossible",
            reactive_collection_snapshot_frame(
                ReactiveCollectionKind::ReactiveMap,
                -1,
                json!([["a", 1]]),
            )
            .unwrap(),
        )
        .unwrap();
    let map_changes = memory_append_log::<ReactiveCollectionChangeFrame>("map/impossible/log");
    map_changes
        .append(
            reactive_collection_change_frame(
                ReactiveCollectionKind::ReactiveMap,
                json!({ "kind": "delete", "key": "a", "previous": 2 }),
            )
            .unwrap(),
        )
        .unwrap();
    let err = load_reactive_map_state::<String, i32>(
        &map_snapshots,
        graphrefly::LoadReactiveCollectionStateOptions {
            storage_prefix: None,
            snapshot_key: Some("map/impossible"),
            change_log: Some(&map_changes),
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("previous value"));

    let index_changes = memory_append_log::<ReactiveCollectionChangeFrame>("index/impossible/log");
    index_changes
        .append(
            reactive_collection_change_frame(
                ReactiveCollectionKind::ReactiveIndex,
                json!({ "kind": "delete", "primary": "missing" }),
            )
            .unwrap(),
        )
        .unwrap();
    let err = load_reactive_index_state::<String, i32, i32>(
        &memory_kv::<ReactiveCollectionSnapshotFrame>(),
        graphrefly::LoadReactiveCollectionStateOptions {
            storage_prefix: Some("index/impossible"),
            snapshot_key: None,
            change_log: Some(&index_changes),
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("delete primary"));

    let index_many_snapshots = memory_kv::<ReactiveCollectionSnapshotFrame>();
    index_many_snapshots
        .set(
            "index/delete-many",
            reactive_collection_snapshot_frame(
                ReactiveCollectionKind::ReactiveIndex,
                -1,
                json!([
                    { "primary": "a", "secondary": 1, "value": 10 },
                    { "primary": "b", "secondary": 2, "value": 20 }
                ]),
            )
            .unwrap(),
        )
        .unwrap();
    let index_many_changes =
        memory_append_log::<ReactiveCollectionChangeFrame>("index/delete-many/log");
    index_many_changes
        .append(
            reactive_collection_change_frame(
                ReactiveCollectionKind::ReactiveIndex,
                json!({ "kind": "deleteMany", "primaries": ["a", "a"] }),
            )
            .unwrap(),
        )
        .unwrap();
    let err = load_reactive_index_state::<String, i32, i32>(
        &index_many_snapshots,
        graphrefly::LoadReactiveCollectionStateOptions {
            storage_prefix: None,
            snapshot_key: Some("index/delete-many"),
            change_log: Some(&index_many_changes),
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("duplicates"));

    let log_snapshots = memory_kv::<ReactiveCollectionSnapshotFrame>();
    log_snapshots
        .set(
            "log/impossible",
            reactive_collection_snapshot_frame(ReactiveCollectionKind::ReactiveLog, -1, json!([1]))
                .unwrap(),
        )
        .unwrap();
    let log_changes = memory_append_log::<ReactiveCollectionChangeFrame>("log/impossible/log");
    log_changes
        .append(
            reactive_collection_change_frame(
                ReactiveCollectionKind::ReactiveLog,
                json!({ "kind": "clear", "count": 2 }),
            )
            .unwrap(),
        )
        .unwrap();
    let err = load_reactive_log_state::<i32>(
        &log_snapshots,
        graphrefly::LoadReactiveCollectionStateOptions {
            storage_prefix: None,
            snapshot_key: Some("log/impossible"),
            change_log: Some(&log_changes),
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("clear count"));
}

#[test]
fn restore_reactive_helpers_seed_backends_without_storage_reads() {
    let list_state = graphrefly::ReactiveCollectionRestoreState {
        kind: ReactiveCollectionKind::ReactiveList,
        state: vec![1, 2],
        source: ReactiveCollectionRestoreSource::Snapshot,
        snapshot: graphrefly::ReactiveCollectionSnapshotRestoreMeta {
            found: true,
            change_cursor: -1,
        },
        changes: graphrefly::ReactiveCollectionChangesRestoreMeta {
            applied: 0,
            cursor: -1,
        },
        cursor: None,
        snapshot_found: true,
        changes_applied: 0,
    };
    let list = restore_reactive_list(list_state, ReactiveListOptions::default()).unwrap();
    assert_eq!(list.to_vec(), vec![1, 2]);

    let log_state = graphrefly::ReactiveCollectionRestoreState {
        kind: ReactiveCollectionKind::ReactiveLog,
        state: vec!["a".to_owned(), "b".to_owned()],
        source: ReactiveCollectionRestoreSource::Snapshot,
        snapshot: graphrefly::ReactiveCollectionSnapshotRestoreMeta {
            found: true,
            change_cursor: -1,
        },
        changes: graphrefly::ReactiveCollectionChangesRestoreMeta {
            applied: 0,
            cursor: -1,
        },
        cursor: None,
        snapshot_found: true,
        changes_applied: 0,
    };
    let log = restore_reactive_log(log_state, ReactiveLogOptions::default()).unwrap();
    assert_eq!(log.to_vec(), vec!["a".to_owned(), "b".to_owned()]);

    let map_state = graphrefly::ReactiveCollectionRestoreState {
        kind: ReactiveCollectionKind::ReactiveMap,
        state: vec![("k".to_owned(), 7)],
        source: ReactiveCollectionRestoreSource::Snapshot,
        snapshot: graphrefly::ReactiveCollectionSnapshotRestoreMeta {
            found: true,
            change_cursor: -1,
        },
        changes: graphrefly::ReactiveCollectionChangesRestoreMeta {
            applied: 0,
            cursor: -1,
        },
        cursor: None,
        snapshot_found: true,
        changes_applied: 0,
    };
    let map = restore_reactive_map(map_state, ReactiveMapOptions::default()).unwrap();
    assert_eq!(map.get(&"k".to_owned()), Some(7));
    let duplicate_map = graphrefly::ReactiveCollectionRestoreState {
        kind: ReactiveCollectionKind::ReactiveMap,
        state: vec![("dup".to_owned(), 1), ("dup".to_owned(), 2)],
        source: ReactiveCollectionRestoreSource::Snapshot,
        snapshot: graphrefly::ReactiveCollectionSnapshotRestoreMeta {
            found: true,
            change_cursor: -1,
        },
        changes: graphrefly::ReactiveCollectionChangesRestoreMeta {
            applied: 0,
            cursor: -1,
        },
        cursor: None,
        snapshot_found: true,
        changes_applied: 0,
    };
    let err = match restore_reactive_map(duplicate_map, ReactiveMapOptions::default()) {
        Ok(_) => panic!("duplicate map keys must fail restore"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("duplicates"));

    let index_state = graphrefly::ReactiveCollectionRestoreState {
        kind: ReactiveCollectionKind::ReactiveIndex,
        state: vec![graphrefly::IndexRow {
            primary: "p".to_owned(),
            secondary: 1,
            value: 9,
        }],
        source: ReactiveCollectionRestoreSource::Snapshot,
        snapshot: graphrefly::ReactiveCollectionSnapshotRestoreMeta {
            found: true,
            change_cursor: -1,
        },
        changes: graphrefly::ReactiveCollectionChangesRestoreMeta {
            applied: 0,
            cursor: -1,
        },
        cursor: None,
        snapshot_found: true,
        changes_applied: 0,
    };
    let index = restore_reactive_index(index_state, ReactiveIndexOptions::default()).unwrap();
    assert_eq!(index.get(&"p".to_owned()), Some(9));
    let duplicate_index = graphrefly::ReactiveCollectionRestoreState {
        kind: ReactiveCollectionKind::ReactiveIndex,
        state: vec![
            graphrefly::IndexRow {
                primary: "p".to_owned(),
                secondary: 1,
                value: 9,
            },
            graphrefly::IndexRow {
                primary: "p".to_owned(),
                secondary: 2,
                value: 10,
            },
        ],
        source: ReactiveCollectionRestoreSource::Snapshot,
        snapshot: graphrefly::ReactiveCollectionSnapshotRestoreMeta {
            found: true,
            change_cursor: -1,
        },
        changes: graphrefly::ReactiveCollectionChangesRestoreMeta {
            applied: 0,
            cursor: -1,
        },
        cursor: None,
        snapshot_found: true,
        changes_applied: 0,
    };
    let err = match restore_reactive_index(duplicate_index, ReactiveIndexOptions::default()) {
        Ok(_) => panic!("duplicate index primaries must fail restore"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("duplicates"));
}

#[test]
fn open_persistent_reactive_list_composes_load_restore_and_persist() {
    let snapshots: Rc<dyn KvStorageTier<ReactiveCollectionSnapshotFrame>> =
        Rc::new(memory_kv::<ReactiveCollectionSnapshotFrame>());
    let changes: Rc<dyn AppendLogStorageTier<ReactiveCollectionChangeFrame>> = Rc::new(
        memory_append_log::<ReactiveCollectionChangeFrame>("persistent-list/changes"),
    );

    let opened_graph = graph();
    let opened = open_persistent_reactive_list(OpenPersistentReactiveListOptions {
        initial: vec![10],
        collection: ReactiveListOptions::default().graph(opened_graph.clone()),
        persistence: PersistReactiveCollectionOptions {
            graph: Some(opened_graph),
            name: None,
            storage_prefix: Some("persistent-list".to_owned()),
            snapshot_key: None,
            snapshot_store: snapshots.clone(),
            change_log: Some(changes.clone()),
            snapshot_on_attach: true,
            snapshot_every_changes: None,
        },
    })
    .unwrap();

    assert_eq!(opened.collection.to_vec(), vec![10]);
    opened.collection.append(11);
    assert_eq!(
        changes.size().unwrap(),
        0,
        "D161: sidecar queues storage writes until flush"
    );
    assert_eq!(opened.persistence.ready.cache(), Some(false));
    opened.persistence.flush().unwrap();
    assert_eq!(changes.size().unwrap(), 1);
    let cursor = opened.persistence.cursor_fact().unwrap();
    assert_eq!(cursor.change_seq, Some(0));
    assert_eq!(cursor.snapshot_writes, 1);
    assert_eq!(cursor.change_writes, 1);
    assert_eq!(opened.persistence.ready.cache(), Some(true));

    let reopened_graph = graph();
    let reopened = open_persistent_reactive_list(OpenPersistentReactiveListOptions {
        initial: vec![99],
        collection: ReactiveListOptions::default().graph(reopened_graph.clone()),
        persistence: PersistReactiveCollectionOptions {
            graph: Some(reopened_graph),
            name: None,
            storage_prefix: Some("persistent-list".to_owned()),
            snapshot_key: None,
            snapshot_store: snapshots.clone(),
            change_log: Some(changes.clone()),
            snapshot_on_attach: true,
            snapshot_every_changes: None,
        },
    })
    .unwrap();

    assert_eq!(
        reopened.collection.to_vec(),
        vec![10, 11],
        "D161: missing durable state uses initial, but existing snapshot+log wins"
    );
    let reopened_cursor = reopened.persistence.cursor_fact().unwrap();
    assert_eq!(reopened_cursor.change_seq, Some(0));
    assert_eq!(reopened_cursor.snapshot_writes, 1);
    assert_eq!(reopened_cursor.change_writes, 0);

    let reopened_again_graph = graph();
    let reopened_again = open_persistent_reactive_list(OpenPersistentReactiveListOptions {
        initial: vec![99],
        collection: ReactiveListOptions::default().graph(reopened_again_graph.clone()),
        persistence: PersistReactiveCollectionOptions {
            graph: Some(reopened_again_graph),
            name: None,
            storage_prefix: Some("persistent-list".to_owned()),
            snapshot_key: None,
            snapshot_store: snapshots,
            change_log: Some(changes),
            snapshot_on_attach: false,
            snapshot_every_changes: None,
        },
    })
    .unwrap();

    assert_eq!(
        reopened_again.collection.to_vec(),
        vec![10, 11],
        "restored cursor must prevent double replay after snapshot_on_attach"
    );
}

#[test]
fn open_persistent_reactive_log_composes_load_restore_and_persist() {
    let snapshots: Rc<dyn KvStorageTier<ReactiveCollectionSnapshotFrame>> =
        Rc::new(memory_kv::<ReactiveCollectionSnapshotFrame>());
    let changes: Rc<dyn AppendLogStorageTier<ReactiveCollectionChangeFrame>> = Rc::new(
        memory_append_log::<ReactiveCollectionChangeFrame>("persistent-log/changes"),
    );

    let opened_graph = graph();
    let opened = open_persistent_reactive_log(OpenPersistentReactiveLogOptions {
        initial: vec!["a".to_owned()],
        collection: ReactiveLogOptions::default().graph(opened_graph.clone()),
        persistence: PersistReactiveCollectionOptions {
            graph: Some(opened_graph),
            name: None,
            storage_prefix: Some("persistent-log".to_owned()),
            snapshot_key: None,
            snapshot_store: snapshots.clone(),
            change_log: Some(changes.clone()),
            snapshot_on_attach: true,
            snapshot_every_changes: None,
        },
    })
    .unwrap();
    opened.collection.append("b".to_owned());
    opened.persistence.flush().unwrap();

    let reopened_graph = graph();
    let reopened = open_persistent_reactive_log(OpenPersistentReactiveLogOptions {
        initial: vec!["ignored".to_owned()],
        collection: ReactiveLogOptions::default().graph(reopened_graph.clone()),
        persistence: PersistReactiveCollectionOptions {
            graph: Some(reopened_graph),
            name: None,
            storage_prefix: Some("persistent-log".to_owned()),
            snapshot_key: None,
            snapshot_store: snapshots,
            change_log: Some(changes),
            snapshot_on_attach: false,
            snapshot_every_changes: None,
        },
    })
    .unwrap();
    assert_eq!(
        reopened.collection.to_vec(),
        vec!["a".to_owned(), "b".to_owned()]
    );
}

#[test]
fn open_persistent_map_and_index_compose_load_restore_and_persist() {
    let map_snapshots: Rc<dyn KvStorageTier<ReactiveCollectionSnapshotFrame>> =
        Rc::new(memory_kv::<ReactiveCollectionSnapshotFrame>());
    let map_changes: Rc<dyn AppendLogStorageTier<ReactiveCollectionChangeFrame>> = Rc::new(
        memory_append_log::<ReactiveCollectionChangeFrame>("persistent-map/changes"),
    );
    let opened_map_graph = graph();
    let opened_map = open_persistent_reactive_map(OpenPersistentReactiveMapOptions {
        initial: vec![("a".to_owned(), 1)],
        collection: ReactiveMapOptions::default().graph(opened_map_graph.clone()),
        persistence: PersistReactiveCollectionOptions {
            graph: Some(opened_map_graph),
            name: None,
            storage_prefix: Some("persistent-map".to_owned()),
            snapshot_key: None,
            snapshot_store: map_snapshots.clone(),
            change_log: Some(map_changes.clone()),
            snapshot_on_attach: true,
            snapshot_every_changes: None,
        },
    })
    .unwrap();
    opened_map.collection.set("b".to_owned(), 2);
    opened_map.persistence.flush().unwrap();

    let reopened_map_graph = graph();
    let reopened_map = open_persistent_reactive_map(OpenPersistentReactiveMapOptions {
        initial: vec![("ignored".to_owned(), 9)],
        collection: ReactiveMapOptions::default().graph(reopened_map_graph.clone()),
        persistence: PersistReactiveCollectionOptions {
            graph: Some(reopened_map_graph),
            name: None,
            storage_prefix: Some("persistent-map".to_owned()),
            snapshot_key: None,
            snapshot_store: map_snapshots,
            change_log: Some(map_changes),
            snapshot_on_attach: false,
            snapshot_every_changes: None,
        },
    })
    .unwrap();
    assert_eq!(reopened_map.collection.get(&"a".to_owned()), Some(1));
    assert_eq!(reopened_map.collection.get(&"b".to_owned()), Some(2));

    let index_snapshots: Rc<dyn KvStorageTier<ReactiveCollectionSnapshotFrame>> =
        Rc::new(memory_kv::<ReactiveCollectionSnapshotFrame>());
    let index_changes: Rc<dyn AppendLogStorageTier<ReactiveCollectionChangeFrame>> = Rc::new(
        memory_append_log::<ReactiveCollectionChangeFrame>("persistent-index/changes"),
    );
    let opened_index_graph = graph();
    let opened_index = open_persistent_reactive_index(OpenPersistentReactiveIndexOptions {
        initial: vec![graphrefly::IndexRow {
            primary: "p1".to_owned(),
            secondary: 2,
            value: 20,
        }],
        collection: ReactiveIndexOptions::default().graph(opened_index_graph.clone()),
        persistence: PersistReactiveCollectionOptions {
            graph: Some(opened_index_graph),
            name: None,
            storage_prefix: Some("persistent-index".to_owned()),
            snapshot_key: None,
            snapshot_store: index_snapshots.clone(),
            change_log: Some(index_changes.clone()),
            snapshot_on_attach: true,
            snapshot_every_changes: None,
        },
    })
    .unwrap();
    opened_index.collection.upsert("p2".to_owned(), 1, 10);
    opened_index.collection.upsert("p3".to_owned(), 3, 30);
    opened_index.collection.delete_many(vec![
        "p1".to_owned(),
        "missing".to_owned(),
        "p3".to_owned(),
    ]);
    opened_index.persistence.flush().unwrap();

    let reopened_index_graph = graph();
    let reopened_index = open_persistent_reactive_index(OpenPersistentReactiveIndexOptions {
        initial: Vec::<graphrefly::IndexRow<String, i32, i32>>::new(),
        collection: ReactiveIndexOptions::default().graph(reopened_index_graph.clone()),
        persistence: PersistReactiveCollectionOptions {
            graph: Some(reopened_index_graph),
            name: None,
            storage_prefix: Some("persistent-index".to_owned()),
            snapshot_key: None,
            snapshot_store: index_snapshots,
            change_log: Some(index_changes),
            snapshot_on_attach: false,
            snapshot_every_changes: None,
        },
    })
    .unwrap();
    assert_eq!(reopened_index.collection.get(&"p1".to_owned()), None);
    assert_eq!(reopened_index.collection.get(&"p2".to_owned()), Some(10));
    assert_eq!(reopened_index.collection.get(&"p3".to_owned()), None);
}

#[test]
fn persistence_sidecar_snapshot_cadence_and_cross_graph_preflight() {
    let snapshots: Rc<dyn KvStorageTier<ReactiveCollectionSnapshotFrame>> =
        Rc::new(memory_kv::<ReactiveCollectionSnapshotFrame>());
    let changes: Rc<dyn AppendLogStorageTier<ReactiveCollectionChangeFrame>> = Rc::new(
        memory_append_log::<ReactiveCollectionChangeFrame>("cadence/changes"),
    );
    let g = graph();
    let list = reactive_list::<i32>(
        vec![0],
        ReactiveListOptions::named("cadence").graph(g.clone()),
    );
    let persistence = persist_reactive_list(
        &list,
        PersistReactiveCollectionOptions {
            graph: Some(g.clone()),
            name: None,
            storage_prefix: Some("cadence".to_owned()),
            snapshot_key: None,
            snapshot_store: snapshots.clone(),
            change_log: Some(changes.clone()),
            snapshot_on_attach: false,
            snapshot_every_changes: Some(2),
        },
    )
    .unwrap();

    list.append(1);
    assert!(snapshots
        .get(&reactive_collection_snapshot_key("cadence").unwrap())
        .unwrap()
        .is_none());
    list.append(2);
    assert_eq!(
        changes.size().unwrap(),
        0,
        "snapshotEveryChanges queues sidecar writes until flush"
    );
    assert!(snapshots
        .get(&reactive_collection_snapshot_key("cadence").unwrap())
        .unwrap()
        .is_none());
    persistence.flush().unwrap();
    assert_eq!(
        changes.size().unwrap(),
        2,
        "flush drains queued change writes before the cadence snapshot"
    );
    let snapshot = snapshots
        .get(&reactive_collection_snapshot_key("cadence").unwrap())
        .unwrap()
        .expect("snapshotEveryChanges writes a snapshot");
    assert_eq!(snapshot.change_cursor, 1);
    assert_eq!(snapshot.snapshot, json!([0, 1, 2]));
    let cursor = persistence.cursor_fact().unwrap();
    assert_eq!(cursor.change_seq, Some(1));
    assert_eq!(cursor.change_writes, 2);
    assert_eq!(cursor.snapshot_writes, 1);
    let status = persistence.status_fact().unwrap();
    assert_eq!(status.state, ReactiveCollectionPersistenceStatus::Ready);
    assert_eq!(status.writes, 3);

    let graphless = reactive_list::<i32>(Vec::new(), ReactiveListOptions::default());
    let graphless_snapshots: Rc<dyn KvStorageTier<ReactiveCollectionSnapshotFrame>> =
        Rc::new(memory_kv::<ReactiveCollectionSnapshotFrame>());
    let graphless_err = match persist_reactive_list(
        &graphless,
        PersistReactiveCollectionOptions {
            graph: None,
            name: Some("graphless/persistence".to_owned()),
            storage_prefix: Some("graphless".to_owned()),
            snapshot_key: Some("graphless/snapshot".to_owned()),
            snapshot_store: graphless_snapshots.clone(),
            change_log: None,
            snapshot_on_attach: false,
            snapshot_every_changes: None,
        },
    ) {
        Ok(_) => panic!("graphless persistence must fail before creating facts"),
        Err(err) => err,
    };
    assert!(graphless_err.to_string().contains("graph is required"));
    assert!(graphless_snapshots
        .get("graphless/snapshot")
        .unwrap()
        .is_none());

    let owner = graph();
    let other = graph();
    let owned = reactive_list::<i32>(
        Vec::new(),
        ReactiveListOptions::named("owned").graph(owner.clone()),
    );
    let cross_snapshots: Rc<dyn KvStorageTier<ReactiveCollectionSnapshotFrame>> =
        Rc::new(memory_kv::<ReactiveCollectionSnapshotFrame>());
    let cross_changes: Rc<dyn AppendLogStorageTier<ReactiveCollectionChangeFrame>> = Rc::new(
        memory_append_log::<ReactiveCollectionChangeFrame>("bad/changes"),
    );
    let err = match persist_reactive_list(
        &owned,
        PersistReactiveCollectionOptions {
            graph: Some(other.clone()),
            name: Some("bad/persistence".to_owned()),
            storage_prefix: Some("bad".to_owned()),
            snapshot_key: Some("bad/snapshot".to_owned()),
            snapshot_store: cross_snapshots.clone(),
            change_log: Some(cross_changes.clone()),
            snapshot_on_attach: false,
            snapshot_every_changes: None,
        },
    ) {
        Ok(_) => panic!("cross-graph persistence must fail before creating facts"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("different graph"));
    assert!(
        other.find("bad/persistence.ready").is_none(),
        "cross-graph preflight must not create graph-visible sidecar facts"
    );
    assert!(
        owner.find("bad/persistence.ready").is_none(),
        "cross-graph preflight must not create facts on the collection owner graph either"
    );
    assert!(cross_snapshots.get("bad/snapshot").unwrap().is_none());
    assert_eq!(cross_changes.size().unwrap(), 0);
}

#[test]
fn persistence_sidecar_flush_without_change_log_writes_snapshot_and_failed_append_retries() {
    let snapshots: Rc<dyn KvStorageTier<ReactiveCollectionSnapshotFrame>> =
        Rc::new(memory_kv::<ReactiveCollectionSnapshotFrame>());
    let snapshot_only_graph = graph();
    let list = reactive_list::<i32>(
        vec![1],
        ReactiveListOptions::default().graph(snapshot_only_graph.clone()),
    );
    let persistence = persist_reactive_list(
        &list,
        PersistReactiveCollectionOptions {
            graph: Some(snapshot_only_graph),
            name: None,
            storage_prefix: Some("snapshot-only".to_owned()),
            snapshot_key: None,
            snapshot_store: snapshots.clone(),
            change_log: None,
            snapshot_on_attach: false,
            snapshot_every_changes: None,
        },
    )
    .unwrap();
    list.append(2);
    persistence.flush().unwrap();
    let snapshot = snapshots
        .get(&reactive_collection_snapshot_key("snapshot-only").unwrap())
        .unwrap()
        .expect("flush writes snapshot when no change log exists");
    assert_eq!(snapshot.snapshot, json!([1, 2]));

    let failing_snapshots = Rc::new(FailOnceSnapshotStore::new());
    let failing_snapshot_graph = graph();
    let failing_snapshot_list = reactive_list::<i32>(
        vec![1],
        ReactiveListOptions::default().graph(failing_snapshot_graph.clone()),
    );
    let failing_snapshot = persist_reactive_list(
        &failing_snapshot_list,
        PersistReactiveCollectionOptions {
            graph: Some(failing_snapshot_graph),
            name: None,
            storage_prefix: Some("snapshot-failure".to_owned()),
            snapshot_key: None,
            snapshot_store: failing_snapshots,
            change_log: None,
            snapshot_on_attach: false,
            snapshot_every_changes: None,
        },
    )
    .unwrap();
    failing_snapshot_list.append(2);
    assert!(failing_snapshot
        .flush()
        .unwrap_err()
        .to_string()
        .contains("injected snapshot failure"));
    let error = failing_snapshot
        .error_fact()
        .unwrap()
        .expect("snapshot error fact is set");
    assert_eq!(error.phase, "snapshot");
    assert!(error.message.contains("injected snapshot failure"));

    let retry_snapshots: Rc<dyn KvStorageTier<ReactiveCollectionSnapshotFrame>> =
        Rc::new(memory_kv::<ReactiveCollectionSnapshotFrame>());
    let failing_log = Rc::new(FailOnceChangeLog::new("retry/changes"));
    let retry_graph = graph();
    let retry_list = reactive_list::<i32>(
        vec![0],
        ReactiveListOptions::default().graph(retry_graph.clone()),
    );
    let retry = persist_reactive_list(
        &retry_list,
        PersistReactiveCollectionOptions {
            graph: Some(retry_graph),
            name: None,
            storage_prefix: Some("retry".to_owned()),
            snapshot_key: None,
            snapshot_store: retry_snapshots,
            change_log: Some(failing_log.clone()),
            snapshot_on_attach: false,
            snapshot_every_changes: None,
        },
    )
    .unwrap();
    retry_list.append(1);
    assert!(retry
        .flush()
        .unwrap_err()
        .to_string()
        .contains("injected append failure"));
    let status = retry.status_fact().unwrap();
    assert_eq!(status.state, ReactiveCollectionPersistenceStatus::Errored);
    assert_eq!(status.errors, 1);
    let error = retry.error_fact().unwrap().expect("error fact is set");
    assert_eq!(error.phase, "change");
    assert!(error.message.contains("injected append failure"));

    // The failed frame remains queued; `snapshot()` drains it before writing the baseline.
    retry.snapshot().unwrap();
    // The public handle stays errored, but the underlying log proves the queued frame was not lost.
    assert_eq!(failing_log.size().unwrap(), 1);
}

#[test]
fn graph_checkpoint_uses_collection_backend_state_as_authority() {
    let g = graph();
    let list = reactive_list::<i32>(vec![1], ReactiveListOptions::named("list").graph(g.clone()));
    list.append(2);
    let _activate_snapshot = list.snapshot.subscribe(|_| {});

    let checkpoint = g.checkpoint().expect("checkpoint succeeds");
    let by_id = checkpoint
        .nodes
        .iter()
        .map(|node| (node.id.as_str(), node))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(by_id["list.delta"].backend_state, Some(json!([1, 2])));
    assert_eq!(by_id["list.snapshot"].value, GraphCheckpointValue::Sentinel);
    assert_eq!(
        by_id["list.snapshot"].ctx_state.value,
        GraphCheckpointValue::Sentinel,
        "R-snapshot/D160: collection helper ctxState is a non-authoritative derived cache"
    );

    let mut stale = checkpoint.clone();
    stale
        .nodes
        .iter_mut()
        .find(|node| node.id == "list.snapshot")
        .expect("snapshot node exists")
        .value = GraphCheckpointValue::Data {
        data: json!(["stale"]),
    };
    let restored = restore_graph(stale, RestoreGraphOptions::new(default_restore_registry()))
        .expect("collection backendState restores");
    let restored_checkpoint = restored.checkpoint().expect("re-checkpoint succeeds");
    let restored_delta = restored_checkpoint
        .nodes
        .iter()
        .find(|node| node.id == "list.delta")
        .expect("restored delta exists");
    assert_eq!(restored_delta.backend_state, Some(json!([1, 2])));

    let view_graph = graph();
    let map = reactive_map::<String, i32>(
        vec![("a".to_owned(), 1), ("b".to_owned(), 2)],
        ReactiveMapOptions::named("map").graph(view_graph.clone()),
    );
    let view = map.select(|value, _key| *value > 1);
    view.snapshot
        .up(vec![Message::Pull(PullDemand::new(view.pull_id.clone()))]);
    let view_checkpoint = view_graph.checkpoint().expect("view checkpoint succeeds");
    let view_by_id = view_checkpoint
        .nodes
        .iter()
        .map(|node| (node.id.as_str(), node))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(
        view_by_id["map.select#0.delta"].value,
        GraphCheckpointValue::Sentinel
    );
    assert_eq!(
        view_by_id["map.select#0.snapshot"].value,
        GraphCheckpointValue::Sentinel
    );
    assert_eq!(
        view_by_id["map.select#0.snapshot"].ctx_state.value,
        GraphCheckpointValue::Sentinel,
        "collection light-view helper caches are also non-authoritative"
    );
}

#[test]
fn collection_checkpoint_fails_honestly_for_unsupported_backend_and_policy_state() {
    let unsupported = graph();
    reactive_list::<i16>(
        vec![1],
        ReactiveListOptions::named("unsupported").graph(unsupported.clone()),
    );
    let err = unsupported.checkpoint().unwrap_err();
    assert!(
        err.to_string().contains("not strict JSON compatible"),
        "unsupported host-ish backend values fail rather than widening D160 implicitly"
    );

    let local_only = graph();
    reactive_list::<i32>(
        vec![1, 2, 3],
        ReactiveListOptions::named("capped")
            .graph(local_only.clone())
            .max_size(2),
    );
    let checkpoint = local_only.checkpoint().unwrap();
    let capped = checkpoint
        .nodes
        .iter()
        .find(|node| node.id == "capped.delta")
        .expect("capped list delta exists");
    assert!(matches!(
        capped.factory,
        GraphCheckpointFactory::LocalOnly { .. }
    ));
    assert_eq!(capped.backend_state, None);
}

#[test]
fn capped_reactive_log_checkpoint_restores_with_max_size_config() {
    let g = graph();
    let log = reactive_log::<i32>(
        vec![1, 2, 3],
        ReactiveLogOptions::named("log")
            .graph(g.clone())
            .max_size(2),
    );
    log.append(4);

    let checkpoint = g.checkpoint().unwrap();
    let by_id = checkpoint
        .nodes
        .iter()
        .map(|node| (node.id.as_str(), node))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(by_id["log.delta"].backend_state, Some(json!([3, 4])));
    assert!(matches!(
        &by_id["log.delta"].factory,
        GraphCheckpointFactory::RegistryRef { config, .. }
            if config.as_ref().is_some_and(|value| value == &json!({ "maxSize": 2 }))
    ));

    let restored = restore_graph(
        checkpoint,
        RestoreGraphOptions::new(default_restore_registry()),
    )
    .expect("capped log restores through config");
    let restored_checkpoint = restored.checkpoint().unwrap();
    let restored_log = restored_checkpoint
        .nodes
        .iter()
        .find(|node| node.id == "log.delta")
        .expect("restored log delta exists");
    assert_eq!(restored_log.backend_state, Some(json!([3, 4])));
}

#[test]
fn json_codecs_round_trip_and_strict_decode_rejects_noncanonical_bytes() {
    let json_codec = json_codec_for::<serde_json::Value>();
    let strict_codec = strict_json_codec_for::<serde_json::Value>();
    let value = json!({ "z": 1, "a": [true, null] });

    assert_eq!(
        String::from_utf8(
            <JsonCodec as Codec<serde_json::Value>>::encode(&json_codec, &value).unwrap()
        )
        .unwrap(),
        r#"{"a":[true,null],"z":1}"#
    );
    assert_eq!(
        <JsonCodec as Codec<serde_json::Value>>::decode(
            &json_codec,
            br#"{ "z": 1, "a": [true, null] }"#
        )
        .unwrap(),
        value
    );
    assert_eq!(
        <graphrefly::StrictJsonCodec as Codec<serde_json::Value>>::decode(
            &strict_codec,
            br#"{"a":[true,null],"z":1}"#
        )
        .unwrap(),
        value
    );
    assert!(
        <graphrefly::StrictJsonCodec as Codec<serde_json::Value>>::decode(
            &strict_codec,
            br#"{ "a": [true, null], "z": 1 }"#
        )
        .unwrap_err()
        .to_string()
        .contains("canonical")
    );
}

#[test]
fn decimal_scalar_helpers_keep_host_conversion_explicit() {
    assert_eq!(i128_to_decimal_string(-12), "-12");
    assert_eq!(i128_to_decimal_string(0), "0");
    assert_eq!(u128_to_non_negative_decimal_string(12), "12");
    assert_eq!(decimal_string_to_i128("-12").unwrap(), -12);
    assert_eq!(decimal_string_to_i128("0").unwrap(), 0);
    assert_eq!(non_negative_decimal_string_to_u128("12").unwrap(), 12);
    assert_eq!(assert_decimal_integer_string("-12", "t_ns").unwrap(), "-12");
    assert_eq!(
        assert_non_negative_decimal_integer_string("12", "t_ns").unwrap(),
        "12"
    );

    assert!(is_decimal_integer_string("-12"));
    assert!(!is_decimal_integer_string("-0"));
    assert!(!is_decimal_integer_string("01"));
    assert!(is_non_negative_decimal_integer_string("12"));
    assert!(!is_non_negative_decimal_integer_string("-12"));
    assert!(assert_decimal_integer_string("01", "t_ns").is_err());
    assert!(assert_non_negative_decimal_integer_string("-12", "t_ns").is_err());
}

#[test]
fn content_addressed_kv_keys_are_deterministic_across_object_key_order() {
    let kv = Rc::new(memory_kv::<String>());
    let cache = content_addressed_kv(
        ContentAddressedKvOptions::new(kv)
            .with_key_prefix("calc")
            .with_key_context(|ctx: &serde_json::Value| Ok(ctx["request"].clone())),
    );

    let a = cache
        .key_for(&json!({ "request": { "b": 2, "a": { "d": 4, "c": 3 } } }))
        .unwrap();
    let b = cache
        .key_for(&json!({ "request": { "a": { "c": 3, "d": 4 }, "b": 2 } }))
        .unwrap();

    assert_eq!(a, b);
    assert_eq!(
        a,
        "calc:[\"c461c47a913352f1a21e3f2ea49e1fd34754c0dc12cb7366e4636d5e186c6c6e\"]"
    );
}

#[test]
fn content_addressed_kv_honors_modes_and_strict_misses() {
    let kv = Rc::new(memory_kv::<String>());
    let ctx = json!({ "prompt": "hello", "opts": { "temp": 0 } });

    let write_only = content_addressed_kv(
        ContentAddressedKvOptions::new(kv.clone()).with_mode(ContentAddressedMode::Write),
    );
    write_only.store(&ctx, "hi".to_owned()).unwrap();
    assert_eq!(write_only.lookup(&ctx).unwrap(), None);

    let read_only = content_addressed_kv(
        ContentAddressedKvOptions::new(kv.clone()).with_mode(ContentAddressedMode::Read),
    );
    assert_eq!(read_only.lookup(&ctx).unwrap(), Some("hi".to_owned()));
    read_only.store(&ctx, "ignored".to_owned()).unwrap();
    assert_eq!(read_only.lookup(&ctx).unwrap(), Some("hi".to_owned()));

    let read_write = content_addressed_storage(ContentAddressedKvOptions::new(kv.clone()));
    let bye = json!({ "prompt": "bye" });
    read_write.store(&bye, "goodbye".to_owned()).unwrap();
    assert_eq!(read_write.lookup(&bye).unwrap(), Some("goodbye".to_owned()));

    let strict = content_addressed_kv(
        ContentAddressedKvOptions::new(kv).with_mode(ContentAddressedMode::ReadStrict),
    );
    assert!(matches!(
        strict.lookup(&json!({ "prompt": "missing" })).unwrap_err(),
        StorageError::ContentAddressedMiss { .. }
    ));
}

#[test]
fn content_addressed_disallowed_modes_skip_bad_key_contexts() {
    let kv = Rc::new(memory_kv::<String>());
    let bad_ctx = json!({ "value": 1 });

    let write_only = content_addressed_kv(
        ContentAddressedKvOptions::new(kv.clone())
            .with_mode(ContentAddressedMode::Write)
            .with_key_context(|_| Err(graphrefly::JsonCodecError::validation("bad context"))),
    );
    assert_eq!(write_only.lookup(&bad_ctx).unwrap(), None);
    assert!(write_only.store(&bad_ctx, "value".to_owned()).is_err());
    assert!(write_only.forget(&bad_ctx).is_ok());

    let read_only = content_addressed_kv(
        ContentAddressedKvOptions::new(kv)
            .with_mode(ContentAddressedMode::Read)
            .with_key_context(|_| Err(graphrefly::JsonCodecError::validation("bad context"))),
    );
    assert!(read_only.store(&bad_ctx, "ignored".to_owned()).is_ok());
    assert!(read_only.forget(&bad_ctx).is_ok());
    assert!(read_only.lookup(&bad_ctx).is_err());
}

#[test]
fn file_backend_persists_bytes_lists_logical_keys_and_supports_put_if_absent() {
    let dir = temp_storage_dir("file-backend");
    let backend =
        file_backend(dir.clone(), FileBackendOptions::new().with_namespace("ns")).unwrap();
    let default_namespace = file_backend(dir.clone(), FileBackendOptions::new()).unwrap();

    default_namespace.put("root", &[5]).unwrap();
    backend.put("", &[0]).unwrap();
    backend.put("a", &[1, 2, 3]).unwrap();
    backend.put("ab", &[9, 8, 7]).unwrap();
    assert!(!backend.put_if_absent("a", &[4]).unwrap());
    assert!(backend.put_if_absent("c", &[3]).unwrap());

    let mut first = backend.get("a").unwrap().unwrap();
    first[0] = 99;
    assert_eq!(backend.get("a").unwrap(), Some(vec![1, 2, 3]));
    assert_eq!(backend.get("ab").unwrap(), Some(vec![9, 8, 7]));
    assert_eq!(backend.list("").unwrap(), vec!["", "a", "ab", "c"]);
    assert_eq!(backend.list("a").unwrap(), vec!["a", "ab"]);

    backend.delete("ab").unwrap();
    assert_eq!(backend.get("ab").unwrap(), None);
    assert_eq!(backend.list("").unwrap(), vec!["", "a", "c"]);

    let other_namespace = file_backend(
        dir.clone(),
        FileBackendOptions::new().with_namespace("other"),
    )
    .unwrap();
    assert_eq!(other_namespace.list("").unwrap(), Vec::<String>::new());
    assert_eq!(default_namespace.list("").unwrap(), vec!["root"]);

    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn file_backend_rejects_ambiguous_keys_and_bad_extensions() {
    let dir = temp_storage_dir("file-backend-invalid");

    assert!(
        file_backend(dir.clone(), FileBackendOptions::new().with_extension("bin"))
            .unwrap_err()
            .to_string()
            .contains("extension")
    );
    assert!(file_backend(
        dir.clone(),
        FileBackendOptions::new().with_extension("../bad")
    )
    .unwrap_err()
    .to_string()
    .contains("extension"));
    assert!(file_backend(
        dir.clone(),
        FileBackendOptions::new().with_extension(".bad\0ext")
    )
    .unwrap_err()
    .to_string()
    .contains("extension"));

    let backend = file_backend(dir.clone(), FileBackendOptions::new()).unwrap();
    backend.put("%", &[7]).unwrap();
    fs::write(dir.join("k-%.bin"), [9]).unwrap();
    fs::write(dir.join("k-%2F.bin"), [9]).unwrap();
    assert_eq!(backend.list("").unwrap(), vec!["%"]);

    backend.put("bad\0key", &[1]).unwrap();
    assert_eq!(backend.get("bad\0key").unwrap(), Some(vec![1]));
    assert_eq!(backend.list("bad\0").unwrap(), vec!["bad\0key"]);

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn file_kv_and_append_log_lift_codec_storage_without_restore_semantics() {
    let dir = temp_storage_dir("file-kv");
    let kv = file_kv::<serde_json::Value, _>(
        dir.clone(),
        FileBackendOptions::new().with_namespace("json"),
        strict_json_codec_for::<serde_json::Value>(),
    )
    .unwrap();
    kv.set("item", json!({ "b": 2, "a": 1 })).unwrap();
    assert_eq!(kv.get("item").unwrap(), Some(json!({ "a": 1, "b": 2 })));
    assert!(!kv
        .put_if_absent("item", json!({ "ignored": true }))
        .unwrap());
    assert!(kv.put_if_absent("other", json!({ "ok": true })).unwrap());
    assert_eq!(kv.list("").unwrap(), vec!["item", "other"]);

    let log = file_append_log::<serde_json::Value, _>(
        dir.clone(),
        FileAppendLogOptions::new()
            .with_backend(FileBackendOptions::new().with_namespace("log"))
            .with_prefix("events"),
        strict_json_codec_for::<serde_json::Value>(),
    )
    .unwrap();
    log.append(json!({ "value": "a" })).unwrap();
    log.append(json!({ "value": "b" })).unwrap();
    assert_eq!(
        read_append_log_page(
            &log,
            AppendLogReadOptions {
                after: None,
                limit: Some(1)
            }
        )
        .unwrap()
        .entries
        .into_iter()
        .map(|entry| entry.value)
        .collect::<Vec<_>>(),
        vec![json!({ "value": "a" })]
    );

    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn wal_frame_key_builds_padded_passive_storage_keys() {
    let prefix = wal_frame_prefix("graph/main");
    assert_eq!(prefix, "graph/main/wal");
    assert_eq!(wal_frame_prefix(""), "wal");
    assert_eq!(
        wal_frame_key(&prefix, 7),
        "graph/main/wal/00000000000000000007"
    );
    assert_eq!(
        wal_frame_key(&prefix, u64::MAX),
        "graph/main/wal/18446744073709551615"
    );
}

#[test]
fn wal_frame_produces_stable_checksums_and_detects_tampering() {
    let body = WalFrameBody {
        t: "c".to_owned(),
        lifecycle: ChangeLifecycle::Data,
        path: "count".to_owned(),
        change: json!({ "op": "set", "value": 1 }),
        frame_seq: 2,
        frame_t_ns: "123".to_owned(),
        format_version: WAL_FORMAT_VERSION,
    };

    let checksum = wal_frame_checksum(&body).unwrap();
    assert_eq!(checksum.len(), 64);
    assert!(checksum
        .bytes()
        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')));
    assert_eq!(wal_frame_checksum(&body).unwrap(), checksum);
    assert_eq!(
        wal_frame_checksum(&WalFrameBody {
            change: json!({ "value": 1, "op": "set" }),
            ..body.clone()
        })
        .unwrap(),
        checksum
    );

    let frame =
        wal_frame(WalFrameOptions::new("count", body.change.clone(), 2).with_frame_t_ns("123"))
            .unwrap();
    assert_eq!(frame.t, "c");
    assert_eq!(frame.lifecycle, ChangeLifecycle::Data);
    assert_eq!(frame.path, "count");
    assert_eq!(frame.change, json!({ "op": "set", "value": 1 }));
    assert_eq!(frame.frame_seq, 2);
    assert_eq!(frame.frame_t_ns, "123");
    assert_eq!(frame.format_version, WAL_FORMAT_VERSION);
    assert!(verify_wal_frame_checksum(&frame).unwrap());
    assert!(!verify_wal_frame_checksum(&WalFrame {
        path: "other".to_owned(),
        ..frame.clone()
    })
    .unwrap());

    let frame_value = serde_json::to_value(&frame).unwrap();
    for forbidden in ["snapshot", "restore", "checkpoint", "factory"] {
        assert!(frame_value.get(forbidden).is_none());
    }
}

#[test]
fn wal_frame_codec_validates_passive_shape_without_restore_semantics() {
    let frame = wal_frame(WalFrameOptions::new("node", json!({ "event": "DATA" }), 0)).unwrap();
    let codec = wal_frame_codec::<serde_json::Value>();
    let decoded = <graphrefly::WalFrameCodec<serde_json::Value> as Codec<
        WalFrame<serde_json::Value>,
    >>::decode(&codec, &codec.encode(&frame).unwrap())
    .unwrap();
    assert_eq!(decoded, frame);
    assert_wal_frame(&frame).unwrap();
    assert!(assert_wal_frame(&WalFrame {
        checksum: "BAD".to_owned(),
        ..frame.clone()
    })
    .unwrap_err()
    .to_string()
    .contains("checksum"));

    let strict = strict_json_codec_for::<serde_json::Value>();
    let mut bad_t = serde_json::to_value(&frame).unwrap();
    bad_t["t"] = json!("r");
    assert!(codec
        .decode(&strict.encode(&bad_t).unwrap())
        .unwrap_err()
        .to_string()
        .contains("t must be c"));

    let mut bad_lifecycle = serde_json::to_value(&frame).unwrap();
    bad_lifecycle["lifecycle"] = json!("restore");
    assert!(codec
        .decode(&strict.encode(&bad_lifecycle).unwrap())
        .unwrap_err()
        .to_string()
        .contains("lifecycle"));

    assert!(codec
        .decode(br#"{"checksum":"bad","checksum":"also-bad"}"#)
        .unwrap_err()
        .to_string()
        .contains("duplicate object key"));
    assert!(codec
        .decode(format!(r#"{{"path":"node","checksum":"{}"}}"#, frame.checksum).as_bytes())
        .unwrap_err()
        .to_string()
        .contains("canonical"));
}

#[test]
fn wal_frame_shape_checks_reject_malformed_passive_frames() {
    let frame = wal_frame(WalFrameOptions::new("node", json!({ "event": "DATA" }), 0)).unwrap();
    let codec = wal_frame_codec::<serde_json::Value>();
    let strict = strict_json_codec_for::<serde_json::Value>();

    let mut missing_change = serde_json::to_value(&frame).unwrap();
    missing_change.as_object_mut().unwrap().remove("change");
    assert!(codec
        .decode(&strict.encode(&missing_change).unwrap())
        .unwrap_err()
        .to_string()
        .contains("change payload"));

    for (field, expected) in [
        ("restore", "unknown field restore"),
        ("checkpoint", "unknown field checkpoint"),
    ] {
        let mut value = serde_json::to_value(&frame).unwrap();
        value[field] = json!(true);
        assert!(codec
            .decode(&strict.encode(&value).unwrap())
            .unwrap_err()
            .to_string()
            .contains(expected));
    }

    let mut empty_path = serde_json::to_value(&frame).unwrap();
    empty_path["path"] = json!("");
    assert!(codec
        .decode(&strict.encode(&empty_path).unwrap())
        .unwrap_err()
        .to_string()
        .contains("path"));

    let mut bad_seq = serde_json::to_value(&frame).unwrap();
    bad_seq["frame_seq"] = json!(-1);
    assert!(codec
        .decode(&strict.encode(&bad_seq).unwrap())
        .unwrap_err()
        .to_string()
        .contains("frame_seq"));

    let mut bad_timestamp = serde_json::to_value(&frame).unwrap();
    bad_timestamp["frame_t_ns"] = json!("01");
    assert!(codec
        .decode(&strict.encode(&bad_timestamp).unwrap())
        .unwrap_err()
        .to_string()
        .contains("frame_t_ns"));

    let mut bad_version = serde_json::to_value(&frame).unwrap();
    bad_version["format_version"] = json!(WAL_FORMAT_VERSION + 1);
    assert!(codec
        .decode(&strict.encode(&bad_version).unwrap())
        .unwrap_err()
        .to_string()
        .contains("format_version"));

    let mut extra = serde_json::to_value(&frame).unwrap();
    extra["checkpoint"] = json!({ "nodes": [] });
    assert!(codec
        .decode(&strict.encode(&extra).unwrap())
        .unwrap_err()
        .to_string()
        .contains("unknown field checkpoint"));
}

#[test]
fn wal_frames_store_and_page_as_ordinary_append_log_facts() {
    let log = memory_append_log::<WalFrame<serde_json::Value>>("wal-store");
    log.append(wal_frame(WalFrameOptions::new("a", json!({ "value": "a" }), 0)).unwrap())
        .unwrap();
    log.append(wal_frame(WalFrameOptions::new("b", json!({ "value": "b" }), 1)).unwrap())
        .unwrap();
    log.append(wal_frame(WalFrameOptions::new("c", json!({ "value": "c" }), 2)).unwrap())
        .unwrap();

    let page = read_append_log_page(
        &log,
        AppendLogReadOptions {
            after: None,
            limit: Some(2),
        },
    )
    .unwrap();
    assert_eq!(
        page.entries
            .iter()
            .map(|entry| (entry.seq, entry.value.frame_seq, entry.value.path.as_str()))
            .collect::<Vec<_>>(),
        vec![(0, 0, "a"), (1, 1, "b")]
    );
    assert!(!page.done);
    assert_eq!(
        read_append_log_page(
            &log,
            AppendLogReadOptions {
                after: page.next_after,
                limit: None,
            },
        )
        .unwrap()
        .entries[0]
            .value
            .path,
        "c"
    );
}

#[test]
fn attach_observe_event_log_persists_mapped_data_frames_in_observe_order() {
    let g = graph();
    let count = g.state_opts(0, graphrefly::GraphNodeOpts::named("count"));
    let log = memory_append_log::<ObserveEventFrame<i32>>("observe");
    let mut handle = attach_observe_event_log(
        &g,
        Rc::new(log.clone()),
        AttachObserveEventLogOptions::from_map(|event| match &event.msg {
            ObserveMessage::Data(value) => value.as_ref().downcast_ref::<i32>().copied(),
            _ => None,
        })
        .with_path("count")
        .with_stream("audit"),
    );

    assert_eq!(log.read(AppendLogReadOptions::default()).unwrap().len(), 0);
    count.set(1);
    count.set(2);
    assert_eq!(log.read(AppendLogReadOptions::default()).unwrap().len(), 0);
    handle.flush().unwrap();

    let frames = log
        .read(AppendLogReadOptions::default())
        .unwrap()
        .into_iter()
        .map(|entry| entry.value)
        .collect::<Vec<_>>();
    assert_eq!(
        frames.iter().map(|frame| frame.change).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(
        frames
            .iter()
            .map(|frame| frame.path.as_str())
            .collect::<Vec<_>>(),
        vec!["count", "count", "count"]
    );
    assert_eq!(
        frames
            .iter()
            .map(|frame| frame.stream.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("audit"), Some("audit"), Some("audit")]
    );
    assert!(frames
        .windows(2)
        .all(|pair| pair[0].observe_seq < pair[1].observe_seq));

    handle.dispose().unwrap();
    count.set(3);
    handle.flush().unwrap();
    assert_eq!(
        log.read(AppendLogReadOptions::default())
            .unwrap()
            .into_iter()
            .map(|entry| entry.value.change)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
}

#[derive(Clone)]
struct FailAfterSuccessfulAppendsLog<T: Clone> {
    inner: graphrefly::AppendLogStorage<T>,
    remaining_successes_before_failure: Rc<Cell<usize>>,
    failed: Rc<Cell<bool>>,
}

impl<T: Clone> AppendLogStorageTier<T> for FailAfterSuccessfulAppendsLog<T> {
    fn append(&self, value: T) -> StorageResult<AppendLogEntry<T>> {
        let remaining = self.remaining_successes_before_failure.get();
        if remaining == 0 && !self.failed.get() {
            self.failed.set(true);
            return Err(StorageError::backend("append failed once"));
        }
        if remaining > 0 {
            self.remaining_successes_before_failure.set(remaining - 1);
        }
        self.inner.append(value)
    }

    fn read(&self, opts: AppendLogReadOptions) -> StorageResult<Vec<AppendLogEntry<T>>> {
        self.inner.read(opts)
    }

    fn truncate_after(&self, seq: u64) -> StorageResult<()> {
        self.inner.truncate_after(seq)
    }

    fn size(&self) -> StorageResult<usize> {
        self.inner.size()
    }
}

#[test]
fn observe_event_log_flush_keeps_failed_frame_for_retry() {
    let g = graph();
    let count = g.state_opts(0, graphrefly::GraphNodeOpts::named("count"));
    let inner = memory_append_log::<ObserveEventFrame<i32>>("observe-retry");
    let log = FailAfterSuccessfulAppendsLog {
        inner: inner.clone(),
        remaining_successes_before_failure: Rc::new(Cell::new(1)),
        failed: Rc::new(Cell::new(false)),
    };
    let handle = attach_observe_event_log(
        &g,
        Rc::new(log),
        AttachObserveEventLogOptions::from_map(|event| match &event.msg {
            ObserveMessage::Data(value) => value.as_ref().downcast_ref::<i32>().copied(),
            _ => None,
        })
        .with_path("count"),
    );

    count.set(1);

    assert!(handle
        .flush()
        .unwrap_err()
        .to_string()
        .contains("append failed once"));
    assert_eq!(
        inner
            .read(AppendLogReadOptions::default())
            .unwrap()
            .into_iter()
            .map(|entry| entry.value.change)
            .collect::<Vec<_>>(),
        vec![0],
        "a successful frame before the failed append is committed exactly once"
    );
    handle.flush().unwrap();
    assert_eq!(
        inner
            .read(AppendLogReadOptions::default())
            .unwrap()
            .into_iter()
            .map(|entry| entry.value.change)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
}

#[test]
fn observe_event_log_rollback_drops_queued_frames_before_flush() {
    let g = graph();
    let count = g.state_opts(0, graphrefly::GraphNodeOpts::named("count"));
    let log = memory_append_log::<ObserveEventFrame<i32>>("observe-rollback");
    let handle = attach_observe_event_log(
        &g,
        Rc::new(log.clone()),
        AttachObserveEventLogOptions::from_map(|event| match &event.msg {
            ObserveMessage::Data(value) => value.as_ref().downcast_ref::<i32>().copied(),
            _ => None,
        })
        .with_path("count"),
    );

    count.set(1);
    handle.rollback().unwrap();
    handle.flush().unwrap();

    assert_eq!(log.read(AppendLogReadOptions::default()).unwrap().len(), 0);
}

#[test]
fn observe_event_log_page_orders_by_append_sequence_not_graph_projection() {
    let entries = memory_kv::<ObserveEventFrame<i32>>();
    entries
        .set(
            &append_log_key("observe-unordered", 0),
            observe_event_frame(10, "count", 1, ObserveEventFrameOptions::default()).unwrap(),
        )
        .unwrap();
    entries
        .set(
            &append_log_key("observe-unordered", 2),
            observe_event_frame(5, "count", 2, ObserveEventFrameOptions::default()).unwrap(),
        )
        .unwrap();
    entries
        .set(
            &append_log_key("observe-unordered", 10),
            observe_event_frame(7, "count", 3, ObserveEventFrameOptions::default()).unwrap(),
        )
        .unwrap();
    let log = append_log_storage(Rc::new(entries), "observe-unordered");

    let page = read_observe_event_log_page(
        &log,
        AppendLogReadOptions {
            after: None,
            limit: Some(3),
        },
    )
    .unwrap();

    assert_eq!(
        page.entries
            .iter()
            .map(|entry| (entry.seq, entry.value.change))
            .collect::<Vec<_>>(),
        vec![(0, 1), (2, 2), (10, 3)]
    );
    assert_eq!(
        page.entries
            .iter()
            .map(|entry| entry.value.observe_seq)
            .collect::<Vec<_>>(),
        vec![10, 5, 7]
    );
    assert!(page.done);
}

#[test]
fn change_and_observe_event_codecs_validate_storage_frames_only() {
    let change_codec = change_envelope_codec::<serde_json::Value>();
    let change = envelope_change(
        json!({ "op": "set" }),
        ChangeEnvelopeOptions::new("kv-change")
            .with_t_ns("123")
            .with_seq(0),
    )
    .unwrap();
    assert_eq!(
        <graphrefly::ChangeEnvelopeCodec<serde_json::Value> as Codec<
            graphrefly::ChangeEnvelope<serde_json::Value>,
        >>::decode(&change_codec, &change_codec.encode(&change).unwrap())
        .unwrap()
        .change,
        json!({ "op": "set" })
    );
    assert!(change_codec
        .decode(br#"{"change":{},"change":{"op":"set"}}"#)
        .unwrap_err()
        .to_string()
        .contains("duplicate object key"));
    assert!(change_codec
        .decode(
            br#"{"lifecycle":"data","structure":"kv-change","version":1,"t_ns":"123","change":{}}"#
        )
        .unwrap_err()
        .to_string()
        .contains("canonical"));
    assert!(envelope_change(
        json!({}),
        ChangeEnvelopeOptions::new("kv-change").with_version(json!({ "restore": true }))
    )
    .unwrap_err()
    .to_string()
    .contains("version"));

    let frame = observe_event_frame(
        7,
        "count",
        json!({ "value": 1 }),
        ObserveEventFrameOptions::default().with_stream("audit"),
    )
    .unwrap();
    let frame_codec = observe_event_frame_codec::<serde_json::Value>();
    let decoded = <graphrefly::ObserveEventFrameCodec<serde_json::Value> as Codec<
        ObserveEventFrame<serde_json::Value>,
    >>::decode(&frame_codec, &frame_codec.encode(&frame).unwrap())
    .unwrap();

    assert_eq!(decoded.structure, "observe-event");
    assert_eq!(decoded.observe_seq, 7);
    assert_eq!(decoded.path, "count");
    assert_eq!(decoded.stream.as_deref(), Some("audit"));
    assert_eq!(decoded.change, json!({ "value": 1 }));
    assert!(frame_codec
        .decode(br#"{"change":{},"change":{"value":1}}"#)
        .unwrap_err()
        .to_string()
        .contains("duplicate object key"));
    assert!(frame_codec
        .decode(
            br#"{"lifecycle":"data","structure":"observe-event","version":1,"t_ns":"123","change":{},"observeSeq":1,"path":"count"}"#
        )
        .unwrap_err()
        .to_string()
        .contains("canonical"));
}

#[test]
fn append_log_key_uses_canonical_padded_sequence_keys() {
    assert_eq!(append_log_key("events", 7), "events/00000000000000000007");
    assert_eq!(
        append_log_key("events", u64::MAX),
        "events/18446744073709551615"
    );
}

#[test]
fn memory_append_log_appends_reads_sizes_and_truncates_in_order() {
    let log = memory_append_log::<String>("changes");
    assert_eq!(log.append("a".to_owned()).unwrap().seq, 0);
    assert_eq!(log.append("b".to_owned()).unwrap().seq, 1);
    assert_eq!(log.append("c".to_owned()).unwrap().seq, 2);

    assert_eq!(log.size().unwrap(), 3);
    assert_eq!(
        log.read(AppendLogReadOptions {
            after: Some(0),
            limit: None
        })
        .unwrap()
        .into_iter()
        .map(|entry| (entry.seq, entry.value))
        .collect::<Vec<_>>(),
        vec![(1, "b".to_owned()), (2, "c".to_owned())]
    );

    log.truncate_after(0).unwrap();
    assert_eq!(
        log.read(AppendLogReadOptions::default())
            .unwrap()
            .into_iter()
            .map(|entry| (entry.seq, entry.value))
            .collect::<Vec<_>>(),
        vec![(0, "a".to_owned())]
    );
    assert_eq!(log.append("d".to_owned()).unwrap().seq, 1);
}

#[test]
fn read_append_log_page_returns_ordered_pages_and_next_cursor() {
    let log = memory_append_log::<String>("page");
    for value in ["a", "b", "c"] {
        log.append(value.to_owned()).unwrap();
    }

    let first = read_append_log_page(
        &log,
        AppendLogReadOptions {
            after: None,
            limit: Some(2),
        },
    )
    .unwrap();
    assert_eq!(
        first
            .entries
            .iter()
            .map(|entry| (entry.seq, entry.value.as_str()))
            .collect::<Vec<_>>(),
        vec![(0, "a"), (1, "b")]
    );
    assert_eq!(first.next_after, Some(1));
    assert!(!first.done);

    let second = read_append_log_page(
        &log,
        AppendLogReadOptions {
            after: first.next_after,
            limit: Some(2),
        },
    )
    .unwrap();
    assert_eq!(
        second
            .entries
            .iter()
            .map(|entry| (entry.seq, entry.value.as_str()))
            .collect::<Vec<_>>(),
        vec![(2, "c")]
    );
    assert_eq!(second.next_after, Some(2));
    assert!(second.done);

    let empty = read_append_log_page(
        &log,
        AppendLogReadOptions {
            after: Some(100),
            limit: Some(2),
        },
    )
    .unwrap();
    assert!(empty.entries.is_empty());
    assert_eq!(empty.next_after, Some(100));
    assert!(empty.done);
    assert!(read_append_log_page(
        &log,
        AppendLogReadOptions {
            after: None,
            limit: Some(0)
        }
    )
    .unwrap_err()
    .to_string()
    .contains("positive"));
    assert!(read_append_log_page(
        &log,
        AppendLogReadOptions {
            after: None,
            limit: Some(usize::MAX)
        }
    )
    .unwrap_err()
    .to_string()
    .contains("lookahead"));
}

#[test]
fn append_log_storage_sorts_unordered_listings_and_rejects_malformed_keys() {
    let kv = memory_kv::<String>();
    kv.set(&append_log_key("unordered", 10), "c".to_owned())
        .unwrap();
    kv.set(&append_log_key("unordered", 0), "a".to_owned())
        .unwrap();
    kv.set(&append_log_key("unordered", 2), "b".to_owned())
        .unwrap();
    let log = append_log_storage(Rc::new(kv.clone()), "unordered");

    assert_eq!(
        log.read(AppendLogReadOptions {
            after: None,
            limit: Some(2)
        })
        .unwrap()
        .into_iter()
        .map(|entry| (entry.seq, entry.value))
        .collect::<Vec<_>>(),
        vec![(0, "a".to_owned()), (2, "b".to_owned())]
    );

    let malformed = memory_kv::<String>();
    malformed
        .set("bad/not-a-sequence", "oops".to_owned())
        .unwrap();
    assert!(append_log_storage(Rc::new(malformed), "bad")
        .read(AppendLogReadOptions::default())
        .unwrap_err()
        .to_string()
        .contains("non-numeric"));

    let torn = TornAppendKv;
    assert!(append_log_storage(Rc::new(torn), "torn")
        .read(AppendLogReadOptions::default())
        .unwrap_err()
        .to_string()
        .contains("listed key is missing"));
}

#[test]
fn append_log_truncate_validates_all_keys_before_deleting() {
    struct OrderedMalformedKv {
        inner: graphrefly::MemoryKv<String>,
    }

    impl KvStorageTier<String> for OrderedMalformedKv {
        fn get(&self, key: &str) -> StorageResult<Option<String>> {
            self.inner.get(key)
        }

        fn set(&self, key: &str, value: String) -> StorageResult<()> {
            self.inner.set(key, value)
        }

        fn delete(&self, key: &str) -> StorageResult<()> {
            self.inner.delete(key)
        }

        fn list(&self, _prefix: &str) -> StorageResult<Vec<String>> {
            Ok(vec![append_log_key("partial", 1), "partial/bad".to_owned()])
        }
    }

    let inner = memory_kv::<String>();
    inner
        .set(&append_log_key("partial", 1), "keep".to_owned())
        .unwrap();
    inner.set("partial/bad", "bad".to_owned()).unwrap();
    let log = append_log_storage(
        Rc::new(OrderedMalformedKv {
            inner: inner.clone(),
        }),
        "partial",
    );

    assert!(log
        .truncate_after(0)
        .unwrap_err()
        .to_string()
        .contains("non-numeric"));
    assert_eq!(
        inner.get(&append_log_key("partial", 1)).unwrap(),
        Some("keep".to_owned())
    );
}

struct TornAppendKv;

impl KvStorageTier<String> for TornAppendKv {
    fn get(&self, _key: &str) -> StorageResult<Option<String>> {
        Ok(None)
    }

    fn set(&self, _key: &str, _value: String) -> StorageResult<()> {
        Ok(())
    }

    fn delete(&self, _key: &str) -> StorageResult<()> {
        Ok(())
    }

    fn list(&self, _prefix: &str) -> StorageResult<Vec<String>> {
        Ok(vec![append_log_key("torn", 0)])
    }
}

#[test]
fn memory_kv_put_if_absent_preserves_existing_values() {
    let kv = memory_kv::<i32>();
    assert!(kv.put_if_absent("k", 1).unwrap());
    assert!(!kv.put_if_absent("k", 2).unwrap());
    assert_eq!(kv.get("k").unwrap(), Some(1));
}

#[test]
fn multi_writer_append_log_uses_put_if_absent_and_rejects_unsupported_truncate() {
    let log = memory_multi_writer_append_log::<String>("mw");
    assert_eq!(log.append("a".to_owned()).unwrap().seq, 0);
    assert_eq!(log.append("b".to_owned()).unwrap().seq, 1);
    assert_eq!(
        log.read(AppendLogReadOptions::default())
            .unwrap()
            .into_iter()
            .map(|entry| (entry.seq, entry.value))
            .collect::<Vec<_>>(),
        vec![(0, "a".to_owned()), (1, "b".to_owned())]
    );
    assert!(log
        .truncate_after(0)
        .unwrap_err()
        .to_string()
        .contains("unsupported"));

    let plain = Rc::new(PlainTier::new(
        "plain",
        Ok(None),
        Rc::new(RefCell::new(Vec::new())),
    ));
    let unsupported = multi_writer_append_log_storage(plain, "plain", 3).unwrap();
    assert!(unsupported
        .append(1)
        .unwrap_err()
        .to_string()
        .contains("put-if-absent"));
    assert!(
        multi_writer_append_log_storage(Rc::new(memory_kv::<i32>()), "bad", 0)
            .unwrap_err()
            .to_string()
            .contains("positive")
    );
}

#[test]
fn multi_writer_append_log_refreshes_tail_after_retry_window() {
    struct StaleListKv {
        inner: graphrefly::MemoryKv<String>,
        stale_once: RefCell<bool>,
    }

    impl KvStorageTier<String> for StaleListKv {
        fn get(&self, key: &str) -> StorageResult<Option<String>> {
            self.inner.get(key)
        }

        fn set(&self, key: &str, value: String) -> StorageResult<()> {
            self.inner.set(key, value)
        }

        fn put_if_absent(&self, key: &str, value: String) -> StorageResult<bool> {
            self.inner.put_if_absent(key, value)
        }

        fn delete(&self, key: &str) -> StorageResult<()> {
            self.inner.delete(key)
        }

        fn list(&self, prefix: &str) -> StorageResult<Vec<String>> {
            if self.stale_once.replace(false) {
                Ok(Vec::new())
            } else {
                self.inner.list(prefix)
            }
        }
    }

    let inner = memory_kv::<String>();
    inner
        .set(&append_log_key("refresh", 0), "existing-0".to_owned())
        .unwrap();
    inner
        .set(&append_log_key("refresh", 1), "existing-1".to_owned())
        .unwrap();
    let kv = Rc::new(StaleListKv {
        inner,
        stale_once: RefCell::new(true),
    });
    let log = multi_writer_append_log_storage(kv, "refresh", 2).unwrap();

    let entry = log.append("new".to_owned()).unwrap();

    assert_eq!(entry.seq, 2);
    assert_eq!(entry.key, append_log_key("refresh", 2));
}

#[test]
fn multi_writer_append_log_keeps_committed_lower_slot_when_fresher_tail_appears() {
    struct StaleHigherTailKv {
        inner: graphrefly::MemoryKv<String>,
        stale_once: RefCell<bool>,
    }

    impl KvStorageTier<String> for StaleHigherTailKv {
        fn get(&self, key: &str) -> StorageResult<Option<String>> {
            self.inner.get(key)
        }

        fn set(&self, key: &str, value: String) -> StorageResult<()> {
            self.inner.set(key, value)
        }

        fn put_if_absent(&self, key: &str, value: String) -> StorageResult<bool> {
            self.inner.put_if_absent(key, value)
        }

        fn delete(&self, key: &str) -> StorageResult<()> {
            self.inner.delete(key)
        }

        fn list(&self, prefix: &str) -> StorageResult<Vec<String>> {
            if self.stale_once.replace(false) {
                Ok(Vec::new())
            } else {
                self.inner.list(prefix)
            }
        }
    }

    let inner = memory_kv::<String>();
    inner
        .set(&append_log_key("stale-higher", 10), "existing".to_owned())
        .unwrap();
    let log = multi_writer_append_log_storage(
        Rc::new(StaleHigherTailKv {
            inner: inner.clone(),
            stale_once: RefCell::new(true),
        }),
        "stale-higher",
        2,
    )
    .unwrap();

    let entry = log.append("new".to_owned()).unwrap();

    assert_eq!(entry.seq, 0);
    assert_eq!(
        inner.get(&append_log_key("stale-higher", 0)).unwrap(),
        Some("new".to_owned())
    );
    assert_eq!(
        log.read(AppendLogReadOptions::default())
            .unwrap()
            .into_iter()
            .map(|entry| entry.seq)
            .collect::<Vec<_>>(),
        vec![0, 10]
    );
}

#[test]
fn memory_kv_stores_cloned_values_and_lists_prefix_in_order() {
    let kv = memory_kv::<Vec<i32>>();
    let mut value = vec![2];
    kv.set("items/002", value.clone()).unwrap();
    kv.set("other/001", vec![9]).unwrap();
    value[0] = 7;
    kv.set("items/001", vec![1]).unwrap();

    assert_eq!(kv.get("items/002").unwrap(), Some(vec![2]));
    let mut read = kv.get("items/002").unwrap().unwrap();
    read[0] = 8;
    assert_eq!(kv.get("items/002").unwrap(), Some(vec![2]));
    assert_eq!(
        kv.list("items/").unwrap(),
        vec!["items/001".to_owned(), "items/002".to_owned()]
    );
}

#[test]
fn dict_kv_preloads_entries() {
    let kv = dict_kv([("a", 1), ("b", 2)]);

    assert_eq!(kv.get("a").unwrap(), Some(1));
    assert_eq!(kv.get("b").unwrap(), Some(2));
}

#[test]
fn memory_kv_versioned_present_and_absent_observations_are_opaque_per_key() {
    let kv = memory_kv::<i32>();

    let absent = kv.get_versioned("k").unwrap();
    let absent_generation = absent.generation().clone();
    assert!(matches!(absent, KvVersionedRead::Miss { .. }));
    assert!(kv.set_if_match("k", 1, &absent_generation).unwrap());
    assert!(!kv.set_if_match("k", 2, &absent_generation).unwrap());
    assert!(!kv.set_if_match("other", 9, &absent_generation).unwrap());
    assert_eq!(kv.get("k").unwrap(), Some(1));

    let present = kv.get_versioned("k").unwrap();
    let present_generation = present.generation().clone();
    assert!(matches!(present, KvVersionedRead::Hit { value: 1, .. }));
    kv.set("k", 3).unwrap();
    assert!(!kv.set_if_match("k", 4, &present_generation).unwrap());
    assert_eq!(kv.get("k").unwrap(), Some(3));

    let fresh = kv.get_versioned("k").unwrap();
    assert!(kv.set_if_match("k", 4, fresh.generation()).unwrap());
    assert_eq!(kv.get("k").unwrap(), Some(4));

    let pre_clear = kv.get_versioned("fresh").unwrap();
    kv.clear();
    assert!(!kv
        .set_if_match("fresh", 11, pre_clear.generation())
        .unwrap());
    assert_eq!(kv.get("fresh").unwrap(), None);

    let miss_before_cycle = kv.get_versioned("cycle").unwrap();
    kv.set("cycle", 5).unwrap();
    kv.delete("cycle").unwrap();
    assert!(!kv
        .set_if_match("cycle", 6, miss_before_cycle.generation())
        .unwrap());
}

#[test]
fn tiered_read_through_orders_tiers_and_promotes_first_hit() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let hot = PlainTier::new("hot", Ok(None), calls.clone());
    let warm = PlainTier::new("warm", Ok(Some(2)), calls.clone());
    let cold = PlainTier::new("cold", Ok(Some(1)), calls.clone());
    let mut opts = TieredReadThroughOptions::new("k", vec![&hot, &warm, &cold]);
    opts.tier_names = vec!["hot".to_owned(), "warm".to_owned(), "cold".to_owned()];

    let result = tiered_read_through(opts);

    assert_eq!(result.status, TieredReadThroughStatus::Hit);
    assert_eq!(result.value, Some(2));
    assert_eq!(result.hit_tier.unwrap().index, 1);
    assert_eq!(
        result
            .facts
            .iter()
            .map(|fact| fact.outcome.clone())
            .collect::<Vec<_>>(),
        vec![ReadThroughOutcome::Miss, ReadThroughOutcome::Hit]
    );
    assert_eq!(result.promotions.len(), 1);
    assert_eq!(result.promotions[0].tier.index, 0);
    assert!(result.promotions[0].ok);
    assert_eq!(*calls.borrow(), vec!["hot:k", "warm:k", "hot:set:k:2"]);
}

#[test]
fn tiered_read_through_can_disable_promotion_for_graph_layer_default() {
    let hot = memory_kv::<i32>();
    let cold = memory_kv::<i32>();
    cold.set("k", 42).unwrap();
    let mut opts = TieredReadThroughOptions::new("k", vec![&hot, &cold]);
    opts.promote_to = PromotionPolicy::Disabled;

    let result = tiered_read_through(opts);

    assert_eq!(result.status, TieredReadThroughStatus::Hit);
    assert_eq!(
        result
            .facts
            .iter()
            .map(|fact| (fact.outcome.clone(), fact.tier.index))
            .collect::<Vec<_>>(),
        vec![(ReadThroughOutcome::Miss, 0), (ReadThroughOutcome::Hit, 1)]
    );
    assert!(result.promotions.is_empty());
    assert_eq!(hot.get("k").unwrap(), None);
}

#[test]
fn tiered_read_through_loader_fallback_and_miss_reporting() {
    let miss_calls = Rc::new(RefCell::new(Vec::new()));
    let error_calls = Rc::new(RefCell::new(Vec::new()));
    let miss_tier = PlainTier::new("miss", Ok(None), Rc::new(RefCell::new(Vec::new())));
    let hit_tier = PlainTier::new("hit", Ok(None), Rc::new(RefCell::new(Vec::new())));
    let mut opts = TieredReadThroughOptions::new("user:1", vec![&miss_tier, &hit_tier]);
    opts.tier_names = vec!["miss".to_owned(), "hit".to_owned()];
    opts.load = Some(Box::new(|key| {
        if key == "user:1" {
            Ok(Some(7))
        } else {
            Ok(None)
        }
    }));
    opts.on_miss = Some(Box::new({
        let miss_calls = miss_calls.clone();
        move |ctx| miss_calls.borrow_mut().push(ctx.tier.index)
    }));
    opts.on_error = Some(Box::new({
        let error_calls = error_calls.clone();
        move |ctx| error_calls.borrow_mut().push(ctx.stage)
    }));

    let result = tiered_read_through(opts);
    assert_eq!(result.status, TieredReadThroughStatus::Hit);
    assert_eq!(result.value, Some(7));
    assert_eq!(result.hit_tier.unwrap().index, -1);
    assert_eq!(
        result
            .facts
            .iter()
            .map(|fact| fact.outcome.clone())
            .collect::<Vec<_>>(),
        vec![
            ReadThroughOutcome::Miss,
            ReadThroughOutcome::Miss,
            ReadThroughOutcome::Hit
        ]
    );
    assert_eq!(*miss_calls.borrow(), vec![0, 1]);
    assert!(error_calls.borrow().is_empty());

    let empty_result =
        tiered_read_through::<i32>(TieredReadThroughOptions::new("nowhere", Vec::new()));
    assert_eq!(empty_result.status, TieredReadThroughStatus::Miss);
    assert!(empty_result.facts.is_empty());

    let errors = Rc::new(RefCell::new(Vec::new()));
    let bad = PlainTier::new(
        "bad",
        Err(StorageError::backend("read failed")),
        Rc::new(RefCell::new(Vec::new())),
    );
    let good = PlainTier::new("good", Ok(Some(9)), Rc::new(RefCell::new(Vec::new())));
    let mut error_opts = TieredReadThroughOptions::new("k", vec![&bad, &good]);
    error_opts.on_error = Some(Box::new({
        let errors = errors.clone();
        move |ctx| {
            assert_eq!(ctx.stage, ReadThroughErrorStage::Lookup);
            errors.borrow_mut().push(ctx.error.to_string());
        }
    }));
    let recovered = tiered_read_through(error_opts);
    assert_eq!(recovered.status, TieredReadThroughStatus::Hit);
    assert_eq!(
        recovered
            .facts
            .iter()
            .map(|fact| fact.outcome.clone())
            .collect::<Vec<_>>(),
        vec![ReadThroughOutcome::Error, ReadThroughOutcome::Hit]
    );
    assert_eq!(*errors.borrow(), vec!["read failed".to_owned()]);
}

struct StaleVersionedTier {
    inner: graphrefly::MemoryKv<i32>,
}

impl KvStorageTier<i32> for StaleVersionedTier {
    fn get(&self, key: &str) -> StorageResult<Option<i32>> {
        self.inner.get(key)
    }

    fn set(&self, _key: &str, _value: i32) -> StorageResult<()> {
        Err(StorageError::backend("plain set must not run"))
    }

    fn delete(&self, key: &str) -> StorageResult<()> {
        self.inner.delete(key)
    }

    fn list(&self, prefix: &str) -> StorageResult<Vec<String>> {
        self.inner.list(prefix)
    }

    fn supports_versioned(&self) -> bool {
        true
    }

    fn get_versioned(&self, key: &str) -> StorageResult<KvVersionedRead<i32>> {
        let observed = self.inner.get_versioned(key)?;
        self.inner.set(key, 1)?;
        Ok(observed)
    }

    fn set_if_match(
        &self,
        key: &str,
        value: i32,
        generation: &graphrefly::KvGeneration,
    ) -> StorageResult<bool> {
        self.inner.set_if_match(key, value, generation)
    }
}

struct BrokenVersionedTier {
    calls: Rc<RefCell<Vec<&'static str>>>,
}

impl KvStorageTier<i32> for BrokenVersionedTier {
    fn get(&self, _key: &str) -> StorageResult<Option<i32>> {
        self.calls.borrow_mut().push("get");
        Ok(None)
    }

    fn set(&self, _key: &str, _value: i32) -> StorageResult<()> {
        self.calls.borrow_mut().push("set");
        Err(StorageError::backend("plain set must not run"))
    }

    fn delete(&self, _key: &str) -> StorageResult<()> {
        Ok(())
    }

    fn list(&self, _prefix: &str) -> StorageResult<Vec<String>> {
        Ok(Vec::new())
    }

    fn supports_versioned(&self) -> bool {
        true
    }

    fn get_versioned(&self, _key: &str) -> StorageResult<KvVersionedRead<i32>> {
        self.calls.borrow_mut().push("get_versioned");
        Err(StorageError::backend("versioned read failed"))
    }

    fn set_if_match(
        &self,
        _key: &str,
        _value: i32,
        _generation: &graphrefly::KvGeneration,
    ) -> StorageResult<bool> {
        self.calls.borrow_mut().push("set_if_match");
        Ok(true)
    }
}

#[test]
fn tiered_read_through_versioned_promotion_requires_generation() {
    let hot_inner = memory_kv::<i32>();
    let hot = StaleVersionedTier {
        inner: hot_inner.clone(),
    };
    let cold = memory_kv::<i32>();
    cold.set("k", 7).unwrap();
    let mut opts = TieredReadThroughOptions::new("k", vec![&hot, &cold]);
    opts.promote_to = PromotionPolicy::Indices(vec![0]);

    let result = tiered_read_through(opts);

    assert_eq!(result.status, TieredReadThroughStatus::Hit);
    assert_eq!(result.value, Some(7));
    assert_eq!(
        result
            .facts
            .iter()
            .map(|fact| (fact.outcome.clone(), fact.tier.index))
            .collect::<Vec<_>>(),
        vec![(ReadThroughOutcome::Miss, 0), (ReadThroughOutcome::Hit, 1)]
    );
    assert_eq!(result.promotions.len(), 1);
    assert_eq!(result.promotions[0].tier.index, 0);
    assert!(!result.promotions[0].ok);
    assert_eq!(hot_inner.get("k").unwrap(), Some(1));

    let calls = Rc::new(RefCell::new(Vec::new()));
    let broken = BrokenVersionedTier {
        calls: calls.clone(),
    };
    let cold = memory_kv::<i32>();
    cold.set("k", 9).unwrap();
    let mut opts = TieredReadThroughOptions::new("k", vec![&broken, &cold]);
    opts.promote_to = PromotionPolicy::Indices(vec![0]);
    let errors = Rc::new(RefCell::new(Vec::new()));
    opts.on_error = Some(Box::new({
        let errors = errors.clone();
        move |ctx| errors.borrow_mut().push((ctx.stage, ctx.error.to_string()))
    }));

    let result = tiered_read_through(opts);

    assert_eq!(result.status, TieredReadThroughStatus::Hit);
    assert_eq!(result.value, Some(9));
    assert_eq!(
        result
            .facts
            .iter()
            .map(|fact| (fact.outcome.clone(), fact.tier.index))
            .collect::<Vec<_>>(),
        vec![(ReadThroughOutcome::Error, 0), (ReadThroughOutcome::Hit, 1)]
    );
    assert_eq!(result.promotions.len(), 1);
    assert!(!result.promotions[0].ok);
    assert!(result.promotions[0]
        .error
        .as_ref()
        .unwrap()
        .to_string()
        .contains("not observed with a generation"));
    assert_eq!(*calls.borrow(), vec!["get_versioned"]);
    assert_eq!(
        *errors.borrow(),
        vec![
            (
                ReadThroughErrorStage::Lookup,
                "versioned read failed".to_owned()
            ),
            (
                ReadThroughErrorStage::Promotion,
                "tiered_read_through: versioned promotion target was not observed with a generation"
                    .to_owned()
            )
        ]
    );
}

#[test]
fn reactive_cascading_cache_exposes_visible_topology_and_default_no_promotion() {
    let g = graph();
    let request = g.state_empty_opts::<String>(graphrefly::GraphNodeOpts::named("request"));
    let hot = memory_kv::<i32>();
    let cold = memory_kv::<i32>();
    cold.set("user:1", 7).unwrap();
    let cache = reactive_cascading_cache(
        &g,
        ReactiveCascadingCacheOptions {
            request: request.clone(),
            tiers: vec![Rc::new(hot.clone()), Rc::new(cold.clone())],
            tier_names: vec!["hot".to_owned(), "cold".to_owned()],
            name: Some("cache".to_owned()),
            ..ReactiveCascadingCacheOptions::new(request.clone(), Vec::new())
        },
    );
    let (statuses, _status_unsub) = data_of(&cache.status);
    let (values, _value_unsub) = data_of(&cache.value);
    let (events, _events_unsub) = data_of(&cache.events);

    assert_eq!(*statuses.borrow(), vec![CascadingCacheStatus::Idle]);
    request.down(vec![Message::Data(Rc::new("user:1".to_owned()))]);

    let snap = g.describe();
    let mut ids = snap.nodes.iter().map(|n| n.id.as_str()).collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec!["cache.events", "cache.status", "cache.value", "request"]
    );
    assert!(snap.edges.contains(&graphrefly::DescribeEdge {
        from: "request".to_owned(),
        to: "cache.events".to_owned(),
    }));
    assert!(snap.edges.contains(&graphrefly::DescribeEdge {
        from: "cache.events".to_owned(),
        to: "cache.status".to_owned(),
    }));
    assert!(snap.edges.contains(&graphrefly::DescribeEdge {
        from: "cache.events".to_owned(),
        to: "cache.value".to_owned(),
    }));

    assert_eq!(*values.borrow(), vec![7]);
    assert_eq!(cache.value.cache(), Some(7));
    assert_eq!(hot.get("user:1").unwrap(), None);
    assert_eq!(
        statuses.borrow().last(),
        Some(&CascadingCacheStatus::Hit {
            key: "user:1".to_owned(),
            request_seq: 1,
            tier: Some(graphrefly::ReadThroughLookupTier {
                index: 1,
                name: Some("cold".to_owned()),
            }),
        })
    );
    assert_eq!(
        events.borrow().iter().map(event_kind).collect::<Vec<_>>(),
        vec!["request", "lookup", "lookup", "fill"]
    );
}

#[test]
fn reactive_cascading_cache_promotes_only_when_explicitly_enabled() {
    let g = graph();
    let request = g.state_empty::<String>();
    let hot = memory_kv::<i32>();
    let cold = memory_kv::<i32>();
    cold.set("k", 42).unwrap();
    let mut opts = ReactiveCascadingCacheOptions::new(
        request.clone(),
        vec![Rc::new(hot.clone()), Rc::new(cold.clone())],
    );
    opts.tier_names = vec!["hot".to_owned(), "cold".to_owned()];
    opts.promote_to = PromotionPolicy::Indices(vec![0]);
    opts.name = Some("cache".to_owned());
    let cache = reactive_cascading_cache(&g, opts);
    let (_statuses, _status_unsub) = data_of(&cache.status);
    let (_values, _value_unsub) = data_of(&cache.value);
    let (events, _events_unsub) = data_of(&cache.events);

    request.down(vec![Message::Data(Rc::new("k".to_owned()))]);

    assert_eq!(cache.value.cache(), Some(42));
    assert_eq!(hot.get("k").unwrap(), Some(42));
    assert_eq!(
        events.borrow().iter().map(event_kind).collect::<Vec<_>>(),
        vec!["request", "lookup", "lookup", "promotion", "fill"]
    );
}

#[test]
fn reactive_cascading_cache_uses_versioned_promotion_without_overwriting_stale_tier() {
    let g = graph();
    let request = g.state_empty::<String>();
    let hot_inner = memory_kv::<i32>();
    let hot = StaleVersionedTier {
        inner: hot_inner.clone(),
    };
    let cold = memory_kv::<i32>();
    cold.set("k", 7).unwrap();
    let mut opts = ReactiveCascadingCacheOptions::new(
        request.clone(),
        vec![Rc::new(hot), Rc::new(cold.clone())],
    );
    opts.promote_to = PromotionPolicy::Indices(vec![0]);
    opts.name = Some("cache".to_owned());
    let cache = reactive_cascading_cache(&g, opts);
    let (_statuses, _status_unsub) = data_of(&cache.status);
    let (_values, _value_unsub) = data_of(&cache.value);
    let (events, _events_unsub) = data_of(&cache.events);

    request.down(vec![Message::Data(Rc::new("k".to_owned()))]);

    assert_eq!(cache.value.cache(), Some(7));
    assert_eq!(hot_inner.get("k").unwrap(), Some(1));
    assert!(events.borrow().iter().any(|event| matches!(
        event,
        CascadingCacheEvent::Promotion {
            tier,
            ok: false,
            ..
        } if tier.index == 0
    )));
}

#[test]
fn reactive_cascading_cache_loader_fallback_policy_and_invalidation_are_graph_events() {
    let g = graph();
    let request = g.state_empty::<String>();
    let policy = g.state_empty::<CascadingCachePolicy>();
    let invalidate = g.state_empty::<String>();
    let hot = memory_kv::<i32>();
    let cold = memory_kv::<i32>();
    let load_calls = Rc::new(RefCell::new(Vec::new()));
    let calls = load_calls.clone();
    let mut opts = ReactiveCascadingCacheOptions::new(
        request.clone(),
        vec![Rc::new(hot.clone()), Rc::new(cold.clone())],
    );
    opts.policy = Some(policy.clone());
    opts.invalidate = Some(invalidate.clone());
    opts.load = Some(Rc::new(move |key| {
        calls.borrow_mut().push(key.to_owned());
        Ok(if key == "remote" { Some(9) } else { None })
    }));
    opts.name = Some("cache".to_owned());
    let cache = reactive_cascading_cache(&g, opts);
    let (statuses, _status_unsub) = data_of(&cache.status);
    let (value_kinds, _value_kind_unsub) = message_kinds(&cache.value);
    let (events, _events_unsub) = data_of(&cache.events);

    request.down(vec![Message::Data(Rc::new("remote".to_owned()))]);

    assert_eq!(cache.value.cache(), Some(9));
    assert_eq!(hot.get("remote").unwrap(), None);
    assert_eq!(
        statuses.borrow().last(),
        Some(&CascadingCacheStatus::Hit {
            key: "remote".to_owned(),
            request_seq: 1,
            tier: Some(graphrefly::ReadThroughLookupTier {
                index: -1,
                name: Some("load".to_owned()),
            }),
        })
    );

    cold.set("remote", 3).unwrap();
    policy.down(vec![Message::Data(Rc::new(CascadingCachePolicy {
        promote_to: Some(PromotionPolicy::Indices(vec![0])),
    }))]);
    assert_eq!(hot.get("remote").unwrap(), Some(3));

    invalidate.down(vec![Message::Data(Rc::new("remote".to_owned()))]);

    assert_eq!(
        events
            .borrow()
            .iter()
            .filter(|event| matches!(event, CascadingCacheEvent::Invalidate { .. }))
            .count(),
        1
    );
    assert!(value_kinds.borrow().contains(&"INVALIDATE"));
    assert_eq!(*load_calls.borrow(), vec!["remote".to_owned()]);
}

#[test]
fn reactive_cascading_cache_load_panic_becomes_error_events_not_terminal_error() {
    let g = graph();
    let request = g.state_empty::<String>();
    let mut opts = ReactiveCascadingCacheOptions::new(request.clone(), Vec::new());
    opts.load = Some(Rc::new(|_| -> StorageResult<Option<i32>> {
        panic!("loader exploded")
    }));
    opts.name = Some("cache".to_owned());
    let cache = reactive_cascading_cache(&g, opts);
    let (statuses, _status_unsub) = data_of(&cache.status);
    let (events, _events_unsub) = data_of(&cache.events);

    request.down(vec![Message::Data(Rc::new("k".to_owned()))]);

    assert_eq!(cache.events.status(), graphrefly::Status::Settled);
    assert_eq!(
        statuses.borrow().last(),
        Some(&CascadingCacheStatus::Error {
            key: "k".to_owned(),
            request_seq: 1,
            error: StorageError::backend("loader exploded"),
        })
    );
    assert_eq!(
        events.borrow().iter().map(event_kind).collect::<Vec<_>>(),
        vec!["request", "error", "fill"]
    );
}
