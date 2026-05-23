//! Integration tests for graph-level storage integration (M4.E2).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use graphrefly_core::{
    BindingBoundary, DepBatch, FnId, FnResult, HandleId, NodeId, OwnedCore, NO_HANDLE,
};
use graphrefly_graph::{Graph, GraphPersistSnapshot, NodeSlice, NodeSnapshotStatus};
use graphrefly_storage::{
    attach_snapshot_storage, decompose_diff_to_frames, diff_snapshots, kv_storage, memory_backend,
    restore_snapshot, snapshot_storage, AttachOptions, AttachTierPair, BaseStorageTier,
    GraphCheckpointRecord, KvStorageOptions, KvStorageTier, RestoreOptions, SnapshotStorageOptions,
    SnapshotStorageTier, StorageBackend, TornWritePolicy, SNAPSHOT_VERSION,
};
use graphrefly_structures::Lifecycle;
use indexmap::IndexMap;
use serde_json::Value;

// ---------------------------------------------------------------------------
// Test binding
// ---------------------------------------------------------------------------

struct TestBinding {
    next_handle: AtomicU64,
    values: Mutex<HashMap<HandleId, Value>>,
}

impl TestBinding {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            next_handle: AtomicU64::new(1),
            values: Mutex::new(HashMap::new()),
        })
    }

    fn intern(&self, value: Value) -> HandleId {
        let id = self.next_handle.fetch_add(1, Ordering::SeqCst);
        let h = HandleId::new(id);
        self.values.lock().unwrap().insert(h, value);
        h
    }

    fn deref(&self, handle: HandleId) -> Value {
        self.values
            .lock()
            .unwrap()
            .get(&handle)
            .cloned()
            .unwrap_or(Value::Null)
    }
}

impl BindingBoundary for TestBinding {
    fn invoke_fn(&self, _node_id: NodeId, _fn_id: FnId, dep_data: &[DepBatch]) -> FnResult {
        dep_data
            .first()
            .map_or(FnResult::Noop { tracked: None }, |db| FnResult::Data {
                handle: db.latest(),
                tracked: None,
            })
    }

    fn custom_equals(&self, _: FnId, a: HandleId, b: HandleId) -> bool {
        a == b
    }

    fn release_handle(&self, _h: HandleId) {}

    fn serialize_handle(&self, handle: HandleId) -> Option<Value> {
        self.values.lock().unwrap().get(&handle).cloned()
    }

    fn deserialize_value(&self, value: Value) -> HandleId {
        self.intern(value)
    }
}

/// D246: the embedder owns the `Core` via [`OwnedCore`]; `Graph` is
/// Core-free (`Graph::new(name)`). Every Core-touching `g.*` call
/// threads `rt.core()`.
fn make_graph(name: &str) -> (OwnedCore, Graph, Arc<TestBinding>) {
    let b = TestBinding::new();
    let rt = OwnedCore::new(b.clone() as Arc<dyn BindingBoundary>);
    let g = Graph::new(name);
    (rt, g, b)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn empty_snapshot(name: &str) -> GraphPersistSnapshot {
    GraphPersistSnapshot {
        name: name.to_owned(),
        nodes: IndexMap::new(),
        subgraphs: IndexMap::new(),
    }
}

fn state_slice(value: Option<Value>) -> NodeSlice {
    NodeSlice {
        node_type: "state".to_owned(),
        value,
        status: NodeSnapshotStatus::Live,
        deps: Vec::new(),
    }
}

type SnapTier = graphrefly_storage::SnapshotStorage<
    graphrefly_storage::MemoryBackend,
    GraphCheckpointRecord,
    graphrefly_storage::JsonCodec,
>;

type WalTier = graphrefly_storage::KvStorage<
    graphrefly_storage::MemoryBackend,
    graphrefly_storage::WALFrame<Value>,
    graphrefly_storage::JsonCodec,
>;

fn make_snap_tier(backend: Arc<graphrefly_storage::MemoryBackend>, name: &str) -> SnapTier {
    snapshot_storage(
        backend,
        SnapshotStorageOptions {
            name: Some(name.to_owned()),
            ..Default::default()
        },
    )
}

fn make_snap_tier_with_compact(
    backend: Arc<graphrefly_storage::MemoryBackend>,
    name: &str,
    compact_every: u32,
) -> SnapTier {
    snapshot_storage(
        backend,
        SnapshotStorageOptions {
            name: Some(name.to_owned()),
            compact_every: Some(compact_every),
            ..Default::default()
        },
    )
}

fn make_wal_tier(backend: Arc<graphrefly_storage::MemoryBackend>, name: &str) -> WalTier {
    kv_storage(
        backend,
        KvStorageOptions {
            name: Some(name.to_owned()),
            ..Default::default()
        },
    )
}

// ---------------------------------------------------------------------------
// diff_snapshots tests
// ---------------------------------------------------------------------------

#[test]
fn diff_empty_snapshots() {
    let a = empty_snapshot("g");
    let b = empty_snapshot("g");
    let diff = diff_snapshots(&a, &b);
    assert!(diff.is_empty());
}

#[test]
fn diff_node_added() {
    let a = empty_snapshot("g");
    let mut b = empty_snapshot("g");
    b.nodes
        .insert("x".to_owned(), state_slice(Some(Value::from(1))));
    let diff = diff_snapshots(&a, &b);
    assert_eq!(diff.nodes_added, vec!["x"]);
    assert_eq!(diff.nodes_added_slices.len(), 1);
    assert!(diff.nodes_removed.is_empty());
}

#[test]
fn diff_node_removed() {
    let mut a = empty_snapshot("g");
    a.nodes
        .insert("x".to_owned(), state_slice(Some(Value::from(1))));
    let b = empty_snapshot("g");
    let diff = diff_snapshots(&a, &b);
    assert_eq!(diff.nodes_removed, vec!["x"]);
}

#[test]
fn diff_value_changed() {
    let mut a = empty_snapshot("g");
    a.nodes
        .insert("x".to_owned(), state_slice(Some(Value::from(1))));
    let mut b = empty_snapshot("g");
    b.nodes
        .insert("x".to_owned(), state_slice(Some(Value::from(2))));
    let diff = diff_snapshots(&a, &b);
    assert_eq!(diff.value_changes.len(), 1);
    assert_eq!(diff.value_changes[0].to, Some(Value::from(2)));
}

#[test]
fn diff_value_to_sentinel() {
    let mut a = empty_snapshot("g");
    a.nodes
        .insert("x".to_owned(), state_slice(Some(Value::from(1))));
    let mut b = empty_snapshot("g");
    b.nodes.insert("x".to_owned(), state_slice(None));
    let diff = diff_snapshots(&a, &b);
    assert_eq!(diff.value_changes.len(), 1);
    assert_eq!(diff.value_changes[0].to, None);
}

#[test]
fn diff_subgraph_added_removed() {
    let mut a = empty_snapshot("g");
    a.subgraphs.insert("c1".to_owned(), empty_snapshot("c1"));
    let mut b = empty_snapshot("g");
    b.subgraphs.insert("c2".to_owned(), empty_snapshot("c2"));
    let diff = diff_snapshots(&a, &b);
    assert_eq!(diff.subgraphs_added, vec!["c2"]);
    assert_eq!(diff.subgraphs_removed, vec!["c1"]);
}

// ---------------------------------------------------------------------------
// decompose_diff_to_frames tests
// ---------------------------------------------------------------------------

#[test]
fn decompose_node_add_produces_spec_frame() {
    let mut diff = diff_snapshots(&empty_snapshot("g"), &empty_snapshot("g"));
    diff.nodes_added.push("x".to_owned());
    diff.nodes_added_slices
        .push(state_slice(Some(Value::from(42))));

    let (frames, next_seq) = decompose_diff_to_frames(&diff, 1000, 0).unwrap();
    assert_eq!(frames.len(), 1);
    assert_eq!(next_seq, 1);
    assert_eq!(frames[0].lifecycle, Lifecycle::Spec);
    assert_eq!(frames[0].path, "x");
    assert_eq!(frames[0].frame_seq, 1);
    assert!(!frames[0].checksum.is_empty());
    let kind = frames[0]
        .change
        .change
        .get("kind")
        .unwrap()
        .as_str()
        .unwrap();
    assert_eq!(kind, "graph.add");
}

#[test]
fn decompose_value_change_produces_data_frame() {
    let mut a = empty_snapshot("g");
    a.nodes
        .insert("x".to_owned(), state_slice(Some(Value::from(1))));
    let mut b = empty_snapshot("g");
    b.nodes
        .insert("x".to_owned(), state_slice(Some(Value::from(2))));
    let diff = diff_snapshots(&a, &b);
    let (frames, _) = decompose_diff_to_frames(&diff, 2000, 5).unwrap();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].lifecycle, Lifecycle::Data);
    assert_eq!(frames[0].frame_seq, 6);
    let kind = frames[0]
        .change
        .change
        .get("kind")
        .unwrap()
        .as_str()
        .unwrap();
    assert_eq!(kind, "node.set");
}

#[test]
fn decompose_checksum_verifies() {
    let diff = {
        let a = empty_snapshot("g");
        let mut b = empty_snapshot("g");
        b.nodes
            .insert("n".to_owned(), state_slice(Some(Value::from("hello"))));
        diff_snapshots(&a, &b)
    };
    let (frames, _) = decompose_diff_to_frames(&diff, 5000, 0).unwrap();
    let ok = graphrefly_storage::verify_wal_frame_checksum(&frames[0]).unwrap();
    assert!(ok);
}

// ---------------------------------------------------------------------------
// attach_snapshot_storage tests
// ---------------------------------------------------------------------------

#[test]
fn attach_full_baseline_on_first_flush() {
    let (rt, g, b) = make_graph("test");
    let h1 = b.intern(Value::from(10));
    g.state(rt.core(), "counter", Some(h1)).unwrap();

    let backend = memory_backend();
    let snap_tier = make_snap_tier(backend.clone(), "test");
    let snap_tier_box: Box<dyn SnapshotStorageTier<GraphCheckpointRecord>> = Box::new(snap_tier);

    let handle = attach_snapshot_storage(
        rt.core(),
        &g,
        vec![AttachTierPair {
            snapshot: snap_tier_box,
            wal: None,
        }],
        AttachOptions::default(),
    );

    // Emit a value to trigger flush.
    let h2 = b.intern(Value::from(20));
    g.set(rt.core(), "counter", h2);
    std::thread::sleep(std::time::Duration::from_millis(20));

    // Verify baseline was written.
    let stored = backend.read("test").unwrap();
    assert!(stored.is_some(), "baseline should be written");
    let record: GraphCheckpointRecord = serde_json::from_slice(&stored.unwrap()).unwrap();
    assert_eq!(record.name, "test");
    assert_eq!(record.mode, "full");
    assert_eq!(record.format_version, SNAPSHOT_VERSION);

    handle.detach(rt.core());
}

#[test]
fn attach_wal_delta_after_baseline() {
    let (rt, g, b) = make_graph("wal-test");
    let h1 = b.intern(Value::from(1));
    g.state(rt.core(), "x", Some(h1)).unwrap();

    let snap_backend = memory_backend();
    let snap_tier = make_snap_tier_with_compact(snap_backend.clone(), "wal-test", 10);
    let snap_box: Box<dyn SnapshotStorageTier<GraphCheckpointRecord>> = Box::new(snap_tier);

    let wal_backend = memory_backend();
    let wal_tier = make_wal_tier(wal_backend.clone(), "wal");
    let wal_box: Box<dyn KvStorageTier<graphrefly_storage::WALFrame<Value>>> = Box::new(wal_tier);

    let handle = attach_snapshot_storage(
        rt.core(),
        &g,
        vec![AttachTierPair {
            snapshot: snap_box,
            wal: Some(wal_box),
        }],
        AttachOptions::default(),
    );

    // First emit → full baseline.
    let h2 = b.intern(Value::from(2));
    g.set(rt.core(), "x", h2);
    std::thread::sleep(std::time::Duration::from_millis(20));
    assert!(snap_backend.read("wal-test").unwrap().is_some());

    // Second emit → WAL delta.
    let h3 = b.intern(Value::from(3));
    g.set(rt.core(), "x", h3);
    std::thread::sleep(std::time::Duration::from_millis(20));

    let wal_keys = wal_backend.list("wal-test/wal").unwrap();
    assert!(!wal_keys.is_empty(), "WAL frames should exist after delta");

    handle.detach(rt.core());
}

/// D171: when the snapshot tier is configured with `debounce_ms > 0`,
/// `attach_snapshot_storage`'s observe sink writes via `save()` (which
/// buffers internally) but does NOT call `flush()`. The caller drains
/// buffered writes via `StorageHandle::flush_all()`.
#[test]
fn attach_respects_snapshot_debounce_ms_buffering() {
    let (rt, g, b) = make_graph("debounce-test");
    let h1 = b.intern(Value::from(10));
    g.state(rt.core(), "counter", Some(h1)).unwrap();

    let backend = memory_backend();
    let snap_tier = snapshot_storage::<_, GraphCheckpointRecord, _>(
        backend.clone(),
        SnapshotStorageOptions {
            name: Some("debounce-test".to_owned()),
            debounce_ms: Some(1000),
            ..Default::default()
        },
    );
    let snap_box: Box<dyn SnapshotStorageTier<GraphCheckpointRecord>> = Box::new(snap_tier);

    let handle = attach_snapshot_storage(
        rt.core(),
        &g,
        vec![AttachTierPair {
            snapshot: snap_box,
            wal: None,
        }],
        AttachOptions::default(),
    );

    // Emit a value — observe fires and tier.save() buffers. tier.flush()
    // is suppressed under debounce_ms > 0, so the backend stays empty.
    let h2 = b.intern(Value::from(20));
    g.set(rt.core(), "counter", h2);
    std::thread::sleep(std::time::Duration::from_millis(20));

    let stored = backend.read("debounce-test").unwrap();
    assert!(
        stored.is_none() || stored.as_ref().is_some_and(std::vec::Vec::is_empty),
        "with debounce_ms > 0, observe-driven save() must NOT force-flush; \
         backend should be empty until flush_all() is called (got {stored:?})",
    );

    // Drain via StorageHandle::flush_all() — backend now has the buffered snapshot.
    handle
        .flush_all()
        .expect("flush_all should commit pending writes");

    let stored = backend.read("debounce-test").unwrap();
    assert!(
        stored.is_some(),
        "after flush_all, the buffered snapshot must be written"
    );
    let record: GraphCheckpointRecord = serde_json::from_slice(&stored.unwrap()).unwrap();
    assert_eq!(record.name, "debounce-test");

    handle.detach(rt.core());
}

#[test]
fn attach_dispose_stops_persistence() {
    let (rt, g, b) = make_graph("dispose");
    let h1 = b.intern(Value::from(1));
    g.state(rt.core(), "x", Some(h1)).unwrap();

    let snap_backend = memory_backend();
    let snap_tier = make_snap_tier(snap_backend.clone(), "dispose");
    let snap_box: Box<dyn SnapshotStorageTier<GraphCheckpointRecord>> = Box::new(snap_tier);

    let handle = attach_snapshot_storage(
        rt.core(),
        &g,
        vec![AttachTierPair {
            snapshot: snap_box,
            wal: None,
        }],
        AttachOptions::default(),
    );

    let h2 = b.intern(Value::from(2));
    g.set(rt.core(), "x", h2);
    std::thread::sleep(std::time::Duration::from_millis(20));
    assert!(snap_backend.read("dispose").unwrap().is_some());

    // Dispose (Core-free flag flip — late persistence stops).
    handle.dispose();
    snap_backend.write("dispose", &[]).unwrap(); // clear

    let h3 = b.intern(Value::from(3));
    g.set(rt.core(), "x", h3);
    std::thread::sleep(std::time::Duration::from_millis(20));

    let stored = snap_backend.read("dispose").unwrap().unwrap();
    assert!(stored.is_empty(), "after dispose, no new snapshot");

    handle.detach(rt.core());
}

// ---------------------------------------------------------------------------
// restore_snapshot tests
// ---------------------------------------------------------------------------

#[test]
fn restore_baseline_only() {
    let (rt, g, b) = make_graph("r");
    let h = b.intern(Value::from(42));
    g.state(rt.core(), "x", Some(h)).unwrap();

    let snap_backend = memory_backend();
    let snap_tier = make_snap_tier(snap_backend, "r");
    snap_tier
        .save(GraphCheckpointRecord {
            name: "r".to_owned(),
            mode: "full".to_owned(),
            snapshot: g.snapshot(rt.core()),
            seq: 0,
            timestamp_ns: 1000,
            format_version: SNAPSHOT_VERSION,
        })
        .unwrap();
    snap_tier.flush().unwrap();

    let wal_backend = memory_backend();
    let wal_tier = make_wal_tier(wal_backend, "wal");

    let (rt2, g2, b2) = make_graph("r");
    g2.state(rt2.core(), "x", None).unwrap();

    let result = restore_snapshot(
        rt2.core(),
        &g2,
        &RestoreOptions {
            snapshot_tier: &snap_tier,
            wal_tier: &wal_tier,
            target_seq: None,
            on_torn_write: None,
        },
    )
    .unwrap();

    assert_eq!(result.replayed_frames, 0);
    let cache = g2.get(rt2.core(), "x");
    assert_ne!(cache, NO_HANDLE);
    assert_eq!(b2.deref(cache), Value::from(42));
}

#[test]
fn restore_missing_baseline_errors() {
    let (rt, g, _) = make_graph("e");
    g.state(rt.core(), "x", None).unwrap();

    let snap_tier = make_snap_tier(memory_backend(), "e");
    let wal_tier = make_wal_tier(memory_backend(), "wal");

    let err = restore_snapshot(
        rt.core(),
        &g,
        &RestoreOptions {
            snapshot_tier: &snap_tier,
            wal_tier: &wal_tier,
            target_seq: None,
            on_torn_write: None,
        },
    );
    assert!(err.is_err());
}

#[test]
fn restore_with_wal_replay() {
    let (rt, g, b) = make_graph("replay");
    let h = b.intern(Value::from(10));
    g.state(rt.core(), "x", Some(h)).unwrap();

    let snap_backend = memory_backend();
    let snap_tier = make_snap_tier(snap_backend, "replay");
    let baseline = g.snapshot(rt.core());
    snap_tier
        .save(GraphCheckpointRecord {
            name: "replay".to_owned(),
            mode: "full".to_owned(),
            snapshot: baseline.clone(),
            seq: 0,
            timestamp_ns: 1000,
            format_version: SNAPSHOT_VERSION,
        })
        .unwrap();
    snap_tier.flush().unwrap();

    // Simulate WAL: x changed 10 → 20.
    let mut after = baseline;
    after.nodes.get_mut("x").unwrap().value = Some(Value::from(20));
    let mut before = empty_snapshot("replay");
    before
        .nodes
        .insert("x".to_owned(), state_slice(Some(Value::from(10))));
    let diff = diff_snapshots(&before, &after);
    let (frames, _) = decompose_diff_to_frames(&diff, 2000, 0).unwrap();

    let wal_backend = memory_backend();
    let wal_tier = make_wal_tier(wal_backend, "wal");
    for frame in &frames {
        let key = graphrefly_storage::wal_frame_key(
            &graphrefly_storage::graph_wal_prefix("replay"),
            frame.frame_seq,
        );
        wal_tier.save(&key, frame.clone()).unwrap();
    }
    wal_tier.flush().unwrap();

    let (rt2, g2, b2) = make_graph("replay");
    g2.state(rt2.core(), "x", None).unwrap();

    let result = restore_snapshot(
        rt2.core(),
        &g2,
        &RestoreOptions {
            snapshot_tier: &snap_tier,
            wal_tier: &wal_tier,
            target_seq: None,
            on_torn_write: None,
        },
    )
    .unwrap();

    assert_eq!(result.replayed_frames, 1);
    assert_eq!(result.phases[0].lifecycle, Lifecycle::Data);
    assert_eq!(b2.deref(g2.get(rt2.core(), "x")), Value::from(20));
}

#[test]
fn restore_torn_tail_skipped() {
    let (rt, g, b) = make_graph("torn");
    let h = b.intern(Value::from(1));
    g.state(rt.core(), "x", Some(h)).unwrap();

    let snap_tier = make_snap_tier(memory_backend(), "torn");
    snap_tier
        .save(GraphCheckpointRecord {
            name: "torn".to_owned(),
            mode: "full".to_owned(),
            snapshot: g.snapshot(rt.core()),
            seq: 0,
            timestamp_ns: 1000,
            format_version: SNAPSHOT_VERSION,
        })
        .unwrap();
    snap_tier.flush().unwrap();

    let wal_tier = make_wal_tier(memory_backend(), "wal");
    let bad = graphrefly_storage::WALFrame {
        t: graphrefly_storage::WalTag,
        lifecycle: Lifecycle::Data,
        path: "x".to_owned(),
        change: graphrefly_structures::BaseChange {
            structure: "graph.value".to_owned(),
            version: graphrefly_structures::Version::Counter(1),
            t_ns: 2000,
            seq: None,
            lifecycle: Lifecycle::Data,
            change: serde_json::json!({"kind": "node.set", "path": "x", "value": 99}),
        },
        frame_seq: 1,
        frame_t_ns: 2000,
        checksum: "bad".to_owned(),
        format_version: 1,
    };
    wal_tier
        .save(
            &graphrefly_storage::wal_frame_key(&graphrefly_storage::graph_wal_prefix("torn"), 1),
            bad,
        )
        .unwrap();
    wal_tier.flush().unwrap();

    let (rt2, g2, _) = make_graph("torn");
    g2.state(rt2.core(), "x", None).unwrap();

    let result = restore_snapshot(
        rt2.core(),
        &g2,
        &RestoreOptions {
            snapshot_tier: &snap_tier,
            wal_tier: &wal_tier,
            target_seq: None,
            on_torn_write: None,
        },
    )
    .unwrap();

    assert_eq!(result.skipped_frames, 1);
    assert_eq!(result.replayed_frames, 0);
}

#[test]
fn restore_torn_mid_aborts() {
    let (rt, g, b) = make_graph("torn-mid");
    let h = b.intern(Value::from(1));
    g.state(rt.core(), "x", Some(h)).unwrap();

    let snap_tier = make_snap_tier(memory_backend(), "torn-mid");
    snap_tier
        .save(GraphCheckpointRecord {
            name: "torn-mid".to_owned(),
            mode: "full".to_owned(),
            snapshot: g.snapshot(rt.core()),
            seq: 0,
            timestamp_ns: 1000,
            format_version: SNAPSHOT_VERSION,
        })
        .unwrap();
    snap_tier.flush().unwrap();

    let wal_tier = make_wal_tier(memory_backend(), "wal");

    // Frame 1: bad checksum (mid-stream because frame 2 follows).
    let bad = graphrefly_storage::WALFrame {
        t: graphrefly_storage::WalTag,
        lifecycle: Lifecycle::Data,
        path: "x".to_owned(),
        change: graphrefly_structures::BaseChange {
            structure: "graph.value".to_owned(),
            version: graphrefly_structures::Version::Counter(1),
            t_ns: 2000,
            seq: None,
            lifecycle: Lifecycle::Data,
            change: serde_json::json!({"kind": "node.set", "path": "x", "value": 50}),
        },
        frame_seq: 1,
        frame_t_ns: 2000,
        checksum: "corrupt".to_owned(),
        format_version: 1,
    };
    wal_tier
        .save(
            &graphrefly_storage::wal_frame_key(
                &graphrefly_storage::graph_wal_prefix("torn-mid"),
                1,
            ),
            bad,
        )
        .unwrap();

    // Frame 2: valid.
    let diff = {
        let a = empty_snapshot("torn-mid");
        let mut b = empty_snapshot("torn-mid");
        b.nodes
            .insert("x".to_owned(), state_slice(Some(Value::from(99))));
        diff_snapshots(&a, &b)
    };
    let (good_frames, _) = decompose_diff_to_frames(&diff, 3000, 1).unwrap();
    wal_tier
        .save(
            &graphrefly_storage::wal_frame_key(
                &graphrefly_storage::graph_wal_prefix("torn-mid"),
                2,
            ),
            good_frames[0].clone(),
        )
        .unwrap();
    wal_tier.flush().unwrap();

    let (rt2, g2, _) = make_graph("torn-mid");
    g2.state(rt2.core(), "x", None).unwrap();

    let err = restore_snapshot(
        rt2.core(),
        &g2,
        &RestoreOptions {
            snapshot_tier: &snap_tier,
            wal_tier: &wal_tier,
            target_seq: None,
            on_torn_write: None,
        },
    );
    assert!(err.is_err());
    assert!(err.unwrap_err().to_string().contains("torn write"));
}

#[test]
fn restore_target_seq_limits_replay() {
    let (rt, g, b) = make_graph("target");
    let h = b.intern(Value::from(1));
    g.state(rt.core(), "x", Some(h)).unwrap();

    let snap_tier = make_snap_tier(memory_backend(), "target");
    snap_tier
        .save(GraphCheckpointRecord {
            name: "target".to_owned(),
            mode: "full".to_owned(),
            snapshot: g.snapshot(rt.core()),
            seq: 0,
            timestamp_ns: 1000,
            format_version: SNAPSHOT_VERSION,
        })
        .unwrap();
    snap_tier.flush().unwrap();

    let wal_tier = make_wal_tier(memory_backend(), "wal");
    for seq in 1..=3u64 {
        let mut a = empty_snapshot("target");
        a.nodes
            .insert("x".to_owned(), state_slice(Some(Value::from(seq - 1))));
        let mut bsnap = empty_snapshot("target");
        bsnap
            .nodes
            .insert("x".to_owned(), state_slice(Some(Value::from(seq * 10))));
        let diff = diff_snapshots(&a, &bsnap);
        let (frames, _) = decompose_diff_to_frames(&diff, 2000, seq - 1).unwrap();
        for frame in &frames {
            wal_tier
                .save(
                    &graphrefly_storage::wal_frame_key(
                        &graphrefly_storage::graph_wal_prefix("target"),
                        frame.frame_seq,
                    ),
                    frame.clone(),
                )
                .unwrap();
        }
    }
    wal_tier.flush().unwrap();

    let (rt2, g2, b2) = make_graph("target");
    g2.state(rt2.core(), "x", None).unwrap();

    let result = restore_snapshot(
        rt2.core(),
        &g2,
        &RestoreOptions {
            snapshot_tier: &snap_tier,
            wal_tier: &wal_tier,
            target_seq: Some(2),
            on_torn_write: None,
        },
    )
    .unwrap();

    assert_eq!(result.replayed_frames, 2);
    assert_eq!(result.final_seq, 2);
    assert_eq!(b2.deref(g2.get(rt2.core(), "x")), Value::from(20));
}

#[test]
fn restore_custom_torn_policy_skip_all() {
    let (rt, g, b) = make_graph("custom");
    let h = b.intern(Value::from(1));
    g.state(rt.core(), "x", Some(h)).unwrap();

    let snap_tier = make_snap_tier(memory_backend(), "custom");
    snap_tier
        .save(GraphCheckpointRecord {
            name: "custom".to_owned(),
            mode: "full".to_owned(),
            snapshot: g.snapshot(rt.core()),
            seq: 0,
            timestamp_ns: 1000,
            format_version: SNAPSHOT_VERSION,
        })
        .unwrap();
    snap_tier.flush().unwrap();

    let wal_tier = make_wal_tier(memory_backend(), "wal");
    for seq in 1..=2u64 {
        let bad = graphrefly_storage::WALFrame {
            t: graphrefly_storage::WalTag,
            lifecycle: Lifecycle::Data,
            path: "x".to_owned(),
            change: graphrefly_structures::BaseChange {
                structure: "graph.value".to_owned(),
                version: graphrefly_structures::Version::Counter(1),
                t_ns: 2000,
                seq: None,
                lifecycle: Lifecycle::Data,
                change: serde_json::json!({"kind": "node.set", "path": "x", "value": seq}),
            },
            frame_seq: seq,
            frame_t_ns: 2000,
            checksum: "bad".to_owned(),
            format_version: 1,
        };
        wal_tier
            .save(
                &graphrefly_storage::wal_frame_key(
                    &graphrefly_storage::graph_wal_prefix("custom"),
                    seq,
                ),
                bad,
            )
            .unwrap();
    }
    wal_tier.flush().unwrap();

    let (rt2, g2, _) = make_graph("custom");
    g2.state(rt2.core(), "x", None).unwrap();

    let result = restore_snapshot(
        rt2.core(),
        &g2,
        &RestoreOptions {
            snapshot_tier: &snap_tier,
            wal_tier: &wal_tier,
            target_seq: None,
            on_torn_write: Some(Box::new(|_, _| TornWritePolicy::Skip)),
        },
    )
    .unwrap();

    assert_eq!(result.skipped_frames, 2);
}

// ---------------------------------------------------------------------------
// GraphCheckpointRecord serde round-trip
// ---------------------------------------------------------------------------

#[test]
fn checkpoint_record_json_round_trip() {
    let record = GraphCheckpointRecord {
        name: "test".to_owned(),
        mode: "full".to_owned(),
        snapshot: empty_snapshot("test"),
        seq: 42,
        timestamp_ns: 1_000_000,
        format_version: SNAPSHOT_VERSION,
    };
    let json = serde_json::to_string(&record).unwrap();
    let parsed: GraphCheckpointRecord = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.name, "test");
    assert_eq!(parsed.seq, 42);
    assert_eq!(parsed.format_version, SNAPSHOT_VERSION);
}

// ---------------------------------------------------------------------------
// B4 (2026-05-22, /porting-to-rs storage-honest-error batch) — apply_wal_frame
// mount/unmount replay coverage.
//
// `decompose_diff_to_frames` already emits `graph.mount` / `graph.unmount`
// frames for net subgraph adds/removes (lines 235-258 of graph_integration.rs).
// Pre-B4 the `apply_wal_frame` `Lifecycle::Spec` match arms ignored them
// (`// graph.mount, graph.unmount — deferred (Phase 14.6+)`), so a replay
// could never reconstruct subgraph topology — only state nodes. B4 wires the
// apply side so a baseline + WAL replay restores the full graph shape.
//
// TS counterpart (`packages/pure-ts/src/graph/graph.ts:6717`) has the same
// gap pre-B4; Rust ships ahead per the cross-track-ledger handoff.
// ---------------------------------------------------------------------------

#[test]
fn restore_replays_mount_frame() {
    // Author graph "g" with subgraph "child" mounted; baseline is empty.
    // Diff(baseline, after) yields one `graph.mount` frame; replay onto a
    // fresh empty graph "g" must reproduce the mounted subgraph.
    let (_rt, g, _b) = make_graph("g");
    g.mount_new(_rt.core(), "child").unwrap();
    let after = g.snapshot(_rt.core());

    let before = empty_snapshot("g");
    let diff = diff_snapshots(&before, &after);
    assert_eq!(diff.subgraphs_added, vec!["child"]);
    let (frames, _) = decompose_diff_to_frames(&diff, 1000, 0).unwrap();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].lifecycle, Lifecycle::Spec);

    // Persist baseline (empty) + WAL frames.
    let snap_backend = memory_backend();
    let snap_tier = make_snap_tier(snap_backend, "g");
    snap_tier
        .save(GraphCheckpointRecord {
            name: "g".to_owned(),
            mode: "full".to_owned(),
            snapshot: before,
            seq: 0,
            timestamp_ns: 1000,
            format_version: SNAPSHOT_VERSION,
        })
        .unwrap();
    snap_tier.flush().unwrap();

    let wal_backend = memory_backend();
    let wal_tier = make_wal_tier(wal_backend, "wal");
    for frame in &frames {
        let key = graphrefly_storage::wal_frame_key(
            &graphrefly_storage::graph_wal_prefix("g"),
            frame.frame_seq,
        );
        wal_tier.save(&key, frame.clone()).unwrap();
    }
    wal_tier.flush().unwrap();

    // Replay onto a fresh empty graph.
    let (rt2, g2, _b2) = make_graph("g");
    assert_eq!(g2.child_names().len(), 0);
    let result = restore_snapshot(
        rt2.core(),
        &g2,
        &RestoreOptions {
            snapshot_tier: &snap_tier,
            wal_tier: &wal_tier,
            target_seq: None,
            on_torn_write: None,
        },
    )
    .unwrap();
    assert_eq!(result.replayed_frames, 1);
    assert_eq!(result.phases[0].lifecycle, Lifecycle::Spec);

    // Verify subgraph "child" landed.
    assert_eq!(g2.child_names(), vec!["child".to_owned()]);
}

#[test]
fn restore_replays_unmount_frame() {
    // Baseline has subgraph; after-snapshot doesn't. Diff produces an
    // `graph.unmount` frame; replay onto a fresh graph that already has
    // the subgraph (via baseline restore) must remove it.
    let (rt, g, _b) = make_graph("g");
    g.mount_new(rt.core(), "child").unwrap();
    let baseline = g.snapshot(rt.core());

    // After: subgraph removed.
    let mut after = baseline.clone();
    after.subgraphs.shift_remove("child");
    let diff = diff_snapshots(&baseline, &after);
    assert_eq!(diff.subgraphs_removed, vec!["child"]);
    let (frames, _) = decompose_diff_to_frames(&diff, 2000, 0).unwrap();
    assert_eq!(frames.len(), 1);

    let snap_tier = make_snap_tier(memory_backend(), "g");
    snap_tier
        .save(GraphCheckpointRecord {
            name: "g".to_owned(),
            mode: "full".to_owned(),
            snapshot: baseline, // baseline carries "child"
            seq: 0,
            timestamp_ns: 1000,
            format_version: SNAPSHOT_VERSION,
        })
        .unwrap();
    snap_tier.flush().unwrap();

    let wal_tier = make_wal_tier(memory_backend(), "wal");
    for frame in &frames {
        let key = graphrefly_storage::wal_frame_key(
            &graphrefly_storage::graph_wal_prefix("g"),
            frame.frame_seq,
        );
        wal_tier.save(&key, frame.clone()).unwrap();
    }
    wal_tier.flush().unwrap();

    // Replay onto a fresh graph that ALREADY has "child" mounted (mimics a
    // baseline restore that hydrated the subgraph). The WAL frame should
    // then remove it.
    let (rt2, g2, _b2) = make_graph("g");
    g2.mount_new(rt2.core(), "child").unwrap();
    assert_eq!(g2.child_names(), vec!["child".to_owned()]);

    let result = restore_snapshot(
        rt2.core(),
        &g2,
        &RestoreOptions {
            snapshot_tier: &snap_tier,
            wal_tier: &wal_tier,
            target_seq: None,
            on_torn_write: None,
        },
    )
    .unwrap();
    assert_eq!(result.replayed_frames, 1);
    assert_eq!(g2.child_names().len(), 0);
}

#[test]
fn apply_wal_frame_mount_is_idempotent() {
    // Replaying the same mount frame onto a graph that already has the
    // subgraph must be a no-op (no error, no duplicate mount). This guards
    // the common case where a baseline snapshot already hydrated the
    // subgraph topology and a tail WAL frame would otherwise attempt a
    // duplicate mount.
    let (rt, g, _b) = make_graph("g");
    g.mount_new(rt.core(), "child").unwrap();
    let snapshot = g.snapshot(rt.core());

    let before = empty_snapshot("g");
    let diff = diff_snapshots(&before, &snapshot);
    let (frames, _) = decompose_diff_to_frames(&diff, 1000, 0).unwrap();

    let snap_tier = make_snap_tier(memory_backend(), "g");
    // Baseline ALREADY carries "child" (so post-baseline-restore the graph
    // has it mounted; the WAL mount frame on tail would normally duplicate).
    snap_tier
        .save(GraphCheckpointRecord {
            name: "g".to_owned(),
            mode: "full".to_owned(),
            snapshot,
            seq: 0,
            timestamp_ns: 1000,
            format_version: SNAPSHOT_VERSION,
        })
        .unwrap();
    snap_tier.flush().unwrap();

    let wal_tier = make_wal_tier(memory_backend(), "wal");
    for frame in &frames {
        let key = graphrefly_storage::wal_frame_key(
            &graphrefly_storage::graph_wal_prefix("g"),
            frame.frame_seq,
        );
        wal_tier.save(&key, frame.clone()).unwrap();
    }
    wal_tier.flush().unwrap();

    // The baseline carries "child" + the WAL has the historical mount frame.
    // Realistic restore scenario: the embedder pre-builds the topology
    // (subgraph mounts) before calling `restore_snapshot`, because
    // baseline restore is strict about subgraph presence. The pre-existing
    // mount + the replay's mount frame are the idempotency exercise.
    let (rt2, g2, _b2) = make_graph("g");
    g2.mount_new(rt2.core(), "child").unwrap();
    assert_eq!(g2.child_names(), vec!["child".to_owned()]);

    let result = restore_snapshot(
        rt2.core(),
        &g2,
        &RestoreOptions {
            snapshot_tier: &snap_tier,
            wal_tier: &wal_tier,
            target_seq: None,
            on_torn_write: None,
        },
    )
    .unwrap();
    // The single mount frame was replayed; the idempotent skip in
    // `apply_wal_frame` (B4) prevents a duplicate mount. Final state has
    // exactly one "child" mount.
    assert_eq!(result.replayed_frames, 1);
    assert_eq!(g2.child_names(), vec!["child".to_owned()]);
}
