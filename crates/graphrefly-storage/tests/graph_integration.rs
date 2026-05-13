//! Integration tests for graph-level storage integration (M4.E2).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use graphrefly_core::{BindingBoundary, DepBatch, FnId, FnResult, HandleId, NodeId, NO_HANDLE};
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

fn make_graph(name: &str) -> (Graph, Arc<TestBinding>) {
    let b = TestBinding::new();
    let g = Graph::new(name, b.clone() as Arc<dyn BindingBoundary>);
    (g, b)
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
    let (g, b) = make_graph("test");
    let h1 = b.intern(Value::from(10));
    g.state("counter", Some(h1)).unwrap();

    let backend = memory_backend();
    let snap_tier = make_snap_tier(backend.clone(), "test");
    let snap_tier_box: Box<dyn SnapshotStorageTier<GraphCheckpointRecord>> = Box::new(snap_tier);

    let _handle = attach_snapshot_storage(
        &g,
        vec![AttachTierPair {
            snapshot: snap_tier_box,
            wal: None,
        }],
        AttachOptions::default(),
    );

    // Emit a value to trigger flush.
    let h2 = b.intern(Value::from(20));
    g.set("counter", h2);
    std::thread::sleep(std::time::Duration::from_millis(20));

    // Verify baseline was written.
    let stored = backend.read("test").unwrap();
    assert!(stored.is_some(), "baseline should be written");
    let record: GraphCheckpointRecord = serde_json::from_slice(&stored.unwrap()).unwrap();
    assert_eq!(record.name, "test");
    assert_eq!(record.mode, "full");
    assert_eq!(record.format_version, SNAPSHOT_VERSION);
}

#[test]
fn attach_wal_delta_after_baseline() {
    let (g, b) = make_graph("wal-test");
    let h1 = b.intern(Value::from(1));
    g.state("x", Some(h1)).unwrap();

    let snap_backend = memory_backend();
    let snap_tier = make_snap_tier_with_compact(snap_backend.clone(), "wal-test", 10);
    let snap_box: Box<dyn SnapshotStorageTier<GraphCheckpointRecord>> = Box::new(snap_tier);

    let wal_backend = memory_backend();
    let wal_tier = make_wal_tier(wal_backend.clone(), "wal");
    let wal_box: Box<dyn KvStorageTier<graphrefly_storage::WALFrame<Value>>> = Box::new(wal_tier);

    let _handle = attach_snapshot_storage(
        &g,
        vec![AttachTierPair {
            snapshot: snap_box,
            wal: Some(wal_box),
        }],
        AttachOptions::default(),
    );

    // First emit → full baseline.
    let h2 = b.intern(Value::from(2));
    g.set("x", h2);
    std::thread::sleep(std::time::Duration::from_millis(20));
    assert!(snap_backend.read("wal-test").unwrap().is_some());

    // Second emit → WAL delta.
    let h3 = b.intern(Value::from(3));
    g.set("x", h3);
    std::thread::sleep(std::time::Duration::from_millis(20));

    let wal_keys = wal_backend.list("wal-test/wal").unwrap();
    assert!(!wal_keys.is_empty(), "WAL frames should exist after delta");
}

#[test]
fn attach_dispose_stops_persistence() {
    let (g, b) = make_graph("dispose");
    let h1 = b.intern(Value::from(1));
    g.state("x", Some(h1)).unwrap();

    let snap_backend = memory_backend();
    let snap_tier = make_snap_tier(snap_backend.clone(), "dispose");
    let snap_box: Box<dyn SnapshotStorageTier<GraphCheckpointRecord>> = Box::new(snap_tier);

    let handle = attach_snapshot_storage(
        &g,
        vec![AttachTierPair {
            snapshot: snap_box,
            wal: None,
        }],
        AttachOptions::default(),
    );

    let h2 = b.intern(Value::from(2));
    g.set("x", h2);
    std::thread::sleep(std::time::Duration::from_millis(20));
    assert!(snap_backend.read("dispose").unwrap().is_some());

    // Dispose.
    handle.dispose();
    snap_backend.write("dispose", &[]).unwrap(); // clear

    let h3 = b.intern(Value::from(3));
    g.set("x", h3);
    std::thread::sleep(std::time::Duration::from_millis(20));

    let stored = snap_backend.read("dispose").unwrap().unwrap();
    assert!(stored.is_empty(), "after dispose, no new snapshot");
}

// ---------------------------------------------------------------------------
// restore_snapshot tests
// ---------------------------------------------------------------------------

#[test]
fn restore_baseline_only() {
    let (g, b) = make_graph("r");
    let h = b.intern(Value::from(42));
    g.state("x", Some(h)).unwrap();

    let snap_backend = memory_backend();
    let snap_tier = make_snap_tier(snap_backend, "r");
    snap_tier
        .save(GraphCheckpointRecord {
            name: "r".to_owned(),
            mode: "full".to_owned(),
            snapshot: g.snapshot(),
            seq: 0,
            timestamp_ns: 1000,
            format_version: SNAPSHOT_VERSION,
        })
        .unwrap();
    snap_tier.flush().unwrap();

    let wal_backend = memory_backend();
    let wal_tier = make_wal_tier(wal_backend, "wal");

    let (g2, b2) = make_graph("r");
    g2.state("x", None).unwrap();

    let result = restore_snapshot(
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
    let cache = g2.get("x");
    assert_ne!(cache, NO_HANDLE);
    assert_eq!(b2.deref(cache), Value::from(42));
}

#[test]
fn restore_missing_baseline_errors() {
    let (g, _) = make_graph("e");
    g.state("x", None).unwrap();

    let snap_tier = make_snap_tier(memory_backend(), "e");
    let wal_tier = make_wal_tier(memory_backend(), "wal");

    let err = restore_snapshot(
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
    let (g, b) = make_graph("replay");
    let h = b.intern(Value::from(10));
    g.state("x", Some(h)).unwrap();

    let snap_backend = memory_backend();
    let snap_tier = make_snap_tier(snap_backend, "replay");
    let baseline = g.snapshot();
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

    let (g2, b2) = make_graph("replay");
    g2.state("x", None).unwrap();

    let result = restore_snapshot(
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
    assert_eq!(b2.deref(g2.get("x")), Value::from(20));
}

#[test]
fn restore_torn_tail_skipped() {
    let (g, b) = make_graph("torn");
    let h = b.intern(Value::from(1));
    g.state("x", Some(h)).unwrap();

    let snap_tier = make_snap_tier(memory_backend(), "torn");
    snap_tier
        .save(GraphCheckpointRecord {
            name: "torn".to_owned(),
            mode: "full".to_owned(),
            snapshot: g.snapshot(),
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

    let (g2, _) = make_graph("torn");
    g2.state("x", None).unwrap();

    let result = restore_snapshot(
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
    let (g, b) = make_graph("torn-mid");
    let h = b.intern(Value::from(1));
    g.state("x", Some(h)).unwrap();

    let snap_tier = make_snap_tier(memory_backend(), "torn-mid");
    snap_tier
        .save(GraphCheckpointRecord {
            name: "torn-mid".to_owned(),
            mode: "full".to_owned(),
            snapshot: g.snapshot(),
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

    let (g2, _) = make_graph("torn-mid");
    g2.state("x", None).unwrap();

    let err = restore_snapshot(
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
    let (g, b) = make_graph("target");
    let h = b.intern(Value::from(1));
    g.state("x", Some(h)).unwrap();

    let snap_tier = make_snap_tier(memory_backend(), "target");
    snap_tier
        .save(GraphCheckpointRecord {
            name: "target".to_owned(),
            mode: "full".to_owned(),
            snapshot: g.snapshot(),
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

    let (g2, b2) = make_graph("target");
    g2.state("x", None).unwrap();

    let result = restore_snapshot(
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
    assert_eq!(b2.deref(g2.get("x")), Value::from(20));
}

#[test]
fn restore_custom_torn_policy_skip_all() {
    let (g, b) = make_graph("custom");
    let h = b.intern(Value::from(1));
    g.state("x", Some(h)).unwrap();

    let snap_tier = make_snap_tier(memory_backend(), "custom");
    snap_tier
        .save(GraphCheckpointRecord {
            name: "custom".to_owned(),
            mode: "full".to_owned(),
            snapshot: g.snapshot(),
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

    let (g2, _) = make_graph("custom");
    g2.state("x", None).unwrap();

    let result = restore_snapshot(
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
