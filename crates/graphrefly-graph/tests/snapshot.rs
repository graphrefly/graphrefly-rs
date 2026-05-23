//! Integration tests for `Graph::snapshot()`, `Graph::restore()`, and
//! `Graph::from_snapshot()` — M4.E1 (R3.8).

mod common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use common::graph_with;
use graphrefly_core::{
    BindingBoundary, DepBatch, EqualsMode, FnId, FnResult, HandleId, NodeId, OwnedCore, NO_HANDLE,
};
use graphrefly_graph::{
    Graph, GraphPersistSnapshot, NodeFactory, NodeSlice, NodeSnapshotStatus, SnapshotError,
};
use indexmap::IndexMap;

// ---------------------------------------------------------------------------
// Test binding that supports snapshot serialization.
// ---------------------------------------------------------------------------

type JsonFn = Arc<dyn Fn(&[serde_json::Value]) -> serde_json::Value + Send + Sync>;

struct SnapshotBinding {
    next_handle: AtomicU64,
    next_fn_id: AtomicU64,
    values: Mutex<HashMap<HandleId, serde_json::Value>>,
    fns: Mutex<HashMap<FnId, JsonFn>>,
}

impl SnapshotBinding {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            next_handle: AtomicU64::new(1),
            next_fn_id: AtomicU64::new(1),
            values: Mutex::new(HashMap::new()),
            fns: Mutex::new(HashMap::new()),
        })
    }

    fn intern(&self, value: serde_json::Value) -> HandleId {
        let id = self.next_handle.fetch_add(1, Ordering::SeqCst);
        let h = HandleId::new(id);
        self.values.lock().unwrap().insert(h, value);
        h
    }

    fn register_fn<F>(&self, f: F) -> FnId
    where
        F: Fn(&[serde_json::Value]) -> serde_json::Value + Send + Sync + 'static,
    {
        let id = self.next_fn_id.fetch_add(1, Ordering::SeqCst);
        let fn_id = FnId::new(id);
        self.fns.lock().unwrap().insert(fn_id, Arc::new(f));
        fn_id
    }

    fn deref(&self, handle: HandleId) -> serde_json::Value {
        self.values
            .lock()
            .unwrap()
            .get(&handle)
            .cloned()
            .unwrap_or(serde_json::Value::Null)
    }
}

impl BindingBoundary for SnapshotBinding {
    fn invoke_fn(&self, _node_id: NodeId, fn_id: FnId, dep_data: &[DepBatch]) -> FnResult {
        let f = self.fns.lock().unwrap().get(&fn_id).cloned();
        if let Some(f) = f {
            let args: Vec<serde_json::Value> =
                dep_data.iter().map(|db| self.deref(db.latest())).collect();
            let result = f(&args);
            let handle = self.intern(result);
            FnResult::Data {
                handle,
                tracked: None,
            }
        } else {
            // Default: pass first dep through.
            dep_data
                .first()
                .map_or(FnResult::Noop { tracked: None }, |db| FnResult::Data {
                    handle: db.latest(),
                    tracked: None,
                })
        }
    }

    fn custom_equals(&self, _: FnId, a: HandleId, b: HandleId) -> bool {
        a == b
    }

    fn release_handle(&self, _h: HandleId) {}

    fn serialize_handle(&self, handle: HandleId) -> Option<serde_json::Value> {
        self.values.lock().unwrap().get(&handle).cloned()
    }

    fn deserialize_value(&self, value: serde_json::Value) -> HandleId {
        self.intern(value)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn r3_8_snapshot_empty_graph() {
    let binding = SnapshotBinding::new();
    let (rt, g) = graph_with("empty", binding as Arc<dyn BindingBoundary>);

    let snap = g.snapshot(rt.core());
    assert_eq!(snap.name, "empty");
    assert!(snap.nodes.is_empty());
    assert!(snap.subgraphs.is_empty());
}

#[test]
fn r3_8_snapshot_state_with_value() {
    let binding = SnapshotBinding::new();
    let h = binding.intern(serde_json::json!(42));
    let (rt, g) = graph_with("test", binding.clone() as Arc<dyn BindingBoundary>);

    g.state(rt.core(), "counter", Some(h)).unwrap();

    let snap = g.snapshot(rt.core());
    assert_eq!(snap.nodes.len(), 1);
    let node = &snap.nodes["counter"];
    assert_eq!(node.node_type, "state");
    assert_eq!(node.value, Some(serde_json::json!(42)));
    assert_eq!(node.status, NodeSnapshotStatus::Live);
    assert!(node.deps.is_empty());
}

#[test]
fn r3_8_snapshot_sentinel_state() {
    let binding = SnapshotBinding::new();
    let (rt, g) = graph_with("test", binding as Arc<dyn BindingBoundary>);

    g.state(rt.core(), "x", None).unwrap();

    let snap = g.snapshot(rt.core());
    let node = &snap.nodes["x"];
    assert_eq!(node.status, NodeSnapshotStatus::Sentinel);
    assert_eq!(node.value, None);
}

#[test]
fn r3_8_snapshot_derived_with_deps() {
    let binding = SnapshotBinding::new();
    let initial = binding.intern(serde_json::json!(10));
    let fn_id = binding.register_fn(|args| {
        let x = args[0].as_i64().unwrap_or(0);
        serde_json::json!(x * 2)
    });
    let (rt, g) = graph_with("test", binding.clone() as Arc<dyn BindingBoundary>);

    let x = g.state(rt.core(), "x", Some(initial)).unwrap();
    g.derived(rt.core(), "doubled", &[x], fn_id, EqualsMode::Identity)
        .unwrap();

    // Subscribe to trigger fn fire.
    let doubled_id = g.node("doubled");
    let sub = g.subscribe(rt.core(), doubled_id, std::rc::Rc::new(|_| {}));

    let snap = g.snapshot(rt.core());
    assert_eq!(snap.nodes.len(), 2);

    let doubled = &snap.nodes["doubled"];
    assert_eq!(doubled.node_type, "derived");
    assert_eq!(doubled.deps, vec!["x"]);
    // Derived should have computed: 10 * 2 = 20
    assert_eq!(doubled.value, Some(serde_json::json!(20)));
    g.unsubscribe(rt.core(), doubled_id, sub);
}

#[test]
fn r3_8_snapshot_completed_terminal() {
    let binding = SnapshotBinding::new();
    let h = binding.intern(serde_json::json!("hello"));
    let (rt, g) = graph_with("test", binding as Arc<dyn BindingBoundary>);

    let x = g.state(rt.core(), "x", Some(h)).unwrap();
    g.complete(rt.core(), x);

    let snap = g.snapshot(rt.core());
    assert_eq!(snap.nodes["x"].status, NodeSnapshotStatus::Completed);
}

#[test]
fn r3_8_snapshot_errored_terminal() {
    let binding = SnapshotBinding::new();
    let h = binding.intern(serde_json::json!("value"));
    let err = binding.intern(serde_json::json!({"code": 500}));
    let (rt, g) = graph_with("test", binding as Arc<dyn BindingBoundary>);

    let x = g.state(rt.core(), "x", Some(h)).unwrap();
    g.error(rt.core(), x, err);

    let snap = g.snapshot(rt.core());
    match &snap.nodes["x"].status {
        NodeSnapshotStatus::Errored { error } => {
            assert_eq!(error, &Some(serde_json::json!({"code": 500})));
        }
        other => panic!("expected Errored, got {other:?}"),
    }
}

#[test]
fn r3_8_snapshot_with_mounted_subgraph() {
    let binding = SnapshotBinding::new();
    let h1 = binding.intern(serde_json::json!("root"));
    let h2 = binding.intern(serde_json::json!("child"));
    let (rt, g) = graph_with("root", binding as Arc<dyn BindingBoundary>);

    g.state(rt.core(), "a", Some(h1)).unwrap();
    let sub = g.mount_new(rt.core(), "sub").unwrap();
    sub.state(rt.core(), "b", Some(h2)).unwrap();

    let snap = g.snapshot(rt.core());
    assert_eq!(snap.nodes.len(), 1);
    assert_eq!(snap.subgraphs.len(), 1);

    let sub_snap = &snap.subgraphs["sub"];
    assert_eq!(sub_snap.name, "sub");
    assert_eq!(sub_snap.nodes.len(), 1);
    assert_eq!(sub_snap.nodes["b"].value, Some(serde_json::json!("child")));
}

#[test]
fn r3_8_snapshot_json_round_trip() {
    let binding = SnapshotBinding::new();
    let h = binding.intern(serde_json::json!(42));
    let (rt, g) = graph_with("test", binding as Arc<dyn BindingBoundary>);
    g.state(rt.core(), "x", Some(h)).unwrap();

    let snap = g.snapshot(rt.core());
    let json = serde_json::to_string(&snap).unwrap();
    let restored: GraphPersistSnapshot = serde_json::from_str(&json).unwrap();

    assert_eq!(restored.name, "test");
    assert_eq!(restored.nodes["x"].value, Some(serde_json::json!(42)));
    assert_eq!(restored.nodes["x"].node_type, "state");
}

#[test]
fn r3_8_restore_state_values() {
    let binding = SnapshotBinding::new();
    let (rt, g) = graph_with("test", binding.clone() as Arc<dyn BindingBoundary>);
    g.state(rt.core(), "x", None).unwrap();
    g.state(rt.core(), "y", None).unwrap();

    // Build a snapshot with values.
    let snap = GraphPersistSnapshot {
        name: "test".to_owned(),
        nodes: {
            let mut m = IndexMap::new();
            m.insert(
                "x".to_owned(),
                NodeSlice {
                    node_type: "state".to_owned(),
                    value: Some(serde_json::json!(100)),
                    status: NodeSnapshotStatus::Live,
                    deps: vec![],
                },
            );
            m.insert(
                "y".to_owned(),
                NodeSlice {
                    node_type: "state".to_owned(),
                    value: Some(serde_json::json!("hello")),
                    status: NodeSnapshotStatus::Live,
                    deps: vec![],
                },
            );
            m
        },
        subgraphs: IndexMap::new(),
    };

    g.restore(rt.core(), &snap).unwrap();

    let x_cache = g.cache_of(rt.core(), g.node("x"));
    let y_cache = g.cache_of(rt.core(), g.node("y"));
    assert_ne!(x_cache, NO_HANDLE);
    assert_ne!(y_cache, NO_HANDLE);
    assert_eq!(binding.deref(x_cache), serde_json::json!(100));
    assert_eq!(binding.deref(y_cache), serde_json::json!("hello"));
}

#[test]
fn r3_8_restore_name_mismatch_errors() {
    let binding = SnapshotBinding::new();
    let (rt, g) = graph_with("graph-a", binding as Arc<dyn BindingBoundary>);

    let snap = GraphPersistSnapshot {
        name: "graph-b".to_owned(),
        nodes: IndexMap::new(),
        subgraphs: IndexMap::new(),
    };

    let err = g.restore(rt.core(), &snap).unwrap_err();
    assert!(matches!(err, SnapshotError::NameMismatch { .. }));
}

#[test]
fn r3_8_restore_unknown_node_errors() {
    let binding = SnapshotBinding::new();
    let (rt, g) = graph_with("test", binding as Arc<dyn BindingBoundary>);

    let snap = GraphPersistSnapshot {
        name: "test".to_owned(),
        nodes: {
            let mut m = IndexMap::new();
            m.insert(
                "nonexistent".to_owned(),
                NodeSlice {
                    node_type: "state".to_owned(),
                    value: Some(serde_json::json!(1)),
                    status: NodeSnapshotStatus::Live,
                    deps: vec![],
                },
            );
            m
        },
        subgraphs: IndexMap::new(),
    };

    let err = g.restore(rt.core(), &snap).unwrap_err();
    assert!(matches!(err, SnapshotError::UnknownNode(_)));
}

#[test]
fn r3_8_restore_completed_terminal() {
    let binding = SnapshotBinding::new();
    let (rt, g) = graph_with("test", binding as Arc<dyn BindingBoundary>);
    let x = g.state(rt.core(), "x", None).unwrap();

    let snap = GraphPersistSnapshot {
        name: "test".to_owned(),
        nodes: {
            let mut m = IndexMap::new();
            m.insert(
                "x".to_owned(),
                NodeSlice {
                    node_type: "state".to_owned(),
                    value: None,
                    status: NodeSnapshotStatus::Completed,
                    deps: vec![],
                },
            );
            m
        },
        subgraphs: IndexMap::new(),
    };

    g.restore(rt.core(), &snap).unwrap();
    assert!(rt.core().is_terminal(x).is_some());
}

#[test]
fn r3_8_snapshot_then_restore_round_trip() {
    let binding = SnapshotBinding::new();
    let h1 = binding.intern(serde_json::json!(42));
    let h2 = binding.intern(serde_json::json!("world"));

    let (rt1, g1) = graph_with("test", binding.clone() as Arc<dyn BindingBoundary>);
    g1.state(rt1.core(), "counter", Some(h1)).unwrap();
    g1.state(rt1.core(), "label", Some(h2)).unwrap();

    let snap = g1.snapshot(rt1.core());

    // Build a second graph and restore.
    let (rt2, g2) = graph_with("test", binding.clone() as Arc<dyn BindingBoundary>);
    g2.state(rt2.core(), "counter", None).unwrap();
    g2.state(rt2.core(), "label", None).unwrap();
    g2.restore(rt2.core(), &snap).unwrap();

    let c = g2.cache_of(rt2.core(), g2.node("counter"));
    let l = g2.cache_of(rt2.core(), g2.node("label"));
    assert_eq!(binding.deref(c), serde_json::json!(42));
    assert_eq!(binding.deref(l), serde_json::json!("world"));
}

#[test]
fn r3_8_from_snapshot_builder_mode() {
    let binding = SnapshotBinding::new();
    let rt = OwnedCore::new(binding.clone() as Arc<dyn BindingBoundary>);

    let snap = GraphPersistSnapshot {
        name: "test".to_owned(),
        nodes: {
            let mut m = IndexMap::new();
            m.insert(
                "x".to_owned(),
                NodeSlice {
                    node_type: "state".to_owned(),
                    value: Some(serde_json::json!(99)),
                    status: NodeSnapshotStatus::Live,
                    deps: vec![],
                },
            );
            m
        },
        subgraphs: IndexMap::new(),
    };

    let g = Graph::from_snapshot(
        rt.core(),
        &snap,
        Some(Box::new(|core: &graphrefly_core::Core, g: &Graph| {
            g.state(core, "x", None).unwrap();
        })),
        None,
    )
    .unwrap();

    let x_cache = g.cache_of(rt.core(), g.node("x"));
    assert_eq!(binding.deref(x_cache), serde_json::json!(99));
}

#[test]
fn r3_8_from_snapshot_auto_hydration_state_only() {
    let binding = SnapshotBinding::new();
    let rt = OwnedCore::new(binding.clone() as Arc<dyn BindingBoundary>);

    let snap = GraphPersistSnapshot {
        name: "test".to_owned(),
        nodes: {
            let mut m = IndexMap::new();
            m.insert(
                "a".to_owned(),
                NodeSlice {
                    node_type: "state".to_owned(),
                    value: Some(serde_json::json!(1)),
                    status: NodeSnapshotStatus::Live,
                    deps: vec![],
                },
            );
            m.insert(
                "b".to_owned(),
                NodeSlice {
                    node_type: "state".to_owned(),
                    value: Some(serde_json::json!(2)),
                    status: NodeSnapshotStatus::Live,
                    deps: vec![],
                },
            );
            m
        },
        subgraphs: IndexMap::new(),
    };

    // Auto-hydrate — state nodes need no factory.
    let g = Graph::from_snapshot(rt.core(), &snap, None, None).unwrap();

    assert_eq!(g.node_count(), 2);
    assert_eq!(
        binding.deref(g.cache_of(rt.core(), g.node("a"))),
        serde_json::json!(1)
    );
    assert_eq!(
        binding.deref(g.cache_of(rt.core(), g.node("b"))),
        serde_json::json!(2)
    );
}

#[test]
fn r3_8_from_snapshot_auto_hydration_with_derived_factory() {
    let binding = SnapshotBinding::new();
    let rt = OwnedCore::new(binding.clone() as Arc<dyn BindingBoundary>);
    let binding_clone = binding.clone();

    let mut factories: IndexMap<String, NodeFactory> = IndexMap::new();
    factories.insert(
        "derived".to_owned(),
        Box::new(
            move |core: &graphrefly_core::Core,
                  graph: &Graph,
                  name: &str,
                  _slice: &NodeSlice,
                  dep_ids: &[NodeId]| {
                let fn_id = binding_clone.register_fn(|args| {
                    let x = args[0].as_i64().unwrap_or(0);
                    serde_json::json!(x * 10)
                });
                graph
                    .derived(core, name, dep_ids, fn_id, EqualsMode::Identity)
                    .map_err(|_| SnapshotError::UnknownNode(name.to_owned()))
            },
        ),
    );

    let snap = GraphPersistSnapshot {
        name: "test".to_owned(),
        nodes: {
            let mut m = IndexMap::new();
            m.insert(
                "x".to_owned(),
                NodeSlice {
                    node_type: "state".to_owned(),
                    value: Some(serde_json::json!(5)),
                    status: NodeSnapshotStatus::Live,
                    deps: vec![],
                },
            );
            m.insert(
                "times_ten".to_owned(),
                NodeSlice {
                    node_type: "derived".to_owned(),
                    value: None, // Derived will recompute.
                    status: NodeSnapshotStatus::Live,
                    deps: vec!["x".to_owned()],
                },
            );
            m
        },
        subgraphs: IndexMap::new(),
    };

    let g = Graph::from_snapshot(rt.core(), &snap, None, Some(factories)).unwrap();

    assert_eq!(g.node_count(), 2);

    // Subscribe to trigger derived computation.
    let times_ten = g.node("times_ten");
    let sub = g.subscribe(rt.core(), times_ten, std::rc::Rc::new(|_| {}));

    let cache = g.cache_of(rt.core(), times_ten);
    assert_ne!(cache, NO_HANDLE);
    assert_eq!(binding.deref(cache), serde_json::json!(50));
    g.unsubscribe(rt.core(), times_ten, sub);
}

#[test]
fn r3_8_from_snapshot_missing_factory_errors() {
    let binding = SnapshotBinding::new();
    let rt = OwnedCore::new(binding as Arc<dyn BindingBoundary>);

    let snap = GraphPersistSnapshot {
        name: "test".to_owned(),
        nodes: {
            let mut m = IndexMap::new();
            m.insert(
                "d".to_owned(),
                NodeSlice {
                    node_type: "derived".to_owned(),
                    value: None,
                    status: NodeSnapshotStatus::Sentinel,
                    deps: vec![],
                },
            );
            m
        },
        subgraphs: IndexMap::new(),
    };

    let result = Graph::from_snapshot(rt.core(), &snap, None, None);
    assert!(matches!(result, Err(SnapshotError::MissingFactory(..))));
}

#[test]
fn r3_8_from_snapshot_unresolvable_deps_errors() {
    let binding = SnapshotBinding::new();
    let rt = OwnedCore::new(binding as Arc<dyn BindingBoundary>);

    let mut factories: IndexMap<String, NodeFactory> = IndexMap::new();
    factories.insert(
        "derived".to_owned(),
        Box::new(
            |core: &graphrefly_core::Core,
             graph: &Graph,
             name: &str,
             _slice: &NodeSlice,
             dep_ids: &[NodeId]| {
                // Won't actually be called — deps can't resolve.
                let fn_id = FnId::new(999);
                graph
                    .derived(core, name, dep_ids, fn_id, EqualsMode::Identity)
                    .map_err(|_| SnapshotError::UnknownNode(name.to_owned()))
            },
        ),
    );

    let snap = GraphPersistSnapshot {
        name: "test".to_owned(),
        nodes: {
            let mut m = IndexMap::new();
            m.insert(
                "d".to_owned(),
                NodeSlice {
                    node_type: "derived".to_owned(),
                    value: None,
                    status: NodeSnapshotStatus::Sentinel,
                    deps: vec!["missing_node".to_owned()],
                },
            );
            m
        },
        subgraphs: IndexMap::new(),
    };

    let result = Graph::from_snapshot(rt.core(), &snap, None, Some(factories));
    assert!(matches!(result, Err(SnapshotError::UnresolvableDeps(..))));
}

#[test]
fn r3_8_from_snapshot_auto_hydration_with_subgraph() {
    let binding = SnapshotBinding::new();
    let rt = OwnedCore::new(binding.clone() as Arc<dyn BindingBoundary>);

    let snap = GraphPersistSnapshot {
        name: "root".to_owned(),
        nodes: {
            let mut m = IndexMap::new();
            m.insert(
                "a".to_owned(),
                NodeSlice {
                    node_type: "state".to_owned(),
                    value: Some(serde_json::json!(1)),
                    status: NodeSnapshotStatus::Live,
                    deps: vec![],
                },
            );
            m
        },
        subgraphs: {
            let mut s = IndexMap::new();
            s.insert(
                "child".to_owned(),
                GraphPersistSnapshot {
                    name: "child".to_owned(),
                    nodes: {
                        let mut m = IndexMap::new();
                        m.insert(
                            "b".to_owned(),
                            NodeSlice {
                                node_type: "state".to_owned(),
                                value: Some(serde_json::json!(2)),
                                status: NodeSnapshotStatus::Live,
                                deps: vec![],
                            },
                        );
                        m
                    },
                    subgraphs: IndexMap::new(),
                },
            );
            s
        },
    };

    let g = Graph::from_snapshot(rt.core(), &snap, None, None).unwrap();

    assert_eq!(g.node_count(), 1);
    assert_eq!(
        binding.deref(g.cache_of(rt.core(), g.node("a"))),
        serde_json::json!(1)
    );

    // Access the child's node via path.
    let b_id = g.try_resolve("child::b").unwrap();
    assert_eq!(
        binding.deref(g.cache_of(rt.core(), b_id)),
        serde_json::json!(2)
    );
}

#[test]
fn r3_8_snapshot_preserves_insertion_order() {
    let binding = SnapshotBinding::new();
    let (rt, g) = graph_with("test", binding.clone() as Arc<dyn BindingBoundary>);

    let names = ["z", "a", "m", "b"];
    for &name in &names {
        let h = binding.intern(serde_json::json!(name));
        g.state(rt.core(), name, Some(h)).unwrap();
    }

    let snap = g.snapshot(rt.core());
    let snap_names: Vec<&str> = snap.nodes.keys().map(String::as_str).collect();
    assert_eq!(snap_names, names);
}

#[test]
fn r3_8_restore_skips_derived_nodes() {
    // Verify that restore doesn't try to emit on derived nodes —
    // only state nodes get their values restored.
    let binding = SnapshotBinding::new();
    let initial = binding.intern(serde_json::json!(5));
    let fn_id = binding.register_fn(|args| {
        let x = args[0].as_i64().unwrap_or(0);
        serde_json::json!(x + 1)
    });
    let (rt, g) = graph_with("test", binding.clone() as Arc<dyn BindingBoundary>);

    let x = g.state(rt.core(), "x", Some(initial)).unwrap();
    g.derived(rt.core(), "inc", &[x], fn_id, EqualsMode::Identity)
        .unwrap();

    // Snapshot includes derived with its computed value.
    let snap = GraphPersistSnapshot {
        name: "test".to_owned(),
        nodes: {
            let mut m = IndexMap::new();
            m.insert(
                "x".to_owned(),
                NodeSlice {
                    node_type: "state".to_owned(),
                    value: Some(serde_json::json!(10)),
                    status: NodeSnapshotStatus::Live,
                    deps: vec![],
                },
            );
            m.insert(
                "inc".to_owned(),
                NodeSlice {
                    node_type: "derived".to_owned(),
                    value: Some(serde_json::json!(999)), // Should be ignored.
                    status: NodeSnapshotStatus::Live,
                    deps: vec!["x".to_owned()],
                },
            );
            m
        },
        subgraphs: IndexMap::new(),
    };

    g.restore(rt.core(), &snap).unwrap();

    // x should have the restored value.
    assert_eq!(
        binding.deref(g.cache_of(rt.core(), g.node("x"))),
        serde_json::json!(10)
    );

    // inc should recompute from x (10 + 1 = 11), NOT use the snapshot's 999.
    let inc_id = g.node("inc");
    let sub = g.subscribe(rt.core(), inc_id, std::rc::Rc::new(|_| {}));
    assert_eq!(
        binding.deref(g.cache_of(rt.core(), inc_id)),
        serde_json::json!(11)
    );
    g.unsubscribe(rt.core(), inc_id, sub);
}

// ---------------------------------------------------------------------------
// D276 — cross-mount dep round-trip via owner-relative path encoding.
// ---------------------------------------------------------------------------

/// D276 (encode): a derived in a child mount whose dep lives in the
/// parent graph must serialize with the owner-relative path `..::name`
/// (pre-D276 the encoder fell through to `_anon_<rawid>` here, destroying
/// the cross-mount reference at serialization time).
#[test]
fn d276_encode_child_to_parent_dep_emits_double_colon_dotdot() {
    let binding = SnapshotBinding::new();
    let initial = binding.intern(serde_json::json!(7));
    let fn_id = binding.register_fn(|args| {
        let x = args[0].as_i64().unwrap_or(0);
        serde_json::json!(x + 100)
    });
    let (rt, root) = graph_with("root", binding.clone() as Arc<dyn BindingBoundary>);

    // root.x  +  root.child.derived_in_child (deps on root.x — cross-mount upward).
    let x = root.state(rt.core(), "x", Some(initial)).unwrap();
    let child = root.mount_new(rt.core(), "child").unwrap();
    child
        .derived(rt.core(), "d", &[x], fn_id, EqualsMode::Identity)
        .unwrap();

    let snap = root.snapshot(rt.core());
    let d = &snap.subgraphs["child"].nodes["d"];
    assert_eq!(d.node_type, "derived");
    // Pre-D276 this was `"_anon_<rawid>"`; post-D276 it is owner-relative.
    assert_eq!(d.deps, vec!["..::x".to_owned()]);
}

/// D276 (encode): a derived in the parent whose dep lives in a mounted
/// child graph must serialize with the owner-relative path `child::name`.
/// (Snapshots produced by the pre-D276 encoder fell through to
/// `_anon_<rawid>` for these too.)
#[test]
fn d276_encode_parent_to_child_dep_emits_child_double_colon() {
    let binding = SnapshotBinding::new();
    let initial = binding.intern(serde_json::json!(3));
    let fn_id = binding.register_fn(|args| {
        let x = args[0].as_i64().unwrap_or(0);
        serde_json::json!(x * 10)
    });
    let (rt, root) = graph_with("root", binding.clone() as Arc<dyn BindingBoundary>);

    // root.child.y  +  root.d (deps on root.child.y — cross-mount downward).
    let child = root.mount_new(rt.core(), "child").unwrap();
    let y = child.state(rt.core(), "y", Some(initial)).unwrap();
    root.derived(rt.core(), "d", &[y], fn_id, EqualsMode::Identity)
        .unwrap();

    let snap = root.snapshot(rt.core());
    let d = &snap.nodes["d"];
    assert_eq!(d.node_type, "derived");
    assert_eq!(d.deps, vec!["child::y".to_owned()]);
}

/// D276 (decode round-trip): a graph with a child-to-parent cross-mount
/// derived dep round-trips through `snapshot() → from_snapshot()` and
/// the derived fires correctly after restore (the tree-wide retry loop
/// is what unblocks this: the parent's state node must be created
/// BEFORE the child's derived in Pass 1 + Pass 2, even though the
/// pre-D276 recursive `hydrate_subgraph` processed the child first
/// and would have failed with UnresolvableDeps).
#[test]
fn d276_round_trip_child_to_parent_cross_mount_dep() {
    let binding = SnapshotBinding::new();
    let initial = binding.intern(serde_json::json!(7));
    let fn_id = binding.register_fn(|args| {
        let x = args[0].as_i64().unwrap_or(0);
        serde_json::json!(x + 100)
    });
    let (rt, root) = graph_with("root", binding.clone() as Arc<dyn BindingBoundary>);

    let x = root.state(rt.core(), "x", Some(initial)).unwrap();
    let child = root.mount_new(rt.core(), "child").unwrap();
    let d = child
        .derived(rt.core(), "d", &[x], fn_id, EqualsMode::Identity)
        .unwrap();

    // Force `d` to fire so we can verify its restored output value.
    let sub = child.subscribe(rt.core(), d, std::rc::Rc::new(|_| {}));
    assert_eq!(
        binding.deref(child.cache_of(rt.core(), d)),
        serde_json::json!(107) // 7 + 100
    );
    child.unsubscribe(rt.core(), d, sub);

    let snap = root.snapshot(rt.core());

    // Restore into a fresh runtime with the same fn registered.
    let binding2 = SnapshotBinding::new();
    let fn_id2 = binding2.register_fn(|args| {
        let x = args[0].as_i64().unwrap_or(0);
        serde_json::json!(x + 100)
    });
    let binding2_for_factory = binding2.clone();

    let mut factories: IndexMap<String, NodeFactory> = IndexMap::new();
    factories.insert(
        "derived".to_owned(),
        Box::new(
            move |core: &graphrefly_core::Core,
                  graph: &Graph,
                  name: &str,
                  _slice: &NodeSlice,
                  dep_ids: &[NodeId]| {
                graph
                    .derived(core, name, dep_ids, fn_id2, EqualsMode::Identity)
                    .map_err(|_| SnapshotError::UnknownNode(name.to_owned()))
            },
        ),
    );
    // Reference binding2_for_factory so the `move` block keeps it alive
    // for the test's subsequent subscribe + cache_of reads (factory
    // captures fn_id only; the value-deref uses the binding directly).
    let _kept = binding2_for_factory;

    let rt2 = OwnedCore::new(binding2.clone() as Arc<dyn BindingBoundary>);
    let restored = Graph::from_snapshot(rt2.core(), &snap, None, Some(factories)).unwrap();

    let restored_d = restored.try_resolve("child::d").expect("child::d resolves");
    let sub2 = restored.subscribe(rt2.core(), restored_d, std::rc::Rc::new(|_| {}));
    let cache = restored.cache_of(rt2.core(), restored_d);
    assert_ne!(cache, NO_HANDLE);
    assert_eq!(binding2.deref(cache), serde_json::json!(107));
    restored.unsubscribe(rt2.core(), restored_d, sub2);
}

/// D276 (decode round-trip): a graph with a parent-to-child cross-mount
/// derived dep round-trips correctly. The pre-D276 ordering (subgraphs
/// before parent) handled THIS direction correctly, but only via the
/// `_anon_<rawid>` encoded shape — which the decode side then rejected
/// because no factory could map `_anon_*` to a real NodeId. D276's
/// owner-relative `child::name` encoding closes the loop.
#[test]
fn d276_round_trip_parent_to_child_cross_mount_dep() {
    let binding = SnapshotBinding::new();
    let initial = binding.intern(serde_json::json!(3));
    let fn_id = binding.register_fn(|args| {
        let x = args[0].as_i64().unwrap_or(0);
        serde_json::json!(x * 10)
    });
    let (rt, root) = graph_with("root", binding.clone() as Arc<dyn BindingBoundary>);

    let child = root.mount_new(rt.core(), "child").unwrap();
    let y = child.state(rt.core(), "y", Some(initial)).unwrap();
    let d = root
        .derived(rt.core(), "d", &[y], fn_id, EqualsMode::Identity)
        .unwrap();
    let sub = root.subscribe(rt.core(), d, std::rc::Rc::new(|_| {}));
    assert_eq!(
        binding.deref(root.cache_of(rt.core(), d)),
        serde_json::json!(30) // 3 * 10
    );
    root.unsubscribe(rt.core(), d, sub);

    let snap = root.snapshot(rt.core());

    // Restore.
    let binding2 = SnapshotBinding::new();
    let fn_id2 = binding2.register_fn(|args| {
        let x = args[0].as_i64().unwrap_or(0);
        serde_json::json!(x * 10)
    });
    let mut factories: IndexMap<String, NodeFactory> = IndexMap::new();
    factories.insert(
        "derived".to_owned(),
        Box::new(
            move |core: &graphrefly_core::Core,
                  graph: &Graph,
                  name: &str,
                  _slice: &NodeSlice,
                  dep_ids: &[NodeId]| {
                graph
                    .derived(core, name, dep_ids, fn_id2, EqualsMode::Identity)
                    .map_err(|_| SnapshotError::UnknownNode(name.to_owned()))
            },
        ),
    );

    let rt2 = OwnedCore::new(binding2.clone() as Arc<dyn BindingBoundary>);
    let restored = Graph::from_snapshot(rt2.core(), &snap, None, Some(factories)).unwrap();

    let restored_d = restored.try_resolve("d").expect("d resolves at root");
    let sub2 = restored.subscribe(rt2.core(), restored_d, std::rc::Rc::new(|_| {}));
    let cache = restored.cache_of(rt2.core(), restored_d);
    assert_ne!(cache, NO_HANDLE);
    assert_eq!(binding2.deref(cache), serde_json::json!(30));
    restored.unsubscribe(rt2.core(), restored_d, sub2);
}

/// D276 (decode round-trip): a 3-level mount tree where the deepest
/// derived deps on a state two mount-levels up
/// (`root.child.nested.d` deps on `root.x`). The encoder emits
/// `..::..::x` for the dep, and the decoder's `try_resolve` walks
/// twice up to the root then looks up `x`.
#[test]
fn d276_round_trip_deep_chain_across_three_mount_levels() {
    let binding = SnapshotBinding::new();
    let initial = binding.intern(serde_json::json!(5));
    let fn_id = binding.register_fn(|args| {
        let x = args[0].as_i64().unwrap_or(0);
        serde_json::json!(x - 1)
    });
    let (rt, root) = graph_with("root", binding.clone() as Arc<dyn BindingBoundary>);

    let x = root.state(rt.core(), "x", Some(initial)).unwrap();
    let child = root.mount_new(rt.core(), "child").unwrap();
    let nested = child.mount_new(rt.core(), "nested").unwrap();
    nested
        .derived(rt.core(), "d", &[x], fn_id, EqualsMode::Identity)
        .unwrap();

    let snap = root.snapshot(rt.core());
    let d = &snap.subgraphs["child"].subgraphs["nested"].nodes["d"];
    assert_eq!(d.deps, vec!["..::..::x".to_owned()]);

    // Round-trip.
    let binding2 = SnapshotBinding::new();
    let fn_id2 = binding2.register_fn(|args| {
        let x = args[0].as_i64().unwrap_or(0);
        serde_json::json!(x - 1)
    });
    let mut factories: IndexMap<String, NodeFactory> = IndexMap::new();
    factories.insert(
        "derived".to_owned(),
        Box::new(
            move |core: &graphrefly_core::Core,
                  graph: &Graph,
                  name: &str,
                  _slice: &NodeSlice,
                  dep_ids: &[NodeId]| {
                graph
                    .derived(core, name, dep_ids, fn_id2, EqualsMode::Identity)
                    .map_err(|_| SnapshotError::UnknownNode(name.to_owned()))
            },
        ),
    );

    let rt2 = OwnedCore::new(binding2.clone() as Arc<dyn BindingBoundary>);
    let restored = Graph::from_snapshot(rt2.core(), &snap, None, Some(factories)).unwrap();

    let restored_d = restored
        .try_resolve("child::nested::d")
        .expect("deep path resolves");
    let sub = restored.subscribe(rt2.core(), restored_d, std::rc::Rc::new(|_| {}));
    let cache = restored.cache_of(rt2.core(), restored_d);
    assert_ne!(cache, NO_HANDLE);
    assert_eq!(binding2.deref(cache), serde_json::json!(4)); // 5 - 1
    restored.unsubscribe(rt2.core(), restored_d, sub);
}

/// D276 back-compat: a hand-constructed snapshot using ONLY bare local
/// dep names (the pre-D276 shape) must continue to round-trip cleanly.
/// This pins the contract that the D276 encoder collapses same-graph
/// deps to bare names AND the D276 decoder resolves bare names
/// owner-locally (via `try_resolve`'s single-segment path → local
/// `names` lookup).
#[test]
fn d276_back_compat_bare_local_name_snapshot_still_restores() {
    let binding = SnapshotBinding::new();
    let rt = OwnedCore::new(binding.clone() as Arc<dyn BindingBoundary>);
    let fn_id = binding.register_fn(|args| {
        let x = args[0].as_i64().unwrap_or(0);
        serde_json::json!(x + 1)
    });

    let mut factories: IndexMap<String, NodeFactory> = IndexMap::new();
    factories.insert(
        "derived".to_owned(),
        Box::new(
            move |core: &graphrefly_core::Core,
                  graph: &Graph,
                  name: &str,
                  _slice: &NodeSlice,
                  dep_ids: &[NodeId]| {
                graph
                    .derived(core, name, dep_ids, fn_id, EqualsMode::Identity)
                    .map_err(|_| SnapshotError::UnknownNode(name.to_owned()))
            },
        ),
    );

    // Pre-D276-shape snapshot: bare local "x" dep name.
    let snap = GraphPersistSnapshot {
        name: "root".to_owned(),
        nodes: {
            let mut m = IndexMap::new();
            m.insert(
                "x".to_owned(),
                NodeSlice {
                    node_type: "state".to_owned(),
                    value: Some(serde_json::json!(42)),
                    status: NodeSnapshotStatus::Live,
                    deps: vec![],
                },
            );
            m.insert(
                "plus_one".to_owned(),
                NodeSlice {
                    node_type: "derived".to_owned(),
                    value: None,
                    status: NodeSnapshotStatus::Live,
                    deps: vec!["x".to_owned()],
                },
            );
            m
        },
        subgraphs: IndexMap::new(),
    };

    let restored = Graph::from_snapshot(rt.core(), &snap, None, Some(factories)).unwrap();
    let plus_one = restored.node("plus_one");
    let sub = restored.subscribe(rt.core(), plus_one, std::rc::Rc::new(|_| {}));
    let cache = restored.cache_of(rt.core(), plus_one);
    assert_ne!(cache, NO_HANDLE);
    assert_eq!(binding.deref(cache), serde_json::json!(43));
    restored.unsubscribe(rt.core(), plus_one, sub);
}
