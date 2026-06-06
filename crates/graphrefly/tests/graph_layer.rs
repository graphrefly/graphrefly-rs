use std::cell::{Cell, RefCell};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;

use graphrefly::{
    default_dispatcher, default_restore_registry, describe_to_ascii, describe_to_d2,
    describe_to_d2_with_direction, describe_to_json, describe_to_mermaid, describe_to_mermaid_url,
    describe_to_mermaid_with_direction, describe_to_pretty, distinct_until_changed, explain_path,
    filter, from_iter, graph, graph_opts, map, merge, reachable, restore_graph, restore_registry,
    scan, take, timeout, validate_no_islands, DescribeEdge, DescribeNode, DescribeOpts,
    DescribeSnapshot, DescribeValue, DiagramDirection, Dispatcher, Explain, ExplainPathOptions,
    ExplainPathReason, GraphCheckpointEdge, GraphCheckpointFactory, GraphCheckpointJson,
    GraphCheckpointValue, GraphNodeOpts, GraphOptions, GraphRestoreDefinition, GraphRestoreEntry,
    IslandReport, LockId, Message, NodeOpts, Pausable, ReachableDirection, ReachableOptions,
    RestoreFactoryMeta, RestoreGraphOptions, Status, Tier, Values, GRAPH_CHECKPOINT_VERSION,
};
use serde_json::json;

fn last_or_prev(values: &Values<'_>, i: usize) -> Option<Rc<i32>> {
    values
        .batches::<i32>(i)
        .last()
        .and_then(|wave| wave.last().cloned())
        .or_else(|| values.prev::<i32>(i))
}

fn json_cache(node: graphrefly::GraphNode) -> GraphCheckpointJson {
    node.cache_any()
        .expect("node has cached DATA")
        .downcast::<GraphCheckpointJson>()
        .expect("restored DATA is JSON")
        .as_ref()
        .clone()
}

#[test]
fn graph_state_derived_effect_and_batch_are_graph_owned() {
    let g = graph();
    let count = g.state_opts(0i32, GraphNodeOpts::named("count"));
    let doubled = g.derived_opts(
        vec![count.erased()],
        |values| Some(*last_or_prev(values, 0).unwrap() * 2),
        GraphNodeOpts::named("doubled"),
    );
    let seen = Rc::new(RefCell::new(Vec::new()));
    let cleanups = Rc::new(Cell::new(0usize));
    let effect_seen = seen.clone();
    let effect_cleanups = cleanups.clone();
    let effect = g.effect_opts(
        vec![doubled.erased()],
        move |values| {
            effect_seen
                .borrow_mut()
                .push(*last_or_prev(values, 0).unwrap());
            let cleanups = effect_cleanups.clone();
            Some(Box::new(move || cleanups.set(cleanups.get() + 1)))
        },
        GraphNodeOpts::named("effect"),
    );

    let effect_unsub = effect.subscribe(|_| {});
    assert_eq!(doubled.cache(), Some(0));
    assert_eq!(*seen.borrow(), vec![0]);

    g.batch(|_| {
        count.set(1);
        count.set(2);
        assert_eq!(doubled.cache(), Some(0));
    });

    assert_eq!(doubled.cache(), Some(4));
    assert_eq!(*seen.borrow(), vec![0, 4]);
    effect_unsub();
    assert_eq!(cleanups.get(), 1);
}

#[test]
fn graph_producer_node_find_describe_and_observe_first_cut() {
    let g = graph_opts(GraphOptions::named("demo"));
    let source = g.producer_opts::<i32, _>(|ctx| ctx.emit(7i32), GraphNodeOpts::named("source"));
    let raw = g.node_opts::<i32, _>(
        vec![source.erased()],
        |ctx| ctx.emit(*ctx.data::<i32>(0).unwrap() + 1),
        GraphNodeOpts::named("raw"),
    );

    let found = g.find("source").expect("registered source is findable");
    assert_eq!(found.status(), Status::Sentinel);

    let observed = Rc::new(RefCell::new(Vec::new()));
    let observed_sink = observed.clone();
    let observer = g
        .observe()
        .subscribe(move |event| observed_sink.borrow_mut().push(event));
    assert_eq!(raw.cache(), Some(8));
    assert_eq!(*found.cache_any().unwrap().downcast::<i32>().unwrap(), 7i32);

    let events = observed.borrow();
    assert!(events
        .iter()
        .any(|e| e.path == "raw" && e.msg.kind() == "DATA" && e.tier == Tier::Value));
    assert!(events.iter().any(|e| {
        e.path == "source"
            && matches!(&e.msg, graphrefly::ObserveMessage::Data(v) if v.as_ref().downcast_ref::<i32>() == Some(&7))
    }));
    assert!(
        events.windows(2).all(|pair| pair[0].seq < pair[1].seq),
        "observe seq is graph-local and monotonic"
    );

    let snap = g.describe();
    assert_eq!(snap.name.as_deref(), Some("demo"));
    let source_node = snap.nodes.iter().find(|n| n.id == "source").unwrap();
    let raw_node = snap.nodes.iter().find(|n| n.id == "raw").unwrap();
    assert_eq!(source_node.factory, "producer");
    assert_eq!(source_node.status, Status::Settled);
    assert_eq!(source_node.value, Some(DescribeValue::I64(7)));
    assert_eq!(raw_node.factory, "node");
    assert_eq!(raw_node.deps, vec!["source"]);
    assert_eq!(raw_node.value, Some(DescribeValue::I64(8)));
    assert!(snap.edges.contains(&DescribeEdge {
        from: "source".to_owned(),
        to: "raw".to_owned(),
    }));
    observer.unsubscribe();
}

#[test]
fn graph_diagnostics_reachable_walks_deterministically() {
    let g = graph();
    let a = g.state_opts(1i32, GraphNodeOpts::named("a"));
    let b = g.derived_opts(
        vec![a.erased()],
        |values| Some(*last_or_prev(values, 0).unwrap() + 1),
        GraphNodeOpts::named("b"),
    );
    let _c = g.derived_opts(
        vec![b.erased()],
        |values| Some(*last_or_prev(values, 0).unwrap() + 1),
        GraphNodeOpts::named("c"),
    );
    let sink = g.effect_opts(vec![a.erased()], |_| None, GraphNodeOpts::named("sink"));
    let _keep_active = sink.subscribe(|_| {});

    let snap = g.describe();
    assert_eq!(
        reachable(
            &snap,
            "c",
            ReachableDirection::Upstream,
            ReachableOptions::default()
        )
        .paths,
        vec!["a".to_owned(), "b".to_owned()]
    );
    assert_eq!(
        reachable(
            &snap,
            "a",
            ReachableDirection::Downstream,
            ReachableOptions::default()
        )
        .paths,
        vec!["b".to_owned(), "c".to_owned(), "sink".to_owned()]
    );
    assert_eq!(
        reachable(
            &snap,
            "b",
            ReachableDirection::Upstream,
            ReachableOptions {
                both: true,
                ..ReachableOptions::default()
            },
        )
        .paths,
        vec!["a".to_owned(), "c".to_owned(), "sink".to_owned()]
    );

    let detailed = reachable(
        &snap,
        "a",
        ReachableDirection::Downstream,
        ReachableOptions {
            max_depth: Some(1),
            both: false,
        },
    );
    assert_eq!(detailed.paths, vec!["b".to_owned(), "sink".to_owned()]);
    assert_eq!(detailed.depths.get("b"), Some(&1));
    assert_eq!(detailed.depths.get("sink"), Some(&1));
    assert!(detailed.truncated);

    let zero = reachable(
        &snap,
        "a",
        ReachableDirection::Downstream,
        ReachableOptions {
            max_depth: Some(0),
            both: false,
        },
    );
    assert!(zero.paths.is_empty());
    assert!(zero.truncated);
}

#[test]
fn graph_diagnostics_explain_path_and_islands_are_pure_snapshot_helpers() {
    let g = graph();
    let a = g.state_opts(1i32, GraphNodeOpts::named("a"));
    let b = g.derived_opts(
        vec![a.erased()],
        |values| Some(*last_or_prev(values, 0).unwrap() + 1),
        GraphNodeOpts::named("b"),
    );
    let _c = g.derived_opts(
        vec![b.erased()],
        |values| Some(*last_or_prev(values, 0).unwrap() + 1),
        GraphNodeOpts::named("c"),
    );

    let snap = g.describe();
    let chain = explain_path(&snap, "a", "c", ExplainPathOptions::default());
    assert!(chain.found);
    assert_eq!(chain.reason, ExplainPathReason::Ok);
    assert_eq!(
        chain
            .steps
            .iter()
            .map(|s| s.id.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b", "c"]
    );
    assert_eq!(
        chain.steps.iter().map(|s| s.hop).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(chain.steps[0].factory, "state");
    assert_eq!(chain.steps[0].dep_index, Some(0));
    assert_eq!(chain.steps[1].factory, "derived");
    assert_eq!(chain.steps[1].dep_index, Some(0));
    assert_eq!(chain.text, "a -> b -> c");
    assert_eq!(
        explain_path(&snap, "missing", "c", ExplainPathOptions::default()).reason,
        ExplainPathReason::NoSuchFrom
    );
    assert_eq!(
        explain_path(&snap, "a", "missing", ExplainPathOptions::default()).reason,
        ExplainPathReason::NoSuchTo
    );
    assert_eq!(
        explain_path(
            &snap,
            "c",
            "a",
            ExplainPathOptions {
                max_depth: Some(1),
                find_cycle: false,
            },
        )
        .reason,
        ExplainPathReason::NoPath
    );

    let parent = graph();
    let root = parent.state_opts(0i32, GraphNodeOpts::named("root"));
    parent.derived_opts(
        vec![root.erased()],
        |values| last_or_prev(values, 0).map(|v| *v),
        GraphNodeOpts::named("rootD"),
    );
    let child = graph();
    let leaf = child.state_opts(1i32, GraphNodeOpts::named("leaf"));
    child.derived_opts(
        vec![leaf.erased()],
        |values| last_or_prev(values, 0).map(|v| *v),
        GraphNodeOpts::named("leafD"),
    );
    child.state_opts(9i32, GraphNodeOpts::named("orphan"));
    parent.mount(child, "child");

    let islands = validate_no_islands(&parent.describe());
    assert!(!islands.ok);
    assert_eq!(
        islands.orphans,
        vec![IslandReport {
            id: "child::orphan".to_owned(),
            factory: "state".to_owned()
        }]
    );
    assert_eq!(
        islands.summary(),
        "validate_no_islands: 1 island node(s) - child::orphan (state)"
    );
}

#[test]
fn graph_diagnostics_accept_synthetic_snapshots_and_cycles() {
    let snap = DescribeSnapshot {
        name: None,
        nodes: vec![
            DescribeNode {
                id: "x".to_owned(),
                name: None,
                factory: "state".to_owned(),
                status: Status::Sentinel,
                value: None,
                deps: vec![],
                meta: None,
            },
            DescribeNode {
                id: "y".to_owned(),
                name: None,
                factory: "derived".to_owned(),
                status: Status::Sentinel,
                value: None,
                deps: vec![],
                meta: None,
            },
        ],
        edges: vec![DescribeEdge {
            from: "x".to_owned(),
            to: "y".to_owned(),
        }],
        subgraphs: None,
    };
    assert_eq!(
        reachable(
            &snap,
            "x",
            ReachableDirection::Downstream,
            ReachableOptions::default()
        )
        .paths,
        vec!["y".to_owned()]
    );
    assert_eq!(
        explain_path(&snap, "x", "y", ExplainPathOptions::default())
            .steps
            .iter()
            .map(|s| s.id.as_str())
            .collect::<Vec<_>>(),
        vec!["x", "y"]
    );
    assert!(validate_no_islands(&snap).ok);

    let dangling = DescribeSnapshot {
        name: None,
        nodes: vec![
            DescribeNode {
                id: "x".to_owned(),
                name: None,
                factory: "state".to_owned(),
                status: Status::Sentinel,
                value: None,
                deps: vec!["missing".to_owned()],
                meta: None,
            },
            DescribeNode {
                id: "y".to_owned(),
                name: None,
                factory: "derived".to_owned(),
                status: Status::Sentinel,
                value: None,
                deps: vec![],
                meta: None,
            },
        ],
        edges: vec![
            DescribeEdge {
                from: "x".to_owned(),
                to: "missing".to_owned(),
            },
            DescribeEdge {
                from: "missing".to_owned(),
                to: "y".to_owned(),
            },
        ],
        subgraphs: None,
    };
    assert!(reachable(
        &dangling,
        "x",
        ReachableDirection::Downstream,
        ReachableOptions::default()
    )
    .paths
    .is_empty());
    assert_eq!(
        explain_path(&dangling, "x", "y", ExplainPathOptions::default()).reason,
        ExplainPathReason::NoPath
    );

    let cycle = DescribeSnapshot {
        name: None,
        nodes: vec![
            DescribeNode {
                id: "a".to_owned(),
                name: None,
                factory: "node".to_owned(),
                status: Status::Sentinel,
                value: None,
                deps: vec!["b".to_owned()],
                meta: None,
            },
            DescribeNode {
                id: "b".to_owned(),
                name: None,
                factory: "node".to_owned(),
                status: Status::Sentinel,
                value: None,
                deps: vec!["a".to_owned()],
                meta: None,
            },
        ],
        edges: vec![
            DescribeEdge {
                from: "b".to_owned(),
                to: "a".to_owned(),
            },
            DescribeEdge {
                from: "a".to_owned(),
                to: "b".to_owned(),
            },
        ],
        subgraphs: None,
    };
    assert_eq!(
        explain_path(
            &cycle,
            "a",
            "a",
            ExplainPathOptions {
                max_depth: None,
                find_cycle: true,
            },
        )
        .steps
        .iter()
        .map(|s| s.id.as_str())
        .collect::<Vec<_>>(),
        vec!["a", "b", "a"]
    );
    assert_eq!(
        explain_path(
            &cycle,
            "a",
            "a",
            ExplainPathOptions {
                max_depth: Some(1),
                find_cycle: true,
            },
        )
        .reason,
        ExplainPathReason::MaxDepthExceeded
    );
}

#[test]
fn checkpoint_uses_neutral_schema_and_restores_fresh_json_state() {
    let g = graph_opts(GraphOptions::named("restore-demo"));
    let count = g.state_opts(2i32, GraphNodeOpts::named("count"));
    let _keep_active = count.subscribe(|_| {});

    let checkpoint = g.checkpoint().expect("checkpoint succeeds");
    assert_eq!(checkpoint.version, GRAPH_CHECKPOINT_VERSION);
    assert_eq!(checkpoint.name.as_deref(), Some("restore-demo"));
    let count_checkpoint = checkpoint
        .nodes
        .iter()
        .find(|node| node.id == "count")
        .expect("count checkpoint exists");
    assert_eq!(
        count_checkpoint.value,
        GraphCheckpointValue::Data { data: json!(2) }
    );
    serde_json::to_string(&checkpoint).expect("checkpoint is JSON serializable");

    let restored = restore_graph(
        checkpoint,
        RestoreGraphOptions::new(default_restore_registry()),
    )
    .expect("fresh restore succeeds");
    let restored_count = restored.find("count").expect("restored node is registered");
    assert_eq!(restored_count.status(), Status::Settled);
    assert_eq!(json_cache(restored_count.clone()), json!(2));

    let seen = Rc::new(RefCell::new(Vec::new()));
    let seen_sink = seen.clone();
    let _unsub = restored_count.subscribe(move |msg| {
        if let Message::Data(value) = msg {
            seen_sink.borrow_mut().push(
                value
                    .as_ref()
                    .downcast_ref::<GraphCheckpointJson>()
                    .cloned(),
            );
        }
    });
    assert_eq!(*seen.borrow(), vec![Some(json!(2))]);
}

#[test]
fn restore_registry_definitions_rebuild_declared_json_map_nodes() {
    let g = graph();
    let source = g.state_opts(json!(3), GraphNodeOpts::named("source"));
    let mut opts = GraphNodeOpts::named("double");
    opts.restore =
        Some(RestoreFactoryMeta::registry_ref("map").with_config(json!({ "fn": "double-json" })));
    let doubled = g.node_opts::<GraphCheckpointJson, _>(
        vec![source.erased()],
        |ctx| {
            for value in ctx.batch::<GraphCheckpointJson>(0) {
                ctx.emit(json!(value.as_i64().expect("number input") * 2));
            }
        },
        opts,
    );
    let _keep_active = doubled.subscribe(|_| {});
    let checkpoint = g.checkpoint().expect("checkpoint succeeds");

    let registry = restore_registry([
        GraphRestoreEntry::descriptor(graphrefly::StateRestoreDescriptor),
        GraphRestoreEntry::descriptor(graphrefly::MapJsonRestoreDescriptor),
        GraphRestoreEntry::definition(GraphRestoreDefinition::json("double-json", |value| {
            Ok(json!(value.as_i64().expect("number input") * 2))
        })),
    ]);
    let restored = restore_graph(checkpoint, RestoreGraphOptions::new(registry))
        .expect("fresh restore succeeds");

    assert_eq!(
        json_cache(restored.find("source").expect("source restored")),
        json!(3)
    );
    assert_eq!(
        json_cache(restored.find("double").expect("double restored")),
        json!(6)
    );
    let snap = restored.describe();
    assert!(snap.edges.contains(&DescribeEdge {
        from: "source".to_owned(),
        to: "double".to_owned(),
    }));
}

#[test]
fn checkpoint_auto_discovers_unregistered_live_deps_as_local_only() {
    let g = graph_opts(GraphOptions::named("synthetic-checkpoint"));
    let source = g.state_empty_opts::<i32>(GraphNodeOpts::named("source"));
    let timed = timeout(&source, 10);
    g.init_node(
        map::<i32, i32>(|v| *v),
        vec![timed.erased()],
        GraphNodeOpts::named("sink"),
    );

    let checkpoint = g.checkpoint().expect("checkpoint succeeds");
    let timeout_node = checkpoint
        .nodes
        .iter()
        .find(|node| {
            node.factory
                == (GraphCheckpointFactory::LocalOnly {
                    name: "timeout".to_owned(),
                    reason: "node is an unregistered live dependency auto-discovered from topology"
                        .to_owned(),
                })
        })
        .expect("timeout helper is checkpointed as local-only");
    let timer_node = checkpoint
        .nodes
        .iter()
        .find(|node| node.id.starts_with("~timer#"))
        .expect("transitive timer helper is checkpointed");

    assert!(timeout_node.id.starts_with("~timeout#"));
    assert!(timeout_node.deps.contains(&"source".to_owned()));
    assert!(timeout_node.deps.contains(&timer_node.id));
    assert!(checkpoint.edges.contains(&GraphCheckpointEdge {
        from: timeout_node.id.clone(),
        to: "sink".to_owned(),
    }));
    assert!(checkpoint.edges.contains(&GraphCheckpointEdge {
        from: timer_node.id.clone(),
        to: timeout_node.id.clone(),
    }));
}

#[test]
fn restored_explicit_hash_ids_advance_unnamed_node_sequence() {
    let g = graph();
    let source = g.state_opts(json!(1), GraphNodeOpts::default());
    let _keep_active = source.subscribe(|_| {});

    let restored = restore_graph(
        g.checkpoint().expect("checkpoint succeeds"),
        RestoreGraphOptions::new(default_restore_registry()),
    )
    .expect("restore succeeds");
    restored.state_opts(json!(2), GraphNodeOpts::default());

    assert!(restored.find("state#0").is_some());
    assert!(restored.find("state#1").is_some());
}

#[test]
fn restore_rejects_local_only_factories_honestly() {
    let g = graph();
    let source = g.state_opts(1i32, GraphNodeOpts::named("source"));
    let derived = g.derived_opts(
        vec![source.erased()],
        |values| Some(*last_or_prev(values, 0).unwrap() + 1),
        GraphNodeOpts::named("local"),
    );
    let _keep_active = derived.subscribe(|_| {});

    let checkpoint = g.checkpoint().expect("checkpoint succeeds");
    let err = match restore_graph(
        checkpoint,
        RestoreGraphOptions::new(default_restore_registry()),
    ) {
        Ok(_) => panic!("local-only derived restore must fail"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("local-only"));
}

#[test]
fn describe_mount_prefixes_child_graph() {
    let parent = graph_opts(GraphOptions::named("parent"));
    parent.state_opts(1i32, GraphNodeOpts::named("root"));
    let child = graph_opts(GraphOptions::named("child"));
    child.state_opts(2i32, GraphNodeOpts::named("leaf"));

    parent.mount(child, "sub");

    let snap = parent.describe();
    let sub = snap.subgraphs.unwrap().into_iter().next().unwrap();
    assert!(sub.nodes.iter().any(|n| n.id == "sub::leaf"));
}

#[test]
#[should_panic(expected = "duplicate mount path")]
fn mount_rejects_duplicate_paths() {
    let parent = graph();
    parent.mount(graph(), "sub");
    parent.mount(graph(), "sub");
}

#[test]
#[should_panic(expected = "graph cycle")]
fn mount_rejects_cycles() {
    let parent = graph();
    parent.mount(parent.clone(), "self");
}

#[test]
fn observe_traverses_mounted_graphs_with_prefixed_paths() {
    let parent = graph();
    let child = graph();
    let leaf = child.state_opts(2i32, GraphNodeOpts::named("leaf"));
    parent.mount(child, "sub");

    let observed = Rc::new(RefCell::new(Vec::new()));
    let observed_sink = observed.clone();
    let observer = parent
        .observe_path("sub::leaf")
        .subscribe(move |event| observed_sink.borrow_mut().push(event));

    assert!(observed.borrow().iter().any(|e| {
        e.path == "sub::leaf"
            && matches!(&e.msg, graphrefly::ObserveMessage::Data(v) if v.as_ref().downcast_ref::<i32>() == Some(&2))
    }));
    observed.borrow_mut().clear();

    leaf.set(3);
    assert!(observed.borrow().iter().any(|e| {
        e.path == "sub::leaf"
            && matches!(&e.msg, graphrefly::ObserveMessage::Data(v) if v.as_ref().downcast_ref::<i32>() == Some(&3))
    }));
    observer.unsubscribe();
}

#[test]
fn observe_cleans_partial_subscriptions_when_later_target_panics() {
    let g = graph();
    let live = g.state_opts(1i32, GraphNodeOpts::named("live"));
    let terminal = g.state_opts(9i32, GraphNodeOpts::named("terminal"));
    let _keep_terminal_active = terminal.subscribe(|_| {});
    terminal.down(vec![Message::Complete]);

    let observed = Rc::new(RefCell::new(Vec::new()));
    let observed_sink = observed.clone();
    let result = catch_unwind(AssertUnwindSafe(|| {
        let _observer = g
            .observe()
            .subscribe(move |event| observed_sink.borrow_mut().push(event));
    }));
    assert!(result.is_err());
    let after_panic = observed.borrow().len();

    live.set(2);
    assert_eq!(
        observed.borrow().len(),
        after_panic,
        "already-created observe subscriptions are cleaned up on panic"
    );
}

#[test]
#[should_panic(expected = "different graph")]
fn graph_rejects_cross_graph_deps_by_arena_identity() {
    let g1 = graph();
    let g2 = graph();
    let source = g1.state(1i32);

    let _bad = g2.derived::<i32, _>(vec![source.erased()], |values| {
        Some(*last_or_prev(values, 0).unwrap())
    });
}

#[test]
#[should_panic(expected = "different graph")]
fn graph_rejects_bare_substrate_deps() {
    let g = graph();
    let bare = graphrefly::Node::<i32>::state(1);

    let _bad = g.derived::<i32, _>(vec![bare.erased()], |values| {
        Some(*last_or_prev(values, 0).unwrap())
    });
}

#[test]
fn derived_values_expose_prev_and_wave_batches_without_raw_ctx_or_sampling_helpers() {
    let g = graph();
    let source = g.state_opts(1i32, GraphNodeOpts::named("source"));
    let seen_wave_batches = Rc::new(RefCell::new(Vec::new()));
    let seen_prev = Rc::new(RefCell::new(Vec::new()));
    let wave_batches = seen_wave_batches.clone();
    let prev = seen_prev.clone();
    let sum = g.derived_opts(
        vec![source.erased()],
        move |values| {
            assert_eq!(values.len(), 1);
            wave_batches.borrow_mut().push(
                values
                    .batches::<i32>(0)
                    .iter()
                    .map(|wave| wave.iter().map(|v| **v).collect::<Vec<_>>())
                    .collect::<Vec<_>>(),
            );
            prev.borrow_mut().push(values.prev::<i32>(0).map(|v| *v));
            Some(
                values
                    .batches::<i32>(0)
                    .iter()
                    .flat_map(|wave| wave.iter())
                    .map(|v| **v)
                    .sum::<i32>(),
            )
        },
        GraphNodeOpts::named("sum"),
    );
    let _keep_alive = sum.subscribe(|_| {});
    assert_eq!(sum.cache(), Some(1));
    assert_eq!(*seen_wave_batches.borrow(), vec![vec![vec![1]]]);
    assert_eq!(*seen_prev.borrow(), vec![None]);

    source.down(vec![
        Message::Data(Rc::new(2i32)),
        Message::Data(Rc::new(3i32)),
    ]);

    assert_eq!(sum.cache(), Some(5));
    assert_eq!(
        *seen_wave_batches.borrow(),
        vec![vec![vec![1]], vec![vec![2, 3]]]
    );
    assert_eq!(*seen_prev.borrow(), vec![None, Some(1)]);

    let pause = LockId::new("coalesce");
    sum.up(vec![Message::Pause(pause.clone())]);
    source.down(vec![Message::Data(Rc::new(4i32))]);
    source.down(vec![
        Message::Data(Rc::new(5i32)),
        Message::Data(Rc::new(6i32)),
    ]);
    assert_eq!(sum.cache(), Some(5));
    sum.up(vec![Message::Resume(pause)]);

    assert_eq!(sum.cache(), Some(15));
    assert_eq!(
        *seen_wave_batches.borrow(),
        vec![vec![vec![1]], vec![vec![2, 3]], vec![vec![4], vec![5, 6]]]
    );
    assert_eq!(*seen_prev.borrow(), vec![None, Some(1), Some(3)]);
}

#[test]
#[should_panic(expected = "different graph")]
fn substrate_derived_rejects_cross_arena_deps() {
    let g = graph();
    let graph_owned = g.state(1i32);

    let _bad = graphrefly::Node::<i32>::derived(vec![graph_owned.erased()], |ctx| {
        ctx.emit(*ctx.data::<i32>(0).unwrap())
    });
}

#[test]
#[should_panic(expected = "different graph")]
fn substrate_rewire_rejects_cross_arena_deps() {
    let g1 = graph();
    let g2 = graph();
    let source = g1.state(1i32);
    let target = g2.derived::<i32, _>(vec![], |_| Some(0));

    target.replace_deps(vec![source.erased()], |ctx| {
        ctx.emit(*ctx.data::<i32>(0).unwrap())
    });
}

#[test]
fn state_empty_describe_omits_sentinel_value() {
    let g = graph();
    let state = g.state_empty_opts::<i32>(GraphNodeOpts::named("empty"));
    let _keep_alive = state.subscribe(|_| {});

    let snap = g.describe();
    let empty = snap.nodes.iter().find(|n| n.id == "empty").unwrap();
    assert_eq!(empty.value, None);
    assert_eq!(empty.status, Status::Sentinel);

    state.down(vec![Message::Data(Rc::new(3i32))]);
    let snap = g.describe();
    let empty = snap.nodes.iter().find(|n| n.id == "empty").unwrap();
    assert_eq!(empty.value, Some(DescribeValue::I64(3)));
}

#[test]
fn describe_explain_and_renderers_are_pure_snapshot_functions() {
    let g = graph_opts(GraphOptions::named("render-demo"));
    let source = g.state_opts(1i32, GraphNodeOpts::named("source"));
    let mapped = g.derived_opts(
        vec![source.erased()],
        |values| Some(*last_or_prev(values, 0).unwrap() + 1),
        GraphNodeOpts::named("mapped"),
    );
    let sink = g.derived_opts(
        vec![mapped.erased()],
        |values| Some(*last_or_prev(values, 0).unwrap() * 10),
        GraphNodeOpts::named("sink"),
    );
    g.state_opts(99i32, GraphNodeOpts::named("unrelated"));
    let _keep_alive = sink.subscribe(|_| {});

    let explain = g.describe_opts(DescribeOpts {
        explain: Some(Explain {
            from: "source".to_owned(),
            to: "sink".to_owned(),
        }),
    });
    assert!(explain.nodes.iter().any(|n| n.id == "source"));
    assert!(explain.nodes.iter().any(|n| n.id == "mapped"));
    assert!(explain.nodes.iter().any(|n| n.id == "sink"));
    assert!(!explain.nodes.iter().any(|n| n.id == "unrelated"));

    let snap = g.describe();
    assert!(describe_to_mermaid(&snap).contains("flowchart LR"));
    assert!(describe_to_d2(&snap).contains("direction: right"));
    assert!(describe_to_pretty(&snap).contains("source (state/Settled): 1"));
    assert!(describe_to_ascii(&snap, true).contains("source [state/Settled 1] -> mapped"));
    assert!(describe_to_json(&snap).contains("\"id\":\"source\""));
    assert!(describe_to_mermaid_url(&snap).starts_with("https://mermaid.live/edit#base64:"));
}

#[test]
fn renderers_respect_direction_and_ascii_value_toggle() {
    let g = graph_opts(GraphOptions::named("render-options"));
    let source = g.state_opts(1i32, GraphNodeOpts::named("source"));
    let sink = g.derived_opts(
        vec![source.erased()],
        |values| Some(*last_or_prev(values, 0).unwrap() + 1),
        GraphNodeOpts::named("sink"),
    );
    let _keep_alive = sink.subscribe(|_| {});
    let snap = g.describe();

    assert!(
        describe_to_mermaid_with_direction(&snap, DiagramDirection::Td).starts_with("flowchart TD")
    );
    assert!(
        describe_to_mermaid_with_direction(&snap, DiagramDirection::Bt).starts_with("flowchart BT")
    );
    assert!(
        describe_to_mermaid_with_direction(&snap, DiagramDirection::Rl).starts_with("flowchart RL")
    );

    assert!(
        describe_to_d2_with_direction(&snap, DiagramDirection::Td).starts_with("direction: down")
    );
    assert!(describe_to_d2_with_direction(&snap, DiagramDirection::Bt).starts_with("direction: up"));
    assert!(
        describe_to_d2_with_direction(&snap, DiagramDirection::Rl).starts_with("direction: left")
    );

    let without_values = describe_to_ascii(&snap, false);
    let with_values = describe_to_ascii(&snap, true);
    assert!(without_values.contains("source [state/Settled] -> sink"));
    assert!(!without_values.contains("source [state/Settled 1] -> sink"));
    assert!(with_values.contains("source [state/Settled 1] -> sink"));
}

#[test]
fn describe_includes_discovered_same_graph_deps_with_synthetic_ids() {
    let g = graph_opts(GraphOptions::named("synthetic-demo"));
    let source = g.state_empty_opts::<i32>(GraphNodeOpts::named("source"));
    let timed = timeout(&source, 10);
    let sink = g.init_node(
        map::<i32, i32>(|v| *v),
        vec![timed.erased()],
        GraphNodeOpts::named("sink"),
    );
    let _keep_alive = sink.subscribe(|_| {});

    let snap = g.describe();
    let timeout_nodes = snap
        .nodes
        .iter()
        .filter(|node| node.factory == "timeout" && node.id.starts_with("~timeout#"))
        .collect::<Vec<_>>();
    assert_eq!(
        timeout_nodes.len(),
        1,
        "unregistered same-graph helper deps should be discovered exactly once"
    );
    let timeout_id = timeout_nodes[0].id.clone();
    let timer_nodes = snap
        .nodes
        .iter()
        .filter(|node| node.factory == "timer" && node.id.starts_with("~timer#"))
        .collect::<Vec<_>>();
    assert_eq!(
        timer_nodes.len(),
        1,
        "transitive helper deps should be discovered exactly once"
    );
    let timer_id = timer_nodes[0].id.clone();
    assert!(timeout_nodes[0].deps.contains(&"source".to_owned()));
    assert!(timeout_nodes[0].deps.contains(&timer_id));
    assert!(snap.edges.contains(&DescribeEdge {
        from: "source".to_owned(),
        to: timeout_id.clone(),
    }));
    assert!(snap.edges.contains(&DescribeEdge {
        from: timer_id,
        to: timeout_id.clone(),
    }));
    assert!(snap.edges.contains(&DescribeEdge {
        from: timeout_id.clone(),
        to: "sink".to_owned(),
    }));

    let mermaid = describe_to_mermaid(&snap);
    let ascii = describe_to_ascii(&snap, false);
    assert!(mermaid.contains("\"source\""));
    assert!(mermaid.contains("\"sink\""));
    assert!(ascii.contains("source [state/"));
    assert!(ascii.contains("sink [map/"));
    assert!(
        ascii.contains("~timeout#"),
        "renderers should include the same visible synthetic helper path set"
    );
}

#[test]
fn explain_filters_mounted_subgraphs_without_leaking_unrelated_nodes() {
    let parent = graph_opts(GraphOptions::named("parent"));
    parent.state_opts(99i32, GraphNodeOpts::named("root_unrelated"));
    let child = graph_opts(GraphOptions::named("child"));
    let source = child.state_opts(1i32, GraphNodeOpts::named("source"));
    let sink = child.derived_opts(
        vec![source.erased()],
        |values| Some(*last_or_prev(values, 0).unwrap() + 1),
        GraphNodeOpts::named("sink"),
    );
    child.state_opts(5i32, GraphNodeOpts::named("child_unrelated"));
    let _keep_alive = sink.subscribe(|_| {});
    parent.mount(child, "sub");

    let explain = parent.describe_opts(DescribeOpts {
        explain: Some(Explain {
            from: "sub::source".to_owned(),
            to: "sub::sink".to_owned(),
        }),
    });

    assert_eq!(explain.subgraphs, None);
    let ids = explain
        .nodes
        .iter()
        .map(|node| node.id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["sub::source", "sub::sink"]);
    assert_eq!(
        explain.edges,
        vec![DescribeEdge {
            from: "sub::source".to_owned(),
            to: "sub::sink".to_owned()
        }]
    );
}

#[test]
fn profile_enabled_graph_defaults_to_default_dispatcher_and_injection_isolates() {
    let default = default_dispatcher();
    default.set_recording(false);

    let g = graph_opts(GraphOptions {
        name: Some("default-profile".to_owned()),
        profile: true,
        ..GraphOptions::default()
    });
    let source = g.state_opts(1i32, GraphNodeOpts::named("source"));
    let doubled = g.derived_opts(
        vec![source.erased()],
        |values| Some(*last_or_prev(values, 0).unwrap() * 2),
        GraphNodeOpts::named("doubled"),
    );
    let _keep_alive = doubled.subscribe(|_| {});
    source.set(2);
    assert!(
        g.profile().nodes["doubled"].invokes >= 2,
        "profile:true records through the default dispatcher when no dispatcher is injected"
    );

    default.set_recording(false);

    let injected = Dispatcher::new();
    let g = graph_opts(GraphOptions {
        name: Some("injected-profile".to_owned()),
        profile: true,
        dispatcher: Some(injected.clone()),
        ..GraphOptions::default()
    });
    let source = g.state_opts(1i32, GraphNodeOpts::named("source"));
    let doubled = g.derived_opts(
        vec![source.erased()],
        |values| Some(*last_or_prev(values, 0).unwrap() * 2),
        GraphNodeOpts::named("doubled"),
    );
    let _keep_alive = doubled.subscribe(|_| {});
    source.set(2);

    let profile = g.profile();
    assert_eq!(profile.nodes["source"].invokes, 0);
    assert!(profile.nodes["doubled"].invokes >= 2);

    let unprofiled_default = graph_opts(GraphOptions::named("unprofiled-default"));
    let source = unprofiled_default.state_opts(1i32, GraphNodeOpts::named("source"));
    let doubled = unprofiled_default.derived_opts(
        vec![source.erased()],
        |values| Some(*last_or_prev(values, 0).unwrap() * 2),
        GraphNodeOpts::named("doubled"),
    );
    let _keep_alive = doubled.subscribe(|_| {});
    source.set(2);
    assert_eq!(
        unprofiled_default.profile().nodes["doubled"].invokes,
        0,
        "explicit injection records on the injected dispatcher, not the default"
    );
}

#[test]
fn profile_counts_graph_registered_invokes_when_enabled() {
    let g = graph_opts(GraphOptions {
        name: Some("profile-demo".to_owned()),
        profile: true,
        ..GraphOptions::default()
    });
    let source = g.state_opts(1i32, GraphNodeOpts::named("source"));
    let doubled = g.derived_opts(
        vec![source.erased()],
        |values| Some(*last_or_prev(values, 0).unwrap() * 2),
        GraphNodeOpts::named("doubled"),
    );
    let _keep_alive = doubled.subscribe(|_| {});
    source.set(2);

    let profile = g.profile();
    assert!(profile.total_invokes >= 2);
    assert_eq!(profile.nodes["source"].invokes, 0);
    assert!(profile.nodes["doubled"].invokes >= 2);
    assert_eq!(profile.nodes["doubled"].status, Status::Settled);
}

#[test]
fn profile_uses_mount_aware_paths_and_counts_panicking_invokes() {
    let parent = graph_opts(GraphOptions {
        name: Some("profile-parent".to_owned()),
        profile: true,
        ..GraphOptions::default()
    });
    let child = graph();
    let leaf = child.state_opts(1i32, GraphNodeOpts::named("leaf"));
    let doubled = child.derived_opts(
        vec![leaf.erased()],
        |values| Some(*last_or_prev(values, 0).unwrap() * 2),
        GraphNodeOpts::named("doubled"),
    );
    parent.mount(child, "sub");
    let _keep_doubled = doubled.subscribe(|_| {});

    let profile = parent.profile();
    assert!(profile.nodes.contains_key("sub::leaf"));
    assert!(profile.nodes["sub::doubled"].invokes >= 1);

    let panics = graph_opts(GraphOptions {
        name: Some("panic-profile".to_owned()),
        profile: true,
        ..GraphOptions::default()
    });
    let bad = panics.producer_opts::<i32, _>(|_| panic!("boom"), GraphNodeOpts::named("bad"));
    let result = catch_unwind(AssertUnwindSafe(|| {
        let _keep_bad = bad.subscribe(|_| {});
    }));
    assert!(
        result.is_ok(),
        "D30 catch boundary converts the user panic to ERROR"
    );
    let profile = panics.profile();
    assert_eq!(profile.nodes["bad"].invokes, 1);
    assert_eq!(profile.nodes["bad"].status, Status::Errored);
}

#[test]
fn first_operator_and_source_slice_is_graph_visible() {
    let g = graph();
    let source = g.init_node(
        from_iter(vec![1i32, 2, 2, 3]),
        vec![],
        GraphNodeOpts::named("source"),
    );
    let mapped = g.init_node(
        map::<i32, i32>(|v| v * 2),
        vec![source.erased()],
        GraphNodeOpts::named("mapped"),
    );
    let filtered = g.init_node(
        filter::<i32>(|v| *v == 4),
        vec![mapped.erased()],
        GraphNodeOpts::named("filtered"),
    );
    let distinct = g.init_node(
        distinct_until_changed::<i32>(|a, b| a == b),
        vec![filtered.erased()],
        GraphNodeOpts::named("distinct"),
    );

    let events = Rc::new(RefCell::new(Vec::new()));
    let event_sink = events.clone();
    let _keep_alive = distinct.subscribe(move |msg| match msg {
        Message::Data(value) => {
            if let Some(v) = value.as_ref().downcast_ref::<i32>() {
                event_sink.borrow_mut().push(format!("DATA:{v}"));
            }
        }
        other => event_sink.borrow_mut().push(format!("{other:?}")),
    });

    assert_eq!(distinct.cache(), Some(4));
    assert!(events.borrow().contains(&"DATA:4".to_owned()));
    assert!(events.borrow().contains(&"COMPLETE".to_owned()));

    let snap = g.describe();
    let factory = |id: &str| {
        snap.nodes
            .iter()
            .find(|node| node.id == id)
            .map(|node| node.factory.as_str())
            .unwrap()
            .to_owned()
    };
    assert_eq!(factory("source"), "fromIter");
    assert_eq!(factory("mapped"), "map");
    assert_eq!(factory("filtered"), "filter");
    assert_eq!(factory("distinct"), "distinctUntilChanged");
}

#[test]
fn scan_and_take_preserve_occurrence_semantics() {
    let g = graph();
    let source = g.init_node(
        from_iter(vec![1i32, 1, 1, 2]),
        vec![],
        GraphNodeOpts::named("source"),
    );
    let scanned = g.init_node(
        scan::<i32, i32>(|acc, v| acc + *v, 0),
        vec![source.erased()],
        GraphNodeOpts::named("scanned"),
    );
    let first_three = g.init_node(
        take::<i32>(3),
        vec![scanned.erased()],
        GraphNodeOpts::named("first_three"),
    );
    let seen = Rc::new(RefCell::new(Vec::new()));
    let seen_sink = seen.clone();
    let _keep_alive = first_three.subscribe(move |msg| {
        if let Message::Data(value) = msg {
            if let Some(v) = value.as_ref().downcast_ref::<i32>() {
                seen_sink.borrow_mut().push(*v);
            }
        }
    });

    assert_eq!(*seen.borrow(), vec![1, 2, 3]);
    assert_eq!(first_three.status(), Status::Completed);
}

#[test]
fn operator_caller_opts_do_not_drop_intrinsic_partial_flag() {
    let g = graph();
    let left = g.state_empty_opts::<i32>(GraphNodeOpts::named("left"));
    let right = g.state_empty_opts::<i32>(GraphNodeOpts::named("right"));
    let mut opts = GraphNodeOpts::named("merged");
    opts.node = NodeOpts {
        pausable: Pausable::False,
        ..NodeOpts::default()
    };
    let merged = g.init_node(merge::<i32>(), vec![left.erased(), right.erased()], opts);
    let seen = Rc::new(RefCell::new(Vec::new()));
    let seen_sink = seen.clone();
    let _keep_alive = merged.subscribe(move |msg| {
        if let Message::Data(value) = msg {
            if let Some(v) = value.as_ref().downcast_ref::<i32>() {
                seen_sink.borrow_mut().push(*v);
            }
        }
    });

    left.set(7);

    assert_eq!(*seen.borrow(), vec![7]);
    assert_eq!(merged.cache(), Some(7));
}
