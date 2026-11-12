use std::cell::{Cell, RefCell};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;

use graphrefly::{
    default_dispatcher, default_restore_registry, distinct_until_changed, explain_path, filter,
    from_iter, graph, graph_opts, map, merge, profile_summary, profile_summary_from_snapshots,
    reachable, restore_graph, restore_registry, scan, take, timeout, topology_diff,
    topology_to_ascii, topology_to_d2, topology_to_d2_with_direction, topology_to_json,
    topology_to_mermaid, topology_to_mermaid_url, topology_to_mermaid_with_direction,
    topology_to_pretty, validate_no_islands, DescribeEvent, DiagramDirection, Dispatcher, Explain,
    ExplainPathOptions, ExplainPathReason, GraphCheckpointEdge, GraphCheckpointFactory,
    GraphCheckpointJson, GraphNodeOpts, GraphOptions, GraphRestoreDefinition, GraphRestoreEntry,
    IslandReport, Message, NodeOpts, ProfileSummaryOptions, ReachableDirection, ReachableOptions,
    RestoreFactoryMeta, RestoreGraphOptions, TopologyEdge, TopologyEvent, TopologyEventKind,
    TopologyGroupOptions, TopologyNode, TopologyOpts, TopologySnapshot, Values,
    GRAPH_CHECKPOINT_VERSION,
};
use serde_json::json;

/// One observed array, owned: the node id, its message kinds and its i32 payloads.
type Observed = Rc<RefCell<Vec<(String, Vec<&'static str>, Vec<i32>)>>>;

fn kind_name(m: &Message<graphrefly::AnyValue>) -> &'static str {
    match m {
        Message::Start => "START",
        Message::Pending => "PENDING",
        Message::Data(_) => "DATA",
        Message::Resolved => "RESOLVED",
        Message::Invalidate => "INVALIDATE",
        Message::Complete => "COMPLETE",
        Message::Error(_) => "ERROR",
    }
}

/// Observes `ids` of `g` (R-graph-surface@1) into an owned log; returns it and the stop.
fn observe_log(g: &graphrefly::Graph, ids: &[&str]) -> (Observed, Box<dyn FnOnce()>) {
    let log: Observed = Rc::new(RefCell::new(Vec::new()));
    let sink = log.clone();
    let stop = g.observe(ids, move |e| {
        sink.borrow_mut().push((
            e.id.to_owned(),
            e.msgs.iter().map(kind_name).collect(),
            e.msgs
                .iter()
                .filter_map(|m| match m {
                    Message::Data(v) => v.downcast_ref::<i32>().copied(),
                    _ => None,
                })
                .collect(),
        ));
    });
    (log, stop)
}

fn last_or_prev(values: &Values<'_>, i: usize) -> Option<Rc<i32>> {
    values.value::<i32>(i)
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

    g.batch(|| -> Result<_, ()> {
        count.set(1);
        count.set(2);
        assert_eq!(doubled.cache(), Some(0));

        Ok(())
    })
    .unwrap();

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
        |ctx| ctx.emit(*ctx.dep(0).value::<i32>().unwrap() + 1),
        GraphNodeOpts::named("raw"),
    );

    let found = g.find("source").expect("registered source is findable");
    assert_eq!(found.status().cache, graphrefly::CacheStatus::Empty);

    let ids: Vec<String> = g.describe().into_iter().map(|n| n.id).collect();
    let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();
    let (observed, observer) = observe_log(&g, &id_refs);
    assert_eq!(raw.cache(), Some(8));
    assert_eq!(*found.cache_any().unwrap().downcast::<i32>().unwrap(), 7i32);

    let events = observed.borrow();
    assert!(events
        .iter()
        .any(|(id, kinds, _)| id == "raw" && kinds.contains(&"DATA")));
    assert!(events
        .iter()
        .any(|(id, _, values)| id == "source" && values == &vec![7]));

    let snap = graphrefly::topology::snapshot(&g);
    assert_eq!(snap.name.as_deref(), Some("demo"));
    let source_node = snap.nodes.iter().find(|n| n.id == "source").unwrap();
    let raw_node = snap.nodes.iter().find(|n| n.id == "raw").unwrap();
    assert_eq!(source_node.factory, "producer");
    assert_eq!(raw_node.factory, "node");
    assert_eq!(raw_node.deps, vec!["source"]);
    assert!(snap.edges().contains(&TopologyEdge {
        from: "source".to_owned(),
        to: "raw".to_owned(),
    }));
    drop(events);
    observer();
}

#[test]
fn observe_topology_emits_registered_nodes_from_existing_registry() {
    let g = graph();
    let events = Rc::new(RefCell::new(Vec::<TopologyEvent>::new()));
    let event_sink = events.clone();
    let observer = graphrefly::topology::observe(&g)
        .subscribe(move |event| event_sink.borrow_mut().push(event));

    let source = g.state_opts(1i32, GraphNodeOpts::named("source"));
    let _derived = g.derived_opts(
        vec![source.erased()],
        |values| Some(*last_or_prev(values, 0).unwrap() + 1),
        GraphNodeOpts::named("derived"),
    );

    observer.unsubscribe();
    let events = events.borrow();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].kind, TopologyEventKind::NodeRegistered);
    assert_eq!(events[0].path, "source");
    assert_eq!(events[0].deps, Vec::<String>::new());
    assert_eq!(events[0].factory.as_deref(), Some("state"));
    assert_eq!(events[0].seq, 0);
    assert_eq!(events[1].kind, TopologyEventKind::NodeRegistered);
    assert_eq!(events[1].path, "derived");
    assert_eq!(events[1].deps, vec!["source".to_owned()]);
    assert_eq!(events[1].factory.as_deref(), Some("derived"));
    assert_eq!(events[1].seq, 1);
}

#[test]
fn observe_topology_deps_changed_matches_describe_edges() {
    let g = graph();
    let a = g.state_opts(1i32, GraphNodeOpts::named("a"));
    let b = g.state_opts(2i32, GraphNodeOpts::named("b"));
    let d = g.node_opts::<i32, _>(
        vec![a.erased()],
        |ctx| ctx.emit(*ctx.dep(0).value::<i32>().unwrap()),
        GraphNodeOpts::named("d"),
    );
    let events = Rc::new(RefCell::new(Vec::<TopologyEvent>::new()));
    let event_sink = events.clone();
    let observer = graphrefly::topology::observe_path(&g, "d")
        .subscribe(move |event| event_sink.borrow_mut().push(event));

    d.replace(
        vec![b.erased()],
        |ctx| {
            ctx.emit(*ctx.dep(0).value::<i32>().unwrap());
        },
        graphrefly::ReplaceOpts::default(),
    );

    observer.unsubscribe();
    let events = events.borrow();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, TopologyEventKind::DepsChanged);
    assert_eq!(events[0].path, "d");
    assert_eq!(events[0].prev_deps, Some(vec!["a".to_owned()]));
    assert_eq!(events[0].deps, vec!["b".to_owned()]);
    assert_eq!(events[0].seq, 0);

    let snap = graphrefly::topology::snapshot(&g);
    let d_node = snap.nodes.iter().find(|node| node.id == "d").unwrap();
    assert_eq!(d_node.deps, vec!["b".to_owned()]);
    assert!(snap.edges().contains(&TopologyEdge {
        from: "b".to_owned(),
        to: "d".to_owned(),
    }));
    assert!(!snap.edges().contains(&TopologyEdge {
        from: "a".to_owned(),
        to: "d".to_owned(),
    }));
}

#[test]
fn observe_topology_emits_mount_changed_from_parent_graph() {
    let parent = graph();
    let child = graph();
    let events = Rc::new(RefCell::new(Vec::<TopologyEvent>::new()));
    let event_sink = events.clone();
    let observer = graphrefly::topology::observe_path(&parent, "sub")
        .subscribe(move |event| event_sink.borrow_mut().push(event));

    graphrefly::topology::mount(&parent, child, "sub");

    observer.unsubscribe();
    let events = events.borrow();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, TopologyEventKind::MountChanged);
    assert_eq!(events[0].path, "sub");
    assert_eq!(events[0].deps, Vec::<String>::new());
    assert_eq!(events[0].factory.as_deref(), Some("mount"));
    assert_eq!(events[0].seq, 0);
    assert!(
        graphrefly::topology::snapshot(&parent).subgraphs.unwrap()[0]
            .nodes
            .is_empty()
    );
}

#[test]
fn observe_topology_forwards_mounted_child_events_with_parent_paths() {
    let parent = graph();
    let child = graph();
    graphrefly::topology::mount(&parent, child.clone(), "sub");
    let events = Rc::new(RefCell::new(Vec::<TopologyEvent>::new()));
    let event_sink = events.clone();
    let observer = graphrefly::topology::observe_path(&parent, "sub")
        .subscribe(move |event| event_sink.borrow_mut().push(event));

    let a = child.state_opts(1i32, GraphNodeOpts::named("a"));
    let b = child.state_opts(2i32, GraphNodeOpts::named("b"));
    let d = child.node_opts::<i32, _>(
        vec![a.erased()],
        |ctx| ctx.emit(*ctx.dep(0).value::<i32>().unwrap()),
        GraphNodeOpts::named("d"),
    );
    d.replace(
        vec![b.erased()],
        |ctx| {
            ctx.emit(*ctx.dep(0).value::<i32>().unwrap());
        },
        graphrefly::ReplaceOpts::default(),
    );

    observer.unsubscribe();
    let events = events.borrow();
    assert_eq!(events.len(), 4);
    assert_eq!(events[0].kind, TopologyEventKind::NodeRegistered);
    assert_eq!(events[0].path, "sub::a");
    assert_eq!(events[0].deps, Vec::<String>::new());
    assert_eq!(events[0].factory.as_deref(), Some("state"));
    assert_eq!(events[0].seq, 0);
    assert_eq!(events[1].kind, TopologyEventKind::NodeRegistered);
    assert_eq!(events[1].path, "sub::b");
    assert_eq!(events[1].deps, Vec::<String>::new());
    assert_eq!(events[1].factory.as_deref(), Some("state"));
    assert_eq!(events[1].seq, 1);
    assert_eq!(events[2].kind, TopologyEventKind::NodeRegistered);
    assert_eq!(events[2].path, "sub::d");
    assert_eq!(events[2].deps, vec!["sub::a".to_owned()]);
    assert_eq!(events[2].factory.as_deref(), Some("node"));
    assert_eq!(events[2].seq, 2);
    assert_eq!(events[3].kind, TopologyEventKind::DepsChanged);
    assert_eq!(events[3].path, "sub::d");
    assert_eq!(events[3].prev_deps, Some(vec!["sub::a".to_owned()]));
    assert_eq!(events[3].deps, vec!["sub::b".to_owned()]);
    assert_eq!(events[3].seq, 3);

    let snap = graphrefly::topology::snapshot(&parent);
    let subgraph = snap.subgraphs.unwrap().remove(0);
    let d_node = subgraph
        .nodes
        .iter()
        .find(|node| node.id == "sub::d")
        .unwrap();
    assert_eq!(d_node.deps, vec!["sub::b".to_owned()]);
}

#[test]
fn observe_topology_stops_forwarding_mounted_child_events_after_unsubscribe() {
    let parent = graph();
    let child = graph();
    graphrefly::topology::mount(&parent, child.clone(), "sub");
    let events = Rc::new(RefCell::new(Vec::<TopologyEvent>::new()));
    let event_sink = events.clone();
    let observer = graphrefly::topology::observe(&parent)
        .subscribe(move |event| event_sink.borrow_mut().push(event));
    observer.unsubscribe();

    let _leaf = child.state_opts(1i32, GraphNodeOpts::named("leaf"));

    assert!(events.borrow().is_empty());
}

#[test]
fn topology_group_creates_registered_child_nodes_and_release_removes_them() {
    let g = graph_opts(GraphOptions {
        profile: true,
        ..GraphOptions::default()
    });
    let source = g.state_opts(1i32, GraphNodeOpts::named("source"));
    let events = Rc::new(RefCell::new(Vec::<TopologyEvent>::new()));
    let event_sink = events.clone();
    let observer = graphrefly::topology::observe(&g)
        .subscribe(move |event| event_sink.borrow_mut().push(event));
    let group = graphrefly::topology::group_opts(&g, TopologyGroupOptions::named("view"));

    let delta = group.derived_opts(
        vec![source.erased()],
        |values| Some(*last_or_prev(values, 0).unwrap() + 1),
        GraphNodeOpts::named("view.delta"),
    );
    let _snapshot = group.derived_opts(
        vec![delta.erased()],
        |values| Some(*last_or_prev(values, 0).unwrap() * 2),
        GraphNodeOpts::named("view.snapshot"),
    );

    assert!(g.find("view.delta").is_some());
    assert!(g.find("view.snapshot").is_some());
    let describe_ids = graphrefly::topology::snapshot(&g)
        .nodes
        .into_iter()
        .map(|node| node.id)
        .collect::<Vec<_>>();
    assert!(describe_ids.contains(&"source".to_owned()));
    assert!(describe_ids.contains(&"view.delta".to_owned()));
    assert!(describe_ids.contains(&"view.snapshot".to_owned()));
    assert!(graphrefly::topology::profile(&g)
        .nodes
        .contains_key("view.delta"));
    assert!(g
        .checkpoint()
        .unwrap()
        .nodes
        .iter()
        .any(|node| node.id == "view.delta"));

    group.release();
    group.release();
    observer.unsubscribe();

    assert!(group.is_released());
    let events = events.borrow();
    assert_eq!(events.len(), 4);
    assert_eq!(events[0].kind, TopologyEventKind::NodeRegistered);
    assert_eq!(events[0].path, "view.delta");
    assert_eq!(events[0].deps, vec!["source".to_owned()]);
    assert_eq!(events[0].factory.as_deref(), Some("derived"));
    assert_eq!(events[0].seq, 0);
    assert_eq!(events[1].kind, TopologyEventKind::NodeRegistered);
    assert_eq!(events[1].path, "view.snapshot");
    assert_eq!(events[1].deps, vec!["view.delta".to_owned()]);
    assert_eq!(events[1].factory.as_deref(), Some("derived"));
    assert_eq!(events[1].seq, 1);
    assert_eq!(events[2].kind, TopologyEventKind::NodeReleased);
    assert_eq!(events[2].path, "view.delta");
    assert_eq!(events[2].deps, vec!["source".to_owned()]);
    assert_eq!(events[2].factory.as_deref(), Some("derived"));
    assert_eq!(events[2].seq, 2);
    assert_eq!(events[3].kind, TopologyEventKind::NodeReleased);
    assert_eq!(events[3].path, "view.snapshot");
    assert_eq!(events[3].deps, vec!["view.delta".to_owned()]);
    assert_eq!(events[3].factory.as_deref(), Some("derived"));
    assert_eq!(events[3].seq, 3);

    assert!(g.find("view.delta").is_none());
    assert!(!graphrefly::topology::snapshot(&g)
        .nodes
        .iter()
        .any(|node| node.id == "view.delta"));
    assert!(!graphrefly::topology::profile(&g)
        .nodes
        .contains_key("view.delta"));
    assert!(!g
        .checkpoint()
        .unwrap()
        .nodes
        .iter()
        .any(|node| node.id == "view.delta"));
    assert!(catch_unwind(AssertUnwindSafe(|| {
        g.state_opts(0i32, GraphNodeOpts::named("view.delta"));
    }))
    .is_err());
    assert!(catch_unwind(AssertUnwindSafe(|| {
        group.state_opts(0i32, GraphNodeOpts::named("late"));
    }))
    .is_err());
}

#[test]
fn topology_group_add_accepts_only_registered_nodes_from_owning_graph() {
    let g = graph();
    let group = graphrefly::topology::group_opts(&g, TopologyGroupOptions::named("view"));
    let registered = g.state_opts(1i32, GraphNodeOpts::named("registered"));
    let added = group.add(&registered);
    assert_eq!(added.cache(), Some(1));
    let duplicate = group.add(&registered);
    assert_eq!(duplicate.cache(), Some(1));

    let other = graph().state_opts(1i32, GraphNodeOpts::named("other"));
    assert!(catch_unwind(AssertUnwindSafe(|| group.add(&other))).is_err());

    let released_group = graphrefly::topology::group(&g);
    let temp = released_group.state_opts(1i32, GraphNodeOpts::named("temp"));
    released_group.release();
    let fresh_group = graphrefly::topology::group(&g);
    assert!(catch_unwind(AssertUnwindSafe(|| fresh_group.add(&temp))).is_err());
}

#[test]
fn topology_group_release_rejects_external_deps_and_subscribers() {
    let g = graph();

    let dep_group = graphrefly::topology::group_opts(&g, TopologyGroupOptions::named("dep"));
    let source = g.state_opts(1i32, GraphNodeOpts::named("source"));
    let child = dep_group.derived_opts(
        vec![source.erased()],
        |values| Some(*last_or_prev(values, 0).unwrap() + 1),
        GraphNodeOpts::named("dep.child"),
    );
    let _consumer = g.derived_opts(
        vec![child.erased()],
        |values| Some(*last_or_prev(values, 0).unwrap() + 1),
        GraphNodeOpts::named("consumer"),
    );
    assert!(catch_unwind(AssertUnwindSafe(|| dep_group.release())).is_err());
    assert!(!dep_group.is_released());
    assert!(g.find("dep.child").is_some());

    let sub_group = graphrefly::topology::group_opts(&g, TopologyGroupOptions::named("sub"));
    let subscribed = sub_group.state_opts(1i32, GraphNodeOpts::named("sub.child"));
    let unsub = subscribed.subscribe(|_| {});
    assert!(catch_unwind(AssertUnwindSafe(|| sub_group.release())).is_err());
    assert!(!sub_group.is_released());
    unsub();
    sub_group.release();
    assert!(sub_group.is_released());
    assert!(g.find("sub.child").is_none());
    // A host cannot leave a node pending between waves (PENDING is the kernel's,
    // R-wave), so the former dirty-node release case is not reachable from the host.
}

#[test]
fn observe_topology_is_read_only_and_separate_from_protocol_observe() {
    let g = graph();
    let runs = Rc::new(Cell::new(0usize));
    let runs_for_fn = runs.clone();
    let cold = g.producer_opts::<i32, _>(
        move |ctx| {
            runs_for_fn.set(runs_for_fn.get() + 1);
            ctx.emit(7i32);
        },
        GraphNodeOpts::named("cold"),
    );
    let topology_events = Rc::new(RefCell::new(Vec::new()));
    let topology_sink = topology_events.clone();
    let topology_observer = graphrefly::topology::observe(&g)
        .subscribe(move |event| topology_sink.borrow_mut().push(event.kind));

    topology_observer.unsubscribe();
    assert_eq!(runs.get(), 0);
    assert_eq!(cold.cache(), None);
    assert!(topology_events.borrow().is_empty());

    let (protocol_events, protocol_observer) = observe_log(&g, &["cold"]);
    protocol_observer();
    assert_eq!(runs.get(), 1);
    assert!(protocol_events
        .borrow()
        .iter()
        .any(|(_, kinds, _)| kinds.contains(&"DATA")));
    assert!(topology_events.borrow().is_empty());
}

#[test]
fn observe_topology_stops_after_unsubscribe() {
    let g = graph();
    let events = Rc::new(RefCell::new(Vec::new()));
    let event_sink = events.clone();
    let observer = graphrefly::topology::observe(&g)
        .subscribe(move |event| event_sink.borrow_mut().push(event.path));
    observer.unsubscribe();

    let _later = g.state_opts(1i32, GraphNodeOpts::named("later"));

    assert!(events.borrow().is_empty());
}

#[test]
fn observe_topology_delivers_nested_events_fifo_and_isolates_panics() {
    let g = graph();
    let first = Rc::new(RefCell::new(Vec::new()));
    let second = Rc::new(RefCell::new(Vec::new()));
    let late = Rc::new(RefCell::new(Vec::new()));
    let late_observers = Rc::new(RefCell::new(Vec::new()));
    let added = Rc::new(Cell::new(false));
    let first_sink = first.clone();
    let late_sink = late.clone();
    let late_handles = late_observers.clone();
    let added_flag = added.clone();
    let graph_for_nested = g.clone();
    let _first_observer = graphrefly::topology::observe(&g).subscribe(move |event| {
        first_sink
            .borrow_mut()
            .push(format!("{}:{}", event.path, event.seq));
        if !added_flag.get() {
            added_flag.set(true);
            let late_events = late_sink.clone();
            let late_observer =
                graphrefly::topology::observe(&graph_for_nested).subscribe(move |late_event| {
                    late_events
                        .borrow_mut()
                        .push(format!("{}:{}", late_event.path, late_event.seq));
                });
            late_handles.borrow_mut().push(late_observer);
            let _nested = graph_for_nested.state_opts(2i32, GraphNodeOpts::named("nested"));
        }
    });
    let second_sink = second.clone();
    let _second_observer = graphrefly::topology::observe(&g).subscribe(move |event| {
        second_sink
            .borrow_mut()
            .push(format!("{}:{}", event.path, event.seq));
        panic!("observer failure must not veto topology mutation");
    });

    let result = catch_unwind(AssertUnwindSafe(|| {
        let _root = g.state_opts(1i32, GraphNodeOpts::named("root"));
    }));

    assert!(result.is_ok());
    assert!(g.find("root").is_some());
    assert!(g.find("nested").is_some());
    assert_eq!(
        *first.borrow(),
        vec!["root:0".to_owned(), "nested:1".to_owned()]
    );
    assert_eq!(
        *second.borrow(),
        vec!["root:0".to_owned(), "nested:1".to_owned()]
    );
    assert_eq!(*late.borrow(), vec!["nested:1".to_owned()]);
}

#[test]
fn observe_topology_skips_observer_unsubscribed_before_its_turn() {
    let g = graph();
    let second = Rc::new(RefCell::new(Vec::new()));
    let second_observer = Rc::new(RefCell::new(None::<graphrefly::GraphTopologyObserver>));
    let second_handle = second_observer.clone();
    let _first = graphrefly::topology::observe(&g).subscribe(move |_| {
        if let Some(observer) = second_handle.borrow_mut().take() {
            observer.unsubscribe();
        }
    });
    let second_sink = second.clone();
    *second_observer.borrow_mut() = Some(
        graphrefly::topology::observe(&g)
            .subscribe(move |event| second_sink.borrow_mut().push(event.path)),
    );

    let _root = g.state_opts(1i32, GraphNodeOpts::named("root"));

    assert!(second.borrow().is_empty());
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

    let snap = graphrefly::topology::snapshot(&g);
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

    let snap = graphrefly::topology::snapshot(&g);
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
    graphrefly::topology::mount(&parent, child, "child");

    let islands = validate_no_islands(&graphrefly::topology::snapshot(&parent));
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
fn graph_diagnostics_topology_diff_is_pure_snapshot_delta() {
    let g = graph();
    let source = g.state_opts(1i32, GraphNodeOpts::named("source"));
    let prev = graphrefly::topology::snapshot(&g);

    let mut opts = GraphNodeOpts::named("mapped");
    opts.meta.insert("role".to_owned(), "projection".to_owned());
    let mapped = g.derived_opts(
        vec![source.erased()],
        |values| Some(*last_or_prev(values, 0).unwrap() + 1),
        opts,
    );
    let next = graphrefly::topology::snapshot(&g);
    let diff = topology_diff(&prev, &next);

    assert!(diff.events.iter().any(|event| matches!(
        event,
        DescribeEvent::NodeAdded { id, node }
            if id == "mapped" && node.factory == "derived"
    )));
    assert!(diff.events.iter().any(|event| matches!(
        event,
        DescribeEvent::EdgeAdded { from, to } if from == "source" && to == "mapped"
    )));

    source.set(2);
    assert!(
        topology_diff(&next, &graphrefly::topology::snapshot(&g))
            .events
            .is_empty(),
        "value/status changes are observe/profile data, not topology"
    );

    let mut meta_prev = next.clone();
    let mut meta_next = next.clone();
    meta_prev
        .nodes
        .iter_mut()
        .find(|n| n.id == "mapped")
        .unwrap()
        .meta = None;
    meta_next
        .nodes
        .iter_mut()
        .find(|n| n.id == "mapped")
        .unwrap()
        .meta
        .as_mut()
        .unwrap()
        .insert("role".to_owned(), "view".to_owned());
    assert!(topology_diff(&meta_prev, &meta_next)
        .events
        .iter()
        .any(|event| matches!(
            event,
            DescribeEvent::NodeMetaChanged { id, next_meta, .. }
                if id == "mapped" && next_meta.get("role") == Some(&"view".to_owned())
        )));

    let child = graph_opts(GraphOptions::named("child"));
    child.state_opts(1i32, GraphNodeOpts::named("leaf"));
    let before_mount = graphrefly::topology::snapshot(&g);
    graphrefly::topology::mount(&g, child, "child");
    let mounted = topology_diff(&before_mount, &graphrefly::topology::snapshot(&g));
    assert!(mounted.events.iter().any(|event| matches!(
        event,
        DescribeEvent::SubgraphMounted { path } if path == "child"
    )));
    assert!(mounted.events.iter().any(|event| matches!(
        event,
        DescribeEvent::NodeAdded { id, .. } if id == "child::leaf"
    )));

    let empty_child = graph_opts(GraphOptions::named("ignored-child-name"));
    let before_empty_mount = graphrefly::topology::snapshot(&g);
    graphrefly::topology::mount(&g, empty_child, "empty");
    let empty_mounted = topology_diff(&before_empty_mount, &graphrefly::topology::snapshot(&g));
    assert!(empty_mounted.events.iter().any(|event| matches!(
        event,
        DescribeEvent::SubgraphMounted { path } if path == "empty"
    )));

    let mut removed = next.clone();
    removed.nodes.retain(|node| node.id != "mapped");
    removed.edges().clear();
    let removed_diff = topology_diff(&next, &removed);
    assert!(removed_diff.events.iter().any(|event| matches!(
        event,
        DescribeEvent::EdgeRemoved { from, to } if from == "source" && to == "mapped"
    )));
    assert!(removed_diff.events.iter().any(|event| matches!(
        event,
        DescribeEvent::NodeRemoved { id } if id == "mapped"
    )));

    drop(mapped);
}

#[test]
fn graph_diagnostics_accept_synthetic_snapshots_and_cycles() {
    let snap = TopologySnapshot {
        name: None,
        nodes: vec![
            TopologyNode {
                id: "x".to_owned(),
                name: None,
                factory: "state".to_owned(),
                kind: graphrefly::DescribeKind::State,
                deps: vec![],
                quiet: false,
                meta: None,
            },
            TopologyNode {
                id: "y".to_owned(),
                name: None,
                factory: "derived".to_owned(),
                kind: graphrefly::DescribeKind::Node,
                deps: vec!["x".to_owned()],
                quiet: false,
                meta: None,
            },
        ],
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

    let dangling = TopologySnapshot {
        name: None,
        nodes: vec![
            TopologyNode {
                id: "x".to_owned(),
                name: None,
                factory: "state".to_owned(),
                kind: graphrefly::DescribeKind::State,
                deps: vec!["missing".to_owned()],
                quiet: false,
                meta: None,
            },
            TopologyNode {
                id: "y".to_owned(),
                name: None,
                factory: "derived".to_owned(),
                kind: graphrefly::DescribeKind::Node,
                deps: vec!["missing".to_owned()],
                quiet: false,
                meta: None,
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

    let cycle = TopologySnapshot {
        name: None,
        nodes: vec![
            TopologyNode {
                id: "a".to_owned(),
                name: None,
                factory: "node".to_owned(),
                kind: graphrefly::DescribeKind::Node,
                deps: vec!["b".to_owned()],
                quiet: false,
                meta: None,
            },
            TopologyNode {
                id: "b".to_owned(),
                name: None,
                factory: "node".to_owned(),
                kind: graphrefly::DescribeKind::Node,
                deps: vec!["a".to_owned()],
                quiet: false,
                meta: None,
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
    assert_eq!(count_checkpoint.values, vec![json!(2)]);
    serde_json::to_string(&checkpoint).expect("checkpoint is JSON serializable");

    let restored = restore_graph(
        checkpoint,
        RestoreGraphOptions::new(default_restore_registry()),
    )
    .expect("fresh restore succeeds");
    let restored_count = restored.find("count").expect("restored node is registered");
    assert_eq!(
        restored_count.status().cache,
        graphrefly::CacheStatus::Ready
    );
    assert_eq!(json_cache(restored_count.clone()), json!(2));

    let seen = Rc::new(RefCell::new(Vec::new()));
    let seen_sink = seen.clone();
    let _unsub =
        restored_count.subscribe(move |msgs: &[graphrefly::Message<graphrefly::AnyValue>]| {
            let handle = |msg: &graphrefly::Message<graphrefly::AnyValue>| {
                if let Message::Data(value) = msg {
                    seen_sink.borrow_mut().push(
                        value
                            .as_ref()
                            .downcast_ref::<GraphCheckpointJson>()
                            .cloned(),
                    );
                }
            };
            for msg in msgs {
                handle(msg);
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
            for value in ctx.dep(0).values::<GraphCheckpointJson>() {
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
    let snap = graphrefly::topology::snapshot(&restored);
    assert!(snap.edges().contains(&TopologyEdge {
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

    graphrefly::topology::mount(&parent, child, "sub");

    let snap = graphrefly::topology::snapshot(&parent);
    let sub = snap.subgraphs.unwrap().into_iter().next().unwrap();
    assert!(sub.nodes.iter().any(|n| n.id == "sub::leaf"));
}

#[test]
#[should_panic(expected = "duplicate mount path")]
fn mount_rejects_duplicate_paths() {
    let parent = graph();
    graphrefly::topology::mount(&parent, graph(), "sub");
    graphrefly::topology::mount(&parent, graph(), "sub");
}

#[test]
#[should_panic(expected = "graph cycle")]
fn mount_rejects_cycles() {
    let parent = graph();
    graphrefly::topology::mount(&parent, parent.clone(), "self");
}

#[test]
fn observe_traverses_mounted_graphs_with_prefixed_paths() {
    let parent = graph();
    let child = graph();
    let leaf = child.state_opts(2i32, GraphNodeOpts::named("leaf"));
    graphrefly::topology::mount(&parent, child, "sub");

    let (observed, observer) = observe_log(&parent, &["sub::leaf"]);

    assert!(observed
        .borrow()
        .iter()
        .any(|(id, _, values)| id == "sub::leaf" && values == &vec![2]));
    observed.borrow_mut().clear();

    leaf.set(3);
    assert!(observed
        .borrow()
        .iter()
        .any(|(id, _, values)| id == "sub::leaf" && values == &vec![3]));
    observer();
}

#[test]
fn observe_includes_ended_nodes_and_keeps_live_ones() {
    // R-terminals: an ended node stays subscribable, so observing a graph that holds
    // one succeeds; the ended node replays its terminal and live nodes keep flowing.
    let g = graph();
    let live = g.state_opts(1i32, GraphNodeOpts::named("live"));
    let terminal = g.state_opts(9i32, GraphNodeOpts::named("terminal"));
    let _keep_terminal_active = terminal.subscribe(|_| {});
    terminal.complete();

    let (observed, observer) = observe_log(&g, &["live", "terminal"]);
    assert!(observed
        .borrow()
        .iter()
        .any(|(id, kinds, _)| id == "terminal" && kinds.contains(&"COMPLETE")));

    let before = observed.borrow().len();
    live.set(2);
    assert!(observed.borrow()[before..]
        .iter()
        .any(|(id, _, values)| id == "live" && values == &vec![2]));
    observer();
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

/// R-primary-api-clean@2 (graphrefly:D805) — derived sees values only: each
/// dependency's latest value and the one before it; no arrays, terminals or policies.
#[test]
fn derived_values_give_the_latest_and_previous_values_only() {
    let g = graph();
    let source = g.state_opts(1i32, GraphNodeOpts::named("source"));
    let seen = Rc::new(RefCell::new(Vec::new()));
    let sum = {
        let seen = seen.clone();
        g.derived_opts(
            vec![source.erased()],
            move |values| {
                assert_eq!(values.len(), 1);
                let latest = values.value::<i32>(0).map(|v| *v);
                seen.borrow_mut()
                    .push((latest, values.prev::<i32>(0).map(|v| *v)));
                latest
            },
            GraphNodeOpts::named("sum"),
        )
    };
    let _keep_alive = sum.subscribe(|_| {});
    assert_eq!(sum.cache(), Some(1));
    g.batch(|| -> Result<_, ()> {
        source.set(2i32);
        source.set(3i32);

        Ok(())
    })
    .unwrap();
    assert_eq!(sum.cache(), Some(3));
    assert_eq!(*seen.borrow(), vec![(Some(1), None), (Some(3), Some(1))]);
}

#[test]
#[should_panic(expected = "different graph")]
fn kernel_node_rejects_cross_graph_deps() {
    let g = graph();
    let other = graph();
    let graph_owned = g.state(1i32);

    let _bad = other.node::<i32, _>(vec![graph_owned.erased()], |ctx| {
        ctx.emit(*ctx.dep(0).value::<i32>().unwrap())
    });
}

#[test]
#[should_panic(expected = "different graph")]
fn substrate_rewire_rejects_cross_arena_deps() {
    let g1 = graph();
    let g2 = graph();
    let source = g1.state(1i32);
    let target = g2.derived::<i32, _>(vec![], |_| Some(0));

    target.replace(
        vec![source.erased()],
        |ctx| ctx.emit(*ctx.dep(0).value::<i32>().unwrap()),
        graphrefly::ReplaceOpts::default(),
    );
}

#[test]
fn state_empty_describe_omits_sentinel_value() {
    let g = graph();
    let state = g.state_empty_opts::<i32>(GraphNodeOpts::named("empty"));
    let _keep_alive = state.subscribe(|_| {});

    // describe carries no value (R-graph-surface@1): it is read from the node.
    assert_eq!(
        g.describe()
            .iter()
            .find(|n| n.id == "empty")
            .map(|n| n.kind),
        Some(graphrefly::DescribeKind::State)
    );
    assert_eq!(state.cache(), None);
    state.set(3i32);
    assert_eq!(state.cache(), Some(3));
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

    let explain = graphrefly::topology::snapshot_opts(
        &g,
        TopologyOpts {
            explain: Some(Explain {
                from: "source".to_owned(),
                to: "sink".to_owned(),
            }),
        },
    );
    assert!(explain.nodes.iter().any(|n| n.id == "source"));
    assert!(explain.nodes.iter().any(|n| n.id == "mapped"));
    assert!(explain.nodes.iter().any(|n| n.id == "sink"));
    assert!(!explain.nodes.iter().any(|n| n.id == "unrelated"));

    let snap = graphrefly::topology::snapshot(&g);
    assert!(topology_to_mermaid(&snap).contains("flowchart LR"));
    assert!(topology_to_d2(&snap).contains("direction: right"));
    assert!(topology_to_pretty(&snap).contains("source (state)"));
    assert!(topology_to_ascii(&snap).contains("source [state] -> mapped"));
    assert!(topology_to_json(&snap).contains("\"id\":\"source\""));
    assert!(topology_to_mermaid_url(&snap).starts_with("https://mermaid.live/edit#base64:"));
}

#[test]
fn renderers_respect_direction() {
    let g = graph_opts(GraphOptions::named("render-options"));
    let source = g.state_opts(1i32, GraphNodeOpts::named("source"));
    let sink = g.derived_opts(
        vec![source.erased()],
        |values| Some(*last_or_prev(values, 0).unwrap() + 1),
        GraphNodeOpts::named("sink"),
    );
    let _keep_alive = sink.subscribe(|_| {});
    let snap = graphrefly::topology::snapshot(&g);

    assert!(
        topology_to_mermaid_with_direction(&snap, DiagramDirection::Td).starts_with("flowchart TD")
    );
    assert!(
        topology_to_mermaid_with_direction(&snap, DiagramDirection::Bt).starts_with("flowchart BT")
    );
    assert!(
        topology_to_mermaid_with_direction(&snap, DiagramDirection::Rl).starts_with("flowchart RL")
    );

    assert!(
        topology_to_d2_with_direction(&snap, DiagramDirection::Td).starts_with("direction: down")
    );
    assert!(topology_to_d2_with_direction(&snap, DiagramDirection::Bt).starts_with("direction: up"));
    assert!(
        topology_to_d2_with_direction(&snap, DiagramDirection::Rl).starts_with("direction: left")
    );

    // The topology view carries no value or status (R-graph-surface@1).
    assert!(topology_to_ascii(&snap).contains("source [state] -> sink"));
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

    let snap = graphrefly::topology::snapshot(&g);
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
    assert!(snap.edges().contains(&TopologyEdge {
        from: "source".to_owned(),
        to: timeout_id.clone(),
    }));
    assert!(snap.edges().contains(&TopologyEdge {
        from: timer_id,
        to: timeout_id.clone(),
    }));
    assert!(snap.edges().contains(&TopologyEdge {
        from: timeout_id.clone(),
        to: "sink".to_owned(),
    }));

    let mermaid = topology_to_mermaid(&snap);
    let ascii = topology_to_ascii(&snap);
    assert!(mermaid.contains("\"source\""));
    assert!(mermaid.contains("\"sink\""));
    assert!(ascii.contains("source [state]"));
    assert!(ascii.contains("sink [map]"));
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
    graphrefly::topology::mount(&parent, child, "sub");

    let explain = graphrefly::topology::snapshot_opts(
        &parent,
        TopologyOpts {
            explain: Some(Explain {
                from: "sub::source".to_owned(),
                to: "sub::sink".to_owned(),
            }),
        },
    );

    assert_eq!(explain.subgraphs, None);
    let ids = explain
        .nodes
        .iter()
        .map(|node| node.id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["sub::source", "sub::sink"]);
    assert_eq!(
        explain.edges(),
        vec![TopologyEdge {
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
        graphrefly::topology::profile(&g).nodes["doubled"].invokes >= 2,
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

    let profile = graphrefly::topology::profile(&g);
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
        graphrefly::topology::profile(&unprofiled_default).nodes["doubled"].invokes,
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

    let profile = graphrefly::topology::profile(&g);
    assert!(profile.total_invokes >= 2);
    assert_eq!(profile.nodes["source"].invokes, 0);
    assert!(profile.nodes["doubled"].invokes >= 2);
    assert_eq!(
        profile.nodes["doubled"].status.cache,
        graphrefly::CacheStatus::Ready
    );
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
    graphrefly::topology::mount(&parent, child, "sub");
    let _keep_doubled = doubled.subscribe(|_| {});

    let profile = graphrefly::topology::profile(&parent);
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
    let profile = graphrefly::topology::profile(&panics);
    assert_eq!(profile.nodes["bad"].invokes, 1);
    assert_eq!(
        profile.nodes["bad"].status.output,
        graphrefly::OutputStatus::Errored
    );
}

#[test]
fn profile_summary_rolls_up_existing_profile_and_describe_facts() {
    let g = graph_opts(GraphOptions {
        name: Some("profile-summary".to_owned()),
        profile: true,
        dispatcher: Some(Dispatcher::new()),
        ..GraphOptions::default()
    });
    let source = g.state_opts(0i32, GraphNodeOpts::named("source"));
    let doubled = g.derived_opts(
        vec![source.erased()],
        |values| Some(*last_or_prev(values, 0).unwrap() * 2),
        GraphNodeOpts::named("doubled"),
    );
    let _keep_alive = doubled.subscribe(|_| {});
    source.set(1);
    source.set(2);

    let described = graphrefly::topology::snapshot(&g);
    let profile = graphrefly::topology::profile(&g);
    let summary = profile_summary_from_snapshots(
        &described,
        &profile,
        ProfileSummaryOptions { limit: Some(1) },
    );

    assert_eq!(summary.node_count, described.nodes.len());
    assert_eq!(summary.total_invokes, profile.total_invokes);
    assert_eq!(
        summary
            .by_status
            .iter()
            .find(|entry| entry.status.cache == graphrefly::CacheStatus::Ready)
            .map(|entry| entry.count),
        Some(2)
    );
    assert_eq!(summary.hot_nodes.len(), 1);
    assert_eq!(summary.hot_nodes[0].path, "doubled");
    assert_eq!(
        summary.hot_nodes[0].invokes,
        profile.nodes["doubled"].invokes
    );

    let profile_again = graphrefly::topology::profile(&g);
    let described_again = graphrefly::topology::snapshot(&g);
    assert_eq!(
        described_again.edges(),
        described.edges(),
        "profile summary is inspection-only and must not change describe-visible edges"
    );
    assert_eq!(
        profile_again.nodes["doubled"].invokes, profile.nodes["doubled"].invokes,
        "profile summary reads existing counters rather than driving new invokes"
    );
}

#[test]
fn profile_summary_does_not_activate_cold_nodes_or_create_topology_events() {
    let g = graph_opts(GraphOptions {
        profile: true,
        dispatcher: Some(Dispatcher::new()),
        ..GraphOptions::default()
    });
    let runs = Rc::new(Cell::new(0usize));
    let runs_for_fn = runs.clone();
    let cold = g.producer_opts::<i32, _>(
        move |ctx| {
            runs_for_fn.set(runs_for_fn.get() + 1);
            ctx.emit(7i32);
        },
        GraphNodeOpts::named("cold"),
    );
    let topology_events = Rc::new(RefCell::new(Vec::<TopologyEvent>::new()));
    let topology_sink = topology_events.clone();
    let observer = graphrefly::topology::observe(&g)
        .subscribe(move |event| topology_sink.borrow_mut().push(event));

    let summary = profile_summary(&g, ProfileSummaryOptions::default());

    observer.unsubscribe();
    assert_eq!(runs.get(), 0);
    assert_eq!(cold.cache(), None);
    assert!(topology_events.borrow().is_empty());
    assert_eq!(summary.node_count, 1);
    assert_eq!(summary.total_invokes, 0);
    assert_eq!(summary.hot_nodes[0].path, "cold");
    assert_eq!(summary.hot_nodes[0].invokes, 0);
    assert_eq!(
        summary.hot_nodes[0].status.cache,
        graphrefly::CacheStatus::Empty
    );
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
    let _keep_alive =
        distinct.subscribe(move |msgs: &[graphrefly::Message<graphrefly::AnyValue>]| {
            let handle = |msg: &graphrefly::Message<graphrefly::AnyValue>| match msg {
                Message::Data(value) => {
                    if let Some(v) = value.as_ref().downcast_ref::<i32>() {
                        event_sink.borrow_mut().push(format!("DATA:{v}"));
                    }
                }
                other => event_sink.borrow_mut().push(format!("{other:?}")),
            };
            for msg in msgs {
                handle(msg);
            }
        });

    assert_eq!(distinct.cache(), Some(4));
    assert!(events.borrow().contains(&"DATA:4".to_owned()));
    assert!(events.borrow().contains(&"COMPLETE".to_owned()));

    let snap = graphrefly::topology::snapshot(&g);
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
    let _keep_alive =
        first_three.subscribe(move |msgs: &[graphrefly::Message<graphrefly::AnyValue>]| {
            let handle = |msg: &graphrefly::Message<graphrefly::AnyValue>| {
                if let Message::Data(value) = msg {
                    if let Some(v) = value.as_ref().downcast_ref::<i32>() {
                        seen_sink.borrow_mut().push(*v);
                    }
                }
            };
            for msg in msgs {
                handle(msg);
            }
        });

    assert_eq!(*seen.borrow(), vec![1, 2, 3]);
    assert_eq!(
        first_three.status().output,
        graphrefly::OutputStatus::Completed
    );
}

#[test]
fn operator_caller_opts_do_not_drop_intrinsic_partial_flag() {
    let g = graph();
    let left = g.state_empty_opts::<i32>(GraphNodeOpts::named("left"));
    let right = g.state_empty_opts::<i32>(GraphNodeOpts::named("right"));
    let mut opts = GraphNodeOpts::named("merged");
    opts.node = NodeOpts {
        factory: Some("callerFactory".to_owned()),
        ..NodeOpts::default()
    };
    let merged = g.init_node(merge::<i32>(), vec![left.erased(), right.erased()], opts);
    let seen = Rc::new(RefCell::new(Vec::new()));
    let seen_sink = seen.clone();
    let _keep_alive =
        merged.subscribe(move |msgs: &[graphrefly::Message<graphrefly::AnyValue>]| {
            let handle = |msg: &graphrefly::Message<graphrefly::AnyValue>| {
                if let Message::Data(value) = msg {
                    if let Some(v) = value.as_ref().downcast_ref::<i32>() {
                        seen_sink.borrow_mut().push(*v);
                    }
                }
            };
            for msg in msgs {
                handle(msg);
            }
        });

    left.set(7);

    assert_eq!(*seen.borrow(), vec![7]);
    assert_eq!(merged.cache(), Some(7));
}
