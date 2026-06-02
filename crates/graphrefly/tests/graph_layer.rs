use std::cell::{Cell, RefCell};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;

use graphrefly::{
    describe_to_ascii, describe_to_d2, describe_to_json, describe_to_mermaid,
    describe_to_mermaid_url, describe_to_pretty, distinct_until_changed, filter, from_iter, graph,
    graph_opts, map, merge, scan, take, DescribeEdge, DescribeOpts, DescribeValue, Explain,
    GraphNodeOpts, GraphOptions, LockId, Message, NodeOpts, Pausable, Status, Tier, Values,
};

fn last_or_prev(values: &Values<'_>, i: usize) -> Option<Rc<i32>> {
    values
        .batches::<i32>(i)
        .last()
        .and_then(|wave| wave.last().cloned())
        .or_else(|| values.prev::<i32>(i))
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

    target.set_deps(vec![source.erased()], |ctx| {
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
fn profile_counts_graph_registered_invokes_when_enabled() {
    let g = graph_opts(GraphOptions {
        name: Some("profile-demo".to_owned()),
        profile: true,
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
    });
    let child = graph();
    let leaf = child.state_opts(1i32, GraphNodeOpts::named("leaf"));
    let doubled = child.derived_opts(
        vec![leaf.erased()],
        |values| Some(*last_or_prev(values, 0).unwrap() * 2),
        GraphNodeOpts::named("doubled"),
    );
    let _keep_doubled = doubled.subscribe(|_| {});
    parent.mount(child, "sub");

    let profile = parent.profile();
    assert!(profile.nodes.contains_key("sub::leaf"));
    assert!(profile.nodes["sub::doubled"].invokes >= 1);

    let panics = graph_opts(GraphOptions {
        name: Some("panic-profile".to_owned()),
        profile: true,
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
