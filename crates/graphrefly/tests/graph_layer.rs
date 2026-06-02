use std::cell::{Cell, RefCell};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;

use graphrefly::{
    graph, graph_opts, DescribeEdge, DescribeValue, GraphNodeOpts, GraphOptions, LockId, Message,
    Status, Tier, Values,
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
