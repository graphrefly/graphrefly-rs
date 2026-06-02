use std::cell::{Cell, RefCell};
use std::rc::Rc;

use graphrefly::{
    batch, buffer, buffer_count, combine, concat, distinct_until_changed, element_at, filter, find,
    first_any, from_iter, graph, last_any, map, on_first_data, pairwise, race, reduce, rescue,
    sample, settle, skip, take, take_until, take_while, tap, valve, with_latest_from, zip,
    GraphNodeOpts, Message,
};

fn collect_data<T: Clone + 'static>(node: &graphrefly::Node<T>) -> Rc<RefCell<Vec<T>>> {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let seen_sink = seen.clone();
    let _keep = node.subscribe(move |msg| {
        if let Message::Data(value) = msg {
            if let Some(v) = value.as_ref().downcast_ref::<T>() {
                seen_sink.borrow_mut().push(v.clone());
            }
        }
    });
    seen
}

fn collect_shapes<T: Clone + 'static>(node: &graphrefly::Node<T>) -> Rc<RefCell<Vec<String>>> {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let seen_sink = seen.clone();
    let _keep = node.subscribe(move |msg| match msg {
        Message::Data(value) => {
            if value.as_ref().downcast_ref::<T>().is_some() {
                seen_sink.borrow_mut().push("DATA".to_owned());
            }
        }
        Message::Complete => seen_sink.borrow_mut().push("COMPLETE".to_owned()),
        Message::Error(_) => seen_sink.borrow_mut().push("ERROR".to_owned()),
        Message::Start | Message::Dirty | Message::Resolved | Message::Invalidate => {}
        Message::Pause(_) | Message::Resume(_) | Message::Teardown => {}
    });
    seen
}

#[test]
fn single_dep_catalog_preserves_occurrences_and_terminal_inputs() {
    let g = graph();

    let src = g.init_node(
        from_iter(vec![1i32, 1, 2, 3]),
        vec![],
        GraphNodeOpts::named("src"),
    );
    let sum = g.init_node(
        reduce::<i32, i32>(|acc, v| acc + *v, 0),
        vec![src.erased()],
        GraphNodeOpts::named("sum"),
    );
    assert_eq!(*collect_data(&sum).borrow(), vec![7]);
    assert_eq!(sum.status(), graphrefly::Status::Completed);

    let src = g.init_node(
        from_iter(vec![1i32, 2, 3]),
        vec![],
        GraphNodeOpts::named("src2"),
    );
    let pairs = g.init_node(
        pairwise::<i32>(),
        vec![src.erased()],
        GraphNodeOpts::named("pairs"),
    );
    assert_eq!(*collect_data(&pairs).borrow(), vec![(1, 2), (2, 3)]);

    let src = g.init_node(
        from_iter(vec![1i32, 2, 3, 4, 5]),
        vec![],
        GraphNodeOpts::named("src3"),
    );
    let skipped = g.init_node(
        skip::<i32>(1),
        vec![src.erased()],
        GraphNodeOpts::named("skip"),
    );
    let until = g.init_node(
        take_while::<i32>(|v| *v < 4),
        vec![skipped.erased()],
        GraphNodeOpts::named("take_while"),
    );
    assert_eq!(*collect_data(&until).borrow(), vec![2, 3]);
    assert_eq!(until.status(), graphrefly::Status::Completed);

    let src = g.init_node(
        from_iter(vec![9i32, 8, 7]),
        vec![],
        GraphNodeOpts::named("src4"),
    );
    let first = g.init_node(
        first_any::<i32>(),
        vec![src.erased()],
        GraphNodeOpts::named("first"),
    );
    assert_eq!(*collect_data(&first).borrow(), vec![9]);
    assert_eq!(first.status(), graphrefly::Status::Completed);

    let src = g.init_node(
        from_iter(vec![1i32, 2, 3]),
        vec![],
        GraphNodeOpts::named("src5"),
    );
    let found = g.init_node(
        find::<i32>(|v| *v == 2),
        vec![src.erased()],
        GraphNodeOpts::named("find"),
    );
    assert_eq!(*collect_data(&found).borrow(), vec![2]);
    assert_eq!(found.status(), graphrefly::Status::Completed);

    let src = g.init_node(
        from_iter(vec![4i32, 5, 6]),
        vec![],
        GraphNodeOpts::named("src6"),
    );
    let at = g.init_node(
        element_at::<i32>(1),
        vec![src.erased()],
        GraphNodeOpts::named("element_at"),
    );
    assert_eq!(*collect_data(&at).borrow(), vec![5]);

    let src = g.init_node(
        from_iter(vec![4i32, 5, 6]),
        vec![],
        GraphNodeOpts::named("src7"),
    );
    let last = g.init_node(
        last_any::<i32>(),
        vec![src.erased()],
        GraphNodeOpts::named("last"),
    );
    assert_eq!(*collect_data(&last).borrow(), vec![6]);
}

#[test]
fn side_effect_error_and_gate_operators_are_graph_visible() {
    let g = graph();

    let tapped_values = Rc::new(RefCell::new(Vec::new()));
    let tapped_sink = tapped_values.clone();
    let src = g.init_node(
        from_iter(vec![1i32, 2]),
        vec![],
        GraphNodeOpts::named("tap_src"),
    );
    let tapped = g.init_node(
        tap::<i32>(move |v| tapped_sink.borrow_mut().push(*v)),
        vec![src.erased()],
        GraphNodeOpts::named("tap"),
    );
    assert_eq!(*collect_data(&tapped).borrow(), vec![1, 2]);
    assert_eq!(*tapped_values.borrow(), vec![1, 2]);

    let first_seen = Rc::new(Cell::new(0));
    let first_seen_sink = first_seen.clone();
    let src = g.init_node(
        from_iter(vec![3i32, 4, 5]),
        vec![],
        GraphNodeOpts::named("first_data_src"),
    );
    let first_data = g.init_node(
        on_first_data::<i32>(move |v| first_seen_sink.set(*v)),
        vec![src.erased()],
        GraphNodeOpts::named("first_data"),
    );
    assert_eq!(*collect_data(&first_data).borrow(), vec![3, 4, 5]);
    assert_eq!(first_seen.get(), 3);

    let source = g.state(1i32);
    let recovered = g.init_node(
        rescue::<i32>(|_| 99),
        vec![source.erased()],
        GraphNodeOpts::named("rescue"),
    );
    let recovered_seen = collect_data(&recovered);
    source.down(vec![Message::Error("boom".into())]);
    assert_eq!(*recovered_seen.borrow(), vec![1, 99]);
    assert_ne!(recovered.status(), graphrefly::Status::Completed);
    assert_ne!(recovered.status(), graphrefly::Status::Errored);

    let source = g.state(10i32);
    let control = g.state_empty::<bool>();
    let gated = g.init_node(
        valve::<i32>(),
        vec![source.erased(), control.erased()],
        GraphNodeOpts::named("valve"),
    );
    let gated_seen = collect_data(&gated);
    control.set(true);
    source.set(11);
    control.set(false);
    source.set(12);
    control.set(true);
    assert_eq!(*gated_seen.borrow(), vec![10, 11, 12]);

    let snap = g.describe();
    let factories = snap
        .nodes
        .iter()
        .map(|n| (n.id.as_str(), n.factory.as_str()))
        .collect::<Vec<_>>();
    assert!(factories.contains(&("tap", "tap")));
    assert!(factories.contains(&("first_data", "onFirstData")));
    assert!(factories.contains(&("rescue", "rescue")));
    assert!(factories.contains(&("valve", "valve")));
}

#[test]
fn static_combinators_use_declared_deps_and_state() {
    let g = graph();

    let left = g.state_empty::<i32>();
    let right = g.state_empty::<i32>();
    let combined = g.init_node(
        combine::<i32>(),
        vec![left.erased(), right.erased()],
        GraphNodeOpts::named("combine"),
    );
    let combined_seen = collect_data(&combined);
    left.set(1);
    assert!(combined_seen.borrow().is_empty());
    right.set(2);
    left.set(3);
    assert_eq!(*combined_seen.borrow(), vec![vec![1, 2], vec![3, 2]]);

    let primary = g.state(1i32);
    let secondary = g.state(10i32);
    let with_latest = g.init_node(
        with_latest_from::<i32, i32>(),
        vec![primary.erased(), secondary.erased()],
        GraphNodeOpts::named("with_latest"),
    );
    let with_latest_seen = collect_data(&with_latest);
    secondary.set(20);
    primary.set(2);
    assert_eq!(*with_latest_seen.borrow(), vec![(1, 10), (2, 20)]);

    let a = g.init_node(
        from_iter(vec![1i32, 2]),
        vec![],
        GraphNodeOpts::named("zip_a"),
    );
    let b = g.init_node(
        from_iter(vec![10i32, 20, 30]),
        vec![],
        GraphNodeOpts::named("zip_b"),
    );
    let zipped = g.init_node(
        zip::<i32>(),
        vec![a.erased(), b.erased()],
        GraphNodeOpts::named("zip"),
    );
    assert_eq!(
        *collect_data(&zipped).borrow(),
        vec![vec![1, 10], vec![2, 20]]
    );

    let empty_zip = g.init_node(zip::<i32>(), vec![], GraphNodeOpts::named("zip_empty"));
    assert_eq!(
        *collect_shapes::<Vec<i32>>(&empty_zip).borrow(),
        vec!["COMPLETE"]
    );

    let a = g.init_node(
        from_iter(vec![1i32, 2]),
        vec![],
        GraphNodeOpts::named("concat_a"),
    );
    let b = g.init_node(
        from_iter(vec![3i32, 4]),
        vec![],
        GraphNodeOpts::named("concat_b"),
    );
    let concatted = g.init_node(
        concat::<i32>(),
        vec![a.erased(), b.erased()],
        GraphNodeOpts::named("concat"),
    );
    assert_eq!(*collect_data(&concatted).borrow(), vec![1, 2, 3, 4]);
}

#[test]
fn notifier_combinators_and_race_follow_static_edges() {
    let g = graph();

    let left = g.state_empty::<i32>();
    let right = g.state_empty::<i32>();
    let raced = g.init_node(
        race::<i32>(),
        vec![left.erased(), right.erased()],
        GraphNodeOpts::named("race"),
    );
    let race_seen = collect_data(&raced);
    right.set(9);
    left.set(1);
    right.set(10);
    assert_eq!(*race_seen.borrow(), vec![9, 10]);

    let source = g.state(1i32);
    let notifier = g.state_empty::<()>();
    let buffered = g.init_node(
        buffer::<i32>(),
        vec![source.erased(), notifier.erased()],
        GraphNodeOpts::named("buffer"),
    );
    let buffer_seen = collect_data(&buffered);
    source.set(2);
    notifier.set(());
    source.set(3);
    source.down(vec![Message::Complete]);
    assert_eq!(*buffer_seen.borrow(), vec![vec![1, 2], vec![3]]);

    let src = g.init_node(
        from_iter(vec![1i32, 2, 3]),
        vec![],
        GraphNodeOpts::named("buffer_count_src"),
    );
    let counted = g.init_node(
        buffer_count::<i32>(2),
        vec![src.erased()],
        GraphNodeOpts::named("buffer_count"),
    );
    assert_eq!(*collect_data(&counted).borrow(), vec![vec![1, 2], vec![3]]);

    let source = g.state(5i32);
    let notifier = g.state_empty::<()>();
    let sampled = g.init_node(
        sample::<i32>(),
        vec![source.erased(), notifier.erased()],
        GraphNodeOpts::named("sample"),
    );
    let sample_seen = collect_data(&sampled);
    notifier.set(());
    source.set(6);
    notifier.set(());
    assert_eq!(*sample_seen.borrow(), vec![5, 6]);

    let source = g.state(5i32);
    let notifier = g.state_empty::<()>();
    let sampled = g.init_node(
        sample::<i32>(),
        vec![source.erased(), notifier.erased()],
        GraphNodeOpts::named("sample_error"),
    );
    let sample_shapes = collect_shapes::<i32>(&sampled);
    batch(|_| {
        source.down(vec![Message::Complete]);
        notifier.down(vec![Message::Error("sample notifier failed".into())]);
    });
    assert_eq!(*sample_shapes.borrow(), vec!["ERROR"]);

    let source = g.state(7i32);
    let notifier = g.state_empty::<()>();
    let until = g.init_node(
        take_until::<i32>(),
        vec![source.erased(), notifier.erased()],
        GraphNodeOpts::named("take_until"),
    );
    let until_seen = collect_shapes::<i32>(&until);
    source.set(8);
    notifier.set(());
    source.set(9);
    assert_eq!(*until_seen.borrow(), vec!["DATA", "DATA", "COMPLETE"]);
}

#[test]
fn existing_core_catalog_still_composes_after_widening() {
    let g = graph();
    let source = g.init_node(
        from_iter(vec![1i32, 1, 2, 2, 3]),
        vec![],
        GraphNodeOpts::named("source"),
    );
    let mapped = g.init_node(
        map::<i32, i32>(|v| v * 2),
        vec![source.erased()],
        GraphNodeOpts::named("map"),
    );
    let filtered = g.init_node(
        filter::<i32>(|v| *v >= 4),
        vec![mapped.erased()],
        GraphNodeOpts::named("filter"),
    );
    let distinct = g.init_node(
        distinct_until_changed::<i32>(|a, b| a == b),
        vec![filtered.erased()],
        GraphNodeOpts::named("distinct"),
    );
    let first_two = g.init_node(
        take::<i32>(2),
        vec![distinct.erased()],
        GraphNodeOpts::named("take"),
    );

    assert_eq!(*collect_data(&first_two).borrow(), vec![4, 6]);

    let src = g.init_node(
        from_iter(vec![1i32, 1, 1]),
        vec![],
        GraphNodeOpts::named("settle_src"),
    );
    let settled = g.init_node(
        settle::<i32>(1, Some(3)),
        vec![src.erased()],
        GraphNodeOpts::named("settle"),
    );
    assert_eq!(
        *collect_shapes::<i32>(&settled).borrow(),
        vec!["DATA", "DATA", "COMPLETE"]
    );
}
