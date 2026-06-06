use std::cell::RefCell;
use std::rc::Rc;

use graphrefly::{
    graph, reactive_list, GraphNodeOpts, ListChange, Message, Node, ReactiveListOptions,
};

fn collect_data<T: Clone + 'static>(node: &Node<T>) -> Rc<RefCell<Vec<T>>> {
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

fn collect_kinds<T: Clone + 'static>(node: &Node<T>) -> Rc<RefCell<Vec<&'static str>>> {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let seen_sink = seen.clone();
    let _keep = node.subscribe(move |msg| {
        seen_sink.borrow_mut().push(match msg {
            Message::Start => "START",
            Message::Pause(_) => "PAUSE",
            Message::Resume(_) => "RESUME",
            Message::Dirty => "DIRTY",
            Message::Data(_) => "DATA",
            Message::Resolved => "RESOLVED",
            Message::Invalidate => "INVALIDATE",
            Message::Complete => "COMPLETE",
            Message::Error(_) => "ERROR",
            Message::Teardown => "TEARDOWN",
        });
    });
    seen
}

fn demand_snapshot<T: Clone + 'static>(list: &graphrefly::ReactiveList<T>) {
    list.snapshot
        .up(vec![Message::Resume(list.pull_id.clone())]);
}

#[test]
fn reactive_list_delta_and_quick_reads() {
    let list = reactive_list::<i32>(vec![1], ReactiveListOptions::default());
    let deltas = collect_data(&list.delta);
    let delta_kinds = collect_kinds(&list.delta);

    list.append(2);
    list.append_many(vec![3, 4]);
    list.insert(1, 9);
    assert_eq!(list.pop(None), 4);
    list.clear();

    assert_eq!(
        *deltas.borrow(),
        vec![
            ListChange::Append { value: 2 },
            ListChange::AppendMany { values: vec![3, 4] },
            ListChange::Insert { index: 1, value: 9 },
            ListChange::Pop { index: 4, value: 4 },
            ListChange::Clear { count: 4 },
        ]
    );
    let kinds = delta_kinds.borrow();
    assert_eq!(kinds.first(), Some(&"START"));
    assert!(!kinds.contains(&"RESOLVED"));
    assert!(kinds
        .iter()
        .skip(1)
        .copied()
        .collect::<Vec<_>>()
        .chunks_exact(2)
        .all(|pair| pair == ["DIRTY", "DATA"]));
    assert!(list.is_empty());
    assert_eq!(list.at(0), None);
}

#[test]
fn reactive_list_snapshot_is_pull_driven_and_coalesces_no_change_demand() {
    let list = reactive_list::<i32>(Vec::new(), ReactiveListOptions::default());
    let other = reactive_list::<i32>(Vec::new(), ReactiveListOptions::default());
    assert_ne!(list.pull_id, other.pull_id);
    let snapshots = collect_data(&list.snapshot);

    list.append(1);
    list.append(2);
    assert!(snapshots.borrow().is_empty());

    demand_snapshot(&list);
    assert_eq!(*snapshots.borrow(), vec![vec![1, 2]]);

    demand_snapshot(&list);
    assert_eq!(
        *snapshots.borrow(),
        vec![vec![1, 2]],
        "a demand with no intervening delta stays silent (D60 over R-pull)"
    );

    list.append(3);
    demand_snapshot(&list);
    assert_eq!(*snapshots.borrow(), vec![vec![1, 2], vec![1, 2, 3]]);
}

#[test]
fn reactive_list_capacity_trims_head_and_emits_delta() {
    let list = reactive_list::<i32>(Vec::new(), ReactiveListOptions::default().max_size(2));
    let deltas = collect_data(&list.delta);

    list.append_many(vec![1, 2, 3]);

    assert_eq!(list.to_vec(), vec![2, 3]);
    assert_eq!(
        *deltas.borrow(),
        vec![
            ListChange::AppendMany {
                values: vec![1, 2, 3]
            },
            ListChange::TrimHead { n: 1 },
        ]
    );
}

#[test]
fn reactive_list_append_from_is_graph_visible_declared_dep() {
    let g = graph();
    let src = g.state_opts(0i32, GraphNodeOpts::named("src"));
    let list = reactive_list::<i32>(
        Vec::new(),
        ReactiveListOptions::named("list").graph(g.clone()),
    );
    let deltas = collect_data(&list.delta);

    let dispose = list.append_from(&src);
    src.set(1);
    src.set(2);

    assert_eq!(
        *deltas.borrow(),
        vec![
            ListChange::Append { value: 0 },
            ListChange::Append { value: 1 },
            ListChange::Append { value: 2 },
        ]
    );
    assert_eq!(list.to_vec(), vec![0, 1, 2]);
    let snap = g.describe();
    assert!(snap.edges.contains(&graphrefly::DescribeEdge {
        from: "src".to_owned(),
        to: "list.bind#0".to_owned(),
    }));
    assert_eq!(
        snap.nodes
            .iter()
            .find(|node| node.id == "list.bind#0")
            .map(|node| node.factory.as_str()),
        Some("reactiveList.bindSource")
    );

    dispose();
    let snap_after_dispose = g.describe();
    assert!(!snap_after_dispose
        .edges
        .contains(&graphrefly::DescribeEdge {
            from: "src".to_owned(),
            to: "list.bind#0".to_owned(),
        }));
    src.set(3);
    assert_eq!(list.to_vec(), vec![0, 1, 2]);
}

#[test]
fn unnamed_graph_reactive_lists_get_distinct_public_ids() {
    let g = graph();
    let a = reactive_list::<i32>(Vec::new(), ReactiveListOptions::default().graph(g.clone()));
    let b = reactive_list::<i32>(Vec::new(), ReactiveListOptions::default().graph(g.clone()));

    assert_ne!(a.pull_id, b.pull_id);
    let snap = g.describe();
    let ids = snap.nodes.iter().map(|n| n.id.as_str()).collect::<Vec<_>>();
    assert_eq!(ids.iter().filter(|id| id.ends_with(".delta")).count(), 2);
    assert_eq!(ids.iter().filter(|id| id.ends_with(".snapshot")).count(), 2);
}

#[test]
#[should_panic(expected = "insert: index 5 out of range")]
fn reactive_list_insert_out_of_range_panics() {
    let list = reactive_list::<i32>(vec![1, 2], ReactiveListOptions::default());
    list.insert(5, 9);
}

#[test]
#[should_panic(expected = "pop from empty list")]
fn reactive_list_pop_empty_panics() {
    let list = reactive_list::<i32>(Vec::new(), ReactiveListOptions::default());
    list.pop(None);
}
