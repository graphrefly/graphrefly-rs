use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;

use graphrefly::{
    graph, merge_reactive_logs, reactive_index, reactive_list, reactive_log, reactive_map,
    scan_log, GraphNodeOpts, IndexChange, IndexRow, ListChange, LogChange, MapChange, Message,
    Node, ReactiveIndexOptions, ReactiveListOptions, ReactiveLogOptions, ReactiveMapOptions,
    TopologyEvent, TopologyEventKind,
};

type DataCollector<T> = (Rc<RefCell<Vec<T>>>, Box<dyn FnOnce()>);
type StringNumberPredicate = Rc<dyn Fn(&i32, &String) -> bool>;

fn collect_data<T: Clone + 'static>(node: &Node<T>) -> Rc<RefCell<Vec<T>>> {
    collect_data_with_unsub(node).0
}

fn collect_data_with_unsub<T: Clone + 'static>(node: &Node<T>) -> DataCollector<T> {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let seen_sink = seen.clone();
    let keep = node.subscribe(move |msg| {
        if let Message::Data(value) = msg {
            if let Some(v) = value.as_ref().downcast_ref::<T>() {
                seen_sink.borrow_mut().push(v.clone());
            }
        }
    });
    (seen, keep)
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

fn demand_log_snapshot<T: Clone + 'static>(log: &graphrefly::ReactiveLog<T>) {
    log.snapshot.up(vec![Message::Resume(log.pull_id.clone())]);
}

fn demand_view_snapshot<C: Clone + 'static, S: Clone + 'static>(
    view: &graphrefly::ReactiveView<C, S>,
) {
    view.snapshot
        .up(vec![Message::Resume(view.pull_id.clone())]);
}

fn demand_index_snapshot<K, S, V>(index: &graphrefly::ReactiveIndex<K, S, V>)
where
    K: Clone + Ord + std::fmt::Debug + 'static,
    S: Clone + Ord + 'static,
    V: Clone + 'static,
{
    index
        .snapshot
        .up(vec![Message::Resume(index.pull_id.clone())]);
}

fn demand_map_snapshot<K, V>(map: &graphrefly::ReactiveMap<K, V>)
where
    K: Clone + Ord + std::fmt::Debug + 'static,
    V: Clone + 'static,
{
    map.snapshot.up(vec![Message::Resume(map.pull_id.clone())]);
}

fn btree<K: Ord, V>(entries: impl IntoIterator<Item = (K, V)>) -> BTreeMap<K, V> {
    entries.into_iter().collect()
}

#[derive(Clone, Eq, PartialEq)]
struct DebugCollisionKey(i32);

impl Ord for DebugCollisionKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for DebugCollisionKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Debug for DebugCollisionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("same-debug")
    }
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
fn reactive_log_delta_quick_reads_and_pull_snapshot() {
    let log = reactive_log::<i32>(vec![1], ReactiveLogOptions::default().max_size(3));
    let deltas = collect_data(&log.delta);
    let snapshots = collect_data(&log.snapshot);

    log.append(2);
    log.append_many(vec![3, 4]);
    assert_eq!(log.to_vec(), vec![2, 3, 4]);
    assert_eq!(log.len(), 3);
    assert_eq!(log.at(-1), Some(4));
    assert_eq!(
        *deltas.borrow(),
        vec![
            LogChange::Append { value: 2 },
            LogChange::AppendMany { values: vec![3, 4] },
            LogChange::TrimHead { n: 1 },
        ]
    );
    assert!(snapshots.borrow().is_empty());

    demand_log_snapshot(&log);
    assert_eq!(*snapshots.borrow(), vec![vec![2, 3, 4]]);
    demand_log_snapshot(&log);
    assert_eq!(
        *snapshots.borrow(),
        vec![vec![2, 3, 4]],
        "D60/R-pull: demand with no intervening delta coalesces"
    );

    log.trim_head(2);
    log.clear();
    assert_eq!(
        *deltas.borrow(),
        vec![
            LogChange::Append { value: 2 },
            LogChange::AppendMany { values: vec![3, 4] },
            LogChange::TrimHead { n: 1 },
            LogChange::TrimHead { n: 2 },
            LogChange::Clear { count: 1 },
        ]
    );
}

#[test]
fn reactive_log_incremental_views_follow_delta_backbone() {
    let log = reactive_log::<i32>(Vec::new(), ReactiveLogOptions::default());
    let tail = log.tail(2);
    let slice = log.slice(1, Some(3));
    let sum = log.scan(0i32, |acc, value| acc + *value);
    let tail_seen = collect_data(&tail);
    let slice_seen = collect_data(&slice);
    let sum_seen = collect_data(&sum);

    log.append(1);
    log.append(2);
    log.append(3);

    assert_eq!(*tail_seen.borrow(), vec![vec![1], vec![1, 2], vec![2, 3]]);
    assert_eq!(*slice_seen.borrow(), vec![vec![], vec![2], vec![2, 3]]);
    assert_eq!(*sum_seen.borrow(), vec![1, 3, 6]);

    log.trim_head(2);
    assert_eq!(
        *tail_seen.borrow(),
        vec![vec![1], vec![1, 2], vec![2, 3], vec![3]]
    );
    assert_eq!(*sum_seen.borrow(), vec![1, 3, 6, 3]);
}

#[test]
fn reactive_log_scan_recomputes_after_max_size_trimmed_append() {
    let log = reactive_log::<i32>(Vec::new(), ReactiveLogOptions::default().max_size(3));
    let sum = log.scan(0i32, |acc, value| acc + *value);
    let sum_seen = collect_data(&sum);

    log.append_many(vec![1, 2, 3]);
    log.append(4);

    assert_eq!(
        *sum_seen.borrow(),
        vec![6, 9],
        "scan must include the appended tail value after a ring-buffer head trim"
    );
}

#[test]
fn reactive_log_scan_recomputes_after_oversized_append_many_trim() {
    let log = reactive_log::<i32>(Vec::new(), ReactiveLogOptions::default().max_size(3));
    let sum = log.scan(0i32, |acc, value| acc + *value);
    let sum_seen = collect_data(&sum);

    log.append_many(vec![1, 2, 3, 4, 5]);

    assert_eq!(
        sum_seen.borrow().last().copied(),
        Some(12),
        "scan must converge on the retained tail after an append_many overflow trim"
    );
}

#[test]
fn reactive_log_page_is_light_view_and_lazy_pull() {
    let g = graph();
    let log = reactive_log::<i32>(vec![1, 2, 3, 4], ReactiveLogOptions::named("log").graph(g));
    let page = log.page(1, 2);
    let same = log.page(1, 2);
    assert_eq!(page.pull_id, same.pull_id);
    let (page_deltas, unsub_page_deltas) = collect_data_with_unsub(&page.delta);
    let (pages, unsub_pages) = collect_data_with_unsub(&page.snapshot);

    log.append(5);
    assert_eq!(
        *page_deltas.borrow(),
        vec![LogChange::Append { value: 5 }],
        "D121: light view delta forwards parent mutations"
    );
    assert!(pages.borrow().is_empty());

    demand_view_snapshot(&page);
    assert_eq!(*pages.borrow(), vec![vec![2, 3]]);

    unsub_pages();
    unsub_page_deltas();
    page.dispose();
    let rebuilt = log.page(1, 2);
    assert!(
        rebuilt.pull_id != page.pull_id,
        "disposing a memoized page removes it from the memo table"
    );
    rebuilt.dispose();
}

#[test]
fn reactive_index_range_is_light_view_and_lazy_pull() {
    let g = graph();
    let index = reactive_index::<i32, String, i32>(
        Vec::new(),
        ReactiveIndexOptions::named("idx").graph(g.clone()),
    );
    let deltas = collect_data(&index.delta);
    let snapshots = collect_data(&index.snapshot);

    index.upsert(30, "z".to_owned(), 300);
    index.upsert(10, "a".to_owned(), 100);
    index.upsert(20, "m".to_owned(), 200);

    assert_eq!(index.range_by_primary(&10, &30), vec![100, 200]);
    assert_eq!(
        *deltas.borrow(),
        vec![
            IndexChange::Upsert {
                primary: 30,
                secondary: "z".to_owned(),
                value: 300,
            },
            IndexChange::Upsert {
                primary: 10,
                secondary: "a".to_owned(),
                value: 100,
            },
            IndexChange::Upsert {
                primary: 20,
                secondary: "m".to_owned(),
                value: 200,
            },
        ]
    );
    assert!(snapshots.borrow().is_empty());

    demand_index_snapshot(&index);
    assert_eq!(
        *snapshots.borrow(),
        vec![vec![
            IndexRow {
                primary: 10,
                secondary: "a".to_owned(),
                value: 100,
            },
            IndexRow {
                primary: 20,
                secondary: "m".to_owned(),
                value: 200,
            },
            IndexRow {
                primary: 30,
                secondary: "z".to_owned(),
                value: 300,
            },
        ]]
    );

    let range = index.range(10, 30);
    let (range_deltas, unsub_range_deltas) = collect_data_with_unsub(&range.delta);
    let (ranges, unsub_ranges) = collect_data_with_unsub(&range.snapshot);
    index.upsert(25, "b".to_owned(), 250);
    assert_eq!(
        *range_deltas.borrow(),
        vec![
            IndexChange::Upsert {
                primary: 20,
                secondary: "m".to_owned(),
                value: 200,
            },
            IndexChange::Upsert {
                primary: 25,
                secondary: "b".to_owned(),
                value: 250,
            },
        ]
    );
    demand_view_snapshot(&range);
    assert_eq!(*ranges.borrow(), vec![vec![100, 200, 250]]);

    unsub_ranges();
    unsub_range_deltas();
    range.dispose();
}

#[test]
fn reactive_index_range_memo_uses_structural_keys_not_debug_text() {
    let index = reactive_index::<DebugCollisionKey, i32, i32>(
        vec![
            IndexRow {
                primary: DebugCollisionKey(1),
                secondary: 1,
                value: 10,
            },
            IndexRow {
                primary: DebugCollisionKey(2),
                secondary: 2,
                value: 20,
            },
            IndexRow {
                primary: DebugCollisionKey(3),
                secondary: 3,
                value: 30,
            },
        ],
        ReactiveIndexOptions::default(),
    );

    let first = index.range(DebugCollisionKey(1), DebugCollisionKey(3));
    let second = index.range(DebugCollisionKey(2), DebugCollisionKey(4));
    let (first_seen, unsub_first) = collect_data_with_unsub(&first.snapshot);
    let (second_seen, unsub_second) = collect_data_with_unsub(&second.snapshot);

    assert_ne!(first.pull_id, second.pull_id);
    index.upsert(DebugCollisionKey(4), 4, 40);
    demand_view_snapshot(&first);
    demand_view_snapshot(&second);
    assert_eq!(*first_seen.borrow(), vec![vec![10, 20]]);
    assert_eq!(*second_seen.borrow(), vec![vec![20, 30]]);

    unsub_first();
    unsub_second();
    first.dispose();
    second.dispose();
}

#[test]
fn reactive_index_range_dispose_releases_graph_registered_nodes() {
    let g = graph();
    let index = reactive_index::<i32, String, i32>(
        vec![
            IndexRow {
                primary: 1,
                secondary: "a".to_owned(),
                value: 10,
            },
            IndexRow {
                primary: 2,
                secondary: "b".to_owned(),
                value: 20,
            },
        ],
        ReactiveIndexOptions::named("idx").graph(g.clone()),
    );
    let range = index.range(1, 3);

    let snap = g.describe();
    assert!(snap
        .nodes
        .iter()
        .any(|node| node.id == "idx.range#0.delta" && node.factory == "reactiveIndex.range.delta"));
    assert!(snap
        .nodes
        .iter()
        .any(|node| node.id == "idx.range#0.snapshot"
            && node.factory == "reactiveIndex.range.snapshot"));
    assert!(snap.edges.contains(&graphrefly::DescribeEdge {
        from: "idx.delta".to_owned(),
        to: "idx.range#0.delta".to_owned(),
    }));
    assert!(snap.edges.contains(&graphrefly::DescribeEdge {
        from: "idx.range#0.delta".to_owned(),
        to: "idx.range#0.snapshot".to_owned(),
    }));

    range.dispose();

    assert!(g.find("idx.range#0.delta").is_none());
    assert!(g.find("idx.range#0.snapshot").is_none());
    let rebuilt = index.range(1, 3);
    assert!(g.find("idx.range#1.delta").is_some());
    rebuilt.dispose();
}

#[test]
fn reactive_index_dispose_releases_memoized_range_views() {
    let g = graph();
    let index = reactive_index::<i32, String, i32>(
        vec![IndexRow {
            primary: 1,
            secondary: "a".to_owned(),
            value: 10,
        }],
        ReactiveIndexOptions::named("idx").graph(g.clone()),
    );
    let _range = index.range(1, 2);

    index.dispose();

    assert!(g.find("idx.range#0.delta").is_none());
    assert!(g.find("idx.range#0.snapshot").is_none());
}

#[test]
fn reactive_log_page_dispose_releases_graph_registered_nodes() {
    let g = graph();
    let log = reactive_log::<i32>(
        vec![1, 2, 3, 4],
        ReactiveLogOptions::named("log").graph(g.clone()),
    );
    let page = log.page(1, 2);

    let snap = g.describe();
    assert!(snap
        .nodes
        .iter()
        .any(|node| node.id == "log.page#0.delta" && node.factory == "reactiveLog.page.delta"));
    assert!(snap.nodes.iter().any(
        |node| node.id == "log.page#0.snapshot" && node.factory == "reactiveLog.page.snapshot"
    ));
    assert!(snap.edges.contains(&graphrefly::DescribeEdge {
        from: "log.delta".to_owned(),
        to: "log.page#0.delta".to_owned(),
    }));
    assert!(snap.edges.contains(&graphrefly::DescribeEdge {
        from: "log.page#0.delta".to_owned(),
        to: "log.page#0.snapshot".to_owned(),
    }));

    page.dispose();

    let after = g.describe();
    assert!(!after.nodes.iter().any(|node| node.id == "log.page#0.delta"));
    assert!(!after
        .nodes
        .iter()
        .any(|node| node.id == "log.page#0.snapshot"));
    assert!(g.find("log.page#0.delta").is_none());
    let rebuilt = log.page(1, 2);
    let rebuilt_snap = g.describe();
    assert!(rebuilt_snap
        .nodes
        .iter()
        .any(|node| node.id == "log.page#1.delta"));
    rebuilt.dispose();
}

#[test]
fn reactive_log_dispose_releases_memoized_page_views() {
    let g = graph();
    let log = reactive_log::<i32>(
        vec![1, 2, 3],
        ReactiveLogOptions::named("log").graph(g.clone()),
    );
    let _page = log.page(0, 2);

    log.dispose();

    assert!(g.find("log.page#0.delta").is_none());
    assert!(g.find("log.page#0.snapshot").is_none());
}

#[test]
fn graphless_light_views_mint_distinct_pull_ids() {
    let log = reactive_log::<i32>(vec![1, 2, 3], ReactiveLogOptions::default());
    let first = log.page(0, 1);
    let second = log.page(1, 1);

    assert_ne!(first.pull_id, second.pull_id);

    first.dispose();
    second.dispose();
}

#[test]
fn reactive_map_delta_quick_reads_and_pull_snapshot() {
    let map = reactive_map::<String, i32>(vec![("a".to_owned(), 1)], ReactiveMapOptions::default());
    let deltas = collect_data(&map.delta);
    let snapshots = collect_data(&map.snapshot);

    map.set("b".to_owned(), 2);
    map.set_many(vec![("c".to_owned(), 3), ("d".to_owned(), 4)]);
    map.delete(&"a".to_owned());
    map.delete_many(vec!["missing".to_owned(), "c".to_owned()]);
    assert_eq!(map.get(&"b".to_owned()), Some(2));
    assert!(map.has(&"d".to_owned()));
    assert_eq!(map.len(), 2);
    assert_eq!(
        map.to_map(),
        btree([("b".to_owned(), 2), ("d".to_owned(), 4)])
    );

    assert_eq!(
        *deltas.borrow(),
        vec![
            MapChange::Set {
                key: "b".to_owned(),
                value: 2,
            },
            MapChange::Set {
                key: "c".to_owned(),
                value: 3,
            },
            MapChange::Set {
                key: "d".to_owned(),
                value: 4,
            },
            MapChange::Delete {
                key: "a".to_owned(),
                previous: 1,
            },
            MapChange::Delete {
                key: "c".to_owned(),
                previous: 3,
            },
        ]
    );
    assert!(snapshots.borrow().is_empty());

    demand_map_snapshot(&map);
    assert_eq!(
        *snapshots.borrow(),
        vec![btree([("b".to_owned(), 2), ("d".to_owned(), 4)])]
    );
    demand_map_snapshot(&map);
    assert_eq!(
        snapshots.borrow().len(),
        1,
        "D60/R-pull: demand with no intervening delta coalesces"
    );

    map.clear();
    assert!(map.is_empty());
    assert_eq!(deltas.borrow().last(), Some(&MapChange::Clear { count: 2 }));
}

#[test]
fn reactive_map_select_is_light_view_and_lazy_pull() {
    let g = graph();
    let map = reactive_map::<String, i32>(
        vec![("a".to_owned(), 1), ("b".to_owned(), 2)],
        ReactiveMapOptions::named("map").graph(g),
    );
    let is_even: StringNumberPredicate = Rc::new(|value, _| *value % 2 == 0);
    let view = map.select_by(is_even.clone());
    let same = map.select_by(is_even);
    assert_eq!(same.pull_id, view.pull_id);
    let (view_deltas, unsub_view_deltas) = collect_data_with_unsub(&view.delta);
    let (selected, unsub_selected) = collect_data_with_unsub(&view.snapshot);

    map.set("c".to_owned(), 4);

    assert_eq!(
        *view_deltas.borrow(),
        vec![MapChange::Set {
            key: "c".to_owned(),
            value: 4,
        }],
        "D121: light view delta forwards parent mutations"
    );
    assert!(selected.borrow().is_empty());

    demand_view_snapshot(&view);
    assert_eq!(
        *selected.borrow(),
        vec![btree([("b".to_owned(), 2), ("c".to_owned(), 4)])]
    );

    unsub_selected();
    unsub_view_deltas();
    view.dispose();

    let fresh = map.select(|value, _| *value > 0);
    assert_ne!(fresh.pull_id, view.pull_id);
    fresh.dispose();
}

#[test]
fn reactive_map_select_dispose_releases_graph_registered_nodes() {
    let g = graph();
    let map = reactive_map::<String, i32>(
        vec![("a".to_owned(), 1), ("b".to_owned(), 2)],
        ReactiveMapOptions::named("map").graph(g.clone()),
    );
    let select = map.select(|value, _| *value > 1);

    let snap = g.describe();
    assert!(snap
        .nodes
        .iter()
        .any(|node| node.id == "map.select#0.delta" && node.factory == "reactiveMap.select.delta"));
    assert!(snap
        .nodes
        .iter()
        .any(|node| node.id == "map.select#0.snapshot"
            && node.factory == "reactiveMap.select.snapshot"));
    assert!(snap.edges.contains(&graphrefly::DescribeEdge {
        from: "map.delta".to_owned(),
        to: "map.select#0.delta".to_owned(),
    }));
    assert!(snap.edges.contains(&graphrefly::DescribeEdge {
        from: "map.select#0.delta".to_owned(),
        to: "map.select#0.snapshot".to_owned(),
    }));

    select.dispose();

    let after = g.describe();
    assert!(!after
        .nodes
        .iter()
        .any(|node| node.id == "map.select#0.delta"));
    assert!(!after
        .nodes
        .iter()
        .any(|node| node.id == "map.select#0.snapshot"));
    assert!(g.find("map.select#0.delta").is_none());
    let rebuilt = map.select(|value, _| *value > 1);
    let rebuilt_snap = g.describe();
    assert!(rebuilt_snap
        .nodes
        .iter()
        .any(|node| node.id == "map.select#1.delta"));
    rebuilt.dispose();
}

#[test]
fn reactive_map_select_dispose_emits_node_released_after_atomic_release() {
    let g = graph();
    let map = reactive_map::<String, i32>(
        vec![("a".to_owned(), 1), ("b".to_owned(), 2)],
        ReactiveMapOptions::named("map").graph(g.clone()),
    );
    let select = map.select(|value, _| *value > 1);
    let events = Rc::new(RefCell::new(Vec::<TopologyEvent>::new()));
    let event_sink = events.clone();
    let observer = g
        .observe_topology()
        .subscribe(move |event| event_sink.borrow_mut().push(event));

    select.dispose();
    observer.unsubscribe();

    let events = events.borrow();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].kind, TopologyEventKind::NodeReleased);
    assert_eq!(events[0].path, "map.select#0.delta");
    assert_eq!(
        events[0].factory.as_deref(),
        Some("reactiveMap.select.delta")
    );
    assert_eq!(events[0].deps, vec!["map.delta".to_owned()]);
    assert_eq!(events[0].seq, 0);
    assert_eq!(events[1].kind, TopologyEventKind::NodeReleased);
    assert_eq!(events[1].path, "map.select#0.snapshot");
    assert_eq!(
        events[1].factory.as_deref(),
        Some("reactiveMap.select.snapshot")
    );
    assert_eq!(events[1].deps, vec!["map.select#0.delta".to_owned()]);
    assert_eq!(events[1].seq, 1);
    assert!(g.find("map.select#0.delta").is_none());
    assert!(g.find("map.select#0.snapshot").is_none());
    let describe = g.describe();
    let describe_ids = describe
        .nodes
        .iter()
        .map(|node| node.id.as_str())
        .collect::<Vec<_>>();
    assert!(!describe_ids.contains(&"map.select#0.delta"));
    assert!(!describe_ids.contains(&"map.select#0.snapshot"));
    let checkpoint = g.checkpoint().expect("checkpoint succeeds");
    assert!(!checkpoint
        .nodes
        .iter()
        .any(|node| node.id == "map.select#0.delta"));
    assert!(!checkpoint
        .nodes
        .iter()
        .any(|node| node.id == "map.select#0.snapshot"));
}

#[test]
#[should_panic(expected = "still depends")]
fn reactive_log_page_dispose_rejects_external_registered_dependents() {
    let g = graph();
    let log = reactive_log::<i32>(
        vec![1, 2, 3, 4],
        ReactiveLogOptions::named("log").graph(g.clone()),
    );
    let page = log.page(1, 2);
    let _consumer = g.derived_opts(
        vec![page.snapshot.erased()],
        |_| Some(0i32),
        GraphNodeOpts::named("consumer"),
    );

    page.dispose();
}

#[test]
fn reactive_log_page_dispose_rejects_live_external_subscribers_and_can_retry() {
    let g = graph();
    let log = reactive_log::<i32>(
        vec![1, 2, 3, 4],
        ReactiveLogOptions::named("log").graph(g.clone()),
    );
    let page = log.page(1, 2);
    let unsub = page.snapshot.subscribe(|_| {});

    let result = catch_unwind(AssertUnwindSafe(|| page.dispose()));

    assert!(result.is_err());
    assert!(
        g.find("log.page#0.snapshot").is_some(),
        "failed D124 release keeps the view nodes visible for retry"
    );
    unsub();
    page.dispose();
    assert!(g.find("log.page#0.snapshot").is_none());
}

#[test]
fn failed_reactive_log_page_dispose_emits_no_node_released_and_keeps_topology() {
    let g = graph();
    let log = reactive_log::<i32>(
        vec![1, 2, 3, 4],
        ReactiveLogOptions::named("log").graph(g.clone()),
    );
    let page = log.page(1, 2);
    let _consumer = g.derived_opts(
        vec![page.snapshot.erased()],
        |_| Some(0i32),
        GraphNodeOpts::named("consumer"),
    );
    let events = Rc::new(RefCell::new(Vec::<TopologyEvent>::new()));
    let event_sink = events.clone();
    let observer = g
        .observe_topology_path("log.page#0")
        .subscribe(move |event| event_sink.borrow_mut().push(event));

    let result = catch_unwind(AssertUnwindSafe(|| page.dispose()));

    observer.unsubscribe();
    assert!(result.is_err());
    assert!(events.borrow().is_empty());
    assert!(g.find("log.page#0.delta").is_some());
    assert!(g.find("log.page#0.snapshot").is_some());
    let describe = g.describe();
    let describe_ids = describe
        .nodes
        .iter()
        .map(|node| node.id.as_str())
        .collect::<Vec<_>>();
    assert!(describe_ids.contains(&"log.page#0.delta"));
    assert!(describe_ids.contains(&"log.page#0.snapshot"));
    let checkpoint = g.checkpoint().expect("checkpoint succeeds");
    assert!(checkpoint
        .nodes
        .iter()
        .any(|node| node.id == "log.page#0.delta"));
    assert!(checkpoint
        .nodes
        .iter()
        .any(|node| node.id == "log.page#0.snapshot"));
}

#[test]
fn reactive_log_attach_is_graph_visible_declared_dep() {
    let g = graph();
    let src = g.state_opts(0i32, GraphNodeOpts::named("src"));
    let log = reactive_log::<i32>(
        Vec::new(),
        ReactiveLogOptions::named("log").graph(g.clone()),
    );
    let deltas = collect_data(&log.delta);

    let dispose = log.attach(&src);
    src.set(1);
    src.set(2);

    assert_eq!(
        *deltas.borrow(),
        vec![
            LogChange::Append { value: 0 },
            LogChange::Append { value: 1 },
            LogChange::Append { value: 2 },
        ]
    );
    assert_eq!(log.to_vec(), vec![0, 1, 2]);
    let snap = g.describe();
    assert!(snap.edges.contains(&graphrefly::DescribeEdge {
        from: "src".to_owned(),
        to: "log.bind#0".to_owned(),
    }));
    assert_eq!(
        snap.nodes
            .iter()
            .find(|node| node.id == "log.bind#0")
            .map(|node| node.factory.as_str()),
        Some("reactiveLog.bindSource")
    );

    dispose();
    let snap_after_dispose = g.describe();
    assert!(!snap_after_dispose
        .edges
        .contains(&graphrefly::DescribeEdge {
            from: "src".to_owned(),
            to: "log.bind#0".to_owned(),
        }));
    src.set(3);
    assert_eq!(log.to_vec(), vec![0, 1, 2]);
}

#[test]
fn merge_reactive_logs_is_declared_dep_fan_in() {
    let a = reactive_log::<i32>(Vec::new(), ReactiveLogOptions::default());
    let b = reactive_log::<i32>(Vec::new(), ReactiveLogOptions::default());
    let merged = merge_reactive_logs(vec![a.clone(), b.clone()]);
    let seen = collect_data(&merged);

    a.append(1);
    b.append(2);
    a.append_many(vec![3, 4]);

    assert_eq!(
        *seen.borrow(),
        vec![
            LogChange::Append { value: 1 },
            LogChange::Append { value: 2 },
            LogChange::AppendMany { values: vec![3, 4] },
        ]
    );
}

#[test]
fn scan_log_helper_delegates_to_reactive_log_scan() {
    let log = reactive_log::<i32>(Vec::new(), ReactiveLogOptions::default());
    let scanned = scan_log(&log, 1i32, |acc, value| acc * *value);
    let seen = collect_data(&scanned);

    log.append(2);
    log.append(3);

    assert_eq!(*seen.borrow(), vec![2, 6]);
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
