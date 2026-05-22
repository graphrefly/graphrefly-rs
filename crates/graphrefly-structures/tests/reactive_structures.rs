//! Integration tests for all 4 reactive data structures (M5.A).
//!
//! Tests verify:
//! - DIRTY->DATA emission sequence on mutations
//! - Backend operations (append, insert, delete, clear, etc.)
//! - Ring-buffer overflow (ReactiveLog)
//! - Mutation log companion recording
//! - Negative index semantics (at, pop)
//! - Fallible API (pop -> Option, insert -> Result)

mod common;

use common::StructuresRuntime;
use graphrefly_structures::{
    AppendLogMode, AppendLogSink, AttachStorageError, DeleteReason, IndexChange, IndexEqualsFn,
    ListChange, LogChange, MapChange, ReactiveIndex, ReactiveIndexOptions, ReactiveList,
    ReactiveListOptions, ReactiveLog, ReactiveLogOptions, ReactiveMap, ReactiveMapOptions,
    RetentionPolicy, UpsertOptions, ViewSpec,
};

// ===========================================================================
// ReactiveLog tests
// ===========================================================================

#[test]
fn log_append_emits_data() {
    let rt = StructuresRuntime::new();
    let log: ReactiveLog<i64> =
        ReactiveLog::new(rt.core(), rt.intern_vec_fn(), ReactiveLogOptions::default());
    let rec = rt.subscribe_recorder(log.node_id);

    log.append(rt.core(), 1);
    log.append(rt.core(), 2);

    assert_eq!(log.size(), 2);
    assert_eq!(log.to_vec(), vec![1, 2]);
    assert_eq!(rec.data_count(), 2);
}

#[test]
fn log_append_many() {
    let rt = StructuresRuntime::new();
    let log: ReactiveLog<i64> =
        ReactiveLog::new(rt.core(), rt.intern_vec_fn(), ReactiveLogOptions::default());
    let rec = rt.subscribe_recorder(log.node_id);

    log.append_many(rt.core(), vec![1, 2, 3]);

    assert_eq!(log.size(), 3);
    assert_eq!(log.to_vec(), vec![1, 2, 3]);
    assert_eq!(rec.data_count(), 1);
}

#[test]
fn log_ring_buffer_overflow() {
    let rt = StructuresRuntime::new();
    let log: ReactiveLog<i64> = ReactiveLog::new(
        rt.core(),
        rt.intern_vec_fn(),
        ReactiveLogOptions {
            max_size: Some(3),
            ..Default::default()
        },
    );

    log.append_many(rt.core(), vec![1, 2, 3, 4, 5]);

    assert_eq!(log.size(), 3);
    assert_eq!(log.to_vec(), vec![3, 4, 5]);
}

#[test]
fn log_clear_and_trim_head() {
    let rt = StructuresRuntime::new();
    let log: ReactiveLog<i64> =
        ReactiveLog::new(rt.core(), rt.intern_vec_fn(), ReactiveLogOptions::default());

    log.append_many(rt.core(), vec![1, 2, 3, 4, 5]);
    log.trim_head(rt.core(), 2);
    assert_eq!(log.to_vec(), vec![3, 4, 5]);

    log.clear(rt.core());
    assert_eq!(log.size(), 0);
    assert_eq!(log.to_vec(), Vec::<i64>::new());
}

#[test]
fn log_negative_index() {
    let rt = StructuresRuntime::new();
    let log: ReactiveLog<i64> =
        ReactiveLog::new(rt.core(), rt.intern_vec_fn(), ReactiveLogOptions::default());

    log.append_many(rt.core(), vec![10, 20, 30]);

    assert_eq!(log.at(-1), Some(30));
    assert_eq!(log.at(-3), Some(10));
    assert_eq!(log.at(-4), None);
    assert_eq!(log.at(0), Some(10));
    assert_eq!(log.at(2), Some(30));
    assert_eq!(log.at(3), None);
}

#[test]
fn log_mutation_log() {
    let rt = StructuresRuntime::new();
    let log: ReactiveLog<i64> = ReactiveLog::new(
        rt.core(),
        rt.intern_vec_fn(),
        ReactiveLogOptions {
            mutation_log: true,
            ..Default::default()
        },
    );

    log.append(rt.core(), 1);
    log.append_many(rt.core(), vec![2, 3]);
    log.trim_head(rt.core(), 1);
    log.clear(rt.core());

    let entries = log.mutation_log_snapshot().expect("mutation log enabled");
    assert_eq!(entries.len(), 4);
    assert!(matches!(entries[0].change, LogChange::Append { value: 1 }));
    assert!(matches!(&entries[1].change, LogChange::AppendMany { values } if values == &[2, 3]));
    assert!(matches!(entries[2].change, LogChange::TrimHead { n: 1 }));
    assert!(matches!(entries[3].change, LogChange::Clear { count: 2 }));
}

#[test]
fn log_empty_operations_dont_emit() {
    let rt = StructuresRuntime::new();
    let log: ReactiveLog<i64> =
        ReactiveLog::new(rt.core(), rt.intern_vec_fn(), ReactiveLogOptions::default());
    let rec = rt.subscribe_recorder(log.node_id);

    log.append_many(rt.core(), vec![]);
    log.clear(rt.core());
    log.trim_head(rt.core(), 5);

    assert_eq!(rec.data_count(), 0);
}

// ===========================================================================
// ReactiveList tests
// ===========================================================================

#[test]
fn list_append_and_insert() {
    let rt = StructuresRuntime::new();
    let list: ReactiveList<String> = ReactiveList::new(
        rt.core(),
        rt.intern_vec_fn(),
        ReactiveListOptions::default(),
    );
    let rec = rt.subscribe_recorder(list.node_id);

    list.append(rt.core(), "a".into());
    list.append(rt.core(), "c".into());
    list.insert(rt.core(), 1, "b".into()).unwrap();

    assert_eq!(list.to_vec(), vec!["a", "b", "c"]);
    assert_eq!(rec.data_count(), 3);
}

#[test]
fn list_pop() {
    let rt = StructuresRuntime::new();
    let list: ReactiveList<i64> = ReactiveList::new(
        rt.core(),
        rt.intern_vec_fn(),
        ReactiveListOptions::default(),
    );

    list.append_many(rt.core(), vec![10, 20, 30]);

    assert_eq!(list.pop(rt.core(), -1), Some(30));
    assert_eq!(list.to_vec(), vec![10, 20]);

    assert_eq!(list.pop(rt.core(), 0), Some(10));
    assert_eq!(list.to_vec(), vec![20]);
}

#[test]
fn list_pop_oob_returns_none() {
    let rt = StructuresRuntime::new();
    let list: ReactiveList<i64> = ReactiveList::new(
        rt.core(),
        rt.intern_vec_fn(),
        ReactiveListOptions::default(),
    );
    let rec = rt.subscribe_recorder(list.node_id);

    assert_eq!(list.pop(rt.core(), 0), None);
    list.append(rt.core(), 1);
    assert_eq!(list.pop(rt.core(), 5), None);
    assert_eq!(list.pop(rt.core(), -2), None);

    assert_eq!(rec.data_count(), 1); // only the append emitted
}

#[test]
fn list_insert_oob_returns_err() {
    let rt = StructuresRuntime::new();
    let list: ReactiveList<i64> = ReactiveList::new(
        rt.core(),
        rt.intern_vec_fn(),
        ReactiveListOptions::default(),
    );
    let rec = rt.subscribe_recorder(list.node_id);

    assert!(list.insert(rt.core(), 1, 42).is_err());
    assert!(list.insert_many(rt.core(), 1, vec![1, 2]).is_err());
    assert_eq!(list.size(), 0);
    assert_eq!(rec.data_count(), 0);
}

#[test]
fn list_insert_many() {
    let rt = StructuresRuntime::new();
    let list: ReactiveList<i64> = ReactiveList::new(
        rt.core(),
        rt.intern_vec_fn(),
        ReactiveListOptions::default(),
    );

    list.append_many(rt.core(), vec![1, 4]);
    list.insert_many(rt.core(), 1, vec![2, 3]).unwrap();

    assert_eq!(list.to_vec(), vec![1, 2, 3, 4]);
}

#[test]
fn list_clear() {
    let rt = StructuresRuntime::new();
    let list: ReactiveList<i64> = ReactiveList::new(
        rt.core(),
        rt.intern_vec_fn(),
        ReactiveListOptions::default(),
    );
    let rec = rt.subscribe_recorder(list.node_id);

    list.append_many(rt.core(), vec![1, 2, 3]);
    list.clear(rt.core());

    assert_eq!(list.size(), 0);
    assert_eq!(rec.data_count(), 2);
}

#[test]
fn list_negative_index() {
    let rt = StructuresRuntime::new();
    let list: ReactiveList<i64> = ReactiveList::new(
        rt.core(),
        rt.intern_vec_fn(),
        ReactiveListOptions::default(),
    );

    list.append_many(rt.core(), vec![10, 20, 30]);

    assert_eq!(list.at(-1), Some(30));
    assert_eq!(list.at(-3), Some(10));
    assert_eq!(list.at(-4), None);
}

#[test]
fn list_mutation_log() {
    let rt = StructuresRuntime::new();
    let list: ReactiveList<i64> = ReactiveList::new(
        rt.core(),
        rt.intern_vec_fn(),
        ReactiveListOptions {
            mutation_log: true,
            ..Default::default()
        },
    );

    list.append(rt.core(), 1);
    list.insert(rt.core(), 0, 0).unwrap();
    let _ = list.pop(rt.core(), -1);
    list.clear(rt.core());

    let entries = list.mutation_log_snapshot().expect("mutation log enabled");
    assert_eq!(entries.len(), 4);
    assert!(matches!(entries[0].change, ListChange::Append { value: 1 }));
    assert!(matches!(
        entries[1].change,
        ListChange::Insert { index: 0, value: 0 }
    ));
    assert!(matches!(
        entries[2].change,
        ListChange::Pop {
            index: -1,
            value: 1
        }
    ));
    assert!(matches!(entries[3].change, ListChange::Clear { count: 1 }));
}

// ===========================================================================
// ReactiveMap tests
// ===========================================================================

#[test]
fn map_set_and_get() {
    let rt = StructuresRuntime::new();
    let map: ReactiveMap<String, i64> = ReactiveMap::new(
        rt.core(),
        rt.intern_pairs_fn(),
        ReactiveMapOptions::default(),
    )
    .unwrap();
    let rec = rt.subscribe_recorder(map.node_id);

    map.set(rt.core(), "a".into(), 1);
    map.set(rt.core(), "b".into(), 2);

    assert_eq!(map.size(), 2);
    assert_eq!(map.get(rt.core(), &"a".into()), Some(1));
    assert_eq!(map.get(rt.core(), &"b".into()), Some(2));
    assert_eq!(map.get(rt.core(), &"c".into()), None);
    assert!(map.has(rt.core(), &"a".into()));
    assert!(!map.has(rt.core(), &"c".into()));
    assert_eq!(rec.data_count(), 2);
}

#[test]
fn map_set_many() {
    let rt = StructuresRuntime::new();
    let map: ReactiveMap<String, i64> = ReactiveMap::new(
        rt.core(),
        rt.intern_pairs_fn(),
        ReactiveMapOptions::default(),
    )
    .unwrap();
    let rec = rt.subscribe_recorder(map.node_id);

    map.set_many(
        rt.core(),
        vec![("a".into(), 1), ("b".into(), 2), ("c".into(), 3)],
    );

    assert_eq!(map.size(), 3);
    assert_eq!(rec.data_count(), 1);
}

#[test]
fn map_delete() {
    let rt = StructuresRuntime::new();
    let map: ReactiveMap<String, i64> = ReactiveMap::new(
        rt.core(),
        rt.intern_pairs_fn(),
        ReactiveMapOptions::default(),
    )
    .unwrap();

    map.set_many(rt.core(), vec![("a".into(), 1), ("b".into(), 2)]);
    map.delete(rt.core(), &"a".into());

    assert_eq!(map.size(), 1);
    assert_eq!(map.get(rt.core(), &"a".into()), None);
    assert_eq!(map.get(rt.core(), &"b".into()), Some(2));
}

#[test]
fn map_delete_nonexistent_no_emit() {
    let rt = StructuresRuntime::new();
    let map: ReactiveMap<String, i64> = ReactiveMap::new(
        rt.core(),
        rt.intern_pairs_fn(),
        ReactiveMapOptions::default(),
    )
    .unwrap();
    let rec = rt.subscribe_recorder(map.node_id);

    map.delete(rt.core(), &"x".into());

    assert_eq!(rec.data_count(), 0);
}

#[test]
fn map_clear() {
    let rt = StructuresRuntime::new();
    let map: ReactiveMap<String, i64> = ReactiveMap::new(
        rt.core(),
        rt.intern_pairs_fn(),
        ReactiveMapOptions::default(),
    )
    .unwrap();

    map.set_many(rt.core(), vec![("a".into(), 1), ("b".into(), 2)]);
    map.clear(rt.core());

    assert_eq!(map.size(), 0);
}

#[test]
fn map_mutation_log() {
    let rt = StructuresRuntime::new();
    let map: ReactiveMap<String, i64> = ReactiveMap::new(
        rt.core(),
        rt.intern_pairs_fn(),
        ReactiveMapOptions {
            mutation_log: true,
            ..Default::default()
        },
    )
    .unwrap();

    map.set(rt.core(), "x".into(), 42);
    map.delete(rt.core(), &"x".into());

    let entries = map.mutation_log_snapshot().expect("mutation log enabled");
    assert_eq!(entries.len(), 2);
    assert!(
        matches!(&entries[0].change, MapChange::Set { key, value } if key == "x" && *value == 42)
    );
    assert!(matches!(&entries[1].change, MapChange::Delete { key, .. } if key == "x"));
}

#[test]
fn map_delete_many_only_logs_existing_keys() {
    let rt = StructuresRuntime::new();
    let map: ReactiveMap<String, i64> = ReactiveMap::new(
        rt.core(),
        rt.intern_pairs_fn(),
        ReactiveMapOptions {
            mutation_log: true,
            ..Default::default()
        },
    )
    .unwrap();

    map.set_many(rt.core(), vec![("a".into(), 1), ("b".into(), 2)]);
    map.delete_many(rt.core(), &["a".into(), "nonexistent".into(), "b".into()]);

    let entries = map.mutation_log_snapshot().unwrap();
    let delete_entries: Vec<_> = entries
        .iter()
        .filter(|e| matches!(&e.change, MapChange::Delete { .. }))
        .collect();
    assert_eq!(delete_entries.len(), 2); // only "a" and "b", not "nonexistent"
}

// ===========================================================================
// ReactiveIndex tests
// ===========================================================================

#[test]
fn index_upsert_and_ordering() {
    let rt = StructuresRuntime::new();
    let index: ReactiveIndex<String, i64> = ReactiveIndex::new(
        rt.core(),
        rt.intern_index_fn(),
        ReactiveIndexOptions::default(),
    );
    let rec = rt.subscribe_recorder(index.node_id);

    assert!(index.upsert(rt.core(), "c".into(), "2".into(), 30));
    assert!(index.upsert(rt.core(), "a".into(), "1".into(), 10));
    assert!(index.upsert(rt.core(), "b".into(), "1".into(), 20));

    assert_eq!(index.size(), 3);
    let ordered = index.to_ordered();
    assert_eq!(ordered[0].primary, "a");
    assert_eq!(ordered[1].primary, "b");
    assert_eq!(ordered[2].primary, "c");
    assert_eq!(rec.data_count(), 3);
}

#[test]
fn index_upsert_update() {
    let rt = StructuresRuntime::new();
    let index: ReactiveIndex<String, i64> = ReactiveIndex::new(
        rt.core(),
        rt.intern_index_fn(),
        ReactiveIndexOptions::default(),
    );

    assert!(index.upsert(rt.core(), "a".into(), "1".into(), 10));
    assert!(!index.upsert(rt.core(), "a".into(), "2".into(), 99));

    assert_eq!(index.size(), 1);
    assert_eq!(index.get(&"a".into()), Some(99));
    let ordered = index.to_ordered();
    assert_eq!(ordered[0].secondary, "2");
}

#[test]
fn index_delete() {
    let rt = StructuresRuntime::new();
    let index: ReactiveIndex<String, i64> = ReactiveIndex::new(
        rt.core(),
        rt.intern_index_fn(),
        ReactiveIndexOptions::default(),
    );

    index.upsert(rt.core(), "a".into(), "1".into(), 10);
    index.upsert(rt.core(), "b".into(), "2".into(), 20);
    index.delete(rt.core(), &"a".into());

    assert_eq!(index.size(), 1);
    assert!(!index.has(&"a".into()));
    assert!(index.has(&"b".into()));
}

#[test]
fn index_delete_nonexistent_no_emit() {
    let rt = StructuresRuntime::new();
    let index: ReactiveIndex<String, i64> = ReactiveIndex::new(
        rt.core(),
        rt.intern_index_fn(),
        ReactiveIndexOptions::default(),
    );
    let rec = rt.subscribe_recorder(index.node_id);

    index.delete(rt.core(), &"x".into());

    assert_eq!(rec.data_count(), 0);
}

#[test]
fn index_clear() {
    let rt = StructuresRuntime::new();
    let index: ReactiveIndex<String, i64> = ReactiveIndex::new(
        rt.core(),
        rt.intern_index_fn(),
        ReactiveIndexOptions::default(),
    );

    index.upsert(rt.core(), "a".into(), "1".into(), 10);
    index.upsert(rt.core(), "b".into(), "2".into(), 20);
    index.clear(rt.core());

    assert_eq!(index.size(), 0);
    assert_eq!(index.to_ordered().len(), 0);
}

// D205: range_by_primary — [start, end) inclusive-start/exclusive-end,
// ascending primary order, independent of the secondary sort axis.
#[test]
fn index_range_by_primary_d205() {
    let rt = StructuresRuntime::new();
    let index: ReactiveIndex<i64, i64> = ReactiveIndex::new(
        rt.core(),
        rt.intern_index_fn(),
        ReactiveIndexOptions::default(),
    );

    // Insert out of primary order; the secondary string is the structure's
    // sort axis, but range_by_primary must order by PRIMARY ascending.
    index.upsert(rt.core(), 30, "z".into(), 300);
    index.upsert(rt.core(), 10, "a".into(), 100);
    index.upsert(rt.core(), 20, "m".into(), 200);
    index.upsert(rt.core(), 40, "b".into(), 400);

    // [10, 40): inclusive start, exclusive end → 10,20,30 ascending.
    assert_eq!(index.range_by_primary(&10, &40), vec![100, 200, 300]);
    // Exclusive upper bound excludes 40; single match.
    assert_eq!(index.range_by_primary(&20, &30), vec![200]);
    // start == end → empty.
    assert_eq!(index.range_by_primary(&20, &20), Vec::<i64>::new());
    // start > end → empty.
    assert_eq!(index.range_by_primary(&40, &10), Vec::<i64>::new());
    // Gap with no matches → empty.
    assert_eq!(index.range_by_primary(&11, &19), Vec::<i64>::new());
    // Wide span covers everything, still ascending by primary.
    assert_eq!(index.range_by_primary(&0, &1000), vec![100, 200, 300, 400]);
}

#[test]
fn index_upsert_many() {
    let rt = StructuresRuntime::new();
    let index: ReactiveIndex<String, i64> = ReactiveIndex::new(
        rt.core(),
        rt.intern_index_fn(),
        ReactiveIndexOptions::default(),
    );
    let rec = rt.subscribe_recorder(index.node_id);

    index.upsert_many(
        rt.core(),
        vec![("a".into(), "1".into(), 10), ("b".into(), "2".into(), 20)],
    );

    assert_eq!(index.size(), 2);
    assert_eq!(rec.data_count(), 1);
}

#[test]
fn index_mutation_log() {
    let rt = StructuresRuntime::new();
    let index: ReactiveIndex<String, i64> = ReactiveIndex::new(
        rt.core(),
        rt.intern_index_fn(),
        ReactiveIndexOptions {
            mutation_log: true,
            ..Default::default()
        },
    );

    index.upsert(rt.core(), "a".into(), "1".into(), 10);
    index.delete(rt.core(), &"a".into());

    let entries = index.mutation_log_snapshot().expect("mutation log enabled");
    assert_eq!(entries.len(), 2);
    assert!(matches!(&entries[0].change, IndexChange::Upsert { primary, .. } if primary == "a"));
    assert!(matches!(&entries[1].change, IndexChange::Delete { primary } if primary == "a"));
}

#[test]
fn index_delete_many() {
    let rt = StructuresRuntime::new();
    let index: ReactiveIndex<String, i64> = ReactiveIndex::new(
        rt.core(),
        rt.intern_index_fn(),
        ReactiveIndexOptions::default(),
    );

    index.upsert_many(
        rt.core(),
        vec![
            ("a".into(), "1".into(), 10),
            ("b".into(), "2".into(), 20),
            ("c".into(), "3".into(), 30),
        ],
    );
    index.delete_many(rt.core(), &["a".into(), "c".into()]);

    assert_eq!(index.size(), 1);
    assert!(index.has(&"b".into()));
}

#[test]
fn index_delete_many_only_logs_existing_keys() {
    let rt = StructuresRuntime::new();
    let index: ReactiveIndex<String, i64> = ReactiveIndex::new(
        rt.core(),
        rt.intern_index_fn(),
        ReactiveIndexOptions {
            mutation_log: true,
            ..Default::default()
        },
    );

    index.upsert_many(
        rt.core(),
        vec![("a".into(), "1".into(), 10), ("b".into(), "2".into(), 20)],
    );
    index.delete_many(rt.core(), &["a".into(), "nonexistent".into()]);

    let entries = index.mutation_log_snapshot().unwrap();
    let delete_entries: Vec<_> = entries
        .iter()
        .filter(|e| matches!(&e.change, IndexChange::DeleteMany { .. }))
        .collect();
    assert_eq!(delete_entries.len(), 1);
    if let IndexChange::DeleteMany { primaries } = &delete_entries[0].change {
        assert_eq!(primaries.len(), 1); // only "a", not "nonexistent"
        assert_eq!(primaries[0], "a");
    } else {
        panic!("expected DeleteMany");
    }
}

// ===========================================================================
// Cross-cutting: version monotonicity in mutation log
// ===========================================================================

#[test]
fn mutation_log_versions_are_monotonic() {
    let rt = StructuresRuntime::new();
    let log: ReactiveLog<i64> = ReactiveLog::new(
        rt.core(),
        rt.intern_vec_fn(),
        ReactiveLogOptions {
            mutation_log: true,
            ..Default::default()
        },
    );

    log.append(rt.core(), 1);
    log.append(rt.core(), 2);
    log.append(rt.core(), 3);

    let entries = log.mutation_log_snapshot().unwrap();
    let versions: Vec<u64> = entries
        .iter()
        .map(|e| match &e.version {
            graphrefly_structures::Version::Counter(n) => *n,
            _ => panic!("expected Counter version"),
        })
        .collect();
    for window in versions.windows(2) {
        assert!(
            window[0] < window[1],
            "versions must be strictly increasing"
        );
    }
}

// ===========================================================================
// Cross-cutting: subscriber can read structure during emission (P1 regression)
// ===========================================================================

#[test]
fn subscriber_can_read_during_emission() {
    use std::sync::{Arc, Mutex};

    let rt = StructuresRuntime::new();
    let log: ReactiveLog<i64> =
        ReactiveLog::new(rt.core(), rt.intern_vec_fn(), ReactiveLogOptions::default());

    // Track DATA deliveries from within a subscriber sink.
    let data_count: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
    let count_clone = data_count.clone();

    let binding = rt.binding.clone();
    let sink: graphrefly_core::Sink = Arc::new(move |msgs: &[graphrefly_core::Message]| {
        for msg in msgs {
            if let graphrefly_core::Message::Data(h) = msg {
                // Dereference the handle — proves Core delivered the message
                // without the inner lock being held (P1 invariant).
                let _ = binding.deref(*h);
                *count_clone.lock().unwrap() += 1;
            }
        }
    });
    // Tracked so OwnedCore's owner-thread Drop tears it down (D246 —
    // no core RAII).
    let _sub = rt.track_subscribe(log.node_id, sink);

    log.append(rt.core(), 42);
    log.append(rt.core(), 43);

    assert_eq!(
        *data_count.lock().unwrap(),
        2,
        "subscriber received both DATA messages"
    );
}

// ===========================================================================
// M5.B: ReactiveLog views (A)
// ===========================================================================

#[test]
fn log_view_tail() {
    let rt = StructuresRuntime::new();
    let log = ReactiveLog::new(rt.core(), rt.intern_vec_fn(), ReactiveLogOptions::default());
    let mut view = log.view(rt.core(), ViewSpec::Tail { n: 2 }, rt.intern_vec_fn());
    let rec = rt.subscribe_recorder(view.node_id);

    log.append(rt.core(), 1);
    log.append(rt.core(), 2);
    log.append(rt.core(), 3);
    // `view` emits via an in-wave `SinkEmitter` (D246 r6 deferred
    // re-entry) — drain the mailbox so the recorder sees the snapshots.
    rt.drain_mailbox();

    // After 3 appends, tail(2) should show [2, 3].
    let vals = rec.data_values();
    let last = vals.last().unwrap();
    let nums: Vec<i64> = last
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_i64().unwrap())
        .collect();
    assert_eq!(nums, vec![2, 3]);
    view.detach(rt.core());
}

#[test]
fn log_view_slice() {
    let rt = StructuresRuntime::new();
    let log = ReactiveLog::new(rt.core(), rt.intern_vec_fn(), ReactiveLogOptions::default());
    let mut view = log.view(
        rt.core(),
        ViewSpec::Slice {
            start: 1,
            stop: Some(3),
        },
        rt.intern_vec_fn(),
    );
    let rec = rt.subscribe_recorder(view.node_id);

    log.append_many(rt.core(), vec![10, 20, 30, 40]);
    rt.drain_mailbox();

    let vals = rec.data_values();
    let last = vals.last().unwrap();
    let nums: Vec<i64> = last
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_i64().unwrap())
        .collect();
    assert_eq!(nums, vec![20, 30]);
    view.detach(rt.core());
}

#[test]
fn log_view_from_cursor() {
    let rt = StructuresRuntime::new();
    let log = ReactiveLog::new(rt.core(), rt.intern_vec_fn(), ReactiveLogOptions::default());

    // Create a cursor state node.
    let cursor_node = rt
        .core()
        .register_state(graphrefly_core::HandleId::new(0), false)
        .unwrap();

    let binding = rt.binding.clone();
    let read_cursor: std::sync::Arc<dyn Fn(graphrefly_core::HandleId) -> usize + Send + Sync> =
        std::sync::Arc::new(move |h| {
            let v = binding.deref(h);
            v.as_u64().unwrap() as usize
        });

    let mut view = log.view(
        rt.core(),
        ViewSpec::FromCursor {
            cursor_node,
            read_cursor,
        },
        rt.intern_vec_fn(),
    );
    let rec = rt.subscribe_recorder(view.node_id);

    log.append_many(rt.core(), vec![10, 20, 30, 40, 50]);

    // Move cursor to position 2.
    let h = rt.binding.intern(serde_json::json!(2));
    rt.core().emit(cursor_node, h);
    // `view` emits via an in-wave `SinkEmitter` (D246 r6 deferred
    // re-entry) — drain the mailbox so the recorder sees the snapshots.
    rt.drain_mailbox();

    let vals = rec.data_values();
    let last = vals.last().unwrap();
    let nums: Vec<i64> = last
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_i64().unwrap())
        .collect();
    assert_eq!(nums, vec![30, 40, 50]);
    view.detach(rt.core());
}

// ===========================================================================
// M5.B: ReactiveLog scan (A)
// ===========================================================================

#[test]
fn log_scan_incremental() {
    let rt = StructuresRuntime::new();
    let log = ReactiveLog::new(rt.core(), rt.intern_vec_fn(), ReactiveLogOptions::default());

    let intern_sum: graphrefly_structures::InternFn<i64> = {
        let b = rt.binding.clone();
        std::sync::Arc::new(move |sum: i64| b.intern(serde_json::json!(sum)))
    };

    let mut scan = log.scan(
        rt.core(),
        0i64,
        std::sync::Arc::new(|acc: &i64, item: &i64| acc + item),
        intern_sum,
    );
    let rec = rt.subscribe_recorder(scan.node_id);

    log.append(rt.core(), 10);
    log.append(rt.core(), 20);
    log.append(rt.core(), 30);
    // `scan` emits via an in-wave `SinkEmitter` (D246 r6 deferred
    // re-entry) — drain the mailbox so the recorder sees the snapshots.
    rt.drain_mailbox();

    let vals = rec.data_values();
    let sums: Vec<i64> = vals.iter().map(|v| v.as_i64().unwrap()).collect();
    assert_eq!(sums, vec![10, 30, 60]);
    scan.detach(rt.core());
}

#[test]
fn log_scan_rescan_on_clear() {
    let rt = StructuresRuntime::new();
    let log = ReactiveLog::new(rt.core(), rt.intern_vec_fn(), ReactiveLogOptions::default());

    let intern_sum: graphrefly_structures::InternFn<i64> = {
        let b = rt.binding.clone();
        std::sync::Arc::new(move |sum: i64| b.intern(serde_json::json!(sum)))
    };

    let mut scan = log.scan(
        rt.core(),
        0i64,
        std::sync::Arc::new(|acc: &i64, item: &i64| acc + item),
        intern_sum,
    );
    let rec = rt.subscribe_recorder(scan.node_id);

    log.append_many(rt.core(), vec![10, 20]);
    log.clear(rt.core());
    log.append(rt.core(), 5);
    // `scan` emits via an in-wave `SinkEmitter` (D246 r6 deferred
    // re-entry) — drain the mailbox so the recorder sees the snapshots.
    rt.drain_mailbox();

    let vals = rec.data_values();
    let sums: Vec<i64> = vals.iter().map(|v| v.as_i64().unwrap()).collect();
    // After append_many(10, 20): sum = 30
    // After clear: sum = 0 (rescan empty)
    // After append(5): sum = 5
    assert_eq!(sums, vec![30, 0, 5]);
    scan.detach(rt.core());
}

// ===========================================================================
// M5.B: ReactiveLog attach (A)
// ===========================================================================

#[test]
fn log_attach_upstream() {
    let rt = StructuresRuntime::new();

    // Create upstream BEFORE the log so it has a lower SubgraphId —
    // Core's ascending-order invariant requires that subscribers emit
    // to nodes with higher SubgraphIds than the source.
    let upstream = rt
        .core()
        .register_state(graphrefly_core::HandleId::new(0), false)
        .unwrap();

    let log = ReactiveLog::new(rt.core(), rt.intern_vec_fn(), ReactiveLogOptions::default());

    let binding = rt.binding.clone();
    let read_value: std::sync::Arc<dyn Fn(graphrefly_core::HandleId) -> i64 + Send + Sync> =
        std::sync::Arc::new(move |h| binding.deref(h).as_i64().unwrap());

    let mut sub = log.attach(rt.core(), upstream, read_value);

    // Emit values to upstream — they should appear in the log.
    let h1 = rt.binding.intern(serde_json::json!(100));
    rt.core().emit(upstream, h1);
    let h2 = rt.binding.intern(serde_json::json!(200));
    rt.core().emit(upstream, h2);

    assert_eq!(log.to_vec(), vec![100, 200]);
    sub.detach(rt.core());
}

// D270 — `AttachOptions { skip_cached_replay }` (memo:Re P2 parity).

#[test]
fn log_attach_with_skip_cached_replay_drops_handshake_data_when_upstream_cached() {
    let rt = StructuresRuntime::new();

    // Pre-seed the upstream with a cached value.
    let upstream = rt
        .core()
        .register_state(rt.binding.intern(serde_json::json!(7)), false)
        .unwrap();

    let log = ReactiveLog::new(rt.core(), rt.intern_vec_fn(), ReactiveLogOptions::default());

    let binding = rt.binding.clone();
    let read_value: std::sync::Arc<dyn Fn(graphrefly_core::HandleId) -> i64 + Send + Sync> =
        std::sync::Arc::new(move |h| binding.deref(h).as_i64().unwrap());

    // D270: skip_cached_replay = true. The push-on-subscribe DATA(7)
    // batch from the cached upstream is dropped; only subsequent live
    // emissions land.
    let mut sub = log.attach_with_options(
        rt.core(),
        upstream,
        read_value,
        graphrefly_structures::AttachOptions {
            skip_cached_replay: true,
        },
    );

    // After attach, the log should be EMPTY — replay was dropped.
    assert_eq!(log.to_vec(), Vec::<i64>::new());

    // Live emissions land normally.
    let h = rt.binding.intern(serde_json::json!(42));
    rt.core().emit(upstream, h);
    assert_eq!(log.to_vec(), vec![42]);
    sub.detach(rt.core());
}

#[test]
fn log_attach_with_skip_cached_replay_no_op_when_upstream_cold() {
    let rt = StructuresRuntime::new();

    // SENTINEL upstream — no cached value.
    let upstream = rt
        .core()
        .register_state(graphrefly_core::NO_HANDLE, false)
        .unwrap();

    let log = ReactiveLog::new(rt.core(), rt.intern_vec_fn(), ReactiveLogOptions::default());

    let binding = rt.binding.clone();
    let read_value: std::sync::Arc<dyn Fn(graphrefly_core::HandleId) -> i64 + Send + Sync> =
        std::sync::Arc::new(move |h| binding.deref(h).as_i64().unwrap());

    // D270: skip_cached_replay = true, but upstream is cold (sentinel),
    // so the flag is a no-op — the first live DATA emit lands.
    let mut sub = log.attach_with_options(
        rt.core(),
        upstream,
        read_value,
        graphrefly_structures::AttachOptions {
            skip_cached_replay: true,
        },
    );

    let h = rt.binding.intern(serde_json::json!(99));
    rt.core().emit(upstream, h);
    assert_eq!(
        log.to_vec(),
        vec![99],
        "cold upstream's first live emit must land"
    );
    sub.detach(rt.core());
}

#[test]
fn log_attach_with_skip_cached_replay_false_default_replays_cache() {
    let rt = StructuresRuntime::new();

    let upstream = rt
        .core()
        .register_state(rt.binding.intern(serde_json::json!(5)), false)
        .unwrap();

    let log = ReactiveLog::new(rt.core(), rt.intern_vec_fn(), ReactiveLogOptions::default());

    let binding = rt.binding.clone();
    let read_value: std::sync::Arc<dyn Fn(graphrefly_core::HandleId) -> i64 + Send + Sync> =
        std::sync::Arc::new(move |h| binding.deref(h).as_i64().unwrap());

    // Default `attach` (no options) preserves push-on-subscribe replay.
    let mut sub = log.attach(rt.core(), upstream, read_value);
    assert_eq!(log.to_vec(), vec![5], "default attach replays cached DATA");
    sub.detach(rt.core());
}

// ===========================================================================
// M5.B: ReactiveLog attachStorage (A)
// ===========================================================================

#[test]
fn log_attach_storage_preload_and_delta() {
    use std::sync::{Arc, Mutex};

    struct TestSink {
        stored: Mutex<Vec<i64>>,
    }
    impl AppendLogSink<i64> for TestSink {
        fn append_entries(
            &self,
            entries: &[i64],
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            self.stored.lock().unwrap().extend_from_slice(entries);
            Ok(())
        }
        fn load_entries(&self) -> Result<Vec<i64>, Box<dyn std::error::Error + Send + Sync>> {
            Ok(self.stored.lock().unwrap().clone())
        }
    }

    let rt = StructuresRuntime::new();
    let log = ReactiveLog::new(rt.core(), rt.intern_vec_fn(), ReactiveLogOptions::default());

    // Pre-seed the sink.
    let sink = Arc::new(TestSink {
        stored: Mutex::new(vec![1, 2]),
    });

    let mut handle = log
        .attach_storage(rt.core(), vec![sink.clone()], true)
        .expect("attach_storage with Append-mode sink succeeds");

    // Pre-loaded entries should be in the log.
    assert_eq!(log.to_vec(), vec![1, 2]);

    // New appends go to sink as deltas.
    log.append(rt.core(), 3);
    // Subscriber was registered AFTER preload, so it only sees post-preload
    // emissions. append(3) produces delta [3]. Sink: [1,2] + [3] = [1,2,3].
    assert_eq!(*sink.stored.lock().unwrap(), vec![1, 2, 3]);
    handle.detach(rt.core());
}

// ===========================================================================
// C/D (next batch, 2026-05-21): AppendLogSink::mode + flush + flush_all
// ===========================================================================

/// **C — D269 part-2 parity.** `attach_storage` must reject any sink
/// declaring `AppendLogMode::Overwrite`. Delta-shipping into an
/// overwrite sink would silently truncate the log to the last batch
/// (memo:Re P1 hazard class).
#[test]
fn log_attach_storage_rejects_overwrite_sink() {
    use std::sync::{Arc, Mutex};

    struct OverwriteSink {
        stored: Mutex<Vec<i64>>,
    }
    impl AppendLogSink<i64> for OverwriteSink {
        fn append_entries(
            &self,
            entries: &[i64],
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            self.stored.lock().unwrap().extend_from_slice(entries);
            Ok(())
        }
        fn load_entries(&self) -> Result<Vec<i64>, Box<dyn std::error::Error + Send + Sync>> {
            Ok(Vec::new())
        }
        fn mode(&self) -> AppendLogMode {
            AppendLogMode::Overwrite
        }
    }

    let rt = StructuresRuntime::new();
    let log = ReactiveLog::new(rt.core(), rt.intern_vec_fn(), ReactiveLogOptions::default());

    let append_sink: Arc<dyn AppendLogSink<i64>> = Arc::new(OverwriteSink {
        stored: Mutex::new(Vec::new()),
    });
    let result = log.attach_storage(rt.core(), vec![append_sink], false);

    match result {
        Err(AttachStorageError::OverwriteSinkRejected { sink_index: 0 }) => {}
        Err(other) => panic!("expected sink_index: 0, got error: {other}"),
        Ok(_) => panic!("expected Overwrite sink to be rejected, got Ok"),
    }
}

/// **C** — multi-sink rejection identifies the FIRST offending sink.
#[test]
fn log_attach_storage_rejects_first_overwrite_in_mixed_sinks() {
    use std::sync::{Arc, Mutex};

    struct AppendOk(Mutex<Vec<i64>>);
    impl AppendLogSink<i64> for AppendOk {
        fn append_entries(
            &self,
            e: &[i64],
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            self.0.lock().unwrap().extend_from_slice(e);
            Ok(())
        }
        fn load_entries(&self) -> Result<Vec<i64>, Box<dyn std::error::Error + Send + Sync>> {
            Ok(Vec::new())
        }
    }
    struct OverwriteBad;
    impl AppendLogSink<i64> for OverwriteBad {
        fn append_entries(
            &self,
            _: &[i64],
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
        }
        fn load_entries(&self) -> Result<Vec<i64>, Box<dyn std::error::Error + Send + Sync>> {
            Ok(Vec::new())
        }
        fn mode(&self) -> AppendLogMode {
            AppendLogMode::Overwrite
        }
    }

    let rt = StructuresRuntime::new();
    let log = ReactiveLog::new(rt.core(), rt.intern_vec_fn(), ReactiveLogOptions::default());

    let s0: Arc<dyn AppendLogSink<i64>> = Arc::new(AppendOk(Mutex::new(Vec::new())));
    let s1: Arc<dyn AppendLogSink<i64>> = Arc::new(OverwriteBad);
    let s2: Arc<dyn AppendLogSink<i64>> = Arc::new(AppendOk(Mutex::new(Vec::new())));

    match log.attach_storage(rt.core(), vec![s0, s1, s2], false) {
        Err(AttachStorageError::OverwriteSinkRejected { sink_index: 1 }) => {}
        Err(other) => panic!("expected sink_index: 1, got error: {other}"),
        Ok(_) => panic!("expected Overwrite sink at index 1 to be rejected, got Ok"),
    }
}

/// **D — F9 parity (Option B per D171 precedent).** `flush_all` drives
/// every sink's `flush()` synchronously. Callers wire this from a
/// reactive timer subgraph (`graphrefly_operators::temporal::interval`);
/// the handle owns no internal timer.
#[test]
fn log_attach_storage_flush_all_drives_each_sink_flush() {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};

    struct BufferingSink {
        buffer: Mutex<Vec<i64>>,
        committed: Mutex<Vec<i64>>,
        flush_calls: AtomicU32,
    }
    impl AppendLogSink<i64> for BufferingSink {
        fn append_entries(
            &self,
            entries: &[i64],
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            self.buffer.lock().unwrap().extend_from_slice(entries);
            Ok(())
        }
        fn load_entries(&self) -> Result<Vec<i64>, Box<dyn std::error::Error + Send + Sync>> {
            Ok(self.committed.lock().unwrap().clone())
        }
        fn flush(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            self.flush_calls.fetch_add(1, Ordering::Relaxed);
            let mut buf = self.buffer.lock().unwrap();
            self.committed.lock().unwrap().append(&mut buf);
            Ok(())
        }
    }

    let rt = StructuresRuntime::new();
    let log = ReactiveLog::new(rt.core(), rt.intern_vec_fn(), ReactiveLogOptions::default());

    let sink_a = Arc::new(BufferingSink {
        buffer: Mutex::new(Vec::new()),
        committed: Mutex::new(Vec::new()),
        flush_calls: AtomicU32::new(0),
    });
    let sink_b = Arc::new(BufferingSink {
        buffer: Mutex::new(Vec::new()),
        committed: Mutex::new(Vec::new()),
        flush_calls: AtomicU32::new(0),
    });

    let sinks: Vec<Arc<dyn AppendLogSink<i64>>> = vec![sink_a.clone() as _, sink_b.clone() as _];
    let mut handle = log
        .attach_storage(rt.core(), sinks, false)
        .expect("Append-mode sinks accepted");

    log.append(rt.core(), 1);
    log.append(rt.core(), 2);

    // Pre-flush_all: entries are in the sinks' buffers, NOT committed.
    assert_eq!(*sink_a.buffer.lock().unwrap(), vec![1, 2]);
    assert!(sink_a.committed.lock().unwrap().is_empty());
    assert_eq!(sink_a.flush_calls.load(Ordering::Relaxed), 0);
    assert_eq!(sink_b.flush_calls.load(Ordering::Relaxed), 0);

    handle
        .flush_all()
        .expect("flush_all succeeds with no errors");

    // Post-flush_all: every sink saw exactly one flush; entries moved
    // from buffer to committed.
    assert_eq!(sink_a.flush_calls.load(Ordering::Relaxed), 1);
    assert_eq!(sink_b.flush_calls.load(Ordering::Relaxed), 1);
    assert!(sink_a.buffer.lock().unwrap().is_empty());
    assert!(sink_b.buffer.lock().unwrap().is_empty());
    assert_eq!(*sink_a.committed.lock().unwrap(), vec![1, 2]);
    assert_eq!(*sink_b.committed.lock().unwrap(), vec![1, 2]);

    handle.detach(rt.core());
}

/// **D** — `flush_all` surfaces a per-sink error with an index prefix
/// for diagnostics; subsequent sinks are NOT attempted after the first
/// failure (matches `graphrefly_storage::StorageHandle::flush_all`).
#[test]
fn log_attach_storage_flush_all_surfaces_first_error_with_index() {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    struct OkSink(AtomicU32);
    impl AppendLogSink<i64> for OkSink {
        fn append_entries(
            &self,
            _: &[i64],
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
        }
        fn load_entries(&self) -> Result<Vec<i64>, Box<dyn std::error::Error + Send + Sync>> {
            Ok(Vec::new())
        }
        fn flush(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }
    struct FailSink;
    impl AppendLogSink<i64> for FailSink {
        fn append_entries(
            &self,
            _: &[i64],
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
        }
        fn load_entries(&self) -> Result<Vec<i64>, Box<dyn std::error::Error + Send + Sync>> {
            Ok(Vec::new())
        }
        fn flush(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Err("injected flush fault".into())
        }
    }

    let rt = StructuresRuntime::new();
    let log = ReactiveLog::new(rt.core(), rt.intern_vec_fn(), ReactiveLogOptions::default());

    let s0 = Arc::new(OkSink(AtomicU32::new(0)));
    let s1 = Arc::new(FailSink);
    let s2 = Arc::new(OkSink(AtomicU32::new(0)));
    let sinks: Vec<Arc<dyn AppendLogSink<i64>>> =
        vec![s0.clone() as _, s1.clone() as _, s2.clone() as _];

    let handle = log
        .attach_storage(rt.core(), sinks, false)
        .expect("Append-mode sinks accepted");

    let err = handle
        .flush_all()
        .expect_err("flush_all should fail at sink[1]");
    let msg = err.to_string();
    assert!(
        msg.starts_with("sink[1] flush:"),
        "error should prefix the failing sink index, got: {msg}",
    );
    assert!(msg.contains("injected flush fault"));

    // s0 was flushed before the failure; s2 was NOT (short-circuit semantic).
    assert_eq!(s0.0.load(Ordering::Relaxed), 1);
    assert_eq!(s2.0.load(Ordering::Relaxed), 0);
}

// ===========================================================================
// M5.B: ReactiveMap TTL (B)
// ===========================================================================

#[test]
fn map_ttl_expired_key_returns_none() {
    let rt = StructuresRuntime::new();
    let map: ReactiveMap<String, i64> = ReactiveMap::new(
        rt.core(),
        rt.intern_pairs_fn(),
        ReactiveMapOptions {
            default_ttl: Some(0.000_000_001), // 1 nanosecond — expires immediately.
            ..Default::default()
        },
    )
    .unwrap();

    map.set(rt.core(), "a".into(), 42);
    // After 1ns, the key should be expired.
    std::thread::sleep(std::time::Duration::from_millis(1));
    assert_eq!(map.get(rt.core(), &"a".into()), None);
}

#[test]
fn map_ttl_mutation_log_records_expired() {
    let rt = StructuresRuntime::new();
    let map: ReactiveMap<String, i64> = ReactiveMap::new(
        rt.core(),
        rt.intern_pairs_fn(),
        ReactiveMapOptions {
            default_ttl: Some(0.000_000_001),
            mutation_log: true,
            ..Default::default()
        },
    )
    .unwrap();

    map.set(rt.core(), "x".into(), 1);
    std::thread::sleep(std::time::Duration::from_millis(1));
    map.prune_expired(rt.core());

    let entries = map.mutation_log_snapshot().unwrap();
    let expired: Vec<_> = entries
        .iter()
        .filter(|e| {
            matches!(
                &e.change,
                MapChange::Delete {
                    reason: DeleteReason::Expired,
                    ..
                }
            )
        })
        .collect();
    assert!(!expired.is_empty(), "should log expired deletion");
}

// ===========================================================================
// M5.B: ReactiveMap LRU (B)
// ===========================================================================

#[test]
fn map_lru_evicts_oldest() {
    let rt = StructuresRuntime::new();
    let map: ReactiveMap<String, i64> = ReactiveMap::new(
        rt.core(),
        rt.intern_pairs_fn(),
        ReactiveMapOptions {
            max_size: Some(2),
            ..Default::default()
        },
    )
    .unwrap();

    map.set(rt.core(), "a".into(), 1);
    map.set(rt.core(), "b".into(), 2);
    map.set(rt.core(), "c".into(), 3); // Should evict "a".

    assert_eq!(map.size(), 2);
    assert_eq!(map.get(rt.core(), &"a".into()), None);
    assert_eq!(map.get(rt.core(), &"b".into()), Some(2));
    assert_eq!(map.get(rt.core(), &"c".into()), Some(3));
}

#[test]
fn map_lru_touch_on_get_prevents_eviction() {
    let rt = StructuresRuntime::new();
    let map: ReactiveMap<String, i64> = ReactiveMap::new(
        rt.core(),
        rt.intern_pairs_fn(),
        ReactiveMapOptions {
            max_size: Some(2),
            ..Default::default()
        },
    )
    .unwrap();

    map.set(rt.core(), "a".into(), 1);
    map.set(rt.core(), "b".into(), 2);
    // Touch "a" so it becomes most-recently-used.
    let _ = map.get(rt.core(), &"a".into());
    // Insert "c" — should evict "b" (now LRU), not "a".
    map.set(rt.core(), "c".into(), 3);

    assert_eq!(map.size(), 2);
    assert_eq!(map.get(rt.core(), &"a".into()), Some(1));
    assert_eq!(map.get(rt.core(), &"b".into()), None);
    assert_eq!(map.get(rt.core(), &"c".into()), Some(3));
}

#[test]
fn map_lru_mutation_log_records_eviction() {
    let rt = StructuresRuntime::new();
    let map: ReactiveMap<String, i64> = ReactiveMap::new(
        rt.core(),
        rt.intern_pairs_fn(),
        ReactiveMapOptions {
            max_size: Some(1),
            mutation_log: true,
            ..Default::default()
        },
    )
    .unwrap();

    map.set(rt.core(), "a".into(), 1);
    map.set(rt.core(), "b".into(), 2); // Evicts "a".

    let entries = map.mutation_log_snapshot().unwrap();
    let evictions: Vec<_> = entries
        .iter()
        .filter(|e| {
            matches!(
                &e.change,
                MapChange::Delete {
                    reason: DeleteReason::LruEvict,
                    ..
                }
            )
        })
        .collect();
    assert_eq!(evictions.len(), 1, "should log LRU eviction of 'a'");
}

// ===========================================================================
// M5.B: ReactiveMap retention (B)
// ===========================================================================

#[test]
fn map_retention_archives_below_threshold() {
    let rt = StructuresRuntime::new();
    let map: ReactiveMap<String, i64> = ReactiveMap::new(
        rt.core(),
        rt.intern_pairs_fn(),
        ReactiveMapOptions {
            retention: Some(RetentionPolicy {
                score: std::sync::Arc::new(|_k, v| *v as f64),
                archive_threshold: Some(10.0),
                max_size: None,
                on_archive: None,
            }),
            ..Default::default()
        },
    )
    .unwrap();

    map.set_many(rt.core(), vec![("low".into(), 5), ("high".into(), 20)]);

    assert_eq!(map.size(), 1);
    assert_eq!(map.get(rt.core(), &"low".into()), None);
    assert_eq!(map.get(rt.core(), &"high".into()), Some(20));
}

#[test]
fn map_retention_max_size_keeps_highest_scored() {
    let rt = StructuresRuntime::new();
    let map: ReactiveMap<String, i64> = ReactiveMap::new(
        rt.core(),
        rt.intern_pairs_fn(),
        ReactiveMapOptions {
            retention: Some(RetentionPolicy {
                score: std::sync::Arc::new(|_k, v| *v as f64),
                archive_threshold: None,
                max_size: Some(2),
                on_archive: None,
            }),
            ..Default::default()
        },
    )
    .unwrap();

    map.set_many(
        rt.core(),
        vec![("c".into(), 30), ("a".into(), 10), ("b".into(), 20)],
    );

    assert_eq!(map.size(), 2);
    // Lowest scored (a=10) should be archived.
    assert_eq!(map.get(rt.core(), &"a".into()), None);
}

#[test]
fn map_lru_and_retention_mutually_exclusive() {
    let rt = StructuresRuntime::new();
    let result = ReactiveMap::<String, i64>::new(
        rt.core(),
        rt.intern_pairs_fn(),
        ReactiveMapOptions {
            max_size: Some(2),
            retention: Some(RetentionPolicy {
                score: std::sync::Arc::new(|_k, v| *v as f64),
                archive_threshold: None,
                max_size: None,
                on_archive: None,
            }),
            ..Default::default()
        },
    );
    assert!(result.is_err());
}

// ===========================================================================
// M5.B: ReactiveIndex custom equals (C)
// ===========================================================================

#[test]
fn index_custom_equals_skips_identical_upsert() {
    let rt = StructuresRuntime::new();
    let eq: IndexEqualsFn<String, i64> =
        std::sync::Arc::new(|a, b| a.secondary == b.secondary && a.value == b.value);
    let index = ReactiveIndex::new(
        rt.core(),
        rt.intern_index_fn(),
        ReactiveIndexOptions {
            equals: Some(eq),
            ..Default::default()
        },
    );
    let rec = rt.subscribe_recorder(index.node_id);

    index.upsert(rt.core(), "a".into(), "1".into(), 10);
    // Same values — should be idempotent (no emission).
    let was_new = index.upsert(rt.core(), "a".into(), "1".into(), 10);
    assert!(!was_new);

    // Different value — should emit.
    index.upsert(rt.core(), "a".into(), "2".into(), 99);

    assert_eq!(rec.data_count(), 2); // First upsert + third upsert, NOT the idempotent one.
}

#[test]
fn index_per_call_equals_override() {
    let rt = StructuresRuntime::new();
    let index = ReactiveIndex::new(
        rt.core(),
        rt.intern_index_fn(),
        ReactiveIndexOptions::default(),
    );
    let rec = rt.subscribe_recorder(index.node_id);

    index.upsert(rt.core(), "a".into(), "1".into(), 10);
    // Per-call equals that always considers rows identical.
    let always_equal: IndexEqualsFn<String, i64> = std::sync::Arc::new(|_a, _b| true);
    let was_new = index.upsert_with(
        rt.core(),
        "a".into(),
        "2".into(),
        99,
        &UpsertOptions {
            equals: Some(always_equal),
        },
    );
    assert!(!was_new);
    assert_eq!(rec.data_count(), 1); // Only the first upsert emitted.
}

#[test]
fn index_upsert_many_respects_factory_equals() {
    let rt = StructuresRuntime::new();
    let eq: IndexEqualsFn<String, i64> =
        std::sync::Arc::new(|a, b| a.secondary == b.secondary && a.value == b.value);
    let index = ReactiveIndex::new(
        rt.core(),
        rt.intern_index_fn(),
        ReactiveIndexOptions {
            equals: Some(eq),
            ..Default::default()
        },
    );
    let rec = rt.subscribe_recorder(index.node_id);

    index.upsert(rt.core(), "a".into(), "1".into(), 10);
    // Batch: "a" is identical (skip), "b" is new (accept).
    index.upsert_many(
        rt.core(),
        vec![
            ("a".into(), "1".into(), 10), // skip
            ("b".into(), "2".into(), 20), // new
        ],
    );

    assert_eq!(index.size(), 2);
    assert_eq!(rec.data_count(), 2); // First upsert + batch (with only "b").
}

// ===========================================================================
// M5.B: DeleteReason on mutation log
// ===========================================================================

#[test]
fn map_delete_reason_explicit() {
    let rt = StructuresRuntime::new();
    let map: ReactiveMap<String, i64> = ReactiveMap::new(
        rt.core(),
        rt.intern_pairs_fn(),
        ReactiveMapOptions {
            mutation_log: true,
            ..Default::default()
        },
    )
    .unwrap();

    map.set(rt.core(), "x".into(), 1);
    map.delete(rt.core(), &"x".into());

    let entries = map.mutation_log_snapshot().unwrap();
    assert!(matches!(
        &entries[1].change,
        MapChange::Delete {
            reason: DeleteReason::Explicit,
            ..
        }
    ));
}
