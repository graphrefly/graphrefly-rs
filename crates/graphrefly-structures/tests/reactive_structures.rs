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
    IndexChange, ListChange, LogChange, MapChange, ReactiveIndex, ReactiveIndexOptions,
    ReactiveList, ReactiveListOptions, ReactiveLog, ReactiveLogOptions, ReactiveMap,
    ReactiveMapOptions,
};

// ===========================================================================
// ReactiveLog tests
// ===========================================================================

#[test]
fn log_append_emits_data() {
    let rt = StructuresRuntime::new();
    let log: ReactiveLog<i64> =
        ReactiveLog::new(&rt.core, rt.intern_vec_fn(), ReactiveLogOptions::default());
    let rec = rt.subscribe_recorder(log.node_id);

    log.append(1);
    log.append(2);

    assert_eq!(log.size(), 2);
    assert_eq!(log.to_vec(), vec![1, 2]);
    assert_eq!(rec.data_count(), 2);
}

#[test]
fn log_append_many() {
    let rt = StructuresRuntime::new();
    let log: ReactiveLog<i64> =
        ReactiveLog::new(&rt.core, rt.intern_vec_fn(), ReactiveLogOptions::default());
    let rec = rt.subscribe_recorder(log.node_id);

    log.append_many(vec![1, 2, 3]);

    assert_eq!(log.size(), 3);
    assert_eq!(log.to_vec(), vec![1, 2, 3]);
    assert_eq!(rec.data_count(), 1);
}

#[test]
fn log_ring_buffer_overflow() {
    let rt = StructuresRuntime::new();
    let log: ReactiveLog<i64> = ReactiveLog::new(
        &rt.core,
        rt.intern_vec_fn(),
        ReactiveLogOptions {
            max_size: Some(3),
            ..Default::default()
        },
    );

    log.append_many(vec![1, 2, 3, 4, 5]);

    assert_eq!(log.size(), 3);
    assert_eq!(log.to_vec(), vec![3, 4, 5]);
}

#[test]
fn log_clear_and_trim_head() {
    let rt = StructuresRuntime::new();
    let log: ReactiveLog<i64> =
        ReactiveLog::new(&rt.core, rt.intern_vec_fn(), ReactiveLogOptions::default());

    log.append_many(vec![1, 2, 3, 4, 5]);
    log.trim_head(2);
    assert_eq!(log.to_vec(), vec![3, 4, 5]);

    log.clear();
    assert_eq!(log.size(), 0);
    assert_eq!(log.to_vec(), Vec::<i64>::new());
}

#[test]
fn log_negative_index() {
    let rt = StructuresRuntime::new();
    let log: ReactiveLog<i64> =
        ReactiveLog::new(&rt.core, rt.intern_vec_fn(), ReactiveLogOptions::default());

    log.append_many(vec![10, 20, 30]);

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
        &rt.core,
        rt.intern_vec_fn(),
        ReactiveLogOptions {
            mutation_log: true,
            ..Default::default()
        },
    );

    log.append(1);
    log.append_many(vec![2, 3]);
    log.trim_head(1);
    log.clear();

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
        ReactiveLog::new(&rt.core, rt.intern_vec_fn(), ReactiveLogOptions::default());
    let rec = rt.subscribe_recorder(log.node_id);

    log.append_many(vec![]);
    log.clear();
    log.trim_head(5);

    assert_eq!(rec.data_count(), 0);
}

// ===========================================================================
// ReactiveList tests
// ===========================================================================

#[test]
fn list_append_and_insert() {
    let rt = StructuresRuntime::new();
    let list: ReactiveList<String> =
        ReactiveList::new(&rt.core, rt.intern_vec_fn(), ReactiveListOptions::default());
    let rec = rt.subscribe_recorder(list.node_id);

    list.append("a".into());
    list.append("c".into());
    list.insert(1, "b".into()).unwrap();

    assert_eq!(list.to_vec(), vec!["a", "b", "c"]);
    assert_eq!(rec.data_count(), 3);
}

#[test]
fn list_pop() {
    let rt = StructuresRuntime::new();
    let list: ReactiveList<i64> =
        ReactiveList::new(&rt.core, rt.intern_vec_fn(), ReactiveListOptions::default());

    list.append_many(vec![10, 20, 30]);

    assert_eq!(list.pop(-1), Some(30));
    assert_eq!(list.to_vec(), vec![10, 20]);

    assert_eq!(list.pop(0), Some(10));
    assert_eq!(list.to_vec(), vec![20]);
}

#[test]
fn list_pop_oob_returns_none() {
    let rt = StructuresRuntime::new();
    let list: ReactiveList<i64> =
        ReactiveList::new(&rt.core, rt.intern_vec_fn(), ReactiveListOptions::default());
    let rec = rt.subscribe_recorder(list.node_id);

    assert_eq!(list.pop(0), None);
    list.append(1);
    assert_eq!(list.pop(5), None);
    assert_eq!(list.pop(-2), None);

    assert_eq!(rec.data_count(), 1); // only the append emitted
}

#[test]
fn list_insert_oob_returns_err() {
    let rt = StructuresRuntime::new();
    let list: ReactiveList<i64> =
        ReactiveList::new(&rt.core, rt.intern_vec_fn(), ReactiveListOptions::default());
    let rec = rt.subscribe_recorder(list.node_id);

    assert!(list.insert(1, 42).is_err());
    assert!(list.insert_many(1, vec![1, 2]).is_err());
    assert_eq!(list.size(), 0);
    assert_eq!(rec.data_count(), 0);
}

#[test]
fn list_insert_many() {
    let rt = StructuresRuntime::new();
    let list: ReactiveList<i64> =
        ReactiveList::new(&rt.core, rt.intern_vec_fn(), ReactiveListOptions::default());

    list.append_many(vec![1, 4]);
    list.insert_many(1, vec![2, 3]).unwrap();

    assert_eq!(list.to_vec(), vec![1, 2, 3, 4]);
}

#[test]
fn list_clear() {
    let rt = StructuresRuntime::new();
    let list: ReactiveList<i64> =
        ReactiveList::new(&rt.core, rt.intern_vec_fn(), ReactiveListOptions::default());
    let rec = rt.subscribe_recorder(list.node_id);

    list.append_many(vec![1, 2, 3]);
    list.clear();

    assert_eq!(list.size(), 0);
    assert_eq!(rec.data_count(), 2);
}

#[test]
fn list_negative_index() {
    let rt = StructuresRuntime::new();
    let list: ReactiveList<i64> =
        ReactiveList::new(&rt.core, rt.intern_vec_fn(), ReactiveListOptions::default());

    list.append_many(vec![10, 20, 30]);

    assert_eq!(list.at(-1), Some(30));
    assert_eq!(list.at(-3), Some(10));
    assert_eq!(list.at(-4), None);
}

#[test]
fn list_mutation_log() {
    let rt = StructuresRuntime::new();
    let list: ReactiveList<i64> = ReactiveList::new(
        &rt.core,
        rt.intern_vec_fn(),
        ReactiveListOptions {
            mutation_log: true,
            ..Default::default()
        },
    );

    list.append(1);
    list.insert(0, 0).unwrap();
    let _ = list.pop(-1);
    list.clear();

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
        &rt.core,
        rt.intern_pairs_fn(),
        ReactiveMapOptions::default(),
    );
    let rec = rt.subscribe_recorder(map.node_id);

    map.set("a".into(), 1);
    map.set("b".into(), 2);

    assert_eq!(map.size(), 2);
    assert_eq!(map.get(&"a".into()), Some(1));
    assert_eq!(map.get(&"b".into()), Some(2));
    assert_eq!(map.get(&"c".into()), None);
    assert!(map.has(&"a".into()));
    assert!(!map.has(&"c".into()));
    assert_eq!(rec.data_count(), 2);
}

#[test]
fn map_set_many() {
    let rt = StructuresRuntime::new();
    let map: ReactiveMap<String, i64> = ReactiveMap::new(
        &rt.core,
        rt.intern_pairs_fn(),
        ReactiveMapOptions::default(),
    );
    let rec = rt.subscribe_recorder(map.node_id);

    map.set_many(vec![("a".into(), 1), ("b".into(), 2), ("c".into(), 3)]);

    assert_eq!(map.size(), 3);
    assert_eq!(rec.data_count(), 1);
}

#[test]
fn map_delete() {
    let rt = StructuresRuntime::new();
    let map: ReactiveMap<String, i64> = ReactiveMap::new(
        &rt.core,
        rt.intern_pairs_fn(),
        ReactiveMapOptions::default(),
    );

    map.set_many(vec![("a".into(), 1), ("b".into(), 2)]);
    map.delete(&"a".into());

    assert_eq!(map.size(), 1);
    assert_eq!(map.get(&"a".into()), None);
    assert_eq!(map.get(&"b".into()), Some(2));
}

#[test]
fn map_delete_nonexistent_no_emit() {
    let rt = StructuresRuntime::new();
    let map: ReactiveMap<String, i64> = ReactiveMap::new(
        &rt.core,
        rt.intern_pairs_fn(),
        ReactiveMapOptions::default(),
    );
    let rec = rt.subscribe_recorder(map.node_id);

    map.delete(&"x".into());

    assert_eq!(rec.data_count(), 0);
}

#[test]
fn map_clear() {
    let rt = StructuresRuntime::new();
    let map: ReactiveMap<String, i64> = ReactiveMap::new(
        &rt.core,
        rt.intern_pairs_fn(),
        ReactiveMapOptions::default(),
    );

    map.set_many(vec![("a".into(), 1), ("b".into(), 2)]);
    map.clear();

    assert_eq!(map.size(), 0);
}

#[test]
fn map_mutation_log() {
    let rt = StructuresRuntime::new();
    let map: ReactiveMap<String, i64> = ReactiveMap::new(
        &rt.core,
        rt.intern_pairs_fn(),
        ReactiveMapOptions {
            mutation_log: true,
            ..Default::default()
        },
    );

    map.set("x".into(), 42);
    map.delete(&"x".into());

    let entries = map.mutation_log_snapshot().expect("mutation log enabled");
    assert_eq!(entries.len(), 2);
    assert!(
        matches!(&entries[0].change, MapChange::Set { key, value } if key == "x" && *value == 42)
    );
    assert!(matches!(&entries[1].change, MapChange::Delete { key } if key == "x"));
}

#[test]
fn map_delete_many_only_logs_existing_keys() {
    let rt = StructuresRuntime::new();
    let map: ReactiveMap<String, i64> = ReactiveMap::new(
        &rt.core,
        rt.intern_pairs_fn(),
        ReactiveMapOptions {
            mutation_log: true,
            ..Default::default()
        },
    );

    map.set_many(vec![("a".into(), 1), ("b".into(), 2)]);
    map.delete_many(&["a".into(), "nonexistent".into(), "b".into()]);

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
        &rt.core,
        rt.intern_index_fn(),
        ReactiveIndexOptions::default(),
    );
    let rec = rt.subscribe_recorder(index.node_id);

    assert!(index.upsert("c".into(), "2".into(), 30));
    assert!(index.upsert("a".into(), "1".into(), 10));
    assert!(index.upsert("b".into(), "1".into(), 20));

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
        &rt.core,
        rt.intern_index_fn(),
        ReactiveIndexOptions::default(),
    );

    assert!(index.upsert("a".into(), "1".into(), 10));
    assert!(!index.upsert("a".into(), "2".into(), 99));

    assert_eq!(index.size(), 1);
    assert_eq!(index.get(&"a".into()), Some(99));
    let ordered = index.to_ordered();
    assert_eq!(ordered[0].secondary, "2");
}

#[test]
fn index_delete() {
    let rt = StructuresRuntime::new();
    let index: ReactiveIndex<String, i64> = ReactiveIndex::new(
        &rt.core,
        rt.intern_index_fn(),
        ReactiveIndexOptions::default(),
    );

    index.upsert("a".into(), "1".into(), 10);
    index.upsert("b".into(), "2".into(), 20);
    index.delete(&"a".into());

    assert_eq!(index.size(), 1);
    assert!(!index.has(&"a".into()));
    assert!(index.has(&"b".into()));
}

#[test]
fn index_delete_nonexistent_no_emit() {
    let rt = StructuresRuntime::new();
    let index: ReactiveIndex<String, i64> = ReactiveIndex::new(
        &rt.core,
        rt.intern_index_fn(),
        ReactiveIndexOptions::default(),
    );
    let rec = rt.subscribe_recorder(index.node_id);

    index.delete(&"x".into());

    assert_eq!(rec.data_count(), 0);
}

#[test]
fn index_clear() {
    let rt = StructuresRuntime::new();
    let index: ReactiveIndex<String, i64> = ReactiveIndex::new(
        &rt.core,
        rt.intern_index_fn(),
        ReactiveIndexOptions::default(),
    );

    index.upsert("a".into(), "1".into(), 10);
    index.upsert("b".into(), "2".into(), 20);
    index.clear();

    assert_eq!(index.size(), 0);
    assert_eq!(index.to_ordered().len(), 0);
}

#[test]
fn index_upsert_many() {
    let rt = StructuresRuntime::new();
    let index: ReactiveIndex<String, i64> = ReactiveIndex::new(
        &rt.core,
        rt.intern_index_fn(),
        ReactiveIndexOptions::default(),
    );
    let rec = rt.subscribe_recorder(index.node_id);

    index.upsert_many(vec![
        ("a".into(), "1".into(), 10),
        ("b".into(), "2".into(), 20),
    ]);

    assert_eq!(index.size(), 2);
    assert_eq!(rec.data_count(), 1);
}

#[test]
fn index_mutation_log() {
    let rt = StructuresRuntime::new();
    let index: ReactiveIndex<String, i64> = ReactiveIndex::new(
        &rt.core,
        rt.intern_index_fn(),
        ReactiveIndexOptions {
            mutation_log: true,
            ..Default::default()
        },
    );

    index.upsert("a".into(), "1".into(), 10);
    index.delete(&"a".into());

    let entries = index.mutation_log_snapshot().expect("mutation log enabled");
    assert_eq!(entries.len(), 2);
    assert!(matches!(&entries[0].change, IndexChange::Upsert { primary, .. } if primary == "a"));
    assert!(matches!(&entries[1].change, IndexChange::Delete { primary } if primary == "a"));
}

#[test]
fn index_delete_many() {
    let rt = StructuresRuntime::new();
    let index: ReactiveIndex<String, i64> = ReactiveIndex::new(
        &rt.core,
        rt.intern_index_fn(),
        ReactiveIndexOptions::default(),
    );

    index.upsert_many(vec![
        ("a".into(), "1".into(), 10),
        ("b".into(), "2".into(), 20),
        ("c".into(), "3".into(), 30),
    ]);
    index.delete_many(&["a".into(), "c".into()]);

    assert_eq!(index.size(), 1);
    assert!(index.has(&"b".into()));
}

#[test]
fn index_delete_many_only_logs_existing_keys() {
    let rt = StructuresRuntime::new();
    let index: ReactiveIndex<String, i64> = ReactiveIndex::new(
        &rt.core,
        rt.intern_index_fn(),
        ReactiveIndexOptions {
            mutation_log: true,
            ..Default::default()
        },
    );

    index.upsert_many(vec![
        ("a".into(), "1".into(), 10),
        ("b".into(), "2".into(), 20),
    ]);
    index.delete_many(&["a".into(), "nonexistent".into()]);

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
        &rt.core,
        rt.intern_vec_fn(),
        ReactiveLogOptions {
            mutation_log: true,
            ..Default::default()
        },
    );

    log.append(1);
    log.append(2);
    log.append(3);

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
        ReactiveLog::new(&rt.core, rt.intern_vec_fn(), ReactiveLogOptions::default());

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
    let _sub = rt.core.subscribe(log.node_id, sink);

    log.append(42);
    log.append(43);

    assert_eq!(
        *data_count.lock().unwrap(),
        2,
        "subscriber received both DATA messages"
    );
}
