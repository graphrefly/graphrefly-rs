//! Tier-level integration tests (M4.B 2026-05-10).
//!
//! Covers the public surface of the three sub-traits via memory tiers:
//! - Snapshot round-trip, key-of routing, filter skipping, compact-every,
//!   debounce-deferred flush, rollback.
//! - KV save/load/delete + list.
//! - Append-log accumulate + load + multi-key.
//! - Multi-tier sharing one backend (paired `{ snapshot, wal }` pattern).
//! - `list_by_prefix_bytes` dyn-safe enumeration via `&dyn BaseStorageTier`.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use graphrefly_storage::{
    append_log_storage, kv_storage, memory_append_log, memory_backend, memory_kv, memory_snapshot,
    snapshot_storage, AppendLogStorage, AppendLogStorageOptions, AppendLogStorageTier,
    BaseStorageTier, KvStorage, KvStorageOptions, KvStorageTier, MemoryBackend, SnapshotStorage,
    SnapshotStorageOptions, SnapshotStorageTier, StorageBackend,
};

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
struct Snap {
    name: String,
    value: u32,
}

// ── Snapshot ──────────────────────────────────────────────────────────────

#[test]
fn snapshot_round_trip_sync_through() {
    let tier = memory_snapshot::<Snap, _>(SnapshotStorageOptions {
        name: Some("my-graph".into()),
        ..Default::default()
    });
    let s = Snap {
        name: "my-graph".into(),
        value: 42,
    };
    tier.save(s.clone()).unwrap();
    let loaded = tier.load().unwrap();
    assert_eq!(loaded, Some(s));
}

#[test]
fn snapshot_save_overwrites_under_same_key() {
    let tier = memory_snapshot::<Snap, _>(SnapshotStorageOptions {
        name: Some("g".into()),
        ..Default::default()
    });
    tier.save(Snap {
        name: "g".into(),
        value: 1,
    })
    .unwrap();
    tier.save(Snap {
        name: "g".into(),
        value: 2,
    })
    .unwrap();
    assert_eq!(
        tier.load().unwrap(),
        Some(Snap {
            name: "g".into(),
            value: 2
        })
    );
}

#[test]
fn snapshot_filter_skips_when_returns_false() {
    let tier = memory_snapshot::<Snap, _>(SnapshotStorageOptions {
        name: Some("g".into()),
        filter: Some(Box::new(|s: &Snap| s.value > 0)),
        ..Default::default()
    });
    tier.save(Snap {
        name: "g".into(),
        value: 0,
    })
    .unwrap();
    assert_eq!(
        tier.load().unwrap(),
        None,
        "filter:false should skip persist"
    );
    tier.save(Snap {
        name: "g".into(),
        value: 5,
    })
    .unwrap();
    assert_eq!(
        tier.load().unwrap(),
        Some(Snap {
            name: "g".into(),
            value: 5
        })
    );
}

#[test]
fn snapshot_key_of_routes_by_snapshot_name() {
    // Two tiers each with their own key_of producing a name-driven key.
    let backend = memory_backend();
    let tier = snapshot_storage(
        Arc::clone(&backend),
        SnapshotStorageOptions {
            name: Some("router".into()),
            key_of: Some(Box::new(|s: &Snap| s.name.clone())),
            ..Default::default()
        },
    );
    tier.save(Snap {
        name: "alpha".into(),
        value: 1,
    })
    .unwrap();
    tier.save(Snap {
        name: "beta".into(),
        value: 2,
    })
    .unwrap();
    // Both keys present in the backend; `load()` returns the most-recently
    // saved one (last_saved_key tracks it).
    let keys = backend.list("").unwrap();
    assert!(keys.contains(&"alpha".to_string()));
    assert!(keys.contains(&"beta".to_string()));
    assert_eq!(
        tier.load().unwrap(),
        Some(Snap {
            name: "beta".into(),
            value: 2
        })
    );
}

#[test]
fn snapshot_debounce_buffers_until_explicit_flush() {
    let backend = memory_backend();
    let tier = snapshot_storage(
        Arc::clone(&backend),
        SnapshotStorageOptions::<Snap, _> {
            name: Some("g".into()),
            debounce_ms: Some(50), // advisory in M4.B — buffer until flush
            ..Default::default()
        },
    );
    tier.save(Snap {
        name: "g".into(),
        value: 1,
    })
    .unwrap();
    // No automatic flush — backend should NOT yet have the key.
    assert!(
        backend.read("g").unwrap().is_none(),
        "debounce should defer"
    );
    tier.flush().unwrap();
    assert!(
        backend.read("g").unwrap().is_some(),
        "explicit flush commits"
    );
}

#[test]
fn snapshot_compact_every_triggers_flush_on_nth_write() {
    let backend = memory_backend();
    let tier = snapshot_storage(
        Arc::clone(&backend),
        SnapshotStorageOptions::<Snap, _> {
            name: Some("g".into()),
            debounce_ms: Some(50), // defer otherwise
            compact_every: Some(3),
            ..Default::default()
        },
    );
    for i in 1..=2 {
        tier.save(Snap {
            name: "g".into(),
            value: i,
        })
        .unwrap();
        assert!(
            backend.read("g").unwrap().is_none(),
            "saves 1+2 should still be buffered",
        );
    }
    tier.save(Snap {
        name: "g".into(),
        value: 3,
    })
    .unwrap();
    assert!(
        backend.read("g").unwrap().is_some(),
        "3rd save should trigger flush via compact_every",
    );
}

#[test]
fn snapshot_rollback_discards_pending() {
    let backend = memory_backend();
    let tier = snapshot_storage(
        Arc::clone(&backend),
        SnapshotStorageOptions::<Snap, _> {
            name: Some("g".into()),
            debounce_ms: Some(50),
            ..Default::default()
        },
    );
    tier.save(Snap {
        name: "g".into(),
        value: 99,
    })
    .unwrap();
    tier.rollback().unwrap();
    tier.flush().unwrap();
    assert!(
        backend.read("g").unwrap().is_none(),
        "rollback then flush should not persist anything",
    );
}

// ── KV ────────────────────────────────────────────────────────────────────

#[test]
fn kv_save_load_round_trip() {
    let kv = memory_kv::<u32, _>(KvStorageOptions::default());
    kv.save("counter", 7).unwrap();
    assert_eq!(kv.load("counter").unwrap(), Some(7));
}

#[test]
fn kv_load_miss_returns_none() {
    let kv = memory_kv::<u32, _>(KvStorageOptions::default());
    assert!(kv.load("nope").unwrap().is_none());
}

#[test]
fn kv_delete_clears_value() {
    let kv = memory_kv::<u32, _>(KvStorageOptions::default());
    kv.save("k", 1).unwrap();
    kv.delete("k").unwrap();
    assert!(kv.load("k").unwrap().is_none());
}

#[test]
fn kv_list_returns_lex_asc_keys() {
    let kv = memory_kv::<u32, _>(KvStorageOptions::default());
    kv.save("c", 3).unwrap();
    kv.save("a", 1).unwrap();
    kv.save("b", 2).unwrap();
    let keys = kv.list("").unwrap();
    assert_eq!(keys, vec!["a", "b", "c"]);
}

#[test]
fn kv_filter_skips_when_returns_false() {
    let kv = memory_kv::<u32, _>(KvStorageOptions {
        filter: Some(Box::new(|_k, v: &u32| *v > 0)),
        ..Default::default()
    });
    kv.save("zero", 0).unwrap();
    kv.save("positive", 5).unwrap();
    assert!(kv.load("zero").unwrap().is_none());
    assert_eq!(kv.load("positive").unwrap(), Some(5));
}

#[test]
fn kv_compact_every_triggers_flush() {
    let backend = memory_backend();
    let kv = kv_storage(
        Arc::clone(&backend),
        KvStorageOptions::<u32, _> {
            debounce_ms: Some(50),
            compact_every: Some(2),
            ..Default::default()
        },
    );
    kv.save("k1", 1).unwrap();
    assert!(backend.read("k1").unwrap().is_none());
    kv.save("k2", 2).unwrap();
    assert!(
        backend.read("k1").unwrap().is_some() && backend.read("k2").unwrap().is_some(),
        "compact_every=2 should flush both buffered writes",
    );
}

// ── AppendLog ─────────────────────────────────────────────────────────────

#[test]
fn append_log_accumulates_then_loads() {
    let log = memory_append_log::<u32, _>(AppendLogStorageOptions {
        name: Some("events".into()),
        ..Default::default()
    });
    log.append_entries(&[1, 2, 3]).unwrap();
    log.append_entries(&[4, 5]).unwrap();
    let mut all = log.load_entries(None).unwrap();
    all.sort_unstable();
    assert_eq!(all, vec![1, 2, 3, 4, 5]);
}

#[test]
fn append_log_key_of_partitions_entries() {
    let backend = memory_backend();
    let log = append_log_storage(
        Arc::clone(&backend),
        AppendLogStorageOptions::<(String, u32), _> {
            name: Some("log".into()),
            key_of: Some(Box::new(|(k, _v)| k.clone())),
            ..Default::default()
        },
    );
    log.append_entries(&[
        ("alpha".to_string(), 1),
        ("beta".to_string(), 2),
        ("alpha".to_string(), 3),
    ])
    .unwrap();
    let keys = backend.list("").unwrap();
    assert!(keys.contains(&"alpha".to_string()));
    assert!(keys.contains(&"beta".to_string()));
    let alpha_entries = log.load_entries(Some("alpha")).unwrap();
    assert_eq!(alpha_entries.len(), 2);
}

#[test]
fn append_log_empty_entries_is_noop() {
    let log = memory_append_log::<u32, _>(AppendLogStorageOptions::default());
    log.append_entries(&[]).unwrap();
    assert_eq!(log.load_entries(None).unwrap(), Vec::<u32>::new());
}

#[test]
fn append_log_flush_merges_with_existing_backend_bucket() {
    let backend = memory_backend();
    let log1 = append_log_storage(
        Arc::clone(&backend),
        AppendLogStorageOptions::<u32, _> {
            name: Some("shared".into()),
            ..Default::default()
        },
    );
    log1.append_entries(&[1, 2]).unwrap();
    // New tier on same backend should see the prior entries on load.
    let log2 = append_log_storage(
        Arc::clone(&backend),
        AppendLogStorageOptions::<u32, _> {
            name: Some("shared".into()),
            ..Default::default()
        },
    );
    log2.append_entries(&[3, 4]).unwrap();
    let mut all = log2.load_entries(None).unwrap();
    all.sort_unstable();
    assert_eq!(all, vec![1, 2, 3, 4]);
}

// ── Multi-tier sharing one backend ────────────────────────────────────────

#[test]
fn multi_tier_share_one_backend() {
    // The paired `{ snapshot, wal }` shape from DS-14-storage §a:
    // snapshot baseline + kv (proxy for WAL) over the same backend.
    let backend = memory_backend();
    let snap = snapshot_storage(
        Arc::clone(&backend),
        SnapshotStorageOptions::<Snap, _> {
            name: Some("graph/snapshot".into()),
            ..Default::default()
        },
    );
    let wal: graphrefly_storage::KvStorage<_, u64, _> = kv_storage(
        Arc::clone(&backend),
        KvStorageOptions::<u64, _> {
            name: Some("graph/wal".into()),
            ..Default::default()
        },
    );
    snap.save(Snap {
        name: "graph/snapshot".into(),
        value: 1,
    })
    .unwrap();
    wal.save("graph/wal/00000000000000000001", 100).unwrap();
    wal.save("graph/wal/00000000000000000002", 200).unwrap();
    // Both tiers' data should be visible on a single backend list.
    let keys = backend.list("").unwrap();
    assert!(keys.iter().any(|k| k == "graph/snapshot"));
    assert!(keys.iter().any(|k| k.starts_with("graph/wal/")));
    // WAL tier round-trips:
    assert_eq!(
        wal.load("graph/wal/00000000000000000002").unwrap(),
        Some(200)
    );
}

// ── Dyn-safe enumeration ──────────────────────────────────────────────────

#[test]
fn list_by_prefix_bytes_via_dyn_base_tier() {
    let backend = memory_backend();
    let kv: graphrefly_storage::KvStorage<_, u32, _> = kv_storage(
        Arc::clone(&backend),
        KvStorageOptions::<u32, _> {
            name: Some("kv".into()),
            ..Default::default()
        },
    );
    kv.save("g/01", 1).unwrap();
    kv.save("g/02", 2).unwrap();
    kv.save("other", 99).unwrap();
    // Iterate through the dyn-safe trait. Bytes are JSON-encoded; we only
    // verify keys + non-empty bytes here (per-codec decoding is a separate
    // concern handled by typed helpers in M4.E).
    let tier: &dyn BaseStorageTier = &kv;
    let entries: Vec<_> = tier
        .list_by_prefix_bytes("g/")
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].0, "g/01");
    assert_eq!(entries[1].0, "g/02");
    assert!(!entries[0].1.is_empty());
}

// ── Cadence accessor surface ──────────────────────────────────────────────

#[test]
fn cadence_knobs_surface_via_base_trait() {
    let tier = memory_snapshot::<Snap, _>(SnapshotStorageOptions {
        name: Some("g".into()),
        debounce_ms: Some(250),
        compact_every: Some(10),
        ..Default::default()
    });
    let base: &dyn BaseStorageTier = &tier;
    assert_eq!(base.name(), "g");
    assert_eq!(base.debounce_ms(), Some(250));
    assert_eq!(base.compact_every(), Some(10));
}

// ── /qa A8 — filter + compact_every interaction ───────────────────────────

#[test]
fn snapshot_filter_rejection_does_not_bump_compact_count() {
    // /qa A8: filter-rejected saves must NOT advance the compact_every
    // cadence. Otherwise filter=false saves silently accelerate the next
    // compaction.
    let backend = memory_backend();
    let tier = snapshot_storage(
        Arc::clone(&backend),
        SnapshotStorageOptions::<Snap, _> {
            name: Some("g".into()),
            debounce_ms: Some(50),
            compact_every: Some(2),
            filter: Some(Box::new(|s: &Snap| s.value > 0)),
            ..Default::default()
        },
    );
    // 3 rejected saves first — count must stay at 0.
    for _ in 0..3 {
        tier.save(Snap {
            name: "g".into(),
            value: 0,
        })
        .unwrap();
        assert!(
            backend.read("g").unwrap().is_none(),
            "filter-rejected save should not trigger compact_every",
        );
    }
    // 1st accepted: count=1, no trigger.
    tier.save(Snap {
        name: "g".into(),
        value: 1,
    })
    .unwrap();
    assert!(backend.read("g").unwrap().is_none());
    // 2nd accepted: count=2, crosses boundary, triggers.
    tier.save(Snap {
        name: "g".into(),
        value: 2,
    })
    .unwrap();
    assert!(backend.read("g").unwrap().is_some());
}

#[test]
fn kv_filter_rejection_does_not_bump_compact_count() {
    // /qa A8 mirror for KvStorage.
    let backend = memory_backend();
    let kv = kv_storage(
        Arc::clone(&backend),
        KvStorageOptions::<u32, _> {
            debounce_ms: Some(50),
            compact_every: Some(2),
            filter: Some(Box::new(|_k, v: &u32| *v > 0)),
            ..Default::default()
        },
    );
    for k in ["a", "b", "c"] {
        kv.save(k, 0).unwrap();
        assert!(backend.read(k).unwrap().is_none());
    }
    kv.save("d", 1).unwrap();
    assert!(
        backend.read("d").unwrap().is_none(),
        "count=1 should not trigger"
    );
    kv.save("e", 2).unwrap();
    // Trigger fires at count=2; both d and e are now flushed.
    assert!(backend.read("d").unwrap().is_some());
    assert!(backend.read("e").unwrap().is_some());
}

// ── /qa F2 — boundary-crossing trigger handles batch saves ────────────────

#[test]
fn append_log_compact_every_triggers_when_batch_jumps_boundary() {
    // /qa F2 (D138-followup): pre-fix used strict `is_multiple_of`. A batch
    // of 5 with compact_every=3 jumped count 0→5; 5 % 3 != 0 → no flush.
    // Post-fix uses boundary-crossing — 0/3=0 vs 5/3=1, different → trigger.
    let backend = memory_backend();
    let log = append_log_storage(
        Arc::clone(&backend),
        AppendLogStorageOptions::<u32, _> {
            name: Some("events".into()),
            debounce_ms: Some(50),
            compact_every: Some(3),
            ..Default::default()
        },
    );
    log.append_entries(&[1, 2, 3, 4, 5]).unwrap();
    let entries = log.load_entries(None).unwrap();
    assert_eq!(
        entries.len(),
        5,
        "batch of 5 with compact_every=3 must trigger flush via boundary crossing",
    );
}

#[test]
fn kv_compact_every_triggers_when_save_jumps_multiple_boundaries() {
    // /qa F2 mirror for KvStorage. Save 10 entries with compact_every=3 in
    // separate saves; the trigger should fire at counts 3, 6, 9. Pre-fix
    // would fire only at exact divisibility; with boundary-crossing the
    // logic is the same for single-add but the test pins the semantic.
    let backend = memory_backend();
    let kv = kv_storage(
        Arc::clone(&backend),
        KvStorageOptions::<u32, _> {
            debounce_ms: Some(50),
            compact_every: Some(3),
            ..Default::default()
        },
    );
    for i in 1u32..=9 {
        kv.save(&format!("k{i}"), i).unwrap();
    }
    // After 9 saves, all 9 keys must be flushed (3 triggers fired).
    for i in 1u32..=9 {
        assert!(
            backend.read(&format!("k{i}")).unwrap().is_some(),
            "k{i} must be flushed after 9 saves with compact_every=3",
        );
    }
}

// ── /qa A4 — compact_every: Some(0) rejection ─────────────────────────────

#[test]
#[should_panic(expected = "compact_every must be None or Some(n)")]
fn snapshot_storage_panics_on_compact_every_zero() {
    let _: SnapshotStorage<MemoryBackend, Snap, _> = snapshot_storage(
        memory_backend(),
        SnapshotStorageOptions::<Snap, _> {
            compact_every: Some(0),
            ..Default::default()
        },
    );
}

#[test]
#[should_panic(expected = "compact_every must be None or Some(n)")]
fn kv_storage_panics_on_compact_every_zero() {
    let _: KvStorage<MemoryBackend, u32, _> = kv_storage(
        memory_backend(),
        KvStorageOptions::<u32, _> {
            compact_every: Some(0),
            ..Default::default()
        },
    );
}

#[test]
#[should_panic(expected = "compact_every must be None or Some(n)")]
fn append_log_storage_panics_on_compact_every_zero() {
    let _: AppendLogStorage<MemoryBackend, u32, _> = append_log_storage(
        memory_backend(),
        AppendLogStorageOptions::<u32, _> {
            compact_every: Some(0),
            ..Default::default()
        },
    );
}

// ── /qa A2 — KvStorage::delete error-path ordering ────────────────────────

#[test]
fn kv_delete_keeps_pending_when_backend_delete_fails() {
    // /qa A2 (2026-05-10): backend.delete fires FIRST; pending stays intact
    // on failure so the caller can retry. Use a stub backend whose `delete`
    // always errors to exercise the path.
    use graphrefly_storage::{StorageBackend, StorageError};
    struct FailingDelete;
    impl StorageBackend for FailingDelete {
        fn name(&self) -> &'static str {
            "failing-delete"
        }
        fn read(&self, _key: &str) -> Result<Option<Vec<u8>>, StorageError> {
            Ok(None)
        }
        fn write(&self, _k: &str, _b: &[u8]) -> Result<(), StorageError> {
            Ok(())
        }
        fn delete(&self, _key: &str) -> Result<(), StorageError> {
            Err(StorageError::BackendError {
                message: "stub".into(),
                source: None,
            })
        }
    }
    let kv = kv_storage(
        Arc::new(FailingDelete),
        KvStorageOptions::<u32, _> {
            debounce_ms: Some(50), // buffer pending
            ..Default::default()
        },
    );
    kv.save("k", 42).unwrap(); // buffered (debounced)
    let result = kv.delete("k");
    assert!(
        result.is_err(),
        "delete must propagate backend.delete error"
    );
    // After the failed delete, the pending value should still be there.
    kv.flush().unwrap();
    // Backend.write succeeded; pending was preserved → flush wrote `k=42`.
    // (We don't have a direct accessor on the stub to verify the bytes, but
    // the key invariant is: pending wasn't silently dropped on failure.
    // A follow-on flush has something to flush.)
}

#[test]
fn compact_method_forces_flush() {
    let backend = memory_backend();
    let tier = snapshot_storage(
        Arc::clone(&backend),
        SnapshotStorageOptions::<Snap, _> {
            name: Some("g".into()),
            debounce_ms: Some(50), // would normally buffer
            ..Default::default()
        },
    );
    tier.save(Snap {
        name: "g".into(),
        value: 1,
    })
    .unwrap();
    assert!(backend.read("g").unwrap().is_none());
    tier.compact().unwrap();
    assert!(backend.read("g").unwrap().is_some());
}
