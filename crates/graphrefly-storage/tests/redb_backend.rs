//! Integration tests for the redb-backed storage tiers (M4.D).
//!
//! These tests exercise the full stack: `RedbBackend` → tier factories →
//! `SnapshotStorageTier` / `KvStorageTier` / `AppendLogStorageTier` impls.
//! Unit tests for `RedbBackend` itself live in `src/redb.rs::tests`.

#![cfg(feature = "redb-store")]

use graphrefly_storage::{
    redb_append_log_default, redb_backend, redb_kv_default, redb_snapshot_default,
    AppendLogStorageTier, BaseStorageTier, KvStorageTier, SnapshotStorageTier,
};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

/// Shared test payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Snap {
    name: String,
    value: u64,
}

fn tmp_dir() -> TempDir {
    TempDir::new().unwrap()
}

// ── Snapshot tier ────────────────────────────────────────────────────────

#[test]
fn snapshot_round_trip() {
    let dir = tmp_dir();
    let tier = redb_snapshot_default::<Snap>(dir.path().join("snap.redb")).unwrap();
    let s = Snap {
        name: "alpha".into(),
        value: 42,
    };
    tier.save(s.clone()).unwrap();
    let loaded = tier.load().unwrap().expect("should load saved snapshot");
    assert_eq!(loaded, s);
}

#[test]
fn snapshot_overwrite_returns_latest() {
    let dir = tmp_dir();
    let tier = redb_snapshot_default::<Snap>(dir.path().join("snap.redb")).unwrap();
    tier.save(Snap {
        name: "v1".into(),
        value: 1,
    })
    .unwrap();
    tier.save(Snap {
        name: "v2".into(),
        value: 2,
    })
    .unwrap();
    let loaded = tier.load().unwrap().unwrap();
    assert_eq!(loaded.value, 2);
}

#[test]
fn snapshot_cross_restart_durability() {
    let dir = tmp_dir();
    let path = dir.path().join("durable.redb");
    {
        let tier = redb_snapshot_default::<Snap>(path.clone()).unwrap();
        tier.save(Snap {
            name: "persist".into(),
            value: 99,
        })
        .unwrap();
    }
    // Reopen from same path.
    let tier2 = redb_snapshot_default::<Snap>(path).unwrap();
    let loaded = tier2.load().unwrap().unwrap();
    assert_eq!(loaded.value, 99);
}

#[test]
fn snapshot_load_empty_returns_none() {
    let dir = tmp_dir();
    let tier = redb_snapshot_default::<Snap>(dir.path().join("empty.redb")).unwrap();
    assert!(tier.load().unwrap().is_none());
}

// ── KV tier ──────────────────────────────────────────────────────────────

#[test]
fn kv_save_load_round_trip() {
    let dir = tmp_dir();
    let tier = redb_kv_default::<u64>(dir.path().join("kv.redb")).unwrap();
    tier.save("a", 10).unwrap();
    tier.save("b", 20).unwrap();
    assert_eq!(tier.load("a").unwrap(), Some(10));
    assert_eq!(tier.load("b").unwrap(), Some(20));
    assert!(tier.load("c").unwrap().is_none());
}

#[test]
fn kv_delete_removes_entry() {
    let dir = tmp_dir();
    let tier = redb_kv_default::<u64>(dir.path().join("kv.redb")).unwrap();
    tier.save("k", 42).unwrap();
    tier.delete("k").unwrap();
    assert!(tier.load("k").unwrap().is_none());
}

#[test]
fn kv_list_lex_asc() {
    let dir = tmp_dir();
    let tier = redb_kv_default::<u64>(dir.path().join("kv.redb")).unwrap();
    tier.save("g/02", 2).unwrap();
    tier.save("g/01", 1).unwrap();
    tier.save("g/10", 10).unwrap();
    tier.save("other", 0).unwrap();
    let keys = tier.list("g/").unwrap();
    assert_eq!(keys, vec!["g/01", "g/02", "g/10"]);
}

#[test]
fn kv_cross_restart_durability() {
    let dir = tmp_dir();
    let path = dir.path().join("kv_durable.redb");
    {
        let tier = redb_kv_default::<String>(path.clone()).unwrap();
        tier.save("key", "value".to_string()).unwrap();
    }
    let tier2 = redb_kv_default::<String>(path).unwrap();
    assert_eq!(tier2.load("key").unwrap(), Some("value".to_string()));
}

// ── Append-log tier ──────────────────────────────────────────────────────

#[test]
fn append_log_accumulates_and_loads() {
    let dir = tmp_dir();
    let tier = redb_append_log_default::<u64>(dir.path().join("log.redb")).unwrap();
    tier.append_entries(&[1, 2, 3]).unwrap();
    tier.append_entries(&[4, 5]).unwrap();
    let entries = tier.load_entries(None).unwrap();
    assert_eq!(entries, vec![1, 2, 3, 4, 5]);
}

#[test]
fn append_log_cross_restart_durability() {
    let dir = tmp_dir();
    let path = dir.path().join("log_durable.redb");
    {
        let tier = redb_append_log_default::<u64>(path.clone()).unwrap();
        tier.append_entries(&[10, 20]).unwrap();
    }
    let tier2 = redb_append_log_default::<u64>(path).unwrap();
    let entries = tier2.load_entries(None).unwrap();
    assert_eq!(entries, vec![10, 20]);
}

// ── Base tier surface ────────────────────────────────────────────────────

#[test]
fn compact_delegates_to_flush() {
    let dir = tmp_dir();
    let tier = redb_snapshot_default::<u64>(dir.path().join("compact.redb")).unwrap();
    tier.save(42).unwrap();
    tier.compact().unwrap();
    assert_eq!(tier.load().unwrap(), Some(42));
}

#[test]
fn rollback_discards_pending() {
    let dir = tmp_dir();
    let path = dir.path().join("rb.redb");
    let backend = redb_backend(&path).unwrap();
    use graphrefly_storage::{snapshot_storage, SnapshotStorageOptions};
    let opts = SnapshotStorageOptions::<u64, _> {
        debounce_ms: Some(1000), // buffer, don't auto-flush
        ..Default::default()
    };
    let tier = snapshot_storage(backend, opts);
    tier.save(42).unwrap();
    tier.rollback().unwrap();
    tier.flush().unwrap();
    assert!(tier.load().unwrap().is_none());
}

#[test]
fn list_by_prefix_bytes_iterates_redb_keys() {
    let dir = tmp_dir();
    let tier = redb_kv_default::<u64>(dir.path().join("prefix.redb")).unwrap();
    tier.save("wal/001", 1).unwrap();
    tier.save("wal/002", 2).unwrap();
    tier.save("snap", 0).unwrap();
    let entries: Vec<_> = tier
        .list_by_prefix_bytes("wal/")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].0, "wal/001");
    assert_eq!(entries[1].0, "wal/002");
}

// ── F1 tier-level pending restore test ───────────────────────────────────

/// Verify that a backend write failure restores the pending state so the
/// caller can retry (D165 — F1 fix).
#[test]
fn f1_snapshot_flush_failure_preserves_pending() {
    use graphrefly_storage::{snapshot_storage, SnapshotStorageOptions, StorageBackend};
    use std::sync::Arc;

    /// Backend that fails on write.
    struct FailWrite;
    impl StorageBackend for FailWrite {
        fn name(&self) -> &str {
            "fail-write"
        }
        fn read(&self, _key: &str) -> Result<Option<Vec<u8>>, graphrefly_storage::StorageError> {
            Ok(None)
        }
        fn write(&self, _key: &str, _bytes: &[u8]) -> Result<(), graphrefly_storage::StorageError> {
            Err(graphrefly_storage::StorageError::BackendError {
                message: "injected failure".into(),
                source: None,
            })
        }
    }

    let backend = Arc::new(FailWrite);
    let tier = snapshot_storage(
        backend,
        SnapshotStorageOptions::<u64, _> {
            debounce_ms: Some(1000), // buffer, don't auto-flush on save
            ..Default::default()
        },
    );
    tier.save(42).unwrap(); // buffers
    let result = tier.flush();
    assert!(result.is_err(), "flush should fail");
    // Pending should be restored — rollback should clear it.
    tier.rollback().unwrap();
    // After rollback, flush should succeed (nothing to flush).
    tier.flush().unwrap();
}

#[test]
fn f1_kv_flush_failure_preserves_pending() {
    use graphrefly_storage::{kv_storage, KvStorageOptions, StorageBackend};
    use std::sync::Arc;

    struct FailWrite;
    impl StorageBackend for FailWrite {
        fn name(&self) -> &str {
            "fail-write"
        }
        fn read(&self, _key: &str) -> Result<Option<Vec<u8>>, graphrefly_storage::StorageError> {
            Ok(None)
        }
        fn write(&self, _key: &str, _bytes: &[u8]) -> Result<(), graphrefly_storage::StorageError> {
            Err(graphrefly_storage::StorageError::BackendError {
                message: "injected failure".into(),
                source: None,
            })
        }
    }

    let backend = Arc::new(FailWrite);
    let tier = kv_storage(
        backend,
        KvStorageOptions::<u64, _> {
            debounce_ms: Some(1000),
            ..Default::default()
        },
    );
    tier.save("k", 42).unwrap(); // buffers
    let result = tier.flush();
    assert!(result.is_err(), "flush should fail");
    // Pending should be restored.
    tier.rollback().unwrap();
    tier.flush().unwrap();
}

// The entire file is `#[cfg(feature = "redb-store")]` — if any test in this
// file runs, the feature gate is active.
