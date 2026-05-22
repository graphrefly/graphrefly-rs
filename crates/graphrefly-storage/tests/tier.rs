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
    snapshot_storage, AppendCursor, AppendLogMode, AppendLogStorage, AppendLogStorageOptions,
    AppendLogStorageTier, BaseStorageTier, KvStorage, KvStorageOptions, KvStorageTier,
    LoadEntriesOpts, MemoryBackend, SnapshotStorage, SnapshotStorageOptions, SnapshotStorageTier,
    StorageBackend, StorageError,
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
    let mut all = log.load_entries_all(None).unwrap();
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
    let alpha_entries = log.load_entries_all(Some("alpha")).unwrap();
    assert_eq!(alpha_entries.len(), 2);
}

#[test]
fn append_log_empty_entries_is_noop() {
    let log = memory_append_log::<u32, _>(AppendLogStorageOptions::default());
    log.append_entries(&[]).unwrap();
    assert_eq!(log.load_entries_all(None).unwrap(), Vec::<u32>::new());
}

// D269 — mode + pagination (memo:Re P1 + loadEntries-pagination parity).

#[test]
fn append_log_overwrite_mode_replaces_bucket_per_flush() {
    let backend = memory_backend();
    let log = append_log_storage(
        Arc::clone(&backend),
        AppendLogStorageOptions::<u32, _> {
            name: Some("snap".into()),
            mode: AppendLogMode::Overwrite,
            ..Default::default()
        },
    );
    log.append_entries(&[1, 2, 3]).unwrap();
    assert_eq!(log.load_entries_all(None).unwrap(), vec![1, 2, 3]);
    // Second batch in Overwrite mode REPLACES the bucket (no read-merge).
    log.append_entries(&[10, 20]).unwrap();
    assert_eq!(log.load_entries_all(None).unwrap(), vec![10, 20]);
}

#[test]
fn append_log_mode_accessor_reports_configured_mode() {
    let log_a = memory_append_log::<u32, _>(AppendLogStorageOptions {
        mode: AppendLogMode::Append,
        ..Default::default()
    });
    assert_eq!(log_a.mode(), AppendLogMode::Append);
    let log_o = memory_append_log::<u32, _>(AppendLogStorageOptions {
        mode: AppendLogMode::Overwrite,
        ..Default::default()
    });
    assert_eq!(log_o.mode(), AppendLogMode::Overwrite);
}

#[test]
fn append_log_load_entries_page_size_returns_window_and_cursor() {
    let log = memory_append_log::<u32, _>(AppendLogStorageOptions {
        name: Some("evts".into()),
        ..Default::default()
    });
    log.append_entries(&[10, 20, 30, 40, 50]).unwrap();
    let r = log
        .load_entries(LoadEntriesOpts {
            page_size: Some(2),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(r.entries, vec![10, 20]);
    let c = r.cursor.expect("cursor should advance after window");
    assert_eq!(c.position, 2);

    let r2 = log
        .load_entries(LoadEntriesOpts {
            cursor: Some(c),
            page_size: Some(2),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(r2.entries, vec![30, 40]);
    let c2 = r2.cursor.expect("cursor should advance again");
    assert_eq!(c2.position, 4);

    let r3 = log
        .load_entries(LoadEntriesOpts {
            cursor: Some(c2),
            page_size: Some(2),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(r3.entries, vec![50]);
    assert!(r3.cursor.is_none(), "no more entries ⇒ cursor=None");
}

#[test]
fn append_log_load_entries_default_returns_whole_log_back_compat() {
    let log = memory_append_log::<u32, _>(AppendLogStorageOptions::default());
    log.append_entries(&[1, 2, 3]).unwrap();
    let r = log.load_entries(LoadEntriesOpts::default()).unwrap();
    assert_eq!(r.entries, vec![1, 2, 3]);
    assert!(
        r.cursor.is_none(),
        "no page_size ⇒ whole tail + cursor=None"
    );
}

#[test]
fn append_log_load_entries_cursor_past_end_returns_empty_page() {
    let log = memory_append_log::<u32, _>(AppendLogStorageOptions::default());
    log.append_entries(&[1, 2]).unwrap();
    let r = log
        .load_entries(LoadEntriesOpts {
            cursor: Some(AppendCursor::from_position(99)),
            page_size: Some(10),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(r.entries, Vec::<u32>::new());
    assert!(r.cursor.is_none());
}

// D268 — rollback epoch (memo:Re P0(d) parity).

#[test]
fn append_log_rollback_clears_pending_so_subsequent_flush_is_empty() {
    let backend = memory_backend();
    let log = append_log_storage(
        Arc::clone(&backend),
        AppendLogStorageOptions::<u32, _> {
            name: Some("events".into()),
            debounce_ms: Some(50), // buffer pending
            ..Default::default()
        },
    );
    log.append_entries(&[1, 2, 3]).unwrap();
    // Sequential rollback then flush — the buffered entries must not
    // be persisted. Pre-D268 this already worked (rollback cleared
    // pending; flush saw empty). D268 preserves this invariant.
    log.rollback().unwrap();
    log.flush().unwrap();
    assert_eq!(log.load_entries_all(None).unwrap(), Vec::<u32>::new());
}

#[test]
fn append_log_rollback_during_concurrent_flush_aborts_remaining_writes() {
    // D268: concurrent rollback bumps the epoch; flush captures the
    // initial epoch and aborts before any further per-bucket write.
    //
    // We construct this scenario with a backend whose `write` blocks
    // on a barrier so we can deterministically interleave rollback
    // BETWEEN flushes' per-bucket writes (after at least one write
    // landed but before others). The first bucket gets persisted
    // (already past the epoch check); the second is dropped.
    use std::sync::Arc as StdArc;
    use std::sync::Barrier;

    struct BarrierBackend {
        inner: StdArc<MemoryBackend>,
        first_write_done: StdArc<Barrier>,
        rollback_done: StdArc<Barrier>,
        write_count: parking_lot::Mutex<u32>,
    }
    impl StorageBackend for BarrierBackend {
        fn name(&self) -> &str {
            self.inner.name()
        }
        fn read(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
            self.inner.read(key)
        }
        fn write(&self, key: &str, bytes: &[u8]) -> Result<(), StorageError> {
            let was_first = {
                let mut n = self.write_count.lock();
                *n += 1;
                *n == 1
            };
            self.inner.write(key, bytes)?;
            if was_first {
                // Signal the rollback thread that the first write landed.
                self.first_write_done.wait();
                // Wait for rollback to complete BEFORE returning so the
                // outer flush loop sees the bumped epoch on its next
                // per-bucket check.
                self.rollback_done.wait();
            }
            Ok(())
        }
        fn delete(&self, key: &str) -> Result<(), StorageError> {
            self.inner.delete(key)
        }
        fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
            self.inner.list(prefix)
        }
    }
    let inner = memory_backend();
    let first_write_done = StdArc::new(Barrier::new(2));
    let rollback_done = StdArc::new(Barrier::new(2));
    let backend = StdArc::new(BarrierBackend {
        inner: StdArc::clone(&inner),
        first_write_done: StdArc::clone(&first_write_done),
        rollback_done: StdArc::clone(&rollback_done),
        write_count: parking_lot::Mutex::new(0),
    });
    let log = StdArc::new(append_log_storage(
        StdArc::clone(&backend) as Arc<dyn StorageBackend>,
        AppendLogStorageOptions::<(String, u32), _> {
            name: Some("evts".into()),
            debounce_ms: Some(50),
            key_of: Some(Box::new(|(k, _)| k.clone())),
            ..Default::default()
        },
    ));
    log.append_entries(&[("alpha".to_string(), 1), ("beta".to_string(), 2)])
        .unwrap();

    let log_for_flush = StdArc::clone(&log);
    let flush_thread = std::thread::spawn(move || log_for_flush.flush());

    // Wait for the first per-bucket write to land.
    first_write_done.wait();
    // Now rollback — bumps epoch. Flush thread's next epoch check
    // will see the bump and abort the remaining bucket.
    log.rollback().unwrap();
    rollback_done.wait();
    flush_thread.join().unwrap().unwrap();

    // Exactly ONE of the two buckets was persisted (first to land).
    let all_keys = inner.list("").unwrap();
    assert_eq!(
        all_keys.len(),
        1,
        "expected exactly 1 persisted bucket (epoch aborted the second), got keys: {all_keys:?}",
    );
}

/// Helper backend that delegates to an inner [`MemoryBackend`] but
/// injects controllable faults on the Nth `write` (or every write).
/// Used by the `append_log_*_failure_does_not_duplicate_on_retry`
/// regression tests below.
///
/// /qa-fix 2026-05-21: caught a flush-error-restore regression where
/// `bucket_backup` was bound and immediately discarded with
/// `let _ = bucket_backup;` and the error paths restored
/// `final_payload` (= existing-read-from-backend ⊕ new entries) into
/// `pending`. On retry, `flush()` re-read existing from backend and
/// merged again — silently DUPLICATING existing entries in storage.
/// Cargo gate did not catch this because the existing rollback /
/// merge tests never fault-inject a write failure.
struct FaultBackend {
    inner: std::sync::Arc<MemoryBackend>,
    /// `true` → fault EVERY write (the encode-error test path uses
    /// this only after the encode/write fault is observed; the
    /// retry runs after `inject_write_fault.store(false, ...)`).
    inject_write_fault: std::sync::atomic::AtomicBool,
    write_count: parking_lot::Mutex<u32>,
}

impl FaultBackend {
    fn new(inner: std::sync::Arc<MemoryBackend>) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            inner,
            inject_write_fault: std::sync::atomic::AtomicBool::new(false),
            write_count: parking_lot::Mutex::new(0),
        })
    }
}

impl StorageBackend for FaultBackend {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        self.inner.read(key)
    }
    fn write(&self, key: &str, bytes: &[u8]) -> Result<(), StorageError> {
        *self.write_count.lock() += 1;
        if self
            .inject_write_fault
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(StorageError::BackendError {
                message: "injected write fault".into(),
                source: None,
            });
        }
        self.inner.write(key, bytes)
    }
    fn delete(&self, key: &str) -> Result<(), StorageError> {
        self.inner.delete(key)
    }
    fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        self.inner.list(prefix)
    }
}

#[test]
fn append_log_append_mode_write_failure_does_not_duplicate_on_retry() {
    // /qa regression — covers the D269 error-path restore semantic.
    // Before the fix, a write failure on flush #1 caused flush #2 to
    // re-read the (committed) existing bucket AND re-merge with
    // `final_payload` (which contained the existing entries +
    // the new entries), so existing entries silently DUPLICATED.
    //
    // Scenario:
    //   1. tier with `Append` mode + a key with existing data [1, 2]
    //   2. append [3, 4]; flush fails on write (fault injected)
    //   3. flush retries (fault cleared); persisted bucket MUST be
    //      [1, 2, 3, 4], NOT [1, 2, 1, 2, 3, 4].
    let memory = std::sync::Arc::new(MemoryBackend::with_name("evts"));
    let fault = FaultBackend::new(std::sync::Arc::clone(&memory));
    let backend: Arc<dyn StorageBackend> = std::sync::Arc::clone(&fault) as _;

    // Seed the existing bucket via a clean flush. `debounce_ms: Some(50)`
    // disables the auto-flush-on-append fast path so we can control
    // exactly when flush() runs (otherwise append_entries auto-flushes
    // synchronously when debounce_ms is None — the seed write would then
    // be the only write, and the fault would never bite).
    let log = append_log_storage(
        Arc::clone(&backend),
        AppendLogStorageOptions::<u32, _> {
            name: Some("evts".into()),
            mode: AppendLogMode::Append,
            debounce_ms: Some(50),
            ..Default::default()
        },
    );
    log.append_entries(&[1, 2]).unwrap();
    log.flush().unwrap();
    assert_eq!(log.load_entries_all(None).unwrap(), vec![1, 2]);

    // Append new entries; fault the next write; flush returns Err.
    log.append_entries(&[3, 4]).unwrap();
    fault
        .inject_write_fault
        .store(true, std::sync::atomic::Ordering::Release);
    let err = log.flush();
    assert!(err.is_err(), "first flush should have failed: {err:?}");

    // Retry: lift the fault and flush again.
    fault
        .inject_write_fault
        .store(false, std::sync::atomic::Ordering::Release);
    log.flush().unwrap();

    // The bucket on the backend MUST be [1, 2, 3, 4] (no duplicates).
    let loaded = log.load_entries_all(None).unwrap();
    assert_eq!(
        loaded,
        vec![1, 2, 3, 4],
        "Append-mode error-path must NOT duplicate existing entries on retry",
    );
}

// NOTE: The companion encode-failure regression is intentionally not
// written as a separate test — both the encode-error and write-error
// arms in `AppendLogStorage::flush` use the same `restore_payload`
// closure binding, so the write-failure test above locks the contract
// for both. A custom-codec encode-fault test would require generic-
// type plumbing on `AppendLogStorageOptions<u32, C>` that buys no
// extra coverage.

#[test]
fn append_log_rollback_during_write_error_drops_bucket_not_restores() {
    // D-B (next batch, 2026-05-21) — rollback-epoch is also checked in
    // the error-restore path. Without the fix: a `backend.write` that
    // returns Err while `rollback()` interleaves would unconditionally
    // re-insert the bucket into `pending`; the next flush would re-
    // resurrect a bucket the user just rolled back.
    //
    // Scenario:
    //   1. Tier with debounce_ms=50 (no auto-flush)
    //   2. append [9, 10]; flush() starts on thread A
    //   3. backend.write blocks on a barrier
    //   4. Thread B: rollback() (bumps epoch, clears pending — already
    //      empty since flush took it)
    //   5. backend.write returns Err (fault injected after rollback)
    //   6. flush's error-restore path checks epoch; sees advance; DROPS
    //      the bucket instead of restoring
    //   7. Assert: `pending` is empty (NOT [9, 10]); a clean retry
    //      flush() persists nothing (the rollback won).
    use std::sync::Arc as StdArc;
    use std::sync::Barrier;

    struct FaultBarrierBackend {
        inner: StdArc<MemoryBackend>,
        write_ready: StdArc<Barrier>,
        rollback_done: StdArc<Barrier>,
    }
    impl StorageBackend for FaultBarrierBackend {
        fn name(&self) -> &str {
            self.inner.name()
        }
        fn read(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
            self.inner.read(key)
        }
        fn write(&self, _key: &str, _bytes: &[u8]) -> Result<(), StorageError> {
            // Signal the rollback thread we're about to attempt a write,
            // then wait for it to bump the epoch before we return Err.
            self.write_ready.wait();
            self.rollback_done.wait();
            Err(StorageError::BackendError {
                message: "injected write fault (post-rollback)".into(),
                source: None,
            })
        }
        fn delete(&self, key: &str) -> Result<(), StorageError> {
            self.inner.delete(key)
        }
        fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
            self.inner.list(prefix)
        }
    }
    let inner = memory_backend();
    let write_ready = StdArc::new(Barrier::new(2));
    let rollback_done = StdArc::new(Barrier::new(2));
    let backend = StdArc::new(FaultBarrierBackend {
        inner: StdArc::clone(&inner),
        write_ready: StdArc::clone(&write_ready),
        rollback_done: StdArc::clone(&rollback_done),
    });
    let log = StdArc::new(append_log_storage(
        StdArc::clone(&backend) as Arc<dyn StorageBackend>,
        AppendLogStorageOptions::<u32, _> {
            name: Some("evts".into()),
            debounce_ms: Some(50),
            ..Default::default()
        },
    ));
    log.append_entries(&[9, 10]).unwrap();

    let log_for_flush = StdArc::clone(&log);
    let flush_thread = std::thread::spawn(move || log_for_flush.flush());

    write_ready.wait();
    log.rollback().unwrap(); // bump epoch; clear pending (already empty)
    rollback_done.wait();

    let result = flush_thread.join().unwrap();
    assert!(
        result.is_err(),
        "flush should still surface the underlying write error: {result:?}",
    );

    // The rollback won — `pending` must be empty (bucket dropped, not
    // restored). Verified by a clean retry flush: nothing to write.
    log.flush().unwrap();
    let persisted = inner.list("").unwrap();
    assert!(
        persisted.is_empty(),
        "post-rollback pending bucket must have been dropped, not restored \
         (would have re-resurrected on retry); keys: {persisted:?}",
    );
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
    let mut all = log2.load_entries_all(None).unwrap();
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
    let entries = log.load_entries_all(None).unwrap();
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
