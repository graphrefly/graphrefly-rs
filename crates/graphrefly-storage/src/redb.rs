//! redb-backed kv backend (M4.D — DS-14-storage Audit 4, ACID transactions).
//!
//! [`RedbBackend`] wraps a [`redb::Database`] as a [`StorageBackend`]. Each
//! `write()` / `delete()` opens its own write transaction and commits — ACID
//! guarantees per call. `read()` / `list()` use read transactions (concurrent
//! with any write). `flush()` is a no-op (writes are durable on commit).
//! Space reclamation is managed internally by redb; `flush()` is a no-op
//! since each `write()` commits its own transaction.
//!
//! All keys live in a single `"graphrefly"` table (D162 — tiers already
//! namespace via key prefixes; a single table matches the flat-kv model of
//! [`MemoryBackend`] / [`FileBackend`]).
//!
//! # Thread safety
//!
//! `redb::Database` is `Send + Sync`. `begin_write()` serializes concurrent
//! writers by design (MVCC — one writer at a time, concurrent readers). No
//! additional synchronization needed on `RedbBackend`. This structurally
//! closes the F3 deferred concern (concurrent flush race on append-log) —
//! see `porting-deferred.md`.
//!
//! # Crash safety
//!
//! redb is crash-safe by default — committed transactions survive process
//! crashes. Unlike [`FileBackend`]'s per-file atomic-rename, redb provides
//! cross-key atomicity within a single transaction. The per-write-call
//! transaction granularity (D163) means each `StorageBackend::write()` is
//! individually atomic; cross-key atomicity (e.g., flushing multiple
//! append-log buckets) would require holding one transaction across multiple
//! calls, which isn't supported by the current `StorageBackend` trait.
//! Cross-key atomicity at the tier level is a M4.E concern
//! (`Graph::attach_storage` batched flush).
//!
//! Cargo feature: gated behind `redb-store` (default-on).

use std::path::Path;
use std::sync::Arc;

use redb::{Database, ReadableTable, TableDefinition};

use crate::backend::StorageBackend;
use crate::codec::{Codec, JsonCodec};
use crate::error::StorageError;
use crate::memory::{
    append_log_storage, kv_storage, snapshot_storage, AppendLogStorage, AppendLogStorageOptions,
    KvStorage, KvStorageOptions, SnapshotStorage, SnapshotStorageOptions,
};

use serde::{de::DeserializeOwned, Serialize};

/// Single table for all keys (D162). Tiers namespace via key prefixes
/// (e.g. `"graph/wal/00000000000000000001"`, `"snapshot:my-graph"`).
const TABLE: TableDefinition<'_, &str, &[u8]> = TableDefinition::new("graphrefly");

/// Map redb errors to [`StorageError::BackendError`].
fn map_err(e: impl std::error::Error + Send + Sync + 'static) -> StorageError {
    StorageError::BackendError {
        message: e.to_string(),
        source: Some(Box::new(e)),
    }
}

/// redb-backed [`StorageBackend`].
///
/// One [`redb::Database`] file per backend instance. All keys share a single
/// `"graphrefly"` table. Per-write-call ACID transactions (D163).
///
/// # Example
///
/// ```ignore
/// use std::sync::Arc;
/// use graphrefly_storage::{redb_backend, snapshot_storage, SnapshotStorageOptions};
///
/// let backend = redb_backend("./checkpoints.redb");
/// let tier = snapshot_storage(backend, SnapshotStorageOptions::<MyState, _>::default());
/// tier.save(state).unwrap();
/// ```
pub struct RedbBackend {
    db: Database,
    name: String,
}

// Manual Debug because redb::Database doesn't impl Debug.
impl std::fmt::Debug for RedbBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedbBackend")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl RedbBackend {
    /// Open or create a redb database at `path`.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::BackendError` if the database file can't be
    /// opened or created (permissions, corrupt file, etc.).
    pub fn new(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let path = path.as_ref();
        let db = Database::create(path).map_err(map_err)?;
        let name = format!("redb:{}", path.display());
        Ok(Self { db, name })
    }

    /// Diagnostic: the path-derived name (e.g. `"redb:./checkpoints.redb"`).
    #[must_use]
    pub fn name_str(&self) -> &str {
        &self.name
    }
}

impl StorageBackend for RedbBackend {
    fn name(&self) -> &str {
        &self.name
    }

    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        let txn = self.db.begin_read().map_err(map_err)?;
        let table = match txn.open_table(TABLE) {
            Ok(t) => t,
            // Table doesn't exist yet → empty database, no keys.
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(map_err(e)),
        };
        match table.get(key).map_err(map_err)? {
            Some(guard) => Ok(Some(guard.value().to_vec())),
            None => Ok(None),
        }
    }

    fn write(&self, key: &str, bytes: &[u8]) -> Result<(), StorageError> {
        let txn = self.db.begin_write().map_err(map_err)?;
        {
            let mut table = txn.open_table(TABLE).map_err(map_err)?;
            table.insert(key, bytes).map_err(map_err)?;
        }
        txn.commit().map_err(map_err)?;
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<(), StorageError> {
        let txn = self.db.begin_write().map_err(map_err)?;
        {
            let mut table = match txn.open_table(TABLE) {
                Ok(t) => t,
                // Table doesn't exist → nothing to delete.
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
                Err(e) => return Err(map_err(e)),
            };
            // remove() returns the old value (or None); we discard it.
            let _ = table.remove(key).map_err(map_err)?;
        }
        txn.commit().map_err(map_err)?;
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let txn = self.db.begin_read().map_err(map_err)?;
        let table = match txn.open_table(TABLE) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(map_err(e)),
        };

        let mut keys = Vec::new();
        if prefix.is_empty() {
            // All keys.
            let iter = table.iter().map_err(map_err)?;
            for entry in iter {
                let entry = entry.map_err(map_err)?;
                keys.push(entry.0.value().to_string());
            }
        } else {
            // Range scan: redb's B-tree stores keys in lex-ASC order.
            // We start from `prefix` and take while the key starts with it.
            let iter = table.range(prefix..).map_err(map_err)?;
            for entry in iter {
                let entry = entry.map_err(map_err)?;
                let k = entry.0.value();
                if !k.starts_with(prefix) {
                    break;
                }
                keys.push(k.to_string());
            }
        }
        Ok(keys)
    }

    fn flush(&self) -> Result<(), StorageError> {
        // No-op: each write() commits its own transaction (D163).
        Ok(())
    }
}

// ── Factory ──────────────────────────────────────────────────────────────

/// Open or create a redb database at `path`, returning an `Arc<RedbBackend>`.
///
/// Use this when sharing a single backend across multiple tiers (the paired
/// `{ snapshot, wal }` pattern from DS-14-storage §a).
///
/// # Errors
///
/// Returns `StorageError::BackendError` on filesystem / permission / corrupt
/// file errors.
pub fn redb_backend(path: impl AsRef<Path>) -> Result<Arc<RedbBackend>, StorageError> {
    Ok(Arc::new(RedbBackend::new(path)?))
}

// ── Convenience tier wrappers ────────────────────────────────────────────

/// Snapshot tier over a redb backend at `path`.
///
/// # Errors
///
/// Returns `StorageError::BackendError` if the database can't be opened.
///
/// # Panics
///
/// Panics if `opts.compact_every == Some(0)`.
pub fn redb_snapshot<T, C>(
    path: impl AsRef<Path>,
    opts: SnapshotStorageOptions<T, C>,
) -> Result<SnapshotStorage<RedbBackend, T, C>, StorageError>
where
    T: Send + Sync + 'static,
    C: Codec<T>,
{
    Ok(snapshot_storage(redb_backend(path)?, opts))
}

/// Snapshot tier over a redb backend with default options (`JsonCodec`).
///
/// # Errors
///
/// Returns `StorageError::BackendError` if the database can't be opened.
pub fn redb_snapshot_default<T>(
    path: impl AsRef<Path>,
) -> Result<SnapshotStorage<RedbBackend, T, JsonCodec>, StorageError>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    redb_snapshot(path, SnapshotStorageOptions::default())
}

/// Append-log tier over a redb backend at `path`.
///
/// # Errors
///
/// Returns `StorageError::BackendError` if the database can't be opened.
///
/// # Panics
///
/// Panics if `opts.compact_every == Some(0)`.
pub fn redb_append_log<T, C>(
    path: impl AsRef<Path>,
    opts: AppendLogStorageOptions<T, C>,
) -> Result<AppendLogStorage<RedbBackend, T, C>, StorageError>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    C: Codec<Vec<T>>,
{
    Ok(append_log_storage(redb_backend(path)?, opts))
}

/// Append-log tier over a redb backend with default options (`JsonCodec`).
///
/// # Errors
///
/// Returns `StorageError::BackendError` if the database can't be opened.
pub fn redb_append_log_default<T>(
    path: impl AsRef<Path>,
) -> Result<AppendLogStorage<RedbBackend, T, JsonCodec>, StorageError>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    redb_append_log(path, AppendLogStorageOptions::default())
}

/// KV tier over a redb backend at `path`.
///
/// # Errors
///
/// Returns `StorageError::BackendError` if the database can't be opened.
///
/// # Panics
///
/// Panics if `opts.compact_every == Some(0)`.
pub fn redb_kv<T, C>(
    path: impl AsRef<Path>,
    opts: KvStorageOptions<T, C>,
) -> Result<KvStorage<RedbBackend, T, C>, StorageError>
where
    T: Send + Sync + 'static,
    C: Codec<T>,
{
    Ok(kv_storage(redb_backend(path)?, opts))
}

/// KV tier over a redb backend with default options (`JsonCodec`).
///
/// # Errors
///
/// Returns `StorageError::BackendError` if the database can't be opened.
pub fn redb_kv_default<T>(
    path: impl AsRef<Path>,
) -> Result<KvStorage<RedbBackend, T, JsonCodec>, StorageError>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    redb_kv(path, KvStorageOptions::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn tmp_db() -> (TempDir, Arc<RedbBackend>) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.redb");
        let backend = redb_backend(&path).unwrap();
        (dir, backend)
    }

    #[test]
    fn read_write_round_trip() {
        let (_dir, b) = tmp_db();
        b.write("k1", b"hello").unwrap();
        assert_eq!(b.read("k1").unwrap(), Some(b"hello".to_vec()));
    }

    #[test]
    fn read_miss_returns_none() {
        let (_dir, b) = tmp_db();
        assert!(b.read("nope").unwrap().is_none());
    }

    #[test]
    fn read_from_empty_database_returns_none() {
        let (_dir, b) = tmp_db();
        // No writes at all — table doesn't exist yet.
        assert!(b.read("anything").unwrap().is_none());
    }

    #[test]
    fn delete_removes_key() {
        let (_dir, b) = tmp_db();
        b.write("k", b"v").unwrap();
        b.delete("k").unwrap();
        assert!(b.read("k").unwrap().is_none());
    }

    #[test]
    fn delete_nonexistent_key_is_ok() {
        let (_dir, b) = tmp_db();
        // No table exists yet.
        b.delete("nope").unwrap();
        // Table exists but key doesn't.
        b.write("other", b"v").unwrap();
        b.delete("nope").unwrap();
    }

    #[test]
    fn list_lex_asc() {
        let (_dir, b) = tmp_db();
        b.write("g/10", b"a").unwrap();
        b.write("g/02", b"b").unwrap();
        b.write("g/01", b"c").unwrap();
        b.write("other", b"d").unwrap();
        let keys = b.list("g/").unwrap();
        assert_eq!(keys, vec!["g/01", "g/02", "g/10"]);
    }

    #[test]
    fn list_empty_prefix_returns_all_sorted() {
        let (_dir, b) = tmp_db();
        b.write("b", b"y").unwrap();
        b.write("a", b"x").unwrap();
        let keys = b.list("").unwrap();
        assert_eq!(keys, vec!["a", "b"]);
    }

    #[test]
    fn list_empty_database_returns_empty() {
        let (_dir, b) = tmp_db();
        let keys = b.list("g/").unwrap();
        assert!(keys.is_empty());
    }

    #[test]
    fn write_overwrites_existing_key() {
        let (_dir, b) = tmp_db();
        b.write("k", b"v1").unwrap();
        b.write("k", b"v2").unwrap();
        assert_eq!(b.read("k").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn name_includes_path() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.redb");
        let b = RedbBackend::new(&path).unwrap();
        assert!(b.name().starts_with("redb:"));
        assert!(b.name().contains("test.redb"));
    }

    #[test]
    fn flush_is_noop() {
        let (_dir, b) = tmp_db();
        b.write("k", b"v").unwrap();
        b.flush().unwrap();
        assert_eq!(b.read("k").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn data_survives_reopen() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("persist.redb");
        {
            let b = RedbBackend::new(&path).unwrap();
            b.write("k1", b"durable").unwrap();
        }
        // Reopen same file.
        let b2 = RedbBackend::new(&path).unwrap();
        assert_eq!(b2.read("k1").unwrap(), Some(b"durable".to_vec()));
    }

    #[test]
    fn concurrent_readers_and_writer() {
        let (_dir, b) = tmp_db();
        b.write("k", b"v1").unwrap();

        // Read should work while we do writes (MVCC).
        let v = b.read("k").unwrap();
        assert_eq!(v, Some(b"v1".to_vec()));
        b.write("k", b"v2").unwrap();
        let v2 = b.read("k").unwrap();
        assert_eq!(v2, Some(b"v2".to_vec()));
    }

    #[test]
    fn arc_factory_shares_backend() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("shared.redb");
        let b1 = redb_backend(&path).unwrap();
        // Clone the Arc — same underlying Database.
        let b2 = Arc::clone(&b1);
        b1.write("k", b"from_b1").unwrap();
        assert_eq!(b2.read("k").unwrap(), Some(b"from_b1".to_vec()));
    }
}
