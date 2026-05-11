//! Concrete tier implementations + memory factories (M4.B 2026-05-10).
//!
//! Three generic structs back the three tier sub-traits:
//! - [`SnapshotStorage<B, T, C>`] → [`SnapshotStorageTier<T>`]
//! - [`AppendLogStorage<B, T, C>`] → [`AppendLogStorageTier<T>`]
//! - [`KvStorage<B, T, C>`] → [`KvStorageTier<T>`]
//!
//! Each holds:
//! - `backend: Arc<B>` (shared, multi-tier-share-one-backend supported per D147)
//! - `codec: C` (default `JsonCodec` per D148)
//! - `name: String`, `debounce_ms`, `compact_every` — cadence knobs
//! - `filter`, `key_of` — optional closures (boxed `dyn Fn` per D149)
//! - Internal pending buffer (`parking_lot::Mutex`'d)
//!
//! Convenience factories `memory_snapshot()` / `memory_append_log()` /
//! `memory_kv()` wrap a fresh in-process backend.

use std::sync::Arc;

use parking_lot::Mutex;
use serde::{de::DeserializeOwned, Serialize};

use crate::backend::{memory_backend, MemoryBackend, StorageBackend};
use crate::codec::{Codec, JsonCodec};
use crate::error::StorageError;
use crate::tier::{
    AppendLogStorageTier, BaseStorageTier, KvStorageTier, PrefixIter, SnapshotStorageTier,
};

type FilterFn<T> = Box<dyn Fn(&T) -> bool + Send + Sync>;
type KeyOfFn<T> = Box<dyn Fn(&T) -> String + Send + Sync>;
type KvFilterFn<T> = Box<dyn Fn(&str, &T) -> bool + Send + Sync>;

// ── Snapshot tier ─────────────────────────────────────────────────────────

/// Snapshot tier — buffers one pending snapshot; `flush()` encodes via codec
/// and writes to the backend under `key_of(snapshot)`. Mirrors TS
/// `snapshotStorage(backend, opts)`.
pub struct SnapshotStorage<B, T, C = JsonCodec>
where
    B: StorageBackend + ?Sized,
    T: Send + Sync + 'static,
    C: Codec<T>,
{
    backend: Arc<B>,
    codec: C,
    name: String,
    debounce_ms: Option<u32>,
    compact_every: Option<u32>,
    filter: Option<FilterFn<T>>,
    key_of: KeyOfFn<T>,
    /// Single buffered snapshot pending flush. `Mutex<Option<T>>` rather
    /// than `Mutex<T>` so `rollback` and `flush` can take ownership cheaply.
    pending: Mutex<Option<T>>,
    /// Total `save()` calls accepted (post-filter). Used by `compact_every`.
    write_count: Mutex<u64>,
    /// Last key written to the backend; cached so `load()` can find the
    /// most recent baseline when `key_of` varies per snapshot.
    last_saved_key: Mutex<Option<String>>,
}

/// Options for [`SnapshotStorage`] construction.
pub struct SnapshotStorageOptions<T, C = JsonCodec>
where
    T: Send + Sync + 'static,
    C: Codec<T>,
{
    pub name: Option<String>,
    pub codec: C,
    pub debounce_ms: Option<u32>,
    pub compact_every: Option<u32>,
    pub filter: Option<FilterFn<T>>,
    pub key_of: Option<KeyOfFn<T>>,
}

impl<T> Default for SnapshotStorageOptions<T, JsonCodec>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn default() -> Self {
        Self {
            name: None,
            codec: JsonCodec,
            debounce_ms: None,
            compact_every: None,
            filter: None,
            key_of: None,
        }
    }
}

/// Factory: wrap a backend as a snapshot tier.
///
/// # Panics
///
/// Panics if `opts.compact_every == Some(0)` (use `None` to disable
/// compaction; values ≥ 1 specify the cadence). Pre-1.0 footgun guard
/// per /qa A4.
pub fn snapshot_storage<B, T, C>(
    backend: Arc<B>,
    opts: SnapshotStorageOptions<T, C>,
) -> SnapshotStorage<B, T, C>
where
    B: StorageBackend + ?Sized,
    T: Send + Sync + 'static,
    C: Codec<T>,
{
    assert!(
        opts.compact_every != Some(0),
        "snapshot_storage: compact_every must be None or Some(n) where n >= 1, got Some(0)",
    );
    let name = opts.name.unwrap_or_else(|| backend.name().to_string());
    let fallback_key = name.clone();
    let key_of = opts
        .key_of
        .unwrap_or_else(|| Box::new(move |_| fallback_key.clone()));
    SnapshotStorage {
        backend,
        codec: opts.codec,
        name,
        debounce_ms: opts.debounce_ms,
        compact_every: opts.compact_every,
        filter: opts.filter,
        key_of,
        pending: Mutex::new(None),
        write_count: Mutex::new(0),
        last_saved_key: Mutex::new(None),
    }
}

/// Convenience: snapshot tier over a fresh in-memory backend.
pub fn memory_snapshot<T, C>(
    opts: SnapshotStorageOptions<T, C>,
) -> SnapshotStorage<MemoryBackend, T, C>
where
    T: Send + Sync + 'static,
    C: Codec<T>,
{
    snapshot_storage(memory_backend(), opts)
}

impl<B, T, C> SnapshotStorage<B, T, C>
where
    B: StorageBackend + ?Sized,
    T: Send + Sync + 'static,
    C: Codec<T>,
{
    /// Encode + write a snapshot to the backend, updating `last_saved_key` on
    /// success. Returns `Err((snapshot, error))` on failure so the caller can
    /// restore the snapshot to the pending slot (D165 — F1 fix).
    fn try_flush(
        backend: &B,
        codec: &C,
        key_of: &KeyOfFn<T>,
        last_saved_key: &Mutex<Option<String>>,
        snapshot: T,
    ) -> Result<(), (T, StorageError)> {
        let key = key_of(&snapshot);
        let bytes = match codec.encode(&snapshot) {
            Ok(b) => b,
            Err(e) => return Err((snapshot, e.into())),
        };
        if let Err(e) = backend.write(&key, &bytes) {
            return Err((snapshot, e));
        }
        *last_saved_key.lock() = Some(key);
        Ok(())
    }
}

impl<B, T, C> BaseStorageTier for SnapshotStorage<B, T, C>
where
    B: StorageBackend + ?Sized,
    T: Send + Sync + 'static,
    C: Codec<T>,
{
    fn name(&self) -> &str {
        &self.name
    }
    fn debounce_ms(&self) -> Option<u32> {
        self.debounce_ms
    }
    fn compact_every(&self) -> Option<u32> {
        self.compact_every
    }

    // D165 — F1 fix: take pending, attempt encode+write, restore on failure.
    // Pre-fix: `mem::take` before encode lost pending on encode/write error.
    fn flush(&self) -> Result<(), StorageError> {
        let slot = self.pending.lock().take();
        let Some(snapshot) = slot else {
            return Ok(());
        };
        match Self::try_flush(
            &*self.backend,
            &self.codec,
            &self.key_of,
            &self.last_saved_key,
            snapshot,
        ) {
            Ok(()) => Ok(()),
            Err((snapshot, err)) => {
                // Restore pending so the caller can retry.
                *self.pending.lock() = Some(snapshot);
                Err(err)
            }
        }
    }

    fn rollback(&self) -> Result<(), StorageError> {
        *self.pending.lock() = None;
        Ok(())
    }

    fn list_by_prefix_bytes<'a>(
        &'a self,
        prefix: &str,
    ) -> Box<dyn Iterator<Item = Result<(String, Vec<u8>), StorageError>> + 'a> {
        Box::new(PrefixIter::new(&*self.backend, prefix))
    }

    fn compact(&self) -> Result<(), StorageError> {
        self.flush()
    }
}

impl<B, T, C> SnapshotStorageTier<T> for SnapshotStorage<B, T, C>
where
    B: StorageBackend + ?Sized,
    T: Send + Sync + 'static,
    C: Codec<T>,
{
    fn save(&self, snapshot: T) -> Result<(), StorageError> {
        if let Some(filter) = &self.filter {
            if !filter(&snapshot) {
                return Ok(());
            }
        }
        // /qa F2 + A1 (D138-followup, 2026-05-10): hold `pending` lock across
        // the count update + trigger decision + capture, so the snapshot that
        // triggers a compact cadence is THE snapshot that gets persisted
        // (closes the snapshot-compact-trigger race). Boundary-crossing
        // trigger logic (`prev/N != new/N`) replaces strict `is_multiple_of`
        // so batch save patterns can't skip the trigger when count jumps
        // multiple boundaries — matches TS-side fix.
        let captured: Option<T> = {
            let mut pending = self.pending.lock();
            *pending = Some(snapshot);
            let mut count = self.write_count.lock();
            let prev = *count;
            *count = count.saturating_add(1);
            let new = *count;
            let compact_trigger = matches!(
                self.compact_every,
                Some(n) if n > 0 && (prev / u64::from(n)) != (new / u64::from(n))
            );
            let trigger = compact_trigger || self.debounce_ms.is_none();
            if trigger {
                pending.take()
            } else {
                None
            }
        };
        if let Some(snap) = captured {
            // D165 — F1 fix: restore pending on encode/write failure.
            if let Err((snap, err)) = Self::try_flush(
                &self.backend,
                &self.codec,
                &self.key_of,
                &self.last_saved_key,
                snap,
            ) {
                *self.pending.lock() = Some(snap);
                return Err(err);
            }
        }
        Ok(())
    }

    fn load(&self) -> Result<Option<T>, StorageError> {
        let key = self
            .last_saved_key
            .lock()
            .clone()
            .unwrap_or_else(|| self.name.clone());
        match self.backend.read(&key)? {
            Some(bytes) if !bytes.is_empty() => Ok(Some(self.codec.decode(&bytes)?)),
            _ => Ok(None),
        }
    }
}

// ── Append-log tier ───────────────────────────────────────────────────────

/// Append-log tier — buffers per-key entries; `flush()` encodes each
/// bucket as an array via codec and merge-writes it into the backend.
/// Mirrors TS `appendLogStorage`.
pub struct AppendLogStorage<B, T, C = JsonCodec>
where
    B: StorageBackend + ?Sized,
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    C: Codec<Vec<T>>,
{
    backend: Arc<B>,
    codec: C,
    name: String,
    debounce_ms: Option<u32>,
    compact_every: Option<u32>,
    key_of: KeyOfFn<T>,
    /// Per-key pending buckets (matches TS `Map<string, T[]>`).
    pending: Mutex<std::collections::HashMap<String, Vec<T>>>,
    /// Total entries appended (post-filter); drives `compact_every`.
    append_count: Mutex<u64>,
}

pub struct AppendLogStorageOptions<T, C = JsonCodec>
where
    T: Send + Sync + 'static,
    C: Codec<Vec<T>>,
{
    pub name: Option<String>,
    pub codec: C,
    pub debounce_ms: Option<u32>,
    pub compact_every: Option<u32>,
    pub key_of: Option<KeyOfFn<T>>,
}

impl<T> Default for AppendLogStorageOptions<T, JsonCodec>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn default() -> Self {
        Self {
            name: None,
            codec: JsonCodec,
            debounce_ms: None,
            compact_every: None,
            key_of: None,
        }
    }
}

/// Factory: wrap a backend as an append-log tier.
///
/// # Panics
///
/// Panics if `opts.compact_every == Some(0)`. See [`snapshot_storage`] for
/// the rationale (pre-1.0 footgun guard per /qa A4).
pub fn append_log_storage<B, T, C>(
    backend: Arc<B>,
    opts: AppendLogStorageOptions<T, C>,
) -> AppendLogStorage<B, T, C>
where
    B: StorageBackend + ?Sized,
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    C: Codec<Vec<T>>,
{
    assert!(
        opts.compact_every != Some(0),
        "append_log_storage: compact_every must be None or Some(n) where n >= 1, got Some(0)",
    );
    let name = opts.name.unwrap_or_else(|| backend.name().to_string());
    let fallback_key = name.clone();
    let key_of = opts
        .key_of
        .unwrap_or_else(|| Box::new(move |_| fallback_key.clone()));
    AppendLogStorage {
        backend,
        codec: opts.codec,
        name,
        debounce_ms: opts.debounce_ms,
        compact_every: opts.compact_every,
        key_of,
        pending: Mutex::new(std::collections::HashMap::new()),
        append_count: Mutex::new(0),
    }
}

pub fn memory_append_log<T, C>(
    opts: AppendLogStorageOptions<T, C>,
) -> AppendLogStorage<MemoryBackend, T, C>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    C: Codec<Vec<T>>,
{
    append_log_storage(memory_backend(), opts)
}

impl<B, T, C> BaseStorageTier for AppendLogStorage<B, T, C>
where
    B: StorageBackend + ?Sized,
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    C: Codec<Vec<T>>,
{
    fn name(&self) -> &str {
        &self.name
    }
    fn debounce_ms(&self) -> Option<u32> {
        self.debounce_ms
    }
    fn compact_every(&self) -> Option<u32> {
        self.compact_every
    }

    // D165 — F1 fix: take pending, attempt per-bucket encode+write, restore
    // unprocessed buckets on failure so the caller can retry.
    fn flush(&self) -> Result<(), StorageError> {
        let mut buckets = std::mem::take(&mut *self.pending.lock());
        let keys: Vec<String> = buckets.keys().cloned().collect();
        for key in keys {
            let bucket = match buckets.remove(&key) {
                Some(b) if !b.is_empty() => b,
                _ => continue,
            };
            // Read existing, merge, write back.
            let existing = match self.backend.read(&key) {
                Ok(e) => e,
                Err(e) => {
                    buckets.insert(key, bucket);
                    *self.pending.lock() = buckets;
                    return Err(e);
                }
            };
            let mut merged = match existing {
                Some(bytes) if !bytes.is_empty() => match self.codec.decode(&bytes) {
                    Ok(v) => v,
                    Err(e) => {
                        buckets.insert(key, bucket);
                        *self.pending.lock() = buckets;
                        return Err(e.into());
                    }
                },
                _ => Vec::new(),
            };
            let bucket_backup = bucket.clone();
            merged.extend(bucket);
            let encoded = match self.codec.encode(&merged) {
                Ok(b) => b,
                Err(e) => {
                    buckets.insert(key, bucket_backup);
                    *self.pending.lock() = buckets;
                    return Err(e.into());
                }
            };
            if let Err(e) = self.backend.write(&key, &encoded) {
                *self.pending.lock() = buckets;
                return Err(e);
            }
        }
        Ok(())
    }

    fn rollback(&self) -> Result<(), StorageError> {
        self.pending.lock().clear();
        Ok(())
    }

    fn list_by_prefix_bytes<'a>(
        &'a self,
        prefix: &str,
    ) -> Box<dyn Iterator<Item = Result<(String, Vec<u8>), StorageError>> + 'a> {
        Box::new(PrefixIter::new(&*self.backend, prefix))
    }
}

impl<B, T, C> AppendLogStorageTier<T> for AppendLogStorage<B, T, C>
where
    B: StorageBackend + ?Sized,
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    C: Codec<Vec<T>>,
{
    fn append_entries(&self, entries: &[T]) -> Result<(), StorageError> {
        if entries.is_empty() {
            return Ok(());
        }
        // /qa F2 (D138-followup, 2026-05-10): boundary-crossing trigger logic
        // — a batch that jumps multiple compact_every boundaries fires one
        // flush (closes the strict-divisibility gap where
        // `append_entries(&[a,b,c,d,e])` with compact_every=3 would miss the
        // boundary at 3). Matches TS-side fix.
        let trigger_now = {
            let mut pending = self.pending.lock();
            for entry in entries {
                let k = (self.key_of)(entry);
                pending.entry(k).or_default().push(entry.clone());
            }
            let mut count = self.append_count.lock();
            let prev = *count;
            *count = count.saturating_add(entries.len() as u64);
            let new = *count;
            let compact_trigger = matches!(
                self.compact_every,
                Some(n) if n > 0 && (prev / u64::from(n)) != (new / u64::from(n))
            );
            compact_trigger || self.debounce_ms.is_none()
        };
        if trigger_now {
            self.flush()?;
        }
        Ok(())
    }

    fn load_entries(&self, key_filter: Option<&str>) -> Result<Vec<T>, StorageError> {
        // Use the backend's enumeration to find keys. If `list` isn't
        // supported, fall back to the tier name as the single key.
        let keys = match self.backend.list(key_filter.unwrap_or("")) {
            Ok(ks) => ks,
            Err(StorageError::BackendNoListSupport { .. }) => match key_filter {
                Some(k) => vec![k.to_string()],
                None => vec![self.name.clone()],
            },
            Err(e) => return Err(e),
        };
        let mut all = Vec::new();
        for k in keys {
            if let Some(bytes) = self.backend.read(&k)? {
                if !bytes.is_empty() {
                    let entries: Vec<T> = self.codec.decode(&bytes)?;
                    all.extend(entries);
                }
            }
        }
        Ok(all)
    }
}

// ── KV tier ───────────────────────────────────────────────────────────────

/// Key-value tier — buffers per-key pending writes; `flush()` encodes each
/// value via codec and writes it to the backend. Mirrors TS `kvStorage`.
pub struct KvStorage<B, T, C = JsonCodec>
where
    B: StorageBackend + ?Sized,
    T: Send + Sync + 'static,
    C: Codec<T>,
{
    backend: Arc<B>,
    codec: C,
    name: String,
    debounce_ms: Option<u32>,
    compact_every: Option<u32>,
    filter: Option<KvFilterFn<T>>,
    pending: Mutex<std::collections::HashMap<String, T>>,
    write_count: Mutex<u64>,
}

pub struct KvStorageOptions<T, C = JsonCodec>
where
    T: Send + Sync + 'static,
    C: Codec<T>,
{
    pub name: Option<String>,
    pub codec: C,
    pub debounce_ms: Option<u32>,
    pub compact_every: Option<u32>,
    pub filter: Option<KvFilterFn<T>>,
}

impl<T> Default for KvStorageOptions<T, JsonCodec>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn default() -> Self {
        Self {
            name: None,
            codec: JsonCodec,
            debounce_ms: None,
            compact_every: None,
            filter: None,
        }
    }
}

/// Factory: wrap a backend as a kv tier.
///
/// # Panics
///
/// Panics if `opts.compact_every == Some(0)`. See [`snapshot_storage`] for
/// the rationale (pre-1.0 footgun guard per /qa A4).
pub fn kv_storage<B, T, C>(backend: Arc<B>, opts: KvStorageOptions<T, C>) -> KvStorage<B, T, C>
where
    B: StorageBackend + ?Sized,
    T: Send + Sync + 'static,
    C: Codec<T>,
{
    assert!(
        opts.compact_every != Some(0),
        "kv_storage: compact_every must be None or Some(n) where n >= 1, got Some(0)",
    );
    let name = opts.name.unwrap_or_else(|| backend.name().to_string());
    KvStorage {
        backend,
        codec: opts.codec,
        name,
        debounce_ms: opts.debounce_ms,
        compact_every: opts.compact_every,
        filter: opts.filter,
        pending: Mutex::new(std::collections::HashMap::new()),
        write_count: Mutex::new(0),
    }
}

pub fn memory_kv<T, C>(opts: KvStorageOptions<T, C>) -> KvStorage<MemoryBackend, T, C>
where
    T: Send + Sync + 'static,
    C: Codec<T>,
{
    kv_storage(memory_backend(), opts)
}

impl<B, T, C> BaseStorageTier for KvStorage<B, T, C>
where
    B: StorageBackend + ?Sized,
    T: Send + Sync + 'static,
    C: Codec<T>,
{
    fn name(&self) -> &str {
        &self.name
    }
    fn debounce_ms(&self) -> Option<u32> {
        self.debounce_ms
    }
    fn compact_every(&self) -> Option<u32> {
        self.compact_every
    }

    // D165 — F1 fix: take pending, attempt per-entry encode+write, restore
    // unprocessed entries on failure so the caller can retry.
    fn flush(&self) -> Result<(), StorageError> {
        let mut entries = std::mem::take(&mut *self.pending.lock());
        let keys: Vec<String> = entries.keys().cloned().collect();
        for key in keys {
            let Some(value) = entries.remove(&key) else {
                continue;
            };
            let bytes = match self.codec.encode(&value) {
                Ok(b) => b,
                Err(e) => {
                    entries.insert(key, value);
                    *self.pending.lock() = entries;
                    return Err(e.into());
                }
            };
            if let Err(e) = self.backend.write(&key, &bytes) {
                entries.insert(key, value);
                *self.pending.lock() = entries;
                return Err(e);
            }
        }
        Ok(())
    }

    fn rollback(&self) -> Result<(), StorageError> {
        self.pending.lock().clear();
        Ok(())
    }

    fn list_by_prefix_bytes<'a>(
        &'a self,
        prefix: &str,
    ) -> Box<dyn Iterator<Item = Result<(String, Vec<u8>), StorageError>> + 'a> {
        Box::new(PrefixIter::new(&*self.backend, prefix))
    }
}

impl<B, T, C> KvStorageTier<T> for KvStorage<B, T, C>
where
    B: StorageBackend + ?Sized,
    T: Send + Sync + 'static,
    C: Codec<T>,
{
    fn save(&self, key: &str, value: T) -> Result<(), StorageError> {
        if let Some(filter) = &self.filter {
            if !filter(key, &value) {
                return Ok(());
            }
        }
        // /qa F2 (D138-followup, 2026-05-10): boundary-crossing trigger logic
        // — matches the Snapshot/AppendLog fix above. A batch of saves that
        // jumps a compact_every boundary fires one flush.
        let trigger_now = {
            self.pending.lock().insert(key.to_string(), value);
            let mut count = self.write_count.lock();
            let prev = *count;
            *count = count.saturating_add(1);
            let new = *count;
            let compact_trigger = matches!(
                self.compact_every,
                Some(n) if n > 0 && (prev / u64::from(n)) != (new / u64::from(n))
            );
            compact_trigger || self.debounce_ms.is_none()
        };
        if trigger_now {
            self.flush()?;
        }
        Ok(())
    }

    fn load(&self, key: &str) -> Result<Option<T>, StorageError> {
        match self.backend.read(key)? {
            Some(bytes) if !bytes.is_empty() => Ok(Some(self.codec.decode(&bytes)?)),
            _ => Ok(None),
        }
    }

    fn delete(&self, key: &str) -> Result<(), StorageError> {
        // /qa A2 (2026-05-10): backend.delete fires FIRST so a failure leaves
        // pending intact (caller can retry). Pre-fix had pending cleared
        // before backend.delete, meaning a backend.delete failure left the
        // backend with stale data + pending empty — silent data divergence
        // visible only on next `load(key)`.
        self.backend.delete(key)?;
        self.pending.lock().remove(key);
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        self.backend.list(prefix)
    }
}
