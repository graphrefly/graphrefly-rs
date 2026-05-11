//! napi-rs storage bindings for parity tests (M4.F — D175).
//!
//! Typed napi classes for each tier specialization + graph-integration
//! free functions. All values cross the FFI boundary as JSON strings
//! (`serde_json::to_string` / `serde_json::from_str`). Memory backends
//! only — file/redb are Rust-internal, not exposed to JS.
//!
//! Arc-wrapper newtypes delegate trait impls so the same underlying tier
//! is shared between the napi class (for inspection in test assertions)
//! and `attach_snapshot_storage` (which takes `Box<dyn Trait>`).

use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi::Error as NapiError;
use napi_derive::napi;
use parking_lot::Mutex;
use serde_json::Value;

use graphrefly_storage::error::{RestoreError, StorageError};
use graphrefly_storage::memory::{
    append_log_storage, kv_storage, snapshot_storage, AppendLogStorage, AppendLogStorageOptions,
    KvStorage, KvStorageOptions, SnapshotStorage, SnapshotStorageOptions,
};
use graphrefly_storage::tier::{
    AppendLogStorageTier, BaseStorageTier, KvStorageTier, SnapshotStorageTier,
};
use graphrefly_storage::wal::{
    verify_wal_frame_checksum, wal_frame_checksum, wal_frame_key, WALFrame, REPLAY_ORDER,
    WAL_FRAME_SEQ_PAD,
};
use graphrefly_storage::{MemoryBackend, StorageBackend};

// ── Memory backend ───────────────────────────────────────────────────────

/// In-memory storage backend shared across tiers.
#[napi]
pub struct BenchMemoryBackend {
    pub(crate) inner: Arc<MemoryBackend>,
}

#[napi]
impl BenchMemoryBackend {
    #[napi(constructor)]
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: graphrefly_storage::memory_backend(),
        }
    }

    /// Read raw bytes for a key (JSON string or null).
    #[napi]
    pub fn read_raw(&self, key: String) -> Result<Option<String>> {
        match self.inner.read(&key) {
            Ok(Some(bytes)) if !bytes.is_empty() => String::from_utf8(bytes)
                .map(Some)
                .map_err(|e| NapiError::from_reason(format!("non-utf8 bytes for key {key}: {e}"))),
            Ok(_) => Ok(None),
            Err(e) => Err(storage_err(e)),
        }
    }

    /// List all keys with a prefix.
    #[napi]
    pub fn list(&self, prefix: String) -> Result<Vec<String>> {
        self.inner.list(&prefix).map_err(storage_err)
    }
}

// ── Value snapshot tier (Tier 1 — generic value tests) ───────────────────

#[napi]
pub struct BenchValueSnapshotTier {
    inner: Arc<SnapshotStorage<MemoryBackend, Value>>,
}

#[napi]
impl BenchValueSnapshotTier {
    #[napi(factory)]
    pub fn create(
        backend: &BenchMemoryBackend,
        name: Option<String>,
        compact_every: Option<u32>,
        debounce_ms: Option<u32>,
    ) -> Self {
        let opts = SnapshotStorageOptions {
            name,
            debounce_ms,
            compact_every,
            ..Default::default()
        };
        Self {
            inner: Arc::new(snapshot_storage(Arc::clone(&backend.inner), opts)),
        }
    }

    #[napi]
    pub fn save(&self, value_json: String) -> Result<()> {
        let v: Value = parse_json(&value_json)?;
        self.inner.save(v).map_err(storage_err)
    }

    #[napi]
    pub fn load(&self) -> Result<Option<String>> {
        match self.inner.load().map_err(storage_err)? {
            Some(v) => Ok(Some(to_json(&v)?)),
            None => Ok(None),
        }
    }

    #[napi]
    pub fn flush(&self) -> Result<()> {
        BaseStorageTier::flush(&*self.inner).map_err(storage_err)
    }

    #[napi]
    pub fn rollback(&self) -> Result<()> {
        BaseStorageTier::rollback(&*self.inner).map_err(storage_err)
    }

    #[napi(getter)]
    pub fn name(&self) -> String {
        BaseStorageTier::name(&*self.inner).to_string()
    }

    #[napi(getter)]
    pub fn debounce_ms(&self) -> Option<u32> {
        BaseStorageTier::debounce_ms(&*self.inner)
    }

    #[napi(getter)]
    pub fn compact_every(&self) -> Option<u32> {
        BaseStorageTier::compact_every(&*self.inner)
    }
}

// ── Value KV tier (Tier 1 — generic value tests) ─────────────────────────

#[napi]
pub struct BenchValueKvTier {
    inner: Arc<KvStorage<MemoryBackend, Value>>,
}

#[napi]
impl BenchValueKvTier {
    #[napi(factory)]
    pub fn create(
        backend: &BenchMemoryBackend,
        name: Option<String>,
        compact_every: Option<u32>,
        debounce_ms: Option<u32>,
    ) -> Self {
        let opts = KvStorageOptions {
            name,
            debounce_ms,
            compact_every,
            ..Default::default()
        };
        Self {
            inner: Arc::new(kv_storage(Arc::clone(&backend.inner), opts)),
        }
    }

    #[napi]
    pub fn save(&self, key: String, value_json: String) -> Result<()> {
        let v: Value = parse_json(&value_json)?;
        self.inner.save(&key, v).map_err(storage_err)
    }

    #[napi]
    pub fn load(&self, key: String) -> Result<Option<String>> {
        match self.inner.load(&key).map_err(storage_err)? {
            Some(v) => Ok(Some(to_json(&v)?)),
            None => Ok(None),
        }
    }

    #[napi]
    pub fn delete(&self, key: String) -> Result<()> {
        self.inner.delete(&key).map_err(storage_err)
    }

    #[napi]
    pub fn list(&self, prefix: String) -> Result<Vec<String>> {
        KvStorageTier::list(&*self.inner, &prefix).map_err(storage_err)
    }

    #[napi]
    pub fn flush(&self) -> Result<()> {
        BaseStorageTier::flush(&*self.inner).map_err(storage_err)
    }

    #[napi]
    pub fn rollback(&self) -> Result<()> {
        BaseStorageTier::rollback(&*self.inner).map_err(storage_err)
    }

    #[napi(getter)]
    pub fn name(&self) -> String {
        BaseStorageTier::name(&*self.inner).to_string()
    }
}

// ── Value append-log tier (Tier 1 — generic value tests) ─────────────────

#[napi]
pub struct BenchValueAppendLogTier {
    inner: Arc<AppendLogStorage<MemoryBackend, Value>>,
}

#[napi]
impl BenchValueAppendLogTier {
    #[napi(factory)]
    pub fn create(
        backend: &BenchMemoryBackend,
        name: Option<String>,
        compact_every: Option<u32>,
        debounce_ms: Option<u32>,
    ) -> Self {
        let opts = AppendLogStorageOptions {
            name,
            debounce_ms,
            compact_every,
            ..Default::default()
        };
        Self {
            inner: Arc::new(append_log_storage(Arc::clone(&backend.inner), opts)),
        }
    }

    /// Append entries (JSON array).
    #[napi]
    pub fn append_entries(&self, entries_json: String) -> Result<()> {
        let entries: Vec<Value> = parse_json(&entries_json)?;
        self.inner.append_entries(&entries).map_err(storage_err)
    }

    /// Load all entries (returns JSON array).
    #[napi]
    pub fn load_entries(&self, key_filter: Option<String>) -> Result<String> {
        let entries = self
            .inner
            .load_entries(key_filter.as_deref())
            .map_err(storage_err)?;
        to_json(&entries)
    }

    #[napi]
    pub fn flush(&self) -> Result<()> {
        BaseStorageTier::flush(&*self.inner).map_err(storage_err)
    }

    #[napi]
    pub fn rollback(&self) -> Result<()> {
        BaseStorageTier::rollback(&*self.inner).map_err(storage_err)
    }

    #[napi(getter)]
    pub fn name(&self) -> String {
        BaseStorageTier::name(&*self.inner).to_string()
    }
}

// ── Graph checkpoint snapshot tier (Tier 2+3 — attach/restore) ───────────

use graphrefly_storage::graph_integration::GraphCheckpointRecord;

#[napi]
pub struct BenchCheckpointSnapshotTier {
    pub(crate) inner: Arc<SnapshotStorage<MemoryBackend, GraphCheckpointRecord>>,
}

#[napi]
impl BenchCheckpointSnapshotTier {
    #[napi(factory)]
    pub fn create(
        backend: &BenchMemoryBackend,
        name: Option<String>,
        compact_every: Option<u32>,
    ) -> Self {
        let opts = SnapshotStorageOptions {
            name,
            compact_every,
            ..Default::default()
        };
        Self {
            inner: Arc::new(snapshot_storage(Arc::clone(&backend.inner), opts)),
        }
    }

    /// Save a checkpoint record (JSON).
    #[napi]
    pub fn save(&self, record_json: String) -> Result<()> {
        let record: GraphCheckpointRecord = parse_json(&record_json)?;
        self.inner.save(record).map_err(storage_err)
    }

    /// Load the latest checkpoint record (JSON or null).
    #[napi]
    pub fn load(&self) -> Result<Option<String>> {
        match self.inner.load().map_err(storage_err)? {
            Some(r) => Ok(Some(to_json(&r)?)),
            None => Ok(None),
        }
    }

    #[napi]
    pub fn flush(&self) -> Result<()> {
        BaseStorageTier::flush(&*self.inner).map_err(storage_err)
    }

    #[napi]
    pub fn rollback(&self) -> Result<()> {
        BaseStorageTier::rollback(&*self.inner).map_err(storage_err)
    }

    #[napi(getter)]
    pub fn name(&self) -> String {
        BaseStorageTier::name(&*self.inner).to_string()
    }
}

// ── Graph WAL KV tier (Tier 2+3 — attach/restore) ───────────────────────

#[napi]
pub struct BenchWalKvTier {
    pub(crate) inner: Arc<KvStorage<MemoryBackend, WALFrame<Value>>>,
}

#[napi]
impl BenchWalKvTier {
    #[napi(factory)]
    pub fn create(
        backend: &BenchMemoryBackend,
        name: Option<String>,
        compact_every: Option<u32>,
    ) -> Self {
        let opts = KvStorageOptions {
            name,
            compact_every,
            ..Default::default()
        };
        Self {
            inner: Arc::new(kv_storage(Arc::clone(&backend.inner), opts)),
        }
    }

    /// Save a WAL frame (JSON) under the given key.
    #[napi]
    pub fn save(&self, key: String, frame_json: String) -> Result<()> {
        let frame: WALFrame<Value> = parse_json(&frame_json)?;
        self.inner.save(&key, frame).map_err(storage_err)
    }

    /// Load a WAL frame by key (JSON or null).
    #[napi]
    pub fn load(&self, key: String) -> Result<Option<String>> {
        match self.inner.load(&key).map_err(storage_err)? {
            Some(f) => Ok(Some(to_json(&f)?)),
            None => Ok(None),
        }
    }

    #[napi]
    pub fn delete(&self, key: String) -> Result<()> {
        self.inner.delete(&key).map_err(storage_err)
    }

    /// List keys under a prefix.
    #[napi]
    pub fn list(&self, prefix: String) -> Result<Vec<String>> {
        KvStorageTier::list(&*self.inner, &prefix).map_err(storage_err)
    }

    #[napi]
    pub fn flush(&self) -> Result<()> {
        BaseStorageTier::flush(&*self.inner).map_err(storage_err)
    }

    #[napi]
    pub fn rollback(&self) -> Result<()> {
        BaseStorageTier::rollback(&*self.inner).map_err(storage_err)
    }

    #[napi(getter)]
    pub fn name(&self) -> String {
        BaseStorageTier::name(&*self.inner).to_string()
    }
}

// ── WAL utility free functions ───────────────────────────────────────────

/// Compute the storage key for a WAL frame: `<prefix>/wal/<seq_padded>`.
#[napi]
pub fn bench_wal_frame_key(prefix: String, frame_seq: u32) -> String {
    wal_frame_key(&prefix, u64::from(frame_seq))
}

/// Compute a WAL frame's checksum (SHA-256 hex). Input: WAL frame JSON.
#[napi]
pub fn bench_wal_frame_checksum(frame_json: String) -> Result<String> {
    let frame: WALFrame<Value> = parse_json(&frame_json)?;
    wal_frame_checksum(&frame).map_err(|e| NapiError::from_reason(format!("{e}")))
}

/// Verify a WAL frame's checksum. Returns true if valid.
#[napi]
pub fn bench_verify_wal_frame_checksum(frame_json: String) -> Result<bool> {
    let frame: WALFrame<Value> = parse_json(&frame_json)?;
    verify_wal_frame_checksum(&frame).map_err(|e| NapiError::from_reason(format!("{e}")))
}

/// WAL frame sequence padding width (20).
#[napi]
pub const BENCH_WAL_FRAME_SEQ_PAD: u32 = WAL_FRAME_SEQ_PAD as u32;

/// Lifecycle replay order: `["spec", "data", "ownership"]`.
#[napi]
pub fn bench_replay_order() -> Vec<String> {
    REPLAY_ORDER
        .iter()
        .map(|l| format!("{l:?}").to_lowercase())
        .collect()
}

// ── Graph integration (attach + restore) ─────────────────────────────────
//
// These require the `graph-codec` feature (for BenchGraph).

#[cfg(feature = "graph-codec")]
mod graph_fns {
    use super::*;
    use crate::core_bindings::run_blocking;
    use crate::graph_bindings::BenchGraph;

    use graphrefly_storage::graph_integration::{
        attach_snapshot_storage, restore_snapshot, AttachOptions, AttachTierPair, RestoreOptions,
        StorageHandle,
    };

    // ── Arc-wrapper newtypes for shared trait delegation ──────────────

    struct SharedCheckpointSnapshotTier(Arc<SnapshotStorage<MemoryBackend, GraphCheckpointRecord>>);

    impl BaseStorageTier for SharedCheckpointSnapshotTier {
        fn name(&self) -> &str {
            self.0.name()
        }
        fn debounce_ms(&self) -> Option<u32> {
            self.0.debounce_ms()
        }
        fn compact_every(&self) -> Option<u32> {
            self.0.compact_every()
        }
        fn flush(&self) -> std::result::Result<(), StorageError> {
            self.0.flush()
        }
        fn rollback(&self) -> std::result::Result<(), StorageError> {
            self.0.rollback()
        }
        fn list_by_prefix_bytes<'a>(
            &'a self,
            prefix: &str,
        ) -> Box<dyn Iterator<Item = std::result::Result<(String, Vec<u8>), StorageError>> + 'a>
        {
            self.0.list_by_prefix_bytes(prefix)
        }
    }

    impl SnapshotStorageTier<GraphCheckpointRecord> for SharedCheckpointSnapshotTier {
        fn save(&self, snapshot: GraphCheckpointRecord) -> std::result::Result<(), StorageError> {
            self.0.save(snapshot)
        }
        fn load(&self) -> std::result::Result<Option<GraphCheckpointRecord>, StorageError> {
            self.0.load()
        }
    }

    // Send+Sync: SharedCheckpointSnapshotTier wraps Arc<T> where T: Send+Sync.
    // SAFETY: no unsafe; Arc<T: Send+Sync> is Send+Sync.
    // The compiler derives this automatically.

    struct SharedWalKvTier(Arc<KvStorage<MemoryBackend, WALFrame<Value>>>);

    impl BaseStorageTier for SharedWalKvTier {
        fn name(&self) -> &str {
            self.0.name()
        }
        fn debounce_ms(&self) -> Option<u32> {
            self.0.debounce_ms()
        }
        fn compact_every(&self) -> Option<u32> {
            self.0.compact_every()
        }
        fn flush(&self) -> std::result::Result<(), StorageError> {
            self.0.flush()
        }
        fn rollback(&self) -> std::result::Result<(), StorageError> {
            self.0.rollback()
        }
        fn list_by_prefix_bytes<'a>(
            &'a self,
            prefix: &str,
        ) -> Box<dyn Iterator<Item = std::result::Result<(String, Vec<u8>), StorageError>> + 'a>
        {
            self.0.list_by_prefix_bytes(prefix)
        }
    }

    impl KvStorageTier<WALFrame<Value>> for SharedWalKvTier {
        fn save(&self, key: &str, value: WALFrame<Value>) -> std::result::Result<(), StorageError> {
            self.0.save(key, value)
        }
        fn load(&self, key: &str) -> std::result::Result<Option<WALFrame<Value>>, StorageError> {
            self.0.load(key)
        }
        fn delete(&self, key: &str) -> std::result::Result<(), StorageError> {
            self.0.delete(key)
        }
        fn list(&self, prefix: &str) -> std::result::Result<Vec<String>, StorageError> {
            self.0.list(prefix)
        }
    }

    // ── Storage handle (RAII) ────────────────────────────────────────

    #[napi]
    pub struct BenchStorageHandle {
        inner: Arc<Mutex<Option<StorageHandle>>>,
        core: graphrefly_core::Core,
    }

    #[napi]
    impl BenchStorageHandle {
        /// Explicitly dispose (stop persistence, unsubscribe sinks).
        #[napi]
        pub async fn dispose(&self) -> Result<()> {
            let taken = self.inner.lock().take();
            if let Some(handle) = taken {
                let core = self.core.clone();
                run_blocking(core, move || handle.dispose()).await?;
            }
            Ok(())
        }
    }

    impl Drop for BenchStorageHandle {
        fn drop(&mut self) {
            let taken = self.inner.lock().take();
            if let Some(handle) = taken {
                let _ = spawn_blocking(move || handle.dispose());
            }
        }
    }

    // ── Free functions ───────────────────────────────────────────────

    /// Attach snapshot storage to a graph. Returns a disposable handle.
    #[napi]
    pub async fn bench_attach_snapshot_storage(
        graph: &BenchGraph,
        snapshot_tier: &BenchCheckpointSnapshotTier,
        wal_tier: Option<&BenchWalKvTier>,
    ) -> Result<BenchStorageHandle> {
        let graph_clone = graph.graph.clone();
        let core = graph.core.clone();
        let snap = Arc::clone(&snapshot_tier.inner);
        let wal = wal_tier.map(|t| Arc::clone(&t.inner));

        let handle = run_blocking(core.clone(), move || {
            let pair = AttachTierPair {
                snapshot: Box::new(SharedCheckpointSnapshotTier(snap)),
                wal: wal.map(|w| {
                    Box::new(SharedWalKvTier(w)) as Box<dyn KvStorageTier<WALFrame<Value>>>
                }),
            };
            attach_snapshot_storage(&graph_clone, vec![pair], AttachOptions::default())
        })
        .await?;

        Ok(BenchStorageHandle {
            inner: Arc::new(Mutex::new(Some(handle))),
            core,
        })
    }

    /// Restore graph state from snapshot + WAL tiers.
    /// Returns a JSON-serialized `RestoreResult`.
    #[napi]
    pub async fn bench_restore_snapshot(
        graph: &BenchGraph,
        snapshot_tier: &BenchCheckpointSnapshotTier,
        wal_tier: &BenchWalKvTier,
        target_seq: Option<u32>,
    ) -> Result<String> {
        let graph_clone = graph.graph.clone();
        let core = graph.core.clone();
        let snap = Arc::clone(&snapshot_tier.inner);
        let wal = Arc::clone(&wal_tier.inner);

        let result = run_blocking(core, move || -> std::result::Result<_, RestoreError> {
            let snap_ref = SharedCheckpointSnapshotTier(snap);
            let wal_ref = SharedWalKvTier(wal);
            let opts = RestoreOptions {
                snapshot_tier: &snap_ref,
                wal_tier: &wal_ref,
                target_seq: target_seq.map(u64::from),
                on_torn_write: None,
            };
            restore_snapshot(&graph_clone, &opts)
        })
        .await?
        .map_err(|e| NapiError::from_reason(format!("{e}")))?;

        to_json(&result)
    }

    /// Take a graph snapshot (JSON-serialized `GraphPersistSnapshot`).
    #[napi]
    pub async fn bench_graph_snapshot(graph: &BenchGraph) -> Result<String> {
        let graph_clone = graph.graph.clone();
        let core = graph.core.clone();
        let snap = run_blocking(core, move || graph_clone.snapshot()).await?;
        to_json(&snap)
    }
}

// Re-export graph_fns items so they're visible as top-level napi exports.
#[cfg(feature = "graph-codec")]
#[allow(unused_imports)] // napi registration happens via macro, not Rust `use`.
pub use graph_fns::*;

// ── Helpers ──────────────────────────────────────────────────────────────

fn parse_json<T: serde::de::DeserializeOwned>(s: &str) -> Result<T> {
    serde_json::from_str(s).map_err(|e| NapiError::from_reason(format!("JSON parse error: {e}")))
}

fn to_json<T: serde::Serialize>(v: &T) -> Result<String> {
    serde_json::to_string(v)
        .map_err(|e| NapiError::from_reason(format!("JSON serialization error: {e}")))
}

fn storage_err(e: StorageError) -> NapiError {
    NapiError::from_reason(format!("{e}"))
}
