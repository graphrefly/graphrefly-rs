//! Bytes-level kv backend (Phase 14.6 — DS-14-storage L1, M4.B 2026-05-10).
//!
//! One responsibility: read/write byte ranges under string keys. Tier
//! specializations (`tier.rs`, `memory.rs`) layer typed serialization on top
//! via [`Codec`](crate::codec::Codec).
//!
//! All operations are **sync** (D143 — pre-Q1 lock). Backends that need
//! async I/O (network, `tokio::fs`) wrap their async surface in
//! `tokio::Handle::block_on` at the call site, or expose a sync facade via
//! `spawn_blocking`. The memory backend in this module is fully sync.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::error::StorageError;

/// Bytes-level kv backend. Tiers layer typed serialization on top via
/// [`Codec`](crate::codec::Codec).
pub trait StorageBackend: Send + Sync {
    /// Diagnostic name (e.g. `"memory"`, `"file:./checkpoints"`). Surfaces
    /// in error messages and tier `Display` impls.
    fn name(&self) -> &str;

    /// Read raw bytes; returns `Ok(None)` on miss.
    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError>;

    /// Write raw bytes.
    fn write(&self, key: &str, bytes: &[u8]) -> Result<(), StorageError>;

    /// Optional delete-by-key. Default is no-op so append-only or read-only
    /// backends can stay quiet.
    fn delete(&self, key: &str) -> Result<(), StorageError> {
        let _ = key;
        Ok(())
    }

    /// Enumerate keys matching `prefix` (lex-ASC). Empty `prefix` enumerates
    /// all keys. Default returns `BackendNoListSupport` — backends that don't
    /// support enumeration surface the diagnostic here at first call, NOT at
    /// attach (mirrors TS lazy-throw semantics for `list_by_prefix`).
    fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let _ = prefix;
        Err(StorageError::BackendNoListSupport {
            tier: self.name().to_string(),
        })
    }

    /// Optional drain hook — adapter authors implement when buffering writes.
    /// Default no-op; tier `flush()` does NOT cascade into this by default
    /// (the tier owns its own buffer; backend buffering is a separate concern
    /// the backend author opts into).
    fn flush(&self) -> Result<(), StorageError> {
        Ok(())
    }
}

/// In-memory bytes backend backed by `parking_lot::Mutex<HashMap<String,
/// Vec<u8>>>`. All operations are synchronous and in-process. Useful for
/// tests, hot tiers, and as the default backend for the `memory_*`
/// convenience factories.
///
/// `Default` uses `"memory"` as the diagnostic name; use [`Self::with_name`]
/// to set a different one (the per-tier-name pattern from the TS impl —
/// helps disambiguate multiple in-process tiers in diagnostics).
#[derive(Debug)]
pub struct MemoryBackend {
    name: String,
    data: Mutex<HashMap<String, Vec<u8>>>,
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryBackend {
    #[must_use]
    pub fn new() -> Self {
        Self {
            name: "memory".into(),
            data: Mutex::new(HashMap::new()),
        }
    }

    #[must_use]
    pub fn with_name(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            data: Mutex::new(HashMap::new()),
        }
    }

    /// Diagnostic helper: number of keys currently stored.
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.lock().len()
    }

    /// Diagnostic helper: whether the backend holds any keys.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.data.lock().is_empty()
    }
}

impl StorageBackend for MemoryBackend {
    fn name(&self) -> &str {
        &self.name
    }

    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self.data.lock().get(key).cloned())
    }

    fn write(&self, key: &str, bytes: &[u8]) -> Result<(), StorageError> {
        self.data.lock().insert(key.to_string(), bytes.to_vec());
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<(), StorageError> {
        self.data.lock().remove(key);
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let guard = self.data.lock();
        let mut keys: Vec<String> = if prefix.is_empty() {
            guard.keys().cloned().collect()
        } else {
            guard
                .keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect()
        };
        keys.sort();
        Ok(keys)
    }
}

/// Convenience constructor returning an `Arc<MemoryBackend>`. Use this when
/// sharing a single backend across multiple tiers (the paired
/// `{ snapshot, wal }` pattern from DS-14-storage §a).
#[must_use]
pub fn memory_backend() -> Arc<MemoryBackend> {
    Arc::new(MemoryBackend::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_backend_read_write_round_trip() {
        let b = MemoryBackend::new();
        assert!(b.is_empty());
        b.write("k1", b"hello").unwrap();
        assert_eq!(b.read("k1").unwrap(), Some(b"hello".to_vec()));
        assert_eq!(b.len(), 1);
    }

    #[test]
    fn memory_backend_read_miss_returns_none() {
        let b = MemoryBackend::new();
        assert!(b.read("nope").unwrap().is_none());
    }

    #[test]
    fn memory_backend_delete_removes_key() {
        let b = MemoryBackend::new();
        b.write("k", b"v").unwrap();
        b.delete("k").unwrap();
        assert!(b.read("k").unwrap().is_none());
    }

    #[test]
    fn memory_backend_list_lex_asc() {
        let b = MemoryBackend::new();
        b.write("g/10", b"a").unwrap();
        b.write("g/02", b"b").unwrap();
        b.write("g/01", b"c").unwrap();
        b.write("other", b"d").unwrap();
        let keys = b.list("g/").unwrap();
        assert_eq!(keys, vec!["g/01", "g/02", "g/10"]);
    }

    #[test]
    fn memory_backend_list_empty_prefix_returns_all() {
        let b = MemoryBackend::new();
        b.write("a", b"x").unwrap();
        b.write("b", b"y").unwrap();
        let keys = b.list("").unwrap();
        assert_eq!(keys, vec!["a", "b"]);
    }

    #[test]
    fn memory_backend_with_custom_name() {
        let b = MemoryBackend::with_name("test");
        assert_eq!(b.name(), "test");
    }

    #[test]
    fn memory_backend_factory_returns_shared_arc() {
        let b = memory_backend();
        let b2 = Arc::clone(&b);
        b.write("k", b"v").unwrap();
        assert_eq!(b2.read("k").unwrap(), Some(b"v".to_vec()));
    }

    /// Verify that a non-list-supporting backend returns the expected error
    /// shape. Constructed via a stub backend that doesn't override `list`.
    #[test]
    fn default_list_returns_backend_no_list_support() {
        struct NoList;
        impl StorageBackend for NoList {
            fn name(&self) -> &'static str {
                "no-list"
            }
            fn read(&self, _key: &str) -> Result<Option<Vec<u8>>, StorageError> {
                Ok(None)
            }
            fn write(&self, _k: &str, _b: &[u8]) -> Result<(), StorageError> {
                Ok(())
            }
        }
        let b = NoList;
        let r = b.list("g/");
        assert!(matches!(r, Err(StorageError::BackendNoListSupport { .. })));
    }
}
