//! Tier abstraction layer (Phase 14.6 — DS-14-storage Audit 4 + Q5 + Q8
//! locks, M4.B 2026-05-10).
//!
//! Three-layer tier model (matching `packages/pure-ts/src/extra/storage/tiers.ts`):
//!
//! - **Layer 1** — [`StorageBackend`](crate::backend::StorageBackend): generic
//!   bytes-level kv I/O.
//! - **Layer 2** — tier specializations parametric over `T`:
//!   - [`SnapshotStorageTier<T>`] — one record per `save(snapshot)` call.
//!   - [`AppendLogStorageTier<T>`] — sequential entries with optional
//!     partitioning via `key_of`.
//!   - [`KvStorageTier<T>`] — many records, addressable by string key.
//! - **Layer 3** — high-level wiring (`Graph::attach_storage`, M4.E
//!   integration).
//!
//! All sub-traits inherit [`BaseStorageTier`], which carries the cadence
//! knobs (`debounce_ms`, `compact_every`), transaction lifecycle (`flush`,
//! `rollback`), and the dyn-safe bytes-level
//! [`BaseStorageTier::list_by_prefix_bytes`] enumeration. Typed enumeration
//! (decoding via the tier's codec) lives on the typed sub-traits as free
//! functions or default-impl helpers.
//!
//! # Sync, NOT async (D143)
//!
//! Every method returns directly (no `Future`). Memory / redb / `std::fs`
//! backends are all sync-compatible; tokio-backed networking backends wrap
//! their async surface at the adapter layer (e.g. `tokio::Handle::block_on`).
//! See [`crate::backend`] module doc.
//!
//! # `debounce_ms` semantics at M4.B (D144 — option (b))
//!
//! The accessor [`BaseStorageTier::debounce_ms`] returns the configured
//! window. **The tier itself does NOT drive a timer.** The Graph layer
//! consumes this value at attach time (M4.E) and schedules `flush()` via its
//! own reactive timer source (`from_timer` / `from_cron`). Until then,
//! `save` always buffers and `flush` always commits — debounce has no
//! automatic effect.

use crate::backend::StorageBackend;
use crate::error::StorageError;

/// Boxed lazy iterator yielded by [`BaseStorageTier::list_by_prefix_bytes`].
/// Each item is either a `(key, bytes)` entry decoded from the backend or a
/// surfaced [`StorageError`] (e.g. on first-yield if the backend doesn't
/// support enumeration — lazy-throw semantics from the TS impl).
pub type ListByPrefixIter<'a> =
    Box<dyn Iterator<Item = Result<(String, Vec<u8>), StorageError>> + 'a>;

// ── Layer 2 — BaseStorageTier (cadence + transaction surface) ─────────────

/// Common tier surface — cadence knobs + transaction lifecycle + bytes-level
/// enumeration.
///
/// Implements Audit 4's "one wave = one transaction" model:
/// - After every wave (and on `batch()` close), Graph iterates attached tiers
///   and calls `tier.flush()`.
/// - On wave-throw, Graph calls `tier.rollback()` to discard pending writes.
/// - Cross-tier atomicity is best-effort; each tier owns its own transaction.
///
/// Lock 4.D / 6.E defaults:
/// - `debounce_ms = None` — sync-through flush at wave close.
/// - `compact_every = None` — no forced flush cap.
pub trait BaseStorageTier: Send + Sync {
    /// Diagnostic tier name (e.g. `"snapshot:my-graph"`). Surfaces in error
    /// messages and `Display` impls.
    fn name(&self) -> &str;

    /// Debounce window in milliseconds. `None` (default) = sync-through.
    /// Graph-layer attach reads this and schedules timed `flush()` via its
    /// own reactive timer (M4.E); the tier itself does NOT drive a timer.
    fn debounce_ms(&self) -> Option<u32> {
        None
    }

    /// Force a flush every Nth accepted write regardless of debounce.
    /// `None` = no cap; `Some(N)` triggers `flush()` on the Nth `save` (or
    /// `append_entries` for log tiers).
    fn compact_every(&self) -> Option<u32> {
        None
    }

    /// Commit pending writes. Graph calls at wave-close / debounce-fire.
    fn flush(&self) -> Result<(), StorageError>;

    /// Discard pending writes. Graph calls on wave-throw.
    fn rollback(&self) -> Result<(), StorageError>;

    /// Lazily enumerate raw `(key, bytes)` pairs under a literal byte-prefix
    /// (DS-14-storage Q5 lock).
    ///
    /// Yields in lex-ASC key order; for the canonical WAL key format
    /// `${prefix}/${frame_seq:020}`, lex-ASC string sort = numeric ASC
    /// `frame_seq` sort.
    ///
    /// Typed enumeration (decode via the tier's codec) lives on the typed
    /// sub-traits as free helpers — see [`crate::wal::iterate_wal_frames`]
    /// (M4.E) for the WAL-aware pattern. Decoupling the enumeration from
    /// the codec keeps this method dyn-safe (avoids a generic method on the
    /// trait).
    ///
    /// Backends without `list` support surface
    /// [`StorageError::BackendNoListSupport`] on first iteration.
    fn list_by_prefix_bytes<'a>(&'a self, prefix: &str) -> ListByPrefixIter<'a>;

    /// Force a `mode:"full"` baseline immediately (Q8 lock). Bypasses
    /// `compact_every` cadence; useful at deploy boundaries, end-of-process
    /// drains, or test fixtures. Default impl is a `flush()` so tiers that
    /// don't write baselines can still implement the method trivially.
    fn compact(&self) -> Result<(), StorageError> {
        self.flush()
    }
}

// ── Layer 2 — Snapshot tier ───────────────────────────────────────────────

/// Snapshot tier — writes a single record per `save(snapshot)` call. Mirrors
/// TS `SnapshotStorageTier<T>`.
///
/// Backend key is determined by the tier's `key_of` closure (default: a
/// constant `tier.name`). Successive saves overwrite the same key.
pub trait SnapshotStorageTier<T>: BaseStorageTier
where
    T: Send + Sync + 'static,
{
    /// Buffer a snapshot pending flush. Honors `compact_every` and
    /// `debounce_ms` semantics per the [`BaseStorageTier`] contract.
    fn save(&self, snapshot: T) -> Result<(), StorageError>;

    /// Load the most-recently-saved snapshot. Returns `Ok(None)` if no
    /// snapshot has been persisted yet.
    fn load(&self) -> Result<Option<T>, StorageError>;
}

// ── Layer 2 — Append-log tier ─────────────────────────────────────────────

/// **D269 — Persistence mode (memo:Re P1 parity).** Mirrors TS
/// `AppendLogStorageOptions.mode`. `Append` (default) reads existing
/// bucket bytes, decodes, merges new entries, encodes, writes back —
/// the M4.B behavior. `Overwrite` skips the read/merge entirely and
/// snapshots the current batch as the bucket's full contents. Used
/// for callers that ship full snapshots per wave (e.g. WAL replay
/// drivers) rather than deltas. Feeding deltas into an `Overwrite`
/// tier silently truncates the log to the last batch — `attach_storage`
/// rejects overwrite sinks at attachment time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AppendLogMode {
    /// Read existing bucket, merge new entries, write back. M4.B default.
    #[default]
    Append,
    /// Replace bucket contents with the current batch (no read-merge).
    Overwrite,
}

/// **D269 — Opaque cursor for windowed `load_entries` pagination
/// (memo:Re loadEntries-pagination parity).** Mirrors TS `AppendCursor`.
/// `position` is a forward-only offset into the flattened, lex-ASC-by-
/// key, entry-order-within-key sequence. `tag` is reserved for future
/// stable-iteration tokens; currently always `None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AppendCursor {
    pub position: u64,
    pub tag: Option<u64>,
}

impl AppendCursor {
    #[must_use]
    pub const fn from_position(position: u64) -> Self {
        Self {
            position,
            tag: None,
        }
    }
}

/// **D269** — Options for [`AppendLogStorageTier::load_entries`].
#[derive(Debug, Clone, Default)]
pub struct LoadEntriesOpts<'a> {
    pub cursor: Option<AppendCursor>,
    pub page_size: Option<u32>,
    pub key_filter: Option<&'a str>,
}

/// **D269** — Result of a paginated [`AppendLogStorageTier::load_entries`].
/// `cursor.is_none()` ⇒ no more entries (consumer should stop). Returning
/// the whole tail with `cursor.is_none()` matches the back-compat shape
/// (bare `load_entries(LoadEntriesOpts::default())` is byte-for-byte the
/// pre-D269 behavior).
#[must_use = "AppendLoadResult.cursor must be threaded into the next load_entries call; ignoring it voids the pagination contract"]
#[derive(Debug, Clone)]
pub struct AppendLoadResult<T> {
    pub entries: Vec<T>,
    pub cursor: Option<AppendCursor>,
}

/// Append-log tier — bulk-friendly entry persistence with optional
/// partitioning via `key_of`. Mirrors TS `AppendLogStorageTier<T>`.
///
/// Storage shape: each backend key holds a serialized array of all entries
/// for that partition, growing on every flush. Adapters that need true
/// append-only semantics layer their own tier over the same backend.
pub trait AppendLogStorageTier<T>: BaseStorageTier
where
    T: Send + Sync + 'static,
{
    /// Append entries to the per-key buckets (each bucket determined by the
    /// tier's `key_of` closure). Honors `compact_every` cadence.
    fn append_entries(&self, entries: &[T]) -> Result<(), StorageError>;

    /// D269: persistence mode (`Append` default — read-merge; `Overwrite` —
    /// replace bucket per flush). Exposed so delta-shipping consumers like
    /// `ReactiveLog::attach_storage` can reject `Overwrite` tiers at
    /// attach time.
    fn mode(&self) -> AppendLogMode {
        AppendLogMode::Append
    }

    /// D269 — windowed cursor pagination (memo:Re loadEntries-pagination
    /// parity). With `LoadEntriesOpts::default()` returns the whole log
    /// (back-compat shape) and `cursor: None`. With `page_size = Some(n)`
    /// returns `[start, start+n)` of the flattened (lex-ASC-by-key,
    /// entry-order-within-key) sequence and a forward-only cursor
    /// (`None` ⇒ no more). Pre-D269 callers using
    /// `load_entries_legacy(key_filter)` get the old signature.
    fn load_entries(&self, opts: LoadEntriesOpts<'_>) -> Result<AppendLoadResult<T>, StorageError>;

    /// Pre-D269 convenience: load all entries (no pagination, no cursor).
    /// Equivalent to `load_entries(LoadEntriesOpts { key_filter, .. })`.
    fn load_entries_all(&self, key_filter: Option<&str>) -> Result<Vec<T>, StorageError> {
        Ok(self
            .load_entries(LoadEntriesOpts {
                cursor: None,
                page_size: None,
                key_filter,
            })?
            .entries)
    }
}

// ── Layer 2 — KV tier ─────────────────────────────────────────────────────

/// Key-value tier — typed records under arbitrary string keys with codec
/// serialization at the storage boundary. Mirrors TS `KvStorageTier<T>`.
///
/// Use for content-addressed caches (replay), multi-record archives
/// (snapshot index, AI memory), and fixture stores. Snapshot is "one
/// record"; append-log is "sequential entries"; kv is "many records,
/// addressable by key".
pub trait KvStorageTier<T>: BaseStorageTier
where
    T: Send + Sync + 'static,
{
    /// Buffer a key→value mapping pending flush. Repeated `save(k, _)`
    /// before flush overwrites the buffered value for that key. Honors
    /// `compact_every` cadence.
    fn save(&self, key: &str, value: T) -> Result<(), StorageError>;

    /// Read the value at `key`. Returns `Ok(None)` on miss.
    fn load(&self, key: &str) -> Result<Option<T>, StorageError>;

    /// Delete the value at `key`. Flushes through to the backend
    /// immediately (matches TS `delete` semantics — no debounce).
    fn delete(&self, key: &str) -> Result<(), StorageError>;

    /// Enumerate keys under `prefix` (lex-ASC). Delegates to
    /// [`StorageBackend::list`] via the tier's backend.
    fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError>;
}

// ── Shared bytes-level prefix-iter helper ─────────────────────────────────

/// Iterator yielded by [`BaseStorageTier::list_by_prefix_bytes`] for tiers
/// backed by a generic `StorageBackend`. Reads the key list eagerly (the
/// backend's `list` is sync); reads + yields each entry lazily as the
/// consumer pulls.
///
/// The eager-list / lazy-read split matches the TS impl behavior: the
/// backend may have to walk a directory or query an index to produce keys
/// (one round-trip); reading the bytes per key is the dominant cost and
/// should stay lazy.
pub(crate) struct PrefixIter<'a, B: StorageBackend + ?Sized> {
    backend: &'a B,
    keys: std::vec::IntoIter<String>,
    /// First-yield error (e.g. backend doesn't support list). Stored on
    /// construction so the consumer sees it on the first `next()` call —
    /// mirrors TS lazy-throw semantics for `backend-no-list-support`.
    pending_error: Option<StorageError>,
}

impl<'a, B: StorageBackend + ?Sized> PrefixIter<'a, B> {
    pub(crate) fn new(backend: &'a B, prefix: &str) -> Self {
        match backend.list(prefix) {
            Ok(mut keys) => {
                // Filter to literal byte-prefix matches (defensive — some
                // backends may return overly-wide lists) and sort lex-ASC.
                keys.retain(|k| k.starts_with(prefix));
                keys.sort();
                Self {
                    backend,
                    keys: keys.into_iter(),
                    pending_error: None,
                }
            }
            Err(e) => Self {
                backend,
                keys: Vec::new().into_iter(),
                pending_error: Some(e),
            },
        }
    }
}

impl<B: StorageBackend + ?Sized> Iterator for PrefixIter<'_, B> {
    type Item = Result<(String, Vec<u8>), StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(e) = self.pending_error.take() {
            return Some(Err(e));
        }
        loop {
            let key = self.keys.next()?;
            match self.backend.read(&key) {
                Ok(Some(bytes)) if !bytes.is_empty() => return Some(Ok((key, bytes))),
                // Empty / absent entries are skipped (the key was listed but
                // is no longer readable — race with a concurrent delete, or
                // a backend that holds empty placeholders).
                Ok(_) => {}
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

// Cross-tier helper traits (e.g. `HasBackend` / `HasCodec` for typed
// list-by-prefix decoders) land in M4.E when the Graph integration actually
// consumes them — keeping speculative infrastructure out of M4.B.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::MemoryBackend;

    /// Compile-time check: all four tier traits are object-safe so callers
    /// can store heterogeneous tiers in `Vec<Box<dyn ...>>`.
    #[test]
    fn tier_traits_are_dyn_safe() {
        fn assert_dyn<T: ?Sized>() {}
        assert_dyn::<dyn BaseStorageTier>();
        assert_dyn::<dyn SnapshotStorageTier<u64>>();
        assert_dyn::<dyn AppendLogStorageTier<u64>>();
        assert_dyn::<dyn KvStorageTier<u64>>();
    }

    #[test]
    fn prefix_iter_yields_lex_asc() {
        let b = MemoryBackend::new();
        b.write("g/02", b"two").unwrap();
        b.write("g/01", b"one").unwrap();
        b.write("g/10", b"ten").unwrap();
        b.write("other", b"x").unwrap();
        let iter = PrefixIter::new(&b, "g/");
        let collected: Vec<_> = iter.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(
            collected,
            vec![
                ("g/01".to_string(), b"one".to_vec()),
                ("g/02".to_string(), b"two".to_vec()),
                ("g/10".to_string(), b"ten".to_vec()),
            ],
        );
    }

    #[test]
    fn prefix_iter_surfaces_backend_no_list_support_lazily() {
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
        // Construction succeeds — error is deferred to first `next()`.
        let mut iter = PrefixIter::new(&b, "g/");
        let first = iter.next();
        assert!(matches!(
            first,
            Some(Err(StorageError::BackendNoListSupport { .. }))
        ));
        // Subsequent `next()` returns `None` (iterator exhausted).
        assert!(iter.next().is_none());
    }
}
