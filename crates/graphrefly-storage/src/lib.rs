//! `GraphReFly` storage tier dispatch + Node-side persistence.
//!
//! Implements the G.27 storage tier protocol: tiered N-way storage with
//! per-tier transactions, debouncing, compaction, and codec
//! parameterization. Phase 13.6's deferred ACID atomicity tightening
//! lands here via [`redb`](https://docs.rs/redb), which provides
//! pure-Rust ACID transactions without a C dependency.
//!
//! # Status
//!
//! M4 complete: tier dispatch, WAL, codecs, memory + file backends,
//! `attach_storage` reactive flush driver, durability/error/rollback
//! contracts (D268–D270). See `docs/migration-status.md` and
//! `docs/porting-deferred.md` for remaining storage follow-ons.
//!
//! Key modules: [`wal`], [`tier`], [`memory`], [`file`] (feature `file`),
//! [`redb`] (feature `redb-store`), [`graph_integration`] (snapshot attach /
//! restore — consumed from `graphrefly-graph`).

#![warn(rust_2018_idioms, unreachable_pub)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::missing_errors_doc)]
#![forbid(unsafe_code)]

pub mod backend;
pub mod codec;
pub mod error;
#[cfg(feature = "file")]
pub mod file;
pub mod graph_integration;
pub mod memory;
#[cfg(feature = "redb-store")]
pub mod redb;
pub mod tier;
pub mod wal;

pub use backend::{memory_backend, MemoryBackend, StorageBackend};
pub use codec::{Codec, CodecError, JsonCodec};
pub use error::{PhaseStat, RestoreError, RestoreResult, StorageError};
#[cfg(feature = "file")]
pub use file::{
    file_append_log, file_append_log_default, file_backend, file_kv, file_kv_default,
    file_snapshot, file_snapshot_default, FileBackend,
};
pub use graph_integration::{
    attach_snapshot_storage, decompose_diff_to_frames, diff_snapshots, restore_snapshot,
    AttachOptions, AttachTierPair, GraphCheckpointRecord, GraphSnapshotDiff, RestoreOptions,
    StorageHandle, TornWritePolicy, ValueChange, SNAPSHOT_VERSION,
};
pub use memory::{
    append_log_storage, kv_storage, memory_append_log, memory_kv, memory_snapshot,
    snapshot_storage, AppendLogStorage, AppendLogStorageOptions, KvStorage, KvStorageOptions,
    SnapshotStorage, SnapshotStorageOptions,
};
#[cfg(feature = "redb-store")]
pub use redb::{
    redb_append_log, redb_append_log_default, redb_backend, redb_kv, redb_kv_default,
    redb_snapshot, redb_snapshot_default, RedbBackend,
};
pub use tier::{
    AppendCursor, AppendLoadResult, AppendLogMode, AppendLogStorageTier, BaseStorageTier,
    KvStorageTier, LoadEntriesOpts, SnapshotStorageTier,
};
pub use wal::{
    graph_wal_prefix, verify_wal_frame_checksum, wal_frame_checksum, wal_frame_key, ChecksumError,
    WALFrame, WalTag, REPLAY_ORDER, WAL_FRAME_SEQ_PAD, WAL_KEY_SEGMENT,
};
