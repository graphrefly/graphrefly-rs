//! `GraphReFly` storage tier dispatch + Node-side persistence.
//!
//! Implements the G.27 storage tier protocol: tiered N-way storage with
//! per-tier transactions, debouncing, compaction, and codec
//! parameterization. Phase 13.6's deferred ACID atomicity tightening
//! lands here via [`redb`](https://docs.rs/redb), which provides
//! pure-Rust ACID transactions without a C dependency.
//!
//! # Status (M4.D — 2026-05-11)
//!
//! - [`wal`] — WAL frame substrate + canonical-JSON SHA-256 checksum
//!   (DS-14-storage Q1 + Q5 locks).
//! - [`error`] — [`StorageError`] / [`RestoreError`] / [`RestoreResult`].
//! - [`codec`] — [`Codec`] trait + [`JsonCodec`] (canonical-JSON encoding,
//!   parity with TS `jsonCodec`).
//! - [`backend`] — [`StorageBackend`] trait + [`MemoryBackend`] +
//!   [`memory_backend`] factory.
//! - [`tier`] — [`BaseStorageTier`] + typed sub-traits ([`SnapshotStorageTier`],
//!   [`AppendLogStorageTier`], [`KvStorageTier`]).
//! - [`memory`] — concrete generic structs `SnapshotStorage` / `AppendLogStorage`
//!   / `KvStorage` + factories `memory_snapshot` / `memory_append_log` /
//!   `memory_kv`.
//! - [`file`] (feature `file`) — [`FileBackend`] + [`file_backend`] factory +
//!   atomic-rename `write` via `tempfile` + percent-encoded filenames
//!   (byte-identical to TS `fileBackend`).
//! - [`redb`] (feature `redb-store`) — [`RedbBackend`] + [`redb_backend`]
//!   factory + ACID per-write transactions via [`redb::Database`]. Structurally
//!   closes F1 (pending retry-safety at tier level) + F3 (concurrent flush
//!   race).
//!
//! - [`graph_integration`] — Graph-level storage integration (M4.E2):
//!   `attach_snapshot_storage`, `restore_snapshot`, snapshot diff engine,
//!   `GraphCheckpointRecord`. Free functions (D170) because `graphrefly-graph`
//!   does not depend on this crate.

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
pub use tier::{AppendLogStorageTier, BaseStorageTier, KvStorageTier, SnapshotStorageTier};
pub use wal::{
    graph_wal_prefix, verify_wal_frame_checksum, wal_frame_checksum, wal_frame_key, ChecksumError,
    WALFrame, WalTag, REPLAY_ORDER, WAL_FRAME_SEQ_PAD, WAL_KEY_SEGMENT,
};
