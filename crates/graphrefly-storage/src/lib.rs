//! `GraphReFly` storage tier dispatch + Node-side persistence.
//!
//! Implements the G.27 storage tier protocol: tiered N-way storage with
//! per-tier transactions, debouncing, compaction, and codec
//! parameterization. Phase 13.6's deferred ACID atomicity tightening
//! lands here via [`redb`](https://docs.rs/redb), which provides
//! pure-Rust ACID transactions without a C dependency.
//!
//! # Status (M4.B — 2026-05-10)
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
//!
//! File backend (M4.C), redb backend (M4.D), and Graph integration
//! (`Graph::attach_storage` / `restore_snapshot mode:"diff"`, M4.E) land in
//! subsequent M4 sub-slices.

#![warn(rust_2018_idioms, unreachable_pub)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::missing_errors_doc)]
#![forbid(unsafe_code)]

pub mod backend;
pub mod codec;
pub mod error;
pub mod memory;
pub mod tier;
pub mod wal;

pub use backend::{memory_backend, MemoryBackend, StorageBackend};
pub use codec::{Codec, CodecError, JsonCodec};
pub use error::{PhaseStat, RestoreError, RestoreResult, StorageError};
pub use memory::{
    append_log_storage, kv_storage, memory_append_log, memory_kv, memory_snapshot,
    snapshot_storage, AppendLogStorage, AppendLogStorageOptions, KvStorage, KvStorageOptions,
    SnapshotStorage, SnapshotStorageOptions,
};
pub use tier::{AppendLogStorageTier, BaseStorageTier, KvStorageTier, SnapshotStorageTier};
pub use wal::{
    graph_wal_prefix, verify_wal_frame_checksum, wal_frame_checksum, wal_frame_key, ChecksumError,
    WALFrame, WalTag, REPLAY_ORDER, WAL_FRAME_SEQ_PAD, WAL_KEY_SEGMENT,
};
