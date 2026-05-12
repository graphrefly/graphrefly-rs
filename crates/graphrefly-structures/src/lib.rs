//! `GraphReFly` reactive data structures (M5.A — D177/D178/D179).
//!
//! Four reactive structures backed by pluggable backends, integrated at
//! the Core level (no Graph dependency required):
//!
//! - [`ReactiveLog`] — append-only log with optional ring-buffer cap
//! - [`ReactiveList`] — ordered list with positional insert/pop
//! - [`ReactiveMap`] — key-value map with set/delete operations
//! - [`ReactiveIndex`] — sorted index with primary key lookup + secondary sort
//!
//! Each structure owns a Core state `NodeId` and emits DIRTY→DATA snapshots
//! on every mutation. Optional `mutation_log` companions record typed
//! `BaseChange<XxxChange<T>>` deltas for op-log changesets (Phase 14).
//!
//! Default backends use `Vec<T>` / `HashMap<K, V>`. `imbl`-backed persistent
//! backends are deferred until bench evidence justifies (D178).
//!
//! CRDT-backed variants (yrs / automerge / loro / diamond-types) are
//! post-1.0 work, gated behind feature flags.

#![warn(rust_2018_idioms, unreachable_pub)]
#![warn(clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::type_complexity,
    clippy::doc_markdown
)]
#![forbid(unsafe_code)]

pub mod backend;
pub mod changeset;
pub mod reactive;

// Re-export core types for ergonomic access.
pub use backend::{
    HashMapBackend, IndexBackend, IndexRow, ListBackend, LogBackend, MapBackend, VecIndexBackend,
    VecListBackend, VecLogBackend,
};
pub use changeset::{
    BaseChange, DeleteReason, IndexChange, Lifecycle, ListChange, LogChange, MapChange, Version,
};
pub use reactive::{
    AppendLogSink, AttachStorageHandle, IndexEqualsFn, IndexOutOfBounds, InternFn, LogView,
    MapConfigError, ReactiveIndex, ReactiveIndexOptions, ReactiveList, ReactiveListOptions,
    ReactiveLog, ReactiveLogOptions, ReactiveMap, ReactiveMapOptions, RetentionPolicy, ScanHandle,
    UpsertOptions, ViewSpec,
};
