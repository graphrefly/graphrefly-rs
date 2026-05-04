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
//! Scaffold. Implementation lands during Milestone 4 of the Rust port.
//!
//! # Module layout (planned)
//!
//! - `tier` — `StorageTier` trait, dispatch, transaction model
//! - `memory` — `MemoryStorage`
//! - `file` — `FileStorage` (atomic rename via tempfile)
//! - `redb_store` — `RedbStorage` (replaces sqliteStorage)
//! - `wal` — write-ahead log (Phase 8.7 + Phase 14 delta-replay substrate)
//! - `compaction` — compactEvery / debounce coalescing

#![warn(rust_2018_idioms, unreachable_pub)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::missing_errors_doc)]

#[cfg(test)]
mod tests {
    #[test]
    fn scaffold_compiles() {}
}
