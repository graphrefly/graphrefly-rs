//! `GraphReFly` reactive data structures.
//!
//! `reactiveMap`, `reactiveList`, `reactiveLog`, `reactiveIndex` —
//! built on `imbl` persistent immutable collections so that Phase 14
//! op-log changesets get O(log n) snapshot-and-revert naturally:
//!
//!   - Each emit is a function `State -> State` returning a new
//!     persistent collection.
//!   - Rollback = use the prior `Arc<State>`. Glitch-free by
//!     construction.
//!   - Diff = pointer compare on `Arc::ptr_eq` is O(1); structural
//!     diff via `imbl::HashMap::diff` is O(log n).
//!
//! CRDT-backed variants (yrs / automerge / loro / diamond-types) are
//! post-1.0 work, gated behind feature flags. The Rust ecosystem's
//! CRDT libraries are the canonical implementations — yjs JS users
//! increasingly run yrs under WASM.
//!
//! # Status
//!
//! Scaffold. Implementation lands during Milestone 5 of the Rust port,
//! together with the user-facing Phase 14 op-log changeset API.
//!
//! # Module layout (planned)
//!
//! - `map` — reactiveMap (`imbl::HashMap-backed`)
//! - `list` — reactiveList (`imbl::Vector-backed`)
//! - `log` — reactiveLog (append-only op-log)
//! - `index` — reactiveIndex (composite index over a reactiveMap)
//! - `changeset` — Phase 14 op-log delta protocol

#![warn(rust_2018_idioms, unreachable_pub)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::missing_errors_doc)]

#[cfg(test)]
mod tests {
    #[test]
    fn scaffold_compiles() {}
}
