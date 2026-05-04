//! `GraphReFly` handle-protocol core dispatcher.
//!
//! This crate is the heart of the protocol: dispatcher, message tiers,
//! batch coalescing, wave engine, dep tracking, equals-substitution,
//! first-run gate, PAUSE/RESUME with lockIds, INVALIDATE broadcast,
//! versioning lifecycle.
//!
//! It operates entirely on opaque [`HandleId`] integers — user values
//! `T` never enter the core. Per-language bindings (napi-rs for
//! JavaScript, pyo3 for Python, wasm-bindgen for WASM) hold the
//! value-to-handle registry. Equals-substitution under
//! `equals: 'identity'` is a u64 compare with zero FFI; user-fn
//! invocation is the only mandatory boundary crossing per fn fire.
//!
//! # Status
//!
//! Scaffold. Implementation will land during Milestone 1 of the Rust
//! port. See `archive/docs/SESSION-rust-port-architecture.md` in
//! graphrefly-ts for the full migration plan, and
//! `~/src/graphrefly-ts/src/__experiments__/handle-core/` for the
//! TS prototype that validated the cleaving plane.
//!
//! # Module layout (planned)
//!
//! - `message` — [`Message`] tuples, tier definitions, interned constants
//! - `handle` — [`NodeId`], [`HandleId`], [`FnId`] newtypes
//! - `node` — `NodeRecord`, dispatch, wave engine
//! - `batch` — wave coalescing, deferred delivery
//! - `boundary` — `BindingBoundary` trait (the FFI surface)
//! - `clock` — `monotonic_ns` / `wall_clock_ns`
//! - `guard` — guard policy engine
//! - `meta` — describe / factory tags
//! - `versioning` — V0 → V3 lifecycle, content addressing hooks

#![cfg_attr(not(feature = "std"), no_std)]
#![warn(rust_2018_idioms, unreachable_pub)]
#![warn(clippy::pedantic)]
// `clippy::pedantic` includes a few that are too noisy for this codebase.
#![allow(clippy::module_name_repetitions, clippy::missing_errors_doc)]

// Module skeleton — uncomment as M1 implementation lands.
// pub mod message;
// pub mod handle;
// pub mod node;
// pub mod batch;
// pub mod boundary;
// pub mod clock;
// pub mod guard;
// pub mod meta;
// pub mod versioning;

#[cfg(test)]
mod tests {
    /// Smoke test: the crate compiles and links. Real tests land with M1.
    #[test]
    fn scaffold_compiles() {}
}
