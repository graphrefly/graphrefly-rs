//! napi-rs JavaScript bindings for `GraphReFly`.
//!
//! Exposes the Rust core via the napi-rs FFI to Node.js. The published
//! npm packages (`@graphrefly/lite`, `@graphrefly/standard`,
//! `@graphrefly/full`) are built from this crate with different feature
//! flags via CI matrix.
//!
//! Each platform (linux-x64, linux-arm64, darwin-x64, darwin-arm64,
//! win-x64) gets its own compiled `.node` binary published as a separate
//! npm subpackage; the parent package selects the right binary at
//! install time via npm's `optionalDependencies`.
//!
//! # Status
//!
//! Scaffold. Bindings land progressively as each crate (core, graph,
//! operators, storage, structures) reaches feature parity with
//! graphrefly-ts.

#![warn(rust_2018_idioms, unreachable_pub)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::missing_errors_doc)]

use napi_derive::napi;

/// Smoke export to verify the napi build chain works end-to-end.
/// Will be replaced by real bindings during M1.
#[napi]
#[must_use] 
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
