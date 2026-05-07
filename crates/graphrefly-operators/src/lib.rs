//! Built-in operator node types for `GraphReFly`.
//!
//! Operators in this crate are specialized node types implemented
//! directly against the Core protocol — they are not "user fns wrapped
//! in nodes." A `map` operator's plumbing (dirty propagation, equals
//! dedup, batch handling) runs entirely in Rust; only the user-supplied
//! `(T) -> U` callback crosses the FFI boundary on each fire.
//!
//! # Status (Slice C-1, 2026-05-06)
//!
//! Transform module landed: [`map`], [`filter`], [`scan`], [`reduce`],
//! [`distinct_until_changed`], [`pairwise`]. Architecture per
//! `docs/rust-port-decisions.md` D009–D019.
//!
//! # Module layout (planned)
//!
//! - [`transform`] — map, filter, scan, reduce, distinctUntilChanged,
//!   pairwise (✅ Slice C-1)
//! - [`combine`] — combine, merge, withLatestFrom (✅ Slice C-2)
//! - `temporal` — throttle, debounce, sample
//! - `flow` — take, skip, takeWhile, skipWhile
//! - `switching` — switchMap, mergeMap, concatMap
//! - `gating` — valve, gate, budgetGate, policyGate
//! - `resilience` — retry, circuitBreaker, timeout, fallback,
//!   rateLimiter, tokenBucket
//!
//! # Layering
//!
//! This crate depends on `graphrefly-core` only (per the user-direction
//! constraint that operators do not depend on `graphrefly-graph`).
//! Operator factories accept `&Core` directly. User callbacks travel
//! through the [`OperatorBinding`] super-trait of `BindingBoundary`,
//! which the binding crate (e.g., a `TestOperatorBinding` for tests, the
//! napi-rs / pyo3 bindings in production) implements.

#![forbid(unsafe_code)]
#![warn(rust_2018_idioms, unreachable_pub)]
#![warn(clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::doc_markdown
)]

pub mod binding;
pub mod combine;
pub mod transform;

pub use binding::OperatorBinding;
pub use combine::{combine as combine_latest, merge, with_latest_from, MergeRegistration};
pub use transform::{
    distinct_until_changed, filter, map, pairwise, reduce, scan, OperatorRegistration,
};
