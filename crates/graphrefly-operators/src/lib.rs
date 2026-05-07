//! Built-in operator node types for `GraphReFly`.
//!
//! Operators in this crate are specialized node types implemented
//! directly against the Core protocol — they are not "user fns wrapped
//! in nodes." A `map` operator's plumbing (dirty propagation, equals
//! dedup, batch handling) runs entirely in Rust; only the user-supplied
//! `(T) -> U` callback crosses the FFI boundary on each fire.
//!
//! # Status (Slice C-3, 2026-05-06)
//!
//! Transform module: [`map`], [`filter`], [`scan`], [`reduce`],
//! [`distinct_until_changed`], [`pairwise`] (Slice C-1, D009–D019).
//! Combine module: [`combine_latest`], [`merge`], [`with_latest_from`]
//! (Slice C-2, D020–D023). Flow module: [`take`], [`skip`],
//! [`take_while`], [`last`], [`last_with_default`] + [`first`],
//! [`find`], [`element_at`] sugar (Slice C-3, D024–D029). Per-operator
//! state lives behind a generic
//! [`OperatorScratch`](graphrefly_core::op_state::OperatorScratch)
//! slot on `NodeRecord` (D026 — replaces the typed `operator_state`
//! field used by Slices C-1 / C-2).
//!
//! # Module layout (planned)
//!
//! - [`transform`] — map, filter, scan, reduce, distinctUntilChanged,
//!   pairwise (✅ Slice C-1)
//! - [`combine`] — combine, merge, withLatestFrom (✅ Slice C-2)
//! - [`flow`] — take, skip, takeWhile, last + first/find/element_at
//!   sugar (✅ Slice C-3)
//! - `temporal` — throttle, debounce, sample
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
pub mod flow;
pub mod ops_impl;
pub mod producer;
pub mod transform;

pub use binding::OperatorBinding;
pub use combine::{combine as combine_latest, merge, with_latest_from, MergeRegistration};
pub use flow::{
    element_at, find, first, last, last_with_default, skip, take, take_while, FlowRegistration,
};
pub use ops_impl::{concat, race, take_until, zip};
pub use producer::{
    default_producer_deactivate, ProducerBinding, ProducerBuildFn, ProducerCtx, ProducerNodeState,
    ProducerStorage,
};
pub use transform::{
    distinct_until_changed, filter, map, pairwise, reduce, scan, OperatorRegistration,
};
