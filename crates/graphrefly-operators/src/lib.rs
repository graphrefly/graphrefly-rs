//! Built-in operator node types for `GraphReFly`.
//!
//! Operators in this crate are specialized node types implemented
//! directly against the Core protocol — they are not "user fns wrapped
//! in nodes." A `map` operator's plumbing (dirty propagation, equals
//! dedup, batch handling) runs entirely in Rust; only the user-supplied
//! `(T) -> U` callback crosses the FFI boundary on each fire.
//!
//! # Status
//!
//! Scaffold. Implementation lands during Milestone 3 of the Rust port.
//!
//! # Module layout (planned)
//!
//! - `transform` — map, filter, scan, distinctUntilChanged
//! - `combine` — combine, merge, withLatestFrom
//! - `temporal` — throttle, debounce, sample
//! - `flow` — take, skip, takeWhile, skipWhile
//! - `switching` — switchMap, mergeMap, concatMap
//! - `gating` — valve, gate, budgetGate, policyGate
//! - `resilience` — retry, circuitBreaker, timeout, fallback,
//!   rateLimiter, tokenBucket

#![warn(rust_2018_idioms, unreachable_pub)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::missing_errors_doc)]

#[cfg(test)]
mod tests {
    #[test]
    fn scaffold_compiles() {}
}
