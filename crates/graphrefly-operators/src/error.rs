//! Operator factory errors (Slice H /qa F7, 2026-05-07).
//!
//! Factory functions in this crate enforce two kinds of invariants:
//!
//! 1. **Core-layer construction invariants** — already typed via
//!    [`graphrefly_core::RegisterError`] (unknown dep, terminal-non-
//!    resubscribable dep, operator without deps, sentinel seed, etc.).
//! 2. **Factory-shape invariants** — pre-conditions on the factory
//!    arguments that Core can't see because the factory hasn't yet
//!    constructed the [`graphrefly_core::OperatorOp`] descriptor (or
//!    the Core check would fire with the wrong error message). Examples:
//!    - `combine(sources, packer)` requires `!sources.is_empty()` (Core
//!      would surface the same condition as `OperatorWithoutDeps`, but
//!      the factory error is named after the public API).
//!    - `last_with_default(source, default)` requires `default !=
//!      NO_HANDLE` — Core accepts `Last { default: NO_HANDLE }` as the
//!      no-default variant (used by `last()`), so the factory must
//!      enforce its tighter contract.
//!
//! Both kinds funnel through [`OperatorFactoryError`] so factory
//! callers handle a single error enum. The Core variant is
//! transparent-wrapped: `?` propagation from `register_operator(...)?`
//! works directly via the auto-derived `From<RegisterError>` impl.

use graphrefly_core::RegisterError;
use thiserror::Error;

/// Errors returnable by [`crate::combine`], [`crate::merge`],
/// [`crate::merge_as_op`], [`crate::last_with_default`], and
/// [`crate::last_with_default_with`].
///
/// Other operator factories ([`crate::map`], [`crate::filter`],
/// [`crate::with_latest_from`], etc.) have no extra factory-shape
/// invariants beyond Core's checks — they panic on Core-layer
/// `RegisterError` via `.expect("invariant: caller has validated dep
/// ids …")` because their public contract is "the caller supplies
/// already-valid `NodeId`s." Slice H `# Errors` rustdoc on those
/// factories documents the panic conditions.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum OperatorFactoryError {
    /// `combine` / `merge` / `merge_as_op` was called with empty
    /// `sources`. At least one upstream is required — for fan-out
    /// broadcast use a producer node with explicit subscriptions.
    #[error("operator factory: at least one source is required")]
    EmptySources,

    /// `last_with_default` / `last_with_default_with` was called with
    /// `NO_HANDLE` as the default. Use [`crate::last`] for the
    /// no-default variant, which emits Complete without a Data on
    /// empty streams.
    #[error(
        "operator factory: last_with_default requires a real default handle \
         (use last() for no-default behavior)"
    )]
    ZeroDefault,

    /// Underlying Core registration failed. Transparent-wrapped so
    /// `register_operator(...)?` propagates correctly.
    #[error(transparent)]
    Register(#[from] RegisterError),
}
