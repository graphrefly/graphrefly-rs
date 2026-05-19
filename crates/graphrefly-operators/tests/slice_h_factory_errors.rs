//! Slice H /qa F7 (2026-05-07) — operator-factory typed-error regression
//! tests.
//!
//! Covers the three factories whose pre-condition asserts were promoted
//! to typed errors via [`graphrefly_operators::OperatorFactoryError`]:
//!
//! - [`combine`] / [`merge`] / [`merge_as_op`] —
//!   [`OperatorFactoryError::EmptySources`] on empty `sources`.
//! - [`last_with_default`] / [`last_with_default_with`] —
//!   [`OperatorFactoryError::ZeroDefault`] on `default == NO_HANDLE`.
//!
//! Also pins the `From<RegisterError>` propagation path: a Core-layer
//! error (e.g., terminal-non-resubscribable dep) wraps cleanly as
//! [`OperatorFactoryError::Register`] without losing the inner variant.

mod common;

use common::{OpRuntime, TestValue};
use graphrefly_core::{NodeId, RegisterError, NO_HANDLE};
use graphrefly_operators::combine::{combine, merge, merge_as_op};
use graphrefly_operators::flow::{last_with_default, last_with_default_with};
use graphrefly_operators::OperatorFactoryError;

// =====================================================================
// combine / merge / merge_as_op — EmptySources
// =====================================================================

#[test]
fn combine_with_empty_sources_errors_empty_sources() {
    let rt = OpRuntime::new();
    let result = combine(rt.core(), &rt.op_binding, &[], rt.make_packer());
    assert_eq!(result.err(), Some(OperatorFactoryError::EmptySources));
}

#[test]
fn merge_with_empty_sources_errors_empty_sources() {
    let rt = OpRuntime::new();
    let result = merge(rt.core(), &[]);
    assert_eq!(result.err(), Some(OperatorFactoryError::EmptySources));
}

#[test]
fn merge_as_op_with_empty_sources_errors_empty_sources() {
    let rt = OpRuntime::new();
    let result = merge_as_op(rt.core(), &[]);
    assert_eq!(result.err(), Some(OperatorFactoryError::EmptySources));
}

// =====================================================================
// last_with_default / last_with_default_with — ZeroDefault
// =====================================================================

#[test]
fn last_with_default_zero_handle_errors_zero_default() {
    let rt = OpRuntime::new();
    let s = rt.state_int(Some(1));
    let result = last_with_default(rt.core(), s, NO_HANDLE);
    assert_eq!(result.err(), Some(OperatorFactoryError::ZeroDefault));
}

#[test]
fn last_with_default_with_zero_handle_errors_zero_default() {
    let rt = OpRuntime::new();
    let s = rt.state_int(Some(1));
    let result = last_with_default_with(
        rt.core(),
        s,
        NO_HANDLE,
        graphrefly_core::OperatorOpts::default(),
    );
    assert_eq!(result.err(), Some(OperatorFactoryError::ZeroDefault));
}

// =====================================================================
// `From<RegisterError>` propagation — a Core-layer error wraps as
// `OperatorFactoryError::Register(...)` without losing the inner variant.
// =====================================================================

#[test]
fn merge_with_unknown_dep_propagates_register_error() {
    let rt = OpRuntime::new();
    let bogus = NodeId::new(99_999);
    let result = merge(rt.core(), &[bogus]);
    assert_eq!(
        result.err(),
        Some(OperatorFactoryError::Register(RegisterError::UnknownDep(
            bogus
        )))
    );
}

#[test]
fn combine_with_unknown_dep_propagates_register_error() {
    let rt = OpRuntime::new();
    let known = rt.state_int(Some(1));
    let bogus = NodeId::new(99_999);
    let result = combine(rt.core(), &rt.op_binding, &[known, bogus], rt.make_packer());
    assert_eq!(
        result.err(),
        Some(OperatorFactoryError::Register(RegisterError::UnknownDep(
            bogus
        )))
    );
}

// =====================================================================
// Error-precedence — factory checks short-circuit before Core checks.
// EmptySources is detected at the factory layer; if we delegated to
// Core, the same condition would surface as RegisterError::OperatorWithoutDeps,
// but the factory error is named after the public API for clearer
// diagnostics.
// =====================================================================

#[test]
fn merge_empty_sources_takes_precedence_over_core_check() {
    let rt = OpRuntime::new();
    let result = merge(rt.core(), &[]);
    // Must be EmptySources (factory layer), NOT
    // Register(OperatorWithoutDeps) — the factory short-circuits before
    // calling register_operator.
    assert!(matches!(
        result.err(),
        Some(OperatorFactoryError::EmptySources)
    ));
}

// =====================================================================
// Smoke check — the existing tuple of (assertion, value) returns are
// unchanged on the success path. Ensures the Result wrapping didn't
// alter the OperatorRegistration / MergeRegistration / FlowRegistration
// payload semantics.
// =====================================================================

#[test]
fn combine_success_path_preserves_registration_shape() {
    let rt = OpRuntime::new();
    let a = rt.state_int(Some(1));
    let b = rt.state_int(Some(2));
    let reg = combine(rt.core(), &rt.op_binding, &[a, b], rt.make_packer()).expect("combine ok");
    // The combined node should fire on subscribe (push-on-subscribe).
    let rec = rt.subscribe_recorder(reg.node);
    let values = rec.data_values();
    assert_eq!(values.len(), 1);
    assert_eq!(
        values[0],
        TestValue::Tuple(vec![TestValue::Int(1), TestValue::Int(2)])
    );
}
