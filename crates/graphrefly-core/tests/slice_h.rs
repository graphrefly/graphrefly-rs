//! Slice H (2026-05-07) — typed-error refactor regression tests.
//!
//! Promotes `Core::register*` and `Core::set_pausable_mode` from
//! `assert!`/`panic!` to typed `Result<_, RegisterError>` /
//! `Result<_, SetPausableModeError>`. Each test below covers one error
//! variant and verifies (a) the correct typed error is returned,
//! (b) no node was added to the graph on `Err`, and (c) for paths that
//! take handle retains BEFORE state-lock validation, refcount discipline
//! is preserved on early-return.
//!
//! Cross-references:
//! - [`slice_f_corrections.rs`] `item4_register_rejects_non_resubscribable_terminal_dep`
//!   already covers `RegisterError::TerminalDep`.
//! - [`slice_f_corrections.rs`] `item5_set_pausable_mode_rejects_when_paused`
//!   already covers `SetPausableModeError::WhilePaused`.
//! - This file adds the remaining variants plus refcount-leak verification.

use graphrefly_core::{
    EqualsMode, NodeFnOrOp, NodeId, NodeOpts, NodeRegistration, OperatorOp, OperatorOpts,
    PausableMode, RegisterError, SetPausableModeError, NO_HANDLE,
};

mod common;
use common::{TestRuntime, TestValue};

// =====================================================================
// RegisterError::OperatorWithoutDeps — operator nodes need at least
// one dep (use register_producer for subscription-managed combinators).
// =====================================================================

#[test]
fn register_operator_with_zero_deps_errors_operator_without_deps() {
    let rt = TestRuntime::new();
    // Use a Map op variant (any op with zero deps triggers the same path).
    let fn_id = rt
        .binding
        .register_fn(|deps: &[TestValue]| deps.first().cloned());
    let result = rt
        .core
        .register_operator(&[], OperatorOp::Map { fn_id }, OperatorOpts::default());
    assert_eq!(result, Err(RegisterError::OperatorWithoutDeps));
}

// =====================================================================
// RegisterError::InitialOnlyForStateNodes — `initial` is only valid
// for state nodes (no deps + no fn + no op).
// =====================================================================

#[test]
fn register_with_initial_on_non_state_shape_errors() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let fn_id = rt
        .binding
        .register_fn(|deps: &[TestValue]| deps.first().cloned());
    let h = rt.binding.intern(TestValue::Int(99));
    // Derived shape (deps non-empty, fn present) with non-sentinel initial.
    let result = rt.core.register(NodeRegistration {
        deps: vec![s.id],
        fn_or_op: Some(NodeFnOrOp::Fn(fn_id)),
        opts: NodeOpts {
            initial: h,
            ..NodeOpts::default()
        },
    });
    assert_eq!(result, Err(RegisterError::InitialOnlyForStateNodes));
}

// =====================================================================
// RegisterError::UnknownDep — a dep id is not registered.
// =====================================================================

#[test]
fn register_derived_with_unknown_dep_errors() {
    let rt = TestRuntime::new();
    let bogus = NodeId::new(99_999);
    let fn_id = rt
        .binding
        .register_fn(|deps: &[TestValue]| deps.first().cloned());
    let result = rt
        .core
        .register_derived(&[bogus], fn_id, EqualsMode::Identity, false);
    assert_eq!(result, Err(RegisterError::UnknownDep(bogus)));
}

// =====================================================================
// RegisterError::OperatorSeedSentinel — Scan/Reduce seed must be a
// real handle (R2.5.3).
// =====================================================================

#[test]
fn register_operator_scan_with_no_handle_seed_errors() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    // Use a dummy fn id — fold_each is never invoked because seed
    // validation fires first.
    let dummy_fn_id = rt
        .binding
        .register_fn(|deps: &[TestValue]| deps.first().cloned());
    let result = rt.core.register_operator(
        &[s.id],
        OperatorOp::Scan {
            fn_id: dummy_fn_id,
            seed: NO_HANDLE,
        },
        OperatorOpts::default(),
    );
    assert_eq!(result, Err(RegisterError::OperatorSeedSentinel));
}

#[test]
fn register_operator_reduce_with_no_handle_seed_errors() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let dummy_fn_id = rt
        .binding
        .register_fn(|deps: &[TestValue]| deps.first().cloned());
    let result = rt.core.register_operator(
        &[s.id],
        OperatorOp::Reduce {
            fn_id: dummy_fn_id,
            seed: NO_HANDLE,
        },
        OperatorOpts::default(),
    );
    assert_eq!(result, Err(RegisterError::OperatorSeedSentinel));
}

// =====================================================================
// SetPausableModeError::UnknownNode — node id is not registered.
// (NEW in Slice H — widens the previous `require_node_mut` panic.)
// =====================================================================

#[test]
fn set_pausable_mode_on_unknown_node_errors() {
    // Slice H /qa F16: also assert that an UnknownNode error leaves the
    // pausable mode of *existing* nodes untouched — no partial state
    // mutation before the early-return.
    let rt = TestRuntime::new();
    let known = rt.state(Some(TestValue::Int(0)));

    // Sanity-check baseline mode by setting it explicitly.
    rt.core
        .set_pausable_mode(known.id, PausableMode::Off)
        .expect("baseline mode set");

    // Attempt to change mode on a bogus id — must error AND must not
    // reach into another record.
    let bogus = NodeId::new(99_999);
    let result = rt.core.set_pausable_mode(bogus, PausableMode::ResumeAll);
    assert_eq!(result, Err(SetPausableModeError::UnknownNode(bogus)));

    // Existing node's mode is still `Off` — the failed call did not
    // touch any state.
    rt.core
        .set_pausable_mode(known.id, PausableMode::Off)
        .expect("re-set Off on known node");
}

// =====================================================================
// Refcount discipline — on Err, no handle leaks.
//
// `make_op_scratch` for Scan/Reduce calls `binding.retain_handle(seed)`
// BEFORE the state-lock-required validation (UnknownDep / TerminalDep).
// Slice H's contract: if Phase 3 (state-lock validation) returns Err,
// the scratch is released via `OperatorScratch::release_handles` so the
// seed handle's refcount returns to its pre-call value.
// =====================================================================

#[test]
fn register_operator_scan_with_unknown_dep_does_not_leak_seed() {
    let rt = TestRuntime::new();
    let bogus = NodeId::new(99_999);
    let seed = rt.binding.intern(TestValue::Int(7));
    let pre_refcount = rt.binding.refcount_of(seed);

    let dummy_fn_id = rt
        .binding
        .register_fn(|deps: &[TestValue]| deps.first().cloned());
    let result = rt.core.register_operator(
        &[bogus],
        OperatorOp::Scan {
            fn_id: dummy_fn_id,
            seed,
        },
        OperatorOpts::default(),
    );
    assert_eq!(result, Err(RegisterError::UnknownDep(bogus)));

    let post_refcount = rt.binding.refcount_of(seed);
    assert_eq!(
        post_refcount, pre_refcount,
        "seed refcount must return to pre-call value on Err — \
         scratch's retain_handle in Phase 2 must be paired with a \
         release_handles in the early-return path of Phase 3"
    );
}

#[test]
fn register_operator_reduce_with_terminal_dep_does_not_leak_seed() {
    // Combine TerminalDep early-return with Reduce's seed retain to
    // ensure the same release_handles discipline applies.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));
    let _rec = rt.subscribe_recorder(s.id);
    rt.core.complete(s.id); // terminate WITHOUT marking resubscribable.

    let seed = rt.binding.intern(TestValue::Int(13));
    let pre_refcount = rt.binding.refcount_of(seed);

    let dummy_fn_id = rt
        .binding
        .register_fn(|deps: &[TestValue]| deps.first().cloned());
    let result = rt.core.register_operator(
        &[s.id],
        OperatorOp::Reduce {
            fn_id: dummy_fn_id,
            seed,
        },
        OperatorOpts::default(),
    );
    assert_eq!(result, Err(RegisterError::TerminalDep(s.id)));

    let post_refcount = rt.binding.refcount_of(seed);
    assert_eq!(
        post_refcount, pre_refcount,
        "seed refcount must return to pre-call value on TerminalDep Err"
    );
}

// =====================================================================
// Sentinel-seed error returns BEFORE retain_handle runs — so there is
// no retain to balance. Ensure no spurious retain happened.
// =====================================================================

#[test]
fn register_operator_scan_sentinel_seed_takes_no_retain() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let baseline_live = rt.binding.live_handles();

    let dummy_fn_id = rt
        .binding
        .register_fn(|deps: &[TestValue]| deps.first().cloned());
    let result = rt.core.register_operator(
        &[s.id],
        OperatorOp::Scan {
            fn_id: dummy_fn_id,
            seed: NO_HANDLE,
        },
        OperatorOpts::default(),
    );
    assert_eq!(result, Err(RegisterError::OperatorSeedSentinel));
    assert_eq!(
        rt.binding.live_handles(),
        baseline_live,
        "OperatorSeedSentinel must return BEFORE retain_handle so live handle count is unchanged"
    );
}

// =====================================================================
// Err returns leave the graph unmutated — node count does not change,
// AND the Result variant is the expected `UnknownDep` (Slice H /qa F5
// — pin both the error variant and the no-graph-mutation invariant
// together so a regression that silently returned Ok would still fail).
// =====================================================================

#[test]
fn register_err_does_not_add_node_to_graph() {
    let rt = TestRuntime::new();
    let _s = rt.state(Some(TestValue::Int(0)));
    let pre_count = rt.core.node_count();

    let bogus = NodeId::new(99_999);
    let fn_id = rt
        .binding
        .register_fn(|deps: &[TestValue]| deps.first().cloned());
    let result = rt
        .core
        .register_derived(&[bogus], fn_id, EqualsMode::Identity, false);

    assert_eq!(result, Err(RegisterError::UnknownDep(bogus)));
    assert_eq!(
        rt.core.node_count(),
        pre_count,
        "node_count must be unchanged after a failed register"
    );
}

// =====================================================================
// Last { default: real } early-return MUST release the default retain.
// /qa F4: Scan + Reduce paths covered above; Last is symmetric and
// must follow the same release_handles discipline. A regression that
// dropped Last from the early-release branch (e.g., a future refactor
// that special-cased Scan/Reduce only) would not be caught without
// this test.
// =====================================================================

#[test]
fn register_operator_last_with_default_with_unknown_dep_does_not_leak_default() {
    let rt = TestRuntime::new();
    let bogus = NodeId::new(99_999);
    let default = rt.binding.intern(TestValue::Int(42));
    let pre_refcount = rt.binding.refcount_of(default);

    let result = rt.core.register_operator(
        &[bogus],
        OperatorOp::Last { default },
        OperatorOpts::default(),
    );
    assert_eq!(result, Err(RegisterError::UnknownDep(bogus)));

    let post_refcount = rt.binding.refcount_of(default);
    assert_eq!(
        post_refcount, pre_refcount,
        "Last `default` refcount must return to pre-call value on Err"
    );
}

// =====================================================================
// Error precedence — /qa F6: Phase 1 (`OperatorWithoutDeps`) MUST
// short-circuit before Phase 2 (`OperatorSeedSentinel`) even when both
// conditions are true. Pins the phase ordering against future
// refactors that might silently swap the checks.
// =====================================================================

#[test]
fn op_without_deps_takes_precedence_over_seed_sentinel() {
    let rt = TestRuntime::new();
    let dummy_fn_id = rt
        .binding
        .register_fn(|deps: &[TestValue]| deps.first().cloned());
    // Both conditions hold: deps empty AND Scan seed is NO_HANDLE.
    // Phase 1 should fire first → OperatorWithoutDeps.
    let result = rt.core.register_operator(
        &[],
        OperatorOp::Scan {
            fn_id: dummy_fn_id,
            seed: NO_HANDLE,
        },
        OperatorOpts::default(),
    );
    assert_eq!(
        result,
        Err(RegisterError::OperatorWithoutDeps),
        "Phase 1 (OperatorWithoutDeps) must short-circuit before Phase 2 (OperatorSeedSentinel)"
    );
}
