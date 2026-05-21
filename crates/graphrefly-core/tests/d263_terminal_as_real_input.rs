//! D263 — `NodeOpts.terminal_as_real_input` substrate flag.
//!
//! When `terminal_as_real_input == true` (or `partial == true`), a
//! non-operator derived node skips Lock 2.B auto-cascade on dep COMPLETE
//! AND the `fire_fn` first-run gate (R2.5.3) treats a dep's terminal as
//! "real input" — so the fn fires on a terminal-without-DATA from any
//! single dep. Mirrors the unconditional `fire_operator` semantics
//! (Reduce/Last/Valve already do this via their `OperatorOp` discriminant).
//!
//! Substrate-only per D263 — NOT widened onto the `Impl` parity
//! contract. See `~/src/graphrefly-ts/docs/rust-port-decisions.md` D263.

use graphrefly_core::{FnEmission, FnResult, NodeFnOrOp, NodeOpts, NodeRegistration};
use smallvec::smallvec;

mod common;
use common::{RecordedEvent, TestRuntime, TestValue};

// Raw-fn helper — emits a single constant DATA regardless of dep state.
// Using `register_raw_fn` (which gets `&[DepBatch]` directly) instead of
// `register_fn` (which derefs `db.latest()` and panics on NO_HANDLE) lets
// these tests exercise the terminal-only-dep path cleanly.
fn make_constant_fn(rt: &TestRuntime, value: TestValue) -> graphrefly_core::FnId {
    let binding = rt.binding.clone();
    rt.binding.register_raw_fn(move |_deps| {
        let h = binding.intern(value.clone());
        FnResult::Batch {
            emissions: smallvec![FnEmission::Data(h)],
            tracked: None,
        }
    })
}

// ---------------------------------------------------------------------------
// Default behaviour: terminal_as_real_input=false → COMPLETE-without-DATA
// from a sentinel dep keeps the gate closed and the node auto-cascades to
// COMPLETE without firing the fn (Lock 2.B preserved).
// ---------------------------------------------------------------------------

#[test]
fn default_terminal_does_not_open_gate_for_fire_fn() {
    let rt = TestRuntime::new();
    let s = rt.state(None); // sentinel state — never emits DATA.
    let fn_id = make_constant_fn(&rt, TestValue::Int(42));

    // Default opts: terminal_as_real_input=false, partial=false.
    let derived = rt
        .core()
        .register(NodeRegistration {
            deps: vec![s.id],
            fn_or_op: Some(NodeFnOrOp::Fn(fn_id)),
            opts: NodeOpts::default(),
        })
        .unwrap();
    let rec = rt.subscribe_recorder(derived);

    rt.core().complete(s.id);

    // The fn must NOT have fired — sentinel-hold preserved; auto-cascade
    // takes the consumer to COMPLETE directly.
    let fired = rec.count(|e| matches!(e, RecordedEvent::Data(_)));
    assert_eq!(
        fired, 0,
        "default terminal_as_real_input=false: fire_fn must remain sentinel-gated on COMPLETE-without-DATA"
    );
}

// ---------------------------------------------------------------------------
// Opt-in: terminal_as_real_input=true → non-operator derived node skips
// auto-cascade and the fn fires once on dep COMPLETE-without-DATA.
// ---------------------------------------------------------------------------

#[test]
fn opt_in_terminal_opens_gate_for_fire_fn() {
    let rt = TestRuntime::new();
    let s = rt.state(None);
    let fn_id = make_constant_fn(&rt, TestValue::Int(42));

    let derived = rt
        .core()
        .register(NodeRegistration {
            deps: vec![s.id],
            fn_or_op: Some(NodeFnOrOp::Fn(fn_id)),
            opts: NodeOpts {
                terminal_as_real_input: true,
                ..NodeOpts::default()
            },
        })
        .unwrap();
    let rec = rt.subscribe_recorder(derived);

    rt.core().complete(s.id);

    let data: Vec<TestValue> = rec.data_values();
    assert_eq!(
        data,
        vec![TestValue::Int(42)],
        "terminal_as_real_input=true: fire_fn must fire once on dep COMPLETE-without-DATA"
    );
}

// ---------------------------------------------------------------------------
// partial=true bypasses the gate entirely regardless of the flag.
// ---------------------------------------------------------------------------

#[test]
fn partial_bypasses_gate_regardless_of_flag() {
    let rt = TestRuntime::new();
    let s = rt.state(None);
    let fn_id = make_constant_fn(&rt, TestValue::Int(7));

    // partial=true, terminal_as_real_input=false — partial alone bypasses
    // the gate AND skips auto-cascade (D263 — `skips_auto_cascade` returns
    // true when `partial || terminal_as_real_input`).
    let derived = rt
        .core()
        .register(NodeRegistration {
            deps: vec![s.id],
            fn_or_op: Some(NodeFnOrOp::Fn(fn_id)),
            opts: NodeOpts {
                partial: true,
                terminal_as_real_input: false,
                ..NodeOpts::default()
            },
        })
        .unwrap();
    let rec = rt.subscribe_recorder(derived);

    rt.core().complete(s.id);

    let data: Vec<TestValue> = rec.data_values();
    assert_eq!(
        data,
        vec![TestValue::Int(7)],
        "partial=true: gate bypassed; fn fires on dep COMPLETE even with terminal_as_real_input=false"
    );
}
