//! Slice Y regression — producer-build closure Arc-cycle is broken.
//!
//! Pre-Slice-Y: every producer-pattern operator factory (`zip` /
//! `concat` / `race` / `take_until` in `ops_impl.rs`; `switch_map` /
//! `exhaust_map` / `merge_map` / `concat_map` in `higher_order.rs`)
//! captured a strong `Arc<dyn ProducerBinding>` (and `Arc<dyn
//! HigherOrderBinding>` for higher-order) inside the `build` closure
//! stored long-term in `binding.registry.producer_builds`. This created
//! a self-referential cycle:
//!
//! ```text
//! BenchBinding → registry → producer_builds[fn_id]
//!   → closure → strong Arc<dyn ProducerBinding> → BenchBinding
//! ```
//!
//! Result: when the host `BenchCore` (or in tests, `OpRuntime`) drops,
//! the binding's strong count never hit zero, leaking the entire graph
//! state per dropped instance. Bounded per-instance, but quadratic in
//! a long-running multi-agent / harness workload that constructs many
//! BenchCore instances over its lifetime.
//!
//! Slice Y fix: producer-build closures capture `WeakCore` and
//! `Weak<dyn ProducerBinding>` (plus `Weak<dyn HigherOrderBinding>` for
//! the higher-order factories), upgrading at build-time. If the host
//! Core dropped between registration and build-time activation, the
//! closure no-ops cleanly via `weak.upgrade().is_none()`.
//!
//! These tests build production-realistic graphs with each producer
//! factory, drop the runtime, and assert the binding's strong count
//! drops to 0 (verified via `Weak::upgrade().is_none()`).

mod common;

use std::sync::Arc;

use common::{InnerBinding, OpRuntime};
use graphrefly_operators::{
    concat, exhaust_map, higher_order::merge_map_with_concurrency, race, switch_map, take_until,
    zip,
};

/// Helper: build the runtime, register the operator, drop the runtime,
/// then assert `Weak<InnerBinding>` no longer upgrades. Returns the
/// final outcome so each test's assertion failure points at the right
/// line.
fn assert_binding_drops_after<F>(register: F)
where
    F: FnOnce(&OpRuntime),
{
    let weak = {
        let rt = OpRuntime::new();
        let weak: std::sync::Weak<InnerBinding> = Arc::downgrade(&rt.binding);
        // Sanity: weak upgrades while runtime is alive.
        assert!(
            weak.upgrade().is_some(),
            "binding should be alive while runtime is held"
        );
        register(&rt);
        // rt drops here — OpRuntime::Drop breaks the binding↔Core
        // `core_ref` back-reference; the weak-Arc fix in producer
        // factories breaks the producer-build closure cycle.
        weak
    };
    assert!(
		weak.upgrade().is_none(),
		"binding strong count > 0 after runtime drop — Arc cycle leak (producer-build closure path)"
	);
}

#[test]
fn zip_no_leak_after_runtime_drop() {
    assert_binding_drops_after(|rt| {
        let s1 = rt.state_int(None);
        let s2 = rt.state_int(None);
        let pack_fn = rt.register_tuple_packer();
        let _ = zip(&rt.core, &rt.producer_binding, vec![s1, s2], pack_fn);
    });
}

#[test]
fn concat_no_leak_after_runtime_drop() {
    assert_binding_drops_after(|rt| {
        let s1 = rt.state_int(None);
        let s2 = rt.state_int(None);
        let _ = concat(&rt.core, &rt.producer_binding, s1, s2);
    });
}

#[test]
fn race_no_leak_after_runtime_drop() {
    assert_binding_drops_after(|rt| {
        let s1 = rt.state_int(None);
        let s2 = rt.state_int(None);
        let _ = race(&rt.core, &rt.producer_binding, vec![s1, s2]);
    });
}

#[test]
fn take_until_no_leak_after_runtime_drop() {
    assert_binding_drops_after(|rt| {
        let s = rt.state_int(None);
        let n = rt.state_int(None);
        let _ = take_until(&rt.core, &rt.producer_binding, s, n);
    });
}

#[test]
fn switch_map_no_leak_after_runtime_drop() {
    assert_binding_drops_after(|rt| {
        let s = rt.state_int(None);
        let project: graphrefly_operators::higher_order::ProjectFn = Box::new(|_h| {
            // Project is never called in this test (we just register
            // the operator and drop). Returns a dummy NodeId.
            graphrefly_core::NodeId::new(0)
        });
        let _ = switch_map(&rt.core, &rt.ho_binding, s, project);
    });
}

#[test]
fn exhaust_map_no_leak_after_runtime_drop() {
    assert_binding_drops_after(|rt| {
        let s = rt.state_int(None);
        let project: graphrefly_operators::higher_order::ProjectFn =
            Box::new(|_h| graphrefly_core::NodeId::new(0));
        let _ = exhaust_map(&rt.core, &rt.ho_binding, s, project);
    });
}

#[test]
fn merge_map_no_leak_after_runtime_drop() {
    assert_binding_drops_after(|rt| {
        let s = rt.state_int(None);
        let project: graphrefly_operators::higher_order::ProjectFn =
            Box::new(|_h| graphrefly_core::NodeId::new(0));
        let _ = merge_map_with_concurrency(&rt.core, &rt.ho_binding, s, project, None);
    });
}

#[test]
fn many_producers_no_leak_after_runtime_drop() {
    // Combined registration — many producer-pattern operators on one
    // runtime. Tests the per-closure cycle accumulation; pre-Slice-Y
    // would leak ~7 strong refs to the binding and ~7 strong refs to
    // the Core.
    assert_binding_drops_after(|rt| {
        let a = rt.state_int(None);
        let b = rt.state_int(None);
        let pack_fn = rt.register_tuple_packer();
        let _ = zip(&rt.core, &rt.producer_binding, vec![a, b], pack_fn);
        let _ = concat(&rt.core, &rt.producer_binding, a, b);
        let _ = race(&rt.core, &rt.producer_binding, vec![a, b]);
        let _ = take_until(&rt.core, &rt.producer_binding, a, b);
        let _ = switch_map(
            &rt.core,
            &rt.ho_binding,
            a,
            Box::new(|_| graphrefly_core::NodeId::new(0)),
        );
        let _ = exhaust_map(
            &rt.core,
            &rt.ho_binding,
            a,
            Box::new(|_| graphrefly_core::NodeId::new(0)),
        );
        let _ = merge_map_with_concurrency(
            &rt.core,
            &rt.ho_binding,
            a,
            Box::new(|_| graphrefly_core::NodeId::new(0)),
            None,
        );
    });
}
