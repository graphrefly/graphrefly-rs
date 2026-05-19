//! Core read-side inspection helpers (Slice E+, M2).
//!
//! Tests `Core::node_ids` / `node_count` / `kind_of` / `deps_of` /
//! `is_terminal` / `is_dirty` / `same_dispatcher`. These back the
//! `graphrefly-graph` `describe()` / `observe()` work.

mod common;
use common::{TestRuntime, TestValue};
use graphrefly_core::{NodeId, NodeKind, TerminalKind};

#[test]
fn node_ids_returns_all_registered_node_ids() {
    let rt = TestRuntime::new();
    let s1 = rt.state(None);
    let s2 = rt.state(None);
    let d = rt.derived(&[s1.id, s2.id], |_| Some(TestValue::Int(1)));
    let mut ids = rt.core.node_ids();
    ids.sort();
    let mut expected = vec![s1.id, s2.id, d];
    expected.sort();
    assert_eq!(ids, expected);
}

#[test]
fn node_count_matches_registrations() {
    let rt = TestRuntime::new();
    assert_eq!(rt.core.node_count(), 0);
    let _s1 = rt.state(None);
    let _s2 = rt.state(None);
    assert_eq!(rt.core.node_count(), 2);
    let _d = rt.derived(&[_s1.id, _s2.id], |_| Some(TestValue::Int(1)));
    assert_eq!(rt.core.node_count(), 3);
}

#[test]
fn kind_of_distinguishes_state_derived_dynamic() {
    let rt = TestRuntime::new();
    let s = rt.state(None);
    let d = rt.derived(&[s.id], |_| Some(TestValue::Int(1)));
    let dy = rt.dynamic(&[s.id], |_| (Some(TestValue::Int(1)), Some(vec![0])));
    assert_eq!(rt.core.kind_of(s.id), Some(NodeKind::State));
    assert_eq!(rt.core.kind_of(d), Some(NodeKind::Derived));
    assert_eq!(rt.core.kind_of(dy), Some(NodeKind::Dynamic));
    assert_eq!(rt.core.kind_of(NodeId::new(99_999)), None);
}

#[test]
fn deps_of_returns_declaration_order() {
    let rt = TestRuntime::new();
    let a = rt.state(None);
    let b = rt.state(None);
    let c = rt.state(None);
    let d = rt.derived(&[c.id, a.id, b.id], |_| Some(TestValue::Int(0)));
    assert_eq!(rt.core.deps_of(d), vec![c.id, a.id, b.id]);
    // State node has no deps.
    assert!(rt.core.deps_of(a.id).is_empty());
    // Unknown node returns empty.
    assert!(rt.core.deps_of(NodeId::new(99_999)).is_empty());
}

#[test]
fn is_terminal_reports_complete_and_error() {
    let rt = TestRuntime::new();
    let s_clean = rt.state(None);
    let s_complete = rt.state(None);
    let s_error = rt.state(None);
    rt.core.complete(s_complete.id);
    let err_h = rt.binding.intern(TestValue::Str("boom".into()));
    rt.core.error(s_error.id, err_h);
    assert_eq!(rt.core.is_terminal(s_clean.id), None);
    assert_eq!(
        rt.core.is_terminal(s_complete.id),
        Some(TerminalKind::Complete)
    );
    matches!(
        rt.core.is_terminal(s_error.id),
        Some(TerminalKind::Error(_))
    );
    // Unknown id is None.
    assert_eq!(rt.core.is_terminal(NodeId::new(99_999)), None);
}

#[test]
fn is_dirty_false_for_unknown_id() {
    let rt = TestRuntime::new();
    assert!(!rt.core.is_dirty(NodeId::new(99_999)));
}

#[test]
fn same_dispatcher_true_for_same_core_false_for_independents() {
    // β/D242: `Core` is move-only; "same dispatcher" is now tested via
    // two borrows of the one owned Core (clones no longer exist).
    let rt = TestRuntime::new();
    let core1 = &rt.core;
    let core2 = &rt.core;
    assert!(core1.same_dispatcher(core2));

    // Independent Core (different binding) — explicit Core::new
    // bypasses TestRuntime; simplest is to compare against another
    // freshly-constructed runtime.
    let rt2 = TestRuntime::new();
    assert!(!rt.core.same_dispatcher(&rt2.core));
}

#[test]
fn meta_companions_of_returns_registered_companions() {
    let rt = TestRuntime::new();
    let parent = rt.state(None);
    let comp_a = rt.state(None);
    let comp_b = rt.state(None);
    rt.core.add_meta_companion(parent.id, comp_a.id);
    rt.core.add_meta_companion(parent.id, comp_b.id);
    let companions = rt.core.meta_companions_of(parent.id);
    assert_eq!(companions.len(), 2);
    assert!(companions.contains(&comp_a.id));
    assert!(companions.contains(&comp_b.id));
    // Empty for nodes with no companions.
    assert!(rt.core.meta_companions_of(comp_a.id).is_empty());
    // Empty for unknown ids.
    assert!(rt.core.meta_companions_of(NodeId::new(99_999)).is_empty());
}

#[test]
fn helpers_are_lock_safe_under_active_subscribers() {
    // Smoke test that the inspection helpers don't deadlock with
    // ongoing subscriptions / wave activity. Subscribe a sink, fire
    // emit, then call every helper.
    let rt = TestRuntime::new();
    let s = rt.state(None);
    let _rec = rt.subscribe_recorder(s.id);
    s.set(TestValue::Int(1));
    let _ = rt.core.node_ids();
    let _ = rt.core.node_count();
    let _ = rt.core.kind_of(s.id);
    let _ = rt.core.deps_of(s.id);
    let _ = rt.core.is_terminal(s.id);
    let _ = rt.core.is_dirty(s.id);
}
