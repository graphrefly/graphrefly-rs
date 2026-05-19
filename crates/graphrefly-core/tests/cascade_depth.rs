//! Slice A-bigger — cascade-recursion → iterative walk stress tests.
//!
//! Verifies that the cascade methods (`terminate_node`, `teardown_inner`,
//! `invalidate_inner`) no longer recurse into the OS thread stack and can
//! handle deep linear chains without overflow. Pre-Slice-A-bigger these
//! recursed once per cascade hop; chains beyond a few thousand nodes
//! crashed the test binary.
//!
//! Chain depth: 5000 (well past the typical 1000-frame Rust default
//! stack limit; further conservative than the 10k mentioned in
//! `porting-deferred.md` so the test runs quickly).

mod common;

use common::{TestRuntime, TestValue};

const CHAIN_LEN: usize = 5_000;

fn build_linear_chain(rt: &TestRuntime, len: usize) -> Vec<graphrefly_core::NodeId> {
    let mut ids = Vec::with_capacity(len);
    let s = rt.state(Some(TestValue::Int(0)));
    ids.push(s.id);
    let mut prev = s.id;
    for _ in 1..len {
        let next = rt.derived(&[prev], |deps| match &deps[0] {
            TestValue::Int(n) => Some(TestValue::Int(*n)),
            _ => None,
        });
        ids.push(next);
        prev = next;
    }
    ids
}

#[test]
fn complete_cascades_through_5000_node_chain_without_stack_overflow() {
    let rt = TestRuntime::new();
    let ids = build_linear_chain(&rt, CHAIN_LEN);
    // Subscribe at the leaf — activation walks the whole chain (also
    // iteratively, via deliver_data_to_consumer, NOT recursive cascade).
    let leaf_rec = rt.subscribe_recorder(*ids.last().expect("non-empty chain"));
    let baseline = leaf_rec.snapshot().len();
    // Complete the head → cascade COMPLETE through 5000 nodes.
    rt.core().complete(ids[0]);
    let post = leaf_rec.snapshot();
    assert!(
        post.iter()
            .skip(baseline)
            .any(|e| matches!(e, common::RecordedEvent::Complete)),
        "leaf should observe COMPLETE after cascade through 5000-node chain"
    );
}

#[test]
fn teardown_cascades_through_5000_node_chain_without_stack_overflow() {
    let rt = TestRuntime::new();
    let ids = build_linear_chain(&rt, CHAIN_LEN);
    let leaf_rec = rt.subscribe_recorder(*ids.last().expect("non-empty chain"));
    let baseline = leaf_rec.snapshot().len();
    // Teardown the head → cascade TEARDOWN (with auto-COMPLETE prepend)
    // through 5000 nodes.
    rt.core().teardown(ids[0]);
    let post = leaf_rec.snapshot();
    let new = &post[baseline..];
    assert!(
        new.iter()
            .any(|e| matches!(e, common::RecordedEvent::Teardown)),
        "leaf should observe TEARDOWN after cascade through 5000-node chain"
    );
    assert!(
        new.iter()
            .any(|e| matches!(e, common::RecordedEvent::Complete)),
        "leaf should observe COMPLETE (auto-prepended by teardown_inner) after cascade"
    );
}

#[test]
fn invalidate_cascades_through_5000_node_chain_without_stack_overflow() {
    let rt = TestRuntime::new();
    let ids = build_linear_chain(&rt, CHAIN_LEN);
    let leaf_rec = rt.subscribe_recorder(*ids.last().expect("non-empty chain"));
    let baseline = leaf_rec.snapshot().len();
    // Invalidate the head → cache-clear cascade through 5000 nodes.
    rt.core().invalidate(ids[0]);
    let post = leaf_rec.snapshot();
    let new = &post[baseline..];
    assert!(
        new.iter()
            .any(|e| matches!(e, common::RecordedEvent::Invalidate)),
        "leaf should observe INVALIDATE after cascade through 5000-node chain"
    );
    // After invalidate, every node's cache should be NO_HANDLE.
    for &id in &ids {
        assert_eq!(
            rt.core().cache_of(id),
            graphrefly_core::NO_HANDLE,
            "node {id:?} cache should be cleared after invalidate cascade"
        );
    }
}

#[test]
fn error_cascades_through_5000_node_chain_without_stack_overflow() {
    let rt = TestRuntime::new();
    let ids = build_linear_chain(&rt, CHAIN_LEN);
    let leaf_rec = rt.subscribe_recorder(*ids.last().expect("non-empty chain"));
    let baseline = leaf_rec.snapshot().len();
    // ERROR at the head → cascade through 5000 nodes (Lock 2.B: ERROR
    // dominates COMPLETE in auto-cascade gating; first ERROR seen wins).
    let err = rt.binding.intern(TestValue::Str("oops".to_string()));
    rt.core().error(ids[0], err);
    let post = leaf_rec.snapshot();
    let new = &post[baseline..];
    assert!(
        new.iter()
            .any(|e| matches!(e, common::RecordedEvent::Error(_))),
        "leaf should observe ERROR after cascade through 5000-node chain"
    );
}
