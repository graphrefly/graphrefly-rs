//! Subscription lifecycle — S2b/β owner-invoked contract (D225/D241).
//!
//! The core RAII `Subscription` (drop-deregisters, `Weak`-upgrade
//! silent-no-op, `Send + Sync` cross-thread move) is **retired** under
//! the actor model: a parameterless `Drop` cannot reach a relocating
//! owned `Core` (D225). `Core::subscribe` now returns a plain `Copy`
//! `SubscriptionId`; deregistration is the explicit owner-invoked
//! `Core::unsubscribe(node, id)` carrying the exact lock-released
//! lifecycle chain. RAII convenience is a binding-layer concern
//! (binding/embedder holds its Core on its affinity worker and calls
//! `core.unsubscribe` from its wrapper's `Drop`); the `TestRuntime`
//! harness models that (tracks subs, tears down on its own `Drop`).
//! This file verifies the replacement contract.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

mod common;
use common::{TestRuntime, TestValue};

/// Subscribe a sink that counts DATA messages. Returns the counter and
/// the `SubscriptionId` (β: a plain `Copy` id, not an RAII handle).
fn data_counter(
    rt: &TestRuntime,
    node_id: graphrefly_core::NodeId,
) -> (Arc<AtomicU32>, graphrefly_core::SubscriptionId) {
    let counter = Arc::new(AtomicU32::new(0));
    let counter_clone = counter.clone();
    let sink: graphrefly_core::Sink = Arc::new(move |msgs: &[graphrefly_core::Message]| {
        for m in msgs {
            if matches!(m, graphrefly_core::Message::Data(_)) {
                counter_clone.fetch_add(1, Ordering::SeqCst);
            }
        }
    });
    let sub = rt.core.subscribe(node_id, sink);
    (counter, sub)
}

#[test]
fn unsubscribe_deregisters_sink() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let (counter, sub) = data_counter(&rt, s.id);

    // Push-on-subscribe delivered initial DATA (cache=0 at subscribe).
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "push-on-subscribe should deliver one DATA"
    );

    // Owner-invoked unsubscribe (β/D225) — sink deregistered.
    rt.core.unsubscribe(s.id, sub);

    s.set(TestValue::Int(1));
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "no DATA after unsubscribe"
    );
}

#[test]
fn double_unsubscribe_is_safe() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let (_counter, sub) = data_counter(&rt, s.id);
    rt.core.unsubscribe(s.id, sub);
    // Idempotent: a second unsubscribe of the same id is a no-op, not
    // a panic (monotonic-never-recycled ids; F4 invariant).
    rt.core.unsubscribe(s.id, sub);
    s.set(TestValue::Int(2));
}

#[test]
fn subscription_id_is_copy_send_sync() {
    // β/D225: the handle is now a plain `SubscriptionId` newtype —
    // `Copy + Send + Sync` (no `Weak<C>`/RAII). Replaces the retired
    // `Subscription: Send + Sync` static assertion.
    fn assert_copy<T: Copy>() {}
    fn assert_send_sync<T: Send + Sync>() {}
    assert_copy::<graphrefly_core::SubscriptionId>();
    assert_send_sync::<graphrefly_core::SubscriptionId>();
}

#[test]
fn multiple_subscriptions_to_same_node_are_independent() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let (c1, sub1) = data_counter(&rt, s.id);
    let (c2, sub2) = data_counter(&rt, s.id);

    assert_eq!(c1.load(Ordering::SeqCst), 1);
    assert_eq!(c2.load(Ordering::SeqCst), 1);

    // Unsubscribing one leaves the other live.
    rt.core.unsubscribe(s.id, sub1);
    s.set(TestValue::Int(1));
    assert_eq!(c1.load(Ordering::SeqCst), 1, "sub1 detached");
    assert_eq!(c2.load(Ordering::SeqCst), 2, "sub2 still live");

    rt.core.unsubscribe(s.id, sub2);
}

#[test]
fn runtime_drop_tears_down_remaining_subscriptions() {
    // β/D241: the binding-layer owner (here `TestRuntime`) holds the
    // Core on its thread and unsubscribes tracked subs on its own
    // `Drop` — the RAII convenience that replaces core-level RAII.
    // Smoke: leaving a sub registered must not panic on runtime drop.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let _rec = rt.subscribe_recorder(s.id);
    s.set(TestValue::Int(1));
    drop(rt); // tracked-sub teardown on the owner thread — no panic.
}
