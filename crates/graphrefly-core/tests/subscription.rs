//! [`Subscription`] RAII semantics — §10.12 of the rust-port session doc.
//!
//! Verifies the public API surface change from manual `unsubscribe(node, id)`
//! to a `Subscription` handle that deregisters its sink on Drop. The Drop
//! is silent if the Core has been dropped (no panic-on-shutdown).

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

mod common;
use common::{TestRuntime, TestValue};

/// Helper: subscribe a sink that just counts DATA messages. Returns the
/// counter (Arc) and the Subscription handle.
fn data_counter(
    rt: &TestRuntime,
    node_id: graphrefly_core::NodeId,
) -> (Arc<AtomicU32>, graphrefly_core::Subscription) {
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
fn dropping_subscription_unsubscribes_sink() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let (counter, sub) = data_counter(&rt, s.id);

    // Push-on-subscribe delivered initial DATA (cache=0 at subscribe time).
    let initial = counter.load(Ordering::SeqCst);
    assert_eq!(initial, 1, "push-on-subscribe should deliver one DATA");

    s.set(TestValue::Int(1));
    assert_eq!(counter.load(Ordering::SeqCst), 2);

    // Drop the subscription — sink should no longer receive DATA.
    drop(sub);
    s.set(TestValue::Int(2));
    s.set(TestValue::Int(3));
    assert_eq!(
        counter.load(Ordering::SeqCst),
        2,
        "post-drop emissions must not reach the sink"
    );
}

#[test]
fn implicit_drop_at_scope_end_unsubscribes() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let counter = Arc::new(AtomicU32::new(0));
    {
        let counter_inner = counter.clone();
        let sink: graphrefly_core::Sink = Arc::new(move |msgs: &[graphrefly_core::Message]| {
            for m in msgs {
                if matches!(m, graphrefly_core::Message::Data(_)) {
                    counter_inner.fetch_add(1, Ordering::SeqCst);
                }
            }
        });
        let _sub = rt.core.subscribe(s.id, sink);
        // _sub is in scope here; subscription is live.
        s.set(TestValue::Int(1));
    }
    // Scope exited — _sub dropped — subscription deregistered.
    let count_at_scope_exit = counter.load(Ordering::SeqCst);
    s.set(TestValue::Int(2));
    assert_eq!(
        counter.load(Ordering::SeqCst),
        count_at_scope_exit,
        "post-scope-exit emissions must not reach the sink"
    );
}

#[test]
fn subscription_holds_node_id() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let sink: graphrefly_core::Sink = Arc::new(|_: &[graphrefly_core::Message]| {});
    let sub = rt.core.subscribe(s.id, sink);
    assert_eq!(sub.node_id(), s.id);
}

#[test]
fn dropping_subscription_after_core_drop_is_silent_no_op() {
    // Subscribe, then drop the runtime (which drops Core), then drop the
    // subscription. The Subscription's Drop tries to upgrade a Weak; if the
    // Core is gone, the Drop is a silent no-op and we don't panic.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let sink: graphrefly_core::Sink = Arc::new(|_: &[graphrefly_core::Message]| {});
    let sub = rt.core.subscribe(s.id, sink);

    // Drop the runtime first (Core goes away).
    drop(rt);

    // Now drop the subscription. Should be silent.
    drop(sub);
    // If we got here without panic, the test passes.
}

#[test]
fn multiple_subscriptions_to_same_node_are_independent() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let (c1, sub1) = data_counter(&rt, s.id);
    let (c2, sub2) = data_counter(&rt, s.id);

    // Both got push-on-subscribe.
    assert_eq!(c1.load(Ordering::SeqCst), 1);
    assert_eq!(c2.load(Ordering::SeqCst), 1);

    s.set(TestValue::Int(1));
    assert_eq!(c1.load(Ordering::SeqCst), 2);
    assert_eq!(c2.load(Ordering::SeqCst), 2);

    // Drop only sub1.
    drop(sub1);
    s.set(TestValue::Int(2));
    assert_eq!(c1.load(Ordering::SeqCst), 2, "sub1 deregistered");
    assert_eq!(c2.load(Ordering::SeqCst), 3, "sub2 still active");

    drop(sub2);
}

#[test]
fn subscription_is_send_and_sync() {
    // Already enforced statically in node.rs via a const fn assertion;
    // this test re-confirms at the integration test layer so a regression
    // in the inner module would still surface here.
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<graphrefly_core::Subscription>();
}

#[test]
fn moving_subscription_across_threads_works() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let (counter, sub) = data_counter(&rt, s.id);
    s.set(TestValue::Int(1));
    let count_before_move = counter.load(Ordering::SeqCst);

    // Move the subscription to another thread; drop it there.
    std::thread::spawn(move || {
        drop(sub);
    })
    .join()
    .expect("worker thread");

    // After remote-thread drop, sink is deregistered.
    s.set(TestValue::Int(2));
    assert_eq!(counter.load(Ordering::SeqCst), count_before_move);
}
