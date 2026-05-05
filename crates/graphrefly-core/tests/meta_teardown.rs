//! Meta TEARDOWN ordering (R1.3.9.d).
//!
//! Meta companion nodes attached to a parent must tear down BEFORE the
//! parent's own TEARDOWN wire emission. This ordering is load-bearing for
//! audit / status / inspection sidecars that subscribe to parent state.

use std::sync::{Arc, Mutex};

mod common;
use common::{RecordedEvent, TestRuntime, TestValue};

/// Track the order in which TEARDOWN events arrive across multiple sinks
/// via a shared sequence counter.
struct OrderTracker {
    next: Arc<Mutex<u32>>,
    /// Map node label → sequence number when its TEARDOWN was observed.
    events: Arc<Mutex<Vec<(String, u32)>>>,
}

impl OrderTracker {
    fn new() -> Self {
        Self {
            next: Arc::new(Mutex::new(0)),
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn record_sink(
        &self,
        label: &'static str,
        binding: Arc<common::TestBinding>,
    ) -> graphrefly_core::Sink {
        let next = self.next.clone();
        let events = self.events.clone();
        Arc::new(move |msgs: &[graphrefly_core::Message]| {
            for msg in msgs {
                if matches!(msg, graphrefly_core::Message::Teardown) {
                    let seq = {
                        let mut n = next.lock().unwrap();
                        *n += 1;
                        *n
                    };
                    events.lock().unwrap().push((label.to_string(), seq));
                }
                // Also need to deref Data handles to keep refcounts balanced
                // across the sink semantics — but for this test we only care
                // about TEARDOWN order, so a no-op on other messages is fine.
                if let graphrefly_core::Message::Data(h) = msg {
                    let _ = binding.deref(*h); // smoke-deref
                }
            }
        })
    }

    fn order(&self) -> Vec<(String, u32)> {
        self.events.lock().unwrap().clone()
    }
}

#[test]
fn single_meta_companion_tears_down_before_parent() {
    let rt = TestRuntime::new();
    let parent = rt.state(Some(TestValue::Int(1)));
    let meta = rt.state(Some(TestValue::Int(99)));
    rt.core.add_meta_companion(parent.id, meta.id);

    let tracker = OrderTracker::new();
    let parent_sub = rt
        .core
        .subscribe(parent.id, tracker.record_sink("parent", rt.binding.clone()));
    let meta_sub = rt
        .core
        .subscribe(meta.id, tracker.record_sink("meta", rt.binding.clone()));

    rt.core.teardown(parent.id);

    let order = tracker.order();
    assert_eq!(order.len(), 2, "both subscribers see TEARDOWN");
    assert_eq!(order[0].0, "meta", "meta tears down before parent");
    assert_eq!(order[1].0, "parent");

    drop(parent_sub);
    drop(meta_sub);
}

#[test]
fn multiple_meta_companions_all_precede_parent() {
    let rt = TestRuntime::new();
    let parent = rt.state(Some(TestValue::Int(1)));
    let m1 = rt.state(Some(TestValue::Int(2)));
    let m2 = rt.state(Some(TestValue::Int(3)));
    let m3 = rt.state(Some(TestValue::Int(4)));
    rt.core.add_meta_companion(parent.id, m1.id);
    rt.core.add_meta_companion(parent.id, m2.id);
    rt.core.add_meta_companion(parent.id, m3.id);

    let tracker = OrderTracker::new();
    let _parent_sub = rt
        .core
        .subscribe(parent.id, tracker.record_sink("parent", rt.binding.clone()));
    let _m1_sub = rt
        .core
        .subscribe(m1.id, tracker.record_sink("m1", rt.binding.clone()));
    let _m2_sub = rt
        .core
        .subscribe(m2.id, tracker.record_sink("m2", rt.binding.clone()));
    let _m3_sub = rt
        .core
        .subscribe(m3.id, tracker.record_sink("m3", rt.binding.clone()));

    rt.core.teardown(parent.id);

    let order = tracker.order();
    assert_eq!(order.len(), 4);
    let parent_seq = order
        .iter()
        .find(|(label, _)| label == "parent")
        .map(|(_, seq)| *seq)
        .unwrap();
    for label in ["m1", "m2", "m3"] {
        let meta_seq = order
            .iter()
            .find(|(l, _)| l == label)
            .map(|(_, seq)| *seq)
            .unwrap();
        assert!(
            meta_seq < parent_seq,
            "meta {label} (seq {meta_seq}) must tear down before parent (seq {parent_seq})"
        );
    }
}

#[test]
fn nested_meta_companion_tears_down_first() {
    // parent → meta1 → meta2  (meta1 has its own meta2 companion)
    // Order on parent.teardown(): meta2, meta1, parent.
    let rt = TestRuntime::new();
    let parent = rt.state(Some(TestValue::Int(1)));
    let meta1 = rt.state(Some(TestValue::Int(2)));
    let meta2 = rt.state(Some(TestValue::Int(3)));
    rt.core.add_meta_companion(parent.id, meta1.id);
    rt.core.add_meta_companion(meta1.id, meta2.id);

    let tracker = OrderTracker::new();
    let _p_sub = rt
        .core
        .subscribe(parent.id, tracker.record_sink("parent", rt.binding.clone()));
    let _m1_sub = rt
        .core
        .subscribe(meta1.id, tracker.record_sink("meta1", rt.binding.clone()));
    let _m2_sub = rt
        .core
        .subscribe(meta2.id, tracker.record_sink("meta2", rt.binding.clone()));

    rt.core.teardown(parent.id);

    let order = tracker.order();
    assert_eq!(order.len(), 3);
    let labels: Vec<String> = order.into_iter().map(|(l, _)| l).collect();
    assert_eq!(labels, vec!["meta2", "meta1", "parent"]);
}

#[test]
fn duplicate_add_meta_companion_is_idempotent() {
    let rt = TestRuntime::new();
    let parent = rt.state(Some(TestValue::Int(1)));
    let meta = rt.state(Some(TestValue::Int(2)));
    rt.core.add_meta_companion(parent.id, meta.id);
    rt.core.add_meta_companion(parent.id, meta.id);
    rt.core.add_meta_companion(parent.id, meta.id);

    let rec_meta = rt.subscribe_recorder(meta.id);
    let _rec_parent = rt.subscribe_recorder(parent.id);

    rt.core.teardown(parent.id);

    let teardown_count = rec_meta
        .snapshot()
        .iter()
        .filter(|e| matches!(e, RecordedEvent::Teardown))
        .count();
    assert_eq!(teardown_count, 1, "meta receives exactly one TEARDOWN");
}

#[test]
fn meta_complete_cascades_independently_of_parent() {
    // Meta companion ordering is for TEARDOWN, NOT for COMPLETE/ERROR.
    // parent.complete() does NOT auto-complete meta companions; they stay
    // live until they themselves complete or until parent.teardown().
    let rt = TestRuntime::new();
    let parent = rt.state(Some(TestValue::Int(1)));
    let meta = rt.state(Some(TestValue::Int(2)));
    rt.core.add_meta_companion(parent.id, meta.id);

    let rec_meta = rt.subscribe_recorder(meta.id);
    rt.core.complete(parent.id);

    let meta_completes = rec_meta
        .snapshot()
        .iter()
        .filter(|e| matches!(e, RecordedEvent::Complete))
        .count();
    assert_eq!(
        meta_completes, 0,
        "parent.complete() does NOT cascade COMPLETE to meta companions"
    );
}

#[test]
#[should_panic(expected = "node cannot be its own meta companion")]
fn self_meta_companion_panics() {
    let rt = TestRuntime::new();
    let n = rt.state(Some(TestValue::Int(1)));
    rt.core.add_meta_companion(n.id, n.id);
}

#[test]
#[should_panic(expected = "unknown parent")]
fn unknown_parent_meta_companion_panics() {
    let rt = TestRuntime::new();
    let bogus = graphrefly_core::NodeId::new(99_999);
    let real = rt.state(Some(TestValue::Int(1)));
    rt.core.add_meta_companion(bogus, real.id);
}

#[test]
#[should_panic(expected = "unknown companion")]
fn unknown_companion_meta_companion_panics() {
    let rt = TestRuntime::new();
    let real = rt.state(Some(TestValue::Int(1)));
    let bogus = graphrefly_core::NodeId::new(99_999);
    rt.core.add_meta_companion(real.id, bogus);
}
