//! Higher-order operator tests (Slice E, D044) — switchMap / exhaustMap
//! / concatMap / mergeMap.
//!
//! Tests use the realistic CACHED-inner pattern (`state(Some(v))`)
//! enabled by the Slice E lock-released handshake rework. Subscribing
//! to a cached state node fires `[Start, Data(cache)]` lock-released
//! with `wave_owner` held; the inner sink's `Core::emit(producer_id,
//! h)` re-enters Core via `wave_owner`'s reentrant mutex (same-thread
//! pass-through), so cached inner deliveries flow through to the
//! producer's emit + downstream sinks transparently.

mod common;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use common::{OpRuntime, RecordedEvent, TestValue};
use graphrefly_core::NodeId;
use graphrefly_operators::{
    concat_map, exhaust_map, merge_map, merge_map_with_concurrency, switch_map,
};

/// Helper: build a project closure that returns the next inner node
/// from a shared FIFO queue, panicking if the queue is empty.
fn make_seq_project(
    inners: Arc<Mutex<Vec<NodeId>>>,
) -> Box<dyn Fn(graphrefly_core::HandleId) -> NodeId + Send + Sync> {
    Box::new(move |_h| {
        let mut v = inners.lock().unwrap();
        assert!(
            !v.is_empty(),
            "project closure ran out of pre-registered inners"
        );
        v.remove(0)
    })
}

// =====================================================================
// switch_map
// =====================================================================

#[test]
fn switch_map_emits_inner_data_from_cached_inner() {
    // Cached-inner pattern: inner1 starts with a pre-populated cache;
    // the subscribe fires [Start, Data(10)] lock-released; the inner
    // sink re-enters core.emit on the producer to forward 10.
    let rt = OpRuntime::new();
    let outer = rt.state_int(None);
    let inner1 = rt.state_int(Some(10));
    let inners = Arc::new(Mutex::new(vec![inner1]));
    let m = switch_map(rt.core(), &rt.ho_binding, outer, make_seq_project(inners));
    let rec = rt.subscribe_recorder(m);

    rt.emit_int(outer, 1);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.
    assert_eq!(rec.data_values(), vec![TestValue::Int(10)]);
}

#[test]
fn switch_map_cancels_previous_inner_on_new_outer_data() {
    let rt = OpRuntime::new();
    let outer = rt.state_int(None);
    let inner1 = rt.state_int(Some(100));
    let inner2 = rt.state_int(Some(200));
    let inners = Arc::new(Mutex::new(vec![inner1, inner2]));
    let m = switch_map(rt.core(), &rt.ho_binding, outer, make_seq_project(inners));
    let rec = rt.subscribe_recorder(m);

    rt.emit_int(outer, 1); // -> subscribe inner1 (cached 100)
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.
    assert_eq!(rec.data_values(), vec![TestValue::Int(100)]);

    rt.emit_int(outer, 2); // -> cancel inner1, subscribe inner2 (cached 200)
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.
    assert_eq!(
        rec.data_values(),
        vec![TestValue::Int(100), TestValue::Int(200)]
    );

    rt.emit_int(inner1, 999); // ignored — canceled
    rt.emit_int(inner2, 222);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.
    assert_eq!(
        rec.data_values(),
        vec![
            TestValue::Int(100),
            TestValue::Int(200),
            TestValue::Int(222)
        ]
    );
}

#[test]
fn switch_map_completes_when_outer_completes_with_no_active_inner() {
    let rt = OpRuntime::new();
    let outer = rt.state_int(None);
    let inners: Arc<Mutex<Vec<NodeId>>> = Arc::new(Mutex::new(vec![]));
    let m = switch_map(rt.core(), &rt.ho_binding, outer, make_seq_project(inners));
    let rec = rt.subscribe_recorder(m);

    rt.core().complete(outer);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.
    let events = rec.events();
    assert!(
        events.iter().any(|e| matches!(e, RecordedEvent::Complete)),
        "switch_map should complete when outer completes with no active inner; got {events:?}"
    );
}

#[test]
fn switch_map_completes_when_inner_completes_after_outer_done() {
    let rt = OpRuntime::new();
    let outer = rt.state_int(None);
    let inner1 = rt.state_int(None);
    let inners = Arc::new(Mutex::new(vec![inner1]));
    let m = switch_map(rt.core(), &rt.ho_binding, outer, make_seq_project(inners));
    let rec = rt.subscribe_recorder(m);

    rt.emit_int(outer, 1);
    rt.core().complete(outer); // outer done, inner1 still active
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.
    let pre = rec.events();
    assert!(
        !pre.iter().any(|e| matches!(e, RecordedEvent::Complete)),
        "switch_map should NOT complete while inner is still active"
    );

    rt.core().complete(inner1);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.
    let post = rec.events();
    assert!(
        post.iter().any(|e| matches!(e, RecordedEvent::Complete)),
        "switch_map should complete after inner completes following outer COMPLETE"
    );
}

#[test]
fn switch_map_propagates_outer_error() {
    let rt = OpRuntime::new();
    let outer = rt.state_int(None);
    let inners: Arc<Mutex<Vec<NodeId>>> = Arc::new(Mutex::new(vec![]));
    let m = switch_map(rt.core(), &rt.ho_binding, outer, make_seq_project(inners));
    let rec = rt.subscribe_recorder(m);

    let err_h = rt.intern(TestValue::Str("boom".into()));
    rt.core().error(outer, err_h);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    let events = rec.events();
    assert!(events.iter().any(|e| matches!(e, RecordedEvent::Error(_))));
}

/// Slice E /qa P1 regression: prior version unconditionally released
/// `outer_h` in phase 2 even when phase 1 skipped the retain because
/// `Error` arrived in the same batch. That caused a binding-side
/// refcount underflow on the chosen handle.
#[test]
fn switch_map_data_then_error_in_same_batch_does_not_underflow_handle() {
    let rt = OpRuntime::new();
    let outer = rt.state_int(None);
    let inner1 = rt.state_int(Some(10));
    let inners = Arc::new(Mutex::new(vec![inner1]));
    let m = switch_map(rt.core(), &rt.ho_binding, outer, make_seq_project(inners));
    let _rec = rt.subscribe_recorder(m);

    // Hold a diagnostic intern share on the DATA handle so we can
    // observe its refcount across the batch without races.
    let data_h = rt.intern_int(7);
    let err_h = rt.intern(TestValue::Str("boom".into()));

    let pre_data_rc = rt.binding.refcount_of(data_h);

    rt.core().batch(|| {
        rt.core().emit(outer, data_h);
        rt.core().error(outer, err_h);
    });

    // After the batch:
    // - `data_h`'s refcount must equal the pre-batch count (we still
    //   hold our diagnostic intern share). Pre-fix, an unbalanced
    //   release would drop it below the diagnostic count.
    let post_data_rc = rt.binding.refcount_of(data_h);
    assert_eq!(
        post_data_rc, pre_data_rc,
        "switch_map [Data, Error] same-batch must not underflow data handle's refcount"
    );
}

/// Slice E /qa P3 regression: build_inner_sink forwards inner
/// INVALIDATE through to the producer via Core::invalidate. Subscribers
/// see INVALIDATE flowing through the higher-order operator.
#[test]
fn switch_map_forwards_inner_invalidate_to_producer() {
    let rt = OpRuntime::new();
    let outer = rt.state_int(None);
    let inner1 = rt.state_int(Some(10));
    let inners = Arc::new(Mutex::new(vec![inner1]));
    let m = switch_map(rt.core(), &rt.ho_binding, outer, make_seq_project(inners));
    let rec = rt.subscribe_recorder(m);

    rt.emit_int(outer, 1);
    rt.settle(); // D246: pump deferred inner-subscribe + Data owner-side.
                 // Subscribed to inner1 (cached 10); rec saw Data(10).

    rt.core().invalidate(inner1);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.
                 // Inner emits [INVALIDATE]; build_inner_sink forwards to producer
                 // via Core::invalidate(producer_id); downstream rec sees Invalidate.
    let events = rec.events();
    let saw_invalidate = events
        .iter()
        .any(|e| matches!(e, RecordedEvent::Invalidate));
    assert!(
        saw_invalidate,
        "expected INVALIDATE forwarded from inner to producer; got {events:?}"
    );
}

// =====================================================================
// exhaust_map
// =====================================================================

#[test]
fn exhaust_map_drops_outer_data_while_inner_active() {
    let rt = OpRuntime::new();
    let outer = rt.state_int(None);
    let inner1 = rt.state_int(None);
    let inners = Arc::new(Mutex::new(vec![inner1]));
    let project_count = Arc::new(AtomicU32::new(0));
    let counter = project_count.clone();
    let inners_for_proj = inners.clone();
    let project = Box::new(move |_h| {
        counter.fetch_add(1, Ordering::SeqCst);
        let mut v = inners_for_proj.lock().unwrap();
        v.remove(0)
    });
    let m = exhaust_map(rt.core(), &rt.ho_binding, outer, project);
    let rec = rt.subscribe_recorder(m);

    rt.emit_int(outer, 1); // -> inner1 active
    rt.emit_int(outer, 2); // dropped (inner1 active; project NOT called)
    rt.emit_int(outer, 3); // dropped
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    assert_eq!(
        project_count.load(Ordering::SeqCst),
        1,
        "project should be called exactly once (first DATA wins)"
    );

    rt.emit_int(inner1, 100);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.
    assert_eq!(rec.data_values(), vec![TestValue::Int(100)]);
}

#[test]
fn exhaust_map_accepts_new_outer_data_after_inner_completes() {
    let rt = OpRuntime::new();
    let outer = rt.state_int(None);
    let inner1 = rt.state_int(None);
    let inner2 = rt.state_int(None);
    let inners = Arc::new(Mutex::new(vec![inner1, inner2]));
    let m = exhaust_map(rt.core(), &rt.ho_binding, outer, make_seq_project(inners));
    let rec = rt.subscribe_recorder(m);

    rt.emit_int(outer, 1);
    rt.emit_int(inner1, 100);
    rt.core().complete(inner1); // window closed
    rt.emit_int(outer, 2); // window reopened -> inner2
    rt.emit_int(inner2, 200);
    assert_eq!(
        rec.data_values(),
        vec![TestValue::Int(100), TestValue::Int(200)]
    );
}

// =====================================================================
// concat_map (= merge_map_with_concurrency(Some(1)))
// =====================================================================

#[test]
fn concat_map_processes_inners_sequentially() {
    let rt = OpRuntime::new();
    let outer = rt.state_int(None);
    let inner1 = rt.state_int(None);
    let inner2 = rt.state_int(None);
    let inner3 = rt.state_int(None);
    let inners = Arc::new(Mutex::new(vec![inner1, inner2, inner3]));
    let project_count = Arc::new(AtomicU32::new(0));
    let counter = project_count.clone();
    let inners_for_proj = inners.clone();
    let project = Box::new(move |_h| {
        counter.fetch_add(1, Ordering::SeqCst);
        let mut v = inners_for_proj.lock().unwrap();
        v.remove(0)
    });
    let m = concat_map(rt.core(), &rt.ho_binding, outer, project);
    let rec = rt.subscribe_recorder(m);

    rt.emit_int(outer, 1);
    rt.emit_int(outer, 2);
    rt.emit_int(outer, 3);
    // Only inner1 spawned; 2/3 queued.
    assert_eq!(project_count.load(Ordering::SeqCst), 1);

    rt.emit_int(inner1, 10);
    rt.core().complete(inner1); // -> dequeue, project inner2
    assert_eq!(project_count.load(Ordering::SeqCst), 2);

    rt.emit_int(inner2, 20);
    rt.core().complete(inner2); // -> dequeue, project inner3
    assert_eq!(project_count.load(Ordering::SeqCst), 3);

    rt.emit_int(inner3, 30);
    assert_eq!(
        rec.data_values(),
        vec![TestValue::Int(10), TestValue::Int(20), TestValue::Int(30)]
    );
}

#[test]
fn concat_map_completes_after_outer_done_and_queue_drains() {
    let rt = OpRuntime::new();
    let outer = rt.state_int(None);
    let inner1 = rt.state_int(None);
    let inner2 = rt.state_int(None);
    let inners = Arc::new(Mutex::new(vec![inner1, inner2]));
    let m = concat_map(rt.core(), &rt.ho_binding, outer, make_seq_project(inners));
    let rec = rt.subscribe_recorder(m);

    rt.emit_int(outer, 1);
    rt.emit_int(outer, 2);
    rt.core().complete(outer);
    rt.core().complete(inner1);
    rt.core().complete(inner2);

    let events = rec.events();
    assert!(
        events.iter().any(|e| matches!(e, RecordedEvent::Complete)),
        "concat_map should complete after outer + all queued inners complete; got {events:?}"
    );
}

// =====================================================================
// merge_map
// =====================================================================

#[test]
fn merge_map_unbounded_spawns_all_inners() {
    let rt = OpRuntime::new();
    let outer = rt.state_int(None);
    let inner1 = rt.state_int(None);
    let inner2 = rt.state_int(None);
    let inner3 = rt.state_int(None);
    let inners = Arc::new(Mutex::new(vec![inner1, inner2, inner3]));
    let m = merge_map(rt.core(), &rt.ho_binding, outer, make_seq_project(inners));
    let rec = rt.subscribe_recorder(m);

    rt.emit_int(outer, 1);
    rt.emit_int(outer, 2);
    rt.emit_int(outer, 3);

    rt.emit_int(inner1, 10);
    rt.emit_int(inner2, 20);
    rt.emit_int(inner3, 30);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    let mut data = rec.data_values();
    data.sort_by_key(|v| match v {
        TestValue::Int(n) => *n,
        _ => i64::MAX,
    });
    assert_eq!(
        data,
        vec![TestValue::Int(10), TestValue::Int(20), TestValue::Int(30)]
    );
}

#[test]
fn merge_map_with_concurrency_one_processes_sequentially() {
    let rt = OpRuntime::new();
    let outer = rt.state_int(None);
    let inner1 = rt.state_int(None);
    let inner2 = rt.state_int(None);
    let inners = Arc::new(Mutex::new(vec![inner1, inner2]));
    let project_count = Arc::new(AtomicU32::new(0));
    let counter = project_count.clone();
    let inners_for_proj = inners.clone();
    let project = Box::new(move |_h| {
        counter.fetch_add(1, Ordering::SeqCst);
        let mut v = inners_for_proj.lock().unwrap();
        v.remove(0)
    });
    let m = merge_map_with_concurrency(rt.core(), &rt.ho_binding, outer, project, Some(1));
    let _rec = rt.subscribe_recorder(m);

    rt.emit_int(outer, 1);
    rt.emit_int(outer, 2);
    assert_eq!(project_count.load(Ordering::SeqCst), 1);

    rt.core().complete(inner1);
    assert_eq!(project_count.load(Ordering::SeqCst), 2);
}

#[test]
fn merge_map_with_concurrency_two_caps_active() {
    let rt = OpRuntime::new();
    let outer = rt.state_int(None);
    let inner1 = rt.state_int(None);
    let inner2 = rt.state_int(None);
    let inner3 = rt.state_int(None);
    let inners = Arc::new(Mutex::new(vec![inner1, inner2, inner3]));
    let project_count = Arc::new(AtomicU32::new(0));
    let counter = project_count.clone();
    let inners_for_proj = inners.clone();
    let project = Box::new(move |_h| {
        counter.fetch_add(1, Ordering::SeqCst);
        let mut v = inners_for_proj.lock().unwrap();
        v.remove(0)
    });
    let m = merge_map_with_concurrency(rt.core(), &rt.ho_binding, outer, project, Some(2));
    let _rec = rt.subscribe_recorder(m);

    rt.emit_int(outer, 1);
    rt.emit_int(outer, 2);
    rt.emit_int(outer, 3);
    assert_eq!(
        project_count.load(Ordering::SeqCst),
        2,
        "concurrency=2: only first two spawn; third buffers"
    );

    rt.core().complete(inner1);
    assert_eq!(
        project_count.load(Ordering::SeqCst),
        3,
        "after inner1 completes, buffered third drains"
    );
}

#[test]
fn merge_map_completes_after_outer_and_all_inners_complete() {
    let rt = OpRuntime::new();
    let outer = rt.state_int(None);
    let inner1 = rt.state_int(None);
    let inner2 = rt.state_int(None);
    let inners = Arc::new(Mutex::new(vec![inner1, inner2]));
    let m = merge_map(rt.core(), &rt.ho_binding, outer, make_seq_project(inners));
    let rec = rt.subscribe_recorder(m);

    rt.emit_int(outer, 1);
    rt.emit_int(outer, 2);
    rt.core().complete(outer);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    let pre = rec.events();
    assert!(
        !pre.iter().any(|e| matches!(e, RecordedEvent::Complete)),
        "merge_map must NOT complete while inners are active"
    );

    rt.core().complete(inner1);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.
    let mid = rec.events();
    assert!(
        !mid.iter().any(|e| matches!(e, RecordedEvent::Complete)),
        "merge_map must NOT complete while any inner is still active"
    );

    rt.core().complete(inner2);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.
    let post = rec.events();
    assert!(
        post.iter().any(|e| matches!(e, RecordedEvent::Complete)),
        "merge_map must complete once outer + all inners are done"
    );
}

#[test]
fn merge_map_propagates_inner_error() {
    let rt = OpRuntime::new();
    let outer = rt.state_int(None);
    let inner1 = rt.state_int(None);
    let inners = Arc::new(Mutex::new(vec![inner1]));
    let m = merge_map(rt.core(), &rt.ho_binding, outer, make_seq_project(inners));
    let rec = rt.subscribe_recorder(m);

    rt.emit_int(outer, 1);
    rt.settle(); // D246: pump deferred inner-subscribe owner-side first.
    let err_h = rt.intern(TestValue::Str("inner1-boom".into()));
    rt.core().error(inner1, err_h);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    let events = rec.events();
    assert!(events.iter().any(|e| matches!(e, RecordedEvent::Error(_))));
}

// =====================================================================
// Producer storage cleanup (D-substrate parity)
// =====================================================================

#[test]
fn switch_map_cleans_up_subs_on_deactivation() {
    // Slice E /qa refactor: inner subs live inside SwitchState, not in
    // producer_storage[producer_id].subs. The producer storage entry
    // should hold ONLY the outer source subscription (single entry).
    // On deactivation, the entire entry is removed; SwitchState is
    // dropped along with it (held by closures referencing it from the
    // build closure, which the binding's fn registry holds).
    let rt = OpRuntime::new();
    let outer = rt.state_int(None);
    let inner1 = rt.state_int(None);
    let inners = Arc::new(Mutex::new(vec![inner1]));
    let m = switch_map(rt.core(), &rt.ho_binding, outer, make_seq_project(inners));

    {
        let _rec = rt.subscribe_recorder(m);
        rt.emit_int(outer, 1);
        rt.settle(); // D246: pump deferred producer-sink emits owner-side.
        let storage = rt.binding.producer_storage().lock();
        let entry = storage
            .get(&m)
            .expect("producer storage entry should exist");
        // Slice E /qa: only the outer source sub lives in producer_storage;
        // inner sub is in SwitchState.inner_sub.
        assert_eq!(entry.subs.len(), 1, "outer source sub only");
    }
    // D246: `_rec` drop posts a deferred unsubscribe; pump it owner-side
    // so `producer_deactivate` runs and clears the storage entry.
    rt.settle();
    let storage = rt.binding.producer_storage().lock();
    assert!(
        storage.get(&m).is_none(),
        "producer storage entry should be cleared on deactivation"
    );
}

#[test]
fn merge_map_does_not_accumulate_completed_inner_subs_in_producer_storage() {
    // Slice E /qa P2 fix regression: inner subs live in MergeMapState.inner_subs
    // (HashMap), not in producer_storage[producer_id].subs. After multiple
    // inners spawn + complete, producer_storage stays at 1 entry (outer source
    // sub only).
    let rt = OpRuntime::new();
    let outer = rt.state_int(None);
    let inner1 = rt.state_int(None);
    let inner2 = rt.state_int(None);
    let inner3 = rt.state_int(None);
    let inners = Arc::new(Mutex::new(vec![inner1, inner2, inner3]));
    let m = merge_map(rt.core(), &rt.ho_binding, outer, make_seq_project(inners));
    let _rec = rt.subscribe_recorder(m);

    rt.emit_int(outer, 1);
    rt.emit_int(outer, 2);
    rt.emit_int(outer, 3);

    {
        let storage = rt.binding.producer_storage().lock();
        let entry = storage.get(&m).expect("entry exists");
        assert_eq!(
            entry.subs.len(),
            1,
            "producer_storage holds outer sub only; inner subs live in MergeMapState"
        );
    }

    rt.core().complete(inner1);
    rt.core().complete(inner2);
    rt.core().complete(inner3);

    {
        let storage = rt.binding.producer_storage().lock();
        let entry = storage.get(&m).expect("entry still exists");
        assert_eq!(
            entry.subs.len(),
            1,
            "producer_storage stays at 1 (outer) after 3 inner completions; no accumulation"
        );
    }
}

#[test]
fn switch_map_with_cached_outer_does_not_drop_outer_sub() {
    // Slice E /qa "cached-outer ordering" latent fix regression: if the
    // outer source has a CACHED value, the synchronous handshake fires
    // outer_sink during ctx.subscribe_to. outer_sink projects + subscribes
    // to inner. Pre-fix this would push the inner sub into
    // producer_storage[producer_id].subs[0] before the outer sub got pushed
    // at subs[1], reordering positional invariants. Drop_inner_subs would
    // then truncate the outer sub instead of the inner.
    //
    // Post-fix: inner sub is in SwitchState.inner_sub, not in producer_storage.
    // No positional ambiguity. The outer source's cached Data is forwarded
    // via the inner correctly.
    let rt = OpRuntime::new();
    let outer = rt.state_int(Some(1));
    let inner1 = rt.state_int(Some(100));
    let inners = Arc::new(Mutex::new(vec![inner1]));
    let m = switch_map(
        rt.core(),
        &rt.ho_binding,
        outer,
        make_seq_project(inners.clone()),
    );
    let rec = rt.subscribe_recorder(m);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    // Cached outer + cached inner: handshake delivers Data(100) immediately.
    let data = rec.data_values();
    assert_eq!(data, vec![TestValue::Int(100)]);

    // Now emit on outer to verify outer sub is still alive (would be
    // dropped by the pre-fix bug).
    let inner2 = rt.state_int(Some(200));
    {
        let mut v = inners.lock().unwrap();
        // Replace the project closure's queue with a new inner.
        // (The closure is fixed at registration; we'd need a different
        // mechanism. Use a fresh make_seq_project + a different operator
        // would be cleaner. For this regression test, just verify outer
        // is still functional.)
        v.push(inner2);
    }
    // Actually the project closure was constructed once; emitting on
    // outer would project the next inner from the queue. If outer was
    // dropped (the bug), nothing would happen. If alive, inner2 gets
    // subscribed.
    rt.emit_int(outer, 2);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.
    let data_after = rec.data_values();
    assert_eq!(
        data_after,
        vec![TestValue::Int(100), TestValue::Int(200)],
        "outer sub stayed alive across cached-outer handshake; inner2 emitted"
    );
}
