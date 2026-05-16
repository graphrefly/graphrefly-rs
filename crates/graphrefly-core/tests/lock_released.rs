//! Slice A close (M1) regression tests — lock-released `invoke_fn` /
//! `custom_equals` + R1.3.5.a handshake tier-split + late-subscriber-
//! during-wave race fix.
//!
//! These exercise the wave-engine paths that lifted from the v1
//! "lock-held during binding callback" discipline. Each test is named for
//! the canonical-spec rule or porting-deferred entry it closes:
//!
//! - **`fire_fn` lock-released `invoke_fn`** — fns may re-enter Core during
//!   their own fire.
//! - **`commit_emission` lock-released `custom_equals`** — equals oracles
//!   may re-enter Core during evaluation.
//! - **R1.3.5.a handshake tier-split** — `[Start]` + `[Data(v)]` arrive as
//!   separate sink calls, not a bundled call.
//! - **Late-subscriber during wave** — a sink installed between fn-fire
//!   iterations does NOT receive duplicate `[Dirty, Data]` from already-
//!   queued wave messages (sink-snapshot-on-first-touch).

mod common;

use std::sync::{Arc, Mutex};

use common::{RecordedEvent, Recorder, TestBinding, TestRuntime, TestValue};

use graphrefly_core::{
    BindingBoundary, Core, DepBatch, EqualsMode, FnId, FnResult, HandleId, Message, NodeId, Sink,
};

// ---------------------------------------------------------------------------
// Fn re-entrance via invoke_fn
// ---------------------------------------------------------------------------

#[test]
fn fn_can_reenter_core_emit_during_invoke_fn_runs_nested_wave() {
    // A derived's fn calls `Core::emit(other_state, ...)` mid-fire. The
    // nested emit should run a nested wave (in_tick re-entrance) and
    // queue downstream work; the outer drain picks it up.
    //
    // **Phase H+ topology requirement (2026-05-09):** the cross-partition
    // emit during fire must obey ascending-order acquisition (Phase H+
    // option (d) limited variant). To satisfy that, declare s_side as a
    // meta-companion of s_in so the wave's `compute_touched_partitions`
    // acquires {partition(s_in), partition(s_side)} ascending UPFRONT;
    // the inner emit on s_side then becomes a re-entrant acquire (already
    // held by this thread) rather than a fresh descending acquire.
    let rt = TestRuntime::new();
    let s_in = rt.state(Some(TestValue::Int(0)));
    let s_side = rt.state(Some(TestValue::Int(0)));

    // Derived from s_in. When it fires, it ALSO emits a side effect on
    // s_side via Core::emit — which previously deadlocked because the
    // state lock was held across invoke_fn.
    let core = rt.core.clone();
    let s_side_id = s_side.id;
    let binding = rt.binding.clone();
    let d = rt.derived(&[s_in.id], move |deps| {
        if let TestValue::Int(n) = deps[0] {
            // Re-enter Core::emit lock-released. Should NOT deadlock.
            //
            // Phase H+ /qa N1(a) (2026-05-09): gate on `n > 0` so the
            // cross-partition emit fires from a TOP-LEVEL wave entry
            // (`s_in.set(7)` below) rather than from d's activation-
            // time fire. Activation goes through `Core::subscribe`'s
            // direct `partition_wave_owner_lock_arc` (NOT `begin_batch_for`),
            // which doesn't walk meta_companions; the cross-partition
            // acquire would then be descending and panic. Top-level
            // `s_in.set(...)` does go through `begin_batch_for(s_in)`
            // which walks `s_in`'s meta_companions and acquires
            // `{partition(s_in), partition(s_side)}` ascending upfront,
            // so the inner emit is re-entrant on a held partition.
            if n > 0 {
                let h = binding.intern(TestValue::Int(n * 10));
                core.emit(s_side_id, h);
            }
        }
        Some(deps[0].clone())
    });
    // Phase H+ topology requirement: declare s_side as a meta-companion
    // of s_in so the top-level `s_in.set(...)` wave below acquires
    // {partition(s_in), partition(s_side)} ascending upfront via
    // `compute_touched_partitions(s_in)`. Inner emit on s_side is
    // then a re-entrant acquire on a partition already held by the
    // wave, not a fresh descending acquire from inside fn-fire.
    //
    // /qa A7 (2026-05-09): note that `add_meta_companion` ALSO
    // induces R1.3.9.d TEARDOWN cascade — when `s_in` is torn down,
    // `s_side` will receive a Teardown event. This test samples its
    // assertions BEFORE `rt` drops, so the cascade doesn't affect
    // observable behavior. Future variants that inspect state AFTER
    // an explicit teardown must account for the cascaded Teardown
    // on s_side's recorder (filter or assert).
    let _ = d; // silence unused-binding warning if present
    rt.core.add_meta_companion(s_in.id, s_side_id);

    let rec_d = rt.subscribe_recorder(d);
    let rec_side = rt.subscribe_recorder(s_side.id);
    s_in.set(TestValue::Int(7));

    // Outer derived emitted 7; side state was emitted 70 from inside the fn.
    assert_eq!(
        rec_d.data_values(),
        vec![TestValue::Int(0), TestValue::Int(7)]
    );
    assert_eq!(
        rec_side.data_values(),
        vec![TestValue::Int(0), TestValue::Int(70)]
    );
}

#[test]
fn fn_can_reenter_core_pause_resume_during_invoke_fn() {
    // A fn calls Core::pause and Core::resume on another node mid-fire.
    let rt = TestRuntime::new();
    let s_in = rt.state(Some(TestValue::Int(0)));
    let s_other = rt.state(Some(TestValue::Int(100)));

    let core = rt.core.clone();
    let s_other_id = s_other.id;
    let pause_lock = core.alloc_lock_id();
    let _d = rt.derived(&[s_in.id], move |_deps| {
        // Pause + resume on s_other from inside fn — both lock-released.
        core.pause(s_other_id, pause_lock).expect("pause");
        let report = core.resume(s_other_id, pause_lock).expect("resume");
        // Both calls succeeded; pause buffer was empty so no replay.
        if let Some(r) = report {
            assert_eq!(r.replayed, 0);
            assert_eq!(r.dropped, 0);
        }
        None
    });

    let _rec_d = rt.subscribe_recorder(_d);
    s_in.set(TestValue::Int(1));
    // No deadlock; assertion above ensures pause/resume worked.
}

#[test]
fn fn_can_reenter_core_invalidate_during_invoke_fn() {
    let rt = TestRuntime::new();
    let s_in = rt.state(Some(TestValue::Int(0)));
    let s_other = rt.state(Some(TestValue::Int(100)));
    let s_other_id = s_other.id;

    // Subscribe rec_other FIRST so the activation-time fire of d (which
    // happens when we subscribe rec_d below) doesn't invalidate s_other
    // before we have an observer. R1.4 is "no-op on already-invalidated"
    // so a second invalidate would be silent.
    let rec_other = rt.subscribe_recorder(s_other_id);

    let core = rt.core.clone();
    let _d = rt.derived(&[s_in.id], move |deps| {
        // Phase H+ /qa N1(a) (2026-05-09): gate on `n != 0` so the
        // cross-partition invalidate fires from a TOP-LEVEL wave
        // entry (`s_in.set(1)` below), not from d's activation-time
        // fire. Same rationale as
        // `fn_can_reenter_core_emit_during_invoke_fn_runs_nested_wave`
        // above — subscribe-time activation goes through
        // `partition_wave_owner_lock_arc` directly, bypassing the
        // meta-companion walk in `compute_touched_partitions`.
        if let TestValue::Int(n) = deps[0] {
            if n != 0 {
                core.invalidate(s_other_id);
            }
        }
        Some(deps[0].clone())
    });
    // Phase H+ topology requirement: every wave entering d's fire
    // must touch s_other's partition upfront. The top-level
    // `s_in.set(...)` below drives begin_batch_for(s_in) which walks
    // s_in's meta_companions to s_other, acquires both partitions
    // ascending, then d's fire's inner invalidate on s_other is
    // re-entrant on a held partition.
    rt.core.add_meta_companion(s_in.id, s_other_id);

    let _rec_d = rt.subscribe_recorder(_d);

    // Activation-time fire of d already invalidated s_other; s_in.set is
    // a redundant trigger but exercises a second wave for good measure.
    s_in.set(TestValue::Int(1));

    // s_other got invalidated from inside the fn; rec_other observed it.
    assert!(rec_other.snapshot().contains(&RecordedEvent::Invalidate));
}

// ---------------------------------------------------------------------------
// custom_equals re-entrance
// ---------------------------------------------------------------------------

#[test]
fn custom_equals_can_reenter_core_during_emission() {
    // A custom equals oracle reads cache via Core::cache_of from inside
    // its evaluation. Previously held the lock across the binding call →
    // deadlock. Lock-released now → succeeds.
    let rt = TestRuntime::new();
    let s_in = rt.state(Some(TestValue::Int(0)));
    let probe_state = rt.state(Some(TestValue::Int(99)));

    let core = rt.core.clone();
    let probe_id = probe_state.id;
    // Custom equals: ALWAYS returns false (force is_data path), but
    // re-enters Core via cache_of mid-evaluation.
    let d = rt.derived_with_equals(
        &[s_in.id],
        |deps| Some(deps[0].clone()),
        move |_a, _b| {
            // Re-enter Core::cache_of from inside custom_equals — lock-released.
            let _ = core.cache_of(probe_id);
            false
        },
    );

    let rec = rt.subscribe_recorder(d);
    s_in.set(TestValue::Int(1));
    s_in.set(TestValue::Int(2));

    // Both updates flowed through (custom equals returned false → DATA).
    assert_eq!(
        rec.data_values(),
        vec![TestValue::Int(0), TestValue::Int(1), TestValue::Int(2)]
    );
}

// ---------------------------------------------------------------------------
// R1.3.5.a handshake tier-split
// ---------------------------------------------------------------------------

#[test]
fn handshake_tier_split_sentinel_state_one_call() {
    // Sentinel state node — handshake is just `[Start]`, one sink call.
    let rt = TestRuntime::new();
    let s = rt.state(None);

    let rec = rt.subscribe_recorder(s.id);

    assert_eq!(rec.call_count(), 1, "sentinel handshake = 1 sink call");
    assert_eq!(rec.call_boundaries(), vec![1]);
    assert_eq!(rec.snapshot(), vec![RecordedEvent::Start]);
}

#[test]
fn handshake_tier_split_cached_state_two_calls() {
    // R1.3.5.a: cached state subscribe should produce `[Start]` then
    // `[Data(v)]` as TWO separate sink calls (per-tier delivery), not
    // one bundled `[Start, Data(v)]` call.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(42)));

    let rec = rt.subscribe_recorder(s.id);

    assert_eq!(rec.call_count(), 2, "cached handshake = 2 sink calls");
    assert_eq!(rec.call_boundaries(), vec![1, 1]);
    assert_eq!(
        rec.snapshot(),
        vec![
            RecordedEvent::Start,
            RecordedEvent::Data(TestValue::Int(42))
        ]
    );
}

// Note: pre-D118, three additional handshake-tier-split tests covered the
// `[Start, Data, Complete]` / `[Start, Data, Complete, Teardown]` / `[Start,
// Data, Error]` replay shape on non-resubscribable terminal nodes. With
// canonical R2.2.7.b (D118, 2026-05-10), subscribe to a non-resubscribable
// terminal node is REJECTED rather than replayed. The handshake-tier-split
// guarantee (R1.3.5.a) is now exercised on:
//   - `handshake_tier_split_sentinel_state_one_call` — `[Start]` (1 call)
//   - `handshake_tier_split_cached_state_two_calls` — `[Start, Data]` (2 calls)
//   - replay-buffer scenarios in `replay_buffer.rs` — `[Start, Data, Data, ...]`
// The rejection paths (replacing the deleted three tests) live in
// `tests/resubscribable.rs::subscribe_to_non_resubscribable_*_panics` (D118).

// ---------------------------------------------------------------------------
// Late-subscriber-during-wave: sink-snapshot-on-first-touch race fix
// ---------------------------------------------------------------------------

#[test]
fn late_subscriber_installed_after_first_queue_notify_does_not_double_receive_data() {
    // Race-fix scenario (sink-snapshot-on-first-touch):
    //
    // The fn calls `core.emit(s, new_value)` (nested wave), then
    // `core.subscribe(s, late_sink)`. The nested emit's commit_emission
    // queues Dirty+Data on `s` BEFORE the late subscribe happens, so the
    // pending_notify entry's snapshot for `s` is taken without late_sink.
    // After subscribe runs, late_sink IS installed in s's `subscribers`,
    // but the wave's flush iterates `pending_notify[s].sinks` (the
    // snapshot), which excludes late_sink. The late subscriber only sees
    // its handshake `[Start, Data(new)]` — no duplicate Dirty+Data from
    // the wave's deferred flush.
    //
    // Without the snapshot-on-first-touch fix (the pre-Slice-A-close
    // model that re-snapshotted subscribers at flush time), late_sink
    // would have received both its handshake [Start, Data(new)] AND the
    // wave's [Dirty, Data(new)] — a duplicate Data delivery.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));

    // Pre-existing subscriber on `s` so its pending_notify entry has a
    // non-empty sink snapshot at first touch (proves the snapshot is
    // populated, not just empty).
    let rec_existing = rt.subscribe_recorder(s.id);

    // Pre-create the late recorder.
    let late_recorder: Arc<Mutex<Option<Recorder>>> = Arc::new(Mutex::new(None));
    let late_rec = Recorder::new();
    let late_sink = late_rec.sink(rt.binding.clone());
    *late_recorder.lock().unwrap() = Some(late_rec);

    let core = rt.core.clone();
    let s_id = s.id;
    let binding = rt.binding.clone();
    let late_recorder_for_fn = late_recorder.clone();
    let late_sink_holder: Arc<Mutex<Option<Sink>>> = Arc::new(Mutex::new(Some(late_sink)));
    let late_sink_for_fn = late_sink_holder.clone();
    let trigger = rt.state(Some(TestValue::Int(0)));
    // Phase H+ topology requirement: declare s as a meta-companion of
    // trigger so the wave entered via trigger.set acquires both
    // partitions ascending UPFRONT; the nested emit + subscribe on s
    // are re-entrant acquires on a held partition.
    rt.core.add_meta_companion(trigger.id, s_id);

    let _d = rt.derived(&[trigger.id], move |deps| {
        if let TestValue::Int(n) = deps[0] {
            if n > 0 {
                // 1. Emit a new value to `s` from inside the fn (nested wave
                //    — commit_emission queues Dirty+Data on s with the
                //    snapshot of s's subscribers AT THIS MOMENT).
                let h = binding.intern(TestValue::Int(n * 100));
                core.emit(s_id, h);
                // 2. Subscribe a new sink to `s` AFTER the emit. The
                //    pending_notify[s] snapshot is already locked in;
                //    late_sink is not in it.
                if let Some(sink) = late_sink_for_fn.lock().unwrap().take() {
                    let sub = core.subscribe(s_id, sink);
                    if let Some(rec) = late_recorder_for_fn.lock().unwrap().as_ref() {
                        rec.attach(sub);
                    }
                }
            }
        }
        None
    });

    // Subscribe to the derived (sentinel cache → no activation-time fire
    // because trigger has Int(0) and the fn's `n > 0` branch is gated).
    let _rec_d = rt.subscribe_recorder(_d);

    // Trigger the derived's fire.
    trigger.set(TestValue::Int(7));

    // Existing subscriber on `s` sees the full sequence — handshake then
    // wave updates.
    let existing_events = rec_existing.snapshot();
    let existing_data: Vec<i64> = existing_events
        .iter()
        .filter_map(|e| match e {
            RecordedEvent::Data(TestValue::Int(n)) => Some(*n),
            _ => None,
        })
        .collect();
    assert_eq!(
        existing_data,
        vec![0, 700],
        "existing subscriber: handshake Data(0) + wave Data(700)"
    );

    // Late subscriber sees ONLY its handshake — `[Start, Data(700)]` as
    // 2 sink calls. NO duplicated Dirty+Data from the wave's flush.
    let late = late_recorder.lock().unwrap();
    let late_rec = late.as_ref().expect("late recorder");
    let late_events = late_rec.snapshot();
    assert_eq!(
        late_events,
        vec![
            RecordedEvent::Start,
            RecordedEvent::Data(TestValue::Int(700)),
        ],
        "late subscriber duplicated Data: {late_events:?}"
    );
    assert_eq!(
        late_rec.call_count(),
        2,
        "late subscriber: handshake split into [Start] + [Data(700)] = 2 calls"
    );
}

// ---------------------------------------------------------------------------
// §7 (D208–D211): the two union-find tests that lived here
// (`concurrent_emit_on_disjoint_partitions_runs_truly_parallel`,
// `concurrent_emit_on_same_partition_serializes`) asserted the DELETED
// per-partition `wave_owner` parallelism/serialization guarantee for
// default (ungrouped) nodes. Under §7 the default `None` path is the
// single-threaded substrate (no per-node wave-lock — that is the floor,
// by design), so those guarantees no longer apply to ungrouped nodes.
// The §7 equivalents — same-group cross-thread serialization without
// deadlock, and disjoint-group waves not blocking each other — live in
// `tests/group_parallelism.rs`. The remaining lock-released re-entrance
// tests below are unaffected and still pass.
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Refcount discipline preserved across the lock-released refactor
// ---------------------------------------------------------------------------

#[test]
fn lock_released_refactor_does_not_leak_handles_under_basic_emit() {
    // Smoke test that the per-iteration lock-acquisition pattern in
    // drain_and_flush + the sinks-snapshot fix in queue_notify don't
    // corrupt refcount discipline.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let d = rt.derived(&[s.id], |deps| Some(deps[0].clone()));

    let _rec_d = rt.subscribe_recorder(d);
    for i in 1..=10 {
        s.set(TestValue::Int(i));
    }
    drop(_rec_d);
    drop(s);

    // After dropping subscribers + state, only the derived's cache holds
    // a handle. We can't easily count from outside without exposing
    // internals, but we can verify the binding's live count is bounded.
    let live_now = rt.binding.live_handles();
    assert!(
        live_now <= 2,
        "expected <= 2 live handles after drop (derived cache + maybe a transient), got {}",
        live_now
    );

    // Drop the runtime — final Drop for CoreState should release every
    // remaining retained handle.
    drop(rt);
}

// ---------------------------------------------------------------------------
// Direct binding hooks used by the dispatcher's lock-released paths
// ---------------------------------------------------------------------------

/// Sentinel binding: a BindingBoundary whose invoke_fn re-acquires the
/// Core's state lock via Core::cache_of mid-fire. Used to verify the
/// lock-released-around-invoke_fn discipline directly (without going
/// through TestRuntime's Sink wrapping).
struct ReentrantBinding {
    inner: Arc<TestBinding>,
    core_slot: Mutex<Option<Core>>,
    probe_node: Mutex<Option<NodeId>>,
}

impl ReentrantBinding {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: TestBinding::new(),
            core_slot: Mutex::new(None),
            probe_node: Mutex::new(None),
        })
    }
}

impl BindingBoundary for ReentrantBinding {
    fn invoke_fn(&self, node_id: NodeId, fn_id: FnId, dep_data: &[DepBatch]) -> FnResult {
        // Re-enter Core::cache_of mid-fire — depends on lock being
        // released around this call.
        if let (Some(core), Some(probe)) = (
            self.core_slot.lock().unwrap().as_ref(),
            *self.probe_node.lock().unwrap(),
        ) {
            let _ = core.cache_of(probe);
        }
        self.inner.invoke_fn(node_id, fn_id, dep_data)
    }

    fn custom_equals(&self, equals_handle: FnId, a: HandleId, b: HandleId) -> bool {
        self.inner.custom_equals(equals_handle, a, b)
    }

    fn release_handle(&self, handle: HandleId) {
        self.inner.release_handle(handle);
    }

    fn retain_handle(&self, handle: HandleId) {
        self.inner.retain_handle(handle);
    }
}

#[test]
fn invoke_fn_can_call_core_cache_of_directly() {
    let binding = ReentrantBinding::new();
    let core = Core::new(binding.clone() as Arc<dyn BindingBoundary>);

    // Wire Core back into the binding.
    *binding.core_slot.lock().unwrap() = Some(core.clone());

    // Set up a state probe + a derived whose fn will re-enter Core.
    let probe_h = binding.inner.intern(TestValue::Int(42));
    let probe_id = core.register_state(probe_h, false).unwrap();
    *binding.probe_node.lock().unwrap() = Some(probe_id);

    let in_h = binding.inner.intern(TestValue::Int(1));
    let in_id = core.register_state(in_h, false).unwrap();
    let fn_id = binding.inner.register_fn(|deps| Some(deps[0].clone()));
    let d = core
        .register_derived(&[in_id], fn_id, EqualsMode::Identity, false)
        .unwrap();

    // Subscribe to drive activation → fn fires → invoke_fn re-enters
    // Core::cache_of(probe_id). If lock-held, deadlock; if lock-released,
    // succeeds and returns the probe's handle.
    let rec_events: Arc<Mutex<Vec<Message>>> = Arc::new(Mutex::new(Vec::new()));
    let rec_events_for_sink = rec_events.clone();
    let sink: Sink = Arc::new(move |msgs: &[Message]| {
        rec_events_for_sink.lock().unwrap().extend_from_slice(msgs);
    });
    let _sub = core.subscribe(d, sink);

    // No deadlock + derived produced its first DATA.
    let events = rec_events.lock().unwrap();
    assert!(events.iter().any(|m| matches!(m, Message::Start)));
    assert!(events.iter().any(|m| matches!(m, Message::Data(_))));
}
