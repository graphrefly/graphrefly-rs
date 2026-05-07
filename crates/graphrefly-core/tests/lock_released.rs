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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

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
            let h = binding.intern(TestValue::Int(n * 10));
            core.emit(s_side_id, h);
        }
        Some(deps[0].clone())
    });

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
        core.invalidate(s_other_id);
        Some(deps[0].clone())
    });

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

#[test]
fn handshake_tier_split_terminated_state_three_calls() {
    // Cached state that has terminated (non-resubscribable). Handshake
    // should be `[Start]`, `[Data(cached)]`, `[Complete]` — three calls.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(7)));
    rt.core.complete(s.id);

    let rec = rt.subscribe_recorder(s.id);

    assert_eq!(
        rec.call_count(),
        3,
        "cached + complete handshake = 3 sink calls"
    );
    assert_eq!(rec.call_boundaries(), vec![1, 1, 1]);
    assert_eq!(
        rec.snapshot(),
        vec![
            RecordedEvent::Start,
            RecordedEvent::Data(TestValue::Int(7)),
            RecordedEvent::Complete,
        ]
    );
}

#[test]
fn handshake_tier_split_torndown_node_four_calls() {
    // Torn-down cached state: `[Start]`, `[Data]`, `[Complete]`,
    // `[Teardown]` — four calls.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(5)));
    rt.core.teardown(s.id);

    let rec = rt.subscribe_recorder(s.id);

    assert_eq!(rec.call_count(), 4, "torn-down handshake = 4 sink calls");
    assert_eq!(rec.call_boundaries(), vec![1, 1, 1, 1]);
    assert_eq!(
        rec.snapshot(),
        vec![
            RecordedEvent::Start,
            RecordedEvent::Data(TestValue::Int(5)),
            RecordedEvent::Complete,
            RecordedEvent::Teardown,
        ]
    );
}

#[test]
fn handshake_tier_split_error_terminated_three_calls() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));
    let err_h = rt.binding.intern(TestValue::Str("boom".into()));
    rt.core.error(s.id, err_h);

    let rec = rt.subscribe_recorder(s.id);

    assert_eq!(rec.call_count(), 3);
    assert_eq!(rec.call_boundaries(), vec![1, 1, 1]);
    assert_eq!(
        rec.snapshot(),
        vec![
            RecordedEvent::Start,
            RecordedEvent::Data(TestValue::Int(1)),
            RecordedEvent::Error(TestValue::Str("boom".into())),
        ]
    );
}

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
// Concurrent emit during invoke_fn — no deadlock
// ---------------------------------------------------------------------------

#[test]
fn concurrent_emit_blocks_until_in_flight_wave_completes() {
    // Q2 contract (Slice A close /qa, M1): cross-thread emits BLOCK at
    // the wave-owner re-entrant mutex until the in-flight wave's drain
    // + flush + sink-fire completes. This preserves the user-facing
    // "emit returning means subscribers have observed" contract that
    // would otherwise be broken by the lock-released drain.
    //
    // Test shape:
    //   1. Thread A starts a wave whose fn blocks on `rx.recv()`.
    //      Thread A holds `wave_owner` for the wave's duration.
    //   2. Thread B emits on a DIFFERENT state node. It must BLOCK at
    //      `wave_owner.lock_arc()` since Thread A holds it.
    //   3. Test thread observes Thread B has not finished after 50ms
    //      (proving the block).
    //   4. Test thread releases Thread A's fn via `tx.send(())`.
    //   5. Thread A's wave completes, releasing `wave_owner`.
    //   6. Thread B's emit unblocks and completes.
    //   7. Both threads join cleanly.
    let rt = TestRuntime::new();
    // Sentinel s_a so subscribe_recorder doesn't trigger an activation-
    // time fire that blocks before thread-A even spawns.
    let s_a = rt.state(None);
    let s_b = rt.state(Some(TestValue::Int(0)));

    let (tx, rx) = std::sync::mpsc::channel::<()>();
    let rx = Arc::new(Mutex::new(Some(rx)));
    let rx_for_fn = rx.clone();
    let fn_entered = Arc::new(AtomicU64::new(0));
    let fn_entered_for_fn = fn_entered.clone();

    let _d = rt.derived(&[s_a.id], move |deps| {
        fn_entered_for_fn.fetch_add(1, Ordering::SeqCst);
        // Block until the test thread releases. The lock-protected Option
        // lets us take() the receiver safely once.
        let recv = rx_for_fn.lock().unwrap().take();
        if let Some(rx) = recv {
            let _ = rx.recv();
        }
        Some(deps[0].clone())
    });

    let _rec_d = rt.subscribe_recorder(_d);

    // Thread A: emit on s_a triggers the first fn fire (which blocks on rx).
    // Thread A holds `wave_owner` for the wave's duration.
    let core_a = rt.core.clone();
    let s_a_id = s_a.id;
    let binding_a = rt.binding.clone();
    let thread_a = thread::spawn(move || {
        let h = binding_a.intern(TestValue::Int(1));
        core_a.emit(s_a_id, h);
    });

    // Wait for thread A's fn to enter (so wave_owner is held).
    let mut waited_ms = 0u64;
    while fn_entered.load(Ordering::SeqCst) == 0 {
        thread::sleep(Duration::from_millis(1));
        waited_ms += 1;
        assert!(
            waited_ms < 5_000,
            "thread A's fn never entered — emit may have deadlocked at wave_owner"
        );
    }

    // Thread B: emits on s_b. EXPECTED to BLOCK at wave_owner since
    // Thread A's wave is in flight.
    let core_b = rt.core.clone();
    let s_b_id = s_b.id;
    let binding_b = rt.binding.clone();
    let thread_b = thread::spawn(move || {
        let h = binding_b.intern(TestValue::Int(2));
        core_b.emit(s_b_id, h);
    });

    // Verify Thread B is blocked. Give it 100ms of wall-clock to be
    // sure it had a chance to attempt the emit; if it finished within
    // that window, the wave-owner mutex isn't doing its job.
    thread::sleep(Duration::from_millis(100));
    assert!(
        !thread_b.is_finished(),
        "Thread B's cross-thread emit should be blocked on wave_owner \
         while Thread A's wave is in flight; instead it finished early"
    );

    // Release Thread A's fn — its wave will now finish, releasing
    // wave_owner. Thread B's emit then unblocks.
    tx.send(()).expect("send to unblock fn");

    // Both threads should now finish.
    let join_with_timeout = |handle: thread::JoinHandle<()>, secs: u64| {
        let start = std::time::Instant::now();
        loop {
            if handle.is_finished() {
                handle.join().expect("thread panicked");
                return;
            }
            if start.elapsed().as_secs() > secs {
                panic!("thread did not finish within {secs}s — likely deadlock");
            }
            thread::sleep(Duration::from_millis(5));
        }
    };
    join_with_timeout(thread_a, 5);
    join_with_timeout(thread_b, 5);
}

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
    let probe_id = core.register_state(probe_h, false);
    *binding.probe_node.lock().unwrap() = Some(probe_id);

    let in_h = binding.inner.intern(TestValue::Int(1));
    let in_id = core.register_state(in_h, false);
    let fn_id = binding.inner.register_fn(|deps| Some(deps[0].clone()));
    let d = core.register_derived(&[in_id], fn_id, EqualsMode::Identity, false);

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
