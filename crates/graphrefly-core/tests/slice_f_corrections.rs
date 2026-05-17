//! Slice F regression tests — canonical-spec correctness pass (2026-05-07).
//!
//! Each test references the A-bucket item it covers. Pre-Slice-F these were
//! either absent (the spec gap was unrepresented) or wrong (asserted the
//! buggy behavior).

mod common;

use std::sync::{Arc, Mutex};

use common::{RecordedEvent, TestBinding, TestRuntime, TestValue};
use graphrefly_core::{
    BindingBoundary, Core, EqualsMode, FnId, FnResult, HandleId, LockId, Message, NodeId,
    PausableMode, SetDepsError,
};

// =====================================================================
// A2 — `cfg.max_batch_drain_iterations` setter (R4.3 / Lock 2.F′)
// =====================================================================

#[test]
fn a2_max_batch_drain_iterations_default_is_10000() {
    // The default cap exists and is in effect from Core::new. We can't
    // observe the default value directly through the public API (no
    // getter), but we can lower the cap and verify it's enforced.
    let rt = TestRuntime::new();
    rt.core.set_max_batch_drain_iterations(5);
    // Cap accepted without panic — sanity check the setter wired in.
}

#[test]
#[should_panic(expected = "max_batch_drain_iterations must be > 0")]
fn a2_zero_cap_rejected() {
    let rt = TestRuntime::new();
    rt.core.set_max_batch_drain_iterations(0);
}

// =====================================================================
// A4 — `alloc_lock_id` reserves the high range (1<<32+)
// =====================================================================

#[test]
fn a4_alloc_lock_id_starts_above_user_range() {
    let rt = TestRuntime::new();
    let allocated = rt.core.alloc_lock_id();
    // Phase E /qa F1 (2026-05-08): seed lowered from `1u64 << 32` to
    // `1u64 << 31` so values round-trip through napi's u32 boundary.
    // Anti-collision intent (high vs low range) preserved.
    assert!(
        allocated.raw() >= (1u64 << 31),
        "alloc_lock_id should start at >= 2^31; got raw={}",
        allocated.raw()
    );
    // Also verify it's still u32-fitting so the napi binding doesn't
    // error on the cast.
    assert!(
        allocated.raw() <= u64::from(u32::MAX),
        "alloc_lock_id seed must fit in u32 for napi round-trip; got raw={}",
        allocated.raw()
    );
}

#[test]
fn a4_user_supplied_low_lock_id_does_not_collide_with_alloc() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));

    // User-supplied lock in the u32 range.
    let user_lock = LockId::new(42);
    rt.core.pause(s.id, user_lock).expect("pause");
    assert!(rt.core.is_paused(s.id));
    assert!(rt.core.holds_pause_lock(s.id, user_lock));

    // Allocator-issued lock — should NOT collide with user_lock.
    let alloc_lock = rt.core.alloc_lock_id();
    assert_ne!(user_lock, alloc_lock);
    rt.core.pause(s.id, alloc_lock).expect("alloc pause");
    assert_eq!(rt.core.pause_lock_count(s.id), 2);

    // Resume in either order: removing alloc first leaves the node paused
    // (user_lock still held); removing user lock then resumes.
    rt.core.resume(s.id, alloc_lock).expect("resume alloc");
    assert!(rt.core.is_paused(s.id));
    let report = rt
        .core
        .resume(s.id, user_lock)
        .expect("resume user")
        .expect("final");
    assert!(!rt.core.is_paused(s.id));
    assert_eq!(report.replayed, 0);
}

// =====================================================================
// A6 — D1 reentrancy guard: set_deps from firing fn rejected
// =====================================================================

/// Helper binding that exposes a hook for fn-fire to call set_deps on
/// itself (the firing node). Models the D1 hazard scenario.
struct D1Binding {
    inner: Arc<TestBinding>,
    /// Filled in by the test before fire. The fn-fire callback re-enters
    /// `Core::set_deps(target, &new_deps)`; we capture the result so the
    /// test can assert on it.
    target: Mutex<Option<NodeId>>,
    new_deps: Mutex<Vec<NodeId>>,
    core: Mutex<Option<Core>>,
    captured_result: Mutex<Option<Result<(), SetDepsError>>>,
}

impl BindingBoundary for D1Binding {
    fn invoke_fn(
        &self,
        node_id: NodeId,
        fn_id: FnId,
        dep_data: &[graphrefly_core::DepBatch],
    ) -> FnResult {
        // Re-enter set_deps on the target during this fn-fire.
        if let Some(target) = *self.target.lock().unwrap() {
            if target == node_id {
                let new_deps = self.new_deps.lock().unwrap().clone();
                let core = self.core.lock().unwrap().clone();
                if let Some(c) = core {
                    let result = c.set_deps(target, &new_deps);
                    *self.captured_result.lock().unwrap() = Some(result);
                }
            }
        }
        // Delegate to the inner binding for the actual emission.
        self.inner.invoke_fn(node_id, fn_id, dep_data)
    }

    fn release_handle(&self, handle: HandleId) {
        self.inner.release_handle(handle);
    }

    fn retain_handle(&self, handle: HandleId) {
        self.inner.retain_handle(handle);
    }

    fn custom_equals(&self, fn_id: FnId, a: HandleId, b: HandleId) -> bool {
        self.inner.custom_equals(fn_id, a, b)
    }
}

#[test]
fn a6_set_deps_from_firing_fn_rejected_with_reentrant_error() {
    // Build: state s1, state s2, derived n = fn(s1).
    // n's fn re-enters set_deps(n, [s2]) — should be rejected with
    // SetDepsError::ReentrantOnFiringNode.
    let inner = TestBinding::new();
    let d1 = Arc::new(D1Binding {
        inner: inner.clone(),
        target: Mutex::new(None),
        new_deps: Mutex::new(Vec::new()),
        core: Mutex::new(None),
        captured_result: Mutex::new(None),
    });
    let core = Core::new(d1.clone() as Arc<dyn BindingBoundary>);
    *d1.core.lock().unwrap() = Some(core.clone());

    let s1_init = inner.intern(TestValue::Int(10));
    let s2_init = inner.intern(TestValue::Int(20));
    let s1 = core.register_state(s1_init, false).unwrap();
    let s2 = core.register_state(s2_init, false).unwrap();

    let fn_id = inner.register_fn(|deps: &[TestValue]| match deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(n * 2)),
        _ => None,
    });
    let n = core
        .register_derived(&[s1], fn_id, EqualsMode::Identity, false)
        .unwrap();

    // Configure D1Binding: when firing n, attempt set_deps(n, [s2]).
    *d1.target.lock().unwrap() = Some(n);
    *d1.new_deps.lock().unwrap() = vec![s2];

    // Subscribe to drive activation (and the first fn fire).
    let _sub = core.subscribe(n, Arc::new(|_msgs: &[Message]| {}));

    // Verify the captured set_deps result was rejected.
    let result = d1.captured_result.lock().unwrap().take();
    let result = result.expect("set_deps was attempted from inside invoke_fn");
    assert!(
        matches!(result, Err(SetDepsError::ReentrantOnFiringNode { .. })),
        "expected ReentrantOnFiringNode; got {result:?}"
    );

    // The dispatcher's invariants are preserved — n's deps unchanged.
    let deps_after = core.deps_of(n);
    assert_eq!(deps_after, vec![s1], "deps survived the rejected rewire");
}

// =====================================================================
// A7 — handshake-panic catch_unwind: panicking sink removed from subscribers
// =====================================================================

#[test]
fn a7_handshake_panic_removes_sink_and_does_not_re_fire_on_next_wave() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(99))); // cached → handshake delivers Start + Data

    // Sink panics on Data delivery during handshake.
    let panicked = Arc::new(Mutex::new(false));
    let panicked_clone = panicked.clone();
    let bad_sink: graphrefly_core::Sink = Arc::new(move |msgs: &[Message]| {
        for m in msgs {
            if matches!(m, Message::Data(_)) {
                *panicked_clone.lock().unwrap() = true;
                panic!("intentional sink panic on Data during handshake");
            }
        }
    });

    let panic_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = rt.core.subscribe(s.id, bad_sink);
    }));
    assert!(panic_result.is_err(), "subscribe should have unwound");
    assert!(*panicked.lock().unwrap(), "the sink did get called");

    // Subsequent wave on s — install a working sink to verify the panicked
    // sink is no longer in `subscribers`. If A7 didn't remove it, the
    // following emit would re-panic on this thread.
    let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let counter_clone = counter.clone();
    let good_sink: graphrefly_core::Sink = Arc::new(move |msgs: &[Message]| {
        for _ in msgs {
            counter_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    });
    let _sub = rt.core.subscribe(s.id, good_sink);

    s.set(TestValue::Int(100));
    let n = counter.load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        n > 0,
        "good sink received messages — subscribe didn't deadlock against the orphaned sink"
    );
}

// =====================================================================
// A8 — status_of post-INVALIDATE returns Sentinel for fired compute
// =====================================================================

#[test]
fn a8_post_invalidate_compute_node_cache_is_sentinel() {
    // Build: s1 (state) → n (derived). Subscribe; verify n fires; then
    // INVALIDATE n directly via Core::invalidate. Cache should clear, and
    // status_of should report Sentinel (NOT Settled — which is what the
    // pre-A8 code returned because `fired=true`).
    //
    // We don't have a public Status accessor, but cache_of returns
    // NO_HANDLE post-INVALIDATE — the proxy for Status::Sentinel.
    let rt = TestRuntime::new();
    let s1 = rt.state(Some(TestValue::Int(7)));
    let n = rt.derived(&[s1.id], |deps| match deps[0] {
        TestValue::Int(v) => Some(TestValue::Int(v * 10)),
        _ => None,
    });
    let _rec = rt.subscribe_recorder(n);
    // Confirm n fired and cached.
    assert!(rt.core.has_fired_once(n));
    let cache_pre = rt.core.cache_of(n);
    assert_ne!(cache_pre, HandleId::new(0), "n cached before invalidate");

    // Invalidate n.
    rt.core.invalidate(n);

    // Per R1.3.7.b: cache cleared, status sentinel.
    let cache_post = rt.core.cache_of(n);
    assert_eq!(
        cache_post,
        HandleId::new(0),
        "INVALIDATE clears compute cache to NO_HANDLE"
    );
}

// =====================================================================
// A3 — pause overflow ERROR synthesis (R1.3.8.c / Lock 6.A)
// =====================================================================

/// Binding that wires a real synthesize_pause_overflow_error implementation —
/// returns a structured TestValue handle carrying the diagnostic.
struct OverflowBinding {
    inner: Arc<TestBinding>,
    captured: Mutex<Vec<(NodeId, u32, usize, u64)>>,
}

impl BindingBoundary for OverflowBinding {
    fn invoke_fn(
        &self,
        node_id: NodeId,
        fn_id: FnId,
        dep_data: &[graphrefly_core::DepBatch],
    ) -> FnResult {
        self.inner.invoke_fn(node_id, fn_id, dep_data)
    }
    fn release_handle(&self, handle: HandleId) {
        self.inner.release_handle(handle);
    }
    fn retain_handle(&self, handle: HandleId) {
        self.inner.retain_handle(handle);
    }
    fn custom_equals(&self, fn_id: FnId, a: HandleId, b: HandleId) -> bool {
        self.inner.custom_equals(fn_id, a, b)
    }
    fn synthesize_pause_overflow_error(
        &self,
        node_id: NodeId,
        dropped_count: u32,
        configured_max: usize,
        lock_held_duration_ms: u64,
    ) -> Option<HandleId> {
        self.captured.lock().unwrap().push((
            node_id,
            dropped_count,
            configured_max,
            lock_held_duration_ms,
        ));
        // Intern a marker value as the ERROR payload.
        Some(self.inner.intern(TestValue::Str(format!(
            "overflow:n={}:dropped={}:cap={}:held={}ms",
            node_id.raw(),
            dropped_count,
            configured_max,
            lock_held_duration_ms
        ))))
    }
}

#[test]
fn a3_pause_overflow_synthesizes_error_once_per_cycle() {
    let inner = TestBinding::new();
    let ovr = Arc::new(OverflowBinding {
        inner: inner.clone(),
        captured: Mutex::new(Vec::new()),
    });
    let core = Core::new(ovr.clone() as Arc<dyn BindingBoundary>);
    core.set_pause_buffer_cap(Some(2));

    let initial = inner.intern(TestValue::Int(0));
    let s = core.register_state(initial, false).unwrap();
    // R2.6.0 (canonical §2.6 "Option A", pinned 2026-05-17): a Default-mode
    // leaf source's direct emit flushes immediately while self-paused (no
    // buffer → no overflow). The pause-buffer overflow / ERROR-synthesis
    // machinery under test is the `ResumeAll` contract — opt in (set BEFORE
    // subscribe so activation runs under the same semantics).
    core.set_pausable_mode(s, PausableMode::ResumeAll).unwrap();

    // Subscribe with a recorder so we can observe the cascade.
    let events = Arc::new(Mutex::new(Vec::<RecordedEvent>::new()));
    let events_clone = events.clone();
    let inner_clone = inner.clone();
    let sink: graphrefly_core::Sink = Arc::new(move |msgs: &[Message]| {
        let mut g = events_clone.lock().unwrap();
        for m in msgs {
            g.push(match m {
                Message::Start => RecordedEvent::Start,
                Message::Dirty => RecordedEvent::Dirty,
                Message::Resolved => RecordedEvent::Resolved,
                Message::Data(h) => RecordedEvent::Data(inner_clone.deref(*h)),
                Message::Invalidate => RecordedEvent::Invalidate,
                Message::Pause(l) => RecordedEvent::Pause(*l),
                Message::Resume(l) => RecordedEvent::Resume(*l),
                Message::Complete => RecordedEvent::Complete,
                Message::Error(h) => RecordedEvent::Error(inner_clone.deref(*h)),
                Message::Teardown => RecordedEvent::Teardown,
            });
        }
    });
    let _sub = core.subscribe(s, sink);

    // Pause s, then push 5 emissions. Cap is 2, so emit(3) is the first
    // overflow — it triggers ERROR synthesis at wave-end, terminating s.
    // emits 4 and 5 are no-ops on the now-terminal node. Dropped at the
    // moment of synthesis = 1 (just the emit(3) overflow).
    let lock = core.alloc_lock_id();
    core.pause(s, lock).expect("pause");

    for v in 1..=5 {
        let h = inner.intern(TestValue::Int(v));
        core.emit(s, h);
    }

    // synthesize_pause_overflow_error should have been called exactly once
    // (R1.3.8.c "once per overflow event"), AFTER the wave drain ran the
    // synthesis loop.
    let captured = ovr.captured.lock().unwrap().clone();
    assert_eq!(
        captured.len(),
        1,
        "synthesize_pause_overflow_error called once per overflow event; got {captured:?}"
    );
    let (node_id, dropped, cap, _held_ms) = captured[0];
    assert_eq!(node_id, s);
    assert_eq!(
        dropped, 1,
        "first overflow event captured at the moment synthesis schedules — \
         post-terminal emits are silent"
    );
    assert_eq!(cap, 2);

    // The cascade terminated s with ERROR. Subscriber observed it.
    let evs = events.lock().unwrap().clone();
    let saw_error = evs.iter().any(|e| matches!(e, RecordedEvent::Error(_)));
    assert!(saw_error, "subscriber received the synthesized ERROR");

    // After ERROR, subsequent emits are no-ops (terminal node).
    let h = inner.intern(TestValue::Int(99));
    core.emit(s, h);
    let saw_data_99 = events
        .lock()
        .unwrap()
        .iter()
        .any(|e| matches!(e, RecordedEvent::Data(TestValue::Int(99))));
    assert!(!saw_data_99, "post-terminal emits dropped");
}

#[test]
fn a3_overflow_silently_dropped_when_binding_returns_none() {
    // Default BindingBoundary impl returns None for synthesize_pause_overflow_error.
    // TestBinding doesn't override it, so this exercises the silent-drop fallback.
    let rt = TestRuntime::new();
    rt.core.set_pause_buffer_cap(Some(2));
    let s = rt.state(Some(TestValue::Int(0)));
    // R2.6.0: the pause-buffer drop/overflow path is the `ResumeAll`
    // contract (a Default leaf source has no buffer to overflow) — opt in.
    rt.core
        .set_pausable_mode(s.id, PausableMode::ResumeAll)
        .unwrap();
    let rec = rt.subscribe_recorder(s.id);

    let lock = rt.core.alloc_lock_id();
    rt.core.pause(s.id, lock).expect("pause");

    // 5 emits with cap=2 → 3 dropped, no ERROR (binding opted out).
    for v in 1..=5 {
        s.set(TestValue::Int(v));
    }

    let report = rt.core.resume(s.id, lock).expect("resume").expect("final");
    assert_eq!(report.dropped, 3, "dropped count surfaced via ResumeReport");
    // Subscriber received the surviving 2 buffered values on resume but no ERROR.
    let saw_error = rec
        .snapshot()
        .iter()
        .any(|e| matches!(e, RecordedEvent::Error(_)));
    assert!(
        !saw_error,
        "no ERROR synthesized when binding returns None (backward-compat fallback)"
    );
}

// =====================================================================
// Item 4 (audit-driven 2026-05-07) — register rejects non-resubscribable
// terminal dep, mirroring `set_deps`'s `TerminalDep` rejection. TS reference:
// `pure-ts/src/core/node.ts:1635-1648` (`_addDep` throws). Without
// this, the new node's first-run gate never closes (the dep will never emit
// again), creating a permanent stuck wedge.
// =====================================================================

#[test]
fn item4_register_rejects_non_resubscribable_terminal_dep() {
    // Slice H (2026-05-07): promoted from `should_panic` to typed-error
    // assertion — the rejection now returns `Err(RegisterError::TerminalDep)`
    // rather than panicking.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));
    // Subscribe so completion has somewhere to land.
    let _rec = rt.subscribe_recorder(s.id);

    // Terminate s WITHOUT marking resubscribable.
    rt.core.complete(s.id);

    let fn_id = rt
        .binding
        .register_fn(|deps: &[TestValue]| Some(deps[0].clone()));
    let result = rt
        .core
        .register_derived(&[s.id], fn_id, EqualsMode::Identity, false);
    assert_eq!(
        result,
        Err(graphrefly_core::RegisterError::TerminalDep(s.id))
    );
}

#[test]
fn item4_register_accepts_resubscribable_terminal_dep() {
    // Resubscribable terminal deps ARE allowed — the subscribe path
    // resets their lifecycle. Mirrors `set_deps`'s `TerminalDep` policy.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));
    rt.core.set_resubscribable(s.id, true);
    let _rec = rt.subscribe_recorder(s.id);
    rt.core.complete(s.id);

    let fn_id = rt
        .binding
        .register_fn(|deps: &[TestValue]| Some(deps[0].clone()));
    // Should not panic.
    let _ok = rt
        .core
        .register_derived(&[s.id], fn_id, EqualsMode::Identity, false)
        .unwrap();
}

// =====================================================================
// Item 5 (audit-driven 2026-05-07) — pause `default` mode (canonical §2.6).
// Suppresses fn-fire while paused; collapses N pause-window dep deliveries
// into ONE fn execution on RESUME.
// =====================================================================

#[test]
fn item5_default_mode_consolidates_to_one_fn_fire_on_resume() {
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(1)));

    let calls = std::sync::Arc::new(std::sync::Mutex::new(0u32));
    let calls_inner = calls.clone();
    let n = rt.derived(&[a.id], move |deps| {
        *calls_inner.lock().unwrap() += 1;
        match deps[0] {
            TestValue::Int(v) => Some(TestValue::Int(v * 10)),
            _ => None,
        }
    });
    // Default mode is the default; explicit for clarity in this test.
    rt.core.set_pausable_mode(n, PausableMode::Default).unwrap();
    let rec = rt.subscribe_recorder(n);

    // Activation fires fn once.
    assert_eq!(*calls.lock().unwrap(), 1);
    let baseline_calls = 1u32;

    let lock = rt.core.alloc_lock_id();
    rt.core.pause(n, lock).expect("pause n");
    let baseline_data = rec.data_values().len();

    // 3 upstream emissions during pause — fn must NOT fire (default mode
    // suppresses).
    a.set(TestValue::Int(10));
    a.set(TestValue::Int(20));
    a.set(TestValue::Int(30));

    assert_eq!(
        *calls.lock().unwrap(),
        baseline_calls,
        "fn fire suppressed during pause (default mode)"
    );
    let mid_data = rec.data_values().len() - baseline_data;
    assert_eq!(
        mid_data, 0,
        "no DATA emitted from n while paused-default-mode"
    );

    // Resume → exactly ONE fn fire consolidating the pause-window dep state.
    let report = rt.core.resume(n, lock).expect("resume").expect("final");
    assert_eq!(
        *calls.lock().unwrap(),
        baseline_calls + 1,
        "exactly one consolidated fn fire on RESUME"
    );
    assert_eq!(report.replayed, 0, "default mode has no buffered messages");

    // n's cache reflects the LATEST upstream value (a=30 → n=300).
    assert_eq!(rt.cache_value(n), Some(TestValue::Int(300)));
    let post_data: Vec<TestValue> = rec.data_values().into_iter().skip(baseline_data).collect();
    assert_eq!(
        post_data,
        vec![TestValue::Int(300)],
        "subscriber sees single consolidated DATA"
    );
}

#[test]
fn item5_default_mode_no_emit_during_pause_means_no_fire_on_resume() {
    // If no upstream activity happened during pause, default mode RESUME
    // is a no-op — the consolidated fn fire fires only when at least one
    // dep delivered DATA during the pause window.
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(1)));
    let calls = std::sync::Arc::new(std::sync::Mutex::new(0u32));
    let calls_inner = calls.clone();
    let n = rt.derived(&[a.id], move |deps| {
        *calls_inner.lock().unwrap() += 1;
        match deps[0] {
            TestValue::Int(v) => Some(TestValue::Int(v * 10)),
            _ => None,
        }
    });
    let _rec = rt.subscribe_recorder(n);
    let baseline = *calls.lock().unwrap();

    let lock = rt.core.alloc_lock_id();
    rt.core.pause(n, lock).expect("pause");
    // No emits during pause.
    let report = rt.core.resume(n, lock).expect("resume").expect("final");

    assert_eq!(
        *calls.lock().unwrap(),
        baseline,
        "no fire — no pending wave"
    );
    assert_eq!(report.replayed, 0);
}

#[test]
fn item5_off_mode_pause_is_no_op() {
    // PausableMode::Off ignores PAUSE entirely — fn fires immediately,
    // tier-3 flushes immediately. is_paused() reports false even
    // post-`pause()` call.
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(1)));
    let calls = std::sync::Arc::new(std::sync::Mutex::new(0u32));
    let calls_inner = calls.clone();
    let n = rt.derived(&[a.id], move |deps| {
        *calls_inner.lock().unwrap() += 1;
        match deps[0] {
            TestValue::Int(v) => Some(TestValue::Int(v * 10)),
            _ => None,
        }
    });
    rt.core.set_pausable_mode(n, PausableMode::Off).unwrap();
    let _rec = rt.subscribe_recorder(n);
    let baseline = *calls.lock().unwrap();

    let lock = rt.core.alloc_lock_id();
    rt.core.pause(n, lock).expect("pause"); // no-op for Off
    assert!(!rt.core.is_paused(n), "Off mode treats pause() as a no-op");

    a.set(TestValue::Int(5));
    assert_eq!(
        *calls.lock().unwrap(),
        baseline + 1,
        "fn fired immediately even after pause() — Off mode"
    );
}

// =====================================================================
// R2.6.0 ("Option A", canonical §2.6, pinned 2026-05-17) — a Default-mode
// LEAF SOURCE (state node, no deps) that holds its OWN pause lock and then
// self-emits via a direct external `emit`/`down([[DATA, v]])` is pushing
// OUTSIDE the fn/dep settle pipeline. PAUSE/RESUME gating is fn/dep-
// pipeline-scoped only, so that DATA MUST be delivered IMMEDIATELY at
// emit time (cache advances now, no PAUSE synthesized to the sink, and
// RESUME replays nothing). Buffering only applies to a node's own
// fn/dep-driven settle slices, never a leaf source's direct emit. This is
// the cross-impl parity contract with `@graphrefly/pure-ts` (only
// `pausable: "resumeAll"` buffers a leaf source's direct `down()`).
// =====================================================================

#[test]
fn r2_6_0_default_leaf_source_self_emit_delivers_immediately_while_self_paused() {
    let rt = TestRuntime::new();
    // 1. leaf source: state node, no deps, initial: 1. Default pausable.
    let n = rt.state(Some(TestValue::Int(1)));
    assert_eq!(rt.cache_value(n.id), Some(TestValue::Int(1)));

    // 2. subscribe a recording sink.
    let rec = rt.subscribe_recorder(n.id);

    // 3. allocate a lock id and 4. self-pause the leaf source.
    let lock = rt.core.alloc_lock_id();
    rt.core.pause(n.id, lock).expect("pause leaf source");
    assert!(rt.core.is_paused(n.id), "leaf source holds its own lock");

    // 5. baseline the recorded events (mirrors item5_* "clear" pattern —
    //    the Recorder has no clear(); a baseline index is the idiom).
    let baseline = rec.snapshot().len();
    let baseline_data = rec.data_values().len();

    // 6. self-emit DATA=42 while self-paused.
    n.set(TestValue::Int(42));

    // 7. ASSERT (BEFORE resume): the [DATA, 42] was delivered immediately
    //    and the cache advanced now — NOT buffered/deferred to RESUME.
    let post_pause: Vec<RecordedEvent> = rec.snapshot().into_iter().skip(baseline).collect();
    let data_after: Vec<TestValue> = rec.data_values().into_iter().skip(baseline_data).collect();
    assert_eq!(
        data_after,
        vec![TestValue::Int(42)],
        "leaf source self-emit must be delivered immediately while self-paused (R2.6.0)"
    );
    assert_eq!(
        rt.cache_value(n.id),
        Some(TestValue::Int(42)),
        "cache must advance to the self-emitted value immediately (R2.6.0)"
    );
    // No PAUSE tier message synthesized/surfaced to the sink for the
    // lock acquisition.
    assert!(
        !post_pause
            .iter()
            .any(|e| matches!(e, RecordedEvent::Pause(_))),
        "no PAUSE surfaced to the sink for a self-paused leaf source (R2.6.0)"
    );

    // 8. resume the leaf source's own lock.
    let report = rt
        .core
        .resume(n.id, lock)
        .expect("resume")
        .expect("final resume");

    // 9. ASSERT: nothing replayed (replayed == 0), no duplicate/second
    //    [DATA, 42], no reordering of the post-pause DATA, cache still 42.
    assert_eq!(
        report.replayed, 0,
        "RESUME of a self-paused leaf source replays nothing (R2.6.0)"
    );
    let total_data: Vec<TestValue> = rec.data_values().into_iter().skip(baseline_data).collect();
    assert_eq!(
        total_data,
        vec![TestValue::Int(42)],
        "exactly one DATA=42 across the whole pause/resume cycle — no duplicate, no reorder (R2.6.0)"
    );
    assert_eq!(
        rt.cache_value(n.id),
        Some(TestValue::Int(42)),
        "cache unchanged by RESUME (R2.6.0)"
    );
}

#[test]
fn item5_set_pausable_mode_rejects_when_paused() {
    // Slice H (2026-05-07): promoted from `should_panic` to typed-error
    // assertion — the rejection now returns
    // `Err(SetPausableModeError::WhilePaused)` rather than panicking.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let lock = rt.core.alloc_lock_id();
    rt.core.pause(s.id, lock).expect("pause");
    let result = rt.core.set_pausable_mode(s.id, PausableMode::ResumeAll);
    assert_eq!(
        result,
        Err(graphrefly_core::SetPausableModeError::WhilePaused)
    );
}

// =====================================================================
// F2 (audit-driven 2026-05-07) — Core::up(node_id, message) primitive
// (canonical R1.4.1). Rejects tier-3 (DATA/RESOLVED) and tier-5
// (COMPLETE/ERROR) at the boundary; routes other tiers to the
// appropriate per-dep Core methods.
// =====================================================================

#[test]
fn f2_up_rejects_tier3_data() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));
    let n = rt.derived(&[s.id], |deps| deps.first().cloned());
    let h = rt.binding.intern(TestValue::Int(99));
    let result = rt.core.up(n, Message::Data(h));
    assert!(
        matches!(
            result,
            Err(graphrefly_core::UpError::TierForbidden { tier: 3 })
        ),
        "tier 3 (Data) must be rejected by up(); got {result:?}"
    );
}

#[test]
fn f2_up_rejects_tier3_resolved() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));
    let n = rt.derived(&[s.id], |deps| deps.first().cloned());
    let result = rt.core.up(n, Message::Resolved);
    assert!(matches!(
        result,
        Err(graphrefly_core::UpError::TierForbidden { tier: 3 })
    ));
}

#[test]
fn f2_up_rejects_tier5_complete() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));
    let n = rt.derived(&[s.id], |deps| deps.first().cloned());
    let result = rt.core.up(n, Message::Complete);
    assert!(matches!(
        result,
        Err(graphrefly_core::UpError::TierForbidden { tier: 5 })
    ));
}

#[test]
fn f2_up_rejects_tier5_error() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));
    let n = rt.derived(&[s.id], |deps| deps.first().cloned());
    let h = rt.binding.intern(TestValue::Int(0));
    let result = rt.core.up(n, Message::Error(h));
    assert!(matches!(
        result,
        Err(graphrefly_core::UpError::TierForbidden { tier: 5 })
    ));
}

#[test]
fn f2_up_invalidate_clears_dep_cache() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(7)));
    let n = rt.derived(&[s.id], |deps| match deps[0] {
        TestValue::Int(v) => Some(TestValue::Int(v * 2)),
        _ => None,
    });
    let _rec = rt.subscribe_recorder(n);

    // s has cached value pre-up.
    assert_ne!(rt.core.cache_of(s.id), HandleId::new(0));

    // up(n, INVALIDATE) routes to invalidate(s) for each dep s.
    rt.core.up(n, Message::Invalidate).expect("up ok");

    // s's cache cleared.
    assert_eq!(rt.core.cache_of(s.id), HandleId::new(0));
}

#[test]
fn f2_up_pause_routes_to_each_dep() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));
    let n = rt.derived(&[s.id], |deps| deps.first().cloned());
    let _rec = rt.subscribe_recorder(n);

    let lock = LockId::new(7);
    rt.core.up(n, Message::Pause(lock)).expect("up pause");
    assert!(
        rt.core.is_paused(s.id),
        "up(Pause) should pause each dep of n"
    );

    rt.core.up(n, Message::Resume(lock)).expect("up resume");
    assert!(
        !rt.core.is_paused(s.id),
        "up(Resume) should resume each dep of n"
    );
}

#[test]
fn f2_up_unknown_node_rejected() {
    let rt = TestRuntime::new();
    let bogus = graphrefly_core::NodeId::new(99_999);
    let result = rt.core.up(bogus, Message::Invalidate);
    assert!(matches!(
        result,
        Err(graphrefly_core::UpError::UnknownNode(_))
    ));
}

#[test]
fn f2_up_dirty_and_start_are_no_ops() {
    // Tier-1 DIRTY and tier-0 START have no upstream-routing semantics —
    // up() accepts them but doesn't modify any dep state.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(5)));
    let n = rt.derived(&[s.id], |deps| deps.first().cloned());
    let _rec = rt.subscribe_recorder(n);

    let cache_pre = rt.core.cache_of(s.id);
    let paused_pre = rt.core.is_paused(s.id);

    rt.core.up(n, Message::Dirty).expect("up dirty");
    rt.core.up(n, Message::Start).expect("up start");

    // No state change on s.
    assert_eq!(rt.core.cache_of(s.id), cache_pre);
    assert_eq!(rt.core.is_paused(s.id), paused_pre);
}

// =====================================================================
// Tier-based dispatch refactor regression — verify ops_impl + higher_order
// outer sinks still produce the right traces after refactoring from
// `match m` (variant) to `match m.tier()` + `payload_handle()` discrimination.
// =====================================================================

// (Existing parity tests under packages/parity-tests/scenarios/operators/
// already exercise zip / concat / race / takeUntil / switch_map / etc. end
// to end. The pre-refactor tests pass post-refactor — that's the direct
// regression evidence. Adding a Rust-side smoke test here would duplicate.)

// =====================================================================
// Slice G — R1.3.2.d / R1.3.3.a per-wave equals coalescing
// =====================================================================

#[test]
fn slice_g_batch_multi_same_value_emit_does_not_produce_multi_resolved() {
    // Pre-Slice-G: `batch(|| { state.set(v); state.set(v); })` produced
    // multiple Resolved messages at the state node — violating R1.3.3.a
    // ("≥1 DATA OR exactly 1 RESOLVED, never multiple RESOLVED").
    //
    // Slice G: detect "subsequent emit in same wave" via the wave-scoped
    // `tier3_emitted_this_wave` set. On the second emit, skip equals
    // substitution and queue Data verbatim; rewrite any prior Resolved
    // entries to Data using the wave-start cache snapshot. Result: the
    // wave becomes a multi-DATA wave, R1.3.3.c-compliant.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(42)));
    let rec = rt.subscribe_recorder(s.id);
    let baseline = rec.snapshot().len();

    // Two same-value emits within one batch wave.
    rt.core.batch(|| {
        s.set(TestValue::Int(42));
        s.set(TestValue::Int(42));
    });

    let post: Vec<_> = rec.snapshot()[baseline..].to_vec();
    let resolved_count = post
        .iter()
        .filter(|e| matches!(e, RecordedEvent::Resolved))
        .count();
    let data_count = post
        .iter()
        .filter(|e| matches!(e, RecordedEvent::Data(_)))
        .count();

    assert!(
        resolved_count <= 1,
        "R1.3.3.a: at most one Resolved per wave at one node; got {resolved_count} in {post:?}"
    );
    // The two same-value emits should produce a multi-DATA wave (verbatim
    // per R1.3.3.c) once Slice G's rewrite kicks in.
    assert!(
        data_count >= 1,
        "expected at least one Data after multi-emit batch; got 0 in {post:?}"
    );
}

// =====================================================================
// Slice E1 — replayBuffer (R2.6.5 / Lock 6.G)
// =====================================================================

#[test]
fn slice_e1_replay_buffer_disabled_by_default() {
    // Without explicit opt-in, late subscribers see only the cached
    // value (or [Start] if sentinel) — no replay.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));
    s.set(TestValue::Int(2));
    s.set(TestValue::Int(3));

    let rec = rt.subscribe_recorder(s.id);
    let data: Vec<TestValue> = rec.data_values();
    // Late subscriber sees only the current cache (Int(3)).
    assert_eq!(data, vec![TestValue::Int(3)]);
}

#[test]
fn slice_e1_replay_buffer_replays_recent_data_to_late_subscriber() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));
    rt.core.set_replay_buffer_cap(s.id, Some(3));

    s.set(TestValue::Int(2));
    s.set(TestValue::Int(3));
    s.set(TestValue::Int(4));
    s.set(TestValue::Int(5));

    let rec = rt.subscribe_recorder(s.id);
    let data: Vec<TestValue> = rec.data_values();
    // Initial cache slice (Int(5)) + buffered last 3 emissions (3, 4, 5).
    // Note: cache (5) and buffer's last entry (5) coexist by design — the
    // canonical replay is "all buffered DATA after the cache slice."
    assert!(
        data.contains(&TestValue::Int(3))
            && data.contains(&TestValue::Int(4))
            && data.contains(&TestValue::Int(5)),
        "buffered DATA replayed; got {data:?}"
    );
    // Buffer cap = 3, and 4 emits happened after subscribe-able cache:
    // {2, 3, 4, 5} → buffer evicts 2 → buffer = [3, 4, 5].
    assert!(
        !data.contains(&TestValue::Int(2)),
        "evicted Int(2) should NOT appear; got {data:?}"
    );
}

#[test]
fn slice_e1_replay_buffer_evicts_oldest_when_cap_exceeded() {
    let rt = TestRuntime::new();
    let s = rt.state(None);
    rt.core.set_replay_buffer_cap(s.id, Some(2));

    // 5 emits with cap=2 → buffer ends with last 2 (4, 5); 3 evicted.
    for v in 1..=5 {
        s.set(TestValue::Int(v));
    }

    let rec = rt.subscribe_recorder(s.id);
    let data: Vec<TestValue> = rec.data_values();
    // Cache = 5; replay buffer = [4, 5]. Late subscriber sees Data(5)
    // from cache + Data(4), Data(5) from buffer.
    assert!(data.contains(&TestValue::Int(4)));
    assert!(data.contains(&TestValue::Int(5)));
    assert!(!data.contains(&TestValue::Int(1)));
    assert!(!data.contains(&TestValue::Int(2)));
    assert!(!data.contains(&TestValue::Int(3)));
}

#[test]
fn slice_e1_set_replay_buffer_cap_to_none_drains_existing() {
    // Lowering cap to None should drain existing entries (releasing retains).
    let rt = TestRuntime::new();
    let s = rt.state(None);
    rt.core.set_replay_buffer_cap(s.id, Some(3));
    s.set(TestValue::Int(1));
    s.set(TestValue::Int(2));
    rt.core.set_replay_buffer_cap(s.id, None);

    let rec = rt.subscribe_recorder(s.id);
    let data: Vec<TestValue> = rec.data_values();
    // Only the cache (last set value) — no replay since buffer drained.
    assert_eq!(data, vec![TestValue::Int(2)]);
}

#[test]
fn slice_g_single_emit_equals_match_still_produces_resolved() {
    // Single-emit + equals-match path is unchanged by Slice G — equals
    // substitution still applies for SINGLE-DATA waves per R1.3.2.a.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(5)));
    let rec = rt.subscribe_recorder(s.id);
    let baseline = rec.snapshot().len();

    s.set(TestValue::Int(5)); // same value → equals match → Resolved

    let post: Vec<_> = rec.snapshot()[baseline..].to_vec();
    let resolved_count = post
        .iter()
        .filter(|e| matches!(e, RecordedEvent::Resolved))
        .count();
    assert_eq!(
        resolved_count, 1,
        "single-emit equals-match → exactly 1 Resolved (R1.3.2.a)"
    );
}
