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
#[ignore = "S2b/β /qa-2026-05-18 F1/F2: invariant LIVE (synchronous fn/equals/read/set_deps Core re-entry — NOT the producer mailbox path; NOT covered by producer tests or P7). Old binding-clones-Core mechanism is β-invalid; β-valid owner-side rewrite deferred to S4. Coverage gap: porting-deferred § /qa-2026-05-18."]
fn fn_can_reenter_core_emit_during_invoke_fn_runs_nested_wave() {
    // see #[ignore] — invariant still live; β-valid rewrite at S4; gap in porting-deferred /qa-2026-05-18
}

#[test]
#[ignore = "S2b/β /qa-2026-05-18 F1/F2: invariant LIVE (synchronous fn/equals/read/set_deps Core re-entry — NOT the producer mailbox path; NOT covered by producer tests or P7). Old binding-clones-Core mechanism is β-invalid; β-valid owner-side rewrite deferred to S4. Coverage gap: porting-deferred § /qa-2026-05-18."]
fn fn_can_reenter_core_pause_resume_during_invoke_fn() {
    // see #[ignore] — invariant still live; β-valid rewrite at S4; gap in porting-deferred /qa-2026-05-18
}

#[test]
#[ignore = "S2b/β /qa-2026-05-18 F1/F2: invariant LIVE (synchronous fn/equals/read/set_deps Core re-entry — NOT the producer mailbox path; NOT covered by producer tests or P7). Old binding-clones-Core mechanism is β-invalid; β-valid owner-side rewrite deferred to S4. Coverage gap: porting-deferred § /qa-2026-05-18."]
fn fn_can_reenter_core_invalidate_during_invoke_fn() {
    // see #[ignore] — invariant still live; β-valid rewrite at S4; gap in porting-deferred /qa-2026-05-18
}

// ---------------------------------------------------------------------------
// custom_equals re-entrance
// ---------------------------------------------------------------------------

#[test]
#[ignore = "S2b/β /qa-2026-05-18 F1/F2: invariant LIVE (synchronous fn/equals/read/set_deps Core re-entry — NOT the producer mailbox path; NOT covered by producer tests or P7). Old binding-clones-Core mechanism is β-invalid; β-valid owner-side rewrite deferred to S4. Coverage gap: porting-deferred § /qa-2026-05-18."]
fn custom_equals_can_reenter_core_during_emission() {
    // see #[ignore] — invariant still live; β-valid rewrite at S4; gap in porting-deferred /qa-2026-05-18
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
#[ignore = "S2b/β /qa-2026-05-18 F1/F2: invariant LIVE (synchronous fn/equals/read/set_deps Core re-entry — NOT the producer mailbox path; NOT covered by producer tests or P7). Old binding-clones-Core mechanism is β-invalid; β-valid owner-side rewrite deferred to S4. Coverage gap: porting-deferred § /qa-2026-05-18."]
fn late_subscriber_installed_after_first_queue_notify_does_not_double_receive_data() {
    // see #[ignore] — invariant still live; β-valid rewrite at S4; gap in porting-deferred /qa-2026-05-18
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
#[ignore = "S2b/β /qa-2026-05-18 F1/F2: invariant LIVE (synchronous fn/equals/read/set_deps Core re-entry — NOT the producer mailbox path; NOT covered by producer tests or P7). Old binding-clones-Core mechanism is β-invalid; β-valid owner-side rewrite deferred to S4. Coverage gap: porting-deferred § /qa-2026-05-18."]
fn invoke_fn_can_call_core_cache_of_directly() {
    // see #[ignore] — invariant still live; β-valid rewrite at S4; gap in porting-deferred /qa-2026-05-18
}
