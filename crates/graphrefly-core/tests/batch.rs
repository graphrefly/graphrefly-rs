#![allow(dead_code)] // Some Diamond fixture fields are referenced only by name, not read.

//! Slice C-1.5 — user-facing `batch()` API + R1.3.1.b cross-node DIRTY-first
//! propagation tests.
//!
//! Two concerns share this file because they're verified by the same harness:
//!
//! 1. **R1.3.1.b two-phase propagation.** Phase 1 (DIRTY) propagates through
//!    the entire graph before phase 2 (DATA / RESOLVED) begins. Verified by
//!    a `GlobalLog` shared across multi-node subscribers — every (node,
//!    Dirty) entry must precede every (any-other-node, Data/Resolved/
//!    Invalidate) entry within the same wave.
//!
//! 2. **`Core::batch` / `Core::begin_batch`.** Multiple emits inside a batch
//!    coalesce into one wave. Verified via fan-in topologies (D fires once),
//!    nested batches, RAII guard form, panic-on-batch behavior.
//!
//! The "strict diamond" tests are the user's explicit ask: verify R1.3.1.b
//! holds under complex graphs and under batch coalescing. If a future
//! refactor regresses cross-node DIRTY-first ordering, these are the first
//! tests to fail.

mod common;

use std::sync::{Arc, Mutex};

use graphrefly_core::{Message, NodeId, Sink};

use common::{TestRuntime, TestValue};

// ---------------------------------------------------------------------------
// Global event log — used to verify cross-node ordering invariants
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
enum LogTier {
    Dirty,
    Tier3, // Data/Resolved
    Invalidate,
    Complete,
    Error,
    Teardown,
    Start,
    Pause,
    Resume,
}

#[derive(Clone)]
struct GlobalLog {
    inner: Arc<Mutex<Vec<(NodeId, LogTier)>>>,
}

impl GlobalLog {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn sink_for(&self, node: NodeId) -> Sink {
        let inner = self.inner.clone();
        Arc::new(move |msgs: &[Message]| {
            let mut g = inner.lock().expect("global log");
            for m in msgs {
                let tier = match m {
                    Message::Start => LogTier::Start,
                    Message::Dirty => LogTier::Dirty,
                    Message::Data(_) | Message::Resolved => LogTier::Tier3,
                    Message::Invalidate => LogTier::Invalidate,
                    Message::Complete => LogTier::Complete,
                    Message::Error(_) => LogTier::Error,
                    Message::Teardown => LogTier::Teardown,
                    Message::Pause(_) => LogTier::Pause,
                    Message::Resume(_) => LogTier::Resume,
                };
                g.push((node, tier));
            }
        })
    }

    fn snapshot(&self) -> Vec<(NodeId, LogTier)> {
        self.inner.lock().expect("global log").clone()
    }

    /// Truncate so subsequent assertions ignore activation chatter.
    fn reset(&self) {
        self.inner.lock().expect("global log").clear();
    }
}

/// Asserts cross-node DIRTY-first: within `entries`, every `Dirty` entry
/// at any node precedes every `Tier3` / `Invalidate` entry at any node.
/// (Subset of R1.3.1.b — the part observable post-wave at sink delivery.)
#[track_caller]
fn assert_dirty_before_settle(entries: &[(NodeId, LogTier)]) {
    let last_dirty_idx = entries
        .iter()
        .rposition(|(_, t)| matches!(t, LogTier::Dirty));
    let first_settle_idx = entries
        .iter()
        .position(|(_, t)| matches!(t, LogTier::Tier3 | LogTier::Invalidate));
    if let (Some(d), Some(s)) = (last_dirty_idx, first_settle_idx) {
        assert!(
            d < s,
            "R1.3.1.b violation: settle (idx {s}) precedes a DIRTY (idx {d}) in {entries:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Diamond — A → {B, C} → D
// ---------------------------------------------------------------------------

struct Diamond {
    runtime: TestRuntime,
    a: NodeId,
    b: NodeId,
    c: NodeId,
    d: NodeId,
    log: GlobalLog,
}

impl Diamond {
    fn new() -> Self {
        let runtime = TestRuntime::new();
        let a_state = runtime.state(Some(TestValue::Int(0)));
        let a = a_state.id;
        let b = runtime.derived(&[a], |deps| match &deps[0] {
            TestValue::Int(n) => Some(TestValue::Int(n * 2)),
            _ => None,
        });
        let c = runtime.derived(&[a], |deps| match &deps[0] {
            TestValue::Int(n) => Some(TestValue::Int(n + 100)),
            _ => None,
        });
        let d = runtime.derived(&[b, c], |deps| match (&deps[0], &deps[1]) {
            (TestValue::Int(b), TestValue::Int(c)) => Some(TestValue::Int(b + c)),
            _ => None,
        });
        let log = GlobalLog::new();
        // Subscribe in topo order (A, B, C, D) so insertion-order
        // `pending_notify` iteration produces a deterministic test sequence.
        for n in [a, b, c, d] {
            std::mem::forget(runtime.core.subscribe(n, log.sink_for(n)));
        }
        // Reset to ignore activation chatter. `a_state` (StateHandle) is
        // dropped here, but the state node itself stays registered in Core;
        // `emit_a` interns + emits directly via `runtime.binding`/`runtime.core`.
        log.reset();
        Self {
            runtime,
            a,
            b,
            c,
            d,
            log,
        }
    }

    fn emit_a(&self, value: i64) {
        let h = self.runtime.binding.intern(TestValue::Int(value));
        self.runtime.core.emit(self.a, h);
    }
}

#[test]
fn r1_3_1_b_diamond_dirty_first_across_nodes() {
    let g = Diamond::new();
    g.emit_a(7);
    let entries = g.log.snapshot();
    // All four nodes (A, B, C, D) should have entries (each has Dirty + Tier3).
    let nodes_with_dirty: Vec<NodeId> = entries
        .iter()
        .filter_map(|(n, t)| {
            if matches!(t, LogTier::Dirty) {
                Some(*n)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(
        nodes_with_dirty.len(),
        4,
        "expected one DIRTY per node (A,B,C,D); got {entries:?}"
    );
    assert_dirty_before_settle(&entries);
}

#[test]
fn r2_7_1_diamond_d_fires_once_per_wave() {
    let g = Diamond::new();
    g.emit_a(5);
    let entries = g.log.snapshot();
    let d_settles = entries
        .iter()
        .filter(|(n, t)| *n == g.d && matches!(t, LogTier::Tier3))
        .count();
    assert_eq!(d_settles, 1, "D should settle exactly once per source emit");
    let d_dirty = entries
        .iter()
        .filter(|(n, t)| *n == g.d && matches!(t, LogTier::Dirty))
        .count();
    assert_eq!(d_dirty, 1, "D should dirty exactly once per source emit");
}

#[test]
fn r1_3_1_b_diamond_multi_emit_outside_batch_still_orders_dirty_first() {
    // No batch — each emit is its own wave. Per-wave R1.3.1.b should hold;
    // across waves, DIRTY/Tier3 alternate.
    let g = Diamond::new();
    g.emit_a(1);
    g.emit_a(2);
    let entries = g.log.snapshot();
    // Slice the log into per-wave segments by walking and starting a new
    // segment after every Tier3 burst followed by a Dirty.
    let mut segments: Vec<Vec<(NodeId, LogTier)>> = vec![Vec::new()];
    let mut last_was_settle = false;
    for e in &entries {
        if last_was_settle && matches!(e.1, LogTier::Dirty) {
            segments.push(Vec::new());
        }
        last_was_settle = matches!(e.1, LogTier::Tier3 | LogTier::Invalidate);
        segments.last_mut().unwrap().push(e.clone());
    }
    for seg in &segments {
        assert_dirty_before_settle(seg);
    }
}

// ---------------------------------------------------------------------------
// Wider topology — A → {B, C, E}; D = combine(B, C); F = combine(D, E)
// Triple fan-in feeding into a two-stage diamond.
// ---------------------------------------------------------------------------

#[test]
fn r1_3_1_b_complex_topology_dirty_first() {
    let runtime = TestRuntime::new();
    let a_state = runtime.state(Some(TestValue::Int(0)));
    let a = a_state.id;
    let mk_unary = |coeff: i64, offset: i64| {
        move |deps: &[TestValue]| match &deps[0] {
            TestValue::Int(n) => Some(TestValue::Int(n * coeff + offset)),
            _ => None,
        }
    };
    let b = runtime.derived(&[a], mk_unary(2, 0));
    let c = runtime.derived(&[a], mk_unary(1, 100));
    let e = runtime.derived(&[a], mk_unary(3, 1));
    let d = runtime.derived(&[b, c], |deps| match (&deps[0], &deps[1]) {
        (TestValue::Int(b), TestValue::Int(c)) => Some(TestValue::Int(b + c)),
        _ => None,
    });
    let f = runtime.derived(&[d, e], |deps| match (&deps[0], &deps[1]) {
        (TestValue::Int(d), TestValue::Int(e)) => Some(TestValue::Int(d + e)),
        _ => None,
    });

    let log = GlobalLog::new();
    for &n in &[a, b, c, d, e, f] {
        std::mem::forget(runtime.core.subscribe(n, log.sink_for(n)));
    }
    log.reset();

    let h = runtime.binding.intern(TestValue::Int(10));
    runtime.core.emit(a, h);

    let entries = log.snapshot();
    assert_dirty_before_settle(&entries);

    // Each of the 6 nodes should fire exactly once this wave.
    for n in [a, b, c, d, e, f] {
        let dirty_count = entries
            .iter()
            .filter(|(nn, t)| *nn == n && matches!(t, LogTier::Dirty))
            .count();
        let settle_count = entries
            .iter()
            .filter(|(nn, t)| *nn == n && matches!(t, LogTier::Tier3))
            .count();
        assert_eq!(dirty_count, 1, "node {n:?} should DIRTY once");
        assert_eq!(settle_count, 1, "node {n:?} should settle once");
    }

    // F's value: f = (b + c) + e = (20 + 110) + 31 = 161.
    if let TestValue::Int(v) = runtime.cache_value(f).unwrap() {
        assert_eq!(v, 161);
    } else {
        panic!("expected Int");
    }
}

// ---------------------------------------------------------------------------
// User-facing batch — coalesce multiple emits into one wave
// ---------------------------------------------------------------------------

#[test]
fn batch_closure_coalesces_two_state_emits_into_one_wave() {
    let runtime = TestRuntime::new();
    let a_state = runtime.state(Some(TestValue::Int(0)));
    let b_state = runtime.state(Some(TestValue::Int(100)));
    let sum = runtime.derived(&[a_state.id, b_state.id], |deps| {
        match (&deps[0], &deps[1]) {
            (TestValue::Int(a), TestValue::Int(b)) => Some(TestValue::Int(a + b)),
            _ => None,
        }
    });
    let rec = runtime.subscribe_recorder(sum);
    let baseline_data_count = rec
        .snapshot()
        .iter()
        .filter(|e| matches!(e, common::RecordedEvent::Data(_)))
        .count();

    let h_a = runtime.binding.intern(TestValue::Int(7));
    let h_b = runtime.binding.intern(TestValue::Int(13));
    runtime.core.batch(|| {
        runtime.core.emit(a_state.id, h_a);
        runtime.core.emit(b_state.id, h_b);
    });

    let post = rec.snapshot();
    let post_data_count = post
        .iter()
        .filter(|e| matches!(e, common::RecordedEvent::Data(_)))
        .count();
    let new_data = post_data_count - baseline_data_count;
    assert_eq!(
        new_data, 1,
        "sum should DATA exactly once after coalesced batch — got events {post:?}"
    );

    if let TestValue::Int(v) = runtime.cache_value(sum).unwrap() {
        assert_eq!(v, 7 + 13);
    } else {
        panic!("expected Int");
    }
}

#[test]
fn batch_diamond_d_fires_once_when_two_state_emits_coalesce() {
    // Diamond where both leaves of the diamond depend on different state
    // sources. Without batch(), two source emits would fan out to two D
    // settles. With batch(), D fires once.
    let runtime = TestRuntime::new();
    let s_left = runtime.state(Some(TestValue::Int(0)));
    let s_right = runtime.state(Some(TestValue::Int(0)));
    let b = runtime.derived(&[s_left.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(n * 2)),
        _ => None,
    });
    let c = runtime.derived(&[s_right.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(n + 100)),
        _ => None,
    });
    let d_fire_count = Arc::new(Mutex::new(0u32));
    let d_fire_count_inner = d_fire_count.clone();
    let d = runtime.derived(&[b, c], move |deps| {
        *d_fire_count_inner.lock().expect("d count") += 1;
        match (&deps[0], &deps[1]) {
            (TestValue::Int(b), TestValue::Int(c)) => Some(TestValue::Int(b + c)),
            _ => None,
        }
    });
    let _rec = runtime.subscribe_recorder(d);
    let baseline = *d_fire_count.lock().expect("d count");

    let h_l = runtime.binding.intern(TestValue::Int(5));
    let h_r = runtime.binding.intern(TestValue::Int(10));
    runtime.core.batch(|| {
        runtime.core.emit(s_left.id, h_l);
        runtime.core.emit(s_right.id, h_r);
    });

    let after = *d_fire_count.lock().expect("d count");
    assert_eq!(
        after - baseline,
        1,
        "D fn should fire exactly once for the coalesced wave (was {baseline}, now {after})"
    );
    if let TestValue::Int(v) = runtime.cache_value(d).unwrap() {
        assert_eq!(v, 10 + 110); // (5*2) + (10+100)
    }
}

#[test]
fn batch_nested_only_outer_drains() {
    let runtime = TestRuntime::new();
    let s = runtime.state(Some(TestValue::Int(0)));
    let derived = runtime.derived(&[s.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(n + 1)),
        _ => None,
    });
    let rec = runtime.subscribe_recorder(derived);
    let pre = rec
        .snapshot()
        .iter()
        .filter(|e| matches!(e, common::RecordedEvent::Data(_)))
        .count();

    let h1 = runtime.binding.intern(TestValue::Int(1));
    let h2 = runtime.binding.intern(TestValue::Int(2));
    let h3 = runtime.binding.intern(TestValue::Int(3));
    runtime.core.batch(|| {
        runtime.core.emit(s.id, h1);
        runtime.core.batch(|| {
            runtime.core.emit(s.id, h2);
        });
        runtime.core.emit(s.id, h3);
    });

    let post = rec
        .snapshot()
        .iter()
        .filter(|e| matches!(e, common::RecordedEvent::Data(_)))
        .count();
    // The wave settles once; coalesced multi-emit on the same source still
    // yields a single derived re-fire. Inner batch does NOT drain mid-wave.
    assert_eq!(
        post - pre,
        1,
        "nested batch should not pre-drain; outer should drain once"
    );
    if let TestValue::Int(v) = runtime.cache_value(derived).unwrap() {
        assert_eq!(v, 4); // 3 + 1
    }
}

#[test]
fn begin_batch_raii_drains_on_drop() {
    let runtime = TestRuntime::new();
    let s = runtime.state(Some(TestValue::Int(0)));
    let derived = runtime.derived(&[s.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(n * 2)),
        _ => None,
    });
    let rec = runtime.subscribe_recorder(derived);
    let pre = rec
        .snapshot()
        .iter()
        .filter(|e| matches!(e, common::RecordedEvent::Data(_)))
        .count();

    {
        let _g = runtime.core.begin_batch();
        let h1 = runtime.binding.intern(TestValue::Int(7));
        let h2 = runtime.binding.intern(TestValue::Int(11));
        runtime.core.emit(s.id, h1);
        runtime.core.emit(s.id, h2);
        // No drain yet — pre-drop; verify cache propagation hasn't happened
        // at the derived yet (the source's own emit-time DIRTY broadcast
        // doesn't fire derived's fn — that's the wave drain's job).
        // We can't observe derived's cache directly without firing fn, but
        // we can check the event count: should still be at baseline.
        let mid = rec
            .snapshot()
            .iter()
            .filter(|e| matches!(e, common::RecordedEvent::Data(_)))
            .count();
        assert_eq!(
            mid,
            pre,
            "no drain expected mid-batch (BatchGuard alive); got events {:?}",
            rec.snapshot()
        );
    }
    let post = rec
        .snapshot()
        .iter()
        .filter(|e| matches!(e, common::RecordedEvent::Data(_)))
        .count();
    assert_eq!(post - pre, 1, "BatchGuard drop should drain the wave");
    if let TestValue::Int(v) = runtime.cache_value(derived).unwrap() {
        assert_eq!(v, 22); // 11 * 2
    }
}

#[test]
fn batch_with_complete_inside_drains_terminal_at_scope_end() {
    let runtime = TestRuntime::new();
    let s = runtime.state(Some(TestValue::Int(0)));
    let rec = runtime.subscribe_recorder(s.id);
    // Trim activation chatter (`[Start, Data(0)]`) so we assert only on the
    // batch's wave.
    let baseline_len = rec.snapshot().len();

    runtime.core.batch(|| {
        let h = runtime.binding.intern(TestValue::Int(5));
        runtime.core.emit(s.id, h);
        runtime.core.complete(s.id);
    });

    let events = rec.snapshot();
    let new_events = &events[baseline_len..];
    // Post-baseline, s should observe Dirty, Data(5), Complete in that order
    // — DIRTY before tier-3, tier-3 before tier-5, all delivered at scope
    // end (R1.3.1.a per-node, R1.3.7 phase ordering).
    let kinds: Vec<&str> = new_events
        .iter()
        .map(|e| match e {
            common::RecordedEvent::Dirty => "Dirty",
            common::RecordedEvent::Data(_) => "Data",
            common::RecordedEvent::Resolved => "Resolved",
            common::RecordedEvent::Complete => "Complete",
            _ => "other",
        })
        .collect();
    let p_dirty = kinds.iter().position(|x| *x == "Dirty");
    let p_data = kinds.iter().position(|x| *x == "Data");
    let p_complete = kinds.iter().position(|x| *x == "Complete");
    assert!(
        matches!((p_dirty, p_data, p_complete), (Some(d), Some(da), Some(c)) if d < da && da < c),
        "tier ordering violated; new events {new_events:?}"
    );
}

#[test]
fn batch_panic_restores_state_node_caches() {
    // Slice A-bigger /qa item B: when a `Core::batch` closure panics,
    // BatchGuard::drop's discard path now restores state-node caches to
    // their pre-wave values. Atomicity guarantee covers BOTH:
    //   - sink-observability (no tier-3+ delivery from the wave)
    //   - state (cache_of returns pre-panic value, not the partially-
    //     committed mid-closure value)
    use std::panic;
    let runtime = TestRuntime::new();
    let s = runtime.state(Some(TestValue::Int(7)));
    let s_id = s.id;
    let initial_cache = runtime.cache_value(s_id).expect("initial cache");
    assert_eq!(initial_cache, TestValue::Int(7));

    let core = runtime.core.clone();
    let binding = runtime.binding.clone();
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        core.batch(|| {
            let h_99 = binding.intern(TestValue::Int(99));
            core.emit(s_id, h_99);
            // At this point cache holds 99 (committed inline).
            let mid = core.cache_of(s_id);
            assert_eq!(mid, h_99, "mid-batch cache should hold the new value");
            panic!("user code threw mid-batch");
        });
    }));
    assert!(result.is_err(), "panic should propagate out of batch()");

    // Cache restored to pre-wave value (7), NOT the committed 99.
    let post_cache = runtime.cache_value(s_id).expect("post-panic cache");
    assert_eq!(
        post_cache,
        TestValue::Int(7),
        "BatchGuard panic-discard should restore state-node cache to pre-wave value"
    );
}

#[test]
fn batch_panic_restores_multi_emit_state_node_to_first_pre_wave_value() {
    // Multi-emit: 3 emits inside a panicked batch. Restore should roll
    // back to the value BEFORE the first emit, not the second-to-last.
    use std::panic;
    let runtime = TestRuntime::new();
    let s = runtime.state(Some(TestValue::Int(0)));
    let s_id = s.id;
    let core = runtime.core.clone();
    let binding = runtime.binding.clone();
    let _ = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        core.batch(|| {
            let h1 = binding.intern(TestValue::Int(1));
            let h2 = binding.intern(TestValue::Int(2));
            let h3 = binding.intern(TestValue::Int(3));
            core.emit(s_id, h1);
            core.emit(s_id, h2);
            core.emit(s_id, h3);
            panic!("user threw");
        });
    }));
    assert_eq!(
        runtime.cache_value(s_id).unwrap(),
        TestValue::Int(0),
        "panic-discard should restore to the value BEFORE the first emit (0), not the partially-committed last value"
    );
}

#[test]
fn batch_success_does_not_revert_caches() {
    // Sanity check: success path keeps the new cache values (no false
    // restoration).
    let runtime = TestRuntime::new();
    let s = runtime.state(Some(TestValue::Int(0)));
    let s_id = s.id;
    let core = runtime.core.clone();
    let binding = runtime.binding.clone();
    core.batch(|| {
        let h = binding.intern(TestValue::Int(42));
        core.emit(s_id, h);
    });
    assert_eq!(
        runtime.cache_value(s_id).unwrap(),
        TestValue::Int(42),
        "successful batch should commit the new cache"
    );
}

#[test]
fn batch_panic_discards_pending_wave() {
    use std::panic;
    let runtime = TestRuntime::new();
    let s = runtime.state(Some(TestValue::Int(0)));
    let derived = runtime.derived(&[s.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(n + 1)),
        _ => None,
    });
    let rec = runtime.subscribe_recorder(derived);
    let pre = rec
        .snapshot()
        .iter()
        .filter(|e| matches!(e, common::RecordedEvent::Data(_)))
        .count();

    let core = runtime.core.clone();
    let s_id = s.id;
    let binding = runtime.binding.clone();
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        core.batch(|| {
            let h = binding.intern(TestValue::Int(42));
            core.emit(s_id, h);
            panic!("user code threw mid-batch");
        });
    }));
    assert!(
        result.is_err(),
        "expected panic to propagate out of batch()"
    );

    // The state node's cache DID advance (emit committed before the panic),
    // but the derived's wave drain was discarded — its sink should not have
    // observed the new wave's tier-3 messages.
    let post = rec
        .snapshot()
        .iter()
        .filter(|e| matches!(e, common::RecordedEvent::Data(_)))
        .count();
    assert_eq!(
        post,
        pre,
        "derived sink should not see tier-3 from a panicked batch; got events {:?}",
        rec.snapshot()
    );

    // And the in-tick state should be cleared — a fresh emit after the
    // panic should drive a normal wave.
    let h2 = runtime.binding.intern(TestValue::Int(100));
    runtime.core.emit(s.id, h2);
    let after_recover = rec
        .snapshot()
        .iter()
        .filter(|e| matches!(e, common::RecordedEvent::Data(_)))
        .count();
    assert!(
        after_recover > post,
        "post-panic recovery emit should drive a normal wave; events {:?}",
        rec.snapshot()
    );
}

#[test]
fn batch_with_pause_resume_buffers_and_replays_at_resume() {
    let runtime = TestRuntime::new();
    let s = runtime.state(Some(TestValue::Int(0)));
    let derived = runtime.derived(&[s.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(n * 10)),
        _ => None,
    });
    let rec = runtime.subscribe_recorder(derived);
    let pre = rec
        .snapshot()
        .iter()
        .filter(|e| matches!(e, common::RecordedEvent::Data(_)))
        .count();
    let lock = runtime.core.alloc_lock_id();

    runtime.core.batch(|| {
        runtime.core.pause(derived, lock).expect("pause");
        let h1 = runtime.binding.intern(TestValue::Int(1));
        let h2 = runtime.binding.intern(TestValue::Int(2));
        runtime.core.emit(s.id, h1);
        runtime.core.emit(s.id, h2);
    });

    // Still paused — derived sink should not have observed any tier-3
    // messages from the wave (they're in the pause buffer).
    let mid = rec
        .snapshot()
        .iter()
        .filter(|e| matches!(e, common::RecordedEvent::Data(_)))
        .count();
    assert_eq!(
        mid,
        pre,
        "paused derived should not emit tier-3 mid-batch; got events {:?}",
        rec.snapshot()
    );

    let report = runtime
        .core
        .resume(derived, lock)
        .expect("resume")
        .expect("final lock report");
    let _ = report; // resume drains the pause buffer to subscribers.

    let post = rec
        .snapshot()
        .iter()
        .filter(|e| matches!(e, common::RecordedEvent::Data(_)))
        .count();
    assert!(
        post > mid,
        "resume should replay buffered tier-3; events {:?}",
        rec.snapshot()
    );
}

// ---------------------------------------------------------------------------
// Per-node insertion order within a tier — preserved across the two-pass flush
// ---------------------------------------------------------------------------

#[test]
fn flush_preserves_per_node_message_order_within_tier() {
    // Coalesced multi-emit on a single source should land in emit order
    // even after the two-pass flush filters tier 1 then tier 3.
    let runtime = TestRuntime::new();
    let s = runtime.state(Some(TestValue::Int(0)));
    let rec = runtime.subscribe_recorder(s.id);
    let pre_data: Vec<TestValue> = rec.data_values();

    let h1 = runtime.binding.intern(TestValue::Int(1));
    let h2 = runtime.binding.intern(TestValue::Int(2));
    let h3 = runtime.binding.intern(TestValue::Int(3));
    runtime.core.batch(|| {
        runtime.core.emit(s.id, h1);
        runtime.core.emit(s.id, h2);
        runtime.core.emit(s.id, h3);
    });

    let post_data: Vec<TestValue> = rec.data_values();
    let new_data: Vec<TestValue> = post_data[pre_data.len()..].to_vec();
    assert_eq!(
        new_data,
        vec![TestValue::Int(1), TestValue::Int(2), TestValue::Int(3),],
        "coalesced emits should deliver in emit order"
    );
}
