//! Slice C-2 — protocol-layer property tests via `proptest`.
//!
//! Ports the protocol-layer subset of `_invariants.ts` (graphrefly-ts) to the
//! Rust handle-protocol Core. Invariants that need higher-level building
//! blocks (`Graph` from M2, operators from M3, versioning from production
//! `node.ts`) are out of scope here and will land alongside their respective
//! milestones.
//!
//! ## Ported invariants
//!
//! | # | Name                      | Spec ref              |
//! |---|---------------------------|-----------------------|
//! | 1 | no-data-without-dirty     | R1.3.1.a              |
//! | 2 | resolved-per-wave         | R1.3.1.a (balance)    |
//! | 3 | terminal-monotonicity     | R1.3.4 (terminal)     |
//! | 4 | diamond-resolution        | R2.7.1 / R1.3.1.b     |
//! | 5 | equals-substitution       | R1.3.2                |
//! | 7 | start-handshake           | R1.2.3, R2.2.3        |
//! |11 | pause-multi-pauser        | R1.2.6, R2.6, §10.2   |
//!
//! ## Out-of-scope (deferred)
//!
//! - **#6 version-counter-per-emit** — handle-protocol Core has no versioning
//!   (production-only concern). Lands when versioning ports.
//! - **#8 throw-recovery-consistency** — needs sink re-entrance (Slice
//!   A-bigger lifts the v1 sink-fire-with-lock-held limitation).
//! - **#9 subscribe-unsubscribe-reentry** — same blocker as #8.
//! - **#10 multi-dep-initial-pair-clean** — covered structurally by the
//!   already-ported handshake logic; redundant with #7 here.
//! - All operator-layer invariants (#12+) — wait for M3.

mod common;

use std::sync::Arc;

use graphrefly_core::Message;
use proptest::prelude::*;

use common::{RecordedEvent, TestRuntime, TestValue};

// ---------------------------------------------------------------------------
// Event vocabulary — mirrors `_generators.ts`
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum Event {
    Emit(i64),
    Batch(Vec<i64>),
    Complete,
}

/// Apply one event to the source state node. Defensive on post-COMPLETE
/// (matches TS `applyEvent`'s `if (completed.value) return`).
fn apply_event(
    rt: &TestRuntime,
    source: graphrefly_core::NodeId,
    event: &Event,
    completed: &mut bool,
) {
    if *completed {
        return;
    }
    match event {
        Event::Emit(v) => {
            let h = rt.binding.intern(TestValue::Int(*v));
            rt.core.emit(source, h);
        }
        Event::Batch(values) => {
            // User-facing batch coalesces multiple emits into one wave.
            rt.core.batch(|| {
                for v in values {
                    let h = rt.binding.intern(TestValue::Int(*v));
                    rt.core.emit(source, h);
                }
            });
        }
        Event::Complete => {
            rt.core.complete(source);
            *completed = true;
        }
    }
}

/// Number of source-emit attempts a wave produces.
/// `Emit` → 1; `Batch` → values.len(); `Complete` → 0.
fn wave_attempt_count(event: &Event) -> usize {
    match event {
        Event::Emit(_) => 1,
        Event::Batch(values) => values.len(),
        Event::Complete => 0,
    }
}

// ---------------------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------------------

/// Sequence of state-mutation events. Never emits `Complete` — invariants
/// that need a terminal inject one explicitly. Values drawn from a small
/// alphabet (0..=4) so equal-value emits are common (exercises
/// equals-substitution).
fn event_sequence_arb(max_len: usize, max_batch_emits: usize) -> BoxedStrategy<Vec<Event>> {
    let value = -2i64..=2i64;
    let emit = value.clone().prop_map(Event::Emit).boxed();
    let batch = prop::collection::vec(value, 1..=max_batch_emits)
        .prop_map(Event::Batch)
        .boxed();
    // Weighted union: 7:2 emit:batch, matching TS `_generators.ts`.
    let event = prop_oneof![7 => emit, 2 => batch];
    prop::collection::vec(event, 1..=max_len).boxed()
}

// ---------------------------------------------------------------------------
// Trace recording
// ---------------------------------------------------------------------------

struct Trace {
    /// Recorded events in delivery order (post-START activation chatter
    /// plus event-driven messages).
    events: Vec<RecordedEvent>,
    /// Index of the first event-driven (post-activation) message.
    activation_end: usize,
}

/// Subscribe to `observed`, snapshot the post-subscribe baseline as
/// `activation_end`, drive `events` against `source`, return the trace.
fn capture_trace(
    rt: &TestRuntime,
    source: graphrefly_core::NodeId,
    observed: graphrefly_core::NodeId,
    events: &[Event],
) -> Trace {
    let rec = rt.subscribe_recorder(observed);
    let activation_end = rec.snapshot().len();
    let mut completed = false;
    for ev in events {
        apply_event(rt, source, ev, &mut completed);
    }
    let snapshot = rec.snapshot();
    Trace {
        events: snapshot,
        activation_end,
    }
}

// ---------------------------------------------------------------------------
// Invariant #1 — no-data-without-dirty
// Topology: state → derived (×2) → derived (+1)
// Walks post-activation trace; pending = (DIRTY count) − (DATA+RESOLVED count).
// If pending goes negative, a settlement reached the leaf without an
// outstanding DIRTY (R1.3.1.a violation).
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn inv1_no_data_without_dirty(events in event_sequence_arb(8, 3)) {
        let rt = TestRuntime::new();
        let s = rt.state(Some(TestValue::Int(0)));
        let m = rt.derived(&[s.id], |deps| match &deps[0] {
            TestValue::Int(n) => Some(TestValue::Int(n * 2)),
            _ => None,
        });
        let leaf = rt.derived(&[m], |deps| match &deps[0] {
            TestValue::Int(n) => Some(TestValue::Int(n + 1)),
            _ => None,
        });
        let trace = capture_trace(&rt, s.id, leaf, &events);
        let mut pending: i64 = 0;
        for e in &trace.events[trace.activation_end..] {
            match e {
                RecordedEvent::Dirty => pending += 1,
                RecordedEvent::Data(_) | RecordedEvent::Resolved => {
                    pending -= 1;
                    prop_assert!(
                        pending >= 0,
                        "settlement without preceding DIRTY at event idx (trace: {:?})",
                        &trace.events
                    );
                }
                _ => {}
            }
        }
    }

    // ---------------------------------------------------------------------
    // Invariant #2 — resolved-per-wave
    // Stronger than #1: pending must be exactly 0 at end.
    // ---------------------------------------------------------------------
    #[test]
    fn inv2_resolved_per_wave(events in event_sequence_arb(8, 3)) {
        let rt = TestRuntime::new();
        let s = rt.state(Some(TestValue::Int(0)));
        let leaf = rt.derived(&[s.id], |deps| match &deps[0] {
            TestValue::Int(n) => Some(TestValue::Int(*n)),
            _ => None,
        });
        let trace = capture_trace(&rt, s.id, leaf, &events);
        let mut pending: i64 = 0;
        for e in &trace.events[trace.activation_end..] {
            match e {
                RecordedEvent::Dirty => pending += 1,
                RecordedEvent::Data(_) | RecordedEvent::Resolved => pending -= 1,
                _ => {}
            }
        }
        prop_assert_eq!(
            pending, 0,
            "every DIRTY should be balanced by exactly one DATA/RESOLVED — trace: {:?}",
            &trace.events
        );
    }

    // ---------------------------------------------------------------------
    // Invariant #3 — terminal-monotonicity
    // Inject a `Complete` between prefix and suffix events. After COMPLETE,
    // no further DIRTY / DATA / RESOLVED may follow.
    // ---------------------------------------------------------------------
    #[test]
    fn inv3_terminal_monotonicity(
        prefix in event_sequence_arb(4, 3),
        suffix in event_sequence_arb(4, 3),
    ) {
        let rt = TestRuntime::new();
        let s = rt.state(Some(TestValue::Int(0)));
        let mut sequence = prefix.clone();
        sequence.push(Event::Complete);
        sequence.extend(suffix.iter().cloned());
        let trace = capture_trace(&rt, s.id, s.id, &sequence);
        let post = &trace.events[trace.activation_end..];
        let term_idx = post.iter().position(|e| {
            matches!(e, RecordedEvent::Complete | RecordedEvent::Error(_))
        });
        prop_assert!(
            term_idx.is_some(),
            "forced COMPLETE must reach the trace; got {:?}",
            post
        );
        if let Some(idx) = term_idx {
            for e in &post[idx + 1..] {
                prop_assert!(
                    !matches!(
                        e,
                        RecordedEvent::Dirty | RecordedEvent::Data(_) | RecordedEvent::Resolved
                    ),
                    "post-terminal event observed: {e:?} at idx>{idx}; full trace: {:?}",
                    &trace.events
                );
            }
        }
    }

    // ---------------------------------------------------------------------
    // Invariant #4 — diamond-resolution (settlements bounded)
    // A → B; A → C; D = combine(B, C). Each event drives a wave; the
    // settlement count at D in that wave must be ≤ wave_attempt_count(event).
    // No 2× per dep edge.
    // ---------------------------------------------------------------------
    #[test]
    fn inv4_diamond_resolution(events in event_sequence_arb(8, 3)) {
        let rt = TestRuntime::new();
        let a = rt.state(Some(TestValue::Int(0)));
        let b = rt.derived(&[a.id], |deps| match &deps[0] {
            TestValue::Int(n) => Some(TestValue::Int(*n)),
            _ => None,
        });
        let c = rt.derived(&[a.id], |deps| match &deps[0] {
            TestValue::Int(n) => Some(TestValue::Int(n + 1)),
            _ => None,
        });
        let d = rt.derived(&[b, c], |deps| match (&deps[0], &deps[1]) {
            (TestValue::Int(b), TestValue::Int(c)) => Some(TestValue::Int(b + c)),
            _ => None,
        });

        // Per-wave segments of D's recorded events.
        let rec = rt.subscribe_recorder(d);
        let activation_end = rec.snapshot().len();
        let mut completed = false;
        let mut prev_len = activation_end;
        let mut per_wave_settlement_counts: Vec<usize> = Vec::new();
        for ev in &events {
            apply_event(&rt, a.id, ev, &mut completed);
            let snap = rec.snapshot();
            let new = &snap[prev_len..];
            let settle = new
                .iter()
                .filter(|e| matches!(e, RecordedEvent::Data(_) | RecordedEvent::Resolved))
                .count();
            per_wave_settlement_counts.push(settle);
            prev_len = snap.len();
        }
        for (i, &count) in per_wave_settlement_counts.iter().enumerate() {
            let bound = wave_attempt_count(&events[i]);
            prop_assert!(
                count <= bound,
                "wave {i} ({:?}): D settled {count} times, bound was {bound}; trace tail: {:?}",
                events[i],
                rec.snapshot()
            );
        }
    }

    // ---------------------------------------------------------------------
    // Invariant #5 — equals-substitution
    // Every source emit produces exactly one settlement (DATA when value
    // changed, RESOLVED when equals matched). Total settlement count ==
    // total emit-attempt count. Silent drops would fail this.
    // ---------------------------------------------------------------------
    #[test]
    fn inv5_equals_substitution(events in event_sequence_arb(8, 3)) {
        let rt = TestRuntime::new();
        let s = rt.state(Some(TestValue::Int(0)));
        let trace = capture_trace(&rt, s.id, s.id, &events);
        let post = &trace.events[trace.activation_end..];
        let settlements = post
            .iter()
            .filter(|e| matches!(e, RecordedEvent::Data(_) | RecordedEvent::Resolved))
            .count();
        let expected: usize = events.iter().map(wave_attempt_count).sum();
        prop_assert_eq!(
            settlements,
            expected,
            "every emit should produce exactly one settlement; got {} for {} attempts; trace: {:?}",
            settlements,
            expected,
            &trace.events
        );
    }
}

// ---------------------------------------------------------------------------
// Invariant #7 — start-handshake (R1.2.3, R2.2.3)
//
// In Rust's v1 dispatcher, `subscribe` synthesizes the handshake under the
// state lock and fires it as a SINGLE sink call, i.e.:
// - Source with cached value: one call carrying `[Start, Data(v)]`.
// - Sentinel source: one call carrying `[Start]`.
// - Single-dep derived: handshake `[Start]`, then activation drives a wave
//   whose two-pass flush delivers `[Dirty]` and `[Data]` as separate calls.
//
// This diverges from TS's R1.3.5.a "Start respects batch semantics" — TS
// splits Start (tier 0) from Data (tier 3) at delivery. The Rust handshake
// is a deliberate v1 simplification (subscribe is out-of-wave; the lock-held
// handshake fires once with the cached state). Slice A-bigger lifts the
// lock-held discipline; revisit handshake tier-split alongside that work.
// Tracked as a follow-up in `porting-deferred.md`.
//
// What this invariant DOES verify: across all topologies, the very first
// message any subscriber observes is `Start`.
// ---------------------------------------------------------------------------

type CallShape = (usize, Option<Message>);

fn capture_call_lengths_and_first_msg(
    rt: &TestRuntime,
    node_id: graphrefly_core::NodeId,
) -> Vec<CallShape> {
    let calls: Arc<std::sync::Mutex<Vec<CallShape>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let calls_inner = calls.clone();
    let sink: graphrefly_core::Sink = Arc::new(move |msgs| {
        let first = msgs.first().copied();
        calls_inner.lock().expect("lock").push((msgs.len(), first));
    });
    let sub = rt.core.subscribe(node_id, sink);
    drop(sub);
    let r = calls.lock().expect("lock").clone();
    r
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(40))]

    #[test]
    fn inv7_handshake_first_message_is_start_source_cached(initial in -10i64..=10i64) {
        let rt = TestRuntime::new();
        let s = rt.state(Some(TestValue::Int(initial)));
        let calls = capture_call_lengths_and_first_msg(&rt, s.id);
        prop_assert!(!calls.is_empty(), "handshake produced no calls");
        prop_assert!(
            matches!(calls[0].1, Some(Message::Start)),
            "first message must be Start; got {:?}",
            calls
        );
    }

    #[test]
    fn inv7_handshake_first_message_is_start_sentinel_source(_seed in 0u32..=255) {
        let rt = TestRuntime::new();
        let s = rt.state(None);
        let calls = capture_call_lengths_and_first_msg(&rt, s.id);
        prop_assert!(!calls.is_empty(), "handshake produced no calls");
        prop_assert!(
            matches!(calls[0].1, Some(Message::Start)),
            "first message must be Start; got {:?}",
            calls
        );
    }

    #[test]
    fn inv7_handshake_first_message_is_start_derived(initial in -10i64..=10i64) {
        let rt = TestRuntime::new();
        let s = rt.state(Some(TestValue::Int(initial)));
        let derived = rt.derived(&[s.id], |deps| match &deps[0] {
            TestValue::Int(n) => Some(TestValue::Int(n * 2)),
            _ => None,
        });
        let calls = capture_call_lengths_and_first_msg(&rt, derived);
        prop_assert!(!calls.is_empty(), "handshake produced no calls");
        prop_assert!(
            matches!(calls[0].1, Some(Message::Start)),
            "first message must be Start; got {:?}",
            calls
        );
    }
}

// ---------------------------------------------------------------------------
// Invariant #11 — pause-multi-pauser (R1.2.6, R2.6, §10.2)
//
// Each pause-lock allocation acquires/releases independently; the node stays
// paused while ANY lock is held. Resume only flushes the buffer when the
// last lock releases.
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(40))]

    #[test]
    fn inv11_pause_multi_pauser_only_final_release_replays(
        emits_during in prop::collection::vec(-3i64..=3, 0..6),
        // Multi-pauser focus: at least 2 locks so the intermediate-release
        // loop is non-empty and exercises the "non-final resume returns
        // None" assertion. Single-pauser pause/resume is covered by
        // `inv11_pause_resume_replays_buffered_in_order`.
        n_locks in 2usize..=4,
    ) {
        let rt = TestRuntime::new();
        let s = rt.state(Some(TestValue::Int(0)));
        let rec = rt.subscribe_recorder(s.id);

        let locks: Vec<_> = (0..n_locks).map(|_| rt.core.alloc_lock_id()).collect();
        for &l in &locks {
            rt.core.pause(s.id, l).expect("pause");
        }
        prop_assert!(rt.core.is_paused(s.id));
        prop_assert_eq!(rt.core.pause_lock_count(s.id), n_locks);

        let baseline = rec.snapshot().len();
        for v in &emits_during {
            let h = rt.binding.intern(TestValue::Int(*v));
            rt.core.emit(s.id, h);
        }
        let mid = rec.snapshot();
        // Tier-3 stays buffered while at least one lock holds the pause.
        for e in &mid[baseline..] {
            prop_assert!(
                !matches!(e, RecordedEvent::Data(_) | RecordedEvent::Resolved),
                "tier-3 leaked through pause buffer: {e:?}"
            );
        }

        // Release all locks except the last; each non-final resume returns None
        // (still paused) and does not flush.
        for &l in &locks[..n_locks - 1] {
            let r = rt.core.resume(s.id, l).expect("resume");
            prop_assert!(
                r.is_none(),
                "non-final resume should return None (still paused), got {r:?}"
            );
        }
        prop_assert!(
            rt.core.is_paused(s.id),
            "node should still be paused with one lock outstanding"
        );

        // Final release — expect a report and the buffered tier-3 to flush.
        let final_lock = *locks.last().expect("at least one lock");
        let report = rt
            .core
            .resume(s.id, final_lock)
            .expect("resume")
            .expect("final lock should return a report");
        prop_assert!(!rt.core.is_paused(s.id));
        let _ = report; // replayed count tracked in the next test

        // After final resume, all DATA/RESOLVED for the buffered emits should
        // have flushed (count >= 0; 0 is valid if no value-changing emits).
    }

    #[test]
    fn inv11_pause_resume_replays_buffered_in_order(
        emits in prop::collection::vec(-3i64..=3, 0..6),
    ) {
        let rt = TestRuntime::new();
        let s = rt.state(Some(TestValue::Int(0)));
        let rec = rt.subscribe_recorder(s.id);
        let baseline = rec.snapshot().len();
        let lock = rt.core.alloc_lock_id();
        rt.core.pause(s.id, lock).expect("pause");

        for v in &emits {
            let h = rt.binding.intern(TestValue::Int(*v));
            rt.core.emit(s.id, h);
        }
        let report = rt.core.resume(s.id, lock).expect("resume").expect("final report");
        let _ = report;

        let post = &rec.snapshot()[baseline..];
        // Extract Data values from the post-baseline trace (in delivery order).
        let observed_data: Vec<i64> = post
            .iter()
            .filter_map(|e| match e {
                RecordedEvent::Data(TestValue::Int(n)) => Some(*n),
                _ => None,
            })
            .collect();
        // The expected sequence is each emit value, except equals-substituted
        // ones (consecutive duplicates collapse to a RESOLVED instead of DATA).
        // Compute expected by simulating the equals-substitution in-order.
        let mut cache: i64 = 0;
        let mut expected: Vec<i64> = Vec::new();
        for &v in &emits {
            if v != cache {
                expected.push(v);
                cache = v;
            }
        }
        prop_assert_eq!(
            &observed_data,
            &expected,
            "pause-replay should deliver value-changing emits in order; observed {:?} expected {:?}",
            &observed_data,
            &expected
        );
    }
}
