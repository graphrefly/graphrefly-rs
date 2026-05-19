//! Slice X4 (2026-05-08) — D2 fix: per-emit sink snapshot via
//! revision-tracked `PendingBatch`es.
//!
//! Pre-X4 the per-node `pending_notify` entry froze its sink list on the
//! FIRST `queue_notify` push for a wave at the node and reused that
//! snapshot for all subsequent emits to the same node. A subscriber
//! installed mid-wave between two emits to the same node was invisible
//! to the second emit's flush slice — only the handshake's cache replay
//! reached them, dropping any subsequent same-wave Data.
//!
//! Post-X4 each `queue_notify` consults `NodeRecord::subscribers_revision`;
//! when the revision advances mid-wave (subscribe / `Subscription::Drop` /
//! handshake-panic eviction), the next push opens a fresh `PendingBatch`
//! with an updated sink snapshot. Pre-subscribe batches retain their
//! original snapshot so earlier emits don't double-deliver via flush AND
//! handshake.
//!
//! These four scenarios pin the fix:
//!
//! - Multi-emit and late-subscribe (the canonical D2 case).
//! - Single-emit and late-subscribe (preserves the original snapshot
//!   fix's target — no duplicate-Data delivery).
//! - Multi-emit and mid-wave unsubscribe (the unsub'd sink doesn't see
//!   the post-unsub emit).
//! - Multi-emit and no sub change (common case — single batch, behavior
//!   unchanged from pre-X4).

mod common;

use std::cell::{Cell, RefCell};

use common::{RecordedEvent, Recorder, TestRuntime, TestValue};

/// Test 1 — Multi-emit + late-subscribe: the late sub receives the
/// post-subscribe emit's Data via the wave's flush slice, in addition to
/// the handshake's cache replay of the pre-subscribe value. R1.3.5.a
/// "consistent post-handshake view" — the late sub observes any tier-3
/// emission that happens AFTER they joined.
#[test]
fn d2_late_subscribe_between_emits_receives_subsequent_data() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let rec_old = rt.subscribe_recorder(s.id);
    let rec_late_holder: RefCell<Option<Recorder>> = RefCell::new(None);

    let h1 = rt.binding.intern(TestValue::Int(1));
    let h2 = rt.binding.intern(TestValue::Int(2));

    rt.core.batch(|| {
        rt.core.emit(s.id, h1);
        // Batch 1 has just opened with sinks = [rec_old] and messages
        // = [Dirty, Data(h1)]. Subscribers revision = R0 at this point.
        let rec_late = rt.subscribe_recorder(s.id);
        // Handshake fires lock-released and delivers [Start, Data(h1)]
        // to rec_late. subscribers_revision bumps to R1.
        *rec_late_holder.borrow_mut() = Some(rec_late);
        rt.core.emit(s.id, h2);
        // Batch 2 opens with sinks = [rec_old, rec_late], messages
        // = [Data(h2)] (Dirty already queued in batch 1; verbatim path
        // skips equals substitution per Slice G).
    });

    let rec_late = rec_late_holder.into_inner().expect("rec_late was set");

    // Post-X4 expected: rec_late sees Data(h2) via batch 2 flush. The
    // Data(h1) it sees is from the handshake replay (cache snapshot at
    // subscribe time), not a duplicate from the wave's flush.
    let late_data = rec_late.data_values();
    assert_eq!(
        late_data,
        vec![TestValue::Int(1), TestValue::Int(2)],
        "late subscriber should observe handshake h1 + post-subscribe h2"
    );

    // rec_old observes the full wave: handshake's [Start, Data(0)] +
    // wave's [Dirty, Data(h1), Data(h2)]. Dirty fires once per wave per
    // node (R1.3.1.a conditional).
    let old_snapshot = rec_old.snapshot();
    assert_eq!(
        old_snapshot,
        vec![
            RecordedEvent::Start,
            RecordedEvent::Data(TestValue::Int(0)),
            RecordedEvent::Dirty,
            RecordedEvent::Data(TestValue::Int(1)),
            RecordedEvent::Data(TestValue::Int(2)),
        ],
        "pre-existing subscriber should see full wave sequence"
    );
}

/// Test 2 — Single-emit + late-subscribe: the late sub sees Data(h1) ONCE
/// (from handshake), not twice (handshake AND flush). This is the
/// original snapshot fix's target — pre-fix the late sub would have been
/// added to the wave's flush list and double-received the emit value.
/// The X4 revision-tracked redesign preserves this property — batch 1's
/// snapshot freezes at the pre-subscribe revision, so the late sub is
/// NOT in batch 1's flush list.
#[test]
fn d2_late_subscribe_after_single_emit_no_duplicate_data() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let rec_old = rt.subscribe_recorder(s.id);
    let rec_late_holder: RefCell<Option<Recorder>> = RefCell::new(None);

    let h1 = rt.binding.intern(TestValue::Int(1));

    rt.core.batch(|| {
        rt.core.emit(s.id, h1);
        let rec_late = rt.subscribe_recorder(s.id);
        *rec_late_holder.borrow_mut() = Some(rec_late);
        // No second emit — batch 1 stays open as the only batch with
        // sinks = [rec_old] (frozen at first push, before subscribe).
    });

    let rec_late = rec_late_holder.into_inner().expect("rec_late was set");

    // rec_late should see Data(h1) exactly once, from the handshake
    // replay. If batch 1's snapshot included the late sub (the
    // duplicate-Data hazard the snapshot-on-first-touch fix targeted),
    // this would be two Data(1) events.
    let late_data = rec_late.data_values();
    assert_eq!(
        late_data,
        vec![TestValue::Int(1)],
        "late subscriber sees handshake h1 once (no duplicate from flush)"
    );

    let old_snapshot = rec_old.snapshot();
    assert_eq!(
        old_snapshot,
        vec![
            RecordedEvent::Start,
            RecordedEvent::Data(TestValue::Int(0)),
            RecordedEvent::Dirty,
            RecordedEvent::Data(TestValue::Int(1)),
        ],
        "pre-existing subscriber sees Dirty + Data(h1) from the single emit"
    );
}

/// Test 3 — Multi-emit + mid-wave unsubscribe: the unsubscribed sink
/// receives the pre-unsub emits (via batch 1's frozen snapshot), but
/// does NOT receive the post-unsub emit (batch 2 opens with the reduced
/// subscriber set after the revision bump from `Subscription::Drop`).
#[test]
fn d2_unsubscribe_between_emits_skips_post_unsub_data() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let rec_doomed = rt.subscribe_recorder(s.id);
    let rec_old = rt.subscribe_recorder(s.id);

    let h1 = rt.binding.intern(TestValue::Int(1));
    let h2 = rt.binding.intern(TestValue::Int(2));

    rt.core.batch(|| {
        rt.core.emit(s.id, h1);
        // Batch 1: sinks = [rec_doomed, rec_old], messages = [Dirty, Data(h1)].
        rt.unsubscribe(rec_doomed.node_id(), rec_doomed.sub_id());
        // Subscription dropped → removed from subscribers map; revision bumps.
        // The Recorder itself stays alive so we can inspect events afterward.
        rt.core.emit(s.id, h2);
        // Batch 2 opens with sinks = [rec_old] (the only remaining
        // subscriber), messages = [Data(h2)].
    });

    // rec_doomed received the pre-unsub events from batch 1's frozen
    // snapshot (their sink Arc was captured before the unsub). It does
    // NOT receive Data(h2), which lands in batch 2's slice — outside
    // their snapshot.
    let doomed_data = rec_doomed.data_values();
    assert_eq!(
        doomed_data,
        vec![TestValue::Int(0), TestValue::Int(1)],
        "unsubscribed sink sees handshake + pre-unsub Data(h1) only"
    );

    // rec_old saw both emits.
    let old_data = rec_old.data_values();
    assert_eq!(
        old_data,
        vec![TestValue::Int(0), TestValue::Int(1), TestValue::Int(2)],
        "remaining subscriber sees full wave (handshake + Data(h1) + Data(h2))"
    );
}

/// Test 4 — Multi-emit + no sub change: the common case stays in a
/// single `PendingBatch` (no extra allocation; same flush behavior as
/// pre-X4). This pins the no-regression contract — the X4 refactor is
/// invisible to users whose subscriber set is stable across the wave.
#[test]
fn d2_multi_emit_no_sub_change_single_batch_behavior() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let rec = rt.subscribe_recorder(s.id);

    let h1 = rt.binding.intern(TestValue::Int(1));
    let h2 = rt.binding.intern(TestValue::Int(2));
    let h3 = rt.binding.intern(TestValue::Int(3));

    // Capture batch count from INSIDE the wave (after emits, before
    // flush) — Slice X4 /qa P4: pin the "common case = single batch,
    // no SmallVec spill" perf invariant. Without this assertion, a
    // future regression that always opens a fresh batch per emit
    // would still pass the message-sequence assertion below.
    let mid_wave_batch_count: Cell<Option<usize>> = Cell::new(None);
    rt.core.batch(|| {
        rt.core.emit(s.id, h1);
        rt.core.emit(s.id, h2);
        rt.core.emit(s.id, h3);
        mid_wave_batch_count.set(rt.core.pending_batch_count(s.id));
    });
    assert_eq!(
        mid_wave_batch_count.get(),
        Some(1),
        "stable-subscriber multi-emit must collapse into a single PendingBatch \
         (X4 perf invariant: subscribers_revision unchanged → append, no fresh batch)"
    );

    // All three emits land in batch 1 (subscribers_revision didn't change).
    // Dirty fires once (R1.3.1.a conditional), then three Data tiers in
    // arrival order. With phase-tier flush, [Dirty] fires in phase 1 and
    // [Data(h1), Data(h2), Data(h3)] together in phase 2.
    let snapshot = rec.snapshot();
    assert_eq!(
        snapshot,
        vec![
            RecordedEvent::Start,
            RecordedEvent::Data(TestValue::Int(0)),
            RecordedEvent::Dirty,
            RecordedEvent::Data(TestValue::Int(1)),
            RecordedEvent::Data(TestValue::Int(2)),
            RecordedEvent::Data(TestValue::Int(3)),
        ],
        "stable-subscriber multi-emit wave: full sequence in single phase-2 slice"
    );
}
