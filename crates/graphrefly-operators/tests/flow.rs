//! Integration tests for `flow` operators (Slice C-3, D024).
//!
//! Each test references the canonical-spec rule it covers in its name
//! or comments. Test naming convention: `<operator>_<scenario>`.

mod common;

use graphrefly_core::Core;
use graphrefly_operators::flow::{
    element_at, find, first, last, last_with_default, skip, take, take_while,
};

use common::{OpRuntime, RecordedEvent, TestValue};

// ---------------------------------------------------------------------
// take(count) — emits first `count` DATA then self-completes
// ---------------------------------------------------------------------

#[test]
fn take_emits_first_n_then_self_completes() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let taken = take(&rt.core, source, 2).into_node();

    let rec = rt.subscribe_recorder(taken);
    rt.emit_int(source, 10);
    rt.emit_int(source, 20);
    rt.emit_int(source, 30); // post-complete: must not surface

    assert_eq!(
        rec.data_values(),
        vec![TestValue::Int(10), TestValue::Int(20)]
    );
    assert!(
        rec.events().contains(&RecordedEvent::Complete),
        "expected Complete in {:?}",
        rec.events()
    );
}

#[test]
fn take_zero_self_completes_on_first_fire_with_no_data() {
    // D027: take(0) is allowed; first fire emits zero items then
    // immediately self-completes. Subscribers see [Start, Complete]
    // (no Data, no Dirty — R1.3.1.a one-DIRTY-per-wave applies to DATA
    // waves; pure-terminal waves don't need a preceding DIRTY).
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let taken = take(&rt.core, source, 0).into_node();

    let rec = rt.subscribe_recorder(taken);
    rt.emit_int(source, 99);

    assert!(rec.data_values().is_empty(), "events={:?}", rec.events());
    assert!(
        rec.events().contains(&RecordedEvent::Complete),
        "expected Complete in {:?}",
        rec.events()
    );
}

#[test]
fn take_propagates_upstream_complete_when_count_not_reached() {
    // Standard auto-cascade: take(5) sees only 2 DATAs then upstream
    // COMPLETE → propagates COMPLETE without a self-trigger. The
    // downstream subscriber should see [Data(1), Data(2), Complete].
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let taken = take(&rt.core, source, 5).into_node();

    let rec = rt.subscribe_recorder(taken);
    rt.emit_int(source, 1);
    rt.emit_int(source, 2);
    rt.core.complete(source);

    assert_eq!(
        rec.data_values(),
        vec![TestValue::Int(1), TestValue::Int(2)]
    );
    assert!(rec.events().contains(&RecordedEvent::Complete));
}

#[test]
fn take_resubscribable_resets_counter_on_lifecycle_reset() {
    // After self-completion, marking a node resubscribable and
    // re-subscribing resets `count_emitted` via
    // `OperatorScratch::release_handles` + fresh scratch install in
    // `reset_for_fresh_lifecycle`. The cached value from the prior
    // cycle replays once via the per-tier handshake (documented
    // Subscribe behavior, independent of operator-state reset); fresh
    // emits then proceed through a clean take(2) cycle.
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let taken = take(&rt.core, source, 2).into_node();
    rt.core.set_resubscribable(taken, true);

    // Cycle 1: emit 2 values, take completes.
    let rec1 = rt.subscribe_recorder(taken);
    rt.emit_int(source, 1);
    rt.emit_int(source, 2);
    let cycle1_data: Vec<_> = rec1
        .data_values()
        .into_iter()
        .filter(|v| matches!(v, TestValue::Int(n) if *n < 100))
        .collect();
    assert_eq!(cycle1_data, vec![TestValue::Int(1), TestValue::Int(2)]);
    assert!(rec1.events().contains(&RecordedEvent::Complete));
    drop(rec1);

    // Cycle 2: invalidate source first so the resubscribe's activation
    // re-walk doesn't consume one of the take(2) quota slots from the
    // cached value (without invalidate, source.cache=2 would re-deliver
    // through fire_op_take and count_emitted would reach 1 before any
    // fresh emits — the count would still be reset by
    // reset_for_fresh_lifecycle, but immediately re-incremented by
    // activation).
    rt.core.invalidate(source);

    let rec2 = rt.subscribe_recorder(taken);
    rt.emit_int(source, 100);
    rt.emit_int(source, 200);
    rt.emit_int(source, 300); // ignored — quota hit at 100/200
    let cycle2_fresh: Vec<_> = rec2
        .data_values()
        .into_iter()
        .filter(|v| matches!(v, TestValue::Int(n) if *n >= 100))
        .collect();
    assert_eq!(
        cycle2_fresh,
        vec![TestValue::Int(100), TestValue::Int(200)],
        "fresh cycle data wrong; events={:?}",
        rec2.events()
    );
    let complete_count = rec2
        .events()
        .iter()
        .filter(|e| matches!(e, RecordedEvent::Complete))
        .count();
    assert_eq!(
        complete_count, 1,
        "cycle 2 must self-complete after 2 fresh emits"
    );
}

// ---------------------------------------------------------------------
// skip(count) — drops first `count` DATA, emits the rest
// ---------------------------------------------------------------------

#[test]
fn skip_drops_first_n_then_emits_remaining() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let skipped = skip(&rt.core, source, 2).into_node();

    let rec = rt.subscribe_recorder(skipped);
    rt.emit_int(source, 1);
    rt.emit_int(source, 2);
    rt.emit_int(source, 3);
    rt.emit_int(source, 4);

    assert_eq!(
        rec.data_values(),
        vec![TestValue::Int(3), TestValue::Int(4)]
    );
}

#[test]
fn skip_full_window_settles_dirty_resolved_per_d018() {
    // While the skip window swallows every input of a wave, the wave
    // still produced DIRTY upstream → operator queues RESOLVED to
    // settle (D018 pattern).
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let skipped = skip(&rt.core, source, 3).into_node();

    let rec = rt.subscribe_recorder(skipped);
    rt.emit_int(source, 1);

    let events = rec.events();
    assert!(
        rec.data_values().is_empty(),
        "skip window swallowed; no Data expected. events={events:?}"
    );
    assert!(
        events.contains(&RecordedEvent::Resolved),
        "expected Resolved settle for full-skip wave: {events:?}"
    );
}

// ---------------------------------------------------------------------
// take_while(predicate) — predicate-gated truncation
// ---------------------------------------------------------------------

#[test]
fn take_while_emits_until_first_false_then_self_completes() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let binding = rt.binding.clone();
    let taken = take_while(&rt.core, &rt.op_binding, source, move |h| {
        binding.deref(h).int() < 10
    })
    .into_node();

    let rec = rt.subscribe_recorder(taken);
    rt.emit_int(source, 3);
    rt.emit_int(source, 7);
    rt.emit_int(source, 12); // first false → complete here, no Data
    rt.emit_int(source, 1); // post-complete: ignored

    assert_eq!(
        rec.data_values(),
        vec![TestValue::Int(3), TestValue::Int(7)],
        "events={:?}",
        rec.events()
    );
    assert!(
        rec.events().contains(&RecordedEvent::Complete),
        "expected Complete, events={:?}",
        rec.events()
    );
}

// ---------------------------------------------------------------------
// last(source) / last_with_default(source, default)
// ---------------------------------------------------------------------

#[test]
fn last_emits_buffered_latest_on_upstream_complete() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let n = last(&rt.core, source).into_node();

    let rec = rt.subscribe_recorder(n);
    rt.emit_int(source, 1);
    rt.emit_int(source, 2);
    rt.emit_int(source, 3);

    // No emit yet — buffering silently.
    assert!(
        rec.data_values().is_empty(),
        "last should not emit before upstream Complete: {:?}",
        rec.events()
    );

    rt.core.complete(source);

    assert_eq!(rec.data_values(), vec![TestValue::Int(3)]);
    assert!(rec.events().contains(&RecordedEvent::Complete));
}

#[test]
fn last_no_default_on_empty_stream_emits_only_complete() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let n = last(&rt.core, source).into_node();

    let rec = rt.subscribe_recorder(n);
    rt.core.complete(source);

    assert!(
        rec.data_values().is_empty(),
        "no default + no DATA → only Complete; events={:?}",
        rec.events()
    );
    assert!(rec.events().contains(&RecordedEvent::Complete));
}

#[test]
fn last_with_default_on_empty_stream_emits_default() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let default = rt.intern_int(42);
    let n = last_with_default(&rt.core, source, default).into_node();

    let rec = rt.subscribe_recorder(n);
    rt.core.complete(source);

    assert_eq!(rec.data_values(), vec![TestValue::Int(42)]);
    assert!(rec.events().contains(&RecordedEvent::Complete));
}

#[test]
fn last_with_default_prefers_latest_over_default() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let default = rt.intern_int(42);
    let n = last_with_default(&rt.core, source, default).into_node();

    let rec = rt.subscribe_recorder(n);
    rt.emit_int(source, 7);
    rt.core.complete(source);

    assert_eq!(rec.data_values(), vec![TestValue::Int(7)]);
}

#[test]
fn last_propagates_upstream_error() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let n = last(&rt.core, source).into_node();

    let rec = rt.subscribe_recorder(n);
    rt.emit_int(source, 1);
    let err_h = rt.binding.intern(TestValue::Str("boom".into()));
    rt.core.error(source, err_h);

    let saw_error = rec
        .events()
        .iter()
        .any(|e| matches!(e, RecordedEvent::Error(TestValue::Str(s)) if s == "boom"));
    assert!(
        saw_error,
        "expected Error('boom'); events={:?}",
        rec.events()
    );
    // No Data — upstream Error short-circuits the buffered emit.
    assert!(
        rec.data_values().is_empty(),
        "last should not emit Data on Error: {:?}",
        rec.events()
    );
}

// ---------------------------------------------------------------------
// Sugar: first / find / element_at
// ---------------------------------------------------------------------

#[test]
fn first_alias_for_take_one() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let n = first(&rt.core, source).into_node();

    let rec = rt.subscribe_recorder(n);
    rt.emit_int(source, 7);
    rt.emit_int(source, 8); // ignored — already complete

    assert_eq!(rec.data_values(), vec![TestValue::Int(7)]);
    assert!(rec.events().contains(&RecordedEvent::Complete));
}

#[test]
fn find_emits_first_matching_then_completes() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let binding = rt.binding.clone();
    let n = find(&rt.core, &rt.op_binding, source, move |h| {
        binding.deref(h).int() > 5
    })
    .into_node();

    let rec = rt.subscribe_recorder(n);
    rt.emit_int(source, 1);
    rt.emit_int(source, 3);
    rt.emit_int(source, 8); // first > 5 → emit + complete
    rt.emit_int(source, 9); // ignored

    assert_eq!(rec.data_values(), vec![TestValue::Int(8)]);
    assert!(rec.events().contains(&RecordedEvent::Complete));
}

#[test]
fn element_at_emits_indexed_value_then_completes() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let n = element_at(&rt.core, source, 2).into_node();

    let rec = rt.subscribe_recorder(n);
    rt.emit_int(source, 10);
    rt.emit_int(source, 20);
    rt.emit_int(source, 30); // index 2 — emit + complete
    rt.emit_int(source, 40); // ignored

    assert_eq!(rec.data_values(), vec![TestValue::Int(30)]);
    assert!(rec.events().contains(&RecordedEvent::Complete));
}

// ---------------------------------------------------------------------
// Refcount / lifecycle discipline
// ---------------------------------------------------------------------

#[test]
fn last_with_default_releases_default_on_core_drop() {
    // The default handle is retained for LastState's lifetime; on
    // Core drop, OperatorScratch::release_handles releases it.
    let binding;
    let default_handle;
    {
        let rt = OpRuntime::new();
        binding = rt.binding.clone();
        let source = rt.state_int(None);
        default_handle = rt.intern_int(99);
        // Caller's intern share: 1.
        assert_eq!(binding.refcount_of(default_handle), 1);
        let _n = last_with_default(&rt.core, source, default_handle).into_node();
        // Core retained inside register_operator: now 2.
        assert_eq!(
            binding.refcount_of(default_handle),
            2,
            "register_operator must retain the default handle"
        );
    }
    // Core dropped → release_handles called → refcount drops to 1
    // (caller's original intern share remains).
    assert_eq!(
        binding.refcount_of(default_handle),
        1,
        "Core drop must release the default handle's share"
    );
}

#[test]
fn last_releases_buffered_latest_on_lifecycle_reset() {
    // Buffered `latest` is retained for LastState's lifetime; on
    // resubscribable terminal cycle reset, release_handles releases it
    // and a fresh LastState (latest=NO_HANDLE) is installed.
    //
    // Slice C-3 /qa T1 — verifies the refcount path explicitly via
    // `refcount_of`, not just "no panic on re-subscribe". The
    // diagnostic intern in cycle 1 holds an extra share of the
    // buffered handle; we observe Core's slot share via the refcount
    // delta between cycle 1 (buffered) and post-reset (released).
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let n = last(&rt.core, source).into_node();
    rt.core.set_resubscribable(n, true);

    // Diagnostic intern: hold an extra share of `5` so its refcount
    // doesn't drop to zero when LastState releases its share. This
    // lets `refcount_of` survive across the lifecycle reset for
    // observation.
    let observed_handle = rt.intern_int(5);
    // After intern_int: refcount = 1 (our diagnostic share).
    assert_eq!(rt.binding.refcount_of(observed_handle), 1);

    let rec1 = rt.subscribe_recorder(n);
    rt.emit_int(source, 5);
    // After emit + wave drain: source's cache holds one share, Last's
    // dep_records[0].prev_data holds one (transferred from data_batch
    // by clear_wave_state's wave-end rotation), LastState.latest
    // holds one (from fire_op_last's retain), and our diagnostic
    // share remains. Total: 4.
    assert_eq!(
        rt.binding.refcount_of(observed_handle),
        4,
        "after emit + wave drain: 1 (diag) + 1 (source cache) + 1 (prev_data) + 1 (LastState.latest)"
    );

    rt.core.complete(source);
    // Terminal-aware fire_op_last emits Data(5) + Complete: it
    // retains the buffered handle once more, commit_emission_verbatim
    // consumes that retain into Last.cache. Refcount becomes 5.
    assert_eq!(rec1.data_values(), vec![TestValue::Int(5)]);
    drop(rec1);
    // After drop(rec1): rec1's Subscription drops, its sink is
    // removed from n's subscribers. Refcount unchanged (no shares
    // to release on subscriber removal for non-producer compute nodes).

    // Re-subscribing triggers reset_for_fresh_lifecycle AND re-activation
    // (because subscribers count went 1→0→1, _rec2 is a "first
    // subscriber" again):
    //   - Phase 2 makes a fresh LastState (latest=NO_HANDLE,
    //     default=NO_HANDLE) — no retain on `5`.
    //   - Phase 3 releases old LastState.latest's share (5 → 4).
    //   - Phase 5 releases the old prev_data share (4 → 3).
    //   - Then activation re-walks source dep, deliver_data_to_consumer
    //     retains source.cache for the new dep_records[0].data_batch
    //     (3 → 4). fire_op_last runs, updates LastState.latest with
    //     a fresh retain (4 → 5). Wave-end rotates data_batch into
    //     prev_data (no net retain change). `Last.cache` is NOT reset
    //     and stays at `5`.
    // Final: 1 (diag) + 1 (source.cache) + 1 (Last.cache) +
    //        1 (re-activated prev_data) + 1 (re-activated LastState.latest)
    //        = 5.
    let _rec2 = rt.subscribe_recorder(n);
    assert_eq!(
        rt.binding.refcount_of(observed_handle),
        5,
        "after reset + re-activation: old shares released by reset, \
         then re-activation re-retains via source.cache + new LastState.latest"
    );
}

// ---------------------------------------------------------------------
// Integration: pipeline composition
// ---------------------------------------------------------------------

#[test]
fn take_after_skip_produces_window() {
    // skip(2) → take(3) → emits inputs 3..=5
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let skipped = skip(&rt.core, source, 2).into_node();
    let windowed = take(&rt.core, skipped, 3).into_node();

    let rec = rt.subscribe_recorder(windowed);
    for v in 1..=10 {
        rt.emit_int(source, v);
    }

    assert_eq!(
        rec.data_values(),
        vec![TestValue::Int(3), TestValue::Int(4), TestValue::Int(5)]
    );
    assert!(rec.events().contains(&RecordedEvent::Complete));
}

// ---------------------------------------------------------------------
// Send + Sync / OperatorScratch trait-object behavior
// ---------------------------------------------------------------------

#[test]
fn flow_registration_is_send_and_sync() {
    fn _check<T: Send + Sync>() {}
    _check::<graphrefly_operators::flow::FlowRegistration>();
    _check::<Core>();
}
