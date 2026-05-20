//! B sub-slice (2026-05-10) — F2 end-to-end Dead-path tests for
//! producer-pattern operators.
//!
//! The D126 substrate (SubscribeOutcome::Dead + per-op handlers in
//! `ops_impl.rs` / `higher_order.rs`) landed 2026-05-10. End-to-end
//! verification was deferred because reaching the immediate-Dead path
//! requires partition-coherent state at activation time: source and
//! producer partitions must both be held by the activation wave's
//! current thread, otherwise H+ STRICT routes through the Deferred
//! path instead.
//!
//! [`OpRuntime::with_all_partitions_held`] (B sub-slice helper) bridges
//! the gap — historically it wrapped `f` in a `core.batch()` scope that
//! acquired every currently-existing partition's `wave_owner`
//! `ReentrantMutex`. Under S2c/D248 single-owner `Core` the per-
//! partition `wave_owner` machinery is deleted (one owner thread, no
//! cross-thread interleaving wave to serialize), so the helper now
//! reduces to entering a `BatchGuard` scope on the one owner. The
//! producer's activation `try_subscribe(source)` runs owner-side, the
//! H+ STRICT ascending-order check passes against the owner's wave,
//! and the source's `resubscribable=false + terminal=Some(...)` state
//! surfaces as `SubscribeError::TornDown` → `SubscribeOutcome::Dead`
//! synchronously.
//!
//! Each test verifies one operator's per-op Dead handling:
//!
//! | Op           | Dead handling                                              |
//! |--------------|------------------------------------------------------------|
//! | `zip`        | self-Complete if any source is Dead (no tuple can form)    |
//! | `concat`     | Dead `first` triggers phase transition; Dead `second` flag |
//! | `race`       | mark `completed[idx]`; self-Complete if all sources Dead   |
//! | `take_until` | Dead `source` → self-Complete; Dead `notifier` → no-op     |

mod common;

use common::{OpRuntime, RecordedEvent};
use graphrefly_operators::{concat, race, take_until, zip};

// =====================================================================
// zip — Dead source forces self-Complete (no tuple stream possible)
// =====================================================================

#[test]
fn zip_self_completes_immediately_when_one_source_is_dead() {
    let rt = OpRuntime::new();
    let live = rt.state_int(Some(1));
    let dead = rt.state_int(Some(2));
    // Make `dead` non-resubscribable (default) and terminal.
    rt.core().complete(dead);

    let pack_fn = rt.register_tuple_packer();
    // Activation must happen with all partitions held so Dead fires
    // synchronously (not deferred).
    let rec = rt.with_all_partitions_held(|rt| {
        let z = zip(rt.core(), &rt.producer_binding, vec![live, dead], pack_fn).unwrap();
        rt.subscribe_recorder(z)
    });

    // The Dead path drains zip's queues and emits Complete. No DATA
    // since no tuple can form (one source is permanently empty post-
    // terminal).
    let events = rec.events();
    assert!(
        events.contains(&RecordedEvent::Complete),
        "zip with one Dead source must self-Complete synchronously \
         (F2 Dead path). events={events:?}"
    );
    let data_count = rec.data_values().len();
    assert_eq!(
        data_count,
        0,
        "zip must NOT emit any tuples when one source is Dead at \
         activation. data={:?}",
        rec.data_values()
    );
}

#[test]
fn zip_self_completes_when_all_sources_are_dead() {
    let rt = OpRuntime::new();
    let a = rt.state_int(Some(1));
    let b = rt.state_int(Some(2));
    rt.core().complete(a);
    rt.core().complete(b);

    let pack_fn = rt.register_tuple_packer();
    let rec = rt.with_all_partitions_held(|rt| {
        let z = zip(rt.core(), &rt.producer_binding, vec![a, b], pack_fn).unwrap();
        rt.subscribe_recorder(z)
    });

    assert!(rec.events().contains(&RecordedEvent::Complete));
    assert!(rec.data_values().is_empty());
}

// =====================================================================
// concat — Dead `first` advances phase; Dead `second` flag set
// =====================================================================

#[test]
fn concat_dead_first_immediately_advances_to_second() {
    // Dead `first` triggers the phase transition (treat as
    // first-Complete). If `second` is live, concat then forwards its
    // emissions.
    let rt = OpRuntime::new();
    let first = rt.state_int(Some(1));
    let second = rt.state_int(None);
    rt.core().complete(first); // first becomes Dead at subscribe time

    let rec = rt.with_all_partitions_held(|rt| {
        let c = concat(rt.core(), &rt.producer_binding, first, second);
        rt.subscribe_recorder(c)
    });

    // Phase transition fired; concat now forwards from second.
    rt.emit_int(second, 42);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.
    let values: Vec<i64> = rec.data_values().into_iter().map(|v| v.int()).collect();
    assert!(
        values.contains(&42),
        "concat must forward `second` data after Dead `first` triggered \
         phase transition. events={:?}",
        rec.events()
    );
}

#[test]
fn concat_dead_first_and_dead_second_self_completes() {
    // Both sources Dead → Dead-first phase-transition + Dead-second
    // flag → self-Complete.
    let rt = OpRuntime::new();
    let first = rt.state_int(Some(1));
    let second = rt.state_int(Some(2));
    rt.core().complete(first);
    rt.core().complete(second);

    let rec = rt.with_all_partitions_held(|rt| {
        let c = concat(rt.core(), &rt.producer_binding, first, second);
        rt.subscribe_recorder(c)
    });

    assert!(
        rec.events().contains(&RecordedEvent::Complete),
        "concat with both sources Dead must self-Complete. events={:?}",
        rec.events()
    );
}

// =====================================================================
// race — Dead source marks `completed[idx]`; all-Dead → self-Complete
// =====================================================================

#[test]
fn race_all_dead_sources_self_completes() {
    let rt = OpRuntime::new();
    let a = rt.state_int(Some(1));
    let b = rt.state_int(Some(2));
    rt.core().complete(a);
    rt.core().complete(b);

    let rec = rt.with_all_partitions_held(|rt| {
        let r = race(rt.core(), &rt.producer_binding, vec![a, b]).unwrap();
        rt.subscribe_recorder(r)
    });

    assert!(
        rec.events().contains(&RecordedEvent::Complete),
        "race with all sources Dead must self-Complete. events={:?}",
        rec.events()
    );
}

#[test]
fn race_one_dead_one_live_continues_with_live() {
    // One Dead source is marked completed but doesn't terminate race
    // (the live source can still race-win).
    let rt = OpRuntime::new();
    let dead = rt.state_int(Some(1));
    let live = rt.state_int(None);
    rt.core().complete(dead);

    let rec = rt.with_all_partitions_held(|rt| {
        let r = race(rt.core(), &rt.producer_binding, vec![dead, live]).unwrap();
        rt.subscribe_recorder(r)
    });

    // Live source emits first → wins the race.
    rt.emit_int(live, 99);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.
    let values: Vec<i64> = rec.data_values().into_iter().map(|v| v.int()).collect();
    assert_eq!(
        values,
        vec![99],
        "race with one Dead + one live: live wins after emitting. \
         events={:?}",
        rec.events()
    );
}

// =====================================================================
// take_until — Dead `source` → self-Complete; Dead `notifier` → no-op
// =====================================================================

#[test]
fn take_until_dead_source_self_completes() {
    let rt = OpRuntime::new();
    let source = rt.state_int(Some(1));
    let notifier = rt.state_int(None);
    rt.core().complete(source); // source becomes Dead at subscribe time

    let rec = rt.with_all_partitions_held(|rt| {
        let n = take_until(rt.core(), &rt.producer_binding, source, notifier);
        rt.subscribe_recorder(n)
    });

    assert!(
        rec.events().contains(&RecordedEvent::Complete),
        "take_until with Dead source must self-Complete. events={:?}",
        rec.events()
    );
}

#[test]
fn take_until_dead_notifier_passes_source_through() {
    // Dead `notifier` is ignored — take_until reduces to a passthrough
    // of `source`.
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let notifier = rt.state_int(Some(99));
    rt.core().complete(notifier); // notifier Dead → ignored

    let rec = rt.with_all_partitions_held(|rt| {
        let n = take_until(rt.core(), &rt.producer_binding, source, notifier);
        rt.subscribe_recorder(n)
    });

    // Source emit should still pass through.
    rt.emit_int(source, 7);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.
    let values: Vec<i64> = rec.data_values().into_iter().map(|v| v.int()).collect();
    assert_eq!(
        values,
        vec![7],
        "take_until with Dead notifier passes source DATA through \
         (notifier signal will never fire). events={:?}",
        rec.events()
    );
    assert!(
        !rec.events().contains(&RecordedEvent::Complete),
        "Dead notifier must NOT trigger take_until's self-Complete \
         (only notifier DATA does)"
    );
}
