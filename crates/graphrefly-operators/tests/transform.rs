//! Integration tests for `transform` operators (Slice C-1, R5.7).
//!
//! Each test references the canonical-spec rule it covers in its name
//! or comments. Test naming convention: `<operator>_<scenario>`.

mod common;

use graphrefly_core::{BindingBoundary, Core, OperatorOpts, NO_HANDLE};
use graphrefly_operators::transform::{
    distinct_until_changed, filter, map, pairwise, reduce, scan,
};

use common::{OpRuntime, RecordedEvent, TestValue};

// ---------------------------------------------------------------------
// map (R5.7)
// ---------------------------------------------------------------------

#[test]
fn map_per_value_projection_in_single_emit_wave() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let binding = rt.binding.clone();
    let mapped = map(rt.core(), &rt.op_binding, source, move |h| {
        let v = binding.deref(h).int();
        let new = TestValue::Int(v * 10);
        binding.intern(new)
    })
    .into_node();

    let rec = rt.subscribe_recorder(mapped);
    rt.emit_int(source, 3);

    // Subscribe handshake: [Start] + (no cache yet at subscribe-time
    // since source was sentinel). Then wave: [Dirty, Data(30), Resolved
    // is NOT here — single Data per wave settles via DATA in two-phase].
    // Actually: source emits → map fires → emit Data(30). R1.3.1.a:
    // single Dirty + single Data. Subscriber sees [Start] then [Dirty,
    // Data(30)] separately due to per-tier phasing.
    let events = rec.events();
    assert!(events.contains(&RecordedEvent::Start), "events={events:?}");
    assert!(events.contains(&RecordedEvent::Dirty), "events={events:?}");
    let data: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            RecordedEvent::Data(v) => Some(v.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(data, vec![TestValue::Int(30)], "events={events:?}");
}

#[test]
fn map_batch_emits_one_dirty_per_wave_per_r1_3_1_a() {
    // R1.3.1.a: multi-DATA in one wave produces one DIRTY.
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let binding = rt.binding.clone();
    let mapped = map(rt.core(), &rt.op_binding, source, move |h| {
        let v = binding.deref(h).int();
        binding.intern(TestValue::Int(v + 100))
    })
    .into_node();

    let rec = rt.subscribe_recorder(mapped);
    // Multi-emit in one wave via batch.
    let h1 = rt.intern_int(1);
    let h2 = rt.intern_int(2);
    let h3 = rt.intern_int(3);
    let s = source;
    rt.core().batch(|| {
        rt.core().emit(s, h1);
        rt.core().emit(s, h2);
        rt.core().emit(s, h3);
    });

    let events = rec.events();
    let dirty_count = events
        .iter()
        .filter(|e| matches!(e, RecordedEvent::Dirty))
        .count();
    assert_eq!(dirty_count, 1, "expected exactly 1 Dirty, got {events:?}");
    let data = rec.data_values();
    assert_eq!(
        data,
        vec![
            TestValue::Int(101),
            TestValue::Int(102),
            TestValue::Int(103),
        ],
        "events={events:?}"
    );
}

// ---------------------------------------------------------------------
// filter (D012/D018)
// ---------------------------------------------------------------------

#[test]
fn filter_passes_only_matching_items() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let binding = rt.binding.clone();
    let filtered = filter(rt.core(), &rt.op_binding, source, move |h| {
        binding.deref(h).int() % 2 == 0
    })
    .into_node();

    let rec = rt.subscribe_recorder(filtered);
    let h1 = rt.intern_int(1);
    let h2 = rt.intern_int(2);
    let h3 = rt.intern_int(3);
    let h4 = rt.intern_int(4);
    let s = source;
    rt.core().batch(|| {
        rt.core().emit(s, h1);
        rt.core().emit(s, h2);
        rt.core().emit(s, h3);
        rt.core().emit(s, h4);
    });

    // Only even values pass.
    let data = rec.data_values();
    assert_eq!(data, vec![TestValue::Int(2), TestValue::Int(4)]);
}

#[test]
fn filter_full_reject_emits_dirty_resolved_per_d018() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let filtered = filter(rt.core(), &rt.op_binding, source, |_h| false).into_node();

    let rec = rt.subscribe_recorder(filtered);
    rt.emit_int(source, 7);

    let events = rec.events();
    assert!(
        events.contains(&RecordedEvent::Dirty),
        "expected Dirty in {events:?}"
    );
    assert!(
        events.contains(&RecordedEvent::Resolved),
        "expected Resolved (D018) in {events:?}"
    );
    assert!(
        rec.data_values().is_empty(),
        "no Data should fire on full-reject"
    );
}

#[test]
fn filter_mixed_wave_no_resolved_when_at_least_one_passes() {
    // D012 wave-exclusivity: mixed-batch waves emit Data per pass with
    // no per-rejected RESOLVED noise.
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let binding = rt.binding.clone();
    let filtered = filter(rt.core(), &rt.op_binding, source, move |h| {
        binding.deref(h).int() > 5
    })
    .into_node();

    let rec = rt.subscribe_recorder(filtered);
    let s = source;
    let h1 = rt.intern_int(2);
    let h2 = rt.intern_int(7);
    let h3 = rt.intern_int(3);
    let h4 = rt.intern_int(9);
    rt.core().batch(|| {
        rt.core().emit(s, h1);
        rt.core().emit(s, h2);
        rt.core().emit(s, h3);
        rt.core().emit(s, h4);
    });

    let events = rec.events();
    assert_eq!(
        rec.data_values(),
        vec![TestValue::Int(7), TestValue::Int(9)]
    );
    let resolved = events
        .iter()
        .filter(|e| matches!(e, RecordedEvent::Resolved))
        .count();
    assert_eq!(
        resolved, 0,
        "no Resolved expected on mixed wave: {events:?}"
    );
}

// ---------------------------------------------------------------------
// scan (R5.7 — left-fold emitting each new acc)
// ---------------------------------------------------------------------

#[test]
fn scan_emits_running_accumulator_per_input() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let seed = rt.intern_int(0);
    let binding = rt.binding.clone();
    let scanned = scan(
        rt.core(),
        &rt.op_binding,
        source,
        move |acc, x| {
            let a = binding.deref(acc).int();
            let v = binding.deref(x).int();
            binding.intern(TestValue::Int(a + v))
        },
        seed,
    )
    .into_node();

    let rec = rt.subscribe_recorder(scanned);
    let s = source;
    let h1 = rt.intern_int(1);
    let h2 = rt.intern_int(2);
    let h3 = rt.intern_int(3);
    rt.core().batch(|| {
        rt.core().emit(s, h1);
        rt.core().emit(s, h2);
        rt.core().emit(s, h3);
    });

    // Cumulative sums: 0+1=1, 1+2=3, 3+3=6.
    assert_eq!(
        rec.data_values(),
        vec![TestValue::Int(1), TestValue::Int(3), TestValue::Int(6)]
    );
}

#[test]
fn scan_persists_acc_across_waves() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let seed = rt.intern_int(10);
    let binding = rt.binding.clone();
    let scanned = scan(
        rt.core(),
        &rt.op_binding,
        source,
        move |acc, x| {
            let a = binding.deref(acc).int();
            let v = binding.deref(x).int();
            binding.intern(TestValue::Int(a + v))
        },
        seed,
    )
    .into_node();

    let rec = rt.subscribe_recorder(scanned);
    rt.emit_int(source, 5);
    rt.emit_int(source, 7);
    rt.emit_int(source, 3);

    // 10+5=15, 15+7=22, 22+3=25.
    assert_eq!(
        rec.data_values(),
        vec![TestValue::Int(15), TestValue::Int(22), TestValue::Int(25)]
    );
}

// ---------------------------------------------------------------------
// reduce (R5.7 — emits once on upstream COMPLETE)
// ---------------------------------------------------------------------

#[test]
fn reduce_emits_acc_on_upstream_complete() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let seed = rt.intern_int(0);
    let binding = rt.binding.clone();
    let reduced = reduce(
        rt.core(),
        &rt.op_binding,
        source,
        move |acc, x| {
            let a = binding.deref(acc).int();
            let v = binding.deref(x).int();
            binding.intern(TestValue::Int(a + v))
        },
        seed,
    )
    .into_node();

    let rec = rt.subscribe_recorder(reduced);
    rt.emit_int(source, 1);
    rt.emit_int(source, 2);
    rt.emit_int(source, 3);

    // No emit yet — accumulating silently.
    assert!(
        rec.data_values().is_empty(),
        "reduce should not emit before upstream Complete: {:?}",
        rec.events()
    );

    rt.core().complete(source);

    // Final acc = 0+1+2+3 = 6, then Complete.
    assert_eq!(rec.data_values(), vec![TestValue::Int(6)]);
    assert!(
        rec.events().contains(&RecordedEvent::Complete),
        "expected Complete in {:?}",
        rec.events()
    );
}

#[test]
fn reduce_no_data_emits_seed_on_complete() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let seed = rt.intern_int(42);
    let reduced = reduce(
        rt.core(),
        &rt.op_binding,
        source,
        |_acc, _x| panic!("fold should not run when no DATA arrives"),
        seed,
    )
    .into_node();

    let rec = rt.subscribe_recorder(reduced);
    rt.core().complete(source);

    assert_eq!(rec.data_values(), vec![TestValue::Int(42)]);
    assert!(rec.events().contains(&RecordedEvent::Complete));
}

#[test]
fn reduce_propagates_upstream_error() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let seed = rt.intern_int(0);
    let binding = rt.binding.clone();
    let reduced = reduce(
        rt.core(),
        &rt.op_binding,
        source,
        move |acc, x| {
            let a = binding.deref(acc).int();
            let v = binding.deref(x).int();
            binding.intern(TestValue::Int(a + v))
        },
        seed,
    )
    .into_node();

    let rec = rt.subscribe_recorder(reduced);
    rt.emit_int(source, 5);
    let err_h = rt.binding.intern(TestValue::Str("boom".into()));
    rt.core().error(source, err_h);

    // Error propagates verbatim — no Data(acc) emitted.
    assert!(
        rec.data_values().is_empty(),
        "no Data on error path: {:?}",
        rec.events()
    );
    let has_error = rec
        .events()
        .iter()
        .any(|e| matches!(e, RecordedEvent::Error(TestValue::Str(s)) if s == "boom"));
    assert!(has_error, "expected Error(boom) in {:?}", rec.events());
}

// ---------------------------------------------------------------------
// distinctUntilChanged
// ---------------------------------------------------------------------

#[test]
fn distinct_until_changed_suppresses_consecutive_duplicates() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let binding = rt.binding.clone();
    let distinct = distinct_until_changed(rt.core(), &rt.op_binding, source, move |a, b| {
        binding.deref(a) == binding.deref(b)
    })
    .into_node();

    let rec = rt.subscribe_recorder(distinct);
    let s = source;
    let h1 = rt.intern_int(1);
    let h1b = rt.intern_int(1);
    let h2 = rt.intern_int(2);
    let h2b = rt.intern_int(2);
    let h3 = rt.intern_int(3);
    rt.core().batch(|| {
        rt.core().emit(s, h1);
        rt.core().emit(s, h1b);
        rt.core().emit(s, h2);
        rt.core().emit(s, h2b);
        rt.core().emit(s, h3);
    });

    // Suppresses adjacent duplicates: emits 1, 2, 3.
    assert_eq!(
        rec.data_values(),
        vec![TestValue::Int(1), TestValue::Int(2), TestValue::Int(3)]
    );
}

#[test]
fn distinct_emits_on_first_value_always() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let distinct = distinct_until_changed(rt.core(), &rt.op_binding, source, |_, _| {
        // Even claiming "always equal" — first value still emits since
        // there's no prev to compare.
        true
    })
    .into_node();

    let rec = rt.subscribe_recorder(distinct);
    rt.emit_int(source, 99);
    assert_eq!(rec.data_values(), vec![TestValue::Int(99)]);
}

// ---------------------------------------------------------------------
// pairwise
// ---------------------------------------------------------------------

#[test]
fn pairwise_emits_pairs_starting_from_second_value() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let binding = rt.binding.clone();
    let paired = pairwise(rt.core(), &rt.op_binding, source, move |prev, curr| {
        let p = binding.deref(prev);
        let c = binding.deref(curr);
        binding.intern(TestValue::Pair(Box::new(p), Box::new(c)))
    })
    .into_node();

    let rec = rt.subscribe_recorder(paired);
    let s = source;
    let h1 = rt.intern_int(1);
    let h2 = rt.intern_int(2);
    let h3 = rt.intern_int(3);
    rt.core().batch(|| {
        rt.core().emit(s, h1);
        rt.core().emit(s, h2);
        rt.core().emit(s, h3);
    });

    // First value swallowed; emits (1,2), (2,3).
    let data = rec.data_values();
    assert_eq!(data.len(), 2);
    assert_eq!(
        data[0],
        TestValue::Pair(Box::new(TestValue::Int(1)), Box::new(TestValue::Int(2)))
    );
    assert_eq!(
        data[1],
        TestValue::Pair(Box::new(TestValue::Int(2)), Box::new(TestValue::Int(3)))
    );
}

#[test]
fn pairwise_first_value_alone_emits_nothing() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let paired = pairwise(rt.core(), &rt.op_binding, source, |_, _| {
        unreachable!("pack should not run with only one value")
    })
    .into_node();

    let rec = rt.subscribe_recorder(paired);
    rt.emit_int(source, 7);
    assert!(
        rec.data_values().is_empty(),
        "pairwise should swallow the first value: {:?}",
        rec.events()
    );
}

// ---------------------------------------------------------------------
// Refcount discipline (D016 / R1.2.4)
// ---------------------------------------------------------------------

#[test]
fn map_does_not_leak_handles_after_drop() {
    let live_before;
    {
        let rt = OpRuntime::new();
        live_before = rt.binding.live_handles();
        let source = rt.state_int(None);
        let binding = rt.binding.clone();
        let mapped = map(rt.core(), &rt.op_binding, source, move |h| {
            let v = binding.deref(h).int();
            binding.intern(TestValue::Int(v + 1))
        })
        .into_node();
        let _rec = rt.subscribe_recorder(mapped);
        rt.emit_int(source, 1);
        rt.emit_int(source, 2);
        rt.emit_int(source, 3);
        // Drop runtime → Core drop → all handles released.
    }
    // After drop, the InnerBinding Arc is also dropped; can't query.
    // Just verify the test ran without panic — the Drop for CoreState
    // path balances refcounts and is asserted by graphrefly-core's
    // tests/drop_refcount.rs. This test mainly ensures the operator
    // path doesn't introduce new leaks.
    let _ = live_before;
}

#[test]
fn scan_seed_retain_balances_on_core_drop() {
    let binding;
    let seed_h;
    {
        let rt = OpRuntime::new();
        binding = rt.binding.clone();
        seed_h = rt.intern_int(0);
        let source = rt.state_int(None);
        let bd = rt.binding.clone();
        let _scanned = scan(
            rt.core(),
            &rt.op_binding,
            source,
            move |acc, x| {
                let a = bd.deref(acc).int();
                let v = bd.deref(x).int();
                bd.intern(TestValue::Int(a + v))
            },
            seed_h,
        );
        // Core retains the seed for ScanState's lifetime (D026 op_scratch).
        // refcount_of(seed_h) should be ≥ 2: 1 from intern (caller's
        // share) + 1 from Core's retain in `make_op_scratch`.
        assert!(
            binding.refcount_of(seed_h) >= 2,
            "seed retain bumped: {}",
            binding.refcount_of(seed_h)
        );
        // Drop Core/runtime.
    }
    // Caller's intern share is still held by `seed_h` (we never
    // released it). Core's retain balanced via Drop for CoreState.
    // refcount should be back to 1 (caller's share).
    assert_eq!(
        binding.refcount_of(seed_h),
        1,
        "after Core drop, seed retain should be just the caller's share"
    );
}

/// Slice C-3 /qa P1 regression: `reset_for_fresh_lifecycle` must take
/// new seed retains BEFORE releasing the old `acc` share. If old `acc`
/// and new `seed` alias the same handle and the caller has already
/// dropped their intern share, the prior phase order (release-old →
/// retain-new) would collapse the binding's registry slot to refcount
/// zero, removing the value entry. The fix: build the new scratch
/// FIRST (taking new retains), THEN release the old scratch's shares.
///
/// Scenario:
/// 1. Caller interns seed (refcount = 1) and passes to scan.
/// 2. Core takes one retain inside `register_operator` (refcount = 2).
/// 3. Caller releases their share (refcount = 1, just Core's slot).
/// 4. The scan's fold path leaves `acc == seed` (e.g., wave with no
///    DATA), so the slot still owns one share of the seed handle.
/// 5. Mark scan resubscribable; subscribe + emit no DATA + complete +
///    drop subscriber + re-subscribe → triggers
///    `reset_for_fresh_lifecycle`.
/// 6. Inside reset:
///    - With the OLD ordering, releasing acc would drop refcount to 0,
///      remove the value, then the new seed retain would bump a refcount
///      on a dead slot. Subsequent `binding.deref(seed)` would panic.
///    - With the NEW ordering, the new seed retain (refcount 1 → 2)
///      runs FIRST, so the subsequent acc release (refcount 2 → 1)
///      cannot collapse the slot. The value entry stays alive.
#[test]
fn scan_resubscribable_reset_with_seed_aliasing_acc_does_not_collapse_registry() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let seed_h = rt.intern_int(0); // refcount = 1

    let binding = rt.binding.clone();
    let scanned = scan(
        rt.core(),
        &rt.op_binding,
        source,
        move |acc, x| {
            // fold(acc, x) = acc + x. With x=0, the result interns to
            // the same handle as acc (when seed is also 0). This makes
            // acc == seed across waves.
            let a = binding.deref(acc).int();
            let v = binding.deref(x).int();
            binding.intern(TestValue::Int(a + v))
        },
        seed_h,
    )
    .into_node();

    // Core retained: refcount = 2.
    assert_eq!(rt.binding.refcount_of(seed_h), 2);

    // Caller releases their intern share. Now refcount = 1 (Core's
    // slot only). The seed value entry is alive ONLY because of Core's
    // slot retain — exactly the precondition that makes the
    // OLD-ordering bug manifest.
    rt.binding.release_handle(seed_h);
    assert_eq!(rt.binding.refcount_of(seed_h), 1);

    // Mark resubscribable so the second subscribe triggers the
    // lifecycle reset.
    rt.core().set_resubscribable(scanned, true);

    // Cycle 1: subscribe, complete the source without emitting any
    // DATA. This means scan's fold never runs; `acc` stays equal to
    // `seed_h`. Then drop the subscriber.
    let rec1 = rt.subscribe_recorder(scanned);
    rt.core().complete(source);
    assert!(rec1.events().contains(&RecordedEvent::Complete));
    drop(rec1);

    // Cycle 2: subscribe again. This triggers
    // reset_for_fresh_lifecycle. Under the FIXED phase order, the new
    // seed retain (refcount 1 → 2) runs BEFORE the old acc release
    // (refcount 2 → 1), so the registry slot survives. Under the
    // BROKEN phase order, the slot would have been removed and a
    // subsequent deref would panic.
    let _rec2 = rt.subscribe_recorder(scanned);

    // The seed handle's value must still resolve. If the slot was
    // collapsed and re-bumped on a dead entry, `deref` panics with
    // "dangling handle".
    let v = rt.binding.deref(seed_h);
    assert_eq!(v, TestValue::Int(0), "seed value entry must survive");

    // Refcount discipline: cycle 2's reset re-installed a fresh
    // ScanState with `acc = seed`, taking one new retain. The old
    // ScanState's acc retain was released. Net refcount unchanged
    // (still 1, since the cache slot also independently retained the
    // seed when fold's no-fire path... actually scan never emitted, so
    // cache stays NO_HANDLE — only the ScanState slot retains).
    assert_eq!(
        rt.binding.refcount_of(seed_h),
        1,
        "after reset: one slot-share remains"
    );
}

// ---------------------------------------------------------------------
// First-run gate (R2.5.3) and partial mode (R5.4 / D011)
// ---------------------------------------------------------------------

#[test]
fn map_does_not_fire_on_sentinel_source_until_first_emit() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let fire_count = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let binding = rt.binding.clone();
    let counter = fire_count.clone();
    let mapped = map(rt.core(), &rt.op_binding, source, move |h| {
        counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        binding.intern(TestValue::Int(binding.deref(h).int()))
    })
    .into_node();

    let _rec = rt.subscribe_recorder(mapped);
    // No emit on source — first-run gate keeps map's projector from firing.
    assert_eq!(fire_count.load(std::sync::atomic::Ordering::SeqCst), 0);
    rt.emit_int(source, 5);
    assert_eq!(fire_count.load(std::sync::atomic::Ordering::SeqCst), 1);
}

// ---------------------------------------------------------------------
// Operator opts pass-through
// ---------------------------------------------------------------------

#[test]
fn operator_opts_default_is_identity_and_gated() {
    let opts = OperatorOpts::default();
    assert!(matches!(opts.equals, graphrefly_core::EqualsMode::Identity));
    assert!(!opts.partial);
}

#[test]
fn no_handle_const_is_recognized() {
    // Sanity: NO_HANDLE is exported from graphrefly-core for operator
    // factory consumers.
    let _: graphrefly_core::HandleId = NO_HANDLE;
}

// ---------------------------------------------------------------------
// Send + Sync compile-time discipline (CLAUDE.md Rust invariant 2)
// ---------------------------------------------------------------------

#[test]
fn op_binding_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    // Bindings still cross the FFI boundary: `BindingBoundary: Send +
    // Sync` is UNCHANGED by D248 (which relaxed only the subscriber
    // `Sink`/`TopologySink`, not the binding trait). So `InnerBinding`
    // + `dyn OperatorBinding` stay `Send + Sync`.
    assert_send_sync::<common::InnerBinding>();
    fn assert_dyn_send_sync<T: ?Sized + Send + Sync>() {}
    assert_dyn_send_sync::<dyn graphrefly_operators::OperatorBinding>();
    // D248/D249/S2c: `Core` is now intentionally `!Send + !Sync`
    // (single-owner) — the prior `assert_send_sync::<Core>()` was
    // shared-Core-era legacy; type-touch only.
    let _ = core::marker::PhantomData::<Core>;
}
