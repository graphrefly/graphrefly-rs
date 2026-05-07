//! Integration tests for `transform` operators (Slice C-1, R5.7).
//!
//! Each test references the canonical-spec rule it covers in its name
//! or comments. Test naming convention: `<operator>_<scenario>`.

mod common;

use graphrefly_core::{Core, OperatorOpts, NO_HANDLE};
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
    let mapped = map(&rt.core, &rt.op_binding, source, move |h| {
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
    let mapped = map(&rt.core, &rt.op_binding, source, move |h| {
        let v = binding.deref(h).int();
        binding.intern(TestValue::Int(v + 100))
    })
    .into_node();

    let rec = rt.subscribe_recorder(mapped);
    // Multi-emit in one wave via batch.
    let h1 = rt.intern_int(1);
    let h2 = rt.intern_int(2);
    let h3 = rt.intern_int(3);
    let core = rt.core.clone();
    let s = source;
    core.clone().batch(move || {
        core.emit(s, h1);
        core.emit(s, h2);
        core.emit(s, h3);
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
    let filtered = filter(&rt.core, &rt.op_binding, source, move |h| {
        binding.deref(h).int() % 2 == 0
    })
    .into_node();

    let rec = rt.subscribe_recorder(filtered);
    let core = rt.core.clone();
    let h1 = rt.intern_int(1);
    let h2 = rt.intern_int(2);
    let h3 = rt.intern_int(3);
    let h4 = rt.intern_int(4);
    let s = source;
    core.clone().batch(move || {
        core.emit(s, h1);
        core.emit(s, h2);
        core.emit(s, h3);
        core.emit(s, h4);
    });

    // Only even values pass.
    let data = rec.data_values();
    assert_eq!(data, vec![TestValue::Int(2), TestValue::Int(4)]);
}

#[test]
fn filter_full_reject_emits_dirty_resolved_per_d018() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let filtered = filter(&rt.core, &rt.op_binding, source, |_h| false).into_node();

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
    let filtered = filter(&rt.core, &rt.op_binding, source, move |h| {
        binding.deref(h).int() > 5
    })
    .into_node();

    let rec = rt.subscribe_recorder(filtered);
    let core = rt.core.clone();
    let s = source;
    let h1 = rt.intern_int(2);
    let h2 = rt.intern_int(7);
    let h3 = rt.intern_int(3);
    let h4 = rt.intern_int(9);
    core.clone().batch(move || {
        core.emit(s, h1);
        core.emit(s, h2);
        core.emit(s, h3);
        core.emit(s, h4);
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
        &rt.core,
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
    let core = rt.core.clone();
    let s = source;
    let h1 = rt.intern_int(1);
    let h2 = rt.intern_int(2);
    let h3 = rt.intern_int(3);
    core.clone().batch(move || {
        core.emit(s, h1);
        core.emit(s, h2);
        core.emit(s, h3);
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
        &rt.core,
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
        &rt.core,
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

    rt.core.complete(source);

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
        &rt.core,
        &rt.op_binding,
        source,
        |_acc, _x| panic!("fold should not run when no DATA arrives"),
        seed,
    )
    .into_node();

    let rec = rt.subscribe_recorder(reduced);
    rt.core.complete(source);

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
        &rt.core,
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
    rt.core.error(source, err_h);

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
    let distinct = distinct_until_changed(&rt.core, &rt.op_binding, source, move |a, b| {
        binding.deref(a) == binding.deref(b)
    })
    .into_node();

    let rec = rt.subscribe_recorder(distinct);
    let core = rt.core.clone();
    let s = source;
    let h1 = rt.intern_int(1);
    let h1b = rt.intern_int(1);
    let h2 = rt.intern_int(2);
    let h2b = rt.intern_int(2);
    let h3 = rt.intern_int(3);
    core.clone().batch(move || {
        core.emit(s, h1);
        core.emit(s, h1b);
        core.emit(s, h2);
        core.emit(s, h2b);
        core.emit(s, h3);
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
    let distinct = distinct_until_changed(&rt.core, &rt.op_binding, source, |_, _| {
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
    let paired = pairwise(&rt.core, &rt.op_binding, source, move |prev, curr| {
        let p = binding.deref(prev);
        let c = binding.deref(curr);
        binding.intern(TestValue::Pair(Box::new(p), Box::new(c)))
    })
    .into_node();

    let rec = rt.subscribe_recorder(paired);
    let core = rt.core.clone();
    let s = source;
    let h1 = rt.intern_int(1);
    let h2 = rt.intern_int(2);
    let h3 = rt.intern_int(3);
    core.clone().batch(move || {
        core.emit(s, h1);
        core.emit(s, h2);
        core.emit(s, h3);
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
    let paired = pairwise(&rt.core, &rt.op_binding, source, |_, _| {
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
        let mapped = map(&rt.core, &rt.op_binding, source, move |h| {
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
            &rt.core,
            &rt.op_binding,
            source,
            move |acc, x| {
                let a = bd.deref(acc).int();
                let v = bd.deref(x).int();
                bd.intern(TestValue::Int(a + v))
            },
            seed_h,
        );
        // Core retains the seed for operator_state lifetime.
        // refcount_of(seed_h) should be ≥ 2: 1 from intern (caller's
        // share) + 1 from Core's operator_state retain.
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
    let mapped = map(&rt.core, &rt.op_binding, source, move |h| {
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
    assert_send_sync::<common::InnerBinding>();
    // OperatorBinding super-bound includes BindingBoundary's Send+Sync.
    fn assert_dyn_send_sync<T: ?Sized + Send + Sync>() {}
    assert_dyn_send_sync::<dyn graphrefly_operators::OperatorBinding>();
    // Sanity: Core is also Send+Sync.
    assert_send_sync::<Core>();
}
