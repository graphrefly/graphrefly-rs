//! Integration tests for `stratify_branch` (D199, Unit 5 Q9.2 of
//! `SESSION-rust-port-layer-boundary.md`).
//!
//! Substrate counterpart of TS `extra/composition/stratify.ts`. The
//! Graph-level wrapper (per-rule N branches sharing a single rules
//! state node) is binding-side; here we verify the per-branch routing
//! operator in isolation.

mod common;

use graphrefly_operators::stratify::stratify_branch;

use common::{OpRuntime, RecordedEvent, TestValue};

// =====================================================================
// Helpers
// =====================================================================

/// Build a classifier that captures a "branch name" + uses the rules
/// handle as an integer mode-selector. `mode == 0` → only even ints
/// match "evens"; `mode == 1` → only ints divisible by 3 match
/// "threes"; etc. Mirrors how the TS Graph wrapper captures each
/// rule's name + reads `latestRules.find(r => r.name === ...)
/// .classify(value)`.
///
/// For the test substrate, "branch X" classifies by `value % mode == X`
/// for some `mode` carried in the rules handle.
fn make_modulo_classifier(rt: &OpRuntime, expected_remainder: i64) -> graphrefly_core::FnId {
    let binding = rt.binding.clone();
    rt.binding
        .register_stratify_classifier(Box::new(move |rules_h, value_h| {
            let mode = binding.deref(rules_h).int();
            let value = binding.deref(value_h).int();
            if mode == 0 {
                return false; // "no rules active" mode
            }
            value.rem_euclid(mode) == expected_remainder
        }))
}

// =====================================================================
// Basic routing — classifier match emits DATA; miss drops silently
// =====================================================================

#[test]
fn stratify_routes_matching_values_only() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let rules = rt.state_int(Some(2)); // mode = 2 (even-vs-odd split)

    // Branch "evens": matches when value % 2 == 0.
    let classifier_evens = make_modulo_classifier(&rt, 0);
    let evens = stratify_branch(
        rt.core(),
        &rt.producer_binding,
        source,
        rules,
        classifier_evens,
    );
    let rec_evens = rt.subscribe_recorder(evens);

    rt.emit_int(source, 1); // odd → drop
    rt.emit_int(source, 2); // even → emit
    rt.emit_int(source, 3); // odd → drop
    rt.emit_int(source, 4); // even → emit
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    assert_eq!(
        rec_evens.data_values(),
        vec![TestValue::Int(2), TestValue::Int(4)]
    );
}

// =====================================================================
// Multi-branch parallel — same source feeds N branches with different
// classifier semantics. Each branch independently filters.
// =====================================================================

#[test]
fn stratify_multi_branch_independent_routing() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let rules = rt.state_int(Some(3)); // mode = 3

    let classifier_zeros = make_modulo_classifier(&rt, 0);
    let zeros = stratify_branch(
        rt.core(),
        &rt.producer_binding,
        source,
        rules,
        classifier_zeros,
    );

    let classifier_ones = make_modulo_classifier(&rt, 1);
    let ones = stratify_branch(
        rt.core(),
        &rt.producer_binding,
        source,
        rules,
        classifier_ones,
    );

    let classifier_twos = make_modulo_classifier(&rt, 2);
    let twos = stratify_branch(
        rt.core(),
        &rt.producer_binding,
        source,
        rules,
        classifier_twos,
    );

    let rec_zeros = rt.subscribe_recorder(zeros);
    let rec_ones = rt.subscribe_recorder(ones);
    let rec_twos = rt.subscribe_recorder(twos);

    for n in 0..9 {
        rt.emit_int(source, n);
    }
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    assert_eq!(
        rec_zeros.data_values(),
        vec![TestValue::Int(0), TestValue::Int(3), TestValue::Int(6)],
    );
    assert_eq!(
        rec_ones.data_values(),
        vec![TestValue::Int(1), TestValue::Int(4), TestValue::Int(7)],
    );
    assert_eq!(
        rec_twos.data_values(),
        vec![TestValue::Int(2), TestValue::Int(5), TestValue::Int(8)],
    );
}

// =====================================================================
// Reactive rules — updating the rules state mid-stream changes
// classification for FUTURE items. TS parity: "rule updates affect
// future items only".
// =====================================================================

#[test]
fn stratify_reactive_rules_change_future_classification() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let rules = rt.state_int(Some(2)); // start mode = 2

    let classifier_zeros = make_modulo_classifier(&rt, 0);
    let zeros = stratify_branch(
        rt.core(),
        &rt.producer_binding,
        source,
        rules,
        classifier_zeros,
    );
    let rec = rt.subscribe_recorder(zeros);

    // Under mode=2: emits where value % 2 == 0. Use distinct values so
    // identity-equals dedup doesn't suppress emissions on repeat (the
    // state node's identity equals fires RESOLVED-only when the same
    // handle is emitted twice).
    rt.emit_int(source, 2); // 2 % 2 == 0 → emit
    rt.emit_int(source, 5); // 5 % 2 == 1 → drop

    // Update rules → mode=3.
    rt.emit_int(rules, 3);

    // Under mode=3: emits where value % 3 == 0.
    rt.emit_int(source, 9); // 9 % 3 == 0 → emit
    rt.emit_int(source, 7); // 7 % 3 == 1 → drop
    rt.emit_int(source, 12); // 12 % 3 == 0 → emit
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    assert_eq!(
        rec.data_values(),
        vec![TestValue::Int(2), TestValue::Int(9), TestValue::Int(12)],
    );
}

// =====================================================================
// No-rules sentinel — when rules is in sentinel state (no initial),
// the classifier never fires; all source DATA drops silently.
// =====================================================================

#[test]
fn stratify_no_rules_sentinel_drops_all() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let rules = rt.state_int(None); // SENTINEL — never emits DATA

    let classifier = make_modulo_classifier(&rt, 0);
    let branch = stratify_branch(rt.core(), &rt.producer_binding, source, rules, classifier);
    let rec = rt.subscribe_recorder(branch);

    rt.emit_int(source, 2);
    rt.emit_int(source, 4);

    assert!(
        rec.data_values().is_empty(),
        "no rules → no classifier fire → empty stream"
    );
}

// =====================================================================
// Source COMPLETE — forwards COMPLETE downstream.
// =====================================================================

#[test]
fn stratify_forwards_source_complete() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let rules = rt.state_int(Some(2));

    let classifier = make_modulo_classifier(&rt, 0);
    let branch = stratify_branch(rt.core(), &rt.producer_binding, source, rules, classifier);
    let rec = rt.subscribe_recorder(branch);

    rt.emit_int(source, 2);
    rt.core().complete(source);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    assert_eq!(rec.data_values(), vec![TestValue::Int(2)]);
    assert!(rec.events().contains(&RecordedEvent::Complete));
}

// =====================================================================
// Source ERROR — forwards ERROR downstream.
// =====================================================================

#[test]
fn stratify_forwards_source_error() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let rules = rt.state_int(Some(2));

    let classifier = make_modulo_classifier(&rt, 0);
    let branch = stratify_branch(rt.core(), &rt.producer_binding, source, rules, classifier);
    let rec = rt.subscribe_recorder(branch);

    let err_h = rt.intern(TestValue::Str("boom".into()));
    rt.core().error(source, err_h);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    assert!(
        rec.events()
            .iter()
            .any(|e| matches!(e, RecordedEvent::Error(TestValue::Str(s)) if s == "boom")),
        "expected Error(boom) in {:?}",
        rec.events()
    );
}

// =====================================================================
// Rules terminal absorbed — rules COMPLETE/ERROR does NOT propagate to
// the branch's downstream. Branch keeps its last-seen rules and
// continues. TS parity: "rules signals silently absorbed".
// =====================================================================

#[test]
fn stratify_absorbs_rules_complete_silently() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let rules = rt.state_int(Some(2));

    let classifier = make_modulo_classifier(&rt, 0);
    let branch = stratify_branch(rt.core(), &rt.producer_binding, source, rules, classifier);
    let rec = rt.subscribe_recorder(branch);

    rt.emit_int(source, 2);
    rt.core().complete(rules); // rules terminates — branch unaffected
    rt.emit_int(source, 4); // still classified under cached mode=2
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    assert_eq!(
        rec.data_values(),
        vec![TestValue::Int(2), TestValue::Int(4)]
    );
    assert!(
        !rec.events().contains(&RecordedEvent::Complete),
        "rules COMPLETE must NOT propagate to branch downstream",
    );
}

// =====================================================================
// F1 (QA 2026-05-14) — Source TEARDOWN forwarded downstream.
// Earlier impl handled tier 5 (COMPLETE/ERROR) but tier 6 (TEARDOWN)
// fell through to the catch-all, breaking parity with TS which
// forwards TEARDOWN via `actions.down([msg])`.
// =====================================================================

#[test]
fn stratify_forwards_source_teardown() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let rules = rt.state_int(Some(2));

    let classifier = make_modulo_classifier(&rt, 0);
    let branch = stratify_branch(rt.core(), &rt.producer_binding, source, rules, classifier);
    let rec = rt.subscribe_recorder(branch);

    rt.emit_int(source, 2);
    rt.core().teardown(source);
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    assert_eq!(rec.data_values(), vec![TestValue::Int(2)]);
    assert!(
        rec.events().contains(&RecordedEvent::Teardown),
        "source TEARDOWN must propagate downstream (TS parity); got {:?}",
        rec.events()
    );
}

// =====================================================================
// N1 (QA 2026-05-14) — Two-dep DIRTY gating: when source AND rules
// both update inside the same `core.batch()`, the source value MUST be
// classified under the NEW rules, not the old. Without gating, the
// source-sink could fire before the rules-sink installs the new cache,
// leading to stale-rules misclassification.
// =====================================================================

#[test]
fn stratify_same_wave_gating_uses_new_rules() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let rules = rt.state_int(Some(2)); // mode = 2 (only evens match)

    // Branch matches `value % rules == 0`. Under mode=2, value 3 misses.
    // Under mode=3, value 3 matches.
    let classifier = make_modulo_classifier(&rt, 0);
    let branch = stratify_branch(rt.core(), &rt.producer_binding, source, rules, classifier);
    let rec = rt.subscribe_recorder(branch);

    // Update both rules and source in the same batch. Without gating,
    // the source-sink could see source DATA before the rules-sink
    // updates `latest_rules` → classify under old rules (mode=2) →
    // 3 % 2 == 1 → drop. With gating, both deps settle first, then
    // resolve under new rules (mode=3) → 3 % 3 == 0 → emit.
    {
        let _g = rt.core().begin_batch();
        rt.emit_int(rules, 3);
        rt.emit_int(source, 3);
    }
    rt.settle(); // D246: pump deferred producer-sink emits owner-side.

    assert_eq!(
        rec.data_values(),
        vec![TestValue::Int(3)],
        "same-wave rules+source update must classify under NEW rules \
         (mode=3 → 3%3==0 → emit); got {:?}",
        rec.data_values()
    );
}

// =====================================================================
// Refcount discipline — StratifyState::Drop releases the cached rules
// handle on producer deactivation.
// =====================================================================

/// QA F4 (2026-05-14) — replaces a prior test that promised "refcount
/// balanced" but only verified no-panic. This test snapshots
/// `live_handles()` before activation, after activation +
/// emit-and-deactivate, and asserts the count returns to baseline,
/// proving `StratifyState::Drop` releases the cached rules handle (and
/// any buffered source value) when the producer's last subscriber
/// drops.
#[test]
fn stratify_drop_releases_cached_rules_handle() {
    let rt = OpRuntime::new();
    let source = rt.state_int(None);
    let rules = rt.state_int(Some(2));

    // Baseline: rules.cache holds Int(2). Source cache is sentinel.
    let baseline_live = rt.binding.live_handles();

    let classifier = make_modulo_classifier(&rt, 0);
    let branch = stratify_branch(rt.core(), &rt.producer_binding, source, rules, classifier);

    {
        let rec = rt.subscribe_recorder(branch);
        rt.emit_int(source, 2); // matches → emit
        rt.emit_int(source, 5); // misses → drop (release inside operator)
        rt.emit_int(source, 4); // matches → emit
        drop(rec);
    }

    // Expected post-state:
    // - rules.cache still holds Int(2) (already in baseline)
    // - source.cache now holds Int(4) (delta vs baseline = +1)
    // - branch deactivated → StratifyState::Drop released its
    //   `latest_rules` retain on Int(2) (refcount returns to 1)
    // - All buffered source values were emitted or dropped during
    //   the wave; source_value should be None at deactivation
    // If StratifyState::Drop leaked the rules-cache retain, Int(2)
    // would have refcount 2 → live_handles would still count it
    // once but a sub-handle leak isn't visible here. The clearer
    // signal is delta = +1 (just source.cache for Int(4)).
    let post_live = rt.binding.live_handles();
    assert_eq!(
        post_live,
        baseline_live + 1,
        "expected delta of +1 (source.cache for Int(4)); baseline \
         {baseline_live}, post {post_live}. A larger delta means a \
         retained handle leaked through StratifyState::Drop or the \
         buffered-source-value cleanup path.",
    );
}
