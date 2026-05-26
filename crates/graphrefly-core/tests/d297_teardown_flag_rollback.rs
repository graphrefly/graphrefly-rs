//! D297 — `has_received_teardown` flag rollback on `BatchGuard`
//! panic-discard (R2.2.7.a + R2.6.4 + R4.3.2 atomicity completeness).
//!
//! Closes D291 deferred-scope Item 1, surfaced by the D297 HALT
//! premise check (2026-05-26):
//!
//! - The original deferral entry framed Item 1 as "academic" — claiming
//!   that combined with D291's COMPLETE-slot rollback, a `teardown` retry
//!   post-rollback "would re-do the auto-COMPLETE prepend correctly."
//! - That framing was WRONG. Walking the code:
//!   `Core::teardown_inner`'s `Action::Visit(id)` arm at
//!   [`crates/graphrefly-core/src/node.rs`] checks
//!   `rec.has_received_teardown` and `continue`s (silent no-op) if true,
//!   BEFORE reaching the `EmitTeardown(id)` → `terminate_node` →
//!   auto-COMPLETE-prepend path. So a retry-teardown post-rollback is
//!   silently swallowed by this very idempotency guard.
//! - Post-rollback the node was left in a contradictory state:
//!   `terminal=None` (D291 restored) ∧ `has_received_teardown=true`
//!   (NOT restored pre-D297). The only path out was
//!   `reset_for_fresh_lifecycle`, gated on
//!   `rec.resubscribable && rec.terminal.is_some()` — neither held
//!   post-rollback. Unreachable from the public surface.
//!
//! **D297 closes this gap** by adding `wave_teardown_snapshots:
//! AHashSet<NodeId>` to [`WaveState`] and
//! `Core::restore_wave_teardown_snapshots` to reset the flag back to
//! `false` on panic-discard. Mirrors D291's `wave_terminal_snapshots`
//! discipline.
//!
//! **Bilateral convergence:** pure-ts has been conformant since D282 —
//! `_preBatchSnapshot.teardownDone` captures the flag at batch entry
//! (`packages/pure-ts/src/core/node.ts:3993`) and
//! `_rollbackBatchPending` restores it (`:4136`). D297 catches Rust up.
//!
//! Source: `~/src/graphrefly-ts/docs/rust-port-decisions.md` D297;
//! `~/src/graphrefly-rs/docs/porting-deferred.md` D291 deferred-scope.

use std::panic;
use std::rc::Rc;
use std::sync::Mutex;

mod common;
use common::{TestRuntime, TestValue};

// ---------------------------------------------------------------------------
// Test 1 — THE regression pin: post-rollback retry-teardown MUST succeed
// (pre-D297 was silently no-op'd by the idempotency guard).
// ---------------------------------------------------------------------------

#[test]
fn d297_retry_teardown_after_panic_succeeds() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let s_id = s.id;

    // Pre-batch: flag is false.
    let core = rt.core();
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        core.batch(|| {
            core.teardown(s_id);
            // At this point inside the wave:
            //   - rec.has_received_teardown = true (set by teardown_inner)
            //   - rec.terminal = Some(Complete) (auto-prepend by terminate_node)
            //   - pending_notify carries the Teardown wire message
            // Throwing triggers BatchGuard::Drop's panic-discard:
            //   - restore_wave_terminal_snapshots → terminal back to None (D291)
            //   - restore_wave_teardown_snapshots → flag back to false (D297)
            //   - pending_notify dropped (Teardown not delivered)
            panic!("user threw mid-batch");
        });
    }));
    assert!(result.is_err(), "panic must propagate out of batch()");

    // D291 invariant: terminal restored.
    assert!(
        rt.core().is_terminal(s_id).is_none(),
        "D291: panic-discard restored rec.terminal to None"
    );

    // Pre-D297 the next teardown silently no-op'd via the
    // has_received_teardown idempotency guard. Subscribe to a sink that
    // records messages, then retry teardown — assert the [COMPLETE,
    // TEARDOWN] pair lands.
    let received: Rc<Mutex<Vec<&'static str>>> = Rc::new(Mutex::new(Vec::new()));
    let received_for_sink = received.clone();
    let sink: graphrefly_core::Sink = Rc::new(move |msgs| {
        let mut r = received_for_sink.lock().unwrap();
        for msg in msgs {
            r.push(match msg {
                graphrefly_core::Message::Start => "START",
                graphrefly_core::Message::Data(_) => "DATA",
                graphrefly_core::Message::Resolved => "RESOLVED",
                graphrefly_core::Message::Dirty => "DIRTY",
                graphrefly_core::Message::Invalidate => "INVALIDATE",
                graphrefly_core::Message::Complete => "COMPLETE",
                graphrefly_core::Message::Error(_) => "ERROR",
                graphrefly_core::Message::Teardown => "TEARDOWN",
                graphrefly_core::Message::Pause(_) => "PAUSE",
                graphrefly_core::Message::Resume(_) => "RESUME",
            });
        }
    });
    let _sub = rt.core().subscribe(s_id, sink);

    // Retry: this MUST land the COMPLETE + TEARDOWN pair (R2.6.4 / Lock 6.F
    // auto-prepend). Pre-D297 this was a silent no-op because
    // has_received_teardown was still true from the rolled-back wave.
    rt.core().teardown(s_id);

    let observed = received.lock().unwrap().clone();
    assert!(
        observed.contains(&"COMPLETE"),
        "D297: retry-teardown post-rollback must auto-prepend COMPLETE \
         (pre-fix the has_received_teardown=true idempotency guard \
         silently swallowed the retry). Observed: {observed:?}"
    );
    assert!(
        observed.contains(&"TEARDOWN"),
        "D297: retry-teardown post-rollback must deliver TEARDOWN. \
         Observed: {observed:?}"
    );
    assert!(
        rt.core().is_terminal(s_id).is_some(),
        "D297: post-retry the node is terminated (auto-COMPLETE landed)"
    );
}

// ---------------------------------------------------------------------------
// Test 2 — success-path idempotency unchanged: a clean `teardown` inside
// `batch()` commits normally; the flag stays true post-commit so a second
// `teardown` is the documented R2.6.4 silent no-op (NOT a regression).
// ---------------------------------------------------------------------------

#[test]
fn d297_success_path_preserves_idempotency_flag() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let s_id = s.id;

    let received: Rc<Mutex<Vec<&'static str>>> = Rc::new(Mutex::new(Vec::new()));
    let received_for_sink = received.clone();
    let sink: graphrefly_core::Sink = Rc::new(move |msgs| {
        let mut r = received_for_sink.lock().unwrap();
        for msg in msgs {
            r.push(match msg {
                graphrefly_core::Message::Complete => "COMPLETE",
                graphrefly_core::Message::Teardown => "TEARDOWN",
                _ => "OTHER",
            });
        }
    });
    let _sub = rt.core().subscribe(s_id, sink);

    // Commit path: teardown inside batch, no throw.
    rt.core().batch(|| {
        rt.core().teardown(s_id);
    });
    let after_first: Vec<&'static str> = received
        .lock()
        .unwrap()
        .iter()
        .filter(|m| **m == "COMPLETE" || **m == "TEARDOWN")
        .copied()
        .collect();
    assert!(
        after_first.contains(&"COMPLETE") && after_first.contains(&"TEARDOWN"),
        "Success path: first teardown delivers [COMPLETE, TEARDOWN]. \
         Observed: {after_first:?}"
    );

    // R2.6.4 contract: second teardown on already-torn-down node is a
    // silent no-op (idempotency guard active). This is the documented
    // contract; D297 only inverts the panic-discard path, not commit.
    let count_before_retry = after_first.len();
    rt.core().teardown(s_id);
    let after_retry: Vec<&'static str> = received
        .lock()
        .unwrap()
        .iter()
        .filter(|m| **m == "COMPLETE" || **m == "TEARDOWN")
        .copied()
        .collect();
    assert_eq!(
        after_retry.len(),
        count_before_retry,
        "R2.6.4: second teardown on already-torn-down node is a silent \
         no-op (idempotency guard still active post-commit). D297 only \
         inverts the panic path. Observed: {after_retry:?}"
    );
}

// ---------------------------------------------------------------------------
// Test 3 — cascade: teardown of a parent visits its children's
// has_received_teardown flag. On panic-discard the children's flags MUST
// also restore (D297's snapshot set captures every node visited by
// teardown_inner, not just the root).
// ---------------------------------------------------------------------------

#[test]
fn d297_cascade_children_teardown_flags_restore() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let s_id = s.id;
    let derived = rt.derived(&[s_id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(n + 1)),
        _ => None,
    });
    // Subscribe derived to instantiate the parent-child edge.
    let _rec = rt.subscribe_recorder(derived);
    assert_eq!(rt.cache_value(derived), Some(TestValue::Int(1)));

    let core = rt.core();
    let _ = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        core.batch(|| {
            // teardown(s_id) cascades through s.children — derived's
            // has_received_teardown also gets flipped to true.
            core.teardown(s_id);
            panic!("rollback me");
        });
    }));

    // Both source AND derived: terminal restored to None (D291).
    assert!(
        rt.core().is_terminal(s_id).is_none(),
        "D291: source rec.terminal restored"
    );
    assert!(
        rt.core().is_terminal(derived).is_none(),
        "D291: derived rec.terminal restored"
    );

    // D297 invariant: both flags restored. Verify by retry-teardown on
    // EACH and asserting it cascades anew. If derived's flag were stuck
    // at true, teardown_inner's Action::Visit would short-circuit and
    // derived would not re-receive COMPLETE.
    let received: Rc<Mutex<Vec<&'static str>>> = Rc::new(Mutex::new(Vec::new()));
    let received_for_sink = received.clone();
    let sink: graphrefly_core::Sink = Rc::new(move |msgs| {
        let mut r = received_for_sink.lock().unwrap();
        for msg in msgs {
            r.push(match msg {
                graphrefly_core::Message::Complete => "COMPLETE",
                graphrefly_core::Message::Teardown => "TEARDOWN",
                _ => "OTHER",
            });
        }
    });
    let _sub_derived = rt.core().subscribe(derived, sink);

    rt.core().teardown(s_id);

    let observed: Vec<&'static str> = received
        .lock()
        .unwrap()
        .iter()
        .filter(|m| **m == "COMPLETE" || **m == "TEARDOWN")
        .copied()
        .collect();
    assert!(
        observed.contains(&"COMPLETE") && observed.contains(&"TEARDOWN"),
        "D297: retry-teardown of source must cascade to derived's flag-reset; \
         derived must receive [COMPLETE, TEARDOWN]. Observed on derived's \
         sink: {observed:?}"
    );
}
