//! D296 — `pending_wipes` panic-discard pin (R2.4.6 + R4.3.2).
//!
//! D291 closed the cross-track-ledger §1 D282 row's "Case 5" — terminal
//! slot rollback on [`BatchGuard`] panic-discard. The /qa deferred-scope
//! recorded four lift candidates touching `Core::terminate_node`'s wave
//! state:
//!
//! - `has_received_teardown` flag rollback
//! - `pause_state` drain rollback
//! - `replay_buffer` drain rollback
//! - `ws.pending_wipes` queue rollback (this slice's scope)
//!
//! Item 4's framing in [`porting-deferred.md`] flagged the `mem::take` at
//! the panic-discard path ([`batch.rs`] `discard_wave_cleanup`) as a
//! D061-style leak: "post-D291 rollback restores `rec.terminal = None`,
//! but the wipe push was already discarded → the binding-side
//! `NodeCtxState` survives indefinitely."
//!
//! **Pre-implementation premise check (D296 HALT, 2026-05-26) reframed
//! that finding.** Walking the panic-discard path post-D291:
//!
//! 1. `wave_terminal_snapshots` restore sets `rec.terminal = None` — the
//!    lifecycle did NOT end (the terminal was rolled back).
//! 2. `wipe_ctx` per R2.4.6 fires "on resubscribable terminal reset" —
//!    when a terminal lifecycle COMPLETES, not when one is
//!    attempted-and-aborted.
//! 3. R4.3.2's atomicity invariant requires post-rollback observable
//!    state ≡ pre-batch observable state. The binding's `NodeCtxState`
//!    entry is the pre-batch lifecycle that **continues** post-rollback
//!    — wiping it would VIOLATE R4.3.2.
//!
//! Therefore: **the existing `mem::take` is the correct behavior, NOT a
//! leak.** Wipes must NOT fire on panic-discard; the binding's
//! `NodeCtxState` must survive across the rolled-back batch.
//!
//! Item 4 ships as test-only hardening (this file + doc comments at
//! `discard_wave_cleanup`) to pin the existing-correct behavior so a
//! future refactor of `Core::terminate_node` / `discard_wave_cleanup` /
//! `wave_terminal_snapshots` can't silently regress the symmetry between
//! terminal-slot rollback and pending-wipe drop.
//!
//! Source: D291 deferred-scope Item 4 (`~/src/graphrefly-rs/docs/porting-deferred.md`);
//! `~/src/graphrefly-ts/docs/rust-port-decisions.md` D296.

use std::panic;

mod common;
use common::{TestRuntime, TestValue};

// ---------------------------------------------------------------------------
// Test 1 — panic-discard: wipe_ctx must NOT fire AND `NodeCtxState`
// must survive the rollback (R4.3.2 atomicity for the binding side).
// ---------------------------------------------------------------------------

#[test]
fn d296_panic_discard_does_not_fire_wipe_and_preserves_node_ctx() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let s_id = s.id;
    rt.core().set_resubscribable(s_id, true);

    // Populate the binding-side NodeCtxState BEFORE the batch so the
    // pre-batch observable state has an entry to preserve.
    rt.binding.store_set(s_id, "k", TestValue::Int(99));
    assert!(
        rt.binding.has_ctx(s_id),
        "pre-batch: NodeCtxState entry seeded"
    );

    let core = rt.core();
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        core.batch(|| {
            // resubscribable + no live subs → terminate_node pushes onto
            // ws.pending_wipes; rec.terminal = Some(Complete) (visible
            // mid-batch; restored to None on panic-discard per D291).
            core.complete(s_id);
            panic!("user threw mid-batch");
        });
    }));
    assert!(result.is_err(), "panic must propagate out of batch()");

    // D291 invariant: terminal slot restored.
    assert!(
        rt.core().is_terminal(s_id).is_none(),
        "D291: panic-discard restored rec.terminal to None"
    );

    // D296 invariant: wipe_ctx must NOT have fired. The lifecycle did
    // not end (terminal rolled back); per R2.4.6 wipe fires on
    // *resubscribable terminal RESET*, not on attempted-and-aborted.
    assert!(
        rt.binding.wipe_calls().is_empty(),
        "D296 / R2.4.6: wipe_ctx must NOT fire on panic-discard; queue \
         dropped via mem::take in discard_wave_cleanup. Observed: {:?}",
        rt.binding.wipe_calls()
    );

    // D296 invariant: binding-side NodeCtxState entry survives. This is
    // the R4.3.2 atomicity contract — pre-batch observable state must
    // equal post-rollback observable state, including binding-side
    // ctx storage.
    assert!(
        rt.binding.has_ctx(s_id),
        "D296 / R4.3.2: NodeCtxState entry must survive the rolled-back \
         batch (pre-batch state preserved)"
    );
    assert_eq!(
        rt.binding.store_get(s_id, "k"),
        Some(TestValue::Int(99)),
        "D296 / R4.3.2: store contents preserved across rollback"
    );
}

// ---------------------------------------------------------------------------
// Test 2 — success path: wipe_ctx MUST fire exactly once (D069 regression
// pin so the panic-path doc-comment + future invariant tightening can't
// accidentally suppress the success-path wipe).
// ---------------------------------------------------------------------------

#[test]
fn d296_success_path_fires_wipe_exactly_once() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let s_id = s.id;
    rt.core().set_resubscribable(s_id, true);

    rt.binding.store_set(s_id, "k", TestValue::Int(7));
    assert!(rt.binding.has_ctx(s_id));

    // Commit path: complete inside batch, no throw. terminate_node's
    // `queue_wipe` arm pushes onto pending_wipes; BatchGuard's success
    // path drains via fire_deferred → binding.wipe_ctx(s_id) → the
    // TestBinding removes the NodeCtxState entry.
    rt.core().batch(|| {
        rt.core().complete(s_id);
    });

    assert!(
        rt.core().is_terminal(s_id).is_some(),
        "D291 / R4.3.2 success: terminal stays committed"
    );
    assert_eq!(
        rt.binding.wipe_calls(),
        vec![s_id],
        "D069 / D296: success-path drain fires wipe_ctx exactly once"
    );
    assert!(
        !rt.binding.has_ctx(s_id),
        "D069 / D296: wipe_ctx evicted the binding-side NodeCtxState entry"
    );
}
