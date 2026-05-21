//! S7 / D260 + D261 regression tests — wave-end + handshake-boundary
//! mailbox-drain-to-quiescence.
//!
//! Both decisions plug the **same root mechanism** (mailbox post stranded
//! past a sink-fire boundary) at two different boundaries:
//!
//! - **D260** (`BatchGuard::Drop`): producer-build sinks fire INSIDE
//!   `fire_deferred` (post-`drain_and_flush`). When they emit via
//!   `MailboxEmitter::emit_or_defer` / `SinkEmitter::emit`, the
//!   `MailboxOp::Emit` post sits stranded — the wave's drain loop already
//!   exited. Pre-D260: next external wave entry would drain it. Post-D260:
//!   `BatchGuard::Drop` loops `drain_and_flush → fire_deferred` until
//!   mailbox + deferred quiesce.
//! - **D261** (`Core::try_subscribe`): the per-tier handshake-fire slices
//!   (`[Start]/[Data(cache)]?/[Complete]?/[Error(h)]?/[Teardown]?`) fire
//!   sinks LOCK-RELEASED outside any `BatchGuard`. A sink that posts to
//!   mailbox during push-on-subscribe replay (e.g. an operator's internal
//!   sink that re-emits via `SinkEmitter`) would otherwise leave the post
//!   stranded until the next external wave. Post-D261: `try_subscribe`
//!   enters a brief `BatchGuard` after the handshake-fire if mailbox/
//!   deferred is runnable; the guard's Drop runs the same D260 loop.
//!
//! Surfaced via parity-test failures in `messaging/subscription.test.ts`
//! (`view(slice)`) + `operators/{buffer,stratify,higher-order,control}.
//! test.ts` (TSFN-bridged sinks). 35 failures → 0 after both fixes.
//!
//! Canonical: `~/src/graphrefly-ts/docs/rust-port-decisions.md` D260 + D261.

mod common;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use common::{TestRuntime, TestValue};
use graphrefly_core::{Message, Sink};

/// **D260** — producer-build sink fires in `fire_deferred` (post-drain),
/// posts `MailboxOp::Emit` for a side node via the captured mailbox;
/// subscriber on the side node MUST receive the emit within the same
/// outer wave (not deferred to the next external wave entry).
///
/// This mirrors the failure shape of `graphrefly_operators::buffer`'s
/// `notifier_sink`: a long-lived sink installed via
/// `core.subscribe(notifier, notifier_sink)` fires in fire_deferred
/// when notifier emits DATA, then calls
/// `ProducerEmitter::emit_or_defer(buffer_pid, packed)` which posts via
/// mailbox. Pre-D260 the buffer's downstream subscriber never saw the
/// flush emission within the wave that triggered it.
#[test]
fn d260_fire_deferred_sink_mailbox_post_drains_within_outer_wave() {
    let rt = TestRuntime::new();

    // `trigger` is the "notifier" node — flips a wave.
    let trigger = rt.state(None);
    // `side` is the "buffer output" — receives the emit posted by the
    // sink that fires when `trigger` emits.
    let side = rt.state(None);
    let side_rec = rt.subscribe_recorder(side.id);

    // Long-lived sink on `trigger` that fires lock-released in
    // `fire_deferred` (post-drain). On DATA, posts `MailboxOp::Emit`
    // for `side` via the captured mailbox — the canonical
    // producer-build sink shape.
    let mailbox = rt.mailbox();
    let binding = rt.binding.clone();
    let side_id = side.id;
    let trigger_sink: Sink = Arc::new(move |msgs| {
        for m in msgs {
            if let Message::Data(_) = m {
                let h = binding.intern(TestValue::Int(100));
                assert!(mailbox.post_emit(side_id, h), "Core alive");
            }
        }
    });
    let _trigger_sub = rt.track_subscribe(trigger.id, trigger_sink);

    // Trigger an emit on `trigger`. The wave fires `trigger_sink` in
    // fire_deferred → sink posts to mailbox → D260's loop drains
    // → side emits → side_rec fires (queued in nested wave's
    // fire_deferred).
    let h_trig = rt.binding.intern(TestValue::Int(1));
    rt.core().emit(trigger.id, h_trig);

    // Pre-D260 expected: `side_rec.data_values()` was `[]` — the
    // mailbox post sat stranded; subscriber never received the emit
    // within this outer wave.
    // Post-D260 expected: `[Int(100)]` — D260's post-fire_deferred
    // re-drain loop applied the queued `MailboxOp::Emit`, the nested
    // wave committed side's emission, and the nested wave's
    // fire_deferred fired `side_rec`'s sink.
    assert_eq!(
        side_rec.data_values(),
        vec![TestValue::Int(100)],
        "D260: producer-build sink's mailbox post in fire_deferred \
         must drain within the same outer wave; subscriber on the \
         posted target must fire before the outer BatchGuard returns"
    );
}

/// **D261** — `Core::try_subscribe`'s handshake-fire phase fires sinks
/// LOCK-RELEASED outside any `BatchGuard`. A sink that posts to mailbox
/// during push-on-subscribe replay must be drained before
/// `try_subscribe` returns — otherwise the **next** subscribe to the
/// posted target sees `cache == NO_HANDLE` and omits the DATA handshake
/// slice.
///
/// This mirrors the failure shape of `reactive_log.view(slice)`:
/// `view()` calls `core.subscribe(log.node_id, internal_sink)` where
/// `internal_sink` is a `SinkEmitter`-emitting closure. Push-on-subscribe
/// replay fires `internal_sink` with log's cached state; the sink computes
/// the slice and posts `MailboxOp::Emit(view_node, packed)` to mailbox.
/// Pre-D261 the post sat stranded; the next `view.subscribe(js_sink)`
/// saw `view_node.cache == NO_HANDLE` and JS sub never received the
/// initial window.
#[test]
fn d261_handshake_fire_sink_mailbox_post_drains_before_subscribe_returns() {
    let rt = TestRuntime::new();

    // `target` is the "view_node" — the sink-fire's mailbox-post target.
    let target = rt.state(None);

    // `source` is the "log.node_id" — pre-populated with a value (cached
    // DATA). Subscribing to it fires the handshake replay with [Data].
    let source = rt.state(Some(TestValue::Int(7)));

    // Track how many times the source-sink fired (verifies the
    // handshake replay actually invoked the sink).
    let source_sink_fires = Arc::new(AtomicU64::new(0));
    let source_sink_fires_c = source_sink_fires.clone();

    // Long-lived sink on `source`. On DATA (including push-on-subscribe
    // replay's [Data(cache)] slice), posts `MailboxOp::Emit` for
    // `target` via the captured mailbox — mirrors the `view`'s internal
    // sink that re-emits via `SinkEmitter` during the replay-fire.
    let mailbox = rt.mailbox();
    let binding = rt.binding.clone();
    let target_id = target.id;
    let source_sink: Sink = Arc::new(move |msgs| {
        for m in msgs {
            if let Message::Data(_) = m {
                source_sink_fires_c.fetch_add(1, Ordering::Relaxed);
                let h = binding.intern(TestValue::Int(99));
                assert!(mailbox.post_emit(target_id, h), "Core alive");
            }
        }
    });
    let _source_sub = rt.track_subscribe(source.id, source_sink);

    // Push-on-subscribe replay must have fired the source-sink
    // EXACTLY ONCE (source.cache was Int(7); R1.3.5.a [Data(cache)]
    // single-slice). A future regression that fires the handshake
    // replay sink twice (double-deliver) would silently pass `>= 1`
    // but fail `== 1` — tighten for regression-pinning.
    assert_eq!(
        source_sink_fires.load(Ordering::Relaxed),
        1,
        "push-on-subscribe replay should have fired source_sink EXACTLY ONCE with \
         cached DATA (R1.3.5.a [Data(cache)] single slice)"
    );

    // The CRITICAL assertion: by the time the source-subscribe returns,
    // the mailbox post for `target` MUST be drained (D261). Therefore
    // `target.cache` is set to the posted handle.
    //
    // Pre-D261 expected: `target.cache_of() == NO_HANDLE` (post stranded).
    // Post-D261 expected: `target.cache_of() != NO_HANDLE` — the
    // try_subscribe-tail mailbox drain applied the queued op.
    let target_cache = rt.cache_value(target.id);
    assert_eq!(
        target_cache,
        Some(TestValue::Int(99)),
        "D261: source_sink's mailbox post during handshake-fire must drain \
         by subscribe-return; target.cache should be the posted handle"
    );

    // A second subscriber on `target` (the "view.subscribe(js_sink)"
    // path) must now see [Data(99)] in its handshake replay. Pre-D261
    // this would be a [Start]-only handshake (cache == NO_HANDLE).
    let target_rec = rt.subscribe_recorder(target.id);
    assert_eq!(
        target_rec.data_values(),
        vec![TestValue::Int(99)],
        "D261: late subscriber to the posted-target node should see the \
         cached value via push-on-subscribe replay"
    );
}
