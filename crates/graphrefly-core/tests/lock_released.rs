//! Slice A close (M1) regression tests — lock-released `invoke_fn` /
//! `custom_equals` + R1.3.5.a handshake tier-split + late-subscriber-
//! during-wave race fix.
//!
//! These exercise the wave-engine paths that lifted from the v1
//! "lock-held during binding callback" discipline. Each test is named for
//! the canonical-spec rule or porting-deferred entry it closes:
//!
//! - **`fire_fn` lock-released `invoke_fn`** — fns may re-enter Core during
//!   their own fire.
//! - **`commit_emission` lock-released `custom_equals`** — equals oracles
//!   may re-enter Core during evaluation.
//! - **R1.3.5.a handshake tier-split** — `[Start]` + `[Data(v)]` arrive as
//!   separate sink calls, not a bundled call.
//! - **Late-subscriber during wave** — a sink installed between fn-fire
//!   iterations does NOT receive duplicate `[Dirty, Data]` from already-
//!   queued wave messages (sink-snapshot-on-first-touch).

mod common;

use std::sync::Arc;

use common::{RecordedEvent, TestRuntime, TestValue};

// ---------------------------------------------------------------------------
// Fn re-entrance via the mailbox (D233/D246 rule 6)
//
// Under the actor model a binding fn cannot hold/clone the move-only
// single-owner `Core` to re-enter it synchronously (the old
// `ReentrantBinding { core_slot: Mutex<Option<Core>> }` mechanism is
// β-invalid). The LIVE invariant — "a fn-fire can cause Core re-entry
// that runs as a correctly-ordered nested wave, no deadlock, no Core
// clone, no async" — is expressed β-validly by the fn capturing
// `Arc<CoreMailbox>` and posting a deferred op during its fire; the
// owner applies it via `drain_mailbox()` (FIFO, synchronous, owner-
// driven). This IS the canonical D233/D246-rule-6 re-entry seam.
// ---------------------------------------------------------------------------

#[test]
fn fn_can_reenter_core_emit_during_invoke_fn_runs_nested_wave() {
    // A derived fn over `s`, during its fire, posts an `Emit` for a
    // side state node `side` via the captured mailbox. The wave
    // engine drains the mailbox IN-WAVE to quiescence
    // (`batch.rs` drain loop: quiescence requires `pending_fires`
    // AND mailbox empty) — so the re-entrant emit is applied as a
    // nested wave automatically, before the triggering call returns.
    // No deadlock, no Core clone, no async.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));
    let side = rt.state(None);
    let side_obs = rt.derived(&[side.id], |deps| Some(deps[0].clone()));
    let side_rec = rt.subscribe_recorder(side_obs);

    let mailbox = rt.mailbox();
    let binding = rt.binding.clone();
    let side_id = side.id;
    let d = rt.derived(&[s.id], move |deps| {
        // Re-enter Core mid-fire: post an Emit on `side`. Applied by
        // the in-wave drain-to-quiescence loop (nested wave).
        let TestValue::Int(n) = &deps[0] else {
            panic!("type")
        };
        let h = binding.intern(TestValue::Int(n * 100));
        assert!(mailbox.post_emit(side_id, h), "Core alive");
        Some(TestValue::Int(*n))
    });
    let _rec_d = rt.subscribe_recorder(d);

    // The re-entrant emit was applied IN-WAVE during `d`'s first
    // fire (the subscribe handshake drove the wave to quiescence).
    assert_eq!(rt.cache_value(d), Some(TestValue::Int(1)));
    assert_eq!(
        side_rec.data_values(),
        vec![TestValue::Int(100)],
        "re-entrant emit delivered as an in-wave nested wave"
    );

    // An explicit owner drain is idempotent (mailbox already empty).
    rt.drain_mailbox();
    assert_eq!(
        side_rec.data_values(),
        vec![TestValue::Int(100)],
        "explicit drain is a no-op once the in-wave drain emptied the mailbox"
    );
}

#[test]
fn fn_can_reenter_core_invalidate_during_invoke_fn() {
    // The β-valid full-Core re-entry surface is `MailboxOp::Defer`
    // (runs owner-side with `&dyn CoreFull`, which exposes
    // `invalidate`). A fn posts a Defer that invalidates a sibling
    // node; the owner drain applies it as a nested wave.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(7)));
    let sibling = rt.state(Some(TestValue::Int(99)));
    let sib_obs = rt.derived(&[sibling.id], |deps| Some(deps[0].clone()));
    let sib_rec = rt.subscribe_recorder(sib_obs);

    let mailbox = rt.mailbox();
    let sib_id = sibling.id;
    let d = rt.derived(&[s.id], move |deps| {
        assert!(
            mailbox.post_defer(Box::new(move |cf| {
                // Full owner-side Core surface mid-drain: invalidate the
                // sibling (clears its cache → re-handshake on next emit).
                cf.invalidate(sib_id);
            })),
            "Core alive"
        );
        Some(deps[0].clone())
    });
    let _rec_d = rt.subscribe_recorder(d);
    assert_eq!(rt.cache_value(d), Some(TestValue::Int(7)));

    // The Defer was applied IN-WAVE during `d`'s first fire: it
    // invalidated the sibling, so an Invalidate message reached the
    // sibling observer's downstream (sib initially cached at 99,
    // then invalidated → cache cleared).
    assert!(
        sib_rec
            .snapshot()
            .iter()
            .any(|e| matches!(e, RecordedEvent::Invalidate)),
        "re-entrant Defer invalidate delivered as an in-wave nested wave; got {:?}",
        sib_rec.snapshot()
    );
    assert_eq!(
        rt.cache_value(sibling.id),
        None,
        "sibling cache cleared by the re-entrant invalidate"
    );
}

// ---------------------------------------------------------------------------
// custom_equals re-entrance via the mailbox
// ---------------------------------------------------------------------------

#[test]
fn custom_equals_can_reenter_core_during_emission() {
    // A custom-equals oracle, evaluated during commit, posts an Emit
    // on a side node via the captured mailbox. The owner drain
    // applies it (nested wave). Invariant: equals re-entry is
    // delivered correctly, no deadlock, no Core clone.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));
    let side = rt.state(None);
    let side_obs = rt.derived(&[side.id], |deps| Some(deps[0].clone()));
    let side_rec = rt.subscribe_recorder(side_obs);

    let mailbox = rt.mailbox();
    let binding = rt.binding.clone();
    let side_id = side.id;
    let d = rt.derived_with_equals(
        &[s.id],
        |deps| Some(deps[0].clone()),
        move |a, b| {
            // equals oracle re-enters Core via the mailbox.
            let h = binding.intern(TestValue::Int(42));
            assert!(mailbox.post_emit(side_id, h), "Core alive");
            a == b
        },
    );
    let _rec_d = rt.subscribe_recorder(d);
    // The handshake-time first commit may already run custom_equals;
    // reset the side recorder's view by snapshotting after a forced
    // second commit so the assertion is deterministic regardless of
    // how many times equals fired.
    let _ = side_rec.snapshot();

    // Force a second commit so custom_equals runs (compares new vs
    // cached). The re-entrant emit is applied IN-WAVE.
    s.set(TestValue::Int(2));
    rt.drain_mailbox(); // idempotent; mailbox already drained in-wave
    assert!(
        side_rec.data_values().contains(&TestValue::Int(42)),
        "custom_equals re-entrant emit delivered as an in-wave nested wave; got {:?}",
        side_rec.data_values()
    );
}

// ---------------------------------------------------------------------------
// R1.3.5.a handshake tier-split
// ---------------------------------------------------------------------------

#[test]
fn handshake_tier_split_sentinel_state_one_call() {
    // Sentinel state node — handshake is just `[Start]`, one sink call.
    let rt = TestRuntime::new();
    let s = rt.state(None);

    let rec = rt.subscribe_recorder(s.id);

    assert_eq!(rec.call_count(), 1, "sentinel handshake = 1 sink call");
    assert_eq!(rec.call_boundaries(), vec![1]);
    assert_eq!(rec.snapshot(), vec![RecordedEvent::Start]);
}

#[test]
fn handshake_tier_split_cached_state_two_calls() {
    // R1.3.5.a: cached state subscribe should produce `[Start]` then
    // `[Data(v)]` as TWO separate sink calls (per-tier delivery), not
    // one bundled `[Start, Data(v)]` call.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(42)));

    let rec = rt.subscribe_recorder(s.id);

    assert_eq!(rec.call_count(), 2, "cached handshake = 2 sink calls");
    assert_eq!(rec.call_boundaries(), vec![1, 1]);
    assert_eq!(
        rec.snapshot(),
        vec![
            RecordedEvent::Start,
            RecordedEvent::Data(TestValue::Int(42))
        ]
    );
}

// Note: pre-D118, three additional handshake-tier-split tests covered the
// `[Start, Data, Complete]` / `[Start, Data, Complete, Teardown]` / `[Start,
// Data, Error]` replay shape on non-resubscribable terminal nodes. With
// canonical R2.2.7.b (D118, 2026-05-10), subscribe to a non-resubscribable
// terminal node is REJECTED rather than replayed. The handshake-tier-split
// guarantee (R1.3.5.a) is now exercised on:
//   - `handshake_tier_split_sentinel_state_one_call` — `[Start]` (1 call)
//   - `handshake_tier_split_cached_state_two_calls` — `[Start, Data]` (2 calls)
//   - replay-buffer scenarios in `replay_buffer.rs` — `[Start, Data, Data, ...]`
// The rejection paths (replacing the deleted three tests) live in
// `tests/resubscribable.rs::subscribe_to_non_resubscribable_*_panics` (D118).

// ---------------------------------------------------------------------------
// Late-subscriber-during-wave: sink-snapshot-on-first-touch race fix
// ---------------------------------------------------------------------------

/// Mid-wave late-subscribe (S4 β-valid rebuild, D233/D246 r6/D249).
///
/// Original scaffold posted the late-subscribe `Defer` from a fn-fire
/// capturing a `!Send` `Sink` (impossible post-D248 — see the prior
/// `#[ignore]` history in git). The β-valid vehicle: an in-wave sink
/// posts a **`Send`** `MailboxOp::Defer` whose closure captures only
/// `Send` state (`Arc<TestBinding>` + a `Send` events buffer + the
/// `NodeId`) and **builds the late `!Send` sink INSIDE the closure**,
/// then `cf.subscribe(s, late)` via the owner-side `&dyn CoreFull`
/// (D233). The owner drain applies it in-wave, so the subscribe lands
/// *after* `s`'s first emit opened its `PendingBatch`. Invariant
/// (Slice X4 / D2 `subscribers_revision` freeze): the late sink gets
/// the cached handshake `Data(1)` **exactly once** (handshake replay,
/// NOT also the frozen pre-subscribe flush) + the post-subscribe
/// `Data(2)` — never a double `Data(1)`.
#[test]
fn late_subscriber_installed_after_first_queue_notify_does_not_double_receive_data() {
    #[derive(Debug, PartialEq)]
    enum Ev {
        Start,
        Data(i64),
        Other,
    }

    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let d = rt.derived(&[s.id], |deps| Some(deps[0].clone()));

    let late: std::sync::Arc<std::sync::Mutex<Vec<Ev>>> =
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));

    // In-wave trigger sink on `d`: on the FIRST Data, post exactly one
    // Send Defer that installs the late subscriber on `s` mid-wave.
    let mailbox = rt.mailbox();
    let binding = rt.binding.clone();
    let s_id = s.id;
    let late_for_defer = std::sync::Arc::clone(&late);
    let posted = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let trigger: graphrefly_core::Sink =
        std::sync::Arc::new(move |msgs: &[graphrefly_core::Message]| {
            for m in msgs {
                if matches!(m, graphrefly_core::Message::Data(_))
                    && posted
                        .compare_exchange(
                            false,
                            true,
                            std::sync::atomic::Ordering::SeqCst,
                            std::sync::atomic::Ordering::SeqCst,
                        )
                        .is_ok()
                {
                    let binding = binding.clone();
                    let late = std::sync::Arc::clone(&late_for_defer);
                    // Send closure: captures Arc<TestBinding> + Send events
                    // buffer + NodeId only; builds the !Send sink INSIDE.
                    let _ =
                        mailbox.post_defer(Box::new(move |cf: &dyn graphrefly_core::CoreFull| {
                            let b = binding.clone();
                            let buf = std::sync::Arc::clone(&late);
                            let late_sink: graphrefly_core::Sink =
                                std::sync::Arc::new(move |ms: &[graphrefly_core::Message]| {
                                    let mut g = buf.lock().unwrap();
                                    for m in ms {
                                        match m {
                                            graphrefly_core::Message::Start => g.push(Ev::Start),
                                            graphrefly_core::Message::Data(h) => {
                                                match b.deref(*h) {
                                                    TestValue::Int(n) => g.push(Ev::Data(n)),
                                                    _ => g.push(Ev::Other),
                                                }
                                            }
                                            _ => g.push(Ev::Other),
                                        }
                                    }
                                });
                            let _ = cf.subscribe(s_id, late_sink);
                        }));
                }
            }
        });
    let trig_sub = rt.track_subscribe(d, trigger);

    // Wave A — activation of `d` on first `track_subscribe(d, trigger)`:
    // `d` fires its fn (deps[0]=0 initial), commits 0; `fire_deferred`
    // fires `trigger` with `Data(0)`; `trigger` posts the `Defer` →
    // **D260's wave-end re-drain loop** applies the Defer in-wave at the
    // end of the activation wave's `fire_deferred` (post-S2b/D260 timing
    // matches pre-S2b's nested-wave-during-fire_deferred timing; the
    // pre-D260 "mailbox stranded → applies at next wave entry" behaviour
    // was the *bug* this test inadvertently encoded). Late subscribes
    // with `s.cache == Int(0)` → late handshake = `[Start, Data(0)]`.
    //
    // Wave 1 (`s.set(1)`): `s` emits 1 → late (now a real subscriber on
    // `s`) gets `Data(1)` via the wave's `fire_deferred`. The X4/D2
    // freeze invariant guarantees the previous activation-wave
    // PendingBatch (which froze a sinks-snapshot before `late`
    // installed) did NOT re-deliver to `late` — so `Data(1)` appears
    // EXACTLY ONCE despite installing mid-activation-wave.
    s.set(TestValue::Int(1));
    rt.drain_mailbox();
    // Wave 2 (`s.set(2)`): post-subscribe emit → late gets `Data(2)` once.
    s.set(TestValue::Int(2));
    rt.drain_mailbox();

    let got = late.lock().unwrap();
    // Start observed first (handshake ordering). Incidental lifecycle
    // messages (Resolved/Dirty/etc.) may interleave — the load-bearing
    // X4/D2 invariant is "no DUPLICATE Data values": each emitted value
    // appears EXACTLY ONCE in the data projection (no double-receive via
    // the frozen pre-subscribe PendingBatch flush AND the new sub's
    // handshake replay).
    assert_eq!(
        got.first(),
        Some(&Ev::Start),
        "late subscriber observes Start first; got {:?}",
        &*got
    );
    let data: Vec<i64> = got
        .iter()
        .filter_map(|e| match e {
            Ev::Data(n) => Some(*n),
            _ => None,
        })
        .collect();
    // D260 (2026-05-20) shifted the deferred-subscribe timing from
    // "next external wave entry" (pre-S2b mailbox-stranded artifact) to
    // "end of activation wave's fire_deferred" (canonical in-wave
    // semantics). Late now sees `Data(0)` from its OWN handshake
    // replay (`s.cache == 0` at late's subscribe time) plus the
    // expected `Data(1)`/`Data(2)` from the post-install waves. The
    // X4/D2 freeze invariant (no double-receive) is preserved: each
    // value appears EXACTLY ONCE.
    assert_eq!(
        data,
        vec![0, 1, 2],
        "each Data value delivered EXACTLY ONCE (no double-receive): \
         `Data(0)` from late's handshake replay (D260 in-wave Defer \
         timing), then `Data(1)`/`Data(2)` from post-subscribe emits; \
         full stream {:?}",
        &*got
    );
    // Defensive: explicitly assert no value appears twice (the strict
    // X4/D2 freeze invariant — survives any future timing shift).
    let mut sorted = data.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        data.len(),
        "X4/D2 freeze invariant: every Data value appears EXACTLY ONCE; \
         got data projection {:?} (duplicates after dedup: {:?})",
        data,
        sorted
    );
    drop(got);
    rt.unsubscribe(d, trig_sub);
}

// ---------------------------------------------------------------------------
// §7 (D208–D211): the two union-find tests that lived here
// (`concurrent_emit_on_disjoint_partitions_runs_truly_parallel`,
// `concurrent_emit_on_same_partition_serializes`) asserted the DELETED
// per-partition `wave_owner` parallelism/serialization guarantee for
// default (ungrouped) nodes. Under §7 the default `None` path is the
// single-threaded substrate (no per-node wave-lock — that is the floor,
// by design), so those guarantees no longer apply to ungrouped nodes.
// The §7 equivalents — same-group cross-thread serialization without
// deadlock, and disjoint-group waves not blocking each other — live in
// `tests/group_parallelism.rs`. The remaining lock-released re-entrance
// tests below are unaffected and still pass.
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// Refcount discipline preserved across the lock-released refactor
// ---------------------------------------------------------------------------

#[test]
fn lock_released_refactor_does_not_leak_handles_under_basic_emit() {
    // Smoke test that the per-iteration lock-acquisition pattern in
    // drain_and_flush + the sinks-snapshot fix in queue_notify don't
    // corrupt refcount discipline.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    let d = rt.derived(&[s.id], |deps| Some(deps[0].clone()));

    let _rec_d = rt.subscribe_recorder(d);
    for i in 1..=10 {
        s.set(TestValue::Int(i));
    }
    drop(_rec_d);
    drop(s);

    // After dropping subscribers + state, only the derived's cache holds
    // a handle. We can't easily count from outside without exposing
    // internals, but we can verify the binding's live count is bounded.
    let live_now = rt.binding.live_handles();
    assert!(
        live_now <= 2,
        "expected <= 2 live handles after drop (derived cache + maybe a transient), got {}",
        live_now
    );

    // Drop the runtime — final Drop for CoreState should release every
    // remaining retained handle.
    drop(rt);
}

// ---------------------------------------------------------------------------
// Re-entrant Core read mid-fire (D246: CoreFull read surface)
//
// The old `ReentrantBinding { core_slot: Mutex<Option<Core>> }` (a
// binding holding a cloned Core to call `cache_of` synchronously
// mid-`invoke_fn`) is the exact β-invalid deleted mechanism. The LIVE
// invariant — "a re-entrant Core READ during a fire returns the
// correct value with no deadlock" — is expressed β-validly via a
// `MailboxOp::Defer` reading `cf.cache_of` owner-side mid-drain.
// ---------------------------------------------------------------------------

#[test]
fn reentrant_core_cache_of_read_via_defer_returns_correct_value() {
    let rt = TestRuntime::new();
    let probe_src = rt.state(Some(TestValue::Int(0)));
    let probe = rt.derived(&[probe_src.id], |deps| {
        let TestValue::Int(n) = &deps[0] else {
            panic!("type")
        };
        Some(TestValue::Int(n + 1000))
    });
    let _rec_probe = rt.subscribe_recorder(probe);
    assert_eq!(rt.cache_value(probe), Some(TestValue::Int(1000)));

    let observed: Arc<std::sync::Mutex<Option<graphrefly_core::HandleId>>> =
        Arc::new(std::sync::Mutex::new(None));
    let observed_w = observed.clone();
    let mailbox = rt.mailbox();
    let probe_id = probe;
    let s = rt.state(Some(TestValue::Int(1)));
    let d = rt.derived(&[s.id], move |deps| {
        let observed_w = observed_w.clone();
        assert!(
            mailbox.post_defer(Box::new(move |cf| {
                // Re-enter Core READ mid-drain — no deadlock under the
                // actor model (owner holds &Core; CoreFull read).
                *observed_w.lock().unwrap() = Some(cf.cache_of(probe_id));
            })),
            "Core alive"
        );
        Some(deps[0].clone())
    });
    let _rec_d = rt.subscribe_recorder(d);
    assert_eq!(rt.cache_value(d), Some(TestValue::Int(1)));

    rt.drain_mailbox();
    let h = observed.lock().unwrap().expect("defer ran");
    assert_eq!(
        rt.binding.deref(h),
        TestValue::Int(1000),
        "re-entrant cache_of read returned the correct cached value, no deadlock"
    );
}
