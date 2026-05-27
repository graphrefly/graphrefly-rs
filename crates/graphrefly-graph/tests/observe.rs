//! `Graph::observe()` / `Graph::observe_all()` default sink-style tap
//! (canonical spec §3.6.2 default mode).

mod common;

use std::sync::{Arc, Mutex};

use common::graph;
use graphrefly_core::{EqualsMode, FnId, HandleId, LockId, Message, Sink, UpError};

fn recording_sink() -> (Arc<Mutex<Vec<Message>>>, Sink) {
    let log: Arc<Mutex<Vec<Message>>> = Arc::new(Mutex::new(Vec::new()));
    let log_for_sink = log.clone();
    let sink: Sink = std::rc::Rc::new(move |msgs: &[Message]| {
        log_for_sink.lock().unwrap().extend_from_slice(msgs);
    });
    (log, sink)
}

#[test]
fn observe_subscribe_taps_named_node() {
    let (rt, g) = graph("system");
    let s = g.state(rt.core(), "a", Some(HandleId::new(1))).unwrap();
    let (log, sink) = recording_sink();
    let obs = g.observe("a").subscribe(rt.core(), sink);
    g.emit(rt.core(), s, HandleId::new(2));
    {
        let log = log.lock().unwrap();
        // Handshake delivers [Start, Data(1)]; wave delivers [Dirty, Data(2)].
        assert!(log.iter().any(|m| matches!(m, Message::Start)));
        assert!(log
            .iter()
            .any(|m| matches!(m, Message::Data(h) if *h == HandleId::new(2))));
    }
    obs.detach(rt.core());
}

#[test]
fn observe_up_pause_pauses_target_node() {
    let (rt, g) = graph("system");
    g.state(rt.core(), "a", Some(HandleId::new(1))).unwrap();
    let observer = g.observe("a");
    let lock = g.alloc_lock_id(rt.core());
    observer.pause(rt.core(), lock).unwrap();
    assert!(rt.core().is_paused(observer.node_id()));
    observer.resume(rt.core(), lock).unwrap();
    assert!(!rt.core().is_paused(observer.node_id()));
}

#[test]
fn observe_up_invalidate_clears_cache() {
    let (rt, g) = graph("system");
    let s = g.state(rt.core(), "a", Some(HandleId::new(7))).unwrap();
    g.observe("a").invalidate(rt.core());
    assert_eq!(g.cache_of(rt.core(), s), graphrefly_core::NO_HANDLE);
}

#[test]
#[should_panic(expected = "Graph::node: no node at path `nope`")]
fn observe_unknown_path_panics() {
    let (_rt, g) = graph("system");
    let _ = g.observe("nope");
}

#[test]
fn observe_all_multicasts_to_every_named_node() {
    let (rt, g) = graph("system");
    let a = g.state(rt.core(), "a", Some(HandleId::new(1))).unwrap();
    let b = g.state(rt.core(), "b", Some(HandleId::new(2))).unwrap();

    let log: Arc<Mutex<Vec<(String, Message)>>> = Arc::new(Mutex::new(Vec::new()));
    let log_for_sink = log.clone();
    let mut all = g.observe_all();
    let count = all.subscribe(rt.core(), move |path: &str, msgs: &[Message]| {
        let mut log = log_for_sink.lock().unwrap();
        for m in msgs {
            log.push((path.to_owned(), *m));
        }
    });
    assert_eq!(count, 2);

    g.emit(rt.core(), a, HandleId::new(10));
    g.emit(rt.core(), b, HandleId::new(20));
    {
        let log = log.lock().unwrap();
        // Each emit produces Dirty + Data; with handshake on both we
        // expect Start + Data(initial) per node + 2 emit waves' Dirty +
        // Data.
        assert!(log
            .iter()
            .any(|(p, m)| p == "a" && matches!(m, Message::Data(h) if *h == HandleId::new(10))));
        assert!(log
            .iter()
            .any(|(p, m)| p == "b" && matches!(m, Message::Data(h) if *h == HandleId::new(20))));
    }
    all.detach(rt.core());
}

#[test]
fn observe_all_late_added_node_not_auto_subscribed_in_v1() {
    let (rt, g) = graph("system");
    g.state(rt.core(), "a", Some(HandleId::new(1))).unwrap();
    let log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let log_for_sink = log.clone();
    let mut all = g.observe_all();
    all.subscribe(rt.core(), move |path: &str, _msgs: &[Message]| {
        log_for_sink.lock().unwrap().push(path.to_owned());
    });
    // Add a node AFTER subscribe — not picked up.
    let b = g.state(rt.core(), "b", Some(HandleId::new(2))).unwrap();
    g.emit(rt.core(), b, HandleId::new(99));
    {
        let log = log.lock().unwrap();
        assert!(!log.iter().any(|p| p == "b"));
        // 'a' however is observed (handshake delivers Start + Data(1)).
        assert!(log.iter().any(|p| p == "a"));
    }
    all.detach(rt.core());
}

#[test]
fn detaching_observe_all_unsubscribes_all_sinks() {
    // D246 rule 3: observe handles have NO RAII `Drop`. Teardown is
    // owner-invoked via `detach`. This test verifies that AFTER
    // `detach`, a subsequent emit reaches no sinks (the behavior the
    // pre-D246 drop-based test asserted — only the trigger changed
    // from scope-drop to explicit detach).
    let (rt, g) = graph("system");
    let s = g.state(rt.core(), "a", Some(HandleId::new(1))).unwrap();
    let log: Arc<Mutex<Vec<Message>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let log_for_sink = log.clone();
        let mut all = g.observe_all();
        all.subscribe(rt.core(), move |_path, msgs| {
            log_for_sink.lock().unwrap().extend_from_slice(msgs);
        });
        all.detach(rt.core()); // explicit owner-invoked teardown
    }
    g.emit(rt.core(), s, HandleId::new(99));
    let log = log.lock().unwrap();
    // We saw the handshake's [Start, Data(1)] before detach, then the
    // emit happens with no live sinks — its Dirty + Data must NOT
    // appear.
    assert!(!log
        .iter()
        .any(|m| matches!(m, Message::Data(h) if *h == HandleId::new(99))));
}

// =====================================================================
// D298 — GraphObserveOne::up(messages) canonical R3.6.2 shape
// =====================================================================
//
// Per Q1 user-locked Option 2 (canonical primary + typed sugar
// coexisting): `up(messages)` dispatches messages UPSTREAM to the
// observed node's deps via [`Core::up`]. Distinct semantic from the
// typed `pause`/`resume`/`invalidate` methods which act on the
// observed node directly. See observe.rs rustdoc on
// `GraphObserveOne` for the two-shape rationale.
//
// Tests pin:
//   (a) up([Pause]) on a derived node pauses each dep (not the obs)
//   (b) up([Pause]) on a leaf state node is a no-op (no upstream)
//   (c) up([Invalidate]) recursively plain-forwards to leaves
//       (R1.4.2 — does not self-process at intermediates)
//   (d) up rejects tier-3 (Data/Resolved) with TierForbidden
//   (e) up rejects tier-5 (Complete/Error) with TierForbidden
//   (f) empty messages slice is Ok(())

#[test]
fn d298_up_pause_pauses_deps_not_observed_node() {
    // Build: a (state) → b (derived from a). Observe b; up([Pause])
    // should pause `a` (the dep), NOT `b` (the observed).
    let (rt, g) = graph("system");
    let a = g.state(rt.core(), "a", Some(HandleId::new(1))).unwrap();
    let b = g
        .derived(rt.core(), "b", &[a], FnId::new(0), EqualsMode::Identity)
        .unwrap();
    let lock = LockId::new(1);

    let observer = g.observe("b");
    observer.up(rt.core(), &[Message::Pause(lock)]).unwrap();

    // Dep was paused (canonical upstream semantic).
    assert!(rt.core().is_paused(a));
    // Observed node was NOT paused (the divergence from the typed
    // `observer.pause(lock)` semantic — that would pause `b`).
    assert!(!rt.core().is_paused(b));
}

#[test]
fn d298_up_pause_on_leaf_state_node_is_noop() {
    // A state node has no deps — `up()` has nothing to forward to.
    // Distinct from typed `observer.pause(lock)` which WOULD pause
    // the state node directly.
    let (rt, g) = graph("system");
    let a = g.state(rt.core(), "a", Some(HandleId::new(1))).unwrap();
    let lock = LockId::new(1);

    let observer = g.observe("a");
    observer.up(rt.core(), &[Message::Pause(lock)]).unwrap();

    // State node a was NOT paused — leaf has no upstream to forward to.
    assert!(!rt.core().is_paused(a));
}

#[test]
fn d298_up_invalidate_plain_forwards_to_leaves_per_r1_4_2() {
    // Build a → b → c (state → derived → derived). Emit a value into
    // `b`'s cache (via emit on `a`, which propagates). Then observe
    // `c` and `up([Invalidate])` — per R1.4.2 plain-forward, the
    // invalidate walks up through `b` to `a`. State node `a` is the
    // leaf; the walk bottoms out there (no deps to recurse into).
    let (rt, g) = graph("system");
    let a = g.state(rt.core(), "a", Some(HandleId::new(1))).unwrap();
    let b = g
        .derived(rt.core(), "b", &[a], FnId::new(0), EqualsMode::Identity)
        .unwrap();
    let _c = g
        .derived(rt.core(), "c", &[b], FnId::new(0), EqualsMode::Identity)
        .unwrap();

    // Activate b + c (subscribe) so their caches are populated.
    let (_log_b, sink_b) = recording_sink();
    let (_log_c, sink_c) = recording_sink();
    let _sub_b = g.observe("b").subscribe(rt.core(), sink_b);
    let _sub_c = g.observe("c").subscribe(rt.core(), sink_c);

    // up([Invalidate]) on `c` plain-forwards to `b` (then to `a`,
    // a leaf). Per R1.4.2 it does NOT self-process at `b` or `c`
    // (no cache clear at the source). The forwarding reaches the
    // leaf state node `a` whose cache is `NO_HANDLE`-rendered when
    // invalidated downstream — but the upstream-forwarded
    // INVALIDATE itself bottoms out at leaves without further effect.
    let observer = g.observe("c");
    observer.up(rt.core(), &[Message::Invalidate]).unwrap();

    // `a` is a state node — leaf in the upstream walk. INVALIDATE
    // forwarded via `up` is plain-forward only; it does not clear
    // the leaf's cache (the downstream cascade from Core::invalidate
    // is what clears caches, and that path is NOT taken by Core::up's
    // plain-forward arm). So `a.cache` remains Data(1).
    assert_eq!(g.cache_of(rt.core(), a), HandleId::new(1));
}

#[test]
fn d298_up_rejects_tier_3_data() {
    let (rt, g) = graph("system");
    g.state(rt.core(), "a", Some(HandleId::new(1))).unwrap();

    let observer = g.observe("a");
    let err = observer
        .up(rt.core(), &[Message::Data(HandleId::new(42))])
        .unwrap_err();
    assert!(matches!(err, UpError::TierForbidden { tier: 3 }));
}

#[test]
fn d298_up_rejects_tier_5_complete() {
    let (rt, g) = graph("system");
    g.state(rt.core(), "a", Some(HandleId::new(1))).unwrap();

    let observer = g.observe("a");
    let err = observer.up(rt.core(), &[Message::Complete]).unwrap_err();
    assert!(matches!(err, UpError::TierForbidden { tier: 5 }));
}

#[test]
fn d298_up_empty_messages_slice_is_ok() {
    let (rt, g) = graph("system");
    g.state(rt.core(), "a", Some(HandleId::new(1))).unwrap();

    let observer = g.observe("a");
    observer.up(rt.core(), &[]).unwrap();
    // No side-effect; just no error.
}

#[test]
fn d298_up_stops_dispatch_on_first_error() {
    // Per the rustdoc contract: "Fails on the first message that
    // errors; subsequent messages are NOT dispatched." We can't easily
    // observe that the SECOND message wasn't dispatched (we'd need an
    // observable side-effect to assert absence), so this test pins
    // the surface contract: a tier-3 in position 0 short-circuits
    // before any of the subsequent tier-allowed messages run.
    let (rt, g) = graph("system");
    let a = g.state(rt.core(), "a", Some(HandleId::new(1))).unwrap();
    let _b = g
        .derived(rt.core(), "b", &[a], FnId::new(0), EqualsMode::Identity)
        .unwrap();
    let lock = LockId::new(1);

    let observer = g.observe("b");
    // Tier-3 first → reject. Tier-2 Pause second → would have paused
    // `a` if dispatched.
    let err = observer
        .up(
            rt.core(),
            &[Message::Data(HandleId::new(99)), Message::Pause(lock)],
        )
        .unwrap_err();
    assert!(matches!(err, UpError::TierForbidden { tier: 3 }));
    // Verify the subsequent Pause was NOT dispatched (short-circuit).
    assert!(!rt.core().is_paused(a));
}
