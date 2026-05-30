//! Behavioral conformance — the **Rust arm** (CSP-6). Public-API-only, mirroring the
//! TS arm's `packages/ts/src/__tests__/conformance.test.ts`. The scenarios are
//! language-neutral (`~/src/graphrefly/spec/conformance.jsonl`); this file drives the
//! Rust runtime to `runtimes.rust = pass` for the **control/terminal slice**:
//!
//! - **C-3** INVALIDATE × ctx.state × onInvalidate (R-invalidate-idempotent /
//!   R-ctx-state / R-cleanup-hooks)
//! - **C-5** PAUSE lockset multi-source (R-pause-lockset / R-pause-modes, default mode)
//! - **C-6** synchronous feedback cycle → ERROR (R-reentrancy / D37 + the D30 catch
//!   boundary), plus the **B25** whole-cascade wave-flag recovery regression.
//!
//! **Async slice (this cut, CSP-6 Slice A):**
//! - **C-2** async result arriving at a paused (async COMPUTE) node — buffer + replay
//!   (R-async-paused / DR-3).
//! - **C-4** mixed sync/async diamond — exactly-once join after both legs settle, the
//!   async leg deferred via a stashed [`DeferredCtx`] (R-diamond / R-two-phase /
//!   R-first-run-gate / R-dirty-before-data).
//! - **C-9** `pausable:false` async leaf source ignores PAUSE (D44 outer gate).
//! - **C-10** `true`-mode async leaf source delivers its own production immediately
//!   under PAUSE (D44 leaf carve-out, B20's twin).
//!
//! Async is SIMULATED deterministically (matching the TS arm, no real timers): the
//! async fn stashes `ctx.defer()` into a shared cell, and the test fires the deferred
//! emit manually — the dispatcher invoke stays sync void (R-sync-core).
//!
//! Authority: `~/src/graphrefly/spec/conformance.jsonl` + `decisions/decisions.jsonl`.
//! (C-7 = up-at-source, C-8 = rewire, C-11 = rewire-deferred — later slices.)

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use graphrefly::{
    AnyValue, DeferredCtx, LockId, Message, Node, NodeOpts, Pausable, PoolKind, Status,
};

type Log = Rc<RefCell<Vec<String>>>;
/// An unsubscribe handle (kept alive to hold a node active).
type Unsub = Box<dyn FnOnce()>;

/// Subscribe a message-kind recorder. Returns `(log, unsub)`; **keep the unsub alive**
/// — dropping it removes the last subscriber and deactivates the node (R-rom-ram).
fn record<T: 'static>(node: &Node<T>) -> (Log, Unsub) {
    let log: Log = Rc::new(RefCell::new(Vec::new()));
    let l = log.clone();
    let unsub = node.subscribe(move |m: &Message<AnyValue>| l.borrow_mut().push(format!("{m:?}")));
    (log, unsub)
}

fn kinds(log: &Log) -> Vec<String> {
    log.borrow().clone()
}

/// C-3 — INVALIDATE cascades exactly once, fires `onInvalidate`, preserves `ctx.state`
/// (lifecycle-continue), resets the dep's `prev_data` → SENTINEL; a second INVALIDATE
/// on an already-reset source is an idempotent no-op (R-invalidate-idempotent).
#[test]
fn c3_invalidate_state_and_on_invalidate() {
    let states_at_run: Rc<RefCell<Vec<Option<String>>>> = Rc::new(RefCell::new(Vec::new()));
    let on_inv = Rc::new(Cell::new(0u32));

    let s = Node::<i32>::state(1);
    let d = {
        let sar = states_at_run.clone();
        let oi = on_inv.clone();
        Node::<i32>::derived(vec![s.erased()], move |ctx| {
            // prior state visible at run time; then set it + register the flush hook
            sar.borrow_mut()
                .push(ctx.state_get::<String>().map(|r| (*r).clone()));
            ctx.state_set("kept".to_string());
            let oi2 = oi.clone();
            ctx.on_invalidate(move || oi2.set(oi2.get() + 1));
            ctx.emit(*ctx.data::<i32>(0).unwrap() * 2);
        })
    };

    let (log, _u) = record(&d);
    assert_eq!(d.cache(), Some(2));
    assert_eq!(*states_at_run.borrow(), vec![None]); // first run: fresh state

    log.borrow_mut().clear();
    s.down(vec![Message::Invalidate]);
    assert_eq!(kinds(&log), vec!["INVALIDATE"]); // cascaded downstream exactly once
    assert_eq!(on_inv.get(), 1); // onInvalidate fired once
    assert_eq!(d.cache(), None); // cache cleared → SENTINEL
    assert_eq!(d.status(), Status::Sentinel);

    // idempotent: a second INVALIDATE on an already-reset source is a no-op
    log.borrow_mut().clear();
    s.down(vec![Message::Invalidate]);
    assert!(kinds(&log).is_empty());
    assert_eq!(on_inv.get(), 1);

    // ctx.state preserved across INVALIDATE (lifecycle-continue, NOT a fresh-lifecycle
    // wipe): the next run sees "kept", not None.
    s.set(5);
    assert_eq!(
        *states_at_run.borrow(),
        vec![None, Some("kept".to_string())]
    );
    assert_eq!(d.cache(), Some(10));
}

/// C-5 — a node stays paused until EVERY lock RESUMEs; a duplicate PAUSE and an
/// unknown-id RESUME are no-ops; default mode fires the fn once with the latest dep
/// value on final-lock RESUME (R-pause-lockset / R-pause-modes).
#[test]
fn c5_pause_lockset_multi_source() {
    let runs = Rc::new(Cell::new(0usize));
    let s = Node::<i32>::state(0);
    let n = {
        let r = runs.clone();
        Node::<i32>::derived(vec![s.erased()], move |ctx| {
            r.set(r.get() + 1);
            ctx.emit(*ctx.data::<i32>(0).unwrap());
        })
    };
    let (_log, _u) = record(&n);
    assert_eq!(n.cache(), Some(0));
    runs.set(0);

    let la = LockId::new("A");
    let lb = LockId::new("B");
    n.up(vec![Message::Pause(la.clone())]);
    n.up(vec![Message::Pause(lb.clone())]);
    n.up(vec![Message::Pause(la.clone())]); // duplicate → idempotent (lockset)

    s.set(1); // dep changes while paused
    assert_eq!(runs.get(), 0); // fn held
    assert_eq!(n.cache(), Some(0));

    n.up(vec![Message::Resume(la.clone())]); // release A — B still held
    assert_eq!(runs.get(), 0); // STILL paused
    assert_eq!(n.cache(), Some(0));

    n.up(vec![Message::Resume(LockId::new("unknown"))]); // unknown id → no-op
    assert_eq!(runs.get(), 0);

    n.up(vec![Message::Resume(lb.clone())]); // last lock released → resume, fire once
    assert_eq!(runs.get(), 1);
    assert_eq!(n.cache(), Some(1));
}

/// C-6 — a synchronous feedback cycle (state→derived→effect, effect writes back to the
/// source) is rejected as ERROR. The substrate panic is caught at the wave-owner and
/// converted to `[[ERROR,e]]` on a node ON the cycle (D30) — it does NOT escape the
/// `subscribe` call (R-reentrancy / D37). B25: the source's wave-flags recover, so a
/// subsequent wave still emits its leading DIRTY (no dropped-DIRTY corruption).
#[test]
fn c6_sync_feedback_cycle_becomes_error_and_recovers() {
    let s = Node::<i32>::state(0);
    let d = Node::<i32>::derived(vec![s.erased()], |ctx| {
        ctx.emit(*ctx.data::<i32>(0).unwrap() + 1)
    });
    let e = {
        let s2 = s.clone(); // a handle to the SAME source node
        Node::<i32>::derived(vec![d.erased()], move |ctx| {
            let v = *ctx.data::<i32>(0).unwrap();
            // feedback: re-drive the source mid-wave, closing s→d→e→s
            s2.set(v);
        })
    };

    // Activating `e` drives the cycle. The throw must NOT escape — the wave-owner catch
    // converts it to ERROR (D30). (If it escaped, this `record`/subscribe would panic
    // and fail the test outright.)
    let (elog, _ue) = record(&e);
    assert!(
        kinds(&elog).contains(&"ERROR".to_string()),
        "the feedback cycle surfaces as ERROR on a cycle node (got {:?})",
        kinds(&elog)
    );
    assert_eq!(e.status(), Status::Errored);

    // B25 regression — the source recovered its wave-flags. The corruption symptom was
    // a stale `emitted_dirty_this_wave` → the NEXT wave drops its leading DIRTY
    // (R-dirty-before-data violation). A clean two-phase [DIRTY, DATA] proves recovery.
    let (slog, _us) = record(&s);
    slog.borrow_mut().clear();
    s.set(10);
    assert_eq!(kinds(&slog), vec!["DIRTY", "DATA"]); // leading DIRTY present
    assert_eq!(d.cache(), Some(11)); // d recomputes; the terminal `e` is ignored
}

/// C-13 — INVALIDATE arriving at a PAUSED compute node (R-paused-invalidate / D50).
/// An INVALIDATE supersedes that dep's buffered paused dep-wave (attributed
/// cancellation): N recomputes on RESUME only if a surviving dep still carries a
/// buffered change — never against an all-SENTINEL dep set (which would spuriously
/// panic→ERROR a non-guarding fn). A later DATA on the same dep re-arms the buffer.
#[test]
fn c13_invalidate_at_paused_node() {
    // (a) SOLE-DEP CANCEL — the bug: a non-guarding fn must NOT spuriously ERROR.
    {
        let d1 = Node::<i32>::state(0);
        let runs = Rc::new(Cell::new(0usize));
        let n = {
            let r = runs.clone();
            Node::<i32>::derived(vec![d1.erased()], move |ctx| {
                r.set(r.get() + 1);
                ctx.emit(*ctx.data::<i32>(0).unwrap() + 1); // NON-guarding unwrap
            })
        };
        let (log, _u) = record(&n);
        assert_eq!(n.cache(), Some(1));
        runs.set(0);

        let l = LockId::new("p");
        n.up(vec![Message::Pause(l.clone())]);
        d1.set(5); // buffered while paused (no recompute)
        assert_eq!(runs.get(), 0);
        d1.down(vec![Message::Invalidate]); // supersedes the buffered wave → cancels the paused recompute
        n.up(vec![Message::Resume(l)]); // RESUME must NOT recompute against the SENTINEL dep

        assert_eq!(
            runs.get(),
            0,
            "the superseded paused dep-wave does not recompute on RESUME (D50)"
        );
        assert_ne!(
            n.status(),
            Status::Errored,
            "no spurious terminal ERROR from recomputing against a SENTINEL dep (the D50 bug)"
        );
        assert!(!kinds(&log).contains(&"ERROR".to_string()));
    }

    // (b) RE-ARM — a DATA after the INVALIDATE re-arms the buffer ([DATA, INVALIDATE, DATA2]).
    {
        let d1 = Node::<i32>::state(0);
        let runs = Rc::new(Cell::new(0usize));
        let n = {
            let r = runs.clone();
            Node::<i32>::derived(vec![d1.erased()], move |ctx| {
                r.set(r.get() + 1);
                ctx.emit(*ctx.data::<i32>(0).unwrap() + 100);
            })
        };
        let (_log, _u) = record(&n);
        runs.set(0);

        let l = LockId::new("p");
        n.up(vec![Message::Pause(l.clone())]);
        d1.set(5); // buffered
        d1.down(vec![Message::Invalidate]); // supersede v1
        d1.set(7); // re-arm with v2
        n.up(vec![Message::Resume(l)]);

        assert_eq!(
            runs.get(),
            1,
            "RESUME recomputes once with the re-armed value"
        );
        assert_eq!(n.cache(), Some(107)); // 7 + 100 (v2, not the superseded v1)
    }

    // (c) MULTI-DEP SURVIVE — an INVALIDATE on D1 does NOT cancel D2's buffered update.
    {
        let d1 = Node::<i32>::state(0);
        let d2 = Node::<i32>::state(0);
        let runs = Rc::new(Cell::new(0usize));
        let n = {
            let r = runs.clone();
            Node::<i32>::derived(vec![d1.erased(), d2.erased()], move |ctx| {
                r.set(r.get() + 1);
                let a = ctx.data::<i32>(0).map(|v| *v).unwrap_or(0); // guards D1 SENTINEL
                let b = *ctx.data::<i32>(1).unwrap();
                ctx.emit(a + b);
            })
        };
        let (_log, _u) = record(&n);
        runs.set(0);

        let l = LockId::new("p");
        n.up(vec![Message::Pause(l.clone())]);
        d1.set(5); // buffered
        d2.set(9); // buffered (the survivor)
        d1.down(vec![Message::Invalidate]); // D1 superseded; D2's buffered wave survives
        n.up(vec![Message::Resume(l)]);

        assert_eq!(
            runs.get(),
            1,
            "RESUME still recomputes for the surviving dep (no lost update)"
        );
        assert_eq!(n.cache(), Some(9)); // D1=SENTINEL→0, D2=9
    }
}

/// C-14 — cleanup hooks are per-run (R-cleanup-hooks / D28 clarification). A fn that
/// registers onInvalidate + onDeactivation on EVERY run: the substrate clears both hook
/// lists before each run, so after K runs an INVALIDATE fires onInvalidate exactly once
/// (the latest run's) and deactivation fires onDeactivation exactly once — NOT K times.
#[test]
fn c14_cleanup_hooks_are_per_run() {
    let flush = Rc::new(Cell::new(0usize));
    let cleanup = Rc::new(Cell::new(0usize));
    let s = Node::<i32>::state(0);
    let d = {
        let f = flush.clone();
        let c = cleanup.clone();
        Node::<i32>::derived(vec![s.erased()], move |ctx| {
            let f2 = f.clone();
            ctx.on_invalidate(move || f2.set(f2.get() + 1));
            let c2 = c.clone();
            ctx.on_deactivation(move || c2.set(c2.get() + 1));
            ctx.emit(*ctx.data::<i32>(0).unwrap());
        })
    };
    let (_log, u) = record(&d); // run 1 (activation, s=0)
    s.set(1); // run 2 — re-registers (prior run's hooks cleared)
    s.set(2); // run 3 — re-registers (D has now run 3×)

    s.down(vec![Message::Invalidate]); // fires onInvalidate ONCE (run-3's), not 3×
    assert_eq!(
        flush.get(),
        1,
        "onInvalidate fires once after K runs, not K times (D28 per-run lifecycle)"
    );

    u(); // D loses its last subscriber → deactivates → fires onDeactivation ONCE (run-3's)
    assert_eq!(
        cleanup.get(),
        1,
        "onDeactivation fires once (the latest run's), not K times"
    );
}

/// C-2 — an async result arriving at a PAUSED async COMPUTE node (deps>0) buffers and
/// replays on final-lock RESUME (R-async-paused / R-pause-lockset / DR-3). The async
/// fn stashes `ctx.defer()`; the deferred emit fired while paused does NOT deliver
/// mid-pause — it enters the node-level pause buffer and replays as one wave on RESUME.
#[test]
fn c2_async_result_at_paused_node() {
    let stash: Rc<RefCell<Option<DeferredCtx>>> = Rc::new(RefCell::new(None));
    let trigger = Node::<i32>::state(0);
    let n = {
        let st = stash.clone();
        // async COMPUTE node (deps>0): the fn stashes its ctx and resolves later.
        Node::<i32>::derived_async(vec![trigger.erased()], move |ctx| {
            *st.borrow_mut() = Some(ctx.defer());
        })
    };
    let (log, _u) = record(&n);
    assert!(stash.borrow().is_some(), "fn ran on activation");
    assert_eq!(n.cache(), None); // deferred → no emit yet

    let l = LockId::new("pause");
    n.up(vec![Message::Pause(l.clone())]);
    log.borrow_mut().clear();

    // async result resolves WHILE paused → buffered, NOT delivered (DR-3).
    let deferred = stash.borrow_mut().take().expect("ctx stashed");
    deferred.emit(42i32);
    assert!(
        kinds(&log).is_empty(),
        "an async COMPUTE node's in-flight result buffers while paused (got {:?})",
        kinds(&log)
    );
    assert_eq!(n.cache(), None);

    n.up(vec![Message::Resume(l)]); // final-lock RESUME → replay the buffered settle slice
    assert_eq!(kinds(&log), vec!["DIRTY", "DATA"]);
    assert_eq!(n.cache(), Some(42));
}

/// C-4 — a mixed sync/async diamond joins EXACTLY ONCE after BOTH legs settle, even
/// though the async leg settles later via a stashed `DeferredCtx`. The first-run gate
/// holds the join until the async leg delivers; the async leg's no-emit run does NOT
/// synthesize an undirty RESOLVED (it deferred, not rejected — C-4 exemption). A
/// post-activation re-join emits DIRTY before DATA (glitch-free two-phase).
#[test]
fn c4_mixed_sync_async_diamond() {
    let stash: Rc<RefCell<Option<DeferredCtx>>> = Rc::new(RefCell::new(None));
    let runs = Rc::new(Cell::new(0usize));
    let a = Node::<i32>::state(1);
    let b: Node<i32> = Node::derived(vec![a.erased()], |ctx| {
        ctx.emit(*ctx.data::<i32>(0).unwrap() + 10) // sync leg
    });
    let c = {
        let st = stash.clone();
        Node::<i32>::derived_async(vec![a.erased()], move |ctx| {
            *st.borrow_mut() = Some(ctx.defer()); // async leg: defer the emit
        })
    };
    let d = {
        let r = runs.clone();
        Node::<i32>::derived(vec![b.erased(), c.erased()], move |ctx| {
            r.set(r.get() + 1);
            ctx.emit(*ctx.data::<i32>(0).unwrap() + *ctx.data::<i32>(1).unwrap());
        })
    };
    let (log, _u) = record(&d);
    // b settled sync (11); the async leg is deferred → the first-run gate holds d.
    assert_eq!(runs.get(), 0);
    assert_eq!(d.cache(), None);

    stash.borrow_mut().take().expect("ctx stashed").emit(21i32); // async leg → first join (activation)
    assert_eq!(runs.get(), 1, "joined exactly once");
    assert_eq!(d.cache(), Some(32)); // 11 + 21

    // Drive a second settle through both legs to observe d past its first-run exemption.
    log.borrow_mut().clear();
    a.set(2); // re-drives b (sync, 12) and c (async, deferred again)
    assert_eq!(runs.get(), 1, "still gated on the async leg");
    stash
        .borrow_mut()
        .take()
        .expect("ctx re-stashed")
        .emit(30i32); // async re-resolves
    assert_eq!(runs.get(), 2);
    assert_eq!(d.cache(), Some(42)); // 12 + 30
    assert_eq!(kinds(&log), vec!["DIRTY", "DATA"]); // DIRTY precedes DATA, glitch-free
}

/// C-9 — a `pausable:false` async LEAF source ignores PAUSE entirely (D44 outer gate):
/// its async production is delivered IMMEDIATELY under a held lock, never buffered
/// (resolves B20; contrast C-2's async COMPUTE node which buffers).
#[test]
fn c9_pausable_false_async_source_ignores_pause() {
    let stash: Rc<RefCell<Option<DeferredCtx>>> = Rc::new(RefCell::new(None));
    let s = {
        let st = stash.clone();
        // depless async LEAF source, pausable:false (timer/interval-class).
        Node::<i32>::producer_opts(
            NodeOpts {
                pool: PoolKind::Async,
                pausable: Pausable::False,
            },
            move |ctx| {
                *st.borrow_mut() = Some(ctx.defer());
            },
        )
    };
    let (log, _u) = record(&s);
    assert!(stash.borrow().is_some());

    let l = LockId::new("pause");
    s.up(vec![Message::Pause(l)]); // pausable:false → the lockset never gates production
    log.borrow_mut().clear();

    stash.borrow_mut().take().expect("ctx stashed").emit(42i32); // production WHILE "paused"
    assert_eq!(kinds(&log), vec!["DIRTY", "DATA"]); // delivered immediately, NOT buffered
    assert_eq!(s.cache(), Some(42));
}

/// C-10 — a `true`-mode (default) async LEAF source delivers its OWN production
/// immediately under PAUSE (D44 leaf carve-out, B20's twin): in `true` mode PAUSE gates
/// recomputation/propagation, not a depless source's own production. (A COMPUTE node in
/// the same spot WOULD buffer — C-2.)
#[test]
fn c10_true_mode_async_leaf_source_delivers_immediately() {
    let stash: Rc<RefCell<Option<DeferredCtx>>> = Rc::new(RefCell::new(None));
    let s = {
        let st = stash.clone();
        // depless async LEAF source, pausable:true default (fromPromise/fromAsyncIter-class).
        Node::<i32>::producer_async(move |ctx| {
            *st.borrow_mut() = Some(ctx.defer());
        })
    };
    let (log, _u) = record(&s);
    assert!(stash.borrow().is_some());

    let l = LockId::new("pause");
    s.up(vec![Message::Pause(l)]);
    log.borrow_mut().clear();

    stash.borrow_mut().take().expect("ctx stashed").emit(7i32); // leaf source's OWN production
    assert_eq!(kinds(&log), vec!["DIRTY", "DATA"]); // delivered immediately, NOT buffered
    assert_eq!(s.cache(), Some(7));
}

/// C-7 — upstream control reaching a depless source (R-up-at-source / D38). A derived D
/// over a depless source S originates `ctx.up`; the dep-bearing D forwards toward its dep,
/// and S (the terminus) is the single actor: INVALIDATE is HONORED (self-invalidate +
/// down-cascade), DIRTY/TEARDOWN are DROPPED (no coherent terminus action).
#[test]
fn c7_upstream_control_at_depless_source() {
    fn passthrough() -> (Node<i32>, Node<i32>, Log, Unsub) {
        let s = Node::<i32>::state(5);
        let d = Node::<i32>::derived(vec![s.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap())
        });
        let (log, u) = record(&d);
        log.borrow_mut().clear();
        (s, d, log, u)
    }

    // (a) INVALIDATE-up is HONORED: D forwards to the depless terminus S → S self-
    //     invalidates (cache → SENTINEL, fires onInvalidate) and cascades INVALIDATE down.
    {
        let (s, d, log, _u) = passthrough();
        d.up(vec![Message::Invalidate]);
        assert_eq!(s.cache(), None);
        assert_eq!(s.status(), Status::Sentinel);
        assert!(
            kinds(&log).contains(&"INVALIDATE".to_string()),
            "the honored invalidate cascades down to D's sink (got {:?})",
            kinds(&log)
        );
        assert_eq!(d.cache(), None);
    }

    // (b) DIRTY-up is DROPPED at the source (untouched, no down-cascade).
    {
        let (s, d, log, _u) = passthrough();
        d.up(vec![Message::Dirty]);
        assert_eq!(s.cache(), Some(5));
        assert_eq!(s.status(), Status::Settled);
        assert!(kinds(&log).is_empty());
    }

    // (c) TEARDOWN-up is DROPPED at the source (not terminated, no down-cascade).
    {
        let (s, d, log, _u) = passthrough();
        d.up(vec![Message::Teardown]);
        assert_eq!(s.status(), Status::Settled);
        assert!(kinds(&log).is_empty());
    }
}

/// C-8 — intra-graph runtime rewire (R-rewire / D42). Surgical Option-C: addDep
/// (push-on-subscribe for a cached dep) → removeDep (drain) → idempotent setDeps. The
/// cache is PRESERVED across every rewire, the first-run gate is never re-armed, and a
/// removed dep's edge is drained so it no longer drives the node.
#[test]
fn c8_intra_graph_runtime_rewire() {
    let a = Node::<i32>::state(1);
    let b = Node::<i32>::state(100); // B carries cached DATA
    let d: Node<i32> = Node::derived(vec![a.erased()], |ctx| {
        ctx.emit(*ctx.data::<i32>(0).unwrap())
    });
    let (_log, _u) = record(&d);
    assert_eq!(d.cache(), Some(1)); // (1) A settled → D ran (a-only)

    // (2) addDep(B): B cached → push-on-subscribe → D recomputes (sum). The first-run gate
    //     is NOT re-armed (D already passed it).
    d.add_dep(b.erased(), |ctx| {
        ctx.emit(*ctx.data::<i32>(0).unwrap() + *ctx.data::<i32>(1).unwrap())
    });
    assert_eq!(d.cache(), Some(101)); // 1 + 100

    b.set(50); // (3) B drives D
    assert_eq!(d.cache(), Some(51)); // 1 + 50

    // (4) removeDep(A) → deps = [B]; cache PRESERVED (A was not dirty → no recompute).
    d.remove_dep(a.erased(), |ctx| ctx.emit(*ctx.data::<i32>(0).unwrap()));
    assert_eq!(d.cache(), Some(51));

    a.set(9); // (5) A no longer drives D — its edge is drained
    assert_eq!(d.cache(), Some(51));

    b.set(7); // B is dep0 now (reroute) → drives D with the a-only fn
    assert_eq!(d.cache(), Some(7));

    // (6) setDeps to the current set → idempotent (no spurious recompute, no churn).
    let runs = Rc::new(Cell::new(0usize));
    let r = runs.clone();
    d.set_deps(vec![b.erased()], move |ctx| {
        r.set(r.get() + 1);
        ctx.emit(*ctx.data::<i32>(0).unwrap());
    });
    assert_eq!(runs.get(), 0); // no spurious recompute
    assert_eq!(d.cache(), Some(7));
}

/// C-8 (reject) — rewire on a terminal node is REJECTED (R-rewire / D42). Called
/// externally (no wave in flight), the reject propagates as a panic to the caller (the
/// Rust analogue of the TS throw; a mid-fn rewire instead becomes ERROR via the wave-owner
/// — covered by a node.rs unit test).
#[test]
#[should_panic(expected = "terminal")]
fn c8_rewire_on_terminal_node_rejected() {
    let a = Node::<i32>::state(1);
    let d: Node<i32> = Node::derived(vec![a.erased()], |ctx| {
        ctx.emit(*ctx.data::<i32>(0).unwrap())
    });
    let _u = d.subscribe(|_| {});
    d.down(vec![Message::Complete]); // D terminal
    assert_eq!(d.status(), Status::Completed);
    d.set_deps(vec![a.erased()], |ctx| {
        ctx.emit(*ctx.data::<i32>(0).unwrap())
    }); // → panic
}
