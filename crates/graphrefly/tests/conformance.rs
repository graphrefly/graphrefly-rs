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
//! Authority: `~/src/graphrefly/spec/conformance.jsonl` + `decisions/decisions.jsonl`.
//! (C-2/C-4/C-9/C-10 = async pool, C-7 = up-at-source, C-8 = rewire — later slices.)

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use graphrefly::{AnyValue, LockId, Message, Node, Status};

type Log = Rc<RefCell<Vec<String>>>;

/// Subscribe a message-kind recorder. Returns `(log, unsub)`; **keep the unsub alive**
/// — dropping it removes the last subscriber and deactivates the node (R-rom-ram).
fn record<T: 'static>(node: &Node<T>) -> (Log, Box<dyn FnOnce()>) {
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
