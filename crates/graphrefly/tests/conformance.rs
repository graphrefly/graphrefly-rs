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
    AnyValue, Core, Ctx, DeferredCtx, DepTerminal, LockId, Message, Node, NodeOpts, Pausable,
    PoolKind, Status,
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

/// Subscribe a recorder that captures DATA **values** (downcast to `i32`), for the
/// occurrence-counting scenarios (C-12). Keep the unsub alive.
fn record_data_i32(node: &Node<i32>) -> (Rc<RefCell<Vec<i32>>>, Unsub) {
    let vals: Rc<RefCell<Vec<i32>>> = Rc::new(RefCell::new(Vec::new()));
    let v = vals.clone();
    let unsub = node.subscribe(move |m: &Message<AnyValue>| {
        if let Message::Data(a) = m {
            if let Ok(rc) = a.clone().downcast::<i32>() {
                v.borrow_mut().push(*rc);
            }
        }
    });
    (vals, unsub)
}

/// Count occurrences of a message kind in a recorded log.
fn count_kind(log: &Log, kind: &str) -> usize {
    log.borrow().iter().filter(|k| *k == kind).count()
}

/// The shared self-fn type for a self-referential operator (the rewire fn re-pairs the deps,
/// SD-1). `NodeFn` is a per-language impl detail (not exported); the test re-declares it.
type OpFn = Rc<dyn Fn(&Ctx)>;

/// A leaf-source inner (the *Map runtime-created node) whose activation + deactivation are
/// OBSERVABLE (cancellation visible). Emits its `seed` on activation; later emits/complete go
/// through a stashed [`DeferredCtx`] (an owned late-emit handle — `&Ctx` can't outlive invoke).
struct Inner {
    node: Node<i32>,
    dctx: Rc<RefCell<Option<DeferredCtx>>>,
    activated: Rc<Cell<bool>>,
    deactivated: Rc<Cell<bool>>,
}
impl Inner {
    fn emit(&self, v: i32) {
        self.dctx
            .borrow()
            .as_ref()
            .expect("inner activated")
            .emit(v);
    }
    fn is_activated(&self) -> bool {
        self.activated.get()
    }
    fn is_deactivated(&self) -> bool {
        self.deactivated.get()
    }
    fn core(&self) -> Core {
        self.node.erased()
    }
}

fn make_inner(seed: Option<i32>) -> Inner {
    let dctx: Rc<RefCell<Option<DeferredCtx>>> = Rc::new(RefCell::new(None));
    let activated = Rc::new(Cell::new(false));
    let deactivated = Rc::new(Cell::new(false));
    let (dc, act, deact) = (dctx.clone(), activated.clone(), deactivated.clone());
    let node = Node::<i32>::producer(move |ctx| {
        act.set(true);
        let d = deact.clone();
        ctx.on_deactivation(move || d.set(true));
        *dc.borrow_mut() = Some(ctx.defer());
        if let Some(s) = seed {
            ctx.emit(s);
        }
    });
    Inner {
        node,
        dctx,
        activated,
        deactivated,
    }
}

/// C-11 — a node fn's SELF-triggered dep-set mutation via `ctx.rewire_next` is DEFERRED to the
/// committed wave boundary and applied as a fresh wave (R-rewire-deferred / D47). Distinct from
/// C-8 (external/immediate rewire between waves) — C-11 is self-triggered/deferred-at-boundary,
/// the substrate prerequisite for the higher-order *Map operators. Mirrors the TS arm's facets.
/// The Rust arm has no *Map sugar yet, so the operators are substrate-expressed (self-ref fns).
#[test]
fn c11_higher_order_inner_rewire_at_wave_boundary() {
    // ── (steps 1-3) addDep deferred to the boundary; the added cached inner pushes
    //    [DIRTY,DATA]; the first-run gate is NOT re-armed (S alone re-drives the fn). ──
    {
        let s = Node::<i32>::state_empty();
        let inners: Rc<RefCell<Vec<Inner>>> = Rc::new(RefCell::new(Vec::new()));
        let op_runs = Rc::new(Cell::new(0usize));
        let deferred_ok = Rc::new(Cell::new(false));
        let cell: Rc<RefCell<Option<OpFn>>> = Rc::new(RefCell::new(None));
        let body: OpFn = {
            let (inners, op_runs, deferred_ok, cell) = (
                inners.clone(),
                op_runs.clone(),
                deferred_ok.clone(),
                cell.clone(),
            );
            Rc::new(move |ctx: &Ctx| {
                op_runs.set(op_runs.get() + 1);
                let self_fn = cell.borrow().clone().expect("op fn installed");
                // forward any inner DATA (dep i >= 1).
                for i in 1..ctx.dep_records().len() {
                    if let Some(b) = &ctx.dep_records()[i].batch {
                        for v in b {
                            if let Ok(rc) = v.clone().downcast::<i32>() {
                                ctx.emit(*rc);
                            }
                        }
                    }
                }
                // on S DATA (dep 0): spawn + REQUEST add an inner (seed = S * 10).
                if let Some(b) = &ctx.dep_records()[0].batch {
                    if let Some(last) = b.last() {
                        let sv = *last.clone().downcast::<i32>().unwrap();
                        let inner = make_inner(Some(sv * 10));
                        let core = inner.core();
                        let act = inner.activated.clone();
                        inners.borrow_mut().push(inner);
                        let sf = self_fn.clone();
                        ctx.rewire_next_add(core, move |c| sf(c));
                        // mid-run: the add is DEFERRED — the inner is NOT wired/activated yet.
                        deferred_ok.set(!act.get());
                    }
                }
            })
        };
        *cell.borrow_mut() = Some(body.clone());
        let cf = cell.clone();
        let op = Node::<i32>::derived_opts(
            vec![s.erased()],
            NodeOpts {
                complete_when_deps_complete: false,
                terminal_as_real_input: true,
                ..NodeOpts::default()
            },
            move |ctx| (cf.borrow().clone().expect("op fn"))(ctx),
        );
        let (log, _u) = record(&op);
        let (vals, _u2) = record_data_i32(&op);

        s.set(1); // request addDep(innerA) → boundary drain wires it → innerA's seed forwarded
        assert!(
            deferred_ok.get(),
            "addDep was deferred (inner not activated mid-run)"
        );
        assert!(
            inners.borrow()[0].is_activated(),
            "boundary drain activated the inner"
        );
        assert!(kinds(&log).contains(&"DIRTY".to_string())); // the boundary wave is two-phase
        assert_eq!(*vals.borrow(), vec![10]); // innerA's seed (1*10) forwarded as DATA

        // S alone re-drives the op fn after the rewire AND adds innerB — i.e. addDep did NOT re-arm
        // the gate to re-wait for all deps. (That gate property is the shared R-rewire path covered by
        // C-8; here we observe the operator stays live + re-drivable post-rewire.)
        op_runs.set(0);
        s.set(2);
        assert!(op_runs.get() > 0, "S re-drives the op fn after the rewire");
        assert_eq!(inners.borrow().len(), 2); // …and innerB was spawned (the rewire path is live)
    }

    // ── (step 7) an inner COMPLETE removes it (bounding); OP stays live while S is live. ──
    {
        let s = Node::<i32>::state_empty();
        let inners: Rc<RefCell<Vec<Inner>>> = Rc::new(RefCell::new(Vec::new()));
        let cell: Rc<RefCell<Option<OpFn>>> = Rc::new(RefCell::new(None));
        let body: OpFn = {
            let (inners, cell) = (inners.clone(), cell.clone());
            Rc::new(move |ctx: &Ctx| {
                let self_fn = cell.borrow().clone().expect("op fn installed");
                // Collect completed inners as (tracking-index, core) — do NOT mutate `inners`
                // mid-loop (a splice would shift later indices and desync the read for ≥2 inners).
                let mut removals: Vec<(usize, Core)> = Vec::new();
                for i in 1..ctx.dep_records().len() {
                    if let Some(b) = &ctx.dep_records()[i].batch {
                        for v in b {
                            if let Ok(rc) = v.clone().downcast::<i32>() {
                                ctx.emit(*rc);
                            }
                        }
                    }
                    if matches!(ctx.dep_records()[i].terminal, Some(DepTerminal::Complete)) {
                        removals.push((i - 1, inners.borrow()[i - 1].core()));
                    }
                }
                if let Some(b) = &ctx.dep_records()[0].batch {
                    if let Some(last) = b.last() {
                        let sv = *last.clone().downcast::<i32>().unwrap();
                        let inner = make_inner(Some(sv * 10));
                        let core = inner.core();
                        inners.borrow_mut().push(inner); // appends at the end → earlier indices stay valid
                        let sf = self_fn.clone();
                        ctx.rewire_next_add(core, move |c| sf(c));
                    }
                }
                // Splice the completed inners out of the tracking list in DESCENDING index order
                // (so each removal keeps the not-yet-removed lower indices valid) + request removeDep,
                // keeping `inners` aligned with the op's deps[1..].
                for (idx, core) in removals.into_iter().rev() {
                    inners.borrow_mut().remove(idx);
                    let sf = self_fn.clone();
                    ctx.rewire_next_remove(core, move |c| sf(c));
                }
            })
        };
        *cell.borrow_mut() = Some(body.clone());
        let cf = cell.clone();
        let op = Node::<i32>::derived_opts(
            vec![s.erased()],
            NodeOpts {
                complete_when_deps_complete: false,
                terminal_as_real_input: true,
                ..NodeOpts::default()
            },
            move |ctx| (cf.borrow().clone().expect("op fn"))(ctx),
        );
        let (log, _u) = record(&op);
        let (vals, _u2) = record_data_i32(&op);

        s.set(5); // add innerA (seed 50 forwarded at the boundary)
        assert!(vals.borrow().contains(&50));
        let inner_a = inners.borrow()[0].node.clone();
        let _ia_act = inners.borrow()[0].activated.clone();
        let ia_deact = inners.borrow()[0].deactivated.clone();

        inner_a.down(vec![Message::Complete]); // innerA COMPLETEs → OP removes it (bounding)
        assert!(
            ia_deact.get(),
            "the completed inner is removed → deactivated (input torn down)"
        );
        assert_ne!(op.status(), Status::Completed); // OP stays live (S live, cwdc:false)
        assert!(!kinds(&log).contains(&"COMPLETE".to_string()));
    }

    // ── (steps 4-6, switch) setDeps tears down the SUPERSEDED inner's source + forwards only
    //    the new one; the superseded inner is DRAINED (a stale emit does not survive). ──
    {
        let s = Node::<i32>::state_empty();
        let inner_a = make_inner(Some(10));
        let inner_b = make_inner(Some(20));
        let a_core = inner_a.core();
        let b_core = inner_b.core();
        let cell: Rc<RefCell<Option<OpFn>>> = Rc::new(RefCell::new(None));
        let body: OpFn = {
            let (s2, a_core, b_core, cell) =
                (s.erased(), a_core.clone(), b_core.clone(), cell.clone());
            Rc::new(move |ctx: &Ctx| {
                let self_fn = cell.borrow().clone().expect("op fn installed");
                for i in 1..ctx.dep_records().len() {
                    if let Some(b) = &ctx.dep_records()[i].batch {
                        for v in b {
                            if let Ok(rc) = v.clone().downcast::<i32>() {
                                ctx.emit(*rc);
                            }
                        }
                    }
                }
                if let Some(b) = &ctx.dep_records()[0].batch {
                    if let Some(last) = b.last() {
                        let sv = *last.clone().downcast::<i32>().unwrap();
                        let current = if sv == 1 {
                            a_core.clone()
                        } else {
                            b_core.clone()
                        };
                        let sf = self_fn.clone();
                        ctx.rewire_next_set(vec![s2.clone(), current], move |c| sf(c));
                    }
                }
            })
        };
        *cell.borrow_mut() = Some(body.clone());
        let cf = cell.clone();
        let op = Node::<i32>::derived_opts(
            vec![s.erased()],
            NodeOpts {
                complete_when_deps_complete: false,
                terminal_as_real_input: true,
                ..NodeOpts::default()
            },
            move |ctx| (cf.borrow().clone().expect("op fn"))(ctx),
        );
        let (_log, _u) = record(&op);
        let (vals, _u2) = record_data_i32(&op);

        s.set(1); // → innerA live (seed 10 forwarded)
        assert!(vals.borrow().contains(&10));
        assert!(inner_a.is_activated());
        vals.borrow_mut().clear();

        s.set(2); // switch → innerB; innerA's SOURCE is torn down (not merely masked)
        assert!(inner_b.is_activated());
        assert!(
            inner_a.is_deactivated(),
            "the superseded inner's source is deactivated"
        );
        assert_eq!(*vals.borrow(), vec![20]); // ONLY the current inner forwarded
        vals.borrow_mut().clear();

        inner_a.emit(999); // the superseded inner is DRAINED — no stale forward survives
        assert_eq!(*vals.borrow(), Vec::<i32>::new());
    }

    // ── (variant) an IMMEDIATE in-fn self-rewire is the D37 feedback cycle → ERROR (NOT
    //    ctx.rewire_next). The immediate add_dep mid-run panics → wave-owner catch → ERROR. ──
    {
        let a = Node::<i32>::state(1);
        let x = Node::<i32>::state(9);
        let op_cell: Rc<RefCell<Option<Node<i32>>>> = Rc::new(RefCell::new(None));
        let cf = op_cell.clone();
        let xc = x.erased();
        let op = Node::<i32>::derived(vec![a.erased()], move |ctx| {
            let op = cf.borrow().clone().expect("op installed");
            // IMMEDIATE self-rewire mid-run (not ctx.rewire_next) → D37 reject panics.
            op.add_dep(xc.clone(), |c| {
                c.emit(c.data::<i32>(0).map(|r| *r).unwrap_or(0))
            });
            ctx.emit(*ctx.data::<i32>(0).unwrap());
        });
        *op_cell.borrow_mut() = Some(op.clone());

        // The substrate panic must be CAUGHT by the wave-owner (D30), not escape subscribe.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _u = op.subscribe(|_| {});
        }));
        assert!(
            result.is_ok(),
            "the D37 reject is converted to ERROR, not escaped"
        );
        assert_eq!(op.status(), Status::Errored);
    }

    // ── (variant) a terminal OP discards its pending rewireNext queue. ──
    {
        let s = Node::<i32>::state_empty();
        let inner = make_inner(Some(1));
        let inner_core = inner.core();
        let inner_act = inner.activated.clone();
        let cell: Rc<RefCell<Option<OpFn>>> = Rc::new(RefCell::new(None));
        let body: OpFn = {
            let (inner_core, cell) = (inner_core.clone(), cell.clone());
            Rc::new(move |ctx: &Ctx| {
                if ctx.dep_records()[0].batch.is_some() {
                    let sf = cell.borrow().clone().expect("op fn");
                    ctx.rewire_next_add(inner_core.clone(), move |c| sf(c)); // queued…
                    ctx.down(vec![Message::Complete]); // …then OP goes terminal THIS wave
                }
            })
        };
        *cell.borrow_mut() = Some(body.clone());
        let cf = cell.clone();
        let op = Node::<i32>::derived_opts(
            vec![s.erased()],
            NodeOpts {
                complete_when_deps_complete: false,
                terminal_as_real_input: true,
                ..NodeOpts::default()
            },
            move |ctx| (cf.borrow().clone().expect("op fn"))(ctx),
        );
        let _u = op.subscribe(|_| {});
        s.set(1);
        assert_eq!(op.status(), Status::Completed);
        assert!(
            !inner_act.get(),
            "the queued addDep was discarded (terminal)"
        );
    }

    // ── (variant) a no-net-change rewireNext is a no-op (no drain loop). ──
    {
        let a = Node::<i32>::state(1);
        let runs = Rc::new(Cell::new(0usize));
        let cell: Rc<RefCell<Option<OpFn>>> = Rc::new(RefCell::new(None));
        let body: OpFn = {
            let (a2, runs, cell) = (a.erased(), runs.clone(), cell.clone());
            Rc::new(move |ctx: &Ctx| {
                runs.set(runs.get() + 1);
                if runs.get() < 5 {
                    let sf = cell.borrow().clone().expect("op fn");
                    // identical dep set every run → no net change → no fresh wave → no loop.
                    ctx.rewire_next_set(vec![a2.clone()], move |c| sf(c));
                }
                ctx.emit(*ctx.data::<i32>(0).unwrap());
            })
        };
        *cell.borrow_mut() = Some(body.clone());
        let cf = cell.clone();
        let op = Node::<i32>::derived(vec![a.erased()], move |ctx| {
            (cf.borrow().clone().expect("op fn"))(ctx)
        });
        let (_log, _u) = record(&op);
        assert_eq!(
            runs.get(),
            1,
            "the idempotent setDeps changes nothing → no loop"
        );
        assert_eq!(op.cache(), Some(1));
    }
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
                ..NodeOpts::default()
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

/// C-15 — a dep's TERMINAL (COMPLETE/ERROR) RELEASES its outstanding in-wave DIRTY
/// contribution (R-terminal-settles-dirty / B35) — the exactly-one-settle invariant — so a
/// DIRTY-then-terminal-without-DATA dep never strands the join node's `pending` and wedges
/// it. The terminal analogue of the INVALIDATE wedge-guard. Mirrors the TS arm a/b/c/d.
#[test]
fn c15_dep_terminal_settles_dirty() {
    fn sum2(ctx: &Ctx) {
        let b = ctx.data::<i32>(0).map(|r| *r).unwrap_or(0);
        let c = ctx.data::<i32>(1).map(|r| *r).unwrap_or(0);
        ctx.emit(b + c);
    }

    // (a) COMPLETE-mid-dirty: B dirties D; C delivers a value; B COMPLETEs with NO DATA →
    //     B's dirty is released, D joins EXACTLY once on the live leg (no wedge, no glitch).
    {
        let b = Node::<i32>::state(1);
        let c = Node::<i32>::state(10);
        let d = Node::<i32>::derived_opts(
            vec![b.erased(), c.erased()],
            NodeOpts {
                complete_when_deps_complete: false,
                ..NodeOpts::default()
            },
            sum2,
        );
        let (log, _u) = record(&d);
        assert_eq!(d.cache(), Some(11)); // first join (activation): 1 + 10
        log.borrow_mut().clear();

        b.down(vec![Message::Dirty]); // B signals a change → D dirty, pending=1
        assert_eq!(d.status(), Status::Dirty);
        c.set(20); // C delivers a real value, but D is gated on B's pending
        assert_eq!(d.cache(), Some(11)); // not yet (B still dirty)
        b.down(vec![Message::Complete]); // B COMPLETEs no-DATA → releases B's dirty → D joins on C

        assert_eq!(kinds(&log), vec!["DIRTY", "DATA"]); // joined exactly once, glitch-free, no wedge
        assert_eq!(d.cache(), Some(21)); // B's last value (1) + C's new value (20)
        assert_ne!(d.status(), Status::Completed); // C still live (completeWhenDepsComplete:false)
    }

    // (b) sole-dirty: B is the only dirty contributor; its COMPLETE drains `pending` → D
    //     un-dirties via a synthesized RESOLVED (no fabricated DATA), cache preserved.
    {
        let b = Node::<i32>::state(1);
        let c = Node::<i32>::state(10);
        let d = Node::<i32>::derived_opts(
            vec![b.erased(), c.erased()],
            NodeOpts {
                complete_when_deps_complete: false,
                ..NodeOpts::default()
            },
            sum2,
        );
        let (log, _u) = record(&d);
        assert_eq!(d.cache(), Some(11));
        log.borrow_mut().clear();

        b.down(vec![Message::Dirty]); // B the SOLE dirty contributor (C unchanged this wave)
        b.down(vec![Message::Complete]); // COMPLETEs with no value → no occurrence → undirty RESOLVED

        assert_eq!(kinds(&log), vec!["DIRTY", "RESOLVED"]); // un-dirtied, NOT a fabricated DATA
        assert_eq!(d.cache(), Some(11)); // cache preserved (a terminal, unlike INVALIDATE, keeps it)
        assert_eq!(d.status(), Status::Resolved); // undirty-RESOLVED convention, not "settled"
    }

    // (c) rescue: an absorbed ERROR releases the dirty + the fn READS the terminal
    //     (terminal_as_real_input, error_when_deps_error:false) — not propagated, no wedge.
    {
        let b = Node::<i32>::state(1);
        let c = Node::<i32>::state(10);
        let d = Node::<i32>::derived_opts(
            vec![b.erased(), c.erased()],
            NodeOpts {
                error_when_deps_error: false,
                terminal_as_real_input: true,
                ..NodeOpts::default()
            },
            |ctx| {
                // rescue B's value to 0 when it errored; else read its latest (R-deps-terminal).
                let bv = if matches!(ctx.dep_records()[0].terminal, Some(DepTerminal::Error(_))) {
                    0
                } else {
                    ctx.data::<i32>(0).map(|r| *r).unwrap_or(0)
                };
                let cv = ctx.data::<i32>(1).map(|r| *r).unwrap_or(0);
                ctx.emit(bv + cv);
            },
        );
        let (_log, _u) = record(&d);
        assert_eq!(d.cache(), Some(11));

        b.down(vec![Message::Dirty]); // B dirties D
        b.down(vec![Message::Error("boom".into())]); // rescued → fn reads the terminal

        assert_ne!(d.status(), Status::Errored); // NOT propagated — rescued
        assert_eq!(d.cache(), Some(10)); // B rescued to 0 + C(10); released dirty, no stranded pending
    }

    // (d) gate-holds: a dirtied dep completing-empty on a PRE-first-run multi-dep node
    //     un-dirties via RESOLVED, never wedges (the QA gate-holds corner — B has DATA but
    //     the first-run gate STILL holds since C never delivered + terminal_as_real_input:false).
    {
        let runs = Rc::new(Cell::new(0usize));
        let b = Node::<i32>::state_empty();
        let c = Node::<i32>::state_empty();
        let d = {
            let r = runs.clone();
            Node::<i32>::derived_opts(
                vec![b.erased(), c.erased()],
                NodeOpts {
                    complete_when_deps_complete: false,
                    ..NodeOpts::default()
                },
                move |ctx| {
                    r.set(r.get() + 1);
                    let bv = ctx.data::<i32>(0).map(|x| *x).unwrap_or(0);
                    let cv = ctx.data::<i32>(1).map(|x| *x).unwrap_or(0);
                    ctx.emit(bv + cv);
                },
            )
        };
        let (log, _u) = record(&d);
        assert_eq!(runs.get(), 0); // gate holds — neither dep delivered yet
        log.borrow_mut().clear();

        c.down(vec![Message::Dirty]); // C signals a change → D dirty, broadcasts DIRTY (pending=1)
        b.set(5); // B delivers a value, but the gate still needs C → D gated
        assert_eq!(runs.get(), 0);
        c.down(vec![Message::Complete]); // C COMPLETEs no-value → releases dirty; gate STILL holds

        assert_eq!(runs.get(), 0); // fn never ran (C never delivered, terminal_as_real_input:false)
        assert_eq!(kinds(&log), vec!["DIRTY", "RESOLVED"]); // un-dirtied downstream, NOT wedged
        assert_eq!(d.cache(), None); // never produced a value
    }
}

/// C-12 — every value-OCCURRENCE stays DATA (no equals-substitution); RESOLVED is the
/// substrate-synthesized UNDIRTY settle only; dedup is OPT-IN at the operator layer
/// (R-resolved-undirty / D49, supersedes D15/R-equals). The Rust arm has no operator
/// library yet, so take / distinctUntilChanged are expressed as inline `ctx.state` derived
/// nodes (per-language, D6/D24) — the substrate property under test is language-neutral.
#[test]
fn c12_occurrences_stay_data_resolved_is_undirty_only() {
    // (a) a repeated value is THREE distinct occurrences — never collapsed to RESOLVED.
    {
        let src = Node::<i32>::producer(|ctx| {
            ctx.emit(1);
            ctx.emit(1);
            ctx.emit(1);
            ctx.down(vec![Message::Complete]);
        });
        let (log, _u) = record(&src);
        assert_eq!(
            count_kind(&log, "DATA"),
            3,
            "three occurrences, all DATA (got {:?})",
            kinds(&log)
        );
        assert_eq!(
            count_kind(&log, "RESOLVED"),
            0,
            "no equals-substitution (got {:?})",
            kinds(&log)
        );
    }

    // (b) a take(3)-style derived (ctx.state counter) counts OCCURRENCES, not distinct values:
    //     fromIter([1,1,1,1]) → take(3) → [1,1,1].
    {
        let src = Node::<i32>::producer(|ctx| {
            for _ in 0..4 {
                ctx.emit(1);
            }
            ctx.down(vec![Message::Complete]);
        });
        let taken = Node::<i32>::derived(vec![src.erased()], |ctx| {
            let n = ctx.state_get::<i32>().map(|r| *r).unwrap_or(0);
            if n < 3 {
                let v = *ctx.data::<i32>(0).expect("a value occurred");
                ctx.state_set(n + 1);
                ctx.emit(v);
                if n + 1 == 3 {
                    ctx.down(vec![Message::Complete]);
                }
            }
        });
        let (vals, _u) = record_data_i32(&taken);
        assert_eq!(*vals.borrow(), vec![1, 1, 1]); // counts occurrences, NOT collapsed to one
    }

    // (c) a filter that REJECTS (fn returns without emitting) makes the substrate SYNTHESIZE
    //     one undirty RESOLVED per rejected wave — no DATA, no wedge; an accepted value is DATA.
    {
        let src = Node::<i32>::state_empty();
        let filtered = Node::<i32>::derived(vec![src.erased()], |ctx| {
            let v = *ctx.data::<i32>(0).unwrap();
            if v % 2 == 0 {
                ctx.emit(v); // accept evens; reject odds (no emit → undirty RESOLVED)
            }
        });
        let (log, _u) = record(&filtered);
        src.set(3); // odd → reject → synthesized undirty RESOLVED (no wedge)
        src.set(4); // even → DATA(4) (an occurrence)
        src.set(5); // odd → RESOLVED
        assert_eq!(
            kinds(&log),
            vec!["START", "DIRTY", "RESOLVED", "DIRTY", "DATA", "DIRTY", "RESOLVED"]
        );
    }

    // (d) dedup is OPT-IN at the operator layer: a distinctUntilChanged-style derived (its
    //     own eq via ctx.state) suppresses an unchanged value → [1,1,2] emits [1,2]; the
    //     suppressed repeat is an undirty RESOLVED (the substrate did NOT dedup it for us).
    {
        let src = Node::<i32>::state_empty();
        let distinct = Node::<i32>::derived(vec![src.erased()], |ctx| {
            let v = *ctx.data::<i32>(0).unwrap();
            let last = ctx.state_get::<i32>().map(|r| *r);
            if last != Some(v) {
                ctx.state_set(v);
                ctx.emit(v);
            } // else unchanged → no emit → substrate synthesizes the undirty RESOLVED
        });
        let (vals, _u) = record_data_i32(&distinct);
        let (log, _u2) = record(&distinct);
        src.set(1); // new → DATA(1)
        src.set(1); // same → deduped by the OPERATOR → RESOLVED, not a dropped/collapsed DATA
        src.set(2); // new → DATA(2)
        assert_eq!(*vals.borrow(), vec![1, 2]); // dedup is opt-in (operator), not substrate
        assert_eq!(
            count_kind(&log, "RESOLVED"),
            1,
            "the deduped repeat is an undirty RESOLVED"
        );
        assert_eq!(count_kind(&log, "DATA"), 2);
    }
}
