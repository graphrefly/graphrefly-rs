//! Pre-D797 behavioral regressions — the Rust arm of the scenarios as they stood
//! before graphrefly:D798 (2026-11-10) rewrote or retired C-1..C-27.
//!
//! These tests are NOT evidence for the current (D797) scenario texts; that evidence
//! is `tests/kernel_conformance.rs`. They are kept, phase by phase, while the
//! behavior they pin still holds, and each is rewritten or removed when
//! graphrefly-rs:D797-KERNEL-MIGRATION-RS changes it. Tests of the retired surfaces
//! (PAUSE/RESUME, PULL and routed up, TEARDOWN, the paused/undirty timing and the
//! committed-boundary self tasks) were removed in phase 1.
//!
//! Async is SIMULATED deterministically (matching the TS arm, no real timers): the
//! async fn stashes `ctx.defer()` into a shared cell, and the test fires the deferred
//! emit manually — the dispatcher invoke stays sync void (R-sync-core).
//!
//! Authority: `~/src/graphrefly/spec/conformance.jsonl` + `decisions/decisions.jsonl`.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use graphrefly::checkpoint::{GraphRestoreDescriptor, RestoreDefineCtx};
use graphrefly::{
    batch, graph, restore_graph, restore_registry, AnyValue, Core, Ctx, DeferredCtx, DepTerminal,
    GraphCheckpointJson, GraphNode, GraphNodeOpts, GraphRestoreEntry, Message, Node, NodeOpts,
    NodeVersion, RestoreFactoryMeta, RestoreGraphOptions, RestoreNodeDefinition, RestoreNodeKind,
    StateRestoreDescriptor, Status, WaveData,
};
use serde_json::json;

type Log = Rc<RefCell<Vec<String>>>;
type WaveShape = Vec<Vec<String>>;
type SeenWavePairs = Rc<RefCell<Vec<(WaveShape, WaveShape)>>>;
type SeenTerminal = Rc<RefCell<Vec<(WaveShape, String)>>>;
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

fn record_graph_node_data_json(node: &GraphNode) -> (Rc<RefCell<Vec<GraphCheckpointJson>>>, Unsub) {
    let vals: Rc<RefCell<Vec<GraphCheckpointJson>>> = Rc::new(RefCell::new(Vec::new()));
    let v = vals.clone();
    let unsub = node.subscribe(move |m: &Message<AnyValue>| {
        if let Message::Data(a) = m {
            if let Ok(rc) = a.clone().downcast::<GraphCheckpointJson>() {
                v.borrow_mut().push(rc.as_ref().clone());
            }
        }
    });
    (vals, unsub)
}

fn graph_node_json_cache(node: &GraphNode) -> Option<GraphCheckpointJson> {
    node.cache_any()
        .and_then(|v| v.downcast::<GraphCheckpointJson>().ok())
        .map(|v| v.as_ref().clone())
}

/// Count occurrences of a message kind in a recorded log.
fn count_kind(log: &Log, kind: &str) -> usize {
    log.borrow().iter().filter(|k| *k == kind).count()
}

fn dep_wave_data(ctx: &Ctx, dep: usize) -> &[Vec<WaveData>] {
    ctx.wave_data().get(dep).copied().unwrap_or(&[])
}

fn wave_shape(ctx: &Ctx, dep: usize) -> WaveShape {
    dep_wave_data(ctx, dep)
        .iter()
        .map(|wave| {
            wave.iter()
                .map(|item| match item {
                    WaveData::Sentinel => "SENTINEL".to_string(),
                    WaveData::Data(v) => {
                        if let Ok(n) = v.clone().downcast::<i32>() {
                            format!("i32:{n}")
                        } else if let Ok(n) = v.clone().downcast::<Option<i32>>() {
                            format!("option:{n:?}")
                        } else if let Ok(xs) = v.clone().downcast::<Vec<i32>>() {
                            format!("vec:{xs:?}")
                        } else {
                            "data".to_string()
                        }
                    }
                })
                .collect()
        })
        .collect()
}

fn terminal_shape(ctx: &Ctx, dep: usize) -> String {
    match ctx.terminal(dep) {
        None => "none".to_string(),
        Some(DepTerminal::Complete) => "complete".to_string(),
        Some(DepTerminal::Error(e)) => format!("error:{e}"),
    }
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
    // ── (steps 1-3) subscribe_dep deferred to the boundary; the added cached inner pushes
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
                for i in 1..ctx.dep_len() {
                    for v in ctx.batch::<i32>(i) {
                        ctx.emit(*v);
                    }
                }
                // on S DATA (dep 0): spawn + REQUEST add an inner (seed = S * 10).
                let source_batch = ctx.batch::<i32>(0);
                if let Some(last) = source_batch.last() {
                    let sv = **last;
                    let inner = make_inner(Some(sv * 10));
                    let core = inner.core();
                    let act = inner.activated.clone();
                    inners.borrow_mut().push(inner);
                    let sf = self_fn.clone();
                    ctx.rewire_next_subscribe_dep(core, move |c| sf(c));
                    // mid-run: the add is DEFERRED — the inner is NOT wired/activated yet.
                    deferred_ok.set(!act.get());
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

        s.set(1); // request subscribe_dep(innerA) → boundary drain wires it → innerA's seed forwarded
        assert!(
            deferred_ok.get(),
            "subscribe_dep was deferred (inner not activated mid-run)"
        );
        assert!(
            inners.borrow()[0].is_activated(),
            "boundary drain activated the inner"
        );
        assert!(kinds(&log).contains(&"PENDING".to_string())); // the boundary wave is two-phase
        assert_eq!(*vals.borrow(), vec![10]); // innerA's seed (1*10) forwarded as DATA

        // S alone re-drives the op fn after the rewire AND adds innerB — i.e. subscribe_dep did NOT re-arm
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
                for i in 1..ctx.dep_len() {
                    for v in ctx.batch::<i32>(i) {
                        ctx.emit(*v);
                    }
                    if matches!(ctx.terminal(i), Some(DepTerminal::Complete)) {
                        removals.push((i - 1, inners.borrow()[i - 1].core()));
                    }
                }
                let source_batch = ctx.batch::<i32>(0);
                if let Some(last) = source_batch.last() {
                    let sv = **last;
                    let inner = make_inner(Some(sv * 10));
                    let core = inner.core();
                    inners.borrow_mut().push(inner); // appends at the end → earlier indices stay valid
                    let sf = self_fn.clone();
                    ctx.rewire_next_subscribe_dep(core, move |c| sf(c));
                }
                // Splice the completed inners out of the tracking list in DESCENDING index order
                // (so each removal keeps the not-yet-removed lower indices valid) + request unsubscribe_dep,
                // keeping `inners` aligned with the op's deps[1..].
                for (idx, core) in removals.into_iter().rev() {
                    inners.borrow_mut().remove(idx);
                    let sf = self_fn.clone();
                    ctx.rewire_next_unsubscribe_dep(core, move |c| sf(c));
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

    // ── (steps 4-6, switch) replace_deps tears down the SUPERSEDED inner's source + forwards only
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
                for i in 1..ctx.dep_len() {
                    for v in ctx.batch::<i32>(i) {
                        ctx.emit(*v);
                    }
                }
                let source_batch = ctx.batch::<i32>(0);
                if let Some(last) = source_batch.last() {
                    let sv = **last;
                    let current = if sv == 1 {
                        a_core.clone()
                    } else {
                        b_core.clone()
                    };
                    let sf = self_fn.clone();
                    ctx.rewire_next_replace_deps(vec![s2.clone(), current], move |c| sf(c));
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
    //    ctx.rewire_next). The immediate subscribe_dep mid-run panics → wave-owner catch → ERROR. ──
    {
        let a = Node::<i32>::state(1);
        let x = Node::<i32>::state(9);
        let op_cell: Rc<RefCell<Option<Node<i32>>>> = Rc::new(RefCell::new(None));
        let cf = op_cell.clone();
        let xc = x.erased();
        let op = Node::<i32>::derived(vec![a.erased()], move |ctx| {
            let op = cf.borrow().clone().expect("op installed");
            // IMMEDIATE self-rewire mid-run (not ctx.rewire_next) → D37 reject panics.
            op.subscribe_dep(xc.clone(), |c| {
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

    // ── (variant) D62: terminal seals output but still drains queued topology. ──
    {
        let s = Node::<i32>::state_empty();
        let inner = make_inner(Some(1));
        let inner_core = inner.core();
        let inner_act = inner.activated.clone();
        let cell: Rc<RefCell<Option<OpFn>>> = Rc::new(RefCell::new(None));
        let body: OpFn = {
            let (inner_core, cell) = (inner_core.clone(), cell.clone());
            Rc::new(move |ctx: &Ctx| {
                if !ctx.batch::<i32>(0).is_empty() {
                    let sf = cell.borrow().clone().expect("op fn");
                    ctx.rewire_next_subscribe_dep(inner_core.clone(), move |c| sf(c)); // queued…
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
        let (log, _u) = record(&op);
        log.borrow_mut().clear();
        s.set(1);
        assert_eq!(op.status(), Status::Completed);
        assert!(
            inner_act.get(),
            "the queued subscribe_dep drained even though OP became terminal"
        );
        assert_eq!(kinds(&log), vec!["PENDING", "COMPLETE"]);
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
                    ctx.rewire_next_replace_deps(vec![a2.clone()], move |c| sf(c));
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
            "the idempotent replace_deps changes nothing → no loop"
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
    assert_eq!(kinds(&slog), vec!["PENDING", "DATA"]); // leading DIRTY present
    assert_eq!(d.cache(), Some(11)); // d recomputes; the terminal `e` is ignored
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
    assert_eq!(kinds(&log), vec!["PENDING", "DATA"]); // DIRTY precedes DATA, glitch-free
}

/// C-8 — intra-graph runtime rewire (R-rewire / D42). Surgical Option-C: subscribe_dep
/// (push-on-subscribe for a cached dep) → unsubscribe_dep (drain) → idempotent replace_deps. The
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

    // (2) subscribe_dep(B): B cached → push-on-subscribe → D recomputes (sum). The first-run gate
    //     is NOT re-armed (D already passed it).
    d.subscribe_dep(b.erased(), |ctx| {
        ctx.emit(*ctx.data::<i32>(0).unwrap() + *ctx.data::<i32>(1).unwrap())
    });
    assert_eq!(d.cache(), Some(101)); // 1 + 100

    b.set(50); // (3) B drives D
    assert_eq!(d.cache(), Some(51)); // 1 + 50

    // (4) unsubscribe_dep(A) → deps = [B]; cache PRESERVED (A was not dirty → no recompute).
    d.unsubscribe_dep(a.erased(), |ctx| ctx.emit(*ctx.data::<i32>(0).unwrap()));
    assert_eq!(d.cache(), Some(51));

    a.set(9); // (5) A no longer drives D — its edge is drained
    assert_eq!(d.cache(), Some(51));

    b.set(7); // B is dep0 now (reroute) → drives D with the a-only fn
    assert_eq!(d.cache(), Some(7));

    // (6) replace_deps to the current set → idempotent (no spurious recompute, no churn).
    let runs = Rc::new(Cell::new(0usize));
    let r = runs.clone();
    d.replace_deps(vec![b.erased()], move |ctx| {
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
    d.replace_deps(vec![a.erased()], |ctx| {
        ctx.emit(*ctx.data::<i32>(0).unwrap())
    }); // → panic
}

/// C-22 — an immediate/external rewire requested while this node has an uncommitted
/// batch settle is accepted but applied only after the batch commits the old shape.
#[test]
fn c22_batch_commit_precedes_rewire_requested_during_open_batch() {
    let a = Node::<i32>::state(1);
    let b = Node::<i32>::state(10);
    let n = Node::<i32>::derived(vec![a.erased()], |ctx| {
        ctx.emit(*ctx.data::<i32>(0).unwrap());
    });
    let (vals, _u) = record_data_i32(&n);
    let (log, _u2) = record(&n);
    assert_eq!(n.cache(), Some(1));
    vals.borrow_mut().clear();
    log.borrow_mut().clear();

    batch(|_| {
        n.down(vec![Message::Data(Rc::new(2i32))]); // N has an uncommitted old-shape DATA(2)
        n.replace_deps(vec![b.erased()], |ctx| {
            ctx.emit(*ctx.data::<i32>(0).unwrap() * 10);
        });

        assert_eq!(
            *vals.borrow(),
            Vec::<i32>::new(),
            "the old-shape DATA is not visible before batch commit"
        );
        assert_eq!(n.cache(), Some(1));
    });

    assert_eq!(
        *vals.borrow(),
        vec![2, 100],
        "commit N's old-shape batched value first, then apply rewire and settle fresh [B] shape"
    );
    assert_eq!(kinds(&log), vec!["PENDING", "DATA", "PENDING", "DATA"]);
    assert_eq!(n.cache(), Some(100));

    vals.borrow_mut().clear();
    a.set(3);
    assert_eq!(*vals.borrow(), Vec::<i32>::new(), "A was drained by rewire");
    b.set(11);
    assert_eq!(*vals.borrow(), vec![110], "B is the live post-batch dep");
}

/// C-17 (R-deps-terminal / B42): an ABSORBED error (error_when_deps_error:false) counts as
/// TERMINAL for the complete_when_deps_complete auto-COMPLETE cascade — order-independent
/// (whichever terminal lands last fires it). The COMPLETION analogue of C-15's dirty-release;
/// NOT hit by the landed operators (which use complete_when_deps_complete:false). Mirrors TS a/b/c.
#[test]
fn c17_absorbed_error_counts_terminal_for_auto_complete() {
    fn fwd(ctx: &Ctx) {
        let v = ctx.data::<i32>(0).map(|r| *r).unwrap_or(0);
        ctx.emit(v);
    }

    // (a) error-then-complete: C errors (absorbed), then B completes → D auto-COMPLETEs.
    {
        let b = Node::<i32>::state_empty();
        let c = Node::<i32>::state_empty();
        let d = Node::<i32>::derived_opts(
            vec![b.erased(), c.erased()],
            NodeOpts {
                error_when_deps_error: false, // absorbs C's error, stays live
                ..NodeOpts::default()
            },
            fwd,
        );
        let (_log, _u) = record(&d);
        c.down(vec![Message::Error("boom".into())]); // absorbed; B still live → not yet all-terminal
        assert_ne!(d.status(), Status::Completed);
        assert_ne!(d.status(), Status::Errored); // error_when_deps_error:false → no auto-ERROR
        b.set(1);
        b.down(vec![Message::Complete]); // B terminal → ALL deps terminal (C errored) → COMPLETE
        assert_eq!(d.status(), Status::Completed);
    }

    // (b) complete-then-error: B completes, then C errors LAST → still auto-COMPLETEs (ERROR-arm mirror).
    {
        let b = Node::<i32>::state_empty();
        let c = Node::<i32>::state_empty();
        let d = Node::<i32>::derived_opts(
            vec![b.erased(), c.erased()],
            NodeOpts {
                error_when_deps_error: false,
                ..NodeOpts::default()
            },
            fwd,
        );
        let (_log, _u) = record(&d);
        b.set(1);
        b.down(vec![Message::Complete]); // B terminal; C still live → not yet all-terminal
        assert_ne!(d.status(), Status::Completed);
        c.down(vec![Message::Error("boom".into())]); // C absorbed-error LANDS LAST → all terminal → COMPLETE
        assert_eq!(d.status(), Status::Completed); // ERROR-absorbed arm mirrors the COMPLETE arm (B42)
    }

    // (c) default error_when_deps_error:true → auto-ERRORs on a dep error (absorbed path gated off).
    {
        let b = Node::<i32>::state_empty();
        let c = Node::<i32>::state_empty();
        let d = Node::<i32>::derived_opts(vec![b.erased(), c.erased()], NodeOpts::default(), fwd);
        let (_log, _u) = record(&d);
        c.down(vec![Message::Error("boom".into())]); // auto-cascade → ERROR before any complete-check
        assert_eq!(d.status(), Status::Errored);
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
            vec!["START", "PENDING", "RESOLVED", "PENDING", "DATA", "PENDING", "RESOLVED"]
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

/// C-23 — D77 raw ctx contract: `wave_data` is the only raw dep-value input
/// surface, preserves per-upstream-wave distinctions, and terminal metadata stays
/// separate (R-fn-contract / R-ctx-wave-data / R-dynamic-node).
#[test]
fn c23_raw_ctx_wave_data_preserves_per_wave_distinctions() {
    // (a) no-wave for A while B changes.
    {
        let a = Node::<i32>::state(1);
        let b = Node::<i32>::state(10);
        let seen: SeenWavePairs = Rc::new(RefCell::new(Vec::new()));
        let out = {
            let seen = seen.clone();
            Node::<()>::derived_opts(
                vec![a.erased(), b.erased()],
                NodeOpts {
                    partial: true,
                    ..NodeOpts::default()
                },
                move |ctx| {
                    seen.borrow_mut()
                        .push((wave_shape(ctx, 0), wave_shape(ctx, 1)));
                },
            )
        };
        let (_log, _u) = record(&out);
        seen.borrow_mut().clear();
        b.set(11);
        assert_eq!(
            seen.borrow().last().cloned(),
            Some((vec![], vec![vec!["i32:11".to_string()]]))
        );
    }

    // (b) RESOLVED-only is one delivered wave with an empty inner projection. Only
    // the kernel sends RESOLVED (R-wave): `silent` sends nothing for its input, so the
    // kernel settles it with RESOLVED.
    {
        let s = Node::<i32>::state(1);
        let silent = Node::<i32>::derived(vec![s.erased()], |_ctx| {});
        let seen: Rc<RefCell<Vec<WaveShape>>> = Rc::new(RefCell::new(Vec::new()));
        let out = {
            let seen = seen.clone();
            Node::<()>::derived_opts(
                vec![silent.erased()],
                NodeOpts {
                    partial: true,
                    ..NodeOpts::default()
                },
                move |ctx| seen.borrow_mut().push(wave_shape(ctx, 0)),
            )
        };
        let (_log, _u) = record(&out);
        seen.borrow_mut().clear();
        s.set(2);
        assert_eq!(seen.borrow().last().cloned(), Some(vec![vec![]]));
    }

    // (c) DATA + INVALIDATE in the same upstream wave preserves DATA occurrences
    // and appends the Rust SENTINEL marker.
    {
        let s = Node::<i32>::state(0);
        let seen: Rc<RefCell<Vec<WaveShape>>> = Rc::new(RefCell::new(Vec::new()));
        let out = {
            let seen = seen.clone();
            Node::<()>::derived_opts(
                vec![s.erased()],
                NodeOpts {
                    partial: true,
                    ..NodeOpts::default()
                },
                move |ctx| seen.borrow_mut().push(wave_shape(ctx, 0)),
            )
        };
        let (_log, _u) = record(&out);
        seen.borrow_mut().clear();
        s.down(vec![
            Message::Data(Rc::new(1i32)),
            Message::Data(Rc::new(2i32)),
            Message::Invalidate,
        ]);
        assert_eq!(
            seen.borrow().last().cloned(),
            Some(vec![vec![
                "i32:1".to_string(),
                "i32:2".to_string(),
                "SENTINEL".to_string()
            ]])
        );
    }

    // (c2) INVALIDATE-only does not leave a stale sentinel for the next invocation.
    {
        let s = Node::<i32>::state(1);
        let seen: Rc<RefCell<Vec<WaveShape>>> = Rc::new(RefCell::new(Vec::new()));
        let out = {
            let seen = seen.clone();
            Node::<()>::derived_opts(
                vec![s.erased()],
                NodeOpts {
                    partial: true,
                    ..NodeOpts::default()
                },
                move |ctx| seen.borrow_mut().push(wave_shape(ctx, 0)),
            )
        };
        let (_log, _u) = record(&out);
        seen.borrow_mut().clear();
        s.down(vec![Message::Invalidate]);
        s.set(3);
        assert_eq!(
            seen.borrow().last().cloned(),
            Some(vec![vec!["i32:3".to_string()]])
        );
    }

    // (d/e) DATA payloads that are Rust "null-like" or empty containers remain
    // legal DATA, not SENTINEL/no-wave.
    {
        let opt = Node::<Option<i32>>::state_empty();
        let empty = Node::<Vec<i32>>::state_empty();
        let seen: SeenWavePairs = Rc::new(RefCell::new(Vec::new()));
        let out = {
            let seen = seen.clone();
            Node::<()>::derived_opts(
                vec![opt.erased(), empty.erased()],
                NodeOpts {
                    partial: true,
                    ..NodeOpts::default()
                },
                move |ctx| {
                    seen.borrow_mut()
                        .push((wave_shape(ctx, 0), wave_shape(ctx, 1)));
                },
            )
        };
        let (_log, _u) = record(&out);
        seen.borrow_mut().clear();
        opt.down(vec![Message::Data(Rc::new(None::<i32>))]);
        assert_eq!(
            seen.borrow().last().cloned().map(|pair| pair.0),
            Some(vec![vec!["option:None".to_string()]])
        );
        empty.down(vec![Message::Data(Rc::new(Vec::<i32>::new()))]);
        assert_eq!(
            seen.borrow().last().cloned().map(|pair| pair.1),
            Some(vec![vec!["vec:[]".to_string()]])
        );
    }

    // (f) COMPLETE/ERROR do not enter wave_data; terminal metadata is separate.
    {
        let complete = Node::<i32>::state(1);
        let seen: SeenTerminal = Rc::new(RefCell::new(Vec::new()));
        let out = {
            let seen = seen.clone();
            Node::<()>::derived_opts(
                vec![complete.erased()],
                NodeOpts {
                    error_when_deps_error: false,
                    terminal_as_real_input: true,
                    ..NodeOpts::default()
                },
                move |ctx| {
                    seen.borrow_mut()
                        .push((wave_shape(ctx, 0), terminal_shape(ctx, 0)));
                },
            )
        };
        let (_log, _u) = record(&out);
        seen.borrow_mut().clear();
        complete.down(vec![Message::Complete]);
        assert_eq!(
            seen.borrow().last().cloned(),
            Some((vec![], "complete".to_string()))
        );

        let live = Node::<i32>::state(10);
        let complete = Node::<i32>::state(1);
        let seen: SeenTerminal = Rc::new(RefCell::new(Vec::new()));
        let out = {
            let seen = seen.clone();
            Node::<()>::derived_opts(
                vec![complete.erased(), live.erased()],
                NodeOpts {
                    complete_when_deps_complete: false,
                    terminal_as_real_input: true,
                    ..NodeOpts::default()
                },
                move |ctx| {
                    seen.borrow_mut()
                        .push((wave_shape(ctx, 1), terminal_shape(ctx, 0)));
                },
            )
        };
        let (_log, _u) = record(&out);
        seen.borrow_mut().clear();
        complete.down(vec![Message::Complete]);
        assert_eq!(
            seen.borrow().last().cloned(),
            Some((vec![], "complete".to_string()))
        );
        seen.borrow_mut().clear();
        live.set(11);
        assert_eq!(
            seen.borrow().last().cloned(),
            Some((vec![vec!["i32:11".to_string()]], "none".to_string()))
        );

        let error = Node::<i32>::state(1);
        let seen: SeenTerminal = Rc::new(RefCell::new(Vec::new()));
        let out = {
            let seen = seen.clone();
            Node::<()>::derived_opts(
                vec![error.erased()],
                NodeOpts {
                    error_when_deps_error: false,
                    terminal_as_real_input: true,
                    ..NodeOpts::default()
                },
                move |ctx| {
                    seen.borrow_mut()
                        .push((wave_shape(ctx, 0), terminal_shape(ctx, 0)));
                },
            )
        };
        let (_log, _u) = record(&out);
        seen.borrow_mut().clear();
        error.down(vec![Message::Error("boom".into())]);
        assert_eq!(
            seen.borrow().last().cloned(),
            Some((vec![], "error:boom".to_string()))
        );
    }

    // (g) dynamic-node-style unread dep: the fn can inspect wave_data and stay
    // quiet on a B-only wave, producing only the substrate undirty settle.
    {
        let a = Node::<i32>::state(1);
        let b = Node::<i32>::state(10);
        let out = Node::<i32>::derived_opts(
            vec![a.erased(), b.erased()],
            NodeOpts {
                partial: true,
                ..NodeOpts::default()
            },
            move |ctx| {
                if dep_wave_data(ctx, 0).is_empty() {
                    return;
                }
                if let Some(v) = ctx.data::<i32>(0) {
                    ctx.emit(*v);
                }
            },
        );
        let (vals, _u) = record_data_i32(&out);
        vals.borrow_mut().clear();
        b.set(11);
        assert_eq!(*vals.borrow(), Vec::<i32>::new());
        a.set(2);
        assert_eq!(*vals.borrow(), vec![2]);
    }
}

struct C24StatefulMapDescriptor {
    runs: Rc<Cell<usize>>,
}

impl GraphRestoreDescriptor for C24StatefulMapDescriptor {
    fn ref_(&self) -> &str {
        "c24-stateful-map"
    }

    fn define(
        &self,
        ctx: RestoreDefineCtx<'_>,
    ) -> graphrefly::GraphRestoreResult<RestoreNodeDefinition> {
        assert_eq!(ctx.deps.len(), 1);
        let runs = self.runs.clone();
        Ok(RestoreNodeDefinition {
            factory: "c24-stateful-map".to_owned(),
            kind: RestoreNodeKind::NodeJson(Rc::new(move |ctx: &Ctx| {
                runs.set(runs.get() + 1);
                let prev = ctx
                    .state_get::<GraphCheckpointJson>()
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                let next = prev + 1;
                for value in ctx.batch::<GraphCheckpointJson>(0) {
                    let input = value.as_i64().expect("C-24 JSON input is a number");
                    ctx.emit(json!(input * 10 + next));
                }
                ctx.state_set(json!(next));
            })),
            opts: GraphNodeOpts {
                name: ctx.checkpoint.name.clone(),
                restore: Some(RestoreFactoryMeta::registry_ref("c24-stateful-map")),
                ..GraphNodeOpts::default()
            },
        })
    }
}

/// C-24 — snapshot/restore is state-preserving, not a fresh lifecycle. A restored
/// dep-bearing node's checkpointed cache is load-bearing for late subscribers; restored
/// dependency activation/push-on-subscribe must not recompute or overwrite it (D117).
/// Local-only and missing factories fail honestly (R-snapshot/R-restore).
#[test]
fn c24_snapshot_restore_preserves_state_and_rejects_local_only_factories() {
    let original_runs = Rc::new(Cell::new(0usize));
    let g = graph();
    let source = g.state_opts(json!(3), GraphNodeOpts::named("source"));
    let mut opts = GraphNodeOpts::named("memo");
    opts.restore = Some(RestoreFactoryMeta::registry_ref("c24-stateful-map"));
    let memo = {
        let runs = original_runs.clone();
        g.node_opts::<GraphCheckpointJson, _>(
            vec![source.erased()],
            move |ctx| {
                runs.set(runs.get() + 1);
                let prev = ctx
                    .state_get::<GraphCheckpointJson>()
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                let next = prev + 1;
                for value in ctx.batch::<GraphCheckpointJson>(0) {
                    let input = value.as_i64().expect("C-24 JSON input is a number");
                    ctx.emit(json!(input * 10 + next));
                }
                ctx.state_set(json!(next));
            },
            opts,
        )
    };
    let _keep_active = memo.subscribe(|_| {});
    assert_eq!(memo.cache(), Some(json!(31)));
    assert_eq!(original_runs.get(), 1);

    let checkpoint = g.checkpoint().expect("C-24 checkpoint succeeds");
    let restored_runs = Rc::new(Cell::new(0usize));
    let restored = restore_graph(
        checkpoint.clone(),
        RestoreGraphOptions::new(restore_registry([
            GraphRestoreEntry::descriptor(StateRestoreDescriptor),
            GraphRestoreEntry::descriptor(C24StatefulMapDescriptor {
                runs: restored_runs.clone(),
            }),
        ])),
    )
    .expect("C-24 restore succeeds");

    let restored_memo = restored.find("memo").expect("memo restored");
    assert_eq!(
        graph_node_json_cache(&restored_memo),
        Some(json!(31)),
        "restore keeps checkpointed cache"
    );
    assert_eq!(
        restored_runs.get(),
        0,
        "fresh restore commit must not run user fn"
    );

    let (seen, _late_sub) = record_graph_node_data_json(&restored_memo);
    assert_eq!(
        *seen.borrow(),
        vec![json!(31)],
        "late subscribe receives restored cache directly"
    );
    assert_eq!(
        restored_runs.get(),
        0,
        "restored dep activation/push-on-subscribe must not recompute"
    );

    let snap = restored.describe();
    assert!(snap
        .edges
        .iter()
        .any(|edge| edge.from == "source" && edge.to == "memo"));

    let restored_checkpoint = restored
        .checkpoint()
        .expect("restored graph can checkpoint again");
    let memo_checkpoint = restored_checkpoint
        .nodes
        .iter()
        .find(|node| node.id == "memo")
        .expect("memo checkpoint exists");
    assert_eq!(
        memo_checkpoint.ctx_state.value,
        graphrefly::GraphCheckpointValue::Data { data: json!(1) }
    );

    let restored_source = restored.find("source").expect("source restored");
    restored_source.down(vec![Message::Data(Rc::new(json!(4)) as AnyValue)]);
    assert_eq!(
        restored_runs.get(),
        1,
        "post-commit DATA is an ordinary wave"
    );
    assert_eq!(
        graph_node_json_cache(&restored_memo),
        Some(json!(42)),
        "post-commit wave updates from restored ctx.state"
    );

    let missing_err = match restore_graph(
        checkpoint,
        RestoreGraphOptions::new(restore_registry([GraphRestoreEntry::descriptor(
            StateRestoreDescriptor,
        )])),
    ) {
        Ok(_) => panic!("missing factory descriptor fails honestly"),
        Err(err) => err,
    };
    assert!(missing_err
        .to_string()
        .contains("missing registry descriptor"));

    let mut versioned_checkpoint = restored_checkpoint.clone();
    versioned_checkpoint
        .nodes
        .iter_mut()
        .find(|node| node.id == "memo")
        .expect("memo checkpoint exists")
        .version = Some(json!({ "level": 0, "counter": 1 }));
    let versioned_restored = restore_graph(
        versioned_checkpoint,
        RestoreGraphOptions::new(restore_registry([
            GraphRestoreEntry::descriptor(StateRestoreDescriptor),
            GraphRestoreEntry::descriptor(C24StatefulMapDescriptor {
                runs: Rc::new(Cell::new(0)),
            }),
        ])),
    )
    .expect("V0 runtime version metadata restores after B68");
    assert_eq!(
        versioned_restored
            .find("memo")
            .and_then(|node| node.version()),
        Some(NodeVersion::V0 { counter: 1 })
    );

    let local_graph = graph();
    let local_source = local_graph.state_opts(json!(1), GraphNodeOpts::named("source"));
    let local = local_graph.node_opts::<GraphCheckpointJson, _>(
        vec![local_source.erased()],
        |ctx| {
            for value in ctx.batch::<GraphCheckpointJson>(0) {
                ctx.emit(json!(value.as_i64().unwrap() + 1));
            }
        },
        GraphNodeOpts::named("local"),
    );
    let _keep_local_active = local.subscribe(|_| {});
    let local_err = match restore_graph(
        local_graph.checkpoint().expect("local checkpoint succeeds"),
        RestoreGraphOptions::new(restore_registry([GraphRestoreEntry::descriptor(
            StateRestoreDescriptor,
        )])),
    ) {
        Ok(_) => panic!("local-only factory restore fails honestly"),
        Err(err) => err,
    };
    assert!(local_err.to_string().contains("local-only"));
}
