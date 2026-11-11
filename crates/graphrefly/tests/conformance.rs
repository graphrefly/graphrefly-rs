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

use std::cell::RefCell;
use std::rc::Rc;

use graphrefly::{Ctx, Node, NodeOpts};

type Log = Rc<RefCell<Vec<String>>>;
type Unsub = Box<dyn FnOnce()>;

/// Subscribe a message-kind recorder. Returns `(log, unsub)`; **keep the unsub alive**
/// — dropping it removes the last subscriber and deactivates the node (R-rom-ram).
fn record<T: 'static>(node: &Node<T>) -> (Log, Unsub) {
    let log: Log = Rc::new(RefCell::new(Vec::new()));
    let l = log.clone();
    let unsub = node.subscribe(move |msgs: &[graphrefly::Message<graphrefly::AnyValue>]| {
        let handle =
            |m: &graphrefly::Message<graphrefly::AnyValue>| l.borrow_mut().push(format!("{m:?}"));
        for m in msgs {
            handle(m);
        }
    });
    (log, unsub)
}

/// C-17 (R-deps-terminal / B42): an ABSORBED error (error_when_deps_error:false) counts as
/// TERMINAL for the complete_when_deps_complete auto-COMPLETE cascade — order-independent
/// (whichever terminal lands last fires it). The COMPLETION analogue of C-15's dirty-release;
/// NOT hit by the landed operators (which use complete_when_deps_complete:false). Mirrors TS a/b/c.
#[test]
fn c17_absorbed_error_counts_terminal_for_auto_complete() {
    let g = graphrefly::graph();
    fn fwd(ctx: &Ctx) {
        let v = ctx.dep(0).value::<i32>().map(|r| *r).unwrap_or(0);
        ctx.emit(v);
    }

    // (a) error-then-complete: C errors (absorbed), then B completes → D auto-COMPLETEs.
    {
        let b = g.state_empty::<i32>();
        let c = g.state_empty::<i32>();
        let d = g.node_opts::<i32, _>(
            vec![b.erased(), c.erased()],
            fwd,
            graphrefly::GraphNodeOpts::default().with_node(NodeOpts {
                error: graphrefly::Policy::Manual, // absorbs C's error, stays live
                ..NodeOpts::default()
            }),
        );
        let (_log, _u) = record(&d);
        c.error("boom"); // absorbed; B still live → not yet all-terminal
        assert_ne!(d.status().output, graphrefly::OutputStatus::Completed);
        assert_ne!(d.status().output, graphrefly::OutputStatus::Errored); // error_when_deps_error:false → no auto-ERROR
        b.set(1);
        b.complete(); // B terminal → ALL deps terminal (C errored) → COMPLETE
        assert_eq!(d.status().output, graphrefly::OutputStatus::Completed);
    }

    // (b) complete-then-error: B completes, then C errors LAST → still auto-COMPLETEs (ERROR-arm mirror).
    {
        let b = g.state_empty::<i32>();
        let c = g.state_empty::<i32>();
        let d = g.node_opts::<i32, _>(
            vec![b.erased(), c.erased()],
            fwd,
            graphrefly::GraphNodeOpts::default().with_node(NodeOpts {
                error: graphrefly::Policy::Manual,
                ..NodeOpts::default()
            }),
        );
        let (_log, _u) = record(&d);
        b.set(1);
        b.complete(); // B terminal; C still live → not yet all-terminal
        assert_ne!(d.status().output, graphrefly::OutputStatus::Completed);
        c.error("boom"); // C absorbed-error LANDS LAST → all terminal → COMPLETE
        assert_eq!(d.status().output, graphrefly::OutputStatus::Completed); // ERROR-absorbed arm mirrors the COMPLETE arm (B42)
    }

    // (c) default error_when_deps_error:true → auto-ERRORs on a dep error (absorbed path gated off).
    {
        let b = g.state_empty::<i32>();
        let c = g.state_empty::<i32>();
        let d = g.node_opts::<i32, _>(
            vec![b.erased(), c.erased()],
            fwd,
            graphrefly::GraphNodeOpts::default().with_node(NodeOpts::default()),
        );
        let (_log, _u) = record(&d);
        c.error("boom"); // auto-cascade → ERROR before any complete-check
        assert_eq!(d.status().output, graphrefly::OutputStatus::Errored);
    }
}
