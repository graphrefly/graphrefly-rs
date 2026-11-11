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
    graph, restore_graph, restore_registry, Ctx, GraphCheckpointJson, GraphNode, GraphNodeOpts,
    GraphRestoreEntry, Message, Node, NodeOpts, NodeVersion, RestoreFactoryMeta,
    RestoreGraphOptions, RestoreNodeDefinition, RestoreNodeKind, StateRestoreDescriptor,
};
use serde_json::json;

type Log = Rc<RefCell<Vec<String>>>;
/// An unsubscribe handle (kept alive to hold a node active).
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

fn record_graph_node_data_json(node: &GraphNode) -> (Rc<RefCell<Vec<GraphCheckpointJson>>>, Unsub) {
    let vals: Rc<RefCell<Vec<GraphCheckpointJson>>> = Rc::new(RefCell::new(Vec::new()));
    let v = vals.clone();
    let unsub = node.subscribe(move |msgs: &[graphrefly::Message<graphrefly::AnyValue>]| {
        let handle = |m: &graphrefly::Message<graphrefly::AnyValue>| {
            if let Message::Data(a) = m {
                if let Ok(rc) = a.clone().downcast::<GraphCheckpointJson>() {
                    v.borrow_mut().push(rc.as_ref().clone());
                }
            }
        };
        for m in msgs {
            handle(m);
        }
    });
    (vals, unsub)
}

fn graph_node_json_cache(node: &GraphNode) -> Option<GraphCheckpointJson> {
    node.cache_any()
        .and_then(|v| v.downcast::<GraphCheckpointJson>().ok())
        .map(|v| v.as_ref().clone())
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
                error: graphrefly::Policy::Manual, // absorbs C's error, stays live
                ..NodeOpts::default()
            },
            fwd,
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
        let b = Node::<i32>::state_empty();
        let c = Node::<i32>::state_empty();
        let d = Node::<i32>::derived_opts(
            vec![b.erased(), c.erased()],
            NodeOpts {
                error: graphrefly::Policy::Manual,
                ..NodeOpts::default()
            },
            fwd,
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
        let b = Node::<i32>::state_empty();
        let c = Node::<i32>::state_empty();
        let d = Node::<i32>::derived_opts(vec![b.erased(), c.erased()], NodeOpts::default(), fwd);
        let (_log, _u) = record(&d);
        c.error("boom"); // auto-cascade → ERROR before any complete-check
        assert_eq!(d.status().output, graphrefly::OutputStatus::Errored);
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
    restored_source
        .as_state::<serde_json::Value>()
        .expect("restored source is a state")
        .set(json!(4));
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
