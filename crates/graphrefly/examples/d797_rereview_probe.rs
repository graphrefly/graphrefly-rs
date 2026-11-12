//! Shared D808 trace. Run with GRAPHREFLY_SPEC_ROOT pointing to the authority spec.
use graphrefly::{
    graph, AnyValue, DepTerminal, GraphNodeOpts, GraphOptions, Message, NodeOpts, Policy,
    ReplaceOpts, RestoreFactoryMeta, StateWrite,
};
use serde_json::{json, Value};
use std::{
    cell::{Cell, RefCell},
    panic::{catch_unwind, AssertUnwindSafe},
    path::PathBuf,
    rc::Rc,
};

type Values = Rc<RefCell<Vec<i32>>>;
type Stop = Rc<RefCell<Option<Box<dyn FnOnce()>>>>;
fn values(msgs: &[Message<AnyValue>]) -> Vec<i32> {
    msgs.iter()
        .filter_map(|m| match m {
            Message::Data(v) => v.downcast_ref::<i32>().copied(),
            _ => None,
        })
        .collect()
}
fn main() {
    let mut result = serde_json::Map::new();
    {
        let g = graph();
        let source = g.state(0i32);
        let quiet = g.node_opts::<i32, _>(
            vec![source.erased()],
            |ctx| {
                if ctx.dep(0).terminal().is_some() {
                    ctx.emit(99i32);
                }
            },
            GraphNodeOpts::named("q").with_node(NodeOpts {
                quiet: true,
                ..NodeOpts::default()
            }),
        );
        let seen = Rc::new(RefCell::new(Vec::<Value>::new()));
        let downstream=g.node_opts::<i32,_>(vec![quiet.erased()], { let seen=seen.clone(); move |ctx| {
            let waves:Vec<Vec<i32>>=ctx.dep(0).waves::<i32>().into_iter().map(|a| a.into_iter().map(|v| *v).collect()).collect();
            seen.borrow_mut().push(json!({"waves":waves,"terminal":match ctx.dep(0).terminal() {Some(DepTerminal::Complete)=>Some("COMPLETE"),_=>None}}));
        }}, GraphNodeOpts::named("d").with_node(NodeOpts { complete:Policy::Manual,..NodeOpts::default() }));
        let _stop = downstream.subscribe(|_| {});
        seen.borrow_mut().clear();
        source.complete();
        let seen = seen.borrow();
        let mut observation = seen[0].clone();
        observation["runs"] = json!(seen.len());
        result.insert("quietTerminal".into(), observation);
    }
    {
        let g = graph();
        let s = g.state(0i32);
        let n = g.node_opts::<i32, _>(
            vec![s.erased()],
            |ctx| {
                if ctx.dep(0).value::<i32>().is_some_and(|v| *v == 1) {
                    ctx.emit(1i32);
                    ctx.emit(2i32);
                }
            },
            GraphNodeOpts::named("n"),
        );
        let observed: Values = Rc::default();
        let subscribed: Values = Rc::default();
        let stop: Stop = Rc::default();
        let unsub: Stop = Rc::default();
        *stop.borrow_mut() = Some(g.observe(&["n"], {
            let (observed, stop) = (observed.clone(), stop.clone());
            move |e| {
                let v = values(e.msgs);
                if !v.is_empty() {
                    observed.borrow_mut().extend(v);
                    if let Some(stop) = stop.borrow_mut().take() {
                        stop();
                    }
                }
            }
        }));
        *unsub.borrow_mut() = Some(n.subscribe({
            let (subscribed, unsub) = (subscribed.clone(), unsub.clone());
            move |msgs| {
                let v = values(msgs);
                if !v.is_empty() {
                    subscribed.borrow_mut().extend(v);
                    if let Some(stop) = unsub.borrow_mut().take() {
                        stop();
                    }
                }
            }
        }));
        s.set(1);
        result.insert("observeStop".into(), json!(*observed.borrow()));
        result.insert("subscribeStop".into(), json!(*subscribed.borrow()));
    }
    {
        let g = graph();
        let slot = Rc::new(RefCell::new(None));
        let n = g.producer::<i32, _>({
            let slot = slot.clone();
            move |ctx| {
                *slot.borrow_mut() = Some(ctx.defer());
            }
        });
        let arrays = Rc::new(Cell::new(0));
        let _stop = n.subscribe({
            let arrays = arrays.clone();
            move |_| arrays.set(arrays.get() + 1)
        });
        arrays.set(0);
        slot.borrow().as_ref().unwrap().down(vec![]);
        result.insert("emptyDown".into(), json!({"arrays":arrays.get()}));
        let state = g.state(0i32);
        let emitted: Values = Rc::default();
        let _sink = n.subscribe({
            let emitted = emitted.clone();
            move |msgs| emitted.borrow_mut().extend(values(msgs))
        });
        let failed = catch_unwind(AssertUnwindSafe(|| {
            g.batch(|| -> Result<(), ()> {
                state.set(StateWrite::from_fn(|_: Option<&i32>| {
                    panic!("failed updater")
                }));
                slot.borrow()
                    .as_ref()
                    .unwrap()
                    .down(vec![Message::Data(Rc::new(42i32))]);
                Ok(())
            })
            .unwrap();
        }));
        assert!(failed.is_err());
        result.insert("lateAfterCommitFailure".into(), json!(*emitted.borrow()));
    }
    {
        let g = graph();
        let ledger = g.state(0i32);
        let n = g.node_opts::<i32, _>(
            vec![],
            |_| {},
            GraphNodeOpts::named("n").with_node(NodeOpts::default().with_setup({
                let ledger = ledger.clone();
                move |ctx| {
                    ctx.state_set(42i32);
                    let later = ctx.defer();
                    let ledger = ledger.clone();
                    Some(Box::new(move |_| {
                        ledger.set(*later.state_get::<i32>().unwrap());
                    }))
                }
            })),
        );
        let _stop = n.subscribe(|_| {});
        let written: Values = Rc::default();
        let _sink = ledger.subscribe({
            let written = written.clone();
            move |msgs| written.borrow_mut().extend(values(msgs))
        });
        written.borrow_mut().clear();
        n.replace(vec![], |_| {}, ReplaceOpts::default());
        result.insert("replaceCleanup".into(), json!(*written.borrow()));
    }
    {
        let reports = Rc::new(Cell::new(0));
        let g = graphrefly::graph_opts(GraphOptions {
            report: Some(Rc::new({
                let reports = reports.clone();
                move |_| reports.set(reports.get() + 1)
            })),
            ..GraphOptions::default()
        });
        let trigger = g.state(0i32);
        let a = g.state(0i32);
        let b = g.state(0i32);
        let runs = Rc::new(Cell::new(0));
        let _stop = trigger.subscribe({
            let (g, a, b, runs) = (g.clone(), a.clone(), b.clone(), runs.clone());
            move |msgs| {
                if !values(msgs).contains(&1) {
                    return;
                }
                a.complete();
                g.batch(|| -> Result<(), ()> {
                    for state in [&b, &a] {
                        let runs = runs.clone();
                        state.set(StateWrite::from_fn(move |_: Option<&i32>| {
                            runs.set(runs.get() + 1);
                            Some(1)
                        }));
                    }
                    Ok(())
                })
                .unwrap();
            }
        });
        trigger.set(1);
        result.insert(
            "endedBatch".into(),
            json!({"updaterRuns":runs.get(),"otherValue":b.cache(),"reports":reports.get()}),
        );
    }
    {
        let g = graph();
        let mut opts = GraphNodeOpts::named("n");
        opts.restore = Some(RestoreFactoryMeta {
            ref_: "absent".into(),
            config: None,
        });
        let _n = g.node_opts::<i32, _>(vec![], |_| {}, opts);
        let restored = graphrefly::restore_graph(
            g.checkpoint().unwrap(),
            graphrefly::RestoreGraphOptions::new(graphrefly::restore_registry([])),
        );
        result.insert(
            "checkpointMissingRecipe".into(),
            json!(restored.err().unwrap().to_string()),
        );
    }
    let mut quiet_errors = Vec::new();
    for invalidate in [Policy::Auto, Policy::Manual] {
        for retained in [true, false] {
            let g = graph();
            let a = g.state(1i32);
            let b = g.state(1i32);
            let q = g.node_opts::<i32, _>(
                vec![a.erased()],
                move |ctx| {
                    if retained {
                        if let Some(value) = ctx.dep(0).value::<i32>() {
                            ctx.emit(*value);
                        }
                    }
                },
                GraphNodeOpts::named("q").with_node(NodeOpts {
                    quiet: true,
                    invalidate,
                    ..NodeOpts::default()
                }),
            );
            let d = g.node::<i32, _>(vec![q.erased(), b.erased()], |ctx| {
                if let (Some(a), Some(b)) = (ctx.dep(0).value::<i32>(), ctx.dep(1).value::<i32>()) {
                    ctx.emit(*a + *b);
                }
            });
            let data: Values = Rc::default();
            let events = Rc::new(RefCell::new(Vec::new()));
            let _stop = d.subscribe({
                let (data, events) = (data.clone(), events.clone());
                move |msgs| {
                    data.borrow_mut().extend(values(msgs));
                    events.borrow_mut().extend(
                        msgs.iter()
                            .filter(|m| !matches!(m, Message::Pending | Message::Data(_)))
                            .map(|m| format!("{m:?}")),
                    );
                }
            });
            data.borrow_mut().clear();
            events.borrow_mut().clear();
            g.batch(|| -> Result<(), ()> {
                a.invalidate();
                a.error("boom");
                b.set(2);
                Ok(())
            })
            .unwrap();
            quiet_errors.push(json!({"invalidate": if invalidate == Policy::Auto {"auto"} else {"manual"}, "retained":retained, "values":*data.borrow(), "events":*events.borrow()}));
        }
    }
    result.insert("quietError".into(), json!(quiet_errors));
    let root = std::env::var_os("GRAPHREFLY_SPEC_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../graphrefly/spec")
        });
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(root.join("fixtures/d797-rereview.v1.json"))
            .expect("shared rereview fixture"),
    )
    .unwrap();
    let result = Value::Object(result);
    assert_eq!(result, fixture["probeExpected"]);
    let reasons = checkpoint_reasons();
    let expected: serde_json::Map<String, Value> = fixture["checkpointReasons"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, reason)| {
            (
                key.clone(),
                json!(format!("not restorable: n: {}", reason.as_str().unwrap())),
            )
        })
        .collect();
    assert_eq!(reasons, Value::Object(expected));
    let output = if std::env::args().any(|a| a == "--reasons") {
        reasons
    } else {
        result
    };
    println!("{}", serde_json::to_string_pretty(&output).unwrap());
}

fn checkpoint_reasons() -> Value {
    use graphrefly::{
        default_restore_registry, restore_graph, restore_registry, GraphCheckpointFactory,
        GraphCheckpointInput, GraphCheckpointTerminal, RestoreGraphOptions,
    };
    let g = graph();
    let n = g.init_node(
        graphrefly::never::<i32>(),
        vec![],
        GraphNodeOpts::named("n"),
    );
    let _stop = n.subscribe(|_| {});
    let mut saved = g.checkpoint().unwrap();
    saved.nodes[0].values = vec![json!(1)];
    saved.nodes[0].received = Some(vec![]);
    let mut reasons = serde_json::Map::new();
    for reason in [
        "missingRecipe",
        "retainedValues",
        "receivedValues",
        "privateState",
        "recipeConfig",
        "errorPayload",
        "localOnly",
        "missingReceived",
    ] {
        let mut cp = saved.clone();
        let node = &mut cp.nodes[0];
        // Outside the package's strict JSON-number range, while representable by serde_json.
        let invalid = json!(u64::MAX);
        match reason {
            "retainedValues" => node.values = vec![invalid],
            "receivedValues" => {
                node.received = Some(vec![GraphCheckpointInput {
                    values: vec![invalid],
                    terminal: GraphCheckpointTerminal::None,
                }])
            }
            "privateState" => node.private_state = Some(invalid),
            "recipeConfig" => {
                node.factory = GraphCheckpointFactory::RegistryRef {
                    ref_: "never".into(),
                    config: Some(invalid),
                }
            }
            "errorPayload" => node.terminal = GraphCheckpointTerminal::Error { error: invalid },
            "localOnly" => {
                node.factory = GraphCheckpointFactory::LocalOnly {
                    name: "n".into(),
                    reason: "built without recipe".into(),
                }
            }
            "missingReceived" => node.received = None,
            _ => {}
        }
        let registry = if reason == "missingRecipe" {
            restore_registry([])
        } else {
            default_restore_registry()
        };
        let error = restore_graph(cp, RestoreGraphOptions::new(registry))
            .err()
            .expect("refusal");
        reasons.insert(reason.into(), json!(error.to_string()));
    }
    Value::Object(reasons)
}
