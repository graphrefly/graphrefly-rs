//! QA2: saved policy ownership and restore admission before recipe side effects.
use graphrefly::{checkpoint::*, graph, GraphNodeOpts, NodeOpts, Policy, RestoreFactoryMeta};
use serde_json::json;
use std::{cell::Cell, rc::Rc};

struct Recipe {
    calls: Rc<Cell<usize>>,
    custom: bool,
}
impl GraphRestoreDescriptor for Recipe {
    fn ref_(&self) -> &str {
        "qa2"
    }
    fn define(&self, ctx: RestoreDefineCtx<'_>) -> GraphRestoreResult<RestoreNodeDefinition> {
        self.calls.set(self.calls.get() + 1);
        // Deliberately wrong defaults: restore owns the policy tuple, including
        // custom constructors which do not use this returned opts structure.
        let opposite = |p| {
            if p == Policy::Auto {
                Policy::Manual
            } else {
                Policy::Auto
            }
        };
        let p = ctx.checkpoint.policies;
        let opts = GraphNodeOpts::named(ctx.id).with_node(NodeOpts {
            quiet: !p.quiet,
            complete: opposite(p.complete),
            error: opposite(p.error),
            invalidate: opposite(p.invalidate),
            ..NodeOpts::default()
        });
        let kind = if self.custom {
            let opts = opts.clone();
            let deps = ctx.deps.to_vec();
            RestoreNodeKind::Custom(Rc::new(move |g| {
                Ok(g.node_opts::<serde_json::Value, _>(
                    deps.iter()
                        .map(|id| g.find(id).unwrap().as_node::<serde_json::Value>().erased())
                        .collect(),
                    |_| {},
                    opts.clone(),
                )
                .erased())
            }))
        } else {
            RestoreNodeKind::NodeJson(Rc::new(|_| {}))
        };
        Ok(RestoreNodeDefinition {
            factory: "qa2".into(),
            kind,
            opts,
        })
    }
}
fn registry(calls: Rc<Cell<usize>>, custom: bool) -> GraphRestoreRegistry {
    restore_registry([
        GraphRestoreEntry::descriptor(StateRestoreDescriptor),
        GraphRestoreEntry::descriptor(Recipe { calls, custom }),
    ])
}
fn capture(policies: GraphCheckpointPolicies) -> GraphCheckpoint {
    let g = graph();
    let source = g.state_opts(json!(1), GraphNodeOpts::named("a"));
    let mut opts = GraphNodeOpts::named("n").with_node(NodeOpts {
        quiet: policies.quiet,
        complete: policies.complete,
        error: policies.error,
        invalidate: policies.invalidate,
        ..NodeOpts::default()
    });
    opts.restore = Some(RestoreFactoryMeta::registry_ref("qa2"));
    let n = g.node_opts::<serde_json::Value, _>(vec![source.erased()], |_| {}, opts);
    let _stop = n.subscribe(|_| {});
    g.checkpoint().unwrap()
}
fn manual() -> GraphCheckpointPolicies {
    GraphCheckpointPolicies {
        quiet: true,
        complete: Policy::Manual,
        error: Policy::Manual,
        invalidate: Policy::Manual,
    }
}

#[test]
fn saved_policies_override_normal_and_custom_factory_defaults() {
    for custom in [false, true] {
        for mask in 0..16 {
            let policy = |bit| {
                if mask & bit == 0 {
                    Policy::Auto
                } else {
                    Policy::Manual
                }
            };
            let policies = GraphCheckpointPolicies {
                quiet: mask & 1 != 0,
                complete: policy(2),
                error: policy(4),
                invalidate: policy(8),
            };
            let cp = capture(policies);
            assert_eq!(
                cp.nodes.iter().find(|n| n.id == "n").unwrap().policies,
                policies
            );
            let restored = restore_graph(
                cp,
                RestoreGraphOptions::new(registry(Rc::default(), custom)),
            )
            .unwrap();
            let n = restored.find("n").unwrap().as_node::<serde_json::Value>();
            let def = n.definition();
            assert_eq!(
                (def.quiet, def.complete, def.error, def.invalidate),
                (
                    policies.quiet,
                    policies.complete,
                    policies.error,
                    policies.invalidate
                )
            );
            let _stop = n.subscribe(|_| {});
            restored
                .find("a")
                .unwrap()
                .as_state::<serde_json::Value>()
                .unwrap()
                .error("boom");
            assert_eq!(
                n.status().output,
                if policies.error == Policy::Auto {
                    graphrefly::OutputStatus::Errored
                } else if policies.complete == Policy::Auto {
                    graphrefly::OutputStatus::Completed
                } else {
                    graphrefly::OutputStatus::Open
                }
            );
        }
    }
}

#[test]
fn policy_tuple_and_each_field_are_required_and_closed() {
    let saved = serde_json::to_value(capture(manual())).unwrap();
    for key in ["policies", "quiet", "complete", "error", "invalidate"] {
        let mut damaged = saved.clone();
        let node = damaged["nodes"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|n| n["id"] == "n")
            .unwrap();
        if key == "policies" {
            node.as_object_mut().unwrap().remove(key);
        } else {
            node["policies"].as_object_mut().unwrap().remove(key);
        }
        assert!(
            serde_json::from_value::<GraphCheckpoint>(damaged).is_err(),
            "missing {key}"
        );
    }
    for key in ["quiet", "complete", "error", "invalidate"] {
        let mut damaged = saved.clone();
        let node = damaged["nodes"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|n| n["id"] == "n")
            .unwrap();
        node["policies"][key] = json!("other");
        assert!(
            serde_json::from_value::<GraphCheckpoint>(damaged).is_err(),
            "invalid {key}"
        );
    }
}

#[test]
fn damaged_checkpoint_is_refused_before_any_recipe_side_effect() {
    let saved = serde_json::to_value(capture(manual())).unwrap();
    let mut cases: Vec<(String, serde_json::Value)> = Vec::new();
    for field in [
        "id",
        "kind",
        "factory",
        "deps",
        "values",
        "terminal",
        "privateState",
        "received",
        "policies",
    ] {
        for index in [0, 1] {
            let mut damaged = saved.clone();
            damaged["nodes"][index]
                .as_object_mut()
                .unwrap()
                .remove(field);
            cases.push((format!("missing {field} on node {index}"), damaged));
        }
    }
    for terminal in [
        json!({"kind":"DATA", "error":23}),
        json!({"kind":"ERROR"}),
        json!({"kind":"ERROR","error":true}),
        json!({"kind":"ERROR","error":false}),
        json!({"kind":"ERROR","error":"boom","extra":1}),
        json!({"kind":"COMPLETE","error":"boom"}),
        json!({"kind":"none","error":"boom"}),
        json!(["COMPLETE", 1]),
    ] {
        for received in [false, true] {
            let mut damaged = saved.clone();
            if received {
                damaged["nodes"][1]["received"][0]["terminal"] = terminal.clone();
            } else {
                damaged["nodes"][1]["terminal"] = terminal.clone();
            }
            cases.push((format!("terminal {terminal} received={received}"), damaged));
        }
    }
    for received in [
        json!([{}]),
        json!([{"values":[]}]),
        json!([{"terminal":{"kind":"none"}}]),
        json!([]),
    ] {
        let mut damaged = saved.clone();
        damaged["nodes"][1]["received"] = received;
        cases.push(("received".into(), damaged));
    }
    let mut duplicate = saved.clone();
    duplicate["nodes"]
        .as_array_mut()
        .unwrap()
        .push(saved["nodes"][1].clone());
    cases.push(("duplicate id".into(), duplicate));
    let mut declared_missing = saved.clone();
    declared_missing["nodes"][1]["deps"] = json!(["missing"]);
    cases.push(("missing declared dependency".into(), declared_missing));
    let mut missing = saved.clone();
    missing["nodes"][1]["lifetimeDeps"] = json!(["missing"]);
    cases.push(("missing lifetime dependency".into(), missing));
    let mut truncated = saved.clone();
    truncated["nodes"][1]["received"] = json!(null);
    truncated["nodes"][1]["values"] = json!([42]);
    cases.push(("lived computed with null received".into(), truncated));
    let mut cycle = saved.clone();
    cycle["nodes"][1]["deps"] = json!(["n"]);
    cases.push(("cycle".into(), cycle));
    let mut duplicate_lifetime = saved.clone();
    duplicate_lifetime["nodes"][1]["lifetimeDeps"] = json!(["a", "a"]);
    let input = saved["nodes"][1]["received"][0].clone();
    duplicate_lifetime["nodes"][1]["received"] = json!([input.clone(), input]);
    cases.push(("duplicate lifetime dependencies".into(), duplicate_lifetime));
    let mut lifetime_self = saved.clone();
    lifetime_self["nodes"][1]["lifetimeDeps"] = json!(["n"]);
    cases.push(("lifetime self-cycle".into(), lifetime_self));
    // Both declared and current graphs can be DAGs while their union is cyclic.
    let chain = graph();
    let a = chain.state_opts(json!(1), GraphNodeOpts::named("a"));
    let options = |id| {
        let mut opts = GraphNodeOpts::named(id);
        opts.restore = Some(RestoreFactoryMeta::registry_ref("qa2"));
        opts
    };
    let n = chain.node_opts::<serde_json::Value, _>(vec![a.erased()], |_| {}, options("n"));
    let m = chain.node_opts::<serde_json::Value, _>(vec![n.erased()], |_| {}, options("m"));
    let _keep = m.subscribe(|_| {});
    let mut union = serde_json::to_value(chain.checkpoint().unwrap()).unwrap();
    union["nodes"][1]["lifetimeDeps"] = json!(["m"]);
    // m's lifetime detaches from n, so each edge set alone remains acyclic.
    union["nodes"][2]["lifetimeDeps"] = json!(["a"]);
    cases.push(("declared/lifetime union cycle".into(), union));
    let mut invalid_kind = saved.clone();
    invalid_kind["nodes"][1]["kind"] = json!("other");
    cases.push(("invalid kind".into(), invalid_kind));
    let mut mounted = saved.clone();
    let mut child = saved.clone();
    child["nodes"][1]["received"] = json!([]);
    mounted["mounts"] = json!([{"at":"child", "checkpoint":child}]);
    cases.push((
        "invalid child admission before parent factory".into(),
        mounted,
    ));
    for (label, damaged) in cases {
        let calls = Rc::new(Cell::new(0));
        let rejected = match serde_json::from_value::<GraphCheckpoint>(damaged) {
            Err(_) => true,
            Ok(checkpoint) => restore_graph(
                checkpoint,
                RestoreGraphOptions::new(registry(calls.clone(), false)),
            )
            .is_err(),
        };
        assert!(rejected, "accepted {label}");
        assert_eq!(calls.get(), 0, "factory ran for {label}");
    }
}

#[test]
fn cold_and_state_records_are_explicit_and_actual_kind_ignores_metadata() {
    let g = graph();
    g.state_opts(json!(17), GraphNodeOpts::named("s"));
    let mut opts = GraphNodeOpts::named("cold");
    opts.restore = Some(RestoreFactoryMeta::registry_ref("qa2"));
    opts.meta.insert("kind".into(), "state".into());
    g.node_opts::<serde_json::Value, _>(vec![], |_| {}, opts);
    let cp = g.checkpoint().unwrap();
    let json = serde_json::to_value(&cp).unwrap();
    assert_eq!(json["nodes"][0]["kind"], "state");
    assert_eq!(json["nodes"][1]["kind"], "computed");
    assert!(json["nodes"][1]
        .as_object()
        .unwrap()
        .contains_key("received"));
    assert!(json["nodes"][1]["received"].is_null());
    assert!(json["nodes"][1]
        .as_object()
        .unwrap()
        .contains_key("privateState"));
    assert!(restore_graph(cp, RestoreGraphOptions::new(registry(Rc::default(), false))).is_ok());
}

#[test]
fn initial_duplicate_dependencies_round_trip_without_a_rewire_record() {
    let g = graph();
    let a = g.state_opts(json!(1), GraphNodeOpts::named("a"));
    let mut opts = GraphNodeOpts::named("n");
    opts.restore = Some(RestoreFactoryMeta::registry_ref("qa2"));
    let n = g.node_opts::<serde_json::Value, _>(vec![a.erased(), a.erased()], |_| {}, opts);
    let _keep = n.subscribe(|_| {});
    let cp = g.checkpoint().unwrap();
    assert_eq!(cp.nodes[1].deps, vec!["a", "a"]);
    assert_eq!(cp.nodes[1].received.as_ref().unwrap().len(), 2);
    assert!(cp.nodes[1].lifetime_deps.is_none());
    let calls = Rc::new(Cell::new(0));
    let restored =
        restore_graph(cp, RestoreGraphOptions::new(registry(calls.clone(), false))).unwrap();
    assert_eq!(calls.get(), 1);
    assert_eq!(
        restored
            .describe()
            .iter()
            .find(|n| n.id == "n")
            .unwrap()
            .deps,
        vec!["a", "a"]
    );
}

#[test]
fn checkpoint_has_no_edges_and_rejects_old_edges_at_every_graph_boundary() {
    let g = graph();
    let a = g.state_opts(json!(1), GraphNodeOpts::named("a"));
    let mut opts = GraphNodeOpts::named("n");
    opts.restore = Some(RestoreFactoryMeta::registry_ref("qa2"));
    let n = g.node_opts::<serde_json::Value, _>(vec![a.erased()], |_| {}, opts);
    let _keep = n.subscribe(|_| {});
    let child = graph();
    child.state_opts(json!(2), GraphNodeOpts::named("child-value"));
    graphrefly::topology::mount(&g, child, "child");
    let actual = serde_json::to_value(g.checkpoint().unwrap()).unwrap();
    assert!(actual.get("edges").is_none());
    assert!(actual["mounts"][0]["checkpoint"].get("edges").is_none());
    let decoded: GraphCheckpoint = serde_json::from_value(actual.clone()).unwrap();
    assert_eq!(decoded.version, "graphrefly-rs/checkpoint@1");
    let restored = restore_graph(
        decoded,
        RestoreGraphOptions::new(registry(Rc::default(), false)),
    )
    .unwrap();
    assert_eq!(
        restored
            .find("child::child-value")
            .unwrap()
            .as_node::<serde_json::Value>()
            .cache(),
        Some(json!(2))
    );
    for nested in [false, true] {
        for edges in [json!([]), json!([{"from":"a","to":"n"}])] {
            let mut old = actual.clone();
            let record = if nested {
                &mut old["mounts"][0]["checkpoint"]
            } else {
                &mut old
            };
            record["edges"] = edges;
            let error = serde_json::from_value::<GraphCheckpoint>(old).unwrap_err();
            assert!(
                error.to_string().contains("unknown field `edges`"),
                "{error}"
            );
        }
    }
}

#[test]
fn checkpoint_rejects_reserved_names_recursively_before_recipe_execution() {
    let base = capture(manual());
    for nested in [false, true] {
        for field in ["id", "name", "mount"] {
            let mut cp = base.clone();
            if field == "mount" {
                cp.mounts = Some(vec![GraphCheckpointMount {
                    at: "bad::mount".into(),
                    checkpoint: base.clone(),
                }]);
            } else if field == "id" {
                cp.nodes[0].id = "bad::node".into();
            } else {
                cp.nodes[0].name = Some("bad::name".into());
            }
            if nested {
                let mut parent = base.clone();
                parent.mounts = Some(vec![GraphCheckpointMount {
                    at: "child".into(),
                    checkpoint: cp,
                }]);
                cp = parent;
            }
            let calls = Rc::new(Cell::new(0));
            let error = restore_graph(cp, RestoreGraphOptions::new(registry(calls.clone(), false)))
                .err()
                .unwrap();
            assert_eq!(error.to_string(), "name must not contain '::'");
            assert_eq!(calls.get(), 0);
        }
    }
}
