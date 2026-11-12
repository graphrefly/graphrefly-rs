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

