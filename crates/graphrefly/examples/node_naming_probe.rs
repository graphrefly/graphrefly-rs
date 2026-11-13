//! R-naming: actual module inventories and corresponding roles, without dummy nodes.
use graphrefly::{
    graph, Graph, GraphNodeOpts, Node, ReactiveCascadingCacheOptions, WireBridgeOptions,
};
use serde_json::{json, Value};
use std::{collections::BTreeMap, path::PathBuf};

fn id<T: 'static>(g: &Graph, node: &Node<T>) -> String {
    g.describe()
        .into_iter()
        .find(|entry| g.find(&entry.id).unwrap().core().ptr_eq(&node.erased()))
        .unwrap()
        .id
}
fn other_dep(g: &Graph, node: &str, known: &str) -> String {
    g.describe()
        .into_iter()
        .find(|entry| entry.id == node)
        .unwrap()
        .deps
        .into_iter()
        .find(|dep| dep != known)
        .unwrap()
}
fn inventory(g: &Graph, exclude: &[&str], roles: BTreeMap<&str, String>) -> Value {
    let mut ids: Vec<_> = g
        .describe()
        .into_iter()
        .map(|entry| entry.id)
        .filter(|id| !exclude.contains(&id.as_str()))
        .collect();
    ids.sort();
    json!({"ids": ids, "roles": roles})
}

pub fn actual() -> Value {
    let mut result = serde_json::Map::new();
    {
        let g = graph();
        let b = graphrefly::wire_bridge::<String, String>(&g, WireBridgeOptions::new("s1"));
        let mut roles = BTreeMap::from([
            ("command", id(&g, &b.command)),
            ("outbound", id(&g, &b.outbound)),
            ("events", id(&g, &b.events)),
            ("acks", id(&g, &b.acks)),
            ("nacks", id(&g, &b.nacks)),
            ("status", id(&g, &b.status)),
            ("errors", id(&g, &b.errors)),
            ("cursor", id(&g, &b.cursor)),
            ("attempts", id(&g, &b.attempts)),
        ]);
        roles.insert(
            "inbound",
            other_dep(&g, &roles["events"], &roles["command"]),
        );
        roles.insert(
            "statusLedger",
            other_dep(&g, &roles["status"], &roles["events"]),
        );
        result.insert("wireBridge".into(), inventory(&g, &[], roles));
    }
    {
        let g = graph();
        let frames = g.state_empty_opts(GraphNodeOpts::named("frames"));
        let b = graphrefly::wire_edge_group_in(&g, "bridge", "group", &frames, &["x", "issues"]);
        result.insert(
            "edgeGroupIn".into(),
            inventory(
                &g,
                &["frames"],
                BTreeMap::from([
                    ("gate", id(&g, &b.gate)),
                    ("edgeX", id(&g, &b.inbound["x"])),
                    ("edgeIssues", id(&g, &b.inbound["issues"])),
                    ("issues", id(&g, &b.issues)),
                ]),
            ),
        );
    }
    {
        let g = graph();
        let request = g.state_empty_opts::<String>(GraphNodeOpts::named("request"));
        let b = graphrefly::reactive_cascading_cache::<String>(
            &g,
            ReactiveCascadingCacheOptions::new(request, vec![]),
        );
        let roles = BTreeMap::from([
            ("events", id(&g, &b.events)),
            ("status", id(&g, &b.status)),
            ("value", id(&g, &b.value)),
            ("ledger", other_dep(&g, &id(&g, &b.events), "request")),
        ]);
        result.insert("cascadingCache".into(), inventory(&g, &["request"], roles));
    }
    result.insert("ackDriver".into(), Value::Null);
    result.insert("workItemSeeds".into(), Value::Null);
    Value::Object(result)
}

pub fn verify() -> Value {
    let root = std::env::var_os("GRAPHREFLY_SPEC_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../graphrefly/spec")
        });
    let fixture: Value = serde_json::from_slice(
        &std::fs::read(root.join("fixtures/node-naming.v1.json"))
            .expect("shared naming fixture must exist"),
    )
    .unwrap();
    assert_eq!(fixture["schema"], "graphrefly/node-naming@1");
    for (name, key) in [("a::b", "error"), ("a:", "boundaryError")] {
        let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            graph().state_opts(1, GraphNodeOpts::named(name));
        }))
        .unwrap_err();
        let text = failure
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| failure.downcast_ref::<&str>().copied())
            .unwrap();
        assert_eq!(text, fixture[key].as_str().unwrap());
    }
    let actual = actual();
    let expected: serde_json::Map<_, _> = fixture["modules"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, value)| (key.clone(), value["runtimes"]["rust"].clone()))
        .collect();
    assert_eq!(actual, Value::Object(expected));
    actual
}
fn main() {
    println!("{}", verify());
}
