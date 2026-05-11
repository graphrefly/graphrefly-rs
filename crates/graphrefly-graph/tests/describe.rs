//! `Graph::describe()` JSON-form output (canonical spec §3.6 + Appendix B).

mod common;

use common::binding;
use graphrefly_core::{EqualsMode, FnId, HandleId};
use graphrefly_graph::{DescribeValue, Graph, NodeStatus, NodeTypeStr};

#[test]
fn empty_graph_describes_as_empty() {
    let g = Graph::new("system", binding());
    let d = g.describe();
    assert_eq!(d.name, "system");
    assert!(d.nodes.is_empty());
    assert!(d.edges.is_empty());
    assert!(d.subgraphs.is_empty());
}

#[test]
fn state_node_status_sentinel() {
    let g = Graph::new("system", binding());
    g.state("knob", None).unwrap();
    let d = g.describe();
    let n = d.nodes.get("knob").unwrap();
    assert!(matches!(n.r#type, NodeTypeStr::State));
    assert_eq!(n.status, NodeStatus::Sentinel);
    assert!(n.value.is_none());
    assert!(n.deps.is_empty());
}

#[test]
fn state_node_status_settled_when_initial_present() {
    let g = Graph::new("system", binding());
    g.state("retry_limit", Some(HandleId::new(3))).unwrap();
    let d = g.describe();
    let n = d.nodes.get("retry_limit").unwrap();
    assert_eq!(n.status, NodeStatus::Settled);
    assert_eq!(n.value, Some(DescribeValue::Handle(HandleId::new(3))));
}

#[test]
fn derived_node_status_pending_before_first_fire() {
    let g = Graph::new("system", binding());
    let s = g.state("a", None).unwrap(); // sentinel — first-run gate not satisfied
    g.derived("d", &[s], FnId::new(1), EqualsMode::Identity)
        .unwrap();
    let d = g.describe();
    let nd = d.nodes.get("d").unwrap();
    assert!(matches!(nd.r#type, NodeTypeStr::Derived));
    assert_eq!(nd.status, NodeStatus::Pending);
}

#[test]
fn derived_node_status_settled_after_dep_fires() {
    let g = Graph::new("system", binding());
    let s = g.state("a", Some(HandleId::new(7))).unwrap();
    g.derived("d", &[s], FnId::new(1), EqualsMode::Identity)
        .unwrap();
    // No subscribers needed: register_derived activation runs activation
    // walk; cached state delivers DATA → fn fires (identity-passthrough
    // via stub binding) → has_fired_once = true.
    // Subscribe to trigger activation:
    let _sub = g.subscribe(g.node("d"), std::sync::Arc::new(|_msgs| {}));
    let d = g.describe();
    let nd = d.nodes.get("d").unwrap();
    assert_eq!(nd.status, NodeStatus::Settled);
    assert_eq!(nd.value, Some(DescribeValue::Handle(HandleId::new(7)))); // identity passthrough
    assert_eq!(nd.deps, vec!["a"]);
}

#[test]
fn complete_node_surfaces_completed_status() {
    let g = Graph::new("system", binding());
    let s = g.state("s", Some(HandleId::new(1))).unwrap();
    g.complete(s);
    let d = g.describe();
    assert_eq!(d.nodes.get("s").unwrap().status, NodeStatus::Completed);
}

#[test]
fn errored_node_surfaces_errored_status() {
    let g = Graph::new("system", binding());
    let s = g.state("s", Some(HandleId::new(1))).unwrap();
    g.error(s, HandleId::new(99));
    let d = g.describe();
    assert_eq!(d.nodes.get("s").unwrap().status, NodeStatus::Errored);
}

#[test]
fn edges_emitted_in_dep_order_per_consumer() {
    let g = Graph::new("system", binding());
    let a = g.state("a", Some(HandleId::new(1))).unwrap();
    let b = g.state("b", Some(HandleId::new(2))).unwrap();
    g.derived("c", &[a, b], FnId::new(1), EqualsMode::Identity)
        .unwrap();
    let d = g.describe();
    assert_eq!(d.edges.len(), 2);
    assert_eq!(d.edges[0].from, "a");
    assert_eq!(d.edges[0].to, "c");
    assert_eq!(d.edges[1].from, "b");
    assert_eq!(d.edges[1].to, "c");
}

#[test]
fn unnamed_dep_surfaces_as_anon_name() {
    let g = Graph::new("system", binding());
    // Register an anonymous Core node (no namespace entry).
    let raw = g.core().register_state(HandleId::new(5), false).unwrap();
    // Now name a derived that depends on the anonymous node + a named one.
    let named = g.state("named", Some(HandleId::new(7))).unwrap();
    g.derived("d", &[raw, named], FnId::new(1), EqualsMode::Identity)
        .unwrap();
    let d = g.describe();
    let nd = d.nodes.get("d").unwrap();
    assert_eq!(nd.deps[0], format!("_anon_{}", raw.raw()));
    assert_eq!(nd.deps[1], "named");
    // The anon dep also surfaces in edges.
    assert!(d
        .edges
        .iter()
        .any(|e| e.from == format!("_anon_{}", raw.raw()) && e.to == "d"));
}

#[test]
fn subgraphs_field_lists_mounted_children() {
    let parent = Graph::new("system", binding());
    parent.mount_new("payment").unwrap();
    parent.mount_new("auth").unwrap();
    let d = parent.describe();
    assert_eq!(d.subgraphs, vec!["payment", "auth"]);
    // Mounted children's nodes are NOT inlined; they're enumerated via
    // the child's own describe().
    assert!(d.nodes.is_empty());
}

#[test]
fn describe_serializes_to_json_string() {
    let g = Graph::new("system", binding());
    g.state("x", Some(HandleId::new(42))).unwrap();
    let d = g.describe();
    let json = serde_json::to_string(&d).unwrap();
    assert!(json.contains("\"name\":\"system\""));
    assert!(json.contains("\"x\""));
    assert!(json.contains("\"value\":42"));
    assert!(json.contains("\"type\":\"state\""));
}

#[test]
fn describe_meta_field_omitted_when_none() {
    // /qa B4: NodeDescribe.meta serialized via skip_serializing_if so
    // current outputs (always None in this slice) don't carry the key.
    let g = Graph::new("system", binding());
    g.state("x", Some(HandleId::new(1))).unwrap();
    let d = g.describe();
    let n = d.nodes.get("x").unwrap();
    assert!(n.meta.is_none());
    let json = serde_json::to_string(&d).unwrap();
    assert!(!json.contains("\"meta\""));
}

#[test]
fn describe_meta_field_round_trips_via_json() {
    // /qa B4: when the binding-side wrapper populates meta, it
    // round-trips through serde unchanged.
    let g = Graph::new("system", binding());
    g.state("x", Some(HandleId::new(1))).unwrap();
    let mut d = g.describe();
    d.nodes.get_mut("x").unwrap().meta = Some(serde_json::json!({ "description": "knob" }));
    let json = serde_json::to_string(&d).unwrap();
    assert!(json.contains("\"meta\":{\"description\":\"knob\"}"));
}

#[test]
fn describe_node_order_matches_namespace_insertion_order() {
    let g = Graph::new("system", binding());
    g.state("z", None).unwrap();
    g.state("a", None).unwrap();
    g.state("m", None).unwrap();
    let d = g.describe();
    let names: Vec<&str> = d.nodes.keys().map(String::as_str).collect();
    assert_eq!(names, vec!["z", "a", "m"]);
}

// ---------------------------------------------------------------------------
// F sub-slice (2026-05-10) — describe_with_debug + DebugBindingBoundary
// ---------------------------------------------------------------------------

mod debug_render {
    use std::collections::HashMap;
    use std::sync::Arc;

    use graphrefly_core::{BindingBoundary, FnResult, HandleId};
    use graphrefly_graph::{DebugBindingBoundary, DescribeValue, Graph};
    use parking_lot::Mutex;

    /// Minimal binding that renders handles via a HashMap<HandleId, T>.
    struct DebugBinding {
        values: Mutex<HashMap<HandleId, serde_json::Value>>,
    }

    impl DebugBinding {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                values: Mutex::new(HashMap::new()),
            })
        }

        fn register(&self, h: HandleId, v: serde_json::Value) {
            self.values.lock().insert(h, v);
        }
    }

    impl BindingBoundary for DebugBinding {
        fn invoke_fn(
            &self,
            _node_id: graphrefly_core::NodeId,
            _fn_id: graphrefly_core::FnId,
            dep_data: &[graphrefly_core::DepBatch],
        ) -> FnResult {
            let h = dep_data
                .first()
                .map_or(HandleId::new(0), graphrefly_core::DepBatch::latest);
            FnResult::Data {
                handle: h,
                tracked: None,
            }
        }
        fn custom_equals(&self, _f: graphrefly_core::FnId, a: HandleId, b: HandleId) -> bool {
            a == b
        }
        fn release_handle(&self, _h: HandleId) {}
    }

    impl DebugBindingBoundary for DebugBinding {
        fn handle_to_debug(&self, handle: HandleId) -> serde_json::Value {
            self.values
                .lock()
                .get(&handle)
                .cloned()
                .unwrap_or(serde_json::Value::Null)
        }
    }

    #[test]
    fn describe_with_debug_renders_value_via_binding() {
        // describe_with_debug surfaces `value: <T>` shapes (whatever
        // the binding's `handle_to_debug` returns) instead of raw
        // `value: <u64>`. Default `describe()` still surfaces raw
        // handle u64.
        let binding = DebugBinding::new();
        let h42 = HandleId::new(42);
        binding.register(h42, serde_json::json!(3));
        let g = Graph::new("system", binding.clone() as Arc<dyn BindingBoundary>);
        g.state("retry_limit", Some(h42)).unwrap();

        // Default describe: raw handle view.
        let default = g.describe();
        assert_eq!(
            default.nodes.get("retry_limit").unwrap().value,
            Some(DescribeValue::Handle(h42))
        );
        let default_json = serde_json::to_string(&default).unwrap();
        assert!(
            default_json.contains("\"value\":42"),
            "default describe() emits raw u64 handle. JSON: {default_json}"
        );

        // describe_with_debug: rendered view via DebugBindingBoundary.
        let rendered = g.describe_with_debug(binding.as_ref());
        assert_eq!(
            rendered.nodes.get("retry_limit").unwrap().value,
            Some(DescribeValue::Rendered(serde_json::json!(3)))
        );
        let rendered_json = serde_json::to_string(&rendered).unwrap();
        assert!(
            rendered_json.contains("\"value\":3"),
            "describe_with_debug emits binding-rendered value. JSON: {rendered_json}"
        );
    }

    #[test]
    fn describe_with_debug_preserves_null_for_sentinel_cache() {
        // Sentinel (no-initial state) → value: null in both modes.
        let binding = DebugBinding::new();
        let g = Graph::new("system", binding.clone() as Arc<dyn BindingBoundary>);
        g.state("knob", None).unwrap();

        let rendered = g.describe_with_debug(binding.as_ref());
        assert!(rendered.nodes.get("knob").unwrap().value.is_none());
        let json = serde_json::to_string(&rendered).unwrap();
        assert!(
            json.contains("\"value\":null"),
            "sentinel cache → value:null. JSON: {json}"
        );
    }

    #[test]
    fn describe_with_debug_supports_complex_json_shapes() {
        // Bindings can return arbitrary JSON shapes (objects, arrays,
        // strings, etc.) — the trait's return type is
        // `serde_json::Value`, no constraint.
        let binding = DebugBinding::new();
        let h1 = HandleId::new(1);
        binding.register(
            h1,
            serde_json::json!({ "kind": "tuple", "items": [1, 2, 3] }),
        );
        let g = Graph::new("system", binding.clone() as Arc<dyn BindingBoundary>);
        g.state("payload", Some(h1)).unwrap();

        let rendered = g.describe_with_debug(binding.as_ref());
        // Compare via the DescribeValue::Rendered branch directly —
        // string comparison would be brittle to serde_json's
        // alphabetic key ordering.
        let v = rendered.nodes.get("payload").unwrap().value.clone();
        assert_eq!(
            v,
            Some(DescribeValue::Rendered(
                serde_json::json!({ "kind": "tuple", "items": [1, 2, 3] })
            )),
            "complex shape round-trips through describe_with_debug"
        );
    }
}
