//! R-naming: local identities cannot masquerade as mounted addresses.
use graphrefly::{graph, topology, GraphNodeOpts, TopologyGroupOptions};
use std::panic::{catch_unwind, AssertUnwindSafe};

const INVALID_NAME: &str = "name must not contain '::'";

fn rejected(f: impl FnOnce()) {
    let error = catch_unwind(AssertUnwindSafe(f)).expect_err("reserved separator must fail");
    let text = error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .unwrap();
    assert_eq!(text, INVALID_NAME);
}

#[test]
fn every_local_name_boundary_rejects_mount_separator_without_registration() {
    let g = graph();
    for name in ["a::b", "::", "module/child::value"] {
        rejected(|| {
            g.state_opts(1, GraphNodeOpts::named(name));
        });
        rejected(|| {
            g.state_empty_opts::<i32>(GraphNodeOpts::named(name));
        });
        rejected(|| {
            g.node_opts::<i32, _>(vec![], |_| {}, GraphNodeOpts::named(name));
        });
        rejected(|| {
            g.producer_opts::<i32, _>(|_| {}, GraphNodeOpts::named(name));
        });
        rejected(|| {
            g.derived_opts::<i32, _>(vec![], |_| Some(1), GraphNodeOpts::named(name));
        });
        rejected(|| {
            g.effect_opts(vec![], |_| None, GraphNodeOpts::named(name));
        });
        rejected(|| {
            g.init_node(graphrefly::of(1i32), vec![], GraphNodeOpts::named(name));
        });
        rejected(|| {
            topology::group_opts(&g, TopologyGroupOptions::named(name));
        });
        rejected(|| {
            topology::mount(&g, graph(), name);
        });
        assert!(g.describe().is_empty());
        assert!(topology::snapshot(&g)
            .subgraphs
            .unwrap_or_default()
            .is_empty());
    }
    let child = graph();
    child.state_opts(7, GraphNodeOpts::named("user-label.withDot/value"));
    topology::mount(&g, child, "orders");
    assert_eq!(
        g.find("orders::user-label.withDot/value")
            .unwrap()
            .as_node::<i32>()
            .cache(),
        Some(7)
    );
}

#[test]
fn boundary_colons_fail_but_internal_colon_addresses_round_trip() {
    for name in [":a", "a:", ":"] {
        let g = graph();
        for kind in ["node", "mount", "group"] {
            let failure = catch_unwind(AssertUnwindSafe(|| match kind {
                "node" => {
                    g.state_opts(1, GraphNodeOpts::named(name));
                }
                "mount" => topology::mount(&g, graph(), name),
                _ => {
                    topology::group_opts(&g, TopologyGroupOptions::named(name));
                }
            }))
            .unwrap_err();
            let text = failure
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| failure.downcast_ref::<&str>().copied())
                .unwrap();
            assert_eq!(text, "name must not start or end with ':'");
            assert!(g.describe().is_empty());
        }
    }
    let parent = graph();
    let child = graph();
    child.state_opts(7, GraphNodeOpts::named("a:b"));
    topology::mount(&parent, child, "c:d");
    assert_eq!(
        parent.find("c:d::a:b").unwrap().as_node::<i32>().cache(),
        Some(7)
    );
}
