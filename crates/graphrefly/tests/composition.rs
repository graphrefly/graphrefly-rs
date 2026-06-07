use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use graphrefly::{
    filter, graph, map, pipe, stratify, stratify_branch, DescribeEdge, GraphNodeOpts, Message,
    Node, StratifyOptions, StratifyRule,
};

fn collect_data<T: Clone + 'static>(node: &Node<T>) -> Rc<RefCell<Vec<T>>> {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let seen_sink = seen.clone();
    let _keep = node.subscribe(move |msg| {
        if let Message::Data(value) = msg {
            if let Some(v) = value.as_ref().downcast_ref::<T>() {
                seen_sink.borrow_mut().push(v.clone());
            }
        }
    });
    seen
}

#[test]
fn pipe_registers_each_operator_through_graph_init_funnel() {
    let g = graph();
    let source = g.state_opts(1i32, GraphNodeOpts::named("source"));
    let out = pipe(&g, source.clone())
        .through(map::<i32, i32>(|n| n + 1))
        .through(filter::<i32>(|n| *n % 2 == 0))
        .through(map::<i32, String>(|n| format!("v{n}")))
        .done();
    let seen = collect_data(&out);

    source.set(2);
    source.set(3);

    assert_eq!(*seen.borrow(), vec!["v2".to_owned(), "v4".to_owned()]);
    let snap = g.describe();
    assert!(snap
        .nodes
        .iter()
        .any(|node| node.id == "map#0" && node.factory == "map"));
    assert!(snap
        .nodes
        .iter()
        .any(|node| node.id == "filter#1" && node.factory == "filter"));
    assert!(snap
        .nodes
        .iter()
        .any(|node| node.id == "map#2" && node.factory == "map"));
    assert!(snap.edges.contains(&DescribeEdge {
        from: "source".to_owned(),
        to: "map#0".to_owned(),
    }));
    assert!(snap.edges.contains(&DescribeEdge {
        from: "map#0".to_owned(),
        to: "filter#1".to_owned(),
    }));
    assert!(snap.edges.contains(&DescribeEdge {
        from: "filter#1".to_owned(),
        to: "map#2".to_owned(),
    }));
}

#[test]
fn pipe_zero_steps_returns_the_original_source_node() {
    let g = graph();
    let source = g.state_opts(1i32, GraphNodeOpts::named("source"));
    let out = pipe(&g, source.clone()).done();
    let seen = collect_data(&out);

    source.set(2);

    assert_eq!(*seen.borrow(), vec![1, 2]);
    assert_eq!(g.describe().nodes.len(), 1);
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ModRule {
    modulo: i32,
}

#[test]
fn stratify_branch_routes_source_values_through_declared_deps() {
    let g = graph();
    let source = g.state_opts(0i32, GraphNodeOpts::named("source"));
    let rules = g.state_opts(ModRule { modulo: 0 }, GraphNodeOpts::named("rules"));
    let branch = stratify_branch(
        &g,
        &source,
        &rules,
        |rule: &ModRule, value: &i32| value % 2 == rule.modulo,
        GraphNodeOpts::named("branch/evenish"),
    );
    let seen = collect_data(&branch);

    source.set(1);
    source.set(2);
    rules.set(ModRule { modulo: 1 });
    source.set(3);

    assert_eq!(*seen.borrow(), vec![0, 2, 3]);
    let snap = g.describe();
    assert!(snap.edges.contains(&DescribeEdge {
        from: "source".to_owned(),
        to: "branch/evenish".to_owned(),
    }));
    assert!(snap.edges.contains(&DescribeEdge {
        from: "rules".to_owned(),
        to: "branch/evenish".to_owned(),
    }));
    let branch_node = snap
        .nodes
        .iter()
        .find(|node| node.id == "branch/evenish")
        .unwrap();
    assert_eq!(
        branch_node.deps,
        vec!["source".to_owned(), "rules".to_owned()]
    );
}

#[test]
fn stratify_branch_drops_source_values_while_rules_are_sentinel_then_rechecks_latest() {
    let g = graph();
    let source = g.state_empty_opts::<i32>(GraphNodeOpts::named("source"));
    let rules = g.state_empty_opts::<bool>(GraphNodeOpts::named("rules"));
    let branch = stratify_branch(
        &g,
        &source,
        &rules,
        |pass, _value| *pass,
        GraphNodeOpts::named("branch"),
    );
    let seen = collect_data(&branch);

    source.down(vec![
        Message::Data(Rc::new(1i32)),
        Message::Data(Rc::new(2i32)),
    ]);
    rules.set(true);
    source.set(3);

    assert_eq!(*seen.borrow(), vec![2, 3]);
}

#[test]
fn stratify_builds_static_named_branches_with_metadata() {
    let g = graph();
    let source = g.state_opts(0i32, GraphNodeOpts::named("source"));
    let mut branch_opts = BTreeMap::new();
    let mut odd_opts = GraphNodeOpts::default();
    odd_opts
        .meta
        .insert("priority".to_owned(), "high".to_owned());
    branch_opts.insert("odd".to_owned(), odd_opts);
    let stratified = stratify(
        &g,
        &source,
        vec![
            StratifyRule::new("even", ModRule { modulo: 0 }).meta("kind", "number_branch"),
            StratifyRule::new("odd", ModRule { modulo: 1 }),
        ],
        |rule: &ModRule, value: &i32| value % 2 == rule.modulo,
        StratifyOptions {
            prefix: "topic".to_owned(),
            branches: branch_opts,
            ..StratifyOptions::default()
        },
    );
    let even_seen = collect_data(stratified.branches.get("even").unwrap());
    let odd_seen = collect_data(stratified.branches.get("odd").unwrap());

    source.set(1);
    source.set(2);
    stratified.rules.set(vec![
        StratifyRule::new("even", ModRule { modulo: 1 }),
        StratifyRule::new("odd", ModRule { modulo: 0 }),
    ]);
    source.set(3);
    source.set(4);

    assert_eq!(*even_seen.borrow(), vec![0, 2, 3]);
    assert_eq!(*odd_seen.borrow(), vec![1, 2, 4]);
    let snap = g.describe();
    let even_node = snap
        .nodes
        .iter()
        .find(|node| node.id == "topic/even")
        .unwrap();
    assert_eq!(even_node.factory, "stratifyBranch");
    assert_eq!(
        even_node.meta.as_ref().and_then(|meta| meta.get("kind")),
        Some(&"number_branch".to_owned())
    );
    let odd_node = snap
        .nodes
        .iter()
        .find(|node| node.id == "topic/odd")
        .unwrap();
    assert_eq!(
        odd_node.meta.as_ref().and_then(|meta| meta.get("priority")),
        Some(&"high".to_owned())
    );
    assert!(snap.edges.contains(&DescribeEdge {
        from: "source".to_owned(),
        to: "topic/even".to_owned(),
    }));
    assert!(snap.edges.contains(&DescribeEdge {
        from: "topic/rules".to_owned(),
        to: "topic/odd".to_owned(),
    }));
    assert_eq!(
        odd_node.deps,
        vec!["source".to_owned(), "topic/rules".to_owned()]
    );
}

#[test]
#[should_panic(expected = "stratify: duplicate rule name 'dup'")]
fn stratify_rejects_duplicate_rule_names() {
    let g = graph();
    let source = g.state_opts(0i32, GraphNodeOpts::named("source"));
    let _ = stratify(
        &g,
        &source,
        vec![
            StratifyRule::new("dup", ModRule { modulo: 0 }),
            StratifyRule::new("dup", ModRule { modulo: 1 }),
        ],
        |rule: &ModRule, value: &i32| value % 2 == rule.modulo,
        StratifyOptions::default(),
    );
}
