//! Graph-layer composition helpers (B70 / D56).
//!
//! These are per-language sugar over declared graph nodes. They do not add
//! protocol verbs, live topology streams, or dynamic branch lifecycles.

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use crate::ctx::{Ctx, DepTerminal};
use crate::graph::{Graph, GraphNodeOpts};
use crate::node::{Node, NodeOpts};
use crate::operators::Operator;
use crate::protocol::Message;

/// Rust-native unary operator composition builder.
pub struct Pipe<T> {
    graph: Graph,
    current: Node<T>,
}

pub fn pipe<T: 'static>(graph: &Graph, source: Node<T>) -> Pipe<T> {
    Pipe {
        graph: graph.clone(),
        current: source,
    }
}

impl<T: 'static> Pipe<T> {
    pub fn through<U: 'static>(self, op: Operator<U>) -> Pipe<U> {
        self.through_opts(op, GraphNodeOpts::default())
    }

    pub fn through_opts<U: 'static>(self, op: Operator<U>, opts: GraphNodeOpts) -> Pipe<U> {
        let current = self.graph.init_node(op, vec![self.current.erased()], opts);
        Pipe {
            graph: self.graph,
            current,
        }
    }

    pub fn done(self) -> Node<T> {
        self.current
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StratifyRule<R> {
    pub name: String,
    pub rule: R,
    pub meta: BTreeMap<String, String>,
}

impl<R> StratifyRule<R> {
    pub fn new(name: impl Into<String>, rule: R) -> Self {
        Self {
            name: name.into(),
            rule,
            meta: BTreeMap::new(),
        }
    }

    pub fn meta(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.meta.insert(key.into(), value.into());
        self
    }
}

#[derive(Clone, Debug)]
pub struct StratifyOptions {
    pub prefix: String,
    pub rules: GraphNodeOpts,
    pub branches: BTreeMap<String, GraphNodeOpts>,
}

impl Default for StratifyOptions {
    fn default() -> Self {
        Self {
            prefix: "branch".to_owned(),
            rules: GraphNodeOpts::default(),
            branches: BTreeMap::new(),
        }
    }
}

#[derive(Clone)]
pub struct Stratified<T, R> {
    pub rules: Node<Vec<StratifyRule<R>>>,
    pub branches: BTreeMap<String, Node<T>>,
}

pub fn stratify_branch<T, R, F>(
    graph: &Graph,
    source: &Node<T>,
    rules: &Node<R>,
    classifier: F,
    opts: GraphNodeOpts,
) -> Node<T>
where
    T: Clone + 'static,
    R: Clone + 'static,
    F: Fn(&R, &T) -> bool + 'static,
{
    let op = stratify_branch_operator(classifier, 0, 1);
    graph.init_node(op, vec![source.erased(), rules.erased()], opts)
}

pub fn stratify<T, R, F>(
    graph: &Graph,
    source: &Node<T>,
    rules: Vec<StratifyRule<R>>,
    classifier: F,
    opts: StratifyOptions,
) -> Stratified<T, R>
where
    T: Clone + 'static,
    R: Clone + 'static,
    F: Fn(&R, &T) -> bool + 'static,
{
    let mut seen = BTreeSet::new();
    for rule in &rules {
        assert!(
            seen.insert(rule.name.clone()),
            "stratify: duplicate rule name '{}'",
            rule.name
        );
    }

    let mut rules_opts = opts.rules.clone();
    if rules_opts.name.is_none() {
        rules_opts.name = Some(format!("{}/rules", opts.prefix));
    }
    rules_opts
        .meta
        .entry("kind".to_owned())
        .or_insert_with(|| "stratify_rules".to_owned());
    let rules_node = graph.state_opts(rules.clone(), rules_opts);

    let classifier = Rc::new(classifier);
    let mut branches = BTreeMap::new();
    for rule in rules {
        let branch_name = rule.name.clone();
        let classifier = classifier.clone();
        let op = stratify_branch_operator(
            move |all: &Vec<StratifyRule<R>>, value: &T| {
                all.iter()
                    .find(|candidate| candidate.name == branch_name)
                    .map(|current| classifier(&current.rule, value))
                    .unwrap_or(false)
            },
            0,
            1,
        );
        let mut branch_opts = opts.branches.get(&rule.name).cloned().unwrap_or_default();
        if branch_opts.name.is_none() {
            branch_opts.name = Some(format!("{}/{}", opts.prefix, rule.name));
        }
        for (key, value) in rule.meta {
            branch_opts.meta.entry(key).or_insert(value);
        }
        branch_opts
            .meta
            .entry("branch".to_owned())
            .or_insert_with(|| rule.name.clone());
        let branch = graph.init_node(op, vec![source.erased(), rules_node.erased()], branch_opts);
        branches.insert(rule.name, branch);
    }

    Stratified {
        rules: rules_node,
        branches,
    }
}

fn stratify_branch_operator<T, R, F>(
    classifier: F,
    source_index: usize,
    rules_index: usize,
) -> Operator<T>
where
    T: Clone + 'static,
    R: Clone + 'static,
    F: Fn(&R, &T) -> bool + 'static,
{
    Operator::with_opts(
        "stratifyBranch",
        NodeOpts {
            partial: true,
            complete_when_deps_complete: false,
            error_when_deps_error: false,
            terminal_as_real_input: true,
            ..NodeOpts::default()
        },
        move |ctx: &Ctx| {
            let initialized = ctx.state_get::<bool>().map(|value| *value).unwrap_or(false);
            if let Some(rules) = ctx.data::<R>(rules_index) {
                let mut values = ctx.batch::<T>(source_index);
                let rules_changed = !ctx.batch::<R>(rules_index).is_empty();
                if values.is_empty() && (!initialized || rules_changed) {
                    if let Some(value) = ctx.data::<T>(source_index) {
                        values.push(value);
                    }
                }
                for value in values {
                    if classifier(rules.as_ref(), value.as_ref()) {
                        ctx.emit((*value).clone());
                    }
                }
            }
            if !initialized {
                ctx.state_set(true);
            }

            match ctx.terminal(source_index) {
                Some(DepTerminal::Complete) => ctx.down(vec![Message::Complete]),
                Some(DepTerminal::Error(error)) => {
                    ctx.down(vec![Message::Error(error.to_string().into())]);
                }
                None => {}
            }
        },
    )
}
