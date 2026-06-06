//! Pure graph diagnostics over `DescribeSnapshot` (D39/R-describe).
//!
//! These helpers are product-surface catch-up with the TypeScript clean-slate graph
//! diagnostics. They never read live nodes or mutate graph state; `describe()` remains
//! the single topology source of truth.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::graph::{DescribeNode, DescribeSnapshot, DescribeValue};
use crate::node::Status;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReachableDirection {
    Upstream,
    Downstream,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReachableOptions {
    pub max_depth: Option<usize>,
    pub both: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReachableResult {
    pub paths: Vec<String>,
    pub depths: BTreeMap<String, usize>,
    pub truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplainPathReason {
    Ok,
    NoSuchFrom,
    NoSuchTo,
    NoPath,
    MaxDepthExceeded,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExplainPathOptions {
    pub max_depth: Option<usize>,
    pub find_cycle: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CausalStep {
    pub id: String,
    pub factory: String,
    pub status: Status,
    pub value: Option<DescribeValue>,
    pub hop: usize,
    pub dep_index: Option<usize>,
    pub dep_indices: Option<Vec<usize>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CausalChain {
    pub from: String,
    pub to: String,
    pub found: bool,
    pub reason: ExplainPathReason,
    pub steps: Vec<CausalStep>,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IslandReport {
    pub id: String,
    pub factory: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidateNoIslandsResult {
    pub ok: bool,
    pub orphans: Vec<IslandReport>,
}

impl ValidateNoIslandsResult {
    pub fn summary(&self) -> String {
        if self.orphans.is_empty() {
            return "validate_no_islands: ok (no islands)".to_owned();
        }
        let head = self
            .orphans
            .iter()
            .take(3)
            .map(|o| format!("{} ({})", o.id, o.factory))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "validate_no_islands: {} island node(s) - {}{}",
            self.orphans.len(),
            head,
            if self.orphans.len() > 3 { ", ..." } else { "" }
        )
    }
}

#[derive(Debug, Default)]
struct SnapshotIndex {
    nodes: BTreeMap<String, DescribeNode>,
    outgoing: BTreeMap<String, BTreeSet<String>>,
    incoming: BTreeMap<String, BTreeSet<String>>,
}

fn flatten(snapshot: &DescribeSnapshot, nodes: &mut Vec<DescribeNode>) {
    nodes.extend(snapshot.nodes.clone());
    for child in snapshot.subgraphs.iter().flatten() {
        flatten(child, nodes);
    }
}

fn add_edge(map: &mut BTreeMap<String, BTreeSet<String>>, from: &str, to: &str) {
    map.entry(from.to_owned())
        .or_default()
        .insert(to.to_owned());
}

fn index_snapshot(snapshot: &DescribeSnapshot) -> SnapshotIndex {
    let mut flat_nodes = Vec::new();
    flatten(snapshot, &mut flat_nodes);
    let mut idx = SnapshotIndex::default();
    for node in flat_nodes {
        idx.outgoing.entry(node.id.clone()).or_default();
        idx.incoming.entry(node.id.clone()).or_default();
        idx.nodes.insert(node.id.clone(), node);
    }

    let nodes = idx.nodes.clone();
    for node in nodes.values() {
        for dep in &node.deps {
            if idx.nodes.contains_key(dep) {
                add_edge(&mut idx.outgoing, dep, &node.id);
                add_edge(&mut idx.incoming, &node.id, dep);
            }
        }
    }

    index_snapshot_edges(snapshot, &mut idx);
    idx
}

fn index_snapshot_edges(snapshot: &DescribeSnapshot, idx: &mut SnapshotIndex) {
    for edge in &snapshot.edges {
        if idx.nodes.contains_key(&edge.from) && idx.nodes.contains_key(&edge.to) {
            add_edge(&mut idx.outgoing, &edge.from, &edge.to);
            add_edge(&mut idx.incoming, &edge.to, &edge.from);
        }
    }
    for child in snapshot.subgraphs.iter().flatten() {
        index_snapshot_edges(child, idx);
    }
}

fn adjacent(
    idx: &SnapshotIndex,
    id: &str,
    direction: ReachableDirection,
    both: bool,
) -> BTreeSet<String> {
    if both {
        let mut out = idx.incoming.get(id).cloned().unwrap_or_default();
        out.extend(idx.outgoing.get(id).cloned().unwrap_or_default());
        return out;
    }
    match direction {
        ReachableDirection::Upstream => idx.incoming.get(id).cloned().unwrap_or_default(),
        ReachableDirection::Downstream => idx.outgoing.get(id).cloned().unwrap_or_default(),
    }
}

pub fn reachable(
    snapshot: &DescribeSnapshot,
    from: &str,
    direction: ReachableDirection,
    options: ReachableOptions,
) -> ReachableResult {
    if from.is_empty() {
        return ReachableResult::default();
    }
    let idx = index_snapshot(snapshot);
    if !idx.nodes.contains_key(from) {
        return ReachableResult::default();
    }
    if options.max_depth == Some(0) {
        return ReachableResult {
            truncated: !adjacent(&idx, from, direction, options.both).is_empty(),
            ..ReachableResult::default()
        };
    }

    let mut depths = BTreeMap::new();
    let mut seen = BTreeSet::from([from.to_owned()]);
    let mut queue = VecDeque::from([(from.to_owned(), 0usize)]);
    let mut truncated = false;
    while let Some((id, depth)) = queue.pop_front() {
        let next = adjacent(&idx, &id, direction, options.both);
        if options.max_depth.is_some_and(|max| depth >= max) {
            if !next.is_empty() {
                truncated = true;
            }
            continue;
        }
        for next_id in next {
            if seen.insert(next_id.clone()) {
                let next_depth = depth + 1;
                depths.insert(next_id.clone(), next_depth);
                queue.push_back((next_id, next_depth));
            }
        }
    }
    ReachableResult {
        paths: depths.keys().cloned().collect(),
        depths,
        truncated,
    }
}

fn dep_indices(next: Option<&DescribeNode>, prev_id: &str) -> Option<Vec<usize>> {
    let next = next?;
    let indices = next
        .deps
        .iter()
        .enumerate()
        .filter_map(|(i, dep)| (dep == prev_id).then_some(i))
        .collect::<Vec<_>>();
    (!indices.is_empty()).then_some(indices)
}

fn step_for(
    node: &DescribeNode,
    hop: usize,
    edge_to_next: Option<(&DescribeNode, &str)>,
) -> CausalStep {
    let indices = edge_to_next.and_then(|(next, prev_id)| dep_indices(Some(next), prev_id));
    CausalStep {
        id: node.id.clone(),
        factory: node.factory.clone(),
        status: node.status,
        value: node.value.clone(),
        hop,
        dep_index: indices.as_ref().and_then(|xs| xs.first().copied()),
        dep_indices: indices.as_ref().filter(|xs| xs.len() > 1).cloned(),
    }
}

fn make_chain(
    from: &str,
    to: &str,
    reason: ExplainPathReason,
    steps: Vec<CausalStep>,
) -> CausalChain {
    let found = reason == ExplainPathReason::Ok;
    let text = if found {
        steps
            .iter()
            .map(|s| s.id.as_str())
            .collect::<Vec<_>>()
            .join(" -> ")
    } else {
        format!("explain_path: {reason:?} from '{from}' to '{to}'")
    };
    CausalChain {
        from: from.to_owned(),
        to: to.to_owned(),
        found,
        reason,
        steps,
        text,
    }
}

fn shortest_path(
    idx: &SnapshotIndex,
    from: &str,
    to: &str,
    max_depth: Option<usize>,
) -> Option<(Vec<String>, bool)> {
    let mut pred = BTreeMap::<String, String>::new();
    let mut seen = BTreeSet::from([from.to_owned()]);
    let mut queue = VecDeque::from([(from.to_owned(), 0usize)]);
    let mut truncated = false;
    while let Some((id, depth)) = queue.pop_front() {
        let next = idx.outgoing.get(&id).cloned().unwrap_or_default();
        if max_depth.is_some_and(|max| depth >= max) {
            if !next.is_empty() {
                truncated = true;
            }
            continue;
        }
        for next_id in next {
            if seen.contains(&next_id) {
                continue;
            }
            seen.insert(next_id.clone());
            pred.insert(next_id.clone(), id.clone());
            if next_id == to {
                let mut path = vec![to.to_owned()];
                let mut p = to.to_owned();
                while p != from {
                    p = pred.get(&p).expect("predecessor exists").clone();
                    path.push(p.clone());
                }
                path.reverse();
                return Some((path, truncated));
            }
            queue.push_back((next_id, depth + 1));
        }
    }
    truncated.then_some((Vec::new(), true))
}

fn shortest_cycle(
    idx: &SnapshotIndex,
    from: &str,
    max_depth: Option<usize>,
) -> Option<(Vec<String>, bool)> {
    let first = idx.outgoing.get(from).cloned().unwrap_or_default();
    if max_depth == Some(0) {
        return (!first.is_empty()).then_some((Vec::new(), true));
    }
    let mut queue = VecDeque::<(String, usize, Vec<String>)>::new();
    let mut seen = BTreeSet::new();
    for id in first {
        if id == from {
            return Some((vec![from.to_owned(), from.to_owned()], false));
        }
        seen.insert(id.clone());
        queue.push_back((id.clone(), 1, vec![from.to_owned(), id]));
    }

    let mut truncated = false;
    while let Some((id, depth, path)) = queue.pop_front() {
        let next = idx.outgoing.get(&id).cloned().unwrap_or_default();
        if max_depth.is_some_and(|max| depth >= max) {
            if !next.is_empty() {
                truncated = true;
            }
            continue;
        }
        for next_id in next {
            if next_id == from {
                let mut found = path;
                found.push(from.to_owned());
                return Some((found, false));
            }
            if seen.insert(next_id.clone()) {
                let mut next_path = path.clone();
                next_path.push(next_id.clone());
                queue.push_back((next_id, depth + 1, next_path));
            }
        }
    }
    truncated.then_some((Vec::new(), true))
}

fn materialize_path(idx: &SnapshotIndex, path: &[String]) -> Vec<CausalStep> {
    path.iter()
        .enumerate()
        .map(|(i, id)| {
            let node = idx.nodes.get(id).expect("path node exists");
            let next = path.get(i + 1).and_then(|next_id| idx.nodes.get(next_id));
            step_for(node, i, next.map(|next| (next, id.as_str())))
        })
        .collect()
}

pub fn explain_path(
    snapshot: &DescribeSnapshot,
    from: &str,
    to: &str,
    options: ExplainPathOptions,
) -> CausalChain {
    let idx = index_snapshot(snapshot);
    if !idx.nodes.contains_key(from) {
        return make_chain(from, to, ExplainPathReason::NoSuchFrom, Vec::new());
    }
    if !idx.nodes.contains_key(to) {
        return make_chain(from, to, ExplainPathReason::NoSuchTo, Vec::new());
    }
    if options.max_depth == Some(0) && from != to {
        return make_chain(from, to, ExplainPathReason::NoPath, Vec::new());
    }

    if from == to && !options.find_cycle {
        let step = step_for(idx.nodes.get(from).expect("node exists"), 0, None);
        return make_chain(from, to, ExplainPathReason::Ok, vec![step]);
    }
    if from == to && options.find_cycle {
        match shortest_cycle(&idx, from, options.max_depth) {
            Some((path, false)) if !path.is_empty() => {
                return make_chain(
                    from,
                    to,
                    ExplainPathReason::Ok,
                    materialize_path(&idx, &path),
                );
            }
            Some((_, true)) => {
                return make_chain(from, to, ExplainPathReason::MaxDepthExceeded, Vec::new());
            }
            _ => {
                let step = step_for(idx.nodes.get(from).expect("node exists"), 0, None);
                return make_chain(from, to, ExplainPathReason::Ok, vec![step]);
            }
        }
    }

    match shortest_path(&idx, from, to, options.max_depth) {
        Some((path, _)) if !path.is_empty() => make_chain(
            from,
            to,
            ExplainPathReason::Ok,
            materialize_path(&idx, &path),
        ),
        Some((_, true)) => make_chain(from, to, ExplainPathReason::MaxDepthExceeded, Vec::new()),
        _ => make_chain(from, to, ExplainPathReason::NoPath, Vec::new()),
    }
}

pub fn validate_no_islands(snapshot: &DescribeSnapshot) -> ValidateNoIslandsResult {
    let idx = index_snapshot(snapshot);
    let mut orphans = Vec::new();
    for node in idx.nodes.values() {
        let has_deps =
            !node.deps.is_empty() || !idx.incoming.get(&node.id).is_none_or(BTreeSet::is_empty);
        let has_dependents = !idx.outgoing.get(&node.id).is_none_or(BTreeSet::is_empty);
        if !has_deps && !has_dependents && !node.id.starts_with("__internal__/") {
            orphans.push(IslandReport {
                id: node.id.clone(),
                factory: node.factory.clone(),
            });
        }
    }
    orphans.sort_by(|a, b| a.id.cmp(&b.id));
    ValidateNoIslandsResult {
        ok: orphans.is_empty(),
        orphans,
    }
}
