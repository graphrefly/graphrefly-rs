//! Pure graph diagnostics over `DescribeSnapshot` (D39/R-describe).
//!
//! These helpers are product-surface catch-up with the TypeScript clean-slate graph
//! diagnostics. They never read live nodes or mutate graph state; `describe()` remains
//! the single topology source of truth.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::graph::{DescribeEdge, DescribeNode, DescribeSnapshot, DescribeValue, Graph, Profile};
use crate::node::Status;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// `ReachableDirection` variants.
pub enum ReachableDirection {
    /// `Upstream` variant.
    Upstream,
    /// `Downstream` variant.
    Downstream,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
/// `ReachableOptions` data container.
pub struct ReachableOptions {
    /// `max_depth` field for max depth.
    pub max_depth: Option<usize>,
    /// `both` field for both.
    pub both: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
/// `ReachableResult` data container.
pub struct ReachableResult {
    /// `paths` field for paths.
    pub paths: Vec<String>,
    /// `depths` field for depths.
    pub depths: BTreeMap<String, usize>,
    /// `truncated` field for truncated.
    pub truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// `ExplainPathReason` variants.
pub enum ExplainPathReason {
    /// `Ok` variant.
    Ok,
    /// `NoSuchFrom` variant.
    NoSuchFrom,
    /// `NoSuchTo` variant.
    NoSuchTo,
    /// `NoPath` variant.
    NoPath,
    /// `MaxDepthExceeded` variant.
    MaxDepthExceeded,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
/// `ExplainPathOptions` data container.
pub struct ExplainPathOptions {
    /// `max_depth` field for max depth.
    pub max_depth: Option<usize>,
    /// `find_cycle` field for find cycle.
    pub find_cycle: bool,
}

#[derive(Debug, Clone, PartialEq)]
/// `CausalStep` data container.
pub struct CausalStep {
    /// `id` field for id.
    pub id: String,
    /// `factory` field for factory.
    pub factory: String,
    /// `status` field for status.
    pub status: Status,
    /// `value` field for value.
    pub value: Option<DescribeValue>,
    /// `hop` field for hop.
    pub hop: usize,
    /// `dep_index` field for dep index.
    pub dep_index: Option<usize>,
    /// `dep_indices` field for dep indices.
    pub dep_indices: Option<Vec<usize>>,
}

#[derive(Debug, Clone, PartialEq)]
/// `CausalChain` data container.
pub struct CausalChain {
    /// `from` field for from.
    pub from: String,
    /// `to` field for to.
    pub to: String,
    /// `found` field for found.
    pub found: bool,
    /// `reason` field for reason.
    pub reason: ExplainPathReason,
    /// `steps` field for steps.
    pub steps: Vec<CausalStep>,
    /// `text` field for text.
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `IslandReport` data container.
pub struct IslandReport {
    /// `id` field for id.
    pub id: String,
    /// `factory` field for factory.
    pub factory: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `ValidateNoIslandsResult` data container.
pub struct ValidateNoIslandsResult {
    /// `ok` field for ok.
    pub ok: bool,
    /// `orphans` field for orphans.
    pub orphans: Vec<IslandReport>,
}

#[derive(Debug, Clone, PartialEq)]
/// `DescribeEvent` variants.
pub enum DescribeEvent {
    /// `NodeAdded` variant.
    NodeAdded {
        /// `id` field for id.
        id: String,
        /// `node` field for node.
        node: DescribeNode,
    },
    /// `NodeRemoved` variant.
    NodeRemoved {
        /// `id` field for id.
        id: String,
    },
    /// `NodeMetaChanged` variant.
    NodeMetaChanged {
        /// `id` field for id.
        id: String,
        /// `prev_meta` field for prev meta.
        prev_meta: BTreeMap<String, String>,
        /// `next_meta` field for next meta.
        next_meta: BTreeMap<String, String>,
    },
    /// `EdgeAdded` variant.
    EdgeAdded {
        /// `from` field for from.
        from: String,
        /// `to` field for to.
        to: String,
    },
    /// `EdgeRemoved` variant.
    EdgeRemoved {
        /// `from` field for from.
        from: String,
        /// `to` field for to.
        to: String,
    },
    /// `SubgraphMounted` variant.
    SubgraphMounted {
        /// `path` field for path.
        path: String,
    },
    /// `SubgraphUnmounted` variant.
    SubgraphUnmounted {
        /// `path` field for path.
        path: String,
    },
}

#[derive(Debug, Clone, Default, PartialEq)]
/// `DescribeChangeset` data container.
pub struct DescribeChangeset {
    /// `events` field for events.
    pub events: Vec<DescribeEvent>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
/// `ProfileSummaryOptions` data container.
pub struct ProfileSummaryOptions {
    /// `limit` field for limit.
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `ProfileSummaryNode` data container.
pub struct ProfileSummaryNode {
    /// `path` field for path.
    pub path: String,
    /// `invokes` field for invokes.
    pub invokes: u64,
    /// `total_duration_ns` field for total duration ns.
    pub total_duration_ns: u128,
    /// `last_duration_ns` field for last duration ns.
    pub last_duration_ns: u128,
    /// `status` field for status.
    pub status: Status,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `ProfileSummaryStatus` data container.
pub struct ProfileSummaryStatus {
    /// `status` field for status.
    pub status: Status,
    /// `count` field for count.
    pub count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `ProfileSummary` data container.
pub struct ProfileSummary {
    /// `node_count` field for node count.
    pub node_count: usize,
    /// `total_invokes` field for total invokes.
    pub total_invokes: u64,
    /// `by_status` field for by status.
    pub by_status: Vec<ProfileSummaryStatus>,
    /// `hot_nodes` field for hot nodes.
    pub hot_nodes: Vec<ProfileSummaryNode>,
}

impl ValidateNoIslandsResult {
    /// Updates or reads `summary`.
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
struct FlatSnapshot {
    nodes: BTreeMap<String, DescribeNode>,
    edges: BTreeMap<(String, String), DescribeEdge>,
    subgraphs: BTreeSet<String>,
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

fn flatten_for_diff(snapshot: &DescribeSnapshot) -> FlatSnapshot {
    fn visit(snapshot: &DescribeSnapshot, index_path: &str, flat: &mut FlatSnapshot) {
        for node in &snapshot.nodes {
            flat.nodes.insert(node.id.clone(), node.clone());
        }
        for edge in &snapshot.edges {
            flat.edges
                .insert((edge.from.clone(), edge.to.clone()), edge.clone());
        }
        if let Some(children) = &snapshot.subgraphs {
            for (index, child) in children.iter().enumerate() {
                let prefixed = child.nodes.iter().find_map(|node| {
                    node.id
                        .rsplit_once("::")
                        .map(|(prefix, _)| prefix.to_owned())
                });
                let key = prefixed.unwrap_or_else(|| {
                    child
                        .name
                        .clone()
                        .unwrap_or_else(|| format!("{index_path}{index}"))
                });
                flat.subgraphs.insert(key.clone());
                visit(child, &format!("{key}/"), flat);
            }
        }
    }

    let mut flat = FlatSnapshot::default();
    visit(snapshot, "", &mut flat);
    flat
}

fn meta_or_empty(node: &DescribeNode) -> BTreeMap<String, String> {
    node.meta.clone().unwrap_or_default()
}

/// Pure topology delta from one D39 `describe()` snapshot to another.
///
/// Values/status changes are intentionally not topology events; use observe/profile for runtime
/// data. This helper never reads live nodes and does not create a topology stream.
pub fn topology_diff(prev: &DescribeSnapshot, next: &DescribeSnapshot) -> DescribeChangeset {
    let prev = flatten_for_diff(prev);
    let next = flatten_for_diff(next);
    let mut events = Vec::new();

    for path in next.subgraphs.difference(&prev.subgraphs) {
        events.push(DescribeEvent::SubgraphMounted { path: path.clone() });
    }
    for (id, node) in next
        .nodes
        .iter()
        .filter(|(id, _)| !prev.nodes.contains_key(*id))
    {
        events.push(DescribeEvent::NodeAdded {
            id: id.clone(),
            node: node.clone(),
        });
    }
    for (id, node) in next
        .nodes
        .iter()
        .filter(|(id, _)| prev.nodes.contains_key(*id))
    {
        let prev_meta = meta_or_empty(
            prev.nodes
                .get(id)
                .expect("filtered to nodes present in the previous snapshot"),
        );
        let next_meta = meta_or_empty(node);
        if prev_meta != next_meta {
            events.push(DescribeEvent::NodeMetaChanged {
                id: id.clone(),
                prev_meta,
                next_meta,
            });
        }
    }
    for ((from, to), _) in next
        .edges
        .iter()
        .filter(|(key, _)| !prev.edges.contains_key(*key))
    {
        events.push(DescribeEvent::EdgeAdded {
            from: from.clone(),
            to: to.clone(),
        });
    }
    for ((from, to), _) in prev
        .edges
        .iter()
        .filter(|(key, _)| !next.edges.contains_key(*key))
    {
        events.push(DescribeEvent::EdgeRemoved {
            from: from.clone(),
            to: to.clone(),
        });
    }
    for id in prev.nodes.keys().filter(|id| !next.nodes.contains_key(*id)) {
        events.push(DescribeEvent::NodeRemoved { id: id.clone() });
    }
    for path in prev.subgraphs.difference(&next.subgraphs) {
        events.push(DescribeEvent::SubgraphUnmounted { path: path.clone() });
    }

    DescribeChangeset { events }
}

/// Summarize an opt-in D39/R-profile snapshot using `describe()` for node cardinality.
///
/// This helper does not subscribe, emit, or create topology. The graph wrapper only reads the
/// existing `describe()` and `profile()` snapshots; the actual rollup is pure over those facts.
#[must_use]
pub fn profile_summary(graph: &Graph, options: ProfileSummaryOptions) -> ProfileSummary {
    let snapshot = graph.describe();
    let profile = graph.profile();
    profile_summary_from_snapshots(&snapshot, &profile, options)
}

/// Pure profile rollup over an already-captured describe/profile pair.
#[must_use]
pub fn profile_summary_from_snapshots(
    snapshot: &DescribeSnapshot,
    profile: &Profile,
    options: ProfileSummaryOptions,
) -> ProfileSummary {
    let mut node_ids = BTreeSet::new();
    collect_describe_ids(snapshot, &mut node_ids);
    node_ids.extend(profile.nodes.keys().cloned());

    let mut by_status_counts = BTreeMap::<usize, ProfileSummaryStatus>::new();
    let mut hot_nodes = Vec::new();
    for path in &node_ids {
        let Some(node_profile) = profile.nodes.get(path) else {
            continue;
        };
        let rank = status_rank(node_profile.status);
        by_status_counts
            .entry(rank)
            .and_modify(|summary| summary.count += 1)
            .or_insert(ProfileSummaryStatus {
                status: node_profile.status,
                count: 1,
            });
        hot_nodes.push(ProfileSummaryNode {
            path: path.clone(),
            invokes: node_profile.invokes,
            total_duration_ns: node_profile.total_duration_ns,
            last_duration_ns: node_profile.last_duration_ns,
            status: node_profile.status,
        });
    }

    hot_nodes.sort_by(|a, b| b.invokes.cmp(&a.invokes).then_with(|| a.path.cmp(&b.path)));
    if let Some(limit) = options.limit {
        hot_nodes.truncate(limit);
    }

    ProfileSummary {
        node_count: node_ids.len(),
        total_invokes: profile.total_invokes,
        by_status: by_status_counts.into_values().collect(),
        hot_nodes,
    }
}

fn collect_describe_ids(snapshot: &DescribeSnapshot, ids: &mut BTreeSet<String>) {
    ids.extend(snapshot.nodes.iter().map(|node| node.id.clone()));
    for child in snapshot.subgraphs.iter().flatten() {
        collect_describe_ids(child, ids);
    }
}

fn status_rank(status: Status) -> usize {
    match status {
        Status::Sentinel => 0,
        Status::Pending => 1,
        Status::Dirty => 2,
        Status::Settled => 3,
        Status::Resolved => 4,
        Status::Completed => 5,
        Status::Errored => 6,
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

/// Creates or computes `reachable`.
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

/// Creates or computes `explain_path`.
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

/// Creates or computes `validate_no_islands`.
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
