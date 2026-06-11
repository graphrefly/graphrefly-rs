//! Horizontal graph-visible patterns.
//!
//! D158 allows semantic-memory patterns when they are ordinary graph nodes with
//! declared deps and graph-visible facts. This module intentionally owns no
//! storage restore/hydration, scheduler, vector DB, LLM extraction, retention
//! loop, consolidation loop, or protocol behavior.

use std::collections::{BTreeMap, HashSet};
use std::rc::Rc;

use crate::graph::{Graph, GraphNodeOpts};
use crate::node::{Node, NodeOpts};
use crate::operators::Operator;

/// Stable identity for a semantic-memory fact.
pub type FactId = String;

/// A single semantic-memory fact. This is pattern vocabulary, not a protocol
/// message, storage record owner, restore contract, or agentic runtime.
#[derive(Clone, Debug, PartialEq)]
pub struct MemoryFragment<T> {
    pub id: FactId,
    pub payload: T,
    pub t_ns: u128,
    pub valid_from: Option<u128>,
    pub valid_to: Option<u128>,
    pub confidence: f64,
    pub tags: Vec<String>,
    pub sources: Vec<FactId>,
    pub embedding: Option<Vec<f64>>,
    pub parent_fragment_id: Option<FactId>,
    pub provenance: Option<String>,
}

impl<T> MemoryFragment<T> {
    pub fn new(id: impl Into<String>, payload: T, t_ns: u128) -> Self {
        Self {
            id: id.into(),
            payload,
            t_ns,
            valid_from: None,
            valid_to: None,
            confidence: 1.0,
            tags: Vec::new(),
            sources: Vec::new(),
            embedding: None,
            parent_fragment_id: None,
            provenance: None,
        }
    }
}

pub type ShardKey = String;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct FactStore<T> {
    pub by_id: BTreeMap<FactId, MemoryFragment<T>>,
}

impl<T> FactStore<T> {
    pub fn read_handle(&self) -> StoreReadHandle<'_, T> {
        StoreReadHandle { by_id: &self.by_id }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct StoreReadHandle<'a, T> {
    by_id: &'a BTreeMap<FactId, MemoryFragment<T>>,
}

impl<'a, T> StoreReadHandle<'a, T> {
    pub fn get(&self, id: &str) -> Option<&'a MemoryFragment<T>> {
        self.by_id.get(id)
    }

    pub fn has(&self, id: &str) -> bool {
        self.by_id.contains_key(id)
    }

    pub fn size(&self) -> usize {
        self.by_id.len()
    }

    pub fn values(&self) -> impl Iterator<Item = &'a MemoryFragment<T>> {
        self.by_id.values()
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MemoryQuery {
    pub tags: Vec<String>,
    pub as_of: Option<u128>,
    pub min_confidence: Option<f64>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryFragmentValidation {
    pub ok: bool,
    pub errors: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OutcomeSignal {
    pub fact_id: FactId,
    pub reward: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CollectionEntry<T> {
    pub id: String,
    pub value: T,
    pub created_at_ns: u128,
    pub last_access_ns: u128,
    pub base_score: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RankedCollectionEntry<T> {
    pub entry: CollectionEntry<T>,
    pub score: f64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct RetrievalQuery {
    pub text: Option<String>,
    pub vector: Option<Vec<f64>>,
    pub entity_ids: Vec<String>,
    pub context: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VectorSearchResult<TMeta> {
    pub id: String,
    pub score: f64,
    pub meta: Option<TMeta>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetrievalEntrySource {
    Vector,
    Graph,
    Store,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RetrievalEntry<TMem> {
    pub key: String,
    pub value: TMem,
    pub score: f64,
    pub sources: Vec<RetrievalEntrySource>,
    pub context: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RetrievalTrace<TMem> {
    pub vector_candidates: Vec<VectorSearchResult<TMem>>,
    pub graph_expanded: Vec<String>,
    pub ranked: Vec<RetrievalEntry<TMem>>,
    pub packed: Vec<RetrievalEntry<TMem>>,
}

pub type AdmissionThresholds = BTreeMap<String, f64>;
pub type AdmissionScoreFn<TRaw> = Rc<dyn Fn(&TRaw) -> BTreeMap<String, f64>>;
pub type AdmissionScore3DFn<TRaw> = Rc<dyn Fn(&TRaw) -> AdmissionScores>;
pub type TenantShardFn<T> = Rc<dyn Fn(&MemoryFragment<T>) -> String>;
pub type ShardByFn<T> = Rc<dyn Fn(&MemoryFragment<T>) -> ShardKey>;

pub struct AdmissionScoredOptions<TRaw> {
    pub score_fn: AdmissionScoreFn<TRaw>,
    pub thresholds: AdmissionThresholds,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AdmissionScores {
    pub persistence: f64,
    pub structure: f64,
    pub personal_value: f64,
}

pub struct AdmissionScore3DOptions<TRaw> {
    pub score_fn: AdmissionScore3DFn<TRaw>,
    pub persistence_threshold: f64,
    pub personal_value_threshold: f64,
    pub require_structured: bool,
}

impl<TRaw> AdmissionScore3DOptions<TRaw> {
    pub fn new(score_fn: AdmissionScore3DFn<TRaw>) -> Self {
        Self {
            score_fn,
            persistence_threshold: 0.3,
            personal_value_threshold: 0.3,
            require_structured: false,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ShardByTenantOptions {
    pub tenants: Vec<String>,
    pub shard_count: Option<usize>,
}

pub struct ShardByTenantConfig<T> {
    pub shard_by: ShardByFn<T>,
    pub shard_count: usize,
}

pub fn cosine_similarity(a: &[f64], b: &[f64]) -> f64 {
    let n = a.len().max(b.len());
    let mut dot = 0.0;
    let mut na = 0.0;
    let mut nb = 0.0;
    for i in 0..n {
        let av = a.get(i).copied().unwrap_or(0.0);
        let bv = b.get(i).copied().unwrap_or(0.0);
        dot += av * bv;
        na += av * av;
        nb += bv * bv;
    }
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    let score = dot / (na.sqrt() * nb.sqrt());
    if score.is_finite() {
        score
    } else {
        0.0
    }
}

pub fn memory_fragment_valid_at<T>(fragment: &MemoryFragment<T>, as_of: Option<u128>) -> bool {
    match as_of {
        None => fragment.valid_from.is_none() && fragment.valid_to.is_none(),
        Some(as_of) => {
            if fragment
                .valid_from
                .is_some_and(|valid_from| valid_from > as_of)
            {
                return false;
            }
            if fragment.valid_to.is_some_and(|valid_to| valid_to <= as_of) {
                return false;
            }
            true
        }
    }
}

pub fn memory_fragment_matches_query<T>(fragment: &MemoryFragment<T>, query: &MemoryQuery) -> bool {
    if !memory_fragment_valid_at(fragment, query.as_of) {
        return false;
    }
    if query
        .min_confidence
        .is_some_and(|min_confidence| fragment.confidence < min_confidence)
    {
        return false;
    }
    if !query.tags.is_empty()
        && !query
            .tags
            .iter()
            .any(|tag| fragment.tags.iter().any(|fragment_tag| fragment_tag == tag))
    {
        return false;
    }
    true
}

pub fn filter_memory_fragments<T: Clone>(
    fragments: impl IntoIterator<Item = MemoryFragment<T>>,
    query: &MemoryQuery,
) -> Vec<MemoryFragment<T>> {
    let mut ranked: Vec<_> = fragments
        .into_iter()
        .filter(|fragment| memory_fragment_matches_query(fragment, query))
        .collect();
    ranked.sort_by(|a, b| {
        b.confidence
            .total_cmp(&a.confidence)
            .then_with(|| b.t_ns.cmp(&a.t_ns))
    });
    if let Some(limit) = query.limit {
        ranked.truncate(limit);
    }
    ranked
}

pub fn validate_memory_fragment<T>(fragment: &MemoryFragment<T>) -> MemoryFragmentValidation {
    let errors = validate_fragment(fragment);
    MemoryFragmentValidation {
        ok: errors.is_empty(),
        errors,
    }
}

pub fn admission_scored<TRaw>(opts: AdmissionScoredOptions<TRaw>) -> impl Fn(&TRaw) -> bool {
    move |raw| {
        let scores = (opts.score_fn)(raw);
        opts.thresholds.iter().all(|(dimension, threshold)| {
            scores
                .get(dimension)
                .copied()
                .filter(|score| score.is_finite())
                .is_some_and(|score| score >= *threshold)
        })
    }
}

pub fn admission_filter_3d<TRaw>(opts: AdmissionScore3DOptions<TRaw>) -> impl Fn(&TRaw) -> bool {
    move |raw| {
        let scores = (opts.score_fn)(raw);
        score_at_least(scores.persistence, opts.persistence_threshold)
            && score_at_least(scores.personal_value, opts.personal_value_threshold)
            && (!opts.require_structured || score_at_least(scores.structure, f64::MIN_POSITIVE))
    }
}

pub fn shard_by_tenant<T: 'static>(
    tenant_of: TenantShardFn<T>,
    opts: ShardByTenantOptions,
) -> ShardByTenantConfig<T> {
    if !opts.tenants.is_empty() {
        let mut tenants = Vec::<String>::new();
        for tenant in opts.tenants {
            if !tenants.iter().any(|known| known == &tenant) {
                tenants.push(tenant);
            }
        }
        let index: BTreeMap<_, _> = tenants
            .iter()
            .enumerate()
            .map(|(i, tenant)| (tenant.clone(), i.to_string()))
            .collect();
        let overflow = tenants.len().to_string();
        return ShardByTenantConfig {
            shard_count: tenants.len() + 1,
            shard_by: Rc::new(move |fragment| {
                index
                    .get(&(tenant_of)(fragment))
                    .cloned()
                    .unwrap_or_else(|| overflow.clone())
            }),
        };
    }
    let shard_count = opts.shard_count.unwrap_or(4).max(1);
    ShardByTenantConfig {
        shard_count,
        shard_by: Rc::new(move |fragment| (tenant_of)(fragment)),
    }
}

fn score_at_least(score: f64, min: f64) -> bool {
    score.is_finite() && score >= min
}

/// Structured query over semantic-memory facts.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MemoryRetrievalQuery {
    pub tags: Vec<String>,
    pub as_of: Option<u128>,
    pub min_confidence: Option<f64>,
    pub limit: Option<usize>,
    pub vector: Option<Vec<f64>>,
}

impl MemoryRetrievalQuery {
    pub fn memory_query(&self) -> MemoryQuery {
        MemoryQuery {
            tags: self.tags.clone(),
            as_of: self.as_of,
            min_confidence: self.min_confidence,
            limit: self.limit,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryRetrievalCursor {
    pub evaluation: u64,
    pub valid_fragments: usize,
    pub invalid_fragments: usize,
    pub result_count: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemoryRetrievalStatusState {
    Ready,
    Empty,
    Partial,
    Error,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MemoryRetrievalStatus {
    pub state: MemoryRetrievalStatusState,
    pub query: MemoryRetrievalQuery,
    pub cursor: MemoryRetrievalCursor,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MemoryRetrievalIndex<T> {
    pub ids: Vec<FactId>,
    pub by_id: BTreeMap<FactId, MemoryFragment<T>>,
    pub cursor: MemoryRetrievalCursor,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemoryRetrievalErrorCode {
    DuplicateFragmentId,
    InvalidFragment,
    InvalidQuery,
    InvalidQueryVector,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryRetrievalError {
    pub code: MemoryRetrievalErrorCode,
    pub message: String,
    pub index: Option<usize>,
    pub fragment_id: Option<FactId>,
    pub validation_errors: Vec<String>,
    pub cursor: MemoryRetrievalCursor,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MemoryAnswer<T> {
    pub query: MemoryRetrievalQuery,
    pub results: Vec<MemoryFragment<T>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MemoryRetrievalSnapshot<T> {
    pub fragments: Vec<MemoryFragment<T>>,
    pub indexed: MemoryRetrievalIndex<T>,
    pub ranked: MemoryAnswer<T>,
    pub status: MemoryRetrievalStatus,
    pub errors: Vec<MemoryRetrievalError>,
    pub cursor: MemoryRetrievalCursor,
}

/// Alias for the aggregate DATA fact emitted by the snapshot node.
pub type MemoryRetrievalFact<T> = MemoryRetrievalSnapshot<T>;

#[derive(Clone)]
pub struct MemoryRetrievalBundleOptions<T> {
    pub name: Option<String>,
    pub fragments: Node<Vec<MemoryFragment<T>>>,
    pub query: Node<MemoryRetrievalQuery>,
}

impl<T> MemoryRetrievalBundleOptions<T> {
    pub fn new(fragments: Node<Vec<MemoryFragment<T>>>, query: Node<MemoryRetrievalQuery>) -> Self {
        Self {
            name: None,
            fragments,
            query,
        }
    }

    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
}

#[derive(Clone)]
pub struct MemoryRetrievalBundle<T> {
    pub fragments_input: Node<Vec<MemoryFragment<T>>>,
    pub query_input: Node<MemoryRetrievalQuery>,
    pub snapshot: Node<MemoryRetrievalSnapshot<T>>,
    pub fragments: Node<Vec<MemoryFragment<T>>>,
    pub indexed: Node<MemoryRetrievalIndex<T>>,
    pub ranked: Node<MemoryAnswer<T>>,
    pub status: Node<MemoryRetrievalStatus>,
    pub errors: Node<Vec<MemoryRetrievalError>>,
    pub cursor: Node<MemoryRetrievalCursor>,
}

#[derive(Clone, Debug)]
struct PendingMemoryRetrievalError {
    code: MemoryRetrievalErrorCode,
    message: String,
    index: Option<usize>,
    fragment_id: Option<FactId>,
    validation_errors: Vec<String>,
}

/// Build a graph-visible memory retrieval bundle from explicit fragment/query deps.
///
/// Invalid fragments and ranking status are emitted as ordinary DATA facts. The
/// bundle never owns storage restore or hidden mutation; callers that need
/// persistence compose D161 collection/storage sidecars outside this pattern.
pub fn memory_retrieval_bundle<T: Clone + 'static>(
    graph: &Graph,
    opts: MemoryRetrievalBundleOptions<T>,
) -> MemoryRetrievalBundle<T> {
    let name = opts.name.unwrap_or_else(|| "memoryRetrieval".to_owned());
    let fragments = opts.fragments;
    let query = opts.query;
    let snapshot = graph.init_node(
        Operator::with_opts("memoryRetrievalSnapshot", pattern_node_config(), |ctx| {
            let evaluation = ctx
                .state_get::<u64>()
                .map(|evaluation| *evaluation + 1)
                .unwrap_or(1);
            let raw_fragments = ctx
                .data::<Vec<MemoryFragment<T>>>(0)
                .map(|fragments| (*fragments).clone())
                .unwrap_or_default();
            let raw_query = ctx
                .data::<MemoryRetrievalQuery>(1)
                .map(|query| (*query).clone())
                .unwrap_or_default();
            let query_errors = validate_query(&raw_query);
            let current_query = if query_errors.is_empty() {
                raw_query
            } else {
                MemoryRetrievalQuery::default()
            };
            let mut valid = Vec::new();
            let mut seen = HashSet::new();
            let mut errors = query_errors;

            for (index, fragment) in raw_fragments.into_iter().enumerate() {
                let fragment_errors = validate_fragment(&fragment);
                if !fragment_errors.is_empty() {
                    errors.push(PendingMemoryRetrievalError {
                        code: MemoryRetrievalErrorCode::InvalidFragment,
                        message: "memory_retrieval_bundle: invalid memory fragment".to_owned(),
                        index: Some(index),
                        fragment_id: Some(fragment.id.clone()),
                        validation_errors: fragment_errors,
                    });
                    continue;
                }
                if !seen.insert(fragment.id.clone()) {
                    errors.push(PendingMemoryRetrievalError {
                        code: MemoryRetrievalErrorCode::DuplicateFragmentId,
                        message: "memory_retrieval_bundle: duplicate fragment id".to_owned(),
                        index: Some(index),
                        fragment_id: Some(fragment.id.clone()),
                        validation_errors: vec![format!("duplicate fragment id '{}'", fragment.id)],
                    });
                    continue;
                }
                valid.push(fragment);
            }

            let ranked = if errors
                .iter()
                .any(|error| !is_recoverable_fragment_error(error))
            {
                Vec::new()
            } else {
                rank_fragments(&valid, &current_query)
            };
            let cursor = MemoryRetrievalCursor {
                evaluation,
                valid_fragments: valid.len(),
                invalid_fragments: errors
                    .iter()
                    .filter(|error| is_recoverable_fragment_error(error))
                    .count(),
                result_count: ranked.len(),
            };
            ctx.state_set(evaluation);
            let status = MemoryRetrievalStatus {
                state: status_state(&errors, ranked.len()),
                query: current_query.clone(),
                cursor: cursor.clone(),
            };
            let indexed = MemoryRetrievalIndex {
                ids: valid.iter().map(|fragment| fragment.id.clone()).collect(),
                by_id: valid
                    .iter()
                    .map(|fragment| (fragment.id.clone(), fragment.clone()))
                    .collect(),
                cursor: cursor.clone(),
            };
            let errors = errors
                .into_iter()
                .map(|error| MemoryRetrievalError {
                    code: error.code,
                    message: error.message,
                    index: error.index,
                    fragment_id: error.fragment_id,
                    validation_errors: error.validation_errors,
                    cursor: cursor.clone(),
                })
                .collect();
            ctx.emit(MemoryRetrievalSnapshot {
                fragments: valid,
                indexed,
                ranked: MemoryAnswer {
                    query: current_query,
                    results: ranked,
                },
                status,
                errors,
                cursor,
            });
        }),
        vec![fragments.erased(), query.erased()],
        named_graph_node_opts(format!("{name}/snapshot")),
    );

    MemoryRetrievalBundle {
        fragments_input: fragments,
        query_input: query,
        fragments: retrieval_projection(
            graph,
            &snapshot,
            format!("{name}/fragments"),
            "memoryRetrievalFragments",
            |snapshot| snapshot.fragments.clone(),
        ),
        indexed: retrieval_projection(
            graph,
            &snapshot,
            format!("{name}/indexed"),
            "memoryRetrievalIndexed",
            |snapshot| snapshot.indexed.clone(),
        ),
        ranked: retrieval_projection(
            graph,
            &snapshot,
            format!("{name}/ranked"),
            "memoryRetrievalRanked",
            |snapshot| snapshot.ranked.clone(),
        ),
        status: retrieval_projection(
            graph,
            &snapshot,
            format!("{name}/status"),
            "memoryRetrievalStatus",
            |snapshot| snapshot.status.clone(),
        ),
        errors: retrieval_projection(
            graph,
            &snapshot,
            format!("{name}/errors"),
            "memoryRetrievalErrors",
            |snapshot| snapshot.errors.clone(),
        ),
        cursor: retrieval_projection(
            graph,
            &snapshot,
            format!("{name}/cursor"),
            "memoryRetrievalCursor",
            |snapshot| snapshot.cursor.clone(),
        ),
        snapshot,
    }
}

fn retrieval_projection<T, U, F>(
    graph: &Graph,
    snapshot: &Node<MemoryRetrievalSnapshot<T>>,
    name: String,
    factory: &'static str,
    select: F,
) -> Node<U>
where
    T: Clone + 'static,
    U: 'static,
    F: Fn(&MemoryRetrievalSnapshot<T>) -> U + 'static,
{
    graph.init_node(
        Operator::with_opts(factory, pattern_node_config(), move |ctx| {
            for snapshot in ctx.batch::<MemoryRetrievalSnapshot<T>>(0) {
                ctx.emit(select(snapshot.as_ref()));
            }
        }),
        vec![snapshot.erased()],
        named_graph_node_opts(name),
    )
}

fn pattern_node_config() -> NodeOpts {
    NodeOpts {
        complete_when_deps_complete: false,
        error_when_deps_error: false,
        ..NodeOpts::default()
    }
}

fn named_graph_node_opts(name: String) -> GraphNodeOpts {
    GraphNodeOpts {
        name: Some(name),
        ..GraphNodeOpts::default()
    }
}

fn validate_query(query: &MemoryRetrievalQuery) -> Vec<PendingMemoryRetrievalError> {
    let mut errors = Vec::new();
    if let Some(min_confidence) = query.min_confidence {
        if !min_confidence.is_finite() || !(0.0..=1.0).contains(&min_confidence) {
            errors.push(PendingMemoryRetrievalError {
                code: MemoryRetrievalErrorCode::InvalidQuery,
                message: "memory_retrieval_bundle: query.min_confidence must be finite in [0, 1]"
                    .to_owned(),
                index: None,
                fragment_id: None,
                validation_errors: vec!["min_confidence must be finite in [0, 1]".to_owned()],
            });
        }
    }
    if let Some(vector) = &query.vector {
        if vector.iter().any(|component| !component.is_finite()) {
            errors.push(PendingMemoryRetrievalError {
                code: MemoryRetrievalErrorCode::InvalidQueryVector,
                message: "memory_retrieval_bundle: query.vector must be a finite number array"
                    .to_owned(),
                index: None,
                fragment_id: None,
                validation_errors: vec!["vector must be a finite number array".to_owned()],
            });
        }
    }
    errors
}

fn validate_fragment<T>(fragment: &MemoryFragment<T>) -> Vec<String> {
    let mut errors = Vec::new();
    if fragment.id.is_empty() {
        errors.push("id must be a non-empty string".to_owned());
    }
    if !fragment.confidence.is_finite() || !(0.0..=1.0).contains(&fragment.confidence) {
        errors.push("confidence must be finite in [0, 1]".to_owned());
    }
    if let (Some(valid_from), Some(valid_to)) = (fragment.valid_from, fragment.valid_to) {
        if valid_from >= valid_to {
            errors.push("valid_from must be earlier than valid_to".to_owned());
        }
    }
    if fragment
        .embedding
        .as_ref()
        .is_some_and(|embedding| embedding.iter().any(|component| !component.is_finite()))
    {
        errors.push("embedding must be a finite number array when present".to_owned());
    }
    errors
}

fn is_recoverable_fragment_error(error: &PendingMemoryRetrievalError) -> bool {
    matches!(
        error.code,
        MemoryRetrievalErrorCode::InvalidFragment | MemoryRetrievalErrorCode::DuplicateFragmentId
    )
}

fn status_state(
    errors: &[PendingMemoryRetrievalError],
    result_count: usize,
) -> MemoryRetrievalStatusState {
    if errors
        .iter()
        .any(|error| !is_recoverable_fragment_error(error))
    {
        MemoryRetrievalStatusState::Error
    } else if !errors.is_empty() {
        MemoryRetrievalStatusState::Partial
    } else if result_count > 0 {
        MemoryRetrievalStatusState::Ready
    } else {
        MemoryRetrievalStatusState::Empty
    }
}

fn rank_fragments<T: Clone>(
    fragments: &[MemoryFragment<T>],
    query: &MemoryRetrievalQuery,
) -> Vec<MemoryFragment<T>> {
    let mut ranked: Vec<_> = fragments
        .iter()
        .filter(|fragment| memory_fragment_matches_query(fragment, &query.memory_query()))
        .cloned()
        .collect();
    if query.vector.is_some() {
        ranked.sort_by(|a, b| {
            vector_score(b, query)
                .total_cmp(&vector_score(a, query))
                .then_with(|| b.confidence.total_cmp(&a.confidence))
                .then_with(|| b.t_ns.cmp(&a.t_ns))
        });
    } else {
        ranked.sort_by(|a, b| {
            b.confidence
                .total_cmp(&a.confidence)
                .then_with(|| b.t_ns.cmp(&a.t_ns))
        });
    }
    if let Some(limit) = query.limit {
        ranked.truncate(limit);
    }
    ranked
}

fn vector_score<T>(fragment: &MemoryFragment<T>, query: &MemoryRetrievalQuery) -> f64 {
    match (&query.vector, &fragment.embedding) {
        (Some(query), Some(embedding)) => cosine_similarity(query, embedding),
        _ => 0.0,
    }
}
