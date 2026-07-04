//! Horizontal graph-visible patterns.
//!
//! D158 allows semantic-memory patterns when they are ordinary graph nodes with
//! declared deps and graph-visible facts. This module intentionally owns no
//! storage restore/hydration, scheduler, vector DB, LLM extraction, retention
//! loop, consolidation loop, or protocol behavior.

use std::collections::{BTreeMap, HashSet};
use std::rc::Rc;

use crate::graph::{Graph, GraphNodeOpts};
use crate::json::JsonValue;
use crate::node::{Node, NodeOpts};
use crate::operators::Operator;

/// Stable identity for a semantic-memory fact.
pub type FactId = String;

/// A single semantic-memory fact. This is pattern vocabulary, not a protocol
/// message, storage record owner, restore contract, or agentic runtime.
#[derive(Clone, Debug, PartialEq)]
pub struct MemoryFragment<T> {
    /// `id` field for id.
    pub id: FactId,
    /// `payload` field for payload.
    pub payload: T,
    /// `t_ns` field for t ns.
    pub t_ns: u128,
    /// `valid_from` field for valid from.
    pub valid_from: Option<u128>,
    /// `valid_to` field for valid to.
    pub valid_to: Option<u128>,
    /// `confidence` field for confidence.
    pub confidence: f64,
    /// `tags` field for tags.
    pub tags: Vec<String>,
    /// `sources` field for sources.
    pub sources: Vec<FactId>,
    /// `embedding` field for embedding.
    pub embedding: Option<Vec<f64>>,
    /// `parent_fragment_id` field for parent fragment id.
    pub parent_fragment_id: Option<FactId>,
    /// `provenance` field for provenance.
    pub provenance: Option<String>,
}

impl<T> MemoryFragment<T> {
    /// Creates or computes `new`.
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

/// Passive lower-layer KG assertion object vocabulary.
///
/// D165 keeps KG assertions independent from the agentic-memory record
/// envelope. Agentic solution bundles may project to this shape, but lower KG
/// reducers do not need `AgenticMemoryRecord`.
#[derive(Clone, Debug, PartialEq)]
pub enum KnowledgeAssertionObject {
    /// `Entity` variant.
    Entity {
        /// `entity_id` field for `Entity`.
        entity_id: FactId,
    },
    /// `Literal` variant.
    Literal {
        /// `value` field for `Literal`.
        value: JsonValue,
    },
}

/// Passive KG assertion fact.
#[derive(Clone, Debug, PartialEq)]
pub struct KnowledgeAssertion {
    /// `id` field for id.
    pub id: FactId,
    /// `subject_id` field for subject id.
    pub subject_id: FactId,
    /// `predicate` field for predicate.
    pub predicate: String,
    /// `object` field for object.
    pub object: KnowledgeAssertionObject,
    /// `sources` field for sources.
    pub sources: Vec<FactId>,
    /// `confidence` field for confidence.
    pub confidence: f64,
    /// `t_ns` field for t ns.
    pub t_ns: u128,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
/// `KnowledgeGraphPolicy` data container.
pub struct KnowledgeGraphPolicy {
    /// `allowed_predicates` field for allowed predicates.
    pub allowed_predicates: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// `KnowledgeGraphEntity` data container.
pub struct KnowledgeGraphEntity {
    /// `id` field for id.
    pub id: FactId,
    /// `assertion_ids` field for assertion ids.
    pub assertion_ids: Vec<FactId>,
    /// `subject_assertion_ids` field for subject assertion ids.
    pub subject_assertion_ids: Vec<FactId>,
    /// `object_assertion_ids` field for object assertion ids.
    pub object_assertion_ids: Vec<FactId>,
}

#[derive(Clone, Debug, PartialEq)]
/// `KnowledgeGraphRelation` data container.
pub struct KnowledgeGraphRelation {
    /// `assertion_id` field for assertion id.
    pub assertion_id: FactId,
    /// `subject_id` field for subject id.
    pub subject_id: FactId,
    /// `predicate` field for predicate.
    pub predicate: String,
    /// `object` field for object.
    pub object: KnowledgeAssertionObject,
    /// `sources` field for sources.
    pub sources: Vec<FactId>,
    /// `confidence` field for confidence.
    pub confidence: f64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// `KnowledgeGraphTopic` data container.
pub struct KnowledgeGraphTopic {
    /// `predicate` field for predicate.
    pub predicate: String,
    /// `assertion_ids` field for assertion ids.
    pub assertion_ids: Vec<FactId>,
    /// `entity_ids` field for entity ids.
    pub entity_ids: Vec<FactId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// `KnowledgeGraphIndex` data container.
pub struct KnowledgeGraphIndex {
    /// `assertion_ids` field for assertion ids.
    pub assertion_ids: Vec<FactId>,
    /// `entity_ids` field for entity ids.
    pub entity_ids: Vec<FactId>,
    /// `relation_ids` field for relation ids.
    pub relation_ids: Vec<FactId>,
    /// `predicates` field for predicates.
    pub predicates: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// `KnowledgeGraphCursor` data container.
pub struct KnowledgeGraphCursor {
    /// `evaluation` field for evaluation.
    pub evaluation: u64,
    /// `valid_assertions` field for valid assertions.
    pub valid_assertions: usize,
    /// `invalid_assertions` field for invalid assertions.
    pub invalid_assertions: usize,
    /// `entity_count` field for entity count.
    pub entity_count: usize,
    /// `relation_count` field for relation count.
    pub relation_count: usize,
    /// `predicate_count` field for predicate count.
    pub predicate_count: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// `KnowledgeGraphStatusState` variants.
pub enum KnowledgeGraphStatusState {
    /// `Ready` variant.
    Ready,
    /// `Empty` variant.
    Empty,
    /// `Partial` variant.
    Partial,
    /// `Error` variant.
    Error,
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// `KnowledgeGraphStatus` data container.
pub struct KnowledgeGraphStatus {
    /// `state` field for state.
    pub state: KnowledgeGraphStatusState,
    /// `cursor` field for cursor.
    pub cursor: KnowledgeGraphCursor,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// `KnowledgeGraphErrorCode` variants.
pub enum KnowledgeGraphErrorCode {
    /// `InvalidAssertion` variant.
    InvalidAssertion,
    /// `DuplicateAssertionId` variant.
    DuplicateAssertionId,
    /// `PolicyConflict` variant.
    PolicyConflict,
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// `KnowledgeGraphError` data container.
pub struct KnowledgeGraphError {
    /// `code` field for code.
    pub code: KnowledgeGraphErrorCode,
    /// `message` field for message.
    pub message: String,
    /// `index` field for index.
    pub index: Option<usize>,
    /// `assertion_id` field for assertion id.
    pub assertion_id: Option<FactId>,
    /// `validation_errors` field for validation errors.
    pub validation_errors: Vec<String>,
    /// `cursor` field for cursor.
    pub cursor: KnowledgeGraphCursor,
}

#[derive(Clone, Debug, PartialEq)]
/// `KnowledgeGraphSnapshot` data container.
pub struct KnowledgeGraphSnapshot {
    /// `assertions` field for assertions.
    pub assertions: Vec<KnowledgeAssertion>,
    /// `entities` field for entities.
    pub entities: Vec<KnowledgeGraphEntity>,
    /// `relations` field for relations.
    pub relations: Vec<KnowledgeGraphRelation>,
    /// `topics` field for topics.
    pub topics: Vec<KnowledgeGraphTopic>,
    /// `index` field for index.
    pub index: KnowledgeGraphIndex,
    /// `status` field for status.
    pub status: KnowledgeGraphStatus,
    /// `errors` field for errors.
    pub errors: Vec<KnowledgeGraphError>,
    /// `cursor` field for cursor.
    pub cursor: KnowledgeGraphCursor,
}

#[derive(Clone)]
/// `KnowledgeGraphReducerBundleOptions` data container.
pub struct KnowledgeGraphReducerBundleOptions {
    /// `name` field for name.
    pub name: Option<String>,
    /// `assertions` field for assertions.
    pub assertions: Node<Vec<KnowledgeAssertion>>,
    /// `policy` field for policy.
    pub policy: Option<Node<KnowledgeGraphPolicy>>,
}

impl KnowledgeGraphReducerBundleOptions {
    /// Creates or computes `new`.
    pub fn new(assertions: Node<Vec<KnowledgeAssertion>>) -> Self {
        Self {
            name: None,
            assertions,
            policy: None,
        }
    }

    /// Updates or reads `named`.
    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Updates or reads `with_policy`.
    pub fn with_policy(mut self, policy: Node<KnowledgeGraphPolicy>) -> Self {
        self.policy = Some(policy);
        self
    }
}

#[derive(Clone)]
/// `KnowledgeGraphReducerBundle` data container.
pub struct KnowledgeGraphReducerBundle {
    /// `assertions_input` field for assertions input.
    pub assertions_input: Node<Vec<KnowledgeAssertion>>,
    /// `policy_input` field for policy input.
    pub policy_input: Option<Node<KnowledgeGraphPolicy>>,
    /// `snapshot` field for snapshot.
    pub snapshot: Node<KnowledgeGraphSnapshot>,
    /// `assertions` field for assertions.
    pub assertions: Node<Vec<KnowledgeAssertion>>,
    /// `entities` field for entities.
    pub entities: Node<Vec<KnowledgeGraphEntity>>,
    /// `relations` field for relations.
    pub relations: Node<Vec<KnowledgeGraphRelation>>,
    /// `topics` field for topics.
    pub topics: Node<Vec<KnowledgeGraphTopic>>,
    /// `index` field for index.
    pub index: Node<KnowledgeGraphIndex>,
    /// `status` field for status.
    pub status: Node<KnowledgeGraphStatus>,
    /// `errors` field for errors.
    pub errors: Node<Vec<KnowledgeGraphError>>,
    /// `cursor` field for cursor.
    pub cursor: Node<KnowledgeGraphCursor>,
}

/// `ShardKey` type alias.
pub type ShardKey = String;

#[derive(Clone, Debug, Default, PartialEq)]
/// `FactStore` data container.
pub struct FactStore<T> {
    /// `by_id` field for by id.
    pub by_id: BTreeMap<FactId, MemoryFragment<T>>,
}

impl<T> FactStore<T> {
    /// Updates or reads `read_handle`.
    pub fn read_handle(&self) -> StoreReadHandle<'_, T> {
        StoreReadHandle { by_id: &self.by_id }
    }
}

#[derive(Clone, Copy, Debug)]
/// `StoreReadHandle` data container.
pub struct StoreReadHandle<'a, T> {
    by_id: &'a BTreeMap<FactId, MemoryFragment<T>>,
}

impl<'a, T> StoreReadHandle<'a, T> {
    /// Updates or reads `get`.
    pub fn get(&self, id: &str) -> Option<&'a MemoryFragment<T>> {
        self.by_id.get(id)
    }

    /// Updates or reads `has`.
    pub fn has(&self, id: &str) -> bool {
        self.by_id.contains_key(id)
    }

    /// Updates or reads `size`.
    pub fn size(&self) -> usize {
        self.by_id.len()
    }

    /// Updates or reads `values`.
    pub fn values(&self) -> impl Iterator<Item = &'a MemoryFragment<T>> {
        self.by_id.values()
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
/// `MemoryQuery` data container.
pub struct MemoryQuery {
    /// `tags` field for tags.
    pub tags: Vec<String>,
    /// `as_of` field for as of.
    pub as_of: Option<u128>,
    /// `min_confidence` field for min confidence.
    pub min_confidence: Option<f64>,
    /// `limit` field for limit.
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// `MemoryFragmentValidation` data container.
pub struct MemoryFragmentValidation {
    /// `ok` field for ok.
    pub ok: bool,
    /// `errors` field for errors.
    pub errors: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
/// `OutcomeSignal` data container.
pub struct OutcomeSignal {
    /// `fact_id` field for fact id.
    pub fact_id: FactId,
    /// `reward` field for reward.
    pub reward: f64,
}

#[derive(Clone, Debug, PartialEq)]
/// `CollectionEntry` data container.
pub struct CollectionEntry<T> {
    /// `id` field for id.
    pub id: String,
    /// `value` field for value.
    pub value: T,
    /// `created_at_ns` field for created at ns.
    pub created_at_ns: u128,
    /// `last_access_ns` field for last access ns.
    pub last_access_ns: u128,
    /// `base_score` field for base score.
    pub base_score: f64,
}

#[derive(Clone, Debug, PartialEq)]
/// `RankedCollectionEntry` data container.
pub struct RankedCollectionEntry<T> {
    /// `entry` field for entry.
    pub entry: CollectionEntry<T>,
    /// `score` field for score.
    pub score: f64,
}

#[derive(Clone, Debug, Default, PartialEq)]
/// `RetrievalQuery` data container.
pub struct RetrievalQuery {
    /// `text` field for text.
    pub text: Option<String>,
    /// `vector` field for vector.
    pub vector: Option<Vec<f64>>,
    /// `entity_ids` field for entity ids.
    pub entity_ids: Vec<String>,
    /// `context` field for context.
    pub context: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
/// `VectorSearchResult` data container.
pub struct VectorSearchResult<TMeta> {
    /// `id` field for id.
    pub id: String,
    /// `score` field for score.
    pub score: f64,
    /// `meta` field for meta.
    pub meta: Option<TMeta>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// `RetrievalEntrySource` variants.
pub enum RetrievalEntrySource {
    /// `Vector` variant.
    Vector,
    /// `Graph` variant.
    Graph,
    /// `Store` variant.
    Store,
}

#[derive(Clone, Debug, PartialEq)]
/// `RetrievalEntry` data container.
pub struct RetrievalEntry<TMem> {
    /// `key` field for key.
    pub key: String,
    /// `value` field for value.
    pub value: TMem,
    /// `score` field for score.
    pub score: f64,
    /// `sources` field for sources.
    pub sources: Vec<RetrievalEntrySource>,
    /// `context` field for context.
    pub context: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
/// `RetrievalTrace` data container.
pub struct RetrievalTrace<TMem> {
    /// `vector_candidates` field for vector candidates.
    pub vector_candidates: Vec<VectorSearchResult<TMem>>,
    /// `graph_expanded` field for graph expanded.
    pub graph_expanded: Vec<String>,
    /// `ranked` field for ranked.
    pub ranked: Vec<RetrievalEntry<TMem>>,
    /// `packed` field for packed.
    pub packed: Vec<RetrievalEntry<TMem>>,
}

/// `AdmissionThresholds` type alias.
pub type AdmissionThresholds = BTreeMap<String, f64>;
/// `AdmissionScoreFn` type alias.
pub type AdmissionScoreFn<TRaw> = Rc<dyn Fn(&TRaw) -> BTreeMap<String, f64>>;
/// `AdmissionScore3DFn` type alias.
pub type AdmissionScore3DFn<TRaw> = Rc<dyn Fn(&TRaw) -> AdmissionScores>;
/// `TenantShardFn` type alias.
pub type TenantShardFn<T> = Rc<dyn Fn(&MemoryFragment<T>) -> String>;
/// `ShardByFn` type alias.
pub type ShardByFn<T> = Rc<dyn Fn(&MemoryFragment<T>) -> ShardKey>;

/// `AdmissionScoredOptions` data container.
pub struct AdmissionScoredOptions<TRaw> {
    /// `score_fn` field for score fn.
    pub score_fn: AdmissionScoreFn<TRaw>,
    /// `thresholds` field for thresholds.
    pub thresholds: AdmissionThresholds,
}

#[derive(Clone, Copy, Debug, PartialEq)]
/// `AdmissionScores` data container.
pub struct AdmissionScores {
    /// `persistence` field for persistence.
    pub persistence: f64,
    /// `structure` field for structure.
    pub structure: f64,
    /// `personal_value` field for personal value.
    pub personal_value: f64,
}

/// `AdmissionScore3DOptions` data container.
pub struct AdmissionScore3DOptions<TRaw> {
    /// `score_fn` field for score fn.
    pub score_fn: AdmissionScore3DFn<TRaw>,
    /// `persistence_threshold` field for persistence threshold.
    pub persistence_threshold: f64,
    /// `personal_value_threshold` field for personal value threshold.
    pub personal_value_threshold: f64,
    /// `require_structured` field for require structured.
    pub require_structured: bool,
}

impl<TRaw> AdmissionScore3DOptions<TRaw> {
    /// Creates or computes `new`.
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
/// `ShardByTenantOptions` data container.
pub struct ShardByTenantOptions {
    /// `tenants` field for tenants.
    pub tenants: Vec<String>,
    /// `shard_count` field for shard count.
    pub shard_count: Option<usize>,
}

/// `ShardByTenantConfig` data container.
pub struct ShardByTenantConfig<T> {
    /// `shard_by` field for shard by.
    pub shard_by: ShardByFn<T>,
    /// `shard_count` field for shard count.
    pub shard_count: usize,
}

/// Creates or computes `cosine_similarity`.
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

/// Creates or computes `memory_fragment_valid_at`.
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

/// Creates or computes `memory_fragment_matches_query`.
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

/// Creates or computes `filter_memory_fragments`.
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

/// Creates or computes `validate_memory_fragment`.
pub fn validate_memory_fragment<T>(fragment: &MemoryFragment<T>) -> MemoryFragmentValidation {
    let errors = validate_fragment(fragment);
    MemoryFragmentValidation {
        ok: errors.is_empty(),
        errors,
    }
}

/// Creates or computes `admission_scored`.
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

/// Creates or computes `admission_filter_3d`.
pub fn admission_filter_3d<TRaw>(opts: AdmissionScore3DOptions<TRaw>) -> impl Fn(&TRaw) -> bool {
    move |raw| {
        let scores = (opts.score_fn)(raw);
        score_at_least(scores.persistence, opts.persistence_threshold)
            && score_at_least(scores.personal_value, opts.personal_value_threshold)
            && (!opts.require_structured || score_at_least(scores.structure, f64::MIN_POSITIVE))
    }
}

/// Creates or computes `shard_by_tenant`.
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
    /// `tags` field for tags.
    pub tags: Vec<String>,
    /// `as_of` field for as of.
    pub as_of: Option<u128>,
    /// `min_confidence` field for min confidence.
    pub min_confidence: Option<f64>,
    /// `limit` field for limit.
    pub limit: Option<usize>,
    /// `vector` field for vector.
    pub vector: Option<Vec<f64>>,
}

impl MemoryRetrievalQuery {
    /// Updates or reads `memory_query`.
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
/// `MemoryRetrievalCursor` data container.
pub struct MemoryRetrievalCursor {
    /// `evaluation` field for evaluation.
    pub evaluation: u64,
    /// `valid_fragments` field for valid fragments.
    pub valid_fragments: usize,
    /// `invalid_fragments` field for invalid fragments.
    pub invalid_fragments: usize,
    /// `result_count` field for result count.
    pub result_count: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// `MemoryRetrievalStatusState` variants.
pub enum MemoryRetrievalStatusState {
    /// `Ready` variant.
    Ready,
    /// `Empty` variant.
    Empty,
    /// `Partial` variant.
    Partial,
    /// `Error` variant.
    Error,
}

#[derive(Clone, Debug, PartialEq)]
/// `MemoryRetrievalStatus` data container.
pub struct MemoryRetrievalStatus {
    /// `state` field for state.
    pub state: MemoryRetrievalStatusState,
    /// `query` field for query.
    pub query: MemoryRetrievalQuery,
    /// `cursor` field for cursor.
    pub cursor: MemoryRetrievalCursor,
}

#[derive(Clone, Debug, PartialEq)]
/// `MemoryRetrievalIndex` data container.
pub struct MemoryRetrievalIndex<T> {
    /// `ids` field for ids.
    pub ids: Vec<FactId>,
    /// `by_id` field for by id.
    pub by_id: BTreeMap<FactId, MemoryFragment<T>>,
    /// `cursor` field for cursor.
    pub cursor: MemoryRetrievalCursor,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// `MemoryRetrievalErrorCode` variants.
pub enum MemoryRetrievalErrorCode {
    /// `DuplicateFragmentId` variant.
    DuplicateFragmentId,
    /// `InvalidFragment` variant.
    InvalidFragment,
    /// `InvalidQuery` variant.
    InvalidQuery,
    /// `InvalidQueryVector` variant.
    InvalidQueryVector,
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// `MemoryRetrievalError` data container.
pub struct MemoryRetrievalError {
    /// `code` field for code.
    pub code: MemoryRetrievalErrorCode,
    /// `message` field for message.
    pub message: String,
    /// `index` field for index.
    pub index: Option<usize>,
    /// `fragment_id` field for fragment id.
    pub fragment_id: Option<FactId>,
    /// `validation_errors` field for validation errors.
    pub validation_errors: Vec<String>,
    /// `cursor` field for cursor.
    pub cursor: MemoryRetrievalCursor,
}

#[derive(Clone, Debug, PartialEq)]
/// `MemoryAnswer` data container.
pub struct MemoryAnswer<T> {
    /// `query` field for query.
    pub query: MemoryRetrievalQuery,
    /// `results` field for results.
    pub results: Vec<MemoryFragment<T>>,
}

#[derive(Clone, Debug, PartialEq)]
/// `MemoryRetrievalSnapshot` data container.
pub struct MemoryRetrievalSnapshot<T> {
    /// `fragments` field for fragments.
    pub fragments: Vec<MemoryFragment<T>>,
    /// `indexed` field for indexed.
    pub indexed: MemoryRetrievalIndex<T>,
    /// `ranked` field for ranked.
    pub ranked: MemoryAnswer<T>,
    /// `status` field for status.
    pub status: MemoryRetrievalStatus,
    /// `errors` field for errors.
    pub errors: Vec<MemoryRetrievalError>,
    /// `cursor` field for cursor.
    pub cursor: MemoryRetrievalCursor,
}

/// Alias for the aggregate DATA fact emitted by the snapshot node.
pub type MemoryRetrievalFact<T> = MemoryRetrievalSnapshot<T>;

#[derive(Clone)]
/// `MemoryRetrievalBundleOptions` data container.
pub struct MemoryRetrievalBundleOptions<T> {
    /// `name` field for name.
    pub name: Option<String>,
    /// `fragments` field for fragments.
    pub fragments: Node<Vec<MemoryFragment<T>>>,
    /// `query` field for query.
    pub query: Node<MemoryRetrievalQuery>,
}

impl<T> MemoryRetrievalBundleOptions<T> {
    /// Creates or computes `new`.
    pub fn new(fragments: Node<Vec<MemoryFragment<T>>>, query: Node<MemoryRetrievalQuery>) -> Self {
        Self {
            name: None,
            fragments,
            query,
        }
    }

    /// Updates or reads `named`.
    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
}

#[derive(Clone)]
/// `MemoryRetrievalBundle` data container.
pub struct MemoryRetrievalBundle<T> {
    /// `fragments_input` field for fragments input.
    pub fragments_input: Node<Vec<MemoryFragment<T>>>,
    /// `query_input` field for query input.
    pub query_input: Node<MemoryRetrievalQuery>,
    /// `snapshot` field for snapshot.
    pub snapshot: Node<MemoryRetrievalSnapshot<T>>,
    /// `fragments` field for fragments.
    pub fragments: Node<Vec<MemoryFragment<T>>>,
    /// `indexed` field for indexed.
    pub indexed: Node<MemoryRetrievalIndex<T>>,
    /// `ranked` field for ranked.
    pub ranked: Node<MemoryAnswer<T>>,
    /// `status` field for status.
    pub status: Node<MemoryRetrievalStatus>,
    /// `errors` field for errors.
    pub errors: Node<Vec<MemoryRetrievalError>>,
    /// `cursor` field for cursor.
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

/// Creates or computes `knowledge_graph_reducer_bundle`.
pub fn knowledge_graph_reducer_bundle(
    graph: &Graph,
    opts: KnowledgeGraphReducerBundleOptions,
) -> KnowledgeGraphReducerBundle {
    let name = opts.name.unwrap_or_else(|| "knowledgeGraph".to_owned());
    let assertions = opts.assertions;
    let policy = opts.policy;
    let mut deps = vec![assertions.erased()];
    if let Some(policy) = &policy {
        deps.push(policy.erased());
    }
    let has_policy = policy.is_some();
    let snapshot = graph.init_node(
        Operator::with_opts(
            "knowledgeGraphReducerSnapshot",
            pattern_node_config(),
            move |ctx| {
                let evaluation = ctx
                    .state_get::<u64>()
                    .map(|evaluation| *evaluation + 1)
                    .unwrap_or(1);
                let raw_assertions = ctx
                    .data::<Vec<KnowledgeAssertion>>(0)
                    .map(|assertions| (*assertions).clone())
                    .unwrap_or_default();
                let policy = if has_policy {
                    ctx.data::<KnowledgeGraphPolicy>(1)
                        .map(|policy| (*policy).clone())
                        .unwrap_or_default()
                } else {
                    KnowledgeGraphPolicy::default()
                };
                let reduced = reduce_knowledge_assertions(raw_assertions, &policy);
                let cursor = KnowledgeGraphCursor {
                    evaluation,
                    valid_assertions: reduced.assertions.len(),
                    invalid_assertions: reduced.errors.len(),
                    entity_count: reduced.entities.len(),
                    relation_count: reduced.relations.len(),
                    predicate_count: reduced.topics.len(),
                };
                let errors = reduced
                    .errors
                    .into_iter()
                    .map(|pending| KnowledgeGraphError {
                        code: pending.code,
                        message: pending.message,
                        index: pending.index,
                        assertion_id: pending.assertion_id,
                        validation_errors: pending.validation_errors,
                        cursor: cursor.clone(),
                    })
                    .collect::<Vec<_>>();
                let status = KnowledgeGraphStatus {
                    state: if reduced.assertions.is_empty() && errors.is_empty() {
                        KnowledgeGraphStatusState::Empty
                    } else if errors.is_empty() {
                        KnowledgeGraphStatusState::Ready
                    } else if !reduced.assertions.is_empty() {
                        KnowledgeGraphStatusState::Partial
                    } else {
                        KnowledgeGraphStatusState::Error
                    },
                    cursor: cursor.clone(),
                };
                ctx.state_set(evaluation);
                ctx.emit(KnowledgeGraphSnapshot {
                    assertions: reduced.assertions,
                    entities: reduced.entities,
                    relations: reduced.relations,
                    topics: reduced.topics,
                    index: reduced.index,
                    status,
                    errors,
                    cursor,
                });
            },
        ),
        deps,
        named_graph_node_opts(format!("{name}/snapshot")),
    );

    KnowledgeGraphReducerBundle {
        assertions_input: assertions,
        policy_input: policy,
        assertions: kg_projection(
            graph,
            &snapshot,
            format!("{name}/assertions"),
            "knowledgeGraphAssertions",
            |snapshot| snapshot.assertions.clone(),
        ),
        entities: kg_projection(
            graph,
            &snapshot,
            format!("{name}/entities"),
            "knowledgeGraphEntities",
            |snapshot| snapshot.entities.clone(),
        ),
        relations: kg_projection(
            graph,
            &snapshot,
            format!("{name}/relations"),
            "knowledgeGraphRelations",
            |snapshot| snapshot.relations.clone(),
        ),
        topics: kg_projection(
            graph,
            &snapshot,
            format!("{name}/topics"),
            "knowledgeGraphTopics",
            |snapshot| snapshot.topics.clone(),
        ),
        index: kg_projection(
            graph,
            &snapshot,
            format!("{name}/index"),
            "knowledgeGraphIndex",
            |snapshot| snapshot.index.clone(),
        ),
        status: kg_projection(
            graph,
            &snapshot,
            format!("{name}/status"),
            "knowledgeGraphStatus",
            |snapshot| snapshot.status.clone(),
        ),
        errors: kg_projection(
            graph,
            &snapshot,
            format!("{name}/errors"),
            "knowledgeGraphErrors",
            |snapshot| snapshot.errors.clone(),
        ),
        cursor: kg_projection(
            graph,
            &snapshot,
            format!("{name}/cursor"),
            "knowledgeGraphCursor",
            |snapshot| snapshot.cursor.clone(),
        ),
        snapshot,
    }
}

fn kg_projection<U, F>(
    graph: &Graph,
    snapshot: &Node<KnowledgeGraphSnapshot>,
    name: String,
    factory: &'static str,
    select: F,
) -> Node<U>
where
    U: 'static,
    F: Fn(&KnowledgeGraphSnapshot) -> U + 'static,
{
    graph.init_node(
        Operator::with_opts(factory, pattern_node_config(), move |ctx| {
            for snapshot in ctx.batch::<KnowledgeGraphSnapshot>(0) {
                ctx.emit(select(snapshot.as_ref()));
            }
        }),
        vec![snapshot.erased()],
        named_graph_node_opts(name),
    )
}

#[derive(Clone, Debug)]
struct PendingKnowledgeGraphError {
    code: KnowledgeGraphErrorCode,
    message: String,
    index: Option<usize>,
    assertion_id: Option<FactId>,
    validation_errors: Vec<String>,
}

struct ReducedKnowledgeGraph {
    assertions: Vec<KnowledgeAssertion>,
    entities: Vec<KnowledgeGraphEntity>,
    relations: Vec<KnowledgeGraphRelation>,
    topics: Vec<KnowledgeGraphTopic>,
    index: KnowledgeGraphIndex,
    errors: Vec<PendingKnowledgeGraphError>,
}

fn reduce_knowledge_assertions(
    raw_assertions: Vec<KnowledgeAssertion>,
    policy: &KnowledgeGraphPolicy,
) -> ReducedKnowledgeGraph {
    let mut assertions = Vec::new();
    let mut errors = Vec::new();
    let mut seen = HashSet::new();
    for (index, assertion) in raw_assertions.into_iter().enumerate() {
        let validation = validate_knowledge_assertion(&assertion, policy);
        if !validation.is_empty() {
            errors.push(PendingKnowledgeGraphError {
                code: if validation.iter().any(|error| error.contains("policy")) {
                    KnowledgeGraphErrorCode::PolicyConflict
                } else {
                    KnowledgeGraphErrorCode::InvalidAssertion
                },
                message: "knowledge_graph_reducer_bundle: assertion is invalid".to_owned(),
                index: Some(index),
                assertion_id: Some(assertion.id.clone()),
                validation_errors: validation,
            });
            continue;
        }
        if !seen.insert(assertion.id.clone()) {
            errors.push(PendingKnowledgeGraphError {
                code: KnowledgeGraphErrorCode::DuplicateAssertionId,
                message: "knowledge_graph_reducer_bundle: duplicate assertion id".to_owned(),
                index: Some(index),
                assertion_id: Some(assertion.id.clone()),
                validation_errors: vec![format!("duplicate assertion id '{}'", assertion.id)],
            });
            continue;
        }
        assertions.push(assertion);
    }
    let (entities, relations, topics, index) = materialize_knowledge_graph(&assertions);
    ReducedKnowledgeGraph {
        assertions,
        entities,
        relations,
        topics,
        index,
        errors,
    }
}

fn validate_knowledge_assertion(
    assertion: &KnowledgeAssertion,
    policy: &KnowledgeGraphPolicy,
) -> Vec<String> {
    let mut errors = Vec::new();
    if assertion.id.is_empty() {
        errors.push("id must be a non-empty string".to_owned());
    }
    if assertion.subject_id.is_empty() {
        errors.push("subject_id must be a non-empty string".to_owned());
    }
    if assertion.predicate.is_empty() {
        errors.push("predicate must be a non-empty string".to_owned());
    }
    if !policy.allowed_predicates.is_empty()
        && !policy.allowed_predicates.contains(&assertion.predicate)
    {
        errors.push(format!(
            "predicate '{}' is rejected by policy",
            assertion.predicate
        ));
    }
    if let KnowledgeAssertionObject::Entity { entity_id } = &assertion.object {
        if entity_id.is_empty() {
            errors.push("object entity_id must be a non-empty string".to_owned());
        }
    }
    if !assertion.confidence.is_finite() || !(0.0..=1.0).contains(&assertion.confidence) {
        errors.push("confidence must be finite in [0, 1]".to_owned());
    }
    errors
}

fn materialize_knowledge_graph(
    assertions: &[KnowledgeAssertion],
) -> (
    Vec<KnowledgeGraphEntity>,
    Vec<KnowledgeGraphRelation>,
    Vec<KnowledgeGraphTopic>,
    KnowledgeGraphIndex,
) {
    type EntityBuckets = (HashSet<FactId>, HashSet<FactId>, HashSet<FactId>);
    type TopicBuckets = (HashSet<FactId>, HashSet<FactId>);

    let mut entity_map: BTreeMap<FactId, EntityBuckets> = BTreeMap::new();
    let mut topic_map: BTreeMap<String, TopicBuckets> = BTreeMap::new();
    let mut relations = Vec::new();
    for assertion in assertions {
        let subject_entry = entity_map.entry(assertion.subject_id.clone()).or_default();
        subject_entry.0.insert(assertion.id.clone());
        subject_entry.1.insert(assertion.id.clone());
        let topic_entry = topic_map.entry(assertion.predicate.clone()).or_default();
        topic_entry.0.insert(assertion.id.clone());
        topic_entry.1.insert(assertion.subject_id.clone());
        if let KnowledgeAssertionObject::Entity { entity_id } = &assertion.object {
            let object_entry = entity_map.entry(entity_id.clone()).or_default();
            object_entry.0.insert(assertion.id.clone());
            object_entry.2.insert(assertion.id.clone());
            topic_entry.1.insert(entity_id.clone());
        }
        relations.push(KnowledgeGraphRelation {
            assertion_id: assertion.id.clone(),
            subject_id: assertion.subject_id.clone(),
            predicate: assertion.predicate.clone(),
            object: assertion.object.clone(),
            sources: assertion.sources.clone(),
            confidence: assertion.confidence,
        });
    }
    relations.sort_by(|a, b| a.assertion_id.cmp(&b.assertion_id));
    let entities = entity_map
        .into_iter()
        .map(
            |(id, (assertions, subjects, objects))| KnowledgeGraphEntity {
                id,
                assertion_ids: sorted_set(assertions),
                subject_assertion_ids: sorted_set(subjects),
                object_assertion_ids: sorted_set(objects),
            },
        )
        .collect::<Vec<_>>();
    let topics = topic_map
        .into_iter()
        .map(|(predicate, (assertions, entities))| KnowledgeGraphTopic {
            predicate,
            assertion_ids: sorted_set(assertions),
            entity_ids: sorted_set(entities),
        })
        .collect::<Vec<_>>();
    let index = KnowledgeGraphIndex {
        assertion_ids: {
            let mut ids = assertions
                .iter()
                .map(|assertion| assertion.id.clone())
                .collect::<Vec<_>>();
            ids.sort();
            ids
        },
        entity_ids: entities.iter().map(|entity| entity.id.clone()).collect(),
        relation_ids: relations
            .iter()
            .map(|relation| relation.assertion_id.clone())
            .collect(),
        predicates: topics.iter().map(|topic| topic.predicate.clone()).collect(),
    };
    (entities, relations, topics, index)
}

fn sorted_set(set: HashSet<FactId>) -> Vec<FactId> {
    let mut out = set.into_iter().collect::<Vec<_>>();
    out.sort();
    out
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
