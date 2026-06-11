//! Vertical graph-visible solutions.
//!
//! D164 keeps the agentic-memory record envelope at solution level. Lower
//! semantic-memory patterns still consume projected [`MemoryFragment`] facts.

use std::collections::BTreeMap;

use crate::graph::{Graph, GraphNodeOpts};
use crate::node::{Node, NodeOpts};
use crate::operators::Operator;
use crate::patterns::{
    memory_retrieval_bundle, validate_memory_fragment, FactId, MemoryAnswer, MemoryFragment,
    MemoryRetrievalBundle, MemoryRetrievalBundleOptions, MemoryRetrievalError,
    MemoryRetrievalIndex, MemoryRetrievalQuery, MemoryRetrievalSnapshot, MemoryRetrievalStatus,
    MemoryRetrievalStatusState,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AgenticMemoryKind {
    Working,
    Episodic,
    Semantic,
    Procedural,
    Profile,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AgenticMemoryPersistenceLevel {
    Turn,
    Session,
    Project,
    LongTerm,
    Permanent,
    Archived,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AgenticMemoryArtifactKind {
    Raw,
    Insight,
    Profile,
    Procedure,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AgenticMemoryScope {
    pub session_id: Option<String>,
    pub project_id: Option<String>,
    pub user_id: Option<String>,
    pub tenant_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AgenticMemoryRecord<T> {
    pub fragment: MemoryFragment<T>,
    pub kind: AgenticMemoryKind,
    pub persistence_level: AgenticMemoryPersistenceLevel,
    pub artifact_kind: AgenticMemoryArtifactKind,
    pub scope: Option<AgenticMemoryScope>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgenticMemoryFieldValidation {
    pub ok: bool,
    pub errors: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgenticMemoryRecordValidation {
    pub ok: bool,
    pub errors: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgenticMemoryRecordMetadata {
    pub kind: AgenticMemoryKind,
    pub persistence_level: AgenticMemoryPersistenceLevel,
    pub artifact_kind: AgenticMemoryArtifactKind,
    pub scope: Option<AgenticMemoryScope>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AgenticMemorySourceProjection {
    pub fragment_id: FactId,
    pub sources: Vec<FactId>,
    pub parent_fragment_id: Option<FactId>,
    pub provenance: Option<String>,
    pub metadata: AgenticMemoryRecordMetadata,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AgenticMemoryStatusState {
    Ready,
    Empty,
    Partial,
    Error,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgenticMemoryCursor {
    pub evaluation: u64,
    pub valid_records: usize,
    pub invalid_records: usize,
    pub projected_fragments: usize,
    pub result_count: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AgenticMemoryStatus {
    pub state: AgenticMemoryStatusState,
    pub query: MemoryRetrievalQuery,
    pub cursor: AgenticMemoryCursor,
    pub retrieval_status: MemoryRetrievalStatus,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AgenticMemoryErrorCode {
    InvalidRecord,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgenticMemoryError {
    pub code: AgenticMemoryErrorCode,
    pub message: String,
    pub index: Option<usize>,
    pub fragment_id: Option<FactId>,
    pub validation_errors: Vec<String>,
    pub cursor: AgenticMemoryCursor,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AgenticMemoryContextEntry<T> {
    pub fragment_id: FactId,
    pub payload: T,
    pub confidence: f64,
    pub tags: Vec<String>,
    pub sources: Vec<FactId>,
    pub fragment: MemoryFragment<T>,
    pub metadata: Option<AgenticMemoryRecordMetadata>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AgenticMemoryContext<T> {
    pub state: AgenticMemoryStatusState,
    pub query: MemoryRetrievalQuery,
    pub entries: Vec<AgenticMemoryContextEntry<T>>,
    pub cursor: AgenticMemoryCursor,
    pub errors: Vec<AgenticMemoryError>,
    pub retrieval_status: MemoryRetrievalStatus,
    pub retrieval_errors: Vec<MemoryRetrievalError>,
    pub context_ready: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AgenticMemoryProjection<T> {
    pub records: Vec<AgenticMemoryRecord<T>>,
    pub fragments: Vec<MemoryFragment<T>>,
    pub metadata_by_fragment_id: BTreeMap<FactId, AgenticMemoryRecordMetadata>,
    pub sources: Vec<AgenticMemorySourceProjection>,
    pub errors: Vec<AgenticMemoryError>,
    pub cursor: AgenticMemoryCursor,
}

#[derive(Clone)]
pub struct AgenticMemoryBundleOptions<T> {
    pub name: Option<String>,
    pub records: Node<Vec<AgenticMemoryRecord<T>>>,
    pub query: Node<MemoryRetrievalQuery>,
}

impl<T> AgenticMemoryBundleOptions<T> {
    pub fn new(
        records: Node<Vec<AgenticMemoryRecord<T>>>,
        query: Node<MemoryRetrievalQuery>,
    ) -> Self {
        Self {
            name: None,
            records,
            query,
        }
    }

    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
}

#[derive(Clone)]
pub struct AgenticMemoryBundle<T> {
    pub records_input: Node<Vec<AgenticMemoryRecord<T>>>,
    pub query_input: Node<MemoryRetrievalQuery>,
    pub projection: Node<AgenticMemoryProjection<T>>,
    pub retrieval: MemoryRetrievalBundle<T>,
    pub retrieval_snapshot: Node<MemoryRetrievalSnapshot<T>>,
    pub retrieval_status: Node<MemoryRetrievalStatus>,
    pub retrieval_errors: Node<Vec<MemoryRetrievalError>>,
    pub fragments: Node<Vec<MemoryFragment<T>>>,
    pub sources: Node<Vec<AgenticMemorySourceProjection>>,
    pub indexed: Node<MemoryRetrievalIndex<T>>,
    pub ranked: Node<MemoryAnswer<T>>,
    pub context: Node<AgenticMemoryContext<T>>,
    pub status: Node<AgenticMemoryStatus>,
    pub errors: Node<Vec<AgenticMemoryError>>,
    pub cursor: Node<AgenticMemoryCursor>,
}

pub fn validate_agentic_memory_kind(_kind: &AgenticMemoryKind) -> AgenticMemoryFieldValidation {
    AgenticMemoryFieldValidation {
        ok: true,
        errors: Vec::new(),
    }
}

pub fn validate_agentic_memory_persistence_level(
    _level: &AgenticMemoryPersistenceLevel,
) -> AgenticMemoryFieldValidation {
    AgenticMemoryFieldValidation {
        ok: true,
        errors: Vec::new(),
    }
}

pub fn validate_agentic_memory_artifact_kind(
    _kind: &AgenticMemoryArtifactKind,
) -> AgenticMemoryFieldValidation {
    AgenticMemoryFieldValidation {
        ok: true,
        errors: Vec::new(),
    }
}

pub fn validate_agentic_memory_scope(scope: &AgenticMemoryScope) -> AgenticMemoryFieldValidation {
    let mut errors = Vec::new();
    if scope
        .session_id
        .as_ref()
        .is_some_and(|value| value.is_empty())
    {
        errors.push("scope.session_id must be a non-empty string when present".to_owned());
    }
    if scope
        .project_id
        .as_ref()
        .is_some_and(|value| value.is_empty())
    {
        errors.push("scope.project_id must be a non-empty string when present".to_owned());
    }
    if scope.user_id.as_ref().is_some_and(|value| value.is_empty()) {
        errors.push("scope.user_id must be a non-empty string when present".to_owned());
    }
    if scope
        .tenant_id
        .as_ref()
        .is_some_and(|value| value.is_empty())
    {
        errors.push("scope.tenant_id must be a non-empty string when present".to_owned());
    }
    AgenticMemoryFieldValidation {
        ok: errors.is_empty(),
        errors,
    }
}

pub fn validate_agentic_memory_record<T>(
    record: &AgenticMemoryRecord<T>,
) -> AgenticMemoryRecordValidation {
    let mut errors = Vec::new();
    let fragment_validation = validate_memory_fragment(&record.fragment);
    errors.extend(
        fragment_validation
            .errors
            .into_iter()
            .map(|error| format!("fragment.{error}")),
    );
    if let Some(scope) = &record.scope {
        errors.extend(validate_agentic_memory_scope(scope).errors);
    }
    AgenticMemoryRecordValidation {
        ok: errors.is_empty(),
        errors,
    }
}

/// Compose D164 agentic-memory records into retrieval and context DATA facts.
///
/// The bundle owns no storage restore/hydration, schedulers, TTL timers, vector
/// DB handles, LLM/tool runners, or protocol behavior. It validates the record
/// envelope, projects valid records to [`MemoryFragment`] facts, then feeds the
/// lower [`memory_retrieval_bundle`] pattern through declared graph deps.
pub fn agentic_memory_bundle<T: Clone + 'static>(
    graph: &Graph,
    opts: AgenticMemoryBundleOptions<T>,
) -> AgenticMemoryBundle<T> {
    let name = opts.name.unwrap_or_else(|| "agenticMemory".to_owned());
    let records = opts.records;
    let query = opts.query;
    let projection = graph.init_node(
        Operator::with_opts("agenticMemoryProjection", solution_node_config(), |ctx| {
            let evaluation = ctx
                .state_get::<u64>()
                .map(|evaluation| *evaluation + 1)
                .unwrap_or(1);
            let raw_records = ctx
                .data::<Vec<AgenticMemoryRecord<T>>>(0)
                .map(|records| (*records).clone())
                .unwrap_or_default();
            let mut records = Vec::new();
            let mut fragments = Vec::new();
            let mut metadata_by_fragment_id = BTreeMap::new();
            let mut sources = Vec::new();
            let mut pending_errors = Vec::<PendingAgenticMemoryError>::new();

            for (index, record) in raw_records.into_iter().enumerate() {
                let validation = validate_agentic_memory_record(&record);
                if !validation.ok {
                    pending_errors.push(PendingAgenticMemoryError {
                        index: Some(index),
                        fragment_id: Some(record.fragment.id.clone()),
                        validation_errors: validation.errors,
                    });
                    continue;
                }
                let metadata = record_metadata(&record);
                metadata_by_fragment_id
                    .entry(record.fragment.id.clone())
                    .or_insert_with(|| metadata.clone());
                sources.push(AgenticMemorySourceProjection {
                    fragment_id: record.fragment.id.clone(),
                    sources: record.fragment.sources.clone(),
                    parent_fragment_id: record.fragment.parent_fragment_id.clone(),
                    provenance: record.fragment.provenance.clone(),
                    metadata,
                });
                fragments.push(record.fragment.clone());
                records.push(record);
            }

            let cursor = AgenticMemoryCursor {
                evaluation,
                valid_records: records.len(),
                invalid_records: pending_errors.len(),
                projected_fragments: fragments.len(),
                result_count: 0,
            };
            ctx.state_set(evaluation);
            let errors = pending_errors
                .into_iter()
                .map(|error| AgenticMemoryError {
                    code: AgenticMemoryErrorCode::InvalidRecord,
                    message: "agentic_memory_bundle: invalid agentic memory record".to_owned(),
                    index: error.index,
                    fragment_id: error.fragment_id,
                    validation_errors: error.validation_errors,
                    cursor: cursor.clone(),
                })
                .collect();
            ctx.emit(AgenticMemoryProjection {
                records,
                fragments,
                metadata_by_fragment_id,
                sources,
                errors,
                cursor,
            });
        }),
        vec![records.erased()],
        named_solution_node_opts(format!("{name}/projection")),
    );
    let fragments = agentic_projection(
        graph,
        &projection,
        format!("{name}/fragments"),
        "agenticMemoryFragments",
        |projection| projection.fragments.clone(),
    );
    let retrieval = memory_retrieval_bundle(
        graph,
        MemoryRetrievalBundleOptions::new(fragments.clone(), query.clone())
            .named(format!("{name}/retrieval")),
    );
    let context = graph.init_node(
        Operator::with_opts("agenticMemoryContext", solution_node_config(), |ctx| {
            let Some(projection) = ctx.data::<AgenticMemoryProjection<T>>(0) else {
                return;
            };
            let Some(snapshot) = ctx.data::<MemoryRetrievalSnapshot<T>>(1) else {
                return;
            };
            ctx.emit(context_from_snapshot(
                projection.as_ref(),
                snapshot.as_ref(),
            ));
        }),
        vec![projection.erased(), retrieval.snapshot.erased()],
        named_solution_node_opts(format!("{name}/context")),
    );

    AgenticMemoryBundle {
        records_input: records,
        query_input: query,
        sources: agentic_projection(
            graph,
            &projection,
            format!("{name}/sources"),
            "agenticMemorySources",
            |projection| projection.sources.clone(),
        ),
        status: context_projection(
            graph,
            &context,
            format!("{name}/status"),
            "agenticMemoryStatus",
            |context| AgenticMemoryStatus {
                state: context.state,
                query: context.query.clone(),
                cursor: context.cursor.clone(),
                retrieval_status: context.retrieval_status.clone(),
            },
        ),
        errors: context_projection(
            graph,
            &context,
            format!("{name}/errors"),
            "agenticMemoryErrors",
            |context| context.errors.clone(),
        ),
        cursor: context_projection(
            graph,
            &context,
            format!("{name}/cursor"),
            "agenticMemoryCursor",
            |context| context.cursor.clone(),
        ),
        indexed: retrieval.indexed.clone(),
        ranked: retrieval.ranked.clone(),
        retrieval_snapshot: retrieval.snapshot.clone(),
        retrieval_status: retrieval.status.clone(),
        retrieval_errors: retrieval.errors.clone(),
        projection,
        fragments,
        context,
        retrieval,
    }
}

fn agentic_projection<T, U, F>(
    graph: &Graph,
    projection: &Node<AgenticMemoryProjection<T>>,
    name: String,
    factory: &'static str,
    select: F,
) -> Node<U>
where
    T: Clone + 'static,
    U: 'static,
    F: Fn(&AgenticMemoryProjection<T>) -> U + 'static,
{
    graph.init_node(
        Operator::with_opts(factory, solution_node_config(), move |ctx| {
            for projection in ctx.batch::<AgenticMemoryProjection<T>>(0) {
                ctx.emit(select(projection.as_ref()));
            }
        }),
        vec![projection.erased()],
        named_solution_node_opts(name),
    )
}

fn context_projection<T, U, F>(
    graph: &Graph,
    context: &Node<AgenticMemoryContext<T>>,
    name: String,
    factory: &'static str,
    select: F,
) -> Node<U>
where
    T: Clone + 'static,
    U: 'static,
    F: Fn(&AgenticMemoryContext<T>) -> U + 'static,
{
    graph.init_node(
        Operator::with_opts(factory, solution_node_config(), move |ctx| {
            for context in ctx.batch::<AgenticMemoryContext<T>>(0) {
                ctx.emit(select(context.as_ref()));
            }
        }),
        vec![context.erased()],
        named_solution_node_opts(name),
    )
}

fn solution_node_config() -> NodeOpts {
    NodeOpts {
        complete_when_deps_complete: false,
        error_when_deps_error: false,
        ..NodeOpts::default()
    }
}

fn named_solution_node_opts(name: String) -> GraphNodeOpts {
    GraphNodeOpts {
        name: Some(name),
        ..GraphNodeOpts::default()
    }
}

fn record_metadata<T>(record: &AgenticMemoryRecord<T>) -> AgenticMemoryRecordMetadata {
    AgenticMemoryRecordMetadata {
        kind: record.kind,
        persistence_level: record.persistence_level,
        artifact_kind: record.artifact_kind,
        scope: record.scope.clone(),
    }
}

fn context_from_snapshot<T: Clone>(
    projection: &AgenticMemoryProjection<T>,
    snapshot: &MemoryRetrievalSnapshot<T>,
) -> AgenticMemoryContext<T> {
    let cursor = AgenticMemoryCursor {
        evaluation: snapshot.cursor.evaluation,
        valid_records: projection.cursor.valid_records,
        invalid_records: projection.cursor.invalid_records,
        projected_fragments: projection.cursor.projected_fragments,
        result_count: snapshot.cursor.result_count,
    };
    let errors = projection.errors.clone();
    let state = agentic_status_state(projection, snapshot);
    let entries = snapshot
        .ranked
        .results
        .iter()
        .map(|fragment| AgenticMemoryContextEntry {
            fragment_id: fragment.id.clone(),
            payload: fragment.payload.clone(),
            confidence: fragment.confidence,
            tags: fragment.tags.clone(),
            sources: fragment.sources.clone(),
            fragment: fragment.clone(),
            metadata: projection
                .metadata_by_fragment_id
                .get(&fragment.id)
                .cloned(),
        })
        .collect::<Vec<_>>();
    let context_ready = !entries.is_empty()
        && matches!(
            state,
            AgenticMemoryStatusState::Ready | AgenticMemoryStatusState::Partial
        );
    AgenticMemoryContext {
        state,
        query: snapshot.ranked.query.clone(),
        entries,
        cursor,
        errors,
        retrieval_status: snapshot.status.clone(),
        retrieval_errors: snapshot.errors.clone(),
        context_ready,
    }
}

fn agentic_status_state<T>(
    projection: &AgenticMemoryProjection<T>,
    snapshot: &MemoryRetrievalSnapshot<T>,
) -> AgenticMemoryStatusState {
    if snapshot.status.state == MemoryRetrievalStatusState::Error {
        return AgenticMemoryStatusState::Error;
    }
    if !projection.errors.is_empty() && projection.cursor.valid_records == 0 {
        return AgenticMemoryStatusState::Error;
    }
    if !projection.errors.is_empty() || snapshot.status.state == MemoryRetrievalStatusState::Partial
    {
        return AgenticMemoryStatusState::Partial;
    }
    if snapshot.cursor.result_count > 0 {
        AgenticMemoryStatusState::Ready
    } else {
        AgenticMemoryStatusState::Empty
    }
}

#[derive(Clone, Debug)]
struct PendingAgenticMemoryError {
    index: Option<usize>,
    fragment_id: Option<FactId>,
    validation_errors: Vec<String>,
}
