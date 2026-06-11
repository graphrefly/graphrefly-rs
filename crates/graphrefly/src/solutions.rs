//! Vertical graph-visible solutions.
//!
//! D164 keeps the agentic-memory record envelope at solution level. Lower
//! semantic-memory patterns still consume projected [`MemoryFragment`] facts.

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::graph::{Graph, GraphNodeOpts};
use crate::json::{
    non_negative_decimal_string_to_u128, strict_canonical_json_bytes, strict_json_decode,
    u128_to_non_negative_decimal_string, validate_strict_json_value, Codec, JsonCodecError,
    JsonCodecResult, JsonValue,
};
use crate::node::{Node, NodeOpts};
use crate::operators::Operator;
use crate::patterns::{
    memory_retrieval_bundle, validate_memory_fragment, FactId, KnowledgeAssertion,
    KnowledgeAssertionObject, MemoryAnswer, MemoryFragment, MemoryRetrievalBundle,
    MemoryRetrievalBundleOptions, MemoryRetrievalError, MemoryRetrievalIndex, MemoryRetrievalQuery,
    MemoryRetrievalSnapshot, MemoryRetrievalStatus, MemoryRetrievalStatusState,
};
use serde_json::{Map as JsonMap, Number as JsonNumber};

pub const AGENTIC_MEMORY_RECORD_FRAME_FORMAT: &str = "graphrefly.agenticMemoryRecord";
pub const AGENTIC_MEMORY_RECORD_FRAME_VERSION: u32 = 1;

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
    pub id: FactId,
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
    pub record_id: FactId,
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
    DuplicateRecordId,
    DuplicateFragmentId,
    InvalidRecord,
    InvalidScope,
    InvalidFragment,
    InvalidKgDraft,
    DuplicateAssertionId,
    InvalidRetentionCommand,
    DuplicateRetentionCommandId,
    InvalidConsolidationOutcome,
    DuplicateConsolidationOutcomeId,
    MissingConsolidationRequest,
    InvalidPackingPolicy,
    InvalidTextProjection,
    DuplicateTextProjection,
    MissingTextProjection,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgenticMemoryError {
    pub code: AgenticMemoryErrorCode,
    pub message: String,
    pub index: Option<usize>,
    pub record_id: Option<FactId>,
    pub fragment_id: Option<FactId>,
    pub validation_errors: Vec<String>,
    pub cursor: AgenticMemoryCursor,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AgenticMemoryRecordFrame {
    pub format: String,
    pub version: u32,
    pub record: AgenticMemoryRecord<JsonValue>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct AgenticMemoryRecordFrameCodec;

pub fn agentic_memory_record_frame(
    record: AgenticMemoryRecord<JsonValue>,
) -> AgenticMemoryRecordFrame {
    AgenticMemoryRecordFrame {
        format: AGENTIC_MEMORY_RECORD_FRAME_FORMAT.to_owned(),
        version: AGENTIC_MEMORY_RECORD_FRAME_VERSION,
        record,
    }
}

pub fn agentic_memory_record_frame_codec() -> AgenticMemoryRecordFrameCodec {
    AgenticMemoryRecordFrameCodec
}

#[derive(Clone, Debug, PartialEq)]
pub struct AgenticMemoryKgAssertionDraft {
    pub id: FactId,
    pub record_id: Option<FactId>,
    pub fragment_id: Option<FactId>,
    pub subject_id: FactId,
    pub predicate: String,
    pub object: KnowledgeAssertionObject,
    pub confidence: f64,
    pub t_ns: u128,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgenticMemoryKgProjectionCursor {
    pub evaluation: u64,
    pub valid_records: usize,
    pub valid_drafts: usize,
    pub invalid_drafts: usize,
    pub projected_assertions: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AgenticMemoryKgProjectionStatus {
    pub state: AgenticMemoryStatusState,
    pub cursor: AgenticMemoryKgProjectionCursor,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AgenticMemoryKgProjectionSnapshot {
    pub assertions: Vec<KnowledgeAssertion>,
    pub status: AgenticMemoryKgProjectionStatus,
    pub errors: Vec<AgenticMemoryError>,
    pub cursor: AgenticMemoryKgProjectionCursor,
}

#[derive(Clone)]
pub struct AgenticMemoryKgProjectionBundleOptions<T> {
    pub name: Option<String>,
    pub records: Node<Vec<AgenticMemoryRecord<T>>>,
    pub drafts: Node<Vec<AgenticMemoryKgAssertionDraft>>,
}

impl<T> AgenticMemoryKgProjectionBundleOptions<T> {
    pub fn new(
        records: Node<Vec<AgenticMemoryRecord<T>>>,
        drafts: Node<Vec<AgenticMemoryKgAssertionDraft>>,
    ) -> Self {
        Self {
            name: None,
            records,
            drafts,
        }
    }

    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
}

#[derive(Clone)]
pub struct AgenticMemoryKgProjectionBundle<T> {
    pub records_input: Node<Vec<AgenticMemoryRecord<T>>>,
    pub drafts_input: Node<Vec<AgenticMemoryKgAssertionDraft>>,
    pub snapshot: Node<AgenticMemoryKgProjectionSnapshot>,
    pub assertions: Node<Vec<KnowledgeAssertion>>,
    pub status: Node<AgenticMemoryKgProjectionStatus>,
    pub errors: Node<Vec<AgenticMemoryError>>,
    pub cursor: Node<AgenticMemoryKgProjectionCursor>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AgenticMemoryRetentionCommandKind {
    Archive,
    Restore,
    RequestConsolidation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgenticMemoryRetentionCommand {
    pub id: FactId,
    pub record_id: FactId,
    pub kind: AgenticMemoryRetentionCommandKind,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgenticMemoryConsolidationRequest {
    pub command_id: FactId,
    pub record_id: FactId,
    pub fragment_id: FactId,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgenticMemoryRetentionCursor {
    pub evaluation: u64,
    pub valid_records: usize,
    pub valid_commands: usize,
    pub invalid_commands: usize,
    pub active_records: usize,
    pub archived_records: usize,
    pub consolidation_requests: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AgenticMemoryRetentionStatus {
    pub state: AgenticMemoryStatusState,
    pub cursor: AgenticMemoryRetentionCursor,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AgenticMemoryRetentionSnapshot<T> {
    pub active_records: Vec<AgenticMemoryRecord<T>>,
    pub archived_records: Vec<AgenticMemoryRecord<T>>,
    pub consolidation_requests: Vec<AgenticMemoryConsolidationRequest>,
    pub status: AgenticMemoryRetentionStatus,
    pub errors: Vec<AgenticMemoryError>,
    pub cursor: AgenticMemoryRetentionCursor,
}

#[derive(Clone)]
pub struct AgenticMemoryRetentionBundleOptions<T> {
    pub name: Option<String>,
    pub records: Node<Vec<AgenticMemoryRecord<T>>>,
    pub commands: Node<Vec<AgenticMemoryRetentionCommand>>,
}

impl<T> AgenticMemoryRetentionBundleOptions<T> {
    pub fn new(
        records: Node<Vec<AgenticMemoryRecord<T>>>,
        commands: Node<Vec<AgenticMemoryRetentionCommand>>,
    ) -> Self {
        Self {
            name: None,
            records,
            commands,
        }
    }

    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
}

#[derive(Clone)]
pub struct AgenticMemoryRetentionBundle<T> {
    pub records_input: Node<Vec<AgenticMemoryRecord<T>>>,
    pub commands_input: Node<Vec<AgenticMemoryRetentionCommand>>,
    pub snapshot: Node<AgenticMemoryRetentionSnapshot<T>>,
    pub active_records: Node<Vec<AgenticMemoryRecord<T>>>,
    pub archived_records: Node<Vec<AgenticMemoryRecord<T>>>,
    pub consolidation_requests: Node<Vec<AgenticMemoryConsolidationRequest>>,
    pub status: Node<AgenticMemoryRetentionStatus>,
    pub errors: Node<Vec<AgenticMemoryError>>,
    pub cursor: Node<AgenticMemoryRetentionCursor>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum AgenticMemoryConsolidationOutcome<T> {
    ProposedRecords {
        id: FactId,
        request_id: FactId,
        records: Vec<AgenticMemoryRecord<T>>,
        provenance: Option<String>,
    },
    Failed {
        id: FactId,
        request_id: FactId,
        message: String,
        provenance: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct AgenticMemoryConsolidationRecordDraft<T> {
    pub id: FactId,
    pub request_id: FactId,
    pub outcome_id: FactId,
    pub record: AgenticMemoryRecord<T>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgenticMemoryConsolidationCommandKind {
    ProposeRecords,
    MarkFailed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgenticMemoryConsolidationCommand {
    pub id: FactId,
    pub kind: AgenticMemoryConsolidationCommandKind,
    pub request_id: FactId,
    pub outcome_id: FactId,
    pub draft_ids: Vec<FactId>,
    pub message: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgenticMemoryConsolidationResultState {
    Proposed,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgenticMemoryConsolidationResult {
    pub id: FactId,
    pub request_id: FactId,
    pub outcome_id: FactId,
    pub state: AgenticMemoryConsolidationResultState,
    pub source_record_ids: Vec<FactId>,
    pub proposed_record_ids: Vec<FactId>,
    pub message: Option<String>,
    pub provenance: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgenticMemoryConsolidationCursor {
    pub evaluation: u64,
    pub valid_requests: usize,
    pub valid_outcomes: usize,
    pub invalid_outcomes: usize,
    pub results: usize,
    pub proposed_record_drafts: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AgenticMemoryConsolidationStatus {
    pub state: AgenticMemoryStatusState,
    pub cursor: AgenticMemoryConsolidationCursor,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AgenticMemoryConsolidationSnapshot<T> {
    pub results: Vec<AgenticMemoryConsolidationResult>,
    pub proposed_record_drafts: Vec<AgenticMemoryConsolidationRecordDraft<T>>,
    pub commands: Vec<AgenticMemoryConsolidationCommand>,
    pub status: AgenticMemoryConsolidationStatus,
    pub errors: Vec<AgenticMemoryError>,
    pub cursor: AgenticMemoryConsolidationCursor,
}

#[derive(Clone)]
pub struct AgenticMemoryConsolidationBundleOptions<T> {
    pub name: Option<String>,
    pub requests: Node<Vec<AgenticMemoryConsolidationRequest>>,
    pub outcomes: Node<Vec<AgenticMemoryConsolidationOutcome<T>>>,
}

impl<T> AgenticMemoryConsolidationBundleOptions<T> {
    pub fn new(
        requests: Node<Vec<AgenticMemoryConsolidationRequest>>,
        outcomes: Node<Vec<AgenticMemoryConsolidationOutcome<T>>>,
    ) -> Self {
        Self {
            name: None,
            requests,
            outcomes,
        }
    }

    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
}

#[derive(Clone)]
pub struct AgenticMemoryConsolidationBundle<T> {
    pub requests_input: Node<Vec<AgenticMemoryConsolidationRequest>>,
    pub outcomes_input: Node<Vec<AgenticMemoryConsolidationOutcome<T>>>,
    pub snapshot: Node<AgenticMemoryConsolidationSnapshot<T>>,
    pub results: Node<Vec<AgenticMemoryConsolidationResult>>,
    pub proposed_record_drafts: Node<Vec<AgenticMemoryConsolidationRecordDraft<T>>>,
    pub commands: Node<Vec<AgenticMemoryConsolidationCommand>>,
    pub status: Node<AgenticMemoryConsolidationStatus>,
    pub errors: Node<Vec<AgenticMemoryError>>,
    pub cursor: Node<AgenticMemoryConsolidationCursor>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgenticMemoryTextProjection {
    pub fragment_id: FactId,
    pub text: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgenticMemoryContextPackingPolicy {
    pub max_chars: Option<usize>,
    pub separator: String,
    pub include_fragment_ids: bool,
}

impl Default for AgenticMemoryContextPackingPolicy {
    fn default() -> Self {
        Self {
            max_chars: None,
            separator: "\n\n".to_owned(),
            include_fragment_ids: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgenticMemoryContextPackingCursor {
    pub evaluation: u64,
    pub context_entries: usize,
    pub text_projection_count: usize,
    pub packed_entries: usize,
    pub missing_text: usize,
    pub char_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgenticMemoryPackedContext {
    pub text: String,
    pub fragment_ids: Vec<FactId>,
    pub truncated: bool,
    pub cursor: AgenticMemoryContextPackingCursor,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AgenticMemoryContextPackingStatus {
    pub state: AgenticMemoryStatusState,
    pub cursor: AgenticMemoryContextPackingCursor,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AgenticMemoryContextPackingSnapshot {
    pub packed_context: AgenticMemoryPackedContext,
    pub status: AgenticMemoryContextPackingStatus,
    pub errors: Vec<AgenticMemoryError>,
    pub cursor: AgenticMemoryContextPackingCursor,
}

#[derive(Clone)]
pub struct AgenticMemoryContextPackingBundleOptions<T> {
    pub name: Option<String>,
    pub context: Node<AgenticMemoryContext<T>>,
    pub texts: Node<Vec<AgenticMemoryTextProjection>>,
    pub policy: Node<AgenticMemoryContextPackingPolicy>,
}

impl<T> AgenticMemoryContextPackingBundleOptions<T> {
    pub fn new(
        context: Node<AgenticMemoryContext<T>>,
        texts: Node<Vec<AgenticMemoryTextProjection>>,
        policy: Node<AgenticMemoryContextPackingPolicy>,
    ) -> Self {
        Self {
            name: None,
            context,
            texts,
            policy,
        }
    }

    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
}

#[derive(Clone)]
pub struct AgenticMemoryContextPackingBundle<T> {
    pub context_input: Node<AgenticMemoryContext<T>>,
    pub texts_input: Node<Vec<AgenticMemoryTextProjection>>,
    pub policy_input: Node<AgenticMemoryContextPackingPolicy>,
    pub snapshot: Node<AgenticMemoryContextPackingSnapshot>,
    pub packed_context: Node<AgenticMemoryPackedContext>,
    pub status: Node<AgenticMemoryContextPackingStatus>,
    pub errors: Node<Vec<AgenticMemoryError>>,
    pub cursor: Node<AgenticMemoryContextPackingCursor>,
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
    if record.id.is_empty() {
        errors.push("id must be a non-empty string".to_owned());
    }
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
            let mut seen_record_ids = HashSet::<FactId>::new();
            let mut seen_fragment_ids = HashSet::<FactId>::new();

            for (index, record) in raw_records.into_iter().enumerate() {
                let validation = validate_agentic_memory_record(&record);
                if !validation.ok {
                    pending_errors.push(PendingAgenticMemoryError {
                        code: AgenticMemoryErrorCode::InvalidRecord,
                        index: Some(index),
                        record_id: Some(record.id.clone()),
                        fragment_id: Some(record.fragment.id.clone()),
                        validation_errors: validation.errors,
                    });
                    continue;
                }
                if !seen_record_ids.insert(record.id.clone()) {
                    pending_errors.push(PendingAgenticMemoryError {
                        code: AgenticMemoryErrorCode::DuplicateRecordId,
                        index: Some(index),
                        record_id: Some(record.id.clone()),
                        fragment_id: Some(record.fragment.id.clone()),
                        validation_errors: vec![format!("duplicate record id '{}'", record.id)],
                    });
                    continue;
                }
                if !seen_fragment_ids.insert(record.fragment.id.clone()) {
                    pending_errors.push(PendingAgenticMemoryError {
                        code: AgenticMemoryErrorCode::DuplicateFragmentId,
                        index: Some(index),
                        record_id: Some(record.id.clone()),
                        fragment_id: Some(record.fragment.id.clone()),
                        validation_errors: vec![format!(
                            "duplicate fragment id '{}'",
                            record.fragment.id
                        )],
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
                    code: error.code,
                    message: "agentic_memory_bundle: invalid agentic memory record".to_owned(),
                    index: error.index,
                    record_id: error.record_id,
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

pub fn agentic_memory_kg_projection_bundle<T: Clone + 'static>(
    graph: &Graph,
    opts: AgenticMemoryKgProjectionBundleOptions<T>,
) -> AgenticMemoryKgProjectionBundle<T> {
    let name = opts
        .name
        .unwrap_or_else(|| "agenticMemoryKgProjection".to_owned());
    let records = opts.records;
    let drafts = opts.drafts;
    let snapshot = graph.init_node(
        Operator::with_opts("agenticMemoryKgProjection", solution_node_config(), |ctx| {
            let evaluation = ctx
                .state_get::<u64>()
                .map(|evaluation| *evaluation + 1)
                .unwrap_or(1);
            let raw_records = ctx
                .data::<Vec<AgenticMemoryRecord<T>>>(0)
                .map(|records| (*records).clone())
                .unwrap_or_default();
            let raw_drafts = ctx
                .data::<Vec<AgenticMemoryKgAssertionDraft>>(1)
                .map(|drafts| (*drafts).clone())
                .unwrap_or_default();
            let valid_records = valid_record_index(raw_records);
            let invalid_record_errors = valid_records.errors.len();
            let fragment_ids = valid_records
                .by_record_id
                .values()
                .map(|record| record.fragment.id.clone())
                .collect::<HashSet<_>>();
            let mut seen_assertion_ids = HashSet::<FactId>::new();
            let mut assertions = Vec::new();
            let mut pending_errors = valid_records.errors;

            for (index, draft) in raw_drafts.into_iter().enumerate() {
                let mut validation_errors = validate_kg_draft(&draft);
                if seen_assertion_ids.contains(&draft.id) {
                    validation_errors.push(format!("duplicate assertion id '{}'", draft.id));
                }
                if let Some(record_id) = &draft.record_id {
                    if !valid_records.by_record_id.contains_key(record_id) {
                        validation_errors.push(format!(
                            "record_id '{record_id}' does not reference a valid record"
                        ));
                    }
                }
                if let Some(fragment_id) = &draft.fragment_id {
                    if !fragment_ids.contains(fragment_id) {
                        validation_errors.push(format!(
                            "fragment_id '{fragment_id}' does not reference a valid fragment"
                        ));
                    }
                }
                if let (Some(record_id), Some(fragment_id)) =
                    (&draft.record_id, &draft.fragment_id)
                {
                    if let Some(record) = valid_records.by_record_id.get(record_id) {
                        if record.fragment.id != *fragment_id {
                            validation_errors.push(format!(
                                "fragment_id '{fragment_id}' is not owned by record_id '{record_id}'"
                            ));
                        }
                    }
                }
                if draft.record_id.is_none() && draft.fragment_id.is_none() {
                    validation_errors
                        .push("draft must reference record_id or fragment_id".to_owned());
                }
                if !validation_errors.is_empty() {
                    pending_errors.push(PendingAgenticMemoryError {
                        code: if validation_errors
                            .iter()
                            .any(|error| error.starts_with("duplicate assertion id"))
                        {
                            AgenticMemoryErrorCode::DuplicateAssertionId
                        } else {
                            AgenticMemoryErrorCode::InvalidKgDraft
                        },
                        index: Some(index),
                        record_id: draft.record_id.clone(),
                        fragment_id: draft.fragment_id.clone(),
                        validation_errors,
                    });
                    continue;
                }
                seen_assertion_ids.insert(draft.id.clone());
                let mut sources = Vec::new();
                if let Some(record_id) = &draft.record_id {
                    if let Some(record) = valid_records.by_record_id.get(record_id) {
                        push_unique(&mut sources, record.fragment.id.clone());
                    }
                }
                if let Some(fragment_id) = &draft.fragment_id {
                    push_unique(&mut sources, fragment_id.clone());
                }
                assertions.push(KnowledgeAssertion {
                    id: draft.id,
                    subject_id: draft.subject_id,
                    predicate: draft.predicate,
                    object: draft.object,
                    sources,
                    confidence: draft.confidence,
                    t_ns: draft.t_ns,
                });
            }
            let cursor = AgenticMemoryKgProjectionCursor {
                evaluation,
                valid_records: valid_records.by_record_id.len(),
                valid_drafts: assertions.len(),
                invalid_drafts: pending_errors.len().saturating_sub(invalid_record_errors),
                projected_assertions: assertions.len(),
            };
            let status = AgenticMemoryKgProjectionStatus {
                state: projection_state(
                    pending_errors.len(),
                    assertions.len(),
                    valid_records.by_record_id.len(),
                ),
                cursor: cursor.clone(),
            };
            ctx.state_set(evaluation);
            let errors = pending_errors
                .into_iter()
                .map(|error| AgenticMemoryError {
                    code: error.code,
                    message: "agentic_memory_kg_projection_bundle: invalid KG assertion draft"
                        .to_owned(),
                    index: error.index,
                    record_id: error.record_id,
                    fragment_id: error.fragment_id,
                    validation_errors: error.validation_errors,
                    cursor: kg_error_cursor(&cursor),
                })
                .collect();
            ctx.emit(AgenticMemoryKgProjectionSnapshot {
                assertions,
                status,
                errors,
                cursor,
            });
        }),
        vec![records.erased(), drafts.erased()],
        named_solution_node_opts(format!("{name}/snapshot")),
    );

    AgenticMemoryKgProjectionBundle {
        records_input: records,
        drafts_input: drafts,
        assertions: solution_projection(
            graph,
            &snapshot,
            format!("{name}/assertions"),
            "agenticMemoryKgAssertions",
            |snapshot: &AgenticMemoryKgProjectionSnapshot| snapshot.assertions.clone(),
        ),
        status: solution_projection(
            graph,
            &snapshot,
            format!("{name}/status"),
            "agenticMemoryKgStatus",
            |snapshot: &AgenticMemoryKgProjectionSnapshot| snapshot.status.clone(),
        ),
        errors: solution_projection(
            graph,
            &snapshot,
            format!("{name}/errors"),
            "agenticMemoryKgErrors",
            |snapshot: &AgenticMemoryKgProjectionSnapshot| snapshot.errors.clone(),
        ),
        cursor: solution_projection(
            graph,
            &snapshot,
            format!("{name}/cursor"),
            "agenticMemoryKgCursor",
            |snapshot: &AgenticMemoryKgProjectionSnapshot| snapshot.cursor.clone(),
        ),
        snapshot,
    }
}

pub fn agentic_memory_retention_bundle<T: Clone + 'static>(
    graph: &Graph,
    opts: AgenticMemoryRetentionBundleOptions<T>,
) -> AgenticMemoryRetentionBundle<T> {
    let name = opts
        .name
        .unwrap_or_else(|| "agenticMemoryRetention".to_owned());
    let records = opts.records;
    let commands = opts.commands;
    let snapshot = graph.init_node(
        Operator::with_opts("agenticMemoryRetention", solution_node_config(), |ctx| {
            let evaluation = ctx
                .state_get::<u64>()
                .map(|evaluation| *evaluation + 1)
                .unwrap_or(1);
            let raw_records = ctx
                .data::<Vec<AgenticMemoryRecord<T>>>(0)
                .map(|records| (*records).clone())
                .unwrap_or_default();
            let raw_commands = ctx
                .data::<Vec<AgenticMemoryRetentionCommand>>(1)
                .map(|commands| (*commands).clone())
                .unwrap_or_default();
            let valid_records = valid_record_index(raw_records);
            let invalid_record_errors = valid_records.errors.len();
            let mut pending_errors = valid_records.errors;
            let mut archived = HashSet::<FactId>::new();
            let mut consolidation_requests = Vec::new();
            let mut seen_command_ids = HashSet::<FactId>::new();
            let mut valid_commands = 0usize;

            for (index, command) in raw_commands.into_iter().enumerate() {
                let mut validation_errors = validate_retention_command(&command);
                if seen_command_ids.contains(&command.id) {
                    validation_errors
                        .push(format!("duplicate retention command id '{}'", command.id));
                }
                if !valid_records.by_record_id.contains_key(&command.record_id) {
                    validation_errors.push(format!(
                        "record_id '{}' does not reference a valid record",
                        command.record_id
                    ));
                }
                if !validation_errors.is_empty() {
                    pending_errors.push(PendingAgenticMemoryError {
                        code: if validation_errors
                            .iter()
                            .any(|error| error.starts_with("duplicate retention command id"))
                        {
                            AgenticMemoryErrorCode::DuplicateRetentionCommandId
                        } else {
                            AgenticMemoryErrorCode::InvalidRetentionCommand
                        },
                        index: Some(index),
                        record_id: Some(command.record_id),
                        fragment_id: None,
                        validation_errors,
                    });
                    continue;
                }
                seen_command_ids.insert(command.id.clone());
                valid_commands += 1;
                match command.kind {
                    AgenticMemoryRetentionCommandKind::Archive => {
                        archived.insert(command.record_id);
                    }
                    AgenticMemoryRetentionCommandKind::Restore => {
                        archived.remove(&command.record_id);
                    }
                    AgenticMemoryRetentionCommandKind::RequestConsolidation => {
                        let record = valid_records
                            .by_record_id
                            .get(&command.record_id)
                            .expect("command validation checked record existence");
                        consolidation_requests.push(AgenticMemoryConsolidationRequest {
                            command_id: command.id,
                            record_id: command.record_id,
                            fragment_id: record.fragment.id.clone(),
                            reason: command.reason,
                        });
                    }
                }
            }

            let mut active_records = Vec::new();
            let mut archived_records = Vec::new();
            for record in valid_records.in_order {
                if archived.contains(&record.id) {
                    archived_records.push(record);
                } else {
                    active_records.push(record);
                }
            }
            let cursor = AgenticMemoryRetentionCursor {
                evaluation,
                valid_records: active_records.len() + archived_records.len(),
                valid_commands,
                invalid_commands: pending_errors.len().saturating_sub(invalid_record_errors),
                active_records: active_records.len(),
                archived_records: archived_records.len(),
                consolidation_requests: consolidation_requests.len(),
            };
            let status = AgenticMemoryRetentionStatus {
                state: projection_state(
                    pending_errors.len(),
                    active_records.len() + archived_records.len(),
                    active_records.len() + archived_records.len(),
                ),
                cursor: cursor.clone(),
            };
            ctx.state_set(evaluation);
            let errors = pending_errors
                .into_iter()
                .map(|error| AgenticMemoryError {
                    code: error.code,
                    message: "agentic_memory_retention_bundle: invalid retention input".to_owned(),
                    index: error.index,
                    record_id: error.record_id,
                    fragment_id: error.fragment_id,
                    validation_errors: error.validation_errors,
                    cursor: retention_error_cursor(&cursor),
                })
                .collect();
            ctx.emit(AgenticMemoryRetentionSnapshot {
                active_records,
                archived_records,
                consolidation_requests,
                status,
                errors,
                cursor,
            });
        }),
        vec![records.erased(), commands.erased()],
        named_solution_node_opts(format!("{name}/snapshot")),
    );

    AgenticMemoryRetentionBundle {
        records_input: records,
        commands_input: commands,
        active_records: solution_projection(
            graph,
            &snapshot,
            format!("{name}/active_records"),
            "agenticMemoryActiveRecords",
            |snapshot: &AgenticMemoryRetentionSnapshot<T>| snapshot.active_records.clone(),
        ),
        archived_records: solution_projection(
            graph,
            &snapshot,
            format!("{name}/archived_records"),
            "agenticMemoryArchivedRecords",
            |snapshot: &AgenticMemoryRetentionSnapshot<T>| snapshot.archived_records.clone(),
        ),
        consolidation_requests: solution_projection(
            graph,
            &snapshot,
            format!("{name}/consolidation_requests"),
            "agenticMemoryConsolidationRequests",
            |snapshot: &AgenticMemoryRetentionSnapshot<T>| snapshot.consolidation_requests.clone(),
        ),
        status: solution_projection(
            graph,
            &snapshot,
            format!("{name}/status"),
            "agenticMemoryRetentionStatus",
            |snapshot: &AgenticMemoryRetentionSnapshot<T>| snapshot.status.clone(),
        ),
        errors: solution_projection(
            graph,
            &snapshot,
            format!("{name}/errors"),
            "agenticMemoryRetentionErrors",
            |snapshot: &AgenticMemoryRetentionSnapshot<T>| snapshot.errors.clone(),
        ),
        cursor: solution_projection(
            graph,
            &snapshot,
            format!("{name}/cursor"),
            "agenticMemoryRetentionCursor",
            |snapshot: &AgenticMemoryRetentionSnapshot<T>| snapshot.cursor.clone(),
        ),
        snapshot,
    }
}

pub fn agentic_memory_consolidation_bundle<T: Clone + 'static>(
    graph: &Graph,
    opts: AgenticMemoryConsolidationBundleOptions<T>,
) -> AgenticMemoryConsolidationBundle<T> {
    let name = opts
        .name
        .unwrap_or_else(|| "agenticMemoryConsolidation".to_owned());
    let requests = opts.requests;
    let outcomes = opts.outcomes;
    let snapshot = graph.init_node(
        Operator::with_opts(
            "agenticMemoryConsolidation",
            solution_node_config(),
            |ctx| {
                let evaluation = ctx
                    .state_get::<u64>()
                    .map(|evaluation| *evaluation + 1)
                    .unwrap_or(1);
                let requests = ctx
                    .data::<Vec<AgenticMemoryConsolidationRequest>>(0)
                    .map(|requests| (*requests).clone())
                    .unwrap_or_default();
                let outcomes = ctx
                    .data::<Vec<AgenticMemoryConsolidationOutcome<T>>>(1)
                    .map(|outcomes| (*outcomes).clone())
                    .unwrap_or_default();
                let projected = project_consolidation_outcomes(requests, outcomes);
                let cursor = AgenticMemoryConsolidationCursor {
                    evaluation,
                    valid_requests: projected.valid_requests,
                    valid_outcomes: projected.valid_outcomes,
                    invalid_outcomes: projected.invalid_outcomes,
                    results: projected.results.len(),
                    proposed_record_drafts: projected.proposed_record_drafts.len(),
                };
                let error_cursor = AgenticMemoryCursor {
                    evaluation,
                    valid_records: 0,
                    invalid_records: cursor.invalid_outcomes,
                    projected_fragments: 0,
                    result_count: cursor.results,
                };
                let errors = projected
                    .errors
                    .into_iter()
                    .map(|error| AgenticMemoryError {
                        code: error.code,
                        message: "agentic_memory_consolidation_bundle: invalid consolidation input"
                            .to_owned(),
                        index: error.index,
                        record_id: error.record_id,
                        fragment_id: error.fragment_id,
                        validation_errors: error.validation_errors,
                        cursor: error_cursor.clone(),
                    })
                    .collect::<Vec<_>>();
                let status = AgenticMemoryConsolidationStatus {
                    state: if errors.is_empty() && !projected.results.is_empty() {
                        AgenticMemoryStatusState::Ready
                    } else if errors.is_empty() {
                        AgenticMemoryStatusState::Empty
                    } else if !projected.results.is_empty() {
                        AgenticMemoryStatusState::Partial
                    } else {
                        AgenticMemoryStatusState::Error
                    },
                    cursor: cursor.clone(),
                };
                ctx.state_set(evaluation);
                ctx.emit(AgenticMemoryConsolidationSnapshot {
                    results: projected.results,
                    proposed_record_drafts: projected.proposed_record_drafts,
                    commands: projected.commands,
                    status,
                    errors,
                    cursor,
                });
            },
        ),
        vec![requests.erased(), outcomes.erased()],
        named_solution_node_opts(format!("{name}/snapshot")),
    );
    AgenticMemoryConsolidationBundle {
        requests_input: requests,
        outcomes_input: outcomes,
        results: solution_projection(
            graph,
            &snapshot,
            format!("{name}/results"),
            "agenticMemoryConsolidationResults",
            |snapshot: &AgenticMemoryConsolidationSnapshot<T>| snapshot.results.clone(),
        ),
        proposed_record_drafts: solution_projection(
            graph,
            &snapshot,
            format!("{name}/proposed_record_drafts"),
            "agenticMemoryConsolidationRecordDrafts",
            |snapshot: &AgenticMemoryConsolidationSnapshot<T>| {
                snapshot.proposed_record_drafts.clone()
            },
        ),
        commands: solution_projection(
            graph,
            &snapshot,
            format!("{name}/commands"),
            "agenticMemoryConsolidationCommands",
            |snapshot: &AgenticMemoryConsolidationSnapshot<T>| snapshot.commands.clone(),
        ),
        status: solution_projection(
            graph,
            &snapshot,
            format!("{name}/status"),
            "agenticMemoryConsolidationStatus",
            |snapshot: &AgenticMemoryConsolidationSnapshot<T>| snapshot.status.clone(),
        ),
        errors: solution_projection(
            graph,
            &snapshot,
            format!("{name}/errors"),
            "agenticMemoryConsolidationErrors",
            |snapshot: &AgenticMemoryConsolidationSnapshot<T>| snapshot.errors.clone(),
        ),
        cursor: solution_projection(
            graph,
            &snapshot,
            format!("{name}/cursor"),
            "agenticMemoryConsolidationCursor",
            |snapshot: &AgenticMemoryConsolidationSnapshot<T>| snapshot.cursor.clone(),
        ),
        snapshot,
    }
}

pub fn agentic_memory_context_packing_bundle<T: Clone + 'static>(
    graph: &Graph,
    opts: AgenticMemoryContextPackingBundleOptions<T>,
) -> AgenticMemoryContextPackingBundle<T> {
    let name = opts
        .name
        .unwrap_or_else(|| "agenticMemoryContextPacking".to_owned());
    let context = opts.context;
    let texts = opts.texts;
    let policy = opts.policy;
    let snapshot = graph.init_node(
        Operator::with_opts(
            "agenticMemoryContextPacking",
            solution_node_config(),
            |ctx| {
                let evaluation = ctx
                    .state_get::<u64>()
                    .map(|evaluation| *evaluation + 1)
                    .unwrap_or(1);
                let context = ctx
                    .data::<AgenticMemoryContext<T>>(0)
                    .map(|context| (*context).clone());
                let text_facts = ctx
                    .data::<Vec<AgenticMemoryTextProjection>>(1)
                    .map(|texts| (*texts).clone())
                    .unwrap_or_default();
                let policy = ctx
                    .data::<AgenticMemoryContextPackingPolicy>(2)
                    .map(|policy| (*policy).clone())
                    .unwrap_or_default();
                let (packed_context, mut pending_errors, state) =
                    pack_context(evaluation, context.as_ref(), &text_facts, &policy);
                let cursor = packed_context.cursor.clone();
                let status = AgenticMemoryContextPackingStatus {
                    state,
                    cursor: cursor.clone(),
                };
                ctx.state_set(evaluation);
                let errors = pending_errors
                    .drain(..)
                    .map(|error| AgenticMemoryError {
                        code: error.code,
                        message: "agentic_memory_context_packing_bundle: invalid packing input"
                            .to_owned(),
                        index: error.index,
                        record_id: error.record_id,
                        fragment_id: error.fragment_id,
                        validation_errors: error.validation_errors,
                        cursor: packing_error_cursor(&cursor),
                    })
                    .collect();
                ctx.emit(AgenticMemoryContextPackingSnapshot {
                    packed_context,
                    status,
                    errors,
                    cursor,
                });
            },
        ),
        vec![context.erased(), texts.erased(), policy.erased()],
        named_solution_node_opts(format!("{name}/snapshot")),
    );

    AgenticMemoryContextPackingBundle {
        context_input: context,
        texts_input: texts,
        policy_input: policy,
        packed_context: solution_projection(
            graph,
            &snapshot,
            format!("{name}/packed_context"),
            "agenticMemoryPackedContext",
            |snapshot: &AgenticMemoryContextPackingSnapshot| snapshot.packed_context.clone(),
        ),
        status: solution_projection(
            graph,
            &snapshot,
            format!("{name}/status"),
            "agenticMemoryContextPackingStatus",
            |snapshot: &AgenticMemoryContextPackingSnapshot| snapshot.status.clone(),
        ),
        errors: solution_projection(
            graph,
            &snapshot,
            format!("{name}/errors"),
            "agenticMemoryContextPackingErrors",
            |snapshot: &AgenticMemoryContextPackingSnapshot| snapshot.errors.clone(),
        ),
        cursor: solution_projection(
            graph,
            &snapshot,
            format!("{name}/cursor"),
            "agenticMemoryContextPackingCursor",
            |snapshot: &AgenticMemoryContextPackingSnapshot| snapshot.cursor.clone(),
        ),
        snapshot,
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

fn solution_projection<S, U, F>(
    graph: &Graph,
    snapshot: &Node<S>,
    name: String,
    factory: &'static str,
    select: F,
) -> Node<U>
where
    S: Clone + 'static,
    U: 'static,
    F: Fn(&S) -> U + 'static,
{
    graph.init_node(
        Operator::with_opts(factory, solution_node_config(), move |ctx| {
            for snapshot in ctx.batch::<S>(0) {
                ctx.emit(select(snapshot.as_ref()));
            }
        }),
        vec![snapshot.erased()],
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
        record_id: record.id.clone(),
        kind: record.kind,
        persistence_level: record.persistence_level,
        artifact_kind: record.artifact_kind,
        scope: record.scope.clone(),
    }
}

struct ValidAgenticRecords<T> {
    by_record_id: BTreeMap<FactId, AgenticMemoryRecord<T>>,
    in_order: Vec<AgenticMemoryRecord<T>>,
    errors: Vec<PendingAgenticMemoryError>,
}

fn valid_record_index<T: Clone>(
    raw_records: Vec<AgenticMemoryRecord<T>>,
) -> ValidAgenticRecords<T> {
    let mut records = BTreeMap::new();
    let mut records_in_order = Vec::new();
    let mut seen_record_ids = HashSet::<FactId>::new();
    let mut seen_fragment_ids = HashSet::<FactId>::new();
    let mut pending_errors = Vec::new();
    for (index, record) in raw_records.into_iter().enumerate() {
        let validation = validate_agentic_memory_record(&record);
        if !validation.ok {
            pending_errors.push(PendingAgenticMemoryError {
                code: AgenticMemoryErrorCode::InvalidRecord,
                index: Some(index),
                record_id: Some(record.id.clone()),
                fragment_id: Some(record.fragment.id.clone()),
                validation_errors: validation.errors,
            });
            continue;
        }
        if !seen_record_ids.insert(record.id.clone()) {
            pending_errors.push(PendingAgenticMemoryError {
                code: AgenticMemoryErrorCode::DuplicateRecordId,
                index: Some(index),
                record_id: Some(record.id.clone()),
                fragment_id: Some(record.fragment.id.clone()),
                validation_errors: vec![format!("duplicate record id '{}'", record.id)],
            });
            continue;
        }
        if !seen_fragment_ids.insert(record.fragment.id.clone()) {
            pending_errors.push(PendingAgenticMemoryError {
                code: AgenticMemoryErrorCode::DuplicateFragmentId,
                index: Some(index),
                record_id: Some(record.id.clone()),
                fragment_id: Some(record.fragment.id.clone()),
                validation_errors: vec![format!("duplicate fragment id '{}'", record.fragment.id)],
            });
            continue;
        }
        records.insert(record.id.clone(), record.clone());
        records_in_order.push(record);
    }
    ValidAgenticRecords {
        by_record_id: records,
        in_order: records_in_order,
        errors: pending_errors,
    }
}

fn validate_kg_draft(draft: &AgenticMemoryKgAssertionDraft) -> Vec<String> {
    let mut errors = Vec::new();
    if draft.id.is_empty() {
        errors.push("id must be a non-empty string".to_owned());
    }
    if draft.subject_id.is_empty() {
        errors.push("subject_id must be a non-empty string".to_owned());
    }
    if draft.predicate.is_empty() {
        errors.push("predicate must be a non-empty string".to_owned());
    }
    if !draft.confidence.is_finite() || !(0.0..=1.0).contains(&draft.confidence) {
        errors.push("confidence must be finite in [0, 1]".to_owned());
    }
    match &draft.object {
        KnowledgeAssertionObject::Entity { entity_id } if entity_id.is_empty() => {
            errors.push("object.entity_id must be a non-empty string".to_owned());
        }
        KnowledgeAssertionObject::Literal { value } => {
            if let Err(error) = validate_strict_json_value(value, "object.literal") {
                errors.push(error.to_string());
            }
        }
        KnowledgeAssertionObject::Entity { .. } => {}
    }
    errors
}

fn validate_retention_command(command: &AgenticMemoryRetentionCommand) -> Vec<String> {
    let mut errors = Vec::new();
    if command.id.is_empty() {
        errors.push("id must be a non-empty string".to_owned());
    }
    if command.record_id.is_empty() {
        errors.push("record_id must be a non-empty string".to_owned());
    }
    if command
        .reason
        .as_ref()
        .is_some_and(|reason| reason.is_empty())
    {
        errors.push("reason must be non-empty when present".to_owned());
    }
    errors
}

struct ProjectedConsolidation<T> {
    results: Vec<AgenticMemoryConsolidationResult>,
    proposed_record_drafts: Vec<AgenticMemoryConsolidationRecordDraft<T>>,
    commands: Vec<AgenticMemoryConsolidationCommand>,
    errors: Vec<PendingAgenticMemoryError>,
    valid_requests: usize,
    valid_outcomes: usize,
    invalid_outcomes: usize,
}

fn project_consolidation_outcomes<T: Clone>(
    requests: Vec<AgenticMemoryConsolidationRequest>,
    outcomes: Vec<AgenticMemoryConsolidationOutcome<T>>,
) -> ProjectedConsolidation<T> {
    let mut by_request = BTreeMap::new();
    for request in requests {
        by_request.insert(request.command_id.clone(), request);
    }
    let valid_requests = by_request.len();
    let mut seen_outcomes = HashSet::new();
    let mut results = Vec::new();
    let mut proposed_record_drafts = Vec::new();
    let mut commands = Vec::new();
    let mut errors = Vec::new();
    let mut valid_outcomes = 0usize;
    let mut invalid_outcomes = 0usize;
    for (index, outcome) in outcomes.into_iter().enumerate() {
        let (outcome_id, request_id) = consolidation_outcome_ids(&outcome);
        let mut validation_errors = validate_consolidation_outcome(&outcome);
        if !seen_outcomes.insert(outcome_id.clone()) {
            validation_errors.push(format!("duplicate consolidation outcome id '{outcome_id}'"));
        }
        let request = by_request.get(&request_id);
        if request.is_none() {
            validation_errors.push(format!(
                "request_id '{request_id}' does not reference a projected request"
            ));
        }
        if !validation_errors.is_empty() {
            let code = if validation_errors
                .iter()
                .any(|error| error.starts_with("duplicate consolidation outcome id"))
            {
                AgenticMemoryErrorCode::DuplicateConsolidationOutcomeId
            } else if validation_errors
                .iter()
                .any(|error| error.starts_with("request_id"))
            {
                AgenticMemoryErrorCode::MissingConsolidationRequest
            } else {
                AgenticMemoryErrorCode::InvalidConsolidationOutcome
            };
            invalid_outcomes += 1;
            errors.push(PendingAgenticMemoryError {
                code,
                index: Some(index),
                record_id: Some(outcome_id),
                fragment_id: None,
                validation_errors,
            });
            continue;
        }
        let request = request.expect("validation checked request existence");
        valid_outcomes += 1;
        match outcome {
            AgenticMemoryConsolidationOutcome::Failed {
                id,
                request_id,
                message,
                provenance,
            } => {
                let result_id = format!("{request_id}:{id}");
                results.push(AgenticMemoryConsolidationResult {
                    id: result_id.clone(),
                    request_id: request_id.clone(),
                    outcome_id: id.clone(),
                    state: AgenticMemoryConsolidationResultState::Failed,
                    source_record_ids: vec![request.record_id.clone()],
                    proposed_record_ids: Vec::new(),
                    message: Some(message.clone()),
                    provenance,
                });
                commands.push(AgenticMemoryConsolidationCommand {
                    id: format!("{result_id}:mark_failed"),
                    kind: AgenticMemoryConsolidationCommandKind::MarkFailed,
                    request_id,
                    outcome_id: id,
                    draft_ids: Vec::new(),
                    message: Some(message),
                });
            }
            AgenticMemoryConsolidationOutcome::ProposedRecords {
                id,
                request_id,
                records,
                provenance,
            } => {
                let result_id = format!("{request_id}:{id}");
                let mut draft_ids = Vec::new();
                let mut proposed_record_ids = Vec::new();
                for record in records {
                    let draft_id = format!("{request_id}:{id}:{}", record.id);
                    draft_ids.push(draft_id.clone());
                    proposed_record_ids.push(record.id.clone());
                    proposed_record_drafts.push(AgenticMemoryConsolidationRecordDraft {
                        id: draft_id,
                        request_id: request_id.clone(),
                        outcome_id: id.clone(),
                        record,
                    });
                }
                results.push(AgenticMemoryConsolidationResult {
                    id: result_id.clone(),
                    request_id: request_id.clone(),
                    outcome_id: id.clone(),
                    state: AgenticMemoryConsolidationResultState::Proposed,
                    source_record_ids: vec![request.record_id.clone()],
                    proposed_record_ids,
                    message: None,
                    provenance,
                });
                commands.push(AgenticMemoryConsolidationCommand {
                    id: format!("{result_id}:propose_records"),
                    kind: AgenticMemoryConsolidationCommandKind::ProposeRecords,
                    request_id,
                    outcome_id: id,
                    draft_ids,
                    message: None,
                });
            }
        }
    }
    ProjectedConsolidation {
        results,
        proposed_record_drafts,
        commands,
        errors,
        valid_requests,
        valid_outcomes,
        invalid_outcomes,
    }
}

fn consolidation_outcome_ids<T>(
    outcome: &AgenticMemoryConsolidationOutcome<T>,
) -> (FactId, FactId) {
    match outcome {
        AgenticMemoryConsolidationOutcome::ProposedRecords { id, request_id, .. }
        | AgenticMemoryConsolidationOutcome::Failed { id, request_id, .. } => {
            (id.clone(), request_id.clone())
        }
    }
}

fn validate_consolidation_outcome<T>(
    outcome: &AgenticMemoryConsolidationOutcome<T>,
) -> Vec<String> {
    let mut errors = Vec::new();
    match outcome {
        AgenticMemoryConsolidationOutcome::ProposedRecords {
            id,
            request_id,
            records,
            provenance,
        } => {
            if id.is_empty() {
                errors.push("id must be a non-empty string".to_owned());
            }
            if request_id.is_empty() {
                errors.push("request_id must be a non-empty string".to_owned());
            }
            if records.is_empty() {
                errors.push("records must be non-empty".to_owned());
            }
            if provenance.as_ref().is_some_and(|value| value.is_empty()) {
                errors.push("provenance must be non-empty when present".to_owned());
            }
            for (index, record) in records.iter().enumerate() {
                let validation = validate_agentic_memory_record(record);
                if !validation.ok {
                    errors.extend(
                        validation
                            .errors
                            .into_iter()
                            .map(|error| format!("records[{index}]: {error}")),
                    );
                }
            }
        }
        AgenticMemoryConsolidationOutcome::Failed {
            id,
            request_id,
            message,
            provenance,
        } => {
            if id.is_empty() {
                errors.push("id must be a non-empty string".to_owned());
            }
            if request_id.is_empty() {
                errors.push("request_id must be a non-empty string".to_owned());
            }
            if message.is_empty() {
                errors.push("message must be a non-empty string".to_owned());
            }
            if provenance.as_ref().is_some_and(|value| value.is_empty()) {
                errors.push("provenance must be non-empty when present".to_owned());
            }
        }
    }
    errors
}

fn pack_context<T: Clone>(
    evaluation: u64,
    context: Option<&AgenticMemoryContext<T>>,
    text_facts: &[AgenticMemoryTextProjection],
    policy: &AgenticMemoryContextPackingPolicy,
) -> (
    AgenticMemoryPackedContext,
    Vec<PendingAgenticMemoryError>,
    AgenticMemoryStatusState,
) {
    let mut pending_errors = Vec::new();
    if policy.max_chars == Some(0) {
        pending_errors.push(PendingAgenticMemoryError {
            code: AgenticMemoryErrorCode::InvalidPackingPolicy,
            index: None,
            record_id: None,
            fragment_id: None,
            validation_errors: vec!["max_chars must be greater than 0 when present".to_owned()],
        });
    }
    let mut text_by_fragment = HashMap::<FactId, String>::new();
    let mut seen_text_fragment_ids = HashSet::<FactId>::new();
    for (index, text) in text_facts.iter().enumerate() {
        let mut validation_errors = Vec::new();
        if text.fragment_id.is_empty() {
            validation_errors.push("fragment_id must be a non-empty string".to_owned());
        }
        if text.text.is_empty() {
            validation_errors.push("text must be a non-empty string".to_owned());
        }
        if validation_errors.is_empty() && !seen_text_fragment_ids.insert(text.fragment_id.clone())
        {
            pending_errors.push(PendingAgenticMemoryError {
                code: AgenticMemoryErrorCode::DuplicateTextProjection,
                index: Some(index),
                record_id: None,
                fragment_id: Some(text.fragment_id.clone()),
                validation_errors: vec![format!(
                    "duplicate text projection for fragment '{}'",
                    text.fragment_id
                )],
            });
            continue;
        }
        if !validation_errors.is_empty() {
            pending_errors.push(PendingAgenticMemoryError {
                code: AgenticMemoryErrorCode::InvalidTextProjection,
                index: Some(index),
                record_id: None,
                fragment_id: if text.fragment_id.is_empty() {
                    None
                } else {
                    Some(text.fragment_id.clone())
                },
                validation_errors,
            });
            continue;
        }
        text_by_fragment.insert(text.fragment_id.clone(), text.text.clone());
    }
    let entries = context
        .map(|context| context.entries.as_slice())
        .unwrap_or(&[]);
    let mut packed_text = String::new();
    let mut fragment_ids = Vec::new();
    let mut missing_text = 0usize;
    let mut truncated = false;
    for (index, entry) in entries.iter().enumerate() {
        let Some(projected_text) = text_by_fragment.get(&entry.fragment_id) else {
            missing_text += 1;
            pending_errors.push(PendingAgenticMemoryError {
                code: AgenticMemoryErrorCode::MissingTextProjection,
                index: Some(index),
                record_id: None,
                fragment_id: Some(entry.fragment_id.clone()),
                validation_errors: vec![format!(
                    "missing text projection for fragment '{}'",
                    entry.fragment_id
                )],
            });
            continue;
        };
        let part = if policy.include_fragment_ids {
            format!("[{}] {projected_text}", entry.fragment_id)
        } else {
            projected_text.clone()
        };
        let addition = if packed_text.is_empty() {
            part
        } else {
            format!("{}{}", policy.separator, part)
        };
        if let Some(max_chars) = policy.max_chars {
            if packed_text.chars().count() + addition.chars().count() > max_chars {
                truncated = true;
                break;
            }
        }
        packed_text.push_str(&addition);
        fragment_ids.push(entry.fragment_id.clone());
    }
    let cursor = AgenticMemoryContextPackingCursor {
        evaluation,
        context_entries: entries.len(),
        text_projection_count: text_facts.len(),
        packed_entries: fragment_ids.len(),
        missing_text,
        char_count: packed_text.chars().count(),
    };
    let base_state = match context.map(|context| context.state) {
        Some(AgenticMemoryStatusState::Error) => AgenticMemoryStatusState::Error,
        _ if !pending_errors.is_empty() => AgenticMemoryStatusState::Partial,
        _ if truncated => AgenticMemoryStatusState::Partial,
        _ if fragment_ids.is_empty() => AgenticMemoryStatusState::Empty,
        _ => AgenticMemoryStatusState::Ready,
    };
    (
        AgenticMemoryPackedContext {
            text: packed_text,
            fragment_ids,
            truncated,
            cursor,
        },
        pending_errors,
        base_state,
    )
}

fn projection_state(
    error_count: usize,
    output_count: usize,
    valid_input_count: usize,
) -> AgenticMemoryStatusState {
    if error_count > 0 && valid_input_count == 0 {
        AgenticMemoryStatusState::Error
    } else if error_count > 0 {
        AgenticMemoryStatusState::Partial
    } else if output_count > 0 {
        AgenticMemoryStatusState::Ready
    } else {
        AgenticMemoryStatusState::Empty
    }
}

fn push_unique(values: &mut Vec<FactId>, value: FactId) {
    if !values.iter().any(|seen| seen == &value) {
        values.push(value);
    }
}

fn kg_error_cursor(cursor: &AgenticMemoryKgProjectionCursor) -> AgenticMemoryCursor {
    AgenticMemoryCursor {
        evaluation: cursor.evaluation,
        valid_records: cursor.valid_records,
        invalid_records: cursor.invalid_drafts,
        projected_fragments: 0,
        result_count: cursor.projected_assertions,
    }
}

fn retention_error_cursor(cursor: &AgenticMemoryRetentionCursor) -> AgenticMemoryCursor {
    AgenticMemoryCursor {
        evaluation: cursor.evaluation,
        valid_records: cursor.valid_records,
        invalid_records: cursor.invalid_commands,
        projected_fragments: cursor.active_records + cursor.archived_records,
        result_count: cursor.consolidation_requests,
    }
}

fn packing_error_cursor(cursor: &AgenticMemoryContextPackingCursor) -> AgenticMemoryCursor {
    AgenticMemoryCursor {
        evaluation: cursor.evaluation,
        valid_records: cursor.context_entries.saturating_sub(cursor.missing_text),
        invalid_records: cursor.missing_text,
        projected_fragments: cursor.packed_entries,
        result_count: cursor.char_count,
    }
}

impl Codec<AgenticMemoryRecordFrame> for AgenticMemoryRecordFrameCodec {
    fn encode(&self, frame: &AgenticMemoryRecordFrame) -> JsonCodecResult<Vec<u8>> {
        if frame.format != AGENTIC_MEMORY_RECORD_FRAME_FORMAT {
            return Err(JsonCodecError::validation(format!(
                "agenticMemoryRecordFrameCodec: format must be {AGENTIC_MEMORY_RECORD_FRAME_FORMAT}"
            )));
        }
        if frame.version != AGENTIC_MEMORY_RECORD_FRAME_VERSION {
            return Err(JsonCodecError::validation(format!(
                "agenticMemoryRecordFrameCodec: version must be {AGENTIC_MEMORY_RECORD_FRAME_VERSION}"
            )));
        }
        validate_agentic_memory_record(&frame.record)
            .errors
            .into_iter()
            .next()
            .map_or(Ok(()), |error| {
                Err(JsonCodecError::validation(format!(
                    "agenticMemoryRecordFrameCodec: {error}"
                )))
            })?;
        validate_strict_json_value(&frame.record.fragment.payload, "record.fragment.payload")?;
        strict_canonical_json_bytes(&record_frame_to_json(frame)?)
    }

    fn decode(&self, bytes: &[u8]) -> JsonCodecResult<AgenticMemoryRecordFrame> {
        let value = strict_json_decode(bytes)?;
        record_frame_from_json(&value)
    }
}

fn record_frame_to_json(frame: &AgenticMemoryRecordFrame) -> JsonCodecResult<JsonValue> {
    let mut root = JsonMap::new();
    root.insert("format".to_owned(), JsonValue::String(frame.format.clone()));
    root.insert("record".to_owned(), record_to_json(&frame.record)?);
    root.insert(
        "version".to_owned(),
        JsonValue::Number(JsonNumber::from(frame.version)),
    );
    Ok(JsonValue::Object(root))
}

fn record_to_json(record: &AgenticMemoryRecord<JsonValue>) -> JsonCodecResult<JsonValue> {
    let mut object = JsonMap::new();
    object.insert(
        "artifactKind".to_owned(),
        JsonValue::String(artifact_kind_to_str(record.artifact_kind).to_owned()),
    );
    object.insert("fragment".to_owned(), fragment_to_json(&record.fragment)?);
    object.insert("id".to_owned(), JsonValue::String(record.id.clone()));
    object.insert(
        "kind".to_owned(),
        JsonValue::String(memory_kind_to_str(record.kind).to_owned()),
    );
    object.insert(
        "persistenceLevel".to_owned(),
        JsonValue::String(persistence_level_to_str(record.persistence_level).to_owned()),
    );
    if let Some(scope) = &record.scope {
        object.insert("scope".to_owned(), scope_to_json(scope));
    }
    Ok(JsonValue::Object(object))
}

fn fragment_to_json(fragment: &MemoryFragment<JsonValue>) -> JsonCodecResult<JsonValue> {
    let mut object = JsonMap::new();
    object.insert(
        "confidence".to_owned(),
        JsonValue::Number(finite_json_number(
            fragment.confidence,
            "agenticMemoryRecordFrameCodec: fragment.confidence",
        )?),
    );
    if let Some(embedding) = &fragment.embedding {
        object.insert(
            "embedding".to_owned(),
            JsonValue::Array(
                embedding
                    .iter()
                    .enumerate()
                    .map(|(index, value)| {
                        finite_json_number(
                            *value,
                            &format!("agenticMemoryRecordFrameCodec: fragment.embedding[{index}]"),
                        )
                        .map(JsonValue::Number)
                    })
                    .collect::<JsonCodecResult<Vec<_>>>()?,
            ),
        );
    }
    object.insert("id".to_owned(), JsonValue::String(fragment.id.clone()));
    if let Some(parent_fragment_id) = &fragment.parent_fragment_id {
        object.insert(
            "parentFragmentId".to_owned(),
            JsonValue::String(parent_fragment_id.clone()),
        );
    }
    object.insert("payload".to_owned(), fragment.payload.clone());
    if let Some(provenance) = &fragment.provenance {
        object.insert(
            "provenance".to_owned(),
            JsonValue::String(provenance.clone()),
        );
    }
    object.insert(
        "sources".to_owned(),
        JsonValue::Array(
            fragment
                .sources
                .iter()
                .cloned()
                .map(JsonValue::String)
                .collect(),
        ),
    );
    object.insert(
        "tags".to_owned(),
        JsonValue::Array(
            fragment
                .tags
                .iter()
                .cloned()
                .map(JsonValue::String)
                .collect(),
        ),
    );
    object.insert(
        "tNs".to_owned(),
        JsonValue::String(u128_to_non_negative_decimal_string(fragment.t_ns)),
    );
    if let Some(valid_from) = fragment.valid_from {
        object.insert(
            "validFrom".to_owned(),
            JsonValue::String(u128_to_non_negative_decimal_string(valid_from)),
        );
    }
    if let Some(valid_to) = fragment.valid_to {
        object.insert(
            "validTo".to_owned(),
            JsonValue::String(u128_to_non_negative_decimal_string(valid_to)),
        );
    }
    Ok(JsonValue::Object(object))
}

fn finite_json_number(value: f64, label: &str) -> JsonCodecResult<JsonNumber> {
    JsonNumber::from_f64(value)
        .ok_or_else(|| JsonCodecError::validation(format!("{label} must be a finite number")))
}

fn scope_to_json(scope: &AgenticMemoryScope) -> JsonValue {
    let mut object = JsonMap::new();
    if let Some(session_id) = &scope.session_id {
        object.insert(
            "sessionId".to_owned(),
            JsonValue::String(session_id.clone()),
        );
    }
    if let Some(project_id) = &scope.project_id {
        object.insert(
            "projectId".to_owned(),
            JsonValue::String(project_id.clone()),
        );
    }
    if let Some(user_id) = &scope.user_id {
        object.insert("userId".to_owned(), JsonValue::String(user_id.clone()));
    }
    if let Some(tenant_id) = &scope.tenant_id {
        object.insert("tenantId".to_owned(), JsonValue::String(tenant_id.clone()));
    }
    JsonValue::Object(object)
}

fn record_frame_from_json(value: &JsonValue) -> JsonCodecResult<AgenticMemoryRecordFrame> {
    let object = as_object(value, "agenticMemoryRecordFrameCodec: frame")?;
    assert_known_keys(
        object,
        &["format", "record", "version"],
        "agenticMemoryRecordFrameCodec: frame",
    )?;
    let format = required_string(object, "format", "agenticMemoryRecordFrameCodec: format")?;
    if format != AGENTIC_MEMORY_RECORD_FRAME_FORMAT {
        return Err(JsonCodecError::validation(format!(
            "agenticMemoryRecordFrameCodec: format must be {AGENTIC_MEMORY_RECORD_FRAME_FORMAT}"
        )));
    }
    let version = required_u32(object, "version", "agenticMemoryRecordFrameCodec: version")?;
    if version != AGENTIC_MEMORY_RECORD_FRAME_VERSION {
        return Err(JsonCodecError::validation(format!(
            "agenticMemoryRecordFrameCodec: version must be {AGENTIC_MEMORY_RECORD_FRAME_VERSION}"
        )));
    }
    let record = record_from_json(required_value(
        object,
        "record",
        "agenticMemoryRecordFrameCodec: record",
    )?)?;
    let frame = AgenticMemoryRecordFrame {
        format,
        version,
        record,
    };
    let validation = validate_agentic_memory_record(&frame.record);
    if !validation.ok {
        return Err(JsonCodecError::validation(format!(
            "agenticMemoryRecordFrameCodec: {}",
            validation.errors.join("; ")
        )));
    }
    validate_strict_json_value(&frame.record.fragment.payload, "record.fragment.payload")?;
    Ok(frame)
}

fn record_from_json(value: &JsonValue) -> JsonCodecResult<AgenticMemoryRecord<JsonValue>> {
    let object = as_object(value, "agenticMemoryRecordFrameCodec: record")?;
    assert_known_keys(
        object,
        &[
            "artifactKind",
            "fragment",
            "id",
            "kind",
            "persistenceLevel",
            "scope",
        ],
        "agenticMemoryRecordFrameCodec: record",
    )?;
    Ok(AgenticMemoryRecord {
        id: required_string(object, "id", "agenticMemoryRecordFrameCodec: record.id")?,
        kind: memory_kind_from_str(&required_string(
            object,
            "kind",
            "agenticMemoryRecordFrameCodec: record.kind",
        )?)?,
        persistence_level: persistence_level_from_str(&required_string(
            object,
            "persistenceLevel",
            "agenticMemoryRecordFrameCodec: record.persistenceLevel",
        )?)?,
        artifact_kind: artifact_kind_from_str(&required_string(
            object,
            "artifactKind",
            "agenticMemoryRecordFrameCodec: record.artifactKind",
        )?)?,
        scope: optional_scope(object.get("scope"))?,
        fragment: fragment_from_json(required_value(
            object,
            "fragment",
            "agenticMemoryRecordFrameCodec: record.fragment",
        )?)?,
    })
}

fn fragment_from_json(value: &JsonValue) -> JsonCodecResult<MemoryFragment<JsonValue>> {
    let object = as_object(value, "agenticMemoryRecordFrameCodec: fragment")?;
    assert_known_keys(
        object,
        &[
            "confidence",
            "embedding",
            "id",
            "parentFragmentId",
            "payload",
            "provenance",
            "sources",
            "tags",
            "tNs",
            "validFrom",
            "validTo",
        ],
        "agenticMemoryRecordFrameCodec: fragment",
    )?;
    Ok(MemoryFragment {
        id: required_string(object, "id", "agenticMemoryRecordFrameCodec: fragment.id")?,
        payload: required_value(
            object,
            "payload",
            "agenticMemoryRecordFrameCodec: fragment.payload",
        )?
        .clone(),
        t_ns: required_decimal_u128(object, "tNs", "agenticMemoryRecordFrameCodec: fragment.tNs")?,
        valid_from: optional_decimal_u128(
            object,
            "validFrom",
            "agenticMemoryRecordFrameCodec: fragment.validFrom",
        )?,
        valid_to: optional_decimal_u128(
            object,
            "validTo",
            "agenticMemoryRecordFrameCodec: fragment.validTo",
        )?,
        confidence: required_f64(
            object,
            "confidence",
            "agenticMemoryRecordFrameCodec: fragment.confidence",
        )?,
        tags: required_string_array(
            object,
            "tags",
            "agenticMemoryRecordFrameCodec: fragment.tags",
        )?,
        sources: required_string_array(
            object,
            "sources",
            "agenticMemoryRecordFrameCodec: fragment.sources",
        )?,
        embedding: optional_f64_array(
            object,
            "embedding",
            "agenticMemoryRecordFrameCodec: fragment.embedding",
        )?,
        parent_fragment_id: optional_string(
            object,
            "parentFragmentId",
            "agenticMemoryRecordFrameCodec: fragment.parentFragmentId",
        )?,
        provenance: optional_string(
            object,
            "provenance",
            "agenticMemoryRecordFrameCodec: fragment.provenance",
        )?,
    })
}

fn optional_scope(value: Option<&JsonValue>) -> JsonCodecResult<Option<AgenticMemoryScope>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let object = as_object(value, "agenticMemoryRecordFrameCodec: scope")?;
    assert_known_keys(
        object,
        &["projectId", "sessionId", "tenantId", "userId"],
        "agenticMemoryRecordFrameCodec: scope",
    )?;
    Ok(Some(AgenticMemoryScope {
        session_id: optional_string(
            object,
            "sessionId",
            "agenticMemoryRecordFrameCodec: scope.sessionId",
        )?,
        project_id: optional_string(
            object,
            "projectId",
            "agenticMemoryRecordFrameCodec: scope.projectId",
        )?,
        user_id: optional_string(
            object,
            "userId",
            "agenticMemoryRecordFrameCodec: scope.userId",
        )?,
        tenant_id: optional_string(
            object,
            "tenantId",
            "agenticMemoryRecordFrameCodec: scope.tenantId",
        )?,
    }))
}

fn as_object<'a>(
    value: &'a JsonValue,
    label: &str,
) -> JsonCodecResult<&'a JsonMap<String, JsonValue>> {
    value
        .as_object()
        .ok_or_else(|| JsonCodecError::validation(format!("{label} must be an object")))
}

fn assert_known_keys(
    object: &JsonMap<String, JsonValue>,
    allowed: &[&str],
    label: &str,
) -> JsonCodecResult<()> {
    for key in object.keys() {
        if !allowed.iter().any(|allowed| allowed == key) {
            return Err(JsonCodecError::validation(format!(
                "{label}: unknown field {key}"
            )));
        }
    }
    Ok(())
}

fn required_value<'a>(
    object: &'a JsonMap<String, JsonValue>,
    key: &str,
    label: &str,
) -> JsonCodecResult<&'a JsonValue> {
    object
        .get(key)
        .ok_or_else(|| JsonCodecError::validation(format!("{label} is required")))
}

fn required_string(
    object: &JsonMap<String, JsonValue>,
    key: &str,
    label: &str,
) -> JsonCodecResult<String> {
    required_value(object, key, label)?
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| JsonCodecError::validation(format!("{label} must be a string")))
}

fn optional_string(
    object: &JsonMap<String, JsonValue>,
    key: &str,
    label: &str,
) -> JsonCodecResult<Option<String>> {
    object
        .get(key)
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| JsonCodecError::validation(format!("{label} must be a string")))
        })
        .transpose()
}

fn required_u32(
    object: &JsonMap<String, JsonValue>,
    key: &str,
    label: &str,
) -> JsonCodecResult<u32> {
    let value = required_value(object, key, label)?
        .as_u64()
        .ok_or_else(|| {
            JsonCodecError::validation(format!("{label} must be a non-negative integer"))
        })?;
    u32::try_from(value)
        .map_err(|err| JsonCodecError::validation(format!("{label} is outside u32 range: {err}")))
}

fn required_f64(
    object: &JsonMap<String, JsonValue>,
    key: &str,
    label: &str,
) -> JsonCodecResult<f64> {
    required_value(object, key, label)?
        .as_f64()
        .filter(|value| value.is_finite())
        .ok_or_else(|| JsonCodecError::validation(format!("{label} must be a finite number")))
}

fn required_decimal_u128(
    object: &JsonMap<String, JsonValue>,
    key: &str,
    label: &str,
) -> JsonCodecResult<u128> {
    non_negative_decimal_string_to_u128(&required_string(object, key, label)?)
}

fn optional_decimal_u128(
    object: &JsonMap<String, JsonValue>,
    key: &str,
    label: &str,
) -> JsonCodecResult<Option<u128>> {
    optional_string(object, key, label)?
        .map(|value| non_negative_decimal_string_to_u128(&value))
        .transpose()
}

fn required_string_array(
    object: &JsonMap<String, JsonValue>,
    key: &str,
    label: &str,
) -> JsonCodecResult<Vec<String>> {
    required_value(object, key, label)?
        .as_array()
        .ok_or_else(|| JsonCodecError::validation(format!("{label} must be an array")))?
        .iter()
        .enumerate()
        .map(|(index, value)| {
            value.as_str().map(str::to_owned).ok_or_else(|| {
                JsonCodecError::validation(format!("{label}[{index}] must be a string"))
            })
        })
        .collect()
}

fn optional_f64_array(
    object: &JsonMap<String, JsonValue>,
    key: &str,
    label: &str,
) -> JsonCodecResult<Option<Vec<f64>>> {
    object
        .get(key)
        .map(|value| {
            value
                .as_array()
                .ok_or_else(|| JsonCodecError::validation(format!("{label} must be an array")))?
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    value
                        .as_f64()
                        .filter(|value| value.is_finite())
                        .ok_or_else(|| {
                            JsonCodecError::validation(format!(
                                "{label}[{index}] must be a finite number"
                            ))
                        })
                })
                .collect()
        })
        .transpose()
}

fn memory_kind_to_str(kind: AgenticMemoryKind) -> &'static str {
    match kind {
        AgenticMemoryKind::Working => "working",
        AgenticMemoryKind::Episodic => "episodic",
        AgenticMemoryKind::Semantic => "semantic",
        AgenticMemoryKind::Procedural => "procedural",
        AgenticMemoryKind::Profile => "profile",
    }
}

fn memory_kind_from_str(value: &str) -> JsonCodecResult<AgenticMemoryKind> {
    match value {
        "working" => Ok(AgenticMemoryKind::Working),
        "episodic" => Ok(AgenticMemoryKind::Episodic),
        "semantic" => Ok(AgenticMemoryKind::Semantic),
        "procedural" => Ok(AgenticMemoryKind::Procedural),
        "profile" => Ok(AgenticMemoryKind::Profile),
        _ => Err(JsonCodecError::validation(format!(
            "agenticMemoryRecordFrameCodec: invalid memory kind '{value}'"
        ))),
    }
}

fn persistence_level_to_str(level: AgenticMemoryPersistenceLevel) -> &'static str {
    match level {
        AgenticMemoryPersistenceLevel::Turn => "turn",
        AgenticMemoryPersistenceLevel::Session => "session",
        AgenticMemoryPersistenceLevel::Project => "project",
        AgenticMemoryPersistenceLevel::LongTerm => "longTerm",
        AgenticMemoryPersistenceLevel::Permanent => "permanent",
        AgenticMemoryPersistenceLevel::Archived => "archived",
    }
}

fn persistence_level_from_str(value: &str) -> JsonCodecResult<AgenticMemoryPersistenceLevel> {
    match value {
        "turn" => Ok(AgenticMemoryPersistenceLevel::Turn),
        "session" => Ok(AgenticMemoryPersistenceLevel::Session),
        "project" => Ok(AgenticMemoryPersistenceLevel::Project),
        "longTerm" => Ok(AgenticMemoryPersistenceLevel::LongTerm),
        "permanent" => Ok(AgenticMemoryPersistenceLevel::Permanent),
        "archived" => Ok(AgenticMemoryPersistenceLevel::Archived),
        _ => Err(JsonCodecError::validation(format!(
            "agenticMemoryRecordFrameCodec: invalid persistence level '{value}'"
        ))),
    }
}

fn artifact_kind_to_str(kind: AgenticMemoryArtifactKind) -> &'static str {
    match kind {
        AgenticMemoryArtifactKind::Raw => "raw",
        AgenticMemoryArtifactKind::Insight => "insight",
        AgenticMemoryArtifactKind::Profile => "profile",
        AgenticMemoryArtifactKind::Procedure => "procedure",
    }
}

fn artifact_kind_from_str(value: &str) -> JsonCodecResult<AgenticMemoryArtifactKind> {
    match value {
        "raw" => Ok(AgenticMemoryArtifactKind::Raw),
        "insight" => Ok(AgenticMemoryArtifactKind::Insight),
        "profile" => Ok(AgenticMemoryArtifactKind::Profile),
        "procedure" => Ok(AgenticMemoryArtifactKind::Procedure),
        _ => Err(JsonCodecError::validation(format!(
            "agenticMemoryRecordFrameCodec: invalid artifact kind '{value}'"
        ))),
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
    code: AgenticMemoryErrorCode,
    index: Option<usize>,
    record_id: Option<FactId>,
    fragment_id: Option<FactId>,
    validation_errors: Vec<String>,
}
