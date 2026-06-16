//! Optional ProcessBundle-over-workQueue recipe (D349/D353).
//!
//! Queue records are mapped to graph-visible process evidence. Queue completion
//! remains disposition evidence, not process completion or domain truth.

use std::collections::{BTreeMap, BTreeSet};

use crate::ctx::Ctx;
use crate::graph::{Graph, GraphNodeOpts};
use crate::messaging::DataIssue;
use crate::node::Node;
use crate::process::ProcessEffectRequest;
use crate::work_queue::{WorkQueueCommand, WorkQueueRecord};

#[derive(Debug, Clone, PartialEq)]
pub struct ProcessQueuedEffectPayload<TEffect> {
    pub kind: String,
    pub effect: ProcessEffectRequest<TEffect>,
    pub idempotency_key: Option<String>,
    pub source_refs: Vec<String>,
    pub policy_refs: Vec<String>,
    pub metadata: Option<String>,
}

impl<TEffect> ProcessQueuedEffectPayload<TEffect> {
    pub fn new(effect: ProcessEffectRequest<TEffect>) -> Self {
        Self {
            kind: "process-queued-effect".to_owned(),
            effect,
            idempotency_key: None,
            source_refs: Vec::new(),
            policy_refs: Vec::new(),
            metadata: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessQueueEvidence {
    pub kind: String,
    pub evidence_id: String,
    pub effect_id: String,
    pub effect_type: String,
    pub work_id: String,
    pub queue_record_kind: String,
    pub result: Option<String>,
    pub error: Option<String>,
    pub recorded_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessQueueStatus {
    pub kind: String,
    pub state: String,
    pub effect_id: Option<String>,
    pub effect_type: Option<String>,
    pub work_id: Option<String>,
    pub queue_record_kind: Option<String>,
    pub evidence_id: Option<String>,
    pub issue_codes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessQueueAuditRecord {
    pub kind: String,
    pub seq: u64,
    pub outcome: String,
    pub effect_id: Option<String>,
    pub effect_type: Option<String>,
    pub work_id: Option<String>,
    pub queue_record_kind: Option<String>,
    pub evidence_id: Option<String>,
}

#[derive(Clone)]
pub struct ProcessWorkQueueRecipeOptions<TEffect> {
    pub name: String,
    pub effect_requests: Option<Node<ProcessEffectRequest<TEffect>>>,
    pub records: Node<WorkQueueRecord<ProcessQueuedEffectPayload<TEffect>>>,
}

impl<TEffect> ProcessWorkQueueRecipeOptions<TEffect> {
    pub fn new(records: Node<WorkQueueRecord<ProcessQueuedEffectPayload<TEffect>>>) -> Self {
        Self {
            name: "processWorkQueue".to_owned(),
            effect_requests: None,
            records,
        }
    }

    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    pub fn with_effect_requests(
        mut self,
        effect_requests: Node<ProcessEffectRequest<TEffect>>,
    ) -> Self {
        self.effect_requests = Some(effect_requests);
        self
    }
}

#[derive(Clone)]
pub struct ProcessWorkQueueRecipeBundle<TEffect> {
    pub submit_commands: Option<Node<WorkQueueCommand<ProcessQueuedEffectPayload<TEffect>>>>,
    pub evidence: Node<ProcessQueueEvidence>,
    pub status: Node<ProcessQueueStatus>,
    pub issues: Node<DataIssue>,
    pub audit: Node<ProcessQueueAuditRecord>,
}

#[derive(Clone)]
enum ProcessQueueFact {
    Evidence(ProcessQueueEvidence),
    Status(ProcessQueueStatus),
    Issue(DataIssue),
    Audit(ProcessQueueAuditRecord),
}

#[derive(Clone)]
struct ProcessQueueState<TEffect> {
    payloads: BTreeMap<String, ProcessQueuedEffectPayload<TEffect>>,
    terminal_records: BTreeSet<String>,
    audit_seq: u64,
}

impl<TEffect> Default for ProcessQueueState<TEffect> {
    fn default() -> Self {
        Self {
            payloads: BTreeMap::new(),
            terminal_records: BTreeSet::new(),
            audit_seq: 0,
        }
    }
}

pub fn process_work_queue_recipe<TEffect: Clone + 'static>(
    graph: &Graph,
    opts: ProcessWorkQueueRecipeOptions<TEffect>,
) -> ProcessWorkQueueRecipeBundle<TEffect> {
    let name = opts.name.clone();
    let submit_commands = opts.effect_requests.as_ref().map(|effect_requests| {
        process_effect_submit_commands(
            graph,
            effect_requests.clone(),
            format!("{name}/submitCommands"),
        )
    });
    let runtime = graph.node_opts::<ProcessQueueFact, _>(
        vec![opts.records.erased()],
        move |ctx| {
            let mut state = ctx
                .state_get::<ProcessQueueState<TEffect>>()
                .map(|state| (*state).clone())
                .unwrap_or_default();
            for record in ctx.batch::<WorkQueueRecord<ProcessQueuedEffectPayload<TEffect>>>(0) {
                reduce_record(ctx, &mut state, &record);
            }
            ctx.state_set(state);
            ctx.state_persist(true);
        },
        GraphNodeOpts::named(format!("{name}/runtime")),
    );
    ProcessWorkQueueRecipeBundle {
        submit_commands,
        evidence: project(
            graph,
            &runtime,
            format!("{name}/evidence"),
            |fact| match fact {
                ProcessQueueFact::Evidence(evidence) => Some(evidence.clone()),
                _ => None,
            },
        ),
        status: project(
            graph,
            &runtime,
            format!("{name}/status"),
            |fact| match fact {
                ProcessQueueFact::Status(status) => Some(status.clone()),
                _ => None,
            },
        ),
        issues: project(
            graph,
            &runtime,
            format!("{name}/issues"),
            |fact| match fact {
                ProcessQueueFact::Issue(issue) => Some(issue.clone()),
                _ => None,
            },
        ),
        audit: project(
            graph,
            &runtime,
            format!("{name}/audit"),
            |fact| match fact {
                ProcessQueueFact::Audit(audit) => Some(audit.clone()),
                _ => None,
            },
        ),
    }
}

pub fn process_effect_submit_command<TEffect: Clone>(
    effect: ProcessEffectRequest<TEffect>,
) -> WorkQueueCommand<ProcessQueuedEffectPayload<TEffect>> {
    let command_id = format!("{}:process-work-queue-submit", effect.id);
    let mut payload = ProcessQueuedEffectPayload::new(effect.clone());
    payload.idempotency_key = Some(effect.id.clone());
    WorkQueueCommand::Submit {
        payload,
        command_id,
        queue_id: None,
        idempotency_key: Some(effect.id),
    }
}

pub fn process_effect_submit_commands<TEffect: Clone + 'static>(
    graph: &Graph,
    effect_requests: Node<ProcessEffectRequest<TEffect>>,
    name: impl Into<String>,
) -> Node<WorkQueueCommand<ProcessQueuedEffectPayload<TEffect>>> {
    graph.node_opts::<WorkQueueCommand<ProcessQueuedEffectPayload<TEffect>>, _>(
        vec![effect_requests.erased()],
        move |ctx| {
            for effect in ctx.batch::<ProcessEffectRequest<TEffect>>(0) {
                ctx.emit(process_effect_submit_command((*effect).clone()));
            }
        },
        GraphNodeOpts::named(name.into()),
    )
}

fn reduce_record<TEffect: Clone + 'static>(
    ctx: &Ctx,
    state: &mut ProcessQueueState<TEffect>,
    record: &WorkQueueRecord<ProcessQueuedEffectPayload<TEffect>>,
) {
    match record {
        WorkQueueRecord::WorkAdmitted {
            work_id, payload, ..
        } => {
            if payload.kind == "process-queued-effect" {
                state.payloads.insert(work_id.clone(), payload.clone());
            } else {
                emit_issue(ctx, state, record, "process-queue-malformed-payload");
            }
        }
        _ if is_terminal_record(record) => {
            let key = format!("{}:{}", record_kind(record), record.record_seq());
            if !state.terminal_records.insert(key) {
                return;
            }
            let Some(payload) = state.payloads.get(record.work_id()).cloned() else {
                emit_issue(ctx, state, record, "process-queue-record-without-payload");
                return;
            };
            let evidence = evidence_from_record(record, &payload);
            ctx.emit(ProcessQueueFact::Evidence(evidence.clone()));
            ctx.emit(ProcessQueueFact::Status(status_from_evidence(
                &evidence, &payload,
            )));
            state.audit_seq += 1;
            ctx.emit(ProcessQueueFact::Audit(audit_from_evidence(
                state.audit_seq,
                &evidence,
                &payload,
            )));
        }
        _ => {}
    }
}

fn is_terminal_record<TEffect>(record: &WorkQueueRecord<TEffect>) -> bool {
    matches!(
        record,
        WorkQueueRecord::WorkCompleted { .. }
            | WorkQueueRecord::AttemptCompleted { .. }
            | WorkQueueRecord::AttemptFailed { .. }
            | WorkQueueRecord::WorkDeadLettered { .. }
            | WorkQueueRecord::WorkCanceled { .. }
    )
}

fn evidence_from_record<TEffect>(
    record: &WorkQueueRecord<ProcessQueuedEffectPayload<TEffect>>,
    payload: &ProcessQueuedEffectPayload<TEffect>,
) -> ProcessQueueEvidence {
    let (result, error, recorded_at_ms) = match record {
        WorkQueueRecord::WorkCompleted {
            result,
            recorded_at_ms,
            ..
        }
        | WorkQueueRecord::AttemptCompleted {
            result,
            recorded_at_ms,
            ..
        } => (result.clone(), None, *recorded_at_ms),
        WorkQueueRecord::AttemptFailed {
            error,
            recorded_at_ms,
            ..
        } => (None, error.clone(), *recorded_at_ms),
        WorkQueueRecord::WorkDeadLettered { recorded_at_ms, .. } => {
            (None, Some("dead-lettered".to_owned()), *recorded_at_ms)
        }
        WorkQueueRecord::WorkCanceled { canceled_at_ms, .. } => {
            (None, Some("canceled".to_owned()), *canceled_at_ms)
        }
        _ => (None, None, 0),
    };
    ProcessQueueEvidence {
        kind: "process-queue-evidence".to_owned(),
        evidence_id: format!("work-queue:{}", record.record_seq()),
        effect_id: payload.effect.id.clone(),
        effect_type: payload.effect.effect_type.clone(),
        work_id: record.work_id().to_owned(),
        queue_record_kind: record_kind(record).to_owned(),
        result,
        error,
        recorded_at_ms,
    }
}

fn status_from_evidence<TEffect>(
    evidence: &ProcessQueueEvidence,
    payload: &ProcessQueuedEffectPayload<TEffect>,
) -> ProcessQueueStatus {
    ProcessQueueStatus {
        kind: "process-queue-status".to_owned(),
        state: "evidence-recorded".to_owned(),
        effect_id: Some(payload.effect.id.clone()),
        effect_type: Some(payload.effect.effect_type.clone()),
        work_id: Some(evidence.work_id.clone()),
        queue_record_kind: Some(evidence.queue_record_kind.clone()),
        evidence_id: Some(evidence.evidence_id.clone()),
        issue_codes: Vec::new(),
    }
}

fn audit_from_evidence<TEffect>(
    seq: u64,
    evidence: &ProcessQueueEvidence,
    payload: &ProcessQueuedEffectPayload<TEffect>,
) -> ProcessQueueAuditRecord {
    ProcessQueueAuditRecord {
        kind: "process-queue-audit".to_owned(),
        seq,
        outcome: "mapped".to_owned(),
        effect_id: Some(payload.effect.id.clone()),
        effect_type: Some(payload.effect.effect_type.clone()),
        work_id: Some(evidence.work_id.clone()),
        queue_record_kind: Some(evidence.queue_record_kind.clone()),
        evidence_id: Some(evidence.evidence_id.clone()),
    }
}

fn emit_issue<TEffect: Clone + 'static>(
    ctx: &Ctx,
    state: &mut ProcessQueueState<TEffect>,
    record: &WorkQueueRecord<ProcessQueuedEffectPayload<TEffect>>,
    code: &str,
) {
    let issue = DataIssue {
        kind: "issue".to_owned(),
        code: code.to_owned(),
        message: format!(
            "Process workQueue recipe could not map workQueue record '{}'",
            record_kind(record)
        ),
        severity: "error".to_owned(),
        source: "process.workQueue".to_owned(),
        topic: None,
        details: Some(format!(
            "work_id={};record_seq={}",
            record.work_id(),
            record.record_seq()
        )),
    };
    ctx.emit(ProcessQueueFact::Issue(issue.clone()));
    ctx.emit(ProcessQueueFact::Status(ProcessQueueStatus {
        kind: "process-queue-status".to_owned(),
        state: "mapping-issue".to_owned(),
        effect_id: None,
        effect_type: None,
        work_id: Some(record.work_id().to_owned()),
        queue_record_kind: Some(record_kind(record).to_owned()),
        evidence_id: None,
        issue_codes: vec![issue.code.clone()],
    }));
    state.audit_seq += 1;
    ctx.emit(ProcessQueueFact::Audit(ProcessQueueAuditRecord {
        kind: "process-queue-audit".to_owned(),
        seq: state.audit_seq,
        outcome: "issue".to_owned(),
        effect_id: None,
        effect_type: None,
        work_id: Some(record.work_id().to_owned()),
        queue_record_kind: Some(record_kind(record).to_owned()),
        evidence_id: None,
    }));
}

fn project<TIn: Clone + 'static, TOut: 'static>(
    graph: &Graph,
    source: &Node<TIn>,
    name: String,
    pick: impl Fn(&TIn) -> Option<TOut> + 'static,
) -> Node<TOut> {
    graph.node_opts::<TOut, _>(
        vec![source.erased()],
        move |ctx| {
            for fact in ctx.batch::<TIn>(0) {
                if let Some(value) = pick(&fact) {
                    ctx.emit(value);
                }
            }
        },
        GraphNodeOpts::named(name),
    )
}

fn record_kind<TEffect>(record: &WorkQueueRecord<TEffect>) -> &'static str {
    match record {
        WorkQueueRecord::WorkAdmitted { .. } => "work-admitted",
        WorkQueueRecord::AdmissionDeduped { .. } => "admission-deduped",
        WorkQueueRecord::WorkScheduled { .. } => "work-scheduled",
        WorkQueueRecord::WorkClaimed { .. } => "work-claimed",
        WorkQueueRecord::LeaseRenewed { .. } => "lease-renewed",
        WorkQueueRecord::WorkReleased { .. } => "work-released",
        WorkQueueRecord::LeaseExpired { .. } => "lease-expired",
        WorkQueueRecord::AttemptCompleted { .. } => "attempt-completed",
        WorkQueueRecord::WorkCompleted { .. } => "work-completed",
        WorkQueueRecord::AttemptFailed { .. } => "attempt-failed",
        WorkQueueRecord::RetryScheduled { .. } => "retry-scheduled",
        WorkQueueRecord::WorkDeadLettered { .. } => "work-dead-lettered",
        WorkQueueRecord::WorkCanceled { .. } => "work-canceled",
    }
}
