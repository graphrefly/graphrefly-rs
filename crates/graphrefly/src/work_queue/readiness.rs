//! workQueue convergence onto scheduled readiness (B95 over D314-D318/D424-D433).
//!
//! This module translates existing Rust workQueue delayed eligibility records
//! into shared scheduled-readiness facts and consumes readiness back into
//! workQueue-owned candidate/status material. It does not append queue lifecycle
//! records and does not claim, expire, cancel, complete, or fail work.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use crate::ctx::Ctx;
use crate::graph::{Graph, GraphNodeOpts};
use crate::identity::{canonical_tuple_key, compound_tuple_key};
use crate::json::JsonValue;
use crate::messaging::DataIssue;
use crate::node::{Node, NodeOpts};
use crate::scheduled_readiness::{
    readiness_issue, ScheduledReadinessAuditRecord, ScheduledReadinessOverdue,
    ScheduledReadinessReady, ScheduledReadinessRequested, SourceRef,
};
use crate::work_queue::{WorkQueueCommand, WorkQueueDerivedState, WorkQueueRecord};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// `WorkQueueReadinessScheduleKind` variants.
pub enum WorkQueueReadinessScheduleKind {
    /// `AdmissionDelay` variant.
    AdmissionDelay,
    /// `WorkScheduled` variant.
    WorkScheduled,
    /// `RetryScheduled` variant.
    RetryScheduled,
    /// `LeaseExpiration` variant.
    LeaseExpiration,
}

impl WorkQueueReadinessScheduleKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::AdmissionDelay => "admission-delay",
            Self::WorkScheduled => "work-scheduled",
            Self::RetryScheduled => "retry-scheduled",
            Self::LeaseExpiration => "lease-expiration",
        }
    }

    fn from_str(value: &str) -> Option<Self> {
        match value {
            "admission-delay" => Some(Self::AdmissionDelay),
            "work-scheduled" => Some(Self::WorkScheduled),
            "retry-scheduled" => Some(Self::RetryScheduled),
            "lease-expiration" => Some(Self::LeaseExpiration),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// `WorkQueueReadinessStatusState` variants.
pub enum WorkQueueReadinessStatusState {
    /// `Translated` variant.
    Translated,
    /// `Candidate` variant.
    Candidate,
    /// `Ignored` variant.
    Ignored,
    /// `Overdue` variant.
    Overdue,
    /// `Issue` variant.
    Issue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// `WorkQueueReadinessCandidateKind` variants.
pub enum WorkQueueReadinessCandidateKind {
    /// `ClaimEligible` variant.
    ClaimEligible,
    /// `LeaseExpirationEligible` variant.
    LeaseExpirationEligible,
}

impl WorkQueueReadinessCandidateKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::ClaimEligible => "claim-eligible",
            Self::LeaseExpirationEligible => "lease-expiration-eligible",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
/// `WorkQueueReadinessStatus` data container.
pub struct WorkQueueReadinessStatus {
    /// `status_id` field for status id.
    pub status_id: String,
    /// `queue_id` field for queue id.
    pub queue_id: String,
    /// `work_id` field for work id.
    pub work_id: Option<String>,
    /// `schedule_id` field for schedule id.
    pub schedule_id: Option<String>,
    /// `state` field for state.
    pub state: WorkQueueReadinessStatusState,
    /// `schedule_kind` field for schedule kind.
    pub schedule_kind: Option<WorkQueueReadinessScheduleKind>,
    /// `ready_at_ms` field for ready at ms.
    pub ready_at_ms: Option<u64>,
    /// `now_ms` field for now ms.
    pub now_ms: Option<u64>,
    /// `issue_code` field for issue code.
    pub issue_code: Option<String>,
    /// `details` field for details.
    pub details: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
/// `WorkQueueReadinessCandidate` data container.
pub struct WorkQueueReadinessCandidate {
    /// `candidate_id` field for candidate id.
    pub candidate_id: String,
    /// `queue_id` field for queue id.
    pub queue_id: String,
    /// `work_id` field for work id.
    pub work_id: String,
    /// `schedule_id` field for schedule id.
    pub schedule_id: String,
    /// `candidate_kind` field for candidate kind.
    pub candidate_kind: WorkQueueReadinessCandidateKind,
    /// `schedule_kind` field for schedule kind.
    pub schedule_kind: WorkQueueReadinessScheduleKind,
    /// `ready_at_ms` field for ready at ms.
    pub ready_at_ms: u64,
    /// `now_ms` field for now ms.
    pub now_ms: u64,
    /// `lease_id` field for lease id.
    pub lease_id: Option<String>,
    /// `attempt` field for attempt.
    pub attempt: Option<u32>,
    /// `worker_id` field for worker id.
    pub worker_id: Option<String>,
    /// `source_refs` field for source refs.
    pub source_refs: Vec<SourceRef>,
}

#[derive(Debug, Clone, PartialEq, Default)]
/// `WorkQueueReadinessViews` data container.
pub struct WorkQueueReadinessViews {
    /// `schedules_by_id` field for schedules by id.
    pub schedules_by_id: BTreeMap<String, ScheduledReadinessRequested>,
    /// `candidates_by_id` field for candidates by id.
    pub candidates_by_id: BTreeMap<String, WorkQueueReadinessCandidate>,
    /// `status_by_id` field for status by id.
    pub status_by_id: BTreeMap<String, WorkQueueReadinessStatus>,
}

#[derive(Clone)]
/// `WorkQueueScheduledReadinessOptions` data container.
pub struct WorkQueueScheduledReadinessOptions<T> {
    /// `name` field for name.
    pub name: Option<String>,
    /// `records` field for records.
    pub records: Vec<Node<WorkQueueRecord<T>>>,
}

impl<T> WorkQueueScheduledReadinessOptions<T> {
    /// Creates or computes `new`.
    pub fn new(records: Vec<Node<WorkQueueRecord<T>>>) -> Self {
        Self {
            name: None,
            records,
        }
    }

    /// Updates or reads `named`.
    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
}

#[derive(Clone)]
/// `WorkQueueReadinessHandoffOptions` data container.
pub struct WorkQueueReadinessHandoffOptions<T> {
    /// `name` field for name.
    pub name: Option<String>,
    /// `records` field for records.
    pub records: Vec<Node<WorkQueueRecord<T>>>,
    /// `ready` field for ready.
    pub ready: Vec<Node<ScheduledReadinessReady>>,
    /// `overdue` field for overdue.
    pub overdue: Vec<Node<ScheduledReadinessOverdue>>,
}

impl<T> WorkQueueReadinessHandoffOptions<T> {
    /// Creates or computes `new`.
    pub fn new(
        records: Vec<Node<WorkQueueRecord<T>>>,
        ready: Vec<Node<ScheduledReadinessReady>>,
    ) -> Self {
        Self {
            name: None,
            records,
            ready,
            overdue: Vec::new(),
        }
    }

    /// Updates or reads `named`.
    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Updates or reads `with_overdue`.
    pub fn with_overdue(mut self, overdue: Vec<Node<ScheduledReadinessOverdue>>) -> Self {
        self.overdue = overdue;
        self
    }
}

#[derive(Clone)]
/// `WorkQueueScheduledReadinessBundle` data container.
pub struct WorkQueueScheduledReadinessBundle {
    /// `readiness_schedules` field for readiness schedules.
    pub readiness_schedules: Node<ScheduledReadinessRequested>,
    /// `status` field for status.
    pub status: Node<WorkQueueReadinessStatus>,
    /// `issues` field for issues.
    pub issues: Node<DataIssue>,
    /// `audit` field for audit.
    pub audit: Node<ScheduledReadinessAuditRecord>,
    /// `views` field for views.
    pub views: Node<WorkQueueReadinessViews>,
}

#[derive(Clone)]
/// `WorkQueueReadinessHandoffBundle` data container.
pub struct WorkQueueReadinessHandoffBundle {
    /// `candidates` field for candidates.
    pub candidates: Node<WorkQueueReadinessCandidate>,
    /// `status` field for status.
    pub status: Node<WorkQueueReadinessStatus>,
    /// `issues` field for issues.
    pub issues: Node<DataIssue>,
    /// `audit` field for audit.
    pub audit: Node<ScheduledReadinessAuditRecord>,
    /// `views` field for views.
    pub views: Node<WorkQueueReadinessViews>,
}

#[derive(Clone)]
/// `WorkQueueLeaseExpirationCommandProjectorOptions` data container.
pub struct WorkQueueLeaseExpirationCommandProjectorOptions {
    /// `name` field for name.
    pub name: Option<String>,
    /// `candidates` field for candidates.
    pub candidates: Vec<Node<WorkQueueReadinessCandidate>>,
    /// `command_prefix` field for command prefix.
    pub command_prefix: Option<String>,
}

impl WorkQueueLeaseExpirationCommandProjectorOptions {
    /// Creates or computes `new`.
    pub fn new(candidates: Vec<Node<WorkQueueReadinessCandidate>>) -> Self {
        Self {
            name: None,
            candidates,
            command_prefix: None,
        }
    }

    /// Updates or reads `named`.
    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Updates or reads `command_prefix`.
    pub fn command_prefix(mut self, command_prefix: impl Into<String>) -> Self {
        self.command_prefix = Some(command_prefix.into());
        self
    }
}

#[derive(Clone)]
enum WorkQueueReadinessFact {
    Schedule(ScheduledReadinessRequested),
    Candidate(WorkQueueReadinessCandidate),
    Status(WorkQueueReadinessStatus),
    Issue(DataIssue),
    Audit(ScheduledReadinessAuditRecord),
    Views(WorkQueueReadinessViews),
}

#[derive(Default)]
struct TranslatorState {
    schedules_by_id: BTreeMap<String, ScheduledReadinessRequested>,
    status_by_id: BTreeMap<String, WorkQueueReadinessStatus>,
    emitted: BTreeSet<String>,
    audit_seq: u64,
}

#[derive(Debug, Clone)]
struct QueueWorkState {
    queue_id: String,
    state: WorkQueueDerivedState,
    effective_ready_at_ms: Option<u64>,
    effective_schedule_kind: Option<WorkQueueReadinessScheduleKind>,
    lease_id: Option<String>,
    attempt: Option<u32>,
    worker_id: Option<String>,
    lease_expires_at_ms: Option<u64>,
}

#[derive(Default)]
struct HandoffState {
    works_by_id: BTreeMap<(String, String), QueueWorkState>,
    ready_by_id: BTreeMap<String, ScheduledReadinessReady>,
    candidates_by_id: BTreeMap<String, WorkQueueReadinessCandidate>,
    status_by_id: BTreeMap<String, WorkQueueReadinessStatus>,
    emitted: BTreeSet<String>,
    audit_seq: u64,
}

/// Creates or computes `work_queue_scheduled_readiness_projector`.
pub fn work_queue_scheduled_readiness_projector<T: Clone + 'static>(
    graph: &Graph,
    opts: WorkQueueScheduledReadinessOptions<T>,
) -> WorkQueueScheduledReadinessBundle {
    let name = opts
        .name
        .clone()
        .unwrap_or_else(|| "workQueueScheduledReadiness".to_owned());
    let deps = opts.records.iter().map(Node::erased).collect::<Vec<_>>();
    let dep_count = opts.records.len();
    let state = Rc::new(RefCell::new(TranslatorState::default()));
    let runtime = graph.node_opts::<WorkQueueReadinessFact, _>(
        deps,
        {
            let state = state.clone();
            move |ctx| {
                let mut state = state.borrow_mut();
                for index in 0..dep_count {
                    for record in ctx.batch::<WorkQueueRecord<T>>(index) {
                        if let Some(schedule) = schedule_from_record(record.as_ref()) {
                            emit_translated_schedule(ctx, &mut state, schedule);
                        }
                    }
                }
                emit_fact(
                    ctx,
                    WorkQueueReadinessFact::Views(WorkQueueReadinessViews {
                        schedules_by_id: state.schedules_by_id.clone(),
                        candidates_by_id: BTreeMap::new(),
                        status_by_id: state.status_by_id.clone(),
                    }),
                );
            }
        },
        {
            let mut node_opts = node_opts(
                format!("{name}/runtime"),
                "workQueueScheduledReadinessProjector",
            );
            node_opts.node.partial = true;
            node_opts
        },
    );
    WorkQueueScheduledReadinessBundle {
        readiness_schedules: project_fact(
            graph,
            &runtime,
            format!("{name}/schedules"),
            "workQueueReadinessSchedules",
            |fact| match fact {
                WorkQueueReadinessFact::Schedule(value) => Some(value.clone()),
                _ => None,
            },
        ),
        status: project_fact(
            graph,
            &runtime,
            format!("{name}/status"),
            "workQueueReadinessStatus",
            |fact| match fact {
                WorkQueueReadinessFact::Status(value) => Some(value.clone()),
                _ => None,
            },
        ),
        issues: project_fact(
            graph,
            &runtime,
            format!("{name}/issues"),
            "workQueueReadinessIssues",
            |fact| match fact {
                WorkQueueReadinessFact::Issue(value) => Some(value.clone()),
                _ => None,
            },
        ),
        audit: project_fact(
            graph,
            &runtime,
            format!("{name}/audit"),
            "workQueueReadinessAudit",
            |fact| match fact {
                WorkQueueReadinessFact::Audit(value) => Some(value.clone()),
                _ => None,
            },
        ),
        views: project_fact(
            graph,
            &runtime,
            format!("{name}/views"),
            "workQueueReadinessViews",
            |fact| match fact {
                WorkQueueReadinessFact::Views(value) => Some(value.clone()),
                _ => None,
            },
        ),
    }
}

/// Creates or computes `work_queue_readiness_handoff_projector`.
pub fn work_queue_readiness_handoff_projector<T: Clone + 'static>(
    graph: &Graph,
    opts: WorkQueueReadinessHandoffOptions<T>,
) -> WorkQueueReadinessHandoffBundle {
    let name = opts
        .name
        .clone()
        .unwrap_or_else(|| "workQueueReadinessHandoff".to_owned());
    let record_count = opts.records.len();
    let ready_start = record_count;
    let overdue_start = ready_start + opts.ready.len();
    let mut deps = Vec::with_capacity(opts.records.len() + opts.ready.len() + opts.overdue.len());
    deps.extend(opts.records.iter().map(Node::erased));
    deps.extend(opts.ready.iter().map(Node::erased));
    deps.extend(opts.overdue.iter().map(Node::erased));
    let state = Rc::new(RefCell::new(HandoffState::default()));
    let runtime = graph.node_opts::<WorkQueueReadinessFact, _>(
        deps,
        {
            let state = state.clone();
            move |ctx| {
                let mut state = state.borrow_mut();
                for index in 0..record_count {
                    for record in ctx.batch::<WorkQueueRecord<T>>(index) {
                        apply_record(&mut state, record.as_ref());
                    }
                }
                for ready in state.ready_by_id.values().cloned().collect::<Vec<_>>() {
                    handoff_ready_inner(ctx, &mut state, &ready);
                }
                for index in ready_start..overdue_start {
                    for ready in ctx.batch::<ScheduledReadinessReady>(index) {
                        handoff_ready(ctx, &mut state, ready.as_ref());
                    }
                }
                for index in overdue_start..(overdue_start + opts.overdue.len()) {
                    for overdue in ctx.batch::<ScheduledReadinessOverdue>(index) {
                        handoff_overdue(ctx, &mut state, overdue.as_ref());
                    }
                }
                emit_fact(
                    ctx,
                    WorkQueueReadinessFact::Views(WorkQueueReadinessViews {
                        schedules_by_id: BTreeMap::new(),
                        candidates_by_id: state.candidates_by_id.clone(),
                        status_by_id: state.status_by_id.clone(),
                    }),
                );
            }
        },
        {
            let mut node_opts = node_opts(
                format!("{name}/runtime"),
                "workQueueReadinessHandoffProjector",
            );
            node_opts.node.partial = true;
            node_opts
        },
    );
    WorkQueueReadinessHandoffBundle {
        candidates: project_fact(
            graph,
            &runtime,
            format!("{name}/candidates"),
            "workQueueReadinessCandidates",
            |fact| match fact {
                WorkQueueReadinessFact::Candidate(value) => Some(value.clone()),
                _ => None,
            },
        ),
        status: project_fact(
            graph,
            &runtime,
            format!("{name}/status"),
            "workQueueReadinessStatus",
            |fact| match fact {
                WorkQueueReadinessFact::Status(value) => Some(value.clone()),
                _ => None,
            },
        ),
        issues: project_fact(
            graph,
            &runtime,
            format!("{name}/issues"),
            "workQueueReadinessIssues",
            |fact| match fact {
                WorkQueueReadinessFact::Issue(value) => Some(value.clone()),
                _ => None,
            },
        ),
        audit: project_fact(
            graph,
            &runtime,
            format!("{name}/audit"),
            "workQueueReadinessAudit",
            |fact| match fact {
                WorkQueueReadinessFact::Audit(value) => Some(value.clone()),
                _ => None,
            },
        ),
        views: project_fact(
            graph,
            &runtime,
            format!("{name}/views"),
            "workQueueReadinessViews",
            |fact| match fact {
                WorkQueueReadinessFact::Views(value) => Some(value.clone()),
                _ => None,
            },
        ),
    }
}

/// Creates or computes `work_queue_lease_expiration_command_projector`.
pub fn work_queue_lease_expiration_command_projector<T: Clone + 'static>(
    graph: &Graph,
    opts: WorkQueueLeaseExpirationCommandProjectorOptions,
) -> Node<WorkQueueCommand<T>> {
    let name = opts
        .name
        .clone()
        .unwrap_or_else(|| "workQueueLeaseExpirationCommands".to_owned());
    let prefix = opts
        .command_prefix
        .clone()
        .unwrap_or_else(|| "readiness-expire".to_owned());
    let dep_count = opts.candidates.len();
    graph.node_opts::<WorkQueueCommand<T>, _>(
        opts.candidates.iter().map(Node::erased).collect(),
        move |ctx| {
            for index in 0..dep_count {
                for candidate in ctx.batch::<WorkQueueReadinessCandidate>(index) {
                    if let Some(command) =
                        work_queue_lease_expiration_command::<T>(candidate.as_ref(), &prefix)
                    {
                        ctx.emit(command);
                    }
                }
            }
        },
        node_opts(name, "workQueueLeaseExpirationCommandProjector"),
    )
}

/// Creates or computes `work_queue_lease_expiration_command`.
pub fn work_queue_lease_expiration_command<T>(
    candidate: &WorkQueueReadinessCandidate,
    command_prefix: &str,
) -> Option<WorkQueueCommand<T>> {
    if candidate.candidate_kind != WorkQueueReadinessCandidateKind::LeaseExpirationEligible {
        return None;
    }
    Some(WorkQueueCommand::ExpireLeases {
        command_id: compound_tuple_key(command_prefix, &[&candidate.candidate_id]),
        queue_id: Some(candidate.queue_id.clone()),
        idempotency_key: Some(compound_tuple_key(
            command_prefix,
            &[&candidate.candidate_id],
        )),
        work_ids: vec![candidate.work_id.clone()],
        limit: Some(1),
        now_ms: Some(candidate.now_ms),
    })
}

fn schedule_from_record<T>(record: &WorkQueueRecord<T>) -> Option<ScheduledReadinessRequested> {
    match record {
        WorkQueueRecord::WorkAdmitted {
            record_seq,
            queue_id,
            work_id,
            not_before_ms: Some(ready_at_ms),
            deadline_ms,
            ..
        } => Some(readiness_schedule(ReadinessScheduleInput {
            queue_id,
            work_id,
            kind: WorkQueueReadinessScheduleKind::AdmissionDelay,
            schedule_id: compound_tuple_key(
                "work-queue-admission-readiness",
                &[queue_id, work_id, &record_seq.to_string()],
            ),
            ready_at_ms: *ready_at_ms,
            deadline_ms: *deadline_ms,
            record_seq: *record_seq,
            lease: None,
        })),
        WorkQueueRecord::WorkScheduled {
            record_seq,
            queue_id,
            work_id,
            command_id,
            schedule_id,
            not_before_ms,
            deadline_ms,
            ..
        } => Some(readiness_schedule(ReadinessScheduleInput {
            queue_id,
            work_id,
            kind: WorkQueueReadinessScheduleKind::WorkScheduled,
            schedule_id: compound_tuple_key(
                "work-queue-scheduled-readiness",
                &[
                    queue_id,
                    work_id,
                    schedule_id.as_deref().unwrap_or(command_id),
                    &record_seq.to_string(),
                ],
            ),
            ready_at_ms: *not_before_ms,
            deadline_ms: *deadline_ms,
            record_seq: *record_seq,
            lease: None,
        })),
        WorkQueueRecord::RetryScheduled {
            record_seq,
            queue_id,
            work_id,
            command_id,
            retry_at_ms,
            ..
        } => Some(readiness_schedule(ReadinessScheduleInput {
            queue_id,
            work_id,
            kind: WorkQueueReadinessScheduleKind::RetryScheduled,
            schedule_id: compound_tuple_key(
                "work-queue-retry-readiness",
                &[queue_id, work_id, command_id, &record_seq.to_string()],
            ),
            ready_at_ms: *retry_at_ms,
            deadline_ms: None,
            record_seq: *record_seq,
            lease: None,
        })),
        WorkQueueRecord::WorkClaimed {
            record_seq,
            queue_id,
            work_id,
            lease_id,
            attempt,
            worker_id,
            lease_expires_at_ms,
            ..
        }
        | WorkQueueRecord::LeaseRenewed {
            record_seq,
            queue_id,
            work_id,
            lease_id,
            attempt,
            worker_id,
            lease_expires_at_ms,
            ..
        } => Some(readiness_schedule(ReadinessScheduleInput {
            queue_id,
            work_id,
            kind: WorkQueueReadinessScheduleKind::LeaseExpiration,
            schedule_id: compound_tuple_key(
                "work-queue-lease-readiness",
                &[
                    queue_id,
                    work_id,
                    lease_id,
                    &attempt.to_string(),
                    &record_seq.to_string(),
                ],
            ),
            ready_at_ms: *lease_expires_at_ms,
            deadline_ms: None,
            record_seq: *record_seq,
            lease: Some((lease_id.clone(), *attempt, worker_id.clone())),
        })),
        _ => None,
    }
}

struct ReadinessScheduleInput<'a> {
    queue_id: &'a str,
    work_id: &'a str,
    kind: WorkQueueReadinessScheduleKind,
    schedule_id: String,
    ready_at_ms: u64,
    deadline_ms: Option<u64>,
    record_seq: u64,
    lease: Option<(String, u32, String)>,
}

fn readiness_schedule(input: ReadinessScheduleInput<'_>) -> ScheduledReadinessRequested {
    let mut metadata = BTreeMap::from([
        (
            "queueId".to_owned(),
            JsonValue::from(input.queue_id.to_owned()),
        ),
        (
            "workId".to_owned(),
            JsonValue::from(input.work_id.to_owned()),
        ),
        (
            "scheduleKind".to_owned(),
            JsonValue::from(input.kind.as_str().to_owned()),
        ),
        ("recordSeq".to_owned(), JsonValue::from(input.record_seq)),
    ]);
    if let Some((lease_id, attempt, worker_id)) = input.lease {
        metadata.insert("leaseId".to_owned(), JsonValue::from(lease_id));
        metadata.insert("attempt".to_owned(), JsonValue::from(attempt));
        metadata.insert("workerId".to_owned(), JsonValue::from(worker_id));
    }
    ScheduledReadinessRequested {
        schedule_id: input.schedule_id.clone(),
        subject_refs: vec![
            SourceRef::new("work-queue", input.queue_id.to_owned()),
            SourceRef::new("work-queue-work", input.work_id.to_owned()),
        ],
        ready_at_ms: input.ready_at_ms,
        deadline_ms: input.deadline_ms,
        reason: Some(input.kind.as_str().to_owned()),
        policy_refs: Vec::new(),
        source_refs: vec![SourceRef::new(
            format!("work-queue-{}", input.kind.as_str()),
            input.schedule_id,
        )],
        metadata: Some(metadata),
    }
}

fn emit_translated_schedule(
    ctx: &Ctx,
    state: &mut TranslatorState,
    schedule: ScheduledReadinessRequested,
) {
    let schedule_id = schedule.schedule_id.clone();
    if state
        .schedules_by_id
        .insert(schedule_id.clone(), schedule.clone())
        .is_none()
    {
        emit_fact(ctx, WorkQueueReadinessFact::Schedule(schedule.clone()));
        emit_status(
            ctx,
            state,
            WorkQueueReadinessStatus {
                status_id: compound_tuple_key("work-queue-readiness-translated", &[&schedule_id]),
                queue_id: metadata_string(&schedule, "queueId").unwrap_or_default(),
                work_id: metadata_string(&schedule, "workId"),
                schedule_id: Some(schedule_id),
                state: WorkQueueReadinessStatusState::Translated,
                schedule_kind: metadata_kind(&schedule),
                ready_at_ms: Some(schedule.ready_at_ms),
                now_ms: None,
                issue_code: None,
                details: None,
            },
        );
        emit_translator_audit(ctx, state, "work-queue-readiness-translated", &schedule);
    }
}

fn apply_record<T>(state: &mut HandoffState, record: &WorkQueueRecord<T>) {
    match record {
        WorkQueueRecord::WorkAdmitted {
            queue_id,
            work_id,
            not_before_ms,
            ..
        } => {
            prune_candidates_for_work(state, queue_id, work_id);
            state.works_by_id.insert(
                work_key(queue_id, work_id),
                QueueWorkState {
                    queue_id: queue_id.clone(),
                    state: if not_before_ms.is_some() {
                        WorkQueueDerivedState::Scheduled
                    } else {
                        WorkQueueDerivedState::Ready
                    },
                    effective_ready_at_ms: *not_before_ms,
                    effective_schedule_kind: not_before_ms
                        .map(|_| WorkQueueReadinessScheduleKind::AdmissionDelay),
                    lease_id: None,
                    attempt: None,
                    worker_id: None,
                    lease_expires_at_ms: None,
                },
            );
        }
        WorkQueueRecord::WorkScheduled {
            queue_id,
            work_id,
            not_before_ms,
            ..
        } => set_work_delayed_state(
            state,
            queue_id,
            work_id,
            WorkQueueDerivedState::Scheduled,
            Some(*not_before_ms),
            Some(WorkQueueReadinessScheduleKind::WorkScheduled),
        ),
        WorkQueueRecord::RetryScheduled {
            queue_id,
            work_id,
            retry_at_ms,
            ..
        } => set_work_delayed_state(
            state,
            queue_id,
            work_id,
            WorkQueueDerivedState::RetryWait,
            Some(*retry_at_ms),
            Some(WorkQueueReadinessScheduleKind::RetryScheduled),
        ),
        WorkQueueRecord::WorkClaimed {
            queue_id,
            work_id,
            lease_id,
            attempt,
            worker_id,
            lease_expires_at_ms,
            ..
        }
        | WorkQueueRecord::LeaseRenewed {
            queue_id,
            work_id,
            lease_id,
            attempt,
            worker_id,
            lease_expires_at_ms,
            ..
        } => {
            prune_candidates_for_work(state, queue_id, work_id);
            state.works_by_id.insert(
                work_key(queue_id, work_id),
                QueueWorkState {
                    queue_id: queue_id.clone(),
                    state: WorkQueueDerivedState::Leased,
                    effective_ready_at_ms: Some(*lease_expires_at_ms),
                    effective_schedule_kind: Some(WorkQueueReadinessScheduleKind::LeaseExpiration),
                    lease_id: Some(lease_id.clone()),
                    attempt: Some(*attempt),
                    worker_id: Some(worker_id.clone()),
                    lease_expires_at_ms: Some(*lease_expires_at_ms),
                },
            );
        }
        WorkQueueRecord::WorkReleased {
            queue_id, work_id, ..
        }
        | WorkQueueRecord::LeaseExpired {
            queue_id, work_id, ..
        } => set_work_state(state, queue_id, work_id, WorkQueueDerivedState::Ready),
        WorkQueueRecord::WorkCompleted {
            queue_id, work_id, ..
        } => set_work_state(state, queue_id, work_id, WorkQueueDerivedState::Completed),
        WorkQueueRecord::WorkCanceled {
            queue_id, work_id, ..
        } => set_work_state(state, queue_id, work_id, WorkQueueDerivedState::Canceled),
        WorkQueueRecord::WorkDeadLettered {
            queue_id, work_id, ..
        } => set_work_state(
            state,
            queue_id,
            work_id,
            WorkQueueDerivedState::DeadLettered,
        ),
        WorkQueueRecord::AttemptFailed { .. }
        | WorkQueueRecord::AttemptCompleted { .. }
        | WorkQueueRecord::AdmissionDeduped { .. } => {}
    }
}

fn set_work_state(
    state: &mut HandoffState,
    queue_id: &str,
    work_id: &str,
    next: WorkQueueDerivedState,
) {
    let terminal = matches!(
        next,
        WorkQueueDerivedState::Completed
            | WorkQueueDerivedState::Canceled
            | WorkQueueDerivedState::DeadLettered
    );
    if terminal {
        prune_candidates_for_work(state, queue_id, work_id);
    }
    set_work_delayed_state(state, queue_id, work_id, next, None, None);
}

fn set_work_delayed_state(
    state: &mut HandoffState,
    queue_id: &str,
    work_id: &str,
    next: WorkQueueDerivedState,
    effective_ready_at_ms: Option<u64>,
    effective_schedule_kind: Option<WorkQueueReadinessScheduleKind>,
) {
    prune_candidates_for_work(state, queue_id, work_id);
    state
        .works_by_id
        .entry(work_key(queue_id, work_id))
        .and_modify(|work| {
            work.state = next;
            work.effective_ready_at_ms = effective_ready_at_ms;
            work.effective_schedule_kind = effective_schedule_kind;
            if next != WorkQueueDerivedState::Leased {
                work.lease_id = None;
                work.attempt = None;
                work.worker_id = None;
                work.lease_expires_at_ms = None;
            }
        })
        .or_insert_with(|| QueueWorkState {
            queue_id: queue_id.to_owned(),
            state: next,
            effective_ready_at_ms,
            effective_schedule_kind,
            lease_id: None,
            attempt: None,
            worker_id: None,
            lease_expires_at_ms: None,
        });
}

fn handoff_ready(ctx: &Ctx, state: &mut HandoffState, ready: &ScheduledReadinessReady) {
    state
        .ready_by_id
        .insert(ready.schedule_id.clone(), ready.clone());
    handoff_ready_inner(ctx, state, ready);
}

fn handoff_ready_inner(ctx: &Ctx, state: &mut HandoffState, ready: &ScheduledReadinessReady) {
    let Some(queue_id) = ready_metadata_string(ready, "queueId") else {
        emit_handoff_issue(
            ctx,
            state,
            ready.schedule_id.clone(),
            "missing queueId metadata",
        );
        return;
    };
    let Some(work_id) = ready_metadata_string(ready, "workId") else {
        emit_handoff_issue(
            ctx,
            state,
            ready.schedule_id.clone(),
            "missing workId metadata",
        );
        return;
    };
    let Some(schedule_kind) = ready_metadata_string(ready, "scheduleKind")
        .and_then(|value| WorkQueueReadinessScheduleKind::from_str(&value))
    else {
        emit_handoff_issue(
            ctx,
            state,
            ready.schedule_id.clone(),
            "missing scheduleKind metadata",
        );
        return;
    };
    let Some(work) = state
        .works_by_id
        .get(&work_key(&queue_id, &work_id))
        .cloned()
    else {
        emit_handoff_status(
            ctx,
            state,
            ignored_status(&queue_id, &work_id, ready, schedule_kind, "unknown-work"),
        );
        return;
    };
    if work.queue_id != queue_id {
        emit_handoff_status(
            ctx,
            state,
            ignored_status(&queue_id, &work_id, ready, schedule_kind, "queue-mismatch"),
        );
        return;
    }
    let candidate = match schedule_kind {
        WorkQueueReadinessScheduleKind::AdmissionDelay
        | WorkQueueReadinessScheduleKind::WorkScheduled
        | WorkQueueReadinessScheduleKind::RetryScheduled => {
            if work.effective_ready_at_ms != Some(ready.ready_at_ms) {
                emit_handoff_status(
                    ctx,
                    state,
                    ignored_status(
                        &queue_id,
                        &work_id,
                        ready,
                        schedule_kind,
                        "superseded-readiness",
                    ),
                );
                return;
            }
            if work
                .effective_schedule_kind
                .is_some_and(|effective| effective != schedule_kind)
                && matches!(
                    work.state,
                    WorkQueueDerivedState::Scheduled | WorkQueueDerivedState::RetryWait
                )
            {
                emit_handoff_status(
                    ctx,
                    state,
                    ignored_status(
                        &queue_id,
                        &work_id,
                        ready,
                        schedule_kind,
                        "stale-readiness-kind",
                    ),
                );
                return;
            }
            if matches!(
                work.state,
                WorkQueueDerivedState::Completed
                    | WorkQueueDerivedState::Canceled
                    | WorkQueueDerivedState::DeadLettered
                    | WorkQueueDerivedState::Leased
            ) {
                prune_candidates_for_work(state, &queue_id, &work_id);
                emit_handoff_status(
                    ctx,
                    state,
                    ignored_status(&queue_id, &work_id, ready, schedule_kind, "stale-readiness"),
                );
                return;
            }
            WorkQueueReadinessCandidate {
                candidate_id: compound_tuple_key(
                    "work-queue-readiness-candidate",
                    &[
                        &ready.schedule_id,
                        WorkQueueReadinessCandidateKind::ClaimEligible.as_str(),
                    ],
                ),
                queue_id,
                work_id,
                schedule_id: ready.schedule_id.clone(),
                candidate_kind: WorkQueueReadinessCandidateKind::ClaimEligible,
                schedule_kind,
                ready_at_ms: ready.ready_at_ms,
                now_ms: ready.now_ms,
                lease_id: None,
                attempt: None,
                worker_id: None,
                source_refs: ready.source_refs.clone(),
            }
        }
        WorkQueueReadinessScheduleKind::LeaseExpiration => {
            let lease_id = ready_metadata_string(ready, "leaseId");
            let attempt =
                ready_metadata_u64(ready, "attempt").and_then(|value| u32::try_from(value).ok());
            if work.state != WorkQueueDerivedState::Leased
                || work.lease_id != lease_id
                || work.attempt != attempt
                || work.lease_expires_at_ms != Some(ready.ready_at_ms)
            {
                prune_candidates_for_work(state, &queue_id, &work_id);
                emit_handoff_status(
                    ctx,
                    state,
                    ignored_status(
                        &queue_id,
                        &work_id,
                        ready,
                        schedule_kind,
                        "stale-lease-readiness",
                    ),
                );
                return;
            }
            WorkQueueReadinessCandidate {
                candidate_id: compound_tuple_key(
                    "work-queue-readiness-candidate",
                    &[
                        &ready.schedule_id,
                        WorkQueueReadinessCandidateKind::LeaseExpirationEligible.as_str(),
                    ],
                ),
                queue_id,
                work_id,
                schedule_id: ready.schedule_id.clone(),
                candidate_kind: WorkQueueReadinessCandidateKind::LeaseExpirationEligible,
                schedule_kind,
                ready_at_ms: ready.ready_at_ms,
                now_ms: ready.now_ms,
                lease_id,
                attempt,
                worker_id: work.worker_id,
                source_refs: ready.source_refs.clone(),
            }
        }
    };
    emit_candidate(ctx, state, candidate);
}

fn work_key(queue_id: &str, work_id: &str) -> (String, String) {
    (queue_id.to_owned(), work_id.to_owned())
}

fn prune_candidates_for_work(state: &mut HandoffState, queue_id: &str, work_id: &str) {
    state
        .candidates_by_id
        .retain(|_, candidate| candidate.queue_id != queue_id || candidate.work_id != work_id);
}

fn handoff_overdue(ctx: &Ctx, state: &mut HandoffState, overdue: &ScheduledReadinessOverdue) {
    let queue_id = overdue_metadata_string(overdue, "queueId").unwrap_or_default();
    let work_id = overdue_metadata_string(overdue, "workId");
    let schedule_kind = overdue_metadata_string(overdue, "scheduleKind")
        .and_then(|value| WorkQueueReadinessScheduleKind::from_str(&value));
    emit_handoff_status(
        ctx,
        state,
        WorkQueueReadinessStatus {
            status_id: compound_tuple_key("work-queue-readiness-overdue", &[&overdue.schedule_id]),
            queue_id,
            work_id,
            schedule_id: Some(overdue.schedule_id.clone()),
            state: WorkQueueReadinessStatusState::Overdue,
            schedule_kind,
            ready_at_ms: Some(overdue.ready_at_ms),
            now_ms: Some(overdue.now_ms),
            issue_code: None,
            details: Some("deadline visibility only; no queue lifecycle mutation".to_owned()),
        },
    );
}

fn emit_candidate(ctx: &Ctx, state: &mut HandoffState, candidate: WorkQueueReadinessCandidate) {
    state
        .candidates_by_id
        .insert(candidate.candidate_id.clone(), candidate.clone());
    if state
        .emitted
        .insert(compound_tuple_key("candidate", &[&candidate.candidate_id]))
    {
        emit_fact(ctx, WorkQueueReadinessFact::Candidate(candidate.clone()));
        emit_handoff_status(
            ctx,
            state,
            WorkQueueReadinessStatus {
                status_id: compound_tuple_key(
                    "work-queue-readiness-candidate-status",
                    &[&candidate.candidate_id],
                ),
                queue_id: candidate.queue_id.clone(),
                work_id: Some(candidate.work_id.clone()),
                schedule_id: Some(candidate.schedule_id.clone()),
                state: WorkQueueReadinessStatusState::Candidate,
                schedule_kind: Some(candidate.schedule_kind),
                ready_at_ms: Some(candidate.ready_at_ms),
                now_ms: Some(candidate.now_ms),
                issue_code: None,
                details: Some(format!("{:?}", candidate.candidate_kind)),
            },
        );
        emit_handoff_audit(ctx, state, "work-queue-readiness-candidate", &candidate);
    }
}

fn emit_status(ctx: &Ctx, state: &mut TranslatorState, status: WorkQueueReadinessStatus) {
    state
        .status_by_id
        .insert(status.status_id.clone(), status.clone());
    if state
        .emitted
        .insert(compound_tuple_key("status", &[&format!("{status:?}")]))
    {
        emit_fact(ctx, WorkQueueReadinessFact::Status(status));
    }
}

fn emit_handoff_status(ctx: &Ctx, state: &mut HandoffState, status: WorkQueueReadinessStatus) {
    state
        .status_by_id
        .insert(status.status_id.clone(), status.clone());
    if state
        .emitted
        .insert(compound_tuple_key("status", &[&format!("{status:?}")]))
    {
        emit_fact(ctx, WorkQueueReadinessFact::Status(status));
    }
}

fn emit_handoff_issue(ctx: &Ctx, state: &mut HandoffState, schedule_id: String, detail: &str) {
    let issue = readiness_issue(
        "work-queue-readiness-malformed-ready",
        "workQueue readiness handoff requires workQueue metadata on ready facts.",
        &schedule_id,
        Some(detail.to_owned()),
        "error",
    );
    if state
        .emitted
        .insert(canonical_tuple_key(&["issue", &schedule_id, detail]))
    {
        emit_fact(ctx, WorkQueueReadinessFact::Issue(issue));
    }
}

fn emit_translator_audit(
    ctx: &Ctx,
    state: &mut TranslatorState,
    kind: &str,
    schedule: &ScheduledReadinessRequested,
) {
    state.audit_seq += 1;
    emit_fact(
        ctx,
        WorkQueueReadinessFact::Audit(ScheduledReadinessAuditRecord {
            id: format!("work-queue-readiness-audit-{}", state.audit_seq),
            kind: kind.to_owned(),
            subject_id: Some(schedule.schedule_id.clone()),
            source_refs: schedule.source_refs.clone(),
            metadata: schedule.metadata.clone(),
        }),
    );
}

fn emit_handoff_audit(
    ctx: &Ctx,
    state: &mut HandoffState,
    kind: &str,
    candidate: &WorkQueueReadinessCandidate,
) {
    state.audit_seq += 1;
    emit_fact(
        ctx,
        WorkQueueReadinessFact::Audit(ScheduledReadinessAuditRecord {
            id: format!("work-queue-readiness-handoff-audit-{}", state.audit_seq),
            kind: kind.to_owned(),
            subject_id: Some(candidate.schedule_id.clone()),
            source_refs: candidate.source_refs.clone(),
            metadata: Some(BTreeMap::from([
                (
                    "queueId".to_owned(),
                    JsonValue::from(candidate.queue_id.clone()),
                ),
                (
                    "workId".to_owned(),
                    JsonValue::from(candidate.work_id.clone()),
                ),
            ])),
        }),
    );
}

fn ignored_status(
    queue_id: &str,
    work_id: &str,
    ready: &ScheduledReadinessReady,
    schedule_kind: WorkQueueReadinessScheduleKind,
    detail: &str,
) -> WorkQueueReadinessStatus {
    WorkQueueReadinessStatus {
        status_id: compound_tuple_key(
            "work-queue-readiness-ignored",
            &[&ready.schedule_id, detail],
        ),
        queue_id: queue_id.to_owned(),
        work_id: Some(work_id.to_owned()),
        schedule_id: Some(ready.schedule_id.clone()),
        state: WorkQueueReadinessStatusState::Ignored,
        schedule_kind: Some(schedule_kind),
        ready_at_ms: Some(ready.ready_at_ms),
        now_ms: Some(ready.now_ms),
        issue_code: None,
        details: Some(detail.to_owned()),
    }
}

fn metadata_string(schedule: &ScheduledReadinessRequested, key: &str) -> Option<String> {
    schedule
        .metadata
        .as_ref()?
        .get(key)?
        .as_str()
        .map(str::to_owned)
}

fn metadata_kind(schedule: &ScheduledReadinessRequested) -> Option<WorkQueueReadinessScheduleKind> {
    metadata_string(schedule, "scheduleKind")
        .as_deref()
        .and_then(WorkQueueReadinessScheduleKind::from_str)
}

fn ready_metadata_string(ready: &ScheduledReadinessReady, key: &str) -> Option<String> {
    ready
        .metadata
        .as_ref()?
        .get(key)?
        .as_str()
        .map(str::to_owned)
}

fn ready_metadata_u64(ready: &ScheduledReadinessReady, key: &str) -> Option<u64> {
    ready.metadata.as_ref()?.get(key)?.as_u64()
}

fn overdue_metadata_string(overdue: &ScheduledReadinessOverdue, key: &str) -> Option<String> {
    overdue
        .metadata
        .as_ref()?
        .get(key)?
        .as_str()
        .map(str::to_owned)
}

fn project_fact<T: Clone + 'static>(
    graph: &Graph,
    runtime: &Node<WorkQueueReadinessFact>,
    name: String,
    factory: &'static str,
    select: impl Fn(&WorkQueueReadinessFact) -> Option<T> + 'static,
) -> Node<T> {
    graph.node_opts::<T, _>(
        vec![runtime.erased()],
        move |ctx| {
            for fact in ctx.batch::<WorkQueueReadinessFact>(0) {
                if let Some(value) = select(&fact) {
                    ctx.emit(value);
                }
            }
        },
        node_opts(name, factory),
    )
}

fn emit_fact(ctx: &Ctx, fact: WorkQueueReadinessFact) {
    ctx.emit(fact);
}

fn node_opts(name: impl Into<String>, factory: impl Into<String>) -> GraphNodeOpts {
    let mut opts = GraphNodeOpts::named(name);
    opts.node = NodeOpts {
        factory: Some(factory.into()),
        complete_when_deps_complete: false,
        error_when_deps_error: false,
        ..opts.node
    };
    opts
}
