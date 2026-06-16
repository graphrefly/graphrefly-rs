//! Optional CQRS-over-workQueue recipe (D350/D352/D353).
//!
//! Queue lifecycle remains worker disposition. CQRS truth remains the ordered
//! event/status/error facts emitted by the CQRS core.

use std::collections::{BTreeMap, BTreeSet};

use crate::cqrs::{CqrsCommand, CqrsError, CqrsErrorCode, CqrsStatus, CqrsStatusState};
use crate::ctx::Ctx;
use crate::graph::{Graph, GraphNodeOpts};
use crate::messaging::DataIssue;
use crate::node::Node;
use crate::work_queue::{WorkQueueCommand, WorkQueueRecord};

#[derive(Debug, Clone, PartialEq)]
pub struct CqrsQueuedCommandPayload<TCommand> {
    pub kind: String,
    pub command: CqrsCommand<TCommand>,
    pub idempotency_key: Option<String>,
    pub source_refs: Vec<String>,
    pub policy_refs: Vec<String>,
    pub actor_refs: Vec<String>,
    pub audit_refs: Vec<String>,
    pub metadata: Option<String>,
}

impl<TCommand> CqrsQueuedCommandPayload<TCommand> {
    pub fn new(command: CqrsCommand<TCommand>) -> Self {
        Self {
            kind: "cqrs-queued-command".to_owned(),
            command,
            idempotency_key: None,
            source_refs: Vec::new(),
            policy_refs: Vec::new(),
            actor_refs: Vec::new(),
            audit_refs: Vec::new(),
            metadata: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CqrsWorkQueueAttempt<TCommand> {
    pub kind: String,
    pub work_id: String,
    pub lease_id: String,
    pub queue_attempt: u32,
    pub worker_id: String,
    pub command: CqrsCommand<TCommand>,
    pub payload: CqrsQueuedCommandPayload<TCommand>,
    pub source_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CqrsWorkQueueOutcome<TCommand> {
    Accepted {
        status: CqrsStatus,
    },
    Rejected {
        status: CqrsStatus,
        error: Option<Box<CqrsError<TCommand>>>,
    },
    Release {
        reason: Option<String>,
    },
}

#[derive(Clone, Default)]
pub struct CqrsWorkQueuePolicy {
    pub deterministic_handler_failures: Vec<CqrsErrorCode>,
}

impl CqrsWorkQueuePolicy {
    pub fn deterministic_handler_failure(mut self, code: CqrsErrorCode) -> Self {
        if !self.deterministic_handler_failures.contains(&code) {
            self.deterministic_handler_failures.push(code);
        }
        self
    }

    fn retryable_failure(&self, code: Option<CqrsErrorCode>) -> bool {
        match code {
            Some(code @ (CqrsErrorCode::HandlerThrew | CqrsErrorCode::ClockThrew)) => {
                !self.deterministic_handler_failures.contains(&code)
            }
            None => true,
            Some(_) => false,
        }
    }
}

#[derive(Clone)]
pub struct CqrsWorkQueueRecipeOptions<TCommand> {
    pub name: String,
    pub records: Node<WorkQueueRecord<CqrsQueuedCommandPayload<TCommand>>>,
    pub status: Node<CqrsStatus>,
    pub errors: Option<Node<CqrsError<TCommand>>>,
    pub worker_id: Option<String>,
    pub policy: CqrsWorkQueuePolicy,
}

impl<TCommand> CqrsWorkQueueRecipeOptions<TCommand> {
    pub fn new(
        records: Node<WorkQueueRecord<CqrsQueuedCommandPayload<TCommand>>>,
        status: Node<CqrsStatus>,
    ) -> Self {
        Self {
            name: "cqrsWorkQueue".to_owned(),
            records,
            status,
            errors: None,
            worker_id: None,
            policy: CqrsWorkQueuePolicy::default(),
        }
    }

    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    pub fn with_errors(mut self, errors: Node<CqrsError<TCommand>>) -> Self {
        self.errors = Some(errors);
        self
    }

    pub fn for_worker(mut self, worker_id: impl Into<String>) -> Self {
        self.worker_id = Some(worker_id.into());
        self
    }

    pub fn with_policy(mut self, policy: CqrsWorkQueuePolicy) -> Self {
        self.policy = policy;
        self
    }
}

#[derive(Clone)]
pub struct CqrsWorkQueueRecipeBundle<TCommand> {
    pub attempts: Node<CqrsWorkQueueAttempt<TCommand>>,
    pub dispatches: Node<CqrsCommand<TCommand>>,
    pub commands: Node<WorkQueueCommand<CqrsQueuedCommandPayload<TCommand>>>,
    pub issues: Node<DataIssue>,
}

#[derive(Clone)]
enum CqrsQueueFact<TCommand> {
    Attempt(CqrsWorkQueueAttempt<TCommand>),
    Dispatch(CqrsCommand<TCommand>),
    Command(WorkQueueCommand<CqrsQueuedCommandPayload<TCommand>>),
    Issue(DataIssue),
}

#[derive(Clone)]
struct CqrsQueueState<TCommand> {
    payloads: BTreeMap<String, CqrsQueuedCommandPayload<TCommand>>,
    active_claims: BTreeMap<String, Vec<CqrsWorkQueueAttempt<TCommand>>>,
    errors: BTreeMap<String, CqrsError<TCommand>>,
    terminal_claims: BTreeSet<String>,
}

impl<TCommand> Default for CqrsQueueState<TCommand> {
    fn default() -> Self {
        Self {
            payloads: BTreeMap::new(),
            active_claims: BTreeMap::new(),
            errors: BTreeMap::new(),
            terminal_claims: BTreeSet::new(),
        }
    }
}

pub fn cqrs_work_queue_recipe<TCommand: Clone + 'static>(
    graph: &Graph,
    opts: CqrsWorkQueueRecipeOptions<TCommand>,
) -> CqrsWorkQueueRecipeBundle<TCommand> {
    let name = opts.name.clone();
    let mut deps = vec![opts.records.erased(), opts.status.erased()];
    if let Some(errors) = &opts.errors {
        deps.push(errors.erased());
    }
    let runtime = graph.node_opts::<CqrsQueueFact<TCommand>, _>(
        deps,
        move |ctx| {
            let mut state = ctx
                .state_get::<CqrsQueueState<TCommand>>()
                .map(|state| (*state).clone())
                .unwrap_or_default();
            for record in ctx.batch::<WorkQueueRecord<CqrsQueuedCommandPayload<TCommand>>>(0) {
                reduce_record(ctx, &mut state, &record, &opts);
            }
            if opts.errors.is_some() {
                for error in ctx.batch::<CqrsError<TCommand>>(2) {
                    if let Some(command) = &error.command {
                        state.errors.insert(command.id.clone(), (*error).clone());
                    }
                }
            }
            for status in ctx.batch::<CqrsStatus>(1) {
                reduce_status(ctx, &mut state, &status, &opts);
            }
            ctx.state_set(state);
            ctx.state_persist(true);
        },
        GraphNodeOpts::named(format!("{name}/runtime")),
    );
    CqrsWorkQueueRecipeBundle {
        attempts: project(
            graph,
            &runtime,
            format!("{name}/attempts"),
            |fact| match fact {
                CqrsQueueFact::Attempt(attempt) => Some(attempt.clone()),
                _ => None,
            },
        ),
        dispatches: project(
            graph,
            &runtime,
            format!("{name}/dispatches"),
            |fact| match fact {
                CqrsQueueFact::Dispatch(command) => Some(command.clone()),
                _ => None,
            },
        ),
        commands: project(
            graph,
            &runtime,
            format!("{name}/commands"),
            |fact| match fact {
                CqrsQueueFact::Command(command) => Some(command.clone()),
                _ => None,
            },
        ),
        issues: project(
            graph,
            &runtime,
            format!("{name}/issues"),
            |fact| match fact {
                CqrsQueueFact::Issue(issue) => Some(issue.clone()),
                _ => None,
            },
        ),
    }
}

pub fn cqrs_submit_command<TCommand: Clone>(
    command: CqrsCommand<TCommand>,
) -> WorkQueueCommand<CqrsQueuedCommandPayload<TCommand>> {
    let command_id = format!("{}:cqrs-work-queue-submit", command.id);
    let mut payload = CqrsQueuedCommandPayload::new(command.clone());
    payload.idempotency_key = Some(command.id.clone());
    WorkQueueCommand::Submit {
        payload,
        command_id,
        queue_id: None,
        idempotency_key: Some(command.id),
    }
}

pub fn cqrs_work_queue_disposition_command<TCommand: Clone>(
    attempt: &CqrsWorkQueueAttempt<TCommand>,
    outcome: CqrsWorkQueueOutcome<TCommand>,
    policy: &CqrsWorkQueuePolicy,
) -> WorkQueueCommand<CqrsQueuedCommandPayload<TCommand>> {
    match outcome {
        CqrsWorkQueueOutcome::Release { reason } => WorkQueueCommand::Release {
            command_id: disposition_command_id(attempt, "release"),
            queue_id: None,
            idempotency_key: None,
            work_id: attempt.work_id.clone(),
            lease_id: attempt.lease_id.clone(),
            attempt: attempt.queue_attempt,
            worker_id: attempt.worker_id.clone(),
            reason,
            now_ms: None,
        },
        CqrsWorkQueueOutcome::Accepted { status } => WorkQueueCommand::Complete {
            command_id: disposition_command_id(attempt, "complete"),
            queue_id: None,
            idempotency_key: None,
            work_id: attempt.work_id.clone(),
            lease_id: attempt.lease_id.clone(),
            attempt: attempt.queue_attempt,
            worker_id: attempt.worker_id.clone(),
            result: Some(format!(
                "cqrs-accepted:command_id={};event_count={}",
                status.command_id.unwrap_or_default(),
                status.event_count
            )),
            now_ms: None,
        },
        CqrsWorkQueueOutcome::Rejected { status, error } => {
            let code = status.error_code;
            if matches!(
                code,
                Some(CqrsErrorCode::HandlerThrew | CqrsErrorCode::ClockThrew) | None
            ) {
                WorkQueueCommand::Fail {
                    command_id: disposition_command_id(attempt, "fail"),
                    queue_id: None,
                    idempotency_key: None,
                    work_id: attempt.work_id.clone(),
                    lease_id: attempt.lease_id.clone(),
                    attempt: attempt.queue_attempt,
                    worker_id: attempt.worker_id.clone(),
                    error: Some(error_message(code, error.as_deref())),
                    retryable: Some(policy.retryable_failure(code)),
                    now_ms: None,
                }
            } else {
                WorkQueueCommand::Complete {
                    command_id: disposition_command_id(attempt, "complete"),
                    queue_id: None,
                    idempotency_key: None,
                    work_id: attempt.work_id.clone(),
                    lease_id: attempt.lease_id.clone(),
                    attempt: attempt.queue_attempt,
                    worker_id: attempt.worker_id.clone(),
                    result: Some(format!(
                        "cqrs-rejected:command_id={};error_code={:?};event_count={}",
                        status.command_id.unwrap_or_default(),
                        code,
                        status.event_count
                    )),
                    now_ms: None,
                }
            }
        }
    }
}

fn reduce_record<TCommand: Clone + 'static>(
    ctx: &Ctx,
    state: &mut CqrsQueueState<TCommand>,
    record: &WorkQueueRecord<CqrsQueuedCommandPayload<TCommand>>,
    opts: &CqrsWorkQueueRecipeOptions<TCommand>,
) {
    match record {
        WorkQueueRecord::WorkAdmitted {
            work_id, payload, ..
        } => {
            if payload.kind == "cqrs-queued-command" {
                state.payloads.insert(work_id.clone(), payload.clone());
            } else {
                ctx.emit(CqrsQueueFact::<TCommand>::Issue(queue_issue(
                    record,
                    "cqrs-queue-malformed-payload",
                )));
            }
        }
        WorkQueueRecord::WorkClaimed {
            work_id,
            lease_id,
            attempt,
            worker_id,
            record_seq,
            ..
        } => {
            if opts
                .worker_id
                .as_deref()
                .is_some_and(|expected| expected != worker_id)
            {
                return;
            }
            let Some(payload) = state.payloads.get(work_id).cloned() else {
                ctx.emit(CqrsQueueFact::<TCommand>::Issue(queue_issue(
                    record,
                    "cqrs-claim-without-payload",
                )));
                ctx.emit(CqrsQueueFact::<TCommand>::Command(
                    WorkQueueCommand::Release {
                        command_id: format!(
                            "cqrs:{work_id}:{lease_id}:{attempt}:release-no-payload"
                        ),
                        queue_id: None,
                        idempotency_key: None,
                        work_id: work_id.clone(),
                        lease_id: lease_id.clone(),
                        attempt: *attempt,
                        worker_id: worker_id.clone(),
                        reason: Some("cqrs-claim-without-payload".to_owned()),
                        now_ms: None,
                    },
                ));
                return;
            };
            let attempt_fact = CqrsWorkQueueAttempt {
                kind: "cqrs-work-queue-attempt".to_owned(),
                work_id: work_id.clone(),
                lease_id: lease_id.clone(),
                queue_attempt: *attempt,
                worker_id: worker_id.clone(),
                command: payload.command.clone(),
                payload,
                source_refs: vec![format!("work-queue-record:{record_seq}")],
            };
            state
                .active_claims
                .entry(attempt_fact.command.id.clone())
                .or_default()
                .push(attempt_fact.clone());
            ctx.emit(CqrsQueueFact::<TCommand>::Attempt(attempt_fact.clone()));
            ctx.emit(CqrsQueueFact::<TCommand>::Dispatch(attempt_fact.command));
        }
        _ => invalidate_claims_for_record(state, record),
    }
}

fn invalidate_claims_for_record<TCommand: Clone>(
    state: &mut CqrsQueueState<TCommand>,
    record: &WorkQueueRecord<CqrsQueuedCommandPayload<TCommand>>,
) {
    match record {
        WorkQueueRecord::WorkReleased {
            work_id,
            lease_id,
            attempt,
            worker_id,
            ..
        }
        | WorkQueueRecord::LeaseExpired {
            work_id,
            lease_id,
            attempt,
            worker_id,
            ..
        }
        | WorkQueueRecord::AttemptFailed {
            work_id,
            lease_id,
            attempt,
            worker_id,
            ..
        }
        | WorkQueueRecord::AttemptCompleted {
            work_id,
            lease_id,
            attempt,
            worker_id,
            ..
        }
        | WorkQueueRecord::WorkCompleted {
            work_id,
            lease_id,
            attempt,
            worker_id,
            ..
        } => retain_active_claims(state, |claim| {
            claim.work_id != *work_id
                || claim.lease_id != *lease_id
                || claim.queue_attempt != *attempt
                || claim.worker_id != *worker_id
        }),
        WorkQueueRecord::WorkDeadLettered { work_id, .. } => {
            retain_active_claims(state, |claim| claim.work_id != *work_id);
        }
        WorkQueueRecord::WorkCanceled {
            work_id,
            canceled_lease_id,
            attempt,
            ..
        } => retain_active_claims(state, |claim| {
            if claim.work_id != *work_id {
                return true;
            }
            if let Some(canceled_lease_id) = canceled_lease_id {
                if claim.lease_id != *canceled_lease_id {
                    return true;
                }
            }
            if let Some(attempt) = attempt {
                if claim.queue_attempt != *attempt {
                    return true;
                }
            }
            false
        }),
        WorkQueueRecord::RetryScheduled { .. }
        | WorkQueueRecord::WorkAdmitted { .. }
        | WorkQueueRecord::AdmissionDeduped { .. }
        | WorkQueueRecord::WorkScheduled { .. }
        | WorkQueueRecord::WorkClaimed { .. }
        | WorkQueueRecord::LeaseRenewed { .. } => {}
    }
}

fn retain_active_claims<TCommand: Clone>(
    state: &mut CqrsQueueState<TCommand>,
    keep: impl Fn(&CqrsWorkQueueAttempt<TCommand>) -> bool,
) {
    state.active_claims.retain(|_, claims| {
        claims.retain(|claim| keep(claim));
        !claims.is_empty()
    });
}

fn reduce_status<TCommand: Clone + 'static>(
    ctx: &Ctx,
    state: &mut CqrsQueueState<TCommand>,
    status: &CqrsStatus,
    opts: &CqrsWorkQueueRecipeOptions<TCommand>,
) {
    let Some(command_id) = &status.command_id else {
        return;
    };
    let Some(attempt) = shift_active_claim(&mut state.active_claims, command_id) else {
        ctx.emit(CqrsQueueFact::<TCommand>::Issue(DataIssue {
            kind: "issue".to_owned(),
            code: "cqrs-status-without-active-queue-claim".to_owned(),
            message: "CQRS workQueue recipe observed status without an active queue claim"
                .to_owned(),
            severity: "error".to_owned(),
            source: "cqrs.workQueue".to_owned(),
            topic: None,
            details: Some(format!("cqrs-command:{command_id}")),
        }));
        return;
    };
    let claim_key = format!(
        "{}:{}:{}",
        attempt.work_id, attempt.lease_id, attempt.queue_attempt
    );
    if !state.terminal_claims.insert(claim_key.clone()) {
        ctx.emit(CqrsQueueFact::<TCommand>::Issue(DataIssue {
            kind: "issue".to_owned(),
            code: "cqrs-duplicate-terminal-outcome-for-queue-claim".to_owned(),
            message: format!(
                "CQRS queue claim '{claim_key}' already produced a terminal disposition"
            ),
            severity: "error".to_owned(),
            source: "cqrs.workQueue".to_owned(),
            topic: None,
            details: Some(format!("cqrs-command:{command_id}")),
        }));
        return;
    }
    let outcome = if status.state == CqrsStatusState::Accepted {
        CqrsWorkQueueOutcome::Accepted {
            status: status.clone(),
        }
    } else {
        CqrsWorkQueueOutcome::Rejected {
            status: status.clone(),
            error: state.errors.get(command_id).cloned().map(Box::new),
        }
    };
    ctx.emit(CqrsQueueFact::<TCommand>::Command(
        cqrs_work_queue_disposition_command(&attempt, outcome, &opts.policy),
    ));
}

fn shift_active_claim<TCommand>(
    claims: &mut BTreeMap<String, Vec<CqrsWorkQueueAttempt<TCommand>>>,
    command_id: &str,
) -> Option<CqrsWorkQueueAttempt<TCommand>> {
    let queue = claims.get_mut(command_id)?;
    if queue.is_empty() {
        return None;
    }
    let first = queue.remove(0);
    if queue.is_empty() {
        claims.remove(command_id);
    }
    Some(first)
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

fn disposition_command_id<TCommand>(
    attempt: &CqrsWorkQueueAttempt<TCommand>,
    suffix: &str,
) -> String {
    format!(
        "cqrs:{}:{}:{}:{suffix}",
        attempt.work_id, attempt.lease_id, attempt.queue_attempt
    )
}

fn error_message<TCommand>(
    code: Option<CqrsErrorCode>,
    error: Option<&CqrsError<TCommand>>,
) -> String {
    error
        .map(|error| error.message.clone())
        .unwrap_or_else(|| format!("cqrs rejected with {code:?}"))
}

fn queue_issue<TCommand>(record: &WorkQueueRecord<TCommand>, code: impl Into<String>) -> DataIssue {
    DataIssue {
        kind: "issue".to_owned(),
        code: code.into(),
        message: format!(
            "CQRS workQueue recipe could not map workQueue record '{}'",
            record_kind(record)
        ),
        severity: "error".to_owned(),
        source: "cqrs.workQueue".to_owned(),
        topic: None,
        details: Some(format!(
            "work_id={};record_seq={}",
            record.work_id(),
            record.record_seq()
        )),
    }
}

fn record_kind<TCommand>(record: &WorkQueueRecord<TCommand>) -> &'static str {
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
