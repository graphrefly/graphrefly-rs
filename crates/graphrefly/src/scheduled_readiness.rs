//! Graph-visible scheduled readiness projector (D424/D432/D433).
//!
//! The projector is intentionally passive: explicit schedule facts plus explicit
//! clock facts produce eligibility/deadline visibility. It never runs timers,
//! claims work, executes providers, or mutates domain lifecycle records.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use serde_json::{Map, Value};

use crate::ctx::Ctx;
use crate::graph::{Graph, GraphNodeOpts};
use crate::identity::{canonical_tuple_key, compound_tuple_key};
use crate::json::{stable_json_string, JsonValue};
use crate::messaging::DataIssue;
use crate::node::{Node, NodeOpts};

#[derive(Debug, Clone, PartialEq)]
pub struct SourceRef {
    pub kind: String,
    pub id: String,
    pub metadata: Option<BTreeMap<String, JsonValue>>,
}

impl SourceRef {
    pub fn new(kind: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            id: id.into(),
            metadata: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScheduledReadinessRequested {
    pub schedule_id: String,
    pub subject_refs: Vec<SourceRef>,
    pub ready_at_ms: u64,
    pub deadline_ms: Option<u64>,
    pub reason: Option<String>,
    pub policy_refs: Vec<SourceRef>,
    pub source_refs: Vec<SourceRef>,
    pub metadata: Option<BTreeMap<String, JsonValue>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScheduledReadinessClock {
    pub clock_id: String,
    pub now_ms: u64,
    pub source_refs: Vec<SourceRef>,
    pub metadata: Option<BTreeMap<String, JsonValue>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScheduledReadinessPending {
    pub schedule_id: String,
    pub subject_refs: Vec<SourceRef>,
    pub ready_at_ms: u64,
    pub deadline_ms: Option<u64>,
    pub now_ms: Option<u64>,
    pub source_refs: Vec<SourceRef>,
    pub metadata: Option<BTreeMap<String, JsonValue>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScheduledReadinessReady {
    pub schedule_id: String,
    pub subject_refs: Vec<SourceRef>,
    pub ready_at_ms: u64,
    pub deadline_ms: Option<u64>,
    pub now_ms: u64,
    pub source_refs: Vec<SourceRef>,
    pub metadata: Option<BTreeMap<String, JsonValue>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScheduledReadinessOverdue {
    pub schedule_id: String,
    pub subject_refs: Vec<SourceRef>,
    pub ready_at_ms: u64,
    pub deadline_ms: u64,
    pub now_ms: u64,
    pub source_refs: Vec<SourceRef>,
    pub metadata: Option<BTreeMap<String, JsonValue>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduledReadinessStatusState {
    Pending,
    Ready,
    Overdue,
    Issue,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScheduledReadinessStatus {
    pub status_id: String,
    pub schedule_id: String,
    pub state: ScheduledReadinessStatusState,
    pub subject_refs: Vec<SourceRef>,
    pub ready_at_ms: Option<u64>,
    pub deadline_ms: Option<u64>,
    pub now_ms: Option<u64>,
    pub source_refs: Vec<SourceRef>,
    pub issue_codes: Vec<String>,
    pub metadata: Option<BTreeMap<String, JsonValue>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScheduledReadinessAuditRecord {
    pub id: String,
    pub kind: String,
    pub subject_id: Option<String>,
    pub source_refs: Vec<SourceRef>,
    pub metadata: Option<BTreeMap<String, JsonValue>>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ScheduledReadinessViews {
    pub schedules_by_id: BTreeMap<String, ScheduledReadinessRequested>,
    pub pending_by_id: BTreeMap<String, ScheduledReadinessPending>,
    pub ready_by_id: BTreeMap<String, ScheduledReadinessReady>,
    pub overdue_by_id: BTreeMap<String, ScheduledReadinessOverdue>,
    pub status_by_id: BTreeMap<String, ScheduledReadinessStatus>,
    pub now_ms: Option<u64>,
}

#[derive(Clone)]
pub struct ScheduledReadinessBundle {
    pub pending: Node<ScheduledReadinessPending>,
    pub ready: Node<ScheduledReadinessReady>,
    pub overdue: Node<ScheduledReadinessOverdue>,
    pub status: Node<ScheduledReadinessStatus>,
    pub issues: Node<DataIssue>,
    pub audit: Node<ScheduledReadinessAuditRecord>,
    pub views: Node<ScheduledReadinessViews>,
}

#[derive(Clone)]
pub struct ScheduledReadinessOptions {
    pub name: Option<String>,
    pub schedules: Vec<Node<ScheduledReadinessRequested>>,
    pub clocks: Vec<Node<ScheduledReadinessClock>>,
}

impl ScheduledReadinessOptions {
    pub fn new(schedules: Vec<Node<ScheduledReadinessRequested>>) -> Self {
        Self {
            name: None,
            schedules,
            clocks: Vec::new(),
        }
    }

    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn with_clocks(mut self, clocks: Vec<Node<ScheduledReadinessClock>>) -> Self {
        self.clocks = clocks;
        self
    }
}

#[derive(Clone)]
enum ScheduledReadinessFact {
    Pending(ScheduledReadinessPending),
    Ready(ScheduledReadinessReady),
    Overdue(ScheduledReadinessOverdue),
    Status(ScheduledReadinessStatus),
    Issue(DataIssue),
    Audit(ScheduledReadinessAuditRecord),
    Views(ScheduledReadinessViews),
}

#[derive(Default)]
struct ScheduledReadinessState {
    schedules: BTreeMap<String, ScheduledReadinessRequested>,
    pending_by_id: BTreeMap<String, ScheduledReadinessPending>,
    ready_by_id: BTreeMap<String, ScheduledReadinessReady>,
    overdue_by_id: BTreeMap<String, ScheduledReadinessOverdue>,
    status_by_id: BTreeMap<String, ScheduledReadinessStatus>,
    emitted_keys: BTreeSet<String>,
    issue_keys: BTreeSet<String>,
    audit_seq: u64,
    now_ms: Option<u64>,
    clock_source_refs: Vec<SourceRef>,
}

pub fn scheduled_readiness_projector(
    graph: &Graph,
    opts: ScheduledReadinessOptions,
) -> ScheduledReadinessBundle {
    let name = opts
        .name
        .clone()
        .unwrap_or_else(|| "scheduledReadiness".to_owned());
    let schedule_count = opts.schedules.len();
    let mut deps = Vec::with_capacity(opts.schedules.len() + opts.clocks.len());
    deps.extend(opts.schedules.iter().map(Node::erased));
    deps.extend(opts.clocks.iter().map(Node::erased));
    let state = Rc::new(RefCell::new(ScheduledReadinessState::default()));
    let runtime = graph.node_opts::<ScheduledReadinessFact, _>(
        deps,
        {
            let state = state.clone();
            move |ctx| {
                let mut state = state.borrow_mut();
                for index in 0..schedule_count {
                    for schedule in ctx.batch::<ScheduledReadinessRequested>(index) {
                        retain_schedule(ctx, &mut state, (*schedule).clone());
                    }
                }
                for index in schedule_count..(schedule_count + opts.clocks.len()) {
                    for clock in ctx.batch::<ScheduledReadinessClock>(index) {
                        retain_clock(ctx, &mut state, (*clock).clone());
                    }
                }
                evaluate_schedules(ctx, &mut state);
                emit_fact(
                    ctx,
                    ScheduledReadinessFact::Views(ScheduledReadinessViews {
                        schedules_by_id: state.schedules.clone(),
                        pending_by_id: state.pending_by_id.clone(),
                        ready_by_id: state.ready_by_id.clone(),
                        overdue_by_id: state.overdue_by_id.clone(),
                        status_by_id: state.status_by_id.clone(),
                        now_ms: state.now_ms,
                    }),
                );
            }
        },
        {
            let mut node_opts = node_opts(format!("{name}/runtime"), "scheduledReadinessProjector");
            node_opts.node.partial = true;
            node_opts
        },
    );
    ScheduledReadinessBundle {
        pending: project_fact(
            graph,
            &runtime,
            format!("{name}/pending"),
            "scheduledReadinessPending",
            |fact| match fact {
                ScheduledReadinessFact::Pending(value) => Some(value.clone()),
                _ => None,
            },
        ),
        ready: project_fact(
            graph,
            &runtime,
            format!("{name}/ready"),
            "scheduledReadinessReady",
            |fact| match fact {
                ScheduledReadinessFact::Ready(value) => Some(value.clone()),
                _ => None,
            },
        ),
        overdue: project_fact(
            graph,
            &runtime,
            format!("{name}/overdue"),
            "scheduledReadinessOverdue",
            |fact| match fact {
                ScheduledReadinessFact::Overdue(value) => Some(value.clone()),
                _ => None,
            },
        ),
        status: project_fact(
            graph,
            &runtime,
            format!("{name}/status"),
            "scheduledReadinessStatus",
            |fact| match fact {
                ScheduledReadinessFact::Status(value) => Some(value.clone()),
                _ => None,
            },
        ),
        issues: project_fact(
            graph,
            &runtime,
            format!("{name}/issues"),
            "scheduledReadinessIssues",
            |fact| match fact {
                ScheduledReadinessFact::Issue(value) => Some(value.clone()),
                _ => None,
            },
        ),
        audit: project_fact(
            graph,
            &runtime,
            format!("{name}/audit"),
            "scheduledReadinessAudit",
            |fact| match fact {
                ScheduledReadinessFact::Audit(value) => Some(value.clone()),
                _ => None,
            },
        ),
        views: project_fact(
            graph,
            &runtime,
            format!("{name}/views"),
            "scheduledReadinessViews",
            |fact| match fact {
                ScheduledReadinessFact::Views(value) => Some(value.clone()),
                _ => None,
            },
        ),
    }
}

pub fn parse_scheduled_readiness_requested(
    value: &JsonValue,
) -> Result<ScheduledReadinessRequested, Box<DataIssue>> {
    let Some(object) = value.as_object() else {
        return Err(Box::new(readiness_issue(
            "scheduled-readiness-malformed-schedule",
            "Scheduled readiness requires scheduleId, subjectRefs, and readyAtMs.",
            "unknown-scheduled-readiness",
            None,
            "error",
        )));
    };
    let schedule_id = string_field(object, "scheduleId")
        .filter(|value| !value.is_empty())
        .unwrap_or("unknown-scheduled-readiness");
    if object.contains_key("subjectRef") || object.contains_key("notBeforeMs") {
        return Err(Box::new(readiness_issue(
            "scheduled-readiness-malformed-schedule",
            "Scheduled readiness v1 rejects stale subjectRef/notBeforeMs aliases.",
            schedule_id,
            None,
            "error",
        )));
    }
    let ready_at_ms = u64_field(object, "readyAtMs");
    let deadline_ms = optional_u64_field(object, "deadlineMs");
    let subject_refs = source_refs_field(object, "subjectRefs");
    if object.get("kind").and_then(Value::as_str) != Some("scheduled-readiness-requested")
        || string_field(object, "scheduleId").is_none_or(str::is_empty)
        || object
            .get("subjectRefs")
            .is_none_or(|value| !value.is_array())
        || ready_at_ms.is_none()
        || deadline_ms.is_err()
    {
        return Err(Box::new(readiness_issue(
            "scheduled-readiness-malformed-schedule",
            "Scheduled readiness requires scheduleId, subjectRefs, and finite readyAtMs.",
            schedule_id,
            None,
            "error",
        )));
    }
    Ok(ScheduledReadinessRequested {
        schedule_id: schedule_id.to_owned(),
        subject_refs,
        ready_at_ms: ready_at_ms.expect("checked readyAtMs"),
        deadline_ms: deadline_ms.expect("checked deadlineMs"),
        reason: string_field(object, "reason").map(str::to_owned),
        policy_refs: source_refs_field(object, "policyRefs"),
        source_refs: source_refs_field(object, "sourceRefs"),
        metadata: object_metadata(object.get("metadata")),
    })
}

fn retain_schedule(
    ctx: &Ctx,
    state: &mut ScheduledReadinessState,
    schedule: ScheduledReadinessRequested,
) {
    let schedule = sanitize_schedule(schedule);
    let schedule_id = schedule.schedule_id.clone();
    match state.schedules.get(&schedule_id) {
        None => {
            state.schedules.insert(schedule_id, schedule);
        }
        Some(existing) if schedule_identity(existing) == schedule_identity(&schedule) => {}
        Some(existing) => {
            let existing = existing.clone();
            let issue = readiness_issue(
                "scheduled-readiness-schedule-conflict",
                "Scheduled readiness scheduleId was replayed with conflicting material; first valid schedule retained.",
                &schedule_id,
                Some(format!(
                    "existingReadyAtMs={}; incomingReadyAtMs={}",
                    existing.ready_at_ms, schedule.ready_at_ms
                )),
                "error",
            );
            emit_issue(ctx, state, issue.clone());
            emit_status(
                ctx,
                state,
                ScheduledReadinessStatus {
                    status_id: format!("{schedule_id}:scheduled-readiness-status:issue"),
                    schedule_id,
                    state: ScheduledReadinessStatusState::Issue,
                    subject_refs: existing.subject_refs.clone(),
                    ready_at_ms: Some(existing.ready_at_ms),
                    deadline_ms: existing.deadline_ms,
                    now_ms: state.now_ms,
                    source_refs: schedule_source_refs(&existing, &state.clock_source_refs),
                    issue_codes: vec![issue.code],
                    metadata: None,
                },
            );
        }
    }
}

fn retain_clock(ctx: &Ctx, state: &mut ScheduledReadinessState, clock: ScheduledReadinessClock) {
    if let Some(previous) = state.now_ms {
        if clock.now_ms < previous {
            emit_issue(
                ctx,
                state,
                readiness_issue(
                    "scheduled-readiness-clock-rollback",
                    "Scheduled readiness clock facts must be monotonic; rollback was ignored.",
                    &clock.clock_id,
                    Some(format!("nowMs={}; previousNowMs={previous}", clock.now_ms)),
                    "warning",
                ),
            );
            return;
        }
    }
    state.now_ms = Some(clock.now_ms);
    state.clock_source_refs = canonical_source_refs(clock.source_refs);
}

fn evaluate_schedules(ctx: &Ctx, state: &mut ScheduledReadinessState) {
    for schedule in state.schedules.values().cloned().collect::<Vec<_>>() {
        let source_refs = schedule_source_refs(&schedule, &state.clock_source_refs);
        let metadata = readiness_metadata(&schedule);
        if state
            .now_ms
            .is_none_or(|now_ms| now_ms < schedule.ready_at_ms)
        {
            let pending = ScheduledReadinessPending {
                schedule_id: schedule.schedule_id.clone(),
                subject_refs: canonical_source_refs(schedule.subject_refs.clone()),
                ready_at_ms: schedule.ready_at_ms,
                deadline_ms: schedule.deadline_ms,
                now_ms: state.now_ms,
                source_refs: source_refs.clone(),
                metadata: metadata.clone(),
            };
            emit_pending(ctx, state, pending);
            emit_status(
                ctx,
                state,
                status_for(
                    &schedule,
                    ScheduledReadinessStatusState::Pending,
                    source_refs,
                    state.now_ms,
                ),
            );
            continue;
        }
        let now_ms = state.now_ms.expect("ready branch has clock");
        let ready = ScheduledReadinessReady {
            schedule_id: schedule.schedule_id.clone(),
            subject_refs: canonical_source_refs(schedule.subject_refs.clone()),
            ready_at_ms: schedule.ready_at_ms,
            deadline_ms: schedule.deadline_ms,
            now_ms,
            source_refs: source_refs.clone(),
            metadata: metadata.clone(),
        };
        emit_ready(ctx, state, ready);
        emit_status(
            ctx,
            state,
            status_for(
                &schedule,
                ScheduledReadinessStatusState::Ready,
                source_refs.clone(),
                Some(now_ms),
            ),
        );
        if schedule
            .deadline_ms
            .is_some_and(|deadline| now_ms > deadline)
        {
            let overdue = ScheduledReadinessOverdue {
                schedule_id: schedule.schedule_id.clone(),
                subject_refs: canonical_source_refs(schedule.subject_refs.clone()),
                ready_at_ms: schedule.ready_at_ms,
                deadline_ms: schedule.deadline_ms.expect("checked deadline"),
                now_ms,
                source_refs,
                metadata,
            };
            emit_overdue(ctx, state, overdue);
            emit_status(
                ctx,
                state,
                status_for(
                    &schedule,
                    ScheduledReadinessStatusState::Overdue,
                    schedule_source_refs(&schedule, &state.clock_source_refs),
                    Some(now_ms),
                ),
            );
        }
    }
}

fn emit_pending(
    ctx: &Ctx,
    state: &mut ScheduledReadinessState,
    pending: ScheduledReadinessPending,
) {
    state
        .pending_by_id
        .insert(pending.schedule_id.clone(), pending.clone());
    let key = format!("pending:{}", pending.schedule_id);
    if state.emitted_keys.insert(key) {
        emit_fact(ctx, ScheduledReadinessFact::Pending(pending));
    }
}

fn emit_ready(ctx: &Ctx, state: &mut ScheduledReadinessState, ready: ScheduledReadinessReady) {
    let key = format!("ready:{}", ready.schedule_id);
    state
        .ready_by_id
        .insert(ready.schedule_id.clone(), ready.clone());
    state.pending_by_id.remove(&ready.schedule_id);
    if state.emitted_keys.insert(key) {
        emit_audit(
            ctx,
            state,
            "scheduled-readiness-ready",
            Some(ready.schedule_id.clone()),
            ready.source_refs.clone(),
            Some(BTreeMap::from([
                ("nowMs".to_owned(), JsonValue::from(ready.now_ms)),
                ("readyAtMs".to_owned(), JsonValue::from(ready.ready_at_ms)),
            ])),
        );
        emit_fact(ctx, ScheduledReadinessFact::Ready(ready));
    }
}

fn emit_overdue(
    ctx: &Ctx,
    state: &mut ScheduledReadinessState,
    overdue: ScheduledReadinessOverdue,
) {
    let key = format!("overdue:{}", overdue.schedule_id);
    state
        .overdue_by_id
        .insert(overdue.schedule_id.clone(), overdue.clone());
    if state.emitted_keys.insert(key) {
        emit_audit(
            ctx,
            state,
            "scheduled-readiness-overdue",
            Some(overdue.schedule_id.clone()),
            overdue.source_refs.clone(),
            Some(BTreeMap::from([
                ("nowMs".to_owned(), JsonValue::from(overdue.now_ms)),
                ("readyAtMs".to_owned(), JsonValue::from(overdue.ready_at_ms)),
                (
                    "deadlineMs".to_owned(),
                    JsonValue::from(overdue.deadline_ms),
                ),
            ])),
        );
        emit_fact(ctx, ScheduledReadinessFact::Overdue(overdue));
    }
}

fn emit_status(ctx: &Ctx, state: &mut ScheduledReadinessState, status: ScheduledReadinessStatus) {
    state
        .status_by_id
        .insert(status.schedule_id.clone(), status.clone());
    let key = compound_tuple_key("status", &[&status_identity(&status)]);
    if state.emitted_keys.insert(key) {
        emit_fact(ctx, ScheduledReadinessFact::Status(status));
    }
}

fn emit_issue(ctx: &Ctx, state: &mut ScheduledReadinessState, issue: DataIssue) {
    let key = canonical_tuple_key(&[
        &issue.source,
        &issue.code,
        issue.details.as_deref().unwrap_or(""),
    ]);
    if state.issue_keys.insert(key) {
        emit_fact(ctx, ScheduledReadinessFact::Issue(issue));
    }
}

fn emit_audit(
    ctx: &Ctx,
    state: &mut ScheduledReadinessState,
    kind: impl Into<String>,
    subject_id: Option<String>,
    source_refs: Vec<SourceRef>,
    metadata: Option<BTreeMap<String, JsonValue>>,
) {
    state.audit_seq += 1;
    emit_fact(
        ctx,
        ScheduledReadinessFact::Audit(ScheduledReadinessAuditRecord {
            id: compound_tuple_key("scheduled-readiness-audit", &[&state.audit_seq.to_string()]),
            kind: kind.into(),
            subject_id,
            source_refs: canonical_source_refs(source_refs),
            metadata: sanitize_metadata(metadata),
        }),
    );
}

fn status_for(
    schedule: &ScheduledReadinessRequested,
    state: ScheduledReadinessStatusState,
    source_refs: Vec<SourceRef>,
    now_ms: Option<u64>,
) -> ScheduledReadinessStatus {
    let status_name = match state {
        ScheduledReadinessStatusState::Pending => "pending",
        ScheduledReadinessStatusState::Ready => "ready",
        ScheduledReadinessStatusState::Overdue => "overdue",
        ScheduledReadinessStatusState::Issue => "issue",
    };
    ScheduledReadinessStatus {
        status_id: compound_tuple_key(
            "scheduled-readiness-status",
            &[&schedule.schedule_id, status_name],
        ),
        schedule_id: schedule.schedule_id.clone(),
        state,
        subject_refs: canonical_source_refs(schedule.subject_refs.clone()),
        ready_at_ms: Some(schedule.ready_at_ms),
        deadline_ms: schedule.deadline_ms,
        now_ms,
        source_refs,
        issue_codes: Vec::new(),
        metadata: schedule
            .reason
            .as_ref()
            .map(|reason| BTreeMap::from([("reason".to_owned(), JsonValue::from(reason.clone()))])),
    }
}

fn sanitize_schedule(mut schedule: ScheduledReadinessRequested) -> ScheduledReadinessRequested {
    schedule.subject_refs = canonical_source_refs(schedule.subject_refs);
    schedule.policy_refs = canonical_source_refs(schedule.policy_refs);
    schedule.source_refs = canonical_source_refs(schedule.source_refs);
    schedule.metadata = sanitize_metadata(schedule.metadata);
    schedule
}

fn readiness_metadata(
    schedule: &ScheduledReadinessRequested,
) -> Option<BTreeMap<String, JsonValue>> {
    let mut metadata = schedule.metadata.clone().unwrap_or_default();
    if let Some(reason) = &schedule.reason {
        metadata.insert("reason".to_owned(), JsonValue::from(reason.clone()));
    }
    sanitize_metadata((!metadata.is_empty()).then_some(metadata))
}

fn schedule_source_refs(
    schedule: &ScheduledReadinessRequested,
    clock_source_refs: &[SourceRef],
) -> Vec<SourceRef> {
    let mut refs = vec![SourceRef::new(
        "scheduled-readiness",
        schedule.schedule_id.clone(),
    )];
    refs.extend(schedule.source_refs.clone());
    refs.extend(schedule.policy_refs.clone());
    refs.extend(clock_source_refs.iter().cloned());
    canonical_source_refs(refs)
}

fn schedule_identity(schedule: &ScheduledReadinessRequested) -> String {
    let value = serde_json::json!({
        "scheduleId": schedule.schedule_id,
        "subjectRefs": source_refs_json(&schedule.subject_refs),
        "readyAtMs": schedule.ready_at_ms,
        "deadlineMs": schedule.deadline_ms,
        "reason": schedule.reason,
        "policyRefs": source_refs_json(&schedule.policy_refs),
        "sourceRefs": source_refs_json(&schedule.source_refs),
        "metadata": schedule.metadata,
    });
    stable_json_string(&value).unwrap_or_else(|_| format!("{schedule:?}"))
}

fn status_identity(status: &ScheduledReadinessStatus) -> String {
    let value = serde_json::json!({
        "statusId": status.status_id,
        "scheduleId": status.schedule_id,
        "state": format!("{:?}", status.state),
        "readyAtMs": status.ready_at_ms,
        "deadlineMs": status.deadline_ms,
        "nowMs": status.now_ms,
        "issueCodes": status.issue_codes,
    });
    stable_json_string(&value).unwrap_or_else(|_| format!("{status:?}"))
}

pub fn readiness_issue(
    code: impl Into<String>,
    message: impl Into<String>,
    subject_id: &str,
    details: Option<String>,
    severity: impl Into<String>,
) -> DataIssue {
    DataIssue {
        kind: "data-issue".to_owned(),
        code: code.into(),
        message: message.into(),
        severity: severity.into(),
        source: format!("scheduled-readiness:{subject_id}"),
        topic: None,
        details,
    }
}

fn canonical_source_refs(refs: Vec<SourceRef>) -> Vec<SourceRef> {
    let mut refs = refs
        .into_iter()
        .filter_map(|mut source_ref| {
            if source_ref.kind.is_empty() || source_ref.id.is_empty() {
                return None;
            }
            source_ref.metadata = sanitize_metadata(source_ref.metadata);
            Some(source_ref)
        })
        .collect::<Vec<_>>();
    refs.sort_by_key(source_ref_sort_key);
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for source_ref in refs {
        let key = canonical_tuple_key(&[&source_ref.kind, &source_ref.id]);
        if seen.insert(key) {
            out.push(source_ref);
        }
    }
    out
}

fn source_ref_sort_key(source_ref: &SourceRef) -> (String, String, String) {
    let metadata = source_ref
        .metadata
        .as_ref()
        .map(|metadata| {
            stable_json_string(&JsonValue::Object(
                metadata
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
            ))
        })
        .transpose()
        .ok()
        .flatten()
        .unwrap_or_default();
    (source_ref.kind.clone(), source_ref.id.clone(), metadata)
}

fn sanitize_metadata(
    metadata: Option<BTreeMap<String, JsonValue>>,
) -> Option<BTreeMap<String, JsonValue>> {
    let mut out = BTreeMap::new();
    for (key, value) in metadata.unwrap_or_default() {
        if is_runtime_metadata_key(&key) {
            continue;
        }
        out.insert(key, value);
    }
    (!out.is_empty()).then_some(out)
}

fn is_runtime_metadata_key(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "apikey"
            | "api_key"
            | "secret"
            | "client"
            | "transport"
            | "subprocess"
            | "sdk"
            | "oauth"
            | "credential"
            | "credentials"
            | "accesstoken"
            | "access_token"
            | "refreshtoken"
            | "refresh_token"
            | "token"
            | "password"
            | "authorization"
            | "cookie"
            | "stdout"
            | "stderr"
            | "stack"
            | "stacktrace"
            | "providerraw"
            | "provider_raw"
            | "rawresponse"
            | "raw_response"
            | "diff"
            | "patch"
            | "filecontents"
            | "file_contents"
            | "binary"
            | "media"
    )
}

fn source_refs_json(refs: &[SourceRef]) -> JsonValue {
    JsonValue::Array(
        refs.iter()
            .map(|source_ref| {
                let mut object = Map::new();
                object.insert("kind".to_owned(), JsonValue::from(source_ref.kind.clone()));
                object.insert("id".to_owned(), JsonValue::from(source_ref.id.clone()));
                if let Some(metadata) = &source_ref.metadata {
                    object.insert(
                        "metadata".to_owned(),
                        JsonValue::Object(
                            metadata
                                .iter()
                                .map(|(key, value)| (key.clone(), value.clone()))
                                .collect(),
                        ),
                    );
                }
                JsonValue::Object(object)
            })
            .collect(),
    )
}

fn string_field<'a>(object: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    object.get(key).and_then(Value::as_str)
}

fn u64_field(object: &Map<String, Value>, key: &str) -> Option<u64> {
    object.get(key).and_then(Value::as_u64)
}

fn optional_u64_field(object: &Map<String, Value>, key: &str) -> Result<Option<u64>, ()> {
    match object.get(key) {
        None => Ok(None),
        Some(value) => value.as_u64().map(Some).ok_or(()),
    }
}

fn source_refs_field(object: &Map<String, Value>, key: &str) -> Vec<SourceRef> {
    let Some(values) = object.get(key).and_then(Value::as_array) else {
        return Vec::new();
    };
    canonical_source_refs(
        values
            .iter()
            .filter_map(|value| {
                let object = value.as_object()?;
                Some(SourceRef {
                    kind: string_field(object, "kind")?.to_owned(),
                    id: string_field(object, "id")?.to_owned(),
                    metadata: object_metadata(object.get("metadata")),
                })
            })
            .collect(),
    )
}

fn object_metadata(value: Option<&Value>) -> Option<BTreeMap<String, JsonValue>> {
    let object = value?.as_object()?;
    sanitize_metadata(Some(
        object
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    ))
}

fn project_fact<T: Clone + 'static>(
    graph: &Graph,
    runtime: &Node<ScheduledReadinessFact>,
    name: String,
    factory: &'static str,
    select: impl Fn(&ScheduledReadinessFact) -> Option<T> + 'static,
) -> Node<T> {
    graph.node_opts::<T, _>(
        vec![runtime.erased()],
        move |ctx| {
            for fact in ctx.batch::<ScheduledReadinessFact>(0) {
                if let Some(value) = select(&fact) {
                    ctx.emit(value);
                }
            }
        },
        node_opts(name, factory),
    )
}

fn emit_fact(ctx: &Ctx, fact: ScheduledReadinessFact) {
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
