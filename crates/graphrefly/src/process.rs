//! Graph-visible process orchestration bundle (D136 / B84).
//!
//! A `ProcessBundle` is facts plus a reducer: command DATA facts enter a
//! graph-owned runtime node, and state/event/effect-request/status/error/audit/
//! cursor projections are ordinary graph nodes with declared deps. It is not a
//! workflow engine, effect runner, storage restore path, or hidden process manager.

use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::rc::Rc;

use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

use crate::ctx::Ctx;
use crate::graph::{Graph, GraphNodeOpts};
use crate::node::Node;
use crate::operators::Operator;

#[derive(Debug, Clone, PartialEq)]
pub struct ProcessCommand<T = crate::protocol::AnyValue> {
    pub id: String,
    pub command_type: String,
    pub payload: T,
    pub process_id: Option<String>,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
}

impl<T> ProcessCommand<T> {
    pub fn new(id: impl Into<String>, command_type: impl Into<String>, payload: T) -> Self {
        Self {
            id: id.into(),
            command_type: command_type.into(),
            payload,
            process_id: None,
            correlation_id: None,
            causation_id: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProcessEventDraft<T = crate::protocol::AnyValue> {
    pub id: Option<String>,
    pub event_type: String,
    pub payload: T,
    pub process_id: Option<String>,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
}

impl<T> ProcessEventDraft<T> {
    pub fn new(event_type: impl Into<String>, payload: T) -> Self {
        Self {
            id: None,
            event_type: event_type.into(),
            payload,
            process_id: None,
            correlation_id: None,
            causation_id: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProcessEvent<T = crate::protocol::AnyValue> {
    pub id: String,
    pub event_type: String,
    pub seq: u64,
    pub cursor: u64,
    pub command_id: String,
    pub command_type: String,
    pub payload: T,
    pub timestamp_ms: u64,
    pub process_id: Option<String>,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProcessEffectRequestDraft<T = crate::protocol::AnyValue> {
    pub id: Option<String>,
    pub effect_type: String,
    pub payload: T,
    pub process_id: Option<String>,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
}

impl<T> ProcessEffectRequestDraft<T> {
    pub fn new(effect_type: impl Into<String>, payload: T) -> Self {
        Self {
            id: None,
            effect_type: effect_type.into(),
            payload,
            process_id: None,
            correlation_id: None,
            causation_id: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProcessEffectRequest<T = crate::protocol::AnyValue> {
    pub id: String,
    pub effect_type: String,
    pub seq: u64,
    pub cursor: u64,
    pub command_id: String,
    pub command_type: String,
    pub payload: T,
    pub timestamp_ms: u64,
    pub process_id: Option<String>,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessErrorCode {
    MalformedCommand,
    ReducerThrew,
    ClockThrew,
    MalformedState,
    MalformedEvent,
    MalformedEffect,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProcessError<TCommand = crate::protocol::AnyValue> {
    pub code: ProcessErrorCode,
    pub message: String,
    pub command: Option<ProcessCommand<TCommand>>,
    pub cursor: ProcessCursor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessCursor {
    pub event_seq: u64,
    pub effect_seq: u64,
    pub command_count: u64,
    pub error_count: u64,
    pub audit_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessStatus {
    pub state: ProcessStatusState,
    pub command_id: Option<String>,
    pub command_type: Option<String>,
    pub event_count: usize,
    pub effect_count: usize,
    pub error_code: Option<ProcessErrorCode>,
    pub cursor: ProcessCursor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessStatusState {
    Accepted,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessAuditRecord {
    pub seq: u64,
    pub command_id: Option<String>,
    pub command_type: Option<String>,
    pub outcome: ProcessAuditOutcome,
    pub event_ids: Vec<String>,
    pub event_types: Vec<String>,
    pub effect_ids: Vec<String>,
    pub effect_types: Vec<String>,
    pub error_code: Option<ProcessErrorCode>,
    pub error_message: Option<String>,
    pub cursor: ProcessCursor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessAuditOutcome {
    Success,
    Failure,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProcessRuntimeFact<
    TState = crate::protocol::AnyValue,
    TEvent = crate::protocol::AnyValue,
    TEffect = crate::protocol::AnyValue,
    TCommand = crate::protocol::AnyValue,
> {
    State {
        state: TState,
        cursor: ProcessCursor,
    },
    Event(ProcessEvent<TEvent>),
    EffectRequest(ProcessEffectRequest<TEffect>),
    Status(ProcessStatus),
    Error(ProcessError<TCommand>),
    Audit(ProcessAuditRecord),
    Cursor(ProcessCursor),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProcessReduction<TState, TEvent, TEffect> {
    pub state: TState,
    pub events: Vec<ProcessEventDraft<TEvent>>,
    pub effects: Vec<ProcessEffectRequestDraft<TEffect>>,
}

impl<TState, TEvent, TEffect> ProcessReduction<TState, TEvent, TEffect> {
    pub fn new(state: TState) -> Self {
        Self {
            state,
            events: Vec::new(),
            effects: Vec::new(),
        }
    }

    pub fn with_events(mut self, events: Vec<ProcessEventDraft<TEvent>>) -> Self {
        self.events = events;
        self
    }

    pub fn with_effects(mut self, effects: Vec<ProcessEffectRequestDraft<TEffect>>) -> Self {
        self.effects = effects;
        self
    }
}

pub type ProcessReducerFn<TCommand, TState, TEvent, TEffect> =
    dyn Fn(&ProcessCommand<TCommand>, TState) -> ProcessReduction<TState, TEvent, TEffect>;

pub type ProcessReducer<TCommand, TState, TEvent, TEffect> =
    Rc<ProcessReducerFn<TCommand, TState, TEvent, TEffect>>;

#[derive(Clone)]
pub struct ProcessBundleOptions<TCommand, TState, TEvent, TEffect> {
    pub name: String,
    pub initial_state: TState,
    pub reduce: ProcessReducer<TCommand, TState, TEvent, TEffect>,
    pub now: Rc<dyn Fn() -> u64>,
}

impl<TCommand, TState, TEvent, TEffect> ProcessBundleOptions<TCommand, TState, TEvent, TEffect> {
    pub fn new(
        initial_state: TState,
        reduce: impl Fn(&ProcessCommand<TCommand>, TState) -> ProcessReduction<TState, TEvent, TEffect>
            + 'static,
    ) -> Self {
        Self {
            name: "process".to_owned(),
            initial_state,
            reduce: Rc::new(reduce),
            now: Rc::new(|| 0),
        }
    }

    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    pub fn with_now(mut self, now: impl Fn() -> u64 + 'static) -> Self {
        self.now = Rc::new(now);
        self
    }
}

#[derive(Clone)]
pub struct ProcessBundle<
    TCommand = crate::protocol::AnyValue,
    TState = crate::protocol::AnyValue,
    TEvent = crate::protocol::AnyValue,
    TEffect = crate::protocol::AnyValue,
> {
    pub command: Node<ProcessCommand<TCommand>>,
    pub state: Node<TState>,
    pub events: Node<ProcessEvent<TEvent>>,
    pub audit: Node<ProcessAuditRecord>,
    pub effect_request: Node<ProcessEffectRequest<TEffect>>,
    pub status: Node<ProcessStatus>,
    pub error: Node<ProcessError<TCommand>>,
    pub cursor: Node<ProcessCursor>,
    _retains: Rc<Vec<ProcessRetain>>,
}

impl<TCommand: Clone + 'static, TState, TEvent, TEffect>
    ProcessBundle<TCommand, TState, TEvent, TEffect>
{
    pub fn dispatch(&self, command: ProcessCommand<TCommand>) -> ProcessCommand<TCommand> {
        self.command.set(command.clone());
        command
    }
}

struct ProcessRetain {
    release: std::cell::RefCell<Option<Box<dyn FnOnce()>>>,
}

impl ProcessRetain {
    fn new(release: Box<dyn FnOnce()>) -> Self {
        Self {
            release: std::cell::RefCell::new(Some(release)),
        }
    }
}

impl Drop for ProcessRetain {
    fn drop(&mut self) {
        if let Some(release) = self.release.borrow_mut().take() {
            release();
        }
    }
}

pub fn process_bundle<
    TCommand: Clone + 'static,
    TState: Clone + Serialize + DeserializeOwned + 'static,
    TEvent: Clone + 'static,
    TEffect: Clone + 'static,
>(
    graph: &Graph,
    opts: ProcessBundleOptions<TCommand, TState, TEvent, TEffect>,
) -> ProcessBundle<TCommand, TState, TEvent, TEffect> {
    assert!(
        !opts.name.is_empty(),
        "process_bundle: name must be non-empty"
    );
    let name = opts.name;
    let reduce = opts.reduce;
    let now = opts.now;
    let initial_state_json = state_to_json(&opts.initial_state)
        .unwrap_or_else(|message| panic!("process_bundle: {message}"));

    let command = graph.init_node::<ProcessCommand<TCommand>>(
        Operator::with_opts("processCommand", no_terminal_opts(), |_| {}),
        Vec::new(),
        meta_opts(format!("{name}/command"), "command"),
    );

    let runtime = graph.init_node::<ProcessRuntimeFact<TState, TEvent, TEffect, TCommand>>(
        Operator::with_opts("processRuntime", no_terminal_opts(), {
            move |ctx: &Ctx| {
                ctx.state_persist(true);
                let mut state = match ctx.state_get::<Value>() {
                    Some(value) => match RuntimeState::from_json(value.as_ref()) {
                        Ok(state) => state,
                        Err(message) => {
                            let mut recovery =
                                RuntimeState::cursor_recovery(initial_state_json.clone());
                            for command in ctx.batch::<ProcessCommand<TCommand>>(0) {
                                for fact in failure::<TCommand, TState, TEvent, TEffect>(
                                    &mut recovery,
                                    Some((*command).clone()),
                                    ProcessErrorCode::MalformedState,
                                    message.clone(),
                                ) {
                                    ctx.emit(fact);
                                }
                            }
                            return;
                        }
                    },
                    None => RuntimeState::new(initial_state_json.clone()),
                };
                for command in ctx.batch::<ProcessCommand<TCommand>>(0) {
                    for fact in reduce_process_command_fact(
                        &mut state,
                        (*command).clone(),
                        reduce.as_ref(),
                        now.as_ref(),
                    ) {
                        ctx.emit(fact);
                    }
                    ctx.state_set(state.to_json());
                }
            }
        }),
        vec![command.erased()],
        meta_opts(format!("{name}/runtime"), "runtime"),
    );

    let state = runtime_projection::<TCommand, TState, TEvent, TEffect, TState>(
        graph,
        &runtime,
        &format!("{name}/state"),
        "processState",
        |fact| match fact {
            ProcessRuntimeFact::State { state, .. } => Some(state.clone()),
            _ => None,
        },
    );
    let events = runtime_projection::<TCommand, TState, TEvent, TEffect, ProcessEvent<TEvent>>(
        graph,
        &runtime,
        &format!("{name}/events"),
        "processEvents",
        |fact| match fact {
            ProcessRuntimeFact::Event(event) => Some(event.clone()),
            _ => None,
        },
    );
    let audit = runtime_projection::<TCommand, TState, TEvent, TEffect, ProcessAuditRecord>(
        graph,
        &runtime,
        &format!("{name}/audit"),
        "processAudit",
        |fact| match fact {
            ProcessRuntimeFact::Audit(audit) => Some(audit.clone()),
            _ => None,
        },
    );
    let effect_request =
        runtime_projection::<TCommand, TState, TEvent, TEffect, ProcessEffectRequest<TEffect>>(
            graph,
            &runtime,
            &format!("{name}/effect_request"),
            "processEffectRequest",
            |fact| match fact {
                ProcessRuntimeFact::EffectRequest(effect) => Some(effect.clone()),
                _ => None,
            },
        );
    let status = runtime_projection::<TCommand, TState, TEvent, TEffect, ProcessStatus>(
        graph,
        &runtime,
        &format!("{name}/status"),
        "processStatus",
        |fact| match fact {
            ProcessRuntimeFact::Status(status) => Some(status.clone()),
            _ => None,
        },
    );
    let error = runtime_projection::<TCommand, TState, TEvent, TEffect, ProcessError<TCommand>>(
        graph,
        &runtime,
        &format!("{name}/error"),
        "processError",
        |fact| match fact {
            ProcessRuntimeFact::Error(error) => Some(error.clone()),
            _ => None,
        },
    );
    let cursor = runtime_projection::<TCommand, TState, TEvent, TEffect, ProcessCursor>(
        graph,
        &runtime,
        &format!("{name}/cursor"),
        "processCursor",
        |fact| match fact {
            ProcessRuntimeFact::Cursor(cursor) => Some(cursor.clone()),
            _ => None,
        },
    );

    let retains = Rc::new(vec![
        ProcessRetain::new(graph.retain(&runtime, &format!("{name}.process.runtime"))),
        ProcessRetain::new(graph.retain(&state, &format!("{name}.process.state"))),
        ProcessRetain::new(graph.retain(&events, &format!("{name}.process.events"))),
        ProcessRetain::new(graph.retain(&audit, &format!("{name}.process.audit"))),
        ProcessRetain::new(
            graph.retain(&effect_request, &format!("{name}.process.effect_request")),
        ),
        ProcessRetain::new(graph.retain(&status, &format!("{name}.process.status"))),
        ProcessRetain::new(graph.retain(&error, &format!("{name}.process.error"))),
        ProcessRetain::new(graph.retain(&cursor, &format!("{name}.process.cursor"))),
    ]);

    ProcessBundle {
        command,
        state,
        events,
        audit,
        effect_request,
        status,
        error,
        cursor,
        _retains: retains,
    }
}

#[derive(Debug)]
struct RuntimeState {
    event_seq: u64,
    effect_seq: u64,
    command_count: u64,
    error_count: u64,
    audit_seq: u64,
    seen_event_ids: Vec<String>,
    seen_effect_ids: Vec<String>,
    state: Value,
}

impl RuntimeState {
    fn new(state: Value) -> Self {
        Self {
            event_seq: 0,
            effect_seq: 0,
            command_count: 0,
            error_count: 0,
            audit_seq: 0,
            seen_event_ids: Vec::new(),
            seen_effect_ids: Vec::new(),
            state,
        }
    }

    fn from_json(value: &Value) -> Result<Self, String> {
        Ok(Self {
            event_seq: json_required_u64(value, "eventSeq")?,
            effect_seq: json_required_u64(value, "effectSeq")?,
            command_count: json_required_u64(value, "commandCount")?,
            error_count: json_required_u64(value, "errorCount")?,
            audit_seq: json_required_u64(value, "auditSeq")?,
            seen_event_ids: json_required_string_array(value, "seenEventIds")?,
            seen_effect_ids: json_required_string_array(value, "seenEffectIds")?,
            state: value
                .get("state")
                .cloned()
                .ok_or_else(|| "process_bundle: runtime state missing 'state'".to_owned())?,
        })
    }

    fn cursor_recovery(state: Value) -> Self {
        Self::new(state)
    }

    fn to_json(&self) -> Value {
        serde_json::json!({
            "eventSeq": self.event_seq,
            "effectSeq": self.effect_seq,
            "commandCount": self.command_count,
            "errorCount": self.error_count,
            "auditSeq": self.audit_seq,
            "seenEventIds": self.seen_event_ids,
            "seenEffectIds": self.seen_effect_ids,
            "state": self.state,
        })
    }
}

fn reduce_process_command_fact<
    TCommand: Clone + 'static,
    TState: Clone + Serialize + DeserializeOwned + 'static,
    TEvent: Clone + 'static,
    TEffect: Clone + 'static,
>(
    state: &mut RuntimeState,
    command: ProcessCommand<TCommand>,
    reduce: &ProcessReducerFn<TCommand, TState, TEvent, TEffect>,
    now: &dyn Fn() -> u64,
) -> Vec<ProcessRuntimeFact<TState, TEvent, TEffect, TCommand>> {
    state.command_count += 1;
    if command.id.is_empty() {
        return failure(
            state,
            Some(command),
            ProcessErrorCode::MalformedCommand,
            "process_bundle: command id must be non-empty".to_owned(),
        );
    }
    if command.command_type.is_empty() {
        return failure(
            state,
            Some(command),
            ProcessErrorCode::MalformedCommand,
            "process_bundle: command type must be non-empty".to_owned(),
        );
    }
    let reducer_state = match state_from_json::<TState>(&state.state) {
        Ok(state) => state,
        Err(message) => {
            return failure(
                state,
                Some(command),
                ProcessErrorCode::MalformedState,
                message,
            );
        }
    };
    let reduction = match catch_unwind(AssertUnwindSafe(|| reduce(&command, reducer_state))) {
        Ok(reduction) => reduction,
        Err(panic) => {
            if is_graph_runtime_panic(&panic) {
                resume_unwind(panic);
            }
            return failure(
                state,
                Some(command),
                ProcessErrorCode::ReducerThrew,
                panic_message(&panic),
            );
        }
    };
    let next_state_json = match state_to_json(&reduction.state) {
        Ok(state) => state,
        Err(message) => {
            return failure(
                state,
                Some(command),
                ProcessErrorCode::MalformedState,
                message,
            );
        }
    };
    let visible_state = match state_from_json::<TState>(&next_state_json) {
        Ok(state) => state,
        Err(message) => {
            return failure(
                state,
                Some(command),
                ProcessErrorCode::MalformedState,
                message,
            );
        }
    };
    let prepared_events = match prepare_events(&command, &reduction.events, state) {
        Ok(events) => events,
        Err(message) => {
            return failure(
                state,
                Some(command),
                ProcessErrorCode::MalformedEvent,
                message,
            );
        }
    };
    let prepared_effects = match prepare_effects(&command, &reduction.effects, state) {
        Ok(effects) => effects,
        Err(message) => {
            return failure(
                state,
                Some(command),
                ProcessErrorCode::MalformedEffect,
                message,
            );
        }
    };
    let timestamp_ms = match process_timestamp(now) {
        Ok(timestamp_ms) => timestamp_ms,
        Err(message) => {
            return failure(state, Some(command), ProcessErrorCode::ClockThrew, message);
        }
    };

    state.state = next_state_json;
    let mut facts = vec![ProcessRuntimeFact::State {
        state: visible_state,
        cursor: cursor_of(state),
    }];

    let mut events = Vec::new();
    for draft in prepared_events {
        state.event_seq += 1;
        state.seen_event_ids.push(draft.id.clone());
        let event = ProcessEvent {
            id: draft.id,
            event_type: draft.event_type,
            seq: state.event_seq,
            cursor: state.event_seq,
            command_id: command.id.clone(),
            command_type: command.command_type.clone(),
            payload: draft.payload,
            timestamp_ms,
            process_id: draft.process_id,
            correlation_id: draft.correlation_id,
            causation_id: draft.causation_id,
        };
        facts.push(ProcessRuntimeFact::Event(event.clone()));
        events.push(event);
    }

    let mut effects = Vec::new();
    for draft in prepared_effects {
        state.effect_seq += 1;
        state.seen_effect_ids.push(draft.id.clone());
        let effect = ProcessEffectRequest {
            id: draft.id,
            effect_type: draft.effect_type,
            seq: state.effect_seq,
            cursor: state.effect_seq,
            command_id: command.id.clone(),
            command_type: command.command_type.clone(),
            payload: draft.payload,
            timestamp_ms,
            process_id: draft.process_id,
            correlation_id: draft.correlation_id,
            causation_id: draft.causation_id,
        };
        facts.push(ProcessRuntimeFact::EffectRequest(effect.clone()));
        effects.push(effect);
    }

    facts.push(ProcessRuntimeFact::Status(ProcessStatus {
        state: ProcessStatusState::Accepted,
        command_id: Some(command.id.clone()),
        command_type: Some(command.command_type.clone()),
        event_count: events.len(),
        effect_count: effects.len(),
        error_code: None,
        cursor: cursor_of(state),
    }));
    facts.push(ProcessRuntimeFact::Audit(audit_record::<
        TCommand,
        TEvent,
        TEffect,
    >(
        state,
        Some(&command),
        ProcessAuditOutcome::Success,
        &events,
        &effects,
        None,
        None,
    )));
    facts.push(ProcessRuntimeFact::Cursor(cursor_of(state)));
    facts
}

#[derive(Clone)]
struct PreparedEvent<T> {
    id: String,
    event_type: String,
    payload: T,
    process_id: Option<String>,
    correlation_id: Option<String>,
    causation_id: Option<String>,
}

#[derive(Clone)]
struct PreparedEffect<T> {
    id: String,
    effect_type: String,
    payload: T,
    process_id: Option<String>,
    correlation_id: Option<String>,
    causation_id: Option<String>,
}

fn prepare_events<TCommand, TEvent: Clone>(
    command: &ProcessCommand<TCommand>,
    drafts: &[ProcessEventDraft<TEvent>],
    state: &RuntimeState,
) -> Result<Vec<PreparedEvent<TEvent>>, String> {
    let mut seen = Vec::<String>::new();
    let mut prepared = Vec::new();
    for (index, draft) in drafts.iter().enumerate() {
        if draft.event_type.is_empty() {
            return Err("process_bundle: event draft must have a non-empty type".to_owned());
        }
        let id = draft
            .id
            .as_ref()
            .filter(|id| !id.is_empty())
            .cloned()
            .unwrap_or_else(|| {
                format!(
                    "{}:event:{}",
                    command.id,
                    state.event_seq + index as u64 + 1
                )
            });
        if seen.contains(&id) || state.seen_event_ids.contains(&id) {
            return Err(format!("process_bundle: duplicate event '{id}'"));
        }
        seen.push(id.clone());
        prepared.push(PreparedEvent {
            id,
            event_type: draft.event_type.clone(),
            payload: draft.payload.clone(),
            process_id: draft.process_id.clone(),
            correlation_id: draft.correlation_id.clone(),
            causation_id: draft.causation_id.clone(),
        });
    }
    Ok(prepared)
}

fn prepare_effects<TCommand, TEffect: Clone>(
    command: &ProcessCommand<TCommand>,
    drafts: &[ProcessEffectRequestDraft<TEffect>],
    state: &RuntimeState,
) -> Result<Vec<PreparedEffect<TEffect>>, String> {
    let mut seen = Vec::<String>::new();
    let mut prepared = Vec::new();
    for (index, draft) in drafts.iter().enumerate() {
        if draft.effect_type.is_empty() {
            return Err("process_bundle: effect draft must have a non-empty type".to_owned());
        }
        let id = draft
            .id
            .as_ref()
            .filter(|id| !id.is_empty())
            .cloned()
            .unwrap_or_else(|| {
                format!(
                    "{}:effect:{}",
                    command.id,
                    state.effect_seq + index as u64 + 1
                )
            });
        if seen.contains(&id) || state.seen_effect_ids.contains(&id) {
            return Err(format!("process_bundle: duplicate effect '{id}'"));
        }
        seen.push(id.clone());
        prepared.push(PreparedEffect {
            id,
            effect_type: draft.effect_type.clone(),
            payload: draft.payload.clone(),
            process_id: draft.process_id.clone(),
            correlation_id: draft.correlation_id.clone(),
            causation_id: draft.causation_id.clone(),
        });
    }
    Ok(prepared)
}

fn failure<
    TCommand: Clone + 'static,
    TState: Clone + 'static,
    TEvent: Clone + 'static,
    TEffect: Clone + 'static,
>(
    state: &mut RuntimeState,
    command: Option<ProcessCommand<TCommand>>,
    code: ProcessErrorCode,
    message: String,
) -> Vec<ProcessRuntimeFact<TState, TEvent, TEffect, TCommand>> {
    state.error_count += 1;
    let cursor = cursor_of(state);
    vec![
        ProcessRuntimeFact::Error(ProcessError {
            code,
            message: message.clone(),
            command: command.clone(),
            cursor: cursor.clone(),
        }),
        ProcessRuntimeFact::Status(ProcessStatus {
            state: ProcessStatusState::Rejected,
            command_id: command.as_ref().map(|command| command.id.clone()),
            command_type: command.as_ref().map(|command| command.command_type.clone()),
            event_count: 0,
            effect_count: 0,
            error_code: Some(code),
            cursor: cursor.clone(),
        }),
        ProcessRuntimeFact::Audit(audit_record::<TCommand, TEvent, TEffect>(
            state,
            command.as_ref(),
            ProcessAuditOutcome::Failure,
            &[],
            &[],
            Some(code),
            Some(message),
        )),
        ProcessRuntimeFact::Cursor(cursor_of(state)),
    ]
}

fn audit_record<TCommand, TEvent, TEffect>(
    state: &mut RuntimeState,
    command: Option<&ProcessCommand<TCommand>>,
    outcome: ProcessAuditOutcome,
    events: &[ProcessEvent<TEvent>],
    effects: &[ProcessEffectRequest<TEffect>],
    error_code: Option<ProcessErrorCode>,
    error_message: Option<String>,
) -> ProcessAuditRecord {
    state.audit_seq += 1;
    ProcessAuditRecord {
        seq: state.audit_seq,
        command_id: command.map(|command| command.id.clone()),
        command_type: command.map(|command| command.command_type.clone()),
        outcome,
        event_ids: events.iter().map(|event| event.id.clone()).collect(),
        event_types: events
            .iter()
            .map(|event| event.event_type.clone())
            .collect(),
        effect_ids: effects.iter().map(|effect| effect.id.clone()).collect(),
        effect_types: effects
            .iter()
            .map(|effect| effect.effect_type.clone())
            .collect(),
        error_code,
        error_message,
        cursor: cursor_of(state),
    }
}

fn runtime_projection<
    TCommand: Clone + 'static,
    TState: Clone + 'static,
    TEvent: Clone + 'static,
    TEffect: Clone + 'static,
    TOut: Clone + 'static,
>(
    graph: &Graph,
    runtime: &Node<ProcessRuntimeFact<TState, TEvent, TEffect, TCommand>>,
    name: &str,
    factory: &'static str,
    select: impl Fn(&ProcessRuntimeFact<TState, TEvent, TEffect, TCommand>) -> Option<TOut> + 'static,
) -> Node<TOut> {
    graph.init_node::<TOut>(
        Operator::with_opts(factory, no_terminal_opts(), move |ctx: &Ctx| {
            for fact in ctx.batch::<ProcessRuntimeFact<TState, TEvent, TEffect, TCommand>>(0) {
                if let Some(selected) = select(fact.as_ref()) {
                    ctx.emit(selected);
                }
            }
        }),
        vec![runtime.erased()],
        GraphNodeOpts::named(name),
    )
}

fn no_terminal_opts() -> crate::node::NodeOpts {
    crate::node::NodeOpts {
        complete_when_deps_complete: false,
        error_when_deps_error: false,
        ..Default::default()
    }
}

fn meta_opts(name: String, role: &'static str) -> GraphNodeOpts {
    let mut opts = GraphNodeOpts::named(name);
    opts.meta.insert("process".to_owned(), role.to_owned());
    opts.meta.insert("d".to_owned(), "D136".to_owned());
    opts
}

fn cursor_of(state: &RuntimeState) -> ProcessCursor {
    ProcessCursor {
        event_seq: state.event_seq,
        effect_seq: state.effect_seq,
        command_count: state.command_count,
        error_count: state.error_count,
        audit_seq: state.audit_seq,
    }
}

fn state_to_json<TState: Serialize>(state: &TState) -> Result<Value, String> {
    serde_json::to_value(state).map_err(|error| format!("state must serialize to JSON ({error})"))
}

fn state_from_json<TState: DeserializeOwned>(state: &Value) -> Result<TState, String> {
    serde_json::from_value(state.clone())
        .map_err(|error| format!("state must deserialize from JSON ({error})"))
}

fn json_required_u64(value: &Value, key: &str) -> Result<u64, String> {
    value.get(key).and_then(Value::as_u64).ok_or_else(|| {
        format!("process_bundle: runtime state field '{key}' must be an unsigned integer")
    })
}

fn json_required_string_array(value: &Value, key: &str) -> Result<Vec<String>, String> {
    let Some(items) = value.get(key).and_then(Value::as_array) else {
        return Err(format!(
            "process_bundle: runtime state field '{key}' must be an array of strings"
        ));
    };
    let mut out = Vec::new();
    for item in items {
        let Some(item) = item.as_str() else {
            return Err(format!(
                "process_bundle: runtime state field '{key}' must be an array of strings"
            ));
        };
        out.push(item.to_owned());
    }
    Ok(out)
}

fn process_timestamp(now: &dyn Fn() -> u64) -> Result<u64, String> {
    catch_unwind(AssertUnwindSafe(now))
        .map_err(|panic| format!("process_bundle: now() threw: {}", panic_message(&panic)))
}

fn panic_message(panic: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        return (*message).to_owned();
    }
    if let Some(message) = panic.downcast_ref::<String>() {
        return message.clone();
    }
    "panic".to_owned()
}

fn is_graph_runtime_panic(panic: &Box<dyn std::any::Any + Send>) -> bool {
    let message = panic_message(panic);
    message.contains("R-reentrancy")
        || message.contains("R-rewire")
        || message.contains("R-graph-domain")
        || message.contains("D22")
        || message.contains("D37")
        || message.contains("feedback cycle")
        || message.contains("different graph")
        || message.contains("cross-graph")
        || message.contains("wire bridge")
        || message.contains("mid-fn topology mutation")
        || message.contains("reentrant dep mutation")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn runtime_state_rejects_corrupt_checkpoint_fields() {
        let valid = json!({
            "eventSeq": 1,
            "effectSeq": 2,
            "commandCount": 3,
            "errorCount": 0,
            "auditSeq": 3,
            "seenEventIds": ["event-1"],
            "seenEffectIds": ["effect-1"],
            "state": { "total": 4 },
        });
        assert!(RuntimeState::from_json(&valid).is_ok());

        let bad_counter = json!({
            "eventSeq": "1",
            "effectSeq": 2,
            "commandCount": 3,
            "errorCount": 0,
            "auditSeq": 3,
            "seenEventIds": ["event-1"],
            "seenEffectIds": ["effect-1"],
            "state": { "total": 4 },
        });
        let err = RuntimeState::from_json(&bad_counter).expect_err("bad counter fails");
        assert!(err.contains("eventSeq"));

        let bad_seen_ids = json!({
            "eventSeq": 1,
            "effectSeq": 2,
            "commandCount": 3,
            "errorCount": 0,
            "auditSeq": 3,
            "seenEventIds": ["event-1", 7],
            "seenEffectIds": ["effect-1"],
            "state": { "total": 4 },
        });
        let err = RuntimeState::from_json(&bad_seen_ids).expect_err("bad id array fails");
        assert!(err.contains("seenEventIds"));

        let missing_state = json!({
            "eventSeq": 1,
            "effectSeq": 2,
            "commandCount": 3,
            "errorCount": 0,
            "auditSeq": 3,
            "seenEventIds": ["event-1"],
            "seenEffectIds": ["effect-1"],
        });
        let err = RuntimeState::from_json(&missing_state).expect_err("missing state fails");
        assert!(err.contains("missing 'state'"));
    }
}
