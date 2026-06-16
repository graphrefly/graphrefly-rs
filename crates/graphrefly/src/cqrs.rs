//! Graph-visible CQRS application infrastructure (D142 / B67).
//!
//! Commands are ordinary DATA facts on a graph-owned command node. Runtime facts,
//! events, status, errors, audit, cursor, and projections are ordinary graph
//! nodes with declared deps; there is no Graph subclass, hidden EventEmitter, or
//! storage-owned restore path.

pub mod messaging;
pub mod work_queue;

use std::collections::{HashMap, HashSet};
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::rc::Rc;

use serde_json::{json, Value};

use crate::ctx::Ctx;
use crate::graph::{Graph, GraphNodeOpts};
use crate::node::Node;
use crate::operators::Operator;

#[derive(Debug, Clone, PartialEq)]
pub struct CqrsCommand<T = crate::protocol::AnyValue> {
    pub id: String,
    pub command_type: String,
    pub payload: T,
    pub aggregate_id: Option<String>,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
}

impl<T> CqrsCommand<T> {
    pub fn new(id: impl Into<String>, command_type: impl Into<String>, payload: T) -> Self {
        Self {
            id: id.into(),
            command_type: command_type.into(),
            payload,
            aggregate_id: None,
            correlation_id: None,
            causation_id: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CqrsEventDraft<T = crate::protocol::AnyValue> {
    pub id: Option<String>,
    pub event_type: String,
    pub payload: T,
    pub aggregate_id: Option<String>,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
}

impl<T> CqrsEventDraft<T> {
    pub fn new(event_type: impl Into<String>, payload: T) -> Self {
        Self {
            id: None,
            event_type: event_type.into(),
            payload,
            aggregate_id: None,
            correlation_id: None,
            causation_id: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CqrsEvent<T = crate::protocol::AnyValue> {
    pub id: String,
    pub event_type: String,
    pub seq: u64,
    pub cursor: u64,
    pub runtime_cursor: CqrsCursor,
    pub command_id: String,
    pub command_type: String,
    pub payload: T,
    pub timestamp_ms: u64,
    pub aggregate_id: Option<String>,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CqrsErrorCode {
    MalformedCommand,
    DuplicateCommand,
    UnknownCommand,
    HandlerThrew,
    ClockThrew,
    MalformedEvent,
    UnknownEvent,
    DuplicateEvent,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CqrsError<TCommand = crate::protocol::AnyValue> {
    pub code: CqrsErrorCode,
    pub message: String,
    pub command: Option<CqrsCommand<TCommand>>,
    pub cursor: CqrsCursor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CqrsDedupeSnapshot {
    pub command_ids_retained: usize,
    pub event_ids_retained: usize,
    pub command_ids_evicted: u64,
    pub event_ids_evicted: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CqrsCursor {
    pub event_seq: u64,
    pub command_count: u64,
    pub error_count: u64,
    pub audit_seq: u64,
    pub dedupe: Option<CqrsDedupeSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CqrsStatus {
    pub state: CqrsStatusState,
    pub command_id: Option<String>,
    pub command_type: Option<String>,
    pub event_count: usize,
    pub error_code: Option<CqrsErrorCode>,
    pub cursor: CqrsCursor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CqrsStatusState {
    Accepted,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CqrsAuditRecord {
    pub seq: u64,
    pub command_id: Option<String>,
    pub command_type: Option<String>,
    pub outcome: CqrsAuditOutcome,
    pub event_ids: Vec<String>,
    pub event_types: Vec<String>,
    pub error_code: Option<CqrsErrorCode>,
    pub error_message: Option<String>,
    pub cursor: CqrsCursor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CqrsAuditOutcome {
    Success,
    Failure,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CqrsRuntimeFact<TCommand = crate::protocol::AnyValue, TEvent = crate::protocol::AnyValue> {
    Event(CqrsEvent<TEvent>),
    Status(CqrsStatus),
    Error(CqrsError<TCommand>),
    Audit(CqrsAuditRecord),
    Cursor(CqrsCursor),
}

/// D151 membership-based duplicate-recognition window for CQRS ids.
///
/// This is passive CQRS vocabulary, not a shared idempotency reducer engine:
/// commands and events each own their own id membership window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CqrsDedupeWindow {
    Unbounded,
    Bounded { max_entries: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CqrsDedupePolicy {
    pub commands: CqrsDedupeWindow,
    pub events: CqrsDedupeWindow,
}

impl Default for CqrsDedupePolicy {
    fn default() -> Self {
        Self {
            commands: CqrsDedupeWindow::Unbounded,
            events: CqrsDedupeWindow::Unbounded,
        }
    }
}

impl CqrsDedupePolicy {
    pub fn bounded(command_max_entries: usize, event_max_entries: usize) -> Self {
        Self {
            commands: CqrsDedupeWindow::Bounded {
                max_entries: command_max_entries,
            },
            events: CqrsDedupeWindow::Bounded {
                max_entries: event_max_entries,
            },
        }
    }

    fn bounded_any(self) -> bool {
        matches!(self.commands, CqrsDedupeWindow::Bounded { .. })
            || matches!(self.events, CqrsDedupeWindow::Bounded { .. })
    }
}

pub type CqrsCommandHandler<TCommand, TEvent> =
    Rc<dyn Fn(&CqrsCommand<TCommand>) -> Vec<CqrsEventDraft<TEvent>>>;

#[derive(Clone)]
pub struct CqrsCommandHandlerDefinition<TCommand, TEvent> {
    pub command_type: String,
    pub handle: CqrsCommandHandler<TCommand, TEvent>,
}

pub fn cqrs_command_handler<TCommand, TEvent>(
    command_type: impl Into<String>,
    handle: impl Fn(&CqrsCommand<TCommand>) -> Vec<CqrsEventDraft<TEvent>> + 'static,
) -> CqrsCommandHandlerDefinition<TCommand, TEvent> {
    let command_type = command_type.into();
    assert!(
        !command_type.is_empty(),
        "cqrs_command_handler: command type must be non-empty"
    );
    CqrsCommandHandlerDefinition {
        command_type,
        handle: Rc::new(handle),
    }
}

#[derive(Clone)]
pub struct CqrsOptions<TCommand, TEvent> {
    pub name: String,
    pub handlers: Vec<CqrsCommandHandlerDefinition<TCommand, TEvent>>,
    pub events: Option<Vec<String>>,
    pub now: Rc<dyn Fn() -> u64>,
    pub dedupe: CqrsDedupePolicy,
}

impl<TCommand, TEvent> Default for CqrsOptions<TCommand, TEvent> {
    fn default() -> Self {
        Self {
            name: "cqrs".to_owned(),
            handlers: Vec::new(),
            events: None,
            now: Rc::new(|| 0),
            dedupe: CqrsDedupePolicy::default(),
        }
    }
}

impl<TCommand, TEvent> CqrsOptions<TCommand, TEvent> {
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Self::default()
        }
    }

    pub fn with_handlers(
        mut self,
        handlers: Vec<CqrsCommandHandlerDefinition<TCommand, TEvent>>,
    ) -> Self {
        self.handlers = handlers;
        self
    }

    pub fn with_events(mut self, events: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.events = Some(events.into_iter().map(Into::into).collect());
        self
    }

    pub fn with_now(mut self, now: impl Fn() -> u64 + 'static) -> Self {
        self.now = Rc::new(now);
        self
    }

    pub fn with_dedupe(mut self, dedupe: CqrsDedupePolicy) -> Self {
        self.dedupe = dedupe;
        self
    }
}

#[derive(Clone)]
pub struct CqrsBundle<TCommand = crate::protocol::AnyValue, TEvent = crate::protocol::AnyValue> {
    pub command: Node<CqrsCommand<TCommand>>,
    pub runtime: Node<CqrsRuntimeFact<TCommand, TEvent>>,
    pub events: Node<CqrsEvent<TEvent>>,
    pub status: Node<CqrsStatus>,
    pub errors: Node<CqrsError<TCommand>>,
    pub audit: Node<CqrsAuditRecord>,
    pub cursor: Node<CqrsCursor>,
    _retains: Rc<Vec<CqrsRetain>>,
}

impl<TCommand: Clone + 'static, TEvent: Clone + 'static> CqrsBundle<TCommand, TEvent> {
    pub fn dispatch(&self, command: CqrsCommand<TCommand>) -> CqrsCommand<TCommand> {
        self.command.set(command.clone());
        command
    }
}

struct CqrsRetain {
    release: std::cell::RefCell<Option<Box<dyn FnOnce()>>>,
}

impl CqrsRetain {
    fn new(release: Box<dyn FnOnce()>) -> Self {
        Self {
            release: std::cell::RefCell::new(Some(release)),
        }
    }
}

impl Drop for CqrsRetain {
    fn drop(&mut self) {
        if let Some(release) = self.release.borrow_mut().take() {
            release();
        }
    }
}

pub fn cqrs<TCommand: Clone + 'static, TEvent: Clone + 'static>(
    graph: &Graph,
) -> CqrsBundle<TCommand, TEvent> {
    cqrs_with_options(graph, CqrsOptions::default())
}

pub fn cqrs_with_options<TCommand: Clone + 'static, TEvent: Clone + 'static>(
    graph: &Graph,
    opts: CqrsOptions<TCommand, TEvent>,
) -> CqrsBundle<TCommand, TEvent> {
    let name = opts.name;
    let handlers = Rc::new(normalize_handlers(opts.handlers));
    let known_events = Rc::new(normalize_events(opts.events));
    let now = opts.now;
    let dedupe = opts.dedupe;

    let command = graph.init_node::<CqrsCommand<TCommand>>(
        Operator::with_opts("cqrsCommand", no_terminal_opts(), |_| {}),
        Vec::new(),
        GraphNodeOpts::named(format!("{name}/command")),
    );

    let runtime_seed = RuntimeState::default().to_json();
    let runtime = graph.init_node::<CqrsRuntimeFact<TCommand, TEvent>>(
        Operator::with_opts("cqrsRuntime", no_terminal_opts(), {
            let handlers = handlers.clone();
            let known_events = known_events.clone();
            let now = now.clone();
            move |ctx: &Ctx| {
                let mut state = ctx
                    .state_get::<Value>()
                    .map(|value| RuntimeState::from_json(value.as_ref()))
                    .unwrap_or_else(|| RuntimeState::from_json(&runtime_seed));
                ctx.state_persist(true);
                for command in ctx.batch::<CqrsCommand<TCommand>>(0) {
                    for fact in reduce_command_fact(
                        &mut state,
                        (*command).clone(),
                        handlers.as_ref(),
                        known_events.as_ref(),
                        now.as_ref(),
                        dedupe,
                    ) {
                        ctx.emit(fact);
                    }
                }
                ctx.state_set(state.to_json());
            }
        }),
        vec![command.erased()],
        {
            let mut node_opts = GraphNodeOpts::named(format!("{name}/runtime"));
            node_opts
                .meta
                .insert("dedupe".to_owned(), dedupe_meta(dedupe));
            node_opts
        },
    );

    let events = runtime_projection::<TCommand, TEvent, CqrsEvent<TEvent>>(
        graph,
        &runtime,
        &format!("{name}/events"),
        "cqrsEvents",
        |fact| match fact {
            CqrsRuntimeFact::Event(event) => Some(event.clone()),
            _ => None,
        },
    );
    let status = runtime_projection::<TCommand, TEvent, CqrsStatus>(
        graph,
        &runtime,
        &format!("{name}/status"),
        "cqrsStatus",
        |fact| match fact {
            CqrsRuntimeFact::Status(status) => Some(status.clone()),
            _ => None,
        },
    );
    let errors = runtime_projection::<TCommand, TEvent, CqrsError<TCommand>>(
        graph,
        &runtime,
        &format!("{name}/errors"),
        "cqrsErrors",
        |fact| match fact {
            CqrsRuntimeFact::Error(error) => Some(error.clone()),
            _ => None,
        },
    );
    let audit = runtime_projection::<TCommand, TEvent, CqrsAuditRecord>(
        graph,
        &runtime,
        &format!("{name}/audit"),
        "cqrsAudit",
        |fact| match fact {
            CqrsRuntimeFact::Audit(audit) => Some(audit.clone()),
            _ => None,
        },
    );
    let cursor = runtime_projection::<TCommand, TEvent, CqrsCursor>(
        graph,
        &runtime,
        &format!("{name}/cursor"),
        "cqrsCursor",
        |fact| match fact {
            CqrsRuntimeFact::Cursor(cursor) => Some(cursor.clone()),
            _ => None,
        },
    );

    let retains = Rc::new(vec![
        CqrsRetain::new(graph.retain(&runtime, &format!("{name}.cqrs.runtime"))),
        CqrsRetain::new(graph.retain(&events, &format!("{name}.cqrs.events"))),
        CqrsRetain::new(graph.retain(&status, &format!("{name}.cqrs.status"))),
        CqrsRetain::new(graph.retain(&errors, &format!("{name}.cqrs.errors"))),
        CqrsRetain::new(graph.retain(&audit, &format!("{name}.cqrs.audit"))),
        CqrsRetain::new(graph.retain(&cursor, &format!("{name}.cqrs.cursor"))),
    ]);

    CqrsBundle {
        command,
        runtime,
        events,
        status,
        errors,
        audit,
        cursor,
        _retains: retains,
    }
}

pub type CqrsProjectionReducer<TState, TEvent> = Rc<dyn Fn(TState, &CqrsEvent<TEvent>) -> TState>;

#[derive(Clone)]
pub struct CqrsProjectionOptions<TState, TEvent> {
    pub name: String,
    pub events: Option<Vec<String>>,
    pub initial: TState,
    pub reducer: CqrsProjectionReducer<TState, TEvent>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CqrsProjectionFrame<TState> {
    Value {
        state: TState,
        event_id: String,
        cursor: CqrsCursor,
    },
    Error(CqrsProjectionError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CqrsProjectionError {
    pub code: CqrsProjectionErrorCode,
    pub message: String,
    pub event_id: String,
    pub event_type: String,
    pub cursor: CqrsCursor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CqrsProjectionErrorCode {
    ProjectionThrew,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CqrsProjectionStatus {
    pub state: CqrsProjectionStatusState,
    pub event_id: String,
    pub event_type: Option<String>,
    pub cursor: CqrsCursor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CqrsProjectionStatusState {
    Updated,
    Errored,
}

#[derive(Clone)]
pub struct CqrsProjection<TState> {
    pub frames: Node<CqrsProjectionFrame<TState>>,
    pub value: Node<TState>,
    pub status: Node<CqrsProjectionStatus>,
    pub errors: Node<CqrsProjectionError>,
    _retains: Rc<Vec<CqrsRetain>>,
}

pub fn cqrs_projection<TState: Clone + 'static, TEvent: Clone + 'static>(
    graph: &Graph,
    source: &CqrsBundle<impl Clone + 'static, TEvent>,
    opts: CqrsProjectionOptions<TState, TEvent>,
) -> CqrsProjection<TState> {
    let name = opts.name;
    let event_filter = Rc::new(normalize_events(opts.events));
    let reducer = opts.reducer;
    let initial = opts.initial;
    let frames = graph.init_node::<CqrsProjectionFrame<TState>>(
        Operator::with_opts("cqrsProjection", no_terminal_opts(), {
            move |ctx: &Ctx| {
                let mut state = ctx
                    .state_get::<ProjectionState<TState>>()
                    .map(|value| (*value).clone())
                    .unwrap_or_else(|| ProjectionState {
                        value: initial.clone(),
                    });
                ctx.state_persist(true);
                for event in ctx.batch::<CqrsEvent<TEvent>>(0) {
                    if let Some(filter) = event_filter.as_ref() {
                        if !filter.contains(&event.event_type) {
                            continue;
                        }
                    }
                    let event_for_reduce = (*event).clone();
                    let reduced = catch_unwind(AssertUnwindSafe(|| {
                        (reducer)(state.value.clone(), &event_for_reduce)
                    }));
                    match reduced {
                        Ok(next) => {
                            state.value = next.clone();
                            ctx.state_set(state.clone());
                            ctx.emit(CqrsProjectionFrame::Value {
                                state: next,
                                event_id: event.id.clone(),
                                cursor: event.runtime_cursor.clone(),
                            });
                        }
                        Err(panic) => {
                            if is_graph_runtime_panic(&panic) {
                                resume_unwind(panic);
                            }
                            ctx.emit(CqrsProjectionFrame::<TState>::Error(CqrsProjectionError {
                                code: CqrsProjectionErrorCode::ProjectionThrew,
                                message: panic_message(&panic),
                                event_id: event.id.clone(),
                                event_type: event.event_type.clone(),
                                cursor: event.runtime_cursor.clone(),
                            }));
                            return;
                        }
                    }
                }
            }
        }),
        vec![source.events.erased()],
        GraphNodeOpts::named(name.clone()),
    );
    let value = graph.init_node::<TState>(
        Operator::with_opts("cqrsProjectionValue", no_terminal_opts(), |ctx: &Ctx| {
            for frame in ctx.batch::<CqrsProjectionFrame<TState>>(0) {
                if let CqrsProjectionFrame::Value { state, .. } = frame.as_ref() {
                    ctx.emit(state.clone());
                }
            }
        }),
        vec![frames.erased()],
        GraphNodeOpts::named(format!("{name}/value")),
    );
    let status = graph.init_node::<CqrsProjectionStatus>(
        Operator::with_opts("cqrsProjectionStatus", no_terminal_opts(), |ctx: &Ctx| {
            for frame in ctx.batch::<CqrsProjectionFrame<TState>>(0) {
                match frame.as_ref() {
                    CqrsProjectionFrame::Value {
                        event_id, cursor, ..
                    } => ctx.emit(CqrsProjectionStatus {
                        state: CqrsProjectionStatusState::Updated,
                        event_id: event_id.clone(),
                        event_type: None,
                        cursor: cursor.clone(),
                    }),
                    CqrsProjectionFrame::Error(error) => ctx.emit(CqrsProjectionStatus {
                        state: CqrsProjectionStatusState::Errored,
                        event_id: error.event_id.clone(),
                        event_type: Some(error.event_type.clone()),
                        cursor: error.cursor.clone(),
                    }),
                }
            }
        }),
        vec![frames.erased()],
        GraphNodeOpts::named(format!("{name}/status")),
    );
    let errors = graph.init_node::<CqrsProjectionError>(
        Operator::with_opts("cqrsProjectionErrors", no_terminal_opts(), |ctx: &Ctx| {
            for frame in ctx.batch::<CqrsProjectionFrame<TState>>(0) {
                if let CqrsProjectionFrame::Error(error) = frame.as_ref() {
                    ctx.emit(error.clone());
                }
            }
        }),
        vec![frames.erased()],
        GraphNodeOpts::named(format!("{name}/errors")),
    );
    let retains = Rc::new(vec![
        CqrsRetain::new(graph.retain(&frames, &format!("{name}.cqrsProjection.frames"))),
        CqrsRetain::new(graph.retain(&value, &format!("{name}.cqrsProjection.value"))),
        CqrsRetain::new(graph.retain(&status, &format!("{name}.cqrsProjection.status"))),
        CqrsRetain::new(graph.retain(&errors, &format!("{name}.cqrsProjection.errors"))),
    ]);
    CqrsProjection {
        frames,
        value,
        status,
        errors,
        _retains: retains,
    }
}

#[derive(Clone)]
struct ProjectionState<T> {
    value: T,
}

#[derive(Clone, Default)]
struct RuntimeState {
    event_seq: u64,
    command_count: u64,
    error_count: u64,
    audit_seq: u64,
    seen_command_ids: Vec<String>,
    seen_event_ids: Vec<String>,
    command_dedupe_evicted: u64,
    event_dedupe_evicted: u64,
}

impl RuntimeState {
    fn from_json(value: &Value) -> Self {
        Self {
            event_seq: json_u64(value, "eventSeq"),
            command_count: json_u64(value, "commandCount"),
            error_count: json_u64(value, "errorCount"),
            audit_seq: json_u64(value, "auditSeq"),
            seen_command_ids: json_string_array(value, "seenCommandIds"),
            seen_event_ids: json_string_array(value, "seenEventIds"),
            command_dedupe_evicted: json_u64(value, "commandDedupeEvicted"),
            event_dedupe_evicted: json_u64(value, "eventDedupeEvicted"),
        }
    }

    fn to_json(&self) -> Value {
        json!({
            "eventSeq": self.event_seq,
            "commandCount": self.command_count,
            "errorCount": self.error_count,
            "auditSeq": self.audit_seq,
            "seenCommandIds": self.seen_command_ids,
            "seenEventIds": self.seen_event_ids,
            "commandDedupeEvicted": self.command_dedupe_evicted,
            "eventDedupeEvicted": self.event_dedupe_evicted,
        })
    }
}

fn reduce_command_fact<TCommand: Clone + 'static, TEvent: Clone + 'static>(
    state: &mut RuntimeState,
    command: CqrsCommand<TCommand>,
    handlers: &HashMap<String, CqrsCommandHandler<TCommand, TEvent>>,
    known_events: &Option<HashSet<String>>,
    now: &dyn Fn() -> u64,
    dedupe: CqrsDedupePolicy,
) -> Vec<CqrsRuntimeFact<TCommand, TEvent>> {
    state.command_count += 1;
    if command.id.is_empty() {
        return failure(
            state,
            Some(command),
            CqrsErrorCode::MalformedCommand,
            "cqrs: command id must be non-empty".to_owned(),
            dedupe,
        );
    }
    if command.command_type.is_empty() {
        return failure(
            state,
            Some(command),
            CqrsErrorCode::MalformedCommand,
            "cqrs: command type must be non-empty".to_owned(),
            dedupe,
        );
    }
    if state.seen_command_ids.contains(&command.id) {
        return failure(
            state,
            Some(command.clone()),
            CqrsErrorCode::DuplicateCommand,
            format!("cqrs: duplicate command '{}'", command.id),
            dedupe,
        );
    }
    state.seen_command_ids.push(command.id.clone());
    trim_dedupe_window(
        &mut state.seen_command_ids,
        dedupe_max_entries(dedupe.commands),
        &mut state.command_dedupe_evicted,
    );

    let Some(handler) = handlers.get(&command.command_type) else {
        return failure(
            state,
            Some(command.clone()),
            CqrsErrorCode::UnknownCommand,
            format!("cqrs: unknown command '{}'", command.command_type),
            dedupe,
        );
    };
    let drafts = match catch_unwind(AssertUnwindSafe(|| (handler)(&command))) {
        Ok(drafts) => drafts,
        Err(panic) => {
            if is_graph_runtime_panic(&panic) {
                resume_unwind(panic);
            }
            return failure(
                state,
                Some(command.clone()),
                CqrsErrorCode::HandlerThrew,
                panic_message(&panic),
                dedupe,
            );
        }
    };
    let prepared = match prepare_events(&command, drafts, state, known_events) {
        Ok(prepared) => prepared,
        Err((code, message)) => return failure(state, Some(command), code, message, dedupe),
    };
    let mut facts = Vec::new();
    let timestamp_ms = match cqrs_timestamp(now) {
        Ok(timestamp_ms) => timestamp_ms,
        Err(message) => {
            return failure(
                state,
                Some(command),
                CqrsErrorCode::ClockThrew,
                message,
                dedupe,
            );
        }
    };
    let mut events = Vec::new();
    for draft in prepared {
        state.event_seq += 1;
        state.seen_event_ids.push(draft.id.clone());
        trim_dedupe_window(
            &mut state.seen_event_ids,
            dedupe_max_entries(dedupe.events),
            &mut state.event_dedupe_evicted,
        );
        let event = CqrsEvent {
            id: draft.id,
            event_type: draft.event_type,
            seq: state.event_seq,
            cursor: state.event_seq,
            runtime_cursor: cursor_of(state, dedupe),
            command_id: command.id.clone(),
            command_type: command.command_type.clone(),
            payload: draft.payload,
            timestamp_ms,
            aggregate_id: draft.aggregate_id,
            correlation_id: draft.correlation_id,
            causation_id: draft.causation_id,
        };
        facts.push(CqrsRuntimeFact::Event(event.clone()));
        events.push(event);
    }
    facts.push(CqrsRuntimeFact::Status(CqrsStatus {
        state: CqrsStatusState::Accepted,
        command_id: Some(command.id.clone()),
        command_type: Some(command.command_type.clone()),
        event_count: events.len(),
        error_code: None,
        cursor: cursor_of(state, dedupe),
    }));
    facts.push(CqrsRuntimeFact::Audit(audit_record(
        state,
        Some(&command),
        CqrsAuditOutcome::Success,
        &events,
        None,
        None,
        dedupe,
    )));
    facts.push(CqrsRuntimeFact::Cursor(cursor_of(state, dedupe)));
    facts
}

#[derive(Clone)]
struct PreparedEvent<T> {
    id: String,
    event_type: String,
    payload: T,
    aggregate_id: Option<String>,
    correlation_id: Option<String>,
    causation_id: Option<String>,
}

fn prepare_events<TCommand, TEvent: Clone + 'static>(
    command: &CqrsCommand<TCommand>,
    drafts: Vec<CqrsEventDraft<TEvent>>,
    state: &RuntimeState,
    known_events: &Option<HashSet<String>>,
) -> Result<Vec<PreparedEvent<TEvent>>, (CqrsErrorCode, String)> {
    let mut seen_in_command = HashSet::new();
    let mut prepared = Vec::new();
    for (index, draft) in drafts.into_iter().enumerate() {
        if draft.event_type.is_empty() {
            return Err((
                CqrsErrorCode::MalformedEvent,
                "cqrs: event draft must have a non-empty type".to_owned(),
            ));
        }
        if let Some(known_events) = known_events {
            if !known_events.contains(&draft.event_type) {
                return Err((
                    CqrsErrorCode::UnknownEvent,
                    format!("cqrs: unknown event '{}'", draft.event_type),
                ));
            }
        }
        let id = draft
            .id
            .clone()
            .filter(|id| !id.is_empty())
            .unwrap_or_else(|| format!("{}:{}", command.id, index + 1));
        if state.seen_event_ids.contains(&id) || seen_in_command.contains(&id) {
            return Err((
                CqrsErrorCode::DuplicateEvent,
                format!("cqrs: duplicate event '{id}'"),
            ));
        }
        seen_in_command.insert(id.clone());
        prepared.push(PreparedEvent {
            id,
            event_type: draft.event_type,
            payload: draft.payload,
            aggregate_id: draft.aggregate_id,
            correlation_id: draft.correlation_id,
            causation_id: draft.causation_id,
        });
    }
    Ok(prepared)
}

fn failure<TCommand: Clone + 'static, TEvent: Clone + 'static>(
    state: &mut RuntimeState,
    command: Option<CqrsCommand<TCommand>>,
    code: CqrsErrorCode,
    message: String,
    dedupe: CqrsDedupePolicy,
) -> Vec<CqrsRuntimeFact<TCommand, TEvent>> {
    state.error_count += 1;
    let cursor = cursor_of(state, dedupe);
    vec![
        CqrsRuntimeFact::Error(CqrsError {
            code,
            message: message.clone(),
            command: command.clone(),
            cursor: cursor.clone(),
        }),
        CqrsRuntimeFact::Status(CqrsStatus {
            state: CqrsStatusState::Rejected,
            command_id: command.as_ref().map(|command| command.id.clone()),
            command_type: command.as_ref().map(|command| command.command_type.clone()),
            event_count: 0,
            error_code: Some(code),
            cursor: cursor.clone(),
        }),
        CqrsRuntimeFact::Audit(audit_record::<TCommand, TEvent>(
            state,
            command.as_ref(),
            CqrsAuditOutcome::Failure,
            &[],
            Some(code),
            Some(message),
            dedupe,
        )),
        CqrsRuntimeFact::Cursor(cursor_of(state, dedupe)),
    ]
}

fn audit_record<TCommand, TEvent>(
    state: &mut RuntimeState,
    command: Option<&CqrsCommand<TCommand>>,
    outcome: CqrsAuditOutcome,
    events: &[CqrsEvent<TEvent>],
    error_code: Option<CqrsErrorCode>,
    error_message: Option<String>,
    dedupe: CqrsDedupePolicy,
) -> CqrsAuditRecord {
    state.audit_seq += 1;
    CqrsAuditRecord {
        seq: state.audit_seq,
        command_id: command.map(|command| command.id.clone()),
        command_type: command.map(|command| command.command_type.clone()),
        outcome,
        event_ids: events.iter().map(|event| event.id.clone()).collect(),
        event_types: events
            .iter()
            .map(|event| event.event_type.clone())
            .collect(),
        error_code,
        error_message,
        cursor: cursor_of(state, dedupe),
    }
}

fn runtime_projection<TCommand: Clone + 'static, TEvent: Clone + 'static, TOut: Clone + 'static>(
    graph: &Graph,
    runtime: &Node<CqrsRuntimeFact<TCommand, TEvent>>,
    name: &str,
    factory: &'static str,
    select: impl Fn(&CqrsRuntimeFact<TCommand, TEvent>) -> Option<TOut> + 'static,
) -> Node<TOut> {
    graph.init_node::<TOut>(
        Operator::with_opts(factory, no_terminal_opts(), move |ctx: &Ctx| {
            for fact in ctx.batch::<CqrsRuntimeFact<TCommand, TEvent>>(0) {
                if let Some(selected) = select(fact.as_ref()) {
                    ctx.emit(selected);
                }
            }
        }),
        vec![runtime.erased()],
        GraphNodeOpts::named(name),
    )
}

fn normalize_handlers<TCommand, TEvent>(
    definitions: Vec<CqrsCommandHandlerDefinition<TCommand, TEvent>>,
) -> HashMap<String, CqrsCommandHandler<TCommand, TEvent>> {
    let mut handlers = HashMap::new();
    for definition in definitions {
        assert!(
            !definition.command_type.is_empty(),
            "cqrs: handler type must be non-empty"
        );
        assert!(
            !handlers.contains_key(&definition.command_type),
            "cqrs: duplicate handler '{}'",
            definition.command_type
        );
        handlers.insert(definition.command_type, definition.handle);
    }
    handlers
}

fn normalize_events(events: Option<Vec<String>>) -> Option<HashSet<String>> {
    let events = events?;
    let mut known = HashSet::new();
    for event in events {
        assert!(!event.is_empty(), "cqrs.events: values must be non-empty");
        assert!(known.insert(event.clone()), "cqrs.events: duplicate value");
    }
    Some(known)
}

fn no_terminal_opts() -> crate::node::NodeOpts {
    crate::node::NodeOpts {
        complete_when_deps_complete: false,
        error_when_deps_error: false,
        ..Default::default()
    }
}

fn cursor_of(state: &RuntimeState, dedupe: CqrsDedupePolicy) -> CqrsCursor {
    CqrsCursor {
        event_seq: state.event_seq,
        command_count: state.command_count,
        error_count: state.error_count,
        audit_seq: state.audit_seq,
        dedupe: if dedupe.bounded_any() {
            Some(CqrsDedupeSnapshot {
                command_ids_retained: state.seen_command_ids.len(),
                event_ids_retained: state.seen_event_ids.len(),
                command_ids_evicted: state.command_dedupe_evicted,
                event_ids_evicted: state.event_dedupe_evicted,
            })
        } else {
            None
        },
    }
}

fn dedupe_max_entries(window: CqrsDedupeWindow) -> Option<usize> {
    match window {
        CqrsDedupeWindow::Unbounded => None,
        CqrsDedupeWindow::Bounded { max_entries } => Some(max_entries),
    }
}

fn dedupe_meta(dedupe: CqrsDedupePolicy) -> String {
    match (dedupe.commands, dedupe.events) {
        (CqrsDedupeWindow::Unbounded, CqrsDedupeWindow::Unbounded) => "unbounded".to_owned(),
        (commands, events) => format!("commands={commands:?};events={events:?}"),
    }
}

fn trim_dedupe_window(ids: &mut Vec<String>, max_entries: Option<usize>, evicted_total: &mut u64) {
    let Some(max_entries) = max_entries else {
        return;
    };
    if ids.len() <= max_entries {
        return;
    }
    let evicted = ids.len() - max_entries;
    ids.drain(0..evicted);
    *evicted_total += evicted as u64;
}

fn json_u64(value: &Value, key: &str) -> u64 {
    value.get(key).and_then(Value::as_u64).unwrap_or(0)
}

fn json_string_array(value: &Value, key: &str) -> Vec<String> {
    value
        .get(key)
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
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

fn cqrs_timestamp(now: &dyn Fn() -> u64) -> Result<u64, String> {
    catch_unwind(AssertUnwindSafe(now))
        .map_err(|panic| format!("cqrs: now() threw: {}", panic_message(&panic)))
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
