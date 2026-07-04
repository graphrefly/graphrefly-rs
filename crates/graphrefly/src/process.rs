//! Graph-visible process orchestration bundle (D136 / B84).
//!
//! A `ProcessBundle` is facts plus a reducer: command DATA facts enter a
//! graph-owned runtime node, and state/event/effect-request/status/error/audit/
//! cursor projections are ordinary graph nodes with declared deps. It is not a
//! workflow engine, effect runner, storage restore path, or hidden process manager.

pub mod messaging;
pub mod work_queue;

use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::rc::Rc;

use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

use crate::ctx::Ctx;
use crate::graph::{Graph, GraphNodeOpts};
use crate::identity::compound_tuple_key;
use crate::node::{Core, Node};
use crate::operators::Operator;

#[derive(Debug, Clone, PartialEq)]
/// `ProcessCommand` data container.
pub struct ProcessCommand<T = crate::protocol::AnyValue> {
    /// `id` field for id.
    pub id: String,
    /// `command_type` field for command type.
    pub command_type: String,
    /// `payload` field for payload.
    pub payload: T,
    /// `process_id` field for process id.
    pub process_id: Option<String>,
    /// `correlation_id` field for correlation id.
    pub correlation_id: Option<String>,
    /// `causation_id` field for causation id.
    pub causation_id: Option<String>,
}

impl<T> ProcessCommand<T> {
    /// Creates or computes `new`.
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
/// `ProcessEventDraft` data container.
pub struct ProcessEventDraft<T = crate::protocol::AnyValue> {
    /// `id` field for id.
    pub id: Option<String>,
    /// `event_type` field for event type.
    pub event_type: String,
    /// `payload` field for payload.
    pub payload: T,
    /// `process_id` field for process id.
    pub process_id: Option<String>,
    /// `correlation_id` field for correlation id.
    pub correlation_id: Option<String>,
    /// `causation_id` field for causation id.
    pub causation_id: Option<String>,
}

impl<T> ProcessEventDraft<T> {
    /// Creates or computes `new`.
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
/// `ProcessEvent` data container.
pub struct ProcessEvent<T = crate::protocol::AnyValue> {
    /// `id` field for id.
    pub id: String,
    /// `event_type` field for event type.
    pub event_type: String,
    /// `seq` field for seq.
    pub seq: u64,
    /// `cursor` field for cursor.
    pub cursor: u64,
    /// `command_id` field for command id.
    pub command_id: String,
    /// `command_type` field for command type.
    pub command_type: String,
    /// `payload` field for payload.
    pub payload: T,
    /// `timestamp_ms` field for timestamp ms.
    pub timestamp_ms: u64,
    /// `process_id` field for process id.
    pub process_id: Option<String>,
    /// `correlation_id` field for correlation id.
    pub correlation_id: Option<String>,
    /// `causation_id` field for causation id.
    pub causation_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
/// `ProcessEffectRequestDraft` data container.
pub struct ProcessEffectRequestDraft<T = crate::protocol::AnyValue> {
    /// `id` field for id.
    pub id: Option<String>,
    /// `effect_type` field for effect type.
    pub effect_type: String,
    /// `payload` field for payload.
    pub payload: T,
    /// `process_id` field for process id.
    pub process_id: Option<String>,
    /// `correlation_id` field for correlation id.
    pub correlation_id: Option<String>,
    /// `causation_id` field for causation id.
    pub causation_id: Option<String>,
}

impl<T> ProcessEffectRequestDraft<T> {
    /// Creates or computes `new`.
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
/// `ProcessEffectRequest` data container.
pub struct ProcessEffectRequest<T = crate::protocol::AnyValue> {
    /// `id` field for id.
    pub id: String,
    /// `effect_type` field for effect type.
    pub effect_type: String,
    /// `seq` field for seq.
    pub seq: u64,
    /// `cursor` field for cursor.
    pub cursor: u64,
    /// `command_id` field for command id.
    pub command_id: String,
    /// `command_type` field for command type.
    pub command_type: String,
    /// `payload` field for payload.
    pub payload: T,
    /// `timestamp_ms` field for timestamp ms.
    pub timestamp_ms: u64,
    /// `process_id` field for process id.
    pub process_id: Option<String>,
    /// `correlation_id` field for correlation id.
    pub correlation_id: Option<String>,
    /// `causation_id` field for causation id.
    pub causation_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// `ProcessEffectOutcomeKind` variants.
pub enum ProcessEffectOutcomeKind {
    /// `Result` variant.
    Result,
    /// `Failure` variant.
    Failure,
    /// `Cancel` variant.
    Cancel,
    /// `Timeout` variant.
    Timeout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// `ProcessEffectCommandType` variants.
pub enum ProcessEffectCommandType {
    /// `Result` variant.
    Result,
    /// `Failure` variant.
    Failure,
    /// `Cancel` variant.
    Cancel,
    /// `Timeout` variant.
    Timeout,
}

impl ProcessEffectCommandType {
    /// Updates or reads `as_str`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Result => "effect.result",
            Self::Failure => "effect.failure",
            Self::Cancel => "effect.cancel",
            Self::Timeout => "effect.timeout",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
/// `ProcessEffectOutcome` data container.
pub struct ProcessEffectOutcome<TResult = crate::protocol::AnyValue> {
    /// `kind` field for kind.
    pub kind: ProcessEffectOutcomeKind,
    /// `effect_id` field for effect id.
    pub effect_id: String,
    /// `effect_type` field for effect type.
    pub effect_type: String,
    /// `value` field for value.
    pub value: Option<TResult>,
    /// `error` field for error.
    pub error: Option<String>,
    /// `reason` field for reason.
    pub reason: Option<String>,
    /// `command_id` field for command id.
    pub command_id: Option<String>,
    /// `process_id` field for process id.
    pub process_id: Option<String>,
    /// `correlation_id` field for correlation id.
    pub correlation_id: Option<String>,
    /// `causation_id` field for causation id.
    pub causation_id: Option<String>,
}

impl<TResult> ProcessEffectOutcome<TResult> {
    /// Creates or computes `result`.
    pub fn result(
        effect_id: impl Into<String>,
        effect_type: impl Into<String>,
        value: TResult,
    ) -> Self {
        Self {
            kind: ProcessEffectOutcomeKind::Result,
            effect_id: effect_id.into(),
            effect_type: effect_type.into(),
            value: Some(value),
            error: None,
            reason: None,
            command_id: None,
            process_id: None,
            correlation_id: None,
            causation_id: None,
        }
    }

    /// Creates or computes `failure`.
    pub fn failure(
        effect_id: impl Into<String>,
        effect_type: impl Into<String>,
        error: impl Into<String>,
    ) -> Self {
        Self {
            kind: ProcessEffectOutcomeKind::Failure,
            effect_id: effect_id.into(),
            effect_type: effect_type.into(),
            value: None,
            error: Some(error.into()),
            reason: None,
            command_id: None,
            process_id: None,
            correlation_id: None,
            causation_id: None,
        }
    }

    /// Creates or computes `cancel`.
    pub fn cancel(effect_id: impl Into<String>, effect_type: impl Into<String>) -> Self {
        Self {
            kind: ProcessEffectOutcomeKind::Cancel,
            effect_id: effect_id.into(),
            effect_type: effect_type.into(),
            value: None,
            error: None,
            reason: None,
            command_id: None,
            process_id: None,
            correlation_id: None,
            causation_id: None,
        }
    }

    /// Creates or computes `timeout`.
    pub fn timeout(
        effect_id: impl Into<String>,
        effect_type: impl Into<String>,
        error: impl Into<String>,
    ) -> Self {
        Self {
            kind: ProcessEffectOutcomeKind::Timeout,
            effect_id: effect_id.into(),
            effect_type: effect_type.into(),
            value: None,
            error: Some(error.into()),
            reason: None,
            command_id: None,
            process_id: None,
            correlation_id: None,
            causation_id: None,
        }
    }

    /// Updates or reads `with_command_id`.
    pub fn with_command_id(mut self, command_id: impl Into<String>) -> Self {
        self.command_id = Some(command_id.into());
        self
    }

    /// Updates or reads `with_process_id`.
    pub fn with_process_id(mut self, process_id: impl Into<String>) -> Self {
        self.process_id = Some(process_id.into());
        self
    }

    /// Updates or reads `with_correlation_id`.
    pub fn with_correlation_id(mut self, correlation_id: impl Into<String>) -> Self {
        self.correlation_id = Some(correlation_id.into());
        self
    }

    /// Updates or reads `with_causation_id`.
    pub fn with_causation_id(mut self, causation_id: impl Into<String>) -> Self {
        self.causation_id = Some(causation_id.into());
        self
    }

    /// Updates or reads `with_reason`.
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
/// `ProcessEffectCommandPayload` data container.
pub struct ProcessEffectCommandPayload<TResult = crate::protocol::AnyValue> {
    /// `kind` field for kind.
    pub kind: ProcessEffectOutcomeKind,
    /// `effect_id` field for effect id.
    pub effect_id: String,
    /// `effect_type` field for effect type.
    pub effect_type: String,
    /// `value` field for value.
    pub value: Option<TResult>,
    /// `error` field for error.
    pub error: Option<String>,
    /// `reason` field for reason.
    pub reason: Option<String>,
    /// `process_id` field for process id.
    pub process_id: Option<String>,
    /// `correlation_id` field for correlation id.
    pub correlation_id: Option<String>,
    /// `causation_id` field for causation id.
    pub causation_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// `ProcessEffectRunnerStatusState` variants.
pub enum ProcessEffectRunnerStatusState {
    /// `Requested` variant.
    Requested,
    /// `Commanded` variant.
    Commanded,
    /// `Rejected` variant.
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `ProcessEffectRunnerStatus` data container.
pub struct ProcessEffectRunnerStatus {
    /// `state` field for state.
    pub state: ProcessEffectRunnerStatusState,
    /// `effect_id` field for effect id.
    pub effect_id: Option<String>,
    /// `effect_type` field for effect type.
    pub effect_type: Option<String>,
    /// `command_id` field for command id.
    pub command_id: Option<String>,
    /// `command_type` field for command type.
    pub command_type: Option<ProcessEffectCommandType>,
    /// `requested` field for requested.
    pub requested: u64,
    /// `commanded` field for commanded.
    pub commanded: u64,
    /// `rejected` field for rejected.
    pub rejected: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// `ProcessEffectRunnerErrorCode` variants.
pub enum ProcessEffectRunnerErrorCode {
    /// `MalformedOutcome` variant.
    MalformedOutcome,
}

#[derive(Debug, Clone, PartialEq)]
/// `ProcessEffectRunnerError` data container.
pub struct ProcessEffectRunnerError<TResult = crate::protocol::AnyValue> {
    /// `code` field for code.
    pub code: ProcessEffectRunnerErrorCode,
    /// `message` field for message.
    pub message: String,
    /// `outcome` field for outcome.
    pub outcome: Option<ProcessEffectOutcome<TResult>>,
    /// `effect_id` field for effect id.
    pub effect_id: Option<String>,
    /// `effect_type` field for effect type.
    pub effect_type: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// `ProcessErrorCode` variants.
pub enum ProcessErrorCode {
    /// `MalformedCommand` variant.
    MalformedCommand,
    /// `ReducerThrew` variant.
    ReducerThrew,
    /// `ClockThrew` variant.
    ClockThrew,
    /// `MalformedState` variant.
    MalformedState,
    /// `MalformedEvent` variant.
    MalformedEvent,
    /// `MalformedEffect` variant.
    MalformedEffect,
}

#[derive(Debug, Clone, PartialEq)]
/// `ProcessError` data container.
pub struct ProcessError<TCommand = crate::protocol::AnyValue> {
    /// `code` field for code.
    pub code: ProcessErrorCode,
    /// `message` field for message.
    pub message: String,
    /// `command` field for command.
    pub command: Option<ProcessCommand<TCommand>>,
    /// `cursor` field for cursor.
    pub cursor: ProcessCursor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `ProcessCursor` data container.
pub struct ProcessCursor {
    /// `event_seq` field for event seq.
    pub event_seq: u64,
    /// `effect_seq` field for effect seq.
    pub effect_seq: u64,
    /// `command_count` field for command count.
    pub command_count: u64,
    /// `error_count` field for error count.
    pub error_count: u64,
    /// `audit_seq` field for audit seq.
    pub audit_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `ProcessStatus` data container.
pub struct ProcessStatus {
    /// `state` field for state.
    pub state: ProcessStatusState,
    /// `command_id` field for command id.
    pub command_id: Option<String>,
    /// `command_type` field for command type.
    pub command_type: Option<String>,
    /// `event_count` field for event count.
    pub event_count: usize,
    /// `effect_count` field for effect count.
    pub effect_count: usize,
    /// `error_code` field for error code.
    pub error_code: Option<ProcessErrorCode>,
    /// `cursor` field for cursor.
    pub cursor: ProcessCursor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// `ProcessStatusState` variants.
pub enum ProcessStatusState {
    /// `Accepted` variant.
    Accepted,
    /// `Rejected` variant.
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `ProcessAuditRecord` data container.
pub struct ProcessAuditRecord {
    /// `seq` field for seq.
    pub seq: u64,
    /// `command_id` field for command id.
    pub command_id: Option<String>,
    /// `command_type` field for command type.
    pub command_type: Option<String>,
    /// `outcome` field for outcome.
    pub outcome: ProcessAuditOutcome,
    /// `event_ids` field for event ids.
    pub event_ids: Vec<String>,
    /// `event_types` field for event types.
    pub event_types: Vec<String>,
    /// `effect_ids` field for effect ids.
    pub effect_ids: Vec<String>,
    /// `effect_types` field for effect types.
    pub effect_types: Vec<String>,
    /// `error_code` field for error code.
    pub error_code: Option<ProcessErrorCode>,
    /// `error_message` field for error message.
    pub error_message: Option<String>,
    /// `cursor` field for cursor.
    pub cursor: ProcessCursor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// `ProcessAuditOutcome` variants.
pub enum ProcessAuditOutcome {
    /// `Success` variant.
    Success,
    /// `Failure` variant.
    Failure,
}

#[derive(Debug, Clone, PartialEq)]
/// `ProcessRuntimeFact` variants.
pub enum ProcessRuntimeFact<
    TState = crate::protocol::AnyValue,
    TEvent = crate::protocol::AnyValue,
    TEffect = crate::protocol::AnyValue,
    TCommand = crate::protocol::AnyValue,
> {
    /// `State` variant.
    State {
        /// `state` field for state.
        state: TState,
        /// `cursor` field for cursor.
        cursor: ProcessCursor,
    },
    /// `Event` variant.
    Event(ProcessEvent<TEvent>),
    /// `EffectRequest` variant.
    EffectRequest(ProcessEffectRequest<TEffect>),
    /// `Status` variant.
    Status(ProcessStatus),
    /// `Error` variant.
    Error(ProcessError<TCommand>),
    /// `Audit` variant.
    Audit(ProcessAuditRecord),
    /// `Cursor` variant.
    Cursor(ProcessCursor),
}

#[derive(Debug, Clone, PartialEq)]
/// `ProcessReduction` data container.
pub struct ProcessReduction<TState, TEvent, TEffect> {
    /// `state` field for state.
    pub state: TState,
    /// `events` field for events.
    pub events: Vec<ProcessEventDraft<TEvent>>,
    /// `effects` field for effects.
    pub effects: Vec<ProcessEffectRequestDraft<TEffect>>,
}

impl<TState, TEvent, TEffect> ProcessReduction<TState, TEvent, TEffect> {
    /// Creates or computes `new`.
    pub fn new(state: TState) -> Self {
        Self {
            state,
            events: Vec::new(),
            effects: Vec::new(),
        }
    }

    /// Updates or reads `with_events`.
    pub fn with_events(mut self, events: Vec<ProcessEventDraft<TEvent>>) -> Self {
        self.events = events;
        self
    }

    /// Updates or reads `with_effects`.
    pub fn with_effects(mut self, effects: Vec<ProcessEffectRequestDraft<TEffect>>) -> Self {
        self.effects = effects;
        self
    }
}

/// `ProcessReducerFn` type alias.
pub type ProcessReducerFn<TCommand, TState, TEvent, TEffect> =
    dyn Fn(&ProcessCommand<TCommand>, TState) -> ProcessReduction<TState, TEvent, TEffect>;

/// `ProcessReducer` type alias.
pub type ProcessReducer<TCommand, TState, TEvent, TEffect> =
    Rc<ProcessReducerFn<TCommand, TState, TEvent, TEffect>>;

#[derive(Clone)]
/// `ProcessBundleOptions` data container.
pub struct ProcessBundleOptions<TCommand, TState, TEvent, TEffect> {
    /// `name` field for name.
    pub name: String,
    /// `initial_state` field for initial state.
    pub initial_state: TState,
    /// `reduce` field for reduce.
    pub reduce: ProcessReducer<TCommand, TState, TEvent, TEffect>,
    /// `now` field for now.
    pub now: Rc<dyn Fn() -> u64>,
}

impl<TCommand, TState, TEvent, TEffect> ProcessBundleOptions<TCommand, TState, TEvent, TEffect> {
    /// Creates or computes `new`.
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

    /// Updates or reads `named`.
    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Updates or reads `with_now`.
    pub fn with_now(mut self, now: impl Fn() -> u64 + 'static) -> Self {
        self.now = Rc::new(now);
        self
    }
}

#[derive(Clone)]
/// `ProcessBundle` data container.
pub struct ProcessBundle<
    TCommand = crate::protocol::AnyValue,
    TState = crate::protocol::AnyValue,
    TEvent = crate::protocol::AnyValue,
    TEffect = crate::protocol::AnyValue,
> {
    /// `command` field for command.
    pub command: Node<ProcessCommand<TCommand>>,
    /// `state` field for state.
    pub state: Node<TState>,
    /// `events` field for events.
    pub events: Node<ProcessEvent<TEvent>>,
    /// `audit` field for audit.
    pub audit: Node<ProcessAuditRecord>,
    /// `effect_request` field for effect request.
    pub effect_request: Node<ProcessEffectRequest<TEffect>>,
    /// `status` field for status.
    pub status: Node<ProcessStatus>,
    /// `error` field for error.
    pub error: Node<ProcessError<TCommand>>,
    /// `cursor` field for cursor.
    pub cursor: Node<ProcessCursor>,
    command_sources: Rc<RefCell<Vec<Core>>>,
    command_id: String,
    _retains: Rc<Vec<ProcessRetain>>,
}

impl<TCommand: Clone + 'static, TState, TEvent, TEffect>
    ProcessBundle<TCommand, TState, TEvent, TEffect>
{
    /// Updates or reads `dispatch`.
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

/// `ProcessEffectRunnerOptions` data container.
pub struct ProcessEffectRunnerOptions<TCommand, TResult = crate::protocol::AnyValue> {
    /// `name` field for name.
    pub name: String,
    /// `outcomes` field for outcomes.
    pub outcomes: Vec<Node<ProcessEffectOutcome<TResult>>>,
    command_payload: Rc<dyn Fn(ProcessEffectCommandPayload<TResult>) -> TCommand>,
}

impl<TResult: Clone + 'static>
    ProcessEffectRunnerOptions<ProcessEffectCommandPayload<TResult>, TResult>
{
    /// Creates or computes `new`.
    pub fn new(outcomes: Vec<Node<ProcessEffectOutcome<TResult>>>) -> Self {
        Self {
            name: "processEffectRunner".to_owned(),
            outcomes,
            command_payload: Rc::new(|payload| payload),
        }
    }
}

impl<TCommand, TResult> ProcessEffectRunnerOptions<TCommand, TResult> {
    /// Updates or reads `named`.
    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Updates or reads `map_command_payload`.
    pub fn map_command_payload<TNext>(
        self,
        map: impl Fn(ProcessEffectCommandPayload<TResult>) -> TNext + 'static,
    ) -> ProcessEffectRunnerOptions<TNext, TResult> {
        ProcessEffectRunnerOptions {
            name: self.name,
            outcomes: self.outcomes,
            command_payload: Rc::new(map),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum ProcessEffectRunnerFact<TCommand, TResult = crate::protocol::AnyValue> {
    Outcome(ProcessEffectOutcome<TResult>),
    Command(
        ProcessCommand<TCommand>,
        ProcessEffectCommandPayload<TResult>,
    ),
    Error(ProcessEffectRunnerError<TResult>),
}

/// `ProcessEffectRunnerBundle` data container.
pub struct ProcessEffectRunnerBundle<
    TCommand = ProcessEffectCommandPayload<crate::protocol::AnyValue>,
    TEffect = crate::protocol::AnyValue,
    TResult = crate::protocol::AnyValue,
> {
    /// `requests` field for requests.
    pub requests: Node<ProcessEffectRequest<TEffect>>,
    /// `outcomes` field for outcomes.
    pub outcomes: Node<ProcessEffectOutcome<TResult>>,
    /// `commands` field for commands.
    pub commands: Node<ProcessCommand<TCommand>>,
    /// `status` field for status.
    pub status: Node<ProcessEffectRunnerStatus>,
    /// `errors` field for errors.
    pub errors: Node<ProcessEffectRunnerError<TResult>>,
    runtime: Node<ProcessEffectRunnerFact<TCommand, TResult>>,
    graph: Graph,
    process_command: Node<ProcessCommand<TCommand>>,
    process_command_id: String,
    name: String,
    command_sources: Rc<RefCell<Vec<Core>>>,
    released: Cell<bool>,
    retains: RefCell<Vec<ProcessRetain>>,
}

impl<TCommand: Clone + 'static, TEffect: Clone + 'static, TResult: Clone + 'static>
    ProcessEffectRunnerBundle<TCommand, TEffect, TResult>
{
    /// D156/D157-style release: detach runner.commands from process.command, then release helper nodes.
    pub fn release(&self) {
        if self.released.get() {
            return;
        }
        self.preflight_release();
        detach_process_command_source(
            &self.process_command,
            &self.command_sources,
            self.commands.erased(),
        );
        let active_retains = self.retains.replace(Vec::new());
        drop(active_retains);
        self.released.set(true);
        self.graph.release_nodes(
            &[
                self.requests.erased(),
                self.outcomes.erased(),
                self.runtime.erased(),
                self.commands.erased(),
                self.status.erased(),
                self.errors.erased(),
            ],
            "process_effect_runner release",
        );
    }

    fn preflight_release(&self) {
        let release_cores = [
            self.requests.erased(),
            self.outcomes.erased(),
            self.runtime.erased(),
            self.commands.erased(),
            self.status.erased(),
            self.errors.erased(),
        ];
        let release_ids = self.release_node_ids();
        let release_id_set = release_ids.iter().cloned().collect::<HashSet<_>>();
        for edge in self.graph.describe().edges {
            if !release_id_set.contains(&edge.from) || release_id_set.contains(&edge.to) {
                continue;
            }
            if edge.from == format!("{}/commands", self.name) && edge.to == self.process_command_id
            {
                continue;
            }
            panic!(
                "process_effect_runner: cannot release '{}'; '{}' still depends on '{}' (D122)",
                self.name, edge.to, edge.from
            );
        }
        for (index, core) in release_cores.iter().enumerate() {
            assert!(
                core.runtime_is_quiescent_for_release(),
                "process_effect_runner: cannot release '{}'; '{}' is not runtime-quiescent (D124)",
                self.name,
                release_ids[index]
            );
            let internal_subscribers = release_cores
                .iter()
                .filter(|dependent| dependent.is_active())
                .flat_map(Core::deps)
                .filter(|dep| dep.ptr_eq(core))
                .count();
            let retain_subscriber = 1;
            let process_command_subscriber =
                usize::from(release_ids[index] == format!("{}/commands", self.name));
            assert!(
                core.subscriber_count()
                    <= internal_subscribers + retain_subscriber + process_command_subscriber,
                "process_effect_runner: cannot release '{}'; '{}' still has live subscribers (D124)",
                self.name,
                release_ids[index]
            );
        }
    }

    fn release_node_ids(&self) -> Vec<String> {
        vec![
            format!("{}/requests", self.name),
            format!("{}/outcomes", self.name),
            format!("{}/runtime", self.name),
            format!("{}/commands", self.name),
            format!("{}/status", self.name),
            format!("{}/errors", self.name),
        ]
    }
}

/// Creates or computes `process_bundle`.
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
    let command_sources = Rc::new(RefCell::new(Vec::new()));
    let initial_state_json = state_to_json(&opts.initial_state)
        .unwrap_or_else(|message| panic!("process_bundle: {message}"));

    let command_id = format!("{name}/command");
    let command = graph.init_node::<ProcessCommand<TCommand>>(
        Operator::with_opts(
            "processCommand",
            no_terminal_opts(),
            process_command_source_body::<TCommand>(0),
        ),
        Vec::new(),
        meta_opts(command_id.clone(), "command"),
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
        command_sources,
        command_id,
        _retains: retains,
    }
}

/// Creates or computes `process_effect_runner`.
pub fn process_effect_runner<TCommand, TState, TEvent, TEffect, TResult>(
    graph: &Graph,
    process: &ProcessBundle<TCommand, TState, TEvent, TEffect>,
    opts: ProcessEffectRunnerOptions<TCommand, TResult>,
) -> ProcessEffectRunnerBundle<TCommand, TEffect, TResult>
where
    TCommand: Clone + 'static,
    TEffect: Clone + 'static,
    TResult: Clone + 'static,
{
    assert!(
        !opts.outcomes.is_empty(),
        "process_effect_runner: outcomes must contain at least one node"
    );
    let name = opts.name;
    let command_payload = opts.command_payload;

    let requests = graph.init_node::<ProcessEffectRequest<TEffect>>(
        Operator::with_opts("processEffectRunnerRequests", no_terminal_opts(), |ctx| {
            for request in ctx.batch::<ProcessEffectRequest<TEffect>>(0) {
                ctx.emit((*request).clone());
            }
        }),
        vec![process.effect_request.erased()],
        effect_runner_meta_opts(format!("{name}/requests"), "requests"),
    );

    let outcome_deps = opts.outcomes.iter().map(Node::erased).collect::<Vec<_>>();
    let runtime = graph.init_node::<ProcessEffectRunnerFact<TCommand, TResult>>(
        Operator::with_opts("processEffectRunner", no_terminal_opts(), {
            let outcome_count = outcome_deps.len();
            move |ctx: &Ctx| {
                for index in 0..outcome_count {
                    for outcome in ctx.batch::<ProcessEffectOutcome<TResult>>(index) {
                        match process_effect_outcome_command(
                            outcome.as_ref(),
                            command_payload.as_ref(),
                        ) {
                            Ok((command, payload)) => {
                                ctx.emit(ProcessEffectRunnerFact::<TCommand, TResult>::Outcome(
                                    (*outcome).clone(),
                                ));
                                ctx.emit(ProcessEffectRunnerFact::Command(command, payload));
                            }
                            Err(error) => {
                                ctx.emit(ProcessEffectRunnerFact::<TCommand, TResult>::Error(
                                    *error,
                                ));
                            }
                        }
                    }
                }
            }
        }),
        outcome_deps,
        effect_runner_meta_opts(format!("{name}/runtime"), "runtime"),
    );

    let outcomes = effect_runner_projection::<TCommand, TResult, ProcessEffectOutcome<TResult>>(
        graph,
        &runtime,
        &format!("{name}/outcomes"),
        "processEffectRunnerOutcomes",
        |fact| match fact {
            ProcessEffectRunnerFact::Outcome(outcome) => Some(outcome.clone()),
            _ => None,
        },
    );
    let commands = effect_runner_projection::<TCommand, TResult, ProcessCommand<TCommand>>(
        graph,
        &runtime,
        &format!("{name}/commands"),
        "processEffectRunnerCommands",
        |fact| match fact {
            ProcessEffectRunnerFact::Command(command, _) => Some(command.clone()),
            _ => None,
        },
    );
    let errors = effect_runner_projection::<TCommand, TResult, ProcessEffectRunnerError<TResult>>(
        graph,
        &runtime,
        &format!("{name}/errors"),
        "processEffectRunnerErrors",
        |fact| match fact {
            ProcessEffectRunnerFact::Error(error) => Some(error.clone()),
            _ => None,
        },
    );
    let status = graph.init_node::<ProcessEffectRunnerStatus>(
        Operator::with_opts("processEffectRunnerStatus", no_terminal_opts(), |ctx| {
            let mut counters = ctx
                .state_get::<ProcessEffectRunnerCounters>()
                .map_or_else(ProcessEffectRunnerCounters::default, |state| {
                    (*state).clone()
                });
            for request in ctx.batch::<ProcessEffectRequest<TEffect>>(0) {
                counters.requested += 1;
                ctx.emit(ProcessEffectRunnerStatus {
                    state: ProcessEffectRunnerStatusState::Requested,
                    effect_id: Some(request.id.clone()),
                    effect_type: Some(request.effect_type.clone()),
                    command_id: None,
                    command_type: None,
                    requested: counters.requested,
                    commanded: counters.commanded,
                    rejected: counters.rejected,
                });
            }
            for fact in ctx.batch::<ProcessEffectRunnerFact<TCommand, TResult>>(1) {
                match fact.as_ref() {
                    ProcessEffectRunnerFact::Command(command, payload) => {
                        counters.commanded += 1;
                        ctx.emit(ProcessEffectRunnerStatus {
                            state: ProcessEffectRunnerStatusState::Commanded,
                            effect_id: Some(payload.effect_id.clone()),
                            effect_type: Some(payload.effect_type.clone()),
                            command_id: Some(command.id.clone()),
                            command_type: Some(command_type_for_outcome(payload.kind)),
                            requested: counters.requested,
                            commanded: counters.commanded,
                            rejected: counters.rejected,
                        });
                    }
                    ProcessEffectRunnerFact::Error(error) => {
                        counters.rejected += 1;
                        ctx.emit(ProcessEffectRunnerStatus {
                            state: ProcessEffectRunnerStatusState::Rejected,
                            effect_id: error.effect_id.clone(),
                            effect_type: error.effect_type.clone(),
                            command_id: None,
                            command_type: None,
                            requested: counters.requested,
                            commanded: counters.commanded,
                            rejected: counters.rejected,
                        });
                    }
                    ProcessEffectRunnerFact::Outcome(_) => {}
                }
            }
            ctx.state_set(counters);
        }),
        vec![requests.erased(), runtime.erased()],
        effect_runner_meta_opts(format!("{name}/status"), "status"),
    );

    let attach = catch_unwind(AssertUnwindSafe(|| {
        attach_process_command_source_parts(
            &process.command,
            &process.command_sources,
            commands.erased(),
        );
    }));
    if let Err(panic) = attach {
        graph.release_nodes(
            &[
                requests.erased(),
                outcomes.erased(),
                runtime.erased(),
                commands.erased(),
                status.erased(),
                errors.erased(),
            ],
            "process_effect_runner failed command wiring",
        );
        resume_unwind(panic);
    }
    let retains = retain_effect_runner_nodes(
        graph,
        EffectRunnerNodeRefs {
            requests: &requests,
            outcomes: &outcomes,
            runtime: &runtime,
            commands: &commands,
            status: &status,
            errors: &errors,
        },
        &name,
    );

    ProcessEffectRunnerBundle {
        requests,
        outcomes,
        commands,
        status,
        errors,
        runtime,
        graph: graph.clone(),
        process_command: process.command.clone(),
        process_command_id: process.command_id.clone(),
        name,
        command_sources: process.command_sources.clone(),
        released: Cell::new(false),
        retains: RefCell::new(retains),
    }
}

#[derive(Clone, Default)]
struct ProcessEffectRunnerCounters {
    requested: u64,
    commanded: u64,
    rejected: u64,
}

type ProcessEffectRunnerCommandResult<TCommand, TResult> = Result<
    (
        ProcessCommand<TCommand>,
        ProcessEffectCommandPayload<TResult>,
    ),
    Box<ProcessEffectRunnerError<TResult>>,
>;

fn process_effect_outcome_command<TCommand, TResult: Clone>(
    outcome: &ProcessEffectOutcome<TResult>,
    map_payload: &dyn Fn(ProcessEffectCommandPayload<TResult>) -> TCommand,
) -> ProcessEffectRunnerCommandResult<TCommand, TResult> {
    validate_process_effect_outcome(outcome)?;
    let command_type = command_type_for_outcome(outcome.kind);
    let payload = ProcessEffectCommandPayload {
        kind: outcome.kind,
        effect_id: outcome.effect_id.clone(),
        effect_type: outcome.effect_type.clone(),
        value: outcome.value.clone(),
        error: outcome.error.clone(),
        reason: outcome.reason.clone(),
        process_id: outcome.process_id.clone(),
        correlation_id: outcome.correlation_id.clone(),
        causation_id: outcome.causation_id.clone(),
    };
    let command = ProcessCommand {
        id: outcome.command_id.clone().unwrap_or_else(|| {
            compound_tuple_key(
                "process-effect-command",
                &[&outcome.effect_id, command_type.as_str()],
            )
        }),
        command_type: command_type.as_str().to_owned(),
        payload: map_payload(payload.clone()),
        process_id: outcome.process_id.clone(),
        correlation_id: outcome.correlation_id.clone(),
        causation_id: outcome.causation_id.clone(),
    };
    Ok((command, payload))
}

fn validate_process_effect_outcome<TResult>(
    outcome: &ProcessEffectOutcome<TResult>,
) -> Result<(), Box<ProcessEffectRunnerError<TResult>>>
where
    TResult: Clone,
{
    let reject = |message: String| {
        Box::new(ProcessEffectRunnerError {
            code: ProcessEffectRunnerErrorCode::MalformedOutcome,
            message,
            outcome: Some(outcome.clone()),
            effect_id: (!outcome.effect_id.is_empty()).then(|| outcome.effect_id.clone()),
            effect_type: (!outcome.effect_type.is_empty()).then(|| outcome.effect_type.clone()),
        })
    };
    if outcome.effect_id.is_empty() {
        return Err(reject(
            "process_effect_runner: outcome effect_id must be a non-empty string".to_owned(),
        ));
    }
    if outcome.effect_type.is_empty() {
        return Err(reject(
            "process_effect_runner: outcome effect_type must be a non-empty string".to_owned(),
        ));
    }
    if matches!(outcome.command_id.as_deref(), Some("")) {
        return Err(reject(
            "process_effect_runner: outcome command_id must be a non-empty string".to_owned(),
        ));
    }
    match outcome.kind {
        ProcessEffectOutcomeKind::Result if outcome.value.is_none() => Err(reject(
            "process_effect_runner: result outcome must carry value".to_owned(),
        )),
        ProcessEffectOutcomeKind::Failure if outcome.error.is_none() => Err(reject(
            "process_effect_runner: failure outcome must carry error".to_owned(),
        )),
        ProcessEffectOutcomeKind::Timeout if outcome.error.is_none() => Err(reject(
            "process_effect_runner: timeout outcome must carry error".to_owned(),
        )),
        ProcessEffectOutcomeKind::Result if outcome.error.is_some() || outcome.reason.is_some() => {
            Err(reject(
                "process_effect_runner: result outcome must not carry error or reason".to_owned(),
            ))
        }
        ProcessEffectOutcomeKind::Failure
            if outcome.value.is_some() || outcome.reason.is_some() =>
        {
            Err(reject(
                "process_effect_runner: failure outcome must not carry value or reason".to_owned(),
            ))
        }
        ProcessEffectOutcomeKind::Cancel if outcome.value.is_some() || outcome.error.is_some() => {
            Err(reject(
                "process_effect_runner: cancel outcome must not carry value or error".to_owned(),
            ))
        }
        ProcessEffectOutcomeKind::Timeout
            if outcome.value.is_some() || outcome.reason.is_some() =>
        {
            Err(reject(
                "process_effect_runner: timeout outcome must not carry value or reason".to_owned(),
            ))
        }
        _ => Ok(()),
    }
}

fn command_type_for_outcome(kind: ProcessEffectOutcomeKind) -> ProcessEffectCommandType {
    match kind {
        ProcessEffectOutcomeKind::Result => ProcessEffectCommandType::Result,
        ProcessEffectOutcomeKind::Failure => ProcessEffectCommandType::Failure,
        ProcessEffectOutcomeKind::Cancel => ProcessEffectCommandType::Cancel,
        ProcessEffectOutcomeKind::Timeout => ProcessEffectCommandType::Timeout,
    }
}

fn effect_runner_projection<TCommand, TResult, TOut>(
    graph: &Graph,
    runtime: &Node<ProcessEffectRunnerFact<TCommand, TResult>>,
    name: &str,
    factory: &'static str,
    select: impl Fn(&ProcessEffectRunnerFact<TCommand, TResult>) -> Option<TOut> + 'static,
) -> Node<TOut>
where
    TCommand: Clone + 'static,
    TResult: Clone + 'static,
    TOut: Clone + 'static,
{
    graph.init_node::<TOut>(
        Operator::with_opts(factory, no_terminal_opts(), move |ctx: &Ctx| {
            for fact in ctx.batch::<ProcessEffectRunnerFact<TCommand, TResult>>(0) {
                if let Some(selected) = select(fact.as_ref()) {
                    ctx.emit(selected);
                }
            }
        }),
        vec![runtime.erased()],
        effect_runner_meta_opts(name.to_owned(), "projection"),
    )
}

struct EffectRunnerNodeRefs<'a, TCommand, TEffect, TResult> {
    requests: &'a Node<ProcessEffectRequest<TEffect>>,
    outcomes: &'a Node<ProcessEffectOutcome<TResult>>,
    runtime: &'a Node<ProcessEffectRunnerFact<TCommand, TResult>>,
    commands: &'a Node<ProcessCommand<TCommand>>,
    status: &'a Node<ProcessEffectRunnerStatus>,
    errors: &'a Node<ProcessEffectRunnerError<TResult>>,
}

fn retain_effect_runner_nodes<TCommand, TEffect, TResult>(
    graph: &Graph,
    nodes: EffectRunnerNodeRefs<'_, TCommand, TEffect, TResult>,
    name: &str,
) -> Vec<ProcessRetain>
where
    TCommand: 'static,
    TEffect: 'static,
    TResult: 'static,
{
    vec![
        ProcessRetain::new(graph.retain(nodes.requests, &format!("{name}.effect_runner.requests"))),
        ProcessRetain::new(graph.retain(nodes.outcomes, &format!("{name}.effect_runner.outcomes"))),
        ProcessRetain::new(graph.retain(nodes.runtime, &format!("{name}.effect_runner.runtime"))),
        ProcessRetain::new(graph.retain(nodes.commands, &format!("{name}.effect_runner.commands"))),
        ProcessRetain::new(graph.retain(nodes.status, &format!("{name}.effect_runner.status"))),
        ProcessRetain::new(graph.retain(nodes.errors, &format!("{name}.effect_runner.errors"))),
    ]
}

fn attach_process_command_source_parts<TCommand>(
    command: &Node<ProcessCommand<TCommand>>,
    sources: &Rc<RefCell<Vec<Core>>>,
    source: Core,
) where
    TCommand: Clone + 'static,
{
    let previous = sources.borrow().clone();
    {
        let mut current = sources.borrow_mut();
        if !current.iter().any(|candidate| candidate.ptr_eq(&source)) {
            current.push(source.clone());
        }
    }
    let command_sources = sources.borrow().clone();
    let source_count = command_sources.len();
    let rewire = catch_unwind(AssertUnwindSafe(|| {
        command.replace_deps(
            command_sources,
            process_command_source_body::<TCommand>(source_count),
        );
    }));
    if let Err(panic) = rewire {
        *sources.borrow_mut() = previous.clone();
        command.replace_deps(
            previous,
            process_command_source_body::<TCommand>(sources.borrow().len()),
        );
        resume_unwind(panic);
    }
}

fn detach_process_command_source<TCommand>(
    command: &Node<ProcessCommand<TCommand>>,
    sources: &Rc<RefCell<Vec<Core>>>,
    source: Core,
) where
    TCommand: Clone + 'static,
{
    let previous = sources.borrow().clone();
    if !previous.iter().any(|candidate| candidate.ptr_eq(&source)) {
        return;
    }
    let next = previous
        .iter()
        .filter(|candidate| !candidate.ptr_eq(&source))
        .cloned()
        .collect::<Vec<_>>();
    *sources.borrow_mut() = next.clone();
    let rewire = catch_unwind(AssertUnwindSafe(|| {
        command.replace_deps(
            next,
            process_command_source_body::<TCommand>(sources.borrow().len()),
        );
    }));
    if let Err(panic) = rewire {
        *sources.borrow_mut() = previous.clone();
        command.replace_deps(
            previous,
            process_command_source_body::<TCommand>(sources.borrow().len()),
        );
        resume_unwind(panic);
    }
}

fn process_command_source_body<TCommand: Clone + 'static>(
    source_count: usize,
) -> impl Fn(&Ctx) + 'static {
    move |ctx: &Ctx| {
        for index in 0..source_count {
            for command in ctx.batch::<ProcessCommand<TCommand>>(index) {
                ctx.emit((*command).clone());
            }
        }
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
                compound_tuple_key(
                    "process-event",
                    &[
                        &command.id,
                        &(state.event_seq + index as u64 + 1).to_string(),
                    ],
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
                compound_tuple_key(
                    "process-effect",
                    &[
                        &command.id,
                        &(state.effect_seq + index as u64 + 1).to_string(),
                    ],
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
        partial: true,
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

fn effect_runner_meta_opts(name: String, role: &'static str) -> GraphNodeOpts {
    let mut opts = GraphNodeOpts::named(name);
    opts.meta
        .insert("process".to_owned(), format!("effect-runner:{role}"));
    opts.meta.insert("d".to_owned(), "D156".to_owned());
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
