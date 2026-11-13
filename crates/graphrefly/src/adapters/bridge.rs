//! Transport-free, graph-visible wire bridge bundle (D134/D140).
//!
//! The local wave ends at this bridge boundary. Commands become ordered outbound
//! envelope facts; remote receipts enter as later inbound DATA envelope facts.
//! Remote ERROR/COMPLETE are bridge facts, never local protocol terminals.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::rc::Rc;

use crate::ctx::{Ctx, DepTerminal};
use crate::graph::{Graph, GraphNodeOpts};
use crate::identity::canonical_tuple_key;
use crate::node::{Node, NodeOpts, Policy};
use crate::protocol::{AnyValue, Message};
use crate::resilience::RetryPolicy;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireBridgeEnvelopeType {
    Start,
    Data,
    Ack,
    Nack,
    Status,
    Error,
    Close,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireBridgeMetadata {
    /// Monotonic envelope sequence within the bridge session.
    pub seq: u64,
    /// Monotonic accepted inbound cursor observed by the sending side.
    pub cursor: u64,
    /// D151: correlation/idempotency metadata, not an authoritative duplicate lookup key.
    pub idempotency_key: String,
    pub attempt: u32,
    pub max_attempts: u32,
    pub timestamp_ms: Option<u64>,
    /// D151 ack/nack correlation target; receipt duplicate recognition still uses seq/cursor.
    pub ack_for_seq: Option<u64>,
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireBridgePayload<T> {
    Data(T),
    Error(String),
    Status(String),
    Close { reason: Option<String> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireBridgeEnvelope<T> {
    pub session_id: String,
    pub envelope_type: WireBridgeEnvelopeType,
    pub payload: Option<WireBridgePayload<T>>,
    pub metadata: WireBridgeMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireBridgeEnvelopeError {
    EmptySessionId,
    ZeroSeq,
    EmptyIdempotencyKey,
    ZeroAttempt,
    MaxAttemptsBeforeAttempt,
    ZeroAckForSeq,
    MissingAckForSeq,
    MissingPayload,
    UnexpectedPayload,
    PayloadTypeMismatch,
}

impl std::fmt::Display for WireBridgeEnvelopeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::EmptySessionId => "wireBridgeEnvelope: session_id must be non-empty",
            Self::ZeroSeq => "wireBridgeEnvelope: seq must be positive",
            Self::EmptyIdempotencyKey => "wireBridgeEnvelope: idempotency_key must be non-empty",
            Self::ZeroAttempt => "wireBridgeEnvelope: attempt must be positive",
            Self::MaxAttemptsBeforeAttempt => "wireBridgeEnvelope: max_attempts must be >= attempt",
            Self::ZeroAckForSeq => "wireBridgeEnvelope: ack_for_seq must be positive",
            Self::MissingAckForSeq => "wireBridgeEnvelope: ack/nack requires ack_for_seq",
            Self::MissingPayload => "wireBridgeEnvelope: envelope type requires a payload",
            Self::UnexpectedPayload => "wireBridgeEnvelope: envelope type must not carry a payload",
            Self::PayloadTypeMismatch => {
                "wireBridgeEnvelope: payload kind does not match envelope type"
            }
        })
    }
}

impl std::error::Error for WireBridgeEnvelopeError {}

#[derive(Debug, Clone)]
pub struct WireBridgeEnvelopeInput<T> {
    pub session_id: String,
    pub envelope_type: WireBridgeEnvelopeType,
    pub seq: u64,
    pub cursor: u64,
    pub payload: Option<WireBridgePayload<T>>,
    pub idempotency_key: Option<String>,
    pub attempt: u32,
    pub max_attempts: u32,
    pub timestamp_ms: Option<u64>,
    pub ack_for_seq: Option<u64>,
    pub request_id: Option<String>,
}

pub fn wire_bridge_idempotency_key(session_id: &str, seq: u64) -> String {
    canonical_tuple_key(&[session_id, &seq.to_string()])
}

pub fn wire_bridge_envelope<T>(
    input: WireBridgeEnvelopeInput<T>,
) -> Result<WireBridgeEnvelope<T>, WireBridgeEnvelopeError> {
    if input.session_id.is_empty() {
        return Err(WireBridgeEnvelopeError::EmptySessionId);
    }
    if input.seq == 0 {
        return Err(WireBridgeEnvelopeError::ZeroSeq);
    }
    if input.attempt == 0 {
        return Err(WireBridgeEnvelopeError::ZeroAttempt);
    }
    if input.max_attempts < input.attempt {
        return Err(WireBridgeEnvelopeError::MaxAttemptsBeforeAttempt);
    }
    if matches!(
        input.envelope_type,
        WireBridgeEnvelopeType::Ack | WireBridgeEnvelopeType::Nack
    ) && input.ack_for_seq.is_none()
    {
        return Err(WireBridgeEnvelopeError::MissingAckForSeq);
    }
    if input.ack_for_seq == Some(0) {
        return Err(WireBridgeEnvelopeError::ZeroAckForSeq);
    }
    validate_payload_for_type(input.envelope_type, &input.payload)?;
    let idempotency_key = input
        .idempotency_key
        .unwrap_or_else(|| wire_bridge_idempotency_key(&input.session_id, input.seq));
    if idempotency_key.is_empty() {
        return Err(WireBridgeEnvelopeError::EmptyIdempotencyKey);
    }
    Ok(WireBridgeEnvelope {
        session_id: input.session_id,
        envelope_type: input.envelope_type,
        payload: input.payload,
        metadata: WireBridgeMetadata {
            seq: input.seq,
            cursor: input.cursor,
            idempotency_key,
            attempt: input.attempt,
            max_attempts: input.max_attempts,
            timestamp_ms: input.timestamp_ms,
            ack_for_seq: input.ack_for_seq,
            request_id: input.request_id,
        },
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireBridgeCommand<T> {
    Start {
        idempotency_key: Option<String>,
        request_id: Option<String>,
    },
    Send {
        payload: T,
        idempotency_key: Option<String>,
        request_id: Option<String>,
    },
    Ack {
        ack_for_seq: u64,
        idempotency_key: Option<String>,
        request_id: Option<String>,
    },
    Nack {
        ack_for_seq: u64,
        error: String,
        idempotency_key: Option<String>,
        request_id: Option<String>,
    },
    Close {
        reason: Option<String>,
        idempotency_key: Option<String>,
    },
    AckTimeout {
        seq: u64,
        attempt: u32,
        observed_at_ms: Option<u64>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireBridgeReceipt {
    Ack,
    Nack,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireBridgeEvent<TOutbound, TInbound> {
    Outbound {
        envelope: WireBridgeEnvelope<TOutbound>,
    },
    Inbound {
        envelope: WireBridgeEnvelope<TInbound>,
    },
    Ack {
        ack_for_seq: u64,
        envelope: WireBridgeEnvelope<TInbound>,
        outbound: WireBridgeEnvelope<TOutbound>,
    },
    Nack {
        ack_for_seq: u64,
        envelope: WireBridgeEnvelope<TInbound>,
        outbound: WireBridgeEnvelope<TOutbound>,
        error: String,
    },
    Timeout {
        seq: u64,
        attempt: u32,
    },
    Retry {
        seq: u64,
        attempt: u32,
        delay_ms: u64,
        error: String,
    },
    Exhausted {
        seq: u64,
        attempt: u32,
        error: String,
    },
    Cursor {
        cursor: u64,
    },
    Duplicate {
        seq: u64,
        cursor: u64,
    },
    OutOfOrder {
        seq: u64,
        expected: u64,
    },
    SessionMismatch {
        expected: String,
        actual: String,
    },
    LateReceipt {
        receipt: WireBridgeReceipt,
        ack_for_seq: u64,
    },
    Invalid {
        error: String,
    },
}

/// One event of a wire bridge with its index: a number local to the bridge, from 1,
/// one more for each event the bridge emits, whatever its kind (review Q13). It is
/// not an envelope's seq.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireBridgeIndexedEvent<TOutbound, TInbound> {
    pub index: u64,
    pub event: WireBridgeEvent<TOutbound, TInbound>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireBridgeAck<TInbound> {
    pub ack_for_seq: u64,
    pub envelope: WireBridgeEnvelope<TInbound>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireBridgeNack<TInbound> {
    pub ack_for_seq: u64,
    pub envelope: WireBridgeEnvelope<TInbound>,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireBridgeAttempt {
    pub seq: u64,
    pub attempt: u32,
    pub max_attempts: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum WireBridgeStatusState {
    Idle,
    Started,
    Open,
    Waiting,
    Closed,
    Errored,
    Exhausted,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WireBridgeStatus {
    pub session_id: String,
    pub state: WireBridgeStatusState,
    pub cursor: u64,
    pub next_seq: u64,
    pub pending: u64,
    pub attempts: u64,
    pub acked: u64,
    pub nacked: u64,
    pub errors: u64,
    pub last_seq: Option<u64>,
    pub last_delay_ms: Option<u64>,
}

#[derive(Clone)]
pub struct WireBridgeOptions {
    pub name: Option<String>,
    pub session_id: String,
    pub retry: RetryPolicy,
    pub now_ms: Option<Rc<dyn Fn() -> u64>>,
}

impl WireBridgeOptions {
    pub fn new(session_id: impl Into<String>) -> Self {
        Self {
            name: None,
            session_id: session_id.into(),
            retry: RetryPolicy::default(),
            now_ms: None,
        }
    }

    pub fn named(session_id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            ..Self::new(session_id)
        }
    }
}

pub struct WireBridgeBundle<TOutbound: Clone + 'static, TInbound: Clone + 'static> {
    pub command: Node<WireBridgeCommand<TOutbound>>,
    pub outbound: Node<WireBridgeEnvelope<TOutbound>>,
    pub inbound: WireBridgeInbound<TInbound>,
    pub events: Node<WireBridgeIndexedEvent<TOutbound, TInbound>>,
    pub acks: Node<WireBridgeAck<TInbound>>,
    pub nacks: Node<WireBridgeNack<TInbound>>,
    pub status: Node<WireBridgeStatus>,
    pub errors: Node<String>,
    pub cursor: Node<u64>,
    pub attempts: Node<WireBridgeAttempt>,
}

impl<TOutbound: Clone + 'static, TInbound: Clone + 'static> WireBridgeBundle<TOutbound, TInbound> {
    pub fn start(&self) {
        self.command.set(WireBridgeCommand::Start {
            idempotency_key: None,
            request_id: None,
        });
    }

    pub fn send(
        &self,
        payload: TOutbound,
        idempotency_key: Option<String>,
        request_id: Option<String>,
    ) {
        self.command.set(WireBridgeCommand::Send {
            payload,
            idempotency_key,
            request_id,
        });
    }

    pub fn ack(
        &self,
        ack_for_seq: u64,
        idempotency_key: Option<String>,
        request_id: Option<String>,
    ) {
        self.command.set(WireBridgeCommand::Ack {
            ack_for_seq,
            idempotency_key,
            request_id,
        });
    }

    pub fn nack(
        &self,
        ack_for_seq: u64,
        error: impl Into<String>,
        idempotency_key: Option<String>,
        request_id: Option<String>,
    ) {
        self.command.set(WireBridgeCommand::Nack {
            ack_for_seq,
            error: error.into(),
            idempotency_key,
            request_id,
        });
    }

    pub fn close(&self, reason: Option<String>, idempotency_key: Option<String>) {
        self.command.set(WireBridgeCommand::Close {
            reason,
            idempotency_key,
        });
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteCallRequest<T> {
    pub operation: String,
    pub request_id: String,
    pub payload: T,
}

impl<T> RemoteCallRequest<T> {
    pub fn new(operation: impl Into<String>, request_id: impl Into<String>, payload: T) -> Self {
        Self {
            operation: operation.into(),
            request_id: request_id.into(),
            payload,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteCallResponse<T> {
    Result {
        operation: String,
        request_id: String,
        payload: T,
    },
    Error {
        operation: String,
        request_id: String,
        error: String,
    },
    Status {
        operation: String,
        request_id: String,
        status: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteCallResult<T> {
    pub operation: String,
    pub request_id: String,
    pub payload: T,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteCallError {
    pub operation: Option<String>,
    pub request_id: Option<String>,
    pub error: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RemoteCallStatusState {
    Idle,
    Requested,
    Responded,
    Errored,
    TimedOut,
    BridgeErrored,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RemoteCallStatus {
    pub state: RemoteCallStatusState,
    pub operation: Option<String>,
    pub request_id: Option<String>,
    pub pending: usize,
    pub completed: u64,
    pub errors: u64,
    pub timeouts: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteCallTimeout {
    pub operation: Option<String>,
    pub request_id: String,
    pub error: String,
}

#[derive(Clone)]
pub struct RemoteCallOptions {
    pub name: String,
}

impl Default for RemoteCallOptions {
    fn default() -> Self {
        Self {
            name: "remoteCall".to_owned(),
        }
    }
}

impl RemoteCallOptions {
    pub fn named(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

pub struct RemoteCallBundle<TRequest: Clone + 'static, TResponse: Clone + 'static> {
    bridge_command: Node<WireBridgeCommand<RemoteCallRequest<TRequest>>>,
    pub responses: Node<RemoteCallResponse<TResponse>>,
    pub results: Node<RemoteCallResult<TResponse>>,
    pub status: Node<RemoteCallStatus>,
    pub errors: Node<RemoteCallError>,
    pub timeouts: Node<RemoteCallTimeout>,
}

impl<TRequest: Clone + 'static, TResponse: Clone + 'static> RemoteCallBundle<TRequest, TResponse> {
    pub fn call(
        &self,
        operation: impl Into<String>,
        request_id: impl Into<String>,
        payload: TRequest,
    ) -> RemoteCallRequest<TRequest> {
        self.call_with_options(operation, request_id, payload, None)
    }

    pub fn call_with_options(
        &self,
        operation: impl Into<String>,
        request_id: impl Into<String>,
        payload: TRequest,
        idempotency_key: Option<String>,
    ) -> RemoteCallRequest<TRequest> {
        let request = RemoteCallRequest::new(operation, request_id, payload);
        assert!(
            !request.operation.is_empty(),
            "remote_call: operation must be non-empty"
        );
        assert!(
            !request.request_id.is_empty(),
            "remote_call: request_id must be non-empty"
        );
        self.bridge_command.set(WireBridgeCommand::Send {
            payload: request.clone(),
            idempotency_key,
            request_id: Some(request.request_id.clone()),
        });
        request
    }

    pub fn timeout(
        &self,
        request_id: impl Into<String>,
        operation: Option<String>,
        error: impl Into<String>,
    ) -> RemoteCallTimeout {
        let timeout = RemoteCallTimeout {
            operation,
            request_id: request_id.into(),
            error: error.into(),
        };
        assert!(
            !timeout.request_id.is_empty(),
            "remote_call: timeout request_id must be non-empty"
        );
        assert!(
            !timeout.error.is_empty(),
            "remote_call: timeout error must be non-empty"
        );
        self.timeouts.set(timeout.clone());
        timeout
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteResponderEvent<TRequest, TResponse> {
    Request {
        request: RemoteCallRequest<TRequest>,
        seq: u64,
    },
    Response {
        request_id: String,
        operation: String,
        command: WireBridgeCommand<RemoteCallResponse<TResponse>>,
    },
    Rejected {
        request_id: Option<String>,
        operation: Option<String>,
        error: String,
        command: Option<WireBridgeCommand<RemoteCallResponse<TResponse>>>,
    },
    Invalid {
        error: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RemoteResponderStatusState {
    Idle,
    Responded,
    Rejected,
    Errored,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RemoteResponderStatus {
    pub state: RemoteResponderStatusState,
    pub operation: Option<String>,
    pub request_id: Option<String>,
    pub handled: u64,
    pub rejected: u64,
    pub errors: u64,
}

pub type RemoteResponderHandler<TRequest, TResponse> =
    Rc<dyn Fn(&RemoteCallRequest<TRequest>) -> Result<TResponse, String>>;

#[derive(Clone)]
pub struct RemoteResponderHandlerDefinition<TRequest, TResponse> {
    pub operation: String,
    pub handle: RemoteResponderHandler<TRequest, TResponse>,
}

pub fn remote_responder_handler<TRequest, TResponse>(
    operation: impl Into<String>,
    handle: impl Fn(&RemoteCallRequest<TRequest>) -> Result<TResponse, String> + 'static,
) -> RemoteResponderHandlerDefinition<TRequest, TResponse> {
    let operation = operation.into();
    assert!(
        !operation.is_empty(),
        "remote_responder_handler: operation must be non-empty"
    );
    RemoteResponderHandlerDefinition {
        operation,
        handle: Rc::new(handle),
    }
}

#[derive(Clone)]
pub struct RemoteResponderOptions<TRequest, TResponse> {
    pub name: String,
    pub handlers: Vec<RemoteResponderHandlerDefinition<TRequest, TResponse>>,
    pub reject_unknown: bool,
}

impl<TRequest, TResponse> Default for RemoteResponderOptions<TRequest, TResponse> {
    fn default() -> Self {
        Self {
            name: "remoteResponder".to_owned(),
            handlers: Vec::new(),
            reject_unknown: false,
        }
    }
}

impl<TRequest, TResponse> RemoteResponderOptions<TRequest, TResponse> {
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Self::default()
        }
    }

    pub fn with_handlers(
        mut self,
        handlers: Vec<RemoteResponderHandlerDefinition<TRequest, TResponse>>,
    ) -> Self {
        self.handlers = handlers;
        self
    }

    pub fn with_reject_unknown(mut self, reject: bool) -> Self {
        self.reject_unknown = reject;
        self
    }
}

pub struct RemoteResponderBundle<TRequest: Clone + 'static, TResponse: Clone + 'static> {
    pub events: Node<RemoteResponderEvent<TRequest, TResponse>>,
    pub response_commands: Node<WireBridgeCommand<RemoteCallResponse<TResponse>>>,
    pub requests: Node<RemoteCallRequest<TRequest>>,
    pub status: Node<RemoteResponderStatus>,
    pub errors: Node<RemoteCallError>,
    graph: Graph,
    /// The sink that writes the response commands into the bridge's command state.
    command_writer: RefCell<Option<Box<dyn FnOnce()>>>,
    bridge_command: Node<WireBridgeCommand<RemoteCallResponse<TResponse>>>,
    released: Cell<bool>,
}

impl<TRequest: Clone + 'static, TResponse: Clone + 'static>
    RemoteResponderBundle<TRequest, TResponse>
{
    /// D157 graph-owned responder release: detach responseCommands before releasing nodes.
    pub fn release(&self) {
        if self.released.get() {
            return;
        }
        if let Some(stop) = self.command_writer.borrow_mut().take() {
            stop();
        }
        let release = catch_unwind(AssertUnwindSafe(|| {
            self.graph.release_nodes(
                &[
                    self.events.erased(),
                    self.response_commands.erased(),
                    self.requests.erased(),
                    self.status.erased(),
                    self.errors.erased(),
                ],
                "remote_responder release",
            );
        }));
        if let Err(panic) = release {
            *self.command_writer.borrow_mut() = Some(write_commands_into(
                &self.response_commands,
                &self.bridge_command,
            ));
            resume_unwind(panic);
        }
        self.released.set(true);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireBridgeIngress<T> {
    Envelope(WireBridgeEnvelope<T>),
    Invalid(String),
}

#[derive(Clone)]
pub struct WireBridgeInbound<T: Clone + 'static> {
    state: crate::node::State<WireBridgeIngress<T>>,
    session_id: String,
}

impl<T: Clone + 'static> WireBridgeInbound<T> {
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Writes one received envelope into the inbound state, as one DATA wave. This is
    /// the inbound's only host write (graphrefly-rs:D3 (2)): there is no raw array
    /// input.
    pub fn set(&self, envelope: WireBridgeEnvelope<T>) {
        self.state.set(WireBridgeIngress::Envelope(envelope));
    }

    /// Subscribes a sink, called once per array (R-wave).
    pub fn subscribe(&self, sink: impl Fn(&[Message<AnyValue>]) + 'static) -> Box<dyn FnOnce()> {
        self.state.subscribe(sink)
    }

    pub fn erased(&self) -> crate::node::Core {
        self.state.erased()
    }
}

#[cfg(test)]
fn data_msg<T: 'static>(value: T) -> Message<AnyValue> {
    Message::Data(Rc::new(value) as AnyValue)
}

struct PendingEnvelope<T> {
    envelope: WireBridgeEnvelope<T>,
    timeout_reported_attempt: Option<u32>,
    retry_due_at_ms: Option<u64>,
}

struct BridgeState<T> {
    active: bool,
    next_seq: u64,
    cursor: u64,
    remote_cursor: u64,
    /// The index the next event gets (review Q13).
    next_index: u64,
    pending: BTreeMap<u64, PendingEnvelope<T>>,
}

pub fn wire_bridge<TOutbound, TInbound>(
    graph: &Graph,
    opts: WireBridgeOptions,
) -> WireBridgeBundle<TOutbound, TInbound>
where
    TOutbound: Clone + 'static,
    TInbound: Clone + 'static,
{
    assert!(
        !opts.session_id.is_empty(),
        "wire_bridge: session_id must be non-empty"
    );
    let name = opts
        .name
        .clone()
        .unwrap_or_else(|| graph.module_name("wireBridge"));
    crate::graph::assert_local_name(&name);
    // The command and inbound inputs are states: the bundle's methods and the host
    // write commands and received envelopes into them, and a writer elsewhere does so
    // from a sink, as a request (graphrefly:D803).
    let command = graph
        .state_empty_opts::<WireBridgeCommand<TOutbound>>({
            let mut opts = GraphNodeOpts::named(format!("{name}/command"));
            opts.node.complete = Policy::Manual;
            opts.node.error = Policy::Manual;
            opts
        })
        .into_node();
    let inbound_state = graph.state_empty_opts::<WireBridgeIngress<TInbound>>(
        GraphNodeOpts::named(format!("{name}/inbound")),
    );
    let inbound_node = inbound_state.node().clone();
    let inbound = WireBridgeInbound {
        state: inbound_state,
        session_id: opts.session_id.clone(),
    };
    let events =
        wire_bridge_events_node(graph, &command, &inbound_node, name.clone(), opts.clone());
    // The event fold must keep receiving: the inbound state keeps only its last
    // envelope, so an inactive fold would miss what arrives meanwhile. It is held for
    // the graph's life (R-graph-surface@1 retain), so its private state keeps the
    // sequence and cursors for that long.
    let _receiving = graph.retain(&format!("{name}/events"));
    let outbound = project_outbound(graph, &events, &name);
    let acks = project_acks(graph, &events, &name);
    let nacks = project_nacks(graph, &events, &name);
    let status = project_status(graph, &events, &name, opts.session_id.clone());
    let errors = project_errors(graph, &events, &name);
    let cursor = project_cursor(graph, &events, &name);
    let attempts = project_attempts(graph, &events, &name);
    WireBridgeBundle {
        command,
        outbound,
        inbound,
        events,
        acks,
        nacks,
        status,
        errors,
        cursor,
        attempts,
    }
}

pub fn remote_call<TRequest, TResponse>(
    graph: &Graph,
    bridge: &WireBridgeBundle<RemoteCallRequest<TRequest>, RemoteCallResponse<TResponse>>,
) -> RemoteCallBundle<TRequest, TResponse>
where
    TRequest: Clone + 'static,
    TResponse: Clone + 'static,
{
    remote_call_with_options(graph, bridge, RemoteCallOptions::default())
}

pub fn remote_call_with_options<TRequest, TResponse>(
    graph: &Graph,
    bridge: &WireBridgeBundle<RemoteCallRequest<TRequest>, RemoteCallResponse<TResponse>>,
    opts: RemoteCallOptions,
) -> RemoteCallBundle<TRequest, TResponse>
where
    TRequest: Clone + 'static,
    TResponse: Clone + 'static,
{
    let name = opts.name;
    crate::graph::assert_local_name(&name);
    let timeouts = graph
        .state_empty_opts::<RemoteCallTimeout>(GraphNodeOpts::named(format!("{name}/timeouts")))
        .into_node();
    let responses =
        remote_call_responses_node(graph, &bridge.command, &bridge.events, &timeouts, &name);
    let results = remote_call_results_node(graph, &responses, &name);
    let status = remote_call_status_node(
        graph,
        &bridge.command,
        &bridge.events,
        &responses,
        &results,
        &timeouts,
        &name,
    );
    let errors = remote_call_errors_node(graph, &responses, &timeouts, &bridge.events, &name);
    RemoteCallBundle {
        bridge_command: bridge.command.clone(),
        responses,
        results,
        status,
        errors,
        timeouts,
    }
}

pub fn remote_responder<TRequest, TResponse>(
    graph: &Graph,
    bridge: &WireBridgeBundle<RemoteCallResponse<TResponse>, RemoteCallRequest<TRequest>>,
    opts: RemoteResponderOptions<TRequest, TResponse>,
) -> RemoteResponderBundle<TRequest, TResponse>
where
    TRequest: Clone + 'static,
    TResponse: Clone + 'static,
{
    let name = opts.name;
    crate::graph::assert_local_name(&name);
    let reject_unknown = opts.reject_unknown;
    let handlers = Rc::new(normalize_remote_handlers(opts.handlers));
    let events = remote_responder_events_node(graph, bridge, &name, handlers, reject_unknown);
    let response_commands = remote_responder_response_commands_node(graph, &events, &name);
    let requests = remote_responder_requests_node(graph, &events, &name);
    let status = remote_responder_status_node(graph, &events, &name);
    let errors = remote_responder_errors_node(graph, &events, &name);
    // The response commands reach the bridge as writes of its command state, from a
    // sink (graphrefly:D803): no edge, no host-level rewire.
    let command_writer = write_commands_into(&response_commands, &bridge.command);
    RemoteResponderBundle {
        events,
        response_commands,
        requests,
        status,
        errors,
        graph: graph.clone(),
        command_writer: RefCell::new(Some(command_writer)),
        bridge_command: bridge.command.clone(),
        released: Cell::new(false),
    }
}

/// Subscribes a sink that writes each command `source` sends into the `command`
/// state; returns its unsubscribe.
fn write_commands_into<T: Clone + 'static>(
    source: &Node<WireBridgeCommand<T>>,
    command: &Node<WireBridgeCommand<T>>,
) -> Box<dyn FnOnce()> {
    let command = command.clone();
    source.subscribe(move |msgs: &[Message<AnyValue>]| {
        for msg in msgs {
            if let Message::Data(value) = msg {
                if let Some(cmd) = value.downcast_ref::<WireBridgeCommand<T>>() {
                    command.set(cmd.clone());
                }
            }
        }
    })
}

fn normalize_remote_handlers<TRequest, TResponse>(
    handlers: Vec<RemoteResponderHandlerDefinition<TRequest, TResponse>>,
) -> HashMap<String, RemoteResponderHandler<TRequest, TResponse>> {
    let mut out = HashMap::new();
    for handler in handlers {
        assert!(
            out.insert(handler.operation.clone(), handler.handle)
                .is_none(),
            "remote_responder: duplicate operation '{}'",
            handler.operation
        );
    }
    out
}

fn remote_call_responses_node<TRequest, TResponse>(
    graph: &Graph,
    commands: &Node<WireBridgeCommand<RemoteCallRequest<TRequest>>>,
    events: &Node<
        WireBridgeIndexedEvent<RemoteCallRequest<TRequest>, RemoteCallResponse<TResponse>>,
    >,
    timeouts: &Node<RemoteCallTimeout>,
    name: &str,
) -> Node<RemoteCallResponse<TResponse>>
where
    TRequest: Clone + 'static,
    TResponse: Clone + 'static,
{
    // The pending-request correlation: a ledger the bundle owns (review Q13).
    let ledger = crate::ledger::Ledger::new(
        graph,
        format!("{name}/responsesLedger"),
        serde_json::Value::Null,
    );
    graph.node_opts::<RemoteCallResponse<TResponse>, _>(
        vec![
            commands.erased(),
            events.erased(),
            timeouts.erased(),
            ledger.erased(),
        ],
        |ctx| {
            let mut working: RemoteCallResponsesState = crate::ledger::load(ctx, ctx.dep_len() - 1);
            let mut ready = Vec::new();
            {
                let state = &mut working;
                let preview_requests = ctx
                    .dep(0)
                    .values::<WireBridgeCommand<RemoteCallRequest<TRequest>>>()
                    .into_iter()
                    .filter_map(|command| pending_request_from_command(command.as_ref()))
                    .collect::<Vec<_>>();
                let has_invalid_bridge_event = ctx
                    .dep(1)
                    .values::<WireBridgeIndexedEvent<
                        RemoteCallRequest<TRequest>,
                        RemoteCallResponse<TResponse>,
                    >>()
                    .into_iter()
                    .any(|event| {
                        matches!(
                            &event.event,
                            WireBridgeEvent::Invalid { .. }
                                | WireBridgeEvent::SessionMismatch { .. }
                                | WireBridgeEvent::OutOfOrder { .. }
                                | WireBridgeEvent::LateReceipt { .. }
                        )
                    });
                if !has_invalid_bridge_event {
                    for request in preview_requests.iter().cloned() {
                        let _ = state.pending.insert(request);
                    }
                }
                for event in ctx.dep(1).values::<WireBridgeIndexedEvent<
                    RemoteCallRequest<TRequest>,
                    RemoteCallResponse<TResponse>,
                >>() {
                    if let WireBridgeEvent::Outbound { envelope } = &event.event {
                        if let Some(request) = pending_request_from_envelope(envelope) {
                            let _ = state.pending.insert(request);
                        }
                    }
                }
                for event in ctx.dep(1).values::<WireBridgeIndexedEvent<
                    RemoteCallRequest<TRequest>,
                    RemoteCallResponse<TResponse>,
                >>() {
                    match &event.event {
                        WireBridgeEvent::Invalid { .. } => {
                            state.pending.remove_all_unbound();
                        }
                        WireBridgeEvent::Inbound { envelope } => {
                            if let Some(WireBridgePayload::Data(response)) = &envelope.payload {
                                let request_id = remote_call_response_request_id(response);
                                if let Some(request) = state.pending.get_by_request_id(request_id) {
                                    if !remote_call_response_matches_pending(response, request) {
                                        continue;
                                    }
                                    if remote_call_response_is_terminal(response) {
                                        state.pending.remove_by_request_id(request_id);
                                    }
                                    ready.push(response.clone());
                                }
                            }
                        }
                        WireBridgeEvent::Nack { outbound, .. } => {
                            let request = state
                                .pending
                                .remove_by_seq(outbound.metadata.seq)
                                .or_else(|| pending_request_from_envelope(outbound));
                            if let Some(request) = request {
                                let _ = request;
                            }
                        }
                        WireBridgeEvent::Exhausted { seq, .. } => {
                            state.pending.remove_by_seq(*seq);
                        }
                        _ => {}
                    }
                }
                for timeout in ctx.dep(2).values::<RemoteCallTimeout>() {
                    state.pending.remove_by_request_id(&timeout.request_id);
                }
            }
            crate::ledger::store(ctx, &working);
            let mut emitted = Vec::new();
            for response in ready {
                emitted.push(response);
            }
            ctx.down(crate::protocol::data_wave(emitted));
        },
        {
            let mut opts = no_terminal_graph_opts(format!("{name}/responses"));
            opts.node = crate::ledger::write_back_json(opts.node);
            opts
        },
    )
}

#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
struct RemoteCallResponsesState {
    pending: RemoteCallPendingState,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct RemoteCallPendingRequest {
    operation: String,
    request_id: String,
    seq: Option<u64>,
}

#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
struct RemoteCallPendingState {
    request_ids: HashSet<String>,
    by_request_id: HashMap<String, RemoteCallPendingRequest>,
    by_seq: HashMap<u64, RemoteCallPendingRequest>,
}

impl RemoteCallPendingState {
    fn get_by_request_id(&self, request_id: &str) -> Option<&RemoteCallPendingRequest> {
        self.by_request_id.get(request_id)
    }

    fn insert(&mut self, request: RemoteCallPendingRequest) -> bool {
        if let Some(existing) = self.by_request_id.get_mut(&request.request_id) {
            if existing.operation == request.operation && existing.seq.is_none() {
                if let Some(seq) = request.seq {
                    if self.by_seq.contains_key(&seq) {
                        return false;
                    }
                    existing.seq = Some(seq);
                    self.by_seq.insert(seq, existing.clone());
                    return true;
                }
            }
            return false;
        }
        if request
            .seq
            .is_some_and(|seq| self.by_seq.contains_key(&seq))
        {
            return false;
        }
        self.request_ids.insert(request.request_id.clone());
        self.by_request_id
            .insert(request.request_id.clone(), request.clone());
        if let Some(seq) = request.seq {
            self.by_seq.insert(seq, request);
        }
        true
    }

    fn remove_by_request_id(&mut self, request_id: &str) -> Option<RemoteCallPendingRequest> {
        if !self.request_ids.remove(request_id) {
            return None;
        }
        let request = self.by_request_id.remove(request_id)?;
        if let Some(seq) = request.seq {
            self.by_seq.remove(&seq);
        }
        Some(request)
    }

    fn remove_by_seq(&mut self, seq: u64) -> Option<RemoteCallPendingRequest> {
        let request = self.by_seq.remove(&seq)?;
        self.request_ids.remove(&request.request_id);
        self.by_request_id.remove(&request.request_id);
        Some(request)
    }

    fn remove_all_unbound(&mut self) {
        let request_ids = self
            .by_request_id
            .values()
            .filter_map(|request| request.seq.is_none().then_some(request.request_id.clone()))
            .collect::<Vec<_>>();
        for request_id in request_ids {
            self.request_ids.remove(&request_id);
            self.by_request_id.remove(&request_id);
        }
    }

    fn len(&self) -> usize {
        self.request_ids.len()
    }
}

fn pending_request_from_envelope<T>(
    envelope: &WireBridgeEnvelope<RemoteCallRequest<T>>,
) -> Option<RemoteCallPendingRequest> {
    if let Some(WireBridgePayload::Data(request)) = &envelope.payload {
        Some(RemoteCallPendingRequest {
            operation: request.operation.clone(),
            request_id: request.request_id.clone(),
            seq: Some(envelope.metadata.seq),
        })
    } else {
        None
    }
}

fn pending_request_from_command<T>(
    command: &WireBridgeCommand<RemoteCallRequest<T>>,
) -> Option<RemoteCallPendingRequest> {
    if let WireBridgeCommand::Send { payload, .. } = command {
        Some(RemoteCallPendingRequest {
            operation: payload.operation.clone(),
            request_id: payload.request_id.clone(),
            seq: None,
        })
    } else {
        None
    }
}

fn remote_call_response_request_id<T>(response: &RemoteCallResponse<T>) -> &str {
    match response {
        RemoteCallResponse::Result { request_id, .. }
        | RemoteCallResponse::Error { request_id, .. }
        | RemoteCallResponse::Status { request_id, .. } => request_id,
    }
}

fn remote_call_response_is_terminal<T>(response: &RemoteCallResponse<T>) -> bool {
    matches!(
        response,
        RemoteCallResponse::Result { .. } | RemoteCallResponse::Error { .. }
    )
}

fn remote_call_response_operation<T>(response: &RemoteCallResponse<T>) -> &str {
    match response {
        RemoteCallResponse::Result { operation, .. }
        | RemoteCallResponse::Error { operation, .. }
        | RemoteCallResponse::Status { operation, .. } => operation,
    }
}

fn remote_call_response_matches_pending<T>(
    response: &RemoteCallResponse<T>,
    request: &RemoteCallPendingRequest,
) -> bool {
    remote_call_response_operation(response) == request.operation
}

fn remote_call_response_key<T>(response: &RemoteCallResponse<T>) -> String {
    format!(
        "{}\u{0}{}",
        remote_call_response_operation(response),
        remote_call_response_request_id(response)
    )
}

fn remote_call_request_key(request: &RemoteCallPendingRequest) -> String {
    format!("{}\u{0}{}", request.operation, request.request_id)
}

#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
struct RemoteResponderCursor {
    cursor: u64,
    remote_cursor: u64,
}

enum RemoteResponderInbound<T> {
    Request {
        request: RemoteCallRequest<T>,
        seq: u64,
    },
    Consumed,
    Invalid {
        error: String,
    },
}

fn reduce_remote_responder_ingress<T>(
    cursor: &mut RemoteResponderCursor,
    ingress: WireBridgeIngress<RemoteCallRequest<T>>,
    session_id: &str,
) -> RemoteResponderInbound<T> {
    let envelope = match ingress {
        WireBridgeIngress::Envelope(envelope) => envelope,
        WireBridgeIngress::Invalid(error) => return RemoteResponderInbound::Invalid { error },
    };
    if let Err(error) = validate_inbound_envelope(&envelope) {
        return RemoteResponderInbound::Invalid {
            error: error.to_string(),
        };
    }
    if envelope.session_id != session_id {
        return RemoteResponderInbound::Invalid {
            error: format!(
                "{session_id}: remoteResponder session mismatch: {}",
                envelope.session_id
            ),
        };
    }
    let seq = envelope.metadata.seq;
    let expected = cursor.cursor.saturating_add(1);
    if seq <= cursor.cursor {
        return RemoteResponderInbound::Invalid {
            error: format!(
                "remoteResponder: duplicate request seq {seq} at cursor {}",
                cursor.cursor
            ),
        };
    }
    if seq > expected {
        return RemoteResponderInbound::Invalid {
            error: format!("remoteResponder: out-of-order request seq {seq}, expected {expected}"),
        };
    }
    if envelope.metadata.cursor < cursor.remote_cursor {
        return RemoteResponderInbound::Invalid {
            error: format!(
                "{session_id}: remoteResponder inbound cursor {} regressed below {}",
                envelope.metadata.cursor, cursor.remote_cursor
            ),
        };
    }
    cursor.cursor = seq;
    cursor.remote_cursor = envelope.metadata.cursor;
    if envelope.envelope_type != WireBridgeEnvelopeType::Data {
        return RemoteResponderInbound::Consumed;
    }
    match envelope.payload {
        Some(WireBridgePayload::Data(request)) => RemoteResponderInbound::Request { request, seq },
        _ => RemoteResponderInbound::Invalid {
            error: "remoteResponder: request envelope must carry request DATA".to_owned(),
        },
    }
}

fn remote_call_results_node<TResponse>(
    graph: &Graph,
    responses: &Node<RemoteCallResponse<TResponse>>,
    name: &str,
) -> Node<RemoteCallResult<TResponse>>
where
    TResponse: Clone + 'static,
{
    graph.node_opts::<RemoteCallResult<TResponse>, _>(
        vec![responses.erased()],
        |ctx| {
            let mut emitted = Vec::new();
            for response in ctx.dep(0).values::<RemoteCallResponse<TResponse>>() {
                if let RemoteCallResponse::Result {
                    operation,
                    request_id,
                    payload,
                } = response.as_ref()
                {
                    emitted.push(RemoteCallResult {
                        operation: operation.clone(),
                        request_id: request_id.clone(),
                        payload: payload.clone(),
                    });
                }
            }
            ctx.down(crate::protocol::data_wave(emitted));
        },
        no_terminal_graph_opts(format!("{name}/results")),
    )
}

fn remote_call_status_node<TRequest, TResponse>(
    graph: &Graph,
    commands: &Node<WireBridgeCommand<RemoteCallRequest<TRequest>>>,
    events: &Node<
        WireBridgeIndexedEvent<RemoteCallRequest<TRequest>, RemoteCallResponse<TResponse>>,
    >,
    responses: &Node<RemoteCallResponse<TResponse>>,
    results: &Node<RemoteCallResult<TResponse>>,
    timeouts: &Node<RemoteCallTimeout>,
    name: &str,
) -> Node<RemoteCallStatus>
where
    TRequest: Clone + 'static,
    TResponse: Clone + 'static,
{
    // Correlation, dedupe and audit state: a ledger the bundle owns (review Q13).
    let ledger = crate::ledger::Ledger::new(
        graph,
        format!("{name}/statusLedger"),
        serde_json::Value::Null,
    );
    graph.node_opts::<RemoteCallStatus, _>(
        vec![
            commands.erased(),
            events.erased(),
            responses.erased(),
            results.erased(),
            timeouts.erased(),
            ledger.erased(),
        ],
        |ctx| {
            let mut reducer =
                crate::ledger::load::<RemoteCallStatusReducer>(ctx, ctx.dep_len() - 1);
            for command in ctx
                .dep(0)
                .values::<WireBridgeCommand<RemoteCallRequest<TRequest>>>()
            {
                if let Some(request) = pending_request_from_command(command.as_ref()) {
                    reducer.status.state = RemoteCallStatusState::Requested;
                    reducer.status.operation = Some(request.operation.clone());
                    reducer.status.request_id = Some(request.request_id.clone());
                    if !reducer
                        .terminal_before_request
                        .contains(&remote_call_request_key(&request))
                    {
                        let _ = reducer.pending.insert(request);
                    }
                }
            }
            for event in
                ctx.dep(1).values::<WireBridgeIndexedEvent<
                    RemoteCallRequest<TRequest>,
                    RemoteCallResponse<TResponse>,
                >>()
            {
                if let WireBridgeEvent::Outbound { envelope } = &event.event {
                    if let Some(request) = pending_request_from_envelope(envelope) {
                        reducer.status.state = RemoteCallStatusState::Requested;
                        reducer.status.operation = Some(request.operation.clone());
                        reducer.status.request_id = Some(request.request_id.clone());
                        if reducer
                            .terminal_before_request
                            .remove(&remote_call_request_key(&request))
                        {
                            continue;
                        }
                        if !reducer.pending.insert(request) {
                            reducer.status.state = RemoteCallStatusState::Errored;
                            reducer.status.errors = reducer.status.errors.saturating_add(1);
                        }
                    }
                }
            }
            for event in
                ctx.dep(1).values::<WireBridgeIndexedEvent<
                    RemoteCallRequest<TRequest>,
                    RemoteCallResponse<TResponse>,
                >>()
            {
                match &event.event {
                    WireBridgeEvent::Invalid { .. } => {
                        reducer.pending.remove_all_unbound();
                        reducer.status.state = RemoteCallStatusState::BridgeErrored;
                        reducer.status.errors = reducer.status.errors.saturating_add(1);
                    }
                    WireBridgeEvent::SessionMismatch { .. }
                    | WireBridgeEvent::OutOfOrder { .. }
                    | WireBridgeEvent::LateReceipt { .. } => {
                        reducer.status.state = RemoteCallStatusState::BridgeErrored;
                        reducer.status.errors = reducer.status.errors.saturating_add(1);
                    }
                    WireBridgeEvent::Nack {
                        outbound, error, ..
                    } => {
                        let request = reducer
                            .pending
                            .remove_by_seq(outbound.metadata.seq)
                            .or_else(|| pending_request_from_envelope(outbound));
                        reducer.status.state = RemoteCallStatusState::BridgeErrored;
                        if let Some(request) = request {
                            reducer.status.operation = Some(request.operation);
                            reducer.status.request_id = Some(request.request_id);
                        }
                        if !error.is_empty() {
                            reducer.status.errors = reducer.status.errors.saturating_add(1);
                        }
                    }
                    WireBridgeEvent::Exhausted { seq, error, .. } => {
                        let request = reducer.pending.remove_by_seq(*seq);
                        reducer.status.state = RemoteCallStatusState::BridgeErrored;
                        if let Some(request) = request {
                            reducer.status.operation = Some(request.operation);
                            reducer.status.request_id = Some(request.request_id);
                        }
                        if !error.is_empty() {
                            reducer.status.errors = reducer.status.errors.saturating_add(1);
                        }
                    }
                    WireBridgeEvent::Inbound { envelope } => {
                        if let Some(WireBridgePayload::Data(response)) = &envelope.payload {
                            let request_id = remote_call_response_request_id(response);
                            if let Some(request) = reducer.pending.get_by_request_id(request_id) {
                                if !remote_call_response_matches_pending(response, request) {
                                    reducer.status.state = RemoteCallStatusState::Errored;
                                    reducer.status.operation = Some(request.operation.clone());
                                    reducer.status.request_id = Some(request.request_id.clone());
                                    reducer.status.errors = reducer.status.errors.saturating_add(1);
                                    continue;
                                }
                            } else {
                                reducer.status.state = RemoteCallStatusState::Errored;
                                reducer.status.operation =
                                    Some(remote_call_response_operation(response).to_owned());
                                reducer.status.request_id = Some(request_id.to_owned());
                                reducer.status.errors = reducer.status.errors.saturating_add(1);
                                continue;
                            }
                            match response {
                                RemoteCallResponse::Result { .. } => {
                                    // Terminal status is projected from the accepted responses dep below.
                                }
                                RemoteCallResponse::Error { .. } => {
                                    // Terminal status is projected from the accepted responses dep below.
                                }
                                RemoteCallResponse::Status {
                                    operation,
                                    request_id,
                                    ..
                                } => {
                                    reducer.status.operation = Some(operation.clone());
                                    reducer.status.request_id = Some(request_id.clone());
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            for response in ctx.dep(2).values::<RemoteCallResponse<TResponse>>() {
                match response.as_ref() {
                    RemoteCallResponse::Result { .. } => {}
                    RemoteCallResponse::Error {
                        operation,
                        request_id,
                        ..
                    } => {
                        let removed = reducer.pending.remove_by_request_id(request_id);
                        if removed.as_ref().is_none_or(|request| request.seq.is_none()) {
                            reducer
                                .terminal_before_request
                                .insert(format!("{}\u{0}{}", operation, request_id));
                        }
                        reducer.status.state = RemoteCallStatusState::Errored;
                        reducer.status.operation = Some(operation.clone());
                        reducer.status.request_id = Some(request_id.clone());
                        reducer.status.errors = reducer.status.errors.saturating_add(1);
                    }
                    RemoteCallResponse::Status {
                        operation,
                        request_id,
                        ..
                    } => {
                        if reducer.pending.get_by_request_id(request_id).is_some() {
                            reducer.status.operation = Some(operation.clone());
                            reducer.status.request_id = Some(request_id.clone());
                        }
                    }
                }
            }
            for result in ctx.dep(3).values::<RemoteCallResult<TResponse>>() {
                let removed = reducer.pending.remove_by_request_id(&result.request_id);
                if removed.as_ref().is_none_or(|request| request.seq.is_none()) {
                    reducer
                        .terminal_before_request
                        .insert(format!("{}\u{0}{}", result.operation, result.request_id));
                }
                reducer.status.state = RemoteCallStatusState::Responded;
                reducer.status.operation = Some(result.operation.clone());
                reducer.status.request_id = Some(result.request_id.clone());
                reducer.status.completed = reducer.status.completed.saturating_add(1);
            }
            for timeout in ctx.dep(4).values::<RemoteCallTimeout>() {
                reducer.status.state = RemoteCallStatusState::TimedOut;
                reducer.status.operation = timeout.operation.clone();
                reducer.status.request_id = Some(timeout.request_id.clone());
                reducer.pending.remove_by_request_id(&timeout.request_id);
                reducer.status.errors = reducer.status.errors.saturating_add(1);
                reducer.status.timeouts = reducer.status.timeouts.saturating_add(1);
            }
            reducer.status.pending = reducer.pending.len();
            crate::ledger::store(ctx, &reducer);
            ctx.emit(reducer.status);
        },
        {
            let mut opts = no_terminal_graph_opts(format!("{name}/status"));
            opts.node = crate::ledger::write_back_json(opts.node);
            opts
        },
    )
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct RemoteCallStatusReducer {
    status: RemoteCallStatus,
    pending: RemoteCallPendingState,
    terminal_before_request: HashSet<String>,
}

impl Default for RemoteCallStatusReducer {
    fn default() -> Self {
        Self {
            status: initial_remote_call_status(),
            pending: RemoteCallPendingState::default(),
            terminal_before_request: HashSet::new(),
        }
    }
}

fn initial_remote_call_status() -> RemoteCallStatus {
    RemoteCallStatus {
        state: RemoteCallStatusState::Idle,
        operation: None,
        request_id: None,
        pending: 0,
        completed: 0,
        errors: 0,
        timeouts: 0,
    }
}

fn remote_call_errors_node<TRequest, TResponse>(
    graph: &Graph,
    responses: &Node<RemoteCallResponse<TResponse>>,
    timeouts: &Node<RemoteCallTimeout>,
    events: &Node<
        WireBridgeIndexedEvent<RemoteCallRequest<TRequest>, RemoteCallResponse<TResponse>>,
    >,
    name: &str,
) -> Node<RemoteCallError>
where
    TRequest: Clone + 'static,
    TResponse: Clone + 'static,
{
    // Correlation, dedupe and audit state: a ledger the bundle owns (review Q13).
    let ledger = crate::ledger::Ledger::new(
        graph,
        format!("{name}/errorsLedger"),
        serde_json::Value::Null,
    );
    graph.node_opts::<RemoteCallError, _>(
        vec![responses.erased(), timeouts.erased(), events.erased(), ledger.erased()],
        |ctx| {
            let mut pending = crate::ledger::load::<RemoteCallPendingState>(ctx, ctx.dep_len() - 1);
            for event in ctx.dep(2).values::<
                WireBridgeIndexedEvent<RemoteCallRequest<TRequest>, RemoteCallResponse<TResponse>>,
            >() {
                if let WireBridgeEvent::Outbound { envelope } = &event.event {
                    if let Some(request) = pending_request_from_envelope(envelope) {
                        if !pending.insert(request.clone()) {
                            ctx.emit(RemoteCallError {
                                operation: Some(request.operation),
                                request_id: Some(request.request_id.clone()),
                                error: format!(
                                    "remote_call: duplicate in-flight request_id '{}'",
                                    request.request_id
                                ),
                            });
                        }
                    }
                }
            }
            let mut accepted_response_counts = HashMap::new();
            let mut emitted = Vec::new();
            for response in ctx.dep(0).values::<RemoteCallResponse<TResponse>>() {
                *accepted_response_counts
                    .entry(remote_call_response_key(response.as_ref()))
                    .or_insert(0usize) += 1;
                match response.as_ref() {
                    RemoteCallResponse::Error {
                        operation,
                        request_id,
                        error,
                    } => {
                        pending.remove_by_request_id(request_id);
                        emitted.push(RemoteCallError {
                            operation: Some(operation.clone()),
                            request_id: Some(request_id.clone()),
                            error: error.clone(),
                        });
                    }
                    RemoteCallResponse::Result { request_id, .. } => {
                        pending.remove_by_request_id(request_id);
                    }
                    RemoteCallResponse::Status { .. } => {}
                }
            }
            ctx.down(crate::protocol::data_wave(emitted));
            let mut emitted = Vec::new();
            for timeout in ctx.dep(1).values::<RemoteCallTimeout>() {
                pending.remove_by_request_id(&timeout.request_id);
                emitted.push(RemoteCallError {
                    operation: timeout.operation.clone(),
                    request_id: Some(timeout.request_id.clone()),
                    error: timeout.error.clone(),
                });
            }
            ctx.down(crate::protocol::data_wave(emitted));
            for event in ctx.dep(2).values::<
                WireBridgeIndexedEvent<RemoteCallRequest<TRequest>, RemoteCallResponse<TResponse>>,
            >() {
                match &event.event {
                    WireBridgeEvent::Nack {
                        outbound, error, ..
                    } => {
                        let request = pending
                            .remove_by_seq(outbound.metadata.seq)
                            .or_else(|| pending_request_from_envelope(outbound));
                        ctx.emit(RemoteCallError {
                            operation: request.as_ref().map(|request| request.operation.clone()),
                            request_id: request.as_ref().map(|request| request.request_id.clone()),
                            error: error.clone(),
                        });
                    }
                    WireBridgeEvent::Exhausted { seq, error, .. } => {
                        let request = pending.remove_by_seq(*seq);
                        ctx.emit(RemoteCallError {
                            operation: request.as_ref().map(|request| request.operation.clone()),
                            request_id: request.as_ref().map(|request| request.request_id.clone()),
                            error: error.clone(),
                        });
                    }
                    WireBridgeEvent::Invalid { error } => ctx.emit(RemoteCallError {
                        operation: None,
                        request_id: None,
                        error: error.clone(),
                    }),
                    WireBridgeEvent::Inbound { envelope } => {
                        if let Some(WireBridgePayload::Data(response)) = &envelope.payload {
                            let response_key = remote_call_response_key(response);
                            if let Some(count) = accepted_response_counts.get_mut(&response_key) {
                                if *count > 0 {
                                    *count -= 1;
                                    continue;
                                }
                            }
                            let request_id = remote_call_response_request_id(response);
                            if let Some(request) = pending.get_by_request_id(request_id) {
                                if !remote_call_response_matches_pending(response, request) {
                                    ctx.emit(RemoteCallError {
                                        operation: Some(request.operation.clone()),
                                        request_id: Some(request.request_id.clone()),
                                        error: format!(
                                            "remote_call: response operation '{}' did not match pending operation '{}'",
                                            remote_call_response_operation(response),
                                            request.operation
                                        ),
                                    });
                                }
                            } else {
                                ctx.emit(RemoteCallError {
                                    operation: Some(remote_call_response_operation(response).to_owned()),
                                    request_id: Some(request_id.to_owned()),
                                    error:
                                        "remote_call: orphan response for unknown or completed request"
                                            .to_owned(),
                                });
                            }
                        }
                    }
                    WireBridgeEvent::SessionMismatch { .. }
                    | WireBridgeEvent::OutOfOrder { .. }
                    | WireBridgeEvent::LateReceipt { .. } => ctx.emit(RemoteCallError {
                        operation: None,
                        request_id: None,
                        error: remote_call_bridge_error_message(&event.event),
                    }),
                    _ => {}
                }
            }
            crate::ledger::store(ctx, &pending);
        },
        {
            let mut opts = no_terminal_graph_opts(format!("{name}/errors"));
            opts.node = crate::ledger::write_back_json(opts.node);
            opts
        },
    )
}

fn remote_call_bridge_error_message<TRequest, TResponse>(
    event: &WireBridgeEvent<RemoteCallRequest<TRequest>, RemoteCallResponse<TResponse>>,
) -> String {
    match event {
        WireBridgeEvent::SessionMismatch { expected, actual } => {
            format!("{expected}: session mismatch: {actual}")
        }
        WireBridgeEvent::OutOfOrder { seq, expected } => {
            format!("wireBridge: out-of-order seq {seq}, expected {expected}")
        }
        WireBridgeEvent::LateReceipt {
            receipt,
            ack_for_seq,
        } => format!("wireBridge: late {receipt:?} for seq {ack_for_seq}"),
        WireBridgeEvent::Invalid { error } => error.clone(),
        _ => "wireBridge: bridge error".to_owned(),
    }
}

fn remote_responder_events_node<TRequest, TResponse>(
    graph: &Graph,
    bridge: &WireBridgeBundle<RemoteCallResponse<TResponse>, RemoteCallRequest<TRequest>>,
    name: &str,
    handlers: Rc<HashMap<String, RemoteResponderHandler<TRequest, TResponse>>>,
    reject_unknown: bool,
) -> Node<RemoteResponderEvent<TRequest, TResponse>>
where
    TRequest: Clone + 'static,
    TResponse: Clone + 'static,
{
    let session_id = bridge.inbound.session_id().to_owned();
    // Correlation, dedupe and audit state: a ledger the bundle owns (review Q13).
    let ledger = crate::ledger::Ledger::new(
        graph,
        format!("{name}/cursorLedger"),
        serde_json::Value::Null,
    );
    graph.node_opts::<RemoteResponderEvent<TRequest, TResponse>, _>(
        vec![bridge.inbound.erased(), ledger.erased()],
        move |ctx| {
            let mut cursor = crate::ledger::load::<RemoteResponderCursor>(ctx, ctx.dep_len() - 1);
            let mut emitted = Vec::new();
            for value in raw_data(ctx, 0) {
                let ingress = match value.downcast::<WireBridgeIngress<RemoteCallRequest<TRequest>>>()
                {
                    Ok(ingress) => (*ingress).clone(),
                    Err(_) => {
                        emitted.push(RemoteResponderEvent::<TRequest, TResponse>::Invalid {
                            error: "remoteResponder: inbound DATA must carry a wire bridge ingress fact"
                                .to_owned(),
                        });
                        continue;
                    }
                };
                let (request, seq) =
                    match reduce_remote_responder_ingress(&mut cursor, ingress, &session_id) {
                        RemoteResponderInbound::Request { request, seq } => (request, seq),
                        RemoteResponderInbound::Consumed => continue,
                        RemoteResponderInbound::Invalid { error } => {
                            emitted.push(RemoteResponderEvent::<TRequest, TResponse>::Invalid {
                                error,
                            });
                            continue;
                        }
                    };
                emitted.push(RemoteResponderEvent::<TRequest, TResponse>::Request {
                    request: request.clone(),
                    seq,
                });
                let Some(handler) = handlers.get(&request.operation) else {
                    if reject_unknown {
                        let error =
                            format!("remoteResponder: unknown operation '{}'", request.operation);
                        emitted.push(remote_responder_rejected_event::<TRequest, TResponse>(
                            request, error,
                        ));
                    }
                    continue;
                };
                let response = match catch_unwind(AssertUnwindSafe(|| handler(&request))) {
                    Ok(Ok(payload)) => RemoteCallResponse::Result {
                        operation: request.operation.clone(),
                        request_id: request.request_id.clone(),
                        payload,
                    },
                    Ok(Err(error)) => RemoteCallResponse::Error {
                        operation: request.operation.clone(),
                        request_id: request.request_id.clone(),
                        error,
                    },
                    Err(panic) => {
                        if let Some(message) = panic_message(&panic) {
                            if is_graph_invariant_panic(&message) {
                                resume_unwind(panic);
                            }
                            RemoteCallResponse::Error {
                                operation: request.operation.clone(),
                                request_id: request.request_id.clone(),
                                error: format!("remoteResponder: handler threw: {message}"),
                            }
                        } else {
                            RemoteCallResponse::Error {
                                operation: request.operation.clone(),
                                request_id: request.request_id.clone(),
                                error: "remoteResponder: handler threw".to_owned(),
                            }
                        }
                    }
                };
                let command = WireBridgeCommand::Send {
                    payload: response,
                    idempotency_key: Some(format!(
                        "{}:{}:response",
                        session_id, request.request_id
                    )),
                    request_id: Some(request.request_id.clone()),
                };
                emitted.push(RemoteResponderEvent::<TRequest, TResponse>::Response {
                    request_id: request.request_id,
                    operation: request.operation,
                    command,
                });
            }
            ctx.down(crate::protocol::data_wave(emitted));
            crate::ledger::store(ctx, &cursor);
        },
        {
            let mut opts = no_terminal_graph_opts(format!("{name}/events"));
            opts.node = crate::ledger::write_back_json(opts.node);
            opts
        },
    )
}

fn remote_responder_rejected_event<TRequest, TResponse>(
    request: RemoteCallRequest<TRequest>,
    error: String,
) -> RemoteResponderEvent<TRequest, TResponse> {
    let command = WireBridgeCommand::Send {
        payload: RemoteCallResponse::Error {
            operation: request.operation.clone(),
            request_id: request.request_id.clone(),
            error: error.clone(),
        },
        idempotency_key: None,
        request_id: Some(request.request_id.clone()),
    };
    RemoteResponderEvent::Rejected {
        request_id: Some(request.request_id),
        operation: Some(request.operation),
        error,
        command: Some(command),
    }
}

fn remote_responder_response_commands_node<TRequest, TResponse>(
    graph: &Graph,
    events: &Node<RemoteResponderEvent<TRequest, TResponse>>,
    name: &str,
) -> Node<WireBridgeCommand<RemoteCallResponse<TResponse>>>
where
    TRequest: Clone + 'static,
    TResponse: Clone + 'static,
{
    graph.node_opts::<WireBridgeCommand<RemoteCallResponse<TResponse>>, _>(
        vec![events.erased()],
        |ctx| {
            for event in ctx
                .dep(0)
                .values::<RemoteResponderEvent<TRequest, TResponse>>()
            {
                match event.as_ref() {
                    RemoteResponderEvent::Response { command, .. } => ctx.emit(command.clone()),
                    RemoteResponderEvent::Rejected {
                        command: Some(command),
                        ..
                    } => ctx.emit(command.clone()),
                    _ => {}
                }
            }
        },
        no_terminal_graph_opts(format!("{name}/responseCommands")),
    )
}

fn remote_responder_requests_node<TRequest, TResponse>(
    graph: &Graph,
    events: &Node<RemoteResponderEvent<TRequest, TResponse>>,
    name: &str,
) -> Node<RemoteCallRequest<TRequest>>
where
    TRequest: Clone + 'static,
    TResponse: Clone + 'static,
{
    graph.node_opts::<RemoteCallRequest<TRequest>, _>(
        vec![events.erased()],
        |ctx| {
            for event in ctx
                .dep(0)
                .values::<RemoteResponderEvent<TRequest, TResponse>>()
            {
                if let RemoteResponderEvent::Request { request, .. } = event.as_ref() {
                    ctx.emit(request.clone());
                }
            }
        },
        no_terminal_graph_opts(format!("{name}/requests")),
    )
}

fn remote_responder_status_node<TRequest, TResponse>(
    graph: &Graph,
    events: &Node<RemoteResponderEvent<TRequest, TResponse>>,
    name: &str,
) -> Node<RemoteResponderStatus>
where
    TRequest: Clone + 'static,
    TResponse: Clone + 'static,
{
    // Correlation, dedupe and audit state: a ledger the bundle owns (review Q13).
    let ledger = crate::ledger::Ledger::new(
        graph,
        format!("{name}/statusLedger"),
        serde_json::Value::Null,
    );
    graph.node_opts::<RemoteResponderStatus, _>(
        vec![events.erased(), ledger.erased()],
        |ctx| {
            let mut status =
                crate::ledger::load_or(ctx, ctx.dep_len() - 1, initial_remote_responder_status);
            for event in ctx
                .dep(0)
                .values::<RemoteResponderEvent<TRequest, TResponse>>()
            {
                match event.as_ref() {
                    RemoteResponderEvent::Response {
                        request_id,
                        operation,
                        command,
                    } => {
                        status.request_id = Some(request_id.clone());
                        status.operation = Some(operation.clone());
                        if response_command_error(command).is_some() {
                            status.state = RemoteResponderStatusState::Rejected;
                            status.rejected = status.rejected.saturating_add(1);
                        } else {
                            status.state = RemoteResponderStatusState::Responded;
                            status.handled = status.handled.saturating_add(1);
                        }
                    }
                    RemoteResponderEvent::Rejected {
                        request_id,
                        operation,
                        ..
                    } => {
                        status.state = RemoteResponderStatusState::Rejected;
                        status.request_id = request_id.clone();
                        status.operation = operation.clone();
                        status.rejected = status.rejected.saturating_add(1);
                    }
                    RemoteResponderEvent::Invalid { .. } => {
                        status.state = RemoteResponderStatusState::Errored;
                        status.errors = status.errors.saturating_add(1);
                    }
                    RemoteResponderEvent::Request { request, .. } => {
                        status.request_id = Some(request.request_id.clone());
                        status.operation = Some(request.operation.clone());
                    }
                }
            }
            crate::ledger::store(ctx, &status);
            ctx.emit(status);
        },
        {
            let mut opts = no_terminal_graph_opts(format!("{name}/status"));
            opts.node = crate::ledger::write_back_json(opts.node);
            opts
        },
    )
}

fn initial_remote_responder_status() -> RemoteResponderStatus {
    RemoteResponderStatus {
        state: RemoteResponderStatusState::Idle,
        operation: None,
        request_id: None,
        handled: 0,
        rejected: 0,
        errors: 0,
    }
}

fn remote_responder_errors_node<TRequest, TResponse>(
    graph: &Graph,
    events: &Node<RemoteResponderEvent<TRequest, TResponse>>,
    name: &str,
) -> Node<RemoteCallError>
where
    TRequest: Clone + 'static,
    TResponse: Clone + 'static,
{
    graph.node_opts::<RemoteCallError, _>(
        vec![events.erased()],
        |ctx| {
            for event in ctx
                .dep(0)
                .values::<RemoteResponderEvent<TRequest, TResponse>>()
            {
                match event.as_ref() {
                    RemoteResponderEvent::Response {
                        request_id,
                        operation,
                        command,
                    } => {
                        if let Some(error) = response_command_error(command) {
                            ctx.emit(RemoteCallError {
                                operation: Some(operation.clone()),
                                request_id: Some(request_id.clone()),
                                error,
                            });
                        }
                    }
                    RemoteResponderEvent::Rejected {
                        request_id,
                        operation,
                        error,
                        ..
                    } => ctx.emit(RemoteCallError {
                        operation: operation.clone(),
                        request_id: request_id.clone(),
                        error: error.clone(),
                    }),
                    RemoteResponderEvent::Invalid { error } => ctx.emit(RemoteCallError {
                        operation: None,
                        request_id: None,
                        error: error.clone(),
                    }),
                    _ => {}
                }
            }
        },
        no_terminal_graph_opts(format!("{name}/errors")),
    )
}

fn response_command_error<T>(command: &WireBridgeCommand<RemoteCallResponse<T>>) -> Option<String> {
    match command {
        WireBridgeCommand::Send {
            payload: RemoteCallResponse::Error { error, .. },
            ..
        } => Some(error.clone()),
        _ => None,
    }
}

fn no_terminal_graph_opts(name: impl Into<String>) -> GraphNodeOpts {
    let mut opts = GraphNodeOpts::named(name);
    opts.node.complete = Policy::Manual;
    opts.node.error = Policy::Manual;
    opts
}

fn panic_message(panic: &Box<dyn std::any::Any + Send>) -> Option<String> {
    panic.downcast_ref::<String>().cloned().or_else(|| {
        panic
            .downcast_ref::<&'static str>()
            .map(|message| (*message).to_owned())
    })
}

fn is_graph_invariant_panic(message: &str) -> bool {
    message.contains("R-request-order")
        || message.contains("R-rewire")
        || message.contains("D22")
        || message.contains("same graph")
        || message.contains("different graph")
}

fn wire_bridge_events_node<TOutbound, TInbound>(
    graph: &Graph,
    command: &Node<WireBridgeCommand<TOutbound>>,
    inbound: &Node<WireBridgeIngress<TInbound>>,
    name: String,
    opts: WireBridgeOptions,
) -> Node<WireBridgeIndexedEvent<TOutbound, TInbound>>
where
    TOutbound: Clone + 'static,
    TInbound: Clone + 'static,
{
    let session_id = opts.session_id.clone();
    let policy = opts.retry.clone();
    let now = opts
        .now_ms
        .clone()
        .unwrap_or_else(|| Rc::new(|| 0_u64) as Rc<dyn Fn() -> u64>);
    let event_opts = GraphNodeOpts {
        name: Some(format!("{name}/events")),
        node: NodeOpts {
            complete: Policy::Manual,
            error: Policy::Manual,
            ..NodeOpts::default()
        }
        // The lifetime's cleanup marks the bridge state inactive (R-lifetime@2).
        .with_setup(|_| {
            crate::lifetime::cleanup(|closing| {
                if let Some(state) = closing.state_get::<RefCell<BridgeState<TOutbound>>>() {
                    let mut state = state.borrow_mut();
                    state.active = false;
                    state.pending.clear();
                }
            })
        }),
        ..GraphNodeOpts::default()
    };
    graph.node_opts::<WireBridgeIndexedEvent<TOutbound, TInbound>, _>(
        vec![command.erased(), inbound.erased()],
        move |ctx| {
            let state = bridge_state::<TOutbound>(ctx);
            state.borrow_mut().active = true;
            let mut emitted = Vec::new();
            for value in raw_data(ctx, 1) {
                match value.downcast::<WireBridgeIngress<TInbound>>() {
                    Ok(ingress) => process_inbound(
                        ctx,
                        &state,
                        (*ingress).clone(),
                        &session_id,
                    ),
                    Err(_) => emitted.push(WireBridgeEvent::<TOutbound, TInbound>::Invalid {
                        error: "wireBridge: inbound DATA must carry a wire bridge envelope"
                            .to_owned(),
                    }),
                }
            }
            emit_events(ctx, &state, emitted);
            let mut emitted = Vec::new();
            for value in raw_data(ctx, 0) {
                match value.downcast::<WireBridgeCommand<TOutbound>>() {
                    Ok(command) => {
                        process_command::<TOutbound, TInbound>(
                            ctx,
                            &state,
                            (*command).clone(),
                            &opts,
                            &policy,
                            &now,
                        );
                    }
                    Err(_) => emitted.push(WireBridgeEvent::<TOutbound, TInbound>::Invalid {
                        error: "wireBridge: command DATA must carry a wire bridge command"
                            .to_owned(),
                    }),
                }
            }
            emit_events(ctx, &state, emitted);
            if let Some(terminal) = ctx.dep(1).terminal() {
                let error = match terminal {
                    DepTerminal::Complete => format!(
                        "{session_id}: inbound protocol COMPLETE is local misuse; remote completion must arrive as a DATA envelope fact"
                    ),
                    DepTerminal::Error(error) => format!(
                        "{session_id}: inbound protocol ERROR {error} is local misuse; remote errors must arrive as DATA envelope facts"
                    ),
                };
                emit_event(ctx, &state, WireBridgeEvent::<TOutbound, TInbound>::Invalid { error });
            }
        },
        event_opts,
    )
}

/// Emits `event` with the bridge's next index (review Q13).
fn emit_event<TOutbound: Clone + 'static, TInbound: Clone + 'static>(
    ctx: &Ctx,
    state: &Rc<RefCell<BridgeState<TOutbound>>>,
    event: WireBridgeEvent<TOutbound, TInbound>,
) {
    ctx.emit(indexed(state, event));
}

/// Emits `events` as one array, each with the bridge's next index.
fn emit_events<TOutbound: Clone + 'static, TInbound: Clone + 'static>(
    ctx: &Ctx,
    state: &Rc<RefCell<BridgeState<TOutbound>>>,
    events: Vec<WireBridgeEvent<TOutbound, TInbound>>,
) {
    let events: Vec<_> = events
        .into_iter()
        .map(|event| indexed(state, event))
        .collect();
    ctx.down(crate::protocol::data_wave(events));
}

fn indexed<TOutbound, TInbound>(
    state: &Rc<RefCell<BridgeState<TOutbound>>>,
    event: WireBridgeEvent<TOutbound, TInbound>,
) -> WireBridgeIndexedEvent<TOutbound, TInbound> {
    let mut state = state.borrow_mut();
    let index = state.next_index;
    state.next_index += 1;
    WireBridgeIndexedEvent { index, event }
}

fn bridge_state<T: Clone + 'static>(ctx: &Ctx) -> Rc<RefCell<BridgeState<T>>> {
    if let Some(state) = ctx.state_get::<RefCell<BridgeState<T>>>() {
        return state;
    }
    let state = RefCell::new(BridgeState::<T> {
        active: true,
        next_seq: 1,
        cursor: 0,
        remote_cursor: 0,
        next_index: 1,
        pending: BTreeMap::new(),
    });
    ctx.state_set(state);
    ctx.state_get::<RefCell<BridgeState<T>>>()
        .expect("bridge state was just installed")
}

fn raw_data(ctx: &Ctx, dep: usize) -> Vec<AnyValue> {
    if dep >= ctx.dep_len() {
        return Vec::new();
    }
    ctx.dep(dep)
        .raw_waves()
        .iter()
        .flat_map(|wave| wave.iter().cloned())
        .collect()
}

fn process_command<TOutbound, TInbound>(
    ctx: &Ctx,
    state: &Rc<RefCell<BridgeState<TOutbound>>>,
    command: WireBridgeCommand<TOutbound>,
    opts: &WireBridgeOptions,
    policy: &RetryPolicy,
    now: &Rc<dyn Fn() -> u64>,
) where
    TOutbound: Clone + 'static,
    TInbound: Clone + 'static,
{
    match command {
        WireBridgeCommand::Start {
            idempotency_key,
            request_id,
        } => emit_outbound::<TOutbound, TInbound>(
            ctx,
            state,
            OutboundSpec {
                envelope_type: WireBridgeEnvelopeType::Start,
                payload: None,
                idempotency_key,
                request_id,
                ack_for_seq: None,
                track_ack: true,
                clear_pending_first: false,
            },
            opts,
            policy,
            now,
        ),
        WireBridgeCommand::Send {
            payload,
            idempotency_key,
            request_id,
        } => emit_outbound::<TOutbound, TInbound>(
            ctx,
            state,
            OutboundSpec {
                envelope_type: WireBridgeEnvelopeType::Data,
                payload: Some(WireBridgePayload::Data(payload)),
                idempotency_key,
                request_id,
                ack_for_seq: None,
                track_ack: true,
                clear_pending_first: false,
            },
            opts,
            policy,
            now,
        ),
        WireBridgeCommand::Ack {
            ack_for_seq,
            idempotency_key,
            request_id,
        } => {
            if ack_for_seq == 0 {
                emit_event(
                    ctx,
                    state,
                    WireBridgeEvent::<TOutbound, TInbound>::Invalid {
                        error: "wireBridge: ack command ack_for_seq must be positive".to_owned(),
                    },
                );
                return;
            }
            emit_outbound::<TOutbound, TInbound>(
                ctx,
                state,
                OutboundSpec {
                    envelope_type: WireBridgeEnvelopeType::Ack,
                    payload: None,
                    idempotency_key,
                    request_id,
                    ack_for_seq: Some(ack_for_seq),
                    track_ack: false,
                    clear_pending_first: false,
                },
                opts,
                policy,
                now,
            );
        }
        WireBridgeCommand::Nack {
            ack_for_seq,
            error,
            idempotency_key,
            request_id,
        } => {
            if ack_for_seq == 0 {
                emit_event(
                    ctx,
                    state,
                    WireBridgeEvent::<TOutbound, TInbound>::Invalid {
                        error: "wireBridge: nack command ack_for_seq must be positive".to_owned(),
                    },
                );
                return;
            }
            emit_outbound::<TOutbound, TInbound>(
                ctx,
                state,
                OutboundSpec {
                    envelope_type: WireBridgeEnvelopeType::Nack,
                    payload: Some(WireBridgePayload::Error(error)),
                    idempotency_key,
                    request_id,
                    ack_for_seq: Some(ack_for_seq),
                    track_ack: false,
                    clear_pending_first: false,
                },
                opts,
                policy,
                now,
            );
        }
        WireBridgeCommand::Close {
            reason,
            idempotency_key,
        } => {
            emit_outbound::<TOutbound, TInbound>(
                ctx,
                state,
                OutboundSpec {
                    envelope_type: WireBridgeEnvelopeType::Close,
                    payload: Some(WireBridgePayload::Close { reason }),
                    idempotency_key,
                    request_id: None,
                    ack_for_seq: None,
                    track_ack: true,
                    clear_pending_first: true,
                },
                opts,
                policy,
                now,
            );
        }
        WireBridgeCommand::AckTimeout {
            seq,
            attempt,
            observed_at_ms,
        } => process_ack_timeout_command::<TOutbound, TInbound>(
            ctx,
            state,
            AckTimeoutCommandInput {
                seq,
                attempt,
                observed_at_ms,
            },
            opts,
            policy,
            now,
        ),
    }
}

struct AckTimeoutCommandInput {
    seq: u64,
    attempt: u32,
    observed_at_ms: Option<u64>,
}

struct OutboundSpec<T> {
    envelope_type: WireBridgeEnvelopeType,
    payload: Option<WireBridgePayload<T>>,
    idempotency_key: Option<String>,
    request_id: Option<String>,
    ack_for_seq: Option<u64>,
    track_ack: bool,
    clear_pending_first: bool,
}

fn emit_outbound<TOutbound, TInbound>(
    ctx: &Ctx,
    state: &Rc<RefCell<BridgeState<TOutbound>>>,
    spec: OutboundSpec<TOutbound>,
    opts: &WireBridgeOptions,
    policy: &RetryPolicy,
    now: &Rc<dyn Fn() -> u64>,
) where
    TOutbound: Clone + 'static,
    TInbound: Clone + 'static,
{
    let Some((seq, cursor, next_seq)) = ({
        let state = state.borrow();
        if state.next_seq == u64::MAX {
            None
        } else {
            Some((state.next_seq, state.cursor, state.next_seq + 1))
        }
    }) else {
        emit_event(
            ctx,
            state,
            WireBridgeEvent::<TOutbound, TInbound>::Invalid {
                error: format!("{}: next outbound seq exceeded u64::MAX", opts.session_id),
            },
        );
        return;
    };
    let envelope = match wire_bridge_envelope(WireBridgeEnvelopeInput {
        session_id: opts.session_id.clone(),
        envelope_type: spec.envelope_type,
        seq,
        cursor,
        payload: spec.payload,
        idempotency_key: spec.idempotency_key,
        attempt: 1,
        max_attempts: policy.max_attempts,
        timestamp_ms: Some(now()),
        ack_for_seq: spec.ack_for_seq,
        request_id: spec.request_id,
    }) {
        Ok(envelope) => envelope,
        Err(error) => {
            emit_event(
                ctx,
                state,
                WireBridgeEvent::<TOutbound, TInbound>::Invalid {
                    error: error.to_string(),
                },
            );
            return;
        }
    };
    if spec.clear_pending_first {
        clear_pending(state);
    }
    state.borrow_mut().next_seq = next_seq;
    emit_event(
        ctx,
        state,
        WireBridgeEvent::<TOutbound, TInbound>::Outbound {
            envelope: envelope.clone(),
        },
    );
    if spec.track_ack {
        state.borrow_mut().pending.insert(
            envelope.metadata.seq,
            PendingEnvelope {
                envelope,
                timeout_reported_attempt: None,
                retry_due_at_ms: None,
            },
        );
    }
}

fn clear_pending<T>(state: &Rc<RefCell<BridgeState<T>>>) {
    state.borrow_mut().pending.clear();
}

fn process_ack_timeout_command<TOutbound, TInbound>(
    ctx: &Ctx,
    state: &Rc<RefCell<BridgeState<TOutbound>>>,
    input: AckTimeoutCommandInput,
    opts: &WireBridgeOptions,
    policy: &RetryPolicy,
    now: &Rc<dyn Fn() -> u64>,
) where
    TOutbound: Clone + 'static,
    TInbound: Clone + 'static,
{
    let AckTimeoutCommandInput {
        seq,
        attempt,
        observed_at_ms,
    } = input;
    if seq == 0 {
        emit_event(
            ctx,
            state,
            WireBridgeEvent::<TOutbound, TInbound>::Invalid {
                error: "wireBridge: ack-timeout command seq must be positive".to_owned(),
            },
        );
        return;
    }
    if attempt == 0 {
        emit_event(
            ctx,
            state,
            WireBridgeEvent::<TOutbound, TInbound>::Invalid {
                error: "wireBridge: ack-timeout command attempt must be positive".to_owned(),
            },
        );
        return;
    }
    let current = {
        let state_ref = state.borrow();
        let Some(pending) = state_ref.pending.get(&seq) else {
            return;
        };
        if pending.envelope.metadata.attempt != attempt {
            return;
        }
        pending.envelope.clone()
    };
    let retry_due = {
        let pending = state.borrow();
        if let Some(pending) = pending.pending.get(&seq) {
            if pending.timeout_reported_attempt == Some(attempt) {
                if let (Some(retry_due_at_ms), Some(observed_at_ms)) =
                    (pending.retry_due_at_ms, observed_at_ms)
                {
                    if observed_at_ms < retry_due_at_ms {
                        return;
                    }
                }
                true
            } else {
                false
            }
        } else {
            false
        }
    };
    if retry_due {
        emit_retry_outbound::<TOutbound, TInbound>(ctx, state, policy, now, current);
        return;
    }
    emit_event(
        ctx,
        state,
        WireBridgeEvent::<TOutbound, TInbound>::Timeout { seq, attempt },
    );
    if !policy.should_retry(attempt) {
        state.borrow_mut().pending.remove(&seq);
        emit_event(
            ctx,
            state,
            WireBridgeEvent::<TOutbound, TInbound>::Exhausted {
                seq,
                attempt,
                error: format!("{}: ack timeout for seq {seq}", opts.session_id),
            },
        );
        return;
    }
    let next_attempt = attempt.saturating_add(1);
    let delay_ms = policy.next_delay_ms(next_attempt).unwrap_or_default();
    if let Some(pending) = state.borrow_mut().pending.get_mut(&seq) {
        pending.timeout_reported_attempt = Some(attempt);
        if delay_ms > 0 {
            pending.retry_due_at_ms = observed_at_ms.map(|ms| ms.saturating_add(delay_ms));
        }
    }
    emit_event(
        ctx,
        state,
        WireBridgeEvent::<TOutbound, TInbound>::Retry {
            seq,
            attempt: next_attempt,
            delay_ms,
            error: format!("{}: ack timeout for seq {seq}", opts.session_id),
        },
    );
    let should_wait = state
        .borrow()
        .pending
        .get(&seq)
        .and_then(|pending| pending.retry_due_at_ms)
        .is_some();
    if !should_wait {
        emit_retry_outbound::<TOutbound, TInbound>(ctx, state, policy, now, current);
    }
}

fn emit_retry_outbound<TOutbound, TInbound>(
    ctx: &Ctx,
    state: &Rc<RefCell<BridgeState<TOutbound>>>,
    policy: &RetryPolicy,
    now: &Rc<dyn Fn() -> u64>,
    current: WireBridgeEnvelope<TOutbound>,
) where
    TOutbound: Clone + 'static,
    TInbound: Clone + 'static,
{
    let seq = current.metadata.seq;
    let attempt = current.metadata.attempt.saturating_add(1);
    let cursor = state.borrow().cursor;
    let timestamp_ms = now();
    let retry = wire_bridge_envelope(WireBridgeEnvelopeInput {
        session_id: current.session_id.clone(),
        envelope_type: current.envelope_type,
        seq,
        cursor,
        payload: current.payload.clone(),
        idempotency_key: Some(current.metadata.idempotency_key.clone()),
        attempt,
        max_attempts: policy.max_attempts,
        timestamp_ms: Some(timestamp_ms),
        ack_for_seq: current.metadata.ack_for_seq,
        request_id: current.metadata.request_id.clone(),
    })
    .expect("retry envelope keeps validated metadata");
    if let Some(pending) = state.borrow_mut().pending.get_mut(&seq) {
        pending.envelope = retry.clone();
        pending.timeout_reported_attempt = None;
        pending.retry_due_at_ms = None;
    }
    emit_event(
        ctx,
        state,
        WireBridgeEvent::<TOutbound, TInbound>::Outbound { envelope: retry },
    );
}

fn process_inbound<TOutbound, TInbound>(
    ctx: &Ctx,
    state: &Rc<RefCell<BridgeState<TOutbound>>>,
    ingress: WireBridgeIngress<TInbound>,
    session_id: &str,
) where
    TOutbound: Clone + 'static,
    TInbound: Clone + 'static,
{
    let envelope = match ingress {
        WireBridgeIngress::Envelope(envelope) => envelope,
        WireBridgeIngress::Invalid(error) => {
            emit_event(
                ctx,
                state,
                WireBridgeEvent::<TOutbound, TInbound>::Invalid { error },
            );
            return;
        }
    };
    if let Err(error) = validate_inbound_envelope(&envelope) {
        emit_event(
            ctx,
            state,
            WireBridgeEvent::<TOutbound, TInbound>::Invalid {
                error: error.to_string(),
            },
        );
        return;
    }
    if envelope.session_id != session_id {
        emit_event(
            ctx,
            state,
            WireBridgeEvent::<TOutbound, TInbound>::SessionMismatch {
                expected: session_id.to_owned(),
                actual: envelope.session_id,
            },
        );
        return;
    }
    let seq = envelope.metadata.seq;
    let early_event = {
        let mut state_mut = state.borrow_mut();
        let expected = state_mut.cursor.saturating_add(1);
        if seq <= state_mut.cursor {
            Some(WireBridgeEvent::<TOutbound, TInbound>::Duplicate {
                seq,
                cursor: state_mut.cursor,
            })
        } else if seq > expected {
            Some(WireBridgeEvent::<TOutbound, TInbound>::OutOfOrder { seq, expected })
        } else if envelope.metadata.cursor < state_mut.remote_cursor {
            Some(WireBridgeEvent::<TOutbound, TInbound>::Invalid {
                error: format!(
                    "{session_id}: inbound cursor {} regressed below {}",
                    envelope.metadata.cursor, state_mut.remote_cursor
                ),
            })
        } else {
            state_mut.remote_cursor = envelope.metadata.cursor;
            state_mut.cursor = seq;
            None
        }
    };
    if let Some(event) = early_event {
        emit_event(ctx, state, event);
        return;
    }
    emit_event(
        ctx,
        state,
        WireBridgeEvent::<TOutbound, TInbound>::Cursor { cursor: seq },
    );
    emit_event(
        ctx,
        state,
        WireBridgeEvent::<TOutbound, TInbound>::Inbound {
            envelope: envelope.clone(),
        },
    );
    match envelope.envelope_type {
        WireBridgeEnvelopeType::Ack => process_ack(ctx, state, envelope),
        WireBridgeEnvelopeType::Nack => process_nack(ctx, state, envelope),
        WireBridgeEnvelopeType::Start
        | WireBridgeEnvelopeType::Data
        | WireBridgeEnvelopeType::Status
        | WireBridgeEnvelopeType::Error
        | WireBridgeEnvelopeType::Close => {}
    }
}

fn validate_inbound_envelope<T>(
    envelope: &WireBridgeEnvelope<T>,
) -> Result<(), WireBridgeEnvelopeError> {
    if envelope.session_id.is_empty() {
        return Err(WireBridgeEnvelopeError::EmptySessionId);
    }
    if envelope.metadata.seq == 0 {
        return Err(WireBridgeEnvelopeError::ZeroSeq);
    }
    if envelope.metadata.idempotency_key.is_empty() {
        return Err(WireBridgeEnvelopeError::EmptyIdempotencyKey);
    }
    if envelope.metadata.attempt == 0 {
        return Err(WireBridgeEnvelopeError::ZeroAttempt);
    }
    if envelope.metadata.max_attempts < envelope.metadata.attempt {
        return Err(WireBridgeEnvelopeError::MaxAttemptsBeforeAttempt);
    }
    if envelope.metadata.ack_for_seq == Some(0) {
        return Err(WireBridgeEnvelopeError::ZeroAckForSeq);
    }
    if matches!(
        envelope.envelope_type,
        WireBridgeEnvelopeType::Ack | WireBridgeEnvelopeType::Nack
    ) && envelope.metadata.ack_for_seq.is_none()
    {
        return Err(WireBridgeEnvelopeError::MissingAckForSeq);
    }
    validate_payload_for_type(envelope.envelope_type, &envelope.payload)?;
    Ok(())
}

fn validate_payload_for_type<T>(
    envelope_type: WireBridgeEnvelopeType,
    payload: &Option<WireBridgePayload<T>>,
) -> Result<(), WireBridgeEnvelopeError> {
    match envelope_type {
        WireBridgeEnvelopeType::Data => match payload {
            Some(WireBridgePayload::Data(_)) => Ok(()),
            Some(_) => Err(WireBridgeEnvelopeError::PayloadTypeMismatch),
            None => Err(WireBridgeEnvelopeError::MissingPayload),
        },
        WireBridgeEnvelopeType::Nack | WireBridgeEnvelopeType::Error => match payload {
            Some(WireBridgePayload::Error(_)) => Ok(()),
            Some(_) => Err(WireBridgeEnvelopeError::PayloadTypeMismatch),
            None => Err(WireBridgeEnvelopeError::MissingPayload),
        },
        WireBridgeEnvelopeType::Status => match payload {
            Some(WireBridgePayload::Status(_)) => Ok(()),
            Some(_) => Err(WireBridgeEnvelopeError::PayloadTypeMismatch),
            None => Err(WireBridgeEnvelopeError::MissingPayload),
        },
        WireBridgeEnvelopeType::Close => match payload {
            Some(WireBridgePayload::Close { .. }) => Ok(()),
            Some(_) => Err(WireBridgeEnvelopeError::PayloadTypeMismatch),
            None => Err(WireBridgeEnvelopeError::MissingPayload),
        },
        WireBridgeEnvelopeType::Start | WireBridgeEnvelopeType::Ack => match payload {
            Some(_) => Err(WireBridgeEnvelopeError::UnexpectedPayload),
            None => Ok(()),
        },
    }
}

fn process_ack<TOutbound, TInbound>(
    ctx: &Ctx,
    state: &Rc<RefCell<BridgeState<TOutbound>>>,
    envelope: WireBridgeEnvelope<TInbound>,
) where
    TOutbound: Clone + 'static,
    TInbound: Clone + 'static,
{
    let ack_for_seq = envelope
        .metadata
        .ack_for_seq
        .expect("validated ack has ack_for_seq");
    let pending = state.borrow_mut().pending.remove(&ack_for_seq);
    match pending {
        Some(pending) => {
            emit_event(
                ctx,
                state,
                WireBridgeEvent::<TOutbound, TInbound>::Ack {
                    ack_for_seq,
                    envelope,
                    outbound: pending.envelope,
                },
            );
        }
        None => emit_event(
            ctx,
            state,
            WireBridgeEvent::<TOutbound, TInbound>::LateReceipt {
                receipt: WireBridgeReceipt::Ack,
                ack_for_seq,
            },
        ),
    }
}

fn process_nack<TOutbound, TInbound>(
    ctx: &Ctx,
    state: &Rc<RefCell<BridgeState<TOutbound>>>,
    envelope: WireBridgeEnvelope<TInbound>,
) where
    TOutbound: Clone + 'static,
    TInbound: Clone + 'static,
{
    let ack_for_seq = envelope
        .metadata
        .ack_for_seq
        .expect("validated nack has ack_for_seq");
    let pending = state.borrow_mut().pending.remove(&ack_for_seq);
    let error = payload_error_string(&envelope.payload, "remote nack");
    match pending {
        Some(pending) => {
            emit_event(
                ctx,
                state,
                WireBridgeEvent::<TOutbound, TInbound>::Nack {
                    ack_for_seq,
                    envelope,
                    outbound: pending.envelope,
                    error,
                },
            );
        }
        None => emit_event(
            ctx,
            state,
            WireBridgeEvent::<TOutbound, TInbound>::LateReceipt {
                receipt: WireBridgeReceipt::Nack,
                ack_for_seq,
            },
        ),
    }
}

fn project_outbound<TOutbound, TInbound>(
    graph: &Graph,
    events: &Node<WireBridgeIndexedEvent<TOutbound, TInbound>>,
    name: &str,
) -> Node<WireBridgeEnvelope<TOutbound>>
where
    TOutbound: Clone + 'static,
    TInbound: Clone + 'static,
{
    graph.node_opts::<WireBridgeEnvelope<TOutbound>, _>(
        vec![events.erased()],
        |ctx| {
            let mut emitted = Vec::new();
            for event in ctx
                .dep(0)
                .values::<WireBridgeIndexedEvent<TOutbound, TInbound>>()
            {
                if let WireBridgeEvent::Outbound { envelope } = &event.event {
                    emitted.push(envelope.clone());
                }
            }
            ctx.down(crate::protocol::data_wave(emitted));
        },
        GraphNodeOpts::named(format!("{name}/outbound")),
    )
}

fn project_acks<TOutbound, TInbound>(
    graph: &Graph,
    events: &Node<WireBridgeIndexedEvent<TOutbound, TInbound>>,
    name: &str,
) -> Node<WireBridgeAck<TInbound>>
where
    TOutbound: Clone + 'static,
    TInbound: Clone + 'static,
{
    graph.node_opts::<WireBridgeAck<TInbound>, _>(
        vec![events.erased()],
        |ctx| {
            let mut emitted = Vec::new();
            for event in ctx
                .dep(0)
                .values::<WireBridgeIndexedEvent<TOutbound, TInbound>>()
            {
                if let WireBridgeEvent::Ack {
                    ack_for_seq,
                    envelope,
                    ..
                } = &event.event
                {
                    emitted.push(WireBridgeAck {
                        ack_for_seq: *ack_for_seq,
                        envelope: envelope.clone(),
                    });
                }
            }
            ctx.down(crate::protocol::data_wave(emitted));
        },
        GraphNodeOpts::named(format!("{name}/acks")),
    )
}

fn project_nacks<TOutbound, TInbound>(
    graph: &Graph,
    events: &Node<WireBridgeIndexedEvent<TOutbound, TInbound>>,
    name: &str,
) -> Node<WireBridgeNack<TInbound>>
where
    TOutbound: Clone + 'static,
    TInbound: Clone + 'static,
{
    graph.node_opts::<WireBridgeNack<TInbound>, _>(
        vec![events.erased()],
        |ctx| {
            let mut emitted = Vec::new();
            for event in ctx
                .dep(0)
                .values::<WireBridgeIndexedEvent<TOutbound, TInbound>>()
            {
                if let WireBridgeEvent::Nack {
                    ack_for_seq,
                    envelope,
                    error,
                    ..
                } = &event.event
                {
                    emitted.push(WireBridgeNack {
                        ack_for_seq: *ack_for_seq,
                        envelope: envelope.clone(),
                        error: error.clone(),
                    });
                }
            }
            ctx.down(crate::protocol::data_wave(emitted));
        },
        GraphNodeOpts::named(format!("{name}/nacks")),
    )
}

fn project_status<TOutbound, TInbound>(
    graph: &Graph,
    events: &Node<WireBridgeIndexedEvent<TOutbound, TInbound>>,
    name: &str,
    session_id: String,
) -> Node<WireBridgeStatus>
where
    TOutbound: Clone + 'static,
    TInbound: Clone + 'static,
{
    // The status counters are an audit ledger the bundle owns (review Q13), with the
    // index of the last event they counted: a new lifetime starts on the events node's
    // snapshot, which replays its last array, and skips every event at or below that
    // high water, so a reactivated status never counts an event twice.
    let ledger = crate::ledger::Ledger::new(
        graph,
        format!("{name}/statusLedger"),
        serde_json::to_value(StatusLedger {
            status: initial_status(&session_id),
            high_water: 0,
        })
        .unwrap_or_default(),
    );
    graph.node_opts::<WireBridgeStatus, _>(
        vec![events.erased(), ledger.erased()],
        move |ctx| {
            let mut ledger: StatusLedger = crate::ledger::load_or(ctx, 1, || StatusLedger {
                status: initial_status(&session_id),
                high_water: 0,
            });
            for event in ctx
                .dep(0)
                .values::<WireBridgeIndexedEvent<TOutbound, TInbound>>()
            {
                if event.index <= ledger.high_water {
                    continue;
                }
                ledger.status = reduce_status(ledger.status, &event.event);
                ledger.high_water = event.index;
            }
            crate::ledger::store(ctx, &ledger);
            ctx.emit(ledger.status);
        },
        {
            let mut opts = GraphNodeOpts::named(format!("{name}/status"));
            opts.node = crate::ledger::write_back_json(opts.node);
            opts
        },
    )
}

/// The bridge status and the index of the last event it counted (review Q13).
#[derive(serde::Serialize, serde::Deserialize)]
struct StatusLedger {
    status: WireBridgeStatus,
    high_water: u64,
}

fn initial_status(session_id: &str) -> WireBridgeStatus {
    WireBridgeStatus {
        session_id: session_id.to_owned(),
        state: WireBridgeStatusState::Idle,
        cursor: 0,
        next_seq: 1,
        pending: 0,
        attempts: 0,
        acked: 0,
        nacked: 0,
        errors: 0,
        last_seq: None,
        last_delay_ms: None,
    }
}

fn reduce_status<TOutbound, TInbound>(
    mut current: WireBridgeStatus,
    event: &WireBridgeEvent<TOutbound, TInbound>,
) -> WireBridgeStatus {
    match event {
        WireBridgeEvent::Outbound { envelope } => {
            current.state = match envelope.envelope_type {
                WireBridgeEnvelopeType::Start => WireBridgeStatusState::Started,
                WireBridgeEnvelopeType::Close => WireBridgeStatusState::Closed,
                WireBridgeEnvelopeType::Data
                | WireBridgeEnvelopeType::Ack
                | WireBridgeEnvelopeType::Nack
                | WireBridgeEnvelopeType::Status
                | WireBridgeEnvelopeType::Error => WireBridgeStatusState::Open,
            };
            current.next_seq = current
                .next_seq
                .max(envelope.metadata.seq.saturating_add(1));
            if envelope.envelope_type == WireBridgeEnvelopeType::Close {
                current.pending = if envelope.metadata.attempt == 1 { 1 } else { 0 };
            } else if should_track_ack(envelope.envelope_type) && envelope.metadata.attempt == 1 {
                current.pending = current.pending.saturating_add(1);
            }
            if should_track_ack(envelope.envelope_type) {
                current.attempts = current.attempts.saturating_add(1);
            }
            current.last_seq = Some(envelope.metadata.seq);
        }
        WireBridgeEvent::Ack {
            envelope, outbound, ..
        } => {
            current.state = if outbound.envelope_type == WireBridgeEnvelopeType::Close {
                WireBridgeStatusState::Closed
            } else {
                WireBridgeStatusState::Open
            };
            current.pending = current.pending.saturating_sub(1);
            current.acked = current.acked.saturating_add(1);
            current.last_seq = Some(envelope.metadata.seq);
        }
        WireBridgeEvent::Nack { envelope, .. } => {
            current.state = WireBridgeStatusState::Errored;
            current.pending = current.pending.saturating_sub(1);
            current.nacked = current.nacked.saturating_add(1);
            current.errors = current.errors.saturating_add(1);
            current.last_seq = Some(envelope.metadata.seq);
        }
        WireBridgeEvent::Retry { seq, delay_ms, .. } => {
            current.state = WireBridgeStatusState::Waiting;
            current.last_seq = Some(*seq);
            current.last_delay_ms = Some(*delay_ms);
        }
        WireBridgeEvent::Exhausted { seq, .. } => {
            current.state = WireBridgeStatusState::Exhausted;
            current.pending = current.pending.saturating_sub(1);
            current.errors = current.errors.saturating_add(1);
            current.last_seq = Some(*seq);
        }
        WireBridgeEvent::Cursor { cursor } => current.cursor = *cursor,
        WireBridgeEvent::OutOfOrder { seq, .. } => {
            current.state = WireBridgeStatusState::Errored;
            current.errors = current.errors.saturating_add(1);
            current.last_seq = Some(*seq);
        }
        WireBridgeEvent::SessionMismatch { .. }
        | WireBridgeEvent::LateReceipt { .. }
        | WireBridgeEvent::Invalid { .. } => {
            current.state = WireBridgeStatusState::Errored;
            current.errors = current.errors.saturating_add(1);
        }
        WireBridgeEvent::Inbound { envelope } => match envelope.envelope_type {
            WireBridgeEnvelopeType::Error => {
                current.state = WireBridgeStatusState::Errored;
                current.errors = current.errors.saturating_add(1);
                current.last_seq = Some(envelope.metadata.seq);
            }
            WireBridgeEnvelopeType::Close => {
                current.state = WireBridgeStatusState::Closed;
                current.last_seq = Some(envelope.metadata.seq);
            }
            WireBridgeEnvelopeType::Start
            | WireBridgeEnvelopeType::Data
            | WireBridgeEnvelopeType::Ack
            | WireBridgeEnvelopeType::Nack
            | WireBridgeEnvelopeType::Status => {}
        },
        WireBridgeEvent::Timeout { .. } | WireBridgeEvent::Duplicate { .. } => {}
    }
    current
}

fn project_errors<TOutbound, TInbound>(
    graph: &Graph,
    events: &Node<WireBridgeIndexedEvent<TOutbound, TInbound>>,
    name: &str,
) -> Node<String>
where
    TOutbound: Clone + 'static,
    TInbound: Clone + 'static,
{
    let name = name.to_owned();
    let node_name = name.clone();
    graph.node_opts::<String, _>(
        vec![events.erased()],
        move |ctx| {
            let mut emitted = Vec::new();
            for event in ctx.dep(0).values::<WireBridgeIndexedEvent<TOutbound, TInbound>>() {
                match &event.event {
                    WireBridgeEvent::Nack { error, .. }
                    | WireBridgeEvent::Exhausted { error, .. }
                    | WireBridgeEvent::Invalid { error } => emitted.push(error.clone()),
                    WireBridgeEvent::OutOfOrder { seq, expected } => emitted.push(format!(
                        "{name}: inbound seq {seq} arrived before expected seq {expected}"
                    )),
                    WireBridgeEvent::SessionMismatch { expected, actual } => emitted.push(format!(
                        "{name}: inbound session {actual} did not match expected {expected}"
                    )),
                    WireBridgeEvent::LateReceipt {
                        receipt,
                        ack_for_seq,
                    } => emitted.push(format!(
                        "{name}: late {receipt:?} for unknown or completed ack_for_seq {ack_for_seq}"
                    )),
                    WireBridgeEvent::Inbound { envelope }
                        if envelope.envelope_type == WireBridgeEnvelopeType::Error =>
                    {
                        emitted.push(payload_error_string(
                            &envelope.payload,
                            "remote error envelope",
                        ));
                    }
                    WireBridgeEvent::Outbound { .. }
                    | WireBridgeEvent::Inbound { .. }
                    | WireBridgeEvent::Ack { .. }
                    | WireBridgeEvent::Timeout { .. }
                    | WireBridgeEvent::Retry { .. }
                    | WireBridgeEvent::Cursor { .. }
                    | WireBridgeEvent::Duplicate { .. } => {}
                }
            }
            ctx.down(crate::protocol::data_wave(emitted));
        },
        GraphNodeOpts::named(format!("{node_name}/errors")),
    )
}

fn project_cursor<TOutbound, TInbound>(
    graph: &Graph,
    events: &Node<WireBridgeIndexedEvent<TOutbound, TInbound>>,
    name: &str,
) -> Node<u64>
where
    TOutbound: Clone + 'static,
    TInbound: Clone + 'static,
{
    graph.node_opts::<u64, _>(
        vec![events.erased()],
        |ctx| {
            let mut emitted = Vec::new();
            for event in ctx
                .dep(0)
                .values::<WireBridgeIndexedEvent<TOutbound, TInbound>>()
            {
                if let WireBridgeEvent::Cursor { cursor } = &event.event {
                    emitted.push(*cursor);
                }
            }
            ctx.down(crate::protocol::data_wave(emitted));
        },
        GraphNodeOpts::named(format!("{name}/cursor")),
    )
}

fn project_attempts<TOutbound, TInbound>(
    graph: &Graph,
    events: &Node<WireBridgeIndexedEvent<TOutbound, TInbound>>,
    name: &str,
) -> Node<WireBridgeAttempt>
where
    TOutbound: Clone + 'static,
    TInbound: Clone + 'static,
{
    graph.node_opts::<WireBridgeAttempt, _>(
        vec![events.erased()],
        |ctx| {
            let mut emitted = Vec::new();
            for event in ctx
                .dep(0)
                .values::<WireBridgeIndexedEvent<TOutbound, TInbound>>()
            {
                if let WireBridgeEvent::Outbound { envelope } = &event.event {
                    if should_track_ack(envelope.envelope_type) {
                        emitted.push(WireBridgeAttempt {
                            seq: envelope.metadata.seq,
                            attempt: envelope.metadata.attempt,
                            max_attempts: envelope.metadata.max_attempts,
                        });
                    }
                }
            }
            ctx.down(crate::protocol::data_wave(emitted));
        },
        GraphNodeOpts::named(format!("{name}/attempts")),
    )
}

fn should_track_ack(envelope_type: WireBridgeEnvelopeType) -> bool {
    matches!(
        envelope_type,
        WireBridgeEnvelopeType::Start
            | WireBridgeEnvelopeType::Data
            | WireBridgeEnvelopeType::Close
    )
}

fn payload_error_string<T>(payload: &Option<WireBridgePayload<T>>, fallback: &str) -> String {
    match payload {
        Some(WireBridgePayload::Error(error)) => error.clone(),
        Some(WireBridgePayload::Data(_))
        | Some(WireBridgePayload::Status(_))
        | Some(WireBridgePayload::Close { .. })
        | None => fallback.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::graph;

    fn envelope<T>(
        session_id: &str,
        envelope_type: WireBridgeEnvelopeType,
        seq: u64,
        cursor: u64,
        payload: Option<WireBridgePayload<T>>,
        ack_for_seq: Option<u64>,
    ) -> WireBridgeEnvelope<T> {
        wire_bridge_envelope(WireBridgeEnvelopeInput {
            session_id: session_id.to_owned(),
            envelope_type,
            seq,
            cursor,
            payload,
            idempotency_key: None,
            attempt: 1,
            max_attempts: 1,
            timestamp_ms: None,
            ack_for_seq,
            request_id: None,
        })
        .expect("test envelope is valid")
    }

    #[test]
    fn wire_bridge_envelope_validates_metadata_and_idempotency() {
        assert_eq!(
            wire_bridge_idempotency_key("session-a", 7),
            canonical_tuple_key(&["session-a", "7"])
        );
        let env = wire_bridge_envelope(WireBridgeEnvelopeInput {
            session_id: "session-a".to_owned(),
            envelope_type: WireBridgeEnvelopeType::Data,
            seq: 7,
            cursor: 3,
            payload: Some(WireBridgePayload::Data("ok".to_owned())),
            idempotency_key: None,
            attempt: 2,
            max_attempts: 4,
            timestamp_ms: Some(10),
            ack_for_seq: None,
            request_id: Some("req-1".to_owned()),
        })
        .expect("valid envelope");
        assert_eq!(
            env.metadata.idempotency_key,
            canonical_tuple_key(&["session-a", "7"])
        );
        assert_eq!(env.metadata.request_id.as_deref(), Some("req-1"));
        assert!(matches!(
            wire_bridge_envelope::<()>(WireBridgeEnvelopeInput {
                session_id: "session-a".to_owned(),
                envelope_type: WireBridgeEnvelopeType::Data,
                seq: 1,
                cursor: 0,
                payload: None,
                idempotency_key: None,
                attempt: 1,
                max_attempts: 1,
                timestamp_ms: None,
                ack_for_seq: None,
                request_id: None,
            }),
            Err(WireBridgeEnvelopeError::MissingPayload)
        ));
        assert!(matches!(
            wire_bridge_envelope::<()>(WireBridgeEnvelopeInput {
                session_id: "session-a".to_owned(),
                envelope_type: WireBridgeEnvelopeType::Error,
                seq: 1,
                cursor: 0,
                payload: Some(WireBridgePayload::Status("wrong".to_owned())),
                idempotency_key: None,
                attempt: 1,
                max_attempts: 1,
                timestamp_ms: None,
                ack_for_seq: None,
                request_id: None,
            }),
            Err(WireBridgeEnvelopeError::PayloadTypeMismatch)
        ));
        assert!(matches!(
            wire_bridge_envelope::<()>(WireBridgeEnvelopeInput {
                session_id: "session-a".to_owned(),
                envelope_type: WireBridgeEnvelopeType::Data,
                seq: 0,
                cursor: 0,
                payload: None,
                idempotency_key: None,
                attempt: 1,
                max_attempts: 1,
                timestamp_ms: None,
                ack_for_seq: None,
                request_id: None,
            }),
            Err(WireBridgeEnvelopeError::ZeroSeq)
        ));
        assert!(matches!(
            wire_bridge_envelope::<()>(WireBridgeEnvelopeInput {
                session_id: "session-a".to_owned(),
                envelope_type: WireBridgeEnvelopeType::Ack,
                seq: 1,
                cursor: 0,
                payload: None,
                idempotency_key: None,
                attempt: 1,
                max_attempts: 1,
                timestamp_ms: None,
                ack_for_seq: None,
                request_id: None,
            }),
            Err(WireBridgeEnvelopeError::MissingAckForSeq)
        ));
    }

    #[test]
    fn wire_bridge_commands_emit_ordered_outbound_facts_and_describe_topology() {
        let g = graph();
        let bridge = wire_bridge::<String, String>(
            &g,
            WireBridgeOptions {
                name: Some("bridge".to_owned()),
                session_id: "session-a".to_owned(),
                now_ms: Some(Rc::new(|| 42)),
                ..WireBridgeOptions::new("session-a")
            },
        );
        let _outbound = bridge.outbound.subscribe(|_| {});
        let _status = bridge.status.subscribe(|_| {});
        let _attempts = bridge.attempts.subscribe(|_| {});

        bridge.start();
        bridge.send("payload".to_owned(), None, Some("req-1".to_owned()));
        bridge.ack(3, None, None);
        bridge.nack(4, "bad", None, None);
        bridge.close(Some("done".to_owned()), None);

        assert_eq!(
            bridge.outbound.cache(),
            Some(WireBridgeEnvelope {
                session_id: "session-a".to_owned(),
                envelope_type: WireBridgeEnvelopeType::Close,
                payload: Some(WireBridgePayload::Close {
                    reason: Some("done".to_owned()),
                }),
                metadata: WireBridgeMetadata {
                    seq: 5,
                    cursor: 0,
                    idempotency_key: canonical_tuple_key(&["session-a", "5"]),
                    attempt: 1,
                    max_attempts: 1,
                    timestamp_ms: Some(42),
                    ack_for_seq: None,
                    request_id: None,
                },
            })
        );
        assert_eq!(
            bridge.status.cache().unwrap().state,
            WireBridgeStatusState::Closed
        );
        let snap = crate::topology::snapshot(&g);
        let mut ids = snap
            .nodes
            .iter()
            .map(|node| node.id.clone())
            .filter(|id| id.starts_with("bridge/"))
            .collect::<Vec<_>>();
        ids.sort();
        assert_eq!(
            ids,
            vec![
                "bridge/acks",
                "bridge/attempts",
                "bridge/command",
                "bridge/cursor",
                "bridge/errors",
                "bridge/events",
                "bridge/inbound",
                "bridge/nacks",
                "bridge/outbound",
                "bridge/status",
                "bridge/statusLedger",
            ]
        );
        // The status counters are a ledger the bundle owns (Q13); the event fold is
        // held for the graph's life and keeps its sequence in its private state.
        assert!(snap
            .edges()
            .iter()
            .any(|edge| edge.from == "bridge/statusLedger" && edge.to == "bridge/status"));
        assert!(snap
            .edges()
            .iter()
            .any(|edge| edge.from == "bridge/command" && edge.to == "bridge/events"));
        assert!(snap
            .edges()
            .iter()
            .any(|edge| edge.from == "bridge/inbound" && edge.to == "bridge/events"));
    }

    #[test]
    fn inbound_ack_advances_cursor_and_clears_pending() {
        let g = graph();
        let bridge =
            wire_bridge::<String, String>(&g, WireBridgeOptions::named("session-a", "bridge"));
        let _acks = bridge.acks.subscribe(|_| {});
        let _cursor = bridge.cursor.subscribe(|_| {});
        let _status = bridge.status.subscribe(|_| {});

        bridge.send("work".to_owned(), None, None);
        bridge.inbound.set(envelope(
            "session-a",
            WireBridgeEnvelopeType::Ack,
            1,
            1,
            None,
            Some(1),
        ));

        assert_eq!(bridge.cursor.cache(), Some(1));
        assert_eq!(bridge.acks.cache().unwrap().ack_for_seq, 1);
        let status = bridge.status.cache().unwrap();
        assert_eq!(status.pending, 0);
        assert_eq!(status.acked, 1);
    }

    #[test]
    fn inbound_nack_error_and_status_are_graph_visible() {
        let g = graph();
        let bridge =
            wire_bridge::<String, String>(&g, WireBridgeOptions::named("session-a", "bridge"));
        let _nacks = bridge.nacks.subscribe(|_| {});
        let _errors = bridge.errors.subscribe(|_| {});
        let _status = bridge.status.subscribe(|_| {});

        bridge.send("work".to_owned(), None, None);
        bridge.inbound.set(envelope(
            "session-a",
            WireBridgeEnvelopeType::Nack,
            1,
            1,
            Some(WireBridgePayload::Error("remote failed".to_owned())),
            Some(1),
        ));

        assert_eq!(bridge.nacks.cache().unwrap().ack_for_seq, 1);
        assert_eq!(bridge.errors.cache(), Some("remote failed".to_owned()));
        let status = bridge.status.cache().unwrap();
        assert_eq!(status.state, WireBridgeStatusState::Errored);
        assert_eq!(status.nacked, 1);
    }

    #[test]
    fn inbound_duplicate_out_of_order_late_and_session_mismatch_are_visible() {
        let g = graph();
        let bridge =
            wire_bridge::<String, String>(&g, WireBridgeOptions::named("session-a", "bridge"));
        let seen = Rc::new(RefCell::new(Vec::new()));
        let seen_sink = seen.clone();
        let _events = bridge.events.subscribe(
            move |msgs: &[crate::protocol::Message<crate::protocol::AnyValue>]| {
                let handle = |msg: &crate::protocol::Message<crate::protocol::AnyValue>| {
                    if let Message::Data(value) = msg {
                        if let Some(event) =
                            value.downcast_ref::<WireBridgeIndexedEvent<String, String>>()
                        {
                            seen_sink.borrow_mut().push(event.event.clone());
                        }
                    }
                };
                for msg in msgs {
                    handle(msg);
                }
            },
        );

        bridge.inbound.set(envelope(
            "session-b",
            WireBridgeEnvelopeType::Data,
            1,
            0,
            Some(WireBridgePayload::Data("bad-session".to_owned())),
            None,
        ));
        bridge.inbound.set(envelope(
            "session-a",
            WireBridgeEnvelopeType::Data,
            2,
            0,
            Some(WireBridgePayload::Data("early".to_owned())),
            None,
        ));
        bridge.inbound.set(envelope(
            "session-a",
            WireBridgeEnvelopeType::Data,
            1,
            0,
            Some(WireBridgePayload::Data("ok".to_owned())),
            None,
        ));
        bridge.inbound.set(envelope(
            "session-a",
            WireBridgeEnvelopeType::Data,
            1,
            0,
            Some(WireBridgePayload::Data("dup".to_owned())),
            None,
        ));
        bridge.inbound.set(envelope(
            "session-a",
            WireBridgeEnvelopeType::Ack,
            2,
            1,
            None,
            Some(99),
        ));

        let events = seen.borrow();
        assert!(events.iter().any(|event| matches!(
            event,
            WireBridgeEvent::SessionMismatch { expected, actual }
                if expected == "session-a" && actual == "session-b"
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            WireBridgeEvent::OutOfOrder {
                seq: 2,
                expected: 1
            }
        )));
        assert!(events
            .iter()
            .any(|event| matches!(event, WireBridgeEvent::Duplicate { seq: 1, cursor: 1 })));
        assert!(events.iter().any(|event| matches!(
            event,
            WireBridgeEvent::LateReceipt {
                receipt: WireBridgeReceipt::Ack,
                ack_for_seq: 99
            }
        )));
    }

    #[test]
    fn malformed_inbound_and_command_reject_as_bridge_error_facts() {
        let g = graph();
        let bridge =
            wire_bridge::<String, String>(&g, WireBridgeOptions::named("session-a", "bridge"));
        let _errors = bridge.errors.subscribe(|_| {});
        let _outbound = bridge.outbound.subscribe(|_| {});

        bridge
            .command
            .down(vec![Message::Data(Rc::new("not-a-command".to_owned()))]);
        // The inbound takes only typed envelopes (WireBridgeInbound::set), so a
        // malformed inbound value cannot be written in process.
        assert!(bridge.errors.cache().is_some());
        assert!(bridge.outbound.cache().is_none());
    }

    #[test]
    fn each_inbound_envelope_is_its_own_local_wave() {
        let g = graph();
        let bridge =
            wire_bridge::<String, String>(&g, WireBridgeOptions::named("session-a", "bridge"));
        let batch_sizes = Rc::new(RefCell::new(Vec::new()));
        let batch_sizes_sink = batch_sizes.clone();
        let observed = g.node_opts::<usize, _>(
            vec![bridge.inbound.erased()],
            move |ctx| {
                let len = ctx.dep(0).values::<WireBridgeIngress<String>>().len();
                batch_sizes_sink.borrow_mut().push(len);
                ctx.emit(len);
            },
            GraphNodeOpts::named("inbound-batch-sizes"),
        );
        let _observed = observed.subscribe(|_| {});

        for (seq, text) in [(1, "one"), (2, "two")] {
            bridge.inbound.set(envelope(
                "session-a",
                WireBridgeEnvelopeType::Data,
                seq,
                0,
                Some(WireBridgePayload::Data(text.to_owned())),
                None,
            ));
        }

        // The first run, on activation, sees no envelope yet (no kernel first-run gate).
        assert_eq!(*batch_sizes.borrow(), vec![0, 1, 1]);
    }

    #[test]
    fn invalid_close_command_does_not_clear_pending_ack() {
        let g = graph();
        let bridge =
            wire_bridge::<String, String>(&g, WireBridgeOptions::named("session-a", "bridge"));
        let _acks = bridge.acks.subscribe(|_| {});
        let _errors = bridge.errors.subscribe(|_| {});
        let _status = bridge.status.subscribe(|_| {});

        bridge.send("work".to_owned(), None, None);
        bridge.close(Some("done".to_owned()), Some(String::new()));
        assert!(bridge
            .errors
            .cache()
            .unwrap()
            .contains("idempotency_key must be non-empty"));

        bridge.inbound.set(envelope(
            "session-a",
            WireBridgeEnvelopeType::Ack,
            1,
            1,
            None,
            Some(1),
        ));

        assert_eq!(bridge.acks.cache().unwrap().ack_for_seq, 1);
        let status = bridge.status.cache().unwrap();
        assert_eq!(status.pending, 0);
        assert_eq!(status.acked, 1);
    }

    #[test]
    fn remote_protocol_error_does_not_terminalize_local_bridge_node() {
        let g = graph();
        let bridge =
            wire_bridge::<String, String>(&g, WireBridgeOptions::named("session-a", "bridge"));
        let _errors = bridge.errors.subscribe(|_| {});
        let _cursor = bridge.cursor.subscribe(|_| {});

        bridge.inbound.set(envelope(
            "session-a",
            WireBridgeEnvelopeType::Data,
            1,
            0,
            Some(WireBridgePayload::Data("still-live".to_owned())),
            None,
        ));

        assert_eq!(bridge.cursor.cache(), Some(1));
        assert_ne!(bridge.events.status().output, crate::OutputStatus::Errored);
        assert_ne!(bridge.status.status().output, crate::OutputStatus::Errored);
    }

    #[test]
    fn remote_error_envelope_and_close_are_facts_not_local_terminals() {
        let g = graph();
        let bridge =
            wire_bridge::<String, String>(&g, WireBridgeOptions::named("session-a", "bridge"));
        let _errors = bridge.errors.subscribe(|_| {});
        let _status = bridge.status.subscribe(|_| {});

        bridge.inbound.set(envelope(
            "session-a",
            WireBridgeEnvelopeType::Error,
            1,
            0,
            Some(WireBridgePayload::Error("remote failed".to_owned())),
            None,
        ));
        assert_eq!(bridge.errors.cache(), Some("remote failed".to_owned()));
        assert_eq!(
            bridge.status.cache().unwrap().state,
            WireBridgeStatusState::Errored
        );
        assert_ne!(bridge.status.status().output, crate::OutputStatus::Errored);

        bridge.close(None, None);
        let status = bridge.status.cache().unwrap();
        assert_eq!(status.state, WireBridgeStatusState::Closed);
        assert_eq!(status.pending, 1);
        assert_ne!(
            bridge.status.status().output,
            crate::OutputStatus::Completed
        );

        bridge.inbound.set(envelope(
            "session-a",
            WireBridgeEnvelopeType::Ack,
            2,
            1,
            None,
            Some(1),
        ));
        let status = bridge.status.cache().unwrap();
        assert_eq!(status.state, WireBridgeStatusState::Closed);
        assert_eq!(status.pending, 0);
        assert_eq!(status.acked, 1);
        assert_ne!(
            bridge.status.status().output,
            crate::OutputStatus::Completed
        );
    }

    #[test]
    fn explicit_ack_timeout_command_retries_and_exhausts_without_hidden_driver() {
        let g = graph();
        let bridge = wire_bridge::<String, String>(
            &g,
            WireBridgeOptions {
                name: Some("bridge".to_owned()),
                session_id: "session-a".to_owned(),
                retry: RetryPolicy::new(
                    2,
                    crate::resilience::BackoffPolicy::Constant { delay_ms: 10 },
                ),
                now_ms: Some(Rc::new(|| 1000)),
            },
        );
        let _outbound = bridge.outbound.subscribe(|_| {});
        let _attempts = bridge.attempts.subscribe(|_| {});
        let _errors = bridge.errors.subscribe(|_| {});
        let _status = bridge.status.subscribe(|_| {});

        bridge.send("work".to_owned(), None, None);
        assert_eq!(bridge.attempts.cache().unwrap().attempt, 1);
        bridge
            .command
            .down(vec![data_msg(WireBridgeCommand::<String>::AckTimeout {
                seq: 1,
                attempt: 1,
                observed_at_ms: Some(1000),
            })]);
        assert_eq!(
            bridge.status.cache().unwrap().state,
            WireBridgeStatusState::Waiting
        );
        assert_eq!(bridge.attempts.cache().unwrap().attempt, 1);
        bridge
            .command
            .down(vec![data_msg(WireBridgeCommand::<String>::AckTimeout {
                seq: 1,
                attempt: 1,
                observed_at_ms: Some(1005),
            })]);
        assert_eq!(bridge.attempts.cache().unwrap().attempt, 1);
        bridge
            .command
            .down(vec![data_msg(WireBridgeCommand::<String>::AckTimeout {
                seq: 1,
                attempt: 1,
                observed_at_ms: Some(1010),
            })]);
        assert_eq!(bridge.attempts.cache().unwrap().attempt, 2);
        bridge
            .command
            .down(vec![data_msg(WireBridgeCommand::<String>::AckTimeout {
                seq: 1,
                attempt: 2,
                observed_at_ms: Some(1020),
            })]);
        assert_eq!(
            bridge.status.cache().unwrap().state,
            WireBridgeStatusState::Exhausted
        );
        assert_eq!(
            bridge.errors.cache(),
            Some("session-a: ack timeout for seq 1".to_owned())
        );
    }

    #[test]
    fn stale_ack_timeout_command_is_fail_closed_noop_after_ack() {
        let g = graph();
        let bridge = wire_bridge::<String, String>(
            &g,
            WireBridgeOptions {
                name: Some("bridge".to_owned()),
                session_id: "session-a".to_owned(),
                ..WireBridgeOptions::new("session-a")
            },
        );
        let _acks = bridge.acks.subscribe(|_| {});
        let _status = bridge.status.subscribe(|_| {});
        let _attempts = bridge.attempts.subscribe(|_| {});

        bridge.send("work".to_owned(), None, None);
        bridge.inbound.set(envelope(
            "session-a",
            WireBridgeEnvelopeType::Ack,
            1,
            1,
            None,
            Some(1),
        ));

        assert_eq!(bridge.acks.cache().unwrap().ack_for_seq, 1);
        bridge
            .command
            .down(vec![data_msg(WireBridgeCommand::<String>::AckTimeout {
                seq: 1,
                attempt: 1,
                observed_at_ms: Some(1000),
            })]);
        let status = bridge.status.cache().unwrap();
        assert_eq!(status.pending, 0);
        assert_eq!(status.acked, 1);
        assert_eq!(status.last_seq, Some(1));
        assert_eq!(bridge.attempts.cache().unwrap().attempt, 1);
    }

    #[test]
    fn malformed_ack_timeout_command_is_invalid_fact_not_terminal() {
        let g = graph();
        let bridge = wire_bridge::<String, String>(
            &g,
            WireBridgeOptions {
                name: Some("bridge".to_owned()),
                session_id: "session-a".to_owned(),
                ..WireBridgeOptions::new("session-a")
            },
        );
        let _errors = bridge.errors.subscribe(|_| {});

        bridge.send("work".to_owned(), None, None);
        bridge
            .command
            .down(vec![data_msg(WireBridgeCommand::<String>::AckTimeout {
                seq: 1,
                attempt: 0,
                observed_at_ms: Some(1000),
            })]);

        assert_eq!(
            bridge.errors.cache(),
            Some("wireBridge: ack-timeout command attempt must be positive".to_owned())
        );
        assert_ne!(bridge.events.status().output, crate::OutputStatus::Errored);

        bridge
            .command
            .down(vec![data_msg(WireBridgeCommand::<String>::AckTimeout {
                seq: 0,
                attempt: 1,
                observed_at_ms: Some(1000),
            })]);
        assert_eq!(
            bridge.errors.cache(),
            Some("wireBridge: ack-timeout command seq must be positive".to_owned())
        );
        assert_ne!(bridge.events.status().output, crate::OutputStatus::Errored);
    }

    #[test]
    fn unknown_ack_timeout_command_is_fail_closed_noop_without_pending_state() {
        let g = graph();
        let bridge = wire_bridge::<String, String>(
            &g,
            WireBridgeOptions {
                name: Some("bridge".to_owned()),
                session_id: "session-a".to_owned(),
                ..WireBridgeOptions::new("session-a")
            },
        );
        let _outbound = bridge.outbound.subscribe(|_| {});
        let _attempts = bridge.attempts.subscribe(|_| {});
        let _errors = bridge.errors.subscribe(|_| {});
        let _status = bridge.status.subscribe(|_| {});

        bridge
            .command
            .down(vec![data_msg(WireBridgeCommand::<String>::AckTimeout {
                seq: 99,
                attempt: 1,
                observed_at_ms: Some(1000),
            })]);

        assert!(bridge.outbound.cache().is_none());
        assert!(bridge.attempts.cache().is_none());
        assert!(bridge.errors.cache().is_none());
        assert!(bridge
            .status
            .cache()
            .is_none_or(|status| status.state == WireBridgeStatusState::Idle));
        assert_ne!(bridge.events.status().output, crate::OutputStatus::Errored);
    }

    #[test]
    fn remote_call_orphan_response_is_visible_and_not_buffered_for_future_request() {
        let g = graph();
        let bridge = wire_bridge::<RemoteCallRequest<String>, RemoteCallResponse<String>>(
            &g,
            WireBridgeOptions::named("session-a", "bridge"),
        );
        let remote = remote_call::<String, String>(&g, &bridge);
        let _results = remote.results.subscribe(|_| {});
        let _errors = remote.errors.subscribe(|_| {});
        let _status = remote.status.subscribe(|_| {});

        bridge.inbound.set(envelope(
            "session-a",
            WireBridgeEnvelopeType::Data,
            1,
            0,
            Some(WireBridgePayload::Data(RemoteCallResponse::Result {
                operation: "upper".to_owned(),
                request_id: "req-1".to_owned(),
                payload: "STALE".to_owned(),
            })),
            None,
        ));
        remote.call("upper", "req-1", "hello".to_owned());

        assert!(remote.results.cache().is_none());
        assert_eq!(
            remote.errors.cache(),
            Some(RemoteCallError {
                operation: Some("upper".to_owned()),
                request_id: Some("req-1".to_owned()),
                error: "remote_call: orphan response for unknown or completed request".to_owned(),
            })
        );
        assert_eq!(
            remote.status.cache().unwrap().state,
            RemoteCallStatusState::Requested
        );

        bridge.inbound.set(envelope(
            "session-a",
            WireBridgeEnvelopeType::Data,
            2,
            1,
            Some(WireBridgePayload::Data(RemoteCallResponse::Result {
                operation: "upper".to_owned(),
                request_id: "req-1".to_owned(),
                payload: "HELLO".to_owned(),
            })),
            None,
        ));

        assert_eq!(
            remote.results.cache(),
            Some(RemoteCallResult {
                operation: "upper".to_owned(),
                request_id: "req-1".to_owned(),
                payload: "HELLO".to_owned(),
            })
        );
    }

    #[test]
    fn remote_call_requires_operation_match_and_status_response_is_non_terminal() {
        let g = graph();
        let bridge = wire_bridge::<RemoteCallRequest<String>, RemoteCallResponse<String>>(
            &g,
            WireBridgeOptions::named("session-a", "bridge"),
        );
        let remote = remote_call::<String, String>(&g, &bridge);
        let _responses = remote.responses.subscribe(|_| {});
        let _results = remote.results.subscribe(|_| {});
        let _errors = remote.errors.subscribe(|_| {});
        let _status = remote.status.subscribe(|_| {});

        remote.call("upper", "req-1", "hello".to_owned());
        bridge.inbound.set(envelope(
            "session-a",
            WireBridgeEnvelopeType::Data,
            1,
            1,
            Some(WireBridgePayload::Data(RemoteCallResponse::Result {
                operation: "lower".to_owned(),
                request_id: "req-1".to_owned(),
                payload: "wrong".to_owned(),
            })),
            None,
        ));

        assert!(remote.results.cache().is_none());
        assert_eq!(
            remote.errors.cache(),
            Some(RemoteCallError {
                operation: Some("upper".to_owned()),
                request_id: Some("req-1".to_owned()),
                error:
                    "remote_call: response operation 'lower' did not match pending operation 'upper'"
                        .to_owned(),
            })
        );

        bridge.inbound.set(envelope(
            "session-a",
            WireBridgeEnvelopeType::Data,
            2,
            1,
            Some(WireBridgePayload::Data(RemoteCallResponse::Status {
                operation: "upper".to_owned(),
                request_id: "req-1".to_owned(),
                status: "working".to_owned(),
            })),
            None,
        ));
        assert_eq!(
            remote.responses.cache(),
            Some(RemoteCallResponse::Status {
                operation: "upper".to_owned(),
                request_id: "req-1".to_owned(),
                status: "working".to_owned(),
            })
        );
        assert_eq!(remote.status.cache().unwrap().pending, 1);

        bridge.inbound.set(envelope(
            "session-a",
            WireBridgeEnvelopeType::Data,
            3,
            1,
            Some(WireBridgePayload::Data(RemoteCallResponse::Result {
                operation: "upper".to_owned(),
                request_id: "req-1".to_owned(),
                payload: "HELLO".to_owned(),
            })),
            None,
        ));
        assert_eq!(
            remote.results.cache(),
            Some(RemoteCallResult {
                operation: "upper".to_owned(),
                request_id: "req-1".to_owned(),
                payload: "HELLO".to_owned(),
            })
        );
        assert_eq!(remote.status.cache().unwrap().pending, 0);
    }

    #[test]
    fn remote_call_duplicate_request_id_is_visible_and_does_not_corrupt_pending() {
        let g = graph();
        let bridge = wire_bridge::<RemoteCallRequest<String>, RemoteCallResponse<String>>(
            &g,
            WireBridgeOptions::named("session-a", "bridge"),
        );
        let remote = remote_call::<String, String>(&g, &bridge);
        let _results = remote.results.subscribe(|_| {});
        let _errors = remote.errors.subscribe(|_| {});
        let _status = remote.status.subscribe(|_| {});

        remote.call("upper", "req-1", "first".to_owned());
        remote.call("upper", "req-1", "second".to_owned());

        assert_eq!(
            remote.errors.cache(),
            Some(RemoteCallError {
                operation: Some("upper".to_owned()),
                request_id: Some("req-1".to_owned()),
                error: "remote_call: duplicate in-flight request_id 'req-1'".to_owned(),
            })
        );
        assert_eq!(remote.status.cache().unwrap().pending, 1);

        bridge.inbound.set(envelope(
            "session-a",
            WireBridgeEnvelopeType::Data,
            1,
            1,
            Some(WireBridgePayload::Data(RemoteCallResponse::Result {
                operation: "upper".to_owned(),
                request_id: "req-1".to_owned(),
                payload: "FIRST".to_owned(),
            })),
            None,
        ));

        assert_eq!(
            remote.results.cache(),
            Some(RemoteCallResult {
                operation: "upper".to_owned(),
                request_id: "req-1".to_owned(),
                payload: "FIRST".to_owned(),
            })
        );
        assert_eq!(remote.status.cache().unwrap().pending, 0);
    }
}
