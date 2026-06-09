//! Transport-free, graph-visible wire bridge bundle (D134/D140).
//!
//! The local wave ends at this bridge boundary. Commands become ordered outbound
//! envelope facts; remote receipts enter as later inbound DATA envelope facts.
//! Remote ERROR/COMPLETE are bridge facts, never local protocol terminals.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;
use std::time::Duration;

use crate::async_driver::{DriverCancel, LocalAsyncDriver};
use crate::ctx::{Ctx, DepTerminal, WaveData};
use crate::graph::{Graph, GraphNodeOpts};
use crate::node::{Node, NodeOpts};
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
    format!("{session_id}:{seq}")
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireBridgeStatusState {
    Idle,
    Started,
    Open,
    Waiting,
    Closed,
    Errored,
    Exhausted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
    pub ack_timeout: Option<Duration>,
    pub now_ms: Option<Rc<dyn Fn() -> u64>>,
}

impl WireBridgeOptions {
    pub fn new(session_id: impl Into<String>) -> Self {
        Self {
            name: None,
            session_id: session_id.into(),
            retry: RetryPolicy::default(),
            ack_timeout: None,
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
    pub events: Node<WireBridgeEvent<TOutbound, TInbound>>,
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
pub enum WireBridgeIngress<T> {
    Envelope(WireBridgeEnvelope<T>),
    Invalid(String),
}

#[derive(Clone)]
pub struct WireBridgeInbound<T: Clone + 'static> {
    node: Node<WireBridgeIngress<T>>,
    session_id: String,
}

impl<T: Clone + 'static> WireBridgeInbound<T> {
    pub fn down(&self, msgs: Vec<Message<AnyValue>>) {
        for msg in msgs {
            self.node.down(vec![self.guard_msg(msg)]);
        }
    }

    fn guard_msg(&self, msg: Message<AnyValue>) -> Message<AnyValue> {
        match msg {
            Message::Data(value) => match value.downcast::<WireBridgeEnvelope<T>>() {
                Ok(envelope) => data_msg(WireBridgeIngress::Envelope((*envelope).clone())),
                Err(value) => match value.downcast::<WireBridgeIngress<T>>() {
                    Ok(ingress) => data_msg((*ingress).clone()),
                    Err(_) => data_msg(WireBridgeIngress::<T>::Invalid(
                        "wireBridge: inbound DATA must carry a wire bridge envelope".to_owned(),
                    )),
                },
            },
            Message::Error(error) => data_msg(WireBridgeIngress::<T>::Invalid(
                format!(
                    "{}: inbound protocol ERROR {error} is local misuse; remote errors must arrive as DATA envelope facts",
                    self.session_id
                ),
            )),
            Message::Complete => data_msg(WireBridgeIngress::<T>::Invalid(
                format!(
                    "{}: inbound protocol COMPLETE is local misuse; remote completion must arrive as a DATA envelope fact",
                    self.session_id
                ),
            )),
            other => data_msg(WireBridgeIngress::<T>::Invalid(format!(
                "{}: inbound port accepts DATA envelope facts only; {other:?} is local protocol traffic",
                self.session_id
            ))),
        }
    }

    pub fn set(&self, envelope: WireBridgeEnvelope<T>) {
        self.down(vec![Message::Data(Rc::new(envelope))]);
    }

    pub fn subscribe(&self, sink: impl Fn(&Message<AnyValue>) + 'static) -> Box<dyn FnOnce()> {
        self.node.subscribe(sink)
    }

    pub fn erased(&self) -> crate::node::Core {
        self.node.erased()
    }
}

fn data_msg<T: 'static>(value: T) -> Message<AnyValue> {
    Message::Data(Rc::new(value) as AnyValue)
}

struct PendingEnvelope<T> {
    envelope: WireBridgeEnvelope<T>,
    cancel: Option<DriverCancel>,
}

struct BridgeState<T> {
    active: bool,
    cleanup_installed: bool,
    next_seq: u64,
    cursor: u64,
    remote_cursor: u64,
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
    let name = opts.name.clone().unwrap_or_else(|| "wireBridge".to_owned());
    let command = graph.state_empty_opts::<WireBridgeCommand<TOutbound>>(GraphNodeOpts::named(
        format!("{name}/command"),
    ));
    let inbound_node = graph.state_empty_opts::<WireBridgeIngress<TInbound>>(GraphNodeOpts::named(
        format!("{name}/inbound"),
    ));
    let inbound = WireBridgeInbound {
        node: inbound_node.clone(),
        session_id: opts.session_id.clone(),
    };
    let events =
        wire_bridge_events_node(graph, &command, &inbound_node, name.clone(), opts.clone());
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

fn wire_bridge_events_node<TOutbound, TInbound>(
    graph: &Graph,
    command: &Node<WireBridgeCommand<TOutbound>>,
    inbound: &Node<WireBridgeIngress<TInbound>>,
    name: String,
    opts: WireBridgeOptions,
) -> Node<WireBridgeEvent<TOutbound, TInbound>>
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
            partial: true,
            complete_when_deps_complete: false,
            error_when_deps_error: false,
            terminal_as_real_input: true,
            ..NodeOpts::default()
        },
        ..GraphNodeOpts::default()
    };
    graph.node_opts::<WireBridgeEvent<TOutbound, TInbound>, _>(
        vec![command.erased(), inbound.erased()],
        move |ctx| {
            let state = bridge_state::<TOutbound>(ctx);
            state.borrow_mut().active = true;
            install_cleanup(ctx, state.clone());
            for value in raw_data(ctx, 1) {
                match value.downcast::<WireBridgeIngress<TInbound>>() {
                    Ok(ingress) => process_inbound(
                        ctx,
                        &state,
                        (*ingress).clone(),
                        &session_id,
                    ),
                    Err(_) => ctx.emit(WireBridgeEvent::<TOutbound, TInbound>::Invalid {
                        error: "wireBridge: inbound DATA must carry a wire bridge envelope"
                            .to_owned(),
                    }),
                }
            }
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
                    Err(_) => ctx.emit(WireBridgeEvent::<TOutbound, TInbound>::Invalid {
                        error: "wireBridge: command DATA must carry a wire bridge command"
                            .to_owned(),
                    }),
                }
            }
            if let Some(terminal) = ctx.terminal(1) {
                let error = match terminal {
                    DepTerminal::Complete => format!(
                        "{session_id}: inbound protocol COMPLETE is local misuse; remote completion must arrive as a DATA envelope fact"
                    ),
                    DepTerminal::Error(error) => format!(
                        "{session_id}: inbound protocol ERROR {error} is local misuse; remote errors must arrive as DATA envelope facts"
                    ),
                };
                ctx.emit(WireBridgeEvent::<TOutbound, TInbound>::Invalid { error });
            }
        },
        event_opts,
    )
}

fn bridge_state<T: Clone + 'static>(ctx: &Ctx) -> Rc<RefCell<BridgeState<T>>> {
    if let Some(state) = ctx.state_get::<RefCell<BridgeState<T>>>() {
        return state;
    }
    let state = RefCell::new(BridgeState::<T> {
        active: true,
        cleanup_installed: false,
        next_seq: 1,
        cursor: 0,
        remote_cursor: 0,
        pending: BTreeMap::new(),
    });
    ctx.state_set(state);
    ctx.state_get::<RefCell<BridgeState<T>>>()
        .expect("bridge state was just installed")
}

fn install_cleanup<T: Clone + 'static>(ctx: &Ctx, state: Rc<RefCell<BridgeState<T>>>) {
    if state.borrow().cleanup_installed {
        return;
    }
    state.borrow_mut().cleanup_installed = true;
    ctx.on_deactivation(move || {
        let cancels = {
            let mut state = state.borrow_mut();
            state.active = false;
            state.cleanup_installed = false;
            take_pending_cancels(&mut state)
        };
        for cancel in cancels {
            run_driver_cancel(cancel);
        }
    });
}

fn raw_data(ctx: &Ctx, dep: usize) -> Vec<AnyValue> {
    ctx.wave_data()
        .get(dep)
        .map(|waves| {
            waves
                .iter()
                .flat_map(|wave| {
                    wave.iter().filter_map(|item| match item {
                        WaveData::Data(value) => Some(value.clone()),
                        WaveData::Sentinel => None,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
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
                ctx.emit(WireBridgeEvent::<TOutbound, TInbound>::Invalid {
                    error: "wireBridge: ack command ack_for_seq must be positive".to_owned(),
                });
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
                ctx.emit(WireBridgeEvent::<TOutbound, TInbound>::Invalid {
                    error: "wireBridge: nack command ack_for_seq must be positive".to_owned(),
                });
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
    }
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
        ctx.emit(WireBridgeEvent::<TOutbound, TInbound>::Invalid {
            error: format!("{}: next outbound seq exceeded u64::MAX", opts.session_id),
        });
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
            ctx.emit(WireBridgeEvent::<TOutbound, TInbound>::Invalid {
                error: error.to_string(),
            });
            return;
        }
    };
    if spec.clear_pending_first {
        clear_pending(state);
    }
    state.borrow_mut().next_seq = next_seq;
    ctx.emit(WireBridgeEvent::<TOutbound, TInbound>::Outbound {
        envelope: envelope.clone(),
    });
    if spec.track_ack {
        arm_ack_timeout::<TOutbound, TInbound>(ctx, state, envelope, opts, policy, now);
    }
}

fn clear_pending<T>(state: &Rc<RefCell<BridgeState<T>>>) {
    let cancels = {
        let mut state = state.borrow_mut();
        take_pending_cancels(&mut state)
    };
    for cancel in cancels {
        run_driver_cancel(cancel);
    }
}

fn take_pending_cancels<T>(state: &mut BridgeState<T>) -> Vec<DriverCancel> {
    let mut cancels = Vec::new();
    for pending in state.pending.values_mut() {
        if let Some(cancel) = pending.cancel.take() {
            cancels.push(cancel);
        }
    }
    state.pending.clear();
    cancels
}

fn run_driver_cancel(cancel: DriverCancel) {
    let _ = catch_unwind(AssertUnwindSafe(cancel));
}

fn arm_ack_timeout<TOutbound, TInbound>(
    ctx: &Ctx,
    state: &Rc<RefCell<BridgeState<TOutbound>>>,
    envelope: WireBridgeEnvelope<TOutbound>,
    opts: &WireBridgeOptions,
    policy: &RetryPolicy,
    now: &Rc<dyn Fn() -> u64>,
) where
    TOutbound: Clone + 'static,
    TInbound: Clone + 'static,
{
    let Some(timeout) = opts.ack_timeout else {
        state.borrow_mut().pending.insert(
            envelope.metadata.seq,
            PendingEnvelope {
                envelope,
                cancel: None,
            },
        );
        return;
    };
    let Some(driver) = ctx.local_async_driver() else {
        ctx.emit(WireBridgeEvent::<TOutbound, TInbound>::Exhausted {
            seq: envelope.metadata.seq,
            attempt: envelope.metadata.attempt,
            error: "wireBridge: missing LocalAsyncDriver for ack timeout".to_owned(),
        });
        return;
    };
    let out = Rc::new(ctx.defer());
    state.borrow_mut().pending.insert(
        envelope.metadata.seq,
        PendingEnvelope {
            envelope: envelope.clone(),
            cancel: None,
        },
    );
    schedule_ack_timeout::<TOutbound, TInbound>(
        state.clone(),
        out,
        driver,
        timeout,
        policy.clone(),
        now.clone(),
        envelope,
    );
}

fn schedule_ack_timeout<TOutbound, TInbound>(
    state: Rc<RefCell<BridgeState<TOutbound>>>,
    out: Rc<crate::ctx::DeferredCtx>,
    driver: Rc<dyn LocalAsyncDriver>,
    timeout: Duration,
    policy: RetryPolicy,
    now: Rc<dyn Fn() -> u64>,
    envelope: WireBridgeEnvelope<TOutbound>,
) where
    TOutbound: Clone + 'static,
    TInbound: Clone + 'static,
{
    enum TimeoutFollowUp<T> {
        None,
        Exhausted {
            seq: u64,
            attempt: u32,
            error: String,
        },
        Retry {
            retry: WireBridgeEnvelope<T>,
            delay_ms: u64,
            error: String,
        },
    }

    let seq = envelope.metadata.seq;
    let state_for_callback = state.clone();
    let out_for_callback = out.clone();
    let driver_for_callback = driver.clone();
    let policy_for_callback = policy.clone();
    let now_for_callback = now.clone();
    let cancel = driver.sleep(
        timeout,
        Box::new(move || {
            let timeout_attempt = envelope.metadata.attempt;
            let follow_up = {
                let mut state = state_for_callback.borrow_mut();
                if !state.active || !state.pending.contains_key(&seq) {
                    TimeoutFollowUp::None
                } else if !policy_for_callback.should_retry(envelope.metadata.attempt) {
                    state.pending.remove(&seq);
                    TimeoutFollowUp::Exhausted {
                        seq,
                        attempt: envelope.metadata.attempt,
                        error: format!("{}: ack timeout for seq {seq}", envelope.session_id),
                    }
                } else {
                    let attempt = envelope.metadata.attempt.saturating_add(1);
                    let delay_ms = policy_for_callback
                        .next_delay_ms(attempt)
                        .unwrap_or_default();
                    let retry = wire_bridge_envelope(WireBridgeEnvelopeInput {
                        session_id: envelope.session_id.clone(),
                        envelope_type: envelope.envelope_type,
                        seq,
                        cursor: state.cursor,
                        payload: envelope.payload.clone(),
                        idempotency_key: Some(envelope.metadata.idempotency_key.clone()),
                        attempt,
                        max_attempts: policy_for_callback.max_attempts,
                        timestamp_ms: Some(now_for_callback()),
                        ack_for_seq: envelope.metadata.ack_for_seq,
                        request_id: envelope.metadata.request_id.clone(),
                    })
                    .expect("retry envelope keeps validated metadata");
                    if let Some(pending) = state.pending.get_mut(&seq) {
                        pending.envelope = retry.clone();
                        pending.cancel = None;
                    }
                    TimeoutFollowUp::Retry {
                        retry,
                        delay_ms,
                        error: format!("{}: ack timeout for seq {seq}", envelope.session_id),
                    }
                }
            };
            if matches!(follow_up, TimeoutFollowUp::None) {
                return;
            }
            out_for_callback.emit(WireBridgeEvent::<TOutbound, TInbound>::Timeout {
                seq,
                attempt: timeout_attempt,
            });
            match follow_up {
                TimeoutFollowUp::None => {}
                TimeoutFollowUp::Exhausted {
                    seq,
                    attempt,
                    error,
                } => {
                    out_for_callback.emit(WireBridgeEvent::<TOutbound, TInbound>::Exhausted {
                        seq,
                        attempt,
                        error,
                    });
                }
                TimeoutFollowUp::Retry {
                    retry,
                    delay_ms,
                    error,
                } => {
                    let retry_seq = retry.metadata.seq;
                    let retry_attempt = retry.metadata.attempt;
                    out_for_callback.emit(WireBridgeEvent::<TOutbound, TInbound>::Retry {
                        seq: retry_seq,
                        attempt: retry_attempt,
                        delay_ms,
                        error,
                    });
                    let emit_retry = {
                        let state = state_for_callback.clone();
                        let out = out_for_callback.clone();
                        let driver = driver_for_callback.clone();
                        let policy = policy_for_callback.clone();
                        let now = now_for_callback.clone();
                        move || {
                            let should_emit = {
                                let state = state.borrow();
                                state.active && state.pending.contains_key(&retry_seq)
                            };
                            if !should_emit {
                                return;
                            }
                            out.emit(WireBridgeEvent::<TOutbound, TInbound>::Outbound {
                                envelope: retry.clone(),
                            });
                            schedule_ack_timeout::<TOutbound, TInbound>(
                                state, out, driver, timeout, policy, now, retry,
                            );
                        }
                    };
                    if delay_ms == 0 {
                        emit_retry();
                    } else {
                        let delay_cancel = driver_for_callback
                            .sleep(Duration::from_millis(delay_ms), Box::new(emit_retry));
                        if let Some(pending) =
                            state_for_callback.borrow_mut().pending.get_mut(&retry_seq)
                        {
                            pending.cancel = Some(delay_cancel);
                        }
                    }
                }
            }
        }),
    );
    if let Some(pending) = state.borrow_mut().pending.get_mut(&seq) {
        pending.cancel = Some(cancel);
    }
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
            ctx.emit(WireBridgeEvent::<TOutbound, TInbound>::Invalid { error });
            return;
        }
    };
    if let Err(error) = validate_inbound_envelope(&envelope) {
        ctx.emit(WireBridgeEvent::<TOutbound, TInbound>::Invalid {
            error: error.to_string(),
        });
        return;
    }
    if envelope.session_id != session_id {
        ctx.emit(WireBridgeEvent::<TOutbound, TInbound>::SessionMismatch {
            expected: session_id.to_owned(),
            actual: envelope.session_id,
        });
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
        ctx.emit(event);
        return;
    }
    ctx.emit(WireBridgeEvent::<TOutbound, TInbound>::Cursor { cursor: seq });
    ctx.emit(WireBridgeEvent::<TOutbound, TInbound>::Inbound {
        envelope: envelope.clone(),
    });
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
        Some(mut pending) => {
            if let Some(cancel) = pending.cancel.take() {
                run_driver_cancel(cancel);
            }
            ctx.emit(WireBridgeEvent::<TOutbound, TInbound>::Ack {
                ack_for_seq,
                envelope,
                outbound: pending.envelope,
            });
        }
        None => ctx.emit(WireBridgeEvent::<TOutbound, TInbound>::LateReceipt {
            receipt: WireBridgeReceipt::Ack,
            ack_for_seq,
        }),
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
        Some(mut pending) => {
            if let Some(cancel) = pending.cancel.take() {
                run_driver_cancel(cancel);
            }
            ctx.emit(WireBridgeEvent::<TOutbound, TInbound>::Nack {
                ack_for_seq,
                envelope,
                outbound: pending.envelope,
                error,
            });
        }
        None => ctx.emit(WireBridgeEvent::<TOutbound, TInbound>::LateReceipt {
            receipt: WireBridgeReceipt::Nack,
            ack_for_seq,
        }),
    }
}

fn project_outbound<TOutbound, TInbound>(
    graph: &Graph,
    events: &Node<WireBridgeEvent<TOutbound, TInbound>>,
    name: &str,
) -> Node<WireBridgeEnvelope<TOutbound>>
where
    TOutbound: Clone + 'static,
    TInbound: Clone + 'static,
{
    graph.node_opts::<WireBridgeEnvelope<TOutbound>, _>(
        vec![events.erased()],
        |ctx| {
            for event in ctx.batch::<WireBridgeEvent<TOutbound, TInbound>>(0) {
                if let WireBridgeEvent::Outbound { envelope } = event.as_ref() {
                    ctx.emit(envelope.clone());
                }
            }
        },
        GraphNodeOpts::named(format!("{name}/outbound")),
    )
}

fn project_acks<TOutbound, TInbound>(
    graph: &Graph,
    events: &Node<WireBridgeEvent<TOutbound, TInbound>>,
    name: &str,
) -> Node<WireBridgeAck<TInbound>>
where
    TOutbound: Clone + 'static,
    TInbound: Clone + 'static,
{
    graph.node_opts::<WireBridgeAck<TInbound>, _>(
        vec![events.erased()],
        |ctx| {
            for event in ctx.batch::<WireBridgeEvent<TOutbound, TInbound>>(0) {
                if let WireBridgeEvent::Ack {
                    ack_for_seq,
                    envelope,
                    ..
                } = event.as_ref()
                {
                    ctx.emit(WireBridgeAck {
                        ack_for_seq: *ack_for_seq,
                        envelope: envelope.clone(),
                    });
                }
            }
        },
        GraphNodeOpts::named(format!("{name}/acks")),
    )
}

fn project_nacks<TOutbound, TInbound>(
    graph: &Graph,
    events: &Node<WireBridgeEvent<TOutbound, TInbound>>,
    name: &str,
) -> Node<WireBridgeNack<TInbound>>
where
    TOutbound: Clone + 'static,
    TInbound: Clone + 'static,
{
    graph.node_opts::<WireBridgeNack<TInbound>, _>(
        vec![events.erased()],
        |ctx| {
            for event in ctx.batch::<WireBridgeEvent<TOutbound, TInbound>>(0) {
                if let WireBridgeEvent::Nack {
                    ack_for_seq,
                    envelope,
                    error,
                    ..
                } = event.as_ref()
                {
                    ctx.emit(WireBridgeNack {
                        ack_for_seq: *ack_for_seq,
                        envelope: envelope.clone(),
                        error: error.clone(),
                    });
                }
            }
        },
        GraphNodeOpts::named(format!("{name}/nacks")),
    )
}

fn project_status<TOutbound, TInbound>(
    graph: &Graph,
    events: &Node<WireBridgeEvent<TOutbound, TInbound>>,
    name: &str,
    session_id: String,
) -> Node<WireBridgeStatus>
where
    TOutbound: Clone + 'static,
    TInbound: Clone + 'static,
{
    graph.node_opts::<WireBridgeStatus, _>(
        vec![events.erased()],
        move |ctx| {
            let mut next = ctx
                .state_get::<WireBridgeStatus>()
                .map_or_else(|| initial_status(&session_id), |status| (*status).clone());
            for event in ctx.batch::<WireBridgeEvent<TOutbound, TInbound>>(0) {
                next = reduce_status(next, event.as_ref());
            }
            ctx.state_set(next.clone());
            ctx.emit(next);
        },
        GraphNodeOpts::named(format!("{name}/status")),
    )
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
    events: &Node<WireBridgeEvent<TOutbound, TInbound>>,
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
            for event in ctx.batch::<WireBridgeEvent<TOutbound, TInbound>>(0) {
                match event.as_ref() {
                    WireBridgeEvent::Nack { error, .. }
                    | WireBridgeEvent::Exhausted { error, .. }
                    | WireBridgeEvent::Invalid { error } => ctx.emit(error.clone()),
                    WireBridgeEvent::OutOfOrder { seq, expected } => ctx.emit(format!(
                        "{name}: inbound seq {seq} arrived before expected seq {expected}"
                    )),
                    WireBridgeEvent::SessionMismatch { expected, actual } => ctx.emit(format!(
                        "{name}: inbound session {actual} did not match expected {expected}"
                    )),
                    WireBridgeEvent::LateReceipt {
                        receipt,
                        ack_for_seq,
                    } => ctx.emit(format!(
                        "{name}: late {receipt:?} for unknown or completed ack_for_seq {ack_for_seq}"
                    )),
                    WireBridgeEvent::Inbound { envelope }
                        if envelope.envelope_type == WireBridgeEnvelopeType::Error =>
                    {
                        ctx.emit(payload_error_string(
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
        },
        GraphNodeOpts::named(format!("{node_name}/errors")),
    )
}

fn project_cursor<TOutbound, TInbound>(
    graph: &Graph,
    events: &Node<WireBridgeEvent<TOutbound, TInbound>>,
    name: &str,
) -> Node<u64>
where
    TOutbound: Clone + 'static,
    TInbound: Clone + 'static,
{
    graph.node_opts::<u64, _>(
        vec![events.erased()],
        |ctx| {
            for event in ctx.batch::<WireBridgeEvent<TOutbound, TInbound>>(0) {
                if let WireBridgeEvent::Cursor { cursor } = event.as_ref() {
                    ctx.emit(*cursor);
                }
            }
        },
        GraphNodeOpts::named(format!("{name}/cursor")),
    )
}

fn project_attempts<TOutbound, TInbound>(
    graph: &Graph,
    events: &Node<WireBridgeEvent<TOutbound, TInbound>>,
    name: &str,
) -> Node<WireBridgeAttempt>
where
    TOutbound: Clone + 'static,
    TInbound: Clone + 'static,
{
    graph.node_opts::<WireBridgeAttempt, _>(
        vec![events.erased()],
        |ctx| {
            for event in ctx.batch::<WireBridgeEvent<TOutbound, TInbound>>(0) {
                if let WireBridgeEvent::Outbound { envelope } = event.as_ref() {
                    if should_track_ack(envelope.envelope_type) {
                        ctx.emit(WireBridgeAttempt {
                            seq: envelope.metadata.seq,
                            attempt: envelope.metadata.attempt,
                            max_attempts: envelope.metadata.max_attempts,
                        });
                    }
                }
            }
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
    use std::future::Future;
    use std::pin::Pin;

    use crate::environment::EnvironmentDrivers;
    use crate::graph::{graph, graph_opts, GraphOptions};

    type SleepCallback = Box<dyn FnOnce()>;
    type SleepQueue = Rc<RefCell<Vec<SleepCallback>>>;

    struct ManualAsyncDriver {
        sleeps: SleepQueue,
    }

    struct PanickingCancelDriver;

    impl ManualAsyncDriver {
        fn new() -> (Rc<Self>, SleepQueue) {
            let sleeps = Rc::new(RefCell::new(Vec::new()));
            (
                Rc::new(Self {
                    sleeps: sleeps.clone(),
                }),
                sleeps,
            )
        }
    }

    impl LocalAsyncDriver for ManualAsyncDriver {
        fn sleep(&self, _duration: Duration, callback: Box<dyn FnOnce()>) -> DriverCancel {
            self.sleeps.borrow_mut().push(callback);
            Box::new(|| {})
        }

        fn interval(&self, _period: Duration, _callback: Rc<dyn Fn()>) -> DriverCancel {
            Box::new(|| {})
        }

        fn spawn_local(&self, _fut: Pin<Box<dyn Future<Output = ()> + 'static>>) -> DriverCancel {
            Box::new(|| {})
        }
    }

    impl LocalAsyncDriver for PanickingCancelDriver {
        fn sleep(&self, _duration: Duration, _callback: Box<dyn FnOnce()>) -> DriverCancel {
            Box::new(|| panic!("cancel failed"))
        }

        fn interval(&self, _period: Duration, _callback: Rc<dyn Fn()>) -> DriverCancel {
            Box::new(|| {})
        }

        fn spawn_local(&self, _fut: Pin<Box<dyn Future<Output = ()> + 'static>>) -> DriverCancel {
            Box::new(|| {})
        }
    }

    fn env_with_manual_async() -> (EnvironmentDrivers, SleepQueue) {
        let (driver, sleeps) = ManualAsyncDriver::new();
        (
            EnvironmentDrivers::new().with_local_async(driver as Rc<dyn LocalAsyncDriver>),
            sleeps,
        )
    }

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
        assert_eq!(wire_bridge_idempotency_key("session-a", 7), "session-a:7");
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
        assert_eq!(env.metadata.idempotency_key, "session-a:7");
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
                    idempotency_key: "session-a:5".to_owned(),
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
        let snap = g.describe();
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
            ]
        );
        assert!(snap
            .edges
            .iter()
            .any(|edge| edge.from == "bridge/command" && edge.to == "bridge/events"));
        assert!(snap
            .edges
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
        let _events = bridge.events.subscribe(move |msg| {
            if let Message::Data(value) = msg {
                if let Some(event) = value.downcast_ref::<WireBridgeEvent<String, String>>() {
                    seen_sink.borrow_mut().push(event.clone());
                }
            }
        });

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
        bridge
            .inbound
            .down(vec![Message::Data(Rc::new("not-an-envelope".to_owned()))]);

        assert!(bridge
            .errors
            .cache()
            .unwrap()
            .contains("inbound DATA must carry"));
        assert!(bridge.outbound.cache().is_none());
    }

    #[test]
    fn inbound_vec_is_split_into_single_receipt_local_waves() {
        let g = graph();
        let bridge =
            wire_bridge::<String, String>(&g, WireBridgeOptions::named("session-a", "bridge"));
        let batch_sizes = Rc::new(RefCell::new(Vec::new()));
        let batch_sizes_sink = batch_sizes.clone();
        let observed = g.node_opts::<usize, _>(
            vec![bridge.inbound.erased()],
            move |ctx| {
                let len = ctx.batch::<WireBridgeIngress<String>>(0).len();
                batch_sizes_sink.borrow_mut().push(len);
                ctx.emit(len);
            },
            GraphNodeOpts::named("inbound-batch-sizes"),
        );
        let _observed = observed.subscribe(|_| {});

        bridge.inbound.down(vec![
            Message::Data(Rc::new(envelope(
                "session-a",
                WireBridgeEnvelopeType::Data,
                1,
                0,
                Some(WireBridgePayload::Data("one".to_owned())),
                None,
            ))),
            Message::Data(Rc::new(envelope(
                "session-a",
                WireBridgeEnvelopeType::Data,
                2,
                0,
                Some(WireBridgePayload::Data("two".to_owned())),
                None,
            ))),
        ]);

        assert_eq!(*batch_sizes.borrow(), vec![1, 1]);
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

        bridge
            .inbound
            .down(vec![Message::Error("remote protocol error".into())]);
        bridge.inbound.set(envelope(
            "session-a",
            WireBridgeEnvelopeType::Data,
            1,
            0,
            Some(WireBridgePayload::Data("still-live".to_owned())),
            None,
        ));

        assert_eq!(bridge.cursor.cache(), Some(1));
        assert_ne!(bridge.events.status(), crate::node::Status::Errored);
        assert_ne!(bridge.status.status(), crate::node::Status::Errored);
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
        assert_ne!(bridge.status.status(), crate::node::Status::Errored);

        bridge.close(None, None);
        let status = bridge.status.cache().unwrap();
        assert_eq!(status.state, WireBridgeStatusState::Closed);
        assert_eq!(status.pending, 1);
        assert_ne!(bridge.status.status(), crate::node::Status::Completed);

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
        assert_ne!(bridge.status.status(), crate::node::Status::Completed);
    }

    #[test]
    fn ack_timeout_retries_through_local_async_driver_when_enabled() {
        let (environment, sleeps) = env_with_manual_async();
        let g = graph_opts(GraphOptions {
            environment,
            ..GraphOptions::default()
        });
        let bridge = wire_bridge::<String, String>(
            &g,
            WireBridgeOptions {
                name: Some("bridge".to_owned()),
                session_id: "session-a".to_owned(),
                retry: RetryPolicy::new(
                    2,
                    crate::resilience::BackoffPolicy::Constant { delay_ms: 10 },
                ),
                ack_timeout: Some(Duration::from_millis(5)),
                now_ms: Some(Rc::new(|| 1000)),
            },
        );
        let _outbound = bridge.outbound.subscribe(|_| {});
        let _attempts = bridge.attempts.subscribe(|_| {});
        let _errors = bridge.errors.subscribe(|_| {});
        let _status = bridge.status.subscribe(|_| {});

        bridge.send("work".to_owned(), None, None);
        assert_eq!(bridge.attempts.cache().unwrap().attempt, 1);
        let timeout = sleeps.borrow_mut().pop().expect("ack timeout scheduled");
        timeout();
        assert_eq!(
            bridge.status.cache().unwrap().state,
            WireBridgeStatusState::Waiting
        );
        let retry_delay = sleeps.borrow_mut().pop().expect("retry delay scheduled");
        retry_delay();
        assert_eq!(bridge.attempts.cache().unwrap().attempt, 2);
        let second_timeout = sleeps.borrow_mut().pop().expect("second timeout scheduled");
        second_timeout();
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
    fn ack_cancel_panic_does_not_suppress_receipt_facts() {
        let g = graph_opts(GraphOptions {
            environment: EnvironmentDrivers::new()
                .with_local_async(Rc::new(PanickingCancelDriver) as Rc<dyn LocalAsyncDriver>),
            ..GraphOptions::default()
        });
        let bridge = wire_bridge::<String, String>(
            &g,
            WireBridgeOptions {
                name: Some("bridge".to_owned()),
                session_id: "session-a".to_owned(),
                ack_timeout: Some(Duration::from_millis(5)),
                ..WireBridgeOptions::new("session-a")
            },
        );
        let _acks = bridge.acks.subscribe(|_| {});
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

        assert_eq!(bridge.acks.cache().unwrap().ack_for_seq, 1);
        let status = bridge.status.cache().unwrap();
        assert_eq!(status.pending, 0);
        assert_eq!(status.acked, 1);
        assert_eq!(status.last_seq, Some(1));
    }

    #[test]
    fn ack_timeout_missing_async_driver_clears_pending_as_error_fact() {
        let g = graph();
        let bridge = wire_bridge::<String, String>(
            &g,
            WireBridgeOptions {
                name: Some("bridge".to_owned()),
                session_id: "session-a".to_owned(),
                ack_timeout: Some(Duration::from_millis(5)),
                ..WireBridgeOptions::new("session-a")
            },
        );
        let _status = bridge.status.subscribe(|_| {});
        let _errors = bridge.errors.subscribe(|_| {});

        bridge.send("work".to_owned(), None, None);

        let status = bridge.status.cache().unwrap();
        assert_eq!(status.state, WireBridgeStatusState::Exhausted);
        assert_eq!(status.pending, 0);
        assert_eq!(
            bridge.errors.cache(),
            Some("wireBridge: missing LocalAsyncDriver for ack timeout".to_owned())
        );
    }
}
