//! Graph-visible outbound environment adapters (D132).
//!
//! Transport work stays behind graph-local EnvironmentDrivers. These helpers
//! expose attempt/status/error facts through declared graph deps.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::Duration;

use crate::async_driver::{DriverCancel, LocalAsyncDriver};
use crate::ctx::{Ctx, DeferredCtx, DepTerminal};
use crate::environment::{
    HttpRequest, HttpResponse, LocalWebSocketSession, ProcessCommand, ProcessResult,
    WebSocketDriverEvent, WebSocketEvent, WebSocketRequest, WebSocketSend, WebSocketSendResult,
};
use crate::graph::{Graph, GraphNodeOpts};
use crate::node::Node;
use crate::protocol::{GraphError, Message};
use crate::resilience::RetryPolicy;

type CancelSlot = Rc<RefCell<Option<DriverCancel>>>;
type CancelSlots = Rc<RefCell<Vec<CancelSlot>>>;
type OutboundSend<T, R> =
    Rc<dyn Fn(T, Box<dyn FnOnce(Result<R, GraphError>)>) -> Option<DriverCancel>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundEvent<T, R> {
    Attempt {
        value: T,
        attempt: u32,
    },
    Retry {
        value: T,
        attempt: u32,
        delay_ms: u64,
        error: String,
    },
    Sent {
        value: T,
        attempt: u32,
        result: R,
    },
    Exhausted {
        value: T,
        attempt: u32,
        error: String,
    },
    UpstreamComplete,
    UpstreamError {
        error: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundState {
    Idle,
    Running,
    Waiting,
    Succeeded,
    Exhausted,
    Failed,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundStatus {
    pub state: OutboundState,
    pub in_flight: u32,
    pub attempt: u32,
    pub sent: u64,
    pub failed: u64,
    pub last_delay_ms: Option<u64>,
}

impl Default for OutboundStatus {
    fn default() -> Self {
        Self {
            state: OutboundState::Idle,
            in_flight: 0,
            attempt: 0,
            sent: 0,
            failed: 0,
            last_delay_ms: None,
        }
    }
}

pub struct OutboundBundle<T: 'static, R: 'static> {
    pub events: Node<OutboundEvent<T, R>>,
    pub status: Node<OutboundStatus>,
    pub attempts: Node<u32>,
    pub errors: Node<String>,
}

#[derive(Clone, Default)]
pub struct OutboundAdapterOptions {
    pub name: Option<String>,
    pub retry: RetryPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebSocketSessionCommand {
    Start,
    Send(WebSocketSend),
    Close {
        code: Option<u16>,
        reason: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebSocketSessionInbound {
    Text(String),
    Binary(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebSocketSessionLifecycle {
    Starting {
        attempt: u32,
        max_attempts: u32,
    },
    Open {
        attempt: u32,
    },
    Sent {
        message: WebSocketSend,
    },
    Closing {
        code: Option<u16>,
        reason: Option<String>,
    },
    Closed {
        code: Option<u16>,
        reason: Option<String>,
    },
    Retrying {
        attempt: u32,
        next_attempt: u32,
        delay_ms: u64,
        error: String,
    },
    Exhausted {
        attempt: u32,
        error: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebSocketSessionStateKind {
    Idle,
    Connecting,
    Open,
    Closing,
    Closed,
    Waiting,
    Exhausted,
    Errored,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebSocketSessionStatus {
    pub state: WebSocketSessionStateKind,
    pub attempt: u32,
    pub max_attempts: u32,
    pub sent: u64,
    pub received: u64,
    pub errors: u64,
    pub last_delay_ms: Option<u64>,
}

pub struct WebSocketSessionBundle {
    pub command: Node<WebSocketSessionCommand>,
    pub inbound: Node<WebSocketSessionInbound>,
    pub lifecycle: Node<WebSocketSessionLifecycle>,
    pub status: Node<WebSocketSessionStatus>,
    pub errors: Node<String>,
    pub attempts: Node<u32>,
}

impl WebSocketSessionBundle {
    pub fn start(&self) {
        self.command.set(WebSocketSessionCommand::Start);
    }

    pub fn send(&self, message: WebSocketSend) {
        self.command.set(WebSocketSessionCommand::Send(message));
    }

    pub fn send_text(&self, text: impl Into<String>) {
        self.send(WebSocketSend::text(text));
    }

    pub fn send_binary(&self, bytes: impl Into<Vec<u8>>) {
        self.send(WebSocketSend::binary(bytes));
    }

    pub fn close(&self, code: Option<u16>, reason: Option<String>) {
        self.command
            .set(WebSocketSessionCommand::Close { code, reason });
    }
}

#[derive(Clone, Default)]
pub struct WebSocketSessionOptions {
    pub name: Option<String>,
    pub retry: RetryPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum WebSocketSessionEvent {
    Attempt {
        attempt: u32,
        max_attempts: u32,
    },
    Open {
        attempt: u32,
    },
    Message(WebSocketSessionInbound),
    Sent {
        message: WebSocketSend,
    },
    Closing {
        code: Option<u16>,
        reason: Option<String>,
    },
    Closed {
        code: Option<u16>,
        reason: Option<String>,
    },
    Retry {
        attempt: u32,
        next_attempt: u32,
        delay_ms: u64,
        error: String,
    },
    Error {
        attempt: Option<u32>,
        error: String,
    },
    Exhausted {
        attempt: u32,
        error: String,
    },
}

#[derive(Clone)]
struct WebSocketSessionStateCell(Rc<RefCell<WebSocketSessionState>>);

struct WebSocketSessionState {
    node_active: bool,
    connected: bool,
    waiting_retry: bool,
    current_attempt: u32,
    current_session: Option<Rc<dyn LocalWebSocketSession>>,
    next_send_id: u64,
    send_cancels: Vec<WebSocketSendCancel>,
    retry_cancels: Vec<CancelSlot>,
}

#[derive(Clone)]
struct WebSocketSendCancel {
    id: u64,
    slot: CancelSlot,
}

pub fn websocket_session(graph: &Graph, request: WebSocketRequest) -> WebSocketSessionBundle {
    websocket_session_with_options(graph, request, WebSocketSessionOptions::default())
}

pub fn websocket_session_with_options(
    graph: &Graph,
    request: WebSocketRequest,
    opts: WebSocketSessionOptions,
) -> WebSocketSessionBundle {
    assert!(
        !request.url.is_empty(),
        "websocket_session: url must be non-empty"
    );
    let name = opts
        .name
        .clone()
        .unwrap_or_else(|| "websocketSession".to_owned());
    let command = graph.state_empty_opts::<WebSocketSessionCommand>(GraphNodeOpts::named(format!(
        "{name}/command"
    )));
    let events =
        websocket_session_events(graph, &command, request, name.clone(), opts.retry.clone());
    websocket_session_nodes(graph, command, events, name, opts.retry)
}

fn websocket_session_events(
    graph: &Graph,
    command: &Node<WebSocketSessionCommand>,
    request: WebSocketRequest,
    name: String,
    policy: RetryPolicy,
) -> Node<WebSocketSessionEvent> {
    let events_name = name.clone();
    graph.node_opts::<WebSocketSessionEvent, _>(
        vec![command.erased()],
        move |ctx| {
            let state = init_websocket_session_state(ctx);
            let out = Rc::new(ctx.defer());
            let driver = ctx.environment().websocket_driver();
            let async_driver = ctx.local_async_driver();
            for command in ctx.batch::<WebSocketSessionCommand>(0) {
                match command.as_ref() {
                    WebSocketSessionCommand::Start => {
                        start_websocket_session_attempt(WebSocketSessionStart {
                            state: state.clone(),
                            out: out.clone(),
                            driver: driver.clone(),
                            async_driver: async_driver.clone(),
                            request: request.clone(),
                            name: name.clone(),
                            policy: policy.clone(),
                            attempt: 1,
                        });
                    }
                    WebSocketSessionCommand::Send(message) => {
                        send_websocket_session_message(&state, &out, &name, message.clone());
                    }
                    WebSocketSessionCommand::Close { code, reason } => {
                        cancel_websocket_retry_timers(&state);
                        out.emit(WebSocketSessionEvent::Closing {
                            code: *code,
                            reason: reason.clone(),
                        });
                        if let Some(session) = state.0.borrow().current_session.clone() {
                            session.close(*code, reason.clone());
                        }
                        close_websocket_session_connection(&state, false);
                        out.emit(WebSocketSessionEvent::Closed {
                            code: *code,
                            reason: reason.clone(),
                        });
                    }
                }
            }
        },
        GraphNodeOpts::named(format!("{events_name}/events")),
    )
}

struct WebSocketSessionStart {
    state: WebSocketSessionStateCell,
    out: Rc<DeferredCtx>,
    driver: Option<Rc<dyn crate::environment::LocalWebSocketDriver>>,
    async_driver: Option<Rc<dyn LocalAsyncDriver>>,
    request: WebSocketRequest,
    name: String,
    policy: RetryPolicy,
    attempt: u32,
}

fn start_websocket_session_attempt(args: WebSocketSessionStart) {
    {
        let mut state = args.state.0.borrow_mut();
        if !state.node_active
            || state.connected
            || state.current_attempt != 0
            || state.waiting_retry
        {
            return;
        }
        state.current_attempt = args.attempt;
    }
    args.out.emit(WebSocketSessionEvent::Attempt {
        attempt: args.attempt,
        max_attempts: args.policy.max_attempts,
    });
    let Some(driver) = args.driver.clone() else {
        exhaust_websocket_session(
            &args.state,
            &args.out,
            args.attempt,
            format!("{}: missing websocket driver", args.name),
        );
        return;
    };
    let state_for_callback = args.state.clone();
    let out_for_callback = args.out.clone();
    let name_for_callback = args.name.clone();
    let policy_for_callback = args.policy.clone();
    let request_for_callback = args.request.clone();
    let driver_for_callback = args.driver.clone();
    let async_for_callback = args.async_driver.clone();
    let session = driver.connect_session(
        args.request.clone(),
        Rc::new(move |event| {
            if !is_current_websocket_attempt(&state_for_callback, args.attempt) {
                return;
            }
            match event {
                WebSocketDriverEvent::Event(WebSocketEvent::Open) => {
                    state_for_callback.0.borrow_mut().connected = true;
                    out_for_callback.emit(WebSocketSessionEvent::Open {
                        attempt: args.attempt,
                    });
                }
                WebSocketDriverEvent::Event(WebSocketEvent::Text(text)) => {
                    out_for_callback.emit(WebSocketSessionEvent::Message(
                        WebSocketSessionInbound::Text(text),
                    ));
                }
                WebSocketDriverEvent::Event(WebSocketEvent::Binary(bytes)) => {
                    out_for_callback.emit(WebSocketSessionEvent::Message(
                        WebSocketSessionInbound::Binary(bytes),
                    ));
                }
                WebSocketDriverEvent::Event(WebSocketEvent::Close { code, reason }) => {
                    let normal_close = code == Some(1000);
                    close_websocket_session_connection(&state_for_callback, false);
                    out_for_callback.emit(WebSocketSessionEvent::Closed { code, reason });
                    if !normal_close {
                        retry_or_exhaust_websocket_session(WebSocketSessionRetry {
                            state: state_for_callback.clone(),
                            out: out_for_callback.clone(),
                            driver: driver_for_callback.clone(),
                            async_driver: async_for_callback.clone(),
                            request: request_for_callback.clone(),
                            name: name_for_callback.clone(),
                            policy: policy_for_callback.clone(),
                            attempt: args.attempt,
                            error: format!("{}: websocket closed", name_for_callback),
                        });
                    }
                }
                WebSocketDriverEvent::Error(error) => {
                    retry_or_exhaust_websocket_session(WebSocketSessionRetry {
                        state: state_for_callback.clone(),
                        out: out_for_callback.clone(),
                        driver: driver_for_callback.clone(),
                        async_driver: async_for_callback.clone(),
                        request: request_for_callback.clone(),
                        name: name_for_callback.clone(),
                        policy: policy_for_callback.clone(),
                        attempt: args.attempt,
                        error: error.to_string(),
                    });
                }
                WebSocketDriverEvent::Complete => {
                    retry_or_exhaust_websocket_session(WebSocketSessionRetry {
                        state: state_for_callback.clone(),
                        out: out_for_callback.clone(),
                        driver: driver_for_callback.clone(),
                        async_driver: async_for_callback.clone(),
                        request: request_for_callback.clone(),
                        name: name_for_callback.clone(),
                        policy: policy_for_callback.clone(),
                        attempt: args.attempt,
                        error: format!("{}: connection completed", name_for_callback),
                    });
                }
            }
        }),
    );
    match session {
        Some(session) => {
            if is_current_websocket_attempt(&args.state, args.attempt) {
                args.state.0.borrow_mut().current_session = Some(session);
            } else {
                session.cancel();
            }
        }
        None => {
            exhaust_websocket_session(
                &args.state,
                &args.out,
                args.attempt,
                format!("{}: missing WebSocket session capability", args.name),
            );
        }
    }
}

struct WebSocketSessionRetry {
    state: WebSocketSessionStateCell,
    out: Rc<DeferredCtx>,
    driver: Option<Rc<dyn crate::environment::LocalWebSocketDriver>>,
    async_driver: Option<Rc<dyn LocalAsyncDriver>>,
    request: WebSocketRequest,
    name: String,
    policy: RetryPolicy,
    attempt: u32,
    error: String,
}

fn retry_or_exhaust_websocket_session(args: WebSocketSessionRetry) {
    close_websocket_session_connection(&args.state, true);
    if !args.policy.should_retry(args.attempt) {
        exhaust_websocket_session(&args.state, &args.out, args.attempt, args.error);
        return;
    }
    let next_attempt = args.attempt.saturating_add(1);
    let delay_ms = args.policy.next_delay_ms(next_attempt).unwrap_or_default();
    args.out.emit(WebSocketSessionEvent::Retry {
        attempt: args.attempt,
        next_attempt,
        delay_ms,
        error: args.error,
    });
    if delay_ms == 0 {
        start_websocket_session_attempt(WebSocketSessionStart {
            state: args.state,
            out: args.out,
            driver: args.driver,
            async_driver: args.async_driver,
            request: args.request,
            name: args.name,
            policy: args.policy,
            attempt: next_attempt,
        });
        return;
    }
    let Some(async_driver) = args.async_driver.clone() else {
        exhaust_websocket_session(
            &args.state,
            &args.out,
            args.attempt,
            format!("{}: missing async driver for delayed reconnect", args.name),
        );
        return;
    };
    let slot: CancelSlot = Rc::new(RefCell::new(None));
    let wake_slot = slot.clone();
    let state = args.state.clone();
    let state_for_slot = args.state.clone();
    args.state.0.borrow_mut().waiting_retry = true;
    let cancel = async_driver.sleep(
        Duration::from_millis(delay_ms),
        Box::new(move || {
            let _ = wake_slot.borrow_mut().take();
            state.0.borrow_mut().waiting_retry = false;
            start_websocket_session_attempt(WebSocketSessionStart {
                state,
                out: args.out,
                driver: args.driver,
                async_driver: args.async_driver,
                request: args.request,
                name: args.name,
                policy: args.policy,
                attempt: next_attempt,
            });
        }),
    );
    *slot.borrow_mut() = Some(cancel);
    state_for_slot.0.borrow_mut().retry_cancels.push(slot);
}

fn send_websocket_session_message(
    state: &WebSocketSessionStateCell,
    out: &Rc<DeferredCtx>,
    name: &str,
    message: WebSocketSend,
) {
    let session = {
        let state_ref = state.0.borrow();
        if !state_ref.connected {
            out.emit(WebSocketSessionEvent::Error {
                attempt: (state_ref.current_attempt > 0).then_some(state_ref.current_attempt),
                error: format!("{name}: session is not open"),
            });
            return;
        }
        state_ref.current_session.clone()
    };
    let Some(session) = session else {
        out.emit(WebSocketSessionEvent::Error {
            attempt: None,
            error: format!("{name}: missing WebSocket session capability"),
        });
        return;
    };
    let state_for_callback = state.clone();
    let out_for_callback = out.clone();
    let message_for_callback = message.clone();
    let send_id = {
        let mut state_ref = state.0.borrow_mut();
        let send_id = state_ref.next_send_id;
        state_ref.next_send_id = state_ref.next_send_id.saturating_add(1);
        send_id
    };
    let send_active = Rc::new(Cell::new(true));
    let send_active_for_callback = send_active.clone();
    let cancel = session.send(
        message,
        Box::new(move |result| {
            if !send_active_for_callback.get() || !state_for_callback.0.borrow().node_active {
                return;
            }
            send_active_for_callback.set(false);
            remove_websocket_send_cancel(&state_for_callback, send_id);
            match result {
                Ok(_) => out_for_callback.emit(WebSocketSessionEvent::Sent {
                    message: message_for_callback,
                }),
                Err(error) => out_for_callback.emit(WebSocketSessionEvent::Error {
                    attempt: (state_for_callback.0.borrow().current_attempt > 0)
                        .then_some(state_for_callback.0.borrow().current_attempt),
                    error: error.to_string(),
                }),
            }
        }),
    );
    let send_active_for_cancel = send_active.clone();
    let slot: CancelSlot = Rc::new(RefCell::new(Some(Box::new(move || {
        send_active_for_cancel.set(false);
        cancel();
    }))));
    if send_active.get() && slot.borrow().is_some() {
        state
            .0
            .borrow_mut()
            .send_cancels
            .push(WebSocketSendCancel { id: send_id, slot });
    }
}

fn websocket_session_nodes(
    graph: &Graph,
    command: Node<WebSocketSessionCommand>,
    events: Node<WebSocketSessionEvent>,
    name: String,
    policy: RetryPolicy,
) -> WebSocketSessionBundle {
    let inbound = graph.node_opts::<WebSocketSessionInbound, _>(
        vec![events.erased()],
        move |ctx| {
            for event in ctx.batch::<WebSocketSessionEvent>(0) {
                if let WebSocketSessionEvent::Message(message) = event.as_ref() {
                    ctx.emit(message.clone());
                }
            }
        },
        GraphNodeOpts::named(format!("{name}/inbound")),
    );
    let lifecycle = graph.node_opts::<WebSocketSessionLifecycle, _>(
        vec![events.erased()],
        move |ctx| {
            for event in ctx.batch::<WebSocketSessionEvent>(0) {
                match event.as_ref() {
                    WebSocketSessionEvent::Attempt {
                        attempt,
                        max_attempts,
                    } => ctx.emit(WebSocketSessionLifecycle::Starting {
                        attempt: *attempt,
                        max_attempts: *max_attempts,
                    }),
                    WebSocketSessionEvent::Open { attempt } => {
                        ctx.emit(WebSocketSessionLifecycle::Open { attempt: *attempt });
                    }
                    WebSocketSessionEvent::Sent { message } => {
                        ctx.emit(WebSocketSessionLifecycle::Sent {
                            message: message.clone(),
                        });
                    }
                    WebSocketSessionEvent::Closing { code, reason } => {
                        ctx.emit(WebSocketSessionLifecycle::Closing {
                            code: *code,
                            reason: reason.clone(),
                        });
                    }
                    WebSocketSessionEvent::Closed { code, reason } => {
                        ctx.emit(WebSocketSessionLifecycle::Closed {
                            code: *code,
                            reason: reason.clone(),
                        });
                    }
                    WebSocketSessionEvent::Retry {
                        attempt,
                        next_attempt,
                        delay_ms,
                        error,
                    } => ctx.emit(WebSocketSessionLifecycle::Retrying {
                        attempt: *attempt,
                        next_attempt: *next_attempt,
                        delay_ms: *delay_ms,
                        error: error.clone(),
                    }),
                    WebSocketSessionEvent::Exhausted { attempt, error } => {
                        ctx.emit(WebSocketSessionLifecycle::Exhausted {
                            attempt: *attempt,
                            error: error.clone(),
                        });
                    }
                    WebSocketSessionEvent::Message(_) | WebSocketSessionEvent::Error { .. } => {}
                }
            }
        },
        GraphNodeOpts::named(format!("{name}/lifecycle")),
    );
    let status = graph.node_opts::<WebSocketSessionStatus, _>(
        vec![events.erased()],
        move |ctx| {
            let mut next = ctx.state_get::<WebSocketSessionStatus>().map_or_else(
                || WebSocketSessionStatus {
                    state: WebSocketSessionStateKind::Idle,
                    attempt: 0,
                    max_attempts: policy.max_attempts,
                    sent: 0,
                    received: 0,
                    errors: 0,
                    last_delay_ms: None,
                },
                |status| (*status).clone(),
            );
            for event in ctx.batch::<WebSocketSessionEvent>(0) {
                next = reduce_websocket_session_status(&next, event.as_ref());
            }
            ctx.state_set(next.clone());
            ctx.emit(next);
        },
        GraphNodeOpts::named(format!("{name}/status")),
    );
    let errors = graph.node_opts::<String, _>(
        vec![events.erased()],
        move |ctx| {
            for event in ctx.batch::<WebSocketSessionEvent>(0) {
                match event.as_ref() {
                    WebSocketSessionEvent::Retry { error, .. }
                    | WebSocketSessionEvent::Error { error, .. }
                    | WebSocketSessionEvent::Exhausted { error, .. } => ctx.emit(error.clone()),
                    WebSocketSessionEvent::Attempt { .. }
                    | WebSocketSessionEvent::Open { .. }
                    | WebSocketSessionEvent::Message(_)
                    | WebSocketSessionEvent::Sent { .. }
                    | WebSocketSessionEvent::Closing { .. }
                    | WebSocketSessionEvent::Closed { .. } => {}
                }
            }
        },
        GraphNodeOpts::named(format!("{name}/errors")),
    );
    let attempts = graph.node_opts::<u32, _>(
        vec![events.erased()],
        move |ctx| {
            for event in ctx.batch::<WebSocketSessionEvent>(0) {
                if let WebSocketSessionEvent::Attempt { attempt, .. } = event.as_ref() {
                    ctx.emit(*attempt);
                }
            }
        },
        GraphNodeOpts::named(format!("{name}/attempts")),
    );
    WebSocketSessionBundle {
        command,
        inbound,
        lifecycle,
        status,
        errors,
        attempts,
    }
}

fn reduce_websocket_session_status(
    current: &WebSocketSessionStatus,
    event: &WebSocketSessionEvent,
) -> WebSocketSessionStatus {
    match event {
        WebSocketSessionEvent::Attempt {
            attempt,
            max_attempts,
        } => WebSocketSessionStatus {
            state: WebSocketSessionStateKind::Connecting,
            attempt: *attempt,
            max_attempts: *max_attempts,
            last_delay_ms: None,
            ..current.clone()
        },
        WebSocketSessionEvent::Open { attempt } => WebSocketSessionStatus {
            state: WebSocketSessionStateKind::Open,
            attempt: *attempt,
            last_delay_ms: None,
            ..current.clone()
        },
        WebSocketSessionEvent::Message(_) => WebSocketSessionStatus {
            received: current.received.saturating_add(1),
            ..current.clone()
        },
        WebSocketSessionEvent::Sent { .. } => WebSocketSessionStatus {
            sent: current.sent.saturating_add(1),
            ..current.clone()
        },
        WebSocketSessionEvent::Closing { .. } => WebSocketSessionStatus {
            state: WebSocketSessionStateKind::Closing,
            ..current.clone()
        },
        WebSocketSessionEvent::Closed { .. } => WebSocketSessionStatus {
            state: WebSocketSessionStateKind::Closed,
            ..current.clone()
        },
        WebSocketSessionEvent::Retry {
            attempt, delay_ms, ..
        } => WebSocketSessionStatus {
            state: WebSocketSessionStateKind::Waiting,
            attempt: *attempt,
            errors: current.errors.saturating_add(1),
            last_delay_ms: Some(*delay_ms),
            ..current.clone()
        },
        WebSocketSessionEvent::Error { attempt, .. } => WebSocketSessionStatus {
            state: WebSocketSessionStateKind::Errored,
            attempt: attempt.unwrap_or(current.attempt),
            errors: current.errors.saturating_add(1),
            ..current.clone()
        },
        WebSocketSessionEvent::Exhausted { attempt, .. } => WebSocketSessionStatus {
            state: WebSocketSessionStateKind::Exhausted,
            attempt: *attempt,
            errors: current.errors.saturating_add(1),
            ..current.clone()
        },
    }
}

fn init_websocket_session_state(ctx: &Ctx) -> WebSocketSessionStateCell {
    if let Some(state) = ctx.state_get::<WebSocketSessionStateCell>() {
        let state = (*state).clone();
        state.0.borrow_mut().node_active = true;
        return state;
    }
    let state = WebSocketSessionStateCell(Rc::new(RefCell::new(WebSocketSessionState {
        node_active: true,
        connected: false,
        waiting_retry: false,
        current_attempt: 0,
        current_session: None,
        next_send_id: 0,
        send_cancels: Vec::new(),
        retry_cancels: Vec::new(),
    })));
    let cleanup_state = state.clone();
    ctx.on_deactivation(move || {
        deactivate_websocket_session_state(&cleanup_state);
    });
    ctx.state_set(state.clone());
    state
}

fn is_current_websocket_attempt(state: &WebSocketSessionStateCell, attempt: u32) -> bool {
    let state = state.0.borrow();
    state.node_active && state.current_attempt == attempt
}

fn exhaust_websocket_session(
    state: &WebSocketSessionStateCell,
    out: &DeferredCtx,
    attempt: u32,
    error: String,
) {
    close_websocket_session_connection(state, true);
    out.emit(WebSocketSessionEvent::Exhausted { attempt, error });
}

fn close_websocket_session_connection(state: &WebSocketSessionStateCell, cancel_session: bool) {
    let mut state = state.0.borrow_mut();
    state.connected = false;
    state.current_attempt = 0;
    if cancel_session {
        if let Some(session) = state.current_session.take() {
            session.cancel();
        }
    } else {
        state.current_session = None;
    }
    for send in state.send_cancels.drain(..) {
        if let Some(cancel) = send.slot.borrow_mut().take() {
            cancel();
        }
    }
}

fn remove_websocket_send_cancel(state: &WebSocketSessionStateCell, send_id: u64) {
    let mut state = state.0.borrow_mut();
    if let Some(index) = state
        .send_cancels
        .iter()
        .position(|send| send.id == send_id)
    {
        let send = state.send_cancels.remove(index);
        let _ = send.slot.borrow_mut().take();
    }
}

fn cancel_websocket_retry_timers(state: &WebSocketSessionStateCell) {
    let mut state = state.0.borrow_mut();
    state.waiting_retry = false;
    for slot in state.retry_cancels.drain(..) {
        if let Some(cancel) = slot.borrow_mut().take() {
            cancel();
        }
    }
}

fn deactivate_websocket_session_state(state: &WebSocketSessionStateCell) {
    {
        let mut state_ref = state.0.borrow_mut();
        state_ref.node_active = false;
    }
    cancel_websocket_retry_timers(state);
    close_websocket_session_connection(state, true);
}

pub fn to_http<T, F>(
    graph: &Graph,
    source: &Node<T>,
    request_of: F,
) -> OutboundBundle<T, HttpResponse>
where
    T: Clone + 'static,
    F: Fn(&T) -> HttpRequest + 'static,
{
    to_http_with_options(graph, source, request_of, OutboundAdapterOptions::default())
}

pub fn to_http_with_options<T, F>(
    graph: &Graph,
    source: &Node<T>,
    request_of: F,
    opts: OutboundAdapterOptions,
) -> OutboundBundle<T, HttpResponse>
where
    T: Clone + 'static,
    F: Fn(&T) -> HttpRequest + 'static,
{
    let name = opts.name.clone().unwrap_or_else(|| "toHttp".to_owned());
    let request_of = Rc::new(request_of);
    let events = outbound_node(graph, source, name.clone(), opts.retry, move |ctx| {
        let driver = ctx.environment().http_driver()?;
        let request_of = request_of.clone();
        Some(Rc::new(move |value: T, callback| {
            let request = request_of(&value);
            Some(driver.request(request, callback))
        }) as OutboundSend<T, HttpResponse>)
    });
    outbound_bundle(graph, events, name)
}

pub fn to_process<T, F>(
    graph: &Graph,
    source: &Node<T>,
    command_of: F,
) -> OutboundBundle<T, ProcessResult>
where
    T: Clone + 'static,
    F: Fn(&T) -> ProcessCommand + 'static,
{
    to_process_with_options(graph, source, command_of, OutboundAdapterOptions::default())
}

pub fn to_process_with_options<T, F>(
    graph: &Graph,
    source: &Node<T>,
    command_of: F,
    opts: OutboundAdapterOptions,
) -> OutboundBundle<T, ProcessResult>
where
    T: Clone + 'static,
    F: Fn(&T) -> ProcessCommand + 'static,
{
    let name = opts.name.clone().unwrap_or_else(|| "toProcess".to_owned());
    let command_of = Rc::new(command_of);
    let events = outbound_node(graph, source, name.clone(), opts.retry, move |ctx| {
        let driver = ctx.environment().process_driver()?;
        let command_of = command_of.clone();
        Some(Rc::new(move |value: T, callback| {
            let command = command_of(&value);
            Some(driver.run(command, callback))
        }) as OutboundSend<T, ProcessResult>)
    });
    outbound_bundle(graph, events, name)
}

pub fn to_websocket<T, F>(
    graph: &Graph,
    source: &Node<T>,
    request: WebSocketRequest,
    send_of: F,
) -> OutboundBundle<T, WebSocketSendResult>
where
    T: Clone + 'static,
    F: Fn(&T) -> WebSocketSend + 'static,
{
    to_websocket_with_options(
        graph,
        source,
        request,
        send_of,
        OutboundAdapterOptions::default(),
    )
}

pub fn to_websocket_with_options<T, F>(
    graph: &Graph,
    source: &Node<T>,
    request: WebSocketRequest,
    send_of: F,
    opts: OutboundAdapterOptions,
) -> OutboundBundle<T, WebSocketSendResult>
where
    T: Clone + 'static,
    F: Fn(&T) -> WebSocketSend + 'static,
{
    let name = opts
        .name
        .clone()
        .unwrap_or_else(|| "toWebSocket".to_owned());
    let request = Rc::new(request);
    let send_of = Rc::new(send_of);
    let events = outbound_node(graph, source, name.clone(), opts.retry, move |ctx| {
        let driver = ctx.environment().websocket_driver()?;
        let request = request.clone();
        let send_of = send_of.clone();
        Some(Rc::new(move |value: T, callback| {
            let message = send_of(&value);
            driver.send((*request).clone(), message, callback)
        }) as OutboundSend<T, WebSocketSendResult>)
    });
    outbound_bundle(graph, events, name)
}

fn outbound_node<T, R, S>(
    graph: &Graph,
    source: &Node<T>,
    name: String,
    policy: RetryPolicy,
    send: S,
) -> Node<OutboundEvent<T, R>>
where
    T: Clone + 'static,
    R: Clone + 'static,
    S: Fn(&Ctx) -> Option<OutboundSend<T, R>> + 'static,
{
    let missing_driver_name = name.clone();
    graph.node_opts::<OutboundEvent<T, R>, _>(
        vec![source.erased()],
        move |ctx| {
            let Some(send) = send(ctx) else {
                ctx.down(vec![Message::Error(
                    format!("{missing_driver_name}: missing environment driver").into(),
                )]);
                return;
            };
            let active = Rc::new(Cell::new(true));
            let cancels = Rc::new(RefCell::new(Vec::<CancelSlot>::new()));
            let cleanup_active = active.clone();
            let cleanup_cancels = cancels.clone();
            ctx.on_deactivation(move || {
                cleanup_active.set(false);
                for slot in cleanup_cancels.borrow_mut().drain(..) {
                    if let Some(cancel) = slot.borrow_mut().take() {
                        cancel();
                    }
                }
            });
            let out = Rc::new(ctx.defer());
            let async_driver = ctx.local_async_driver();
            for value in ctx.batch::<T>(0) {
                start_attempt(StartArgs {
                    value: (*value).clone(),
                    attempt: 1,
                    active: active.clone(),
                    out: out.clone(),
                    cancels: cancels.clone(),
                    policy: policy.clone(),
                    async_driver: async_driver.clone(),
                    send: send.clone(),
                });
            }
            match ctx.terminal(0) {
                Some(DepTerminal::Complete) => ctx.emit(OutboundEvent::<T, R>::UpstreamComplete),
                Some(DepTerminal::Error(error)) => ctx.emit(OutboundEvent::<T, R>::UpstreamError {
                    error: error.to_string(),
                }),
                None => {}
            }
        },
        GraphNodeOpts::named(name),
    )
}

struct StartArgs<T: Clone + 'static, R: Clone + 'static> {
    value: T,
    attempt: u32,
    active: Rc<Cell<bool>>,
    out: Rc<DeferredCtx>,
    cancels: CancelSlots,
    policy: RetryPolicy,
    async_driver: Option<Rc<dyn LocalAsyncDriver>>,
    send: OutboundSend<T, R>,
}

fn start_attempt<T, R>(args: StartArgs<T, R>)
where
    T: Clone + 'static,
    R: Clone + 'static,
{
    if !args.active.get() {
        return;
    }
    args.out.emit(OutboundEvent::<T, R>::Attempt {
        value: args.value.clone(),
        attempt: args.attempt,
    });
    let send_value = args.value.clone();
    let cancel_slot: CancelSlot = Rc::new(RefCell::new(None));
    let done = Rc::new(Cell::new(false));
    let callback_args = StartArgs {
        value: args.value.clone(),
        attempt: args.attempt,
        active: args.active.clone(),
        out: args.out.clone(),
        cancels: args.cancels.clone(),
        policy: args.policy.clone(),
        async_driver: args.async_driver.clone(),
        send: args.send.clone(),
    };
    let out = args.out.clone();
    let callback_slot = cancel_slot.clone();
    let callback_done = done.clone();
    let cancel = (args.send)(
        send_value,
        Box::new(move |result| {
            callback_done.set(true);
            let _ = callback_slot.borrow_mut().take();
            if !callback_args.active.get() {
                return;
            }
            match result {
                Ok(result) => callback_args.out.emit(OutboundEvent::<T, R>::Sent {
                    value: callback_args.value,
                    attempt: callback_args.attempt,
                    result,
                }),
                Err(error) => {
                    let error = error.to_string();
                    if callback_args.policy.should_retry(callback_args.attempt) {
                        let next_attempt = callback_args.attempt.saturating_add(1);
                        let delay_ms = callback_args
                            .policy
                            .next_delay_ms(next_attempt)
                            .unwrap_or_default();
                        if delay_ms > 0 && callback_args.async_driver.is_none() {
                            let scheduler_error =
                                "outbound adapter: missing async driver for delayed retry"
                                    .to_owned();
                            callback_args.out.emit(OutboundEvent::<T, R>::Exhausted {
                                value: callback_args.value,
                                attempt: callback_args.attempt,
                                error: scheduler_error.clone(),
                            });
                            callback_args
                                .out
                                .down(vec![Message::Error(scheduler_error.into())]);
                            return;
                        }
                        let active = callback_args.active.clone();
                        let out = callback_args.out.clone();
                        let cancels = callback_args.cancels.clone();
                        let policy = callback_args.policy.clone();
                        let async_driver = callback_args.async_driver.clone();
                        let send = callback_args.send.clone();
                        callback_args.out.emit(OutboundEvent::<T, R>::Retry {
                            value: callback_args.value.clone(),
                            attempt: callback_args.attempt,
                            delay_ms,
                            error: error.clone(),
                        });
                        let next_args = StartArgs {
                            value: callback_args.value,
                            attempt: next_attempt,
                            active,
                            out,
                            cancels: cancels.clone(),
                            policy,
                            async_driver: async_driver.clone(),
                            send,
                        };
                        if delay_ms == 0 {
                            start_attempt(next_args);
                        } else if let Some(driver) = async_driver {
                            let sleep_slot: CancelSlot = Rc::new(RefCell::new(None));
                            let wake_slot = sleep_slot.clone();
                            let cancel = driver.sleep(
                                Duration::from_millis(delay_ms),
                                Box::new(move || {
                                    let _ = wake_slot.borrow_mut().take();
                                    start_attempt(next_args);
                                }),
                            );
                            *sleep_slot.borrow_mut() = Some(cancel);
                            cancels.borrow_mut().push(sleep_slot);
                        }
                    } else {
                        callback_args.out.emit(OutboundEvent::<T, R>::Exhausted {
                            value: callback_args.value,
                            attempt: callback_args.attempt,
                            error,
                        });
                    }
                }
            }
        }),
    );
    match cancel {
        Some(cancel) => {
            if !done.get() {
                *cancel_slot.borrow_mut() = Some(cancel);
                args.cancels.borrow_mut().push(cancel_slot);
            }
        }
        None => {
            if !done.get() {
                let error = "outbound adapter: missing driver send capability".to_owned();
                out.emit(OutboundEvent::<T, R>::Exhausted {
                    value: args.value,
                    attempt: args.attempt,
                    error: error.clone(),
                });
                out.down(vec![Message::Error(error.into())]);
            }
        }
    }
}

fn outbound_bundle<T, R>(
    graph: &Graph,
    events: Node<OutboundEvent<T, R>>,
    name: String,
) -> OutboundBundle<T, R>
where
    T: Clone + 'static,
    R: Clone + 'static,
{
    let status = graph.node_opts::<OutboundStatus, _>(
        vec![events.erased()],
        move |ctx| {
            let mut next = ctx
                .state_get::<OutboundStatus>()
                .map_or_else(OutboundStatus::default, |value| (*value).clone());
            for event in ctx.batch::<OutboundEvent<T, R>>(0) {
                next = reduce_status(&next, event.as_ref());
            }
            ctx.state_set(next.clone());
            ctx.emit(next);
        },
        GraphNodeOpts::named(format!("{name}/status")),
    );
    let attempts = graph.node_opts::<u32, _>(
        vec![events.erased()],
        move |ctx| {
            for event in ctx.batch::<OutboundEvent<T, R>>(0) {
                match event.as_ref() {
                    OutboundEvent::Attempt { attempt, .. }
                    | OutboundEvent::Retry { attempt, .. }
                    | OutboundEvent::Sent { attempt, .. }
                    | OutboundEvent::Exhausted { attempt, .. } => ctx.emit(*attempt),
                    OutboundEvent::UpstreamComplete | OutboundEvent::UpstreamError { .. } => {}
                }
            }
        },
        GraphNodeOpts::named(format!("{name}/attempts")),
    );
    let errors = graph.node_opts::<String, _>(
        vec![events.erased()],
        move |ctx| {
            for event in ctx.batch::<OutboundEvent<T, R>>(0) {
                match event.as_ref() {
                    OutboundEvent::Retry { error, .. }
                    | OutboundEvent::Exhausted { error, .. }
                    | OutboundEvent::UpstreamError { error } => ctx.emit(error.clone()),
                    OutboundEvent::Attempt { .. }
                    | OutboundEvent::Sent { .. }
                    | OutboundEvent::UpstreamComplete => {}
                }
            }
        },
        GraphNodeOpts::named(format!("{name}/errors")),
    );
    OutboundBundle {
        events,
        status,
        attempts,
        errors,
    }
}

fn reduce_status<T, R>(current: &OutboundStatus, event: &OutboundEvent<T, R>) -> OutboundStatus {
    match event {
        OutboundEvent::Attempt { attempt, .. } => OutboundStatus {
            state: OutboundState::Running,
            in_flight: current.in_flight.saturating_add(1),
            attempt: *attempt,
            sent: current.sent,
            failed: current.failed,
            last_delay_ms: current.last_delay_ms,
        },
        OutboundEvent::Retry {
            attempt, delay_ms, ..
        } => OutboundStatus {
            state: OutboundState::Waiting,
            in_flight: current.in_flight.saturating_sub(1),
            attempt: *attempt,
            sent: current.sent,
            failed: current.failed,
            last_delay_ms: Some(*delay_ms),
        },
        OutboundEvent::Sent { attempt, .. } => OutboundStatus {
            state: OutboundState::Succeeded,
            in_flight: current.in_flight.saturating_sub(1),
            attempt: *attempt,
            sent: current.sent.saturating_add(1),
            failed: current.failed,
            last_delay_ms: None,
        },
        OutboundEvent::Exhausted { attempt, .. } => OutboundStatus {
            state: OutboundState::Exhausted,
            in_flight: current.in_flight.saturating_sub(1),
            attempt: *attempt,
            sent: current.sent,
            failed: current.failed.saturating_add(1),
            last_delay_ms: current.last_delay_ms,
        },
        OutboundEvent::UpstreamComplete => OutboundStatus {
            state: OutboundState::Completed,
            ..current.clone()
        },
        OutboundEvent::UpstreamError { .. } => OutboundStatus {
            state: OutboundState::Failed,
            failed: current.failed.saturating_add(1),
            ..current.clone()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::environment::{
        EnvironmentDrivers, LocalHttpDriver, LocalWebSocketDriver, LocalWebSocketSession,
        WebSocketDriverEvent, WebSocketEvent,
    };
    use crate::graph::{graph_opts, GraphNodeOpts, GraphOptions};

    struct ManualHttpDriver {
        requests: Rc<RefCell<Vec<HttpRequest>>>,
    }

    impl LocalHttpDriver for ManualHttpDriver {
        fn request(
            &self,
            request: HttpRequest,
            callback: Box<dyn FnOnce(Result<HttpResponse, GraphError>)>,
        ) -> DriverCancel {
            self.requests.borrow_mut().push(request);
            callback(Ok(HttpResponse {
                status: 204,
                headers: Vec::new(),
                body: Vec::new(),
            }));
            Box::new(|| {})
        }
    }

    struct ConnectOnlyWebSocketDriver;

    impl LocalWebSocketDriver for ConnectOnlyWebSocketDriver {
        fn connect(
            &self,
            _request: WebSocketRequest,
            _callback: Rc<dyn Fn(WebSocketDriverEvent)>,
        ) -> DriverCancel {
            Box::new(|| {})
        }
    }

    #[derive(Default)]
    struct ManualSessionWebSocketDriver {
        sessions: RefCell<Vec<Rc<ManualWebSocketSession>>>,
    }

    impl ManualSessionWebSocketDriver {
        fn session(&self, index: usize) -> Rc<ManualWebSocketSession> {
            self.sessions.borrow()[index].clone()
        }
    }

    impl LocalWebSocketDriver for ManualSessionWebSocketDriver {
        fn connect(
            &self,
            _request: WebSocketRequest,
            _callback: Rc<dyn Fn(WebSocketDriverEvent)>,
        ) -> DriverCancel {
            Box::new(|| {})
        }

        fn connect_session(
            &self,
            request: WebSocketRequest,
            callback: Rc<dyn Fn(WebSocketDriverEvent)>,
        ) -> Option<Rc<dyn LocalWebSocketSession>> {
            let session = Rc::new(ManualWebSocketSession {
                url: request.url,
                active: Cell::new(true),
                callback,
                sent: RefCell::new(Vec::new()),
                closes: RefCell::new(Vec::new()),
            });
            self.sessions.borrow_mut().push(session.clone());
            Some(session)
        }
    }

    struct ManualWebSocketSession {
        url: String,
        active: Cell<bool>,
        callback: Rc<dyn Fn(WebSocketDriverEvent)>,
        sent: RefCell<Vec<WebSocketSend>>,
        closes: RefCell<Vec<(Option<u16>, Option<String>)>>,
    }

    impl ManualWebSocketSession {
        fn emit(&self, event: WebSocketDriverEvent) {
            if self.active.get() {
                (self.callback)(event);
            }
        }

        fn emit_ignoring_cancel(&self, event: WebSocketDriverEvent) {
            (self.callback)(event);
        }
    }

    impl LocalWebSocketSession for ManualWebSocketSession {
        fn send(
            &self,
            message: WebSocketSend,
            callback: Box<dyn FnOnce(Result<WebSocketSendResult, GraphError>)>,
        ) -> DriverCancel {
            if self.active.get() {
                self.sent.borrow_mut().push(message);
                callback(Ok(WebSocketSendResult { sent: true }));
            }
            Box::new(|| {})
        }

        fn close(&self, code: Option<u16>, reason: Option<String>) {
            self.closes.borrow_mut().push((code, reason));
            self.active.set(false);
        }

        fn cancel(&self) {
            self.active.set(false);
        }
    }

    struct FailOnceWebSocketDriver {
        attempts: Cell<u32>,
    }

    impl LocalWebSocketDriver for FailOnceWebSocketDriver {
        fn connect(
            &self,
            _request: WebSocketRequest,
            _callback: Rc<dyn Fn(WebSocketDriverEvent)>,
        ) -> DriverCancel {
            Box::new(|| {})
        }

        fn connect_session(
            &self,
            _request: WebSocketRequest,
            callback: Rc<dyn Fn(WebSocketDriverEvent)>,
        ) -> Option<Rc<dyn LocalWebSocketSession>> {
            let attempt = self.attempts.get().saturating_add(1);
            self.attempts.set(attempt);
            let session = Rc::new(ManualWebSocketSession {
                url: format!("attempt-{attempt}"),
                active: Cell::new(true),
                callback: callback.clone(),
                sent: RefCell::new(Vec::new()),
                closes: RefCell::new(Vec::new()),
            });
            if attempt == 1 {
                callback(WebSocketDriverEvent::Error("boom".into()));
            } else {
                callback(WebSocketDriverEvent::Event(WebSocketEvent::Open));
            }
            Some(session)
        }
    }

    type PendingSleep = (Rc<Cell<bool>>, Box<dyn FnOnce()>);

    #[derive(Default)]
    struct ManualAsyncDriver {
        sleeps: RefCell<Vec<PendingSleep>>,
    }

    impl ManualAsyncDriver {
        fn fire_next(&self) {
            let Some((active, callback)) = self.sleeps.borrow_mut().pop() else {
                panic!("expected a pending sleep");
            };
            if active.get() {
                callback();
            }
        }
    }

    impl LocalAsyncDriver for ManualAsyncDriver {
        fn sleep(&self, _duration: Duration, callback: Box<dyn FnOnce()>) -> DriverCancel {
            let active = Rc::new(Cell::new(true));
            self.sleeps.borrow_mut().push((active.clone(), callback));
            Box::new(move || active.set(false))
        }

        fn interval(&self, _period: Duration, _callback: Rc<dyn Fn()>) -> DriverCancel {
            Box::new(|| {})
        }

        fn spawn_local(
            &self,
            _fut: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'static>>,
        ) -> DriverCancel {
            Box::new(|| {})
        }
    }

    fn collect_node_data<T: Clone + 'static>(node: &Node<T>) -> Rc<RefCell<Vec<T>>> {
        let seen = Rc::new(RefCell::new(Vec::new()));
        let seen_sink = seen.clone();
        let _keep = node.subscribe(move |msg| {
            if let Message::Data(value) = msg {
                if let Some(value) = value.as_ref().downcast_ref::<T>() {
                    seen_sink.borrow_mut().push(value.clone());
                }
            }
        });
        seen
    }

    #[test]
    fn to_http_emits_graph_visible_attempt_sent_and_status() {
        let requests = Rc::new(RefCell::new(Vec::new()));
        let environment = EnvironmentDrivers::new().with_http(Rc::new(ManualHttpDriver {
            requests: requests.clone(),
        }));
        let g = graph_opts(GraphOptions {
            environment,
            ..GraphOptions::default()
        });
        let source = g.state_empty_opts::<String>(GraphNodeOpts::named("source"));
        let bundle = to_http(&g, &source, |value| {
            HttpRequest::new("POST", format!("https://example.test/{value}"))
        });
        let _events = bundle.events.subscribe(|_| {});
        let _status = bundle.status.subscribe(|_| {});

        source.set("order".to_owned());

        assert_eq!(requests.borrow()[0].url, "https://example.test/order");
        assert_eq!(
            bundle.events.cache(),
            Some(OutboundEvent::Sent {
                value: "order".to_owned(),
                attempt: 1,
                result: HttpResponse {
                    status: 204,
                    headers: Vec::new(),
                    body: Vec::new(),
                },
            })
        );
        assert_eq!(
            bundle.status.cache(),
            Some(OutboundStatus {
                state: OutboundState::Succeeded,
                in_flight: 0,
                attempt: 1,
                sent: 1,
                failed: 0,
                last_delay_ms: None,
            })
        );
        let snap = g.describe();
        assert!(snap
            .edges
            .iter()
            .any(|edge| edge.from == "source" && edge.to == "toHttp"));
    }

    #[test]
    fn missing_send_capability_closes_status_ledger() {
        let environment =
            EnvironmentDrivers::new().with_websocket(Rc::new(ConnectOnlyWebSocketDriver));
        let g = graph_opts(GraphOptions {
            environment,
            ..GraphOptions::default()
        });
        let source = g.state_empty_opts::<String>(GraphNodeOpts::named("source"));
        let bundle = to_websocket(
            &g,
            &source,
            WebSocketRequest::new("wss://example.test"),
            |value| WebSocketSend::text(value.clone()),
        );
        let _events = bundle.events.subscribe(|_| {});
        let _status = bundle.status.subscribe(|_| {});

        source.set("order".to_owned());

        assert_eq!(
            bundle.events.cache(),
            Some(OutboundEvent::Exhausted {
                value: "order".to_owned(),
                attempt: 1,
                error: "outbound adapter: missing driver send capability".to_owned(),
            })
        );
        assert_eq!(
            bundle.status.cache(),
            Some(OutboundStatus {
                state: OutboundState::Exhausted,
                in_flight: 0,
                attempt: 1,
                sent: 0,
                failed: 1,
                last_delay_ms: None,
            })
        );
    }

    #[test]
    fn websocket_session_missing_session_capability_is_graph_visible() {
        let environment =
            EnvironmentDrivers::new().with_websocket(Rc::new(ConnectOnlyWebSocketDriver));
        let g = graph_opts(GraphOptions {
            environment,
            ..GraphOptions::default()
        });
        let bundle = websocket_session(&g, WebSocketRequest::new("wss://example.test/session"));
        let errors = collect_node_data(&bundle.errors);
        let attempts = collect_node_data(&bundle.attempts);
        let lifecycle = collect_node_data(&bundle.lifecycle);
        let _status = bundle.status.subscribe(|_| {});

        bundle.start();

        assert_eq!(*attempts.borrow(), vec![1]);
        assert_eq!(
            *errors.borrow(),
            vec!["websocketSession: missing WebSocket session capability".to_owned()]
        );
        assert_eq!(
            bundle.status.cache(),
            Some(WebSocketSessionStatus {
                state: WebSocketSessionStateKind::Exhausted,
                attempt: 1,
                max_attempts: 1,
                sent: 0,
                received: 0,
                errors: 1,
                last_delay_ms: None,
            })
        );
        assert!(matches!(
            lifecycle.borrow().last(),
            Some(WebSocketSessionLifecycle::Exhausted { attempt: 1, .. })
        ));
    }

    #[test]
    fn websocket_session_helpers_publish_command_facts_only() {
        let g = graph_opts(GraphOptions::default());
        let bundle = websocket_session(&g, WebSocketRequest::new("wss://example.test/session"));
        let commands = collect_node_data(&bundle.command);

        bundle.start();
        bundle.send_text("hello");
        bundle.close(Some(1000), Some("done".to_owned()));

        assert_eq!(
            *commands.borrow(),
            vec![
                WebSocketSessionCommand::Start,
                WebSocketSessionCommand::Send(WebSocketSend::text("hello")),
                WebSocketSessionCommand::Close {
                    code: Some(1000),
                    reason: Some("done".to_owned()),
                },
            ]
        );
    }

    #[test]
    fn websocket_session_uses_same_connection_send_close_and_fences_late_callbacks() {
        let driver = Rc::new(ManualSessionWebSocketDriver::default());
        let g = graph_opts(GraphOptions {
            environment: EnvironmentDrivers::new().with_websocket(driver.clone()),
            ..GraphOptions::default()
        });
        let bundle = websocket_session_with_options(
            &g,
            WebSocketRequest::new("wss://example.test/session"),
            WebSocketSessionOptions {
                name: Some("ws".to_owned()),
                ..WebSocketSessionOptions::default()
            },
        );
        let inbound = collect_node_data(&bundle.inbound);
        let lifecycle = collect_node_data(&bundle.lifecycle);
        let _status = bundle.status.subscribe(|_| {});

        bundle.start();
        let session = driver.session(0);
        assert_eq!(session.url, "wss://example.test/session");
        session.emit(WebSocketDriverEvent::Event(WebSocketEvent::Open));
        bundle.send_text("hello");
        session.emit(WebSocketDriverEvent::Event(WebSocketEvent::Text(
            "server".to_owned(),
        )));
        bundle.close(Some(1000), Some("done".to_owned()));
        session.emit_ignoring_cancel(WebSocketDriverEvent::Event(WebSocketEvent::Text(
            "late".to_owned(),
        )));

        assert_eq!(
            session.sent.borrow().as_slice(),
            &[WebSocketSend::text("hello")]
        );
        assert_eq!(
            session.closes.borrow().as_slice(),
            &[(Some(1000), Some("done".to_owned()))]
        );
        assert_eq!(
            *inbound.borrow(),
            vec![WebSocketSessionInbound::Text("server".to_owned())]
        );
        assert!(lifecycle
            .borrow()
            .contains(&WebSocketSessionLifecycle::Sent {
                message: WebSocketSend::text("hello"),
            }));
        assert_eq!(
            bundle.status.cache(),
            Some(WebSocketSessionStatus {
                state: WebSocketSessionStateKind::Closed,
                attempt: 1,
                max_attempts: 1,
                sent: 1,
                received: 1,
                errors: 0,
                last_delay_ms: None,
            })
        );
    }

    #[test]
    fn one_driver_serves_multiple_independent_websocket_sessions() {
        let driver = Rc::new(ManualSessionWebSocketDriver::default());
        let g = graph_opts(GraphOptions {
            environment: EnvironmentDrivers::new().with_websocket(driver.clone()),
            ..GraphOptions::default()
        });
        let first = websocket_session_with_options(
            &g,
            WebSocketRequest::new("wss://example.test/a"),
            WebSocketSessionOptions {
                name: Some("first".to_owned()),
                ..WebSocketSessionOptions::default()
            },
        );
        let second = websocket_session_with_options(
            &g,
            WebSocketRequest::new("wss://example.test/b"),
            WebSocketSessionOptions {
                name: Some("second".to_owned()),
                ..WebSocketSessionOptions::default()
            },
        );
        let first_inbound = collect_node_data(&first.inbound);
        let second_inbound = collect_node_data(&second.inbound);
        let _first_status = first.status.subscribe(|_| {});
        let _second_status = second.status.subscribe(|_| {});

        first.start();
        second.start();
        let first_session = driver.session(0);
        let second_session = driver.session(1);
        first_session.emit(WebSocketDriverEvent::Event(WebSocketEvent::Open));
        second_session.emit(WebSocketDriverEvent::Event(WebSocketEvent::Open));
        first_session.emit(WebSocketDriverEvent::Event(WebSocketEvent::Text(
            "a".to_owned(),
        )));
        second_session.emit(WebSocketDriverEvent::Event(WebSocketEvent::Text(
            "b".to_owned(),
        )));
        first.close(Some(1000), None);
        second_session.emit(WebSocketDriverEvent::Event(WebSocketEvent::Text(
            "b2".to_owned(),
        )));

        assert_eq!(
            *first_inbound.borrow(),
            vec![WebSocketSessionInbound::Text("a".to_owned())]
        );
        assert_eq!(
            *second_inbound.borrow(),
            vec![
                WebSocketSessionInbound::Text("b".to_owned()),
                WebSocketSessionInbound::Text("b2".to_owned()),
            ]
        );
        assert_eq!(driver.sessions.borrow().len(), 2);
        assert!(!first_session.active.get());
        assert!(second_session.active.get());
    }

    #[test]
    fn websocket_session_retry_is_visible_and_bounded() {
        let driver = Rc::new(FailOnceWebSocketDriver {
            attempts: Cell::new(0),
        });
        let g = graph_opts(GraphOptions {
            environment: EnvironmentDrivers::new().with_websocket(driver.clone()),
            ..GraphOptions::default()
        });
        let bundle = websocket_session_with_options(
            &g,
            WebSocketRequest::new("wss://example.test/retry"),
            WebSocketSessionOptions {
                retry: RetryPolicy::new(2, crate::resilience::BackoffPolicy::None),
                ..WebSocketSessionOptions::default()
            },
        );
        let attempts = collect_node_data(&bundle.attempts);
        let errors = collect_node_data(&bundle.errors);
        let lifecycle = collect_node_data(&bundle.lifecycle);
        let _status = bundle.status.subscribe(|_| {});

        bundle.start();

        assert_eq!(*attempts.borrow(), vec![1, 2]);
        assert_eq!(*errors.borrow(), vec!["boom".to_owned()]);
        assert!(lifecycle.borrow().iter().any(|event| matches!(
            event,
            WebSocketSessionLifecycle::Retrying {
                attempt: 1,
                next_attempt: 2,
                ..
            }
        )));
        assert_eq!(
            bundle.status.cache(),
            Some(WebSocketSessionStatus {
                state: WebSocketSessionStateKind::Open,
                attempt: 2,
                max_attempts: 2,
                sent: 0,
                received: 0,
                errors: 1,
                last_delay_ms: None,
            })
        );
        assert_eq!(driver.attempts.get(), 2);
    }

    #[test]
    fn websocket_session_delayed_retry_reconnects_after_timer() {
        let driver = Rc::new(FailOnceWebSocketDriver {
            attempts: Cell::new(0),
        });
        let async_driver = Rc::new(ManualAsyncDriver::default());
        let g = graph_opts(GraphOptions {
            environment: EnvironmentDrivers::new()
                .with_websocket(driver.clone())
                .with_local_async(async_driver.clone()),
            ..GraphOptions::default()
        });
        let bundle = websocket_session_with_options(
            &g,
            WebSocketRequest::new("wss://example.test/retry"),
            WebSocketSessionOptions {
                retry: RetryPolicy::new(
                    2,
                    crate::resilience::BackoffPolicy::Constant { delay_ms: 10 },
                ),
                ..WebSocketSessionOptions::default()
            },
        );
        let attempts = collect_node_data(&bundle.attempts);
        let _status = bundle.status.subscribe(|_| {});

        bundle.start();

        assert_eq!(*attempts.borrow(), vec![1]);
        assert_eq!(
            bundle.status.cache().map(|status| status.state),
            Some(WebSocketSessionStateKind::Waiting)
        );

        async_driver.fire_next();

        assert_eq!(*attempts.borrow(), vec![1, 2]);
        assert_eq!(
            bundle.status.cache().map(|status| status.state),
            Some(WebSocketSessionStateKind::Open)
        );
        assert_eq!(driver.attempts.get(), 2);
    }
}
