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
    HttpRequest, HttpResponse, ProcessCommand, ProcessResult, WebSocketRequest, WebSocketSend,
    WebSocketSendResult,
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
        EnvironmentDrivers, LocalHttpDriver, LocalWebSocketDriver, WebSocketDriverEvent,
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
}
