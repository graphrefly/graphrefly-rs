//! Graph-owned environment driver bag (D130/D131).
//!
//! Environment drivers host wall-clock, process, network, messaging, and similar
//! boundary work outside the synchronous wave core. The bag is graph-local and
//! is attached to node ctx for source/adapter bodies.

use std::fmt;
use std::path::PathBuf;
use std::rc::Rc;

use crate::async_driver::DriverCancel;
use crate::async_driver::LocalAsyncDriver;
#[cfg(feature = "tokio")]
use crate::async_driver::TokioLocalDriver;
use crate::protocol::GraphError;

#[cfg(feature = "tokio-websocket")]
use futures_util::{SinkExt, StreamExt};

/// Process command request for graph environment process drivers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessCommand {
    /// `program` field for program.
    pub program: String,
    /// `args` field for args.
    pub args: Vec<String>,
    /// `cwd` field for cwd.
    pub cwd: Option<PathBuf>,
    /// `env` field for env.
    pub env: Vec<(String, String)>,
}

impl ProcessCommand {
    /// Creates or computes `new`.
    pub fn new(program: impl Into<String>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            cwd: None,
            env: Vec::new(),
        }
    }

    /// Updates or reads `args`.
    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.args = args.into_iter().map(Into::into).collect();
        self
    }

    /// Updates or reads `cwd`.
    pub fn cwd(mut self, cwd: impl Into<PathBuf>) -> Self {
        self.cwd = Some(cwd.into());
        self
    }

    /// Updates or reads `env`.
    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.push((key.into(), value.into()));
        self
    }
}

/// Completed process result. Exit status is DATA, including non-zero exits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessResult {
    /// `stdout` field for stdout.
    pub stdout: String,
    /// `stderr` field for stderr.
    pub stderr: String,
    /// `exit_code` field for exit code.
    pub exit_code: Option<i32>,
    /// `signal` field for signal.
    pub signal: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `HttpRequest` data container.
pub struct HttpRequest {
    /// `method` field for method.
    pub method: String,
    /// `url` field for url.
    pub url: String,
    /// `headers` field for headers.
    pub headers: Vec<(String, String)>,
    /// `body` field for body.
    pub body: Vec<u8>,
}

impl HttpRequest {
    /// Creates or computes `new`.
    pub fn new(method: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            method: method.into(),
            url: url.into(),
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    /// Creates or computes `get`.
    pub fn get(url: impl Into<String>) -> Self {
        Self::new("GET", url)
    }

    /// Updates or reads `header`.
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((key.into(), value.into()));
        self
    }

    /// Updates or reads `body`.
    pub fn body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.body = body.into();
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `HttpResponse` data container.
pub struct HttpResponse {
    /// `status` field for status.
    pub status: u16,
    /// `headers` field for headers.
    pub headers: Vec<(String, String)>,
    /// `body` field for body.
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `HttpStreamHead` data container.
pub struct HttpStreamHead {
    /// `status` field for status.
    pub status: u16,
    /// `headers` field for headers.
    pub headers: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `SseRequest` data container.
pub struct SseRequest {
    /// `url` field for url.
    pub url: String,
    /// `headers` field for headers.
    pub headers: Vec<(String, String)>,
}

impl SseRequest {
    /// Creates or computes `new`.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            headers: Vec::new(),
        }
    }

    /// Updates or reads `header`.
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((key.into(), value.into()));
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `SseEvent` data container.
pub struct SseEvent {
    /// `event` field for event.
    pub event: Option<String>,
    /// `data` field for data.
    pub data: String,
    /// `id` field for id.
    pub id: Option<String>,
    /// `retry_ms` field for retry ms.
    pub retry_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `WebSocketRequest` data container.
pub struct WebSocketRequest {
    /// `url` field for url.
    pub url: String,
    /// `headers` field for headers.
    pub headers: Vec<(String, String)>,
}

impl WebSocketRequest {
    /// Creates or computes `new`.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            headers: Vec::new(),
        }
    }

    /// Updates or reads `header`.
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((key.into(), value.into()));
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `WebSocketEvent` variants.
pub enum WebSocketEvent {
    /// `Open` variant.
    Open,
    /// `Text` variant.
    Text(String),
    /// `Binary` variant.
    Binary(Vec<u8>),
    /// `Close` variant.
    Close {
        /// `code` field for code.
        code: Option<u16>,
        /// `reason` field for reason.
        reason: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `WebSocketSend` data container.
pub struct WebSocketSend {
    /// `data` field for data.
    pub data: Vec<u8>,
    frame_kind: WebSocketFrameKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WebSocketFrameKind {
    Text,
    Binary,
}

impl WebSocketSend {
    /// Creates or computes `text`.
    pub fn text(data: impl Into<String>) -> Self {
        Self {
            data: data.into().into_bytes(),
            frame_kind: WebSocketFrameKind::Text,
        }
    }

    /// Creates or computes `binary`.
    pub fn binary(data: impl Into<Vec<u8>>) -> Self {
        Self {
            data: data.into(),
            frame_kind: WebSocketFrameKind::Binary,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// `WebSocketSendResult` data container.
pub struct WebSocketSendResult {
    /// `sent` field for sent.
    pub sent: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `WebhookRegistration` data container.
pub struct WebhookRegistration {
    /// `id` field for id.
    pub id: String,
    /// `method` field for method.
    pub method: Option<String>,
    /// `path` field for path.
    pub path: Option<String>,
}

impl WebhookRegistration {
    /// Creates or computes `new`.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            method: None,
            path: None,
        }
    }

    /// Updates or reads `method`.
    pub fn method(mut self, method: impl Into<String>) -> Self {
        self.method = Some(method.into());
        self
    }

    /// Updates or reads `path`.
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `WebhookEvent` data container.
pub struct WebhookEvent {
    /// `registration_id` field for registration id.
    pub registration_id: String,
    /// `method` field for method.
    pub method: String,
    /// `path` field for path.
    pub path: String,
    /// `headers` field for headers.
    pub headers: Vec<(String, String)>,
    /// `query` field for query.
    pub query: Vec<(String, String)>,
    /// `body` field for body.
    pub body: Vec<u8>,
}

/// Graph-local process driver. Implementations own process/runtime details.
pub trait LocalProcessDriver {
    /// Updates or reads `run`.
    fn run(
        &self,
        command: ProcessCommand,
        callback: Box<dyn FnOnce(Result<ProcessResult, GraphError>)>,
    ) -> DriverCancel;
}

/// `LocalHttpDriver` behavior contract.
pub trait LocalHttpDriver {
    /// Updates or reads `request`.
    fn request(
        &self,
        request: HttpRequest,
        callback: Box<dyn FnOnce(Result<HttpResponse, GraphError>)>,
    ) -> DriverCancel;
}

/// `HttpStreamDriverEvent` variants.
pub enum HttpStreamDriverEvent {
    /// `Head` variant.
    Head(HttpStreamHead),
    /// `Chunk` variant.
    Chunk(Vec<u8>),
    /// `Error` variant.
    Error(GraphError),
    /// `Complete` variant.
    Complete,
}

/// `LocalHttpStreamDriver` behavior contract.
pub trait LocalHttpStreamDriver {
    /// Updates or reads `stream`.
    fn stream(
        &self,
        request: HttpRequest,
        callback: Rc<dyn Fn(HttpStreamDriverEvent)>,
    ) -> DriverCancel;
}

/// `SseDriverEvent` variants.
pub enum SseDriverEvent {
    /// `Event` variant.
    Event(SseEvent),
    /// `Error` variant.
    Error(GraphError),
    /// `Complete` variant.
    Complete,
}

/// `LocalSseDriver` behavior contract.
pub trait LocalSseDriver {
    /// Updates or reads `connect`.
    fn connect(&self, request: SseRequest, callback: Rc<dyn Fn(SseDriverEvent)>) -> DriverCancel;
}

/// `WebSocketDriverEvent` variants.
pub enum WebSocketDriverEvent {
    /// `Event` variant.
    Event(WebSocketEvent),
    /// `Error` variant.
    Error(GraphError),
    /// `Complete` variant.
    Complete,
}

/// `LocalWebSocketDriver` behavior contract.
pub trait LocalWebSocketDriver {
    /// Updates or reads `connect`.
    fn connect(
        &self,
        request: WebSocketRequest,
        callback: Rc<dyn Fn(WebSocketDriverEvent)>,
    ) -> DriverCancel;

    /// Updates or reads `send`.
    fn send(
        &self,
        _request: WebSocketRequest,
        _message: WebSocketSend,
        _callback: Box<dyn FnOnce(Result<WebSocketSendResult, GraphError>)>,
    ) -> Option<DriverCancel> {
        None
    }

    /// Updates or reads `connect_session`.
    fn connect_session(
        &self,
        _request: WebSocketRequest,
        _callback: Rc<dyn Fn(WebSocketDriverEvent)>,
    ) -> Option<Rc<dyn LocalWebSocketSession>> {
        None
    }
}

/// Live same-connection WebSocket session handle for D133/D174 SessionBundles.
///
/// Drivers create handles; graph-visible bundles own lifecycle, retry, status,
/// command facts, and callback fencing.
pub trait LocalWebSocketSession {
    /// Updates or reads `send`.
    fn send(
        &self,
        message: WebSocketSend,
        callback: Box<dyn FnOnce(Result<WebSocketSendResult, GraphError>)>,
    ) -> DriverCancel;

    /// Updates or reads `close`.
    fn close(&self, code: Option<u16>, reason: Option<String>);

    /// Updates or reads `cancel`.
    fn cancel(&self);
}

/// `WebhookDriverEvent` variants.
pub enum WebhookDriverEvent {
    /// `Event` variant.
    Event(WebhookEvent),
    /// `Error` variant.
    Error(GraphError),
    /// `Complete` variant.
    Complete,
}

/// `LocalWebhookDriver` behavior contract.
pub trait LocalWebhookDriver {
    /// Updates or reads `register`.
    fn register(
        &self,
        registration: WebhookRegistration,
        callback: Rc<dyn Fn(WebhookDriverEvent)>,
    ) -> DriverCancel;
}

#[cfg(feature = "tokio")]
#[derive(Debug, Clone, Copy, Default)]
/// `TokioProcessDriver` data container.
pub struct TokioProcessDriver;

#[cfg(feature = "tokio")]
impl LocalProcessDriver for TokioProcessDriver {
    fn run(
        &self,
        command: ProcessCommand,
        callback: Box<dyn FnOnce(Result<ProcessResult, GraphError>)>,
    ) -> DriverCancel {
        let active = Rc::new(std::cell::Cell::new(true));
        let active_for_task = active.clone();
        let cancel_task = TokioLocalDriver.spawn_local(Box::pin(async move {
            let mut cmd = tokio::process::Command::new(&command.program);
            cmd.args(&command.args);
            cmd.kill_on_drop(true);
            if let Some(cwd) = command.cwd {
                cmd.current_dir(cwd);
            }
            for (key, value) in command.env {
                cmd.env(key, value);
            }
            match cmd.output().await {
                Ok(output) => {
                    if active_for_task.get() {
                        callback(Ok(ProcessResult {
                            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
                            exit_code: output.status.code(),
                            signal: exit_signal(&output.status),
                        }));
                    }
                }
                Err(error) => {
                    if active_for_task.get() {
                        callback(Err(Box::new(error)));
                    }
                }
            }
        }));
        Box::new(move || {
            active.set(false);
            cancel_task();
        })
    }
}

#[cfg(all(feature = "tokio", unix))]
fn exit_signal(status: &std::process::ExitStatus) -> Option<String> {
    use std::os::unix::process::ExitStatusExt;
    status.signal().map(|signal| signal.to_string())
}

#[cfg(all(feature = "tokio", not(unix)))]
fn exit_signal(_status: &std::process::ExitStatus) -> Option<String> {
    None
}

#[cfg(feature = "tokio-http")]
#[derive(Debug, Clone, Default)]
/// `TokioHttpDriver` data container.
pub struct TokioHttpDriver {
    client: reqwest::Client,
}

#[cfg(feature = "tokio-http")]
impl TokioHttpDriver {
    /// Creates or computes `new`.
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

#[cfg(feature = "tokio-http")]
impl LocalHttpDriver for TokioHttpDriver {
    fn request(
        &self,
        request: HttpRequest,
        callback: Box<dyn FnOnce(Result<HttpResponse, GraphError>)>,
    ) -> DriverCancel {
        let active = Rc::new(std::cell::Cell::new(true));
        let active_for_task = active.clone();
        let client = self.client.clone();
        let cancel_task = TokioLocalDriver.spawn_local(Box::pin(async move {
            let method = match reqwest::Method::from_bytes(request.method.as_bytes()) {
                Ok(method) => method,
                Err(error) => {
                    if active_for_task.get() {
                        callback(Err(Box::new(error)));
                    }
                    return;
                }
            };
            let mut builder = client.request(method, request.url);
            for (key, value) in request.headers {
                builder = builder.header(key, value);
            }
            if !request.body.is_empty() {
                builder = builder.body(request.body);
            }
            match builder.send().await {
                Ok(response) => {
                    let status = response.status().as_u16();
                    let headers = response
                        .headers()
                        .iter()
                        .map(|(key, value)| {
                            (
                                key.as_str().to_owned(),
                                value.to_str().unwrap_or_default().to_owned(),
                            )
                        })
                        .collect::<Vec<_>>();
                    match response.bytes().await {
                        Ok(body) => {
                            if active_for_task.get() {
                                callback(Ok(HttpResponse {
                                    status,
                                    headers,
                                    body: body.to_vec(),
                                }));
                            }
                        }
                        Err(error) => {
                            if active_for_task.get() {
                                callback(Err(Box::new(error)));
                            }
                        }
                    }
                }
                Err(error) => {
                    if active_for_task.get() {
                        callback(Err(Box::new(error)));
                    }
                }
            }
        }));
        Box::new(move || {
            active.set(false);
            cancel_task();
        })
    }
}

#[cfg(feature = "tokio-http-stream")]
#[derive(Debug, Clone, Default)]
/// `TokioHttpStreamDriver` data container.
pub struct TokioHttpStreamDriver {
    client: reqwest::Client,
}

#[cfg(feature = "tokio-http-stream")]
impl TokioHttpStreamDriver {
    /// Creates or computes `new`.
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

#[cfg(feature = "tokio-http-stream")]
impl LocalHttpStreamDriver for TokioHttpStreamDriver {
    fn stream(
        &self,
        request: HttpRequest,
        callback: Rc<dyn Fn(HttpStreamDriverEvent)>,
    ) -> DriverCancel {
        let active = Rc::new(std::cell::Cell::new(true));
        let active_for_task = active.clone();
        let client = self.client.clone();
        let cancel_task = TokioLocalDriver.spawn_local(Box::pin(async move {
            let method = match reqwest::Method::from_bytes(request.method.as_bytes()) {
                Ok(method) => method,
                Err(error) => {
                    if active_for_task.replace(false) {
                        callback(HttpStreamDriverEvent::Error(Box::new(error)));
                    }
                    return;
                }
            };
            let mut builder = client.request(method, request.url);
            for (key, value) in request.headers {
                builder = builder.header(key, value);
            }
            if !request.body.is_empty() {
                builder = builder.body(request.body);
            }
            let mut response = match builder.send().await {
                Ok(response) => response,
                Err(error) => {
                    if active_for_task.replace(false) {
                        callback(HttpStreamDriverEvent::Error(Box::new(error)));
                    }
                    return;
                }
            };
            let head = HttpStreamHead {
                status: response.status().as_u16(),
                headers: response
                    .headers()
                    .iter()
                    .map(|(key, value)| {
                        (
                            key.as_str().to_owned(),
                            value.to_str().unwrap_or_default().to_owned(),
                        )
                    })
                    .collect(),
            };
            if active_for_task.get() {
                callback(HttpStreamDriverEvent::Head(head));
            }
            while active_for_task.get() {
                match response.chunk().await {
                    Ok(Some(chunk)) => {
                        if !chunk.is_empty() && active_for_task.get() {
                            callback(HttpStreamDriverEvent::Chunk(chunk.to_vec()));
                        }
                    }
                    Ok(None) => {
                        if active_for_task.replace(false) {
                            callback(HttpStreamDriverEvent::Complete);
                        }
                        break;
                    }
                    Err(error) => {
                        if active_for_task.replace(false) {
                            callback(HttpStreamDriverEvent::Error(Box::new(error)));
                        }
                        break;
                    }
                }
            }
        }));
        Box::new(move || {
            active.set(false);
            cancel_task();
        })
    }
}

#[cfg(feature = "tokio-websocket")]
#[derive(Debug, Clone, Copy, Default)]
/// `TokioWebSocketDriver` data container.
pub struct TokioWebSocketDriver;

#[cfg(feature = "tokio-websocket")]
impl LocalWebSocketDriver for TokioWebSocketDriver {
    fn connect(
        &self,
        request: WebSocketRequest,
        callback: Rc<dyn Fn(WebSocketDriverEvent)>,
    ) -> DriverCancel {
        let active = Rc::new(std::cell::Cell::new(true));
        let active_for_task = active.clone();
        let cancel_task = TokioLocalDriver.spawn_local(Box::pin(async move {
            let client_request = match websocket_client_request(request) {
                Ok(request) => request,
                Err(error) => {
                    if active_for_task.get() {
                        callback(WebSocketDriverEvent::Error(error));
                    }
                    return;
                }
            };
            match tokio_tungstenite::connect_async(client_request).await {
                Ok((mut socket, _response)) => {
                    if active_for_task.get() {
                        callback(WebSocketDriverEvent::Event(WebSocketEvent::Open));
                    }
                    while active_for_task.get() {
                        match socket.next().await {
                            Some(Ok(message)) => {
                                if !active_for_task.get() {
                                    break;
                                }
                                if let Some((event, complete)) =
                                    websocket_event_from_message(message)
                                {
                                    callback(WebSocketDriverEvent::Event(event));
                                    if complete {
                                        callback(WebSocketDriverEvent::Complete);
                                        break;
                                    }
                                }
                            }
                            Some(Err(error)) => {
                                if active_for_task.get() {
                                    callback(WebSocketDriverEvent::Error(Box::new(error)));
                                }
                                break;
                            }
                            None => {
                                if active_for_task.get() {
                                    callback(WebSocketDriverEvent::Complete);
                                }
                                break;
                            }
                        }
                    }
                }
                Err(error) => {
                    if active_for_task.get() {
                        callback(WebSocketDriverEvent::Error(Box::new(error)));
                    }
                }
            }
        }));
        Box::new(move || {
            active.set(false);
            cancel_task();
        })
    }

    fn send(
        &self,
        request: WebSocketRequest,
        message: WebSocketSend,
        callback: Box<dyn FnOnce(Result<WebSocketSendResult, GraphError>)>,
    ) -> Option<DriverCancel> {
        let active = Rc::new(std::cell::Cell::new(true));
        let active_for_task = active.clone();
        let cancel_task = TokioLocalDriver.spawn_local(Box::pin(async move {
            let client_request = match websocket_client_request(request) {
                Ok(request) => request,
                Err(error) => {
                    if active_for_task.get() {
                        callback(Err(error));
                    }
                    return;
                }
            };
            match tokio_tungstenite::connect_async(client_request).await {
                Ok((mut socket, _response)) => {
                    let result = match websocket_message_from_send(message) {
                        Ok(message) => socket
                            .send(message)
                            .await
                            .map(|()| WebSocketSendResult { sent: true })
                            .map_err(|error| Box::new(error) as GraphError),
                        Err(error) => Err(error),
                    };
                    let _ = socket.close(None).await;
                    if active_for_task.get() {
                        callback(result);
                    }
                }
                Err(error) => {
                    if active_for_task.get() {
                        callback(Err(Box::new(error)));
                    }
                }
            }
        }));
        Some(Box::new(move || {
            active.set(false);
            cancel_task();
        }))
    }

    fn connect_session(
        &self,
        request: WebSocketRequest,
        callback: Rc<dyn Fn(WebSocketDriverEvent)>,
    ) -> Option<Rc<dyn LocalWebSocketSession>> {
        let active = Rc::new(std::cell::Cell::new(true));
        let opened = Rc::new(std::cell::Cell::new(false));
        let (tx, mut rx) = tokio::sync::mpsc::channel::<TokioWebSocketSessionCommand>(1);
        let active_for_task = active.clone();
        let opened_for_task = opened.clone();
        let cancel_task = TokioLocalDriver.spawn_local(Box::pin(async move {
            let client_request = match websocket_client_request(request) {
                Ok(request) => request,
                Err(error) => {
                    if active_for_task.get() {
                        callback(WebSocketDriverEvent::Error(error));
                    }
                    return;
                }
            };
            let (mut socket, _response) =
                match tokio_tungstenite::connect_async(client_request).await {
                    Ok(connected) => connected,
                    Err(error) => {
                        if active_for_task.get() {
                            callback(WebSocketDriverEvent::Error(Box::new(error)));
                        }
                        return;
                    }
                };
            opened_for_task.set(true);
            if active_for_task.get() {
                callback(WebSocketDriverEvent::Event(WebSocketEvent::Open));
            }
            while active_for_task.get() {
                tokio::select! {
                    command = rx.recv() => {
                        match command {
                            Some(TokioWebSocketSessionCommand::Send { message, active, callback }) => {
                                if !active.get() || !active_for_task.get() {
                                    continue;
                                }
                                let result = match websocket_message_from_send(message) {
                                    Ok(message) => socket
                                        .send(message)
                                        .await
                                        .map(|()| WebSocketSendResult { sent: true })
                                        .map_err(|error| Box::new(error) as GraphError),
                                    Err(error) => Err(error),
                                };
                                if active.get() && active_for_task.get() {
                                    callback(result);
                                }
                            }
                            Some(TokioWebSocketSessionCommand::Close { code, reason }) => {
                                let frame = websocket_close_frame(code, reason);
                                let _ = socket.close(frame).await;
                                active_for_task.set(false);
                                opened_for_task.set(false);
                                break;
                            }
                            Some(TokioWebSocketSessionCommand::Cancel) | None => {
                                let _ = socket.close(None).await;
                                active_for_task.set(false);
                                opened_for_task.set(false);
                                break;
                            }
                        }
                    }
                    message = socket.next() => {
                        match message {
                            Some(Ok(message)) => {
                                if !active_for_task.get() {
                                    break;
                                }
                                if let Some((event, complete)) = websocket_event_from_message(message) {
                                    callback(WebSocketDriverEvent::Event(event));
                                    if complete {
                                        callback(WebSocketDriverEvent::Complete);
                                        active_for_task.set(false);
                                        opened_for_task.set(false);
                                        break;
                                    }
                                }
                            }
                            Some(Err(error)) => {
                                if active_for_task.get() {
                                    callback(WebSocketDriverEvent::Error(Box::new(error)));
                                }
                                active_for_task.set(false);
                                opened_for_task.set(false);
                                break;
                            }
                            None => {
                                if active_for_task.get() {
                                    callback(WebSocketDriverEvent::Complete);
                                }
                                active_for_task.set(false);
                                opened_for_task.set(false);
                                break;
                            }
                        }
                    }
                }
            }
        }));
        Some(Rc::new(TokioWebSocketSession {
            active,
            closing: Rc::new(std::cell::Cell::new(false)),
            opened,
            tx,
            cancel_task: Rc::new(std::cell::RefCell::new(Some(cancel_task))),
        }))
    }
}

#[cfg(feature = "tokio-websocket")]
enum TokioWebSocketSessionCommand {
    Send {
        message: WebSocketSend,
        active: Rc<std::cell::Cell<bool>>,
        callback: Box<dyn FnOnce(Result<WebSocketSendResult, GraphError>)>,
    },
    Close {
        code: Option<u16>,
        reason: Option<String>,
    },
    Cancel,
}

#[cfg(feature = "tokio-websocket")]
struct TokioWebSocketSession {
    active: Rc<std::cell::Cell<bool>>,
    closing: Rc<std::cell::Cell<bool>>,
    opened: Rc<std::cell::Cell<bool>>,
    tx: tokio::sync::mpsc::Sender<TokioWebSocketSessionCommand>,
    cancel_task: Rc<std::cell::RefCell<Option<DriverCancel>>>,
}

#[cfg(feature = "tokio-websocket")]
impl LocalWebSocketSession for TokioWebSocketSession {
    fn send(
        &self,
        message: WebSocketSend,
        callback: Box<dyn FnOnce(Result<WebSocketSendResult, GraphError>)>,
    ) -> DriverCancel {
        let send_active = Rc::new(std::cell::Cell::new(true));
        if !self.active.get() {
            send_active.set(false);
            callback(Err("websocket session is closed".into()));
            return Box::new(|| {});
        }
        match self.tx.try_send(TokioWebSocketSessionCommand::Send {
            message,
            active: send_active.clone(),
            callback,
        }) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(command)) => {
                send_active.set(false);
                let TokioWebSocketSessionCommand::Send { callback, .. } = command else {
                    return Box::new(|| {});
                };
                callback(Err("websocket session send queue is busy".into()));
                return Box::new(|| {});
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(command)) => {
                send_active.set(false);
                let TokioWebSocketSessionCommand::Send { callback, .. } = command else {
                    return Box::new(|| {});
                };
                callback(Err("websocket session is closed".into()));
                return Box::new(|| {});
            }
        }
        Box::new(move || {
            send_active.set(false);
        })
    }

    fn close(&self, code: Option<u16>, reason: Option<String>) {
        if self.active.get() {
            self.closing.set(true);
            if !self.opened.get()
                || self
                    .tx
                    .try_send(TokioWebSocketSessionCommand::Close { code, reason })
                    .is_err()
            {
                self.cancel();
            }
        }
    }

    fn cancel(&self) {
        if self.active.replace(false) {
            let _ = self.tx.try_send(TokioWebSocketSessionCommand::Cancel);
            if let Some(cancel) = self.cancel_task.borrow_mut().take() {
                cancel();
            }
        }
    }
}

#[cfg(feature = "tokio-websocket")]
impl Drop for TokioWebSocketSession {
    fn drop(&mut self) {
        if self.active.get() && !self.closing.get() {
            self.cancel();
        }
    }
}

#[cfg(feature = "tokio-websocket")]
fn websocket_client_request(
    request: WebSocketRequest,
) -> Result<tokio_tungstenite::tungstenite::handshake::client::Request, GraphError> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let mut client_request = request
        .url
        .as_str()
        .into_client_request()
        .map_err(|error| Box::new(error) as GraphError)?;
    for (key, value) in request.headers {
        let name = tokio_tungstenite::tungstenite::http::HeaderName::from_bytes(key.as_bytes())
            .map_err(|error| Box::new(error) as GraphError)?;
        let value = tokio_tungstenite::tungstenite::http::HeaderValue::from_str(&value)
            .map_err(|error| Box::new(error) as GraphError)?;
        client_request.headers_mut().append(name, value);
    }
    Ok(client_request)
}

#[cfg(feature = "tokio-websocket")]
fn websocket_message_from_send(
    message: WebSocketSend,
) -> Result<tokio_tungstenite::tungstenite::Message, GraphError> {
    match message.frame_kind {
        WebSocketFrameKind::Text => String::from_utf8(message.data)
            .map(|text| tokio_tungstenite::tungstenite::Message::Text(text.into()))
            .map_err(|error| Box::new(error) as GraphError),
        WebSocketFrameKind::Binary => Ok(tokio_tungstenite::tungstenite::Message::Binary(
            message.data.into(),
        )),
    }
}

#[cfg(feature = "tokio-websocket")]
fn websocket_event_from_message(
    message: tokio_tungstenite::tungstenite::Message,
) -> Option<(WebSocketEvent, bool)> {
    match message {
        tokio_tungstenite::tungstenite::Message::Text(text) => {
            Some((WebSocketEvent::Text(text.to_string()), false))
        }
        tokio_tungstenite::tungstenite::Message::Binary(bytes) => {
            Some((WebSocketEvent::Binary(bytes.to_vec()), false))
        }
        tokio_tungstenite::tungstenite::Message::Close(frame) => Some((
            WebSocketEvent::Close {
                code: frame.as_ref().map(|frame| u16::from(frame.code)),
                reason: frame.map(|frame| frame.reason.to_string()),
            },
            true,
        )),
        tokio_tungstenite::tungstenite::Message::Ping(_)
        | tokio_tungstenite::tungstenite::Message::Pong(_)
        | tokio_tungstenite::tungstenite::Message::Frame(_) => None,
    }
}

#[cfg(feature = "tokio-websocket")]
fn websocket_close_frame(
    code: Option<u16>,
    reason: Option<String>,
) -> Option<tokio_tungstenite::tungstenite::protocol::CloseFrame> {
    if code.is_none() && reason.is_none() {
        return None;
    }
    let code = code
        .map(tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::from)
        .unwrap_or(tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal);
    Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
        code,
        reason: reason.unwrap_or_default().into(),
    })
}

/// Graph-local environment capabilities for source/adapter boundaries.
///
/// First slice carries the existing local async/time driver. Process, network,
/// messaging, and resilience driver groups grow here rather than on
/// [`crate::dispatcher::Dispatcher`] (D131).
#[derive(Clone, Default)]
pub struct EnvironmentDrivers {
    local_async: Option<Rc<dyn LocalAsyncDriver>>,
    process: Option<Rc<dyn LocalProcessDriver>>,
    http: Option<Rc<dyn LocalHttpDriver>>,
    http_stream: Option<Rc<dyn LocalHttpStreamDriver>>,
    sse: Option<Rc<dyn LocalSseDriver>>,
    websocket: Option<Rc<dyn LocalWebSocketDriver>>,
    webhook: Option<Rc<dyn LocalWebhookDriver>>,
}

impl EnvironmentDrivers {
    /// Creates or computes `new`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Updates or reads `with_local_async`.
    pub fn with_local_async(mut self, driver: Rc<dyn LocalAsyncDriver>) -> Self {
        self.local_async = Some(driver);
        self
    }

    /// Updates or reads `with_process`.
    pub fn with_process(mut self, driver: Rc<dyn LocalProcessDriver>) -> Self {
        self.process = Some(driver);
        self
    }

    /// Updates or reads `with_http`.
    pub fn with_http(mut self, driver: Rc<dyn LocalHttpDriver>) -> Self {
        self.http = Some(driver);
        self
    }

    /// Updates or reads `with_http_stream`.
    pub fn with_http_stream(mut self, driver: Rc<dyn LocalHttpStreamDriver>) -> Self {
        self.http_stream = Some(driver);
        self
    }

    /// Updates or reads `with_sse`.
    pub fn with_sse(mut self, driver: Rc<dyn LocalSseDriver>) -> Self {
        self.sse = Some(driver);
        self
    }

    /// Updates or reads `with_websocket`.
    pub fn with_websocket(mut self, driver: Rc<dyn LocalWebSocketDriver>) -> Self {
        self.websocket = Some(driver);
        self
    }

    /// Updates or reads `with_webhook`.
    pub fn with_webhook(mut self, driver: Rc<dyn LocalWebhookDriver>) -> Self {
        self.webhook = Some(driver);
        self
    }

    /// Updates or reads `local_async_driver`.
    pub fn local_async_driver(&self) -> Option<Rc<dyn LocalAsyncDriver>> {
        self.local_async.clone()
    }

    /// Updates or reads `process_driver`.
    pub fn process_driver(&self) -> Option<Rc<dyn LocalProcessDriver>> {
        self.process.clone()
    }

    /// Updates or reads `http_driver`.
    pub fn http_driver(&self) -> Option<Rc<dyn LocalHttpDriver>> {
        self.http.clone()
    }

    /// Updates or reads `http_stream_driver`.
    pub fn http_stream_driver(&self) -> Option<Rc<dyn LocalHttpStreamDriver>> {
        self.http_stream.clone()
    }

    /// Updates or reads `sse_driver`.
    pub fn sse_driver(&self) -> Option<Rc<dyn LocalSseDriver>> {
        self.sse.clone()
    }

    /// Updates or reads `websocket_driver`.
    pub fn websocket_driver(&self) -> Option<Rc<dyn LocalWebSocketDriver>> {
        self.websocket.clone()
    }

    /// Updates or reads `webhook_driver`.
    pub fn webhook_driver(&self) -> Option<Rc<dyn LocalWebhookDriver>> {
        self.webhook.clone()
    }

    pub(crate) fn set_local_async_driver(&mut self, driver: Option<Rc<dyn LocalAsyncDriver>>) {
        self.local_async = driver;
    }
}

impl fmt::Debug for EnvironmentDrivers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnvironmentDrivers")
            .field(
                "local_async",
                &self.local_async.as_ref().map(|_| "<installed>"),
            )
            .field("process", &self.process.as_ref().map(|_| "<installed>"))
            .field("http", &self.http.as_ref().map(|_| "<installed>"))
            .field(
                "http_stream",
                &self.http_stream.as_ref().map(|_| "<installed>"),
            )
            .field("sse", &self.sse.as_ref().map(|_| "<installed>"))
            .field("websocket", &self.websocket.as_ref().map(|_| "<installed>"))
            .field("webhook", &self.webhook.as_ref().map(|_| "<installed>"))
            .finish()
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::time::Duration;

    #[cfg(feature = "tokio")]
    fn run_tokio_local<F>(future: F) -> F::Output
    where
        F: std::future::Future,
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio current-thread runtime");
        let local = tokio::task::LocalSet::new();
        local.block_on(&runtime, future)
    }

    #[cfg(feature = "tokio")]
    async fn wait_until(label: &str, mut done: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !done() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {label}"));
    }

    #[cfg(feature = "tokio-http")]
    async fn read_http_request(stream: &mut tokio::net::TcpStream) -> String {
        use tokio::io::AsyncReadExt;

        let mut buf = Vec::new();
        let mut chunk = [0_u8; 1024];
        let mut header_end = None;
        let mut content_length = 0_usize;

        loop {
            let n = stream.read(&mut chunk).await.expect("read http request");
            assert_ne!(n, 0, "client closed before full http request arrived");
            buf.extend_from_slice(&chunk[..n]);

            if header_end.is_none() {
                if let Some(pos) = buf.windows(4).position(|window| window == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&buf[..pos]);
                    content_length = headers
                        .lines()
                        .filter_map(|line| line.split_once(':'))
                        .find_map(|(key, value)| {
                            key.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())
                                .flatten()
                        })
                        .unwrap_or(0);
                    header_end = Some(pos + 4);
                }
            }

            if let Some(end) = header_end {
                if buf.len() >= end + content_length {
                    break;
                }
            }
        }

        String::from_utf8_lossy(&buf).into_owned()
    }

    #[cfg(feature = "tokio-websocket")]
    struct CaptureWebSocketHeader {
        key: &'static str,
        target: Rc<RefCell<Option<String>>>,
    }

    #[cfg(feature = "tokio-websocket")]
    impl tokio_tungstenite::tungstenite::handshake::server::Callback for CaptureWebSocketHeader {
        #[allow(clippy::result_large_err)]
        fn on_request(
            self,
            request: &tokio_tungstenite::tungstenite::handshake::server::Request,
            response: tokio_tungstenite::tungstenite::handshake::server::Response,
        ) -> Result<
            tokio_tungstenite::tungstenite::handshake::server::Response,
            tokio_tungstenite::tungstenite::handshake::server::ErrorResponse,
        > {
            *self.target.borrow_mut() = request
                .headers()
                .get(self.key)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            Ok(response)
        }
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn tokio_process_driver_runs_real_process_boundary() {
        run_tokio_local(async {
            let result = Rc::new(RefCell::new(None::<Result<ProcessResult, String>>));
            let result_for_callback = result.clone();
            let cancel = TokioProcessDriver.run(
                ProcessCommand::new("sh").args(["-c", "printf graphrefly"]),
                Box::new(move |value| {
                    *result_for_callback.borrow_mut() =
                        Some(value.map_err(|error| error.to_string()));
                }),
            );

            wait_until("process driver callback", || result.borrow().is_some()).await;
            cancel();

            let result = result
                .borrow_mut()
                .take()
                .expect("process driver callback fired")
                .expect("process completed");
            assert_eq!(result.stdout, "graphrefly");
            assert_eq!(result.stderr, "");
            assert_eq!(result.exit_code, Some(0));
        });
    }

    #[cfg(feature = "tokio-http")]
    #[test]
    fn tokio_http_driver_requests_loopback_server() {
        run_tokio_local(async {
            use tokio::io::AsyncWriteExt;

            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind loopback http server");
            let addr = listener.local_addr().expect("loopback addr");
            let seen_request = Rc::new(RefCell::new(String::new()));
            let seen_request_for_task = seen_request.clone();
            tokio::task::spawn_local(async move {
                let (mut stream, _) = listener.accept().await.expect("accept http client");
                *seen_request_for_task.borrow_mut() = read_http_request(&mut stream).await;
                stream
                    .write_all(
                        b"HTTP/1.1 201 Created\r\nx-answer: 42\r\nContent-Length: 2\r\n\r\nok",
                    )
                    .await
                    .expect("write http response");
            });

            let result = Rc::new(RefCell::new(None::<Result<HttpResponse, String>>));
            let result_for_callback = result.clone();
            let cancel = TokioHttpDriver::new().request(
                HttpRequest::new("POST", format!("http://{addr}/orders"))
                    .header("x-test", "yes")
                    .body(b"hi".to_vec()),
                Box::new(move |value| {
                    *result_for_callback.borrow_mut() =
                        Some(value.map_err(|error| error.to_string()));
                }),
            );

            wait_until("http driver callback", || result.borrow().is_some()).await;
            cancel();

            let response = result
                .borrow_mut()
                .take()
                .expect("http driver callback fired")
                .expect("http response");
            assert_eq!(response.status, 201);
            assert_eq!(response.body, b"ok".to_vec());
            assert!(response
                .headers
                .iter()
                .any(|(key, value)| key == "x-answer" && value == "42"));
            let request = seen_request.borrow();
            assert!(request.starts_with("POST /orders HTTP/1.1"));
            assert!(request.contains("x-test: yes"));
            assert!(request.ends_with("hi"));
        });
    }

    #[cfg(feature = "tokio-http-stream")]
    #[test]
    fn tokio_http_stream_driver_streams_loopback_response_body() {
        run_tokio_local(async {
            use tokio::io::AsyncWriteExt;

            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind loopback http stream server");
            let addr = listener.local_addr().expect("loopback addr");
            let seen_request = Rc::new(RefCell::new(String::new()));
            let seen_request_for_task = seen_request.clone();
            tokio::task::spawn_local(async move {
                let (mut stream, _) = listener.accept().await.expect("accept http client");
                *seen_request_for_task.borrow_mut() = read_http_request(&mut stream).await;
                stream
                    .write_all(
                        b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nContent-Length: 5\r\n\r\nhello",
                    )
                    .await
                    .expect("write http stream response");
            });

            let events = Rc::new(RefCell::new(Vec::<String>::new()));
            let events_for_callback = events.clone();
            let cancel = TokioHttpStreamDriver::new().stream(
                HttpRequest::get(format!("http://{addr}/events"))
                    .header("accept", "text/event-stream"),
                Rc::new(move |event| match event {
                    HttpStreamDriverEvent::Head(head) => {
                        events_for_callback
                            .borrow_mut()
                            .push(format!("head:{}", head.status));
                    }
                    HttpStreamDriverEvent::Chunk(chunk) => {
                        events_for_callback
                            .borrow_mut()
                            .push(format!("chunk:{}", String::from_utf8_lossy(&chunk)));
                    }
                    HttpStreamDriverEvent::Error(error) => {
                        events_for_callback
                            .borrow_mut()
                            .push(format!("error:{error}"));
                    }
                    HttpStreamDriverEvent::Complete => {
                        events_for_callback.borrow_mut().push("complete".to_owned());
                    }
                }),
            );

            wait_until("http stream complete", || {
                events.borrow().iter().any(|event| event == "complete")
            })
            .await;
            cancel();

            assert_eq!(events.borrow().first(), Some(&"head:200".to_owned()));
            assert!(events.borrow().contains(&"chunk:hello".to_owned()));
            assert_eq!(events.borrow().last(), Some(&"complete".to_owned()));
            let request = seen_request.borrow();
            assert!(request.starts_with("GET /events HTTP/1.1"));
            assert!(request.contains("accept: text/event-stream"));
        });
    }

    #[cfg(feature = "tokio-websocket")]
    #[test]
    fn tokio_websocket_driver_connects_and_streams_events() {
        run_tokio_local(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind loopback websocket server");
            let addr = listener.local_addr().expect("loopback addr");
            let seen_header = Rc::new(RefCell::new(None::<String>));
            let seen_header_for_task = seen_header.clone();
            tokio::task::spawn_local(async move {
                let (stream, _) = listener.accept().await.expect("accept websocket client");
                let mut socket = tokio_tungstenite::accept_hdr_async(
                    stream,
                    CaptureWebSocketHeader {
                        key: "x-graphrefly",
                        target: seen_header_for_task,
                    },
                )
                .await
                .expect("accept websocket handshake");
                socket
                    .send(tokio_tungstenite::tungstenite::Message::Text(
                        "hello".into(),
                    ))
                    .await
                    .expect("send websocket text");
                socket.close(None).await.expect("close websocket");
            });

            let events = Rc::new(RefCell::new(Vec::<String>::new()));
            let events_for_callback = events.clone();
            let cancel = TokioWebSocketDriver.connect(
                WebSocketRequest::new(format!("ws://{addr}")).header("x-graphrefly", "connect"),
                Rc::new(move |event| match event {
                    WebSocketDriverEvent::Event(WebSocketEvent::Open) => {
                        events_for_callback.borrow_mut().push("open".to_owned());
                    }
                    WebSocketDriverEvent::Event(WebSocketEvent::Text(text)) => {
                        events_for_callback
                            .borrow_mut()
                            .push(format!("text:{text}"));
                    }
                    WebSocketDriverEvent::Event(WebSocketEvent::Binary(_))
                    | WebSocketDriverEvent::Event(WebSocketEvent::Close { .. }) => {}
                    WebSocketDriverEvent::Error(error) => {
                        events_for_callback
                            .borrow_mut()
                            .push(format!("error:{error}"));
                    }
                    WebSocketDriverEvent::Complete => {
                        events_for_callback.borrow_mut().push("complete".to_owned());
                    }
                }),
            );

            wait_until("websocket complete", || {
                events.borrow().iter().any(|event| event == "complete")
            })
            .await;
            cancel();

            assert!(events.borrow().contains(&"open".to_owned()));
            assert!(events.borrow().contains(&"text:hello".to_owned()));
            assert_eq!(events.borrow().last(), Some(&"complete".to_owned()));
            assert_eq!(seen_header.borrow().as_deref(), Some("connect"));
        });
    }

    #[cfg(feature = "tokio-websocket")]
    #[test]
    fn tokio_websocket_driver_sends_one_shot_message() {
        run_tokio_local(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind loopback websocket server");
            let addr = listener.local_addr().expect("loopback addr");
            let received = Rc::new(RefCell::new(None::<String>));
            let received_for_task = received.clone();
            let seen_header = Rc::new(RefCell::new(None::<String>));
            let seen_header_for_task = seen_header.clone();
            tokio::task::spawn_local(async move {
                let (stream, _) = listener.accept().await.expect("accept websocket client");
                let mut socket = tokio_tungstenite::accept_hdr_async(
                    stream,
                    CaptureWebSocketHeader {
                        key: "x-graphrefly",
                        target: seen_header_for_task,
                    },
                )
                .await
                .expect("accept websocket handshake");
                while let Some(message) = socket.next().await {
                    match message.expect("websocket message") {
                        tokio_tungstenite::tungstenite::Message::Binary(bytes) => {
                            *received_for_task.borrow_mut() =
                                Some(format!("binary:{}", String::from_utf8_lossy(&bytes)));
                            break;
                        }
                        tokio_tungstenite::tungstenite::Message::Text(text) => {
                            *received_for_task.borrow_mut() = Some(format!("text:{text}"));
                            break;
                        }
                        tokio_tungstenite::tungstenite::Message::Close(_) => break,
                        tokio_tungstenite::tungstenite::Message::Ping(_)
                        | tokio_tungstenite::tungstenite::Message::Pong(_)
                        | tokio_tungstenite::tungstenite::Message::Frame(_) => {}
                    }
                }
            });

            let result = Rc::new(RefCell::new(None::<Result<WebSocketSendResult, String>>));
            let result_for_callback = result.clone();
            let cancel = TokioWebSocketDriver
                .send(
                    WebSocketRequest::new(format!("ws://{addr}")).header("x-graphrefly", "send"),
                    WebSocketSend::text("hello"),
                    Box::new(move |value| {
                        *result_for_callback.borrow_mut() =
                            Some(value.map_err(|error| error.to_string()));
                    }),
                )
                .expect("send capability installed");

            wait_until("websocket send callback and server receive", || {
                result.borrow().is_some() && received.borrow().is_some()
            })
            .await;
            cancel();

            assert_eq!(
                result
                    .borrow_mut()
                    .take()
                    .expect("websocket send callback fired")
                    .expect("websocket send result"),
                WebSocketSendResult { sent: true }
            );
            assert_eq!(received.borrow().as_deref(), Some("text:hello"));
            assert_eq!(seen_header.borrow().as_deref(), Some("send"));
        });
    }

    #[cfg(feature = "tokio-websocket")]
    #[test]
    fn tokio_websocket_driver_session_sends_over_same_connection_and_closes() {
        run_tokio_local(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind loopback websocket server");
            let addr = listener.local_addr().expect("loopback addr");
            let received = Rc::new(RefCell::new(Vec::<String>::new()));
            let received_for_task = received.clone();
            let closed = Rc::new(std::cell::Cell::new(false));
            let closed_for_task = closed.clone();
            tokio::task::spawn_local(async move {
                let (stream, _) = listener.accept().await.expect("accept websocket client");
                let mut socket = tokio_tungstenite::accept_async(stream)
                    .await
                    .expect("accept websocket handshake");
                while let Some(message) = socket.next().await {
                    match message.expect("websocket message") {
                        tokio_tungstenite::tungstenite::Message::Text(text) => {
                            received_for_task.borrow_mut().push(text.to_string());
                        }
                        tokio_tungstenite::tungstenite::Message::Binary(bytes) => {
                            received_for_task
                                .borrow_mut()
                                .push(String::from_utf8_lossy(&bytes).into_owned());
                        }
                        tokio_tungstenite::tungstenite::Message::Close(_) => {
                            closed_for_task.set(true);
                            break;
                        }
                        tokio_tungstenite::tungstenite::Message::Ping(_)
                        | tokio_tungstenite::tungstenite::Message::Pong(_)
                        | tokio_tungstenite::tungstenite::Message::Frame(_) => {}
                    }
                }
            });

            let events = Rc::new(RefCell::new(Vec::<String>::new()));
            let events_for_callback = events.clone();
            let session = TokioWebSocketDriver
                .connect_session(
                    WebSocketRequest::new(format!("ws://{addr}")),
                    Rc::new(move |event| match event {
                        WebSocketDriverEvent::Event(WebSocketEvent::Open) => {
                            events_for_callback.borrow_mut().push("open".to_owned());
                        }
                        WebSocketDriverEvent::Event(WebSocketEvent::Close { .. }) => {
                            events_for_callback.borrow_mut().push("close".to_owned());
                        }
                        WebSocketDriverEvent::Complete => {
                            events_for_callback.borrow_mut().push("complete".to_owned());
                        }
                        WebSocketDriverEvent::Event(WebSocketEvent::Text(_))
                        | WebSocketDriverEvent::Event(WebSocketEvent::Binary(_)) => {}
                        WebSocketDriverEvent::Error(error) => {
                            events_for_callback
                                .borrow_mut()
                                .push(format!("error:{error}"));
                        }
                    }),
                )
                .expect("session capability installed");
            wait_until("session open", || {
                events.borrow().iter().any(|event| event == "open")
            })
            .await;

            let first = Rc::new(RefCell::new(None::<Result<WebSocketSendResult, String>>));
            let first_for_callback = first.clone();
            let _cancel_first = session.send(
                WebSocketSend::text("one"),
                Box::new(move |result| {
                    *first_for_callback.borrow_mut() =
                        Some(result.map_err(|error| error.to_string()));
                }),
            );
            wait_until("first session send", || {
                first.borrow().is_some() && received.borrow().len() == 1
            })
            .await;
            let second = Rc::new(RefCell::new(None::<Result<WebSocketSendResult, String>>));
            let second_for_callback = second.clone();
            let _cancel_second = session.send(
                WebSocketSend::text("two"),
                Box::new(move |result| {
                    *second_for_callback.borrow_mut() =
                        Some(result.map_err(|error| error.to_string()));
                }),
            );

            wait_until("second session send", || {
                second.borrow().is_some() && received.borrow().len() == 2
            })
            .await;
            session.close(Some(1000), Some("done".to_owned()));
            wait_until("session close reaches server", || closed.get()).await;

            assert_eq!(
                first
                    .borrow_mut()
                    .take()
                    .expect("first send callback")
                    .expect("first send result"),
                WebSocketSendResult { sent: true }
            );
            assert_eq!(
                second
                    .borrow_mut()
                    .take()
                    .expect("second send callback")
                    .expect("second send result"),
                WebSocketSendResult { sent: true }
            );
            assert_eq!(
                received.borrow().as_slice(),
                &["one".to_owned(), "two".to_owned()]
            );
        });
    }
}
