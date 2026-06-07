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

/// Process command request for graph environment process drivers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessCommand {
    pub program: String,
    pub args: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub env: Vec<(String, String)>,
}

impl ProcessCommand {
    pub fn new(program: impl Into<String>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            cwd: None,
            env: Vec::new(),
        }
    }

    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.args = args.into_iter().map(Into::into).collect();
        self
    }

    pub fn cwd(mut self, cwd: impl Into<PathBuf>) -> Self {
        self.cwd = Some(cwd.into());
        self
    }

    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.push((key.into(), value.into()));
        self
    }
}

/// Completed process result. Exit status is DATA, including non-zero exits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: Option<i32>,
    pub signal: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRequest {
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl HttpRequest {
    pub fn new(method: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            method: method.into(),
            url: url.into(),
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    pub fn get(url: impl Into<String>) -> Self {
        Self::new("GET", url)
    }

    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((key.into(), value.into()));
        self
    }

    pub fn body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.body = body.into();
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseRequest {
    pub url: String,
    pub headers: Vec<(String, String)>,
}

impl SseRequest {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            headers: Vec::new(),
        }
    }

    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((key.into(), value.into()));
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseEvent {
    pub event: Option<String>,
    pub data: String,
    pub id: Option<String>,
    pub retry_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebSocketRequest {
    pub url: String,
    pub headers: Vec<(String, String)>,
}

impl WebSocketRequest {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            headers: Vec::new(),
        }
    }

    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((key.into(), value.into()));
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebSocketEvent {
    Open,
    Text(String),
    Binary(Vec<u8>),
    Close {
        code: Option<u16>,
        reason: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookRegistration {
    pub id: String,
    pub method: Option<String>,
    pub path: Option<String>,
}

impl WebhookRegistration {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            method: None,
            path: None,
        }
    }

    pub fn method(mut self, method: impl Into<String>) -> Self {
        self.method = Some(method.into());
        self
    }

    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookEvent {
    pub registration_id: String,
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub query: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// Graph-local process driver. Implementations own process/runtime details.
pub trait LocalProcessDriver {
    fn run(
        &self,
        command: ProcessCommand,
        callback: Box<dyn FnOnce(Result<ProcessResult, GraphError>)>,
    ) -> DriverCancel;
}

pub trait LocalHttpDriver {
    fn request(
        &self,
        request: HttpRequest,
        callback: Box<dyn FnOnce(Result<HttpResponse, GraphError>)>,
    ) -> DriverCancel;
}

pub enum SseDriverEvent {
    Event(SseEvent),
    Error(GraphError),
    Complete,
}

pub trait LocalSseDriver {
    fn connect(&self, request: SseRequest, callback: Rc<dyn Fn(SseDriverEvent)>) -> DriverCancel;
}

pub enum WebSocketDriverEvent {
    Event(WebSocketEvent),
    Error(GraphError),
    Complete,
}

pub trait LocalWebSocketDriver {
    fn connect(
        &self,
        request: WebSocketRequest,
        callback: Rc<dyn Fn(WebSocketDriverEvent)>,
    ) -> DriverCancel;
}

pub enum WebhookDriverEvent {
    Event(WebhookEvent),
    Error(GraphError),
    Complete,
}

pub trait LocalWebhookDriver {
    fn register(
        &self,
        registration: WebhookRegistration,
        callback: Rc<dyn Fn(WebhookDriverEvent)>,
    ) -> DriverCancel;
}

#[cfg(feature = "tokio")]
#[derive(Debug, Clone, Copy, Default)]
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
    sse: Option<Rc<dyn LocalSseDriver>>,
    websocket: Option<Rc<dyn LocalWebSocketDriver>>,
    webhook: Option<Rc<dyn LocalWebhookDriver>>,
}

impl EnvironmentDrivers {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_local_async(mut self, driver: Rc<dyn LocalAsyncDriver>) -> Self {
        self.local_async = Some(driver);
        self
    }

    pub fn with_process(mut self, driver: Rc<dyn LocalProcessDriver>) -> Self {
        self.process = Some(driver);
        self
    }

    pub fn with_http(mut self, driver: Rc<dyn LocalHttpDriver>) -> Self {
        self.http = Some(driver);
        self
    }

    pub fn with_sse(mut self, driver: Rc<dyn LocalSseDriver>) -> Self {
        self.sse = Some(driver);
        self
    }

    pub fn with_websocket(mut self, driver: Rc<dyn LocalWebSocketDriver>) -> Self {
        self.websocket = Some(driver);
        self
    }

    pub fn with_webhook(mut self, driver: Rc<dyn LocalWebhookDriver>) -> Self {
        self.webhook = Some(driver);
        self
    }

    pub fn local_async_driver(&self) -> Option<Rc<dyn LocalAsyncDriver>> {
        self.local_async.clone()
    }

    pub fn process_driver(&self) -> Option<Rc<dyn LocalProcessDriver>> {
        self.process.clone()
    }

    pub fn http_driver(&self) -> Option<Rc<dyn LocalHttpDriver>> {
        self.http.clone()
    }

    pub fn sse_driver(&self) -> Option<Rc<dyn LocalSseDriver>> {
        self.sse.clone()
    }

    pub fn websocket_driver(&self) -> Option<Rc<dyn LocalWebSocketDriver>> {
        self.websocket.clone()
    }

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
            .field("sse", &self.sse.as_ref().map(|_| "<installed>"))
            .field("websocket", &self.websocket.as_ref().map(|_| "<installed>"))
            .field("webhook", &self.webhook.as_ref().map(|_| "<installed>"))
            .finish()
    }
}
