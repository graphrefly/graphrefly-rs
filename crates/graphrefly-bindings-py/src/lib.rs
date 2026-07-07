//! `PyO3` foundation for Python hosts over the clean-slate Rust engine (D415).
//!
//! This crate intentionally exposes a small native binding layer, not the final
//! Python package API. Python owns idiomatic typing/decorators/context managers,
//! value registries, exception taxonomy, asyncio/runtime adapters, ecosystem
//! adapters, and vertical recipes. Rust owns the synchronous graph engine and
//! reusable graph-infrastructure foundation.
//!
//! Boundary rules pinned here:
//! - `#[pyclass(unsendable)]`: the Rust graph is a D22 single-thread causal
//!   domain (`Rc`, not `Arc`); bindings must not pretend to be `Send`.
//! - Python callbacks are installed as Rust node fns, so invocation goes through
//!   the dispatcher (F-DISPATCH-ALL).
//! - Python values are held as owned `Py<PyAny>` payloads inside Rust `AnyValue`.
//!   `None` is a valid DATA payload; absence-of-DATA is represented by native
//!   cache presence flags, not by Python `None`.
//! - Python callback exceptions become graph `ERROR` messages at the Rust graph
//!   boundary; the richer Python exception hierarchy is a CSP-7 concern.

#![warn(rust_2018_idioms, unreachable_pub)]
#![warn(clippy::pedantic)]
#![allow(
    clippy::missing_errors_doc,
    clippy::module_name_repetitions,
    clippy::needless_pass_by_value,
    clippy::too_many_arguments,
    clippy::fn_params_excessive_bools
)]

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashSet};
use std::error::Error;
use std::fmt;
use std::panic::{self, catch_unwind, AssertUnwindSafe};
use std::rc::Rc;
use std::sync::{Mutex, OnceLock};

use graphrefly_rs::{
    decode_canonical_wire_bridge_envelope, decode_canonical_wire_edge_frame,
    decode_wire_bridge_protobuf_bytes, encode_canonical_wire_bridge_envelope,
    encode_canonical_wire_edge_frame, encode_wire_bridge_protobuf_bytes, restored_opts,
    wire_bridge, wire_edge_group, AnyValue, BackoffPolicy, CanonicalProtobufError, Core, Ctx,
    DeferredCtx, DepTerminal, DescribeSnapshot, DescribeValue, Graph, GraphCheckpoint,
    GraphCheckpointJson, GraphNode, GraphNodeOpts, GraphRestoreDescriptor, GraphRestoreEntry,
    GraphRestoreError, GraphRestoreRegistry, GraphRestoreResult, LockId, MapJsonRestoreDescriptor,
    Message, Node, NodeOpts, NodeVersion, Operator, Pausable, PoolKind, PullDemand,
    RestoreDefineCtx, RestoreFactoryMeta, RestoreGraphOptions, RestoreNodeDefinition,
    RestoreNodeKind, RetryPolicy, StateRestoreDescriptor, Status, TopologyGroup,
    TopologyGroupOptions, WaveData, WireBridgeAck, WireBridgeAttempt, WireBridgeBundle,
    WireBridgeCommand, WireBridgeEnvelope, WireBridgeEnvelopeType, WireBridgeIngress,
    WireBridgeNack, WireBridgeOptions, WireBridgePayload, WireBridgeProtobufDataBody,
    WireBridgeProtobufEnvelope, WireBridgeProtobufPayload, WireBridgeStatus, WireBridgeStatusState,
    WireEdgeGroupBundle, WireEdgeGroupEdge, WireEdgeGroupIssue, WireEdgeGroupIssueCode,
    WireEdgeGroupOptions, WireEdgeGroupStatus, WireEdgeGroupStatusState,
};
use pyo3::exceptions::{PyException, PyIndexError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple,
};
use pyo3::IntoPyObjectExt;
use serde_json::{Map as JsonMap, Number as JsonNumber};

type PendingFatal = Rc<RefCell<Option<PyErr>>>;

/// Python-owned DATA payload stored in Rust `AnyValue`.
struct PyValue {
    object: Py<PyAny>,
}

impl PyValue {
    fn new(object: Py<PyAny>) -> Self {
        Self { object }
    }

    fn clone_object(&self, py: Python<'_>) -> Py<PyAny> {
        self.object.clone_ref(py)
    }
}

impl Clone for PyValue {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            object: self.object.clone_ref(py),
        })
    }
}

fn register_py_checkpoint_encoder() {
    graphrefly_rs::__binding_private::register_checkpoint_json_encoder::<PyValue>(|value, path| {
        Python::attach(|py| {
            py_to_checkpoint_json(py, &value.object).map_err(|err| {
                GraphRestoreError::new(format!(
                    "checkpoint: value at {path} is not strict JSON compatible: {err}"
                ))
            })
        })
    });
}

#[derive(Debug)]
struct PyCallbackError {
    message: String,
}

impl fmt::Display for PyCallbackError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for PyCallbackError {}

fn py_exception_to_error(error: PyErr) -> graphrefly_rs::GraphError {
    let message = Python::attach(|py| format_callback_error(py, &error));
    Box::new(PyCallbackError { message })
}

fn py_error_is_fatal(py: Python<'_>, error: &PyErr) -> bool {
    !error.is_instance_of::<PyException>(py)
}

fn store_pending_fatal(pending: &PendingFatal, error: PyErr) {
    let mut slot = pending.borrow_mut();
    if slot.is_none() {
        *slot = Some(error);
    }
}

fn raise_pending_fatal(pending: &PendingFatal) -> PyResult<()> {
    match pending.borrow_mut().take() {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn catch_graph_panic<T>(pending_fatal: &PendingFatal, f: impl FnOnce() -> T) -> PyResult<T> {
    static PANIC_HOOK_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    let hook_lock = PANIC_HOOK_LOCK.get_or_init(|| Mutex::new(()));
    let result = if let Ok(_guard) = hook_lock.try_lock() {
        let previous_hook = panic::take_hook();
        panic::set_hook(Box::new(|_| {}));
        let result = catch_unwind(AssertUnwindSafe(|| {
            graphrefly_rs::host_boundary::with_host_boundary_abort_armed(f)
        }));
        panic::set_hook(previous_hook);
        result
    } else {
        catch_unwind(AssertUnwindSafe(|| {
            graphrefly_rs::host_boundary::with_host_boundary_abort_armed(f)
        }))
    };

    result.map_err(|payload| {
        if graphrefly_rs::host_boundary::is_host_boundary_abort_payload(payload.as_ref()) {
            return raise_pending_fatal(pending_fatal).err().unwrap_or_else(|| {
                PyRuntimeError::new_err("host boundary abort without pending fatal")
            });
        }
        let message = payload.downcast_ref::<String>().map_or_else(
            || {
                payload
                    .downcast_ref::<&'static str>()
                    .map_or("Rust graph operation panicked", |message| *message)
                    .to_owned()
            },
            Clone::clone,
        );
        PyRuntimeError::new_err(message)
    })
}

fn format_callback_error(py: Python<'_>, error: &PyErr) -> String {
    let ty = error
        .get_type(py)
        .name()
        .map_or_else(|_| "Exception".to_owned(), |name| name.to_string());
    format!("{ty}: {}", error.value(py))
}

fn emit_callback_result(ctx: &Ctx, pending_fatal: &PendingFatal, result: PyResult<Py<PyAny>>) {
    match result {
        Ok(value) => {
            ctx.emit(PyValue::new(value));
        }
        Err(error) => {
            Python::attach(|py| {
                if py_error_is_fatal(py, &error) {
                    store_pending_fatal(pending_fatal, error);
                    graphrefly_rs::host_boundary::abort_host_boundary();
                } else {
                    ctx.down(vec![Message::Error(py_exception_to_error(error))]);
                }
            });
        }
    }
}

fn handle_callback_void_result(
    ctx: &Ctx,
    pending_fatal: &PendingFatal,
    result: PyResult<Py<PyAny>>,
) {
    if let Err(error) = result {
        Python::attach(|py| {
            if py_error_is_fatal(py, &error) {
                store_pending_fatal(pending_fatal, error);
                graphrefly_rs::host_boundary::abort_host_boundary();
            } else {
                ctx.down(vec![Message::Error(py_exception_to_error(error))]);
            }
        });
    }
}

fn value_from_any(py: Python<'_>, value: &AnyValue) -> PyResult<Py<PyAny>> {
    if let Some(value) = value.downcast_ref::<PyValue>() {
        return Ok(value.clone_object(py));
    }
    if let Some(value) = value.downcast_ref::<GraphCheckpointJson>() {
        return checkpoint_json_to_py(py, value);
    }
    Err(PyTypeError::new_err(
        "cached value is not owned by the Python binding foundation",
    ))
}

fn ctx_data_value(py: Python<'_>, ctx: &Ctx, index: usize) -> PyResult<Option<Py<PyAny>>> {
    if let Some(value) = ctx.data::<PyValue>(index) {
        return Ok(Some(value.clone_object(py)));
    }
    if let Some(value) = ctx.data::<GraphCheckpointJson>(index) {
        return checkpoint_json_to_py(py, value.as_ref()).map(Some);
    }
    Ok(None)
}

fn deferred_data_value(
    py: Python<'_>,
    ctx: &DeferredCtx,
    index: usize,
) -> PyResult<Option<Py<PyAny>>> {
    if let Some(value) = ctx.data::<PyValue>(index) {
        return Ok(Some(value.clone_object(py)));
    }
    if let Some(value) = ctx.data::<GraphCheckpointJson>(index) {
        return checkpoint_json_to_py(py, value.as_ref()).map(Some);
    }
    Ok(None)
}

fn ctx_state_value(py: Python<'_>, ctx: &Ctx) -> PyResult<Option<Py<PyAny>>> {
    if let Some(value) = ctx.state_get::<PyValue>() {
        return Ok(Some(value.clone_object(py)));
    }
    if let Some(value) = ctx.state_get::<GraphCheckpointJson>() {
        return checkpoint_json_to_py(py, value.as_ref()).map(Some);
    }
    Ok(None)
}

fn py_value_from_msg(py: Python<'_>, msg: &Message<AnyValue>) -> PyResult<Py<PyAny>> {
    match msg {
        Message::Data(value) => value_from_any(py, value),
        Message::Pause(lock) | Message::Resume(lock) => Ok(py_string(py, &lock.0)),
        Message::Pull(demand) => Ok(py_string(py, &demand.pull_id.0)),
        Message::Error(error) => Ok(py_string(py, &error.to_string())),
        Message::Start
        | Message::Dirty
        | Message::Resolved
        | Message::Invalidate
        | Message::Complete
        | Message::Teardown => Ok(py.None()),
    }
}

fn py_string(py: Python<'_>, value: &str) -> Py<PyAny> {
    PyString::new(py, value).into_any().unbind()
}

#[pyclass(name = "CanonicalProtobufValidation", unsendable)]
struct PyCanonicalProtobufValidation {
    ok: bool,
    category: Option<String>,
    message: Option<String>,
}

#[pyclass(name = "CanonicalProtobufRoundtrip", unsendable)]
struct PyCanonicalProtobufRoundtrip {
    ok: bool,
    bytes: Option<Vec<u8>>,
    category: Option<String>,
    message: Option<String>,
}

#[pymethods]
impl PyCanonicalProtobufValidation {
    #[getter]
    fn ok(&self) -> bool {
        self.ok
    }

    #[getter]
    fn category(&self) -> Option<String> {
        self.category.clone()
    }

    #[getter]
    fn message(&self) -> Option<String> {
        self.message.clone()
    }
}

#[pymethods]
impl PyCanonicalProtobufRoundtrip {
    #[getter]
    fn ok(&self) -> bool {
        self.ok
    }

    #[getter]
    fn bytes(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.bytes
            .as_deref()
            .map(|bytes| PyBytes::new(py, bytes).into_any().unbind())
    }

    #[getter]
    fn category(&self) -> Option<String> {
        self.category.clone()
    }

    #[getter]
    fn message(&self) -> Option<String> {
        self.message.clone()
    }
}

fn canonical_protobuf_validation(
    py: Python<'_>,
    result: Result<(), CanonicalProtobufError>,
) -> PyResult<Py<PyCanonicalProtobufValidation>> {
    let validation = match result {
        Ok(()) => PyCanonicalProtobufValidation {
            ok: true,
            category: None,
            message: None,
        },
        Err(error) => PyCanonicalProtobufValidation {
            ok: false,
            category: Some(error.category.as_str().to_owned()),
            message: Some(error.to_string()),
        },
    };
    Py::new(py, validation)
}

fn canonical_protobuf_roundtrip(
    py: Python<'_>,
    result: Result<Vec<u8>, CanonicalProtobufError>,
) -> PyResult<Py<PyCanonicalProtobufRoundtrip>> {
    let roundtrip = match result {
        Ok(bytes) => PyCanonicalProtobufRoundtrip {
            ok: true,
            bytes: Some(bytes),
            category: None,
            message: None,
        },
        Err(error) => PyCanonicalProtobufRoundtrip {
            ok: false,
            bytes: None,
            category: Some(error.category.as_str().to_owned()),
            message: Some(error.to_string()),
        },
    };
    Py::new(py, roundtrip)
}

#[pyfunction]
fn _validate_canonical_wire_bridge_envelope(
    py: Python<'_>,
    bytes: &Bound<'_, pyo3::types::PyBytes>,
) -> PyResult<Py<PyCanonicalProtobufValidation>> {
    canonical_protobuf_validation(
        py,
        decode_canonical_wire_bridge_envelope(bytes.as_bytes()).map(|_| ()),
    )
}

#[pyfunction]
fn _roundtrip_canonical_wire_bridge_envelope(
    py: Python<'_>,
    bytes: &Bound<'_, pyo3::types::PyBytes>,
) -> PyResult<Py<PyCanonicalProtobufRoundtrip>> {
    canonical_protobuf_roundtrip(
        py,
        decode_canonical_wire_bridge_envelope(bytes.as_bytes())
            .and_then(|envelope| encode_canonical_wire_bridge_envelope(&envelope)),
    )
}

#[pyfunction]
fn _validate_canonical_wire_edge_frame(
    py: Python<'_>,
    bytes: &Bound<'_, pyo3::types::PyBytes>,
) -> PyResult<Py<PyCanonicalProtobufValidation>> {
    canonical_protobuf_validation(
        py,
        decode_canonical_wire_edge_frame(bytes.as_bytes()).map(|_| ()),
    )
}

#[pyfunction]
fn _roundtrip_canonical_wire_edge_frame(
    py: Python<'_>,
    bytes: &Bound<'_, pyo3::types::PyBytes>,
) -> PyResult<Py<PyCanonicalProtobufRoundtrip>> {
    canonical_protobuf_roundtrip(
        py,
        decode_canonical_wire_edge_frame(bytes.as_bytes())
            .and_then(|frame| encode_canonical_wire_edge_frame(&frame)),
    )
}

type PyWireBridgeCore = WireBridgeBundle<WireBridgeProtobufDataBody, WireBridgeProtobufDataBody>;

#[pyclass(name = "_WireBridge", unsendable)]
struct PyWireBridge {
    bridge: Rc<PyWireBridgeCore>,
    status: PyNode,
    issues: PyNode,
    released: Cell<bool>,
}

#[pymethods]
impl PyWireBridge {
    #[getter]
    fn status(&self) -> PyNode {
        self.status.clone()
    }

    #[getter]
    fn issues(&self) -> PyNode {
        self.issues.clone()
    }

    fn release(&self) {
        self.released.set(true);
    }
}

#[pyclass(name = "_WireBridgeProtobuf", unsendable)]
struct PyWireBridgeProtobuf {
    bridge: Rc<PyWireBridgeCore>,
    topology: TopologyGroup,
    inbound_source: Node<WireBridgeIngress<WireBridgeProtobufDataBody>>,
    inbound_bytes: PyNode,
    outbound_bytes: PyNode,
    status: PyNode,
    issues: PyNode,
    released: Cell<bool>,
}

#[pymethods]
impl PyWireBridgeProtobuf {
    #[getter]
    fn inbound_bytes(&self) -> PyNode {
        self.inbound_bytes.clone()
    }

    #[getter]
    fn outbound_bytes(&self) -> PyNode {
        self.outbound_bytes.clone()
    }

    #[getter]
    fn status(&self) -> PyNode {
        self.status.clone()
    }

    #[getter]
    fn issues(&self) -> PyNode {
        self.issues.clone()
    }

    fn release(&self) {
        if self.released.get() {
            return;
        }
        self.bridge
            .detach_inbound_source_for_native(self.inbound_source.erased());
        let release = catch_unwind(AssertUnwindSafe(|| {
            self.topology
                .release_with_reason("wire_bridge_protobuf release");
        }));
        if let Err(panic) = release {
            self.bridge
                .attach_inbound_source_for_native(self.inbound_source.erased());
            panic::resume_unwind(panic);
        }
        self.released.set(true);
    }
}

#[pyclass(name = "_WireEdgeGroup", unsendable)]
struct PyWireEdgeGroup {
    group: Rc<WireEdgeGroupBundle>,
    facade_topology: TopologyGroup,
    adapter_topology: Option<TopologyGroup>,
    adapter_projection_topology: Option<TopologyGroup>,
    inbound_keepalives: RefCell<Vec<Box<dyn FnOnce()>>>,
    inbound_edges: Py<PyDict>,
    status: PyNode,
    issues: PyNode,
    released: Cell<bool>,
}

#[pymethods]
impl PyWireEdgeGroup {
    #[getter]
    fn inbound_edges(&self, py: Python<'_>) -> Py<PyAny> {
        self.inbound_edges.clone_ref(py).into_any()
    }

    #[getter]
    fn status(&self) -> PyNode {
        self.status.clone()
    }

    #[getter]
    fn issues(&self) -> PyNode {
        self.issues.clone()
    }

    fn release(&self) {
        if self.released.get() {
            return;
        }
        self.facade_topology
            .release_with_reason("wire_edge_group python facade release");
        if let Some(topology) = &self.adapter_projection_topology {
            topology.release_with_reason("wire_edge_group python adapter projection release");
        }
        for unsubscribe in self.inbound_keepalives.borrow_mut().drain(..) {
            unsubscribe();
        }
        self.group.release();
        if let Some(topology) = &self.adapter_topology {
            topology.release_with_reason("wire_edge_group python adapter release");
        }
        self.released.set(true);
    }
}

#[pyclass(name = "_WireBridgeAckDriver", unsendable)]
struct PyWireBridgeAckDriver {
    bridge: Rc<PyWireBridgeCore>,
    topology: TopologyGroup,
    command_source: Node<WireBridgeCommand<WireBridgeProtobufDataBody>>,
    subscriptions: RefCell<Vec<Box<dyn FnOnce()>>>,
    timeouts: PyNode,
    status: PyNode,
    issues: PyNode,
    released: Cell<bool>,
}

#[pymethods]
impl PyWireBridgeAckDriver {
    #[getter]
    fn timeouts(&self) -> PyNode {
        self.timeouts.clone()
    }

    #[getter]
    fn status(&self) -> PyNode {
        self.status.clone()
    }

    #[getter]
    fn issues(&self) -> PyNode {
        self.issues.clone()
    }

    fn release(&self) {
        if self.released.get() {
            return;
        }
        self.bridge
            .detach_command_source_for_native(self.command_source.erased());
        self.topology
            .release_with_reason("wire_bridge_ack_driver release");
        for unsubscribe in self.subscriptions.borrow_mut().drain(..) {
            unsubscribe();
        }
        self.released.set(true);
    }

    fn _conformance_ack_timeout(&self, seq: u64, attempt: u32, observed_at_ms: Option<u64>) {
        if self.released.get() {
            return;
        }
        self.command_source.set(WireBridgeCommand::AckTimeout {
            seq,
            attempt,
            observed_at_ms,
        });
    }
}

#[derive(Clone)]
enum PyProtobufEvent {
    Status {
        direction: &'static str,
        state: &'static str,
    },
    Issue {
        direction: &'static str,
        category: String,
        message: String,
    },
}

#[derive(Clone)]
struct PyAckDriverPending {
    seq: u64,
    attempt: u32,
    observed_at_ms: Option<u64>,
    timed_out_attempt: Option<u32>,
    timed_out_at_ms: Option<u64>,
    retry_due_at_ms: Option<u64>,
    retry_released_attempt: Option<u32>,
}

#[derive(Clone, Default)]
struct PyAckDriverState {
    now_ms: Option<u64>,
    pending: BTreeMap<u64, PyAckDriverPending>,
}

struct PySubscriptionDrainGuard {
    subscriptions: Option<Vec<Box<dyn FnOnce()>>>,
}

impl PySubscriptionDrainGuard {
    fn new(subscriptions: Vec<Box<dyn FnOnce()>>) -> Self {
        Self {
            subscriptions: Some(subscriptions),
        }
    }

    fn disarm(mut self) -> Vec<Box<dyn FnOnce()>> {
        self.subscriptions.take().unwrap_or_default()
    }
}

impl Drop for PySubscriptionDrainGuard {
    fn drop(&mut self) {
        if let Some(subscriptions) = self.subscriptions.take() {
            for unsubscribe in subscriptions {
                unsubscribe();
            }
        }
    }
}

#[derive(Clone)]
enum PyAckDriverEvent {
    State {
        pending: usize,
        now_ms: Option<u64>,
    },
    Timeout {
        seq: u64,
        attempt: u32,
        observed_at_ms: u64,
        pending: usize,
        now_ms: u64,
    },
    Issue {
        code: &'static str,
        message: &'static str,
        pending: usize,
        now_ms: Option<u64>,
    },
}

impl Clone for PyNode {
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
            graph_node: self.graph_node.clone(),
            pending_fatal: self.pending_fatal.clone(),
        }
    }
}

fn py_node(node: Node<PyValue>, pending_fatal: &PendingFatal) -> PyNode {
    PyNode {
        node,
        graph_node: None,
        pending_fatal: pending_fatal.clone(),
    }
}

fn py_bridge_status(py: Python<'_>, status: &WireBridgeStatus) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("state", wire_bridge_status_state(status.state))?;
    dict.set_item("session_id", status.session_id.clone())?;
    dict.set_item("cursor", status.cursor)?;
    dict.set_item("next_seq", status.next_seq)?;
    dict.set_item("pending", status.pending)?;
    dict.set_item("attempts", status.attempts)?;
    dict.set_item("acked", status.acked)?;
    dict.set_item("nacked", status.nacked)?;
    dict.set_item("errors", status.errors)?;
    dict.set_item("last_seq", status.last_seq)?;
    dict.set_item("last_delay_ms", status.last_delay_ms)?;
    Ok(dict.into_any().unbind())
}

fn wire_bridge_status_state(state: WireBridgeStatusState) -> &'static str {
    match state {
        WireBridgeStatusState::Idle => "idle",
        WireBridgeStatusState::Started => "started",
        WireBridgeStatusState::Open => "open",
        WireBridgeStatusState::Waiting => "waiting",
        WireBridgeStatusState::Closed => "closed",
        WireBridgeStatusState::Errored => "errored",
        WireBridgeStatusState::Exhausted => "exhausted",
    }
}

fn py_wire_edge_group_status(py: Python<'_>, status: &WireEdgeGroupStatus) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("state", wire_edge_group_status_state(status.state))?;
    dict.set_item("expected_edges", status.expected_edges.clone())?;
    dict.set_item("active_cause_id", status.active_cause_id.clone())?;
    dict.set_item("dirty", status.dirty)?;
    dict.set_item("data", status.data)?;
    dict.set_item("released", status.released)?;
    dict.set_item("issues", status.issues)?;
    if let Some(issue) = &status.last_issue {
        dict.set_item("last_issue", py_wire_edge_group_issue(py, issue)?)?;
    } else {
        dict.set_item("last_issue", py.None())?;
    }
    Ok(dict.into_any().unbind())
}

fn wire_edge_group_status_state(state: WireEdgeGroupStatusState) -> &'static str {
    match state {
        WireEdgeGroupStatusState::Idle => "idle",
        WireEdgeGroupStatusState::Collecting => "collecting",
        WireEdgeGroupStatusState::Released => "released",
        WireEdgeGroupStatusState::Issues => "issues",
    }
}

fn py_wire_edge_group_issue(py: Python<'_>, issue: &WireEdgeGroupIssue) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("code", wire_edge_group_issue_code(issue.code))?;
    dict.set_item("message", issue.message.clone())?;
    dict.set_item("edge_id", issue.edge_id.clone())?;
    dict.set_item("cause_id", issue.cause_id.clone())?;
    dict.set_item("active_cause_id", issue.active_cause_id.clone())?;
    Ok(dict.into_any().unbind())
}

fn wire_edge_group_issue_code(code: WireEdgeGroupIssueCode) -> &'static str {
    match code {
        WireEdgeGroupIssueCode::MissingSnapshot => "missing_snapshot",
        WireEdgeGroupIssueCode::UnknownEdge => "unknown_edge",
        WireEdgeGroupIssueCode::DuplicateDirty => "duplicate_dirty",
        WireEdgeGroupIssueCode::DuplicateData => "duplicate_data",
        WireEdgeGroupIssueCode::DataBeforeDirty => "data_before_dirty",
        WireEdgeGroupIssueCode::CompetingCause => "competing_cause",
        WireEdgeGroupIssueCode::MalformedFrame => "malformed_frame",
        WireEdgeGroupIssueCode::IncompleteCause => "incomplete_cause",
    }
}

fn py_status_node<T: Clone + 'static>(
    graph: &Graph,
    dep: &Node<T>,
    name: String,
    pending_fatal: &PendingFatal,
    map: impl Fn(Python<'_>, &T) -> PyResult<Py<PyAny>> + 'static,
) -> PyNode {
    let pending = pending_fatal.clone();
    let node = graph.node_opts::<PyValue, _>(
        vec![dep.erased()],
        move |ctx| {
            for value in ctx.batch::<T>(0) {
                Python::attach(|py| match map(py, value.as_ref()) {
                    Ok(value) => ctx.emit(PyValue::new(value)),
                    Err(error) => {
                        store_pending_fatal(&pending, error);
                        graphrefly_rs::host_boundary::abort_host_boundary();
                    }
                });
            }
        },
        graph_node_opts(Some(name)),
    );
    py_node(node, pending_fatal)
}

fn py_status_node_in_topology<T: Clone + 'static>(
    topology: &TopologyGroup,
    dep: &Node<T>,
    name: String,
    pending_fatal: &PendingFatal,
    map: impl Fn(Python<'_>, &T) -> PyResult<Py<PyAny>> + 'static,
) -> PyNode {
    let pending = pending_fatal.clone();
    let node = topology.node_opts::<PyValue, _>(
        vec![dep.erased()],
        move |ctx| {
            for value in ctx.batch::<T>(0) {
                Python::attach(|py| match map(py, value.as_ref()) {
                    Ok(value) => ctx.emit(PyValue::new(value)),
                    Err(error) => {
                        store_pending_fatal(&pending, error);
                        graphrefly_rs::host_boundary::abort_host_boundary();
                    }
                });
            }
        },
        graph_node_opts(Some(name)),
    );
    py_node(node, pending_fatal)
}

fn py_bytes_node_in_topology(
    topology: &TopologyGroup,
    dep: &Node<Vec<u8>>,
    name: String,
    pending_fatal: &PendingFatal,
) -> PyNode {
    py_status_node_in_topology(topology, dep, name, pending_fatal, |py, bytes| {
        Ok(PyBytes::new(py, bytes).into_any().unbind())
    })
}

fn py_error_issue_node(
    graph: &Graph,
    dep: &Node<String>,
    name: String,
    pending_fatal: &PendingFatal,
    code: &'static str,
) -> PyNode {
    py_status_node(graph, dep, name, pending_fatal, move |py, message| {
        let dict = PyDict::new(py);
        dict.set_item("code", code)?;
        dict.set_item("message", message.clone())?;
        Ok(dict.into_any().unbind())
    })
}

fn py_protobuf_event_status(
    py: Python<'_>,
    event: &PyProtobufEvent,
) -> PyResult<Option<Py<PyAny>>> {
    if let PyProtobufEvent::Status { direction, state } = event {
        let dict = PyDict::new(py);
        dict.set_item("direction", *direction)?;
        dict.set_item("state", *state)?;
        Ok(Some(dict.into_any().unbind()))
    } else {
        Ok(None)
    }
}

fn py_protobuf_event_issue(py: Python<'_>, event: &PyProtobufEvent) -> PyResult<Option<Py<PyAny>>> {
    if let PyProtobufEvent::Issue {
        direction,
        category,
        message,
    } = event
    {
        let dict = PyDict::new(py);
        dict.set_item("direction", *direction)?;
        dict.set_item("category", category.clone())?;
        dict.set_item("message", message.clone())?;
        Ok(Some(dict.into_any().unbind()))
    } else {
        Ok(None)
    }
}

fn py_protobuf_filter_node(
    topology: &TopologyGroup,
    events: &Node<PyProtobufEvent>,
    name: String,
    pending_fatal: &PendingFatal,
    filter: impl Fn(Python<'_>, &PyProtobufEvent) -> PyResult<Option<Py<PyAny>>> + 'static,
) -> PyNode {
    let pending = pending_fatal.clone();
    let node = topology.node_opts::<PyValue, _>(
        vec![events.erased()],
        move |ctx| {
            for event in ctx.batch::<PyProtobufEvent>(0) {
                Python::attach(|py| match filter(py, event.as_ref()) {
                    Ok(Some(value)) => ctx.emit(PyValue::new(value)),
                    Ok(None) => {}
                    Err(error) => {
                        store_pending_fatal(&pending, error);
                        graphrefly_rs::host_boundary::abort_host_boundary();
                    }
                });
            }
        },
        graph_node_opts(Some(name)),
    );
    py_node(node, pending_fatal)
}

fn py_ack_timeout(py: Python<'_>, event: &PyAckDriverEvent) -> PyResult<Option<Py<PyAny>>> {
    if let PyAckDriverEvent::Timeout {
        seq,
        attempt,
        observed_at_ms,
        ..
    } = event
    {
        let dict = PyDict::new(py);
        dict.set_item("seq", *seq)?;
        dict.set_item("attempt", *attempt)?;
        dict.set_item("observed_at_ms", *observed_at_ms)?;
        Ok(Some(dict.into_any().unbind()))
    } else {
        Ok(None)
    }
}

fn py_ack_issue(py: Python<'_>, event: &PyAckDriverEvent) -> PyResult<Option<Py<PyAny>>> {
    if let PyAckDriverEvent::Issue { code, message, .. } = event {
        let dict = PyDict::new(py);
        dict.set_item("code", *code)?;
        dict.set_item("message", *message)?;
        Ok(Some(dict.into_any().unbind()))
    } else {
        Ok(None)
    }
}

fn py_ack_status(py: Python<'_>, event: &PyAckDriverEvent, timeout_ms: u64) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("timeout_ms", timeout_ms)?;
    match event {
        PyAckDriverEvent::Timeout {
            seq,
            attempt,
            observed_at_ms,
            pending,
            now_ms,
        } => {
            dict.set_item("state", "ready")?;
            dict.set_item("pending", *pending)?;
            dict.set_item("now_ms", *now_ms)?;
            let timeout = PyDict::new(py);
            timeout.set_item("seq", *seq)?;
            timeout.set_item("attempt", *attempt)?;
            timeout.set_item("observed_at_ms", *observed_at_ms)?;
            dict.set_item("last_timeout", timeout)?;
        }
        PyAckDriverEvent::Issue {
            pending, now_ms, ..
        } => {
            dict.set_item("state", "issues")?;
            dict.set_item("pending", *pending)?;
            dict.set_item("now_ms", *now_ms)?;
            dict.set_item("last_timeout", py.None())?;
        }
        PyAckDriverEvent::State { pending, now_ms } => {
            dict.set_item("state", "ready")?;
            dict.set_item("pending", *pending)?;
            dict.set_item("now_ms", *now_ms)?;
            dict.set_item("last_timeout", py.None())?;
        }
    }
    Ok(dict.into_any().unbind())
}

fn py_ack_filter_node(
    topology: &TopologyGroup,
    events: &Node<PyAckDriverEvent>,
    name: String,
    pending_fatal: &PendingFatal,
    filter: impl Fn(Python<'_>, &PyAckDriverEvent) -> PyResult<Option<Py<PyAny>>> + 'static,
) -> PyNode {
    let pending = pending_fatal.clone();
    let node = topology.node_opts::<PyValue, _>(
        vec![events.erased()],
        move |ctx| {
            for event in ctx.batch::<PyAckDriverEvent>(0) {
                Python::attach(|py| match filter(py, event.as_ref()) {
                    Ok(Some(value)) => ctx.emit(PyValue::new(value)),
                    Ok(None) => {}
                    Err(error) => {
                        store_pending_fatal(&pending, error);
                        graphrefly_rs::host_boundary::abort_host_boundary();
                    }
                });
            }
        },
        graph_node_opts(Some(name)),
    );
    py_node(node, pending_fatal)
}

fn py_ack_status_node(
    topology: &TopologyGroup,
    events: &Node<PyAckDriverEvent>,
    name: String,
    pending_fatal: &PendingFatal,
    timeout_ms: u64,
) -> PyNode {
    let pending = pending_fatal.clone();
    let node = topology.node_opts::<PyValue, _>(
        vec![events.erased()],
        move |ctx| {
            for event in ctx.batch::<PyAckDriverEvent>(0) {
                Python::attach(|py| match py_ack_status(py, event.as_ref(), timeout_ms) {
                    Ok(value) => ctx.emit(PyValue::new(value)),
                    Err(error) => {
                        store_pending_fatal(&pending, error);
                        graphrefly_rs::host_boundary::abort_host_boundary();
                    }
                });
            }
        },
        graph_node_opts(Some(name)),
    );
    py_node(node, pending_fatal)
}

fn bridge_to_protobuf_envelope(
    envelope: WireBridgeEnvelope<WireBridgeProtobufDataBody>,
) -> WireBridgeProtobufEnvelope {
    let payload = match envelope.payload {
        Some(WireBridgePayload::Data(body)) => WireBridgeProtobufPayload::Data(body),
        Some(WireBridgePayload::Error(error)) => WireBridgeProtobufPayload::Error {
            error: error.into_bytes(),
        },
        Some(WireBridgePayload::Status(status)) => WireBridgeProtobufPayload::Status {
            status: status.into_bytes(),
        },
        Some(WireBridgePayload::Close { reason }) => WireBridgeProtobufPayload::Close {
            reason: reason.map(String::into_bytes),
        },
        None => match envelope.envelope_type {
            WireBridgeEnvelopeType::Ack => WireBridgeProtobufPayload::Ack,
            WireBridgeEnvelopeType::Nack => WireBridgeProtobufPayload::Nack { error: None },
            WireBridgeEnvelopeType::Close => WireBridgeProtobufPayload::Close { reason: None },
            WireBridgeEnvelopeType::Start
            | WireBridgeEnvelopeType::Data
            | WireBridgeEnvelopeType::Status
            | WireBridgeEnvelopeType::Error => WireBridgeProtobufPayload::Start,
        },
    };
    WireBridgeProtobufEnvelope {
        session_id: envelope.session_id,
        metadata: envelope.metadata,
        payload,
    }
}

fn protobuf_to_bridge_envelope(
    envelope: WireBridgeProtobufEnvelope,
) -> WireBridgeEnvelope<WireBridgeProtobufDataBody> {
    let (envelope_type, payload) = match envelope.payload {
        WireBridgeProtobufPayload::Start => (WireBridgeEnvelopeType::Start, None),
        WireBridgeProtobufPayload::Data(body) => (
            WireBridgeEnvelopeType::Data,
            Some(WireBridgePayload::Data(body)),
        ),
        WireBridgeProtobufPayload::Ack => (WireBridgeEnvelopeType::Ack, None),
        WireBridgeProtobufPayload::Nack { error } => (
            WireBridgeEnvelopeType::Nack,
            Some(WireBridgePayload::Error(
                error.map_or_else(|| "remote nack".to_owned(), bytes_to_string),
            )),
        ),
        WireBridgeProtobufPayload::Status { status } => (
            WireBridgeEnvelopeType::Status,
            Some(WireBridgePayload::Status(bytes_to_string(status))),
        ),
        WireBridgeProtobufPayload::Error { error } => (
            WireBridgeEnvelopeType::Error,
            Some(WireBridgePayload::Error(bytes_to_string(error))),
        ),
        WireBridgeProtobufPayload::Close { reason } => (
            WireBridgeEnvelopeType::Close,
            Some(WireBridgePayload::Close {
                reason: reason.map(bytes_to_string),
            }),
        ),
    };
    WireBridgeEnvelope {
        session_id: envelope.session_id,
        envelope_type,
        payload,
        metadata: envelope.metadata,
    }
}

fn bytes_to_string(bytes: Vec<u8>) -> String {
    String::from_utf8(bytes)
        .unwrap_or_else(|error| String::from_utf8_lossy(error.as_bytes()).into_owned())
}

fn py_bytes_like_to_vec(value: &Bound<'_, PyAny>) -> Option<Vec<u8>> {
    if let Ok(bytes) = value.cast::<PyBytes>() {
        return Some(bytes.as_bytes().to_vec());
    }
    if let Ok(bytes) = value.cast::<PyByteArray>() {
        // SAFETY: the bytearray slice is used only long enough to copy into an
        // owned Vec; no Python code or PyO3 API is called while the slice lives.
        return Some(unsafe { bytes.as_bytes() }.to_vec());
    }
    None
}

fn py_to_checkpoint_json(py: Python<'_>, value: &Py<PyAny>) -> PyResult<GraphCheckpointJson> {
    let mut seen = HashSet::new();
    py_bound_to_checkpoint_json(value.bind(py), &mut seen, 0)
}

fn py_bound_to_checkpoint_json(
    value: &Bound<'_, PyAny>,
    seen: &mut HashSet<usize>,
    depth: usize,
) -> PyResult<GraphCheckpointJson> {
    if depth > 128 {
        return Err(PyValueError::new_err(
            "checkpoint value exceeds strict JSON nesting depth",
        ));
    }
    if value.is_none() {
        return Ok(GraphCheckpointJson::Null);
    }
    if value.is_exact_instance_of::<PyBool>() {
        let value = value.extract::<bool>()?;
        return Ok(GraphCheckpointJson::Bool(value));
    }
    if value.is_exact_instance_of::<PyInt>() {
        if let Ok(value) = value.extract::<i64>() {
            return Ok(GraphCheckpointJson::Number(JsonNumber::from(value)));
        }
        if let Ok(value) = value.extract::<u64>() {
            return Ok(GraphCheckpointJson::Number(JsonNumber::from(value)));
        }
        return Err(PyValueError::new_err(
            "checkpoint integer is outside strict JSON range",
        ));
    }
    if value.is_exact_instance_of::<PyFloat>() {
        let value = value.extract::<f64>()?;
        return JsonNumber::from_f64(value).map_or_else(
            || {
                Err(PyValueError::new_err(
                    "checkpoint value must be finite strict JSON",
                ))
            },
            |number| Ok(GraphCheckpointJson::Number(number)),
        );
    }
    if value.is_exact_instance_of::<PyString>() {
        let value = value.extract::<String>()?;
        return Ok(GraphCheckpointJson::String(value));
    }
    if let Ok(sequence) = value.cast_exact::<PyList>() {
        let ptr = sequence.as_ptr() as usize;
        if !seen.insert(ptr) {
            return Err(PyValueError::new_err(
                "checkpoint value contains a cyclic JSON array",
            ));
        }
        let mut out = Vec::with_capacity(sequence.len());
        for item in sequence {
            out.push(py_bound_to_checkpoint_json(&item, seen, depth + 1)?);
        }
        seen.remove(&ptr);
        return Ok(GraphCheckpointJson::Array(out));
    }
    if let Ok(dict) = value.cast_exact::<PyDict>() {
        let ptr = dict.as_ptr() as usize;
        if !seen.insert(ptr) {
            return Err(PyValueError::new_err(
                "checkpoint value contains a cyclic JSON object",
            ));
        }
        let mut out = JsonMap::new();
        for (key, item) in dict {
            if !key.is_exact_instance_of::<PyString>() {
                return Err(PyValueError::new_err(
                    "checkpoint object keys must be strings",
                ));
            }
            let key = key
                .extract::<String>()
                .map_err(|_| PyValueError::new_err("checkpoint object keys must be strings"))?;
            if out
                .insert(key, py_bound_to_checkpoint_json(&item, seen, depth + 1)?)
                .is_some()
            {
                return Err(PyValueError::new_err("checkpoint object has duplicate key"));
            }
        }
        seen.remove(&ptr);
        return Ok(GraphCheckpointJson::Object(out));
    }
    Err(PyValueError::new_err(
        "checkpoint value is not strict JSON compatible",
    ))
}

fn checkpoint_json_to_py(py: Python<'_>, value: &GraphCheckpointJson) -> PyResult<Py<PyAny>> {
    match value {
        GraphCheckpointJson::Null => Ok(py.None()),
        GraphCheckpointJson::Bool(value) => value.into_py_any(py),
        GraphCheckpointJson::Number(value) => {
            if let Some(value) = value.as_i64() {
                value.into_py_any(py)
            } else if let Some(value) = value.as_u64() {
                value.into_py_any(py)
            } else if let Some(value) = value.as_f64() {
                value.into_py_any(py)
            } else {
                Err(PyValueError::new_err(
                    "checkpoint number is not representable",
                ))
            }
        }
        GraphCheckpointJson::String(value) => value.clone().into_py_any(py),
        GraphCheckpointJson::Array(items) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(checkpoint_json_to_py(py, item)?)?;
            }
            Ok(list.into())
        }
        GraphCheckpointJson::Object(map) => {
            let dict = PyDict::new(py);
            for (key, value) in map {
                dict.set_item(key, checkpoint_json_to_py(py, value)?)?;
            }
            Ok(dict.into())
        }
    }
}

fn py_checkpoint_to_native(py: Python<'_>, checkpoint: &Py<PyAny>) -> PyResult<GraphCheckpoint> {
    let value = py_to_checkpoint_json(py, checkpoint)?;
    serde_json::from_value(value).map_err(|err| PyValueError::new_err(err.to_string()))
}

fn native_checkpoint_to_py(py: Python<'_>, checkpoint: &GraphCheckpoint) -> PyResult<Py<PyAny>> {
    let value =
        serde_json::to_value(checkpoint).map_err(|err| PyValueError::new_err(err.to_string()))?;
    checkpoint_json_to_py(py, &value)
}

fn dep_args_from_ctx(py: Python<'_>, ctx: &Ctx) -> PyResult<Vec<Py<PyAny>>> {
    let mut values = Vec::with_capacity(ctx.dep_len());
    for index in 0..ctx.dep_len() {
        let Some(value) = ctx_data_value(py, ctx, index)? else {
            return Err(PyRuntimeError::new_err(
                "dependency DATA is absent at the Python callback boundary",
            ));
        };
        values.push(value);
    }
    Ok(values)
}

fn status_name(status: Status) -> &'static str {
    match status {
        Status::Sentinel => "sentinel",
        Status::Pending => "pending",
        Status::Dirty => "dirty",
        Status::Settled => "settled",
        Status::Resolved => "resolved",
        Status::Completed => "completed",
        Status::Errored => "errored",
    }
}

fn graph_node_opts(name: Option<String>) -> GraphNodeOpts {
    match name {
        Some(name) => GraphNodeOpts::named(name),
        None => GraphNodeOpts::default(),
    }
}

fn graph_node_opts_with_node(
    name: Option<String>,
    partial: bool,
    complete_when_deps_complete: bool,
    error_when_deps_error: bool,
    terminal_as_real_input: bool,
) -> GraphNodeOpts {
    let mut opts = graph_node_opts(name);
    opts.node = NodeOpts {
        partial,
        complete_when_deps_complete,
        error_when_deps_error,
        terminal_as_real_input,
        ..NodeOpts::default()
    };
    opts
}

fn graph_node_opts_with_conformance(
    name: Option<String>,
    partial: bool,
    complete_when_deps_complete: bool,
    error_when_deps_error: bool,
    terminal_as_real_input: bool,
    pausable: Option<String>,
    pull_id: Option<String>,
) -> PyResult<GraphNodeOpts> {
    let mut opts = graph_node_opts_with_node(
        name,
        partial,
        complete_when_deps_complete,
        error_when_deps_error,
        terminal_as_real_input,
    );
    opts.node.pausable = match pausable.as_deref() {
        None | Some("true") => Pausable::True,
        Some("resumeAll") => Pausable::ResumeAll,
        Some("false") => Pausable::False,
        Some(_) => {
            return Err(PyValueError::new_err(
                "pausable must be true, 'resumeAll', or false",
            ));
        }
    };
    opts.node.pull_id = pull_id.map(LockId::new);
    Ok(opts)
}

fn apply_restore_opts(
    py: Python<'_>,
    opts: &mut GraphNodeOpts,
    restore_ref: Option<String>,
    restore_config: Option<Py<PyAny>>,
    restore_config_version: Option<Py<PyAny>>,
) -> PyResult<()> {
    let Some(restore_ref) = restore_ref else {
        if restore_config.is_some() || restore_config_version.is_some() {
            return Err(PyValueError::new_err(
                "restore config requires a restore ref",
            ));
        }
        return Ok(());
    };
    let mut restore = RestoreFactoryMeta::registry_ref(restore_ref);
    if let Some(config) = restore_config {
        restore = restore.with_config(py_to_checkpoint_json(py, &config)?);
    }
    if let Some(config_version) = restore_config_version {
        restore = restore.with_config_version(py_to_checkpoint_json(py, &config_version)?);
    }
    opts.restore = Some(restore);
    Ok(())
}

fn py_wave_data(py: Python<'_>, ctx: &DeferredCtx, sentinel: &Py<PyAny>) -> PyResult<Py<PyAny>> {
    let outer = PyList::empty(py);
    for dep_waves in ctx.wave_data() {
        let py_dep_waves = PyList::empty(py);
        for wave in dep_waves {
            let py_wave = PyList::empty(py);
            for item in wave {
                match item {
                    WaveData::Data(value) => py_wave.append(value_from_any(py, value)?)?,
                    WaveData::Sentinel => py_wave.append(sentinel.clone_ref(py))?,
                }
            }
            py_dep_waves.append(py_wave)?;
        }
        outer.append(py_dep_waves)?;
    }
    Ok(outer.into())
}

fn py_terminal(py: Python<'_>, terminal: Option<&DepTerminal>) -> PyResult<Py<PyAny>> {
    match terminal {
        None => false.into_py_any(py),
        Some(DepTerminal::Complete) => true.into_py_any(py),
        Some(DepTerminal::Error(error)) => error.to_string().into_py_any(py),
    }
}

#[pyclass(name = "Ctx", unsendable)]
struct PyCtx {
    snapshot: DeferredCtx,
    pull: Option<PullDemand>,
    initial_state: Option<Py<PyAny>>,
    ops: RefCell<Vec<PyCtxOp>>,
    active: Rc<Cell<bool>>,
}

#[pyclass(name = "ConformanceAsyncHandle", unsendable)]
struct PyConformanceAsyncHandle {
    pending: Rc<RefCell<Option<DeferredCtx>>>,
    pending_fatal: PendingFatal,
}

enum PyAsyncCtxOp {
    OnDeactivation(Py<PyAny>),
}

#[pyclass(name = "AsyncCtx", unsendable)]
struct PyAsyncCtx {
    deferred: DeferredCtx,
    ops: RefCell<Vec<PyAsyncCtxOp>>,
    active: Rc<Cell<bool>>,
    pending_fatal: PendingFatal,
}

#[pymethods]
impl PyAsyncCtx {
    fn emit(&self, value: Py<PyAny>) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        self.assert_live()?;
        catch_graph_panic(&self.pending_fatal, || {
            self.deferred.emit(PyValue::new(value));
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn complete(&self) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        self.assert_live()?;
        catch_graph_panic(&self.pending_fatal, || {
            self.deferred.down(vec![Message::Complete]);
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn resolve(&self, _py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        self.assert_live()?;
        catch_graph_panic(&self.pending_fatal, || {
            self.deferred.down(vec![
                Message::Data(Rc::new(PyValue::new(value))),
                Message::Complete,
            ]);
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn error(&self, message: String) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        self.assert_live()?;
        catch_graph_panic(&self.pending_fatal, || {
            self.deferred.down(vec![Message::Error(message.into())]);
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn is_live(&self) -> PyResult<bool> {
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(self.active.get())
    }

    fn on_deactivation(&self, callback: Py<PyAny>) -> PyResult<()> {
        self.assert_configuring()?;
        self.ops
            .borrow_mut()
            .push(PyAsyncCtxOp::OnDeactivation(callback));
        Ok(())
    }
}

impl PyAsyncCtx {
    fn assert_configuring(&self) -> PyResult<()> {
        if self.active.get() {
            Ok(())
        } else {
            Err(PyRuntimeError::new_err(
                "async ctx setup is only valid during activation",
            ))
        }
    }

    fn assert_live(&self) -> PyResult<()> {
        if self.active.get() {
            Ok(())
        } else {
            Err(PyRuntimeError::new_err(
                "async ctx is no longer live for this activation",
            ))
        }
    }
}

fn commit_py_async_ctx(py: Python<'_>, py_ctx: &Py<PyAsyncCtx>, ctx: &Ctx) {
    let py_ctx = py_ctx.borrow(py);
    for op in py_ctx.ops.borrow_mut().drain(..) {
        match op {
            PyAsyncCtxOp::OnDeactivation(callback) => {
                let pending_fatal = py_ctx.pending_fatal.clone();
                let active = py_ctx.active.clone();
                let hook_ctx = ctx.defer();
                ctx.on_deactivation(move || {
                    active.set(false);
                    call_py_hook_callback(&callback, &pending_fatal, &hook_ctx);
                });
            }
        }
    }
}

#[pymethods]
impl PyConformanceAsyncHandle {
    fn has_pending(&self) -> PyResult<bool> {
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(self.pending.borrow().is_some())
    }

    fn resolve(&self, _py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        let deferred = self.pending.borrow_mut().take().ok_or_else(|| {
            PyRuntimeError::new_err("no pending conformance async ctx to resolve")
        })?;
        catch_graph_panic(&self.pending_fatal, || {
            deferred.emit(PyValue::new(value));
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn invalidate_live_deps(&self) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        let deferred = self.pending.borrow_mut().take().ok_or_else(|| {
            PyRuntimeError::new_err("no pending conformance async ctx to invalidate")
        })?;
        catch_graph_panic(&self.pending_fatal, || {
            deferred.up(vec![Message::Invalidate]);
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }
}

enum PyCtxOp {
    Emit(Py<PyAny>),
    StateSet(Py<PyAny>),
    StatePersist(bool),
    OnInvalidate(Py<PyAny>),
    OnDeactivation(Py<PyAny>),
    RewireNextSubscribeDep(Node<PyValue>, Py<PyAny>),
    RewireNextUnsubscribeDep(Node<PyValue>, Py<PyAny>),
    RewireNextReplaceDeps(Vec<Node<PyValue>>, Py<PyAny>),
    UpNextPull(String, Option<Py<PyAny>>, Option<usize>),
    UpPull(String, Option<Py<PyAny>>, Option<usize>),
    ConformanceDownComplete,
    ConformanceUpData(Py<PyAny>),
}

#[pymethods]
impl PyCtx {
    fn dep_len(&self) -> PyResult<usize> {
        self.assert_active()?;
        Ok(self.snapshot.wave_data().len())
    }

    fn data_entry(&self, py: Python<'_>, index: usize) -> PyResult<(bool, Py<PyAny>)> {
        self.assert_active()?;
        if index >= self.snapshot.wave_data().len() {
            return Err(PyIndexError::new_err("dependency index out of range"));
        }
        match deferred_data_value(py, &self.snapshot, index)? {
            Some(value) => Ok((true, value)),
            None => Ok((false, py.None())),
        }
    }

    fn wave_data(&self, py: Python<'_>, sentinel: Py<PyAny>) -> PyResult<Py<PyAny>> {
        self.assert_active()?;
        py_wave_data(py, &self.snapshot, &sentinel)
    }

    fn terminal(&self, py: Python<'_>, index: usize) -> PyResult<Py<PyAny>> {
        self.assert_active()?;
        if index >= self.snapshot.wave_data().len() {
            return Err(PyIndexError::new_err("dependency index out of range"));
        }
        py_terminal(py, self.snapshot.terminal(index))
    }

    fn _pull_context(&self, py: Python<'_>) -> PyResult<Option<(String, Option<Py<PyAny>>)>> {
        self.assert_active()?;
        Ok(self.pull.as_ref().map(|pull| {
            (
                pull.pull_id.0.clone(),
                pull.params::<PyValue>().map(|value| value.clone_object(py)),
            )
        }))
    }

    fn emit(&self, value: Py<PyAny>) -> PyResult<()> {
        self.assert_active()?;
        self.ops.borrow_mut().push(PyCtxOp::Emit(value));
        Ok(())
    }

    fn state_entry(&self, py: Python<'_>) -> PyResult<(bool, Py<PyAny>)> {
        self.assert_active()?;
        for op in self.ops.borrow().iter().rev() {
            if let PyCtxOp::StateSet(value) = op {
                return Ok((true, value.clone_ref(py)));
            }
        }
        match self.initial_state.as_ref() {
            Some(value) => Ok((true, value.clone_ref(py))),
            None => Ok((false, py.None())),
        }
    }

    fn set_state(&self, value: Py<PyAny>) -> PyResult<()> {
        self.assert_active()?;
        self.ops.borrow_mut().push(PyCtxOp::StateSet(value));
        Ok(())
    }

    #[pyo3(signature = (on = true))]
    fn state_persist(&self, on: bool) -> PyResult<()> {
        self.assert_active()?;
        self.ops.borrow_mut().push(PyCtxOp::StatePersist(on));
        Ok(())
    }

    fn on_invalidate(&self, callback: Py<PyAny>) -> PyResult<()> {
        self.assert_active()?;
        self.ops.borrow_mut().push(PyCtxOp::OnInvalidate(callback));
        Ok(())
    }

    fn on_deactivation(&self, callback: Py<PyAny>) -> PyResult<()> {
        self.assert_active()?;
        self.ops
            .borrow_mut()
            .push(PyCtxOp::OnDeactivation(callback));
        Ok(())
    }

    fn _rewire_next_subscribe_dep(
        &self,
        py: Python<'_>,
        dep: Py<PyNode>,
        callback: Py<PyAny>,
    ) -> PyResult<()> {
        self.assert_active()?;
        let dep = dep.borrow(py).node.clone();
        self.ops
            .borrow_mut()
            .push(PyCtxOp::RewireNextSubscribeDep(dep, callback));
        Ok(())
    }

    fn _rewire_next_unsubscribe_dep(
        &self,
        py: Python<'_>,
        dep: Py<PyNode>,
        callback: Py<PyAny>,
    ) -> PyResult<()> {
        self.assert_active()?;
        let dep = dep.borrow(py).node.clone();
        self.ops
            .borrow_mut()
            .push(PyCtxOp::RewireNextUnsubscribeDep(dep, callback));
        Ok(())
    }

    fn _rewire_next_replace_deps(
        &self,
        py: Python<'_>,
        deps: Vec<Py<PyNode>>,
        callback: Py<PyAny>,
    ) -> PyResult<()> {
        self.assert_active()?;
        let deps = deps
            .iter()
            .map(|dep| dep.borrow(py).node.clone())
            .collect::<Vec<_>>();
        self.ops
            .borrow_mut()
            .push(PyCtxOp::RewireNextReplaceDeps(deps, callback));
        Ok(())
    }

    #[pyo3(signature = (pull_id, params = None, toward_dep = None))]
    fn _up_next_pull(
        &self,
        pull_id: String,
        params: Option<Py<PyAny>>,
        toward_dep: Option<usize>,
    ) -> PyResult<()> {
        self.assert_active()?;
        self.ops
            .borrow_mut()
            .push(PyCtxOp::UpNextPull(pull_id, params, toward_dep));
        Ok(())
    }

    #[pyo3(signature = (pull_id, params = None, toward_dep = None))]
    fn _up_pull(
        &self,
        pull_id: String,
        params: Option<Py<PyAny>>,
        toward_dep: Option<usize>,
    ) -> PyResult<()> {
        self.assert_active()?;
        self.ops
            .borrow_mut()
            .push(PyCtxOp::UpPull(pull_id, params, toward_dep));
        Ok(())
    }

    fn _conformance_up_data(&self, value: Py<PyAny>) -> PyResult<()> {
        self.assert_active()?;
        self.ops
            .borrow_mut()
            .push(PyCtxOp::ConformanceUpData(value));
        Ok(())
    }

    fn _conformance_down_complete(&self) -> PyResult<()> {
        self.assert_active()?;
        self.ops.borrow_mut().push(PyCtxOp::ConformanceDownComplete);
        Ok(())
    }
}

impl PyCtx {
    fn assert_active(&self) -> PyResult<()> {
        if self.active.get() {
            Ok(())
        } else {
            Err(PyRuntimeError::new_err(
                "ctx is only valid during its Python node callback",
            ))
        }
    }
}

fn commit_py_ctx(py: Python<'_>, py_ctx: &Py<PyCtx>, ctx: &Ctx, pending_fatal: &PendingFatal) {
    let py_ctx = py_ctx.borrow(py);
    for op in py_ctx.ops.borrow_mut().drain(..) {
        match op {
            PyCtxOp::Emit(value) => ctx.emit(PyValue::new(value)),
            PyCtxOp::StateSet(value) => ctx.state_set(PyValue::new(value)),
            PyCtxOp::StatePersist(on) => ctx.state_persist(on),
            PyCtxOp::OnInvalidate(callback) => {
                let pending_fatal = pending_fatal.clone();
                let hook_ctx = ctx.defer();
                ctx.on_invalidate(move || {
                    call_py_hook_callback(&callback, &pending_fatal, &hook_ctx);
                });
            }
            PyCtxOp::OnDeactivation(callback) => {
                let pending_fatal = pending_fatal.clone();
                let hook_ctx = ctx.defer();
                ctx.on_deactivation(move || {
                    call_py_hook_callback(&callback, &pending_fatal, &hook_ctx);
                });
            }
            PyCtxOp::RewireNextSubscribeDep(dep, callback) => {
                let pending_fatal = pending_fatal.clone();
                ctx.rewire_next_subscribe_dep(dep.erased(), move |ctx| {
                    invoke_py_ctx_callback(ctx, &callback, &pending_fatal);
                });
            }
            PyCtxOp::RewireNextUnsubscribeDep(dep, callback) => {
                let pending_fatal = pending_fatal.clone();
                ctx.rewire_next_unsubscribe_dep(dep.erased(), move |ctx| {
                    invoke_py_ctx_callback(ctx, &callback, &pending_fatal);
                });
            }
            PyCtxOp::RewireNextReplaceDeps(deps, callback) => {
                let pending_fatal = pending_fatal.clone();
                let deps = deps.into_iter().map(|dep| dep.erased()).collect::<Vec<_>>();
                ctx.rewire_next_replace_deps(deps, move |ctx| {
                    invoke_py_ctx_callback(ctx, &callback, &pending_fatal);
                });
            }
            PyCtxOp::UpNextPull(pull_id, params, toward_dep) => {
                let wave = vec![private_pull_demand(pull_id, params)];
                match toward_dep {
                    Some(toward_dep) => ctx.up_next_toward(toward_dep, wave),
                    None => ctx.up_next(wave),
                }
            }
            PyCtxOp::UpPull(pull_id, params, toward_dep) => {
                let wave = vec![private_pull_demand(pull_id, params)];
                match toward_dep {
                    Some(toward_dep) => ctx.up_toward(toward_dep, wave),
                    None => ctx.up(wave),
                }
            }
            PyCtxOp::ConformanceDownComplete => {
                ctx.down(vec![Message::Complete]);
            }
            PyCtxOp::ConformanceUpData(value) => {
                ctx.up(vec![Message::Data(Rc::new(PyValue::new(value)))]);
            }
        }
    }
}

fn invoke_py_ctx_callback(ctx: &Ctx, callback: &Py<PyAny>, pending_fatal: &PendingFatal) {
    let active = Rc::new(Cell::new(true));
    let result = Python::attach(|py| {
        let initial_state = ctx_state_value(py, ctx)?;
        let py_ctx = Py::new(
            py,
            PyCtx {
                snapshot: ctx.defer(),
                pull: ctx.pull().cloned(),
                initial_state,
                ops: RefCell::new(Vec::new()),
                active: active.clone(),
            },
        )?;
        let callback_result = callback.call1(py, (py_ctx.clone_ref(py),));
        match callback_result {
            Ok(value) => {
                active.set(false);
                commit_py_ctx(py, &py_ctx, ctx, pending_fatal);
                Ok(value)
            }
            Err(error) => {
                active.set(false);
                Err(error)
            }
        }
    });
    handle_callback_void_result(ctx, pending_fatal, result);
}

fn private_pull_demand(pull_id: String, params: Option<Py<PyAny>>) -> Message<AnyValue> {
    match params {
        Some(params) => Message::Pull(PullDemand::with_params(
            LockId::new(pull_id),
            PyValue::new(params),
        )),
        None => Message::Pull(PullDemand::new(LockId::new(pull_id))),
    }
}

fn call_py_hook_callback(
    callback: &Py<PyAny>,
    pending_fatal: &PendingFatal,
    hook_ctx: &DeferredCtx,
) {
    Python::attach(|py| {
        if let Err(error) = callback.call0(py) {
            if py_error_is_fatal(py, &error) {
                store_pending_fatal(pending_fatal, error);
                graphrefly_rs::host_boundary::abort_host_boundary();
            } else {
                hook_ctx.down(vec![Message::Error(py_exception_to_error(error))]);
            }
        }
    });
}

fn describe_value(py: Python<'_>, value: &DescribeValue) -> PyResult<Py<PyAny>> {
    match value {
        DescribeValue::Bool(value) => (*value).into_py_any(py),
        DescribeValue::I64(value) => (*value).into_py_any(py),
        DescribeValue::U64(value) => (*value).into_py_any(py),
        DescribeValue::F64(value) => (*value).into_py_any(py),
        DescribeValue::String(value) => value.clone().into_py_any(py),
        DescribeValue::Opaque => Ok(py_string(py, "<opaque>")),
    }
}

fn describe_snapshot(py: Python<'_>, snapshot: DescribeSnapshot) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("name", snapshot.name)?;

    let nodes = PyList::empty(py);
    for node in snapshot.nodes {
        let entry = PyDict::new(py);
        entry.set_item("id", node.id)?;
        entry.set_item("name", node.name)?;
        entry.set_item("factory", node.factory)?;
        entry.set_item("status", status_name(node.status))?;
        entry.set_item("has_value", node.value.is_some())?;
        entry.set_item(
            "value",
            match node.value.as_ref() {
                Some(value) => describe_value(py, value)?,
                None => py.None(),
            },
        )?;
        entry.set_item("deps", node.deps)?;
        entry.set_item("meta", node.meta)?;
        nodes.append(entry)?;
    }
    dict.set_item("nodes", nodes)?;

    let edges = PyList::empty(py);
    for edge in snapshot.edges {
        let entry = PyDict::new(py);
        entry.set_item("from", edge.from)?;
        entry.set_item("to", edge.to)?;
        edges.append(entry)?;
    }
    dict.set_item("edges", edges)?;

    if let Some(subgraphs) = snapshot.subgraphs {
        let py_subgraphs = PyList::empty(py);
        for subgraph in subgraphs {
            py_subgraphs.append(describe_snapshot(py, subgraph)?)?;
        }
        dict.set_item("subgraphs", py_subgraphs)?;
    } else {
        dict.set_item("subgraphs", py.None())?;
    }

    Ok(dict.into())
}

enum PyRestoreRecordedKind {
    State,
    Node {
        callback: Py<PyAny>,
        factory: String,
    },
}

struct PyRestoreRecorded {
    kind: PyRestoreRecordedKind,
}

#[pyclass(name = "RestoreContext", unsendable)]
struct PyRestoreContext {
    id: String,
    name: Option<String>,
    deps: Vec<String>,
    config: Py<PyAny>,
    config_version: Py<PyAny>,
    checkpoint: Py<PyAny>,
    active: Rc<Cell<bool>>,
    recorded: Rc<RefCell<Option<PyRestoreRecorded>>>,
}

#[pymethods]
impl PyRestoreContext {
    #[getter]
    fn id(&self) -> &str {
        &self.id
    }

    #[getter]
    fn name(&self) -> Option<String> {
        self.name.clone()
    }

    #[getter]
    fn deps(&self) -> Vec<String> {
        self.deps.clone()
    }

    #[getter]
    fn config(&self, py: Python<'_>) -> Py<PyAny> {
        self.config.clone_ref(py)
    }

    #[getter]
    fn config_version(&self, py: Python<'_>) -> Py<PyAny> {
        self.config_version.clone_ref(py)
    }

    #[getter]
    fn checkpoint(&self, py: Python<'_>) -> Py<PyAny> {
        self.checkpoint.clone_ref(py)
    }

    fn register_state(&self) -> PyResult<()> {
        self.record(PyRestoreRecorded {
            kind: PyRestoreRecordedKind::State,
        })
    }

    #[pyo3(signature = (callback, factory = None))]
    fn register_node(&self, callback: Py<PyAny>, factory: Option<String>) -> PyResult<()> {
        self.record(PyRestoreRecorded {
            kind: PyRestoreRecordedKind::Node {
                callback,
                factory: factory.unwrap_or_else(|| "node".to_owned()),
            },
        })
    }
}

impl PyRestoreContext {
    fn record(&self, recorded: PyRestoreRecorded) -> PyResult<()> {
        if !self.active.get() {
            return Err(PyRuntimeError::new_err(
                "restore descriptor context is only valid during create()",
            ));
        }
        let mut slot = self.recorded.borrow_mut();
        if slot.is_some() {
            return Err(PyRuntimeError::new_err(
                "restore descriptor registered more than one node",
            ));
        }
        *slot = Some(recorded);
        Ok(())
    }
}

struct PyRestoreDescriptorBridge {
    ref_: String,
    descriptor: Py<PyAny>,
    pending_fatal: PendingFatal,
}

impl GraphRestoreDescriptor for PyRestoreDescriptorBridge {
    fn ref_(&self) -> &str {
        self.ref_.as_str()
    }

    fn define(&self, ctx: RestoreDefineCtx<'_>) -> GraphRestoreResult<RestoreNodeDefinition> {
        let recorded = Rc::new(RefCell::new(None));
        let active = Rc::new(Cell::new(true));
        Python::attach(|py| -> GraphRestoreResult<()> {
            let py_ctx = Py::new(
                py,
                PyRestoreContext {
                    id: ctx.id.to_owned(),
                    name: ctx.checkpoint.name.clone(),
                    deps: ctx.deps.to_vec(),
                    config: ctx
                        .config
                        .map_or_else(|| Ok(py.None()), |value| checkpoint_json_to_py(py, value))
                        .map_err(|err| GraphRestoreError::new(err.to_string()))?,
                    config_version: ctx
                        .config_version
                        .map_or_else(|| Ok(py.None()), |value| checkpoint_json_to_py(py, value))
                        .map_err(|err| GraphRestoreError::new(err.to_string()))?,
                    checkpoint: native_checkpoint_node_to_py(py, ctx.checkpoint)
                        .map_err(|err| GraphRestoreError::new(err.to_string()))?,
                    active: active.clone(),
                    recorded: recorded.clone(),
                },
            )
            .map_err(|err| GraphRestoreError::new(err.to_string()))?;
            let result = self.descriptor.call_method1(py, "create", (py_ctx,));
            active.set(false);
            match result {
                Ok(_) => Ok(()),
                Err(err) if py_error_is_fatal(py, &err) => {
                    store_pending_fatal(&self.pending_fatal, err);
                    Err(GraphRestoreError::new(
                        "restore_graph: fatal Python restore descriptor error",
                    ))
                }
                Err(err) => Err(GraphRestoreError::new(err.to_string())),
            }
        })?;
        let recorded = recorded.borrow_mut().take().ok_or_else(|| {
            GraphRestoreError::new(format!(
                "restore_graph: descriptor '{}' did not register a node for '{}'",
                self.ref_, ctx.id
            ))
        })?;
        let opts = restored_opts(ctx.checkpoint)?;
        match recorded.kind {
            PyRestoreRecordedKind::State => Ok(RestoreNodeDefinition {
                factory: "state".to_owned(),
                kind: RestoreNodeKind::StateJson,
                opts,
            }),
            PyRestoreRecordedKind::Node { callback, factory } => {
                let pending_fatal = self.pending_fatal.clone();
                Ok(RestoreNodeDefinition {
                    factory,
                    kind: RestoreNodeKind::NodeJson(Rc::new(move |ctx: &Ctx| {
                        invoke_py_ctx_callback(ctx, &callback, &pending_fatal);
                    })),
                    opts,
                })
            }
        }
    }
}

fn native_checkpoint_node_to_py(
    py: Python<'_>,
    node: &graphrefly_rs::GraphCheckpointNode,
) -> PyResult<Py<PyAny>> {
    let value = serde_json::to_value(node).map_err(|err| PyValueError::new_err(err.to_string()))?;
    checkpoint_json_to_py(py, &value)
}

#[pyclass(name = "RestoreRegistry", unsendable)]
struct PyRestoreRegistry {
    registry: GraphRestoreRegistry,
    pending_fatal: PendingFatal,
}

#[pyclass(name = "Graph", unsendable)]
struct PyGraph {
    graph: Graph,
    pending_fatal: PendingFatal,
}

impl PyGraph {
    fn restored(graph: Graph, pending_fatal: PendingFatal) -> Self {
        Self {
            graph,
            pending_fatal,
        }
    }
}

#[pymethods]
impl PyGraph {
    #[new]
    #[pyo3(signature = (name = None))]
    fn new(name: Option<String>) -> Self {
        let graph = match name {
            Some(name) => graphrefly_rs::graph_opts(graphrefly_rs::GraphOptions::named(name)),
            None => graphrefly_rs::graph(),
        };
        Self {
            graph,
            pending_fatal: Rc::new(RefCell::new(None)),
        }
    }

    fn checkpoint(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        raise_pending_fatal(&self.pending_fatal)?;
        let checkpoint = catch_graph_panic(&self.pending_fatal, || self.graph.checkpoint())?
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        native_checkpoint_to_py(py, &checkpoint)
    }

    #[pyo3(signature = (value, name = None))]
    fn state(&self, _py: Python<'_>, value: Py<PyAny>, name: Option<String>) -> PyResult<PyNode> {
        raise_pending_fatal(&self.pending_fatal)?;
        let value = PyValue::new(value);
        let node = catch_graph_panic(&self.pending_fatal, || PyNode {
            node: self.graph.state_opts(value, graph_node_opts(name)),
            graph_node: None,
            pending_fatal: self.pending_fatal.clone(),
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(node)
    }

    #[pyo3(signature = (name = None))]
    fn state_empty(&self, name: Option<String>) -> PyResult<PyNode> {
        raise_pending_fatal(&self.pending_fatal)?;
        let node = catch_graph_panic(&self.pending_fatal, || PyNode {
            node: self.graph.state_empty_opts(graph_node_opts(name)),
            graph_node: None,
            pending_fatal: self.pending_fatal.clone(),
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(node)
    }

    #[pyo3(signature = (session_id, name = None))]
    fn _wire_bridge(&self, session_id: String, name: Option<String>) -> PyResult<PyWireBridge> {
        raise_pending_fatal(&self.pending_fatal)?;
        let bridge = catch_graph_panic(&self.pending_fatal, || {
            Rc::new(wire_bridge::<
                WireBridgeProtobufDataBody,
                WireBridgeProtobufDataBody,
            >(
                &self.graph,
                WireBridgeOptions {
                    name: name.clone(),
                    session_id: session_id.clone(),
                    now_ms: Some(Rc::new(|| 1)),
                    retry: RetryPolicy::new(2, BackoffPolicy::None),
                },
            ))
        })?;
        let bridge_name = name.unwrap_or_else(|| "wireBridge".to_owned());
        let status = py_status_node(
            &self.graph,
            &bridge.status,
            format!("{bridge_name}/py/status"),
            &self.pending_fatal,
            py_bridge_status,
        );
        let issues = py_error_issue_node(
            &self.graph,
            &bridge.errors,
            format!("{bridge_name}/py/issues"),
            &self.pending_fatal,
            "bridge_error",
        );
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(PyWireBridge {
            bridge,
            status,
            issues,
            released: Cell::new(false),
        })
    }

    #[pyo3(signature = (bridge, name = None))]
    #[allow(clippy::too_many_lines)]
    fn _wire_bridge_protobuf(
        &self,
        bridge: PyRef<'_, PyWireBridge>,
        name: Option<String>,
    ) -> PyResult<PyWireBridgeProtobuf> {
        raise_pending_fatal(&self.pending_fatal)?;
        let helper_name = name.unwrap_or_else(|| "wireBridgeProtobuf".to_owned());
        let bridge_core = bridge.bridge.clone();
        let topology = self
            .graph
            .topology_group_opts(TopologyGroupOptions::named(format!(
                "{helper_name}.wireBridgeProtobuf"
            )));
        let inbound_bytes = topology.state_empty_opts::<PyValue>(graph_node_opts(Some(format!(
            "{helper_name}/inbound_bytes"
        ))));
        let decoded_inbound = topology
            .node_opts::<WireBridgeIngress<WireBridgeProtobufDataBody>, _>(
                vec![inbound_bytes.erased()],
                move |ctx| {
                    for value in ctx.batch::<PyValue>(0) {
                        Python::attach(|py| {
                            let object = value.object.bind(py);
                            let Some(bytes) = py_bytes_like_to_vec(object) else {
                                ctx.emit(WireBridgeIngress::<WireBridgeProtobufDataBody>::Invalid(
                                    "wire_bridge_protobuf inbound_bytes requires bytes".to_owned(),
                                ));
                                return;
                            };
                            let decoded = decode_wire_bridge_protobuf_bytes(&bytes);
                            if let Some(envelope) = decoded.envelope {
                                ctx.emit(
                                    WireBridgeIngress::<WireBridgeProtobufDataBody>::Envelope(
                                        protobuf_to_bridge_envelope(envelope),
                                    ),
                                );
                            } else {
                                let message = decoded
                                    .issues
                                    .into_iter()
                                    .map(|issue| {
                                        format!("{}: {}", issue.category.as_str(), issue.message)
                                    })
                                    .collect::<Vec<_>>()
                                    .join("; ");
                                ctx.emit(WireBridgeIngress::<WireBridgeProtobufDataBody>::Invalid(
                                    message,
                                ));
                            }
                        });
                    }
                },
                graph_node_opts(Some(format!("{helper_name}/decoded_inbound"))),
            );
        let events = topology.node_opts::<PyProtobufEvent, _>(
            vec![inbound_bytes.erased(), bridge_core.outbound.erased()],
            move |ctx| {
                for value in ctx.batch::<PyValue>(0) {
                    Python::attach(|py| {
                        let object = value.object.bind(py);
                        let Some(bytes) = py_bytes_like_to_vec(object) else {
                            ctx.emit(PyProtobufEvent::Issue {
                                direction: "inbound",
                                category: "malformed".to_owned(),
                                message: "wire_bridge_protobuf inbound_bytes requires bytes"
                                    .to_owned(),
                            });
                            ctx.emit(PyProtobufEvent::Status {
                                direction: "inbound",
                                state: "invalid",
                            });
                            return;
                        };
                        let decoded = decode_wire_bridge_protobuf_bytes(&bytes);
                        if decoded.envelope.is_some() {
                            ctx.emit(PyProtobufEvent::Status {
                                direction: "inbound",
                                state: "valid",
                            });
                        } else {
                            for issue in decoded.issues {
                                ctx.emit(PyProtobufEvent::Issue {
                                    direction: "inbound",
                                    category: issue.category.as_str().to_owned(),
                                    message: issue.message,
                                });
                            }
                            ctx.emit(PyProtobufEvent::Status {
                                direction: "inbound",
                                state: "invalid",
                            });
                        }
                    });
                }
                for envelope in ctx.batch::<WireBridgeEnvelope<WireBridgeProtobufDataBody>>(1) {
                    let encoded = encode_wire_bridge_protobuf_bytes(&bridge_to_protobuf_envelope(
                        envelope.as_ref().clone(),
                    ));
                    if encoded.bytes.is_some() {
                        ctx.emit(PyProtobufEvent::Status {
                            direction: "outbound",
                            state: "valid",
                        });
                    } else {
                        for issue in encoded.issues {
                            ctx.emit(PyProtobufEvent::Issue {
                                direction: "outbound",
                                category: issue.category.as_str().to_owned(),
                                message: issue.message,
                            });
                        }
                        ctx.emit(PyProtobufEvent::Status {
                            direction: "outbound",
                            state: "invalid",
                        });
                    }
                }
            },
            graph_node_opts_with_node(
                Some(format!("{helper_name}/events")),
                true,
                false,
                false,
                false,
            ),
        );
        let outbound_bytes = {
            let node = topology.node_opts::<PyValue, _>(
                vec![bridge_core.outbound.erased()],
                move |ctx| {
                    for envelope in ctx.batch::<WireBridgeEnvelope<WireBridgeProtobufDataBody>>(0) {
                        let encoded = encode_wire_bridge_protobuf_bytes(
                            &bridge_to_protobuf_envelope(envelope.as_ref().clone()),
                        );
                        if let Some(bytes) = encoded.bytes {
                            Python::attach(|py| {
                                ctx.emit(PyValue::new(
                                    PyBytes::new(py, &bytes).into_any().unbind(),
                                ));
                            });
                        }
                    }
                },
                graph_node_opts(Some(format!("{helper_name}/outbound_bytes"))),
            );
            py_node(node, &self.pending_fatal)
        };
        let status = py_protobuf_filter_node(
            &topology,
            &events,
            format!("{helper_name}/status"),
            &self.pending_fatal,
            py_protobuf_event_status,
        );
        let issues = py_protobuf_filter_node(
            &topology,
            &events,
            format!("{helper_name}/issues"),
            &self.pending_fatal,
            py_protobuf_event_issue,
        );
        let attach = catch_unwind(AssertUnwindSafe(|| {
            bridge_core.attach_inbound_source_for_native(decoded_inbound.erased());
        }));
        if let Err(panic) = attach {
            topology.release_with_reason("wire_bridge_protobuf failed inbound attachment");
            panic::resume_unwind(panic);
        }
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(PyWireBridgeProtobuf {
            bridge: bridge_core,
            topology,
            inbound_source: decoded_inbound,
            inbound_bytes: py_node(inbound_bytes, &self.pending_fatal),
            outbound_bytes,
            status,
            issues,
            released: Cell::new(false),
        })
    }

    #[pyo3(signature = (bridge, inbound_edges, name = None))]
    fn _wire_edge_group(
        &self,
        bridge: PyRef<'_, PyWireBridge>,
        inbound_edges: Vec<String>,
        name: Option<String>,
    ) -> PyResult<PyWireEdgeGroup> {
        raise_pending_fatal(&self.pending_fatal)?;
        let group_name = name.unwrap_or_else(|| "wireEdgeGroup".to_owned());
        let group = catch_graph_panic(&self.pending_fatal, || {
            Rc::new(wire_edge_group(
                &self.graph,
                &bridge.bridge,
                WireEdgeGroupOptions::named(
                    group_name.clone(),
                    inbound_edges
                        .iter()
                        .map(|edge| WireEdgeGroupEdge::inbound(edge.clone()))
                        .collect(),
                ),
            ))
        })?;
        let facade_topology = self
            .graph
            .topology_group_opts(TopologyGroupOptions::named(format!(
                "{group_name}.pythonWireEdgeGroupFacade"
            )));
        let status = py_status_node_in_topology(
            &facade_topology,
            &group.status,
            format!("{group_name}/py/status"),
            &self.pending_fatal,
            py_wire_edge_group_status,
        );
        let issues = py_status_node_in_topology(
            &facade_topology,
            &group.issues,
            format!("{group_name}/py/issues"),
            &self.pending_fatal,
            py_wire_edge_group_issue,
        );
        let mut inbound_keepalives = Vec::new();
        let inbound_edges_dict = Python::attach(|py| -> PyResult<Py<PyDict>> {
            let dict = PyDict::new(py);
            for (edge_id, node) in &group.inbound {
                let py_edge = py_bytes_node_in_topology(
                    &facade_topology,
                    node,
                    format!("{group_name}/py/inbound/{edge_id}"),
                    &self.pending_fatal,
                );
                inbound_keepalives.push(node.subscribe(|_| {}));
                dict.set_item(edge_id, py_edge)?;
            }
            Ok(dict.unbind())
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(PyWireEdgeGroup {
            group,
            facade_topology,
            adapter_topology: None,
            adapter_projection_topology: None,
            inbound_keepalives: RefCell::new(inbound_keepalives),
            inbound_edges: inbound_edges_dict,
            status,
            issues,
            released: Cell::new(false),
        })
    }

    #[pyo3(signature = (bridge, outbound_edges, name = None))]
    #[allow(clippy::too_many_lines)]
    fn _wire_edge_group_outbound(
        &self,
        bridge: PyRef<'_, PyWireBridge>,
        outbound_edges: Vec<(String, PyNode)>,
        name: Option<String>,
    ) -> PyResult<PyWireEdgeGroup> {
        raise_pending_fatal(&self.pending_fatal)?;
        let group_name = name.unwrap_or_else(|| "wireEdgeGroup".to_owned());
        for (edge_id, node) in &outbound_edges {
            let current_version = node
                .graph_node
                .as_ref()
                .map_or_else(|| node.node.version(), GraphNode::version);
            if current_version.is_none() {
                return Err(PyValueError::new_err(format!(
                    "wire_edge_group outbound edge {edge_id} requires node runtime versioning for D561 fresh-source admission"
                )));
            }
        }
        let adapter_topology =
            self.graph
                .topology_group_opts(TopologyGroupOptions::named(format!(
                    "{group_name}.pythonWireEdgeGroupOutbound"
                )));
        let mut edges = Vec::new();
        let mut adapter_issues = Vec::new();
        let expected_edges = outbound_edges
            .iter()
            .map(|(edge_id, _)| edge_id.clone())
            .collect::<Vec<_>>();
        for (edge_id, node) in outbound_edges {
            let edge_name = edge_id.clone();
            let version_source_node = node.node.clone();
            let version_source_graph_node = node.graph_node.clone();
            let admitted_version: Rc<RefCell<Option<Option<NodeVersion>>>> =
                Rc::new(RefCell::new(None));
            let bytes_node = adapter_topology.node_opts::<Vec<u8>, _>(
                vec![node.erased_core()],
                {
                    let admitted_version = admitted_version.clone();
                    move |ctx| {
                        for value in ctx.batch::<PyValue>(0) {
                            let version = version_source_graph_node
                                .as_ref()
                                .map_or_else(|| version_source_node.version(), GraphNode::version);
                            if admitted_version.borrow().as_ref() == Some(&version) {
                                continue;
                            }
                            Python::attach(|py| {
                                if let Some(bytes) = py_bytes_like_to_vec(value.object.bind(py)) {
                                    *admitted_version.borrow_mut() = Some(version.clone());
                                    ctx.emit(bytes);
                                }
                            });
                        }
                    }
                },
                graph_node_opts(Some(format!("{group_name}/py/outbound/{edge_name}"))),
            );
            let issue_group_name = group_name.clone();
            let issue_edge_id = edge_id.clone();
            let issue_node = adapter_topology.node_opts::<WireEdgeGroupIssue, _>(
                vec![node.erased_core()],
                move |ctx| {
                    for value in ctx.batch::<PyValue>(0) {
                        Python::attach(|py| {
                            if let Some(bytes) = py_bytes_like_to_vec(value.object.bind(py)) {
                                drop(bytes);
                                return;
                            }
                            ctx.emit(WireEdgeGroupIssue {
                                code: WireEdgeGroupIssueCode::MalformedFrame,
                                message: format!(
                                    "{issue_group_name}: outbound edge {issue_edge_id} must emit bytes"
                                ),
                                edge_id: Some(issue_edge_id.clone()),
                                cause_id: None,
                                active_cause_id: None,
                            });
                        });
                    }
                },
                graph_node_opts(Some(format!("{group_name}/py/issues/{edge_name}"))),
            );
            adapter_issues.push(issue_node);
            edges.push(WireEdgeGroupEdge::outbound(edge_id, bytes_node));
        }
        let group = catch_graph_panic(&self.pending_fatal, || {
            Rc::new(wire_edge_group(
                &self.graph,
                &bridge.bridge,
                WireEdgeGroupOptions::named(group_name.clone(), edges),
            ))
        })?;
        let adapter_projection_topology =
            self.graph
                .topology_group_opts(TopologyGroupOptions::named(format!(
                    "{group_name}.pythonWireEdgeGroupOutboundProjection"
                )));
        let merged_issues = {
            let mut deps = vec![group.issues.erased()];
            deps.extend(adapter_issues.iter().map(Node::erased));
            adapter_projection_topology.node_opts::<WireEdgeGroupIssue, _>(
                deps,
                {
                    let adapter_issue_count = adapter_issues.len();
                    move |ctx| {
                        for issue in ctx.batch::<WireEdgeGroupIssue>(0) {
                            ctx.emit((*issue).clone());
                        }
                        for dep_index in 1..=adapter_issue_count {
                            for issue in ctx.batch::<WireEdgeGroupIssue>(dep_index) {
                                ctx.emit((*issue).clone());
                            }
                        }
                    }
                },
                graph_node_opts_with_node(
                    Some(format!("{group_name}/py/merged_issues")),
                    true,
                    false,
                    false,
                    false,
                ),
            )
        };
        let merged_status = {
            let mut deps = vec![group.status.erased()];
            deps.extend(adapter_issues.iter().map(Node::erased));
            adapter_projection_topology.node_opts::<WireEdgeGroupStatus, _>(
                deps,
                {
                    let adapter_issue_count = adapter_issues.len();
                    let status_expected_edges = expected_edges.clone();
                    move |ctx| {
                        let mut status = ctx.state_get::<WireEdgeGroupStatus>().map_or_else(
                            || WireEdgeGroupStatus {
                                state: WireEdgeGroupStatusState::Idle,
                                expected_edges: status_expected_edges.clone(),
                                active_cause_id: None,
                                dirty: 0,
                                data: 0,
                                released: 0,
                                issues: 0,
                                last_issue: None,
                            },
                            |status| (*status).clone(),
                        );
                        for group_status in ctx.batch::<WireEdgeGroupStatus>(0) {
                            status = (*group_status).clone();
                            ctx.emit(status.clone());
                        }
                        for dep_index in 1..=adapter_issue_count {
                            for issue in ctx.batch::<WireEdgeGroupIssue>(dep_index) {
                                status.state = WireEdgeGroupStatusState::Issues;
                                status.active_cause_id = None;
                                status.dirty = 0;
                                status.data = 0;
                                status.issues = status.issues.saturating_add(1);
                                status.last_issue = Some((*issue).clone());
                                ctx.emit(status.clone());
                            }
                        }
                        ctx.state_set(status);
                    }
                },
                graph_node_opts(Some(format!("{group_name}/py/merged_status"))),
            )
        };
        let facade_topology = self
            .graph
            .topology_group_opts(TopologyGroupOptions::named(format!(
                "{group_name}.pythonWireEdgeGroupFacade"
            )));
        let status = py_status_node_in_topology(
            &facade_topology,
            &merged_status,
            format!("{group_name}/py/status"),
            &self.pending_fatal,
            py_wire_edge_group_status,
        );
        let issues = py_status_node_in_topology(
            &facade_topology,
            &merged_issues,
            format!("{group_name}/py/issues"),
            &self.pending_fatal,
            py_wire_edge_group_issue,
        );
        let inbound_edges_dict =
            Python::attach(|py| -> PyResult<Py<PyDict>> { Ok(PyDict::new(py).unbind()) })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(PyWireEdgeGroup {
            group,
            facade_topology,
            adapter_topology: Some(adapter_topology),
            adapter_projection_topology: Some(adapter_projection_topology),
            inbound_keepalives: RefCell::new(Vec::new()),
            inbound_edges: inbound_edges_dict,
            status,
            issues,
            released: Cell::new(false),
        })
    }

    #[pyo3(signature = (bridge, clock, timeout_ms, name = None))]
    #[allow(clippy::too_many_lines)]
    fn _wire_bridge_ack_driver(
        &self,
        bridge: PyRef<'_, PyWireBridge>,
        clock: PyNode,
        timeout_ms: u64,
        name: Option<String>,
    ) -> PyResult<PyWireBridgeAckDriver> {
        raise_pending_fatal(&self.pending_fatal)?;
        let driver_name = name.unwrap_or_else(|| "wireBridgeAckDriver".to_owned());
        let bridge_core = bridge.bridge.clone();
        let state = Rc::new(RefCell::new(PyAckDriverState::default()));
        let mut subscriptions: Vec<Box<dyn FnOnce()>> = Vec::new();
        {
            let state = state.clone();
            subscriptions.push(bridge_core.attempts.subscribe(move |msg| {
                if let Message::Data(value) = msg {
                    let Some(attempt) = value.downcast_ref::<WireBridgeAttempt>() else {
                        return;
                    };
                    let mut state = state.borrow_mut();
                    let observed_at_ms = state.now_ms;
                    state.pending.insert(
                        attempt.seq,
                        PyAckDriverPending {
                            seq: attempt.seq,
                            attempt: attempt.attempt,
                            observed_at_ms,
                            timed_out_attempt: None,
                            timed_out_at_ms: None,
                            retry_due_at_ms: None,
                            retry_released_attempt: None,
                        },
                    );
                }
            }));
        }
        {
            let state = state.clone();
            subscriptions.push(bridge_core.acks.subscribe(move |msg| {
                if let Message::Data(value) = msg {
                    let Some(ack) =
                        value.downcast_ref::<WireBridgeAck<WireBridgeProtobufDataBody>>()
                    else {
                        return;
                    };
                    state.borrow_mut().pending.remove(&ack.ack_for_seq);
                }
            }));
        }
        {
            let state = state.clone();
            subscriptions.push(bridge_core.nacks.subscribe(move |msg| {
                if let Message::Data(value) = msg {
                    let Some(nack) =
                        value.downcast_ref::<WireBridgeNack<WireBridgeProtobufDataBody>>()
                    else {
                        return;
                    };
                    state.borrow_mut().pending.remove(&nack.ack_for_seq);
                }
            }));
        }
        {
            let state = state.clone();
            subscriptions.push(bridge_core.status.subscribe(move |msg| {
                if let Message::Data(value) = msg {
                    let Some(status) = value.downcast_ref::<WireBridgeStatus>() else {
                        return;
                    };
                    let mut state = state.borrow_mut();
                    if status.pending == 0 {
                        state.pending.clear();
                    } else if status.state == WireBridgeStatusState::Exhausted {
                        if let Some(seq) = status.last_seq {
                            state.pending.remove(&seq);
                        }
                    } else if status.state == WireBridgeStatusState::Waiting {
                        if let (Some(seq), Some(delay_ms)) = (status.last_seq, status.last_delay_ms)
                        {
                            if let Some(pending) = state.pending.get_mut(&seq) {
                                if pending.timed_out_at_ms.is_some()
                                    && pending.retry_released_attempt != Some(pending.attempt)
                                {
                                    pending.retry_due_at_ms = pending
                                        .timed_out_at_ms
                                        .map(|ms| ms.saturating_add(delay_ms));
                                }
                            }
                        }
                    }
                }
            }));
        }
        let subscription_guard = PySubscriptionDrainGuard::new(subscriptions);
        let topology = self
            .graph
            .topology_group_opts(TopologyGroupOptions::named(format!(
                "{driver_name}.wireBridgeAckDriver"
            )));
        let events = topology.node_opts::<PyAckDriverEvent, _>(
            vec![clock.erased_core()],
            {
                let state = state.clone();
                move |ctx| {
                    let mut emitted = false;
                    for value in ctx.batch::<PyValue>(0) {
                        let mut valid_clock = true;
                        Python::attach(|py| {
                            let object = value.object.bind(py);
                            let issue_message = if object.is_instance_of::<PyBool>() {
                                Some("wire_bridge_ack_driver clock facts must be non-negative integers")
                            } else {
                                match object.extract::<u64>() {
                                    Ok(observed_at_ms) => {
                                        let mut state = state.borrow_mut();
                                        if state.now_ms.is_some_and(|last| observed_at_ms < last) {
                                            Some("wire_bridge_ack_driver clock facts must be monotonic non-decreasing")
                                        } else {
                                            state.now_ms = Some(observed_at_ms);
                                            None
                                        }
                                    }
                                    Err(_) => Some(
                                        "wire_bridge_ack_driver clock facts must be non-negative integers",
                                    ),
                                }
                            };
                            if let Some(message) = issue_message {
                                let state = state.borrow();
                                ctx.emit(PyAckDriverEvent::Issue {
                                    code: "invalid_clock",
                                    message,
                                    pending: state.pending.len(),
                                    now_ms: state.now_ms,
                                });
                                emitted = true;
                                valid_clock = false;
                            }
                        });
                        if !valid_clock {
                            continue;
                        }
                        let now_ms = state.borrow().now_ms;
                        let Some(now_ms) = now_ms else {
                            continue;
                        };
                        let events = {
                            let mut state = state.borrow_mut();
                            let pending_len = state.pending.len();
                            let mut events = Vec::new();
                            for pending in state.pending.values_mut() {
                                if pending.observed_at_ms.is_none() {
                                    pending.observed_at_ms = Some(now_ms);
                                }
                                if pending.observed_at_ms.is_some_and(|observed| {
                                    now_ms.saturating_sub(observed) >= timeout_ms
                                        && pending.timed_out_attempt != Some(pending.attempt)
                                }) {
                                    pending.timed_out_attempt = Some(pending.attempt);
                                    pending.timed_out_at_ms = Some(now_ms);
                                    events.push(PyAckDriverEvent::Timeout {
                                        seq: pending.seq,
                                        attempt: pending.attempt,
                                        observed_at_ms: now_ms,
                                        pending: pending_len,
                                        now_ms,
                                    });
                                } else if pending.retry_due_at_ms.is_some_and(|due| {
                                    now_ms >= due
                                        && pending.retry_released_attempt != Some(pending.attempt)
                                }) {
                                    pending.retry_released_attempt = Some(pending.attempt);
                                    events.push(PyAckDriverEvent::Timeout {
                                        seq: pending.seq,
                                        attempt: pending.attempt,
                                        observed_at_ms: now_ms,
                                        pending: pending_len,
                                        now_ms,
                                    });
                                }
                            }
                            if events.is_empty() {
                                events.push(PyAckDriverEvent::State {
                                    pending: state.pending.len(),
                                    now_ms: state.now_ms,
                                });
                            }
                            events
                        };
                        for event in events {
                            emitted = true;
                            ctx.emit(event);
                        }
                    }
                }
            },
            graph_node_opts_with_node(
                Some(format!("{driver_name}/events")),
                true,
                false,
                false,
                false,
            ),
        );
        let command_source = topology
            .node_opts::<WireBridgeCommand<WireBridgeProtobufDataBody>, _>(
                vec![events.erased()],
                move |ctx| {
                    for event in ctx.batch::<PyAckDriverEvent>(0) {
                        if let PyAckDriverEvent::Timeout {
                            seq,
                            attempt,
                            observed_at_ms,
                            ..
                        } = event.as_ref()
                        {
                            ctx.emit::<WireBridgeCommand<WireBridgeProtobufDataBody>>(
                                WireBridgeCommand::AckTimeout {
                                    seq: *seq,
                                    attempt: *attempt,
                                    observed_at_ms: Some(*observed_at_ms),
                                },
                            );
                        }
                    }
                },
                graph_node_opts(Some(format!("{driver_name}/commands"))),
            );
        let timeout_node = py_ack_filter_node(
            &topology,
            &events,
            format!("{driver_name}/timeouts"),
            &self.pending_fatal,
            py_ack_timeout,
        );
        let status = py_ack_status_node(
            &topology,
            &events,
            format!("{driver_name}/status"),
            &self.pending_fatal,
            timeout_ms,
        );
        let issues = py_ack_filter_node(
            &topology,
            &events,
            format!("{driver_name}/issues"),
            &self.pending_fatal,
            py_ack_issue,
        );
        let attach = catch_unwind(AssertUnwindSafe(|| {
            bridge_core.attach_command_source_for_native(command_source.erased());
        }));
        if let Err(panic) = attach {
            topology.release_with_reason("wire_bridge_ack_driver failed command attachment");
            panic::resume_unwind(panic);
        }
        if let Err(error) = raise_pending_fatal(&self.pending_fatal) {
            bridge_core.detach_command_source_for_native(command_source.erased());
            topology.release_with_reason("wire_bridge_ack_driver failed after command attachment");
            return Err(error);
        }
        let subscriptions = subscription_guard.disarm();
        Ok(PyWireBridgeAckDriver {
            bridge: bridge_core,
            topology,
            command_source,
            subscriptions: RefCell::new(subscriptions),
            timeouts: timeout_node,
            status,
            issues,
            released: Cell::new(false),
        })
    }

    #[pyo3(signature = (callback, name = None))]
    fn producer(&self, callback: Py<PyAny>, name: Option<String>) -> PyResult<PyNode> {
        raise_pending_fatal(&self.pending_fatal)?;
        let pending_fatal = self.pending_fatal.clone();
        let node = catch_graph_panic(&self.pending_fatal, || {
            let callback_pending_fatal = pending_fatal.clone();
            let node = self.graph.producer_opts(
                move |ctx| {
                    let result = Python::attach(|py| callback.call0(py));
                    emit_callback_result(ctx, &callback_pending_fatal, result);
                },
                graph_node_opts(name),
            );
            PyNode {
                node,
                graph_node: None,
                pending_fatal,
            }
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(node)
    }

    #[pyo3(signature = (
        deps,
        callback,
        name = None,
        partial = false,
        complete_when_deps_complete = true,
        error_when_deps_error = true,
        terminal_as_real_input = false,
        pausable = None,
        pull_id = None,
        restore_ref = None,
        restore_config = None,
        restore_config_version = None
    ))]
    fn node(
        &self,
        py: Python<'_>,
        deps: Vec<Py<PyNode>>,
        callback: Py<PyAny>,
        name: Option<String>,
        partial: bool,
        complete_when_deps_complete: bool,
        error_when_deps_error: bool,
        terminal_as_real_input: bool,
        pausable: Option<String>,
        pull_id: Option<String>,
        restore_ref: Option<String>,
        restore_config: Option<Py<PyAny>>,
        restore_config_version: Option<Py<PyAny>>,
    ) -> PyResult<PyNode> {
        raise_pending_fatal(&self.pending_fatal)?;
        let deps = deps
            .iter()
            .map(|dep| dep.borrow(py).erased_core())
            .collect::<Vec<_>>();
        let mut opts = graph_node_opts_with_conformance(
            name,
            partial,
            complete_when_deps_complete,
            error_when_deps_error,
            terminal_as_real_input,
            pausable,
            pull_id,
        )?;
        apply_restore_opts(
            py,
            &mut opts,
            restore_ref,
            restore_config,
            restore_config_version,
        )?;
        let pending_fatal = self.pending_fatal.clone();
        let node = catch_graph_panic(&self.pending_fatal, || {
            let callback_pending_fatal = pending_fatal.clone();
            let node = self.graph.node_opts::<PyValue, _>(
                deps,
                move |ctx| {
                    let active = Rc::new(Cell::new(true));
                    let result = Python::attach(|py| {
                        let initial_state = ctx_state_value(py, ctx)?;
                        let py_ctx = Py::new(
                            py,
                            PyCtx {
                                snapshot: ctx.defer(),
                                pull: ctx.pull().cloned(),
                                initial_state,
                                ops: RefCell::new(Vec::new()),
                                active: active.clone(),
                            },
                        )?;
                        let callback_result = callback.call1(py, (py_ctx.clone_ref(py),));
                        match callback_result {
                            Ok(value) => {
                                active.set(false);
                                commit_py_ctx(py, &py_ctx, ctx, &callback_pending_fatal);
                                Ok(value)
                            }
                            Err(error) => {
                                active.set(false);
                                Err(error)
                            }
                        }
                    });
                    handle_callback_void_result(ctx, &callback_pending_fatal, result);
                },
                opts,
            );
            PyNode {
                node,
                graph_node: None,
                pending_fatal,
            }
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(node)
    }

    #[pyo3(signature = (
        deps,
        callback,
        name = None,
        partial = false,
        complete_when_deps_complete = true,
        error_when_deps_error = true,
        terminal_as_real_input = false,
        pausable = None,
        pull_id = None
    ))]
    fn _conformance_node(
        &self,
        py: Python<'_>,
        deps: Vec<Py<PyNode>>,
        callback: Py<PyAny>,
        name: Option<String>,
        partial: bool,
        complete_when_deps_complete: bool,
        error_when_deps_error: bool,
        terminal_as_real_input: bool,
        pausable: Option<String>,
        pull_id: Option<String>,
    ) -> PyResult<PyNode> {
        raise_pending_fatal(&self.pending_fatal)?;
        let deps = deps
            .iter()
            .map(|dep| dep.borrow(py).erased_core())
            .collect::<Vec<_>>();
        let opts = graph_node_opts_with_conformance(
            name,
            partial,
            complete_when_deps_complete,
            error_when_deps_error,
            terminal_as_real_input,
            pausable,
            pull_id,
        )?;
        let pending_fatal = self.pending_fatal.clone();
        let node = catch_graph_panic(&self.pending_fatal, || {
            let callback_pending_fatal = pending_fatal.clone();
            let node = self.graph.node_opts::<PyValue, _>(
                deps,
                move |ctx| {
                    let active = Rc::new(Cell::new(true));
                    let result = Python::attach(|py| {
                        let initial_state = ctx_state_value(py, ctx)?;
                        let py_ctx = Py::new(
                            py,
                            PyCtx {
                                snapshot: ctx.defer(),
                                pull: ctx.pull().cloned(),
                                initial_state,
                                ops: RefCell::new(Vec::new()),
                                active: active.clone(),
                            },
                        )?;
                        let callback_result = callback.call1(py, (py_ctx.clone_ref(py),));
                        match callback_result {
                            Ok(value) => {
                                active.set(false);
                                commit_py_ctx(py, &py_ctx, ctx, &callback_pending_fatal);
                                Ok(value)
                            }
                            Err(error) => {
                                active.set(false);
                                Err(error)
                            }
                        }
                    });
                    handle_callback_void_result(ctx, &callback_pending_fatal, result);
                },
                opts,
            );
            PyNode {
                node,
                graph_node: None,
                pending_fatal,
            }
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(node)
    }

    #[pyo3(signature = (deps, name = None, pausable = None))]
    fn _conformance_async_node(
        &self,
        py: Python<'_>,
        deps: Vec<Py<PyNode>>,
        name: Option<String>,
        pausable: Option<String>,
    ) -> PyResult<(PyNode, PyConformanceAsyncHandle)> {
        raise_pending_fatal(&self.pending_fatal)?;
        let deps = deps
            .iter()
            .map(|dep| dep.borrow(py).erased_core())
            .collect::<Vec<_>>();
        let mut opts =
            graph_node_opts_with_conformance(name, false, true, true, false, pausable, None)?;
        opts.node.pool = PoolKind::Async;
        let pending: Rc<RefCell<Option<DeferredCtx>>> = Rc::new(RefCell::new(None));
        let node_pending = pending.clone();
        let node = catch_graph_panic(&self.pending_fatal, || {
            let node = self.graph.node_opts::<PyValue, _>(
                deps,
                move |ctx| {
                    *node_pending.borrow_mut() = Some(ctx.defer());
                },
                opts,
            );
            PyNode {
                node,
                graph_node: None,
                pending_fatal: self.pending_fatal.clone(),
            }
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok((
            node,
            PyConformanceAsyncHandle {
                pending,
                pending_fatal: self.pending_fatal.clone(),
            },
        ))
    }

    #[pyo3(signature = (name = None, pausable = None))]
    fn _conformance_async_source(
        &self,
        name: Option<String>,
        pausable: Option<String>,
    ) -> PyResult<(PyNode, PyConformanceAsyncHandle)> {
        raise_pending_fatal(&self.pending_fatal)?;
        let mut opts =
            graph_node_opts_with_conformance(name, false, true, true, false, pausable, None)?;
        opts.node.pool = PoolKind::Async;
        let pending: Rc<RefCell<Option<DeferredCtx>>> = Rc::new(RefCell::new(None));
        let node_pending = pending.clone();
        let node = catch_graph_panic(&self.pending_fatal, || {
            let node = self.graph.producer_opts::<PyValue, _>(
                move |ctx| {
                    *node_pending.borrow_mut() = Some(ctx.defer());
                },
                opts,
            );
            PyNode {
                node,
                graph_node: None,
                pending_fatal: self.pending_fatal.clone(),
            }
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok((
            node,
            PyConformanceAsyncHandle {
                pending,
                pending_fatal: self.pending_fatal.clone(),
            },
        ))
    }

    #[pyo3(signature = (callback, name = None, pausable = None))]
    fn _async_source(
        &self,
        callback: Py<PyAny>,
        name: Option<String>,
        pausable: Option<String>,
    ) -> PyResult<PyNode> {
        raise_pending_fatal(&self.pending_fatal)?;
        let mut opts =
            graph_node_opts_with_conformance(name, false, true, true, false, pausable, None)?;
        opts.node.pool = PoolKind::Async;
        let pending_fatal = self.pending_fatal.clone();
        let node = catch_graph_panic(&self.pending_fatal, || {
            let callback_pending_fatal = pending_fatal.clone();
            let node = self.graph.producer_opts::<PyValue, _>(
                move |ctx| {
                    let active = Rc::new(Cell::new(true));
                    let result = Python::attach(|py| {
                        let py_ctx = Py::new(
                            py,
                            PyAsyncCtx {
                                deferred: ctx.defer(),
                                ops: RefCell::new(Vec::new()),
                                active: active.clone(),
                                pending_fatal: callback_pending_fatal.clone(),
                            },
                        )?;
                        let callback_result = callback.call1(py, (py_ctx.clone_ref(py),));
                        match callback_result {
                            Ok(value) => {
                                commit_py_async_ctx(py, &py_ctx, ctx);
                                Ok(value)
                            }
                            Err(error) => {
                                active.set(false);
                                Err(error)
                            }
                        }
                    });
                    handle_callback_void_result(ctx, &callback_pending_fatal, result);
                },
                opts,
            );
            PyNode {
                node,
                graph_node: None,
                pending_fatal,
            }
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(node)
    }

    #[pyo3(signature = (deps, callback, name = None, pausable = None))]
    fn _async_node(
        &self,
        py: Python<'_>,
        deps: Vec<Py<PyNode>>,
        callback: Py<PyAny>,
        name: Option<String>,
        pausable: Option<String>,
    ) -> PyResult<PyNode> {
        raise_pending_fatal(&self.pending_fatal)?;
        let deps = deps
            .iter()
            .map(|dep| dep.borrow(py).erased_core())
            .collect::<Vec<_>>();
        let mut opts =
            graph_node_opts_with_conformance(name, false, true, true, false, pausable, None)?;
        opts.node.pool = PoolKind::Async;
        let pending_fatal = self.pending_fatal.clone();
        let node = catch_graph_panic(&self.pending_fatal, || {
            let callback_pending_fatal = pending_fatal.clone();
            let node = self.graph.node_opts::<PyValue, _>(
                deps,
                move |ctx| {
                    let active = Rc::new(Cell::new(true));
                    let result = Python::attach(|py| {
                        let args = dep_args_from_ctx(py, ctx)?;
                        let py_ctx = Py::new(
                            py,
                            PyAsyncCtx {
                                deferred: ctx.defer(),
                                ops: RefCell::new(Vec::new()),
                                active: active.clone(),
                                pending_fatal: callback_pending_fatal.clone(),
                            },
                        )?;
                        let mut call_args = Vec::with_capacity(args.len() + 1);
                        call_args.push(py_ctx.clone_ref(py).into_any());
                        call_args.extend(args);
                        let tuple = PyTuple::new(py, call_args)?;
                        let callback_result = callback.call1(py, tuple);
                        match callback_result {
                            Ok(value) => {
                                commit_py_async_ctx(py, &py_ctx, ctx);
                                Ok(value)
                            }
                            Err(error) => {
                                active.set(false);
                                Err(error)
                            }
                        }
                    });
                    handle_callback_void_result(ctx, &callback_pending_fatal, result);
                },
                opts,
            );
            PyNode {
                node,
                graph_node: None,
                pending_fatal,
            }
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(node)
    }

    #[pyo3(signature = (deps, callback, name = None))]
    fn derived(
        &self,
        py: Python<'_>,
        deps: Vec<Py<PyNode>>,
        callback: Py<PyAny>,
        name: Option<String>,
    ) -> PyResult<PyNode> {
        raise_pending_fatal(&self.pending_fatal)?;
        let deps = deps
            .iter()
            .map(|dep| dep.borrow(py).erased_core())
            .collect::<Vec<_>>();
        let pending_fatal = self.pending_fatal.clone();
        let node = catch_graph_panic(&self.pending_fatal, || {
            let callback_pending_fatal = pending_fatal.clone();
            let op = Operator::<PyValue>::new("derived", move |ctx| {
                let result = Python::attach(|py| {
                    let args = PyTuple::new(py, dep_args_from_ctx(py, ctx)?)?;
                    callback.call1(py, args)
                });
                emit_callback_result(ctx, &callback_pending_fatal, result);
            });
            let node = self.graph.init_node(op, deps, graph_node_opts(name));
            PyNode {
                node,
                graph_node: None,
                pending_fatal,
            }
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(node)
    }

    #[pyo3(signature = (deps, callback, name = None))]
    fn effect(
        &self,
        py: Python<'_>,
        deps: Vec<Py<PyNode>>,
        callback: Py<PyAny>,
        name: Option<String>,
    ) -> PyResult<PyNode> {
        raise_pending_fatal(&self.pending_fatal)?;
        let deps = deps
            .iter()
            .map(|dep| dep.borrow(py).erased_core())
            .collect::<Vec<_>>();
        let pending_fatal = self.pending_fatal.clone();
        let node = catch_graph_panic(&self.pending_fatal, || {
            let callback_pending_fatal = pending_fatal.clone();
            let op = Operator::<PyValue>::new("effect", move |ctx| {
                let result = Python::attach(|py| {
                    let args = PyTuple::new(py, dep_args_from_ctx(py, ctx)?)?;
                    callback.call1(py, args)
                });
                if let Err(error) = result {
                    Python::attach(|py| {
                        if py_error_is_fatal(py, &error) {
                            store_pending_fatal(&callback_pending_fatal, error);
                            graphrefly_rs::host_boundary::abort_host_boundary();
                        } else {
                            ctx.down(vec![Message::Error(py_exception_to_error(error))]);
                        }
                    });
                }
            });
            let node = self.graph.init_node(op, deps, graph_node_opts(name));
            PyNode {
                node,
                graph_node: None,
                pending_fatal,
            }
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(node)
    }

    fn batch(&self, callback: Py<PyAny>) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        catch_graph_panic(&self.pending_fatal, || {
            self.graph.batch(|batch| {
                let result = Python::attach(|py| callback.call0(py));
                if result.is_err() {
                    batch.rollback();
                }
                result
            })
        })??;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn describe(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        raise_pending_fatal(&self.pending_fatal)?;
        describe_snapshot(py, self.graph.describe())
    }

    fn _find(&self, id: String) -> PyResult<Option<PyNode>> {
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(self.graph.find(&id).map(|node| PyNode {
            node: Node::<PyValue>::state_empty(),
            graph_node: Some(node),
            pending_fatal: self.pending_fatal.clone(),
        }))
    }

    fn _set_state_by_id(&self, id: String, value: Py<PyAny>) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        let node = self.graph.find(&id).ok_or_else(|| {
            PyRuntimeError::new_err(format!("conformance node '{id}' was not found"))
        })?;
        let is_state = self
            .graph
            .describe()
            .nodes
            .iter()
            .any(|entry| entry.id == id && entry.factory == "state");
        if !is_state {
            return Err(PyRuntimeError::new_err(
                "conformance state setter requires a graph state node",
            ));
        }
        catch_graph_panic(&self.pending_fatal, || {
            let value: AnyValue = Rc::new(PyValue::new(value));
            node.down(vec![Message::Data(value)]);
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn observe(&self, callback: Py<PyAny>) -> PyResult<PySubscription> {
        raise_pending_fatal(&self.pending_fatal)?;
        let pending_fatal = self.pending_fatal.clone();
        let observer = catch_graph_panic(&self.pending_fatal, || {
            self.graph.observe().subscribe(move |event| {
                Python::attach(|py| {
                    let payload = match &event.msg {
                        graphrefly_rs::ObserveMessage::Data(value) => value_from_any(py, value),
                        graphrefly_rs::ObserveMessage::Error(error) => {
                            Ok(PyString::new(py, error).into())
                        }
                        _ => Ok(py.None()),
                    };
                    match payload.and_then(|payload| {
                        callback.call1(
                            py,
                            (
                                event.path,
                                event.msg.kind(),
                                payload,
                                event.tier.as_u8(),
                                event.seq,
                            ),
                        )
                    }) {
                        Ok(_) => {}
                        Err(error) => {
                            if py_error_is_fatal(py, &error) {
                                store_pending_fatal(&pending_fatal, error);
                                graphrefly_rs::host_boundary::abort_host_boundary();
                            } else {
                                error.print(py);
                            }
                        }
                    }
                });
            })
        })?;
        let subscription = PySubscription {
            unsubscribe: RefCell::new(Some(Box::new(move || drop(observer)))),
            pending_fatal: self.pending_fatal.clone(),
        };
        if let Err(error) = raise_pending_fatal(&self.pending_fatal) {
            let _ = subscription.unsubscribe();
            return Err(error);
        }
        Ok(subscription)
    }

    fn raise_pending_fatal(&self) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)
    }
}

#[pyclass(name = "Node", unsendable, from_py_object)]
struct PyNode {
    node: Node<PyValue>,
    graph_node: Option<GraphNode>,
    pending_fatal: PendingFatal,
}

impl PyNode {
    fn erased_core(&self) -> Core {
        self.graph_node
            .as_ref()
            .map_or_else(|| self.node.erased(), GraphNode::core)
    }

    fn reject_lookup_mutation(&self, method: &str) -> PyResult<()> {
        if self.graph_node.is_some() {
            Err(PyRuntimeError::new_err(format!(
                "{method} is not available on graph lookup handles"
            )))
        } else {
            Ok(())
        }
    }
}

#[pymethods]
impl PyNode {
    fn set(&self, _py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        self.reject_lookup_mutation("set()")?;
        catch_graph_panic(&self.pending_fatal, || {
            self.node.set(PyValue::new(value));
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn send(&self, _py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        self.reject_lookup_mutation("send()")?;
        catch_graph_panic(&self.pending_fatal, || {
            let value: AnyValue = Rc::new(PyValue::new(value));
            let msg = vec![Message::Data(value)];
            self.node.down(msg);
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn pause(&self, lock_id: String) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        self.reject_lookup_mutation("pause()")?;
        catch_graph_panic(&self.pending_fatal, || {
            self.node.up(vec![Message::Pause(LockId::new(lock_id))]);
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn resume(&self, lock_id: String) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        self.reject_lookup_mutation("resume()")?;
        catch_graph_panic(&self.pending_fatal, || {
            self.node.up(vec![Message::Resume(LockId::new(lock_id))]);
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn invalidate(&self) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        self.reject_lookup_mutation("invalidate()")?;
        catch_graph_panic(&self.pending_fatal, || {
            self.node.up(vec![Message::Invalidate]);
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn _up_dirty(&self) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        catch_graph_panic(&self.pending_fatal, || {
            self.node.up(vec![Message::Dirty]);
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn _up_teardown(&self) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        catch_graph_panic(&self.pending_fatal, || {
            self.node.up(vec![Message::Teardown]);
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    #[pyo3(signature = (pull_id, params = None))]
    fn _conformance_up_pull(&self, pull_id: String, params: Option<Py<PyAny>>) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        catch_graph_panic(&self.pending_fatal, || {
            self.node.up(vec![private_pull_demand(pull_id, params)]);
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    #[pyo3(signature = (toward_dep, pull_id, params = None))]
    fn _conformance_up_pull_toward(
        &self,
        toward_dep: usize,
        pull_id: String,
        params: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        catch_graph_panic(&self.pending_fatal, || {
            self.node
                .up_toward(toward_dep, vec![private_pull_demand(pull_id, params)]);
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn _conformance_up_data_forbidden(&self, value: Py<PyAny>) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        catch_graph_panic(&self.pending_fatal, || {
            self.node
                .up(vec![Message::Data(Rc::new(PyValue::new(value)))]);
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn _conformance_immediate_subscribe_dep(
        &self,
        py: Python<'_>,
        dep: Py<PyNode>,
        callback: Py<PyAny>,
    ) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        let dep = dep.borrow(py).erased_core();
        let pending_fatal = self.pending_fatal.clone();
        catch_graph_panic(&self.pending_fatal, || {
            let callback_pending_fatal = pending_fatal.clone();
            self.node.subscribe_dep(dep, move |ctx| {
                invoke_py_ctx_callback(ctx, &callback, &callback_pending_fatal);
            });
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn _conformance_immediate_unsubscribe_dep(
        &self,
        py: Python<'_>,
        dep: Py<PyNode>,
        callback: Py<PyAny>,
    ) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        let dep = dep.borrow(py).erased_core();
        let pending_fatal = self.pending_fatal.clone();
        catch_graph_panic(&self.pending_fatal, || {
            let callback_pending_fatal = pending_fatal.clone();
            self.node.unsubscribe_dep(dep, move |ctx| {
                invoke_py_ctx_callback(ctx, &callback, &callback_pending_fatal);
            });
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn _conformance_immediate_replace_deps(
        &self,
        py: Python<'_>,
        deps: Vec<Py<PyNode>>,
        callback: Py<PyAny>,
    ) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        let deps = deps
            .iter()
            .map(|dep| dep.borrow(py).erased_core())
            .collect::<Vec<_>>();
        let pending_fatal = self.pending_fatal.clone();
        catch_graph_panic(&self.pending_fatal, || {
            let callback_pending_fatal = pending_fatal.clone();
            self.node.replace_deps(deps, move |ctx| {
                invoke_py_ctx_callback(ctx, &callback, &callback_pending_fatal);
            });
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn _conformance_c21_replace_with_live_dep(
        &self,
        py: Python<'_>,
        dep: Py<PyNode>,
        async_handle: Py<PyConformanceAsyncHandle>,
    ) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        let dep = dep.borrow(py).erased_core();
        let node_pending = {
            let async_handle = async_handle.borrow(py);
            raise_pending_fatal(&async_handle.pending_fatal)?;
            async_handle.pending.clone()
        };
        catch_graph_panic(&self.pending_fatal, || {
            self.node.replace_deps(vec![dep], move |ctx| {
                *node_pending.borrow_mut() = Some(ctx.defer());
            });
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn _down_resolved(&self) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        catch_graph_panic(&self.pending_fatal, || {
            self.node.down(vec![Message::Resolved]);
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn _down_dirty(&self) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        catch_graph_panic(&self.pending_fatal, || {
            self.node.down(vec![Message::Dirty]);
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn _conformance_c22_down_data(&self, value: Py<PyAny>) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        catch_graph_panic(&self.pending_fatal, || {
            self.node
                .down(vec![Message::Data(Rc::new(PyValue::new(value)))]);
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn _down_invalidate(&self) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        catch_graph_panic(&self.pending_fatal, || {
            self.node.down(vec![Message::Invalidate]);
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn _down_data_data_invalidate(&self, first: Py<PyAny>, second: Py<PyAny>) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        catch_graph_panic(&self.pending_fatal, || {
            self.node.down(vec![
                Message::Data(Rc::new(PyValue::new(first))),
                Message::Data(Rc::new(PyValue::new(second))),
                Message::Invalidate,
            ]);
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn _conformance_c12_down_data_resolved(&self, value: Py<PyAny>) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        catch_graph_panic(&self.pending_fatal, || {
            self.node.down(vec![
                Message::Data(Rc::new(PyValue::new(value))),
                Message::Resolved,
            ]);
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn _down_data_complete(&self, value: Py<PyAny>) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        catch_graph_panic(&self.pending_fatal, || {
            self.node.down(vec![
                Message::Data(Rc::new(PyValue::new(value))),
                Message::Complete,
            ]);
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn _down_complete(&self) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        catch_graph_panic(&self.pending_fatal, || {
            self.node.down(vec![Message::Complete]);
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn _down_teardown(&self) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        catch_graph_panic(&self.pending_fatal, || {
            self.node.down(vec![Message::Teardown]);
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn _down_error(&self, message: String) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        catch_graph_panic(&self.pending_fatal, || {
            self.node.down(vec![Message::Error(message.into())]);
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn cache(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        raise_pending_fatal(&self.pending_fatal)?;
        if let Some(node) = &self.graph_node {
            return node
                .cache_any()
                .map(|value| value_from_any(py, &value))
                .transpose();
        }
        if let Some(value) = self.node.cache() {
            return Ok(Some(value.clone_object(py)));
        }
        Ok(None)
    }

    fn cache_entry(&self, py: Python<'_>) -> PyResult<(bool, Py<PyAny>)> {
        raise_pending_fatal(&self.pending_fatal)?;
        if let Some(node) = &self.graph_node {
            return match node.cache_any() {
                Some(value) => Ok((true, value_from_any(py, &value)?)),
                None => Ok((false, py.None())),
            };
        }
        if let Some(value) = self.node.cache() {
            return Ok((true, value.clone_object(py)));
        }
        Ok((false, py.None()))
    }

    fn status(&self) -> PyResult<&'static str> {
        raise_pending_fatal(&self.pending_fatal)?;
        if let Some(node) = &self.graph_node {
            return Ok(status_name(node.status()));
        }
        Ok(status_name(self.node.status()))
    }

    fn subscribe(&self, callback: Py<PyAny>) -> PyResult<PySubscription> {
        raise_pending_fatal(&self.pending_fatal)?;
        let pending_fatal = self.pending_fatal.clone();
        let unsubscribe = catch_graph_panic(&self.pending_fatal, || {
            let callback_pending_fatal = pending_fatal.clone();
            let callback = move |msg: &Message<AnyValue>| {
                Python::attach(|py| {
                    let kind = format!("{msg:?}");
                    match py_value_from_msg(py, msg)
                        .and_then(|payload| callback.call1(py, (kind, payload)))
                    {
                        Ok(_) => {}
                        Err(error) => {
                            if py_error_is_fatal(py, &error) {
                                store_pending_fatal(&callback_pending_fatal, error);
                                graphrefly_rs::host_boundary::abort_host_boundary();
                            } else {
                                error.print(py);
                            }
                        }
                    }
                });
            };
            if let Some(node) = &self.graph_node {
                node.subscribe(callback)
            } else {
                self.node.subscribe(callback)
            }
        })?;
        if let Err(error) = raise_pending_fatal(&self.pending_fatal) {
            unsubscribe();
            return Err(error);
        }
        Ok(PySubscription {
            unsubscribe: RefCell::new(Some(unsubscribe)),
            pending_fatal: self.pending_fatal.clone(),
        })
    }
}

#[pyclass(name = "Subscription", unsendable)]
struct PySubscription {
    unsubscribe: RefCell<Option<Box<dyn FnOnce()>>>,
    pending_fatal: PendingFatal,
}

#[pymethods]
impl PySubscription {
    fn unsubscribe(&self) -> PyResult<()> {
        catch_graph_panic(&self.pending_fatal, || {
            if let Some(unsubscribe) = self.unsubscribe.borrow_mut().take() {
                unsubscribe();
            }
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }
}

impl Drop for PySubscription {
    fn drop(&mut self) {
        if let Some(unsubscribe) = self.unsubscribe.get_mut().take() {
            let pending_fatal = self.pending_fatal.clone();
            let _ = catch_graph_panic(&pending_fatal, unsubscribe);
            let _ = pending_fatal.borrow_mut().take();
        }
    }
}

#[pyfunction]
#[pyo3(signature = (entries, include_builtins = true))]
fn restore_registry(
    py: Python<'_>,
    entries: Vec<Py<PyAny>>,
    include_builtins: bool,
) -> PyResult<PyRestoreRegistry> {
    let pending_fatal = Rc::new(RefCell::new(None));
    let mut native_entries = Vec::new();
    if include_builtins {
        native_entries.push(GraphRestoreEntry::descriptor(StateRestoreDescriptor));
        native_entries.push(GraphRestoreEntry::descriptor(MapJsonRestoreDescriptor));
    }
    for entry in entries {
        let ref_obj = entry.getattr(py, "ref").map_err(|_| {
            PyValueError::new_err("restore registry entries must expose a string 'ref'")
        })?;
        let ref_: String = ref_obj
            .extract(py)
            .map_err(|_| PyValueError::new_err("restore registry entry 'ref' must be a string"))?;
        native_entries.push(GraphRestoreEntry::descriptor(PyRestoreDescriptorBridge {
            ref_,
            descriptor: entry,
            pending_fatal: pending_fatal.clone(),
        }));
    }
    let registry = GraphRestoreRegistry::try_new(native_entries)
        .map_err(|err| PyValueError::new_err(err.to_string()))?;
    Ok(PyRestoreRegistry {
        registry,
        pending_fatal,
    })
}

#[pyfunction]
fn restore_graph(
    py: Python<'_>,
    checkpoint: Py<PyAny>,
    registry: PyRef<'_, PyRestoreRegistry>,
) -> PyResult<PyGraph> {
    raise_pending_fatal(&registry.pending_fatal)?;
    let checkpoint = py_checkpoint_to_native(py, &checkpoint)?;
    let restored = match graphrefly_rs::restore_graph(
        checkpoint,
        RestoreGraphOptions::new(registry.registry.clone()),
    ) {
        Ok(restored) => restored,
        Err(err) => {
            raise_pending_fatal(&registry.pending_fatal)?;
            return Err(PyRuntimeError::new_err(err.to_string()));
        }
    };
    raise_pending_fatal(&registry.pending_fatal)?;
    Ok(PyGraph::restored(restored, registry.pending_fatal.clone()))
}

#[pyfunction]
fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    register_py_checkpoint_encoder();
    m.add_class::<PyCanonicalProtobufValidation>()?;
    m.add_class::<PyCanonicalProtobufRoundtrip>()?;
    m.add_class::<PyWireBridge>()?;
    m.add_class::<PyWireBridgeProtobuf>()?;
    m.add_class::<PyWireEdgeGroup>()?;
    m.add_class::<PyWireBridgeAckDriver>()?;
    m.add_class::<PyAsyncCtx>()?;
    m.add_class::<PyCtx>()?;
    m.add_class::<PyGraph>()?;
    m.add_class::<PyNode>()?;
    m.add_class::<PyRestoreContext>()?;
    m.add_class::<PyRestoreRegistry>()?;
    m.add_class::<PySubscription>()?;
    m.add_function(wrap_pyfunction!(
        _validate_canonical_wire_bridge_envelope,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(_validate_canonical_wire_edge_frame, m)?)?;
    m.add_function(wrap_pyfunction!(
        _roundtrip_canonical_wire_bridge_envelope,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(_roundtrip_canonical_wire_edge_frame, m)?)?;
    m.add_function(wrap_pyfunction!(restore_graph, m)?)?;
    m.add_function(wrap_pyfunction!(restore_registry, m)?)?;
    m.add_function(wrap_pyfunction!(version, m)?)?;
    Ok(())
}

#[cfg(all(test, feature = "test-python"))]
mod tests {
    use super::*;
    use pyo3::ffi::c_str;
    use pyo3::types::PyModule;

    #[test]
    fn python_callback_runs_through_rust_graph_and_subscriber_observes_wave() {
        Python::attach(|py| {
            let module = PyModule::from_code(
                py,
                c_str!(
                    r#"
def inc(x):
    return x + 1
"#
                ),
                c_str!("smoke.py"),
                c_str!("smoke"),
            )
            .expect("module builds");
            let inc = module.getattr("inc").expect("inc exists").unbind();

            let graph = PyGraph::new(Some("py-smoke".to_owned()));
            let source = graph
                .state(
                    py,
                    1i64.into_py_any(py).expect("int object"),
                    Some("source".to_owned()),
                )
                .expect("source");
            let dep = Py::new(py, source).expect("py node");
            let derived = graph.derived(py, vec![dep], inc, Some("plus_one".to_owned()));
            let seen: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(Vec::new()));
            let seen_for_callback = seen.clone();
            let callback = PyModule::from_code(
                py,
                c_str!(
                    r#"
def observe(kind, value):
    seen.append((kind, value))
"#
                ),
                c_str!("observe.py"),
                c_str!("observe"),
            )
            .expect("observer module");
            callback
                .setattr("seen", PyList::empty(py))
                .expect("seen install");
            let observe = callback.getattr("observe").expect("observe").unbind();

            let sub = derived.subscribe(observe);
            let cache = derived.cache(py).expect("cache after push-on-subscribe");
            let value: i64 = cache.extract(py).expect("integer cache");
            assert_eq!(value, 2);

            let py_seen = callback.getattr("seen").expect("seen list");
            for item in py_seen.try_iter().expect("iter") {
                let tuple: (String, i64) = item.expect("item").extract().expect("tuple");
                seen_for_callback
                    .borrow_mut()
                    .push(format!("{}:{}", tuple.0, tuple.1));
            }
            assert!(seen.borrow().iter().any(|entry| entry == "DATA:2"));
            sub.unsubscribe();
        });
    }
}
