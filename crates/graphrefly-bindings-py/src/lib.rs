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
use std::error::Error;
use std::fmt;
use std::panic::{self, catch_unwind, AssertUnwindSafe};
use std::rc::Rc;
use std::sync::{Mutex, OnceLock};

use graphrefly_rs::{
    restored_opts, AnyValue, Ctx, DeferredCtx, DepTerminal, DescribeSnapshot, DescribeValue, Graph,
    GraphCheckpoint, GraphCheckpointJson, GraphNode, GraphNodeOpts, GraphRestoreDescriptor,
    GraphRestoreEntry, GraphRestoreError, GraphRestoreRegistry, GraphRestoreResult, LockId,
    MapJsonRestoreDescriptor, Message, Node, NodeOpts, Operator, Pausable, PoolKind, PullDemand,
    RestoreDefineCtx, RestoreFactoryMeta, RestoreGraphOptions, RestoreNodeDefinition,
    RestoreNodeKind, StateRestoreDescriptor, Status, WaveData,
};
use pyo3::exceptions::{PyException, PyIndexError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyTuple};
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
        Python::with_gil(|py| Self {
            object: self.object.clone_ref(py),
        })
    }
}

fn register_py_checkpoint_encoder() {
    graphrefly_rs::register_checkpoint_json_encoder::<PyValue>(|value, path| {
        Python::with_gil(|py| {
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
    let message = Python::with_gil(|py| format_callback_error(py, &error));
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
            Python::with_gil(|py| {
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
        Python::with_gil(|py| {
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

fn py_to_checkpoint_json(py: Python<'_>, value: &Py<PyAny>) -> PyResult<GraphCheckpointJson> {
    py_bound_to_checkpoint_json(py, value.bind(py))
}

fn py_bound_to_checkpoint_json(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> PyResult<GraphCheckpointJson> {
    if value.is_none() {
        return Ok(GraphCheckpointJson::Null);
    }
    if let Ok(value) = value.extract::<bool>() {
        return Ok(GraphCheckpointJson::Bool(value));
    }
    if let Ok(value) = value.extract::<i64>() {
        return Ok(GraphCheckpointJson::Number(JsonNumber::from(value)));
    }
    if let Ok(value) = value.extract::<u64>() {
        return Ok(GraphCheckpointJson::Number(JsonNumber::from(value)));
    }
    if let Ok(value) = value.extract::<f64>() {
        return JsonNumber::from_f64(value).map_or_else(
            || {
                Err(PyValueError::new_err(
                    "checkpoint value must be finite strict JSON",
                ))
            },
            |number| Ok(GraphCheckpointJson::Number(number)),
        );
    }
    if let Ok(value) = value.extract::<String>() {
        return Ok(GraphCheckpointJson::String(value));
    }
    if let Ok(sequence) = value.downcast::<PyList>() {
        let mut out = Vec::with_capacity(sequence.len());
        for item in sequence {
            out.push(py_bound_to_checkpoint_json(py, &item)?);
        }
        return Ok(GraphCheckpointJson::Array(out));
    }
    if let Ok(tuple) = value.downcast::<PyTuple>() {
        let mut out = Vec::with_capacity(tuple.len());
        for item in tuple {
            out.push(py_bound_to_checkpoint_json(py, &item)?);
        }
        return Ok(GraphCheckpointJson::Array(out));
    }
    if let Ok(dict) = value.downcast::<PyDict>() {
        let mut out = JsonMap::new();
        for (key, item) in dict {
            let key = key
                .extract::<String>()
                .map_err(|_| PyValueError::new_err("checkpoint object keys must be strings"))?;
            if out
                .insert(key, py_bound_to_checkpoint_json(py, &item)?)
                .is_some()
            {
                return Err(PyValueError::new_err("checkpoint object has duplicate key"));
            }
        }
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
    let result = Python::with_gil(|py| {
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
    Python::with_gil(|py| {
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

fn leak_static_str(value: String) -> &'static str {
    Box::leak(value.into_boxed_str())
}

enum PyRestoreRecordedKind {
    State,
    Node {
        callback: Py<PyAny>,
        factory: &'static str,
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
                factory: leak_static_str(factory.unwrap_or_else(|| "node".to_owned())),
            },
        })
    }
}

impl PyRestoreContext {
    fn record(&self, recorded: PyRestoreRecorded) -> PyResult<()> {
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
    ref_: &'static str,
    descriptor: Py<PyAny>,
    pending_fatal: PendingFatal,
}

impl GraphRestoreDescriptor for PyRestoreDescriptorBridge {
    fn ref_(&self) -> &'static str {
        self.ref_
    }

    fn define(&self, ctx: RestoreDefineCtx<'_>) -> GraphRestoreResult<RestoreNodeDefinition> {
        let recorded = Rc::new(RefCell::new(None));
        Python::with_gil(|py| -> GraphRestoreResult<()> {
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
                    recorded: recorded.clone(),
                },
            )
            .map_err(|err| GraphRestoreError::new(err.to_string()))?;
            self.descriptor
                .call_method1(py, "create", (py_ctx,))
                .map_err(|err| GraphRestoreError::new(err.to_string()))?;
            Ok(())
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
                factory: "state",
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

    #[pyo3(signature = (callback, name = None))]
    fn producer(&self, callback: Py<PyAny>, name: Option<String>) -> PyResult<PyNode> {
        raise_pending_fatal(&self.pending_fatal)?;
        let pending_fatal = self.pending_fatal.clone();
        let node = catch_graph_panic(&self.pending_fatal, || {
            let callback_pending_fatal = pending_fatal.clone();
            let node = self.graph.producer_opts(
                move |ctx| {
                    let result = Python::with_gil(|py| callback.call0(py));
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
            .map(|dep| dep.borrow(py).node.erased())
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
                    let result = Python::with_gil(|py| {
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
            .map(|dep| dep.borrow(py).node.erased())
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
                    let result = Python::with_gil(|py| {
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
            .map(|dep| dep.borrow(py).node.erased())
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
                    let result = Python::with_gil(|py| {
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
            .map(|dep| dep.borrow(py).node.erased())
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
                    let result = Python::with_gil(|py| {
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
            .map(|dep| dep.borrow(py).node.erased())
            .collect::<Vec<_>>();
        let pending_fatal = self.pending_fatal.clone();
        let node = catch_graph_panic(&self.pending_fatal, || {
            let callback_pending_fatal = pending_fatal.clone();
            let op = Operator::<PyValue>::new("derived", move |ctx| {
                let result = Python::with_gil(|py| {
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
            .map(|dep| dep.borrow(py).node.erased())
            .collect::<Vec<_>>();
        let pending_fatal = self.pending_fatal.clone();
        let node = catch_graph_panic(&self.pending_fatal, || {
            let callback_pending_fatal = pending_fatal.clone();
            let op = Operator::<PyValue>::new("effect", move |ctx| {
                let result = Python::with_gil(|py| {
                    let args = PyTuple::new(py, dep_args_from_ctx(py, ctx)?)?;
                    callback.call1(py, args)
                });
                if let Err(error) = result {
                    Python::with_gil(|py| {
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
                let result = Python::with_gil(|py| callback.call0(py));
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

    fn observe(&self, callback: Py<PyAny>) -> PyResult<PySubscription> {
        raise_pending_fatal(&self.pending_fatal)?;
        let pending_fatal = self.pending_fatal.clone();
        let observer = catch_graph_panic(&self.pending_fatal, || {
            self.graph.observe().subscribe(move |event| {
                Python::with_gil(|py| {
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

#[pyclass(name = "Node", unsendable)]
struct PyNode {
    node: Node<PyValue>,
    graph_node: Option<GraphNode>,
    pending_fatal: PendingFatal,
}

#[pymethods]
impl PyNode {
    fn set(&self, _py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        catch_graph_panic(&self.pending_fatal, || {
            if let Some(node) = &self.graph_node {
                let value: AnyValue = Rc::new(PyValue::new(value));
                node.down(vec![Message::Data(value)]);
            } else {
                self.node.set(PyValue::new(value));
            }
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn send(&self, _py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        catch_graph_panic(&self.pending_fatal, || {
            let value: AnyValue = Rc::new(PyValue::new(value));
            let msg = vec![Message::Data(value)];
            if let Some(node) = &self.graph_node {
                node.down(msg);
            } else {
                self.node.down(msg);
            }
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn pause(&self, lock_id: String) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        catch_graph_panic(&self.pending_fatal, || {
            self.node.up(vec![Message::Pause(LockId::new(lock_id))]);
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn resume(&self, lock_id: String) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        catch_graph_panic(&self.pending_fatal, || {
            self.node.up(vec![Message::Resume(LockId::new(lock_id))]);
        })?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn invalidate(&self) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
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
        let dep = dep.borrow(py).node.erased();
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
        let dep = dep.borrow(py).node.erased();
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
            .map(|dep| dep.borrow(py).node.erased())
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
        let dep = dep.borrow(py).node.erased();
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
                Python::with_gil(|py| {
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
            ref_: leak_static_str(ref_),
            descriptor: entry,
            pending_fatal: pending_fatal.clone(),
        }));
    }
    Ok(PyRestoreRegistry {
        registry: GraphRestoreRegistry::new(native_entries),
        pending_fatal,
    })
}

#[pyfunction]
fn restore_graph(
    py: Python<'_>,
    checkpoint: Py<PyAny>,
    registry: PyRef<'_, PyRestoreRegistry>,
) -> PyResult<PyGraph> {
    let checkpoint = py_checkpoint_to_native(py, &checkpoint)?;
    let restored = graphrefly_rs::restore_graph(
        checkpoint,
        RestoreGraphOptions::new(registry.registry.clone()),
    )
    .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
    Ok(PyGraph::restored(restored, registry.pending_fatal.clone()))
}

#[pyfunction]
fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    register_py_checkpoint_encoder();
    m.add_class::<PyAsyncCtx>()?;
    m.add_class::<PyCtx>()?;
    m.add_class::<PyGraph>()?;
    m.add_class::<PyNode>()?;
    m.add_class::<PyRestoreContext>()?;
    m.add_class::<PyRestoreRegistry>()?;
    m.add_class::<PySubscription>()?;
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
        Python::with_gil(|py| {
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
