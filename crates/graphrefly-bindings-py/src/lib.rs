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
    clippy::needless_pass_by_value
)]

use std::cell::RefCell;
use std::error::Error;
use std::fmt;
use std::panic::{self, catch_unwind, AssertUnwindSafe};
use std::rc::Rc;
use std::sync::{Mutex, OnceLock};

use graphrefly_rs::{
    AnyValue, Ctx, DescribeSnapshot, DescribeValue, Graph, GraphNodeOpts, LockId, Message, Node,
    Operator, Status,
};
use pyo3::exceptions::{PyException, PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyTuple};
use pyo3::IntoPyObjectExt;

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

fn value_from_any(py: Python<'_>, value: &AnyValue) -> PyResult<Py<PyAny>> {
    if let Some(value) = value.downcast_ref::<PyValue>() {
        return Ok(value.clone_object(py));
    }
    Err(PyTypeError::new_err(
        "cached value is not owned by the Python binding foundation",
    ))
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

fn dep_args_from_ctx(py: Python<'_>, ctx: &Ctx) -> PyResult<Vec<Py<PyAny>>> {
    let mut values = Vec::with_capacity(ctx.dep_len());
    for index in 0..ctx.dep_len() {
        let Some(value) = ctx.data::<PyValue>(index) else {
            return Err(PyRuntimeError::new_err(
                "dependency DATA is absent at the Python callback boundary",
            ));
        };
        values.push(value.clone_object(py));
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

#[pyclass(name = "Graph", unsendable)]
struct PyGraph {
    graph: Graph,
    pending_fatal: PendingFatal,
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

    #[pyo3(signature = (value, name = None))]
    fn state(&self, _py: Python<'_>, value: Py<PyAny>, name: Option<String>) -> PyResult<PyNode> {
        raise_pending_fatal(&self.pending_fatal)?;
        let value = PyValue::new(value);
        let node = catch_graph_panic(&self.pending_fatal, || PyNode {
            node: self.graph.state_opts(value, graph_node_opts(name)),
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
        };
        if let Err(error) = raise_pending_fatal(&self.pending_fatal) {
            subscription.unsubscribe();
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
    pending_fatal: PendingFatal,
}

#[pymethods]
impl PyNode {
    fn set(&self, _py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        catch_graph_panic(&self.pending_fatal, || self.node.set(PyValue::new(value)))?;
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(())
    }

    fn send(&self, _py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        raise_pending_fatal(&self.pending_fatal)?;
        catch_graph_panic(&self.pending_fatal, || {
            self.node
                .down(vec![Message::Data(Rc::new(PyValue::new(value)))]);
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

    fn cache(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(self.node.cache().map(|value| value.clone_object(py)))
    }

    fn cache_entry(&self, py: Python<'_>) -> PyResult<(bool, Py<PyAny>)> {
        raise_pending_fatal(&self.pending_fatal)?;
        match self.node.cache() {
            Some(value) => Ok((true, value.clone_object(py))),
            None => Ok((false, py.None())),
        }
    }

    fn status(&self) -> PyResult<&'static str> {
        raise_pending_fatal(&self.pending_fatal)?;
        Ok(status_name(self.node.status()))
    }

    fn subscribe(&self, callback: Py<PyAny>) -> PyResult<PySubscription> {
        raise_pending_fatal(&self.pending_fatal)?;
        let pending_fatal = self.pending_fatal.clone();
        let unsubscribe = catch_graph_panic(&self.pending_fatal, || {
            let callback_pending_fatal = pending_fatal.clone();
            self.node.subscribe(move |msg| {
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
            })
        })?;
        if let Err(error) = raise_pending_fatal(&self.pending_fatal) {
            unsubscribe();
            return Err(error);
        }
        Ok(PySubscription {
            unsubscribe: RefCell::new(Some(unsubscribe)),
        })
    }
}

#[pyclass(name = "Subscription", unsendable)]
struct PySubscription {
    unsubscribe: RefCell<Option<Box<dyn FnOnce()>>>,
}

#[pymethods]
impl PySubscription {
    fn unsubscribe(&self) {
        if let Some(unsubscribe) = self.unsubscribe.borrow_mut().take() {
            unsubscribe();
        }
    }
}

impl Drop for PySubscription {
    fn drop(&mut self) {
        if let Some(unsubscribe) = self.unsubscribe.get_mut().take() {
            unsubscribe();
        }
    }
}

#[pyfunction]
fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyGraph>()?;
    m.add_class::<PyNode>()?;
    m.add_class::<PySubscription>()?;
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
