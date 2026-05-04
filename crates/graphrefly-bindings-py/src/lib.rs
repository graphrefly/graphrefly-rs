//! pyo3 Python bindings for `GraphReFly`.
//!
//! Exposes the Rust core via `PyO3`. The published `PyPI` distribution
//! (`graphrefly`, `graphrefly[full]`, etc.) is built from this crate
//! using maturin. The `abi3-py39` feature means a single wheel works
//! on Python 3.9+; only one wheel per platform is published.
//!
//! Free-threaded Python (3.13+ no-GIL builds) is supported by
//! construction: the Rust core's `Arc<RwLock<...>>` and
//! `parking_lot::ReentrantMutex` provide all the thread-safety the
//! Python standard library doesn't. `PyO3` calls back into Python under
//! the GIL (or, in free-threaded mode, in true parallel) only when
//! invoking user fns.
//!
//! # Status
//!
//! Scaffold. Bindings land during M6 of the Rust port (Python parity
//! milestone). Today's `graphrefly-py` repo retires once feature parity
//! is reached.

#![warn(rust_2018_idioms, unreachable_pub)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::missing_errors_doc)]

use pyo3::prelude::*;

/// Smoke export. Verifies the pyo3 + maturin build chain works end-to-end.
/// Will be replaced by real bindings during M6.
#[pyfunction]
fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[pymodule]
fn graphrefly(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(version, m)?)?;
    Ok(())
}
