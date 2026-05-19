//! Shared test scaffolding for graphrefly-graph integration tests.
//!
//! Pared-down binding suitable for graph-layer tests that don't need
//! real value-to-handle resolution. Tests that need richer behavior
//! (e.g., dynamic-tracked deps) construct a custom binding inline.
//!
//! D246: `Graph` is Core-free. The embedder owns the `Core` via
//! [`graphrefly_core::OwnedCore`]; every Core-touching `Graph::*` op
//! takes an explicit `&Core` first argument. [`graph`] returns the
//! owned-core runtime alongside a fresh `Graph`; reach `&Core` via
//! [`Rt::core`].

#![allow(dead_code)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use graphrefly_core::{
    BindingBoundary, Core, DepBatch, FnId, FnResult, HandleId, NodeId, OwnedCore,
};
use graphrefly_graph::Graph;

pub struct StubBinding {
    pub retains: AtomicUsize,
}

impl StubBinding {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            retains: AtomicUsize::new(0),
        })
    }
}

impl BindingBoundary for StubBinding {
    fn invoke_fn(&self, _node_id: NodeId, _fn_id: FnId, dep_data: &[DepBatch]) -> FnResult {
        // Identity passthrough: emit the first dep's latest, or noop if no deps.
        dep_data
            .first()
            .map_or(FnResult::Noop { tracked: None }, |db| FnResult::Data {
                handle: db.latest(),
                tracked: None,
            })
    }

    fn custom_equals(&self, _: FnId, a: HandleId, b: HandleId) -> bool {
        a == b
    }

    fn release_handle(&self, _h: HandleId) {
        self.retains.fetch_sub(1, Ordering::Relaxed);
    }

    fn retain_handle(&self, _h: HandleId) {
        self.retains.fetch_add(1, Ordering::Relaxed);
    }
}

pub fn binding() -> Arc<dyn BindingBoundary> {
    StubBinding::new()
}

/// Owned-core test runtime: owns the single `Core` (D246) and exposes
/// `&Core` for threading into Core-touching `Graph::*` ops. `Drop`
/// performs the owner-thread synchronous teardown of tracked subs.
pub struct Rt {
    rt: OwnedCore,
}

impl Rt {
    #[must_use]
    pub fn core(&self) -> &Core {
        self.rt.core()
    }
}

/// Construct a named, empty root `Graph` plus the owned-core runtime
/// that backs it. Replaces the pre-D246 `Graph::new(name, binding)`.
#[must_use]
pub fn graph(name: &str) -> (Rt, Graph) {
    let rt = Rt {
        rt: OwnedCore::new(binding()),
    };
    (rt, Graph::new(name))
}

/// As [`graph`], but with a caller-supplied binding (for tests that
/// need richer `invoke_fn`/serialization behavior).
#[must_use]
pub fn graph_with(name: &str, binding: Arc<dyn BindingBoundary>) -> (Rt, Graph) {
    let rt = Rt {
        rt: OwnedCore::new(binding),
    };
    (rt, Graph::new(name))
}
