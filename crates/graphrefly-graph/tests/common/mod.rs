//! Shared test scaffolding for graphrefly-graph integration tests.
//!
//! Pared-down binding suitable for graph-layer tests that don't need
//! real value-to-handle resolution. Tests that need richer behavior
//! (e.g., dynamic-tracked deps) construct a custom binding inline.

#![allow(dead_code)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use graphrefly_core::{BindingBoundary, FnId, FnResult, HandleId, NodeId};

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
    fn invoke_fn(&self, _node_id: NodeId, _fn_id: FnId, dep_handles: &[HandleId]) -> FnResult {
        // Identity passthrough: emit the first dep, or noop if no deps.
        dep_handles
            .first()
            .map_or(FnResult::Noop { tracked: None }, |&h| FnResult::Data {
                handle: h,
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
