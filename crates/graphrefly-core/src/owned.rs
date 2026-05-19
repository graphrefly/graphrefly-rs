//! [`OwnedCore`] — the one canonical Core-ownership keystone (D246 rule 4).
//!
//! The actor-model [`Core`] is move-only and single-owner (D221/D223).
//! Every embedder — the napi/pyo3/wasm bindings, every test harness, an
//! application embedding GraphReFly — needs the *same* three things:
//!
//! 1. **own** the relocatable `Core` by value,
//! 2. **track** the subscriptions it opens, and
//! 3. **tear them down on the owner thread** when it goes away.
//!
//! Before D246 this keystone was triplicated as `TestRuntime` /
//! `StructuresRuntime` / the operators harness runtime (plus the napi
//! `CURRENT_CORE` hack). `OwnedCore` is the single abstraction they all
//! compose. RAII teardown lives **here, at the embedder boundary** —
//! never below it (D246 rule 3: `subscribe → id` + `core.unsubscribe`;
//! no parameterless `Drop` reaching Core anywhere in the substrate). The
//! `Drop` here is sound because `OwnedCore` *owns* the `Core` by value:
//! it is on the dropping thread's stack, alive, no relocation, no
//! `Weak<C>` (this is exactly the D225 owner-invoked-synchronous-
//! unsubscribe shape, not the retired core RAII).
//!
//! Producer build/teardown (D231/D245) is just "the owner calls
//! `owned.core()` synchronously" — `OwnedCore` *is* the canonical
//! producer-build site; no per-binding "how do I reach Core" remains.

use std::sync::{Arc, Mutex, PoisonError};

use crate::boundary::BindingBoundary;
use crate::handle::NodeId;
use crate::node::{Core, Sink, SubscriptionId};
use crate::state_cell::{LockedCell, StateCell};
use crate::topology::{TopologySink, TopologySubscriptionId};

/// Owns a [`Core`], tracks the subscriptions opened through it, and
/// tears them down on the owner thread in `Drop`.
///
/// Generic over the state cell `C` (default [`LockedCell`]) so it
/// serves every substrate consumer uniformly. Construct with
/// [`OwnedCore::new`] (fresh `Core`) or [`OwnedCore::with_core`] (adopt
/// a `Core` the caller already built).
pub struct OwnedCore<C: StateCell = LockedCell> {
    core: Core<C>,
    subs: Mutex<Vec<(NodeId, SubscriptionId)>>,
    topo_subs: Mutex<Vec<TopologySubscriptionId>>,
}

impl OwnedCore<LockedCell> {
    /// Build a fresh `Core` wired to `binding` and own it.
    #[must_use]
    pub fn new(binding: Arc<dyn BindingBoundary>) -> Self {
        Self::with_core(Core::new(binding))
    }
}

impl<C: StateCell> OwnedCore<C> {
    /// Adopt a `Core` the caller already constructed (binding crates
    /// that build `Core` with a non-default cell).
    #[must_use]
    pub fn with_core(core: Core<C>) -> Self {
        Self {
            core,
            subs: Mutex::new(Vec::new()),
            topo_subs: Mutex::new(Vec::new()),
        }
    }

    /// Borrow the owned dispatcher. This is the D231 owner-side `&Core`
    /// — pass it explicitly into every Core-touching op (graph,
    /// structures, storage, producer build). Never store or clone it.
    #[must_use]
    #[inline]
    pub fn core(&self) -> &Core<C> {
        &self.core
    }

    /// The binding this `Core` was wired with.
    #[must_use]
    #[inline]
    pub fn binding(&self) -> Arc<dyn BindingBoundary> {
        self.core.binding()
    }

    /// Subscribe a sink and track it for owner-thread teardown on drop.
    /// Returns the [`SubscriptionId`] for an explicit early
    /// [`Self::unsubscribe`] (D246 rule 3 — owner-invoked, synchronous;
    /// no RAII below the binding).
    pub fn track_subscribe(&self, node_id: NodeId, sink: Sink) -> SubscriptionId {
        let sub_id = self.core.subscribe(node_id, sink);
        self.subs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((node_id, sub_id));
        sub_id
    }

    /// Subscribe a topology sink and track it for teardown on drop.
    pub fn track_subscribe_topology(&self, sink: TopologySink) -> TopologySubscriptionId {
        let id = self.core.subscribe_topology(sink);
        self.topo_subs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(id);
        id
    }

    /// Explicit early unsubscribe (owner-invoked, synchronous — D225/
    /// D241). Idempotent: a later `Drop` won't double-unsubscribe a
    /// detached id (and core unsubscribe is itself idempotent on
    /// monotonic never-recycled ids).
    pub fn unsubscribe(&self, node_id: NodeId, sub_id: SubscriptionId) {
        self.subs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|&(n, s)| !(n == node_id && s == sub_id));
        self.core.unsubscribe(node_id, sub_id);
    }

    /// Explicit early topology unsubscribe (owner-invoked, synchronous).
    pub fn unsubscribe_topology(&self, id: TopologySubscriptionId) {
        self.topo_subs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|&i| i != id);
        self.core.unsubscribe_topology(id);
    }
}

impl<C: StateCell> Drop for OwnedCore<C> {
    fn drop(&mut self) {
        // Owner-thread, `Core` owned-by-value-and-alive: synchronous
        // unsubscribe is sound (D225 owner-invoked shape, NOT the
        // retired relocating-Core RAII). Topology subs first so a
        // topo fire mid-teardown can't re-enter a half-removed sink.
        //
        // QA-A2: pop-one-at-a-time (re-lock per item, lock released
        // across `core.unsubscribe`) rather than take-then-iterate.
        // `Core::unsubscribe` runs the lock-released deactivation
        // chain (`OnDeactivation` cleanup, `producer_deactivate`,
        // `wipe_ctx`) which can fire sinks synchronously and re-enter
        // this same `OwnedCore` via `track_subscribe*`. A drained
        // snapshot would silently drop any sub registered during
        // teardown; the pop-loop observes those and tears them down
        // too (the lock is never held across `core.unsubscribe`, so
        // no self-deadlock). This is the single canonical teardown
        // keystone — re-entrancy-safety matters more than the
        // micro-cost of per-item re-locking on a cold drop path.
        while let Some(id) = pop(&self.topo_subs) {
            self.core.unsubscribe_topology(id);
        }
        while let Some((node_id, sub_id)) = pop(&self.subs) {
            self.core.unsubscribe(node_id, sub_id);
        }
    }
}

#[inline]
fn pop<T>(m: &Mutex<Vec<T>>) -> Option<T> {
    m.lock().unwrap_or_else(PoisonError::into_inner).pop()
}

// QA-A4: lock the D246 invariant — `OwnedCore` (default cell) must stay
// `Send + Sync + 'static` so embedders/bindings can hold it across the
// host executor. A future non-Send/Sync field fails the build here, at
// the cause, not far downstream.
const _: fn() = || {
    fn assert_send_sync_static<T: Send + Sync + 'static>() {}
    assert_send_sync_static::<OwnedCore<LockedCell>>();
};
