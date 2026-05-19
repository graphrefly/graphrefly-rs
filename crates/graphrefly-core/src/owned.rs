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
//! it is on the dropping thread's stack, alive, no relocation (this is
//! exactly the D225 owner-invoked-synchronous-unsubscribe shape, not the
//! retired core RAII).
//!
//! Producer build/teardown (D231/D245) is just "the owner calls
//! `owned.core()` synchronously" — `OwnedCore` *is* the canonical
//! producer-build site; no per-binding "how do I reach Core" remains.
//!
//! D246/S2c: single-owner ⇒ the sub-tracking vecs are plain `RefCell`
//! (the prior `Mutex` was shared-Core-era legacy — an `OwnedCore` is
//! touched by exactly one thread, the one that owns the `Core`). A
//! re-entrant double-borrow on the cold teardown path panics loudly
//! rather than corrupting state.

use std::cell::RefCell;
use std::sync::Arc;

use crate::boundary::BindingBoundary;
use crate::handle::NodeId;
use crate::node::{Core, Sink, SubscriptionId};
use crate::topology::{TopologySink, TopologySubscriptionId};

/// Owns a [`Core`], tracks the subscriptions opened through it, and
/// tears them down on the owner thread in `Drop`. Construct with
/// [`OwnedCore::new`] (fresh `Core`) or [`OwnedCore::with_core`] (adopt
/// a `Core` the caller already built).
pub struct OwnedCore {
    core: Core,
    subs: RefCell<Vec<(NodeId, SubscriptionId)>>,
    topo_subs: RefCell<Vec<TopologySubscriptionId>>,
}

impl OwnedCore {
    /// Build a fresh `Core` wired to `binding` and own it.
    #[must_use]
    pub fn new(binding: Arc<dyn BindingBoundary>) -> Self {
        Self::with_core(Core::new(binding))
    }

    /// Adopt a `Core` the caller already constructed.
    #[must_use]
    pub fn with_core(core: Core) -> Self {
        Self {
            core,
            subs: RefCell::new(Vec::new()),
            topo_subs: RefCell::new(Vec::new()),
        }
    }

    /// Borrow the owned dispatcher. This is the D231 owner-side `&Core`
    /// — pass it explicitly into every Core-touching op (graph,
    /// structures, storage, producer build). Never store or clone it.
    #[must_use]
    #[inline]
    pub fn core(&self) -> &Core {
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
        self.subs.borrow_mut().push((node_id, sub_id));
        sub_id
    }

    /// Subscribe a topology sink and track it for teardown on drop.
    pub fn track_subscribe_topology(&self, sink: TopologySink) -> TopologySubscriptionId {
        let id = self.core.subscribe_topology(sink);
        self.topo_subs.borrow_mut().push(id);
        id
    }

    /// Explicit early unsubscribe (owner-invoked, synchronous — D225/
    /// D241). Idempotent: a later `Drop` won't double-unsubscribe a
    /// detached id (and core unsubscribe is itself idempotent on
    /// monotonic never-recycled ids).
    pub fn unsubscribe(&self, node_id: NodeId, sub_id: SubscriptionId) {
        self.subs
            .borrow_mut()
            .retain(|&(n, s)| !(n == node_id && s == sub_id));
        self.core.unsubscribe(node_id, sub_id);
    }

    /// Explicit early topology unsubscribe (owner-invoked, synchronous).
    pub fn unsubscribe_topology(&self, id: TopologySubscriptionId) {
        self.topo_subs.borrow_mut().retain(|&i| i != id);
        self.core.unsubscribe_topology(id);
    }
}

impl Drop for OwnedCore {
    fn drop(&mut self) {
        // Owner-thread, `Core` owned-by-value-and-alive: synchronous
        // unsubscribe is sound (D225 owner-invoked shape, NOT the
        // retired relocating-Core RAII). Topology subs first so a
        // topo fire mid-teardown can't re-enter a half-removed sink.
        //
        // QA-A2: pop-one-at-a-time (re-borrow per item, borrow released
        // across `core.unsubscribe`) rather than take-then-iterate.
        // `Core::unsubscribe` runs the lock-released deactivation
        // chain (`OnDeactivation` cleanup, `producer_deactivate`,
        // `wipe_ctx`) which can fire sinks synchronously and re-enter
        // this same `OwnedCore` via `track_subscribe*`. A drained
        // snapshot would silently drop any sub registered during
        // teardown; the pop-loop observes those and tears them down
        // too (the borrow is never held across `core.unsubscribe`, so
        // no double-borrow panic). This is the single canonical
        // teardown keystone — re-entrancy-safety matters more than the
        // micro-cost of per-item re-borrowing on a cold drop path.
        while let Some(id) = pop(&self.topo_subs) {
            self.core.unsubscribe_topology(id);
        }
        while let Some((node_id, sub_id)) = pop(&self.subs) {
            self.core.unsubscribe(node_id, sub_id);
        }
    }
}

#[inline]
fn pop<T>(c: &RefCell<Vec<T>>) -> Option<T> {
    c.borrow_mut().pop()
}

// QA-A4 (deleted D248): the `OwnedCore: Send + Sync` assertion is gone.
// Under D246/S2c/D248 full single-owner the substrate `Sink` /
// `TopologySink` dropped their `Send + Sync` bound (shared-Core-era
// legacy), so `Core` — which owns the subscriber map — is `!Send +
// !Sync`, and therefore so is `OwnedCore`. This is the actor-model
// shape: an `OwnedCore` is constructed, driven, and dropped on exactly
// one thread; the **only** cross-thread bridge is the `Arc<CoreMailbox>`
// (id-only timer `Emit` posts), which stays `Send + Sync` independently
// (asserted in `mailbox.rs`).
