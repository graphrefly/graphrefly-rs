//! The fn-body context — the substrate↔user-fn boundary (D8).
//!
//! A node fn receives a [`Ctx`] and communicates **only** through it: no
//! `actions.emit/forward/backward`, the whole surface unifies on `up`/`down`.
//! Values cross erased as [`AnyValue`]; the typed `data`/`emit` helpers downcast.
//!
//! Slice scope: `down` (DATA/DIRTY/RESOLVED), typed `data`/`emit`, `state`, and
//! the cleanup-hook registration. `up` validates control-only (R-ctx-up) but its
//! INVALIDATE/PAUSE terminus actions are deferred. See `CLEAN-SLATE.md`.

use std::rc::Rc;

use crate::node::Core;
use crate::protocol::{AnyValue, Message, Wave};

/// Per-dependency record visible to a node fn (R-fn-contract). One per declared
/// dep, a snapshot of this wave's view (values erased — downcast via [`Ctx::data`]).
#[derive(Clone)]
pub struct DepRecord {
    /// DATA values this dep delivered in the current wave; `None` = not involved.
    pub batch: Option<Vec<AnyValue>>,
    /// Last DATA from any wave; `None` = SENTINEL (never emitted DATA) — the
    /// canonical never-emitted detector (R-sentinel).
    pub prev_data: Option<AnyValue>,
    /// Latest DATA = last of `batch` if present, else `prev_data`.
    pub latest: Option<AnyValue>,
    /// Tier of this dep's most recent message in the current wave (0 if none).
    pub tier: u8,
}

/// The single argument to a node fn. All emission is explicit via [`Ctx::down`] /
/// [`Ctx::emit`]; there is no return-value framing (R-fn-contract / D8).
pub struct Ctx {
    pub(crate) node: Core,
    dep_records: Vec<DepRecord>,
}

impl Ctx {
    pub(crate) fn new(node: Core, dep_records: Vec<DepRecord>) -> Self {
        Self { node, dep_records }
    }

    /// The per-dep wave snapshot (R-fn-contract). Read erased values positionally.
    pub fn dep_records(&self) -> &[DepRecord] {
        &self.dep_records
    }

    /// Typed read of dep `i`'s latest DATA, downcast to `U` (SENTINEL → `None`).
    /// Returns `Rc<U>` (shared, no value clone). Data flows through messages —
    /// this reads the wave snapshot, never peeks the dep's `.cache` (R-data-not-peek).
    pub fn data<U: 'static>(&self, i: usize) -> Option<Rc<U>> {
        self.dep_records
            .get(i)
            .and_then(|r| r.latest.clone())
            .and_then(|a| a.downcast::<U>().ok())
    }

    /// Typed read of dep `i`'s prior-wave DATA (the SENTINEL-aware never-emitted
    /// detector reads `prev_data.is_none()`).
    pub fn prev<U: 'static>(&self, i: usize) -> Option<Rc<U>> {
        self.dep_records
            .get(i)
            .and_then(|r| r.prev_data.clone())
            .and_then(|a| a.downcast::<U>().ok())
    }

    /// Emit one typed DATA downstream — the value-level sugar (R-primary-api-clean):
    /// the protocol DIRTY-before-DATA framing is synthesized by the substrate.
    pub fn emit<T: 'static>(&self, v: T) {
        self.node.down(vec![Message::Data(Rc::new(v))]);
    }

    /// Emit a raw wave downstream toward sinks (the ctx-level power surface, DR-1).
    pub fn down(&self, msgs: Wave<AnyValue>) {
        self.node.down(msgs);
    }

    /// Emit upstream toward deps — control tiers only (R-ctx-up). Validates the
    /// kind; the terminus actions (PAUSE lockset / INVALIDATE honor) are deferred.
    pub fn up(&self, msgs: Wave<AnyValue>) {
        self.node.up(msgs);
    }

    /// Read this node's private cross-wave state (R-ctx-state / D23), typed.
    pub fn state_get<S: 'static>(&self) -> Option<Rc<S>> {
        self.node.get_state().and_then(|a| a.downcast::<S>().ok())
    }

    /// Set this node's private cross-wave state (R-ctx-state).
    pub fn state_set<S: 'static>(&self, v: S) {
        self.node.set_state(Rc::new(v));
    }

    /// Keep `state` across the fresh-lifecycle wipe (R-ctx-state / D29).
    pub fn state_persist(&self, on: bool) {
        self.node.set_state_persist(on);
    }

    /// Release external resources on deactivation (R-cleanup-hooks / D28).
    pub fn on_deactivation(&self, f: impl FnOnce() + 'static) {
        self.node.register_on_deactivation(Box::new(f));
    }
}
