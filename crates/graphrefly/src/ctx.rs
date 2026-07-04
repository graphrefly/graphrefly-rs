//! The fn-body context — the substrate↔user-fn boundary (D8).
//!
//! A node fn receives a [`Ctx`] and communicates **only** through it: no
//! `actions.emit/forward/backward`, the whole surface unifies on `up`/`down`.
//! Values cross erased as [`AnyValue`]; the typed `data`/`emit` helpers downcast.
//!
//! Slice scope: `down` (DATA/DIRTY/RESOLVED/INVALIDATE/terminal), typed `data`/
//! `emit`, `state`, and the cleanup hooks (`on_deactivation` + `on_invalidate`).
//! `up` validates control/demand-only (R-ctx-up) and self-handles PAUSE/RESUME/PULL; the
//! INVALIDATE-at-depless-source terminus (D38, C-7) is deferred. See `CLEAN-SLATE.md`.

use std::rc::Rc;

use crate::node::{Core, Node, NodeOpts, RewireRequest};
use crate::operators::Operator;
use crate::protocol::{AnyValue, Message, PullDemand, Wave};

/// A dep's terminal state, visible to the fn via [`Ctx::terminal`]
/// (R-deps-terminal / C-15). `None` ⇔ the dep is live; `Some(..)` ⇔ it COMPLETEd
/// or ERRORed. Read by `terminalAsRealInput` operators (rescue/reduce/*Map) that
/// treat an inner terminal as a real input rather than an absorbed settle.
///
/// `Error` carries the dep's error **message** (an `Rc<str>`), not the original
/// `GraphError`: `Box<dyn Error>` is not `Clone`, and a message crosses a dep
/// subscription as a borrow (`&Message`), so the concrete error type cannot be
/// preserved into this per-wave-cloneable record. Per-language (D24, D31
/// error=unknown — the top type); the message survives, the downcast does not.
#[derive(Clone)]
pub enum DepTerminal {
    /// The dep emitted COMPLETE.
    Complete,
    /// The dep emitted ERROR; carries its `Display` message (see type doc).
    Error(Rc<str>),
}

/// One raw ctx wave-data projection item (R-ctx-wave-data / D77).
#[derive(Clone)]
pub enum WaveData {
    /// A legal DATA payload.
    Data(AnyValue),
    /// The protocol SENTINEL marker used for INVALIDATE inside `ctx.wave_data`.
    Sentinel,
}

impl WaveData {
    /// Typed read of a DATA payload; returns `None` for SENTINEL or type mismatch.
    pub fn data<U: 'static>(&self) -> Option<Rc<U>> {
        match self {
            Self::Data(v) => v.clone().downcast::<U>().ok(),
            Self::Sentinel => None,
        }
    }

    /// True iff this item is the protocol SENTINEL marker.
    pub fn is_sentinel(&self) -> bool {
        matches!(self, Self::Sentinel)
    }
}

/// Internal per-dependency snapshot backing Ctx helpers. Raw ctx exposes only
/// `wave_data` + `terminal` as public dep-input surfaces (R-fn-contract / D77);
/// latest/prev helpers are derived reads over this snapshot and node-owned cache.
#[derive(Clone)]
pub(crate) struct DepRecord {
    /// Per-upstream-wave projections delivered to this fn invocation.
    pub(crate) wave_data: Vec<Vec<WaveData>>,
    /// Last committed DATA before the current batch; `None` = SENTINEL (never emitted DATA) —
    /// the canonical never-emitted detector (R-sentinel).
    pub(crate) prev_data: Option<AnyValue>,
    /// Latest DATA = last of `batch` if present, else `prev_data`.
    pub(crate) latest: Option<AnyValue>,
    /// The dep's terminal state (R-deps-terminal). `None` ⇔ live. A
    /// `terminalAsRealInput` fn reads this to react to an inner COMPLETE/ERROR.
    pub(crate) terminal: Option<DepTerminal>,
}

/// The single argument to a node fn. All emission is explicit via [`Ctx::down`] /
/// [`Ctx::emit`]; there is no return-value framing (R-fn-contract / D8).
pub struct Ctx {
    pub(crate) node: Core,
    dep_records: Vec<DepRecord>,
    pull: Option<PullDemand>,
}

impl Ctx {
    pub(crate) fn new(node: Core, dep_records: Vec<DepRecord>, pull: Option<PullDemand>) -> Self {
        Self {
            node,
            dep_records,
            pull,
        }
    }

    pub(crate) fn dep_records(&self) -> &[DepRecord] {
        &self.dep_records
    }

    /// Raw dep-value input surface (R-ctx-wave-data / D77): dep -> waves -> values/SENTINEL.
    pub fn wave_data(&self) -> Vec<&[Vec<WaveData>]> {
        self.dep_records
            .iter()
            .map(|record| record.wave_data.as_slice())
            .collect()
    }

    /// Terminal metadata for dep `i`, separate from `wave_data` (R-fn-contract / D77).
    pub fn terminal(&self, i: usize) -> Option<&DepTerminal> {
        self.dep_records.get(i).and_then(|r| r.terminal.as_ref())
    }

    /// Number of declared deps in this fn invocation.
    pub fn dep_len(&self) -> usize {
        self.dep_records.len()
    }

    /// True iff no deps are declared.
    pub fn deps_empty(&self) -> bool {
        self.dep_records.is_empty()
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

    /// Typed read of dep `i`'s accumulated DATA grouped by upstream wave.
    pub fn batches<U: 'static>(&self, i: usize) -> Vec<Vec<Rc<U>>> {
        self.dep_records
            .get(i)
            .map(|r| {
                r.wave_data
                    .iter()
                    .map(|wave| wave.iter().filter_map(WaveData::data::<U>).collect())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Typed DATA payloads dep `i` delivered in this invocation, flattened across
    /// upstream waves. This is a derived helper; raw occurrence shape lives in `wave_data`.
    pub fn batch<U: 'static>(&self, i: usize) -> Vec<Rc<U>> {
        self.batches(i).into_iter().flatten().collect()
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
        self.node.up(msgs, None);
    }

    /// Directed upstream control along one declared dep edge (R-up-routing).
    pub fn up_toward(&self, toward_dep: usize, msgs: Wave<AnyValue>) {
        self.node.up(msgs, Some(toward_dep));
    }

    /// Defer an upstream control wave to the committed wave boundary (R-rewire-deferred).
    /// This is the self-demand path for pull nodes: `PULL({pullId, params?})` routes
    /// after the current fn settles, avoiding D37 mid-wave re-entry.
    pub fn up_next(&self, msgs: Wave<AnyValue>) {
        self.node.request_up_next(msgs, None);
    }

    /// Directed form of [`Ctx::up_next`].
    pub fn up_next_toward(&self, toward_dep: usize, msgs: Wave<AnyValue>) {
        self.node.request_up_next(msgs, Some(toward_dep));
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

    /// Holder-visible context for a PULL-caused invocation (D272). Absent for
    /// normal dep-settle, activation, pause replay, terminal, and non-pull runs.
    pub fn pull(&self) -> Option<&PullDemand> {
        self.pull.as_ref()
    }

    /// Release external resources on deactivation (R-cleanup-hooks / D28). Fires once.
    pub fn on_deactivation(&self, f: impl FnOnce() + 'static) {
        self.node.register_on_deactivation(Box::new(f));
    }

    /// Flush external state on INVALIDATE (R-cleanup-hooks / D28). Re-callable: fires
    /// once per INVALIDATE wave. INVALIDATE is lifecycle-continue (R-ctx-state / D29) —
    /// it does NOT wipe `ctx.state`, so any internal reset must be done here.
    pub fn on_invalidate(&self, f: impl Fn() + 'static) {
        self.node.register_on_invalidate(Rc::new(f));
    }

    /// Graph-local async/time driver for source bodies (D111).
    pub(crate) fn local_async_driver(
        &self,
    ) -> Option<Rc<dyn crate::async_driver::LocalAsyncDriver>> {
        self.node.local_async_driver()
    }

    #[cfg(feature = "tokio-worker")]
    pub(crate) fn dispatcher(&self) -> crate::dispatcher::Dispatcher {
        self.node.dispatcher()
    }

    pub(crate) fn environment(&self) -> crate::environment::EnvironmentDrivers {
        self.node.environment()
    }

    pub(crate) fn init_node_in_scope<T: 'static>(
        &self,
        op: Operator<T>,
        deps: Vec<Core>,
    ) -> Node<T> {
        let node = crate::operators::init_node_in_arena_with_dispatcher(
            op,
            &self.node.arena(),
            self.node.dispatcher(),
            deps,
            NodeOpts::default(),
        );
        node.erased().set_environment(self.node.environment());
        node
    }

    /// Request a deferred self-rewire subscribe at the committed wave boundary (R-rewire-deferred /
    /// D47): subscribe to `dep` (and swap the fn) AFTER the current wave settles — never in place, so
    /// this is NOT the D37 mid-fn reject. Higher-order operators (*Map) use this to wire
    /// runtime-created inner nodes as VISIBLE self-deps. `f` re-pairs the deps (SD-1 pairing).
    pub fn rewire_next_subscribe_dep<F: Fn(&Ctx) + 'static>(&self, dep: Core, f: F) {
        self.node
            .request_rewire_next(RewireRequest::Add(dep, Rc::new(f)));
    }

    /// Request a deferred self-rewire unsubscribe (R-rewire-deferred): if `dep` is still live at the
    /// boundary, unsubscribe from it + swap the fn. If the identity is already absent at apply time, the
    /// request is a full no-op, including no fn swap, so stale duplicate unsubscribes cannot desync
    /// the live dep/fn pairing. A successful unsubscribe DRAINS the dep's edge + tears down its source
    /// on last-subscriber (onDeactivation) = the switchMap/abortInFlight cancellation + memory
    /// bounding (D47 beta).
    pub fn rewire_next_unsubscribe_dep<F: Fn(&Ctx) + 'static>(&self, dep: Core, f: F) {
        self.node
            .request_rewire_next(RewireRequest::remove(&dep, Rc::new(f)));
    }

    /// Request a deferred self-rewire replace (R-rewire-deferred): replace the whole dep set + swap
    /// the fn at the boundary (the switch-variant — removes-before-adds keeps the tracked inner
    /// list aligned across boundary waves).
    pub fn rewire_next_replace_deps<F: Fn(&Ctx) + 'static>(&self, deps: Vec<Core>, f: F) {
        self.node
            .request_rewire_next(RewireRequest::Set(deps, Rc::new(f)));
    }

    /// Obtain an owned, `'static` deferred-emit handle for **async-pool** late emission
    /// (R-sync-core / R-no-raw-async: async lives in the fn body; the emit serializes
    /// back onto the single thread). The Rust analogue of the TS async fn capturing its
    /// `ctx` — here `&Ctx` is a borrow that cannot outlive `invoke`, so an async fn must
    /// stash this owned handle and call [`DeferredCtx::emit`] / [`DeferredCtx::down`]
    /// later. Carries the wave's dep snapshot so a deferred read sees this wave's view
    /// (matches the TS per-invocation async ctx snapshot).
    pub fn defer(&self) -> DeferredCtx {
        DeferredCtx {
            node: self.node.clone(),
            dep_records: self.dep_records.clone(),
        }
    }
}

/// An owned, `'static` late-emit handle obtained via [`Ctx::defer`] — the async-pool
/// boundary. An async node fn stashes one and emits LATER (the deferred result of
/// async work). The emit re-enters the wave engine as a FRESH external wave (its own
/// wave-owner / D30 catch boundary; the leading DIRTY is synthesized like any external
/// tier-3 emit, R-dirty-before-data). Holds the node handle ([`Core`] = an `Rc`) + the
/// dep snapshot at defer time.
pub struct DeferredCtx {
    node: Core,
    dep_records: Vec<DepRecord>,
}

impl DeferredCtx {
    /// Raw dep-value input surface captured at defer time (R-ctx-wave-data / D77).
    pub fn wave_data(&self) -> Vec<&[Vec<WaveData>]> {
        self.dep_records
            .iter()
            .map(|record| record.wave_data.as_slice())
            .collect()
    }

    /// Terminal metadata for dep `i`, separate from captured `wave_data`.
    pub fn terminal(&self, i: usize) -> Option<&DepTerminal> {
        self.dep_records.get(i).and_then(|r| r.terminal.as_ref())
    }

    /// Typed read of dep `i`'s latest DATA at defer time (SENTINEL → `None`).
    pub fn data<U: 'static>(&self, i: usize) -> Option<Rc<U>> {
        self.dep_records
            .get(i)
            .and_then(|r| r.latest.clone())
            .and_then(|a| a.downcast::<U>().ok())
    }

    /// Typed read of dep `i`'s captured DATA grouped by upstream wave.
    pub fn batches<U: 'static>(&self, i: usize) -> Vec<Vec<Rc<U>>> {
        self.dep_records
            .get(i)
            .map(|r| {
                r.wave_data
                    .iter()
                    .map(|wave| wave.iter().filter_map(WaveData::data::<U>).collect())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Typed DATA payloads dep `i` delivered in the captured invocation, flattened
    /// across upstream waves.
    pub fn batch<U: 'static>(&self, i: usize) -> Vec<Rc<U>> {
        self.batches(i).into_iter().flatten().collect()
    }

    /// Emit one typed DATA downstream as the deferred async result.
    pub fn emit<T: 'static>(&self, v: T) {
        self.node.owned_down(vec![Message::Data(Rc::new(v))]);
    }

    /// Emit a raw wave downstream as the deferred async result (the ctx-level power
    /// surface, DR-1).
    pub fn down(&self, msgs: Wave<AnyValue>) {
        self.node.owned_down(msgs);
    }

    /// Emit upstream through the node's LIVE topology at emission time (R-rewire-async-live-edge).
    pub fn up(&self, msgs: Wave<AnyValue>) {
        self.node.owned_up(msgs, None);
    }

    /// Directed form of [`DeferredCtx::up`].
    pub fn up_toward(&self, toward_dep: usize, msgs: Wave<AnyValue>) {
        self.node.owned_up(msgs, Some(toward_dep));
    }
}
