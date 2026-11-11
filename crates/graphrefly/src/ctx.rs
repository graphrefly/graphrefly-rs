//! The fn-body context — the substrate↔user-fn boundary (D8).
//!
//! A node fn receives a [`Ctx`] and communicates **only** through it: `down` is
//! the function's only output (R-invocation). Nothing travels up the graph
//! (R-messages): there is no `up`. Values cross erased as [`AnyValue`]; the typed
//! `data`/`emit` helpers downcast.
//!
//! Slice scope: `down` (DATA/INVALIDATE/terminal; START, PENDING and RESOLVED are the
//! kernel's and are rejected), typed `data`/`emit`, the lifetime's private state and
//! abort signal (R-lifetime@2). A function registers no cleanup: what must last for
//! the lifetime is opened by the node's setup ([`crate::NodeOpts::with_setup`]).
//! Every ctx belongs to one lifetime; output through a closed lifetime's ctx is
//! dropped.

use std::rc::Rc;

use crate::lifetime::AbortSignal;
use crate::node::{Core, Node, NodeOpts, RewireRequest};
use crate::operators::Operator;
use crate::protocol::{reject_kernel_only_output, AnyValue, Message, Wave};

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

/// Internal per-dependency snapshot backing Ctx helpers. Raw ctx exposes only
/// `wave_data` + `terminal` as public dep-input surfaces (R-fn-contract / D77);
/// latest/prev helpers are derived reads over this snapshot and node-owned cache.
#[derive(Clone)]
pub(crate) struct DepRecord {
    /// Which node this dependency is (R-invocation: ctx.deps[i] gives its id).
    pub(crate) dep: crate::node::CoreIdentity,
    /// The arrays this dependency delivered to this invocation, each as its DATA
    /// payloads in order (a RESOLVED array is empty), keeping array boundaries
    /// (R-invocation).
    pub(crate) wave_data: Vec<Vec<AnyValue>>,
    /// Last committed DATA before the current batch; `None` = SENTINEL (never emitted DATA) —
    /// the canonical never-emitted detector (R-sentinel).
    pub(crate) prev_data: Option<AnyValue>,
    /// Latest DATA = last of `batch` if present, else `prev_data`.
    pub(crate) latest: Option<AnyValue>,
    /// The dependency's terminal in this invocation, if any (R-invocation).
    pub(crate) terminal: Option<DepTerminal>,
    /// An INVALIDATE arrived on this input in this invocation; set only under the
    /// manual INVALIDATE policy (R-invalidation).
    pub(crate) invalidated: bool,
}

/// The single argument to a node fn. All emission is explicit via [`Ctx::down`] /
/// [`Ctx::emit`]; there is no return-value framing (R-fn-contract / D8).
pub struct Ctx {
    pub(crate) node: Core,
    dep_records: Vec<DepRecord>,
    lifetime: u64,
    signal: AbortSignal,
}

impl Ctx {
    pub(crate) fn new(
        node: Core,
        dep_records: Vec<DepRecord>,
        lifetime: u64,
        signal: AbortSignal,
    ) -> Self {
        Self {
            node,
            dep_records,
            lifetime,
            signal,
        }
    }

    pub(crate) fn lifetime_id(&self) -> u64 {
        self.lifetime
    }

    fn open(&self) -> bool {
        self.node.lifetime_open(self.lifetime)
    }

    /// The lifetime's abort signal (R-lifetime@2), aborted when the lifetime closes.
    pub fn signal(&self) -> AbortSignal {
        self.signal.clone()
    }

    pub(crate) fn dep_records(&self) -> &[DepRecord] {
        &self.dep_records
    }

    /// The raw input surface (R-invocation): per dependency, the arrays it delivered
    /// to this invocation, each as its DATA payloads in order. A RESOLVED array is
    /// empty; what came before an INVALIDATE is not shown.
    pub fn wave_data(&self) -> Vec<&[Vec<AnyValue>]> {
        self.dep_records
            .iter()
            .map(|record| record.wave_data.as_slice())
            .collect()
    }

    /// Terminal metadata for dep `i`, separate from `wave_data` (R-fn-contract / D77).
    pub fn terminal(&self, i: usize) -> Option<&DepTerminal> {
        self.dep_records.get(i).and_then(|r| r.terminal.as_ref())
    }

    /// Whether dependency `i` of this invocation is `node` (R-invocation: each input
    /// says which dependency it is).
    #[must_use]
    pub fn dep_is(&self, i: usize, node: &Core) -> bool {
        self.dep_records.get(i).is_some_and(|r| r.dep.matches(node))
    }

    /// Whether an INVALIDATE arrived on dependency `i` in this invocation, under the
    /// manual INVALIDATE policy (R-invalidation); always false under the auto policy,
    /// where the kernel handles the INVALIDATE without running the function.
    #[must_use]
    pub fn invalidated(&self, i: usize) -> bool {
        self.dep_records.get(i).is_some_and(|r| r.invalidated)
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
                    .map(|wave| {
                        wave.iter()
                            .filter_map(|v| v.clone().downcast::<U>().ok())
                            .collect()
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Typed DATA payloads dep `i` delivered in this invocation, flattened across
    /// upstream waves. This is a derived helper; raw occurrence shape lives in `wave_data`.
    pub fn batch<U: 'static>(&self, i: usize) -> Vec<Rc<U>> {
        self.batches(i).into_iter().flatten().collect()
    }

    /// Emit one typed DATA downstream — the value-level sugar (R-primary-api-clean).
    /// Consecutive emits of one run join one array, closed by any `down` (Rust sugar;
    /// graphrefly:D797 (9)); the PENDING prefix is the kernel's (R-wave).
    pub fn emit<T: 'static>(&self, v: T) {
        if self.open() {
            self.node.emit_value(Rc::new(v));
        }
    }

    /// Emits each of `values`, as [`Ctx::emit`] does: they join the run's open array.
    pub fn emit_all<T: 'static>(&self, values: impl IntoIterator<Item = T>) {
        if !self.open() {
            return;
        }
        for v in values {
            self.node.emit_value(Rc::new(v));
        }
    }

    /// Emit a raw array downstream toward sinks (the ctx-level power surface, DR-1).
    ///
    /// # Panics
    ///
    /// Panics when `msgs` contains START, PENDING or RESOLVED, which only the kernel
    /// sends (R-subscription, R-wave, R-failure); nothing of the array is applied.
    /// Uncaught, the panic is the node's ERROR.
    pub fn down(&self, msgs: Wave<AnyValue>) {
        reject_kernel_only_output(&msgs);
        if self.open() {
            self.node.down(msgs);
        }
    }

    /// Read the lifetime's private state (R-lifetime@2), typed. It lasts across the
    /// lifetime's waves, INVALIDATE included, and is dropped when the lifetime closes.
    pub fn state_get<S: 'static>(&self) -> Option<Rc<S>> {
        if !self.open() {
            return None;
        }
        self.node.get_state().and_then(|a| a.downcast::<S>().ok())
    }

    /// Set the lifetime's private state (R-lifetime@2); dropped through a closed ctx.
    pub fn state_set<S: 'static>(&self, v: S) {
        if self.open() {
            self.node.set_state(Rc::new(v));
        }
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
        if !self.open() {
            return;
        }
        self.node
            .request_rewire_next(RewireRequest::Add(dep, Rc::new(f)));
    }

    /// Request a deferred self-rewire unsubscribe (R-rewire-deferred): if `dep` is still live at the
    /// boundary, unsubscribe from it + swap the fn. If the identity is already absent at apply time, the
    /// request is a full no-op, including no fn swap, so stale duplicate unsubscribes cannot desync
    /// the live dep/fn pairing. A successful unsubscribe DRAINS the dep's edge + tears down its source
    /// on last-subscriber (its lifetime cleanup) = the switchMap/abortInFlight cancellation + memory
    /// bounding (D47 beta).
    pub fn rewire_next_unsubscribe_dep<F: Fn(&Ctx) + 'static>(&self, dep: Core, f: F) {
        if !self.open() {
            return;
        }
        self.node
            .request_rewire_next(RewireRequest::remove(&dep, Rc::new(f)));
    }

    /// Request a deferred self-rewire replace (R-rewire-deferred): replace the whole dep set + swap
    /// the fn at the boundary (the switch-variant — removes-before-adds keeps the tracked inner
    /// list aligned across boundary waves).
    pub fn rewire_next_replace_deps<F: Fn(&Ctx) + 'static>(&self, deps: Vec<Core>, f: F) {
        if !self.open() {
            return;
        }
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
            lifetime: self.lifetime,
            signal: self.signal.clone(),
        }
    }
}

/// An owned, `'static` late-emit handle obtained via [`Ctx::defer`] — the async-pool
/// boundary. An async node fn stashes one and emits LATER (the deferred result of
/// async work). The emit re-enters the wave engine as a FRESH external wave (its own
/// wave-owner / D30 catch boundary; the leading PENDING is the kernel's, R-wave). Holds the node handle ([`Core`] = an `Rc`) + the
/// dep snapshot at defer time.
pub struct DeferredCtx {
    node: Core,
    dep_records: Vec<DepRecord>,
    lifetime: u64,
    signal: AbortSignal,
}

impl DeferredCtx {
    /// The lifetime's abort signal (R-lifetime@2).
    pub fn signal(&self) -> AbortSignal {
        self.signal.clone()
    }

    /// The raw input surface captured at defer time (R-invocation).
    pub fn wave_data(&self) -> Vec<&[Vec<AnyValue>]> {
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
                    .map(|wave| {
                        wave.iter()
                            .filter_map(|v| v.clone().downcast::<U>().ok())
                            .collect()
                    })
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
        if self.node.lifetime_open(self.lifetime) {
            self.node.owned_down(vec![Message::Data(Rc::new(v))]);
        }
    }

    /// Emit a raw array downstream as the deferred async result (the ctx-level power
    /// surface, DR-1).
    ///
    /// # Panics
    ///
    /// Panics when `msgs` contains PENDING or RESOLVED, which only the kernel sends
    /// (R-wave); nothing of the array is applied.
    pub fn down(&self, msgs: Wave<AnyValue>) {
        reject_kernel_only_output(&msgs);
        // Output through a closed lifetime's ctx is dropped (R-lifetime@2).
        if self.node.lifetime_open(self.lifetime) {
            self.node.owned_down(msgs);
        }
    }
}
