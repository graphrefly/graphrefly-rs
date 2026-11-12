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
use crate::node::{Core, LifetimeRewire, Node, NodeOpts};
use crate::operators::Operator;
use crate::protocol::{reject_kernel_only_output, AnyValue, Message, Wave};

/// A dep's terminal state, visible to the fn via [`DepView::terminal`]
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
    pub(crate) dep: crate::node::NodeIdentity,
    pub(crate) id: String,
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

/// One dependency's input to an invocation (R-invocation@2: `ctx.deps[i]`), from
/// [`Ctx::dep`] or [`DeferredCtx::dep`]: which dependency it is, the arrays it
/// delivered to this invocation, its terminal, its latest value and whether an
/// INVALIDATE arrived on it. This is the function's only per-dependency read.
#[derive(Clone, Copy)]
pub struct DepView<'a> {
    record: &'a DepRecord,
}

impl<'a> DepView<'a> {
    /// Which dependency this is.
    #[must_use]
    pub fn id(&self) -> &'a str {
        &self.record.id
    }

    /// Whether this dependency is `node`.
    #[must_use]
    pub fn is(&self, node: &Core) -> bool {
        self.record.dep.matches(node)
    }

    /// The arrays this dependency delivered to this invocation, each as its DATA
    /// payloads in order: a RESOLVED array is empty, and what came before an
    /// INVALIDATE is not shown.
    #[must_use]
    pub fn raw_waves(&self) -> &'a [Vec<AnyValue>] {
        &self.record.wave_data
    }

    /// [`DepView::raw_waves`], typed: each array's DATA payloads that are a `U`.
    #[must_use]
    pub fn waves<U: 'static>(&self) -> Vec<Vec<Rc<U>>> {
        self.record
            .wave_data
            .iter()
            .map(|wave| {
                wave.iter()
                    .filter_map(|v| v.clone().downcast::<U>().ok())
                    .collect()
            })
            .collect()
    }

    /// Every DATA payload of this invocation's arrays, in order, typed.
    #[must_use]
    pub fn values<U: 'static>(&self) -> Vec<Rc<U>> {
        self.waves().into_iter().flatten().collect()
    }

    /// The dependency's latest value: the last DATA it delivered, in this invocation
    /// or before; `None` if it holds none (or it is not a `U`).
    #[must_use]
    pub fn value<U: 'static>(&self) -> Option<Rc<U>> {
        self.record
            .latest
            .clone()
            .and_then(|a| a.downcast::<U>().ok())
    }

    /// Whether the dependency holds a value.
    #[must_use]
    pub fn has(&self) -> bool {
        self.record.latest.is_some()
    }

    /// The dependency's terminal, if it has ended.
    #[must_use]
    pub fn terminal(&self) -> Option<&'a DepTerminal> {
        self.record.terminal.as_ref()
    }

    /// Whether an INVALIDATE arrived on this input in this invocation, under the
    /// manual INVALIDATE policy (R-invalidation); always false under the auto
    /// policy, where the kernel handles the INVALIDATE without running the function.
    #[must_use]
    pub fn invalidated(&self) -> bool {
        self.record.invalidated
    }
}

/// The view of dependency `i` of `records`.
#[track_caller]
fn dep_view(records: &[DepRecord], i: usize) -> DepView<'_> {
    let n = records.len();
    DepView {
        record: records
            .get(i)
            .unwrap_or_else(|| panic!("ctx.dep({i}): the node has {n} dependencies")),
    }
}

/// The single argument to a node fn. All emission is explicit via [`Ctx::down`] /
/// [`Ctx::emit`]; there is no return-value framing (R-fn-contract / D8).
pub struct Ctx {
    pub(crate) node: Core,
    dep_records: Vec<DepRecord>,
    lifetime: u64,
    signal: AbortSignal,
    pub(crate) seed: Option<AnyValue>,
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
            seed: None,
        }
    }

    /// The seed's note on a seeded instance's first run, typed; `None` on every
    /// other run (R-invocation@2, R-subscription@3). A restored node's note holds what
    /// it had received, as the checkpoint's `received` records.
    pub fn seed<S: 'static>(&self) -> Option<Rc<S>> {
        self.seed.clone().and_then(|note| note.downcast::<S>().ok())
    }

    /// This ctx with `dep_records` in place of its own: the same node, lifetime and
    /// note (a restored node's first run sees only its changed dependencies).
    pub(crate) fn with_dep_records(&self, dep_records: Vec<DepRecord>) -> Ctx {
        Ctx {
            node: self.node.clone(),
            dep_records,
            lifetime: self.lifetime,
            signal: self.signal.clone(),
            seed: self.seed.clone(),
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

    /// Dependency `i`'s input to this invocation (R-invocation@2).
    ///
    /// # Panics
    ///
    /// Panics when `i` is not one of the node's dependencies.
    #[track_caller]
    #[must_use]
    pub fn dep(&self, i: usize) -> DepView<'_> {
        dep_view(&self.dep_records, i)
    }

    /// Dependency `i`'s input, or `None` past the current dependencies: for nodes
    /// that rewire themselves, whose dependency count changes between runs.
    pub(crate) fn dep_if(&self, i: usize) -> Option<DepView<'_>> {
        self.dep_records.get(i).map(|record| DepView { record })
    }

    /// Every dependency's input to this invocation, in order.
    pub fn deps(&self) -> impl Iterator<Item = DepView<'_>> {
        self.dep_records.iter().map(|record| DepView { record })
    }

    /// Number of declared deps in this fn invocation.
    pub fn dep_len(&self) -> usize {
        self.dep_records.len()
    }

    /// True iff no deps are declared.
    pub fn deps_empty(&self) -> bool {
        self.dep_records.is_empty()
    }

    /// Emit one typed DATA downstream as one array of its own, the value-level sugar
    /// (R-primary-api-clean): `ctx.emit(v)` is `ctx.down(vec![Message::Data(v)])`. Each
    /// call is its own array (R-wave-boundary); the PENDING prefix is the kernel's.
    pub fn emit<T: 'static>(&self, v: T) {
        self.down(vec![Message::Data(Rc::new(v) as AnyValue)]);
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

    /// Changes this node's own dependencies for the current lifetime
    /// (R-request-order@2), keeping its instance, values, private state, function and
    /// subscribers. A request: it runs after the wave, and is dropped if the lifetime
    /// has closed by then. Kept dependencies keep their records, removed ones are
    /// released silently, added ones are acquired; if an added one has a snapshot, the
    /// snapshots are the function's input as one wave. When the lifetime closes, the
    /// declared dependencies come back.
    ///
    /// # Panics
    ///
    /// Panics for a duplicate dependency, another graph's node, the node itself, or a
    /// node that depends on it through current or declared dependencies.
    pub fn rewire(&self, deps: Vec<Core>) {
        if !self.open() {
            return;
        }
        self.node.check_new_deps("rewire", &deps);
        self.node
            .request_lifetime_rewire(self.lifetime, LifetimeRewire::Set(deps));
    }

    /// [`Ctx::rewire`] adding `dep` to the list current when the rewire runs.
    pub(crate) fn rewire_add(&self, dep: Core) {
        if self.open() {
            self.node
                .request_lifetime_rewire(self.lifetime, LifetimeRewire::Add(dep));
        }
    }

    /// [`Ctx::rewire`] removing `dep` from the list current when the rewire runs; a
    /// no-op if it is absent then.
    pub(crate) fn rewire_remove(&self, dep: Core) {
        if self.open() {
            self.node
                .request_lifetime_rewire(self.lifetime, LifetimeRewire::Remove(dep));
        }
    }

    /// Obtain an owned, `'static` deferred-emit handle for late emission
    /// (R-wave@2 / R-no-raw-async: async lives in the fn body; the emit serializes
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

/// An owned, `'static` late-emit handle obtained via [`Ctx::defer`] — the async
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

    /// The lifetime's private state, typed; `None` once the lifetime has closed.
    pub fn state_get<S: 'static>(&self) -> Option<Rc<S>> {
        if !self.node.lifetime_open(self.lifetime) {
            return None;
        }
        self.node.get_state().and_then(|a| a.downcast::<S>().ok())
    }

    /// Sets the lifetime's private state; dropped once the lifetime has closed.
    pub fn state_set<S: 'static>(&self, v: S) {
        if self.node.lifetime_open(self.lifetime) {
            self.node.set_state(Rc::new(v));
        }
    }

    /// [`Ctx::rewire`] through this kept ctx: a request scoped to its lifetime, dropped
    /// if the lifetime has closed.
    ///
    /// # Panics
    ///
    /// Panics as [`Ctx::rewire`] does.
    pub fn rewire(&self, deps: Vec<Core>) {
        if !self.node.lifetime_open(self.lifetime) {
            return;
        }
        self.node.check_new_deps("rewire", &deps);
        self.node
            .request_lifetime_rewire(self.lifetime, LifetimeRewire::Set(deps));
    }

    /// Dependency `i`'s input to the invocation this ctx was kept from.
    ///
    /// # Panics
    ///
    /// Panics when `i` is not one of the node's dependencies.
    #[track_caller]
    #[must_use]
    pub fn dep(&self, i: usize) -> DepView<'_> {
        dep_view(&self.dep_records, i)
    }

    /// Emit one typed DATA downstream as the deferred async result.
    pub fn emit<T: 'static>(&self, v: T) {
        if self.node.lifetime_open(self.lifetime) {
            self.node
                .owned_down_from(self.lifetime, vec![Message::Data(Rc::new(v))]);
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
            self.node.owned_down_from(self.lifetime, msgs);
        }
    }
}
