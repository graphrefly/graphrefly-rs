//! Test infrastructure for `graphrefly-operators` integration tests.
//!
//! Mirrors the patterns from `graphrefly-core/tests/common/mod.rs`:
//!
//! - [`TestValue`] — a small enum that's the "T" travelling through the
//!   binding's value registry. Cheap to clone, distinguishable by tests.
//! - [`InnerBinding`] — value registry + closure registry with handle
//!   refcounting. Implements [`BindingBoundary`] **and**
//!   [`OperatorBinding`] — the two-trait shape an operator-aware binding
//!   takes (D015).
//! - [`TestOperatorBinding`] — `Arc<InnerBinding>` newtype that's the
//!   `Arc<dyn OperatorBinding>` operator factories accept.
//! - [`Recorder`] — sink that records `[Message]` calls per fire,
//!   resolving handles to `TestValue` via the binding for ergonomic
//!   assertions.
//!
//! Tests construct a [`OpRuntime`] (`Core` + `TestOperatorBinding`),
//! register state nodes via the runtime, build operator chains via the
//! `transform::*` factories, and assert on recorded events.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use ahash::{AHashMap, AHashSet};
use parking_lot::Mutex;
use smallvec::SmallVec;

use graphrefly_core::{
    BindingBoundary, Core, FnId, HandleId, Message, NodeId, Sink, Subscription, NO_HANDLE,
};
use graphrefly_operators::{
    higher_order::{HigherOrderBinding, ProjectFn},
    producer::{default_producer_deactivate, ProducerBuildFn, ProducerCtx, ProducerStorage},
    OperatorBinding, ProducerBinding,
};

// ---------------------------------------------------------------------
// TestValue — what user code sees.
// ---------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum TestValue {
    Int(i64),
    Str(String),
    Pair(Box<TestValue>, Box<TestValue>),
    Tuple(Vec<TestValue>),
}

impl TestValue {
    pub fn int(self) -> i64 {
        match self {
            TestValue::Int(n) => n,
            other => panic!("expected Int, got {other:?}"),
        }
    }

    pub fn pair(self) -> (TestValue, TestValue) {
        match self {
            TestValue::Pair(a, b) => (*a, *b),
            other => panic!("expected Pair, got {other:?}"),
        }
    }

    pub fn tuple(self) -> Vec<TestValue> {
        match self {
            TestValue::Tuple(vs) => vs,
            other => panic!("expected Tuple, got {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------
// InnerBinding — value registry + closure registry + refcounts.
// ---------------------------------------------------------------------

type ProjectorBox = Box<dyn Fn(HandleId) -> HandleId + Send + Sync>;
type PredicateBox = Box<dyn Fn(HandleId) -> bool + Send + Sync>;
type FolderBox = Box<dyn Fn(HandleId, HandleId) -> HandleId + Send + Sync>;
type EqualsBox = Box<dyn Fn(HandleId, HandleId) -> bool + Send + Sync>;
type PairwiseBox = Box<dyn Fn(HandleId, HandleId) -> HandleId + Send + Sync>;
type PackerBox = Box<dyn Fn(&[HandleId]) -> HandleId + Send + Sync>;

type ProjectorFn = Arc<dyn Fn(HandleId) -> HandleId + Send + Sync>;
type PredicateFn = Arc<dyn Fn(HandleId) -> bool + Send + Sync>;
type FolderFn = Arc<dyn Fn(HandleId, HandleId) -> HandleId + Send + Sync>;
type EqualsFn = Arc<dyn Fn(HandleId, HandleId) -> bool + Send + Sync>;
type PairwiseFn = Arc<dyn Fn(HandleId, HandleId) -> HandleId + Send + Sync>;
type PackerFn = Arc<dyn Fn(&[HandleId]) -> HandleId + Send + Sync>;

type ProducerBuildArc = Arc<dyn Fn(ProducerCtx<'_>) + Send + Sync>;

type ProjectArc = Arc<dyn Fn(HandleId) -> NodeId + Send + Sync>;

pub struct InnerBinding {
    state: Mutex<RegistryState>,
    /// Per-producer-node storage shared with [`ProducerCtx`]. Outside
    /// the main `state` Mutex so the producer's build closure can
    /// access it without nested-locking against `state`.
    producer_storage: ProducerStorage,
    /// Self-Core back-reference, set post-construction via
    /// [`InnerBinding::set_core_ref`]. Required for
    /// [`ProducerCtx::new`] to construct a Core ref. Creates an Arc
    /// cycle (Core → binding → core_ref → Core); broken explicitly
    /// when the test runtime drops via [`OpRuntime::drop`].
    core_ref: Mutex<Option<Core>>,
}

struct RegistryState {
    values: HashMap<HandleId, TestValue>,
    refcount: HashMap<HandleId, u32>,
    by_value: HashMap<TestValue, HandleId>,
    next_handle: u64,
    projectors: HashMap<FnId, ProjectorFn>, // Arc'd for clone+drop-lock pattern
    predicates: HashMap<FnId, PredicateFn>,
    folders: HashMap<FnId, FolderFn>,
    equals: HashMap<FnId, EqualsFn>,
    pairwises: HashMap<FnId, PairwiseFn>,
    packers: HashMap<FnId, PackerFn>,
    projects: HashMap<FnId, ProjectArc>,
    /// Producer build closures, keyed by FnId allocated at register
    /// time. Looked up by `invoke_fn` when a producer node fires.
    producer_builds: HashMap<FnId, ProducerBuildArc>,
    next_fn_id: u64,
}

impl InnerBinding {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(RegistryState {
                values: HashMap::new(),
                refcount: HashMap::new(),
                by_value: HashMap::new(),
                next_handle: 1,
                projectors: HashMap::new(),
                predicates: HashMap::new(),
                folders: HashMap::new(),
                equals: HashMap::new(),
                pairwises: HashMap::new(),
                packers: HashMap::new(),
                projects: HashMap::new(),
                producer_builds: HashMap::new(),
                next_fn_id: 1,
            }),
            producer_storage: Arc::new(parking_lot::Mutex::new(ahash::AHashMap::new())),
            core_ref: Mutex::new(None),
        })
    }

    /// Access the producer-state storage. Used by the
    /// [`ProducerBinding`] impl + by tests that want to assert on the
    /// storage's invariants (e.g. "deactivation cleared the entry").
    pub fn producer_storage(&self) -> &ProducerStorage {
        &self.producer_storage
    }

    /// Set the binding's self-Core back-reference. Called by
    /// [`OpRuntime::new`] after Core construction.
    pub fn set_core_ref(&self, core: Core) {
        *self.core_ref.lock() = Some(core);
    }

    /// Take the Core ref out (breaks the Arc cycle on shutdown).
    /// Called from [`OpRuntime::drop`].
    pub fn take_core_ref(&self) -> Option<Core> {
        self.core_ref.lock().take()
    }

    fn test_core_ref(&self) -> Core {
        self.core_ref.lock().clone().expect(
            "InnerBinding::set_core_ref must be called post-construction for producer dispatch",
        )
    }

    /// Intern a value, returning the (possibly-shared) handle. Bumps
    /// refcount.
    pub fn intern(&self, v: TestValue) -> HandleId {
        let mut s = self.state.lock();
        if let Some(&existing) = s.by_value.get(&v) {
            *s.refcount.entry(existing).or_insert(0) += 1;
            return existing;
        }
        let id = HandleId::new(s.next_handle);
        s.next_handle += 1;
        s.values.insert(id, v.clone());
        s.refcount.insert(id, 1);
        s.by_value.insert(v, id);
        id
    }

    pub fn deref(&self, h: HandleId) -> TestValue {
        let s = self.state.lock();
        s.values
            .get(&h)
            .cloned()
            .unwrap_or_else(|| panic!("dangling handle {h:?}"))
    }

    pub fn refcount_of(&self, h: HandleId) -> u32 {
        let s = self.state.lock();
        s.refcount.get(&h).copied().unwrap_or(0)
    }

    pub fn live_handles(&self) -> usize {
        let s = self.state.lock();
        s.refcount.values().filter(|c| **c > 0).count()
    }

    fn alloc_fn_id(&self, s: &mut RegistryState) -> FnId {
        let id = FnId::new(s.next_fn_id);
        s.next_fn_id += 1;
        id
    }
}

impl BindingBoundary for InnerBinding {
    fn invoke_fn(
        &self,
        node_id: NodeId,
        fn_id: FnId,
        _dep_data: &[graphrefly_core::DepBatch],
    ) -> graphrefly_core::FnResult {
        // Producer dispatch (Slice D, D031): if the FnId is a registered
        // producer build closure, run it with a ProducerCtx. The build
        // closure subscribes to upstream sources via the ctx; emissions
        // come later from sink callbacks re-entering Core. The fn
        // itself returns Noop because there's no immediate emission.
        let build = self.state.lock().producer_builds.get(&fn_id).cloned();
        if let Some(build) = build {
            // Need a Core ref for ProducerCtx::core(). Tests that
            // exercise producers do so via `OpRuntime::set_core_self_ref`
            // which gives the binding a Weak<...> — but for the v1
            // substrate we accept the cycle and clone Core via
            // `OpRuntime::core_arc`. The build closure captured Core
            // at registration time (via the operator factory), so it
            // doesn't need ctx.core(); ctx is just for `subscribe_to`.
            //
            // For ctx.core() to work, we'd need a binding-side Core
            // ref. Operators that use ctx.core() directly aren't in
            // scope for the substrate tests (operators capture Core
            // at factory time). So we panic if a build closure tries
            // ctx.core() — tests must construct a separate ctx with
            // their own Core ref if needed.
            let core_ref = self.test_core_ref();
            let ctx = ProducerCtx::new(node_id, &core_ref, &self.producer_storage);
            build(ctx);
            return graphrefly_core::FnResult::Noop { tracked: None };
        }
        // Operator tests don't go through invoke_fn beyond producer
        // dispatch — fire_operator's path doesn't touch invoke_fn for
        // Operator nodes.
        unreachable!("InnerBinding only supports operator + producer dispatch (got fn_id {fn_id:?} not in registry)")
    }

    fn custom_equals(&self, equals_handle: FnId, a: HandleId, b: HandleId) -> bool {
        let f: EqualsFn = self
            .state
            .lock()
            .equals
            .get(&equals_handle)
            .cloned()
            .expect("equals fn not registered");
        // Lock dropped — closure may re-enter the binding to deref handles.
        f(a, b)
    }

    fn release_handle(&self, h: HandleId) {
        let mut s = self.state.lock();
        let count = s.refcount.entry(h).or_insert(0);
        if *count == 0 {
            return;
        }
        *count -= 1;
        if *count == 0 {
            if let Some(v) = s.values.remove(&h) {
                s.by_value.remove(&v);
            }
        }
    }

    fn retain_handle(&self, h: HandleId) {
        let mut s = self.state.lock();
        *s.refcount.entry(h).or_insert(0) += 1;
    }

    fn project_each(&self, fn_id: FnId, inputs: &[HandleId]) -> SmallVec<[HandleId; 1]> {
        let proj: ProjectorFn = self
            .state
            .lock()
            .projectors
            .get(&fn_id)
            .cloned()
            .expect("projector not registered");
        // Drop the lock; the closure may re-enter the binding via deref/
        // intern, so it must run unlocked.
        inputs.iter().map(|&h| proj(h)).collect()
    }

    fn predicate_each(&self, fn_id: FnId, inputs: &[HandleId]) -> SmallVec<[bool; 4]> {
        let pred: PredicateFn = self
            .state
            .lock()
            .predicates
            .get(&fn_id)
            .cloned()
            .expect("predicate not registered");
        inputs.iter().map(|&h| pred(h)).collect()
    }

    fn fold_each(
        &self,
        fn_id: FnId,
        acc: HandleId,
        inputs: &[HandleId],
    ) -> SmallVec<[HandleId; 1]> {
        let folder: FolderFn = self
            .state
            .lock()
            .folders
            .get(&fn_id)
            .cloned()
            .expect("folder not registered");
        let mut out = SmallVec::with_capacity(inputs.len());
        let mut current = acc;
        for &h in inputs {
            let next = folder(current, h);
            out.push(next);
            current = next;
        }
        out
    }

    fn pairwise_pack(&self, fn_id: FnId, prev: HandleId, current: HandleId) -> HandleId {
        let f: PairwiseFn = self
            .state
            .lock()
            .pairwises
            .get(&fn_id)
            .cloned()
            .expect("pairwise not registered");
        f(prev, current)
    }

    fn pack_tuple(&self, fn_id: FnId, handles: &[HandleId]) -> HandleId {
        let f: PackerFn = self
            .state
            .lock()
            .packers
            .get(&fn_id)
            .cloned()
            .expect("packer not registered");
        f(handles)
    }

    fn producer_deactivate(&self, node_id: NodeId) {
        // Default behavior — drop the producer's storage entry, which
        // cascades into Subscription drops + sink unsubscription.
        default_producer_deactivate(&self.producer_storage, node_id);
    }
}

impl ProducerBinding for InnerBinding {
    fn register_producer_build(&self, build: ProducerBuildFn) -> FnId {
        let mut s = self.state.lock();
        let id = self.alloc_fn_id(&mut s);
        s.producer_builds.insert(id, Arc::from(build));
        id
    }

    fn producer_storage(&self) -> &ProducerStorage {
        &self.producer_storage
    }
}

impl OperatorBinding for InnerBinding {
    fn register_projector(&self, f: ProjectorBox) -> FnId {
        let mut s = self.state.lock();
        let id = self.alloc_fn_id(&mut s);
        s.projectors.insert(id, Arc::from(f));
        id
    }

    fn register_predicate(&self, f: PredicateBox) -> FnId {
        let mut s = self.state.lock();
        let id = self.alloc_fn_id(&mut s);
        s.predicates.insert(id, Arc::from(f));
        id
    }

    fn register_folder(&self, f: FolderBox) -> FnId {
        let mut s = self.state.lock();
        let id = self.alloc_fn_id(&mut s);
        s.folders.insert(id, Arc::from(f));
        id
    }

    fn register_equals(&self, f: EqualsBox) -> FnId {
        let mut s = self.state.lock();
        let id = self.alloc_fn_id(&mut s);
        s.equals.insert(id, Arc::from(f));
        id
    }

    fn register_pairwise_packer(&self, f: PairwiseBox) -> FnId {
        let mut s = self.state.lock();
        let id = self.alloc_fn_id(&mut s);
        s.pairwises.insert(id, Arc::from(f));
        id
    }

    fn register_packer(&self, f: PackerBox) -> FnId {
        let mut s = self.state.lock();
        let id = self.alloc_fn_id(&mut s);
        s.packers.insert(id, Arc::from(f));
        id
    }
}

impl HigherOrderBinding for InnerBinding {
    fn register_project(&self, project: ProjectFn) -> FnId {
        let mut s = self.state.lock();
        let id = self.alloc_fn_id(&mut s);
        s.projects.insert(id, Arc::from(project));
        id
    }

    fn invoke_project(&self, fn_id: FnId, value: HandleId) -> NodeId {
        let f: ProjectArc = self
            .state
            .lock()
            .projects
            .get(&fn_id)
            .cloned()
            .expect("project closure not registered");
        f(value)
    }
}

// ---------------------------------------------------------------------
// OpRuntime — Core + binding glue for tests.
// ---------------------------------------------------------------------

pub struct OpRuntime {
    pub core: Core,
    pub binding: Arc<InnerBinding>,
    pub op_binding: Arc<dyn OperatorBinding>,
    pub producer_binding: Arc<dyn ProducerBinding>,
    pub ho_binding: Arc<dyn HigherOrderBinding>,
}

impl OpRuntime {
    pub fn new() -> Self {
        let inner = InnerBinding::new();
        let core = Core::new(inner.clone() as Arc<dyn BindingBoundary>);
        // Set the binding's self-Core back-ref. Required for producer
        // dispatch (the `invoke_fn` path constructs a `ProducerCtx`
        // with this ref).
        inner.set_core_ref(core.clone());
        let op_binding: Arc<dyn OperatorBinding> = inner.clone();
        let producer_binding: Arc<dyn ProducerBinding> = inner.clone();
        let ho_binding: Arc<dyn HigherOrderBinding> = inner.clone();
        Self {
            core,
            binding: inner,
            op_binding,
            producer_binding,
            ho_binding,
        }
    }

    pub fn intern(&self, v: TestValue) -> HandleId {
        self.binding.intern(v)
    }

    pub fn intern_int(&self, n: i64) -> HandleId {
        self.binding.intern(TestValue::Int(n))
    }

    pub fn deref(&self, h: HandleId) -> TestValue {
        self.binding.deref(h)
    }

    pub fn state_int(&self, initial: Option<i64>) -> NodeId {
        let h = match initial {
            Some(n) => self.intern_int(n),
            None => NO_HANDLE,
        };
        self.core.register_state(h, false).unwrap()
    }

    pub fn subscribe_recorder(&self, node: NodeId) -> Recorder {
        let recorder = Recorder::new(self.binding.clone());
        let sink: Sink = recorder.sink();
        let sub = self.core.subscribe(node, sink);
        recorder.attach(sub);
        recorder
    }

    /// Convenience: emit an integer DATA on a state node.
    pub fn emit_int(&self, node: NodeId, n: i64) {
        let h = self.intern_int(n);
        self.core.emit(node, h);
    }

    /// Create a packer closure that packs N HandleIds into a Tuple TestValue.
    /// The closure captures the binding for deref/intern.
    pub fn make_packer(&self) -> graphrefly_operators::combine::PackerFn {
        let binding = self.binding.clone();
        Box::new(move |handles: &[HandleId]| {
            let values: Vec<TestValue> = handles.iter().map(|&h| binding.deref(h)).collect();
            binding.intern(TestValue::Tuple(values))
        })
    }

    /// Register a packer FnId for tuple-packing in zip / combine ops.
    /// Returns the FnId to pass to the operator factory.
    pub fn register_tuple_packer(&self) -> FnId {
        let packer = self.make_packer();
        self.op_binding.register_packer(packer)
    }
}

impl Drop for OpRuntime {
    fn drop(&mut self) {
        // Break the binding ⇆ Core Arc cycle introduced by
        // `set_core_ref`. Without this, every OpRuntime leaks Core
        // state (the binding's `core_ref` keeps Core alive; Core
        // keeps binding alive via its `binding` Arc). Tests rely on
        // refcount assertions, so this matters.
        self.binding.take_core_ref();
    }
}

// ---------------------------------------------------------------------
// Recorder — captures messages with handles resolved to TestValue.
// ---------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub enum RecordedEvent {
    Start,
    Data(TestValue),
    Resolved,
    Dirty,
    Complete,
    Error(TestValue),
    Teardown,
    Pause,
    Resume,
    Invalidate,
}

pub struct Recorder {
    inner: Arc<RecorderInner>,
}

struct RecorderInner {
    binding: Arc<InnerBinding>,
    events: Mutex<Vec<RecordedEvent>>,
    sub: Mutex<Option<Subscription>>,
    fire_count: AtomicU64,
}

impl Recorder {
    fn new(binding: Arc<InnerBinding>) -> Self {
        Self {
            inner: Arc::new(RecorderInner {
                binding,
                events: Mutex::new(Vec::new()),
                sub: Mutex::new(None),
                fire_count: AtomicU64::new(0),
            }),
        }
    }

    fn sink(&self) -> Sink {
        // Capture `Weak<RecorderInner>` (NOT Arc) so that dropping the
        // Recorder triggers `Subscription::Drop` even when the sink
        // is still in the producer's `subscribers` map. Otherwise the
        // sink Arc → RecorderInner → sub → Subscription cycle pins
        // RecorderInner alive (sink holds it) and Subscription::Drop
        // never fires until Core drops, breaking the
        // `producer_deactivate` lifecycle hook.
        //
        // Sink fires post-Recorder-drop become silent no-ops, which
        // matches the user's intent (they dropped the Recorder, so
        // they don't want events anymore).
        let inner_weak: std::sync::Weak<RecorderInner> = Arc::downgrade(&self.inner);
        Arc::new(move |msgs: &[Message]| {
            let Some(inner) = inner_weak.upgrade() else {
                return;
            };
            inner.fire_count.fetch_add(1, Ordering::SeqCst);
            let mut events = inner.events.lock();
            for &m in msgs {
                let event = match m {
                    Message::Start => RecordedEvent::Start,
                    Message::Data(h) => RecordedEvent::Data(inner.binding.deref(h)),
                    Message::Resolved => RecordedEvent::Resolved,
                    Message::Dirty => RecordedEvent::Dirty,
                    Message::Complete => RecordedEvent::Complete,
                    Message::Error(h) => RecordedEvent::Error(inner.binding.deref(h)),
                    Message::Teardown => RecordedEvent::Teardown,
                    Message::Pause(_) => RecordedEvent::Pause,
                    Message::Resume(_) => RecordedEvent::Resume,
                    Message::Invalidate => RecordedEvent::Invalidate,
                };
                events.push(event);
            }
        })
    }

    fn attach(&self, sub: Subscription) {
        *self.inner.sub.lock() = Some(sub);
    }

    pub fn events(&self) -> Vec<RecordedEvent> {
        self.inner.events.lock().clone()
    }

    pub fn data_values(&self) -> Vec<TestValue> {
        self.inner
            .events
            .lock()
            .iter()
            .filter_map(|e| match e {
                RecordedEvent::Data(v) => Some(v.clone()),
                _ => None,
            })
            .collect()
    }

    pub fn fire_count(&self) -> u64 {
        self.inner.fire_count.load(Ordering::SeqCst)
    }

    pub fn clear(&self) {
        self.inner.events.lock().clear();
    }
}

// Silence unused-import warnings when tests don't use every helper.
const _: fn() = || {
    fn _check<T: Send + Sync>() {}
    _check::<InnerBinding>();
    _check::<Recorder>();
    let _ = AHashMap::<u64, u64>::new();
    let _ = AHashSet::<u64>::new();
};
