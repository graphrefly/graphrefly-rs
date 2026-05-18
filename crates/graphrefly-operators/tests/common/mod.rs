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
    BindingBoundary, Core, FnId, HandleId, Message, NodeId, Sink, NO_HANDLE,
};
use graphrefly_operators::{
    higher_order::{HigherOrderBinding, ProjectFn},
    producer::{
        default_producer_deactivate, ProducerBuildFn, ProducerCtx, ProducerEmitter,
        ProducerStorage, SubGuard,
    },
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
type TapBox = Box<dyn Fn(HandleId) + Send + Sync>;
type TapErrorBox = Box<dyn Fn(HandleId) + Send + Sync>;
type TapCompleteBox = Box<dyn Fn() + Send + Sync>;
type RescueBox = Box<dyn Fn(HandleId) -> Result<HandleId, ()> + Send + Sync>;
type StratifyClassifierBox = Box<dyn Fn(HandleId, HandleId) -> bool + Send + Sync>;

type ProjectorFn = Arc<dyn Fn(HandleId) -> HandleId + Send + Sync>;
type PredicateFn = Arc<dyn Fn(HandleId) -> bool + Send + Sync>;
type FolderFn = Arc<dyn Fn(HandleId, HandleId) -> HandleId + Send + Sync>;
type EqualsFn = Arc<dyn Fn(HandleId, HandleId) -> bool + Send + Sync>;
type PairwiseFn = Arc<dyn Fn(HandleId, HandleId) -> HandleId + Send + Sync>;
type PackerFn = Arc<dyn Fn(&[HandleId]) -> HandleId + Send + Sync>;
type TapFn = Arc<dyn Fn(HandleId) + Send + Sync>;
type TapErrorFn = Arc<dyn Fn(HandleId) + Send + Sync>;
type TapCompleteFn = Arc<dyn Fn() + Send + Sync>;
type RescueFn = Arc<dyn Fn(HandleId) -> Result<HandleId, ()> + Send + Sync>;
type StratifyClassifierFn = Arc<dyn Fn(HandleId, HandleId) -> bool + Send + Sync>;

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
    taps: HashMap<FnId, TapFn>,
    tap_errors: HashMap<FnId, TapErrorFn>,
    tap_completes: HashMap<FnId, TapCompleteFn>,
    rescues: HashMap<FnId, RescueFn>,
    stratify_classifiers: HashMap<FnId, StratifyClassifierFn>,
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
                taps: HashMap::new(),
                tap_errors: HashMap::new(),
                tap_completes: HashMap::new(),
                rescues: HashMap::new(),
                stratify_classifiers: HashMap::new(),
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

    pub fn register_tap(&self, f: TapBox) -> FnId {
        let mut s = self.state.lock();
        let id = self.alloc_fn_id(&mut s);
        s.taps.insert(id, Arc::from(f));
        id
    }

    pub fn register_tap_error(&self, f: TapErrorBox) -> FnId {
        let mut s = self.state.lock();
        let id = self.alloc_fn_id(&mut s);
        s.tap_errors.insert(id, Arc::from(f));
        id
    }

    pub fn register_tap_complete(&self, f: TapCompleteBox) -> FnId {
        let mut s = self.state.lock();
        let id = self.alloc_fn_id(&mut s);
        s.tap_completes.insert(id, Arc::from(f));
        id
    }

    pub fn register_rescue(&self, f: RescueBox) -> FnId {
        let mut s = self.state.lock();
        let id = self.alloc_fn_id(&mut s);
        s.rescues.insert(id, Arc::from(f));
        id
    }

    pub fn register_stratify_classifier(&self, f: StratifyClassifierBox) -> FnId {
        let mut s = self.state.lock();
        let id = self.alloc_fn_id(&mut s);
        s.stratify_classifiers.insert(id, Arc::from(f));
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

    fn invoke_tap_fn(&self, fn_id: FnId, handle: HandleId) {
        let f: TapFn = self
            .state
            .lock()
            .taps
            .get(&fn_id)
            .cloned()
            .expect("tap fn not registered");
        f(handle);
    }

    fn invoke_tap_error_fn(&self, fn_id: FnId, handle: HandleId) {
        let f: TapErrorFn = self
            .state
            .lock()
            .tap_errors
            .get(&fn_id)
            .cloned()
            .expect("tap_error fn not registered");
        f(handle);
    }

    fn invoke_tap_complete_fn(&self, fn_id: FnId) {
        let f: TapCompleteFn = self
            .state
            .lock()
            .tap_completes
            .get(&fn_id)
            .cloned()
            .expect("tap_complete fn not registered");
        f();
    }

    fn invoke_rescue_fn(&self, fn_id: FnId, handle: HandleId) -> Result<HandleId, ()> {
        let f: RescueFn = self
            .state
            .lock()
            .rescues
            .get(&fn_id)
            .cloned()
            .expect("rescue fn not registered");
        f(handle)
    }

    fn invoke_stratify_classifier_fn(
        &self,
        fn_id: FnId,
        rules_handle: HandleId,
        value_handle: HandleId,
    ) -> bool {
        let f: StratifyClassifierFn = self
            .state
            .lock()
            .stratify_classifiers
            .get(&fn_id)
            .cloned()
            .expect("stratify classifier not registered");
        // Lock dropped — closure may re-enter the binding to deref handles.
        f(rules_handle, value_handle)
    }

    fn intern_node(&self, node_id: NodeId) -> HandleId {
        // Intern the NodeId as a TestValue::Int(raw_id) for simplicity.
        let raw = node_id.raw();
        self.intern(TestValue::Int(raw as i64))
    }

    fn producer_deactivate(
        &self,
        node_id: NodeId,
        unsub: &dyn Fn(NodeId, graphrefly_core::SubscriptionId),
    ) {
        // S2b/D229: drop the producer's storage entry, explicitly
        // unsubscribing recorded upstream subs via the owner-supplied
        // closure (was the retired `Vec<Subscription>`-drop cascade).
        default_producer_deactivate(&self.producer_storage, node_id, unsub);
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

    fn register_stratify_classifier(&self, f: StratifyClassifierBox) -> FnId {
        let mut s = self.state.lock();
        let id = self.alloc_fn_id(&mut s);
        s.stratify_classifiers.insert(id, Arc::from(f));
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
        // S2b: `subscribe` returns a `SubscriptionId`; wrap it in a
        // binding-layer `SubGuard` (D228-A) so dropping the Recorder
        // schedules the unsubscribe. The harness co-owns the Core, so
        // `ProducerEmitter::for_core` is the sanctioned bridge.
        let sub_id = self.core.subscribe(node, sink);
        recorder.attach(SubGuard::new(
            node,
            sub_id,
            ProducerEmitter::for_core(&self.core),
        ));
        recorder
    }

    /// Drain any deferred mailbox ops (timer/producer/`SubGuard`
    /// unsubscribe) **now**, owner-side (S2b/D230/D234). The sync test
    /// harness has no autonomous pump; tests that drop a Recorder and
    /// then assert deactivation/unsubscribe *without* a subsequent wave
    /// call this to force the deferred unsubscribe to apply (mirrors the
    /// embedder pump point — behaviour-equivalent to the retired
    /// synchronous `Subscription::Drop`).
    pub fn settle(&self) {
        self.core.drain_mailbox();
    }

    /// Convenience: emit an integer DATA on a state node.
    pub fn emit_int(&self, node: NodeId, n: i64) {
        let h = self.intern_int(n);
        self.core.emit(node, h);
    }

    /// Create a packer closure that packs N HandleIds into a Tuple TestValue.
    /// The closure captures the binding via [`Weak`] so it doesn't pin the
    /// binding alive past the runtime's `Drop` — mirrors the production
    /// producer-build cycle discipline (Slice Y).
    pub fn make_packer(&self) -> graphrefly_operators::combine::PackerFn {
        let binding_weak: std::sync::Weak<InnerBinding> = Arc::downgrade(&self.binding);
        Box::new(move |handles: &[HandleId]| {
            let binding = binding_weak
                .upgrade()
                .expect("test invariant: packer fired after binding drop");
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

    /// B sub-slice (2026-05-10): execute `f` inside a `core.batch()`
    /// scope that pre-acquires every CURRENTLY-EXISTING partition's
    /// wave_owner. Sources registered BEFORE the helper call are
    /// pre-held; subscribing to them yields `SubscribeOutcome::Dead`
    /// SYNCHRONOUSLY when terminal + non-resubscribable (vs the
    /// `SubscribeOutcome::Deferred` path under Phase H+ STRICT).
    ///
    /// This unblocks F2 end-to-end Dead-path tests: pre-D-α the
    /// existing test workarounds (using meta-companion partitions)
    /// hit the Phase H+ Deferred path instead of the immediate-Dead
    /// path, so per-op Dead handlers couldn't be exercised end-to-end.
    ///
    /// Why holding all partitions works: `try_subscribe(source)`
    /// inside the producer's activation wave acquires `source`'s
    /// partition lock. Because the outer `batch()` already holds it
    /// (via the parking_lot::ReentrantMutex), same-thread re-acquire
    /// is free; the Phase H+ ascending-order check passes because all
    /// partitions are already held by this thread. The subscribe
    /// path then sees the source's `resubscribable=false +
    /// terminal=Some(...)` state and returns
    /// `SubscribeError::TornDown`, which `ProducerCtx::subscribe_to`
    /// surfaces as `SubscribeOutcome::Dead`.
    ///
    /// **Scope caveats (/qa m10/m24, 2026-05-10):**
    /// 1. **New partitions created INSIDE the closure are NOT
    ///    pre-held.** When the closure calls `zip(...)` or another
    ///    factory that registers a producer-shape node, that node's
    ///    own partition is created mid-closure. The producer's
    ///    activation wave acquires it independently. This still
    ///    works for the Dead path because the activation wave's
    ///    `try_subscribe(source)` only needs SOURCE's partition held
    ///    (which it is, via the outer batch's pre-acquire), not the
    ///    producer's own partition.
    /// 2. **Cross-Core scenarios are out of scope.** `core.batch()`
    ///    acquires only THIS Core's partitions. A source on a
    ///    different Core (cross-Core mount) is not held by this
    ///    helper.
    /// 3. **Retry-validate panic possibility.** `core.begin_batch()`
    ///    uses a bounded retry-validate loop and panics with
    ///    `"exceeded {MAX_LOCK_RETRIES} retries"` on pathological
    ///    concurrent `register` / `set_deps` activity. Test workloads
    ///    that interleave registration with this helper may surface
    ///    this; the panic is informative.
    pub fn with_all_partitions_held<R>(&self, f: impl FnOnce(&Self) -> R) -> R {
        let _g = self.core.begin_batch();
        f(self)
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
    // S2b/D228-A: binding-layer RAII over `Core::unsubscribe` (core
    // RAII retired). `SubGuard::Drop` posts the unsubscribe via the
    // mailbox; the in-wave `drain_mailbox` (A′) applies it on the next
    // wave, or `TestRuntime::settle()` forces it immediately.
    sub: Mutex<Option<SubGuard>>,
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

    fn attach(&self, guard: SubGuard) {
        *self.inner.sub.lock() = Some(guard);
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
