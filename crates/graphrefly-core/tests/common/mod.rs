//! Test runtime — Rust analogue of `bindings.ts`'s `HandleRuntime`.
//!
//! Implements the binding-side value registry that the handle-protocol Core
//! talks to. Tests use this to wire user values through the dispatcher
//! without writing the FFI plumbing themselves.
//!
//! Dedup discipline mirrors the TS prototype:
//! - Primitives intern by value (same value → same handle).
//! - Objects (`Arc<TestObject>`) intern by pointer identity (`Arc::as_ptr`).
//! - Null is a primitive value (R1.2.4: `null` is a valid DATA payload;
//!   `undefined`/`None` is the SENTINEL).

#![allow(dead_code)] // Some helpers used only by certain test files.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use graphrefly_core::{
    BindingBoundary, Core, DepBatch, EqualsMode, FnId, FnResult, HandleId, Message, NodeId, Sink,
    Subscription,
};

#[derive(Clone, Debug)]
pub enum TestValue {
    Int(i64),
    Str(String),
    /// Object identity carrier — Arc allows `ptr_eq`-based dedup, mirroring
    /// JS object identity via WeakMap.
    Object(Arc<TestObject>),
    Null,
}

#[derive(Debug)]
pub struct TestObject {
    pub label: String,
    pub x: i64,
}

impl TestValue {
    /// Hashable+equatable key for primitive interning. Returns `None` for
    /// `Object` (objects intern by pointer, not value).
    fn primitive_key(&self) -> Option<PrimitiveKey> {
        match self {
            Self::Int(n) => Some(PrimitiveKey::Int(*n)),
            Self::Str(s) => Some(PrimitiveKey::Str(s.clone())),
            Self::Null => Some(PrimitiveKey::Null),
            Self::Object(_) => None,
        }
    }

    fn object_ptr(&self) -> Option<usize> {
        match self {
            Self::Object(arc) => Some(Arc::as_ptr(arc) as usize),
            _ => None,
        }
    }
}

#[derive(Clone, Eq, PartialEq, Hash)]
enum PrimitiveKey {
    Int(i64),
    Str(String),
    Null,
}

type FnImpl = Arc<dyn Fn(&[TestValue]) -> Option<TestValue> + Send + Sync>;
type DynamicFnImpl =
    Arc<dyn Fn(&[TestValue]) -> (Option<TestValue>, Option<Vec<usize>>) + Send + Sync>;
/// Raw fn that returns a fully-formed `FnResult` — used by tests that need
/// `FnResult::Batch` or other non-standard return shapes. The closure
/// captures its own `Arc<TestBinding>` for handle interning.
type RawFnImpl = Arc<dyn Fn(&[DepBatch]) -> FnResult + Send + Sync>;
type EqualsImpl = Arc<dyn Fn(&TestValue, &TestValue) -> bool + Send + Sync>;

struct RegistryInner {
    next_handle: u64,
    next_fn_id: u64,
    values: HashMap<HandleId, TestValue>,
    refcounts: HashMap<HandleId, u64>,
    primitive_index: HashMap<PrimitiveKey, HandleId>,
    object_index: HashMap<usize, HandleId>,
    fns: HashMap<FnId, FnImpl>,
    /// Dynamic fns return both a value and a tracked-deps set. A given fn_id
    /// is in EITHER `fns` OR `dynamic_fns`; never both.
    dynamic_fns: HashMap<FnId, DynamicFnImpl>,
    /// Raw fns that return a fully-formed `FnResult` — for testing
    /// `FnResult::Batch` and other advanced emission patterns.
    raw_fns: HashMap<FnId, RawFnImpl>,
    custom_equals: HashMap<FnId, EqualsImpl>,
}

pub struct TestBinding {
    inner: Mutex<RegistryInner>,
}

impl TestBinding {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(RegistryInner {
                next_handle: 1,
                next_fn_id: 1,
                values: HashMap::new(),
                refcounts: HashMap::new(),
                primitive_index: HashMap::new(),
                object_index: HashMap::new(),
                fns: HashMap::new(),
                dynamic_fns: HashMap::new(),
                raw_fns: HashMap::new(),
                custom_equals: HashMap::new(),
            }),
        })
    }

    /// Intern a value, returning its handle. Bumps refcount.
    /// Mirrors `bindings.ts` `valueRegistry.intern(value)`.
    pub fn intern(&self, value: TestValue) -> HandleId {
        let mut inner = self.inner.lock().expect("registry lock");
        let existing = if let Some(key) = value.primitive_key() {
            inner.primitive_index.get(&key).copied()
        } else if let Some(ptr) = value.object_ptr() {
            inner.object_index.get(&ptr).copied()
        } else {
            None
        };
        if let Some(h) = existing {
            *inner.refcounts.entry(h).or_insert(0) += 1;
            return h;
        }
        let h = HandleId::new(inner.next_handle);
        inner.next_handle += 1;
        if let Some(key) = value.primitive_key() {
            inner.primitive_index.insert(key, h);
        }
        if let Some(ptr) = value.object_ptr() {
            inner.object_index.insert(ptr, h);
        }
        inner.values.insert(h, value);
        inner.refcounts.insert(h, 1);
        h
    }

    /// Resolve a handle to its underlying value.
    pub fn deref(&self, handle: HandleId) -> TestValue {
        self.inner
            .lock()
            .expect("registry lock")
            .values
            .get(&handle)
            .cloned()
            .unwrap_or_else(|| panic!("handle {handle:?} not in registry"))
    }

    /// Number of handles currently held (refcount > 0).
    pub fn live_handles(&self) -> usize {
        self.inner
            .lock()
            .expect("registry lock")
            .refcounts
            .values()
            .filter(|&&n| n > 0)
            .count()
    }

    /// Inspector for the current refcount on a specific handle. Returns 0 if
    /// the handle has been fully released or never existed. Used by audit-fix
    /// regression tests that need to verify retain/release pairs balance
    /// even when a handle stays alive through other shares.
    pub fn refcount_of(&self, handle: HandleId) -> u64 {
        self.inner
            .lock()
            .expect("registry lock")
            .refcounts
            .get(&handle)
            .copied()
            .unwrap_or(0)
    }

    pub fn register_fn<F>(&self, f: F) -> FnId
    where
        F: Fn(&[TestValue]) -> Option<TestValue> + Send + Sync + 'static,
    {
        let mut inner = self.inner.lock().expect("registry lock");
        let id = FnId::new(inner.next_fn_id);
        inner.next_fn_id += 1;
        inner.fns.insert(id, Arc::new(f));
        id
    }

    pub fn register_dynamic_fn<F>(&self, f: F) -> FnId
    where
        F: Fn(&[TestValue]) -> (Option<TestValue>, Option<Vec<usize>>) + Send + Sync + 'static,
    {
        let mut inner = self.inner.lock().expect("registry lock");
        let id = FnId::new(inner.next_fn_id);
        inner.next_fn_id += 1;
        inner.dynamic_fns.insert(id, Arc::new(f));
        id
    }

    pub fn register_custom_equals<F>(&self, f: F) -> FnId
    where
        F: Fn(&TestValue, &TestValue) -> bool + Send + Sync + 'static,
    {
        let mut inner = self.inner.lock().expect("registry lock");
        let id = FnId::new(inner.next_fn_id);
        inner.next_fn_id += 1;
        inner.custom_equals.insert(id, Arc::new(f));
        id
    }

    /// Register a raw fn that returns a fully-formed `FnResult` — for testing
    /// `FnResult::Batch` and other advanced emission patterns. The closure
    /// receives `&[DepBatch]` directly (not resolved values).
    pub fn register_raw_fn<F>(&self, f: F) -> FnId
    where
        F: Fn(&[DepBatch]) -> FnResult + Send + Sync + 'static,
    {
        let mut inner = self.inner.lock().expect("registry lock");
        let id = FnId::new(inner.next_fn_id);
        inner.next_fn_id += 1;
        inner.raw_fns.insert(id, Arc::new(f));
        id
    }
}

impl BindingBoundary for TestBinding {
    fn invoke_fn(&self, _node_id: NodeId, fn_id: FnId, dep_data: &[DepBatch]) -> FnResult {
        // Dispatch to raw_fns first — these get full DepBatch control for
        // testing FnResult::Batch and other advanced patterns.
        let raw_f = self
            .inner
            .lock()
            .expect("registry lock")
            .raw_fns
            .get(&fn_id)
            .cloned();
        if let Some(f) = raw_f {
            return f(dep_data);
        }

        // Resolve each dep's latest handle to a value. Outside batch() scope
        // this is the single DATA handle; inside batch() it's the last of the
        // K coalesced emits. Existing test fns see the same &[TestValue] shape
        // they always did — batch-array-aware tests use register_dynamic_fn
        // and inspect the full DepBatch slice via a separate path if needed.
        let dep_values: Vec<TestValue> =
            dep_data.iter().map(|db| self.deref(db.latest())).collect();

        // Dispatch to dynamic_fns first; fall back to plain fns.
        let dyn_f = self
            .inner
            .lock()
            .expect("registry lock")
            .dynamic_fns
            .get(&fn_id)
            .cloned();
        if let Some(f) = dyn_f {
            let (value, tracked) = f(&dep_values);
            return match value {
                Some(v) => {
                    let handle = self.intern(v);
                    FnResult::Data { handle, tracked }
                }
                None => FnResult::Noop { tracked },
            };
        }

        let f = self
            .inner
            .lock()
            .expect("registry lock")
            .fns
            .get(&fn_id)
            .cloned()
            .unwrap_or_else(|| panic!("unknown fn_id {fn_id:?}"));
        match f(&dep_values) {
            Some(value) => {
                let handle = self.intern(value);
                FnResult::Data {
                    handle,
                    tracked: None,
                }
            }
            None => FnResult::Noop { tracked: None },
        }
    }

    fn custom_equals(&self, equals_handle: FnId, a: HandleId, b: HandleId) -> bool {
        let f = self
            .inner
            .lock()
            .expect("registry lock")
            .custom_equals
            .get(&equals_handle)
            .cloned()
            .unwrap_or_else(|| panic!("unknown custom-equals fn_id {equals_handle:?}"));
        let av = self.deref(a);
        let bv = self.deref(b);
        f(&av, &bv)
    }

    fn release_handle(&self, handle: HandleId) {
        let mut inner = self.inner.lock().expect("registry lock");
        let count = inner.refcounts.entry(handle).or_insert(0);
        if *count > 0 {
            *count -= 1;
        }
        if *count == 0 {
            // Drop the value; remove from indices.
            if let Some(value) = inner.values.remove(&handle) {
                if let Some(key) = value.primitive_key() {
                    if inner.primitive_index.get(&key).copied() == Some(handle) {
                        inner.primitive_index.remove(&key);
                    }
                }
                if let Some(ptr) = value.object_ptr() {
                    if inner.object_index.get(&ptr).copied() == Some(handle) {
                        inner.object_index.remove(&ptr);
                    }
                }
            }
            inner.refcounts.remove(&handle);
        }
    }

    fn retain_handle(&self, handle: HandleId) {
        let mut inner = self.inner.lock().expect("registry lock");
        *inner.refcounts.entry(handle).or_insert(0) += 1;
    }
}

/// High-level facade: wraps Core + TestBinding with ergonomics matching the
/// TS `HandleRuntime`. Hides the handle/binding plumbing from test code.
pub struct TestRuntime {
    pub binding: Arc<TestBinding>,
    pub core: Core,
}

impl TestRuntime {
    pub fn new() -> Self {
        let binding = TestBinding::new();
        let core = Core::new(binding.clone() as Arc<dyn BindingBoundary>);
        Self { binding, core }
    }

    /// Register a state node. `initial = None` ⇒ sentinel.
    pub fn state(&self, initial: Option<TestValue>) -> StateHandle {
        let initial_handle = match initial {
            Some(v) => self.binding.intern(v),
            None => HandleId::new(0), // NO_HANDLE
        };
        let id = self.core.register_state(initial_handle, false).unwrap();
        StateHandle {
            id,
            binding: self.binding.clone(),
            core: self.core.clone(),
        }
    }

    /// Register a derived node from a Rust closure that takes `&[TestValue]`
    /// and returns an `Option<TestValue>`.
    pub fn derived<F>(&self, deps: &[NodeId], f: F) -> NodeId
    where
        F: Fn(&[TestValue]) -> Option<TestValue> + Send + Sync + 'static,
    {
        let fn_id = self.binding.register_fn(f);
        self.core
            .register_derived(deps, fn_id, EqualsMode::Identity, false)
            .unwrap()
    }

    /// Derived with custom equals.
    pub fn derived_with_equals<F, E>(&self, deps: &[NodeId], f: F, equals: E) -> NodeId
    where
        F: Fn(&[TestValue]) -> Option<TestValue> + Send + Sync + 'static,
        E: Fn(&TestValue, &TestValue) -> bool + Send + Sync + 'static,
    {
        let fn_id = self.binding.register_fn(f);
        let eq_id = self.binding.register_custom_equals(equals);
        self.core
            .register_derived(deps, fn_id, EqualsMode::Custom(eq_id), false)
            .unwrap()
    }

    /// Register a dynamic node. The fn returns `(value, tracked_indices)`:
    /// `tracked_indices` declares which dep indices fn actually read this
    /// run. Untracked deps still update cache but do not re-fire fn.
    pub fn dynamic<F>(&self, deps: &[NodeId], f: F) -> NodeId
    where
        F: Fn(&[TestValue]) -> (Option<TestValue>, Option<Vec<usize>>) + Send + Sync + 'static,
    {
        let fn_id = self.binding.register_dynamic_fn(f);
        self.core
            .register_dynamic(deps, fn_id, EqualsMode::Identity, false)
            .unwrap()
    }

    /// Subscribe a sink that records events into a shared `Vec<RecordedEvent>`.
    /// The returned [`Recorder`] owns the [`Subscription`] — when the recorder
    /// drops, the sink unsubscribes. Tests bind to a named variable
    /// (`let rec = ...` or `let _rec = ...`); binding to bare `_` drops
    /// immediately and unsubscribes before the test can act, which is rarely
    /// what you want.
    pub fn subscribe_recorder(&self, node_id: NodeId) -> Recorder {
        let recorder = Recorder::new();
        let sink: Sink = recorder.sink(self.binding.clone());
        let sub = self.core.subscribe(node_id, sink);
        recorder.attach(sub);
        recorder
    }

    pub fn cache_value(&self, node_id: NodeId) -> Option<TestValue> {
        let h = self.core.cache_of(node_id);
        if h == HandleId::new(0) {
            None
        } else {
            Some(self.binding.deref(h))
        }
    }
}

/// Test handle for state nodes; mirrors the TS `state.set(value)` ergonomics.
pub struct StateHandle {
    pub id: NodeId,
    pub binding: Arc<TestBinding>,
    pub core: Core,
}

impl StateHandle {
    pub fn set(&self, value: TestValue) {
        let handle = self.binding.intern(value);
        self.core.emit(self.id, handle);
    }

    pub fn current(&self) -> Option<TestValue> {
        let h = self.core.cache_of(self.id);
        if h == HandleId::new(0) {
            None
        } else {
            Some(self.binding.deref(h))
        }
    }
}

/// Recorded event from a subscriber — value-resolved, like `core.test.ts`'s
/// `Event` type.
#[derive(Clone, Debug, PartialEq)]
pub enum RecordedEvent {
    Start,
    Dirty,
    Data(TestValue),
    Resolved,
    Invalidate,
    Pause(graphrefly_core::LockId),
    Resume(graphrefly_core::LockId),
    Complete,
    Error(TestValue),
    Teardown,
}

/// Records events from a subscriber. Owns its [`Subscription`] once attached
/// — drop the recorder to unsubscribe.
///
/// Not `Clone` because it owns the Subscription (which is not Clone). If a
/// test wants to inspect events from multiple places, share the inner
/// `events` Arc via [`Self::events`].
pub struct Recorder {
    events: Arc<Mutex<Vec<RecordedEvent>>>,
    /// Per-call event counts — `call_boundaries[i]` is the number of events
    /// in the i-th sink call. Sum equals `events.len()`. Used by R1.3.5.a
    /// handshake-tier-split tests that need to verify "the [Start] and
    /// [Data(v)] handshake came as 2 separate sink calls, not 1 batched
    /// call".
    call_boundaries: Arc<Mutex<Vec<usize>>>,
    /// Number of sink calls observed. Cheap to read from any thread.
    call_count: Arc<AtomicU64>,
    /// Holds the subscription so that dropping the Recorder unsubscribes.
    /// `Option` so [`Self::new`] can construct the recorder before the sink
    /// is registered, then [`Self::attach`] fills it in.
    subscription: Mutex<Option<Subscription>>,
}

impl Recorder {
    pub fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            call_boundaries: Arc::new(Mutex::new(Vec::new())),
            call_count: Arc::new(AtomicU64::new(0)),
            subscription: Mutex::new(None),
        }
    }

    pub fn sink(&self, binding: Arc<TestBinding>) -> Sink {
        let events = self.events.clone();
        let call_boundaries = self.call_boundaries.clone();
        let call_count = self.call_count.clone();
        Arc::new(move |msgs: &[Message]| {
            let mut guard = events.lock().expect("recorder lock");
            for msg in msgs {
                let recorded = match msg {
                    Message::Start => RecordedEvent::Start,
                    Message::Dirty => RecordedEvent::Dirty,
                    Message::Resolved => RecordedEvent::Resolved,
                    Message::Data(h) => RecordedEvent::Data(binding.deref(*h)),
                    Message::Invalidate => RecordedEvent::Invalidate,
                    Message::Pause(l) => RecordedEvent::Pause(*l),
                    Message::Resume(l) => RecordedEvent::Resume(*l),
                    Message::Complete => RecordedEvent::Complete,
                    Message::Error(h) => RecordedEvent::Error(binding.deref(*h)),
                    Message::Teardown => RecordedEvent::Teardown,
                };
                guard.push(recorded);
            }
            call_boundaries
                .lock()
                .expect("recorder lock")
                .push(msgs.len());
            call_count.fetch_add(1, Ordering::SeqCst);
        })
    }

    /// Number of times the sink was invoked. Each `sink(&[...])` callback
    /// is one call regardless of `msgs.len()`. Used by tier-split tests.
    #[must_use]
    pub fn call_count(&self) -> u64 {
        self.call_count.load(Ordering::SeqCst)
    }

    /// Per-call message counts in arrival order — `[2, 1]` means two
    /// sink invocations: the first delivered 2 messages, the second
    /// delivered 1.
    #[must_use]
    pub fn call_boundaries(&self) -> Vec<usize> {
        self.call_boundaries.lock().expect("recorder lock").clone()
    }

    pub fn attach(&self, sub: Subscription) {
        *self.subscription.lock().expect("recorder lock") = Some(sub);
    }

    /// Manually unsubscribe early (before recorder drop). Useful for tests
    /// that want to verify post-unsubscribe behavior without dropping the
    /// whole recorder.
    pub fn unsubscribe(&self) {
        *self.subscription.lock().expect("recorder lock") = None;
    }

    pub fn snapshot(&self) -> Vec<RecordedEvent> {
        self.events.lock().expect("recorder lock").clone()
    }

    pub fn data_values(&self) -> Vec<TestValue> {
        self.snapshot()
            .into_iter()
            .filter_map(|e| match e {
                RecordedEvent::Data(v) => Some(v),
                _ => None,
            })
            .collect()
    }

    pub fn count(&self, predicate: impl Fn(&RecordedEvent) -> bool) -> usize {
        self.snapshot().iter().filter(|e| predicate(e)).count()
    }
}

/// Convenience equality on TestValue for assertions.
impl PartialEq for TestValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Int(a), Self::Int(b)) => a == b,
            (Self::Str(a), Self::Str(b)) => a == b,
            (Self::Object(a), Self::Object(b)) => Arc::ptr_eq(a, b),
            (Self::Null, Self::Null) => true,
            _ => false,
        }
    }
}
