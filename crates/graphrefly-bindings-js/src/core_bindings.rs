//! Core dispatcher bindings — wraps `graphrefly_core::Core` for JS callers.
//!
//! Build profile: only enabled with the `tracing` feature (default), which
//! pulls in `graphrefly-core`.
//!
//! # Design notes
//!
//! - **Value registry on the Rust side.** For v0 FFI bench, we move the
//!   value registry into Rust (vs. the binding-side TS `valueRegistry` in
//!   the prototype). This avoids the Send+Sync conflict between
//!   napi-rs `JsFunction` (`!Send`) and the Core's `BindingBoundary: Send + Sync`.
//!   Trade-off: every emit serializes the value (i32 or string) through napi
//!   into a Rust handle. That's the FFI cost we're measuring.
//!
//! - **Pre-baked derived fns.** v0 supports a small set of built-in fn types
//!   (`Identity`, `Add`, `Mul`) registered as Rust closures. Real JS
//!   callbacks via TSFN come later — they're async (event-loop scheduled),
//!   which would conflate "true FFI cost" with "TSFN scheduling overhead."
//!
//! - **Subscribe via TSFN.** Subscribe takes a JS callback; we wrap it in
//!   a `ThreadsafeFunction` so it can be called from any thread. For v0
//!   bench we only test single-thread; cross-Worker scenarios use `Arc<Core>`.

use std::sync::{Arc, Mutex, OnceLock};

use graphrefly_core::{BindingBoundary, Core, EqualsMode, FnId, FnResult, HandleId, NodeId};
use napi_derive::napi;

// ---------------------------------------------------------------------------
// Value registry — owned by the Rust side for v0 FFI bench.
// In production, the registry lives in TS to enable WeakMap-based identity
// dedup. For bench purposes (primitives only), Rust-side intern is simpler
// and faster.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum BenchValue {
    Int(i32),
    #[allow(dead_code)] // reserved for v1 string-value benchmarks
    Str(String),
}

struct Registry {
    next_handle: u64,
    values: ahash::AHashMap<HandleId, BenchValue>,
    primitive_index: ahash::AHashMap<BenchValue, HandleId>,
    refcounts: ahash::AHashMap<HandleId, u64>,
    /// Pre-baked fn implementations.
    fns: ahash::AHashMap<FnId, Arc<dyn Fn(&[BenchValue]) -> Option<BenchValue> + Send + Sync>>,
    next_fn_id: u64,
}

impl Registry {
    fn new() -> Self {
        Self {
            next_handle: 1,
            values: ahash::AHashMap::new(),
            primitive_index: ahash::AHashMap::new(),
            refcounts: ahash::AHashMap::new(),
            fns: ahash::AHashMap::new(),
            next_fn_id: 1,
        }
    }

    fn intern(&mut self, value: BenchValue) -> HandleId {
        if let Some(&h) = self.primitive_index.get(&value) {
            *self.refcounts.entry(h).or_insert(0) += 1;
            return h;
        }
        let h = HandleId::new(self.next_handle);
        self.next_handle += 1;
        self.primitive_index.insert(value.clone(), h);
        self.values.insert(h, value);
        self.refcounts.insert(h, 1);
        h
    }

    fn deref(&self, h: HandleId) -> Option<BenchValue> {
        self.values.get(&h).cloned()
    }

    fn release(&mut self, h: HandleId) {
        let count = self.refcounts.entry(h).or_insert(0);
        if *count > 0 {
            *count -= 1;
        }
        if *count == 0 {
            if let Some(value) = self.values.remove(&h) {
                if self.primitive_index.get(&value).copied() == Some(h) {
                    self.primitive_index.remove(&value);
                }
            }
            self.refcounts.remove(&h);
        }
    }
}

struct BenchBinding {
    registry: Mutex<Registry>,
}

impl BenchBinding {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            registry: Mutex::new(Registry::new()),
        })
    }
}

impl BindingBoundary for BenchBinding {
    fn invoke_fn(&self, _: NodeId, fn_id: FnId, dep_handles: &[HandleId]) -> FnResult {
        let reg = self.registry.lock().expect("registry lock");
        let f = reg
            .fns
            .get(&fn_id)
            .cloned()
            .unwrap_or_else(|| panic!("unknown fn_id {fn_id:?}"));
        let dep_values: Vec<BenchValue> = dep_handles
            .iter()
            .map(|&h| {
                reg.deref(h)
                    .unwrap_or_else(|| panic!("unknown handle {h:?}"))
            })
            .collect();
        drop(reg);
        match f(&dep_values) {
            Some(v) => {
                let h = self.registry.lock().expect("registry lock").intern(v);
                FnResult::Data {
                    handle: h,
                    tracked: None,
                }
            }
            None => FnResult::Noop { tracked: None },
        }
    }

    fn custom_equals(&self, _: FnId, a: HandleId, b: HandleId) -> bool {
        a == b
    }

    fn release_handle(&self, h: HandleId) {
        self.registry.lock().expect("registry lock").release(h);
    }
}

// ---------------------------------------------------------------------------
// Pre-baked fn variants (v0 bench scope — no JS callbacks)
// ---------------------------------------------------------------------------

/// Built-in fn shapes selectable by name. Real JS-callback fns come later.
#[napi(string_enum)]
pub enum BuiltinFn {
    /// `f(x) = x` — passthrough; measures pure dispatch with one fn fire.
    Identity,
    /// `f(x) = x + 1` (Int only) — measures dispatch + simple computation.
    AddOne,
}

// ---------------------------------------------------------------------------
// JS-facing API: a single `BenchCore` class with the minimum to bench.
// ---------------------------------------------------------------------------

#[napi]
pub struct BenchCore {
    core: Core,
    binding: Arc<BenchBinding>,
}

#[napi]
impl BenchCore {
    /// Construct a fresh BenchCore wired to a Rust-side value registry.
    #[napi(constructor)]
    #[allow(clippy::new_without_default)]
    #[must_use]
    pub fn new() -> Self {
        let binding = BenchBinding::new();
        let core = Core::new(binding.clone() as Arc<dyn BindingBoundary>);
        Self { core, binding }
    }

    /// Register a state node with an i32 initial value. Returns the node id
    /// as a u32 (so JS can pass it back without BigInt overhead).
    #[napi]
    pub fn register_state_int(&self, initial: i32) -> u32 {
        let handle = self
            .binding
            .registry
            .lock()
            .expect("registry lock")
            .intern(BenchValue::Int(initial));
        u32::try_from(self.core.register_state(handle).raw())
            .expect("node id exceeds u32")
    }

    /// Register a state node with sentinel cache (no initial value).
    #[napi]
    pub fn register_state_sentinel(&self) -> u32 {
        u32::try_from(
            self.core
                .register_state(graphrefly_core::NO_HANDLE)
                .raw(),
        )
        .expect("node id exceeds u32")
    }

    /// Register a derived node with a built-in fn shape. `dep_ids` are the
    /// node ids returned by `register_state*` / `register_derived*`.
    #[napi]
    pub fn register_derived(&self, dep_ids: Vec<u32>, builtin: BuiltinFn) -> u32 {
        let fn_impl: Arc<dyn Fn(&[BenchValue]) -> Option<BenchValue> + Send + Sync> = match builtin
        {
            BuiltinFn::Identity => Arc::new(|deps: &[BenchValue]| deps.first().cloned()),
            BuiltinFn::AddOne => Arc::new(|deps: &[BenchValue]| match deps.first() {
                Some(BenchValue::Int(n)) => Some(BenchValue::Int(n + 1)),
                _ => None,
            }),
        };
        let mut reg = self.binding.registry.lock().expect("registry lock");
        let fn_id = FnId::new(reg.next_fn_id);
        reg.next_fn_id += 1;
        reg.fns.insert(fn_id, fn_impl);
        drop(reg);
        let deps: Vec<NodeId> = dep_ids.into_iter().map(|id| NodeId::new(u64::from(id))).collect();
        u32::try_from(
            self.core
                .register_derived(&deps, fn_id, EqualsMode::Identity)
                .raw(),
        )
        .expect("node id exceeds u32")
    }

    /// Subscribe a noop sink to a node — required to activate compute nodes.
    /// Returns the subscription id (u32) for symmetry; we don't expose unsub
    /// in v0 bench.
    #[napi]
    pub fn subscribe_noop(&self, node_id: u32) -> u32 {
        let sink: graphrefly_core::Sink = Arc::new(|_| {});
        u32::try_from(
            self.core
                .subscribe(NodeId::new(u64::from(node_id)), sink)
                .raw(),
        )
        .expect("sub id exceeds u32")
    }

    /// Emit an i32 value on a state node. Single FFI call per emit.
    #[napi]
    pub fn emit_int(&self, node_id: u32, value: i32) {
        let handle = self
            .binding
            .registry
            .lock()
            .expect("registry lock")
            .intern(BenchValue::Int(value));
        self.core.emit(NodeId::new(u64::from(node_id)), handle);
    }

    /// Read a state node's current cache as i32. Sentinel returns -1 (sentinel
    /// distinguishable in tests; for bench we always init non-sentinel).
    #[napi]
    pub fn cache_int(&self, node_id: u32) -> i32 {
        let h = self.core.cache_of(NodeId::new(u64::from(node_id)));
        if h == graphrefly_core::NO_HANDLE {
            return -1;
        }
        let reg = self.binding.registry.lock().expect("registry lock");
        match reg.deref(h) {
            Some(BenchValue::Int(n)) => n,
            _ => -1,
        }
    }

    /// Tight Rust loop: emit `n` times on a state node, no JS call between.
    /// Measures Rust dispatcher amortized cost. JS divides elapsed by `n`.
    #[napi]
    pub fn rust_emit_loop(&self, node_id: u32, n: u32) {
        let nid = NodeId::new(u64::from(node_id));
        let mut reg = self.binding.registry.lock().expect("registry lock");
        // Pre-intern handles to avoid hashing in the hot loop.
        let handles: Vec<HandleId> = (0..n)
            .map(|i| reg.intern(BenchValue::Int(i as i32)))
            .collect();
        drop(reg);
        for h in handles {
            self.core.emit(nid, h);
        }
    }
}

// ---------------------------------------------------------------------------
// Shared singleton — for cross-Worker bench.
//
// Node.js native modules are loaded once per process; static state in Rust
// is visible from every Worker thread that loads the addon. We expose a
// single global `BenchCore` that all Workers can call into. Operations on
// the global serialize through `Core`'s internal `parking_lot::Mutex` —
// concurrency is a Rust-side concern, transparent to JS callers.
// ---------------------------------------------------------------------------

static GLOBAL_CORE: OnceLock<Arc<BenchCore>> = OnceLock::new();

fn global() -> Arc<BenchCore> {
    GLOBAL_CORE
        .get_or_init(|| Arc::new(BenchCore::new()))
        .clone()
}

/// Register a state node on the process-global shared core. Returns the node id.
#[napi]
pub fn global_register_state_int(initial: i32) -> u32 {
    global().register_state_int(initial)
}

/// Register a derived (Identity) node on the global core.
#[napi]
pub fn global_register_derived_identity(dep_ids: Vec<u32>) -> u32 {
    global().register_derived(dep_ids, BuiltinFn::Identity)
}

/// Subscribe a noop sink on the global core.
#[napi]
pub fn global_subscribe_noop(node_id: u32) -> u32 {
    global().subscribe_noop(node_id)
}

/// Emit on the global core. Callable from any Worker; concurrent calls
/// serialize through the Core's internal Mutex.
#[napi]
pub fn global_emit_int(node_id: u32, value: i32) {
    global().emit_int(node_id, value);
}

/// Read cache from the global core. Callable from any Worker.
#[napi]
pub fn global_cache_int(node_id: u32) -> i32 {
    global().cache_int(node_id)
}

/// Tight Rust loop on the global core.
#[napi]
pub fn global_rust_emit_loop(node_id: u32, n: u32) {
    global().rust_emit_loop(node_id, n);
}
