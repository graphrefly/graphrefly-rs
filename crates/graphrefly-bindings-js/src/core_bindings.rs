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

use graphrefly_core::{
    BindingBoundary, Core, DepBatch, EqualsMode, FnEmission, FnId, FnResult, HandleId, LockId,
    NodeId, Subscription,
};
use napi::Error as NapiError;
use napi_derive::napi;
use smallvec::SmallVec;

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

/// User-side emission shape for the FnResult::Batch builtin fn registry.
/// Distinct from `graphrefly_core::FnEmission` because builtin fns emit
/// `BenchValue`s; the binding interns them into HandleIds at fire time.
#[derive(Clone, Debug)]
enum BenchEmission {
    Data(BenchValue),
    Complete,
    Error(BenchValue),
}

type SingleFnImpl = Arc<dyn Fn(&[BenchValue]) -> Option<BenchValue> + Send + Sync>;
type BatchFnImpl = Arc<dyn Fn(&[DepBatch], &Registry) -> Vec<BenchEmission> + Send + Sync>;

struct Registry {
    next_handle: u64,
    values: ahash::AHashMap<HandleId, BenchValue>,
    primitive_index: ahash::AHashMap<BenchValue, HandleId>,
    refcounts: ahash::AHashMap<HandleId, u64>,
    /// Pre-baked single-emission fn implementations (`FnResult::Data`).
    fns: ahash::AHashMap<FnId, SingleFnImpl>,
    /// Pre-baked batch fn implementations — fire returns `FnResult::Batch`
    /// with multiple emissions per wave. Models `actions.down(msgs)`.
    batch_fns: ahash::AHashMap<FnId, BatchFnImpl>,
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
            batch_fns: ahash::AHashMap::new(),
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
    fn invoke_fn(&self, _: NodeId, fn_id: FnId, dep_data: &[DepBatch]) -> FnResult {
        // Try batch fns first — they receive `&[DepBatch]` directly and may
        // emit multiple values per fire (FnResult::Batch).
        //
        // Single registry lock acquisition across the whole batch-fn fire:
        // fn lookup + value deref (via &Registry passed to the closure) +
        // emission intern. Eliminates the prior 3-acquisition TOCTOU window
        // where another thread could mutate `primitive_index` between phases.
        // No re-entrancy possible: the closure only reads via `&Registry`
        // (no `&mut`), and Core never re-enters the binding mid-fire.
        let batch_f = {
            let reg = self.registry.lock().expect("registry lock");
            reg.batch_fns.get(&fn_id).cloned()
        };
        if let Some(f) = batch_f {
            let mut reg = self.registry.lock().expect("registry lock");
            let emissions = f(dep_data, &reg);
            let converted: SmallVec<[FnEmission; 2]> = emissions
                .into_iter()
                .map(|e| match e {
                    BenchEmission::Data(v) => FnEmission::Data(reg.intern(v)),
                    BenchEmission::Complete => FnEmission::Complete,
                    BenchEmission::Error(v) => FnEmission::Error(reg.intern(v)),
                })
                .collect();
            return FnResult::Batch {
                emissions: converted,
                tracked: None,
            };
        }

        // Single-emission path: latest-value-per-dep, returns at most one
        // FnResult::Data (or FnResult::Noop).
        let reg = self.registry.lock().expect("registry lock");
        let f = reg
            .fns
            .get(&fn_id)
            .cloned()
            .unwrap_or_else(|| panic!("unknown fn_id {fn_id:?}"));
        let dep_values: Vec<BenchValue> = dep_data
            .iter()
            .map(|db| {
                let h = db.latest();
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

    fn retain_handle(&self, h: HandleId) {
        let mut reg = self.registry.lock().expect("registry lock");
        *reg.refcounts.entry(h).or_insert(0) += 1;
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

/// Built-in batch fn shapes — fire returns `FnResult::Batch` with one
/// emission per dep-batch entry (R1.3.6.b multi-emit).
///
/// Demonstrates the `actions.down(msgs)` substrate through the napi
/// boundary. Real JS-callback batch fns require TSFN and come later.
#[napi(string_enum)]
pub enum BuiltinBatchFn {
    /// For each handle in `dep_data[0].data`, emit `value + 1` as a
    /// `FnEmission::Data`. Demonstrates K-emit-per-fire coalescing through
    /// the FFI: a 3-emit batch on the source produces a 3-emission Batch
    /// result, all delivered in the same wave.
    MapAddOneBatch,
    /// Emit `value * 10` for each input, then a `FnEmission::Complete` —
    /// demonstrates terminal-in-batch (R1.3.4.a: terminals stop further
    /// processing within the same Batch).
    MulTenThenComplete,
}

/// User-side emission shape for the batch surfaces. The kind discriminator
/// is a string so JS callers don't depend on enum integer values.
///
/// - `kind: "data"` — `value` is the i32 payload
/// - `kind: "complete"` — `value` ignored
/// - `kind: "error"` — `value` is the i32 error-code payload
///
/// Cleaner than mirroring the Rust `FnEmission` tagged union directly: at
/// the JS layer one struct shape covers all three variants and TypeScript
/// consumers can narrow on `kind`.
#[napi(object)]
pub struct BatchEmissionJs {
    pub kind: String,
    pub value: Option<i32>,
}

// Note: a `DepBatchJs` napi struct was considered for inspecting per-dep
// wave state from JS. Dropped because there's no consumer yet — the M3
// Slice A regression tests live Rust-side via `register_raw_fn` capture.
// Re-add if a JS-side parity scenario needs to verify R1.3.6.b shape.

// ---------------------------------------------------------------------------
// JS-facing API: a single `BenchCore` class with the minimum to bench.
// ---------------------------------------------------------------------------

#[napi]
pub struct BenchCore {
    core: Core,
    binding: Arc<BenchBinding>,
    /// Holds noop subscriptions so they stay alive for the bench's lifetime.
    /// Per §10.12, dropping a `Subscription` deregisters it; the bench needs
    /// nodes activated for the whole run, so we keep them here.
    subscriptions: Mutex<Vec<Subscription>>,
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
        Self {
            core,
            binding,
            subscriptions: Mutex::new(Vec::new()),
        }
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
        u32::try_from(self.core.register_state(handle).raw()).expect("node id exceeds u32")
    }

    /// Register a state node with sentinel cache (no initial value).
    #[napi]
    pub fn register_state_sentinel(&self) -> u32 {
        u32::try_from(self.core.register_state(graphrefly_core::NO_HANDLE).raw())
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
        let deps: Vec<NodeId> = dep_ids
            .into_iter()
            .map(|id| NodeId::new(u64::from(id)))
            .collect();
        u32::try_from(
            self.core
                .register_derived(&deps, fn_id, EqualsMode::Identity)
                .raw(),
        )
        .expect("node id exceeds u32")
    }

    /// Subscribe a noop sink to a node — required to activate compute nodes.
    /// The Subscription is retained inside `BenchCore` so it lives for the
    /// bench's lifetime (per §10.12 RAII semantics). Returns the index in the
    /// subscriptions vec for tests that want to release one early via
    /// [`Self::unsubscribe_index`].
    #[napi]
    pub fn subscribe_noop(&self, node_id: u32) -> u32 {
        let sink: graphrefly_core::Sink = Arc::new(|_| {});
        let sub = self.core.subscribe(NodeId::new(u64::from(node_id)), sink);
        let mut subs = self.subscriptions.lock().expect("subscriptions lock");
        let idx = subs.len();
        subs.push(sub);
        u32::try_from(idx).expect("subscription index exceeds u32")
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

    // -------------------------------------------------------------------
    // Slice A close (M1) parity — wave-emitting + lifecycle methods
    //
    // Each new public Core method gets a thin wrapper here so JS / TS
    // tests can drive it. Error surfaces use `napi::Error` for the
    // standard JS Error shape; structured Rust errors stringify into
    // the message body.
    //
    // LockId is exposed as `u32` to JS (matches NodeId's napi shape).
    // The Core's u64 LockId space is wider, but tests fit comfortably
    // in u32. Use `alloc_lock_id` for collision-free allocation.
    // -------------------------------------------------------------------

    /// Allocate a fresh `LockId` for use with [`Self::pause`] /
    /// [`Self::resume`]. Core's lock-id space is `u64`; JS gets a `u32`.
    /// If Core has allocated more than `u32::MAX` lock ids over its
    /// lifetime, returns an error rather than silently clamping (which
    /// would alias every subsequent allocation to the same id and cause
    /// cross-controller pause leak — see the `LockId`-collision note in
    /// `porting-deferred.md`).
    #[napi]
    pub fn alloc_lock_id(&self) -> Result<u32, NapiError> {
        u32::try_from(self.core.alloc_lock_id().raw()).map_err(|_| {
            NapiError::from_reason(
                "alloc_lock_id exceeded u32 range — Core has allocated more \
                 than u32::MAX lock ids; bench binding cannot widen without \
                 BigInt — restart the BenchCore or migrate to BigInt-typed binding",
            )
        })
    }

    /// Configure the Core-global pause replay buffer cap. `None`
    /// (no value) means unbounded; messages buffer indefinitely until
    /// the lockset clears. Per-node override is not exposed at this
    /// layer in v1.
    #[napi]
    pub fn set_pause_buffer_cap(&self, cap: Option<u32>) {
        self.core.set_pause_buffer_cap(cap.map(|c| c as usize));
    }

    /// Acquire a pause lock. While paused, tier-3 (DATA/RESOLVED) and
    /// tier-4 (INVALIDATE) outgoing messages buffer in the node's pause
    /// buffer; other tiers flush immediately.
    #[napi]
    pub fn pause(&self, node_id: u32, lock_id: u32) -> Result<(), NapiError> {
        self.core
            .pause(
                NodeId::new(u64::from(node_id)),
                LockId::new(u64::from(lock_id)),
            )
            .map_err(|e| NapiError::from_reason(format!("{e}")))
    }

    /// Release a pause lock. Returns the resume report when the final
    /// lock releases (`replayed` + `dropped` counts); `None` when more
    /// locks are still held.
    #[napi]
    pub fn resume(&self, node_id: u32, lock_id: u32) -> Result<Option<ResumeReportJs>, NapiError> {
        self.core
            .resume(
                NodeId::new(u64::from(node_id)),
                LockId::new(u64::from(lock_id)),
            )
            .map(|opt| {
                opt.map(|r| ResumeReportJs {
                    replayed: r.replayed,
                    dropped: r.dropped,
                })
            })
            .map_err(|e| NapiError::from_reason(format!("{e}")))
    }

    /// True if the node currently holds at least one pause lock.
    #[napi]
    pub fn is_paused(&self, node_id: u32) -> bool {
        self.core.is_paused(NodeId::new(u64::from(node_id)))
    }

    /// Number of pause locks currently held on the node.
    #[napi]
    pub fn pause_lock_count(&self, node_id: u32) -> u32 {
        u32::try_from(self.core.pause_lock_count(NodeId::new(u64::from(node_id))))
            .unwrap_or(u32::MAX)
    }

    /// Test inspector: whether `node_id` currently holds the given `lock_id`.
    #[napi]
    pub fn holds_pause_lock(&self, node_id: u32, lock_id: u32) -> bool {
        self.core.holds_pause_lock(
            NodeId::new(u64::from(node_id)),
            LockId::new(u64::from(lock_id)),
        )
    }

    /// Clear `node_id`'s cache and cascade `[INVALIDATE]` to dependents.
    /// No-op if the node was never populated (R1.4 idempotency).
    #[napi]
    pub fn invalidate(&self, node_id: u32) {
        self.core.invalidate(NodeId::new(u64::from(node_id)));
    }

    /// Emit `[COMPLETE]` (R1.3.4) on `node_id`. After this call the node
    /// is terminal: subsequent emits are silent no-ops, fn no longer
    /// fires, and children's `dep_terminals` slots cascade per Lock 2.B.
    #[napi]
    pub fn complete(&self, node_id: u32) {
        self.core.complete(NodeId::new(u64::from(node_id)));
    }

    /// Emit `[ERROR]` on `node_id` with a primitive i32 error payload.
    /// JS bench-binding limitation: the error is a fresh i32 interned
    /// here (no JS Error object roundtrip). Real bindings that want
    /// arbitrary error payloads will surface a separate path.
    #[napi]
    pub fn error_int(&self, node_id: u32, err_code: i32) {
        let h = self
            .binding
            .registry
            .lock()
            .expect("registry lock")
            .intern(BenchValue::Int(err_code));
        self.core.error(NodeId::new(u64::from(node_id)), h);
    }

    /// Tear `node_id` down (R2.6.4). Auto-prepends COMPLETE if not yet
    /// terminal; cascades through children. Idempotent on duplicate
    /// delivery via `has_received_teardown`.
    #[napi]
    pub fn teardown(&self, node_id: u32) {
        self.core.teardown(NodeId::new(u64::from(node_id)));
    }

    /// Attach `companion` as a meta companion of `parent` (R1.3.9.d).
    /// Companions tear down before their parent in TEARDOWN ordering.
    #[napi]
    pub fn add_meta_companion(&self, parent: u32, companion: u32) {
        self.core.add_meta_companion(
            NodeId::new(u64::from(parent)),
            NodeId::new(u64::from(companion)),
        );
    }

    /// Mark `node_id` as resubscribable per R2.2.7. Resubscribable nodes
    /// reset their terminal-lifecycle state on a fresh subscribe (unless
    /// they have received TEARDOWN). Configuration call — must be made
    /// before the node has any active subscribers.
    #[napi]
    pub fn set_resubscribable(&self, node_id: u32, resubscribable: bool) {
        self.core
            .set_resubscribable(NodeId::new(u64::from(node_id)), resubscribable);
    }

    /// Coalesce multiple emits into a single wave. Emits inside the
    /// closure-equivalent — for the napi binding, we expose the simpler
    /// shape "emit a list of ints in one wave" since closures don't
    /// cross cleanly over FFI.
    ///
    /// On panic in the binding-side, the RAII `BatchGuard` discards
    /// pending tier-3+ work; subscribers do not observe a half-built
    /// wave.
    #[napi]
    pub fn batch_emit_ints(&self, node_id: u32, values: Vec<i32>) {
        let nid = NodeId::new(u64::from(node_id));
        let handles: Vec<HandleId> = {
            let mut reg = self.binding.registry.lock().expect("registry lock");
            values
                .into_iter()
                .map(|v| reg.intern(BenchValue::Int(v)))
                .collect()
        };
        // begin_batch returns a BatchGuard that drops at scope end.
        let _guard = self.core.begin_batch();
        for h in handles {
            self.core.emit(nid, h);
        }
        // _guard drops here → drain + flush.
    }

    /// Atomically rewire a node's deps. New deps are validated for
    /// existence and cycle-freedom; terminal-rejection policy applies
    /// per Phase 13.8 Q1. Returns `Err` with a stringified
    /// `SetDepsError` describing the rejection.
    #[napi]
    pub fn set_deps(&self, node_id: u32, new_dep_ids: Vec<u32>) -> Result<(), NapiError> {
        let n = NodeId::new(u64::from(node_id));
        let new_deps: Vec<NodeId> = new_dep_ids
            .into_iter()
            .map(|id| NodeId::new(u64::from(id)))
            .collect();
        self.core
            .set_deps(n, &new_deps)
            .map_err(|e| NapiError::from_reason(format!("{e}")))
    }

    /// Read whether the node has fired its fn at least once (compute) OR
    /// has had a non-sentinel value (state). Useful as a smoke test for
    /// the activation drain in the new lock-released drain.
    #[napi]
    pub fn has_fired_once(&self, node_id: u32) -> bool {
        self.core.has_fired_once(NodeId::new(u64::from(node_id)))
    }

    // -------------------------------------------------------------------
    // M3 — DepBatch + FnResult::Batch parity surface
    //
    // `register_batch_derived` exposes the FnResult::Batch substrate —
    // a derived node whose fn fire returns multiple emissions (R1.3.6.b
    // coalescing on the output side). `batch_emit_messages` is the
    // user-facing batch: a heterogeneous wave of data/complete/error
    // emissions applied atomically inside a `Core::batch` scope.
    // -------------------------------------------------------------------

    /// Register a derived node whose fn returns `FnResult::Batch` with
    /// one emission per input batch entry. Demonstrates the M3 Slice B
    /// `FnResult::Batch` substrate through the napi boundary.
    ///
    /// The fn fires once per wave; its output emissions all flow within
    /// the same wave. `EqualsMode::Identity` is hardcoded because
    /// `FnResult::Batch` skips equals substitution per R1.3.2.d (multi-
    /// message waves pass through verbatim) — the equals mode only
    /// affects single-emission `FnResult::Data` paths, which a batch fn
    /// doesn't produce.
    ///
    /// # Panics
    ///
    /// Bench builtins (`MapAddOneBatch`, `MulTenThenComplete`) panic if
    /// a dep handle resolves to a non-Int value or is missing from the
    /// registry. These are bench helpers; production batch fns will
    /// surface domain errors via dedicated channels.
    #[napi]
    pub fn register_batch_derived(&self, dep_ids: Vec<u32>, builtin: BuiltinBatchFn) -> u32 {
        let fn_impl: BatchFnImpl = match builtin {
            BuiltinBatchFn::MapAddOneBatch => Arc::new(|deps: &[DepBatch], reg: &Registry| {
                let mut emissions = Vec::new();
                if let Some(d) = deps.first() {
                    for h in &d.data {
                        match reg.deref(*h) {
                            Some(BenchValue::Int(n)) => {
                                emissions.push(BenchEmission::Data(BenchValue::Int(n + 1)));
                            }
                            Some(other) => {
                                panic!("MapAddOneBatch only supports Int dep values, got {other:?}",)
                            }
                            None => panic!("MapAddOneBatch: dep handle {h:?} not in registry"),
                        }
                    }
                }
                emissions
            }),
            BuiltinBatchFn::MulTenThenComplete => Arc::new(|deps: &[DepBatch], reg: &Registry| {
                let mut emissions = Vec::new();
                if let Some(d) = deps.first() {
                    for h in &d.data {
                        match reg.deref(*h) {
                            Some(BenchValue::Int(n)) => {
                                emissions.push(BenchEmission::Data(BenchValue::Int(n * 10)));
                            }
                            Some(other) => panic!(
                                "MulTenThenComplete only supports Int dep values, got {other:?}",
                            ),
                            None => panic!("MulTenThenComplete: dep handle {h:?} not in registry",),
                        }
                    }
                }
                emissions.push(BenchEmission::Complete);
                emissions
            }),
        };
        let mut reg = self.binding.registry.lock().expect("registry lock");
        let fn_id = FnId::new(reg.next_fn_id);
        reg.next_fn_id += 1;
        reg.batch_fns.insert(fn_id, fn_impl);
        drop(reg);
        let deps: Vec<NodeId> = dep_ids
            .into_iter()
            .map(|id| NodeId::new(u64::from(id)))
            .collect();
        u32::try_from(
            self.core
                .register_derived(&deps, fn_id, EqualsMode::Identity)
                .raw(),
        )
        .expect("node id exceeds u32")
    }

    /// User-facing batch — apply a sequence of mixed emissions on a state
    /// node in one wave. Internally uses `Core::batch` to coalesce; each
    /// emission maps to the underlying source-level call (`emit` /
    /// `complete` / `error`) inside the batch scope.
    ///
    /// This is the "user-facing batch" surface: cleaner than mirroring
    /// `FnResult::Batch` directly (which is a fn-fire return shape, not a
    /// caller-driven wave). Heterogeneous waves of mixed emissions are
    /// expressible without exposing the internal `FnEmission` enum to JS.
    ///
    /// # Errors
    ///
    /// Returns `napi::Error` (NOT panic) when:
    /// - Any `kind` is not one of `"data"` / `"complete"` / `"error"`.
    /// - `kind` is `"data"` or `"error"` and `value` is `None` (R1.2.5
    ///   intent: payload must be supplied at construction; silent default
    ///   to `0` would mask user error and produce phantom-zero events
    ///   indistinguishable from legitimate `0` payloads).
    ///
    /// All input is validated up-front BEFORE opening the batch, so a
    /// malformed `msgs` never produces a partially-applied wave.
    #[napi]
    pub fn batch_emit_messages(
        &self,
        node_id: u32,
        msgs: Vec<BatchEmissionJs>,
    ) -> Result<(), NapiError> {
        // Up-front validation — bad input MUST NOT open a batch.
        for (i, m) in msgs.iter().enumerate() {
            match m.kind.as_str() {
                "data" | "error" => {
                    if m.value.is_none() {
                        return Err(NapiError::from_reason(format!(
                            "batch_emit_messages: msgs[{i}] kind={:?} requires a `value` field",
                            m.kind
                        )));
                    }
                }
                "complete" => {}
                other => {
                    return Err(NapiError::from_reason(format!(
                        "batch_emit_messages: msgs[{i}] unknown kind {other:?} (expected \"data\" / \"complete\" / \"error\")",
                    )));
                }
            }
        }

        let nid = NodeId::new(u64::from(node_id));
        self.core.batch(|| {
            for m in msgs {
                match m.kind.as_str() {
                    "data" => {
                        let v = m.value.expect("validated up-front");
                        let h = self
                            .binding
                            .registry
                            .lock()
                            .expect("registry lock")
                            .intern(BenchValue::Int(v));
                        self.core.emit(nid, h);
                    }
                    "complete" => self.core.complete(nid),
                    "error" => {
                        let v = m.value.expect("validated up-front");
                        let h = self
                            .binding
                            .registry
                            .lock()
                            .expect("registry lock")
                            .intern(BenchValue::Int(v));
                        self.core.error(nid, h);
                    }
                    _ => unreachable!("validated up-front"),
                }
            }
        });

        Ok(())
    }
}

/// JS-exposed mirror of [`graphrefly_core::ResumeReport`].
#[napi(object)]
pub struct ResumeReportJs {
    pub replayed: u32,
    pub dropped: u32,
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
