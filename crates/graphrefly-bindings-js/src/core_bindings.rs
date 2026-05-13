//! Core dispatcher bindings — wraps `graphrefly_core::Core` for JS callers.
//!
//! Build profile: only enabled with the `tracing` feature (default), which
//! pulls in `graphrefly-core`.
//!
//! # Design notes — Option E (post-QA refactor 2026-05-07)
//!
//! - **Async napi methods.** Every `#[napi]` method that touches Core is
//!   `async fn`, returning a JS `Promise` automatically via napi-rs's
//!   `tokio_rt` integration. Inside, the body uses
//!   [`napi::tokio_runtime::spawn_blocking`] to run Core's sync wave
//!   engine on a tokio blocking-pool thread. The JS thread stays free
//!   to pump libuv during `await`, so TSFN-backed JS callbacks (in
//!   [`crate::operator_bindings`]) can be delivered without deadlock.
//!
//! - **Rust core stays sync.** The `tokio::task::spawn_blocking` boundary
//!   is the ONLY place tokio enters; everything inside the closure (and
//!   thus all of `graphrefly-core` / `graphrefly-operators` /
//!   `graphrefly-graph`) runs synchronously. CLAUDE.md invariant 4
//!   ("no async runtime in Core") preserved.
//!
//! - **Value registry on the Rust side.** Same as before — primitives
//!   intern via `Registry`. Switched to `parking_lot::Mutex` (M1 fix:
//!   no poisoning; matches `producer_storage`'s flavor).
//!
//! - **Pre-baked derived fns.** `BuiltinFn` / `BuiltinBatchFn` enums.
//!   Real JS-callback fns live on `BenchOperators` (TSFN-backed; M3
//!   napi-rs operator parity).
//!
//! - **Thread-local Core for producer dispatch.** [`CURRENT_CORE`] is
//!   set inside each `spawn_blocking` closure via a RAII guard so
//!   `BindingBoundary::invoke_fn`'s producer-dispatch branch can build a
//!   `ProducerCtx` against the calling thread's `Core` clone. Replaces
//!   the old `worker::WORKER_CORE` thread-local.

use std::cell::RefCell;
use std::sync::mpsc::sync_channel;
use std::sync::{Arc, OnceLock};

use graphrefly_core::{
    BindingBoundary, Core, DepBatch, EqualsMode, FnEmission, FnId, FnResult, HandleId, LockId,
    Message, NodeId, Sink, Subscription,
};
use napi::bindgen_prelude::*;
use napi::threadsafe_function::{
    ThreadsafeCallContext, ThreadsafeFunction, ThreadsafeFunctionCallMode,
};
use napi::{Env, Error as NapiError, Status};
use napi_derive::napi;
use parking_lot::Mutex;
use smallvec::SmallVec;

#[cfg(feature = "operators")]
use graphrefly_operators::producer::{
    default_producer_deactivate, ProducerCtx, ProducerNodeState, ProducerStorage,
};

// ---------------------------------------------------------------------------
// Thread-local Core (replaces worker::WORKER_CORE)
//
// Set inside each `spawn_blocking` closure; cleared by the RAII guard at
// scope exit. `BindingBoundary::invoke_fn`'s producer-dispatch branch
// reads it to construct `ProducerCtx::new(node_id, &core, &storage)`
// without forcing the binding to hold its own `Core` clone (which would
// create an Arc cycle with `Core.binding`).
// ---------------------------------------------------------------------------

thread_local! {
    static CURRENT_CORE: RefCell<Option<Core>> = const { RefCell::new(None) };
}

/// Returns a clone of the current thread's `Core`, if one was installed
/// via [`CoreThreadGuard::set`]. Returns `None` if called from outside a
/// `spawn_blocking` closure (e.g., directly from the JS thread).
#[cfg(feature = "operators")]
pub(crate) fn current_core() -> Option<Core> {
    CURRENT_CORE.with(|c| c.borrow().clone())
}

/// RAII guard for the thread-local `Core`. Sets on construction; clears
/// on Drop. Use at the top of every `spawn_blocking` closure that runs
/// Core operations to make `current_core()` valid for the duration of
/// the closure.
pub(crate) struct CoreThreadGuard;

impl CoreThreadGuard {
    #[must_use]
    pub(crate) fn set(core: Core) -> Self {
        CURRENT_CORE.with(|c| *c.borrow_mut() = Some(core));
        Self
    }
}

impl Drop for CoreThreadGuard {
    fn drop(&mut self) {
        CURRENT_CORE.with(|c| *c.borrow_mut() = None);
    }
}

/// Static no-op sink shared across all `subscribe_noop` calls (M9 fix
/// — was per-call `Arc::new(|_| {})` allocation).
fn noop_sink() -> graphrefly_core::Sink {
    static NOOP: OnceLock<graphrefly_core::Sink> = OnceLock::new();
    NOOP.get_or_init(|| Arc::new(|_| {})).clone()
}

/// Spawn a sync closure on the napi-rs tokio runtime's blocking pool,
/// installing the thread-local `Core` for the duration. Returns the
/// closure's result via the awaited JoinHandle.
///
/// Centralizes (a) the `CoreThreadGuard` install, (b) the `JoinError`
/// → `napi::Error` conversion, (c) the `Result<T>` flattening.
pub(crate) async fn run_blocking<F, R>(core: Core, f: F) -> Result<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    spawn_blocking(move || {
        let _guard = CoreThreadGuard::set(core);
        f()
    })
    .await
    .map_err(|e| NapiError::from_reason(format!("worker thread panicked: {e}")))
}

// ---------------------------------------------------------------------------
// Value registry — owned by the Rust side for v0 FFI bench.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum BenchValue {
    Int(i32),
    #[allow(dead_code)] // reserved for v1 string-value benchmarks
    Str(String),
    /// JS-allocated handle marker (D073). Value lives in the JS-side
    /// adapter registry; this Rust-side variant is only stored for
    /// refcount tracking. `release_handle → 0` for one of these fires
    /// the TSFN registered via `BenchCore::set_release_callback`,
    /// letting the JS-side mirror prune. NOT inserted into
    /// `primitive_index` (every JS-allocated handle is a fresh allocation
    /// — no de-duping by value, since Rust has no value to de-dup on).
    JsAllocated,
}

/// User-side emission shape for the FnResult::Batch builtin fn registry.
#[derive(Clone, Debug)]
enum BenchEmission {
    Data(BenchValue),
    Complete,
    Error(BenchValue),
}

type SingleFnImpl = Arc<dyn Fn(&[BenchValue]) -> Option<BenchValue> + Send + Sync>;
type BatchFnImpl = Arc<dyn Fn(&[DepBatch], &Registry) -> Vec<BenchEmission> + Send + Sync>;

pub(crate) struct Registry {
    next_handle: u64,
    values: ahash::AHashMap<HandleId, BenchValue>,
    primitive_index: ahash::AHashMap<BenchValue, HandleId>,
    pub(crate) refcounts: ahash::AHashMap<HandleId, u64>,
    pub(crate) fns: ahash::AHashMap<FnId, SingleFnImpl>,
    pub(crate) batch_fns: ahash::AHashMap<FnId, BatchFnImpl>,
    pub(crate) next_fn_id: u64,
    pub(crate) projectors: ahash::AHashMap<FnId, Arc<dyn Fn(HandleId) -> HandleId + Send + Sync>>,
    pub(crate) predicates: ahash::AHashMap<FnId, Arc<dyn Fn(HandleId) -> bool + Send + Sync>>,
    pub(crate) folders:
        ahash::AHashMap<FnId, Arc<dyn Fn(HandleId, HandleId) -> HandleId + Send + Sync>>,
    pub(crate) equalses:
        ahash::AHashMap<FnId, Arc<dyn Fn(HandleId, HandleId) -> bool + Send + Sync>>,
    pub(crate) pairwise_packers:
        ahash::AHashMap<FnId, Arc<dyn Fn(HandleId, HandleId) -> HandleId + Send + Sync>>,
    pub(crate) packers: ahash::AHashMap<FnId, Arc<dyn Fn(&[HandleId]) -> HandleId + Send + Sync>>,
    pub(crate) higher_order_projectors:
        ahash::AHashMap<FnId, Arc<dyn Fn(HandleId) -> NodeId + Send + Sync>>,
    /// Producer build closures, looked up by `BenchBinding::invoke_fn`
    /// when a producer node fires. The closure receives a `ProducerCtx`
    /// built against the thread-local `current_core()`.
    #[cfg(feature = "operators")]
    pub(crate) producer_builds: ahash::AHashMap<FnId, Arc<dyn Fn(ProducerCtx<'_>) + Send + Sync>>,
    // Control operator closure registries (Slice U napi parity).
    #[cfg(feature = "operators")]
    pub(crate) tap_fns: ahash::AHashMap<FnId, Arc<dyn Fn(HandleId) + Send + Sync>>,
    #[cfg(feature = "operators")]
    pub(crate) tap_error_fns: ahash::AHashMap<FnId, Arc<dyn Fn(HandleId) + Send + Sync>>,
    #[cfg(feature = "operators")]
    pub(crate) tap_complete_fns: ahash::AHashMap<FnId, Arc<dyn Fn() + Send + Sync>>,
    #[cfg(feature = "operators")]
    pub(crate) rescue_fns: ahash::AHashMap<
        FnId,
        Arc<dyn Fn(HandleId) -> std::result::Result<HandleId, ()> + Send + Sync>,
    >,
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
            projectors: ahash::AHashMap::new(),
            predicates: ahash::AHashMap::new(),
            folders: ahash::AHashMap::new(),
            equalses: ahash::AHashMap::new(),
            pairwise_packers: ahash::AHashMap::new(),
            packers: ahash::AHashMap::new(),
            higher_order_projectors: ahash::AHashMap::new(),
            #[cfg(feature = "operators")]
            producer_builds: ahash::AHashMap::new(),
            #[cfg(feature = "operators")]
            tap_fns: ahash::AHashMap::new(),
            #[cfg(feature = "operators")]
            tap_error_fns: ahash::AHashMap::new(),
            #[cfg(feature = "operators")]
            tap_complete_fns: ahash::AHashMap::new(),
            #[cfg(feature = "operators")]
            rescue_fns: ahash::AHashMap::new(),
        }
    }

    pub(crate) fn intern(&mut self, value: BenchValue) -> HandleId {
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

    /// Allocate a fresh `HandleId` for a value that lives JS-side (D073).
    /// Stores `BenchValue::JsAllocated` as a marker (NOT indexed by value
    /// — every JS-allocated handle is a fresh allocation since Rust has
    /// no value to dedup on). Refcount=1 — caller (JS adapter) takes
    /// ownership of the share, which transfers when the handle is
    /// passed to `register_state_with_handle` / `emit_handle` / etc.
    pub(crate) fn alloc_external_handle(&mut self) -> HandleId {
        let h = HandleId::new(self.next_handle);
        self.next_handle += 1;
        self.values.insert(h, BenchValue::JsAllocated);
        self.refcounts.insert(h, 1);
        h
    }

    /// True if `h` was allocated via `alloc_external_handle` (JS-side).
    /// Currently unused — the JS-prune gate moved into `release` for
    /// atomicity (Phase E /qa F5). Kept as a predicate for future
    /// debug / introspection paths.
    #[allow(dead_code)]
    pub(crate) fn is_js_allocated(&self, h: HandleId) -> bool {
        matches!(self.values.get(&h), Some(BenchValue::JsAllocated))
    }

    pub(crate) fn deref(&self, h: HandleId) -> Option<BenchValue> {
        self.values.get(&h).cloned()
    }

    /// True if `h` is a known handle (has a value mapping). Used by
    /// TSFN closures (C2 / H-01 fix — phantom registry entries on
    /// unknown JS-returned HandleIds).
    pub(crate) fn contains(&self, h: HandleId) -> bool {
        self.values.contains_key(&h)
    }

    /// Validate `h` exists, then bump its refcount. Returns false if
    /// `h` is unknown — caller should panic with a JS-bug diagnostic.
    /// Matches `BindingBoundary::retain_handle`'s pattern
    /// (`.entry(h).or_insert(0) += 1`) for known handles.
    /// (C2 / H-01 / H-02 fix — single-source-of-truth retain pattern.)
    pub(crate) fn validate_and_retain(&mut self, h: HandleId) -> bool {
        if !self.values.contains_key(&h) {
            return false;
        }
        *self.refcounts.entry(h).or_insert(0) += 1;
        true
    }

    /// Decrement refcount for `h` and evict if zero. Returns `true` IFF
    /// (a) the entry was a `JsAllocated` marker AND (b) the post-release
    /// refcount hit zero (i.e., the entry was just evicted). Caller
    /// fires the JS-side prune TSFN on `true` (D076).
    ///
    /// Phase E /qa F5 (2026-05-08): replaced the prior two-phase
    /// snapshot-then-release pattern in `BindingBoundary::release_handle`
    /// (lock-acquire → snapshot → drop lock → lock-acquire → release →
    /// notify) with an atomic single-acquisition variant. The TOCTOU
    /// window between the two acquisitions allowed concurrent
    /// retain/release to flip the JS-prune decision (false-positive
    /// prune → JS adapter loses live values; false-negative prune →
    /// JS mirror leaks).
    fn release(&mut self, h: HandleId) -> bool {
        let was_js_allocated = matches!(self.values.get(&h), Some(BenchValue::JsAllocated));
        let count = self.refcounts.entry(h).or_insert(0);
        if *count > 0 {
            *count -= 1;
        }
        let evicted = *count == 0;
        if evicted {
            if let Some(value) = self.values.remove(&h) {
                if self.primitive_index.get(&value).copied() == Some(h) {
                    self.primitive_index.remove(&value);
                }
            }
            self.refcounts.remove(&h);
        }
        was_js_allocated && evicted
    }
}

/// TSFN that fires JS-side prune notifications when a JS-allocated
/// handle's refcount drops to 0 (D076). Single-arg `(handle: u32)`
/// callback; JS adapter wires this to `Map.delete(handle)`.
/// **Non-blocking call mode** — `release_handle` may run from inside
/// Core's wave (state lock held); we MUST NOT block the Core thread.
/// Late delivery is benign: handle IDs are never reused (allocator is
/// monotonic), so prune-after-Core-drop is a `Map.delete` of an
/// unknown key.
pub(crate) type ReleaseTsfn = ThreadsafeFunction<u32, (), u32, Status, false, false, 1>;

pub(crate) struct BenchBinding {
    pub(crate) registry: Mutex<Registry>,
    /// JS-side prune callback for JS-allocated handles (D076).
    /// `None` until the JS adapter calls `BenchCore::set_release_callback`.
    /// Mutex (not Arc-based swap) because installation is one-shot at
    /// adapter init; the read path on every `release_handle` clones the
    /// Arc<Tsfn> out under a quick lock.
    pub(crate) release_callback: Mutex<Option<Arc<ReleaseTsfn>>>,
    /// `ProducerStorage` for `ProducerBinding` impl. Used by zip/concat/
    /// race/take_until and the higher-order family.
    #[cfg(feature = "operators")]
    pub(crate) producer_storage: ProducerStorage,
}

impl BenchBinding {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            registry: Mutex::new(Registry::new()),
            release_callback: Mutex::new(None),
            #[cfg(feature = "operators")]
            producer_storage: Arc::new(Mutex::new(
                ahash::AHashMap::<NodeId, ProducerNodeState>::new(),
            )),
        })
    }
}

impl BindingBoundary for BenchBinding {
    /// Spec: R5.7 (operator-fn dispatch shape) + R1.3.6.b (multi-emit
    /// batch via `FnResult::Batch`). Producer dispatch (D031) checked
    /// FIRST when `feature = "operators"` is on, so producer fn_ids
    /// don't fall through to the batch_fns / fns lookups.
    fn invoke_fn(&self, _node_id: NodeId, fn_id: FnId, dep_data: &[DepBatch]) -> FnResult {
        // Producer dispatch (Slice D, D031) — try first since producer
        // nodes have empty dep_data and the build closure is keyed by
        // FnId. Only relevant when the operators feature is on.
        #[cfg(feature = "operators")]
        {
            let build = {
                let reg = self.registry.lock();
                reg.producer_builds.get(&fn_id).cloned()
            };
            if let Some(build) = build {
                let core = current_core().expect(
                    "invariant: producer fn fires from inside a `spawn_blocking` closure that \
                     installed the thread-local Core via CoreThreadGuard — current_core() must be Some",
                );
                let ctx = ProducerCtx::new(_node_id, &core, &self.producer_storage);
                build(ctx);
                return FnResult::Noop { tracked: None };
            }
        }

        // Try batch fns first (single-emission path follows).
        let batch_f = {
            let reg = self.registry.lock();
            reg.batch_fns.get(&fn_id).cloned()
        };
        if let Some(f) = batch_f {
            let mut reg = self.registry.lock();
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

        // Single-emission path.
        let reg = self.registry.lock();
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
                let h = self.registry.lock().intern(v);
                FnResult::Data {
                    handle: h,
                    tracked: None,
                }
            }
            None => FnResult::Noop { tracked: None },
        }
    }

    /// Spec: R5.5 (custom-equals semantics) + R1.3.2.d (per-wave
    /// equals coalescing — Slice G). Falls back to identity-equals
    /// (`HandleId u64` compare) when no custom closure is registered;
    /// otherwise dispatches to the closure stored by
    /// `OperatorBinding::register_equals` or
    /// `BenchOperators::register_state_int_with_custom_equals`.
    fn custom_equals(&self, fn_id: FnId, a: HandleId, b: HandleId) -> bool {
        let f = {
            let reg = self.registry.lock();
            reg.equalses.get(&fn_id).cloned()
        };
        match f {
            Some(closure) => closure(a, b),
            None => a == b,
        }
    }

    /// Spec: D016 (handle-protocol cleaving plane — Core sees only
    /// opaque `HandleId` integers; the binding owns the value
    /// registry). Each `release_handle` pairs with one `retain_handle`
    /// or `intern` (which sets refcount=1). Refcount→0 evicts both
    /// `values[h]` and `primitive_index` mapping for that value.
    ///
    /// **D076 — JS-side prune notification:** if `h` was allocated via
    /// `alloc_external_handle` (JsAllocated marker) AND refcount drops
    /// to 0 in this call AND a release_callback TSFN is installed,
    /// fire the TSFN non-blocking with the handle ID. JS adapter
    /// prunes its mirror map. Non-blocking is REQUIRED — this method
    /// may run from inside Core's wave with state lock held; blocking
    /// would deadlock the wave.
    ///
    /// Phase E /qa F5: snapshot-and-release happens under one
    /// `registry.lock()` acquisition via `Registry::release`'s `bool`
    /// return — closes the TOCTOU window where concurrent retain/
    /// release between two separate acquisitions could flip the
    /// JS-prune decision.
    fn release_handle(&self, h: HandleId) {
        let should_notify_js = self.registry.lock().release(h);
        if should_notify_js {
            let cb = self.release_callback.lock().clone();
            if let Some(tsfn) = cb {
                let h_u32 = u32::try_from(h.raw()).unwrap_or(u32::MAX);
                // Non-blocking — fire-and-forget. `_status` ignored
                // because there's no caller to recover; if the queue
                // is full or the TSFN is closed during shutdown, the
                // JS-side mirror just doesn't prune for this handle
                // (benign — handle IDs are never reused).
                // CalleeHandled=false (Slice Y) — `call(value)` takes
                // plain `u32`, not `Result<u32>`.
                let _status = tsfn.call(h_u32, ThreadsafeFunctionCallMode::NonBlocking);
            }
        }
    }

    /// Spec: D016 (refcount-bump pairing with release_handle).
    /// Inserts refcount=1 if `h` was unknown — by design, since this
    /// is called by Core when it takes ownership of a handle returned
    /// from `project_each` / `pack_tuple` etc., and the closure
    /// (D020) pre-bumps for the unknown-handle edge case.
    fn retain_handle(&self, h: HandleId) {
        let mut reg = self.registry.lock();
        *reg.refcounts.entry(h).or_insert(0) += 1;
    }

    // -----------------------------------------------------------------
    // Operator FFI dispatch (Slice C-1 / C-2 / C-3 + Slice E).
    // -----------------------------------------------------------------

    /// Spec: R5.7 (transform-operator dispatch) + D016 (closure-
    /// wrapping discipline — Core invokes a `Fn(HandleId) → HandleId`
    /// closure stored by `OperatorBinding::register_projector`).
    /// Caller (Core) takes ownership of the returned handle's retain
    /// share; the TSFN-wrapped closures bump retain via
    /// `Registry::validate_and_retain` (C2 / C3 fix).
    #[cfg(feature = "operators")]
    fn project_each(&self, fn_id: FnId, inputs: &[HandleId]) -> SmallVec<[HandleId; 1]> {
        let f = {
            let reg = self.registry.lock();
            reg.projectors
                .get(&fn_id)
                .cloned()
                .unwrap_or_else(|| panic!("unknown projector fn_id {fn_id:?}"))
        };
        inputs.iter().map(|h| f(*h)).collect()
    }

    /// Spec: R5.7 (filter / takeWhile / find dispatch) + D016. Returns
    /// per-input bool. No retain-bump (predicate doesn't return a
    /// handle).
    #[cfg(feature = "operators")]
    fn predicate_each(&self, fn_id: FnId, inputs: &[HandleId]) -> SmallVec<[bool; 4]> {
        let f = {
            let reg = self.registry.lock();
            reg.predicates
                .get(&fn_id)
                .cloned()
                .unwrap_or_else(|| panic!("unknown predicate fn_id {fn_id:?}"))
        };
        inputs.iter().map(|h| f(*h)).collect()
    }

    /// Spec: R5.7 (scan / reduce dispatch) + D016. Folds left-to-right
    /// with `acc` carrying forward; returns each intermediate handle
    /// (Scan emits each entry; Reduce uses only the last). Caller owns
    /// retain shares on returned handles (D016).
    #[cfg(feature = "operators")]
    fn fold_each(
        &self,
        fn_id: FnId,
        acc: HandleId,
        inputs: &[HandleId],
    ) -> SmallVec<[HandleId; 1]> {
        let f = {
            let reg = self.registry.lock();
            reg.folders
                .get(&fn_id)
                .cloned()
                .unwrap_or_else(|| panic!("unknown folder fn_id {fn_id:?}"))
        };
        let mut current_acc = acc;
        let mut results: SmallVec<[HandleId; 1]> = SmallVec::with_capacity(inputs.len());
        for h in inputs {
            current_acc = f(current_acc, *h);
            results.push(current_acc);
        }
        results
    }

    /// Spec: R5.7 (pairwise dispatch) + D016. Returns the binding-side
    /// handle for the new tuple value `(prev, current)`. Caller owns
    /// the returned retain share.
    #[cfg(feature = "operators")]
    fn pairwise_pack(&self, fn_id: FnId, prev: HandleId, current: HandleId) -> HandleId {
        let f = {
            let reg = self.registry.lock();
            reg.pairwise_packers
                .get(&fn_id)
                .cloned()
                .unwrap_or_else(|| panic!("unknown pairwise packer fn_id {fn_id:?}"))
        };
        f(prev, current)
    }

    /// Spec: D020 (pre-bumped retain). The closure stored at
    /// `reg.packers[fn_id]` MUST pre-bump the returned handle's retain
    /// — the TSFN-derived `closure_packer` does this via
    /// `validate_and_retain`. Used by `OperatorOp::Combine` /
    /// `WithLatestFrom` and by `ops_impl::zip`. Caller (Core) does NOT
    /// call `retain_handle` on the returned handle.
    #[cfg(feature = "operators")]
    fn pack_tuple(&self, fn_id: FnId, handles: &[HandleId]) -> HandleId {
        let f = {
            let reg = self.registry.lock();
            reg.packers
                .get(&fn_id)
                .cloned()
                .unwrap_or_else(|| panic!("unknown packer fn_id {fn_id:?}"))
        };
        f(handles)
    }

    /// Spec: D031 / D035 (producer lifecycle — Slice D-substrate).
    /// Fired by Core when a producer node loses its last subscriber;
    /// the binding drops the per-node entry from `producer_storage`,
    /// which transitively drops upstream subscriptions via
    /// `Subscription::Drop`. Fires lock-released per D045.
    #[cfg(feature = "operators")]
    fn producer_deactivate(&self, node_id: NodeId) {
        default_producer_deactivate(&self.producer_storage, node_id);
    }

    // -----------------------------------------------------------------
    // Control operator FFI dispatch (Slice U napi parity).
    // -----------------------------------------------------------------

    /// Side-effect tap on DATA: call the registered closure for fn_id.
    #[cfg(feature = "operators")]
    fn invoke_tap_fn(&self, fn_id: FnId, handle: HandleId) {
        let f = {
            let reg = self.registry.lock();
            reg.tap_fns
                .get(&fn_id)
                .cloned()
                .unwrap_or_else(|| panic!("unknown tap fn_id {fn_id:?}"))
        };
        f(handle);
    }

    /// Side-effect tap on ERROR.
    #[cfg(feature = "operators")]
    fn invoke_tap_error_fn(&self, fn_id: FnId, handle: HandleId) {
        let f = {
            let reg = self.registry.lock();
            reg.tap_error_fns
                .get(&fn_id)
                .cloned()
                .unwrap_or_else(|| panic!("unknown tap_error fn_id {fn_id:?}"))
        };
        f(handle);
    }

    /// Side-effect tap on COMPLETE.
    #[cfg(feature = "operators")]
    fn invoke_tap_complete_fn(&self, fn_id: FnId) {
        let f = {
            let reg = self.registry.lock();
            reg.tap_complete_fns
                .get(&fn_id)
                .cloned()
                .unwrap_or_else(|| panic!("unknown tap_complete fn_id {fn_id:?}"))
        };
        f();
    }

    /// Error recovery: call the registered rescue closure. Returns
    /// `Ok(recovered_handle)` on success, `Err(())` if recovery failed.
    #[cfg(feature = "operators")]
    fn invoke_rescue_fn(&self, fn_id: FnId, handle: HandleId) -> std::result::Result<HandleId, ()> {
        let f = {
            let reg = self.registry.lock();
            reg.rescue_fns
                .get(&fn_id)
                .cloned()
                .unwrap_or_else(|| panic!("unknown rescue fn_id {fn_id:?}"))
        };
        f(handle)
    }

    /// Window operators: map a NodeId to a HandleId. Allocates a fresh
    /// handle whose value is the raw NodeId as i32. JS adapter retrieves
    /// it via `deref_int(handle)` to get the inner window's NodeId.
    #[cfg(feature = "operators")]
    fn intern_node(&self, node_id: NodeId) -> HandleId {
        let mut reg = self.registry.lock();
        let raw = u32::try_from(node_id.raw()).expect("NodeId exceeds u32") as i32;
        reg.intern(BenchValue::Int(raw))
    }
}

// ---------------------------------------------------------------------------
// TSFN-backed sink — JS subscribe API (D077 — async-everywhere parity).
//
// The user-side JS sink `(msgs: number[]) => void` receives a flat-encoded
// message batch: alternating (variantCode, payload) u32 pairs. JS adapter
// translates variant codes to canonical message-type symbols (DATA /
// RESOLVED / etc., re-exported from `@graphrefly/pure-ts` per D066)
// and dereferences payload handles via the JS-side mirror map (D073).
//
// **Sync bridge.** Sinks fire from inside a wave running on a tokio
// blocking-pool thread. To preserve "all sinks completed before Promise
// resolves" semantics (so `await impl.subscribe(...)` followed by sync
// `expect(seen)...` works), the sink uses `bridge_sync_unit`: blocks the
// tokio thread on a sync_channel until the JS sink callback returns via
// libuv pump. JS thread MUST be `await`ing the napi method that
// triggered the wave; otherwise the bridge deadlocks (no libuv pump).
//
// **Variant code table** — must match the JS-side decode table in
// `packages/parity-tests/impls/rust.ts`. Don't reorder without updating
// both sides.
// ---------------------------------------------------------------------------

pub(crate) const MSG_CODE_START: u32 = 0;
pub(crate) const MSG_CODE_DIRTY: u32 = 1;
pub(crate) const MSG_CODE_RESOLVED: u32 = 2;
pub(crate) const MSG_CODE_DATA: u32 = 3;
pub(crate) const MSG_CODE_INVALIDATE: u32 = 4;
pub(crate) const MSG_CODE_PAUSE: u32 = 5;
pub(crate) const MSG_CODE_RESUME: u32 = 6;
pub(crate) const MSG_CODE_COMPLETE: u32 = 7;
pub(crate) const MSG_CODE_ERROR: u32 = 8;
pub(crate) const MSG_CODE_TEARDOWN: u32 = 9;

#[inline]
fn encode_message(m: Message) -> (u32, u32) {
    match m {
        Message::Start => (MSG_CODE_START, 0),
        Message::Dirty => (MSG_CODE_DIRTY, 0),
        Message::Resolved => (MSG_CODE_RESOLVED, 0),
        Message::Data(h) => (MSG_CODE_DATA, u32::try_from(h.raw()).unwrap_or(u32::MAX)),
        Message::Invalidate => (MSG_CODE_INVALIDATE, 0),
        Message::Pause(l) => (MSG_CODE_PAUSE, u32::try_from(l.raw()).unwrap_or(u32::MAX)),
        Message::Resume(l) => (MSG_CODE_RESUME, u32::try_from(l.raw()).unwrap_or(u32::MAX)),
        Message::Complete => (MSG_CODE_COMPLETE, 0),
        Message::Error(h) => (MSG_CODE_ERROR, u32::try_from(h.raw()).unwrap_or(u32::MAX)),
        Message::Teardown => (MSG_CODE_TEARDOWN, 0),
    }
}

/// TSFN type for sinks. Single arg `Vec<u32>` (flat-encoded batch); JS
/// callback returns nothing (`()` → undefined). `MaxQueueSize = 1`
/// per D077 sync-bridge invariant. `CalleeHandled = false` (Slice Y) —
/// JS receives `(value)` directly, matching the typed
/// `Function<Vec<u32>, ()>` signature.
type SinkTsfn = ThreadsafeFunction<Vec<u32>, (), Vec<u32>, Status, false, false, 1>;

fn build_sink_tsfn(callback: Function<Vec<u32>, ()>) -> Result<Arc<SinkTsfn>> {
    let tsfn = callback
        .build_threadsafe_function::<Vec<u32>>()
        .max_queue_size::<1>()
        .callee_handled::<false>()
        .build_callback(|ctx: ThreadsafeCallContext<Vec<u32>>| Ok(ctx.value))?;
    Ok(Arc::new(tsfn))
}

/// Fire the TSFN with the encoded batch; block the calling tokio
/// blocking-pool thread until the JS sink callback returns. JS-callback
/// throws panic on the tokio thread (per D065 wave-discard discipline);
/// Core's `BatchGuard::drop` panic path discards the wave cleanly.
///
/// Symmetric with `operator_bindings::bridge_sync` but for `()` return.
fn bridge_sync_unit(tsfn: &Arc<SinkTsfn>, payload: Vec<u32>) {
    let (tx, rx) = sync_channel::<Result<()>>(1);
    // CalleeHandled=false → plain `T` arg (no Result wrapping).
    let status = tsfn.call_with_return_value(
        payload,
        ThreadsafeFunctionCallMode::Blocking,
        move |result: Result<()>, _env: Env| -> Result<()> {
            let _ = tx.send(result);
            Ok(())
        },
    );
    if status != Status::Ok {
        panic!(
            "TSFN sink call_with_return_value failed with status {status:?} (likely TSFN closed during shutdown). \
             Wave panic-discarded via Core's BatchGuard drop discipline."
        );
    }
    match rx.recv() {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            eprintln!("[graphrefly] JS sink callback threw: {e}");
            panic!(
                "JS sink callback threw: {e}. Wave panic-discarded via Core's BatchGuard drop discipline."
            );
        }
        Err(_) => panic!(
            "TSFN sink result channel closed without delivering. \
             Wave panic-discarded via Core's BatchGuard drop discipline."
        ),
    }
}

/// Build a `Sink` closure that delivers each message batch to JS via
/// the TSFN, blocking until JS finishes processing.
fn build_tsfn_sink(tsfn: Arc<SinkTsfn>) -> Sink {
    Arc::new(move |msgs: &[Message]| {
        let mut payload: Vec<u32> = Vec::with_capacity(msgs.len() * 2);
        for m in msgs {
            let (code, arg) = encode_message(*m);
            payload.push(code);
            payload.push(arg);
        }
        bridge_sync_unit(&tsfn, payload);
    })
}

// ---------------------------------------------------------------------------
// Pre-baked fn variants (v0 bench scope — no JS callbacks).
// ---------------------------------------------------------------------------

#[napi(string_enum)]
pub enum BuiltinFn {
    Identity,
    AddOne,
}

#[napi(string_enum)]
pub enum BuiltinBatchFn {
    MapAddOneBatch,
    MulTenThenComplete,
}

#[napi(object)]
pub struct BatchEmissionJs {
    pub kind: String,
    pub value: Option<i32>,
}

// ---------------------------------------------------------------------------
// JS-facing API: `BenchCore` napi class.
//
// Per Option E (post-QA refactor 2026-05-07), every method that touches
// Core is `async fn`. Inside, `run_blocking` (a `napi::tokio_runtime
// ::spawn_blocking` wrapper) installs the thread-local `Core` via
// `CoreThreadGuard` and runs the sync work on tokio's blocking pool.
// JS sees a Promise; libuv on the JS thread is free to drain TSFN ticks
// fired by operator callbacks during the wave.
// ---------------------------------------------------------------------------

#[napi]
pub struct BenchCore {
    /// Subscriptions retained for activation lifetimes. `None` slots
    /// represent unsubscribed entries (we don't compact to keep indices
    /// stable). Drop happens on whatever tokio thread last touches the
    /// slot; safe because `parking_lot` is non-poisoning.
    ///
    /// **Arc-wrapped (D077, Phase E)** — `subscribe_with_tsfn` returns
    /// a `PromiseRaw` whose `spawn_future` closure outlives `&self`;
    /// Arc lets the closure capture a clone for storing the
    /// Subscription after the blocking task resolves.
    ///
    /// **Drop-order discipline (Phase E /qa F8 — was m26):** with the
    /// Arc wrapper, Rust's field-drop order no longer guarantees
    /// `Subscription::Drop` runs strictly BEFORE `core` — any in-flight
    /// `subscribe_with_tsfn` future that hasn't been polled to
    /// completion holds a clone of this Arc, so the Vec can outlive
    /// `BenchCore::core` if the JS side drops the Promise. This is
    /// SAFE because `Subscription` holds a `Weak<Mutex<CoreState>>`
    /// (not a strong `Arc<Core>`) — when the Vec finally drops, each
    /// `Subscription::Drop` either upgrades the Weak (Core still
    /// alive) and releases activation, or no-ops (Core gone, state
    /// already cleaned by the matching binding-side
    /// `producer_deactivate` cascade). The field declaration stays
    /// FIRST in struct order so the COMMON case (no in-flight
    /// futures) drops subscriptions before `core` deterministically.
    pub(crate) subscriptions: Arc<Mutex<Vec<Option<Subscription>>>>,
    pub(crate) binding: Arc<BenchBinding>,
    pub(crate) core: Core,
}

impl BenchCore {
    /// Internal accessor — `BenchOperators::from_core` clones the Core
    /// + binding so the companion class drives the same instance.
    pub(crate) fn core_clone(&self) -> Core {
        self.core.clone()
    }

    pub(crate) fn binding_arc(&self) -> Arc<BenchBinding> {
        Arc::clone(&self.binding)
    }

    /// Clone the subscriptions Arc for capture into futures (D077).
    pub(crate) fn subscriptions_arc(&self) -> Arc<Mutex<Vec<Option<Subscription>>>> {
        Arc::clone(&self.subscriptions)
    }
}

#[napi]
impl BenchCore {
    /// Construct a fresh `BenchCore` wired to a Rust-side value registry.
    #[napi(constructor)]
    #[allow(clippy::new_without_default)]
    #[must_use]
    pub fn new() -> Self {
        let binding = BenchBinding::new();
        let core = Core::new(binding.clone() as Arc<dyn BindingBoundary>);
        Self {
            subscriptions: Arc::new(Mutex::new(Vec::new())),
            binding,
            core,
        }
    }

    // -----------------------------------------------------------------
    // Sync registry helpers — pure binding-side ops, no Core wave.
    // Exposed sync because they don't risk libuv-pump deadlock.
    // -----------------------------------------------------------------

    /// Intern an i32 value; returns its HandleId. Used by JS-side
    /// adapters to construct fresh handles for operator callback returns
    /// (e.g., `register_map(src, x => intern_int(deref_int(x) + 1))`).
    #[napi]
    #[must_use]
    pub fn intern_int(&self, value: i32) -> u32 {
        let h = self.binding.registry.lock().intern(BenchValue::Int(value));
        u32::try_from(h.raw()).expect("handle exceeds u32")
    }

    /// Dereference a HandleId to its i32 payload. Returns -1 for
    /// unknown handles or non-Int values (matches `cache_int`'s
    /// sentinel convention for the bench surface).
    #[napi]
    #[must_use]
    pub fn deref_int(&self, handle: u32) -> i32 {
        let h = HandleId::new(u64::from(handle));
        let reg = self.binding.registry.lock();
        match reg.deref(h) {
            Some(BenchValue::Int(n)) => n,
            _ => -1,
        }
    }

    // -----------------------------------------------------------------
    // Handle-passthrough surface (D073 — JS-side value registry).
    //
    // The JS adapter (`packages/parity-tests/impls/rust.ts`) holds a
    // `Map<u32, T>` mirror for arbitrary `T`. Rust binding stays
    // handle-opaque: it only refcount-tracks the JS-allocated handles
    // via `BenchValue::JsAllocated` markers. When refcount drops to 0
    // for a JS-allocated handle, `BenchBinding::release_handle` fires
    // the TSFN registered via `set_release_callback` so the JS adapter
    // can prune its mirror (D076).
    // -----------------------------------------------------------------

    /// Allocate a fresh handle for the JS adapter to associate with a
    /// JS-side value (D073). Returns the handle ID; refcount=1.
    /// **Caller responsibility:** the JS adapter MUST register the
    /// value in its mirror map BEFORE handing the handle to anything
    /// that may dereference it (operator callbacks, sinks).
    /// Sync — pure Registry op, no Core wave.
    #[napi]
    #[must_use]
    pub fn alloc_external_handle(&self) -> u32 {
        let h = self.binding.registry.lock().alloc_external_handle();
        u32::try_from(h.raw()).expect("handle exceeds u32")
    }

    /// Register a state node initialized with a JS-allocated handle.
    /// JS adapter must have called `alloc_external_handle` and stored
    /// the value in its mirror first. The handle's refcount=1 share is
    /// consumed by the state node's initial cache slot.
    #[napi]
    pub async fn register_state_with_handle(&self, initial_handle: u32) -> Result<u32> {
        let core = self.core.clone();
        run_blocking(core.clone(), move || -> Result<u32> {
            let h = HandleId::new(u64::from(initial_handle));
            let id = core
                .register_state(h, false)
                .map_err(|e| NapiError::from_reason(format!("{e}")))?;
            u32::try_from(id.raw()).map_err(|_| NapiError::from_reason("node id exceeds u32"))
        })
        .await?
    }

    /// Emit a JS-allocated handle on a node. JS adapter must have
    /// stored the value in its mirror first. The handle's refcount
    /// share is consumed by Core's emission path (Core retains the
    /// handle into the cache slot).
    #[napi]
    pub async fn emit_handle(&self, node_id: u32, handle: u32) -> Result<()> {
        let core = self.core.clone();
        run_blocking(core.clone(), move || {
            core.emit(
                NodeId::new(u64::from(node_id)),
                HandleId::new(u64::from(handle)),
            );
        })
        .await
    }

    /// Read a node's current cache as a HandleId. Returns 0
    /// (NO_HANDLE) for sentinel cache. JS adapter looks up the handle
    /// in its mirror map.
    #[napi]
    pub async fn cache_handle(&self, node_id: u32) -> Result<u32> {
        let core = self.core.clone();
        let h = run_blocking(core.clone(), move || {
            core.cache_of(NodeId::new(u64::from(node_id)))
        })
        .await?;
        u32::try_from(h.raw()).map_err(|_| NapiError::from_reason("handle exceeds u32"))
    }

    // -----------------------------------------------------------------
    // Async Core-touching methods (Promise-returning; non-deadlocking).
    // -----------------------------------------------------------------

    /// Register a state node with an i32 initial value.
    #[napi]
    pub async fn register_state_int(&self, initial: i32) -> Result<u32> {
        let binding = Arc::clone(&self.binding);
        let core = self.core.clone();
        run_blocking(core.clone(), move || -> Result<u32> {
            let handle = binding.registry.lock().intern(BenchValue::Int(initial));
            let id = core
                .register_state(handle, false)
                .map_err(|e| NapiError::from_reason(format!("{e}")))?;
            u32::try_from(id.raw()).map_err(|_| NapiError::from_reason("node id exceeds u32"))
        })
        .await?
    }

    /// Register a state node with sentinel cache (no initial value).
    #[napi]
    pub async fn register_state_sentinel(&self) -> Result<u32> {
        let core = self.core.clone();
        run_blocking(core.clone(), move || -> Result<u32> {
            let id = core
                .register_state(graphrefly_core::NO_HANDLE, false)
                .map_err(|e| NapiError::from_reason(format!("{e}")))?;
            u32::try_from(id.raw()).map_err(|_| NapiError::from_reason("node id exceeds u32"))
        })
        .await?
    }

    /// Register a derived node with a built-in fn shape.
    #[napi]
    pub async fn register_derived(&self, dep_ids: Vec<u32>, builtin: BuiltinFn) -> Result<u32> {
        let binding = Arc::clone(&self.binding);
        let core = self.core.clone();
        run_blocking(core.clone(), move || -> Result<u32> {
            let fn_impl: Arc<dyn Fn(&[BenchValue]) -> Option<BenchValue> + Send + Sync> =
                match builtin {
                    BuiltinFn::Identity => Arc::new(|deps: &[BenchValue]| deps.first().cloned()),
                    BuiltinFn::AddOne => Arc::new(|deps: &[BenchValue]| match deps.first() {
                        Some(BenchValue::Int(n)) => Some(BenchValue::Int(n + 1)),
                        _ => None,
                    }),
                };
            let mut reg = binding.registry.lock();
            let fn_id = FnId::new(reg.next_fn_id);
            reg.next_fn_id += 1;
            reg.fns.insert(fn_id, fn_impl);
            drop(reg);
            let deps: Vec<NodeId> = dep_ids
                .into_iter()
                .map(|id| NodeId::new(u64::from(id)))
                .collect();
            let id = core
                .register_derived(&deps, fn_id, EqualsMode::Identity, false)
                .map_err(|e| NapiError::from_reason(format!("{e}")))?;
            u32::try_from(id.raw()).map_err(|_| NapiError::from_reason("node id exceeds u32"))
        })
        .await?
    }

    /// Subscribe a noop sink. Activation may fire fns (including
    /// JS-callback operators), so this MUST be async.
    #[napi]
    pub async fn subscribe_noop(&self, node_id: u32) -> Result<u32> {
        let core = self.core.clone();
        // Subscriptions live on `BenchCore`'s `subscriptions` Mutex; we
        // can't move that ref into spawn_blocking (it's tied to &self).
        // Instead, build the Subscription inside the closure, ship it
        // back, and store on the JS-thread side after the spawn_blocking
        // returns. Subscription is `Send` so this is fine.
        //
        // M9 fix: noop sink is a static `OnceLock<Sink>` shared across
        // all subscribe_noop calls — was per-call `Arc::new(|_| {})`.
        let sub = run_blocking(core.clone(), move || -> Subscription {
            core.subscribe(NodeId::new(u64::from(node_id)), noop_sink())
        })
        .await?;
        let mut subs = self.subscriptions.lock();
        // Phase E /qa F7 (2026-05-08): validate idx BEFORE push so
        // overflow drops `sub` (auto-unsubscribes via Drop) instead of
        // leaking a Subscription into a slot that no JS caller can
        // address.
        let idx_u32 = u32::try_from(subs.len())
            .map_err(|_| NapiError::from_reason("subscription index exceeds u32"))?;
        subs.push(Some(sub));
        Ok(idx_u32)
    }

    /// Subscribe with a JS callback sink (D077). The callback receives
    /// a flat-encoded `Vec<u32>` per message batch — alternating
    /// `(variantCode, payload)` pairs. JS adapter decodes via the
    /// `MSG_CODE_*` table (mirrored in `rust.ts`) and dereferences
    /// payload handles via the JS-side mirror map.
    ///
    /// **Sync-bridge semantics:** the returned Promise resolves AFTER
    /// the subscribe-time handshake fires AND the JS sink callback
    /// completes (per `bridge_sync_unit` blocking the tokio thread on
    /// a sync_channel until JS responds via libuv pump). JS code
    /// MUST `await` this method — synchronous calling deadlocks.
    ///
    /// `pub fn` (not `async fn`) because `Function<>` is `!Send`; the
    /// async work runs inside `env.spawn_future(async move { ... })`.
    #[napi]
    pub fn subscribe_with_tsfn<'env>(
        &self,
        env: &'env Env,
        node_id: u32,
        sink_callback: Function<Vec<u32>, ()>,
    ) -> Result<PromiseRaw<'env, u32>> {
        let tsfn = build_sink_tsfn(sink_callback)?;
        let sink = build_tsfn_sink(tsfn);
        let core = self.core.clone();
        let subs = self.subscriptions_arc();
        env.spawn_future(async move {
            let core_for_blocking = core.clone();
            let sub = run_blocking(core, move || -> Subscription {
                core_for_blocking.subscribe(NodeId::new(u64::from(node_id)), sink)
            })
            .await?;
            let mut s = subs.lock();
            // Phase E /qa F7: validate-before-push (see subscribe_noop).
            let idx_u32 = u32::try_from(s.len())
                .map_err(|_| NapiError::from_reason("subscription index exceeds u32"))?;
            s.push(Some(sub));
            Ok(idx_u32)
        })
    }

    /// Install the JS-side prune callback (D076). Called once at JS
    /// adapter construction. Subsequent calls overwrite. The callback
    /// receives a single `(handle: u32)` and should `Map.delete(handle)`
    /// in the JS-side mirror. Fired NON-BLOCKING from
    /// `BindingBoundary::release_handle`.
    #[napi]
    pub fn set_release_callback(&self, callback: Function<'_, u32, ()>) -> Result<()> {
        let tsfn = callback
            .build_threadsafe_function::<u32>()
            .max_queue_size::<1>()
            .callee_handled::<false>()
            .build_callback(|ctx: ThreadsafeCallContext<u32>| Ok(ctx.value))?;
        *self.binding.release_callback.lock() = Some(Arc::new(tsfn));
        Ok(())
    }

    /// Drop the subscription at `idx` (releases activation refcount).
    /// Async because Subscription's Drop locks Core's mutex; running
    /// on a tokio blocking thread keeps the JS thread free during any
    /// terminal-cascade work the unsubscribe triggers.
    #[napi]
    pub async fn unsubscribe(&self, idx: u32) -> Result<()> {
        // Take the Subscription out of the slot on the calling thread,
        // ship it into spawn_blocking for drop. This way the actual
        // Drop happens on a tokio blocking thread (which is allowed to
        // block on Core's mutex).
        let sub = {
            let mut subs = self.subscriptions.lock();
            subs.get_mut(idx as usize).and_then(Option::take)
        };
        if let Some(sub) = sub {
            let core = self.core.clone();
            run_blocking(core, move || drop(sub)).await?;
        }
        Ok(())
    }

    /// Drain all retained subscriptions on a tokio blocking thread —
    /// JS code MUST `await core.dispose()` before letting `BenchCore`
    /// drop. Closes the BenchCore::Drop deadlock vector (Slice Y):
    /// without `dispose`, GC of the napi instance runs `Drop` on the
    /// JS thread; each `Subscription::Drop` blocks on `Core`'s mutex;
    /// if a tokio blocking-pool thread is mid-wave (parked in a TSFN
    /// bridge waiting for libuv to pump a JS-callback result), the JS
    /// thread waiting for the mutex stalls libuv → TSFN never
    /// delivers → tokio thread blocks forever. Calling `dispose` first
    /// ships the subscription drop work onto a tokio blocking thread
    /// (which is allowed to block on the mutex while the JS thread
    /// stays free to pump libuv).
    ///
    /// Idempotent: subsequent calls are no-ops once the vec is drained.
    #[napi]
    pub async fn dispose(&self) -> Result<()> {
        // Take the entire Vec out on the calling thread (cheap — Vec
        // pointer swap). Ship into spawn_blocking for drop so each
        // Subscription's Drop runs on a tokio thread that can block
        // on Core's mutex without stalling libuv.
        let subs: Vec<Option<Subscription>> = {
            let mut guard = self.subscriptions.lock();
            std::mem::take(&mut *guard)
        };
        if !subs.is_empty() {
            let core = self.core.clone();
            run_blocking(core, move || drop(subs)).await?;
        }
        Ok(())
    }

    /// Emit an i32 value on a state node.
    #[napi]
    pub async fn emit_int(&self, node_id: u32, value: i32) -> Result<()> {
        let binding = Arc::clone(&self.binding);
        let core = self.core.clone();
        run_blocking(core.clone(), move || {
            let handle = binding.registry.lock().intern(BenchValue::Int(value));
            core.emit(NodeId::new(u64::from(node_id)), handle);
        })
        .await
    }

    /// Read a state node's current cache as i32.
    #[napi]
    pub async fn cache_int(&self, node_id: u32) -> Result<i32> {
        let binding = Arc::clone(&self.binding);
        let core = self.core.clone();
        run_blocking(core.clone(), move || -> i32 {
            let h = core.cache_of(NodeId::new(u64::from(node_id)));
            if h == graphrefly_core::NO_HANDLE {
                return -1;
            }
            let reg = binding.registry.lock();
            match reg.deref(h) {
                Some(BenchValue::Int(n)) => n,
                _ => -1,
            }
        })
        .await
    }

    /// Tight loop: emit `n` times on a state node, no JS call between.
    #[napi]
    pub async fn rust_emit_loop(&self, node_id: u32, n: u32) -> Result<()> {
        let binding = Arc::clone(&self.binding);
        let core = self.core.clone();
        run_blocking(core.clone(), move || {
            let nid = NodeId::new(u64::from(node_id));
            let mut reg = binding.registry.lock();
            let handles: Vec<HandleId> = (0..n)
                .map(|i| reg.intern(BenchValue::Int(i as i32)))
                .collect();
            drop(reg);
            for h in handles {
                core.emit(nid, h);
            }
        })
        .await
    }

    // -------------------------------------------------------------------
    // Slice A close (M1) parity — wave-emitting + lifecycle methods.
    // -------------------------------------------------------------------

    #[napi]
    pub async fn alloc_lock_id(&self) -> Result<u32> {
        let core = self.core.clone();
        let raw = run_blocking(core.clone(), move || core.alloc_lock_id().raw()).await?;
        u32::try_from(raw).map_err(|_| {
            NapiError::from_reason(
                "alloc_lock_id exceeded u32 range — restart BenchCore or migrate to BigInt",
            )
        })
    }

    #[napi]
    pub async fn set_pause_buffer_cap(&self, cap: Option<u32>) -> Result<()> {
        let core = self.core.clone();
        run_blocking(core.clone(), move || {
            core.set_pause_buffer_cap(cap.map(|c| c as usize));
        })
        .await
    }

    #[napi]
    pub async fn pause(&self, node_id: u32, lock_id: u32) -> Result<()> {
        let core = self.core.clone();
        run_blocking(core.clone(), move || {
            core.pause(
                NodeId::new(u64::from(node_id)),
                LockId::new(u64::from(lock_id)),
            )
        })
        .await?
        .map_err(|e| NapiError::from_reason(format!("{e}")))
    }

    #[napi]
    pub async fn resume(&self, node_id: u32, lock_id: u32) -> Result<Option<ResumeReportJs>> {
        let core = self.core.clone();
        let result = run_blocking(core.clone(), move || {
            core.resume(
                NodeId::new(u64::from(node_id)),
                LockId::new(u64::from(lock_id)),
            )
        })
        .await?;
        result
            .map(|opt| {
                opt.map(|r| ResumeReportJs {
                    replayed: r.replayed,
                    dropped: r.dropped,
                })
            })
            .map_err(|e| NapiError::from_reason(format!("{e}")))
    }

    #[napi]
    pub async fn is_paused(&self, node_id: u32) -> Result<bool> {
        let core = self.core.clone();
        run_blocking(core.clone(), move || {
            core.is_paused(NodeId::new(u64::from(node_id)))
        })
        .await
    }

    /// Number of pause locks currently held. Returns Err on overflow
    /// instead of saturating silently (M8 fix — matches `alloc_lock_id`).
    #[napi]
    pub async fn pause_lock_count(&self, node_id: u32) -> Result<u32> {
        let core = self.core.clone();
        let count = run_blocking(core.clone(), move || {
            core.pause_lock_count(NodeId::new(u64::from(node_id)))
        })
        .await?;
        u32::try_from(count).map_err(|_| NapiError::from_reason("pause_lock_count exceeds u32"))
    }

    #[napi]
    pub async fn holds_pause_lock(&self, node_id: u32, lock_id: u32) -> Result<bool> {
        let core = self.core.clone();
        run_blocking(core.clone(), move || {
            core.holds_pause_lock(
                NodeId::new(u64::from(node_id)),
                LockId::new(u64::from(lock_id)),
            )
        })
        .await
    }

    #[napi]
    pub async fn invalidate(&self, node_id: u32) -> Result<()> {
        let core = self.core.clone();
        run_blocking(core.clone(), move || {
            core.invalidate(NodeId::new(u64::from(node_id)));
        })
        .await
    }

    #[napi]
    pub async fn complete(&self, node_id: u32) -> Result<()> {
        let core = self.core.clone();
        run_blocking(core.clone(), move || {
            core.complete(NodeId::new(u64::from(node_id)));
        })
        .await
    }

    /// Emit ERROR with a JS-allocated handle (D076 parity — non-i32 error
    /// payloads). JS adapter must have called `alloc_external_handle` and
    /// stored the value first. The handle's refcount share is consumed by
    /// Core's error path.
    #[napi]
    pub async fn error_handle(&self, node_id: u32, handle: u32) -> Result<()> {
        let core = self.core.clone();
        run_blocking(core.clone(), move || {
            core.error(
                NodeId::new(u64::from(node_id)),
                HandleId::new(u64::from(handle)),
            );
        })
        .await
    }

    #[napi]
    pub async fn error_int(&self, node_id: u32, err_code: i32) -> Result<()> {
        let binding = Arc::clone(&self.binding);
        let core = self.core.clone();
        run_blocking(core.clone(), move || {
            let h = binding.registry.lock().intern(BenchValue::Int(err_code));
            core.error(NodeId::new(u64::from(node_id)), h);
        })
        .await
    }

    #[napi]
    pub async fn teardown(&self, node_id: u32) -> Result<()> {
        let core = self.core.clone();
        run_blocking(core.clone(), move || {
            core.teardown(NodeId::new(u64::from(node_id)));
        })
        .await
    }

    #[napi]
    pub async fn add_meta_companion(&self, parent: u32, companion: u32) -> Result<()> {
        let core = self.core.clone();
        run_blocking(core.clone(), move || {
            core.add_meta_companion(
                NodeId::new(u64::from(parent)),
                NodeId::new(u64::from(companion)),
            );
        })
        .await
    }

    #[napi]
    pub async fn set_resubscribable(&self, node_id: u32, resubscribable: bool) -> Result<()> {
        let core = self.core.clone();
        run_blocking(core.clone(), move || {
            core.set_resubscribable(NodeId::new(u64::from(node_id)), resubscribable);
        })
        .await
    }

    #[napi]
    pub async fn batch_emit_ints(&self, node_id: u32, values: Vec<i32>) -> Result<()> {
        let binding = Arc::clone(&self.binding);
        let core = self.core.clone();
        run_blocking(core.clone(), move || {
            let nid = NodeId::new(u64::from(node_id));
            let handles: Vec<HandleId> = {
                let mut reg = binding.registry.lock();
                values
                    .into_iter()
                    .map(|v| reg.intern(BenchValue::Int(v)))
                    .collect()
            };
            let _guard = core.begin_batch();
            for h in handles {
                core.emit(nid, h);
            }
        })
        .await
    }

    #[napi]
    pub async fn set_deps(&self, node_id: u32, new_dep_ids: Vec<u32>) -> Result<()> {
        let core = self.core.clone();
        let result = run_blocking(core.clone(), move || {
            let n = NodeId::new(u64::from(node_id));
            let new_deps: Vec<NodeId> = new_dep_ids
                .into_iter()
                .map(|id| NodeId::new(u64::from(id)))
                .collect();
            core.set_deps(n, &new_deps)
        })
        .await?;
        result.map_err(|e| NapiError::from_reason(format!("{e}")))
    }

    #[napi]
    pub async fn has_fired_once(&self, node_id: u32) -> Result<bool> {
        let core = self.core.clone();
        run_blocking(core.clone(), move || {
            core.has_fired_once(NodeId::new(u64::from(node_id)))
        })
        .await
    }

    // -------------------------------------------------------------------
    // M3 — DepBatch + FnResult::Batch parity surface.
    // -------------------------------------------------------------------

    /// Register a derived node whose fn returns `FnResult::Batch`.
    /// R1.3.6.b — multi-emit per fire flows in same wave.
    #[napi]
    pub async fn register_batch_derived(
        &self,
        dep_ids: Vec<u32>,
        builtin: BuiltinBatchFn,
    ) -> Result<u32> {
        let binding = Arc::clone(&self.binding);
        let core = self.core.clone();
        run_blocking(core.clone(), move || -> Result<u32> {
            let fn_impl: BatchFnImpl = match builtin {
                BuiltinBatchFn::MapAddOneBatch => Arc::new(|deps: &[DepBatch], reg: &Registry| {
                    let mut emissions = Vec::new();
                    if let Some(d) = deps.first() {
                        for h in &d.data {
                            match reg.deref(*h) {
                                Some(BenchValue::Int(n)) => {
                                    emissions.push(BenchEmission::Data(BenchValue::Int(n + 1)));
                                }
                                Some(other) => panic!(
                                    "MapAddOneBatch only supports Int dep values, got {other:?}"
                                ),
                                None => panic!("MapAddOneBatch: dep handle {h:?} not in registry"),
                            }
                        }
                    }
                    emissions
                }),
                BuiltinBatchFn::MulTenThenComplete => {
                    Arc::new(|deps: &[DepBatch], reg: &Registry| {
                        let mut emissions = Vec::new();
                        if let Some(d) = deps.first() {
                            for h in &d.data {
                                match reg.deref(*h) {
                                    Some(BenchValue::Int(n)) => {
                                        emissions
                                            .push(BenchEmission::Data(BenchValue::Int(n * 10)));
                                    }
                                    Some(other) => panic!(
                                        "MulTenThenComplete only supports Int dep values, got {other:?}"
                                    ),
                                    None => panic!(
                                        "MulTenThenComplete: dep handle {h:?} not in registry"
                                    ),
                                }
                            }
                        }
                        emissions.push(BenchEmission::Complete);
                        emissions
                    })
                }
            };
            let mut reg = binding.registry.lock();
            let fn_id = FnId::new(reg.next_fn_id);
            reg.next_fn_id += 1;
            reg.batch_fns.insert(fn_id, fn_impl);
            drop(reg);
            let deps: Vec<NodeId> = dep_ids
                .into_iter()
                .map(|id| NodeId::new(u64::from(id)))
                .collect();
            let id = core
                .register_derived(&deps, fn_id, EqualsMode::Identity, false)
                .map_err(|e| NapiError::from_reason(format!("{e}")))?;
            u32::try_from(id.raw()).map_err(|_| NapiError::from_reason("node id exceeds u32"))
        })
        .await?
    }

    /// Batch-dispatch a handle-encoded message sequence atomically (Phase E
    /// /qa F2 — D077 "one call = one wave"). The `encoded` argument is a
    /// flat `[code_0, payload_0, code_1, payload_1, ...]` array using the
    /// same `MSG_CODE_*` table that `subscribe_with_tsfn` decodes — so
    /// `RustNode.down(msgs)` can encode once on the JS side and replay
    /// the whole batch through one Core wave.
    ///
    /// Codes accepted:
    /// - 1 (DIRTY) — Core auto-emits DIRTY before tier-3 emissions; an
    ///   explicit DIRTY in `encoded` is a no-op (matches legacy semantics).
    /// - 3 (DATA) — payload is a JS-allocated handle (D073).
    /// - 4 (INVALIDATE) — no payload.
    /// - 5 (PAUSE) — payload is a lock id.
    /// - 6 (RESUME) — payload is a lock id.
    /// - 7 (COMPLETE) — no payload.
    /// - 8 (ERROR) — payload is a JS-allocated handle.
    /// - 9 (TEARDOWN) — no payload.
    ///
    /// Codes 0 (Start), 2 (Resolved) are not accepted on the down-path
    /// (legacy parity — Start is per-subscription, Resolved is generated
    /// by Core's equals path).
    #[napi]
    pub async fn batch_emit_handle_messages(&self, node_id: u32, encoded: Vec<u32>) -> Result<()> {
        if encoded.len() % 2 != 0 {
            return Err(NapiError::from_reason(format!(
                "batch_emit_handle_messages: encoded length must be even (alternating code, payload pairs); got {}",
                encoded.len()
            )));
        }
        // Up-front validation — bad input MUST NOT open a batch.
        for chunk in encoded.chunks_exact(2) {
            let code = chunk[0];
            match code {
                MSG_CODE_DIRTY | MSG_CODE_DATA | MSG_CODE_INVALIDATE | MSG_CODE_PAUSE
                | MSG_CODE_RESUME | MSG_CODE_COMPLETE | MSG_CODE_ERROR | MSG_CODE_TEARDOWN => {}
                MSG_CODE_START | MSG_CODE_RESOLVED => {
                    return Err(NapiError::from_reason(format!(
                        "batch_emit_handle_messages: code {code} (START/RESOLVED) not valid on the down-path",
                    )));
                }
                _ => {
                    return Err(NapiError::from_reason(format!(
                        "batch_emit_handle_messages: unknown message code {code}",
                    )));
                }
            }
        }

        let core = self.core.clone();
        run_blocking(core.clone(), move || {
            let nid = NodeId::new(u64::from(node_id));
            core.batch(|| {
                for chunk in encoded.chunks_exact(2) {
                    let code = chunk[0];
                    let payload = chunk[1];
                    match code {
                        MSG_CODE_DIRTY => { /* Core auto-emits; no-op */ }
                        MSG_CODE_DATA => {
                            core.emit(nid, HandleId::new(u64::from(payload)));
                        }
                        MSG_CODE_INVALIDATE => core.invalidate(nid),
                        MSG_CODE_PAUSE => {
                            // Ignore PauseError — matches `RustNode.pause` shape;
                            // explicit pause errors surface via direct napi calls.
                            let _ = core.pause(nid, LockId::new(u64::from(payload)));
                        }
                        MSG_CODE_RESUME => {
                            let _ = core.resume(nid, LockId::new(u64::from(payload)));
                        }
                        MSG_CODE_COMPLETE => core.complete(nid),
                        MSG_CODE_ERROR => {
                            core.error(nid, HandleId::new(u64::from(payload)));
                        }
                        MSG_CODE_TEARDOWN => core.teardown(nid),
                        _ => unreachable!("validated up-front"),
                    }
                }
            });
        })
        .await
    }

    #[napi]
    pub async fn batch_emit_messages(
        &self,
        node_id: u32,
        msgs: Vec<BatchEmissionJs>,
    ) -> Result<()> {
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

        let binding = Arc::clone(&self.binding);
        let core = self.core.clone();
        run_blocking(core.clone(), move || {
            let nid = NodeId::new(u64::from(node_id));
            core.batch(|| {
                for m in msgs {
                    match m.kind.as_str() {
                        "data" => {
                            let v = m.value.expect("validated up-front");
                            let h = binding.registry.lock().intern(BenchValue::Int(v));
                            core.emit(nid, h);
                        }
                        "complete" => core.complete(nid),
                        "error" => {
                            let v = m.value.expect("validated up-front");
                            let h = binding.registry.lock().intern(BenchValue::Int(v));
                            core.error(nid, h);
                        }
                        _ => unreachable!("validated up-front"),
                    }
                }
            });
        })
        .await
    }
}

/// JS-exposed mirror of [`graphrefly_core::ResumeReport`].
#[napi(object)]
pub struct ResumeReportJs {
    pub replayed: u32,
    pub dropped: u32,
}

// ---------------------------------------------------------------------------
// Shared singleton — for cross-Worker bench (legacy; kept for back-compat).
// Each global call routes through the same `BenchCore`'s tokio dispatch.
// ---------------------------------------------------------------------------

static GLOBAL_CORE: OnceLock<Arc<BenchCore>> = OnceLock::new();

fn global() -> Arc<BenchCore> {
    GLOBAL_CORE
        .get_or_init(|| Arc::new(BenchCore::new()))
        .clone()
}

#[napi]
pub async fn global_register_state_int(initial: i32) -> Result<u32> {
    global().register_state_int(initial).await
}

#[napi]
pub async fn global_register_derived_identity(dep_ids: Vec<u32>) -> Result<u32> {
    global()
        .register_derived(dep_ids, BuiltinFn::Identity)
        .await
}

#[napi]
pub async fn global_subscribe_noop(node_id: u32) -> Result<u32> {
    global().subscribe_noop(node_id).await
}

#[napi]
pub async fn global_emit_int(node_id: u32, value: i32) -> Result<()> {
    global().emit_int(node_id, value).await
}

#[napi]
pub async fn global_cache_int(node_id: u32) -> Result<i32> {
    global().cache_int(node_id).await
}

#[napi]
pub async fn global_rust_emit_loop(node_id: u32, n: u32) -> Result<()> {
    global().rust_emit_loop(node_id, n).await
}
