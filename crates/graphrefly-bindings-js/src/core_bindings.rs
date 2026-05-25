//! Core dispatcher bindings — wraps `graphrefly_core::Core` for JS callers.
//!
//! Build profile: only enabled with the `tracing` feature (default), which
//! pulls in `graphrefly-core`.
//!
//! # Design notes — α/γ-converged actor model (S6/D255, 2026-05-19)
//!
//! Post-S2c (D247/D248/D249) `Core` is `!Send + !Sync` (substrate `Sink`/
//! `TopologySink` dropped `Send + Sync`); `impl Clone for Core` is deleted
//! (D221/D246). The previous `Bench* { core: Core }` + `self.core.clone()`
//! + `napi::tokio_runtime::spawn_blocking(move || { core... })` shape
//! fails to compile against the post-S2c substrate.
//!
//! - **`Bench*` wrappers store `Arc<CoreActor>`** (S6/D255). The actor
//!   owns `Core` on a dedicated `std::thread::spawn`-ed worker thread
//!   for the actor's lifetime; napi methods send `FnOnce(&Core) + Send
//!   + 'static` closures into the actor's [`crossbeam_channel`] inbox
//!   and await a `sync_channel(1)` oneshot reply (mirrors the existing
//!   `operator_bindings::bridge_sync` style). See [`crate::core_actor`].
//!
//! - **Async napi methods.** Every `#[napi]` method that touches Core is
//!   `async fn`, returning a JS `Promise` automatically via napi-rs's
//!   `tokio_rt` integration. Inside, the body posts a closure to the
//!   actor via `self.actor.run(move |core| { ... }).await`. The JS
//!   thread stays free to pump libuv during `await`, so TSFN-backed JS
//!   callbacks (in [`crate::operator_bindings`]) can be delivered
//!   without deadlock.
//!
//! - **Rust core stays sync.** The actor's worker thread runs the
//!   closure synchronously; tokio enters ONLY at the napi boundary (to
//!   bridge the sync `sync_channel::recv()` reply into the async
//!   context). Everything inside the closure (and thus all of
//!   `graphrefly-core` / `graphrefly-operators` / `graphrefly-graph`)
//!   runs synchronously. CLAUDE.md invariant 4 ("no async runtime in
//!   Core") preserved.
//!
//! - **Value registry on the Rust side.** Same as before — primitives
//!   intern via `Registry`. Switched to `parking_lot::Mutex` (M1 fix:
//!   no poisoning; matches `producer_storage`'s flavor).
//!
//! - **Pre-baked derived fns.** `BuiltinFn` / `BuiltinBatchFn` enums.
//!   Real JS-callback fns live on `BenchOperators` (TSFN-backed; M3
//!   napi-rs operator parity).
//!
//! - **Producer dispatch via `invoke_fn_with_core` (D245/D246-r5 / D256
//!   reconciled).** `BindingBoundary::invoke_fn_with_core` was added to
//!   the substrate in D245 with a default delegating impl. `BenchBinding`
//!   overrides it to receive `&dyn CoreFull` directly when Core fires a
//!   producer build closure. The pre-D245 `CURRENT_CORE` thread-local
//!   + `CoreThreadGuard` plumbing was legacy that nobody had reason to
//!   delete until S6; deleted in this slice — no thread-local, no
//!   stored Core back-reference, no `current_core()` clone.

use std::sync::mpsc::sync_channel;
use std::sync::Arc;
use std::sync::OnceLock;

use graphrefly_core::{
    BindingBoundary, DepBatch, EqualsMode, FnEmission, FnId, FnResult, HandleId, LockId, Message,
    NodeId, Sink, SubscriptionId,
};
// `CoreFull` is only referenced by the `invoke_fn_with_core` override
// signature, which is itself feature-gated on `operators` (producer
// dispatch lives there). Gating the import too keeps the
// tracing-only / operators-off build clean.
#[cfg(feature = "operators")]
use graphrefly_core::CoreFull;
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

use crate::core_actor::CoreActor;

/// Build a fresh no-op sink. Pre-S6 (D248) this was a `OnceLock<Sink>`
/// shared across calls (M9), but post-D248/D272 `Sink = Rc<dyn Fn(&[Message])>`
/// is `!Send + !Sync` — `OnceLock<T>` requires `T: Sync` to share across
/// threads. Per-call allocation is acceptable: `Rc::new(|_| {})` is
/// one heap allocation per `subscribe_noop` call, called rarely (only
/// in bench paths that don't observe sink messages).
fn noop_sink() -> graphrefly_core::Sink {
    std::rc::Rc::new(|_| {})
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
pub(crate) enum BenchEmission {
    Data(BenchValue),
    Complete,
    // Error channel of the emission ADT (mirrors `FnEmission::Error`).
    // Matched in the registry dispatch; not yet produced by any bench
    // scenario builtin.
    #[allow(dead_code)]
    Error(BenchValue),
}

type SingleFnImpl = Arc<dyn Fn(&[BenchValue]) -> Option<BenchValue> + Send + Sync>;
type BatchFnImpl = Arc<dyn Fn(&[DepBatch], &Registry) -> Vec<BenchEmission> + Send + Sync>;
/// D263/D264 — TSFN-backed generic user-derived fn: receives the raw
/// per-dep handle slices for this wave and returns a flat list of
/// output `HandleId`s (each becomes one `FnEmission::Data`). The
/// closure is responsible for `validate_and_retain` on each returned
/// handle BEFORE returning it (caller-side discipline in `wrapper.js`'s
/// `makeUserDerived` adapter — JS-side handles are minted via
/// `core.allocExternalHandle` which already retains).
type UserDerivedFnImpl = Arc<dyn Fn(&[DepBatch]) -> Vec<HandleId> + Send + Sync>;

pub(crate) struct Registry {
    next_handle: u64,
    values: ahash::AHashMap<HandleId, BenchValue>,
    primitive_index: ahash::AHashMap<BenchValue, HandleId>,
    pub(crate) refcounts: ahash::AHashMap<HandleId, u64>,
    pub(crate) fns: ahash::AHashMap<FnId, SingleFnImpl>,
    pub(crate) batch_fns: ahash::AHashMap<FnId, BatchFnImpl>,
    /// D263/D264 — generic TSFN-backed user-derived fns. Dispatched
    /// before `fns`/`batch_fns` in `dispatch_non_producer_fn`.
    pub(crate) user_derived_fns: ahash::AHashMap<FnId, UserDerivedFnImpl>,
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
    /// Producer build closures, looked up by
    /// [`BenchBinding::invoke_fn_with_core`] (D245/D246-r5) when a
    /// producer node fires. The closure receives a `ProducerCtx`
    /// built against the `&dyn CoreFull` Core hands the binding —
    /// no thread-local indirection (S6/D256 reconciled: the pre-D245
    /// `current_core()` plumbing was deleted in this slice).
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
    /// Stratify classifier closures (D199). Closure shape:
    /// `Fn(rules_handle, value_handle) -> bool`.
    #[cfg(feature = "operators")]
    pub(crate) stratify_classifiers:
        ahash::AHashMap<FnId, Arc<dyn Fn(HandleId, HandleId) -> bool + Send + Sync>>,
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
            user_derived_fns: ahash::AHashMap::new(),
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
            #[cfg(feature = "operators")]
            stratify_classifiers: ahash::AHashMap::new(),
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

    /// Shared non-producer dispatch — batch fn, then single-emission fn.
    /// Used by both `invoke_fn` (legacy path / non-producer dispatch) and
    /// `invoke_fn_with_core` (after the producer branch misses). Factored
    /// out so the producer-path doc stays clean and so we never accidentally
    /// drift between the two callers (they share the same fn-id lookup
    /// table — `Registry::{fns, batch_fns}`).
    fn dispatch_non_producer_fn(&self, fn_id: FnId, dep_data: &[DepBatch]) -> FnResult {
        // D263/D264 — generic TSFN-backed user-derived fn (parity slice
        // for the `setDeps`/`addDep`/`removeDep` rewire surface). The
        // closure already operates on raw `HandleId`s and retains its
        // own outputs (JS-side `core.allocExternalHandle` does the
        // retain at allocation time), so we wrap each output as
        // `FnEmission::Data` directly without re-interning.
        let user_f = {
            let reg = self.registry.lock();
            reg.user_derived_fns.get(&fn_id).cloned()
        };
        if let Some(f) = user_f {
            let out_handles = f(dep_data);
            let converted: SmallVec<[FnEmission; 2]> =
                out_handles.into_iter().map(FnEmission::Data).collect();
            return FnResult::Batch {
                emissions: converted,
                tracked: None,
            };
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
}

impl BindingBoundary for BenchBinding {
    /// Spec: R5.7 (operator-fn dispatch shape) + R1.3.6.b (multi-emit
    /// batch via `FnResult::Batch`). Non-producer fast path — Core
    /// dispatches operator/batch/single fns here; the producer-build
    /// path goes through [`Self::invoke_fn_with_core`] (D245/D246-r5)
    /// which receives the `&dyn CoreFull` facade Core construct a
    /// `ProducerCtx` against.
    ///
    /// **S6/D256 reconciled (2026-05-19):** the producer-dispatch
    /// branch that previously read a `CURRENT_CORE` thread-local was
    /// removed. Under the actor model `Core` cannot be cloned/stored,
    /// and D245 already wired the `invoke_fn_with_core` override path
    /// for exactly this purpose. The unused `_node_id` parameter is
    /// preserved at the substrate trait signature.
    fn invoke_fn(&self, _node_id: NodeId, fn_id: FnId, dep_data: &[DepBatch]) -> FnResult {
        self.dispatch_non_producer_fn(fn_id, dep_data)
    }

    /// Spec: D245 / D246-r5 — producer-build dispatch with `&dyn
    /// CoreFull` from Core. Overrides the substrate's default
    /// delegating impl ([`crates/graphrefly-core/src/boundary.rs:230`])
    /// to construct a `ProducerCtx` against the passed Core facade,
    /// then fall through to non-producer dispatch on cache miss.
    ///
    /// This is the post-S2c replacement for the pre-D245 `CURRENT_CORE`
    /// thread-local + `CoreThreadGuard` plumbing — the actor's worker
    /// loop runs `closure(&core)`, Core's batch dispatcher calls
    /// `invoke_fn_with_core(self as &dyn CoreFull)`, and the producer
    /// build closure receives `ctx.core() -> &dyn CoreFull` directly
    /// without any thread-local indirection.
    #[cfg(feature = "operators")]
    fn invoke_fn_with_core(
        &self,
        node_id: NodeId,
        fn_id: FnId,
        dep_data: &[DepBatch],
        core: &dyn CoreFull,
    ) -> FnResult {
        // Producer dispatch (Slice D, D031) — try first since producer
        // nodes have empty dep_data and the build closure is keyed by
        // FnId.
        let build = {
            let reg = self.registry.lock();
            reg.producer_builds.get(&fn_id).cloned()
        };
        if let Some(build) = build {
            let ctx = ProducerCtx::new(node_id, core, &self.producer_storage);
            build(ctx);
            return FnResult::Noop { tracked: None };
        }
        // Not a producer fn — fall through to the parameterless path.
        self.dispatch_non_producer_fn(fn_id, dep_data)
    }

    // (operator feature off: invoke_fn_with_core's default delegating
    // impl in graphrefly-core forwards to invoke_fn — no override
    // needed; the producer branch is unreachable when operators is
    // off, since no producer build can be registered.)

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
    fn producer_deactivate(
        &self,
        node_id: NodeId,
        unsub: &dyn Fn(NodeId, graphrefly_core::SubscriptionId),
    ) {
        // S2b/D229: owner-driven explicit upstream unsubscribe.
        default_producer_deactivate(&self.producer_storage, node_id, unsub);
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

    /// Stratify classifier: call the registered closure with the
    /// latest rules handle + current value handle. Returns `true` if
    /// the value belongs to the closure's branch (D199).
    #[cfg(feature = "operators")]
    fn invoke_stratify_classifier_fn(
        &self,
        fn_id: FnId,
        rules_handle: HandleId,
        value_handle: HandleId,
    ) -> bool {
        let f = {
            let reg = self.registry.lock();
            reg.stratify_classifiers
                .get(&fn_id)
                .cloned()
                .unwrap_or_else(|| panic!("unknown stratify classifier fn_id {fn_id:?}"))
        };
        f(rules_handle, value_handle)
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

    /// Serialize a handle to JSON for snapshot/WAL persistence (M4.F).
    ///
    /// For `BenchValue::Int` handles, serialize the actual integer value.
    /// For `JsAllocated` handles, serialize the raw handle ID — the
    /// actual value lives in JS and is opaque to Rust. This gives the
    /// diff engine a stable identity to detect value changes (different
    /// handles → different serialized values), even though the
    /// round-tripped value won't be the original JS value.
    fn serialize_handle(&self, handle: HandleId) -> Option<serde_json::Value> {
        let reg = self.registry.lock();
        match reg.deref(handle) {
            Some(BenchValue::Int(n)) => {
                Some(serde_json::Value::Number(serde_json::Number::from(n)))
            }
            Some(BenchValue::Str(s)) => Some(serde_json::Value::String(s)),
            Some(BenchValue::JsAllocated) => {
                // Use raw handle ID as a stable identity for diff detection.
                Some(serde_json::Value::Number(serde_json::Number::from(
                    handle.raw(),
                )))
            }
            None => None,
        }
    }

    /// Deserialize a JSON value back into a handle for snapshot restore.
    /// Interns the value as a `BenchValue` and returns the handle.
    fn deserialize_value(&self, value: serde_json::Value) -> HandleId {
        let mut reg = self.registry.lock();
        match &value {
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    reg.intern(BenchValue::Int(i as i32))
                } else {
                    reg.alloc_external_handle()
                }
            }
            serde_json::Value::String(s) => reg.intern(BenchValue::Str(s.clone())),
            _ => reg.alloc_external_handle(),
        }
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
/// `Function<'_, Vec<u32>, ()>` signature.
type SinkTsfn = ThreadsafeFunction<Vec<u32>, (), Vec<u32>, Status, false, false, 1>;

fn build_sink_tsfn(callback: Function<'_, Vec<u32>, ()>) -> Result<Arc<SinkTsfn>> {
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
    // D289 / D288 Q2 tripwire — see `batch_bindings::assert_no_batch_handle`.
    crate::batch_bindings::assert_no_batch_handle("core_bindings::bridge_sync_unit");
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
    std::rc::Rc::new(move |msgs: &[Message]| {
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
// Per S6/D255 (2026-05-19), every `#[napi] async fn` that touches Core
// routes through the actor model: `self.actor.run(move |core| { ... }).
// await`. The actor's worker thread owns Core by value for its
// lifetime; the worker runs each closure synchronously with the
// worker-owned `&Core`. The JS thread stays free to pump libuv during
// `await`, so TSFN-backed JS callbacks (in [`crate::operator_bindings`])
// can be delivered without deadlock — same end-to-end discipline as
// the pre-S6 `spawn_blocking`-based shape, just with thread affinity
// guaranteed.
// ---------------------------------------------------------------------------

#[napi]
pub struct BenchCore {
    /// Per-node subscription bookkeeping. Each entry pairs the node id
    /// with the substrate `SubscriptionId` returned by `Core::subscribe`;
    /// `None` slots represent unsubscribed entries (we don't compact to
    /// keep JS-side indices stable).
    ///
    /// **D241/D246-r3 reconciled (S6):** the pre-D225 RAII `Subscription`
    /// type was deleted from the substrate — unsubscribe is now owner-
    /// invoked via `Core::unsubscribe(node_id, sub_id)`. We store the
    /// `(NodeId, SubscriptionId)` pair so `BenchCore::unsubscribe` /
    /// `dispose` / `Drop` can post the explicit unsubscribe call to the
    /// actor.
    ///
    /// **Arc-wrapped** — `subscribe_with_tsfn` returns a `PromiseRaw`
    /// whose `spawn_future` closure outlives `&self`; Arc lets the
    /// closure capture a clone for storing the SubscriptionId after the
    /// actor closure resolves.
    pub(crate) subscriptions: Arc<Mutex<Vec<Option<(NodeId, SubscriptionId)>>>>,
    pub(crate) binding: Arc<BenchBinding>,
    /// Single-thread Core owner. The actor's worker thread holds the
    /// `Core` for the actor's lifetime; this `Arc<CoreActor>` is the
    /// only handle into it. Cloning the Arc clones the actor handle
    /// (shared with `BenchOperators` / `BenchGraph` / `BenchStorage*` /
    /// `BenchReactive*` companion classes that route through the same
    /// `Core`); the inner worker thread + `Core` are uniquely owned by
    /// the actor.
    pub(crate) actor: Arc<CoreActor>,
}

impl BenchCore {
    /// Share the actor handle with a companion class (`BenchOperators::
    /// from_core`, `BenchGraph::from_core`, etc.). All companions
    /// driving the same Core route through this one `Arc<CoreActor>`.
    pub(crate) fn actor_arc(&self) -> Arc<CoreActor> {
        Arc::clone(&self.actor)
    }

    pub(crate) fn binding_arc(&self) -> Arc<BenchBinding> {
        Arc::clone(&self.binding)
    }

    /// Clone the subscriptions Arc for capture into futures (D077).
    pub(crate) fn subscriptions_arc(&self) -> Arc<Mutex<Vec<Option<(NodeId, SubscriptionId)>>>> {
        Arc::clone(&self.subscriptions)
    }
}

#[napi]
impl BenchCore {
    /// Construct a fresh `BenchCore` wired to a Rust-side value registry.
    ///
    /// **S6/D255:** the actor's worker thread spawns inside
    /// `CoreActor::spawn(binding)` and constructs the `Core` on the
    /// worker (Core is `!Send`, so it must be born on the thread that
    /// owns it). This constructor returns immediately; the worker is
    /// ready to receive closures by the time any subsequent napi
    /// method's first `actor.run(...)` resolves.
    #[napi(constructor)]
    #[allow(clippy::new_without_default)]
    #[must_use]
    pub fn new() -> Self {
        let binding = BenchBinding::new();
        let actor = CoreActor::spawn(binding.clone() as Arc<dyn BindingBoundary>);
        Self {
            subscriptions: Arc::new(Mutex::new(Vec::new())),
            binding,
            actor,
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
        self.actor
            .run(move |core| -> Result<u32> {
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
        self.actor
            .run(move |core| {
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
        let h = self
            .actor
            .run(move |core| core.cache_of(NodeId::new(u64::from(node_id))))
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
        self.actor
            .run(move |core| -> Result<u32> {
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
        self.actor
            .run(move |core| -> Result<u32> {
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
        self.actor
            .run(move |core| -> Result<u32> {
                let fn_impl: Arc<dyn Fn(&[BenchValue]) -> Option<BenchValue> + Send + Sync> =
                    match builtin {
                        BuiltinFn::Identity => {
                            Arc::new(|deps: &[BenchValue]| deps.first().cloned())
                        }
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
    ///
    /// **S6 / D241+D255 reconciled:** Core::subscribe now returns a
    /// `SubscriptionId` (D241) — the substrate-level RAII `Subscription`
    /// type was deleted in D225. We post the subscribe through the
    /// actor and record the `(NodeId, SubscriptionId)` pair so the
    /// matching unsubscribe in `BenchCore::unsubscribe` / `dispose` /
    /// `Drop` can call `Core::unsubscribe(node_id, sub_id)` explicitly.
    ///
    /// **D248/D272 sink-shape note:** `graphrefly_core::Sink =
    /// Rc<dyn Fn(&[Message])>` (`!Send + !Sync`) — owner-thread-only.
    /// We build the sink *inside* the actor closure, on the worker
    /// thread, so the `!Send` Sink never has to cross the actor channel.
    #[napi]
    pub async fn subscribe_noop(&self, node_id: u32) -> Result<u32> {
        let nid = NodeId::new(u64::from(node_id));
        let sub_id = self
            .actor
            .run(move |core| {
                // Per-call `Rc::new(|_| {})` (D272 — Sink is
                // `Rc<dyn Fn>`, `!Send + !Sync`). The earlier M9 plan
                // was a shared `OnceLock<Sink>` but that required
                // `Sink: Sync` which D248 deliberately relaxed away.
                // Built on the worker thread inside the actor closure.
                core.subscribe(nid, noop_sink())
            })
            .await?;
        let mut subs = self.subscriptions.lock();
        // Phase E /qa F7 (2026-05-08): validate idx BEFORE push so an
        // overflow path doesn't strand a recorded SubscriptionId in a
        // slot no JS caller can address. If validate fails we
        // explicitly unsubscribe before returning the error.
        let idx = subs.len();
        let idx_u32 = match u32::try_from(idx) {
            Ok(n) => n,
            Err(_) => {
                drop(subs);
                let _ = self
                    .actor
                    .dispatch_detached(move |core| core.unsubscribe(nid, sub_id));
                return Err(NapiError::from_reason("subscription index exceeds u32"));
            }
        };
        subs.push(Some((nid, sub_id)));
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
    /// completes (per `bridge_sync_unit` blocking the actor worker on
    /// a sync_channel until JS responds via libuv pump). JS code
    /// MUST `await` this method — synchronous calling deadlocks.
    ///
    /// `pub fn` (not `async fn`) because `Function<'_, >` is `!Send`; the
    /// async work runs inside `env.spawn_future(async move { ... })`.
    ///
    /// **D248/D272 sink-shape:** the `tsfn: Arc<SinkTsfn>` we move into
    /// the actor closure IS `Send + Sync` (`ThreadsafeFunction` is
    /// Send+Sync by napi-rs design); the `Sink = Rc<dyn Fn(&[Message])>`
    /// that wraps it is `!Send + !Sync`, so we build it on the worker
    /// thread inside the actor closure.
    #[napi]
    pub fn subscribe_with_tsfn<'env>(
        &self,
        env: &'env Env,
        node_id: u32,
        sink_callback: Function<'_, Vec<u32>, ()>,
    ) -> Result<PromiseRaw<'env, u32>> {
        let tsfn = build_sink_tsfn(sink_callback)?;
        let actor = self.actor_arc();
        let subs = self.subscriptions_arc();
        env.spawn_future(async move {
            let nid = NodeId::new(u64::from(node_id));
            let sub_id = actor
                .run(move |core| {
                    // Build the Sink on the worker thread — `Sink` is
                    // `Rc<dyn Fn(&[Message])>` (`!Send + !Sync` per
                    // D248/D272). `tsfn` (Arc<SinkTsfn>) IS `Send + Sync`
                    // so it crossed the channel fine.
                    let sink = build_tsfn_sink(tsfn);
                    core.subscribe(nid, sink)
                })
                .await?;
            let mut s = subs.lock();
            // Phase E /qa F7: validate-before-push (see subscribe_noop).
            let idx = s.len();
            let idx_u32 = match u32::try_from(idx) {
                Ok(n) => n,
                Err(_) => {
                    drop(s);
                    let _ = actor.dispatch_detached(move |core| core.unsubscribe(nid, sub_id));
                    return Err(NapiError::from_reason("subscription index exceeds u32"));
                }
            };
            s.push(Some((nid, sub_id)));
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
    /// Posts the unsubscribe through the actor so the JS thread stays
    /// free to pump libuv if the unsubscribe triggers terminal-cascade
    /// work that fires TSFN-backed sinks.
    ///
    /// **S6 / D241+D255 reconciled:** explicit `Core::unsubscribe(node,
    /// sub_id)` replaces the deleted `impl Drop for Subscription`
    /// (D225).
    #[napi]
    pub async fn unsubscribe(&self, idx: u32) -> Result<()> {
        let entry = {
            let mut subs = self.subscriptions.lock();
            subs.get_mut(idx as usize).and_then(Option::take)
        };
        if let Some((nid, sub_id)) = entry {
            self.actor
                .run(move |core| core.unsubscribe(nid, sub_id))
                .await?;
        }
        Ok(())
    }

    /// Drain all retained subscriptions through the actor — JS code
    /// MUST `await core.dispose()` before letting `BenchCore` drop.
    /// Closes the BenchCore::Drop deadlock vector (Slice Y): without
    /// `dispose`, GC of the napi instance runs `Drop` on the JS thread;
    /// each unsubscribe would post to the actor and *not* be able to
    /// `.await` (Drop is sync). The actor's worker thread would still
    /// run the work fire-and-forget, but the JS thread would have
    /// already moved on without ensuring terminal-cascade TSFNs
    /// flushed. Calling `dispose` first guarantees the JS thread waits
    /// for the cascade to settle.
    ///
    /// Idempotent: subsequent calls are no-ops once the vec is drained.
    #[napi]
    pub async fn dispose(&self) -> Result<()> {
        // Take the entire Vec out on the calling thread (cheap — Vec
        // pointer swap). Post the entire drain as one actor closure so
        // we don't pay one round-trip per subscription.
        let subs: Vec<Option<(NodeId, SubscriptionId)>> = {
            let mut guard = self.subscriptions.lock();
            std::mem::take(&mut *guard)
        };
        if !subs.is_empty() {
            self.actor
                .run(move |core| {
                    for entry in subs.into_iter().flatten() {
                        let (nid, sub_id) = entry;
                        core.unsubscribe(nid, sub_id);
                    }
                })
                .await?;
        }
        Ok(())
    }

    /// Emit an i32 value on a state node.
    #[napi]
    pub async fn emit_int(&self, node_id: u32, value: i32) -> Result<()> {
        let binding = Arc::clone(&self.binding);
        self.actor
            .run(move |core| {
                let handle = binding.registry.lock().intern(BenchValue::Int(value));
                core.emit(NodeId::new(u64::from(node_id)), handle);
            })
            .await
    }

    /// Read a state node's current cache as i32.
    #[napi]
    pub async fn cache_int(&self, node_id: u32) -> Result<i32> {
        let binding = Arc::clone(&self.binding);
        self.actor
            .run(move |core| -> i32 {
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
        self.actor
            .run(move |core| {
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
        let raw = self
            .actor
            .run(move |core| core.alloc_lock_id().raw())
            .await?;
        u32::try_from(raw).map_err(|_| {
            NapiError::from_reason(
                "alloc_lock_id exceeded u32 range — restart BenchCore or migrate to BigInt",
            )
        })
    }

    #[napi]
    pub async fn set_pause_buffer_cap(&self, cap: Option<u32>) -> Result<()> {
        self.actor
            .run(move |core| {
                core.set_pause_buffer_cap(cap.map(|c| c as usize));
            })
            .await
    }

    #[napi]
    pub async fn pause(&self, node_id: u32, lock_id: u32) -> Result<()> {
        self.actor
            .run(move |core| {
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
        let result = self
            .actor
            .run(move |core| {
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
        self.actor
            .run(move |core| core.is_paused(NodeId::new(u64::from(node_id))))
            .await
    }

    /// Number of pause locks currently held. Returns Err on overflow
    /// instead of saturating silently (M8 fix — matches `alloc_lock_id`).
    #[napi]
    pub async fn pause_lock_count(&self, node_id: u32) -> Result<u32> {
        let count = self
            .actor
            .run(move |core| core.pause_lock_count(NodeId::new(u64::from(node_id))))
            .await?;
        u32::try_from(count).map_err(|_| NapiError::from_reason("pause_lock_count exceeds u32"))
    }

    #[napi]
    pub async fn holds_pause_lock(&self, node_id: u32, lock_id: u32) -> Result<bool> {
        self.actor
            .run(move |core| {
                core.holds_pause_lock(
                    NodeId::new(u64::from(node_id)),
                    LockId::new(u64::from(lock_id)),
                )
            })
            .await
    }

    #[napi]
    pub async fn invalidate(&self, node_id: u32) -> Result<()> {
        self.actor
            .run(move |core| {
                core.invalidate(NodeId::new(u64::from(node_id)));
            })
            .await
    }

    #[napi]
    pub async fn complete(&self, node_id: u32) -> Result<()> {
        self.actor
            .run(move |core| {
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
        self.actor
            .run(move |core| {
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
        self.actor
            .run(move |core| {
                let h = binding.registry.lock().intern(BenchValue::Int(err_code));
                core.error(NodeId::new(u64::from(node_id)), h);
            })
            .await
    }

    #[napi]
    pub async fn teardown(&self, node_id: u32) -> Result<()> {
        self.actor
            .run(move |core| {
                core.teardown(NodeId::new(u64::from(node_id)));
            })
            .await
    }

    #[napi]
    pub async fn add_meta_companion(&self, parent: u32, companion: u32) -> Result<()> {
        self.actor
            .run(move |core| {
                core.add_meta_companion(
                    NodeId::new(u64::from(parent)),
                    NodeId::new(u64::from(companion)),
                );
            })
            .await
    }

    #[napi]
    pub async fn set_resubscribable(&self, node_id: u32, resubscribable: bool) -> Result<()> {
        self.actor
            .run(move |core| {
                core.set_resubscribable(NodeId::new(u64::from(node_id)), resubscribable);
            })
            .await
    }

    #[napi]
    pub async fn batch_emit_ints(&self, node_id: u32, values: Vec<i32>) -> Result<()> {
        let binding = Arc::clone(&self.binding);
        self.actor
            .run(move |core| {
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
        let result = self
            .actor
            .run(move |core| {
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
        self.actor
            .run(move |core| core.has_fired_once(NodeId::new(u64::from(node_id))))
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
        self.actor
            .run(move |core| -> Result<u32> {
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

        self.actor
            .run(move |core| {
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
        self.actor
            .run(move |core| {
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

    // -------------------------------------------------------------------
    // Option C (D206/D207) — N1 substrate-infra surface for the
    // hand-written async `@graphrefly/native` public wrapper.
    // -------------------------------------------------------------------

    /// Single-node describe projection (N1 `describeNode`).
    ///
    /// **D207 (locked 2026-05-15): REUSE the existing snapshot /
    /// describe-projection path — no new `graphrefly-core` public
    /// method.** This composes Core's existing *read-side inspection
    /// helpers* (`kind_of` / `deps_of` / `cache_of` / `has_fired_once`
    /// / `is_terminal`) — the very accessors that back
    /// `Graph::describe` (`graphrefly-graph::describe::describe_inner`)
    /// — into the per-node JSON slice the `Impl.describeNode(node)`
    /// contract expects. Mirrors the pure-ts reference arm
    /// (`pure-ts.ts` `describeNode → legacy.describeNode(node.inner)`),
    /// which returns the same `{ type, status, deps, value? }` shape.
    ///
    /// Sync — pure read accessors, single state-lock acquire each, no
    /// wave entry, no binding callbacks. Returns a JSON string the
    /// JS wrapper parses (same marshaling convention as
    /// `BenchGraph::describe_json`). Dep ids surface as raw `NodeId`
    /// u64s (the namespace lives at the Graph layer; an unattached
    /// node has no names — the JS wrapper renders them as
    /// `_anon_<id>`, matching `describe_inner`'s unnamed-dep rule).
    ///
    /// **D267 — async** (was `run_sync`). The `run_sync` shape would
    /// deadlock if a JS sink callback (already blocking the actor
    /// worker via TSFN Blocking) called `impl.describeNode(node)` to
    /// project a mid-wave snapshot. `actor.run` keeps libuv free to
    /// pump the actor reply during the await. The parity `Impl
    /// .describeNode` contract is widened to `T | Promise<T>` so the
    /// pure-ts arm returns its sync projection unchanged.
    #[napi]
    pub async fn describe_node(&self, node_id: u32) -> Result<String> {
        let nid = NodeId::new(u64::from(node_id));
        self.actor
            .run(move |core| -> String {
                let kind = core.kind_of(nid);
                let Some(kind) = kind else {
                    // Unknown node — absence over panic (mirrors the Core
                    // read-side "Option/empty for unknown ids" discipline).
                    return "null".to_string();
                };
                let type_str = match kind {
                    graphrefly_core::NodeKind::State => "state",
                    graphrefly_core::NodeKind::Producer => "producer",
                    graphrefly_core::NodeKind::Derived => "derived",
                    graphrefly_core::NodeKind::Dynamic => "dynamic",
                    graphrefly_core::NodeKind::Operator(_) => "operator",
                };
                let status = match core.is_terminal(nid) {
                    Some(graphrefly_core::TerminalKind::Complete) => "complete",
                    Some(graphrefly_core::TerminalKind::Error(_)) => "error",
                    None => {
                        if core.has_fired_once(nid) {
                            "active"
                        } else {
                            "sentinel"
                        }
                    }
                };
                let deps: Vec<u64> = core.deps_of(nid).iter().map(|d| d.raw()).collect();
                let cache = core.cache_of(nid);
                let cache_raw = if cache == graphrefly_core::NO_HANDLE {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::from(cache.raw())
                };
                let projection = serde_json::json!({
                    "type": type_str,
                    "status": status,
                    "deps": deps,
                    "valueHandle": cache_raw,
                    "sentinel": cache == graphrefly_core::NO_HANDLE,
                });
                projection.to_string()
            })
            .await
    }

    /// Hex SHA-256 (N1 `sha256Hex`).
    ///
    /// Hashing is **synchronous in `graphrefly-core`**
    /// ([`graphrefly_core::sha256_hex`]) — there is nothing async about
    /// SHA-256, and Core forbids a tokio runtime (CLAUDE.md invariant 4
    /// / D070 / D077). Async-everywhere is a *binding-contract* shape
    /// (`Impl.sha256Hex` is `Promise<string>`), so the async wrapping
    /// lives only here at the napi boundary. **Hashing is Core-free**
    /// — it does NOT touch the actor / `Core` / WORKER_EXTRAS. We
    /// route through `napi::bindgen_prelude::spawn_blocking` (the
    /// generic tokio blocking pool, NOT the actor's pinned worker)
    /// just to keep the (potentially large) input hashing off the
    /// libuv thread. (QA fix 2026-05-20 m4: doc previously claimed
    /// "we route through the actor" — false; corrected to match the
    /// actual `spawn_blocking` body.)
    #[napi]
    pub async fn sha256_hex(&self, input: Uint8Array) -> Result<String> {
        spawn_blocking(move || -> String { graphrefly_core::sha256_hex(input.as_ref()) })
            .await
            .map_err(|e| NapiError::from_reason(format!("sha256_hex worker panicked: {e}")))
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
