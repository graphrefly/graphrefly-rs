//! The FFI surface — the only path from Core to user code.
//!
//! Mirrors `BindingBoundary` in
//! `~/src/graphrefly-ts/src/__experiments__/handle-core/core.ts:122–126`.
//!
//! # Boundary discipline (handle-protocol cleaving plane)
//!
//! The Core never sees user values `T`. When the dispatcher needs to invoke
//! user code, run a custom equals oracle, or release a value-handle's
//! refcount, it calls into [`BindingBoundary`]. The binding-side implementation
//! resolves handles to values, runs the user code, and returns either a new
//! handle or a no-op signal.
//!
//! In a Rust core compiled to a napi-rs / pyo3 / wasm-bindgen cdylib, the
//! `impl BindingBoundary` lives in the bindings crate; it owns the
//! value registry (`HashMap<HandleId, T>` plus a value→handle dedup map).
//!
//! Per the rust-port session doc Part 2: this trait is the *only* mandatory
//! FFI crossing per fn-fire. Internal protocol bookkeeping (DIRTY propagation,
//! batch coalescing, equals-substitution under identity, version counters,
//! PAUSE/RESUME, INVALIDATE, first-run gate) stays Core-internal — zero FFI.

use smallvec::SmallVec;

use crate::handle::{FnId, HandleId, NodeId, NO_HANDLE};

/// Per-dep batch data passed to [`BindingBoundary::invoke_fn`].
///
/// Mirrors the canonical spec R2.9.b `DepRecord` shape at the FFI boundary.
/// Each entry represents one dep's state for the current wave:
///
/// - `data` — DATA handles accumulated this wave (R1.3.6.b coalescing).
///   Empty means the dep settled RESOLVED or was not involved.
/// - `prev_data` — last DATA handle from the end of the previous wave.
///   [`NO_HANDLE`] if the dep has never emitted DATA.
/// - `involved` — `true` iff the dep was dirtied-then-settled this wave.
///   Distinguishes "RESOLVED in wave" (`involved && data.is_empty()`) from
///   "not involved" (`!involved && data.is_empty()`).
#[derive(Clone, Debug)]
pub struct DepBatch {
    /// DATA handles accumulated this wave. Outside `batch()` scope, at most
    /// 1 element. Inside `batch()`, K consecutive emits on the same source
    /// produce K entries per R1.3.6.b coalescing.
    pub data: SmallVec<[HandleId; 1]>,
    /// Last DATA handle from the end of the previous wave. [`NO_HANDLE`]
    /// means the dep has never emitted DATA.
    pub prev_data: HandleId,
    /// Whether this dep was involved (dirtied → settled) in the current wave.
    pub involved: bool,
}

impl DepBatch {
    /// The "latest" handle for this dep — the last DATA in the current wave's
    /// batch, falling back to `prev_data` if no DATA arrived this wave.
    /// Returns [`NO_HANDLE`] only when the dep has never emitted.
    #[must_use]
    pub fn latest(&self) -> HandleId {
        self.data.last().copied().unwrap_or(self.prev_data)
    }

    /// Convenience: is this dep in sentinel state (never emitted DATA)?
    #[must_use]
    pub fn is_sentinel(&self) -> bool {
        self.prev_data == NO_HANDLE && self.data.is_empty()
    }
}

/// Lifecycle trigger discriminator for [`BindingBoundary::cleanup_for`].
///
/// Slice E2 (R2.4.5 / Lock 4.A / Lock 4.A′) — the named-hook cleanup spec
/// returned by user fns has three independent slots; Core fires each slot
/// at its own lifecycle moment via `cleanup_for(node_id, CleanupTrigger)`.
///
/// All three triggers fire **lock-released** per Slice E D045 handshake
/// discipline. The binding is responsible for resolving the trigger to its
/// stored cleanup closure (typically a `node_id → NodeFnCleanup` map) and
/// invoking the matching slot.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum CleanupTrigger {
    /// **R2.4.5 `onRerun`** — fires before the next fn run within the same
    /// activation cycle. Core fires this in `fire_regular` between the
    /// lock-held dep-snapshot phase and the lock-released `invoke_fn` call,
    /// gated on `has_fired_once == true` (so first-fire never sees an
    /// `onRerun`). The user closure receives no arguments and is expected
    /// to release fn-local resources from the previous run before the
    /// fresh `invoke_fn` allocates new ones.
    OnRerun,
    /// **R2.4.5 `onDeactivation`** — fires when subscriber count drops to
    /// zero. Core fires this from `Subscription::Drop` (alongside the
    /// existing [`BindingBoundary::producer_deactivate`] call), gated on
    /// `has_fired_once == true`. Order: cleanup-first (`OnDeactivation`)
    /// then `producer_deactivate`, because cleanup may release handles the
    /// producer subscription owns (D056). Per D059, bindings SHOULD clear
    /// `current_cleanup` on this trigger — the next subscribe + first-fire
    /// will re-register a fresh closure.
    OnDeactivation,
    /// **R2.4.5 `onInvalidate`** — fires when an `[[INVALIDATE]]` arrives at
    /// the node and clears its cache. Per **R1.3.9.b** strict reading
    /// (D057): fires **at most once per wave per node**, regardless of
    /// fan-in shape. Per **R1.3.9.c**: never fires when the node's cache
    /// is the never-populated sentinel (a node that has not yet emitted
    /// has nothing to clean up). Per **D058**: fires at cache-clear time,
    /// not at wire-delivery time — pause buffering doesn't defer the
    /// hook. **Per D061**: when the wave is panic-discarded mid-flight,
    /// queued OnInvalidate hooks are dropped silently (the cleanup never
    /// fires for the panicked wave); see [`BindingBoundary::cleanup_for`]
    /// rustdoc for the panic-discard guarantee gap and the bindings'
    /// idempotent-cleanup recommendation.
    OnInvalidate,
}

/// A single emission within a [`FnResult::Batch`] — one element of an
/// `actions.down(msgs)` call. Processed in sequence within the same wave.
#[derive(Clone, Debug)]
#[must_use = "FnEmission may contain handles that must be processed or released"]
pub enum FnEmission {
    /// DATA payload. Processed via `commit_emission` — sets cache, queues
    /// Dirty (if not already dirty per R1.3.1.a) + Data, propagates to
    /// children. No equals substitution in Batch context (R1.3.2.d /
    /// R1.3.3.c: multi-message waves pass through verbatim).
    Data(HandleId),
    /// COMPLETE terminal. Cascades per R1.3.4 / Lock 2.B.
    Complete,
    /// ERROR terminal with error-value handle. Cascades per R1.3.4;
    /// ERROR dominates COMPLETE (Lock 2.B).
    Error(HandleId),
}

/// What the binding side returns when the Core invokes a fn via
/// [`BindingBoundary::invoke_fn`].
///
/// Models the three emission modes from the canonical spec:
/// - `FnResult::Data` — single DATA, equals substitution applies (R1.3.2).
///   Maps to `actions.emit(v)` in sugar constructors.
/// - `FnResult::Batch` — multi-message wave, no equals substitution
///   (R1.3.2.d / R1.3.3.c). Maps to `actions.down(msgs)`.
/// - `FnResult::Noop` — no emission. RESOLVED if node was DIRTY.
///
/// Per R2.4.5, the fn return value in the canonical spec is cleanup hooks
/// only — all emission is explicit via actions. The Rust Core folds the
/// emission into `FnResult` as a pragmatic simplification; the binding
/// layer maps between the two representations.
#[derive(Clone, Debug)]
pub enum FnResult {
    /// fn produced a single value. The Core treats this as outgoing DATA —
    /// equals-substitution against the cache may rewrite it to RESOLVED
    /// on the wire (R1.3.2).
    Data {
        handle: HandleId,
        /// For dynamic nodes only: the dep indices fn actually read this run.
        /// Static derived nodes pass `None`. See dynamic-node semantics in the
        /// canonical spec §2.8 / Lock 2.B.
        tracked: Option<Vec<usize>>,
    },

    /// fn ran but produced no emission this wave. The Core sends RESOLVED
    /// to subscribers if the node was already DIRTY this wave; otherwise no
    /// outgoing message.
    Noop {
        /// Same as `Data::tracked` — dynamic nodes only.
        tracked: Option<Vec<usize>>,
    },

    /// Multi-message wave — models `actions.down(msgs)`. Emissions are
    /// processed in sequence within the same wave. No equals substitution
    /// on any Data emission (R1.3.2.d: substitution only on single-DATA
    /// waves; R1.3.3.c: multi-DATA passes verbatim). DIRTY auto-prefix
    /// only on first Data per R1.3.1.a.
    Batch {
        emissions: SmallVec<[FnEmission; 2]>,
        /// Same as `Data::tracked` — dynamic nodes only.
        tracked: Option<Vec<usize>>,
    },
}

/// The FFI surface: every Core → user-code crossing goes through one of these
/// three methods.
///
/// # Thread safety
///
/// `Send + Sync` because the Core dispatcher is sync but may be called from
/// multiple binding threads (e.g. multiple Node Workers sharing one Core via
/// `Arc<Core>`). Implementors must serialize access to the value registry
/// internally if needed. Free-threaded Python parity is the target — see
/// `~/src/graphrefly-py` `compat/asyncio.py` for the current shape that
/// will simplify dramatically once this trait is the substrate.
pub trait BindingBoundary: Send + Sync {
    /// Invoke a user function. The Core knows the fn's identity (`fn_id`) and
    /// the current dep batch data; the binding side dereferences handles,
    /// runs the fn, registers the output, and returns the new handle.
    ///
    /// `dep_data` carries per-dep batch arrays (R1.3.6.b coalescing): outside
    /// `batch()` scope each dep has at most 1 DATA handle; inside `batch()`,
    /// K consecutive emits on the same source produce K entries per dep.
    /// The binding side bulk-dereferences all handles, calls user code, and
    /// returns the result — one FFI call per fn fire regardless of dep count
    /// or batch depth.
    ///
    /// Errors thrown by user code are reported by returning a [`FnResult::Data`]
    /// with a handle that resolves to an error value; the Core then forwards
    /// `[ERROR, handle]` per R1.2.5. (Or the binding side surfaces them via a
    /// separate channel — exact error-propagation discipline is binding-side.)
    fn invoke_fn(&self, node_id: NodeId, fn_id: FnId, dep_data: &[DepBatch]) -> FnResult;

    /// Custom equals oracle. Called only when a node declares
    /// `EqualsMode::Custom`. Identity equals (the default) is a `u64` compare
    /// inside the Core — zero FFI per check.
    ///
    /// Per `H2 IdentityEqualsIsPureCore` ASSUME in
    /// `~/src/graphrefly-ts/docs/research/handle-protocol.tla`,
    /// the binding-side impl MUST extend identity (i.e. always treat
    /// `a == b` as equal at the handle level). Otherwise a node could observe
    /// its own cached value as different from itself — a fundamental violation.
    fn custom_equals(&self, equals_handle: FnId, a: HandleId, b: HandleId) -> bool;

    /// Decrement the refcount on `handle`. Called when the Core no longer
    /// holds `handle` in any cache slot or message buffer. The binding side
    /// drops the underlying value when its refcount reaches zero.
    ///
    /// Implementing this as a no-op is safe during prototyping; it matters
    /// for memory pressure under sustained load. The TS prototype's
    /// `bindings.ts` uses this to drive a `Map<HandleId, { value, refcount }>`.
    ///
    /// # Leaf-operation contract (/qa F2/M1, 2026-05-10) — HARD requirement
    ///
    /// Implementations MUST NOT re-enter Core from `release_handle`. The
    /// Core's lock-held release paths (`Drop for CoreState`,
    /// `Core::reset_for_fresh_lifecycle` Phase 3 / 3b / 5,
    /// `OperatorScratch::release_handles` callers) invoke
    /// `release_handle` while the state mutex is held; re-entering Core
    /// (via `emit` / `subscribe` / `register` / nested `release_handle`
    /// on a different handle that triggers a final-Drop hook into Core,
    /// etc.) deadlocks against that lock.
    ///
    /// "Re-entry into Core" here means: calling any method on the same
    /// `Core` instance (or any `Core` that shares state via `Arc::clone`
    /// of the inner state mutex). Independent Cores are fine.
    ///
    /// Safe operations inside `release_handle`:
    /// - Pure value-registry bookkeeping (decrement refcount, drop the
    ///   underlying `T` when count reaches zero).
    /// - Logging / metrics that don't re-enter Core.
    /// - Calling other binding methods that themselves honor the
    ///   leaf-op contract (e.g., a binding-internal mutex).
    ///
    /// Forbidden operations inside `release_handle`:
    /// - Any `Core::emit` / `subscribe` / `register*` / `complete` /
    ///   `error` / `teardown` / `invalidate` / `pause` / `resume` /
    ///   `set_deps` / `batch` / `begin_batch`.
    /// - Recursive `BindingBoundary::release_handle` on this binding
    ///   that fans into Core via a binding-side final-Drop.
    ///
    /// Bindings that need lifecycle hooks on value-drop should buffer
    /// the events and process them asynchronously (e.g., next tick on
    /// the host runtime) so the Core lock is released before
    /// re-entrance.
    fn release_handle(&self, handle: HandleId);

    /// Increment the refcount on `handle`. Called when the Core takes an
    /// additional reference to an existing handle that the binding side
    /// already interned — used by the pause buffer (a buffered
    /// `Data(H)` outlives the cache slot that originally interned `H`, so
    /// the buffer needs its own refcount share to keep `H` alive across
    /// later cache replacements), the operator-scratch pipeline (Scan/Reduce
    /// seed retain, Last default retain), and Phase G's D-α fresh-scratch
    /// install.
    ///
    /// Default: no-op. Bindings that don't track refcounts (e.g., trivial
    /// always-alive registries) can leave the default. Bindings that do
    /// (TS `bindings.ts`, the test runtime, the napi-rs bench harness) must
    /// override to bump the per-handle refcount.
    ///
    /// # Leaf-operation contract — HARD requirement
    ///
    /// Same leaf-op contract as [`Self::release_handle`]: implementations
    /// MUST NOT re-enter Core from `retain_handle`. Core call sites
    /// (notably `make_op_scratch_with_binding` called from Phase G's
    /// lock-held window) assume the retain is a pure refcount bump.
    fn retain_handle(&self, _handle: HandleId) {}

    // -----------------------------------------------------------------
    // Operator FFI surface (Slice C-1, D009). Bulk projection methods
    // for the built-in operator dispatch path. Default impls panic so
    // bindings that don't ship operators (e.g., minimal test bindings
    // that only exercise raw fn-fire) don't pay the registry cost; the
    // dispatch path only routes here when an `Operator(_)` node fires,
    // so a binding that doesn't register operator nodes never reaches
    // these defaults.
    //
    // Each method returns the per-input result. Caller (Core) owns the
    // resulting handles' retains — the binding must `retain_handle`-bump
    // any handles it returns that share an existing registry slot.
    // -----------------------------------------------------------------

    /// `OperatorOp::Map` — element-wise transform. For each input handle,
    /// the binding dereferences to `T`, calls the user `Fn(T) -> R`, and
    /// returns the new handle. Output length equals input length.
    fn project_each(&self, _fn_id: FnId, _inputs: &[HandleId]) -> SmallVec<[HandleId; 1]> {
        unimplemented!("project_each: this binding does not support operators (D009)")
    }

    /// `OperatorOp::Filter` — element-wise predicate. For each input
    /// handle, the binding dereferences to `T`, calls the user
    /// `Fn(T) -> bool`, and returns the boolean. Output length equals
    /// input length.
    fn predicate_each(&self, _fn_id: FnId, _inputs: &[HandleId]) -> SmallVec<[bool; 4]> {
        unimplemented!("predicate_each: this binding does not support operators (D009)")
    }

    /// `OperatorOp::Scan` / `OperatorOp::Reduce` — left-fold. The binding
    /// dereferences `acc` to `R` and each input to `T`, runs
    /// `Fn(R, T) -> R` for each input feeding the previous result forward,
    /// and returns each intermediate `R` as a fresh handle. Output length
    /// equals input length (Scan emits each entry; Reduce uses only the
    /// last). The starting `acc` is owned by Core; the binding must NOT
    /// release it.
    fn fold_each(
        &self,
        _fn_id: FnId,
        _acc: HandleId,
        _inputs: &[HandleId],
    ) -> SmallVec<[HandleId; 1]> {
        unimplemented!("fold_each: this binding does not support operators (D009)")
    }

    /// `OperatorOp::Pairwise` — pack `(prev, current)` into a tuple value.
    /// Called once per pair (Core iterates and updates `prev` between
    /// calls). Returns the binding-side handle for the new tuple value.
    fn pairwise_pack(&self, _fn_id: FnId, _prev: HandleId, _current: HandleId) -> HandleId {
        unimplemented!("pairwise_pack: this binding does not support operators (D009)")
    }

    /// `OperatorOp::Combine` / `OperatorOp::WithLatestFrom` — pack N
    /// handles into a single tuple/array handle (Slice C-2, D020). The
    /// binding dereferences each input handle to `T`, constructs a tuple
    /// value (e.g., `[T0, T1, ..., Tn]`), and returns the new handle.
    /// The returned handle has a pre-bumped retain (caller takes ownership
    /// without additional `retain_handle` call).
    fn pack_tuple(&self, _fn_id: FnId, _handles: &[HandleId]) -> HandleId {
        unimplemented!("pack_tuple: this binding does not support combinator operators (D020)")
    }

    // -----------------------------------------------------------------
    // Producer lifecycle (Slice D, D031, D035). Producers are nodes
    // with no deps + a fn — fn fires once on first subscribe and may
    // call `Core::subscribe` from inside its body to wire up upstream
    // sources (the zip / concat / race / takeUntil pattern).
    //
    // The binding maintains its own per-producer state (subscription
    // handles, captured closure state) outside Core. When the LAST
    // subscriber unsubscribes from a producer, Core invokes
    // `producer_deactivate(node_id)` so the binding can drop that
    // state — which transitively drops the producer's upstream
    // subscriptions via `Subscription::Drop`.
    //
    // The hook fires lock-released (after the state lock is dropped),
    // so the binding's deactivation impl may re-enter Core if needed
    // (e.g., calling `release_handle` on captured handle shares).
    //
    // Symmetric with FnCtx: Core hands the binding a lifecycle signal
    // and lets the binding shape its ergonomic surface (the
    // `ProducerCtx` helper in `graphrefly-operators::producer` is one
    // such shape; bindings may roll their own).
    // -----------------------------------------------------------------

    /// Called when a producer node loses its last subscriber. The binding
    /// should drop any per-node state for `node_id` — captured closures,
    /// `Vec<Subscription>` to upstream sources, etc. Default no-op for
    /// bindings that don't ship producers.
    ///
    /// Fires lock-released; re-entrance into Core (e.g., `release_handle`
    /// on captured handle shares, or even `subscribe` for re-activation
    /// scenarios — though the latter is unusual) is permitted.
    fn producer_deactivate(&self, _node_id: NodeId) {}

    /// R1.3.8.c / Lock 6.A — synthesize an ERROR payload when a paused
    /// node's pause buffer overflows the configured cap. Called once per
    /// overflow event (the first drop of a pause cycle); the returned
    /// handle becomes the payload of `Message::Error` that Core then
    /// emits via the standard terminal cascade.
    ///
    /// Bindings that ship their own diagnostic shape (e.g. structured
    /// `{ code, nodeId, dropped, configuredMax, lockHeldDurationMs }`
    /// JSON for the JS / Python sides) override this to intern such a
    /// value and return its handle.
    ///
    /// **Default returns `None`** — Rust core falls back to the silent
    /// drop + `ResumeReport.dropped` path. This preserves backward
    /// compatibility for bindings that haven't yet wired up R1.3.8.c.
    /// New bindings SHOULD implement this method to satisfy the
    /// canonical-spec invariant.
    ///
    /// **Returning `None` consumes the overflow event for the cycle**
    /// (QA A9, 2026-05-07): the per-pause-cycle `overflow_reported` flag
    /// is set BEFORE this hook is called, so a `None` return doesn't
    /// re-attempt synthesis on subsequent overflows in the same cycle.
    /// If the binding is going to dynamically wire up the hook
    /// (configuration-driven), do so before any pause cycle that may
    /// overflow.
    ///
    /// Fires lock-released. The binding may re-enter Core (typical
    /// implementation just calls `intern_value` and returns).
    ///
    /// `lock_held_duration_ms` is the wall-clock-monotonic duration in
    /// milliseconds since the node first transitioned `Active → Paused`
    /// at the start of this pause cycle; it gives consumers a sense of
    /// how long a leaked controller held the lockset before overflow.
    /// Sub-millisecond durations truncate to `0` (QA A8, 2026-05-07).
    fn synthesize_pause_overflow_error(
        &self,
        _node_id: NodeId,
        _dropped_count: u32,
        _configured_max: usize,
        _lock_held_duration_ms: u64,
    ) -> Option<HandleId> {
        None
    }

    // -----------------------------------------------------------------
    // Cleanup-hook lifecycle (Slice E2 — R2.4.5 / R2.4.6 / Lock 4.A / 4.A′
    // / Lock 6.D). Decisions: D054 (lifecycle-trigger hooks; binding owns
    // ctx state), D055 (binding-side `Mutex<HashMap<NodeId, NodeCtxState>>`,
    // wipe only on resubscribable terminal reset), D056 (cleanup-first
    // before producer_deactivate), D057 (strict per-wave-per-node dedup),
    // D058 (fire at cache-clear time), D059 (one-shot current_cleanup on
    // OnDeactivation), D060 (binding-side panic isolation, drain
    // iterates-don't-short-circuit), D061 (panic-discard wave drops
    // deferred queue silently). Full design lives in
    // `~/src/graphrefly-ts/archive/docs/SESSION-rust-port-fn-ctx-cleanup.md`.
    //
    // Bindings opt in by overriding `cleanup_for` and `wipe_ctx`. Default
    // no-ops keep non-cleanup-aware bindings (e.g. minimal test bindings,
    // bench harnesses) compiling unchanged.
    // -----------------------------------------------------------------

    /// Fire a registered user-cleanup hook for `node_id` at the lifecycle
    /// moment indicated by `trigger`.
    ///
    /// # Binding-side contract
    ///
    /// Per D055, bindings own a `Mutex<HashMap<NodeId, NodeCtxState>>` where
    /// `NodeCtxState = { store, current_cleanup }`. On each `invoke_fn` the
    /// binding overwrites `current_cleanup` with whatever cleanup spec the
    /// user fn returned. When Core later fires `cleanup_for(node, trigger)`,
    /// the binding's impl looks up `current_cleanup.<trigger_slot>` and
    /// invokes it if present. Absent slots are no-ops.
    ///
    /// **Lock discipline.** Bindings MUST release their `node_ctx` lock
    /// before invoking the user closure (clone the closure handle out of
    /// the map, drop the lock, fire). Holding the binding-side lock
    /// across the user closure deadlocks if the user re-enters the
    /// binding's high-level API from inside the cleanup.
    ///
    /// **Re-entrance into Core (D045 / D060).** Permitted: `release_handle`,
    /// `Core::up(other_node, Pause/Resume/Invalidate/Teardown)`, `Core::emit`
    /// for unrelated nodes, `Core::subscribe` / drop a `Subscription`. Not
    /// permitted (undefined behavior): `Core::emit(self_node, ...)` from
    /// inside `OnRerun` (creates a fresh emit during the wave's pre-fire
    /// window); `Core::subscribe(self_node)` from inside `OnDeactivation`
    /// (self-resubscribe race during deactivation).
    ///
    /// **Panic isolation (D060).** Core stays panic-naive about user code
    /// — bindings SHOULD wrap user-closure invocations in `catch_unwind`
    /// and surface failures via the host language's idiom (JS exception
    /// → console.error, Python panic → warning, Rust panic → log + propagate
    /// per binding policy). Core's deferred-drain for `OnInvalidate` wraps
    /// each `cleanup_for` call in `catch_unwind` itself to prevent a single
    /// panic from short-circuiting the per-wave drain (all queued cleanup
    /// attempts run; the last panic re-raises after the drain completes).
    /// `OnRerun` and `OnDeactivation` fire inline lock-released — a
    /// panicking `cleanup_for` propagates out of `fire_regular` Phase 1.5
    /// or `Subscription::Drop` respectively (Drop guarantees apply: state
    /// lock already released).
    ///
    /// **Panic-discard guarantee gap (D061).** When a wave is panic-discarded
    /// (an `invoke_fn` panics mid-wave), the queued `OnInvalidate` cleanup
    /// hooks are dropped silently — the cascade's recorded cache-clears
    /// never fire their cleanups. External-resource cleanup (file handles,
    /// network sockets, external transactions) attached to `OnInvalidate`
    /// MUST be idempotent at process exit / next successful invalidate
    /// cycle. `OnRerun` panic-discard is moot (panic in `OnRerun` aborts
    /// the wave's `fire_regular` before it can corrupt state). `OnDeactivation`
    /// panic-discard is moot (Drop is invoked during stack unwinding;
    /// double-panic during drop aborts per `std::process::abort` semantics).
    ///
    /// **Per-trigger lifecycle.** See [`CleanupTrigger`] for per-variant
    /// firing rules. After each trigger fires, the binding decides whether
    /// to clear `current_cleanup`:
    /// - `OnRerun`: do NOT clear; the next `invoke_fn` will overwrite.
    /// - `OnInvalidate`: do NOT clear; multiple INVALIDATEs across waves
    ///   can re-fire the same closure.
    /// - `OnDeactivation`: clear (D059) — one-shot per activation cycle;
    ///   `store` persists separately per R2.4.6.
    ///
    /// Default no-op so bindings without cleanup-hook support compile
    /// unchanged.
    fn cleanup_for(&self, _node_id: NodeId, _trigger: CleanupTrigger) {}

    // -----------------------------------------------------------------
    // Snapshot serialization (M4.E1 — D166). The Core operates on
    // opaque HandleId integers; snapshot persistence needs to cross
    // the cleaving plane to serialize/deserialize user values as JSON.
    // Default impls return None / panic so bindings without snapshot
    // support compile unchanged.
    // -----------------------------------------------------------------

    /// Serialize a handle's value to JSON for snapshot persistence.
    /// Returns `None` if the handle is unknown or unresolvable.
    ///
    /// Called by `Graph::snapshot()` for each node's cache handle.
    /// The binding dereferences `handle` to its underlying `T` and
    /// produces a `serde_json::Value` that will survive serialization
    /// round-trips (i.e., JSON-safe: no functions, no circular refs).
    ///
    /// Default returns `None` — bindings that don't support snapshots
    /// get `value: null` in the snapshot output.
    fn serialize_handle(&self, _handle: HandleId) -> Option<serde_json::Value> {
        None
    }

    /// Deserialize a JSON value back into a handle for snapshot restore.
    /// The binding interns the value and returns its `HandleId`.
    ///
    /// Called by `Graph::restore()` / `Graph::from_snapshot()` for each
    /// node's serialized value.
    ///
    /// Default panics — bindings that restore snapshots MUST override.
    fn deserialize_value(&self, _value: serde_json::Value) -> HandleId {
        unimplemented!("deserialize_value: this binding does not support snapshot restore (D166)")
    }

    /// Wipe the binding-side ctx state for `node_id`.
    ///
    /// Called by Core ONLY on resubscribable terminal reset, per **R2.4.6**:
    /// `ctx.store` is "wiped automatically: on resubscribable terminal
    /// reset (when a `resubscribable: true` node hits `COMPLETE`/`ERROR`
    /// and is later resubscribed)". Bindings drop their `NodeCtxState`
    /// entry for `node_id`, releasing both `store` and any residual
    /// `current_cleanup`.
    ///
    /// Default deactivation does NOT trigger wipe — `store` persists
    /// across deactivation/reactivation cycles by spec design (mirrored
    /// here to match canonical; current TS impl wipes on deactivation
    /// per docstring at `node.ts:189-190` and is on the Phase 13.6.B
    /// migration list per canonical-spec §11 item 3).
    ///
    /// Fires lock-released. Re-entrance into Core is permitted; typical
    /// implementations just call `node_ctx.lock().remove(&node_id)`.
    /// Default no-op.
    fn wipe_ctx(&self, _node_id: NodeId) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handle::{FnId, HandleId, NodeId};
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Test double mirroring `bindings.ts` `BindingBoundary` test patterns —
    /// counts FFI crossings per method to verify the cleaving plane's
    /// "zero FFI on identity-equals path" claim experimentally.
    #[allow(clippy::struct_field_names)]
    struct TestBinding {
        invoke_count: AtomicU64,
        equals_count: AtomicU64,
        release_count: AtomicU64,
    }

    impl TestBinding {
        fn new() -> Self {
            Self {
                invoke_count: AtomicU64::new(0),
                equals_count: AtomicU64::new(0),
                release_count: AtomicU64::new(0),
            }
        }
    }

    impl BindingBoundary for TestBinding {
        fn invoke_fn(&self, _node_id: NodeId, _fn_id: FnId, dep_data: &[DepBatch]) -> FnResult {
            self.invoke_count.fetch_add(1, Ordering::SeqCst);
            // Echo first dep's latest handle as result; not realistic but exercises the path.
            let handle = dep_data.first().map_or(HandleId::new(99), DepBatch::latest);
            FnResult::Data {
                handle,
                tracked: None,
            }
        }

        fn custom_equals(&self, _equals_handle: FnId, a: HandleId, b: HandleId) -> bool {
            self.equals_count.fetch_add(1, Ordering::SeqCst);
            a == b
        }

        fn release_handle(&self, _handle: HandleId) {
            self.release_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn boundary_calls_route_correctly() {
        let b = TestBinding::new();
        let dep = DepBatch {
            data: smallvec::smallvec![HandleId::new(7)],
            prev_data: NO_HANDLE,
            involved: true,
        };
        let result = b.invoke_fn(NodeId::new(1), FnId::new(2), &[dep]);
        match result {
            FnResult::Data { handle, .. } => assert_eq!(handle, HandleId::new(7)),
            FnResult::Noop { .. } | FnResult::Batch { .. } => panic!("expected Data variant"),
        }
        assert!(b.custom_equals(FnId::new(3), HandleId::new(7), HandleId::new(7)));
        assert!(!b.custom_equals(FnId::new(3), HandleId::new(7), HandleId::new(8)));
        b.release_handle(HandleId::new(7));

        assert_eq!(b.invoke_count.load(Ordering::SeqCst), 1);
        assert_eq!(b.equals_count.load(Ordering::SeqCst), 2);
        assert_eq!(b.release_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn binding_is_send_and_sync() {
        // Compile-time check: BindingBoundary impls must be Send + Sync.
        // If TestBinding ever gains a !Send field, this fails to compile.
        fn assert_send_sync<T: Send + Sync>() {}
        // dyn-trait variant is the production shape (Core holds Arc<dyn BindingBoundary>).
        fn assert_dyn_send_sync<T: ?Sized + Send + Sync>() {}
        assert_send_sync::<TestBinding>();
        assert_dyn_send_sync::<dyn BindingBoundary>();
    }
}
