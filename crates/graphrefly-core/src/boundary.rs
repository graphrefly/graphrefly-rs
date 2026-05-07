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
    fn release_handle(&self, handle: HandleId);

    /// Increment the refcount on `handle`. Called when the Core takes an
    /// additional reference to an existing handle that the binding side
    /// already interned — currently used by the pause buffer (a buffered
    /// `Data(H)` outlives the cache slot that originally interned `H`, so
    /// the buffer needs its own refcount share to keep `H` alive across
    /// later cache replacements).
    ///
    /// Default: no-op. Bindings that don't track refcounts (e.g., trivial
    /// always-alive registries) can leave the default. Bindings that do
    /// (TS `bindings.ts`, the test runtime, the napi-rs bench harness) must
    /// override to bump the per-handle refcount.
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
