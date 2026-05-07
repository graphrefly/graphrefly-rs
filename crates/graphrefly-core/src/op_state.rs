//! Generic per-node operator state storage (D026).
//!
//! Each `OperatorOp` variant that needs cross-wave state defines a state
//! struct implementing [`OperatorScratch`]. The struct lives behind
//! `Box<dyn OperatorScratch>` in [`NodeRecord::op_scratch`](super::node::NodeRecord),
//! one slot per operator node. The trait carries a `release_handles`
//! method so `Drop for CoreState` and `reset_for_fresh_lifecycle` can
//! release any handles owned by the state without per-operator match
//! arms.
//!
//! # Why a generic slot
//!
//! Older operators ([`OperatorOp::Scan`](super::node::OperatorOp::Scan) /
//! [`Reduce`](super::node::OperatorOp::Reduce) /
//! [`DistinctUntilChanged`](super::node::OperatorOp::DistinctUntilChanged) /
//! [`Pairwise`](super::node::OperatorOp::Pairwise)) used a typed
//! `operator_state: HandleId` field. New flow operators (Take / Skip)
//! need a `u32` counter; Last needs a buffered handle plus a registered
//! default. Different shapes don't fit a single typed field cleanly.
//! Generic scratch scales to N operators with one field; the
//! [`OperatorScratch::release_handles`] method consolidates refcount
//! discipline across all variants.
//!
//! # Refcount discipline
//!
//! Each state struct is responsible for retaining handles it owns when
//! it stores them, releasing the prior handle on overwrite, and
//! releasing all current handles via [`OperatorScratch::release_handles`]
//! when the slot is reset or the Core drops. The state struct itself
//! holds raw [`HandleId`] integers — Core takes the retain on initial
//! population, the struct's owner keeps the share alive, and
//! `release_handles` returns the share at end-of-life.
//!
//! # Lifecycle hooks
//!
//! - **Registration:** `register_operator` allocates the state struct
//!   (via `T::default()` followed by an explicit init for variants with
//!   a registered seed/default) and retains any handles the seed
//!   carries.
//! - **Per-fire update:** `fire_op_*` helpers mutate the struct via
//!   [`Core::op_scratch_mut`](super::node::Core).
//! - **`reset_for_fresh_lifecycle`** (resubscribable terminal cycle):
//!   calls `release_handles`, then either re-seeds (Scan/Reduce) or
//!   clears (Distinct/Pairwise/Take/Skip/Last) the struct.
//! - **`Drop for CoreState`:** walks every node, calls `release_handles`
//!   on its scratch slot, then drops the boxed struct.

use std::any::Any;

use crate::boundary::BindingBoundary;
use crate::handle::{HandleId, NO_HANDLE};

/// Trait for per-operator runtime state that lives in
/// [`NodeRecord::op_scratch`](super::node::NodeRecord).
///
/// Implementors are concrete state structs (e.g., [`ScanState`],
/// [`TakeState`]). Each declares which handles it owns; the
/// [`release_handles`](OperatorScratch::release_handles) method is the
/// single consolidated release point called from
/// [`reset_for_fresh_lifecycle`](super::node::Core) and
/// [`Drop for CoreState`](super::node::CoreState).
pub(crate) trait OperatorScratch: Any + Send + Sync + std::fmt::Debug {
    /// Release any [`HandleId`] shares this state currently holds. Called
    /// once at end-of-life (Core drop or resubscribable reset). After
    /// the call returns, the state struct's handles are considered
    /// released — future calls on the same instance must not re-release.
    /// (Default implementations typically clear any handle fields to
    /// [`NO_HANDLE`] before returning to enforce that.)
    ///
    /// **Leaf-operation invariant (Slice C-3 /qa P7):** the binding's
    /// [`release_handle`](BindingBoundary::release_handle) implementation
    /// MUST be a leaf operation — no Core re-entrance permitted. Both
    /// call sites ([`Drop for CoreState`](super::node::CoreState) walking
    /// `nodes.values_mut()`, and `reset_for_fresh_lifecycle` mid-borrow)
    /// invoke `release_handles` while structurally inside Core. A
    /// binding that re-enters Core (e.g., emits / pauses / subscribes
    /// from inside a release) would observe partial state and risk
    /// deadlock against the held mutex / iterator. Production bindings
    /// (napi-rs, pyo3, wasm-bindgen) MUST keep refcount paths
    /// callback-free.
    fn release_handles(&mut self, binding: &dyn BindingBoundary);

    /// Downcast hook (mutable) — all impls return `self`. Required
    /// because [`Box<dyn OperatorScratch>`] doesn't auto-coerce to
    /// [`Box<dyn Any>`] across crate boundaries.
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// Downcast hook (immutable) — all impls return `self`.
    fn as_any_ref(&self) -> &dyn Any;
}

// =====================================================================
// Concrete state structs (one per stateful OperatorOp variant)
// =====================================================================

/// Scan / Reduce running accumulator. Initialized with the operator's
/// seed at registration; updated to the latest fold result per fire.
///
/// **Refcount:** the struct owns one share of `acc` whenever it is
/// non-`NO_HANDLE`. Per-fire updates retain the new value before
/// releasing the old.
#[derive(Debug)]
pub(crate) struct ScanState {
    pub(crate) acc: HandleId,
}

impl Default for ScanState {
    fn default() -> Self {
        Self { acc: NO_HANDLE }
    }
}

impl OperatorScratch for ScanState {
    fn release_handles(&mut self, binding: &dyn BindingBoundary) {
        if self.acc != NO_HANDLE {
            binding.release_handle(self.acc);
            self.acc = NO_HANDLE;
        }
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

/// Reduce running accumulator. Identical shape to [`ScanState`] but
/// kept as a distinct type so `op_scratch_mut::<ReduceState>` doesn't
/// collide with Scan in shared code paths (debug ergonomics).
#[derive(Debug)]
pub(crate) struct ReduceState {
    pub(crate) acc: HandleId,
}

impl Default for ReduceState {
    fn default() -> Self {
        Self { acc: NO_HANDLE }
    }
}

impl OperatorScratch for ReduceState {
    fn release_handles(&mut self, binding: &dyn BindingBoundary) {
        if self.acc != NO_HANDLE {
            binding.release_handle(self.acc);
            self.acc = NO_HANDLE;
        }
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

/// DistinctUntilChanged previous emitted value. `NO_HANDLE` until the
/// first emission.
#[derive(Debug)]
pub(crate) struct DistinctState {
    pub(crate) prev: HandleId,
}

impl Default for DistinctState {
    fn default() -> Self {
        Self { prev: NO_HANDLE }
    }
}

impl OperatorScratch for DistinctState {
    fn release_handles(&mut self, binding: &dyn BindingBoundary) {
        if self.prev != NO_HANDLE {
            binding.release_handle(self.prev);
            self.prev = NO_HANDLE;
        }
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

/// Pairwise previous value. Same shape as [`DistinctState`] but a
/// distinct type for downcast clarity.
#[derive(Debug)]
pub(crate) struct PairwiseState {
    pub(crate) prev: HandleId,
}

impl Default for PairwiseState {
    fn default() -> Self {
        Self { prev: NO_HANDLE }
    }
}

impl OperatorScratch for PairwiseState {
    fn release_handles(&mut self, binding: &dyn BindingBoundary) {
        if self.prev != NO_HANDLE {
            binding.release_handle(self.prev);
            self.prev = NO_HANDLE;
        }
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

// ---- Slice C-3: flow operators ----

/// Take(n) emission counter. Increments per emitted DATA; on
/// `count_emitted >= count`, the operator self-completes.
///
/// No handles owned — `release_handles` is a no-op.
#[derive(Debug, Default)]
pub(crate) struct TakeState {
    pub(crate) count_emitted: u32,
}

impl OperatorScratch for TakeState {
    fn release_handles(&mut self, _binding: &dyn BindingBoundary) {
        // No handles owned; reset counter so a resubscribable cycle
        // restarts at zero.
        self.count_emitted = 0;
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

/// Skip(n) drop counter. Increments per dropped DATA; once
/// `count_skipped >= count`, subsequent DATAs pass through.
#[derive(Debug, Default)]
pub(crate) struct SkipState {
    pub(crate) count_skipped: u32,
}

impl OperatorScratch for SkipState {
    fn release_handles(&mut self, _binding: &dyn BindingBoundary) {
        self.count_skipped = 0;
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

/// TakeWhile state — currently empty. `fire_operator`'s
/// `terminal.is_some()` short-circuit (set by the operator's own
/// `Core::complete` self-trigger) is the sole gate against re-entry
/// after the predicate's first `false`. Kept as a struct (rather than
/// dropping the scratch entirely) so the dispatch invariant "every
/// stateful operator has an `op_scratch`" stays uniform — useful when
/// future TakeWhile variants want to add per-fire diagnostics.
#[derive(Debug, Default)]
pub(crate) struct TakeWhileState;

impl OperatorScratch for TakeWhileState {
    fn release_handles(&mut self, _binding: &dyn BindingBoundary) {
        // No handles owned and no counter — no-op.
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

/// Last buffered value plus optional default.
///
/// **Refcount:**
/// - `latest`: one share whenever the operator has seen any DATA
///   (`NO_HANDLE` until first DATA). Updated each wave: retain new,
///   release old.
/// - `default`: one share if the operator was registered with a default
///   (`NO_HANDLE` for the no-default factory). Stable for the
///   operator's lifetime; only released by `release_handles`.
#[derive(Debug)]
pub(crate) struct LastState {
    pub(crate) latest: HandleId,
    pub(crate) default: HandleId,
}

impl Default for LastState {
    fn default() -> Self {
        Self {
            latest: NO_HANDLE,
            default: NO_HANDLE,
        }
    }
}

impl OperatorScratch for LastState {
    fn release_handles(&mut self, binding: &dyn BindingBoundary) {
        if self.latest != NO_HANDLE {
            binding.release_handle(self.latest);
            self.latest = NO_HANDLE;
        }
        if self.default != NO_HANDLE {
            binding.release_handle(self.default);
            self.default = NO_HANDLE;
        }
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

// =====================================================================
// Send + Sync compile-time asserts (Slice C-3 /qa P9)
// =====================================================================
//
// Each concrete state struct must be `Send + Sync` because
// `Box<dyn OperatorScratch>` lives behind the `parking_lot::Mutex<CoreState>`
// boundary, and `Core` itself is asserted `Send + Sync` (see
// `boundary.rs` and `flow.rs::flow_registration_is_send_and_sync`).
// Without these per-struct asserts, a future state field that
// accidentally introduces a `!Send` (e.g., `Cell<HandleId>` or
// `Rc<...>`) would only break at the trait-object level — slow to
// diagnose. The asserts below catch the regression at the offending
// type's definition.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<ScanState>();
    assert_send_sync::<ReduceState>();
    assert_send_sync::<DistinctState>();
    assert_send_sync::<PairwiseState>();
    assert_send_sync::<TakeState>();
    assert_send_sync::<SkipState>();
    assert_send_sync::<TakeWhileState>();
    assert_send_sync::<LastState>();
    // Trait-object-level check (defensive).
    assert_send_sync::<Box<dyn OperatorScratch>>();
};
