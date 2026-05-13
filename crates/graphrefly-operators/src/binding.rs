//! [`OperatorBinding`] — closure-registration helper trait for the
//! operators crate. Sub-trait of [`BindingBoundary`] (D015).
//!
//! Operator factories ([`super::transform::map`], etc.) accept user
//! closures of shape `Fn(T) -> R` / `Fn(T) -> bool` / `Fn(R, T) -> R`.
//! At the FFI plane (per the handle-protocol cleaving plane invariant),
//! Core only ever sees opaque [`HandleId`] integers. Bindings therefore
//! need to wrap user closures into the `Fn(HandleId) -> HandleId` shape
//! (and friends) — that wrapping is binding-side because it requires
//! deref+intern operations against the binding's value registry, and
//! returning a [`FnId`] that Core stores in the [`OperatorOp`] variant.
//!
//! Bindings impl both [`BindingBoundary`] (Core-callable FFI for
//! `project_each` / `predicate_each` / `fold_each` / `pairwise_pack`) AND
//! [`OperatorBinding`] (the closure-registration calls below).
//!
//! See [`OperatorOp`] for the per-operator FFI dispatch and `D016` in
//! `docs/rust-port-decisions.md` for the wrapping discipline.
//!
//! [`BindingBoundary`]: graphrefly_core::BindingBoundary
//! [`HandleId`]: graphrefly_core::HandleId
//! [`FnId`]: graphrefly_core::FnId
//! [`OperatorOp`]: graphrefly_core::OperatorOp

use graphrefly_core::{BindingBoundary, FnId, HandleId};

/// Closure-registration interface used by transform-operator factories.
///
/// Each method takes ownership of a user closure (boxed for type erasure)
/// and returns the [`FnId`] under which the binding registered it. The
/// operator factory then passes that `FnId` into [`graphrefly_core::OperatorOp`]
/// so the wave engine's per-fire FFI calls can reach back through the
/// registry.
///
/// # Closure shape
///
/// All closures take and return [`HandleId`] (or `bool` for predicates) —
/// not `T` / `R`. Wrapping `Fn(T) -> R` into `Fn(HandleId) -> HandleId` is
/// a binding-side concern: deref incoming handles to `T`, run user code,
/// intern the output to a fresh `HandleId`. See `D016` for rationale.
///
/// Implementations must be `Send + Sync` for the closure storage so the
/// binding can be shared across threads (matching `BindingBoundary`'s
/// `Send + Sync` super-bounds).
pub trait OperatorBinding: BindingBoundary {
    /// Register a single-arg projector: `Fn(T) -> R` wrapped into
    /// `Fn(HandleId) -> HandleId`. Used by [`super::transform::map`].
    /// The returned [`FnId`] is passed to
    /// [`graphrefly_core::OperatorOp::Map`].
    fn register_projector(&self, f: Box<dyn Fn(HandleId) -> HandleId + Send + Sync>) -> FnId;

    /// Register a single-arg predicate: `Fn(T) -> bool`. Used by
    /// [`super::transform::filter`]. The returned [`FnId`] is passed to
    /// [`graphrefly_core::OperatorOp::Filter`].
    fn register_predicate(&self, f: Box<dyn Fn(HandleId) -> bool + Send + Sync>) -> FnId;

    /// Register a left-fold reducer: `Fn(R, T) -> R` wrapped into
    /// `Fn(HandleId, HandleId) -> HandleId`. Used by
    /// [`super::transform::scan`] and [`super::transform::reduce`]. The
    /// returned [`FnId`] is passed to [`graphrefly_core::OperatorOp::Scan`]
    /// or [`graphrefly_core::OperatorOp::Reduce`].
    fn register_folder(&self, f: Box<dyn Fn(HandleId, HandleId) -> HandleId + Send + Sync>)
        -> FnId;

    /// Register a custom-equals oracle: `Fn(T, T) -> bool` reused via
    /// [`BindingBoundary::custom_equals`]. Used by
    /// [`super::transform::distinct_until_changed`]. The returned
    /// [`FnId`] is passed to
    /// [`graphrefly_core::OperatorOp::DistinctUntilChanged`].
    fn register_equals(&self, f: Box<dyn Fn(HandleId, HandleId) -> bool + Send + Sync>) -> FnId;

    /// Register a pairwise packer: `Fn(prev: T, current: T) -> (T, T)`
    /// wrapped into `Fn(HandleId, HandleId) -> HandleId` (where the
    /// returned handle resolves to the binding's tuple representation).
    /// Used by [`super::transform::pairwise`]. The returned [`FnId`] is
    /// passed to [`graphrefly_core::OperatorOp::Pairwise`].
    fn register_pairwise_packer(
        &self,
        f: Box<dyn Fn(HandleId, HandleId) -> HandleId + Send + Sync>,
    ) -> FnId;

    /// Register a tuple packer: `Fn(&[T]) -> Tuple` wrapped into
    /// `Fn(&[HandleId]) -> HandleId` (where the returned handle resolves
    /// to the binding's N-ary tuple representation).
    /// Used by [`super::combine::combine`] and
    /// [`super::combine::with_latest_from`]. The returned [`FnId`] is
    /// passed to [`graphrefly_core::OperatorOp::Combine`] or
    /// [`graphrefly_core::OperatorOp::WithLatestFrom`].
    fn register_packer(&self, f: super::combine::PackerFn) -> FnId;

    /// Register a side-effect callback: `Fn(T)` wrapped into
    /// `Fn(HandleId)`. Used by [`super::control::tap`] and
    /// [`super::control::on_first_data`]. The returned [`FnId`] is passed
    /// to [`graphrefly_core::OperatorOp::Tap`] or
    /// [`graphrefly_core::OperatorOp::TapFirst`]. The binding invokes
    /// the stored closure from
    /// [`BindingBoundary::invoke_tap_fn`](graphrefly_core::BindingBoundary::invoke_tap_fn).
    fn register_tap(&self, _f: Box<dyn Fn(HandleId) + Send + Sync>) -> FnId {
        unimplemented!("register_tap: this binding does not support control operators")
    }

    /// Register an error-recovery callback: `Fn(HandleId) -> HandleId`.
    /// Used by [`super::control::rescue`]. The binding invokes the stored
    /// closure from
    /// [`BindingBoundary::invoke_rescue_fn`](graphrefly_core::BindingBoundary::invoke_rescue_fn).
    fn register_rescue(
        &self,
        _f: Box<dyn Fn(HandleId) -> Result<HandleId, ()> + Send + Sync>,
    ) -> FnId {
        unimplemented!("register_rescue: this binding does not support control operators")
    }
}
