//! Transform operators (R5.7) — element-wise mappings and folds.
//!
//! Mirrors the TS shapes in
//! `~/src/graphrefly-ts/packages/legacy-pure-ts/src/extra/operators/transform.ts`,
//! but driven by Core dispatch ([`graphrefly_core::OperatorOp`]) instead
//! of derived-fn factories. Per Slice C-1 / D009.
//!
//! Each factory takes `&Core`, an `&Arc<dyn OperatorBinding>` (for
//! registering the user closure), the `source` node id, the user-supplied
//! callback, and any operator-specific arguments (seeds for folders).
//! The factory:
//!
//! 1. Registers the user closure on the binding (returns a [`FnId`]).
//! 2. Calls [`Core::register_operator`] with the appropriate
//!    [`OperatorOp`] variant.
//! 3. Returns an [`OperatorRegistration`] carrying the new node id and
//!    (for folders) the registered seed handle.
//!
//! # Refcount discipline
//!
//! For [`scan`] and [`reduce`], the seed handle ownership transfers from
//! the caller's binding-side intern into Core's `operator_state` slot.
//! Core takes one retain via [`BindingBoundary::retain_handle`] inside
//! `register_operator`; the caller is expected to retain a share for
//! themselves if they want to reference the seed elsewhere.

use std::sync::Arc;

use graphrefly_core::{Core, FnId, HandleId, NodeId, OperatorOp, OperatorOpts};

use crate::binding::OperatorBinding;

/// Output of an operator factory call. Carries the new operator node id
/// plus operator-specific context (e.g., the registered closure's `FnId`
/// for traceability / debugging).
#[derive(Copy, Clone, Debug)]
#[must_use = "the operator's NodeId is the value of registering it"]
pub struct OperatorRegistration {
    pub node: NodeId,
    pub fn_id: FnId,
}

impl OperatorRegistration {
    /// Convenience: extract just the node id (the typical caller need).
    #[must_use]
    pub fn into_node(self) -> NodeId {
        self.node
    }
}

impl From<OperatorRegistration> for NodeId {
    fn from(r: OperatorRegistration) -> Self {
        r.node
    }
}

// ---------------------------------------------------------------------
// Stateless: map, filter
// ---------------------------------------------------------------------

/// `map(source, project)` — element-wise transform.
///
/// Maps each settled value from `source` through `project`. Each input
/// in a wave's batch produces one output (R5.7 batch-mapping).
///
/// # Example
///
/// ```ignore
/// use graphrefly_operators::map;
/// let mapped = map(&core, &binding, source, |h: HandleId| { /* deref + transform */ });
/// ```
pub fn map<F>(
    core: &Core,
    binding: &Arc<dyn OperatorBinding>,
    source: NodeId,
    project: F,
) -> OperatorRegistration
where
    F: Fn(HandleId) -> HandleId + Send + Sync + 'static,
{
    map_with(core, binding, source, project, OperatorOpts::default())
}

/// [`map`] with explicit [`OperatorOpts`].
pub fn map_with<F>(
    core: &Core,
    binding: &Arc<dyn OperatorBinding>,
    source: NodeId,
    project: F,
    opts: OperatorOpts,
) -> OperatorRegistration
where
    F: Fn(HandleId) -> HandleId + Send + Sync + 'static,
{
    let fn_id = binding.register_projector(Box::new(project));
    let node = core.register_operator(&[source], OperatorOp::Map { fn_id }, opts);
    OperatorRegistration { node, fn_id }
}

/// `filter(source, predicate)` — silent-drop selection (D012/D018).
///
/// Forwards values where `predicate` returns `true`. Mixed-batch waves
/// emit `[Dirty, Data(v_pass), ...]` per passing item with no settle
/// noise for dropped items. Full-reject waves emit `[Dirty, Resolved]`
/// to settle (D018).
pub fn filter<F>(
    core: &Core,
    binding: &Arc<dyn OperatorBinding>,
    source: NodeId,
    predicate: F,
) -> OperatorRegistration
where
    F: Fn(HandleId) -> bool + Send + Sync + 'static,
{
    filter_with(core, binding, source, predicate, OperatorOpts::default())
}

/// [`filter`] with explicit [`OperatorOpts`].
pub fn filter_with<F>(
    core: &Core,
    binding: &Arc<dyn OperatorBinding>,
    source: NodeId,
    predicate: F,
    opts: OperatorOpts,
) -> OperatorRegistration
where
    F: Fn(HandleId) -> bool + Send + Sync + 'static,
{
    let fn_id = binding.register_predicate(Box::new(predicate));
    let node = core.register_operator(&[source], OperatorOp::Filter { fn_id }, opts);
    OperatorRegistration { node, fn_id }
}

// ---------------------------------------------------------------------
// Stateful folders: scan, reduce
// ---------------------------------------------------------------------

/// `scan(source, fold, seed)` — left-fold emitting each new accumulator.
///
/// Required `seed`: there is no seedless mode where the first value
/// becomes the accumulator (matches TS legacy). The seed handle must be
/// pre-registered by the caller with the binding (so it has a real
/// [`HandleId`]). Core takes one retain on the seed for the
/// `operator_state` slot's lifetime.
pub fn scan<F>(
    core: &Core,
    binding: &Arc<dyn OperatorBinding>,
    source: NodeId,
    fold: F,
    seed: HandleId,
) -> OperatorRegistration
where
    F: Fn(HandleId, HandleId) -> HandleId + Send + Sync + 'static,
{
    scan_with(core, binding, source, fold, seed, OperatorOpts::default())
}

/// [`scan`] with explicit [`OperatorOpts`].
pub fn scan_with<F>(
    core: &Core,
    binding: &Arc<dyn OperatorBinding>,
    source: NodeId,
    fold: F,
    seed: HandleId,
    opts: OperatorOpts,
) -> OperatorRegistration
where
    F: Fn(HandleId, HandleId) -> HandleId + Send + Sync + 'static,
{
    let fn_id = binding.register_folder(Box::new(fold));
    let node = core.register_operator(&[source], OperatorOp::Scan { fn_id, seed }, opts);
    OperatorRegistration { node, fn_id }
}

/// `reduce(source, fold, seed)` — left-fold emitting once on upstream
/// COMPLETE.
///
/// Accumulates silently while `source` emits DATA; on `source` COMPLETE,
/// emits `[Dirty, Data(acc), Complete]` (where `acc` is the seed if no
/// DATA arrived). On `source` ERROR, propagates the error verbatim
/// without emitting the partial accumulator.
pub fn reduce<F>(
    core: &Core,
    binding: &Arc<dyn OperatorBinding>,
    source: NodeId,
    fold: F,
    seed: HandleId,
) -> OperatorRegistration
where
    F: Fn(HandleId, HandleId) -> HandleId + Send + Sync + 'static,
{
    reduce_with(core, binding, source, fold, seed, OperatorOpts::default())
}

/// [`reduce`] with explicit [`OperatorOpts`].
pub fn reduce_with<F>(
    core: &Core,
    binding: &Arc<dyn OperatorBinding>,
    source: NodeId,
    fold: F,
    seed: HandleId,
    opts: OperatorOpts,
) -> OperatorRegistration
where
    F: Fn(HandleId, HandleId) -> HandleId + Send + Sync + 'static,
{
    let fn_id = binding.register_folder(Box::new(fold));
    let node = core.register_operator(&[source], OperatorOp::Reduce { fn_id, seed }, opts);
    OperatorRegistration { node, fn_id }
}

// ---------------------------------------------------------------------
// Stateful pair-aware: distinctUntilChanged, pairwise
// ---------------------------------------------------------------------

/// `distinctUntilChanged(source, equals)` — suppresses adjacent duplicates.
///
/// Each input is compared against the previous emitted value via
/// `equals`. If equal (suppressed), no output. If not equal, emit
/// verbatim and update prev. First-ever input is always emitted.
///
/// # Default identity
///
/// For Identity-equals (the common case), pass a closure that returns
/// `a == b`. For deeper comparison (`Object.is`-style or struct-equal),
/// the binding should deref both handles and call the user's
/// `Fn(T, T) -> bool` underneath.
pub fn distinct_until_changed<F>(
    core: &Core,
    binding: &Arc<dyn OperatorBinding>,
    source: NodeId,
    equals: F,
) -> OperatorRegistration
where
    F: Fn(HandleId, HandleId) -> bool + Send + Sync + 'static,
{
    distinct_until_changed_with(core, binding, source, equals, OperatorOpts::default())
}

/// [`distinct_until_changed`] with explicit [`OperatorOpts`].
pub fn distinct_until_changed_with<F>(
    core: &Core,
    binding: &Arc<dyn OperatorBinding>,
    source: NodeId,
    equals: F,
    opts: OperatorOpts,
) -> OperatorRegistration
where
    F: Fn(HandleId, HandleId) -> bool + Send + Sync + 'static,
{
    let equals_fn_id = binding.register_equals(Box::new(equals));
    let node = core.register_operator(
        &[source],
        OperatorOp::DistinctUntilChanged { equals_fn_id },
        opts,
    );
    OperatorRegistration {
        node,
        fn_id: equals_fn_id,
    }
}

/// `pairwise(source)` — emits `(prev, current)` pairs starting after the
/// second value. First value is swallowed (used as `prev`).
///
/// The `pack` closure is provided by the binding-side wrapping helper —
/// it takes two handles and returns a tuple-handle. For tests that
/// don't care about the pair representation, `pack = |_, b| b` is a
/// valid degenerate implementation that emits the current value only;
/// production bindings install a real pair-packer.
pub fn pairwise<F>(
    core: &Core,
    binding: &Arc<dyn OperatorBinding>,
    source: NodeId,
    pack: F,
) -> OperatorRegistration
where
    F: Fn(HandleId, HandleId) -> HandleId + Send + Sync + 'static,
{
    pairwise_with(core, binding, source, pack, OperatorOpts::default())
}

/// [`pairwise`] with explicit [`OperatorOpts`].
pub fn pairwise_with<F>(
    core: &Core,
    binding: &Arc<dyn OperatorBinding>,
    source: NodeId,
    pack: F,
    opts: OperatorOpts,
) -> OperatorRegistration
where
    F: Fn(HandleId, HandleId) -> HandleId + Send + Sync + 'static,
{
    let fn_id = binding.register_pairwise_packer(Box::new(pack));
    let node = core.register_operator(&[source], OperatorOp::Pairwise { fn_id }, opts);
    OperatorRegistration { node, fn_id }
}
