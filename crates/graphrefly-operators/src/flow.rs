//! Flow operators (Slice C-3, D024) — count / predicate / terminal-aware
//! gates that bound which `DATA` reaches the downstream output.
//!
//! Mirrors the TS shapes in
//! `~/src/graphrefly-ts/packages/legacy-pure-ts/src/extra/operators/take.ts`,
//! driven by Core dispatch ([`graphrefly_core::OperatorOp::Take`] /
//! [`Skip`] / [`TakeWhile`] / [`Last`]) instead of derived-fn factories.
//!
//! - [`take`] — emits the first `count` DATA values then self-completes.
//!   `count == 0` is allowed (D027): self-completes on first fire with
//!   no `Data`.
//! - [`skip`] — drops the first `count` DATA values; emits the rest.
//! - [`take_while`] — emits while `predicate` holds; on first `false`,
//!   emits any preceding passes then self-completes.
//! - [`last`] / [`last_with_default`] — buffers the latest `DATA`;
//!   emits `Data(latest)` (or `Data(default)` if no DATA arrived and a
//!   default was registered) then `Complete` on upstream COMPLETE.
//! - [`first`] — sugar for `take(source, 1)`.
//! - [`find`] — sugar for `take(filter(source, predicate), 1)`.
//! - [`element_at`] — sugar for `take(skip(source, index), 1)`.
//!
//! `take_until(source, notifier)` is intentionally NOT in this slice —
//! it requires the producer / subscription-managed pattern (D020
//! category B), out of scope for the Core-dispatch operator family.
//!
//! # Refcount discipline
//!
//! For [`last_with_default`], the `default` handle ownership transfers
//! from the caller's binding-side intern into Core's
//! [`LastState`](graphrefly_core::op_state::LastState) via a retain
//! taken inside `register_operator`. The caller is expected to retain a
//! share for themselves if they want to reference the default
//! elsewhere.

use std::sync::Arc;

use graphrefly_core::{Core, HandleId, NodeId, OperatorOp, OperatorOpts, NO_HANDLE};

use crate::binding::OperatorBinding;
use crate::transform::{filter, OperatorRegistration};

/// Registration output for flow operators that don't carry a user
/// closure ([`take`], [`skip`], [`last`], [`last_with_default`]). Zero
/// FFI on the fire path; only the `node` id matters.
#[derive(Copy, Clone, Debug)]
#[must_use = "the flow operator's NodeId is the value of registering it"]
pub struct FlowRegistration {
    pub node: NodeId,
}

impl FlowRegistration {
    /// Convenience: extract just the node id.
    #[must_use]
    pub fn into_node(self) -> NodeId {
        self.node
    }
}

impl From<FlowRegistration> for NodeId {
    fn from(r: FlowRegistration) -> Self {
        r.node
    }
}

// ---------------------------------------------------------------------
// Count-based flow: take, skip
// ---------------------------------------------------------------------

/// `take(source, count)` — emits the first `count` DATA values then
/// self-completes via `Core::complete`. When upstream completes before
/// `count` is reached, the standard auto-cascade propagates COMPLETE.
///
/// `count == 0` is allowed (D027): the first fire emits zero items
/// then immediately self-completes — subscribers see `[Start,
/// Complete]` (no `Dirty` precedes the terminal because no `Data` is
/// emitted on this wave; the canonical-spec one-DIRTY-per-wave rule
/// (R1.3.1.a) governs DATA waves, not pure-terminal waves).
pub fn take(core: &Core, source: NodeId, count: u32) -> FlowRegistration {
    take_with(core, source, count, OperatorOpts::default())
}

/// [`take`] with explicit [`OperatorOpts`].
pub fn take_with(core: &Core, source: NodeId, count: u32, opts: OperatorOpts) -> FlowRegistration {
    let node = core.register_operator(&[source], OperatorOp::Take { count }, opts);
    FlowRegistration { node }
}

/// `skip(source, count)` — drops the first `count` DATA values; once
/// the threshold is crossed, subsequent DATAs pass through verbatim.
/// On a wave where every input is still in the skip window, settles
/// `[Dirty, Resolved]` (D018 pattern).
pub fn skip(core: &Core, source: NodeId, count: u32) -> FlowRegistration {
    skip_with(core, source, count, OperatorOpts::default())
}

/// [`skip`] with explicit [`OperatorOpts`].
pub fn skip_with(core: &Core, source: NodeId, count: u32, opts: OperatorOpts) -> FlowRegistration {
    let node = core.register_operator(&[source], OperatorOp::Skip { count }, opts);
    FlowRegistration { node }
}

// ---------------------------------------------------------------------
// Predicate-based flow: take_while
// ---------------------------------------------------------------------

/// `take_while(source, predicate)` — emits while `predicate(input)`
/// holds; on the first `false`, emits any preceding passes from the
/// same batch then self-completes. Reuses
/// [`BindingBoundary::predicate_each`](graphrefly_core::BindingBoundary::predicate_each)
/// (D029).
pub fn take_while<F>(
    core: &Core,
    binding: &Arc<dyn OperatorBinding>,
    source: NodeId,
    predicate: F,
) -> OperatorRegistration
where
    F: Fn(HandleId) -> bool + Send + Sync + 'static,
{
    take_while_with(core, binding, source, predicate, OperatorOpts::default())
}

/// [`take_while`] with explicit [`OperatorOpts`].
pub fn take_while_with<F>(
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
    let node = core.register_operator(&[source], OperatorOp::TakeWhile { fn_id }, opts);
    OperatorRegistration { node, fn_id }
}

// ---------------------------------------------------------------------
// Terminal-aware flow: last, last_with_default
// ---------------------------------------------------------------------

/// `last(source)` — buffers the latest DATA; emits `Data(latest)` then
/// `Complete` on upstream COMPLETE. On empty stream (no DATA arrived),
/// emits only `Complete` — subscribers see `[Start, Complete]`. For a
/// fallback value on empty streams, use [`last_with_default`].
///
/// Opts out of Lock 2.B auto-cascade so it can intercept upstream
/// COMPLETE (same pattern as [`reduce`](crate::transform::reduce)).
pub fn last(core: &Core, source: NodeId) -> FlowRegistration {
    last_with(core, source, OperatorOpts::default())
}

/// [`last`] with explicit [`OperatorOpts`].
pub fn last_with(core: &Core, source: NodeId, opts: OperatorOpts) -> FlowRegistration {
    let node = core.register_operator(&[source], OperatorOp::Last { default: NO_HANDLE }, opts);
    FlowRegistration { node }
}

/// `last_with_default(source, default)` — buffers the latest DATA;
/// emits `Data(latest)` then `Complete` on upstream COMPLETE. On empty
/// stream (no DATA arrived), emits `Data(default)` then `Complete`.
///
/// Core takes one retain on `default` for the
/// [`LastState`](graphrefly_core::op_state::LastState)'s lifetime; the
/// caller should retain a share for themselves if they want to
/// reference the handle elsewhere.
///
/// # Panics
///
/// Panics if `default == NO_HANDLE` — use [`last`] for the no-default
/// behavior instead.
pub fn last_with_default(core: &Core, source: NodeId, default: HandleId) -> FlowRegistration {
    last_with_default_with(core, source, default, OperatorOpts::default())
}

/// [`last_with_default`] with explicit [`OperatorOpts`].
pub fn last_with_default_with(
    core: &Core,
    source: NodeId,
    default: HandleId,
    opts: OperatorOpts,
) -> FlowRegistration {
    assert!(
        default != NO_HANDLE,
        "last_with_default requires a real default handle; use last() for no-default"
    );
    let node = core.register_operator(&[source], OperatorOp::Last { default }, opts);
    FlowRegistration { node }
}

// ---------------------------------------------------------------------
// Sugar: first, find, element_at (compositions of take / skip / filter)
// ---------------------------------------------------------------------

/// `first(source)` — emits the first DATA then `Complete`. Sugar for
/// `take(source, 1)`.
pub fn first(core: &Core, source: NodeId) -> FlowRegistration {
    take(core, source, 1)
}

/// `find(source, predicate)` — emits the first DATA matching `predicate`
/// then `Complete`. Sugar for `take(filter(source, predicate), 1)`.
pub fn find<F>(
    core: &Core,
    binding: &Arc<dyn OperatorBinding>,
    source: NodeId,
    predicate: F,
) -> FlowRegistration
where
    F: Fn(HandleId) -> bool + Send + Sync + 'static,
{
    let filtered = filter(core, binding, source, predicate);
    take(core, filtered.node, 1)
}

/// `element_at(source, index)` — emits the `index`th DATA (zero-based)
/// then `Complete`. Sugar for `take(skip(source, index), 1)`.
pub fn element_at(core: &Core, source: NodeId, index: u32) -> FlowRegistration {
    let skipped = skip(core, source, index);
    take(core, skipped.node, 1)
}
