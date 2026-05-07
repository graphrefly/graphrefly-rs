//! Multi-dep combinator operators (Slice C-2, D020).
//!
//! Mirrors the TS shapes in
//! `~/src/graphrefly-ts/packages/legacy-pure-ts/src/extra/operators/combine.ts`,
//! but driven by Core dispatch ([`graphrefly_core::OperatorOp`]) instead
//! of derived-fn factories.
//!
//! - [`combine`] — N-dep combineLatest (any dep fire → emit packed tuple)
//! - [`with_latest_from`] — 2-dep, fire-on-primary-only (Phase 10.5)
//! - [`merge`] — N-dep, forward all DATA handles verbatim (zero FFI)

use std::sync::Arc;

use graphrefly_core::{Core, FnId, HandleId, NodeId, OperatorOp, OperatorOpts};

use crate::binding::OperatorBinding;
use crate::transform::OperatorRegistration;

/// Boxed packer closure type — converts N HandleIds to a single tuple HandleId.
pub type PackerFn = Box<dyn Fn(&[HandleId]) -> HandleId + Send + Sync>;

/// `combine(...sources)` — combineLatest semantics.
///
/// Emits a packed tuple of the latest handle per dep whenever any dep
/// fires. First-run gate (`partial: false` default) holds until all deps
/// deliver real DATA (R2.5.3).
///
/// # Arguments
///
/// - `core` — the Core dispatcher.
/// - `binding` — implements [`OperatorBinding`] for closure registration.
/// - `sources` — N dep node ids (order determines tuple position).
/// - `packer` — closure that takes N `HandleId`s and returns a single
///   tuple `HandleId`. The binding wraps `Fn(&[T]) -> Tuple` into this.
///
/// # Panics
///
/// Panics if `sources` is empty.
pub fn combine(
    core: &Core,
    binding: &Arc<dyn OperatorBinding>,
    sources: &[NodeId],
    packer: PackerFn,
) -> OperatorRegistration {
    assert!(!sources.is_empty(), "combine requires at least one source");
    let pack_fn = binding.register_packer(packer);
    let opts = OperatorOpts::default(); // partial: false — gate all deps
    let node = core.register_operator(sources, OperatorOp::Combine { pack_fn }, opts);
    OperatorRegistration {
        node,
        fn_id: pack_fn,
    }
}

/// `with_latest_from(primary, secondary)` — fire-on-primary-only.
///
/// Emits a packed pair `[primary, secondary]` only when `primary` (dep[0])
/// has DATA in the wave. If only `secondary` (dep[1]) fires, settles with
/// RESOLVED. Matches Phase 10.5 semantics (D021).
///
/// First-run gate (`partial: false`) holds until both deps deliver real
/// DATA (R2.5.3). Post-warmup, fires on primary alone; if secondary has
/// been invalidated (prev_data == NO_HANDLE), settles with RESOLVED
/// instead of emitting a stale pair.
///
/// # Arguments
///
/// - `core` — the Core dispatcher.
/// - `binding` — implements [`OperatorBinding`] for closure registration.
/// - `primary` — the triggering dep (dep[0]).
/// - `secondary` — the sampled dep (dep[1]).
/// - `packer` — closure that packs 2 `HandleId`s into a pair handle.
pub fn with_latest_from(
    core: &Core,
    binding: &Arc<dyn OperatorBinding>,
    primary: NodeId,
    secondary: NodeId,
    packer: PackerFn,
) -> OperatorRegistration {
    let pack_fn = binding.register_packer(packer);
    let opts = OperatorOpts::default(); // partial: false — gate both deps
    let node = core.register_operator(
        &[primary, secondary],
        OperatorOp::WithLatestFrom { pack_fn },
        opts,
    );
    OperatorRegistration {
        node,
        fn_id: pack_fn,
    }
}

/// Registration output for [`merge`] — no closure involved (zero FFI).
#[derive(Copy, Clone, Debug)]
#[must_use = "the merge operator's NodeId is the value of registering it"]
pub struct MergeRegistration {
    pub node: NodeId,
}

impl MergeRegistration {
    /// Convenience: extract just the node id.
    #[must_use]
    pub fn into_node(self) -> NodeId {
        self.node
    }
}

/// `merge(...sources)` — forward all DATA handles verbatim (zero FFI).
///
/// Each dep's DATA is forwarded individually without transformation.
/// COMPLETE cascades when all deps complete (R1.3.4.b). No binding
/// call on the fire path — maximally efficient.
///
/// # Arguments
///
/// - `core` — the Core dispatcher.
/// - `sources` — N dep node ids.
///
/// # Panics
///
/// Panics if `sources` is empty.
pub fn merge(core: &Core, sources: &[NodeId]) -> MergeRegistration {
    assert!(!sources.is_empty(), "merge requires at least one source");
    let opts = OperatorOpts {
        partial: true, // merge fires as soon as ANY dep delivers
        ..OperatorOpts::default()
    };
    let node = core.register_operator(sources, OperatorOp::Merge, opts);
    MergeRegistration { node }
}

/// Convenience: `merge` with an explicit [`FnId`] return for API
/// consistency. Since merge is zero-FFI, the `FnId` is a sentinel.
/// Prefer [`merge`] unless you need the [`OperatorRegistration`] shape.
pub fn merge_as_op(core: &Core, sources: &[NodeId]) -> OperatorRegistration {
    let reg = merge(core, sources);
    OperatorRegistration {
        node: reg.node,
        fn_id: FnId::new(0), // sentinel — no closure registered
    }
}
