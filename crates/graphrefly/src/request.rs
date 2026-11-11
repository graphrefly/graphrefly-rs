//! Request state nodes: how a projection is asked for a delivery.
//!
//! The D797 kernel has no upward channel and no PULL message (R-messages): control
//! is data on a declared edge (R-no-imperative). A projection that used to answer a
//! routed `PULL` now declares a request state node as a dependency; a request is a
//! write to that state, with its paging or limit params as the value
//! (graphrefly-rs:D1 (1)). The projection serves a request in the run in which the
//! request state delivered a value, and sends nothing in a run that brought none.

use crate::ctx::Ctx;

/// The request the request dependency at `request_dep` delivered in this run, if any.
///
/// Several requests written before one run collapse to the last one, as several
/// demands in one wave did.
pub(crate) fn requested<P: Clone + 'static>(ctx: &Ctx, request_dep: usize) -> Option<P> {
    ctx.batch::<P>(request_dep)
        .last()
        .map(|params| (**params).clone())
}
