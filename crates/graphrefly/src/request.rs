//! Request state nodes: how a projection is asked for a delivery.
//!
//! Control is data on a declared edge (R-messages, R-no-imperative). A projection
//! declares a request state node as a dependency; a request is a
//! write to that state, with its paging or limit params as the value
//! (graphrefly-rs:D1 (1)). The projection serves a request in the run in which the
//! request state delivered a value, and sends nothing in a run that brought none.

use crate::ctx::Ctx;

/// The request the request dependency at `request_dep` delivered in this run, if any.
///
/// Several requests written before one run collapse to the last one.
pub(crate) fn requested<P: Clone + 'static>(ctx: &Ctx, request_dep: usize) -> Option<P> {
    ctx.dep(request_dep)
        .values::<P>()
        .last()
        .map(|params| (**params).clone())
}
