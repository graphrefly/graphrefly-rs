//! Cold synchronous sources — finite-sequence and terminal-constant
//! producer factories.
//!
//! These are the handle-protocol analogues of TS `extra/sources/iter.ts`:
//!
//! - [`from_iter`] — emit each `HandleId` as DATA in order, then COMPLETE.
//! - [`of`] — convenience wrapper: `from_iter` over a `Vec`.
//! - [`empty`] — COMPLETE immediately with no DATA.
//! - [`never`] — silent until teardown (no DATA, no terminal).
//! - [`throw_error`] — emit ERROR immediately.
//!
//! All factories register a **producer node** with no deps. The build
//! closure fires once on first activation (first subscriber) and emits
//! synchronously. Deactivation (last subscriber drops) auto-cleans via
//! the standard `producer_deactivate` path.
//!
//! # Handle protocol
//!
//! Values live on the binding side; Core sees only opaque `HandleId`.
//! `from_iter` / `of` take pre-interned `HandleId`s — the caller
//! (binding layer) must `retain_handle` each handle before passing it
//! in (the source retains once more per emission and releases the
//! caller's share on activation). `throw_error` takes a pre-interned
//! error handle.
//!
//! `empty` and `never` need no handles — they are pure lifecycle
//! sources.

use std::sync::Arc;

use graphrefly_core::{Core, HandleId, NodeId};

use crate::producer::{ProducerBinding, ProducerBuildFn, ProducerCtx};

/// Emit each handle as DATA in order, then COMPLETE. If the iterator
/// is empty, behaves like [`empty`] (COMPLETE only).
///
/// The caller must pre-intern each handle on the binding side and pass
/// ownership to this factory. Handles are retained once per emission
/// and the caller's original shares are released during the build
/// closure (so the caller should NOT release them after calling this).
#[must_use]
pub fn from_iter(
    core: &Core,
    binding: &Arc<dyn ProducerBinding>,
    handles: Vec<HandleId>,
) -> NodeId {
    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        // S2b/D231: build-side only (no spawned sinks) — use the
        // ctx-borrowed `&Core` directly; no `WeakCore`/`Clone`.
        let core_s = ctx.core();
        let binding_s = ctx.core().binding();
        let pid = ctx.node_id();

        for &h in &handles {
            // Retain for the emission — Core takes ownership of this share.
            binding_s.retain_handle(h);
            core_s.emit_or_defer(pid, h);
        }
        // Release the caller's original shares — ownership was
        // transferred to Core via the retain+emit above.
        for &h in &handles {
            binding_s.release_handle(h);
        }
        core_s.complete_or_defer(pid);
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
        .expect("invariant: register_producer has no deps; no error variants reachable")
}

/// Emit each handle as DATA in order, then COMPLETE. Convenience
/// wrapper over [`from_iter`].
#[must_use]
pub fn of(core: &Core, binding: &Arc<dyn ProducerBinding>, handles: Vec<HandleId>) -> NodeId {
    from_iter(core, binding, handles)
}

/// Complete immediately with no DATA (cold EMPTY analogue).
#[must_use]
pub fn empty(core: &Core, binding: &Arc<dyn ProducerBinding>) -> NodeId {
    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        ctx.core().complete_or_defer(ctx.node_id());
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
        .expect("invariant: register_producer has no deps; no error variants reachable")
}

/// Never emit and never complete until teardown (cold NEVER analogue).
/// The build closure is a no-op — the producer stays active but silent
/// until the last subscriber drops.
#[must_use]
pub fn never(core: &Core, binding: &Arc<dyn ProducerBinding>) -> NodeId {
    let build: ProducerBuildFn = Box::new(|_ctx: ProducerCtx<'_>| {
        // Intentionally empty — no emissions, no terminal.
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
        .expect("invariant: register_producer has no deps; no error variants reachable")
}

/// Emit ERROR immediately with the given error handle (cold error source).
///
/// The caller must pre-intern the error handle on the binding side.
/// The handle is retained once for the emission; the caller's original
/// share is consumed (do NOT release after calling this).
#[must_use]
pub fn throw_error(
    core: &Core,
    binding: &Arc<dyn ProducerBinding>,
    error_handle: HandleId,
) -> NodeId {
    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let core_s = ctx.core();
        let binding_s = ctx.core().binding();
        // Retain for the emission — Core takes ownership.
        binding_s.retain_handle(error_handle);
        core_s.error_or_defer(ctx.node_id(), error_handle);
        // Release the caller's original share.
        binding_s.release_handle(error_handle);
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
        .expect("invariant: register_producer has no deps; no error variants reachable")
}
