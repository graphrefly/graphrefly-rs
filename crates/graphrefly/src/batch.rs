//! Batch — declarative transactional wave grouping (D12 / L2.E).
//!
//! `batch` runs a closure that may drive multiple nodes; **commit is always
//! implicit** on success, **rollback on `Err`/panic**. This eliminates the
//! forgot-to-commit class of bugs, and the three languages align naturally.
//! `bctx.rollback()` is an explicit escape hatch within the closure.
//!
//! ## Interaction with the tier table (D34)
//!
//! Batch-deferred tiers (`>= Value`, see [`crate::protocol::Tier::is_batch_deferred`])
//! are held until the batch commits, then flushed as the committed boundary;
//! immediate tiers (`< Value`) still propagate within the batch. The deferred
//! self-rewire drain (`ctx.rewire_next`, D47) applies AT the committed boundary —
//! never on an un-committed/paused view (C-11; batch/pause drain timing = B24).
//!
//! Conformance touchpoints: C-3 (INVALIDATE fan-in idempotency exercised via
//! batch), and batch×rewire edges (backlog B16–B19/B24).

// TODO(CSP-5): batch(closure) + BatchCtx { rollback }. The commit/rollback
// boundary and the batch-deferred flush ordering are CSP-5 work — drive via
// /dev-dispatch + /conformance. Not pre-committed.
