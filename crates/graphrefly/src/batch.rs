//! Batch — declarative transactional wave grouping (D12 / L2.E).
//!
//! `batch` runs a closure that may drive multiple nodes; **commit is always
//! implicit** on success, **rollback on panic or explicit rollback**. This eliminates the
//! forgot-to-commit class of bugs, and the three languages align naturally.
//! `bctx.rollback()` is an explicit escape hatch within the closure.
//!
//! ## Interaction with the tier table (D34)
//!
//! Batch-deferred tiers (`>= Value`, see [`crate::protocol::Tier::is_batch_deferred`])
//! are held until the batch commits, then flushed as the committed boundary;
//! immediate tiers (`< Value`) still propagate within the batch. The deferred
//! self-rewire drain (`ctx.rewire_next`, D47) applies AT the committed boundary —
//! never on an un-committed/paused view (C-11/C-22; pause drain timing = B24).
//!
//! Conformance touchpoints: C-3 (INVALIDATE fan-in idempotency exercised via
//! batch), C-19 (undirty timing), and C-22 (batch-before-rewire).

use std::cell::{Cell, RefCell};
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::rc::Rc;

use crate::node::{
    clear_deferred_boundary_tasks, defer_boundary_task, drain_committed_boundary, Core,
};
use crate::protocol::{AnyValue, Wave};

type DeferredEntry = (Core, Wave<AnyValue>);

/// Explicit rollback escape hatch for [`batch`] (D12).
pub struct BatchCtx {
    rolled_back: Rc<Cell<bool>>,
}

impl BatchCtx {
    /// Discard all deferred settle slices instead of committing them.
    pub fn rollback(&self) {
        self.rolled_back.set(true);
    }
}

struct ActiveBatch {
    order: Vec<Core>,
    deferred: Vec<DeferredEntry>,
    committed: Rc<Cell<bool>>,
    rolled_back: Rc<Cell<bool>>,
    collecting: bool,
}

impl ActiveBatch {
    fn new(committed: Rc<Cell<bool>>, rolled_back: Rc<Cell<bool>>) -> Self {
        Self {
            order: Vec::new(),
            deferred: Vec::new(),
            committed,
            rolled_back,
            collecting: true,
        }
    }
}

thread_local! {
    static ACTIVE_BATCH: RefCell<Option<ActiveBatch>> = const { RefCell::new(None) };
}

/// True while a batch frame is open. Boundary drains are blocked for the whole frame,
/// including the commit loop, so queued topology applies only after the committed view.
pub(crate) fn boundary_drains_blocked() -> bool {
    ACTIVE_BATCH.with(|b| b.borrow().is_some())
}

pub(crate) fn collecting_batch() -> bool {
    ACTIVE_BATCH.with(|b| b.borrow().as_ref().is_some_and(|batch| batch.collecting))
}

/// Capture a tier>=3 settle slice into the current collecting batch. Returns false when
/// there is no open collecting batch (including the commit phase).
pub(crate) fn defer_to_batch(target: &Core, wave: Wave<AnyValue>) -> bool {
    ACTIVE_BATCH.with(|b| {
        let mut active = b.borrow_mut();
        let Some(batch) = active.as_mut() else {
            return false;
        };
        if !batch.collecting {
            return false;
        }
        if let Some((_, slot)) = batch
            .deferred
            .iter_mut()
            .find(|(existing, _)| existing.ptr_eq(target))
        {
            *slot = wave;
        } else {
            batch.order.push(target.clone());
            batch.deferred.push((target.clone(), wave));
        }
        true
    })
}

/// Queue a topology mutation until after the active batch commits, but only when this
/// target has an uncommitted settle slice in that batch (R-rewire-batch-boundary / D67).
pub(crate) fn defer_after_batch_for_target(target: &Core, f: Box<dyn FnOnce()>) -> bool {
    ACTIVE_BATCH.with(|b| {
        let active = b.borrow();
        let Some(batch) = active.as_ref() else {
            return false;
        };
        if !batch.collecting
            || !batch
                .deferred
                .iter()
                .any(|(existing, _)| existing.ptr_eq(target))
        {
            return false;
        }
        let committed = batch.committed.clone();
        defer_boundary_task(Box::new(move || {
            if committed.get() {
                f();
            }
        }));
        true
    })
}

struct BatchFinish {
    order: Vec<Core>,
    deferred: Vec<DeferredEntry>,
    committed: Rc<Cell<bool>>,
    explicit_rollback: bool,
}

fn take_for_finish() -> BatchFinish {
    ACTIVE_BATCH.with(|b| {
        let mut active = b.borrow_mut();
        let batch = active.as_mut().expect("batch frame installed");
        batch.collecting = false;
        BatchFinish {
            order: std::mem::take(&mut batch.order),
            deferred: std::mem::take(&mut batch.deferred),
            committed: batch.committed.clone(),
            explicit_rollback: batch.rolled_back.get(),
        }
    })
}

fn clear_active() {
    ACTIVE_BATCH.with(|b| {
        *b.borrow_mut() = None;
    });
}

fn commit(deferred: Vec<DeferredEntry>) {
    for (target, wave) in deferred {
        target.commit_batched_wave(wave);
    }
}

fn rollback(order: Vec<Core>) {
    for target in order {
        target.rollback_batched();
    }
}

fn rollback_and_cleanup(order: Vec<Core>) -> std::thread::Result<()> {
    let result = catch_unwind(AssertUnwindSafe(|| rollback(order)));
    clear_active();
    clear_deferred_boundary_tasks();
    result
}

/// Run `f` as a declarative batch (D12): success commits, panic/rollback discards.
///
/// Nested batches join the outer frame; the outermost frame owns commit/rollback.
pub fn batch<R>(f: impl FnOnce(&BatchCtx) -> R) -> R {
    if let Some(rolled_back) =
        ACTIVE_BATCH.with(|b| b.borrow().as_ref().map(|batch| batch.rolled_back.clone()))
    {
        return f(&BatchCtx { rolled_back });
    }

    let committed = Rc::new(Cell::new(false));
    let rolled_back = Rc::new(Cell::new(false));
    ACTIVE_BATCH.with(|b| {
        *b.borrow_mut() = Some(ActiveBatch::new(committed.clone(), rolled_back.clone()));
    });

    let ctx = BatchCtx {
        rolled_back: rolled_back.clone(),
    };
    let result = catch_unwind(AssertUnwindSafe(|| f(&ctx)));
    let finish = take_for_finish();

    match result {
        Ok(value) => {
            if finish.explicit_rollback {
                if let Err(payload) = rollback_and_cleanup(finish.order) {
                    resume_unwind(payload);
                }
            } else {
                let commit_result = catch_unwind(AssertUnwindSafe(|| commit(finish.deferred)));
                if let Err(payload) = commit_result {
                    clear_active();
                    clear_deferred_boundary_tasks();
                    resume_unwind(payload);
                }
                finish.committed.set(true);
                clear_active();
                drain_committed_boundary();
            }
            value
        }
        Err(payload) => {
            if let Err(rollback_payload) = rollback_and_cleanup(finish.order) {
                resume_unwind(rollback_payload);
            }
            resume_unwind(payload);
        }
    }
}
