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
    boundary_root_for, clear_deferred_boundary_root, clear_deferred_boundary_tasks,
    drain_committed_boundaries, BoundaryRoot, Core,
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
    boundary_roots: Vec<BoundaryRoot>,
    committed: Rc<Cell<bool>>,
    rolled_back: Rc<Cell<bool>>,
    collecting: bool,
}

impl ActiveBatch {
    fn new(committed: Rc<Cell<bool>>, rolled_back: Rc<Cell<bool>>) -> Self {
        Self {
            order: Vec::new(),
            deferred: Vec::new(),
            boundary_roots: Vec::new(),
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

/// Remember that a graph-local committed-boundary task was queued during this batch.
/// Some tasks do not also create a tier>=3 batched wave, so they need their graph root
/// included in the post-commit drain/rollback cleanup set (R-rewire-batch-boundary).
pub(crate) fn register_boundary_root(target: &Core) {
    ACTIVE_BATCH.with(|b| {
        let mut active = b.borrow_mut();
        let Some(batch) = active.as_mut() else {
            return;
        };
        if batch.boundary_roots.iter().any(|r| r.same_graph(target)) {
            return;
        }
        batch.boundary_roots.push(boundary_root_for(target));
    });
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

/// Return the active batch's commit token, but only when this target has an
/// uncommitted settle slice in that batch (R-rewire-batch-boundary / D67).
pub(crate) fn committed_after_batch_for_target(target: &Core) -> Option<Rc<Cell<bool>>> {
    ACTIVE_BATCH.with(|b| {
        let active = b.borrow();
        let batch = active.as_ref()?;
        if !batch.collecting
            || !batch
                .deferred
                .iter()
                .any(|(existing, _)| existing.ptr_eq(target))
        {
            return None;
        }
        Some(batch.committed.clone())
    })
}

struct BatchFinish {
    order: Vec<Core>,
    deferred: Vec<DeferredEntry>,
    boundary_roots: Vec<BoundaryRoot>,
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
            boundary_roots: std::mem::take(&mut batch.boundary_roots),
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

fn boundary_graph_roots(order: &[Core], deferred: &[DeferredEntry]) -> Vec<Core> {
    let mut roots: Vec<Core> = Vec::new();
    let mut push_unique = |node: &Core| {
        if roots.iter().any(|r| r.same_graph(node)) {
            return;
        }
        roots.push(node.clone());
    };
    for node in order {
        push_unique(node);
    }
    for (node, _) in deferred {
        push_unique(node);
    }
    roots
}

fn take_late_boundary_roots() -> Vec<BoundaryRoot> {
    ACTIVE_BATCH.with(|b| {
        let mut active = b.borrow_mut();
        active
            .as_mut()
            .map(|batch| std::mem::take(&mut batch.boundary_roots))
            .unwrap_or_default()
    })
}

fn append_boundary_roots(
    task_roots: &mut Vec<BoundaryRoot>,
    core_roots: &[Core],
    boundary_roots: Vec<BoundaryRoot>,
) {
    for root in boundary_roots {
        if core_roots.iter().any(|r| root.same_graph(r))
            || task_roots.iter().any(|r| r.same_root(&root))
        {
            continue;
        }
        task_roots.push(root);
    }
}

fn clear_boundary_for_roots(core_roots: &[Core], task_roots: &[BoundaryRoot]) {
    for root in core_roots {
        clear_deferred_boundary_tasks(root);
    }
    for root in task_roots {
        clear_deferred_boundary_root(root);
    }
}

fn drain_boundary_for_roots(core_roots: &[Core], task_roots: &[BoundaryRoot]) {
    drain_committed_boundaries(core_roots, task_roots);
}

fn rollback_and_cleanup(
    order: Vec<Core>,
    mut task_roots: Vec<BoundaryRoot>,
) -> std::thread::Result<()> {
    let core_roots = boundary_graph_roots(&order, &[]);
    let result = catch_unwind(AssertUnwindSafe(|| rollback(order)));
    let late_roots = take_late_boundary_roots();
    append_boundary_roots(&mut task_roots, &core_roots, late_roots);
    clear_active();
    clear_boundary_for_roots(&core_roots, &task_roots);
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
    let boundary_core_roots = boundary_graph_roots(&finish.order, &finish.deferred);
    let mut boundary_task_roots = Vec::new();
    append_boundary_roots(
        &mut boundary_task_roots,
        &boundary_core_roots,
        finish.boundary_roots,
    );

    match result {
        Ok(value) => {
            if finish.explicit_rollback {
                if let Err(payload) = rollback_and_cleanup(finish.order, boundary_task_roots) {
                    resume_unwind(payload);
                }
            } else {
                let commit_result = catch_unwind(AssertUnwindSafe(|| commit(finish.deferred)));
                let late_roots = take_late_boundary_roots();
                append_boundary_roots(&mut boundary_task_roots, &boundary_core_roots, late_roots);
                if let Err(payload) = commit_result {
                    clear_active();
                    clear_boundary_for_roots(&boundary_core_roots, &boundary_task_roots);
                    resume_unwind(payload);
                }
                finish.committed.set(true);
                clear_active();
                drain_boundary_for_roots(&boundary_core_roots, &boundary_task_roots);
            }
            value
        }
        Err(payload) => {
            if let Err(rollback_payload) = rollback_and_cleanup(finish.order, boundary_task_roots) {
                resume_unwind(rollback_payload);
            }
            resume_unwind(payload);
        }
    }
}
