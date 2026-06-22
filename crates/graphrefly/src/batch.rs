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

use crate::host_boundary::is_host_boundary_abort_payload;
use crate::node::{
    boundary_root_for, clear_all_deferred_boundary_root, clear_deferred_boundary_root,
    drain_committed_boundary_root, BatchTarget, BoundaryRoot, Core,
};
use crate::protocol::{AnyValue, Wave};

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
    targets: Vec<BatchTarget>,
    order: Vec<usize>,
    deferred: Vec<Option<Wave<AnyValue>>>,
    boundary_roots: Vec<BoundaryRoot>,
    committed: Rc<Cell<bool>>,
    rolled_back: Rc<Cell<bool>>,
    collecting: bool,
}

impl ActiveBatch {
    fn new(committed: Rc<Cell<bool>>, rolled_back: Rc<Cell<bool>>) -> Self {
        Self {
            targets: Vec::new(),
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

/// Return the active batch's commit token for boundary tasks caused while the
/// batch frame is open. The token flips only after commit succeeds; rollback or
/// commit failure leaves it false so cleanup can drop only uncommitted tasks.
pub(crate) fn active_batch_committed_token() -> Option<Rc<Cell<bool>>> {
    ACTIVE_BATCH.with(|b| b.borrow().as_ref().map(|batch| batch.committed.clone()))
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
        let index = match batch.targets.iter().position(|entry| entry.matches(target)) {
            Some(index) => index,
            None => {
                let Some(target) = BatchTarget::from_core(target) else {
                    return false;
                };
                let index = batch.targets.len();
                batch.targets.push(target);
                batch.deferred.push(None);
                batch.order.push(index);
                index
            }
        };
        batch.deferred[index] = Some(wave);
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
            || !batch.targets.iter().enumerate().any(|(index, existing)| {
                existing.matches(target) && batch.deferred.get(index).is_some_and(Option::is_some)
            })
        {
            return None;
        }
        Some(batch.committed.clone())
    })
}

struct BatchFinish {
    targets: Vec<BatchTarget>,
    order: Vec<usize>,
    deferred: Vec<Option<Wave<AnyValue>>>,
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
            targets: std::mem::take(&mut batch.targets),
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

fn commit(targets: &[BatchTarget], deferred: Vec<Option<Wave<AnyValue>>>) {
    for (target_index, wave) in deferred.into_iter().enumerate() {
        if let Some(wave) = wave {
            targets[target_index].commit_batched_wave(wave);
        }
    }
}

fn rollback(targets: &[BatchTarget], order: Vec<usize>) {
    for target_index in order {
        targets[target_index].rollback_batched();
    }
}

fn boundary_graph_roots(
    targets: &[BatchTarget],
    order: &[usize],
    deferred: &[Option<Wave<AnyValue>>],
) -> Vec<BoundaryRoot> {
    let mut roots: Vec<BoundaryRoot> = Vec::new();
    let mut push_unique = |target: &BatchTarget| {
        let root = target.boundary_root();
        if roots.iter().any(|r| r.same_root(&root)) {
            return;
        }
        roots.push(root);
    };
    for target_index in order {
        if let Some(target) = targets.get(*target_index) {
            push_unique(target);
        }
    }
    for (target_index, wave) in deferred.iter().enumerate() {
        if wave.is_some() {
            let Some(target) = targets.get(target_index) else {
                continue;
            };
            push_unique(target);
        }
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
    core_roots: &[BoundaryRoot],
    boundary_roots: Vec<BoundaryRoot>,
) {
    for root in boundary_roots {
        if core_roots.iter().any(|r| r.same_root(&root))
            || task_roots.iter().any(|r| r.same_root(&root))
        {
            continue;
        }
        task_roots.push(root);
    }
}

fn clear_boundary_for_roots(core_roots: &[BoundaryRoot], task_roots: &[BoundaryRoot]) {
    for root in core_roots {
        clear_deferred_boundary_root(root);
    }
    for root in task_roots {
        clear_deferred_boundary_root(root);
    }
}

fn drain_boundary_for_roots(core_roots: &[BoundaryRoot], task_roots: &[BoundaryRoot]) {
    let mut escaped: Option<Box<dyn std::any::Any + Send>> = None;
    for root in core_roots {
        let result = catch_unwind(AssertUnwindSafe(|| drain_committed_boundary_root(root)));
        if let Err(payload) = result {
            if is_host_boundary_abort_payload(payload.as_ref()) {
                clear_all_boundary_for_roots(core_roots, task_roots);
                resume_unwind(payload);
            }
            remember_first_boundary_panic(&mut escaped, Err(payload));
        }
    }
    for root in task_roots {
        let result = catch_unwind(AssertUnwindSafe(|| drain_committed_boundary_root(root)));
        if let Err(payload) = result {
            if is_host_boundary_abort_payload(payload.as_ref()) {
                clear_all_boundary_for_roots(core_roots, task_roots);
                resume_unwind(payload);
            }
            remember_first_boundary_panic(&mut escaped, Err(payload));
        }
    }
    if let Some(e) = escaped {
        std::panic::resume_unwind(e);
    }
}

fn clear_all_boundary_for_roots(core_roots: &[BoundaryRoot], task_roots: &[BoundaryRoot]) {
    for root in core_roots {
        clear_all_deferred_boundary_root(root);
    }
    for root in task_roots {
        clear_all_deferred_boundary_root(root);
    }
}

fn remember_first_boundary_panic(
    escaped: &mut Option<Box<dyn std::any::Any + Send>>,
    result: std::thread::Result<()>,
) {
    if let Err(e) = result {
        if escaped.is_none() {
            *escaped = Some(e);
        }
    }
}

fn rollback_and_cleanup(
    targets: Vec<BatchTarget>,
    order: Vec<usize>,
    mut task_roots: Vec<BoundaryRoot>,
) -> std::thread::Result<()> {
    let core_roots = boundary_graph_roots(&targets, &order, &[]);
    let result = catch_unwind(AssertUnwindSafe(|| rollback(&targets, order)));
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
    let boundary_core_roots =
        boundary_graph_roots(&finish.targets, &finish.order, &finish.deferred);
    let mut boundary_task_roots = Vec::new();
    append_boundary_roots(
        &mut boundary_task_roots,
        &boundary_core_roots,
        finish.boundary_roots,
    );

    match result {
        Ok(value) => {
            if finish.explicit_rollback {
                if let Err(payload) =
                    rollback_and_cleanup(finish.targets, finish.order, boundary_task_roots)
                {
                    resume_unwind(payload);
                }
            } else {
                let commit_result = catch_unwind(AssertUnwindSafe(|| {
                    commit(&finish.targets, finish.deferred)
                }));
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
            if let Err(rollback_payload) =
                rollback_and_cleanup(finish.targets, finish.order, boundary_task_roots)
            {
                resume_unwind(rollback_payload);
            }
            resume_unwind(payload);
        }
    }
}
