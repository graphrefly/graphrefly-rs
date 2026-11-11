//! Batch — declarative transactional wave grouping (D12 / L2.E).
//!
//! `batch` runs a closure that may drive multiple nodes; **commit is always
//! implicit** on success, **rollback on panic or explicit rollback**. This eliminates the
//! forgot-to-commit class of bugs, and the three languages align naturally.
//! `bctx.rollback()` is an explicit escape hatch within the closure.
//!
//! ## Interaction with message classes
//!
//! `batch(body)` runs `body` at once, also during a wave, and delivers nothing while
//! it runs: host inputs are staged (a write function computes at the commit from the
//! writes staged before it), and topology requests (subscribe, unsubscribe, replace,
//! ctx.rewire) wait. When `body` returns, its inputs commit as one wave, and its
//! requests then run in order (R-request-order@2). The commit is itself a request:
//! during a wave it runs after the wave. A body that panics commits nothing and drops
//! its requests; output a ctx staged through a lifetime that closed before the commit
//! is dropped at the commit.
//!
//! Conformance touchpoints: C-3 (INVALIDATE fan-in idempotency exercised via
//! batch) and C-22 (batch-before-rewire).

use std::cell::{Cell, RefCell};
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::rc::Rc;

use crate::node::{
    boundary_root_for, clear_deferred_boundary_root, commit_batched_waves,
    drain_committed_boundary_root, BatchTarget, BoundaryRoot, Core,
};
use crate::protocol::{AnyValue, Message, Wave};

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

/// A state write function staged in a batch: it computes from the value the writes
/// staged before it leave, at the commit (R-request-order@2).
pub(crate) type StagedWrite = Box<dyn FnOnce(Option<AnyValue>) -> Option<AnyValue>>;

/// One staged item of a target, in staging order.
enum Staged {
    /// Messages, with the lifetime of the ctx that staged them, if any: output staged
    /// through a lifetime that closed before the commit is dropped at the commit.
    Msgs {
        msgs: Wave<AnyValue>,
        lifetime: Option<u64>,
    },
    /// A state write function.
    Write(StagedWrite),
}

type StagedItems = Vec<Staged>;

fn ends_with_terminal(items: &[Staged]) -> bool {
    items
        .iter()
        .rev()
        .find_map(|item| match item {
            Staged::Msgs { msgs, .. } if !msgs.is_empty() => Some(msgs),
            _ => None,
        })
        .is_some_and(|msgs| msgs.last().is_some_and(Message::is_terminal))
}

struct ActiveBatch {
    targets: Vec<BatchTarget>,
    order: Vec<usize>,
    deferred: Vec<Option<StagedItems>>,
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

/// Stage a node's output into the current collecting batch, appended to the one array
/// the node commits (R-request-order). Returns false when there is no open collecting
/// batch (including the commit phase).
///
/// # Panics
///
/// Panics, leaving the earlier staging as it was, when the submission would follow a
/// terminal already staged for the node: a terminal must be the last message of its
/// array (R-wave).
pub(crate) fn defer_to_batch(target: &Core, wave: Wave<AnyValue>) -> bool {
    stage(target, |staged| {
        assert!(
            wave.is_empty() || !ends_with_terminal(staged),
            "batch: output staged after a terminal (R-wave)"
        );
        staged.push(Staged::Msgs {
            msgs: wave,
            lifetime: None,
        });
    })
}

/// [`defer_to_batch`] for output a ctx of `lifetime` staged.
pub(crate) fn defer_to_batch_from(target: &Core, wave: Wave<AnyValue>, lifetime: u64) -> bool {
    stage(target, |staged| {
        assert!(
            wave.is_empty() || !ends_with_terminal(staged),
            "batch: output staged after a terminal (R-wave)"
        );
        staged.push(Staged::Msgs {
            msgs: wave,
            lifetime: Some(lifetime),
        });
    })
}

/// Stages a state write function for `target` in the collecting batch.
pub(crate) fn stage_write(target: &Core, write: StagedWrite) -> bool {
    stage(target, |staged| staged.push(Staged::Write(write)))
}

fn stage(target: &Core, push: impl FnOnce(&mut StagedItems)) -> bool {
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
        push(batch.deferred[index].get_or_insert_with(Vec::new));
        true
    })
}

/// Panics, before anything is staged, when `wave` would follow a terminal already
/// staged for `target` in the collecting batch (R-wave: a terminal is last). A host
/// input checks this before it enters a wave, so the rejection reaches its caller.
pub(crate) fn assert_stageable(target: &Core, wave: &[Message<AnyValue>]) {
    if wave.is_empty() {
        return;
    }
    ACTIVE_BATCH.with(|b| {
        let active = b.borrow();
        let Some(batch) = active.as_ref().filter(|batch| batch.collecting) else {
            return;
        };
        let staged_terminal = batch.targets.iter().enumerate().any(|(index, entry)| {
            entry.matches(target)
                && batch
                    .deferred
                    .get(index)
                    .and_then(Option::as_ref)
                    .is_some_and(|staged| ends_with_terminal(staged))
        });
        drop(active);
        assert!(
            !staged_terminal,
            "batch: output staged after a terminal (R-wave)"
        );
    });
}

struct BatchFinish {
    targets: Vec<BatchTarget>,
    order: Vec<usize>,
    deferred: Vec<Option<StagedItems>>,
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

/// Resolves each target's staged items into the one array it commits, before
/// anything is delivered: output from a closed lifetime is dropped, and each write
/// function computes from the value the items before it leave (an INVALIDATE leaves
/// none). A panicking write function fails the whole commit.
fn resolve(
    targets: &[BatchTarget],
    deferred: Vec<Option<StagedItems>>,
) -> std::thread::Result<Vec<(usize, Wave<AnyValue>)>> {
    let mut resolved = Vec::new();
    for (index, items) in deferred.into_iter().enumerate() {
        let Some(items) = items else { continue };
        let Some(core) = targets[index].borrowed_core() else {
            continue;
        };
        let mut current = core.cache_any();
        let mut wave: Wave<AnyValue> = Vec::new();
        for item in items {
            match item {
                Staged::Msgs { msgs, lifetime } => {
                    if lifetime.is_some_and(|id| !core.lifetime_open(id)) {
                        continue;
                    }
                    for m in &msgs {
                        match m {
                            Message::Data(v) => current = Some(v.clone()),
                            Message::Invalidate => current = None,
                            _ => {}
                        }
                    }
                    wave.extend(msgs);
                }
                Staged::Write(write) => {
                    let input = current.clone();
                    if let Some(v) = catch_unwind(AssertUnwindSafe(move || write(input)))? {
                        current = Some(v.clone());
                        wave.push(Message::Data(v));
                    }
                }
            }
        }
        resolved.push((index, wave));
    }
    Ok(resolved)
}

fn commit(targets: &[BatchTarget], resolved: Vec<(usize, Wave<AnyValue>)>) {
    commit_batched_waves(
        resolved
            .into_iter()
            .map(|(index, wave)| (&targets[index], wave))
            .collect(),
    );
}

fn boundary_graph_roots(
    targets: &[BatchTarget],
    order: &[usize],
    deferred: &[Option<StagedItems>],
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
            remember_first_boundary_panic(&mut escaped, Err(payload));
        }
    }
    for root in task_roots {
        let result = catch_unwind(AssertUnwindSafe(|| drain_committed_boundary_root(root)));
        if let Err(payload) = result {
            remember_first_boundary_panic(&mut escaped, Err(payload));
        }
    }
    if let Some(e) = escaped {
        std::panic::resume_unwind(e);
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
    // Nothing was delivered before the commit, so a rollback only drops what was
    // staged (R-request-order): there is nothing to undo or balance.
    let core_roots = boundary_graph_roots(&targets, &order, &[]);
    let late_roots = take_late_boundary_roots();
    append_boundary_roots(&mut task_roots, &core_roots, late_roots);
    clear_active();
    clear_boundary_for_roots(&core_roots, &task_roots);
    Ok(())
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
    let request_mark = crate::node::request_mark();
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
                crate::node::drop_requests_after(request_mark);
                if let Err(payload) =
                    rollback_and_cleanup(finish.targets, finish.order, boundary_task_roots)
                {
                    resume_unwind(payload);
                }
            } else if crate::node::in_wave() {
                // The commit is a request: during a wave it runs after the wave, and
                // the body's topology requests run after it (R-request-order@2).
                clear_active();
                let BatchFinish {
                    targets,
                    deferred,
                    committed,
                    ..
                } = finish;
                // The body's own requests run right after its commit, before any
                // request the commit wave makes.
                let body_requests = crate::node::take_requests_after(request_mark);
                // A commit spanning domains reports to its first target's.
                let reporter = targets
                    .first()
                    .and_then(BatchTarget::borrowed_core)
                    .and_then(|core| core.reporter());
                crate::node::request_reporting(reporter, move || {
                    committed.set(true);
                    let failed = match resolve(&targets, deferred) {
                        Ok(resolved) => {
                            commit(&targets, resolved);
                            None
                        }
                        Err(payload) => Some(payload),
                    };
                    // The body's topology requests run even when a write failed.
                    drain_boundary_for_roots(&boundary_core_roots, &boundary_task_roots);
                    for body_request in body_requests {
                        crate::node::run_queued_request(body_request);
                    }
                    if let Some(payload) = failed {
                        resume_unwind(payload);
                    }
                });
            } else {
                let resolved = match resolve(&finish.targets, finish.deferred) {
                    Ok(resolved) => resolved,
                    Err(payload) => {
                        // A failed write function commits nothing; the body's topology
                        // requests still run, then the failure reaches the caller.
                        finish.committed.set(true);
                        clear_active();
                        drain_boundary_for_roots(&boundary_core_roots, &boundary_task_roots);
                        crate::node::drain_requests();
                        resume_unwind(payload);
                    }
                };
                let commit_result =
                    catch_unwind(AssertUnwindSafe(|| commit(&finish.targets, resolved)));
                let late_roots = take_late_boundary_roots();
                append_boundary_roots(&mut boundary_task_roots, &boundary_core_roots, late_roots);
                if let Err(payload) = commit_result {
                    clear_active();
                    clear_boundary_for_roots(&boundary_core_roots, &boundary_task_roots);
                    crate::node::drain_requests();
                    resume_unwind(payload);
                }
                finish.committed.set(true);
                clear_active();
                drain_boundary_for_roots(&boundary_core_roots, &boundary_task_roots);
                // Requests made during the commit's waves run after it.
                crate::node::drain_requests();
            }
            value
        }
        Err(payload) => {
            crate::node::drop_requests_after(request_mark);
            if let Err(rollback_payload) =
                rollback_and_cleanup(finish.targets, finish.order, boundary_task_roots)
            {
                resume_unwind(rollback_payload);
            }
            resume_unwind(payload);
        }
    }
}
