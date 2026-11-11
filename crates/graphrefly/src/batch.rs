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

use crate::node::{commit_batched_waves, BatchTarget, Core, Domain, DomainRef};
use crate::protocol::{AnyValue, Message, Wave};

/// The rollback escape hatch of [`crate::Graph::batch`] (D12).
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

pub(crate) struct ActiveBatch {
    targets: Vec<BatchTarget>,
    deferred: Vec<Option<StagedItems>>,
    rolled_back: Rc<Cell<bool>>,
    collecting: bool,
}

impl ActiveBatch {
    fn new(rolled_back: Rc<Cell<bool>>) -> Self {
        Self {
            targets: Vec::new(),
            deferred: Vec::new(),
            rolled_back,
            collecting: true,
        }
    }
}

/// A graph's open batch frame: each graph (its domain) has its own
/// (R-graph-domain@3).
pub(crate) type BatchCell = RefCell<Option<ActiveBatch>>;

fn with_batch<R>(d: &Domain, f: impl FnOnce(&BatchCell) -> R) -> R {
    f(&d.batch)
}

/// True while a batch frame is open, its commit included: requests wait until the
/// commit is done (R-request-order@2).
pub(crate) fn batch_frame_open(d: &Domain) -> bool {
    with_batch(d, |b| b.borrow().is_some())
}

pub(crate) fn collecting_batch(d: &Domain) -> bool {
    with_batch(d, |b| {
        b.borrow().as_ref().is_some_and(|batch| batch.collecting)
    })
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
    with_batch(&target.domain(), |b| {
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
    with_batch(&target.domain(), |b| {
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
    deferred: Vec<Option<StagedItems>>,
    explicit_rollback: bool,
}

fn take_for_finish(d: &Domain) -> BatchFinish {
    with_batch(d, |b| {
        let mut active = b.borrow_mut();
        let batch = active.as_mut().expect("batch frame installed");
        batch.collecting = false;
        BatchFinish {
            targets: std::mem::take(&mut batch.targets),
            deferred: std::mem::take(&mut batch.deferred),
            explicit_rollback: batch.rolled_back.get(),
        }
    })
}

fn clear_active(d: &Domain) {
    with_batch(d, |b| {
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

/// A batch of domain `d` (R-request-order@2, R-graph-domain@3).
pub(crate) fn batch_in<R>(d: &DomainRef, f: impl FnOnce(&BatchCtx) -> R) -> R {
    if let Some(rolled_back) = with_batch(d, |b| {
        b.borrow().as_ref().map(|batch| batch.rolled_back.clone())
    }) {
        return f(&BatchCtx { rolled_back });
    }

    let rolled_back = Rc::new(Cell::new(false));
    with_batch(d, |b| {
        *b.borrow_mut() = Some(ActiveBatch::new(rolled_back.clone()));
    });

    let ctx = BatchCtx {
        rolled_back: rolled_back.clone(),
    };
    let request_mark = crate::node::request_mark(d);
    let result = catch_unwind(AssertUnwindSafe(|| f(&ctx)));
    let finish = take_for_finish(d);

    match result {
        Ok(value) => {
            if finish.explicit_rollback {
                // Nothing was delivered before the commit, so a rollback only drops
                // what was staged and the body's requests (R-request-order@2).
                crate::node::drop_requests_after(d, request_mark);
                clear_active(d);
            } else if crate::node::in_wave(d) {
                // The commit is a request: during a wave it runs after the wave, and
                // the body's topology requests run after it (R-request-order@2).
                clear_active(d);
                let BatchFinish {
                    targets, deferred, ..
                } = finish;
                // The body's own requests run right after its commit, before any
                // request the commit wave makes.
                let body_requests = crate::node::take_requests_after(d, request_mark);
                let reporter = targets
                    .first()
                    .and_then(BatchTarget::borrowed_core)
                    .and_then(|core| core.reporter());
                crate::node::request_reporting(d, reporter, move || {
                    let failed = match resolve(&targets, deferred) {
                        Ok(resolved) => {
                            commit(&targets, resolved);
                            None
                        }
                        Err(payload) => Some(payload),
                    };
                    // The body's topology requests run even when a write failed.
                    for body_request in body_requests {
                        crate::node::run_queued_request(body_request);
                    }
                    if let Some(payload) = failed {
                        resume_unwind(payload);
                    }
                });
            } else {
                let committed = resolve(&finish.targets, finish.deferred).and_then(|resolved| {
                    catch_unwind(AssertUnwindSafe(|| commit(&finish.targets, resolved)))
                });
                // A failed write function commits nothing; the body's topology requests
                // and those the commit's waves made still run, then the failure reaches
                // the caller.
                clear_active(d);
                crate::node::drain_requests(d);
                if let Err(payload) = committed {
                    resume_unwind(payload);
                }
            }
            value
        }
        Err(payload) => {
            crate::node::drop_requests_after(d, request_mark);
            clear_active(d);
            resume_unwind(payload);
        }
    }
}
