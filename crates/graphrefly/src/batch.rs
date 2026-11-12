//! Batch — declarative transactional wave grouping (D12 / L2.E).
//!
//! `batch` runs a closure that may drive multiple nodes; **commit is always
//! implicit** on `Ok`, **discard on `Err` or panic**. Nested bodies join the outer
//! transaction: a caught inner error does not discard its accumulated writes.
//!
//! ## Interaction with message classes
//!
//! `batch(body)` runs `body` at once, also during a wave, and delivers nothing while
//! it runs: host inputs are staged (a write function computes at the commit from the
//! writes staged before it), and topology requests (subscribe, unsubscribe, replace,
//! ctx.rewire) wait. When `body` returns, its inputs commit as one wave, and its
//! requests then run in order (R-request-order@2). The commit is itself a request:
//! during a wave it runs after the wave. A body that panics commits nothing and drops
//! its requests. Late ctx output is a separate request and survives a commit failure.
//!
//! Conformance touchpoints: C-3 (INVALIDATE fan-in idempotency exercised via
//! batch) and C-22 (batch-before-rewire).

use std::cell::RefCell;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};

use crate::node::{commit_batched_waves, BatchTarget, Core, Domain, DomainRef};
use crate::protocol::{AnyValue, Message, Wave};

/// A state write function staged in a batch: it computes from the value the writes
/// staged before it leave, at the commit (R-request-order@2).
pub(crate) type StagedWrite = Box<dyn FnOnce(Option<AnyValue>) -> Option<AnyValue>>;

/// One staged item of a target, in staging order.
enum Staged {
    /// Host messages, accumulated into one array at commit.
    Msgs { msgs: Wave<AnyValue> },
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
    collecting: bool,
}

impl ActiveBatch {
    fn new() -> Self {
        Self {
            targets: Vec::new(),
            deferred: Vec::new(),
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
        staged.push(Staged::Msgs { msgs: wave });
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
}

fn take_for_finish(d: &Domain) -> BatchFinish {
    with_batch(d, |b| {
        let mut active = b.borrow_mut();
        let batch = active.as_mut().expect("batch frame installed");
        batch.collecting = false;
        BatchFinish {
            targets: std::mem::take(&mut batch.targets),
            deferred: std::mem::take(&mut batch.deferred),
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
    // Validate every host target before any updater runs. A terminal queued before
    // this commit refuses the whole commit, including writes to other states.
    catch_unwind(AssertUnwindSafe(|| {
        for target in targets {
            if let Some(core) = target.borrowed_core().filter(Core::is_state) {
                crate::node::refuse_if_ended(&core);
            }
        }
    }))?;

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
                Staged::Msgs { msgs } => {
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
pub(crate) fn batch_in<R, E>(d: &DomainRef, f: impl FnOnce() -> Result<R, E>) -> Result<R, E> {
    if batch_frame_open(d) {
        return f();
    }
    with_batch(d, |b| *b.borrow_mut() = Some(ActiveBatch::new()));
    let request_mark = crate::node::request_mark(d);
    let result = catch_unwind(AssertUnwindSafe(f));
    let finish = take_for_finish(d);

    match result {
        Ok(Ok(value)) => {
            if crate::node::in_wave(d) {
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
            Ok(value)
        }
        Ok(Err(error)) => {
            crate::node::drop_requests_after(d, request_mark);
            clear_active(d);
            Err(error)
        }
        Err(payload) => {
            crate::node::drop_requests_after(d, request_mark);
            clear_active(d);
            resume_unwind(payload);
        }
    }
}
