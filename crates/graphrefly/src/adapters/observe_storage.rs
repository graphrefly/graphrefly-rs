//! Graph.observe() -> passive storage adapter (D57/D74/D125).
//!
//! Storage frame codecs and append-log paging stay in [`crate::storage`]. This module owns the
//! graph-bound observe subscription that writes those passive frames.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;

use crate::graph::{Graph, ObserveEvent};
use crate::storage::{
    observe_event_frame, AppendLogStorageTier, ObserveEventFrame, ObserveEventFrameOptions,
    StorageError, StorageResult,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObserveEventLogErrorPhase {
    Map,
    Write,
    Flush,
    Rollback,
    Dispose,
}

#[derive(Clone)]
pub struct ObserveEventLogErrorContext<T> {
    pub phase: ObserveEventLogErrorPhase,
    /// The id of the observed node whose array failed, if any.
    pub id: Option<String>,
    pub value: Option<ObserveEventFrame<T>>,
}

pub type ObserveEventLogMap<T> = dyn Fn(&ObserveEvent<'_>) -> Option<T>;
pub type ObserveEventLogErrorFn<T> = dyn Fn(StorageError, ObserveEventLogErrorContext<T>);

#[derive(Clone)]
pub struct AttachObserveEventLogOptions<T: Clone> {
    /// The ids to observe; `None` observes every node [`Graph::describe`] gives.
    pub ids: Option<Vec<String>>,
    pub stream: Option<String>,
    pub map: Rc<ObserveEventLogMap<T>>,
    pub on_error: Option<Rc<ObserveEventLogErrorFn<T>>>,
}

impl<T: Clone> AttachObserveEventLogOptions<T> {
    pub fn from_map(map: impl Fn(&ObserveEvent<'_>) -> Option<T> + 'static) -> Self {
        Self {
            ids: None,
            stream: None,
            map: Rc::new(map),
            on_error: None,
        }
    }

    /// Observes only the nodes `ids` names.
    pub fn with_ids(mut self, ids: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.ids = Some(ids.into_iter().map(Into::into).collect());
        self
    }

    pub fn with_stream(mut self, stream: impl Into<String>) -> Self {
        self.stream = Some(stream.into());
        self
    }

    pub fn with_map(mut self, map: impl Fn(&ObserveEvent<'_>) -> Option<T> + 'static) -> Self {
        self.map = Rc::new(map);
        self
    }

    pub fn with_on_error(
        mut self,
        on_error: impl Fn(StorageError, ObserveEventLogErrorContext<T>) + 'static,
    ) -> Self {
        self.on_error = Some(Rc::new(on_error));
        self
    }
}

pub struct ObserveEventLogHandle {
    observer: Option<Box<dyn FnOnce()>>,
    flush: Rc<dyn Fn() -> StorageResult<()>>,
    rollback: Rc<dyn Fn() -> StorageResult<()>>,
}

impl ObserveEventLogHandle {
    pub fn flush(&self) -> StorageResult<()> {
        (self.flush)()
    }

    pub fn rollback(&self) -> StorageResult<()> {
        (self.rollback)()
    }

    pub fn dispose(&mut self) -> StorageResult<()> {
        if let Some(stop) = self.observer.take() {
            stop();
        }
        (self.flush)()
    }
}

impl Drop for ObserveEventLogHandle {
    fn drop(&mut self) {
        let _ = self.dispose();
    }
}

pub fn attach_observe_event_log<T: Clone + 'static>(
    graph: &Graph,
    log: Rc<dyn AppendLogStorageTier<ObserveEventFrame<T>>>,
    opts: AttachObserveEventLogOptions<T>,
) -> ObserveEventLogHandle {
    let ids = opts
        .ids
        .clone()
        .unwrap_or_else(|| graph.describe().into_iter().map(|node| node.id).collect());
    // The log's own order: one sequence number per observed array it appends.
    let observe_seq = Rc::new(std::cell::Cell::new(0u64));
    let map = opts.map.clone();
    let on_error = opts.on_error.clone();
    let frame_opts = ObserveEventFrameOptions {
        stream: opts.stream.clone(),
    };
    let pending = Rc::new(RefCell::new(
        VecDeque::<(String, ObserveEventFrame<T>)>::new(),
    ));
    let flush_pending = {
        let pending = pending.clone();
        let log = log.clone();
        let on_error = on_error.clone();
        Rc::new(move || {
            let mut first_error = None;
            loop {
                let Some((id, frame)) = pending.borrow().front().cloned() else {
                    break;
                };
                if let Err(error) = log.append(frame.clone()) {
                    report_observe_event_log_error(
                        &on_error,
                        error.clone(),
                        ObserveEventLogErrorContext {
                            phase: ObserveEventLogErrorPhase::Write,
                            id: Some(id),
                            value: Some(frame),
                        },
                    );
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                    break;
                }
                pending.borrow_mut().pop_front();
            }
            match first_error {
                Some(error) => Err(error),
                None => Ok(()),
            }
        }) as Rc<dyn Fn() -> StorageResult<()>>
    };
    let rollback_pending = {
        let pending = pending.clone();
        Rc::new(move || {
            pending.borrow_mut().clear();
            Ok(())
        }) as Rc<dyn Fn() -> StorageResult<()>>
    };
    let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();
    let observer = graph.observe(&id_refs, move |event| {
        let mapped = match catch_unwind(AssertUnwindSafe(|| (map)(event))) {
            Ok(mapped) => mapped,
            Err(_) => {
                report_observe_event_log_error(
                    &on_error,
                    StorageError::backend("attach_observe_event_log: map panicked"),
                    ObserveEventLogErrorContext {
                        phase: ObserveEventLogErrorPhase::Map,
                        id: Some(event.id.to_owned()),
                        value: None,
                    },
                );
                return;
            }
        };
        let Some(value) = mapped else {
            return;
        };
        let seq = observe_seq.get();
        observe_seq.set(seq + 1);
        let frame = match observe_event_frame(
            seq,
            event.id.to_owned(),
            value,
            ObserveEventFrameOptions {
                stream: frame_opts.stream.clone(),
            },
        ) {
            Ok(frame) => frame,
            Err(error) => {
                report_observe_event_log_error(
                    &on_error,
                    error,
                    ObserveEventLogErrorContext {
                        phase: ObserveEventLogErrorPhase::Map,
                        id: Some(event.id.to_owned()),
                        value: None,
                    },
                );
                return;
            }
        };
        pending.borrow_mut().push_back((event.id.to_owned(), frame));
    });
    ObserveEventLogHandle {
        observer: Some(observer),
        flush: flush_pending,
        rollback: rollback_pending,
    }
}

fn report_observe_event_log_error<T: Clone>(
    on_error: &Option<Rc<ObserveEventLogErrorFn<T>>>,
    error: StorageError,
    ctx: ObserveEventLogErrorContext<T>,
) {
    if let Some(on_error) = on_error {
        let _ = catch_unwind(AssertUnwindSafe(|| on_error(error, ctx)));
    }
}
