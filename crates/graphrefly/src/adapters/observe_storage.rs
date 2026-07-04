//! Graph.observe() -> passive storage adapter (D57/D74/D125).
//!
//! Storage frame codecs and append-log paging stay in [`crate::storage`]. This module owns the
//! graph-bound observe subscription that writes those passive frames.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;

use crate::graph::{Graph, GraphObserver, ObserveEvent};
use crate::storage::{
    observe_event_frame, AppendLogStorageTier, ObserveEventFrame, ObserveEventFrameOptions,
    StorageError, StorageResult,
};

#[derive(Clone, Debug, Eq, PartialEq)]
/// `ObserveEventLogErrorPhase` variants.
pub enum ObserveEventLogErrorPhase {
    /// `Map` variant.
    Map,
    /// `Write` variant.
    Write,
    /// `Flush` variant.
    Flush,
    /// `Rollback` variant.
    Rollback,
    /// `Dispose` variant.
    Dispose,
}

#[derive(Clone)]
/// `ObserveEventLogErrorContext` data container.
pub struct ObserveEventLogErrorContext<T> {
    /// `phase` field for phase.
    pub phase: ObserveEventLogErrorPhase,
    /// `event` field for event.
    pub event: Option<ObserveEvent>,
    /// `value` field for value.
    pub value: Option<ObserveEventFrame<T>>,
}

/// `ObserveEventLogMap` type alias.
pub type ObserveEventLogMap<T> = dyn Fn(&ObserveEvent) -> Option<T>;
/// `ObserveEventLogErrorFn` type alias.
pub type ObserveEventLogErrorFn<T> = dyn Fn(StorageError, ObserveEventLogErrorContext<T>);

#[derive(Clone)]
/// `AttachObserveEventLogOptions` data container.
pub struct AttachObserveEventLogOptions<T: Clone> {
    /// `path` field for path.
    pub path: Option<String>,
    /// `stream` field for stream.
    pub stream: Option<String>,
    /// `map` field for map.
    pub map: Rc<ObserveEventLogMap<T>>,
    /// `on_error` field for on error.
    pub on_error: Option<Rc<ObserveEventLogErrorFn<T>>>,
}

impl<T> AttachObserveEventLogOptions<T>
where
    T: Clone + From<ObserveEvent> + 'static,
{
    /// Creates or computes `new`.
    pub fn new() -> Self {
        Self {
            path: None,
            stream: None,
            map: Rc::new(|event| Some(event.clone().into())),
            on_error: None,
        }
    }
}

impl<T> Default for AttachObserveEventLogOptions<T>
where
    T: Clone + From<ObserveEvent> + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone> AttachObserveEventLogOptions<T> {
    /// Creates or computes `from_map`.
    pub fn from_map(map: impl Fn(&ObserveEvent) -> Option<T> + 'static) -> Self {
        Self {
            path: None,
            stream: None,
            map: Rc::new(map),
            on_error: None,
        }
    }

    /// Updates or reads `with_path`.
    pub fn with_path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }

    /// Updates or reads `with_stream`.
    pub fn with_stream(mut self, stream: impl Into<String>) -> Self {
        self.stream = Some(stream.into());
        self
    }

    /// Updates or reads `with_map`.
    pub fn with_map(mut self, map: impl Fn(&ObserveEvent) -> Option<T> + 'static) -> Self {
        self.map = Rc::new(map);
        self
    }

    /// Updates or reads `with_on_error`.
    pub fn with_on_error(
        mut self,
        on_error: impl Fn(StorageError, ObserveEventLogErrorContext<T>) + 'static,
    ) -> Self {
        self.on_error = Some(Rc::new(on_error));
        self
    }
}

/// `ObserveEventLogHandle` data container.
pub struct ObserveEventLogHandle {
    observer: Option<GraphObserver>,
    flush: Rc<dyn Fn() -> StorageResult<()>>,
    rollback: Rc<dyn Fn() -> StorageResult<()>>,
}

impl ObserveEventLogHandle {
    /// Updates or reads `flush`.
    pub fn flush(&self) -> StorageResult<()> {
        (self.flush)()
    }

    /// Updates or reads `rollback`.
    pub fn rollback(&self) -> StorageResult<()> {
        (self.rollback)()
    }

    /// Updates or reads `dispose`.
    pub fn dispose(&mut self) -> StorageResult<()> {
        self.observer.take();
        (self.flush)()
    }
}

impl Drop for ObserveEventLogHandle {
    fn drop(&mut self) {
        let _ = self.dispose();
    }
}

/// Creates or computes `attach_observe_event_log`.
pub fn attach_observe_event_log<T: Clone + 'static>(
    graph: &Graph,
    log: Rc<dyn AppendLogStorageTier<ObserveEventFrame<T>>>,
    opts: AttachObserveEventLogOptions<T>,
) -> ObserveEventLogHandle {
    let stream = match &opts.path {
        Some(path) => graph.observe_path(path),
        None => graph.observe(),
    };
    let map = opts.map.clone();
    let on_error = opts.on_error.clone();
    let frame_opts = ObserveEventFrameOptions {
        stream: opts.stream.clone(),
    };
    let pending = Rc::new(RefCell::new(
        VecDeque::<(ObserveEvent, ObserveEventFrame<T>)>::new(),
    ));
    let flush_pending = {
        let pending = pending.clone();
        let log = log.clone();
        let on_error = on_error.clone();
        Rc::new(move || {
            let mut first_error = None;
            loop {
                let Some((event, frame)) = pending.borrow().front().cloned() else {
                    break;
                };
                if let Err(error) = log.append(frame.clone()) {
                    report_observe_event_log_error(
                        &on_error,
                        error.clone(),
                        ObserveEventLogErrorContext {
                            phase: ObserveEventLogErrorPhase::Write,
                            event: Some(event),
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
    let observer = stream.subscribe(move |event| {
        let mapped = match catch_unwind(AssertUnwindSafe(|| (map)(&event))) {
            Ok(mapped) => mapped,
            Err(_) => {
                report_observe_event_log_error(
                    &on_error,
                    StorageError::backend("attach_observe_event_log: map panicked"),
                    ObserveEventLogErrorContext {
                        phase: ObserveEventLogErrorPhase::Map,
                        event: Some(event),
                        value: None,
                    },
                );
                return;
            }
        };
        let Some(value) = mapped else {
            return;
        };
        let frame = match observe_event_frame(
            event.seq,
            event.path.clone(),
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
                        event: Some(event),
                        value: None,
                    },
                );
                return;
            }
        };
        pending.borrow_mut().push_back((event, frame));
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
