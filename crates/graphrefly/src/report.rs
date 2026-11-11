//! The host reporter (R-failure@3): a callback that receives failures with no caller
//! and no node to end, such as a sink that panics during a fanout. The kernel keeps
//! no failure history; without a reporter such a failure is dropped.
//!
//! The reporter belongs to the graph, the concurrency domain (R-graph-domain@2): it
//! is the `report` option of [`crate::GraphOptions`], as the reference's
//! `createGraph({ report })` (graphrefly-rs:D6). Every node belongs to a graph
//! (review Q2), so every failure has a graph whose option decides where it goes.

use std::any::Any;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;

/// One failure handed to the host reporter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostFailure {
    /// What failed, for example `"sink"`.
    pub source: String,
    /// The failure's message.
    pub error: String,
}

/// The host reporter callback.
pub type HostReporter = Rc<dyn Fn(&HostFailure)>;

/// Hands a failure to the domain's host reporter, if it has one. A reporter that
/// panics is ignored, so it cannot break the wave it reports.
pub(crate) fn report(reporter: Option<&HostReporter>, source: &str, payload: &(dyn Any + Send)) {
    let Some(reporter) = reporter else {
        return;
    };
    let failure = HostFailure {
        source: source.to_owned(),
        error: panic_message(payload),
    };
    let _ = catch_unwind(AssertUnwindSafe(|| reporter(&failure)));
}

pub(crate) fn panic_message(payload: &(dyn Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "panicked".to_owned())
}

/// Wraps a user sink so its panic goes to the host reporter and the fanout goes on
/// (R-failure@3): a sink failure has no node to end.
/// `reporter` looks up the reporter of the domain the sink's node runs in.
pub(crate) fn guard_sink<M>(
    sink: impl Fn(&[M]) + 'static,
    reporter: impl Fn() -> Option<HostReporter> + 'static,
) -> impl Fn(&[M]) + 'static {
    move |msgs: &[M]| {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| sink(msgs))) {
            report(reporter().as_ref(), "sink", payload.as_ref());
        }
    }
}
