//! The host reporter (R-failure@3): a callback that receives failures with no caller
//! and no node to end, such as a sink that panics during a fanout. The kernel keeps
//! no failure history; without a reporter such a failure is dropped.
//!
//! Rust-local shape (graphrefly-rs:D6): one reporter per thread, since the runtime
//! is single-threaded (each thread runs its own waves).

use std::any::Any;
use std::cell::RefCell;
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

thread_local! {
    static REPORTER: RefCell<Option<HostReporter>> = const { RefCell::new(None) };
}

/// Install (or with `None` remove) this thread's host reporter; returns the previous one.
pub fn set_host_reporter(reporter: Option<HostReporter>) -> Option<HostReporter> {
    REPORTER.with(|slot| std::mem::replace(&mut *slot.borrow_mut(), reporter))
}

/// Hands a failure to the host reporter, if one is installed. A reporter that panics
/// is ignored, so it cannot break the wave it reports.
pub(crate) fn report(source: &str, payload: &(dyn Any + Send)) {
    let Some(reporter) = REPORTER.with(|slot| slot.borrow().clone()) else {
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
pub(crate) fn guard_sink<M>(sink: impl Fn(&[M]) + 'static) -> impl Fn(&[M]) + 'static {
    move |msgs: &[M]| {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| sink(msgs))) {
            report("sink", payload.as_ref());
        }
    }
}
