//! Injectable local async/time driver boundary (D111).
//!
//! The wave core stays synchronous: driver callbacks re-enter through
//! `DeferredCtx`, and `Dispatcher::invoke` remains sync void. Tokio, when enabled,
//! is only an adapter behind this trait.

use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::time::Duration;

/// Cancels driver-owned work. Dropping it intentionally does nothing; sources
/// register it with `ctx.on_deactivation` so teardown is explicit.
pub type DriverCancel = Box<dyn FnOnce()>;

/// Local, single-thread async/time driver.
///
/// No `Send`/`Sync` bounds: a graph is one single-thread concurrency domain (D22).
pub trait LocalAsyncDriver {
    /// Updates or reads `sleep`.
    fn sleep(&self, duration: Duration, callback: Box<dyn FnOnce()>) -> DriverCancel;
    /// Updates or reads `interval`.
    fn interval(&self, period: Duration, callback: Rc<dyn Fn()>) -> DriverCancel;
    /// Updates or reads `spawn_local`.
    fn spawn_local(&self, fut: Pin<Box<dyn Future<Output = ()> + 'static>>) -> DriverCancel;
}

#[cfg(feature = "tokio")]
#[derive(Debug, Clone, Copy, Default)]
/// `TokioLocalDriver` data container.
pub struct TokioLocalDriver;

#[cfg(feature = "tokio")]
impl LocalAsyncDriver for TokioLocalDriver {
    fn sleep(&self, duration: Duration, callback: Box<dyn FnOnce()>) -> DriverCancel {
        let handle = tokio::task::spawn_local(async move {
            tokio::time::sleep(duration).await;
            callback();
        });
        Box::new(move || handle.abort())
    }

    fn interval(&self, period: Duration, callback: Rc<dyn Fn()>) -> DriverCancel {
        let handle = tokio::task::spawn_local(async move {
            loop {
                tokio::time::sleep(period).await;
                callback();
            }
        });
        Box::new(move || handle.abort())
    }

    fn spawn_local(&self, fut: Pin<Box<dyn Future<Output = ()> + 'static>>) -> DriverCancel {
        let handle = tokio::task::spawn_local(fut);
        Box::new(move || handle.abort())
    }
}
