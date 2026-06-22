//! Host-language control-flow escapes for native bindings (D431).
//!
//! This module is intentionally not part of the graph protocol. Bindings use it
//! to tunnel process-control failures such as Python `KeyboardInterrupt` through
//! Rust wave/batch unwind boundaries without converting them to graph ERROR.

use std::any::Any;
use std::cell::Cell;
use std::panic::panic_any;

thread_local! {
    static HOST_BOUNDARY_ABORT_ARMED: Cell<usize> = const { Cell::new(0) };
}

/// Opaque panic payload used by native host bindings to abort back to the host.
#[derive(Debug)]
pub struct HostBoundaryAbort {
    _private: (),
}

/// Run a native binding entry point with D431 host-boundary abort enabled.
pub fn with_host_boundary_abort_armed<R>(f: impl FnOnce() -> R) -> R {
    struct ArmedGuard;

    impl Drop for ArmedGuard {
        fn drop(&mut self) {
            HOST_BOUNDARY_ABORT_ARMED.with(|armed| armed.set(armed.get().saturating_sub(1)));
        }
    }

    HOST_BOUNDARY_ABORT_ARMED.with(|armed| armed.set(armed.get() + 1));
    let _guard = ArmedGuard;
    f()
}

/// Abort the current Rust graph boundary for a host-language fatal exception.
///
/// The binding must store the original host exception before calling this
/// helper; the marker carries no host object and must never become graph DATA.
pub fn abort_host_boundary() -> ! {
    let armed = HOST_BOUNDARY_ABORT_ARMED.with(|armed| armed.get() > 0);
    if armed {
        panic_any(HostBoundaryAbort { _private: () })
    }
    panic!("host boundary abort requested outside a native host boundary (D431)")
}

/// Return true when a panic payload is the D431 host-boundary abort marker.
pub fn is_host_boundary_abort_payload(payload: &(dyn Any + Send)) -> bool {
    payload.is::<HostBoundaryAbort>()
}
