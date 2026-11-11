//! A node's lifetime (R-lifetime@2): one activation of an instance, with its ctx,
//! private state, abort signal and cleanup.
//!
//! A computed node may have a setup ([`crate::NodeOpts::with_setup`]). It runs once
//! per lifetime, just before the function's first run in it, with that run's ctx: what
//! it sends is part of that run's output, a panic in it ends the node as a panic of the
//! function does, and the [`Cleanup`] it returns is the lifetime's cleanup. Closing a
//! lifetime, at deactivation or when the node ends, aborts its signal, releases its
//! dependencies, runs the cleanup (which may still read the private state) and then
//! drops the private state. A cleanup's panic goes to the host reporter.
//!
//! Output and requests made through a ctx whose lifetime has closed are dropped.

use std::cell::Cell;
use std::fmt;
use std::rc::Rc;

use crate::ctx::Ctx;
use crate::protocol::AnyValue;

/// A lifetime's abort signal (R-lifetime@2): aborted when the lifetime closes. Work a
/// function or setup starts checks it before it sends a late result.
#[derive(Clone, Default)]
pub struct AbortSignal(Rc<Cell<bool>>);

impl AbortSignal {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Whether the lifetime this signal belongs to has closed.
    pub fn is_aborted(&self) -> bool {
        self.0.get()
    }

    pub(crate) fn abort(&self) {
        self.0.set(true);
    }
}

impl fmt::Debug for AbortSignal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AbortSignal")
            .field("aborted", &self.is_aborted())
            .finish()
    }
}

/// What a running cleanup can read: the closing lifetime's private state, which is
/// dropped only after the cleanup returns (R-lifetime@2).
pub struct CleanupCtx {
    pub(crate) state: Option<AnyValue>,
}

impl CleanupCtx {
    /// The closing lifetime's private state, typed.
    pub fn state_get<S: 'static>(&self) -> Option<Rc<S>> {
        self.state.clone().and_then(|a| a.downcast::<S>().ok())
    }
}

/// A lifetime's cleanup, returned by its setup.
pub type Cleanup = Box<dyn FnOnce(&CleanupCtx)>;

/// A node's setup (R-lifetime@2): runs once per lifetime, before the function's first
/// run in it, with that run's ctx, and may return the lifetime's cleanup.
#[derive(Clone)]
pub struct Setup(pub(crate) Rc<SetupFn>);

/// The function a [`Setup`] wraps.
pub type SetupFn = dyn Fn(&Ctx) -> Option<Cleanup>;

impl Setup {
    /// Wraps a setup function.
    pub fn new(f: impl Fn(&Ctx) -> Option<Cleanup> + 'static) -> Self {
        Self(Rc::new(f))
    }
}

impl fmt::Debug for Setup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Setup(..)")
    }
}

/// Wraps a closure as a lifetime [`Cleanup`], for a setup's return value.
pub fn cleanup(f: impl FnOnce(&CleanupCtx) + 'static) -> Option<Cleanup> {
    Some(Box::new(f))
}

/// The open lifetime of a node, kept in its arena slot.
pub(crate) struct Lifetime {
    pub(crate) id: u64,
    pub(crate) signal: AbortSignal,
    pub(crate) cleanup: Option<Cleanup>,
    pub(crate) setup_done: bool,
}

thread_local! {
    static NEXT_LIFETIME: Cell<u64> = const { Cell::new(1) };
}

impl Lifetime {
    pub(crate) fn open() -> Self {
        let id = NEXT_LIFETIME.with(|n| {
            let id = n.get();
            n.set(id + 1);
            id
        });
        Self {
            id,
            signal: AbortSignal::new(),
            cleanup: None,
            setup_done: false,
        }
    }
}
