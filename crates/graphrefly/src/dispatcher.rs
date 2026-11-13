//! The dispatcher — first-class invoke funnel over one function table (D21,
//! graphrefly:D807).
//!
//! [`Dispatcher::invoke`] is synchronous and returns `()`: a node function has the
//! shape `(ctx) => void` (R-invocation@2). The wave core never blocks on async;
//! async work runs through the local async driver, a worker, or the wire bridge,
//! and comes back as a later wave (R-wave@2, R-graph-domain@3).
//!
//! Every node function is registered in the dispatcher's one function table and
//! addressed by a pure-data [`Handle`]: its index and the slot's generation (D7,
//! B32). The table's
//! make-up is this crate's implementation, not protocol, and it has no pools
//! (graphrefly:D807): an async function kicks
//! off work and emits later through a kept [`crate::ctx::DeferredCtx`], with the
//! same synchronous invoke.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;
#[cfg(feature = "tokio-worker")]
use std::sync::Arc;
use std::time::Instant;

use crate::async_driver::LocalAsyncDriver;
use crate::ctx::Ctx;
pub use crate::protocol::Handle;

/// A node fn: `(&Ctx) -> ()` (R-fn-contract / D8). Single-thread (D22) ⇒ no
/// `Send` bound. Stored as `Rc` so [`Dispatcher::invoke`] can clone it out of the
/// table and **release the table borrow before calling it** — a fn that re-drives a
/// downstream node (nested `invoke`) must not find the table already borrowed.
pub type NodeFn = Rc<dyn Fn(&Ctx)>;

/// One slotmap entry: a fn (`None` once freed) + a generation bumped on each free,
/// so a recycled slot is distinguishable from a stale handle (B32).
struct Slot {
    f: Option<NodeFn>,
    generation: u32,
}

/// The dispatcher's one function table: a **slotmap** of fns addressed by
/// `(index, generation)`.
///
/// A node's fn is freed (slot recycled) when the node drops, releasing its captured
/// upstream handles (B32).
struct FunctionTable {
    slots: Vec<Slot>,
    /// Recycled (freed) slot ids, reused on the next register.
    free: Vec<u32>,
}

impl FunctionTable {
    fn new() -> Self {
        Self {
            slots: Vec::new(),
            free: Vec::new(),
        }
    }

    /// Register a fn, returning its handle. Reuses a freed slot if one is available
    /// (keeping the slot vec bounded under churn).
    fn register(&mut self, f: NodeFn) -> Handle {
        if let Some(index) = self.free.pop() {
            let slot = &mut self.slots[index as usize];
            slot.f = Some(f);
            Handle {
                index,
                generation: slot.generation,
            }
        } else {
            let index = self.slots.len() as u32;
            self.slots.push(Slot {
                f: Some(f),
                generation: 0,
            });
            Handle {
                index,
                generation: 0,
            }
        }
    }

    /// Free a slot (drop its fn) + bump its generation + recycle it. A stale
    /// unregister (generation mismatch, or already free) is a no-op.
    /// Returns the freed fn, for the caller to drop after its borrow ends: the fn may
    /// hold the last handles to other nodes, whose drop unregisters their fns.
    fn unregister(&mut self, handle: Handle) -> Option<NodeFn> {
        let slot = self.slots.get_mut(handle.index as usize)?;
        if slot.generation != handle.generation || slot.f.is_none() {
            return None;
        }
        let freed = slot.f.take();
        slot.generation = slot.generation.wrapping_add(1);
        self.free.push(handle.index);
        freed
    }

    /// Clone out the fn for a handle, iff the generation matches and the slot is live.
    /// A stale handle (mismatched generation / freed slot) yields `None`.
    fn get(&self, handle: Handle) -> Option<NodeFn> {
        self.slots
            .get(handle.index as usize)
            .filter(|s| s.generation == handle.generation)
            .and_then(|s| s.f.clone())
    }
}

struct DispatcherInner {
    /// The one function table (graphrefly:D807).
    functions: FunctionTable,
    /// Opt-in profile recorder (D39 / R-profile). Default off = one boolean branch
    /// per invoke; counters live on the dispatcher, not on the thin node.
    recording: bool,
    stats: HashMap<Handle, ProfileStat>,
    local_async_driver: Option<Rc<dyn LocalAsyncDriver>>,
    #[cfg(feature = "tokio-worker")]
    worker_backend: Option<WorkerBackend>,
}

/// First-class dispatcher (D21), cloneable handle over shared inner state. A graph
/// binds one; the default is a thread-local singleton (D26).
#[derive(Clone)]
pub struct Dispatcher(Rc<RefCell<DispatcherInner>>);

impl fmt::Debug for Dispatcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Dispatcher").finish_non_exhaustive()
    }
}

/// Per-handle accumulated profile counters (D39).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProfileStat {
    pub invokes: u64,
    pub total_duration_ns: u128,
    pub last_duration_ns: u128,
}

#[cfg(feature = "tokio-worker")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkerSubmitError {
    MissingBackend,
    MissingRuntime,
}

#[cfg(feature = "tokio-worker")]
#[derive(Debug, Clone, Copy, Default)]
struct WorkerBackend;

#[cfg(feature = "tokio-worker")]
type WorkerTask<R> = Box<dyn FnOnce() -> Result<R, String> + Send + 'static>;

#[cfg(feature = "tokio-worker")]
pub(crate) struct WorkerJob<R> {
    handle: tokio::runtime::Handle,
    task: WorkerTask<R>,
}

#[cfg(feature = "tokio-worker")]
impl<R: Send + 'static> WorkerJob<R> {
    pub(crate) fn spawn(self) -> tokio::task::JoinHandle<Result<R, String>> {
        let task = self.task;
        self.handle.spawn_blocking(task)
    }
}

#[cfg(feature = "tokio-worker")]
impl WorkerBackend {
    fn submit<I, R, E, C>(
        self,
        input: I,
        compute: Arc<C>,
    ) -> Result<WorkerJob<R>, WorkerSubmitError>
    where
        I: Send + 'static,
        R: Send + 'static,
        E: fmt::Display + Send + 'static,
        C: Fn(I) -> Result<R, E> + Send + Sync + 'static,
    {
        let handle =
            tokio::runtime::Handle::try_current().map_err(|_| WorkerSubmitError::MissingRuntime)?;
        Ok(WorkerJob {
            handle,
            task: Box::new(move || compute(input).map_err(|error| error.to_string())),
        })
    }
}

impl Dispatcher {
    pub fn new() -> Self {
        Dispatcher(Rc::new(RefCell::new(DispatcherInner {
            functions: FunctionTable::new(),
            recording: false,
            stats: HashMap::new(),
            local_async_driver: None,
            #[cfg(feature = "tokio-worker")]
            worker_backend: Some(WorkerBackend),
        })))
    }

    /// Turn the profile recorder on/off (D39). Off = near-zero invoke overhead.
    pub fn set_recording(&self, on: bool) {
        self.0.borrow_mut().recording = on;
    }

    /// Read one handle's accumulated counters.
    pub fn stat_for(&self, handle: Handle) -> Option<ProfileStat> {
        self.0.borrow().stats.get(&handle).copied()
    }

    /// Install or clear the graph-local async/time source driver (D111).
    pub fn set_local_async_driver(&self, driver: Option<Rc<dyn LocalAsyncDriver>>) {
        self.0.borrow_mut().local_async_driver = driver;
    }

    /// Read the installed local async/time source driver, if any.
    pub fn local_async_driver(&self) -> Option<Rc<dyn LocalAsyncDriver>> {
        self.0.borrow().local_async_driver.clone()
    }

    #[cfg(feature = "tokio-worker")]
    pub(crate) fn submit_worker<I, R, E, C>(
        &self,
        input: I,
        compute: Arc<C>,
    ) -> Result<WorkerJob<R>, WorkerSubmitError>
    where
        I: Send + 'static,
        R: Send + 'static,
        E: fmt::Display + Send + 'static,
        C: Fn(I) -> Result<R, E> + Send + Sync + 'static,
    {
        let backend = self
            .0
            .borrow()
            .worker_backend
            .ok_or(WorkerSubmitError::MissingBackend)?;
        backend.submit(input, compute)
    }

    #[cfg(all(test, feature = "tokio-worker"))]
    pub(crate) fn set_worker_backend_for_test(&self, installed: bool) {
        self.0.borrow_mut().worker_backend = installed.then_some(WorkerBackend);
    }

    /// Register a fn in the function table, returning its [`Handle`].
    pub fn register(&self, f: NodeFn) -> Handle {
        self.0.borrow_mut().functions.register(f)
    }

    /// The function registered under `handle`, if it is still registered.
    pub(crate) fn function(&self, handle: Handle) -> Option<NodeFn> {
        self.0.borrow().functions.get(handle)
    }

    /// Free a node's fn slot (B32) — called when a node's slot is freed. The table stops
    /// holding a dropped node's fn (and thus its captured upstream `Core`s) alive for
    /// the whole process. Idempotent / generation-checked: a stale unregister is a no-op.
    pub fn unregister(&self, handle: Handle) {
        let freed = {
            let mut inner = self.0.borrow_mut();
            let freed = inner.functions.unregister(handle);
            inner.stats.remove(&handle);
            freed
        };
        drop(freed);
    }

    /// Uniform synchronous invoke (R-invocation@2). Clones the fn out
    /// and drops the table borrow before calling, so a nested downstream invoke
    /// triggered from inside the fn does not double-borrow the table.
    pub fn invoke(&self, handle: Handle, ctx: &Ctx) {
        let (f, recording) = {
            let inner = self.0.borrow();
            (inner.functions.get(handle), inner.recording)
        };
        // A live node is the SOLE invoker of its own handle, so the slot is present on
        // the live path. A generation mismatch (a stale handle — e.g. a future
        // async deferred callback firing after the node dropped + the slot was
        // recycled) yields None → a silent no-op, never a recycled-slot misfire.
        if let Some(f) = f {
            if recording {
                let _profile = InvokeProfileGuard {
                    dispatcher: self.clone(),
                    handle,
                    start: Instant::now(),
                };
                f(ctx);
            } else {
                f(ctx);
            }
        }
    }
}

struct InvokeProfileGuard {
    dispatcher: Dispatcher,
    handle: Handle,
    start: Instant,
}

impl Drop for InvokeProfileGuard {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed().as_nanos();
        let mut inner = self.dispatcher.0.borrow_mut();
        let stat = inner.stats.entry(self.handle).or_default();
        stat.invokes += 1;
        stat.total_duration_ns += elapsed;
        stat.last_duration_ns = elapsed;
    }
}

impl Default for Dispatcher {
    fn default() -> Self {
        Self::new()
    }
}

thread_local! {
    /// The only global singleton (D26) — thread-local because the substrate is
    /// single-thread / `!Send` (D22). Overridable by passing an explicit dispatcher.
    static DEFAULT: Dispatcher = Dispatcher::new();
}

/// A clone of the thread-local default dispatcher (D26).
pub fn default_dispatcher() -> Dispatcher {
    DEFAULT.with(|d| d.clone())
}
