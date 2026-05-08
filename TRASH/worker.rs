//! Worker-thread Core dispatcher (D062 + D063).
//!
//! Per the M3 napi-rs operator parity design call (see
//! `~/src/graphrefly-ts/archive/docs/SESSION-rust-port-napi-operators.md`
//! §2.2 / §7 Q1 / §7 Q2), the napi binding cannot drive `Core` directly
//! from inside a `#[napi]` method handler when JS callbacks are involved:
//!
//! - `BenchOperators::register_map(...)` stores a JS callback.
//! - A subsequent `BenchCore::emit_int(...)` triggers a wave that fires
//!   `OperatorBinding::project_each(fn_id, h)` → the closure invokes the
//!   stored JS callback via `ThreadsafeFunction::call_with_return_value`.
//! - That call queues to libuv and the result-handler runs on the JS
//!   event-loop thread. If the calling Rust thread IS the JS thread,
//!   libuv cannot drain the tick → **deadlock**.
//!
//! Fix per D062: every `#[napi]` method enqueues work to a dedicated
//! per-`BenchCore` dispatcher thread. The worker drives Core; TSFN calls
//! from the worker → JS thread are non-deadlocking because the JS thread
//! is parked inside the napi method's `recv()` and can drain libuv when
//! the napi method's blocking `recv` releases via the bridge oneshot.
//!
//! # Subscription ownership
//!
//! [`Subscription`]s are retained inside [`WorkerState`] (worker-local
//! state). Subscription `Drop` accesses Core's internal mutex; dropping
//! on the JS thread would race with the worker holding that mutex
//! mid-wave. Storing subs on the worker keeps Drop on the same thread
//! that drives Core. JS-thread methods access subs via `dispatch(...)`.
//!
//! # Re-entrance
//!
//! If a JS callback (running on the JS thread inside a TSFN tick) calls
//! back into `BenchCore::*`, that nested napi method tries to dispatch
//! to the worker — but the worker is currently parked inside the
//! TSFN-bridge oneshot waiting for the JS callback to return. Sending a
//! new command would deadlock.
//!
//! v1 policy: **JS callbacks must not re-enter the binding.** Documented
//! in `BenchOperators` rustdoc + `porting-deferred.md`.
//!
//! # Shutdown
//!
//! `WorkerHandle::Drop` sends `CoreCommand::Shutdown`, then joins the
//! worker thread. The worker exits its loop on `Shutdown`, then drops
//! `WorkerState` — which drops `subscriptions` first (each Subscription
//! `Drop` runs on the worker thread, locking Core's mutex safely) then
//! drops `core` (releases Arc<CoreState>).

use std::cell::RefCell;
use std::sync::mpsc::{self, SyncSender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use graphrefly_core::{Core, Subscription};

thread_local! {
    /// Worker-thread-local handle on the worker's `Core`. Set when the
    /// worker thread starts; cleared when it exits. Read by
    /// [`BenchBinding::invoke_fn`]'s producer-dispatch branch (when the
    /// operators feature is on) to construct
    /// [`graphrefly_operators::producer::ProducerCtx`] without forcing
    /// `BenchBinding` to hold its own `Core` clone (which would create
    /// an Arc cycle with `Core.binding`).
    ///
    /// **Read access**: only valid from inside `WorkerHandle::dispatch`'s
    /// closure (which always runs on the worker thread). `current_core`
    /// returns `None` if called from any other thread.
    static WORKER_CORE: RefCell<Option<Core>> = const { RefCell::new(None) };
}

/// Returns a clone of the current worker thread's `Core`, if the caller
/// is on the worker thread. Used by `BenchBinding::invoke_fn` to access
/// `Core` for producer dispatch without holding a `Core` clone on the
/// binding (which would cycle).
#[must_use]
pub(crate) fn current_core() -> Option<Core> {
    WORKER_CORE.with(|c| c.borrow().clone())
}

/// Worker-thread-local state. Owned exclusively by the worker thread;
/// JS-thread callers reach it via `WorkerHandle::dispatch`.
pub(crate) struct WorkerState {
    pub(crate) core: Core,
    /// Retained subscriptions keyed by index. `None` slots represent
    /// unsubscribed entries (we don't compact to keep indices stable).
    pub(crate) subscriptions: Vec<Option<Subscription>>,
}

impl WorkerState {
    /// Push a subscription and return its stable index.
    pub(crate) fn push_subscription(&mut self, sub: Subscription) -> usize {
        let idx = self.subscriptions.len();
        self.subscriptions.push(Some(sub));
        idx
    }

    /// Drop the subscription at `idx` (releases activation refcount).
    /// No-op if the index is out of range or already `None`.
    pub(crate) fn unsubscribe(&mut self, idx: usize) {
        if let Some(slot) = self.subscriptions.get_mut(idx) {
            // Replacing with None drops the inner Subscription, which
            // releases Core's per-node activation refcount on the
            // worker thread.
            *slot = None;
        }
    }
}

/// Boxed work unit sent to the worker thread.
type Work = Box<dyn FnOnce(&mut WorkerState) + Send + 'static>;

enum CoreCommand {
    Run(Work),
    Shutdown,
}

/// Handle to the per-`BenchCore` worker thread. All `Core`-touching
/// `#[napi]` methods route through `dispatch(...)`.
pub(crate) struct WorkerHandle {
    sender: SyncSender<CoreCommand>,
    join: Option<JoinHandle<()>>,
}

impl WorkerHandle {
    /// Spawn a fresh worker thread that owns `core`. Subscriptions Vec
    /// starts empty.
    #[must_use]
    pub(crate) fn spawn(core: Core) -> Self {
        // Bounded channel — backlog beyond 64 in-flight commands signals
        // upstream issues; provide backpressure rather than unbounded
        // memory growth.
        let (sender, receiver) = mpsc::sync_channel::<CoreCommand>(64);
        let join = thread::Builder::new()
            .name("graphrefly-bindings-js-worker".to_owned())
            .spawn(move || {
                // Establish the worker-local Core handle BEFORE the
                // first command runs. Producer dispatch in
                // `BenchBinding::invoke_fn` uses this via
                // `current_core()` to build `ProducerCtx::new(...)`.
                WORKER_CORE.with(|cell| *cell.borrow_mut() = Some(core.clone()));

                let mut state = WorkerState {
                    core,
                    subscriptions: Vec::new(),
                };
                while let Ok(cmd) = receiver.recv() {
                    match cmd {
                        CoreCommand::Run(work) => work(&mut state),
                        CoreCommand::Shutdown => break,
                    }
                }
                // Clear the thread-local BEFORE WorkerState drops; the
                // Drop pass triggers Subscription cleanups which call
                // back into Core, and we don't want stale state in the
                // thread-local during those.
                WORKER_CORE.with(|cell| *cell.borrow_mut() = None);

                // state drops here on the worker thread:
                //   subscriptions Vec drops first (each Subscription
                //   releases Core's activation refcount via worker-side
                //   Mutex access — safe), then core drops (Arc<CoreState>
                //   release).
                drop(state);
            })
            .expect("invariant: worker thread spawn must succeed");
        Self {
            sender,
            join: Some(join),
        }
    }

    /// Run `f` on the worker thread, blocking the caller until it
    /// returns.
    ///
    /// # Panics
    ///
    /// Panics if the worker thread has died (post-`Shutdown` or after a
    /// worker-side panic). In normal operation this is unreachable.
    pub(crate) fn dispatch<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut WorkerState) -> R + Send + 'static,
        R: Send + 'static,
    {
        let (result_tx, result_rx) = mpsc::sync_channel::<R>(1);
        let work: Work = Box::new(move |state| {
            let result = f(state);
            // Silently drop if the caller is no longer waiting.
            let _ = result_tx.send(result);
        });
        self.sender
            .send(CoreCommand::Run(work))
            .expect("invariant: worker thread alive (send fails only after Drop)");
        result_rx
            .recv()
            .expect("invariant: worker delivers exactly one result per Run")
    }
}

impl Drop for WorkerHandle {
    fn drop(&mut self) {
        let _ = self.sender.send(CoreCommand::Shutdown);
        if let Some(join) = self.join.take() {
            // Ignore worker-side panics — they were surfaced via the
            // dispatch that triggered them; nothing to do here.
            let _ = join.join();
        }
    }
}

/// Convenience constructor — both `BenchCore` and `BenchOperators` share
/// a single `Arc<WorkerHandle>` per D051.
#[must_use]
pub(crate) fn spawn(core: Core) -> Arc<WorkerHandle> {
    Arc::new(WorkerHandle::spawn(core))
}
