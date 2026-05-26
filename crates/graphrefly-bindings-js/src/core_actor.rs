//! Core-owner actor — α/γ-converged single-thread Core wrapper (S6/D255).
//!
//! # Why
//!
//! Post-S2c (D247/D248/D249) `Core` is `!Send + !Sync` (substrate `Sink`/
//! `TopologySink` dropped `Send + Sync`; the actor-model shape D221/D223/
//! D248 locked in). `impl Clone for Core` is deleted (D221/D246). Every
//! napi `async fn` runs on tokio's blocking pool, which has no thread-
//! affinity guarantee — successive calls land on arbitrary workers — so
//! the previous `Bench* { core: Core }` + `self.core.clone()` + `napi::
//! tokio_runtime::spawn_blocking(move || { core... })` shape fails to
//! compile against the post-S2c substrate.
//!
//! # What
//!
//! `CoreActor` owns `Core` on a dedicated `std::thread::spawn`-ed worker
//! thread for the actor's lifetime. The worker loops on a
//! [`crossbeam_channel::Receiver`] of `FnOnce(&Core) + Send + 'static`
//! closures; every napi method posts a closure + awaits its reply via
//! [`std::sync::mpsc::sync_channel(1)`] (same shape as
//! [`crate::operator_bindings::bridge_sync`]). When all `Arc<CoreActor>`
//! clones drop, the channel sender count hits zero, the worker's
//! `recv()` returns `Err`, the loop exits, and `Core` drops on the
//! worker stack — single-owner invariant preserved end-to-end.
//!
//! # How it fits the actor model
//!
//! - **`Core` never leaves the worker thread.** No `Send`/`Sync` bound
//!   on `Core` is implied by the bindings (they store `Arc<CoreActor>`,
//!   not `Core`).
//! - **Producer-dispatch path uses [`crate::core_bindings::BenchBinding`]'s
//!   `BindingBoundary::invoke_fn_with_core` override** (D245/D246-r5):
//!   Core hands the binding `&dyn CoreFull` directly when firing a
//!   producer build closure, so no thread-local `current_core()` /
//!   `CoreThreadGuard` plumbing is needed (the previous shape was
//!   pre-D245 legacy; deleted in this slice per D256 reconciled).
//! - **Cross-thread bridge for autonomous async producers** stays the
//!   `Arc<CoreMailbox>` (D227/D230) — `Core::mailbox()` returns it,
//!   timer tasks etc. post `MailboxOp::Emit` through it; the worker
//!   drains owner-side via `Core::drain_mailbox` / in-wave `BatchGuard`.
//! - **Single-owner invariant (D252).** Each `BenchCore` instance spawns
//!   its own actor thread, so each Core has its own dedicated OS thread.
//!   The D252 "one Core per OS thread" `IN_TICK_OWNED` panic invariant
//!   is satisfied by construction in-tree (a cross-Core same-thread
//!   nesting requires two BenchCores on the *same* thread — only
//!   possible from an out-of-tree consumer instantiating two BenchCores
//!   *and* synchronously dispatching cross-Core, which is itself
//!   structurally prevented now that actor threads are private to each
//!   BenchCore). The deferred S7 follow-on slice softens the panic
//!   message under `cfg(napi)` for that hypothetical case (D258).
//!
//! # Drop discipline
//!
//! - `Arc<CoreActor>::run<F, R>(f) -> impl Future<Output = R>` —
//!   awaits the closure's reply. Used by every napi method body.
//! - `Arc<CoreActor>::dispatch_detached<F>(f)` — fire-and-forget; used
//!   by `Drop` impls that can't `await`. The dropped reply channel is
//!   benign — the worker still runs the closure to completion.
//! - When `Arc<CoreActor>` strong count hits zero, the worker's
//!   `recv()` returns `Err(RecvError)`, the loop exits, the worker
//!   thread joins via the `JoinHandle` held by the last surviving
//!   actor field (Mutex-wrapped so `Drop` can take it). The worker's
//!   thread terminating drops `Core` on its own stack — the move-only
//!   `Core` never crosses a thread boundary.
//!
//! # Performance characteristics
//!
//! - **Cost per napi call:** one channel `send` + one `sync_channel`
//!   `recv` round-trip. With `crossbeam_channel::unbounded` + a
//!   parked worker, the wake-up latency is ~100ns on common hardware
//!   (better than tokio's blocking-pool dispatch, which involves
//!   pool-scheduler bookkeeping).
//! - **No contention.** The single-owner invariant means the inbox is
//!   the only contended primitive; under uncontended steady-state the
//!   worker thread spends most of its time in `recv()` and dispatches
//!   closures with no synchronization overhead.

use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

use crossbeam_channel::{unbounded, Sender};
use graphrefly_core::{BindingBoundary, Core};
use parking_lot::Mutex;

// ---------------------------------------------------------------------------
// Worker-thread-local extras registry (S6/D255).
//
// Some binding-side resources are `!Send` and CAN'T live on the napi class
// directly (which must be `Send + Sync`). Examples:
//
// - `graphrefly_graph::Graph` is `!Send + !Sync` (it stores
//   `Rc<RefCell<GraphInner>>`). Every Graph method takes `&Core`.
// - `graphrefly_structures::ReactiveLog<T>` was previously stored behind
//   `Arc<parking_lot::Mutex<...>>` in BenchReactiveLog; post-S2c this
//   still works (parking_lot Mutex<T: Send>), but the SubscriptionId
//   handling needs the actor anyway.
//
// To handle the `!Send` case (Graph), each binding allocates a unique
// `worker_key: u64` via [`alloc_worker_key`] and inserts its `!Send`
// resource into [`WORKER_EXTRAS`] inside an actor closure. Subsequent
// closures look the resource up by key. The thread_local lives on the
// actor's worker thread; access is safe because we're always inside an
// actor-dispatched closure (which by definition runs on that thread).
//
// Drop discipline: when the napi class drops (e.g. `BenchGraph`), it
// posts a `dispatch_detached` closure that removes the entry from
// `WORKER_EXTRAS`, dropping the `!Send` resource on the worker stack.
// ---------------------------------------------------------------------------

thread_local! {
    pub(crate) static WORKER_EXTRAS: RefCell<HashMap<u64, Box<dyn Any>>> =
        RefCell::new(HashMap::new());
}

static NEXT_WORKER_KEY: AtomicU64 = AtomicU64::new(1);

/// Allocate a fresh worker-extras key. Used by `BenchGraph` /
/// `BenchReactive*` constructors to identify their slot in
/// [`WORKER_EXTRAS`].
#[must_use]
pub(crate) fn alloc_worker_key() -> u64 {
    NEXT_WORKER_KEY.fetch_add(1, Ordering::Relaxed)
}

/// Insert (or replace) a `!Send` resource into the worker's extras
/// registry. Call only from inside a [`CoreActor::run`] /
/// [`CoreActor::run_sync`] closure.
///
/// ## INVARIANT — do NOT call from inside [`with_extra`] / [`with_extra_mut`]
///
/// `WORKER_EXTRAS` is a `thread_local!(RefCell<HashMap<…>>)`. `with_extra*`
/// hold a `Ref<…>` / `RefMut<…>` across the user closure; if that closure
/// recursively calls `install_extra` (or any other `cell.borrow*` path),
/// `std::cell::RefCell` panics with `BorrowError`/`BorrowMutError`. Use
/// [`with_extra_install_child`] for "read parent, write child" combos in
/// a single borrow.
pub(crate) fn install_extra<T: Any>(key: u64, value: T) {
    WORKER_EXTRAS.with(|cell| {
        cell.borrow_mut().insert(key, Box::new(value));
    });
}

/// Borrow a `!Send` resource from the worker's extras registry by key
/// and pass it to the given closure. Returns `None` if no entry exists
/// for `key` or if the entry's type doesn't match `T`. Call only from
/// inside a [`CoreActor::run`] / [`CoreActor::run_sync`] closure.
///
/// ## INVARIANT — the closure must NOT touch [`WORKER_EXTRAS`]
///
/// `with_extra` holds an immutable `Ref<HashMap<…>>` across `f(typed)`.
/// A recursive call to `install_extra` / `with_extra_mut` / `take_extra`
/// inside `f` panics with `BorrowError`/`BorrowMutError` from
/// `std::cell::RefCell`. Compile-time enforcement isn't possible
/// (`Box<dyn Any>` + thread_local defeats borrow tracking); structural
/// enforcement lives in [`with_extra_install_child`] for the common
/// "read parent, install child" pattern.
pub(crate) fn with_extra<T: Any, R>(key: u64, f: impl FnOnce(&T) -> R) -> Option<R> {
    WORKER_EXTRAS.with(|cell| {
        let map = cell.borrow();
        let any = map.get(&key)?;
        let typed = any.downcast_ref::<T>()?;
        Some(f(typed))
    })
}

/// "Read parent extra + install (infallibly-produced) child extra"
/// in a SINGLE `RefCell::borrow_mut` — avoids the nested-borrow
/// panic that [`with_extra`] + [`install_extra`] would trigger.
///
/// Used by `BenchGraph::describe_reactive`, `BenchGraph::observe_all_reactive`,
/// `BenchGraph::observe_subscribe`, and `bench_attach_snapshot_storage`.
/// For fallible factories (`mount_new`, where parent.mount_new can
/// return a `MountError`) use [`with_extra_try_install_child`].
///
/// Returns `None` if `parent_key` is missing or stored under a
/// different type than `P`. Returns `Some(out)` after installing the
/// child and propagating the closure's secondary return value.
///
/// ## INVARIANT — `f` must NOT touch `WORKER_EXTRAS`
///
/// `f(parent)` runs while a `RefMut<HashMap<…>>` is alive on this
/// thread. Any recursive `with_extra*`/`install_extra`/`take_extra`
/// call panics with `BorrowMutError` from `std::cell::RefCell`.
///
/// QA fix 2026-05-20 (Edge Case Hunter F2 + Blind Hunter M5):
/// the prior `with_extra(parent_key, |p| { ...; install_extra(child_key, c) })`
/// pattern panicked on first call.
pub(crate) fn with_extra_install_child<P, C, R, F>(
    parent_key: u64,
    child_key: u64,
    f: F,
) -> Option<R>
where
    P: Any,
    C: Any,
    F: FnOnce(&P) -> (C, R),
{
    WORKER_EXTRAS.with(|cell| {
        let mut map = cell.borrow_mut();
        let parent = map.get(&parent_key)?.downcast_ref::<P>()?;
        let (child, output) = f(parent);
        map.insert(child_key, Box::new(child));
        Some(output)
    })
}

/// Fallible variant of [`with_extra_install_child`] — the closure
/// returns `Result<(C, R), E>`. On `Ok`, install the child + return
/// `Ok(R)`. On `Err`, the child is NOT installed and the error
/// propagates as `Err(E)`. The outer `Option` distinguishes
/// "parent missing/wrong type" (`None`) from "factory failed"
/// (`Some(Err(e))`).
pub(crate) fn with_extra_try_install_child<P, C, R, E, F>(
    parent_key: u64,
    child_key: u64,
    f: F,
) -> Option<std::result::Result<R, E>>
where
    P: Any,
    C: Any,
    F: FnOnce(&P) -> std::result::Result<(C, R), E>,
{
    WORKER_EXTRAS.with(|cell| {
        let mut map = cell.borrow_mut();
        let parent = map.get(&parent_key)?.downcast_ref::<P>()?;
        match f(parent) {
            Ok((child, output)) => {
                map.insert(child_key, Box::new(child));
                Some(Ok(output))
            }
            Err(e) => Some(Err(e)),
        }
    })
}

/// Mutably borrow a `!Send` resource from the worker's extras
/// registry. Same shape as [`with_extra`] but for `&mut T`.
#[allow(dead_code)] // reserved for binding follow-on slices
pub(crate) fn with_extra_mut<T: Any, R>(key: u64, f: impl FnOnce(&mut T) -> R) -> Option<R> {
    WORKER_EXTRAS.with(|cell| {
        let mut map = cell.borrow_mut();
        let any = map.get_mut(&key)?;
        let typed = any.downcast_mut::<T>()?;
        Some(f(typed))
    })
}

/// Take a `!Send` resource out of the worker's extras registry by key,
/// returning ownership. Used by drop paths — the resource drops on the
/// worker stack after this returns to the closure.
///
/// **Panics** on type mismatch (Blind Hunter M3, QA 2026-05-20): a
/// `downcast::<T>` failure indicates a programming-error key collision
/// (two bindings minted the same key, or a binding used the wrong `T`).
/// The prior shape "put the entry back and return None" silently
/// leaked the resource AND let Drop paths swallow `let _ = take_extra(…)`
/// without observing the bug; converting to a panic makes the mismatch
/// fail-loud in the worker (caught by the M1 `catch_unwind` wrap; the
/// reply channel returns "worker panicked while executing closure" to
/// the JS caller).
pub(crate) fn take_extra<T: Any>(key: u64) -> Option<T> {
    WORKER_EXTRAS.with(|cell| {
        let any = cell.borrow_mut().remove(&key)?;
        match any.downcast::<T>() {
            Ok(boxed) => Some(*boxed),
            Err(_any_back) => panic!(
                "WORKER_EXTRAS key {key} type mismatch: caller asked for {} \
                 but the entry was a different type. Programming bug — \
                 likely two bindings minted the same key, or the wrong \
                 type parameter was passed to take_extra.",
                std::any::type_name::<T>()
            ),
        }
    })
}

/// Monotonic generation counter for `CoreActor` instances. Mirrors
/// `Core::generation`'s discipline but at the actor layer — useful for
/// diagnostics and for asserting actor identity across detached drop
/// paths.
static ACTOR_GENERATION: AtomicU64 = AtomicU64::new(1);

/// Type alias for the actor's inbox payload: a sync closure that
/// receives a borrow of the worker-owned `Core` and runs to completion.
/// `Send + 'static` because it crosses the channel; `FnOnce(&Core)`
/// because each closure runs exactly once with the worker's `&core`.
pub(crate) type CoreClosure = Box<dyn FnOnce(&Core) + Send + 'static>;

/// Owner of a `Core` instance, pinned to a dedicated worker thread.
///
/// Construct with [`CoreActor::spawn`]; clones via `Arc::clone`. When
/// the last `Arc<CoreActor>` drops, the worker thread exits cleanly
/// and `Core` is dropped on the worker's stack.
pub(crate) struct CoreActor {
    /// Closure inbox. `Arc<CoreActor>` clones each clone this `Sender`,
    /// so the worker's `recv` returns `Err` only when ALL handles drop
    /// (RAII actor shutdown).
    sender: Sender<CoreClosure>,
    /// Worker thread join handle. Taken on the last-actor drop (via
    /// `Mutex::lock().take()`) for orderly join. `Mutex` (not `Cell`)
    /// because `Arc::get_mut` is not generally callable on the last
    /// surviving Arc inside `Drop`.
    join: Mutex<Option<JoinHandle<()>>>,
    /// D293 (2026-05-25): explicit-shutdown flag, checked by the worker
    /// loop at the top of each iteration. Set to `true` exactly once by
    /// [`Self::shutdown`] (or by [`Drop`]'s sender-swap path via the
    /// channel close); the worker observes the flag (or `Err` from
    /// `recv()`), breaks the loop, drops `Core` on its stack, and
    /// exits. Wrapped in `Arc` because the worker holds its own clone.
    ///
    /// **Why a flag in addition to the existing sender-drop trigger?**
    /// The sender-drop trigger only fires when the LAST `Arc<CoreActor>`
    /// drops — JS-side GC of `BenchCore` is non-deterministic, so a
    /// short-lived napi consumer (test framework, CLI script, serverless
    /// cold-start) hangs the Node process indefinitely on the
    /// non-daemon worker thread (`std::thread::spawn` has no daemon
    /// concept on POSIX). The flag lets `BenchCore::close()` /
    /// `Symbol.asyncDispose` trigger the same orderly shutdown that
    /// `Drop` already performs, WITHOUT requiring the Arc to drop.
    /// Closes the cross-track-ledger §1 "process never exits" hazard
    /// for every napi consumer (D293 mint; v0.0.8).
    shutdown_flag: Arc<AtomicBool>,
    /// Unique per-actor identity. Diagnostic only.
    generation: u64,
}

impl CoreActor {
    /// Spawn a new actor that owns a fresh `Core` wired to the supplied
    /// binding. The binding `Arc<dyn BindingBoundary>` is `Send + Sync`
    /// (BindingBoundary requires both); `Core::new` runs inside the
    /// worker thread so the resulting `!Send` `Core` never crosses a
    /// thread boundary.
    #[must_use = "dropping the only Arc<CoreActor> immediately spawns then kills the worker"]
    pub(crate) fn spawn(binding: Arc<dyn BindingBoundary>) -> Arc<Self> {
        let (sender, receiver) = unbounded::<CoreClosure>();
        let generation = ACTOR_GENERATION.fetch_add(1, Ordering::Relaxed);
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let worker_flag = Arc::clone(&shutdown_flag);
        let join = std::thread::Builder::new()
            .name(format!("graphrefly-core-actor-{generation}"))
            .spawn(move || {
                // Core construction happens inside the worker — Core is
                // `!Send`, so it must be born on the thread that will
                // own it. After this point, `&core` is the only
                // reference the worker hands out (via the closures'
                // arguments).
                let core = Core::new(binding);
                // Each closure runs with a borrow of the worker-owned
                // Core. The producer-dispatch path threads `&dyn
                // CoreFull` through via
                // `BindingBoundary::invoke_fn_with_core` (D245/D246-r5
                // / D256 reconciled — no thread-local needed).
                //
                // Panic isolation (QA fix 2026-05-20, Blind Hunter M1):
                // `catch_unwind` around each closure so a JS-throw
                // (`bridge_sync` panics with "JS packer callback threw"),
                // an `.expect(...)` miss, or a `RefCell::borrow*`
                // violation doesn't terminate the worker. On panic, the
                // closure's reply channel (`tx`) was dropped during
                // unwind without `send`ing, so the `CoreActor::run`
                // caller's `rx.recv()` returns `Err` and maps to
                // "worker panicked while executing closure" — same
                // observable behavior at the napi surface, but the
                // BenchCore is NOT bricked: the next closure dispatches
                // cleanly. `AssertUnwindSafe` is sound because the
                // closure receives only a borrow (`&Core`); any
                // partially-mutated state inside Core is the
                // substrate's responsibility, and the substrate
                // already discards partially-applied waves via the
                // `BatchGuard` panic path (D065).
                //
                // D293 (2026-05-25): the `worker_flag` check at the
                // top of each iteration is the explicit-shutdown gate.
                // [`Self::shutdown`] sets the flag + sends a no-op
                // wake closure; the worker pops the wake, observes the
                // flag, breaks. Same exit path as the existing
                // channel-disconnect (recv() Err) path — `core` drops
                // on this stack, thread terminates, JoinHandle joins.
                while let Ok(closure) = receiver.recv() {
                    if worker_flag.load(Ordering::Acquire) {
                        break;
                    }
                    let _ =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| closure(&core)));
                }
                // Channel closed OR shutdown flag observed: all
                // `Arc<CoreActor>` handles dropped, OR an explicit
                // shutdown was triggered. `core` drops here, on the
                // worker's stack — single-owner invariant preserved.
            })
            .expect("CoreActor: failed to spawn worker thread");
        Arc::new(Self {
            sender,
            join: Mutex::new(Some(join)),
            shutdown_flag,
            generation,
        })
    }

    /// Construct an actor around an *existing* `Core`-construction
    /// closure. Used when the caller needs to wire additional state
    /// into the Core construction (e.g., installing a custom binding
    /// adapter). The closure runs on the freshly-spawned worker thread
    /// and must return the `Core` that the worker will own thereafter.
    ///
    /// Currently unused — `CoreActor::spawn` covers the BenchBinding
    /// case. Kept as the lower-level primitive in case a follow-on
    /// slice needs a custom Core construction path.
    #[allow(dead_code)]
    #[must_use = "dropping the only Arc<CoreActor> immediately spawns then kills the worker"]
    pub(crate) fn spawn_with<F>(make_core: F) -> Arc<Self>
    where
        F: FnOnce() -> Core + Send + 'static,
    {
        let (sender, receiver) = unbounded::<CoreClosure>();
        let generation = ACTOR_GENERATION.fetch_add(1, Ordering::Relaxed);
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let worker_flag = Arc::clone(&shutdown_flag);
        let join = std::thread::Builder::new()
            .name(format!("graphrefly-core-actor-{generation}"))
            .spawn(move || {
                let core = make_core();
                // Panic isolation (QA fix 2026-05-20, Blind Hunter M1)
                // + D293 (2026-05-25) shutdown-flag gate — see `spawn`
                // above for the rationale.
                while let Ok(closure) = receiver.recv() {
                    if worker_flag.load(Ordering::Acquire) {
                        break;
                    }
                    let _ =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| closure(&core)));
                }
            })
            .expect("CoreActor: failed to spawn worker thread");
        Arc::new(Self {
            sender,
            join: Mutex::new(Some(join)),
            shutdown_flag,
            generation,
        })
    }

    /// Send a closure to the worker and await its return value.
    ///
    /// Used by every napi `async fn` body that touches Core. The
    /// pattern at the call site is:
    ///
    /// ```ignore
    /// #[napi]
    /// pub async fn emit_int(&self, node_id: u32, value: i32) -> Result<()> {
    ///     let binding = Arc::clone(&self.binding);
    ///     self.actor.run(move |core| {
    ///         let handle = binding.registry.lock().intern(BenchValue::Int(value));
    ///         core.emit(NodeId::new(u64::from(node_id)), handle);
    ///     }).await
    /// }
    /// ```
    ///
    /// `R: Send + 'static` because the reply crosses the worker→caller
    /// channel. `F: FnOnce(&Core) -> R + Send + 'static` because the
    /// closure crosses the caller→worker channel.
    ///
    /// # D292 D.2 R2 — sync-channel waits MUST use `spawn_blocking`
    ///
    /// **Do not call `rx.recv()` (or any blocking sync op) inside an
    /// `actor.run` closure body.** D255 α-shape locks single-worker-
    /// per-Core; a blocked closure serializes every subsequent
    /// `actor.run` against the same Core until the block resolves —
    /// for a `rx.recv()` waiting on the parked-batch actor (which IS
    /// the Core's worker), that's an immediate self-deadlock. Use
    /// [`napi::bindgen_prelude::spawn_blocking`] for sync-channel
    /// waits (offloads to tokio's blocking pool — Core actor stays
    /// free) and `tokio::sync` primitives for async waits.
    ///
    /// Reference: D292 D.2 R2 refinement; pattern landed in
    /// `BenchBatchContext::commit` / `rollback` (`batch_bindings.rs`).
    ///
    /// # Errors
    ///
    /// Returns a `napi::Error` if (a) the worker thread has already
    /// dropped (`send` fails — actor shutdown raced with this call),
    /// or (b) the worker panicked while executing the closure (the
    /// reply channel closes without a value).
    pub(crate) async fn run<F, R>(self: &Arc<Self>, f: F) -> napi::Result<R>
    where
        F: FnOnce(&Core) -> R + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = std::sync::mpsc::sync_channel::<R>(1);
        let generation = self.generation;
        let closure: CoreClosure = Box::new(move |core: &Core| {
            let result = f(core);
            // If the receiver dropped (caller aborted), the send
            // returns Err — benign; the closure still completed.
            let _ = tx.send(result);
        });
        self.sender.send(closure).map_err(|_| {
            // D293 (2026-05-25): error parenthetical broadened from
            // "(actor shutdown raced with napi call)" to "(actor is
            // shut down or shutting down)" — covers both the original
            // race (Drop trigger) AND the new explicit-shutdown path
            // ([`Self::shutdown`]) introduced by D293. Both produce
            // the same observable behavior at this layer (channel
            // disconnect), so one message covers both.
            napi::Error::from_reason(format!(
                "CoreActor#{generation}: worker thread dropped before closure dispatch \
                 (actor is shut down or shutting down)"
            ))
        })?;
        // Bridge the sync `Receiver::recv()` into the async context via
        // napi's tokio runtime blocking-pool. The blocking pool is
        // tokio-managed but we're NOT routing Core onto it — only the
        // brief `recv()` wait. The actor's worker thread runs the
        // closure outside the tokio pool entirely.
        napi::bindgen_prelude::spawn_blocking(move || rx.recv())
            .await
            .map_err(|e| {
                napi::Error::from_reason(format!(
                    "CoreActor#{generation}: reply-channel waiter panicked: {e}"
                ))
            })?
            .map_err(|_| {
                napi::Error::from_reason(format!(
                    "CoreActor#{generation}: worker panicked while executing closure \
                     (reply channel closed without a value)"
                ))
            })
    }

    /// Synchronous closure dispatch + reply. Used by sync `#[napi]`
    /// methods (e.g. `BenchCore::describe_node`) that can't `.await` —
    /// they're called from JS as synchronous functions returning a
    /// value directly, not a `Promise`.
    ///
    /// **⚠ Deadlock surface.** This blocks the calling thread (which
    /// is typically the libuv JS thread) on the worker's reply. If the
    /// worker is currently servicing a long-running closure that fires
    /// a TSFN-backed JS callback via `bridge_sync` / `bridge_sync_unit`,
    /// the TSFN call needs libuv to pump microtasks — and libuv is
    /// blocked here on our `rx.recv()`. 3-way deadlock.
    ///
    /// **Safe-use contract:**
    /// - The closure MUST be **purely read-side** Core accessors (no
    ///   wave entry, no `subscribe` / `emit` / `register*` / etc.).
    /// - Documented at every call site whether the surrounding usage
    ///   pattern is guaranteed not to interleave with TSFN-active
    ///   waves.
    /// - If a future use case surfaces an interleave, switch the
    ///   `#[napi]` method to `async fn` and use [`Self::run`].
    ///
    /// Currently used only by `BenchCore::describe_node` (D207 — read
    /// projection over `kind_of` / `is_terminal` / `has_fired_once` /
    /// `deps_of` / `cache_of`) which the parity-tests + Option C
    /// wrapper use exclusively for between-mutation inspection.
    pub(crate) fn run_sync<F, R>(self: &Arc<Self>, f: F) -> napi::Result<R>
    where
        F: FnOnce(&Core) -> R + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = std::sync::mpsc::sync_channel::<R>(1);
        let generation = self.generation;
        let closure: CoreClosure = Box::new(move |core: &Core| {
            let result = f(core);
            let _ = tx.send(result);
        });
        self.sender.send(closure).map_err(|_| {
            // D293 (2026-05-25): broadened parenthetical — see
            // [`Self::run`] above for the same edit's rationale.
            napi::Error::from_reason(format!(
                "CoreActor#{generation}: worker thread dropped before sync closure dispatch \
                 (actor is shut down or shutting down)"
            ))
        })?;
        rx.recv().map_err(|_| {
            napi::Error::from_reason(format!(
                "CoreActor#{generation}: worker panicked while executing sync closure \
                 (reply channel closed without a value)"
            ))
        })
    }

    /// Fire-and-forget closure dispatch. Used by `Drop` impls that
    /// can't `await`. The reply channel is dropped immediately, so the
    /// worker's `tx.send` becomes a no-op — but the closure still runs
    /// to completion on the worker.
    ///
    /// `F: FnOnce(&Core) + Send + 'static` — no return value because
    /// nobody's listening.
    ///
    /// Returns `Err(())` if the worker has already shut down. Callers
    /// typically ignore — there's nothing useful to do if the actor is
    /// gone (the work would have been a cleanup operation, which is
    /// moot at that point).
    pub(crate) fn dispatch_detached<F>(self: &Arc<Self>, f: F) -> std::result::Result<(), ()>
    where
        F: FnOnce(&Core) + Send + 'static,
    {
        let closure: CoreClosure = Box::new(f);
        self.sender.send(closure).map_err(|_| ())
    }

    /// Diagnostic accessor — the per-actor generation counter. Mirrors
    /// `Core::generation`'s role for diagnostics but at the actor
    /// layer.
    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    /// D293 (2026-05-25): explicit, idempotent shutdown of the worker
    /// thread. Used by [`BenchCore::close`] to terminate the worker
    /// WITHOUT requiring the last `Arc<CoreActor>` to drop — companion
    /// classes (`BenchGraph`, `BenchOperators`, `BenchStorage*`,
    /// `BenchReactive*`) hold their own clones, so Arc-refcount-zero
    /// is non-deterministic and often "never" in short-lived consumers
    /// (test frameworks, CLI scripts, serverless cold-start). Without
    /// this, the non-daemon worker thread keeps the Node process alive
    /// indefinitely (cross-track-ledger §1 "process never exits"
    /// hazard).
    ///
    /// **Mechanism (deliberately mirrors `Drop`'s shape):**
    /// 1. Set the [`Self::shutdown_flag`] (swap-and-check makes this
    ///    idempotent — only the FIRST caller does the work; subsequent
    ///    callers return early).
    /// 2. Post a no-op wake closure so the worker, which is typically
    ///    parked in `receiver.recv()`, returns from `recv()` and
    ///    observes the flag at the top of its next iteration.
    /// 3. Take + join the `JoinHandle` so this method only returns
    ///    after the worker has actually exited (and `core` has dropped
    ///    on the worker's stack).
    ///
    /// **Post-shutdown behavior:**
    /// - Subsequent [`Self::run`] / [`Self::run_sync`] /
    ///   [`Self::dispatch_detached`] calls observe the receiver-drop
    ///   via `sender.send(...)` returning `SendError` →
    ///   `napi::Error::from_reason("CoreActor#N: worker thread dropped
    ///   before closure dispatch (actor is shut down or shutting
    ///   down)")`. The error type is plain `napi::Error`; the parity
    ///   `Impl` contract surfaces it as a JS-side `Error` rejection.
    /// - `Drop for CoreActor` is idempotent against a prior
    ///   `shutdown()` — the flag swap returns true, the join handle is
    ///   `None`, and the sender-swap-and-drop becomes a no-op
    ///   (channel already disconnected via the worker exit).
    ///
    /// # D292 WATCH items (out of scope for D293)
    /// - **Drop-running-join on the libuv thread.** Today `Drop` (and
    ///   therefore this `shutdown`) calls `handle.join()`
    ///   synchronously. If `BenchCore` is GC'd on the libuv JS thread,
    ///   `Drop for CoreActor` blocks the event loop until the worker's
    ///   current closure completes. D292's lifecycle design session
    ///   should consider async-shutdown-from-finalizer (e.g.,
    ///   `FinalizationRegistry`-driven `napi::bindgen_prelude::spawn`
    ///   of the join).
    /// - **Idempotency race on join.** If TWO callers race
    ///   `shutdown()`, the second observes `swap → true` and returns
    ///   IMMEDIATELY — even though the first caller's `handle.join()`
    ///   may still be in flight. For BenchCore's call pattern this is
    ///   fine (only one BenchCore instance drives shutdown), but a
    ///   future caller wanting "wait for shutdown to fully complete"
    ///   would need a separate `Condvar` / one-shot signal. Tracked
    ///   for D292.
    pub(crate) fn shutdown(&self) {
        // Idempotency check: first caller advances the flag false →
        // true and does the work; subsequent callers observe the prior
        // value as true and return early. `AcqRel` ordering pairs with
        // the worker's `Acquire` load + the join's release semantics.
        if self.shutdown_flag.swap(true, Ordering::AcqRel) {
            return;
        }
        // Wake the worker out of `receiver.recv()` so it observes the
        // flag at the top of the next loop iteration and exits. The
        // no-op closure does nothing if the worker happens to be
        // executing a closure right now (the post is buffered; worker
        // pops it after the current closure returns, checks the flag,
        // breaks). If the send itself fails (worker already exited via
        // some other path), the join below still completes — the wake
        // is a best-effort optimization, not a correctness requirement.
        let _ = self.sender.send(Box::new(|_| {}));
        // Take + join the worker. This is what makes `shutdown()` a
        // synchronous "the thread is dead and `core` has dropped"
        // promise. `parking_lot::Mutex` is non-poisoning so `lock().
        // take()` is safe even if a prior `shutdown()` panicked.
        if let Some(handle) = self.join.lock().take() {
            // Best-effort: a thread-level panic indicates a Rust bug
            // (every closure is wrapped in `catch_unwind` per the
            // worker loop above), not a misuse. Surfacing it here
            // would force callers to handle a panic during a cleanup
            // path — unhelpful.
            let _ = handle.join();
        }
    }
}

impl Drop for CoreActor {
    fn drop(&mut self) {
        // D293 (2026-05-25): delegate to the explicit `shutdown()`
        // path. `shutdown()` is idempotent — if `BenchCore::close()`
        // already ran it, this is a no-op (the swap returns true, the
        // JoinHandle is None). If it didn't, this `Drop` is the only
        // shutdown trigger (the pre-D293 behavior). Either way the
        // worker exits cleanly and `core` drops on the worker's stack.
        //
        // **Why ALSO drop the sender via the swap-and-drop pattern?**
        // The `shutdown()` flag + wake-closure mechanism is the
        // primary trigger, but a defensive sender-drop guarantees the
        // worker exits even if (a) the wake-closure send fails
        // somehow, or (b) a future refactor moves the flag-check
        // somewhere the worker can't observe. The sender-swap pattern
        // (replace with a throwaway, then let the old sender drop)
        // avoids the field-destructor-order trap that would otherwise
        // join BEFORE the channel closes.
        //
        // # D292 WATCH — Drop-running-join on libuv thread
        // If `BenchCore` is GC'd on the libuv JS thread (a real path
        // — napi-rs invokes the `napi_class` finalizer on libuv when
        // the JS object is collected), this `Drop` blocks the JS
        // event loop on `handle.join()` until the worker's current
        // closure completes. For long-running closures (a stuck JS
        // packer callback inside `bridge_sync`, a deep wave drain)
        // this surfaces as a frozen UI / unresponsive Node process
        // — visually indistinguishable from the very hang D293 is
        // trying to fix. D292's lifecycle design session should lock
        // an async-shutdown-from-finalizer pattern (e.g., FRegistry
        // → `napi::bindgen_prelude::spawn`) so finalizers post the
        // shutdown work off-thread.
        self.shutdown();
        let (throwaway, _) = unbounded::<CoreClosure>();
        drop(std::mem::replace(&mut self.sender, throwaway));
        // `shutdown()` already took + joined the handle. If a future
        // refactor changes that, this defensive take-and-join is the
        // safety net. Today this is a guaranteed no-op (None).
        if let Some(handle) = self.join.lock().take() {
            let _ = handle.join();
        }
    }
}

// `CoreActor` MUST be Send + Sync — it's stored inside `Arc<...>` on
// the `Bench*` napi wrappers, which napi-rs requires `Send + Sync` for
// (the JS object can be referenced from any tokio worker). All fields
// satisfy: `crossbeam_channel::Sender<T> where T: Send` is Send + Sync;
// `parking_lot::Mutex<Option<JoinHandle<()>>>` is Send + Sync;
// `Arc<AtomicBool>` is Send + Sync (D293, 2026-05-25 — the
// shutdown_flag is shared with the worker thread); `u64` is Send +
// Sync. The auto-derived bounds hold; no manual impl needed.
// Compile-time check below:
const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<CoreActor>();
};
