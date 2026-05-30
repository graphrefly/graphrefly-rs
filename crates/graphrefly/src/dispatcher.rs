//! The dispatcher — first-class invoke funnel + pool host (D20 / D21).
//!
//! **F-SYNC-CORE:** [`Dispatcher::invoke`] is synchronous and returns `()`. The
//! wave-protocol core never blocks on async; async lives only in pools
//! (LocalAsync, a later slice) and the wire bridge.
//!
//! **F-DISPATCH-ALL:** every node fn goes through the dispatcher — no inline-fn
//! bypass. A fn is registered into a pool and addressed by a pure-data [`Handle`]
//! `(pool_id, handle_id)` (D7).
//!
//! Slice scope: LocalSync + LocalAsync pools (D20). The async pool's `invoke` is
//! ALSO sync void (R-sync-core) — "async" is a node-kind LABEL, not a different
//! call mechanism: an async fn kicks off work and emits LATER via a stashed
//! [`crate::ctx::DeferredCtx`] (the Rust analogue of TS capturing `ctx`). The
//! grouped `DispatcherOpts` (DR-6) and the opt-in profile recorder (D39) land in
//! later slices.

use std::cell::RefCell;
use std::rc::Rc;

use crate::ctx::Ctx;
pub use crate::protocol::Handle;

/// A node fn: `(&Ctx) -> ()` (R-fn-contract / D8). Single-thread (D22) ⇒ no
/// `Send` bound. Stored as `Rc` so [`Dispatcher::invoke`] can clone it out of the
/// pool and **release the pool borrow before calling it** — a fn that re-drives a
/// downstream node (nested `invoke`) must not find the pool already borrowed.
pub type NodeFn = Rc<dyn Fn(&Ctx)>;

/// Pool kind label. LocalSync + LocalAsync ship in 1.0 (D20). The kind drives the
/// NODE'S behavior (async-paused buffering / per-invocation ctx snapshot — see
/// [`crate::node`]); the dispatcher invoke is sync void either way (R-sync-core).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PoolKind {
    /// LocalSync — the fn runs to completion synchronously.
    #[default]
    Sync,
    /// LocalAsync — the fn kicks off async work and emits later via a stashed
    /// [`crate::ctx::DeferredCtx`] (the emit serializes back onto the single thread).
    Async,
}

/// One slotmap entry: a fn (`None` once freed) + a generation bumped on each free,
/// so a recycled slot is distinguishable from a stale handle (B32).
struct Slot {
    f: Option<NodeFn>,
    generation: u32,
}

/// A dispatch pool: a **slotmap** of fns addressed by `(handle_id, generation)`.
///
/// B32: a node's fn is freed (slot recycled) when the node drops, so the pool no
/// longer holds a dropped node's fn — and thus its captured upstream handles —
/// alive for the whole process (the no-GC leak; `Node: Clone` made capturing
/// handles-into-fns idiomatic). Without this, every registered fn leaked, pinning
/// its captured `Core`s forever.
struct Pool {
    kind: PoolKind,
    slots: Vec<Slot>,
    /// Recycled (freed) slot ids, reused on the next register.
    free: Vec<u32>,
}

impl Pool {
    fn new(kind: PoolKind) -> Self {
        Self {
            kind,
            slots: Vec::new(),
            free: Vec::new(),
        }
    }

    /// Register a fn, returning `(handle_id, generation)`. Reuses a freed slot if one
    /// is available (keeping the slot vec bounded under churn).
    fn register(&mut self, f: NodeFn) -> (u32, u32) {
        if let Some(id) = self.free.pop() {
            let slot = &mut self.slots[id as usize];
            slot.f = Some(f);
            (id, slot.generation)
        } else {
            let id = self.slots.len() as u32;
            self.slots.push(Slot {
                f: Some(f),
                generation: 0,
            });
            (id, 0)
        }
    }

    /// Free a slot (drop its fn) + bump its generation + recycle it. A stale
    /// unregister (generation mismatch, or already free) is a no-op.
    fn unregister(&mut self, id: u32, generation: u32) {
        if let Some(slot) = self.slots.get_mut(id as usize) {
            if slot.generation == generation && slot.f.is_some() {
                slot.f = None;
                slot.generation = slot.generation.wrapping_add(1);
                self.free.push(id);
            }
        }
    }

    /// Clone out the fn for a handle, iff the generation matches and the slot is live.
    /// A stale handle (mismatched generation / freed slot) yields `None`.
    fn get(&self, id: u32, generation: u32) -> Option<NodeFn> {
        self.slots
            .get(id as usize)
            .filter(|s| s.generation == generation)
            .and_then(|s| s.f.clone())
    }
}

struct DispatcherInner {
    /// 1.0 ships LocalSync + LocalAsync (D20), indexed by `pool_id`:
    /// `pools[SYNC_POOL_ID]` = sync, `pools[ASYNC_POOL_ID]` = async. The trait stays
    /// pluggable for WorkerPool/RemotePool (D20) — a `Vec` so a new pool just appends.
    pools: Vec<Pool>,
}

/// First-class dispatcher (D21), cloneable handle over shared inner state. A graph
/// binds one; the default is the process(thread)-global singleton (D26).
#[derive(Clone)]
pub struct Dispatcher(Rc<RefCell<DispatcherInner>>);

/// The LocalSync pool id (D20).
pub const SYNC_POOL_ID: u32 = 0;
/// The LocalAsync pool id (D20).
pub const ASYNC_POOL_ID: u32 = 1;

impl Dispatcher {
    pub fn new() -> Self {
        Dispatcher(Rc::new(RefCell::new(DispatcherInner {
            // index order MUST match SYNC_POOL_ID / ASYNC_POOL_ID.
            pools: vec![Pool::new(PoolKind::Sync), Pool::new(PoolKind::Async)],
        })))
    }

    /// Register a fn in the sync pool, returning its [`Handle`] (R-dispatch-all).
    pub fn register(&self, f: NodeFn) -> Handle {
        self.register_in(SYNC_POOL_ID, f)
    }

    /// Register a fn in the LocalAsync pool (D20). The invoke is still sync void
    /// (R-sync-core) — the node's async-pool kind drives the deferred-emit / pause
    /// buffering behavior, not the call mechanism.
    pub fn register_async(&self, f: NodeFn) -> Handle {
        self.register_in(ASYNC_POOL_ID, f)
    }

    fn register_in(&self, pool_id: u32, f: NodeFn) -> Handle {
        let (handle_id, generation) = self.0.borrow_mut().pools[pool_id as usize].register(f);
        Handle {
            pool_id,
            handle_id,
            generation,
        }
    }

    /// The kind of a pool by id — the node reads this to decide async-paused
    /// buffering + per-invocation ctx snapshotting (R-pause-modes / R-async-paused).
    pub fn pool_kind(&self, pool_id: u32) -> PoolKind {
        self.0.borrow().pools[pool_id as usize].kind
    }

    /// Free a node's fn slot (B32) — called from `NodeInner::Drop`. The pool stops
    /// holding a dropped node's fn (and thus its captured upstream `Core`s) alive for
    /// the whole process. Idempotent / generation-checked: a stale unregister is a no-op.
    pub fn unregister(&self, handle: Handle) {
        self.0.borrow_mut().pools[handle.pool_id as usize]
            .unregister(handle.handle_id, handle.generation);
    }

    /// Uniform sync-void invoke (R-sync-core / R-dispatch-all). Clones the fn out
    /// and drops the pool borrow before calling, so a nested downstream invoke
    /// triggered from inside the fn does not double-borrow the pool.
    pub fn invoke(&self, handle: Handle, ctx: &Ctx) {
        let f =
            self.0.borrow().pools[handle.pool_id as usize].get(handle.handle_id, handle.generation);
        // A live node is the SOLE invoker of its own handle, so the slot is present on
        // the live path. A generation mismatch (a stale handle — e.g. a future
        // async-pool deferred callback firing after the node dropped + the slot was
        // recycled) yields None → a silent no-op, never a recycled-slot misfire.
        if let Some(f) = f {
            f(ctx);
        }
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
