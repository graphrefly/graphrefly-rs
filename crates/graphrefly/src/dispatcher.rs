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
//! Slice scope: the LocalSync pool only. LocalAsync, the grouped `DispatcherOpts`
//! (DR-6), and the opt-in profile recorder (D39) land in later slices.

use std::cell::RefCell;
use std::rc::Rc;

use crate::ctx::Ctx;
pub use crate::protocol::Handle;

/// A node fn: `(&Ctx) -> ()` (R-fn-contract / D8). Single-thread (D22) ⇒ no
/// `Send` bound. Stored as `Rc` so [`Dispatcher::invoke`] can clone it out of the
/// pool and **release the pool borrow before calling it** — a fn that re-drives a
/// downstream node (nested `invoke`) must not find the pool already borrowed.
pub type NodeFn = Rc<dyn Fn(&Ctx)>;

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
    slots: Vec<Slot>,
    /// Recycled (freed) slot ids, reused on the next register.
    free: Vec<u32>,
}

impl Pool {
    fn new() -> Self {
        Self {
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
    /// 1.0 ships LocalSync + LocalAsync (D20); this slice is sync-only — pool 0.
    sync: Pool,
}

/// First-class dispatcher (D21), cloneable handle over shared inner state. A graph
/// binds one; the default is the process(thread)-global singleton (D26).
#[derive(Clone)]
pub struct Dispatcher(Rc<RefCell<DispatcherInner>>);

/// The sync pool id (D20). The async pool id arrives with the LocalAsync slice.
pub const SYNC_POOL_ID: u32 = 0;

impl Dispatcher {
    pub fn new() -> Self {
        Dispatcher(Rc::new(RefCell::new(DispatcherInner { sync: Pool::new() })))
    }

    /// Register a fn in the sync pool, returning its [`Handle`] (R-dispatch-all).
    pub fn register(&self, f: NodeFn) -> Handle {
        let (handle_id, generation) = self.0.borrow_mut().sync.register(f);
        Handle {
            pool_id: SYNC_POOL_ID,
            handle_id,
            generation,
        }
    }

    /// Free a node's fn slot (B32) — called from `NodeInner::Drop`. The pool stops
    /// holding a dropped node's fn (and thus its captured upstream `Core`s) alive for
    /// the whole process. Idempotent / generation-checked: a stale unregister is a no-op.
    pub fn unregister(&self, handle: Handle) {
        debug_assert_eq!(
            handle.pool_id, SYNC_POOL_ID,
            "only the sync pool exists in this slice"
        );
        self.0
            .borrow_mut()
            .sync
            .unregister(handle.handle_id, handle.generation);
    }

    /// Uniform sync-void invoke (R-sync-core / R-dispatch-all). Clones the fn out
    /// and drops the pool borrow before calling, so a nested downstream invoke
    /// triggered from inside the fn does not double-borrow the pool.
    pub fn invoke(&self, handle: Handle, ctx: &Ctx) {
        debug_assert_eq!(
            handle.pool_id, SYNC_POOL_ID,
            "only the sync pool exists in this slice"
        );
        let f = self
            .0
            .borrow()
            .sync
            .get(handle.handle_id, handle.generation);
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
