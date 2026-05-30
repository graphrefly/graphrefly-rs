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

/// A dispatch pool: a flat fn table addressed by `handle_id`.
struct Pool {
    fns: Vec<NodeFn>,
}

impl Pool {
    fn new() -> Self {
        Self { fns: Vec::new() }
    }
    fn register(&mut self, f: NodeFn) -> u32 {
        let id = self.fns.len() as u32;
        self.fns.push(f);
        id
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
        let handle_id = self.0.borrow_mut().sync.register(f);
        Handle {
            pool_id: SYNC_POOL_ID,
            handle_id,
        }
    }

    /// Uniform sync-void invoke (R-sync-core / R-dispatch-all). Clones the fn out
    /// and drops the pool borrow before calling, so a nested downstream invoke
    /// triggered from inside the fn does not double-borrow the pool.
    pub fn invoke(&self, handle: Handle, ctx: &Ctx) {
        debug_assert_eq!(
            handle.pool_id, SYNC_POOL_ID,
            "only the sync pool exists in this slice"
        );
        let f = self.0.borrow().sync.fns[handle.handle_id as usize].clone();
        f(ctx);
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
