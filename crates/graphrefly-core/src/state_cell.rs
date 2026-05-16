//! The `StateCell` seam — the single-threaded / cross-thread interior-mutability
//! split that makes the §7 "single-threaded substrate + `Option<LockId>`"
//! contract expressible without `unsafe` (D208, Option A).
//!
//! The [`Core`](crate::Core) dispatcher is generic over `C: StateCell`. The
//! seam has exactly two monomorphizations:
//!
//! - [`SingleThreadCell`] — `RefCell<CoreState>`. **`!Sync`**, single-threaded,
//!   lock-free. This is the §7 *floor* path: `RefCell::borrow_mut` lowers to a
//!   non-atomic borrow-flag check the optimizer largely elides, and — because
//!   no node carries a `SerializationGroupId` on this path by construction —
//!   the wave engine's group-collect + ordered-acquire step iterates an empty
//!   set and monomorphizes away entirely. ~83 ns/emit; ~7.6× faster than the
//!   pre-rewrite production dispatcher.
//! - [`LockedCell`] — `parking_lot::Mutex<CoreState>`. **`Send + Sync`**. The
//!   default exported `Core` type parameter (so `graphrefly-graph`,
//!   `graphrefly-operators`, `graphrefly-storage`, `graphrefly-structures` and
//!   the napi bindings keep compiling unchanged — D211), and the substrate for
//!   cross-thread serialization: nodes that share a `SerializationGroupId`
//!   serialize through a per-group `Arc<ReentrantMutex<()>>` acquired in
//!   sorted order at wave entry.
//!
//! Both impls are 100% safe (`RefCell` / `parking_lot` wrappers); the crate's
//! `#![forbid(unsafe_code)]` is preserved.
//!
//! The seam is a GAT-based guard rather than a closure callback so the
//! existing `let mut st = self.lock_state(); … drop(st); …` access pattern
//! (~50 sites across `node.rs` / `batch.rs`, several with mid-fn lock-release
//! for re-entrant binding callbacks) ports unchanged: both guard types impl
//! `DerefMut<Target = CoreState>`.

use std::cell::{RefCell, RefMut};
use std::ops::DerefMut;

use crate::node::CoreState;

/// Interior-mutability strategy for [`CoreState`].
///
/// See the module docs. Used only via generics/monomorphization — never as a
/// trait object — so the chosen strategy's acquire cost is inlined and, for
/// [`SingleThreadCell`], elided.
pub trait StateCell: 'static {
    /// Whether waves on this substrate need cross-thread serialization.
    ///
    /// `false` for [`SingleThreadCell`] (the lock-free floor — `!Send`+`!Sync`
    /// by construction, so cross-thread sharing is compile-impossible and a
    /// per-wave lock would be pure overhead; the branch dead-code-eliminates
    /// at monomorphization, preserving the §7 ~83 ns floor).
    ///
    /// `true` for [`LockedCell`] (`Send + Sync`, the shared default). When a
    /// wave's transitively-touched `SerializationGroupId` set is **empty**
    /// (an all-`None` cascade), the per-thread `WAVE_STATE` /
    /// `TIER3_EMITTED_THIS_WAVE` / `IN_TICK_OWNED` invariants — which assume
    /// "cross-thread emits BLOCK on a wave-lock" (`batch.rs`) — would
    /// otherwise be unprotected. The engine acquires a single Core-global
    /// wave `ReentrantMutex` for that case (QA F1), restoring the
    /// pre-rewrite "every wave serializes per Core" safety floor that the
    /// deleted union-find `wave_owner` used to provide. Grouped waves still
    /// acquire only their disjoint group locks (parallelism preserved).
    const SERIALIZE_WAVES: bool;

    /// The RAII guard yielded by [`StateCell::lock`]. `DerefMut` to
    /// [`CoreState`] so call sites are strategy-agnostic.
    type Guard<'a>: DerefMut<Target = CoreState>
    where
        Self: 'a;

    /// Wrap freshly-constructed [`CoreState`] in this cell.
    #[must_use]
    fn from_state(state: CoreState) -> Self;

    /// Acquire mutable access. For [`SingleThreadCell`] this is a
    /// non-atomic borrow; for [`LockedCell`] a `parking_lot::Mutex::lock`.
    #[must_use]
    fn lock(&self) -> Self::Guard<'_>;
}

/// Single-threaded, lock-free cell — the §7 floor path. **`!Sync`.**
///
/// Used by `Core<SingleThreadCell>` (the `minimal_handle_core` bench and the
/// floor regression tests). All nodes on this path have
/// `serialization_group == None` by construction; the wave engine's
/// group-acquisition machinery monomorphizes out.
pub struct SingleThreadCell(RefCell<CoreState>);

impl StateCell for SingleThreadCell {
    const SERIALIZE_WAVES: bool = false;

    type Guard<'a> = RefMut<'a, CoreState>;

    #[inline]
    fn from_state(state: CoreState) -> Self {
        Self(RefCell::new(state))
    }

    #[inline]
    fn lock(&self) -> RefMut<'_, CoreState> {
        // `borrow_mut` (not `try_borrow_mut`): a re-entrant double-borrow is a
        // dispatcher bug (lock-released bracket missing around a binding
        // callback), not a user error — panic is the correct, loud failure,
        // same observable contract as the `LockedCell` re-entrant-deadlock
        // path surfacing as a hang in tests.
        self.0.borrow_mut()
    }
}

/// Cross-thread serialization-capable cell. **`Send + Sync`.**
///
/// The default `Core` type parameter (D211) and the substrate for
/// `SerializationGroupId` cross-thread serialization. Uses
/// `parking_lot::Mutex` (no poisoning — matches the pre-rewrite
/// `Arc<parking_lot::Mutex<CoreState>>` shape).
pub struct LockedCell(parking_lot::Mutex<CoreState>);

impl StateCell for LockedCell {
    const SERIALIZE_WAVES: bool = true;

    type Guard<'a> = parking_lot::MutexGuard<'a, CoreState>;

    #[inline]
    fn from_state(state: CoreState) -> Self {
        Self(parking_lot::Mutex::new(state))
    }

    #[inline]
    fn lock(&self) -> parking_lot::MutexGuard<'_, CoreState> {
        self.0.lock()
    }
}
