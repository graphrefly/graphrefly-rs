//! The `StateCell` seam — reshaped for Slice B-2 Step 2 (D220-EXEC) into
//! **two independently-lockable regions**: a single Core-global
//! [`CoreShared`] + a per-[`ShardKey`] [`CoreState`].
//!
//! Step 2a (this stage) keeps exactly ONE shard (`key` ignored —
//! `DEFAULT_SHARD`); [`Core::lock_state`](crate::Core) returns a *combined*
//! guard that holds the default-shard guard + the shared guard together
//! (shard OUTER, shared INNER — the D220-EXEC deadlock-free order), so the
//! ~104 existing `let mut s = self.lock_state(); … s.nodes … s.shared.x …`
//! call sites port **verbatim** (the combined guard `DerefMut`s to
//! `CoreState` for `nodes`/`children`/`binding` and exposes an inherent
//! `shared` field for `s.shared.<f>`). Behaviour-identical for the all-`None`
//! suite (one shard). Step 2b turns the single shard into a per-`ShardKey`
//! `Arc<Mutex<CoreState>>` map and routes the wave hot path by
//! `group_of(node)` for true disjoint-group parallelism.
//!
//! [`Core`](crate::Core) is generic over `C: StateCell`. Two
//! monomorphizations:
//!
//! - [`SingleThreadCell`] — `RefCell` regions. **`!Sync`**, lock-free; the
//!   §7 ≈515 ns floor path (one shard by construction; the group machinery
//!   monomorphizes away).
//! - [`LockedCell`] — `parking_lot::Mutex` regions. **`Send + Sync`**; the
//!   default `Core` type parameter (D211) and the cross-thread substrate.
//!
//! Both impls are 100% safe; `#![forbid(unsafe_code)]` is preserved.
//!
//! **Lock-ordering rule (deadlock-free; D220-EXEC):** when both regions are
//! needed, acquire **shard guard OUTER, shared guard INNER**, never
//! reversed. The combined `lock_state()` guard encodes exactly that order;
//! pure-shared ops use [`StateCell::lock_shared`] only.

use crate::node::{CoreShared, CoreState};

/// The shard selector. `None` = `DEFAULT_SHARD` (the all-`None` §7 floor;
/// in Step 2a the *only* shard). Step 2b keys a per-shard `CoreState` map
/// by this so disjoint [`crate::handle::SerializationGroupId`]s hold
/// disjoint mutexes (true parallelism).
pub type ShardKey = Option<crate::handle::SerializationGroupId>;

/// Interior-mutability strategy for the two state regions
/// ([`CoreShared`] + per-[`ShardKey`] [`CoreState`]).
///
/// Used only via generics/monomorphization — never as a trait object — so
/// the acquire cost is inlined and, for [`SingleThreadCell`], elided.
pub trait StateCell: 'static {
    /// Whether waves on this substrate need cross-thread serialization.
    /// `false` for [`SingleThreadCell`] (the lock-free floor); `true` for
    /// [`LockedCell`] (the QA-F1 all-`None` global wave lock — vestigial,
    /// folds into B-3's §7-C deletion once Step 2b's shard routing
    /// replaces it).
    const SERIALIZE_WAVES: bool;

    /// RAII guard over the Core-global [`CoreShared`] region.
    type SharedGuard<'a>: std::ops::DerefMut<Target = CoreShared>
    where
        Self: 'a;

    /// RAII guard over a shard's [`CoreState`] region.
    type ShardGuard<'a>: std::ops::DerefMut<Target = CoreState>
    where
        Self: 'a;

    /// Wrap a freshly-constructed `CoreShared` + the default-shard
    /// `CoreState` in this cell. (Step 2a: the single shard.)
    #[must_use]
    fn from_parts(shared: CoreShared, default_shard: CoreState) -> Self;

    /// Acquire the Core-global [`CoreShared`] region.
    #[must_use]
    fn lock_shared(&self) -> Self::SharedGuard<'_>;

    /// Acquire `key`'s shard [`CoreState`] region. Step 2a: `key` is
    /// ignored — exactly one shard (behaviour-identical with the pre-B-2
    /// single `CoreState`). Step 2b routes by `key`.
    #[must_use]
    fn lock_shard(&self, key: ShardKey) -> Self::ShardGuard<'_>;
}

/// Single-threaded, lock-free cell — the §7 floor path. **`!Sync`.**
///
/// `RefCell` regions; `key` ignored (one shard by construction). A
/// re-entrant double-borrow panics loudly — a dispatcher bug (a missing
/// lock-released bracket around a binding callback), not a user error;
/// same observable contract as the `LockedCell` re-entrant-deadlock path.
pub struct SingleThreadCell {
    shared: std::cell::RefCell<CoreShared>,
    shard: std::cell::RefCell<CoreState>,
}

impl StateCell for SingleThreadCell {
    const SERIALIZE_WAVES: bool = false;

    type SharedGuard<'a> = std::cell::RefMut<'a, CoreShared>;
    type ShardGuard<'a> = std::cell::RefMut<'a, CoreState>;

    #[inline]
    fn from_parts(shared: CoreShared, default_shard: CoreState) -> Self {
        Self {
            shared: std::cell::RefCell::new(shared),
            shard: std::cell::RefCell::new(default_shard),
        }
    }

    #[inline]
    fn lock_shared(&self) -> std::cell::RefMut<'_, CoreShared> {
        self.shared.borrow_mut()
    }

    #[inline]
    fn lock_shard(&self, _key: ShardKey) -> std::cell::RefMut<'_, CoreState> {
        self.shard.borrow_mut()
    }
}

/// Cross-thread serialization-capable cell. **`Send + Sync`.**
///
/// The default `Core` type parameter (D211). `parking_lot::Mutex` regions
/// (no poisoning — matches the pre-rewrite shape). Step 2a holds ONE shard
/// mutex; Step 2b replaces `shard` with a per-`ShardKey`
/// `Mutex<HashMap<ShardKey, Arc<Mutex<CoreState>>>>` + `lock_arc` owned
/// guards.
pub struct LockedCell {
    shared: parking_lot::Mutex<CoreShared>,
    /// `DEFAULT_SHARD` (`ShardKey::None`) held as an explicit field —
    /// NOT in the map — so the all-`None` hot path
    /// (`lock_shard(None)`, the floor + every cold-path `lock_state`)
    /// is a single `lock_arc` with **zero map lock / zero hash / zero
    /// `or_insert`** (D220-EXEC: "one default shard = zero
    /// regression"; preserves the ≈515 ns `floor_compare` gate). The
    /// `Arc` is the `lock_arc` carrier for the owned `'static` guard.
    default_shard: ShardArc,
    /// Per-`SerializationGroupId` shards (`ShardKey::Some` only —
    /// `None` never enters this map). Each is its OWN
    /// `Arc<parking_lot::Mutex<CoreState>>` so disjoint groups hold
    /// disjoint mutexes (the parallelism the D216 bench found
    /// missing). The map mutex is held only to resolve/insert the
    /// shard's `Arc`, then dropped before the shard is locked, so it
    /// never serializes wave execution.
    ///
    /// **Step 2b-i (this stage): behaviour-identical** — every caller
    /// still routes `key = None` (via `lock_state`/`St`) → only
    /// `default_shard` is ever touched, this map stays empty. Step
    /// 2b-ii routes registration + the wave hot path by
    /// `group_of(node)` so grouped nodes populate it.
    grouped_shards: parking_lot::Mutex<
        std::collections::HashMap<crate::handle::SerializationGroupId, ShardArc>,
    >,
    /// Binding clone used to construct a fresh empty `CoreState` when a
    /// new group shard is first touched (its `Drop for CoreState`
    /// node-retain walk needs a binding).
    binding: std::sync::Arc<dyn crate::boundary::BindingBoundary>,
}

/// One shard: an independently-lockable `CoreState`. The `Arc` lets
/// `lock_shard` clone-out + `lock_arc` an **owned** (`'static`) guard,
/// so the guard's lifetime is NOT tied to the brief map-lock borrow
/// (the D220-EXEC "Guard-lifetime crux" — solved with `parking_lot`'s
/// `arc_lock` feature, zero `unsafe`).
type ShardArc = std::sync::Arc<parking_lot::Mutex<CoreState>>;

impl StateCell for LockedCell {
    const SERIALIZE_WAVES: bool = true;

    type SharedGuard<'a> = parking_lot::MutexGuard<'a, CoreShared>;
    /// Owned `'static` guard (does NOT borrow the cell) — `lock_arc`
    /// over the shard's `Arc<Mutex<CoreState>>`. The GAT `'a` is
    /// unused (the owned guard trivially satisfies `where Self: 'a`).
    type ShardGuard<'a> = parking_lot::ArcMutexGuard<parking_lot::RawMutex, CoreState>;

    #[inline]
    fn from_parts(shared: CoreShared, default_shard: CoreState) -> Self {
        let binding = default_shard.binding.clone();
        Self {
            shared: parking_lot::Mutex::new(shared),
            default_shard: std::sync::Arc::new(parking_lot::Mutex::new(default_shard)),
            grouped_shards: parking_lot::Mutex::new(std::collections::HashMap::new()),
            binding,
        }
    }

    #[inline]
    fn lock_shared(&self) -> parking_lot::MutexGuard<'_, CoreShared> {
        self.shared.lock()
    }

    #[inline]
    fn lock_shard(
        &self,
        key: ShardKey,
    ) -> parking_lot::ArcMutexGuard<parking_lot::RawMutex, CoreState> {
        match key {
            // Floor / cold-path fast path: ZERO map lock, ZERO hash —
            // a single `lock_arc` on the explicit default-shard `Arc`
            // (D220-EXEC zero-regression invariant).
            None => parking_lot::Mutex::lock_arc(&self.default_shard),
            // Grouped shard: hold the map mutex ONLY long enough to
            // resolve/insert the shard's `Arc`, then drop it before
            // locking the shard so the map never serializes wave
            // execution (D220-EXEC crux). Disjoint groups → disjoint
            // `Arc<Mutex<CoreState>>` → true parallelism.
            Some(g) => {
                let shard_arc: ShardArc = {
                    let mut map = self.grouped_shards.lock();
                    map.entry(g)
                        .or_insert_with(|| {
                            std::sync::Arc::new(parking_lot::Mutex::new(CoreState::empty_shard(
                                self.binding.clone(),
                            )))
                        })
                        .clone()
                };
                parking_lot::Mutex::lock_arc(&shard_arc)
            }
        }
    }
}
