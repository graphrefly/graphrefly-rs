//! Identifier newtypes — the handle-protocol's core type vocabulary.
//!
//! Mirrors `~/src/graphrefly-ts/src/__experiments__/handle-core/core.ts:52–57`
//! under Rust's stricter type discipline (CLAUDE.md Rust invariant 8 — no raw
//! integers in public APIs).
//!
//! # Cleaving plane
//!
//! - [`NodeId`] — identifies a node in the graph. Allocated by the Core; opaque
//!   to bindings.
//! - [`HandleId`] — identifies a user value `T` in the binding-side registry.
//!   The Core never sees `T`; equals-substitution under `equals: identity` is
//!   a `u64` compare on `HandleId` (zero FFI). Custom equals crosses the
//!   boundary explicitly via [`crate::boundary::BindingBoundary::custom_equals`].
//! - [`FnId`] — identifies a user function (or a custom-equals oracle) in the
//!   binding-side registry.
//! - [`LockId`] — identifies a pause-lock. Multiple pausers can hold distinct
//!   locks on the same node; the node remains paused until the lockset is
//!   empty (R1.2.6, R2.6).
//!
//! All four are `u64` newtypes for cheap hashing, atomic increments, and
//! lock-free version counters. They are intentionally NOT
//! interconvertible — `NodeId(7)` and `HandleId(7)` are different things.

/// Identifier for a node in the Core's dispatcher.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Ord, PartialOrd)]
pub struct NodeId(u64);

impl NodeId {
    /// Wrap a raw `u64` as a `NodeId`. Production code allocates via the Core's
    /// id counter; this constructor exists for tests and for the binding-side
    /// registry to round-trip ids through serialization.
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Unwrap to the underlying `u64`. Use for hashing, logging, or
    /// FFI marshalling — never for arithmetic that mixes id spaces.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

/// Identifier for a user value `T` in the binding-side registry.
///
/// The Core stores `HandleId` everywhere user values would otherwise sit:
/// `cache`, `prevData`, `Data`/`Error` payloads, dep records. Equals-substitution
/// under `EqualsMode::Identity` compares `HandleId` directly (a `u64` ==).
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Ord, PartialOrd)]
pub struct HandleId(u64);

impl HandleId {
    /// Wrap a raw `u64`. Real handles come from the binding-side registry
    /// (`bindings.ts` `valueRegistry.intern(value)`); this constructor is for
    /// tests and FFI round-tripping.
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Unwrap to the underlying `u64`.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// True if this is the [`NO_HANDLE`] sentinel (handle id 0 is never
    /// assigned to a real value). Equivalent to `self == NO_HANDLE`.
    #[must_use]
    pub const fn is_sentinel(self) -> bool {
        self.0 == NO_HANDLE.0
    }
}

/// Sentinel "no handle" — distinct from any valid handle (which start at 1).
///
/// Mirrors `core.ts:57` `NO_HANDLE = 0`. Used for:
/// - `cache` of compute nodes that haven't fired yet.
/// - `prevData[i]` for deps that haven't delivered DATA yet.
/// - First-run gate condition (R2.5.3).
///
/// Per the handle-protocol cleaving plane, the binding-side registry refuses to
/// intern `undefined`/`None` (the global SENTINEL per R1.2.4 / Lock 5.A);
/// no real handle ever collides with this.
pub const NO_HANDLE: HandleId = HandleId(0);

/// Identifier for a user function (or a custom-equals oracle) in the
/// binding-side registry.
///
/// The Core invokes user code by sending `(node_id, fn_id, dep_handles)` across
/// the [`crate::boundary::BindingBoundary`]; the binding-side dereferences
/// `fn_id` to a callable and `dep_handles` to user values, then registers the
/// fn's output as a new handle and returns it.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Ord, PartialOrd)]
pub struct FnId(u64);

impl FnId {
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

/// Identifier for a pause-lock.
///
/// Per R1.2.6, `[PAUSE, lockId]` and `[RESUME, lockId]` MUST carry a lock id.
/// Each node maintains a lockset (`HashSet<LockId>`); `paused` derives from
/// `lockset.is_empty()`. Unknown-lockId `Resume` is a no-op (idempotent
/// dispose).
///
/// # Allocation ranges (Slice F, A4 — 2026-05-07)
///
/// To prevent collision between user-supplied and dispatcher-allocated lock
/// ids:
///
/// - `[0, 1<<32)` — **user range.** Direct callers of [`LockId::new`] (and
///   the napi-rs binding's `u32 → LockId` marshalling) live here. Pick any
///   value you like; the dispatcher will not allocate any id in this range.
/// - `[1<<32, u64::MAX]` — **dispatcher range.** [`crate::Core::alloc_lock_id`]
///   draws from this range, starting at `1<<32` and incrementing. Allocation
///   is monotonic; no recycling.
///
/// Both constructors are public — the range convention is by construction at
/// the dispatcher, not by visibility on the type. If a binding marshals lock
/// ids beyond `u32::MAX` from user-facing input, raise the dispatcher floor
/// (`CoreState::next_lock_id`) at construction time.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Ord, PartialOrd)]
pub struct LockId(u64);

impl LockId {
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_distinct_types() {
        // Compile-time: these would not coexist if NodeId/HandleId/FnId/LockId
        // were aliases. Run-time: just confirm the round-trip works.
        let n = NodeId::new(42);
        let h = HandleId::new(42);
        let f = FnId::new(42);
        let l = LockId::new(42);
        assert_eq!(n.raw(), 42);
        assert_eq!(h.raw(), 42);
        assert_eq!(f.raw(), 42);
        assert_eq!(l.raw(), 42);
    }

    #[test]
    fn no_handle_is_sentinel() {
        assert!(NO_HANDLE.is_sentinel());
        assert_eq!(NO_HANDLE.raw(), 0);
        assert!(!HandleId::new(1).is_sentinel());
    }

    #[test]
    fn copy_eq_hash() {
        use std::collections::HashSet;
        // u64 newtypes round-trip through Copy + Eq + Hash for use in
        // HashMap / HashSet / DashMap keys.
        let a = HandleId::new(7);
        let b = a;
        assert_eq!(a, b);

        let mut set = HashSet::new();
        set.insert(NodeId::new(1));
        set.insert(NodeId::new(2));
        assert!(set.contains(&NodeId::new(1)));
        assert!(!set.contains(&NodeId::new(99)));
    }
}
