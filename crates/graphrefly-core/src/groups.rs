//! Serialization-group wave-lock registry — the §7 concurrency substrate
//! that replaces the deleted D3 union-find per-subgraph partitioning
//! (`SESSION-rust-port-d3-per-subgraph-parallelism.md`, D085/D086 —
//! SUPERSEDED 2026-05-16; D208–D211).
//!
//! # Model
//!
//! A node carries `serialization_group: Option<SerializationGroupId>`
//! (see [`crate::handle::SerializationGroupId`]). Nodes sharing a group
//! serialize through one `Arc<ReentrantMutex<()>>` held for the lifetime
//! of any wave that touches them. Distinct groups with disjoint
//! touched-sets run truly parallel; the wave engine acquires every
//! touched group's mutex in **ascending `SerializationGroupId` order**
//! (deadlock-free under any interleaving — replaces the union-find
//! ascending-`SubgraphId` discipline).
//!
//! Unlike the union-find registry this replaces, groups are
//! **user-declared and static** — they do not migrate with topology.
//! There is therefore no `find` / path-compression / epoch /
//! `PARTITION_CACHE` / retry-validate / split / merge: the residual is
//! exactly "look up the touched groups' mutexes and acquire them sorted."
//!
//! # Single-threaded floor
//!
//! An all-`None` graph never inserts into this registry and the wave
//! engine's group-collect step iterates an empty set — the entire
//! acquisition path monomorphizes/branches out on the
//! `Core<SingleThreadCell>` floor (the §7 ~83 ns target).

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::ReentrantMutex;

use crate::handle::SerializationGroupId;

/// Get-or-create registry of per-group wave-serialization mutexes.
///
/// Held by [`crate::Core`] in `Arc<parking_lot::Mutex<…>>` so the locked
/// (`LockedCell`) substrate path is `Send + Sync`. The single-threaded
/// floor never touches it (no node carries a group).
#[derive(Default)]
pub(crate) struct GroupLockRegistry {
    locks: HashMap<SerializationGroupId, Arc<ReentrantMutex<()>>>,
}

impl GroupLockRegistry {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            locks: HashMap::new(),
        }
    }

    /// Resolve (creating on first touch) the wave-serialization mutex for
    /// group `g`. The `Arc` identity is stable for a group's lifetime, so
    /// the same group always serializes through the same mutex.
    #[must_use]
    pub(crate) fn lock_arc(&mut self, g: SerializationGroupId) -> Arc<ReentrantMutex<()>> {
        Arc::clone(
            self.locks
                .entry(g)
                .or_insert_with(|| Arc::new(ReentrantMutex::new(()))),
        )
    }

    /// Every known group's mutex, ascending by id. Backs the closure-form
    /// [`crate::Core::begin_batch`] "serialize against every group"
    /// contract (the per-seed [`crate::Core::begin_batch_for`] acquires
    /// only the seed's transitively-touched groups).
    #[must_use]
    pub(crate) fn all_sorted(&self) -> Vec<(SerializationGroupId, Arc<ReentrantMutex<()>>)> {
        let mut v: Vec<(SerializationGroupId, Arc<ReentrantMutex<()>>)> = self
            .locks
            .iter()
            .map(|(k, a)| (*k, Arc::clone(a)))
            .collect();
        v.sort_by_key(|(k, _)| k.raw());
        v
    }

    /// Number of distinct groups that have been touched (test/inspection).
    #[must_use]
    pub(crate) fn group_count(&self) -> usize {
        self.locks.len()
    }
}
