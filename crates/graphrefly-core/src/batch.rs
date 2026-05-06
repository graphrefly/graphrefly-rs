//! Wave engine — drain loop, fire selection, emission commit, sink dispatch.
//!
//! Ports the wave-engine portion of the handle-protocol prototype
//! (`~/src/graphrefly-ts/src/__experiments__/handle-core/core.ts`).
//! Sibling to [`super::node`]; the dispatcher's other concerns
//! (registration, subscription, pause/resume, terminal cascade,
//! `set_deps`) live there.
//!
//! # Wave engine entry points
//!
//! - [`Core::run_wave`] — wave entry. Claims `in_tick` under the state lock,
//!   runs `op` lock-released, then drains all transitive fn-fires and
//!   flushes per-subscriber notifications. Each fn-fire iteration drops
//!   the state lock around `BindingBoundary::invoke_fn` so user fn callbacks
//!   can re-enter Core safely.
//! - [`Core::drain_and_flush`] — drain phase + flush phase. Acquires/drops
//!   the state lock per iteration around `invoke_fn`.
//! - [`Core::commit_emission`] — equals-substitution + DIRTY/DATA/RESOLVED
//!   queueing + child propagation. `&self`-only; bracket-fires
//!   `BindingBoundary::custom_equals` lock-released.
//! - [`Core::queue_notify`] — per-subscriber message queueing with
//!   pause-buffer routing. Snapshots the subscriber list at first-touch-
//!   per-wave so late subscribers (installed mid-wave between drain
//!   iterations) don't receive duplicate deliveries from messages already
//!   queued before they subscribed.
//! - [`Core::deliver_data_to_consumer`] — single-edge propagation; marks
//!   the consumer for fn-fire if its tracked-deps set is satisfied.
//!   Called from `commit_emission`, plus `activate_derived` and
//!   `set_deps` in [`super::node`].
//!
//! # Re-entrance discipline (Slice A close — M1 fully lock-released)
//!
//! - **Wave-end sink fires** drop the state lock first (Slice A-bigger
//!   discipline).
//! - **`BindingBoundary::invoke_fn`** in `fire_fn` fires lock-released —
//!   user fn callbacks may re-enter `Core::emit` / `pause` / `resume` /
//!   `invalidate` / `complete` / `error` / `teardown` and run a nested
//!   wave (the existing `s.in_tick` re-entrance gate composes
//!   transparently).
//! - **`BindingBoundary::custom_equals`** in `commit_emission`'s equals
//!   check fires lock-released.
//! - **Subscribe-time handshake** is the one remaining lock-held callback.
//!   It now fires per-tier (`[Start]`, `[Data(v)]`, `[Complete|Error]`,
//!   `[Teardown]`) as separate sink calls, matching the canonical R1.3.5.a
//!   tier-split. Re-entrance from a handshake sink callback panics with
//!   the [`reentrance_guard`] diagnostic.

use std::sync::Arc;

use indexmap::map::Entry;
use parking_lot::ArcReentrantMutexGuard;

use smallvec::SmallVec;

use crate::boundary::{DepBatch, FnEmission, FnResult};
use crate::handle::{HandleId, NodeId, NO_HANDLE};
use crate::message::Message;
use crate::node::{Core, CoreState, EqualsMode, NodeKind, Sink};

/// Deferred sink-fire jobs collected during `flush_notifications`. Each
/// entry pairs a snapshot of the sink Arcs to fire with the messages to
/// deliver to them — one entry per (node × phase) cell with non-empty
/// content. Drained from `CoreState` and fired lock-released.
pub(crate) type DeferredJobs = Vec<(Vec<Sink>, Vec<Message>)>;

/// Per-node wave-end notification queue.
///
/// Subscribers are snapshotted on the FIRST `queue_notify` call for the
/// node within a wave. Subsequent calls append messages but reuse the
/// snapshot. This makes late subscribers (installed between fn-fire
/// iterations now that the state lock is dropped per-iteration) invisible
/// to messages already queued before they subscribed — they get the
/// already-fresh post-commit cache via their handshake, but do not
/// double-receive the wave's `[Dirty, Data]` that would otherwise duplicate
/// the value.
///
/// Bookkeeping: `payload_retains` is the count of `payload_handle()`
/// retains owed to the binding when this entry's messages release at
/// flush time. Each push of a payload-bearing message bumps it; flush
/// drains it via `deferred_handle_releases`.
pub(crate) struct PendingPerNode {
    pub(crate) sinks: Vec<Sink>,
    pub(crate) messages: Vec<Message>,
}

impl Core {
    // -------------------------------------------------------------------
    // Wave entry + drain
    // -------------------------------------------------------------------

    /// Wave entry. The caller passes a closure that performs the wave's
    /// triggering operation (`commit_emission`, `terminate_node`, etc.).
    /// The closure runs lock-released; closure-internal Core methods
    /// acquire the state lock as they go.
    ///
    /// **Implementation:** delegates to [`Self::begin_batch`] for the
    /// wave's RAII lifecycle. The returned `BatchGuard` holds the
    /// `wave_owner` re-entrant mutex for the wave's duration (cross-thread
    /// emits block; same-thread re-entry passes through), claims `in_tick`,
    /// and on drop runs the drain + flush + sink-fire phases — OR, if the
    /// closure panicked, the panic-discard path that restores cache
    /// snapshots and clears in_tick. This unification gives `run_wave` the
    /// same panic-safety guarantee as the user-facing `Core::batch`.
    ///
    /// **Re-entrance:** a closure invoked from inside another wave — the
    /// inner `run_wave`'s `begin_batch` observes `in_tick=true`, the
    /// returned guard is non-owning (`owns_tick=false`), drop is a no-op.
    /// The outer wave's drain picks up the inner closure's queued work.
    ///
    /// **Lock-release discipline (Slice A close, M1):** all binding-side
    /// callbacks except the subscribe-time handshake fire lock-released.
    /// Sinks that re-enter Core run a nested wave; user fns that re-enter
    /// Core run a nested wave; custom-equals oracles that re-enter Core
    /// run a nested wave. Cross-thread emits block at `wave_owner` until
    /// the in-flight wave's drain completes — preserving the user-facing
    /// "emit returning means subscribers have observed" contract.
    pub(crate) fn run_wave<F>(&self, op: F)
    where
        F: FnOnce(&Self),
    {
        // The guard holds wave_owner + claims in_tick. On normal scope
        // exit it drains + flushes + fires sinks. On panic-during-op it
        // restores cache snapshots + clears in_tick (panic-discard).
        let _guard = self.begin_batch();
        op(self);
        // _guard drops here → drain or panic-discard, depending on whether
        // op() returned normally.
    }

    /// Release retains held by `wave_cache_snapshots` and clear the map.
    /// Called from the wave-success paths in `run_wave` and `BatchGuard`.
    pub(crate) fn commit_wave_cache_snapshots(&self, s: &mut CoreState) {
        if s.wave_cache_snapshots.is_empty() {
            return;
        }
        let snapshots = std::mem::take(&mut s.wave_cache_snapshots);
        for (_, old_handle) in snapshots {
            self.binding.release_handle(old_handle);
        }
    }

    /// Restore cache slots from `wave_cache_snapshots` and clear the map.
    /// Called from the wave-abort path in `BatchGuard::drop` (panic).
    ///
    /// For each snapshotted node:
    ///
    /// 1. Read the current cache (the in-flight new value).
    /// 2. Set `cache = old_handle` (the snapshot's retained value).
    /// 3. Release the now-unowned current cache handle.
    ///
    /// Returns the list of "current" handles to release outside the lock.
    pub(crate) fn restore_wave_cache_snapshots(&self, s: &mut CoreState) -> Vec<HandleId> {
        if s.wave_cache_snapshots.is_empty() {
            return Vec::new();
        }
        let snapshots = std::mem::take(&mut s.wave_cache_snapshots);
        let mut releases = Vec::with_capacity(snapshots.len());
        for (node_id, old_handle) in snapshots {
            let Some(rec) = s.nodes.get_mut(&node_id) else {
                releases.push(old_handle);
                continue;
            };
            let current = std::mem::replace(&mut rec.cache, old_handle);
            if current != NO_HANDLE {
                releases.push(current);
            }
        }
        releases
    }

    /// Drain pending fires until quiescent, then flush wave-end notifications
    /// to subscribers. Each fire iteration drops the state lock around the
    /// binding's `invoke_fn` callback so user fns may re-enter Core safely.
    ///
    /// `&self`-only — manages its own locking. Called from [`Self::run_wave`]
    /// and [`super::node::Core::activate_derived`] (via `run_wave`).
    pub(crate) fn drain_and_flush(&self) {
        let mut guard = 0u32;
        loop {
            guard += 1;
            assert!(
                guard < 10_000,
                "wave drain exceeded 10k iterations (cycle?)"
            );
            // Pick next fire under a short lock.
            let next = {
                let s = self.lock_state();
                if s.pending_fires.is_empty() {
                    break;
                }
                self.pick_next_fire(&s)
            };
            let Some(next) = next else { break };
            // fire_fn manages its own locking around invoke_fn.
            self.fire_fn(next);
        }
        // Auto-resolve sweep: nodes registered in pending_auto_resolve
        // by the RESOLVED child propagation need a Resolved if they didn't
        // fire and settle via their own commit_emission. Check pending_notify
        // for each candidate — if it has Dirty but no tier-3+ message, the
        // node never settled and needs auto-Resolved. Route through
        // queue_notify so paused nodes get the Resolved into their pause
        // buffer.
        let mut s = self.lock_state();
        let candidates = std::mem::take(&mut s.pending_auto_resolve);
        for node_id in candidates {
            let needs_resolve = s
                .pending_notify
                .get(&node_id)
                .is_some_and(|entry| !entry.messages.iter().any(|m| m.tier() >= 3));
            if needs_resolve {
                self.queue_notify(&mut s, node_id, Message::Resolved);
            }
        }
        // Final flush phase — populates deferred_flush_jobs
        // from pending_notify (already carries per-node sink snapshots).
        self.flush_notifications(&mut s);
    }

    /// Pick a node whose **transitive** upstream is fully settled.
    ///
    /// A node `id` is ready to fire iff none of its transitive ancestors
    /// (via the `deps` chain) is currently in `pending_fires`. Diamond
    /// glitch prevention requires transitive (not immediate-only) reasoning
    /// — for graph `A → {B, C, E}; D = combine(B, C); F = combine(D, E)`,
    /// a wave that fires `E` first adds `F` to `pending_fires` before `D`
    /// is added. An immediate-only readiness check would mistakenly pick
    /// `F` because neither `D` nor `E` are in `pending_fires` at that
    /// moment, firing `F` against the stale activation-time `D` cache and
    /// again later when `D` actually settles. Transitive upstream walk
    /// catches `B`/`C` pending and correctly defers `F`.
    ///
    /// Cost: O(V) per candidate; worst case O(N·V) per pick. The existing
    /// porting-deferred entry on `pick_next_fire` perf flagged this as a
    /// future per-node `unresolved_dep_count` refactor.
    fn pick_next_fire(&self, s: &CoreState) -> Option<NodeId> {
        for &id in &s.pending_fires {
            if Self::transitive_upstream_settled(s, id) {
                return Some(id);
            }
        }
        // Cycle / no eligible candidate (every node has an upstream pending,
        // possibly via a cycle path): pick any so the drain guard advances.
        // The drain-iteration cap will catch genuine cycles.
        s.pending_fires.iter().copied().next()
    }

    fn transitive_upstream_settled(s: &CoreState, node_id: NodeId) -> bool {
        let rec = s.require_node(node_id);
        if rec.dep_count() == 0 {
            return true;
        }
        let mut visited: ahash::AHashSet<NodeId> = ahash::AHashSet::new();
        let mut stack: Vec<NodeId> = rec.dep_ids_vec();
        while let Some(id) = stack.pop() {
            if !visited.insert(id) {
                continue;
            }
            if s.pending_fires.contains(&id) {
                return false;
            }
            if let Some(r) = s.nodes.get(&id) {
                for dep_id in r.dep_ids() {
                    if !visited.contains(&dep_id) {
                        stack.push(dep_id);
                    }
                }
            }
        }
        true
    }

    /// Fire a node's fn lock-released around `invoke_fn`.
    ///
    /// Phase 1 (lock-held): remove from pending_fires, snapshot fn_id +
    /// dep_records → DepBatch + kind. Skip if terminal, first-run-gate-closed,
    /// or stateless.
    ///
    /// Phase 2 (lock-released): call `BindingBoundary::invoke_fn`. User fn
    /// callbacks may re-enter Core (`emit`, `pause`, etc.) and run a nested
    /// wave — the in_tick gate composes naturally because nested calls
    /// observe `in_tick = true` and skip their own drain.
    ///
    /// Phase 3 (lock-held): mark `has_fired_once`, store dynamic-tracked,
    /// decide between Noop+RESOLVED, single Data, or Batch.
    ///
    /// Phase 4: commit emissions. Single Data goes through
    /// `commit_emission` (with equals substitution). Batch emissions are
    /// processed in sequence — Data via `commit_emission_verbatim` (no
    /// equals substitution per R1.3.2.d / R1.3.3.c), Complete/Error via
    /// terminal cascade.
    fn fire_fn(&self, node_id: NodeId) {
        enum FireAction {
            None,
            SingleData(HandleId),
            Batch(SmallVec<[FnEmission; 2]>),
        }

        // Phase 1: snapshot inputs — build DepBatch per dep from dep_records.
        let prep: Option<(crate::handle::FnId, Vec<DepBatch>, NodeKind)> = {
            let mut s = self.lock_state();
            s.pending_fires.remove(&node_id);
            let rec = s.require_node(node_id);
            // Skip: terminal, first-run-gate-closed, or stateless.
            if rec.terminal.is_some() || rec.has_sentinel_deps() {
                None
            } else {
                rec.fn_id.map(|fn_id| {
                    let dep_batches: Vec<DepBatch> = rec
                        .dep_records
                        .iter()
                        .map(|dr| DepBatch {
                            data: dr.data_batch.clone(),
                            prev_data: dr.prev_data,
                            involved: dr.involved_this_wave,
                        })
                        .collect();
                    (fn_id, dep_batches, rec.kind)
                })
            }
        };
        let Some((fn_id, dep_batches, kind)) = prep else {
            return;
        };

        // Phase 2: invoke fn lock-released.
        let result = self.binding.invoke_fn(node_id, fn_id, &dep_batches);

        // Phase 3: apply result under the lock — defensive terminal check
        // (a sibling cascade may have terminated this node during phase 2).
        let action: FireAction = {
            let mut s = self.lock_state();
            // Defensive: node may have terminated mid-phase-2 via a sibling
            // cascade (a fn that re-entered `Core::error` on a path that
            // cascaded here). If so, release any payload handles and no-op.
            if s.require_node(node_id).terminal.is_some() {
                match &result {
                    FnResult::Data { handle, .. } => {
                        self.binding.release_handle(*handle);
                    }
                    FnResult::Batch { emissions, .. } => {
                        for em in emissions {
                            match em {
                                FnEmission::Data(h) | FnEmission::Error(h) => {
                                    self.binding.release_handle(*h);
                                }
                                FnEmission::Complete => {}
                            }
                        }
                    }
                    FnResult::Noop { .. } => {}
                }
                return;
            }
            let rec = s.require_node_mut(node_id);
            rec.has_fired_once = true;
            if kind == NodeKind::Dynamic {
                let tracked = match &result {
                    FnResult::Data { tracked, .. }
                    | FnResult::Noop { tracked }
                    | FnResult::Batch { tracked, .. } => tracked.clone(),
                };
                if let Some(t) = tracked {
                    rec.tracked = t.into_iter().collect();
                }
            }
            match result {
                FnResult::Noop { .. } => {
                    let already_dirty = s.require_node(node_id).dirty;
                    if already_dirty {
                        self.queue_notify(&mut s, node_id, Message::Resolved);
                    }
                    FireAction::None
                }
                FnResult::Data { handle, .. } => FireAction::SingleData(handle),
                FnResult::Batch { emissions, .. } if emissions.is_empty() => {
                    // Empty Batch is equivalent to Noop — settle with
                    // RESOLVED if the node was dirty (R1.3.1.a).
                    let already_dirty = s.require_node(node_id).dirty;
                    if already_dirty {
                        self.queue_notify(&mut s, node_id, Message::Resolved);
                    }
                    FireAction::None
                }
                FnResult::Batch { emissions, .. } => FireAction::Batch(emissions),
            }
        };

        // Phase 4: commit emissions.
        match action {
            FireAction::None => {}
            // Single Data — equals substitution applies (R1.3.2).
            FireAction::SingleData(handle) => {
                self.commit_emission(node_id, handle);
            }
            // Batch — process in sequence. No equals substitution
            // (R1.3.2.d / R1.3.3.c: multi-message waves pass verbatim).
            FireAction::Batch(emissions) => {
                self.commit_batch(node_id, emissions);
            }
        }
    }

    /// Process a `FnResult::Batch` emissions sequence. Each `Data` goes
    /// through `commit_emission_verbatim` (no equals substitution per
    /// R1.3.2.d / R1.3.3.c). Terminal emissions (`Complete` / `Error`)
    /// cascade per R1.3.4; processing stops at the first terminal and
    /// remaining handles are released (R1.3.4.a: no further messages
    /// after terminal).
    fn commit_batch(&self, node_id: NodeId, emissions: SmallVec<[FnEmission; 2]>) {
        let mut iter = emissions.into_iter();
        for em in iter.by_ref() {
            match em {
                FnEmission::Data(handle) => {
                    self.commit_emission_verbatim(node_id, handle);
                }
                FnEmission::Complete => {
                    self.complete(node_id);
                    break;
                }
                FnEmission::Error(handle) => {
                    self.error(node_id, handle);
                    break;
                }
            }
        }
        // Release handles from any emissions after the terminal break.
        for em in iter {
            match em {
                FnEmission::Data(h) | FnEmission::Error(h) => {
                    self.binding.release_handle(h);
                }
                FnEmission::Complete => {}
            }
        }
    }

    // -------------------------------------------------------------------
    // Emission commit — equals-substitution lives here
    // -------------------------------------------------------------------

    /// Apply a node's emission. `&self`-only; brackets the equals check
    /// around a lock release so `BindingBoundary::custom_equals` can re-enter
    /// Core safely.
    ///
    /// Phase 1 (lock-held): defensive terminal short-circuit; snapshot
    /// equals_mode + old cache handle.
    ///
    /// Phase 2 (lock-released): call `handles_equal` — `EqualsMode::Identity`
    /// is a pure `u64` compare with no boundary call; `EqualsMode::Custom`
    /// crosses to the binding's `custom_equals` oracle, which may re-enter
    /// Core.
    ///
    /// Phase 3 (lock-held): set cache, queue Dirty + Data/Resolved into
    /// pending_notify (which snapshots subscribers on first touch),
    /// propagate to children.
    pub(crate) fn commit_emission(&self, node_id: NodeId, new_handle: HandleId) {
        assert!(
            new_handle != NO_HANDLE,
            "NO_HANDLE is not a valid DATA payload (R1.2.4) for node {node_id:?}",
        );

        // Phase 1: terminal short-circuit + snapshot equals/cache.
        let snapshot = {
            let s = self.lock_state();
            let rec = s.require_node(node_id);
            if rec.terminal.is_some() {
                drop(s);
                self.binding.release_handle(new_handle);
                return;
            }
            (rec.cache, rec.equals)
        };
        let (old_handle, equals_mode) = snapshot;

        // Phase 2: equals check (lock-released for Custom).
        let is_data = !self.handles_equal_lock_released(equals_mode, old_handle, new_handle);

        // Phase 3: apply emission under the lock. Defensive terminal
        // re-check — a concurrent cascade between phase 2 and phase 3
        // could have terminated the node.
        let mut s = self.lock_state();
        if s.require_node(node_id).terminal.is_some() {
            drop(s);
            self.binding.release_handle(new_handle);
            return;
        }

        // R1.3.1.a condition (b): synthesize DIRTY only if node not already
        // dirty from an earlier emission in the same wave.
        let already_dirty = s.require_node(node_id).dirty;
        s.require_node_mut(node_id).dirty = true;
        if !already_dirty {
            self.queue_notify(&mut s, node_id, Message::Dirty);
        }

        if is_data {
            // P3 (Slice A close /qa): re-read CURRENT cache. Same-thread
            // re-entry from a `custom_equals` oracle that called back into
            // `Core::emit` on this same node during phase 2's lock-released
            // equals check could have advanced the cache between phase 1's
            // snapshot (`old_handle`) and this point. The wave-owner mutex
            // (Q2) prevents cross-thread concurrent commits, but same-thread
            // nested commits are intentionally allowed. Releasing
            // `old_handle` instead of the actual displaced handle would
            // cause a double-release on the binding side AND leak the
            // intermediate cache (the value the nested commit wrote).
            //
            // Note: `is_data` was computed against `(old_handle, new_handle)`
            // during phase 2; if the cache moved, the equals decision could
            // be stale. For Identity equals: if `current_cache == new_handle`,
            // we'll redundantly emit DATA when RESOLVED would suffice
            // (benign — subscribers see one extra DATA in the rare race).
            // For Custom equals racing the same-node: undefined, documented
            // in `porting-deferred.md`.
            let current_cache = s.require_node(node_id).cache;
            let snapshot_taken = if s.in_tick && current_cache != NO_HANDLE {
                use std::collections::hash_map::Entry;
                match s.wave_cache_snapshots.entry(node_id) {
                    Entry::Vacant(slot) => {
                        slot.insert(current_cache);
                        true
                    }
                    Entry::Occupied(_) => false,
                }
            } else {
                false
            };
            s.require_node_mut(node_id).cache = new_handle;
            if current_cache != NO_HANDLE && !snapshot_taken {
                self.binding.release_handle(current_cache);
            }
            self.queue_notify(&mut s, node_id, Message::Data(new_handle));
            // Propagate to children
            let child_ids: Vec<NodeId> = s
                .children
                .get(&node_id)
                .map(|c| c.iter().copied().collect())
                .unwrap_or_default();
            for child_id in child_ids {
                let dep_idx = s.require_node(child_id).dep_index_of(node_id);
                if let Some(idx) = dep_idx {
                    self.deliver_data_to_consumer(&mut s, child_id, idx, new_handle);
                }
            }
        } else {
            // RESOLVED: handle unchanged. Don't release; old still in use.
            self.queue_notify(&mut s, node_id, Message::Resolved);
            // Children that haven't been involved this wave need DIRTY
            // propagation so their subscribers see wave-open. Don't queue
            // Resolved here — the child may later receive DATA in the same
            // wave (e.g. batch scope with multiple emits on the same source)
            // and fire, settling via commit_emission. Eagerly queueing
            // Resolved would double-settle the Dirty. The post-drain
            // auto-resolve sweep in drain_and_flush settles any node with
            // Dirty but no tier-3 message.
            let child_ids: Vec<NodeId> = s
                .children
                .get(&node_id)
                .map(|c| c.iter().copied().collect())
                .unwrap_or_default();
            for child_id in child_ids {
                let already_involved = s.require_node(child_id).involved_this_wave;
                if !already_involved {
                    {
                        let child = s.require_node_mut(child_id);
                        child.involved_this_wave = true;
                        child.dirty = true;
                    }
                    self.queue_notify(&mut s, child_id, Message::Dirty);
                    s.pending_auto_resolve.insert(child_id);
                }
            }
        }
    }

    /// Equals check that crosses the binding boundary lock-released for
    /// `EqualsMode::Custom`. Caller must NOT hold the state lock.
    fn handles_equal_lock_released(&self, mode: EqualsMode, a: HandleId, b: HandleId) -> bool {
        if a == b {
            return true; // identity-on-handles always sufficient
        }
        if a == NO_HANDLE || b == NO_HANDLE {
            return false;
        }
        match mode {
            EqualsMode::Identity => false,
            EqualsMode::Custom(handle) => self.binding.custom_equals(handle, a, b),
        }
    }

    /// Commit a DATA emission **without** equals substitution — used by
    /// `FnResult::Batch` processing where multi-message waves pass through
    /// verbatim per R1.3.2.d / R1.3.3.c. DIRTY auto-prefix respects
    /// R1.3.1.a condition (b): only queued if node not already dirty.
    ///
    /// Structurally identical to the DATA branch of [`Self::commit_emission`]
    /// but skips the Phase 2 equals check entirely.
    fn commit_emission_verbatim(&self, node_id: NodeId, new_handle: HandleId) {
        assert!(
            new_handle != NO_HANDLE,
            "NO_HANDLE is not a valid DATA payload (R1.2.4) for node {node_id:?}",
        );

        let mut s = self.lock_state();
        let rec = s.require_node(node_id);
        if rec.terminal.is_some() {
            drop(s);
            self.binding.release_handle(new_handle);
            return;
        }

        // R1.3.1.a condition (b): DIRTY only if not already dirty.
        let already_dirty = s.require_node(node_id).dirty;
        s.require_node_mut(node_id).dirty = true;
        if !already_dirty {
            self.queue_notify(&mut s, node_id, Message::Dirty);
        }

        // Always DATA — no equals substitution for Batch emissions.
        let current_cache = s.require_node(node_id).cache;
        let snapshot_taken = if s.in_tick && current_cache != NO_HANDLE {
            use std::collections::hash_map::Entry;
            match s.wave_cache_snapshots.entry(node_id) {
                Entry::Vacant(slot) => {
                    slot.insert(current_cache);
                    true
                }
                Entry::Occupied(_) => false,
            }
        } else {
            false
        };
        s.require_node_mut(node_id).cache = new_handle;
        if current_cache != NO_HANDLE && !snapshot_taken {
            self.binding.release_handle(current_cache);
        }
        self.queue_notify(&mut s, node_id, Message::Data(new_handle));
        // Propagate to children
        let child_ids: Vec<NodeId> = s
            .children
            .get(&node_id)
            .map(|c| c.iter().copied().collect())
            .unwrap_or_default();
        for child_id in child_ids {
            let dep_idx = s.require_node(child_id).dep_index_of(node_id);
            if let Some(idx) = dep_idx {
                self.deliver_data_to_consumer(&mut s, child_id, idx, new_handle);
            }
        }
    }

    pub(crate) fn deliver_data_to_consumer(
        &self,
        s: &mut CoreState,
        consumer_id: NodeId,
        dep_idx: usize,
        handle: HandleId,
    ) {
        // Retain the handle for the batch accumulation slot — each DATA
        // handle in `data_batch` owns a retain share, released at wave-end
        // rotation in `clear_wave_state`.
        self.binding.retain_handle(handle);

        let kind;
        let tracked_or_first_fire;
        {
            let consumer = s.require_node_mut(consumer_id);
            consumer.dep_records[dep_idx].data_batch.push(handle);
            consumer.dep_records[dep_idx].involved_this_wave = true;
            consumer.involved_this_wave = true;
            kind = consumer.kind;
            tracked_or_first_fire = !consumer.has_fired_once || consumer.tracked.contains(&dep_idx);
        }
        match kind {
            NodeKind::Derived => {
                s.pending_fires.insert(consumer_id);
            }
            NodeKind::Dynamic => {
                if tracked_or_first_fire {
                    s.pending_fires.insert(consumer_id);
                }
            }
            NodeKind::State => {
                // State nodes don't have deps; unreachable in practice.
            }
        }
    }

    // -------------------------------------------------------------------
    // Subscriber notification
    // -------------------------------------------------------------------

    /// Queue a wave-end message for `node_id`'s subscribers.
    ///
    /// **Sink-snapshot-on-first-touch:** the per-wave subscriber list is
    /// snapshotted the first time `queue_notify` is called for each node
    /// in a wave (via the `Vacant` arm of `pending_notify.entry()`).
    /// Subsequent calls for the same node append messages but reuse the
    /// snapshot. This makes late subscribers (installed mid-wave between
    /// fn-fire iterations now that the state lock drops per-iteration)
    /// invisible to messages already queued before they subscribed —
    /// avoiding the duplicate-Data delivery hazard where a late subscriber
    /// would see `[Start, Data(post)]` from its handshake AND `[Dirty,
    /// Data(post)]` from the wave's flush.
    ///
    /// Pause routing decision (R1.3.7.b tier table, §10.2 buffering):
    ///   Tier 3 (DATA / RESOLVED) and Tier 4 (INVALIDATE) buffer while
    ///   paused; all other tiers (DIRTY tier 1, PAUSE/RESUME tier 2,
    ///   COMPLETE/ERROR tier 5, TEARDOWN tier 6) bypass the buffer and
    ///   flush immediately. START (tier 0) is per-subscription and never
    ///   transits queue_notify.
    pub(crate) fn queue_notify(&self, s: &mut CoreState, node_id: NodeId, msg: Message) {
        let buffered_tier = matches!(msg.tier(), 3 | 4);
        let cap = s.pause_buffer_cap;

        // Pause-routing branch — handles its own retain/release and returns
        // before we touch `pending_notify`, so the rec borrow is contained.
        {
            let rec = s.require_node_mut(node_id);
            if rec.subscribers.is_empty() {
                return;
            }
            if buffered_tier && rec.pause_state.is_paused() {
                if let Some(h) = msg.payload_handle() {
                    self.binding.retain_handle(h);
                }
                let dropped_msgs = rec.pause_state.push_buffered(msg, cap);
                for dm in dropped_msgs {
                    if let Some(h) = dm.payload_handle() {
                        self.binding.release_handle(h);
                    }
                }
                return;
            }
        }

        // Non-paused queue path: retain payload handle and queue into
        // pending_notify with a per-wave subscriber snapshot taken at
        // first touch. Released in `flush_notifications` after sinks fire.
        if let Some(h) = msg.payload_handle() {
            self.binding.retain_handle(h);
        }

        // Snapshot subscribers on first touch per wave. We need to read
        // `subscribers` separately from the `pending_notify.entry()` borrow
        // because both are fields of `CoreState` and split-borrowing
        // through a method call (`require_node_mut`) defeats the borrow
        // checker. So precompute the snapshot iff the entry is vacant.
        let needs_snapshot = !s.pending_notify.contains_key(&node_id);
        let sinks_snapshot: Vec<Sink> = if needs_snapshot {
            s.require_node(node_id)
                .subscribers
                .values()
                .cloned()
                .collect()
        } else {
            Vec::new()
        };
        match s.pending_notify.entry(node_id) {
            Entry::Vacant(slot) => {
                slot.insert(PendingPerNode {
                    sinks: sinks_snapshot,
                    messages: vec![msg],
                });
            }
            Entry::Occupied(mut slot) => {
                slot.get_mut().messages.push(msg);
            }
        }
    }

    /// Collect wave-end sink-fire jobs into `s.deferred_flush_jobs` and the
    /// payload-handle releases owed for `pending_notify` into
    /// `s.deferred_handle_releases`. The actual sink fires + handle releases
    /// run **after** the state lock is dropped — see [`Core::run_wave`].
    ///
    /// R1.3.1.b two-phase propagation: phase 1 (DIRTY) propagates through
    /// the entire graph before phase 2 (DATA / RESOLVED) begins. Implemented
    /// here as cross-node tier-then-node collect — phase 1's jobs sit before
    /// phase 2's in `deferred_flush_jobs`, so when `run_wave` drains the
    /// queue lock-released, multi-node subscribers see all DIRTYs before any
    /// settle. Matches TS's drainPhase model without the per-tier queue
    /// indirection.
    ///
    /// Phase ordering:
    ///   1 → tier 1   (DIRTY)
    ///   2 → tier 3+4 (DATA/RESOLVED + INVALIDATE — the "settle slice")
    ///   3 → tier 5   (COMPLETE/ERROR)
    ///   4 → tier 6   (TEARDOWN)
    ///
    /// Tier 0 (START) is per-subscription (never enters pending_notify) and
    /// tier 2 (PAUSE/RESUME) is delivered through dedicated paths, also
    /// bypassing pending_notify; both are absent from this enumeration.
    ///
    /// Within a single phase, per-node insertion order (IndexMap iteration)
    /// is preserved — an emit on A before B → A's phase-2 messages flush
    /// before B's. Within a single node, message order is preserved.
    fn flush_notifications(&self, s: &mut CoreState) {
        const PHASES: &[&[u8]] = &[
            &[1],    // DIRTY
            &[3, 4], // DATA/RESOLVED + INVALIDATE
            &[5],    // COMPLETE/ERROR
            &[6],    // TEARDOWN
        ];
        let pending = std::mem::take(&mut s.pending_notify);
        for &phase_tiers in PHASES {
            for (_node_id, entry) in &pending {
                let phase_msgs: Vec<Message> = entry
                    .messages
                    .iter()
                    .copied()
                    .filter(|m| phase_tiers.contains(&m.tier()))
                    .collect();
                if phase_msgs.is_empty() {
                    continue;
                }
                if entry.sinks.is_empty() {
                    continue;
                }
                // Clone the per-node sink snapshot — same Arcs, cheap.
                let sinks_clone: Vec<Sink> = entry.sinks.iter().map(Arc::clone).collect();
                s.deferred_flush_jobs.push((sinks_clone, phase_msgs));
            }
        }
        // Refcount release: balance the retain done in `queue_notify` for
        // every payload-bearing message that landed in pending_notify.
        // Deferred to post-lock-drop so the binding's release path can't
        // re-enter Core under our lock.
        for entry in pending.values() {
            for msg in &entry.messages {
                if let Some(h) = msg.payload_handle() {
                    s.deferred_handle_releases.push(h);
                }
            }
        }
    }

    /// Take the deferred sink-fire jobs and payload-handle releases from
    /// `CoreState`. Callers pair this with a `drop(state_guard)` and a
    /// subsequent [`Self::fire_deferred`] to deliver the wave's sinks
    /// lock-released. Returning a 2-tuple keeps the call sites concise.
    pub(crate) fn drain_deferred(s: &mut CoreState) -> (DeferredJobs, Vec<HandleId>) {
        (
            std::mem::take(&mut s.deferred_flush_jobs),
            std::mem::take(&mut s.deferred_handle_releases),
        )
    }

    /// Fire deferred sink-fire jobs in collected order, then release the
    /// payload handles owed for messages that landed in `pending_notify`
    /// during the wave. Both phases run lock-released so:
    /// - Sinks that call back into Core (emit, pause, etc.) re-acquire the
    ///   state lock cleanly and run their own nested wave.
    /// - The binding's `release_handle` path can't deadlock against a
    ///   binding-side mutex held by Core.
    pub(crate) fn fire_deferred(&self, jobs: DeferredJobs, releases: Vec<HandleId>) {
        for (sinks, msgs) in jobs {
            for sink in &sinks {
                sink(&msgs);
            }
        }
        for h in releases {
            self.binding.release_handle(h);
        }
    }

    // -------------------------------------------------------------------
    // User-facing batch — coalesce multiple emits into one wave
    // -------------------------------------------------------------------

    /// Coalesce multiple emissions into a single wave. Every `emit` /
    /// `complete` / `error` / `teardown` / `invalidate` call inside `f`
    /// queues its downstream work; the wave drains when `f` returns.
    ///
    /// **R1.3.6.a** — DIRTY still propagates immediately (tier 1 isn't
    /// deferred); only tier-3+ delivery is held until scope exit. **R1.3.6.b**
    /// — repeated emits on the same node coalesce into a single multi-message
    /// delivery (one [`Message::Dirty`] for the wave + one [`Message::Data`]
    /// per emit, all delivered together in the per-node phase-2 pass).
    ///
    /// Nested `batch()` calls share the outer wave; only the outermost call
    /// drives the drain. Re-entrant calls from inside an `emit`/fn (the wave
    /// engine's own `s.in_tick` re-entrance) compose with this method
    /// transparently — they observe `in_tick = true` and skip drain just
    /// like nested `batch()`.
    ///
    /// On panic inside `f`, the `BatchGuard` returned by the internal
    /// `begin_batch` call drops normally and discards pending tier-3+ work
    /// (subscribers do not observe the half-built wave). See
    /// [`Core::begin_batch`] for the RAII variant if you need explicit control
    /// over the scope boundary.
    pub fn batch<F>(&self, f: F)
    where
        F: FnOnce(),
    {
        let _guard = self.begin_batch();
        f();
    }

    /// RAII batch handle — opens a wave when constructed, drains on drop.
    ///
    /// Mirrors the closure-based [`Self::batch`] but exposes the scope
    /// boundary so callers can compose batches with non-`FnOnce` control
    /// flow (e.g. async-state-machine code paths, or splitting setup and
    /// drain across helper functions).
    ///
    /// ```ignore
    /// let g = core.begin_batch();
    /// core.emit(state_a, h1);
    /// core.emit(state_b, h2);
    /// drop(g); // wave drains here
    /// ```
    ///
    /// Like the closure form, nested `begin_batch` calls share the outer
    /// wave (only the outermost guard drains).
    #[must_use = "BatchGuard drains the wave on drop; assign to a named binding"]
    pub fn begin_batch(&self) -> BatchGuard {
        // Acquire the wave-owner re-entrant mutex BEFORE the state lock.
        // Same-thread re-entry passes through (e.g. fn → emit → run_wave →
        // begin_batch from inside outer wave's invoke_fn); cross-thread
        // entries block until the in-flight wave's `_wave_guard` drops.
        // `lock_arc()` is `!Send` — strengthens BatchGuard's `!Send`
        // guarantee at the type level.
        let wave_guard = self.wave_owner.lock_arc();
        let owns_tick = {
            let mut s = self.lock_state();
            let was_in = s.in_tick;
            if !was_in {
                s.in_tick = true;
            }
            !was_in
        };
        BatchGuard {
            core: self.clone(),
            owns_tick,
            _wave_guard: wave_guard,
            _not_send: std::marker::PhantomData,
        }
    }
}

/// RAII guard returned by [`Core::begin_batch`].
///
/// While alive, suppresses per-emit wave drains — multiple `emit` /
/// `complete` / `error` / `teardown` / `invalidate` calls coalesce into one
/// wave. On drop:
/// - Outermost guard: drains the wave (fires sinks, runs cleanup, clears
///   in-tick).
/// - Nested guard (an outer `BatchGuard` or an in-progress wave already owns
///   the in-tick flag): silently no-ops.
///
/// On thread panic during the closure body, the drop path discards pending
/// tier-3+ delivery rather than firing sinks (avoids cascading panics).
/// Subscribers observe **no tier-3+ delivery for the panicked wave**.
/// State-node cache writes that already executed inside the closure are
/// rolled back via wave-cache snapshots — `cache_of(s)` returns the pre-
/// panic value. The atomicity guarantee covers both sink-observability and
/// cache state.
///
/// # Thread safety
///
/// `BatchGuard` is **`!Send`** by design. `begin_batch` claims the
/// per-`Core` `in_tick` flag AND the per-`Core` `wave_owner` re-entrant
/// mutex on the calling thread; sending the guard to another thread
/// and dropping it there would clear `in_tick` and release `wave_owner`
/// from a different thread than the one that acquired them, breaking
/// both the thread-local "I own the wave scope" semantic and
/// `parking_lot::ReentrantMutex`'s ownership invariant. The
/// `_wave_guard` field is `!Send` (`ArcReentrantMutexGuard<()>`); the
/// `PhantomData<*const ()>` marker is belt-and-suspenders.
///
/// ```compile_fail
/// use graphrefly_core::{BatchGuard, BindingBoundary, Core, DepBatch, FnId, FnResult, HandleId, NodeId};
/// use std::sync::Arc;
///
/// struct Stub;
/// impl BindingBoundary for Stub {
///     fn invoke_fn(&self, _: NodeId, _: FnId, _: &[DepBatch]) -> FnResult {
///         FnResult::Noop { tracked: None }
///     }
///     fn custom_equals(&self, _: FnId, _: HandleId, _: HandleId) -> bool { false }
///     fn release_handle(&self, _: HandleId) {}
/// }
/// fn requires_send<T: Send>(_: T) {}
/// let core = Core::new(Arc::new(Stub) as Arc<dyn BindingBoundary>);
/// let guard = core.begin_batch();
/// requires_send(guard); // <- compile_fail: BatchGuard is !Send.
/// ```
#[must_use = "BatchGuard drains the wave on drop; assign to a named binding"]
pub struct BatchGuard {
    core: Core,
    owns_tick: bool,
    /// Re-entrant mutex guard held for the wave's duration. Drop releases
    /// `wave_owner`, allowing cross-thread waves blocked at
    /// `Core::begin_batch` to proceed. Same-thread re-entry (nested
    /// `begin_batch` from a fn callback) re-acquires this mutex
    /// transparently.
    ///
    /// `ArcReentrantMutexGuard<()>` is `!Send`, so `BatchGuard` is `!Send`
    /// at the type level — sending across threads would violate
    /// `parking_lot::ReentrantMutex`'s thread-ownership invariant.
    _wave_guard: ArcReentrantMutexGuard<parking_lot::RawMutex, parking_lot::RawThreadId, ()>,
    _not_send: std::marker::PhantomData<*const ()>,
}

impl Drop for BatchGuard {
    fn drop(&mut self) {
        if !self.owns_tick {
            return;
        }
        if std::thread::panicking() {
            // Discard pending wave work to avoid firing sinks during
            // unwind (sink panic during unwind would abort the process).
            //
            // Refcount discipline: pending_notify entries (or any
            // already-collected deferred_handle_releases from a partial
            // drain) hold a queue_notify-time retain on every payload
            // handle. Release them here so the discard doesn't leak.
            //
            // Wave-cache snapshots restore pre-panic cache values so the
            // atomicity guarantee covers state, not just observability.
            let (pending, deferred_releases, restored_releases) = {
                let mut s = self.core.lock_state();
                let pending = std::mem::take(&mut s.pending_notify);
                let _: DeferredJobs = std::mem::take(&mut s.deferred_flush_jobs);
                s.pending_fires.clear();
                let restored = self.core.restore_wave_cache_snapshots(&mut s);
                // clear_wave_state pushes batch-handle releases into
                // deferred_handle_releases, so take it AFTER the clear.
                s.clear_wave_state();
                let deferred_releases = std::mem::take(&mut s.deferred_handle_releases);
                s.in_tick = false;
                (pending, deferred_releases, restored)
            };
            // Lock dropped — release retains lock-released so the binding
            // can't deadlock against an internal binding mutex.
            for entry in pending.values() {
                for msg in &entry.messages {
                    if let Some(h) = msg.payload_handle() {
                        self.core.binding.release_handle(h);
                    }
                }
            }
            for h in deferred_releases {
                self.core.binding.release_handle(h);
            }
            for h in restored_releases {
                self.core.binding.release_handle(h);
            }
            return;
        }
        // Successful drain — drain_and_flush manages its own locking.
        self.core.drain_and_flush();
        // Wave cleanup + extract deferred jobs under the lock.
        let (jobs, releases) = {
            let mut s = self.core.lock_state();
            s.clear_wave_state();
            s.in_tick = false;
            self.core.commit_wave_cache_snapshots(&mut s);
            Core::drain_deferred(&mut s)
        };
        // Lock dropped — fire deferred sinks + release retains.
        self.core.fire_deferred(jobs, releases);
    }
}
