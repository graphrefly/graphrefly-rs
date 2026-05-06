//! Wave engine — drain loop, fire selection, emission commit, sink dispatch.
//!
//! Ports the wave-engine portion of the handle-protocol prototype
//! (`~/src/graphrefly-ts/src/__experiments__/handle-core/core.ts`).
//! Sibling to [`super::node`]; the dispatcher's other concerns
//! (registration, subscription, pause/resume, terminal cascade,
//! `set_deps`) live there.
//!
//! # Slice C-1: split point
//!
//! This module was extracted from [`super::node`] in Slice C-1 (M1-close
//! refactor; no semantic change). The methods are still on [`Core`] —
//! Rust supports `impl Core` blocks across multiple files in the same
//! crate. The split is purely organizational: it keeps the wave engine
//! reviewable as a unit and makes the dispatcher's "non-wave" concerns
//! (subscribe handshakes, terminal cascade graph walk, dep-mutation
//! validation) easier to audit independently.
//!
//! See [`migration-status.md`](../../../docs/migration-status.md) for the
//! milestone tracker.
//!
//! # Wave engine entry points
//!
//! - [`Core::run_wave`] — wave entry. Runs `thunk` (which queues the
//!   triggering messages), then drains all transitive fn-fires until
//!   `pending_fires` is empty, then flushes per-subscriber notifications.
//! - [`Core::drain_and_flush`] — drain phase + flush phase. Called
//!   internally by `run_wave`; also reachable from [`super::node`] for
//!   the `activate_derived` path that sets up dep handles for a
//!   newly-subscribed compute node.
//! - [`Core::commit_emission`] — equals-substitution + DIRTY/DATA/RESOLVED
//!   queueing + child propagation. Called from `emit()` (state node
//!   mutation) and `fire_fn` (compute node fn return).
//! - [`Core::queue_notify`] — per-subscriber message queueing with
//!   pause-buffer routing. Called from `commit_emission` (this module),
//!   plus `terminate_node`, `teardown_inner`, and `invalidate_inner`
//!   (in [`super::node`]).
//! - [`Core::deliver_data_to_consumer`] — single-edge propagation; marks
//!   the consumer for fn-fire if its tracked-deps set is satisfied.
//!   Called from `commit_emission`, plus `activate_derived` and
//!   `set_deps` in [`super::node`].
//!
//! # Re-entrance discipline (carried over from the v1 dispatcher)
//!
//! Sinks fire while the `parking_lot::Mutex<CoreState>` is held. A sink
//! that calls back into `Core::emit` deadlocks. Production hardening
//! (Slice A-bigger, M1-close) replaces this with snapshot-then-drop-then-
//! fire across all sink-fire sites; see [`super::node`] module doc.

use crate::boundary::FnResult;
use crate::handle::{HandleId, NodeId, NO_HANDLE};
use crate::message::Message;
use crate::node::{Core, CoreState, EqualsMode, NodeKind, Sink};

/// Deferred sink-fire jobs collected during `flush_notifications`. Each
/// entry pairs a snapshot of the sink Arcs to fire with the messages to
/// deliver to them — one entry per (node × phase) cell with non-empty
/// content. Drained from `CoreState` and fired lock-released.
pub(crate) type DeferredJobs = Vec<(Vec<Sink>, Vec<Message>)>;

impl Core {
    // -------------------------------------------------------------------
    // Wave entry + drain
    // -------------------------------------------------------------------

    /// Wave entry. The caller passes a thunk that performs the wave's
    /// triggering operation (`commit_emission`, `terminate_node`, etc.) under
    /// the held lock. After the thunk returns, this method drains all
    /// transitive fn-fires, collects sink-fire jobs into
    /// `s.deferred_flush_jobs`, runs wave cleanup, then **drops the lock and
    /// fires the deferred jobs** so subscribers run lock-released.
    ///
    /// Re-entrant note: if the wave engine is already in-tick (the thunk
    /// itself was invoked from inside another wave), the thunk runs and
    /// returns immediately — the outer wave's drain handles the queued
    /// work. The drop-then-fire dance happens only at the outermost wave.
    ///
    /// Lock-release discipline (Slice A-bigger, M1-close): all sink-fire
    /// sites in the dispatcher now follow the snapshot-then-drop-then-fire
    /// pattern. A sink that calls back into `Core::emit` / `pause` / `resume`
    /// / `invalidate` / `complete` / `error` / `teardown` re-acquires the
    /// lock fresh and runs a nested wave (the existing `s.in_tick` re-entrance
    /// gate composes with this transparently — nested calls observe
    /// `in_tick = false` because we cleared it before firing sinks). What
    /// is *still* held lock-during-callback is `BindingBoundary::invoke_fn`
    /// (fn re-entrance into Core deadlocks); that lift is a follow-up.
    pub(crate) fn run_wave<F>(&self, s: &mut CoreState, thunk: F)
    where
        F: FnOnce(&Self, &mut CoreState),
    {
        if s.in_tick {
            // Re-entrant: the outer wave's drain loop will pick up our queued work.
            thunk(self, s);
            return;
        }
        s.in_tick = true;
        thunk(self, s);
        self.drain_and_flush(s);
        // Wave cleanup — clear wave-scoped flags and the in-tick gate
        // BEFORE firing sinks, so a sink that re-enters Core sees a clean
        // wave-state and can start its own fresh wave.
        s.clear_wave_state();
        s.in_tick = false;
        // Commit snapshots — wave succeeded, snapshot retains are no longer
        // needed for restoration. The handles release into the binding-side
        // refcount; the cache slots already hold the new values.
        self.commit_wave_cache_snapshots(s);
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
            // Skip if the node was unregistered in the meantime (defensive;
            // can't happen in v1).
            let Some(rec) = s.nodes.get_mut(&node_id) else {
                // The snapshot retain is orphaned — release directly.
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

    pub(crate) fn drain_and_flush(&self, s: &mut CoreState) {
        // Drain pending fires until quiescent.
        let mut guard = 0u32;
        while !s.pending_fires.is_empty() {
            guard += 1;
            assert!(
                guard < 10_000,
                "wave drain exceeded 10k iterations (cycle?)"
            );
            let Some(next) = self.pick_next_fire(s) else {
                break;
            };
            self.fire_fn(s, next);
        }
        // Phase 2: deliver wave-close to subscribers.
        self.flush_notifications(s);
    }

    /// Pick a node whose **transitive** upstream is fully settled.
    ///
    /// A node `id` is ready to fire iff none of its transitive ancestors
    /// (via the `deps` chain) is currently in `pending_fires`. Diamond
    /// glitch prevention requires transitive (not immediate-only) reasoning
    /// — for graph `A → {B, C, E}; D = combine(B, C); F = combine(D, E)`,
    /// a wave that fires `E` first adds `F` to `pending_fires` before `D`
    /// is added (`D` only enters `pending_fires` after `B` and `C` fire).
    /// An immediate-only readiness check would mistakenly pick `F` because
    /// neither `D` nor `E` are in `pending_fires` at that moment, firing
    /// `F` against the stale activation-time `D` cache and again later
    /// when `D` actually settles. Transitive upstream walk catches `B`/`C`
    /// pending and correctly defers `F`.
    ///
    /// Cost: O(V) per candidate; worst case O(N·V) per pick. The existing
    /// porting-deferred entry on `pick_next_fire` perf flagged this as a
    /// future per-node `unresolved_dep_count` refactor; the diamond
    /// correctness comes first.
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
        // Empty deps (e.g. a state node — shouldn't normally be in
        // pending_fires, but defensive) → trivially settled.
        if rec.deps.is_empty() {
            return true;
        }
        // BFS up via `deps`. Return false on first hit in pending_fires;
        // skip nodes already visited in this walk.
        let mut visited: ahash::AHashSet<NodeId> = ahash::AHashSet::new();
        let mut stack: Vec<NodeId> = rec.deps.clone();
        while let Some(id) = stack.pop() {
            if !visited.insert(id) {
                continue;
            }
            if s.pending_fires.contains(&id) {
                return false;
            }
            if let Some(r) = s.nodes.get(&id) {
                for &d in &r.deps {
                    if !visited.contains(&d) {
                        stack.push(d);
                    }
                }
            }
        }
        true
    }

    fn fire_fn(&self, s: &mut CoreState, node_id: NodeId) {
        s.pending_fires.remove(&node_id);
        let (fn_id, dep_handles, kind) = {
            let rec = s.require_node(node_id);
            // Terminal node — fn never fires again (R1.3.4 / Lock 2.B).
            if rec.terminal.is_some() {
                return;
            }
            // First-run gate: every dep must have a handle before fn fires.
            if rec.dep_handles.contains(&NO_HANDLE) {
                return;
            }
            let Some(fn_id) = rec.fn_id else { return };
            (fn_id, rec.dep_handles.clone(), rec.kind)
        };
        // Drop the borrow across the binding call. The lock IS held; the
        // binding side must not re-enter Core methods (CLAUDE.md re-entrance
        // discipline; sinks fire later in flush phase).
        let result = self.binding.invoke_fn(node_id, fn_id, &dep_handles);

        {
            let rec = s.require_node_mut(node_id);
            rec.has_fired_once = true;
            // Dynamic: fn declares its tracked dep indices.
            if kind == NodeKind::Dynamic {
                let tracked = match &result {
                    FnResult::Data { tracked, .. } | FnResult::Noop { tracked } => tracked.clone(),
                };
                if let Some(t) = tracked {
                    rec.tracked = t.into_iter().collect();
                }
            }
        }

        match result {
            FnResult::Noop { .. } => {
                // No emission this wave — RESOLVED if we already DIRTY'd subscribers.
                let already_dirty = s.require_node(node_id).dirty;
                if already_dirty {
                    self.queue_notify(s, node_id, Message::Resolved);
                }
            }
            FnResult::Data { handle, .. } => {
                self.commit_emission(s, node_id, handle);
            }
        }
    }

    // -------------------------------------------------------------------
    // Emission commit — equals-substitution lives here
    // -------------------------------------------------------------------

    pub(crate) fn commit_emission(&self, s: &mut CoreState, node_id: NodeId, new_handle: HandleId) {
        assert!(
            new_handle != NO_HANDLE,
            "NO_HANDLE is not a valid DATA payload (R1.2.4) for node {node_id:?}",
        );
        // Defensive R1.3.4 terminal guard. Current call sites (`Core::emit`
        // and `fire_fn`) already screen out terminal nodes, so this is dead
        // for v1 — but a future operator's fn returning Data on a node that
        // became terminal mid-wave (e.g., via a sibling rewire path) would
        // silently violate "no DIRTY/DATA/RESOLVED after COMPLETE/ERROR"
        // without this. Released the caller's intern share if we short-
        // circuit (mirrors `Core::emit`'s terminal short-circuit).
        if s.require_node(node_id).terminal.is_some() {
            self.binding.release_handle(new_handle);
            return;
        }
        let (old_handle, equals_mode) = {
            let rec = s.require_node(node_id);
            (rec.cache, rec.equals)
        };
        let is_data = !self.handles_equal(equals_mode, old_handle, new_handle);

        // R1.3.1.a + R1.3.6.b: each `commit_emission` produces one DIRTY
        // followed by one tier-3 settlement (DATA or RESOLVED). For K
        // consecutive `emit` calls on a state node within one wave (e.g.
        // inside `batch()`), each emit's commit_emission queues its own
        // DIRTY/settlement pair, so subscribers see K DIRTYs balanced by
        // K settlements. The previous "skip DIRTY when already dirty"
        // optimization left subscribers with 1 DIRTY + K settlements when
        // an equals-match emit's RESOLVED-children-broadcast had already
        // queued DIRTY for a downstream node and a later value-changing
        // emit then triggered the same downstream's fn-fire — net imbalance.
        // The `dirty` flag stays purely for the RESOLVED-children-broadcast
        // gate on `involved_this_wave` (kept as `true` after first set so
        // wave cleanup resets it) but no longer gates DIRTY queueing.
        s.require_node_mut(node_id).dirty = true;
        self.queue_notify(s, node_id, Message::Dirty);

        if is_data {
            // Snapshot the OLD cache before overwriting, so a `BatchGuard`
            // panic-discard can roll back to pre-wave state. We retain the
            // old handle via the snapshot slot (it transfers from the cache
            // slot's existing retain — no new binding-side retain needed,
            // we just elide the `release_handle(old_handle)` below when
            // snapshotting). On wave commit, snapshots are dropped and
            // their retains released. On wave abort, the snapshot's retain
            // transfers back to the restored cache slot.
            let snapshot_taken = if s.in_tick && old_handle != NO_HANDLE {
                use std::collections::hash_map::Entry;
                match s.wave_cache_snapshots.entry(node_id) {
                    Entry::Vacant(slot) => {
                        slot.insert(old_handle);
                        true
                    }
                    Entry::Occupied(_) => false, // first snapshot for this node already exists
                }
            } else {
                false
            };
            s.require_node_mut(node_id).cache = new_handle;
            // Refcount: release prior cache handle UNLESS we just snapshotted
            // it (snapshot owns the retain now, on behalf of potential restore).
            if old_handle != NO_HANDLE && !snapshot_taken {
                self.binding.release_handle(old_handle);
            }
            self.queue_notify(s, node_id, Message::Data(new_handle));
            // Propagate to children
            let child_ids: Vec<NodeId> = s
                .children
                .get(&node_id)
                .map(|c| c.iter().copied().collect())
                .unwrap_or_default();
            for child_id in child_ids {
                let dep_idx = s
                    .require_node(child_id)
                    .deps
                    .iter()
                    .position(|&d| d == node_id);
                if let Some(idx) = dep_idx {
                    self.deliver_data_to_consumer(s, child_id, idx, new_handle);
                }
            }
        } else {
            // RESOLVED: handle unchanged. Don't release; old still in use.
            self.queue_notify(s, node_id, Message::Resolved);
            // Children that haven't been involved this wave still need DIRTY+RESOLVED
            // for the wave-mask bookkeeping (so their subscribers see consistent
            // wave-close on diamond fan-in).
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
                    self.queue_notify(s, child_id, Message::Dirty);
                    self.queue_notify(s, child_id, Message::Resolved);
                }
            }
        }
    }

    fn handles_equal(&self, mode: EqualsMode, a: HandleId, b: HandleId) -> bool {
        if a == b {
            return true; // identity-on-handles always sufficient
        }
        // Sentinel on either side: not equal, no boundary call.
        if a == NO_HANDLE || b == NO_HANDLE {
            return false;
        }
        match mode {
            EqualsMode::Identity => false,
            EqualsMode::Custom(handle) => self.binding.custom_equals(handle, a, b),
        }
    }

    pub(crate) fn deliver_data_to_consumer(
        &self,
        s: &mut CoreState,
        consumer_id: NodeId,
        dep_idx: usize,
        handle: HandleId,
    ) {
        let kind;
        let tracked_or_first_fire;
        {
            let consumer = s.require_node_mut(consumer_id);
            consumer.dep_handles[dep_idx] = handle;
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
                // State nodes don't have deps, so this branch is unreachable
                // in practice. No-op for safety.
            }
        }
    }

    // -------------------------------------------------------------------
    // Subscriber notification
    // -------------------------------------------------------------------

    pub(crate) fn queue_notify(&self, s: &mut CoreState, node_id: NodeId, msg: Message) {
        // Pause routing decision (R1.3.7.b tier table, §10.2 buffering):
        //   Tier 3 (DATA / RESOLVED) and Tier 4 (INVALIDATE) buffer while
        //   paused; all other tiers (DIRTY tier 1, PAUSE/RESUME tier 2,
        //   COMPLETE/ERROR tier 5, TEARDOWN tier 6) bypass the buffer and
        //   flush immediately. START (tier 0) is per-subscription and never
        //   transits queue_notify.
        let buffered_tier = matches!(msg.tier(), 3 | 4);
        let cap = s.pause_buffer_cap;
        // Refcount discipline:
        //   - For pause-buffer storage (paused + tier-3/4): retain on push,
        //     release on overflow drop or in `Core::resume`'s drain.
        //   - For pending_notify storage (every payload-bearing message
        //     queued for the wave-end flush): retain on push, release in
        //     `flush_notifications` after the sink fires. This is required
        //     because `commit_emission` releases the prior cache handle
        //     before the wave drains — without the pending_notify retain,
        //     a coalesced multi-emit (`emit(s, h1)` then `emit(s, h2)`
        //     inside one wave, e.g. via `Core::batch`) would underflow h1's
        //     refcount when emit2's commit_emission release fires before
        //     the wave-end flush. v1 single-emit-per-wave never exercised
        //     this; user-facing batch (Slice C-1.5) does.
        //
        // Subscribers.is_empty() shortcut deliberately runs BEFORE the retain
        // — zero subscribers means the message is dropped altogether and
        // never queued, so there's no buffer slot to keep alive.
        let rec = s.require_node_mut(node_id);
        if rec.subscribers.is_empty() {
            return;
        }
        if buffered_tier && rec.pause_state.is_paused() {
            if let Some(h) = msg.payload_handle() {
                self.binding.retain_handle(h);
            }
            let dropped_msgs = rec.pause_state.push_buffered(msg, cap);
            // Drop borrow on rec before calling binding for the dropped
            // payloads' release_handle (binding mutex is independent of
            // CoreState mutex, but staying conservative).
            for dm in dropped_msgs {
                if let Some(h) = dm.payload_handle() {
                    self.binding.release_handle(h);
                }
            }
            return;
        }
        // Non-paused queue: retain payload handle so coalesced multi-emit
        // doesn't dangle. Released in `flush_notifications`.
        if let Some(h) = msg.payload_handle() {
            self.binding.retain_handle(h);
        }
        s.pending_notify.entry(node_id).or_default().push(msg);
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
    /// before B's. Within a single node, message order is preserved
    /// (e.g. `[Data(h1), Data(h2)]` from coalesced multi-emits stays in
    /// emit order).
    fn flush_notifications(&self, s: &mut CoreState) {
        const PHASES: &[&[u8]] = &[
            &[1],    // DIRTY
            &[3, 4], // DATA/RESOLVED + INVALIDATE
            &[5],    // COMPLETE/ERROR
            &[6],    // TEARDOWN
        ];
        let pending = std::mem::take(&mut s.pending_notify);
        for &phase_tiers in PHASES {
            for (node_id, msgs) in &pending {
                let phase_msgs: Vec<Message> = msgs
                    .iter()
                    .copied()
                    .filter(|m| phase_tiers.contains(&m.tier()))
                    .collect();
                if phase_msgs.is_empty() {
                    continue;
                }
                let Some(rec) = s.nodes.get(node_id) else {
                    continue;
                };
                let sinks: Vec<Sink> = rec.subscribers.values().cloned().collect();
                if sinks.is_empty() {
                    continue;
                }
                s.deferred_flush_jobs.push((sinks, phase_msgs));
            }
        }
        // Refcount release: balance the retain done in `queue_notify` for
        // every payload-bearing message that landed in pending_notify.
        // Deferred to post-lock-drop so the binding's release path can't
        // re-enter Core under our lock.
        for msgs in pending.values() {
            for msg in msgs {
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
/// State-node cache writes that already executed inside the closure (via
/// `Core::emit`'s inline `commit_emission`) remain committed — subsequent
/// `cache_of` calls return the post-panic state. The atomicity guarantee is
/// sink-observability, not state-rollback.
///
/// # Thread safety
///
/// `BatchGuard` is **`!Send`** by design. `begin_batch` claims the
/// per-`Core` `in_tick` flag on the calling thread; sending the guard to
/// another thread and dropping it there would clear `in_tick` from a
/// different thread than the one that set it, breaking the implicit
/// thread-local "I own the wave scope" semantic. The marker
/// (`PhantomData<*const ()>`) makes the type unsuitable for cross-thread
/// transfer at the type level. Verified by the doctest below.
///
/// ```compile_fail
/// use graphrefly_core::{BatchGuard, BindingBoundary, Core, FnId, FnResult, HandleId, NodeId};
/// use std::sync::Arc;
///
/// struct Stub;
/// impl BindingBoundary for Stub {
///     fn invoke_fn(&self, _: NodeId, _: FnId, _: &[HandleId]) -> FnResult {
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
    // !Send marker. `*const ()` is neither Send nor Sync, so a guard can
    // never be moved to another thread or shared across them — matches the
    // thread-local nature of the in_tick claim.
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
            // Restore wave-cache snapshots — for each state node whose
            // cache was advanced during the panicked closure, roll back
            // to the pre-wave value. Subscribers see no tier-3+ delivery,
            // AND `cache_of(s)` returns the pre-panic state — atomicity
            // guarantee covers both observability and state.
            //
            // Refcount discipline: pending_notify entries (or any
            // already-collected deferred_handle_releases from a partial
            // drain) hold a queue_notify-time retain on every payload
            // handle. Release them here so the discard doesn't leak.
            let (pending, deferred_releases, restored_releases) = {
                let mut s = self.core.lock_state();
                let pending = std::mem::take(&mut s.pending_notify);
                let deferred_releases = std::mem::take(&mut s.deferred_handle_releases);
                let _: DeferredJobs = std::mem::take(&mut s.deferred_flush_jobs);
                s.pending_fires.clear();
                let restored = self.core.restore_wave_cache_snapshots(&mut s);
                s.clear_wave_state();
                s.in_tick = false;
                (pending, deferred_releases, restored)
            };
            // Lock dropped — release retains lock-released so the binding
            // can't deadlock against an internal binding mutex.
            for msgs in pending.values() {
                for msg in msgs {
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
        let (jobs, releases) = {
            let mut s = self.core.lock_state();
            self.core.drain_and_flush(&mut s);
            s.clear_wave_state();
            s.in_tick = false;
            // Wave succeeded — drop snapshot retains (no longer needed for
            // restoration; cache slots already hold the new values).
            self.core.commit_wave_cache_snapshots(&mut s);
            Core::drain_deferred(&mut s)
        };
        // Lock dropped — fire deferred sinks + release retains.
        self.core.fire_deferred(jobs, releases);
    }
}
