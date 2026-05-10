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

use smallvec::SmallVec;

use crate::boundary::{DepBatch, FnEmission, FnResult};
use crate::handle::{HandleId, NodeId, NO_HANDLE};
use crate::message::Message;
use crate::node::{Core, CoreState, EqualsMode, OperatorOp, Sink, TerminalKind};

/// Deferred sink-fire jobs collected during `flush_notifications`. Each
/// entry pairs a snapshot of the sink Arcs to fire with the messages to
/// deliver to them — one entry per (node × phase) cell with non-empty
/// content. Drained from `CoreState` and fired lock-released.
pub(crate) type DeferredJobs = Vec<(Vec<Sink>, Vec<Message>)>;

/// Lock-released drain payload of the wave's BatchGuard:
/// `(sink_jobs, handle_releases, OnInvalidate cleanup hooks, pending wipe_ctx fires)`.
/// Returned by [`Core::drain_deferred`], consumed by [`Core::fire_deferred`].
/// Sliced into a type alias to satisfy `clippy::type_complexity`.
pub(crate) type WaveDeferred = (
    DeferredJobs,
    Vec<HandleId>,
    Vec<(crate::handle::NodeId, crate::boundary::CleanupTrigger)>,
    Vec<crate::handle::NodeId>,
);

/// One subscriber-snapshot epoch within a node's wave-end notification
/// queue. A `PendingBatch` is opened the first time `queue_notify` runs
/// for the node in a wave, and a fresh batch is opened whenever the node's
/// `subscribers_revision` advances mid-wave (a new sink subscribes, an
/// existing sink unsubscribes, or a handshake-time panic evicts an
/// orphaned sink). All messages within one batch flush to the same sink
/// list — the snapshot taken when the batch opened, frozen against
/// subsequent revision bumps.
pub(crate) struct PendingBatch {
    /// `NodeRecord::subscribers_revision` value at the moment this batch
    /// opened. Used by `queue_notify` to decide append-to-last-batch vs
    /// open-fresh-batch on every push.
    pub(crate) snapshot_revision: u64,
    pub(crate) sinks: Vec<Sink>,
    pub(crate) messages: Vec<Message>,
}

/// Per-node wave-end notification queue, structured as one or more
/// subscriber-snapshot epochs (`batches`). The common case (no
/// mid-wave subscribe / unsubscribe at this node) keeps a single
/// inline batch — `SmallVec<[_; 1]>` keeps that allocation-free.
///
/// **Slice X4 / D2 (2026-05-08):** the prior shape was a single
/// `(sinks, messages)` pair per node — the snapshot froze on first
/// `queue_notify` and was reused for every subsequent emit to the same
/// node in the wave. That caused the documented late-subscriber +
/// multi-emit-per-wave gap (R1.3.5.a divergence): a sub installed
/// between two emits to the same node was invisible to the second
/// emit's flush slice. The revision-tracked batch list resolves it —
/// late subs land in a fresh batch that frozenly carries them, while
/// pre-subscribe batches retain their original snapshot so the new
/// sub doesn't double-receive earlier emits via flush AND handshake.
pub(crate) struct PendingPerNode {
    pub(crate) batches: SmallVec<[PendingBatch; 1]>,
}

impl PendingPerNode {
    /// Iterate every queued message for this node across all batches in
    /// arrival order. Used by R1.3.3.a invariant assertions and the
    /// auto-resolve / Slice-G coalescing tier-3-presence checks, which
    /// reason about wave-content per node, not per batch.
    pub(crate) fn iter_messages(&self) -> impl Iterator<Item = &Message> + '_ {
        self.batches.iter().flat_map(|b| b.messages.iter())
    }

    /// Mutable counterpart for `iter_messages`. Used by
    /// `rewrite_prior_resolved_to_data` to in-place rewrite Resolved
    /// entries to Data when a wave detects a multi-emit case after the
    /// fact.
    pub(crate) fn iter_messages_mut(&mut self) -> impl Iterator<Item = &mut Message> + '_ {
        self.batches.iter_mut().flat_map(|b| b.messages.iter_mut())
    }
}

/// RAII helper for the A6 reentrancy guard (Slice F, 2026-05-07).
///
/// Pushes `node_id` onto [`CoreState::currently_firing`] on construction,
/// pops it on Drop. [`Core::set_deps`] consults the stack and rejects
/// `set_deps(N, ...)` from inside N's own fn-fire with
/// [`crate::node::SetDepsError::ReentrantOnFiringNode`] — closing the
/// D1 hazard where Phase-1's snapshot of `dep_handles` would refer to
/// a different dep ordering than Phase-3's `tracked` storage.
///
/// Wraps the lock-released `invoke_fn` (and operator-equivalent FFI
/// callbacks like `project_each` / `predicate_each`). Drop fires even
/// on panic, so the stack stays balanced under user-fn unwinds.
///
/// Membership semantics (NOT strict LIFO): the only consumer of
/// `currently_firing` is `Core::set_deps`'s reentrancy check, which uses
/// `contains(&n)` — a set-membership test. Drop pops the right-most
/// matching `node_id` via `rposition` + `swap_remove`. For a stack like
/// `[A, B, A]` (A's fn re-enters B, B's fn re-enters A), B's drop pops
/// the SECOND A (index 1) via swap_remove, leaving `[A, A]` — the
/// physical order of the remaining As may not match construction order,
/// but membership is preserved. If a future call site needs strict LIFO
/// (e.g. "pop the most recently fired node"), switch to `pop()` + assert
/// the popped value equals `self.node_id`. (QA A6, 2026-05-07)
pub(crate) struct FiringGuard {
    core: Core,
    node_id: NodeId,
    /// Phase H+ option (d) /qa N1(a) widened variant (2026-05-09):
    /// whether this guard participates in the per-thread
    /// `IN_PRODUCER_BUILD` accounting. Producer-pattern operator
    /// activations (`zip` / `concat` / `race` / `take_until` /
    /// `switch_map` / `exhaust_map` / `concat_map` / `merge_map`
    /// — all `is_producer()`) DO participate: they SUPPRESS the H+
    /// check during their build/project closure because those
    /// closures legitimately subscribe to upstream sources
    /// cross-partition (operator-internal activation-time setup,
    /// not user-fn re-entry). Refactoring those operators to defer
    /// inner subscribes to wave-end is the broader Phase H+ STRICT
    /// variant scope; the limited variant carves them out via this
    /// flag. Derived / dynamic / state user-fn fires do NOT
    /// participate — the H+ check applies to them under the
    /// "held non-empty AND not in producer build" gate (see
    /// `crate::node::held_partitions` module docstring).
    ///
    /// **INVARIANT:** the value is captured at `FiringGuard::new`
    /// from `NodeRecord::is_producer()` for the firing node and
    /// must NEVER be re-derived at `Drop` time. `is_producer` is
    /// stable for a node's lifetime per the current registration
    /// API (a node's kind cannot change once registered), but a
    /// future contributor adding mutability MUST honor this snapshot
    /// to keep the `producer_build_enter` / `producer_build_exit`
    /// pair balanced. A debug_assert in Drop verifies the snapshot
    /// still matches when assertions are enabled.
    is_producer_build: bool,
}

impl FiringGuard {
    pub(crate) fn new(core: &Core, node_id: NodeId) -> Self {
        // Detect node kind under the same lock used to push
        // currently_firing. Producer-pattern nodes (build/project
        // closures) suppress the H+ check; everything else
        // (state / derived / dynamic / operators) is subject to it.
        let is_producer = {
            let mut s = core.lock_state();
            s.currently_firing.push(node_id);
            s.nodes
                .get(&node_id)
                .is_some_and(crate::node::NodeRecord::is_producer)
        };
        // Construct Self FIRST (capture the cached `is_producer_build`
        // snapshot in the struct). Then call `producer_build_enter()`
        // as a separate step. If a future contributor adds fallible
        // / panicking code between Self construction and the enter
        // call, the panic still leaves Self abandoned (no Drop runs
        // because Self isn't bound to a name yet) and the
        // `producer_build_enter` call hasn't been made — so no
        // imbalance. This is the panic-safe ordering per /qa A4.
        let guard = Self {
            core: core.clone(),
            node_id,
            is_producer_build: is_producer,
        };
        if is_producer {
            crate::node::producer_build_enter();
        }
        guard
    }
}

impl Drop for FiringGuard {
    fn drop(&mut self) {
        // INVARIANT (debug-asserted): `is_producer_build` must match
        // the node's current `is_producer()` at Drop time. If a future
        // refactor introduces post-construction node-kind mutation,
        // this fails loudly under debug builds — the
        // `producer_build_enter` / `producer_build_exit` pair would
        // otherwise become unbalanced. Per /qa A5.
        #[cfg(debug_assertions)]
        {
            let s = self.core.lock_state();
            let now_producer = s
                .nodes
                .get(&self.node_id)
                .is_some_and(crate::node::NodeRecord::is_producer);
            // Allow node-removed-mid-fire (now `is_some_and(...)` is
            // false) — that's a benign asymmetry (the producer flag
            // was true at construction; node removed before drop).
            // Real concern: a node that was non-producer at
            // construction is now reported as producer (or vice versa
            // for an existing node).
            if s.nodes.contains_key(&self.node_id) {
                debug_assert_eq!(
                    self.is_producer_build,
                    now_producer,
                    "FiringGuard invariant violation: node {:?} was {} at \
                     construction but is {} at Drop. The is_producer flag \
                     must be stable for a node's lifetime; see FiringGuard \
                     struct docstring.",
                    self.node_id,
                    if self.is_producer_build {
                        "is_producer=true"
                    } else {
                        "is_producer=false"
                    },
                    if now_producer {
                        "is_producer=true"
                    } else {
                        "is_producer=false"
                    },
                );
            }
            drop(s);
        }
        let mut s = self.core.lock_state();
        if let Some(pos) = s.currently_firing.iter().rposition(|n| *n == self.node_id) {
            s.currently_firing.swap_remove(pos);
        }
        // else: already popped by an external rebalance — silent no-op
        // for Drop discipline (panic-in-Drop is poison).
        drop(s);
        // Phase H+ pair: decrement IN_PRODUCER_BUILD IFF we incremented
        // in `new`. Done lock-released so the next thread waiting on
        // the state lock can proceed without our cell access on its
        // hot path.
        if self.is_producer_build {
            crate::node::producer_build_exit();
        }
    }
}

/// Borrow the per-operator scratch slot as `&T`. Panics if the slot is
/// uninitialized or the contained type doesn't match `T` — both are
/// invariant violations for any `fire_op_*` helper that should only be
/// called from `fire_operator`'s match arm for the matching variant.
fn scratch_ref<T: crate::op_state::OperatorScratch>(s: &CoreState, node_id: NodeId) -> &T {
    s.require_node(node_id)
        .op_scratch
        .as_ref()
        .expect("op_scratch slot uninitialized for operator node")
        .as_any_ref()
        .downcast_ref::<T>()
        .expect("op_scratch type mismatch")
}

/// Mutable borrow of the per-operator scratch slot. Same invariants as
/// [`scratch_ref`].
fn scratch_mut<T: crate::op_state::OperatorScratch>(s: &mut CoreState, node_id: NodeId) -> &mut T {
    s.require_node_mut(node_id)
        .op_scratch
        .as_mut()
        .expect("op_scratch slot uninitialized for operator node")
        .as_any_mut()
        .downcast_mut::<T>()
        .expect("op_scratch type mismatch")
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
    /// Wave entry with a known `seed` node. Acquires only the partitions
    /// transitively touched from `seed` (downstream cascade via
    /// `s.children` + R1.3.9.d meta-companion cascade) instead of every
    /// current partition. The canonical Y1 parallelism win for per-seed
    /// entry points (`Core::emit`, `Core::subscribe`'s activation,
    /// `Core::pause` / `Core::resume` / `Core::invalidate` / `Core::complete`
    /// / `Core::error` / `Core::teardown` / `Core::set_deps`'s
    /// push-on-subscribe).
    ///
    /// Two threads with disjoint touched-partition sets run truly
    /// parallel — they don't block each other on Core-global locks.
    /// Same-thread re-entry passes through each partition's
    /// `ReentrantMutex` transparently. Cross-thread emits on the SAME
    /// partition (or any overlapping touched-partition set) serialize
    /// per the per-partition `wave_owner` mutex, preserving the
    /// "emit returning means subscribers have observed" contract.
    ///
    /// Slice Y1 / Phase E (2026-05-08).
    pub(crate) fn run_wave_for<F>(&self, seed: crate::handle::NodeId, op: F)
    where
        F: FnOnce(&Self),
    {
        let _guard = self.begin_batch_for(seed);
        op(self);
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
            // R1.3.8.c (Slice F, A3): if no fires are pending but there are
            // queued pause-overflow ERRORs, synthesize them now. The
            // resulting ERROR cascade may add to pending_fires (children
            // settling their terminal state), so we loop back to drain.
            let synth_pending = {
                let mut s = self.lock_state();
                if s.pending_fires.is_empty() && !s.pending_pause_overflow.is_empty() {
                    std::mem::take(&mut s.pending_pause_overflow)
                } else {
                    Vec::new()
                }
            };
            for entry in synth_pending {
                // Lock-released call to the binding hook. Default impl
                // returns None — the binding has opted out of R1.3.8.c
                // and we fall back to silent-drop + ResumeReport.dropped.
                let handle = self.binding.synthesize_pause_overflow_error(
                    entry.node_id,
                    entry.dropped_count,
                    entry.configured_max,
                    entry.lock_held_ns / 1_000_000,
                );
                if let Some(h) = handle {
                    // Re-enter Core::error to terminate the node and
                    // cascade. We're inside a wave (`in_tick = true`),
                    // so error() gets a non-owning batch guard — it
                    // doesn't try to start its own drain. The cascade
                    // queues into our outer drain via pending_fires
                    // and pending_notify.
                    self.error(entry.node_id, h);
                }
            }

            // Pick next fire under a short lock. Also re-read the configured
            // drain cap so callers can tune via `Core::set_max_batch_drain_iterations`
            // without restarting waves mid-flight.
            let (next, cap, pending_size) = {
                let s = self.lock_state();
                if s.pending_fires.is_empty() {
                    break;
                }
                let cap = s.max_batch_drain_iterations;
                let pending_size = s.pending_fires.len();
                let next = self.pick_next_fire(&s);
                (next, cap, pending_size)
            };
            guard += 1;
            assert!(
                guard < cap,
                "wave drain exceeded {cap} iterations \
                 (pending_fires={pending_size}). Most likely cause: a runtime \
                 cycle introduced by an operator that re-arms its own pending_fires \
                 slot from inside `invoke_fn` (e.g. a producer that subscribes to \
                 itself, or a fn that calls Core::emit on a node whose fn fires \
                 the original node again). Structural cycles via set_deps are \
                 rejected at edge-mutation time. Tune via Core::set_max_batch_drain_iterations \
                 only with concrete evidence the workload needs more iterations."
            );
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
                .is_some_and(|entry| !entry.iter_messages().any(|m| m.tier() >= 3));
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

    /// Wave drain entry point. Dispatches via `rec.op` to either the
    /// regular fn-fire path ([`Self::fire_regular`]) or the operator
    /// dispatch ([`Self::fire_operator`]).
    pub(crate) fn fire_fn(&self, node_id: NodeId) {
        let op = {
            let s = self.lock_state();
            s.nodes.get(&node_id).and_then(|r| r.op)
        };
        match op {
            Some(operator_op) => self.fire_operator(node_id, operator_op),
            None => {
                // State / Derived / Dynamic / Producer all dispatch via fn_id.
                self.fire_regular(node_id);
            }
        }
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
    #[allow(clippy::too_many_lines)] // Slice G added Noop / Batch tier-3 guards
    fn fire_regular(&self, node_id: NodeId) {
        enum FireAction {
            None,
            SingleData(HandleId),
            Batch(SmallVec<[FnEmission; 2]>),
        }

        // Phase 1: snapshot inputs — build DepBatch per dep from dep_records.
        // `has_fired_once` is captured here for the Slice E2 OnRerun gate
        // (Phase 1.5 below): the cleanup hook only fires when the fn has
        // run at least once already in this activation cycle.
        let prep: Option<(crate::handle::FnId, Vec<DepBatch>, bool, bool)> = {
            let mut s = self.lock_state();
            s.pending_fires.remove(&node_id);
            let rec = s.require_node(node_id);
            // Skip: terminal, first-run-gate-closed (R2.5.3 / R5.4 — partial
            // mode opts out of the gate per D011), or stateless.
            if rec.terminal.is_some() || (!rec.partial && rec.has_sentinel_deps()) {
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
                    (fn_id, dep_batches, rec.is_dynamic, rec.has_fired_once)
                })
            }
        };
        let Some((fn_id, dep_batches, is_dynamic, has_fired_once)) = prep else {
            return;
        };

        // Phase 1.5 (Slice E2 — R2.4.5 OnRerun, lock-released per D045): if
        // the fn has fired at least once in this activation cycle, fire its
        // OnRerun cleanup hook BEFORE the next invoke_fn re-allocates fn-
        // local resources. First-fire is intentionally skipped — there is
        // no prior run to clean up. Fires OUTSIDE `FiringGuard` because
        // cleanup re-entrance is not the A6 reentrancy concern (which
        // protects against `set_deps(self, ...)` from inside the in-flight
        // invoke_fn). Operator nodes never reach this path (`fire_regular`
        // is the fn-id branch of `fire_fn`; operators dispatch via
        // `fire_operator`), so cleanup hooks correctly only fire for fn-
        // shaped nodes (state / derived / dynamic / producer).
        if has_fired_once {
            self.binding
                .cleanup_for(node_id, crate::boundary::CleanupTrigger::OnRerun);
        }

        // Phase 2: invoke fn lock-released. A6 reentrancy guard is scoped to
        // the FFI call only — Phase 3's lock-held state mutation is not part
        // of "currently firing" because set_deps would already block on the
        // state lock by then. Drop on the guard pops the stack even if
        // invoke_fn panics, keeping `currently_firing` balanced.
        let result = {
            let _firing = FiringGuard::new(self, node_id);
            self.binding.invoke_fn(node_id, fn_id, &dep_batches)
        };

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
            if is_dynamic {
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
                    // Slice G: skip Resolved if a prior emission in the same
                    // wave already queued tier-3 (would violate R1.3.3.a).
                    let already_dirty = s.require_node(node_id).dirty;
                    let already_tier3 = s
                        .pending_notify
                        .get(&node_id)
                        .is_some_and(|entry| entry.iter_messages().any(|m| m.tier() == 3));
                    if already_dirty && !already_tier3 {
                        self.queue_notify(&mut s, node_id, Message::Resolved);
                    }
                    FireAction::None
                }
                FnResult::Data { handle, .. } => FireAction::SingleData(handle),
                FnResult::Batch { emissions, .. } if emissions.is_empty() => {
                    // Empty Batch is equivalent to Noop — settle with
                    // RESOLVED if the node was dirty (R1.3.1.a). Slice G:
                    // skip if a prior emission already queued tier-3.
                    let already_dirty = s.require_node(node_id).dirty;
                    let already_tier3 = s
                        .pending_notify
                        .get(&node_id)
                        .is_some_and(|entry| entry.iter_messages().any(|m| m.tier() == 3));
                    if already_dirty && !already_tier3 {
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

        // Slice G (2026-05-07): R1.3.2.d says equals substitution only
        // fires for SINGLE-DATA waves at one node. Detect "this is a
        // subsequent emit in the same wave at this node" via the
        // wave-scoped `tier3_emitted_this_wave` set. If set → multi-emit
        // wave: skip equals, queue Data verbatim, retroactively rewrite
        // any prior Resolved (queued by an earlier same-value emit's
        // equals match) to Data using the wave-start cache snapshot.
        // Outside batch / first emit: standard per-emit equals path.
        let is_subsequent_emit_in_wave = {
            let s = self.lock_state();
            s.tier3_emitted_this_wave.contains(&node_id)
        };

        if is_subsequent_emit_in_wave {
            // Multi-emit wave detected. Skip equals, queue Data verbatim.
            // Also rewrite any prior Resolved entries to Data using the
            // wave-start cache snapshot.
            self.rewrite_prior_resolved_to_data(node_id);
            self.commit_emission_verbatim(node_id, new_handle);
            return;
        }

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
            // snapshot (`old_handle`) and this point.
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
            // Slice E1 (R2.6.5 / Lock 6.G): push DATA into the replay
            // buffer if the node opted in. RESOLVED entries are NOT
            // buffered (canonical "DATA only").
            self.push_replay_buffer(&mut s, node_id, new_handle);
            // Slice G: mark this node as having emitted tier-3 in this wave.
            s.tier3_emitted_this_wave.insert(node_id);
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
            // Slice G: snapshot cache so a subsequent same-wave emit can
            // rewrite this Resolved to Data using the snapshot.
            let current_cache = s.require_node(node_id).cache;
            if s.in_tick && current_cache != NO_HANDLE {
                use std::collections::hash_map::Entry;
                if let Entry::Vacant(slot) = s.wave_cache_snapshots.entry(node_id) {
                    self.binding.retain_handle(current_cache);
                    slot.insert(current_cache);
                }
            }
            // Slice G: mark this node as having emitted tier-3 in this wave.
            s.tier3_emitted_this_wave.insert(node_id);
            self.queue_notify(&mut s, node_id, Message::Resolved);
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

    /// Slice G: when a multi-emit wave is detected at `node_id` (a second
    /// emit arrives while a prior tier-3 message is still pending), rewrite
    /// any `Resolved` entries from earlier emits to `Data(snapshot_cache)`
    /// so the wave conforms to R1.3.3.a (≥1 DATA OR exactly 1 RESOLVED).
    /// Touches both `pending_notify` (immediate-flush path) and the per-node
    /// pause buffer (paused path).
    fn rewrite_prior_resolved_to_data(&self, node_id: NodeId) {
        let mut s = self.lock_state();
        let snapshot = match s.wave_cache_snapshots.get(&node_id).copied() {
            Some(h) if h != NO_HANDLE => h,
            // No snapshot available — the prior Resolved was queued without
            // a cache (sentinel pre-emit). Nothing to rewrite to; the
            // multi-emit case from sentinel is fine (verbatim Data path).
            _ => return,
        };
        let mut retains_needed = 0u32;
        // Pending_notify path. Walk all batches' messages — Slice-G
        // coalescing reasons about wave-content per node, not per-batch.
        if let Some(entry) = s.pending_notify.get_mut(&node_id) {
            for msg in entry.iter_messages_mut() {
                if matches!(msg, Message::Resolved) {
                    *msg = Message::Data(snapshot);
                    retains_needed += 1;
                }
            }
        }
        // Pause-buffer path.
        if let Some(rec) = s.nodes.get_mut(&node_id) {
            if let crate::node::PauseState::Paused { buffer, .. } = &mut rec.pause_state {
                for msg in &mut *buffer {
                    if matches!(msg, Message::Resolved) {
                        *msg = Message::Data(snapshot);
                        retains_needed += 1;
                    }
                }
            }
        }
        drop(s);
        // Each rewritten Resolved → Data adds a payload retain that
        // queue_notify would otherwise have taken at emit time. The
        // snapshot already owns one retain (taken when cache was
        // snapshotted); we need one fresh retain per rewrite.
        for _ in 0..retains_needed {
            self.binding.retain_handle(snapshot);
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
        // Slice E1: replay buffer push (R2.6.5 / Lock 6.G).
        self.push_replay_buffer(&mut s, node_id, new_handle);
        // Slice G QA fix (A2, 2026-05-07): mark tier3_emitted_this_wave
        // even on the verbatim path. A subsequent commit_emission at the
        // same node in the same wave needs this flag to detect multi-emit
        // and skip equals substitution; without it, a Batch-then-standard
        // sequence would queue Resolved into a wave that already has Data
        // — violating R1.3.3.a. The Batch path itself still passes
        // verbatim per R1.3.3.c (we don't re-run equals here); we just
        // record that "this node has emitted tier-3 in this wave."
        s.tier3_emitted_this_wave.insert(node_id);
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

    /// Slice E1 (R2.6.5 / Lock 6.G): push a DATA handle into the node's
    /// replay buffer if opted in. Evicts oldest if cap exceeded; takes a
    /// fresh retain on push. RESOLVED is NOT buffered per canonical
    /// "DATA only" — call sites only invoke this for Data emissions.
    ///
    /// Evicted handle is queued into `s.deferred_handle_releases`
    /// (released lock-released at flush time) per the binding-boundary
    /// lock-release discipline — `release_handle` may re-enter Core via
    /// finalizers and must not run while the state lock is held
    /// (QA A3, 2026-05-07).
    fn push_replay_buffer(&self, s: &mut CoreState, node_id: NodeId, new_handle: HandleId) {
        let rec = s.require_node_mut(node_id);
        let cap = match rec.replay_buffer_cap {
            Some(c) if c > 0 => c,
            _ => return,
        };
        self.binding.retain_handle(new_handle);
        rec.replay_buffer.push_back(new_handle);
        let evicted = if rec.replay_buffer.len() > cap {
            rec.replay_buffer.pop_front()
        } else {
            None
        };
        if let Some(h) = evicted {
            s.deferred_handle_releases.push(h);
        }
    }

    // ===================================================================
    // Operator dispatch (Slice C-1, D009).
    //
    // `fire_operator` is the entry point for nodes whose `kind` is
    // `NodeKind::Operator(_)`. It branches on the `OperatorOp` discriminant
    // to per-operator helpers that snapshot inputs under the lock, drop the
    // lock to call the binding's bulk projection FFI, and reacquire to
    // apply emissions via `commit_emission_verbatim` (no per-item equals
    // dedup at the wire — operator output passes verbatim per the same
    // R1.3.2.d / R1.3.3.c rule that governs `FnResult::Batch`).
    //
    // **Refcount discipline:** inputs sourced from `dep_records[i].data_batch`
    // share retains owned by the wave's data-batch slot (released at
    // wave-end rotation in `clear_wave_state`). Operators that emit those
    // handles unchanged (`Filter`, `DistinctUntilChanged`, `Pairwise`'s
    // `prev` carry-over) take an additional retain via `retain_handle`
    // before passing to `commit_emission_verbatim` — the cache slot owns
    // its own share, independent of the data-batch slot's. Operators that
    // produce fresh handles (`Map` / `Scan` / `Reduce` / `Pairwise`'s
    // packed tuples) receive retains pre-bumped by the binding's bulk-
    // projection method.
    // ===================================================================

    /// Operator dispatch entry. Pre-checks (terminal short-circuit, first-
    /// run gate accounting for `partial`, terminal-aware fire for `Reduce`)
    /// happen here; per-operator behavior lives in the `fire_op_*` helpers.
    fn fire_operator(&self, node_id: NodeId, op: OperatorOp) {
        // Phase 1 (lock-held): remove from pending_fires, evaluate skip.
        let proceed = {
            let mut s = self.lock_state();
            s.pending_fires.remove(&node_id);
            let rec = s.require_node(node_id);
            if rec.terminal.is_some() {
                false
            } else {
                // First-run gate (R2.5.3 / R5.4). Partial-mode operators
                // (D011) opt out of the gate; otherwise we wait for every
                // dep to have delivered at least one real handle. Terminal-
                // aware operators (currently `Reduce`) additionally count a
                // dep terminal as "real input" so they can fire on
                // upstream COMPLETE-without-DATA and emit the seed.
                let has_real_input = !rec.has_sentinel_deps()
                    || rec.dep_records.iter().any(|dr| dr.terminal.is_some());
                rec.partial || has_real_input
            }
        };
        if !proceed {
            return;
        }

        // A6 (Slice F, 2026-05-07): track operator fire on the
        // `currently_firing` stack so a binding-side project/predicate/fold
        // FFI callback that re-enters `Core::set_deps(node_id, ...)` is
        // rejected with `SetDepsError::ReentrantOnFiringNode`. Drop pops
        // the stack on panic too.
        let _firing = FiringGuard::new(self, node_id);

        match op {
            OperatorOp::Map { fn_id } => self.fire_op_map(node_id, fn_id),
            OperatorOp::Filter { fn_id } => self.fire_op_filter(node_id, fn_id),
            OperatorOp::Scan { fn_id, .. } => self.fire_op_scan(node_id, fn_id),
            OperatorOp::Reduce { fn_id, .. } => self.fire_op_reduce(node_id, fn_id),
            OperatorOp::DistinctUntilChanged { equals_fn_id } => {
                self.fire_op_distinct(node_id, equals_fn_id);
            }
            OperatorOp::Pairwise { fn_id } => self.fire_op_pairwise(node_id, fn_id),
            OperatorOp::Combine { pack_fn } => self.fire_op_combine(node_id, pack_fn),
            OperatorOp::WithLatestFrom { pack_fn } => {
                self.fire_op_with_latest_from(node_id, pack_fn);
            }
            OperatorOp::Merge => self.fire_op_merge(node_id),
            OperatorOp::Take { count } => self.fire_op_take(node_id, count),
            OperatorOp::Skip { count } => self.fire_op_skip(node_id, count),
            OperatorOp::TakeWhile { fn_id } => self.fire_op_take_while(node_id, fn_id),
            // The variant carries `default` for `register_operator`'s
            // `make_op_scratch` path; once registered, the live default
            // is read from `LastState::default` inside `fire_op_last`.
            OperatorOp::Last { .. } => self.fire_op_last(node_id),
        }
    }

    /// Snapshot the operator's single dep batch (transform constraint —
    /// R5.7 single-dep). Returns `(inputs, terminal)` where `inputs` is a
    /// fresh `Vec<HandleId>` (no retains) and `terminal` reflects
    /// `dep_records[0].terminal` at snapshot time.
    fn snapshot_op_dep0(&self, node_id: NodeId) -> (Vec<HandleId>, Option<TerminalKind>) {
        let s = self.lock_state();
        let rec = s.require_node(node_id);
        debug_assert!(
            !rec.dep_records.is_empty(),
            "transform operator must have ≥1 dep"
        );
        let dr = &rec.dep_records[0];
        (dr.data_batch.iter().copied().collect(), dr.terminal)
    }

    /// Emit DIRTY (if not already dirty) followed by RESOLVED. Used by
    /// silent-drop operators (Filter / DistinctUntilChanged / Pairwise)
    /// when a wave's inputs all suppress and the operator needs to settle
    /// the wave for its subscribers (D018 — let DIRTY ride; queue RESOLVED
    /// on full-reject).
    fn settle_dirty_resolved(&self, node_id: NodeId) {
        let mut s = self.lock_state();
        if s.require_node(node_id).terminal.is_some() {
            return;
        }
        let already_dirty = s.require_node(node_id).dirty;
        s.require_node_mut(node_id).dirty = true;
        if !already_dirty {
            self.queue_notify(&mut s, node_id, Message::Dirty);
        }
        // Slice G: skip Resolved if pending_notify already has a tier-3
        // message — adding Resolved would violate R1.3.3.a.
        let already_tier3 = s
            .pending_notify
            .get(&node_id)
            .is_some_and(|entry| entry.iter_messages().any(|m| m.tier() == 3));
        if !already_tier3 {
            self.queue_notify(&mut s, node_id, Message::Resolved);
        }
    }

    /// `OperatorOp::Map` dispatch.
    fn fire_op_map(&self, node_id: NodeId, fn_id: crate::handle::FnId) {
        let (inputs, _terminal) = self.snapshot_op_dep0(node_id);
        // Mark fired regardless of input count (activation gate already
        // satisfied or partial-mode).
        {
            let mut s = self.lock_state();
            s.require_node_mut(node_id).has_fired_once = true;
        }
        if inputs.is_empty() {
            return;
        }
        // Phase 2 (lock-released): bulk project. Binding returns one
        // handle per input, each with a retain share already taken.
        let outputs = self.binding.project_each(fn_id, &inputs);
        // Phase 3: emit each output. `commit_emission_verbatim` consumes
        // the retain into the cache slot (and releases the prior cache
        // handle internally).
        for h in outputs {
            self.commit_emission_verbatim(node_id, h);
        }
    }

    /// `OperatorOp::Filter` dispatch (D012/D018).
    fn fire_op_filter(&self, node_id: NodeId, fn_id: crate::handle::FnId) {
        let (inputs, _terminal) = self.snapshot_op_dep0(node_id);
        {
            let mut s = self.lock_state();
            s.require_node_mut(node_id).has_fired_once = true;
        }
        if inputs.is_empty() {
            return;
        }
        // Phase 2: predicate per input.
        let pass = self.binding.predicate_each(fn_id, &inputs);
        debug_assert!(
            pass.len() == inputs.len(),
            "predicate_each returned {} bools for {} inputs",
            pass.len(),
            inputs.len()
        );
        // Phase 3: emit passing items verbatim. Take a fresh retain for
        // each — the data_batch slot still owns its retain (released at
        // wave-end rotation), and the cache slot needs its own.
        let mut emitted = 0usize;
        for (i, &h) in inputs.iter().enumerate() {
            if pass.get(i).copied().unwrap_or(false) {
                self.binding.retain_handle(h);
                self.commit_emission_verbatim(node_id, h);
                emitted += 1;
            }
        }
        // D018: full-reject settles with DIRTY+RESOLVED.
        if emitted == 0 {
            self.settle_dirty_resolved(node_id);
        }
    }

    /// `OperatorOp::Scan` dispatch — left-fold emitting each new acc.
    fn fire_op_scan(&self, node_id: NodeId, fn_id: crate::handle::FnId) {
        use crate::op_state::ScanState;
        let (inputs, _terminal) = self.snapshot_op_dep0(node_id);
        let acc = {
            let s = self.lock_state();
            scratch_ref::<ScanState>(&s, node_id).acc
        };
        {
            let mut s = self.lock_state();
            s.require_node_mut(node_id).has_fired_once = true;
        }
        if inputs.is_empty() {
            return;
        }
        // Phase 2: fold each input through. Returns N new handles, each
        // with a fresh retain.
        let new_states = self.binding.fold_each(fn_id, acc, &inputs);
        debug_assert!(
            new_states.len() == inputs.len(),
            "fold_each returned {} accs for {} inputs",
            new_states.len(),
            inputs.len()
        );
        // Phase 3a: update ScanState.acc to the LAST new acc. Take an
        // extra retain for the slot; release the prior acc's slot retain.
        let last_acc = new_states.last().copied();
        if let Some(last) = last_acc {
            let prev_acc = {
                let mut s = self.lock_state();
                let scratch = scratch_mut::<ScanState>(&mut s, node_id);
                let prev = scratch.acc;
                scratch.acc = last;
                prev
            };
            // Take the slot's retain on the new acc.
            self.binding.retain_handle(last);
            // Release the prior slot's retain (post-lock to keep binding
            // free to re-enter Core safely).
            if prev_acc != crate::handle::NO_HANDLE {
                self.binding.release_handle(prev_acc);
            }
        }
        // Phase 3b: emit each intermediate acc verbatim. `new_states`
        // entries each carry one retain from `fold_each`; that retain is
        // consumed by `commit_emission_verbatim` into the cache slot.
        for h in new_states {
            self.commit_emission_verbatim(node_id, h);
        }
    }

    /// `OperatorOp::Reduce` dispatch — accumulates silently; emits acc on
    /// upstream COMPLETE (cascades ERROR verbatim).
    fn fire_op_reduce(&self, node_id: NodeId, fn_id: crate::handle::FnId) {
        use crate::op_state::ReduceState;
        let (inputs, terminal) = self.snapshot_op_dep0(node_id);
        let acc = {
            let s = self.lock_state();
            scratch_ref::<ReduceState>(&s, node_id).acc
        };
        {
            let mut s = self.lock_state();
            s.require_node_mut(node_id).has_fired_once = true;
        }
        // Phase 2: accumulate (silent — no per-input emit).
        let new_states = if inputs.is_empty() {
            SmallVec::<[HandleId; 1]>::new()
        } else {
            self.binding.fold_each(fn_id, acc, &inputs)
        };
        debug_assert!(
            new_states.len() == inputs.len(),
            "fold_each returned {} accs for {} inputs",
            new_states.len(),
            inputs.len()
        );
        // Update ReduceState.acc to last new acc; release intermediate
        // states (we don't emit them) and the prior acc's slot retain.
        let last_acc = new_states.last().copied();
        let intermediates_to_release: Vec<HandleId> = if new_states.len() > 1 {
            new_states[..new_states.len() - 1].to_vec()
        } else {
            Vec::new()
        };
        let prev_acc_to_release = if let Some(last) = last_acc {
            let prev_acc = {
                let mut s = self.lock_state();
                let scratch = scratch_mut::<ReduceState>(&mut s, node_id);
                let prev = scratch.acc;
                scratch.acc = last;
                prev
            };
            self.binding.retain_handle(last);
            if prev_acc == crate::handle::NO_HANDLE {
                None
            } else {
                Some(prev_acc)
            }
        } else {
            None
        };
        // Release intermediate fold results (Reduce only emits the LAST,
        // but only on terminal). Each was retained by `fold_each`.
        for h in intermediates_to_release {
            self.binding.release_handle(h);
        }
        if let Some(h) = prev_acc_to_release {
            self.binding.release_handle(h);
        }

        // Phase 3: emit on terminal.
        match terminal {
            None => {
                // Still accumulating; no emit. Subscribers see no message
                // for this wave (silent accumulation). The first wave that
                // pushes Reduce to fire produces a Dirty entry on the
                // upstream's commit, but Reduce itself doesn't queue any
                // tier-3 since R5 silently absorbs. v1: leave the
                // post-drain auto-resolve sweep to settle nothing —
                // pending_notify has no entry for Reduce so the sweep is
                // a no-op.
            }
            Some(TerminalKind::Complete) => {
                // Read the live acc (may be the seed if no DATA arrived)
                // and emit Data(acc) + Complete.
                let final_acc = {
                    let s = self.lock_state();
                    scratch_ref::<ReduceState>(&s, node_id).acc
                };
                if final_acc != crate::handle::NO_HANDLE {
                    // Emission needs its own retain (slot's retain is
                    // owned by ReduceState.acc until reset/Drop).
                    self.binding.retain_handle(final_acc);
                    self.commit_emission_verbatim(node_id, final_acc);
                }
                self.complete(node_id);
            }
            Some(TerminalKind::Error(h)) => {
                // Core::error transfers the caller's share into the
                // cascade (node.terminal + per-child dep_terminal slots);
                // no release at the error() boundary. Take a fresh share
                // here so the cascade owns it independently of the
                // dep_records[0].terminal slot's share.
                self.binding.retain_handle(h);
                self.error(node_id, h);
            }
        }
    }

    /// `OperatorOp::DistinctUntilChanged` dispatch.
    fn fire_op_distinct(&self, node_id: NodeId, equals_fn_id: crate::handle::FnId) {
        use crate::op_state::DistinctState;
        let (inputs, _terminal) = self.snapshot_op_dep0(node_id);
        let mut prev = {
            let s = self.lock_state();
            scratch_ref::<DistinctState>(&s, node_id).prev
        };
        {
            let mut s = self.lock_state();
            s.require_node_mut(node_id).has_fired_once = true;
        }
        if inputs.is_empty() {
            return;
        }
        // Take a working-copy retain on the initial prev so both the loop
        // (which releases old_prev on each non-equal item) and phase 3
        // (which releases the slot's original handle) each have their own
        // share. Without this, the loop's release of old_prev (== original
        // DistinctState.prev) double-releases against phase 3's stale_slot
        // release.
        if prev != crate::handle::NO_HANDLE {
            self.binding.retain_handle(prev);
        }
        // Phase 2: per-input equals(prev, current). Each non-equal input
        // is emitted and becomes the new prev. Equals fn_id reuses
        // `BindingBoundary::custom_equals`.
        let mut emitted = 0usize;
        for &h in &inputs {
            let equal = if prev == crate::handle::NO_HANDLE {
                false
            } else if prev == h {
                true
            } else {
                self.binding.custom_equals(equals_fn_id, prev, h)
            };
            if !equal {
                // Emit this input verbatim.
                self.binding.retain_handle(h);
                self.commit_emission_verbatim(node_id, h);
                // Update prev: take retain on new prev, release old
                // (working-copy retain from above or from prior iteration).
                self.binding.retain_handle(h);
                let old_prev = prev;
                prev = h;
                if old_prev != crate::handle::NO_HANDLE {
                    self.binding.release_handle(old_prev);
                }
                emitted += 1;
            }
        }
        // Phase 3: persist prev into DistinctState.prev slot. Release the
        // slot's original retain (stale_slot) — this is the slot-owned
        // share, independent of the working-copy share released in the
        // loop above.
        {
            let mut s = self.lock_state();
            let scratch = scratch_mut::<DistinctState>(&mut s, node_id);
            let stale_slot = scratch.prev;
            scratch.prev = prev;
            if stale_slot != prev && stale_slot != crate::handle::NO_HANDLE {
                drop(s);
                self.binding.release_handle(stale_slot);
            }
        }
        // Release the working-copy retain on the final prev if it was
        // never released in the loop (i.e. no non-equal items passed,
        // prev == original). In that case stale_slot == prev, so phase 3
        // didn't release it either — but the working-copy retain is still
        // outstanding. Release it now.
        if emitted == 0 && prev != crate::handle::NO_HANDLE {
            self.binding.release_handle(prev);
        }
        if emitted == 0 {
            self.settle_dirty_resolved(node_id);
        }
    }

    /// `OperatorOp::Pairwise` dispatch — emits `(prev, current)` tuples
    /// starting after the second value (first input swallowed, sets `prev`).
    fn fire_op_pairwise(&self, node_id: NodeId, fn_id: crate::handle::FnId) {
        use crate::op_state::PairwiseState;
        let (inputs, _terminal) = self.snapshot_op_dep0(node_id);
        let mut prev = {
            let s = self.lock_state();
            scratch_ref::<PairwiseState>(&s, node_id).prev
        };
        {
            let mut s = self.lock_state();
            s.require_node_mut(node_id).has_fired_once = true;
        }
        if inputs.is_empty() {
            return;
        }
        let mut emitted = 0usize;
        for &h in &inputs {
            if prev == crate::handle::NO_HANDLE {
                // First-ever value — swallow, set prev. Retain for the
                // PairwiseState.prev slot (persisted in phase 3 below).
                self.binding.retain_handle(h);
                prev = h;
                continue;
            }
            // Pack (prev, current) into a tuple handle. Binding returns a
            // fresh retain on the packed handle.
            let packed = self.binding.pairwise_pack(fn_id, prev, h);
            self.commit_emission_verbatim(node_id, packed);
            // Advance prev: take retain on h, release old prev.
            self.binding.retain_handle(h);
            let old_prev = prev;
            prev = h;
            self.binding.release_handle(old_prev);
            emitted += 1;
        }
        // Persist prev into PairwiseState.prev slot.
        {
            let mut s = self.lock_state();
            let scratch = scratch_mut::<PairwiseState>(&mut s, node_id);
            let stale_slot = scratch.prev;
            scratch.prev = prev;
            if stale_slot != prev && stale_slot != crate::handle::NO_HANDLE {
                drop(s);
                self.binding.release_handle(stale_slot);
            }
        }
        if emitted == 0 {
            self.settle_dirty_resolved(node_id);
        }
    }

    // =================================================================
    // Slice C-2: multi-dep combinator operators (D020)
    // =================================================================

    /// Snapshot all deps' "latest" handle for multi-dep combinators.
    /// For each dep: returns `data_batch.last()` if non-empty (dep fired
    /// this wave), else `prev_data` (last handle from previous wave).
    /// Also returns whether dep[0] (primary) had DATA this wave —
    /// needed by `fire_op_with_latest_from`.
    fn snapshot_op_all_latest(&self, node_id: NodeId) -> (SmallVec<[HandleId; 4]>, bool) {
        let s = self.lock_state();
        let rec = s.require_node(node_id);
        let primary_fired = rec
            .dep_records
            .first()
            .is_some_and(|dr| !dr.data_batch.is_empty());
        let latest: SmallVec<[HandleId; 4]> = rec
            .dep_records
            .iter()
            .map(|dr| dr.data_batch.last().copied().unwrap_or(dr.prev_data))
            .collect();
        (latest, primary_fired)
    }

    /// `OperatorOp::Combine` dispatch — N-dep combineLatest. Packs the
    /// latest handle per dep into a tuple via `pack_tuple`, emits on
    /// any dep fire. First-run gate (R2.5.3, partial: false) guarantees
    /// all deps have a real handle on first fire. Post-warmup INVALIDATE
    /// guard: if any dep's prev_data was cleared, settles with RESOLVED
    /// instead of packing a NO_HANDLE into the tuple.
    fn fire_op_combine(&self, node_id: NodeId, pack_fn: crate::handle::FnId) {
        let (latest, _primary_fired) = self.snapshot_op_all_latest(node_id);
        {
            let mut s = self.lock_state();
            s.require_node_mut(node_id).has_fired_once = true;
        }
        // Post-warmup INVALIDATE guard: a dep may have been invalidated
        // (prev_data cleared to NO_HANDLE) and not yet re-delivered.
        if latest.contains(&crate::handle::NO_HANDLE) {
            self.settle_dirty_resolved(node_id);
            return;
        }
        let tuple_handle = self.binding.pack_tuple(pack_fn, &latest);
        self.commit_emission_verbatim(node_id, tuple_handle);
    }

    /// `OperatorOp::WithLatestFrom` dispatch — 2-dep, fire-on-primary-only
    /// (D021 / Phase 10.5). Emits `[primary, secondary]` pair only when
    /// dep[0] (primary) has DATA in the wave. If only dep[1] fires →
    /// RESOLVED. Post-warmup INVALIDATE guard: if secondary latest is
    /// `NO_HANDLE` (INVALIDATE cleared it), settles with RESOLVED.
    fn fire_op_with_latest_from(&self, node_id: NodeId, pack_fn: crate::handle::FnId) {
        let (latest, primary_fired) = self.snapshot_op_all_latest(node_id);
        let first_fire = {
            let mut s = self.lock_state();
            let rec = s.require_node_mut(node_id);
            let was_first = !rec.has_fired_once;
            rec.has_fired_once = true;
            was_first
        };
        // On first fire (gate release), always emit — the first-run gate
        // guarantees both deps have values (via prev_data fallback in
        // snapshot). On subsequent fires, only emit when primary fires.
        if !first_fire && !primary_fired {
            // Secondary-only update — no downstream DATA.
            self.settle_dirty_resolved(node_id);
            return;
        }
        // Post-warmup INVALIDATE guard: secondary may have been invalidated
        // (prev_data cleared to NO_HANDLE) and not yet re-delivered.
        debug_assert!(latest.len() == 2, "withLatestFrom requires exactly 2 deps");
        if latest[1] == crate::handle::NO_HANDLE {
            self.settle_dirty_resolved(node_id);
            return;
        }
        let tuple_handle = self.binding.pack_tuple(pack_fn, &latest);
        self.commit_emission_verbatim(node_id, tuple_handle);
    }

    /// `OperatorOp::Merge` dispatch — N-dep, forward all DATA handles
    /// verbatim (D022). Zero FFI on fire: no transformation. Each dep's
    /// batch handles are collected, retained, and emitted individually.
    fn fire_op_merge(&self, node_id: NodeId) {
        // Collect all batch handles from all deps (flat).
        let all_handles: Vec<HandleId> = {
            let s = self.lock_state();
            let rec = s.require_node(node_id);
            rec.dep_records
                .iter()
                .flat_map(|dr| dr.data_batch.iter().copied())
                .collect()
        };
        {
            let mut s = self.lock_state();
            s.require_node_mut(node_id).has_fired_once = true;
        }
        if all_handles.is_empty() {
            // All deps settled RESOLVED this wave — no DATA to forward.
            self.settle_dirty_resolved(node_id);
            return;
        }
        // Emit each handle verbatim. Take a fresh retain per handle
        // (independent of the dep batch's retain which gets released at
        // wave-end). Matches Filter's discipline for passing inputs.
        for &h in &all_handles {
            self.binding.retain_handle(h);
            self.commit_emission_verbatim(node_id, h);
        }
    }

    // =================================================================
    // Slice C-3: flow operators (D024)
    // =================================================================

    /// `OperatorOp::Take` dispatch — emits the first `count` DATA values
    /// then self-completes via `Core::complete`. When `count == 0`, the
    /// first fire emits zero items then immediately self-completes
    /// (D027). Cross-wave counter lives in
    /// [`TakeState::count_emitted`](super::op_state::TakeState::count_emitted).
    fn fire_op_take(&self, node_id: NodeId, count: u32) {
        use crate::op_state::TakeState;
        let (inputs, terminal) = self.snapshot_op_dep0(node_id);
        // Snapshot current counter; mark fired regardless of input count
        // (activation gate already satisfied or partial-mode).
        let mut count_emitted = {
            let s = self.lock_state();
            scratch_ref::<TakeState>(&s, node_id).count_emitted
        };
        {
            let mut s = self.lock_state();
            s.require_node_mut(node_id).has_fired_once = true;
        }
        // Already at quota before any input this wave — self-complete
        // immediately. Covers `count == 0` (first-fire short-circuit) and
        // any defensive re-entry after the terminal-skip in `fire_operator`
        // already guards against double-complete.
        if count_emitted >= count {
            self.complete(node_id);
            return;
        }
        // Per-input emission loop. Each pass takes a fresh retain for the
        // cache slot; data_batch slot's retain is released at wave-end
        // rotation independently.
        for &h in &inputs {
            self.binding.retain_handle(h);
            self.commit_emission_verbatim(node_id, h);
            count_emitted = count_emitted.saturating_add(1);
            if count_emitted >= count {
                break;
            }
        }
        // Persist the updated counter.
        {
            let mut s = self.lock_state();
            scratch_mut::<TakeState>(&mut s, node_id).count_emitted = count_emitted;
        }
        // Self-complete if we hit the quota this wave. Upstream COMPLETE
        // (terminal == Some(Complete)) without us hitting the count
        // propagates via the standard auto-cascade — we don't intercept it.
        if count_emitted >= count {
            self.complete(node_id);
            return;
        }
        // If upstream is already Errored and we haven't hit count, the
        // standard cascade will propagate it. If the wave delivered no
        // inputs (e.g. RESOLVED from upstream), settle DIRTY+RESOLVED so
        // subscribers see the wave close.
        if inputs.is_empty() && terminal.is_none() {
            self.settle_dirty_resolved(node_id);
        }
    }

    /// `OperatorOp::Skip` dispatch — drops the first `count` DATA values,
    /// then forwards the rest. Cross-wave counter lives in
    /// [`SkipState::count_skipped`](super::op_state::SkipState::count_skipped).
    /// On a wave where every input is still in the skip window, settles
    /// DIRTY+RESOLVED (D018 pattern) so subscribers see the wave close.
    fn fire_op_skip(&self, node_id: NodeId, count: u32) {
        use crate::op_state::SkipState;
        let (inputs, _terminal) = self.snapshot_op_dep0(node_id);
        let mut count_skipped = {
            let s = self.lock_state();
            scratch_ref::<SkipState>(&s, node_id).count_skipped
        };
        {
            let mut s = self.lock_state();
            s.require_node_mut(node_id).has_fired_once = true;
        }
        // No early-return on empty inputs: the post-loop `emitted == 0`
        // settle handles the empty-inputs case identically to the
        // all-swallowed-by-skip-window case (Slice C-3 /qa P6 — symmetry
        // with `fire_op_take`).
        let mut emitted = 0usize;
        for &h in &inputs {
            if count_skipped < count {
                count_skipped = count_skipped.saturating_add(1);
                // Drop this input — the data_batch slot still owns its
                // retain (released at wave-end rotation). No emission.
                continue;
            }
            // Past the skip window — emit verbatim. Take a fresh retain
            // for the cache slot.
            self.binding.retain_handle(h);
            self.commit_emission_verbatim(node_id, h);
            emitted += 1;
        }
        // Persist the updated counter.
        {
            let mut s = self.lock_state();
            scratch_mut::<SkipState>(&mut s, node_id).count_skipped = count_skipped;
        }
        if emitted == 0 {
            self.settle_dirty_resolved(node_id);
        }
    }

    /// `OperatorOp::TakeWhile` dispatch — emits while the predicate
    /// holds; on the first `false`, emits any preceding passes from the
    /// same batch then self-completes via `Core::complete`. Reuses
    /// [`BindingBoundary::predicate_each`] (D029).
    fn fire_op_take_while(&self, node_id: NodeId, fn_id: crate::handle::FnId) {
        let (inputs, _terminal) = self.snapshot_op_dep0(node_id);
        {
            let mut s = self.lock_state();
            s.require_node_mut(node_id).has_fired_once = true;
        }
        if inputs.is_empty() {
            return;
        }
        // Phase 2: predicate per input.
        let pass = self.binding.predicate_each(fn_id, &inputs);
        debug_assert!(
            pass.len() == inputs.len(),
            "predicate_each returned {} bools for {} inputs",
            pass.len(),
            inputs.len()
        );
        // Phase 3: emit each input until the first false; then
        // self-complete. `fire_operator`'s `terminal.is_some()`
        // short-circuit gates re-entry after the self-complete cascade
        // installs the terminal slot — no extra `done` flag needed.
        let mut emitted = 0usize;
        let mut first_false_seen = false;
        for (i, &h) in inputs.iter().enumerate() {
            if pass.get(i).copied().unwrap_or(false) {
                self.binding.retain_handle(h);
                self.commit_emission_verbatim(node_id, h);
                emitted += 1;
            } else {
                first_false_seen = true;
                break;
            }
        }
        if first_false_seen {
            self.complete(node_id);
            return;
        }
        if emitted == 0 {
            // Whole batch passed but was empty (impossible here since
            // inputs.is_empty() returned early above) — defensive only.
            self.settle_dirty_resolved(node_id);
        }
    }

    /// `OperatorOp::Last` dispatch — buffers the latest DATA; emits
    /// `Data(latest)` (or `Data(default)` if no DATA arrived and a
    /// default was registered) then `Complete` on upstream COMPLETE.
    /// On upstream ERROR, propagates verbatim. Storage:
    /// [`LastState`](super::op_state::LastState).
    ///
    /// **Silent-buffer semantics (mirrors Reduce):** on a non-terminal
    /// wave (`terminal == None`), `fire_op_last` updates the buffered
    /// `latest` handle but produces NO downstream wire message —
    /// subscribers observe the operator only when upstream
    /// COMPLETE/ERROR triggers the terminal branch. Intermediate
    /// inputs from the dep's batch are dropped on the floor (their
    /// `data_batch` retains release at wave-end rotation
    /// independently). Per-wave settlement on intermediate waves is
    /// the canonical behavior for terminal-aware operators.
    fn fire_op_last(&self, node_id: NodeId) {
        use crate::op_state::LastState;
        let (inputs, terminal) = self.snapshot_op_dep0(node_id);
        {
            let mut s = self.lock_state();
            s.require_node_mut(node_id).has_fired_once = true;
        }

        // Phase 2: buffer the latest input handle (if any). Retain new,
        // release old. data_batch slot's retain is released at wave-end
        // rotation independently — the LastState slot keeps its own
        // share so the value survives across waves.
        if let Some(&new_latest) = inputs.last() {
            let prev_latest = {
                let mut s = self.lock_state();
                let scratch = scratch_mut::<LastState>(&mut s, node_id);
                let prev = scratch.latest;
                scratch.latest = new_latest;
                prev
            };
            self.binding.retain_handle(new_latest);
            if prev_latest != crate::handle::NO_HANDLE {
                self.binding.release_handle(prev_latest);
            }
        }

        // Phase 3: emit on terminal. Buffer-only fires (no terminal yet)
        // produce no downstream message — Reduce-style silent
        // accumulation. The post-drain auto-resolve sweep is a no-op
        // because pending_notify has no entry for Last.
        match terminal {
            None => {}
            Some(TerminalKind::Complete) => {
                // Read the live latest + default. If latest != NO_HANDLE,
                // emit it. Otherwise, if default != NO_HANDLE, emit default.
                // Otherwise, emit only Complete (empty stream, no default).
                let (latest, default) = {
                    let s = self.lock_state();
                    let scratch = scratch_ref::<LastState>(&s, node_id);
                    (scratch.latest, scratch.default)
                };
                let to_emit = if latest != crate::handle::NO_HANDLE {
                    Some(latest)
                } else if default != crate::handle::NO_HANDLE {
                    Some(default)
                } else {
                    None
                };
                if let Some(h) = to_emit {
                    // Emission needs its own retain — the LastState slot
                    // keeps its share until reset/Drop.
                    self.binding.retain_handle(h);
                    self.commit_emission_verbatim(node_id, h);
                }
                self.complete(node_id);
            }
            Some(TerminalKind::Error(h)) => {
                // Take a fresh share for the error cascade — the
                // dep_records[0].terminal slot keeps its own share
                // (released by reset_for_fresh_lifecycle / Drop).
                self.binding.retain_handle(h);
                self.error(node_id, h);
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

        let is_dynamic;
        let is_state;
        let tracked_or_first_fire;
        // Slice F audit close (2026-05-07): default-mode pause suppression.
        // If the consumer is paused with `PausableMode::Default`, the
        // canonical-spec §2.6 behavior is to suppress fn-fire and consolidate
        // pause-window dep deliveries into one fn execution on RESUME.
        // Mark `pending_wave` on the pause state instead of adding to
        // `pending_fires`. The dep state still advances (the data_batch push
        // above is unchanged), and clear_wave_state still rotates the latest
        // dep DATA into prev_data — so when the fn ultimately fires on
        // RESUME, it sees the consolidated post-pause state.
        let suppressed_for_default_pause;
        {
            let consumer = s.require_node_mut(consumer_id);
            consumer.dep_records[dep_idx].data_batch.push(handle);
            consumer.dep_records[dep_idx].involved_this_wave = true;
            consumer.involved_this_wave = true;
            is_dynamic = consumer.is_dynamic;
            is_state = consumer.is_state();
            tracked_or_first_fire = !consumer.has_fired_once || consumer.tracked.contains(&dep_idx);
            suppressed_for_default_pause = consumer.pause_state.is_paused()
                && consumer.pausable == crate::node::PausableMode::Default;
            if suppressed_for_default_pause {
                consumer.pause_state.mark_pending_wave();
            }
        }
        if suppressed_for_default_pause {
            // Default-mode pause: don't add to pending_fires; RESUME will
            // schedule one consolidated fire.
            return;
        }
        if is_state {
            // State nodes don't have deps; unreachable in practice.
        } else if is_dynamic {
            if tracked_or_first_fire {
                s.pending_fires.insert(consumer_id);
            }
        } else {
            // Derived / Operator / Producer (Producer has no deps so won't
            // reach here, but the predicate-based dispatch handles it
            // uniformly).
            s.pending_fires.insert(consumer_id);
        }
    }

    // -------------------------------------------------------------------
    // Subscriber notification
    // -------------------------------------------------------------------

    /// Queue a wave-end message for `node_id`'s subscribers.
    ///
    /// **Revision-tracked sink-snapshot batches (Slice X4 / D2,
    /// 2026-05-08):** each push for a given node either appends the
    /// message to the open batch (if `NodeRecord::subscribers_revision`
    /// hasn't advanced since that batch opened — the common case — no
    /// extra allocation), or opens a fresh batch with a current sink
    /// snapshot frozen at the new revision. A sub installed mid-wave
    /// bumps `subscribers_revision`; the next `queue_notify` for the
    /// same node observes the bump and starts a new batch that includes
    /// the new sub. Pre-subscribe batches retain their original snapshot,
    /// so earlier emits flush to their original sink list — the new sub
    /// does NOT double-receive them via flush AND handshake replay,
    /// closing the late-subscriber + multi-emit-per-wave R1.3.5.a gap.
    ///
    /// Pause routing decision (R1.3.7.b tier table, §10.2 buffering):
    ///   Tier 3 (DATA / RESOLVED) and Tier 4 (INVALIDATE) buffer while
    ///   paused; all other tiers (DIRTY tier 1, PAUSE/RESUME tier 2,
    ///   COMPLETE/ERROR tier 5, TEARDOWN tier 6) bypass the buffer and
    ///   flush immediately. START (tier 0) is per-subscription and never
    ///   transits queue_notify.
    pub(crate) fn queue_notify(&self, s: &mut CoreState, node_id: NodeId, msg: Message) {
        // R1.3.3.a / R1.3.3.d (Slice G — re-added 2026-05-07): dev-mode
        // wave-content invariant assertion. The tier-3 slot at one node in
        // one wave is either ≥1 DATA or exactly 1 RESOLVED — never mixed,
        // never multiple RESOLVED. Slice G moved equals substitution from
        // per-emit to wave-end coalescing; this assert pins that the
        // dispatcher itself never queues a violating combination at the
        // queue_notify granularity. Resolved arrivals come from:
        //   1. The auto-resolve sweep in `drain_and_flush` (gates on
        //      `!any tier-3` so it can't add to a wave with Data).
        //   2. The wave-end equals-substitution pass (rewrites in place,
        //      doesn't go through queue_notify).
        // Both honor R1.3.3.a by construction post-Slice-G.
        #[cfg(debug_assertions)]
        if matches!(msg.tier(), 3) {
            if let Some(entry) = s.pending_notify.get(&node_id) {
                // Walk all batches' messages — R1.3.3.a is a per-node
                // wave-content invariant, not per-batch (the X4 batches
                // are subscriber-snapshot epochs; the protocol-level
                // tier-3 invariant spans the whole wave for the node).
                let has_data = entry.iter_messages().any(|m| matches!(m, Message::Data(_)));
                let resolved_count = entry
                    .iter_messages()
                    .filter(|m| matches!(m, Message::Resolved))
                    .count();
                let incoming_is_data = matches!(msg, Message::Data(_));
                if incoming_is_data {
                    debug_assert!(
                        resolved_count == 0,
                        "R1.3.3.a violation at {node_id:?}: queueing Data into a \
                         wave that already contains Resolved — Slice G should have \
                         prevented this via wave-end coalescing"
                    );
                } else {
                    debug_assert!(
                        !has_data,
                        "R1.3.3.a violation at {node_id:?}: queueing Resolved into a \
                         wave that already contains Data"
                    );
                    debug_assert!(
                        resolved_count == 0,
                        "R1.3.3.a violation at {node_id:?}: multiple Resolved in one \
                         wave at one node"
                    );
                }
            }
        }

        let buffered_tier = matches!(msg.tier(), 3 | 4);
        let cap = s.pause_buffer_cap;

        // Pause-routing branch — handles its own retain/release and returns
        // before we touch `pending_notify`, so the rec borrow is contained.
        {
            let rec = s.require_node_mut(node_id);
            if rec.subscribers.is_empty() {
                return;
            }
            // Slice F audit close (2026-05-07): pause routing depends on mode.
            //   - `ResumeAll`: buffer tier-3/4 for verbatim replay on RESUME.
            //   - `Default` + STATE node: state nodes have no fn-fire to
            //     suppress, so buffer like resumeAll (collapse-to-latest is
            //     a future enhancement; v1 keeps verbatim).
            //   - `Default` + COMPUTE node: suppression happens upstream at
            //     fn-fire scheduling (see `deliver_data_to_consumer`); no
            //     outgoing tier-3 is produced from this node while paused,
            //     so this branch is unreachable for compute-default-paused.
            //     Fallthrough to the non-paused queue path is fine.
            //   - `Off`: pause is ignored entirely — tier-3 flushes
            //     immediately. Fallthrough.
            let mode_buffers_tier3 = match rec.pausable {
                crate::node::PausableMode::ResumeAll => true,
                crate::node::PausableMode::Default => rec.is_state(),
                crate::node::PausableMode::Off => false,
            };
            if buffered_tier && mode_buffers_tier3 && rec.pause_state.is_paused() {
                if let Some(h) = msg.payload_handle() {
                    self.binding.retain_handle(h);
                }
                let push_result = rec.pause_state.push_buffered(msg, cap);
                for dm in push_result.dropped_msgs {
                    if let Some(h) = dm.payload_handle() {
                        self.binding.release_handle(h);
                    }
                }
                // R1.3.8.c (Slice F, A3): on first overflow this cycle,
                // schedule a synthesized ERROR for wave-end emission.
                // `cap` is `Some` here (an overflow can only happen with a
                // configured cap), so `unwrap` is safe.
                if push_result.first_overflow_this_cycle {
                    if let Some((dropped_count, lock_held_ns)) =
                        rec.pause_state.overflow_diagnostic()
                    {
                        s.pending_pause_overflow
                            .push(crate::node::PendingPauseOverflow {
                                node_id,
                                dropped_count,
                                configured_max: cap.unwrap_or(0),
                                lock_held_ns,
                            });
                    }
                }
                return;
            }
        }

        // Non-paused queue path: retain payload handle and queue into
        // pending_notify. Released in `flush_notifications` after sinks
        // fire.
        if let Some(h) = msg.payload_handle() {
            self.binding.retain_handle(h);
        }
        Self::push_into_pending_notify(s, node_id, msg);
    }

    /// Slice X4 / D2: revision-tracked batch decision for `queue_notify`'s
    /// non-paused path. Either appends `msg` to the open batch (if
    /// `subscribers_revision` hasn't advanced since it opened — common
    /// case, no extra allocation) or opens a fresh batch with a current
    /// sink snapshot frozen at the new revision.
    ///
    /// Borrow discipline: reads `subscribers_revision` and the snapshot
    /// from `s.nodes` BEFORE calling `s.pending_notify.entry()` to keep
    /// the two field borrows disjoint (split-borrow through
    /// `require_node_mut` defeats the borrow checker).
    ///
    /// Lock-discipline assumption: this read of `subscribers_revision`
    /// is safe because both the subscribe install path
    /// ([`crate::node::Core::subscribe`]) and `queue_notify` hold
    /// `CoreState`'s mutex when they bump / read the revision —
    /// concurrent subscribe/unsubscribe cannot interleave. **If
    /// `Core::subscribe` ever moves the sink-install lock-released
    /// (mirroring the lock-released drain refactor), the revision read
    /// here must re-validate post-borrow — otherwise a fresh batch
    /// could open with a stale snapshot.**
    fn push_into_pending_notify(s: &mut CoreState, node_id: NodeId, msg: Message) {
        let current_rev = s.require_node(node_id).subscribers_revision;
        let needs_new_batch = s.pending_notify.get(&node_id).is_none_or(|entry| {
            entry
                .batches
                .last()
                .is_none_or(|b| b.snapshot_revision != current_rev)
        });
        let sinks_snapshot: Vec<Sink> = if needs_new_batch {
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
                let mut batches: SmallVec<[PendingBatch; 1]> = SmallVec::new();
                batches.push(PendingBatch {
                    snapshot_revision: current_rev,
                    sinks: sinks_snapshot,
                    messages: vec![msg],
                });
                slot.insert(PendingPerNode { batches });
            }
            Entry::Occupied(mut slot) => {
                let entry = slot.get_mut();
                if needs_new_batch {
                    entry.batches.push(PendingBatch {
                        snapshot_revision: current_rev,
                        sinks: sinks_snapshot,
                        messages: vec![msg],
                    });
                } else {
                    entry
                        .batches
                        .last_mut()
                        .expect("non-empty by construction (entry exists implies batch exists)")
                        .messages
                        .push(msg);
                }
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
                // Slice X4 / D2: iterate batches in arrival order. Each
                // batch carries its own sink snapshot frozen at open-time;
                // a batch's messages flush to ITS sinks only. Within a
                // single (phase, node), batches stay in arrival order so
                // emit-order semantics are preserved across batches.
                for batch in &entry.batches {
                    if batch.sinks.is_empty() {
                        continue;
                    }
                    let phase_msgs: Vec<Message> = batch
                        .messages
                        .iter()
                        .copied()
                        .filter(|m| phase_tiers.contains(&m.tier()))
                        .collect();
                    if phase_msgs.is_empty() {
                        continue;
                    }
                    let sinks_clone: Vec<Sink> = batch.sinks.iter().map(Arc::clone).collect();
                    s.deferred_flush_jobs.push((sinks_clone, phase_msgs));
                }
            }
        }
        // Refcount release: balance the retain done in `queue_notify` for
        // every payload-bearing message that landed in pending_notify
        // (across ALL batches per node). Deferred to post-lock-drop so the
        // binding's release path can't re-enter Core under our lock.
        for entry in pending.values() {
            for msg in entry.iter_messages() {
                if let Some(h) = msg.payload_handle() {
                    s.deferred_handle_releases.push(h);
                }
            }
        }
    }

    /// Take the deferred sink-fire jobs, payload-handle releases,
    /// cleanup-hook fire queue, and pending-wipe queue from `CoreState`.
    /// Callers pair this with a `drop(state_guard)` and a subsequent
    /// [`Self::fire_deferred`] to deliver the wave's sinks + handle
    /// releases + Slice E2 OnInvalidate cleanup hooks + Slice E2 /qa
    /// Q2(b) eager wipe_ctx fires lock-released.
    pub(crate) fn drain_deferred(s: &mut CoreState) -> WaveDeferred {
        (
            std::mem::take(&mut s.deferred_flush_jobs),
            std::mem::take(&mut s.deferred_handle_releases),
            std::mem::take(&mut s.deferred_cleanup_hooks),
            std::mem::take(&mut s.pending_wipes),
        )
    }

    /// Fire deferred sink-fire jobs in collected order, then release the
    /// payload handles owed for messages that landed in `pending_notify`
    /// during the wave, then fire any queued Slice E2 OnInvalidate cleanup
    /// hooks. All three phases run lock-released so:
    /// - Sinks that call back into Core (emit, pause, etc.) re-acquire the
    ///   state lock cleanly and run their own nested wave.
    /// - The binding's `release_handle` path can't deadlock against a
    ///   binding-side mutex held by Core.
    /// - User cleanup closures (invoked via `BindingBoundary::cleanup_for`)
    ///   may safely re-enter Core for unrelated nodes.
    ///
    /// **Cleanup-drain panic discipline (D060):** each `cleanup_for` call
    /// is wrapped in `catch_unwind` so a single binding panic doesn't
    /// short-circuit the per-wave drain. All queued cleanup attempts run;
    /// if any panicked, the LAST panic re-raises after the loop completes
    /// (preserving wave-end discipline while still surfacing failures).
    /// Per D060, Core stays panic-naive about user code — bindings own
    /// their host-language panic policy inside `cleanup_for`; this
    /// `catch_unwind` is purely about drain-don't-short-circuit.
    pub(crate) fn fire_deferred(
        &self,
        jobs: DeferredJobs,
        releases: Vec<HandleId>,
        cleanup_hooks: Vec<(crate::handle::NodeId, crate::boundary::CleanupTrigger)>,
        pending_wipes: Vec<crate::handle::NodeId>,
    ) {
        // Slice E2 /qa P1 (2026-05-07): wrap each sink-fire in
        // `catch_unwind` so a panicking sink doesn't unwind out of
        // `fire_deferred` and drop the queued `releases` +
        // `cleanup_hooks`. Mirrors Slice F audit fix A7's per-tier
        // handshake-fire discipline. Without this guard, a sink panic
        // here would silently leak handle retains AND silently drop
        // OnInvalidate cleanup hooks. AssertUnwindSafe is safe because
        // we re-raise the last panic at the end after running every
        // queued fire — drain ordering is preserved.
        let mut last_panic: Option<Box<dyn std::any::Any + Send>> = None;
        for (sinks, msgs) in jobs {
            for sink in &sinks {
                let sink = sink.clone();
                let msgs_ref = &msgs;
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                    sink(msgs_ref);
                }));
                if let Err(payload) = result {
                    last_panic = Some(payload);
                }
            }
        }
        for h in releases {
            self.binding.release_handle(h);
        }
        // Slice E2 (D060): drain cleanup hooks with per-item panic
        // isolation so the loop always completes. AssertUnwindSafe is
        // safe here because we don't rely on logical state being valid
        // post-panic — the panic propagates anyway after the drain ends.
        for (node_id, trigger) in cleanup_hooks {
            let binding = &self.binding;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                binding.cleanup_for(node_id, trigger);
            }));
            if let Err(payload) = result {
                last_panic = Some(payload);
            }
        }
        // Slice E2 /qa Q2(b) (D069): drain eager wipe_ctx queue with the
        // same per-item panic isolation. Fires AFTER cleanup hooks so a
        // resubscribable node's OnInvalidate (or any tier-3+ cleanup that
        // fires in the same wave) sees pre-wipe binding state if it
        // landed in the same wave as the terminal cascade. Mutually
        // exclusive with `Subscription::Drop`'s direct-fire site, but
        // even concurrent fires are idempotent (binding's `wipe_ctx`
        // calls `HashMap::remove` which is a no-op on absent keys).
        for node_id in pending_wipes {
            let binding = &self.binding;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                binding.wipe_ctx(node_id);
            }));
            if let Err(payload) = result {
                last_panic = Some(payload);
            }
        }
        if let Some(payload) = last_panic {
            std::panic::resume_unwind(payload);
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
    ///
    /// # Panics
    ///
    /// Panics if the registry-epoch retry-validate loop exceeds
    /// [`crate::subgraph::MAX_LOCK_RETRIES`] iterations — pathological
    /// concurrent `register` / `set_deps` activity racing with
    /// closure-form batch entry. Unreachable in correct call paths.
    #[must_use = "BatchGuard drains the wave on drop; assign to a named binding"]
    pub fn begin_batch(&self) -> BatchGuard {
        // Slice Y1 / Phase E (2026-05-08): closure-form batch has no known
        // seed; per session-doc Q7 / D092 it MUST serialize against every
        // currently-existing partition. Acquire each partition's
        // `wave_owner` in ascending [`SubgraphId`] order via the retry-
        // validate primitive. Same-thread re-entry passes through each
        // ReentrantMutex transparently; cross-thread waves on any of the
        // touched partitions block until our `wave_guards` drop.
        //
        // **QA-fix #2 (2026-05-09) — registry epoch retry-validate:** a
        // concurrent `register` / `set_deps`-driven union/split between
        // our `all_partitions_lock_boxes()` snapshot and the post-
        // acquire epoch read changes the partition set. We then retry
        // the whole acquire with the new snapshot. Without this, a
        // partition added after our snapshot would not be held by our
        // batch — breaking the closure-form's "all-partitions
        // serialization" contract.
        //
        // Trade-off (documented v1 contract): closure-form batch is the
        // serialization point under per-partition parallelism. Per-seed
        // entry points (`Core::subscribe`, [`Self::begin_batch_for`])
        // acquire only the touched partitions and run truly parallel
        // for disjoint partitions.
        for _ in 0..crate::subgraph::MAX_LOCK_RETRIES {
            let epoch_before = self.registry.lock().epoch();
            let partition_boxes = self.all_partitions_lock_boxes();
            let mut wave_guards: SmallVec<[crate::node::WaveOwnerGuard; 4]> = SmallVec::new();
            for (sid, _box) in &partition_boxes {
                // Use the partition's root NodeId as the lock_for retry
                // seed. SubgraphId.raw() == root NodeId.raw(); the root
                // is always registered in the X5 / Phase-E substrate
                // (cleanup_node is gated, Phase G activates).
                let representative = crate::handle::NodeId::new(sid.raw());
                wave_guards.push(self.partition_wave_owner_lock_arc(representative));
            }
            // Post-acquire epoch read. If unchanged, our snapshot is
            // still authoritative — every existing partition was held
            // throughout. If changed, drop guards and retry.
            let epoch_after = self.registry.lock().epoch();
            if epoch_after == epoch_before {
                return self.begin_batch_with_guards(wave_guards);
            }
            // Drop guards lock-released so retries don't accumulate.
            drop(wave_guards);
            std::thread::yield_now();
        }
        panic!(
            "Core::begin_batch: exceeded {} retries — pathological concurrent \
             register/union/split activity racing with closure-form batch entry",
            crate::subgraph::MAX_LOCK_RETRIES
        );
    }

    /// Begin a batch scoped to the partitions transitively touched from
    /// `seed`. Walks `s.children` (downstream cascade) + `meta_companions`
    /// (R1.3.9.d TEARDOWN cascade) starting at `seed`, collects every
    /// reachable partition, and acquires each in ascending
    /// [`crate::subgraph::SubgraphId`] order via
    /// [`Core::partition_wave_owner_lock_arc`].
    ///
    /// Two threads with disjoint touched-partition sets run truly
    /// parallel — the per-partition `wave_owner` mutexes don't block
    /// each other. This is the canonical Y1 parallelism win for
    /// per-seed wave-driving entry points (subscribe, emit, pause,
    /// resume, invalidate, complete, error, teardown,
    /// set_deps push-on-subscribe).
    ///
    /// **QA-fix #2 (2026-05-09):** retry-validate the touched-partition
    /// set against the registry epoch — same protection as
    /// [`Self::begin_batch`] but scoped to a per-seed touched set
    /// rather than every partition. Conservative: any registry
    /// mutation (even on a partition unrelated to seed's touched set)
    /// triggers a retry. This avoids a precise "did MY touched set
    /// change?" check at the cost of occasional spurious retries.
    ///
    /// # Panics
    ///
    /// Panics if the registry-epoch retry-validate loop exceeds
    /// [`crate::subgraph::MAX_LOCK_RETRIES`] iterations, OR if
    /// [`Core::partition_wave_owner_lock_arc`] panics on an
    /// unregistered seed. Both are unreachable in correct call paths
    /// (P12 invariant guarantees registry membership matches
    /// `s.nodes`).
    ///
    /// Slice Y1 / Phase E (2026-05-08); QA-fix #2 (2026-05-09).
    #[must_use = "BatchGuard drains the wave on drop; assign to a named binding"]
    pub fn begin_batch_for(&self, seed: crate::handle::NodeId) -> BatchGuard {
        for _ in 0..crate::subgraph::MAX_LOCK_RETRIES {
            let epoch_before = self.registry.lock().epoch();
            let touched = self.compute_touched_partitions(seed);
            let mut wave_guards: SmallVec<[crate::node::WaveOwnerGuard; 4]> = SmallVec::new();
            for sid in &touched {
                let representative = crate::handle::NodeId::new(sid.raw());
                wave_guards.push(self.partition_wave_owner_lock_arc(representative));
            }
            let epoch_after = self.registry.lock().epoch();
            if epoch_after == epoch_before {
                return self.begin_batch_with_guards(wave_guards);
            }
            drop(wave_guards);
            std::thread::yield_now();
        }
        panic!(
            "Core::begin_batch_for(seed={seed:?}): exceeded {} retries — \
             pathological concurrent register/union/split activity racing \
             with per-seed batch entry",
            crate::subgraph::MAX_LOCK_RETRIES
        );
    }

    /// Internal helper: claim `in_tick` and assemble a [`BatchGuard`]
    /// with the supplied (already-acquired) partition wave-owner guards.
    /// `wave_guards` MUST be in ascending [`crate::subgraph::SubgraphId`]
    /// order (the canonical lock-acquisition order) — both
    /// [`Self::begin_batch`] (all-partitions) and
    /// [`Self::begin_batch_for`] (touched-partitions) construct the
    /// vector in that order before calling here.
    fn begin_batch_with_guards(
        &self,
        wave_guards: SmallVec<[crate::node::WaveOwnerGuard; 4]>,
    ) -> BatchGuard {
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
            wave_guards,
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
/// per-`Core` `in_tick` flag AND the per-partition `wave_owner`
/// re-entrant mutex(es) on the calling thread; sending the guard to
/// another thread and dropping it there would clear `in_tick` and
/// release the wave-owner guards from a different thread than the
/// one that acquired them, breaking both the thread-local "I own
/// the wave scope" semantic and `parking_lot::ReentrantMutex`'s
/// ownership invariant. The `wave_guards` field is a `SmallVec` of
/// `!Send` `ArcReentrantMutexGuard<()>`; the `PhantomData<*const ()>`
/// marker is belt-and-suspenders.
///
/// Slice Y1 / Phase E (2026-05-08): the field migrated from a single
/// `ArcReentrantMutexGuard` (legacy Core-global `wave_owner`) to a
/// `SmallVec` of partition wave-owner guards. Closure-form
/// `begin_batch` acquires every current partition (serialization
/// point); `begin_batch_for(seed)` acquires only the transitively-
/// touched partitions (parallel for disjoint sets).
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
    /// Re-entrant mutex guards held for the wave's duration. One entry
    /// per touched partition's `wave_owner`, in ascending
    /// [`crate::subgraph::SubgraphId`] order. Drop releases each guard
    /// (any order — `parking_lot::ReentrantMutex` doesn't care since all
    /// are held by the same thread). Cross-thread waves on any of the
    /// held partitions block until our scope ends; cross-thread waves
    /// on partitions NOT in this vector run truly parallel — the
    /// canonical Y1 parallelism property.
    ///
    /// Each `ArcReentrantMutexGuard<()>` is `!Send`, so the `SmallVec`
    /// (and thus `BatchGuard`) is `!Send` at the type level — sending
    /// across threads would violate `parking_lot::ReentrantMutex`'s
    /// thread-ownership invariant.
    wave_guards: SmallVec<[crate::node::WaveOwnerGuard; 4]>,
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
                // Slice E2 (D061): panic-discard wave drops queued
                // OnInvalidate cleanup hooks SILENTLY. Bindings using
                // OnInvalidate for external-resource cleanup MUST
                // idempotent-cleanup at process exit / next successful
                // invalidate. Mirrors A3 `pending_pause_overflow`
                // panic-discard precedent.
                let _: Vec<(crate::handle::NodeId, crate::boundary::CleanupTrigger)> =
                    std::mem::take(&mut s.deferred_cleanup_hooks);
                // Slice E2 /qa Q2(b) (D069): same panic-discard discipline
                // for the eager-wipe queue. A panic-discarded wave drops
                // queued `wipe_ctx` fires silently; the binding-side
                // `NodeCtxState` entry remains until the next successful
                // terminate-with-no-subs cycle (or until `Core` drops).
                // This mirrors D061's external-resource-cleanup gap and
                // is documented similarly.
                let _: Vec<crate::handle::NodeId> = std::mem::take(&mut s.pending_wipes);
                s.in_tick = false;
                (pending, deferred_releases, restored)
            };
            // Lock dropped — release retains lock-released so the binding
            // can't deadlock against an internal binding mutex.
            for entry in pending.values() {
                for msg in entry.iter_messages() {
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
        let (jobs, releases, cleanup_hooks, pending_wipes) = {
            let mut s = self.core.lock_state();
            s.clear_wave_state();
            s.in_tick = false;
            self.core.commit_wave_cache_snapshots(&mut s);
            // `drain_deferred` takes `deferred_flush_jobs` +
            // `deferred_handle_releases` (incl. rotation releases pushed
            // by `clear_wave_state` above) + Slice E2
            // `deferred_cleanup_hooks` + Slice E2 /qa Q2(b)
            // `pending_wipes`.
            Core::drain_deferred(&mut s)
        };
        // Lock dropped — fire deferred sinks + release retains + fire
        // cleanup hooks (Slice E2 OnInvalidate, D060 catch_unwind drain)
        // + fire eager wipes (D069).
        self.core
            .fire_deferred(jobs, releases, cleanup_hooks, pending_wipes);
        // QA-fix group 2 (2026-05-09): explicitly drop the wave guards
        // in REVERSE acquisition order. `parking_lot::ReentrantMutex`
        // doesn't care about release order for same-thread holders, but
        // a future migration to a non-reentrant lock (or one with a
        // Drop side-effect tied to ordering) would silently break if we
        // relied on `SmallVec`'s default forward-iteration drop. The
        // ascending-acquire / descending-release pattern is the
        // canonical lock-discipline shape.
        while let Some(guard) = self.wave_guards.pop() {
            drop(guard);
        }
    }
}
