//! The dispatcher — node registration, subscription, wave engine.
//!
//! Mirrors `~/src/graphrefly-ts/src/__experiments__/handle-core/core.ts`
//! (the Phase 13.6 brainstorm prototype, ~370 lines, 22 invariant tests).
//!
//! # Scope (M1 dispatcher + Slice A+B parity, closed 2026-05-05)
//!
//! - State + derived + dynamic node registration.
//! - Subscribe / unsubscribe with push-on-subscribe (R1.2.3).
//! - RAII [`Subscription`] with Drop-based deregister (§10.12).
//! - DIRTY → DATA / RESOLVED ordering (R1.3.1.b two-phase push).
//! - Equals-substitution (R1.3.2): identity is zero-FFI; custom crosses boundary.
//! - First-run gate (R2.5.3) — fn does not fire until every dep has a handle.
//! - Diamond resolution — one fn fire per wave even with shared upstream.
//! - `set_deps()` atomic dep mutation with cycle detection + Phase 13.8 Q1
//!   terminal-rejection policy (R3.3.1).
//! - PAUSE / RESUME with lockId set + replay buffer (R1.2.6, R2.6, §10.2).
//! - INVALIDATE broadcast + cascade with R1.4 idempotency.
//! - COMPLETE / ERROR cascade + Lock 2.B auto-cascade gating
//!   (ERROR dominates COMPLETE; first error wins).
//! - TEARDOWN auto-precedes COMPLETE (R2.6.4 / Lock 6.F) +
//!   `has_received_teardown` idempotency.
//! - Meta TEARDOWN ordering (R1.3.9.d) — companions tear down before parent.
//! - Resubscribable terminal lifecycle (R2.2.7, R2.5.3) — late subscribe to a
//!   resubscribable terminal node resets lifecycle, except after TEARDOWN
//!   (per F3 audit guard: TEARDOWN is permanent).
//!
//! # Module split (Slice C-1, 2026-05-05)
//!
//! Wave-engine internals (drain loop, fire selection, emission commit, sink
//! dispatch) live in [`crate::batch`]. The split is purely organizational —
//! the methods are still on `Core`. See `batch.rs` for the wave-engine
//! entry points (`run_wave`, `drain_and_flush`, `commit_emission`,
//! `queue_notify`, `deliver_data_to_consumer`).
//!
//! # Out of scope (later slices / milestones)
//!
//! - Deactivation cleanup (RAM nodes clear cache when sink count → 0) — M2.
//!
//! See [`migration-status.md`](../../../docs/migration-status.md) for the
//! milestone tracker and [`porting-deferred.md`](../../../docs/porting-deferred.md)
//! for surfaced concerns deferred to evidence-driven slices.
//!
//! # Re-entrance discipline (Slice A close, M1: fully lock-released)
//!
//! - **Wave-end sink fires** drop the state lock first. A subscriber's sink
//!   that calls back into `Core::emit` / `pause` / `resume` / `invalidate` /
//!   `complete` / `error` / `teardown` re-acquires the lock cleanly and runs
//!   a nested wave (`s.in_tick` is cleared before the deferred-fire phase).
//! - **`BindingBoundary::invoke_fn`** fires lock-released. The wave engine
//!   acquires + drops the state lock per fn-fire iteration around the
//!   `invoke_fn` callback. User fns may re-enter `Core::emit` / `pause` /
//!   etc. and run a nested wave.
//! - **`BindingBoundary::custom_equals`** fires lock-released.
//!   `commit_emission` brackets the equals check around a lock release;
//!   custom equals oracles may re-enter Core safely.
//! - **Subscribe-time handshake** is the one remaining lock-held callback.
//!   Trade-off for the subscribe-vs-emit race fix: the new sink is registered
//!   under the lock and the handshake fires under the lock (per-tier — see
//!   [`Core::subscribe`] for R1.3.5.a tier-split), so concurrent emits on
//!   other threads observe the new sink in normal happens-after order. A
//!   handshake-time sink callback that re-enters Core hits the
//!   `IN_HANDSHAKE_FIRE` thread-local diagnostic and panics with a clear
//!   message (lifting it requires per-sink staging-buffer machinery; tracked
//!   in `porting-deferred.md`).

use std::cell::Cell;
use std::collections::VecDeque;
use std::sync::{Arc, Weak};

use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use indexmap::IndexMap;
use parking_lot::{Mutex, MutexGuard, ReentrantMutex};
use smallvec::SmallVec;
use thiserror::Error;

use crate::batch::PendingPerNode;
use crate::boundary::BindingBoundary;
use crate::clock::monotonic_ns;
use crate::handle::{FnId, HandleId, LockId, NodeId, NO_HANDLE};
use crate::message::Message;

thread_local! {
    /// Set to `true` while a [`Core::subscribe`] handshake sink callback is
    /// running on this thread. The state lock is held during the handshake
    /// fire (trade-off for the subscribe-vs-emit race fix), so any Core
    /// method called by the handshake sink would deadlock.
    ///
    /// [`Core::lock_state`] checks this flag before acquiring the state
    /// lock and panics with a clear diagnostic instead of letting the
    /// program hang. Lifts together with the lock-released-handshake
    /// rework planned alongside the `MutexGuard`-ownership refactor.
    static IN_HANDSHAKE_FIRE: Cell<bool> = const { Cell::new(false) };
}

/// Handle returned by [`HandshakeFireGuard::enter`] that clears the
/// thread-local handshake-fire flag on drop. Using RAII ensures the flag
/// is cleared even if the sink callback panics.
struct HandshakeFireGuard;

impl HandshakeFireGuard {
    fn enter() -> Self {
        IN_HANDSHAKE_FIRE.with(|f| {
            debug_assert!(
                !f.get(),
                "nested handshake-fire entry — subscribe is not re-entrant"
            );
            f.set(true);
        });
        Self
    }
}

impl Drop for HandshakeFireGuard {
    fn drop(&mut self) {
        IN_HANDSHAKE_FIRE.with(|f| f.set(false));
    }
}

/// Terminal-lifecycle state — once set on a node, the node will not emit
/// further DATA; per-dep slots on consumers also use this to track which
/// upstreams have terminated (R1.3.4 / Lock 2.B).
///
/// `Error` carries a [`HandleId`] resolving to the error value. Refcount is
/// retained when the variant is stored in a node's `terminal` slot or any
/// consumer's `dep_terminals` slot; v1 does not release these (terminal
/// state is one-shot at this layer; release happens on resubscribable
/// terminal-lifecycle reset, a separate slice).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum TerminalKind {
    Complete,
    Error(HandleId),
}

/// Node kind discriminant. State nodes are ROM (cache survives deactivation);
/// compute nodes (`Derived`, `Dynamic`) are RAM (cache cleared on
/// deactivation — not yet modeled in v1).
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum NodeKind {
    /// Source node: cache is intrinsic, no fn, no deps. Mutated via [`Core::emit`].
    State,
    /// Derived node: fn fires on every dep change; all deps tracked.
    Derived,
    /// Dynamic node: fn declares which dep indices it actually read this run.
    /// Untracked dep updates flow through cache but do NOT re-fire fn.
    Dynamic,
}

/// Equality mode for a node's outgoing emissions.
///
/// `Identity` is the default: cache vs. new handle compare is a `u64` equal —
/// zero FFI. `Custom` invokes [`BindingBoundary::custom_equals`] every check
/// (R1.3.2.b two-arg call when both sides are non-sentinel).
#[derive(Copy, Clone, Debug)]
pub enum EqualsMode {
    Identity,
    Custom(FnId),
}

/// Internal identifier for a single subscription. Allocated per
/// [`Core::subscribe`] call. Wrapped by [`Subscription`] for the public API;
/// consumed directly only by Core internals and the [`Subscription::Drop`]
/// path.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub(crate) struct SubscriptionId(u64);

/// RAII subscription handle.
///
/// Returned by [`Core::subscribe`]. While the handle is held, the sink stays
/// registered against its node. Dropping the handle (explicitly via
/// `drop(sub)` or implicitly at scope exit) unsubscribes the sink — no manual
/// `unsubscribe()` call is needed. Per §10.12 of the rust-port session doc.
///
/// # Lifetime semantics
///
/// The subscription holds a [`Weak`] reference back to the Core's state. If
/// the Core is dropped before the subscription, the Drop impl is a silent
/// no-op (the sink has nowhere to deregister from anyway). This avoids a
/// reference cycle when subscribers capture an `Arc<Core>` in their closure.
///
/// # Thread safety
///
/// `Send + Sync`. The handle can be moved across threads or dropped from
/// any thread.
///
/// # Not Clone
///
/// `Subscription` owns the unsubscribe action exclusively. Cloning would
/// require either "first drop wins" or "last drop wins" semantics, both
/// of which surprise. If a binding needs multiple deregistration handles,
/// it should subscribe multiple times (each producing a fresh handle) or
/// wrap the single `Subscription` in `Arc<Mutex<Option<Subscription>>>`.
#[must_use = "dropping a Subscription unsubscribes its sink immediately"]
pub struct Subscription {
    state: Weak<Mutex<CoreState>>,
    node_id: NodeId,
    sub_id: SubscriptionId,
}

impl Subscription {
    /// The node this subscription is attached to.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        // Silent no-op if Core is gone. This keeps Drop infallible (no panics
        // from a dropped subscription racing a dropped Core) and avoids
        // surprising users with errors on shutdown.
        if let Some(state) = self.state.upgrade() {
            let mut s = state.lock();
            if let Some(rec) = s.nodes.get_mut(&self.node_id) {
                rec.subscribers.remove(&self.sub_id);
            }
        }
    }
}

// Compile-time assertion that Subscription is Send + Sync. If a future field
// breaks this, the build fails here rather than downstream at the binding
// site.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Subscription>();
};

/// A subscriber callback. `Send + Sync` so the Core can fire it from any
/// thread; `Fn` (not `FnMut`) so multiple references coexist — capture
/// mutable state in `Mutex<T>` or atomics on the binding side.
pub type Sink = Arc<dyn Fn(&[Message]) + Send + Sync>;

// ---------------------------------------------------------------------------
// PAUSE/RESUME state — §10.2 of the rust-port session doc
// ---------------------------------------------------------------------------

/// Per-node pause state.
///
/// Replaces the four TS fields (`_pauseLocks`, `_pauseBuffer`,
/// `_pauseDroppedCount`, `_pauseStartNs`) with a single enum where
/// the buffered fields are unreachable in the [`Self::Active`] variant —
/// the compiler refuses access. Per §10.2 simplification.
///
/// # Invariants
///
/// - `Active` ⇔ no lockId held.
/// - `Paused { locks, .. }` ⇔ `!locks.is_empty()`.
/// - Buffered messages are tier 3 (DATA/RESOLVED) and tier 4 (INVALIDATE)
///   only. Other tiers pass through immediately even while paused.
/// - `dropped` counts messages that fell out the front of `buffer` due to
///   the Core-global `pause_buffer_cap`; it is reported on resume so callers
///   can detect overflow without re-tracking it externally.
#[derive(Debug)]
pub(crate) enum PauseState {
    Active,
    Paused {
        /// Active lock holders. `SmallVec` keeps the common 1–2 lock case
        /// stack-allocated. Replaces `Set<unknown>` from TS.
        locks: SmallVec<[LockId; 2]>,
        /// Buffered tier-3/tier-4 outgoing messages, in arrival order.
        /// Replayed on the final RESUME.
        buffer: VecDeque<Message>,
        /// Count of messages dropped from the front when `buffer.len()` would
        /// exceed `pause_buffer_cap`. Cleared on final RESUME (next pause
        /// cycle starts fresh).
        dropped: u32,
        /// Wall-clock-monotonic ns when the lock first transitioned this node
        /// from `Active` to `Paused`. Reserved for the overflow-error detail
        /// path (TS Lock 6.A `pauseStartNs`); exposed to callers via a future
        /// `ResumeReport.pause_duration_ns` extension when an overflow is
        /// reported. Not currently read; allowed dead until that wires in.
        #[allow(dead_code)]
        started_at_ns: u64,
    },
}

impl PauseState {
    pub(crate) fn is_paused(&self) -> bool {
        matches!(self, Self::Paused { .. })
    }

    fn lock_count(&self) -> usize {
        match self {
            Self::Active => 0,
            Self::Paused { locks, .. } => locks.len(),
        }
    }

    fn contains_lock(&self, lock_id: LockId) -> bool {
        match self {
            Self::Active => false,
            Self::Paused { locks, .. } => locks.contains(&lock_id),
        }
    }

    /// Add a lock; transitions Active → Paused on first lock. Idempotent on
    /// duplicate lock_id (matches TS convention; spec is silent on the case).
    fn add_lock(&mut self, lock_id: LockId) {
        match self {
            Self::Active => {
                let mut locks = SmallVec::new();
                locks.push(lock_id);
                *self = Self::Paused {
                    locks,
                    buffer: VecDeque::new(),
                    dropped: 0,
                    started_at_ns: monotonic_ns(),
                };
            }
            Self::Paused { locks, .. } => {
                if !locks.contains(&lock_id) {
                    locks.push(lock_id);
                }
            }
        }
    }

    /// Remove a lock; if the lockset becomes empty, transition Paused →
    /// Active and return the buffered messages for replay (along with the
    /// dropped count for diagnostics). Unknown lock_id is an idempotent
    /// no-op (matches TS, R1.2.6 implicit).
    fn remove_lock(&mut self, lock_id: LockId) -> Option<(VecDeque<Message>, u32)> {
        match self {
            Self::Active => None,
            Self::Paused { locks, .. } => {
                if let Some(idx) = locks.iter().position(|l| *l == lock_id) {
                    locks.swap_remove(idx);
                }
                if locks.is_empty() {
                    let prev = std::mem::replace(self, Self::Active);
                    if let Self::Paused {
                        buffer, dropped, ..
                    } = prev
                    {
                        return Some((buffer, dropped));
                    }
                }
                None
            }
        }
    }

    /// Append a message to the buffer; if the buffer would exceed `cap`,
    /// pop from the front (oldest-first), increment `dropped`, and return
    /// the dropped messages so the caller can release any payload handles
    /// they reference. `cap` of `None` means unbounded.
    ///
    /// Note: refcount management for the message's payload handle is the
    /// caller's responsibility — see [`Core::queue_notify`] for the
    /// retain/release discipline. The buffer itself is just a message
    /// container; refcounts cross the binding boundary.
    pub(crate) fn push_buffered(&mut self, msg: Message, cap: Option<usize>) -> Vec<Message> {
        let mut overflow_dropped: Vec<Message> = Vec::new();
        if let Self::Paused {
            buffer, dropped, ..
        } = self
        {
            buffer.push_back(msg);
            if let Some(c) = cap {
                while buffer.len() > c {
                    if let Some(dropped_msg) = buffer.pop_front() {
                        overflow_dropped.push(dropped_msg);
                    }
                    *dropped = dropped.saturating_add(1);
                }
            }
        }
        overflow_dropped
    }
}

/// Errors returnable by [`Core::pause`] and [`Core::resume`].
#[derive(Error, Debug, Clone, PartialEq)]
pub enum PauseError {
    #[error("pause/resume: unknown node {0:?}")]
    UnknownNode(NodeId),
}

/// Per-dep record. Replaces the parallel `deps` / `dep_handles` /
/// `dep_terminals` vectors from v1. Canonical spec R2.9.b alignment.
///
/// Each entry tracks one dep's lifecycle state, wave-scoped batch data,
/// and cross-wave `prev_data` for `ctx.prevData` access.
pub(crate) struct DepRecord {
    /// The dep node this record tracks.
    pub(crate) node: NodeId,
    /// Last DATA handle from the end of the previous wave. [`NO_HANDLE`]
    /// means the dep has never emitted DATA.
    pub(crate) prev_data: HandleId,
    /// Per-dep dirty flag — awaiting DATA/RESOLVED for current wave.
    pub(crate) dirty: bool,
    /// Per-dep involved-this-wave flag. Distinguishes:
    /// - `involved && data_batch.is_empty()` → dep settled RESOLVED
    /// - `!involved && data_batch.is_empty()` → dep was not in this wave
    pub(crate) involved_this_wave: bool,
    /// DATA handles accumulated this wave. Outside `batch()` scope, at most
    /// 1 element. Inside `batch()`, K emits on the source produce K entries
    /// per R1.3.6.b coalescing. Each handle holds a `retain_handle` share
    /// taken at `deliver_data_to_consumer` time; released at wave-end
    /// rotation in `clear_wave_state`.
    pub(crate) data_batch: SmallVec<[HandleId; 1]>,
    /// Terminal state for this dep. `None` = dep is live.
    /// `Some` = dep emitted COMPLETE/ERROR. When ALL entries are Some,
    /// the node auto-cascades per Lock 2.B (ERROR dominates COMPLETE).
    pub(crate) terminal: Option<TerminalKind>,
}

impl DepRecord {
    fn new(node: NodeId) -> Self {
        Self {
            node,
            prev_data: NO_HANDLE,
            dirty: false,
            involved_this_wave: false,
            data_batch: SmallVec::new(),
            terminal: None,
        }
    }
}

/// Internal node record. Mirrors `core.ts:132–154`.
///
/// The 4 bool fields (`has_fired_once`, `dirty`, `involved_this_wave`,
/// `has_received_teardown`, `resubscribable`) each represent an orthogonal
/// lifecycle concern. Collapsing them into a bitfield would obscure intent
/// and complicate the wave-cleanup phase that resets only the wave-scoped
/// pair (`dirty`, `involved_this_wave`).
#[allow(clippy::struct_excessive_bools)]
pub(crate) struct NodeRecord {
    /// Per-dep records. Replaces the old parallel `deps` / `dep_handles` /
    /// `dep_terminals` vecs. Dep NodeIds derived via `dep_ids()`.
    pub(crate) dep_records: Vec<DepRecord>,
    pub(crate) kind: NodeKind,
    pub(crate) fn_id: Option<FnId>,
    pub(crate) equals: EqualsMode,

    // Mutable state
    pub(crate) cache: HandleId,
    pub(crate) has_fired_once: bool,
    pub(crate) subscribers: HashMap<SubscriptionId, Sink>,
    /// For dynamic nodes: which dep indices fn actually tracks.
    /// For static derived: all indices, populated at construction.
    pub(crate) tracked: HashSet<usize>,

    // Wave-scoped state — cleared at wave end.
    pub(crate) dirty: bool,
    pub(crate) involved_this_wave: bool,

    /// Per-node pause state. Default `Active`. See [`PauseState`].
    pub(crate) pause_state: PauseState,

    /// Terminal lifecycle state for THIS node's outgoing stream. Once set,
    /// further `emit` calls are silent no-ops, fn no longer fires, and only
    /// the terminal message has been queued downstream.
    pub(crate) terminal: Option<TerminalKind>,
    /// True after the first TEARDOWN has been processed for this node
    /// (R2.6.4 / Lock 6.F). Subsequent TEARDOWN deliveries are idempotent
    /// — the auto-prepended COMPLETE only fires on the first one. Without
    /// this flag, a redundant TEARDOWN delivered via the cascade plus an
    /// explicit `core.teardown(node)` would re-emit `[COMPLETE, TEARDOWN]`
    /// to subscribers per delivery, which is incorrect.
    pub(crate) has_received_teardown: bool,
    /// Per R2.2.7 / R2.5.3 — resubscribable terminal lifecycle.
    /// When `true` AND `terminal == Some(...)`, a fresh subscribe call
    /// will reset the node: clear `terminal`, `has_fired_once`,
    /// `has_received_teardown`, all dep_records to sentinel, and drain the
    /// pause lockset. Default `false`.
    pub(crate) resubscribable: bool,
    /// Meta companion nodes attached to this node per R1.3.9.d. When this
    /// node tears down, its meta companions are torn down FIRST (before
    /// the main node's auto-COMPLETE + TEARDOWN wire emission), so
    /// observers see companions terminate before the parent. The ordering
    /// is load-bearing — meta nodes typically subscribe to parent state
    /// that becomes inconsistent during the parent's destruction phase.
    pub(crate) meta_companions: Vec<NodeId>,
}

impl NodeRecord {
    /// Iterator over dep NodeIds in declaration order.
    pub(crate) fn dep_ids(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.dep_records.iter().map(|r| r.node)
    }

    /// Collected dep NodeIds — for call sites that need a `Vec<NodeId>`.
    pub(crate) fn dep_ids_vec(&self) -> Vec<NodeId> {
        self.dep_ids().collect()
    }

    /// Number of deps.
    pub(crate) fn dep_count(&self) -> usize {
        self.dep_records.len()
    }

    /// True if any dep is in sentinel state (never emitted DATA and no
    /// data this wave). Replaces the old `dep_handles.contains(&NO_HANDLE)`.
    pub(crate) fn has_sentinel_deps(&self) -> bool {
        self.dep_records
            .iter()
            .any(|r| r.prev_data == NO_HANDLE && r.data_batch.is_empty())
    }

    /// Find the index of a dep by NodeId.
    pub(crate) fn dep_index_of(&self, dep_id: NodeId) -> Option<usize> {
        self.dep_records.iter().position(|r| r.node == dep_id)
    }

    /// True if ALL dep terminal slots are populated (Lock 2.B cascade check).
    pub(crate) fn all_deps_terminal(&self) -> bool {
        !self.dep_records.is_empty() && self.dep_records.iter().all(|r| r.terminal.is_some())
    }
}

/// All mutable Core state, behind one [`parking_lot::Mutex`].
///
/// v1 single-mutex; per-subgraph `ReentrantMutex` parallelism is a later
/// optimization (CLAUDE.md Rust invariant 3).
pub(crate) struct CoreState {
    pub(crate) next_node_id: u64,
    pub(crate) next_subscription_id: u64,
    pub(crate) next_lock_id: u64,
    pub(crate) nodes: HashMap<NodeId, NodeRecord>,
    /// Inverted adjacency: `parent → children`. Updated on registration.
    pub(crate) children: HashMap<NodeId, HashSet<NodeId>>,
    /// Nodes whose fn we owe a fire to — drained by [`Core::run_wave`].
    pub(crate) pending_fires: HashSet<NodeId>,
    /// Per-node outgoing message buffer; flushed at wave end. Insertion-
    /// ordered so flush order is deterministic — load-bearing for
    /// R1.3.9.d meta-TEARDOWN ordering: when a parent and its meta
    /// companion both have queued messages in the same wave, the meta
    /// (queued first via `teardown_inner`'s recursion order) flushes
    /// first.
    ///
    /// Each entry carries the per-wave subscriber snapshot taken at first
    /// touch (Slice A close, M1: lock-released drain). Late subscribers
    /// installed mid-wave between fn-fire iterations don't appear in
    /// already-snapshotted entries; this is the load-bearing fix that
    /// prevents duplicate-Data delivery when a handshake delivers the
    /// post-commit cache and the wave's flush would otherwise also fire
    /// to the same sink.
    pub(crate) pending_notify: IndexMap<NodeId, PendingPerNode>,
    pub(crate) in_tick: bool,
    /// Core-global cap on per-node pause replay buffer length. `None` means
    /// unbounded. Per the user direction (Q1, 2026-05-05): start core-global;
    /// per-node override can be added later as a pure addition without API
    /// breakage. Default `None`.
    pub(crate) pause_buffer_cap: Option<usize>,
    /// Deferred sink-fire jobs collected by `flush_notifications`. The wave
    /// engine populates this under the state lock during the flush phase;
    /// `run_wave` then drops the lock and fires the jobs. Each tuple is
    /// `(sinks_for_one_node_one_phase, phase_messages)`. Empty between waves.
    pub(crate) deferred_flush_jobs: crate::batch::DeferredJobs,
    /// Payload-handle releases owed for messages that landed in
    /// `pending_notify` during this wave (one per `payload_handle()`).
    /// `run_wave` releases these after sinks fire and the lock is dropped,
    /// balancing the retain done in `queue_notify`.
    pub(crate) deferred_handle_releases: Vec<HandleId>,
    /// Binding-boundary handle for `Drop`-time refcount balancing.
    /// `Core` also holds a clone of this Arc; storing it here lets
    /// `Drop for CoreState` walk every retained slot and release the
    /// binding-side share when the last `Core` clone drops. Without this,
    /// `cache` / `terminal` / `dep_terminals` Error / pause-buffer payload
    /// handle refs leak in the binding registry until process exit.
    pub(crate) binding: Arc<dyn BindingBoundary>,
    /// Pre-wave cache snapshots used to restore state if the wave aborts
    /// mid-flight (e.g., a `Core::batch` closure panics). Each entry is
    /// `(node_id → old_cache_handle)` — the handle the node held BEFORE
    /// the wave started writing to it. The snapshotted handle holds a
    /// retain (taken when the snapshot was inserted) so it stays alive
    /// for restoration. On wave success, snapshots are dropped and their
    /// retains released. On wave abort (`BatchGuard::drop` panic-discard
    /// path), each cache slot is restored from the snapshot — the slot's
    /// current handle is released, and the snapshot's retain transfers
    /// to the cache slot. Only populated for in-flight waves; empty
    /// between waves.
    pub(crate) wave_cache_snapshots: HashMap<NodeId, HandleId>,
    /// Nodes that need an auto-Resolved at wave end if they don't receive
    /// a tier-3+ message from their own commit_emission. Populated by
    /// the RESOLVED child propagation in `commit_emission` (which queues
    /// Dirty but defers Resolved to avoid double-settlement). Drained by
    /// the auto-resolve sweep in `drain_and_flush`. Cleared by
    /// `clear_wave_state`.
    pub(crate) pending_auto_resolve: ahash::AHashSet<NodeId>,
    /// Topology-change sinks. Keyed by subscription id for O(1) removal.
    pub(crate) topology_sinks: HashMap<u64, crate::topology::TopologySink>,
    pub(crate) next_topology_id: u64,
}

/// The handle-protocol Core dispatcher.
///
/// Holds an [`Arc`] to the [`BindingBoundary`] and all dispatch state. Cheap
/// to clone (the inner `Arc<Mutex<CoreState>>` is shared); pass `Core` by
/// value to threads.
///
/// # Wave-owner re-entrant mutex (Slice A close /qa, M1)
///
/// The state lock (`state: Mutex<CoreState>`) is **dropped** around binding
/// callbacks (`invoke_fn`, `custom_equals`) so user fns may re-enter Core.
/// To preserve serializability of WAVE EXECUTION across threads — without
/// re-introducing the lock-held-during-fn-fire deadlock the Slice A close
/// refactor lifted — the wave engine acquires `wave_owner` (a
/// [`parking_lot::ReentrantMutex`]) for the lifetime of each wave.
///
/// Properties:
///
/// - **Same-thread re-entrance is free.** A user fn that calls back into
///   `Core::emit` / `Core::pause` / etc. mid-fire re-acquires `wave_owner`
///   on the same thread and runs as a nested wave (the inner `run_wave`
///   sees `in_tick=true` and skips drain — outer drain picks up).
/// - **Cross-thread emits BLOCK** at `wave_owner.lock_arc()` until the
///   in-flight wave completes (drain + flush + sink fire all done). This
///   serializes wave OWNERSHIP across threads, while still allowing the
///   state lock to drop inside the wave for binding callbacks.
///
/// Without this, Slice A close's lock-released drain let cross-thread
/// emits absorb into the in-flight wave's `pending_notify` and return
/// before subscribers fire — breaking the user-facing happens-after
/// contract that `emit` returning means subscribers have observed.
#[derive(Clone)]
pub struct Core {
    pub(crate) state: Arc<Mutex<CoreState>>,
    pub(crate) binding: Arc<dyn BindingBoundary>,
    pub(crate) wave_owner: Arc<ReentrantMutex<()>>,
}

impl Core {
    /// Construct a fresh Core wired to the given binding. Pause buffer cap
    /// defaults to unbounded; set via [`Self::set_pause_buffer_cap`].
    #[must_use]
    pub fn new(binding: Arc<dyn BindingBoundary>) -> Self {
        Self {
            state: Arc::new(Mutex::new(CoreState {
                next_node_id: 1,
                next_subscription_id: 1,
                next_lock_id: 1,
                nodes: HashMap::new(),
                children: HashMap::new(),
                pending_fires: HashSet::new(),
                pending_notify: IndexMap::new(),
                in_tick: false,
                pause_buffer_cap: None,
                deferred_flush_jobs: Vec::new(),
                deferred_handle_releases: Vec::new(),
                binding: binding.clone(),
                wave_cache_snapshots: HashMap::new(),
                pending_auto_resolve: ahash::AHashSet::new(),
                topology_sinks: HashMap::new(),
                next_topology_id: 1,
            })),
            binding,
            wave_owner: Arc::new(ReentrantMutex::new(())),
        }
    }

    /// Acquire the state lock, with a re-entrance check that panics with a
    /// clear diagnostic if called from inside a [`Core::subscribe`]
    /// handshake sink callback.
    ///
    /// All public Core methods route their lock acquisition through here
    /// so that handshake-time re-entrance produces a panic rather than a
    /// silent deadlock against the held lock. Subscribers' wave-end sinks
    /// (post drop-then-fire) are NOT subject to this restriction — the
    /// flag is set ONLY while the handshake fires under the lock; the
    /// drop-then-fire path clears the lock first.
    pub(crate) fn lock_state(&self) -> MutexGuard<'_, CoreState> {
        IN_HANDSHAKE_FIRE.with(|f| {
            assert!(
                !f.get(),
                "Core method called from inside a subscribe handshake sink \
                 callback — this would deadlock the state lock. Handshake \
                 sinks must consume the [Start, Data?, ...] payload \
                 synchronously without re-entering Core. v1 limitation; \
                 lifts with the planned MutexGuard-ownership refactor."
            );
        });
        self.state.lock()
    }

    /// Whether `self` and `other` point to the same dispatcher state.
    /// True when one was produced by `Clone`-ing the other (or they
    /// were both cloned from a common ancestor); false for two
    /// independently `Core::new`-constructed instances even with the
    /// same binding.
    ///
    /// Used by `graphrefly-graph`'s `mount` to enforce the "shared-Core
    /// only" v1 invariant — cross-Core mount is post-M6.
    #[must_use]
    pub fn same_dispatcher(&self, other: &Core) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }

    /// Configure the Core-global cap on pause replay buffer length. When set,
    /// any per-node pause buffer that would exceed `cap` drops the oldest
    /// message(s) from the front; the dropped count is reported back via the
    /// resume callback (see [`ResumeReport`]). `None` (default) means
    /// unbounded; messages buffer indefinitely until the lockset clears.
    pub fn set_pause_buffer_cap(&self, cap: Option<usize>) {
        self.lock_state().pause_buffer_cap = cap;
    }

    /// Allocate a unique [`LockId`] for use with [`Self::pause`] /
    /// [`Self::resume`]. Convenience for callers that don't already have an
    /// id-allocation scheme; user-supplied ids work too.
    #[must_use]
    pub fn alloc_lock_id(&self) -> LockId {
        let mut s = self.lock_state();
        let id = LockId::new(s.next_lock_id);
        s.next_lock_id += 1;
        id
    }

    // -------------------------------------------------------------------
    // Registration
    // -------------------------------------------------------------------

    /// Register a state node. `initial` may be [`NO_HANDLE`] to start sentinel.
    #[must_use]
    pub fn register_state(&self, initial: HandleId) -> NodeId {
        let mut s = self.lock_state();
        let id = s.alloc_node_id();
        let rec = NodeRecord {
            dep_records: Vec::new(),
            kind: NodeKind::State,
            fn_id: None,
            equals: EqualsMode::Identity,
            cache: initial,
            has_fired_once: initial != NO_HANDLE,
            subscribers: HashMap::new(),
            tracked: HashSet::new(),
            dirty: false,
            involved_this_wave: false,
            pause_state: PauseState::Active,
            terminal: None,
            has_received_teardown: false,
            resubscribable: false,
            meta_companions: Vec::new(),
        };
        s.nodes.insert(id, rec);
        s.children.insert(id, HashSet::new());
        drop(s);
        self.fire_topology_event(&crate::topology::TopologyEvent::NodeRegistered(id));
        id
    }

    /// Register a derived (static) node. `fn_id` invokes via the binding;
    /// `equals` defaults to identity in callers.
    ///
    /// # Panics
    ///
    /// Panics if any element of `deps` is not a registered node id. The
    /// panic message includes the offending id. Production bindings should
    /// validate dep ids before calling.
    #[must_use]
    pub fn register_derived(&self, deps: &[NodeId], fn_id: FnId, equals: EqualsMode) -> NodeId {
        self.register_computed(deps, fn_id, equals, NodeKind::Derived)
    }

    /// Register a dynamic node — fn declares its actually-tracked dep indices
    /// per fire (canonical spec §2.8 / Lock 2.B).
    ///
    /// # Panics
    ///
    /// Panics if any element of `deps` is not a registered node id. The
    /// panic message includes the offending id. Production bindings should
    /// validate dep ids before calling.
    #[must_use]
    pub fn register_dynamic(&self, deps: &[NodeId], fn_id: FnId, equals: EqualsMode) -> NodeId {
        self.register_computed(deps, fn_id, equals, NodeKind::Dynamic)
    }

    fn register_computed(
        &self,
        deps: &[NodeId],
        fn_id: FnId,
        equals: EqualsMode,
        kind: NodeKind,
    ) -> NodeId {
        let mut s = self.lock_state();
        let id = s.alloc_node_id();
        // Static derived tracks all deps; dynamic starts empty and fills via fn return.
        let tracked: HashSet<usize> = match kind {
            NodeKind::Derived => (0..deps.len()).collect(),
            NodeKind::Dynamic | NodeKind::State => HashSet::new(),
        };
        let dep_records: Vec<DepRecord> = deps.iter().map(|&d| DepRecord::new(d)).collect();
        let rec = NodeRecord {
            dep_records,
            kind,
            fn_id: Some(fn_id),
            equals,
            cache: NO_HANDLE,
            has_fired_once: false,
            subscribers: HashMap::new(),
            tracked,
            dirty: false,
            involved_this_wave: false,
            pause_state: PauseState::Active,
            terminal: None,
            has_received_teardown: false,
            resubscribable: false,
            meta_companions: Vec::new(),
        };
        s.nodes.insert(id, rec);
        s.children.insert(id, HashSet::new());
        for &dep in deps {
            // Validate dep exists; record inverted edge.
            assert!(s.nodes.contains_key(&dep), "unknown dep {dep:?}");
            s.children.entry(dep).or_default().insert(id);
        }
        drop(s);
        self.fire_topology_event(&crate::topology::TopologyEvent::NodeRegistered(id));
        id
    }

    // -------------------------------------------------------------------
    // Subscription
    // -------------------------------------------------------------------

    /// Subscribe a sink to a node. Returns a [`Subscription`] handle —
    /// dropping the handle unsubscribes the sink. Per §10.12, no manual
    /// `unsubscribe(node, id)` call is required.
    ///
    /// Push-on-subscribe (R1.2.3, R2.2.3 step 4): the sink is registered AFTER
    /// the START handshake fires. The handshake contents depend on node
    /// state:
    /// - Sentinel cache + live (non-terminal): `[START]`
    /// - Cached + live: `[START, DATA(handle)]`
    /// - Cached + terminated (non-resubscribable): `[START, DATA(handle), <terminal>]`
    /// - Sentinel + terminated (non-resubscribable): `[START, <terminal>]`
    ///
    /// Resubscribable terminal lifecycle (R2.2.7 / R2.5.3): if the node was
    /// marked resubscribable via [`Self::set_resubscribable`] AND has
    /// terminated, the subscribe call first **resets** the node — clears
    /// `terminal`, `has_fired_once`, `has_received_teardown`, all
    /// `dep_handles` to `NO_HANDLE`, all `dep_terminals` to `None`, and
    /// drains the pause lockset. The new subscriber then receives a fresh
    /// `[START]` (cache may survive for state nodes; sentinel for compute).
    ///
    /// Activation (R2.2.3 step 5): if this is the first subscriber and the
    /// node is a derived/dynamic compute, recursively activate deps so their
    /// cached handles fill our `dep_handles`.
    #[allow(clippy::needless_pass_by_value)] // Sink is `Arc<dyn Fn>`; we clone for the subscribers map and call it directly. Taking by value matches the ergonomics callers expect.
    pub fn subscribe(&self, node_id: NodeId, sink: Sink) -> Subscription {
        // Subscribe protocol (Slice A close, M1):
        //
        // 1. Under the state lock: alloc sub_id, run resubscribable reset
        //    if applicable, build the per-tier handshake slices, install
        //    the sink in `subscribers`, fire the handshake as separate
        //    sink calls per tier, observe whether activation is needed.
        // 2. Lock dropped: if first compute subscriber, run a wave via
        //    `run_wave` to drive activation + drain. The activation thunk
        //    re-acquires the lock briefly to walk deps and queue fires;
        //    `run_wave` drains lock-released around `invoke_fn`.
        //
        // Race-fix discipline (carried from Slice A-bigger): the sink is
        // installed BEFORE the handshake fires AND BEFORE the lock drops,
        // so concurrent emits on other threads (which block on the state
        // lock) observe the new sink when they eventually acquire. Their
        // wave's flush queues the new sink into `pending_notify`'s per-
        // wave snapshot at first touch, ensuring happens-after delivery
        // relative to our handshake.
        //
        // Handshake re-entrance: the handshake fires lock-held, so a
        // handshake-time sink callback that re-enters Core hits the
        // `IN_HANDSHAKE_FIRE` thread-local diagnostic and panics with a
        // clear message instead of deadlocking. Lifting this requires a
        // staging-buffer machinery (per-sink "handshake pending" flag);
        // tracked in `porting-deferred.md` as M1-close+ work.
        //
        // R1.3.5.a tier-split: each tier of the handshake fires as a
        // separate sink call — `[Start]`, `[Data(cache)]`, `[Complete]` /
        // `[Error(h)]`, `[Teardown]`. This matches TS's `downWithBatch`
        // routing for cached-source subscribes. Empty tiers are skipped.
        let (sub_id, needs_activation) = {
            let mut s = self.lock_state();
            let sub_id = s.alloc_sub_id();

            // Resubscribable reset: terminal + flagged → clear lifecycle
            // state so the incoming subscriber starts fresh. F3 audit
            // guard: a node that has received TEARDOWN (R2.6.4) is
            // permanently destroyed at this layer; resurrecting it via a
            // late subscribe is a category error. COMPLETE/ERROR is
            // recoverable for resubscribable nodes; TEARDOWN is not. The
            // handshake will still replay the terminal in the non-reset
            // branch so the late subscriber sees a clean
            // `[START, ?DATA, COMPLETE|ERROR, TEARDOWN]` stream.
            let needs_reset = {
                let rec = s.require_node(node_id);
                rec.resubscribable && rec.terminal.is_some() && !rec.has_received_teardown
            };
            if needs_reset {
                self.reset_for_fresh_lifecycle(&mut s, node_id);
            }

            // Snapshot handshake state under lock.
            let (cache, kind, first_subscriber, terminal, torn_down) = {
                let rec = s.require_node(node_id);
                (
                    rec.cache,
                    rec.kind,
                    rec.subscribers.is_empty(),
                    rec.terminal,
                    rec.has_received_teardown,
                )
            };

            // Build per-tier handshake slices. Each non-empty slice is
            // fired as a separate sink call (R1.3.5.a tier-split).
            let mut tier_slices: SmallVec<[Vec<Message>; 4]> = SmallVec::new();
            tier_slices.push(vec![Message::Start]);
            if cache != NO_HANDLE {
                tier_slices.push(vec![Message::Data(cache)]);
            }
            if let Some(t) = terminal {
                tier_slices.push(vec![match t {
                    TerminalKind::Complete => Message::Complete,
                    TerminalKind::Error(h) => Message::Error(h),
                }]);
            }
            if torn_down {
                tier_slices.push(vec![Message::Teardown]);
            }

            // Install sink BEFORE firing handshake so concurrent emits
            // observe it when they acquire the lock (race fix).
            s.require_node_mut(node_id)
                .subscribers
                .insert(sub_id, sink.clone());

            // Fire handshake per-tier under the lock. Re-entrance from a
            // handshake sink callback panics with a clear diagnostic.
            {
                let _reentrance_guard = HandshakeFireGuard::enter();
                for slice in &tier_slices {
                    sink(slice);
                }
            }

            let needs_activation = first_subscriber && kind != NodeKind::State;
            (sub_id, needs_activation)
        };

        // Lock dropped — run activation under run_wave. The activation
        // thunk re-acquires the lock briefly; run_wave drains lock-
        // released around `invoke_fn`.
        if needs_activation {
            self.run_wave(|this| {
                let mut s = this.lock_state();
                this.activate_derived(&mut s, node_id);
            });
        }

        Subscription {
            state: Arc::downgrade(&self.state),
            node_id,
            sub_id,
        }
    }

    /// Mark `node_id` as resubscribable per R2.2.7. Resubscribable nodes
    /// reset their terminal-lifecycle state on a fresh subscribe — see
    /// [`Self::subscribe`].
    ///
    /// Configuration call — must be made before the node has any active
    /// subscribers, since changing the policy mid-flight would surprise
    /// existing observers.
    ///
    /// # Panics
    ///
    /// Panics if the node has subscribers (the policy is observable
    /// behavior; changing it after the fact would change semantics for
    /// existing sinks).
    pub fn set_resubscribable(&self, node_id: NodeId, resubscribable: bool) {
        let mut s = self.lock_state();
        let rec = s.require_node_mut(node_id);
        assert!(
            rec.subscribers.is_empty(),
            "set_resubscribable: node already has subscribers; \
             configure resubscribable before any subscribe call"
        );
        rec.resubscribable = resubscribable;
    }

    /// Reset a resubscribable node's terminal-lifecycle state. Called from
    /// `subscribe` when a late subscriber arrives at a flagged node.
    ///
    /// Released: terminal-slot retain (Error handle), all per-dep terminal
    /// retains (Error handles), all data_batch retains.
    /// Cleared: `terminal`, `has_fired_once`, `has_received_teardown`, all
    /// dep_records to sentinel, the pause lockset (any held locks are
    /// released — replay buffer drops silently because there are no
    /// subscribers to flush to).
    fn reset_for_fresh_lifecycle(&self, s: &mut CoreState, node_id: NodeId) {
        // Collect handles to release (after dropping the borrow).
        let handles_to_release: Vec<HandleId> = {
            let rec = s.require_node(node_id);
            let mut hs = Vec::new();
            if let Some(TerminalKind::Error(h)) = rec.terminal {
                hs.push(h);
            }
            for dr in &rec.dep_records {
                if let Some(TerminalKind::Error(h)) = dr.terminal {
                    hs.push(h);
                }
                // Release data_batch retains.
                for &h in &dr.data_batch {
                    hs.push(h);
                }
            }
            hs
        };
        let pause_buffer_payloads: Vec<HandleId> = {
            let rec = s.require_node_mut(node_id);
            // Take pause_state's buffer; release any retained payload
            // handles (they were retained at queue_notify time; buffer is
            // dropped without flushing because the new subscriber is the
            // only consumer and they get a clean start).
            let mut pulled = Vec::new();
            if let PauseState::Paused { ref mut buffer, .. } = rec.pause_state {
                for msg in buffer.drain(..) {
                    if let Some(h) = msg.payload_handle() {
                        pulled.push(h);
                    }
                }
            }
            // Reset everything.
            rec.terminal = None;
            rec.has_fired_once = rec.cache != NO_HANDLE && rec.kind == NodeKind::State;
            rec.has_received_teardown = false;
            for dr in &mut rec.dep_records {
                dr.prev_data = NO_HANDLE;
                dr.data_batch.clear(); // retains already collected above
                dr.terminal = None;
                dr.dirty = false;
                dr.involved_this_wave = false;
            }
            rec.pause_state = PauseState::Active;
            rec.involved_this_wave = false;
            rec.dirty = false;
            // P7 (Slice A close /qa): for Dynamic nodes, clear `tracked`
            // so the post-reset first fire repopulates it from the fn's
            // returned tracked-deps set. Mirrors the `set_deps` Dynamic
            // path's discipline. Without this, a resubscribed dynamic
            // node's first fire would use the OLD tracked set — masked
            // today by `has_fired_once = false` (first-fire branch ignores
            // `tracked`), but a latent foot-gun. Derived's `tracked` is
            // `(0..deps.len())` from registration and stays valid across
            // a lifecycle reset, so we leave it alone.
            if rec.kind == NodeKind::Dynamic {
                rec.tracked.clear();
            }
            pulled
        };
        for h in handles_to_release {
            self.binding.release_handle(h);
        }
        for h in pause_buffer_payloads {
            self.binding.release_handle(h);
        }
    }

    /// Activate `root` and any transitive uncached compute deps so their
    /// caches fill our dep_handles slots.
    ///
    /// Slice A close (M1): pure dep-walk + dep_handles population +
    /// pending_fires queueing. No `in_tick` management or `drain_and_flush`
    /// call — the outer caller (typically `Core::subscribe` via
    /// [`Core::run_wave`]) owns the wave lifecycle and drains lock-released
    /// around `invoke_fn`.
    ///
    /// Walk shape:
    ///   1. **Discover phase (DFS via Vec stack):** starting at `root`,
    ///      walk transitively-needing-activation deps via the `deps`
    ///      chain. Build an ordering where each node appears AFTER all
    ///      of its uncached compute deps — i.e., reverse topological
    ///      among the visited subgraph.
    ///   2. **Deliver phase (forward iteration):** for each visited
    ///      node in dep-first order, push deps' caches into the node's
    ///      `dep_handles` slots. Caches that were sentinel pre-walk are
    ///      filled because their parent's fn fires later in the wave's
    ///      drain loop and `commit_emission` propagates new caches forward
    ///      via `deliver_data_to_consumer` — the same path this method
    ///      uses for the initial seed. Adds the node to `pending_fires`
    ///      if its tracked-deps gate is satisfied; the wave-engine drain
    ///      fires the fn lock-released around `invoke_fn`.
    pub(crate) fn activate_derived(&self, s: &mut CoreState, root: NodeId) {
        // Phase 1: discover. DFS to collect every compute node reachable
        // via deps that doesn't yet have a cache and hasn't fired.
        // Record them in dep-first (post-order) so phase 2 can deliver
        // caches forward. Frame is `(node_id, finalize)` — `finalize=false`
        // means "first visit: push deps then re-push self with finalize=true";
        // `finalize=true` means "deps have been expanded, append self to
        // `order`."
        let mut visited: HashSet<NodeId> = HashSet::new();
        let mut order: Vec<NodeId> = Vec::new();
        let mut stack: Vec<(NodeId, bool)> = vec![(root, false)];
        while let Some((id, finalize)) = stack.pop() {
            if finalize {
                order.push(id);
                continue;
            }
            if !visited.insert(id) {
                continue;
            }
            stack.push((id, true));
            let dep_ids: Vec<NodeId> = s.require_node(id).dep_ids_vec();
            for dep_id in dep_ids {
                let (dep_kind, dep_cache, dep_has_fired) = {
                    let dep_rec = s.require_node(dep_id);
                    (dep_rec.kind, dep_rec.cache, dep_rec.has_fired_once)
                };
                if dep_kind != NodeKind::State
                    && dep_cache == NO_HANDLE
                    && !dep_has_fired
                    && !visited.contains(&dep_id)
                {
                    stack.push((dep_id, false));
                }
            }
        }

        // Phase 2: deliver caches in dep-first order. For each node, walk
        // its deps and call `deliver_data_to_consumer` for any with caches.
        for &id in &order {
            let dep_ids: Vec<NodeId> = s.require_node(id).dep_ids_vec();
            for (i, dep_id) in dep_ids.iter().copied().enumerate() {
                let dep_cache = s.require_node(dep_id).cache;
                if dep_cache != NO_HANDLE {
                    self.deliver_data_to_consumer(s, id, i, dep_cache);
                }
            }
        }
    }

    // -------------------------------------------------------------------
    // Emission entry point
    // -------------------------------------------------------------------

    /// Set a state node's value. Triggers a wave (DIRTY → DATA/RESOLVED →
    /// fn fires for downstream).
    ///
    /// Silent no-op if the node has already terminated (R1.3.4). The handle
    /// passed in is still released by the caller's binding-side intern path
    /// — no implicit retain is consumed when the call short-circuits.
    ///
    /// # Panics
    ///
    /// Panics if `node_id` is not a state node, or if `new_handle` is
    /// [`NO_HANDLE`] (per R1.2.4, sentinel is not a valid DATA payload).
    pub fn emit(&self, node_id: NodeId, new_handle: HandleId) {
        assert!(
            new_handle != NO_HANDLE,
            "NO_HANDLE is not a valid DATA payload (R1.2.4)"
        );
        // Validate + terminal short-circuit under a brief lock.
        {
            let s = self.lock_state();
            let rec = s.require_node(node_id);
            assert!(
                rec.kind == NodeKind::State,
                "emit() is for state nodes only; derived emits via fn"
            );
            if rec.terminal.is_some() {
                drop(s);
                // Caller's intern share would otherwise leak; cache slot
                // ownership doesn't transfer because we're not advancing
                // cache. Released lock-released so the binding can't
                // deadlock against an internal binding mutex.
                self.binding.release_handle(new_handle);
                return;
            }
        }
        // Run wave — `run_wave` and `commit_emission` manage their own
        // locking; binding callbacks (custom_equals, sinks) fire lock-
        // released.
        self.run_wave(|this| {
            this.commit_emission(node_id, new_handle);
        });
    }

    /// Read a node's current cache. Returns [`NO_HANDLE`] if sentinel.
    #[must_use]
    pub fn cache_of(&self, node_id: NodeId) -> HandleId {
        self.lock_state().require_node(node_id).cache
    }

    /// Whether the node's fn has fired at least once (compute) OR it has had
    /// a non-sentinel value (state).
    #[must_use]
    pub fn has_fired_once(&self, node_id: NodeId) -> bool {
        self.lock_state().require_node(node_id).has_fired_once
    }

    // -------------------------------------------------------------------
    // Read-side inspection helpers (Slice E+, M2)
    //
    // Non-panicking accessors for graph-layer introspection (`describe()`,
    // `observe()`, `node_count()`). All five return Option/empty for
    // unknown ids — they're meant to back walks over `node_ids()` where
    // the caller already knows the ids are valid, plus debugging /
    // dry-run probes that prefer "absence" over "panic".
    //
    // Keep these strictly read-only: no wave entry, no binding callbacks,
    // no lock release. Each takes the state lock once, copies a small
    // value, and drops the lock.
    // -------------------------------------------------------------------

    /// Snapshot of every registered `NodeId` in unspecified order. The
    /// order matches `HashMap` iteration over the internal node table —
    /// callers that need stable ordering should track names at the
    /// `Graph` layer (canonical spec §3.5 namespace).
    #[must_use]
    pub fn node_ids(&self) -> Vec<NodeId> {
        self.lock_state().nodes.keys().copied().collect()
    }

    /// Total number of nodes registered in this Core.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.lock_state().nodes.len()
    }

    /// Returns `Some(kind)` for known nodes, `None` for unknown.
    #[must_use]
    pub fn kind_of(&self, node_id: NodeId) -> Option<NodeKind> {
        self.lock_state().nodes.get(&node_id).map(|r| r.kind)
    }

    /// Snapshot of the node's deps in declaration order. Empty for
    /// unknown nodes or for state nodes (which have no deps).
    #[must_use]
    pub fn deps_of(&self, node_id: NodeId) -> Vec<NodeId> {
        self.lock_state()
            .nodes
            .get(&node_id)
            .map(NodeRecord::dep_ids_vec)
            .unwrap_or_default()
    }

    /// Returns `Some(kind)` if the node has terminated (R1.3.4) — the
    /// pair `Some(Complete)` / `Some(Error(h))` mirrors the wire message
    /// the node emitted. `None` for live nodes or unknown ids.
    #[must_use]
    pub fn is_terminal(&self, node_id: NodeId) -> Option<TerminalKind> {
        self.lock_state()
            .nodes
            .get(&node_id)
            .and_then(|r| r.terminal)
    }

    /// Whether the node has wave-scoped DIRTY pending (a tier-1 message
    /// queued but the matching tier-3 settle has not yet flushed).
    /// `false` for unknown ids. Mostly useful for `describe()` status
    /// classification (R3.6.1 `"dirty"`).
    #[must_use]
    pub fn is_dirty(&self, node_id: NodeId) -> bool {
        self.lock_state()
            .nodes
            .get(&node_id)
            .is_some_and(|r| r.dirty)
    }

    /// Snapshot of `parent`'s meta companion list (R1.3.9.d / R2.3.3 —
    /// the companions added via [`Self::add_meta_companion`]). Empty
    /// for unknown ids or for nodes with no companions registered.
    ///
    /// Used by the graph layer's `signal_invalidate` to filter meta
    /// children out of the broadcast (canonical R3.7.2 — meta caches
    /// are preserved across graph-wide INVALIDATE).
    #[must_use]
    pub fn meta_companions_of(&self, parent: NodeId) -> Vec<NodeId> {
        self.lock_state()
            .nodes
            .get(&parent)
            .map(|r| r.meta_companions.clone())
            .unwrap_or_default()
    }

    // -------------------------------------------------------------------
    // Wave engine — lives in `crate::batch` (Slice C-1 module split;
    // Slice A close M1 refactor lifted the binding-callback re-entrance
    // restrictions). The methods are still on `Core`; see `batch.rs` for:
    //
    //   - `run_wave` — wave entry, manages own locking.
    //   - `drain_and_flush` — drain phase, lock-released around invoke_fn.
    //   - `commit_emission` — lock-released around custom_equals.
    //   - `pick_next_fire`, `deliver_data_to_consumer`, `queue_notify`,
    //     `flush_notifications` — wave-engine helpers.
    // -------------------------------------------------------------------
}

// -----------------------------------------------------------------------
// COMPLETE / ERROR — terminal lifecycle + auto-cascade gating
// -----------------------------------------------------------------------

impl Core {
    /// Emit `[COMPLETE]` (R1.3.4) on `node_id`, marking it terminal. After
    /// this call:
    ///
    /// - Subsequent `Core::emit` on this node is a silent no-op (idempotent
    ///   termination).
    /// - The node's fn no longer fires.
    /// - The node's cache is preserved (last value still observable via
    ///   `cache_of`).
    /// - Children receive `[COMPLETE]` (tier 5 — bypasses pause buffer).
    /// - Auto-cascade gating (Lock 2.B): each child that has all of its
    ///   deps in a terminal state auto-emits its own `[COMPLETE]`. ERROR
    ///   dominates COMPLETE — if any of a child's deps emitted ERROR, the
    ///   child auto-cascades that ERROR instead.
    ///
    /// Idempotent: calling `complete` on an already-terminal node is a no-op.
    ///
    /// # Panics
    ///
    /// Panics if `node_id` is unknown.
    pub fn complete(&self, node_id: NodeId) {
        self.emit_terminal(node_id, TerminalKind::Complete);
    }

    /// Emit `[ERROR, error_handle]` (R1.3.4) on `node_id`. `error_handle`
    /// must resolve to a non-sentinel value (R1.2.5) — the binding side has
    /// already interned the error value before this call. Same lifecycle
    /// effects as [`Self::complete`]; ERROR dominates COMPLETE in auto-
    /// cascade gating.
    ///
    /// # Panics
    ///
    /// Panics if `node_id` is unknown or `error_handle == NO_HANDLE`.
    pub fn error(&self, node_id: NodeId, error_handle: HandleId) {
        assert!(
            error_handle != NO_HANDLE,
            "NO_HANDLE is not a valid ERROR payload (R1.2.5)"
        );
        self.emit_terminal(node_id, TerminalKind::Error(error_handle));
        // The caller's intern share for `error_handle` is NOT transferred
        // to any slot — `terminate_node` takes its OWN retain for every
        // populated `terminal` and `dep_terminals` slot. Release the
        // caller's share here (mirrors `Core::emit`'s short-circuit
        // release on terminal). Without this, every `error()` call leaks
        // one binding-side handle ref. Slice A-bigger /qa item D fix.
        self.binding.release_handle(error_handle);
    }

    fn emit_terminal(&self, node_id: NodeId, terminal: TerminalKind) {
        {
            let s = self.lock_state();
            assert!(s.nodes.contains_key(&node_id), "unknown node {node_id:?}");
        }
        // Wave runs with `run_wave` orchestrating drain. The thunk acquires
        // its own lock to queue the cascade (terminate_node is a fast
        // structural walk; no binding callbacks beyond non-re-entrant
        // retain/release).
        self.run_wave(|this| {
            let mut s = this.lock_state();
            this.terminate_node(&mut s, node_id, terminal);
        });
    }

    /// Set the node's terminal slot, queue the wire message, and cascade to
    /// children. Idempotent on already-terminal node (no-op).
    ///
    /// Iterative implementation (Slice A-bigger, M1-close): a work-queue
    /// drives the cascade so deep linear chains don't overflow the OS
    /// thread stack. Mirrors `path_from_to`'s explicit-stack pattern.
    fn terminate_node(&self, s: &mut CoreState, node_id: NodeId, terminal: TerminalKind) {
        let mut work: Vec<(NodeId, TerminalKind)> = vec![(node_id, terminal)];
        while let Some((id, t)) = work.pop() {
            if s.require_node(id).terminal.is_some() {
                continue; // Idempotent — already terminal.
            }
            // Take a refcount share for the terminal slot so the error
            // handle outlives the binding-side intern's transient share.
            if let TerminalKind::Error(h) = t {
                self.binding.retain_handle(h);
            }
            s.require_node_mut(id).terminal = Some(t);
            // Drain pending fires for this node — fn won't fire on a
            // terminal node.
            s.pending_fires.remove(&id);
            // Queue the wire message (tier 5 — bypasses pause buffer).
            let msg = match t {
                TerminalKind::Complete => Message::Complete,
                TerminalKind::Error(h) => Message::Error(h),
            };
            self.queue_notify(s, id, msg);
            // Cascade to children.
            let child_ids: Vec<NodeId> = s
                .children
                .get(&id)
                .map(|c| c.iter().copied().collect())
                .unwrap_or_default();
            for child_id in child_ids {
                let dep_idx = s.require_node(child_id).dep_index_of(id);
                let Some(idx) = dep_idx else { continue };
                // Mark this child's per-dep terminal slot. Take a retain on
                // the error handle for the slot share.
                {
                    let child = s.require_node_mut(child_id);
                    if child.dep_records[idx].terminal.is_some() {
                        // Idempotent — child already saw this dep terminate.
                        continue;
                    }
                    child.dep_records[idx].terminal = Some(t);
                }
                if let TerminalKind::Error(h) = t {
                    self.binding.retain_handle(h);
                }
                // Auto-cascade gating: if all deps now terminal, push child
                // onto the work queue with the chosen terminal.
                let cascade = {
                    let child = s.require_node(child_id);
                    if child.terminal.is_some() {
                        None // Already terminated — no-op.
                    } else if child.all_deps_terminal() {
                        Some(pick_cascade_terminal(&child.dep_records))
                    } else {
                        None
                    }
                };
                if let Some(t_child) = cascade {
                    work.push((child_id, t_child));
                }
            }
        }
    }
}

/// Lock 2.B cascade-terminal selection: ERROR dominates COMPLETE; first
/// ERROR seen wins. Caller has already verified all deps are terminal.
fn pick_cascade_terminal(dep_records: &[DepRecord]) -> TerminalKind {
    for dr in dep_records {
        if let Some(TerminalKind::Error(h)) = dr.terminal {
            return TerminalKind::Error(h);
        }
    }
    TerminalKind::Complete
}

// -----------------------------------------------------------------------
// TEARDOWN — destruction, with auto-COMPLETE prepend (R2.6.4 / Lock 6.F)
// -----------------------------------------------------------------------

impl Core {
    /// Tear `node_id` down. Per R2.6.4 / Lock 6.F:
    ///
    /// - **Auto-prepend COMPLETE.** If the node has not yet emitted a
    ///   terminal (`COMPLETE` / `ERROR`), `terminate_node` is called with
    ///   `Complete` first so subscribers see `[COMPLETE, TEARDOWN]`, not
    ///   bare `[TEARDOWN]`. This guarantees a clean end-of-stream signal
    ///   to async iterators and other consumers that wait on terminal
    ///   delivery.
    /// - **Idempotent on duplicate delivery.** The per-node
    ///   `has_received_teardown` flag is set on the first call; subsequent
    ///   `teardown` calls (or cascade visits from other paths) are silent
    ///   no-ops — no second `[COMPLETE, TEARDOWN]` pair to subscribers.
    /// - **Cascade downstream.** Each child is recursively torn down. The
    ///   child's own COMPLETE auto-cascades from `terminate_node`'s logic
    ///   (Lock 2.B); its TEARDOWN comes from this cascade.
    ///
    /// # Panics
    ///
    /// Panics if `node_id` is unknown.
    pub fn teardown(&self, node_id: NodeId) {
        {
            let s = self.lock_state();
            assert!(s.nodes.contains_key(&node_id), "unknown node {node_id:?}");
        }
        let torn_down: Arc<Mutex<Vec<NodeId>>> = Arc::new(Mutex::new(Vec::new()));
        let torn_down_for_wave = torn_down.clone();
        self.run_wave(move |this| {
            let mut s = this.lock_state();
            let collected = this.teardown_inner(&mut s, node_id);
            torn_down_for_wave.lock().extend(collected);
        });
        // Fire NodeTornDown for every cascaded id (root + metas +
        // downstream consumers that auto-cascaded). Outside the state
        // lock, matching fire_topology_event discipline.
        let ids = std::mem::take(&mut *torn_down.lock());
        for id in ids {
            self.fire_topology_event(&crate::topology::TopologyEvent::NodeTornDown(id));
        }
    }

    /// Iterative teardown walk (Slice A-bigger, M1-close).
    ///
    /// The recursive shape was:
    ///   ```text
    ///   teardown(n):
    ///     if torn_down: return
    ///     mark torn_down
    ///     for meta in metas: teardown(meta)
    ///     terminate_node + queue Teardown
    ///     for child in children: teardown(child)
    ///   ```
    /// Deep linear chains (~10k nodes) overflowed the OS thread stack.
    ///
    /// The iterative shape uses a `Vec<Action>` stack with `Visit` and
    /// `EmitTeardown` actions. `Visit(n)` marks `n` torn-down (or no-ops
    /// if already), then pushes (in reverse order so LIFO pops in forward
    /// order) `Visit(child_K), …, Visit(child_1), EmitTeardown(n),
    /// Visit(meta_M), …, Visit(meta_1)`. The R1.3.9.d "metas first, then
    /// self, then children" ordering is preserved by the push order:
    /// metas pop first, recursively expand and emit; then `EmitTeardown(n)`
    /// pops and runs `terminate_node` + queue `Teardown`; then children
    /// pop. Idempotency via `has_received_teardown` keeps each node
    /// visited at most once even when multi-parent diamonds re-enter via
    /// a sibling path.
    fn teardown_inner(&self, s: &mut CoreState, root: NodeId) -> Vec<NodeId> {
        enum Action {
            Visit(NodeId),
            EmitTeardown(NodeId),
        }
        let mut stack: Vec<Action> = vec![Action::Visit(root)];
        // Topology accumulator: every node that actually emits TEARDOWN
        // (i.e. each `EmitTeardown(id)` site, NOT each `Visit` — visits
        // for already-torn-down nodes short-circuit on idempotency).
        let mut torn_down: Vec<NodeId> = Vec::new();
        while let Some(action) = stack.pop() {
            match action {
                Action::Visit(id) => {
                    if s.require_node(id).has_received_teardown {
                        continue; // Idempotent (R2.6.4).
                    }
                    s.require_node_mut(id).has_received_teardown = true;
                    // Push order: children first (pop LAST), then
                    // EmitTeardown(id), then metas (pop FIRST). Reverse
                    // each list so within-group order matches the original
                    // recursive iteration.
                    let children: Vec<NodeId> = s
                        .children
                        .get(&id)
                        .map(|c| c.iter().copied().collect())
                        .unwrap_or_default();
                    for &child in children.iter().rev() {
                        stack.push(Action::Visit(child));
                    }
                    stack.push(Action::EmitTeardown(id));
                    let metas: Vec<NodeId> = s.require_node(id).meta_companions.clone();
                    for &meta in metas.iter().rev() {
                        stack.push(Action::Visit(meta));
                    }
                }
                Action::EmitTeardown(id) => {
                    // Auto-prepend COMPLETE if not yet terminal. The (now
                    // iterative) terminate_node handles auto-cascade to
                    // children's own terminal slots per Lock 2.B.
                    let already_terminal = s.require_node(id).terminal.is_some();
                    if !already_terminal {
                        self.terminate_node(s, id, TerminalKind::Complete);
                    }
                    // Wire emission of the TEARDOWN itself (tier 6).
                    self.queue_notify(s, id, Message::Teardown);
                    torn_down.push(id);
                }
            }
        }
        torn_down
    }

    /// Attach `companion` as a meta companion of `parent` per R1.3.9.d.
    /// Meta companions are nodes whose lifecycle is bound to the parent's
    /// in TEARDOWN ordering: when `parent` tears down, `companion` tears
    /// down first.
    ///
    /// Use this for inspection / audit / sidecar nodes that subscribe to
    /// parent state — without the ordering, the companion could observe
    /// the parent mid-destruction and emit garbage.
    ///
    /// Idempotent on duplicate registration of the same companion.
    ///
    /// # Lifecycle constraint
    ///
    /// Intended for **setup-time** wiring — call this before `parent` or
    /// `companion` enters a wave. Mid-wave registration (especially during
    /// a teardown cascade in flight) is implementation-defined: the new
    /// edge takes effect on the *next* wave. Adding a companion to a
    /// torn-down parent silently no-ops (the parent will not tear down
    /// again). For dynamic companion attachment with deterministic
    /// ordering, prefer constructing the wiring before subscribers exist.
    ///
    /// # Panics
    ///
    /// Panics if either node id is unknown, or if `parent == companion`
    /// (a node cannot be its own meta companion — would loop on TEARDOWN).
    pub fn add_meta_companion(&self, parent: NodeId, companion: NodeId) {
        assert!(parent != companion, "node cannot be its own meta companion");
        let mut s = self.lock_state();
        assert!(s.nodes.contains_key(&parent), "unknown parent {parent:?}");
        assert!(
            s.nodes.contains_key(&companion),
            "unknown companion {companion:?}"
        );
        let metas = &mut s.require_node_mut(parent).meta_companions;
        if !metas.contains(&companion) {
            metas.push(companion);
        }
    }
}

// -----------------------------------------------------------------------
// INVALIDATE — cache clear + downstream cascade
// -----------------------------------------------------------------------

impl Core {
    /// Clear `node_id`'s cache and cascade `[INVALIDATE]` to downstream
    /// dependents per canonical spec §1.4.
    ///
    /// Semantics:
    /// - **Never-populated case (R1.4 line 197):** if `cache == NO_HANDLE`,
    ///   the call is a no-op — no cache to clear, no INVALIDATE emitted.
    ///   This naturally provides idempotency within a wave: once a node has
    ///   been invalidated this wave (cache = NO_HANDLE), a second invalidate
    ///   on the same node does nothing.
    /// - **Cache clear (immediate):** the node's cached handle is dropped
    ///   (refcount released), `cache` becomes `NO_HANDLE`. State nodes
    ///   keep `has_fired_once` per spec — INVALIDATE is not a re-gating
    ///   event (the next emission to a previously-fired state still does
    ///   not re-trigger the first-run gate; that's a resubscribable-terminal
    ///   lifecycle concern, separate slice).
    /// - **Wire emission (tier 4):** `[INVALIDATE]` is queued via the
    ///   normal pause-aware notify path. Buffers while paused, flushes
    ///   immediately otherwise.
    /// - **Downstream cascade:** for each child of this node, the child's
    ///   `dep_handles[idx_of_node]` is reset to `NO_HANDLE` (its previous
    ///   value referenced a now-released handle). The child is then
    ///   recursively invalidated (no-op if its cache was already
    ///   `NO_HANDLE`). This re-closes the child's first-run gate — fn
    ///   won't fire again until the upstream re-emits a value.
    ///
    /// Wraps in a fresh wave when called from outside a wave, so
    /// notifications flush at the natural wave boundary.
    ///
    /// # Panics
    ///
    /// Panics if `node_id` is unknown, consistent with `emit` / `pause`.
    pub fn invalidate(&self, node_id: NodeId) {
        {
            let s = self.lock_state();
            assert!(s.nodes.contains_key(&node_id), "unknown node {node_id:?}");
        }
        self.run_wave(|this| {
            let mut s = this.lock_state();
            this.invalidate_inner(&mut s, node_id);
        });
    }

    /// Iterative invalidate cascade (Slice A-bigger, M1-close).
    ///
    /// The recursive shape was a depth-first cache-clear walk:
    ///   ```text
    ///   invalidate(n):
    ///     if cache(n) == NO_HANDLE: return  // already-invalidated guard
    ///     cache(n) = NO_HANDLE; release handle
    ///     queue Invalidate(n)
    ///     for child in children:
    ///       child.dep_handles[idx] = NO_HANDLE
    ///       invalidate(child)
    ///   ```
    /// Deep linear chains overflowed the OS thread stack. The work-queue
    /// rewrite has no ordering subtleties (unlike teardown's R1.3.9.d
    /// metas-first constraint) — Invalidate is a tier-4 broadcast where
    /// the never-populated / already-invalidated guard provides natural
    /// idempotency for diamond fan-in.
    fn invalidate_inner(&self, s: &mut CoreState, root: NodeId) {
        let mut work: Vec<NodeId> = vec![root];
        while let Some(node_id) = work.pop() {
            // Never-populated / already-invalidated: no-op (R1.4 idempotency).
            let old_handle = s.require_node(node_id).cache;
            if old_handle == NO_HANDLE {
                continue;
            }
            // Clear cache + release the handle's slot ownership.
            s.require_node_mut(node_id).cache = NO_HANDLE;
            self.binding.release_handle(old_handle);
            // Wire emission. Pause-aware via queue_notify.
            self.queue_notify(s, node_id, Message::Invalidate);
            // Cascade: for each child, clear the dep record's prev_data
            // referencing this node and push child onto the work queue.
            let child_ids: Vec<NodeId> = s
                .children
                .get(&node_id)
                .map(|c| c.iter().copied().collect())
                .unwrap_or_default();
            for child_id in child_ids {
                let dep_idx = s.require_node(child_id).dep_index_of(node_id);
                if let Some(idx) = dep_idx {
                    // Reset the child's dep record — the handle was just
                    // released. Subsequent first-run-gate checks see
                    // sentinel and re-close.
                    //
                    // Snapshot prev_data + data_batch retains for deferred
                    // release, then clear the record. Two-phase to satisfy
                    // the borrow checker (nodes + deferred_handle_releases
                    // are separate CoreState fields).
                    let (old_prev, batch_hs): (HandleId, SmallVec<[HandleId; 1]>) = {
                        let dr = &s.require_node(child_id).dep_records[idx];
                        (dr.prev_data, dr.data_batch.clone())
                    };
                    if old_prev != NO_HANDLE {
                        s.deferred_handle_releases.push(old_prev);
                    }
                    for h in batch_hs {
                        s.deferred_handle_releases.push(h);
                    }
                    let dr = &mut s.require_node_mut(child_id).dep_records[idx];
                    dr.prev_data = NO_HANDLE;
                    dr.data_batch.clear();
                    work.push(child_id);
                }
            }
        }
    }
}

// -----------------------------------------------------------------------
// PAUSE / RESUME — multi-pauser lockset + replay buffer
// -----------------------------------------------------------------------

/// Reported back from [`Core::resume`] when the final lock releases.
///
/// `replayed` is the number of tier-3/tier-4 messages dispatched to
/// subscribers as part of the drain. `dropped` is the number of messages
/// that fell out the front of the buffer due to the Core-global
/// `pause_buffer_cap` while this pause cycle was active. A non-zero
/// `dropped` indicates a controller held the lock long enough to overflow
/// the cap; the binding may want to surface a warning or error.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ResumeReport {
    pub replayed: u32,
    pub dropped: u32,
}

impl Core {
    /// Acquire a pause lock on `node_id`. The first lock transitions the
    /// node from `Active` to `Paused`; further locks add to the lockset.
    /// While paused, tier-3 (DATA/RESOLVED) and tier-4 (INVALIDATE) outgoing
    /// messages buffer in the node's pause buffer; other tiers flush
    /// immediately.
    ///
    /// Re-acquiring the same `lock_id` is an idempotent no-op (matches TS
    /// convention, R1.2.6 silent on the case).
    pub fn pause(&self, node_id: NodeId, lock_id: LockId) -> Result<(), PauseError> {
        let mut s = self.lock_state();
        let rec = s
            .nodes
            .get_mut(&node_id)
            .ok_or(PauseError::UnknownNode(node_id))?;
        rec.pause_state.add_lock(lock_id);
        Ok(())
    }

    /// Release a pause lock on `node_id`. If the lockset becomes empty, the
    /// node transitions back to `Active` and the buffered messages are
    /// dispatched to subscribers in arrival order. Returns a [`ResumeReport`]
    /// when the final lock released; `None` if the lockset is still
    /// non-empty (further locks held).
    ///
    /// Releasing an unknown `lock_id` (or releasing on an already-Active
    /// node) is an idempotent no-op returning `None`.
    pub fn resume(
        &self,
        node_id: NodeId,
        lock_id: LockId,
    ) -> Result<Option<ResumeReport>, PauseError> {
        // Phase 1 (lock-held): collect drained buffer + sink Arcs.
        let (sinks, messages, dropped) = {
            let mut s = self.lock_state();
            let rec = s
                .nodes
                .get_mut(&node_id)
                .ok_or(PauseError::UnknownNode(node_id))?;
            let Some((buffer, dropped)) = rec.pause_state.remove_lock(lock_id) else {
                return Ok(None);
            };
            let sinks: Vec<Sink> = rec.subscribers.values().cloned().collect();
            let messages: Vec<Message> = buffer.into_iter().collect();
            (sinks, messages, dropped)
        };
        let replayed = u32::try_from(messages.len()).unwrap_or(u32::MAX);

        // Phase 2 (lock-released): fire sinks. Re-entrance into Core
        // methods from a sink would re-acquire the parking_lot::Mutex; per
        // the v1 limitation that's documented as forbidden, but we still
        // release the lock here so a sink that *only* needs the binding
        // (e.g., resolves a handle to a value via the registry) doesn't
        // hold the Core lock unnecessarily during user callback execution.
        // This deviates slightly from `flush_notifications` (which fires
        // sinks with the lock held); resume is the only path that drops
        // the lock for sink fires today, justified by it being a discrete
        // out-of-wave dispatch.
        if !messages.is_empty() {
            for sink in &sinks {
                sink(&messages);
            }
            // Phase 3: balance the retain_handle calls done at buffer-push
            // time — sinks observe values but don't own refcount shares.
            for msg in &messages {
                if let Some(h) = msg.payload_handle() {
                    self.binding.release_handle(h);
                }
            }
        }
        Ok(Some(ResumeReport { replayed, dropped }))
    }

    /// True if the node currently holds at least one pause lock.
    #[must_use]
    pub fn is_paused(&self, node_id: NodeId) -> bool {
        self.state
            .lock()
            .require_node(node_id)
            .pause_state
            .is_paused()
    }

    /// Number of pause locks currently held on `node_id`. `0` if Active.
    #[must_use]
    pub fn pause_lock_count(&self, node_id: NodeId) -> usize {
        self.state
            .lock()
            .require_node(node_id)
            .pause_state
            .lock_count()
    }

    /// Test helper: whether `node_id` currently holds the given `lock_id`.
    #[must_use]
    pub fn holds_pause_lock(&self, node_id: NodeId, lock_id: LockId) -> bool {
        self.state
            .lock()
            .require_node(node_id)
            .pause_state
            .contains_lock(lock_id)
    }
}

// -----------------------------------------------------------------------
// set_deps — atomic dep mutation
// -----------------------------------------------------------------------

/// Errors returnable by [`Core::set_deps`].
///
/// Per `~/src/graphrefly-ts/docs/research/rewire-design-notes.md` and the
/// Phase 13.8 Q1 lock:
/// - `SelfDependency` — `n in newDeps` (self-loops are pathological without
///   explicit fixed-point semantics, which GraphReFly does not provide).
/// - `WouldCreateCycle { path }` — adding the new edge would create a cycle.
///   The `path` field reports the offending dep chain for debuggability.
/// - `UnknownNode` / `NotComputeNode` — invariant violations from the caller.
/// - `TerminalNode` — `n` itself has emitted COMPLETE/ERROR; rewiring a
///   terminal stream is a category error (terminal is one-shot at this
///   layer; recovery is the resubscribable path on a fresh subscribe).
/// - `TerminalDep` — a newly-added dep is terminal AND not resubscribable.
///   Resubscribable terminal deps are accepted because the subscribe path
///   resets their lifecycle. Non-resubscribable terminal deps would deliver
///   their already-emitted terminal directly to `n`'s `dep_terminals` slot,
///   which is rarely intended.
#[derive(Error, Debug, Clone, PartialEq)]
pub enum SetDepsError {
    /// `n` appeared in `new_deps` (self-loop rejection).
    #[error("set_deps({n:?}, ...): self-dependency rejected (n appeared in new_deps)")]
    SelfDependency { n: NodeId },

    /// Adding the new dep would create a cycle. `path` is the chain
    /// `[added_dep, ..., n]` reachable via existing deps.
    #[error(
        "set_deps({n:?}, ...): cycle would form via path {path:?} \
         (adding {added_dep:?} → {n:?} closes the loop)"
    )]
    WouldCreateCycle {
        n: NodeId,
        added_dep: NodeId,
        path: Vec<NodeId>,
    },

    #[error("set_deps: unknown node {0:?}")]
    UnknownNode(NodeId),

    #[error("set_deps: node {0:?} is not a compute node (state nodes have no deps)")]
    NotComputeNode(NodeId),

    /// `n` itself has terminated (COMPLETE / ERROR). Rewiring a terminal node
    /// is rejected — the stream has ended at this layer. To recover, mark
    /// the node resubscribable before terminate; a fresh subscribe will then
    /// reset its lifecycle.
    #[error("set_deps({n:?}, ...): node has already terminated; cannot rewire a terminal node")]
    TerminalNode { n: NodeId },

    /// A newly-added dep is terminal AND non-resubscribable. Per Phase 13.8
    /// Q1, this is rejected; resubscribable terminal deps are allowed
    /// because the subscribe path resets them when activated. Already-present
    /// terminal deps are unaffected (their terminal status was accepted at
    /// the time they terminated).
    #[error(
        "set_deps({n:?}, ...): added dep {dep:?} is terminal and not resubscribable; \
         either mark it resubscribable before terminate, or remove the dep from new_deps"
    )]
    TerminalDep { n: NodeId, dep: NodeId },
}

impl Core {
    /// Atomic dep mutation — change a node's upstream deps without TEARDOWN
    /// cascading and without losing cache.
    ///
    /// Per the TLA+-verified design at
    /// `~/src/graphrefly-ts/docs/research/wave_protocol_rewire.tla`
    /// (35,950 distinct states, all 7 invariants clean):
    ///
    /// - Removed deps: clear dirtyMask bit, drain pending queue, drop DepRecord.
    /// - Added deps: SENTINEL prevData; push-on-subscribe if added dep has cached DATA.
    /// - Preserved: `firstRunPassed`, `pauseLocks`, `pauseBuffer`, `cache` (ROM/RAM).
    /// - Status auto-settles if dirtyMask becomes empty.
    /// - Idempotent on `new_deps == current deps`.
    /// - Self-rewire `n ∈ new_deps` rejected (`SelfDependency`).
    /// - Cycles rejected (`WouldCreateCycle`).
    /// - Allowed mid-wave + while paused.
    /// - Phase 13.8 Q1: terminal `n` rejected (`TerminalNode`); newly-added
    ///   terminal non-resubscribable deps rejected (`TerminalDep`).
    ///
    /// The body is a single atomic dep-mutation transaction with several
    /// discrete validation stages. Splitting would require passing a
    /// partially-mutable CoreState across helpers, and the transaction's
    /// locality is what makes the F1 refcount-leak collection work.
    #[allow(clippy::too_many_lines)]
    pub fn set_deps(&self, n: NodeId, new_deps: &[NodeId]) -> Result<(), SetDepsError> {
        let mut s = self.lock_state();
        // Validate node exists and is compute. Read-once via the helper so
        // subsequent code can use `require_node(n)` without re-checking.
        let (kind, is_terminal) = {
            let rec = s.nodes.get(&n).ok_or(SetDepsError::UnknownNode(n))?;
            (rec.kind, rec.terminal.is_some())
        };
        if kind == NodeKind::State {
            return Err(SetDepsError::NotComputeNode(n));
        }
        // Reject if `n` itself is terminal (Phase 13.8 Q1: terminal nodes
        // cannot be rewired; recovery is via resubscribable subscribe).
        if is_terminal {
            return Err(SetDepsError::TerminalNode { n });
        }
        // Self-rewire rejection.
        if new_deps.contains(&n) {
            return Err(SetDepsError::SelfDependency { n });
        }
        // Validate all new deps exist.
        for &d in new_deps {
            if !s.nodes.contains_key(&d) {
                return Err(SetDepsError::UnknownNode(d));
            }
        }
        // Cycle detection: data flows parent → child via the `children` map.
        // Adding edge `d → n` (d becomes a dep of n) creates a cycle iff
        // `d` is already reachable from `n` via existing data-flow edges
        // (so `n → ... → d` exists, and the new `d → n` closes the loop).
        // DFS from `n` along `children` edges, looking for each added dep.
        let current_deps: HashSet<NodeId> = s.require_node(n).dep_ids().collect();
        let new_deps_set: HashSet<NodeId> = new_deps.iter().copied().collect();
        let added: HashSet<NodeId> = new_deps_set.difference(&current_deps).copied().collect();
        for &d in &added {
            if let Some(path) = self.path_from_to(&s, n, d) {
                return Err(SetDepsError::WouldCreateCycle {
                    n,
                    added_dep: d,
                    path,
                });
            }
        }
        // Phase 13.8 Q1: reject newly-added deps that are terminal AND not
        // resubscribable. Resubscribable terminal deps are allowed — the
        // subscribe path resets their lifecycle when something activates
        // them. Already-present (kept) deps are unaffected; their terminal
        // status was accepted at the time they terminated.
        for &d in &added {
            let dep_rec = s.require_node(d);
            if dep_rec.terminal.is_some() && !dep_rec.resubscribable {
                return Err(SetDepsError::TerminalDep { n, dep: d });
            }
        }
        // Idempotent fast-path.
        if added.is_empty() && current_deps == new_deps_set {
            return Ok(());
        }
        let removed: HashSet<NodeId> = current_deps.difference(&new_deps_set).copied().collect();

        // Snapshot old deps (ordered) for topology event, before mutation.
        let old_deps_vec: Vec<NodeId> = s.require_node(n).dep_ids_vec();

        // Carry out the rewire atomically.
        // 1. Build new dep_records, preserving DepRecord state for kept deps.
        let new_deps_vec: Vec<NodeId> = new_deps.to_vec();
        //
        // Refcount discipline (F1 audit fix): each `Some(TerminalKind::Error(h))`
        // slot owns a refcount share retained at `terminate_node` time. When a
        // dep is REMOVED, its slot is dropped — the corresponding handle's
        // share must be released here, otherwise it leaks until Core drop.
        // Also release data_batch retains for removed deps.
        let (new_dep_records, removed_handles): (Vec<DepRecord>, Vec<HandleId>) = {
            let rec = s.require_node(n);
            // Index old dep_records by NodeId for O(1) lookup of kept deps.
            let old_by_node: HashMap<NodeId, &DepRecord> =
                rec.dep_records.iter().map(|dr| (dr.node, dr)).collect();
            let new_records: Vec<DepRecord> = new_deps_vec
                .iter()
                .map(|&d| {
                    if let Some(old) = old_by_node.get(&d) {
                        // Kept dep: preserve all state (prev_data, data_batch,
                        // terminal, wave flags). Subscriptions stay live.
                        DepRecord {
                            node: d,
                            prev_data: old.prev_data,
                            dirty: old.dirty,
                            involved_this_wave: old.involved_this_wave,
                            data_batch: old.data_batch.clone(),
                            terminal: old.terminal,
                        }
                    } else {
                        // Added dep: fresh sentinel record.
                        DepRecord::new(d)
                    }
                })
                .collect();
            // Collect handles to release from REMOVED dep records.
            let mut to_release: Vec<HandleId> = Vec::new();
            for d in &removed {
                if let Some(old) = old_by_node.get(d) {
                    if let Some(TerminalKind::Error(h)) = old.terminal {
                        to_release.push(h);
                    }
                    // Release data_batch retains for removed deps.
                    for &h in &old.data_batch {
                        to_release.push(h);
                    }
                }
            }
            (new_records, to_release)
        };
        // Clear dirtyMask bit by re-emitting the wave-bookkeeping: we don't
        // currently model a per-dep dirtyMask explicitly (we use the boolean
        // `dirty` flag at node level). Removing a dep's entry from the implicit
        // mask is therefore implicit — by removing the dep, future emissions
        // from it can't re-arm the bit. The per-dep `involved_this_wave` flag
        // stays wave-scoped and gets cleared at wave end. The setDeps action
        // itself does NOT change the dirty boolean unless all deps are cleared;
        // in that case we settle.
        {
            let rec = s.require_node_mut(n);
            rec.dep_records = new_dep_records;
            // Re-derive `tracked` for static derived: all indices.
            // For dynamic: clear `tracked` AND reset `has_fired_once` so the
            // next dep delivery satisfies the first-fire branch in
            // `deliver_data_to_consumer` (`!has_fired_once || tracked.contains(...)`).
            // Without resetting `has_fired_once`, the cleared `tracked` blocks
            // every future fire — fn never re-runs and the dynamic node sits
            // on stale cache derived from the old dep set. The next fire
            // re-runs fn unconditionally; fn's returned `tracked` then
            // repopulates `rec.tracked` and normal selective-deps semantics
            // resume from the next dep update onward.
            if rec.kind == NodeKind::Derived {
                rec.tracked = (0..new_deps_vec.len()).collect();
            } else if rec.kind == NodeKind::Dynamic {
                rec.tracked.clear();
                rec.has_fired_once = false;
            }
        }

        // 2. Update inverted-edge map (children).
        for &removed_dep in &removed {
            if let Some(set) = s.children.get_mut(&removed_dep) {
                set.remove(&n);
            }
        }
        for &added_dep in &added {
            s.children.entry(added_dep).or_default().insert(n);
        }

        // 3. Push-on-subscribe for added deps with cached DATA. Wraps in a
        // wave so any downstream propagation runs cleanly. We capture only
        // the LIST of added deps (not their cache values) because the cache
        // can change between releasing the validation lock and the wave's
        // re-acquisition — see the P2 race fix below.
        //
        // P2 (Slice A close /qa) — between `drop(s)` and `run_wave`'s
        // closure re-acquiring the lock, a concurrent thread could
        // invalidate one of the added deps, releasing its cache handle. A
        // pre-snapshot of `(added_dep, cache)` pairs would then carry a
        // dangling HandleId into `deliver_data_to_consumer`. The fix is to
        // re-read each added dep's `cache` INSIDE the closure (under the
        // freshly re-acquired state lock). The wave-owner re-entrant mutex
        // (Q2) blocks concurrent waves once we enter `run_wave`, so the
        // re-read sees a coherent post-validation state.
        let added_for_wave: Vec<NodeId> = added.iter().copied().collect();
        // Drop the state lock before run_wave (which acquires its own) and
        // before crossing the binding boundary for the F1 refcount-fix
        // releases. Keeps the lock-discipline split (binding calls outside
        // the state lock) consistent with the rest of the dispatcher.
        drop(s);
        // Fire topology event after lock is dropped.
        self.fire_topology_event(&crate::topology::TopologyEvent::DepsChanged {
            node: n,
            old_deps: old_deps_vec,
            new_deps: new_deps_vec.clone(),
        });
        if !added_for_wave.is_empty() {
            self.run_wave(|this| {
                let mut s = this.lock_state();
                // Defensive: re-validate `n` still exists and isn't terminal.
                // A concurrent path could have terminated it between
                // validation and run_wave's wave_owner acquisition.
                if !s.nodes.contains_key(&n) || s.require_node(n).terminal.is_some() {
                    return;
                }
                for added_dep in &added_for_wave {
                    // Re-read cache under the wave-owner-held lock — this
                    // is the post-validation, post-concurrent-action
                    // snapshot. NO_HANDLE means the dep was invalidated
                    // concurrently; skip (no data to push).
                    let cache = match s.nodes.get(added_dep) {
                        Some(rec) => rec.cache,
                        None => continue, // dep deleted concurrently
                    };
                    if cache == NO_HANDLE {
                        continue;
                    }
                    let dep_idx = s.require_node(n).dep_index_of(*added_dep);
                    if let Some(idx) = dep_idx {
                        this.deliver_data_to_consumer(&mut s, n, idx, cache);
                    }
                }
            });
        }
        for h in removed_handles {
            self.binding.release_handle(h);
        }
        Ok(())
    }

    /// DFS from `from` along data-flow edges (children map) looking for `to`.
    /// Returns the path including endpoints, or `None` if unreachable. Used
    /// for cycle detection in [`Self::set_deps`].
    fn path_from_to(&self, s: &CoreState, from: NodeId, to: NodeId) -> Option<Vec<NodeId>> {
        if from == to {
            return Some(vec![from]);
        }
        let mut stack: Vec<(NodeId, Vec<NodeId>)> = vec![(from, vec![from])];
        let mut visited: HashSet<NodeId> = HashSet::new();
        while let Some((cur, path)) = stack.pop() {
            if !visited.insert(cur) {
                continue;
            }
            if cur == to {
                return Some(path);
            }
            if let Some(children) = s.children.get(&cur) {
                for &child in children {
                    let mut new_path = path.clone();
                    new_path.push(child);
                    stack.push((child, new_path));
                }
            }
        }
        None
    }
}

// CoreState helpers — kept on the inner struct so they're naturally scoped
// to the lock guard.
impl CoreState {
    fn alloc_node_id(&mut self) -> NodeId {
        let id = NodeId::new(self.next_node_id);
        self.next_node_id += 1;
        id
    }

    fn alloc_sub_id(&mut self) -> SubscriptionId {
        let id = SubscriptionId(self.next_subscription_id);
        self.next_subscription_id += 1;
        id
    }

    /// Clear wave-scoped flags and rotate per-dep batch data on every
    /// node. Run at the end of every wave (regular drain via `run_wave`,
    /// activation drain via `activate_derived`, and `BatchGuard::drop`'s
    /// drain). Centralized so a future wave-state field can't be missed
    /// at one of the cleanup sites.
    ///
    /// Per-dep rotation (R2.9.b / R1.3.6.b):
    /// - `prev_data` ← last element of `data_batch` (or unchanged if empty).
    ///   The last batch entry's retain transfers to `prev_data`; the old
    ///   `prev_data`'s retain is released. All earlier batch entries are
    ///   released.
    /// - `data_batch` cleared.
    /// - Per-dep `dirty` and `involved_this_wave` cleared.
    ///
    /// Handle releases are pushed to `deferred_handle_releases` for
    /// post-lock-drop release by the caller.
    pub(crate) fn clear_wave_state(&mut self) {
        self.pending_auto_resolve.clear();
        for rec in self.nodes.values_mut() {
            rec.dirty = false;
            rec.involved_this_wave = false;
            for dr in &mut rec.dep_records {
                let batch_len = dr.data_batch.len();
                if batch_len > 0 {
                    // Release all batch entries EXCEPT the last — the last
                    // entry's retain transfers to prev_data.
                    for &h in &dr.data_batch[..batch_len - 1] {
                        self.deferred_handle_releases.push(h);
                    }
                    // Release the OLD prev_data (its retain was from the
                    // previous wave's rotation or from initial delivery).
                    if dr.prev_data != NO_HANDLE {
                        self.deferred_handle_releases.push(dr.prev_data);
                    }
                    // Rotate: last batch entry becomes new prev_data.
                    // Its retain carries over — no extra retain needed.
                    dr.prev_data = dr.data_batch[batch_len - 1];
                    dr.data_batch.clear();
                }
                dr.dirty = false;
                dr.involved_this_wave = false;
            }
        }
    }

    pub(crate) fn require_node(&self, id: NodeId) -> &NodeRecord {
        self.nodes
            .get(&id)
            .unwrap_or_else(|| panic!("unknown node {id:?}"))
    }

    pub(crate) fn require_node_mut(&mut self, id: NodeId) -> &mut NodeRecord {
        self.nodes
            .get_mut(&id)
            .unwrap_or_else(|| panic!("unknown node {id:?}"))
    }
}

/// Release every binding-side refcount share owned by this `CoreState`
/// when the last `Core` clone drops the inner Mutex.
///
/// Without this, every retained handle in `cache` / `terminal` Error /
/// `dep_terminals` Error / pause-buffer-payload would leak in the binding
/// registry until process exit. Production bindings (napi-rs, pyo3,
/// wasm-bindgen) all maintain handle-ref maps that grow unbounded without
/// this cleanup.
///
/// Safe to call during panic unwinding — `BindingBoundary::release_handle`
/// is the only call, and a panicking binding during cleanup would already
/// have been a problem in normal operation.
impl Drop for CoreState {
    fn drop(&mut self) {
        // Drain pending in-flight retains too, so a panic mid-wave doesn't
        // strand the queue_notify retains in `deferred_handle_releases`.
        let pending = std::mem::take(&mut self.pending_notify);
        let deferred_releases = std::mem::take(&mut self.deferred_handle_releases);
        // `deferred_flush_jobs` carries `Vec<Sink>` clones — those Arcs
        // drop naturally when this CoreState drops; no handles to release
        // there.
        let _ = std::mem::take(&mut self.deferred_flush_jobs);

        // Per-node retained handles:
        //   - `cache` (1 retain per non-NO_HANDLE state cache or
        //     populated compute cache).
        //   - `terminal == Some(Error(h))` (1 retain on the terminal slot).
        //   - `dep_terminals[i] == Some(Error(h))` (1 retain per consumer's
        //     terminated-dep slot).
        //   - `pause_state` paused buffer messages with payload handles
        //     (1 retain per buffered Data/Error).
        for rec in self.nodes.values() {
            if rec.cache != NO_HANDLE {
                self.binding.release_handle(rec.cache);
            }
            if let Some(TerminalKind::Error(h)) = rec.terminal {
                self.binding.release_handle(h);
            }
            for dr in &rec.dep_records {
                if let Some(TerminalKind::Error(h)) = dr.terminal {
                    self.binding.release_handle(h);
                }
                // Release data_batch retains (in-flight wave data).
                for &h in &dr.data_batch {
                    self.binding.release_handle(h);
                }
                // Release prev_data retain (cross-wave persistence).
                if dr.prev_data != NO_HANDLE {
                    self.binding.release_handle(dr.prev_data);
                }
            }
            if let PauseState::Paused { buffer, .. } = &rec.pause_state {
                for msg in buffer {
                    if let Some(h) = msg.payload_handle() {
                        self.binding.release_handle(h);
                    }
                }
            }
        }

        // Pending wave retains.
        for entry in pending.values() {
            for msg in &entry.messages {
                if let Some(h) = msg.payload_handle() {
                    self.binding.release_handle(h);
                }
            }
        }
        for h in deferred_releases {
            self.binding.release_handle(h);
        }
        // Wave-cache snapshot retains (defensive — should normally be
        // empty by the time Core drops, but a panicked-mid-wave Core
        // could leave them populated).
        let snapshots = std::mem::take(&mut self.wave_cache_snapshots);
        for (_, h) in snapshots {
            self.binding.release_handle(h);
        }
    }
}
