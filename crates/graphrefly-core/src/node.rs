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
//! # Out of scope (later slices / milestones)
//!
//! - Deactivation cleanup (RAM nodes clear cache when sink count → 0) — M2.
//! - Property-test fixtures via `proptest` — Slice C.
//! - `batch.rs` extraction (wave coalescing into its own module) — Slice C.
//! - ReentrantMutex / lock-released sink fire — M1-close hardening.
//!
//! See [`migration-status.md`](../../../docs/migration-status.md) for the
//! milestone tracker and [`porting-deferred.md`](../../../docs/porting-deferred.md)
//! for surfaced concerns deferred to evidence-driven slices.
//!
//! # Re-entrance discipline
//!
//! The dispatcher uses `parking_lot::Mutex<CoreState>`. Sinks fire while the
//! lock is held — so a sink that calls back into `Core::emit` would deadlock.
//! v1 bench tests do not re-enter from sinks. Production hardening will move
//! to `ReentrantMutex` with internal `RefCell`-style cells, OR collect
//! notifications inside the lock and fire outside; that's a follow-up.

use std::collections::VecDeque;
use std::sync::{Arc, Weak};

use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use indexmap::IndexMap;
use parking_lot::Mutex;
use smallvec::SmallVec;
use thiserror::Error;

use crate::boundary::{BindingBoundary, FnResult};
use crate::clock::monotonic_ns;
use crate::handle::{FnId, HandleId, LockId, NodeId, NO_HANDLE};
use crate::message::Message;

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
enum DepTerminal {
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
enum PauseState {
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
    fn is_paused(&self) -> bool {
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
    fn push_buffered(&mut self, msg: Message, cap: Option<usize>) -> Vec<Message> {
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

/// Internal node record. Mirrors `core.ts:132–154`.
///
/// The 5 bool fields (`has_fired_once`, `dirty`, `involved_this_wave`,
/// `has_received_teardown`, `resubscribable`) each represent an orthogonal
/// lifecycle concern. Collapsing them into a bitfield would obscure intent
/// and complicate the wave-cleanup phase that resets only the wave-scoped
/// pair (`dirty`, `involved_this_wave`).
#[allow(clippy::struct_excessive_bools)]
struct NodeRecord {
    deps: Vec<NodeId>,
    kind: NodeKind,
    fn_id: Option<FnId>,
    equals: EqualsMode,

    // Mutable state
    cache: HandleId,
    /// Per-dep handle of last DATA seen. Same length as `deps`.
    dep_handles: Vec<HandleId>,
    has_fired_once: bool,
    subscribers: HashMap<SubscriptionId, Sink>,
    /// For dynamic nodes: which dep indices fn actually tracks.
    /// For static derived: all indices, populated at construction.
    tracked: HashSet<usize>,

    // Wave-scoped state — cleared at wave end.
    dirty: bool,
    involved_this_wave: bool,

    /// Per-node pause state. Default `Active`. See [`PauseState`].
    pause_state: PauseState,

    /// Terminal lifecycle state for THIS node's outgoing stream. Once set,
    /// further `emit` calls are silent no-ops, fn no longer fires, and only
    /// the terminal message has been queued downstream.
    terminal: Option<DepTerminal>,
    /// Per-dep terminal slots aligned with `deps`. None = dep is live.
    /// Some = dep emitted COMPLETE/ERROR. When ALL entries are Some, the
    /// node auto-cascades its own terminal per Lock 2.B (ERROR dominates
    /// COMPLETE — first error wins; if no errors, COMPLETE).
    dep_terminals: Vec<Option<DepTerminal>>,
    /// True after the first TEARDOWN has been processed for this node
    /// (R2.6.4 / Lock 6.F). Subsequent TEARDOWN deliveries are idempotent
    /// — the auto-prepended COMPLETE only fires on the first one. Without
    /// this flag, a redundant TEARDOWN delivered via the cascade plus an
    /// explicit `core.teardown(node)` would re-emit `[COMPLETE, TEARDOWN]`
    /// to subscribers per delivery, which is incorrect.
    has_received_teardown: bool,
    /// Per R2.2.7 / R2.5.3 — resubscribable terminal lifecycle.
    /// When `true` AND `terminal == Some(...)`, a fresh subscribe call
    /// will reset the node: clear `terminal`, `has_fired_once`,
    /// `has_received_teardown`, all `dep_handles` to `NO_HANDLE`, all
    /// `dep_terminals` to `None`, and drain the pause lockset. The new
    /// subscriber then receives a vanilla `[START]` (or `[START, DATA(cache)]`
    /// for state nodes whose cache survived the reset) handshake.
    /// Default `false`.
    resubscribable: bool,
    /// Meta companion nodes attached to this node per R1.3.9.d. When this
    /// node tears down, its meta companions are torn down FIRST (before
    /// the main node's auto-COMPLETE + TEARDOWN wire emission), so
    /// observers see companions terminate before the parent. The ordering
    /// is load-bearing — meta nodes typically subscribe to parent state
    /// that becomes inconsistent during the parent's destruction phase.
    meta_companions: Vec<NodeId>,
}

/// All mutable Core state, behind one [`parking_lot::Mutex`].
///
/// v1 single-mutex; per-subgraph `ReentrantMutex` parallelism is a later
/// optimization (CLAUDE.md Rust invariant 3).
struct CoreState {
    next_node_id: u64,
    next_subscription_id: u64,
    next_lock_id: u64,
    nodes: HashMap<NodeId, NodeRecord>,
    /// Inverted adjacency: `parent → children`. Updated on registration.
    children: HashMap<NodeId, HashSet<NodeId>>,
    /// Nodes whose fn we owe a fire to — drained by [`Core::run_wave`].
    pending_fires: HashSet<NodeId>,
    /// Per-node outgoing message buffer; flushed at wave end. Insertion-
    /// ordered so flush order is deterministic — load-bearing for
    /// R1.3.9.d meta-TEARDOWN ordering: when a parent and its meta
    /// companion both have queued messages in the same wave, the meta
    /// (queued first via `teardown_inner`'s recursion order) flushes
    /// first.
    pending_notify: IndexMap<NodeId, Vec<Message>>,
    in_tick: bool,
    /// Core-global cap on per-node pause replay buffer length. `None` means
    /// unbounded. Per the user direction (Q1, 2026-05-05): start core-global;
    /// per-node override can be added later as a pure addition without API
    /// breakage. Default `None`.
    pause_buffer_cap: Option<usize>,
}

/// The handle-protocol Core dispatcher.
///
/// Holds an [`Arc`] to the [`BindingBoundary`] and all dispatch state. Cheap
/// to clone (the inner `Arc<Mutex<CoreState>>` is shared); pass `Core` by
/// value to threads.
#[derive(Clone)]
pub struct Core {
    state: Arc<Mutex<CoreState>>,
    binding: Arc<dyn BindingBoundary>,
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
            })),
            binding,
        }
    }

    /// Configure the Core-global cap on pause replay buffer length. When set,
    /// any per-node pause buffer that would exceed `cap` drops the oldest
    /// message(s) from the front; the dropped count is reported back via the
    /// resume callback (see [`ResumeReport`]). `None` (default) means
    /// unbounded; messages buffer indefinitely until the lockset clears.
    pub fn set_pause_buffer_cap(&self, cap: Option<usize>) {
        self.state.lock().pause_buffer_cap = cap;
    }

    /// Allocate a unique [`LockId`] for use with [`Self::pause`] /
    /// [`Self::resume`]. Convenience for callers that don't already have an
    /// id-allocation scheme; user-supplied ids work too.
    #[must_use]
    pub fn alloc_lock_id(&self) -> LockId {
        let mut s = self.state.lock();
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
        let mut s = self.state.lock();
        let id = s.alloc_node_id();
        let rec = NodeRecord {
            deps: Vec::new(),
            kind: NodeKind::State,
            fn_id: None,
            equals: EqualsMode::Identity,
            cache: initial,
            dep_handles: Vec::new(),
            has_fired_once: initial != NO_HANDLE,
            subscribers: HashMap::new(),
            tracked: HashSet::new(),
            dirty: false,
            involved_this_wave: false,
            pause_state: PauseState::Active,
            terminal: None,
            dep_terminals: Vec::new(),
            has_received_teardown: false,
            resubscribable: false,
            meta_companions: Vec::new(),
        };
        s.nodes.insert(id, rec);
        s.children.insert(id, HashSet::new());
        id
    }

    /// Register a derived (static) node. `fn_id` invokes via the binding;
    /// `equals` defaults to identity in callers.
    #[must_use]
    pub fn register_derived(&self, deps: &[NodeId], fn_id: FnId, equals: EqualsMode) -> NodeId {
        self.register_computed(deps, fn_id, equals, NodeKind::Derived)
    }

    /// Register a dynamic node — fn declares its actually-tracked dep indices
    /// per fire (canonical spec §2.8 / Lock 2.B).
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
        let mut s = self.state.lock();
        let id = s.alloc_node_id();
        // Static derived tracks all deps; dynamic starts empty and fills via fn return.
        let tracked: HashSet<usize> = match kind {
            NodeKind::Derived => (0..deps.len()).collect(),
            NodeKind::Dynamic | NodeKind::State => HashSet::new(),
        };
        let rec = NodeRecord {
            deps: deps.to_vec(),
            kind,
            fn_id: Some(fn_id),
            equals,
            cache: NO_HANDLE,
            dep_handles: vec![NO_HANDLE; deps.len()],
            has_fired_once: false,
            subscribers: HashMap::new(),
            tracked,
            dirty: false,
            involved_this_wave: false,
            pause_state: PauseState::Active,
            terminal: None,
            dep_terminals: vec![None; deps.len()],
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
    pub fn subscribe(&self, node_id: NodeId, sink: Sink) -> Subscription {
        let sub_id;
        {
            let mut s = self.state.lock();
            sub_id = s.alloc_sub_id();

            // Resubscribable reset: terminal + flagged → clear lifecycle
            // state so the incoming subscriber starts fresh. F3 audit guard:
            // a node that has received TEARDOWN (R2.6.4) is permanently
            // destroyed at this layer; resurrecting it via a late subscribe
            // is a category error. COMPLETE/ERROR is recoverable for
            // resubscribable nodes; TEARDOWN is not. The handshake will
            // still replay the terminal in the non-reset branch so the late
            // subscriber sees a clean `[START, ?DATA, COMPLETE|ERROR,
            // TEARDOWN]` stream.
            let needs_reset = {
                let rec = s.require_node(node_id);
                rec.resubscribable && rec.terminal.is_some() && !rec.has_received_teardown
            };
            if needs_reset {
                self.reset_for_fresh_lifecycle(&mut s, node_id);
            }

            // Build the START handshake (R1.2.3) — outside the subscribers map
            // because it goes only to the new sink, not the established set.
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
            let mut handshake: Vec<Message> = if cache == NO_HANDLE {
                vec![Message::Start]
            } else {
                vec![Message::Start, Message::Data(cache)]
            };
            // Replay the terminal so a late subscriber sees a clean
            // end-of-stream signal. (Resubscribable nodes that were eligible
            // for reset have `terminal == None` after `reset_for_fresh_lifecycle`
            // — no replay there.)
            if let Some(t) = terminal {
                handshake.push(match t {
                    DepTerminal::Complete => Message::Complete,
                    DepTerminal::Error(h) => Message::Error(h),
                });
            }
            // Replay TEARDOWN if the node has been torn down. Per F3, a
            // torn-down node cannot be reset even if resubscribable, so
            // late subscribers always see the full lifecycle replay
            // `[..., COMPLETE|ERROR, TEARDOWN]`.
            if torn_down {
                handshake.push(Message::Teardown);
            }
            // Lock briefly released for the sink call.
            drop(s);
            sink(&handshake);
            // Re-acquire to register the sink.
            let mut s = self.state.lock();
            s.require_node_mut(node_id).subscribers.insert(sub_id, sink);
            // Activate compute on first subscriber (push deps' cached handles down).
            if first_subscriber && kind != NodeKind::State {
                self.activate_derived(&mut s, node_id);
            }
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
        let mut s = self.state.lock();
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
    /// Released: terminal-slot retain (Error handle), all dep_terminals
    /// retains (Error handles in any per-dep terminal slots).
    /// Cleared: `terminal`, `has_fired_once`, `has_received_teardown`, all
    /// `dep_handles` to `NO_HANDLE`, all `dep_terminals` to `None`, the
    /// pause lockset (any held locks are released — replay buffer drops
    /// silently because there are no subscribers to flush to).
    fn reset_for_fresh_lifecycle(&self, s: &mut CoreState, node_id: NodeId) {
        // Collect handles to release (after dropping the borrow).
        let handles_to_release: Vec<HandleId> = {
            let rec = s.require_node(node_id);
            let mut hs = Vec::new();
            if let Some(DepTerminal::Error(h)) = rec.terminal {
                hs.push(h);
            }
            for slot in &rec.dep_terminals {
                if let Some(DepTerminal::Error(h)) = slot {
                    hs.push(*h);
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
            for slot in &mut rec.dep_handles {
                *slot = NO_HANDLE;
            }
            for slot in &mut rec.dep_terminals {
                *slot = None;
            }
            rec.pause_state = PauseState::Active;
            rec.involved_this_wave = false;
            rec.dirty = false;
            pulled
        };
        for h in handles_to_release {
            self.binding.release_handle(h);
        }
        for h in pause_buffer_payloads {
            self.binding.release_handle(h);
        }
    }

    fn activate_derived(&self, s: &mut CoreState, node_id: NodeId) {
        // Recursive activation: for each dep with no cache yet (compute, never fired),
        // give it an internal subscription so its activation runs.
        // Then push its cached handle into our dep_handles slot.
        //
        // This must run inside a wave so any first-fire emissions propagate
        // correctly; we set in_tick if not already and drain at the end.
        let already_in_tick = s.in_tick;
        if !already_in_tick {
            s.in_tick = true;
        }
        let dep_ids: Vec<NodeId> = s.require_node(node_id).deps.clone();
        for (i, dep_id) in dep_ids.iter().copied().enumerate() {
            let dep_kind;
            let dep_cache;
            let dep_has_fired;
            {
                let dep_rec = s.require_node(dep_id);
                dep_kind = dep_rec.kind;
                dep_cache = dep_rec.cache;
                dep_has_fired = dep_rec.has_fired_once;
            }
            if dep_kind != NodeKind::State && dep_cache == NO_HANDLE && !dep_has_fired {
                // Internal subscription marker — just recurse.
                self.activate_derived(s, dep_id);
            }
            // Re-read cache after possible activation.
            let dep_cache_after = s.require_node(dep_id).cache;
            if dep_cache_after != NO_HANDLE {
                self.deliver_data_to_consumer(s, node_id, i, dep_cache_after);
            }
        }
        if !already_in_tick {
            self.drain_and_flush(s);
            s.in_tick = false;
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
        let mut s = self.state.lock();
        {
            let rec = s.require_node(node_id);
            assert!(
                rec.kind == NodeKind::State,
                "emit() is for state nodes only; derived emits via fn"
            );
            if rec.terminal.is_some() {
                // Terminal — release the handle the caller just interned
                // (it would otherwise leak; cache slot ownership doesn't
                // transfer because we're not advancing cache).
                let _ = rec; // drop the immutable borrow
                self.binding.release_handle(new_handle);
                return;
            }
        }
        self.run_wave(&mut s, |this, s| {
            this.commit_emission(s, node_id, new_handle);
        });
    }

    /// Read a node's current cache. Returns [`NO_HANDLE`] if sentinel.
    #[must_use]
    pub fn cache_of(&self, node_id: NodeId) -> HandleId {
        self.state.lock().require_node(node_id).cache
    }

    /// Whether the node's fn has fired at least once (compute) OR it has had
    /// a non-sentinel value (state).
    #[must_use]
    pub fn has_fired_once(&self, node_id: NodeId) -> bool {
        self.state.lock().require_node(node_id).has_fired_once
    }

    // -------------------------------------------------------------------
    // Wave engine
    // -------------------------------------------------------------------

    fn run_wave<F>(&self, s: &mut CoreState, thunk: F)
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
        // Wave cleanup
        for rec in s.nodes.values_mut() {
            rec.dirty = false;
            rec.involved_this_wave = false;
        }
        s.in_tick = false;
    }

    fn drain_and_flush(&self, s: &mut CoreState) {
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

    /// Pick a node whose deps are all settled (no upstream pending fire).
    /// Topological-ish order ensures diamond resolution: D fires once,
    /// after both B and C settle.
    fn pick_next_fire(&self, s: &CoreState) -> Option<NodeId> {
        for &id in &s.pending_fires {
            let rec = s.require_node(id);
            let all_deps_settled = rec.deps.iter().all(|d| !s.pending_fires.contains(d));
            if all_deps_settled {
                return Some(id);
            }
        }
        // Cycle: pick any (may stabilize via guard, or assert).
        s.pending_fires.iter().copied().next()
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

    fn commit_emission(&self, s: &mut CoreState, node_id: NodeId, new_handle: HandleId) {
        assert!(
            new_handle != NO_HANDLE,
            "NO_HANDLE is not a valid DATA payload"
        );
        let (old_handle, equals_mode, was_dirty) = {
            let rec = s.require_node(node_id);
            (rec.cache, rec.equals, rec.dirty)
        };
        let is_data = !self.handles_equal(equals_mode, old_handle, new_handle);

        // R1.3.1.a: DIRTY precedes tier-3 in the same wave.
        if !was_dirty {
            s.require_node_mut(node_id).dirty = true;
            self.queue_notify(s, node_id, Message::Dirty);
        }

        if is_data {
            s.require_node_mut(node_id).cache = new_handle;
            // Refcount: release prior cache handle.
            if old_handle != NO_HANDLE {
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

    fn deliver_data_to_consumer(
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

    fn queue_notify(&self, s: &mut CoreState, node_id: NodeId, msg: Message) {
        // Pause routing decision (R1.3.7.b tier table, §10.2 buffering):
        //   Tier 3 (DATA / RESOLVED) and Tier 4 (INVALIDATE) buffer while
        //   paused; all other tiers (DIRTY tier 1, PAUSE/RESUME tier 2,
        //   COMPLETE/ERROR tier 5, TEARDOWN tier 6) bypass the buffer and
        //   flush immediately. START (tier 0) is per-subscription and never
        //   transits queue_notify.
        let buffered_tier = matches!(msg.tier(), 3 | 4);
        let cap = s.pause_buffer_cap;
        // Refcount discipline (paired with resume drain):
        //   1. Pre-buffer: retain the payload handle (if any) so the buffer's
        //      own ownership share keeps it alive across later cache
        //      replacements that would otherwise release_handle the
        //      previously-cached `H`.
        //   2. On cap overflow: release the dropped message's payload handle
        //      to balance step 1.
        //   3. On resume: release each replayed message's payload handle in
        //      `Core::resume` to balance step 1 (sinks observe the value but
        //      don't own a refcount share).
        // Note that `Subscribers.is_empty()` shortcut deliberately runs AFTER
        // the pause-state check would, but BEFORE the retain — if the node
        // has zero subscribers, we drop the message altogether and do not
        // retain (no buffer entry to keep alive).
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
        s.pending_notify.entry(node_id).or_default().push(msg);
    }

    fn flush_notifications(&self, s: &mut CoreState) {
        // Fire sinks while holding the lock. v1 limitation: sinks must not
        // call back into Core methods (would deadlock on the parking_lot
        // Mutex). Production hardening will move to ReentrantMutex with
        // internal cells, OR snapshot+drop+fire — see module doc.
        let pending = std::mem::take(&mut s.pending_notify);
        for (node_id, msgs) in pending {
            let Some(rec) = s.nodes.get(&node_id) else {
                continue;
            };
            for sink in rec.subscribers.values() {
                sink(&msgs);
            }
        }
    }
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
        self.emit_terminal(node_id, DepTerminal::Complete);
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
        self.emit_terminal(node_id, DepTerminal::Error(error_handle));
    }

    fn emit_terminal(&self, node_id: NodeId, terminal: DepTerminal) {
        let mut s = self.state.lock();
        assert!(s.nodes.contains_key(&node_id), "unknown node {node_id:?}");
        self.run_wave(&mut s, |this, s| {
            this.terminate_node(s, node_id, terminal);
        });
    }

    /// Set the node's terminal slot, queue the wire message, and cascade to
    /// children. Idempotent on already-terminal node (no-op).
    fn terminate_node(&self, s: &mut CoreState, node_id: NodeId, terminal: DepTerminal) {
        if s.require_node(node_id).terminal.is_some() {
            return; // Idempotent — already terminal.
        }
        // Take a refcount share for the terminal slot so the error handle
        // outlives the binding-side intern's transient share.
        if let DepTerminal::Error(h) = terminal {
            self.binding.retain_handle(h);
        }
        s.require_node_mut(node_id).terminal = Some(terminal);
        // Drain pending fires for this node — fn won't fire on a terminal node.
        s.pending_fires.remove(&node_id);
        // Queue the wire message (tier 5 — bypasses pause buffer).
        let msg = match terminal {
            DepTerminal::Complete => Message::Complete,
            DepTerminal::Error(h) => Message::Error(h),
        };
        self.queue_notify(s, node_id, msg);
        // Cascade to children.
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
            let Some(idx) = dep_idx else { continue };
            // Mark this child's per-dep terminal slot. Take a retain on the
            // error handle for the slot share.
            {
                let child = s.require_node_mut(child_id);
                if child.dep_terminals[idx].is_some() {
                    // Idempotent — child already saw this dep terminate.
                    continue;
                }
                child.dep_terminals[idx] = Some(terminal);
            }
            if let DepTerminal::Error(h) = terminal {
                self.binding.retain_handle(h);
            }
            // Auto-cascade gating: if all deps now terminal, propagate.
            let cascade = {
                let child = s.require_node(child_id);
                if child.terminal.is_some() {
                    None // Already terminated — no-op.
                } else if child.dep_terminals.iter().all(Option::is_some) {
                    Some(pick_cascade_terminal(&child.dep_terminals))
                } else {
                    None
                }
            };
            if let Some(t) = cascade {
                // Recurse with the chosen terminal. A retain happens inside
                // terminate_node for the child's own terminal slot.
                self.terminate_node(s, child_id, t);
            }
        }
    }
}

/// Lock 2.B cascade-terminal selection: ERROR dominates COMPLETE; first
/// ERROR seen wins. Caller has already verified all deps are terminal.
fn pick_cascade_terminal(dep_terminals: &[Option<DepTerminal>]) -> DepTerminal {
    for t in dep_terminals {
        if let Some(DepTerminal::Error(h)) = t {
            return DepTerminal::Error(*h);
        }
    }
    DepTerminal::Complete
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
        let mut s = self.state.lock();
        assert!(s.nodes.contains_key(&node_id), "unknown node {node_id:?}");
        self.run_wave(&mut s, |this, s| {
            this.teardown_inner(s, node_id);
        });
    }

    fn teardown_inner(&self, s: &mut CoreState, node_id: NodeId) {
        if s.require_node(node_id).has_received_teardown {
            return; // Idempotent (R2.6.4).
        }
        s.require_node_mut(node_id).has_received_teardown = true;
        // Meta TEARDOWN ordering (R1.3.9.d) — companions tear down BEFORE
        // the main node's auto-COMPLETE + TEARDOWN wire emission so
        // observers see companion lifecycle terminate first. Recursive:
        // a meta-of-meta tears down first, all the way down.
        let metas: Vec<NodeId> = s.require_node(node_id).meta_companions.clone();
        for meta in metas {
            self.teardown_inner(s, meta);
        }
        // Auto-prepend COMPLETE if not yet terminal. terminate_node handles
        // the auto-cascade to children's own terminal slots (Lock 2.B).
        let already_terminal = s.require_node(node_id).terminal.is_some();
        if !already_terminal {
            self.terminate_node(s, node_id, DepTerminal::Complete);
        }
        // Wire emission of the TEARDOWN itself (tier 6).
        self.queue_notify(s, node_id, Message::Teardown);
        // Cascade tear-down to regular children (the deps→consumers
        // direction). Idempotency on `has_received_teardown` keeps each
        // child visited once even when multi-parent diamonds re-enter via
        // a sibling path.
        let child_ids: Vec<NodeId> = s
            .children
            .get(&node_id)
            .map(|c| c.iter().copied().collect())
            .unwrap_or_default();
        for child_id in child_ids {
            self.teardown_inner(s, child_id);
        }
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
    /// # Panics
    ///
    /// Panics if either node id is unknown, or if `parent == companion`
    /// (a node cannot be its own meta companion — would loop on TEARDOWN).
    pub fn add_meta_companion(&self, parent: NodeId, companion: NodeId) {
        assert!(parent != companion, "node cannot be its own meta companion");
        let mut s = self.state.lock();
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
        let mut s = self.state.lock();
        // Verify the node exists (consistent panic across operations).
        assert!(s.nodes.contains_key(&node_id), "unknown node {node_id:?}");
        self.run_wave(&mut s, |this, s| {
            this.invalidate_inner(s, node_id);
        });
    }

    fn invalidate_inner(&self, s: &mut CoreState, node_id: NodeId) {
        // Never-populated / already-invalidated: no-op (R1.4 idempotency).
        let old_handle = s.require_node(node_id).cache;
        if old_handle == NO_HANDLE {
            return;
        }
        // Clear cache + release the handle's slot ownership.
        s.require_node_mut(node_id).cache = NO_HANDLE;
        self.binding.release_handle(old_handle);
        // Wire emission. Pause-aware via queue_notify.
        self.queue_notify(s, node_id, Message::Invalidate);
        // Cascade: for each child, clear the dep_handles slot referencing
        // this node and recurse. Collect ids first to avoid invalidating
        // the iterator while mutating children's dep_handles.
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
                // Reset the child's dep_handles entry — the handle stored
                // there was just released. Subsequent first-run-gate checks
                // see NO_HANDLE and re-close the gate.
                s.require_node_mut(child_id).dep_handles[idx] = NO_HANDLE;
                // Recursively invalidate. If the child's cache is already
                // NO_HANDLE (e.g., never populated, or already invalidated
                // earlier this wave), the recursive call is a no-op.
                self.invalidate_inner(s, child_id);
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
        let mut s = self.state.lock();
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
            let mut s = self.state.lock();
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
        let mut s = self.state.lock();
        // Validate node exists and is compute.
        let kind = s.nodes.get(&n).ok_or(SetDepsError::UnknownNode(n))?.kind;
        if kind == NodeKind::State {
            return Err(SetDepsError::NotComputeNode(n));
        }
        // Reject if `n` itself is terminal (Phase 13.8 Q1: terminal nodes
        // cannot be rewired; recovery is via resubscribable subscribe).
        if s.nodes[&n].terminal.is_some() {
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
        let current_deps: HashSet<NodeId> = s.nodes[&n].deps.iter().copied().collect();
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

        // Carry out the rewire atomically.
        // 1. Update the node's deps + dep_handles, clearing dirtyMask bits for removed.
        let new_deps_vec: Vec<NodeId> = new_deps.to_vec();
        // We need to preserve dep_handles AND dep_terminals for unchanged deps
        // and reset for added. Build new vecs aligned to new_deps_vec.
        //
        // Refcount discipline (F1 audit fix): each `Some(DepTerminal::Error(h))`
        // slot owns a refcount share retained at `terminate_node` time. When a
        // dep is REMOVED, its slot is dropped — the corresponding handle's
        // share must be released here, otherwise it leaks until Core drop.
        // Collect handles to release while we still hold the borrow; release
        // them after the lock is no longer needed for inspection.
        let (new_dep_handles, new_dep_terminals, removed_terminal_handles): (
            Vec<HandleId>,
            Vec<Option<DepTerminal>>,
            Vec<HandleId>,
        ) = {
            let rec = s.require_node(n);
            let old_handle_pairs: HashMap<NodeId, HandleId> = rec
                .deps
                .iter()
                .copied()
                .zip(rec.dep_handles.iter().copied())
                .collect();
            let old_terminal_pairs: HashMap<NodeId, Option<DepTerminal>> = rec
                .deps
                .iter()
                .copied()
                .zip(rec.dep_terminals.iter().copied())
                .collect();
            let handles = new_deps_vec
                .iter()
                .map(|d| old_handle_pairs.get(d).copied().unwrap_or(NO_HANDLE))
                .collect();
            let terms = new_deps_vec
                .iter()
                .map(|d| old_terminal_pairs.get(d).copied().unwrap_or(None))
                .collect();
            // Collect Error handles from REMOVED dep slots (slots whose
            // dep is in `removed` and the slot held an Error).
            let mut to_release: Vec<HandleId> = Vec::new();
            for d in &removed {
                if let Some(Some(DepTerminal::Error(h))) = old_terminal_pairs.get(d) {
                    to_release.push(*h);
                }
            }
            (handles, terms, to_release)
        };
        // Clear dirtyMask bit by re-emitting the wave-bookkeeping: we don't
        // currently model a per-dep dirtyMask explicitly (we use the boolean
        // `dirty` flag at node level). Removing a dep's entry from the implicit
        // mask is therefore implicit — by removing the dep, future emissions
        // from it can't re-arm the bit. The `involved_this_wave` flag stays
        // wave-scoped and gets cleared at wave end. The setDeps action itself
        // does NOT change the dirty boolean unless all deps are cleared; in
        // that case we settle.
        {
            let rec = s.require_node_mut(n);
            rec.deps.clone_from(&new_deps_vec);
            rec.dep_handles = new_dep_handles;
            rec.dep_terminals = new_dep_terminals;
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

        // 3. Push-on-subscribe for added deps with cached DATA. Wraps in a wave
        // so any downstream propagation runs cleanly.
        let mut needs_wave = false;
        let mut additions_with_cache: Vec<(NodeId, HandleId)> = Vec::new();
        for &added_dep in &added {
            let cache = s.require_node(added_dep).cache;
            if cache != NO_HANDLE {
                additions_with_cache.push((added_dep, cache));
                needs_wave = true;
            }
        }
        if needs_wave {
            self.run_wave(&mut s, |this, s| {
                for (added_dep, cache) in additions_with_cache {
                    let dep_idx = s.require_node(n).deps.iter().position(|&d| d == added_dep);
                    if let Some(idx) = dep_idx {
                        this.deliver_data_to_consumer(s, n, idx, cache);
                    }
                }
            });
        }
        // Drop the state lock before crossing the binding boundary for the
        // F1 refcount-fix releases. Keeps the lock-discipline split (binding
        // calls outside the state lock) consistent with the resume drain.
        drop(s);
        for h in removed_terminal_handles {
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

    fn require_node(&self, id: NodeId) -> &NodeRecord {
        self.nodes
            .get(&id)
            .unwrap_or_else(|| panic!("unknown node {id:?}"))
    }

    fn require_node_mut(&mut self, id: NodeId) -> &mut NodeRecord {
        self.nodes
            .get_mut(&id)
            .unwrap_or_else(|| panic!("unknown node {id:?}"))
    }
}
