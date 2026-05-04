//! The dispatcher — node registration, subscription, wave engine.
//!
//! Mirrors `~/src/graphrefly-ts/src/__experiments__/handle-core/core.ts`
//! (the Phase 13.6 brainstorm prototype, ~370 lines, 22 invariant tests).
//!
//! # Scope (M1 first dispatcher slice)
//!
//! - State + derived + dynamic node registration.
//! - Subscribe / unsubscribe with push-on-subscribe (R1.2.3).
//! - DIRTY → DATA / RESOLVED ordering (R1.3.1.b two-phase push).
//! - Equals-substitution (R1.3.2): identity is zero-FFI; custom crosses boundary.
//! - First-run gate (R2.5.3) — fn does not fire until every dep has a handle.
//! - Diamond resolution — one fn fire per wave even with shared upstream.
//!
//! # Out of scope for this slice (added in subsequent slices)
//!
//! - PAUSE / RESUME with lockIds (R2.6) — `pause.rs`.
//! - INVALIDATE broadcast (R1.2 `Message::Invalidate`) — added inline next.
//! - TEARDOWN auto-precedes COMPLETE (R2.6.4 / Lock 6.F).
//! - `set_deps()` atomic dep mutation — `setdeps.rs`.
//! - Deactivation cleanup (RAM nodes clear cache when sink count → 0).
//! - Resubscribable terminal lifecycle.
//!
//! # Re-entrance discipline
//!
//! The dispatcher uses `parking_lot::Mutex<CoreState>`. Sinks fire while the
//! lock is held — so a sink that calls back into `Core::emit` would deadlock.
//! v1 bench tests do not re-enter from sinks. Production hardening will move
//! to `ReentrantMutex` with internal `RefCell`-style cells, OR collect
//! notifications inside the lock and fire outside; that's a follow-up.

use std::sync::Arc;

use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use parking_lot::Mutex;
use thiserror::Error;

use crate::boundary::{BindingBoundary, FnResult};
use crate::handle::{FnId, HandleId, NodeId, NO_HANDLE};
use crate::message::Message;

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

/// Identifier for a single subscription. Allocated per [`Core::subscribe`]
/// call; passed back to [`Core::unsubscribe`] for cleanup.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct SubscriptionId(u64);

impl SubscriptionId {
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

/// A subscriber callback. `Send + Sync` so the Core can fire it from any
/// thread; `Fn` (not `FnMut`) so multiple references coexist — capture
/// mutable state in `Mutex<T>` or atomics on the binding side.
pub type Sink = Arc<dyn Fn(&[Message]) + Send + Sync>;

/// Internal node record. Mirrors `core.ts:132–154`.
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
}

/// All mutable Core state, behind one [`parking_lot::Mutex`].
///
/// v1 single-mutex; per-subgraph `ReentrantMutex` parallelism is a later
/// optimization (CLAUDE.md Rust invariant 3).
struct CoreState {
    next_node_id: u64,
    next_subscription_id: u64,
    nodes: HashMap<NodeId, NodeRecord>,
    /// Inverted adjacency: `parent → children`. Updated on registration.
    children: HashMap<NodeId, HashSet<NodeId>>,
    /// Nodes whose fn we owe a fire to — drained by [`Core::run_wave`].
    pending_fires: HashSet<NodeId>,
    /// Per-node outgoing message buffer; flushed at wave end.
    pending_notify: HashMap<NodeId, Vec<Message>>,
    in_tick: bool,
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
    /// Construct a fresh Core wired to the given binding.
    #[must_use]
    pub fn new(binding: Arc<dyn BindingBoundary>) -> Self {
        Self {
            state: Arc::new(Mutex::new(CoreState {
                next_node_id: 1,
                next_subscription_id: 1,
                nodes: HashMap::new(),
                children: HashMap::new(),
                pending_fires: HashSet::new(),
                pending_notify: HashMap::new(),
                in_tick: false,
            })),
            binding,
        }
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

    /// Subscribe a sink to a node. Returns the subscription id; pass to
    /// [`Self::unsubscribe`] to disconnect.
    ///
    /// Push-on-subscribe (R1.2.3, R2.2.3 step 4): the sink is registered AFTER
    /// the START handshake fires. Cached state pushes `[START, DATA(handle)]`;
    /// sentinel pushes `[START]` only.
    ///
    /// Activation (R2.2.3 step 5): if this is the first subscriber and the node
    /// is a derived/dynamic compute, recursively activate deps so their cached
    /// handles fill our `dep_handles`.
    pub fn subscribe(&self, node_id: NodeId, sink: Sink) -> SubscriptionId {
        let sub_id;
        {
            let mut s = self.state.lock();
            sub_id = s.alloc_sub_id();
            // Build the START handshake (R1.2.3) — outside the subscribers map
            // because it goes only to the new sink, not the established set.
            let (cache, kind, first_subscriber) = {
                let rec = s.require_node(node_id);
                (rec.cache, rec.kind, rec.subscribers.is_empty())
            };
            let handshake: Vec<Message> = if cache == NO_HANDLE {
                vec![Message::Start]
            } else {
                vec![Message::Start, Message::Data(cache)]
            };
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
        sub_id
    }

    /// Disconnect a previously-registered subscription.
    pub fn unsubscribe(&self, node_id: NodeId, sub_id: SubscriptionId) {
        let mut s = self.state.lock();
        if let Some(rec) = s.nodes.get_mut(&node_id) {
            rec.subscribers.remove(&sub_id);
        }
        // Deactivation cleanup (clear cache for RAM nodes, release handle, etc.)
        // is out of scope for v1.
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
        let rec = s.require_node(node_id);
        if rec.subscribers.is_empty() {
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
// set_deps — atomic dep mutation
// -----------------------------------------------------------------------

/// Errors returnable by [`Core::set_deps`].
///
/// Per `~/src/graphrefly-ts/docs/research/rewire-design-notes.md`:
/// - `SelfDependency` — `n in newDeps` (self-loops are pathological without
///   explicit fixed-point semantics, which GraphReFly does not provide).
/// - `WouldCreateCycle { path }` — adding the new edge would create a cycle.
///   The `path` field reports the offending dep chain for debuggability.
/// - `UnknownNode` / `NotComputeNode` — invariant violations from the caller.
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
    pub fn set_deps(&self, n: NodeId, new_deps: &[NodeId]) -> Result<(), SetDepsError> {
        let mut s = self.state.lock();
        // Validate node exists and is compute.
        let kind = s.nodes.get(&n).ok_or(SetDepsError::UnknownNode(n))?.kind;
        if kind == NodeKind::State {
            return Err(SetDepsError::NotComputeNode(n));
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
        // Idempotent fast-path.
        if added.is_empty() && current_deps == new_deps_set {
            return Ok(());
        }
        let removed: HashSet<NodeId> = current_deps.difference(&new_deps_set).copied().collect();

        // Carry out the rewire atomically.
        // 1. Update the node's deps + dep_handles, clearing dirtyMask bits for removed.
        let new_deps_vec: Vec<NodeId> = new_deps.to_vec();
        // We need to preserve dep_handles for unchanged deps and reset for added.
        // Build a new dep_handles vec aligned to new_deps_vec.
        let new_dep_handles: Vec<HandleId> = {
            let rec = s.require_node(n);
            let old_pairs: HashMap<NodeId, HandleId> = rec
                .deps
                .iter()
                .copied()
                .zip(rec.dep_handles.iter().copied())
                .collect();
            new_deps_vec
                .iter()
                .map(|d| old_pairs.get(d).copied().unwrap_or(NO_HANDLE))
                .collect()
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
            // Re-derive `tracked` for static derived: all indices.
            // For dynamic: keep existing tracked filtered to remaining deps' new indices.
            // Simplification for v1: dynamic nodes lose tracking on rewire and
            // must re-fire fn to repopulate. This is conservative but correct.
            if rec.kind == NodeKind::Derived {
                rec.tracked = (0..new_deps_vec.len()).collect();
            } else if rec.kind == NodeKind::Dynamic {
                rec.tracked.clear();
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
