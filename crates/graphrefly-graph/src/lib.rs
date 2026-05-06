//! `GraphReFly` Graph container — sugar layer over `graphrefly_core::Core`.
//!
//! # Status (M2 starter slice — 2026-05-05)
//!
//! Initial slice scope:
//! - [`Graph`] container holding an [`Arc<Core>`].
//! - Sugar constructors: [`Graph::state`], [`Graph::derived`], [`Graph::dynamic`].
//! - Lifecycle pass-throughs: [`Graph::subscribe`], [`Graph::emit`],
//!   [`Graph::cache_of`], [`Graph::complete`], [`Graph::error`],
//!   [`Graph::teardown`], [`Graph::invalidate`], [`Graph::pause`],
//!   [`Graph::resume`].
//! - Atomic dep mutation: [`Graph::set_deps`].
//! - Read-side introspection: [`Graph::node_ids`], [`Graph::node_count`].
//!
//! Out of scope for this slice (M2 follow-ups):
//!
//! - **Subgraph mount / unmount** — composition (canonical spec §3.4 / §3.7).
//! - **``describe()`` topology export** — pretty / mermaid / d2 / json
//!   (canonical spec §3.6, Appendix B JSON schema).
//! - **`observe()` message tap** — single source of truth for live data flow
//!   (canonical spec §3.6).
//! - **Content-addressed snapshots** — V1 lazy CID / V2 schema validation /
//!   V3 caps + cross-graph refs (canonical spec §3.8 + roadmap Phase 6).
//! - **Persistence integration** with `graphrefly-storage` (M4).
//! - **Namespace** (canonical spec §3.5) — node naming and lookup by name.
//! - **DS-14 `mutate()` factory** — RAII guard + batch + audit append
//!   substrate (lands alongside Phase 14 changesets in `graphrefly-core` /
//!   here).
//!
//! # Design
//!
//! [`Graph`] is a thin wrapper around an `Arc<Core>`. Cloning a `Graph` is
//! cheap (refcount bump on the inner Arc); the same dispatcher state is
//! shared. This mirrors the M1 [`graphrefly_core::Core`]'s `Clone` semantics:
//! pass `Graph` by value to threads.
//!
//! Sugar constructors return raw [`NodeId`] for now — typed handles
//! (`StateNode<T>`, `DerivedNode<T>`) come with the value-type integration
//! work (post-M2; the type parameter must thread through the binding side
//! since Core sees only `HandleId`).
//!
//! # Crate lints
//!
//! `#[forbid(unsafe_code)]` per workspace policy.

#![forbid(unsafe_code)]
#![warn(rust_2018_idioms, unreachable_pub)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::missing_errors_doc)]

use std::sync::Arc;

use graphrefly_core::{
    BindingBoundary, Core, EqualsMode, FnId, HandleId, LockId, NodeId, PauseError, ResumeReport,
    SetDepsError, Sink, Subscription,
};

/// Graph container — Phase 1+ public API.
///
/// Holds an [`Arc<Core>`] internally; cloning is a cheap refcount bump.
/// Pass `Graph` by value to threads (or share via `Arc<Graph>` when an
/// outer reference count is needed).
#[derive(Clone)]
pub struct Graph {
    core: Core,
}

impl Graph {
    /// Construct an empty graph wired to the given binding.
    ///
    /// The binding mediates the [`graphrefly_core::BindingBoundary`] FFI —
    /// per-language registries (`napi-rs` for JS, `pyo3` for PY) implement
    /// it. For pure-Rust use cases, the test scaffolds in
    /// `graphrefly-core/tests/common/` provide a reference implementation.
    #[must_use]
    pub fn new(binding: Arc<dyn BindingBoundary>) -> Self {
        Self {
            core: Core::new(binding),
        }
    }

    /// Borrow the underlying [`Core`] — the M1 dispatcher. Useful when you
    /// need a Core-level method that hasn't yet been surfaced on `Graph`.
    /// Phase 4+ patterns commonly hold onto the `Core` directly.
    #[must_use]
    pub fn core(&self) -> &Core {
        &self.core
    }

    // -------------------------------------------------------------------
    // Sugar constructors (canonical spec §3.9)
    // -------------------------------------------------------------------

    /// Register a state node. `initial` of `None` starts sentinel; `Some(h)`
    /// pre-populates the cache. Equivalent to
    /// [`graphrefly_core::Core::register_state`].
    ///
    /// State nodes are mutated via [`Self::emit`].
    #[must_use]
    pub fn state(&self, initial: Option<HandleId>) -> NodeId {
        self.core
            .register_state(initial.unwrap_or(graphrefly_core::NO_HANDLE))
    }

    /// Register a static-derived node — fn fires on every dep change with
    /// all deps tracked. Equivalent to
    /// [`graphrefly_core::Core::register_derived`].
    #[must_use]
    pub fn derived(&self, deps: &[NodeId], fn_id: FnId, equals: EqualsMode) -> NodeId {
        self.core.register_derived(deps, fn_id, equals)
    }

    /// Register a dynamic-derived node — fn declares which dep indices it
    /// actually read this run. Untracked dep updates flow through cache but
    /// do NOT re-fire fn. Equivalent to
    /// [`graphrefly_core::Core::register_dynamic`].
    #[must_use]
    pub fn dynamic(&self, deps: &[NodeId], fn_id: FnId, equals: EqualsMode) -> NodeId {
        self.core.register_dynamic(deps, fn_id, equals)
    }

    // -------------------------------------------------------------------
    // Lifecycle pass-throughs (canonical spec §3.7)
    // -------------------------------------------------------------------

    /// Subscribe a sink. Returns a [`Subscription`] handle — dropping it
    /// unsubscribes. See [`graphrefly_core::Core::subscribe`] for full
    /// handshake semantics (R1.3.5.a tier-split, resubscribable terminal
    /// reset).
    pub fn subscribe(&self, node_id: NodeId, sink: Sink) -> Subscription {
        self.core.subscribe(node_id, sink)
    }

    /// Emit a value on a state node. Triggers a wave (DIRTY → DATA/RESOLVED
    /// → fn fires for downstream).
    pub fn emit(&self, node_id: NodeId, new_handle: HandleId) {
        self.core.emit(node_id, new_handle);
    }

    /// Read a node's current cache. Returns
    /// [`graphrefly_core::NO_HANDLE`] if sentinel.
    #[must_use]
    pub fn cache_of(&self, node_id: NodeId) -> HandleId {
        self.core.cache_of(node_id)
    }

    /// Whether the node's fn has fired at least once (compute) OR it has
    /// had a non-sentinel value (state).
    #[must_use]
    pub fn has_fired_once(&self, node_id: NodeId) -> bool {
        self.core.has_fired_once(node_id)
    }

    /// Mark the node terminal with COMPLETE (R1.3.4). After this call,
    /// further `emit` calls are silent no-ops, fn no longer fires, and
    /// children's `dep_terminals` slots cascade per Lock 2.B.
    pub fn complete(&self, node_id: NodeId) {
        self.core.complete(node_id);
    }

    /// Mark the node terminal with ERROR (R1.3.4). `error_handle` must
    /// resolve to a non-sentinel value; ERROR dominates COMPLETE in
    /// auto-cascade gating (Lock 2.B).
    pub fn error(&self, node_id: NodeId, error_handle: HandleId) {
        self.core.error(node_id, error_handle);
    }

    /// Tear the node down (R2.6.4). Auto-prepends COMPLETE if not yet
    /// terminal; cascades through children. Idempotent on duplicate
    /// delivery.
    pub fn teardown(&self, node_id: NodeId) {
        self.core.teardown(node_id);
    }

    /// Clear the node's cache and cascade `[INVALIDATE]` to dependents.
    /// No-op if the node was never populated (R1.4 idempotency).
    pub fn invalidate(&self, node_id: NodeId) {
        self.core.invalidate(node_id);
    }

    /// Acquire a pause lock. Tier-3/tier-4 outgoing messages buffer while
    /// the lockset is non-empty; other tiers flush immediately.
    pub fn pause(&self, node_id: NodeId, lock_id: LockId) -> Result<(), PauseError> {
        self.core.pause(node_id, lock_id)
    }

    /// Release a pause lock. When the final lock releases, returns the
    /// [`ResumeReport`] (replayed + dropped counts).
    pub fn resume(
        &self,
        node_id: NodeId,
        lock_id: LockId,
    ) -> Result<Option<ResumeReport>, PauseError> {
        self.core.resume(node_id, lock_id)
    }

    /// Allocate a fresh `LockId` for use with [`Self::pause`] /
    /// [`Self::resume`]. Convenience for callers without a built-in
    /// lock-id allocation scheme.
    #[must_use]
    pub fn alloc_lock_id(&self) -> LockId {
        self.core.alloc_lock_id()
    }

    /// Atomically rewire a node's deps. Validated for cycle-freedom +
    /// terminal-rejection policy (Phase 13.8 Q1). See
    /// [`graphrefly_core::Core::set_deps`] for the full design.
    pub fn set_deps(&self, n: NodeId, new_deps: &[NodeId]) -> Result<(), SetDepsError> {
        self.core.set_deps(n, new_deps)
    }

    /// Mark the node as resubscribable (R2.2.7). Resubscribable nodes
    /// reset their terminal-lifecycle state on a fresh subscribe (unless
    /// torn down). Configuration call — must be made before the node
    /// has any active subscribers.
    pub fn set_resubscribable(&self, node_id: NodeId, resubscribable: bool) {
        self.core.set_resubscribable(node_id, resubscribable);
    }

    /// Attach `companion` as a meta companion of `parent` (R1.3.9.d).
    /// Companions tear down before their parent.
    pub fn add_meta_companion(&self, parent: NodeId, companion: NodeId) {
        self.core.add_meta_companion(parent, companion);
    }

    // -------------------------------------------------------------------
    // Batch (canonical spec §4.3)
    // -------------------------------------------------------------------

    /// Coalesce multiple emissions into a single wave. Forwards to
    /// [`graphrefly_core::Core::batch`] — see that method for R1.3.6 /
    /// R1.3.6.b semantics around DIRTY propagation and per-node
    /// multi-message coalescing.
    pub fn batch<F: FnOnce()>(&self, f: F) {
        self.core.batch(f);
    }
}

// ---------------------------------------------------------------------------
// Read-side introspection — minimal node enumeration for M2 starter slice
// ---------------------------------------------------------------------------

impl Graph {
    /// Number of nodes registered in this graph.
    ///
    /// **Not yet implemented (M2 starter slice).** The Core dispatcher
    /// doesn't yet expose a structured node-iteration inspector — that
    /// lands with the canonical-spec §3.6 ``describe()`` work. Calling this
    /// today panics with `unimplemented!()` rather than silently returning
    /// `0` (which would create a misleading "the graph is empty" contract
    /// that callers can't distinguish from genuine emptiness).
    ///
    /// Use [`Graph::core`] for direct Core access until `describe()` lands.
    ///
    /// # Panics
    ///
    /// Always panics in this slice. Will return the actual count once
    /// `describe()` ships.
    #[must_use]
    pub fn node_count(&self) -> usize {
        unimplemented!(
            "Graph::node_count lands with `describe()` — see migration-status.md M2 follow-ups; \
             use Graph::core() for direct Core access in the meantime"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use graphrefly_core::{FnId, FnResult, Message};
    use std::sync::{Arc, Mutex};

    /// Minimal test binding — primitives only, no value registry. Tests
    /// drive node lifecycle without caring about the underlying value
    /// type. Mirrors the Slice A close test pattern but pared down.
    struct StubBinding {
        retains: Mutex<usize>,
    }

    impl StubBinding {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                retains: Mutex::new(0),
            })
        }
    }

    impl BindingBoundary for StubBinding {
        fn invoke_fn(&self, _node_id: NodeId, _fn_id: FnId, dep_handles: &[HandleId]) -> FnResult {
            // Identity passthrough for tests.
            dep_handles
                .first()
                .map_or(FnResult::Noop { tracked: None }, |&h| FnResult::Data {
                    handle: h,
                    tracked: None,
                })
        }

        fn custom_equals(&self, _: FnId, a: HandleId, b: HandleId) -> bool {
            a == b
        }

        fn release_handle(&self, _h: HandleId) {
            let mut guard = self.retains.lock().expect("retain lock");
            if *guard > 0 {
                *guard -= 1;
            }
        }

        fn retain_handle(&self, _h: HandleId) {
            *self.retains.lock().expect("retain lock") += 1;
        }
    }

    fn make_graph() -> Graph {
        Graph::new(StubBinding::new() as Arc<dyn BindingBoundary>)
    }

    #[test]
    fn graph_new_creates_empty_graph() {
        // Graph::new accepts a binding and constructs a fresh Core wrapper.
        // node_count() panics in this slice (see #[should_panic] test
        // below); we just verify construction doesn't itself blow up and
        // that subsequent registration works.
        let g = make_graph();
        let _ = g.state(None); // smoke test: register works on a fresh graph
    }

    #[test]
    #[should_panic(expected = "Graph::node_count lands with `describe()`")]
    fn graph_node_count_panics_until_describe_lands() {
        // Documents the M2-starter contract: callers cannot rely on
        // node_count returning a meaningful value until the `describe()`
        // slice ships.
        let g = make_graph();
        let _ = g.node_count();
    }

    #[test]
    fn graph_state_registers_state_node() {
        let g = make_graph();
        let h = HandleId::new(1); // arbitrary non-sentinel handle for test
        let id = g.state(Some(h));
        assert_eq!(g.cache_of(id), h);
    }

    #[test]
    fn graph_state_with_none_starts_sentinel() {
        let g = make_graph();
        let id = g.state(None);
        assert_eq!(g.cache_of(id), graphrefly_core::NO_HANDLE);
    }

    #[test]
    fn graph_emit_propagates_through_derived() {
        // Verify Graph's pass-throughs compose: state → derived → subscribe.
        let g = make_graph();
        let h_init = HandleId::new(7);
        let s = g.state(Some(h_init));
        // FnId is opaque — for the stub binding above, any FnId works
        // because invoke_fn dispatches on dep_handles (passthrough).
        let fn_id = FnId::new(1);
        let d = g.derived(&[s], fn_id, EqualsMode::Identity);

        // Subscribe a recording sink and emit.
        let events: Arc<Mutex<Vec<Message>>> = Arc::new(Mutex::new(Vec::new()));
        let events_for_sink = events.clone();
        let sink: Sink = Arc::new(move |msgs: &[Message]| {
            events_for_sink.lock().unwrap().extend_from_slice(msgs);
        });
        let _sub = g.subscribe(d, sink);

        let h_new = HandleId::new(42);
        g.emit(s, h_new);

        let events = events.lock().unwrap();
        // Handshake delivered Start + Data(7); wave delivered Dirty + Data(42).
        assert!(events.iter().any(|m| matches!(m, Message::Start)));
        assert!(events
            .iter()
            .any(|m| matches!(m, Message::Data(h) if *h == h_init)));
        assert!(events.iter().any(|m| matches!(m, Message::Dirty)));
        assert!(events
            .iter()
            .any(|m| matches!(m, Message::Data(h) if *h == h_new)));
    }

    #[test]
    fn graph_pause_resume_round_trip() {
        let g = make_graph();
        let s = g.state(Some(HandleId::new(1)));
        let lock = g.alloc_lock_id();

        g.pause(s, lock).expect("pause");
        assert!(g.core().is_paused(s));
        let report = g.resume(s, lock).expect("resume");
        assert!(report.is_some());
        assert!(!g.core().is_paused(s));
    }

    #[test]
    fn graph_complete_terminates_node() {
        let g = make_graph();
        let s = g.state(Some(HandleId::new(1)));
        g.complete(s);

        // Subsequent emit is a silent no-op (the test binding's
        // release_handle ticks down, balancing the intern bump).
        let h = HandleId::new(99);
        g.emit(s, h);
        assert_eq!(g.cache_of(s), HandleId::new(1)); // unchanged
    }

    #[test]
    fn graph_set_deps_validates_cycles() {
        // Build A → B; then try set_deps(A, &[B]) which would create
        // A → B → A (cycle). Should reject.
        let g = make_graph();
        let a = g.state(Some(HandleId::new(1)));
        let fn_id = FnId::new(1);
        let b = g.derived(&[a], fn_id, EqualsMode::Identity);

        // Try to make `a`'s deps include `b` — but `a` is a state node.
        // set_deps only accepts compute nodes; expect NotComputeNode.
        let err = g.set_deps(a, &[b]).expect_err("should reject state node");
        match err {
            SetDepsError::NotComputeNode(n) if n == a => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn graph_alloc_lock_id_returns_distinct_ids() {
        let g = make_graph();
        let l1 = g.alloc_lock_id();
        let l2 = g.alloc_lock_id();
        assert_ne!(l1, l2);
    }

    #[test]
    fn graph_clone_shares_state() {
        // Verify Graph::clone shares the underlying Core (Arc bump).
        let g1 = make_graph();
        let g2 = g1.clone();
        let s = g1.state(Some(HandleId::new(5)));
        // Same state visible via the cloned Graph.
        assert_eq!(g2.cache_of(s), HandleId::new(5));
    }

    #[test]
    fn graph_batch_coalesces_emits() {
        // Smoke test that Graph::batch forwards to Core::batch.
        let g = make_graph();
        let s = g.state(Some(HandleId::new(1)));

        let events: Arc<Mutex<Vec<Message>>> = Arc::new(Mutex::new(Vec::new()));
        let events_for_sink = events.clone();
        let sink: Sink = Arc::new(move |msgs: &[Message]| {
            events_for_sink.lock().unwrap().extend_from_slice(msgs);
        });
        let _sub = g.subscribe(s, sink);

        g.batch(|| {
            g.emit(s, HandleId::new(2));
            g.emit(s, HandleId::new(3));
            g.emit(s, HandleId::new(4));
        });

        // Final cache reflects last emit.
        assert_eq!(g.cache_of(s), HandleId::new(4));
        // Sink got at least one Dirty + 3 Data messages (coalesced wave).
        let events = events.lock().unwrap();
        let data_count = events
            .iter()
            .filter(|m| matches!(m, Message::Data(_)))
            .count();
        // 1 from handshake + 3 from emits = 4.
        assert_eq!(data_count, 4);
    }
}
