//! Producer substrate tests (Slice D, Commit 1).
//!
//! Exercises the unified `Core::register` shape for producer-kind nodes
//! (D030) and the lifecycle hooks `BindingBoundary::producer_deactivate`
//! (D031, D035). The full `ProducerCtx` + zip/concat/race/takeUntil
//! machinery lands in Commit 2 (`graphrefly-operators::producer`); this
//! file verifies the core substrate without that layer.

mod common;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use ahash::AHashMap as HashMap;

use graphrefly_core::{
    BindingBoundary, Core, DepBatch, EqualsMode, FnId, FnResult, HandleId, NodeFnOrOp, NodeId,
    NodeKind, NodeOpts, NodeRegistration, Sink, NO_HANDLE,
};

/// Test binding with producer activation/deactivation tracking.
///
/// Tracks per-node:
/// - `fn_invocations[node_id]` — how many times `invoke_fn` was called
///   for that node (used to verify "fires once on activation").
/// - `deactivate_calls[node_id]` — how many times `producer_deactivate`
///   was called (verifies last-unsub triggers cleanup).
/// - `fn_results[fn_id]` — what each FnId returns from `invoke_fn`. Tests
///   register a producer with a specific result via
///   [`ProducerBinding::register_producer_fn`].
struct ProducerBinding {
    inner: Mutex<ProducerInner>,
}

struct ProducerInner {
    next_fn_id: u64,
    next_handle: u64,
    refcounts: HashMap<HandleId, u64>,
    fn_invocations: HashMap<NodeId, u64>,
    deactivate_calls: HashMap<NodeId, u64>,
    fn_results: HashMap<FnId, FnResult>,
}

impl ProducerBinding {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(ProducerInner {
                next_fn_id: 1,
                next_handle: 1,
                refcounts: HashMap::new(),
                fn_invocations: HashMap::new(),
                deactivate_calls: HashMap::new(),
                fn_results: HashMap::new(),
            }),
        })
    }

    /// Register a producer fn that returns a fixed `FnResult` on every
    /// invocation. Returns the FnId for use with `Core::register_producer`.
    fn register_producer_fn(&self, result: FnResult) -> FnId {
        let mut inner = self.inner.lock().unwrap();
        let id = FnId::new(inner.next_fn_id);
        inner.next_fn_id += 1;
        inner.fn_results.insert(id, result);
        id
    }

    /// Allocate a fresh handle (tests use this to mint emission payloads).
    fn alloc_handle(&self) -> HandleId {
        let mut inner = self.inner.lock().unwrap();
        let h = HandleId::new(inner.next_handle);
        inner.next_handle += 1;
        inner.refcounts.insert(h, 1);
        h
    }

    fn fn_invocation_count(&self, node_id: NodeId) -> u64 {
        self.inner
            .lock()
            .unwrap()
            .fn_invocations
            .get(&node_id)
            .copied()
            .unwrap_or(0)
    }

    fn deactivate_count(&self, node_id: NodeId) -> u64 {
        self.inner
            .lock()
            .unwrap()
            .deactivate_calls
            .get(&node_id)
            .copied()
            .unwrap_or(0)
    }
}

impl BindingBoundary for ProducerBinding {
    fn invoke_fn(&self, node_id: NodeId, fn_id: FnId, _dep_data: &[DepBatch]) -> FnResult {
        let mut inner = self.inner.lock().unwrap();
        *inner.fn_invocations.entry(node_id).or_insert(0) += 1;
        inner
            .fn_results
            .get(&fn_id)
            .cloned()
            .unwrap_or(FnResult::Noop { tracked: None })
    }

    fn custom_equals(&self, _f: FnId, a: HandleId, b: HandleId) -> bool {
        a == b
    }

    fn release_handle(&self, handle: HandleId) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(c) = inner.refcounts.get_mut(&handle) {
            if *c > 0 {
                *c -= 1;
            }
        }
    }

    fn retain_handle(&self, handle: HandleId) {
        let mut inner = self.inner.lock().unwrap();
        *inner.refcounts.entry(handle).or_insert(0) += 1;
    }

    fn producer_deactivate(
        &self,
        node_id: NodeId,
        _unsub: &dyn Fn(NodeId, graphrefly_core::SubscriptionId),
    ) {
        // S2b/D229: this test binding only counts deactivations; it
        // records no upstream subs, so the owner-supplied `unsub` is
        // unused here.
        let mut inner = self.inner.lock().unwrap();
        *inner.deactivate_calls.entry(node_id).or_insert(0) += 1;
    }
}

fn make_runtime() -> (Core, Arc<ProducerBinding>) {
    let binding = ProducerBinding::new();
    let core = Core::new(binding.clone() as Arc<dyn BindingBoundary>);
    (core, binding)
}

fn noop_sink() -> Sink {
    Arc::new(|_| {})
}

// =====================================================================
// 1. Shape: kind derives correctly for producer registrations
// =====================================================================

#[test]
fn r_d030_producer_kind_derives_from_field_shape() {
    // D030 — kind is derived, not stored. A node with empty deps + a
    // fn_id but no op should report Producer.
    let (core, binding) = make_runtime();
    let fn_id = binding.register_producer_fn(FnResult::Noop { tracked: None });
    let producer = core.register_producer(fn_id).unwrap();
    assert_eq!(core.kind_of(producer), Some(NodeKind::Producer));
}

#[test]
fn r_d030_state_kind_unchanged_after_unification() {
    // State node: empty deps + no fn + no op → State.
    let (core, _binding) = make_runtime();
    let s = core.register_state(NO_HANDLE, false).unwrap();
    assert_eq!(core.kind_of(s), Some(NodeKind::State));
}

#[test]
fn r_d030_unified_register_dispatches_correctly() {
    // Drive the unified `Core::register` directly — exercise all four
    // shape combinations to confirm `kind()` returns the right variant.
    let (core, binding) = make_runtime();
    let fn_id = binding.register_producer_fn(FnResult::Noop { tracked: None });

    let state = core
        .register(NodeRegistration {
            deps: Vec::new(),
            fn_or_op: None,
            opts: NodeOpts::default(),
        })
        .unwrap();
    assert_eq!(core.kind_of(state), Some(NodeKind::State));

    let producer = core
        .register(NodeRegistration {
            deps: Vec::new(),
            fn_or_op: Some(NodeFnOrOp::Fn(fn_id)),
            opts: NodeOpts {
                partial: true,
                ..Default::default()
            },
        })
        .unwrap();
    assert_eq!(core.kind_of(producer), Some(NodeKind::Producer));

    let derived = core
        .register(NodeRegistration {
            deps: vec![state],
            fn_or_op: Some(NodeFnOrOp::Fn(fn_id)),
            opts: NodeOpts {
                equals: EqualsMode::Identity,
                ..Default::default()
            },
        })
        .unwrap();
    assert_eq!(core.kind_of(derived), Some(NodeKind::Derived));

    let dynamic = core
        .register(NodeRegistration {
            deps: vec![state],
            fn_or_op: Some(NodeFnOrOp::Fn(fn_id)),
            opts: NodeOpts {
                is_dynamic: true,
                partial: true,
                ..Default::default()
            },
        })
        .unwrap();
    assert_eq!(core.kind_of(dynamic), Some(NodeKind::Dynamic));
}

// =====================================================================
// 2. Activation: fn fires ONCE on first subscribe
// =====================================================================

#[test]
fn r_d031_producer_fn_fires_once_on_first_subscribe() {
    let (core, binding) = make_runtime();
    let fn_id = binding.register_producer_fn(FnResult::Noop { tracked: None });
    let producer = core.register_producer(fn_id).unwrap();

    assert_eq!(
        binding.fn_invocation_count(producer),
        0,
        "fn should not fire before first subscribe"
    );

    let _sub = core.subscribe(producer, noop_sink());

    assert_eq!(
        binding.fn_invocation_count(producer),
        1,
        "fn should fire exactly once on first subscribe"
    );
}

#[test]
fn r_d031_producer_fn_does_not_re_fire_on_additional_subscribers() {
    // R2.2.3 — activation is per-node, not per-subscriber. The producer
    // fn fires once when the first subscriber attaches; subsequent
    // subscribers attach without re-firing.
    let (core, binding) = make_runtime();
    let fn_id = binding.register_producer_fn(FnResult::Noop { tracked: None });
    let producer = core.register_producer(fn_id).unwrap();

    let _sub1 = core.subscribe(producer, noop_sink());
    let _sub2 = core.subscribe(producer, noop_sink());
    let _sub3 = core.subscribe(producer, noop_sink());

    assert_eq!(
        binding.fn_invocation_count(producer),
        1,
        "fn should fire only once across multiple subscribers"
    );
}

// =====================================================================
// 3. Deactivation: producer_deactivate fires on last unsubscribe
// =====================================================================

#[test]
fn r_d031_producer_deactivate_fires_on_last_unsub() {
    let (core, binding) = make_runtime();
    let fn_id = binding.register_producer_fn(FnResult::Noop { tracked: None });
    let producer = core.register_producer(fn_id).unwrap();

    let sub = core.subscribe(producer, noop_sink());
    assert_eq!(
        binding.deactivate_count(producer),
        0,
        "deactivate should not fire while a subscriber is alive"
    );
    drop(sub);
    assert_eq!(
        binding.deactivate_count(producer),
        1,
        "deactivate should fire exactly once when the last subscriber drops"
    );
}

#[test]
fn r_d031_producer_deactivate_does_not_fire_when_one_of_many_unsubs() {
    let (core, binding) = make_runtime();
    let fn_id = binding.register_producer_fn(FnResult::Noop { tracked: None });
    let producer = core.register_producer(fn_id).unwrap();

    let sub1 = core.subscribe(producer, noop_sink());
    let _sub2 = core.subscribe(producer, noop_sink());

    drop(sub1);
    assert_eq!(
        binding.deactivate_count(producer),
        0,
        "deactivate should NOT fire while at least one subscriber remains"
    );
}

// =====================================================================
// 4. Re-subscribe after deactivation: fn re-fires on next subscribe
// =====================================================================

#[test]
fn r_d031_producer_re_fires_fn_after_deactivate_then_resubscribe() {
    let (core, binding) = make_runtime();
    let fn_id = binding.register_producer_fn(FnResult::Noop { tracked: None });
    let producer = core.register_producer(fn_id).unwrap();

    let sub1 = core.subscribe(producer, noop_sink());
    assert_eq!(binding.fn_invocation_count(producer), 1);
    drop(sub1);
    assert_eq!(binding.deactivate_count(producer), 1);

    // Fresh subscribe re-activates → fn re-fires.
    let _sub2 = core.subscribe(producer, noop_sink());
    assert_eq!(
        binding.fn_invocation_count(producer),
        2,
        "fn should re-fire after deactivation + new subscribe"
    );
}

// =====================================================================
// 5. Deactivate hook is producer-only — non-producer nodes don't fire it
// =====================================================================

#[test]
fn r_d031_deactivate_does_not_fire_for_state_nodes() {
    let (core, binding) = make_runtime();
    let s = core.register_state(NO_HANDLE, false).unwrap();

    let sub = core.subscribe(s, noop_sink());
    drop(sub);

    assert_eq!(
        binding.deactivate_count(s),
        0,
        "producer_deactivate is producer-only; state nodes don't fire it"
    );
}

#[test]
fn r_d031_deactivate_does_not_fire_for_derived_nodes() {
    // Even non-producer compute nodes (Derived) don't fire
    // producer_deactivate. The hook is keyed on `is_producer()`.
    let (core, binding) = make_runtime();
    let s = core.register_state(NO_HANDLE, false).unwrap();
    let fn_id = binding.register_producer_fn(FnResult::Noop { tracked: None });
    let derived = core
        .register_derived(&[s], fn_id, EqualsMode::Identity, true)
        .unwrap();

    let sub = core.subscribe(derived, noop_sink());
    drop(sub);

    assert_eq!(
        binding.deactivate_count(derived),
        0,
        "producer_deactivate fires only for Producer nodes"
    );
}

// =====================================================================
// 6. Producer fn return-value emission flows through the wave engine
// =====================================================================

#[test]
fn r_d031_producer_returning_data_emits_value() {
    // When a producer fn returns FnResult::Data, the subscriber receives
    // the corresponding wave just like a derived would.
    let (core, binding) = make_runtime();
    let payload = binding.alloc_handle();
    let fn_id = binding.register_producer_fn(FnResult::Data {
        handle: payload,
        tracked: None,
    });
    let producer = core.register_producer(fn_id).unwrap();

    let received: Arc<Mutex<Vec<HandleId>>> = Arc::new(Mutex::new(Vec::new()));
    let received_clone = received.clone();
    let sink: Sink = Arc::new(move |msgs| {
        for m in msgs {
            if let graphrefly_core::Message::Data(h) = m {
                received_clone.lock().unwrap().push(*h);
            }
        }
    });
    let _sub = core.subscribe(producer, sink);

    assert_eq!(
        *received.lock().unwrap(),
        vec![payload],
        "subscriber should receive Data(payload) from producer fn return"
    );
}

// =====================================================================
// 7. kind_of round-trip: register via unified API, read via kind_of
// =====================================================================

#[test]
fn r_d030_kind_of_is_derived_metadata_not_stored() {
    // Sanity: register a node, verify `kind_of` returns the expected
    // variant. Confirms the derive-on-read path works for all kinds.
    let (core, binding) = make_runtime();
    let fn_id = binding.register_producer_fn(FnResult::Noop { tracked: None });

    let state = core.register_state(NO_HANDLE, false).unwrap();
    let producer = core.register_producer(fn_id).unwrap();
    let derived = core
        .register_derived(&[state], fn_id, EqualsMode::Identity, false)
        .unwrap();
    let dynamic = core
        .register_dynamic(&[state], fn_id, EqualsMode::Identity, true)
        .unwrap();

    assert_eq!(core.kind_of(state), Some(NodeKind::State));
    assert_eq!(core.kind_of(producer), Some(NodeKind::Producer));
    assert_eq!(core.kind_of(derived), Some(NodeKind::Derived));
    assert_eq!(core.kind_of(dynamic), Some(NodeKind::Dynamic));
    assert_eq!(core.kind_of(NodeId::new(9_999)), None);
}

// =====================================================================
// 8. Smoke: ProducerBinding compiles as a BindingBoundary impl
// =====================================================================

#[test]
fn producer_binding_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<ProducerBinding>();
    let _atomic_check: AtomicU64 = AtomicU64::new(0);
    let _ = _atomic_check.load(Ordering::SeqCst);
}
