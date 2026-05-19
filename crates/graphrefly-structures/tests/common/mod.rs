//! Minimal test binding for reactive structure tests.
//!
//! Mirrors `graphrefly-core/tests/common/mod.rs` but simplified: stores
//! `serde_json::Value` directly (no TestValue enum needed). Structures
//! emit snapshots as serialized JSON values via the intern closure.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use graphrefly_core::{
    BindingBoundary, Core, DepBatch, FnId, FnResult, HandleId, Message, NodeId, OwnedCore, Sink,
    SubscriptionId,
};
use serde_json::Value;

struct RegistryInner {
    next_handle: u64,
    values: HashMap<HandleId, Value>,
    refcounts: HashMap<HandleId, u64>,
}

pub struct StructuresTestBinding {
    inner: Mutex<RegistryInner>,
}

impl StructuresTestBinding {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(RegistryInner {
                next_handle: 1,
                values: HashMap::new(),
                refcounts: HashMap::new(),
            }),
        })
    }

    /// Intern a JSON value, returning its handle.
    pub fn intern(&self, value: Value) -> HandleId {
        let mut inner = self.inner.lock().expect("registry lock");
        let h = HandleId::new(inner.next_handle);
        inner.next_handle += 1;
        inner.values.insert(h, value);
        inner.refcounts.insert(h, 1);
        h
    }

    /// Resolve a handle to its underlying JSON value.
    pub fn deref(&self, handle: HandleId) -> Value {
        self.inner
            .lock()
            .expect("registry lock")
            .values
            .get(&handle)
            .cloned()
            .unwrap_or(Value::Null)
    }
}

impl BindingBoundary for StructuresTestBinding {
    fn invoke_fn(&self, _node_id: NodeId, _fn_id: FnId, _dep_data: &[DepBatch]) -> FnResult {
        FnResult::Noop { tracked: None }
    }

    fn custom_equals(&self, _equals_handle: FnId, a: HandleId, b: HandleId) -> bool {
        a == b
    }

    fn release_handle(&self, handle: HandleId) {
        let mut inner = self.inner.lock().expect("registry lock");
        let count = inner.refcounts.entry(handle).or_insert(0);
        if *count > 0 {
            *count -= 1;
        }
        if *count == 0 {
            inner.values.remove(&handle);
            inner.refcounts.remove(&handle);
        }
    }

    fn retain_handle(&self, handle: HandleId) {
        let mut inner = self.inner.lock().expect("registry lock");
        *inner.refcounts.entry(handle).or_insert(0) += 1;
    }
}

/// Recorded event from a subscriber — simplified for structure tests.
#[derive(Clone, Debug)]
pub enum RecordedEvent {
    Start,
    Dirty,
    Data(Value),
    Resolved,
    Other(String),
}

/// Records events from a subscriber.
pub struct Recorder {
    events: Arc<Mutex<Vec<RecordedEvent>>>,
    /// β/D238: owner-driven teardown via [`StructuresRuntime`] (core
    /// RAII `Subscription` retired under the actor model).
    attached: Mutex<Option<(NodeId, SubscriptionId)>>,
}

impl Recorder {
    pub fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            attached: Mutex::new(None),
        }
    }

    pub fn sink(&self, binding: Arc<StructuresTestBinding>) -> Sink {
        let events = self.events.clone();
        Arc::new(move |msgs: &[Message]| {
            let mut guard = events.lock().expect("recorder lock");
            for msg in msgs {
                let recorded = match msg {
                    Message::Start => RecordedEvent::Start,
                    Message::Dirty => RecordedEvent::Dirty,
                    Message::Data(h) => RecordedEvent::Data(binding.deref(*h)),
                    Message::Resolved => RecordedEvent::Resolved,
                    _ => RecordedEvent::Other(format!("{msg:?}")),
                };
                guard.push(recorded);
            }
        })
    }

    pub fn attach(&self, node_id: NodeId, sub_id: SubscriptionId) {
        *self.attached.lock().expect("lock") = Some((node_id, sub_id));
    }

    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.attached.lock().expect("lock").expect("not attached").0
    }

    #[must_use]
    pub fn sub_id(&self) -> SubscriptionId {
        self.attached.lock().expect("lock").expect("not attached").1
    }

    pub fn snapshot(&self) -> Vec<RecordedEvent> {
        self.events.lock().expect("lock").clone()
    }

    pub fn data_values(&self) -> Vec<Value> {
        self.snapshot()
            .into_iter()
            .filter_map(|e| match e {
                RecordedEvent::Data(v) => Some(v),
                _ => None,
            })
            .collect()
    }

    pub fn data_count(&self) -> usize {
        self.snapshot()
            .iter()
            .filter(|e| matches!(e, RecordedEvent::Data(_)))
            .count()
    }
}

/// Test runtime for structures — composes [`OwnedCore`] +
/// [`StructuresTestBinding`].
///
/// D246: Core ownership + subscription tracking + owner-thread `Drop`
/// teardown all live in `OwnedCore`. This newtype only keeps the
/// structures-specific `binding`/`Recorder`/intern infra and routes
/// Core access through `self.core()`.
pub struct StructuresRuntime {
    pub binding: Arc<StructuresTestBinding>,
    rt: OwnedCore,
}

impl StructuresRuntime {
    pub fn new() -> Self {
        let binding = StructuresTestBinding::new();
        let rt = OwnedCore::new(binding.clone() as Arc<dyn BindingBoundary>);
        Self { binding, rt }
    }

    pub fn core(&self) -> &Core {
        self.rt.core()
    }

    pub fn track_subscribe(&self, node_id: NodeId, sink: Sink) -> SubscriptionId {
        self.rt.track_subscribe(node_id, sink)
    }

    pub fn unsubscribe(&self, node_id: NodeId, sub_id: SubscriptionId) {
        self.rt.unsubscribe(node_id, sub_id);
    }

    /// Owner-side mailbox drain (D227/D230/D246 rule 6). Applies every
    /// queued deferred op (e.g. `view`/`scan` in-wave `SinkEmitter`
    /// `post_emit`) in FIFO order via the synchronous Core surface,
    /// cascading nested waves. Idempotent on an empty mailbox.
    pub fn drain_mailbox(&self) {
        self.rt.core().drain_mailbox();
    }

    /// Subscribe a recorder to a node.
    pub fn subscribe_recorder(&self, node_id: NodeId) -> Recorder {
        let recorder = Recorder::new();
        let sink = recorder.sink(self.binding.clone());
        let sub_id = self.track_subscribe(node_id, sink);
        recorder.attach(node_id, sub_id);
        recorder
    }

    /// Create an intern closure that serializes a Vec<T> to JSON.
    pub fn intern_vec_fn<T: serde::Serialize + Send + Sync + 'static>(
        &self,
    ) -> graphrefly_structures::InternFn<Vec<T>> {
        let binding = self.binding.clone();
        Arc::new(move |snapshot: Vec<T>| {
            let json = serde_json::to_value(&snapshot).expect("serialize snapshot");
            binding.intern(json)
        })
    }

    /// Create an intern closure for Vec<(K, V)> (map snapshots).
    pub fn intern_pairs_fn<
        K: serde::Serialize + Send + Sync + 'static,
        V: serde::Serialize + Send + Sync + 'static,
    >(
        &self,
    ) -> graphrefly_structures::InternFn<Vec<(K, V)>> {
        let binding = self.binding.clone();
        Arc::new(move |snapshot: Vec<(K, V)>| {
            let json = serde_json::to_value(&snapshot).expect("serialize snapshot");
            binding.intern(json)
        })
    }

    /// Create an intern closure for Vec<IndexRow<K, V>>.
    pub fn intern_index_fn<
        K: serde::Serialize + Send + Sync + 'static,
        V: serde::Serialize + Send + Sync + 'static,
    >(
        &self,
    ) -> graphrefly_structures::InternFn<Vec<graphrefly_structures::IndexRow<K, V>>> {
        let binding = self.binding.clone();
        Arc::new(
            move |snapshot: Vec<graphrefly_structures::IndexRow<K, V>>| {
                let json = serde_json::to_value(&snapshot).expect("serialize snapshot");
                binding.intern(json)
            },
        )
    }
}
