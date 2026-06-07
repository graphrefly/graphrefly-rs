use std::cell::RefCell;
use std::rc::Rc;

use graphrefly::{
    dict_kv, graph, memory_kv, reactive_cascading_cache, tiered_read_through, CascadingCacheEvent,
    CascadingCachePolicy, CascadingCacheStatus, KvStorageTier, KvVersionedRead, Message, Node,
    PromotionPolicy, ReactiveCascadingCacheOptions, ReadThroughErrorStage, ReadThroughOutcome,
    StorageError, StorageResult, TieredReadThroughOptions, TieredReadThroughStatus,
};

#[derive(Clone)]
struct PlainTier {
    label: &'static str,
    read: StorageResult<Option<i32>>,
    calls: Rc<RefCell<Vec<String>>>,
}

impl PlainTier {
    fn new(
        label: &'static str,
        read: StorageResult<Option<i32>>,
        calls: Rc<RefCell<Vec<String>>>,
    ) -> Self {
        Self { label, read, calls }
    }
}

impl KvStorageTier<i32> for PlainTier {
    fn get(&self, key: &str) -> StorageResult<Option<i32>> {
        self.calls
            .borrow_mut()
            .push(format!("{}:{key}", self.label));
        self.read.clone()
    }

    fn set(&self, key: &str, value: i32) -> StorageResult<()> {
        self.calls
            .borrow_mut()
            .push(format!("{}:set:{key}:{value}", self.label));
        Ok(())
    }

    fn delete(&self, key: &str) -> StorageResult<()> {
        self.calls
            .borrow_mut()
            .push(format!("{}:delete:{key}", self.label));
        Ok(())
    }

    fn list(&self, _prefix: &str) -> StorageResult<Vec<String>> {
        Ok(Vec::new())
    }
}

type Collected<T> = (Rc<RefCell<Vec<T>>>, Box<dyn FnOnce()>);
type KindLog = (Rc<RefCell<Vec<&'static str>>>, Box<dyn FnOnce()>);

fn data_of<T: Clone + 'static>(node: &Node<T>) -> Collected<T> {
    let out = Rc::new(RefCell::new(Vec::new()));
    let sink = out.clone();
    let unsub = node.subscribe(move |msg| {
        if let Message::Data(value) = msg {
            if let Some(value) = value.as_ref().downcast_ref::<T>() {
                sink.borrow_mut().push(value.clone());
            }
        }
    });
    (out, unsub)
}

fn message_kinds<T: 'static>(node: &Node<T>) -> KindLog {
    let out = Rc::new(RefCell::new(Vec::new()));
    let sink = out.clone();
    let unsub = node.subscribe(move |msg| {
        let kind = match msg {
            Message::Start => "START",
            Message::Pause(_) => "PAUSE",
            Message::Resume(_) => "RESUME",
            Message::Dirty => "DIRTY",
            Message::Data(_) => "DATA",
            Message::Resolved => "RESOLVED",
            Message::Invalidate => "INVALIDATE",
            Message::Complete => "COMPLETE",
            Message::Error(_) => "ERROR",
            Message::Teardown => "TEARDOWN",
        };
        sink.borrow_mut().push(kind);
    });
    (out, unsub)
}

fn event_kind<V>(event: &CascadingCacheEvent<V>) -> &'static str {
    match event {
        CascadingCacheEvent::Request { .. } => "request",
        CascadingCacheEvent::Invalidate { .. } => "invalidate",
        CascadingCacheEvent::Lookup { .. } => "lookup",
        CascadingCacheEvent::Promotion { .. } => "promotion",
        CascadingCacheEvent::Fill { .. } => "fill",
        CascadingCacheEvent::Error { .. } => "error",
    }
}

#[test]
fn memory_kv_stores_cloned_values_and_lists_prefix_in_order() {
    let kv = memory_kv::<Vec<i32>>();
    let mut value = vec![2];
    kv.set("items/002", value.clone()).unwrap();
    kv.set("other/001", vec![9]).unwrap();
    value[0] = 7;
    kv.set("items/001", vec![1]).unwrap();

    assert_eq!(kv.get("items/002").unwrap(), Some(vec![2]));
    let mut read = kv.get("items/002").unwrap().unwrap();
    read[0] = 8;
    assert_eq!(kv.get("items/002").unwrap(), Some(vec![2]));
    assert_eq!(
        kv.list("items/").unwrap(),
        vec!["items/001".to_owned(), "items/002".to_owned()]
    );
}

#[test]
fn dict_kv_preloads_entries() {
    let kv = dict_kv([("a", 1), ("b", 2)]);

    assert_eq!(kv.get("a").unwrap(), Some(1));
    assert_eq!(kv.get("b").unwrap(), Some(2));
}

#[test]
fn memory_kv_versioned_present_and_absent_observations_are_opaque_per_key() {
    let kv = memory_kv::<i32>();

    let absent = kv.get_versioned("k").unwrap();
    let absent_generation = absent.generation().clone();
    assert!(matches!(absent, KvVersionedRead::Miss { .. }));
    assert!(kv.set_if_match("k", 1, &absent_generation).unwrap());
    assert!(!kv.set_if_match("k", 2, &absent_generation).unwrap());
    assert!(!kv.set_if_match("other", 9, &absent_generation).unwrap());
    assert_eq!(kv.get("k").unwrap(), Some(1));

    let present = kv.get_versioned("k").unwrap();
    let present_generation = present.generation().clone();
    assert!(matches!(present, KvVersionedRead::Hit { value: 1, .. }));
    kv.set("k", 3).unwrap();
    assert!(!kv.set_if_match("k", 4, &present_generation).unwrap());
    assert_eq!(kv.get("k").unwrap(), Some(3));

    let fresh = kv.get_versioned("k").unwrap();
    assert!(kv.set_if_match("k", 4, fresh.generation()).unwrap());
    assert_eq!(kv.get("k").unwrap(), Some(4));

    let pre_clear = kv.get_versioned("fresh").unwrap();
    kv.clear();
    assert!(!kv
        .set_if_match("fresh", 11, pre_clear.generation())
        .unwrap());
    assert_eq!(kv.get("fresh").unwrap(), None);

    let miss_before_cycle = kv.get_versioned("cycle").unwrap();
    kv.set("cycle", 5).unwrap();
    kv.delete("cycle").unwrap();
    assert!(!kv
        .set_if_match("cycle", 6, miss_before_cycle.generation())
        .unwrap());
}

#[test]
fn tiered_read_through_orders_tiers_and_promotes_first_hit() {
    let calls = Rc::new(RefCell::new(Vec::new()));
    let hot = PlainTier::new("hot", Ok(None), calls.clone());
    let warm = PlainTier::new("warm", Ok(Some(2)), calls.clone());
    let cold = PlainTier::new("cold", Ok(Some(1)), calls.clone());
    let mut opts = TieredReadThroughOptions::new("k", vec![&hot, &warm, &cold]);
    opts.tier_names = vec!["hot".to_owned(), "warm".to_owned(), "cold".to_owned()];

    let result = tiered_read_through(opts);

    assert_eq!(result.status, TieredReadThroughStatus::Hit);
    assert_eq!(result.value, Some(2));
    assert_eq!(result.hit_tier.unwrap().index, 1);
    assert_eq!(
        result
            .facts
            .iter()
            .map(|fact| fact.outcome.clone())
            .collect::<Vec<_>>(),
        vec![ReadThroughOutcome::Miss, ReadThroughOutcome::Hit]
    );
    assert_eq!(result.promotions.len(), 1);
    assert_eq!(result.promotions[0].tier.index, 0);
    assert!(result.promotions[0].ok);
    assert_eq!(*calls.borrow(), vec!["hot:k", "warm:k", "hot:set:k:2"]);
}

#[test]
fn tiered_read_through_can_disable_promotion_for_graph_layer_default() {
    let hot = memory_kv::<i32>();
    let cold = memory_kv::<i32>();
    cold.set("k", 42).unwrap();
    let mut opts = TieredReadThroughOptions::new("k", vec![&hot, &cold]);
    opts.promote_to = PromotionPolicy::Disabled;

    let result = tiered_read_through(opts);

    assert_eq!(result.status, TieredReadThroughStatus::Hit);
    assert_eq!(
        result
            .facts
            .iter()
            .map(|fact| (fact.outcome.clone(), fact.tier.index))
            .collect::<Vec<_>>(),
        vec![(ReadThroughOutcome::Miss, 0), (ReadThroughOutcome::Hit, 1)]
    );
    assert!(result.promotions.is_empty());
    assert_eq!(hot.get("k").unwrap(), None);
}

#[test]
fn tiered_read_through_loader_fallback_and_miss_reporting() {
    let miss_calls = Rc::new(RefCell::new(Vec::new()));
    let error_calls = Rc::new(RefCell::new(Vec::new()));
    let miss_tier = PlainTier::new("miss", Ok(None), Rc::new(RefCell::new(Vec::new())));
    let hit_tier = PlainTier::new("hit", Ok(None), Rc::new(RefCell::new(Vec::new())));
    let mut opts = TieredReadThroughOptions::new("user:1", vec![&miss_tier, &hit_tier]);
    opts.tier_names = vec!["miss".to_owned(), "hit".to_owned()];
    opts.load = Some(Box::new(|key| {
        if key == "user:1" {
            Ok(Some(7))
        } else {
            Ok(None)
        }
    }));
    opts.on_miss = Some(Box::new({
        let miss_calls = miss_calls.clone();
        move |ctx| miss_calls.borrow_mut().push(ctx.tier.index)
    }));
    opts.on_error = Some(Box::new({
        let error_calls = error_calls.clone();
        move |ctx| error_calls.borrow_mut().push(ctx.stage)
    }));

    let result = tiered_read_through(opts);
    assert_eq!(result.status, TieredReadThroughStatus::Hit);
    assert_eq!(result.value, Some(7));
    assert_eq!(result.hit_tier.unwrap().index, -1);
    assert_eq!(
        result
            .facts
            .iter()
            .map(|fact| fact.outcome.clone())
            .collect::<Vec<_>>(),
        vec![
            ReadThroughOutcome::Miss,
            ReadThroughOutcome::Miss,
            ReadThroughOutcome::Hit
        ]
    );
    assert_eq!(*miss_calls.borrow(), vec![0, 1]);
    assert!(error_calls.borrow().is_empty());

    let empty_result =
        tiered_read_through::<i32>(TieredReadThroughOptions::new("nowhere", Vec::new()));
    assert_eq!(empty_result.status, TieredReadThroughStatus::Miss);
    assert!(empty_result.facts.is_empty());

    let errors = Rc::new(RefCell::new(Vec::new()));
    let bad = PlainTier::new(
        "bad",
        Err(StorageError::backend("read failed")),
        Rc::new(RefCell::new(Vec::new())),
    );
    let good = PlainTier::new("good", Ok(Some(9)), Rc::new(RefCell::new(Vec::new())));
    let mut error_opts = TieredReadThroughOptions::new("k", vec![&bad, &good]);
    error_opts.on_error = Some(Box::new({
        let errors = errors.clone();
        move |ctx| {
            assert_eq!(ctx.stage, ReadThroughErrorStage::Lookup);
            errors.borrow_mut().push(ctx.error.to_string());
        }
    }));
    let recovered = tiered_read_through(error_opts);
    assert_eq!(recovered.status, TieredReadThroughStatus::Hit);
    assert_eq!(
        recovered
            .facts
            .iter()
            .map(|fact| fact.outcome.clone())
            .collect::<Vec<_>>(),
        vec![ReadThroughOutcome::Error, ReadThroughOutcome::Hit]
    );
    assert_eq!(*errors.borrow(), vec!["read failed".to_owned()]);
}

struct StaleVersionedTier {
    inner: graphrefly::MemoryKv<i32>,
}

impl KvStorageTier<i32> for StaleVersionedTier {
    fn get(&self, key: &str) -> StorageResult<Option<i32>> {
        self.inner.get(key)
    }

    fn set(&self, _key: &str, _value: i32) -> StorageResult<()> {
        Err(StorageError::backend("plain set must not run"))
    }

    fn delete(&self, key: &str) -> StorageResult<()> {
        self.inner.delete(key)
    }

    fn list(&self, prefix: &str) -> StorageResult<Vec<String>> {
        self.inner.list(prefix)
    }

    fn supports_versioned(&self) -> bool {
        true
    }

    fn get_versioned(&self, key: &str) -> StorageResult<KvVersionedRead<i32>> {
        let observed = self.inner.get_versioned(key)?;
        self.inner.set(key, 1)?;
        Ok(observed)
    }

    fn set_if_match(
        &self,
        key: &str,
        value: i32,
        generation: &graphrefly::KvGeneration,
    ) -> StorageResult<bool> {
        self.inner.set_if_match(key, value, generation)
    }
}

struct BrokenVersionedTier {
    calls: Rc<RefCell<Vec<&'static str>>>,
}

impl KvStorageTier<i32> for BrokenVersionedTier {
    fn get(&self, _key: &str) -> StorageResult<Option<i32>> {
        self.calls.borrow_mut().push("get");
        Ok(None)
    }

    fn set(&self, _key: &str, _value: i32) -> StorageResult<()> {
        self.calls.borrow_mut().push("set");
        Err(StorageError::backend("plain set must not run"))
    }

    fn delete(&self, _key: &str) -> StorageResult<()> {
        Ok(())
    }

    fn list(&self, _prefix: &str) -> StorageResult<Vec<String>> {
        Ok(Vec::new())
    }

    fn supports_versioned(&self) -> bool {
        true
    }

    fn get_versioned(&self, _key: &str) -> StorageResult<KvVersionedRead<i32>> {
        self.calls.borrow_mut().push("get_versioned");
        Err(StorageError::backend("versioned read failed"))
    }

    fn set_if_match(
        &self,
        _key: &str,
        _value: i32,
        _generation: &graphrefly::KvGeneration,
    ) -> StorageResult<bool> {
        self.calls.borrow_mut().push("set_if_match");
        Ok(true)
    }
}

#[test]
fn tiered_read_through_versioned_promotion_requires_generation() {
    let hot_inner = memory_kv::<i32>();
    let hot = StaleVersionedTier {
        inner: hot_inner.clone(),
    };
    let cold = memory_kv::<i32>();
    cold.set("k", 7).unwrap();
    let mut opts = TieredReadThroughOptions::new("k", vec![&hot, &cold]);
    opts.promote_to = PromotionPolicy::Indices(vec![0]);

    let result = tiered_read_through(opts);

    assert_eq!(result.status, TieredReadThroughStatus::Hit);
    assert_eq!(result.value, Some(7));
    assert_eq!(
        result
            .facts
            .iter()
            .map(|fact| (fact.outcome.clone(), fact.tier.index))
            .collect::<Vec<_>>(),
        vec![(ReadThroughOutcome::Miss, 0), (ReadThroughOutcome::Hit, 1)]
    );
    assert_eq!(result.promotions.len(), 1);
    assert_eq!(result.promotions[0].tier.index, 0);
    assert!(!result.promotions[0].ok);
    assert_eq!(hot_inner.get("k").unwrap(), Some(1));

    let calls = Rc::new(RefCell::new(Vec::new()));
    let broken = BrokenVersionedTier {
        calls: calls.clone(),
    };
    let cold = memory_kv::<i32>();
    cold.set("k", 9).unwrap();
    let mut opts = TieredReadThroughOptions::new("k", vec![&broken, &cold]);
    opts.promote_to = PromotionPolicy::Indices(vec![0]);
    let errors = Rc::new(RefCell::new(Vec::new()));
    opts.on_error = Some(Box::new({
        let errors = errors.clone();
        move |ctx| errors.borrow_mut().push((ctx.stage, ctx.error.to_string()))
    }));

    let result = tiered_read_through(opts);

    assert_eq!(result.status, TieredReadThroughStatus::Hit);
    assert_eq!(result.value, Some(9));
    assert_eq!(
        result
            .facts
            .iter()
            .map(|fact| (fact.outcome.clone(), fact.tier.index))
            .collect::<Vec<_>>(),
        vec![(ReadThroughOutcome::Error, 0), (ReadThroughOutcome::Hit, 1)]
    );
    assert_eq!(result.promotions.len(), 1);
    assert!(!result.promotions[0].ok);
    assert!(result.promotions[0]
        .error
        .as_ref()
        .unwrap()
        .to_string()
        .contains("not observed with a generation"));
    assert_eq!(*calls.borrow(), vec!["get_versioned"]);
    assert_eq!(
        *errors.borrow(),
        vec![
            (
                ReadThroughErrorStage::Lookup,
                "versioned read failed".to_owned()
            ),
            (
                ReadThroughErrorStage::Promotion,
                "tiered_read_through: versioned promotion target was not observed with a generation"
                    .to_owned()
            )
        ]
    );
}

#[test]
fn reactive_cascading_cache_exposes_visible_topology_and_default_no_promotion() {
    let g = graph();
    let request = g.state_empty_opts::<String>(graphrefly::GraphNodeOpts::named("request"));
    let hot = memory_kv::<i32>();
    let cold = memory_kv::<i32>();
    cold.set("user:1", 7).unwrap();
    let cache = reactive_cascading_cache(
        &g,
        ReactiveCascadingCacheOptions {
            request: request.clone(),
            tiers: vec![Rc::new(hot.clone()), Rc::new(cold.clone())],
            tier_names: vec!["hot".to_owned(), "cold".to_owned()],
            name: Some("cache".to_owned()),
            ..ReactiveCascadingCacheOptions::new(request.clone(), Vec::new())
        },
    );
    let (statuses, _status_unsub) = data_of(&cache.status);
    let (values, _value_unsub) = data_of(&cache.value);
    let (events, _events_unsub) = data_of(&cache.events);

    assert_eq!(*statuses.borrow(), vec![CascadingCacheStatus::Idle]);
    request.down(vec![Message::Data(Rc::new("user:1".to_owned()))]);

    let snap = g.describe();
    let mut ids = snap.nodes.iter().map(|n| n.id.as_str()).collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec!["cache.events", "cache.status", "cache.value", "request"]
    );
    assert!(snap.edges.contains(&graphrefly::DescribeEdge {
        from: "request".to_owned(),
        to: "cache.events".to_owned(),
    }));
    assert!(snap.edges.contains(&graphrefly::DescribeEdge {
        from: "cache.events".to_owned(),
        to: "cache.status".to_owned(),
    }));
    assert!(snap.edges.contains(&graphrefly::DescribeEdge {
        from: "cache.events".to_owned(),
        to: "cache.value".to_owned(),
    }));

    assert_eq!(*values.borrow(), vec![7]);
    assert_eq!(cache.value.cache(), Some(7));
    assert_eq!(hot.get("user:1").unwrap(), None);
    assert_eq!(
        statuses.borrow().last(),
        Some(&CascadingCacheStatus::Hit {
            key: "user:1".to_owned(),
            request_seq: 1,
            tier: Some(graphrefly::ReadThroughLookupTier {
                index: 1,
                name: Some("cold".to_owned()),
            }),
        })
    );
    assert_eq!(
        events.borrow().iter().map(event_kind).collect::<Vec<_>>(),
        vec!["request", "lookup", "lookup", "fill"]
    );
}

#[test]
fn reactive_cascading_cache_promotes_only_when_explicitly_enabled() {
    let g = graph();
    let request = g.state_empty::<String>();
    let hot = memory_kv::<i32>();
    let cold = memory_kv::<i32>();
    cold.set("k", 42).unwrap();
    let mut opts = ReactiveCascadingCacheOptions::new(
        request.clone(),
        vec![Rc::new(hot.clone()), Rc::new(cold.clone())],
    );
    opts.tier_names = vec!["hot".to_owned(), "cold".to_owned()];
    opts.promote_to = PromotionPolicy::Indices(vec![0]);
    opts.name = Some("cache".to_owned());
    let cache = reactive_cascading_cache(&g, opts);
    let (_statuses, _status_unsub) = data_of(&cache.status);
    let (_values, _value_unsub) = data_of(&cache.value);
    let (events, _events_unsub) = data_of(&cache.events);

    request.down(vec![Message::Data(Rc::new("k".to_owned()))]);

    assert_eq!(cache.value.cache(), Some(42));
    assert_eq!(hot.get("k").unwrap(), Some(42));
    assert_eq!(
        events.borrow().iter().map(event_kind).collect::<Vec<_>>(),
        vec!["request", "lookup", "lookup", "promotion", "fill"]
    );
}

#[test]
fn reactive_cascading_cache_uses_versioned_promotion_without_overwriting_stale_tier() {
    let g = graph();
    let request = g.state_empty::<String>();
    let hot_inner = memory_kv::<i32>();
    let hot = StaleVersionedTier {
        inner: hot_inner.clone(),
    };
    let cold = memory_kv::<i32>();
    cold.set("k", 7).unwrap();
    let mut opts = ReactiveCascadingCacheOptions::new(
        request.clone(),
        vec![Rc::new(hot), Rc::new(cold.clone())],
    );
    opts.promote_to = PromotionPolicy::Indices(vec![0]);
    opts.name = Some("cache".to_owned());
    let cache = reactive_cascading_cache(&g, opts);
    let (_statuses, _status_unsub) = data_of(&cache.status);
    let (_values, _value_unsub) = data_of(&cache.value);
    let (events, _events_unsub) = data_of(&cache.events);

    request.down(vec![Message::Data(Rc::new("k".to_owned()))]);

    assert_eq!(cache.value.cache(), Some(7));
    assert_eq!(hot_inner.get("k").unwrap(), Some(1));
    assert!(events.borrow().iter().any(|event| matches!(
        event,
        CascadingCacheEvent::Promotion {
            tier,
            ok: false,
            ..
        } if tier.index == 0
    )));
}

#[test]
fn reactive_cascading_cache_loader_fallback_policy_and_invalidation_are_graph_events() {
    let g = graph();
    let request = g.state_empty::<String>();
    let policy = g.state_empty::<CascadingCachePolicy>();
    let invalidate = g.state_empty::<String>();
    let hot = memory_kv::<i32>();
    let cold = memory_kv::<i32>();
    let load_calls = Rc::new(RefCell::new(Vec::new()));
    let calls = load_calls.clone();
    let mut opts = ReactiveCascadingCacheOptions::new(
        request.clone(),
        vec![Rc::new(hot.clone()), Rc::new(cold.clone())],
    );
    opts.policy = Some(policy.clone());
    opts.invalidate = Some(invalidate.clone());
    opts.load = Some(Rc::new(move |key| {
        calls.borrow_mut().push(key.to_owned());
        Ok(if key == "remote" { Some(9) } else { None })
    }));
    opts.name = Some("cache".to_owned());
    let cache = reactive_cascading_cache(&g, opts);
    let (statuses, _status_unsub) = data_of(&cache.status);
    let (value_kinds, _value_kind_unsub) = message_kinds(&cache.value);
    let (events, _events_unsub) = data_of(&cache.events);

    request.down(vec![Message::Data(Rc::new("remote".to_owned()))]);

    assert_eq!(cache.value.cache(), Some(9));
    assert_eq!(hot.get("remote").unwrap(), None);
    assert_eq!(
        statuses.borrow().last(),
        Some(&CascadingCacheStatus::Hit {
            key: "remote".to_owned(),
            request_seq: 1,
            tier: Some(graphrefly::ReadThroughLookupTier {
                index: -1,
                name: Some("load".to_owned()),
            }),
        })
    );

    cold.set("remote", 3).unwrap();
    policy.down(vec![Message::Data(Rc::new(CascadingCachePolicy {
        promote_to: Some(PromotionPolicy::Indices(vec![0])),
    }))]);
    assert_eq!(hot.get("remote").unwrap(), Some(3));

    invalidate.down(vec![Message::Data(Rc::new("remote".to_owned()))]);

    assert_eq!(
        events
            .borrow()
            .iter()
            .filter(|event| matches!(event, CascadingCacheEvent::Invalidate { .. }))
            .count(),
        1
    );
    assert!(value_kinds.borrow().contains(&"INVALIDATE"));
    assert_eq!(*load_calls.borrow(), vec!["remote".to_owned()]);
}

#[test]
fn reactive_cascading_cache_load_panic_becomes_error_events_not_terminal_error() {
    let g = graph();
    let request = g.state_empty::<String>();
    let mut opts = ReactiveCascadingCacheOptions::new(request.clone(), Vec::new());
    opts.load = Some(Rc::new(|_| -> StorageResult<Option<i32>> {
        panic!("loader exploded")
    }));
    opts.name = Some("cache".to_owned());
    let cache = reactive_cascading_cache(&g, opts);
    let (statuses, _status_unsub) = data_of(&cache.status);
    let (events, _events_unsub) = data_of(&cache.events);

    request.down(vec![Message::Data(Rc::new("k".to_owned()))]);

    assert_eq!(cache.events.status(), graphrefly::Status::Settled);
    assert_eq!(
        statuses.borrow().last(),
        Some(&CascadingCacheStatus::Error {
            key: "k".to_owned(),
            request_seq: 1,
            error: StorageError::backend("loader exploded"),
        })
    );
    assert_eq!(
        events.borrow().iter().map(event_kind).collect::<Vec<_>>(),
        vec!["request", "error", "fill"]
    );
}
