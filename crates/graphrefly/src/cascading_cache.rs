//! Graph-layer reactive cascading cache helper (D104/D105/D107/D123).
//!
//! The passive read-through algorithm stays in [`crate::storage`]. This module
//! wraps it as visible graph topology: request/policy/invalidate deps drive an
//! events node, and status/value are derived from those events.

use std::collections::BTreeMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;

use crate::graph::{Graph, GraphNodeOpts};
use crate::node::{Node, NodeOpts};
use crate::protocol::{AnyValue, Message};
use crate::storage::{
    tiered_read_through, KvStorageTier, PromotionPolicy, ReadThroughErrorStage,
    ReadThroughLookupTier, ReadThroughOutcome, StorageError, StorageResult,
    TieredReadThroughOptions, TieredReadThroughResult, TieredReadThroughStatus,
};

/// Dynamic promotion policy for [`reactive_cascading_cache`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CascadingCachePolicy {
    pub promote_to: Option<PromotionPolicy>,
}

/// Visible cache status emitted by [`ReactiveCascadingCache::status`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CascadingCacheStatus {
    Idle,
    Loading {
        key: String,
        request_seq: u64,
    },
    Hit {
        key: String,
        request_seq: u64,
        tier: Option<ReadThroughLookupTier>,
    },
    Miss {
        key: String,
        request_seq: u64,
    },
    Error {
        key: String,
        request_seq: u64,
        error: StorageError,
    },
}

/// Visible cache facts emitted by [`ReactiveCascadingCache::events`].
#[derive(Debug, Clone, PartialEq)]
pub enum CascadingCacheEvent<V> {
    Request {
        key: String,
        request_seq: u64,
    },
    Invalidate {
        key: String,
        request_seq: u64,
    },
    Lookup {
        key: String,
        request_seq: u64,
        outcome: ReadThroughOutcome,
        tier: ReadThroughLookupTier,
        value: Option<V>,
        error: Option<StorageError>,
    },
    Promotion {
        key: String,
        request_seq: u64,
        tier: ReadThroughLookupTier,
        ok: bool,
        error: Option<StorageError>,
    },
    Fill {
        key: String,
        request_seq: u64,
        status: TieredReadThroughStatus,
        value: Option<V>,
        tier: Option<ReadThroughLookupTier>,
        error: Option<StorageError>,
    },
    Error {
        key: String,
        request_seq: u64,
        stage: ReadThroughErrorStage,
        tier: Option<ReadThroughLookupTier>,
        error: StorageError,
    },
}

pub type ReactiveCascadingCacheLoadFn<V> = dyn Fn(&str) -> StorageResult<Option<V>>;

/// Options for the Rust graph-layer cache factory.
pub struct ReactiveCascadingCacheOptions<V: Clone + 'static> {
    pub request: Node<String>,
    pub policy: Option<Node<CascadingCachePolicy>>,
    pub invalidate: Option<Node<String>>,
    pub tiers: Vec<Rc<dyn KvStorageTier<V>>>,
    pub load: Option<Rc<ReactiveCascadingCacheLoadFn<V>>>,
    pub tier_names: Vec<String>,
    pub promote_to: PromotionPolicy,
    pub name: Option<String>,
    pub meta: BTreeMap<String, String>,
}

impl<V: Clone + 'static> ReactiveCascadingCacheOptions<V> {
    pub fn new(request: Node<String>, tiers: Vec<Rc<dyn KvStorageTier<V>>>) -> Self {
        Self {
            request,
            policy: None,
            invalidate: None,
            tiers,
            load: None,
            tier_names: Vec::new(),
            promote_to: PromotionPolicy::Disabled,
            name: None,
            meta: BTreeMap::new(),
        }
    }
}

/// Graph-visible bundle returned by [`reactive_cascading_cache`].
pub struct ReactiveCascadingCache<V: Clone + 'static> {
    pub value: Node<V>,
    pub status: Node<CascadingCacheStatus>,
    pub events: Node<CascadingCacheEvent<V>>,
}

#[derive(Debug, Clone, Default)]
struct DriverState {
    seq: u64,
    latest_key: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct SeqState {
    seq: u64,
}

/// Free-standing graph-layer cascading cache factory (D104/D105/D107/D123).
///
/// This is not a [`Graph`] method and not an imperative cache object. Request,
/// optional policy, and optional invalidation deps are declared topology. Storage
/// promotion defaults to disabled at this graph layer; opt in through
/// [`ReactiveCascadingCacheOptions::promote_to`] or a policy node.
pub fn reactive_cascading_cache<V: Clone + 'static>(
    graph: &Graph,
    opts: ReactiveCascadingCacheOptions<V>,
) -> ReactiveCascadingCache<V> {
    let ReactiveCascadingCacheOptions {
        request,
        policy,
        invalidate,
        tiers,
        load,
        tier_names,
        promote_to,
        name,
        meta,
    } = opts;

    let base_name = name.unwrap_or_else(|| "reactiveCascadingCache".to_owned());
    let mut event_deps = vec![request.erased()];
    let policy_index = policy.as_ref().map(|node| {
        event_deps.push(node.erased());
        event_deps.len() - 1
    });
    let invalidate_index = invalidate.as_ref().map(|node| {
        event_deps.push(node.erased());
        event_deps.len() - 1
    });

    let events = graph.node_opts::<CascadingCacheEvent<V>, _>(
        event_deps,
        {
            let tiers = tiers.clone();
            let tier_names = tier_names.clone();
            let load = load.clone();
            let base_promote_to = promote_to.clone();
            move |ctx| {
                let mut st = ctx
                    .state_get::<DriverState>()
                    .map(|s| (*s).clone())
                    .unwrap_or_default();
                let current_policy = policy_index
                    .and_then(|idx| ctx.data::<CascadingCachePolicy>(idx))
                    .map(|p| (*p).clone());
                let effective_promote_to = current_policy
                    .as_ref()
                    .and_then(|p| p.promote_to.clone())
                    .unwrap_or_else(|| base_promote_to.clone());

                for key in ctx.batch::<String>(0) {
                    start_lookup(
                        ctx,
                        &mut st,
                        LookupCause::Request,
                        (*key).clone(),
                        LookupInputs {
                            tiers: &tiers,
                            tier_names: &tier_names,
                            load: load.as_ref(),
                            promote_to: effective_promote_to.clone(),
                        },
                    );
                }

                if let Some(idx) = invalidate_index {
                    for key in ctx.batch::<String>(idx) {
                        start_lookup(
                            ctx,
                            &mut st,
                            LookupCause::Invalidate,
                            (*key).clone(),
                            LookupInputs {
                                tiers: &tiers,
                                tier_names: &tier_names,
                                load: load.as_ref(),
                                promote_to: effective_promote_to.clone(),
                            },
                        );
                    }
                }

                if ctx.batch::<String>(0).is_empty()
                    && invalidate_index
                        .map(|idx| ctx.batch::<String>(idx).is_empty())
                        .unwrap_or(true)
                    && policy_index
                        .map(|idx| !ctx.batch::<CascadingCachePolicy>(idx).is_empty())
                        .unwrap_or(false)
                {
                    if let Some(key) = st.latest_key.clone() {
                        start_lookup(
                            ctx,
                            &mut st,
                            LookupCause::Request,
                            key,
                            LookupInputs {
                                tiers: &tiers,
                                tier_names: &tier_names,
                                load: load.as_ref(),
                                promote_to: effective_promote_to,
                            },
                        );
                    }
                }

                ctx.state_set(st);
            }
        },
        GraphNodeOpts {
            name: Some(format!("{base_name}.events")),
            meta: meta.clone(),
            node: NodeOpts {
                partial: true,
                ..NodeOpts::default()
            },
            ..GraphNodeOpts::default()
        },
    );

    let status = graph.node_opts_initial::<CascadingCacheStatus, _>(
        vec![events.erased()],
        |ctx| {
            let mut st = ctx
                .state_get::<SeqState>()
                .map(|s| (*s).clone())
                .unwrap_or_default();
            for event in ctx.batch::<CascadingCacheEvent<V>>(0) {
                match event.as_ref() {
                    CascadingCacheEvent::Request { key, request_seq }
                    | CascadingCacheEvent::Invalidate { key, request_seq } => {
                        st.seq = *request_seq;
                        ctx.emit(CascadingCacheStatus::Loading {
                            key: key.clone(),
                            request_seq: *request_seq,
                        });
                    }
                    CascadingCacheEvent::Fill {
                        key,
                        request_seq,
                        status,
                        tier,
                        error,
                        ..
                    } if *request_seq == st.seq => match status {
                        TieredReadThroughStatus::Hit => ctx.emit(CascadingCacheStatus::Hit {
                            key: key.clone(),
                            request_seq: *request_seq,
                            tier: tier.clone(),
                        }),
                        TieredReadThroughStatus::Miss => ctx.emit(CascadingCacheStatus::Miss {
                            key: key.clone(),
                            request_seq: *request_seq,
                        }),
                        TieredReadThroughStatus::Error => ctx.emit(CascadingCacheStatus::Error {
                            key: key.clone(),
                            request_seq: *request_seq,
                            error: error
                                .clone()
                                .unwrap_or_else(|| StorageError::backend("read-through failed")),
                        }),
                    },
                    CascadingCacheEvent::Error {
                        key,
                        request_seq,
                        error,
                        ..
                    } if *request_seq == st.seq => ctx.emit(CascadingCacheStatus::Error {
                        key: key.clone(),
                        request_seq: *request_seq,
                        error: error.clone(),
                    }),
                    _ => {}
                }
            }
            ctx.state_set(st);
        },
        GraphNodeOpts {
            name: Some(format!("{base_name}.status")),
            meta: meta.clone(),
            node: NodeOpts {
                partial: true,
                ..NodeOpts::default()
            },
            ..GraphNodeOpts::default()
        },
        Some(CascadingCacheStatus::Idle),
    );

    let value = graph.node_opts::<V, _>(
        vec![events.erased()],
        |ctx| {
            let mut st = ctx
                .state_get::<SeqState>()
                .map(|s| (*s).clone())
                .unwrap_or_default();
            for event in ctx.batch::<CascadingCacheEvent<V>>(0) {
                match event.as_ref() {
                    CascadingCacheEvent::Request { request_seq, .. } => {
                        st.seq = *request_seq;
                    }
                    CascadingCacheEvent::Invalidate { request_seq, .. } => {
                        st.seq = *request_seq;
                        ctx.down(vec![Message::Invalidate]);
                    }
                    CascadingCacheEvent::Fill {
                        request_seq,
                        status,
                        value,
                        ..
                    } if *request_seq == st.seq => {
                        if *status == TieredReadThroughStatus::Hit {
                            if let Some(value) = value.clone() {
                                ctx.emit(value);
                            } else {
                                ctx.down(vec![Message::Invalidate]);
                            }
                        } else {
                            ctx.down(vec![Message::Invalidate]);
                        }
                    }
                    _ => {}
                }
            }
            ctx.state_set(st);
        },
        GraphNodeOpts {
            name: Some(format!("{base_name}.value")),
            meta,
            node: NodeOpts {
                partial: true,
                ..NodeOpts::default()
            },
            ..GraphNodeOpts::default()
        },
    );

    ReactiveCascadingCache {
        value,
        status,
        events,
    }
}

#[derive(Clone, Copy)]
enum LookupCause {
    Request,
    Invalidate,
}

struct LookupInputs<'a, V: Clone + 'static> {
    tiers: &'a [Rc<dyn KvStorageTier<V>>],
    tier_names: &'a [String],
    load: Option<&'a Rc<ReactiveCascadingCacheLoadFn<V>>>,
    promote_to: PromotionPolicy,
}

fn start_lookup<V: Clone + 'static>(
    ctx: &crate::ctx::Ctx,
    st: &mut DriverState,
    cause: LookupCause,
    key: String,
    inputs: LookupInputs<'_, V>,
) {
    st.seq += 1;
    let request_seq = st.seq;
    st.latest_key = Some(key.clone());
    let start_event: CascadingCacheEvent<V> = match cause {
        LookupCause::Request => CascadingCacheEvent::Request {
            key: key.clone(),
            request_seq,
        },
        LookupCause::Invalidate => CascadingCacheEvent::Invalidate {
            key: key.clone(),
            request_seq,
        },
    };
    emit_event(ctx, start_event);

    let tier_refs = inputs
        .tiers
        .iter()
        .map(|tier| tier.as_ref())
        .collect::<Vec<&dyn KvStorageTier<V>>>();
    let mut read_opts = TieredReadThroughOptions::new(key.clone(), tier_refs);
    read_opts.tier_names = inputs.tier_names.to_vec();
    read_opts.promote_to = inputs.promote_to;
    if let Some(load) = inputs.load {
        let load = load.clone();
        read_opts.load = Some(Box::new(move |key| load(key)));
    }
    let messages = match catch_unwind(AssertUnwindSafe(|| tiered_read_through(read_opts))) {
        Ok(result) => events_from_result(key, request_seq, result),
        Err(payload) => events_from_error::<V>(
            key,
            request_seq,
            StorageError::backend(panic_payload(payload)),
        ),
    };
    if !messages.is_empty() {
        ctx.down(messages);
    }
}

fn emit_event<V: Clone + 'static>(ctx: &crate::ctx::Ctx, event: CascadingCacheEvent<V>) {
    let value: AnyValue = Rc::new(event);
    ctx.down(vec![Message::Data(value)]);
}

fn events_from_result<V: Clone + 'static>(
    key: String,
    request_seq: u64,
    result: TieredReadThroughResult<V>,
) -> Vec<Message<AnyValue>> {
    let mut events = Vec::new();
    let mut first_error = None;
    for fact in result.facts {
        let error = fact.error.clone();
        let lookup_event = CascadingCacheEvent::Lookup {
            key: key.clone(),
            request_seq,
            outcome: fact.outcome.clone(),
            tier: fact.tier.clone(),
            value: fact.value.clone(),
            error: error.clone(),
        };
        events.push(data_msg(lookup_event));
        if fact.outcome == ReadThroughOutcome::Error {
            if let Some(error) = error {
                first_error.get_or_insert_with(|| error.clone());
                events.push(data_msg::<V>(CascadingCacheEvent::Error {
                    key: key.clone(),
                    request_seq,
                    stage: ReadThroughErrorStage::Lookup,
                    tier: Some(fact.tier),
                    error,
                }));
            }
        }
    }
    for promotion in result.promotions {
        let error = promotion.error.clone();
        events.push(data_msg::<V>(CascadingCacheEvent::Promotion {
            key: key.clone(),
            request_seq,
            tier: promotion.tier.clone(),
            ok: promotion.ok,
            error: error.clone(),
        }));
        if !promotion.ok {
            if let Some(error) = error {
                first_error.get_or_insert_with(|| error.clone());
                events.push(data_msg::<V>(CascadingCacheEvent::Error {
                    key: key.clone(),
                    request_seq,
                    stage: ReadThroughErrorStage::Promotion,
                    tier: Some(promotion.tier),
                    error,
                }));
            }
        }
    }
    events.push(data_msg(CascadingCacheEvent::Fill {
        key,
        request_seq,
        status: result.status,
        value: result.value,
        tier: result.hit_tier,
        error: first_error,
    }));
    events
}

fn events_from_error<V: Clone + 'static>(
    key: String,
    request_seq: u64,
    error: StorageError,
) -> Vec<Message<AnyValue>> {
    vec![
        data_msg::<V>(CascadingCacheEvent::Error {
            key: key.clone(),
            request_seq,
            stage: ReadThroughErrorStage::Lookup,
            tier: None,
            error: error.clone(),
        }),
        data_msg::<V>(CascadingCacheEvent::Fill {
            key,
            request_seq,
            status: TieredReadThroughStatus::Error,
            value: None,
            tier: None,
            error: Some(error),
        }),
    ]
}

fn data_msg<V: Clone + 'static>(event: CascadingCacheEvent<V>) -> Message<AnyValue> {
    Message::Data(Rc::new(event))
}

fn panic_payload(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_owned()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "reactive_cascading_cache read-through panicked".to_owned()
    }
}
