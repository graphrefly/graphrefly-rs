//! # GraphReFly — Rust clean-slate substrate (`@graphrefly/rust`)
//!
//! Reactive **universal reduction layer**: high fan-in/fan-out → information
//! reduction → push. Not LLM-limited; performance first-class (D1).
//!
//! ## Authority — the truth lives in `~/src/graphrefly` (branch `clean-slate`)
//!
//! This crate is the Rust **implementation**. The language-neutral authority —
//! protocol spec, decisions, formal model, conformance scenarios — is in the
//! `graphrefly` design repo. On any disagreement, **that repo wins**.
//!
//! | Concern | Source of truth |
//! |---|---|
//! | Decisions (D#) | `~/src/graphrefly/decisions/decisions.jsonl` |
//! | Protocol rules (宪法) | `~/src/graphrefly/spec/rules.jsonl` |
//! | Conformance (parity) | `~/src/graphrefly/spec/conformance.jsonl` |
//! | Formal model | `~/src/graphrefly/formal/*.tla` |
//! | Phase plan | `~/src/graphrefly/plan/phases.jsonl` (this crate = CSP-5) |
//!
//! Sibling self-contained packages: `@graphrefly/ts` (`~/src/graphrefly-ts`),
//! `@graphrefly/py` (`~/src/graphrefly-py`). Cross-language = a coarse wire
//! bridge, never in-process (D32, no cross-language peer-deps).
//!
//! ## Floor (cite, never violate)
//!
//! - **D22** — a graph is a single-thread causal/concurrency domain. This crate
//!   is therefore `!Send + !Sync`: state lives behind `Rc<RefCell<…>>`, **not**
//!   `Arc<Mutex<…>>`. The actor model is dropped. Parallelism = pool callback or
//!   multi-graph + wire bridge.
//! - **F-SYNC-CORE** — the wave-protocol core is synchronous; `dispatcher.invoke`
//!   is `fn(&Ctx)` returning `()`. Async lives only in pools (LocalAsync) and the
//!   wire bridge.
//! - **F-DISPATCH-ALL** — every node fn goes through the dispatcher; no inline-fn
//!   bypass.
//! - **D4** — 8-verb closed set (node/graph/batch/state + producer/derived/effect/
//!   mount). Operators are `node` sugar, per-language, never in parity (D6/D24).
//! - **D8** — the fn boundary is `ctx.up(msgs)` / `ctx.down(msgs)`; one `msgs`
//!   array = one wave. `ctx.up` is control-tier only (R-ctx-up).
//!
//! ## Clean-slate scope (what this crate builds)
//!
//! A self-contained Rust package (D32): protocol + node + dispatcher (LocalSync +
//! LocalAsync pools) + ctx + batch + rewire, plus the B53 graph-layer MVP
//! (Graph/graph, graph-owned 8-verb sugar, find/describe/observe first cut).
//! Operators remain per-language graph-layer sugar (D6/D24) and are re-derived
//! after this MVP rather than shimmed from the retired port model.
//!
//! > **Status:** [`protocol`], [`node`], [`dispatcher`], [`ctx`] are implemented
//! > (kernel + control/terminal + async + rewire + dep-terminal + `ctx.rewire_next`
//! > slices, plus pull/routed-up, terminal/later-async catch-up, and batch). [`graph`]
//! > adds the first product-completeness layer (B53). The Rust conformance arm is green
//! > for **C-2..C-22 except C-1**; **C-1** remains wire-bridge-blocked. See
//! > `CLEAN-SLATE.md` for the per-module status + conformance target map.

#![forbid(unsafe_code)]

pub mod adapters;
pub mod async_driver;
pub mod batch;
pub mod cascading_cache;
pub mod checkpoint;
pub mod combinators;
pub mod composition;
pub mod cqrs;
pub mod ctx;
pub mod data_structures;
pub mod diagnostics;
pub mod dispatcher;
pub mod environment;
pub mod graph;
pub mod higher_order;
pub mod json;
pub mod messaging;
pub mod node;
pub mod operators;
pub mod process;
pub mod protocol;
pub mod render;
pub mod resilience;
pub mod sources;
pub mod storage;
pub mod time;
mod versioning;
#[cfg(feature = "tokio-worker")]
pub mod worker;

pub use adapters::bridge::{
    remote_call, remote_call_with_options, remote_responder, remote_responder_handler, wire_bridge,
    wire_bridge_envelope, wire_bridge_idempotency_key, RemoteCallBundle, RemoteCallError,
    RemoteCallOptions, RemoteCallRequest, RemoteCallResponse, RemoteCallResult, RemoteCallStatus,
    RemoteCallStatusState, RemoteCallTimeout, RemoteResponderBundle, RemoteResponderEvent,
    RemoteResponderHandlerDefinition, RemoteResponderOptions, RemoteResponderStatus,
    RemoteResponderStatusState, WireBridgeAck, WireBridgeAttempt, WireBridgeBundle,
    WireBridgeCommand, WireBridgeEnvelope, WireBridgeEnvelopeError, WireBridgeEnvelopeInput,
    WireBridgeEnvelopeType, WireBridgeEvent, WireBridgeInbound, WireBridgeIngress,
    WireBridgeMetadata, WireBridgeNack, WireBridgeOptions, WireBridgePayload, WireBridgeReceipt,
    WireBridgeStatus, WireBridgeStatusState,
};
pub use adapters::environment::{
    to_http, to_http_with_options, to_process, to_process_with_options, to_websocket,
    to_websocket_with_options, OutboundAdapterOptions, OutboundBundle, OutboundEvent,
    OutboundState, OutboundStatus,
};
#[cfg(feature = "tokio")]
pub use async_driver::TokioLocalDriver;
pub use async_driver::{DriverCancel, LocalAsyncDriver};
pub use batch::{batch, BatchCtx};
pub use cascading_cache::{
    reactive_cascading_cache, CascadingCacheEvent, CascadingCachePolicy, CascadingCacheStatus,
    ReactiveCascadingCache, ReactiveCascadingCacheLoadFn, ReactiveCascadingCacheOptions,
};
pub use checkpoint::{
    default_restore_registry, restore_graph, restore_registry, GraphCheckpoint,
    GraphCheckpointCtxState, GraphCheckpointEdge, GraphCheckpointFactory, GraphCheckpointJson,
    GraphCheckpointLifecycle, GraphCheckpointMount, GraphCheckpointNode, GraphCheckpointTerminal,
    GraphCheckpointValue, GraphRestoreDefinition, GraphRestoreEntry, GraphRestoreError,
    GraphRestoreRegistry, GraphRestoreResult, MapJsonRestoreDescriptor, RestoreGraphOptions,
    RestoreNodeDefinition, RestoreNodeKind, StateRestoreDescriptor, GRAPH_CHECKPOINT_VERSION,
};
pub use combinators::{
    buffer, buffer_count, combine, combine_latest, concat, race, sample, take_until,
    with_latest_from, zip,
};
pub use composition::{
    pipe, stratify, stratify_branch, Pipe, Stratified, StratifyOptions, StratifyRule,
};
pub use cqrs::{
    cqrs, cqrs_command_handler, cqrs_projection, cqrs_with_options, CqrsAuditOutcome,
    CqrsAuditRecord, CqrsBundle, CqrsCommand, CqrsCommandHandlerDefinition, CqrsCursor,
    CqrsDedupePolicy, CqrsDedupeSnapshot, CqrsDedupeWindow, CqrsError, CqrsErrorCode, CqrsEvent,
    CqrsEventDraft, CqrsOptions, CqrsProjection, CqrsProjectionError, CqrsProjectionErrorCode,
    CqrsProjectionFrame, CqrsProjectionOptions, CqrsProjectionReducer, CqrsProjectionStatus,
    CqrsProjectionStatusState, CqrsRuntimeFact, CqrsStatus, CqrsStatusState,
};
pub use ctx::{Ctx, DeferredCtx, DepTerminal, WaveData};
pub use data_structures::{
    merge_reactive_logs, reactive_index, reactive_list, reactive_log, reactive_map, scan_log,
    IndexChange, IndexRow, ListChange, LogChange, MapChange, ReactiveIndex, ReactiveIndexOptions,
    ReactiveList, ReactiveListOptions, ReactiveLog, ReactiveLogOptions, ReactiveMap,
    ReactiveMapOptions, ReactiveView,
};
pub use diagnostics::{
    explain_path, reachable, topology_diff, validate_no_islands, CausalChain, CausalStep,
    DescribeChangeset, DescribeEvent, ExplainPathOptions, ExplainPathReason, IslandReport,
    ReachableDirection, ReachableOptions, ReachableResult, ValidateNoIslandsResult,
};
pub use dispatcher::{default_dispatcher, Dispatcher, PoolKind};
#[cfg(feature = "tokio-http")]
pub use environment::TokioHttpDriver;
#[cfg(feature = "tokio")]
pub use environment::TokioProcessDriver;
#[cfg(feature = "tokio-websocket")]
pub use environment::TokioWebSocketDriver;
pub use environment::{
    EnvironmentDrivers, HttpRequest, HttpResponse, LocalHttpDriver, LocalProcessDriver,
    LocalSseDriver, LocalWebSocketDriver, LocalWebhookDriver, ProcessCommand, ProcessResult,
    SseDriverEvent, SseEvent, SseRequest, WebSocketDriverEvent, WebSocketEvent, WebSocketRequest,
    WebSocketSend, WebSocketSendResult, WebhookDriverEvent, WebhookEvent, WebhookRegistration,
};
pub use graph::{
    graph, graph_opts, DescribeEdge, DescribeNode, DescribeOpts, DescribeSnapshot, DescribeValue,
    Explain, Graph, GraphNode, GraphNodeOpts, GraphObserver, GraphOptions, GraphTopologyObserver,
    NodeProfile, ObserveEvent, ObserveMessage, ObserveStream, Profile, RestoreFactoryMeta,
    TopologyEvent, TopologyEventKind, TopologyGroup, TopologyGroupOptions, TopologyStream, Values,
};
pub use higher_order::{
    concat_map, exhaust_map, flat_map, merge_map, merge_map_with_options, repeat, switch_map,
    MergeMapOptions,
};
pub use json::{
    assert_decimal_integer_string, assert_non_negative_decimal_integer_string,
    decimal_string_to_i128, i128_to_decimal_string, is_decimal_integer_string,
    is_non_negative_decimal_integer_string, json_codec_for, non_negative_decimal_string_to_u128,
    stable_json_string, strict_canonical_json_bytes, strict_json_codec_for, strict_json_decode,
    u128_to_non_negative_decimal_string, Codec, DecimalIntegerString, JsonCodec, JsonCodecError,
    JsonCodecResult, JsonValue, NonNegativeDecimalIntegerString, StrictJsonCodec,
};
pub use messaging::{
    dynamic_hub, dynamic_hub_with_options, from_hub_topic, from_hub_topic_with_name, from_topic,
    message_bus, to_hub_topic, to_topic, topic_core, DynamicHub, DynamicHubCommand,
    DynamicHubDeadLetter, DynamicHubError, DynamicHubEvent, DynamicHubEventKind,
    DynamicHubMetadata, DynamicHubOptions, DynamicHubStatus, DynamicHubUnknownTopicPolicy,
    MessageBus, MessageBusEvent, MessageEnvelope, ToHubTopicBundle,
};
pub use node::{Core, Node, NodeOpts, Pausable, Status};
pub use operators::{
    catch_error, distinct_until_changed, element_at, filter, find, first, first_any, init_node,
    last, last_any, map, merge, on_first_data, on_first_data_where, pairwise, reduce, rescue, scan,
    settle, settle_by, skip, take, take_while, tap, tap_first, valve, Operator,
};
pub use process::{
    process_bundle, process_effect_runner, ProcessAuditOutcome, ProcessAuditRecord, ProcessBundle,
    ProcessBundleOptions, ProcessCursor, ProcessEffectCommandPayload, ProcessEffectCommandType,
    ProcessEffectOutcome, ProcessEffectOutcomeKind, ProcessEffectRequest,
    ProcessEffectRequestDraft, ProcessEffectRunnerBundle, ProcessEffectRunnerError,
    ProcessEffectRunnerErrorCode, ProcessEffectRunnerOptions, ProcessEffectRunnerStatus,
    ProcessEffectRunnerStatusState, ProcessError, ProcessErrorCode, ProcessEvent,
    ProcessEventDraft, ProcessReducer, ProcessReducerFn, ProcessReduction, ProcessRuntimeFact,
    ProcessStatus, ProcessStatusState,
};
pub use protocol::{AnyValue, GraphError, Handle, LockId, Message, Tier, Wave};
pub use render::{
    describe_to_ascii, describe_to_d2, describe_to_d2_with_direction, describe_to_json,
    describe_to_mermaid, describe_to_mermaid_url, describe_to_mermaid_with_direction,
    describe_to_pretty, mermaid_live_url, DiagramDirection,
};
pub use resilience::{
    breaker_status_node, rate_limit_bundle, retry_status_node, timeout_bundle, BackoffPolicy,
    BreakerState, BreakerStatus, RateLimitBundle, RateLimitStatus, RetryEvent, RetryPolicy,
    RetryState, RetryStatus, TimeoutBundle, TimeoutStatus,
};
pub use sources::{
    empty, from_cron, from_cron_with_options, from_fs_watch, from_fs_watch_with_options,
    from_git_hook, from_git_hook_with_options, from_http, from_http_with_options, from_iter,
    from_process, from_sse, from_sse_with_options, from_timer, from_webhook,
    from_webhook_with_options, from_websocket, from_websocket_with_options, future_local, interval,
    matches_cron, never, of, parse_cron, run_process, run_process_with_options, stream_local,
    throw_error, timer, CronInstant, CronParseError, CronSchedule, CronTick, FromCronOptions,
    FromFsWatchOptions, FromGitHookOptions, FsEvent, FsEventKind, GitEvent, GitHookType,
};
pub use storage::{
    append_log_key, append_log_storage, assert_wal_frame, change_envelope_codec, codec_kv_storage,
    content_addressed_kv, content_addressed_storage, dict_kv, envelope_change, file_append_log,
    file_backend, file_kv, memory_append_log, memory_kv, memory_multi_writer_append_log,
    multi_writer_append_log_storage, now_ns, observe_event_frame, observe_event_frame_codec,
    read_append_log_page, read_observe_event_log_page, read_through_kv, tiered_read_through,
    verify_wal_frame_checksum, wal_frame, wal_frame_checksum, wal_frame_codec, wal_frame_key,
    wal_frame_prefix, AppendLogEntry, AppendLogPage, AppendLogReadOptions, AppendLogStorage,
    AppendLogStorageTier, ByteStorageBackend, ChangeEnvelope, ChangeEnvelopeCodec,
    ChangeEnvelopeOptions, ChangeLifecycle, CodecKvStorage, ContentAddressedKeyContext,
    ContentAddressedKv, ContentAddressedKvOptions, ContentAddressedMode, ContentAddressedStorage,
    ContentAddressedStorageOptions, FileAppendLogOptions, FileBackend, FileBackendOptions, FileKv,
    KvGeneration, KvStorageTier, KvVersionedRead, MemoryKv, MultiWriterAppendLogStorage,
    ObserveEventFrame, ObserveEventFrameCodec, ObserveEventFrameOptions, ObserveEventLogPage,
    PromotionPolicy, ReadThroughErrorContext, ReadThroughErrorFn, ReadThroughErrorStage,
    ReadThroughLoadFn, ReadThroughLookupFact, ReadThroughLookupTier, ReadThroughMissContext,
    ReadThroughMissFn, ReadThroughOutcome, ReadThroughPromotionFact, StorageError, StorageResult,
    TieredReadThroughOptions, TieredReadThroughResult, TieredReadThroughStatus, WalFrame,
    WalFrameBody, WalFrameCodec, WalFrameOptions, WalFrameTimestampNs, APPEND_LOG_SEQ_PAD,
    WAL_FORMAT_VERSION, WAL_FRAME_SEQ_PAD, WAL_KEY_SEGMENT,
};
pub use time::{
    audit, audit_time, buffer_time, debounce, debounce_time, delay, throttle, throttle_time,
    timeout,
};
pub use versioning::{
    default_node_version_hash, NodeVersion, NodeVersionHashFn, NodeVersioningPolicy,
    ResolvedNodeVersioningPolicy,
};
#[cfg(feature = "tokio-worker")]
pub use worker::worker_derived;
