use graphrefly::{
    agentic_memory_bundle, agentic_memory_consolidation_bundle,
    agentic_memory_context_packing_bundle, agentic_memory_kg_projection_bundle,
    agentic_memory_record_frame, agentic_memory_record_frame_codec,
    agentic_memory_records_snapshot_key, agentic_memory_retention_bundle, first_sync_value_from,
    from_iter, from_sse_with_options, graph, graph_opts, knowledge_graph_reducer_bundle, map,
    memory_append_log, memory_kv, message_bus, persist_agentic_memory_records, pipe, reactive_map,
    retry_status_node, scheduled_readiness_projector, single_sync_value_from, timeout_bundle,
    to_http_with_options, to_process_with_options, topology_diff, validate_topic_message_payload,
    work_queue, work_queue_readiness_handoff_projector, work_queue_scheduled_readiness_projector,
    AgenticMemoryArtifactKind, AgenticMemoryBundleOptions, AgenticMemoryConsolidationBundleOptions,
    AgenticMemoryConsolidationOutcome, AgenticMemoryContextPackingBundleOptions,
    AgenticMemoryContextPackingPolicy, AgenticMemoryKgAssertionDraft,
    AgenticMemoryKgProjectionBundleOptions, AgenticMemoryKind, AgenticMemoryPersistenceLevel,
    AgenticMemoryRecord, AgenticMemoryRetentionBundleOptions, AgenticMemoryRetentionCommand,
    AgenticMemoryRetentionCommandKind, AgenticMemoryScope, AgenticMemoryStatusState,
    AgenticMemoryTextProjection, AppendLogReadOptions, AppendLogStorageTier, BackoffPolicy, Codec,
    CqrsCommand, CqrsEventDraft, CqrsOptions, CqrsStatusState, DescribeEvent, DriverCancel,
    EnvironmentDrivers, GraphNodeOpts, GraphOptions, HttpRequest, HttpResponse,
    HttpStreamDriverEvent, HttpStreamHead, JsonSchema, JsonSchemaAdditionalProperties,
    JsonSchemaType, JsonSchemaTypeSpec, KnowledgeAssertionObject, KnowledgeGraphPolicy,
    KnowledgeGraphReducerBundleOptions, KnowledgeGraphStatusState, KvStorageTier, LocalAsyncDriver,
    LocalHttpDriver, LocalHttpStreamDriver, LocalProcessDriver, MemoryFragment,
    MemoryRetrievalQuery, Message, MessageBusOptions, MessageBusStatusKind, OutboundAdapterOptions,
    OutboundEvent, OutboundState, OutboundStatus, PersistAgenticMemoryRecordsOptions,
    ProcessCommand, ProcessResult, PullDemand, ReactiveMapOptions, RetryEvent, RetryPolicy,
    RetryState, ScheduledReadinessClock, ScheduledReadinessOptions, SseEvent, SseRequest,
    SyncValueFromError, TimeoutStatus, TopicMessage, WebSocketRequest, WebSocketSend,
    WorkQueueClaimOptions, WorkQueueOptions, WorkQueueReadinessCandidateKind,
    WorkQueueReadinessHandoffOptions, WorkQueueRecord, WorkQueueScheduledReadinessOptions,
    WorkQueueSubmit, WorkQueueSubmitOptions, PROMPTS_TOPIC, STANDARD_TOPICS,
};
use serde_json::{json, Value};
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::rc::Rc;
use std::time::Duration;

fn collect_data<T: Clone + 'static>(node: &graphrefly::Node<T>) -> Rc<RefCell<Vec<T>>> {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let sink = seen.clone();
    let _keep = node.subscribe(move |msg| {
        if let Message::Data(value) = msg {
            if let Some(value) = value.as_ref().downcast_ref::<T>() {
                sink.borrow_mut().push(value.clone());
            }
        }
    });
    seen
}

#[derive(Default)]
struct D566HttpDriver {
    attempts: Cell<u32>,
    requests: RefCell<Vec<HttpRequest>>,
}

impl LocalHttpDriver for D566HttpDriver {
    fn request(
        &self,
        request: HttpRequest,
        callback: Box<dyn FnOnce(Result<HttpResponse, graphrefly::GraphError>)>,
    ) -> DriverCancel {
        let attempt = self.attempts.get().saturating_add(1);
        self.attempts.set(attempt);
        self.requests.borrow_mut().push(request);
        if attempt == 1 {
            callback(Err("temporary http outage".into()));
        } else {
            callback(Ok(HttpResponse {
                status: 202,
                headers: Vec::new(),
                body: b"accepted".to_vec(),
            }));
        }
        Box::new(|| {})
    }
}

type B72HttpStreamCallback = Rc<dyn Fn(HttpStreamDriverEvent)>;
type B72PendingHttpStream = (HttpRequest, Rc<Cell<bool>>, B72HttpStreamCallback);

#[derive(Default)]
struct B72HttpStreamDriver {
    streams: RefCell<Vec<B72PendingHttpStream>>,
}

impl B72HttpStreamDriver {
    fn request(&self) -> HttpRequest {
        self.streams.borrow()[0].0.clone()
    }

    fn emit(&self, event: HttpStreamDriverEvent) {
        let streams = self.streams.borrow();
        let (_, active, callback) = &streams[0];
        if active.get() {
            callback(event);
        }
    }
}

impl LocalHttpStreamDriver for B72HttpStreamDriver {
    fn stream(&self, request: HttpRequest, callback: B72HttpStreamCallback) -> DriverCancel {
        let active = Rc::new(Cell::new(true));
        self.streams
            .borrow_mut()
            .push((request, active.clone(), callback));
        Box::new(move || active.set(false))
    }
}

#[derive(Default)]
struct D566ProcessDriver {
    commands: RefCell<Vec<ProcessCommand>>,
}

impl LocalProcessDriver for D566ProcessDriver {
    fn run(
        &self,
        command: ProcessCommand,
        callback: Box<dyn FnOnce(Result<ProcessResult, graphrefly::GraphError>)>,
    ) -> DriverCancel {
        self.commands.borrow_mut().push(command);
        callback(Ok(ProcessResult {
            stdout: "process accepted".to_owned(),
            stderr: String::new(),
            exit_code: Some(0),
            signal: None,
        }));
        Box::new(|| {})
    }
}

type D566Sleep = (Rc<Cell<bool>>, Box<dyn FnOnce()>);

#[derive(Default)]
struct D566AsyncDriver {
    sleeps: RefCell<Vec<D566Sleep>>,
}

impl D566AsyncDriver {
    fn fire_sleepers(&self) {
        for (active, callback) in self.sleeps.borrow_mut().drain(..) {
            if active.get() {
                callback();
            }
        }
    }
}

impl LocalAsyncDriver for D566AsyncDriver {
    fn sleep(&self, _duration: Duration, callback: Box<dyn FnOnce()>) -> DriverCancel {
        let active = Rc::new(Cell::new(true));
        self.sleeps.borrow_mut().push((active.clone(), callback));
        Box::new(move || active.set(false))
    }

    fn interval(&self, _period: Duration, _callback: Rc<dyn Fn()>) -> DriverCancel {
        Box::new(|| {})
    }

    fn spawn_local(&self, _fut: Pin<Box<dyn Future<Output = ()> + 'static>>) -> DriverCancel {
        Box::new(|| {})
    }
}

fn record(id: &str, text: &str) -> AgenticMemoryRecord<Value> {
    AgenticMemoryRecord {
        id: format!("record-{id}"),
        fragment: MemoryFragment {
            id: id.to_owned(),
            payload: json!({ "text": text }),
            t_ns: 1,
            valid_from: None,
            valid_to: None,
            confidence: 1.0,
            tags: vec!["project".to_owned(), "acceptance".to_owned()],
            sources: vec!["acceptance-test".to_owned()],
            embedding: None,
            parent_fragment_id: None,
            provenance: None,
        },
        kind: AgenticMemoryKind::Semantic,
        persistence_level: AgenticMemoryPersistenceLevel::Project,
        artifact_kind: AgenticMemoryArtifactKind::Insight,
        scope: Some(AgenticMemoryScope {
            session_id: Some("session-1".to_owned()),
            project_id: Some("project-1".to_owned()),
            user_id: None,
            tenant_id: None,
        }),
    }
}

#[test]
fn public_crate_root_b74_messaging_vocabulary_is_passive() {
    let mut properties = BTreeMap::new();
    properties.insert(
        "text".to_owned(),
        JsonSchema {
            schema_type: Some(JsonSchemaTypeSpec::Single(JsonSchemaType::String)),
            ..JsonSchema::default()
        },
    );
    let schema = JsonSchema {
        schema_type: Some(JsonSchemaTypeSpec::Single(JsonSchemaType::Object)),
        properties: Some(properties),
        required: Some(vec!["text".to_owned()]),
        additional_properties: Some(JsonSchemaAdditionalProperties::Bool(false)),
        ..JsonSchema::default()
    };
    let valid = TopicMessage {
        id: "msg-valid".to_owned(),
        schema: Some(schema.clone()),
        expires_at: Some("2030-01-01T00:00:00Z".to_owned()),
        correlation_id: Some("corr-1".to_owned()),
        payload: json!({ "text": "schema checks are explicit" }),
    };
    let invalid = TopicMessage {
        id: "msg-invalid".to_owned(),
        schema: Some(schema),
        expires_at: Some("2030-01-01T00:00:00Z".to_owned()),
        correlation_id: Some("corr-2".to_owned()),
        payload: json!({ "text": 7, "extra": true }),
    };
    let expired = TopicMessage {
        id: "msg-expired".to_owned(),
        schema: valid.schema.clone(),
        expires_at: Some("2000-01-01T00:00:00Z".to_owned()),
        correlation_id: Some("corr-3".to_owned()),
        payload: json!({ "text": "expiration is an application fact" }),
    };

    assert!(
        STANDARD_TOPICS.contains(&PROMPTS_TOPIC),
        "D159 standard topic vocabulary should be available from the crate root"
    );
    validate_topic_message_payload(&valid).unwrap();
    assert!(
        validate_topic_message_payload(&invalid).is_err(),
        "D159 validation is caller-invoked, not a bus/hub side effect"
    );
    validate_topic_message_payload(&expired).unwrap();

    let g = graph();
    let bus = message_bus::<TopicMessage<Value>>(
        &g,
        MessageBusOptions::named("acceptance/b74Bus").with_topics([PROMPTS_TOPIC]),
    );
    let messages = collect_data(&bus.messages);
    let statuses = collect_data(&bus.status);
    let issues = collect_data(&bus.issues);
    bus.publish(
        PROMPTS_TOPIC,
        invalid.clone(),
        None,
        Some("publish-invalid-schema".to_owned()),
        None,
    );
    bus.publish(
        PROMPTS_TOPIC,
        expired.clone(),
        None,
        Some("publish-expired-application-fact".to_owned()),
        None,
    );

    assert_eq!(
        messages
            .borrow()
            .iter()
            .map(|message| message.payload.id.as_str())
            .collect::<Vec<_>>(),
        vec!["msg-invalid", "msg-expired"],
        "messageBus must carry TopicMessage payload facts without auto-validation"
    );
    assert!(statuses.borrow().iter().any(|status| {
        status.kind == MessageBusStatusKind::MessagePublished
            && status.topic.as_deref() == Some(PROMPTS_TOPIC)
    }));
    assert_eq!(
        issues.borrow().len(),
        0,
        "schema metadata must not create a registry, topic auth, expiration, or validation hub"
    );
}

#[test]
fn public_crate_root_agentic_memory_csp10_acceptance() {
    let g = graph();
    let records = g.state_opts(
        vec![
            record(
                "near",
                "Durable project memory belongs in the context window.",
            ),
            record("archive", "Completed scratch note ready for retention."),
        ],
        GraphNodeOpts::named("records"),
    );

    let snapshots = Rc::new(memory_kv::<Value>());
    let changes = Rc::new(memory_append_log::<Value>("acceptance-agentic/changes"));
    let persistence = persist_agentic_memory_records(
        &records,
        PersistAgenticMemoryRecordsOptions {
            graph: Some(g.clone()),
            name: Some("recordsPersistence".to_owned()),
            storage_prefix: Some("acceptance-agentic".to_owned()),
            snapshot_key: None,
            snapshot_store: snapshots.clone(),
            change_log: Some(changes.clone()),
            snapshot_on_attach: true,
        },
    )
    .unwrap();
    let _persistence_ready = persistence.ready.subscribe(|_| {});
    let _persistence_status = persistence.status.subscribe(|_| {});

    assert!(snapshots
        .get(&agentic_memory_records_snapshot_key("acceptance-agentic").unwrap())
        .unwrap()
        .is_some());
    let frame = agentic_memory_record_frame(record(
        "frame",
        "Record frames should be available from the public crate root.",
    ));
    let codec = agentic_memory_record_frame_codec();
    assert_eq!(codec.decode(&codec.encode(&frame).unwrap()).unwrap(), frame);

    let query = g.state_opts(
        MemoryRetrievalQuery {
            tags: vec!["project".to_owned()],
            limit: Some(4),
            ..Default::default()
        },
        GraphNodeOpts::named("query"),
    );
    let memory = agentic_memory_bundle(
        &g,
        AgenticMemoryBundleOptions::new(records.clone(), query).named("memory"),
    );

    let kg_drafts = g.state_opts(
        vec![AgenticMemoryKgAssertionDraft {
            id: "assertion-near".to_owned(),
            record_id: Some("record-near".to_owned()),
            fragment_id: Some("near".to_owned()),
            subject_id: "project:graphrefly".to_owned(),
            predicate: "uses".to_owned(),
            object: KnowledgeAssertionObject::Entity {
                entity_id: "capability:agentic-memory".to_owned(),
            },
            confidence: 0.95,
            t_ns: 2,
        }],
        GraphNodeOpts::named("kgDrafts"),
    );
    let kg = agentic_memory_kg_projection_bundle(
        &g,
        AgenticMemoryKgProjectionBundleOptions::new(records.clone(), kg_drafts).named("kg"),
    );
    let kg_policy = g.state_opts(
        KnowledgeGraphPolicy {
            allowed_predicates: vec!["uses".to_owned()],
        },
        GraphNodeOpts::named("kgPolicy"),
    );
    let graph_projection = knowledge_graph_reducer_bundle(
        &g,
        KnowledgeGraphReducerBundleOptions::new(kg.assertions.clone())
            .with_policy(kg_policy)
            .named("knowledgeGraph"),
    );

    let retention_commands = g.state_opts(
        vec![
            AgenticMemoryRetentionCommand {
                id: "archive-done-note".to_owned(),
                record_id: "record-archive".to_owned(),
                kind: AgenticMemoryRetentionCommandKind::Archive,
                reason: Some("done".to_owned()),
            },
            AgenticMemoryRetentionCommand {
                id: "consolidate-near".to_owned(),
                record_id: "record-near".to_owned(),
                kind: AgenticMemoryRetentionCommandKind::RequestConsolidation,
                reason: Some("promote durable insight".to_owned()),
            },
        ],
        GraphNodeOpts::named("retentionCommands"),
    );
    let retention = agentic_memory_retention_bundle(
        &g,
        AgenticMemoryRetentionBundleOptions::new(records.clone(), retention_commands)
            .named("retention"),
    );
    let consolidation_outcomes = g.state_opts(
        vec![AgenticMemoryConsolidationOutcome::ProposedRecords {
            id: "outcome-near".to_owned(),
            request_id: "consolidate-near".to_owned(),
            records: vec![record(
                "durable",
                "Keep project memory close to graph context.",
            )],
            provenance: Some("acceptance".to_owned()),
        }],
        GraphNodeOpts::named("consolidationOutcomes"),
    );
    let consolidation = agentic_memory_consolidation_bundle(
        &g,
        AgenticMemoryConsolidationBundleOptions::new(
            retention.consolidation_requests.clone(),
            consolidation_outcomes,
        )
        .named("consolidation"),
    );

    let texts = g.state_opts(
        vec![
            AgenticMemoryTextProjection {
                fragment_id: "near".to_owned(),
                text: "Durable project memory belongs in the context window.".to_owned(),
            },
            AgenticMemoryTextProjection {
                fragment_id: "archive".to_owned(),
                text: "Completed scratch note ready for retention.".to_owned(),
            },
        ],
        GraphNodeOpts::named("textProjections"),
    );
    let policy = g.state_opts(
        AgenticMemoryContextPackingPolicy {
            max_chars: Some(256),
            separator: "\n".to_owned(),
            include_fragment_ids: true,
        },
        GraphNodeOpts::named("packingPolicy"),
    );
    let packing = agentic_memory_context_packing_bundle(
        &g,
        AgenticMemoryContextPackingBundleOptions::new(memory.context.clone(), texts, policy)
            .named("packing"),
    );

    let _context = memory.context.subscribe(|_| {});
    let _memory_status = memory.status.subscribe(|_| {});
    let _kg_assertions = kg.assertions.subscribe(|_| {});
    let _kg_status = kg.status.subscribe(|_| {});
    let _graph_entities = graph_projection.entities.subscribe(|_| {});
    let _graph_status = graph_projection.status.subscribe(|_| {});
    let _retention_archived = retention.archived_records.subscribe(|_| {});
    let _retention_requests = retention.consolidation_requests.subscribe(|_| {});
    let _retention_status = retention.status.subscribe(|_| {});
    let _consolidation_drafts = consolidation.proposed_record_drafts.subscribe(|_| {});
    let _consolidation_commands = consolidation.commands.subscribe(|_| {});
    let _packing_context = packing.packed_context.subscribe(|_| {});
    let _packing_status = packing.status.subscribe(|_| {});

    assert_eq!(
        memory.context.cache().unwrap().entries.len(),
        2,
        "agentic retrieval should feed context from public crate-root APIs"
    );
    assert_eq!(
        memory.status.cache().unwrap().state,
        AgenticMemoryStatusState::Ready
    );
    assert_eq!(kg.assertions.cache().unwrap().len(), 1);
    assert_eq!(
        graph_projection.status.cache().unwrap().state,
        KnowledgeGraphStatusState::Ready
    );
    assert_eq!(
        graph_projection
            .entities
            .cache()
            .unwrap()
            .iter()
            .map(|entity| entity.id.as_str())
            .collect::<Vec<_>>(),
        vec!["capability:agentic-memory", "project:graphrefly"]
    );
    assert_eq!(
        retention.archived_records.cache().unwrap()[0].id,
        "record-archive"
    );
    assert_eq!(
        retention.consolidation_requests.cache().unwrap()[0].command_id,
        "consolidate-near"
    );
    assert_eq!(
        consolidation.proposed_record_drafts.cache().unwrap()[0]
            .record
            .id,
        "record-durable"
    );
    assert!(packing
        .packed_context
        .cache()
        .unwrap()
        .text
        .contains("[near] Durable project memory"));

    let described = g.describe();
    assert!(described
        .edges
        .iter()
        .any(|edge| edge.from == "records" && edge.to == "memory/projection"));
    assert!(described
        .edges
        .iter()
        .any(|edge| edge.from == "kg/assertions" && edge.to == "knowledgeGraph/snapshot"));
    assert!(described
        .edges
        .iter()
        .any(|edge| edge.from == "memory/context" && edge.to == "packing/snapshot"));

    records.set(vec![record(
        "near-updated",
        "Updated durable project memory should append a sidecar change frame.",
    )]);
    assert_eq!(
        changes.read(AppendLogReadOptions::default()).unwrap().len(),
        1
    );
    persistence.flush().unwrap();
    persistence.dispose();
}

#[test]
fn public_crate_root_b72_sources_io_and_resilience_acceptance() {
    let http_stream_driver = Rc::new(B72HttpStreamDriver::default());
    let g = graph_opts(GraphOptions {
        name: Some("acceptance/b72".to_owned()),
        profile: true,
        environment: EnvironmentDrivers::new().with_http_stream(http_stream_driver.clone()),
        ..GraphOptions::default()
    });

    let first = g.init_node(
        from_iter(vec![10_i32, 20]),
        vec![],
        GraphNodeOpts::named("acceptance/b72/first"),
    );
    assert_eq!(first_sync_value_from(&first), Ok(10));
    let single = g.init_node(
        from_iter(vec!["only".to_owned()]),
        vec![],
        GraphNodeOpts::named("acceptance/b72/single"),
    );
    assert_eq!(single_sync_value_from(&single), Ok("only".to_owned()));
    let live = g.state_opts(7_i32, GraphNodeOpts::named("acceptance/b72/live"));
    assert_eq!(
        single_sync_value_from(&live),
        Err(SyncValueFromError::Pending),
        "host-boundary source helper must not wait, poll, or install a runtime"
    );

    let sse = g.init_node(
        from_sse_with_options(
            SseRequest::new("https://example.test/events").header("x-source", "b72"),
        ),
        vec![],
        GraphNodeOpts::named("acceptance/b72/sse"),
    );
    let events = Rc::new(RefCell::new(Vec::<SseEvent>::new()));
    let completed = Rc::new(Cell::new(false));
    let events_sink = events.clone();
    let completed_sink = completed.clone();
    let unsubscribe_sse = sse.subscribe(move |msg| match msg {
        Message::Data(value) => {
            if let Some(event) = value.as_ref().downcast_ref::<SseEvent>() {
                events_sink.borrow_mut().push(event.clone());
            }
        }
        Message::Complete => completed_sink.set(true),
        _ => {}
    });

    assert_eq!(http_stream_driver.request().method, "GET");
    assert_eq!(
        http_stream_driver.request().url,
        "https://example.test/events"
    );
    assert!(http_stream_driver
        .request()
        .headers
        .iter()
        .any(|(key, value)| key == "x-source" && value == "b72"));
    assert!(http_stream_driver
        .request()
        .headers
        .iter()
        .any(|(key, value)| key.eq_ignore_ascii_case("accept") && value == "text/event-stream"));

    http_stream_driver.emit(HttpStreamDriverEvent::Head(HttpStreamHead {
        status: 200,
        headers: vec![(
            "content-type".to_owned(),
            "text/event-stream; charset=utf-8".to_owned(),
        )],
    }));
    http_stream_driver.emit(HttpStreamDriverEvent::Chunk(
        b"id: evt-1\nevent: patch\ndata: hello".to_vec(),
    ));
    http_stream_driver.emit(HttpStreamDriverEvent::Chunk(
        b"\ndata: world\nretry: 50\n\n".to_vec(),
    ));
    http_stream_driver.emit(HttpStreamDriverEvent::Complete);

    assert_eq!(
        events.borrow().as_slice(),
        &[SseEvent {
            event: Some("patch".to_owned()),
            data: "hello\nworld".to_owned(),
            id: Some("evt-1".to_owned()),
            retry_ms: Some(50),
        }]
    );
    assert!(completed.get());
    assert!(g
        .describe()
        .nodes
        .iter()
        .any(|node| { node.id == "acceptance/b72/sse" && node.factory == "fromSSE" }));
    assert!(g.profile().nodes.contains_key("acceptance/b72/sse"));

    unsubscribe_sse();
}

#[test]
fn public_crate_root_b70_b71_composition_and_reactive_view_acceptance() {
    let g = graph_opts(GraphOptions {
        name: Some("acceptance/b70-b71".to_owned()),
        profile: true,
        ..GraphOptions::default()
    });
    let source = g.state_opts(1_i32, GraphNodeOpts::named("acceptance/source"));
    let before_pipe = g.describe();

    let doubled = pipe(&g, source.clone())
        .through(map::<i32, i32>(|value| value * 2))
        .done();
    let doubled_values = Rc::new(RefCell::new(Vec::new()));
    let doubled_sink = doubled_values.clone();
    let unsubscribe_doubled = doubled.subscribe(move |msg| {
        if let Message::Data(value) = msg {
            if let Some(value) = value.as_ref().downcast_ref::<i32>() {
                doubled_sink.borrow_mut().push(*value);
            }
        }
    });
    source.set(2);

    assert_eq!(doubled_values.borrow().as_slice(), &[2, 4]);
    let after_pipe = g.describe();
    let pipe_diff = topology_diff(&before_pipe, &after_pipe);
    assert!(pipe_diff.events.iter().any(|event| matches!(
        event,
        DescribeEvent::NodeAdded { id, node }
            if id == "map#0" && node.factory == "map"
    )));
    assert!(pipe_diff.events.iter().any(|event| matches!(
        event,
        DescribeEvent::EdgeAdded { from, to }
            if from == "acceptance/source" && to == "map#0"
    )));
    unsubscribe_doubled();

    let map_store = reactive_map::<String, i32>(
        vec![("a".to_owned(), 1), ("b".to_owned(), 2)],
        ReactiveMapOptions::named("acceptance/items").graph(g.clone()),
    );
    let selected = map_store.select(|value, _key| *value >= 2);
    let selected_snapshots = Rc::new(RefCell::new(Vec::new()));
    let selected_snapshot_sink = selected_snapshots.clone();
    let unsubscribe_selected_snapshot = selected.snapshot.subscribe(move |msg| {
        if let Message::Data(value) = msg {
            if let Some(snapshot) = value
                .as_ref()
                .downcast_ref::<std::collections::BTreeMap<String, i32>>()
            {
                selected_snapshot_sink.borrow_mut().push(snapshot.clone());
            }
        }
    });

    map_store.set("c".to_owned(), 3);
    selected.snapshot.up(vec![Message::Pull(PullDemand::new(
        selected.pull_id.clone(),
    ))]);

    assert_eq!(
        selected_snapshots.borrow().last().cloned().unwrap(),
        [("b".to_owned(), 2), ("c".to_owned(), 3)]
            .into_iter()
            .collect()
    );
    let with_view = g.describe();
    assert!(with_view.nodes.iter().any(|node| {
        node.id == "acceptance/items.select#0.delta" && node.factory == "reactiveMap.select.delta"
    }));
    assert!(with_view.edges.iter().any(|edge| {
        edge.from == "acceptance/items.delta" && edge.to == "acceptance/items.select#0.delta"
    }));
    assert!(g
        .profile()
        .nodes
        .contains_key("acceptance/items.select#0.delta"));
    assert!(g
        .checkpoint()
        .unwrap()
        .nodes
        .iter()
        .any(|node| node.id == "acceptance/items.select#0.snapshot"));

    unsubscribe_selected_snapshot();
    selected.dispose();
    let after_dispose = g.describe();
    let dispose_diff = topology_diff(&with_view, &after_dispose);
    assert!(dispose_diff.events.iter().any(|event| matches!(
        event,
        DescribeEvent::NodeRemoved { id } if id == "acceptance/items.select#0.delta"
    )));
    assert!(dispose_diff.events.iter().any(|event| matches!(
        event,
        DescribeEvent::NodeRemoved { id } if id == "acceptance/items.select#0.snapshot"
    )));
    assert!(g.find("acceptance/items.select#0.delta").is_none());
}

#[test]
fn public_crate_root_d566_app_infra_acceptance() {
    let g = graph();
    let now = Rc::new(Cell::new(0_u64));
    let bus = message_bus::<WorkQueueSubmit<String>>(
        &g,
        MessageBusOptions::named("acceptance/d566Bus")
            .with_topics(["work"])
            .with_now({
                let now = now.clone();
                move || now.get()
            }),
    );
    let queue = work_queue(
        &g,
        WorkQueueOptions::new("q", bus, "work", "q-admit")
            .with_now({
                let now = now.clone();
                move || now.get()
            })
            .with_retry(RetryPolicy::new(
                3,
                BackoffPolicy::Constant { delay_ms: 10 },
            )),
    );
    let schedules = work_queue_scheduled_readiness_projector(
        &g,
        WorkQueueScheduledReadinessOptions::new(vec![queue.records.clone()])
            .named("acceptance/d566Schedules"),
    );
    let clock = g.state_empty::<ScheduledReadinessClock>();
    let readiness = scheduled_readiness_projector(
        &g,
        ScheduledReadinessOptions::new(vec![schedules.readiness_schedules.clone()])
            .with_clocks(vec![clock.clone()])
            .named("acceptance/d566Readiness"),
    );
    let handoff = work_queue_readiness_handoff_projector(
        &g,
        WorkQueueReadinessHandoffOptions::new(
            vec![queue.records.clone()],
            vec![readiness.ready.clone()],
        )
        .named("acceptance/d566Handoff"),
    );
    let candidates = collect_data(&handoff.candidates);

    let cqrs = graphrefly::cqrs_with_options::<String, String>(
        &g,
        CqrsOptions::named("acceptance/d566Cqrs")
            .with_handlers(vec![graphrefly::cqrs_command_handler(
                "PlaceOrder",
                |command: &CqrsCommand<String>| {
                    vec![CqrsEventDraft::new("OrderPlaced", command.payload.clone())]
                },
            )])
            .with_events(["OrderPlaced"]),
    );

    queue.submit(
        "payload".to_owned(),
        WorkQueueSubmitOptions {
            work_id: Some("work-1".to_owned()),
            not_before_ms: Some(10),
            ..WorkQueueSubmitOptions::default()
        },
    );
    clock.set(ScheduledReadinessClock {
        clock_id: "clock".to_owned(),
        now_ms: 10,
        source_refs: Vec::new(),
        metadata: None,
    });
    now.set(10);
    let records = collect_data(&queue.records);
    queue.claim(WorkQueueClaimOptions::new("worker").command_id("claim-1"));
    cqrs.dispatch(CqrsCommand::new(
        "cmd-1",
        "PlaceOrder",
        "payload".to_owned(),
    ));

    assert!(candidates.borrow().iter().any(|candidate| {
        candidate.work_id == "work-1"
            && candidate.candidate_kind == WorkQueueReadinessCandidateKind::ClaimEligible
    }));
    assert!(records
        .borrow()
        .iter()
        .any(|record| matches!(record, WorkQueueRecord::WorkClaimed { .. })));
    assert!(cqrs
        .status
        .cache()
        .is_some_and(|status| { status.state == CqrsStatusState::Accepted }));
}

#[test]
fn public_crate_root_d566_environment_resilience_acceptance() {
    let http_driver = Rc::new(D566HttpDriver::default());
    let process_driver = Rc::new(D566ProcessDriver::default());
    let async_driver = Rc::new(D566AsyncDriver::default());
    let g = graph_opts(GraphOptions {
        name: Some("acceptance/d566Environment".to_owned()),
        environment: EnvironmentDrivers::new()
            .with_local_async(async_driver.clone())
            .with_http(http_driver.clone())
            .with_process(process_driver.clone()),
        ..GraphOptions::default()
    });
    let source = g.state_empty_opts::<String>(GraphNodeOpts::named("acceptance/env/source"));
    let bundle = to_http_with_options(
        &g,
        &source,
        |value| {
            HttpRequest::new("POST", format!("https://example.test/{value}"))
                .header("x-graphrefly", "d566")
                .body(value.as_bytes().to_vec())
        },
        OutboundAdapterOptions {
            name: Some("acceptance/env/http".to_owned()),
            retry: RetryPolicy::new(2, BackoffPolicy::None),
        },
    );
    let events = collect_data(&bundle.events);
    let attempts = collect_data(&bundle.attempts);
    let errors = collect_data(&bundle.errors);
    let _status = bundle.status.subscribe(|_| {});

    let retry_events = g.state_empty::<RetryEvent>();
    let retry_status = retry_status_node(
        &g,
        &retry_events,
        RetryPolicy::new(2, BackoffPolicy::Constant { delay_ms: 25 }),
        "acceptance/env/retry",
    );
    let _retry_status = retry_status.subscribe(|_| {});
    let process_source =
        g.state_empty_opts::<String>(GraphNodeOpts::named("acceptance/env/processSource"));
    let process_bundle = to_process_with_options(
        &g,
        &process_source,
        |value| {
            ProcessCommand::new("accept-process")
                .args([value.clone()])
                .cwd(PathBuf::from("/tmp/graphrefly-d566"))
                .env("GRAPHREFLY_ENV", "acceptance")
        },
        OutboundAdapterOptions {
            name: Some("acceptance/env/process".to_owned()),
            ..OutboundAdapterOptions::default()
        },
    );
    let _process_status = process_bundle.status.subscribe(|_| {});

    source.set("order".to_owned());
    process_source.set("process-payload".to_owned());
    retry_events.set(RetryEvent::Attempt { attempt: 1 });
    retry_events.set(RetryEvent::Failure {
        attempt: 1,
        error: "temporary http outage".to_owned(),
    });
    async_driver.fire_sleepers();
    async_driver.fire_sleepers();

    assert_eq!(
        http_driver.attempts.get(),
        2,
        "D130-D132: retry stays bounded and observable at the adapter boundary"
    );
    assert_eq!(
        http_driver.requests.borrow()[0].url,
        "https://example.test/order"
    );
    assert_eq!(
        http_driver.requests.borrow()[0].headers,
        vec![("x-graphrefly".to_owned(), "d566".to_owned())]
    );
    assert_eq!(http_driver.requests.borrow()[0].body, b"order".to_vec());
    assert!(events.borrow().iter().any(|event| matches!(
        event,
        OutboundEvent::Retry {
            value,
            attempt: 1,
            error,
            ..
        } if value == "order" && error == "temporary http outage"
    )));
    assert!(events.borrow().iter().any(|event| matches!(
        event,
        OutboundEvent::Sent {
            value,
            attempt: 2,
            result,
        } if value == "order" && result.status == 202
    )));
    assert_eq!(attempts.borrow().as_slice(), &[1, 1, 2, 2]);
    assert_eq!(
        errors.borrow().as_slice(),
        &["temporary http outage".to_owned()]
    );
    assert_eq!(
        bundle.status.cache(),
        Some(OutboundStatus {
            state: OutboundState::Succeeded,
            in_flight: 0,
            attempt: 2,
            sent: 1,
            failed: 0,
            last_delay_ms: None,
        })
    );
    assert!(retry_status.cache().is_some_and(|status| {
        status.state == RetryState::Failed && status.delay_ms == Some(25)
    }));
    assert_eq!(
        process_driver.commands.borrow()[0].program,
        "accept-process"
    );
    assert_eq!(
        process_driver.commands.borrow()[0].args,
        vec!["process-payload".to_owned()]
    );
    assert_eq!(
        process_driver.commands.borrow()[0].cwd,
        Some(PathBuf::from("/tmp/graphrefly-d566"))
    );
    assert_eq!(
        process_driver.commands.borrow()[0].env,
        vec![("GRAPHREFLY_ENV".to_owned(), "acceptance".to_owned())]
    );
    assert!(matches!(
        process_bundle.events.cache(),
        Some(OutboundEvent::Sent {
            value,
            attempt: 1,
            result,
        }) if value == "process-payload" && result.stdout == "process accepted"
    ));
    assert_eq!(
        WebSocketRequest::new("wss://example.test")
            .header("x-graphrefly", "d566")
            .headers,
        vec![("x-graphrefly".to_owned(), "d566".to_owned())]
    );
    assert_eq!(WebSocketSend::binary([1_u8, 2, 3]).data, vec![1, 2, 3]);
    assert!(g
        .describe()
        .edges
        .iter()
        .any(|edge| { edge.from == "acceptance/env/source" && edge.to == "acceptance/env/http" }));
}

#[test]
fn public_crate_root_d566_timeout_bundle_acceptance() {
    let async_driver = Rc::new(D566AsyncDriver::default());
    let g = graph_opts(GraphOptions {
        name: Some("acceptance/d566Timeout".to_owned()),
        environment: EnvironmentDrivers::new().with_local_async(async_driver.clone()),
        ..GraphOptions::default()
    });
    let source = g.state_empty_opts::<String>(GraphNodeOpts::named("acceptance/timeout/source"));
    let timeout = timeout_bundle(&g, &source, 5, "acceptance/timeout");
    let values = collect_data(&timeout.node);
    let errors = collect_data(&timeout.errors);
    let _status = timeout.status.subscribe(|_| {});

    source.set("slow".to_owned());
    async_driver.fire_sleepers();
    async_driver.fire_sleepers();

    assert_eq!(values.borrow().as_slice(), &["slow".to_owned()]);
    assert_eq!(timeout.status.cache(), Some(TimeoutStatus::Errored));
    assert!(errors
        .borrow()
        .iter()
        .any(|error| error.contains("timeout: no value within 5ms")));
}
