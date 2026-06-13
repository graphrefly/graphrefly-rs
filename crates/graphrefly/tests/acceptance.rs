use graphrefly::{
    agentic_memory_bundle, agentic_memory_consolidation_bundle,
    agentic_memory_context_packing_bundle, agentic_memory_kg_projection_bundle,
    agentic_memory_record_frame, agentic_memory_record_frame_codec,
    agentic_memory_records_snapshot_key, agentic_memory_retention_bundle, graph,
    knowledge_graph_reducer_bundle, memory_append_log, memory_kv, persist_agentic_memory_records,
    AgenticMemoryArtifactKind, AgenticMemoryBundleOptions, AgenticMemoryConsolidationBundleOptions,
    AgenticMemoryConsolidationOutcome, AgenticMemoryContextPackingBundleOptions,
    AgenticMemoryContextPackingPolicy, AgenticMemoryKgAssertionDraft,
    AgenticMemoryKgProjectionBundleOptions, AgenticMemoryKind, AgenticMemoryPersistenceLevel,
    AgenticMemoryRecord, AgenticMemoryRetentionBundleOptions, AgenticMemoryRetentionCommand,
    AgenticMemoryRetentionCommandKind, AgenticMemoryScope, AgenticMemoryStatusState,
    AgenticMemoryTextProjection, AppendLogReadOptions, AppendLogStorageTier, Codec, GraphNodeOpts,
    KnowledgeAssertionObject, KnowledgeGraphPolicy, KnowledgeGraphReducerBundleOptions,
    KnowledgeGraphStatusState, KvStorageTier, MemoryFragment, MemoryRetrievalQuery,
    PersistAgenticMemoryRecordsOptions,
};
use serde_json::{json, Value};
use std::rc::Rc;

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
