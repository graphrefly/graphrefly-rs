use graphrefly::{
    admission_filter_3d, admission_scored, agentic_memory_bundle,
    agentic_memory_context_packing_bundle, agentic_memory_kg_projection_bundle,
    agentic_memory_record_frame, agentic_memory_record_frame_codec,
    agentic_memory_retention_bundle, cosine_similarity, filter_memory_fragments, graph,
    memory_fragment_matches_query, memory_fragment_valid_at, memory_retrieval_bundle,
    shard_by_tenant, validate_agentic_memory_record, validate_agentic_memory_scope,
    validate_memory_fragment, AdmissionScore3DOptions, AdmissionScoredOptions, AdmissionScores,
    AgenticMemoryArtifactKind, AgenticMemoryBundleOptions,
    AgenticMemoryContextPackingBundleOptions, AgenticMemoryContextPackingPolicy,
    AgenticMemoryErrorCode, AgenticMemoryKgAssertionDraft, AgenticMemoryKgProjectionBundleOptions,
    AgenticMemoryKind, AgenticMemoryPersistenceLevel, AgenticMemoryRecord,
    AgenticMemoryRetentionBundleOptions, AgenticMemoryRetentionCommand,
    AgenticMemoryRetentionCommandKind, AgenticMemoryScope, AgenticMemoryStatusState,
    AgenticMemoryTextProjection, Codec, FactStore, GraphNodeOpts, KnowledgeAssertionObject,
    MemoryFragment, MemoryQuery, MemoryRetrievalBundleOptions, MemoryRetrievalErrorCode,
    MemoryRetrievalQuery, MemoryRetrievalStatusState, ShardByTenantOptions,
};
use serde_json::json;
use std::collections::BTreeMap;
use std::rc::Rc;

fn fragment(id: &str, payload: &str) -> MemoryFragment<String> {
    MemoryFragment {
        id: id.to_owned(),
        payload: payload.to_owned(),
        t_ns: 1,
        valid_from: None,
        valid_to: None,
        confidence: 1.0,
        tags: vec!["project".to_owned(), "policy".to_owned()],
        sources: Vec::new(),
        embedding: None,
        parent_fragment_id: None,
        provenance: None,
    }
}

fn record(id: &str, payload: &str) -> AgenticMemoryRecord<String> {
    AgenticMemoryRecord {
        id: format!("record-{id}"),
        fragment: fragment(id, payload),
        kind: AgenticMemoryKind::Episodic,
        persistence_level: AgenticMemoryPersistenceLevel::Session,
        artifact_kind: AgenticMemoryArtifactKind::Raw,
        scope: Some(AgenticMemoryScope {
            session_id: Some("session-1".to_owned()),
            project_id: Some("project-1".to_owned()),
            user_id: Some("user-1".to_owned()),
            tenant_id: Some("tenant-1".to_owned()),
        }),
    }
}

#[test]
fn semantic_memory_passive_fragment_validation_and_store_handle() {
    let mut ok = fragment("fact-1", "payload");
    ok.embedding = Some(vec![1.0, 0.0, 1.0]);
    ok.parent_fragment_id = Some("parent".to_owned());
    assert_eq!(
        validate_memory_fragment(&ok),
        graphrefly::MemoryFragmentValidation {
            ok: true,
            errors: Vec::new()
        }
    );

    let mut invalid = fragment("", "bad");
    invalid.confidence = f64::NAN;
    invalid.valid_from = Some(10);
    invalid.valid_to = Some(5);
    invalid.embedding = Some(vec![1.0, f64::INFINITY]);
    let validation = validate_memory_fragment(&invalid);
    assert!(!validation.ok);
    assert_eq!(
        validation.errors,
        vec![
            "id must be a non-empty string",
            "confidence must be finite in [0, 1]",
            "valid_from must be earlier than valid_to",
            "embedding must be a finite number array when present",
        ]
    );

    let mut store = FactStore::default();
    store.by_id.insert(ok.id.clone(), ok.clone());
    let handle = store.read_handle();
    assert!(handle.has("fact-1"));
    assert_eq!(handle.size(), 1);
    assert_eq!(handle.get("fact-1").unwrap().payload, "payload");
    assert_eq!(
        handle
            .values()
            .map(|fragment| fragment.id.as_str())
            .collect::<Vec<_>>(),
        vec!["fact-1"]
    );
}

#[test]
fn semantic_memory_passive_query_filtering_and_scoring_helpers() {
    let mut live = fragment("live", "live");
    live.confidence = 0.7;
    let mut old = fragment("old", "old");
    old.t_ns = 30;
    old.confidence = 0.9;
    old.valid_from = Some(1);
    old.valid_to = Some(8);
    old.tags = vec!["archive".to_owned()];
    let mut future = fragment("future", "future");
    future.valid_from = Some(40);
    let mut weak = fragment("weak", "weak");
    weak.confidence = 0.2;

    assert!(memory_fragment_valid_at(&live, None));
    assert!(!memory_fragment_valid_at(&old, None));
    assert!(!memory_fragment_valid_at(&future, None));
    assert!(memory_fragment_valid_at(&future, Some(41)));
    assert!(memory_fragment_valid_at(&old, Some(4)));
    assert!(memory_fragment_matches_query(
        &live,
        &MemoryQuery {
            tags: vec!["project".to_owned()],
            min_confidence: Some(0.5),
            ..MemoryQuery::default()
        }
    ));
    assert!(!memory_fragment_matches_query(
        &weak,
        &MemoryQuery {
            min_confidence: Some(0.5),
            ..MemoryQuery::default()
        }
    ));
    assert_eq!(
        filter_memory_fragments(
            vec![live.clone(), old.clone(), weak.clone()],
            &MemoryQuery {
                as_of: Some(4),
                min_confidence: Some(0.5),
                limit: Some(1),
                ..MemoryQuery::default()
            }
        )
        .into_iter()
        .map(|fragment| fragment.id)
        .collect::<Vec<_>>(),
        vec!["old".to_owned()]
    );

    assert_eq!(cosine_similarity(&[1.0, 0.0], &[1.0, 0.0]), 1.0);
    assert_eq!(cosine_similarity(&[1.0, 0.0], &[0.0, 1.0]), 0.0);
    assert_eq!(cosine_similarity(&[0.0, 0.0], &[1.0, 1.0]), 0.0);
    assert_eq!(cosine_similarity(&[f64::NAN], &[1.0]), 0.0);
    assert_eq!(cosine_similarity(&[f64::INFINITY], &[1.0]), 0.0);
    assert_eq!(cosine_similarity(&[1.0], &[1.0, 0.0]), 1.0);

    let scored = admission_scored(AdmissionScoredOptions {
        score_fn: Rc::new(|raw: &BTreeMap<String, f64>| raw.clone()),
        thresholds: BTreeMap::from([("relevance".to_owned(), 0.5)]),
    });
    assert!(scored(&BTreeMap::from([("relevance".to_owned(), 0.6)])));
    assert!(!scored(&BTreeMap::from([("relevance".to_owned(), 0.4)])));
    assert!(!scored(&BTreeMap::new()));

    let three_d = admission_filter_3d(AdmissionScore3DOptions {
        score_fn: Rc::new(|scores: &AdmissionScores| *scores),
        persistence_threshold: 0.3,
        personal_value_threshold: 0.3,
        require_structured: true,
    });
    assert!(three_d(&AdmissionScores {
        persistence: 0.5,
        structure: 0.1,
        personal_value: 0.5,
    }));
    assert!(!three_d(&AdmissionScores {
        persistence: 0.5,
        structure: 0.0,
        personal_value: 0.5,
    }));

    let strict = shard_by_tenant(
        Rc::new(|fragment: &MemoryFragment<String>| fragment.payload.clone()),
        ShardByTenantOptions {
            tenants: vec!["acme".to_owned(), "globex".to_owned(), "acme".to_owned()],
            shard_count: None,
        },
    );
    assert_eq!(strict.shard_count, 3);
    assert_eq!((strict.shard_by)(&fragment("a", "acme")), "0");
    assert_eq!((strict.shard_by)(&fragment("b", "other")), "2");

    let soft = shard_by_tenant(
        Rc::new(|fragment: &MemoryFragment<String>| fragment.payload.clone()),
        ShardByTenantOptions {
            tenants: Vec::new(),
            shard_count: Some(0),
        },
    );
    assert_eq!(soft.shard_count, 1);
    assert_eq!((soft.shard_by)(&fragment("c", "acme")), "acme");
}

#[test]
fn memory_retrieval_bundle_exposes_snapshot_and_projection_topology() {
    let g = graph();
    let fragments = g.state_opts(
        Vec::<MemoryFragment<String>>::new(),
        GraphNodeOpts::named("fragments"),
    );
    let query = g.state_opts(
        MemoryRetrievalQuery {
            tags: vec!["policy".to_owned()],
            ..MemoryRetrievalQuery::default()
        },
        GraphNodeOpts::named("query"),
    );
    let bundle = memory_retrieval_bundle(
        &g,
        MemoryRetrievalBundleOptions::new(fragments.clone(), query.clone()).named("memory"),
    );

    let edges = g.describe().edges;
    assert!(edges
        .iter()
        .any(|edge| edge.from == "fragments" && edge.to == "memory/snapshot"));
    assert!(edges
        .iter()
        .any(|edge| edge.from == "query" && edge.to == "memory/snapshot"));
    assert!(edges
        .iter()
        .any(|edge| edge.from == "memory/snapshot" && edge.to == "memory/ranked"));
    assert!(edges
        .iter()
        .any(|edge| edge.from == "memory/snapshot" && edge.to == "memory/status"));
    let described = g.describe();
    assert!(described
        .nodes
        .iter()
        .any(|node| node.id == "memory/snapshot" && node.factory == "memoryRetrievalSnapshot"));
    assert!(described
        .nodes
        .iter()
        .any(|node| node.id == "memory/ranked" && node.factory == "memoryRetrievalRanked"));
    let deps_by_id = described
        .nodes
        .iter()
        .map(|node| (node.id.as_str(), node.deps.clone()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        deps_by_id["memory/snapshot"],
        vec!["fragments".to_owned(), "query".to_owned()]
    );
    for projection in [
        "memory/fragments",
        "memory/indexed",
        "memory/ranked",
        "memory/status",
        "memory/errors",
        "memory/cursor",
    ] {
        assert_eq!(
            deps_by_id[projection],
            vec!["memory/snapshot".to_owned()],
            "{projection} should depend only on the aggregate snapshot fact"
        );
    }
    assert_eq!(bundle.fragments_input.cache(), Some(Vec::new()));
    assert_eq!(
        bundle.query_input.cache().unwrap().tags,
        vec!["policy".to_owned()]
    );
}

#[test]
fn memory_retrieval_bundle_ranks_and_serves_late_projection_subscribers() {
    let g = graph();
    let mut near = fragment("near", "near");
    near.confidence = 0.6;
    near.embedding = Some(vec![1.0, 0.0]);
    near.valid_from = Some(1);
    near.valid_to = Some(20);
    let mut far = fragment("far", "far");
    far.confidence = 0.95;
    far.embedding = Some(vec![0.0, 1.0]);
    far.valid_from = Some(1);
    far.valid_to = Some(20);
    let mut no_embedding = fragment("no-embedding", "no-embedding");
    no_embedding.confidence = 0.99;
    no_embedding.valid_from = Some(1);
    no_embedding.valid_to = Some(20);
    let mut other = fragment("other", "other");
    other.tags = vec!["other".to_owned()];
    other.embedding = Some(vec![1.0, 0.0]);
    other.valid_from = Some(1);
    other.valid_to = Some(20);
    let mut future = fragment("future", "future");
    future.embedding = Some(vec![1.0, 0.0]);
    future.valid_from = Some(30);
    let mut weak = fragment("weak", "weak");
    weak.embedding = Some(vec![1.0, 0.0]);
    weak.confidence = 0.2;
    weak.valid_from = Some(1);
    weak.valid_to = Some(20);
    let fragments = g.state_opts(
        vec![near, far, no_embedding, other, future, weak],
        GraphNodeOpts::named("fragments"),
    );
    let query = g.state_opts(
        MemoryRetrievalQuery {
            tags: vec!["policy".to_owned()],
            as_of: Some(10),
            min_confidence: Some(0.5),
            vector: Some(vec![1.0, 0.0]),
            limit: Some(3),
        },
        GraphNodeOpts::named("query"),
    );
    let bundle = memory_retrieval_bundle(
        &g,
        MemoryRetrievalBundleOptions::new(fragments, query.clone()).named("memory"),
    );

    let _ranked = bundle.ranked.subscribe(|_| {});
    assert_eq!(
        bundle
            .ranked
            .cache()
            .unwrap()
            .results
            .iter()
            .map(|fragment| fragment.id.as_str())
            .collect::<Vec<_>>(),
        vec!["near", "no-embedding", "far"],
    );

    let _status = bundle.status.subscribe(|_| {});
    let _errors = bundle.errors.subscribe(|_| {});
    let _cursor = bundle.cursor.subscribe(|_| {});
    assert_eq!(
        bundle.status.cache().unwrap().state,
        MemoryRetrievalStatusState::Ready
    );
    assert!(bundle.errors.cache().unwrap().is_empty());
    assert_eq!(bundle.cursor.cache().unwrap().result_count, 3);
    assert_eq!(bundle.status.cache().unwrap().cursor.result_count, 3);

    query.set(MemoryRetrievalQuery {
        tags: vec!["other".to_owned()],
        as_of: Some(10),
        vector: Some(vec![1.0, 0.0]),
        ..MemoryRetrievalQuery::default()
    });
    assert_eq!(
        bundle
            .ranked
            .cache()
            .unwrap()
            .results
            .iter()
            .map(|fragment| fragment.id.as_str())
            .collect::<Vec<_>>(),
        vec!["other"],
    );
}

#[test]
fn memory_retrieval_bundle_surfaces_errors_as_data_facts() {
    let g = graph();
    let mut invalid = fragment("", "invalid");
    invalid.confidence = f64::NAN;
    let fragments = g.state_opts(
        vec![fragment("ok", "ok"), invalid, fragment("ok", "dup")],
        GraphNodeOpts::named("fragments"),
    );
    let query = g.state_opts(
        MemoryRetrievalQuery::default(),
        GraphNodeOpts::named("query"),
    );
    let bundle = memory_retrieval_bundle(
        &g,
        MemoryRetrievalBundleOptions::new(fragments, query).named("memory"),
    );

    let _errors = bundle.errors.subscribe(|_| {});
    let errors = bundle.errors.cache().unwrap();
    assert_eq!(errors.len(), 2);
    assert_eq!(errors[0].code, MemoryRetrievalErrorCode::InvalidFragment);
    assert_eq!(
        errors[1].code,
        MemoryRetrievalErrorCode::DuplicateFragmentId
    );
    let _status = bundle.status.subscribe(|_| {});
    assert_eq!(
        bundle.status.cache().unwrap().state,
        MemoryRetrievalStatusState::Partial
    );
    assert_eq!(bundle.status.cache().unwrap().cursor.valid_fragments, 1);
    assert_eq!(bundle.status.cache().unwrap().cursor.invalid_fragments, 2);
}

#[test]
fn memory_retrieval_bundle_invalid_query_is_graph_visible_error_fact() {
    let g = graph();
    let mut ok = fragment("ok", "ok");
    ok.embedding = Some(vec![1.0, 0.0]);
    let fragments = g.state_opts(vec![ok], GraphNodeOpts::named("fragments"));
    let query = g.state_opts(
        MemoryRetrievalQuery {
            vector: Some(vec![1.0, 0.0]),
            ..MemoryRetrievalQuery::default()
        },
        GraphNodeOpts::named("query"),
    );
    let bundle = memory_retrieval_bundle(
        &g,
        MemoryRetrievalBundleOptions::new(fragments, query.clone()).named("memory"),
    );

    let _errors = bundle.errors.subscribe(|_| {});
    let _status = bundle.status.subscribe(|_| {});
    let _ranked = bundle.ranked.subscribe(|_| {});

    query.set(MemoryRetrievalQuery {
        min_confidence: Some(f64::NAN),
        vector: Some(vec![1.0, f64::INFINITY]),
        ..MemoryRetrievalQuery::default()
    });

    let errors = bundle.errors.cache().unwrap();
    assert_eq!(
        errors.iter().map(|error| error.code).collect::<Vec<_>>(),
        vec![
            MemoryRetrievalErrorCode::InvalidQuery,
            MemoryRetrievalErrorCode::InvalidQueryVector,
        ]
    );
    assert_eq!(
        bundle.status.cache().unwrap().state,
        MemoryRetrievalStatusState::Error
    );
    assert_eq!(bundle.status.cache().unwrap().cursor.valid_fragments, 1);
    assert_eq!(bundle.status.cache().unwrap().cursor.result_count, 0);
    assert!(bundle.ranked.cache().unwrap().results.is_empty());
}

#[test]
fn memory_retrieval_bundle_clears_errors_on_clean_rerun_and_dedupes_ids() {
    let g = graph();
    let fragments = g.state_opts(
        Vec::<MemoryFragment<String>>::new(),
        GraphNodeOpts::named("fragments"),
    );
    let query = g.state_opts(
        MemoryRetrievalQuery::default(),
        GraphNodeOpts::named("query"),
    );
    let bundle = memory_retrieval_bundle(
        &g,
        MemoryRetrievalBundleOptions::new(fragments.clone(), query).named("memory"),
    );
    let _errors = bundle.errors.subscribe(|_| {});
    let _indexed = bundle.indexed.subscribe(|_| {});
    let _ranked = bundle.ranked.subscribe(|_| {});
    let _status = bundle.status.subscribe(|_| {});

    fragments.set(vec![fragment("dup", "a"), fragment("dup", "b")]);
    let errors = bundle.errors.cache().unwrap();
    assert_eq!(errors.len(), 1);
    assert_eq!(
        errors[0].code,
        MemoryRetrievalErrorCode::DuplicateFragmentId
    );
    assert_eq!(errors[0].index, Some(1));
    assert_eq!(bundle.indexed.cache().unwrap().ids, vec!["dup".to_owned()]);
    assert_eq!(
        bundle
            .ranked
            .cache()
            .unwrap()
            .results
            .iter()
            .map(|fragment| fragment.id.as_str())
            .collect::<Vec<_>>(),
        vec!["dup"]
    );
    assert_eq!(
        bundle.status.cache().unwrap().state,
        MemoryRetrievalStatusState::Partial
    );

    fragments.set(vec![fragment("clean", "clean")]);
    assert!(bundle.errors.cache().unwrap().is_empty());
    assert_eq!(
        bundle.indexed.cache().unwrap().ids,
        vec!["clean".to_owned()]
    );
}

#[test]
fn memory_retrieval_bundle_keeps_storage_controls_out_of_surface() {
    let g = graph();
    let fragments = g.state_opts(
        Vec::<MemoryFragment<String>>::new(),
        GraphNodeOpts::named("fragments"),
    );
    let query = g.state_opts(
        MemoryRetrievalQuery::default(),
        GraphNodeOpts::named("query"),
    );
    let bundle = memory_retrieval_bundle(
        &g,
        MemoryRetrievalBundleOptions::new(fragments, query).named("memory"),
    );

    assert!(g.find("memory/snapshot").is_some());
    assert!(g.find("memory/ranked").is_some());
    assert_eq!(bundle.snapshot.cache(), None);
}

#[test]
fn agentic_memory_record_validation_keeps_axes_and_scope_explicit() {
    let ok = record("ok", "payload");
    assert!(validate_agentic_memory_record(&ok).ok);
    assert!(validate_agentic_memory_scope(ok.scope.as_ref().unwrap()).ok);

    let mut invalid = record("", "bad");
    invalid.fragment.confidence = f64::NAN;
    invalid.scope = Some(AgenticMemoryScope {
        session_id: Some(String::new()),
        project_id: Some("project".to_owned()),
        user_id: None,
        tenant_id: Some(String::new()),
    });
    let validation = validate_agentic_memory_record(&invalid);
    assert!(!validation.ok);
    assert_eq!(
        validation.errors,
        vec![
            "fragment.id must be a non-empty string",
            "fragment.confidence must be finite in [0, 1]",
            "scope.session_id must be a non-empty string when present",
            "scope.tenant_id must be a non-empty string when present",
        ]
    );
}

#[test]
fn agentic_memory_bundle_projects_records_to_retrieval_and_context_metadata() {
    let g = graph();
    let mut near = record("near", "close");
    near.kind = AgenticMemoryKind::Semantic;
    near.persistence_level = AgenticMemoryPersistenceLevel::LongTerm;
    near.artifact_kind = AgenticMemoryArtifactKind::Insight;
    near.fragment.confidence = 0.6;
    near.fragment.embedding = Some(vec![1.0, 0.0]);
    near.fragment.sources = vec!["seed".to_owned(), "note".to_owned()];
    near.fragment.parent_fragment_id = Some("seed".to_owned());
    let mut far = record("far", "distant");
    far.fragment.confidence = 0.95;
    far.fragment.embedding = Some(vec![0.0, 1.0]);
    far.fragment.provenance = Some("fixture".to_owned());
    let records = g.state_opts(vec![far, near], GraphNodeOpts::named("records"));
    let query = g.state_opts(
        MemoryRetrievalQuery {
            tags: vec!["policy".to_owned()],
            vector: Some(vec![1.0, 0.0]),
            limit: Some(2),
            ..MemoryRetrievalQuery::default()
        },
        GraphNodeOpts::named("query"),
    );

    let bundle = agentic_memory_bundle(
        &g,
        AgenticMemoryBundleOptions::new(records.clone(), query.clone()).named("memory"),
    );

    let described = g.describe();
    assert!(described
        .edges
        .iter()
        .any(|edge| edge.from == "records" && edge.to == "memory/projection"));
    assert!(described
        .edges
        .iter()
        .any(|edge| edge.from == "memory/projection" && edge.to == "memory/fragments"));
    assert!(described
        .edges
        .iter()
        .any(|edge| edge.from == "memory/fragments" && edge.to == "memory/retrieval/snapshot"));
    assert!(described
        .edges
        .iter()
        .any(|edge| edge.from == "memory/retrieval/snapshot" && edge.to == "memory/context"));
    assert!(described
        .nodes
        .iter()
        .any(|node| node.id == "memory/projection" && node.factory == "agenticMemoryProjection"));
    assert!(described
        .nodes
        .iter()
        .any(|node| node.id == "memory/context" && node.factory == "agenticMemoryContext"));

    let _context = bundle.context.subscribe(|_| {});
    let _sources = bundle.sources.subscribe(|_| {});
    let _status = bundle.status.subscribe(|_| {});
    let context = bundle.context.cache().unwrap();
    assert_eq!(context.state, AgenticMemoryStatusState::Ready);
    assert!(context.context_ready);
    assert_eq!(
        context
            .entries
            .iter()
            .map(|entry| entry.fragment_id.as_str())
            .collect::<Vec<_>>(),
        vec!["near", "far"]
    );
    let near_entry = &context.entries[0];
    assert_eq!(near_entry.payload, "close");
    assert_eq!(
        near_entry.metadata.as_ref().unwrap().kind,
        AgenticMemoryKind::Semantic
    );
    assert_eq!(
        near_entry.metadata.as_ref().unwrap().persistence_level,
        AgenticMemoryPersistenceLevel::LongTerm
    );
    assert_eq!(
        near_entry.metadata.as_ref().unwrap().artifact_kind,
        AgenticMemoryArtifactKind::Insight
    );
    assert_eq!(bundle.status.cache().unwrap().cursor.result_count, 2);
    assert_eq!(
        bundle
            .sources
            .cache()
            .unwrap()
            .iter()
            .map(|source| source.fragment_id.as_str())
            .collect::<Vec<_>>(),
        vec!["far", "near"]
    );
    assert_eq!(bundle.records_input.cache().unwrap().len(), 2);
    assert_eq!(bundle.query_input.cache().unwrap().limit, Some(2));
}

#[test]
fn agentic_memory_bundle_surfaces_solution_errors_and_preserves_retrieval_errors() {
    let g = graph();
    let mut invalid = record("", "invalid");
    invalid.fragment.confidence = f64::NAN;
    let records = g.state_opts(
        vec![record("dup", "kept"), record("dup", "duplicate"), invalid],
        GraphNodeOpts::named("records"),
    );
    let query = g.state_opts(
        MemoryRetrievalQuery::default(),
        GraphNodeOpts::named("query"),
    );
    let bundle = agentic_memory_bundle(
        &g,
        AgenticMemoryBundleOptions::new(records, query.clone()).named("memory"),
    );
    let _context = bundle.context.subscribe(|_| {});
    let _errors = bundle.errors.subscribe(|_| {});
    let _retrieval_errors = bundle.retrieval_errors.subscribe(|_| {});
    let _status = bundle.status.subscribe(|_| {});

    let errors = bundle.errors.cache().unwrap();
    assert_eq!(
        errors.iter().map(|error| error.code).collect::<Vec<_>>(),
        vec![
            AgenticMemoryErrorCode::DuplicateRecordId,
            AgenticMemoryErrorCode::InvalidRecord,
        ]
    );
    assert_eq!(errors[1].index, Some(2));
    let projection_error_evaluation = errors[0].cursor.evaluation;
    assert_eq!(
        errors[1].validation_errors,
        vec![
            "fragment.id must be a non-empty string",
            "fragment.confidence must be finite in [0, 1]",
        ]
    );
    assert_eq!(
        bundle
            .retrieval_errors
            .cache()
            .unwrap()
            .iter()
            .map(|error| error.code)
            .collect::<Vec<_>>(),
        Vec::<MemoryRetrievalErrorCode>::new()
    );
    assert_eq!(
        bundle.context.cache().unwrap().state,
        AgenticMemoryStatusState::Partial
    );
    assert_eq!(bundle.context.cache().unwrap().entries.len(), 1);

    query.set(MemoryRetrievalQuery {
        vector: Some(vec![1.0, f64::INFINITY]),
        ..MemoryRetrievalQuery::default()
    });
    assert_eq!(
        bundle.context.cache().unwrap().state,
        AgenticMemoryStatusState::Error
    );
    assert_eq!(
        bundle.errors.cache().unwrap()[0].cursor.evaluation,
        projection_error_evaluation,
        "query-only reruns must not rewrite the projection cursor on solution validation errors"
    );
    assert_eq!(
        bundle
            .retrieval_errors
            .cache()
            .unwrap()
            .iter()
            .map(|error| error.code)
            .collect::<Vec<_>>(),
        vec![MemoryRetrievalErrorCode::InvalidQueryVector,]
    );
    assert!(bundle.context.cache().unwrap().entries.is_empty());

    query.set(MemoryRetrievalQuery::default());
    let mut all_invalid = record("", "all-invalid");
    all_invalid.fragment.confidence = f64::NAN;
    bundle.records_input.set(vec![all_invalid]);
    assert_eq!(
        bundle.context.cache().unwrap().state,
        AgenticMemoryStatusState::Error,
        "a batch with only invalid solution records is an agentic-memory error, not partial context"
    );
    assert!(!bundle.context.cache().unwrap().context_ready);
}

#[test]
fn agentic_memory_bundle_rejects_duplicate_projected_fragment_ownership() {
    let g = graph();
    let mut a = record("a", "first");
    a.fragment.id = "same-fragment".to_owned();
    let mut b = record("b", "second");
    b.fragment.id = "same-fragment".to_owned();
    let records = g.state_opts(vec![a, b], GraphNodeOpts::named("records"));
    let query = g.state_opts(
        MemoryRetrievalQuery::default(),
        GraphNodeOpts::named("query"),
    );
    let bundle = agentic_memory_bundle(
        &g,
        AgenticMemoryBundleOptions::new(records, query).named("memory"),
    );
    let _errors = bundle.errors.subscribe(|_| {});
    let _fragments = bundle.fragments.subscribe(|_| {});
    let _indexed = bundle.indexed.subscribe(|_| {});
    let _context = bundle.context.subscribe(|_| {});

    let errors = bundle.errors.cache().unwrap();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].code, AgenticMemoryErrorCode::DuplicateFragmentId);
    assert_eq!(errors[0].record_id.as_deref(), Some("record-b"));
    assert_eq!(errors[0].fragment_id.as_deref(), Some("same-fragment"));
    assert_eq!(
        bundle
            .fragments
            .cache()
            .unwrap()
            .iter()
            .map(|fragment| (fragment.id.as_str(), fragment.payload.as_str()))
            .collect::<Vec<_>>(),
        vec![("same-fragment", "first")]
    );
    assert_eq!(
        bundle.indexed.cache().unwrap().ids,
        vec!["same-fragment".to_owned()]
    );
    assert_eq!(bundle.context.cache().unwrap().entries.len(), 1);
}

#[test]
fn agentic_memory_kg_projection_validates_drafts_and_exposes_topology() {
    let g = graph();
    let records = g.state_opts(
        vec![record("fact", "payload"), record("other", "other")],
        GraphNodeOpts::named("records"),
    );
    let drafts = g.state_opts(
        vec![
            AgenticMemoryKgAssertionDraft {
                id: "assertion-1".to_owned(),
                record_id: Some("record-fact".to_owned()),
                fragment_id: Some("fact".to_owned()),
                subject_id: "entity:user".to_owned(),
                predicate: "prefers".to_owned(),
                object: KnowledgeAssertionObject::Literal {
                    value: json!("quiet tools"),
                },
                confidence: 0.9,
                t_ns: 42,
            },
            AgenticMemoryKgAssertionDraft {
                id: "assertion-1".to_owned(),
                record_id: Some("missing".to_owned()),
                fragment_id: None,
                subject_id: String::new(),
                predicate: String::new(),
                object: KnowledgeAssertionObject::Entity {
                    entity_id: String::new(),
                },
                confidence: f64::NAN,
                t_ns: 43,
            },
            AgenticMemoryKgAssertionDraft {
                id: "assertion-2".to_owned(),
                record_id: Some("record-fact".to_owned()),
                fragment_id: Some("other".to_owned()),
                subject_id: "entity:user".to_owned(),
                predicate: "mentions".to_owned(),
                object: KnowledgeAssertionObject::Entity {
                    entity_id: "entity:other".to_owned(),
                },
                confidence: 0.8,
                t_ns: 44,
            },
        ],
        GraphNodeOpts::named("drafts"),
    );
    let bundle = agentic_memory_kg_projection_bundle(
        &g,
        AgenticMemoryKgProjectionBundleOptions::new(records, drafts).named("kg"),
    );
    let described = g.describe();
    assert!(described
        .edges
        .iter()
        .any(|edge| edge.from == "records" && edge.to == "kg/snapshot"));
    assert!(described
        .edges
        .iter()
        .any(|edge| edge.from == "drafts" && edge.to == "kg/snapshot"));
    assert!(described
        .nodes
        .iter()
        .any(|node| node.id == "kg/assertions" && node.factory == "agenticMemoryKgAssertions"));

    let _assertions = bundle.assertions.subscribe(|_| {});
    let _errors = bundle.errors.subscribe(|_| {});
    let _status = bundle.status.subscribe(|_| {});
    assert_eq!(bundle.assertions.cache().unwrap().len(), 1);
    assert_eq!(bundle.assertions.cache().unwrap()[0].id, "assertion-1");
    assert_eq!(
        bundle
            .errors
            .cache()
            .unwrap()
            .iter()
            .map(|error| error.code)
            .collect::<Vec<_>>(),
        vec![
            AgenticMemoryErrorCode::DuplicateAssertionId,
            AgenticMemoryErrorCode::InvalidKgDraft,
        ]
    );
    assert_eq!(
        bundle.status.cache().unwrap().state,
        AgenticMemoryStatusState::Partial
    );
}

#[test]
fn agentic_memory_kg_projection_invalid_draft_does_not_poison_later_valid_id() {
    let g = graph();
    let records = g.state_opts(
        vec![record("fact", "payload")],
        GraphNodeOpts::named("records"),
    );
    let drafts = g.state_opts(
        vec![
            AgenticMemoryKgAssertionDraft {
                id: "assertion-reused".to_owned(),
                record_id: Some("missing".to_owned()),
                fragment_id: None,
                subject_id: String::new(),
                predicate: "bad".to_owned(),
                object: KnowledgeAssertionObject::Entity {
                    entity_id: "entity:ok".to_owned(),
                },
                confidence: 0.9,
                t_ns: 1,
            },
            AgenticMemoryKgAssertionDraft {
                id: "assertion-reused".to_owned(),
                record_id: Some("record-fact".to_owned()),
                fragment_id: Some("fact".to_owned()),
                subject_id: "entity:user".to_owned(),
                predicate: "prefers".to_owned(),
                object: KnowledgeAssertionObject::Literal {
                    value: json!("quiet tools"),
                },
                confidence: 0.9,
                t_ns: 2,
            },
        ],
        GraphNodeOpts::named("drafts"),
    );
    let bundle = agentic_memory_kg_projection_bundle(
        &g,
        AgenticMemoryKgProjectionBundleOptions::new(records, drafts).named("kg"),
    );
    let _assertions = bundle.assertions.subscribe(|_| {});
    let _errors = bundle.errors.subscribe(|_| {});

    assert_eq!(bundle.assertions.cache().unwrap().len(), 1);
    assert_eq!(bundle.assertions.cache().unwrap()[0].id, "assertion-reused");
    assert_eq!(
        bundle.errors.cache().unwrap()[0].code,
        AgenticMemoryErrorCode::InvalidKgDraft
    );
}

#[test]
fn agentic_memory_record_frame_codec_roundtrips_and_rejects_corruption() {
    let mut fragment = MemoryFragment::new("fact-json", json!({"text": "payload"}), 12);
    fragment.valid_from = Some(10);
    fragment.valid_to = Some(20);
    fragment.sources = vec!["source".to_owned()];
    let record = AgenticMemoryRecord {
        id: "record-json".to_owned(),
        fragment,
        kind: AgenticMemoryKind::Semantic,
        persistence_level: AgenticMemoryPersistenceLevel::Project,
        artifact_kind: AgenticMemoryArtifactKind::Insight,
        scope: Some(AgenticMemoryScope {
            session_id: Some("session".to_owned()),
            project_id: Some("project".to_owned()),
            user_id: None,
            tenant_id: None,
        }),
    };
    let frame = agentic_memory_record_frame(record);
    let codec = agentic_memory_record_frame_codec();
    let encoded = codec.encode(&frame).unwrap();
    let encoded_text = std::str::from_utf8(&encoded).unwrap();
    assert!(encoded_text.contains("\"tNs\":\"12\""));
    assert!(encoded_text.contains("\"validFrom\":\"10\""));
    assert_eq!(codec.decode(&encoded).unwrap(), frame);

    let mut corrupt = serde_json::to_value(json!({
        "format": "graphrefly.agentic-memory.record",
        "version": 1,
        "record": {
            "id": "record-json",
            "kind": "cold",
            "persistenceLevel": "project",
            "artifactKind": "insight",
            "fragment": {
                "id": "fact-json",
                "payload": {},
                "tNs": "01",
                "confidence": 1.0,
                "tags": [],
                "sources": []
            },
            "unknown": true
        }
    }))
    .unwrap();
    let corrupt_bytes = serde_json::to_vec(&corrupt).unwrap();
    assert!(codec.decode(&corrupt_bytes).is_err());
    corrupt["record"]["kind"] = json!("semantic");
    corrupt["record"].as_object_mut().unwrap().remove("unknown");
    let corrupt_bytes = serde_json::to_vec(&corrupt).unwrap();
    assert!(codec.decode(&corrupt_bytes).is_err());
}

#[test]
fn agentic_memory_retention_commands_project_archive_and_consolidation_requests() {
    let g = graph();
    let mut archived_metadata_only = record("meta-archived", "meta-archived");
    archived_metadata_only.persistence_level = AgenticMemoryPersistenceLevel::Archived;
    let records = g.state_opts(
        vec![
            record("active", "active"),
            archived_metadata_only,
            record("archive", "archive"),
        ],
        GraphNodeOpts::named("records"),
    );
    let commands = g.state_opts(
        vec![
            AgenticMemoryRetentionCommand {
                id: "archive-cmd".to_owned(),
                record_id: "record-archive".to_owned(),
                kind: AgenticMemoryRetentionCommandKind::Archive,
                reason: Some("done".to_owned()),
            },
            AgenticMemoryRetentionCommand {
                id: "consolidate-cmd".to_owned(),
                record_id: "record-active".to_owned(),
                kind: AgenticMemoryRetentionCommandKind::RequestConsolidation,
                reason: Some("merge".to_owned()),
            },
            AgenticMemoryRetentionCommand {
                id: "bad-cmd".to_owned(),
                record_id: "missing".to_owned(),
                kind: AgenticMemoryRetentionCommandKind::Archive,
                reason: None,
            },
        ],
        GraphNodeOpts::named("commands"),
    );
    let bundle = agentic_memory_retention_bundle(
        &g,
        AgenticMemoryRetentionBundleOptions::new(records, commands).named("retention"),
    );
    let _active = bundle.active_records.subscribe(|_| {});
    let _archived = bundle.archived_records.subscribe(|_| {});
    let _requests = bundle.consolidation_requests.subscribe(|_| {});
    let _errors = bundle.errors.subscribe(|_| {});
    let _status = bundle.status.subscribe(|_| {});

    assert_eq!(
        bundle
            .active_records
            .cache()
            .unwrap()
            .iter()
            .map(|record| record.id.as_str())
            .collect::<Vec<_>>(),
        vec!["record-active", "record-meta-archived"]
    );
    assert_eq!(
        bundle
            .archived_records
            .cache()
            .unwrap()
            .iter()
            .map(|record| record.id.as_str())
            .collect::<Vec<_>>(),
        vec!["record-archive"]
    );
    assert_eq!(bundle.consolidation_requests.cache().unwrap().len(), 1);
    assert_eq!(
        bundle.consolidation_requests.cache().unwrap()[0].fragment_id,
        "active"
    );
    assert_eq!(
        bundle.errors.cache().unwrap()[0].code,
        AgenticMemoryErrorCode::InvalidRetentionCommand
    );
    assert_eq!(
        bundle.status.cache().unwrap().state,
        AgenticMemoryStatusState::Partial
    );
}

#[test]
fn agentic_memory_retention_invalid_command_does_not_poison_later_valid_id() {
    let g = graph();
    let records = g.state_opts(
        vec![record("active", "active")],
        GraphNodeOpts::named("records"),
    );
    let commands = g.state_opts(
        vec![
            AgenticMemoryRetentionCommand {
                id: "reused-command".to_owned(),
                record_id: "missing".to_owned(),
                kind: AgenticMemoryRetentionCommandKind::Archive,
                reason: None,
            },
            AgenticMemoryRetentionCommand {
                id: "reused-command".to_owned(),
                record_id: "record-active".to_owned(),
                kind: AgenticMemoryRetentionCommandKind::Archive,
                reason: None,
            },
        ],
        GraphNodeOpts::named("commands"),
    );
    let bundle = agentic_memory_retention_bundle(
        &g,
        AgenticMemoryRetentionBundleOptions::new(records, commands).named("retention"),
    );
    let _active = bundle.active_records.subscribe(|_| {});
    let _archived = bundle.archived_records.subscribe(|_| {});
    let _errors = bundle.errors.subscribe(|_| {});

    assert!(bundle.active_records.cache().unwrap().is_empty());
    assert_eq!(
        bundle.archived_records.cache().unwrap()[0].id,
        "record-active"
    );
    assert_eq!(
        bundle.errors.cache().unwrap()[0].code,
        AgenticMemoryErrorCode::InvalidRetentionCommand
    );
}

#[test]
fn agentic_memory_context_packing_is_deterministic_and_projection_only() {
    let g = graph();
    let records = g.state_opts(
        vec![record("near", "payload-near"), record("far", "payload-far")],
        GraphNodeOpts::named("records"),
    );
    let query = g.state_opts(
        MemoryRetrievalQuery {
            limit: Some(2),
            ..MemoryRetrievalQuery::default()
        },
        GraphNodeOpts::named("query"),
    );
    let memory = agentic_memory_bundle(
        &g,
        AgenticMemoryBundleOptions::new(records, query).named("memory"),
    );
    let texts = g.state_opts(
        vec![
            AgenticMemoryTextProjection {
                fragment_id: "near".to_owned(),
                text: "near text".to_owned(),
            },
            AgenticMemoryTextProjection {
                fragment_id: "far".to_owned(),
                text: "far text".to_owned(),
            },
        ],
        GraphNodeOpts::named("texts"),
    );
    let policy = g.state_opts(
        AgenticMemoryContextPackingPolicy {
            max_chars: Some(24),
            separator: " | ".to_owned(),
            include_fragment_ids: true,
        },
        GraphNodeOpts::named("policy"),
    );
    let packing = agentic_memory_context_packing_bundle(
        &g,
        AgenticMemoryContextPackingBundleOptions::new(memory.context.clone(), texts, policy)
            .named("packing"),
    );
    let described = g.describe();
    assert!(described
        .edges
        .iter()
        .any(|edge| edge.from == "memory/context" && edge.to == "packing/snapshot"));
    assert!(described
        .edges
        .iter()
        .any(|edge| edge.from == "texts" && edge.to == "packing/snapshot"));
    assert!(described
        .edges
        .iter()
        .any(|edge| edge.from == "policy" && edge.to == "packing/snapshot"));

    let _packed = packing.packed_context.subscribe(|_| {});
    let _status = packing.status.subscribe(|_| {});
    let packed = packing.packed_context.cache().unwrap();
    assert_eq!(packed.fragment_ids, vec!["near".to_owned()]);
    assert_eq!(packed.text, "[near] near text");
    assert!(packed.truncated);
    assert_eq!(
        packing.status.cache().unwrap().state,
        AgenticMemoryStatusState::Partial
    );

    packing.policy_input.set(AgenticMemoryContextPackingPolicy {
        max_chars: Some(5),
        separator: " | ".to_owned(),
        include_fragment_ids: true,
    });
    let packed = packing.packed_context.cache().unwrap();
    assert!(packed.fragment_ids.is_empty());
    assert!(packed.text.is_empty());
    assert!(packed.truncated);
    assert_eq!(
        packing.status.cache().unwrap().state,
        AgenticMemoryStatusState::Partial
    );
}

#[test]
fn agentic_memory_context_packing_validates_text_projection_facts() {
    let g = graph();
    let records = g.state_opts(
        vec![record("near", "payload-near")],
        GraphNodeOpts::named("records"),
    );
    let query = g.state_opts(
        MemoryRetrievalQuery::default(),
        GraphNodeOpts::named("query"),
    );
    let memory = agentic_memory_bundle(
        &g,
        AgenticMemoryBundleOptions::new(records, query).named("memory"),
    );
    let texts = g.state_opts(
        vec![
            AgenticMemoryTextProjection {
                fragment_id: "near".to_owned(),
                text: "near text".to_owned(),
            },
            AgenticMemoryTextProjection {
                fragment_id: "near".to_owned(),
                text: "conflicting text".to_owned(),
            },
            AgenticMemoryTextProjection {
                fragment_id: String::new(),
                text: String::new(),
            },
        ],
        GraphNodeOpts::named("texts"),
    );
    let policy = g.state_opts(
        AgenticMemoryContextPackingPolicy::default(),
        GraphNodeOpts::named("policy"),
    );
    let packing = agentic_memory_context_packing_bundle(
        &g,
        AgenticMemoryContextPackingBundleOptions::new(memory.context.clone(), texts, policy)
            .named("packing"),
    );
    let _packed = packing.packed_context.subscribe(|_| {});
    let _errors = packing.errors.subscribe(|_| {});
    let _status = packing.status.subscribe(|_| {});

    assert_eq!(packing.packed_context.cache().unwrap().text, "near text");
    assert_eq!(
        packing
            .errors
            .cache()
            .unwrap()
            .iter()
            .map(|error| error.code)
            .collect::<Vec<_>>(),
        vec![
            AgenticMemoryErrorCode::DuplicateTextProjection,
            AgenticMemoryErrorCode::InvalidTextProjection,
        ]
    );
    assert_eq!(
        packing.status.cache().unwrap().state,
        AgenticMemoryStatusState::Partial
    );
}
