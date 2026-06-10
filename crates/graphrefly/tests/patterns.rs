use graphrefly::{
    graph, memory_retrieval_bundle, GraphNodeOpts, MemoryFragment, MemoryRetrievalBundleOptions,
    MemoryRetrievalErrorCode, MemoryRetrievalQuery, MemoryRetrievalStatusState,
};

fn fragment(id: &str, payload: &str) -> MemoryFragment<String> {
    MemoryFragment {
        id: id.to_owned(),
        payload: payload.to_owned(),
        t_ns: 1,
        valid_from: None,
        valid_to: None,
        confidence: 1.0,
        tags: vec!["policy".to_owned()],
        sources: Vec::new(),
        embedding: None,
        parent_fragment_id: None,
        provenance: None,
    }
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
    let mut far = fragment("far", "far");
    far.confidence = 0.95;
    far.embedding = Some(vec![0.0, 1.0]);
    let mut other = fragment("other", "other");
    other.tags = vec!["other".to_owned()];
    other.embedding = Some(vec![1.0, 0.0]);
    let fragments = g.state_opts(vec![near, far, other], GraphNodeOpts::named("fragments"));
    let query = g.state_opts(
        MemoryRetrievalQuery {
            tags: vec!["policy".to_owned()],
            vector: Some(vec![1.0, 0.0]),
            limit: Some(2),
            ..MemoryRetrievalQuery::default()
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
        vec!["near", "far"],
    );

    let _status = bundle.status.subscribe(|_| {});
    assert_eq!(
        bundle.status.cache().unwrap().state,
        MemoryRetrievalStatusState::Ready
    );
    assert_eq!(bundle.status.cache().unwrap().cursor.result_count, 2);

    query.set(MemoryRetrievalQuery {
        tags: vec!["other".to_owned()],
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
