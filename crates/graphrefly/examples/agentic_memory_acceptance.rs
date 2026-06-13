use graphrefly::{
    agentic_memory_bundle, agentic_memory_context_packing_bundle, graph, memory_append_log,
    memory_kv, persist_agentic_memory_records, AgenticMemoryArtifactKind,
    AgenticMemoryBundleOptions, AgenticMemoryContextPackingBundleOptions,
    AgenticMemoryContextPackingPolicy, AgenticMemoryKind, AgenticMemoryPersistenceLevel,
    AgenticMemoryRecord, AgenticMemoryScope, AgenticMemoryTextProjection, AppendLogReadOptions,
    AppendLogStorageTier, GraphNodeOpts, MemoryFragment, MemoryRetrievalQuery,
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
            tags: vec!["project".to_owned()],
            sources: Vec::new(),
            embedding: None,
            parent_fragment_id: None,
            provenance: None,
        },
        kind: AgenticMemoryKind::Semantic,
        persistence_level: AgenticMemoryPersistenceLevel::Project,
        artifact_kind: AgenticMemoryArtifactKind::Insight,
        scope: Some(AgenticMemoryScope {
            session_id: None,
            project_id: Some("graphrefly-rs".to_owned()),
            user_id: None,
            tenant_id: None,
        }),
    }
}

fn main() {
    let g = graph();
    let records = g.state_opts(
        vec![record(
            "project-memory",
            "Agentic memory is graph-shaped product surface, not runtime magic.",
        )],
        GraphNodeOpts::named("records"),
    );

    let snapshots = Rc::new(memory_kv::<Value>());
    let changes = Rc::new(memory_append_log::<Value>("example-agentic/changes"));
    let persistence = persist_agentic_memory_records(
        &records,
        PersistAgenticMemoryRecordsOptions {
            graph: Some(g.clone()),
            name: Some("recordsPersistence".to_owned()),
            storage_prefix: Some("example-agentic".to_owned()),
            snapshot_key: None,
            snapshot_store: snapshots,
            change_log: Some(changes.clone()),
            snapshot_on_attach: true,
        },
    )
    .expect("attach agentic memory persistence");

    let query = g.state_opts(
        MemoryRetrievalQuery {
            tags: vec!["project".to_owned()],
            ..Default::default()
        },
        GraphNodeOpts::named("query"),
    );
    let memory = agentic_memory_bundle(
        &g,
        AgenticMemoryBundleOptions::new(records.clone(), query).named("memory"),
    );
    let texts = g.state_opts(
        vec![AgenticMemoryTextProjection {
            fragment_id: "project-memory".to_owned(),
            text: "Agentic memory is graph-shaped product surface, not runtime magic.".to_owned(),
        }],
        GraphNodeOpts::named("texts"),
    );
    let policy = g.state_opts(
        AgenticMemoryContextPackingPolicy {
            max_chars: Some(512),
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
    let _packed = packing.packed_context.subscribe(|_| {});
    let _ready = persistence.ready.subscribe(|_| {});

    println!("{}", packing.packed_context.cache().unwrap().text);

    records.set(vec![record(
        "project-memory-updated",
        "Persistence sidecars record public API updates as append-only change frames.",
    )]);

    println!(
        "change frames: {}",
        changes
            .read(AppendLogReadOptions::default())
            .expect("read change log")
            .len()
    );

    persistence.flush().expect("flush agentic memory records");
    persistence.dispose();
}
