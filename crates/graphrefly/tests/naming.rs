//! R-naming: local identities cannot masquerade as mounted addresses.
use graphrefly::{graph, topology, GraphNodeOpts, TopologyGroupOptions};
use std::panic::{catch_unwind, AssertUnwindSafe};

const INVALID_NAME: &str = "name must not contain '::'";

fn rejected(f: impl FnOnce()) {
    rejected_with(f, INVALID_NAME);
}

fn rejected_with(f: impl FnOnce(), expected: &str) {
    let error = catch_unwind(AssertUnwindSafe(f)).expect_err("reserved separator must fail");
    let text = error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .unwrap();
    assert_eq!(text, expected);
}

#[test]
fn every_local_name_boundary_rejects_mount_separator_without_registration() {
    let g = graph();
    for name in ["a::b", "::", "module/child::value", "#a::b"] {
        rejected(|| {
            g.state_opts(1, GraphNodeOpts::named(name));
        });
        rejected(|| {
            g.state_empty_opts::<i32>(GraphNodeOpts::named(name));
        });
        rejected(|| {
            g.node_opts::<i32, _>(vec![], |_| {}, GraphNodeOpts::named(name));
        });
        rejected(|| {
            g.producer_opts::<i32, _>(|_| {}, GraphNodeOpts::named(name));
        });
        rejected(|| {
            g.derived_opts::<i32, _>(vec![], |_| Some(1), GraphNodeOpts::named(name));
        });
        rejected(|| {
            g.effect_opts(vec![], |_| None, GraphNodeOpts::named(name));
        });
        rejected(|| {
            g.init_node(graphrefly::of(1i32), vec![], GraphNodeOpts::named(name));
        });
        rejected(|| {
            topology::group_opts(&g, TopologyGroupOptions::named(name));
        });
        rejected(|| {
            topology::mount(&g, graph(), name);
        });
        assert!(g.describe().is_empty());
        assert!(topology::snapshot(&g)
            .subgraphs
            .unwrap_or_default()
            .is_empty());
    }
    let child = graph();
    child.state_opts(7, GraphNodeOpts::named("user-label.withDot/value"));
    topology::mount(&g, child, "orders");
    assert_eq!(
        g.find("orders::user-label.withDot/value")
            .unwrap()
            .as_node::<i32>()
            .cache(),
        Some(7)
    );
}

#[allow(dead_code)]
#[path = "../examples/node_naming_probe.rs"]
mod probe;

fn words(segment: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut word = String::new();
    for c in segment.chars().filter(|c| !c.is_ascii_digit()) {
        if c.is_ascii_uppercase() && !word.is_empty() {
            out.push(std::mem::take(&mut word));
        }
        word.push(c.to_ascii_lowercase());
    }
    if !word.is_empty() {
        out.push(word);
    }
    out
}
fn scan(id: &str) {
    let segments: Vec<_> = id.split('/').collect();
    for segment in &segments {
        assert!(
            segment.starts_with(|c: char| c.is_ascii_lowercase()),
            "not camelCase: {id}"
        );
        assert!(
            segment.chars().all(|c| c.is_ascii_alphanumeric()),
            "not camelCase: {id}"
        );
    }
    for pair in segments.windows(2) {
        let prefix = words(pair[0]);
        let suffix = words(pair[1]);
        assert!(
            !suffix.iter().any(|word| prefix.contains(word)),
            "repeated module word: {id}"
        );
    }
}
fn scan_graph(g: &graphrefly::Graph) {
    for entry in g.describe() {
        scan(&entry.id);
    }
}
fn input<T: 'static>(g: &graphrefly::Graph, name: &str) -> graphrefly::State<T> {
    g.state_empty_opts(GraphNodeOpts::named(name))
}

#[test]
fn shared_modules_match_complete_inventories_and_corresponding_roles() {
    let actual = probe::verify();
    for module in actual
        .as_object()
        .unwrap()
        .values()
        .filter(|value| !value.is_null())
    {
        for id in module["ids"].as_array().unwrap() {
            scan(id.as_str().unwrap());
        }
    }
}

#[test]
fn default_collections_and_dynamic_views_use_camel_case_names() {
    use graphrefly::*;
    let g = graph();
    let source = input::<i32>(&g, "input");
    let list = reactive_list::<i32>(&g, vec![], ReactiveListOptions::default());
    let _bind = list.append_from(&source);
    let log = reactive_log::<i32>(&g, vec![], ReactiveLogOptions::default());
    let _bind = log.attach(&source);
    let _ = log.page(0, 1);
    let _ = log.tail(1);
    let _ = log.tail(2);
    let _ = log.slice(0, None);
    let _ = log.scan(0, |acc, value| acc + value);
    let map = reactive_map::<String, i32>(&g, vec![], ReactiveMapOptions::default());
    let _ = map.select(|_, _| true);
    let index = reactive_index::<i32, String, i32>(&g, vec![], ReactiveIndexOptions::default());
    let _ = index.range(0, 1);
    let _ = merge_reactive_logs(&g, vec![log]);
    scan_graph(&g);
    assert!(g.find("reactiveList/request").is_some());
    assert!(g.find("reactiveLog/tail1").is_some());
    let _other = reactive_list::<i32>(&g, vec![], ReactiveListOptions::default());
    assert!(g.find("reactiveList1/delta").is_some());
}

#[test]
fn default_memory_modules_use_camel_case_role_names() {
    use graphrefly::*;
    let g = graph();
    let _ = memory_retrieval_bundle::<String>(
        &g,
        MemoryRetrievalBundleOptions::new(input(&g, "fragments"), input(&g, "query")),
    );
    let _ = knowledge_graph_reducer_bundle(
        &g,
        KnowledgeGraphReducerBundleOptions::new(input(&g, "assertions")),
    );
    let records = input::<Vec<AgenticMemoryRecord<String>>>(&g, "records");
    let memory = agentic_memory_bundle(
        &g,
        AgenticMemoryBundleOptions::new(records.clone(), input(&g, "memoryQuery")),
    );
    let _ = agentic_memory_kg_projection_bundle(
        &g,
        AgenticMemoryKgProjectionBundleOptions::new(records.clone(), input(&g, "draftInput")),
    );
    let _ = agentic_memory_retention_bundle(
        &g,
        AgenticMemoryRetentionBundleOptions::new(records, input(&g, "retentionCommands")),
    );
    let _ = agentic_memory_consolidation_bundle::<String>(
        &g,
        AgenticMemoryConsolidationBundleOptions::new(input(&g, "requests"), input(&g, "outcomes")),
    );
    let _ = agentic_memory_context_packing_bundle(
        &g,
        AgenticMemoryContextPackingBundleOptions::new(
            memory.context,
            input(&g, "texts"),
            input(&g, "policy"),
        ),
    );
    scan_graph(&g);
    for id in [
        "agenticMemoryPacking/context",
        "agenticMemoryPacking/projection",
        "agenticMemoryRetention/projection",
        "agenticMemoryConsolidation/projection",
        "agenticMemoryKgProjection",
        "memoryRetrieval/snapshot",
        "knowledgeGraph/snapshot",
    ] {
        assert!(g.find(id).is_some(), "missing corresponding role {id}");
    }
    assert!(g.find("agenticMemoryConsolidation/drafts").is_some());
    assert!(g.find("agenticMemoryRetention/requests").is_some());
}

#[test]
fn boundary_colons_fail_but_internal_colon_addresses_round_trip() {
    for name in [":a", "a:", ":"] {
        let g = graph();
        for kind in ["node", "mount", "group"] {
            let failure = catch_unwind(AssertUnwindSafe(|| match kind {
                "node" => {
                    g.state_opts(1, GraphNodeOpts::named(name));
                }
                "mount" => topology::mount(&g, graph(), name),
                _ => {
                    topology::group_opts(&g, TopologyGroupOptions::named(name));
                }
            }))
            .unwrap_err();
            let text = failure
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| failure.downcast_ref::<&str>().copied())
                .unwrap();
            assert_eq!(text, "name must not start or end with ':'");
            assert!(g.describe().is_empty());
        }
    }
    let parent = graph();
    let child = graph();
    child.state_opts(7, GraphNodeOpts::named("a:b"));
    topology::mount(&parent, child, "c:d");
    assert_eq!(
        parent.find("c:d::a:b").unwrap().as_node::<i32>().cache(),
        Some(7)
    );
}

#[test]
fn default_process_messaging_readiness_and_environment_modules_are_scanned() {
    use graphrefly::*;
    let g = graph();
    let _ = cqrs::<String, String>(&g);
    let _ = process_bundle::<String, i32, String, String>(
        &g,
        ProcessBundleOptions::new(0, |_, state| ProcessReduction::new(state)),
    );
    let bus = message_bus::<WorkQueueSubmit<String>>(&g, MessageBusOptions::default());
    let queue = work_queue(
        &g,
        WorkQueueOptions::new("jobs", bus, "topic", "subscription"),
    );
    let _ = scheduled_readiness_projector(&g, ScheduledReadinessOptions::new(vec![]));
    let _ = work_queue_scheduled_readiness_projector::<String>(
        &g,
        WorkQueueScheduledReadinessOptions::new(vec![queue.records]),
    );
    let _ = work_queue_readiness_handoff_projector::<String>(
        &g,
        WorkQueueReadinessHandoffOptions::new(vec![], vec![]),
    );
    let source = input::<String>(&g, "environmentInput").into_node();
    let _ = to_http(&g, &source, |_| HttpRequest::get("https://example.invalid"));
    let _ = to_process(&g, &source, |_| {
        graphrefly::environment::ProcessCommand::new("unused")
    });
    let _ = websocket_session(&g, WebSocketRequest::new("wss://example.invalid"));
    for id in [
        "webSocketSession/events",
        "process/effectRequests",
        "process/errors",
    ] {
        assert!(g.find(id).is_some(), "missing corresponding role {id}");
    }
    scan_graph(&g);
}

#[test]
fn default_remote_and_persistence_modules_are_scanned() {
    use graphrefly::*;
    use std::rc::Rc;
    let g = graph();
    let outgoing = wire_bridge::<RemoteCallRequest<String>, RemoteCallResponse<String>>(
        &g,
        WireBridgeOptions::new("s1"),
    );
    let _ = remote_call(&g, &outgoing);
    let incoming = wire_bridge::<RemoteCallResponse<String>, RemoteCallRequest<String>>(
        &g,
        WireBridgeOptions::named("s2", "incoming"),
    );
    let _ = remote_responder(&g, &incoming, RemoteResponderOptions::default());
    let list = reactive_list::<i32>(&g, vec![], ReactiveListOptions::default());
    let _ = persist_reactive_list(
        &list,
        PersistReactiveCollectionOptions::new(Rc::new(memory_kv()), "storage"),
    )
    .unwrap();
    scan_graph(&g);
}

#[test]
fn module_prefix_admission_happens_before_appending_roles_or_registering_nodes() {
    use graphrefly::*;
    for name in ["bad::name", ":bad", "bad:"] {
        let error = if name.contains("::") {
            INVALID_NAME
        } else {
            "name must not start or end with ':'"
        };
        let g = graph();
        let request = input::<String>(&g, "request");
        let records = input::<Vec<AgenticMemoryRecord<String>>>(&g, "records");
        let query = input::<MemoryRetrievalQuery>(&g, "query");
        let before = g.describe().len();
        rejected_with(
            || {
                let _ = reactive_list::<i32>(&g, vec![], ReactiveListOptions::named(name));
            },
            error,
        );
        rejected_with(
            || {
                let _ = wire_bridge::<String, String>(&g, WireBridgeOptions::named("s", name));
            },
            error,
        );
        rejected_with(
            || {
                let mut opts =
                    ReactiveCascadingCacheOptions::<String>::new(request.clone(), vec![]);
                opts.name = Some(name.into());
                let _ = reactive_cascading_cache(&g, opts);
            },
            error,
        );
        rejected_with(
            || {
                let _ = agentic_memory_bundle(
                    &g,
                    AgenticMemoryBundleOptions::new(records.clone(), query.clone()).named(name),
                );
            },
            error,
        );
        assert_eq!(g.describe().len(), before);
    }
}

#[test]
fn stratify_names_user_rules_below_branches_without_diagnostic_collision() {
    use graphrefly::*;
    let g = graph();
    let source = g.state_opts(1, GraphNodeOpts::named("source"));
    let bundle = stratify(
        &g,
        &source,
        vec![StratifyRule::new("rules", ())],
        |_, _| true,
        StratifyOptions::default(),
    );
    let _keep = bundle.branches["rules"].subscribe(|_| {});
    assert_eq!(bundle.branches["rules"].cache(), Some(1));
    assert!(g.find("branch/rules").is_some());
    assert!(g.find("branch/branches/rules").is_some());
    scan_graph(&g);
    let before = g.describe().len();
    for name in ["bad::name", ":bad", "bad:"] {
        let expected = if name.contains("::") {
            INVALID_NAME
        } else {
            "name must not start or end with ':'"
        };
        rejected_with(
            || {
                let _ = stratify(
                    &g,
                    &source,
                    vec![StratifyRule::new(name, ())],
                    |_, _| true,
                    StratifyOptions::default(),
                );
            },
            expected,
        );
        rejected_with(
            || {
                let _ = stratify(
                    &g,
                    &source,
                    vec![],
                    |_: &(), _: &i32| true,
                    StratifyOptions {
                        prefix: name.into(),
                        ..StratifyOptions::default()
                    },
                );
            },
            expected,
        );
        assert_eq!(g.describe().len(), before);
    }
}

#[test]
fn default_optional_recipes_and_memory_persistence_are_scanned() {
    use graphrefly::*;
    use graphrefly::{
        cqrs::messaging::*, cqrs::work_queue::*, process::messaging::*, process::work_queue::*,
    };
    use std::rc::Rc;
    let g = graph();
    let _ = cqrs_work_queue_recipe::<String>(
        &g,
        CqrsWorkQueueRecipeOptions::new(input(&g, "queuedCommands"), input(&g, "cqrsStatus")),
    );
    let _ = process_work_queue_recipe::<String>(
        &g,
        ProcessWorkQueueRecipeOptions::new(input(&g, "queuedEffects")),
    );
    let process = process_bundle::<ProcessEffectCommandPayload<String>, i32, String, String>(
        &g,
        ProcessBundleOptions::new(0, |_, state| ProcessReduction::new(state)),
    );
    let _ = process_effect_runner(
        &g,
        &process,
        ProcessEffectRunnerOptions::new(vec![input(&g, "effectOutcomes").into_node()]),
    );
    let _ = cqrs_messaging_recipe::<String, String, String>(
        &g,
        CqrsMessagingRecipeOptions::new(
            input(&g, "cqrsDeliveries"),
            input(&g, "cqrsMessageStatus"),
            CqrsMessagingPolicy::new(|_, _| None),
        ),
    );
    let _ = process_messaging_recipe::<String, String, String>(
        &g,
        ProcessMessagingRecipeOptions::new(
            input(&g, "processDeliveries"),
            input(&g, "processMessageStatus"),
            ProcessMessagingPolicy::new(|_, _| None),
        ),
    );
    let _ = persist_agentic_memory_records(
        &g,
        &input(&g, "storedRecords").into_node(),
        PersistAgenticMemoryRecordsOptions::new(Rc::new(memory_kv()), "memoryStorage"),
    )
    .unwrap();
    scan_graph(&g);
}

#[test]
fn persistence_default_node_scopes_do_not_rename_storage_keys() {
    use graphrefly::*;
    use std::rc::Rc;
    let g = graph();
    let store = Rc::new(memory_kv());
    let records = g.state_opts(vec![], GraphNodeOpts::named("records"));
    for _ in 0..2 {
        let mut options = PersistAgenticMemoryRecordsOptions::new(store.clone(), "unused");
        options.storage_prefix = None;
        let _ = persist_agentic_memory_records(&g, &records, options).unwrap();
    }
    let key = agentic_memory_records_snapshot_key("agenticMemoryRecords.persistence").unwrap();
    assert!(store.get(&key).unwrap().is_some());
    assert!(g.find("agenticMemoryRecords/persistence/ready").is_some());
    assert!(g.find("agenticMemoryRecords/persistence1/ready").is_some());
    let list = reactive_list::<i32>(&g, vec![], ReactiveListOptions::default());
    for key in ["first", "second"] {
        let mut options = PersistReactiveCollectionOptions::new(Rc::new(memory_kv()), "unused");
        options.storage_prefix = None;
        options.snapshot_key = Some(key.into());
        let _ = persist_reactive_list(&list, options).unwrap();
    }
    assert!(g.find("reactiveList/persistence/ready").is_some());
    assert!(g.find("reactiveList/persistence1/ready").is_some());
    scan_graph(&g);
}

#[test]
fn message_bus_lazy_default_scopes_separate_user_keys_and_validate_before_mutation() {
    use graphrefly::*;
    let g = graph();
    let bus = message_bus::<String>(&g, MessageBusOptions::default());
    let _ = bus.topic("catalog");
    let _ = bus.topic("catalog");
    let _ = bus.catalog();
    let _ = bus.catalog();
    let _ = bus.subscription(MessageBusSubscriptionOptions::new("catalog", "worker"));
    let _ = bus.subscription(MessageBusSubscriptionOptions::new("catalog", "worker"));
    for id in [
        "messageBus/topics/catalog",
        "messageBus/topics/catalog1",
        "messageBus/catalog",
        "messageBus/catalog1",
        "messageBus/subscriptions/catalog/worker/available",
        "messageBus/subscriptions/catalog/worker1/available",
    ] {
        assert!(g.find(id).is_some(), "missing lazy role {id}");
    }
    let before = g.describe().len();
    for name in ["bad::name", ":bad", "bad:"] {
        let error = if name.contains("::") {
            INVALID_NAME
        } else {
            "name must not start or end with ':'"
        };
        rejected_with(
            || {
                let _ = bus.topic(name);
            },
            error,
        );
        rejected_with(
            || {
                let _ = bus.subscription(MessageBusSubscriptionOptions::new("safe", name));
            },
            error,
        );
        assert_eq!(g.describe().len(), before);
    }
    scan_graph(&g);
}
