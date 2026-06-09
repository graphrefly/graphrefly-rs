use std::cell::RefCell;
use std::rc::Rc;

use graphrefly::process::ProcessCommand;
use graphrefly::{
    graph, GraphCheckpointValue, Message, ProcessAuditOutcome, ProcessBundleOptions,
    ProcessEffectRequestDraft, ProcessErrorCode, ProcessEventDraft, ProcessReduction,
    ProcessStatusState, Values,
};
use serde::{Deserialize, Serialize};

fn collect_data<T: Clone + 'static>(node: &graphrefly::Node<T>) -> Rc<RefCell<Vec<T>>> {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let seen_sink = seen.clone();
    let _keep = node.subscribe(move |msg| {
        if let Message::Data(value) = msg {
            if let Some(value) = value.as_ref().downcast_ref::<T>() {
                seen_sink.borrow_mut().push(value.clone());
            }
        }
    });
    seen
}

fn last_or_prev(values: &Values<'_>, i: usize) -> Option<Rc<i32>> {
    values
        .batches::<i32>(i)
        .last()
        .and_then(|wave| wave.last().cloned())
        .or_else(|| values.prev::<i32>(i))
}

#[derive(Debug, Clone, PartialEq)]
struct CommandPayload {
    amount: i32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct ProcessState {
    total: i32,
}

#[derive(Debug, Clone, PartialEq)]
struct EventPayload {
    total: i32,
}

#[derive(Debug, Clone, PartialEq)]
struct EffectPayload {
    url: String,
}

#[test]
fn process_bundle_reduces_commands_into_graph_visible_facts() {
    let g = graph();
    let process =
        graphrefly::process_bundle::<CommandPayload, ProcessState, EventPayload, EffectPayload>(
            &g,
            ProcessBundleOptions::<CommandPayload, ProcessState, EventPayload, EffectPayload>::new(
                ProcessState { total: 0 },
                |command, state| {
                    let total = state.total + command.payload.amount;
                    ProcessReduction::new(ProcessState { total })
                        .with_events(vec![ProcessEventDraft::new(
                            "amount-added",
                            EventPayload { total },
                        )])
                        .with_effects(vec![ProcessEffectRequestDraft::new(
                            "notify",
                            EffectPayload {
                                url: format!(
                                    "/orders/{}",
                                    command.process_id.as_deref().unwrap_or("none")
                                ),
                            },
                        )])
                },
            )
            .named("order")
            .with_now(|| 123),
        );
    let events = collect_data(&process.events);
    let effect_requests = collect_data(&process.effect_request);
    let status = collect_data(&process.status);
    let audit = collect_data(&process.audit);

    process.dispatch(ProcessCommand {
        id: "cmd-1".to_owned(),
        command_type: "add".to_owned(),
        payload: CommandPayload { amount: 7 },
        process_id: Some("p-1".to_owned()),
        correlation_id: Some("corr-1".to_owned()),
        causation_id: None,
    });

    assert_eq!(process.state.cache(), Some(ProcessState { total: 7 }));
    let event = events.borrow().last().unwrap().clone();
    assert_eq!(event.id, "cmd-1:event:1");
    assert_eq!(event.event_type, "amount-added");
    assert_eq!(event.seq, 1);
    assert_eq!(event.cursor, 1);
    assert_eq!(event.command_id, "cmd-1");
    assert_eq!(event.command_type, "add");
    assert_eq!(event.payload, EventPayload { total: 7 });
    assert_eq!(event.timestamp_ms, 123);

    let effect = effect_requests.borrow().last().unwrap().clone();
    assert_eq!(effect.id, "cmd-1:effect:1");
    assert_eq!(effect.effect_type, "notify");
    assert_eq!(effect.seq, 1);
    assert_eq!(effect.cursor, 1);
    assert_eq!(
        effect.payload,
        EffectPayload {
            url: "/orders/p-1".to_owned()
        }
    );

    let status = status.borrow().last().unwrap().clone();
    assert_eq!(status.state, ProcessStatusState::Accepted);
    assert_eq!(status.command_id.as_deref(), Some("cmd-1"));
    assert_eq!(status.event_count, 1);
    assert_eq!(status.effect_count, 1);
    assert_eq!(status.cursor.audit_seq, 0);

    let audit = audit.borrow().last().unwrap().clone();
    assert_eq!(audit.seq, 1);
    assert_eq!(audit.outcome, ProcessAuditOutcome::Success);
    assert_eq!(audit.event_ids, vec!["cmd-1:event:1".to_owned()]);
    assert_eq!(audit.effect_ids, vec!["cmd-1:effect:1".to_owned()]);
    assert_eq!(process.cursor.cache().unwrap().audit_seq, 1);
    assert!(process.error.cache().is_none());
}

#[test]
fn process_cursor_is_command_attempt_high_water_after_audit_closes() {
    let g = graph();
    let process = graphrefly::process_bundle::<bool, u64, String, String>(
        &g,
        ProcessBundleOptions::new(0, |command, state| {
            if command.payload {
                ProcessReduction::new(state + 1)
                    .with_events(vec![ProcessEventDraft::new("accepted", "event".to_owned())])
                    .with_effects(vec![ProcessEffectRequestDraft::new(
                        "notify",
                        "effect".to_owned(),
                    )])
            } else {
                panic!("nope");
            }
        }),
    );

    process.dispatch(ProcessCommand::new("cmd-1", "run", true));
    assert_eq!(process.events.cache().unwrap().cursor, 1);
    assert_eq!(process.effect_request.cache().unwrap().cursor, 1);
    assert_eq!(process.status.cache().unwrap().cursor.audit_seq, 0);
    assert_eq!(process.cursor.cache().unwrap().audit_seq, 1);

    process.dispatch(ProcessCommand::new("cmd-2", "run", false));
    assert_eq!(process.error.cache().unwrap().cursor.audit_seq, 1);
    assert_eq!(process.cursor.cache().unwrap().audit_seq, 2);
    assert_eq!(process.cursor.cache().unwrap().command_count, 2);
    assert_eq!(process.cursor.cache().unwrap().error_count, 1);
}

#[test]
fn process_bundle_topology_is_describe_visible_with_declared_deps() {
    let g = graph();
    let _process = graphrefly::process_bundle::<String, String, String, String>(
        &g,
        ProcessBundleOptions::new("start".to_owned(), |_command, state| {
            ProcessReduction::new(state)
        }),
    );

    let snap = g.describe();
    let process_command = snap
        .nodes
        .iter()
        .find(|node| node.id == "process/command")
        .unwrap();
    let process_runtime = snap
        .nodes
        .iter()
        .find(|node| node.id == "process/runtime")
        .unwrap();
    let process_effect = snap
        .nodes
        .iter()
        .find(|node| node.id == "process/effect_request")
        .unwrap();
    assert_eq!(process_command.factory, "processCommand");
    assert_eq!(process_runtime.deps, vec!["process/command"]);
    assert_eq!(process_effect.deps, vec!["process/runtime"]);
    assert!(snap
        .edges
        .iter()
        .any(|edge| { edge.from == "process/command" && edge.to == "process/runtime" }));
}

#[test]
fn process_bundle_surfaces_reducer_failures_without_mutating_state() {
    let g = graph();
    let process = graphrefly::process_bundle::<bool, ProcessState, String, String>(
        &g,
        ProcessBundleOptions::<bool, ProcessState, String, String>::new(
            ProcessState { total: 0 },
            |command, mut state| {
                state.total += 100;
                if !command.payload {
                    panic!("process failed");
                }
                ProcessReduction::new(ProcessState {
                    total: state.total + 1,
                })
            },
        ),
    );
    let errors = collect_data(&process.error);

    process.dispatch(ProcessCommand::new("bad", "run", false));
    process.dispatch(ProcessCommand::new("good", "run", true));

    assert_eq!(
        errors.borrow().last().unwrap().code,
        ProcessErrorCode::ReducerThrew
    );
    assert_eq!(process.state.cache(), Some(ProcessState { total: 101 }));
    assert_eq!(
        process.status.cache().unwrap().state,
        ProcessStatusState::Accepted
    );
}

#[test]
fn process_bundle_keeps_runtime_state_checkpoint_friendly() {
    let g = graph();
    let process = graphrefly::process_bundle::<i32, ProcessState, String, String>(
        &g,
        ProcessBundleOptions::new(ProcessState { total: 0 }, |command, state| {
            ProcessReduction::new(ProcessState {
                total: state.total + command.payload,
            })
            .with_events(vec![ProcessEventDraft::new("added", "event".to_owned())])
        })
        .named("orders"),
    );

    process.dispatch(ProcessCommand::new("cmd-1", "add", 3));
    process.command.down(vec![Message::Invalidate]);

    let checkpoint = g.checkpoint().expect("checkpoint succeeds");
    let runtime = checkpoint
        .nodes
        .iter()
        .find(|node| node.id == "orders/runtime")
        .expect("runtime node checkpointed");
    let data = match &runtime.ctx_state.value {
        GraphCheckpointValue::Data { data } => data,
        GraphCheckpointValue::Sentinel => panic!("runtime ctx state is persisted"),
    };
    assert!(runtime.ctx_state.persist);
    assert_eq!(data["eventSeq"], 1);
    assert_eq!(data["commandCount"], 1);
    assert_eq!(data["seenEventIds"][0], "cmd-1:event:1");
    assert_eq!(data["state"]["total"], 3);
}

#[test]
fn process_bundle_state_cache_mutation_does_not_alias_next_reducer_input() {
    let g = graph();
    let process = graphrefly::process_bundle::<i32, ProcessState, String, String>(
        &g,
        ProcessBundleOptions::new(ProcessState { total: 0 }, |command, state| {
            ProcessReduction::new(ProcessState {
                total: state.total + command.payload,
            })
        }),
    );

    process.dispatch(ProcessCommand::new("cmd-1", "add", 1));
    let mut cache = process.state.cache().unwrap();
    cache.total = 1000;
    assert_eq!(cache.total, 1000);
    process.dispatch(ProcessCommand::new("cmd-2", "add", 1));

    assert_eq!(process.state.cache(), Some(ProcessState { total: 2 }));
}

#[test]
fn process_bundle_rejects_malformed_and_duplicate_fact_ids() {
    let g = graph();
    let process = graphrefly::process_bundle::<&'static str, u64, &'static str, &'static str>(
        &g,
        ProcessBundleOptions::new(0, |command, state| match command.payload {
            "event" => ProcessReduction::new(state + 1).with_events(vec![ProcessEventDraft {
                id: Some("same".to_owned()),
                event_type: "seen".to_owned(),
                payload: "event",
                process_id: None,
                correlation_id: None,
                causation_id: None,
            }]),
            "effect" => {
                ProcessReduction::new(state + 1).with_effects(vec![ProcessEffectRequestDraft {
                    id: Some("same-effect".to_owned()),
                    effect_type: "call".to_owned(),
                    payload: "effect",
                    process_id: None,
                    correlation_id: None,
                    causation_id: None,
                }])
            }
            _ => ProcessReduction::new(state),
        }),
    );

    process.dispatch(ProcessCommand::new("", "run", "event"));
    assert_eq!(
        process.error.cache().unwrap().code,
        ProcessErrorCode::MalformedCommand
    );
    process.dispatch(ProcessCommand::new("event-1", "run", "event"));
    process.dispatch(ProcessCommand::new("event-2", "run", "event"));
    assert_eq!(
        process.error.cache().unwrap().message,
        "process_bundle: duplicate event 'same'"
    );
    process.dispatch(ProcessCommand::new("effect-1", "run", "effect"));
    process.dispatch(ProcessCommand::new("effect-2", "run", "effect"));
    assert_eq!(
        process.error.cache().unwrap().message,
        "process_bundle: duplicate effect 'same-effect'"
    );
}

#[test]
fn process_bundle_state_can_feed_regular_graph_reductions() {
    let g = graph();
    let process = graphrefly::process_bundle::<i32, i32, String, String>(
        &g,
        ProcessBundleOptions::new(0, |command, state| {
            ProcessReduction::new(state + command.payload)
        }),
    );
    let doubled = g.derived_opts(
        vec![process.state.erased()],
        |values: &Values<'_>| last_or_prev(values, 0).map(|value| *value * 2),
        graphrefly::GraphNodeOpts::named("process/doubled"),
    );
    let doubled_values = collect_data(&doubled);

    process.dispatch(ProcessCommand::new("cmd-1", "add", 4));

    assert_eq!(doubled_values.borrow().last().cloned(), Some(8));
    assert_eq!(doubled.cache(), Some(8));
}
