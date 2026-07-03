use std::cell::RefCell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;

use graphrefly::{
    cqrs_command_handler, cqrs_projection, cqrs_with_options, graph, CqrsAuditOutcome, CqrsCommand,
    CqrsDedupePolicy, CqrsErrorCode, CqrsEventDraft, CqrsOptions, CqrsProjectionErrorCode,
    CqrsProjectionFrame, CqrsProjectionOptions, CqrsStatusState, Message, Values,
};

fn last_or_prev(values: &Values<'_>, i: usize) -> Option<Rc<i32>> {
    values
        .batches::<i32>(i)
        .last()
        .and_then(|wave| wave.last().cloned())
        .or_else(|| values.prev::<i32>(i))
}

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

fn cqrs_event_id(command_id: &str, seq: u64) -> String {
    let parts = [command_id.to_owned(), seq.to_string()];
    format!(
        "cqrs-event:{}",
        serde_json::to_string(&parts).expect("test tuple key serializes")
    )
}

#[test]
fn cqrs_dispatch_emits_ordered_graph_visible_facts() {
    let g = graph();
    let app = cqrs_with_options::<String, String>(
        &g,
        CqrsOptions::named("orders")
            .with_events(["OrderPlaced", "OrderConfirmed"])
            .with_now(|| 20)
            .with_handlers(vec![cqrs_command_handler(
                "PlaceOrder",
                |command: &CqrsCommand<String>| {
                    vec![
                        CqrsEventDraft::new("OrderPlaced", command.payload.clone()),
                        CqrsEventDraft::new("OrderConfirmed", command.payload.clone()),
                    ]
                },
            )]),
    );
    let events = collect_data(&app.events);
    let status = collect_data(&app.status);
    let audit = collect_data(&app.audit);

    app.dispatch(CqrsCommand::new("cmd-1", "PlaceOrder", "o1".to_owned()));

    let events = events.borrow();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].id, cqrs_event_id("cmd-1", 1));
    assert_eq!(events[0].seq, 1);
    assert_eq!(events[0].timestamp_ms, 20);
    assert_eq!(events[1].id, cqrs_event_id("cmd-1", 2));
    assert_eq!(events[1].seq, 2);
    assert_eq!(
        status.borrow().last().unwrap().state,
        CqrsStatusState::Accepted
    );
    let audit = audit.borrow();
    assert_eq!(audit.last().unwrap().outcome, CqrsAuditOutcome::Success);
    assert_eq!(
        audit.last().unwrap().event_ids,
        vec![cqrs_event_id("cmd-1", 1), cqrs_event_id("cmd-1", 2)]
    );
}

#[test]
fn cqrs_bounded_dedupe_window_is_explicit() {
    let g = graph();
    let app = cqrs_with_options::<String, String>(
        &g,
        CqrsOptions::named("orders")
            .with_events(["OrderPlaced"])
            .with_dedupe(CqrsDedupePolicy::bounded(1, 1))
            .with_handlers(vec![cqrs_command_handler(
                "PlaceOrder",
                |command: &CqrsCommand<String>| {
                    let mut event = CqrsEventDraft::new("OrderPlaced", command.payload.clone());
                    event.id = Some(command.payload.clone());
                    vec![event]
                },
            )]),
    );
    let events = collect_data(&app.events);
    let errors = collect_data(&app.errors);
    let cursor = collect_data(&app.cursor);

    app.dispatch(CqrsCommand::new(
        "cmd-1",
        "PlaceOrder",
        "event-1".to_owned(),
    ));
    app.dispatch(CqrsCommand::new(
        "cmd-2",
        "PlaceOrder",
        "event-2".to_owned(),
    ));
    app.dispatch(CqrsCommand::new(
        "cmd-1",
        "PlaceOrder",
        "event-1".to_owned(),
    ));
    app.dispatch(CqrsCommand::new(
        "cmd-1",
        "PlaceOrder",
        "event-3".to_owned(),
    ));

    assert_eq!(events.borrow().len(), 3);
    assert_eq!(
        errors.borrow().last().unwrap().code,
        CqrsErrorCode::DuplicateCommand
    );
    let dedupe = cursor.borrow().last().unwrap().dedupe.clone().unwrap();
    assert_eq!(dedupe.command_ids_retained, 1);
    assert_eq!(dedupe.event_ids_retained, 1);
    assert_eq!(dedupe.command_ids_evicted, 2);
    assert_eq!(dedupe.event_ids_evicted, 2);
}

#[test]
fn cqrs_default_dedupe_is_unbounded_id_membership() {
    let g = graph();
    let app = cqrs_with_options::<String, String>(
        &g,
        CqrsOptions::named("orders")
            .with_events(["OrderPlaced"])
            .with_handlers(vec![cqrs_command_handler(
                "PlaceOrder",
                |command: &CqrsCommand<String>| {
                    let mut event = CqrsEventDraft::new("OrderPlaced", command.payload.clone());
                    event.id = Some(command.payload.clone());
                    vec![event]
                },
            )]),
    );
    let events = collect_data(&app.events);
    let errors = collect_data(&app.errors);
    let cursor = collect_data(&app.cursor);

    app.dispatch(CqrsCommand::new(
        "cmd-1",
        "PlaceOrder",
        "event-1".to_owned(),
    ));
    app.dispatch(CqrsCommand::new(
        "cmd-2",
        "PlaceOrder",
        "event-2".to_owned(),
    ));
    app.dispatch(CqrsCommand::new(
        "cmd-3",
        "PlaceOrder",
        "event-3".to_owned(),
    ));
    app.dispatch(CqrsCommand::new(
        "cmd-1",
        "PlaceOrder",
        "event-4".to_owned(),
    ));

    assert_eq!(events.borrow().len(), 3);
    assert_eq!(
        errors.borrow().last().unwrap().code,
        CqrsErrorCode::DuplicateCommand
    );
    let cursor = cursor.borrow().last().unwrap().clone();
    assert_eq!(cursor.event_seq, 3);
    assert_eq!(cursor.command_count, 4);
    assert_eq!(cursor.error_count, 1);
    assert!(cursor.dedupe.is_none());
}

#[test]
fn cqrs_rejects_empty_command_ids_as_graph_visible_errors() {
    let g = graph();
    let app = cqrs_with_options::<String, String>(
        &g,
        CqrsOptions::named("orders")
            .with_events(["OrderPlaced"])
            .with_handlers(vec![cqrs_command_handler(
                "PlaceOrder",
                |command: &CqrsCommand<String>| {
                    vec![CqrsEventDraft::new("OrderPlaced", command.payload.clone())]
                },
            )]),
    );
    let events = collect_data(&app.events);
    let errors = collect_data(&app.errors);

    app.dispatch(CqrsCommand::new("", "PlaceOrder", "o1".to_owned()));
    app.dispatch(CqrsCommand::new("cmd-1", "", "o2".to_owned()));

    assert!(events.borrow().is_empty());
    let errors = errors.borrow();
    assert_eq!(errors[0].code, CqrsErrorCode::MalformedCommand);
    assert_eq!(errors[0].message, "cqrs: command id must be non-empty");
    assert_eq!(errors[1].code, CqrsErrorCode::MalformedCommand);
    assert_eq!(errors[1].message, "cqrs: command type must be non-empty");
}

#[test]
fn cqrs_timestamp_panic_is_graph_visible_error_fact() {
    let g = graph();
    let app = cqrs_with_options::<String, String>(
        &g,
        CqrsOptions::named("orders")
            .with_events(["OrderPlaced"])
            .with_now(|| panic!("clock down"))
            .with_handlers(vec![cqrs_command_handler(
                "PlaceOrder",
                |command: &CqrsCommand<String>| {
                    vec![CqrsEventDraft::new("OrderPlaced", command.payload.clone())]
                },
            )]),
    );
    let events = collect_data(&app.events);
    let errors = collect_data(&app.errors);

    app.dispatch(CqrsCommand::new("cmd-1", "PlaceOrder", "o1".to_owned()));

    assert!(events.borrow().is_empty());
    let error = errors.borrow().last().unwrap().clone();
    assert_eq!(error.code, CqrsErrorCode::ClockThrew);
    assert_eq!(error.message, "cqrs: now() threw: clock down");
}

#[test]
fn cqrs_does_not_downgrade_graph_domain_panics_to_handler_errors() {
    let g = graph();
    let other = graph();
    let foreign = other.state(1i32).erased();
    let owner = g.clone();
    let app = cqrs_with_options::<i32, i32>(
        &g,
        CqrsOptions::named("orders")
            .with_events(["OrderPlaced"])
            .with_handlers(vec![cqrs_command_handler(
                "PlaceOrder",
                move |command: &CqrsCommand<i32>| {
                    let _bad = owner.derived::<i32, _>(vec![foreign.clone()], |values| {
                        Some(*last_or_prev(values, 0).unwrap())
                    });
                    vec![CqrsEventDraft::new("OrderPlaced", command.payload)]
                },
            )]),
    );
    let errors = collect_data(&app.errors);

    let _ = catch_unwind(AssertUnwindSafe(|| {
        app.dispatch(CqrsCommand::new("cmd-1", "PlaceOrder", 1));
    }));

    assert!(errors.borrow().is_empty());
}

#[test]
fn cqrs_projection_is_declared_and_reducer_panics_are_error_facts() {
    let g = graph();
    let app = cqrs_with_options::<String, String>(
        &g,
        CqrsOptions::named("orders")
            .with_events(["OrderPlaced", "OrderFailed"])
            .with_handlers(vec![cqrs_command_handler(
                "PlaceOrder",
                |command: &CqrsCommand<String>| {
                    vec![
                        CqrsEventDraft::new("OrderPlaced", command.payload.clone()),
                        CqrsEventDraft::new("OrderFailed", command.payload.clone()),
                    ]
                },
            )]),
    );
    let projection = cqrs_projection(
        &g,
        &app,
        CqrsProjectionOptions {
            name: "orders/count".to_owned(),
            events: Some(vec!["OrderPlaced".to_owned(), "OrderFailed".to_owned()]),
            initial: 0usize,
            reducer: Rc::new(|state, event| {
                if event.event_type == "OrderFailed" {
                    panic!("projection failed");
                }
                state + 1
            }),
        },
    );
    let values = collect_data(&projection.value);
    let errors = collect_data(&projection.errors);

    app.dispatch(CqrsCommand::new("cmd-1", "PlaceOrder", "o1".to_owned()));

    assert_eq!(*values.borrow().last().unwrap(), 1);
    assert_eq!(
        errors.borrow().last().unwrap().code,
        CqrsProjectionErrorCode::ProjectionThrew
    );
    assert_eq!(
        errors.borrow().last().unwrap().event_id,
        cqrs_event_id("cmd-1", 2)
    );

    let snap = g.describe();
    assert!(snap
        .nodes
        .iter()
        .any(|node| node.id == "orders/count" && node.factory == "cqrsProjection"));
    assert!(snap.edges.contains(&graphrefly::DescribeEdge {
        from: "orders/events".to_owned(),
        to: "orders/count".to_owned(),
    }));
    assert!(matches!(
        projection.frames.cache(),
        Some(CqrsProjectionFrame::Error(_))
    ));
}

#[test]
fn cqrs_runtime_state_is_checkpoint_json_friendly() {
    let g = graph();
    let app = cqrs_with_options::<String, String>(
        &g,
        CqrsOptions::named("orders")
            .with_events(["OrderPlaced"])
            .with_dedupe(CqrsDedupePolicy::bounded(2, 2))
            .with_handlers(vec![cqrs_command_handler(
                "PlaceOrder",
                |command: &CqrsCommand<String>| {
                    let mut event = CqrsEventDraft::new("OrderPlaced", command.payload.clone());
                    event.id = Some("event-1".to_owned());
                    vec![event]
                },
            )]),
    );

    app.dispatch(CqrsCommand::new("cmd-1", "PlaceOrder", "o1".to_owned()));
    app.command.down(vec![Message::Invalidate]);

    let checkpoint = g.checkpoint().expect("checkpoint succeeds");
    let runtime = checkpoint
        .nodes
        .iter()
        .find(|node| node.id == "orders/runtime")
        .expect("runtime node checkpointed");
    let data = match &runtime.ctx_state.value {
        graphrefly::GraphCheckpointValue::Data { data } => data,
        graphrefly::GraphCheckpointValue::Sentinel => panic!("runtime ctx state is persisted"),
    };
    assert_eq!(data["eventSeq"], 1);
    assert_eq!(data["commandCount"], 1);
    assert_eq!(data["seenCommandIds"][0], "cmd-1");
    assert_eq!(data["seenEventIds"][0], "event-1");
}
