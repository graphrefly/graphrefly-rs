use std::cell::RefCell;
use std::rc::Rc;

use graphrefly::{
    batch, graph, remote_call, remote_call_with_options, remote_responder,
    remote_responder_handler, wire_bridge, wire_bridge_envelope, Message, RemoteCallOptions,
    RemoteCallRequest, RemoteCallResponse, RemoteCallResult, RemoteCallStatusState,
    RemoteResponderOptions, RemoteResponderStatusState, WireBridgeCommand, WireBridgeEnvelopeInput,
    WireBridgeEnvelopeType, WireBridgeEvent, WireBridgeOptions, WireBridgePayload,
    WireBridgeStatusState,
};

fn envelope<T>(
    session_id: &str,
    envelope_type: WireBridgeEnvelopeType,
    seq: u64,
    payload: Option<WireBridgePayload<T>>,
    idempotency_key: Option<&str>,
    ack_for_seq: Option<u64>,
) -> graphrefly::WireBridgeEnvelope<T> {
    wire_bridge_envelope(WireBridgeEnvelopeInput {
        session_id: session_id.to_owned(),
        envelope_type,
        seq,
        cursor: 0,
        payload,
        idempotency_key: idempotency_key.map(str::to_owned),
        attempt: 1,
        max_attempts: 1,
        timestamp_ms: None,
        ack_for_seq,
        request_id: None,
    })
    .expect("test envelope is valid")
}

fn envelope_with_request<T>(
    session_id: &str,
    envelope_type: WireBridgeEnvelopeType,
    seq: u64,
    payload: Option<WireBridgePayload<T>>,
    request_id: Option<&str>,
) -> graphrefly::WireBridgeEnvelope<T> {
    wire_bridge_envelope(WireBridgeEnvelopeInput {
        session_id: session_id.to_owned(),
        envelope_type,
        seq,
        cursor: 0,
        payload,
        idempotency_key: None,
        attempt: 1,
        max_attempts: 1,
        timestamp_ms: None,
        ack_for_seq: None,
        request_id: request_id.map(str::to_owned),
    })
    .expect("test envelope is valid")
}

fn envelope_with_cursor<T>(
    session_id: &str,
    envelope_type: WireBridgeEnvelopeType,
    seq: u64,
    cursor: u64,
    payload: Option<WireBridgePayload<T>>,
    request_id: Option<&str>,
) -> graphrefly::WireBridgeEnvelope<T> {
    wire_bridge_envelope(WireBridgeEnvelopeInput {
        session_id: session_id.to_owned(),
        envelope_type,
        seq,
        cursor,
        payload,
        idempotency_key: None,
        attempt: 1,
        max_attempts: 1,
        timestamp_ms: None,
        ack_for_seq: None,
        request_id: request_id.map(str::to_owned),
    })
    .expect("test envelope is valid")
}

fn collect_data<T: Clone + 'static>(node: &graphrefly::Node<T>) -> Rc<RefCell<Vec<T>>> {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let seen_sink = seen.clone();
    let _keep = node.subscribe(move |msg| {
        if let Message::Data(value) = msg {
            if let Some(value) = value.downcast_ref::<T>() {
                seen_sink.borrow_mut().push(value.clone());
            }
        }
    });
    seen
}

#[test]
fn idempotency_key_is_metadata_not_duplicate_lookup() {
    let g = graph();
    let bridge = wire_bridge::<String, String>(&g, WireBridgeOptions::named("session-a", "bridge"));
    let events = Rc::new(RefCell::new(Vec::new()));
    let events_sink = events.clone();
    let _events = bridge.events.subscribe(move |msg| {
        if let Message::Data(value) = msg {
            if let Some(event) = value.downcast_ref::<WireBridgeEvent<String, String>>() {
                events_sink.borrow_mut().push(event.clone());
            }
        }
    });
    let _cursor = bridge.cursor.subscribe(|_| {});

    bridge.inbound.set(envelope(
        "session-a",
        WireBridgeEnvelopeType::Data,
        1,
        Some(WireBridgePayload::Data("remote-one".to_owned())),
        Some("same-key"),
        None,
    ));
    bridge.inbound.set(envelope(
        "session-a",
        WireBridgeEnvelopeType::Data,
        2,
        Some(WireBridgePayload::Data("remote-two".to_owned())),
        Some("same-key"),
        None,
    ));

    assert_eq!(bridge.cursor.cache(), Some(2));
    assert!(!events
        .borrow()
        .iter()
        .any(|event| matches!(event, WireBridgeEvent::Duplicate { .. })));
}

#[test]
fn ack_for_seq_is_receipt_correlation_not_idempotency_key_lookup() {
    let g = graph();
    let bridge = wire_bridge::<String, String>(&g, WireBridgeOptions::named("session-a", "bridge"));
    let _acks = bridge.acks.subscribe(|_| {});
    let _cursor = bridge.cursor.subscribe(|_| {});
    let _status = bridge.status.subscribe(|_| {});

    bridge.send("first".to_owned(), Some("same-key".to_owned()), None);
    bridge.send("second".to_owned(), Some("same-key".to_owned()), None);
    bridge.inbound.set(envelope(
        "session-a",
        WireBridgeEnvelopeType::Ack,
        1,
        None,
        Some("different-correlation-key"),
        Some(2),
    ));

    assert_eq!(bridge.cursor.cache(), Some(1));
    assert_eq!(bridge.acks.cache().unwrap().ack_for_seq, 2);
    let status = bridge.status.cache().unwrap();
    assert_eq!(status.state, WireBridgeStatusState::Open);
    assert_eq!(status.pending, 1);
    assert_eq!(status.acked, 1);
    assert_eq!(status.last_seq, Some(1));
}

#[test]
fn remote_call_sends_request_facts_and_projects_later_response() {
    let g = graph();
    let bridge = wire_bridge::<RemoteCallRequest<String>, RemoteCallResponse<String>>(
        &g,
        WireBridgeOptions::named("session-a", "bridge"),
    );
    let remote = remote_call_with_options(&g, &bridge, RemoteCallOptions::named("rpc"));
    let outbound = collect_data(&bridge.outbound);
    let results = collect_data(&remote.results);
    let status = collect_data(&remote.status);
    let errors = collect_data(&remote.errors);

    let request = remote.call("uppercase", "req-1", "hello".to_owned());

    assert_eq!(request.operation, "uppercase");
    assert_eq!(request.request_id, "req-1");
    let sent = outbound.borrow().last().unwrap().clone();
    assert_eq!(sent.metadata.request_id.as_deref(), Some("req-1"));
    assert_eq!(sent.envelope_type, WireBridgeEnvelopeType::Data);
    match sent.payload.unwrap() {
        WireBridgePayload::Data(payload) => {
            assert_eq!(payload.operation, "uppercase");
            assert_eq!(payload.payload, "hello");
        }
        _ => panic!("remote_call must send request as DATA payload"),
    }
    let requested = status.borrow().last().unwrap().clone();
    assert_eq!(requested.state, RemoteCallStatusState::Requested);
    assert_eq!(requested.pending, 1);

    bridge.inbound.set(envelope_with_request(
        "session-a",
        WireBridgeEnvelopeType::Data,
        1,
        Some(WireBridgePayload::Data(RemoteCallResponse::Result {
            operation: "uppercase".to_owned(),
            request_id: "req-1".to_owned(),
            payload: "HELLO".to_owned(),
        })),
        Some("req-1"),
    ));

    assert_eq!(
        *results.borrow(),
        vec![RemoteCallResult {
            operation: "uppercase".to_owned(),
            request_id: "req-1".to_owned(),
            payload: "HELLO".to_owned(),
        }]
    );
    let responded = status.borrow().last().unwrap().clone();
    assert_eq!(responded.state, RemoteCallStatusState::Responded);
    assert_eq!(responded.pending, 0);
    assert_eq!(responded.completed, 1);
    assert!(errors.borrow().is_empty());

    bridge.inbound.set(envelope_with_request(
        "session-a",
        WireBridgeEnvelopeType::Data,
        2,
        Some(WireBridgePayload::Data(RemoteCallResponse::Result {
            operation: "uppercase".to_owned(),
            request_id: "late-unknown".to_owned(),
            payload: "STALE".to_owned(),
        })),
        Some("late-unknown"),
    ));
    assert_eq!(
        results.borrow().len(),
        1,
        "D147: late/unknown responses are bridge facts but not remote_call results"
    );

    let snapshot = g.describe();
    assert!(snapshot.nodes.iter().any(|node| node.id == "rpc/responses"));
    assert!(snapshot.nodes.iter().any(|node| node.id == "rpc/results"));
    assert!(snapshot
        .edges
        .iter()
        .any(|edge| edge.from == "bridge/events" && edge.to == "rpc/responses"));
    assert!(snapshot
        .edges
        .iter()
        .any(|edge| edge.from == "rpc/responses" && edge.to == "rpc/results"));
}

#[test]
fn remote_call_status_response_keeps_request_pending_until_result() {
    let g = graph();
    let bridge = wire_bridge::<RemoteCallRequest<String>, RemoteCallResponse<String>>(
        &g,
        WireBridgeOptions::named("session-a", "bridge"),
    );
    let remote = remote_call(&g, &bridge);
    let responses = collect_data(&remote.responses);
    let results = collect_data(&remote.results);
    let status = collect_data(&remote.status);

    remote.call("poll", "req-1", "payload".to_owned());
    bridge.inbound.set(envelope_with_request(
        "session-a",
        WireBridgeEnvelopeType::Data,
        1,
        Some(WireBridgePayload::Data(RemoteCallResponse::Status {
            operation: "poll".to_owned(),
            request_id: "req-1".to_owned(),
            status: "accepted".to_owned(),
        })),
        Some("req-1"),
    ));

    assert_eq!(responses.borrow().len(), 1);
    assert_eq!(
        status.borrow().last().unwrap().pending,
        1,
        "status responses are non-terminal call facts"
    );

    bridge.inbound.set(envelope_with_request(
        "session-a",
        WireBridgeEnvelopeType::Data,
        2,
        Some(WireBridgePayload::Data(RemoteCallResponse::Result {
            operation: "poll".to_owned(),
            request_id: "req-1".to_owned(),
            payload: "done".to_owned(),
        })),
        Some("req-1"),
    ));

    assert_eq!(results.borrow().len(), 1);
    assert_eq!(status.borrow().last().unwrap().pending, 0);
}

#[test]
fn remote_call_accepts_same_wave_response_after_request_registration() {
    let g = graph();
    let bridge = wire_bridge::<RemoteCallRequest<String>, RemoteCallResponse<String>>(
        &g,
        WireBridgeOptions::named("session-a", "bridge"),
    );
    let remote = remote_call(&g, &bridge);
    let results = collect_data(&remote.results);
    let status = collect_data(&remote.status);

    batch(|_| {
        remote.call("echo", "req-1", "payload".to_owned());
        bridge.inbound.set(envelope_with_request(
            "session-a",
            WireBridgeEnvelopeType::Data,
            1,
            Some(WireBridgePayload::Data(RemoteCallResponse::Result {
                operation: "echo".to_owned(),
                request_id: "req-1".to_owned(),
                payload: "done".to_owned(),
            })),
            Some("req-1"),
        ));
    });

    assert_eq!(
        *results.borrow(),
        vec![RemoteCallResult {
            operation: "echo".to_owned(),
            request_id: "req-1".to_owned(),
            payload: "done".to_owned(),
        }]
    );
    assert_eq!(status.borrow().last().unwrap().pending, 0);
}

#[test]
fn remote_call_timeout_is_local_status_and_error_fact() {
    let g = graph();
    let bridge = wire_bridge::<RemoteCallRequest<String>, RemoteCallResponse<String>>(
        &g,
        WireBridgeOptions::named("session-a", "bridge"),
    );
    let remote = remote_call(&g, &bridge);
    let errors = collect_data(&remote.errors);
    let status = collect_data(&remote.status);

    remote.call("slow", "req-1", "payload".to_owned());
    remote.timeout(
        "req-1",
        Some("slow".to_owned()),
        "remote call timed out locally",
    );

    let timeout_status = status.borrow().last().unwrap().clone();
    assert_eq!(timeout_status.state, RemoteCallStatusState::TimedOut);
    assert_eq!(timeout_status.pending, 0);
    assert_eq!(timeout_status.timeouts, 1);
    assert_eq!(
        errors.borrow().last().unwrap().request_id.as_deref(),
        Some("req-1")
    );
    assert_eq!(
        errors.borrow().last().unwrap().error,
        "remote call timed out locally"
    );
}

#[test]
fn remote_call_unknown_timeout_does_not_consume_pending_request() {
    let g = graph();
    let bridge = wire_bridge::<RemoteCallRequest<String>, RemoteCallResponse<String>>(
        &g,
        WireBridgeOptions::named("session-a", "bridge"),
    );
    let remote = remote_call(&g, &bridge);
    let results = collect_data(&remote.results);
    let status = collect_data(&remote.status);

    remote.call("slow", "req-1", "payload".to_owned());
    remote.timeout(
        "missing",
        Some("slow".to_owned()),
        "unknown timeout remains local",
    );

    assert_eq!(
        status.borrow().last().unwrap().pending,
        1,
        "unknown timeout must not consume an unrelated pending request"
    );

    bridge.inbound.set(envelope_with_request(
        "session-a",
        WireBridgeEnvelopeType::Data,
        1,
        Some(WireBridgePayload::Data(RemoteCallResponse::Result {
            operation: "slow".to_owned(),
            request_id: "req-1".to_owned(),
            payload: "done".to_owned(),
        })),
        Some("req-1"),
    ));

    assert_eq!(results.borrow().len(), 1);
    assert_eq!(status.borrow().last().unwrap().pending, 0);
}

#[test]
fn remote_call_nack_clears_pending_and_rejects_late_result() {
    let g = graph();
    let bridge = wire_bridge::<RemoteCallRequest<String>, RemoteCallResponse<String>>(
        &g,
        WireBridgeOptions::named("session-a", "bridge"),
    );
    let remote = remote_call(&g, &bridge);
    let errors = collect_data(&remote.errors);
    let results = collect_data(&remote.results);
    let status = collect_data(&remote.status);

    remote.call("echo", "req-1", "payload".to_owned());
    bridge.inbound.set(envelope(
        "session-a",
        WireBridgeEnvelopeType::Nack,
        1,
        Some(WireBridgePayload::Error("remote rejected".to_owned())),
        None,
        Some(1),
    ));

    let error = errors.borrow().last().unwrap().clone();
    assert_eq!(error.operation.as_deref(), Some("echo"));
    assert_eq!(error.request_id.as_deref(), Some("req-1"));
    assert_eq!(error.error, "remote rejected");
    let nack_status = status.borrow().last().unwrap().clone();
    assert_eq!(nack_status.state, RemoteCallStatusState::BridgeErrored);
    assert_eq!(nack_status.pending, 0);

    bridge.inbound.set(envelope_with_request(
        "session-a",
        WireBridgeEnvelopeType::Data,
        2,
        Some(WireBridgePayload::Data(RemoteCallResponse::Result {
            operation: "echo".to_owned(),
            request_id: "req-1".to_owned(),
            payload: "late".to_owned(),
        })),
        Some("req-1"),
    ));

    assert!(
        results.borrow().is_empty(),
        "bridge-failed requests must not accept later stale responses"
    );
}

#[test]
fn remote_responder_invokes_sync_handler_and_sends_response_through_bridge_command() {
    let g = graph();
    let bridge = wire_bridge::<RemoteCallResponse<String>, RemoteCallRequest<String>>(
        &g,
        WireBridgeOptions::named("session-a", "bridge"),
    );
    let responder = remote_responder(
        &g,
        &bridge,
        RemoteResponderOptions::named("responder").with_handlers(vec![remote_responder_handler(
            "uppercase",
            |request: &RemoteCallRequest<String>| Ok(request.payload.to_uppercase()),
        )]),
    );
    let requests = collect_data(&responder.requests);
    let response_commands = collect_data(&responder.response_commands);
    let outbound = collect_data(&bridge.outbound);
    let status = collect_data(&responder.status);
    let command_msgs = Rc::new(RefCell::new(Vec::new()));
    let command_msgs_sink = command_msgs.clone();
    let _command_observer = bridge.command.subscribe(move |msg| {
        command_msgs_sink.borrow_mut().push(match msg {
            Message::Data(_) => "DATA",
            Message::Start => "START",
            Message::Pause(_) => "PAUSE",
            Message::Resume(_) => "RESUME",
            Message::Dirty => "DIRTY",
            Message::Resolved => "RESOLVED",
            Message::Invalidate => "INVALIDATE",
            Message::Complete => "COMPLETE",
            Message::Error(_) => "ERROR",
            Message::Teardown => "TEARDOWN",
        });
    });

    bridge.inbound.set(envelope_with_request(
        "session-a",
        WireBridgeEnvelopeType::Data,
        1,
        Some(WireBridgePayload::Data(RemoteCallRequest::new(
            "uppercase",
            "req-1",
            "hello".to_owned(),
        ))),
        Some("req-1"),
    ));

    assert_eq!(requests.borrow().len(), 1);
    assert_eq!(requests.borrow()[0].operation, "uppercase");
    assert!(matches!(
        response_commands.borrow().last(),
        Some(WireBridgeCommand::Send { request_id, .. }) if request_id.as_deref() == Some("req-1")
    ));
    let response = outbound.borrow().last().unwrap().clone();
    assert_eq!(response.metadata.request_id.as_deref(), Some("req-1"));
    match response.payload.unwrap() {
        WireBridgePayload::Data(RemoteCallResponse::Result {
            operation,
            request_id,
            payload,
        }) => {
            assert_eq!(operation, "uppercase");
            assert_eq!(request_id, "req-1");
            assert_eq!(payload, "HELLO");
        }
        _ => panic!("remote_responder must send result as bridge DATA fact"),
    }
    let responded = status.borrow().last().unwrap().clone();
    assert_eq!(responded.state, RemoteResponderStatusState::Responded);
    assert_eq!(responded.handled, 1);
    assert!(
        command_msgs.borrow().iter().all(|kind| *kind != "ERROR"),
        "remote responder command helper must publish DATA facts, not protocol ERROR"
    );

    let snapshot = g.describe();
    assert!(snapshot
        .edges
        .iter()
        .any(|edge| edge.from == "bridge/inbound" && edge.to == "responder/events"));
    assert!(snapshot
        .edges
        .iter()
        .any(|edge| edge.from == "responder/events" && edge.to == "responder/responseCommands"));
    assert!(snapshot
        .edges
        .iter()
        .any(|edge| edge.from == "responder/responseCommands" && edge.to == "bridge/command"));
}

#[test]
fn remote_responder_unknown_operation_is_graph_visible_error_response() {
    let g = graph();
    let bridge = wire_bridge::<RemoteCallResponse<String>, RemoteCallRequest<String>>(
        &g,
        WireBridgeOptions::named("session-a", "bridge"),
    );
    let responder = remote_responder(
        &g,
        &bridge,
        RemoteResponderOptions::<String, String>::named("responder").with_reject_unknown(true),
    );
    let errors = collect_data(&responder.errors);
    let outbound = collect_data(&bridge.outbound);

    bridge.inbound.set(envelope_with_request(
        "session-a",
        WireBridgeEnvelopeType::Data,
        1,
        Some(WireBridgePayload::Data(RemoteCallRequest::new(
            "missing",
            "req-1",
            "hello".to_owned(),
        ))),
        Some("req-1"),
    ));

    assert_eq!(
        errors.borrow().last().unwrap().error,
        "remoteResponder: unknown operation 'missing'"
    );
    let payload = outbound.borrow().last().unwrap().payload.clone().unwrap();
    match payload {
        WireBridgePayload::Data(RemoteCallResponse::Error {
            operation,
            request_id,
            error,
        }) => {
            assert_eq!(operation, "missing");
            assert_eq!(request_id, "req-1");
            assert_eq!(error, "remoteResponder: unknown operation 'missing'");
        }
        _ => panic!("unknown operation must stay a bridge DATA error response"),
    };
}

#[test]
fn multiple_remote_responders_ignore_non_owned_operations_by_default() {
    let g = graph();
    let bridge = wire_bridge::<RemoteCallResponse<String>, RemoteCallRequest<String>>(
        &g,
        WireBridgeOptions::named("session-a", "bridge"),
    );
    let _upper = remote_responder(
        &g,
        &bridge,
        RemoteResponderOptions::named("upper").with_handlers(vec![remote_responder_handler(
            "uppercase",
            |request: &RemoteCallRequest<String>| Ok(request.payload.to_uppercase()),
        )]),
    );
    let lower = remote_responder(
        &g,
        &bridge,
        RemoteResponderOptions::named("lower").with_handlers(vec![remote_responder_handler(
            "lowercase",
            |request: &RemoteCallRequest<String>| Ok(request.payload.to_lowercase()),
        )]),
    );
    let lower_errors = collect_data(&lower.errors);
    let outbound = collect_data(&bridge.outbound);

    bridge.inbound.set(envelope_with_request(
        "session-a",
        WireBridgeEnvelopeType::Data,
        1,
        Some(WireBridgePayload::Data(RemoteCallRequest::new(
            "uppercase",
            "req-1",
            "Hello".to_owned(),
        ))),
        Some("req-1"),
    ));

    assert!(
        lower_errors.borrow().is_empty(),
        "non-owning responders must not emit unknown-operation errors by default"
    );
    assert_eq!(outbound.borrow().len(), 1);
    match outbound.borrow().last().unwrap().payload.clone().unwrap() {
        WireBridgePayload::Data(RemoteCallResponse::Result { payload, .. }) => {
            assert_eq!(payload, "HELLO");
        }
        _ => panic!("owning responder should be the only response"),
    };
}

#[test]
fn remote_responder_consumes_non_data_bridge_frames_for_ordering() {
    let g = graph();
    let bridge = wire_bridge::<RemoteCallResponse<String>, RemoteCallRequest<String>>(
        &g,
        WireBridgeOptions::named("session-a", "bridge"),
    );
    let _responder = remote_responder(
        &g,
        &bridge,
        RemoteResponderOptions::named("responder").with_handlers(vec![remote_responder_handler(
            "uppercase",
            |request: &RemoteCallRequest<String>| Ok(request.payload.to_uppercase()),
        )]),
    );
    let outbound = collect_data(&bridge.outbound);

    bridge.inbound.set(envelope_with_request(
        "session-a",
        WireBridgeEnvelopeType::Start,
        1,
        None,
        None,
    ));
    bridge.inbound.set(envelope_with_request(
        "session-a",
        WireBridgeEnvelopeType::Data,
        2,
        Some(WireBridgePayload::Data(RemoteCallRequest::new(
            "uppercase",
            "req-1",
            "hello".to_owned(),
        ))),
        Some("req-1"),
    ));

    assert_eq!(
        outbound.borrow().len(),
        1,
        "valid Start(seq=1) must advance responder ordering before Data(seq=2)"
    );
}

#[test]
fn remote_responder_rejects_remote_cursor_regression_before_dispatch() {
    let g = graph();
    let bridge = wire_bridge::<RemoteCallResponse<String>, RemoteCallRequest<String>>(
        &g,
        WireBridgeOptions::named("session-a", "bridge"),
    );
    let responder = remote_responder(
        &g,
        &bridge,
        RemoteResponderOptions::named("responder").with_handlers(vec![remote_responder_handler(
            "uppercase",
            |request: &RemoteCallRequest<String>| Ok(request.payload.to_uppercase()),
        )]),
    );
    let errors = collect_data(&responder.errors);
    let outbound = collect_data(&bridge.outbound);

    bridge.inbound.set(envelope_with_cursor(
        "session-a",
        WireBridgeEnvelopeType::Data,
        1,
        10,
        Some(WireBridgePayload::Data(RemoteCallRequest::new(
            "uppercase",
            "req-1",
            "hello".to_owned(),
        ))),
        Some("req-1"),
    ));
    bridge.inbound.set(envelope_with_cursor(
        "session-a",
        WireBridgeEnvelopeType::Data,
        2,
        9,
        Some(WireBridgePayload::Data(RemoteCallRequest::new(
            "uppercase",
            "req-2",
            "hello".to_owned(),
        ))),
        Some("req-2"),
    ));

    assert_eq!(outbound.borrow().len(), 1);
    assert!(errors
        .borrow()
        .last()
        .unwrap()
        .error
        .contains("regressed below"));
}

#[test]
fn remote_responder_handler_error_is_local_error_fact_and_remote_error_response() {
    let g = graph();
    let bridge = wire_bridge::<RemoteCallResponse<String>, RemoteCallRequest<String>>(
        &g,
        WireBridgeOptions::named("session-a", "bridge"),
    );
    let responder = remote_responder(
        &g,
        &bridge,
        RemoteResponderOptions::named("responder").with_handlers(vec![remote_responder_handler(
            "fail",
            |_request: &RemoteCallRequest<String>| Err("handler rejected".to_owned()),
        )]),
    );
    let errors = collect_data(&responder.errors);
    let status = collect_data(&responder.status);
    let outbound = collect_data(&bridge.outbound);

    bridge.inbound.set(envelope_with_request(
        "session-a",
        WireBridgeEnvelopeType::Data,
        1,
        Some(WireBridgePayload::Data(RemoteCallRequest::new(
            "fail",
            "req-1",
            "hello".to_owned(),
        ))),
        Some("req-1"),
    ));

    assert_eq!(errors.borrow().last().unwrap().error, "handler rejected");
    assert_eq!(
        status.borrow().last().unwrap().state,
        RemoteResponderStatusState::Rejected
    );
    let payload = outbound.borrow().last().unwrap().payload.clone().unwrap();
    match payload {
        WireBridgePayload::Data(RemoteCallResponse::Error {
            operation,
            request_id,
            error,
        }) => {
            assert_eq!(operation, "fail");
            assert_eq!(request_id, "req-1");
            assert_eq!(error, "handler rejected");
        }
        _ => panic!("handler Err must stay a bridge DATA error response"),
    };
}
