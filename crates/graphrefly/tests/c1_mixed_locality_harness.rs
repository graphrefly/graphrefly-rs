use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

use graphrefly::{
    decode_wire_bridge_protobuf_bytes, encode_wire_bridge_protobuf_bytes, graph, wire_bridge,
    wire_edge_group, CanonicalWireEdgeKind, GraphNodeOpts, Message, WireBridgeEnvelopeType,
    WireBridgeInbound, WireBridgeOptions, WireBridgePayload, WireBridgeProtobufDataBody,
    WireBridgeProtobufEnvelope, WireBridgeProtobufPayload, WireBridgeProtobufStatusKind,
    WireEdgeGroupEdge, WireEdgeGroupOptions,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct TraceRow {
    phase: &'static str,
    direction: &'static str,
    pump_step: usize,
    queue_index: usize,
    bytes_len: usize,
    session_id: Option<String>,
    seq: Option<u64>,
    cursor: Option<u64>,
    attempt: Option<u32>,
    envelope_type: Option<&'static str>,
    data_body_kind: Option<&'static str>,
    edge_id: Option<String>,
    cause_id: Option<String>,
    wire_edge_kind: Option<&'static str>,
    value_hex_prefix: Option<String>,
    decode_error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplayClass {
    NoOldCauseReplayObserved,
    OutboundCauseGeneration,
    InboundWireEdgeGroupGate,
    WarmupActivationDrainBoundary,
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

fn hex_prefix(bytes: &[u8]) -> String {
    bytes
        .iter()
        .take(16)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn payload_envelope_type(payload: &WireBridgeProtobufPayload) -> WireBridgeEnvelopeType {
    match payload {
        WireBridgeProtobufPayload::Start => WireBridgeEnvelopeType::Start,
        WireBridgeProtobufPayload::Data(_) => WireBridgeEnvelopeType::Data,
        WireBridgeProtobufPayload::Ack => WireBridgeEnvelopeType::Ack,
        WireBridgeProtobufPayload::Nack { .. } => WireBridgeEnvelopeType::Nack,
        WireBridgeProtobufPayload::Status { .. } => WireBridgeEnvelopeType::Status,
        WireBridgeProtobufPayload::Error { .. } => WireBridgeEnvelopeType::Error,
        WireBridgeProtobufPayload::Close { .. } => WireBridgeEnvelopeType::Close,
    }
}

fn envelope_type_label(envelope_type: WireBridgeEnvelopeType) -> &'static str {
    match envelope_type {
        WireBridgeEnvelopeType::Start => "start",
        WireBridgeEnvelopeType::Data => "data",
        WireBridgeEnvelopeType::Ack => "ack",
        WireBridgeEnvelopeType::Nack => "nack",
        WireBridgeEnvelopeType::Status => "status",
        WireBridgeEnvelopeType::Error => "error",
        WireBridgeEnvelopeType::Close => "close",
    }
}

fn semantic_to_protobuf_envelope(
    envelope: &graphrefly::WireBridgeEnvelope<WireBridgeProtobufDataBody>,
) -> WireBridgeProtobufEnvelope {
    let payload = match envelope.envelope_type {
        WireBridgeEnvelopeType::Start => WireBridgeProtobufPayload::Start,
        WireBridgeEnvelopeType::Data => match &envelope.payload {
            Some(WireBridgePayload::Data(body)) => WireBridgeProtobufPayload::Data(body.clone()),
            _ => panic!("DATA envelope must carry protobuf DATA payload"),
        },
        WireBridgeEnvelopeType::Ack => WireBridgeProtobufPayload::Ack,
        WireBridgeEnvelopeType::Nack => match &envelope.payload {
            Some(WireBridgePayload::Error(error)) => WireBridgeProtobufPayload::Nack {
                error: Some(error.as_bytes().to_vec()),
            },
            _ => panic!("NACK envelope must carry error payload"),
        },
        WireBridgeEnvelopeType::Status => match &envelope.payload {
            Some(WireBridgePayload::Status(status)) => WireBridgeProtobufPayload::Status {
                status: status.as_bytes().to_vec(),
            },
            _ => panic!("STATUS envelope must carry status payload"),
        },
        WireBridgeEnvelopeType::Error => match &envelope.payload {
            Some(WireBridgePayload::Error(error)) => WireBridgeProtobufPayload::Error {
                error: error.as_bytes().to_vec(),
            },
            _ => panic!("ERROR envelope must carry error payload"),
        },
        WireBridgeEnvelopeType::Close => match &envelope.payload {
            Some(WireBridgePayload::Close { reason }) => WireBridgeProtobufPayload::Close {
                reason: reason.as_ref().map(|value| value.as_bytes().to_vec()),
            },
            _ => panic!("CLOSE envelope must carry close payload"),
        },
    };
    WireBridgeProtobufEnvelope {
        session_id: envelope.session_id.clone(),
        metadata: envelope.metadata.clone(),
        payload,
    }
}

fn protobuf_to_semantic_envelope(
    envelope: WireBridgeProtobufEnvelope,
) -> graphrefly::WireBridgeEnvelope<WireBridgeProtobufDataBody> {
    let envelope_type = payload_envelope_type(&envelope.payload);
    let payload = match envelope.payload {
        WireBridgeProtobufPayload::Start | WireBridgeProtobufPayload::Ack => None,
        WireBridgeProtobufPayload::Data(body) => Some(WireBridgePayload::Data(body)),
        WireBridgeProtobufPayload::Nack { error } => Some(WireBridgePayload::Error(
            String::from_utf8_lossy(error.as_deref().unwrap_or_default()).into_owned(),
        )),
        WireBridgeProtobufPayload::Status { status } => Some(WireBridgePayload::Status(
            String::from_utf8_lossy(&status).into_owned(),
        )),
        WireBridgeProtobufPayload::Error { error } => Some(WireBridgePayload::Error(
            String::from_utf8_lossy(&error).into_owned(),
        )),
        WireBridgeProtobufPayload::Close { reason } => Some(WireBridgePayload::Close {
            reason: reason.map(|value| String::from_utf8_lossy(&value).into_owned()),
        }),
    };
    graphrefly::WireBridgeEnvelope {
        session_id: envelope.session_id,
        envelope_type,
        payload,
        metadata: envelope.metadata,
    }
}

fn capture_outbound_bytes(
    node: &graphrefly::Node<graphrefly::WireBridgeEnvelope<WireBridgeProtobufDataBody>>,
) -> Rc<RefCell<VecDeque<Vec<u8>>>> {
    let queue = Rc::new(RefCell::new(VecDeque::new()));
    let queue_sink = queue.clone();
    let _keep = node.subscribe(move |msg| {
        if let Message::Data(value) = msg {
            if let Some(envelope) =
                value.downcast_ref::<graphrefly::WireBridgeEnvelope<WireBridgeProtobufDataBody>>()
            {
                let encoded =
                    encode_wire_bridge_protobuf_bytes(&semantic_to_protobuf_envelope(envelope));
                assert_eq!(encoded.status.kind, WireBridgeProtobufStatusKind::Valid);
                assert!(encoded.issues.is_empty());
                queue_sink
                    .borrow_mut()
                    .push_back(encoded.bytes.expect("valid encode yields bytes"));
            }
        }
    });
    queue
}

fn decode_trace_row(
    raw: &[u8],
    phase: &'static str,
    direction: &'static str,
    pump_step: usize,
    queue_index: usize,
) -> TraceRow {
    let mut row = TraceRow {
        phase,
        direction,
        pump_step,
        queue_index,
        bytes_len: raw.len(),
        session_id: None,
        seq: None,
        cursor: None,
        attempt: None,
        envelope_type: None,
        data_body_kind: None,
        edge_id: None,
        cause_id: None,
        wire_edge_kind: None,
        value_hex_prefix: None,
        decode_error: None,
    };
    let decoded = decode_wire_bridge_protobuf_bytes(raw);
    if decoded.status.kind != WireBridgeProtobufStatusKind::Valid {
        row.decode_error = decoded.issues.first().map(|issue| issue.message.clone());
        return row;
    }
    let Some(envelope) = decoded.envelope else {
        row.decode_error = Some("valid decode without envelope".to_owned());
        return row;
    };
    row.session_id = Some(envelope.session_id.clone());
    row.seq = Some(envelope.metadata.seq);
    row.cursor = Some(envelope.metadata.cursor);
    row.attempt = Some(envelope.metadata.attempt);
    row.envelope_type = Some(envelope_type_label(payload_envelope_type(
        &envelope.payload,
    )));
    match &envelope.payload {
        WireBridgeProtobufPayload::Data(WireBridgeProtobufDataBody::Value(value)) => {
            row.data_body_kind = Some("value");
            row.value_hex_prefix = Some(hex_prefix(value));
        }
        WireBridgeProtobufPayload::Data(WireBridgeProtobufDataBody::WireEdge(frame)) => {
            row.data_body_kind = Some("wire_edge");
            row.edge_id = Some(frame.edge_id.clone());
            row.cause_id = Some(frame.cause_id.clone());
            row.wire_edge_kind = Some(match frame.kind {
                CanonicalWireEdgeKind::Dirty => "dirty",
                CanonicalWireEdgeKind::Data => "data",
            });
            row.value_hex_prefix = frame.value.as_deref().map(hex_prefix);
        }
        _ => {}
    }
    row
}

fn pump_fifo(
    queue: &Rc<RefCell<VecDeque<Vec<u8>>>>,
    target: &WireBridgeInbound<WireBridgeProtobufDataBody>,
    trace: &mut Vec<TraceRow>,
    phase: &'static str,
    direction: &'static str,
    limit: Option<usize>,
) -> usize {
    let mut pumped = 0;
    while limit.is_none_or(|limit| pumped < limit) {
        let Some(raw) = queue.borrow_mut().pop_front() else {
            break;
        };
        let row = decode_trace_row(&raw, phase, direction, pumped, 0);
        assert!(
            row.decode_error.is_none(),
            "C-1 host pump only transports canonical protobuf bytes: {row:?}"
        );
        let decoded = decode_wire_bridge_protobuf_bytes(&raw);
        let envelope = decoded.envelope.expect("valid decode yields envelope");
        let reencoded = encode_wire_bridge_protobuf_bytes(&envelope);
        assert_eq!(reencoded.status.kind, WireBridgeProtobufStatusKind::Valid);
        assert_eq!(
            reencoded.bytes.as_deref(),
            Some(raw.as_slice()),
            "C-1 host pump preserves FIFO byte material unchanged"
        );
        trace.push(row);
        target.set(protobuf_to_semantic_envelope(envelope));
        pumped += 1;
    }
    pumped
}

fn classify_replay_source(trace: &[TraceRow]) -> ReplayClass {
    if trace.iter().any(|row| {
        row.phase == "stimulus"
            && row.direction == "g1_to_g2"
            && row.wire_edge_kind == Some("data")
            && matches!(
                row.value_hex_prefix.as_deref(),
                Some("422d696e3a6130" | "432d696e3a6130")
            )
    }) {
        return ReplayClass::OutboundCauseGeneration;
    }
    if trace.iter().any(|row| {
        row.phase == "stimulus"
            && row.direction == "g2_to_g1"
            && row.wire_edge_kind == Some("data")
            && row
                .value_hex_prefix
                .as_deref()
                .is_some_and(|value| value.contains("6130"))
    }) {
        return ReplayClass::InboundWireEdgeGroupGate;
    }
    if trace.iter().any(|row| {
        row.phase == "stimulus"
            && matches!(row.direction, "g1_to_g2" | "g2_to_g1")
            && row
                .cause_id
                .as_deref()
                .is_some_and(|cause_id| cause_id.ends_with(":1"))
    }) {
        return ReplayClass::WarmupActivationDrainBoundary;
    }
    ReplayClass::NoOldCauseReplayObserved
}

fn trace_data_values(
    trace: &[TraceRow],
    phase: &'static str,
    direction: &'static str,
) -> Vec<String> {
    trace
        .iter()
        .filter(|row| {
            row.phase == phase
                && row.direction == direction
                && row.envelope_type == Some("data")
                && row.wire_edge_kind == Some("data")
        })
        .filter_map(|row| row.value_hex_prefix.clone())
        .collect()
}

fn trace_wire_edge_steps(
    trace: &[TraceRow],
    phase: &'static str,
    direction: &'static str,
) -> Vec<(String, String)> {
    trace
        .iter()
        .filter(|row| {
            row.phase == phase
                && row.direction == direction
                && row.envelope_type == Some("data")
                && row.data_body_kind == Some("wire_edge")
        })
        .map(|row| {
            (
                row.edge_id.clone().expect("wire edge trace has edge_id"),
                row.wire_edge_kind
                    .expect("wire edge trace has kind")
                    .to_owned(),
            )
        })
        .collect()
}

fn c1_bridge_options(session_id: &str, name: &str) -> WireBridgeOptions {
    let mut opts = WireBridgeOptions::named(session_id, name);
    opts.now_ms = Some(Rc::new(|| 1_u64));
    opts
}

#[test]
fn c1_rust_two_graph_mixed_locality_bridge_diamond_is_coherent() {
    let g1 = graph();
    let g2 = graph();

    let g1_to_g2_bridge = wire_bridge::<WireBridgeProtobufDataBody, WireBridgeProtobufDataBody>(
        &g1,
        c1_bridge_options("g1-g2", "g1_to_g2/bridge"),
    );
    let g2_from_g1_bridge = wire_bridge::<WireBridgeProtobufDataBody, WireBridgeProtobufDataBody>(
        &g2,
        c1_bridge_options("g1-g2", "g2_from_g1/bridge"),
    );
    let g2_to_g1_bridge = wire_bridge::<WireBridgeProtobufDataBody, WireBridgeProtobufDataBody>(
        &g2,
        c1_bridge_options("g2-g1", "g2_to_g1/bridge"),
    );
    let g1_from_g2_bridge = wire_bridge::<WireBridgeProtobufDataBody, WireBridgeProtobufDataBody>(
        &g1,
        c1_bridge_options("g2-g1", "g1_from_g2/bridge"),
    );

    let source = g1.state_opts(b"a0".to_vec(), GraphNodeOpts::named("A"));
    let a_to_b = g1.node_opts::<Vec<u8>, _>(
        vec![source.erased()],
        |ctx| {
            let value = ctx.data::<Vec<u8>>(0).expect("A has data");
            let mut out = b"B-in:".to_vec();
            out.extend_from_slice(value.as_slice());
            ctx.emit(out);
        },
        GraphNodeOpts::named("A_to_B_payload"),
    );
    let a_to_c = g1.node_opts::<Vec<u8>, _>(
        vec![source.erased()],
        |ctx| {
            let value = ctx.data::<Vec<u8>>(0).expect("A has data");
            let mut out = b"C-in:".to_vec();
            out.extend_from_slice(value.as_slice());
            ctx.emit(out);
        },
        GraphNodeOpts::named("A_to_C_payload"),
    );
    let _g1_split = wire_edge_group(
        &g1,
        &g1_to_g2_bridge,
        WireEdgeGroupOptions::named(
            "g1_split",
            vec![
                WireEdgeGroupEdge::outbound("a-to-b", a_to_b),
                WireEdgeGroupEdge::outbound("a-to-c", a_to_c),
            ],
        ),
    );

    let g2_split_in = wire_edge_group(
        &g2,
        &g2_from_g1_bridge,
        WireEdgeGroupOptions::named(
            "g2_split_in",
            vec![
                WireEdgeGroupEdge::inbound("a-to-b"),
                WireEdgeGroupEdge::inbound("a-to-c"),
            ],
        ),
    );
    let b_in = g2_split_in
        .inbound
        .get("a-to-b")
        .expect("a-to-b inbound edge");
    let c_in = g2_split_in
        .inbound
        .get("a-to-c")
        .expect("a-to-c inbound edge");
    let b_leg = g2.node_opts::<Vec<u8>, _>(
        vec![b_in.erased()],
        |ctx| {
            let value = ctx.data::<Vec<u8>>(0).expect("B input has data");
            let mut out = b"B-out:".to_vec();
            out.extend_from_slice(value.as_slice());
            ctx.emit(out);
        },
        GraphNodeOpts::named("B"),
    );
    let c_leg = g2.node_opts::<Vec<u8>, _>(
        vec![c_in.erased()],
        |ctx| {
            let value = ctx.data::<Vec<u8>>(0).expect("C input has data");
            let mut out = b"C-out:".to_vec();
            out.extend_from_slice(value.as_slice());
            ctx.emit(out);
        },
        GraphNodeOpts::named("C"),
    );
    let _g2_join_out = wire_edge_group(
        &g2,
        &g2_to_g1_bridge,
        WireEdgeGroupOptions::named(
            "g2_join_out",
            vec![
                WireEdgeGroupEdge::outbound("b-to-d", b_leg),
                WireEdgeGroupEdge::outbound("c-to-d", c_leg),
            ],
        ),
    );

    let g1_join_in = wire_edge_group(
        &g1,
        &g1_from_g2_bridge,
        WireEdgeGroupOptions::named(
            "g1_join_in",
            vec![
                WireEdgeGroupEdge::inbound("b-to-d"),
                WireEdgeGroupEdge::inbound("c-to-d"),
            ],
        ),
    );
    let b_to_d = g1_join_in
        .inbound
        .get("b-to-d")
        .expect("b-to-d inbound edge");
    let c_to_d = g1_join_in
        .inbound
        .get("c-to-d")
        .expect("c-to-d inbound edge");
    let runs = Rc::new(RefCell::new(Vec::<(Vec<u8>, Vec<u8>)>::new()));
    let runs_sink = runs.clone();
    let join = g1.node_opts::<Vec<u8>, _>(
        vec![b_to_d.erased(), c_to_d.erased()],
        move |ctx| {
            let b_value = ctx.data::<Vec<u8>>(0).expect("B leg has data");
            let c_value = ctx.data::<Vec<u8>>(1).expect("C leg has data");
            runs_sink
                .borrow_mut()
                .push(((*b_value).clone(), (*c_value).clone()));
            let mut out = b_value.as_ref().clone();
            out.push(b'|');
            out.extend_from_slice(c_value.as_slice());
            ctx.emit(out);
        },
        GraphNodeOpts::named("D"),
    );
    let joined = collect_data(&join);
    let g1_to_g2_queue = capture_outbound_bytes(&g1_to_g2_bridge.outbound);
    let g2_to_g1_queue = capture_outbound_bytes(&g2_to_g1_bridge.outbound);
    let mut trace = Vec::new();

    assert_eq!(
        pump_fifo(
            &g1_to_g2_queue,
            &g2_from_g1_bridge.inbound,
            &mut trace,
            "warmup",
            "g1_to_g2",
            None,
        ),
        4
    );
    assert_eq!(
        pump_fifo(
            &g2_to_g1_queue,
            &g1_from_g2_bridge.inbound,
            &mut trace,
            "warmup",
            "g2_to_g1",
            None,
        ),
        4
    );
    assert_eq!(
        joined.borrow().as_slice(),
        &[b"B-out:B-in:a0|C-out:C-in:a0".to_vec()]
    );
    joined.borrow_mut().clear();
    runs.borrow_mut().clear();

    source.set(b"a1".to_vec());
    assert_eq!(
        pump_fifo(
            &g1_to_g2_queue,
            &g2_from_g1_bridge.inbound,
            &mut trace,
            "stimulus",
            "g1_to_g2",
            Some(2),
        ),
        2,
        "partial inbound progress must not settle g2's split"
    );
    assert!(
        g2_to_g1_queue.borrow().is_empty(),
        "partial g1->g2 cause must not produce return-leg fan-in"
    );
    assert!(joined.borrow().is_empty());
    assert!(runs.borrow().is_empty());
    assert_eq!(
        pump_fifo(
            &g1_to_g2_queue,
            &g2_from_g1_bridge.inbound,
            &mut trace,
            "stimulus",
            "g1_to_g2",
            None,
        ),
        2
    );
    assert_eq!(
        pump_fifo(
            &g2_to_g1_queue,
            &g1_from_g2_bridge.inbound,
            &mut trace,
            "stimulus",
            "g2_to_g1",
            Some(2),
        ),
        2,
        "partial return cause must not settle D with cached old values"
    );
    assert!(joined.borrow().is_empty());
    assert!(runs.borrow().is_empty());
    assert_eq!(
        pump_fifo(
            &g2_to_g1_queue,
            &g1_from_g2_bridge.inbound,
            &mut trace,
            "stimulus",
            "g2_to_g1",
            None,
        ),
        2
    );

    let classification = classify_replay_source(&trace);
    assert_eq!(
        joined.borrow().as_slice(),
        &[b"B-out:B-in:a1|C-out:C-in:a1".to_vec()],
        "C-1 replay source classification: {classification:?}; trace={trace:?}"
    );
    assert_eq!(
        runs.borrow().as_slice(),
        &[(b"B-out:B-in:a1".to_vec(), b"C-out:C-in:a1".to_vec())],
        "C-1 replay source classification: {classification:?}; trace={trace:?}"
    );
    assert_eq!(classification, ReplayClass::NoOldCauseReplayObserved);
    assert!(
        g1_to_g2_queue.borrow().is_empty() && g2_to_g1_queue.borrow().is_empty(),
        "C-1 FIFO host pump leaves no hidden repair/dedupe work queued"
    );
    assert_eq!(
        trace_data_values(&trace, "stimulus", "g1_to_g2"),
        vec!["422d696e3a6131".to_owned(), "432d696e3a6131".to_owned()],
        "stimulus frames must be fresh outbound cause material, not a0 replay"
    );
    assert_eq!(
        trace_wire_edge_steps(&trace, "stimulus", "g1_to_g2"),
        vec![
            ("a-to-b".to_owned(), "dirty".to_owned()),
            ("a-to-c".to_owned(), "dirty".to_owned()),
            ("a-to-b".to_owned(), "data".to_owned()),
            ("a-to-c".to_owned(), "data".to_owned()),
        ],
        "g1->g2 stimulus must preserve WireEdgeGroup two-phase ordering"
    );
    assert_eq!(
        trace_data_values(&trace, "stimulus", "g2_to_g1"),
        vec![
            "422d6f75743a422d696e3a6131".to_owned(),
            "432d6f75743a432d696e3a6131".to_owned(),
        ],
        "return frames must carry coherent B(a1), C(a1)"
    );
    assert_eq!(
        trace_wire_edge_steps(&trace, "stimulus", "g2_to_g1"),
        vec![
            ("b-to-d".to_owned(), "dirty".to_owned()),
            ("c-to-d".to_owned(), "dirty".to_owned()),
            ("b-to-d".to_owned(), "data".to_owned()),
            ("c-to-d".to_owned(), "data".to_owned()),
        ],
        "g2->g1 stimulus must preserve WireEdgeGroup two-phase ordering"
    );
    assert!(trace.iter().all(|row| row.queue_index == 0));
    assert!(trace.iter().all(|row| row.bytes_len > 0));
    for direction in ["g1_to_g2", "g2_to_g1"] {
        let stimulus_steps = trace
            .iter()
            .filter(|row| row.phase == "stimulus" && row.direction == direction)
            .map(|row| row.pump_step)
            .collect::<Vec<_>>();
        assert_eq!(
            stimulus_steps,
            vec![0, 1, 0, 1],
            "pump records strict pop-front order for {direction}"
        );
    }
}
