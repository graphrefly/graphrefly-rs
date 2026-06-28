use std::any::Any;
use std::cell::RefCell;
use std::rc::Rc;

use graphrefly::{
    batch, graph, remote_call, remote_call_with_options, remote_responder,
    remote_responder_handler, wire_bridge, wire_bridge_envelope, wire_edge_group,
    CanonicalWireEdgeFrame, CanonicalWireEdgeKind, GraphNodeOpts, Message, NodeVersioningPolicy,
    RemoteCallOptions, RemoteCallRequest, RemoteCallResponse, RemoteCallResult,
    RemoteCallStatusState, RemoteResponderOptions, RemoteResponderStatusState, WireBridgeCommand,
    WireBridgeEnvelopeInput, WireBridgeEnvelopeType, WireBridgeEvent, WireBridgeOptions,
    WireBridgePayload, WireBridgeProtobufDataBody, WireBridgeStatusState, WireEdgeGroupEdge,
    WireEdgeGroupIssueCode, WireEdgeGroupOptions, WireEdgeGroupStatusState,
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

fn wire_edge_envelope(
    seq: u64,
    kind: CanonicalWireEdgeKind,
    edge_id: &str,
    cause_id: &str,
    value: Option<Vec<u8>>,
) -> graphrefly::WireBridgeEnvelope<WireBridgeProtobufDataBody> {
    envelope(
        "session-a",
        WireBridgeEnvelopeType::Data,
        seq,
        Some(WireBridgePayload::Data(
            WireBridgeProtobufDataBody::WireEdge(CanonicalWireEdgeFrame {
                kind,
                edge_id: edge_id.to_owned(),
                cause_id: cause_id.to_owned(),
                value,
            }),
        )),
        None,
        None,
    )
}

fn wire_edge_frames(
    outbound: &Rc<RefCell<Vec<graphrefly::WireBridgeEnvelope<WireBridgeProtobufDataBody>>>>,
) -> Vec<CanonicalWireEdgeFrame> {
    outbound
        .borrow()
        .iter()
        .filter_map(|envelope| match &envelope.payload {
            Some(WireBridgePayload::Data(WireBridgeProtobufDataBody::WireEdge(frame))) => {
                Some(frame.clone())
            }
            _ => None,
        })
        .collect()
}

#[test]
fn wire_edge_group_emits_two_phase_frames_gates_release_describes_and_releases() {
    let g = graph();
    let source_a = g.state_empty_opts::<Vec<u8>>(GraphNodeOpts::named("edge/a"));
    let source_b = g.state_empty_opts::<Vec<u8>>(GraphNodeOpts::named("edge/b"));
    let bridge = wire_bridge::<WireBridgeProtobufDataBody, WireBridgeProtobufDataBody>(
        &g,
        WireBridgeOptions::named("session-a", "bridge"),
    );
    let group = wire_edge_group(
        &g,
        &bridge,
        WireEdgeGroupOptions::named(
            "group",
            vec![
                WireEdgeGroupEdge::outbound("a", source_a.clone()),
                WireEdgeGroupEdge::outbound("b", source_b.clone()),
            ],
        ),
    );
    let outbound = collect_data(&bridge.outbound);
    let inbound_a = collect_data(group.inbound.get("a").expect("edge a exists"));
    let status = collect_data(&group.status);
    let issues = collect_data(&group.issues);

    batch(|_| {
        source_a.set(vec![1]);
        source_b.set(vec![2]);
    });
    let frames = wire_edge_frames(&outbound);
    assert_eq!(
        frames,
        vec![
            CanonicalWireEdgeFrame {
                kind: CanonicalWireEdgeKind::Dirty,
                edge_id: "a".to_owned(),
                cause_id: "group:cause:1".to_owned(),
                value: None,
            },
            CanonicalWireEdgeFrame {
                kind: CanonicalWireEdgeKind::Dirty,
                edge_id: "b".to_owned(),
                cause_id: "group:cause:1".to_owned(),
                value: None,
            },
            CanonicalWireEdgeFrame {
                kind: CanonicalWireEdgeKind::Data,
                edge_id: "a".to_owned(),
                cause_id: "group:cause:1".to_owned(),
                value: Some(vec![1]),
            },
            CanonicalWireEdgeFrame {
                kind: CanonicalWireEdgeKind::Data,
                edge_id: "b".to_owned(),
                cause_id: "group:cause:1".to_owned(),
                value: Some(vec![2]),
            },
        ]
    );

    bridge.inbound.set(wire_edge_envelope(
        1,
        CanonicalWireEdgeKind::Dirty,
        "a",
        "c1",
        None,
    ));
    bridge.inbound.set(wire_edge_envelope(
        2,
        CanonicalWireEdgeKind::Data,
        "a",
        "c1",
        Some(vec![10]),
    ));
    assert!(
        inbound_a.borrow().is_empty(),
        "DATA must not release before every DIRTY arrives"
    );

    bridge.inbound.set(wire_edge_envelope(
        3,
        CanonicalWireEdgeKind::Dirty,
        "b",
        "c1",
        None,
    ));
    bridge.inbound.set(wire_edge_envelope(
        4,
        CanonicalWireEdgeKind::Data,
        "b",
        "c1",
        Some(vec![20]),
    ));
    assert_eq!(*inbound_a.borrow(), vec![vec![10]]);
    assert!(issues.borrow().is_empty());
    assert_eq!(
        status.borrow().last().unwrap().state,
        WireEdgeGroupStatusState::Released
    );

    let snap = g.describe();
    assert!(snap
        .edges
        .iter()
        .any(|edge| edge.from == "bridge/inbound" && edge.to == "group/events"));
    assert!(snap
        .edges
        .iter()
        .any(|edge| edge.from == "edge/a" && edge.to == "group/events"));
    assert!(snap
        .edges
        .iter()
        .any(|edge| edge.from == "group/events" && edge.to == "group/gate"));
    assert!(snap
        .edges
        .iter()
        .any(|edge| edge.from == "group/events" && edge.to == "group/commands"));
    assert!(snap
        .edges
        .iter()
        .any(|edge| edge.from == "group/events" && edge.to == "group/issues"));
    assert!(snap
        .edges
        .iter()
        .any(|edge| edge.from == "group/events" && edge.to == "group/status"));
    assert!(snap
        .edges
        .iter()
        .any(|edge| edge.from == "group/gate" && edge.to == "group/issues"));
    assert!(snap
        .edges
        .iter()
        .any(|edge| edge.from == "group/gate" && edge.to == "group/status"));
    assert!(snap
        .edges
        .iter()
        .any(|edge| edge.from == "group/gate" && edge.to == "group/inbound/a"));
    assert!(snap
        .edges
        .iter()
        .any(|edge| edge.from == "group/commands" && edge.to == "bridge/command"));

    let bridge2 = wire_bridge::<WireBridgeProtobufDataBody, WireBridgeProtobufDataBody>(
        &g,
        WireBridgeOptions::named("session-b", "bridge2"),
    );
    let group2 = wire_edge_group(
        &g,
        &bridge2,
        WireEdgeGroupOptions::named(
            "group2",
            vec![
                WireEdgeGroupEdge::outbound("a", source_a),
                WireEdgeGroupEdge::outbound("b", source_b),
            ],
        ),
    );
    assert!(g
        .describe()
        .edges
        .iter()
        .any(|edge| edge.from == "group2/commands" && edge.to == "bridge2/command"));
    group2.release();
    assert!(!g
        .describe()
        .edges
        .iter()
        .any(|edge| edge.from == "group2/commands" && edge.to == "bridge2/command"));
}

#[test]
#[should_panic(
    expected = "wire_edge_group: inbound and outbound edges must be declared in separate groups"
)]
fn wire_edge_group_rejects_mixed_inbound_outbound_edges() {
    let g = graph();
    let source_a = g.state_empty_opts::<Vec<u8>>(GraphNodeOpts::named("mixed/edge/a"));
    let bridge = wire_bridge::<WireBridgeProtobufDataBody, WireBridgeProtobufDataBody>(
        &g,
        WireBridgeOptions::named("session-a", "mixed/bridge"),
    );

    let _group = wire_edge_group(
        &g,
        &bridge,
        WireEdgeGroupOptions::named(
            "mixed/group",
            vec![
                WireEdgeGroupEdge::outbound("a", source_a),
                WireEdgeGroupEdge::inbound("b"),
            ],
        ),
    );
}

#[test]
fn wire_edge_group_outbound_invalidate_clears_snapshot_before_next_cause() {
    let g = graph();
    let source_a = g.state_empty_opts::<Vec<u8>>(GraphNodeOpts::named("invalidate/edge/a"));
    let source_b = g.state_empty_opts::<Vec<u8>>(GraphNodeOpts::named("invalidate/edge/b"));
    let bridge = wire_bridge::<WireBridgeProtobufDataBody, WireBridgeProtobufDataBody>(
        &g,
        WireBridgeOptions::named("session-a", "invalidate/bridge"),
    );
    let group = wire_edge_group(
        &g,
        &bridge,
        WireEdgeGroupOptions::named(
            "invalidate/group",
            vec![
                WireEdgeGroupEdge::outbound("a", source_a.clone()),
                WireEdgeGroupEdge::outbound("b", source_b.clone()),
            ],
        ),
    );
    let outbound = collect_data(&bridge.outbound);
    let issues = collect_data(&group.issues);

    batch(|_| {
        source_a.set(vec![1]);
        source_b.set(vec![2]);
    });
    outbound.borrow_mut().clear();

    source_a.down(vec![Message::Invalidate]);
    assert!(
        outbound.borrow().is_empty(),
        "INVALIDATE must not resend the stale a=1 snapshot"
    );

    source_b.set(vec![3]);
    assert!(issues
        .borrow()
        .iter()
        .any(|issue| issue.code == WireEdgeGroupIssueCode::MissingSnapshot));
    assert!(outbound.borrow().is_empty());
    source_a.set(vec![4]);

    let data_frames = outbound
        .borrow()
        .iter()
        .filter_map(|envelope| match &envelope.payload {
            Some(WireBridgePayload::Data(WireBridgeProtobufDataBody::WireEdge(frame)))
                if frame.kind == CanonicalWireEdgeKind::Data =>
            {
                Some((frame.edge_id.clone(), frame.value.clone()))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        data_frames,
        vec![
            ("a".to_owned(), Some(vec![4])),
            ("b".to_owned(), Some(vec![3])),
        ]
    );
}

#[test]
fn wire_edge_group_outbound_edge_invalidate_preserves_other_pending_fresh_data() {
    let g = graph();
    let source_a = g.state_empty_opts::<Vec<u8>>(GraphNodeOpts::named("edge-invalidate/edge/a"));
    let source_b = g.state_empty_opts::<Vec<u8>>(GraphNodeOpts::named("edge-invalidate/edge/b"));
    let bridge = wire_bridge::<WireBridgeProtobufDataBody, WireBridgeProtobufDataBody>(
        &g,
        WireBridgeOptions::named("session-a", "edge-invalidate/bridge"),
    );
    let _group = wire_edge_group(
        &g,
        &bridge,
        WireEdgeGroupOptions::named(
            "edge-invalidate/group",
            vec![
                WireEdgeGroupEdge::outbound("a", source_a.clone()),
                WireEdgeGroupEdge::outbound("b", source_b.clone()),
            ],
        ),
    );
    let outbound = collect_data(&bridge.outbound);

    source_b.set(vec![3]);
    assert!(outbound.borrow().is_empty());

    source_a.down(vec![Message::Invalidate]);
    assert!(
        outbound.borrow().is_empty(),
        "D560: invalidating one edge must not emit or clear unrelated pending fresh DATA"
    );

    source_a.set(vec![4]);
    let data_frames = wire_edge_frames(&outbound)
        .into_iter()
        .filter(|frame| frame.kind == CanonicalWireEdgeKind::Data)
        .map(|frame| (frame.edge_id, frame.value))
        .collect::<Vec<_>>();
    assert_eq!(
        data_frames,
        vec![
            ("a".to_owned(), Some(vec![4])),
            ("b".to_owned(), Some(vec![3])),
        ]
    );
}

#[test]
fn wire_edge_group_outbound_initial_bootstrap_allows_one_current_cohort() {
    let g = graph();
    let source_a = g.state_empty_opts::<Vec<u8>>(GraphNodeOpts::named("bootstrap/edge/a"));
    let source_b = g.state_empty_opts::<Vec<u8>>(GraphNodeOpts::named("bootstrap/edge/b"));
    let bridge = wire_bridge::<WireBridgeProtobufDataBody, WireBridgeProtobufDataBody>(
        &g,
        WireBridgeOptions::named("session-a", "bootstrap/bridge"),
    );
    let outbound = collect_data(&bridge.outbound);

    source_a.set(vec![1]);
    source_b.set(vec![2]);
    let _group = wire_edge_group(
        &g,
        &bridge,
        WireEdgeGroupOptions::named(
            "bootstrap/group",
            vec![
                WireEdgeGroupEdge::outbound("a", source_a.clone()),
                WireEdgeGroupEdge::outbound("b", source_b.clone()),
            ],
        ),
    );

    let frames = wire_edge_frames(&outbound);
    assert_eq!(
        frames
            .iter()
            .map(|frame| (frame.kind, frame.edge_id.as_str(), frame.cause_id.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (CanonicalWireEdgeKind::Dirty, "a", "bootstrap/group:cause:1"),
            (CanonicalWireEdgeKind::Dirty, "b", "bootstrap/group:cause:1"),
            (CanonicalWireEdgeKind::Data, "a", "bootstrap/group:cause:1"),
            (CanonicalWireEdgeKind::Data, "b", "bootstrap/group:cause:1"),
        ],
        "D561: the first complete activation/current cohort is the only legal bootstrap"
    );
}

#[test]
fn wire_edge_group_outbound_late_subscriber_current_drain_does_not_admit_cause() {
    let g = graph();
    let source_a = g.state_empty_opts::<Vec<u8>>(GraphNodeOpts::named("late-drain/edge/a"));
    let source_b = g.state_empty_opts::<Vec<u8>>(GraphNodeOpts::named("late-drain/edge/b"));
    let bridge = wire_bridge::<WireBridgeProtobufDataBody, WireBridgeProtobufDataBody>(
        &g,
        WireBridgeOptions::named("session-a", "late-drain/bridge"),
    );
    let _group = wire_edge_group(
        &g,
        &bridge,
        WireEdgeGroupOptions::named(
            "late-drain/group",
            vec![
                WireEdgeGroupEdge::outbound("a", source_a.clone()),
                WireEdgeGroupEdge::outbound("b", source_b.clone()),
            ],
        ),
    );
    let outbound = collect_data(&bridge.outbound);

    batch(|_| {
        source_a.set(vec![1]);
        source_b.set(vec![2]);
    });
    assert_eq!(wire_edge_frames(&outbound).len(), 4);
    outbound.borrow_mut().clear();

    let late_seen = collect_data(&bridge.outbound);
    assert_eq!(
        late_seen.borrow().len(),
        1,
        "late subscriber receives bridge.outbound current DATA directly"
    );
    assert!(
        outbound.borrow().is_empty(),
        "D561: late-subscriber current drain must not cause WireEdgeGroup to admit a new cohort"
    );
}

#[test]
#[should_panic(
    expected = "wire_edge_group: outbound edge 'a' requires node runtime versioning for D561 fresh-source admission"
)]
fn wire_edge_group_outbound_rejects_disabled_versioning_edges() {
    let g = graph();
    let mut disabled_opts = GraphNodeOpts::named("disabled-version/edge/a");
    disabled_opts.node.versioning = Some(NodeVersioningPolicy::Disabled);
    let source_a = g.state_empty_opts::<Vec<u8>>(disabled_opts);
    let source_b = g.state_empty_opts::<Vec<u8>>(GraphNodeOpts::named("disabled-version/edge/b"));
    let bridge = wire_bridge::<WireBridgeProtobufDataBody, WireBridgeProtobufDataBody>(
        &g,
        WireBridgeOptions::named("session-a", "disabled-version/bridge"),
    );

    let _group = wire_edge_group(
        &g,
        &bridge,
        WireEdgeGroupOptions::named(
            "disabled-version/group",
            vec![
                WireEdgeGroupEdge::outbound("a", source_a),
                WireEdgeGroupEdge::outbound("b", source_b),
            ],
        ),
    );
}

#[test]
fn wire_edge_group_outbound_fresh_cohort_does_not_reuse_stale_snapshots() {
    let g = graph();
    let source_a = g.state_empty_opts::<Vec<u8>>(GraphNodeOpts::named("fresh/edge/a"));
    let source_b = g.state_empty_opts::<Vec<u8>>(GraphNodeOpts::named("fresh/edge/b"));
    let bridge = wire_bridge::<WireBridgeProtobufDataBody, WireBridgeProtobufDataBody>(
        &g,
        WireBridgeOptions::named("session-a", "fresh/bridge"),
    );
    let group = wire_edge_group(
        &g,
        &bridge,
        WireEdgeGroupOptions::named(
            "fresh/group",
            vec![
                WireEdgeGroupEdge::outbound("a", source_a.clone()),
                WireEdgeGroupEdge::outbound("b", source_b.clone()),
            ],
        ),
    );
    let outbound = collect_data(&bridge.outbound);
    let issues = collect_data(&group.issues);
    let status = collect_data(&group.status);

    batch(|_| {
        source_a.set(vec![1]);
        source_b.set(vec![2]);
    });
    assert_eq!(wire_edge_frames(&outbound).len(), 4);
    outbound.borrow_mut().clear();

    source_a.set(vec![10]);
    assert!(
        outbound.borrow().is_empty(),
        "D560: stale b=2 retained snapshot must not fill a new outbound cohort"
    );
    assert!(issues.borrow().iter().any(|issue| issue.code
        == WireEdgeGroupIssueCode::MissingSnapshot
        && issue.edge_id.as_deref() == Some("b")));
    assert_eq!(
        status.borrow().last().unwrap().state,
        WireEdgeGroupStatusState::Issues
    );

    source_b.set(vec![20]);
    let frames = wire_edge_frames(&outbound);
    assert_eq!(
        frames,
        vec![
            CanonicalWireEdgeFrame {
                kind: CanonicalWireEdgeKind::Dirty,
                edge_id: "a".to_owned(),
                cause_id: "fresh/group:cause:2".to_owned(),
                value: None,
            },
            CanonicalWireEdgeFrame {
                kind: CanonicalWireEdgeKind::Dirty,
                edge_id: "b".to_owned(),
                cause_id: "fresh/group:cause:2".to_owned(),
                value: None,
            },
            CanonicalWireEdgeFrame {
                kind: CanonicalWireEdgeKind::Data,
                edge_id: "a".to_owned(),
                cause_id: "fresh/group:cause:2".to_owned(),
                value: Some(vec![10]),
            },
            CanonicalWireEdgeFrame {
                kind: CanonicalWireEdgeKind::Data,
                edge_id: "b".to_owned(),
                cause_id: "fresh/group:cause:2".to_owned(),
                value: Some(vec![20]),
            },
        ]
    );
}

#[test]
fn wire_edge_group_outbound_same_bytes_from_fresh_events_still_form_cause() {
    let g = graph();
    let source_a = g.state_empty_opts::<Vec<u8>>(GraphNodeOpts::named("same-bytes/edge/a"));
    let source_b = g.state_empty_opts::<Vec<u8>>(GraphNodeOpts::named("same-bytes/edge/b"));
    let bridge = wire_bridge::<WireBridgeProtobufDataBody, WireBridgeProtobufDataBody>(
        &g,
        WireBridgeOptions::named("session-a", "same-bytes/bridge"),
    );
    let _group = wire_edge_group(
        &g,
        &bridge,
        WireEdgeGroupOptions::named(
            "same-bytes/group",
            vec![
                WireEdgeGroupEdge::outbound("a", source_a.clone()),
                WireEdgeGroupEdge::outbound("b", source_b.clone()),
            ],
        ),
    );
    let outbound = collect_data(&bridge.outbound);

    batch(|_| {
        source_a.set(vec![1]);
        source_b.set(vec![2]);
    });
    outbound.borrow_mut().clear();

    batch(|_| {
        source_a.set(vec![1]);
        source_b.set(vec![2]);
    });
    let data_frames = wire_edge_frames(&outbound)
        .into_iter()
        .filter(|frame| frame.kind == CanonicalWireEdgeKind::Data)
        .map(|frame| (frame.edge_id, frame.cause_id, frame.value))
        .collect::<Vec<_>>();
    assert_eq!(
        data_frames,
        vec![
            (
                "a".to_owned(),
                "same-bytes/group:cause:2".to_owned(),
                Some(vec![1])
            ),
            (
                "b".to_owned(),
                "same-bytes/group:cause:2".to_owned(),
                Some(vec![2])
            ),
        ],
        "D561: freshness is occurrence/version based, not payload equality"
    );
}

#[test]
fn wire_edge_group_outbound_duplicate_edge_data_latest_wins_before_cohort_completion() {
    let g = graph();
    let source_a = g.state_empty_opts::<Vec<u8>>(GraphNodeOpts::named("latest/edge/a"));
    let source_b = g.state_empty_opts::<Vec<u8>>(GraphNodeOpts::named("latest/edge/b"));
    let bridge = wire_bridge::<WireBridgeProtobufDataBody, WireBridgeProtobufDataBody>(
        &g,
        WireBridgeOptions::named("session-a", "latest/bridge"),
    );
    let _group = wire_edge_group(
        &g,
        &bridge,
        WireEdgeGroupOptions::named(
            "latest/group",
            vec![
                WireEdgeGroupEdge::outbound("a", source_a.clone()),
                WireEdgeGroupEdge::outbound("b", source_b.clone()),
            ],
        ),
    );
    let outbound = collect_data(&bridge.outbound);

    source_a.set(vec![1]);
    source_a.set(vec![2]);
    assert!(
        outbound.borrow().is_empty(),
        "partial fresh cohort must not emit DIRTY/DATA wire frames"
    );

    source_b.set(vec![3]);
    let data_frames = wire_edge_frames(&outbound)
        .into_iter()
        .filter(|frame| frame.kind == CanonicalWireEdgeKind::Data)
        .map(|frame| (frame.edge_id, frame.value))
        .collect::<Vec<_>>();
    assert_eq!(
        data_frames,
        vec![
            ("a".to_owned(), Some(vec![2])),
            ("b".to_owned(), Some(vec![3])),
        ]
    );
}

#[test]
fn wire_edge_group_outbound_malformed_data_clears_that_edge_pending_material() {
    let g = graph();
    let source_a = g.state_empty_opts::<Vec<u8>>(GraphNodeOpts::named("malformed/edge/a"));
    let source_b = g.state_empty_opts::<Vec<u8>>(GraphNodeOpts::named("malformed/edge/b"));
    let bridge = wire_bridge::<WireBridgeProtobufDataBody, WireBridgeProtobufDataBody>(
        &g,
        WireBridgeOptions::named("session-a", "malformed/bridge"),
    );
    let group = wire_edge_group(
        &g,
        &bridge,
        WireEdgeGroupOptions::named(
            "malformed/group",
            vec![
                WireEdgeGroupEdge::outbound("a", source_a.clone()),
                WireEdgeGroupEdge::outbound("b", source_b.clone()),
            ],
        ),
    );
    let outbound = collect_data(&bridge.outbound);
    let issues = collect_data(&group.issues);

    source_a.set(vec![1]);
    source_a.down(vec![Message::Data(Rc::new("not-bytes".to_owned()))]);
    assert!(issues
        .borrow()
        .iter()
        .any(|issue| issue.code == WireEdgeGroupIssueCode::MalformedFrame
            && issue.edge_id.as_deref() == Some("a")));

    source_b.set(vec![3]);
    assert!(
        outbound.borrow().is_empty(),
        "malformed outbound DATA must clear stale pending bytes for that edge"
    );

    source_a.set(vec![2]);
    let data_frames = wire_edge_frames(&outbound)
        .into_iter()
        .filter(|frame| frame.kind == CanonicalWireEdgeKind::Data)
        .map(|frame| (frame.edge_id, frame.value))
        .collect::<Vec<_>>();
    assert_eq!(
        data_frames,
        vec![
            ("a".to_owned(), Some(vec![2])),
            ("b".to_owned(), Some(vec![3])),
        ]
    );
}

#[test]
fn wire_edge_group_fail_closed_cases_are_issues_not_terminals() {
    let cases: Vec<(&str, WireEdgeGroupIssueCode, Vec<CanonicalWireEdgeFrame>)> = vec![
        (
            "unknown",
            WireEdgeGroupIssueCode::UnknownEdge,
            vec![CanonicalWireEdgeFrame {
                kind: CanonicalWireEdgeKind::Dirty,
                edge_id: "z".to_owned(),
                cause_id: "c1".to_owned(),
                value: None,
            }],
        ),
        (
            "duplicate-dirty",
            WireEdgeGroupIssueCode::DuplicateDirty,
            vec![
                CanonicalWireEdgeFrame {
                    kind: CanonicalWireEdgeKind::Dirty,
                    edge_id: "a".to_owned(),
                    cause_id: "c1".to_owned(),
                    value: None,
                },
                CanonicalWireEdgeFrame {
                    kind: CanonicalWireEdgeKind::Dirty,
                    edge_id: "a".to_owned(),
                    cause_id: "c1".to_owned(),
                    value: None,
                },
            ],
        ),
        (
            "duplicate-data",
            WireEdgeGroupIssueCode::DuplicateData,
            vec![
                CanonicalWireEdgeFrame {
                    kind: CanonicalWireEdgeKind::Dirty,
                    edge_id: "a".to_owned(),
                    cause_id: "c1".to_owned(),
                    value: None,
                },
                CanonicalWireEdgeFrame {
                    kind: CanonicalWireEdgeKind::Dirty,
                    edge_id: "b".to_owned(),
                    cause_id: "c1".to_owned(),
                    value: None,
                },
                CanonicalWireEdgeFrame {
                    kind: CanonicalWireEdgeKind::Data,
                    edge_id: "a".to_owned(),
                    cause_id: "c1".to_owned(),
                    value: Some(vec![1]),
                },
                CanonicalWireEdgeFrame {
                    kind: CanonicalWireEdgeKind::Data,
                    edge_id: "a".to_owned(),
                    cause_id: "c1".to_owned(),
                    value: Some(vec![2]),
                },
            ],
        ),
        (
            "data-before-dirty",
            WireEdgeGroupIssueCode::DataBeforeDirty,
            vec![CanonicalWireEdgeFrame {
                kind: CanonicalWireEdgeKind::Data,
                edge_id: "a".to_owned(),
                cause_id: "c1".to_owned(),
                value: Some(vec![1]),
            }],
        ),
        (
            "competing",
            WireEdgeGroupIssueCode::CompetingCause,
            vec![
                CanonicalWireEdgeFrame {
                    kind: CanonicalWireEdgeKind::Dirty,
                    edge_id: "a".to_owned(),
                    cause_id: "c1".to_owned(),
                    value: None,
                },
                CanonicalWireEdgeFrame {
                    kind: CanonicalWireEdgeKind::Dirty,
                    edge_id: "b".to_owned(),
                    cause_id: "c2".to_owned(),
                    value: None,
                },
            ],
        ),
        (
            "malformed-poisons-cause",
            WireEdgeGroupIssueCode::MalformedFrame,
            vec![
                CanonicalWireEdgeFrame {
                    kind: CanonicalWireEdgeKind::Dirty,
                    edge_id: "a".to_owned(),
                    cause_id: "c1".to_owned(),
                    value: None,
                },
                CanonicalWireEdgeFrame {
                    kind: CanonicalWireEdgeKind::Data,
                    edge_id: "b".to_owned(),
                    cause_id: "c1".to_owned(),
                    value: None,
                },
                CanonicalWireEdgeFrame {
                    kind: CanonicalWireEdgeKind::Dirty,
                    edge_id: "b".to_owned(),
                    cause_id: "c1".to_owned(),
                    value: None,
                },
                CanonicalWireEdgeFrame {
                    kind: CanonicalWireEdgeKind::Data,
                    edge_id: "a".to_owned(),
                    cause_id: "c1".to_owned(),
                    value: Some(vec![1]),
                },
                CanonicalWireEdgeFrame {
                    kind: CanonicalWireEdgeKind::Data,
                    edge_id: "b".to_owned(),
                    cause_id: "c1".to_owned(),
                    value: Some(vec![2]),
                },
            ],
        ),
        (
            "unknown-competing-cause",
            WireEdgeGroupIssueCode::CompetingCause,
            vec![
                CanonicalWireEdgeFrame {
                    kind: CanonicalWireEdgeKind::Dirty,
                    edge_id: "a".to_owned(),
                    cause_id: "c1".to_owned(),
                    value: None,
                },
                CanonicalWireEdgeFrame {
                    kind: CanonicalWireEdgeKind::Dirty,
                    edge_id: "z".to_owned(),
                    cause_id: "c2".to_owned(),
                    value: None,
                },
                CanonicalWireEdgeFrame {
                    kind: CanonicalWireEdgeKind::Dirty,
                    edge_id: "b".to_owned(),
                    cause_id: "c1".to_owned(),
                    value: None,
                },
                CanonicalWireEdgeFrame {
                    kind: CanonicalWireEdgeKind::Data,
                    edge_id: "a".to_owned(),
                    cause_id: "c1".to_owned(),
                    value: Some(vec![1]),
                },
                CanonicalWireEdgeFrame {
                    kind: CanonicalWireEdgeKind::Data,
                    edge_id: "b".to_owned(),
                    cause_id: "c1".to_owned(),
                    value: Some(vec![2]),
                },
            ],
        ),
    ];
    for (name, code, frames) in cases {
        let g = graph();
        let bridge = wire_bridge::<WireBridgeProtobufDataBody, WireBridgeProtobufDataBody>(
            &g,
            WireBridgeOptions::named("session-a", format!("{name}/bridge")),
        );
        let group = wire_edge_group(
            &g,
            &bridge,
            WireEdgeGroupOptions::named(
                name,
                vec![
                    WireEdgeGroupEdge::inbound("a"),
                    WireEdgeGroupEdge::inbound("b"),
                ],
            ),
        );
        let inbound_a = collect_data(group.inbound.get("a").expect("edge a exists"));
        let issues = collect_data(&group.issues);
        for (index, frame) in frames.into_iter().enumerate() {
            bridge.inbound.set(envelope(
                "session-a",
                WireBridgeEnvelopeType::Data,
                u64::try_from(index + 1).unwrap(),
                Some(WireBridgePayload::Data(
                    WireBridgeProtobufDataBody::WireEdge(frame),
                )),
                None,
                None,
            ));
        }
        assert!(inbound_a.borrow().is_empty());
        assert!(issues.borrow().iter().any(|issue| issue.code == code));
        assert_ne!(group.issues.status(), graphrefly::Status::Errored);
        assert_ne!(group.status.status(), graphrefly::Status::Errored);
    }
}

#[test]
fn wire_edge_group_released_cause_id_replay_is_issue_not_second_release() {
    let g = graph();
    let bridge = wire_bridge::<WireBridgeProtobufDataBody, WireBridgeProtobufDataBody>(
        &g,
        WireBridgeOptions::named("session-a", "replay/bridge"),
    );
    let group = wire_edge_group(
        &g,
        &bridge,
        WireEdgeGroupOptions::named(
            "replay",
            vec![
                WireEdgeGroupEdge::inbound("a"),
                WireEdgeGroupEdge::inbound("b"),
            ],
        ),
    );
    let inbound_a = collect_data(group.inbound.get("a").expect("edge a exists"));
    let issues = collect_data(&group.issues);
    let status = collect_data(&group.status);

    for (seq, kind, edge_id, value) in [
        (1, CanonicalWireEdgeKind::Dirty, "a", None),
        (2, CanonicalWireEdgeKind::Dirty, "b", None),
        (3, CanonicalWireEdgeKind::Data, "a", Some(vec![1])),
        (4, CanonicalWireEdgeKind::Data, "b", Some(vec![2])),
    ] {
        bridge
            .inbound
            .set(wire_edge_envelope(seq, kind, edge_id, "c1", value));
    }
    assert_eq!(*inbound_a.borrow(), vec![vec![1]]);
    assert_eq!(
        status.borrow().last().unwrap().state,
        WireEdgeGroupStatusState::Released
    );
    assert_eq!(status.borrow().last().unwrap().active_cause_id, None);
    assert_eq!(status.borrow().last().unwrap().dirty, 0);
    assert_eq!(status.borrow().last().unwrap().data, 0);

    for (seq, kind, edge_id, value) in [
        (5, CanonicalWireEdgeKind::Dirty, "a", None),
        (6, CanonicalWireEdgeKind::Dirty, "b", None),
        (7, CanonicalWireEdgeKind::Data, "a", Some(vec![10])),
        (8, CanonicalWireEdgeKind::Data, "b", Some(vec![20])),
    ] {
        bridge
            .inbound
            .set(wire_edge_envelope(seq, kind, edge_id, "c1", value));
    }

    assert_eq!(*inbound_a.borrow(), vec![vec![1]]);
    assert!(issues.borrow().iter().any(|issue| {
        issue.cause_id.as_deref() == Some("c1")
            && matches!(
                issue.code,
                WireEdgeGroupIssueCode::DuplicateDirty | WireEdgeGroupIssueCode::DuplicateData
            )
    }));
    assert_eq!(
        status.borrow().last().unwrap().state,
        WireEdgeGroupStatusState::Issues
    );
    assert_eq!(status.borrow().last().unwrap().active_cause_id, None);
    assert_eq!(status.borrow().last().unwrap().dirty, 0);
    assert_eq!(status.borrow().last().unwrap().data, 0);
}

#[test]
fn wire_edge_group_failed_cause_replay_is_issue_not_resurrection() {
    let g = graph();
    let bridge = wire_bridge::<WireBridgeProtobufDataBody, WireBridgeProtobufDataBody>(
        &g,
        WireBridgeOptions::named("session-a", "failed-replay/bridge"),
    );
    let group = wire_edge_group(
        &g,
        &bridge,
        WireEdgeGroupOptions::named(
            "failed-replay",
            vec![
                WireEdgeGroupEdge::inbound("a"),
                WireEdgeGroupEdge::inbound("b"),
            ],
        ),
    );
    let inbound_a = collect_data(group.inbound.get("a").expect("edge a exists"));
    let inbound_b = collect_data(group.inbound.get("b").expect("edge b exists"));
    let issues = collect_data(&group.issues);
    let status = collect_data(&group.status);

    for (seq, kind, edge_id, value) in [
        (1, CanonicalWireEdgeKind::Dirty, "a", None),
        (2, CanonicalWireEdgeKind::Dirty, "b", None),
        (3, CanonicalWireEdgeKind::Data, "a", Some(vec![1])),
        (4, CanonicalWireEdgeKind::Data, "a", Some(vec![2])),
        (5, CanonicalWireEdgeKind::Data, "b", Some(vec![3])),
        (6, CanonicalWireEdgeKind::Dirty, "a", None),
        (7, CanonicalWireEdgeKind::Dirty, "b", None),
        (8, CanonicalWireEdgeKind::Data, "a", Some(vec![4])),
        (9, CanonicalWireEdgeKind::Data, "b", Some(vec![5])),
    ] {
        bridge
            .inbound
            .set(wire_edge_envelope(seq, kind, edge_id, "c1", value));
    }

    assert!(inbound_a.borrow().is_empty());
    assert!(inbound_b.borrow().is_empty());
    assert!(issues
        .borrow()
        .iter()
        .any(|issue| issue.code == WireEdgeGroupIssueCode::DuplicateData));
    assert!(issues
        .borrow()
        .iter()
        .any(|issue| issue.code == WireEdgeGroupIssueCode::IncompleteCause));
    assert_eq!(
        status.borrow().last().unwrap().state,
        WireEdgeGroupStatusState::Issues
    );
    assert_eq!(status.borrow().last().unwrap().active_cause_id, None);
    assert_eq!(status.borrow().last().unwrap().dirty, 0);
    assert_eq!(status.borrow().last().unwrap().data, 0);
    assert_ne!(group.issues.status(), graphrefly::Status::Errored);
    assert_ne!(group.status.status(), graphrefly::Status::Errored);
}

#[test]
fn wire_edge_group_replay_tombstones_are_bounded_recent_memory() {
    let g = graph();
    let bridge = wire_bridge::<WireBridgeProtobufDataBody, WireBridgeProtobufDataBody>(
        &g,
        WireBridgeOptions::named("session-a", "bounded-replay/bridge"),
    );
    let group = wire_edge_group(
        &g,
        &bridge,
        WireEdgeGroupOptions::named("bounded-replay", vec![WireEdgeGroupEdge::inbound("a")]),
    );
    let inbound_a = collect_data(group.inbound.get("a").expect("edge a exists"));
    let issues = collect_data(&group.issues);
    let mut seq = 1_u64;
    let release = |seq: &mut u64, cause_id: &str, value: u8| {
        bridge.inbound.set(wire_edge_envelope(
            *seq,
            CanonicalWireEdgeKind::Dirty,
            "a",
            cause_id,
            None,
        ));
        *seq += 1;
        bridge.inbound.set(wire_edge_envelope(
            *seq,
            CanonicalWireEdgeKind::Data,
            "a",
            cause_id,
            Some(vec![value]),
        ));
        *seq += 1;
    };

    release(&mut seq, "c1", 1);
    bridge.inbound.set(wire_edge_envelope(
        seq,
        CanonicalWireEdgeKind::Dirty,
        "a",
        "c1",
        None,
    ));
    assert!(issues.borrow().iter().any(|issue| {
        issue.cause_id.as_deref() == Some("c1")
            && issue.code == WireEdgeGroupIssueCode::DuplicateDirty
    }));

    for n in 2_u16..=1026 {
        release(&mut seq, &format!("c{n}"), (n % 256) as u8);
    }
    let release_count_before_old_replay = inbound_a.borrow().len();
    release(&mut seq, "c1", 99);

    assert_eq!(
        inbound_a.borrow().len(),
        release_count_before_old_replay + 1
    );
    assert_eq!(inbound_a.borrow().last(), Some(&vec![99]));
}

#[test]
fn wire_edge_group_status_and_issues_only_cover_malformed_and_competing_causes() {
    let g = graph();
    let bridge = wire_bridge::<WireBridgeProtobufDataBody, WireBridgeProtobufDataBody>(
        &g,
        WireBridgeOptions::named("session-a", "status-only/bridge"),
    );
    let group = wire_edge_group(
        &g,
        &bridge,
        WireEdgeGroupOptions::named(
            "status-only",
            vec![
                WireEdgeGroupEdge::inbound("a"),
                WireEdgeGroupEdge::inbound("b"),
            ],
        ),
    );
    let status = collect_data(&group.status);
    let issues = collect_data(&group.issues);

    bridge.inbound.set(wire_edge_envelope(
        1,
        CanonicalWireEdgeKind::Dirty,
        "a",
        "c1",
        None,
    ));
    bridge.inbound.set(wire_edge_envelope(
        2,
        CanonicalWireEdgeKind::Data,
        "b",
        "c1",
        None,
    ));
    bridge.inbound.set(wire_edge_envelope(
        3,
        CanonicalWireEdgeKind::Dirty,
        "a",
        "c2",
        None,
    ));
    bridge.inbound.set(wire_edge_envelope(
        4,
        CanonicalWireEdgeKind::Dirty,
        "b",
        "c3",
        None,
    ));

    assert!(issues
        .borrow()
        .iter()
        .any(|issue| issue.code == WireEdgeGroupIssueCode::MalformedFrame));
    assert!(issues
        .borrow()
        .iter()
        .any(|issue| issue.code == WireEdgeGroupIssueCode::CompetingCause));
    assert_eq!(
        status.borrow().last().unwrap().state,
        WireEdgeGroupStatusState::Issues
    );
    assert_ne!(group.issues.status(), graphrefly::Status::Errored);
    assert_ne!(group.status.status(), graphrefly::Status::Errored);
}

#[test]
fn wire_edge_group_status_first_subscription_still_releases_inbound() {
    let g = graph();
    let bridge = wire_bridge::<WireBridgeProtobufDataBody, WireBridgeProtobufDataBody>(
        &g,
        WireBridgeOptions::named("session-a", "order/bridge"),
    );
    let group = wire_edge_group(
        &g,
        &bridge,
        WireEdgeGroupOptions::named(
            "order",
            vec![
                WireEdgeGroupEdge::inbound("a"),
                WireEdgeGroupEdge::inbound("b"),
            ],
        ),
    );
    let status = collect_data(&group.status);
    let issues = collect_data(&group.issues);
    let inbound_a = collect_data(group.inbound.get("a").expect("edge a exists"));

    bridge.inbound.set(wire_edge_envelope(
        1,
        CanonicalWireEdgeKind::Dirty,
        "a",
        "c1",
        None,
    ));
    bridge.inbound.set(wire_edge_envelope(
        2,
        CanonicalWireEdgeKind::Dirty,
        "b",
        "c1",
        None,
    ));
    bridge.inbound.set(wire_edge_envelope(
        3,
        CanonicalWireEdgeKind::Data,
        "a",
        "c1",
        Some(vec![1]),
    ));
    bridge.inbound.set(wire_edge_envelope(
        4,
        CanonicalWireEdgeKind::Data,
        "b",
        "c1",
        Some(vec![2]),
    ));

    assert_eq!(*inbound_a.borrow(), vec![vec![1]]);
    assert!(issues.borrow().is_empty());
    assert_eq!(
        status.borrow().last().unwrap().state,
        WireEdgeGroupStatusState::Released
    );
    assert_eq!(status.borrow().last().unwrap().active_cause_id, None);
    assert_eq!(status.borrow().last().unwrap().dirty, 0);
    assert_eq!(status.borrow().last().unwrap().data, 0);
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
fn remote_call_invalid_outbound_command_does_not_register_unsent_pending_request() {
    let g = graph();
    let bridge = wire_bridge::<RemoteCallRequest<String>, RemoteCallResponse<String>>(
        &g,
        WireBridgeOptions::named("session-a", "bridge"),
    );
    let remote = remote_call(&g, &bridge);
    let results = collect_data(&remote.results);
    let status = collect_data(&remote.status);
    let errors = collect_data(&remote.errors);

    remote.call_with_options("echo", "req-1", "payload".to_owned(), Some(String::new()));

    let current_status = status.borrow().last().unwrap().clone();
    assert_eq!(current_status.state, RemoteCallStatusState::BridgeErrored);
    assert_eq!(current_status.pending, 0);
    assert!(errors
        .borrow()
        .last()
        .unwrap()
        .error
        .contains("idempotency"));

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

    assert!(
        results.borrow().is_empty(),
        "a request rejected before outbound emission must not accept a later response"
    );
    assert_eq!(status.borrow().last().unwrap().pending, 0);
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
fn remote_call_wrong_session_response_does_not_consume_pending_request() {
    let g = graph();
    let bridge = wire_bridge::<RemoteCallRequest<String>, RemoteCallResponse<String>>(
        &g,
        WireBridgeOptions::named("session-a", "bridge"),
    );
    let remote = remote_call(&g, &bridge);
    let results = collect_data(&remote.results);
    let status = collect_data(&remote.status);

    remote.call("echo", "req-1", "payload".to_owned());
    bridge.inbound.set(envelope_with_request(
        "wrong-session",
        WireBridgeEnvelopeType::Data,
        1,
        Some(WireBridgePayload::Data(RemoteCallResponse::Result {
            operation: "echo".to_owned(),
            request_id: "req-1".to_owned(),
            payload: "wrong".to_owned(),
        })),
        Some("req-1"),
    ));

    let mismatch_status = status.borrow().last().unwrap().clone();
    assert_eq!(mismatch_status.state, RemoteCallStatusState::BridgeErrored);
    assert_eq!(mismatch_status.pending, 1);
    assert!(results.borrow().is_empty());

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
fn remote_call_duplicate_after_completion_is_visible_in_same_event_batch() {
    let g = graph();
    let bridge = wire_bridge::<RemoteCallRequest<String>, RemoteCallResponse<String>>(
        &g,
        WireBridgeOptions::named("session-a", "bridge"),
    );
    let remote = remote_call(&g, &bridge);
    let results = collect_data(&remote.results);
    let errors = collect_data(&remote.errors);

    remote.call("echo", "req-1", "payload".to_owned());
    let inbound_events: Vec<Message<Rc<dyn Any>>> = vec![
        Message::Data(Rc::new(WireBridgeEvent::<
            RemoteCallRequest<String>,
            RemoteCallResponse<String>,
        >::Inbound {
            envelope: envelope_with_request(
                "session-a",
                WireBridgeEnvelopeType::Data,
                1,
                Some(WireBridgePayload::Data(RemoteCallResponse::Result {
                    operation: "echo".to_owned(),
                    request_id: "req-1".to_owned(),
                    payload: "first".to_owned(),
                })),
                Some("req-1"),
            ),
        }) as Rc<dyn Any>),
        Message::Data(Rc::new(WireBridgeEvent::<
            RemoteCallRequest<String>,
            RemoteCallResponse<String>,
        >::Inbound {
            envelope: envelope_with_request(
                "session-a",
                WireBridgeEnvelopeType::Data,
                2,
                Some(WireBridgePayload::Data(RemoteCallResponse::Result {
                    operation: "echo".to_owned(),
                    request_id: "req-1".to_owned(),
                    payload: "duplicate".to_owned(),
                })),
                Some("req-1"),
            ),
        }) as Rc<dyn Any>),
    ];
    bridge.events.down(inbound_events);

    assert_eq!(
        *results.borrow(),
        vec![RemoteCallResult {
            operation: "echo".to_owned(),
            request_id: "req-1".to_owned(),
            payload: "first".to_owned(),
        }]
    );
    assert!(errors.borrow().iter().any(
        |error| error.error == "remote_call: orphan response for unknown or completed request"
    ));
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
            Message::Pull(_) => "PULL",
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

#[test]
fn remote_responder_release_detaches_response_commands_from_long_lived_bridge() {
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
    let outbound = collect_data(&bridge.outbound);
    assert!(g
        .describe()
        .edges
        .iter()
        .any(|edge| { edge.from == "responder/responseCommands" && edge.to == "bridge/command" }));

    responder.release();
    responder.release();

    assert!(g.find("responder/responseCommands").is_none());
    assert!(!g
        .describe()
        .edges
        .iter()
        .any(|edge| { edge.from == "responder/responseCommands" && edge.to == "bridge/command" }));
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
    assert!(
        outbound.borrow().is_empty(),
        "released responder must not publish response command facts"
    );
}

#[test]
fn remote_responder_release_is_retryable_when_external_subscriber_blocks_release() {
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
    let outbound = collect_data(&bridge.outbound);
    let unsub = responder.status.subscribe(|_| {});

    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| responder.release())).is_err()
    );
    assert!(g
        .describe()
        .edges
        .iter()
        .any(|edge| { edge.from == "responder/responseCommands" && edge.to == "bridge/command" }));

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
    assert_eq!(outbound.borrow().len(), 1);

    unsub();
    responder.release();
    assert!(g.find("responder/status").is_none());
}
