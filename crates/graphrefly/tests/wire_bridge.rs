use std::cell::RefCell;
use std::rc::Rc;

use graphrefly::{
    graph, wire_bridge, wire_bridge_envelope, Message, WireBridgeEnvelopeInput,
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
