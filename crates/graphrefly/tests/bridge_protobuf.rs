use std::fs;
use std::path::PathBuf;

use graphrefly::{
    decode_canonical_wire_bridge_envelope, decode_canonical_wire_edge_frame,
    decode_wire_bridge_protobuf_bytes, encode_canonical_wire_bridge_envelope,
    encode_canonical_wire_edge_frame, encode_wire_bridge_protobuf_bytes,
    CanonicalProtobufErrorCategory, CanonicalWireBridgeDataBody, CanonicalWireBridgeEnvelope,
    CanonicalWireBridgeMetadata, CanonicalWireBridgePayload, CanonicalWireEdgeKind,
    WireBridgeProtobufDataBody, WireBridgeProtobufPayload, WireBridgeProtobufStatusKind,
    WIRE_BRIDGE_PROTOBUF_HELPER_SHAPE,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct VectorRecord {
    schema: String,
    id: String,
    message: String,
    description: String,
    hex: String,
    canonical: bool,
    #[serde(rename = "errorCategory")]
    error_category: Option<String>,
}

fn fixture_path() -> PathBuf {
    spec_root().join("fixtures/protobuf/wire_bridge_envelope.v1.jsonl")
}

fn wire_edge_fixture_path() -> PathBuf {
    spec_root().join("fixtures/protobuf/wire_edge_frame.v1.jsonl")
}

fn spec_root() -> PathBuf {
    std::env::var_os("GRAPHREFLY_SPEC_ROOT").map_or_else(
        || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../graphrefly/spec"),
        PathBuf::from,
    )
}

fn vectors(path: PathBuf, message: &str) -> Vec<VectorRecord> {
    fs::read_to_string(path)
        .expect("fixture file is readable")
        .lines()
        .enumerate()
        .map(|(idx, line)| {
            let record: VectorRecord = serde_json::from_str(line).expect("fixture record is JSON");
            assert_eq!(
                record.schema,
                "graphrefly.protobuf.golden.v1",
                "schema line {}",
                idx + 1
            );
            assert_eq!(record.message, message, "message line {}", idx + 1);
            assert!(
                record.id.starts_with("positive.") || record.id.starts_with("negative."),
                "id line {}",
                idx + 1
            );
            assert!(
                !record.description.is_empty(),
                "description line {}",
                idx + 1
            );
            assert!(
                record.hex.len().is_multiple_of(2)
                    && record.hex.chars().all(|ch| ch.is_ascii_hexdigit()),
                "hex line {}",
                idx + 1
            );
            if record.canonical {
                assert!(
                    record.error_category.is_none(),
                    "{} must not declare an error",
                    record.id
                );
            } else {
                assert!(
                    record.error_category.is_some(),
                    "{} must declare an error",
                    record.id
                );
            }
            record
        })
        .collect()
}

fn from_hex(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|idx| u8::from_str_radix(&hex[idx..idx + 2], 16).expect("valid hex byte"))
        .collect()
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[test]
fn fixture_records_have_schema_and_positive_negative_coverage() {
    let records = vectors(fixture_path(), "WireBridgeEnvelope");
    assert!(records.iter().any(|record| record.canonical));
    assert!(records.iter().any(|record| !record.canonical));
}

#[test]
fn positive_vectors_decode_validate_and_reencode_byte_equal() {
    for record in vectors(fixture_path(), "WireBridgeEnvelope")
        .into_iter()
        .filter(|record| record.canonical)
    {
        let bytes = from_hex(&record.hex);
        let decoded = decode_canonical_wire_bridge_envelope(&bytes)
            .unwrap_or_else(|err| panic!("{} unexpectedly failed: {err}", record.id));
        let encoded = encode_canonical_wire_bridge_envelope(&decoded)
            .unwrap_or_else(|err| panic!("{} unexpectedly failed to encode: {err}", record.id));
        assert_eq!(to_hex(&encoded), record.hex, "{}", record.id);

        if record.id == "positive.data.wire_edge_dirty" {
            match &decoded.payload {
                CanonicalWireBridgePayload::Data(CanonicalWireBridgeDataBody::WireEdge(frame)) => {
                    assert_eq!(frame.kind, CanonicalWireEdgeKind::Dirty);
                    assert!(frame.value.is_none());
                }
                _ => panic!("{} decoded to the wrong payload", record.id),
            }
        }
        if record.id == "positive.data.wire_edge_data" {
            match &decoded.payload {
                CanonicalWireBridgePayload::Data(CanonicalWireBridgeDataBody::WireEdge(frame)) => {
                    assert_eq!(frame.kind, CanonicalWireEdgeKind::Data);
                    assert!(frame.value.is_some());
                }
                _ => panic!("{} decoded to the wrong payload", record.id),
            }
        }
    }
}

#[test]
fn positive_vectors_round_trip_through_focused_byte_helper() {
    for record in vectors(fixture_path(), "WireBridgeEnvelope")
        .into_iter()
        .filter(|record| record.canonical)
    {
        let bytes = from_hex(&record.hex);
        let decoded = decode_wire_bridge_protobuf_bytes(&bytes);
        assert_eq!(
            decoded.status.kind,
            WireBridgeProtobufStatusKind::Valid,
            "{}",
            record.id
        );
        assert!(decoded.issues.is_empty(), "{}", record.id);
        let envelope = decoded
            .envelope
            .unwrap_or_else(|| panic!("{} decoded without envelope", record.id));
        let encoded = encode_wire_bridge_protobuf_bytes(&envelope);
        assert_eq!(
            encoded.status.kind,
            WireBridgeProtobufStatusKind::Valid,
            "{}",
            record.id
        );
        assert!(encoded.issues.is_empty(), "{}", record.id);
        assert_eq!(to_hex(&encoded.bytes.unwrap()), record.hex, "{}", record.id);
    }
}

#[test]
fn negative_vectors_fail_by_semantic_category() {
    for record in vectors(fixture_path(), "WireBridgeEnvelope")
        .into_iter()
        .filter(|record| !record.canonical)
    {
        let bytes = from_hex(&record.hex);
        let err = match decode_canonical_wire_bridge_envelope(&bytes) {
            Ok(_) => panic!("{} unexpectedly decoded", record.id),
            Err(err) => err,
        };
        assert_eq!(
            err.category.as_str(),
            record.error_category.as_deref().unwrap(),
            "{}",
            record.id
        );
    }
}

#[test]
fn negative_vectors_become_invalid_helper_results_not_protocol_terminals() {
    for record in vectors(fixture_path(), "WireBridgeEnvelope")
        .into_iter()
        .filter(|record| !record.canonical)
    {
        let bytes = from_hex(&record.hex);
        let decoded = decode_wire_bridge_protobuf_bytes(&bytes);
        assert_eq!(
            decoded.status.kind,
            WireBridgeProtobufStatusKind::Invalid,
            "{}",
            record.id
        );
        assert!(decoded.envelope.is_none(), "{}", record.id);
        assert_eq!(decoded.issues.len(), 1, "{}", record.id);
        assert_eq!(
            decoded.issues[0].category.as_str(),
            record.error_category.as_deref().unwrap(),
            "{}",
            record.id
        );
    }
}

#[test]
fn focused_byte_helper_accepts_empty_data_values() {
    let records = vectors(fixture_path(), "WireBridgeEnvelope");
    let empty_value = records
        .iter()
        .find(|record| record.id == "positive.data.empty_value")
        .expect("empty DATA value fixture exists");
    let decoded = decode_wire_bridge_protobuf_bytes(&from_hex(&empty_value.hex));
    let envelope = decoded.envelope.expect("empty DATA value decodes");
    match envelope.payload {
        WireBridgeProtobufPayload::Data(WireBridgeProtobufDataBody::Value(value)) => {
            assert!(value.is_empty());
        }
        _ => panic!("empty DATA value decoded to wrong payload"),
    }

    let empty_wire_edge_value = records
        .iter()
        .find(|record| record.id == "positive.data.wire_edge_data_empty_value")
        .expect("empty wire-edge DATA value fixture exists");
    let decoded = decode_wire_bridge_protobuf_bytes(&from_hex(&empty_wire_edge_value.hex));
    let envelope = decoded
        .envelope
        .expect("empty wire-edge DATA value decodes");
    match envelope.payload {
        WireBridgeProtobufPayload::Data(WireBridgeProtobufDataBody::WireEdge(frame)) => {
            assert_eq!(frame.kind, CanonicalWireEdgeKind::Data);
            assert_eq!(frame.value, Some(Vec::new()));
        }
        _ => panic!("empty wire-edge DATA value decoded to wrong payload"),
    }
}

#[test]
fn focused_byte_helper_rejects_old_wireframe_shape_as_issue_result() {
    let record = vectors(fixture_path(), "WireBridgeEnvelope")
        .into_iter()
        .find(|record| record.id == "negative.old_wireframe_shape")
        .expect("old WireFrame negative fixture exists");
    let decoded = decode_wire_bridge_protobuf_bytes(&from_hex(&record.hex));
    assert_eq!(decoded.status.kind, WireBridgeProtobufStatusKind::Invalid);
    assert!(decoded.envelope.is_none());
    assert_eq!(decoded.issues[0].category.as_str(), "unknown_field");
}

#[test]
fn focused_byte_helper_shape_stays_adapter_local() {
    let shape = std::hint::black_box(WIRE_BRIDGE_PROTOBUF_HELPER_SHAPE);
    assert!(shape.byte_specific);
    assert!(shape.semantic_wire_bridge_dto);
    assert!(!shape.core_wire_bridge_options);
    assert!(!shape.protocol_surface);
    assert!(!shape.value_codec_registry);
}

#[test]
fn standalone_wire_edge_vectors_decode_validate_and_reencode_byte_equal() {
    for record in vectors(wire_edge_fixture_path(), "WireEdgeFrame")
        .into_iter()
        .filter(|record| record.canonical)
    {
        let bytes = from_hex(&record.hex);
        let decoded = decode_canonical_wire_edge_frame(&bytes)
            .unwrap_or_else(|err| panic!("{} unexpectedly failed: {err}", record.id));
        let encoded = encode_canonical_wire_edge_frame(&decoded)
            .unwrap_or_else(|err| panic!("{} unexpectedly failed to encode: {err}", record.id));
        assert_eq!(to_hex(&encoded), record.hex, "{}", record.id);
    }
}

#[test]
fn standalone_wire_edge_negative_vectors_fail_by_semantic_category() {
    for record in vectors(wire_edge_fixture_path(), "WireEdgeFrame")
        .into_iter()
        .filter(|record| !record.canonical)
    {
        let bytes = from_hex(&record.hex);
        let err = match decode_canonical_wire_edge_frame(&bytes) {
            Ok(_) => panic!("{} unexpectedly decoded", record.id),
            Err(err) => err,
        };
        assert_eq!(
            err.category.as_str(),
            record.error_category.as_deref().unwrap(),
            "{}",
            record.id
        );
    }
}

#[test]
fn encode_rejects_empty_bridge_fact_bytes() {
    let metadata = CanonicalWireBridgeMetadata {
        seq: 1,
        cursor: 0,
        idempotency_key: "s1:1".to_owned(),
        attempt: 1,
        max_attempts: 1,
        timestamp_ms: None,
        ack_for_seq: None,
        request_id: None,
    };
    for payload in [
        CanonicalWireBridgePayload::Status { status: vec![] },
        CanonicalWireBridgePayload::Error { error: vec![] },
        CanonicalWireBridgePayload::Close {
            reason: Some(vec![]),
        },
    ] {
        let envelope = CanonicalWireBridgeEnvelope {
            session_id: "s1".to_owned(),
            metadata: metadata.clone(),
            payload,
        };
        assert!(encode_canonical_wire_bridge_envelope(&envelope).is_err());
    }

    let envelope = CanonicalWireBridgeEnvelope {
        session_id: "s1".to_owned(),
        metadata: CanonicalWireBridgeMetadata {
            ack_for_seq: Some(1),
            ..metadata
        },
        payload: CanonicalWireBridgePayload::Nack {
            error: Some(vec![]),
        },
    };
    assert!(encode_canonical_wire_bridge_envelope(&envelope).is_err());
}

#[test]
fn category_strings_match_d497_fixture_vocabulary() {
    assert_eq!(
        CanonicalProtobufErrorCategory::UnknownField.as_str(),
        "unknown_field"
    );
    assert_eq!(
        CanonicalProtobufErrorCategory::DuplicateSingular.as_str(),
        "duplicate_singular"
    );
    assert_eq!(
        CanonicalProtobufErrorCategory::NoncanonicalBytes.as_str(),
        "noncanonical_bytes"
    );
    assert_eq!(
        CanonicalProtobufErrorCategory::InvalidOneof.as_str(),
        "invalid_oneof"
    );
    assert_eq!(
        CanonicalProtobufErrorCategory::MissingRequired.as_str(),
        "missing_required"
    );
    assert_eq!(
        CanonicalProtobufErrorCategory::InvalidWireEdge.as_str(),
        "invalid_wire_edge"
    );
    assert_eq!(
        CanonicalProtobufErrorCategory::DefaultEmission.as_str(),
        "default_emission"
    );
}
