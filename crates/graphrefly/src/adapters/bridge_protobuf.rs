//! D497 canonical protobuf profile for D134/D496 wire bridge envelopes and the wire
//! edge group's `WireEdgeCause` frame (R-bridge@2, graphrefly:D803 and D804).
//!
//! This module is schema-specific on purpose: it validates the locked
//! `protocol.proto` bridge envelope subset without introducing a protobuf
//! codegen/build step or a production value codec registry.

use std::collections::HashSet;
use std::error::Error;
use std::fmt;

use super::bridge::WireBridgeMetadata as SemanticWireBridgeMetadata;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CanonicalProtobufErrorCategory {
    UnknownField,
    DuplicateSingular,
    NoncanonicalBytes,
    InvalidOneof,
    MissingRequired,
    InvalidWireEdge,
    DefaultEmission,
    Malformed,
}

impl CanonicalProtobufErrorCategory {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UnknownField => "unknown_field",
            Self::DuplicateSingular => "duplicate_singular",
            Self::NoncanonicalBytes => "noncanonical_bytes",
            Self::InvalidOneof => "invalid_oneof",
            Self::MissingRequired => "missing_required",
            Self::InvalidWireEdge => "invalid_wire_edge",
            Self::DefaultEmission => "default_emission",
            Self::Malformed => "malformed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalProtobufError {
    pub category: CanonicalProtobufErrorCategory,
    message: String,
}

impl CanonicalProtobufError {
    fn new(category: CanonicalProtobufErrorCategory, message: impl Into<String>) -> Self {
        Self {
            category,
            message: message.into(),
        }
    }
}

impl fmt::Display for CanonicalProtobufError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.category.as_str(), self.message)
    }
}

impl Error for CanonicalProtobufError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalWireBridgeMetadata {
    pub seq: u64,
    pub cursor: u64,
    pub idempotency_key: String,
    pub attempt: u32,
    pub max_attempts: u32,
    pub timestamp_ms: Option<u64>,
    pub ack_for_seq: Option<u64>,
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CanonicalWireBridgeDataBody {
    Value(Vec<u8>),
    /// A wire edge group's cause (field 3; field 2 is reserved).
    EdgeCause(WireEdgeCause),
}

/// One frame of a wire edge group: one sender wave (R-bridge@2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireEdgeCause {
    /// The group's name.
    pub group: String,
    /// The session: one lifetime of the group's outbound node.
    pub session: String,
    /// The cause number, from 1 within the session.
    pub cause: u64,
    /// The edges that brought input, each with what it brought.
    pub edges: Vec<CanonicalWireEdgeArrays>,
}

/// One edge of a cause: its id and the arrays it brought, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalWireEdgeArrays {
    /// The edge id, declared on both sides.
    pub edge_id: String,
    /// The arrays: an INVALIDATE, then the arrays that carried DATA, then a terminal.
    pub arrays: Vec<Vec<CanonicalWireMessage>>,
}

/// A message that crosses a wire edge: PENDING, RESOLVED and START are each kernel's
/// own and never cross (R-bridge@2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CanonicalWireMessage {
    /// DATA with the application's value bytes.
    Data(Vec<u8>),
    /// INVALIDATE.
    Invalidate,
    /// COMPLETE.
    Complete,
    /// ERROR with the application's payload bytes.
    Error(Vec<u8>),
}

impl CanonicalWireMessage {
    fn is_terminal(&self) -> bool {
        matches!(self, Self::Complete | Self::Error(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CanonicalWireBridgePayload {
    Start,
    Data(CanonicalWireBridgeDataBody),
    Ack,
    Nack { error: Option<Vec<u8>> },
    Status { status: Vec<u8> },
    Error { error: Vec<u8> },
    Close { reason: Option<Vec<u8>> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalWireBridgeEnvelope {
    pub session_id: String,
    pub metadata: CanonicalWireBridgeMetadata,
    pub payload: CanonicalWireBridgePayload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireBridgeProtobufHelperShape {
    pub byte_specific: bool,
    pub semantic_wire_bridge_dto: bool,
    pub core_wire_bridge_options: bool,
    pub protocol_surface: bool,
    pub value_codec_registry: bool,
}

pub const WIRE_BRIDGE_PROTOBUF_HELPER_SHAPE: WireBridgeProtobufHelperShape =
    WireBridgeProtobufHelperShape {
        byte_specific: true,
        semantic_wire_bridge_dto: true,
        core_wire_bridge_options: false,
        protocol_surface: false,
        value_codec_registry: false,
    };

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireBridgeProtobufDataBody {
    Value(Vec<u8>),
    /// A wire edge group's cause.
    EdgeCause(WireEdgeCause),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireBridgeProtobufPayload {
    Start,
    Data(WireBridgeProtobufDataBody),
    Ack,
    Nack { error: Option<Vec<u8>> },
    Status { status: Vec<u8> },
    Error { error: Vec<u8> },
    Close { reason: Option<Vec<u8>> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireBridgeProtobufEnvelope {
    pub session_id: String,
    pub metadata: SemanticWireBridgeMetadata,
    pub payload: WireBridgeProtobufPayload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireBridgeProtobufStatusKind {
    Valid,
    Invalid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireBridgeProtobufStatus {
    pub kind: WireBridgeProtobufStatusKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireBridgeProtobufIssue {
    pub category: CanonicalProtobufErrorCategory,
    pub message: String,
}

impl From<CanonicalProtobufError> for WireBridgeProtobufIssue {
    fn from(error: CanonicalProtobufError) -> Self {
        Self {
            category: error.category,
            message: error.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireBridgeProtobufDecode {
    pub envelope: Option<WireBridgeProtobufEnvelope>,
    pub status: WireBridgeProtobufStatus,
    pub issues: Vec<WireBridgeProtobufIssue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireBridgeProtobufEncode {
    pub bytes: Option<Vec<u8>>,
    pub status: WireBridgeProtobufStatus,
    pub issues: Vec<WireBridgeProtobufIssue>,
}

pub fn encode_canonical_wire_bridge_envelope(
    envelope: &CanonicalWireBridgeEnvelope,
) -> Result<Vec<u8>, CanonicalProtobufError> {
    validate_wire_bridge_envelope(envelope)?;
    let mut out = Writer::default();
    out.string_field(1, &envelope.session_id);
    out.message_field(2, &encode_metadata(&envelope.metadata));
    match &envelope.payload {
        CanonicalWireBridgePayload::Start => out.message_field(3, &[]),
        CanonicalWireBridgePayload::Data(body) => {
            out.message_field(4, &encode_data_payload(body)?);
        }
        CanonicalWireBridgePayload::Ack => out.message_field(5, &[]),
        CanonicalWireBridgePayload::Nack { error } => {
            out.message_field(6, &encode_optional_bytes_message(1, error.as_deref())?);
        }
        CanonicalWireBridgePayload::Status { status } => {
            out.message_field(7, &encode_required_bytes_message(1, status));
        }
        CanonicalWireBridgePayload::Error { error } => {
            out.message_field(8, &encode_required_bytes_message(1, error));
        }
        CanonicalWireBridgePayload::Close { reason } => {
            out.message_field(9, &encode_optional_bytes_message(1, reason.as_deref())?);
        }
    }
    Ok(out.finish())
}

pub fn decode_canonical_wire_bridge_envelope(
    bytes: &[u8],
) -> Result<CanonicalWireBridgeEnvelope, CanonicalProtobufError> {
    let envelope = parse_wire_bridge_envelope(bytes)?;
    validate_wire_bridge_envelope(&envelope)?;
    let canonical = encode_canonical_wire_bridge_envelope(&envelope)?;
    if canonical != bytes {
        return Err(CanonicalProtobufError::new(
            CanonicalProtobufErrorCategory::NoncanonicalBytes,
            "WireBridgeEnvelope bytes are not canonical deterministic protobuf",
        ));
    }
    Ok(envelope)
}

/// Encodes a wire edge group's cause as canonical protobuf bytes.
pub fn encode_canonical_wire_edge_cause(
    cause: &WireEdgeCause,
) -> Result<Vec<u8>, CanonicalProtobufError> {
    validate_wire_edge_cause(cause)?;
    Ok(encode_wire_edge_cause(cause))
}

/// Decodes canonical `WireEdgeCause` bytes; a frame a gate would refuse whole is an
/// `invalid_wire_edge` error.
pub fn decode_canonical_wire_edge_cause(
    bytes: &[u8],
) -> Result<WireEdgeCause, CanonicalProtobufError> {
    let cause = parse_wire_edge_cause(bytes)?;
    validate_wire_edge_cause(&cause)?;
    if encode_wire_edge_cause(&cause) != bytes {
        return Err(CanonicalProtobufError::new(
            CanonicalProtobufErrorCategory::NoncanonicalBytes,
            "WireEdgeCause bytes are not canonical deterministic protobuf",
        ));
    }
    Ok(cause)
}

#[must_use]
pub fn decode_wire_bridge_protobuf_bytes(bytes: &[u8]) -> WireBridgeProtobufDecode {
    match decode_canonical_wire_bridge_envelope(bytes) {
        Ok(envelope) => WireBridgeProtobufDecode {
            envelope: Some(canonical_to_protobuf_envelope(envelope)),
            status: WireBridgeProtobufStatus {
                kind: WireBridgeProtobufStatusKind::Valid,
            },
            issues: Vec::new(),
        },
        Err(error) => WireBridgeProtobufDecode {
            envelope: None,
            status: WireBridgeProtobufStatus {
                kind: WireBridgeProtobufStatusKind::Invalid,
            },
            issues: vec![error.into()],
        },
    }
}

#[must_use]
pub fn encode_wire_bridge_protobuf_bytes(
    envelope: &WireBridgeProtobufEnvelope,
) -> WireBridgeProtobufEncode {
    match protobuf_to_canonical_envelope(envelope)
        .and_then(|canonical| encode_canonical_wire_bridge_envelope(&canonical))
    {
        Ok(bytes) => WireBridgeProtobufEncode {
            bytes: Some(bytes),
            status: WireBridgeProtobufStatus {
                kind: WireBridgeProtobufStatusKind::Valid,
            },
            issues: Vec::new(),
        },
        Err(error) => WireBridgeProtobufEncode {
            bytes: None,
            status: WireBridgeProtobufStatus {
                kind: WireBridgeProtobufStatusKind::Invalid,
            },
            issues: vec![error.into()],
        },
    }
}

fn canonical_to_protobuf_envelope(
    envelope: CanonicalWireBridgeEnvelope,
) -> WireBridgeProtobufEnvelope {
    WireBridgeProtobufEnvelope {
        session_id: envelope.session_id,
        metadata: SemanticWireBridgeMetadata {
            seq: envelope.metadata.seq,
            cursor: envelope.metadata.cursor,
            idempotency_key: envelope.metadata.idempotency_key,
            attempt: envelope.metadata.attempt,
            max_attempts: envelope.metadata.max_attempts,
            timestamp_ms: envelope.metadata.timestamp_ms,
            ack_for_seq: envelope.metadata.ack_for_seq,
            request_id: envelope.metadata.request_id,
        },
        payload: match envelope.payload {
            CanonicalWireBridgePayload::Start => WireBridgeProtobufPayload::Start,
            CanonicalWireBridgePayload::Data(CanonicalWireBridgeDataBody::Value(value)) => {
                WireBridgeProtobufPayload::Data(WireBridgeProtobufDataBody::Value(value))
            }
            CanonicalWireBridgePayload::Data(CanonicalWireBridgeDataBody::EdgeCause(cause)) => {
                WireBridgeProtobufPayload::Data(WireBridgeProtobufDataBody::EdgeCause(cause))
            }
            CanonicalWireBridgePayload::Ack => WireBridgeProtobufPayload::Ack,
            CanonicalWireBridgePayload::Nack { error } => WireBridgeProtobufPayload::Nack { error },
            CanonicalWireBridgePayload::Status { status } => {
                WireBridgeProtobufPayload::Status { status }
            }
            CanonicalWireBridgePayload::Error { error } => {
                WireBridgeProtobufPayload::Error { error }
            }
            CanonicalWireBridgePayload::Close { reason } => {
                WireBridgeProtobufPayload::Close { reason }
            }
        },
    }
}

fn protobuf_to_canonical_envelope(
    envelope: &WireBridgeProtobufEnvelope,
) -> Result<CanonicalWireBridgeEnvelope, CanonicalProtobufError> {
    let canonical = CanonicalWireBridgeEnvelope {
        session_id: envelope.session_id.clone(),
        metadata: CanonicalWireBridgeMetadata {
            seq: envelope.metadata.seq,
            cursor: envelope.metadata.cursor,
            idempotency_key: envelope.metadata.idempotency_key.clone(),
            attempt: envelope.metadata.attempt,
            max_attempts: envelope.metadata.max_attempts,
            timestamp_ms: envelope.metadata.timestamp_ms,
            ack_for_seq: envelope.metadata.ack_for_seq,
            request_id: envelope.metadata.request_id.clone(),
        },
        payload: match &envelope.payload {
            WireBridgeProtobufPayload::Start => CanonicalWireBridgePayload::Start,
            WireBridgeProtobufPayload::Data(WireBridgeProtobufDataBody::Value(value)) => {
                CanonicalWireBridgePayload::Data(CanonicalWireBridgeDataBody::Value(value.clone()))
            }
            WireBridgeProtobufPayload::Data(WireBridgeProtobufDataBody::EdgeCause(cause)) => {
                CanonicalWireBridgePayload::Data(CanonicalWireBridgeDataBody::EdgeCause(
                    cause.clone(),
                ))
            }
            WireBridgeProtobufPayload::Ack => CanonicalWireBridgePayload::Ack,
            WireBridgeProtobufPayload::Nack { error } => CanonicalWireBridgePayload::Nack {
                error: error.clone(),
            },
            WireBridgeProtobufPayload::Status { status } => CanonicalWireBridgePayload::Status {
                status: status.clone(),
            },
            WireBridgeProtobufPayload::Error { error } => CanonicalWireBridgePayload::Error {
                error: error.clone(),
            },
            WireBridgeProtobufPayload::Close { reason } => CanonicalWireBridgePayload::Close {
                reason: reason.clone(),
            },
        },
    };
    validate_wire_bridge_envelope(&canonical)?;
    Ok(canonical)
}

fn parse_wire_bridge_envelope(
    bytes: &[u8],
) -> Result<CanonicalWireBridgeEnvelope, CanonicalProtobufError> {
    let fields = read_fields(
        bytes,
        &[
            (1, 2),
            (2, 2),
            (3, 2),
            (4, 2),
            (5, 2),
            (6, 2),
            (7, 2),
            (8, 2),
            (9, 2),
        ],
        "WireBridgeEnvelope",
    )?;
    let session = bytes_field(&fields, 1);
    let metadata = bytes_field(&fields, 2);
    let payload_fields: Vec<&Field> = fields
        .iter()
        .filter(|field| (3..=9).contains(&field.no))
        .collect();
    if session.is_none() || metadata.is_none() || payload_fields.is_empty() {
        return Err(CanonicalProtobufError::new(
            CanonicalProtobufErrorCategory::MissingRequired,
            "WireBridgeEnvelope missing required fields",
        ));
    }
    if payload_fields.len() != 1 {
        return Err(CanonicalProtobufError::new(
            CanonicalProtobufErrorCategory::InvalidOneof,
            "WireBridgeEnvelope payload oneof has multiple cases",
        ));
    }
    let payload_field = payload_fields[0];
    let payload_bytes = payload_field.as_bytes()?;
    Ok(CanonicalWireBridgeEnvelope {
        session_id: utf8_string(session.unwrap(), "session_id")?,
        metadata: parse_metadata(metadata.unwrap())?,
        payload: parse_envelope_payload(payload_field.no, payload_bytes)?,
    })
}

fn parse_envelope_payload(
    field_no: u32,
    bytes: &[u8],
) -> Result<CanonicalWireBridgePayload, CanonicalProtobufError> {
    match field_no {
        3 => {
            require_empty_message(bytes, "start")?;
            Ok(CanonicalWireBridgePayload::Start)
        }
        4 => Ok(CanonicalWireBridgePayload::Data(parse_data_payload(bytes)?)),
        5 => {
            require_empty_message(bytes, "ack")?;
            Ok(CanonicalWireBridgePayload::Ack)
        }
        6 => Ok(CanonicalWireBridgePayload::Nack {
            error: parse_optional_bytes_payload(bytes, "nack")?,
        }),
        7 => Ok(CanonicalWireBridgePayload::Status {
            status: parse_required_bytes_payload(bytes, "status")?,
        }),
        8 => Ok(CanonicalWireBridgePayload::Error {
            error: parse_required_bytes_payload(bytes, "error")?,
        }),
        9 => Ok(CanonicalWireBridgePayload::Close {
            reason: parse_optional_bytes_payload(bytes, "close")?,
        }),
        _ => Err(CanonicalProtobufError::new(
            CanonicalProtobufErrorCategory::UnknownField,
            "unknown envelope payload field",
        )),
    }
}

fn parse_metadata(bytes: &[u8]) -> Result<CanonicalWireBridgeMetadata, CanonicalProtobufError> {
    let fields = read_fields(
        bytes,
        &[
            (1, 0),
            (2, 0),
            (3, 2),
            (4, 0),
            (5, 0),
            (6, 0),
            (7, 0),
            (8, 2),
        ],
        "WireBridgeMetadata",
    )?;
    let seq = uint_field(&fields, 1);
    let cursor = uint_field(&fields, 2);
    let key = bytes_field(&fields, 3);
    let attempt = uint_field(&fields, 4);
    let max_attempts = uint_field(&fields, 5);
    if seq.is_none()
        || cursor.is_none()
        || key.is_none()
        || attempt.is_none()
        || max_attempts.is_none()
    {
        return Err(CanonicalProtobufError::new(
            CanonicalProtobufErrorCategory::MissingRequired,
            "WireBridgeMetadata missing required fields",
        ));
    }
    let timestamp_ms = uint_field(&fields, 6);
    let ack_for_seq = uint_field(&fields, 7);
    let request_id = bytes_field(&fields, 8);
    if timestamp_ms == Some(0) || ack_for_seq == Some(0) || request_id == Some(&[][..]) {
        return Err(CanonicalProtobufError::new(
            CanonicalProtobufErrorCategory::DefaultEmission,
            "optional metadata default value was emitted",
        ));
    }
    Ok(CanonicalWireBridgeMetadata {
        seq: seq.unwrap(),
        cursor: cursor.unwrap(),
        idempotency_key: utf8_string(key.unwrap(), "idempotency_key")?,
        attempt: uint32(attempt.unwrap(), "attempt")?,
        max_attempts: uint32(max_attempts.unwrap(), "max_attempts")?,
        timestamp_ms,
        ack_for_seq,
        request_id: request_id
            .map(|value| utf8_string(value, "request_id"))
            .transpose()?,
    })
}

fn parse_data_payload(bytes: &[u8]) -> Result<CanonicalWireBridgeDataBody, CanonicalProtobufError> {
    // Field 2 carried the per-edge WireEdgeFrame until D803 and is reserved.
    let fields = read_fields(bytes, &[(1, 2), (3, 2)], "WireBridgeDataPayload")?;
    let value = bytes_field(&fields, 1);
    let edge_cause = bytes_field(&fields, 3);
    match (value, edge_cause) {
        (Some(_), Some(_)) => Err(CanonicalProtobufError::new(
            CanonicalProtobufErrorCategory::InvalidOneof,
            "WireBridgeDataPayload body has multiple cases",
        )),
        (Some(value), None) => Ok(CanonicalWireBridgeDataBody::Value(value.to_vec())),
        (None, Some(edge_cause)) => Ok(CanonicalWireBridgeDataBody::EdgeCause(
            parse_wire_edge_cause(edge_cause)?,
        )),
        (None, None) => Err(CanonicalProtobufError::new(
            CanonicalProtobufErrorCategory::MissingRequired,
            "WireBridgeDataPayload missing body",
        )),
    }
}

fn invalid_edge(message: impl Into<String>) -> CanonicalProtobufError {
    CanonicalProtobufError::new(CanonicalProtobufErrorCategory::InvalidWireEdge, message)
}

fn parse_wire_edge_cause(bytes: &[u8]) -> Result<WireEdgeCause, CanonicalProtobufError> {
    let fields = read_fields_with_repeated(
        bytes,
        &[(1, 2), (2, 2), (3, 0), (4, 2)],
        &[4],
        "WireEdgeCause",
    )?;
    let group = bytes_field(&fields, 1).ok_or_else(|| missing("WireEdgeCause group"))?;
    let session = bytes_field(&fields, 2).ok_or_else(|| missing("WireEdgeCause session"))?;
    let cause = uint_field(&fields, 3).ok_or_else(|| missing("WireEdgeCause cause"))?;
    let edges = fields
        .iter()
        .filter(|f| f.no == 4)
        .map(|f| parse_wire_edge_arrays(f.as_bytes()?))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(WireEdgeCause {
        group: utf8_string(group, "group")?,
        session: utf8_string(session, "session")?,
        cause,
        edges,
    })
}

fn parse_wire_edge_arrays(bytes: &[u8]) -> Result<CanonicalWireEdgeArrays, CanonicalProtobufError> {
    let fields = read_fields_with_repeated(bytes, &[(1, 2), (2, 2)], &[2], "WireEdgeArrays")?;
    let edge_id = bytes_field(&fields, 1).ok_or_else(|| missing("WireEdgeArrays edge_id"))?;
    let arrays = fields
        .iter()
        .filter(|f| f.no == 2)
        .map(|f| parse_wave(f.as_bytes()?))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(CanonicalWireEdgeArrays {
        edge_id: utf8_string(edge_id, "edge_id")?,
        arrays,
    })
}

fn parse_wave(bytes: &[u8]) -> Result<Vec<CanonicalWireMessage>, CanonicalProtobufError> {
    let fields = read_fields_with_repeated(bytes, &[(1, 2)], &[1], "Wave")?;
    fields
        .iter()
        .map(|f| parse_wire_message(f.as_bytes()?))
        .collect()
}

fn parse_wire_message(bytes: &[u8]) -> Result<CanonicalWireMessage, CanonicalProtobufError> {
    let fields = read_fields(bytes, &[(1, 0), (2, 2)], "Message")?;
    let data = bytes_field(&fields, 2).map(<[u8]>::to_vec);
    match (uint_field(&fields, 1).unwrap_or(0), data) {
        (3, Some(value)) => Ok(CanonicalWireMessage::Data(value)),
        (3, None) => Err(invalid_edge("DATA requires a value")),
        (5, None) => Ok(CanonicalWireMessage::Invalidate),
        (6, None) => Ok(CanonicalWireMessage::Complete),
        (5 | 6, Some(_)) => Err(invalid_edge("INVALIDATE and COMPLETE carry no data")),
        (7, Some(payload)) => Ok(CanonicalWireMessage::Error(payload)),
        (7, None) => Err(invalid_edge("ERROR requires a payload")),
        (1 | 2 | 4, _) => Err(invalid_edge(
            "START, PENDING and RESOLVED are each kernel's own",
        )),
        (0, _) => Err(invalid_edge("message type is unspecified")),
        _ => Err(invalid_edge("message type does not cross a wire edge")),
    }
}

fn missing(what: &str) -> CanonicalProtobufError {
    CanonicalProtobufError::new(
        CanonicalProtobufErrorCategory::MissingRequired,
        format!("{what} is missing"),
    )
}

/// A frame a receiver's gate refuses whole is `invalid_wire_edge` (R-bridge@2).
fn validate_wire_edge_cause(cause: &WireEdgeCause) -> Result<(), CanonicalProtobufError> {
    if cause.group.is_empty() || cause.session.is_empty() {
        return Err(missing("WireEdgeCause group or session"));
    }
    if cause.cause == 0 {
        return Err(missing("WireEdgeCause cause"));
    }
    if cause.edges.is_empty() {
        return Err(invalid_edge("a cause carries at least one edge"));
    }
    let mut seen = HashSet::new();
    for edge in &cause.edges {
        if edge.edge_id.is_empty() {
            return Err(missing("WireEdgeArrays edge_id"));
        }
        if !seen.insert(edge.edge_id.as_str()) {
            return Err(invalid_edge(format!("edge {} twice", edge.edge_id)));
        }
        if edge.arrays.is_empty() {
            return Err(invalid_edge(format!(
                "edge {} carries no array",
                edge.edge_id
            )));
        }
        let mut ended = false;
        for array in &edge.arrays {
            if array.is_empty() {
                return Err(invalid_edge(format!(
                    "edge {}: an empty array",
                    edge.edge_id
                )));
            }
            if ended {
                return Err(invalid_edge(format!(
                    "edge {}: an array after its terminal",
                    edge.edge_id
                )));
            }
            if let Some(i) = array.iter().position(CanonicalWireMessage::is_terminal) {
                if i != array.len() - 1 {
                    return Err(invalid_edge(format!(
                        "edge {}: a terminal not last",
                        edge.edge_id
                    )));
                }
                ended = true;
            }
        }
    }
    Ok(())
}

fn validate_wire_bridge_envelope(
    envelope: &CanonicalWireBridgeEnvelope,
) -> Result<(), CanonicalProtobufError> {
    if envelope.session_id.is_empty()
        || envelope.metadata.seq == 0
        || envelope.metadata.attempt == 0
        || envelope.metadata.max_attempts < envelope.metadata.attempt
        || envelope.metadata.idempotency_key.is_empty()
    {
        return Err(CanonicalProtobufError::new(
            CanonicalProtobufErrorCategory::MissingRequired,
            "WireBridgeEnvelope required semantics are invalid",
        ));
    }
    if matches!(
        envelope.payload,
        CanonicalWireBridgePayload::Ack | CanonicalWireBridgePayload::Nack { .. }
    ) && envelope.metadata.ack_for_seq.is_none()
    {
        return Err(CanonicalProtobufError::new(
            CanonicalProtobufErrorCategory::MissingRequired,
            "ACK/NACK requires metadata.ack_for_seq",
        ));
    }
    if envelope.metadata.timestamp_ms == Some(0)
        || envelope.metadata.ack_for_seq == Some(0)
        || envelope.metadata.request_id.as_deref() == Some("")
    {
        return Err(CanonicalProtobufError::new(
            CanonicalProtobufErrorCategory::DefaultEmission,
            "optional metadata default value was emitted",
        ));
    }
    if let CanonicalWireBridgePayload::Data(CanonicalWireBridgeDataBody::EdgeCause(cause)) =
        &envelope.payload
    {
        validate_wire_edge_cause(cause)?;
    }
    match &envelope.payload {
        CanonicalWireBridgePayload::Nack { error } if error.as_deref() == Some(&[]) => {
            return Err(CanonicalProtobufError::new(
                CanonicalProtobufErrorCategory::DefaultEmission,
                "nack optional bytes default value was emitted",
            ));
        }
        CanonicalWireBridgePayload::Status { status } if status.is_empty() => {
            return Err(CanonicalProtobufError::new(
                CanonicalProtobufErrorCategory::MissingRequired,
                "status payload bytes must be non-empty",
            ));
        }
        CanonicalWireBridgePayload::Error { error } if error.is_empty() => {
            return Err(CanonicalProtobufError::new(
                CanonicalProtobufErrorCategory::MissingRequired,
                "error payload bytes must be non-empty",
            ));
        }
        CanonicalWireBridgePayload::Close { reason } if reason.as_deref() == Some(&[]) => {
            return Err(CanonicalProtobufError::new(
                CanonicalProtobufErrorCategory::DefaultEmission,
                "close optional bytes default value was emitted",
            ));
        }
        _ => {}
    }
    Ok(())
}

fn encode_metadata(metadata: &CanonicalWireBridgeMetadata) -> Vec<u8> {
    let mut out = Writer::default();
    out.varint_field(1, metadata.seq);
    out.varint_field(2, metadata.cursor);
    out.string_field(3, &metadata.idempotency_key);
    out.varint_field(4, u64::from(metadata.attempt));
    out.varint_field(5, u64::from(metadata.max_attempts));
    if let Some(timestamp_ms) = metadata.timestamp_ms {
        out.varint_field(6, timestamp_ms);
    }
    if let Some(ack_for_seq) = metadata.ack_for_seq {
        out.varint_field(7, ack_for_seq);
    }
    if let Some(request_id) = &metadata.request_id {
        out.string_field(8, request_id);
    }
    out.finish()
}

fn encode_data_payload(
    body: &CanonicalWireBridgeDataBody,
) -> Result<Vec<u8>, CanonicalProtobufError> {
    let mut out = Writer::default();
    match body {
        CanonicalWireBridgeDataBody::Value(value) => out.bytes_field(1, value),
        CanonicalWireBridgeDataBody::EdgeCause(cause) => {
            out.message_field(3, &encode_canonical_wire_edge_cause(cause)?);
        }
    }
    Ok(out.finish())
}

fn encode_wire_edge_cause(cause: &WireEdgeCause) -> Vec<u8> {
    let mut out = Writer::default();
    out.string_field(1, &cause.group);
    out.string_field(2, &cause.session);
    out.varint_field(3, cause.cause);
    for edge in &cause.edges {
        let mut arrays = Writer::default();
        arrays.string_field(1, &edge.edge_id);
        for array in &edge.arrays {
            let mut wave = Writer::default();
            for message in array {
                let mut m = Writer::default();
                let (kind, data) = match message {
                    CanonicalWireMessage::Data(value) => (3, Some(value)),
                    CanonicalWireMessage::Invalidate => (5, None),
                    CanonicalWireMessage::Complete => (6, None),
                    CanonicalWireMessage::Error(payload) => (7, Some(payload)),
                };
                m.varint_field(1, kind);
                if let Some(data) = data {
                    m.bytes_field(2, data);
                }
                wave.message_field(1, &m.finish());
            }
            arrays.message_field(2, &wave.finish());
        }
        out.message_field(4, &arrays.finish());
    }
    out.finish()
}

fn encode_required_bytes_message(field_no: u32, value: &[u8]) -> Vec<u8> {
    let mut out = Writer::default();
    out.bytes_field(field_no, value);
    out.finish()
}

fn encode_optional_bytes_message(
    field_no: u32,
    value: Option<&[u8]>,
) -> Result<Vec<u8>, CanonicalProtobufError> {
    let mut out = Writer::default();
    if let Some(value) = value {
        if value.is_empty() {
            return Err(CanonicalProtobufError::new(
                CanonicalProtobufErrorCategory::DefaultEmission,
                "optional bytes default value must be omitted",
            ));
        }
        out.bytes_field(field_no, value);
    }
    Ok(out.finish())
}

fn parse_required_bytes_payload(
    bytes: &[u8],
    name: &str,
) -> Result<Vec<u8>, CanonicalProtobufError> {
    let fields = read_fields(bytes, &[(1, 2)], &format!("WireBridge{name}Payload"))?;
    let value = bytes_field(&fields, 1).ok_or_else(|| {
        CanonicalProtobufError::new(
            CanonicalProtobufErrorCategory::MissingRequired,
            format!("{name} payload missing required bytes"),
        )
    })?;
    if value.is_empty() {
        return Err(CanonicalProtobufError::new(
            CanonicalProtobufErrorCategory::MissingRequired,
            format!("{name} payload bytes must be non-empty"),
        ));
    }
    Ok(value.to_vec())
}

fn parse_optional_bytes_payload(
    bytes: &[u8],
    name: &str,
) -> Result<Option<Vec<u8>>, CanonicalProtobufError> {
    let fields = read_fields(bytes, &[(1, 2)], &format!("WireBridge{name}Payload"))?;
    let value = bytes_field(&fields, 1);
    if value == Some(&[]) {
        return Err(CanonicalProtobufError::new(
            CanonicalProtobufErrorCategory::DefaultEmission,
            format!("{name} optional bytes default value was emitted"),
        ));
    }
    Ok(value.map(<[u8]>::to_vec))
}

fn require_empty_message(bytes: &[u8], name: &str) -> Result<(), CanonicalProtobufError> {
    if bytes.is_empty() {
        Ok(())
    } else {
        Err(CanonicalProtobufError::new(
            CanonicalProtobufErrorCategory::UnknownField,
            format!("{name} payload must be empty"),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Field {
    no: u32,
    wire_type: u8,
    value: FieldValue,
}

impl Field {
    fn as_bytes(&self) -> Result<&[u8], CanonicalProtobufError> {
        match &self.value {
            FieldValue::Bytes(value) => Ok(value),
            FieldValue::Varint(_) => Err(CanonicalProtobufError::new(
                CanonicalProtobufErrorCategory::Malformed,
                "expected length-delimited field",
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FieldValue {
    Varint(u64),
    Bytes(Vec<u8>),
}

fn read_fields(
    bytes: &[u8],
    allowed: &[(u32, u8)],
    message_name: &str,
) -> Result<Vec<Field>, CanonicalProtobufError> {
    read_fields_with_repeated(bytes, allowed, &[], message_name)
}

/// [`read_fields`] where the fields in `repeated` may occur more than once.
fn read_fields_with_repeated(
    bytes: &[u8],
    allowed: &[(u32, u8)],
    repeated: &[u32],
    message_name: &str,
) -> Result<Vec<Field>, CanonicalProtobufError> {
    let mut reader = Reader::new(bytes);
    let mut fields = Vec::new();
    let mut seen = HashSet::new();
    while !reader.done() {
        let key = reader.varint()?;
        let field_no = u32::try_from(key >> 3).map_err(|_| {
            CanonicalProtobufError::new(
                CanonicalProtobufErrorCategory::Malformed,
                "field number is too large",
            )
        })?;
        let wire_type = u8::try_from(key & 7).expect("wire type fits in u8");
        let expected = allowed
            .iter()
            .find_map(|(no, wt)| (*no == field_no).then_some(*wt));
        let Some(expected_wire_type) = expected else {
            return Err(CanonicalProtobufError::new(
                CanonicalProtobufErrorCategory::UnknownField,
                format!("{message_name} contains unknown field {field_no}"),
            ));
        };
        if field_no == 0 || wire_type != expected_wire_type {
            return Err(CanonicalProtobufError::new(
                CanonicalProtobufErrorCategory::Malformed,
                format!("{message_name} field {field_no} has wrong wire type"),
            ));
        }
        if !repeated.contains(&field_no) && !seen.insert(field_no) {
            return Err(CanonicalProtobufError::new(
                CanonicalProtobufErrorCategory::DuplicateSingular,
                format!("{message_name} field {field_no} is duplicated"),
            ));
        }
        let value = match wire_type {
            0 => FieldValue::Varint(reader.varint()?),
            2 => FieldValue::Bytes(reader.read_bytes()?),
            _ => {
                return Err(CanonicalProtobufError::new(
                    CanonicalProtobufErrorCategory::Malformed,
                    "unsupported wire type",
                ));
            }
        };
        fields.push(Field {
            no: field_no,
            wire_type,
            value,
        });
    }
    Ok(fields)
}

fn uint_field(fields: &[Field], no: u32) -> Option<u64> {
    fields.iter().find_map(|field| match field {
        Field {
            no: field_no,
            value: FieldValue::Varint(value),
            ..
        } if *field_no == no => Some(*value),
        _ => None,
    })
}

fn bytes_field(fields: &[Field], no: u32) -> Option<&[u8]> {
    fields.iter().find_map(|field| match field {
        Field {
            no: field_no,
            value: FieldValue::Bytes(value),
            ..
        } if *field_no == no => Some(value.as_slice()),
        _ => None,
    })
}

fn utf8_string(bytes: &[u8], field: &str) -> Result<String, CanonicalProtobufError> {
    let value = std::str::from_utf8(bytes).map_err(|_| {
        CanonicalProtobufError::new(
            CanonicalProtobufErrorCategory::Malformed,
            format!("{field} is not valid utf-8"),
        )
    })?;
    if value.is_empty() {
        return Err(CanonicalProtobufError::new(
            CanonicalProtobufErrorCategory::MissingRequired,
            format!("{field} must be non-empty"),
        ));
    }
    Ok(value.to_owned())
}

fn uint32(value: u64, field: &str) -> Result<u32, CanonicalProtobufError> {
    u32::try_from(value).map_err(|_| {
        CanonicalProtobufError::new(
            CanonicalProtobufErrorCategory::Malformed,
            format!("{field} exceeds uint32"),
        )
    })
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn done(&self) -> bool {
        self.offset == self.bytes.len()
    }

    fn varint(&mut self) -> Result<u64, CanonicalProtobufError> {
        let mut shift = 0u32;
        let mut result = 0u64;
        for idx in 0..10 {
            let byte = *self.bytes.get(self.offset).ok_or_else(|| {
                CanonicalProtobufError::new(
                    CanonicalProtobufErrorCategory::Malformed,
                    "truncated varint",
                )
            })?;
            self.offset += 1;
            if idx == 9 && byte & 0xfe != 0 {
                return Err(CanonicalProtobufError::new(
                    CanonicalProtobufErrorCategory::Malformed,
                    "varint exceeds 64 bits",
                ));
            }
            result |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(result);
            }
            shift += 7;
        }
        Err(CanonicalProtobufError::new(
            CanonicalProtobufErrorCategory::Malformed,
            "varint exceeds 64 bits",
        ))
    }

    fn read_bytes(&mut self) -> Result<Vec<u8>, CanonicalProtobufError> {
        let len = usize::try_from(self.varint()?).map_err(|_| {
            CanonicalProtobufError::new(
                CanonicalProtobufErrorCategory::Malformed,
                "length-delimited field is too large",
            )
        })?;
        let end = self.offset.checked_add(len).ok_or_else(|| {
            CanonicalProtobufError::new(
                CanonicalProtobufErrorCategory::Malformed,
                "length-delimited field length overflows",
            )
        })?;
        if end > self.bytes.len() {
            return Err(CanonicalProtobufError::new(
                CanonicalProtobufErrorCategory::Malformed,
                "length-delimited field is truncated",
            ));
        }
        let out = self.bytes[self.offset..end].to_vec();
        self.offset = end;
        Ok(out)
    }
}

#[derive(Default)]
struct Writer {
    bytes: Vec<u8>,
}

impl Writer {
    fn varint_field(&mut self, field_no: u32, value: u64) {
        self.tag(field_no, 0);
        self.varint(value);
    }

    fn bytes_field(&mut self, field_no: u32, value: &[u8]) {
        self.tag(field_no, 2);
        self.varint(value.len() as u64);
        self.bytes.extend_from_slice(value);
    }

    fn string_field(&mut self, field_no: u32, value: &str) {
        self.bytes_field(field_no, value.as_bytes());
    }

    fn message_field(&mut self, field_no: u32, value: &[u8]) {
        self.bytes_field(field_no, value);
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }

    fn tag(&mut self, field_no: u32, wire_type: u8) {
        self.varint(u64::from((field_no << 3) | u32::from(wire_type)));
    }

    fn varint(&mut self, mut value: u64) {
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            self.bytes.push(byte);
            if value == 0 {
                break;
            }
        }
    }
}
