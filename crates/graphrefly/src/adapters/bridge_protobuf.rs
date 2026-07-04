//! D497 canonical protobuf profile for D134/D496 wire bridge envelopes.
//!
//! This module is schema-specific on purpose: it validates the locked
//! `protocol.proto` bridge envelope subset without introducing a protobuf
//! codegen/build step or a production value codec registry.

use std::collections::HashSet;
use std::error::Error;
use std::fmt;

use super::bridge::WireBridgeMetadata as SemanticWireBridgeMetadata;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// `CanonicalProtobufErrorCategory` variants.
pub enum CanonicalProtobufErrorCategory {
    /// `UnknownField` variant.
    UnknownField,
    /// `DuplicateSingular` variant.
    DuplicateSingular,
    /// `NoncanonicalBytes` variant.
    NoncanonicalBytes,
    /// `InvalidOneof` variant.
    InvalidOneof,
    /// `MissingRequired` variant.
    MissingRequired,
    /// `InvalidWireEdge` variant.
    InvalidWireEdge,
    /// `DefaultEmission` variant.
    DefaultEmission,
    /// `Malformed` variant.
    Malformed,
}

impl CanonicalProtobufErrorCategory {
    #[must_use]
    /// Updates or reads `as_str`.
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
/// `CanonicalProtobufError` data container.
pub struct CanonicalProtobufError {
    /// `category` field for category.
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
/// `CanonicalWireBridgeMetadata` data container.
pub struct CanonicalWireBridgeMetadata {
    /// `seq` field for seq.
    pub seq: u64,
    /// `cursor` field for cursor.
    pub cursor: u64,
    /// `idempotency_key` field for idempotency key.
    pub idempotency_key: String,
    /// `attempt` field for attempt.
    pub attempt: u32,
    /// `max_attempts` field for max attempts.
    pub max_attempts: u32,
    /// `timestamp_ms` field for timestamp ms.
    pub timestamp_ms: Option<u64>,
    /// `ack_for_seq` field for ack for seq.
    pub ack_for_seq: Option<u64>,
    /// `request_id` field for request id.
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `CanonicalWireBridgeDataBody` variants.
pub enum CanonicalWireBridgeDataBody {
    /// `Value` variant.
    Value(Vec<u8>),
    /// `WireEdge` variant.
    WireEdge(CanonicalWireEdgeFrame),
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `CanonicalWireEdgeFrame` data container.
pub struct CanonicalWireEdgeFrame {
    /// `kind` field for kind.
    pub kind: CanonicalWireEdgeKind,
    /// `edge_id` field for edge id.
    pub edge_id: String,
    /// `cause_id` field for cause id.
    pub cause_id: String,
    /// `value` field for value.
    pub value: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// `CanonicalWireEdgeKind` variants.
pub enum CanonicalWireEdgeKind {
    /// `Dirty` variant.
    Dirty,
    /// `Data` variant.
    Data,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `CanonicalWireBridgePayload` variants.
pub enum CanonicalWireBridgePayload {
    /// `Start` variant.
    Start,
    /// `Data` variant.
    Data(CanonicalWireBridgeDataBody),
    /// `Ack` variant.
    Ack,
    /// `Nack` variant.
    Nack {
        /// `error` field for `Nack`.
        error: Option<Vec<u8>>,
    },
    /// `Status` variant.
    Status {
        /// `status` field for `Status`.
        status: Vec<u8>,
    },
    /// `Error` variant.
    Error {
        /// `error` field for `Error`.
        error: Vec<u8>,
    },
    /// `Close` variant.
    Close {
        /// `reason` field for `Close`.
        reason: Option<Vec<u8>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `CanonicalWireBridgeEnvelope` data container.
pub struct CanonicalWireBridgeEnvelope {
    /// `session_id` field for session id.
    pub session_id: String,
    /// `metadata` field for metadata.
    pub metadata: CanonicalWireBridgeMetadata,
    /// `payload` field for payload.
    pub payload: CanonicalWireBridgePayload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// `WireBridgeProtobufHelperShape` data container.
pub struct WireBridgeProtobufHelperShape {
    /// `byte_specific` field for byte specific.
    pub byte_specific: bool,
    /// `semantic_wire_bridge_dto` field for semantic wire bridge dto.
    pub semantic_wire_bridge_dto: bool,
    /// `core_wire_bridge_options` field for core wire bridge options.
    pub core_wire_bridge_options: bool,
    /// `protocol_surface` field for protocol surface.
    pub protocol_surface: bool,
    /// `value_codec_registry` field for value codec registry.
    pub value_codec_registry: bool,
}

/// `constant` constant.
pub const WIRE_BRIDGE_PROTOBUF_HELPER_SHAPE: WireBridgeProtobufHelperShape =
    WireBridgeProtobufHelperShape {
        byte_specific: true,
        semantic_wire_bridge_dto: true,
        core_wire_bridge_options: false,
        protocol_surface: false,
        value_codec_registry: false,
    };

#[derive(Debug, Clone, PartialEq, Eq)]
/// `WireBridgeProtobufDataBody` variants.
pub enum WireBridgeProtobufDataBody {
    /// `Value` variant.
    Value(Vec<u8>),
    /// `WireEdge` variant.
    WireEdge(CanonicalWireEdgeFrame),
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `WireBridgeProtobufPayload` variants.
pub enum WireBridgeProtobufPayload {
    /// `Start` variant.
    Start,
    /// `Data` variant.
    Data(WireBridgeProtobufDataBody),
    /// `Ack` variant.
    Ack,
    /// `Nack` variant.
    Nack {
        /// `error` field for `Nack`.
        error: Option<Vec<u8>>,
    },
    /// `Status` variant.
    Status {
        /// `status` field for `Status`.
        status: Vec<u8>,
    },
    /// `Error` variant.
    Error {
        /// `error` field for `Error`.
        error: Vec<u8>,
    },
    /// `Close` variant.
    Close {
        /// `reason` field for `Close`.
        reason: Option<Vec<u8>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `WireBridgeProtobufEnvelope` data container.
pub struct WireBridgeProtobufEnvelope {
    /// `session_id` field for session id.
    pub session_id: String,
    /// `metadata` field for metadata.
    pub metadata: SemanticWireBridgeMetadata,
    /// `payload` field for payload.
    pub payload: WireBridgeProtobufPayload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// `WireBridgeProtobufStatusKind` variants.
pub enum WireBridgeProtobufStatusKind {
    /// `Valid` variant.
    Valid,
    /// `Invalid` variant.
    Invalid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `WireBridgeProtobufStatus` data container.
pub struct WireBridgeProtobufStatus {
    /// `kind` field for kind.
    pub kind: WireBridgeProtobufStatusKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `WireBridgeProtobufIssue` data container.
pub struct WireBridgeProtobufIssue {
    /// `category` field for category.
    pub category: CanonicalProtobufErrorCategory,
    /// `message` field for message.
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
/// `WireBridgeProtobufDecode` data container.
pub struct WireBridgeProtobufDecode {
    /// `envelope` field for envelope.
    pub envelope: Option<WireBridgeProtobufEnvelope>,
    /// `status` field for status.
    pub status: WireBridgeProtobufStatus,
    /// `issues` field for issues.
    pub issues: Vec<WireBridgeProtobufIssue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// `WireBridgeProtobufEncode` data container.
pub struct WireBridgeProtobufEncode {
    /// `bytes` field for bytes.
    pub bytes: Option<Vec<u8>>,
    /// `status` field for status.
    pub status: WireBridgeProtobufStatus,
    /// `issues` field for issues.
    pub issues: Vec<WireBridgeProtobufIssue>,
}

/// Creates or computes `encode_canonical_wire_bridge_envelope`.
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

/// Creates or computes `decode_canonical_wire_bridge_envelope`.
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

/// Creates or computes `encode_canonical_wire_edge_frame`.
pub fn encode_canonical_wire_edge_frame(
    frame: &CanonicalWireEdgeFrame,
) -> Result<Vec<u8>, CanonicalProtobufError> {
    validate_wire_edge_frame(frame)?;
    Ok(encode_wire_edge_frame(frame))
}

/// Creates or computes `decode_canonical_wire_edge_frame`.
pub fn decode_canonical_wire_edge_frame(
    bytes: &[u8],
) -> Result<CanonicalWireEdgeFrame, CanonicalProtobufError> {
    let frame = parse_wire_edge_frame(bytes)?;
    validate_wire_edge_frame(&frame)?;
    let canonical = encode_wire_edge_frame(&frame);
    if canonical != bytes {
        return Err(CanonicalProtobufError::new(
            CanonicalProtobufErrorCategory::NoncanonicalBytes,
            "WireEdgeFrame bytes are not canonical deterministic protobuf",
        ));
    }
    Ok(frame)
}

#[must_use]
/// Creates or computes `decode_wire_bridge_protobuf_bytes`.
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
/// Creates or computes `encode_wire_bridge_protobuf_bytes`.
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
            CanonicalWireBridgePayload::Data(CanonicalWireBridgeDataBody::WireEdge(frame)) => {
                WireBridgeProtobufPayload::Data(WireBridgeProtobufDataBody::WireEdge(frame))
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
            WireBridgeProtobufPayload::Data(WireBridgeProtobufDataBody::WireEdge(frame)) => {
                CanonicalWireBridgePayload::Data(CanonicalWireBridgeDataBody::WireEdge(
                    frame.clone(),
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
    let fields = read_fields(bytes, &[(1, 2), (2, 2)], "WireBridgeDataPayload")?;
    let value = bytes_field(&fields, 1);
    let wire_edge = bytes_field(&fields, 2);
    match (value, wire_edge) {
        (Some(_), Some(_)) => Err(CanonicalProtobufError::new(
            CanonicalProtobufErrorCategory::InvalidOneof,
            "WireBridgeDataPayload body has multiple cases",
        )),
        (Some(value), None) => Ok(CanonicalWireBridgeDataBody::Value(value.to_vec())),
        (None, Some(wire_edge)) => Ok(CanonicalWireBridgeDataBody::WireEdge(
            parse_wire_edge_frame(wire_edge)?,
        )),
        (None, None) => Err(CanonicalProtobufError::new(
            CanonicalProtobufErrorCategory::MissingRequired,
            "WireBridgeDataPayload missing body",
        )),
    }
}

fn parse_wire_edge_frame(bytes: &[u8]) -> Result<CanonicalWireEdgeFrame, CanonicalProtobufError> {
    let fields = read_fields(bytes, &[(1, 0), (2, 2), (3, 2), (4, 2)], "WireEdgeFrame")?;
    let kind = uint_field(&fields, 1);
    let edge = bytes_field(&fields, 2);
    let cause = bytes_field(&fields, 3);
    let value = bytes_field(&fields, 4);
    if kind.is_none() || edge.is_none() || cause.is_none() {
        return Err(CanonicalProtobufError::new(
            CanonicalProtobufErrorCategory::MissingRequired,
            "WireEdgeFrame missing required fields",
        ));
    }
    let edge_id = utf8_string(edge.unwrap(), "edge_id")?;
    let cause_id = utf8_string(cause.unwrap(), "cause_id")?;
    match kind.unwrap() {
        1 => {
            if value.is_some() {
                return Err(CanonicalProtobufError::new(
                    CanonicalProtobufErrorCategory::InvalidWireEdge,
                    "DIRTY WireEdgeFrame must not carry value",
                ));
            }
            Ok(CanonicalWireEdgeFrame {
                kind: CanonicalWireEdgeKind::Dirty,
                edge_id,
                cause_id,
                value: None,
            })
        }
        2 => {
            let value = value.ok_or_else(|| {
                CanonicalProtobufError::new(
                    CanonicalProtobufErrorCategory::InvalidWireEdge,
                    "DATA WireEdgeFrame requires value",
                )
            })?;
            Ok(CanonicalWireEdgeFrame {
                kind: CanonicalWireEdgeKind::Data,
                edge_id,
                cause_id,
                value: Some(value.to_vec()),
            })
        }
        _ => Err(CanonicalProtobufError::new(
            CanonicalProtobufErrorCategory::InvalidWireEdge,
            "WireEdgeFrame kind is invalid",
        )),
    }
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
    if let CanonicalWireBridgePayload::Data(CanonicalWireBridgeDataBody::WireEdge(frame)) =
        &envelope.payload
    {
        validate_wire_edge_frame(frame)?;
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

fn validate_wire_edge_frame(frame: &CanonicalWireEdgeFrame) -> Result<(), CanonicalProtobufError> {
    if frame.edge_id.is_empty() || frame.cause_id.is_empty() {
        return Err(CanonicalProtobufError::new(
            CanonicalProtobufErrorCategory::MissingRequired,
            "WireEdgeFrame edge_id/cause_id must be non-empty",
        ));
    }
    match frame.kind {
        CanonicalWireEdgeKind::Dirty if frame.value.is_some() => Err(CanonicalProtobufError::new(
            CanonicalProtobufErrorCategory::InvalidWireEdge,
            "DIRTY WireEdgeFrame must not carry value",
        )),
        CanonicalWireEdgeKind::Data if frame.value.is_none() => Err(CanonicalProtobufError::new(
            CanonicalProtobufErrorCategory::InvalidWireEdge,
            "DATA WireEdgeFrame requires value",
        )),
        _ => Ok(()),
    }
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
        CanonicalWireBridgeDataBody::WireEdge(frame) => {
            out.message_field(2, &encode_canonical_wire_edge_frame(frame)?);
        }
    }
    Ok(out.finish())
}

fn encode_wire_edge_frame(frame: &CanonicalWireEdgeFrame) -> Vec<u8> {
    let mut out = Writer::default();
    out.varint_field(
        1,
        match frame.kind {
            CanonicalWireEdgeKind::Dirty => 1,
            CanonicalWireEdgeKind::Data => 2,
        },
    );
    out.string_field(2, &frame.edge_id);
    out.string_field(3, &frame.cause_id);
    if let Some(value) = &frame.value {
        out.bytes_field(4, value);
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
        if !seen.insert(field_no) {
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
