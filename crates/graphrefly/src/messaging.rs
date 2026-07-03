//! Graph-visible messaging application infrastructure.
//!
//! D279/D282/D284/D285/D276 define `messageBus` as a retained topic log plus
//! independent subscription cursors. DynamicHub is intentionally retired: topic
//! lifecycle, publish, cursor movement, status, and issues are graph-visible
//! command/fact streams on one static bus surface.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::rc::Rc;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ctx::Ctx;
use crate::graph::{Graph, GraphNodeOpts};
use crate::identity::canonical_tuple_key;
use crate::json::JsonValue;
use crate::node::{Core, Node, NodeOpts};
use crate::protocol::{AnyValue, LockId};

pub const PROMPTS_TOPIC: &str = "prompts";
pub const RESPONSES_TOPIC: &str = "responses";
pub const INJECTIONS_TOPIC: &str = "injections";
pub const DEFERRED_TOPIC: &str = "deferred";
pub const SPAWNS_TOPIC: &str = "spawns";
pub const CONTEXT_TOPIC: &str = "context";
pub const TODOS_TOPIC: &str = "todos";

pub const STANDARD_TOPICS: [&str; 7] = [
    PROMPTS_TOPIC,
    RESPONSES_TOPIC,
    INJECTIONS_TOPIC,
    DEFERRED_TOPIC,
    SPAWNS_TOPIC,
    CONTEXT_TOPIC,
    TODOS_TOPIC,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JsonSchemaType {
    String,
    Number,
    Integer,
    Boolean,
    Object,
    Array,
    Null,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum JsonSchemaTypeSpec {
    Single(JsonSchemaType),
    AnyOf(Vec<JsonSchemaType>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum JsonSchemaAdditionalProperties {
    Bool(bool),
    Schema(Box<JsonSchema>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum JsonSchemaItems {
    Schema(Box<JsonSchema>),
    Tuple(Vec<JsonSchema>),
}

/// Minimal passive JSON Schema vocabulary for D159 messaging payload facts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct JsonSchema {
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub schema_type: Option<JsonSchemaTypeSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<BTreeMap<String, JsonSchema>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required: Option<Vec<String>>,
    #[serde(
        rename = "additionalProperties",
        skip_serializing_if = "Option::is_none"
    )]
    pub additional_properties: Option<JsonSchemaAdditionalProperties>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<JsonSchemaItems>,
    #[serde(rename = "enum", skip_serializing_if = "Option::is_none")]
    pub enum_values: Option<Vec<JsonValue>>,
    #[serde(rename = "const", skip_serializing_if = "Option::is_none")]
    pub const_value: Option<JsonValue>,
    #[serde(rename = "$ref", skip_serializing_if = "Option::is_none")]
    pub ref_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub definitions: Option<BTreeMap<String, JsonSchema>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}

/// Passive D159 envelope for payloads that cross topic, agent, or graph boundaries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TopicMessage<T> {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<JsonSchema>,
    #[serde(rename = "expiresAt", skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
    #[serde(rename = "correlationId", skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    pub payload: T,
}

/// Passive domain event vocabulary for messageBus/eventFlow composition (D329).
#[derive(Clone, Debug, PartialEq)]
pub struct EventMessage<T> {
    pub id: String,
    pub type_: String,
    pub payload: T,
    pub key: Option<String>,
    pub subject_id: Option<String>,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
    pub occurred_at_ms: Option<u64>,
    pub actor: Option<String>,
    pub evidence_refs: Vec<String>,
    pub schema: Option<JsonSchema>,
    pub metadata: Option<BTreeMap<String, JsonValue>>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct EventMessageOptions {
    pub id: String,
    pub key: Option<String>,
    pub subject_id: Option<String>,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
    pub occurred_at_ms: Option<u64>,
    pub actor: Option<String>,
    pub evidence_refs: Vec<String>,
    pub schema: Option<JsonSchema>,
    pub metadata: Option<BTreeMap<String, JsonValue>>,
}

pub fn event_message<T>(
    type_: impl Into<String>,
    payload: T,
    opts: EventMessageOptions,
) -> EventMessage<T> {
    let type_ = type_.into();
    assert_non_empty(&type_, "eventMessage.type");
    assert_non_empty(&opts.id, "eventMessage.id");
    EventMessage {
        id: opts.id,
        type_,
        payload,
        key: opts.key,
        subject_id: opts.subject_id,
        correlation_id: opts.correlation_id,
        causation_id: opts.causation_id,
        occurred_at_ms: opts.occurred_at_ms,
        actor: opts.actor,
        evidence_refs: opts.evidence_refs,
        schema: opts.schema,
        metadata: opts.metadata,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonSchemaValidationError {
    pub path: String,
    pub message: String,
}

impl JsonSchemaValidationError {
    fn new(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for JsonSchemaValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.path, self.message)
    }
}

impl Error for JsonSchemaValidationError {}

pub type JsonSchemaValidationResult<T> = Result<T, JsonSchemaValidationError>;

/// Caller-invoked, side-effect-free validation for JSON-like payload values (D159).
pub fn validate_json_schema(
    schema: &JsonSchema,
    value: &JsonValue,
) -> JsonSchemaValidationResult<()> {
    validate_json_schema_inner(schema, schema, value, "$", &mut Vec::new())
}

pub fn is_json_schema_valid(schema: &JsonSchema, value: &JsonValue) -> bool {
    validate_json_schema(schema, value).is_ok()
}

/// Validate a passive topic message payload only when the caller explicitly asks.
pub fn validate_topic_message_payload(
    message: &TopicMessage<JsonValue>,
) -> JsonSchemaValidationResult<()> {
    if let Some(schema) = &message.schema {
        validate_json_schema(schema, &message.payload)?;
    }
    Ok(())
}

fn validate_json_schema_inner(
    schema: &JsonSchema,
    root: &JsonSchema,
    value: &JsonValue,
    path: &str,
    refs_seen: &mut Vec<(String, String)>,
) -> JsonSchemaValidationResult<()> {
    if let Some(ref_path) = &schema.ref_path {
        validate_json_schema_ref(root, ref_path, value, path, refs_seen)?;
    }
    if let Some(const_value) = &schema.const_value {
        if value != const_value {
            return Err(JsonSchemaValidationError::new(
                path,
                "value does not match const",
            ));
        }
    }
    if let Some(enum_values) = &schema.enum_values {
        if !enum_values.iter().any(|candidate| candidate == value) {
            return Err(JsonSchemaValidationError::new(
                path,
                "value is not one of the allowed enum values",
            ));
        }
    }
    if let Some(schema_type) = &schema.schema_type {
        validate_json_schema_type(schema_type, value, path)?;
    }
    if let Value::Object(object) = value {
        validate_json_schema_object(schema, root, object, path, refs_seen)?;
    }
    if let Value::Array(items) = value {
        validate_json_schema_array(schema, root, items, path, refs_seen)?;
    }
    Ok(())
}

fn validate_json_schema_ref(
    root: &JsonSchema,
    ref_path: &str,
    value: &JsonValue,
    path: &str,
    refs_seen: &mut Vec<(String, String)>,
) -> JsonSchemaValidationResult<()> {
    let name = ref_path.strip_prefix("#/definitions/").ok_or_else(|| {
        JsonSchemaValidationError::new(path, "only local #/definitions refs are supported")
    })?;
    if refs_seen
        .iter()
        .any(|(seen_ref, seen_path)| seen_ref == ref_path && seen_path == path)
    {
        return Err(JsonSchemaValidationError::new(
            path,
            format!("cyclic JSON schema ref '{ref_path}'"),
        ));
    }
    let definitions = root.definitions.as_ref().ok_or_else(|| {
        JsonSchemaValidationError::new(path, format!("unknown JSON schema ref '{ref_path}'"))
    })?;
    let referenced = definitions.get(name).ok_or_else(|| {
        JsonSchemaValidationError::new(path, format!("unknown JSON schema ref '{ref_path}'"))
    })?;
    refs_seen.push((ref_path.to_owned(), path.to_owned()));
    let result = validate_json_schema_inner(referenced, root, value, path, refs_seen);
    refs_seen.pop();
    result
}

fn validate_json_schema_type(
    schema_type: &JsonSchemaTypeSpec,
    value: &JsonValue,
    path: &str,
) -> JsonSchemaValidationResult<()> {
    let ok = match schema_type {
        JsonSchemaTypeSpec::Single(expected) => json_value_matches_type(value, *expected),
        JsonSchemaTypeSpec::AnyOf(expected) => expected
            .iter()
            .any(|expected| json_value_matches_type(value, *expected)),
    };
    if ok {
        Ok(())
    } else {
        Err(JsonSchemaValidationError::new(
            path,
            format!(
                "expected {}, got {}",
                json_schema_type_label(schema_type),
                json_value_type_label(value)
            ),
        ))
    }
}

fn json_value_matches_type(value: &JsonValue, expected: JsonSchemaType) -> bool {
    match expected {
        JsonSchemaType::String => value.is_string(),
        JsonSchemaType::Number => value.is_number(),
        JsonSchemaType::Integer => {
            value.as_i64().is_some()
                || value.as_u64().is_some()
                || value
                    .as_f64()
                    .is_some_and(|number| number.is_finite() && number.fract() == 0.0)
        }
        JsonSchemaType::Boolean => value.is_boolean(),
        JsonSchemaType::Object => value.is_object(),
        JsonSchemaType::Array => value.is_array(),
        JsonSchemaType::Null => value.is_null(),
    }
}

fn json_schema_type_label(schema_type: &JsonSchemaTypeSpec) -> String {
    match schema_type {
        JsonSchemaTypeSpec::Single(expected) => format!("{expected:?}").to_lowercase(),
        JsonSchemaTypeSpec::AnyOf(expected) => expected
            .iter()
            .map(|expected| format!("{expected:?}").to_lowercase())
            .collect::<Vec<_>>()
            .join("|"),
    }
}

fn json_value_type_label(value: &JsonValue) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn validate_json_schema_object(
    schema: &JsonSchema,
    root: &JsonSchema,
    object: &serde_json::Map<String, Value>,
    path: &str,
    refs_seen: &mut Vec<(String, String)>,
) -> JsonSchemaValidationResult<()> {
    if let Some(required) = &schema.required {
        for key in required {
            if !object.contains_key(key) {
                return Err(JsonSchemaValidationError::new(
                    path,
                    format!("missing required property '{key}'"),
                ));
            }
        }
    }
    let empty_properties = BTreeMap::new();
    let properties = schema.properties.as_ref().unwrap_or(&empty_properties);
    for (key, property_schema) in properties {
        if let Some(property_value) = object.get(key) {
            validate_json_schema_inner(
                property_schema,
                root,
                property_value,
                &format!("{path}.{key}"),
                refs_seen,
            )?;
        }
    }
    validate_additional_properties(schema, root, object, properties, path, refs_seen)?;
    Ok(())
}

fn validate_additional_properties(
    schema: &JsonSchema,
    root: &JsonSchema,
    object: &serde_json::Map<String, Value>,
    properties: &BTreeMap<String, JsonSchema>,
    path: &str,
    refs_seen: &mut Vec<(String, String)>,
) -> JsonSchemaValidationResult<()> {
    let Some(additional_properties) = &schema.additional_properties else {
        return Ok(());
    };
    for (key, value) in object {
        if properties.contains_key(key) {
            continue;
        }
        match additional_properties {
            JsonSchemaAdditionalProperties::Bool(true) => {}
            JsonSchemaAdditionalProperties::Bool(false) => {
                return Err(JsonSchemaValidationError::new(
                    path,
                    format!("unexpected additional property '{key}'"),
                ));
            }
            JsonSchemaAdditionalProperties::Schema(additional_schema) => {
                validate_json_schema_inner(
                    additional_schema,
                    root,
                    value,
                    &format!("{path}.{key}"),
                    refs_seen,
                )?;
            }
        }
    }
    Ok(())
}

fn validate_json_schema_array(
    schema: &JsonSchema,
    root: &JsonSchema,
    values: &[Value],
    path: &str,
    refs_seen: &mut Vec<(String, String)>,
) -> JsonSchemaValidationResult<()> {
    let Some(items) = &schema.items else {
        return Ok(());
    };
    match items {
        JsonSchemaItems::Schema(item_schema) => {
            for (index, value) in values.iter().enumerate() {
                validate_json_schema_inner(
                    item_schema,
                    root,
                    value,
                    &format!("{path}[{index}]"),
                    refs_seen,
                )?;
            }
        }
        JsonSchemaItems::Tuple(item_schemas) => {
            for (index, item_schema) in item_schemas.iter().enumerate() {
                if let Some(value) = values.get(index) {
                    validate_json_schema_inner(
                        item_schema,
                        root,
                        value,
                        &format!("{path}[{index}]"),
                        refs_seen,
                    )?;
                }
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub struct DataIssue {
    pub kind: String,
    pub code: String,
    pub message: String,
    pub severity: String,
    pub source: String,
    pub topic: Option<String>,
    pub details: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageEnvelope<T = AnyValue> {
    pub topic: String,
    pub seq: u64,
    pub payload: T,
    pub key: Option<String>,
    pub timestamp_ms: u64,
    pub command_id: Option<String>,
    pub idempotency_key: Option<String>,
}

impl MessageEnvelope<AnyValue> {
    pub fn payload_as<T: 'static>(&self) -> Option<Rc<T>> {
        self.payload.clone().downcast::<T>().ok()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageBusTopicPolicy {
    Strict,
    CreateAsFact,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MessageBusRetentionPolicy {
    pub max_messages: Option<usize>,
    pub max_age_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageBusDedupeAction {
    Status,
    Issue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageBusDedupePolicy {
    pub command_id: MessageBusDedupeAction,
}

impl Default for MessageBusDedupePolicy {
    fn default() -> Self {
        Self {
            command_id: MessageBusDedupeAction::Status,
        }
    }
}

#[derive(Clone)]
pub struct MessageBusOptions {
    pub name: String,
    pub topics: Vec<String>,
    pub topic_policy: MessageBusTopicPolicy,
    pub retention: MessageBusRetentionPolicy,
    pub dedupe: MessageBusDedupePolicy,
    pub now: Rc<dyn Fn() -> u64>,
}

impl Default for MessageBusOptions {
    fn default() -> Self {
        Self {
            name: "messageBus".to_owned(),
            topics: Vec::new(),
            topic_policy: MessageBusTopicPolicy::Strict,
            retention: MessageBusRetentionPolicy::default(),
            dedupe: MessageBusDedupePolicy::default(),
            now: Rc::new(|| 0),
        }
    }
}

impl MessageBusOptions {
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Self::default()
        }
    }

    pub fn with_topics(mut self, topics: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.topics = topics.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_topic_policy(mut self, policy: MessageBusTopicPolicy) -> Self {
        self.topic_policy = policy;
        self
    }

    pub fn with_retention(mut self, retention: MessageBusRetentionPolicy) -> Self {
        self.retention = retention;
        self
    }

    pub fn with_dedupe(mut self, dedupe: MessageBusDedupePolicy) -> Self {
        self.dedupe = dedupe;
        self
    }

    pub fn with_now(mut self, now: impl Fn() -> u64 + 'static) -> Self {
        self.now = Rc::new(now);
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum MessageBusCommand<T = AnyValue> {
    EnsureTopic {
        topic: String,
        command_id: Option<String>,
    },
    CloseTopic {
        topic: String,
        command_id: Option<String>,
    },
    Publish {
        topic: String,
        payload: T,
        key: Option<String>,
        command_id: Option<String>,
        idempotency_key: Option<String>,
    },
    TopicPolicy {
        topic_policy: MessageBusTopicPolicy,
        command_id: Option<String>,
    },
    Ack {
        topic: String,
        subscription_id: String,
        seq: u64,
        command_id: Option<String>,
    },
    Seek {
        topic: String,
        subscription_id: String,
        next_seq: u64,
        command_id: Option<String>,
    },
    CloseSubscription {
        topic: String,
        subscription_id: String,
        command_id: Option<String>,
    },
}

impl<T> MessageBusCommand<T> {
    fn topic(&self) -> Option<&str> {
        match self {
            Self::EnsureTopic { topic, .. }
            | Self::CloseTopic { topic, .. }
            | Self::Publish { topic, .. }
            | Self::Ack { topic, .. }
            | Self::Seek { topic, .. }
            | Self::CloseSubscription { topic, .. } => Some(topic),
            Self::TopicPolicy { .. } => None,
        }
    }

    fn command_id(&self) -> Option<&str> {
        match self {
            Self::EnsureTopic { command_id, .. }
            | Self::CloseTopic { command_id, .. }
            | Self::Publish { command_id, .. }
            | Self::TopicPolicy { command_id, .. }
            | Self::Ack { command_id, .. }
            | Self::Seek { command_id, .. }
            | Self::CloseSubscription { command_id, .. } => command_id.as_deref(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageBusStatusKind {
    TopicCreated,
    TopicClosed,
    MessagePublished,
    RetentionTrimmed,
    DuplicateCommand,
    SubscriptionOpened,
    SubscriptionAcked,
    SubscriptionSought,
    SubscriptionClosed,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageBusStatus {
    pub kind: MessageBusStatusKind,
    pub topic: Option<String>,
    pub seq: Option<u64>,
    pub head_seq: Option<u64>,
    pub subscription_id: Option<String>,
    pub next_seq: Option<u64>,
    pub command_id: Option<String>,
    pub issue_code: Option<String>,
    pub timestamp_ms: u64,
    pub details: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageBusCatalogEntry {
    pub topic: String,
    pub closed: bool,
    pub head_seq: u64,
    pub next_seq: u64,
    pub message_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageBusCatalogPage {
    pub topics: Vec<MessageBusCatalogEntry>,
    pub next_after_topic: Option<String>,
    pub has_more: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageBusDeadLetterEntry<T = AnyValue> {
    pub entry_seq: u64,
    pub topic: Option<String>,
    pub command: Option<MessageBusCommand<T>>,
    pub message: Option<MessageEnvelope<T>>,
    pub issue: DataIssue,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageBusDeadLetterPage<T = AnyValue> {
    pub entries: Vec<MessageBusDeadLetterEntry<T>>,
    pub next_after_entry_seq: Option<u64>,
    pub has_more: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageBusTopicPage<T = AnyValue> {
    pub topic: String,
    pub messages: Vec<MessageEnvelope<T>>,
    pub from_seq: u64,
    pub through_seq: Option<u64>,
    pub next_after_seq: Option<u64>,
    pub has_more: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageBusCursor {
    pub topic: String,
    pub subscription_id: String,
    pub next_seq: u64,
    pub closed: bool,
    pub retention_gap: bool,
    pub head_seq: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MessageBusAvailablePage<T = AnyValue> {
    pub topic: String,
    pub subscription_id: String,
    pub cursor: MessageBusCursor,
    pub messages: Vec<MessageEnvelope<T>>,
    pub from_seq: u64,
    pub through_seq: Option<u64>,
    pub next_after_seq: Option<u64>,
    pub has_more: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageBusCatalogParams {
    pub limit: Option<usize>,
    pub after_topic: Option<String>,
    pub include_closed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageBusDeadLetterParams {
    pub limit: Option<usize>,
    pub after_entry_seq: Option<u64>,
    pub topic: Option<String>,
    pub code: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageBusTopicParams {
    pub limit: Option<usize>,
    pub after_seq: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageBusAvailableParams {
    pub limit: Option<usize>,
    pub after_seq: Option<u64>,
}

#[derive(Clone)]
pub struct MessageBusPullProjection<TPage> {
    pub snapshot: Node<TPage>,
    pub snapshot_pull_id: LockId,
    pub status: Node<MessageBusStatus>,
    pub issues: Node<DataIssue>,
}

pub type MessageBusTopicProjection<T = AnyValue> = MessageBusPullProjection<MessageBusTopicPage<T>>;

#[derive(Clone)]
pub struct MessageBusSubscription<T = AnyValue> {
    pub available: Node<MessageBusAvailablePage<T>>,
    pub available_pull_id: LockId,
    pub cursor: Node<MessageBusCursor>,
    pub status: Node<MessageBusStatus>,
    pub issues: Node<DataIssue>,
    commands: Node<MessageBusCommand<T>>,
    topic: String,
    subscription_id: String,
}

#[derive(Clone)]
pub struct MessageBus<T = AnyValue> {
    graph: Graph,
    name: Rc<String>,
    pub commands: Node<MessageBusCommand<T>>,
    pub messages: Node<MessageEnvelope<T>>,
    pub status: Node<MessageBusStatus>,
    pub issues: Node<DataIssue>,
    state: Rc<RefCell<MessageBusState<T>>>,
    command_sources: Rc<RefCell<Vec<Core>>>,
    _runtime_retain: Rc<RetainNode>,
}

#[derive(Clone)]
pub struct ToTopicBundle<T> {
    pub commands: Node<MessageBusCommand<T>>,
}

struct RetainNode {
    release: RefCell<Option<Box<dyn FnOnce()>>>,
}

impl RetainNode {
    fn new(release: Box<dyn FnOnce()>) -> Self {
        Self {
            release: RefCell::new(Some(release)),
        }
    }
}

impl Drop for RetainNode {
    fn drop(&mut self) {
        if let Some(release) = self.release.borrow_mut().take() {
            release();
        }
    }
}

#[derive(Clone)]
struct TopicState<T> {
    closed: bool,
    head_seq: u64,
    next_seq: u64,
    messages: Vec<MessageEnvelope<T>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SubscriptionState {
    topic: String,
    subscription_id: String,
    next_seq: u64,
    closed: bool,
    retention_gap: bool,
}

struct MessageBusState<T = AnyValue> {
    now: Rc<dyn Fn() -> u64>,
    topic_policy: MessageBusTopicPolicy,
    retention: MessageBusRetentionPolicy,
    dedupe: MessageBusDedupePolicy,
    topics: BTreeMap<String, TopicState<T>>,
    subscriptions: BTreeMap<String, SubscriptionState>,
    seen_command_ids: HashSet<String>,
    seen_idempotency_keys: HashSet<String>,
    dead_letters: Vec<MessageBusDeadLetterEntry<T>>,
    dead_letter_seq: u64,
}

#[derive(Clone)]
enum RuntimeEvent<T = AnyValue> {
    Message(MessageEnvelope<T>),
    Status(MessageBusStatus),
    Issue(DataIssue),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageBusSubscriptionFrom {
    Earliest,
    Latest,
    Seq(u64),
}

#[derive(Clone)]
pub struct MessageBusSubscriptionOptions {
    pub topic: String,
    pub subscription_id: String,
    pub from: MessageBusSubscriptionFrom,
    pub name: Option<String>,
}

impl MessageBusSubscriptionOptions {
    pub fn new(topic: impl Into<String>, subscription_id: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            subscription_id: subscription_id.into(),
            from: MessageBusSubscriptionFrom::Earliest,
            name: None,
        }
    }

    pub fn from(mut self, from: MessageBusSubscriptionFrom) -> Self {
        self.from = from;
        self
    }

    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
}

impl<T: Clone + 'static> MessageBus<T> {
    pub fn new(graph: &Graph, topics: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self::with_options(graph, MessageBusOptions::default().with_topics(topics))
    }

    pub fn with_options(graph: &Graph, opts: MessageBusOptions) -> Self {
        let name = Rc::new(opts.name);
        let command_sources = Rc::new(RefCell::new(Vec::new()));
        let commands = graph.node_opts::<MessageBusCommand<T>, _>(
            Vec::new(),
            message_bus_command_body::<T>(0),
            node_opts(format!("{name}/commands"), "messageBusCommands"),
        );
        let initial_topics = unique_topics(opts.topics);
        let state = Rc::new(RefCell::new(MessageBusState {
            now: opts.now,
            topic_policy: opts.topic_policy,
            retention: opts.retention,
            dedupe: opts.dedupe,
            topics: initial_topics
                .into_iter()
                .map(|topic| (topic, make_topic_state()))
                .collect(),
            subscriptions: BTreeMap::new(),
            seen_command_ids: HashSet::new(),
            seen_idempotency_keys: HashSet::new(),
            dead_letters: Vec::new(),
            dead_letter_seq: 0,
        }));
        let runtime_state = state.clone();
        let runtime = graph.node_opts::<RuntimeEvent<T>, _>(
            vec![commands.erased()],
            move |ctx| {
                for command in ctx.batch::<MessageBusCommand<T>>(0) {
                    let events = {
                        let mut state = runtime_state.borrow_mut();
                        reduce_message_bus_command(&mut state, (*command).clone())
                    };
                    for event in events {
                        ctx.emit(event);
                    }
                }
            },
            node_opts(format!("{name}/runtime"), "messageBusRuntime"),
        );
        let runtime_retain = Rc::new(RetainNode::new(
            graph.retain(&runtime, &format!("{name}.messageBus.runtime")),
        ));
        let messages = graph.node_opts::<MessageEnvelope<T>, _>(
            vec![runtime.erased()],
            move |ctx| {
                for event in ctx.batch::<RuntimeEvent<T>>(0) {
                    if let RuntimeEvent::Message(message) = event.as_ref() {
                        ctx.emit(message.clone());
                    }
                }
            },
            node_opts(format!("{name}/messages"), "messageBusMessages"),
        );
        let status = graph.node_opts::<MessageBusStatus, _>(
            vec![runtime.erased()],
            move |ctx| {
                for event in ctx.batch::<RuntimeEvent<T>>(0) {
                    if let RuntimeEvent::Status(status) = event.as_ref() {
                        ctx.emit(status.clone());
                    }
                }
            },
            node_opts(format!("{name}/status"), "messageBusStatus"),
        );
        let issues = graph.node_opts::<DataIssue, _>(
            vec![runtime.erased()],
            move |ctx| {
                for event in ctx.batch::<RuntimeEvent<T>>(0) {
                    if let RuntimeEvent::Issue(issue) = event.as_ref() {
                        ctx.emit(issue.clone());
                    }
                }
            },
            node_opts(format!("{name}/issues"), "messageBusIssues"),
        );
        Self {
            graph: graph.clone(),
            name,
            commands,
            messages,
            status,
            issues,
            state,
            command_sources,
            _runtime_retain: runtime_retain,
        }
    }

    pub fn ensure_topic(
        &self,
        topic: impl Into<String>,
        command_id: Option<String>,
    ) -> MessageBusCommand<T> {
        self.publish_command(MessageBusCommand::EnsureTopic {
            topic: topic.into(),
            command_id,
        })
    }

    pub fn close_topic(
        &self,
        topic: impl Into<String>,
        command_id: Option<String>,
    ) -> MessageBusCommand<T> {
        self.publish_command(MessageBusCommand::CloseTopic {
            topic: topic.into(),
            command_id,
        })
    }

    pub fn publish(
        &self,
        topic: impl Into<String>,
        payload: T,
        key: Option<String>,
        command_id: Option<String>,
        idempotency_key: Option<String>,
    ) -> MessageBusCommand<T> {
        self.publish_command(MessageBusCommand::Publish {
            topic: topic.into(),
            payload,
            key,
            command_id,
            idempotency_key,
        })
    }

    pub fn set_topic_policy(
        &self,
        topic_policy: MessageBusTopicPolicy,
        command_id: Option<String>,
    ) -> MessageBusCommand<T> {
        self.publish_command(MessageBusCommand::TopicPolicy {
            topic_policy,
            command_id,
        })
    }

    pub fn topic(&self, topic: impl Into<String>) -> MessageBusTopicProjection<T> {
        self.topic_named(topic, None::<String>)
    }

    pub fn topic_named(
        &self,
        topic: impl Into<String>,
        name: Option<impl Into<String>>,
    ) -> MessageBusTopicProjection<T> {
        let topic = topic.into();
        assert_topic_key(&topic, "messageBus.topic");
        let snapshot_pull_id = LockId::new(format!("{}/{topic}/topicSnapshot", self.name));
        let state = self.state.clone();
        let topic_for_fn = topic.clone();
        let snapshot = self.graph.node_opts::<MessageBusTopicPage<T>, _>(
            vec![self.messages.erased()],
            move |ctx| {
                let params =
                    pull_params::<MessageBusTopicParams>(ctx).unwrap_or(MessageBusTopicParams {
                        limit: None,
                        after_seq: None,
                    });
                ctx.emit(topic_page(&state.borrow(), &topic_for_fn, &params));
            },
            pull_node_opts(
                name.map(Into::into)
                    .unwrap_or_else(|| format!("{}/{topic}/topic", self.name)),
                "messageBusTopicProjection",
                snapshot_pull_id.clone(),
            ),
        );
        MessageBusPullProjection {
            snapshot,
            snapshot_pull_id,
            status: self.status.clone(),
            issues: self.issues.clone(),
        }
    }

    pub fn catalog(&self) -> MessageBusPullProjection<MessageBusCatalogPage> {
        self.catalog_named(None::<String>)
    }

    pub fn catalog_named(
        &self,
        name: Option<impl Into<String>>,
    ) -> MessageBusPullProjection<MessageBusCatalogPage> {
        let snapshot_pull_id = LockId::new(format!("{}/catalogSnapshot", self.name));
        let state = self.state.clone();
        let snapshot = self.graph.node_opts::<MessageBusCatalogPage, _>(
            vec![self.status.erased()],
            move |ctx| {
                let params = pull_params::<MessageBusCatalogParams>(ctx).unwrap_or(
                    MessageBusCatalogParams {
                        limit: None,
                        after_topic: None,
                        include_closed: false,
                    },
                );
                ctx.emit(catalog_page(&state.borrow(), &params));
            },
            pull_node_opts(
                name.map(Into::into)
                    .unwrap_or_else(|| format!("{}/catalog", self.name)),
                "messageBusCatalog",
                snapshot_pull_id.clone(),
            ),
        );
        MessageBusPullProjection {
            snapshot,
            snapshot_pull_id,
            status: self.status.clone(),
            issues: self.issues.clone(),
        }
    }

    pub fn dead_letter(&self) -> MessageBusPullProjection<MessageBusDeadLetterPage<T>> {
        self.dead_letter_named(None::<String>)
    }

    pub fn dead_letter_named(
        &self,
        name: Option<impl Into<String>>,
    ) -> MessageBusPullProjection<MessageBusDeadLetterPage<T>> {
        let snapshot_pull_id = LockId::new(format!("{}/deadLetterSnapshot", self.name));
        let state = self.state.clone();
        let snapshot = self.graph.node_opts::<MessageBusDeadLetterPage<T>, _>(
            vec![self.issues.erased(), self.status.erased()],
            move |ctx| {
                let params = pull_params::<MessageBusDeadLetterParams>(ctx).unwrap_or(
                    MessageBusDeadLetterParams {
                        limit: None,
                        after_entry_seq: None,
                        topic: None,
                        code: None,
                    },
                );
                ctx.emit(dead_letter_page(&state.borrow(), &params));
            },
            pull_node_opts(
                name.map(Into::into)
                    .unwrap_or_else(|| format!("{}/deadLetter", self.name)),
                "messageBusDeadLetter",
                snapshot_pull_id.clone(),
            ),
        );
        MessageBusPullProjection {
            snapshot,
            snapshot_pull_id,
            status: self.status.clone(),
            issues: self.issues.clone(),
        }
    }

    pub fn subscription(&self, opts: MessageBusSubscriptionOptions) -> MessageBusSubscription<T> {
        assert_topic_key(&opts.topic, "messageBus.subscription");
        assert_non_empty(&opts.subscription_id, "messageBus.subscriptionId");
        let (sub, opened, issue) = ensure_subscription(&mut self.state.borrow_mut(), &opts);
        let available_pull_id = LockId::new(format!(
            "{}/{}/{}/available",
            self.name, opts.topic, opts.subscription_id
        ));
        let projection_name = opts
            .name
            .clone()
            .unwrap_or_else(|| format!("{}/{}/{}", self.name, opts.topic, opts.subscription_id));
        let available_state = self.state.clone();
        let available_sub = sub.clone();
        let available = self.graph.node_opts::<MessageBusAvailablePage<T>, _>(
            vec![self.messages.erased(), self.status.erased()],
            move |ctx| {
                let params = pull_params::<MessageBusAvailableParams>(ctx).unwrap_or(
                    MessageBusAvailableParams {
                        limit: None,
                        after_seq: None,
                    },
                );
                ctx.emit(available_page(
                    &available_state.borrow(),
                    &available_sub,
                    &params,
                ));
            },
            pull_node_opts(
                format!("{projection_name}/available"),
                "messageBusSubscriptionAvailable",
                available_pull_id.clone(),
            ),
        );
        let cursor_state = self.state.clone();
        let cursor_sub = sub.clone();
        let cursor = self.graph.node_opts::<MessageBusCursor, _>(
            vec![self.status.erased()],
            move |ctx| {
                for status in ctx.batch::<MessageBusStatus>(0) {
                    let subscription_moved = status.topic.as_deref()
                        == Some(cursor_sub.topic.as_str())
                        && status.subscription_id.as_deref()
                            == Some(cursor_sub.subscription_id.as_str())
                        && matches!(
                            status.kind,
                            MessageBusStatusKind::SubscriptionOpened
                                | MessageBusStatusKind::SubscriptionAcked
                                | MessageBusStatusKind::SubscriptionSought
                                | MessageBusStatusKind::SubscriptionClosed
                        );
                    let retention_changed = status.topic.as_deref()
                        == Some(cursor_sub.topic.as_str())
                        && status.kind == MessageBusStatusKind::RetentionTrimmed;
                    if subscription_moved || retention_changed {
                        ctx.emit(cursor_snapshot(&cursor_state.borrow(), &cursor_sub));
                    }
                }
            },
            node_opts(
                format!("{projection_name}/cursor"),
                "messageBusSubscriptionCursor",
            ),
        );
        if opened {
            let opened_status = status_fact(
                &self.state.borrow(),
                MessageBusStatusKind::SubscriptionOpened,
                StatusFields {
                    topic: Some(sub.topic.clone()),
                    subscription_id: Some(sub.subscription_id.clone()),
                    next_seq: Some(sub.next_seq),
                    details: Some(format!("from={:?}", opts.from)),
                    ..StatusFields::default()
                },
            );
            self.status.set(opened_status);
        }
        if let Some(issue) = issue {
            self.issues.set(issue);
        }
        MessageBusSubscription {
            available,
            available_pull_id,
            cursor,
            status: self.status.clone(),
            issues: self.issues.clone(),
            commands: self.commands.clone(),
            topic: sub.topic,
            subscription_id: sub.subscription_id,
        }
    }

    fn publish_command(&self, command: MessageBusCommand<T>) -> MessageBusCommand<T> {
        self.commands.set(command.clone());
        command
    }
}

impl<T: Clone + 'static> MessageBusSubscription<T> {
    pub fn ack(&self, seq: u64, command_id: Option<String>) -> MessageBusCommand<T> {
        let command = MessageBusCommand::Ack {
            topic: self.topic.clone(),
            subscription_id: self.subscription_id.clone(),
            seq,
            command_id,
        };
        self.commands.set(command.clone());
        command
    }

    pub fn seek(&self, next_seq: u64, command_id: Option<String>) -> MessageBusCommand<T> {
        let command = MessageBusCommand::Seek {
            topic: self.topic.clone(),
            subscription_id: self.subscription_id.clone(),
            next_seq,
            command_id,
        };
        self.commands.set(command.clone());
        command
    }

    pub fn close(&self, command_id: Option<String>) -> MessageBusCommand<T> {
        let command = MessageBusCommand::CloseSubscription {
            topic: self.topic.clone(),
            subscription_id: self.subscription_id.clone(),
            command_id,
        };
        self.commands.set(command.clone());
        command
    }
}

pub fn message_bus<T: Clone + 'static>(graph: &Graph, opts: MessageBusOptions) -> MessageBus<T> {
    MessageBus::with_options(graph, opts)
}

pub fn to_topic<T: Clone + 'static>(
    graph: &Graph,
    source: &Node<T>,
    bus: &MessageBus<T>,
    topic: impl Into<String>,
    name: impl Into<String>,
) -> ToTopicBundle<T> {
    let topic = topic.into();
    assert_topic_key(&topic, "toTopic");
    assert!(
        source.erased().same_graph(&bus.commands.erased()),
        "toTopic: bus and source graph must match"
    );
    assert!(
        !is_reachable_upstream(&source.erased(), &bus.commands.erased()),
        "toTopic: source already depends on bus command path"
    );
    let topic_for_fn = topic.clone();
    let commands = graph.node_opts::<MessageBusCommand<T>, _>(
        vec![source.erased()],
        move |ctx| {
            for value in ctx.batch::<T>(0) {
                ctx.emit(MessageBusCommand::Publish {
                    topic: topic_for_fn.clone(),
                    payload: (*value).clone(),
                    key: None,
                    command_id: None,
                    idempotency_key: None,
                });
            }
        },
        node_opts(name.into(), "messageBusToTopic"),
    );
    let command_source = commands.erased();
    bus.command_sources
        .borrow_mut()
        .push(command_source.clone());
    let command_sources = bus.command_sources.borrow().clone();
    let command_source_count = command_sources.len();
    let rewire = catch_unwind(AssertUnwindSafe(|| {
        bus.commands.replace_deps(
            command_sources,
            message_bus_command_body::<T>(command_source_count),
        );
    }));
    if let Err(panic) = rewire {
        bus.command_sources
            .borrow_mut()
            .retain(|source| !source.ptr_eq(&command_source));
        graph.release_nodes(&[commands.erased()], "toTopic failed command wiring");
        resume_unwind(panic);
    }
    ToTopicBundle { commands }
}

pub(crate) fn attach_message_bus_deferred_command_sink<T: Clone + 'static>(
    graph: &Graph,
    bus: &MessageBus<T>,
    commands: &Node<MessageBusCommand<T>>,
) -> Box<dyn FnOnce()> {
    assert!(
        commands.erased().same_graph(&bus.commands.erased()),
        "messageBus: command sink graph must match"
    );
    let _ = graph;
    let bus_commands = bus.commands.erased();
    commands.subscribe(move |msg| {
        if let crate::protocol::Message::Data(value) = msg {
            if let Ok(command) = value.clone().downcast::<MessageBusCommand<T>>() {
                bus_commands.request_down_next(vec![crate::protocol::Message::Data(Rc::new(
                    (*command).clone(),
                ))]);
            }
        }
    })
}

fn message_bus_command_body<T: Clone + 'static>(
    command_source_count: usize,
) -> impl Fn(&Ctx) + 'static {
    move |ctx| {
        for index in 0..command_source_count {
            for command in ctx.batch::<MessageBusCommand<T>>(index) {
                ctx.emit((*command).clone());
            }
        }
    }
}

fn reduce_message_bus_command<T: Clone + 'static>(
    state: &mut MessageBusState<T>,
    command: MessageBusCommand<T>,
) -> Vec<RuntimeEvent<T>> {
    if let Some(topic) = command.topic() {
        if let Some(error) = validate_topic_key(topic, "messageBus") {
            return reject_command(state, command, error);
        }
    }
    if let Some(subscription_id) = command_subscription_id(&command) {
        if subscription_id.is_empty() {
            return reject_command(
                state,
                command,
                "subscriptionId must be a non-empty string".to_owned(),
            );
        }
    }
    if let Some(command_id) = command.command_id() {
        if state.seen_command_ids.contains(command_id) {
            return duplicate_command_events(state, command, "duplicate commandId");
        }
        state.seen_command_ids.insert(command_id.to_owned());
    }
    if let MessageBusCommand::Publish {
        topic,
        idempotency_key: Some(idempotency_key),
        ..
    } = &command
    {
        if state
            .seen_idempotency_keys
            .contains(&idempotency_key_for(topic, idempotency_key))
        {
            return duplicate_command_events(state, command, "duplicate idempotencyKey");
        }
    }
    match command {
        MessageBusCommand::EnsureTopic { topic, command_id } => {
            ensure_topic(state, topic, command_id)
        }
        MessageBusCommand::CloseTopic { topic, command_id } => {
            close_topic(state, topic, command_id)
        }
        MessageBusCommand::TopicPolicy {
            topic_policy,
            command_id,
        } => {
            state.topic_policy = topic_policy;
            let _ = command_id;
            Vec::new()
        }
        MessageBusCommand::Publish { .. } => publish_message(state, command),
        MessageBusCommand::Ack { .. } => ack_subscription(state, command),
        MessageBusCommand::Seek { .. } => seek_subscription(state, command),
        MessageBusCommand::CloseSubscription { .. } => close_subscription(state, command),
    }
}

fn publish_message<T: Clone + 'static>(
    state: &mut MessageBusState<T>,
    command: MessageBusCommand<T>,
) -> Vec<RuntimeEvent<T>> {
    let MessageBusCommand::Publish {
        topic,
        payload,
        key,
        command_id,
        idempotency_key,
    } = command
    else {
        return Vec::new();
    };
    let mut events = Vec::new();
    if !state.topics.contains_key(&topic) {
        if state.topic_policy != MessageBusTopicPolicy::CreateAsFact {
            return issue_events(
                state,
                Some(MessageBusCommand::Publish {
                    topic,
                    payload,
                    key,
                    command_id,
                    idempotency_key,
                }),
                None,
                "unknown-topic",
                "unknown topic",
            );
        }
        events.extend(ensure_topic(state, topic.clone(), command_id.clone()));
    }
    let Some(topic_state) = state.topics.get(&topic) else {
        return issue_events::<T>(state, None, None, "unknown-topic", "unknown topic");
    };
    if topic_state.closed {
        return issue_events(
            state,
            Some(MessageBusCommand::Publish {
                topic,
                payload,
                key,
                command_id,
                idempotency_key,
            }),
            None,
            "closed-topic",
            "closed topic",
        );
    }
    let timestamp_ms = timestamp_or_zero(state);
    let topic_state = state
        .topics
        .get_mut(&topic)
        .expect("topic checked immediately above");
    let message = MessageEnvelope {
        topic: topic.clone(),
        seq: topic_state.next_seq,
        payload,
        key,
        timestamp_ms,
        command_id: command_id.clone(),
        idempotency_key: idempotency_key.clone(),
    };
    topic_state.next_seq += 1;
    if let Some(idempotency_key) = idempotency_key {
        state
            .seen_idempotency_keys
            .insert(idempotency_key_for(&topic, &idempotency_key));
    }
    topic_state.messages.push(message.clone());
    events.extend([
        RuntimeEvent::Message(message.clone()),
        RuntimeEvent::Status(status_fact(
            state,
            MessageBusStatusKind::MessagePublished,
            StatusFields {
                topic: Some(topic.clone()),
                seq: Some(message.seq),
                command_id,
                ..StatusFields::default()
            },
        )),
    ]);
    events.extend(trim_retention(state, &topic));
    events
}

fn ensure_topic<T: Clone + 'static>(
    state: &mut MessageBusState<T>,
    topic: String,
    command_id: Option<String>,
) -> Vec<RuntimeEvent<T>> {
    state
        .topics
        .entry(topic.clone())
        .or_insert_with(make_topic_state);
    vec![RuntimeEvent::Status(status_fact(
        state,
        MessageBusStatusKind::TopicCreated,
        StatusFields {
            topic: Some(topic),
            command_id,
            ..StatusFields::default()
        },
    ))]
}

fn close_topic<T: Clone + 'static>(
    state: &mut MessageBusState<T>,
    topic: String,
    command_id: Option<String>,
) -> Vec<RuntimeEvent<T>> {
    let Some(topic_state) = state.topics.get_mut(&topic) else {
        return issue_events(
            state,
            Some(MessageBusCommand::CloseTopic { topic, command_id }),
            None,
            "unknown-topic",
            "unknown topic",
        );
    };
    topic_state.closed = true;
    vec![RuntimeEvent::Status(status_fact(
        state,
        MessageBusStatusKind::TopicClosed,
        StatusFields {
            topic: Some(topic),
            command_id,
            ..StatusFields::default()
        },
    ))]
}

fn trim_retention<T: Clone + 'static>(
    state: &mut MessageBusState<T>,
    topic_name: &str,
) -> Vec<RuntimeEvent<T>> {
    let now = timestamp_or_zero(state);
    let Some(topic) = state.topics.get_mut(topic_name) else {
        return Vec::new();
    };
    let before_head = topic.head_seq;
    if let Some(max_age_ms) = state.retention.max_age_ms {
        topic
            .messages
            .retain(|message| now.saturating_sub(message.timestamp_ms) <= max_age_ms);
    }
    if let Some(max_messages) = state.retention.max_messages {
        if max_messages == 0 {
            return issue_events::<T>(
                state,
                None,
                None,
                "policy-rejected",
                "retention.maxMessages must be positive",
            );
        }
        if topic.messages.len() > max_messages {
            let trim_count = topic.messages.len() - max_messages;
            topic.messages.drain(0..trim_count);
        }
    }
    topic.head_seq = topic
        .messages
        .first()
        .map_or(topic.next_seq, |message| message.seq);
    if topic.head_seq == before_head {
        return Vec::new();
    }
    let head_seq = topic.head_seq;
    let trim_count = head_seq.saturating_sub(before_head);
    let mut events = vec![RuntimeEvent::Status(status_fact(
        state,
        MessageBusStatusKind::RetentionTrimmed,
        StatusFields {
            topic: Some(topic_name.to_owned()),
            head_seq: Some(head_seq),
            details: Some(format!("trimCount={trim_count}")),
            ..StatusFields::default()
        },
    ))];
    let affected = state
        .subscriptions
        .values_mut()
        .filter(|sub| {
            !sub.closed && sub.topic == topic_name && sub.next_seq < head_seq && !sub.retention_gap
        })
        .map(|sub| {
            sub.retention_gap = true;
            sub.subscription_id.clone()
        })
        .collect::<Vec<_>>();
    for subscription_id in affected {
        events.extend(issue_events::<T>(
            state,
            None,
            None,
            "retention-gap",
            format!("subscription '{subscription_id}' is before retained headSeq"),
        ));
    }
    events
}

fn ack_subscription<T: Clone + 'static>(
    state: &mut MessageBusState<T>,
    command: MessageBusCommand<T>,
) -> Vec<RuntimeEvent<T>> {
    let MessageBusCommand::Ack {
        topic,
        subscription_id,
        seq,
        command_id,
    } = command
    else {
        return Vec::new();
    };
    let Some(topic_state) = state.topics.get(&topic) else {
        return issue_events::<T>(state, None, None, "unknown-topic", "unknown topic");
    };
    let key = subscription_key(&topic, &subscription_id);
    let Some(sub) = state.subscriptions.get_mut(&key) else {
        return issue_events::<T>(
            state,
            None,
            None,
            "unknown-subscription",
            "unknown subscription",
        );
    };
    if sub.closed {
        return issue_events::<T>(
            state,
            None,
            None,
            "subscription-closed",
            "subscription is closed",
        );
    }
    if sub.retention_gap {
        return issue_events::<T>(
            state,
            None,
            None,
            "retention-gap",
            "subscription must seek before ack",
        );
    }
    if seq < sub.next_seq {
        return issue_events::<T>(
            state,
            None,
            None,
            "source-cursor-stale",
            "ack is behind subscription cursor",
        );
    }
    if seq >= topic_state.next_seq {
        return issue_events::<T>(
            state,
            None,
            None,
            "cursor-out-of-range",
            "ack is beyond topic tail",
        );
    }
    sub.next_seq = seq + 1;
    vec![RuntimeEvent::Status(status_fact(
        state,
        MessageBusStatusKind::SubscriptionAcked,
        StatusFields {
            topic: Some(topic),
            subscription_id: Some(subscription_id),
            next_seq: Some(seq + 1),
            command_id,
            ..StatusFields::default()
        },
    ))]
}

fn seek_subscription<T: Clone + 'static>(
    state: &mut MessageBusState<T>,
    command: MessageBusCommand<T>,
) -> Vec<RuntimeEvent<T>> {
    let MessageBusCommand::Seek {
        topic,
        subscription_id,
        next_seq,
        command_id,
    } = command
    else {
        return Vec::new();
    };
    let Some(topic_state) = state.topics.get(&topic) else {
        return issue_events::<T>(state, None, None, "unknown-topic", "unknown topic");
    };
    let key = subscription_key(&topic, &subscription_id);
    let Some(sub) = state.subscriptions.get_mut(&key) else {
        return issue_events::<T>(
            state,
            None,
            None,
            "unknown-subscription",
            "unknown subscription",
        );
    };
    if sub.closed {
        return issue_events::<T>(
            state,
            None,
            None,
            "subscription-closed",
            "subscription is closed",
        );
    }
    if next_seq < topic_state.head_seq {
        return issue_events::<T>(
            state,
            None,
            None,
            "retention-gap",
            "seek is before retained headSeq",
        );
    }
    if next_seq > topic_state.next_seq {
        return issue_events::<T>(
            state,
            None,
            None,
            "cursor-out-of-range",
            "seek is beyond topic tail",
        );
    }
    sub.next_seq = next_seq;
    sub.retention_gap = false;
    vec![RuntimeEvent::Status(status_fact(
        state,
        MessageBusStatusKind::SubscriptionSought,
        StatusFields {
            topic: Some(topic),
            subscription_id: Some(subscription_id),
            next_seq: Some(next_seq),
            command_id,
            ..StatusFields::default()
        },
    ))]
}

fn close_subscription<T: Clone + 'static>(
    state: &mut MessageBusState<T>,
    command: MessageBusCommand<T>,
) -> Vec<RuntimeEvent<T>> {
    let MessageBusCommand::CloseSubscription {
        topic,
        subscription_id,
        command_id,
    } = command
    else {
        return Vec::new();
    };
    if !state.topics.contains_key(&topic) {
        return issue_events::<T>(state, None, None, "unknown-topic", "unknown topic");
    }
    let key = subscription_key(&topic, &subscription_id);
    let Some(sub) = state.subscriptions.get_mut(&key) else {
        return issue_events::<T>(
            state,
            None,
            None,
            "unknown-subscription",
            "unknown subscription",
        );
    };
    sub.closed = true;
    let next_seq = sub.next_seq;
    vec![RuntimeEvent::Status(status_fact(
        state,
        MessageBusStatusKind::SubscriptionClosed,
        StatusFields {
            topic: Some(topic),
            subscription_id: Some(subscription_id),
            next_seq: Some(next_seq),
            command_id,
            ..StatusFields::default()
        },
    ))]
}

fn issue_events<T: Clone + 'static>(
    state: &mut MessageBusState<T>,
    command: Option<MessageBusCommand<T>>,
    message: Option<MessageEnvelope<T>>,
    code: &str,
    issue_message: impl Into<String>,
) -> Vec<RuntimeEvent<T>> {
    let topic = command
        .as_ref()
        .and_then(MessageBusCommand::topic)
        .map(str::to_owned)
        .or_else(|| message.as_ref().map(|message| message.topic.clone()));
    let issue = DataIssue {
        kind: "issue".to_owned(),
        code: code.to_owned(),
        message: issue_message.into(),
        severity: "error".to_owned(),
        source: "messageBus".to_owned(),
        topic: topic.clone(),
        details: None,
    };
    let entry = MessageBusDeadLetterEntry {
        entry_seq: state.dead_letter_seq + 1,
        topic: topic.clone(),
        command,
        message,
        issue: issue.clone(),
        timestamp_ms: timestamp_or_zero(state),
    };
    state.dead_letter_seq = entry.entry_seq;
    state.dead_letters.push(entry);
    vec![RuntimeEvent::Issue(issue)]
}

fn reject_command<T: Clone + 'static>(
    state: &mut MessageBusState<T>,
    command: MessageBusCommand<T>,
    reason: String,
) -> Vec<RuntimeEvent<T>> {
    issue_events(state, Some(command), None, "malformed-command", reason)
}

fn duplicate_command_events<T: Clone + 'static>(
    state: &mut MessageBusState<T>,
    command: MessageBusCommand<T>,
    message: &str,
) -> Vec<RuntimeEvent<T>> {
    let status = RuntimeEvent::Status(status_fact(
        state,
        MessageBusStatusKind::DuplicateCommand,
        StatusFields {
            topic: command.topic().map(str::to_owned),
            command_id: command.command_id().map(str::to_owned),
            ..StatusFields::default()
        },
    ));
    if state.dedupe.command_id == MessageBusDedupeAction::Issue {
        return issue_events(state, Some(command), None, "duplicate-command", message);
    }
    vec![status]
}

#[derive(Default)]
struct StatusFields {
    topic: Option<String>,
    seq: Option<u64>,
    head_seq: Option<u64>,
    subscription_id: Option<String>,
    next_seq: Option<u64>,
    command_id: Option<String>,
    details: Option<String>,
}

fn status_fact<T>(
    state: &MessageBusState<T>,
    kind: MessageBusStatusKind,
    fields: StatusFields,
) -> MessageBusStatus {
    MessageBusStatus {
        kind,
        topic: fields.topic,
        seq: fields.seq,
        head_seq: fields.head_seq,
        subscription_id: fields.subscription_id,
        next_seq: fields.next_seq,
        command_id: fields.command_id,
        issue_code: None,
        timestamp_ms: timestamp_or_zero(state),
        details: fields.details,
    }
}

fn catalog_page<T>(
    state: &MessageBusState<T>,
    params: &MessageBusCatalogParams,
) -> MessageBusCatalogPage {
    let limit = positive_limit(params.limit);
    let topics = state
        .topics
        .iter()
        .filter(|(topic, value)| {
            (params.include_closed || !value.closed)
                && params
                    .after_topic
                    .as_ref()
                    .is_none_or(|after| *topic > after)
        })
        .collect::<Vec<_>>();
    let has_more = topics.len() > limit;
    let page = topics
        .into_iter()
        .take(limit)
        .map(|(topic, value)| MessageBusCatalogEntry {
            topic: topic.clone(),
            closed: value.closed,
            head_seq: value.head_seq,
            next_seq: value.next_seq,
            message_count: value.messages.len(),
        })
        .collect::<Vec<_>>();
    let next_after_topic = if has_more {
        page.last().map(|entry| entry.topic.clone())
    } else {
        None
    };
    MessageBusCatalogPage {
        topics: page,
        next_after_topic,
        has_more,
    }
}

fn topic_page<T: Clone>(
    state: &MessageBusState<T>,
    topic_name: &str,
    params: &MessageBusTopicParams,
) -> MessageBusTopicPage<T> {
    let start = params.after_seq.map_or_else(
        || {
            state
                .topics
                .get(topic_name)
                .map_or(1, |topic| topic.head_seq)
        },
        |seq| seq + 1,
    );
    let limit = positive_limit(params.limit);
    let all = state
        .topics
        .get(topic_name)
        .map(|topic| {
            topic
                .messages
                .iter()
                .filter(|message| message.seq >= start)
                .cloned()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let has_more = all.len() > limit;
    let messages = all.into_iter().take(limit).collect::<Vec<_>>();
    let through_seq = messages.last().map(|message| message.seq);
    MessageBusTopicPage {
        topic: topic_name.to_owned(),
        messages,
        from_seq: start,
        through_seq,
        next_after_seq: if has_more { through_seq } else { None },
        has_more,
    }
}

fn available_page<T: Clone>(
    state: &MessageBusState<T>,
    sub_key: &SubscriptionState,
    params: &MessageBusAvailableParams,
) -> MessageBusAvailablePage<T> {
    let key = subscription_key(&sub_key.topic, &sub_key.subscription_id);
    let sub = state.subscriptions.get(&key).unwrap_or(sub_key);
    let cursor = cursor_snapshot(state, sub);
    let start = params.after_seq.map_or(sub.next_seq, |seq| seq + 1);
    let all = if sub.retention_gap {
        Vec::new()
    } else {
        state
            .topics
            .get(&sub.topic)
            .map(|topic| {
                topic
                    .messages
                    .iter()
                    .filter(|message| message.seq >= start)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    };
    let limit = positive_limit(params.limit);
    let has_more = all.len() > limit;
    let messages = all.into_iter().take(limit).collect::<Vec<_>>();
    let through_seq = messages.last().map(|message| message.seq);
    MessageBusAvailablePage {
        topic: sub.topic.clone(),
        subscription_id: sub.subscription_id.clone(),
        cursor,
        messages,
        from_seq: start,
        through_seq,
        next_after_seq: if has_more { through_seq } else { None },
        has_more,
    }
}

fn dead_letter_page<T: Clone>(
    state: &MessageBusState<T>,
    params: &MessageBusDeadLetterParams,
) -> MessageBusDeadLetterPage<T> {
    let limit = positive_limit(params.limit);
    let entries = state
        .dead_letters
        .iter()
        .filter(|entry| {
            params
                .after_entry_seq
                .is_none_or(|after| entry.entry_seq > after)
                && params
                    .topic
                    .as_ref()
                    .is_none_or(|topic| entry.topic.as_ref() == Some(topic))
                && params
                    .code
                    .as_ref()
                    .is_none_or(|code| &entry.issue.code == code)
        })
        .cloned()
        .collect::<Vec<_>>();
    let has_more = entries.len() > limit;
    let page = entries.into_iter().take(limit).collect::<Vec<_>>();
    let next_after_entry_seq = if has_more {
        page.last().map(|entry| entry.entry_seq)
    } else {
        None
    };
    MessageBusDeadLetterPage {
        entries: page,
        next_after_entry_seq,
        has_more,
    }
}

fn cursor_snapshot<T>(state: &MessageBusState<T>, sub: &SubscriptionState) -> MessageBusCursor {
    let key = subscription_key(&sub.topic, &sub.subscription_id);
    let sub = state.subscriptions.get(&key).unwrap_or(sub);
    MessageBusCursor {
        topic: sub.topic.clone(),
        subscription_id: sub.subscription_id.clone(),
        next_seq: sub.next_seq,
        closed: sub.closed,
        retention_gap: sub.retention_gap,
        head_seq: state
            .topics
            .get(&sub.topic)
            .map_or(1, |topic| topic.head_seq),
    }
}

fn ensure_subscription<T>(
    state: &mut MessageBusState<T>,
    opts: &MessageBusSubscriptionOptions,
) -> (SubscriptionState, bool, Option<DataIssue>) {
    let key = subscription_key(&opts.topic, &opts.subscription_id);
    if let Some(existing) = state.subscriptions.get(&key) {
        return (existing.clone(), false, None);
    }
    let topic_range = state
        .topics
        .get(&opts.topic)
        .map(|topic| (topic.head_seq, topic.next_seq));
    let next_seq = match opts.from {
        MessageBusSubscriptionFrom::Earliest => topic_range.map_or(1, |(head_seq, _)| head_seq),
        MessageBusSubscriptionFrom::Latest => topic_range.map_or(1, |(_, next_seq)| next_seq),
        MessageBusSubscriptionFrom::Seq(seq) => seq,
    };
    let issue = topic_range.and_then(|(head_seq, tail_seq)| {
        if next_seq < head_seq || next_seq > tail_seq {
            Some(DataIssue {
                kind: "issue".to_owned(),
                code: "cursor-out-of-range".to_owned(),
                message: format!(
                    "subscription start seq {next_seq} is outside retained range {}..={}",
                    head_seq, tail_seq
                ),
                severity: "error".to_owned(),
                source: "messageBus".to_owned(),
                topic: Some(opts.topic.clone()),
                details: Some(format!("subscriptionId={}", opts.subscription_id)),
            })
        } else {
            None
        }
    });
    let next_seq = if issue.is_some() {
        topic_range.map_or(1, |(head_seq, _)| head_seq)
    } else {
        next_seq
    };
    if let Some(issue) = issue.clone() {
        let entry = MessageBusDeadLetterEntry {
            entry_seq: state.dead_letter_seq + 1,
            topic: Some(opts.topic.clone()),
            command: None,
            message: None,
            issue,
            timestamp_ms: timestamp_or_zero(state),
        };
        state.dead_letter_seq = entry.entry_seq;
        state.dead_letters.push(entry);
    }
    let sub = SubscriptionState {
        topic: opts.topic.clone(),
        subscription_id: opts.subscription_id.clone(),
        next_seq,
        closed: false,
        retention_gap: topic_range.is_some_and(|(head_seq, _)| next_seq < head_seq),
    };
    state.subscriptions.insert(key, sub.clone());
    (sub, true, issue)
}

fn pull_params<T: Clone + 'static>(ctx: &Ctx) -> Option<T> {
    ctx.pull()
        .and_then(|pull| pull.params::<T>())
        .map(|params| (*params).clone())
}

fn make_topic_state<T>() -> TopicState<T> {
    TopicState {
        closed: false,
        head_seq: 1,
        next_seq: 1,
        messages: Vec::new(),
    }
}

fn unique_topics(topics: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut unique = Vec::with_capacity(topics.len());
    for topic in topics {
        assert_topic_key(&topic, "messageBus");
        assert!(seen.insert(topic.clone()), "messageBus: duplicate topic");
        unique.push(topic);
    }
    unique.sort();
    unique
}

fn assert_topic_key(topic: &str, owner: &str) {
    if let Some(error) = validate_topic_key(topic, owner) {
        panic!("{error}");
    }
}

fn validate_topic_key(topic: &str, owner: &str) -> Option<String> {
    if topic.is_empty() {
        return Some(format!("{owner}: topic must be a non-empty string"));
    }
    None
}

fn assert_non_empty(value: &str, owner: &str) {
    assert!(!value.is_empty(), "{owner}: must be a non-empty string");
}

fn positive_limit(limit: Option<usize>) -> usize {
    let limit = limit.unwrap_or(100);
    assert!(limit > 0, "messageBus: limit must be positive");
    limit
}

fn command_subscription_id<T>(command: &MessageBusCommand<T>) -> Option<&str> {
    match command {
        MessageBusCommand::Ack {
            subscription_id, ..
        }
        | MessageBusCommand::Seek {
            subscription_id, ..
        }
        | MessageBusCommand::CloseSubscription {
            subscription_id, ..
        } => Some(subscription_id),
        _ => None,
    }
}

fn subscription_key(topic: &str, subscription_id: &str) -> String {
    canonical_tuple_key(&[topic, subscription_id])
}

fn idempotency_key_for(topic: &str, idempotency_key: &str) -> String {
    canonical_tuple_key(&[topic, idempotency_key])
}

fn timestamp_or_zero<T>(state: &MessageBusState<T>) -> u64 {
    catch_unwind(AssertUnwindSafe(|| (state.now)())).unwrap_or(0)
}

fn node_opts(name: impl Into<String>, factory: impl Into<String>) -> GraphNodeOpts {
    let mut opts = GraphNodeOpts::named(name);
    opts.node = NodeOpts {
        factory: Some(factory.into()),
        complete_when_deps_complete: false,
        error_when_deps_error: false,
        ..opts.node
    };
    opts
}

fn pull_node_opts(
    name: impl Into<String>,
    factory: impl Into<String>,
    pull_id: LockId,
) -> GraphNodeOpts {
    let mut opts = node_opts(name, factory);
    opts.node.pull_id = Some(pull_id);
    opts.node.partial = true;
    opts
}

fn is_reachable_upstream(from: &Core, target: &Core) -> bool {
    let mut seen = HashSet::new();
    let mut stack = vec![from.clone()];
    while let Some(node) = stack.pop() {
        if node.ptr_eq(target) {
            return true;
        }
        if !seen.insert(node.identity_key()) {
            continue;
        }
        stack.extend(node.deps());
    }
    false
}

#[cfg(test)]
mod clean_slate_tests {
    use super::*;
    use crate::graph::graph;
    use crate::protocol::{Message, PullDemand};

    #[test]
    fn message_bus_core_exposes_clean_slate_nodes() {
        let g = graph();
        let _bus = message_bus::<String>(
            &g,
            MessageBusOptions::named("bus")
                .with_topics(["orders"])
                .with_now(|| 10),
        );
        let snap = g.describe();
        for id in [
            "bus/commands",
            "bus/runtime",
            "bus/messages",
            "bus/status",
            "bus/issues",
        ] {
            assert!(snap.nodes.iter().any(|node| node.id == id), "{id}");
        }
        assert!(!snap.nodes.iter().any(|node| node.id.contains("dynamicHub")));
    }

    #[test]
    fn unknown_topic_is_strict_issue_without_retained_message() {
        let g = graph();
        let bus = message_bus::<String>(&g, MessageBusOptions::named("bus").with_now(|| 20));
        let _messages = bus.messages.subscribe(|_| {});
        let _issues = bus.issues.subscribe(|_| {});
        let _status = bus.status.subscribe(|_| {});

        bus.publish(
            "missing",
            "payload".to_owned(),
            None,
            Some("c1".to_owned()),
            None,
        );

        assert!(bus.messages.cache().is_none());
        assert_eq!(bus.issues.cache().unwrap().code, "unknown-topic");
        assert!(bus.status.cache().is_none());
    }

    #[test]
    fn catalog_topic_and_dead_letter_are_pull_read_only_projections() {
        let g = graph();
        let bus = message_bus::<String>(
            &g,
            MessageBusOptions::named("bus")
                .with_topics(["orders"])
                .with_now(|| 30),
        );
        let catalog = bus.catalog();
        let topic = bus.topic("orders");
        let dead = bus.dead_letter();
        let _catalog = catalog.snapshot.subscribe(|_| {});
        let _topic = topic.snapshot.subscribe(|_| {});
        let _dead = dead.snapshot.subscribe(|_| {});

        bus.publish("orders", "o1".to_owned(), None, None, None);
        bus.publish("missing", "x".to_owned(), None, None, None);
        catalog.snapshot.up(vec![Message::Pull(PullDemand::new(
            catalog.snapshot_pull_id.clone(),
        ))]);
        topic
            .snapshot
            .up(vec![Message::Pull(PullDemand::with_params(
                topic.snapshot_pull_id.clone(),
                MessageBusTopicParams {
                    limit: Some(1),
                    after_seq: None,
                },
            ))]);
        dead.snapshot.up(vec![Message::Pull(PullDemand::with_params(
            dead.snapshot_pull_id.clone(),
            MessageBusDeadLetterParams {
                limit: None,
                after_entry_seq: None,
                topic: None,
                code: Some("unknown-topic".to_owned()),
            },
        ))]);

        assert_eq!(catalog.snapshot.cache().unwrap().topics[0].topic, "orders");
        assert_eq!(topic.snapshot.cache().unwrap().messages[0].payload, "o1");
        assert_eq!(
            dead.snapshot.cache().unwrap().entries[0].issue.code,
            "unknown-topic"
        );
    }

    #[test]
    fn available_pull_does_not_move_cursor_ack_seek_close_do() {
        let g = graph();
        let bus = message_bus::<String>(
            &g,
            MessageBusOptions::named("bus")
                .with_topics(["orders"])
                .with_now(|| 40),
        );
        let sub = bus.subscription(MessageBusSubscriptionOptions::new("orders", "s1"));
        let _available = sub.available.subscribe(|_| {});
        let _cursor = sub.cursor.subscribe(|_| {});

        bus.publish("orders", "o1".to_owned(), None, None, None);
        bus.publish("orders", "o2".to_owned(), None, None, None);
        sub.available.up(vec![Message::Pull(PullDemand::with_params(
            sub.available_pull_id.clone(),
            MessageBusAvailableParams {
                limit: Some(1),
                after_seq: Some(1),
            },
        ))]);

        let page = sub.available.cache().unwrap();
        assert_eq!(page.messages[0].seq, 2);
        assert_eq!(page.cursor.next_seq, 1);
        let opened_cursor = sub.cursor.cache().unwrap();
        assert_eq!(opened_cursor.next_seq, 1);
        assert!(!opened_cursor.retention_gap);

        sub.ack(1, None);
        assert_eq!(sub.cursor.cache().unwrap().next_seq, 2);
        sub.seek(1, None);
        assert_eq!(sub.cursor.cache().unwrap().next_seq, 1);
        sub.close(None);
        assert!(sub.cursor.cache().unwrap().closed);
    }

    #[test]
    fn retention_count_advances_head_and_marks_gap_until_seek() {
        let g = graph();
        let bus = message_bus::<String>(
            &g,
            MessageBusOptions::named("bus")
                .with_topics(["orders"])
                .with_retention(MessageBusRetentionPolicy {
                    max_messages: Some(1),
                    max_age_ms: None,
                }),
        );
        let sub = bus.subscription(MessageBusSubscriptionOptions::new("orders", "s1"));
        let _cursor = sub.cursor.subscribe(|_| {});
        let _issues = bus.issues.subscribe(|_| {});

        bus.publish("orders", "o1".to_owned(), None, None, None);
        bus.publish("orders", "o2".to_owned(), None, None, None);

        assert_eq!(bus.issues.cache().unwrap().code, "retention-gap");
        assert!(sub.cursor.cache().unwrap().retention_gap);
        sub.seek(2, None);
        let cursor = sub.cursor.cache().unwrap();
        assert_eq!(cursor.next_seq, 2);
        assert!(!cursor.retention_gap);
    }

    #[test]
    fn invalid_subscription_start_seq_is_visible_issue_not_impossible_cursor() {
        let g = graph();
        let bus = message_bus::<String>(
            &g,
            MessageBusOptions::named("bus")
                .with_topics(["orders"])
                .with_retention(MessageBusRetentionPolicy {
                    max_messages: Some(1),
                    max_age_ms: None,
                }),
        );
        let _issues = bus.issues.subscribe(|_| {});
        let dead = bus.dead_letter();
        let _dead = dead.snapshot.subscribe(|_| {});

        bus.publish("orders", "o1".to_owned(), None, None, None);
        bus.publish("orders", "o2".to_owned(), None, None, None);
        let sub = bus.subscription(
            MessageBusSubscriptionOptions::new("orders", "late")
                .from(MessageBusSubscriptionFrom::Seq(1)),
        );
        let _cursor = sub.cursor.subscribe(|_| {});
        dead.snapshot.up(vec![Message::Pull(PullDemand::with_params(
            dead.snapshot_pull_id.clone(),
            MessageBusDeadLetterParams {
                limit: None,
                after_entry_seq: None,
                topic: Some("orders".to_owned()),
                code: Some("cursor-out-of-range".to_owned()),
            },
        ))]);

        assert_eq!(bus.issues.cache().unwrap().code, "cursor-out-of-range");
        assert_eq!(sub.cursor.cache().unwrap().next_seq, 2);
        assert_eq!(
            dead.snapshot.cache().unwrap().entries[0].issue.code,
            "cursor-out-of-range"
        );
    }
}
