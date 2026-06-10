//! Graph-visible message bus application infrastructure (D132/D135).
//!
//! Topics are declared up front and represented as graph-owned fan-in nodes.
//! `publish` is boundary sugar that writes an ordinary DATA fact; `to_topic`
//! wires an explicit producer node into the topic so the graph topology remains
//! inspectable (D39/D132).
//!
//! Dynamic hubs are facts-dynamic and topology-static (D135): topic lifecycle is
//! represented by DATA facts on fixed graph-visible nodes; topic keys do not
//! create or delete graph nodes.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::rc::Rc;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ctx::{Ctx, DepTerminal};
use crate::graph::{Graph, GraphNodeOpts};
use crate::json::JsonValue;
use crate::node::{Core, Node};
use crate::protocol::AnyValue;

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

#[derive(Clone)]
pub struct MessageEnvelope {
    pub topic: String,
    pub seq: u64,
    pub payload: AnyValue,
    pub key: Option<String>,
    pub timestamp_ms: u64,
}

impl MessageEnvelope {
    pub fn payload<T: 'static>(&self) -> Option<Rc<T>> {
        self.payload.clone().downcast::<T>().ok()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageBusEvent {
    Publish { topic: String, seq: u64 },
    Complete { topic: String },
    Error { topic: String, error: String },
}

#[derive(Clone)]
pub struct MessageBus {
    topics: Rc<Vec<String>>,
    records: Rc<HashMap<String, TopicRecord>>,
    next_seq: Rc<Cell<u64>>,
    now: Rc<dyn Fn() -> u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DynamicHubUnknownTopicPolicy {
    Drop,
    Error,
    DeadLetter,
    CreateAsFact,
}

#[derive(Clone)]
pub struct DynamicHubOptions {
    pub name: String,
    pub topics: Vec<String>,
    pub unknown_topic: DynamicHubUnknownTopicPolicy,
    pub dead_letter: bool,
    pub max_topics: usize,
    pub max_topic_length: usize,
    pub now: Rc<dyn Fn() -> u64>,
}

impl Default for DynamicHubOptions {
    fn default() -> Self {
        Self {
            name: "dynamicHub".to_owned(),
            topics: Vec::new(),
            unknown_topic: DynamicHubUnknownTopicPolicy::Error,
            dead_letter: false,
            max_topics: DEFAULT_DYNAMIC_HUB_MAX_TOPICS,
            max_topic_length: DEFAULT_DYNAMIC_HUB_MAX_TOPIC_LENGTH,
            now: Rc::new(|| 0),
        }
    }
}

impl DynamicHubOptions {
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

    pub fn with_unknown_topic(mut self, policy: DynamicHubUnknownTopicPolicy) -> Self {
        self.unknown_topic = policy;
        self
    }

    pub fn with_dead_letter(mut self, on: bool) -> Self {
        self.dead_letter = on;
        self
    }

    pub fn with_max_topics(mut self, max_topics: usize) -> Self {
        self.max_topics = max_topics;
        self
    }

    pub fn with_max_topic_length(mut self, max_topic_length: usize) -> Self {
        self.max_topic_length = max_topic_length;
        self
    }

    pub fn with_now(mut self, now: impl Fn() -> u64 + 'static) -> Self {
        self.now = Rc::new(now);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DynamicHubMetadata {
    pub seq: u64,
    pub cursor: u64,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DynamicHubEventKind {
    Create,
    Delete,
    Message,
    Subscribe,
    Close,
    Error,
    DeadLetter,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DynamicHubCommand<T = AnyValue> {
    Create {
        topic: String,
        key: Option<String>,
        payload: Option<T>,
    },
    Delete {
        topic: String,
        key: Option<String>,
        payload: Option<T>,
    },
    Publish {
        topic: String,
        payload: T,
        key: Option<String>,
    },
    Subscribe {
        topic: String,
        key: Option<String>,
        payload: Option<T>,
    },
    Close {
        key: Option<String>,
        payload: Option<T>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DynamicHubStatus {
    pub open: bool,
    pub topics: Vec<String>,
    pub seq: u64,
    pub cursor: u64,
    pub last_event_kind: Option<DynamicHubEventKind>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DynamicHubEvent<T = AnyValue> {
    pub kind: DynamicHubEventKind,
    pub topic: Option<String>,
    pub payload: Option<T>,
    pub key: Option<String>,
    pub error: Option<String>,
    pub reason: Option<String>,
    pub command: Option<DynamicHubCommand<T>>,
    pub meta: DynamicHubMetadata,
    pub status: DynamicHubStatus,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DynamicHubError<T = AnyValue> {
    pub topic: Option<String>,
    pub error: String,
    pub command: DynamicHubCommand<T>,
    pub meta: DynamicHubMetadata,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DynamicHubDeadLetter<T = AnyValue> {
    pub topic: Option<String>,
    pub reason: String,
    pub command: DynamicHubCommand<T>,
    pub meta: DynamicHubMetadata,
}

#[derive(Clone)]
pub struct DynamicHub<T = AnyValue> {
    graph: Graph,
    pub command: Node<DynamicHubCommand<T>>,
    pub events: Node<DynamicHubEvent<T>>,
    pub status: Node<DynamicHubStatus>,
    pub errors: Node<DynamicHubError<T>>,
    pub dead_letter: Option<Node<DynamicHubDeadLetter<T>>>,
    name: Rc<String>,
    max_topic_length: usize,
    command_sources: Rc<RefCell<Vec<Core>>>,
    _events_retain: Rc<DynamicHubRetain>,
}

#[derive(Clone)]
pub struct ToHubTopicBundle<T> {
    pub commands: Node<DynamicHubCommand<T>>,
}

#[derive(Clone)]
struct DynamicHubRuntime {
    topics: Vec<String>,
    open: bool,
    seq: u64,
    cursor: u64,
}

struct DynamicHubRetain {
    release: RefCell<Option<Box<dyn FnOnce()>>>,
}

impl DynamicHubRetain {
    fn new(release: Box<dyn FnOnce()>) -> Self {
        Self {
            release: RefCell::new(Some(release)),
        }
    }
}

impl Drop for DynamicHubRetain {
    fn drop(&mut self) {
        if let Some(release) = self.release.borrow_mut().take() {
            release();
        }
    }
}

const DEFAULT_DYNAMIC_HUB_MAX_TOPICS: usize = 1024;
const DEFAULT_DYNAMIC_HUB_MAX_TOPIC_LENGTH: usize = 256;

#[derive(Clone)]
struct TopicRecord {
    node: Node<MessageEnvelope>,
    producers: Rc<RefCell<Vec<Core>>>,
}

#[derive(Clone)]
enum ProducedTopicMessage {
    Publish(MessageEnvelope),
}

impl MessageBus {
    pub fn new(graph: &Graph, topics: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self::with_name(graph, topics, "messageBus", || 0)
    }

    pub fn with_name(
        graph: &Graph,
        topics: impl IntoIterator<Item = impl Into<String>>,
        name: impl Into<String>,
        now: impl Fn() -> u64 + 'static,
    ) -> Self {
        let name = name.into();
        let mut topic_names = Vec::new();
        let mut records = HashMap::new();
        for topic in topics {
            let topic = topic.into();
            assert!(!topic.is_empty(), "MessageBus: topic must not be empty");
            assert!(
                !records.contains_key(&topic),
                "MessageBus: duplicate topic '{topic}'"
            );
            let producers = Rc::new(RefCell::new(Vec::new()));
            let mut opts = GraphNodeOpts::named(format!("{name}/{topic}"));
            opts.node.complete_when_deps_complete = false;
            opts.node.error_when_deps_error = false;
            let node =
                graph.node_opts::<MessageEnvelope, _>(vec![], topic_body(producers.clone()), opts);
            topic_names.push(topic.clone());
            records.insert(topic, TopicRecord { node, producers });
        }
        assert!(
            !topic_names.is_empty(),
            "MessageBus: at least one topic is required"
        );
        Self {
            topics: Rc::new(topic_names),
            records: Rc::new(records),
            next_seq: Rc::new(Cell::new(0)),
            now: Rc::new(now),
        }
    }

    pub fn topics(&self) -> &[String] {
        self.topics.as_slice()
    }

    pub fn has(&self, topic: &str) -> bool {
        self.records.contains_key(topic)
    }

    pub fn topic(&self, topic: &str) -> Node<MessageEnvelope> {
        self.records
            .get(topic)
            .unwrap_or_else(|| panic!("MessageBus: unknown topic '{topic}'"))
            .node
            .clone()
    }

    pub fn publish<T: 'static>(
        &self,
        topic: &str,
        payload: T,
        key: Option<String>,
    ) -> MessageEnvelope {
        let node = self.topic(topic);
        let envelope = self.envelope(topic, payload, key);
        node.set(envelope.clone());
        envelope
    }

    fn envelope<T: 'static>(
        &self,
        topic: &str,
        payload: T,
        key: Option<String>,
    ) -> MessageEnvelope {
        let seq = self.next_seq.get() + 1;
        self.next_seq.set(seq);
        MessageEnvelope {
            topic: topic.to_owned(),
            seq,
            payload: Rc::new(payload),
            key,
            timestamp_ms: (self.now)(),
        }
    }

    fn record(&self, topic: &str) -> TopicRecord {
        self.records
            .get(topic)
            .unwrap_or_else(|| panic!("MessageBus: unknown topic '{topic}'"))
            .clone()
    }
}

pub fn message_bus(
    graph: &Graph,
    topics: impl IntoIterator<Item = impl Into<String>>,
) -> MessageBus {
    MessageBus::new(graph, topics)
}

pub fn from_topic(bus: &MessageBus, topic: &str) -> Node<MessageEnvelope> {
    bus.topic(topic)
}

pub fn to_topic<T: Clone + 'static>(
    graph: &Graph,
    source: &Node<T>,
    bus: MessageBus,
    topic: impl Into<String>,
    name: impl Into<String>,
) -> Node<MessageBusEvent> {
    let topic = topic.into();
    let name = name.into();
    let record = bus.record(&topic);
    let topic_for_fn = topic.clone();
    let bus_for_producer = bus.clone();
    let producer_topic = topic.clone();
    let producer = graph.node_opts::<ProducedTopicMessage, _>(
        vec![source.erased()],
        move |ctx: &Ctx| {
            for value in ctx.batch::<T>(0) {
                let envelope = bus_for_producer.envelope(&producer_topic, (*value).clone(), None);
                ctx.emit(ProducedTopicMessage::Publish(envelope));
            }
        },
        {
            let mut opts = GraphNodeOpts::named(name.clone());
            opts.node.complete_when_deps_complete = false;
            opts.node.error_when_deps_error = false;
            opts
        },
    );
    record.producers.borrow_mut().push(producer.erased());
    let topic_deps = record.producers.borrow().clone();
    let topic_producers = record.producers.clone();
    record
        .node
        .replace_deps(topic_deps, topic_body(topic_producers));

    graph.node_opts::<MessageBusEvent, _>(
        vec![producer.erased(), source.erased()],
        move |ctx: &Ctx| {
            for produced in ctx.batch::<ProducedTopicMessage>(0) {
                let ProducedTopicMessage::Publish(envelope) = produced.as_ref();
                ctx.emit(MessageBusEvent::Publish {
                    topic: topic_for_fn.clone(),
                    seq: envelope.seq,
                });
            }
            match ctx.terminal(1) {
                Some(DepTerminal::Complete) => ctx.emit(MessageBusEvent::Complete {
                    topic: topic_for_fn.clone(),
                }),
                Some(DepTerminal::Error(error)) => ctx.emit(MessageBusEvent::Error {
                    topic: topic_for_fn.clone(),
                    error: error.to_string(),
                }),
                None => {}
            }
        },
        {
            let mut opts = GraphNodeOpts::named(format!("{name}/events"));
            opts.node.complete_when_deps_complete = false;
            opts.node.error_when_deps_error = false;
            opts.node.terminal_as_real_input = true;
            opts
        },
    )
}

fn topic_body(producers: Rc<RefCell<Vec<Core>>>) -> impl Fn(&Ctx) + 'static {
    move |ctx: &Ctx| {
        let len = producers.borrow().len();
        for index in 0..len {
            for produced in ctx.batch::<ProducedTopicMessage>(index) {
                let ProducedTopicMessage::Publish(envelope) = produced.as_ref();
                ctx.emit(envelope.clone());
            }
        }
    }
}

pub fn topic_core(bus: &MessageBus, topic: &str) -> Core {
    bus.topic(topic).erased()
}

pub fn dynamic_hub<T: Clone + 'static>(graph: &Graph) -> DynamicHub<T> {
    dynamic_hub_with_options(graph, DynamicHubOptions::default())
}

pub fn dynamic_hub_with_options<T: Clone + 'static>(
    graph: &Graph,
    opts: DynamicHubOptions,
) -> DynamicHub<T> {
    assert!(
        opts.max_topics > 0,
        "dynamicHub: maxTopics must be positive"
    );
    assert!(
        opts.max_topic_length > 0,
        "dynamicHub: maxTopicLength must be positive"
    );
    let initial_topics = unique_hub_topics(&opts.topics, opts.max_topic_length);
    assert!(
        initial_topics.len() <= opts.max_topics,
        "dynamicHub: topics exceed maxTopics"
    );

    let name = Rc::new(opts.name);
    let command_sources = Rc::new(RefCell::new(Vec::new()));
    let command =
        graph.node_opts::<DynamicHubCommand<T>, _>(Vec::new(), dynamic_hub_command_body::<T>(0), {
            let mut opts = GraphNodeOpts::named(format!("{name}/command"));
            opts.node.complete_when_deps_complete = false;
            opts.node.error_when_deps_error = false;
            opts
        });

    let runtime_seed = DynamicHubRuntime {
        topics: initial_topics,
        open: true,
        seq: 0,
        cursor: 0,
    };
    let unknown_topic = opts.unknown_topic;
    let max_topics = opts.max_topics;
    let max_topic_length = opts.max_topic_length;
    let now = opts.now.clone();
    let events = graph.node_opts::<DynamicHubEvent<T>, _>(
        vec![command.erased()],
        move |ctx: &Ctx| {
            let mut runtime = ctx
                .state_get::<DynamicHubRuntime>()
                .map(|state| (*state).clone())
                .unwrap_or_else(|| runtime_seed.clone());
            ctx.state_persist(true);
            for command in ctx.batch::<DynamicHubCommand<T>>(0) {
                for event in reduce_dynamic_hub_command(
                    &mut runtime,
                    (*command).clone(),
                    unknown_topic,
                    max_topics,
                    max_topic_length,
                    now.as_ref(),
                ) {
                    ctx.emit(event);
                }
            }
            ctx.state_set(runtime);
        },
        {
            let mut opts = GraphNodeOpts::named(format!("{name}/events"));
            opts.meta
                .insert("unknownTopic".to_owned(), format!("{:?}", unknown_topic));
            opts.node.complete_when_deps_complete = false;
            opts.node.error_when_deps_error = false;
            opts
        },
    );

    let status = graph.node_opts::<DynamicHubStatus, _>(
        vec![events.erased()],
        move |ctx: &Ctx| {
            for event in ctx.batch::<DynamicHubEvent<T>>(0) {
                ctx.emit(event.status.clone());
            }
        },
        {
            let mut opts = GraphNodeOpts::named(format!("{name}/status"));
            opts.node.complete_when_deps_complete = false;
            opts.node.error_when_deps_error = false;
            opts
        },
    );

    let errors = graph.node_opts::<DynamicHubError<T>, _>(
        vec![events.erased()],
        move |ctx: &Ctx| {
            for event in ctx.batch::<DynamicHubEvent<T>>(0) {
                if event.kind != DynamicHubEventKind::Error {
                    continue;
                }
                if let (Some(error), Some(command)) = (event.error.clone(), event.command.clone()) {
                    ctx.emit(DynamicHubError {
                        topic: event.topic.clone(),
                        error,
                        command,
                        meta: event.meta.clone(),
                    });
                }
            }
        },
        {
            let mut opts = GraphNodeOpts::named(format!("{name}/errors"));
            opts.node.complete_when_deps_complete = false;
            opts.node.error_when_deps_error = false;
            opts
        },
    );

    let dead_letter =
        if opts.dead_letter || opts.unknown_topic == DynamicHubUnknownTopicPolicy::DeadLetter {
            Some(graph.node_opts::<DynamicHubDeadLetter<T>, _>(
                vec![events.erased()],
                move |ctx: &Ctx| {
                    for event in ctx.batch::<DynamicHubEvent<T>>(0) {
                        if event.kind != DynamicHubEventKind::DeadLetter {
                            continue;
                        }
                        if let (Some(reason), Some(command)) =
                            (event.reason.clone(), event.command.clone())
                        {
                            ctx.emit(DynamicHubDeadLetter {
                                topic: event.topic.clone(),
                                reason,
                                command,
                                meta: event.meta.clone(),
                            });
                        }
                    }
                },
                {
                    let mut opts = GraphNodeOpts::named(format!("{name}/deadLetter"));
                    opts.node.complete_when_deps_complete = false;
                    opts.node.error_when_deps_error = false;
                    opts
                },
            ))
        } else {
            None
        };
    let events_retain = Rc::new(DynamicHubRetain::new(
        graph.retain(&events, &format!("{name}.dynamicHub.events")),
    ));

    DynamicHub {
        graph: graph.clone(),
        command,
        events,
        status,
        errors,
        dead_letter,
        name,
        max_topic_length,
        command_sources,
        _events_retain: events_retain,
    }
}

impl<T: Clone + 'static> DynamicHub<T> {
    pub fn create(
        &self,
        topic: impl Into<String>,
        key: Option<String>,
        payload: Option<T>,
    ) -> DynamicHubCommand<T> {
        self.publish_command(DynamicHubCommand::Create {
            topic: topic.into(),
            key,
            payload,
        })
    }

    pub fn delete(
        &self,
        topic: impl Into<String>,
        key: Option<String>,
        payload: Option<T>,
    ) -> DynamicHubCommand<T> {
        self.publish_command(DynamicHubCommand::Delete {
            topic: topic.into(),
            key,
            payload,
        })
    }

    pub fn publish(
        &self,
        topic: impl Into<String>,
        payload: T,
        key: Option<String>,
    ) -> DynamicHubCommand<T> {
        self.publish_command(DynamicHubCommand::Publish {
            topic: topic.into(),
            payload,
            key,
        })
    }

    pub fn subscribe_topic(
        &self,
        topic: impl Into<String>,
        key: Option<String>,
        payload: Option<T>,
    ) -> DynamicHubCommand<T> {
        self.publish_command(DynamicHubCommand::Subscribe {
            topic: topic.into(),
            key,
            payload,
        })
    }

    pub fn close(&self, key: Option<String>, payload: Option<T>) -> DynamicHubCommand<T> {
        self.publish_command(DynamicHubCommand::Close { key, payload })
    }

    fn publish_command(&self, command: DynamicHubCommand<T>) -> DynamicHubCommand<T> {
        self.command.set(command.clone());
        command
    }
}

pub fn from_hub_topic<T: Clone + 'static>(
    hub: &DynamicHub<T>,
    topic: &str,
) -> Node<MessageEnvelope> {
    from_hub_topic_with_name(hub, topic, format!("{}/{topic}/fromHubTopic", hub.name))
}

pub fn from_hub_topic_with_name<T: Clone + 'static>(
    hub: &DynamicHub<T>,
    topic: &str,
    name: impl Into<String>,
) -> Node<MessageEnvelope> {
    assert_topic_key(topic, "fromHubTopic", hub.max_topic_length);
    let topic = topic.to_owned();
    let topic_for_fn = topic.clone();
    hub.graph.node_opts::<MessageEnvelope, _>(
        vec![hub.events.erased()],
        move |ctx: &Ctx| {
            for event in ctx.batch::<DynamicHubEvent<T>>(0) {
                if event.kind != DynamicHubEventKind::Message
                    || event.topic.as_deref() != Some(topic_for_fn.as_str())
                {
                    continue;
                }
                if let Some(payload) = event.payload.clone() {
                    ctx.emit(MessageEnvelope {
                        topic: topic_for_fn.clone(),
                        seq: event.meta.seq,
                        payload: Rc::new(payload),
                        key: event.key.clone(),
                        timestamp_ms: event.meta.timestamp_ms,
                    });
                }
            }
        },
        {
            let mut opts = GraphNodeOpts::named(name);
            opts.meta.insert("topic".to_owned(), topic);
            opts.node.complete_when_deps_complete = false;
            opts.node.error_when_deps_error = false;
            opts
        },
    )
}

pub fn to_hub_topic<T: Clone + 'static>(
    graph: &Graph,
    source: &Node<T>,
    hub: &DynamicHub<T>,
    topic: impl Into<String>,
    name: impl Into<String>,
) -> ToHubTopicBundle<T> {
    let topic = topic.into();
    assert_topic_key(&topic, "toHubTopic", hub.max_topic_length);
    assert!(
        source.erased().same_graph(&hub.command.erased()),
        "toHubTopic: hub and source graph must match"
    );
    assert!(
        !is_reachable_upstream(&source.erased(), &hub.command.erased()),
        "toHubTopic: source already depends on hub command path"
    );

    let topic_for_fn = topic.clone();
    let commands = graph.node_opts::<DynamicHubCommand<T>, _>(
        vec![source.erased()],
        move |ctx: &Ctx| {
            for value in ctx.batch::<T>(0) {
                ctx.emit(DynamicHubCommand::Publish {
                    topic: topic_for_fn.clone(),
                    payload: (*value).clone(),
                    key: None,
                });
            }
        },
        {
            let mut opts = GraphNodeOpts::named(name);
            opts.meta.insert("topic".to_owned(), topic);
            opts.node.complete_when_deps_complete = false;
            opts.node.error_when_deps_error = false;
            opts
        },
    );

    let command_source = commands.erased();
    hub.command_sources
        .borrow_mut()
        .push(command_source.clone());
    let command_sources = hub.command_sources.borrow().clone();
    let command_source_count = command_sources.len();
    let rewire = catch_unwind(AssertUnwindSafe(|| {
        hub.command.replace_deps(
            command_sources,
            dynamic_hub_command_body::<T>(command_source_count),
        );
    }));
    if let Err(panic) = rewire {
        hub.command_sources
            .borrow_mut()
            .retain(|source| !source.ptr_eq(&command_source));
        graph.release_nodes(&[commands.erased()], "toHubTopic failed command wiring");
        resume_unwind(panic);
    }

    ToHubTopicBundle { commands }
}

fn dynamic_hub_command_body<T: Clone + 'static>(
    command_source_count: usize,
) -> impl Fn(&Ctx) + 'static {
    move |ctx: &Ctx| {
        for index in 0..command_source_count {
            for command in ctx.batch::<DynamicHubCommand<T>>(index) {
                ctx.emit((*command).clone());
            }
        }
    }
}

fn reduce_dynamic_hub_command<T: Clone + 'static>(
    runtime: &mut DynamicHubRuntime,
    command: DynamicHubCommand<T>,
    unknown_topic: DynamicHubUnknownTopicPolicy,
    max_topics: usize,
    max_topic_length: usize,
    now: &dyn Fn() -> u64,
) -> Vec<DynamicHubEvent<T>> {
    if let Some(topic) = command.topic() {
        if let Some(error) = validate_topic_key(topic, "dynamicHub", max_topic_length) {
            return vec![hub_error_now(runtime, command, error, now)];
        }
    }
    if !runtime.open && !matches!(command, DynamicHubCommand::Close { .. }) {
        return vec![hub_error_now(
            runtime,
            command,
            "dynamicHub: hub is closed".to_owned(),
            now,
        )];
    }
    match command {
        DynamicHubCommand::Create {
            topic,
            key,
            payload,
        } => {
            let command = DynamicHubCommand::Create {
                topic: topic.clone(),
                key: key.clone(),
                payload: payload.clone(),
            };
            if let Some(error) = can_add_hub_topic(runtime, &topic, max_topics) {
                return vec![hub_error_now(runtime, command, error, now)];
            }
            let timestamp_ms = match hub_timestamp_or_error(runtime, command, now) {
                Ok(timestamp_ms) => timestamp_ms,
                Err(events) => return events,
            };
            add_hub_topic(runtime, &topic);
            vec![hub_event(
                runtime,
                timestamp_ms,
                DynamicHubEventDraft {
                    kind: DynamicHubEventKind::Create,
                    topic: Some(topic),
                    payload,
                    key,
                    error: None,
                    reason: None,
                    command: None,
                },
            )]
        }
        DynamicHubCommand::Delete {
            topic,
            key,
            payload,
        } => {
            let command = DynamicHubCommand::Delete {
                topic: topic.clone(),
                key: key.clone(),
                payload: payload.clone(),
            };
            if !has_hub_topic(runtime, &topic) {
                return unknown_hub_topic(runtime, command, unknown_topic, now);
            }
            let timestamp_ms = match hub_timestamp_or_error(runtime, command, now) {
                Ok(timestamp_ms) => timestamp_ms,
                Err(events) => return events,
            };
            delete_hub_topic(runtime, &topic);
            vec![hub_event(
                runtime,
                timestamp_ms,
                DynamicHubEventDraft {
                    kind: DynamicHubEventKind::Delete,
                    topic: Some(topic),
                    payload,
                    key,
                    error: None,
                    reason: None,
                    command: None,
                },
            )]
        }
        DynamicHubCommand::Publish {
            topic,
            payload,
            key,
        } => {
            let command = DynamicHubCommand::Publish {
                topic: topic.clone(),
                payload: payload.clone(),
                key: key.clone(),
            };
            if !has_hub_topic(runtime, &topic) {
                if unknown_topic == DynamicHubUnknownTopicPolicy::CreateAsFact {
                    if let Some(error) = can_add_hub_topic(runtime, &topic, max_topics) {
                        return vec![hub_error_now(runtime, command, error, now)];
                    }
                    let create_timestamp_ms =
                        match hub_timestamp_or_error(runtime, command.clone(), now) {
                            Ok(timestamp_ms) => timestamp_ms,
                            Err(events) => return events,
                        };
                    let message_timestamp_ms = match hub_timestamp_or_error(runtime, command, now) {
                        Ok(timestamp_ms) => timestamp_ms,
                        Err(events) => return events,
                    };
                    add_hub_topic(runtime, &topic);
                    return vec![
                        hub_event(
                            runtime,
                            create_timestamp_ms,
                            DynamicHubEventDraft {
                                kind: DynamicHubEventKind::Create,
                                topic: Some(topic.clone()),
                                payload: None,
                                key: None,
                                error: None,
                                reason: None,
                                command: None,
                            },
                        ),
                        hub_event(
                            runtime,
                            message_timestamp_ms,
                            DynamicHubEventDraft {
                                kind: DynamicHubEventKind::Message,
                                topic: Some(topic),
                                payload: Some(payload),
                                key,
                                error: None,
                                reason: None,
                                command: None,
                            },
                        ),
                    ];
                }
                return unknown_hub_topic(runtime, command, unknown_topic, now);
            }
            let timestamp_ms = match hub_timestamp_or_error(runtime, command, now) {
                Ok(timestamp_ms) => timestamp_ms,
                Err(events) => return events,
            };
            vec![hub_event(
                runtime,
                timestamp_ms,
                DynamicHubEventDraft {
                    kind: DynamicHubEventKind::Message,
                    topic: Some(topic),
                    payload: Some(payload),
                    key,
                    error: None,
                    reason: None,
                    command: None,
                },
            )]
        }
        DynamicHubCommand::Subscribe {
            topic,
            key,
            payload,
        } => {
            let command = DynamicHubCommand::Subscribe {
                topic: topic.clone(),
                key: key.clone(),
                payload: payload.clone(),
            };
            if !has_hub_topic(runtime, &topic) {
                if unknown_topic == DynamicHubUnknownTopicPolicy::CreateAsFact {
                    if let Some(error) = can_add_hub_topic(runtime, &topic, max_topics) {
                        return vec![hub_error_now(runtime, command, error, now)];
                    }
                    let create_timestamp_ms =
                        match hub_timestamp_or_error(runtime, command.clone(), now) {
                            Ok(timestamp_ms) => timestamp_ms,
                            Err(events) => return events,
                        };
                    let subscribe_timestamp_ms = match hub_timestamp_or_error(runtime, command, now)
                    {
                        Ok(timestamp_ms) => timestamp_ms,
                        Err(events) => return events,
                    };
                    add_hub_topic(runtime, &topic);
                    return vec![
                        hub_event(
                            runtime,
                            create_timestamp_ms,
                            DynamicHubEventDraft {
                                kind: DynamicHubEventKind::Create,
                                topic: Some(topic.clone()),
                                payload: None,
                                key: None,
                                error: None,
                                reason: None,
                                command: None,
                            },
                        ),
                        hub_event(
                            runtime,
                            subscribe_timestamp_ms,
                            DynamicHubEventDraft {
                                kind: DynamicHubEventKind::Subscribe,
                                topic: Some(topic),
                                payload,
                                key,
                                error: None,
                                reason: None,
                                command: None,
                            },
                        ),
                    ];
                }
                return unknown_hub_topic(runtime, command, unknown_topic, now);
            }
            let timestamp_ms = match hub_timestamp_or_error(runtime, command, now) {
                Ok(timestamp_ms) => timestamp_ms,
                Err(events) => return events,
            };
            vec![hub_event(
                runtime,
                timestamp_ms,
                DynamicHubEventDraft {
                    kind: DynamicHubEventKind::Subscribe,
                    topic: Some(topic),
                    payload,
                    key,
                    error: None,
                    reason: None,
                    command: None,
                },
            )]
        }
        DynamicHubCommand::Close { key, payload } => {
            let command = DynamicHubCommand::Close {
                key: key.clone(),
                payload: payload.clone(),
            };
            let timestamp_ms = match hub_timestamp_or_error(runtime, command, now) {
                Ok(timestamp_ms) => timestamp_ms,
                Err(events) => return events,
            };
            runtime.open = false;
            vec![hub_event(
                runtime,
                timestamp_ms,
                DynamicHubEventDraft {
                    kind: DynamicHubEventKind::Close,
                    topic: None,
                    payload,
                    key,
                    error: None,
                    reason: None,
                    command: None,
                },
            )]
        }
    }
}

impl<T> DynamicHubCommand<T> {
    fn topic(&self) -> Option<&str> {
        match self {
            Self::Create { topic, .. }
            | Self::Delete { topic, .. }
            | Self::Publish { topic, .. }
            | Self::Subscribe { topic, .. } => Some(topic),
            Self::Close { .. } => None,
        }
    }
}

struct DynamicHubEventDraft<T> {
    kind: DynamicHubEventKind,
    topic: Option<String>,
    payload: Option<T>,
    key: Option<String>,
    error: Option<String>,
    reason: Option<String>,
    command: Option<DynamicHubCommand<T>>,
}

fn hub_event<T>(
    runtime: &mut DynamicHubRuntime,
    timestamp_ms: u64,
    draft: DynamicHubEventDraft<T>,
) -> DynamicHubEvent<T> {
    let meta = next_hub_metadata(runtime, timestamp_ms);
    DynamicHubEvent {
        kind: draft.kind,
        topic: draft.topic,
        payload: draft.payload,
        key: draft.key,
        error: draft.error,
        reason: draft.reason,
        command: draft.command,
        status: snapshot_hub_status(runtime, &meta, draft.kind),
        meta,
    }
}

fn hub_error<T>(
    runtime: &mut DynamicHubRuntime,
    command: DynamicHubCommand<T>,
    error: String,
    timestamp_ms: u64,
) -> DynamicHubEvent<T> {
    let topic = command.topic().map(str::to_owned);
    hub_event(
        runtime,
        timestamp_ms,
        DynamicHubEventDraft {
            kind: DynamicHubEventKind::Error,
            topic,
            payload: None,
            key: None,
            error: Some(error),
            reason: None,
            command: Some(command),
        },
    )
}

fn hub_error_now<T>(
    runtime: &mut DynamicHubRuntime,
    command: DynamicHubCommand<T>,
    error: String,
    now: &dyn Fn() -> u64,
) -> DynamicHubEvent<T> {
    let (error, timestamp_ms) = match hub_timestamp(now) {
        Ok(timestamp_ms) => (error, timestamp_ms),
        Err(clock_error) => (format!("{error}; {clock_error}"), 0),
    };
    hub_error(runtime, command, error, timestamp_ms)
}

fn hub_timestamp_or_error<T>(
    runtime: &mut DynamicHubRuntime,
    command: DynamicHubCommand<T>,
    now: &dyn Fn() -> u64,
) -> Result<u64, Vec<DynamicHubEvent<T>>> {
    hub_timestamp(now).map_err(|error| vec![hub_error(runtime, command, error, 0)])
}

fn hub_timestamp(now: &dyn Fn() -> u64) -> Result<u64, String> {
    catch_unwind(AssertUnwindSafe(now)).map_err(|panic| {
        format!(
            "dynamicHub: metadata clock panicked: {}",
            panic_message(&panic)
        )
    })
}

fn panic_message(panic: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic".to_owned()
    }
}

fn unknown_hub_topic<T: Clone + 'static>(
    runtime: &mut DynamicHubRuntime,
    command: DynamicHubCommand<T>,
    policy: DynamicHubUnknownTopicPolicy,
    now: &dyn Fn() -> u64,
) -> Vec<DynamicHubEvent<T>> {
    let topic = command.topic().map(str::to_owned);
    let reason = format!(
        "dynamicHub: unknown topic '{}'",
        topic.as_deref().unwrap_or("")
    );
    match policy {
        DynamicHubUnknownTopicPolicy::Drop => Vec::new(),
        DynamicHubUnknownTopicPolicy::DeadLetter => vec![hub_event(
            runtime,
            match hub_timestamp(now) {
                Ok(timestamp_ms) => timestamp_ms,
                Err(error) => return vec![hub_error(runtime, command, error, 0)],
            },
            DynamicHubEventDraft {
                kind: DynamicHubEventKind::DeadLetter,
                topic,
                payload: None,
                key: None,
                error: None,
                reason: Some(reason),
                command: Some(command),
            },
        )],
        DynamicHubUnknownTopicPolicy::Error | DynamicHubUnknownTopicPolicy::CreateAsFact => {
            vec![hub_error_now(runtime, command, reason, now)]
        }
    }
}

fn next_hub_metadata(runtime: &mut DynamicHubRuntime, timestamp_ms: u64) -> DynamicHubMetadata {
    runtime.seq += 1;
    runtime.cursor = runtime.seq;
    DynamicHubMetadata {
        seq: runtime.seq,
        cursor: runtime.cursor,
        timestamp_ms,
    }
}

fn snapshot_hub_status(
    runtime: &DynamicHubRuntime,
    meta: &DynamicHubMetadata,
    kind: DynamicHubEventKind,
) -> DynamicHubStatus {
    let mut topics = runtime.topics.clone();
    topics.sort();
    DynamicHubStatus {
        open: runtime.open,
        topics,
        seq: meta.seq,
        cursor: meta.cursor,
        last_event_kind: Some(kind),
    }
}

fn unique_hub_topics(topics: &[String], max_topic_length: usize) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut unique = Vec::with_capacity(topics.len());
    for topic in topics {
        assert_topic_key(topic, "dynamicHub", max_topic_length);
        assert!(seen.insert(topic.clone()), "dynamicHub: duplicate topic");
        unique.push(topic.clone());
    }
    unique.sort();
    unique
}

fn assert_topic_key(topic: &str, owner: &str, max_topic_length: usize) {
    if let Some(error) = validate_topic_key(topic, owner, max_topic_length) {
        panic!("{error}");
    }
}

fn validate_topic_key(topic: &str, owner: &str, max_topic_length: usize) -> Option<String> {
    if topic.is_empty() {
        return Some(format!("{owner}: topic must be a non-empty string"));
    }
    if topic.len() > max_topic_length {
        return Some(format!("{owner}: topic exceeds maxTopicLength"));
    }
    None
}

fn has_hub_topic(runtime: &DynamicHubRuntime, topic: &str) -> bool {
    runtime.topics.iter().any(|existing| existing == topic)
}

fn add_hub_topic(runtime: &mut DynamicHubRuntime, topic: &str) {
    if !has_hub_topic(runtime, topic) {
        runtime.topics.push(topic.to_owned());
    }
}

fn delete_hub_topic(runtime: &mut DynamicHubRuntime, topic: &str) {
    runtime.topics.retain(|existing| existing != topic);
}

fn can_add_hub_topic(
    runtime: &DynamicHubRuntime,
    topic: &str,
    max_topics: usize,
) -> Option<String> {
    if has_hub_topic(runtime, topic) {
        return None;
    }
    if runtime.topics.len() >= max_topics {
        return Some("dynamicHub: topic count exceeds maxTopics".to_owned());
    }
    None
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
mod tests {
    use super::*;
    use crate::graph::{graph, GraphNodeOpts};
    use serde_json::json;

    #[test]
    fn standard_topic_constants_are_stable_d159_vocabulary() {
        assert_eq!(PROMPTS_TOPIC, "prompts");
        assert_eq!(RESPONSES_TOPIC, "responses");
        assert_eq!(INJECTIONS_TOPIC, "injections");
        assert_eq!(DEFERRED_TOPIC, "deferred");
        assert_eq!(SPAWNS_TOPIC, "spawns");
        assert_eq!(CONTEXT_TOPIC, "context");
        assert_eq!(TODOS_TOPIC, "todos");
        assert_eq!(
            STANDARD_TOPICS,
            [
                "prompts",
                "responses",
                "injections",
                "deferred",
                "spawns",
                "context",
                "todos"
            ]
        );
    }

    #[test]
    fn topic_message_is_passive_metadata_plus_payload() {
        let message = TopicMessage {
            id: "msg-1".to_owned(),
            schema: Some(JsonSchema {
                schema_type: Some(JsonSchemaTypeSpec::Single(JsonSchemaType::Object)),
                title: Some("Prompt".to_owned()),
                ..JsonSchema::default()
            }),
            expires_at: Some("2026-06-09T00:00:00Z".to_owned()),
            correlation_id: Some("corr-1".to_owned()),
            payload: json!({ "prompt": "hello" }),
        };

        assert_eq!(message.id, "msg-1");
        assert_eq!(message.expires_at.as_deref(), Some("2026-06-09T00:00:00Z"));
        assert_eq!(message.correlation_id.as_deref(), Some("corr-1"));
        assert_eq!(message.payload["prompt"], "hello");

        let encoded = serde_json::to_value(&message).expect("topic message encodes");
        assert_eq!(encoded["expiresAt"], "2026-06-09T00:00:00Z");
        assert_eq!(encoded["correlationId"], "corr-1");
        assert_eq!(encoded["schema"]["type"], "object");
    }

    #[test]
    fn json_schema_representation_round_trips_camel_case_fields() {
        let schema = JsonSchema {
            schema_type: Some(JsonSchemaTypeSpec::Single(JsonSchemaType::Object)),
            properties: Some(BTreeMap::from([(
                "id".to_owned(),
                JsonSchema {
                    schema_type: Some(JsonSchemaTypeSpec::Single(JsonSchemaType::String)),
                    ..JsonSchema::default()
                },
            )])),
            required: Some(vec!["id".to_owned()]),
            additional_properties: Some(JsonSchemaAdditionalProperties::Bool(false)),
            description: Some("payload shape".to_owned()),
            ..JsonSchema::default()
        };

        let encoded = serde_json::to_value(&schema).expect("schema encodes");
        assert_eq!(encoded["type"], "object");
        assert_eq!(encoded["properties"]["id"]["type"], "string");
        assert_eq!(encoded["required"], json!(["id"]));
        assert_eq!(encoded["additionalProperties"], false);
        assert_eq!(encoded["description"], "payload shape");
    }

    #[test]
    fn json_schema_deserialization_rejects_unsupported_keywords() {
        let error = serde_json::from_value::<JsonSchema>(json!({
            "type": "string",
            "minLength": 2
        }))
        .expect_err("unsupported schema keywords fail closed");

        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn explicit_json_schema_validation_accepts_matching_payloads() {
        let schema = JsonSchema {
            schema_type: Some(JsonSchemaTypeSpec::Single(JsonSchemaType::Object)),
            properties: Some(BTreeMap::from([
                (
                    "id".to_owned(),
                    JsonSchema {
                        schema_type: Some(JsonSchemaTypeSpec::Single(JsonSchemaType::String)),
                        ..JsonSchema::default()
                    },
                ),
                (
                    "attempts".to_owned(),
                    JsonSchema {
                        schema_type: Some(JsonSchemaTypeSpec::Single(JsonSchemaType::Integer)),
                        ..JsonSchema::default()
                    },
                ),
                (
                    "tags".to_owned(),
                    JsonSchema {
                        schema_type: Some(JsonSchemaTypeSpec::Single(JsonSchemaType::Array)),
                        items: Some(JsonSchemaItems::Schema(Box::new(JsonSchema {
                            schema_type: Some(JsonSchemaTypeSpec::Single(JsonSchemaType::String)),
                            ..JsonSchema::default()
                        }))),
                        ..JsonSchema::default()
                    },
                ),
            ])),
            required: Some(vec!["id".to_owned(), "attempts".to_owned()]),
            additional_properties: Some(JsonSchemaAdditionalProperties::Bool(false)),
            ..JsonSchema::default()
        };

        let payload = json!({ "id": "job-1", "attempts": 2, "tags": ["fast", "safe"] });

        validate_json_schema(&schema, &payload).expect("payload matches schema");
        assert!(is_json_schema_valid(&schema, &payload));

        let integer_schema = JsonSchema {
            schema_type: Some(JsonSchemaTypeSpec::Single(JsonSchemaType::Integer)),
            ..JsonSchema::default()
        };
        validate_json_schema(&integer_schema, &json!(1.0)).expect("1.0 is an integer JSON number");
    }

    #[test]
    fn explicit_json_schema_validation_accepts_finite_recursive_refs() {
        let node_schema = JsonSchema {
            schema_type: Some(JsonSchemaTypeSpec::Single(JsonSchemaType::Object)),
            properties: Some(BTreeMap::from([
                (
                    "value".to_owned(),
                    JsonSchema {
                        schema_type: Some(JsonSchemaTypeSpec::Single(JsonSchemaType::String)),
                        ..JsonSchema::default()
                    },
                ),
                (
                    "child".to_owned(),
                    JsonSchema {
                        ref_path: Some("#/definitions/Node".to_owned()),
                        ..JsonSchema::default()
                    },
                ),
            ])),
            required: Some(vec!["value".to_owned()]),
            additional_properties: Some(JsonSchemaAdditionalProperties::Bool(false)),
            ..JsonSchema::default()
        };
        let schema = JsonSchema {
            ref_path: Some("#/definitions/Node".to_owned()),
            definitions: Some(BTreeMap::from([("Node".to_owned(), node_schema)])),
            ..JsonSchema::default()
        };

        validate_json_schema(
            &schema,
            &json!({
                "value": "root",
                "child": { "value": "leaf" }
            }),
        )
        .expect("finite recursive payload validates");
    }

    #[test]
    fn explicit_json_schema_validation_reports_failures_without_side_effects() {
        let schema = JsonSchema {
            schema_type: Some(JsonSchemaTypeSpec::Single(JsonSchemaType::Object)),
            properties: Some(BTreeMap::from([(
                "id".to_owned(),
                JsonSchema {
                    schema_type: Some(JsonSchemaTypeSpec::Single(JsonSchemaType::String)),
                    ..JsonSchema::default()
                },
            )])),
            required: Some(vec!["id".to_owned()]),
            additional_properties: Some(JsonSchemaAdditionalProperties::Bool(false)),
            ..JsonSchema::default()
        };

        let missing = validate_json_schema(&schema, &json!({})).expect_err("missing id fails");
        assert_eq!(missing.path, "$");
        assert_eq!(missing.message, "missing required property 'id'");

        let wrong_type =
            validate_json_schema(&schema, &json!({ "id": 42 })).expect_err("wrong type fails");
        assert_eq!(wrong_type.path, "$.id");
        assert_eq!(wrong_type.message, "expected string, got number");

        let extra = validate_json_schema(&schema, &json!({ "id": "a", "extra": true }))
            .expect_err("extra property fails");
        assert_eq!(extra.path, "$");
        assert_eq!(extra.message, "unexpected additional property 'extra'");

        let no_declared_properties = JsonSchema {
            schema_type: Some(JsonSchemaTypeSpec::Single(JsonSchemaType::Object)),
            additional_properties: Some(JsonSchemaAdditionalProperties::Bool(false)),
            ..JsonSchema::default()
        };
        let extra_without_properties =
            validate_json_schema(&no_declared_properties, &json!({ "extra": true }))
                .expect_err("additionalProperties applies without properties");
        assert_eq!(extra_without_properties.path, "$");
        assert_eq!(
            extra_without_properties.message,
            "unexpected additional property 'extra'"
        );
    }

    #[test]
    fn topic_message_payload_validation_is_explicit_and_schema_optional() {
        let unchecked = TopicMessage {
            id: "msg-1".to_owned(),
            schema: None,
            expires_at: None,
            correlation_id: None,
            payload: json!({ "anything": true }),
        };
        validate_topic_message_payload(&unchecked).expect("no schema means no validation");

        let checked = TopicMessage {
            id: "msg-2".to_owned(),
            schema: Some(JsonSchema {
                schema_type: Some(JsonSchemaTypeSpec::Single(JsonSchemaType::String)),
                ..JsonSchema::default()
            }),
            expires_at: None,
            correlation_id: None,
            payload: json!(10),
        };

        let error = validate_topic_message_payload(&checked).expect_err("schema is explicit");
        assert_eq!(error.path, "$");
        assert_eq!(error.message, "expected string, got number");
    }

    #[test]
    fn message_bus_topic_starts_sentinel_then_publishes_envelope() {
        let g = graph();
        let bus = MessageBus::with_name(&g, ["orders"], "bus", || 10);
        let topic = from_topic(&bus, "orders");

        assert!(topic.cache().is_none());
        let envelope = bus.publish("orders", 7_i32, Some("o1".to_owned()));

        assert_eq!(envelope.topic, "orders");
        assert_eq!(envelope.seq, 1);
        assert_eq!(envelope.key.as_deref(), Some("o1"));
        assert_eq!(envelope.timestamp_ms, 10);
        assert_eq!(*topic.cache().unwrap().payload::<i32>().unwrap(), 7);
    }

    #[test]
    fn to_topic_is_declared_graph_topology() {
        let g = graph();
        let bus = MessageBus::with_name(&g, ["orders"], "bus", || 20);
        let source = g.state_empty_opts::<i32>(GraphNodeOpts::named("source"));
        let events = to_topic(&g, &source, bus.clone(), "orders", "orders/out");
        let _sub = events.subscribe(|_| {});

        source.set(9);

        assert!(matches!(
            events.cache().unwrap(),
            MessageBusEvent::Publish { seq: 1, .. }
        ));
        let snap = g.describe();
        assert!(snap
            .edges
            .iter()
            .any(|edge| edge.from == "source" && edge.to == "orders/out"));
        assert!(snap
            .edges
            .iter()
            .any(|edge| edge.from == "orders/out" && edge.to == "bus/orders"));
        assert!(snap
            .edges
            .iter()
            .any(|edge| edge.from == "orders/out" && edge.to == "orders/out/events"));
    }

    #[test]
    fn dynamic_hub_is_facts_dynamic_and_topology_static() {
        let g = graph();
        let hub = dynamic_hub_with_options::<String>(
            &g,
            DynamicHubOptions::named("hub")
                .with_topics(["orders"])
                .with_now(|| 100),
        );
        let orders = from_hub_topic_with_name(&hub, "orders", "orders/in");
        let _orders = orders.subscribe(|_| {});
        let _status = hub.status.subscribe(|_| {});
        let _errors = hub.errors.subscribe(|_| {});

        hub.publish("orders", "o1".to_owned(), Some("k1".to_owned()));

        let envelope = orders.cache().unwrap();
        assert_eq!(envelope.topic, "orders");
        assert_eq!(envelope.seq, 1);
        assert_eq!(envelope.key.as_deref(), Some("k1"));
        assert_eq!(envelope.timestamp_ms, 100);
        assert_eq!(&*envelope.payload::<String>().unwrap(), "o1");
        let status = hub.status.cache().unwrap();
        assert_eq!(status.topics, vec!["orders"]);
        assert_eq!(status.seq, 1);
        assert_eq!(status.cursor, 1);
        assert_eq!(status.last_event_kind, Some(DynamicHubEventKind::Message));
        assert!(hub.errors.cache().is_none());

        let snap = g.describe();
        for id in [
            "hub/command",
            "hub/events",
            "hub/status",
            "hub/errors",
            "orders/in",
        ] {
            assert!(snap.nodes.iter().any(|node| node.id == id), "{id}");
        }
        assert!(snap
            .edges
            .iter()
            .any(|edge| edge.from == "hub/command" && edge.to == "hub/events"));
        assert!(snap
            .edges
            .iter()
            .any(|edge| edge.from == "hub/events" && edge.to == "hub/status"));
        assert!(snap
            .edges
            .iter()
            .any(|edge| edge.from == "hub/events" && edge.to == "hub/errors"));
        assert!(snap
            .edges
            .iter()
            .any(|edge| edge.from == "hub/events" && edge.to == "orders/in"));
    }

    #[test]
    fn dynamic_hub_unknown_topic_defaults_to_graph_visible_error() {
        let g = graph();
        let hub = dynamic_hub_with_options::<String>(
            &g,
            DynamicHubOptions::named("hub").with_now(|| 200),
        );
        let _errors = hub.errors.subscribe(|_| {});
        let _status = hub.status.subscribe(|_| {});

        hub.publish("missing", "payload".to_owned(), None);

        let error = hub.errors.cache().unwrap();
        assert_eq!(error.topic.as_deref(), Some("missing"));
        assert_eq!(error.error, "dynamicHub: unknown topic 'missing'");
        assert_eq!(error.meta.seq, 1);
        assert_eq!(error.meta.cursor, 1);
        assert_eq!(hub.status.cache().unwrap().topics, Vec::<String>::new());
    }

    #[test]
    fn dynamic_hub_reduces_helper_commands_before_external_observation() {
        let g = graph();
        let hub = dynamic_hub_with_options::<String>(
            &g,
            DynamicHubOptions::named("hub").with_now(|| 250),
        );

        hub.create("orders", None, None);
        hub.publish("orders", "o1".to_owned(), None);

        let orders = from_hub_topic_with_name(&hub, "orders", "orders/in");
        let _orders = orders.subscribe(|_| {});
        let _status = hub.status.subscribe(|_| {});

        let envelope = orders.cache().unwrap();
        assert_eq!(envelope.topic, "orders");
        assert_eq!(envelope.seq, 2);
        assert_eq!(&*envelope.payload::<String>().unwrap(), "o1");
        let status = hub.status.cache().unwrap();
        assert_eq!(status.topics, vec!["orders"]);
        assert_eq!(status.seq, 2);
        assert_eq!(status.cursor, 2);
        assert_eq!(status.last_event_kind, Some(DynamicHubEventKind::Message));
    }

    #[test]
    fn dynamic_hub_reduces_to_topic_commands_before_external_observation() {
        let g = graph();
        let hub = dynamic_hub_with_options::<String>(
            &g,
            DynamicHubOptions::named("hub")
                .with_unknown_topic(DynamicHubUnknownTopicPolicy::CreateAsFact)
                .with_now(|| 275),
        );
        let source = g.state_empty_opts::<String>(GraphNodeOpts::named("source"));
        let _bundle = to_hub_topic(&g, &source, &hub, "orders", "orders/out");

        source.set("o1".to_owned());

        let orders = from_hub_topic_with_name(&hub, "orders", "orders/in");
        let _orders = orders.subscribe(|_| {});
        let _status = hub.status.subscribe(|_| {});

        let envelope = orders.cache().unwrap();
        assert_eq!(envelope.topic, "orders");
        assert_eq!(envelope.seq, 2);
        assert_eq!(&*envelope.payload::<String>().unwrap(), "o1");
        let status = hub.status.cache().unwrap();
        assert_eq!(status.topics, vec!["orders"]);
        assert_eq!(status.seq, 2);
        assert_eq!(status.cursor, 2);
        assert_eq!(status.last_event_kind, Some(DynamicHubEventKind::Message));
    }

    #[test]
    fn dynamic_hub_clock_panic_is_graph_visible_error_without_topic_mutation() {
        let g = graph();
        let hub = dynamic_hub_with_options::<String>(
            &g,
            DynamicHubOptions::named("hub")
                .with_unknown_topic(DynamicHubUnknownTopicPolicy::CreateAsFact)
                .with_now(|| panic!("clock boom")),
        );
        let _errors = hub.errors.subscribe(|_| {});
        let _status = hub.status.subscribe(|_| {});

        hub.publish("orders", "o1".to_owned(), None);

        let error = hub.errors.cache().unwrap();
        assert_eq!(error.topic.as_deref(), Some("orders"));
        assert_eq!(
            error.error,
            "dynamicHub: metadata clock panicked: clock boom"
        );
        assert_eq!(error.meta.seq, 1);
        assert_eq!(error.meta.cursor, 1);
        assert_eq!(error.meta.timestamp_ms, 0);
        let status = hub.status.cache().unwrap();
        assert_eq!(status.topics, Vec::<String>::new());
        assert_eq!(status.last_event_kind, Some(DynamicHubEventKind::Error));
    }

    #[test]
    fn dynamic_hub_dead_letter_policy_is_visible_without_topic_nodes() {
        let g = graph();
        let hub = dynamic_hub_with_options::<String>(
            &g,
            DynamicHubOptions::named("hub")
                .with_unknown_topic(DynamicHubUnknownTopicPolicy::DeadLetter)
                .with_now(|| 300),
        );
        let dead_letter = hub.dead_letter.clone().expect("dead-letter node");
        let _dead = dead_letter.subscribe(|_| {});

        hub.publish("missing", "payload".to_owned(), None);

        let letter = dead_letter.cache().unwrap();
        assert_eq!(letter.topic.as_deref(), Some("missing"));
        assert_eq!(letter.reason, "dynamicHub: unknown topic 'missing'");
        assert_eq!(letter.meta.seq, 1);
        assert!(g
            .describe()
            .nodes
            .iter()
            .any(|node| node.id == "hub/deadLetter"));
    }

    #[test]
    fn to_hub_topic_is_static_command_helper_and_create_as_fact_bounds_runtime_topics() {
        let g = graph();
        let hub = dynamic_hub_with_options::<String>(
            &g,
            DynamicHubOptions::named("hub")
                .with_unknown_topic(DynamicHubUnknownTopicPolicy::CreateAsFact)
                .with_max_topics(2)
                .with_now(|| 400),
        );
        let source = g.state_empty_opts::<String>(GraphNodeOpts::named("source"));
        let orders = from_hub_topic_with_name(&hub, "orders", "orders/in");
        let _orders = orders.subscribe(|_| {});
        let _status = hub.status.subscribe(|_| {});
        let bundle = to_hub_topic(&g, &source, &hub, "orders", "orders/out");
        let _commands = bundle.commands.subscribe(|_| {});

        source.set("o2".to_owned());

        let envelope = orders.cache().unwrap();
        assert_eq!(envelope.topic, "orders");
        assert_eq!(envelope.seq, 2);
        assert_eq!(&*envelope.payload::<String>().unwrap(), "o2");
        let status = hub.status.cache().unwrap();
        assert_eq!(status.topics, vec!["orders"]);
        assert_eq!(status.seq, 2);
        assert_eq!(status.cursor, 2);
        assert_eq!(status.last_event_kind, Some(DynamicHubEventKind::Message));

        let snap = g.describe();
        assert!(snap
            .edges
            .iter()
            .any(|edge| edge.from == "source" && edge.to == "orders/out"));
        assert!(snap
            .edges
            .iter()
            .any(|edge| edge.from == "orders/out" && edge.to == "hub/command"));
        assert!(snap
            .edges
            .iter()
            .any(|edge| edge.from == "hub/events" && edge.to == "orders/in"));

        hub.publish("audit", "a1".to_owned(), None);
        hub.publish("overflow", "x".to_owned(), None);
        assert_eq!(
            hub.status.cache().unwrap().last_event_kind,
            Some(DynamicHubEventKind::Error)
        );
        assert_eq!(hub.status.cache().unwrap().topics, vec!["audit", "orders"]);
    }

    #[test]
    #[should_panic(expected = "dynamicHub: topic exceeds maxTopicLength")]
    fn dynamic_hub_validates_initial_topic_bounds() {
        let g = graph();
        let _hub = dynamic_hub_with_options::<String>(
            &g,
            DynamicHubOptions::named("hub")
                .with_topics(["toolong"])
                .with_max_topic_length(3),
        );
    }
}
