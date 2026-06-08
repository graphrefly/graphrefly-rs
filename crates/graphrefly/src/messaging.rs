//! Graph-visible message bus application infrastructure (D132).
//!
//! Topics are declared up front and represented as graph-owned fan-in nodes.
//! `publish` is boundary sugar that writes an ordinary DATA fact; `to_topic`
//! wires an explicit producer node into the topic so the graph topology remains
//! inspectable (D39/D132).

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use crate::ctx::{Ctx, DepTerminal};
use crate::graph::{Graph, GraphNodeOpts};
use crate::node::{Core, Node};
use crate::protocol::AnyValue;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{graph, GraphNodeOpts};

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
}
