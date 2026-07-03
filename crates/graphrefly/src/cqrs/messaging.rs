//! Optional CQRS-over-messageBus recipe (D350/D351/D353).
//!
//! Retained delivery is lowered to CQRS command facts. Ack commands are emitted
//! only after a graph-visible accepted/rejected status or lowering issue exists;
//! receiving a retained message is never itself an ack.

use std::collections::BTreeMap;
use std::rc::Rc;

use crate::cqrs::{CqrsCommand, CqrsEvent, CqrsStatus, CqrsStatusState};
use crate::ctx::Ctx;
use crate::graph::{Graph, GraphNodeOpts};
use crate::identity::{canonical_tuple_key, compound_tuple_key};
use crate::messaging::{DataIssue, MessageBusAvailablePage, MessageBusCommand, MessageEnvelope};
use crate::node::Node;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageBusDelivery {
    pub topic: String,
    pub seq: u64,
    pub subscription_id: String,
    pub command_id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CqrsDeliveredCommand<TCommand> {
    pub command: CqrsCommand<TCommand>,
    pub delivery: MessageBusDelivery,
}

pub type CqrsMessageCommandFn<TPayload, TCommand> =
    Rc<dyn Fn(&MessageEnvelope<TPayload>, &MessageBusDelivery) -> Option<CqrsCommand<TCommand>>>;

#[derive(Clone)]
pub struct CqrsMessagingPolicy<TPayload, TCommand> {
    pub command: CqrsMessageCommandFn<TPayload, TCommand>,
    pub ack_rejected: bool,
    pub outbox_topic: Option<String>,
}

impl<TPayload, TCommand> CqrsMessagingPolicy<TPayload, TCommand> {
    pub fn new(
        command: impl Fn(&MessageEnvelope<TPayload>, &MessageBusDelivery) -> Option<CqrsCommand<TCommand>>
            + 'static,
    ) -> Self {
        Self {
            command: Rc::new(command),
            ack_rejected: true,
            outbox_topic: None,
        }
    }

    pub fn ack_rejected(mut self, ack_rejected: bool) -> Self {
        self.ack_rejected = ack_rejected;
        self
    }

    pub fn with_outbox_topic(mut self, topic: impl Into<String>) -> Self {
        self.outbox_topic = Some(topic.into());
        self
    }
}

#[derive(Clone)]
pub struct CqrsMessagingRecipeOptions<TPayload, TCommand, TEvent> {
    pub name: String,
    pub deliveries: Node<MessageBusAvailablePage<TPayload>>,
    pub status: Node<CqrsStatus>,
    pub events: Option<Node<CqrsEvent<TEvent>>>,
    pub policy: CqrsMessagingPolicy<TPayload, TCommand>,
}

impl<TPayload, TCommand, TEvent> CqrsMessagingRecipeOptions<TPayload, TCommand, TEvent> {
    pub fn new(
        deliveries: Node<MessageBusAvailablePage<TPayload>>,
        status: Node<CqrsStatus>,
        policy: CqrsMessagingPolicy<TPayload, TCommand>,
    ) -> Self {
        Self {
            name: "cqrsMessaging".to_owned(),
            deliveries,
            status,
            events: None,
            policy,
        }
    }

    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    pub fn with_events(mut self, events: Node<CqrsEvent<TEvent>>) -> Self {
        self.events = Some(events);
        self
    }
}

#[derive(Clone)]
pub struct CqrsMessagingRecipeBundle<TCommand, TEvent> {
    pub delivered_commands: Node<CqrsDeliveredCommand<TCommand>>,
    pub commands: Node<CqrsCommand<TCommand>>,
    pub ack_commands: Node<MessageBusCommand<TCommand>>,
    pub outbox_commands: Option<Node<MessageBusCommand<CqrsEvent<TEvent>>>>,
    pub issues: Node<DataIssue>,
}

#[derive(Clone)]
enum CqrsMessagingFact<TCommand> {
    Command(CqrsDeliveredCommand<TCommand>),
    Issue(DataIssue),
}

#[derive(Clone, Default)]
struct AckState {
    deliveries: BTreeMap<String, Vec<MessageBusDelivery>>,
}

pub fn cqrs_messaging_recipe<
    TPayload: Clone + 'static,
    TCommand: Clone + 'static,
    TEvent: Clone + 'static,
>(
    graph: &Graph,
    opts: CqrsMessagingRecipeOptions<TPayload, TCommand, TEvent>,
) -> CqrsMessagingRecipeBundle<TCommand, TEvent> {
    let name = opts.name.clone();
    let policy = opts.policy.clone();
    let runtime = graph.node_opts::<CqrsMessagingFact<TCommand>, _>(
        vec![opts.deliveries.erased()],
        move |ctx| {
            for page in ctx.batch::<MessageBusAvailablePage<TPayload>>(0) {
                for message in &page.messages {
                    let delivery = message_delivery(page.as_ref(), message);
                    if let Some(command) = (policy.command)(message, &delivery) {
                        ctx.emit(CqrsMessagingFact::Command(CqrsDeliveredCommand {
                            command,
                            delivery,
                        }));
                    } else {
                        ctx.emit(CqrsMessagingFact::<TCommand>::Issue(message_issue(
                            &delivery,
                        )));
                    }
                }
            }
        },
        GraphNodeOpts::named(format!("{name}/runtime")),
    );
    let delivered_commands = project(
        graph,
        &runtime,
        format!("{name}/deliveredCommands"),
        |fact| match fact {
            CqrsMessagingFact::Command(delivered) => Some(delivered.clone()),
            CqrsMessagingFact::Issue(_) => None,
        },
    );
    let commands = project(
        graph,
        &runtime,
        format!("{name}/commands"),
        |fact| match fact {
            CqrsMessagingFact::Command(delivered) => Some(delivered.command.clone()),
            CqrsMessagingFact::Issue(_) => None,
        },
    );
    let issues = project(
        graph,
        &runtime,
        format!("{name}/issues"),
        |fact| match fact {
            CqrsMessagingFact::Issue(issue) => Some(issue.clone()),
            CqrsMessagingFact::Command(_) => None,
        },
    );
    let ack_commands = cqrs_message_ack_commands(
        graph,
        CqrsMessageAckOptions {
            name: format!("{name}/ackCommands"),
            delivered_commands: delivered_commands.clone(),
            status: opts.status,
            issues: Some(issues.clone()),
            ack_rejected: opts.policy.ack_rejected,
        },
    );
    let outbox_commands = opts.events.and_then(|events| {
        opts.policy.outbox_topic.map(|topic| {
            cqrs_event_outbox_commands(graph, events, topic, format!("{name}/outboxCommands"))
        })
    });
    CqrsMessagingRecipeBundle {
        delivered_commands,
        commands,
        ack_commands,
        outbox_commands,
        issues,
    }
}

pub struct CqrsMessageAckOptions<TCommand> {
    pub name: String,
    pub delivered_commands: Node<CqrsDeliveredCommand<TCommand>>,
    pub status: Node<CqrsStatus>,
    pub issues: Option<Node<DataIssue>>,
    pub ack_rejected: bool,
}

pub fn cqrs_message_ack_commands<TCommand: Clone + 'static>(
    graph: &Graph,
    opts: CqrsMessageAckOptions<TCommand>,
) -> Node<MessageBusCommand<TCommand>> {
    let mut deps = vec![opts.delivered_commands.erased(), opts.status.erased()];
    if let Some(issues) = &opts.issues {
        deps.push(issues.erased());
    }
    graph.node_opts::<MessageBusCommand<TCommand>, _>(
        deps,
        move |ctx| {
            let mut state = ctx
                .state_get::<AckState>()
                .map(|state| (*state).clone())
                .unwrap_or_default();
            for delivered in ctx.batch::<CqrsDeliveredCommand<TCommand>>(0) {
                state
                    .deliveries
                    .entry(delivered.command.id.clone())
                    .or_default()
                    .push(delivered.delivery.clone());
            }
            for status in ctx.batch::<CqrsStatus>(1) {
                if status.state == CqrsStatusState::Rejected && !opts.ack_rejected {
                    continue;
                }
                let Some(command_id) = &status.command_id else {
                    continue;
                };
                if let Some(delivery) = shift_delivery(&mut state.deliveries, command_id) {
                    ctx.emit(ack_command::<TCommand>(
                        &delivery,
                        cqrs_ack_id(&delivery, "status-ack"),
                    ));
                }
            }
            if opts.issues.is_some() {
                for issue in ctx.batch::<DataIssue>(2) {
                    if let Some(delivery) = issue_delivery(&issue) {
                        ctx.emit(ack_command::<TCommand>(
                            &delivery,
                            cqrs_ack_id(&delivery, "issue-ack"),
                        ));
                    }
                }
            }
            ctx.state_set(state);
            ctx.state_persist(true);
        },
        GraphNodeOpts::named(opts.name),
    )
}

fn cqrs_ack_id(delivery: &MessageBusDelivery, reason: &str) -> String {
    compound_tuple_key(
        "cqrs-message-ack",
        &[
            &delivery.topic,
            &delivery.subscription_id,
            &delivery.seq.to_string(),
            reason,
        ],
    )
}

pub fn cqrs_event_outbox_commands<TEvent: Clone + 'static>(
    graph: &Graph,
    events: Node<CqrsEvent<TEvent>>,
    topic: impl Into<String>,
    name: impl Into<String>,
) -> Node<MessageBusCommand<CqrsEvent<TEvent>>> {
    let topic = topic.into();
    graph.node_opts::<MessageBusCommand<CqrsEvent<TEvent>>, _>(
        vec![events.erased()],
        move |ctx| {
            for event in ctx.batch::<CqrsEvent<TEvent>>(0) {
                ctx.emit(MessageBusCommand::Publish {
                    topic: topic.clone(),
                    payload: (*event).clone(),
                    key: event.aggregate_id.clone(),
                    command_id: Some(compound_tuple_key("cqrs-outbox", &[&event.id])),
                    idempotency_key: Some(event.id.clone()),
                });
            }
        },
        GraphNodeOpts::named(name.into()),
    )
}

fn project<TIn: Clone + 'static, TOut: 'static>(
    graph: &Graph,
    source: &Node<TIn>,
    name: String,
    pick: impl Fn(&TIn) -> Option<TOut> + 'static,
) -> Node<TOut> {
    graph.node_opts::<TOut, _>(
        vec![source.erased()],
        move |ctx: &Ctx| {
            for fact in ctx.batch::<TIn>(0) {
                if let Some(value) = pick(&fact) {
                    ctx.emit(value);
                }
            }
        },
        GraphNodeOpts::named(name),
    )
}

fn message_delivery<T>(
    page: &MessageBusAvailablePage<T>,
    message: &MessageEnvelope<T>,
) -> MessageBusDelivery {
    MessageBusDelivery {
        topic: page.topic.clone(),
        seq: message.seq,
        subscription_id: page.subscription_id.clone(),
        command_id: message
            .command_id
            .clone()
            .unwrap_or_else(|| canonical_tuple_key(&[&page.topic, &message.seq.to_string()])),
    }
}

fn message_issue(delivery: &MessageBusDelivery) -> DataIssue {
    DataIssue {
        kind: "issue".to_owned(),
        code: "cqrs-message-lowering-rejected".to_owned(),
        message: "CQRS messaging recipe could not lower retained message to a command fact"
            .to_owned(),
        severity: "error".to_owned(),
        source: "cqrs.messaging".to_owned(),
        topic: Some(delivery.topic.clone()),
        details: Some(delivery_details(delivery)),
    }
}

fn issue_delivery(issue: &DataIssue) -> Option<MessageBusDelivery> {
    issue.details.as_deref().and_then(parse_delivery_details)
}

fn ack_command<T>(delivery: &MessageBusDelivery, command_id: String) -> MessageBusCommand<T> {
    MessageBusCommand::Ack {
        topic: delivery.topic.clone(),
        subscription_id: delivery.subscription_id.clone(),
        seq: delivery.seq,
        command_id: Some(command_id),
    }
}

fn shift_delivery(
    deliveries: &mut BTreeMap<String, Vec<MessageBusDelivery>>,
    command_id: &str,
) -> Option<MessageBusDelivery> {
    let queue = deliveries.get_mut(command_id)?;
    if queue.is_empty() {
        return None;
    }
    let first = queue.remove(0);
    if queue.is_empty() {
        deliveries.remove(command_id);
    }
    Some(first)
}

fn delivery_details(delivery: &MessageBusDelivery) -> String {
    format!(
        "messageBus:topic={};subscription_id={};seq={};command_id={}",
        delivery.topic, delivery.subscription_id, delivery.seq, delivery.command_id
    )
}

fn parse_delivery_details(details: &str) -> Option<MessageBusDelivery> {
    let rest = details.strip_prefix("messageBus:")?;
    let mut topic = None;
    let mut subscription_id = None;
    let mut seq = None;
    let mut command_id = None;
    for part in rest.split(';') {
        let (key, value) = part.split_once('=')?;
        match key {
            "topic" => topic = Some(value.to_owned()),
            "subscription_id" => subscription_id = Some(value.to_owned()),
            "seq" => seq = value.parse::<u64>().ok(),
            "command_id" => command_id = Some(value.to_owned()),
            _ => {}
        }
    }
    Some(MessageBusDelivery {
        topic: topic?,
        subscription_id: subscription_id?,
        seq: seq?,
        command_id: command_id?,
    })
}
