//! The command-over-message-bus recipe, written once for the CQRS and process
//! bundles (review §4.2: the two copies differed in nothing but names): retained
//! delivery is lowered to the bundle's command facts, and ack commands are emitted
//! only after a graph-visible accepted/rejected status or lowering issue exists;
//! receiving a retained message is never itself an ack. Each bundle's `messaging`
//! module instantiates [`command_messaging_recipe`] with its own types.

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MessageBusDelivery {
    pub topic: String,
    pub seq: u64,
    pub subscription_id: String,
    pub command_id: String,
}

/// Defines a bundle's messaging recipe over its command, event and status types.
macro_rules! command_messaging_recipe {
    (
        kind: $kind:literal,
        event_key: $event_key:ident,
        Command: $Command:ident,
        Event: $Event:ident,
        Status: $Status:ident,
        StatusState: $StatusState:ident,
        DeliveredCommand: $DeliveredCommand:ident,
        MessageCommandFn: $MessageCommandFn:ident,
        MessagingPolicy: $MessagingPolicy:ident,
        MessagingRecipeOptions: $MessagingRecipeOptions:ident,
        MessagingRecipeBundle: $MessagingRecipeBundle:ident,
        MessagingFact: $MessagingFact:ident,
        MessageAckOptions: $MessageAckOptions:ident,
        messaging_recipe: $messaging_recipe:ident,
        message_ack_commands: $message_ack_commands:ident,
        ack_id: $ack_id:ident,
        event_outbox_commands: $event_outbox_commands:ident $(,)?
    ) => {
        use std::collections::BTreeMap;
        use std::rc::Rc;

        use crate::ctx::Ctx;
        use crate::graph::{Graph, GraphNodeOpts};
        use crate::identity::{canonical_tuple_key, compound_tuple_key};
        use crate::messaging::{
            DataIssue, MessageBusAvailablePage, MessageBusCommand, MessageEnvelope,
        };
        use crate::node::Node;

        pub use crate::command_messaging::MessageBusDelivery;

        #[derive(Debug, Clone, PartialEq)]
        pub struct $DeliveredCommand<TCommand> {
            pub command: $Command<TCommand>,
            pub delivery: MessageBusDelivery,
        }

        pub type $MessageCommandFn<TPayload, TCommand> = Rc<
            dyn Fn(&MessageEnvelope<TPayload>, &MessageBusDelivery) -> Option<$Command<TCommand>>,
        >;

        #[derive(Clone)]
        pub struct $MessagingPolicy<TPayload, TCommand> {
            pub command: $MessageCommandFn<TPayload, TCommand>,
            pub ack_rejected: bool,
            pub outbox_topic: Option<String>,
        }

        impl<TPayload, TCommand> $MessagingPolicy<TPayload, TCommand> {
            pub fn new(
                command: impl Fn(
                        &MessageEnvelope<TPayload>,
                        &MessageBusDelivery,
                    ) -> Option<$Command<TCommand>>
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
        pub struct $MessagingRecipeOptions<TPayload, TCommand, TEvent> {
            pub name: String,
            pub deliveries: Node<MessageBusAvailablePage<TPayload>>,
            pub status: Node<$Status>,
            pub events: Option<Node<$Event<TEvent>>>,
            pub policy: $MessagingPolicy<TPayload, TCommand>,
        }

        impl<TPayload, TCommand, TEvent> $MessagingRecipeOptions<TPayload, TCommand, TEvent> {
            pub fn new(
                deliveries: impl Into<Node<MessageBusAvailablePage<TPayload>>>,
                status: impl Into<Node<$Status>>,
                policy: $MessagingPolicy<TPayload, TCommand>,
            ) -> Self {
                let deliveries: Node<MessageBusAvailablePage<TPayload>> = deliveries.into();
                let status: Node<$Status> = status.into();
                Self {
                    name: concat!($kind, "Messaging").to_owned(),
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

            pub fn with_events(mut self, events: impl Into<Node<$Event<TEvent>>>) -> Self {
                let events: Node<$Event<TEvent>> = events.into();
                self.events = Some(events);
                self
            }
        }

        #[derive(Clone)]
        pub struct $MessagingRecipeBundle<TCommand, TEvent> {
            pub delivered_commands: Node<$DeliveredCommand<TCommand>>,
            pub commands: Node<$Command<TCommand>>,
            pub ack_commands: Node<MessageBusCommand<TCommand>>,
            pub outbox_commands: Option<Node<MessageBusCommand<$Event<TEvent>>>>,
            pub issues: Node<DataIssue>,
        }

        #[derive(Clone)]
        enum $MessagingFact<TCommand> {
            Command($DeliveredCommand<TCommand>),
            Issue(DataIssue),
        }

        /// The pending deliveries an ack is matched against: a ledger the bundle owns
        /// (review Q13), kept as JSON.
        #[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
        struct AckState {
            deliveries: BTreeMap<String, Vec<MessageBusDelivery>>,
        }

        pub fn $messaging_recipe<
            TPayload: Clone + 'static,
            TCommand: Clone + 'static,
            TEvent: Clone + 'static,
        >(
            graph: &Graph,
            opts: $MessagingRecipeOptions<TPayload, TCommand, TEvent>,
        ) -> $MessagingRecipeBundle<TCommand, TEvent> {
            let name = opts.name.clone();
            let policy = opts.policy.clone();
            let runtime = graph.node_opts::<$MessagingFact<TCommand>, _>(
                vec![opts.deliveries.erased()],
                move |ctx| {
                    let mut emitted = Vec::new();
                    for page in ctx.dep(0).values::<MessageBusAvailablePage<TPayload>>() {
                        for message in &page.messages {
                            let delivery = message_delivery(page.as_ref(), message);
                            if let Some(command) = (policy.command)(message, &delivery) {
                                emitted.push($MessagingFact::Command($DeliveredCommand {
                                    command,
                                    delivery,
                                }));
                            } else {
                                emitted.push($MessagingFact::<TCommand>::Issue(message_issue(
                                    &delivery,
                                )));
                            }
                        }
                    }
                    ctx.down(crate::protocol::data_wave(emitted));
                },
                GraphNodeOpts::named(format!("{name}/runtime")),
            );
            let delivered_commands = project(
                graph,
                &runtime,
                format!("{name}/deliveredCommands"),
                |fact| match fact {
                    $MessagingFact::Command(delivered) => Some(delivered.clone()),
                    $MessagingFact::Issue(_) => None,
                },
            );
            let commands = project(
                graph,
                &runtime,
                format!("{name}/commands"),
                |fact| match fact {
                    $MessagingFact::Command(delivered) => Some(delivered.command.clone()),
                    $MessagingFact::Issue(_) => None,
                },
            );
            let issues = project(
                graph,
                &runtime,
                format!("{name}/issues"),
                |fact| match fact {
                    $MessagingFact::Issue(issue) => Some(issue.clone()),
                    $MessagingFact::Command(_) => None,
                },
            );
            let ack_commands = $message_ack_commands(
                graph,
                $MessageAckOptions {
                    name: format!("{name}/ackCommands"),
                    delivered_commands: delivered_commands.clone(),
                    status: opts.status,
                    issues: Some(issues.clone()),
                    ack_rejected: opts.policy.ack_rejected,
                },
            );
            let outbox_commands = opts.events.and_then(|events| {
                opts.policy.outbox_topic.map(|topic| {
                    $event_outbox_commands(graph, events, topic, format!("{name}/outboxCommands"))
                })
            });
            $MessagingRecipeBundle {
                delivered_commands,
                commands,
                ack_commands,
                outbox_commands,
                issues,
            }
        }

        pub struct $MessageAckOptions<TCommand> {
            pub name: String,
            pub delivered_commands: Node<$DeliveredCommand<TCommand>>,
            pub status: Node<$Status>,
            pub issues: Option<Node<DataIssue>>,
            pub ack_rejected: bool,
        }

        pub fn $message_ack_commands<TCommand: Clone + 'static>(
            graph: &Graph,
            opts: $MessageAckOptions<TCommand>,
        ) -> Node<MessageBusCommand<TCommand>> {
            let mut deps = vec![opts.delivered_commands.erased(), opts.status.erased()];
            if let Some(issues) = &opts.issues {
                deps.push(issues.erased());
            }
            let ledger = crate::ledger::Ledger::new(
                graph,
                format!("{}/ledger", opts.name),
                serde_json::to_value(AckState::default()).unwrap_or_default(),
            );
            deps.push(ledger.erased());
            graph.node_opts::<MessageBusCommand<TCommand>, _>(
                deps,
                move |ctx| {
                    let mut state: AckState = crate::ledger::load(ctx, ctx.dep_len() - 1);
                    for delivered in ctx.dep(0).values::<$DeliveredCommand<TCommand>>() {
                        state
                            .deliveries
                            .entry(delivered.command.id.clone())
                            .or_default()
                            .push(delivered.delivery.clone());
                    }
                    let mut emitted = Vec::new();
                    for status in ctx.dep(1).values::<$Status>() {
                        if status.state == $StatusState::Rejected && !opts.ack_rejected {
                            continue;
                        }
                        let Some(command_id) = &status.command_id else {
                            continue;
                        };
                        if let Some(delivery) = shift_delivery(&mut state.deliveries, command_id) {
                            emitted.push(ack_command::<TCommand>(
                                &delivery,
                                $ack_id(&delivery, "status-ack"),
                            ));
                        }
                    }
                    ctx.down(crate::protocol::data_wave(emitted));
                    if opts.issues.is_some() {
                        let mut emitted = Vec::new();
                        for issue in ctx.dep(2).values::<DataIssue>() {
                            if let Some(delivery) = issue_delivery(&issue) {
                                emitted.push(ack_command::<TCommand>(
                                    &delivery,
                                    $ack_id(&delivery, "issue-ack"),
                                ));
                            }
                        }
                        ctx.down(crate::protocol::data_wave(emitted));
                    }
                    crate::ledger::store(ctx, &state);
                },
                {
                    let mut node_opts = GraphNodeOpts::named(opts.name);
                    node_opts.node = crate::ledger::write_back_json(node_opts.node);
                    node_opts
                },
            )
        }

        fn $ack_id(delivery: &MessageBusDelivery, reason: &str) -> String {
            compound_tuple_key(
                concat!($kind, "-message-ack"),
                &[
                    &delivery.topic,
                    &delivery.subscription_id,
                    &delivery.seq.to_string(),
                    reason,
                ],
            )
        }

        pub fn $event_outbox_commands<TEvent: Clone + 'static>(
            graph: &Graph,
            events: impl Into<Node<$Event<TEvent>>>,
            topic: impl Into<String>,
            name: impl Into<String>,
        ) -> Node<MessageBusCommand<$Event<TEvent>>> {
            let events: Node<$Event<TEvent>> = events.into();
            let topic = topic.into();
            graph.node_opts::<MessageBusCommand<$Event<TEvent>>, _>(
                vec![events.erased()],
                move |ctx| {
                    let mut emitted = Vec::new();
                    for event in ctx.dep(0).values::<$Event<TEvent>>() {
                        emitted.push(MessageBusCommand::Publish {
                            topic: topic.clone(),
                            payload: (*event).clone(),
                            key: event.$event_key.clone(),
                            command_id: Some(compound_tuple_key(
                                concat!($kind, "-outbox"),
                                &[&event.id],
                            )),
                            idempotency_key: Some(event.id.clone()),
                        });
                    }
                    ctx.down(crate::protocol::data_wave(emitted));
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
                    let mut emitted = Vec::new();
                    for fact in ctx.dep(0).values::<TIn>() {
                        if let Some(value) = pick(&fact) {
                            emitted.push(value);
                        }
                    }
                    ctx.down(crate::protocol::data_wave(emitted));
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
                command_id: message.command_id.clone().unwrap_or_else(|| {
                    canonical_tuple_key(&[&page.topic, &message.seq.to_string()])
                }),
            }
        }

        fn message_issue(delivery: &MessageBusDelivery) -> DataIssue {
            DataIssue {
                kind: "issue".to_owned(),
                code: concat!($kind, "-message-lowering-rejected").to_owned(),
                message: "CQRS messaging recipe could not lower retained message to a command fact"
                    .to_owned(),
                severity: "error".to_owned(),
                source: concat!($kind, ".messaging").to_owned(),
                topic: Some(delivery.topic.clone()),
                details: Some(delivery_details(delivery)),
            }
        }

        fn issue_delivery(issue: &DataIssue) -> Option<MessageBusDelivery> {
            issue.details.as_deref().and_then(parse_delivery_details)
        }

        fn ack_command<T>(
            delivery: &MessageBusDelivery,
            command_id: String,
        ) -> MessageBusCommand<T> {
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
    };
}

pub(crate) use command_messaging_recipe;
