//! Optional CQRS-over-messageBus recipe (D350/D351/D353), the shared
//! command-over-message-bus recipe over the CQRS command, event and status (the
//! process bundle uses the same one).

use crate::cqrs::{CqrsCommand, CqrsEvent, CqrsStatus, CqrsStatusState};

crate::command_messaging::command_messaging_recipe! {
    kind: "cqrs",
    event_key: aggregate_id,
    Command: CqrsCommand,
    Event: CqrsEvent,
    Status: CqrsStatus,
    StatusState: CqrsStatusState,
    DeliveredCommand: CqrsDeliveredCommand,
    MessageCommandFn: CqrsMessageCommandFn,
    MessagingPolicy: CqrsMessagingPolicy,
    MessagingRecipeOptions: CqrsMessagingRecipeOptions,
    MessagingRecipeBundle: CqrsMessagingRecipeBundle,
    MessagingFact: CqrsMessagingFact,
    MessageAckOptions: CqrsMessageAckOptions,
    messaging_recipe: cqrs_messaging_recipe,
    message_ack_commands: cqrs_message_ack_commands,
    ack_id: cqrs_ack_id,
    event_outbox_commands: cqrs_event_outbox_commands,
}
