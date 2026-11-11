//! Optional process-over-messageBus recipe (D349/D351/D353), the shared
//! command-over-message-bus recipe over the process command, event and status (the
//! CQRS bundle uses the same one).

use crate::process::{ProcessCommand, ProcessEvent, ProcessStatus, ProcessStatusState};

crate::command_messaging::command_messaging_recipe! {
    kind: "process",
    event_key: process_id,
    Command: ProcessCommand,
    Event: ProcessEvent,
    Status: ProcessStatus,
    StatusState: ProcessStatusState,
    DeliveredCommand: ProcessDeliveredCommand,
    MessageCommandFn: ProcessMessageCommandFn,
    MessagingPolicy: ProcessMessagingPolicy,
    MessagingRecipeOptions: ProcessMessagingRecipeOptions,
    MessagingRecipeBundle: ProcessMessagingRecipeBundle,
    MessagingFact: ProcessMessagingFact,
    MessageAckOptions: ProcessMessageAckOptions,
    messaging_recipe: process_messaging_recipe,
    message_ack_commands: process_message_ack_commands,
    ack_id: process_ack_id,
    event_outbox_commands: process_event_outbox_commands,
}
