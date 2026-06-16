use std::cell::{Cell, RefCell};
use std::rc::Rc;

use graphrefly::cqrs::messaging::{
    cqrs_message_ack_commands, CqrsDeliveredCommand, CqrsMessageAckOptions,
    MessageBusDelivery as CqrsMessageBusDelivery,
};
use graphrefly::cqrs::work_queue::{
    cqrs_work_queue_disposition_command, CqrsQueuedCommandPayload, CqrsWorkQueueAttempt,
    CqrsWorkQueueOutcome, CqrsWorkQueuePolicy,
};
use graphrefly::process::messaging::{
    process_message_ack_commands, MessageBusDelivery as ProcessMessageBusDelivery,
    ProcessDeliveredCommand, ProcessMessageAckOptions,
};
use graphrefly::process::work_queue::{
    process_work_queue_recipe, ProcessQueuedEffectPayload, ProcessWorkQueueRecipeOptions,
};
use graphrefly::{
    graph, message_bus, work_queue, BackoffPolicy, CqrsCommand, CqrsCursor, CqrsErrorCode,
    CqrsStatus, CqrsStatusState, Message, MessageBusCommand, MessageBusOptions, ProcessCursor,
    ProcessEffectRequest, ProcessStatus, ProcessStatusState, RetryPolicy, WorkQueueClaimOptions,
    WorkQueueCommand, WorkQueueMessageBusRef, WorkQueueOptions, WorkQueueRecord, WorkQueueSubmit,
    WorkQueueSubmitOptions,
};

fn collect_data<T: Clone + 'static>(node: &graphrefly::Node<T>) -> Rc<RefCell<Vec<T>>> {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let sink = seen.clone();
    let _keep = node.subscribe(move |msg| {
        if let Message::Data(value) = msg {
            if let Some(value) = value.as_ref().downcast_ref::<T>() {
                sink.borrow_mut().push(value.clone());
            }
        }
    });
    seen
}

fn cqrs_cursor() -> CqrsCursor {
    CqrsCursor {
        event_seq: 0,
        command_count: 1,
        error_count: 0,
        audit_seq: 1,
        dedupe: None,
    }
}

#[test]
fn cqrs_messaging_ack_waits_for_visible_status() {
    let g = graph();
    let delivered = g.state_empty::<CqrsDeliveredCommand<String>>();
    let status = g.state_empty::<CqrsStatus>();
    let ack = cqrs_message_ack_commands(
        &g,
        CqrsMessageAckOptions {
            name: "test/cqrsAck".to_owned(),
            delivered_commands: delivered.clone(),
            status: status.clone(),
            issues: None,
            ack_rejected: true,
        },
    );
    let seen = collect_data(&ack);
    let delivery = CqrsMessageBusDelivery {
        topic: "commands".to_owned(),
        seq: 7,
        subscription_id: "cqrs-worker".to_owned(),
        command_id: "msg-7".to_owned(),
    };

    delivered.set(CqrsDeliveredCommand {
        command: CqrsCommand::new("cmd-1", "PlaceOrder", "payload".to_owned()),
        delivery: delivery.clone(),
    });

    assert!(
        seen.borrow().is_empty(),
        "D351: retained delivery receipt must not ack before visible CQRS outcome"
    );

    status.set(CqrsStatus {
        state: CqrsStatusState::Accepted,
        command_id: Some("cmd-1".to_owned()),
        command_type: Some("PlaceOrder".to_owned()),
        event_count: 0,
        error_code: None,
        cursor: cqrs_cursor(),
    });

    assert!(matches!(
        seen.borrow().last(),
        Some(MessageBusCommand::Ack {
            topic,
            subscription_id,
            seq,
            ..
        }) if topic == "commands" && subscription_id == "cqrs-worker" && *seq == 7
    ));
}

#[test]
fn process_messaging_ack_can_skip_rejected_when_policy_says_so() {
    let g = graph();
    let delivered = g.state_empty::<ProcessDeliveredCommand<String>>();
    let status = g.state_empty::<ProcessStatus>();
    let ack = process_message_ack_commands(
        &g,
        ProcessMessageAckOptions {
            name: "test/processAck".to_owned(),
            delivered_commands: delivered.clone(),
            status: status.clone(),
            issues: None,
            ack_rejected: false,
        },
    );
    let seen = collect_data(&ack);

    delivered.set(ProcessDeliveredCommand {
        command: graphrefly::process::ProcessCommand::new("cmd-1", "DoIt", "payload".to_owned()),
        delivery: ProcessMessageBusDelivery {
            topic: "process-commands".to_owned(),
            seq: 3,
            subscription_id: "process-worker".to_owned(),
            command_id: "msg-3".to_owned(),
        },
    });
    status.set(ProcessStatus {
        state: ProcessStatusState::Rejected,
        command_id: Some("cmd-1".to_owned()),
        command_type: Some("DoIt".to_owned()),
        event_count: 0,
        effect_count: 0,
        error_code: None,
        cursor: ProcessCursor {
            event_seq: 0,
            effect_seq: 0,
            command_count: 1,
            error_count: 1,
            audit_seq: 1,
        },
    });

    assert!(
        seen.borrow().is_empty(),
        "D351: rejected outcomes are visible but ack remains policy-controlled"
    );
}

#[test]
fn cqrs_work_queue_disposition_matches_d352_matrix() {
    let command = CqrsCommand::new("cmd-1", "PlaceOrder", "payload".to_owned());
    let payload = CqrsQueuedCommandPayload::new(command.clone());
    let attempt = CqrsWorkQueueAttempt {
        kind: "cqrs-work-queue-attempt".to_owned(),
        work_id: "work-1".to_owned(),
        lease_id: "lease-1".to_owned(),
        queue_attempt: 2,
        worker_id: "worker-a".to_owned(),
        command,
        payload,
        source_refs: Vec::new(),
    };
    let policy = CqrsWorkQueuePolicy::default();
    let accepted = CqrsStatus {
        state: CqrsStatusState::Accepted,
        command_id: Some("cmd-1".to_owned()),
        command_type: Some("PlaceOrder".to_owned()),
        event_count: 0,
        error_code: None,
        cursor: cqrs_cursor(),
    };
    let rejected = CqrsStatus {
        state: CqrsStatusState::Rejected,
        command_id: Some("cmd-1".to_owned()),
        command_type: Some("PlaceOrder".to_owned()),
        event_count: 0,
        error_code: Some(CqrsErrorCode::UnknownCommand),
        cursor: cqrs_cursor(),
    };
    let handler_threw = CqrsStatus {
        error_code: Some(CqrsErrorCode::HandlerThrew),
        ..rejected.clone()
    };

    assert!(matches!(
        cqrs_work_queue_disposition_command(
            &attempt,
            CqrsWorkQueueOutcome::Accepted { status: accepted },
            &policy
        ),
        WorkQueueCommand::Complete { result: Some(result), .. }
            if result.contains("cqrs-accepted") && result.contains("event_count=0")
    ));
    assert!(matches!(
        cqrs_work_queue_disposition_command(
            &attempt,
            CqrsWorkQueueOutcome::Rejected {
                status: rejected,
                error: None
            },
            &policy
        ),
        WorkQueueCommand::Complete { result: Some(result), .. }
            if result.contains("cqrs-rejected")
    ));
    assert!(matches!(
        cqrs_work_queue_disposition_command(
            &attempt,
            CqrsWorkQueueOutcome::Rejected {
                status: handler_threw.clone(),
                error: None
            },
            &policy
        ),
        WorkQueueCommand::Fail {
            retryable: Some(true),
            ..
        }
    ));
    assert!(matches!(
        cqrs_work_queue_disposition_command(
            &attempt,
            CqrsWorkQueueOutcome::Rejected {
                status: handler_threw,
                error: None
            },
            &CqrsWorkQueuePolicy::default()
                .deterministic_handler_failure(CqrsErrorCode::HandlerThrew)
        ),
        WorkQueueCommand::Fail {
            retryable: Some(false),
            ..
        }
    ));
    assert!(matches!(
        cqrs_work_queue_disposition_command(
            &attempt,
            CqrsWorkQueueOutcome::Release {
                reason: Some("shutdown".to_owned())
            },
            &policy
        ),
        WorkQueueCommand::Release {
            reason: Some(reason),
            ..
        } if reason == "shutdown"
    ));
}

#[test]
fn cqrs_work_queue_release_invalidates_active_claim_before_status() {
    let g = graph();
    let records = g.state_empty::<WorkQueueRecord<CqrsQueuedCommandPayload<String>>>();
    let status = g.state_empty::<CqrsStatus>();
    let recipe = graphrefly::cqrs::work_queue::cqrs_work_queue_recipe(
        &g,
        graphrefly::cqrs::work_queue::CqrsWorkQueueRecipeOptions::new(
            records.clone(),
            status.clone(),
        )
        .named("test/cqrsWqStale"),
    );
    let commands = collect_data(&recipe.commands);
    let issues = collect_data(&recipe.issues);
    let command = CqrsCommand::new("cmd-1", "PlaceOrder", "payload".to_owned());
    let payload = CqrsQueuedCommandPayload::new(command);
    records.set(WorkQueueRecord::WorkAdmitted {
        record_seq: 1,
        queue_id: "q".to_owned(),
        work_id: "work-1".to_owned(),
        payload,
        message_bus: WorkQueueMessageBusRef {
            topic: "work".to_owned(),
            seq: 1,
            subscription_id: "q-admit".to_owned(),
        },
        priority: None,
        tags: Vec::new(),
        requirements: Vec::new(),
        not_before_ms: None,
        deadline_ms: None,
        recorded_at_ms: 10,
    });
    records.set(WorkQueueRecord::WorkClaimed {
        record_seq: 2,
        queue_id: "q".to_owned(),
        work_id: "work-1".to_owned(),
        command_id: "claim-1".to_owned(),
        lease_id: "lease-1".to_owned(),
        attempt: 1,
        worker_id: "worker-a".to_owned(),
        claimed_at_ms: 11,
        lease_expires_at_ms: 20,
    });
    records.set(WorkQueueRecord::WorkReleased {
        record_seq: 3,
        queue_id: "q".to_owned(),
        work_id: "work-1".to_owned(),
        command_id: "release-1".to_owned(),
        lease_id: "lease-1".to_owned(),
        attempt: 1,
        worker_id: "worker-a".to_owned(),
        released_at_ms: 12,
        reason: Some("shutdown".to_owned()),
    });
    status.set(CqrsStatus {
        state: CqrsStatusState::Accepted,
        command_id: Some("cmd-1".to_owned()),
        command_type: Some("PlaceOrder".to_owned()),
        event_count: 1,
        error_code: None,
        cursor: cqrs_cursor(),
    });

    assert!(
        commands.borrow().is_empty(),
        "D352: stale released claims must not produce queue dispositions"
    );
    assert!(issues
        .borrow()
        .iter()
        .any(|issue| issue.code == "cqrs-status-without-active-queue-claim"));
}

#[test]
fn work_queue_nonretryable_fail_dead_letters_without_retry() {
    let g = graph();
    let now = Rc::new(Cell::new(0));
    let bus = message_bus::<WorkQueueSubmit<String>>(
        &g,
        MessageBusOptions::named("bus")
            .with_topics(["work"])
            .with_now({
                let now = now.clone();
                move || now.get()
            }),
    );
    let queue = work_queue(
        &g,
        WorkQueueOptions::new("q", bus, "work", "q-admit")
            .with_now({
                let now = now.clone();
                move || now.get()
            })
            .with_retry(RetryPolicy::new(
                5,
                BackoffPolicy::Constant { delay_ms: 10 },
            )),
    );
    let records = collect_data(&queue.records);

    queue.submit("payload".to_owned(), WorkQueueSubmitOptions::default());
    queue.claim(WorkQueueClaimOptions::new("worker-a").command_id("claim-1"));
    queue.fail(
        "q:work:1",
        "q:work:1:lease:1",
        1,
        "worker-a",
        "fail-1",
        Some(false),
    );

    assert!(records.borrow().iter().any(|record| matches!(
        record,
        WorkQueueRecord::AttemptFailed {
            retryable: Some(false),
            ..
        }
    )));
    assert!(
        !records
            .borrow()
            .iter()
            .any(|record| matches!(record, WorkQueueRecord::RetryScheduled { .. })),
        "D352: nonretryable fail must not schedule retry"
    );
    assert!(records
        .borrow()
        .iter()
        .any(|record| matches!(record, WorkQueueRecord::WorkDeadLettered { .. })));
}

#[test]
fn process_work_queue_maps_terminal_record_to_evidence() {
    let g = graph();
    let records = g.state_empty::<WorkQueueRecord<ProcessQueuedEffectPayload<String>>>();
    let recipe = process_work_queue_recipe(
        &g,
        ProcessWorkQueueRecipeOptions::new(records.clone()).named("test/processWq"),
    );
    let evidence = collect_data(&recipe.evidence);
    let effect = ProcessEffectRequest {
        id: "effect-1".to_owned(),
        effect_type: "send-email".to_owned(),
        seq: 1,
        cursor: 1,
        command_id: "cmd-1".to_owned(),
        command_type: "Notify".to_owned(),
        payload: "payload".to_owned(),
        timestamp_ms: 10,
        process_id: Some("process-1".to_owned()),
        correlation_id: None,
        causation_id: None,
    };
    let payload = ProcessQueuedEffectPayload::new(effect);
    records.set(WorkQueueRecord::WorkAdmitted {
        record_seq: 1,
        queue_id: "q".to_owned(),
        work_id: "work-1".to_owned(),
        payload,
        message_bus: WorkQueueMessageBusRef {
            topic: "work".to_owned(),
            seq: 1,
            subscription_id: "q-admit".to_owned(),
        },
        priority: None,
        tags: Vec::new(),
        requirements: Vec::new(),
        not_before_ms: None,
        deadline_ms: None,
        recorded_at_ms: 10,
    });
    records.set(WorkQueueRecord::WorkCompleted {
        record_seq: 2,
        queue_id: "q".to_owned(),
        work_id: "work-1".to_owned(),
        command_id: "complete-1".to_owned(),
        lease_id: "lease-1".to_owned(),
        attempt: 1,
        worker_id: "worker-a".to_owned(),
        result: Some("ok".to_owned()),
        recorded_at_ms: 20,
    });

    let evidence = evidence.borrow();
    assert_eq!(evidence.len(), 1);
    assert_eq!(evidence[0].effect_id, "effect-1");
    assert_eq!(evidence[0].queue_record_kind, "work-completed");
    assert_eq!(evidence[0].result.as_deref(), Some("ok"));
}
