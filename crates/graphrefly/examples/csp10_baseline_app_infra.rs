use std::cell::{Cell, RefCell};
use std::rc::Rc;

use graphrefly::cqrs::messaging::{
    cqrs_message_ack_commands, CqrsDeliveredCommand, CqrsMessageAckOptions,
};
use graphrefly::cqrs::work_queue::{
    cqrs_work_queue_disposition_command, cqrs_work_queue_recipe, CqrsQueuedCommandPayload,
    CqrsWorkQueueAttempt, CqrsWorkQueueOutcome, CqrsWorkQueuePolicy, CqrsWorkQueueRecipeOptions,
};
use graphrefly::process::work_queue::{
    process_work_queue_recipe, ProcessQueuedEffectPayload, ProcessWorkQueueRecipeOptions,
};
use graphrefly::{
    cqrs_command_handler, cqrs_with_options, graph, message_bus, scheduled_readiness_projector,
    work_queue, work_queue_readiness_handoff_projector, work_queue_scheduled_readiness_projector,
    CqrsCommand, CqrsEventDraft, CqrsOptions, Message, MessageBusOptions, ScheduledReadinessClock,
    ScheduledReadinessOptions, WorkQueueClaimOptions, WorkQueueOptions,
    WorkQueueReadinessHandoffOptions, WorkQueueRecord, WorkQueueScheduledReadinessOptions,
    WorkQueueSubmitOptions,
};

struct Collected<T> {
    values: Rc<RefCell<Vec<T>>>,
    unsubscribe: Option<Box<dyn FnOnce()>>,
}

impl<T> Drop for Collected<T> {
    fn drop(&mut self) {
        if let Some(unsubscribe) = self.unsubscribe.take() {
            unsubscribe();
        }
    }
}

fn collect_data<T: Clone + 'static>(node: &graphrefly::Node<T>) -> Collected<T> {
    let seen = Rc::new(RefCell::new(Vec::new()));
    let sink = seen.clone();
    let unsubscribe = node.subscribe(move |msg| {
        if let Message::Data(value) = msg {
            if let Some(value) = value.as_ref().downcast_ref::<T>() {
                sink.borrow_mut().push(value.clone());
            }
        }
    });
    Collected {
        values: seen,
        unsubscribe: Some(unsubscribe),
    }
}

fn main() {
    let graph = graph();
    let now = Rc::new(Cell::new(0_u64));

    let cqrs = cqrs_with_options::<String, String>(
        &graph,
        CqrsOptions::named("example/cqrs")
            .with_handlers(vec![cqrs_command_handler(
                "PlaceOrder",
                |command: &CqrsCommand<String>| {
                    vec![CqrsEventDraft::new("OrderPlaced", command.payload.clone())]
                },
            )])
            .with_events(["OrderPlaced"]),
    );
    let bus = message_bus(
        &graph,
        MessageBusOptions::named("example/bus")
            .with_topics(["cqrs-work"])
            .with_now({
                let now = now.clone();
                move || now.get()
            }),
    );
    let queue = work_queue(
        &graph,
        WorkQueueOptions::new("cqrs-q", bus, "cqrs-work", "cqrs-admit").with_now({
            let now = now.clone();
            move || now.get()
        }),
    );
    let cqrs_queue = cqrs_work_queue_recipe(
        &graph,
        CqrsWorkQueueRecipeOptions::new(queue.records.clone(), cqrs.status.clone())
            .named("example/cqrsQueue"),
    );
    let _queue_commands = collect_data(&cqrs_queue.commands);
    let _ack_commands = cqrs_message_ack_commands(
        &graph,
        CqrsMessageAckOptions {
            name: "example/cqrsAck".to_owned(),
            delivered_commands: graph.state_empty::<CqrsDeliveredCommand<String>>(),
            status: cqrs.status.clone(),
            issues: None,
            ack_rejected: true,
        },
    );

    let readiness_schedules = work_queue_scheduled_readiness_projector(
        &graph,
        WorkQueueScheduledReadinessOptions::new(vec![queue.records.clone()])
            .named("example/queueReadinessSchedules"),
    );
    let clock = graph.state_empty::<ScheduledReadinessClock>();
    let readiness = scheduled_readiness_projector(
        &graph,
        ScheduledReadinessOptions::new(vec![readiness_schedules.readiness_schedules.clone()])
            .with_clocks(vec![clock.clone()])
            .named("example/readiness"),
    );
    let ready = collect_data(&readiness.ready);
    let handoff = work_queue_readiness_handoff_projector(
        &graph,
        WorkQueueReadinessHandoffOptions::new(
            vec![queue.records.clone()],
            vec![readiness.ready.clone()],
        )
        .named("example/queueReadinessHandoff"),
    );
    let candidates = collect_data(&handoff.candidates);
    let records = collect_data(&queue.records);

    let process_records =
        graph.state_empty::<WorkQueueRecord<ProcessQueuedEffectPayload<String>>>();
    let _process_queue = process_work_queue_recipe(
        &graph,
        ProcessWorkQueueRecipeOptions::new(process_records).named("example/processQueue"),
    );

    queue.submit(
        CqrsQueuedCommandPayload::new(CqrsCommand::new(
            "cmd-1",
            "PlaceOrder",
            "payload".to_owned(),
        )),
        WorkQueueSubmitOptions {
            work_id: Some("work-1".to_owned()),
            not_before_ms: Some(10),
            ..WorkQueueSubmitOptions::default()
        },
    );
    clock.set(ScheduledReadinessClock {
        clock_id: "clock".to_owned(),
        now_ms: 10,
        source_refs: Vec::new(),
        metadata: None,
    });
    now.set(10);
    queue.claim(WorkQueueClaimOptions::new("cqrs-worker").command_id("claim-1"));

    let admitted_payload = records
        .values
        .borrow()
        .iter()
        .find_map(|record| match record {
            WorkQueueRecord::WorkAdmitted { payload, .. } => Some(payload.clone()),
            _ => None,
        })
        .expect("messageBus admission creates visible CQRS work payload");
    let attempt = records
        .values
        .borrow()
        .iter()
        .find_map(|record| match record {
            WorkQueueRecord::WorkClaimed {
                work_id,
                lease_id,
                attempt,
                worker_id,
                ..
            } => Some(CqrsWorkQueueAttempt {
                kind: "cqrs-work-queue-attempt".to_owned(),
                work_id: work_id.clone(),
                lease_id: lease_id.clone(),
                queue_attempt: *attempt,
                worker_id: worker_id.clone(),
                command: admitted_payload.command.clone(),
                payload: admitted_payload.clone(),
                source_refs: Vec::new(),
            }),
            _ => None,
        })
        .expect("workQueue claim creates visible CQRS attempt coordinates");
    cqrs.dispatch(admitted_payload.command.clone());
    let disposition = cqrs_work_queue_disposition_command(
        &attempt,
        CqrsWorkQueueOutcome::Accepted {
            status: cqrs.status.cache().expect("CQRS status"),
        },
        &CqrsWorkQueuePolicy::default(),
    );
    queue.commands.set(disposition);

    assert!(ready.values.borrow().iter().any(|fact| fact
        .subject_refs
        .iter()
        .any(|subject| subject.id == "work-1")));
    assert!(candidates
        .values
        .borrow()
        .iter()
        .any(|candidate| candidate.work_id == "work-1"));
    assert!(records.values.borrow().iter().any(|record| matches!(
        record,
        WorkQueueRecord::WorkCompleted {
            result: Some(result),
            ..
        } if result.contains("cqrs-accepted")
    )));
    assert!(cqrs
        .status
        .cache()
        .is_some_and(|status| { status.state == graphrefly::CqrsStatusState::Accepted }));
}
