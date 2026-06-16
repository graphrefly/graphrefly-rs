use std::cell::{Cell, RefCell};
use std::rc::Rc;

use graphrefly::{
    graph, message_bus, work_queue, BackoffPolicy, DataIssue, Message, MessageBusOptions,
    PullDemand, RetryPolicy, WorkQueue, WorkQueueAvailableParams, WorkQueueClaimOptions,
    WorkQueueDerivedState, WorkQueueOptions, WorkQueueRecord, WorkQueueSubmit,
    WorkQueueSubmitOptions,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct Payload {
    id: &'static str,
}

fn test_bus(
    g: &graphrefly::Graph,
    now: Rc<Cell<u64>>,
) -> graphrefly::MessageBus<WorkQueueSubmit<Payload>> {
    message_bus::<WorkQueueSubmit<Payload>>(
        g,
        MessageBusOptions::named("bus")
            .with_topics(["work"])
            .with_now(move || now.get()),
    )
}

fn test_queue(
    g: &graphrefly::Graph,
    bus: graphrefly::MessageBus<WorkQueueSubmit<Payload>>,
    now: Rc<Cell<u64>>,
) -> WorkQueue<Payload> {
    work_queue(
        g,
        WorkQueueOptions::new("q", bus, "work", "q-admit").with_now(move || now.get()),
    )
}

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

#[test]
fn admits_submitted_work_through_message_bus_and_acks_after_record() {
    let g = graph();
    let now = Rc::new(Cell::new(100));
    let bus = test_bus(&g, now.clone());
    let queue = test_queue(&g, bus.clone(), now);
    let records = collect_data(&queue.records);
    let cursor = collect_data(
        &bus.subscription(graphrefly::MessageBusSubscriptionOptions::new(
            "work", "q-admit",
        ))
        .cursor,
    );

    queue.submit(Payload { id: "a" }, WorkQueueSubmitOptions::default());

    assert!(records.borrow().iter().any(|record| matches!(
        record,
        WorkQueueRecord::WorkAdmitted {
            queue_id,
            work_id,
            payload,
            message_bus,
            ..
        } if queue_id == "q"
            && work_id == "q:work:1"
            && payload.id == "a"
            && message_bus.topic == "work"
            && message_bus.seq == 1
            && message_bus.subscription_id == "q-admit"
    )));
    assert_eq!(
        cursor.borrow().last().map(|cursor| cursor.next_seq),
        Some(2)
    );
    let describe = g.describe();
    assert!(
        describe
            .nodes
            .iter()
            .any(|node| { node.id == "workQueue/q/admissionAckCommands" }),
        "{:?}",
        describe.nodes
    );
}

#[test]
fn admits_retained_backlog_through_subscription_available() {
    let g = graph();
    let now = Rc::new(Cell::new(101));
    let bus = test_bus(&g, now.clone());
    bus.publish(
        "work",
        WorkQueueSubmit::new(Payload { id: "before" }),
        None,
        Some("pre".to_owned()),
        None,
    );
    let queue = test_queue(&g, bus.clone(), now);
    let records = collect_data(&queue.records);
    let cursor = collect_data(
        &bus.subscription(graphrefly::MessageBusSubscriptionOptions::new(
            "work", "q-admit",
        ))
        .cursor,
    );

    assert!(records.borrow().iter().any(|record| matches!(
        record,
        WorkQueueRecord::WorkAdmitted { work_id, payload, .. }
            if work_id == "q:work:1" && payload.id == "before"
    )));
    assert_eq!(
        cursor.borrow().last().map(|cursor| cursor.next_seq),
        Some(2)
    );
}

#[test]
fn claim_race_emits_issue_and_lease_lifecycle_records() {
    let g = graph();
    let now = Rc::new(Cell::new(10));
    let bus = test_bus(&g, now.clone());
    let queue = test_queue(&g, bus, now);
    let records = collect_data(&queue.records);
    let issues = collect_data::<DataIssue>(&queue.issues);

    queue.submit(Payload { id: "a" }, WorkQueueSubmitOptions::default());
    queue.claim(WorkQueueClaimOptions::new("w1").command_id("claim-1"));
    queue.claim(
        WorkQueueClaimOptions::new("w2")
            .requested_work_ids(["q:work:1"])
            .command_id("claim-2"),
    );
    queue.renew_lease("q:work:1", "q:work:1:lease:1", 1, "w1", "renew-1");
    queue.release("q:work:1", "q:work:1:lease:1", 1, "w1", "release-1");
    queue.claim(WorkQueueClaimOptions::new("w2").command_id("claim-3"));
    queue.complete(
        "q:work:1",
        "q:work:1:lease:2",
        2,
        "w2",
        "complete-1",
        Some("ok".to_owned()),
    );
    queue.fail("q:work:1", "q:work:1:lease:2", 2, "w2", "fail-stale", None);

    assert!(records.borrow().iter().any(|record| matches!(
        record,
        WorkQueueRecord::WorkClaimed { worker_id, lease_id, .. }
            if worker_id == "w1" && lease_id == "q:work:1:lease:1"
    )));
    assert!(records
        .borrow()
        .iter()
        .any(|record| matches!(record, WorkQueueRecord::LeaseRenewed { .. })));
    assert!(records
        .borrow()
        .iter()
        .any(|record| matches!(record, WorkQueueRecord::WorkReleased { .. })));
    assert!(records.borrow().iter().any(|record| matches!(
        record,
        WorkQueueRecord::WorkCompleted { result, .. } if result.as_deref() == Some("ok")
    )));
    assert!(issues
        .borrow()
        .iter()
        .any(|issue| issue.code == "already-leased"));
    assert_eq!(
        issues.borrow().last().map(|issue| issue.code.as_str()),
        Some("terminal-work")
    );
}

#[test]
fn explicit_expiry_retry_and_dead_letter_are_recorded() {
    let g = graph();
    let now = Rc::new(Cell::new(0));
    let bus = test_bus(&g, now.clone());
    let queue = work_queue(
        &g,
        WorkQueueOptions::new("q", bus, "work", "q-admit")
            .with_now({
                let now = now.clone();
                move || now.get()
            })
            .with_lease_duration_ms(5)
            .with_retry(RetryPolicy::new(2, BackoffPolicy::Constant { delay_ms: 5 })),
    );
    let records = collect_data(&queue.records);

    queue.submit(Payload { id: "a" }, WorkQueueSubmitOptions::default());
    queue.claim(WorkQueueClaimOptions::new("w1").command_id("claim-1"));
    now.set(6);
    queue.expire_leases("expire-1");
    queue.claim(WorkQueueClaimOptions::new("w2").command_id("claim-2"));
    queue.fail("q:work:1", "q:work:1:lease:2", 2, "w2", "fail-2", None);

    assert!(records.borrow().iter().any(|record| matches!(
        record,
        WorkQueueRecord::LeaseExpired { lease_id, .. } if lease_id == "q:work:1:lease:1"
    )));
    assert!(records.borrow().iter().any(|record| matches!(
        record,
        WorkQueueRecord::WorkDeadLettered { reason, .. } if reason == "attempts-exhausted"
    )));
    let dead = queue.dead_letter();
    dead.snapshot.up(vec![Message::Pull(PullDemand::new(
        dead.snapshot_pull_id.clone(),
    ))]);
    assert_eq!(dead.snapshot.cache().unwrap().entries.len(), 1);
}

#[test]
fn read_projections_are_pull_only_and_do_not_mutate_lifecycle() {
    let g = graph();
    let now = Rc::new(Cell::new(0));
    let bus = test_bus(&g, now.clone());
    let queue = test_queue(&g, bus, now);
    queue.submit(
        Payload { id: "later" },
        WorkQueueSubmitOptions {
            work_id: Some("custom".to_owned()),
            not_before_ms: Some(10),
            ..WorkQueueSubmitOptions::default()
        },
    );
    let available = queue.available();
    let work = queue.work("custom");

    available
        .available
        .up(vec![Message::Pull(PullDemand::with_params(
            available.available_pull_id.clone(),
            WorkQueueAvailableParams {
                now_ms: Some(0),
                ..WorkQueueAvailableParams::default()
            },
        ))]);
    assert_eq!(available.available.cache().unwrap().items, Vec::new());

    available
        .available
        .up(vec![Message::Pull(PullDemand::with_params(
            available.available_pull_id.clone(),
            WorkQueueAvailableParams {
                now_ms: Some(10),
                ..WorkQueueAvailableParams::default()
            },
        ))]);
    assert_eq!(
        available.available.cache().unwrap().items[0].work_id,
        "custom"
    );

    work.snapshot.up(vec![Message::Pull(PullDemand::new(
        work.snapshot_pull_id.clone(),
    ))]);
    assert_eq!(
        work.snapshot.cache().unwrap().state,
        Some(WorkQueueDerivedState::Scheduled)
    );
    queue.cancel("custom", "cancel-1", Some("user".to_owned()));
    assert_eq!(
        work.snapshot.cache().unwrap().state,
        Some(WorkQueueDerivedState::Scheduled)
    );
    work.snapshot.up(vec![Message::Pull(PullDemand::new(
        work.snapshot_pull_id.clone(),
    ))]);
    assert_eq!(
        work.snapshot.cache().unwrap().state,
        Some(WorkQueueDerivedState::Canceled)
    );
}

#[test]
fn claim_defaults_to_fifo_admission_order_not_work_id_order() {
    let g = graph();
    let now = Rc::new(Cell::new(0));
    let bus = test_bus(&g, now.clone());
    let queue = test_queue(&g, bus, now);
    let records = collect_data(&queue.records);

    queue.submit(
        Payload { id: "first" },
        WorkQueueSubmitOptions {
            work_id: Some("z-first".to_owned()),
            ..WorkQueueSubmitOptions::default()
        },
    );
    queue.submit(
        Payload { id: "second" },
        WorkQueueSubmitOptions {
            work_id: Some("a-second".to_owned()),
            ..WorkQueueSubmitOptions::default()
        },
    );
    queue.claim(
        WorkQueueClaimOptions::new("w")
            .command_id("claim-fifo")
            .requested_work_ids(Vec::<String>::new()),
    );

    assert!(records.borrow().iter().any(|record| matches!(
        record,
        WorkQueueRecord::WorkClaimed { work_id, command_id, .. }
            if work_id == "z-first" && command_id == "claim-fifo"
    )));
}

#[test]
fn available_pagination_uses_the_same_admission_order_cursor_as_pages() {
    let g = graph();
    let now = Rc::new(Cell::new(0));
    let bus = test_bus(&g, now.clone());
    let queue = test_queue(&g, bus, now);
    queue.submit(
        Payload { id: "first" },
        WorkQueueSubmitOptions {
            work_id: Some("z-first".to_owned()),
            ..WorkQueueSubmitOptions::default()
        },
    );
    queue.submit(
        Payload { id: "second" },
        WorkQueueSubmitOptions {
            work_id: Some("a-second".to_owned()),
            ..WorkQueueSubmitOptions::default()
        },
    );
    let available = queue.available();

    available
        .available
        .up(vec![Message::Pull(PullDemand::with_params(
            available.available_pull_id.clone(),
            WorkQueueAvailableParams {
                limit: Some(1),
                ..WorkQueueAvailableParams::default()
            },
        ))]);
    let page1 = available.available.cache().unwrap();
    assert_eq!(page1.items[0].work_id, "z-first");
    assert!(page1.has_more);

    available
        .available
        .up(vec![Message::Pull(PullDemand::with_params(
            available.available_pull_id.clone(),
            WorkQueueAvailableParams {
                limit: Some(1),
                after_work_id: page1.next_after_work_id,
                after_admission_seq: page1.next_after_admission_seq,
                ..WorkQueueAvailableParams::default()
            },
        ))]);
    let page2 = available.available.cache().unwrap();
    assert_eq!(page2.items[0].work_id, "a-second");
    assert!(!page2.has_more);
}

#[test]
fn leased_work_cannot_be_scheduled_and_clock_overflow_is_visible_issue() {
    let g = graph();
    let now = Rc::new(Cell::new(0));
    let bus = test_bus(&g, now.clone());
    let queue = test_queue(&g, bus, now);
    let issues = collect_data::<DataIssue>(&queue.issues);

    queue.submit(Payload { id: "a" }, WorkQueueSubmitOptions::default());
    queue.claim(WorkQueueClaimOptions::new("w").command_id("claim-1"));
    queue.schedule("q:work:1", 10, "schedule-while-leased");

    assert!(issues
        .borrow()
        .iter()
        .any(|issue| issue.code == "schedule-conflict"));

    queue.commands.set(graphrefly::WorkQueueCommand::Claim {
        command_id: "overflow-claim".to_owned(),
        queue_id: None,
        idempotency_key: None,
        worker_id: "w2".to_owned(),
        requested_work_ids: Vec::new(),
        limit: None,
        lease_duration_ms: Some(1),
        now_ms: Some(u64::MAX),
    });

    assert!(issues
        .borrow()
        .iter()
        .any(|issue| issue.code == "clock-overflow"));
}

#[test]
fn duplicate_and_wrong_queue_commands_emit_issues_without_records() {
    let g = graph();
    let now = Rc::new(Cell::new(0));
    let bus = test_bus(&g, now.clone());
    let queue = test_queue(&g, bus, now);
    let issues = collect_data::<DataIssue>(&queue.issues);
    let records = collect_data(&queue.records);

    queue.commands.set(graphrefly::WorkQueueCommand::Claim {
        command_id: String::new(),
        queue_id: None,
        idempotency_key: None,
        worker_id: "w".to_owned(),
        requested_work_ids: Vec::new(),
        limit: None,
        lease_duration_ms: None,
        now_ms: None,
    });
    queue.commands.set(graphrefly::WorkQueueCommand::Cancel {
        command_id: "bad-2".to_owned(),
        queue_id: Some("other".to_owned()),
        idempotency_key: None,
        work_id: "x".to_owned(),
        reason: None,
        now_ms: None,
    });

    assert!(issues
        .borrow()
        .iter()
        .any(|issue| issue.code == "malformed-command"));
    assert!(issues
        .borrow()
        .iter()
        .any(|issue| issue.code == "queue-mismatch"));
    assert!(records.borrow().is_empty());
}
