use std::cell::{Cell, RefCell};
use std::rc::Rc;

use graphrefly::{
    graph, message_bus, scheduled_readiness_projector,
    work_queue_lease_expiration_command_projector, work_queue_readiness_handoff_projector,
    work_queue_scheduled_readiness_projector, BackoffPolicy, Message, MessageBusOptions,
    ScheduledReadinessClock, ScheduledReadinessOptions, WorkQueue, WorkQueueClaimOptions,
    WorkQueueCommand, WorkQueueLeaseExpirationCommandProjectorOptions, WorkQueueOptions,
    WorkQueueReadinessCandidate, WorkQueueReadinessCandidateKind, WorkQueueReadinessHandoffOptions,
    WorkQueueReadinessStatusState, WorkQueueRecord, WorkQueueScheduledReadinessOptions,
    WorkQueueSubmit, WorkQueueSubmitOptions,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct Payload {
    id: &'static str,
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

fn test_queue(g: &graphrefly::Graph, now: Rc<Cell<u64>>) -> WorkQueue<Payload> {
    test_queue_named(g, now, "q", "work", "q-admit")
}

fn test_queue_named(
    g: &graphrefly::Graph,
    now: Rc<Cell<u64>>,
    queue_id: &str,
    topic: &str,
    admission_id: &str,
) -> WorkQueue<Payload> {
    let bus = message_bus::<WorkQueueSubmit<Payload>>(
        g,
        MessageBusOptions::named(format!("bus-{queue_id}"))
            .with_topics([topic])
            .with_now({
                let now = now.clone();
                move || now.get()
            }),
    );
    graphrefly::work_queue(
        g,
        WorkQueueOptions::new(queue_id, bus, topic, admission_id)
            .with_now(move || now.get())
            .with_retry(graphrefly::RetryPolicy::new(
                3,
                BackoffPolicy::Constant { delay_ms: 10 },
            )),
    )
}

#[test]
fn translator_lowers_scheduled_retry_and_lease_to_ready_at_ms() {
    let g = graph();
    let now = Rc::new(Cell::new(0));
    let queue = test_queue(&g, now.clone());
    let translator = work_queue_scheduled_readiness_projector(
        &g,
        WorkQueueScheduledReadinessOptions::new(vec![queue.records.clone()]),
    );
    let schedules = collect_data(&translator.readiness_schedules);

    queue.submit(
        Payload { id: "delayed" },
        WorkQueueSubmitOptions {
            work_id: Some("delayed".to_owned()),
            not_before_ms: Some(20),
            ..WorkQueueSubmitOptions::default()
        },
    );
    now.set(20);
    queue.claim(
        WorkQueueClaimOptions::new("w")
            .command_id("claim-delayed")
            .requested_work_ids(["delayed"]),
    );
    queue.fail("delayed", "delayed:lease:1", 1, "w", "fail-1", Some(true));
    queue.claim(
        WorkQueueClaimOptions::new("w")
            .command_id("claim-retry-too-soon")
            .requested_work_ids(["delayed"])
            .now_ms(20),
    );
    now.set(30);
    queue.claim(
        WorkQueueClaimOptions::new("w2")
            .command_id("claim-retry")
            .requested_work_ids(["delayed"]),
    );
    queue.submit(
        Payload { id: "manual" },
        WorkQueueSubmitOptions {
            work_id: Some("manual".to_owned()),
            ..WorkQueueSubmitOptions::default()
        },
    );
    queue.schedule("manual", 40, "schedule-manual");

    assert!(schedules.borrow().iter().any(|schedule| {
        schedule.ready_at_ms == 20
            && schedule.subject_refs.iter().any(|r| r.id == "delayed")
            && !schedule
                .metadata
                .as_ref()
                .unwrap()
                .contains_key("notBeforeMs")
    }));
    assert!(schedules.borrow().iter().any(|schedule| {
        schedule.ready_at_ms == 30
            && schedule
                .metadata
                .as_ref()
                .and_then(|m| m.get("scheduleKind"))
                .and_then(|v| v.as_str())
                == Some("retry-scheduled")
    }));
    assert!(schedules.borrow().iter().any(|schedule| {
        schedule
            .metadata
            .as_ref()
            .and_then(|m| m.get("scheduleKind"))
            .and_then(|v| v.as_str())
            == Some("lease-expiration")
    }));
    assert!(schedules.borrow().iter().any(|schedule| {
        schedule.ready_at_ms == 40
            && schedule.subject_refs.iter().any(|r| r.id == "manual")
            && schedule
                .metadata
                .as_ref()
                .and_then(|m| m.get("scheduleKind"))
                .and_then(|v| v.as_str())
                == Some("work-scheduled")
            && !schedule
                .metadata
                .as_ref()
                .unwrap()
                .contains_key("notBeforeMs")
    }));
}

#[test]
fn handoff_emits_candidates_but_stale_terminal_ready_does_not_mutate_queue() {
    let g = graph();
    let now = Rc::new(Cell::new(0));
    let queue = test_queue(&g, now.clone());
    let translator = work_queue_scheduled_readiness_projector(
        &g,
        WorkQueueScheduledReadinessOptions::new(vec![queue.records.clone()]),
    );
    let clocks: graphrefly::Node<ScheduledReadinessClock> = g.node(Vec::new(), |_| {});
    let readiness = scheduled_readiness_projector(
        &g,
        ScheduledReadinessOptions::new(vec![translator.readiness_schedules.clone()])
            .with_clocks(vec![clocks.clone()]),
    );
    let handoff = work_queue_readiness_handoff_projector(
        &g,
        WorkQueueReadinessHandoffOptions::new(
            vec![queue.records.clone()],
            vec![readiness.ready.clone()],
        )
        .with_overdue(vec![readiness.overdue.clone()]),
    );
    let candidates = collect_data::<WorkQueueReadinessCandidate>(&handoff.candidates);
    let statuses = collect_data(&handoff.status);
    let views = collect_data(&handoff.views);
    let records = collect_data::<WorkQueueRecord<Payload>>(&queue.records);

    queue.submit(
        Payload { id: "later" },
        WorkQueueSubmitOptions {
            work_id: Some("later".to_owned()),
            not_before_ms: Some(10),
            ..WorkQueueSubmitOptions::default()
        },
    );
    clocks.set(ScheduledReadinessClock {
        clock_id: "clock".to_owned(),
        now_ms: 10,
        source_refs: Vec::new(),
        metadata: None,
    });
    assert!(candidates.borrow().iter().any(|candidate| {
        candidate.work_id == "later"
            && candidate.candidate_kind == WorkQueueReadinessCandidateKind::ClaimEligible
    }));
    queue.cancel("later", "cancel-later", None);
    clocks.set(ScheduledReadinessClock {
        clock_id: "clock".to_owned(),
        now_ms: 11,
        source_refs: Vec::new(),
        metadata: None,
    });

    assert!(statuses
        .borrow()
        .iter()
        .any(|status| status.state == WorkQueueReadinessStatusState::Ignored));
    assert!(views
        .borrow()
        .last()
        .is_some_and(|view| view.candidates_by_id.is_empty()));
    assert!(!records
        .borrow()
        .iter()
        .any(|record| matches!(record, WorkQueueRecord::LeaseExpired { .. })));
}

#[test]
fn later_work_schedule_suppresses_older_ready_until_effective_time() {
    let g = graph();
    let now = Rc::new(Cell::new(0));
    let queue = test_queue(&g, now.clone());
    let translator = work_queue_scheduled_readiness_projector(
        &g,
        WorkQueueScheduledReadinessOptions::new(vec![queue.records.clone()]),
    );
    let clocks: graphrefly::Node<ScheduledReadinessClock> = g.node(Vec::new(), |_| {});
    let readiness = scheduled_readiness_projector(
        &g,
        ScheduledReadinessOptions::new(vec![translator.readiness_schedules.clone()])
            .with_clocks(vec![clocks.clone()]),
    );
    let handoff = work_queue_readiness_handoff_projector(
        &g,
        WorkQueueReadinessHandoffOptions::new(
            vec![queue.records.clone()],
            vec![readiness.ready.clone()],
        ),
    );
    let candidates = collect_data::<WorkQueueReadinessCandidate>(&handoff.candidates);
    let statuses = collect_data(&handoff.status);

    queue.submit(
        Payload { id: "later-again" },
        WorkQueueSubmitOptions {
            work_id: Some("later-again".to_owned()),
            not_before_ms: Some(10),
            ..WorkQueueSubmitOptions::default()
        },
    );
    queue.schedule("later-again", 40, "reschedule-later");
    clocks.set(ScheduledReadinessClock {
        clock_id: "clock".to_owned(),
        now_ms: 10,
        source_refs: Vec::new(),
        metadata: None,
    });

    assert!(candidates.borrow().is_empty());
    assert!(statuses.borrow().iter().any(|status| {
        status.state == WorkQueueReadinessStatusState::Ignored
            && status.details.as_deref() == Some("superseded-readiness")
    }));

    clocks.set(ScheduledReadinessClock {
        clock_id: "clock".to_owned(),
        now_ms: 40,
        source_refs: Vec::new(),
        metadata: None,
    });
    assert!(candidates
        .borrow()
        .iter()
        .any(|candidate| candidate.work_id == "later-again" && candidate.ready_at_ms == 40));
}

#[test]
fn same_kind_reschedule_ignores_non_active_ready_coordinate() {
    let g = graph();
    let now = Rc::new(Cell::new(0));
    let queue = test_queue(&g, now.clone());
    let translator = work_queue_scheduled_readiness_projector(
        &g,
        WorkQueueScheduledReadinessOptions::new(vec![queue.records.clone()]),
    );
    let clocks: graphrefly::Node<ScheduledReadinessClock> = g.node(Vec::new(), |_| {});
    let readiness = scheduled_readiness_projector(
        &g,
        ScheduledReadinessOptions::new(vec![translator.readiness_schedules.clone()])
            .with_clocks(vec![clocks.clone()]),
    );
    let handoff = work_queue_readiness_handoff_projector(
        &g,
        WorkQueueReadinessHandoffOptions::new(
            vec![queue.records.clone()],
            vec![readiness.ready.clone()],
        ),
    );
    let candidates = collect_data::<WorkQueueReadinessCandidate>(&handoff.candidates);
    let statuses = collect_data(&handoff.status);

    queue.submit(
        Payload { id: "resched" },
        WorkQueueSubmitOptions {
            work_id: Some("resched".to_owned()),
            ..WorkQueueSubmitOptions::default()
        },
    );
    queue.schedule("resched", 40, "schedule-late");
    queue.schedule("resched", 20, "schedule-earlier");
    clocks.set(ScheduledReadinessClock {
        clock_id: "clock".to_owned(),
        now_ms: 20,
        source_refs: Vec::new(),
        metadata: None,
    });
    clocks.set(ScheduledReadinessClock {
        clock_id: "clock".to_owned(),
        now_ms: 40,
        source_refs: Vec::new(),
        metadata: None,
    });

    assert!(candidates
        .borrow()
        .iter()
        .any(|candidate| candidate.work_id == "resched" && candidate.ready_at_ms == 20));
    assert!(!candidates
        .borrow()
        .iter()
        .any(|candidate| candidate.work_id == "resched" && candidate.ready_at_ms == 40));
    assert!(statuses.borrow().iter().any(|status| {
        status.work_id.as_deref() == Some("resched")
            && status.state == WorkQueueReadinessStatusState::Ignored
            && status.details.as_deref() == Some("superseded-readiness")
            && status.ready_at_ms == Some(40)
    }));
}

#[test]
fn handoff_keeps_same_work_id_isolated_by_queue() {
    let g = graph();
    let now = Rc::new(Cell::new(0));
    let queue_a = test_queue_named(&g, now.clone(), "qa", "work-a", "qa-admit");
    let queue_b = test_queue_named(&g, now.clone(), "qb", "work-b", "qb-admit");
    let translator = work_queue_scheduled_readiness_projector(
        &g,
        WorkQueueScheduledReadinessOptions::new(vec![
            queue_a.records.clone(),
            queue_b.records.clone(),
        ]),
    );
    let clocks: graphrefly::Node<ScheduledReadinessClock> = g.node(Vec::new(), |_| {});
    let readiness = scheduled_readiness_projector(
        &g,
        ScheduledReadinessOptions::new(vec![translator.readiness_schedules.clone()])
            .with_clocks(vec![clocks.clone()]),
    );
    let handoff = work_queue_readiness_handoff_projector(
        &g,
        WorkQueueReadinessHandoffOptions::new(
            vec![queue_a.records.clone(), queue_b.records.clone()],
            vec![readiness.ready.clone()],
        ),
    );
    let candidates = collect_data::<WorkQueueReadinessCandidate>(&handoff.candidates);

    queue_a.submit(
        Payload { id: "a" },
        WorkQueueSubmitOptions {
            work_id: Some("shared".to_owned()),
            not_before_ms: Some(10),
            ..WorkQueueSubmitOptions::default()
        },
    );
    queue_b.submit(
        Payload { id: "b" },
        WorkQueueSubmitOptions {
            work_id: Some("shared".to_owned()),
            not_before_ms: Some(20),
            ..WorkQueueSubmitOptions::default()
        },
    );
    clocks.set(ScheduledReadinessClock {
        clock_id: "clock".to_owned(),
        now_ms: 10,
        source_refs: Vec::new(),
        metadata: None,
    });
    clocks.set(ScheduledReadinessClock {
        clock_id: "clock".to_owned(),
        now_ms: 20,
        source_refs: Vec::new(),
        metadata: None,
    });

    assert!(candidates.borrow().iter().any(|candidate| {
        candidate.queue_id == "qa" && candidate.work_id == "shared" && candidate.ready_at_ms == 10
    }));
    assert!(candidates.borrow().iter().any(|candidate| {
        candidate.queue_id == "qb" && candidate.work_id == "shared" && candidate.ready_at_ms == 20
    }));
}

#[test]
fn lease_expiration_candidate_lowers_only_to_existing_expire_leases_command() {
    let g = graph();
    let now = Rc::new(Cell::new(0));
    let queue = test_queue(&g, now.clone());
    let translator = work_queue_scheduled_readiness_projector(
        &g,
        WorkQueueScheduledReadinessOptions::new(vec![queue.records.clone()]),
    );
    let clocks: graphrefly::Node<ScheduledReadinessClock> = g.node(Vec::new(), |_| {});
    let readiness = scheduled_readiness_projector(
        &g,
        ScheduledReadinessOptions::new(vec![translator.readiness_schedules.clone()])
            .with_clocks(vec![clocks.clone()]),
    );
    let handoff = work_queue_readiness_handoff_projector(
        &g,
        WorkQueueReadinessHandoffOptions::new(
            vec![queue.records.clone()],
            vec![readiness.ready.clone()],
        ),
    );
    let commands = work_queue_lease_expiration_command_projector::<Payload>(
        &g,
        WorkQueueLeaseExpirationCommandProjectorOptions::new(vec![handoff.candidates.clone()]),
    );
    let emitted_commands = collect_data::<WorkQueueCommand<Payload>>(&commands);
    let records = collect_data::<WorkQueueRecord<Payload>>(&queue.records);

    queue.submit(Payload { id: "lease" }, WorkQueueSubmitOptions::default());
    queue.claim(WorkQueueClaimOptions::new("w").command_id("claim-lease"));
    clocks.set(ScheduledReadinessClock {
        clock_id: "clock".to_owned(),
        now_ms: 30_000,
        source_refs: Vec::new(),
        metadata: None,
    });

    assert!(emitted_commands
        .borrow()
        .iter()
        .any(|command| matches!(command, WorkQueueCommand::ExpireLeases { work_ids, .. } if work_ids == &vec!["q:work:1".to_owned()])));
    assert!(!records
        .borrow()
        .iter()
        .any(|record| matches!(record, WorkQueueRecord::LeaseExpired { .. })));
}

#[test]
fn stale_lease_expiration_candidate_is_pruned_after_release() {
    let g = graph();
    let now = Rc::new(Cell::new(0));
    let queue = test_queue(&g, now.clone());
    let translator = work_queue_scheduled_readiness_projector(
        &g,
        WorkQueueScheduledReadinessOptions::new(vec![queue.records.clone()]),
    );
    let clocks: graphrefly::Node<ScheduledReadinessClock> = g.node(Vec::new(), |_| {});
    let readiness = scheduled_readiness_projector(
        &g,
        ScheduledReadinessOptions::new(vec![translator.readiness_schedules.clone()])
            .with_clocks(vec![clocks.clone()]),
    );
    let handoff = work_queue_readiness_handoff_projector(
        &g,
        WorkQueueReadinessHandoffOptions::new(
            vec![queue.records.clone()],
            vec![readiness.ready.clone()],
        ),
    );
    let candidates = collect_data::<WorkQueueReadinessCandidate>(&handoff.candidates);
    let views = collect_data(&handoff.views);

    queue.submit(Payload { id: "lease" }, WorkQueueSubmitOptions::default());
    queue.claim(WorkQueueClaimOptions::new("w").command_id("claim-lease"));
    clocks.set(ScheduledReadinessClock {
        clock_id: "clock".to_owned(),
        now_ms: 30_000,
        source_refs: Vec::new(),
        metadata: None,
    });
    assert!(candidates.borrow().iter().any(|candidate| {
        candidate.candidate_kind == WorkQueueReadinessCandidateKind::LeaseExpirationEligible
    }));

    queue.release("q:work:1", "q:work:1:lease:1", 1, "w", "release-lease");
    clocks.set(ScheduledReadinessClock {
        clock_id: "clock".to_owned(),
        now_ms: 30_001,
        source_refs: Vec::new(),
        metadata: None,
    });

    assert!(views
        .borrow()
        .last()
        .is_some_and(|view| view.candidates_by_id.is_empty()));
}
