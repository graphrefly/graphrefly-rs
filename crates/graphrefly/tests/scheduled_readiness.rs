use std::cell::RefCell;
use std::rc::Rc;

use graphrefly::{
    graph, parse_scheduled_readiness_requested, scheduled_readiness_projector, DataIssue, Message,
    ScheduledReadinessClock, ScheduledReadinessOptions, ScheduledReadinessReady,
    ScheduledReadinessRequested, ScheduledReadinessStatus, ScheduledReadinessStatusState,
    SourceRef,
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

fn schedule(schedule_id: &str, ready_at_ms: u64) -> ScheduledReadinessRequested {
    ScheduledReadinessRequested {
        schedule_id: schedule_id.to_owned(),
        subject_refs: vec![SourceRef::new("test-subject", schedule_id)],
        ready_at_ms,
        deadline_ms: None,
        reason: None,
        policy_refs: Vec::new(),
        source_refs: Vec::new(),
        metadata: None,
    }
}

fn source_ref(kind: &str, id: &str) -> SourceRef {
    SourceRef::new(kind, id)
}

#[test]
fn schedule_and_clock_emit_pending_then_ready() {
    let g = graph();
    let schedules: graphrefly::Node<ScheduledReadinessRequested> = g.node(Vec::new(), |_| {});
    let clocks: graphrefly::Node<ScheduledReadinessClock> = g.node(Vec::new(), |_| {});
    let bundle = scheduled_readiness_projector(
        &g,
        ScheduledReadinessOptions::new(vec![schedules.clone()]).with_clocks(vec![clocks.clone()]),
    );
    let pending = collect_data(&bundle.pending);
    let ready = collect_data(&bundle.ready);
    let status = collect_data::<ScheduledReadinessStatus>(&bundle.status);

    schedules.set(schedule("s1", 10));
    assert_eq!(
        pending
            .borrow()
            .last()
            .map(|item| item.schedule_id.as_str()),
        Some("s1")
    );
    assert!(ready.borrow().is_empty());

    clocks.set(ScheduledReadinessClock {
        clock_id: "clock".to_owned(),
        now_ms: 10,
        source_refs: Vec::new(),
        metadata: None,
    });
    assert_eq!(
        ready.borrow().last().map(|item| item.schedule_id.as_str()),
        Some("s1")
    );
    assert!(status.borrow().iter().any(|item| {
        item.schedule_id == "s1"
            && item.state == ScheduledReadinessStatusState::Ready
            && item.now_ms == Some(10)
    }));
}

#[test]
fn deadline_overdue_is_visibility_only_and_ready_still_emits() {
    let g = graph();
    let schedules: graphrefly::Node<ScheduledReadinessRequested> = g.node(Vec::new(), |_| {});
    let clocks: graphrefly::Node<ScheduledReadinessClock> = g.node(Vec::new(), |_| {});
    let bundle = scheduled_readiness_projector(
        &g,
        ScheduledReadinessOptions::new(vec![schedules.clone()]).with_clocks(vec![clocks.clone()]),
    );
    let ready = collect_data::<ScheduledReadinessReady>(&bundle.ready);
    let overdue = collect_data(&bundle.overdue);

    let mut requested = schedule("deadline", 10);
    requested.deadline_ms = Some(20);
    schedules.set(requested);
    clocks.set(ScheduledReadinessClock {
        clock_id: "clock".to_owned(),
        now_ms: 25,
        source_refs: Vec::new(),
        metadata: None,
    });

    assert_eq!(ready.borrow().len(), 1);
    assert_eq!(overdue.borrow().len(), 1);
    assert_eq!(overdue.borrow()[0].deadline_ms, 20);
}

#[test]
fn duplicate_schedules_are_idempotent_and_conflicts_issue() {
    let g = graph();
    let schedules: graphrefly::Node<ScheduledReadinessRequested> = g.node(Vec::new(), |_| {});
    let clocks: graphrefly::Node<ScheduledReadinessClock> = g.node(Vec::new(), |_| {});
    let bundle = scheduled_readiness_projector(
        &g,
        ScheduledReadinessOptions::new(vec![schedules.clone()]).with_clocks(vec![clocks.clone()]),
    );
    let ready = collect_data::<ScheduledReadinessReady>(&bundle.ready);
    let issues = collect_data::<DataIssue>(&bundle.issues);

    schedules.set(schedule("same", 5));
    schedules.set(schedule("same", 5));
    clocks.set(ScheduledReadinessClock {
        clock_id: "clock".to_owned(),
        now_ms: 5,
        source_refs: Vec::new(),
        metadata: None,
    });
    assert_eq!(ready.borrow().len(), 1);

    schedules.set(schedule("same", 8));
    assert!(issues
        .borrow()
        .iter()
        .any(|issue| issue.code == "scheduled-readiness-schedule-conflict"));
}

#[test]
fn duplicate_schedules_compare_canonical_material() {
    let g = graph();
    let schedules: graphrefly::Node<ScheduledReadinessRequested> = g.node(Vec::new(), |_| {});
    let clocks: graphrefly::Node<ScheduledReadinessClock> = g.node(Vec::new(), |_| {});
    let bundle = scheduled_readiness_projector(
        &g,
        ScheduledReadinessOptions::new(vec![schedules.clone()]).with_clocks(vec![clocks.clone()]),
    );
    let issues = collect_data::<DataIssue>(&bundle.issues);
    let views = collect_data(&bundle.views);

    let mut first = schedule("canonical", 5);
    first.subject_refs = vec![
        source_ref("subject", "b"),
        source_ref("subject", "a"),
        source_ref("subject", "a"),
    ];
    first.metadata = Some(
        [
            ("token".to_owned(), serde_json::json!("drop-me")),
            ("visible".to_owned(), serde_json::json!("keep")),
        ]
        .into_iter()
        .collect(),
    );
    schedules.set(first);

    let mut replay = schedule("canonical", 5);
    replay.subject_refs = vec![source_ref("subject", "a"), source_ref("subject", "b")];
    replay.metadata = Some(
        [("visible".to_owned(), serde_json::json!("keep"))]
            .into_iter()
            .collect(),
    );
    schedules.set(replay);

    assert!(!issues
        .borrow()
        .iter()
        .any(|issue| issue.code == "scheduled-readiness-schedule-conflict"));
    let last_view = views.borrow().last().cloned().expect("view emitted");
    let retained = last_view
        .schedules_by_id
        .get("canonical")
        .expect("schedule retained");
    assert_eq!(
        retained
            .subject_refs
            .iter()
            .map(|source_ref| source_ref.id.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b"]
    );
    assert!(!retained
        .metadata
        .as_ref()
        .is_some_and(|metadata| metadata.contains_key("token")));
}

#[test]
fn conflicts_with_same_details_remain_distinct_by_source() {
    let g = graph();
    let schedules: graphrefly::Node<ScheduledReadinessRequested> = g.node(Vec::new(), |_| {});
    let clocks: graphrefly::Node<ScheduledReadinessClock> = g.node(Vec::new(), |_| {});
    let bundle = scheduled_readiness_projector(
        &g,
        ScheduledReadinessOptions::new(vec![schedules.clone()]).with_clocks(vec![clocks.clone()]),
    );
    let issues = collect_data::<DataIssue>(&bundle.issues);

    schedules.set(schedule("same-a", 5));
    schedules.set(schedule("same-a", 8));
    schedules.set(schedule("same-b", 5));
    schedules.set(schedule("same-b", 8));

    assert_eq!(
        issues
            .borrow()
            .iter()
            .filter(|issue| issue.code == "scheduled-readiness-schedule-conflict")
            .count(),
        2
    );
}

#[test]
fn out_of_order_clock_is_visible_issue_and_ignored() {
    let g = graph();
    let schedules: graphrefly::Node<ScheduledReadinessRequested> = g.node(Vec::new(), |_| {});
    let clocks: graphrefly::Node<ScheduledReadinessClock> = g.node(Vec::new(), |_| {});
    let bundle = scheduled_readiness_projector(
        &g,
        ScheduledReadinessOptions::new(vec![schedules.clone()]).with_clocks(vec![clocks.clone()]),
    );
    let issues = collect_data::<DataIssue>(&bundle.issues);
    let status = collect_data::<ScheduledReadinessStatus>(&bundle.status);

    schedules.set(schedule("rollback", 10));
    clocks.set(ScheduledReadinessClock {
        clock_id: "clock".to_owned(),
        now_ms: 15,
        source_refs: Vec::new(),
        metadata: None,
    });
    clocks.set(ScheduledReadinessClock {
        clock_id: "clock".to_owned(),
        now_ms: 4,
        source_refs: Vec::new(),
        metadata: None,
    });

    assert!(issues
        .borrow()
        .iter()
        .any(|issue| issue.code == "scheduled-readiness-clock-rollback"));
    assert!(status
        .borrow()
        .iter()
        .any(|item| item.state == ScheduledReadinessStatusState::Ready));
}

#[test]
fn json_parser_rejects_stale_subject_ref_and_not_before_aliases() {
    let value = serde_json::json!({
        "kind": "scheduled-readiness-requested",
        "scheduleId": "s1",
        "subjectRefs": [],
        "subjectRef": { "kind": "stale", "id": "x" },
        "readyAtMs": 1
    });
    assert_eq!(
        parse_scheduled_readiness_requested(&value)
            .unwrap_err()
            .code,
        "scheduled-readiness-malformed-schedule"
    );

    let value = serde_json::json!({
        "kind": "scheduled-readiness-requested",
        "scheduleId": "s2",
        "subjectRefs": [],
        "notBeforeMs": 1,
        "readyAtMs": 1
    });
    assert_eq!(
        parse_scheduled_readiness_requested(&value)
            .unwrap_err()
            .code,
        "scheduled-readiness-malformed-schedule"
    );
}
