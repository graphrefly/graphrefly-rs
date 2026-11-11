//! D797 kernel conformance — the Rust arm of the kernel scenarios in
//! `~/src/graphrefly/spec/conformance.jsonl`, as activated by graphrefly:D798 and
//! revised by graphrefly:D799.
//!
//! Each test names the scenario and the clauses it checks. Scenarios land phase by
//! phase with graphrefly-rs:D797-KERNEL-MIGRATION-RS; a scenario is reported as
//! passing only once every one of its clauses is checked here. Public API only.
//!
//! Rust reading of "rejected" for the retired message types (graphrefly-rs:D1 (6)):
//! `Message` is a closed enum, so PULL, PAUSE, RESUME and TEARDOWN cannot be
//! constructed in process at all; there is no upward call on `Ctx` or `Node`; and the
//! crate's wire codec carries no `Message` or `Wave`, so none can arrive from a peer.
//! What a node can still write, PENDING and RESOLVED, is rejected by `ctx.down` at
//! run time.

use std::cell::{Cell, RefCell};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;

use graphrefly::{AnyValue, Message, Node, Status};

type Log = Rc<RefCell<Vec<String>>>;
type Unsub = Box<dyn FnOnce()>;

/// Subscribe a recorder of message kinds; keep the returned handle alive.
fn record<T: 'static>(node: &Node<T>) -> (Log, Unsub) {
    let log: Log = Rc::new(RefCell::new(Vec::new()));
    let sink = log.clone();
    let unsub =
        node.subscribe(move |m: &Message<AnyValue>| sink.borrow_mut().push(format!("{m:?}")));
    (log, unsub)
}

fn kinds(log: &Log) -> Vec<String> {
    log.borrow().clone()
}

/// The closed message set, by exhaustive match: adding or removing a variant breaks
/// this function's compilation.
fn kind_of<T>(m: &Message<T>) -> &'static str {
    match m {
        Message::Start => "START",
        Message::Pending => "PENDING",
        Message::Data(_) => "DATA",
        Message::Resolved => "RESOLVED",
        Message::Invalidate => "INVALIDATE",
        Message::Complete => "COMPLETE",
        Message::Error(_) => "ERROR",
    }
}

/// C-26 — the message set is closed: PULL, PAUSE, RESUME and TEARDOWN are not messages.
///
/// In Rust the four are unrepresentable (no variant), so no node can send them and
/// none reaches a sink. The seven that exist are START, PENDING, DATA, RESOLVED,
/// INVALIDATE, COMPLETE and ERROR, and their `Debug` tags are those names.
#[test]
fn c26_message_set_is_closed() {
    let all: Vec<Message<i32>> = vec![
        Message::Start,
        Message::Pending,
        Message::Data(1),
        Message::Resolved,
        Message::Invalidate,
        Message::Complete,
        Message::Error("e".into()),
    ];
    let names: Vec<&str> = all.iter().map(kind_of).collect();
    assert_eq!(
        names,
        vec![
            "START",
            "PENDING",
            "DATA",
            "RESOLVED",
            "INVALIDATE",
            "COMPLETE",
            "ERROR"
        ]
    );
    let debug: Vec<String> = all.iter().map(|m| format!("{m:?}")).collect();
    assert_eq!(debug, names);

    // Control is ordinary DATA on a declared edge (R-no-imperative): a controller writes
    // a state node the target declares as a dependency.
    let control = Node::<bool>::state(false);
    let source = Node::<i32>::state(1);
    let target = Node::<i32>::derived(vec![source.erased(), control.erased()], |ctx| {
        let held = ctx.data::<bool>(1).is_some_and(|held| *held);
        if !held {
            if let Some(v) = ctx.data::<i32>(0) {
                ctx.emit(*v);
            }
        }
    });
    let (log, _u) = record(&target);
    assert_eq!(target.cache(), Some(1));
    log.borrow_mut().clear();
    control.set(true);
    source.set(2);
    assert_eq!(target.cache(), Some(1));
    control.set(false);
    assert_eq!(target.cache(), Some(2));
    assert!(kinds(&log).iter().all(|k| k != "ERROR"));
}

/// C-42 — the message set is closed and nothing travels upstream.
///
/// N's function sends arrays that only the kernel may send (PENDING, RESOLVED),
/// catching each rejection, and then sends DATA: nothing of a rejected array is
/// applied, N's retained value is the DATA sent afterwards, and the sink receives
/// none of the rejected arrays. Uncaught, a rejection is N's ERROR. (PAUSE, RESUME,
/// PULL, TEARDOWN and unknown types cannot be written in Rust; there is no upward
/// call on ctx: see the module doc.)
#[test]
fn c42_rejected_output_is_applied_nowhere_and_uncaught_is_error() {
    let a = Node::<i32>::state(1);
    let rejections = Rc::new(Cell::new(0usize));
    let rejections_in_fn = rejections.clone();
    let n = Node::<i32>::derived(vec![a.erased()], move |ctx| {
        for wave in [
            vec![Message::Pending],
            vec![Message::Resolved],
            vec![Message::Data(Rc::new(99i32) as AnyValue), Message::Pending],
        ] {
            let rejected = catch_unwind(AssertUnwindSafe(|| ctx.down(wave))).is_err();
            assert!(rejected, "ctx.down must reject kernel-only output");
            rejections_in_fn.set(rejections_in_fn.get() + 1);
        }
        let v = ctx.data::<i32>(0).map_or(0, |v| *v);
        ctx.emit(v * 10);
    });
    let (log, _u) = record(&n);
    assert_eq!(rejections.get(), 3);
    assert_eq!(
        n.cache(),
        Some(10),
        "the DATA sent after the rejections is retained"
    );
    assert_eq!(kinds(&log), vec!["START", "DATA"]);

    log.borrow_mut().clear();
    a.set(2);
    assert_eq!(rejections.get(), 6);
    assert_eq!(n.cache(), Some(20));
    assert_eq!(
        kinds(&log),
        vec!["PENDING", "DATA"],
        "no rejected array reached the sink"
    );

    // Uncaught, the rejection is the node's ERROR.
    let b = Node::<i32>::state(1);
    let m = Node::<i32>::derived(vec![b.erased()], |ctx| {
        ctx.down(vec![Message::Pending]);
    });
    let (log, _u) = record(&m);
    assert_eq!(m.status(), Status::Errored);
    assert_eq!(kinds(&log), vec!["START", "ERROR"]);

    // The host's own down on a node is held to the same rule.
    let s = Node::<i32>::state(1);
    let host_rejected = catch_unwind(AssertUnwindSafe(|| s.down(vec![Message::Resolved]))).is_err();
    assert!(host_rejected);
    assert_eq!(s.cache(), Some(1));
    assert_eq!(s.status(), Status::Settled);
}

/// C-12 (c) — a node whose function sends RESOLVED: the run is rejected and ends the
/// node with ERROR, since RESOLVED is the kernel's alone (graphrefly:D799).
#[test]
fn c12c_function_sending_resolved_ends_with_error() {
    let input = Node::<i32>::state(1);
    let sends_resolved = Rc::new(Cell::new(false));
    let flag = sends_resolved.clone();
    let n = Node::<i32>::derived(vec![input.erased()], move |ctx| {
        if flag.get() {
            ctx.down(vec![Message::Resolved]);
        } else if let Some(v) = ctx.data::<i32>(0) {
            ctx.emit(*v);
        }
    });
    let (log, _u) = record(&n);
    assert_eq!(n.cache(), Some(1));
    log.borrow_mut().clear();

    sends_resolved.set(true);
    input.set(2);
    assert_eq!(n.status(), Status::Errored);
    let seen = kinds(&log);
    assert_eq!(seen.last().map(String::as_str), Some("ERROR"));
    assert!(
        !seen.iter().any(|k| k == "RESOLVED"),
        "RESOLVED never left the node: {seen:?}"
    );
}
