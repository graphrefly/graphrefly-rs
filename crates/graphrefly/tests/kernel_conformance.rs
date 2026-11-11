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
    let unsub = node.subscribe(move |msgs: &[graphrefly::Message<graphrefly::AnyValue>]| {
        let handle = |m: &graphrefly::Message<graphrefly::AnyValue>| {
            sink.borrow_mut().push(format!("{m:?}"))
        };
        for m in msgs {
            handle(m);
        }
    });
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

/// C-42 — the message set is closed and nothing travels upstream (R-messages,
/// R-failure@3, R-subscription@3 by graphrefly:D801).
///
/// N's function sends arrays that only the kernel may send (START, PENDING, RESOLVED),
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
            vec![Message::Start],
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
    assert_eq!(rejections.get(), 4);
    assert_eq!(
        n.cache(),
        Some(10),
        "the DATA sent after the rejections is retained"
    );
    assert_eq!(kinds(&log), vec!["START", "DATA"]);

    log.borrow_mut().clear();
    a.set(2);
    assert_eq!(rejections.get(), 8);
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

    // The host writes only through a state's `set`, `invalidate`, `complete` and
    // `error` (graphrefly-rs:D3), so it has no way to send PENDING or RESOLVED.
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

// ── Phase 2: waves and arrays ──────────────────────────────────────────────

/// A sink recording each array it receives, as message kinds.
fn record_arrays<T: 'static>(node: &Node<T>) -> (Rc<RefCell<Vec<Vec<String>>>>, Unsub) {
    let log: Rc<RefCell<Vec<Vec<String>>>> = Rc::new(RefCell::new(Vec::new()));
    let sink = log.clone();
    let unsub = node.subscribe(move |msgs: &[Message<AnyValue>]| {
        sink.borrow_mut()
            .push(msgs.iter().map(|m| format!("{m:?}")).collect());
    });
    (log, unsub)
}

fn arrays(log: &Rc<RefCell<Vec<Vec<String>>>>) -> Vec<Vec<String>> {
    log.borrow().clone()
}

fn shape(kinds: &[&str]) -> Vec<String> {
    kinds.iter().map(|k| (*k).to_owned()).collect()
}

/// The DATA payloads (as i32) of every array a sink received.
fn record_values(node: &Node<i32>) -> (Rc<RefCell<Vec<Vec<i32>>>>, Unsub) {
    let log: Rc<RefCell<Vec<Vec<i32>>>> = Rc::new(RefCell::new(Vec::new()));
    let sink = log.clone();
    let unsub = node.subscribe(move |msgs: &[Message<AnyValue>]| {
        sink.borrow_mut().push(
            msgs.iter()
                .filter_map(|m| match m {
                    Message::Data(v) => v.downcast_ref::<i32>().copied(),
                    _ => None,
                })
                .collect(),
        );
    });
    (log, unsub)
}

/// C-12 (a) and (b) — occurrences stay DATA; RESOLVED only settles a node that sent
/// nothing. (c) is `c12c_function_sending_resolved_ends_with_error`.
#[test]
fn c12_occurrences_stay_data_and_kernel_resolved_settles_silence() {
    // (a) equal values are distinct occurrences, never turned into RESOLVED.
    let source = Node::<i32>::state_empty();
    let (log, _u) = record_arrays(&source);
    source.set(1);
    source.set(1);
    source.set(1);
    source.complete();
    assert_eq!(
        arrays(&log),
        vec![
            shape(&["START"]),
            shape(&["PENDING", "DATA"]),
            shape(&["PENDING", "DATA"]),
            shape(&["PENDING", "DATA"]),
            shape(&["PENDING", "COMPLETE"]),
        ]
    );

    // (b) a node that sends nothing for any input settles with exactly one RESOLVED in
    // each wave, and its dependent is never left pending.
    let input = Node::<i32>::state(0);
    let silent = Node::<i32>::derived(vec![input.erased()], |_ctx| {});
    let dependent_runs = Rc::new(Cell::new(0usize));
    let runs = dependent_runs.clone();
    let dependent = Node::<i32>::derived_opts(
        vec![silent.erased()],
        graphrefly::NodeOpts {
            partial: true,
            ..graphrefly::NodeOpts::default()
        },
        move |_ctx| runs.set(runs.get() + 1),
    );
    let (silent_log, _s) = record_arrays(&silent);
    let (_dependent_log, _d) = record_arrays(&dependent);
    silent_log.borrow_mut().clear();
    let runs_before = dependent_runs.get();
    for v in 1..=3 {
        input.set(v);
        assert_ne!(dependent.status(), Status::Dirty, "never left pending");
    }
    assert_eq!(
        arrays(&silent_log),
        vec![shape(&["PENDING", "RESOLVED"]); 3]
    );
    assert_eq!(
        dependent_runs.get(),
        runs_before + 3,
        "one settlement per wave"
    );
}

/// C-16 — a quiet node absorbs its input waves without leaving dependents pending, and
/// what it sends after returning is a later wave with the PENDING prefix.
#[test]
fn c16_quiet_node_absorbs_waves_and_publishes_later() {
    let a = Node::<i32>::state(0);
    let out: Rc<RefCell<Option<graphrefly::DeferredCtx>>> = Rc::new(RefCell::new(None));
    let slot = out.clone();
    let q = Node::<i32>::derived_opts(
        vec![a.erased()],
        graphrefly::NodeOpts {
            quiet: true,
            partial: true,
            ..graphrefly::NodeOpts::default()
        },
        move |ctx| {
            // Hold what arrives; send nothing during the run.
            let mut held = ctx
                .state_get::<Vec<i32>>()
                .map(|v| (*v).clone())
                .unwrap_or_default();
            held.extend(ctx.batch::<i32>(0).iter().map(|v| **v));
            ctx.state_set(held);
            *slot.borrow_mut() = Some(ctx.defer());
        },
    );
    let e_runs = Rc::new(Cell::new(0usize));
    let runs = e_runs.clone();
    let e = Node::<i32>::derived_opts(
        vec![q.erased()],
        graphrefly::NodeOpts {
            partial: true,
            ..graphrefly::NodeOpts::default()
        },
        move |_ctx| runs.set(runs.get() + 1),
    );
    let (q_log, _uq) = record_arrays(&q);
    let (_e_log, _ue) = record_arrays(&e);
    q_log.borrow_mut().clear();
    let e_before = e_runs.get();

    for v in 1..=3 {
        a.set(v);
        assert_ne!(e.status(), Status::Dirty, "E is never left waiting");
    }
    assert!(
        arrays(&q_log).is_empty(),
        "nothing reaches Q's sink, not even PENDING"
    );
    assert_eq!(
        e_runs.get(),
        e_before,
        "Q's input waves mark no dependent pending"
    );

    // Q sends what it held after its function returned: a later wave of its own.
    out.borrow().as_ref().expect("Q ran").down(vec![
        Message::Data(Rc::new(1i32) as AnyValue),
        Message::Data(Rc::new(2i32) as AnyValue),
        Message::Data(Rc::new(3i32) as AnyValue),
    ]);
    assert_eq!(
        arrays(&q_log),
        vec![shape(&["PENDING", "DATA", "DATA", "DATA"])]
    );
    assert_eq!(e_runs.get(), e_before + 1, "E runs once on Q's wave");

    // A new subscriber gets Q's last array in its START.
    let (late, _ul) = record_arrays(&q);
    assert_eq!(
        arrays(&late),
        vec![shape(&["START", "DATA", "DATA", "DATA"])]
    );
}

/// C-23 — ctx.deps is the function's only input: which dependency, the arrays, the
/// terminal and the latest value.
#[test]
fn c23_ctx_deps_gives_arrays_terminal_and_latest() {
    type Seen = Rc<RefCell<Vec<(Vec<Vec<i32>>, Vec<Vec<i32>>, Option<String>, Option<String>)>>>;
    fn terminal_name(t: Option<&graphrefly::DepTerminal>) -> Option<String> {
        t.map(|t| match t {
            graphrefly::DepTerminal::Complete => "COMPLETE".to_owned(),
            graphrefly::DepTerminal::Error(e) => format!("ERROR {e}"),
        })
    }
    let a = Node::<i32>::state_empty();
    let b = Node::<i32>::state_empty();
    let seen: Seen = Rc::new(RefCell::new(Vec::new()));
    let ids_ok = Rc::new(Cell::new(true));
    let n = {
        let seen = seen.clone();
        let ids_ok = ids_ok.clone();
        let (a_core, b_core) = (a.erased(), b.erased());
        Node::<i32>::derived_opts(
            vec![a.erased(), b.erased()],
            graphrefly::NodeOpts {
                partial: true,
                terminal_as_real_input: true,
                error_when_deps_error: false,
                complete_when_deps_complete: false,
                ..graphrefly::NodeOpts::default()
            },
            move |ctx| {
                ids_ok.set(ids_ok.get() && ctx.dep_is(0, &a_core) && ctx.dep_is(1, &b_core));
                seen.borrow_mut().push((
                    ctx.batches::<i32>(0)
                        .iter()
                        .map(|w| w.iter().map(|v| **v).collect())
                        .collect(),
                    ctx.batches::<i32>(1)
                        .iter()
                        .map(|w| w.iter().map(|v| **v).collect())
                        .collect(),
                    terminal_name(ctx.terminal(0)),
                    terminal_name(ctx.terminal(1)),
                ));
            },
        )
    };
    let _u = n.subscribe(|_| {});
    seen.borrow_mut().clear();

    // (a) only B was written: A's entry has no arrays; the latest of A stays absent.
    b.set(5);
    assert_eq!(seen.borrow().last().unwrap().0, Vec::<Vec<i32>>::new());
    assert_eq!(seen.borrow().last().unwrap().1, vec![vec![5]]);

    // (c) [DATA 1, DATA 2, INVALIDATE, DATA 3] in one array: split at its last
    // INVALIDATE, so N sees [3].
    graphrefly::batch(|_| {
        a.set(1);
        a.set(2);
        a.invalidate();
        a.set(3);
    });
    assert_eq!(seen.borrow().last().unwrap().0, vec![vec![3]]);

    // (e) terminals are not arrays: A's COMPLETE and B's ERROR in one wave.
    graphrefly::batch(|_| {
        a.complete();
        b.error("e");
    });
    let last = seen.borrow().last().cloned().unwrap();
    assert_eq!(last.0, Vec::<Vec<i32>>::new());
    assert_eq!(last.1, Vec::<Vec<i32>>::new());
    assert_eq!(last.2.as_deref(), Some("COMPLETE"));
    assert_eq!(last.3.as_deref(), Some("ERROR e"));
    assert!(ids_ok.get(), "each input says which dependency it is");

    // (b) a RESOLVED array is one empty array.
    let x = Node::<i32>::state(0);
    let silent = Node::<i32>::derived(vec![x.erased()], |_ctx| {});
    let empties: Rc<RefCell<Vec<Vec<Vec<i32>>>>> = Rc::new(RefCell::new(Vec::new()));
    let reader = {
        let empties = empties.clone();
        Node::<i32>::derived_opts(
            vec![silent.erased()],
            graphrefly::NodeOpts {
                partial: true,
                ..graphrefly::NodeOpts::default()
            },
            move |ctx| {
                empties.borrow_mut().push(
                    ctx.batches::<i32>(0)
                        .iter()
                        .map(|w| w.iter().map(|v| **v).collect())
                        .collect(),
                );
            },
        )
    };
    let _r = reader.subscribe(|_| {});
    x.set(1);
    assert_eq!(
        empties.borrow().last().cloned(),
        Some(vec![Vec::<i32>::new()])
    );

    // (d) None and an empty Vec are ordinary payloads.
    let opt = Node::<Option<i32>>::state_empty();
    let list = Node::<Vec<i32>>::state_empty();
    let payloads = Rc::new(Cell::new((0usize, 0usize)));
    let probe = {
        let payloads = payloads.clone();
        Node::<i32>::derived_opts(
            vec![opt.erased(), list.erased()],
            graphrefly::NodeOpts {
                partial: true,
                ..graphrefly::NodeOpts::default()
            },
            move |ctx| {
                let (o, l) = payloads.get();
                payloads.set((
                    o + ctx.batch::<Option<i32>>(0).len(),
                    l + ctx.batch::<Vec<i32>>(1).len(),
                ));
            },
        )
    };
    let _p = probe.subscribe(|_| {});
    opt.set(None);
    list.set(Vec::new());
    assert_eq!(payloads.get(), (1, 1));
    // (f) DATA SENTINEL cannot be written in Rust: `Message::Data` always carries a value.
}

/// C-41 — the snapshot is the node's last array that carried DATA.
#[test]
fn c41_snapshot_is_the_last_array_that_carried_data() {
    let a = Node::<i32>::state(0);
    let n = Node::<i32>::derived(vec![a.erased()], |ctx| match *ctx.data::<i32>(0).unwrap() {
        1 => ctx.down(vec![Message::Data(Rc::new(1i32) as AnyValue)]),
        2 => ctx.down(vec![
            Message::Data(Rc::new(2i32) as AnyValue),
            Message::Data(Rc::new(3i32) as AnyValue),
        ]),
        4 => ctx.down(vec![Message::Invalidate]),
        _ => {} // sends nothing: the kernel's RESOLVED
    });
    let (_first, u1) = record_arrays(&n);
    a.set(1);
    a.set(2);
    a.set(3);
    let (late, u2) = record_values(&n);
    assert_eq!(*late.borrow(), vec![vec![2, 3]], "[START, DATA 2, DATA 3]");

    // A new dependent's first run sees N's waves [[2, 3]].
    let first_waves: Rc<RefCell<Vec<Vec<Vec<i32>>>>> = Rc::new(RefCell::new(Vec::new()));
    let dependent = {
        let first_waves = first_waves.clone();
        Node::<i32>::derived(vec![n.erased()], move |ctx| {
            first_waves.borrow_mut().push(
                ctx.batches::<i32>(0)
                    .iter()
                    .map(|w| w.iter().map(|v| **v).collect())
                    .collect(),
            );
        })
    };
    let u3 = dependent.subscribe(|_| {});
    assert_eq!(
        first_waves.borrow().first().cloned(),
        Some(vec![vec![2, 3]])
    );

    // After N's INVALIDATE a new sink receives [START] only.
    a.set(4);
    let (after_invalidate, u4) = record_arrays(&n);
    assert_eq!(arrays(&after_invalidate), vec![shape(&["START"])]);

    // A batch's writes to one state are one array, so its snapshot carries both.
    let q = Node::<i32>::state_empty();
    graphrefly::batch(|_| {
        q.set(10);
        q.set(11);
    });
    let (q_late, uq) = record_values(&q);
    assert_eq!(*q_late.borrow(), vec![vec![10, 11]]);

    // Deactivation clears a computed node's values; a state keeps its own.
    for u in [u1, u2, u3, u4, uq] {
        u();
    }
    assert_eq!(
        n.cache(),
        None,
        "N has no retained values until it runs again"
    );
    assert_eq!(q.cache(), Some(11));
}

/// C-28, the array, prefix and rejection clauses (the sink-request, callback-batch and
/// sink-failure clauses follow the queued requests of phase 5).
#[test]
fn c28_arrays_stay_intact_with_one_kernel_prefix_each() {
    // A diamond A->B,C->D with two sinks on A; A sends three DATA in one array.
    let trigger = Node::<i32>::state(0);
    let a = Node::<i32>::derived_opts(
        vec![trigger.erased()],
        graphrefly::NodeOpts {
            partial: true,
            ..graphrefly::NodeOpts::default()
        },
        |ctx| {
            if ctx.batch::<i32>(0).iter().any(|v| **v == 1) {
                ctx.down(vec![
                    Message::Data(Rc::new(23i32) as AnyValue),
                    Message::Data(Rc::new(23i32) as AnyValue),
                    Message::Data(Rc::new(30i32) as AnyValue),
                ]);
            }
        },
    );
    let b = Node::<i32>::derived(vec![a.erased()], |ctx| {
        ctx.emit(*ctx.data::<i32>(0).unwrap())
    });
    let c = Node::<i32>::derived(vec![a.erased()], |ctx| {
        ctx.emit(*ctx.data::<i32>(0).unwrap())
    });
    let d_runs = Rc::new(RefCell::new(Vec::<(i32, i32)>::new()));
    let d = {
        let runs = d_runs.clone();
        Node::<i32>::derived(vec![b.erased(), c.erased()], move |ctx| {
            runs.borrow_mut()
                .push((*ctx.data::<i32>(0).unwrap(), *ctx.data::<i32>(1).unwrap()));
        })
    };
    let (s1, _u1) = record_arrays(&a);
    let (s2, _u2) = record_arrays(&a);
    let _ud = d.subscribe(|_| {});
    s1.borrow_mut().clear();
    s2.borrow_mut().clear();
    d_runs.borrow_mut().clear();
    trigger.set(1);
    let one = vec![shape(&["PENDING", "DATA", "DATA", "DATA"])];
    assert_eq!(arrays(&s1), one);
    assert_eq!(arrays(&s2), one);
    assert_eq!(
        *d_runs.borrow(),
        vec![(30, 30)],
        "D runs once after B and C settle"
    );

    // Separate calls stay separate arrays, each with its own prefix.
    let separate = Node::<i32>::derived(vec![trigger.erased()], |ctx| {
        if *ctx.data::<i32>(0).unwrap() == 2 {
            for v in [23i32, 23, 30] {
                ctx.down(vec![Message::Data(Rc::new(v) as AnyValue)]);
            }
        }
    });
    let (sep, _us) = record_arrays(&separate);
    sep.borrow_mut().clear();
    trigger.set(2);
    assert_eq!(arrays(&sep), vec![shape(&["PENDING", "DATA"]); 3]);

    // A host batch of three sets delivers one accumulated array at commit.
    let s = Node::<i32>::state(0);
    let (sb, _usb) = record_arrays(&s);
    sb.borrow_mut().clear();
    graphrefly::batch(|_| {
        s.set(23);
        s.set(23);
        s.set(30);
        assert!(arrays(&sb).is_empty(), "nothing before the commit");
    });
    assert_eq!(
        arrays(&sb),
        vec![shape(&["PENDING", "DATA", "DATA", "DATA"])]
    );

    // [DATA 1, INVALIDATE, DATA 2, DATA 3] goes out as [PENDING, INVALIDATE] then
    // [PENDING, DATA 2, DATA 3].
    let split = Node::<i32>::derived(vec![trigger.erased()], |ctx| {
        if *ctx.data::<i32>(0).unwrap() == 3 {
            ctx.down(vec![
                Message::Data(Rc::new(1i32) as AnyValue),
                Message::Invalidate,
                Message::Data(Rc::new(2i32) as AnyValue),
                Message::Data(Rc::new(3i32) as AnyValue),
            ]);
        } else {
            ctx.emit(0);
        }
    });
    let (sp, _usp) = record_arrays(&split);
    sp.borrow_mut().clear();
    trigger.set(3);
    assert_eq!(
        arrays(&sp),
        vec![
            shape(&["PENDING", "INVALIDATE"]),
            shape(&["PENDING", "DATA", "DATA"])
        ]
    );

    // In a host batch, a DATA after a staged COMPLETE is rejected and leaves the
    // earlier staging as it was; the batch commits [PENDING, DATA, COMPLETE].
    let t = Node::<i32>::state_empty();
    let (tb, _ut) = record_arrays(&t);
    tb.borrow_mut().clear();
    graphrefly::batch(|_| {
        t.set(1);
        t.complete();
        let rejected = catch_unwind(AssertUnwindSafe(|| t.set(2))).is_err();
        assert!(rejected, "a DATA staged after a terminal is rejected");
    });
    assert_eq!(arrays(&tb), vec![shape(&["PENDING", "DATA", "COMPLETE"])]);
}

/// C-44, the value and quiet-node clauses (the terminal and manual-INVALIDATE clauses
/// follow the policies of phase 3).
#[test]
fn c44_kernel_adds_only_the_settling_resolved_and_quiet_nodes_hold_no_wave() {
    let a = Node::<i32>::state(0);
    let f = Node::<i32>::derived(vec![a.erased()], |ctx| {
        let v = *ctx.data::<i32>(0).unwrap();
        if v % 2 == 0 {
            ctx.emit(v);
        }
    });
    let q_out: Rc<RefCell<Option<graphrefly::DeferredCtx>>> = Rc::new(RefCell::new(None));
    let q = {
        let slot = q_out.clone();
        Node::<i32>::derived_opts(
            vec![a.erased()],
            graphrefly::NodeOpts {
                quiet: true,
                ..graphrefly::NodeOpts::default()
            },
            move |ctx| {
                *slot.borrow_mut() = Some(ctx.defer());
                if ctx.batch::<i32>(0).iter().any(|v| **v == 2) {
                    ctx.emit(200);
                }
            },
        )
    };
    let d_runs = Rc::new(RefCell::new(Vec::<(i32, Option<i32>)>::new()));
    let d = {
        let runs = d_runs.clone();
        Node::<i32>::derived_opts(
            vec![a.erased(), q.erased()],
            graphrefly::NodeOpts {
                partial: true,
                ..graphrefly::NodeOpts::default()
            },
            move |ctx| {
                let pair = (
                    ctx.data::<i32>(0).map_or(-1, |v| *v),
                    ctx.data::<i32>(1).map(|v| *v),
                );
                runs.borrow_mut().push(pair);
                // D retains a value, so an INVALIDATE is effective at D (R-invalidation).
                ctx.emit(pair.0);
            },
        )
    };
    let (f_log, _uf) = record_arrays(&f);
    let (q_log, _uq) = record_arrays(&q);
    let (d_log, _ud) = record_arrays(&d);
    f_log.borrow_mut().clear();
    q_log.borrow_mut().clear();
    d_log.borrow_mut().clear();
    d_runs.borrow_mut().clear();

    // A = 3: F was pending and sent nothing, so it gets the kernel's RESOLVED; D runs once.
    a.set(3);
    assert_eq!(arrays(&f_log), vec![shape(&["PENDING", "RESOLVED"])]);
    assert_eq!(d_runs.borrow().len(), 1);

    // A = 2: Q is never marked pending; D runs in A's wave without waiting for Q, and
    // Q's output is a later wave of its own, which runs D again.
    d_runs.borrow_mut().clear();
    a.set(2);
    assert_eq!(*d_runs.borrow(), vec![(2, None), (2, Some(200))]);
    assert_eq!(arrays(&q_log), vec![shape(&["PENDING", "DATA"])]);

    // A later ctx.down through Q's kept ctx is a new wave too.
    d_runs.borrow_mut().clear();
    q_out
        .borrow()
        .as_ref()
        .unwrap()
        .down(vec![Message::Data(Rc::new(201i32) as AnyValue)]);
    assert_eq!(*d_runs.borrow(), vec![(2, Some(201))]);

    // A = 5: Q sends nothing and no RESOLVED is added for it.
    q_log.borrow_mut().clear();
    a.set(5);
    assert!(arrays(&q_log).is_empty());

    // Invalidating A reaches Q's sink and D's at once, in A's wave, each as
    // [PENDING, INVALIDATE].
    q_log.borrow_mut().clear();
    d_log.borrow_mut().clear();
    a.invalidate();
    assert_eq!(arrays(&q_log), vec![shape(&["PENDING", "INVALIDATE"])]);
    assert_eq!(arrays(&d_log), vec![shape(&["PENDING", "INVALIDATE"])]);
}
