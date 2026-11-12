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

use graphrefly::{AnyValue, Message, Node};

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
    let g = graphrefly::graph();
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
    let control = g.state::<bool>(false);
    let source = g.state::<i32>(1);
    let target = g.node::<i32, _>(vec![source.erased(), control.erased()], |ctx| {
        let held = ctx.dep(1).value::<bool>().is_some_and(|held| *held);
        if !held {
            if let Some(v) = ctx.dep(0).value::<i32>() {
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
    let g = graphrefly::graph();
    let a = g.state::<i32>(1);
    let rejections = Rc::new(Cell::new(0usize));
    let rejections_in_fn = rejections.clone();
    let n = g.node::<i32, _>(vec![a.erased()], move |ctx| {
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
        let v = ctx.dep(0).value::<i32>().map_or(0, |v| *v);
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
    let b = g.state::<i32>(1);
    let m = g.node::<i32, _>(vec![b.erased()], |ctx| {
        ctx.down(vec![Message::Pending]);
    });
    let (log, _u) = record(&m);
    assert_eq!(m.status().output, graphrefly::OutputStatus::Errored);
    assert_eq!(kinds(&log), vec!["START", "ERROR"]);

    // The host writes only through a state's `set`, `invalidate`, `complete` and
    // `error` (graphrefly-rs:D3), so it has no way to send PENDING or RESOLVED.
}

/// C-12 (c) — a node whose function sends RESOLVED: the run is rejected and ends the
/// node with ERROR, since RESOLVED is the kernel's alone (graphrefly:D799).
#[test]
fn c12c_function_sending_resolved_ends_with_error() {
    let g = graphrefly::graph();
    let input = g.state::<i32>(1);
    let sends_resolved = Rc::new(Cell::new(false));
    let flag = sends_resolved.clone();
    let n = g.node::<i32, _>(vec![input.erased()], move |ctx| {
        if flag.get() {
            ctx.down(vec![Message::Resolved]);
        } else if let Some(v) = ctx.dep(0).value::<i32>() {
            ctx.emit(*v);
        }
    });
    let (log, _u) = record(&n);
    assert_eq!(n.cache(), Some(1));
    log.borrow_mut().clear();

    sends_resolved.set(true);
    input.set(2);
    assert_eq!(n.status().output, graphrefly::OutputStatus::Errored);
    let seen = kinds(&log);
    assert_eq!(seen.last().map(String::as_str), Some("ERROR"));
    assert!(
        !seen.iter().any(|k| k == "RESOLVED"),
        "RESOLVED never left the node: {seen:?}"
    );
}

// ── Phase 2: waves and arrays ──────────────────────────────────────────────

/// A sink recording each array it receives, as message kinds.
/// A kernel node's function that forwards its one dependency's value, once it has one
/// (the kernel has no first-run gate, review Q1).
fn forward_i32(ctx: &graphrefly::Ctx) {
    if let Some(v) = ctx.dep(0).value::<i32>() {
        ctx.emit(*v);
    }
}

/// A kernel node's function that forwards each array of its one dependency's values.
fn forward_arrays_i32(ctx: &graphrefly::Ctx) {
    for wave in ctx.dep(0).waves::<i32>() {
        ctx.down(
            wave.iter()
                .map(|v| Message::Data(Rc::new(**v) as graphrefly::AnyValue))
                .collect(),
        );
    }
}

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
    let g = graphrefly::graph();
    // (a) equal values are distinct occurrences, never turned into RESOLVED.
    let source = g.state_empty::<i32>();
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
    let input = g.state::<i32>(0);
    let silent = g.node::<i32, _>(vec![input.erased()], |_ctx| {});
    let dependent_runs = Rc::new(Cell::new(0usize));
    let runs = dependent_runs.clone();
    let dependent = g.node_opts::<i32, _>(
        vec![silent.erased()],
        move |_ctx| runs.set(runs.get() + 1),
        graphrefly::GraphNodeOpts::default().with_node(graphrefly::NodeOpts {
            ..graphrefly::NodeOpts::default()
        }),
    );
    let (silent_log, _s) = record_arrays(&silent);
    let (_dependent_log, _d) = record_arrays(&dependent);
    silent_log.borrow_mut().clear();
    let runs_before = dependent_runs.get();
    for v in 1..=3 {
        input.set(v);
        assert_ne!(
            dependent.status().cache,
            graphrefly::CacheStatus::Stale,
            "never left pending"
        );
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
    let g = graphrefly::graph();
    let a = g.state::<i32>(0);
    let out: Rc<RefCell<Option<graphrefly::DeferredCtx>>> = Rc::new(RefCell::new(None));
    let slot = out.clone();
    let q = g.node_opts::<i32, _>(
        vec![a.erased()],
        move |ctx| {
            // Hold what arrives; send nothing during the run.
            let mut held = ctx
                .state_get::<Vec<i32>>()
                .map(|v| (*v).clone())
                .unwrap_or_default();
            held.extend(ctx.dep(0).values::<i32>().iter().map(|v| **v));
            ctx.state_set(held);
            *slot.borrow_mut() = Some(ctx.defer());
        },
        graphrefly::GraphNodeOpts::default().with_node(graphrefly::NodeOpts {
            quiet: true,
            ..graphrefly::NodeOpts::default()
        }),
    );
    let e_runs = Rc::new(Cell::new(0usize));
    let runs = e_runs.clone();
    let e = g.node_opts::<i32, _>(
        vec![q.erased()],
        move |_ctx| runs.set(runs.get() + 1),
        graphrefly::GraphNodeOpts::default().with_node(graphrefly::NodeOpts {
            ..graphrefly::NodeOpts::default()
        }),
    );
    let (q_log, _uq) = record_arrays(&q);
    let (_e_log, _ue) = record_arrays(&e);
    q_log.borrow_mut().clear();
    let e_before = e_runs.get();

    for v in 1..=3 {
        a.set(v);
        assert_ne!(
            e.status().cache,
            graphrefly::CacheStatus::Stale,
            "E is never left waiting"
        );
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
    let g = graphrefly::graph();
    type Seen = Rc<RefCell<Vec<(Vec<Vec<i32>>, Vec<Vec<i32>>, Option<String>, Option<String>)>>>;
    fn terminal_name(t: Option<&graphrefly::DepTerminal>) -> Option<String> {
        t.map(|t| match t {
            graphrefly::DepTerminal::Complete => "COMPLETE".to_owned(),
            graphrefly::DepTerminal::Error(e) => format!("ERROR {e}"),
        })
    }
    let a = g.state_empty::<i32>();
    let b = g.state_empty::<i32>();
    let seen: Seen = Rc::new(RefCell::new(Vec::new()));
    let ids_ok = Rc::new(Cell::new(true));
    let n = {
        let seen = seen.clone();
        let ids_ok = ids_ok.clone();
        let (a_core, b_core) = (a.erased(), b.erased());
        g.node_opts::<i32, _>(
            vec![a.erased(), b.erased()],
            move |ctx| {
                ids_ok.set(ids_ok.get() && ctx.dep(0).is(&a_core) && ctx.dep(1).is(&b_core));
                seen.borrow_mut().push((
                    ctx.dep(0)
                        .waves::<i32>()
                        .iter()
                        .map(|w| w.iter().map(|v| **v).collect())
                        .collect(),
                    ctx.dep(1)
                        .waves::<i32>()
                        .iter()
                        .map(|w| w.iter().map(|v| **v).collect())
                        .collect(),
                    terminal_name(ctx.dep(0).terminal()),
                    terminal_name(ctx.dep(1).terminal()),
                ));
            },
            graphrefly::GraphNodeOpts::default().with_node(graphrefly::NodeOpts {
                error: graphrefly::Policy::Manual,
                complete: graphrefly::Policy::Manual,
                ..graphrefly::NodeOpts::default()
            }),
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
    g.batch(|_| {
        a.set(1);
        a.set(2);
        a.invalidate();
        a.set(3);
    });
    assert_eq!(seen.borrow().last().unwrap().0, vec![vec![3]]);

    // (e) terminals are not arrays: A's COMPLETE and B's ERROR in one wave.
    g.batch(|_| {
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
    let x = g.state::<i32>(0);
    let silent = g.node::<i32, _>(vec![x.erased()], |_ctx| {});
    let empties: Rc<RefCell<Vec<Vec<Vec<i32>>>>> = Rc::new(RefCell::new(Vec::new()));
    let reader = {
        let empties = empties.clone();
        g.node_opts::<i32, _>(
            vec![silent.erased()],
            move |ctx| {
                empties.borrow_mut().push(
                    ctx.dep(0)
                        .waves::<i32>()
                        .iter()
                        .map(|w| w.iter().map(|v| **v).collect())
                        .collect(),
                );
            },
            graphrefly::GraphNodeOpts::default().with_node(graphrefly::NodeOpts {
                ..graphrefly::NodeOpts::default()
            }),
        )
    };
    let _r = reader.subscribe(|_| {});
    x.set(1);
    assert_eq!(
        empties.borrow().last().cloned(),
        Some(vec![Vec::<i32>::new()])
    );

    // (d) None and an empty Vec are ordinary payloads.
    let opt = g.state_empty::<Option<i32>>();
    let list = g.state_empty::<Vec<i32>>();
    let payloads = Rc::new(Cell::new((0usize, 0usize)));
    let probe = {
        let payloads = payloads.clone();
        g.node_opts::<i32, _>(
            vec![opt.erased(), list.erased()],
            move |ctx| {
                let (o, l) = payloads.get();
                payloads.set((
                    o + ctx.dep(0).values::<Option<i32>>().len(),
                    l + ctx.dep(1).values::<Vec<i32>>().len(),
                ));
            },
            graphrefly::GraphNodeOpts::default().with_node(graphrefly::NodeOpts {
                ..graphrefly::NodeOpts::default()
            }),
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
    let g = graphrefly::graph();
    let a = g.state::<i32>(0);
    let n = g.node::<i32, _>(vec![a.erased()], |ctx| {
        match *ctx.dep(0).value::<i32>().unwrap() {
            1 => ctx.down(vec![Message::Data(Rc::new(1i32) as AnyValue)]),
            2 => ctx.down(vec![
                Message::Data(Rc::new(2i32) as AnyValue),
                Message::Data(Rc::new(3i32) as AnyValue),
            ]),
            4 => ctx.down(vec![Message::Invalidate]),
            _ => {} // sends nothing: the kernel's RESOLVED
        }
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
        g.node::<i32, _>(vec![n.erased()], move |ctx| {
            first_waves.borrow_mut().push(
                ctx.dep(0)
                    .waves::<i32>()
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
    let q = g.state_empty::<i32>();
    g.batch(|_| {
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
    let g = graphrefly::graph();
    // A diamond A->B,C->D with two sinks on A; A sends three DATA in one array.
    let trigger = g.state::<i32>(0);
    let a = g.node_opts::<i32, _>(
        vec![trigger.erased()],
        |ctx| {
            if ctx.dep(0).values::<i32>().iter().any(|v| **v == 1) {
                ctx.down(vec![
                    Message::Data(Rc::new(23i32) as AnyValue),
                    Message::Data(Rc::new(23i32) as AnyValue),
                    Message::Data(Rc::new(30i32) as AnyValue),
                ]);
            }
        },
        graphrefly::GraphNodeOpts::default().with_node(graphrefly::NodeOpts {
            ..graphrefly::NodeOpts::default()
        }),
    );
    let b = g.node::<i32, _>(vec![a.erased()], forward_i32);
    let c = g.node::<i32, _>(vec![a.erased()], forward_i32);
    let d_runs = Rc::new(RefCell::new(Vec::<(i32, i32)>::new()));
    let d = {
        let runs = d_runs.clone();
        g.node::<i32, _>(vec![b.erased(), c.erased()], move |ctx| {
            if let (Some(x), Some(y)) = (ctx.dep(0).value::<i32>(), ctx.dep(1).value::<i32>()) {
                runs.borrow_mut().push((*x, *y));
            }
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
    let separate = g.node::<i32, _>(vec![trigger.erased()], |ctx| {
        if *ctx.dep(0).value::<i32>().unwrap() == 2 {
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
    let s = g.state::<i32>(0);
    let (sb, _usb) = record_arrays(&s);
    sb.borrow_mut().clear();
    g.batch(|_| {
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
    let split = g.node::<i32, _>(vec![trigger.erased()], |ctx| {
        if *ctx.dep(0).value::<i32>().unwrap() == 3 {
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
    let t = g.state_empty::<i32>();
    let (tb, _ut) = record_arrays(&t);
    tb.borrow_mut().clear();
    g.batch(|_| {
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
    let g = graphrefly::graph();
    let a = g.state::<i32>(0);
    let f = g.node::<i32, _>(vec![a.erased()], |ctx| {
        let v = *ctx.dep(0).value::<i32>().unwrap();
        if v % 2 == 0 {
            ctx.emit(v);
        }
    });
    let q_out: Rc<RefCell<Option<graphrefly::DeferredCtx>>> = Rc::new(RefCell::new(None));
    let q = {
        let slot = q_out.clone();
        g.node_opts::<i32, _>(
            vec![a.erased()],
            move |ctx| {
                *slot.borrow_mut() = Some(ctx.defer());
                if ctx.dep(0).values::<i32>().iter().any(|v| **v == 2) {
                    ctx.emit(200);
                }
            },
            graphrefly::GraphNodeOpts::default().with_node(graphrefly::NodeOpts {
                quiet: true,
                ..graphrefly::NodeOpts::default()
            }),
        )
    };
    let d_runs = Rc::new(RefCell::new(Vec::<(i32, Option<i32>)>::new()));
    let d = {
        let runs = d_runs.clone();
        g.node_opts::<i32, _>(
            vec![a.erased(), q.erased()],
            move |ctx| {
                let pair = (
                    ctx.dep(0).value::<i32>().map_or(-1, |v| *v),
                    ctx.dep(1).value::<i32>().map(|v| *v),
                );
                runs.borrow_mut().push(pair);
                // D retains a value, so an INVALIDATE is effective at D (R-invalidation).
                ctx.emit(pair.0);
            },
            graphrefly::GraphNodeOpts::default().with_node(graphrefly::NodeOpts {
                ..graphrefly::NodeOpts::default()
            }),
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

// ── Phase 3: terminal and INVALIDATE policies; ending closes the lifetime ────

fn opts(
    complete: graphrefly::Policy,
    error: graphrefly::Policy,
    invalidate: graphrefly::Policy,
) -> graphrefly::NodeOpts {
    graphrefly::NodeOpts {
        complete,
        error,
        invalidate,
        ..graphrefly::NodeOpts::default()
    }
}

use graphrefly::Policy::{Auto, Manual};

fn terminal_name(t: Option<&graphrefly::DepTerminal>) -> Option<String> {
    t.map(|t| match t {
        graphrefly::DepTerminal::Complete => "COMPLETE".to_owned(),
        graphrefly::DepTerminal::Error(e) => format!("ERROR {e}"),
    })
}

fn send(slot: &Rc<RefCell<Option<graphrefly::DeferredCtx>>>, wave: Vec<Message<AnyValue>>) {
    // Taken out for the call, so the sink side can never meet a held borrow.
    let ctx = slot.borrow_mut().take().expect("producer activated");
    let sent = catch_unwind(AssertUnwindSafe(|| ctx.down(wave)));
    slot.borrow_mut().get_or_insert(ctx);
    if let Err(panic) = sent {
        std::panic::resume_unwind(panic);
    }
}

/// Node options whose setup returns a cleanup that runs `f` (R-lifetime@2).
fn with_cleanup(f: impl Fn() + 'static) -> graphrefly::NodeOpts {
    let f = Rc::new(f);
    graphrefly::NodeOpts::default().with_setup(move |_| {
        let f = f.clone();
        graphrefly::cleanup(move |_| f())
    })
}

fn data(v: i32) -> Message<AnyValue> {
    Message::Data(Rc::new(v) as AnyValue)
}

/// C-3 — INVALIDATE through a diamond: one INVALIDATE out, private state kept, the
/// policy decides who runs.
#[test]
fn c3_invalidate_through_a_diamond() {
    let g = graphrefly::graph();
    for policy in [Auto, Manual] {
        let s = g.state::<i32>(1);
        let b = g.node::<i32, _>(vec![s.erased()], |ctx| {
            ctx.emit(*ctx.dep(0).value::<i32>().unwrap())
        });
        let c = g.node::<i32, _>(vec![s.erased()], |ctx| {
            ctx.emit(*ctx.dep(0).value::<i32>().unwrap() * 10)
        });
        let runs = Rc::new(RefCell::new(Vec::<(i32, bool, bool)>::new()));
        let d = {
            let runs = runs.clone();
            g.node_opts::<i32, _>(
                vec![b.erased(), c.erased()],
                move |ctx| {
                    let count = ctx.state_get::<i32>().map_or(0, |n| *n) + 1;
                    ctx.state_set(count);
                    let inv = (ctx.dep(0).invalidated(), ctx.dep(1).invalidated());
                    runs.borrow_mut().push((count, inv.0, inv.1));
                    if inv.0 || inv.1 {
                        // Manual policy: the function decides; here it clears its value.
                        ctx.down(vec![Message::Invalidate]);
                        return;
                    }
                    if let (Some(x), Some(y)) =
                        (ctx.dep(0).value::<i32>(), ctx.dep(1).value::<i32>())
                    {
                        ctx.emit(*x + *y);
                    }
                },
                graphrefly::GraphNodeOpts::default().with_node(opts(Auto, Auto, policy)),
            )
        };
        let (log, _u) = record_arrays(&d);
        s.set(2);
        assert_eq!(d.cache(), Some(22));
        let count_before = runs.borrow().last().unwrap().0;
        assert!(count_before > 0);
        log.borrow_mut().clear();
        runs.borrow_mut().clear();

        s.invalidate();
        assert_eq!(
            arrays(&log),
            vec![shape(&["PENDING", "INVALIDATE"])],
            "{policy:?}: exactly one INVALIDATE, with the PENDING prefix"
        );
        assert_eq!(d.cache(), None, "{policy:?}: D loses its value");
        match policy {
            Auto => assert!(runs.borrow().is_empty(), "auto: the function does not run"),
            Manual => assert_eq!(
                *runs.borrow(),
                vec![(count_before + 1, true, true)],
                "manual: one run with both inputs invalidated"
            ),
        }

        // S has no value now, so a second INVALIDATE is dropped.
        log.borrow_mut().clear();
        s.invalidate();
        assert!(
            arrays(&log).is_empty(),
            "{policy:?}: no value, no INVALIDATE"
        );

        // The write after it reaches D as ordinary input; private state continued.
        let count_after_invalidate = runs.borrow().last().map_or(count_before, |r| r.0);
        runs.borrow_mut().clear();
        s.set(3);
        assert_eq!(d.cache(), Some(33));
        assert_eq!(
            *runs.borrow(),
            vec![(count_after_invalidate + 1, false, false)],
            "{policy:?}: the count continues"
        );
    }
}

/// C-15 — a dependency's terminal settles its pending mark (no wedge).
#[test]
fn c15_dependency_terminal_settles_its_pending_mark() {
    let g = graphrefly::graph();
    type Runs = Rc<RefCell<Vec<(Option<String>, Vec<i32>)>>>;
    fn build(
        g: &graphrefly::Graph,
    ) -> (
        graphrefly::State<i32>,
        graphrefly::State<i32>,
        Node<i32>,
        Runs,
    ) {
        let a = g.state::<i32>(1);
        let a2 = g.state::<i32>(10);
        let b = g.node::<i32, _>(vec![a.erased()], |ctx| {
            if let Some(v) = ctx.dep(0).values::<i32>().last() {
                ctx.emit(**v);
            }
        });
        let c = g.node::<i32, _>(vec![a2.erased()], |ctx| {
            if let Some(v) = ctx.dep(0).values::<i32>().last() {
                ctx.emit(**v);
            }
        });
        let runs: Runs = Rc::new(RefCell::new(Vec::new()));
        let d = {
            let runs = runs.clone();
            g.node_opts::<i32, _>(
                vec![b.erased(), c.erased()],
                move |ctx| {
                    runs.borrow_mut().push((
                        terminal_name(ctx.dep(0).terminal()),
                        ctx.dep(1).values::<i32>().iter().map(|v| **v).collect(),
                    ));
                },
                graphrefly::GraphNodeOpts::default().with_node(opts(Manual, Manual, Auto)),
            )
        };
        (a, a2, d, runs)
    }

    // (a) In one batch A ends (B completes without a value) and A2 is written.
    let (a, a2, d, runs) = build(&g);
    let (log, _u) = record_arrays(&d);
    runs.borrow_mut().clear();
    log.borrow_mut().clear();
    g.batch(|_| {
        a.complete();
        a2.set(20);
    });
    assert_eq!(
        *runs.borrow(),
        vec![(Some("COMPLETE".to_owned()), vec![20])],
        "(a) one run sees B's terminal and C's value"
    );
    assert_eq!(arrays(&log), vec![shape(&["PENDING", "RESOLVED"])]);

    // (b) A ends alone: one run; D sends nothing, so it settles with RESOLVED and its
    // dependent is not left pending.
    let (a, _a2, d, runs) = build(&g);
    let e = g.node::<i32, _>(vec![d.erased()], |_| {});
    let (e_log, _ue) = record_arrays(&e);
    runs.borrow_mut().clear();
    e_log.borrow_mut().clear();
    a.complete();
    assert_eq!(*runs.borrow(), vec![(Some("COMPLETE".to_owned()), vec![])]);
    assert_eq!(arrays(&e_log), vec![shape(&["PENDING", "RESOLVED"])]);

    // (c) B ends with ERROR while C sends a value.
    let (a, a2, d, runs) = build(&g);
    let _u = d.subscribe(|_| {});
    runs.borrow_mut().clear();
    g.batch(|_| {
        a.error("boom");
        a2.set(30);
    });
    assert_eq!(
        *runs.borrow(),
        vec![(Some("ERROR boom".to_owned()), vec![30])],
        "(c) one run sees B's ERROR and C's value"
    );
    assert_ne!(
        d.status().output,
        graphrefly::OutputStatus::Errored,
        "manual ERROR: D stays open"
    );
}

/// C-17 — an ended dependency counts toward auto COMPLETE, whatever ended it and in
/// any order.
#[test]
fn c17_ended_dependency_counts_toward_auto_complete() {
    let g = graphrefly::graph();
    for c_first in [true, false] {
        // B sends a value and COMPLETE in one array.
        let (b, b_slot) = driven_in(&g);
        let c = g.state_empty::<i32>();
        let runs = Rc::new(RefCell::new(Vec::<(Option<String>, Option<String>)>::new()));
        let d = {
            let runs = runs.clone();
            g.node_opts::<i32, _>(
                vec![b.erased(), c.erased()],
                move |ctx| {
                    runs.borrow_mut().push((
                        terminal_name(ctx.dep(0).terminal()),
                        terminal_name(ctx.dep(1).terminal()),
                    ));
                },
                graphrefly::GraphNodeOpts::default().with_node(opts(Auto, Manual, Auto)),
            )
        };
        let ctrl_runs = Rc::new(Cell::new(0usize));
        let dctrl = {
            let ctrl_runs = ctrl_runs.clone();
            g.node_opts::<i32, _>(
                vec![b.erased(), c.erased()],
                move |_| ctrl_runs.set(ctrl_runs.get() + 1),
                graphrefly::GraphNodeOpts::default().with_node(opts(Auto, Auto, Auto)),
            )
        };
        let (log, _u) = record_arrays(&d);
        let _uc = dctrl.subscribe(|_| {});
        runs.borrow_mut().clear();

        let end_c = || {
            let before = ctrl_runs.get();
            c.error("e");
            assert_eq!(dctrl.status().output, graphrefly::OutputStatus::Errored);
            assert_eq!(ctrl_runs.get(), before, "Dctrl ends without running");
        };
        if c_first {
            end_c();
            assert_eq!(
                runs.borrow().last().unwrap(),
                &(None, Some("ERROR e".to_owned())),
                "D's function sees C's ERROR as input"
            );
            assert_ne!(
                d.status().output,
                graphrefly::OutputStatus::Completed,
                "D stays open"
            );
            send(&b_slot, vec![data(1), Message::Complete]);
        } else {
            send(&b_slot, vec![data(1), Message::Complete]);
            assert_ne!(
                d.status().output,
                graphrefly::OutputStatus::Completed,
                "C is still open"
            );
            end_c();
        }
        assert_eq!(
            d.status().output,
            graphrefly::OutputStatus::Completed,
            "c_first={c_first}"
        );
        // ctx.terminal is the terminal in this invocation (R-invocation@2), so the
        // last run sees only the terminal that arrived last.
        let last_seen = if c_first {
            (Some("COMPLETE".to_owned()), None)
        } else {
            (None, Some("ERROR e".to_owned()))
        };
        assert_eq!(
            runs.borrow().last().unwrap(),
            &last_seen,
            "D completes after the run that sees the last terminal"
        );
        let completes = arrays(&log)
            .iter()
            .flatten()
            .filter(|k| *k == "COMPLETE")
            .count();
        assert_eq!(completes, 1, "exactly once");
    }
}

/// C-36 — input arrays and terminal admission in one invocation.
#[test]
fn c36_input_arrays_and_terminal_admission() {
    let g = graphrefly::graph();
    // Manual COMPLETE: one invocation sees [[20]] and the COMPLETE together.
    let (a, slot) = driven_in(&g);
    let seen = Rc::new(RefCell::new(Vec::<(Vec<Vec<i32>>, Option<String>)>::new()));
    let d = {
        let seen = seen.clone();
        g.node_opts::<i32, _>(
            vec![a.erased()],
            move |ctx| {
                seen.borrow_mut().push((
                    ctx.dep(0)
                        .waves::<i32>()
                        .iter()
                        .map(|w| w.iter().map(|v| **v).collect())
                        .collect(),
                    terminal_name(ctx.dep(0).terminal()),
                ));
            },
            graphrefly::GraphNodeOpts::default().with_node(opts(Manual, Auto, Auto)),
        )
    };
    let _u = d.subscribe(|_| {});
    seen.borrow_mut().clear();
    send(&slot, vec![data(20), Message::Complete]);
    assert_eq!(
        *seen.borrow(),
        vec![(vec![vec![20]], Some("COMPLETE".to_owned()))]
    );
    assert_ne!(
        d.status().output,
        graphrefly::OutputStatus::Completed,
        "d stays open until its fn ends it"
    );

    // Auto COMPLETE: d runs on DATA 5, then completes after its run, with no
    // balancing RESOLVED.
    let (a, slot) = driven_in(&g);
    let d = g.node_opts::<i32, _>(
        vec![a.erased()],
        |ctx| {
            if let Some(v) = ctx.dep(0).values::<i32>().last() {
                ctx.emit(**v);
            }
        },
        graphrefly::GraphNodeOpts::default().with_node(opts(Auto, Auto, Auto)),
    );
    let (log, _u) = record_arrays(&d);
    log.borrow_mut().clear();
    send(&slot, vec![data(5), Message::Complete]);
    let flat: Vec<String> = arrays(&log).into_iter().flatten().collect();
    assert_eq!(flat.first().map(String::as_str), Some("PENDING"));
    assert!(flat.iter().any(|k| k == "DATA"));
    assert_eq!(flat.last().map(String::as_str), Some("COMPLETE"));
    assert!(
        !flat.iter().any(|k| k == "RESOLVED"),
        "no balancing RESOLVED"
    );
    assert_eq!(d.status().output, graphrefly::OutputStatus::Completed);

    // [DATA 7, INVALIDATE] goes out as [PENDING, INVALIDATE]; under auto INVALIDATE d
    // does not run, and both lose their values.
    let (a, slot) = driven_in(&g);
    let runs = Rc::new(Cell::new(0usize));
    let d = {
        let runs = runs.clone();
        g.node_opts::<i32, _>(
            vec![a.erased()],
            move |ctx| {
                runs.set(runs.get() + 1);
                if let Some(v) = ctx.dep(0).values::<i32>().last() {
                    ctx.emit(**v);
                }
            },
            graphrefly::GraphNodeOpts::default().with_node(opts(Auto, Auto, Auto)),
        )
    };
    let (a_log, _ua) = record_arrays(&a);
    let (d_log, _ud) = record_arrays(&d);
    send(&slot, vec![data(1)]);
    assert_eq!(d.cache(), Some(1));
    a_log.borrow_mut().clear();
    d_log.borrow_mut().clear();
    let before = runs.get();
    send(&slot, vec![data(7), Message::Invalidate]);
    assert_eq!(arrays(&a_log), vec![shape(&["PENDING", "INVALIDATE"])]);
    assert_eq!(arrays(&d_log), vec![shape(&["PENDING", "INVALIDATE"])]);
    assert_eq!(runs.get(), before);
    assert_eq!(a.cache(), None);
    assert_eq!(d.cache(), None);
}

/// C-40 — an ended node can be subscribed and depended on (the replace clause lands
/// with phase 5).
#[test]
fn c40_ended_node_can_be_subscribed_and_depended_on() {
    let g = graphrefly::graph();
    let s = g.state::<i32>(1);
    s.complete();
    let t = g.state_empty::<i32>();
    t.error("e");

    let (log, _u) = record_arrays(&s);
    assert_eq!(arrays(&log), vec![shape(&["START", "DATA", "COMPLETE"])]);

    let d_runs = Rc::new(RefCell::new(Vec::<i32>::new()));
    let d = {
        let d_runs = d_runs.clone();
        g.node_opts::<i32, _>(
            vec![s.erased()],
            move |ctx| {
                d_runs
                    .borrow_mut()
                    .push(*ctx.dep(0).value::<i32>().unwrap());
            },
            graphrefly::GraphNodeOpts::default().with_node(opts(Auto, Auto, Auto)),
        )
    };
    let _ud = d.subscribe(|_| {});
    assert_eq!(*d_runs.borrow(), vec![1], "D runs once on S's snapshot");
    assert_eq!(
        d.status().output,
        graphrefly::OutputStatus::Completed,
        "and completes after that run"
    );

    let e_runs = Rc::new(Cell::new(0usize));
    let e = {
        let e_runs = e_runs.clone();
        g.node_opts::<i32, _>(
            vec![t.erased()],
            move |_| e_runs.set(e_runs.get() + 1),
            graphrefly::GraphNodeOpts::default().with_node(opts(Auto, Auto, Auto)),
        )
    };
    let _ue = e.subscribe(|_| {});
    assert_eq!(e.status().output, graphrefly::OutputStatus::Errored);
    assert_eq!(e_runs.get(), 0, "E ends with ERROR e without running");

    let m_seen = Rc::new(RefCell::new(Vec::<Option<String>>::new()));
    let m = {
        let m_seen = m_seen.clone();
        g.node_opts::<i32, _>(
            vec![t.erased()],
            move |ctx| {
                m_seen
                    .borrow_mut()
                    .push(terminal_name(ctx.dep(0).terminal()));
            },
            graphrefly::GraphNodeOpts::default().with_node(opts(Manual, Manual, Auto)),
        )
    };
    let _um = m.subscribe(|_| {});
    assert_eq!(*m_seen.borrow(), vec![Some("ERROR e".to_owned())]);
    assert_ne!(
        m.status().output,
        graphrefly::OutputStatus::Errored,
        "M stays open"
    );

    let refused = catch_unwind(AssertUnwindSafe(|| s.set(2)));
    assert!(refused.is_err(), "the set on S is refused");
    assert_eq!(s.cache(), Some(1), "S never changes again");
}

/// C-44 — the terminal and manual-INVALIDATE clauses: a wave of terminals holds a
/// fan-in.
#[test]
fn c44_a_wave_of_terminals_holds_a_fan_in() {
    let g = graphrefly::graph();
    for (complete, invalidate) in [(Manual, Auto), (Auto, Manual)] {
        let s = g.state::<i32>(1);
        // Forwarders of what S sends: under auto COMPLETE they also run on S's
        // COMPLETE alone (review Q12) and send nothing for it.
        let b = g.node::<i32, _>(vec![s.erased()], forward_arrays_i32);
        let c = g.node::<i32, _>(vec![s.erased()], forward_arrays_i32);
        let runs = Rc::new(RefCell::new(Vec::<(
            Option<String>,
            Option<String>,
            bool,
            bool,
        )>::new()));
        let d = {
            let runs = runs.clone();
            g.node_opts::<i32, _>(
                vec![b.erased(), c.erased()],
                move |ctx| {
                    runs.borrow_mut().push((
                        terminal_name(ctx.dep(0).terminal()),
                        terminal_name(ctx.dep(1).terminal()),
                        ctx.dep(0).invalidated(),
                        ctx.dep(1).invalidated(),
                    ));
                },
                graphrefly::GraphNodeOpts::default().with_node(opts(complete, Auto, invalidate)),
            )
        };
        let (b_log, _ub) = record_arrays(&b);
        let (c_log, _uc) = record_arrays(&c);
        let _ud = d.subscribe(|_| {});
        runs.borrow_mut().clear();
        b_log.borrow_mut().clear();
        c_log.borrow_mut().clear();
        if complete == Manual {
            s.complete();
            let want = vec![shape(&["PENDING", "COMPLETE"])];
            assert_eq!(arrays(&b_log), want);
            assert_eq!(arrays(&c_log), want);
            assert_eq!(
                *runs.borrow(),
                vec![(
                    Some("COMPLETE".into()),
                    Some("COMPLETE".into()),
                    false,
                    false
                )],
                "manual COMPLETE: one run sees both terminals"
            );
        } else {
            s.invalidate();
            assert_eq!(
                *runs.borrow(),
                vec![(None, None, true, true)],
                "manual INVALIDATE: one run sees both inputs invalidated"
            );
        }
    }
}

/// C-34 — the terminal fanout, the ended node's lifetime closing, and acquiring an
/// ended node.
#[test]
fn c34_terminal_fanout_closes_the_lifetime() {
    let reports = Rc::new(RefCell::new(Vec::<graphrefly::HostFailure>::new()));
    let g = reporting_graph(&reports);

    let close_order: Rc<RefCell<Vec<&'static str>>> = Rc::new(RefCell::new(Vec::new()));
    // T is a source with no other dependent; its release is visible as its cleanup.
    let t_released = Rc::new(Cell::new(false));
    let t_slot: Rc<RefCell<Option<graphrefly::DeferredCtx>>> = Rc::new(RefCell::new(None));
    let t = {
        let t_released = t_released.clone();
        let keep = t_slot.clone();
        producer_in::<i32>(
            &g,
            with_cleanup({
                let close_order = close_order.clone();
                move || {
                    t_released.set(true);
                    close_order.borrow_mut().push("T");
                }
            }),
            move |ctx| {
                *keep.borrow_mut() = Some(ctx.defer());
                ctx.emit(0);
            },
        )
    };
    let cleanup = Rc::new(Cell::new(0usize));
    let s_runs = Rc::new(Cell::new(0usize));
    let s = {
        let cleanup = cleanup.clone();
        let s_runs = s_runs.clone();
        derived_in::<i32>(
            &g,
            vec![t.erased()],
            with_cleanup({
                let close_order = close_order.clone();
                move || {
                    cleanup.set(cleanup.get() + 1);
                    close_order.borrow_mut().push("S");
                }
            }),
            move |ctx| {
                s_runs.set(s_runs.get() + 1);
                if *ctx.dep(0).value::<i32>().unwrap() == 1 {
                    ctx.down(vec![data(1), Message::Complete]);
                }
            },
        )
    };
    let x_log: Rc<RefCell<Vec<Vec<String>>>> = Rc::new(RefCell::new(Vec::new()));
    let z_log: Rc<RefCell<Vec<Vec<String>>>> = Rc::new(RefCell::new(Vec::new()));
    let z_unsub: Rc<RefCell<Option<Unsub>>> = Rc::new(RefCell::new(None));
    let _ux = {
        let x_log = x_log.clone();
        let z_log = z_log.clone();
        let z_unsub = z_unsub.clone();
        let s2 = s.clone();
        s.subscribe(move |msgs: &[Message<AnyValue>]| {
            x_log
                .borrow_mut()
                .push(msgs.iter().map(|m| format!("{m:?}")).collect());
            if msgs.iter().any(|m| matches!(m, Message::Complete)) && z_unsub.borrow().is_none() {
                let z_log = z_log.clone();
                let unsub = s2.subscribe(move |msgs: &[Message<AnyValue>]| {
                    z_log
                        .borrow_mut()
                        .push(msgs.iter().map(|m| format!("{m:?}")).collect());
                });
                *z_unsub.borrow_mut() = Some(unsub);
                panic!("x broke");
            }
        })
    };
    let (y_log, _uy) = record_arrays(&s);
    x_log.borrow_mut().clear();
    y_log.borrow_mut().clear();

    assert!(!t_released.get());
    send(&t_slot, vec![data(1)]);
    let full = vec![
        "PENDING".to_owned(),
        "DATA".to_owned(),
        "COMPLETE".to_owned(),
    ];
    assert_eq!(
        *x_log.borrow(),
        vec![full.clone()],
        "X gets the whole array"
    );
    assert_eq!(arrays(&y_log), vec![full.clone()], "Y gets the whole array");
    assert_eq!(cleanup.get(), 1, "S's lifetime closes: its cleanup runs");
    assert!(t_released.get(), "S releases T, which deactivates");
    assert_eq!(
        *close_order.borrow(),
        vec!["T", "S"],
        "S releases T before its own cleanup runs (R-lifetime@2)"
    );
    assert_eq!(s.cache(), Some(1), "S keeps its value");
    assert_eq!(
        s.status().output,
        graphrefly::OutputStatus::Completed,
        "and its COMPLETE"
    );
    let z_first = z_log.borrow().first().cloned();
    assert_eq!(
        z_first,
        Some(vec![
            "START".to_owned(),
            "DATA".to_owned(),
            "COMPLETE".to_owned()
        ]),
        "Z gets the ended node's snapshot"
    );
    let runs = s_runs.get();
    let _late = s.subscribe(|_| {});
    assert_eq!(s_runs.get(), runs, "S never runs again");
    assert_eq!(
        *reports.borrow(),
        vec![graphrefly::HostFailure {
            source: "sink".into(),
            error: "x broke".into()
        }],
        "X's throw goes to the host reporter"
    );
    assert_eq!(
        s.status().output,
        graphrefly::OutputStatus::Completed,
        "and is not S's ERROR"
    );
}

/// R-terminals (phase 3 acceptance): under auto ERROR, an INVALIDATE that reached the
/// node earlier in the wave takes effect first, whatever the INVALIDATE policy, so the
/// node never ends holding a value its input invalidated.
#[test]
fn auto_error_applies_an_earlier_invalidate_first() {
    let g = graphrefly::graph();
    for invalidate in [Auto, Manual] {
        let a = g.state::<i32>(1);
        let b = g.state::<i32>(2);
        let runs = Rc::new(Cell::new(0usize));
        let d = {
            let runs = runs.clone();
            g.node_opts::<i32, _>(
                vec![a.erased(), b.erased()],
                move |ctx| {
                    runs.set(runs.get() + 1);
                    if let (Some(x), Some(y)) =
                        (ctx.dep(0).value::<i32>(), ctx.dep(1).value::<i32>())
                    {
                        ctx.emit(*x + *y);
                    }
                },
                graphrefly::GraphNodeOpts::default().with_node(opts(Auto, Auto, invalidate)),
            )
        };
        let (log, _u) = record_arrays(&d);
        assert_eq!(d.cache(), Some(3));
        log.borrow_mut().clear();
        let before = runs.get();
        g.batch(|_| {
            a.invalidate();
            b.error("x");
        });
        let flat: Vec<String> = arrays(&log).into_iter().flatten().collect();
        let inv = flat.iter().position(|k| k == "INVALIDATE");
        let err = flat.iter().position(|k| k == "ERROR");
        assert!(
            matches!((inv, err), (Some(i), Some(e)) if i < e),
            "{invalidate:?}: INVALIDATE goes out before the ERROR, got {flat:?}"
        );
        assert_eq!(
            runs.get(),
            before,
            "{invalidate:?}: the function does not run"
        );
        assert_eq!(
            d.cache(),
            None,
            "{invalidate:?}: the ended node holds no value"
        );
        assert_eq!(d.status().output, graphrefly::OutputStatus::Errored);
    }
}

// ── Phase 4: lifetime setup, signal and close order ──────────────────────────

use graphrefly::{CacheStatus, Lifecycle, OutputStatus};

/// C-4 — a sync leg and an async leg: the async result is a later wave, nothing
/// waits across it, and a result arriving after the lifetime closed is dropped.
#[test]
fn c4_async_result_is_a_later_wave_and_dropped_after_close() {
    let g = graphrefly::graph();
    let a = g.state::<i32>(1);
    let b = g.node::<i32, _>(vec![a.erased()], |ctx| {
        ctx.emit(*ctx.dep(0).value::<i32>().unwrap() * 10)
    });
    // C answers its first run at once, and later runs through async work.
    type Work = Rc<RefCell<Vec<(graphrefly::DeferredCtx, i32)>>>;
    let work: Work = Rc::new(RefCell::new(Vec::new()));
    let c = {
        let work = work.clone();
        g.node::<i32, _>(vec![a.erased()], move |ctx| {
            let v = *ctx.dep(0).value::<i32>().unwrap();
            if ctx.state_get::<bool>().is_none() {
                ctx.state_set(true);
                ctx.emit(v * 100);
            } else {
                work.borrow_mut().push((ctx.defer(), v * 100));
            }
        })
    };
    let d_runs = Rc::new(RefCell::new(Vec::<(i32, i32)>::new()));
    let d = {
        let d_runs = d_runs.clone();
        g.node::<i32, _>(vec![b.erased(), c.erased()], move |ctx| {
            let pair = (
                *ctx.dep(0).value::<i32>().unwrap(),
                *ctx.dep(1).value::<i32>().unwrap(),
            );
            d_runs.borrow_mut().push(pair);
        })
    };
    let (c_log, uc) = record_arrays(&c);
    let ud = d.subscribe(|_| {});
    d_runs.borrow_mut().clear();
    c_log.borrow_mut().clear();

    a.set(2);
    assert_eq!(arrays(&c_log), vec![shape(&["PENDING", "RESOLVED"])]);
    assert_eq!(
        *d_runs.borrow(),
        vec![(20, 100)],
        "B's new value, C's old one"
    );
    assert_ne!(
        d.status().cache,
        CacheStatus::Stale,
        "no PENDING stays open"
    );

    let (ctx, v) = work.borrow_mut().remove(0);
    ctx.emit(v);
    assert_eq!(
        *d_runs.borrow(),
        vec![(20, 100), (20, 200)],
        "a new wave from C"
    );

    // Write again, then close C's lifetime before the result arrives.
    a.set(3);
    let (late, v) = work.borrow_mut().remove(0);
    uc();
    ud();
    assert_eq!(c.status().lifecycle, Lifecycle::Inactive);
    late.emit(v);
    late.down(vec![data(v)]);
    assert_eq!(c.cache(), None, "the late result is dropped");
}

/// C-33 — a closed lifetime revokes its ctx, and cleanup failures stay isolated
/// (the lifetime clauses; the request-queue clauses land in phase 5).
#[test]
fn c33_closed_lifetime_revokes_ctx_and_cleanup_failures_stay_isolated() {
    struct Dropped(Rc<Cell<bool>>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.set(true);
        }
    }
    let reports = Rc::new(RefCell::new(Vec::<graphrefly::HostFailure>::new()));
    let g = reporting_graph(&reports);
    let events: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(Vec::new()));

    let a = state_in::<i32>(&g, 1);
    let p = {
        let events = events.clone();
        derived_in::<i32>(
            &g,
            vec![a.erased()],
            graphrefly::NodeOpts::default().with_setup(move |_| {
                let events = events.clone();
                graphrefly::cleanup(move |_| events.borrow_mut().push("P cleanup".into()))
            }),
            |ctx| {
                if let Some(v) = ctx.dep(0).value::<i32>() {
                    ctx.emit(*v)
                }
            },
        )
    };
    let n_handle: Rc<RefCell<Option<Node<i32>>>> = Rc::new(RefCell::new(None));
    let signals: Rc<RefCell<Vec<graphrefly::AbortSignal>>> = Rc::new(RefCell::new(Vec::new()));
    let late: Rc<RefCell<Option<graphrefly::DeferredCtx>>> = Rc::new(RefCell::new(None));
    let state_dropped = Rc::new(Cell::new(false));
    let n_runs = Rc::new(Cell::new(0usize));
    let n = {
        let events = events.clone();
        let n_handle = n_handle.clone();
        let signals = signals.clone();
        let late = late.clone();
        let state_dropped = state_dropped.clone();
        let n_runs = n_runs.clone();
        derived_in::<i32>(
            &g,
            vec![p.erased()],
            graphrefly::NodeOpts {
                ..graphrefly::NodeOpts::default()
            }
            .with_setup(move |ctx| {
                events.borrow_mut().push("N setup".into());
                signals.borrow_mut().push(ctx.signal());
                let events = events.clone();
                let n_handle = n_handle.clone();
                graphrefly::cleanup(move |closing| {
                    let ran_on = closing.state_get::<(i32, Dropped)>().map(|state| state.0);
                    let lifecycle = n_handle.borrow().as_ref().map(|n| n.status().lifecycle);
                    events
                        .borrow_mut()
                        .push(format!("N cleanup {ran_on:?} {lifecycle:?}"));
                    panic!("N cleanup boom");
                })
            }),
            move |ctx| {
                n_runs.set(n_runs.get() + 1);
                *late.borrow_mut() = Some(ctx.defer());
                if let Some(v) = ctx.dep(0).value::<i32>() {
                    ctx.state_set((*v, Dropped(state_dropped.clone())));
                    ctx.emit(*v);
                }
            },
        )
    };
    *n_handle.borrow_mut() = Some(n.clone());

    // Status is derived: inactive with an empty cache before the subscription.
    let before = n.status();
    assert_eq!(
        (before.lifecycle, before.cache, before.output),
        (Lifecycle::Inactive, CacheStatus::Empty, OutputStatus::Open)
    );
    let u = n.subscribe(|_| {});
    let active = n.status();
    assert_eq!(
        (active.lifecycle, active.cache),
        (Lifecycle::Active, CacheStatus::Ready)
    );
    assert_eq!(
        *events.borrow(),
        vec!["N setup".to_owned()],
        "setup before the first run"
    );

    // A second run runs no setup and drops no cleanup.
    a.set(2);
    assert_eq!(n_runs.get(), 2);
    assert_eq!(*events.borrow(), vec!["N setup".to_owned()]);
    a.invalidate();
    assert_eq!(
        n.status().cache,
        CacheStatus::Empty,
        "empty after the INVALIDATE"
    );
    a.set(3);
    assert_eq!(n.cache(), Some(3));

    // Deactivation: P's cleanup first (released dependency), then N's, which reads
    // the last run's value, sees `closing`, and throws to the host reporter.
    state_dropped.set(false);
    u();
    assert_eq!(
        *events.borrow(),
        vec![
            "N setup".to_owned(),
            "P cleanup".to_owned(),
            "N cleanup Some(3) Some(Closing)".to_owned(),
        ]
    );
    assert_eq!(
        *reports.borrow(),
        vec![graphrefly::HostFailure {
            source: "cleanup".into(),
            error: "N cleanup boom".into()
        }]
    );
    assert_eq!(
        n.status().lifecycle,
        Lifecycle::Inactive,
        "the close completes"
    );
    assert!(
        signals.borrow()[0].is_aborted(),
        "the old signal is aborted"
    );
    assert!(state_dropped.get(), "private state is dropped");

    // The old ctx.down is dropped.
    let old = late.borrow_mut().take().unwrap();
    old.down(vec![data(999)]);
    assert_eq!(n.cache(), None);

    // A new lifetime: distinct signal, setup again, fresh snapshots.
    let (log, _u2) = record_values(&n);
    assert_eq!(events.borrow().last().map(String::as_str), Some("N setup"));
    assert_eq!(signals.borrow().len(), 2);
    assert!(!signals.borrow()[1].is_aborted());
    assert_eq!(
        *log.borrow(),
        vec![vec![3]],
        "fresh snapshot, not the dropped 999"
    );

    a.complete();
    assert_eq!(n.status().output, OutputStatus::Completed);
    n_handle.borrow_mut().take();
}

// ── Phase 5: requests, batch, set fn, replace and ctx.rewire ─────────────────

/// The current dependencies of `name`, as the graph describes them.
fn deps_of(g: &graphrefly::Graph, name: &str) -> Vec<String> {
    g.topology()
        .edges
        .iter()
        .filter(|e| e.to == name)
        .map(|e| e.from.clone())
        .collect()
}

fn named(name: &str) -> graphrefly::GraphNodeOpts {
    graphrefly::GraphNodeOpts::named(name)
}

/// C-6 — a function's write to its own upstream is a later request, never a reentry.
#[test]
fn c6_write_to_own_upstream_is_a_later_request() {
    let g = graphrefly::graph();
    let s = g.state_empty::<i32>();
    let d = g.node::<i32, _>(vec![s.erased()], |ctx| {
        if let Some(v) = ctx.dep(0).value::<i32>() {
            ctx.emit(*v + 1)
        }
    });
    let running = Rc::new(Cell::new(false));
    let reentered = Rc::new(Cell::new(false));
    let e = {
        let s = s.clone();
        let (running, reentered) = (running.clone(), reentered.clone());
        g.node::<(), _>(vec![d.erased()], move |ctx| {
            let Some(v) = ctx.dep(0).value::<i32>().map(|v| *v) else {
                return;
            };
            reentered.set(reentered.get() || running.get());
            running.set(true);
            if v < 3 {
                s.set(v);
            }
            running.set(false);
        })
    };
    let scan_runs = Rc::new(Cell::new(0usize));
    let scan = {
        let scan_runs = scan_runs.clone();
        g.node::<i32, _>(vec![s.erased()], move |ctx| {
            let Some(v) = ctx.dep(0).value::<i32>() else {
                return;
            };
            scan_runs.set(scan_runs.get() + 1);
            let total = ctx.state_get::<i32>().map_or(0, |t| *t) + *v;
            ctx.state_set(total);
            ctx.emit(total);
        })
    };
    let (e_log, _ue) = record_arrays(&e);
    let (d_log, _ud) = record_values(&d);
    let _us = scan.subscribe(|_| {});

    s.set(0);
    assert!(!reentered.get(), "no function is reentered");
    assert_eq!(d.cache(), Some(3), "the loop ends once D reaches 3");
    assert_eq!(s.cache(), Some(2));
    let d_waves: Vec<Vec<i32>> = d_log
        .borrow()
        .iter()
        .filter(|w| !w.is_empty())
        .cloned()
        .collect();
    assert_eq!(
        d_waves,
        vec![vec![1], vec![2], vec![3]],
        "each write reached E again in a later wave"
    );
    assert!(
        arrays(&e_log).iter().flatten().all(|k| k != "ERROR"),
        "nothing throws"
    );
    assert_eq!(scan_runs.get(), 3, "a scan over S runs once per wave");
    assert_eq!(scan.cache(), Some(3));
}

/// C-11 — a node rewires itself from its function, for its lifetime.
#[test]
fn c11_a_node_rewires_itself_for_its_lifetime() {
    let g = graphrefly::graph();
    let s = g.state_opts(0i32, named("s"));
    type Leaf = (
        Node<i32>,
        Rc<RefCell<Option<graphrefly::DeferredCtx>>>,
        Rc<Cell<usize>>,
    );
    let leaf = |name: &str| -> Leaf {
        let kept: Rc<RefCell<Option<graphrefly::DeferredCtx>>> = Rc::new(RefCell::new(None));
        let cleanups = Rc::new(Cell::new(0usize));
        let node = g.producer_opts(
            {
                let kept = kept.clone();
                move |ctx: &graphrefly::Ctx| *kept.borrow_mut() = Some(ctx.defer())
            },
            named(name).with_node(with_cleanup({
                let cleanups = cleanups.clone();
                move || cleanups.set(cleanups.get() + 1)
            })),
        );
        (node, kept, cleanups)
    };
    let (inner_a, a_ctx, a_cleanups) = leaf("inner_a");
    let (inner_b, b_ctx, b_cleanups) = leaf("inner_b");
    let op_runs = Rc::new(Cell::new(0usize));
    let op = {
        let (s_core, a, b) = (s.erased(), inner_a.erased(), inner_b.erased());
        let op_runs = op_runs.clone();
        g.node_opts::<i32, _>(
            vec![s.erased()],
            move |ctx| {
                op_runs.set(op_runs.get() + 1);
                let runs = ctx.state_get::<usize>().map_or(0, |n| *n) + 1;
                ctx.state_set(runs);
                let inner = match ctx.dep(0).value::<i32>().map(|v| *v) {
                    Some(1) => Some(a.clone()),
                    Some(2) => Some(b.clone()),
                    _ => None,
                };
                if let Some(inner) = inner {
                    ctx.rewire(vec![s_core.clone(), inner]);
                }
                if ctx.dep_len() > 1 {
                    for x in ctx.dep(1).values::<i32>() {
                        ctx.emit(*x);
                    }
                }
            },
            graphrefly::GraphNodeOpts {
                node: graphrefly::NodeOpts {
                    ..graphrefly::NodeOpts::default()
                },
                ..named("op")
            },
        )
    };
    let u = op.subscribe(|_| {});
    assert_eq!(deps_of(&g, "op"), vec!["s"]);

    s.set(1);
    assert_eq!(
        deps_of(&g, "op"),
        vec!["s", "inner_a"],
        "the rewire ran after the wave"
    );
    send(&a_ctx, vec![data(10)]);
    assert_eq!(op.cache(), Some(10));

    s.set(2);
    assert_eq!(deps_of(&g, "op"), vec!["s", "inner_b"]);
    assert_eq!(
        a_cleanups.get(),
        1,
        "inner a deactivates and its cleanup runs"
    );
    assert_eq!(op.cache(), Some(10), "OP keeps its value across the rewire");
    send(&b_ctx, vec![data(20)]);
    assert_eq!(op.cache(), Some(20));
    let runs_before_close = op_runs.get();

    // The lifetime closes: the dependencies go back to the declared [S].
    u();
    assert_eq!(deps_of(&g, "op"), vec!["s"]);
    assert_eq!(b_cleanups.get(), 1);
    assert_eq!(
        op.definition().deps.len(),
        1,
        "the declared dependencies stayed [S]"
    );
    // The new lifetime starts on [S] and rewires again from its first run.
    let _u2 = op.subscribe(|_| {});
    assert_eq!(deps_of(&g, "op"), vec!["s", "inner_b"]);
    assert!(op_runs.get() > runs_before_close);
}

/// C-35 — rewire keeps the instance and runs between waves.
#[test]
fn c35_rewire_keeps_the_instance_and_runs_between_waves() {
    let g = graphrefly::graph();
    let b = g.state_opts(1i32, named("b"));
    let c = g.state_opts(10i32, named("c"));
    let e = g.state_opts(0i32, named("e"));
    e.complete();
    let gst = g.state_empty_opts::<i32>(named("g"));
    let kept: Rc<RefCell<Option<graphrefly::DeferredCtx>>> = Rc::new(RefCell::new(None));
    let runs = Rc::new(Cell::new(0usize));
    let in_run_rewire: Rc<RefCell<Option<Vec<graphrefly::Core>>>> = Rc::new(RefCell::new(None));
    let deps_during_run = Rc::new(Cell::new(usize::MAX));
    let d = {
        let (kept, runs) = (kept.clone(), runs.clone());
        let (in_run_rewire, deps_during_run) = (in_run_rewire.clone(), deps_during_run.clone());
        let ids = (b.erased(), c.erased(), gst.erased());
        g.node_opts::<i32, _>(
            vec![b.erased()],
            move |ctx| {
                runs.set(runs.get() + 1);
                *kept.borrow_mut() = Some(ctx.defer());
                ctx.state_set(runs.get());
                if let Some(next) = in_run_rewire.borrow_mut().take() {
                    ctx.rewire(next);
                    deps_during_run.set(ctx.dep_len());
                }
                // Reads its dependencies by id.
                let mut out = None;
                for i in 0..ctx.dep_len() {
                    if ctx.dep(i).is(&ids.0) || ctx.dep(i).is(&ids.1) || ctx.dep(i).is(&ids.2) {
                        if let Some(v) = ctx.dep(i).values::<i32>().last() {
                            out = Some(**v);
                        }
                    }
                }
                if let Some(v) = out {
                    ctx.emit(v);
                }
            },
            graphrefly::GraphNodeOpts {
                node: graphrefly::NodeOpts {
                    ..graphrefly::NodeOpts::default()
                },
                ..named("d")
            },
        )
    };
    let (log, u) = record_arrays(&d);
    assert_eq!(d.cache(), Some(1));
    let rewire = |deps: Vec<graphrefly::Core>| {
        let ctx = kept.borrow_mut().take().expect("kept ctx");
        let asked = catch_unwind(AssertUnwindSafe(|| ctx.rewire(deps)));
        kept.borrow_mut().get_or_insert(ctx);
        if let Err(panic) = asked {
            std::panic::resume_unwind(panic);
        }
    };
    log.borrow_mut().clear();
    let r0 = runs.get();

    // Adding C: no START, one input wave, the function runs once; C then drives D.
    rewire(vec![b.erased(), c.erased()]);
    assert_eq!(deps_of(&g, "d"), vec!["b", "c"]);
    assert_eq!(runs.get(), r0 + 1);
    assert!(arrays(&log).iter().flatten().all(|k| k != "START"));
    assert_eq!(d.cache(), Some(10));
    c.set(11);
    assert_eq!(d.cache(), Some(11));

    // Reordering keeps the records: nothing runs.
    let r1 = runs.get();
    rewire(vec![c.erased(), b.erased()]);
    assert_eq!(deps_of(&g, "d"), vec!["c", "b"]);
    assert_eq!(runs.get(), r1);

    // Removing B releases it without a wave; D keeps its value.
    log.borrow_mut().clear();
    rewire(vec![c.erased()]);
    assert!(arrays(&log).is_empty(), "released without a wave");
    b.set(2);
    assert_eq!(d.cache(), Some(11), "B no longer drives D");
    // Rewiring to the current list does nothing.
    rewire(vec![c.erased()]);
    assert_eq!(runs.get(), r1);

    // Adding the ended E: its COMPLETE counts toward the policy (C is still open).
    rewire(vec![c.erased(), e.erased()]);
    assert_eq!(d.status().output, OutputStatus::Open);

    // Adding G with no snapshot runs nothing; G's first value then drives D.
    let r2 = runs.get();
    rewire(vec![c.erased(), e.erased(), gst.erased()]);
    assert_eq!(runs.get(), r2, "no snapshot, no run");
    assert_eq!(d.cache(), Some(11));
    gst.set(7);
    assert_eq!(d.cache(), Some(7));

    // Rewiring to no dependencies keeps D's value; later writes no longer reach D.
    rewire(Vec::new());
    assert_eq!(deps_of(&g, "d"), Vec::<String>::new());
    c.set(12);
    assert_eq!(d.cache(), Some(7));

    // A rewire requested from D's function waits until the wave ends.
    rewire(vec![b.erased()]);
    *in_run_rewire.borrow_mut() = Some(vec![b.erased(), c.erased()]);
    b.set(3);
    assert_eq!(deps_during_run.get(), 1, "not applied during the run");
    assert_eq!(deps_of(&g, "d"), vec!["b", "c"], "applied after the wave");

    // Refused: the node itself, a duplicate, a node that depends on D.
    let x = g.node_opts::<i32, _>(vec![d.erased()], |_| {}, named("x"));
    for bad in [
        vec![d.erased()],
        vec![b.erased(), b.erased()],
        vec![x.erased()],
    ] {
        let refused = catch_unwind(AssertUnwindSafe(|| rewire(bad)));
        let message = refused
            .err()
            .and_then(|p| p.downcast_ref::<String>().cloned())
            .unwrap_or_default();
        assert!(
            message.contains("depend on itself") || message.contains("duplicate"),
            "refused by the rewire check, got {message:?}"
        );
    }
    assert_eq!(
        d.definition().deps.len(),
        1,
        "the declared dependencies stay [B]"
    );

    // After the resubscription D is back on [B]; the closed lifetime's ctx is dropped.
    let old_ctx = {
        b.set(4);
        kept.borrow_mut().take().expect("kept ctx")
    };
    u();
    let (_log2, u2) = record_arrays(&d);
    assert_eq!(deps_of(&g, "d"), vec!["b"]);
    old_ctx.rewire(vec![b.erased(), c.erased()]);
    assert_eq!(
        deps_of(&g, "d"),
        vec!["b"],
        "a closed lifetime's rewire is dropped"
    );

    // After D ends, a rewire through its last ctx is dropped too.
    let last = kept.borrow_mut().take().expect("kept ctx");
    b.complete();
    assert_eq!(d.status().output, OutputStatus::Completed);
    last.rewire(vec![b.erased(), c.erased()]);
    assert_eq!(deps_of(&g, "d"), vec!["b"]);
    drop(u2);
}

/// C-37 — a closed lifetime revokes old work while the node and topology remain.
#[test]
fn c37_closed_lifetime_revokes_old_work() {
    let reports = Rc::new(RefCell::new(Vec::<graphrefly::HostFailure>::new()));
    let g = reporting_graph(&reports);
    let a = g.state_opts(1i32, named("a"));
    let bb = g.state_opts(100i32, named("bb"));
    let kept: Rc<RefCell<Vec<graphrefly::DeferredCtx>>> = Rc::new(RefCell::new(Vec::new()));
    let n = {
        let kept = kept.clone();
        g.node_opts::<i32, _>(
            vec![a.erased()],
            move |ctx| kept.borrow_mut().push(ctx.defer()),
            named("n"),
        )
    };
    let (log, u) = record_values(&n);
    let first = kept.borrow_mut().remove(0);
    first.rewire(vec![a.erased(), bb.erased()]);
    assert_eq!(deps_of(&g, "n"), vec!["a", "bb"]);
    log.borrow_mut().clear();
    first.down(vec![data(5)]);
    assert_eq!(*log.borrow(), vec![vec![5]], "the rewire kept the lifetime");

    kept.borrow_mut().clear();
    a.set(2);
    let old = kept.borrow_mut().remove(0);
    u();
    let (log2, u2) = record_values(&n);
    assert_eq!(
        deps_of(&g, "n"),
        vec!["a"],
        "the new lifetime starts on the declared deps"
    );
    log2.borrow_mut().clear();
    old.down(vec![data(9)]);
    assert!(log2.borrow().is_empty(), "the old ctx.down is dropped");
    let fresh = kept.borrow_mut().pop().expect("new lifetime ran");
    assert!(!fresh.signal().is_aborted());
    assert!(
        old.signal().is_aborted(),
        "the old ctx is a distinct, closed one"
    );

    // A rewire asked for while the lifetime is open is dropped if the lifetime has
    // closed by the time it runs: here an unsubscribe requested first closes it.
    u2();
    let u3: Rc<RefCell<Option<Unsub>>> = Rc::new(RefCell::new(None));
    let current: Rc<RefCell<Option<graphrefly::DeferredCtx>>> = Rc::new(RefCell::new(None));
    let trigger = g.state_empty::<i32>();
    let _ut = {
        let (u3, current, bb, a) = (u3.clone(), current.clone(), bb.erased(), a.erased());
        trigger.subscribe(move |msgs: &[Message<AnyValue>]| {
            if msgs.iter().any(|m| matches!(m, Message::Data(_))) {
                if let Some(u) = u3.borrow_mut().take() {
                    u();
                }
                if let Some(ctx) = current.borrow().as_ref() {
                    ctx.rewire(vec![a.clone(), bb.clone()]);
                }
            }
        })
    };
    kept.borrow_mut().clear();
    *u3.borrow_mut() = Some(n.subscribe(|_| {}));
    *current.borrow_mut() = kept.borrow_mut().pop();
    assert!(current.borrow().is_some(), "the new lifetime ran");
    trigger.set(1);
    assert!(
        reports.borrow().is_empty(),
        "the dropped rewire is silent: {:?}",
        reports.borrow()
    );
    assert_eq!(n.status().lifecycle, Lifecycle::Inactive);
    assert_eq!(n.status().output, OutputStatus::Open, "and ends nothing");
    assert_eq!(deps_of(&g, "n"), vec!["a"], "the late rewire was dropped");
}

/// C-40 — the replace clause: a replace gives an ended node a new instance.
#[test]
fn c40_replace_gives_an_ended_node_a_new_instance() {
    let g = graphrefly::graph();
    let s = g.state::<i32>(1);
    s.complete();
    let d = g.node::<i32, _>(vec![s.erased()], |ctx| {
        ctx.emit(*ctx.dep(0).value::<i32>().unwrap())
    });
    let (log, _u) = record_arrays(&d);
    assert_eq!(d.status().output, OutputStatus::Completed);
    let s2 = g.state::<i32>(5);
    log.borrow_mut().clear();
    d.replace(
        vec![s2.erased()],
        |ctx| ctx.emit(*ctx.dep(0).value::<i32>().unwrap() * 2),
        graphrefly::ReplaceOpts::default(),
    );
    assert_eq!(arrays(&log), vec![shape(&["START", "DATA"])]);
    assert_eq!(d.cache(), Some(10));
    assert_eq!(
        d.status().output,
        OutputStatus::Open,
        "a terminal ends an instance, not the node"
    );
    s2.set(6);
    assert_eq!(d.cache(), Some(12));
}

/// C-33 — the request-queue clauses: an unsubscribe asked for during an invocation
/// waits until the wave ends; output staged through a lifetime that closes before the
/// commit is dropped at the commit.
#[test]
fn c33_requests_wait_and_closed_lifetime_staging_is_dropped() {
    let g = graphrefly::graph();
    let a = g.state::<i32>(1);
    let unsub: Rc<RefCell<Option<Unsub>>> = Rc::new(RefCell::new(None));
    let still_subscribed_in_run = Rc::new(Cell::new(false));
    let n = {
        let unsub = unsub.clone();
        let flag = still_subscribed_in_run.clone();
        g.node::<i32, _>(vec![a.erased()], move |ctx| {
            let v = *ctx.dep(0).value::<i32>().unwrap();
            if v == 2 {
                if let Some(u) = unsub.borrow_mut().take() {
                    u();
                    flag.set(true);
                }
            }
            ctx.emit(v);
        })
    };
    let (log, u) = record_values(&n);
    *unsub.borrow_mut() = Some(u);
    a.set(2);
    assert!(still_subscribed_in_run.get());
    assert_eq!(
        log.borrow().last(),
        Some(&vec![2]),
        "the sink still got this wave: the unsubscribe waited until it ended"
    );
    assert_eq!(n.status().lifecycle, Lifecycle::Inactive, "then it ran");

    // Staging through a lifetime that closes before the commit is dropped. During a
    // wave the commit is a request; a replace requested before it closes P's
    // lifetime first.
    let (p, slot) = driven_in(&g);
    let (plog, _up) = record_values(&p);
    let trigger = g.state_empty::<i32>();
    let replace_first = Rc::new(Cell::new(true));
    let _ut = {
        let (p, slot, replace_first) = (p.clone(), slot.clone(), replace_first.clone());
        trigger.subscribe(move |msgs: &[Message<AnyValue>]| {
            if !msgs.iter().any(|m| matches!(m, Message::Data(_))) {
                return;
            }
            if replace_first.get() {
                p.replace(Vec::new(), |_| {}, graphrefly::ReplaceOpts::default());
            }
            g.batch(|_| send(&slot, vec![data(3)]));
        })
    };
    // Control: without the replace the staged output commits after the wave.
    replace_first.set(false);
    trigger.set(1);
    assert_eq!(plog.borrow().last(), Some(&vec![3]));
    // With the replace first, the staged output of the closed lifetime is dropped.
    replace_first.set(true);
    plog.borrow_mut().clear();
    trigger.set(2);
    assert!(
        plog.borrow().iter().all(|w| !w.contains(&3)),
        "output staged by a closed lifetime is dropped at the commit: {:?}",
        plog.borrow()
    );
}

/// C-29 — a batch delivers nothing before commit, and its topology requests follow the
/// commit. (Rust has no async batch body: `batch` takes a synchronous closure, so the
/// async and promise-returning clauses do not apply.)
#[test]
fn c29_batch_delivers_nothing_before_commit() {
    let g = graphrefly::graph();
    let a = g.state_opts(1i32, named("a"));
    let c = g.state_opts(100i32, named("c"));
    let kept: Rc<RefCell<Option<graphrefly::DeferredCtx>>> = Rc::new(RefCell::new(None));
    let b = {
        let kept = kept.clone();
        g.node_opts::<i32, _>(
            vec![a.erased()],
            move |ctx| {
                *kept.borrow_mut() = Some(ctx.defer());
                if let Some(v) = ctx.dep(0).values::<i32>().last() {
                    ctx.emit(**v);
                }
            },
            named("b"),
        )
    };
    let (s_log, _us) = record_arrays(&b);
    s_log.borrow_mut().clear();
    let t_log: Rc<RefCell<Vec<Vec<i32>>>> = Rc::new(RefCell::new(Vec::new()));
    let t_unsub: Rc<RefCell<Option<Unsub>>> = Rc::new(RefCell::new(None));
    let read = g.batch(|_| {
        a.set(2);
        let t_log = t_log.clone();
        *t_unsub.borrow_mut() = Some(b.subscribe(move |msgs: &[Message<AnyValue>]| {
            t_log.borrow_mut().push(
                msgs.iter()
                    .filter_map(|m| match m {
                        Message::Data(v) => v.downcast_ref::<i32>().copied(),
                        _ => None,
                    })
                    .collect(),
            )
        }));
        kept.borrow()
            .as_ref()
            .unwrap()
            .rewire(vec![a.erased(), c.erased()]);
        assert!(
            arrays(&s_log).is_empty(),
            "nothing is delivered in the body"
        );
        assert_eq!(deps_of(&g, "b"), vec!["a"], "nothing changes in the body");
        a.cache()
    });
    assert_eq!(
        read,
        Some(1),
        "the read in the body returns the retained value"
    );
    assert_eq!(
        arrays(&s_log),
        vec![shape(&["PENDING", "DATA"])],
        "one commit wave"
    );
    assert_eq!(
        t_log.borrow().first(),
        Some(&vec![2]),
        "t starts from the committed value"
    );
    assert_eq!(
        deps_of(&g, "b"),
        vec!["a", "c"],
        "the rewire ran after the commit"
    );

    // A throwing body delivers nothing and drops its sets and requests.
    s_log.borrow_mut().clear();
    let thrown = catch_unwind(AssertUnwindSafe(|| {
        g.batch(|_| {
            a.set(9);
            panic!("body boom");
        })
    }));
    assert!(thrown.is_err());
    assert!(arrays(&s_log).is_empty());
    assert_eq!(a.cache(), Some(2));

    // A nested batch joins the outer one and returns its value.
    s_log.borrow_mut().clear();
    let inner = g.batch(|_| {
        a.set(3);
        g.batch(|_| {
            a.set(4);
            7
        })
    });
    assert_eq!(inner, 7);
    assert_eq!(arrays(&s_log).len(), 1, "one commit wave");

    // Write functions run at the commit, in staging order.
    let seen: Rc<RefCell<Vec<Option<i32>>>> = Rc::new(RefCell::new(Vec::new()));
    let record_and = |next: Option<i32>| {
        let seen = seen.clone();
        graphrefly::StateWrite::from_fn(move |cur: Option<&i32>| {
            seen.borrow_mut().push(cur.copied());
            next
        })
    };
    g.batch(|_| {
        a.set(record_and(Some(20)));
        a.set(30);
        a.set(record_and(Some(40)));
        a.invalidate();
        a.set(record_and(Some(50)));
    });
    assert_eq!(*seen.borrow(), vec![Some(4), Some(30), None]);
    assert_eq!(a.cache(), Some(50));

    // A throwing function commits nothing, not even the set of B's input.
    let thrown = catch_unwind(AssertUnwindSafe(|| {
        g.batch(|_| {
            c.set(101);
            a.set(graphrefly::StateWrite::from_fn(
                |_: Option<&i32>| -> Option<i32> { panic!("write boom") },
            ));
        })
    }));
    assert!(thrown.is_err());
    assert_eq!(c.cache(), Some(100));
    assert_eq!(a.cache(), Some(50));

    // A function returning None writes nothing; an equal plain value starts a wave.
    s_log.borrow_mut().clear();
    a.set(graphrefly::StateWrite::from_fn(|_: Option<&i32>| None));
    assert!(arrays(&s_log).is_empty());
    a.set(50);
    assert_eq!(arrays(&s_log).len(), 1);

    // From a sink callback during a wave: the body runs at once and its set commits
    // after the current wave reached every subscriber.
    let order: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(Vec::new()));
    let returned = Rc::new(Cell::new(0i32));
    let first = Rc::new(Cell::new(true));
    let _cb = {
        let (a, order, returned, first) =
            (a.clone(), order.clone(), returned.clone(), first.clone());
        b.subscribe(move |msgs: &[Message<AnyValue>]| {
            if !msgs.iter().any(|m| matches!(m, Message::Data(_))) {
                return;
            }
            order.borrow_mut().push("cb".into());
            if first.replace(false) {
                let r = g.batch(|_| {
                    let seen = a.cache().unwrap_or(-1);
                    a.set(60);
                    seen
                });
                returned.set(r);
            }
        })
    };
    let _last = {
        let order = order.clone();
        b.subscribe(move |msgs: &[Message<AnyValue>]| {
            if msgs.iter().any(|m| matches!(m, Message::Data(_))) {
                order.borrow_mut().push("last".into());
            }
        })
    };
    order.borrow_mut().clear();
    first.set(true);
    a.set(55);
    assert_eq!(
        returned.get(),
        55,
        "the body read the value retained so far"
    );
    assert_eq!(
        *order.borrow(),
        vec!["cb", "last", "cb", "last"],
        "the commit is a later wave, after this wave reached every subscriber"
    );
    assert_eq!(a.cache(), Some(60));
}

/// C-28 — the request and batch clauses: sink requests run after the whole fanout,
/// rejected submissions leave the staging unchanged, and a sink that throws after a
/// commit goes to the host reporter and rolls nothing back.
#[test]
fn c28_sink_requests_follow_the_fanout_and_batches_keep_their_staging() {
    let reports = Rc::new(RefCell::new(Vec::<graphrefly::HostFailure>::new()));
    let g = reporting_graph(&reports);
    let a = state_in::<i32>(&g, 0);
    let other = state_in::<i32>(&g, 0);
    let order: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(Vec::new()));
    let late: Rc<RefCell<Option<Unsub>>> = Rc::new(RefCell::new(None));
    let _first = {
        let (other, order, late, a2) = (other.clone(), order.clone(), late.clone(), a.clone());
        a.subscribe(move |msgs: &[Message<AnyValue>]| {
            if !msgs.iter().any(|m| matches!(m, Message::Data(_))) {
                return;
            }
            order.borrow_mut().push("first".into());
            if late.borrow().is_none() && a2.cache() == Some(1) {
                other.set(5);
                let order = order.clone();
                *late.borrow_mut() =
                    Some(a2.subscribe(move |_| order.borrow_mut().push("late".into())));
            }
        })
    };
    let _second = {
        let (order, other) = (order.clone(), other.clone());
        a.subscribe(move |msgs: &[Message<AnyValue>]| {
            if msgs.iter().any(|m| matches!(m, Message::Data(_))) {
                order
                    .borrow_mut()
                    .push(format!("second other={:?}", other.cache()));
            }
        })
    };
    order.borrow_mut().clear();
    a.set(1);
    assert_eq!(
        *order.borrow(),
        vec!["first", "second other=Some(0)", "late"],
        "the set and the subscription ran after the whole fanout"
    );
    assert_eq!(other.cache(), Some(5));

    // Rejected submissions leave the earlier staging unchanged. (A State write cannot
    // carry RESOLVED at all; a kept ctx's output can, and is rejected.)
    let (sp, slot) = driven_in(&g);
    let (log, _u) = record_arrays(&sp);
    log.borrow_mut().clear();
    g.batch(|_| {
        send(&slot, vec![data(7)]);
        let resolved = catch_unwind(AssertUnwindSafe(|| send(&slot, vec![Message::Resolved])));
        assert!(resolved.is_err(), "RESOLVED is the kernel's");
        send(&slot, vec![Message::Complete]);
        let after = catch_unwind(AssertUnwindSafe(|| send(&slot, vec![data(8)])));
        assert!(after.is_err(), "nothing follows a staged terminal");
    });
    assert_eq!(arrays(&log), vec![shape(&["PENDING", "DATA", "COMPLETE"])]);

    // A sink that throws after a commit goes to the reporter; nothing rolls back.
    let t = state_in::<i32>(&g, 0);
    let _boom = t.subscribe(|msgs: &[Message<AnyValue>]| {
        if msgs.iter().any(|m| matches!(m, Message::Data(_))) {
            panic!("sink boom");
        }
    });
    g.batch(|_| t.set(3));
    assert_eq!(t.cache(), Some(3));
    assert!(reports.borrow().iter().any(|r| r.error == "sink boom"));
}

// ── Checkpoint stage: R-checkpoint@1 (graphrefly:D802) ───────────────────────

mod c38 {
    use super::*;
    use graphrefly::checkpoint::{GraphRestoreDescriptor, RestoreDefineCtx};
    use graphrefly::{
        graph, restore_graph, restore_registry, restored_opts, Graph, GraphCheckpoint,
        GraphCheckpointJson, GraphNodeOpts, GraphRestoreEntry, GraphRestoreResult,
        RestoreFactoryMeta, RestoreGraphOptions, RestoreNodeDefinition, RestoreNodeKind,
        StateRestoreDescriptor,
    };
    use serde_json::json;

    type Json = GraphCheckpointJson;

    /// Counters a test reads: function runs per recipe, timer starts and stops.
    #[derive(Default, Clone)]
    pub(super) struct Counters {
        pub p_runs: Rc<Cell<usize>>,
        pub p_inputs: Rc<RefCell<Vec<(usize, bool)>>>,
        pub timer_runs: Rc<Cell<usize>>,
        pub timers_started: Rc<Cell<usize>>,
        pub timers_stopped: Rc<Cell<usize>>,
        pub timer: Rc<RefCell<Option<graphrefly::DeferredCtx>>>,
    }

    /// p: sums the values it receives into its private state and sends the total.
    fn p_fn(counters: &Counters) -> impl Fn(&graphrefly::Ctx) + 'static {
        let counters = counters.clone();
        move |ctx| {
            counters.p_runs.set(counters.p_runs.get() + 1);
            for i in 0..ctx.dep_len() {
                counters.p_inputs.borrow_mut().push((
                    ctx.dep(i).values::<Json>().len(),
                    ctx.dep(i).terminal().is_some(),
                ));
            }
            let mut total = ctx
                .state_get::<Json>()
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            let batch = ctx.dep(0).values::<Json>();
            for v in &batch {
                total += v.as_i64().unwrap_or(0);
            }
            ctx.state_set(json!(total));
            if !batch.is_empty() {
                ctx.emit(json!(total));
            }
        }
    }

    fn fwd(ctx: &graphrefly::Ctx) {
        if let Some(v) = ctx.dep(0).values::<Json>().last() {
            ctx.emit((**v).clone());
        }
    }

    fn timer_opts(counters: &Counters, base: graphrefly::NodeOpts) -> graphrefly::NodeOpts {
        let counters = counters.clone();
        base.with_setup(move |ctx| {
            counters
                .timers_started
                .set(counters.timers_started.get() + 1);
            *counters.timer.borrow_mut() = Some(ctx.defer());
            let stopped = counters.timers_stopped.clone();
            graphrefly::cleanup(move |_| stopped.set(stopped.get() + 1))
        })
    }

    fn timer_fn(counters: &Counters) -> impl Fn(&graphrefly::Ctx) + 'static {
        let runs = counters.timer_runs.clone();
        move |_| runs.set(runs.get() + 1)
    }

    /// Fires the timer source's timer: it counts on in private state and sends it.
    pub(super) fn fire(counters: &Counters) {
        let ctx = counters.timer.borrow_mut().take().expect("timer started");
        let n = ctx
            .state_get::<Json>()
            .and_then(|v| v.as_i64())
            .unwrap_or(0)
            + 1;
        ctx.state_set(json!(n));
        ctx.down(vec![Message::Data(Rc::new(json!(n)) as AnyValue)]);
        counters.timer.borrow_mut().get_or_insert(ctx);
    }

    struct Recipe {
        ref_: &'static str,
        counters: Counters,
    }

    impl GraphRestoreDescriptor for Recipe {
        fn ref_(&self) -> &str {
            self.ref_
        }

        fn define(&self, ctx: RestoreDefineCtx<'_>) -> GraphRestoreResult<RestoreNodeDefinition> {
            let mut opts = restored_opts(ctx.checkpoint)?;
            let kind = match self.ref_ {
                "c38-p" => RestoreNodeKind::NodeJson(Rc::new(p_fn(&self.counters))),
                "c38-p2" => RestoreNodeKind::NodeJson(Rc::new(fwd)),
                "c38-fwd" => RestoreNodeKind::NodeJson(Rc::new(fwd)),
                "c38-timer" => {
                    opts.node = timer_opts(&self.counters, opts.node);
                    RestoreNodeKind::NodeJson(Rc::new(timer_fn(&self.counters)))
                }
                other => panic!("unknown recipe {other}"),
            };
            Ok(RestoreNodeDefinition {
                factory: self.ref_.to_owned(),
                kind,
                opts,
            })
        }
    }

    pub(super) fn registry(counters: &Counters) -> graphrefly::GraphRestoreRegistry {
        restore_registry(
            std::iter::once(GraphRestoreEntry::descriptor(StateRestoreDescriptor)).chain(
                ["c38-p", "c38-p2", "c38-fwd", "c38-timer"].map(|ref_| {
                    GraphRestoreEntry::descriptor(Recipe {
                        ref_,
                        counters: counters.clone(),
                    })
                }),
            ),
        )
    }

    pub(super) fn with_recipe(name: &str, ref_: &str) -> GraphNodeOpts {
        let mut opts = GraphNodeOpts::named(name);
        opts.restore = Some(RestoreFactoryMeta::registry_ref(ref_));
        opts
    }

    pub(super) fn restore(checkpoint: &GraphCheckpoint, counters: &Counters) -> Graph {
        restore_graph(
            checkpoint.clone(),
            RestoreGraphOptions::new(registry(counters)),
        )
        .expect("restore succeeds")
    }

    pub(super) fn state_of(g: &Graph, id: &str) -> graphrefly::State<Json> {
        g.find(id)
            .and_then(|n| n.as_state::<Json>())
            .unwrap_or_else(|| panic!("{id} is a restored state"))
    }

    pub(super) fn json_node(g: &Graph, id: &str) -> Node<Json> {
        g.find(id)
            .unwrap_or_else(|| panic!("{id} restored"))
            .as_node::<Json>()
    }

    /// The original graph: state a; p over a; an ended fan-out; the timer source.
    pub(super) fn build(counters: &Counters) -> (Graph, Node<Json>, Vec<Unsub>) {
        let g = graph();
        let a = g.state_opts(json!(1), GraphNodeOpts::named("a"));
        let p = g.node_opts::<Json, _>(vec![a.erased()], p_fn(counters), with_recipe("p", "c38-p"));
        let z = g.state_opts(json!(7), GraphNodeOpts::named("z"));
        let ended = g.node_opts::<Json, _>(vec![z.erased()], fwd, with_recipe("ended", "c38-fwd"));
        let timer = g.node_opts::<Json, _>(Vec::new(), timer_fn(counters), {
            let mut opts = with_recipe("timer", "c38-timer");
            opts.node = timer_opts(counters, opts.node);
            opts
        });
        let mut keep = vec![
            p.subscribe(|_| {}),
            ended.subscribe(|_| {}),
            timer.subscribe(|_| {}),
        ];
        z.complete();
        fire(counters);
        fire(counters);
        keep.push(Box::new(|| {}));
        (g, p, keep)
    }
}

/// C-38 — capture and restore (R-checkpoint@1).
#[test]
fn c38_capture_and_restore() {
    use c38::*;
    use graphrefly::restore_graph;
    use graphrefly::GraphCheckpointFactory;
    use serde_json::json;
    let counters = Counters::default();
    let (g, p, _keep) = build(&counters);
    assert_eq!(p.cache(), Some(json!(1)));
    assert_eq!(counters.timers_started.get(), 1);
    let checkpoint = g.checkpoint().expect("capture at a wave boundary");
    let serialized = serde_json::to_value(&checkpoint).unwrap();
    for node in serialized["nodes"].as_array().unwrap() {
        assert!(node.get("version").is_none(), "no version evidence");
        assert!(node.get("status").is_none());
    }

    // Restore runs no function, sends nothing, and gives the same topology.
    let runs_before = counters.p_runs.get();
    let restored = restore(&checkpoint, &counters);
    assert_eq!(
        counters.p_runs.get(),
        runs_before,
        "no function runs at restore"
    );
    let edges = |g: &graphrefly::Graph| {
        let mut e: Vec<(String, String)> = g
            .topology()
            .edges
            .into_iter()
            .map(|e| (e.from, e.to))
            .collect();
        e.sort();
        e
    };
    let mut saved: Vec<(String, String)> = checkpoint
        .edges
        .iter()
        .map(|e| (e.from.clone(), e.to.clone()))
        .collect();
    saved.sort();
    assert_eq!(edges(&restored), saved);

    // The first subscription gets p's saved values; p keeps its private state and
    // does not run.
    let rp = json_node(&restored, "p");
    let (log, _u) = record_arrays(&rp);
    assert_eq!(arrays(&log), vec![shape(&["START", "DATA"])]);
    assert_eq!(rp.cache(), Some(json!(1)));
    assert_eq!(
        counters.p_runs.get(),
        runs_before,
        "unchanged input: no run"
    );
    state_of(&restored, "a").set(json!(5));
    assert_eq!(
        rp.cache(),
        Some(json!(6)),
        "the saved private state continued"
    );

    // A changed a before the first subscription: one run with only a as input.
    let restored = restore(&checkpoint, &counters);
    state_of(&restored, "a").set(json!(3));
    counters.p_inputs.borrow_mut().clear();
    let runs = counters.p_runs.get();
    let rp = json_node(&restored, "p");
    let _u = rp.subscribe(|_| {});
    assert_eq!(counters.p_runs.get(), runs + 1);
    assert_eq!(*counters.p_inputs.borrow(), vec![(1, false)]);
    assert_eq!(rp.cache(), Some(json!(4)));

    // INVALIDATE then a set before the first subscription is seen as the set alone.
    let restored = restore(&checkpoint, &counters);
    let ra = state_of(&restored, "a");
    ra.invalidate();
    ra.set(json!(2));
    counters.p_inputs.borrow_mut().clear();
    let rp = json_node(&restored, "p");
    let _u = rp.subscribe(|_| {});
    assert_eq!(*counters.p_inputs.borrow(), vec![(1, false)]);
    assert_eq!(rp.cache(), Some(json!(3)));

    // A dependency that only completed brings its COMPLETE without its values.
    let restored = restore(&checkpoint, &counters);
    state_of(&restored, "a").complete();
    let runs = counters.p_runs.get();
    let rp = json_node(&restored, "p");
    let (log, _u) = record_arrays(&rp);
    assert_eq!(
        counters.p_runs.get(),
        runs + 1,
        "auto COMPLETE: the run that sees the last terminal calls the function (review Q12)"
    );
    assert_eq!(rp.status().output, OutputStatus::Completed);
    assert_eq!(rp.cache(), Some(json!(1)), "the saved value is kept");
    assert!(arrays(&log).iter().flatten().any(|k| k == "COMPLETE"));

    // The ended node comes back ended, with its dependencies, without private state.
    let ended = checkpoint.nodes.iter().find(|n| n.id == "ended").unwrap();
    assert_eq!(
        ended.terminal,
        graphrefly::GraphCheckpointTerminal::Complete
    );
    assert_eq!(ended.deps, vec!["z".to_owned()]);
    assert_eq!(ended.private_state, None);
    let restored = restore(&checkpoint, &counters);
    let (log, _u) = record_arrays(&json_node(&restored, "ended"));
    assert_eq!(arrays(&log), vec![shape(&["START", "DATA", "COMPLETE"])]);

    // The timer source: its setup runs once at the first activation, its function
    // does not run, and it counts on from its saved private state.
    let restored = restore(&checkpoint, &counters);
    let started = counters.timers_started.get();
    let timer_runs = counters.timer_runs.get();
    let rt = json_node(&restored, "timer");
    let (tlog, _ut) = record_values_json(&rt);
    assert_eq!(
        counters.timers_started.get(),
        started + 1,
        "one timer started"
    );
    assert_eq!(
        counters.timer_runs.get(),
        timer_runs,
        "its function does not run"
    );
    fire(&counters);
    assert_eq!(tlog.borrow().last(), Some(&vec![json!(3)]));

    // A restored graph captured again before anything subscribes restores the same.
    let restored = restore(&checkpoint, &counters);
    let again = restored.checkpoint().expect("capture of a restored graph");
    assert_eq!(again.nodes, checkpoint.nodes);

    // Capture is refused inside a wave and inside a batch body.
    let in_batch = g.batch(|_| g.checkpoint().map(|_| ()));
    assert!(in_batch.is_err());
    let in_wave: Rc<RefCell<Option<bool>>> = Rc::new(RefCell::new(None));
    // The trigger is in g: a capture of g during g's own wave is refused.
    let trigger = g.state_empty::<i32>();
    let _ut = {
        let (g, in_wave) = (g.clone(), in_wave.clone());
        trigger.subscribe(move |msgs: &[Message<AnyValue>]| {
            if msgs.iter().any(|m| matches!(m, Message::Data(_))) {
                *in_wave.borrow_mut() = Some(g.checkpoint().is_err());
            }
        })
    };
    trigger.set(1);
    assert_eq!(*in_wave.borrow(), Some(true), "refused inside a wave");

    // Refusals: a missing recipe, a local-only node, a missing received record, and
    // values or private state that are not JSON.
    let missing = restore_graph(
        checkpoint.clone(),
        graphrefly::RestoreGraphOptions::new(graphrefly::restore_registry([
            graphrefly::GraphRestoreEntry::descriptor(graphrefly::StateRestoreDescriptor),
        ])),
    );
    assert!(missing.is_err());
    let mut old_format = checkpoint.clone();
    old_format
        .nodes
        .iter_mut()
        .find(|n| n.id == "p")
        .unwrap()
        .received = None;
    let refused = restore_graph(
        old_format,
        graphrefly::RestoreGraphOptions::new(registry(&counters)),
    );
    assert!(refused.is_err_and(|e| e.to_string().contains("received")));
    // Without private state too (a plain map node with values): still refused, not
    // completed from its dependencies (review F 2.7, C F8).
    let mut plain = checkpoint.clone();
    {
        let p = plain.nodes.iter_mut().find(|n| n.id == "p").unwrap();
        p.private_state = None;
        p.received = None;
        assert!(!p.values.is_empty());
    }
    let refused = restore_graph(
        plain,
        graphrefly::RestoreGraphOptions::new(registry(&counters)),
    );
    assert!(refused.is_err_and(|e| e.to_string().contains("received")));
    assert_eq!(
        checkpoint.version, "graphrefly-rs/checkpoint@1",
        "the tag names the package"
    );
    for other in [
        "graphrefly.checkpoint.v1",
        "graphrefly.checkpoint.v2",
        "graphrefly-ts/checkpoint@1",
    ] {
        let mut tagged = checkpoint.clone();
        tagged.version = other.into();
        assert!(
            restore_graph(
                tagged,
                graphrefly::RestoreGraphOptions::new(registry(&counters))
            )
            .is_err(),
            "{other} is refused"
        );
    }

    let local = graphrefly::graph();
    let la = local.state_opts(json!(1), graphrefly::GraphNodeOpts::named("a"));
    let ln = local.node_opts::<serde_json::Value, _>(
        vec![la.erased()],
        |ctx| {
            if let Some(v) = ctx.dep(0).values::<serde_json::Value>().last() {
                ctx.emit((**v).clone());
            }
        },
        graphrefly::GraphNodeOpts::named("local"),
    );
    let _ul = ln.subscribe(|_| {});
    let cp = local.checkpoint().unwrap();
    assert!(matches!(
        cp.nodes.iter().find(|n| n.id == "local").unwrap().factory,
        GraphCheckpointFactory::LocalOnly { .. }
    ));
    assert!(restore_graph(
        cp,
        graphrefly::RestoreGraphOptions::new(registry(&counters))
    )
    .is_err());

    let bad = graphrefly::graph();
    bad.state_opts(
        std::time::Duration::from_secs(1),
        graphrefly::GraphNodeOpts::named("typed"),
    );
    assert!(bad
        .checkpoint()
        .unwrap_err()
        .to_string()
        .starts_with("not restorable: typed: its retained values are not JSON"));
    let bad_state = graphrefly::graph();
    let src = bad_state.state_opts(json!(1), graphrefly::GraphNodeOpts::named("a"));
    let holder = bad_state.node_opts::<serde_json::Value, _>(
        vec![src.erased()],
        |ctx| ctx.state_set(std::time::Duration::from_secs(1)),
        with_recipe("holder", "c38-fwd"),
    );
    let _uh = holder.subscribe(|_| {});
    assert_eq!(
        bad_state.checkpoint().unwrap_err().to_string(),
        "not restorable: holder: its private state is not JSON"
    );
}

/// C-38 — the replace and rewire clauses: the recipe belongs to the instance, a node
/// replaced onto a later dependency is restored after it, and a rewired node is
/// restored onto its lifetime's dependencies.
#[test]
fn c38_replace_and_rewire_are_restored_as_the_instance() {
    use c38::*;
    use graphrefly::restore_graph;
    use graphrefly::GraphCheckpointFactory;
    use serde_json::json;
    let counters = Counters::default();
    let g = graphrefly::graph();
    let a = g.state_opts(json!(1), graphrefly::GraphNodeOpts::named("a"));
    let fwd = |ctx: &graphrefly::Ctx| {
        if let Some(v) = ctx.dep(0).values::<serde_json::Value>().last() {
            ctx.emit((**v).clone());
        }
    };
    let n1 =
        g.node_opts::<serde_json::Value, _>(vec![a.erased()], fwd, with_recipe("n1", "c38-fwd"));
    let n2 =
        g.node_opts::<serde_json::Value, _>(vec![a.erased()], fwd, with_recipe("n2", "c38-fwd"));
    let n3 =
        g.node_opts::<serde_json::Value, _>(vec![a.erased()], fwd, with_recipe("n3", "c38-fwd"));
    let later = g.state_opts(json!(9), graphrefly::GraphNodeOpts::named("later"));
    let b = g.state_opts(json!(100), graphrefly::GraphNodeOpts::named("b"));
    let kept: Rc<RefCell<Option<graphrefly::DeferredCtx>>> = Rc::new(RefCell::new(None));
    let r = {
        let kept = kept.clone();
        g.node_opts::<serde_json::Value, _>(
            vec![a.erased()],
            move |ctx| {
                *kept.borrow_mut() = Some(ctx.defer());
                fwd(ctx);
            },
            with_recipe("r", "c38-fwd"),
        )
    };
    let _keep: Vec<Unsub> = [&n1, &n2, &n3, &r]
        .iter()
        .map(|n| n.subscribe(|_| {}))
        .collect();
    // Replace keeping deps with a recipe; replace without a recipe; replace onto a
    // dependency made after the node.
    n1.replace(
        vec![a.erased()],
        fwd,
        graphrefly::ReplaceOpts {
            recipe: Some(json!({ "ref": "c38-p2" })),
            ..graphrefly::ReplaceOpts::default()
        },
    );
    n2.replace(vec![a.erased()], fwd, graphrefly::ReplaceOpts::default());
    n3.replace(
        vec![later.erased()],
        fwd,
        graphrefly::ReplaceOpts {
            recipe: Some(json!({ "ref": "c38-fwd" })),
            ..graphrefly::ReplaceOpts::default()
        },
    );
    let r_ctx = kept.borrow_mut().take().unwrap();
    r_ctx.rewire(vec![a.erased(), b.erased()]);

    let checkpoint = g.checkpoint().unwrap();
    let node = |id: &str| {
        checkpoint
            .nodes
            .iter()
            .find(|n| n.id == id)
            .unwrap()
            .clone()
    };
    assert!(
        matches!(&node("n1").factory, GraphCheckpointFactory::RegistryRef { ref_, .. } if ref_ == "c38-p2")
    );
    assert!(matches!(
        node("n2").factory,
        GraphCheckpointFactory::LocalOnly { .. }
    ));
    assert_eq!(node("n3").deps, vec!["later".to_owned()]);
    assert_eq!(
        node("r").deps,
        vec!["a".to_owned()],
        "declared dependencies"
    );
    assert_eq!(
        node("r").lifetime_deps,
        Some(vec!["a".to_owned(), "b".to_owned()])
    );

    // n2 is local-only, so this checkpoint does not restore; without it, it does.
    assert!(restore_graph(
        checkpoint.clone(),
        graphrefly::RestoreGraphOptions::new(registry(&counters))
    )
    .is_err());
    let mut without_local = checkpoint.clone();
    without_local.nodes.retain(|n| n.id != "n2");
    without_local.edges.retain(|e| e.to != "n2");
    let restored = restore_graph(
        without_local,
        graphrefly::RestoreGraphOptions::new(registry(&counters)),
    )
    .expect("restores after each node's dependencies");
    let rr = json_node(&restored, "r");
    let _ur = rr.subscribe(|_| {});
    let deps: Vec<String> = restored
        .topology()
        .edges
        .into_iter()
        .filter(|e| e.to == "r")
        .map(|e| e.from)
        .collect();
    assert_eq!(
        deps,
        vec!["a".to_owned(), "b".to_owned()],
        "restored onto the lifetime's deps"
    );
    let rn3 = json_node(&restored, "n3");
    let _u3 = rn3.subscribe(|_| {});
    assert_eq!(rn3.cache(), Some(json!(9)));
}

/// The DATA payloads (as JSON) of every array a sink received.
fn record_values_json(
    node: &Node<serde_json::Value>,
) -> (Rc<RefCell<Vec<Vec<serde_json::Value>>>>, Unsub) {
    let log: Rc<RefCell<Vec<Vec<serde_json::Value>>>> = Rc::new(RefCell::new(Vec::new()));
    let sink = log.clone();
    let unsub = node.subscribe(move |msgs: &[Message<AnyValue>]| {
        sink.borrow_mut().push(
            msgs.iter()
                .filter_map(|m| match m {
                    Message::Data(v) => v.downcast_ref::<serde_json::Value>().cloned(),
                    _ => None,
                })
                .collect(),
        );
    });
    (log, unsub)
}

/// A graph whose host reporter records into `reports` (R-failure@3: the reporter is
/// the graph's `report` option, graphrefly-rs:D6).
fn reporting_graph(reports: &Rc<RefCell<Vec<graphrefly::HostFailure>>>) -> graphrefly::Graph {
    let reports = reports.clone();
    graphrefly::graph_opts(
        graphrefly::GraphOptions::default()
            .with_report(move |f: &graphrefly::HostFailure| reports.borrow_mut().push(f.clone())),
    )
}

fn state_in<T: 'static>(g: &graphrefly::Graph, v: T) -> graphrefly::State<T> {
    g.state_opts(v, graphrefly::GraphNodeOpts::default())
}

fn derived_in<T: 'static>(
    g: &graphrefly::Graph,
    deps: Vec<graphrefly::Core>,
    opts: graphrefly::NodeOpts,
    f: impl Fn(&graphrefly::Ctx) + 'static,
) -> Node<T> {
    g.node_opts::<T, _>(
        deps,
        f,
        graphrefly::GraphNodeOpts::default().with_node(opts),
    )
}

fn producer_in<T: 'static>(
    g: &graphrefly::Graph,
    opts: graphrefly::NodeOpts,
    f: impl Fn(&graphrefly::Ctx) + 'static,
) -> Node<T> {
    g.producer_opts(f, graphrefly::GraphNodeOpts::default().with_node(opts))
}

/// A producer in `g` whose kept ctx lets a test send any array from it.
fn driven_in(g: &graphrefly::Graph) -> (Node<i32>, Rc<RefCell<Option<graphrefly::DeferredCtx>>>) {
    let slot: Rc<RefCell<Option<graphrefly::DeferredCtx>>> = Rc::new(RefCell::new(None));
    let keep = slot.clone();
    let node = producer_in(g, graphrefly::NodeOpts::default(), move |ctx| {
        *keep.borrow_mut() = Some(ctx.defer());
    });
    (node, slot)
}

/// R-failure@3 with R-graph-domain@2: the host reporter is the graph's `report` option.
/// A failure goes to its own graph's reporter, never another's, and a graph without a
/// reporter keeps nothing of its failures (it writes them to standard error).
#[test]
fn host_reporter_is_the_graph_option() {
    let g = graphrefly::graph();
    let reports_a = Rc::new(RefCell::new(Vec::<graphrefly::HostFailure>::new()));
    let reports_b = Rc::new(RefCell::new(Vec::<graphrefly::HostFailure>::new()));
    let ga = reporting_graph(&reports_a);
    let gb = reporting_graph(&reports_b);
    let sa = ga.state_empty::<i32>();
    let sb = state_in(&gb, 0i32);
    let _ua = sa.subscribe(|msgs: &[Message<AnyValue>]| {
        if msgs.iter().any(|m| matches!(m, Message::Data(_))) {
            panic!("a sink");
        }
    });
    let pb = producer_in::<i32>(&gb, with_cleanup(|| panic!("b cleanup")), |_| {});
    let ub = pb.subscribe(|_| {});
    sa.set(1);
    ub();
    sb.set(1);
    let errors = |r: &Rc<RefCell<Vec<graphrefly::HostFailure>>>| {
        r.borrow()
            .iter()
            .map(|f| (f.source.clone(), f.error.clone()))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        errors(&reports_a),
        vec![("sink".to_owned(), "a sink".to_owned())]
    );
    assert_eq!(
        errors(&reports_b),
        vec![("cleanup".to_owned(), "b cleanup".to_owned())]
    );

    // A graph without a reporter: nothing is kept.
    let lone = g.state::<i32>(0);
    let _ul = lone.subscribe(|msgs: &[Message<AnyValue>]| {
        if msgs.iter().any(|m| matches!(m, Message::Data(_))) {
            panic!("lone sink");
        }
    });
    lone.set(1);
    assert_eq!(lone.cache(), Some(1));
    assert_eq!(reports_a.borrow().len(), 1);
    assert_eq!(reports_b.borrow().len(), 1);
}

/// C-47 — graphs on one thread share no wave, request queue or batch
/// (R-graph-domain@3).
#[test]
fn c47_graphs_on_one_thread_share_no_wave_queue_or_batch() {
    use serde_json::{json, Value};
    let g1 = graphrefly::graph();
    let g2 = graphrefly::graph();
    let a = g1.state_opts(json!(0), graphrefly::GraphNodeOpts::named("a"));
    let c = g1.state_opts(json!(0), graphrefly::GraphNodeOpts::named("c"));
    let b = g2.state_opts(json!(0), graphrefly::GraphNodeOpts::named("b"));
    let order: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(Vec::new()));
    let captures: Rc<RefCell<Option<(bool, bool)>>> = Rc::new(RefCell::new(None));
    let first = Rc::new(Cell::new(true));
    let _s1 = {
        let (b, g1, g2, order, captures, first) = (
            b.clone(),
            g1.clone(),
            g2.clone(),
            order.clone(),
            captures.clone(),
            first.clone(),
        );
        a.subscribe(move |msgs: &[Message<AnyValue>]| {
            let is_one = msgs.iter().any(
                |m| matches!(m, Message::Data(v) if v.downcast_ref::<Value>() == Some(&json!(1))),
            );
            if !is_one || !first.replace(false) {
                return;
            }
            order.borrow_mut().push("s1 writes b".into());
            b.set(json!(1));
            order
                .borrow_mut()
                .push(format!("s1 after write, b={}", b.cache().unwrap()));
            *captures.borrow_mut() = Some((g1.checkpoint().is_ok(), g2.checkpoint().is_ok()));
            order.borrow_mut().push("a's wave ends".into());
        })
    };
    let _s2 = {
        let (c, order) = (c.clone(), order.clone());
        b.subscribe(move |msgs: &[Message<AnyValue>]| {
            if let Some(Message::Data(v)) = msgs.iter().find(|m| matches!(m, Message::Data(_))) {
                let v = v.downcast_ref::<Value>().cloned().unwrap();
                if v == json!(1) {
                    order.borrow_mut().push(format!("s2 sees b={v}"));
                    c.set(json!(9));
                    order.borrow_mut().push("s2 after write of c".into());
                }
            }
        })
    };
    let _sc = {
        let order = order.clone();
        c.subscribe(move |msgs: &[Message<AnyValue>]| {
            if msgs.iter().any(
                |m| matches!(m, Message::Data(v) if v.downcast_ref::<Value>() == Some(&json!(9))),
            ) {
                order.borrow_mut().push("c's wave".into());
            }
        })
    };
    order.borrow_mut().clear();
    a.set(json!(1));
    assert_eq!(
        *order.borrow(),
        vec![
            "s1 writes b",
            "s2 sees b=1",
            "s2 after write of c",
            "s1 after write, b=1",
            "a's wave ends",
            "c's wave",
        ],
        "b's wave runs at once; the write of c waits for g1's wave"
    );
    assert_eq!(
        *captures.borrow(),
        Some((false, true)),
        "g1 is in its wave, g2 is idle"
    );

    // In a g1 batch body, the write of b is g2's own request and runs at once.
    let (a_log, _ua) = record_arrays(&a);
    a_log.borrow_mut().clear();
    let read = g1.batch(|_| {
        a.set(json!(2));
        b.set(json!(5));
        let read = b.cache();
        assert!(arrays(&a_log).is_empty(), "a's write waits for the commit");
        read
    });
    assert_eq!(read, Some(json!(5)), "the read sees b's new value");
    assert_eq!(arrays(&a_log), vec![shape(&["PENDING", "DATA"])]);
}

// ── Review fixes (REVIEW-D797-migration, graphrefly-rs:D797-KERNEL-MIGRATION-RS) ──

fn quiet_opts() -> graphrefly::NodeOpts {
    graphrefly::NodeOpts {
        quiet: true,
        ..graphrefly::NodeOpts::default()
    }
}

/// R-lifetime@2 (review F 2.3): output a kept ctx sends during a wave is a request,
/// and is dropped if its lifetime has closed by the time it runs, even when a new
/// lifetime has opened meanwhile.
#[test]
fn queued_output_of_a_closed_lifetime_is_dropped() {
    let g = graphrefly::graph();
    let (n, slot) = driven_in(&g);
    let trigger = state_in(&g, 0i32);
    let old_unsub: Rc<RefCell<Option<Unsub>>> = Rc::new(RefCell::new(Some(n.subscribe(|_| {}))));
    let old = slot.borrow_mut().take().expect("first lifetime ran");
    let fresh: Rc<RefCell<Vec<Vec<String>>>> = Rc::new(RefCell::new(Vec::new()));
    let new_unsub: Rc<RefCell<Option<Unsub>>> = Rc::new(RefCell::new(None));
    let _ut = {
        let (n, fresh, old_unsub, new_unsub) = (
            n.clone(),
            fresh.clone(),
            old_unsub.clone(),
            new_unsub.clone(),
        );
        trigger.subscribe(move |msgs: &[Message<AnyValue>]| {
            let start = msgs.iter().any(|m| matches!(m, Message::Start));
            if start || !msgs.iter().any(|m| matches!(m, Message::Data(_))) {
                return;
            }
            // All three are requests, run after this wave in this order.
            if let Some(u) = old_unsub.borrow_mut().take() {
                u();
            }
            let sink = fresh.clone();
            *new_unsub.borrow_mut() = Some(n.subscribe(move |m: &[Message<AnyValue>]| {
                sink.borrow_mut()
                    .push(m.iter().map(|x| kind_of(x).to_owned()).collect());
            }));
            old.down(vec![data(999)]);
        })
    };
    trigger.set(1);
    assert_eq!(
        *fresh.borrow(),
        vec![shape(&["START"])],
        "the old lifetime's output does not reach the new one"
    );
    assert_eq!(n.cache(), None);
}

/// R-lifetime@2 with R-wave@2 (review F 2.3): a quiet node's output is a request made
/// through its lifetime; when the node loses its last subscriber in the same wave,
/// the output is dropped and the inactive node holds no value.
#[test]
fn quiet_output_after_its_lifetime_closed_is_dropped() {
    let g = graphrefly::graph();
    let a = state_in(&g, 1i32);
    let q_unsub: Rc<RefCell<Option<Unsub>>> = Rc::new(RefCell::new(None));
    // Subscribed to `a` before Q is, so this sink's request comes first.
    let _ua = {
        let q_unsub = q_unsub.clone();
        a.subscribe(move |msgs: &[Message<AnyValue>]| {
            if msgs.iter().any(|m| matches!(m, Message::Data(_))) {
                if let Some(u) = q_unsub.borrow_mut().take() {
                    u();
                }
            }
        })
    };
    let q = derived_in::<i32>(&g, vec![a.erased()], quiet_opts(), |ctx| {
        let v = ctx.dep(0).value::<i32>().map_or(0, |v| *v);
        ctx.emit(v * 10);
    });
    *q_unsub.borrow_mut() = Some(q.subscribe(|_| {}));
    a.set(2);
    assert_eq!(q.status().lifecycle, graphrefly::Lifecycle::Inactive);
    assert_eq!(q.cache(), None, "the closed lifetime's output was dropped");
}

/// R-wave@2 with R-request-order@2 (review F 2.4): a quiet node's output waits until
/// the wave ends and then runs in the order the requests were made.
#[test]
fn quiet_output_runs_in_request_order() {
    let g = graphrefly::graph();
    let a = state_in(&g, 0i32);
    let other = state_in(&g, 0i32);
    let order: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(Vec::new()));
    // A sink of `a` asks for `other.set(99)` before Q settles in the same wave.
    let _ua = {
        let other = other.clone();
        a.subscribe(move |msgs: &[Message<AnyValue>]| {
            if msgs.iter().any(|m| matches!(m, Message::Data(_))) {
                other.set(99);
            }
        })
    };
    let q = derived_in::<i32>(&g, vec![a.erased()], quiet_opts(), |ctx| {
        let v = ctx.dep(0).value::<i32>().map_or(0, |v| *v);
        ctx.emit(v + 100);
    });
    let sink = |name: &'static str| {
        let order = order.clone();
        move |msgs: &[Message<AnyValue>]| {
            if msgs.iter().any(|m| matches!(m, Message::Data(_)))
                && !msgs.iter().any(|m| matches!(m, Message::Start))
            {
                order.borrow_mut().push(name.to_owned());
            }
        }
    };
    let _uq = q.subscribe(sink("q"));
    let _uo = other.subscribe(sink("other"));
    order.borrow_mut().clear();
    a.set(1);
    assert_eq!(*order.borrow(), vec!["other", "q"]);
    assert_eq!(q.cache(), Some(101));
}

/// R-wave@2 (the Rust arm of review F 2.10): the auto INVALIDATE a quiet node passes
/// on now marks its cone pending, so a diamond below settles once.
#[test]
fn quiet_auto_invalidate_settles_a_diamond_once() {
    let g = graphrefly::graph();
    let a = state_in(&g, 1i32);
    let q = derived_in::<i32>(&g, vec![a.erased()], quiet_opts(), |ctx| {
        let v = ctx.dep(0).value::<i32>().map_or(0, |v| *v);
        ctx.emit(v);
    });
    let fwd = |ctx: &graphrefly::Ctx| {
        let v = ctx.dep(0).value::<i32>().map_or(0, |v| *v);
        ctx.emit(v);
    };
    let b = derived_in::<i32>(&g, vec![q.erased()], graphrefly::NodeOpts::default(), fwd);
    let c = derived_in::<i32>(&g, vec![q.erased()], graphrefly::NodeOpts::default(), fwd);
    let runs = Rc::new(Cell::new(0usize));
    let d = {
        let runs = runs.clone();
        derived_in::<i32>(
            &g,
            vec![b.erased(), c.erased()],
            graphrefly::NodeOpts {
                invalidate: graphrefly::Policy::Manual,
                ..graphrefly::NodeOpts::default()
            },
            move |_ctx| runs.set(runs.get() + 1),
        )
    };
    let _ud = d.subscribe(|_| {});
    assert_eq!(b.cache(), Some(1));
    let before = runs.get();
    a.invalidate();
    assert_eq!(b.cache(), None);
    assert_eq!(runs.get(), before + 1, "D settles once on the diamond");
}

/// R-terminals@1 with R-failure@3 (review F 2.8): a write queued behind a terminal is
/// refused when it runs; the refusal goes to the reporter and the write function does
/// not run.
#[test]
fn a_write_queued_behind_a_terminal_is_refused_and_reported() {
    let reports = Rc::new(RefCell::new(Vec::<graphrefly::HostFailure>::new()));
    let g = reporting_graph(&reports);
    let trigger = state_in(&g, 0i32);
    let s = state_in(&g, 1i32);
    let ran: Rc<RefCell<Vec<Option<i32>>>> = Rc::new(RefCell::new(Vec::new()));
    let _ut = {
        let (s, ran) = (s.clone(), ran.clone());
        trigger.subscribe(move |msgs: &[Message<AnyValue>]| {
            let start = msgs.iter().any(|m| matches!(m, Message::Start));
            if start || !msgs.iter().any(|m| matches!(m, Message::Data(_))) {
                return;
            }
            s.complete();
            let ran = ran.clone();
            s.set(graphrefly::StateWrite::from_fn(move |v: Option<&i32>| {
                ran.borrow_mut().push(v.copied());
                Some(2)
            }));
            s.set(3);
        })
    };
    trigger.set(1);
    assert!(ran.borrow().is_empty(), "the write function did not run");
    assert_eq!(s.status().output, graphrefly::OutputStatus::Completed);
    assert_eq!(s.cache(), Some(1));
    let errors: Vec<(String, String)> = reports
        .borrow()
        .iter()
        .map(|f| (f.source.clone(), f.error.clone()))
        .collect();
    let refused = (
        "queued request".to_owned(),
        "host input to an ended node is refused (R-terminals)".to_owned(),
    );
    assert_eq!(errors, vec![refused.clone(), refused]);
}

/// R-request-order@2 (review F 2.9, C F5): a queued replace is checked again when it
/// runs, on the topology the requests before it committed, so two replaces in one
/// batch cannot commit a cycle.
#[test]
fn queued_replaces_cannot_commit_a_cycle() {
    let reports = Rc::new(RefCell::new(Vec::<graphrefly::HostFailure>::new()));
    let g = reporting_graph(&reports);
    let a = producer_in::<i32>(&g, graphrefly::NodeOpts::default(), |_| {});
    let b = producer_in::<i32>(&g, graphrefly::NodeOpts::default(), |_| {});
    let c = producer_in::<i32>(&g, graphrefly::NodeOpts::default(), |_| {});
    let noop = |_: &graphrefly::Ctx| {};
    g.batch(|_| {
        a.replace(vec![b.erased()], noop, graphrefly::ReplaceOpts::default());
        b.replace(vec![c.erased()], noop, graphrefly::ReplaceOpts::default());
        // Legal when asked for; a cycle a -> b -> c -> a once the two above have run.
        c.replace(vec![a.erased()], noop, graphrefly::ReplaceOpts::default());
    });
    let deps = |n: &Node<i32>| n.definition().deps.len();
    assert_eq!((deps(&a), deps(&b), deps(&c)), (1, 1, 0));
    let errors: Vec<(String, String)> = reports
        .borrow()
        .iter()
        .map(|f| (f.source.clone(), f.error.clone()))
        .collect();
    assert_eq!(
        errors,
        vec![(
            "queued request".to_owned(),
            "replace: the node would depend on itself (R-request-order)".to_owned()
        )]
    );
}

/// R-node@2 (review F 2.16, C F12): the definition gives the current instance's
/// function, and describe gives each node's kind, dependencies and whether it is
/// quiet.
#[test]
fn definition_and_describe_give_the_r_node_content() {
    let g = graphrefly::graph();
    let a = g.state_opts(1i32, named("a"));
    let q = g.node_opts::<i32, _>(vec![a.erased()], |_| {}, named("q").with_node(quiet_opts()));
    assert!(a.definition().function.is_none(), "a state has no function");
    let first = q.definition().function.expect("q has a function");
    q.replace(vec![a.erased()], |_| {}, graphrefly::ReplaceOpts::default());
    let second = q
        .definition()
        .function
        .expect("the new instance's function");
    assert!(!Rc::ptr_eq(&first, &second), "replace gives a new function");

    let described: Vec<(String, graphrefly::DescribeKind, Vec<String>, bool)> = g
        .topology()
        .nodes
        .into_iter()
        .map(|n| (n.id, n.kind, n.deps, n.quiet))
        .collect();
    assert_eq!(
        described,
        vec![
            (
                "a".to_owned(),
                graphrefly::DescribeKind::State,
                vec![],
                false
            ),
            (
                "q".to_owned(),
                graphrefly::DescribeKind::Node,
                vec!["a".to_owned()],
                true
            ),
        ]
    );
}

/// R-checkpoint@1, the Rust limit recorded in graphrefly-rs:D9 (review F 2.18): restored
/// values are `serde_json::Value`, whatever type the saved graph used.
#[test]
fn restored_values_are_json_whatever_the_saved_type() {
    let g = graphrefly::graph();
    let _a = g.state_opts(41i32, named("a"));
    let checkpoint = g.checkpoint().expect("an i32 is saved as JSON");
    let restored = graphrefly::restore_graph(
        checkpoint,
        graphrefly::RestoreGraphOptions::new(graphrefly::restore_registry([
            graphrefly::GraphRestoreEntry::descriptor(graphrefly::StateRestoreDescriptor),
        ])),
    )
    .expect("restore succeeds");
    let node = restored.find("a").expect("a restored");
    assert_eq!(
        node.as_node::<serde_json::Value>().cache(),
        Some(serde_json::json!(41))
    );
    assert_eq!(node.as_node::<i32>().cache(), None, "not the saved type");
}

/// Review Q1 (graphrefly:D797 (9)) — the kernel has no first-run gate: a node's first
/// run happens on activation whatever its dependencies hold.
#[test]
fn a_kernel_node_runs_first_without_waiting_for_values() {
    let g = graphrefly::graph();
    let a = g.state_empty::<i32>();
    let b = g.state(2i32);
    let seen = Rc::new(RefCell::new(Vec::<(Option<i32>, Option<i32>)>::new()));
    let n = {
        let seen = seen.clone();
        g.node::<i32, _>(vec![a.erased(), b.erased()], move |ctx| {
            seen.borrow_mut().push((
                ctx.dep(0).value::<i32>().map(|v| *v),
                ctx.dep(1).value::<i32>().map(|v| *v),
            ));
        })
    };
    let _u = n.subscribe(|_| {});
    assert_eq!(
        *seen.borrow(),
        vec![(None, Some(2))],
        "the first run sees B alone"
    );
    a.set(1);
    assert_eq!(*seen.borrow(), vec![(None, Some(2)), (Some(1), Some(2))]);
}

/// Review Q1 — derived and effect are the sugar that waits: the callback runs once
/// every dependency has a value, and the value a dependency sent while it waited is
/// kept for that run.
#[test]
fn derived_and_effect_wait_until_every_dependency_has_a_value() {
    let g = graphrefly::graph();
    let a = g.state_empty::<i32>();
    let b = g.state_empty::<i32>();
    let runs = Rc::new(Cell::new(0usize));
    let sum = {
        let runs = runs.clone();
        g.derived::<i32, _>(vec![a.erased(), b.erased()], move |values| {
            runs.set(runs.get() + 1);
            Some(*values.value::<i32>(0)? + *values.value::<i32>(1)?)
        })
    };
    let effects = Rc::new(RefCell::new(Vec::<i32>::new()));
    let effect = {
        let effects = effects.clone();
        g.effect(vec![sum.erased()], move |values| {
            effects.borrow_mut().push(*values.value::<i32>(0).unwrap());
            None
        })
    };
    let _ue = effect.subscribe(|_| {});
    let _us = sum.subscribe(|_| {});
    assert_eq!(runs.get(), 0, "no dependency has a value yet");
    a.set(1);
    assert_eq!(runs.get(), 0, "B has no value yet");
    assert!(effects.borrow().is_empty());
    b.set(2);
    assert_eq!(runs.get(), 1);
    assert_eq!(
        sum.cache(),
        Some(3),
        "A's value was kept while the gate waited"
    );
    assert_eq!(*effects.borrow(), vec![3]);
    a.set(5);
    assert_eq!(sum.cache(), Some(7));
    assert_eq!(*effects.borrow(), vec![3, 7]);
}

/// Review Q4 (R-wave-boundary) — each ctx.emit is one array of its own: emits are
/// never joined.
#[test]
fn each_emit_is_one_array() {
    let g = graphrefly::graph();
    let trigger = g.state(0i32);
    let n = g.node::<i32, _>(vec![trigger.erased()], |ctx| {
        if ctx.dep(0).values::<i32>().iter().any(|v| **v == 1) {
            ctx.emit(1i32);
            ctx.emit(2i32);
        }
    });
    let (log, _u) = record_values(&n);
    log.borrow_mut().clear();
    trigger.set(1);
    let data: Vec<Vec<i32>> = log
        .borrow()
        .iter()
        .filter(|w| !w.is_empty())
        .cloned()
        .collect();
    assert_eq!(data, vec![vec![1], vec![2]]);
    let (snap, _u2) = record_values(&n);
    assert_eq!(
        *snap.borrow(),
        vec![vec![2]],
        "a new subscriber's snapshot is the last array that carried DATA"
    );
}

/// Review Q4 — a source with several values at once sends them as one array, and an
/// operator over it forwards one array per run, so a late subscriber's snapshot keeps
/// every value.
#[test]
fn multi_value_sources_keep_every_value_in_the_snapshot() {
    use graphrefly::{concat, from_iter, tap};
    let g = graphrefly::graph();
    let src = g.init_node(from_iter(vec![1i32, 2, 3]), vec![], named("src"));
    let tapped = g.init_node(tap::<i32>(|_| {}), vec![src.erased()], named("tap"));
    let _keep = tapped.subscribe(|_| {});
    let (snap, _u) = record_values(&tapped);
    assert_eq!(snap.borrow()[0], vec![1, 2, 3]);

    let a = g.init_node(from_iter(vec![1i32, 2]), vec![], named("a"));
    let b = g.init_node(from_iter(vec![3i32, 4]), vec![], named("b"));
    let both = g.init_node(
        concat::<i32>(),
        vec![a.erased(), b.erased()],
        named("concat"),
    );
    let (snap, _u) = record_values(&both);
    assert_eq!(snap.borrow()[0], vec![1, 2, 3, 4]);
}

/// Review Q9 (R-failure@3) — with no `report` option a failure is written to standard
/// error; with one, it goes to the reporter only. Checked in a child run of this test,
/// whose standard error is captured.
#[test]
fn failures_go_to_stderr_without_a_reporter_and_only_to_the_reporter_with_one() {
    const CHILD: &str = "GRAPHREFLY_Q9_CHILD";
    let name = "failures_go_to_stderr_without_a_reporter_and_only_to_the_reporter_with_one";
    if let Some(mode) = std::env::var_os(CHILD) {
        let reports = Rc::new(RefCell::new(Vec::<graphrefly::HostFailure>::new()));
        let g = if mode == "reported" {
            reporting_graph(&reports)
        } else {
            graphrefly::graph()
        };
        let s = g.state(0i32);
        let _u = s.subscribe(|msgs: &[Message<AnyValue>]| {
            if msgs
                .iter()
                .any(|m| matches!(m, Message::Data(v) if v.downcast_ref::<i32>() == Some(&1)))
            {
                panic!("q9 sink");
            }
        });
        s.set(1);
        if mode == "reported" {
            assert_eq!(reports.borrow().len(), 1);
        }
        return;
    }
    let run = |mode: &str| {
        let out = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", name, "--nocapture", "--test-threads", "1"])
            .env(CHILD, mode)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stderr).into_owned()
    };
    let plain = run("plain");
    assert!(
        plain.contains("graphrefly: sink failed: q9 sink"),
        "{plain}"
    );
    let reported = run("reported");
    assert!(!reported.contains("graphrefly: sink failed"), "{reported}");
}

/// Review Q12 (graphrefly:D806, R-terminals@1, C-17) — under auto COMPLETE the run that
/// sees the last dependency's terminal calls the function, also when that terminal is
/// its only input, and the node completes after it.
#[test]
fn auto_complete_runs_the_function_on_the_last_terminal() {
    let g = graphrefly::graph();
    let a = g.state(1i32);
    let b = g.state(2i32);
    let runs = Rc::new(RefCell::new(Vec::<(Option<String>, Option<String>)>::new()));
    let n = {
        let runs = runs.clone();
        g.node::<i32, _>(vec![a.erased(), b.erased()], move |ctx| {
            runs.borrow_mut().push((
                terminal_name(ctx.dep(0).terminal()),
                terminal_name(ctx.dep(1).terminal()),
            ));
        })
    };
    let (log, _u) = record_arrays(&n);
    runs.borrow_mut().clear();
    a.complete();
    assert!(
        runs.borrow().is_empty(),
        "A's COMPLETE alone, with B open, is no input"
    );
    b.complete();
    assert_eq!(
        *runs.borrow(),
        vec![(None, Some("COMPLETE".to_owned()))],
        "the run that sees the last terminal, B's COMPLETE alone, calls the function once"
    );
    assert_eq!(n.status().output, graphrefly::OutputStatus::Completed);
    assert!(arrays(&log).iter().flatten().any(|k| k == "COMPLETE"));
}

/// The DATA payloads of an observed array, as i32.
fn i32s(msgs: &[Message<AnyValue>]) -> Vec<i32> {
    msgs.iter()
        .filter_map(|m| match m {
            Message::Data(v) => v.downcast_ref::<i32>().copied(),
            _ => None,
        })
        .collect()
}

/// C-48 (R-graph-surface@1, graphrefly:D805) — find, describe, observe and retain.
#[test]
fn c48_find_describe_observe_and_retain() {
    use graphrefly::{DescribeKind, DescribeNode, Lifecycle};
    let g = graphrefly::graph();
    let a = g.state_opts(1i32, named("a"));
    let b_runs = Rc::new(Cell::new(0usize));
    let b = {
        let b_runs = b_runs.clone();
        g.node_opts::<i32, _>(
            vec![a.erased()],
            move |ctx| {
                b_runs.set(b_runs.get() + 1);
                if let Some(v) = ctx.dep(0).value::<i32>() {
                    ctx.emit(*v * 10);
                }
            },
            named("b"),
        )
    };
    let c = g.node_opts::<i32, _>(vec![a.erased()], forward_i32, named("c"));
    let d_ctx: Rc<RefCell<Option<graphrefly::DeferredCtx>>> = Rc::new(RefCell::new(None));
    let d = {
        let d_ctx = d_ctx.clone();
        g.node_opts::<i32, _>(
            vec![a.erased()],
            move |ctx| {
                *d_ctx.borrow_mut() = Some(ctx.defer());
            },
            named("d"),
        )
    };

    assert!(g.find("b").is_some());
    assert!(g.find("nope").is_none());
    let node = |id: &str, kind, deps: &[&str]| DescribeNode {
        id: id.to_owned(),
        kind,
        deps: deps.iter().map(|d| (*d).to_owned()).collect(),
        quiet: false,
    };
    assert_eq!(
        g.describe(),
        vec![
            node("a", DescribeKind::State, &[]),
            node("b", DescribeKind::Node, &["a"]),
            node("c", DescribeKind::Node, &["a"]),
            node("d", DescribeKind::Node, &["a"]),
        ],
        "creation order, exactly {{ id, kind, deps, quiet }}"
    );
    assert_eq!(b_runs.get(), 0, "find and describe activate nothing");
    assert_eq!(b.status().lifecycle, Lifecycle::Inactive);

    type Events = Rc<RefCell<Vec<(String, usize, Vec<i32>)>>>;
    let record = |events: &Events| {
        let events = events.clone();
        move |e: &graphrefly::ObserveEvent<'_>| {
            events
                .borrow_mut()
                .push((e.id.to_owned(), e.msgs.len(), i32s(e.msgs)));
        }
    };
    let events: Events = Rc::new(RefCell::new(Vec::new()));
    let stop = g.observe(&["b"], record(&events));
    assert_eq!(
        b.status().lifecycle,
        Lifecycle::Active,
        "observe activates b"
    );
    a.set(2);
    assert_eq!(
        *events.borrow(),
        vec![("b".to_owned(), 2, vec![10]), ("b".to_owned(), 2, vec![20])],
        "one event per array: [START, DATA 10], then [PENDING, DATA 20]"
    );
    let other: Events = Rc::new(RefCell::new(Vec::new()));
    let stop2 = g.observe(&["b"], record(&other));
    stop();
    a.set(3);
    assert_eq!(events.borrow().len(), 2, "nothing after stop");
    assert_eq!(
        other.borrow().last().unwrap().2,
        vec![30],
        "another observer goes on"
    );
    stop2();
    assert_eq!(b.status().lifecycle, Lifecycle::Inactive);

    let r1 = g.retain("c");
    let r2 = g.retain("c");
    assert_eq!(c.status().lifecycle, Lifecycle::Active, "retain activates");
    r1();
    assert_eq!(
        c.status().lifecycle,
        Lifecycle::Active,
        "each retain is its own subscription"
    );
    r2();
    assert_eq!(c.status().lifecycle, Lifecycle::Inactive);

    let _rd = g.retain("d");
    let d_ctx = d_ctx.borrow_mut().take().unwrap();
    d_ctx.rewire(vec![a.erased(), c.erased()]);
    assert_eq!(
        g.describe().into_iter().find(|n| n.id == "d").unwrap().deps,
        vec!["a".to_owned(), "c".to_owned()],
        "describe gives the current dependencies"
    );
    assert_eq!(
        d.definition().deps.len(),
        1,
        "definition gives the declared ones"
    );
}

/// A stop or release kept where a sink can reach it.
type StopSlot = Rc<RefCell<Option<Box<dyn FnOnce()>>>>;

/// A node that sends two arrays in one run, when its trigger sends 1.
fn two_arrays(g: &graphrefly::Graph) -> (graphrefly::State<i32>, Node<i32>) {
    let t = g.state_opts(0i32, named("t"));
    let n = g.node_opts::<i32, _>(
        vec![t.erased()],
        |ctx| {
            if ctx.dep(0).values::<i32>().iter().any(|v| **v == 1) {
                ctx.down(vec![data(1)]);
                ctx.down(vec![data(2)]);
            }
        },
        named("n"),
    );
    (t, n)
}

/// Stopping from inside a sink in the middle of a wave. An observe stops at once:
/// nothing more is handed on (R-graph-surface@1), so the run's second array does not
/// reach it. A plain unsubscribe is a request (R-request-order@2, C-33): it waits until
/// the wave ends, so the second array of the same run still reaches that sink, and
/// nothing after the wave does. A retain released mid-wave is gone after the wave.
#[test]
fn stop_in_the_middle_of_a_wave() {
    // subscribe
    let g = graphrefly::graph();
    let (t, n) = two_arrays(&g);
    let seen = Rc::new(RefCell::new(Vec::<Vec<i32>>::new()));
    let stop: StopSlot = Rc::new(RefCell::new(None));
    let unsub = {
        let (seen, stop) = (seen.clone(), stop.clone());
        n.subscribe(move |msgs: &[Message<AnyValue>]| {
            let values = i32s(msgs);
            if values == vec![1] {
                if let Some(stop) = stop.borrow_mut().take() {
                    stop();
                }
            }
            seen.borrow_mut().push(values);
        })
    };
    *stop.borrow_mut() = Some(unsub);
    t.set(1);
    assert!(seen.borrow().contains(&vec![1]));
    assert!(
        seen.borrow().contains(&vec![2]),
        "subscribe: the unsubscribe waits until the wave ends (C-33)"
    );
    let before = seen.borrow().len();
    t.set(1);
    assert_eq!(
        seen.borrow().len(),
        before,
        "subscribe: nothing after the wave"
    );

    // observe
    let g = graphrefly::graph();
    let (t, _n) = two_arrays(&g);
    let seen = Rc::new(RefCell::new(Vec::<Vec<i32>>::new()));
    let stop: StopSlot = Rc::new(RefCell::new(None));
    let observer = {
        let (seen, stop) = (seen.clone(), stop.clone());
        g.observe(&["n"], move |e| {
            let values = i32s(e.msgs);
            if values == vec![1] {
                if let Some(stop) = stop.borrow_mut().take() {
                    stop();
                }
            }
            seen.borrow_mut().push(values);
        })
    };
    *stop.borrow_mut() = Some(observer);
    t.set(1);
    assert!(seen.borrow().contains(&vec![1]));
    assert!(
        !seen.borrow().contains(&vec![2]),
        "observe: no array after stop"
    );

    // retain: a release from inside a wave takes effect; the node closes once its
    // other subscriber leaves too.
    let g = graphrefly::graph();
    let (t, n) = two_arrays(&g);
    let release: StopSlot = Rc::new(RefCell::new(Some(g.retain("n"))));
    let witness = {
        let release = release.clone();
        n.subscribe(move |msgs: &[Message<AnyValue>]| {
            if i32s(msgs) == vec![1] {
                if let Some(release) = release.borrow_mut().take() {
                    release();
                }
            }
        })
    };
    t.set(1);
    assert!(
        release.borrow().is_none(),
        "the retain was released mid-wave"
    );
    assert_eq!(n.status().lifecycle, graphrefly::Lifecycle::Active);
    witness();
    assert_eq!(
        n.status().lifecycle,
        graphrefly::Lifecycle::Inactive,
        "retain: the released subscription is gone"
    );
}

/// C-35 (graphrefly:D806, F 2.9) — a queued ctx.rewire is checked again when it runs:
/// two rewires in one batch, each legal when asked for, cannot commit a cycle. The
/// refused one changes nothing and goes to the host reporter as a queued request's
/// failure.
#[test]
fn queued_rewires_cannot_commit_a_cycle() {
    let reports = Rc::new(RefCell::new(Vec::<graphrefly::HostFailure>::new()));
    let g = reporting_graph(&reports);
    let (x, x_ctx) = driven_in(&g);
    let (y, y_ctx) = driven_in(&g);
    let _ux = x.subscribe(|_| {});
    let _uy = y.subscribe(|_| {});
    let x_ctx = x_ctx.borrow_mut().take().unwrap();
    let y_ctx = y_ctx.borrow_mut().take().unwrap();
    g.batch(|_| {
        x_ctx.rewire(vec![y.erased()]);
        // Legal when asked for; a cycle x -> y -> x once the one above has run.
        y_ctx.rewire(vec![x.erased()]);
    });
    let deps_of = |id: &str| {
        g.describe()
            .into_iter()
            .find(|n| n.id == id)
            .map(|n| n.deps.len())
            .unwrap()
    };
    let ids: Vec<String> = g.describe().into_iter().map(|n| n.id).collect();
    assert_eq!(
        (deps_of(&ids[0]), deps_of(&ids[1])),
        (1, 0),
        "the first rewire ran; the second changed nothing"
    );
    let errors: Vec<(String, String)> = reports
        .borrow()
        .iter()
        .map(|f| (f.source.clone(), f.error.clone()))
        .collect();
    assert_eq!(
        errors,
        vec![(
            "queued request".to_owned(),
            "rewire: the node would depend on itself (R-request-order)".to_owned()
        )]
    );
}

/// R-checkpoint@1 with derived sugar — a restored derived is rebuilt with the same
/// sugar and gate: a dependency that only ended brings no new DATA, so the restored
/// node does not compute again; a dependency that changed does. A derived definition
/// that brings a setup is refused.
mod restored_derived {
    use super::*;
    use graphrefly::checkpoint::{GraphRestoreDescriptor, RestoreDefineCtx};
    use graphrefly::{
        restore_graph, restore_registry, restored_opts, GraphNodeOpts, GraphRestoreEntry,
        GraphRestoreResult, RestoreFactoryMeta, RestoreGraphOptions, RestoreNodeDefinition,
        RestoreNodeKind, StateRestoreDescriptor,
    };
    use serde_json::{json, Value};

    struct Double {
        runs: Rc<Cell<usize>>,
        with_setup: bool,
    }

    impl GraphRestoreDescriptor for Double {
        fn ref_(&self) -> &str {
            "double"
        }

        fn define(&self, ctx: RestoreDefineCtx<'_>) -> GraphRestoreResult<RestoreNodeDefinition> {
            let mut opts = restored_opts(ctx.checkpoint)?;
            if self.with_setup {
                opts.node = opts.node.with_setup(|_| None);
            }
            let runs = self.runs.clone();
            Ok(RestoreNodeDefinition {
                factory: "derived".to_owned(),
                kind: RestoreNodeKind::Derived(Rc::new(move |values| {
                    runs.set(runs.get() + 1);
                    Some(json!(values.value::<Value>(0)?.as_i64()? * 2))
                })),
                opts,
            })
        }
    }

    fn registry(runs: &Rc<Cell<usize>>, with_setup: bool) -> graphrefly::GraphRestoreRegistry {
        restore_registry([
            GraphRestoreEntry::descriptor(StateRestoreDescriptor),
            GraphRestoreEntry::descriptor(Double {
                runs: runs.clone(),
                with_setup,
            }),
        ])
    }

    fn checkpoint() -> graphrefly::GraphCheckpoint {
        let g = graphrefly::graph();
        let s = g.state_opts(json!(1), GraphNodeOpts::named("s"));
        let mut opts = GraphNodeOpts::named("d");
        opts.restore = Some(RestoreFactoryMeta::registry_ref("double"));
        let d = g.derived_opts::<Value, _>(
            vec![s.erased()],
            |values| Some(json!(values.value::<Value>(0)?.as_i64()? * 2)),
            opts,
        );
        let _u = d.subscribe(|_| {});
        assert_eq!(d.cache(), Some(json!(2)));
        g.checkpoint().expect("a JSON derived is captured")
    }

    #[test]
    fn a_restored_derived_keeps_its_gate() {
        let runs = Rc::new(Cell::new(0));
        let r = restore_graph(
            checkpoint(),
            RestoreGraphOptions::new(registry(&runs, false)),
        )
        .expect("restores");
        let rs = r.find("s").unwrap().as_state::<Value>().unwrap();
        let rd = r.find("d").unwrap().as_node::<Value>();
        rs.complete();
        let _u = rd.subscribe(|_| {});
        assert_eq!(runs.get(), 0, "an end alone is no new DATA: no recompute");
        assert_eq!(rd.cache(), Some(json!(2)), "the saved value is kept");
        assert_eq!(rd.status().output, graphrefly::OutputStatus::Completed);

        let runs = Rc::new(Cell::new(0));
        let r = restore_graph(
            checkpoint(),
            RestoreGraphOptions::new(registry(&runs, false)),
        )
        .expect("restores");
        let rs = r.find("s").unwrap().as_state::<Value>().unwrap();
        let rd = r.find("d").unwrap().as_node::<Value>();
        let _u = rd.subscribe(|_| {});
        rs.set(json!(5));
        assert_eq!(runs.get(), 1);
        assert_eq!(rd.cache(), Some(json!(10)));
    }

    #[test]
    fn a_derived_definition_with_a_setup_is_refused() {
        let runs = Rc::new(Cell::new(0));
        let err = match restore_graph(
            checkpoint(),
            RestoreGraphOptions::new(registry(&runs, true)),
        ) {
            Ok(_) => panic!("a derived definition with a setup is refused"),
            Err(err) => err,
        };
        assert_eq!(
            err.to_string(),
            "not restorable: d: a derived definition gives a value-level callback and no setup"
        );
    }

    #[test]
    fn a_local_only_node_is_refused_at_restore() {
        let g = graphrefly::graph();
        let s = g.state_opts(json!(1), GraphNodeOpts::named("s"));
        let _local = g.node_opts::<Value, _>(
            vec![s.erased()],
            forward_json,
            GraphNodeOpts::named("local"),
        );
        let checkpoint = g
            .checkpoint()
            .expect("capture takes a local-only node as such");
        let err = match restore_graph(
            checkpoint,
            RestoreGraphOptions::new(restore_registry([GraphRestoreEntry::descriptor(
                StateRestoreDescriptor,
            )])),
        ) {
            Ok(_) => panic!("a local-only node is refused"),
            Err(err) => err,
        };
        assert_eq!(
            err.to_string(),
            "not restorable: local: local-only, built or replaced without a recipe"
        );
    }

    fn forward_json(ctx: &graphrefly::Ctx) {
        if let Some(v) = ctx.dep(0).value::<Value>() {
            ctx.emit((*v).clone());
        }
    }

    #[test]
    fn a_non_json_private_state_is_refused_at_capture() {
        let g = graphrefly::graph();
        let s = g.state_opts(json!(1), GraphNodeOpts::named("s"));
        let n = g.node_opts::<Value, _>(
            vec![s.erased()],
            |ctx| ctx.state_set(std::time::Duration::from_secs(1)),
            GraphNodeOpts::named("n"),
        );
        let _u = n.subscribe(|_| {});
        let err = g
            .checkpoint()
            .expect_err("a non-JSON private state is refused");
        assert_eq!(
            err.to_string(),
            "not restorable: n: its private state is not JSON"
        );
    }

    #[test]
    fn an_effect_that_ran_does_not_block_a_checkpoint() {
        let g = graphrefly::graph();
        let s = g.state_opts(json!(1), GraphNodeOpts::named("s"));
        let ran = Rc::new(Cell::new(0));
        let effect = {
            let ran = ran.clone();
            g.effect(vec![s.erased()], move |_| {
                let ran = ran.clone();
                Some(Box::new(move || ran.set(ran.get() + 1)) as Box<dyn FnOnce()>)
            })
        };
        let stop = effect.subscribe(|_| {});
        s.set(json!(2));
        assert!(
            g.checkpoint().is_ok(),
            "the effect's cleanup is not private state"
        );
        stop();
        assert_eq!(ran.get(), 1, "the latest cleanup still runs at the close");
    }
}

/// R-checkpoint@1 — the built-in operators and sources whose values and state are JSON
/// carry a recipe, and the default registry rebuilds them with their own policies.
#[test]
fn built_in_operators_and_sources_restore_from_their_recipes() {
    use graphrefly::{
        default_restore_registry, element_at, from_iter, merge, of, restore_graph, skip, take,
        GraphCheckpointFactory, GraphNodeOpts, RestoreGraphOptions,
    };
    use serde_json::{json, Value};
    let g = graphrefly::graph();
    let src = g.init_node(
        from_iter(vec![json!(1), json!(2), json!(3)]),
        vec![],
        GraphNodeOpts::named("src"),
    );
    let one = g.init_node(of(json!("x")), vec![], GraphNodeOpts::named("one"));
    let live = g.state_opts(json!(0), GraphNodeOpts::named("live"));
    let firsts = g.init_node(
        take::<Value>(3),
        vec![live.erased()],
        GraphNodeOpts::named("firsts"),
    );
    let later = g.init_node(
        skip::<Value>(1),
        vec![live.erased()],
        GraphNodeOpts::named("later"),
    );
    let third = g.init_node(
        element_at::<Value>(2),
        vec![src.erased()],
        GraphNodeOpts::named("third"),
    );
    let both = g.init_node(
        merge::<Value>(),
        vec![src.erased(), one.erased()],
        GraphNodeOpts::named("both"),
    );
    let keep: Vec<_> = [&firsts, &later, &third, &both]
        .iter()
        .map(|n| n.subscribe(|_| {}))
        .collect();
    live.set(json!(1));
    let checkpoint = g.checkpoint().expect("JSON operators are captured");
    for id in ["src", "one", "firsts", "later", "third", "both"] {
        let node = checkpoint.nodes.iter().find(|n| n.id == id).unwrap();
        assert!(
            matches!(node.factory, GraphCheckpointFactory::RegistryRef { .. }),
            "{id} carries a recipe"
        );
    }
    drop(keep);
    let r = restore_graph(
        checkpoint,
        RestoreGraphOptions::new(default_restore_registry()),
    )
    .expect("the default registry rebuilds them");
    let rlive = r.find("live").unwrap().as_state::<Value>().unwrap();
    let rfirsts = r.find("firsts").unwrap().as_node::<Value>();
    let rthird = r.find("third").unwrap().as_node::<Value>();
    let _u = (rfirsts.subscribe(|_| {}), rthird.subscribe(|_| {}));
    rlive.set(json!(2));
    assert_eq!(
        rfirsts.cache(),
        Some(json!(2)),
        "take(3) goes on from its saved count: 0 and 1 before, 2 after"
    );
    assert_eq!(rfirsts.status().output, graphrefly::OutputStatus::Completed);
    assert_eq!(rthird.cache(), Some(json!(3)));
}

/// Review Q15 (F 2.11) — activation, delivery, pending marks, closing and freeing are
/// iterative: a chain far longer than 3,000 nodes activates, carries a wave, closes,
/// activates again and is freed, on a test thread's small stack.
#[test]
fn a_chain_of_many_thousand_nodes_runs_on_a_small_stack() {
    const N: usize = 20_000;
    let g = graphrefly::graph();
    let src = g.state(0i32);
    let mut last = src.erased();
    let mut tail = None;
    for _ in 0..N {
        let node = g.node::<i32, _>(vec![last.clone()], |ctx| {
            if let Some(v) = ctx.dep(0).value::<i32>() {
                ctx.emit(*v + 1);
            }
        });
        last = node.erased();
        tail = Some(node);
    }
    let tail = tail.unwrap();
    let stop = tail.subscribe(|_| {});
    assert_eq!(tail.cache(), Some(N as i32), "activation reaches the end");
    src.set(1);
    assert_eq!(tail.cache(), Some(N as i32 + 1), "one wave reaches the end");
    stop();
    assert_eq!(tail.cache(), None, "closing reaches the source");
    let again = tail.subscribe(|_| {});
    assert_eq!(tail.cache(), Some(N as i32 + 1));
    again();
    drop((tail, last, src, g));
}

/// N2: a queued batch revalidates every host target before evaluating any write.
#[test]
fn queued_batch_refuses_an_ended_target_before_any_updater_or_commit() {
    for function_write in [false, true] {
        let reports = Rc::new(RefCell::new(Vec::new()));
        let g = reporting_graph(&reports);
        let trigger = g.state_empty::<i32>();
        let ended = g.state(1i32);
        let other = g.state(10i32);
        let runs = Rc::new(Cell::new(0));
        let topology_ran = Rc::new(Cell::new(false));
        let requested = g.producer::<i32, _>({
            let topology_ran = topology_ran.clone();
            move |_| topology_ran.set(true)
        });
        let _trigger = trigger.subscribe({
            let (g, ended, other, runs) = (g.clone(), ended.clone(), other.clone(), runs.clone());
            move |msgs| {
                if !msgs.iter().any(|msg| matches!(msg, Message::Data(_))) {
                    return;
                }
                ended.complete();
                g.batch(|_| {
                    let runs = runs.clone();
                    other.set(graphrefly::StateWrite::from_fn(move |v: Option<&i32>| {
                        runs.set(runs.get() + 1);
                        v.map(|v| v + 1)
                    }));
                    if function_write {
                        ended.set(graphrefly::StateWrite::from_fn(|v: Option<&i32>| {
                            v.copied()
                        }));
                    } else {
                        ended.set(2);
                    }
                    let _stop = requested.subscribe(|_| {});
                });
            }
        });
        trigger.set(1);
        assert_eq!(
            runs.get(),
            0,
            "preflight precedes even another target's updater"
        );
        assert_eq!(ended.cache(), Some(1));
        assert_eq!(other.cache(), Some(10), "no partial commit");
        assert!(topology_ran.get(), "body requests survive a failed commit");
        assert_eq!(reports.borrow().len(), 1);
        assert!(reports.borrow()[0]
            .error
            .contains("host input to an ended node"));
    }
}

#[test]
fn fixed_arity_operators_refuse_before_registering_or_running() {
    use graphrefly::{buffer, concat, sample, take_until, valve, with_latest_from};
    use std::panic::{catch_unwind, AssertUnwindSafe};
    let g = graphrefly::graph();
    let a = g.state(1i32);
    let b = g.state(2i32);
    for count in [0, 1, 3] {
        for factory in [
            "withLatestFrom",
            "concat",
            "buffer",
            "sample",
            "takeUntil",
            "valve",
        ] {
            let deps = (0..count)
                .map(|i| if i % 2 == 0 { a.erased() } else { b.erased() })
                .collect();
            let before = g.describe().len();
            let rejected = catch_unwind(AssertUnwindSafe(|| match factory {
                "withLatestFrom" => {
                    g.init_node(with_latest_from::<i32, i32>(), deps, named("bad"));
                }
                "concat" => {
                    g.init_node(concat::<i32>(), deps, named("bad"));
                }
                "buffer" => {
                    g.init_node(buffer::<i32>(), deps, named("bad"));
                }
                "sample" => {
                    g.init_node(sample::<i32>(), deps, named("bad"));
                }
                "takeUntil" => {
                    g.init_node(take_until::<i32>(), deps, named("bad"));
                }
                _ => {
                    g.init_node(valve::<i32>(), deps, named("bad"));
                }
            }));
            assert!(rejected.is_err(), "{factory} accepted {count} dependencies");
            assert_eq!(g.describe().len(), before);
        }
    }
    let _valid = g.init_node(
        with_latest_from::<i32, i32>(),
        vec![a.erased(), b.erased()],
        named("valid"),
    );
    assert!(catch_unwind(AssertUnwindSafe(|| g.init_node(
        graphrefly::map(|v: &i32| *v),
        vec![],
        named("map")
    )))
    .is_err());
    assert!(catch_unwind(AssertUnwindSafe(|| g.init_node(
        graphrefly::of(1i32),
        vec![a.erased()],
        named("source")
    )))
    .is_err());
}

#[test]
fn dependency_ids_are_graph_ids_during_setup_runs_and_rewire() {
    let g = graphrefly::graph();
    let a = g.state_opts(1i32, named("named-input"));
    let b = g.state(2i32);
    let b_id = g
        .describe()
        .into_iter()
        .find(|n| n.id != "named-input")
        .unwrap()
        .id;
    let seen = Rc::new(RefCell::new(Vec::new()));
    let slot = Rc::new(RefCell::new(None));
    let setup_g = g.clone();
    let n = g.node_opts::<i32, _>(
        vec![a.erased()],
        {
            let (g, seen, slot) = (g.clone(), seen.clone(), slot.clone());
            move |ctx| {
                let id = ctx.dep(0).id().to_owned();
                assert!(g.find(&id).is_some());
                seen.borrow_mut().push(id);
                *slot.borrow_mut() = Some(ctx.defer());
            }
        },
        named("reader").with_node(graphrefly::NodeOpts::default().with_setup(move |ctx| {
            assert_eq!(ctx.dep(0).id(), "named-input");
            assert!(setup_g.find(ctx.dep(0).id()).is_some());
            None
        })),
    );
    let _stop = n.subscribe(|_| {});
    assert_eq!(*seen.borrow(), vec!["named-input"]);
    let kept = slot.borrow_mut().take().unwrap();
    kept.rewire(vec![b.erased()]);
    assert_eq!(seen.borrow().last(), Some(&b_id));
    assert_eq!(
        g.describe()
            .into_iter()
            .find(|n| n.id == "reader")
            .unwrap()
            .deps,
        vec![b_id]
    );
}
