//! C-1 and C-1b — wire edge groups (R-bridge@2, graphrefly:D803 and D804): each sender wave is
//! one cause and one receiver wave; a gate releases a session's causes whole, in order
//! and at most once. The tests carry each group's frames by hand, through the
//! canonical protobuf codec for C-1.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

use graphrefly::{
    decode_canonical_wire_edge_cause, encode_canonical_wire_edge_cause, frame_sink, frame_source,
    graph, wire_edge_group_in, wire_edge_group_out, AnyValue, CanonicalWireEdgeArrays,
    CanonicalWireMessage, FrameDeliver, Graph, GraphNodeOpts, Message, Node, WireEdgeCause,
    WireEdgeIssue,
};

type Queue = Rc<RefCell<VecDeque<WireEdgeCause>>>;
type Slot = Rc<RefCell<Option<FrameDeliver>>>;
type Runs = Rc<RefCell<Vec<(Option<u8>, Option<u8>)>>>;

/// A frame sink that queues each frame, as encoded and decoded canonical bytes.
fn queue_sink(queue: &Queue) -> impl Fn(&[Message<AnyValue>]) + 'static {
    let queue = queue.clone();
    frame_sink(move |frame| {
        let bytes = encode_canonical_wire_edge_cause(&frame).expect("a sent frame encodes");
        let frame = decode_canonical_wire_edge_cause(&bytes).expect("and decodes");
        queue.borrow_mut().push_back(frame);
    })
}

/// A frame source whose receiving end the test drives through `slot`.
fn hand_source(g: &Graph, name: &str, slot: &Slot) -> Node<WireEdgeCause> {
    let slot = slot.clone();
    frame_source(g, name, move |deliver| {
        *slot.borrow_mut() = Some(deliver);
        let slot = slot.clone();
        Box::new(move || {
            slot.borrow_mut().take();
        })
    })
}

fn deliver_all(queue: &Queue, slot: &Slot) -> usize {
    let mut n = 0;
    loop {
        let next = queue.borrow_mut().pop_front();
        let Some(frame) = next else { break };
        let deliver = slot.borrow().clone().expect("receiving end open");
        deliver(frame);
        n += 1;
    }
    n
}

fn byte(v: &[u8]) -> u8 {
    v[0]
}

fn bytes_node(g: &Graph, name: &str, deps: Vec<graphrefly::Core>, add: u8) -> Node<Vec<u8>> {
    g.node_opts::<Vec<u8>, _>(
        deps,
        move |ctx| {
            if let Some(v) = ctx.dep(0).value::<Vec<u8>>() {
                ctx.emit(vec![byte(&v) + add]);
            }
        },
        GraphNodeOpts::named(name),
    )
}

/// C-1 — a cross-graph diamond runs its fan-in once: each sender wave is one cause and
/// one receiver wave.
#[test]
fn c1_cross_graph_diamond_runs_its_fan_in_once() {
    let g1 = graph();
    let g2 = graph();
    let a = g1.state_opts(vec![1u8], GraphNodeOpts::named("A"));
    // ga carries a from g1 to g2; gbc carries b and c from g2 to g1.
    let (qa, qbc): (Queue, Queue) = (Rc::default(), Rc::default());
    let (slot_a, slot_bc): (Slot, Slot) = (Rc::default(), Rc::default());
    let ga_out = wire_edge_group_out(&g1, "ga/out", "ga", vec![("a".into(), a.erased())]);
    let ga_src = hand_source(&g2, "ga/frames", &slot_a);
    let ga_in = wire_edge_group_in(&g2, "ga", "ga", &ga_src, &["a"]);
    let b = bytes_node(&g2, "B", vec![ga_in.inbound["a"].erased()], 10);
    let c = bytes_node(&g2, "C", vec![ga_in.inbound["a"].erased()], 20);
    let gbc_out = wire_edge_group_out(
        &g2,
        "gbc/out",
        "gbc",
        vec![("b".into(), b.erased()), ("c".into(), c.erased())],
    );
    let gbc_src = hand_source(&g1, "gbc/frames", &slot_bc);
    let gbc_in = wire_edge_group_in(&g1, "gbc", "gbc", &gbc_src, &["b", "c"]);
    let d_runs: Rc<RefCell<Vec<(u8, u8)>>> = Rc::default();
    let d = {
        let d_runs = d_runs.clone();
        g1.node_opts::<Vec<u8>, _>(
            vec![gbc_in.inbound["b"].erased(), gbc_in.inbound["c"].erased()],
            move |ctx| {
                if let (Some(b), Some(c)) =
                    (ctx.dep(0).value::<Vec<u8>>(), ctx.dep(1).value::<Vec<u8>>())
                {
                    d_runs.borrow_mut().push((byte(&b), byte(&c)));
                    ctx.emit(vec![byte(&b), byte(&c)]);
                }
            },
            GraphNodeOpts::named("D"),
        )
    };
    let d_arrays: Rc<RefCell<Vec<Vec<Vec<u8>>>>> = Rc::default();
    let _ud = {
        let d_arrays = d_arrays.clone();
        d.subscribe(move |msgs: &[Message<AnyValue>]| {
            let data: Vec<Vec<u8>> = msgs
                .iter()
                .filter_map(|m| match m {
                    Message::Data(v) => v.downcast_ref::<Vec<u8>>().cloned(),
                    _ => None,
                })
                .collect();
            if !data.is_empty() {
                d_arrays.borrow_mut().push(data);
            }
        })
    };
    // The transport's sinks open both sessions.
    let _sa = ga_out.subscribe(queue_sink(&qa));
    let _sbc = gbc_out.subscribe(queue_sink(&qbc));

    // First causes: D starts on B's and C's values over A.
    deliver_all(&qa, &slot_a);
    deliver_all(&qbc, &slot_bc);
    assert_eq!(d_runs.borrow().last(), Some(&(11, 21)));

    // One write of A: one ga cause, one g2 wave with B and C once each, one gbc cause
    // carrying both, one g1 wave with D once.
    d_runs.borrow_mut().clear();
    d_arrays.borrow_mut().clear();
    a.set(vec![2]);
    assert_eq!(qa.borrow().len(), 1, "one g1 wave, one cause");
    deliver_all(&qa, &slot_a);
    assert_eq!(qbc.borrow().len(), 1, "B and C ran once, in one cause");
    let edges: Vec<String> = qbc.borrow()[0]
        .edges
        .iter()
        .map(|e| e.edge_id.clone())
        .collect();
    assert_eq!(edges, vec!["b", "c"]);
    deliver_all(&qbc, &slot_bc);
    assert_eq!(*d_runs.borrow(), vec![(12, 22)], "D ran once, on one A");
    assert_eq!(
        *d_arrays.borrow(),
        vec![vec![vec![12, 22]]],
        "its sinks got one array"
    );

    // Two writes held back on gbc: two g1 waves, in order, each consistent.
    d_runs.borrow_mut().clear();
    a.set(vec![3]);
    a.set(vec![4]);
    deliver_all(&qa, &slot_a);
    assert_eq!(deliver_all(&qbc, &slot_bc), 2);
    assert_eq!(*d_runs.borrow(), vec![(13, 23), (14, 24)]);
    assert!(
        d_runs.borrow().iter().all(|(b, c)| b + 10 == *c),
        "D never runs on one new and one old leg"
    );
}

fn frame(
    group: &str,
    session: &str,
    cause: u64,
    edges: Vec<(&str, Vec<Vec<CanonicalWireMessage>>)>,
) -> WireEdgeCause {
    WireEdgeCause {
        group: group.into(),
        session: session.into(),
        cause,
        edges: edges
            .into_iter()
            .map(|(edge, arrays)| CanonicalWireEdgeArrays {
                edge_id: edge.into(),
                arrays,
            })
            .collect(),
    }
}

fn data(v: u8) -> CanonicalWireMessage {
    CanonicalWireMessage::Data(vec![v])
}

/// C-1b — a wire edge group releases each cause whole, in order and at most once per
/// session. (Rust frames are typed: a frame with PENDING or RESOLVED in an array
/// cannot be built, and the codec refuses such bytes as `invalid_wire_edge`.)
#[test]
fn c1b_gate_releases_each_cause_whole_in_order_at_most_once() {
    let s = graph();
    let r = graph();
    let tx = s.state_opts(vec![1u8], GraphNodeOpts::named("tx"));
    // x sends two arrays in one wave for the value 99.
    let x = s.node_opts::<Vec<u8>, _>(
        vec![tx.erased()],
        |ctx| {
            if let Some(v) = ctx.dep(0).value::<Vec<u8>>() {
                if v[0] == 99 {
                    ctx.down(vec![Message::Data(Rc::new(vec![99u8]) as AnyValue)]);
                    ctx.down(vec![Message::Data(Rc::new(vec![100u8]) as AnyValue)]);
                } else {
                    ctx.emit((*v).clone());
                }
            }
        },
        GraphNodeOpts::named("x"),
    );
    let y = s.state_opts(vec![2u8], GraphNodeOpts::named("y"));
    let out = wire_edge_group_out(
        &s,
        "g/out",
        "g",
        vec![("x".into(), x.erased()), ("y".into(), y.erased())],
    );
    let q: Queue = Rc::default();
    let slot: Slot = Rc::default();
    let src = hand_source(&r, "g/frames", &slot);
    let gin = wire_edge_group_in(&r, "g", "g", &src, &["x", "y"]);
    let f_runs: Runs = Rc::default();
    let f = {
        let f_runs = f_runs.clone();
        r.node_opts::<(), _>(
            vec![gin.inbound["x"].erased(), gin.inbound["y"].erased()],
            move |ctx| {
                f_runs.borrow_mut().push((
                    ctx.dep(0).value::<Vec<u8>>().map(|v| v[0]),
                    ctx.dep(1).value::<Vec<u8>>().map(|v| v[0]),
                ));
            },
            GraphNodeOpts {
                node: graphrefly::NodeOpts {
                    partial: true,
                    ..graphrefly::NodeOpts::default()
                },
                ..GraphNodeOpts::named("F")
            },
        )
    };
    let _uf = f.subscribe(|_| {});
    let x_arrays: Rc<RefCell<Vec<Vec<String>>>> = Rc::default();
    let _ux = {
        let x_arrays = x_arrays.clone();
        gin.inbound["x"].subscribe(move |msgs: &[Message<AnyValue>]| {
            x_arrays
                .borrow_mut()
                .push(msgs.iter().map(|m| format!("{m:?}")).collect());
        })
    };
    let issues: Rc<RefCell<Vec<WireEdgeIssue>>> = Rc::default();
    let _ui = {
        let issues = issues.clone();
        gin.issues.subscribe(move |msgs: &[Message<AnyValue>]| {
            for m in msgs {
                if let Message::Data(v) = m {
                    if let Some(i) = v.downcast_ref::<WireEdgeIssue>() {
                        issues.borrow_mut().push(i.clone());
                    }
                }
            }
        })
    };
    let u_out = out.subscribe(queue_sink(&q));

    // The first cause carries x's and y's snapshots.
    let first = q.borrow()[0].clone();
    assert_eq!(first.cause, 1);
    assert_eq!(first.edges.len(), 2);
    deliver_all(&q, &slot);
    assert_eq!(f_runs.borrow().last(), Some(&(Some(1), Some(2))));

    // Each sender wave in which an edge sent is one cause carrying only those edges.
    let runs = f_runs.borrow().len();
    tx.set(vec![3]);
    assert_eq!(q.borrow().len(), 1);
    assert_eq!(q.borrow()[0].edges.len(), 1, "only x");
    tx.set(vec![4]);
    y.set(vec![5]);
    assert_eq!(q.borrow().len(), 3, "separate waves, separate causes");
    s.batch(|_| {
        tx.set(vec![6]);
        y.set(vec![7]);
    });
    assert_eq!(q.borrow().len(), 4);
    assert_eq!(
        q.borrow()[3].edges.len(),
        2,
        "one batch, one cause with both"
    );
    tx.set(vec![99]);
    assert_eq!(
        q.borrow()[4].edges[0].arrays.len(),
        2,
        "two arrays stay two"
    );
    let delivered = deliver_all(&q, &slot);
    assert_eq!(delivered, 5);
    assert_eq!(
        f_runs.borrow().len(),
        runs + 5,
        "each delivery: F runs once"
    );
    assert_eq!(f_runs.borrow().last(), Some(&(Some(100), Some(7))));

    // After a cause that carried only x, a new sink on inbound y gets y's last value.
    let y_seen: Rc<RefCell<Vec<u8>>> = Rc::default();
    let _uy = {
        let y_seen = y_seen.clone();
        gin.inbound["y"].subscribe(move |msgs: &[Message<AnyValue>]| {
            for m in msgs {
                if let Message::Data(v) = m {
                    y_seen
                        .borrow_mut()
                        .push(v.downcast_ref::<Vec<u8>>().unwrap()[0]);
                }
            }
        })
    };
    assert_eq!(*y_seen.borrow(), vec![7]);

    // INVALIDATE and COMPLETE arrive as the inbound nodes' own.
    x_arrays.borrow_mut().clear();
    tx.invalidate();
    y.complete();
    deliver_all(&q, &slot);
    assert!(x_arrays
        .borrow()
        .iter()
        .flatten()
        .any(|k| k == "INVALIDATE"));
    assert_eq!(
        gin.inbound["y"].status().output,
        graphrefly::OutputStatus::Completed
    );

    // A new session starts the count at 1 and carries an INVALIDATE for x.
    u_out();
    let u_out2 = out.subscribe(queue_sink(&q));
    let fresh = q.borrow().back().cloned().unwrap();
    assert_eq!(fresh.cause, 1);
    assert!(fresh
        .edges
        .iter()
        .any(|e| e.edge_id == "x" && e.arrays == vec![vec![CanonicalWireMessage::Invalidate]]));
    let session = fresh.session.clone();
    assert_ne!(session, first.session);
    // It repeats y's snapshot and COMPLETE: inbound y has ended, so they are skipped
    // without an issue, and x's INVALIDATE is released (R-bridge@2).
    assert!(fresh.edges.iter().any(
        |e| e.edge_id == "y" && e.arrays.last() == Some(&vec![CanonicalWireMessage::Complete])
    ));
    issues.borrow_mut().clear();
    x_arrays.borrow_mut().clear();
    deliver_all(&q, &slot);
    assert!(issues.borrow().is_empty(), "{:?}", issues.borrow());
    let released = gin
        .gate
        .cache()
        .expect("the gate released the cause")
        .released;
    let carried: Vec<&str> = released[0]
        .edges
        .iter()
        .map(|e| e.edge_id.as_str())
        .collect();
    assert_eq!(
        carried,
        vec!["x"],
        "y's arrays are skipped, x's INVALIDATE released"
    );

    // Frames handed to the receiver: duplicate, older, two past, refused, other group.
    let deliver = slot.borrow().clone().unwrap();
    issues.borrow_mut().clear();
    let runs = f_runs.borrow().len();
    deliver(frame("g", &session, 1, vec![("x", vec![vec![data(8)]])]));
    deliver(frame("g", &session, 3, vec![("x", vec![vec![data(9)]])]));
    deliver(frame("g", &session, 2, vec![("x", vec![vec![data(10)]])]));
    deliver(frame("g", &session, 4, vec![("z", vec![vec![data(1)]])]));
    deliver(frame(
        "g",
        &session,
        5,
        vec![(
            "x",
            vec![vec![CanonicalWireMessage::Complete], vec![data(1)]],
        )],
    ));
    // Arrays for the ended y that do not end with a terminal: skipped, the rest of
    // the frame released, the divergence reported.
    deliver(frame(
        "g",
        &session,
        6,
        vec![("x", vec![vec![data(11)]]), ("y", vec![vec![data(12)]])],
    ));
    deliver(frame(
        "other",
        &session,
        7,
        vec![("x", vec![vec![data(1)]])],
    ));
    let got = issues.borrow().clone();
    assert!(
        matches!(
            got[0],
            WireEdgeIssue::Stale {
                cause: 1,
                last: 1,
                ..
            }
        ),
        "{got:?}"
    );
    assert!(
        matches!(got[1], WireEdgeIssue::Gap { from: 2, to: 2, .. }),
        "{got:?}"
    );
    assert!(
        matches!(
            got[2],
            WireEdgeIssue::Stale {
                cause: 2,
                last: 3,
                ..
            }
        ),
        "{got:?}"
    );
    assert!(
        matches!(&got[3], WireEdgeIssue::Refused { cause: 4, reason, .. } if reason.contains("undeclared"))
    );
    assert!(
        matches!(&got[4], WireEdgeIssue::Refused { cause: 5, reason, .. } if reason.contains("after its terminal"))
    );
    assert!(
        matches!(&got[5], WireEdgeIssue::Ended { cause: 6, edge, .. } if edge == "y"),
        "{got:?}"
    );
    assert_eq!(got.len(), 6, "another group's frame is ignored");
    // F may run on the waves that release nothing (its inputs send RESOLVED, which is
    // input, R-invocation@2), but only the cause two past brings it a value.
    let seen_x: Vec<Option<u8>> = f_runs.borrow()[runs..].iter().map(|r| r.0).collect();
    assert!(seen_x.contains(&Some(9)), "{seen_x:?}");
    assert!(
        !seen_x.iter().any(|x| matches!(x, Some(8 | 10 | 1))),
        "dropped and refused frames bring nothing: {seen_x:?}"
    );
    assert!(
        seen_x.contains(&Some(11)),
        "the rest of the diverged frame is released"
    );
    assert_eq!(
        f_runs.borrow().last().unwrap(),
        &(Some(11), Some(7)),
        "y is unchanged"
    );
    assert_eq!(
        gin.inbound["x"].status().output,
        graphrefly::OutputStatus::Open
    );
    assert_eq!(
        gin.inbound["y"].status().output,
        graphrefly::OutputStatus::Completed
    );

    // After x lost its value and completed, a new session's first cause carries an
    // INVALIDATE ahead of x's COMPLETE, so inbound x ends without the old value.
    assert_eq!(gin.inbound["x"].cache(), Some(vec![11]));
    tx.complete();
    q.borrow_mut().clear(); // the session-2 causes are never delivered
    u_out2();
    let _u_out3 = out.subscribe(queue_sink(&q));
    let third = q.borrow().back().cloned().unwrap();
    let x_entry = third.edges.iter().find(|e| e.edge_id == "x").unwrap();
    assert_eq!(
        x_entry.arrays,
        vec![
            vec![CanonicalWireMessage::Invalidate],
            vec![CanonicalWireMessage::Complete]
        ]
    );
    issues.borrow_mut().clear();
    deliver_all(&q, &slot);
    assert_eq!(
        gin.inbound["x"].status().output,
        graphrefly::OutputStatus::Completed
    );
    assert_eq!(
        gin.inbound["x"].cache(),
        None,
        "ended without the old value"
    );
    assert!(
        issues.borrow().is_empty(),
        "y's repeated snapshot is skipped silently: {:?}",
        issues.borrow()
    );

    // An edge value that is not bytes is a throw, so an ERROR, of the outbound node.
    let n = s.state_opts(1i32, GraphNodeOpts::named("n"));
    let bad = wire_edge_group_out(&s, "bad/out", "bad", vec![("n".into(), n.erased())]);
    let _ub = bad.subscribe(|_| {});
    assert_eq!(bad.status().output, graphrefly::OutputStatus::Errored);
}

/// R-bridge@2: an inbound node's snapshot is what its edge last brought, whichever
/// frames carried it — also for an inbound node first activated after the causes
/// were released.
#[test]
fn inbound_activated_late_starts_from_what_its_edge_last_brought() {
    let r = graph();
    let slot: Slot = Rc::default();
    let src = hand_source(&r, "g/frames", &slot);
    let gin = wire_edge_group_in(&r, "g", "g", &src, &["x", "y"]);
    // Only the issues node is active, so the gate takes frames; inbound x is not.
    let _ui = gin.issues.subscribe(|_| {});
    let deliver = slot.borrow().clone().unwrap();
    deliver(frame(
        "g",
        "s1",
        1,
        vec![("x", vec![vec![data(1)]]), ("y", vec![vec![data(2)]])],
    ));
    deliver(frame("g", "s1", 2, vec![("y", vec![vec![data(3)]])]));
    let seen: Rc<RefCell<Vec<u8>>> = Rc::default();
    let _ux = {
        let seen = seen.clone();
        gin.inbound["x"].subscribe(move |msgs: &[Message<AnyValue>]| {
            for m in msgs {
                if let Message::Data(v) = m {
                    seen.borrow_mut()
                        .push(v.downcast_ref::<Vec<u8>>().unwrap()[0]);
                }
            }
        })
    };
    assert_eq!(*seen.borrow(), vec![1], "x's last value, from cause 1");
}
