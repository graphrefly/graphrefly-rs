//! Wire edge groups (R-bridge@2, graphrefly:D803 and D804).
//!
//! A group carries a fixed set of edges from a sender graph to a receiver graph.
//!
//! - Sender: the group's outbound node ([`wire_edge_group_out`]) depends on the
//!   group's edges under manual COMPLETE, ERROR and INVALIDATE policies. It is a
//!   fan-in, so it runs at most once per wave on a consistent set of inputs, and each
//!   run in which some edge brought input is one cause: one [`WireEdgeCause`] frame,
//!   sent as one DATA value. A sink ([`frame_sink`]) hands frames to the transport;
//!   the transport is not the bridge's.
//! - Receiver: frames come out of a node with no dependencies whose setup opens the
//!   receiving end ([`frame_source`]), so each frame is one wave. The group's gate
//!   ([`wire_edge_group_in`]) checks it, and every inbound node depends on the gate, so
//!   the edges of a cause settle in that one wave and a fan-in over them runs once.
//!   Nothing waits across graphs and nothing is drained by hand.
//!
//! DATA values and ERROR payloads cross as bytes (`Vec<u8>`), which the application
//! encodes; an ERROR's payload is its message's UTF-8 bytes. Bridge nodes carry no
//! recipe: they are local-only for a checkpoint (R-bridge@2 does not cover it).

pub use super::bridge_protobuf::WireEdgeCause;
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use super::bridge_protobuf::{CanonicalWireEdgeArrays, CanonicalWireMessage};
use crate::ctx::{Ctx, DepTerminal};
use crate::graph::{Graph, GraphNodeOpts};
use crate::node::{Core, Node, NodeOpts, Policy};
use crate::protocol::{AnyValue, Message};

/// What an edge last brought, as the gate keeps it: the values of its last array that
/// carried DATA, then its terminal.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WireEdgeView {
    /// The values of the edge's last released array that carried DATA, if any.
    pub values: Option<Vec<Vec<u8>>>,
    /// The edge's terminal, if it has ended.
    pub terminal: Option<CanonicalWireMessage>,
}

/// A drop, gap or refusal of the receiving gate: a value of the issues node, never a
/// terminal of an inbound node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireEdgeIssue {
    /// A frame without a session or with cause 0.
    Malformed,
    /// A cause at or below the last one taken: dropped.
    Stale {
        /// The frame's session.
        session: String,
        /// The frame's cause.
        cause: u64,
        /// The last cause taken in the session.
        last: u64,
    },
    /// Causes `from..=to` were never taken; the cause after them was released.
    Gap {
        /// The session.
        session: String,
        /// The first missing cause.
        from: u64,
        /// The last missing cause.
        to: u64,
    },
    /// An ended inbound edge was sent arrays that do not end with a terminal: they
    /// were skipped, and the sender's edge goes on while the receiver's has ended,
    /// which only a new group repairs (R-bridge@2).
    Ended {
        /// The session.
        session: String,
        /// The cause.
        cause: u64,
        /// The edge.
        edge: String,
    },
    /// A frame refused whole; its number is taken.
    Refused {
        /// The session.
        session: String,
        /// The cause.
        cause: u64,
        /// Why it was refused.
        reason: String,
    },
}

/// The gate's value in a wave: the causes it released, the issues, and the view after
/// them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireEdgeGateValue {
    /// The causes released in this wave, in order.
    pub released: Vec<Rc<WireEdgeCause>>,
    /// The issues of this wave.
    pub issues: Vec<WireEdgeIssue>,
    /// What each edge last brought, after this wave.
    pub view: Rc<BTreeMap<String, WireEdgeView>>,
}

/// The receiver side of a group: the gate, an inbound node per declared edge, and the
/// issues node.
#[derive(Clone)]
pub struct WireEdgeGroupIn {
    /// The gate.
    pub gate: Node<WireEdgeGateValue>,
    /// The inbound node of each declared edge, sending its edge's arrays as its own.
    pub inbound: BTreeMap<String, Node<Vec<u8>>>,
    /// The issues node: drops, gaps and refusals as values.
    pub issues: Node<WireEdgeIssue>,
}

/// The outbound node's private state: the session and the last cause number.
struct OutState {
    session: String,
    cause: u64,
}

thread_local! {
    static NEXT_SESSION: Cell<u64> = const { Cell::new(1) };
}

fn new_session(name: &str) -> String {
    let n = NEXT_SESSION.with(|next| {
        let n = next.get();
        next.set(n + 1);
        n
    });
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    format!("{name}#{nanos:x}-{n}")
}

fn manual_opts(name: &str, invalidate_manual: bool) -> GraphNodeOpts {
    GraphNodeOpts::named(name).with_node(NodeOpts {
        complete: Policy::Manual,
        error: Policy::Manual,
        invalidate: if invalidate_manual {
            Policy::Manual
        } else {
            Policy::Auto
        },
        ..NodeOpts::default()
    })
}

fn bytes_of(value: &AnyValue, id: &str, edge: &str) -> Vec<u8> {
    match value.downcast_ref::<Vec<u8>>() {
        Some(bytes) => bytes.clone(),
        None => {
            panic!("{id}: edge {edge} sent a DATA that is not bytes; encode it before the edge")
        }
    }
}

/// What input `i` brought in this run, as the ctx gives it: an INVALIDATE, then the
/// arrays that carried DATA, in order, then a terminal.
fn brought(ctx: &Ctx, i: usize, id: &str, edge: &str) -> Vec<Vec<CanonicalWireMessage>> {
    let mut arrays = Vec::new();
    if ctx.dep(i).invalidated() {
        arrays.push(vec![CanonicalWireMessage::Invalidate]);
    }
    for array in ctx.dep(i).raw_waves() {
        if !array.is_empty() {
            arrays.push(
                array
                    .iter()
                    .map(|v| CanonicalWireMessage::Data(bytes_of(v, id, edge)))
                    .collect(),
            );
        }
    }
    match ctx.dep(i).terminal() {
        Some(DepTerminal::Complete) => arrays.push(vec![CanonicalWireMessage::Complete]),
        Some(DepTerminal::Error(message)) => arrays.push(vec![CanonicalWireMessage::Error(
            message.as_bytes().to_vec(),
        )]),
        None => {}
    }
    arrays
}

/// The sender side of group `group`: the outbound node `name` over `edges` (edge id,
/// node). A session is one lifetime of this node: its setup names the session, and
/// its causes are numbered from 1. The first run carries every edge's snapshot, with
/// an INVALIDATE ahead of it for an edge without a value, one whose snapshot is its
/// terminal alone included (R-bridge@2). A DATA value that is not `Vec<u8>` is a panic
/// of the outbound node, so its ERROR.
pub fn wire_edge_group_out(
    graph: &Graph,
    name: impl Into<String>,
    group: impl Into<String>,
    edges: Vec<(String, Core)>,
) -> Node<WireEdgeCause> {
    let name = name.into();
    let group = group.into();
    let ids: Vec<String> = edges.iter().map(|(id, _)| id.clone()).collect();
    let deps: Vec<Core> = edges.into_iter().map(|(_, core)| core).collect();
    let session_name = name.clone();
    let opts = manual_opts(&name, true);
    let opts = GraphNodeOpts {
        node: opts.node.with_setup(move |ctx| {
            ctx.state_set(Rc::new(std::cell::RefCell::new(OutState {
                session: new_session(&session_name),
                cause: 0,
            })));
            None
        }),
        ..opts
    };
    let id = name.clone();
    graph.node_opts::<WireEdgeCause, _>(
        deps,
        move |ctx| {
            let state = ctx
                .state_get::<Rc<std::cell::RefCell<OutState>>>()
                .expect("the outbound node's setup names the session");
            let first = state.borrow().cause == 0;
            let mut carried = Vec::new();
            for (i, edge) in ids.iter().enumerate() {
                let mut arrays = brought(ctx, i, &id, edge);
                // The first cause clears, on the receiver, a value an earlier session left
                // for an edge that has none now, also one whose snapshot is its terminal.
                let has_value = arrays
                    .iter()
                    .any(|a| matches!(a.first(), Some(CanonicalWireMessage::Data(_))));
                if first && !has_value {
                    arrays.insert(0, vec![CanonicalWireMessage::Invalidate]);
                }
                if !arrays.is_empty() {
                    carried.push(CanonicalWireEdgeArrays {
                        edge_id: edge.clone(),
                        arrays,
                    });
                }
            }
            if carried.is_empty() {
                return; // a run with no new input forms no cause
            }
            let mut st = state.borrow_mut();
            st.cause += 1;
            let frame = WireEdgeCause {
                group: group.clone(),
                session: st.session.clone(),
                cause: st.cause,
                edges: carried,
            };
            drop(st);
            ctx.emit(frame);
        },
        opts,
    )
}

/// A sink callback that hands every frame a subscription delivers, the snapshot's
/// included, to `send`. A frame delivered twice is dropped by the receiver as a cause
/// already taken.
pub fn frame_sink(
    send: impl Fn(WireEdgeCause) + 'static,
) -> impl Fn(&[Message<AnyValue>]) + 'static {
    move |msgs: &[Message<AnyValue>]| {
        for msg in msgs {
            if let Message::Data(value) = msg {
                if let Some(frame) = value.downcast_ref::<WireEdgeCause>() {
                    send(frame.clone());
                }
            }
        }
    }
}

/// Delivers one received frame as a wave of the frame source.
pub type FrameDeliver = Rc<dyn Fn(WireEdgeCause)>;

/// The receiving end: a node `name` with no dependencies. Its setup calls
/// `open(deliver)`, which returns the close function, run when the lifetime closes;
/// each `deliver(frame)` is a late output, so one wave.
pub fn frame_source(
    graph: &Graph,
    name: impl Into<String>,
    open: impl Fn(FrameDeliver) -> Box<dyn FnOnce()> + 'static,
) -> Node<WireEdgeCause> {
    let name = name.into();
    graph.producer_opts(
        |_| {},
        GraphNodeOpts::named(name).with_node(NodeOpts::default().with_setup(move |ctx| {
            let out = Rc::new(ctx.defer());
            let deliver: FrameDeliver = Rc::new(move |frame| out.emit(frame));
            let close = open(deliver);
            crate::lifetime::cleanup(move |_| close())
        })),
    )
}

/// The receiver side of group `group`: the gate `name` over `frames`, an inbound node
/// `{name}/{edge}` per declared edge and the issues node `{name}.issues`.
pub fn wire_edge_group_in(
    graph: &Graph,
    name: impl Into<String>,
    group: impl Into<String>,
    frames: &Node<WireEdgeCause>,
    edges: &[&str],
) -> WireEdgeGroupIn {
    let name = name.into();
    let group = group.into();
    let declared: BTreeSet<String> = edges.iter().map(|e| (*e).to_owned()).collect();
    let gate = graph.node_opts::<WireEdgeGateValue, _>(
        vec![frames.erased()],
        gate_fn(group, declared),
        manual_opts(&name, false),
    );
    let inbound = edges
        .iter()
        .map(|edge| {
            let node = graph.node_opts::<Vec<u8>, _>(
                vec![gate.erased()],
                inbound_fn((*edge).to_owned()),
                manual_opts(&format!("{name}/{edge}"), false),
            );
            ((*edge).to_owned(), node)
        })
        .collect();
    let issues = graph.node_opts::<WireEdgeIssue, _>(
        vec![gate.erased()],
        issues_fn,
        manual_opts(&format!("{name}.issues"), false),
    );
    WireEdgeGroupIn {
        gate,
        inbound,
        issues,
    }
}

/// The gate's private state: the current session, the last cause taken in it, and
/// the view, what each edge last brought.
#[derive(Clone, Default)]
struct GateState {
    session: Option<String>,
    last: u64,
    view: Rc<BTreeMap<String, WireEdgeView>>,
}

fn gate_fn(group: String, declared: BTreeSet<String>) -> impl Fn(&Ctx) + 'static {
    move |ctx| {
        let mut st = ctx
            .state_get::<GateState>()
            .map(|s| (*s).clone())
            .unwrap_or_default();
        let mut released = Vec::new();
        let mut issues = Vec::new();
        for frame in ctx.dep(0).values::<WireEdgeCause>() {
            if frame.group != group {
                continue; // another group's frame on the same channel
            }
            if frame.session.is_empty() || frame.cause == 0 {
                issues.push(WireEdgeIssue::Malformed);
                continue;
            }
            if st.session.as_deref() != Some(frame.session.as_str()) {
                st.session = Some(frame.session.clone());
                st.last = 0;
            }
            let (session, cause) = (frame.session.clone(), frame.cause);
            if cause <= st.last {
                issues.push(WireEdgeIssue::Stale {
                    session,
                    cause,
                    last: st.last,
                });
                continue;
            }
            let gap = (cause > st.last + 1).then(|| WireEdgeIssue::Gap {
                session: session.clone(),
                from: st.last + 1,
                to: cause - 1,
            });
            st.last = cause;
            match view_after(&frame, &declared, &st.view) {
                Err(reason) => issues.push(WireEdgeIssue::Refused {
                    session,
                    cause,
                    reason,
                }),
                Ok(checked) => {
                    issues.extend(gap);
                    for edge in checked.diverged {
                        issues.push(WireEdgeIssue::Ended {
                            session: session.clone(),
                            cause,
                            edge,
                        });
                    }
                    st.view = Rc::new(checked.view);
                    released.push(checked.frame.map_or(frame, Rc::new));
                }
            }
        }
        let view = st.view.clone();
        ctx.state_set(st);
        if !released.is_empty() || !issues.is_empty() {
            ctx.emit(WireEdgeGateValue {
                released,
                issues,
                view,
            });
        }
    }
}

/// A frame that passed the gate's checks: the view after it, the frame to release
/// when edges were skipped (`None`: release it as it is), and the ended edges whose
/// skipped arrays did not end with a terminal.
struct Checked {
    view: BTreeMap<String, WireEdgeView>,
    frame: Option<WireEdgeCause>,
    diverged: Vec<String>,
}

fn is_terminal(m: &CanonicalWireMessage) -> bool {
    matches!(
        m,
        CanonicalWireMessage::Complete | CanonicalWireMessage::Error(_)
    )
}

/// Checks a frame against what any node could send as its output. Returns the reason
/// it is refused whole, or the view after it with the frame to release. An edge whose
/// inbound node has ended takes nothing more (R-bridge@2): its arrays are skipped and
/// the rest of the frame is released; when the skipped arrays do not end with a
/// terminal the edge is reported. A new session's first cause repeats an ended edge's
/// snapshot with its terminal, which is not reported.
fn view_after(
    frame: &WireEdgeCause,
    declared: &BTreeSet<String>,
    view: &BTreeMap<String, WireEdgeView>,
) -> Result<Checked, String> {
    let mut next = view.clone();
    let mut seen = BTreeSet::new();
    let mut kept = Vec::new();
    let mut diverged = Vec::new();
    for entry in &frame.edges {
        let edge = &entry.edge_id;
        if !declared.contains(edge) {
            return Err(format!("undeclared edge {edge}"));
        }
        if !seen.insert(edge.clone()) {
            return Err(format!("edge {edge} twice"));
        }
        if entry.arrays.is_empty() {
            return Err(format!("edge {edge} carries no array"));
        }
        let mut ended = false; // within this frame
        for array in &entry.arrays {
            if array.is_empty() {
                return Err(format!("edge {edge}: an empty array"));
            }
            if ended {
                return Err(format!("edge {edge}: an array after its terminal"));
            }
            if let Some(i) = array.iter().position(is_terminal) {
                if i != array.len() - 1 {
                    return Err(format!("edge {edge}: a terminal not last"));
                }
            }
            ended = array.last().is_some_and(is_terminal);
        }
        if next.get(edge).is_some_and(|v| v.terminal.is_some()) {
            if !ended {
                diverged.push(edge.clone());
            }
            continue;
        }
        let mut state = next.get(edge).cloned().unwrap_or_default();
        state.terminal = None;
        for array in &entry.arrays {
            // As the kernel applies it: what came before the last INVALIDATE is gone and
            // the value with it, the DATA after it is the value, a terminal ends the edge.
            let k = array
                .iter()
                .rposition(|m| matches!(m, CanonicalWireMessage::Invalidate));
            if k.is_some() {
                state.values = None;
            }
            let data: Vec<Vec<u8>> = array[k.map_or(0, |k| k + 1)..]
                .iter()
                .filter_map(|m| match m {
                    CanonicalWireMessage::Data(v) => Some(v.clone()),
                    _ => None,
                })
                .collect();
            if !data.is_empty() {
                state.values = Some(data);
            }
            state.terminal = array.iter().find(|m| is_terminal(m)).cloned();
        }
        next.insert(edge.clone(), state);
        kept.push(entry.clone());
    }
    let frame = (kept.len() != frame.edges.len()).then(|| WireEdgeCause {
        edges: kept,
        ..frame.clone()
    });
    Ok(Checked {
        view: next,
        frame,
        diverged,
    })
}

fn to_message(m: &CanonicalWireMessage) -> Message<AnyValue> {
    match m {
        CanonicalWireMessage::Data(v) => Message::Data(Rc::new(v.clone()) as AnyValue),
        CanonicalWireMessage::Invalidate => Message::Invalidate,
        CanonicalWireMessage::Complete => Message::Complete,
        CanonicalWireMessage::Error(payload) => {
            Message::Error(String::from_utf8_lossy(payload).into_owned().into())
        }
    }
}

/// An inbound node sends, in each wave, the arrays of its edge in the causes released,
/// as its own output, and nothing when they do not carry it. Its first run in a
/// lifetime sends instead what the edge last brought, from the gate's view: its
/// snapshot.
fn inbound_fn(edge: String) -> impl Fn(&Ctx) + 'static {
    move |ctx| {
        let gates = ctx.dep(0).values::<WireEdgeGateValue>();
        if ctx.state_get::<bool>().is_none() {
            ctx.state_set(true);
            let last = gates.last().map(|g| g.view.clone()).or_else(|| {
                ctx.dep(0)
                    .value::<WireEdgeGateValue>()
                    .map(|g| g.view.clone())
            });
            if let Some(view) = last {
                if let Some(seen) = view.get(&edge) {
                    let mut out: Vec<Message<AnyValue>> = seen
                        .values
                        .iter()
                        .flatten()
                        .map(|v| Message::Data(Rc::new(v.clone()) as AnyValue))
                        .collect();
                    if let Some(terminal) = &seen.terminal {
                        out.push(to_message(terminal));
                    }
                    if !out.is_empty() {
                        ctx.down(out);
                    }
                }
            }
            return;
        }
        for gate in &gates {
            for frame in &gate.released {
                if let Some(entry) = frame.edges.iter().find(|e| e.edge_id == edge) {
                    for array in &entry.arrays {
                        ctx.down(array.iter().map(to_message).collect());
                    }
                }
            }
        }
    }
}

/// The group's issues: drops, gaps and refusals, each a value. The first run of a
/// lifetime does not repeat the issues of the gate's last value.
fn issues_fn(ctx: &Ctx) {
    if ctx.state_get::<bool>().is_none() {
        ctx.state_set(true);
        return;
    }
    let issues: Vec<WireEdgeIssue> = ctx
        .dep(0)
        .values::<WireEdgeGateValue>()
        .iter()
        .flat_map(|g| g.issues.clone())
        .collect();
    if !issues.is_empty() {
        ctx.down(crate::protocol::data_wave(issues));
    }
}
