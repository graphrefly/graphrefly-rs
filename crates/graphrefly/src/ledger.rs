//! Bundle-owned ledgers (review Q13; the shape of graphrefly-ts:D180).
//!
//! A ledger that carries idempotency, a cursor or an audit sequence must not reset
//! when a node's lifetime closes. The bundle owns it as a state node holding strict
//! JSON, and the node that keeps it declares that state as a dependency, so the
//! relation is an edge of the graph and the value is part of a checkpoint. Within a
//! lifetime the node's private state is the working copy: it is read from the ledger
//! only when the lifetime has none, so a working copy restored from a checkpoint
//! (newer than the ledger, which is written only at a close) wins; the lifetime's
//! cleanup writes the working copy back, a request that lands before any later
//! lifetime starts. The node may deactivate as usual.

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use crate::ctx::Ctx;
use crate::graph::{Graph, GraphNodeOpts};
use crate::node::{Core, NodeOpts, State};

/// A bundle's ledger state node.
#[derive(Clone)]
pub(crate) struct Ledger {
    state: State<Value>,
}

impl Ledger {
    /// A ledger registered as `id`, holding `initial` until a lifetime writes it.
    pub(crate) fn new(graph: &Graph, id: impl Into<String>, initial: Value) -> Self {
        Self {
            state: graph.state_opts(initial, GraphNodeOpts::named(id)),
        }
    }

    /// The ledger node, to declare as the keeping node's dependency.
    pub(crate) fn erased(&self) -> Core {
        self.state.erased()
    }
}

/// `opts` with a setup whose cleanup writes the lifetime's working copy back to the
/// ledger, the node's last dependency (so a restored instance writes to its restored
/// ledger as well); a setup already in `opts` still runs first and its cleanup after.
/// `extract` turns the private state, of type `S`, into the ledger's JSON.
pub(crate) fn write_back<S: 'static>(
    mut opts: NodeOpts,
    extract: impl Fn(&S) -> Value + 'static,
) -> NodeOpts {
    let extract = std::rc::Rc::new(extract);
    let user_setup = opts.setup.take();
    opts.with_setup(move |ctx| {
        let user_cleanup = user_setup.as_ref().and_then(|setup| (setup.0)(ctx));
        let ledger = ctx
            .node
            .deps()
            .last()
            .cloned()
            .map(|core| State::<Value>::from_node(crate::node::Node::from_core(core)));
        let extract = extract.clone();
        crate::lifetime::cleanup(move |closing| {
            if let (Some(ledger), Some(working)) = (&ledger, closing.state_get::<S>()) {
                ledger.set(extract(&working));
            }
            if let Some(user_cleanup) = user_cleanup {
                user_cleanup(closing);
            }
        })
    })
}

/// [`write_back`] for a pure ledger, whose private state is the JSON working copy
/// itself.
pub(crate) fn write_back_json(opts: NodeOpts) -> NodeOpts {
    write_back::<Value>(opts, Value::clone)
}

/// The lifetime's working copy of a pure ledger: the private state when the lifetime
/// has one, else the ledger's value, read through dependency `dep`.
pub(crate) fn load<L: DeserializeOwned + Default>(ctx: &Ctx, dep: usize) -> L {
    if let Some(working) = ctx.state_get::<Value>() {
        return serde_json::from_value((*working).clone()).unwrap_or_default();
    }
    from_ledger(ctx, dep)
}

/// [`load`] for a working copy with no `Default`: `default` gives it when neither the
/// lifetime nor the ledger holds one.
pub(crate) fn load_or<L: DeserializeOwned>(
    ctx: &Ctx,
    dep: usize,
    default: impl FnOnce() -> L,
) -> L {
    let decode = |value: &Value| serde_json::from_value::<L>(value.clone()).ok();
    if let Some(working) = ctx.state_get::<Value>().and_then(|v| decode(&v)) {
        return working;
    }
    ctx.dep(dep)
        .value::<Value>()
        .and_then(|v| decode(&v))
        .unwrap_or_else(default)
}

/// The ledger's value read through dependency `dep`, or the default.
pub(crate) fn from_ledger<L: DeserializeOwned + Default>(ctx: &Ctx, dep: usize) -> L {
    ctx.dep(dep)
        .value::<Value>()
        .and_then(|value| serde_json::from_value((*value).clone()).ok())
        .unwrap_or_default()
}

/// Keeps `working` as the lifetime's working copy of a pure ledger, as JSON.
pub(crate) fn store<L: Serialize>(ctx: &Ctx, working: &L) {
    ctx.state_set(serde_json::to_value(working).unwrap_or(Value::Null));
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use serde_json::{json, Value};

    use super::*;
    use crate::checkpoint::{
        restore_graph, restore_registry, restored_opts, GraphRestoreDescriptor, GraphRestoreEntry,
        GraphRestoreResult, RestoreDefineCtx, RestoreGraphOptions, RestoreNodeDefinition,
        RestoreNodeKind, StateRestoreDescriptor,
    };
    use crate::graph::{graph, RestoreFactoryMeta};

    /// Counts the values of dependency 0 into a pure ledger, the last dependency.
    fn counter(ctx: &Ctx) {
        let count = load::<u64>(ctx, ctx.dep_len() - 1) + ctx.dep(0).values::<Value>().len() as u64;
        store(ctx, &count);
        ctx.emit(json!(count));
    }

    struct Counter;

    impl GraphRestoreDescriptor for Counter {
        fn ref_(&self) -> &str {
            "q13-counter"
        }

        fn define(&self, ctx: RestoreDefineCtx<'_>) -> GraphRestoreResult<RestoreNodeDefinition> {
            let mut opts = restored_opts(ctx.checkpoint)?;
            opts.node = write_back_json(opts.node);
            Ok(RestoreNodeDefinition {
                factory: "q13-counter".to_owned(),
                kind: RestoreNodeKind::NodeJson(Rc::new(counter)),
                opts,
            })
        }
    }

    fn value_of(g: &Graph, id: &str) -> Option<Value> {
        g.find(id)?.as_node::<Value>().cache()
    }

    /// Review Q13 — a capture in the middle of a lifetime keeps the working copy (the
    /// ledger is written only at a close), and the restored instance goes on from it;
    /// its next close writes the restored ledger.
    #[test]
    fn a_capture_in_the_middle_of_a_lifetime_keeps_the_ledger() {
        let g = graph();
        let t = g.state_opts(json!(0), GraphNodeOpts::named("t"));
        let ledger = Ledger::new(&g, "n/ledger", json!(0));
        let mut opts = GraphNodeOpts::named("n");
        opts.restore = Some(RestoreFactoryMeta::registry_ref("q13-counter"));
        opts.node = write_back_json(opts.node);
        let n = g.node_opts::<Value, _>(vec![t.erased(), ledger.erased()], counter, opts);
        let _u = n.subscribe(|_| {});
        t.set(json!(1));
        t.set(json!(2));
        assert_eq!(value_of(&g, "n"), Some(json!(3)));
        assert_eq!(
            value_of(&g, "n/ledger"),
            Some(json!(0)),
            "written only at a close"
        );

        let checkpoint = g
            .checkpoint()
            .expect("a pure ledger's working copy is JSON");
        let registry = restore_registry([
            GraphRestoreEntry::descriptor(StateRestoreDescriptor),
            GraphRestoreEntry::descriptor(Counter),
        ]);
        let r = restore_graph(checkpoint, RestoreGraphOptions::new(registry)).expect("restores");
        let rn = r.find("n").unwrap().as_node::<Value>();
        let rt = r.find("t").unwrap().as_state::<Value>().unwrap();
        let stop = rn.subscribe(|_| {});
        rt.set(json!(3));
        assert_eq!(
            value_of(&r, "n"),
            Some(json!(4)),
            "the restored working copy goes on"
        );
        stop();
        assert_eq!(
            value_of(&r, "n/ledger"),
            Some(json!(4)),
            "the close writes it back"
        );
        let _again = rn.subscribe(|_| {});
        assert_eq!(
            value_of(&r, "n"),
            Some(json!(5)),
            "a new lifetime reads the ledger and counts t's snapshot"
        );
    }
}
