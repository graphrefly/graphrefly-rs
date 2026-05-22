//! Mount / unmount / ancestors / destroy / signal_invalidate
//! (canonical spec §3.4 + §3.7).

mod common;

use std::sync::{Arc, Mutex};

use common::graph;
use graphrefly_core::{HandleId, Message, Sink};
use graphrefly_graph::MountError;

fn recording_sink() -> (Arc<Mutex<Vec<Message>>>, Sink) {
    let log: Arc<Mutex<Vec<Message>>> = Arc::new(Mutex::new(Vec::new()));
    let log_for_sink = log.clone();
    let sink: Sink = std::rc::Rc::new(move |msgs: &[Message]| {
        log_for_sink.lock().unwrap().extend_from_slice(msgs);
    });
    (log, sink)
}

#[test]
fn mount_new_creates_subgraph() {
    let (rt, parent) = graph("system");
    let child = parent.mount_new(rt.core(), "payment").unwrap();
    assert_eq!(child.name(), "payment");
    assert_eq!(parent.child_names(), vec!["payment"]);
}

#[test]
fn mount_new_collision_rejected() {
    let (rt, parent) = graph("system");
    parent.mount_new(rt.core(), "payment").unwrap();
    let err = parent.mount_new(rt.core(), "payment").unwrap_err();
    assert!(matches!(err, MountError::NameCollision(ref n) if n == "payment"));
}

// NOTE (D246): `mount_existing_with_different_core_rejected` deleted —
// `MountError::CoreMismatch` no longer exists. Post-D246 there is only
// ever the one embedder `Core` (no `Core` lives on a `Graph` to
// mismatch), so cross-Core mount rejection is unrepresentable.

#[test]
fn mount_collides_with_local_node_name() {
    let (rt, parent) = graph("system");
    parent.state(rt.core(), "payment", None).unwrap();
    let err = parent.mount_new(rt.core(), "payment").unwrap_err();
    assert!(matches!(err, MountError::NodeNameCollision(ref n) if n == "payment"));
}

#[test]
fn mount_with_builder_runs_builder_against_subgraph() {
    let (rt, parent) = graph("system");
    let child = parent
        .mount_with(rt.core(), "payment", |g| {
            g.state(rt.core(), "amount", Some(HandleId::new(99)))
                .unwrap();
        })
        .unwrap();
    assert_eq!(child.node_count(), 1);
    assert_eq!(
        child.cache_of(rt.core(), child.node("amount")),
        HandleId::new(99)
    );
}

#[test]
fn cross_subgraph_path_resolution() {
    let (rt, root) = graph("system");
    let payment = root.mount_new(rt.core(), "payment").unwrap();
    let validate = payment
        .state(rt.core(), "validate", Some(HandleId::new(1)))
        .unwrap();
    // Path resolution from root.
    assert_eq!(root.try_resolve("payment::validate"), Some(validate));
    // try_resolve from intermediate
    assert_eq!(payment.try_resolve("validate"), Some(validate));
}

#[test]
fn ancestors_walks_parent_chain() {
    let (rt, root) = graph("system");
    let payment = root.mount_new(rt.core(), "payment").unwrap();
    let leaf = payment.mount_new(rt.core(), "validate").unwrap();
    let chain_self = leaf.ancestors(true);
    let names: Vec<String> = chain_self
        .iter()
        .map(graphrefly_graph::Graph::name)
        .collect();
    assert_eq!(names, vec!["validate", "payment", "system"]);
    let chain_only = leaf.ancestors(false);
    let names: Vec<String> = chain_only
        .iter()
        .map(graphrefly_graph::Graph::name)
        .collect();
    assert_eq!(names, vec!["payment", "system"]);
}

#[test]
fn unmount_returns_audit_and_tears_down_subgraph() {
    let (rt, root) = graph("system");
    let payment = root.mount_new(rt.core(), "payment").unwrap();
    payment
        .state(rt.core(), "a", Some(HandleId::new(1)))
        .unwrap();
    payment
        .state(rt.core(), "b", Some(HandleId::new(2)))
        .unwrap();
    payment.mount_new(rt.core(), "inner").unwrap(); // empty inner mount
    let audit = root.unmount(rt.core(), "payment").unwrap();
    assert_eq!(audit.node_count, 2);
    assert_eq!(audit.mount_count, 1);
    assert!(payment.is_destroyed());
    assert!(root.try_resolve("payment::a").is_none());
}

#[test]
fn unmount_unknown_name_returns_error() {
    let (rt, root) = graph("system");
    let err = root.unmount(rt.core(), "nope").unwrap_err();
    assert!(matches!(err, MountError::NotMounted(ref n) if n == "nope"));
}

#[test]
fn destroy_tears_down_named_nodes_and_marks_destroyed() {
    let (rt, g) = graph("system");
    let s = g.state(rt.core(), "x", Some(HandleId::new(7))).unwrap();
    let (log, sink) = recording_sink();
    let sub = g.subscribe(rt.core(), s, sink);
    g.destroy(rt.core());
    assert!(g.is_destroyed());
    {
        let log = log.lock().unwrap();
        assert!(log.iter().any(|m| matches!(m, Message::Complete)));
        assert!(log.iter().any(|m| matches!(m, Message::Teardown)));
    }
    g.unsubscribe(rt.core(), s, sub);
}

#[test]
fn destroy_recurses_into_mounts() {
    let (rt, root) = graph("system");
    let payment = root.mount_new(rt.core(), "payment").unwrap();
    payment
        .state(rt.core(), "a", Some(HandleId::new(1)))
        .unwrap();
    root.destroy(rt.core());
    assert!(root.is_destroyed());
    assert!(payment.is_destroyed());
}

#[test]
fn signal_invalidate_clears_caches_recursively() {
    // Custom binding where the fn produces deterministic Identity — same
    // as the stub.
    let (rt, parent) = graph("system");
    let child = parent.mount_new(rt.core(), "payment").unwrap();
    let s_root = parent
        .state(rt.core(), "a", Some(HandleId::new(1)))
        .unwrap();
    let s_child = child.state(rt.core(), "b", Some(HandleId::new(2))).unwrap();
    parent.signal_invalidate(rt.core());
    assert_eq!(
        parent.cache_of(rt.core(), s_root),
        graphrefly_core::NO_HANDLE
    );
    assert_eq!(
        child.cache_of(rt.core(), s_child),
        graphrefly_core::NO_HANDLE
    );
}

#[test]
fn add_after_destroy_returns_destroyed_error() {
    let (rt, g) = graph("system");
    g.destroy(rt.core());
    let err = g.state(rt.core(), "x", None).unwrap_err();
    assert!(matches!(err, graphrefly_graph::NameError::Destroyed));
}

#[test]
fn mount_path_with_separator_rejected() {
    let (rt, g) = graph("system");
    let err = g.mount_new(rt.core(), "a::b").unwrap_err();
    assert!(matches!(err, MountError::InvalidName(ref n) if n == "a::b"));
}

#[test]
fn re_mount_into_same_parent_collides_on_name() {
    // D246: `Graph` is `Clone` (a cheap `Arc` bump) and clones share
    // the same inner namespace/mount-tree state. Re-mounting under the
    // same parent (via a clone of the parent handle) collides on the
    // namespace key before any backlink check can fire.
    let (rt, parent) = graph("system");
    let other_parent = parent.clone();
    let child = parent.mount_new(rt.core(), "p").unwrap();
    let err = other_parent.mount(rt.core(), "p", &child).unwrap_err();
    assert!(matches!(err, MountError::NameCollision(_)));
}

#[test]
fn unmount_clears_parent_backlink() {
    // After `unmount`, the child's parent slot is `None`, so
    // `ancestors()` returns an empty chain.
    let (rt, parent) = graph("system");
    let child = parent.mount_new(rt.core(), "p").unwrap();
    let audit = parent.unmount(rt.core(), "p").unwrap();
    assert_eq!(audit.node_count, 0);
    let chain = child.ancestors(false);
    assert!(chain.is_empty());
}

// ---------------------------------------------------------------------------
// /qa Slice E+ regression tests
// ---------------------------------------------------------------------------

#[test]
fn destroy_propagates_destroyed_flag_to_user_held_child_clone() {
    // /qa F8: parent.destroy() must flip the destroyed flag on a
    // user-held clone of the child Graph (the child's
    // Arc<Mutex<GraphInner>> is shared).
    let (rt, parent) = graph("system");
    let child = parent.mount_new(rt.core(), "p").unwrap();
    let third_clone = child.clone();
    parent.destroy(rt.core());
    assert!(child.is_destroyed());
    assert!(third_clone.is_destroyed());
}

#[test]
fn destroy_preserves_namespace_during_teardown_cascade() {
    // /qa B3 fix (R3.7.3): registries are cleared AFTER the cascade
    // fires, so a sink subscribing during teardown can still resolve
    // the node's name via Graph::name_of.
    use std::sync::{Arc, Mutex};
    let (rt, g) = graph("system");
    let s = g
        .state(rt.core(), "sentinel_node", Some(HandleId::new(1)))
        .unwrap();
    let observed_name: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let observed_for_sink = observed_name.clone();
    // D246: capture a `Graph` clone (Core-free, Send + Sync + 'static)
    // into the long-lived Sink.
    let ns_for_sink = g.clone();
    let sink: Sink = std::rc::Rc::new(move |msgs: &[Message]| {
        if msgs.iter().any(|m| matches!(m, Message::Teardown)) {
            // Look up the node's name DURING the teardown cascade.
            *observed_for_sink.lock().unwrap() = ns_for_sink.name_of(s);
        }
    });
    let sub = g.subscribe(rt.core(), s, sink);
    g.destroy(rt.core());
    let name = observed_name.lock().unwrap().clone();
    assert_eq!(name.as_deref(), Some("sentinel_node"));
    // After destroy returns, the namespace IS cleared.
    assert_eq!(g.name_of(s), None);
    g.unsubscribe(rt.core(), s, sub);
}

#[test]
fn signal_invalidate_skips_meta_companions() {
    // /qa B2 fix (R3.7.2): graph-layer meta filter excludes
    // companions registered via add_meta_companion from the
    // signal_invalidate broadcast.
    let (rt, g) = graph("system");
    let parent = g
        .state(rt.core(), "parent", Some(HandleId::new(1)))
        .unwrap();
    let companion = g
        .state(rt.core(), "description", Some(HandleId::new(99)))
        .unwrap();
    g.add_meta_companion(rt.core(), parent, companion);
    g.signal_invalidate(rt.core());
    // Parent's cache wiped, companion's cache preserved.
    assert_eq!(g.cache_of(rt.core(), parent), graphrefly_core::NO_HANDLE);
    assert_eq!(g.cache_of(rt.core(), companion), HandleId::new(99));
}

#[test]
fn signal_invalidate_on_destroyed_graph_is_noop() {
    // /qa A4: destroyed graphs short-circuit signal_invalidate.
    let (rt, g) = graph("system");
    g.destroy(rt.core());
    g.signal_invalidate(rt.core()); // must not panic, no-op
    assert!(g.is_destroyed());
}

// ---------------------------------------------------------------------------
// E sub-slice (2026-05-10) — tree-wide gather-then-invalidate
// ---------------------------------------------------------------------------

#[test]
fn signal_invalidate_traverses_deep_mount_tree() {
    // Three-level mount tree: root → child → grandchild. Tree-wide
    // gather pass collects ids from all three levels under per-graph
    // locks (each lock held briefly); invalidation runs lock-released
    // afterward on the flat list.
    let (rt, root) = graph("root");
    let child = root.mount_new(rt.core(), "child").unwrap();
    let grandchild = child.mount_new(rt.core(), "grandchild").unwrap();

    let r_node = root.state(rt.core(), "r", Some(HandleId::new(1))).unwrap();
    let c_node = child.state(rt.core(), "c", Some(HandleId::new(2))).unwrap();
    let g_node = grandchild
        .state(rt.core(), "g", Some(HandleId::new(3)))
        .unwrap();

    root.signal_invalidate(rt.core());

    assert_eq!(
        root.cache_of(rt.core(), r_node),
        graphrefly_core::NO_HANDLE,
        "root level invalidated"
    );
    assert_eq!(
        child.cache_of(rt.core(), c_node),
        graphrefly_core::NO_HANDLE,
        "child level invalidated"
    );
    assert_eq!(
        grandchild.cache_of(rt.core(), g_node),
        graphrefly_core::NO_HANDLE,
        "grandchild level invalidated"
    );
}

#[test]
fn signal_invalidate_skips_destroyed_subtree() {
    // A child graph destroyed BEFORE signal_invalidate must be skipped
    // by the gather pass (the destroyed-check short-circuits inside
    // each subgraph's snapshot). The parent's own nodes still get
    // invalidated.
    let (rt, root) = graph("root");
    let child = root.mount_new(rt.core(), "child").unwrap();
    let r_node = root.state(rt.core(), "r", Some(HandleId::new(1))).unwrap();
    let c_node = child.state(rt.core(), "c", Some(HandleId::new(2))).unwrap();

    child.destroy(rt.core());
    // /qa m6 (2026-05-10): tighten the test — verify the destroyed
    // child is still in the parent's mount tree (so the gather pass
    // actually exercises the `inner.destroyed` short-circuit at line
    // 864). If a future Graph refactor removes destroyed children
    // from the parent's `children` map, this assertion catches the
    // shift and forces a test rewrite to match the new semantics.
    assert_eq!(
        root.child_names(),
        vec!["child"],
        "destroyed child still listed in parent's mount tree (proves \
         the destroyed-skip path is exercised, not the no-child path)"
    );
    assert!(
        child.is_destroyed(),
        "child marked destroyed (gather should short-circuit)"
    );

    // /qa m25 (2026-05-10): pre-invalidate snapshot of c_node's cache.
    // For STATE nodes, R2.2.8 ROM preserves cache across destroy's
    // TEARDOWN cascade (cache is intrinsic, not derived). The
    // destroyed-skip path means signal_invalidate must NOT touch
    // c_node — we verify by asserting cache is unchanged afterward.
    let c_cache_pre = child.cache_of(rt.core(), c_node);
    assert_eq!(
        c_cache_pre,
        HandleId::new(2),
        "state node cache preserved through destroy (R2.2.8 ROM)"
    );

    // After child.destroy(): child's namespace is cleared, but the
    // gather pass still walks into the child entry and short-circuits
    // on the destroyed flag. root.r should still wipe.
    root.signal_invalidate(rt.core());
    assert_eq!(
        root.cache_of(rt.core(), r_node),
        graphrefly_core::NO_HANDLE,
        "parent invalidate ran even though child subtree was destroyed"
    );
    // /qa m25: signal_invalidate must NOT touch c_node (gather pass
    // short-circuited on destroyed flag). c_node's cache stays at the
    // same value as before signal_invalidate.
    assert_eq!(
        child.cache_of(rt.core(), c_node),
        c_cache_pre,
        "destroyed child's node cache untouched by signal_invalidate \
         (gather short-circuited on destroyed flag)"
    );
}

#[test]
fn signal_invalidate_gather_does_not_hold_graph_lock_during_core_call() {
    // Regression-style: the two-phase split (gather then invalidate)
    // means no Graph lock is held while Core::invalidate fires. We
    // verify by attaching a sink that, on receiving INVALIDATE,
    // re-enters the Graph layer (calls `Graph::name_of`) — this would
    // deadlock if signal_invalidate held the Graph::inner lock across
    // the Core call.
    use graphrefly_core::Message;
    let (rt, g) = graph("system");
    let s = g.state(rt.core(), "named", Some(HandleId::new(1))).unwrap();
    let ns_for_sink = g.clone();
    let observed_name: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let observed_for_sink = observed_name.clone();
    let sink: Sink = std::rc::Rc::new(move |msgs: &[Message]| {
        for m in msgs {
            if matches!(m, Message::Invalidate) {
                // Re-enter Graph layer mid-cascade. Pre-fix this would
                // deadlock against the held inner lock; post-fix it
                // resolves cleanly.
                *observed_for_sink.lock().unwrap() = ns_for_sink.name_of(s);
            }
        }
    });
    let sub = g.subscribe(rt.core(), s, sink);
    g.signal_invalidate(rt.core());
    assert_eq!(
        observed_name.lock().unwrap().as_deref(),
        Some("named"),
        "sink observed INVALIDATE and re-entered Graph layer successfully \
         (no Graph lock held during Core cascade — E sub-slice invariant)"
    );
    g.unsubscribe(rt.core(), s, sub);
}
