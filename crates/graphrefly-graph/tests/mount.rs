//! Mount / unmount / ancestors / destroy / signal_invalidate
//! (canonical spec §3.4 + §3.7).

mod common;

use std::sync::{Arc, Mutex};

use common::binding;
use graphrefly_core::{HandleId, Message, Sink};
use graphrefly_graph::{Graph, MountError};

fn recording_sink() -> (Arc<Mutex<Vec<Message>>>, Sink) {
    let log: Arc<Mutex<Vec<Message>>> = Arc::new(Mutex::new(Vec::new()));
    let log_for_sink = log.clone();
    let sink: Sink = Arc::new(move |msgs: &[Message]| {
        log_for_sink.lock().unwrap().extend_from_slice(msgs);
    });
    (log, sink)
}

#[test]
fn mount_new_creates_subgraph_sharing_core() {
    let parent = Graph::new("system", binding());
    let child = parent.mount_new("payment").unwrap();
    assert!(parent.core().same_dispatcher(child.core()));
    assert_eq!(child.name(), "payment");
    assert_eq!(parent.child_names(), vec!["payment"]);
}

#[test]
fn mount_new_collision_rejected() {
    let parent = Graph::new("system", binding());
    parent.mount_new("payment").unwrap();
    let err = parent.mount_new("payment").unwrap_err();
    assert!(matches!(err, MountError::NameCollision(ref n) if n == "payment"));
}

#[test]
fn mount_existing_with_different_core_rejected() {
    let parent = Graph::new("system", binding());
    let other = Graph::new("foreign", binding()); // different binding => different Core
    let err = parent.mount("foreign", other).unwrap_err();
    assert!(matches!(err, MountError::CoreMismatch));
}

#[test]
fn mount_collides_with_local_node_name() {
    let parent = Graph::new("system", binding());
    parent.state("payment", None).unwrap();
    let err = parent.mount_new("payment").unwrap_err();
    assert!(matches!(err, MountError::NodeNameCollision(ref n) if n == "payment"));
}

#[test]
fn mount_with_builder_runs_builder_against_subgraph() {
    let parent = Graph::new("system", binding());
    let child = parent
        .mount_with("payment", |g| {
            g.state("amount", Some(HandleId::new(99))).unwrap();
        })
        .unwrap();
    assert_eq!(child.node_count(), 1);
    assert_eq!(child.cache_of(child.node("amount")), HandleId::new(99));
}

#[test]
fn cross_subgraph_path_resolution() {
    let root = Graph::new("system", binding());
    let payment = root.mount_new("payment").unwrap();
    let validate = payment.state("validate", Some(HandleId::new(1))).unwrap();
    // Path resolution from root.
    assert_eq!(root.try_resolve("payment::validate"), Some(validate));
    // try_resolve from intermediate
    assert_eq!(payment.try_resolve("validate"), Some(validate));
}

#[test]
fn ancestors_walks_parent_chain() {
    let root = Graph::new("system", binding());
    let payment = root.mount_new("payment").unwrap();
    let leaf = payment.mount_new("validate").unwrap();
    let chain_self = leaf.ancestors(true);
    let names: Vec<String> = chain_self.iter().map(Graph::name).collect();
    assert_eq!(names, vec!["validate", "payment", "system"]);
    let chain_only = leaf.ancestors(false);
    let names: Vec<String> = chain_only.iter().map(Graph::name).collect();
    assert_eq!(names, vec!["payment", "system"]);
}

#[test]
fn unmount_returns_audit_and_tears_down_subgraph() {
    let root = Graph::new("system", binding());
    let payment = root.mount_new("payment").unwrap();
    payment.state("a", Some(HandleId::new(1))).unwrap();
    payment.state("b", Some(HandleId::new(2))).unwrap();
    payment.mount_new("inner").unwrap(); // empty inner mount
    let audit = root.unmount("payment").unwrap();
    assert_eq!(audit.node_count, 2);
    assert_eq!(audit.mount_count, 1);
    assert!(payment.is_destroyed());
    assert!(root.try_resolve("payment::a").is_none());
}

#[test]
fn unmount_unknown_name_returns_error() {
    let root = Graph::new("system", binding());
    let err = root.unmount("nope").unwrap_err();
    assert!(matches!(err, MountError::NotMounted(ref n) if n == "nope"));
}

#[test]
fn destroy_tears_down_named_nodes_and_marks_destroyed() {
    let g = Graph::new("system", binding());
    let s = g.state("x", Some(HandleId::new(7))).unwrap();
    let (log, sink) = recording_sink();
    let _sub = g.subscribe(s, sink);
    g.destroy();
    assert!(g.is_destroyed());
    let log = log.lock().unwrap();
    assert!(log.iter().any(|m| matches!(m, Message::Complete)));
    assert!(log.iter().any(|m| matches!(m, Message::Teardown)));
}

#[test]
fn destroy_recurses_into_mounts() {
    let root = Graph::new("system", binding());
    let payment = root.mount_new("payment").unwrap();
    payment.state("a", Some(HandleId::new(1))).unwrap();
    root.destroy();
    assert!(root.is_destroyed());
    assert!(payment.is_destroyed());
}

#[test]
fn signal_invalidate_clears_caches_recursively() {
    // Custom binding where the fn produces deterministic Identity — same
    // as the stub.
    let parent = Graph::new("system", binding());
    let child = parent.mount_new("payment").unwrap();
    let s_root = parent.state("a", Some(HandleId::new(1))).unwrap();
    let s_child = child.state("b", Some(HandleId::new(2))).unwrap();
    parent.signal_invalidate();
    assert_eq!(parent.cache_of(s_root), graphrefly_core::NO_HANDLE);
    assert_eq!(child.cache_of(s_child), graphrefly_core::NO_HANDLE);
}

#[test]
fn add_after_destroy_returns_destroyed_error() {
    let g = Graph::new("system", binding());
    g.destroy();
    let err = g.state("x", None).unwrap_err();
    assert!(matches!(err, graphrefly_graph::NameError::Destroyed));
}

#[test]
fn mount_path_with_separator_rejected() {
    let g = Graph::new("system", binding());
    let err = g.mount_new("a::b").unwrap_err();
    assert!(matches!(err, MountError::InvalidName(ref n) if n == "a::b"));
}

#[test]
fn re_mount_into_same_parent_collides_on_name() {
    // Cloning a `Graph` shares the inner state, so re-mounting under
    // the same parent collides on the namespace key before the
    // AlreadyMounted backlink check can fire. Multi-parent same-Core
    // mount (where AlreadyMounted is the user-visible error) needs a
    // separate Graph::with_core-equivalent escape hatch we don't
    // expose in v1.
    let parent = Graph::new("system", binding());
    let other_parent = parent.clone();
    let child = parent.mount_new("p").unwrap();
    let err = other_parent.mount("p", child).unwrap_err();
    assert!(matches!(err, MountError::NameCollision(_)));
}

#[test]
fn unmount_clears_parent_backlink() {
    // After `unmount`, the child's parent slot is `None`, so
    // `ancestors()` returns an empty chain.
    let parent = Graph::new("system", binding());
    let child = parent.mount_new("p").unwrap();
    let audit = parent.unmount("p").unwrap();
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
    let parent = Graph::new("system", binding());
    let child = parent.mount_new("p").unwrap();
    let third_clone = child.clone();
    parent.destroy();
    assert!(child.is_destroyed());
    assert!(third_clone.is_destroyed());
}

#[test]
fn destroy_preserves_namespace_during_teardown_cascade() {
    // /qa B3 fix (R3.7.3): registries are cleared AFTER the cascade
    // fires, so a sink subscribing during teardown can still resolve
    // the node's name via Graph::name_of.
    use std::sync::{Arc, Mutex};
    let g = Graph::new("system", binding());
    let s = g.state("sentinel_node", Some(HandleId::new(1))).unwrap();
    let observed_name: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let observed_for_sink = observed_name.clone();
    let g_for_sink = g.clone();
    let sink: Sink = Arc::new(move |msgs: &[Message]| {
        if msgs.iter().any(|m| matches!(m, Message::Teardown)) {
            // Look up the node's name DURING the teardown cascade.
            *observed_for_sink.lock().unwrap() = g_for_sink.name_of(s);
        }
    });
    let _sub = g.subscribe(s, sink);
    g.destroy();
    let name = observed_name.lock().unwrap().clone();
    assert_eq!(name.as_deref(), Some("sentinel_node"));
    // After destroy returns, the namespace IS cleared.
    assert_eq!(g.name_of(s), None);
}

#[test]
fn signal_invalidate_skips_meta_companions() {
    // /qa B2 fix (R3.7.2): graph-layer meta filter excludes
    // companions registered via add_meta_companion from the
    // signal_invalidate broadcast.
    let g = Graph::new("system", binding());
    let parent = g.state("parent", Some(HandleId::new(1))).unwrap();
    let companion = g.state("description", Some(HandleId::new(99))).unwrap();
    g.add_meta_companion(parent, companion);
    g.signal_invalidate();
    // Parent's cache wiped, companion's cache preserved.
    assert_eq!(g.cache_of(parent), graphrefly_core::NO_HANDLE);
    assert_eq!(g.cache_of(companion), HandleId::new(99));
}

#[test]
fn signal_invalidate_on_destroyed_graph_is_noop() {
    // /qa A4: destroyed graphs short-circuit signal_invalidate.
    let g = Graph::new("system", binding());
    g.destroy();
    g.signal_invalidate(); // must not panic, no-op
    assert!(g.is_destroyed());
}
