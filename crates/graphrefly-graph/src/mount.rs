//! Mount / unmount + `ancestors()` (canonical spec §3.4).
//!
//! D246: subgraphs are just child levels of the **Core-free** namespace
//! tree (`Arc<Mutex<GraphInner>>`); the embedder owns the one `Core`.
//! `mount` / `mount_new` / `ancestors` are pure-namespace (no `&Core`);
//! `unmount` takes the embedder's `&Core` because it runs the TEARDOWN
//! cascade. Returns plain [`Graph`] handles (a cheap `Arc` clone). The
//! old `SubgraphRef`/`same_dispatcher`/`CoreMismatch` machinery is
//! deleted — there is only ever the one embedder `Core` (no Core lives
//! on a `Graph` to mismatch).

use std::sync::{Arc, Weak};

use graphrefly_core::Core;
use parking_lot::Mutex;

use crate::graph::{destroy_subtree, fire_ns, Graph, GraphInner, NameError};

/// Errors from `mount` / `mount_new` / `unmount`.
#[derive(Debug, thiserror::Error)]
pub enum MountError {
    #[error("Graph::mount: name `{0}` already mounted in this graph")]
    NameCollision(String),
    #[error("Graph::mount: name `{0}` collides with an existing local node name")]
    NodeNameCollision(String),
    #[error("Graph::mount: name `{0}` may not contain the `::` path separator")]
    InvalidName(String),
    #[error("Graph::mount: child graph already has a parent; unmount it first")]
    AlreadyMounted,
    #[error("Graph::unmount: no subgraph named `{0}`")]
    NotMounted(String),
    #[error("Graph::mount: graph has been destroyed")]
    Destroyed,
}

/// Result of [`Graph::unmount`] / [`Graph::remove`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphRemoveAudit {
    /// Number of nodes torn down (own + recursive into mounts).
    pub node_count: usize,
    /// Number of mounts torn down (recursive count).
    pub mount_count: usize,
}

impl From<NameError> for MountError {
    fn from(err: NameError) -> Self {
        match err {
            NameError::Collision(n) => Self::NodeNameCollision(n),
            NameError::InvalidName(n) | NameError::ReservedPrefix(n) => Self::InvalidName(n),
            NameError::Destroyed => Self::Destroyed,
        }
    }
}

/// Construct a fresh empty child `GraphInner` handle with `parent` set.
fn new_child_inner(name: String, parent: Weak<Mutex<GraphInner>>) -> Arc<Mutex<GraphInner>> {
    Arc::new(Mutex::new(GraphInner {
        name,
        names: indexmap::IndexMap::new(),
        names_inverse: indexmap::IndexMap::new(),
        children: indexmap::IndexMap::new(),
        parent: Some(parent),
        destroyed: false,
        namespace_sinks: indexmap::IndexMap::new(),
        next_ns_sink_id: 0,
    }))
}

pub(crate) fn mount(
    core: &Core,
    parent_inner: &Arc<Mutex<GraphInner>>,
    name: String,
    child: &Graph,
) -> Result<Graph, MountError> {
    if name.contains("::") {
        return Err(MountError::InvalidName(name));
    }
    let child_inner = child.inner_arc().clone();
    // Validate + claim the namespace slot under the parent lock so
    // concurrent mount(name, ...) calls cannot both pass validation
    // (TOCTOU fix from /qa Slice E+). Lock order: parent → child.
    {
        let mut p = parent_inner.lock();
        if p.destroyed {
            return Err(MountError::Destroyed);
        }
        if p.children.contains_key(&name) {
            return Err(MountError::NameCollision(name));
        }
        if p.names.contains_key(&name) {
            return Err(MountError::NodeNameCollision(name));
        }
        {
            let mut c = child_inner.lock();
            if c.parent.is_some() {
                return Err(MountError::AlreadyMounted);
            }
            c.parent = Some(Arc::downgrade(parent_inner));
        }
        p.children.insert(name, child_inner.clone());
    }
    // Fire AFTER lock drops (P3 — reactive describe / observe_all must
    // see mounts as namespace changes). Owner-side `&Core` (D246 r2:
    // firing ns sinks hands them `&Core`, so it IS a Core-touching op).
    fire_ns(core, parent_inner);
    Ok(Graph::from_inner(child_inner))
}

pub(crate) fn mount_new(
    core: &Core,
    parent_inner: &Arc<Mutex<GraphInner>>,
    name: String,
) -> Result<Graph, MountError> {
    if name.contains("::") {
        return Err(MountError::InvalidName(name));
    }
    let parent_weak = Arc::downgrade(parent_inner);
    // Hold the parent lock across validation + child construction +
    // insert (TOCTOU fix from /qa Slice E+).
    let child_inner = {
        let mut p = parent_inner.lock();
        if p.destroyed {
            return Err(MountError::Destroyed);
        }
        if p.children.contains_key(&name) {
            return Err(MountError::NameCollision(name));
        }
        if p.names.contains_key(&name) {
            return Err(MountError::NodeNameCollision(name));
        }
        let child_inner = new_child_inner(name.clone(), parent_weak);
        p.children.insert(name, child_inner.clone());
        child_inner
    };
    // Fire AFTER lock drops (P3). Owner-side `&Core` (D246 r2).
    fire_ns(core, parent_inner);
    Ok(Graph::from_inner(child_inner))
}

pub(crate) fn unmount(
    core: &Core,
    parent_inner: &Arc<Mutex<GraphInner>>,
    name: &str,
) -> Result<GraphRemoveAudit, MountError> {
    let child = {
        let mut p = parent_inner.lock();
        if p.destroyed {
            return Err(MountError::Destroyed);
        }
        p.children
            .shift_remove(name)
            .ok_or_else(|| MountError::NotMounted(name.to_owned()))?
    };
    let audit = audit_of(&child);
    // Detach + destroy.
    child.lock().parent = None;
    destroy_subtree(core, &child);
    // Fire on the parent AFTER the child's destroy completes (P3).
    fire_ns(core, parent_inner);
    Ok(audit)
}

pub(crate) fn ancestors(inner: &Arc<Mutex<GraphInner>>, include_self: bool) -> Vec<Graph> {
    let mut chain: Vec<Graph> = Vec::new();
    if include_self {
        chain.push(Graph::from_inner(inner.clone()));
    }
    // Walk up via Weak parent pointers.
    //
    // Slice V3: visited-set cycle insurance per porting-deferred.md.
    let mut visited = std::collections::HashSet::new();
    visited.insert(Arc::as_ptr(inner) as usize);
    let mut cursor: Option<Arc<Mutex<GraphInner>>> = inner
        .lock()
        .parent
        .as_ref()
        .and_then(std::sync::Weak::upgrade);
    while let Some(cur) = cursor {
        let ptr = Arc::as_ptr(&cur) as usize;
        if !visited.insert(ptr) {
            break; // Cycle detected — break rather than infinite-loop.
        }
        let next_parent = cur
            .lock()
            .parent
            .as_ref()
            .and_then(std::sync::Weak::upgrade);
        chain.push(Graph::from_inner(cur));
        cursor = next_parent;
    }
    chain
}

fn audit_of(inner_arc: &Arc<Mutex<GraphInner>>) -> GraphRemoveAudit {
    let inner = inner_arc.lock();
    let own = inner.names.len();
    let mount_count_self = inner.children.len();
    let mut node_count = own;
    let mut mount_count = mount_count_self;
    let kids: Vec<Arc<Mutex<GraphInner>>> = inner.children.values().cloned().collect();
    drop(inner);
    for kid in kids {
        let sub = audit_of(&kid);
        node_count += sub.node_count;
        mount_count += sub.mount_count;
    }
    GraphRemoveAudit {
        node_count,
        mount_count,
    }
}
