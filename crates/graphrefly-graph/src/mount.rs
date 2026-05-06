//! Mount / unmount + `ancestors()` (canonical spec §3.4).
//!
//! v1 limitation: shared-Core only. Mounting a child graph requires
//! `Arc::ptr_eq` of the inner `Arc<Core>` — cross-Core (multi-binding)
//! mount is post-M6 per Open Question 1 in
//! `archive/docs/SESSION-rust-port-architecture.md` Part 6.

use std::sync::Arc;

use crate::graph::{Graph, GraphInner, NameError};

/// Errors from `mount` / `mount_new` / `unmount`.
#[derive(Debug, thiserror::Error)]
pub enum MountError {
    #[error("Graph::mount: name `{0}` already mounted in this graph")]
    NameCollision(String),
    #[error("Graph::mount: name `{0}` collides with an existing local node name")]
    NodeNameCollision(String),
    #[error("Graph::mount: name `{0}` may not contain the `::` path separator")]
    InvalidName(String),
    #[error(
        "Graph::mount: child graph has a different Core (cross-Core mount is post-M6); \
         clone-and-rebuild against this graph's Core, or use `mount_new` + builder"
    )]
    CoreMismatch,
    #[error("Graph::mount: child graph already has a parent; unmount it first")]
    AlreadyMounted,
    #[error("Graph::unmount: no subgraph named `{0}`")]
    NotMounted(String),
    #[error("Graph::mount: graph has been destroyed")]
    Destroyed,
}

/// Result of [`Graph::unmount`] (and exposed for future
/// [`Graph::remove`] parity, canonical spec §3.2.1 `GraphRemoveAudit`).
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
            NameError::InvalidName(n) => Self::InvalidName(n),
            NameError::Destroyed => Self::Destroyed,
        }
    }
}

pub(crate) fn mount(parent: &Graph, name: String, child: Graph) -> Result<Graph, MountError> {
    if name.contains("::") {
        return Err(MountError::InvalidName(name));
    }
    // Same-Core check — child must share the parent's Core. Cross-Core
    // mount is post-M6 per session-doc Open Question 1.
    if !parent.core.same_dispatcher(&child.core) {
        return Err(MountError::CoreMismatch);
    }
    // Validate + claim the namespace slot under the parent lock so
    // concurrent mount(name, ...) calls cannot both pass validation
    // and overwrite each other's insert (TOCTOU fix from /qa Slice E+).
    // We acquire the child lock under the parent lock (lock ordering:
    // parent → child, never child → parent — there is no Graph code
    // that acquires a parent lock from inside a child lock). Both are
    // `parking_lot::Mutex` so the hold is short and contention-bound.
    {
        let mut parent_inner = parent.inner.lock();
        if parent_inner.destroyed {
            return Err(MountError::Destroyed);
        }
        if parent_inner.children.contains_key(&name) {
            return Err(MountError::NameCollision(name));
        }
        if parent_inner.names.contains_key(&name) {
            return Err(MountError::NodeNameCollision(name));
        }
        {
            let mut child_inner = child.inner.lock();
            if child_inner.parent.is_some() {
                return Err(MountError::AlreadyMounted);
            }
            child_inner.parent = Some(Arc::downgrade(&parent.inner));
        }
        parent_inner.children.insert(name, child.clone());
    }
    // Fire AFTER lock drops (P3 — reactive describe / observe_all
    // must see mounts as namespace changes).
    parent.fire_namespace_change();
    Ok(child)
}

pub(crate) fn mount_new(parent: &Graph, name: String) -> Result<Graph, MountError> {
    if name.contains("::") {
        return Err(MountError::InvalidName(name));
    }
    let parent_weak = Arc::downgrade(&parent.inner);
    // Hold the parent lock across validation, child construction, and
    // insert (TOCTOU fix from /qa Slice E+). `Graph::with_core` does
    // not take any lock that conflicts — it allocates `Arc<Mutex<...>>`
    // for the new child's GraphInner.
    let child = {
        let mut parent_inner = parent.inner.lock();
        if parent_inner.destroyed {
            return Err(MountError::Destroyed);
        }
        if parent_inner.children.contains_key(&name) {
            return Err(MountError::NameCollision(name));
        }
        if parent_inner.names.contains_key(&name) {
            return Err(MountError::NodeNameCollision(name));
        }
        let child = Graph::with_core(name.clone(), parent.core.clone(), Some(parent_weak));
        parent_inner.children.insert(name, child.clone());
        child
    };
    // Fire AFTER lock drops (P3).
    parent.fire_namespace_change();
    Ok(child)
}

pub(crate) fn unmount(parent: &Graph, name: &str) -> Result<GraphRemoveAudit, MountError> {
    let child = {
        let mut parent_inner = parent.inner.lock();
        if parent_inner.destroyed {
            return Err(MountError::Destroyed);
        }
        parent_inner
            .children
            .shift_remove(name)
            .ok_or_else(|| MountError::NotMounted(name.to_owned()))?
    };
    let audit = audit_of(&child);
    // Detach + destroy.
    child.inner.lock().parent = None;
    child.destroy();
    // Fire on the parent AFTER the child's destroy completes (P3).
    parent.fire_namespace_change();
    Ok(audit)
}

pub(crate) fn ancestors(graph: &Graph, include_self: bool) -> Vec<Graph> {
    let mut chain: Vec<Graph> = Vec::new();
    if include_self {
        chain.push(graph.clone());
    }
    // Walk up via Weak parent pointers. We need to reconstruct a `Graph`
    // from the `Arc<Mutex<GraphInner>>` — but `Graph` also needs a
    // `Core`. Because mount is shared-Core only, walking up never
    // changes the Core; reuse `graph.core.clone()`.
    let mut cursor: Option<Arc<parking_lot::Mutex<GraphInner>>> = graph
        .inner
        .lock()
        .parent
        .as_ref()
        .and_then(std::sync::Weak::upgrade);
    while let Some(inner) = cursor {
        let next_parent = inner
            .lock()
            .parent
            .as_ref()
            .and_then(std::sync::Weak::upgrade);
        chain.push(Graph {
            core: graph.core.clone(),
            inner,
        });
        cursor = next_parent;
    }
    chain
}

fn audit_of(graph: &Graph) -> GraphRemoveAudit {
    let inner = graph.inner.lock();
    let own = inner.names.len();
    let mount_count_self = inner.children.len();
    let mut node_count = own;
    let mut mount_count = mount_count_self;
    let kids: Vec<Graph> = inner.children.values().cloned().collect();
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
