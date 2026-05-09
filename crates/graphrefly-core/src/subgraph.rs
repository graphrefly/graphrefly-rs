//! Per-subgraph union-find registry (Slice X5, 2026-05-08).
//!
//! Direct port of [`graphrefly-py`'s
//! `subgraph_locks.py`](https://github.com/graphrefly/graphrefly-py/blob/main/src/graphrefly/core/subgraph_locks.py)
//! (locked design via [`SESSION-rust-port-d3-per-subgraph-parallelism.md`](https://github.com/graphrefly/graphrefly-ts/blob/main/archive/docs/SESSION-rust-port-d3-per-subgraph-parallelism.md)
//! — Q1 = (c-uf split-eager); see decision log D086).
//!
//! # Concept
//!
//! Each registered node is a member of exactly one connected
//! component (a "subgraph") at any moment. Two nodes are in the same
//! subgraph iff they're connected via dep edges (transitively).
//!
//! Connected-component membership is tracked via union-find with
//! union-by-rank + path compression. Each component's root carries
//! an [`Arc<SubgraphLockBox>`] that owns the partition's
//! `wave_owner` re-entrant mutex — nodes in the same component share
//! one lock, disjoint components run truly parallel.
//!
//! # Lifecycle hooks
//!
//! - [`SubgraphRegistry::ensure_registered`] — called when a node is
//!   first registered via [`crate::Core::register`]. Allocates a
//!   fresh singleton component for the new node.
//! - [`SubgraphRegistry::union_nodes`] — called for each new dep
//!   edge (in `register` and `set_deps`'s add-edge path). Merges
//!   the two endpoints' components.
//! - [`SubgraphRegistry::cleanup_node`] — wired but **not yet called
//!   in X5**. In Y1 the wave engine will invoke it from sites that
//!   actually remove a `NodeRecord` from `CoreState.nodes` (today
//!   `terminate_node` only marks the node terminal — `NodeRecord`s
//!   persist for the life of `CoreState`). The registry's HashMap
//!   entries drop together with `CoreState` when the last `Core`
//!   clone goes, so X5 doesn't leak partitions in practice; the
//!   per-node cleanup hook becomes load-bearing once Y1 lands a
//!   `Drop`-via-removal lifecycle for `NodeRecord` (post-Y1 work).
//! - [`SubgraphRegistry::on_edge_removed`] — called for each removed
//!   dep edge in `set_deps`'s remove path. Slice X5 commit-1 just
//!   notes the removal; **Y1 (split-eager)** adds the reachability
//!   walk that splits disconnected components.
//!
//! # Lock-acquisition discipline (Y1)
//!
//! [`SubgraphRegistry::lock_for`] returns the component's
//! [`Arc<SubgraphLockBox>`] — caller acquires `box.wave_owner` and
//! re-validates that the resolved root hasn't been redirected by a
//! concurrent `union` (mirrors py's `MAX_LOCK_RETRIES` retry loop).
//!
//! # Slice X5 scope
//!
//! Substrate types + registry tracking are wired to `Core::register`
//! and `Core::set_deps`'s edge add path so the union-find state is
//! maintained as nodes register and topology changes. The wave engine
//! itself still uses the legacy Core-level `wave_owner` — Y1 (the
//! wave-engine migration to per-partition `wave_owner`) is carried
//! forward as an explicit follow-on slice given its scope (every
//! `begin_batch` / `run_wave` call site + `Core::subscribe` lock
//! acquisition + `BatchGuard` lock retention + retry-validate
//! semantics for held-Arc-vs-current-root divergence on union).
//!
//! Several substrate methods (`cleanup_node`, `lock_for`,
//! `lock_for_validate`) and the `SubgraphLockBox::wave_owner` field
//! are wired but unused in X5; they're annotated per-item with
//! `#[allow(dead_code)]` until Y1 activates the wave engine through
//! them. (Per-item rather than module-level shotgun so any genuinely
//! unused new item still flags as dead code during X5 follow-up
//! review.)

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::ReentrantMutex;

use crate::handle::NodeId;

/// Newtype identifier for a connected-component partition.
///
/// Internally a `u64` (the union-find root's `NodeId.raw()`). Distinct
/// from [`NodeId`] at the type system level — partitions and nodes are
/// not interchangeable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SubgraphId(pub(crate) u64);

impl SubgraphId {
    /// Construct from a [`NodeId`] root. Internal use — partition
    /// identity is the union-find root's NodeId.
    #[must_use]
    pub(crate) fn from_node(node: NodeId) -> Self {
        Self(node.raw())
    }

    /// Raw u64 view. Used for total-ordering across multi-partition
    /// wave acquisitions per Q4=(a) (deadlock-free via ascending
    /// SubgraphId order).
    #[must_use]
    pub fn raw(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for SubgraphId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "subgraph#{}", self.0)
    }
}

/// Per-component lock box. Holds the partition's `wave_owner`
/// re-entrant mutex. On `union`, the box of the smaller-rank root is
/// replaced by the larger-rank root's box — the lock identity is
/// preserved across merges via the [`Arc`] reference (mirrors py's
/// `_LockBox.lock` redirect on union).
///
/// **Slice Y1 (D3 / Phase E, 2026-05-08):** the wave engine acquires
/// this per-partition lock via [`crate::Core::partition_wave_owner_lock_arc`]
/// (with retry-validate against concurrent union/split). Closure-form
/// `Core::batch` acquires every partition's lock in ascending
/// [`SubgraphId`] order; per-seed entry points (`emit`, `subscribe`,
/// etc.) acquire only the seed's transitively-touched partitions.
pub struct SubgraphLockBox {
    /// Per-partition wave ownership. Cross-thread emits to the same
    /// partition serialize here; same-thread re-entry passes through.
    /// Wrapped in `Arc` at the registry level so two roots' boxes
    /// can share the same mutex identity after `union`.
    pub(crate) wave_owner: Arc<ReentrantMutex<()>>,
}

impl SubgraphLockBox {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            wave_owner: Arc::new(ReentrantMutex::new(())),
        })
    }
}

/// Default cap for [`SubgraphRegistry::lock_for`]'s root-validation
/// retry loop. Mirrors graphrefly-py's `_MAX_LOCK_RETRIES` constant
/// (`subgraph_locks.py` line 39). Reached only under continuous
/// `union` activity racing with `lock_for` — pathological in
/// practice but bounded for safety.
///
/// Consumed by [`crate::Core::partition_wave_owner_lock_arc`] under
/// the retry-validate loop (Slice Y1 / Phase E, 2026-05-08).
pub(crate) const MAX_LOCK_RETRIES: u32 = 100;

/// Union-find registry tracking each node's connected-component
/// membership. Wrapped by [`crate::Core`] in `Arc<Mutex<...>>` so
/// the wave engine can resolve a node's partition before acquiring
/// that partition's `wave_owner`.
pub struct SubgraphRegistry {
    /// `node_id → parent_id` union-find array. A root has
    /// `parent[root] == root`.
    parent: HashMap<NodeId, NodeId>,
    /// `node_id → rank` for union-by-rank. Only meaningful for roots.
    rank: HashMap<NodeId, u32>,
    /// Reverse map for cleanup-time re-rooting: `node_id → set of
    /// direct children whose `parent` field points at `node_id``.
    /// On root cleanup, we promote one child to root and re-attach
    /// the others.
    children: HashMap<NodeId, HashSet<NodeId>>,
    /// `root_node_id → Arc<SubgraphLockBox>`. Only roots have entries.
    /// On `union`, the loser's entry is removed and any future
    /// `lock_for` calls on its members find the winner's box via
    /// `find`.
    boxes: HashMap<NodeId, Arc<SubgraphLockBox>>,
}

impl SubgraphRegistry {
    /// Construct an empty registry. `pub(crate)` because all useful
    /// operations on the registry are crate-internal — only `Core::new`
    /// needs to construct one. External callers receive
    /// [`SubgraphId`]s via [`Core::partition_of`] but cannot construct
    /// a registry from outside the crate.
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            parent: HashMap::new(),
            rank: HashMap::new(),
            children: HashMap::new(),
            boxes: HashMap::new(),
        }
    }

    /// Register `node` as the root of a fresh singleton component.
    /// Idempotent — calling twice on the same node is a no-op.
    ///
    /// Called from [`crate::Core::register`] right after the new
    /// `NodeId` is allocated, BEFORE any `union_nodes` calls for the
    /// node's deps.
    pub(crate) fn ensure_registered(&mut self, node: NodeId) {
        if self.parent.contains_key(&node) {
            return;
        }
        self.parent.insert(node, node);
        self.rank.insert(node, 0);
        self.children.insert(node, HashSet::new());
        self.boxes.insert(node, SubgraphLockBox::new());
    }

    /// Find the root of `node`'s component, with path compression.
    /// Panics if `node` is not registered.
    ///
    /// **Iterative two-pass implementation** (Slice X5 /qa):
    /// pass 1 walks up the parent chain to the root; pass 2 re-links
    /// every node on the path directly to the root + maintains the
    /// `children` reverse-map. Iterative form mirrors the existing
    /// `terminate_node` cascade discipline (and graphrefly-py's
    /// `_find_locked` while-loop) — keeps stack depth O(1) regardless
    /// of pre-compression chain length, avoiding stack overflow on
    /// pathological un-compressed trees that can briefly form before
    /// path compression settles (especially relevant once Y1 puts
    /// `find` on the hot path under `lock_for`).
    #[must_use]
    pub(crate) fn find(&mut self, node: NodeId) -> NodeId {
        // Pass 1: walk up to root (no mutation).
        let mut cur = node;
        loop {
            let parent = *self
                .parent
                .get(&cur)
                .expect("subgraph_registry::find: node not registered");
            if parent == cur {
                break; // cur is root
            }
            cur = parent;
        }
        let root = cur;

        // Pass 2: path compression — re-link every node on the original
        // walk directly to root, maintaining the `children` reverse-map.
        // Skip when already pointing at root (no-op for root itself, and
        // for already-compressed nodes).
        let mut walker = node;
        while walker != root {
            let parent = *self
                .parent
                .get(&walker)
                .expect("walker on path-to-root must be registered");
            if parent != root {
                self.parent.insert(walker, root);
                if let Some(old_kids) = self.children.get_mut(&parent) {
                    old_kids.remove(&walker);
                }
                self.children.entry(root).or_default().insert(walker);
            }
            walker = parent;
        }
        root
    }

    /// Merge the components containing `a` and `b`. Both nodes must
    /// already be registered. After this call, `find(a) == find(b)`.
    ///
    /// Union-by-rank: the smaller-rank root becomes a child of the
    /// larger. Equal-rank breaks the tie by promoting `a`'s root and
    /// bumping its rank.
    ///
    /// On merge, the loser's [`SubgraphLockBox`] entry is removed
    /// from `boxes`. Future `lock_for` calls on the loser's members
    /// resolve to the winner's root → winner's box.
    pub(crate) fn union_nodes(&mut self, a: NodeId, b: NodeId) {
        debug_assert!(
            self.parent.contains_key(&a) && self.parent.contains_key(&b),
            "union_nodes: both nodes must be registered first"
        );
        // Defense-in-depth against bypassed cycle detection: a self-edge
        // `set_deps(n, &[n])` would reach here as `union_nodes(n, n)`.
        // Cycle rejection in `Core::set_deps` should catch this BEFORE
        // we get called, but the registry has no other defense.
        debug_assert!(
            a != b,
            "union_nodes called with self-edge — \
             Core's cycle detection bypassed?"
        );
        let mut root_a = self.find(a);
        let mut root_b = self.find(b);
        if root_a == root_b {
            return;
        }
        let rank_a = *self.rank.get(&root_a).unwrap_or(&0);
        let rank_b = *self.rank.get(&root_b).unwrap_or(&0);
        if rank_a < rank_b {
            std::mem::swap(&mut root_a, &mut root_b);
        }
        // root_a is the winner (kept). root_b becomes a child.
        self.parent.insert(root_b, root_a);
        self.children.entry(root_a).or_default().insert(root_b);
        if rank_a == rank_b {
            self.rank.insert(root_a, rank_a + 1);
        }
        // Drop the loser's box. Any in-flight readers holding an Arc
        // clone keep it alive; the registry no longer references it.
        self.boxes.remove(&root_b);
    }

    /// Remove `node` from the registry. If `node` was a root, promote
    /// one of its direct children as the new root and re-link the
    /// others. Mirrors py's `_on_gc` (`subgraph_locks.py` lines 53–92).
    ///
    /// **Slice X5 status:** wired but NOT called from Core. Today
    /// `Core::terminate_node` marks a node terminal but does NOT remove
    /// it from `CoreState.nodes` (NodeRecords persist for the life of
    /// `CoreState`); the registry's HashMap entries drop together with
    /// `CoreState` when the last `Core` clone goes, so the partition
    /// state is reclaimed without an explicit per-node cleanup. Y1
    /// will revisit this — when the wave engine activates per-partition
    /// `wave_owner`, terminated nodes' partition entries should be
    /// purged so a stale partition doesn't keep its `Arc<SubgraphLockBox>`
    /// alive in the registry. Idempotent on unregistered nodes.
    #[allow(dead_code)]
    pub(crate) fn cleanup_node(&mut self, node: NodeId) {
        let Some(parent) = self.parent.get(&node).copied() else {
            return; // already cleaned up — idempotent.
        };

        let direct_children: Vec<NodeId> = self
            .children
            .get(&node)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default();

        if parent == node {
            // `node` was the root.
            if let Some(&new_root) = direct_children.first() {
                self.parent.insert(new_root, new_root);
                // Detach `node` from each grandchild's children-set first
                // (before borrowing children mut for the new_root entry).
                for child in &direct_children {
                    if let Some(kids) = self.children.get_mut(child) {
                        kids.remove(&node);
                    }
                }
                // Re-attach all children except `new_root` to `new_root`.
                let new_root_kids = self.children.entry(new_root).or_default();
                for child in direct_children.iter().skip(1).copied() {
                    self.parent.insert(child, new_root);
                    new_root_kids.insert(child);
                }
                // Box ownership transfers to the new root — same Arc, so
                // any in-flight `lock_for` holders keep their guard alive.
                if let Some(box_arc) = self.boxes.remove(&node) {
                    self.boxes.insert(new_root, box_arc);
                }
                let old_rank = self.rank.get(&node).copied().unwrap_or(0);
                let new_rank = self.rank.entry(new_root).or_insert(0);
                if old_rank > *new_rank {
                    *new_rank = old_rank;
                }
            } else {
                // Singleton root — just remove the box.
                self.boxes.remove(&node);
            }
        } else {
            // `node` was a non-root. Detach from its parent's children;
            // re-attach its own children to its parent.
            if let Some(parent_kids) = self.children.get_mut(&parent) {
                parent_kids.remove(&node);
                for child in &direct_children {
                    parent_kids.insert(*child);
                }
            }
            for child in &direct_children {
                self.parent.insert(*child, parent);
            }
        }

        self.children.remove(&node);
        self.parent.remove(&node);
        self.rank.remove(&node);
        // boxes already removed above on root path; non-root path never
        // had a box entry.
    }

    /// Hook for an edge removal. Slice X5 commit-1: was a no-op
    /// (monotonic-merge stepping stone). Slice Y1 / Phase F (D3
    /// split-eager, 2026-05-09): the actual reachability walk +
    /// split decision lives in `Core::set_deps` (it has access to
    /// `s.children` and `s.nodes`'s `dep_records` for the undirected
    /// dep-edge graph). When `Core` detects disconnection it calls
    /// [`Self::split_partition`] directly. This `on_edge_removed`
    /// hook remains as a no-op marker — kept for API symmetry with
    /// [`Self::union_nodes`] and for future use if the registry
    /// gains its own edge-graph view.
    pub(crate) fn on_edge_removed(&mut self, _from: NodeId, _to: NodeId) {
        // No-op. See `Core::set_deps` Phase F split-eager block which
        // calls `split_partition` directly when an edge removal causes
        // disconnection in the undirected dep-edge graph.
    }

    /// Split an existing component into two, given the keep-side and
    /// orphan-side membership. Slice Y1 / Phase F (D3 split-eager,
    /// 2026-05-09).
    ///
    /// **Invariants assumed by caller:**
    /// 1. `component_nodes` lists every node currently in some single
    ///    component C of this registry.
    /// 2. `keep_side` ⊆ `component_nodes` is the post-removal
    ///    connected subset that should retain C's existing
    ///    [`SubgraphLockBox`] (Arc identity preserved). `keep_side`
    ///    must be non-empty.
    /// 3. The orphan side (`component_nodes - keep_side`) must be
    ///    non-empty. (If it were empty, no split is needed; caller
    ///    should not invoke this method.)
    /// 4. `edges_in_component` lists the dep edges (as `(parent,
    ///    child)` data-flow pairs) currently present in
    ///    `Core::s.children` / `s.nodes[*].dep_records`,
    ///    POST-removal of the triggering edge. The edges connect
    ///    nodes within `component_nodes`. The caller is responsible
    ///    for filtering to in-component edges.
    ///
    /// **Algorithm (mirrors graphrefly-py [`subgraph_locks.py`](https://github.com/graphrefly/graphrefly-py/blob/main/src/graphrefly/core/subgraph_locks.py)
    /// `_on_split` plus a Rust-idiomatic Arc redirect):**
    /// 1. Capture the original [`Arc<SubgraphLockBox>`] for the
    ///    component's pre-split root.
    /// 2. Reset every component node to a singleton (parent[n] = n,
    ///    rank = 0, children = ∅).
    /// 3. Re-union via `edges_in_component` — each edge calls
    ///    [`Self::union_nodes`], which restores connectivity within
    ///    each side independently (since `edges_in_component`
    ///    contains only the post-removal edges, the two sides stay
    ///    disconnected from each other in the union-find tree).
    /// 4. Resolve the new keep-side root and orphan-side root.
    /// 5. Assign the original lock box to the keep-side root (so
    ///    in-flight waves holding the original `Arc` succeed
    ///    `lock_for_validate` against the keep-side); allocate a
    ///    fresh box for the orphan-side root (in-flight waves on
    ///    the orphan side fail validate, retry with the new box).
    pub(crate) fn split_partition(
        &mut self,
        component_nodes: &[NodeId],
        keep_side_nodes: &[NodeId],
        edges_in_component: &[(NodeId, NodeId)],
    ) {
        debug_assert!(
            !component_nodes.is_empty(),
            "component_nodes must be non-empty"
        );
        debug_assert!(
            !keep_side_nodes.is_empty(),
            "keep_side_nodes must be non-empty"
        );
        // Build a contains-lookup set for the keep side. The caller
        // passes a slice (avoids cross-crate-internal HashSet flavor
        // mismatch — `std::collections::HashSet` vs `ahash::AHashSet`).
        let keep_side: HashSet<NodeId> = keep_side_nodes.iter().copied().collect();
        debug_assert!(
            component_nodes.iter().any(|n| !keep_side.contains(n)),
            "orphan side must be non-empty (no-op caller)"
        );

        // Step 1: capture original box.
        let original_root = self.find(component_nodes[0]);
        let original_box = self
            .boxes
            .remove(&original_root)
            .expect("original_root must have a registered box");

        // Step 2: reset every component node to a singleton.
        for &n in component_nodes {
            self.parent.insert(n, n);
            self.rank.insert(n, 0);
            self.children.insert(n, HashSet::new());
        }

        // Step 3: re-union via post-removal edges. Both sides become
        // internally connected; the two sides remain disconnected from
        // each other (since the triggering edge was removed and the
        // caller's BFS verified disconnection).
        for &(a, b) in edges_in_component {
            // `union_nodes` is idempotent on already-merged components,
            // so multiple edges into the same connected subset are fine.
            // Self-edges shouldn't appear here (Core's cycle detection
            // rejects them at edge-mutation time) but guard defensively.
            if a != b {
                self.union_nodes(a, b);
            }
        }

        // Step 4: resolve roots. Pick any keep-side / orphan-side
        // representative and find its post-re-union root.
        let keep_repr = keep_side_nodes[0];
        let keep_root = self.find(keep_repr);
        let orphan_repr = *component_nodes
            .iter()
            .find(|n| !keep_side.contains(n))
            .expect("non-empty orphan side");
        let orphan_root = self.find(orphan_repr);
        debug_assert!(
            keep_root != orphan_root,
            "split_partition: keep_root {keep_root:?} and orphan_root {orphan_root:?} \
             must be distinct after re-union — caller's BFS must have asserted \
             disconnection"
        );

        // Step 5: assign boxes. Original box → keep-side root (so
        // in-flight waves' `lock_for_validate` against the held Arc
        // succeed for keep-side nodes). Fresh box → orphan-side root.
        self.boxes.insert(keep_root, original_box);
        self.boxes.insert(orphan_root, SubgraphLockBox::new());
    }

    /// Resolve `node`'s partition lock box. Caller acquires the
    /// box's `wave_owner`, then SHOULD re-validate via
    /// [`Self::lock_for_validate`] that the resolved root hasn't
    /// shifted under a concurrent `union` (lock-validation retry
    /// loop, mirroring py `lock_for` lines 154–178).
    ///
    /// Returns `None` if `node` is not registered (defensive — should
    /// not happen in correct call paths).
    ///
    /// Consumed by [`crate::Core::partition_wave_owner_lock_arc`]
    /// under the retry-validate loop (Slice Y1 / Phase E, 2026-05-08).
    #[must_use]
    pub(crate) fn lock_for(&mut self, node: NodeId) -> Option<(SubgraphId, Arc<SubgraphLockBox>)> {
        if !self.parent.contains_key(&node) {
            return None;
        }
        let root = self.find(node);
        let box_arc = self.boxes.get(&root).cloned()?;
        Some((SubgraphId::from_node(root), box_arc))
    }

    /// Re-validate that `node`'s root resolves to `expected_box`. Used
    /// in the retry loop after acquiring the box's `wave_owner`: if a
    /// concurrent `union` redirected the root mid-acquire, the held
    /// lock is for the wrong partition and the caller must release +
    /// retry.
    ///
    /// Consumed by [`crate::Core::partition_wave_owner_lock_arc`]
    /// under the retry-validate loop (Slice Y1 / Phase E, 2026-05-08).
    #[must_use]
    pub(crate) fn lock_for_validate(
        &mut self,
        node: NodeId,
        expected_box: &Arc<SubgraphLockBox>,
    ) -> bool {
        let Some(root) = self.parent.get(&node).copied() else {
            return false;
        };
        let actual_root = self.find(root);
        match self.boxes.get(&actual_root) {
            Some(actual) => Arc::ptr_eq(actual, expected_box),
            None => false,
        }
    }

    /// Snapshot of every currently-existing partition, sorted in
    /// ascending [`SubgraphId`] order. Returns `(partition_id, lock_box)`
    /// pairs — the caller acquires `box.wave_owner.lock_arc()` on each
    /// in ascending order to enter a closure-form batch (which doesn't
    /// have a known seed and must serialize against ALL partitions per
    /// session-doc Q7 + decision D092).
    ///
    /// Consumed by `Core::all_partitions_lock_boxes` (Slice Y1 /
    /// Phase E, 2026-05-08).
    #[must_use]
    pub(crate) fn all_partitions(&self) -> Vec<(SubgraphId, Arc<SubgraphLockBox>)> {
        let mut out: Vec<(SubgraphId, Arc<SubgraphLockBox>)> = self
            .boxes
            .iter()
            .map(|(root, box_arc)| (SubgraphId::from_node(*root), Arc::clone(box_arc)))
            .collect();
        out.sort_unstable_by_key(|(sid, _)| *sid);
        out
    }

    /// Number of registered nodes. Useful for debugging + acceptance
    /// tests that verify the registry stays in sync with `Core::nodes`.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.parent.len()
    }

    /// Snapshot of every currently-registered node. Used by
    /// `Core::set_deps`'s split-eager block (Slice Y1 / Phase F,
    /// 2026-05-09) to enumerate candidate nodes for a component-
    /// membership filter; iterating + calling `find` would alias-
    /// borrow `&mut self`, so we snapshot first.
    #[must_use]
    pub(crate) fn registered_nodes(&self) -> Vec<NodeId> {
        self.parent.keys().copied().collect()
    }

    /// Number of distinct connected components. Two threads emitting
    /// into nodes with distinct partitions can run truly parallel
    /// (Y1+); X5 substrate does not yet exercise that property.
    #[must_use]
    pub fn component_count(&self) -> usize {
        self.boxes.len()
    }

    /// Resolve `node`'s partition. Returns `None` for unregistered
    /// nodes. Mutating because path compression may relink under
    /// `find`.
    #[must_use]
    pub fn partition_of(&mut self, node: NodeId) -> Option<SubgraphId> {
        if !self.parent.contains_key(&node) {
            return None;
        }
        Some(SubgraphId::from_node(self.find(node)))
    }
}

// No `Default` impl — the registry is crate-internal infrastructure;
// `Core::new` is the only construction site. Adding a public `Default`
// would let external callers build a registry that can do nothing
// useful (every operation method is `pub(crate)`).

#[cfg(test)]
mod tests {
    use super::*;

    fn n(raw: u64) -> NodeId {
        NodeId::new(raw)
    }

    #[test]
    fn singleton_register_creates_one_partition() {
        let mut r = SubgraphRegistry::new();
        r.ensure_registered(n(1));
        assert_eq!(r.node_count(), 1);
        assert_eq!(r.component_count(), 1);
        assert_eq!(r.find(n(1)), n(1));
    }

    #[test]
    fn union_merges_two_singletons() {
        let mut r = SubgraphRegistry::new();
        r.ensure_registered(n(1));
        r.ensure_registered(n(2));
        assert_eq!(r.component_count(), 2);
        r.union_nodes(n(1), n(2));
        assert_eq!(r.component_count(), 1);
        assert_eq!(r.find(n(1)), r.find(n(2)));
    }

    #[test]
    fn union_idempotent_within_same_component() {
        let mut r = SubgraphRegistry::new();
        r.ensure_registered(n(1));
        r.ensure_registered(n(2));
        r.union_nodes(n(1), n(2));
        let comp_before = r.component_count();
        r.union_nodes(n(1), n(2));
        assert_eq!(r.component_count(), comp_before);
    }

    #[test]
    fn cleanup_singleton_removes_partition() {
        let mut r = SubgraphRegistry::new();
        r.ensure_registered(n(1));
        r.cleanup_node(n(1));
        assert_eq!(r.node_count(), 0);
        assert_eq!(r.component_count(), 0);
    }

    #[test]
    fn cleanup_root_promotes_child() {
        let mut r = SubgraphRegistry::new();
        r.ensure_registered(n(1));
        r.ensure_registered(n(2));
        r.union_nodes(n(1), n(2));
        // After union: one of {1, 2} is root.
        let root_before = r.find(n(1));
        let child = if root_before == n(1) { n(2) } else { n(1) };
        r.cleanup_node(root_before);
        // The remaining node should be its own root.
        assert_eq!(r.find(child), child);
        assert_eq!(r.component_count(), 1);
    }

    #[test]
    fn cleanup_non_root_re_links_grandchildren_to_parent() {
        let mut r = SubgraphRegistry::new();
        for i in 1..=3 {
            r.ensure_registered(n(i));
        }
        r.union_nodes(n(1), n(2));
        r.union_nodes(n(2), n(3));
        let root_before = r.find(n(1));
        // Pick a non-root node to clean up.
        let non_root = if root_before == n(1) {
            n(2)
        } else if root_before == n(2) {
            n(1)
        } else {
            n(2)
        };
        r.cleanup_node(non_root);
        // Remaining nodes should still find a single root.
        let other = (1..=3u64)
            .map(n)
            .find(|x| *x != root_before && *x != non_root)
            .expect("third node");
        assert_eq!(r.find(root_before), r.find(other));
    }

    #[test]
    fn lock_for_returns_same_box_for_same_component() {
        let mut r = SubgraphRegistry::new();
        r.ensure_registered(n(1));
        r.ensure_registered(n(2));
        r.union_nodes(n(1), n(2));
        let (_sid_a, box_a) = r.lock_for(n(1)).expect("registered");
        let (_sid_b, box_b) = r.lock_for(n(2)).expect("registered");
        assert!(Arc::ptr_eq(&box_a, &box_b));
    }

    #[test]
    fn lock_for_validate_detects_redirect_after_union() {
        // Slice X5 /qa P6: deterministic redirect via forced rank
        // skew. Pre-fix this test asserted conditionally based on
        // which node union promoted, masking a hypothetical regression
        // where `lock_for_validate` always returned `true`. Now the
        // setup forces n(1)'s root to be displaced — the assertion
        // is unconditionally `false`.
        let mut r = SubgraphRegistry::new();
        for i in 1..=4 {
            r.ensure_registered(n(i));
        }
        // Build a higher-rank tree under n(2)'s root: union n(2)+n(3)
        // and n(2)+n(4), which under union-by-rank with equal initial
        // ranks promotes n(2) and bumps its rank by the second union.
        // After this, find(n(2)) == n(2) and rank[n(2)] >= 1.
        r.union_nodes(n(2), n(3));
        r.union_nodes(n(2), n(4));
        let n2_root = r.find(n(2));
        // n(1) stays its own singleton (rank 0). Resolve its box BEFORE
        // the cross-tree union.
        let (_sid_before, box_1_alone) = r.lock_for(n(1)).expect("registered");
        let n1_root_before = r.find(n(1));
        assert_eq!(n1_root_before, n(1), "n(1) is still its own root");

        // Cross-tree union: union-by-rank promotes the higher-rank tree
        // (n(2)'s) — n(1)'s root MUST become n(2)'s root.
        r.union_nodes(n(1), n(2));
        let n1_root_after = r.find(n(1));
        assert_eq!(
            n1_root_after, n2_root,
            "union-by-rank promoted n(2)'s tree; n(1)'s root displaced"
        );

        // The previously-resolved box is now stale: lock_for_validate
        // must detect the redirect unconditionally.
        let still_valid = r.lock_for_validate(n(1), &box_1_alone);
        assert!(
            !still_valid,
            "lock_for_validate must detect the box-redirect after union promotes a different root"
        );
        // And lock_for now returns a different box.
        let (_sid_after, box_after) = r.lock_for(n(1)).expect("registered");
        assert!(
            !Arc::ptr_eq(&box_1_alone, &box_after),
            "stale box and active box must be distinct Arc identities"
        );
    }

    #[test]
    fn partition_of_distinct_singletons_differ() {
        let mut r = SubgraphRegistry::new();
        r.ensure_registered(n(1));
        r.ensure_registered(n(2));
        let p1 = r.partition_of(n(1)).expect("registered");
        let p2 = r.partition_of(n(2)).expect("registered");
        assert_ne!(p1, p2);
    }

    #[test]
    fn partition_of_unioned_nodes_match() {
        let mut r = SubgraphRegistry::new();
        r.ensure_registered(n(1));
        r.ensure_registered(n(2));
        r.union_nodes(n(1), n(2));
        let p1 = r.partition_of(n(1)).expect("registered");
        let p2 = r.partition_of(n(2)).expect("registered");
        assert_eq!(p1, p2);
    }
}
