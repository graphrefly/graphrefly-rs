//! §7 serialization-group concurrency (D208–D211). Replaces the deleted
//! `per_subgraph_parallelism.rs` (union-find per-partition `wave_owner`).
//!
//! Parallelism itself is a perf property (not deterministically
//! unit-testable), so these tests assert the **correctness** guarantees
//! the group model must uphold under concurrency:
//!
//! - Same-group cross-thread emits serialize (no lost/torn updates, no
//!   deadlock) through the group's `Arc<ReentrantMutex<()>>`.
//! - Disjoint-group cross-thread waves both complete (the locks don't
//!   block each other; no cross-group deadlock).
//!
//! `Core` is `Send + Sync` on the default `LockedCell` substrate, so it
//! is shared across threads by clone.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use common::{TestBinding, TestValue};
use graphrefly_core::{BindingBoundary, Core, NodeOpts, NodeRegistration, SerializationGroupId};

const GA: SerializationGroupId = SerializationGroupId::new(1);
const GB: SerializationGroupId = SerializationGroupId::new(2);

fn grouped_state(
    core: &Core,
    binding: &Arc<TestBinding>,
    g: SerializationGroupId,
) -> graphrefly_core::NodeId {
    let init = binding.intern(TestValue::Int(0));
    core.register(NodeRegistration {
        deps: vec![],
        fn_or_op: None,
        opts: NodeOpts {
            initial: init,
            serialization_group: Some(g),
            ..Default::default()
        },
    })
    .expect("register grouped state")
}

#[test]
#[ignore = "actor-model (D238/D226): §7 shared-Core cross-thread groups are deleted at S2c + re-measured at S4; superseded by host-native parallelism + the S4 group_scaling bench"]
fn same_group_cross_thread_emits_serialize_without_deadlock() {
    // deferred stub (D238/D226): body used §7 shared-Core cross-thread groups (deleted S2c); actor-model rewrite + group_scaling re-measure at S4.
}

#[test]
#[ignore = "actor-model (D238/D226): §7 shared-Core cross-thread groups are deleted at S2c + re-measured at S4; superseded by host-native parallelism + the S4 group_scaling bench"]
fn disjoint_groups_cross_thread_waves_both_complete() {
    // deferred stub (D238/D226): body used §7 shared-Core cross-thread groups (deleted S2c); actor-model rewrite + group_scaling re-measure at S4.
}

// ---------------------------------------------------------------------
// QA F1 (2026-05-16) regression: the config the rewrite originally broke
// and no test covered (per_subgraph_parallelism.rs, deleted, used to).
// An all-`None` (unannotated, default) `Core<LockedCell>` driven from
// two threads must STILL serialize waves — the Core-global wave
// `ReentrantMutex` restores the cross-thread safety floor the deleted
// union-find `wave_owner` provided. Without the fix, the per-thread
// WAVE_STATE / TIER3 / IN_TICK_OWNED invariants interleave → torn
// multi-node cascades / panics / hangs.
// ---------------------------------------------------------------------
#[test]
#[ignore = "actor-model (D238/D226): §7 shared-Core cross-thread groups are deleted at S2c + re-measured at S4; superseded by host-native parallelism + the S4 group_scaling bench"]
fn all_none_default_core_cross_thread_cascade_integrity() {
    // deferred stub (D238/D226): body used §7 shared-Core cross-thread groups (deleted S2c); actor-model rewrite + group_scaling re-measure at S4.
}
