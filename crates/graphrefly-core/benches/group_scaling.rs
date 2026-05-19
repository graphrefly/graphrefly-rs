//! §7 parallel-scaling bench — **superseded by the actor model (D246
//! Blind #2)**.
//!
//! The original instrument drove N threads on ONE shared
//! `Core<LockedCell>` cloned across threads, with disjoint
//! `SerializationGroupId`s, to ask "do disjoint groups run in
//! parallel?". That premise is exactly the cross-thread-shared-`Core`
//! model the actor / work-stealing rewrite **deletes**: D221/D246 make
//! `Core` move-only `!Sync` (`Core::clone` removed), D246 rule 3
//! removes the core RAII `Subscription` (→ `SubscriptionId` +
//! owner-invoked `core.unsubscribe`), S2c deletes the §7 grouping
//! machinery, and S3 renames `SerializationGroupId` →
//! `SchedulingGroupId` with a user-partition-key semantics change.
//! Rebuilding the real instrument — disjoint parallelism measured with
//! **independent per-worker `OwnedCore`s** (no shared Core, no clone) —
//! is the S4 deliverable, not boundary-1 work. This mirrors the sibling
//! `benches/per_subgraph_parallelism.rs` and the
//! `examples/profile_disjoint_*` migrations (all gutted to
//! compile-only superseded notes for the same invalid premise).
//!
//! Gutted to a no-op criterion target so `cargo clippy --all-targets` /
//! the gate compiles; do NOT resurrect the cloned-shared-`Core` shape.
//! See `~/src/graphrefly-rs/docs/porting-deferred.md` § "D246
//! record-and-skip" and the S4 plan.

use criterion::{criterion_group, criterion_main, Criterion};

fn group_scaling(c: &mut Criterion) {
    // Single trivial sample so criterion has a measurable target without
    // touching the deleted shared-Core model. The real disjoint-vs-
    // serialized scaling instrument is rebuilt at S4 with independent
    // per-worker Cores (actor model).
    c.bench_function("group_scaling/superseded_see_S4", |b| {
        b.iter(|| std::hint::black_box(0u64));
    });
}

criterion_group!(benches, group_scaling);
criterion_main!(benches);
