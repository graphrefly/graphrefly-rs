//! §7 floor-comparison bench — **SUPERSEDED by D246/S2c (D248/D249)**.
//!
//! This bench's entire premise was a side-by-side of the two
//! `StateCell` monomorphizations (`Core<SingleThreadCell>` vs
//! `Core<LockedCell>`). D246/S2c **collapsed the `StateCell` generic**:
//! there is now exactly one (non-generic, `RefCell`-backed,
//! single-owner `!Send + !Sync`) `Core`, and the cross-thread
//! wave-lock / `SERIALIZE_WAVES` machinery is deleted. The
//! "floor vs default" comparison no longer has two arms — the
//! single-owner `Core` *is* the floor.
//!
//! Per the boundary-1 precedent (`migration-status.md`: the §7
//! deleted-model bench artifacts — `group_scaling` etc. — were gutted
//! to superseded notes and **rebuilt at S4**), this file is reduced to
//! a compiling placeholder. The honest single-owner dispatcher numbers
//! live in `benches/dispatcher.rs`; the actor-model re-measure
//! (independent per-worker `Core`s) is rebuilt with
//! `benches/group_scaling.rs` at **S4** (migration-status S4).
//! Tracked: `docs/porting-deferred.md` § "D246 record-and-skip".

use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn floor(c: &mut Criterion) {
    // Minimal placeholder so the registered `[[bench]]` harness keeps
    // compiling. Real dispatcher numbers: `benches/dispatcher.rs`.
    c.bench_function("floor_compare_superseded_placeholder", |b| {
        b.iter(|| black_box(1u64 + black_box(1)));
    });
}

criterion_group!(benches, floor);
criterion_main!(benches);
