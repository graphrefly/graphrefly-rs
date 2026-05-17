//! Profile target (D217-AMEND, 2026-05-16) — attribute the §7 floor gap.
//!
//! The real shipped `Core<SingleThreadCell>` identity-dedup hot path
//! (`floor_compare.rs::floor/identity_dedup/SingleThreadCell` ≈ 627 ns)
//! vs the lean `minimal_handle_core` mirror (≈ 83 ns). D217's lever
//! ranking ("slab store is the biggest lever") was FALSIFIED — the
//! mirror uses the same `HashMap` node store (slower `std` SipHash) and
//! is 7.6× faster anyway. This binary exists to **empirically attribute**
//! the 627→83 ns gap by symbol, so Slice A's lever set is re-locked from
//! data, not assumption.
//!
//! Single-threaded, no parallelism: a tight loop emitting the SAME
//! handle on one state node with a noop sink — the zero-FFI
//! equals-substitution dedup path. Whatever `sample`/`samply` shows
//! eating cycles here IS the §7 tax (binding-side work is a noop
//! `release_handle`; there is no `invoke_fn`/`custom_equals` on the
//! dedup path beyond the equals check).
//!
//! Build + profile (repo convention, see `[profile.profiling]`):
//!   cargo build --profile profiling --example profile_st_emit -p graphrefly-core
//!   # macOS textual call-tree (parseable attribution):
//!   ./target/profiling/examples/profile_st_emit 60000000 &
//!   sample $! 8 -file /tmp/st_emit.sample.txt
//!   # or samply (Firefox-profiler JSON):
//!   samply record --save-only -o /tmp/st_emit.json.gz -- \
//!     ./target/profiling/examples/profile_st_emit 60000000
//!
//! Arg 1 = emit count (default 60_000_000 ≈ ~38 s at 627 ns — ample
//! sampling window).

use std::sync::Arc;

use graphrefly_core::{
    BindingBoundary, Core, DepBatch, FnId, FnResult, HandleId, Message, NodeId, SingleThreadCell,
    Sink,
};

/// Minimal binding — shape-matched to `floor_compare.rs::BenchBinding`
/// (so the profile attributes the same code path the bench measures).
/// `Send + Sync` is required by the `BindingBoundary` trait; it never
/// crosses a thread here.
struct ProfBinding;

impl BindingBoundary for ProfBinding {
    fn invoke_fn(&self, _: NodeId, _: FnId, _: &[DepBatch]) -> FnResult {
        // Unreached on the state-node dedup path (no fn). Present for
        // trait completeness.
        FnResult::Data {
            handle: HandleId::new(1),
            tracked: None,
        }
    }
    fn custom_equals(&self, _: FnId, a: HandleId, b: HandleId) -> bool {
        a == b
    }
    fn release_handle(&self, _: HandleId) {}
}

fn noop_sink() -> Sink {
    Arc::new(|_: &[Message]| {})
}

fn main() {
    let emits: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(60_000_000);

    let binding: Arc<dyn BindingBoundary> = Arc::new(ProfBinding);
    // The real shipped §7 floor path — NOT the minimal_handle_core mirror.
    let core = Core::<SingleThreadCell>::new_with_cell(binding);
    let s = core.register_state(HandleId::new(1), false).unwrap();
    // RAII: keep the subscription alive for the whole run (drop at end).
    let _sub = core.subscribe(s, noop_sink());
    let same = HandleId::new(1);

    let start = std::time::Instant::now();
    for _ in 0..emits {
        core.emit(std::hint::black_box(s), std::hint::black_box(same));
    }
    let dt = start.elapsed();

    // Printed AFTER the loop so it doesn't pollute the sampled region;
    // also a sanity check that the profiled path matches floor_compare.
    eprintln!(
        "profile_st_emit: {emits} emits in {dt:?} = {:.1} ns/emit (real Core<SingleThreadCell> dedup)",
        dt.as_nanos() as f64 / emits as f64
    );
    drop(_sub);
}
