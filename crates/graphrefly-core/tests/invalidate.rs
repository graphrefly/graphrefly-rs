//! INVALIDATE — cache clear + downstream cascade.
//!
//! Maps to canonical spec §1.4 (INVALIDATE delivery is idempotent within a
//! wave; never-populated case is a no-op) and the TLA+ scenario MC
//! `handle_invalidate_MC` from `~/src/graphrefly-ts/docs/research/`.

mod common;
use common::{RecordedEvent, TestRuntime, TestValue};

#[test]
fn invalidate_clears_cache_and_emits_invalidate() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(42)));
    let rec = rt.subscribe_recorder(s.id);
    let baseline = rec.snapshot().len();

    rt.core().invalidate(s.id);

    assert_eq!(rt.cache_value(s.id), None, "cache cleared to sentinel");
    let snap = rec.snapshot();
    let post: Vec<&RecordedEvent> = snap[baseline..].iter().collect();
    assert!(
        post.iter().any(|e| matches!(e, RecordedEvent::Invalidate)),
        "subscriber sees [INVALIDATE]; got: {post:?}"
    );
}

#[test]
fn invalidate_on_never_populated_state_is_noop() {
    let rt = TestRuntime::new();
    let s = rt.state(None);
    let rec = rt.subscribe_recorder(s.id);
    let baseline = rec.snapshot().len();

    rt.core().invalidate(s.id);

    assert_eq!(rt.cache_value(s.id), None);
    let snap = rec.snapshot();
    let post: Vec<&RecordedEvent> = snap[baseline..].iter().collect();
    assert_eq!(post.len(), 0, "no INVALIDATE for never-populated node");
}

#[test]
fn invalidate_is_idempotent_within_wave() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(1)));
    let rec = rt.subscribe_recorder(s.id);
    let baseline = rec.snapshot().len();

    rt.core().invalidate(s.id);
    rt.core().invalidate(s.id); // second call — cache already NO_HANDLE
    rt.core().invalidate(s.id); // third call — same

    let invalidate_count = rec
        .snapshot()
        .iter()
        .skip(baseline)
        .filter(|e| matches!(e, RecordedEvent::Invalidate))
        .count();
    assert_eq!(
        invalidate_count, 1,
        "only the first invalidate emits INVALIDATE; subsequent are no-ops"
    );
}

#[test]
fn invalidate_cascades_to_derived_dependents() {
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(10)));
    let b = rt.derived(&[a.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(n * 2)),
        _ => panic!("type"),
    });
    let rec_b = rt.subscribe_recorder(b);
    assert_eq!(rt.cache_value(b), Some(TestValue::Int(20)));
    let baseline_b = rec_b.snapshot().len();

    rt.core().invalidate(a.id);

    // A's cache cleared.
    assert_eq!(rt.cache_value(a.id), None);
    // B's cache also cleared via cascade (its dep was invalidated).
    assert_eq!(rt.cache_value(b), None);
    // B's subscriber sees INVALIDATE.
    let b_snap = rec_b.snapshot();
    let b_post: Vec<&RecordedEvent> = b_snap[baseline_b..].iter().collect();
    assert!(b_post
        .iter()
        .any(|e| matches!(e, RecordedEvent::Invalidate)));
}

#[test]
fn re_emit_after_invalidate_repopulates_cache() {
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(10)));
    let b = rt.derived(&[a.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(n * 2)),
        _ => panic!("type"),
    });
    let _rec = rt.subscribe_recorder(b);

    rt.core().invalidate(a.id);
    assert_eq!(rt.cache_value(a.id), None);
    assert_eq!(rt.cache_value(b), None);

    a.set(TestValue::Int(7));
    assert_eq!(rt.cache_value(a.id), Some(TestValue::Int(7)));
    // b's first-run gate reopened by re-emission, fn fires.
    assert_eq!(rt.cache_value(b), Some(TestValue::Int(14)));
}

#[test]
fn invalidate_on_diamond_root_cascades_to_sink_once_per_path() {
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(1)));
    let b = rt.derived(&[a.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(n + 100)),
        _ => panic!("type"),
    });
    let c = rt.derived(&[a.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(n + 200)),
        _ => panic!("type"),
    });
    let d = rt.derived(&[b, c], |deps| match (&deps[0], &deps[1]) {
        (TestValue::Int(bv), TestValue::Int(cv)) => Some(TestValue::Int(bv + cv)),
        _ => panic!("type"),
    });
    let rec_d = rt.subscribe_recorder(d);
    assert_eq!(rt.cache_value(d), Some(TestValue::Int(302))); // (1+100)+(1+200)
    let baseline = rec_d.snapshot().len();

    rt.core().invalidate(a.id);
    assert_eq!(rt.cache_value(d), None, "diamond sink invalidated");

    // The cascade visits D twice (once via B, once via C). The second visit
    // is a no-op because D's cache is already NO_HANDLE — INVALIDATE
    // delivery is idempotent within the wave per spec §1.4.
    let invalidate_count = rec_d
        .snapshot()
        .iter()
        .skip(baseline)
        .filter(|e| matches!(e, RecordedEvent::Invalidate))
        .count();
    assert_eq!(invalidate_count, 1);
}

#[test]
fn invalidate_on_unsubscribed_compute_with_no_cache_is_noop() {
    let rt = TestRuntime::new();
    let a = rt.state(None); // sentinel
    let b = rt.derived(&[a.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(*n)),
        _ => panic!("type"),
    });
    // Not subscribed — b never fires; cache stays NO_HANDLE.
    rt.core().invalidate(b);
    // No panic, no cascade visible.
    assert_eq!(rt.cache_value(b), None);
}

#[test]
fn invalidate_releases_handle_via_refcount() {
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(7)));
    let _rec = rt.subscribe_recorder(s.id);
    let live_before = rt.binding.live_handles();

    rt.core().invalidate(s.id);
    let live_after = rt.binding.live_handles();
    // The handle for `7` is no longer in the cache; its registry entry
    // (refcount=1 from intern) drops to 0 → evicted.
    assert!(
        live_after < live_before,
        "invalidate should release the cached handle; live_handles {live_before} → {live_after}"
    );
}

#[test]
fn invalidate_buffers_through_pause() {
    // R2.6.0 (canonical §2.6 "Option A", pinned 2026-05-17): a Default-mode
    // leaf source's direct external messages — tier-3 AND tier-4 — flush
    // immediately while self-paused (gating is fn/dep-pipeline-scoped only).
    // The tier-4 buffer-then-replay machinery this test exercises is the
    // `ResumeAll` contract — opt in.
    let rt = TestRuntime::new();
    let s = rt.state(Some(TestValue::Int(0)));
    rt.core()
        .set_pausable_mode(s.id, graphrefly_core::PausableMode::ResumeAll)
        .unwrap();
    let rec = rt.subscribe_recorder(s.id);
    let baseline = rec.snapshot().len();

    let lock = rt.core().alloc_lock_id();
    rt.core().pause(s.id, lock).expect("pause");
    rt.core().invalidate(s.id);

    // Cache cleared immediately (it's bookkeeping); INVALIDATE wire-message
    // is buffered (tier 4).
    assert_eq!(rt.cache_value(s.id), None);
    let mid_invalidate = rec
        .snapshot()
        .iter()
        .skip(baseline)
        .filter(|e| matches!(e, RecordedEvent::Invalidate))
        .count();
    assert_eq!(
        mid_invalidate, 0,
        "INVALIDATE should buffer alongside DATA while paused"
    );

    let report = rt
        .core()
        .resume(s.id, lock)
        .expect("resume")
        .expect("final");
    assert_eq!(report.replayed, 1);

    let post_invalidate = rec
        .snapshot()
        .iter()
        .skip(baseline)
        .filter(|e| matches!(e, RecordedEvent::Invalidate))
        .count();
    assert_eq!(post_invalidate, 1);
}

#[test]
fn invalidated_dep_closes_first_run_gate_until_re_emit() {
    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(10)));
    let b = rt.state(Some(TestValue::Int(20)));
    let calls = std::sync::Arc::new(std::sync::Mutex::new(0u32));
    let calls_inner = calls.clone();
    let sum = rt.derived(&[a.id, b.id], move |deps| {
        *calls_inner.lock().unwrap() += 1;
        match (&deps[0], &deps[1]) {
            (TestValue::Int(av), TestValue::Int(bv)) => Some(TestValue::Int(av + bv)),
            _ => panic!("type"),
        }
    });
    let _rec = rt.subscribe_recorder(sum);
    assert_eq!(rt.cache_value(sum), Some(TestValue::Int(30)));
    let initial_calls = *calls.lock().unwrap();

    rt.core().invalidate(a.id);
    // sum's dep_handles[idx_a] = NO_HANDLE → first-run gate closed.
    // b emits — gate still closed (a still NO_HANDLE).
    b.set(TestValue::Int(99));
    assert_eq!(
        *calls.lock().unwrap(),
        initial_calls,
        "fn must not fire while a's slot is NO_HANDLE post-invalidate"
    );

    // a re-emits — gate releases.
    a.set(TestValue::Int(1));
    assert!(*calls.lock().unwrap() > initial_calls);
    assert_eq!(rt.cache_value(sum), Some(TestValue::Int(100))); // 1 + 99
}

#[test]
#[should_panic(expected = "unknown node")]
fn invalidate_unknown_node_panics() {
    let rt = TestRuntime::new();
    let bogus = graphrefly_core::NodeId::new(99_999);
    rt.core().invalidate(bogus);
}

// D281 (2026-05-22, E-i.1) — `Core::up(node, Invalidate)` is R1.4.2
// plain-forward: it must NOT self-process at intermediate/source nodes
// (no `_emit`, no cache clear at source), and the cache-clearing
// semantic applies downstream-side only via the separate
// `Core::invalidate(node_id)` entry. Pre-D281 the arm called
// `self.invalidate(dep_id)` which is the downstream-cascade entry —
// it cleared the dep's cache + cascaded INVALIDATE to the dep's
// children. Cross-arm verified: TS pure-ts `Node.up()` was already
// plain-forward (recursive walk to leaves, no self-process); Rust was
// the divergent arm. The fix mirrors TS by recursively calling
// `Core::up(dep_id, Message::Invalidate)` — the recursion bottoms out
// at leaf sources where `dep_ids_vec()` is empty.
#[test]
fn r1_4_2_up_invalidate_does_not_clear_dep_cache_or_cascade() {
    use graphrefly_core::Message;

    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(10)));
    let b = rt.derived(&[a.id], |deps| match &deps[0] {
        TestValue::Int(n) => Some(TestValue::Int(n * 2)),
        _ => panic!("type"),
    });
    let rec_a = rt.subscribe_recorder(a.id);
    let rec_b = rt.subscribe_recorder(b);
    assert_eq!(rt.cache_value(a.id), Some(TestValue::Int(10)));
    assert_eq!(rt.cache_value(b), Some(TestValue::Int(20)));
    let baseline_a = rec_a.snapshot().len();
    let baseline_b = rec_b.snapshot().len();

    // Up-direction INVALIDATE from `b` to its dep `a`. Per R1.4.2 plain-
    // forward: no self-process at `a`, no cache clear at `a`, no
    // downstream cascade back to `b`.
    rt.core().up(b, Message::Invalidate).expect("known node");

    // Caches unchanged — no self-process, no cache clear at the source
    // (or at the intermediate that was originally targeted).
    assert_eq!(
        rt.cache_value(a.id),
        Some(TestValue::Int(10)),
        "R1.4.2: up-direction INVALIDATE must NOT clear dep `a`'s cache"
    );
    assert_eq!(
        rt.cache_value(b),
        Some(TestValue::Int(20)),
        "R1.4.2: no downstream cascade — `b`'s cache stays settled"
    );

    // Neither subscriber sees an INVALIDATE message.
    let snap_a = rec_a.snapshot();
    let snap_b = rec_b.snapshot();
    let a_post: Vec<&RecordedEvent> = snap_a[baseline_a..].iter().collect();
    let b_post: Vec<&RecordedEvent> = snap_b[baseline_b..].iter().collect();
    assert!(
        !a_post
            .iter()
            .any(|e| matches!(e, RecordedEvent::Invalidate)),
        "R1.4.2: `a` subscriber must NOT see INVALIDATE (no self-process). got: {a_post:?}"
    );
    assert!(
        !b_post
            .iter()
            .any(|e| matches!(e, RecordedEvent::Invalidate)),
        "R1.4.2: `b` subscriber must NOT see INVALIDATE (no downstream cascade). got: {b_post:?}"
    );
}

// D281 (E-i.1) — `Core::up(leaf, Invalidate)` at a leaf source (no deps)
// is a true no-op. The recursion bottoms out at the leaf where
// `dep_ids_vec()` is empty; cache stays populated, no events delivered.
// Mirrors TS leaf-source no-op at `node.ts:1334` (`if (this._deps.length
// === 0) return;`).
#[test]
fn r1_4_2_up_invalidate_at_leaf_source_is_noop() {
    use graphrefly_core::Message;

    let rt = TestRuntime::new();
    let a = rt.state(Some(TestValue::Int(42)));
    let rec_a = rt.subscribe_recorder(a.id);
    let baseline = rec_a.snapshot().len();

    // `a` is a leaf source — no deps. `up(a, Invalidate)` recurses into
    // an empty dep list and bottoms out without effect.
    rt.core().up(a.id, Message::Invalidate).expect("known node");

    assert_eq!(
        rt.cache_value(a.id),
        Some(TestValue::Int(42)),
        "R1.4.2: leaf-source up(Invalidate) leaves cache untouched"
    );
    let snap = rec_a.snapshot();
    let post: Vec<&RecordedEvent> = snap[baseline..].iter().collect();
    assert_eq!(
        post.len(),
        0,
        "R1.4.2: leaf-source up(Invalidate) emits nothing. got: {post:?}"
    );
}
