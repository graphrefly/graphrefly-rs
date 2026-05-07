//! Loom model-check for the [`Subscription::Drop`] race
//! ([porting-deferred.md D5]; D042).
//!
//! D5 hypothesised: "if two threads race to drop the last two
//! subscriptions, both could see count=1 before decrement, leading to
//! double-deactivate or missed deactivate."
//!
//! The current production impl in [`crates/graphrefly-core/src/node.rs`]
//! `impl Drop for Subscription` removes the sub AND checks "was last
//! one?" under the same `parking_lot::Mutex<CoreState>` acquisition,
//! so there is no TOCTOU window — but verifying with loom permutes
//! thread interleavings exhaustively to catch any future regression
//! (e.g., someone splitting the remove from the check).
//!
//! Per D042 (option B), this test models a focused minimal shape with
//! `loom::sync::Mutex` directly, rather than cfg-gating the production
//! Mutex/Arc primitives across the dispatcher. That keeps the loom
//! integration's blast radius minimal.
//!
//! Run via: `RUSTFLAGS="--cfg loom" cargo test --test loom_subscription`
//! (or just `cargo test --test loom_subscription` — the test body is
//! `#[cfg(loom)]`-gated so it compiles to nothing without the cfg).

#![cfg(loom)]
#![forbid(unsafe_code)]

use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::sync::{Arc, Mutex};
use std::collections::HashMap;

/// Mirrors the relevant slice of `Subscription::Drop`:
/// 1. Remove this sub from `subscribers` under the state lock.
/// 2. Check `subscribers.is_empty()` under the SAME lock.
/// 3. If empty AND node is "producer-like", fire `producer_deactivate`
///    AFTER releasing the lock.
///
/// The `producer_deactivate` count is incremented on every fire; the
/// invariant is that across all loom-permuted interleavings of the two
/// drops, the count is exactly 1.
struct SubRegistry {
    subscribers: HashMap<u64, ()>,
}

/// Modeled as strong-Arc (not Weak) because the race we're verifying
/// happens while the registry is alive. The production
/// `Weak<Mutex<CoreState>>` shape protects against "Core dropped first"
/// — a separate concern from the unsubscribe race; modeling it here
/// would not exercise additional interleavings of the decrement +
/// is-empty check.
#[derive(Clone)]
struct Sub {
    state: Arc<Mutex<SubRegistry>>,
    deactivate_count: Arc<AtomicUsize>,
    is_producer: bool,
    sub_id: u64,
}

impl Sub {
    fn unsubscribe(&self) {
        let was_last = {
            let mut s = self.state.lock().unwrap();
            s.subscribers.remove(&self.sub_id);
            s.subscribers.is_empty()
        };
        if was_last && self.is_producer {
            self.deactivate_count.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[test]
fn concurrent_drop_of_last_two_subs_fires_deactivate_exactly_once() {
    loom::model(|| {
        let registry = Arc::new(Mutex::new(SubRegistry {
            subscribers: {
                let mut m = HashMap::new();
                m.insert(1u64, ());
                m.insert(2u64, ());
                m
            },
        }));
        let count = Arc::new(AtomicUsize::new(0));

        let sub_a = Sub {
            state: registry.clone(),
            deactivate_count: count.clone(),
            is_producer: true,
            sub_id: 1,
        };
        let sub_b = Sub {
            state: registry,
            deactivate_count: count.clone(),
            is_producer: true,
            sub_id: 2,
        };

        let h1 = loom::thread::spawn(move || sub_a.unsubscribe());
        let h2 = loom::thread::spawn(move || sub_b.unsubscribe());
        h1.join().unwrap();
        h2.join().unwrap();

        let final_count = count.load(Ordering::SeqCst);
        assert_eq!(
            final_count, 1,
            "producer_deactivate must fire exactly once across all interleavings of the last two unsubscribes; got {final_count}"
        );
    });
}

#[test]
fn concurrent_drop_with_one_remaining_sub_does_not_fire_deactivate() {
    loom::model(|| {
        let registry = Arc::new(Mutex::new(SubRegistry {
            subscribers: {
                let mut m = HashMap::new();
                m.insert(1u64, ());
                m.insert(2u64, ());
                m.insert(3u64, ()); // 3 subs; dropping 2 leaves 1 behind
                m
            },
        }));
        let count = Arc::new(AtomicUsize::new(0));

        let sub_a = Sub {
            state: registry.clone(),
            deactivate_count: count.clone(),
            is_producer: true,
            sub_id: 1,
        };
        let sub_b = Sub {
            state: registry,
            deactivate_count: count.clone(),
            is_producer: true,
            sub_id: 2,
        };

        let h1 = loom::thread::spawn(move || sub_a.unsubscribe());
        let h2 = loom::thread::spawn(move || sub_b.unsubscribe());
        h1.join().unwrap();
        h2.join().unwrap();

        // Sub 3 still alive — no deactivate should fire.
        let final_count = count.load(Ordering::SeqCst);
        assert_eq!(
            final_count, 0,
            "producer_deactivate must NOT fire while subs remain; got {final_count}"
        );
    });
}

#[test]
fn drop_of_last_sub_on_non_producer_node_does_not_deactivate() {
    loom::model(|| {
        let registry = Arc::new(Mutex::new(SubRegistry {
            subscribers: {
                let mut m = HashMap::new();
                m.insert(1u64, ());
                m
            },
        }));
        let count = Arc::new(AtomicUsize::new(0));

        let sub = Sub {
            state: registry,
            deactivate_count: count.clone(),
            is_producer: false, // non-producer; deactivate hook does NOT apply
            sub_id: 1,
        };

        let h = loom::thread::spawn(move || sub.unsubscribe());
        h.join().unwrap();

        let final_count = count.load(Ordering::SeqCst);
        assert_eq!(
            final_count, 0,
            "producer_deactivate must not fire for non-producer nodes; got {final_count}"
        );
    });
}
