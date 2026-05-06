//! Slice A-bigger /qa item D — `impl Drop for CoreState` refcount balance.
//!
//! Production bindings (napi-rs, pyo3, wasm-bindgen) maintain handle-ref
//! maps. Without `Drop` cleanup, every retained handle slot — `cache`,
//! `terminal` Error, `dep_terminals` Error, pause-buffer payload — would
//! leak refs until process exit. These tests confirm `live_handles()` on
//! the binding goes to zero (or the expected baseline) after the last
//! `Core` clone drops.

mod common;

use common::{TestRuntime, TestValue};

#[test]
fn drop_releases_state_node_caches() {
    let binding = {
        let rt = TestRuntime::new();
        let _s1 = rt.state(Some(TestValue::Int(7)));
        let _s2 = rt.state(Some(TestValue::Int(11)));
        let _s3 = rt.state(Some(TestValue::Str("hello".into())));
        // Non-zero baseline: 3 state caches retained.
        assert!(rt.binding.live_handles() >= 3);
        rt.binding.clone()
    };
    // After rt drops (and with it Core + the inner CoreState), every
    // retained cache slot should be released.
    assert_eq!(
        binding.live_handles(),
        0,
        "drop should release all state caches; live_handles still {}",
        binding.live_handles()
    );
}

#[test]
fn drop_releases_terminal_error_handles() {
    let binding = {
        let rt = TestRuntime::new();
        let s = rt.state(Some(TestValue::Int(0)));
        let derived = rt.derived(&[s.id], |deps| match &deps[0] {
            TestValue::Int(n) => Some(TestValue::Int(*n)),
            _ => None,
        });
        // Force activation so derived has a cache.
        let _rec = rt.subscribe_recorder(derived);
        // Error the source; the error handle is retained on s.terminal AND
        // on derived.dep_terminals[0] AND s.cache stays. Cascade also marks
        // derived terminal with the same error — third retain.
        let err = rt.binding.intern(TestValue::Str("oops".into()));
        rt.core.error(s.id, err);
        // Live handles: s.cache (1), s.terminal err (1), derived.cache (1),
        // derived.dep_terminals[0] err (1), derived.terminal err (1).
        // Plus the err handle from intern transferred to s.terminal — same
        // handle, refcount accumulates.
        assert!(rt.binding.live_handles() > 0);
        rt.binding.clone()
    };
    assert_eq!(
        binding.live_handles(),
        0,
        "drop should release every terminal-Error retain; live_handles still {}",
        binding.live_handles()
    );
}

#[test]
fn drop_releases_pause_buffer_payloads() {
    let binding = {
        let rt = TestRuntime::new();
        let s = rt.state(Some(TestValue::Int(0)));
        let derived = rt.derived(&[s.id], |deps| match &deps[0] {
            TestValue::Int(n) => Some(TestValue::Int(*n)),
            _ => None,
        });
        let _rec = rt.subscribe_recorder(derived);
        // Pause the derived; emits to source push tier-3 messages into
        // its pause buffer (each retain'd).
        let lock = rt.core.alloc_lock_id();
        rt.core.pause(derived, lock).expect("pause");
        for v in 1..=5 {
            let h = rt.binding.intern(TestValue::Int(v));
            rt.core.emit(s.id, h);
        }
        // Buffer should hold 5 retained payloads (one per emit, all reach
        // derived since each value is distinct). Don't resume — let drop
        // handle the cleanup.
        assert!(rt.binding.live_handles() > 0);
        rt.binding.clone()
    };
    assert_eq!(
        binding.live_handles(),
        0,
        "drop should release pause-buffer payloads; live_handles still {}",
        binding.live_handles()
    );
}

#[test]
fn drop_balanced_after_normal_wave() {
    let binding = {
        let rt = TestRuntime::new();
        let s = rt.state(Some(TestValue::Int(0)));
        let m = rt.derived(&[s.id], |deps| match &deps[0] {
            TestValue::Int(n) => Some(TestValue::Int(n * 2)),
            _ => None,
        });
        let leaf = rt.derived(&[m], |deps| match &deps[0] {
            TestValue::Int(n) => Some(TestValue::Int(n + 1)),
            _ => None,
        });
        let _rec = rt.subscribe_recorder(leaf);
        // Push a few waves through.
        for v in 1..=10 {
            let h = rt.binding.intern(TestValue::Int(v));
            rt.core.emit(s.id, h);
        }
        rt.binding.clone()
    };
    assert_eq!(
        binding.live_handles(),
        0,
        "after a normal wave run + drop, refcounts should balance; got {}",
        binding.live_handles()
    );
}
