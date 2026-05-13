//! Buffer operators — collect and batch reactive values.
//!
//! # Operators
//!
//! - [`buffer`] — notifier-triggered flush of accumulated DATA handles.
//! - [`buffer_count`] — fixed-count flush.
//! - [`window`] — notifier-triggered sub-node splitting.
//! - [`window_count`] — count-based sub-node splitting.

use std::sync::{Arc, Weak};

use parking_lot::Mutex;

use graphrefly_core::{BindingBoundary, Core, FnId, HandleId, NodeId, Sink};
use smallvec::SmallVec;

use crate::producer::{ProducerBinding, ProducerBuildFn, ProducerCtx, SubscribeOutcome};

// =========================================================================
// buffer(source, notifier) — notifier-triggered flush
// =========================================================================

/// Per-buffer-node shared state.
struct BufferState {
    buf: Vec<HandleId>,
    terminated: bool,
    source_completed: bool,
}

impl BufferState {
    fn new() -> Self {
        Self {
            buf: Vec::new(),
            terminated: false,
            source_completed: false,
        }
    }
}

/// Buffers source DATA handles; flushes as a packed array when notifier
/// emits DATA.
///
/// - Source DATA: retain + push to buffer.
/// - Notifier DATA: pack buffer via `pack_tuple`, emit, clear, release
///   component handles.
/// - Source COMPLETE: flush remaining buffer (if non-empty), then complete.
/// - Either ERROR: terminate immediately, release buffered handles.
/// - Notifier COMPLETE: does NOT auto-complete (keep buffering; source
///   COMPLETE triggers final flush).
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn buffer(
    core: &Core,
    binding: &Arc<dyn ProducerBinding>,
    source: NodeId,
    notifier: NodeId,
    pack_fn_id: FnId,
) -> NodeId {
    let core_weak = core.weak_handle();
    let binding_weak: Weak<dyn ProducerBinding> = Arc::downgrade(binding);

    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let (Some(core_s), Some(binding_s)) = (core_weak.upgrade(), binding_weak.upgrade()) else {
            return;
        };
        let pid = ctx.node_id();
        let state: Arc<Mutex<BufferState>> = Arc::new(Mutex::new(BufferState::new()));

        // --- source sink ---
        let st = state.clone();
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();
        let core_src = core_s.clone();
        let source_sink: Sink = Arc::new(move |msgs| {
            enum Act {
                Flush(Vec<HandleId>),
                Complete,
                Error(HandleId),
                Release(Vec<HandleId>),
            }
            let mut actions: SmallVec<[Act; 2]> = SmallVec::new();
            {
                let mut s = st.lock();
                if s.terminated {
                    return;
                }
                for m in msgs {
                    if s.terminated {
                        break;
                    }
                    match m.tier() {
                        3 => {
                            if let Some(h) = m.payload_handle() {
                                bb.retain_handle(h);
                                s.buf.push(h);
                            }
                        }
                        5 => {
                            if let Some(h) = m.payload_handle() {
                                // ERROR — terminate, release buffered
                                s.terminated = true;
                                bb.retain_handle(h);
                                let to_release: Vec<HandleId> = s.buf.drain(..).collect();
                                actions.push(Act::Release(to_release));
                                actions.push(Act::Error(h));
                            } else {
                                // COMPLETE — flush remainder, then complete
                                s.source_completed = true;
                                s.terminated = true;
                                if !s.buf.is_empty() {
                                    let flushed: Vec<HandleId> = s.buf.drain(..).collect();
                                    actions.push(Act::Flush(flushed));
                                }
                                actions.push(Act::Complete);
                            }
                        }
                        _ => {}
                    }
                }
            }
            for a in actions {
                match a {
                    Act::Flush(handles) => {
                        let packed = bb.pack_tuple(pack_fn_id, &handles);
                        for h in &handles {
                            bb.release_handle(*h);
                        }
                        core_src.emit_or_defer(pid, packed);
                    }
                    Act::Complete => core_src.complete_or_defer(pid),
                    Act::Error(h) => core_src.error_or_defer(pid, h),
                    Act::Release(handles) => {
                        for h in handles {
                            bb.release_handle(h);
                        }
                    }
                }
            }
        });

        let src_outcome = ctx.subscribe_to(source, source_sink);
        if matches!(src_outcome, SubscribeOutcome::Dead { .. }) {
            let mut s = state.lock();
            if !s.terminated {
                s.terminated = true;
                s.source_completed = true;
                // No buffered data yet at activation time.
                drop(s);
                core_s.complete_or_defer(pid);
                return;
            }
        }

        // --- notifier sink ---
        let st2 = state.clone();
        let core_n = core_s.clone();
        let bb2: Arc<dyn BindingBoundary> = binding_s.clone();
        let notifier_sink: Sink = Arc::new(move |msgs| {
            enum Act {
                Flush(Vec<HandleId>),
                Error(HandleId),
                Release(Vec<HandleId>),
            }
            let mut actions: SmallVec<[Act; 2]> = SmallVec::new();
            {
                let mut s = st2.lock();
                if s.terminated {
                    return;
                }
                for m in msgs {
                    if s.terminated {
                        break;
                    }
                    match m.tier() {
                        3 if m.payload_handle().is_some() && !s.buf.is_empty() => {
                            let flushed: Vec<HandleId> = s.buf.drain(..).collect();
                            actions.push(Act::Flush(flushed));
                        }
                        5 => {
                            if let Some(h) = m.payload_handle() {
                                // Notifier ERROR — terminate, release buffered
                                s.terminated = true;
                                bb2.retain_handle(h);
                                let to_release: Vec<HandleId> = s.buf.drain(..).collect();
                                actions.push(Act::Release(to_release));
                                actions.push(Act::Error(h));
                            }
                            // Notifier COMPLETE: do NOT auto-complete.
                        }
                        _ => {}
                    }
                }
            }
            for a in actions {
                match a {
                    Act::Flush(handles) => {
                        let packed = bb2.pack_tuple(pack_fn_id, &handles);
                        for h in &handles {
                            bb2.release_handle(*h);
                        }
                        core_n.emit_or_defer(pid, packed);
                    }
                    Act::Error(h) => core_n.error_or_defer(pid, h),
                    Act::Release(handles) => {
                        for h in handles {
                            bb2.release_handle(h);
                        }
                    }
                }
            }
        });

        let not_outcome = ctx.subscribe_to(notifier, notifier_sink);
        // Dead notifier: ignore — source COMPLETE will handle final flush.
        let _ = not_outcome;
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
        .expect("buffer: register_producer failed")
}

// =========================================================================
// buffer_count(source, count) — fixed-size buffer
// =========================================================================

/// Per-buffer_count-node shared state.
struct BufferCountState {
    buf: Vec<HandleId>,
    terminated: bool,
}

impl BufferCountState {
    fn new() -> Self {
        Self {
            buf: Vec::new(),
            terminated: false,
        }
    }
}

/// Fixed-size buffer. Accumulate DATA handles; emit packed array every
/// `count` items.
///
/// - Source DATA: retain + push. When `buf.len() == count`, pack + emit + clear.
/// - Source COMPLETE: flush remainder (may be < count), then complete.
/// - Source ERROR: terminate, release buffered.
///
/// # Panics
///
/// Panics if `count` is 0.
#[must_use]
pub fn buffer_count(
    core: &Core,
    binding: &Arc<dyn ProducerBinding>,
    source: NodeId,
    count: usize,
    pack_fn_id: FnId,
) -> NodeId {
    assert!(count > 0, "buffer_count: count must be > 0");

    let core_weak = core.weak_handle();
    let binding_weak: Weak<dyn ProducerBinding> = Arc::downgrade(binding);

    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let (Some(core_s), Some(binding_s)) = (core_weak.upgrade(), binding_weak.upgrade()) else {
            return;
        };
        let pid = ctx.node_id();
        let state: Arc<Mutex<BufferCountState>> = Arc::new(Mutex::new(BufferCountState::new()));

        let st = state.clone();
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();
        let core_src = core_s.clone();
        let source_sink: Sink = Arc::new(move |msgs| {
            enum Act {
                Flush(Vec<HandleId>),
                Complete,
                Error(HandleId),
                Release(Vec<HandleId>),
            }
            let mut actions: SmallVec<[Act; 4]> = SmallVec::new();
            {
                let mut s = st.lock();
                if s.terminated {
                    return;
                }
                for m in msgs {
                    if s.terminated {
                        break;
                    }
                    match m.tier() {
                        3 => {
                            if let Some(h) = m.payload_handle() {
                                bb.retain_handle(h);
                                s.buf.push(h);
                                if s.buf.len() == count {
                                    let flushed: Vec<HandleId> = s.buf.drain(..).collect();
                                    actions.push(Act::Flush(flushed));
                                }
                            }
                        }
                        5 => {
                            if let Some(h) = m.payload_handle() {
                                // ERROR
                                s.terminated = true;
                                bb.retain_handle(h);
                                let to_release: Vec<HandleId> = s.buf.drain(..).collect();
                                actions.push(Act::Release(to_release));
                                actions.push(Act::Error(h));
                            } else {
                                // COMPLETE — flush remainder
                                s.terminated = true;
                                if !s.buf.is_empty() {
                                    let flushed: Vec<HandleId> = s.buf.drain(..).collect();
                                    actions.push(Act::Flush(flushed));
                                }
                                actions.push(Act::Complete);
                            }
                        }
                        _ => {}
                    }
                }
            }
            for a in actions {
                match a {
                    Act::Flush(handles) => {
                        let packed = bb.pack_tuple(pack_fn_id, &handles);
                        for h in &handles {
                            bb.release_handle(*h);
                        }
                        core_src.emit_or_defer(pid, packed);
                    }
                    Act::Complete => core_src.complete_or_defer(pid),
                    Act::Error(h) => core_src.error_or_defer(pid, h),
                    Act::Release(handles) => {
                        for h in handles {
                            bb.release_handle(h);
                        }
                    }
                }
            }
        });

        let outcome = ctx.subscribe_to(source, source_sink);
        if matches!(outcome, SubscribeOutcome::Dead { .. }) {
            let mut s = state.lock();
            if !s.terminated {
                s.terminated = true;
                drop(s);
                core_s.complete_or_defer(pid);
            }
        }
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
        .expect("buffer_count: register_producer failed")
}

// =========================================================================
// window(source, notifier) — notifier-triggered sub-node splitting
// =========================================================================

/// Per-window-node shared state.
struct WindowState {
    /// The current inner window's NodeId.
    inner_id: Option<NodeId>,
    terminated: bool,
}

impl WindowState {
    fn new() -> Self {
        Self {
            inner_id: None,
            terminated: false,
        }
    }
}

/// Splits source into sub-nodes triggered by notifier. Each "window" is
/// a fresh state node created via `Core::register_state()`. The operator
/// emits the inner node's `NodeId` as a handle via
/// `binding.intern_node(inner_id)`.
///
/// - On activation: create first inner window node, emit it.
/// - Source DATA: forward to current inner window via
///   `core.emit_or_defer(inner_id, handle)`.
/// - Notifier DATA: complete current window, create new window, emit it.
/// - Source COMPLETE: complete current window, then complete self.
/// - Either ERROR: error current window + error self.
/// - Notifier COMPLETE: do NOT auto-complete.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn window(
    core: &Core,
    binding: &Arc<dyn ProducerBinding>,
    source: NodeId,
    notifier: NodeId,
) -> NodeId {
    let core_weak = core.weak_handle();
    let binding_weak: Weak<dyn ProducerBinding> = Arc::downgrade(binding);

    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let (Some(core_s), Some(binding_s)) = (core_weak.upgrade(), binding_weak.upgrade()) else {
            return;
        };
        let pid = ctx.node_id();
        let state: Arc<Mutex<WindowState>> = Arc::new(Mutex::new(WindowState::new()));
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();

        // Create first inner window node and emit it.
        let first_inner = create_window_node(&core_s, &*bb);
        {
            let mut s = state.lock();
            s.inner_id = Some(first_inner.0);
        }
        core_s.emit_or_defer(pid, first_inner.1);

        // --- source sink ---
        let st = state.clone();
        let bb_src: Arc<dyn BindingBoundary> = binding_s.clone();
        let core_src = core_s.clone();
        let source_sink: Sink = Arc::new(move |msgs| {
            enum Act {
                Forward(NodeId, HandleId),
                CompleteInner(NodeId),
                CompleteSelf,
                ErrorInner(NodeId, HandleId),
                ErrorSelf(HandleId),
            }
            let mut actions: SmallVec<[Act; 4]> = SmallVec::new();
            {
                let mut s = st.lock();
                if s.terminated {
                    return;
                }
                for m in msgs {
                    if s.terminated {
                        break;
                    }
                    match m.tier() {
                        3 => {
                            if let Some(h) = m.payload_handle() {
                                bb_src.retain_handle(h);
                                if let Some(inner) = s.inner_id {
                                    actions.push(Act::Forward(inner, h));
                                } else {
                                    bb_src.release_handle(h);
                                }
                            }
                        }
                        5 => {
                            if let Some(h) = m.payload_handle() {
                                // ERROR — error current window + error self
                                s.terminated = true;
                                bb_src.retain_handle(h);
                                if let Some(inner) = s.inner_id.take() {
                                    // Retain again for inner error
                                    bb_src.retain_handle(h);
                                    actions.push(Act::ErrorInner(inner, h));
                                }
                                actions.push(Act::ErrorSelf(h));
                            } else {
                                // COMPLETE — complete current window, then self
                                s.terminated = true;
                                if let Some(inner) = s.inner_id.take() {
                                    actions.push(Act::CompleteInner(inner));
                                }
                                actions.push(Act::CompleteSelf);
                            }
                        }
                        _ => {}
                    }
                }
            }
            for a in actions {
                match a {
                    Act::Forward(inner, h) => core_src.emit_or_defer(inner, h),
                    Act::CompleteInner(inner) => core_src.complete_or_defer(inner),
                    Act::CompleteSelf => core_src.complete_or_defer(pid),
                    Act::ErrorInner(inner, h) => core_src.error_or_defer(inner, h),
                    Act::ErrorSelf(h) => core_src.error_or_defer(pid, h),
                }
            }
        });

        let src_outcome = ctx.subscribe_to(source, source_sink);
        if matches!(src_outcome, SubscribeOutcome::Dead { .. }) {
            let mut s = state.lock();
            if !s.terminated {
                s.terminated = true;
                let inner = s.inner_id.take();
                drop(s);
                if let Some(inner_id) = inner {
                    core_s.complete_or_defer(inner_id);
                }
                core_s.complete_or_defer(pid);
                return;
            }
        }

        // --- notifier sink ---
        let st2 = state.clone();
        let core_n = core_s.clone();
        let bb_not: Arc<dyn BindingBoundary> = binding_s.clone();
        let notifier_sink: Sink = Arc::new(move |msgs| {
            enum Act {
                CompleteOldEmitNew(NodeId, HandleId),
                ErrorInner(NodeId, HandleId),
                ErrorSelf(HandleId),
            }
            let mut actions: SmallVec<[Act; 2]> = SmallVec::new();
            {
                let mut s = st2.lock();
                if s.terminated {
                    return;
                }
                for m in msgs {
                    if s.terminated {
                        break;
                    }
                    match m.tier() {
                        3 if m.payload_handle().is_some() => {
                            // Complete current window, create new one, emit it.
                            let old_inner = s.inner_id.take();
                            let (new_id, new_handle) = create_window_node(&core_n, &*bb_not);
                            s.inner_id = Some(new_id);
                            if let Some(old) = old_inner {
                                actions.push(Act::CompleteOldEmitNew(old, new_handle));
                            } else {
                                // Degenerate: inner_id was None (unreachable if
                                // not terminated). Emit the new window directly.
                                actions.push(Act::CompleteOldEmitNew(new_id, new_handle));
                            }
                        }
                        5 => {
                            if let Some(h) = m.payload_handle() {
                                // Notifier ERROR — error current window + error self
                                s.terminated = true;
                                bb_not.retain_handle(h);
                                if let Some(inner) = s.inner_id.take() {
                                    bb_not.retain_handle(h);
                                    actions.push(Act::ErrorInner(inner, h));
                                }
                                actions.push(Act::ErrorSelf(h));
                            }
                            // Notifier COMPLETE: do NOT auto-complete.
                        }
                        _ => {}
                    }
                }
            }
            for a in actions {
                match a {
                    Act::CompleteOldEmitNew(old, new_handle) => {
                        core_n.complete_or_defer(old);
                        core_n.emit_or_defer(pid, new_handle);
                    }
                    Act::ErrorInner(inner, h) => core_n.error_or_defer(inner, h),
                    Act::ErrorSelf(h) => core_n.error_or_defer(pid, h),
                }
            }
        });

        let _ = ctx.subscribe_to(notifier, notifier_sink);
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
        .expect("window: register_producer failed")
}

/// Create a new inner window state node and return `(NodeId, HandleId)`.
/// The `HandleId` represents the inner node as a value (via `intern_node`).
fn create_window_node(core: &Core, binding: &dyn BindingBoundary) -> (NodeId, HandleId) {
    let inner_id = core
        .register_state(graphrefly_core::NO_HANDLE, false)
        .expect("window: register_state for inner node failed");
    let handle = binding.intern_node(inner_id);
    (inner_id, handle)
}

// =========================================================================
// window_count(source, count) — count-based sub-node splitting
// =========================================================================

/// Per-window_count-node shared state.
struct WindowCountState {
    /// The current inner window's NodeId.
    inner_id: Option<NodeId>,
    /// Items forwarded to the current window.
    counter: usize,
    terminated: bool,
}

impl WindowCountState {
    fn new() -> Self {
        Self {
            inner_id: None,
            counter: 0,
            terminated: false,
        }
    }
}

/// Like [`window`] but closes window every `count` DATA items.
///
/// - On activation: create first window, emit it.
/// - Source DATA: forward to current window + increment counter. When
///   counter hits `count`, complete current window, create new one, emit
///   it, reset counter.
/// - Source COMPLETE: complete current window (even if < count items),
///   complete self.
/// - Source ERROR: error current window, error self.
///
/// # Panics
///
/// Panics if `count` is 0.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn window_count(
    core: &Core,
    binding: &Arc<dyn ProducerBinding>,
    source: NodeId,
    count: usize,
) -> NodeId {
    assert!(count > 0, "window_count: count must be > 0");

    let core_weak = core.weak_handle();
    let binding_weak: Weak<dyn ProducerBinding> = Arc::downgrade(binding);

    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let (Some(core_s), Some(binding_s)) = (core_weak.upgrade(), binding_weak.upgrade()) else {
            return;
        };
        let pid = ctx.node_id();
        let state: Arc<Mutex<WindowCountState>> = Arc::new(Mutex::new(WindowCountState::new()));
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();

        // Create first inner window and emit it.
        let first_inner = create_window_node(&core_s, &*bb);
        {
            let mut s = state.lock();
            s.inner_id = Some(first_inner.0);
            s.counter = 0;
        }
        core_s.emit_or_defer(pid, first_inner.1);

        // --- source sink ---
        let st = state.clone();
        let bb_src: Arc<dyn BindingBoundary> = binding_s.clone();
        let core_src = core_s.clone();
        let source_sink: Sink = Arc::new(move |msgs| {
            enum Act {
                Forward(NodeId, HandleId),
                CompleteWindowEmitNew(NodeId, HandleId),
                CompleteInner(NodeId),
                CompleteSelf,
                ErrorInner(NodeId, HandleId),
                ErrorSelf(HandleId),
            }
            let mut actions: SmallVec<[Act; 4]> = SmallVec::new();
            {
                let mut s = st.lock();
                if s.terminated {
                    return;
                }
                for m in msgs {
                    if s.terminated {
                        break;
                    }
                    match m.tier() {
                        3 => {
                            if let Some(h) = m.payload_handle() {
                                bb_src.retain_handle(h);
                                if let Some(inner) = s.inner_id {
                                    actions.push(Act::Forward(inner, h));
                                    s.counter += 1;
                                    if s.counter == count {
                                        // Window full — complete it, open new one.
                                        let (new_id, new_handle) =
                                            create_window_node(&core_src, &*bb_src);
                                        actions.push(Act::CompleteWindowEmitNew(inner, new_handle));
                                        s.inner_id = Some(new_id);
                                        s.counter = 0;
                                    }
                                } else {
                                    bb_src.release_handle(h);
                                }
                            }
                        }
                        5 => {
                            if let Some(h) = m.payload_handle() {
                                // ERROR — error current window + self
                                s.terminated = true;
                                bb_src.retain_handle(h);
                                if let Some(inner) = s.inner_id.take() {
                                    bb_src.retain_handle(h);
                                    actions.push(Act::ErrorInner(inner, h));
                                }
                                actions.push(Act::ErrorSelf(h));
                            } else {
                                // COMPLETE
                                s.terminated = true;
                                if let Some(inner) = s.inner_id.take() {
                                    actions.push(Act::CompleteInner(inner));
                                }
                                actions.push(Act::CompleteSelf);
                            }
                        }
                        _ => {}
                    }
                }
            }
            for a in actions {
                match a {
                    Act::Forward(inner, h) => core_src.emit_or_defer(inner, h),
                    Act::CompleteWindowEmitNew(old_inner, new_handle) => {
                        core_src.complete_or_defer(old_inner);
                        core_src.emit_or_defer(pid, new_handle);
                    }
                    Act::CompleteInner(inner) => core_src.complete_or_defer(inner),
                    Act::CompleteSelf => core_src.complete_or_defer(pid),
                    Act::ErrorInner(inner, h) => core_src.error_or_defer(inner, h),
                    Act::ErrorSelf(h) => core_src.error_or_defer(pid, h),
                }
            }
        });

        let outcome = ctx.subscribe_to(source, source_sink);
        if matches!(outcome, SubscribeOutcome::Dead { .. }) {
            let mut s = state.lock();
            if !s.terminated {
                s.terminated = true;
                let inner = s.inner_id.take();
                drop(s);
                if let Some(inner_id) = inner {
                    core_s.complete_or_defer(inner_id);
                }
                core_s.complete_or_defer(pid);
            }
        }
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
        .expect("window_count: register_producer failed")
}
