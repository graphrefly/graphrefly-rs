//! Buffer operators — collect and batch reactive values.
//!
//! # Operators
//!
//! - [`buffer`] — notifier-triggered flush of accumulated DATA handles.
//! - [`buffer_count`] — fixed-count flush.
//! - [`window`] — notifier-triggered sub-node splitting.
//! - [`window_count`] — count-based sub-node splitting.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

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
    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let core_s = ctx.core();
        let binding_s = ctx.core().binding();
        let em = ctx.emitter();
        let pid = ctx.node_id();
        let state: Rc<RefCell<BufferState>> = Rc::new(RefCell::new(BufferState::new()));

        // --- source sink ---
        let st = state.clone();
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();
        let core_src = em.clone();
        let source_sink: Sink = Rc::new(move |msgs| {
            enum Act {
                Flush(Vec<HandleId>),
                Complete,
                Error(HandleId),
                Release(Vec<HandleId>),
            }
            let mut actions: SmallVec<[Act; 2]> = SmallVec::new();
            {
                let mut s = st.borrow_mut();
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
                        core_src.emit(pid, packed);
                    }
                    Act::Complete => core_src.complete(pid),
                    Act::Error(h) => core_src.error(pid, h),
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
            let mut s = state.borrow_mut();
            if !s.terminated {
                s.terminated = true;
                s.source_completed = true;
                // No buffered data yet at activation time.
                drop(s);
                core_s.complete(pid);
                return;
            }
        }

        // --- notifier sink ---
        let st2 = state.clone();
        let core_n = em.clone();
        let bb2: Arc<dyn BindingBoundary> = binding_s.clone();
        let notifier_sink: Sink = Rc::new(move |msgs| {
            enum Act {
                Flush(Vec<HandleId>),
                Error(HandleId),
                Release(Vec<HandleId>),
            }
            let mut actions: SmallVec<[Act; 2]> = SmallVec::new();
            {
                let mut s = st2.borrow_mut();
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
                        core_n.emit(pid, packed);
                    }
                    Act::Error(h) => core_n.error(pid, h),
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

    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let core_s = ctx.core();
        let binding_s = ctx.core().binding();
        let em = ctx.emitter();
        let pid = ctx.node_id();
        let state: Rc<RefCell<BufferCountState>> = Rc::new(RefCell::new(BufferCountState::new()));

        let st = state.clone();
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();
        let core_src = em.clone();
        let source_sink: Sink = Rc::new(move |msgs| {
            enum Act {
                Flush(Vec<HandleId>),
                Complete,
                Error(HandleId),
                Release(Vec<HandleId>),
            }
            let mut actions: SmallVec<[Act; 4]> = SmallVec::new();
            {
                let mut s = st.borrow_mut();
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
                        core_src.emit(pid, packed);
                    }
                    Act::Complete => core_src.complete(pid),
                    Act::Error(h) => core_src.error(pid, h),
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
            let mut s = state.borrow_mut();
            if !s.terminated {
                s.terminated = true;
                drop(s);
                core_s.complete(pid);
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
///   `core.emit(inner_id, handle)`.
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
    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let core_s = ctx.core();
        let binding_s = ctx.core().binding();
        let em = ctx.emitter();
        let pid = ctx.node_id();
        let state: Rc<RefCell<WindowState>> = Rc::new(RefCell::new(WindowState::new()));
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();

        // Create first inner window node and emit it.
        let first_inner = create_window_node(core_s, &*bb);
        {
            let mut s = state.borrow_mut();
            s.inner_id = Some(first_inner.0);
        }
        core_s.emit(pid, first_inner.1);

        // --- source sink ---
        // D234: the inner-window selector `s.inner_id` MUST be read
        // INSIDE the owner-serialized defer closures (never at sink-fire
        // time) so a source-DATA firing between a notifier-DATA and its
        // window-roll defer still routes to the correct window. Mailbox
        // FIFO = arrival order ⇒ a roll posted before a forward is
        // applied first, so the in-closure `inner_id` read sees the new
        // window. `terminated` stays a fire-time monotonic gate; retains
        // happen at fire time (Core owns the share for the emit) and are
        // released if `em.defer` reports the Core gone (F2 contract).
        let st = state.clone();
        let bb_src: Arc<dyn BindingBoundary> = binding_s.clone();
        let em_src = em.clone();
        let source_sink: Sink = Rc::new(move |msgs| {
            for m in msgs {
                // QA P1: per-message terminal gate (restores the retired
                // `if s.terminated { break }`) — a batched
                // `[COMPLETE, DATA]` must NOT forward DATA after the
                // COMPLETE (R2.6.4 / no-data-after-terminal).
                if st.borrow_mut().terminated {
                    break;
                }
                match m.tier() {
                    3 => {
                        if let Some(h) = m.payload_handle() {
                            bb_src.retain_handle(h);
                            let st_c = st.clone();
                            let bb_c = bb_src.clone();
                            if !em_src.defer(move |c| {
                                let inner = st_c.borrow_mut().inner_id;
                                match inner {
                                    Some(i) => c.emit(i, h),
                                    None => bb_c.release_handle(h),
                                }
                            }) {
                                bb_src.release_handle(h);
                            }
                        }
                    }
                    5 => {
                        // QA P1: atomic terminal-winner — only the first
                        // terminal (across THIS sink and the notifier
                        // sink) defers the terminal cascade. Without
                        // this, source-COMPLETE and notifier-ERROR each
                        // pass their fire-time check before either defer
                        // runs → double terminal on `pid`.
                        let was = std::mem::replace(&mut st.borrow_mut().terminated, true);
                        if was {
                            break;
                        }
                        if let Some(h) = m.payload_handle() {
                            // ERROR — error current window + error self.
                            bb_src.retain_handle(h);
                            let st_c = st.clone();
                            let bb_c = bb_src.clone();
                            if !em_src.defer(move |c| {
                                if let Some(i) = st_c.borrow_mut().inner_id.take() {
                                    bb_c.retain_handle(h);
                                    c.error(i, h);
                                }
                                c.error(pid, h);
                            }) {
                                bb_src.release_handle(h);
                            }
                        } else {
                            // COMPLETE — complete current window, then self.
                            let st_c = st.clone();
                            let _ = em_src.defer(move |c| {
                                if let Some(i) = st_c.borrow_mut().inner_id.take() {
                                    c.complete(i);
                                }
                                c.complete(pid);
                            });
                        }
                        break;
                    }
                    _ => {}
                }
            }
        });

        let src_outcome = ctx.subscribe_to(source, source_sink);
        if matches!(src_outcome, SubscribeOutcome::Dead { .. }) {
            let mut s = state.borrow_mut();
            if !s.terminated {
                s.terminated = true;
                let inner = s.inner_id.take();
                drop(s);
                if let Some(inner_id) = inner {
                    core_s.complete(inner_id);
                }
                core_s.complete(pid);
                return;
            }
        }

        // --- notifier sink ---
        // D234: the window roll (complete old → create new → swap
        // `inner_id` → emit new) is ONE owner-side defer closure, so it
        // is atomic w.r.t. the FIFO drain and `create_window_node` (a
        // `CoreFull::register_state`) runs in-wave. A roll posted before
        // a later source-forward defer is applied first ⇒ that forward's
        // in-closure `inner_id` read sees the new window. The `new_id`
        // is consumed entirely inside the closure (drives captured
        // state); a Core-gone `defer` drops it unrun with nothing
        // created/retained yet (no leak — that is why the create lives
        // in the closure, not before it).
        let st2 = state.clone();
        let em_not = em.clone();
        let bb_not: Arc<dyn BindingBoundary> = binding_s.clone();
        let notifier_sink: Sink = Rc::new(move |msgs| {
            for m in msgs {
                // QA P1: per-message terminal gate.
                if st2.borrow_mut().terminated {
                    break;
                }
                match m.tier() {
                    3 if m.payload_handle().is_some() => {
                        let st_c = st2.clone();
                        let bb_c = bb_not.clone();
                        let _ = em_not.defer(move |c| {
                            let (new_id, new_handle) = create_window_node(c, &*bb_c);
                            let old_inner = st_c.borrow_mut().inner_id.replace(new_id);
                            // Degenerate `None` (unreachable when not
                            // terminated): there is no prior window to
                            // complete; just emit the new one.
                            if let Some(old) = old_inner {
                                c.complete(old);
                            }
                            c.emit(pid, new_handle);
                        });
                    }
                    5 => {
                        if let Some(h) = m.payload_handle() {
                            // Notifier ERROR — error current window + self.
                            // QA P1: atomic terminal-winner vs the source
                            // sink's COMPLETE/ERROR (no double-terminal).
                            let was = std::mem::replace(&mut st2.borrow_mut().terminated, true);
                            if was {
                                break;
                            }
                            bb_not.retain_handle(h);
                            let st_c = st2.clone();
                            let bb_c = bb_not.clone();
                            if !em_not.defer(move |c| {
                                if let Some(inner) = st_c.borrow_mut().inner_id.take() {
                                    bb_c.retain_handle(h);
                                    c.error(inner, h);
                                }
                                c.error(pid, h);
                            }) {
                                bb_not.release_handle(h);
                            }
                            break;
                        }
                        // Notifier COMPLETE: do NOT auto-complete.
                    }
                    _ => {}
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
fn create_window_node(
    core: &dyn graphrefly_core::CoreFull,
    binding: &dyn BindingBoundary,
) -> (NodeId, HandleId) {
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

    let build: ProducerBuildFn = Box::new(move |ctx: ProducerCtx<'_>| {
        let core_s = ctx.core();
        let binding_s = ctx.core().binding();
        let em = ctx.emitter();
        let pid = ctx.node_id();
        let state: Rc<RefCell<WindowCountState>> = Rc::new(RefCell::new(WindowCountState::new()));
        let bb: Arc<dyn BindingBoundary> = binding_s.clone();

        // Create first inner window and emit it.
        let first_inner = create_window_node(core_s, &*bb);
        {
            let mut s = state.borrow_mut();
            s.inner_id = Some(first_inner.0);
            s.counter = 0;
        }
        core_s.emit(pid, first_inner.1);

        // --- source sink ---
        // D234: forward + count + window-roll are ONE owner-side defer
        // per source-DATA. Defer closures run serially in FIFO drain
        // order, so the counter increment, the threshold check, the
        // `create_window_node` (a `CoreFull::register_state`), the
        // `inner_id` swap, the old-window complete and the new-window
        // emit are all consistent — equivalent to the old
        // synchronous-under-lock path, just relocated owner-side. The
        // operator `Mutex` is dropped before every `CoreFull` call
        // (no operator-lock held across Core re-entry); it is safe to
        // re-lock because no other defer runs concurrently (serial drain).
        let st = state.clone();
        let bb_src: Arc<dyn BindingBoundary> = binding_s.clone();
        let em_src = em.clone();
        let source_sink: Sink = Rc::new(move |msgs| {
            for m in msgs {
                // QA P1: per-message terminal gate.
                if st.borrow_mut().terminated {
                    break;
                }
                match m.tier() {
                    3 => {
                        if let Some(h) = m.payload_handle() {
                            bb_src.retain_handle(h);
                            let st_c = st.clone();
                            let bb_c = bb_src.clone();
                            if !em_src.defer(move |c| {
                                let (inner, roll) = {
                                    let mut s = st_c.borrow_mut();
                                    match s.inner_id {
                                        Some(inner) => {
                                            s.counter += 1;
                                            let roll = s.counter == count;
                                            (Some(inner), roll)
                                        }
                                        None => (None, false),
                                    }
                                };
                                let Some(inner) = inner else {
                                    bb_c.release_handle(h);
                                    return;
                                };
                                c.emit(inner, h);
                                if roll {
                                    let (new_id, new_handle) = create_window_node(c, &*bb_c);
                                    {
                                        let mut s = st_c.borrow_mut();
                                        s.inner_id = Some(new_id);
                                        s.counter = 0;
                                    }
                                    c.complete(inner);
                                    c.emit(pid, new_handle);
                                }
                            }) {
                                bb_src.release_handle(h);
                            }
                        }
                    }
                    5 => {
                        // QA P1: atomic terminal-winner.
                        let was = std::mem::replace(&mut st.borrow_mut().terminated, true);
                        if was {
                            break;
                        }
                        if let Some(h) = m.payload_handle() {
                            // ERROR — error current window + self.
                            bb_src.retain_handle(h);
                            let st_c = st.clone();
                            let bb_c = bb_src.clone();
                            if !em_src.defer(move |c| {
                                if let Some(inner) = st_c.borrow_mut().inner_id.take() {
                                    bb_c.retain_handle(h);
                                    c.error(inner, h);
                                }
                                c.error(pid, h);
                            }) {
                                bb_src.release_handle(h);
                            }
                        } else {
                            // COMPLETE.
                            let st_c = st.clone();
                            let _ = em_src.defer(move |c| {
                                if let Some(inner) = st_c.borrow_mut().inner_id.take() {
                                    c.complete(inner);
                                }
                                c.complete(pid);
                            });
                        }
                        break;
                    }
                    _ => {}
                }
            }
        });

        let outcome = ctx.subscribe_to(source, source_sink);
        if matches!(outcome, SubscribeOutcome::Dead { .. }) {
            let mut s = state.borrow_mut();
            if !s.terminated {
                s.terminated = true;
                let inner = s.inner_id.take();
                drop(s);
                if let Some(inner_id) = inner {
                    core_s.complete(inner_id);
                }
                core_s.complete(pid);
            }
        }
    });

    let fn_id = binding.register_producer_build(build);
    core.register_producer(fn_id)
        .expect("window_count: register_producer failed")
}
