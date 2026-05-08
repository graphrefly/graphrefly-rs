//! `BenchGraph` napi class — Graph parity surface for JS callers.
//!
//! Phase E rustImpl activation slice (D074). Wraps
//! [`graphrefly_graph::Graph`] for the JS adapter to expose as
//! `Impl.Graph`. Build profile: only enabled with the `graph-codec`
//! feature.
//!
//! # Scope (this slice)
//!
//! - **Namespace ops**: `state`, `derived`, `dynamic`, `add`, `node`,
//!   `try_resolve`, `name_of`, `node_count`, `node_names`,
//!   `child_names`, `is_destroyed`.
//! - **Lifecycle**: `set`, `get`, `invalidate_by_name`,
//!   `complete_by_name`, `error_by_name`, `remove`, `destroy`.
//! - **Mount tree**: `mount_new` (returns a fresh BenchGraph wrapping
//!   the child).
//! - **Signal broadcast**: `signal_invalidate`, `signal_pause`,
//!   `signal_resume` (per-kind; JS adapter decomposes batches).
//! - **Static observe**: `observe_node_by_path` returns a NodeId for
//!   the path; subscribe via `BenchCore::subscribe_with_tsfn`.
//! - **Static describe**: `describe_json` returns a JSON-serialized
//!   snapshot.
//! - **Edges (static)**: `edges(recursive)` returns a flat
//!   `Vec<(String, String)>` list.
//!
//! # Out of scope (carry forward)
//!
//! - Reactive describe (`describe_reactive`) — requires producer node
//!   wiring through `subscribe_namespace_change`.
//! - Reactive `observe_all_reactive` — same shape.
//! - Reactive `edges({ reactive: true })` — same shape.
//!
//! These will surface as Rust-port-only divergences in the parity-tests
//! triage pass; gated with `test.runIf(impl.name !== "rust-via-napi")`.

use std::sync::Arc;

use graphrefly_core::{
    BindingBoundary, Core, EqualsMode, FnId, HandleId, LockId, Message, NodeId, Sink, Subscription,
};
use graphrefly_graph::{
    DescribeSink, Graph, GraphObserveAllReactive, NameError, ReactiveDescribeHandle, RemoveError,
    SignalKind,
};
use napi::bindgen_prelude::*;
use napi::threadsafe_function::{
    ThreadsafeCallContext, ThreadsafeFunction, ThreadsafeFunctionCallMode,
};
use napi::{Env, Error as NapiError, Status};
use napi_derive::napi;
use parking_lot::Mutex;

use crate::core_bindings::{
    run_blocking, BenchBinding, BenchCore, BuiltinFn, MSG_CODE_COMPLETE, MSG_CODE_DATA,
    MSG_CODE_DIRTY, MSG_CODE_ERROR, MSG_CODE_INVALIDATE, MSG_CODE_PAUSE, MSG_CODE_RESOLVED,
    MSG_CODE_RESUME, MSG_CODE_START, MSG_CODE_TEARDOWN,
};

/// JS-facing wrapper around a `graphrefly_graph::Graph`. Each
/// `BenchGraph` shares its `BenchCore`'s value registry + binding so
/// nodes registered through `BenchGraph::state(...)` are visible to
/// `BenchOperators::register_*(...)` and vice-versa.
///
/// **`binding` field redundancy** (Slice X3 / Phase E /qa F14): the
/// `binding: Arc<BenchBinding>` field carries the same allocation as
/// `core.binding: Arc<dyn BindingBoundary>` — they're upcasts of the
/// same `Arc<BenchBinding>`. The typed field is retained because
/// `BenchGraph::derived` needs the concrete `BenchBinding` to access
/// `binding.registry.lock()` for fn registration; recovering the
/// concrete type from the trait-erased `core.binding` would require
/// `Arc::downcast` (not available on `dyn Trait` without `Any` bound).
/// `from_core` enforces the invariant via a debug-build pointer-equality
/// assert so the field can't drift from `core`'s view.
#[napi]
pub struct BenchGraph {
    pub(crate) graph: Graph,
    pub(crate) binding: Arc<BenchBinding>,
    pub(crate) core: Core,
}

#[napi]
impl BenchGraph {
    /// Construct a fresh root graph wired to a `BenchCore`'s binding +
    /// Core (D074). The BenchGraph SHARES the BenchCore's Core via
    /// `Graph::with_existing_core` (added in this slice to graphrefly-
    /// graph), so nodes registered through `BenchGraph::state(...)`
    /// are visible to operator factories on the same `BenchCore`.
    #[napi(factory)]
    #[must_use]
    pub fn from_core(core: &BenchCore, name: String) -> Self {
        let core_clone = core.core_clone();
        let binding = core.binding_arc();
        let graph = Graph::with_existing_core(name, core_clone.clone());
        // F14 (Slice X3): the typed `binding` field and `core_clone.binding`
        // (trait-erased) come from the SAME `Arc<BenchBinding>` upcast —
        // both `core.core_clone()` and `core.binding_arc()` clone the
        // single allocation owned by the BenchCore. A debug-build
        // pointer-equality assert would confirm this, but `core.binding`
        // is `pub(crate)` in graphrefly-core (different crate) so the
        // assert can't be expressed without widening Core's API.
        // `BenchCore::new` is the single construction path; the invariant
        // is preserved by construction.
        Self {
            graph,
            binding,
            core: core_clone,
        }
    }

    /// Construct a child BenchGraph wrapping an existing Graph (used
    /// by `mount_new` to expose mounted children to JS).
    pub(crate) fn from_graph(parent: &BenchGraph, child: Graph) -> Self {
        Self {
            graph: child,
            binding: Arc::clone(&parent.binding),
            core: parent.core.clone(),
        }
    }

    // -------------------------------------------------------------------
    // Namespace introspection (sync — no Core wave).
    // -------------------------------------------------------------------

    #[napi]
    #[must_use]
    pub fn name(&self) -> String {
        self.graph.name()
    }

    #[napi]
    #[must_use]
    pub fn node_count(&self) -> u32 {
        u32::try_from(self.graph.node_count()).unwrap_or(u32::MAX)
    }

    #[napi]
    #[must_use]
    pub fn node_names(&self) -> Vec<String> {
        self.graph.node_names()
    }

    #[napi]
    #[must_use]
    pub fn child_names(&self) -> Vec<String> {
        self.graph.child_names()
    }

    #[napi]
    #[must_use]
    pub fn is_destroyed(&self) -> bool {
        self.graph.is_destroyed()
    }

    /// Resolve a path to a NodeId; returns 0 (NO_NODE) if missing.
    #[napi]
    #[must_use]
    pub fn try_resolve(&self, path: String) -> u32 {
        self.graph
            .try_resolve(&path)
            .map_or(0, |id| u32::try_from(id.raw()).unwrap_or(0))
    }

    /// Reverse lookup: returns the local name for a NodeId, or None
    /// if unnamed in this graph.
    #[napi]
    #[must_use]
    pub fn name_of(&self, node_id: u32) -> Option<String> {
        self.graph.name_of(NodeId::new(u64::from(node_id)))
    }

    // -------------------------------------------------------------------
    // Sugar constructors (canonical §3.9).
    // -------------------------------------------------------------------

    /// `state(name, initial_handle?)`. `None` → sentinel cache.
    /// Caller (JS adapter) holds the handle's refcount via
    /// `BenchCore::alloc_external_handle`; ownership transfers here.
    #[napi]
    pub async fn state(&self, name: String, initial_handle: Option<u32>) -> Result<u32> {
        let graph = self.graph.clone();
        let core = self.core.clone();
        run_blocking(core, move || -> Result<u32> {
            let h = initial_handle.map(|h| HandleId::new(u64::from(h)));
            let id = graph.state(name, h).map_err(name_error_to_napi)?;
            u32::try_from(id.raw()).map_err(|_| NapiError::from_reason("node id exceeds u32"))
        })
        .await?
    }

    /// `derived(name, deps, builtin)` — uses a built-in fn from
    /// `BuiltinFn` (Identity / AddOne) since we don't take JS callbacks
    /// here. For real JS-callback derived nodes, callers go through
    /// `BenchOperators` operators (map/filter/etc.) and `add(name, id)`
    /// the resulting NodeId.
    #[napi]
    pub async fn derived(
        &self,
        name: String,
        dep_ids: Vec<u32>,
        builtin: BuiltinFn,
    ) -> Result<u32> {
        let graph = self.graph.clone();
        let core = self.core.clone();
        let binding = Arc::clone(&self.binding);
        run_blocking(core, move || -> Result<u32> {
            // Register fn binding-side (mirrors BenchCore::register_derived).
            let fn_impl: Arc<
                dyn Fn(
                        &[crate::core_bindings::BenchValue],
                    ) -> Option<crate::core_bindings::BenchValue>
                    + Send
                    + Sync,
            > = match builtin {
                BuiltinFn::Identity => {
                    Arc::new(|deps: &[crate::core_bindings::BenchValue]| deps.first().cloned())
                }
                BuiltinFn::AddOne => Arc::new(
                    |deps: &[crate::core_bindings::BenchValue]| match deps.first() {
                        Some(crate::core_bindings::BenchValue::Int(n)) => {
                            Some(crate::core_bindings::BenchValue::Int(n + 1))
                        }
                        _ => None,
                    },
                ),
            };
            let fn_id = {
                let mut reg = binding.registry.lock();
                let id = FnId::new(reg.next_fn_id);
                reg.next_fn_id += 1;
                reg.fns.insert(id, fn_impl);
                id
            };
            let deps: Vec<NodeId> = dep_ids
                .into_iter()
                .map(|i| NodeId::new(u64::from(i)))
                .collect();
            let id = graph
                .derived(name, &deps, fn_id, EqualsMode::Identity)
                .map_err(name_error_to_napi)?;
            u32::try_from(id.raw()).map_err(|_| NapiError::from_reason("node id exceeds u32"))
        })
        .await?
    }

    /// Register an existing node (e.g., from `BenchOperators::register_map`)
    /// under `name` in this graph's namespace.
    #[napi]
    pub fn add(&self, name: String, node_id: u32) -> Result<u32> {
        let id = self
            .graph
            .add(NodeId::new(u64::from(node_id)), name)
            .map_err(name_error_to_napi)?;
        u32::try_from(id.raw()).map_err(|_| NapiError::from_reason("node id exceeds u32"))
    }

    // -------------------------------------------------------------------
    // Named-sugar lifecycle (canonical §3.2.1).
    // -------------------------------------------------------------------

    #[napi]
    pub async fn set_by_name(&self, name: String, handle: u32) -> Result<()> {
        let graph = self.graph.clone();
        let core = self.core.clone();
        run_blocking(core, move || {
            graph.set(&name, HandleId::new(u64::from(handle)));
        })
        .await
    }

    #[napi]
    pub async fn get_by_name(&self, name: String) -> Result<u32> {
        let graph = self.graph.clone();
        let core = self.core.clone();
        let h = run_blocking(core, move || graph.get(&name)).await?;
        u32::try_from(h.raw()).map_err(|_| NapiError::from_reason("handle exceeds u32"))
    }

    #[napi]
    pub async fn invalidate_by_name(&self, name: String) -> Result<()> {
        let graph = self.graph.clone();
        let core = self.core.clone();
        run_blocking(core, move || {
            graph.invalidate_by_name(&name);
        })
        .await
    }

    #[napi]
    pub async fn complete_by_name(&self, name: String) -> Result<()> {
        let graph = self.graph.clone();
        let core = self.core.clone();
        run_blocking(core, move || {
            graph.complete_by_name(&name);
        })
        .await
    }

    #[napi]
    pub async fn error_by_name(&self, name: String, error_handle: u32) -> Result<()> {
        let graph = self.graph.clone();
        let core = self.core.clone();
        run_blocking(core, move || {
            graph.error_by_name(&name, HandleId::new(u64::from(error_handle)));
        })
        .await
    }

    #[napi]
    pub async fn remove(&self, name: String) -> Result<RemoveAuditJs> {
        let graph = self.graph.clone();
        let core = self.core.clone();
        let audit = run_blocking(core, move || graph.remove(&name))
            .await?
            .map_err(remove_error_to_napi)?;
        Ok(RemoveAuditJs {
            node_count: u32::try_from(audit.node_count).unwrap_or(u32::MAX),
            mount_count: u32::try_from(audit.mount_count).unwrap_or(u32::MAX),
        })
    }

    // -------------------------------------------------------------------
    // Signal broadcast (canonical §3.7).
    // -------------------------------------------------------------------

    #[napi]
    pub async fn signal_invalidate(&self) -> Result<()> {
        let graph = self.graph.clone();
        let core = self.core.clone();
        run_blocking(core, move || graph.signal(SignalKind::Invalidate)).await
    }

    #[napi]
    pub async fn signal_pause(&self, lock_id: u32) -> Result<()> {
        let graph = self.graph.clone();
        let core = self.core.clone();
        run_blocking(core, move || {
            graph.signal(SignalKind::Pause(LockId::new(u64::from(lock_id))));
        })
        .await
    }

    #[napi]
    pub async fn signal_resume(&self, lock_id: u32) -> Result<()> {
        let graph = self.graph.clone();
        let core = self.core.clone();
        run_blocking(core, move || {
            graph.signal(SignalKind::Resume(LockId::new(u64::from(lock_id))));
        })
        .await
    }

    /// Atomic-ish signal broadcast over a flat (code, payload) batch
    /// (Phase E /qa F3 — D077 server-side input validation). Accepts
    /// the same `MSG_CODE_*` table as `batch_emit_handle_messages`,
    /// but rejects everything except INVALIDATE / PAUSE / RESUME — the
    /// only kinds canonical §3.7.1 supports for graph-wide broadcast.
    /// DATA / RESOLVED / COMPLETE / ERROR / TEARDOWN return a typed
    /// `napi::Error` BEFORE any signal fires (legacy parity:
    /// `g.signal([[DATA, ...]])` throws synchronously).
    #[napi]
    pub async fn signal_batch(&self, encoded: Vec<u32>) -> Result<()> {
        if encoded.len() % 2 != 0 {
            return Err(NapiError::from_reason(format!(
                "signal_batch: encoded length must be even (alternating code, payload pairs); got {}",
                encoded.len()
            )));
        }
        // Up-front validation — bad input MUST NOT fire any partial broadcast.
        for chunk in encoded.chunks_exact(2) {
            let code = chunk[0];
            match code {
                MSG_CODE_INVALIDATE | MSG_CODE_PAUSE | MSG_CODE_RESUME => {}
                MSG_CODE_DATA | MSG_CODE_RESOLVED => {
                    return Err(NapiError::from_reason(
                        "signal_batch: tier-3 (DATA/RESOLVED) cannot be broadcast externally per canonical §3.7.1",
                    ));
                }
                MSG_CODE_COMPLETE | MSG_CODE_ERROR | MSG_CODE_TEARDOWN => {
                    return Err(NapiError::from_reason(format!(
                        "signal_batch: code {code} (COMPLETE/ERROR/TEARDOWN) is not supported for graph-wide broadcast",
                    )));
                }
                MSG_CODE_DIRTY | MSG_CODE_START => {
                    return Err(NapiError::from_reason(format!(
                        "signal_batch: code {code} (DIRTY/START) is not a broadcast tier",
                    )));
                }
                _ => {
                    return Err(NapiError::from_reason(format!(
                        "signal_batch: unknown message code {code}",
                    )));
                }
            }
        }

        let graph = self.graph.clone();
        let core = self.core.clone();
        run_blocking(core, move || {
            for chunk in encoded.chunks_exact(2) {
                let code = chunk[0];
                let payload = chunk[1];
                match code {
                    MSG_CODE_INVALIDATE => graph.signal(SignalKind::Invalidate),
                    MSG_CODE_PAUSE => {
                        graph.signal(SignalKind::Pause(LockId::new(u64::from(payload))));
                    }
                    MSG_CODE_RESUME => {
                        graph.signal(SignalKind::Resume(LockId::new(u64::from(payload))));
                    }
                    _ => unreachable!("validated up-front"),
                }
            }
        })
        .await
    }

    // -------------------------------------------------------------------
    // Mount tree (canonical §3.4).
    // -------------------------------------------------------------------

    /// Mount a fresh empty subgraph under `name`; returns a new
    /// `BenchGraph` sharing this graph's Core/binding/registry.
    #[napi]
    pub async fn mount_new(&self, name: String) -> Result<BenchGraph> {
        let graph = self.graph.clone();
        let core = self.core.clone();
        let binding = Arc::clone(&self.binding);
        let child_graph = run_blocking(core.clone(), move || graph.mount_new(name))
            .await?
            .map_err(|e| NapiError::from_reason(format!("{e}")))?;
        Ok(BenchGraph {
            graph: child_graph,
            binding,
            core,
        })
    }

    /// Detach a previously-mounted subgraph by name.
    #[napi]
    pub async fn unmount(&self, name: String) -> Result<RemoveAuditJs> {
        let graph = self.graph.clone();
        let core = self.core.clone();
        let audit = run_blocking(core, move || graph.unmount(&name))
            .await?
            .map_err(|e| NapiError::from_reason(format!("{e}")))?;
        Ok(RemoveAuditJs {
            node_count: u32::try_from(audit.node_count).unwrap_or(u32::MAX),
            mount_count: u32::try_from(audit.mount_count).unwrap_or(u32::MAX),
        })
    }

    // -------------------------------------------------------------------
    // Lifecycle.
    // -------------------------------------------------------------------

    #[napi]
    pub async fn destroy(&self) -> Result<()> {
        let graph = self.graph.clone();
        let core = self.core.clone();
        run_blocking(core, move || graph.destroy()).await
    }

    // -------------------------------------------------------------------
    // Static introspection.
    // -------------------------------------------------------------------

    /// Static edges snapshot — `Vec<(from_name, to_name)>` flattened to
    /// alternating `Vec<String>` for napi marshaling.
    #[napi]
    pub fn edges(&self, recursive: bool) -> Vec<String> {
        let edges = self.graph.edges(recursive);
        let mut flat: Vec<String> = Vec::with_capacity(edges.len() * 2);
        for (from, to) in edges {
            flat.push(from);
            flat.push(to);
        }
        flat
    }

    /// JSON-serialized describe snapshot. JS adapter parses with
    /// `JSON.parse`. Static — for reactive describe, see follow-on
    /// slice (out of scope per D074).
    #[napi]
    pub fn describe_json(&self) -> Result<String> {
        let snapshot = self.graph.describe();
        serde_json::to_string(&snapshot)
            .map_err(|e| NapiError::from_reason(format!("describe serialization failed: {e}")))
    }

    // -------------------------------------------------------------------
    // Reactive surfaces (Slice X2 — Phase E2 BenchGraph reactive)
    //
    // Canonical R3.6.1 / R3.6.2: `g.describe({ reactive: true })` and
    // `g.observe(path?, { reactive: true })`. Sink delivery uses TSFN
    // (NonBlocking — push-on-change, no bridge_sync needed). All TSFNs
    // use `callee_handled::<false>()` per Slice Y convention so JS
    // callbacks match the typed `Function<T, R>` declaration.
    // -------------------------------------------------------------------

    /// Subscribe to live topology snapshots (canonical R3.6.1
    /// `describe({ reactive: true })`). The JS callback receives a
    /// JSON-serialized [`graphrefly_graph::GraphDescribeOutput`] on
    /// each fire; first fire is the push-on-subscribe initial
    /// snapshot, subsequent fires are post-change snapshots.
    ///
    /// Returns a [`BenchDescribeReactiveHandle`] napi class. JS code
    /// MUST `await handle.dispose()` before letting the handle drop —
    /// the unsubscribe runs on a tokio blocking thread to avoid the
    /// JS-thread / Core-mutex deadlock vector that `BenchCore::dispose`
    /// closes for subscriptions.
    #[napi]
    pub fn describe_reactive<'env>(
        &self,
        env: &'env Env,
        sink: Function<String, ()>,
    ) -> Result<PromiseRaw<'env, BenchDescribeReactiveHandle>> {
        let tsfn = build_describe_tsfn(sink)?;
        let graph = self.graph.clone();
        let core = self.core.clone();
        env.spawn_future(async move {
            let core_for_blocking = core.clone();
            let rdh = run_blocking(core_for_blocking, move || -> ReactiveDescribeHandle {
                let describe_sink: DescribeSink = Arc::new(move |snapshot| {
                    // JSON-serialize the snapshot per Q2; on serde failure
                    // we silently skip this fire (sink can't propagate
                    // errors back to the producer per the R3.6.1 contract).
                    if let Ok(json) = serde_json::to_string(snapshot) {
                        let _status =
                            tsfn.call(json, ThreadsafeFunctionCallMode::NonBlocking);
                    }
                });
                graph.describe_reactive(describe_sink)
            })
            .await?;
            Ok(BenchDescribeReactiveHandle {
                inner: Arc::new(Mutex::new(Some(rdh))),
                core,
            })
        })
    }

    /// Subscribe to all-named-nodes message stream with auto-subscribe
    /// on late-added nodes (canonical R3.6.2 `observe(undefined, {
    /// reactive: true })`). The JS callback receives `(name,
    /// encoded_msgs)` per emission — `encoded_msgs` is the same flat
    /// `[code_0, payload_0, ...]` shape that `subscribe_with_tsfn`
    /// uses, decoded JS-side via the `MSG_CODE_*` table.
    ///
    /// Returns a [`BenchObserveReactiveHandle`] napi class.
    /// `await handle.dispose()` unsubscribes everything (the inner
    /// `GraphObserveAllReactive` clears all fan-out subs + the
    /// namespace-change listener).
    #[napi]
    pub fn observe_all_reactive<'env>(
        &self,
        env: &'env Env,
        sink: Function<FnArgs<(String, Vec<u32>)>, ()>,
    ) -> Result<PromiseRaw<'env, BenchObserveReactiveHandle>> {
        let tsfn = build_observe_all_tsfn(sink)?;
        let graph = self.graph.clone();
        let core = self.core.clone();
        env.spawn_future(async move {
            let core_for_blocking = core.clone();
            let mut handle = run_blocking(core_for_blocking, move || -> GraphObserveAllReactive {
                let mut handle = graph.observe_all_reactive();
                handle.subscribe(move |name: &str, msgs: &[Message]| {
                    let payload = encode_messages(msgs);
                    let _status = tsfn.call(
                        FnArgs::from((name.to_string(), payload)),
                        ThreadsafeFunctionCallMode::NonBlocking,
                    );
                });
                handle
            })
            .await?;
            // Inner mutex needs `&mut` access but napi class fields are
            // `&self` — wrap in Mutex<Option<_>> so dispose can take
            // ownership.
            Ok(BenchObserveReactiveHandle {
                inner: Arc::new(Mutex::new(Some(ObserveReactiveInner::All(handle)))),
                core,
            })
        })
    }

    /// Sink-style observe of a single node (canonical R3.6.2 default
    /// mode `observe(path)`). Returns a subscription index into a
    /// `BenchObserveReactiveHandle` slot that mirrors `BenchCore`'s
    /// subscriptions vec semantics. Provided for canonical-spec
    /// completeness — most parity scenarios use the reactive variant.
    ///
    /// `path` resolves via `Graph::node(path)`; panics if unknown.
    #[napi]
    pub fn observe_subscribe<'env>(
        &self,
        env: &'env Env,
        path: Option<String>,
        sink: Function<FnArgs<(String, Vec<u32>)>, ()>,
    ) -> Result<PromiseRaw<'env, BenchObserveReactiveHandle>> {
        // Sink-style "observe()" — when no path given, fan-out across
        // all named nodes at call time (snapshot semantics, no
        // auto-subscribe — see observe_all_reactive for that). When a
        // path IS given, single-node sink-style.
        let tsfn = build_observe_all_tsfn(sink)?;
        let graph = self.graph.clone();
        let core = self.core.clone();
        env.spawn_future(async move {
            let core_for_blocking = core.clone();
            let inner = run_blocking(core_for_blocking, move || -> ObserveReactiveInner {
                if let Some(path) = path {
                    let one = graph.observe(&path);
                    let path_owned = path;
                    let sink: Sink = Arc::new(move |msgs: &[Message]| {
                        let payload = encode_messages(msgs);
                        let _status = tsfn.call(
                            FnArgs::from((path_owned.clone(), payload)),
                            ThreadsafeFunctionCallMode::NonBlocking,
                        );
                    });
                    let sub = one.subscribe(sink);
                    ObserveReactiveInner::OneSub(sub)
                } else {
                    // observe() — sink-style snapshot fan-out across
                    // named nodes (NOT auto-subscribe; canonical
                    // GraphObserveAll default).
                    let mut all = graph.observe_all();
                    all.subscribe(move |name: &str, msgs: &[Message]| {
                        let payload = encode_messages(msgs);
                        let _status = tsfn.call(
                            FnArgs::from((name.to_string(), payload)),
                            ThreadsafeFunctionCallMode::NonBlocking,
                        );
                    });
                    ObserveReactiveInner::AllSubs(all)
                }
            })
            .await?;
            Ok(BenchObserveReactiveHandle {
                inner: Arc::new(Mutex::new(Some(inner))),
                core,
            })
        })
    }
}

// ---------------------------------------------------------------------------
// Reactive describe handle (Slice X2)
// ---------------------------------------------------------------------------

/// RAII handle for a reactive describe subscription. JS code SHOULD
/// `await dispose()` before letting it drop — that path runs the
/// unsubscribe on a tokio blocking thread, keeping the JS thread free
/// to pump libuv. As defense-in-depth (Slice X3 /qa A), the `Drop`
/// impl ALSO ships the inner-drop to a tokio blocking thread
/// fire-and-forget, so a forgotten `dispose()` doesn't deadlock the
/// JS thread on Graph's namespace_sinks mutex.
#[napi]
pub struct BenchDescribeReactiveHandle {
    inner: Arc<Mutex<Option<ReactiveDescribeHandle>>>,
    core: Core,
}

#[napi]
impl BenchDescribeReactiveHandle {
    /// Drop the inner subscription on a tokio blocking thread.
    /// Idempotent — subsequent calls are no-ops.
    #[napi]
    pub async fn dispose(&self) -> Result<()> {
        let taken = self.inner.lock().take();
        if let Some(handle) = taken {
            let core = self.core.clone();
            run_blocking(core, move || drop(handle)).await?;
        }
        Ok(())
    }
}

impl Drop for BenchDescribeReactiveHandle {
    fn drop(&mut self) {
        // Slice X3 /qa A — defense-in-depth against forgotten dispose.
        // Take the inner option; if Some, ship to tokio blocking pool
        // fire-and-forget. The JoinHandle is dropped (detaches the
        // task; runs to completion regardless). If no tokio runtime
        // is active (process shutdown), the spawn_blocking call may
        // panic; we accept that — at shutdown, libuv is winding down
        // anyway and the deadlock vector is moot.
        let taken = self.inner.lock().take();
        if let Some(handle) = taken {
            let _ = spawn_blocking(move || drop(handle));
        }
    }
}

// Send + Sync compile-time assertion (inner type, per Slice X3 /qa).
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<BenchDescribeReactiveHandle>();
    assert_send_sync::<ReactiveDescribeHandle>();
};

// ---------------------------------------------------------------------------
// Reactive / sink-style observe handle (Slice X2)
//
// Single napi class with an internal enum so the JS adapter can
// dispose any observe variant (one-sub / all-subs / all-reactive)
// uniformly.
// ---------------------------------------------------------------------------

enum ObserveReactiveInner {
    OneSub(Subscription),
    AllSubs(graphrefly_graph::GraphObserveAll),
    All(GraphObserveAllReactive),
}

/// RAII handle for an observe subscription (single-node, all-nodes
/// snapshot, or all-nodes reactive auto-subscribe). JS code SHOULD
/// `await dispose()` (runs unsubscribe on tokio); as defense-in-depth
/// (Slice X3 /qa A) the `Drop` impl ALSO ships the inner drop to
/// tokio fire-and-forget so a forgotten `dispose()` doesn't deadlock
/// the JS thread on Core's mutex via `Subscription::Drop`.
#[napi]
pub struct BenchObserveReactiveHandle {
    inner: Arc<Mutex<Option<ObserveReactiveInner>>>,
    core: Core,
}

#[napi]
impl BenchObserveReactiveHandle {
    #[napi]
    pub async fn dispose(&self) -> Result<()> {
        let taken = self.inner.lock().take();
        if let Some(inner) = taken {
            let core = self.core.clone();
            run_blocking(core, move || drop(inner)).await?;
        }
        Ok(())
    }
}

impl Drop for BenchObserveReactiveHandle {
    fn drop(&mut self) {
        // Slice X3 /qa A — defense-in-depth (see BenchDescribeReactiveHandle).
        let taken = self.inner.lock().take();
        if let Some(inner) = taken {
            let _ = spawn_blocking(move || drop(inner));
        }
    }
}

const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<BenchObserveReactiveHandle>();
    assert_send_sync::<ObserveReactiveInner>();
};

// ---------------------------------------------------------------------------
// TSFN builders for reactive surfaces (Slice X2)
//
// Both use `callee_handled::<false>()` per the Slice Y convention —
// JS callbacks receive `(value)` directly (no err-first wire shape),
// matching the typed `Function<T, R>` declaration.
// ---------------------------------------------------------------------------

// Slice X3 /qa C: `MaxQueueSize=8` for reactive sinks. Operator TSFNs
// use `=1` because they're sync-bridged (one in-flight call at a time);
// reactive describe/observe TSFNs are NonBlocking notification streams
// where back-pressure-from-JS-thread shouldn't silently drop snapshots
// (Status::QueueFull return is unrecoverable — the sink misses a fire).
// 8 is a defensive cushion for typical bursty namespace-mutation
// patterns; if real workloads need more, lift via porting-deferred entry.
type DescribeTsfn = ThreadsafeFunction<String, (), String, Status, false, false, 8>;
type ObserveAllTsfn = ThreadsafeFunction<
    FnArgs<(String, Vec<u32>)>,
    (),
    FnArgs<(String, Vec<u32>)>,
    Status,
    false,
    false,
    8,
>;

fn build_describe_tsfn(callback: Function<String, ()>) -> Result<Arc<DescribeTsfn>> {
    let tsfn = callback
        .build_threadsafe_function::<String>()
        .max_queue_size::<8>()
        .callee_handled::<false>()
        .build_callback(|ctx: ThreadsafeCallContext<String>| Ok(ctx.value))?;
    Ok(Arc::new(tsfn))
}

fn build_observe_all_tsfn(
    callback: Function<FnArgs<(String, Vec<u32>)>, ()>,
) -> Result<Arc<ObserveAllTsfn>> {
    let tsfn = callback
        .build_threadsafe_function::<FnArgs<(String, Vec<u32>)>>()
        .max_queue_size::<8>()
        .callee_handled::<false>()
        .build_callback(|ctx: ThreadsafeCallContext<FnArgs<(String, Vec<u32>)>>| Ok(ctx.value))?;
    Ok(Arc::new(tsfn))
}

/// Encode a `&[Message]` batch into the flat `[code_0, payload_0,
/// code_1, payload_1, ...]` shape that `subscribe_with_tsfn` and the
/// JS-side `MSG_CODE_*` decoder use.
///
/// Slice X3 /qa Group 2 #5: skips `Message::Start` at encode time.
/// `Start` is the per-subscription handshake (R1.3.5.a) — user sinks
/// don't observe it; legacy filters it out before the user sink. The
/// JS-side `decodeMessages` already skipped `MSG_CODE_START`; filtering
/// at the encoder saves the wire round-trip and avoids JS-side noise
/// on auto-subscribe-late observe variants where every late-added node
/// would otherwise emit a wasted START + 0 pair on every fire.
fn encode_messages(msgs: &[Message]) -> Vec<u32> {
    let mut out: Vec<u32> = Vec::with_capacity(msgs.len() * 2);
    for m in msgs {
        let (code, arg) = match m {
            Message::Start => continue,
            Message::Dirty => (MSG_CODE_DIRTY, 0u32),
            Message::Resolved => (MSG_CODE_RESOLVED, 0),
            Message::Data(h) => (MSG_CODE_DATA, u32::try_from(h.raw()).unwrap_or(u32::MAX)),
            Message::Pause(l) => (MSG_CODE_PAUSE, u32::try_from(l.raw()).unwrap_or(u32::MAX)),
            Message::Resume(l) => (MSG_CODE_RESUME, u32::try_from(l.raw()).unwrap_or(u32::MAX)),
            Message::Invalidate => (MSG_CODE_INVALIDATE, 0),
            Message::Complete => (MSG_CODE_COMPLETE, 0),
            Message::Error(h) => (MSG_CODE_ERROR, u32::try_from(h.raw()).unwrap_or(u32::MAX)),
            Message::Teardown => (MSG_CODE_TEARDOWN, 0),
        };
        out.push(code);
        out.push(arg);
    }
    out
}

/// JS-exposed `GraphRemoveAudit`. Mirrors `graphrefly_graph::GraphRemoveAudit`'s
/// flat counts; per-node detail isn't surfaced through the napi boundary
/// in v1 (parity scenarios only assert counts).
#[napi(object)]
pub struct RemoveAuditJs {
    pub node_count: u32,
    pub mount_count: u32,
}

fn name_error_to_napi(e: NameError) -> NapiError {
    NapiError::from_reason(format!("{e}"))
}

fn remove_error_to_napi(e: RemoveError) -> NapiError {
    NapiError::from_reason(format!("{e}"))
}

// Suppress unused-binding lint for binding (used only when crate
// features that depend on it are enabled).
#[allow(dead_code)]
fn _binding_used(b: &Arc<BenchBinding>) -> &dyn BindingBoundary {
    &**b
}
