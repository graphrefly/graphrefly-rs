//! `BenchGraph` napi class — Graph parity surface for JS callers.
//!
//! Phase E rustImpl activation slice (D074). Wraps
//! [`graphrefly_graph::Graph`] for the JS adapter to expose as
//! `Impl.Graph`. Build profile: only enabled with the `graph-codec`
//! feature.
//!
//! # Scope (this slice)
//!
//! - **Namespace ops**: `state`, `derived`, `dynamic` (placeholder),
//!   `add`, `node`, `try_resolve`, `name_of`, `node_count`,
//!   `node_names`, `child_names`, `is_destroyed`.
//! - **Lifecycle**: `set`, `get`, `invalidate_by_name`,
//!   `complete_by_name`, `error_by_name`, `remove`, `destroy`.
//! - **Mount tree**: `mount_new` (returns a fresh BenchGraph wrapping
//!   the child).
//! - **Signal broadcast**: `signal_invalidate`, `signal_pause`,
//!   `signal_resume` (per-kind; JS adapter decomposes batches).
//! - **Static describe**: `describe_json` returns a JSON-serialized
//!   snapshot.
//! - **Edges (static)**: `edges(recursive)` returns a flat
//!   `Vec<(String, String)>` list.
//! - **Reactive describe / observe**: `describe_reactive`,
//!   `observe_all_reactive`, `observe_subscribe`. Each returns a
//!   napi handle whose `dispose()` posts the owner-invoked detach
//!   through the actor (D246 r3 — no RAII below the binding).
//!
//! # S6/D255 actor model
//!
//! The `Graph` is `!Send + !Sync` post-D246 (`Rc<RefCell<GraphInner>>`,
//! no Core stored inside; every method takes `&Core` explicitly), so
//! `BenchGraph` CANNOT store `Graph` directly. Instead, each
//! `BenchGraph` instance allocates a `graph_key: u64` and inserts its
//! `Graph` into the actor's [`crate::core_actor::WORKER_EXTRAS`]
//! registry on the worker thread. Every method then dispatches a
//! closure that looks the `Graph` up by key.
//!
//! Reactive handles (`BenchDescribeReactiveHandle`,
//! `BenchObserveReactiveHandle`) follow the same pattern: the inner
//! `!Send` resource (`ReactiveDescribeHandle` / `GraphObserveAll{,
//! Reactive}`, plus the one-node `(NodeId, SubscriptionId)` pair for
//! sink-style observe) is stored in `WORKER_EXTRAS` keyed by
//! `handle_key`. `dispose()` posts an actor closure that takes the
//! entry out and calls the resource's `detach(core)` (or
//! `core.unsubscribe(...)` for the one-node case).

use std::sync::Arc;

use graphrefly_core::{
    BindingBoundary, EqualsMode, FnId, HandleId, LockId, Message, NodeId, Sink, SubscriptionId,
};
use graphrefly_graph::{
    DescribeSink, Graph, GraphObserveAll, GraphObserveAllReactive, NameError,
    ReactiveDescribeHandle, RemoveError, SignalKind,
};
use napi::bindgen_prelude::*;
use napi::threadsafe_function::{
    ThreadsafeCallContext, ThreadsafeFunction, ThreadsafeFunctionCallMode,
};
use napi::{Env, Error as NapiError, Status};
use napi_derive::napi;

use crate::core_actor::{
    alloc_worker_key, install_extra, take_extra, with_extra, with_extra_install_child,
    with_extra_try_install_child, CoreActor,
};
use crate::core_bindings::{
    BenchBinding, BenchCore, BuiltinFn, MSG_CODE_COMPLETE, MSG_CODE_DATA, MSG_CODE_DIRTY,
    MSG_CODE_ERROR, MSG_CODE_INVALIDATE, MSG_CODE_PAUSE, MSG_CODE_RESOLVED, MSG_CODE_RESUME,
    MSG_CODE_START, MSG_CODE_TEARDOWN,
};

/// JS-facing wrapper around a `graphrefly_graph::Graph` (S6/D255).
///
/// Each `BenchGraph` shares its `BenchCore`'s `Arc<CoreActor>` + value
/// registry + binding so nodes registered through `BenchGraph::state(...)`
/// are visible to `BenchOperators::register_*(...)` and vice-versa.
/// The actual `Graph` instance (which is `!Send + !Sync`) lives in
/// the actor's `WORKER_EXTRAS` thread-local on the worker thread,
/// keyed by [`Self::graph_key`].
#[napi]
pub struct BenchGraph {
    pub(crate) actor: Arc<CoreActor>,
    pub(crate) binding: Arc<BenchBinding>,
    /// Identifier into the actor's `WORKER_EXTRAS` registry where this
    /// graph's `Graph` instance lives. The `Drop` impl posts a
    /// `dispatch_detached` closure that removes the entry, dropping
    /// the `!Send` Graph on the worker stack.
    pub(crate) graph_key: u64,
}

#[napi]
impl BenchGraph {
    /// Construct a fresh root graph wired to a `BenchCore`'s binding +
    /// actor (D074 + D255). The BenchGraph SHARES the BenchCore's
    /// actor — nodes registered through `BenchGraph::state(...)` are
    /// visible to operator factories on the same `BenchCore`.
    ///
    /// **Note:** this constructor blocks the JS thread briefly (one
    /// sync_channel round-trip with the actor) to install the Graph
    /// in `WORKER_EXTRAS`. The work is trivial (one `Graph::new` +
    /// HashMap insert); blocking is acceptable for a one-shot factory
    /// call.
    ///
    /// **F14 invariant (restored 2026-05-20 QA F9/m12, preserved by
    /// construction):** the `binding: Arc<BenchBinding>` field and
    /// the actor's Core's `BindingBoundary` upcast come from the
    /// SAME `Arc<BenchBinding>` — both `core.actor_arc()` and
    /// `core.binding_arc()` clone fields owned by the supplied
    /// `BenchCore`. `BenchCore::new` is the single construction path
    /// for that Arc; no public API mints a divergent binding.
    #[napi(factory)]
    pub fn from_core(core: &BenchCore, name: String) -> Result<Self> {
        let actor = core.actor_arc();
        let binding = core.binding_arc();
        let graph_key = alloc_worker_key();
        actor.run_sync(move |_core| {
            install_extra::<Graph>(graph_key, Graph::new(name));
        })?;
        Ok(Self {
            actor,
            binding,
            graph_key,
        })
    }

    // -------------------------------------------------------------------
    // Namespace introspection (D267 — ALL async, post-deadlock-fix).
    //
    // Pre-D267 these used `actor.run_sync` (blocks libuv on the actor
    // reply). That shape DEADLOCKS when a JS sink callback running on
    // the libuv thread (dispatched from the actor worker via TSFN
    // Blocking) calls one of these read methods: JS blocks libuv on
    // the actor reply, but the actor worker is itself blocked on the
    // TSFN sink reply → 3-way deadlock. This was the cross-track-ledger
    // §2 `Graph::remove`/`destroy` deadlock root cause (R3.7.3
    // `nameOf`-inside-TEARDOWN-sink test).
    //
    // D267 fix: every read method routes through `actor.run` (async).
    // libuv stays free to pump the actor reply during the await. The
    // parity `Impl` was widened to `T | Promise<T>` so the pure-ts
    // arm returns sync values unchanged.
    // -------------------------------------------------------------------

    #[napi]
    pub async fn name(&self) -> Result<String> {
        let key = self.graph_key;
        self.actor
            .run(move |_core| {
                with_extra::<Graph, _>(key, |g| g.name()).expect("BenchGraph: graph entry missing")
            })
            .await
    }

    #[napi]
    pub async fn node_count(&self) -> Result<u32> {
        let key = self.graph_key;
        let n = self
            .actor
            .run(move |_core| {
                with_extra::<Graph, _>(key, |g| g.node_count())
                    .expect("BenchGraph: graph entry missing")
            })
            .await?;
        // QA fix 2026-05-20 (Blind Hunter M2): fail-loud on >u32::MAX
        // nodes instead of saturating silently. Matches the (correct)
        // overflow-on-Err pattern used everywhere else in this file.
        u32::try_from(n).map_err(|_| NapiError::from_reason("node_count exceeds u32"))
    }

    #[napi]
    pub async fn node_names(&self) -> Result<Vec<String>> {
        let key = self.graph_key;
        self.actor
            .run(move |_core| {
                with_extra::<Graph, _>(key, |g| g.node_names())
                    .expect("BenchGraph: graph entry missing")
            })
            .await
    }

    #[napi]
    pub async fn child_names(&self) -> Result<Vec<String>> {
        let key = self.graph_key;
        self.actor
            .run(move |_core| {
                with_extra::<Graph, _>(key, |g| g.child_names())
                    .expect("BenchGraph: graph entry missing")
            })
            .await
    }

    #[napi]
    pub async fn is_destroyed(&self) -> Result<bool> {
        let key = self.graph_key;
        self.actor
            .run(move |_core| {
                with_extra::<Graph, _>(key, |g| g.is_destroyed())
                    .expect("BenchGraph: graph entry missing")
            })
            .await
    }

    /// Resolve a path to a NodeId; returns 0 (NO_NODE) if missing.
    #[napi]
    pub async fn try_resolve(&self, path: String) -> Result<u32> {
        let key = self.graph_key;
        self.actor
            .run(move |_core| {
                with_extra::<Graph, _>(key, |g| {
                    g.try_resolve(&path)
                        .map_or(0, |id| u32::try_from(id.raw()).unwrap_or(0))
                })
                .expect("BenchGraph: graph entry missing")
            })
            .await
    }

    /// Reverse lookup: returns the local name for a NodeId, or None
    /// if unnamed in this graph.
    #[napi]
    pub async fn name_of(&self, node_id: u32) -> Result<Option<String>> {
        let key = self.graph_key;
        self.actor
            .run(move |_core| {
                with_extra::<Graph, _>(key, |g| g.name_of(NodeId::new(u64::from(node_id))))
                    .expect("BenchGraph: graph entry missing")
            })
            .await
    }

    // -------------------------------------------------------------------
    // Sugar constructors (canonical §3.9).
    // -------------------------------------------------------------------

    /// `state(name, initial_handle?)`. `None` → sentinel cache.
    /// Caller (JS adapter) holds the handle's refcount via
    /// `BenchCore::alloc_external_handle`; ownership transfers here.
    #[napi]
    pub async fn state(&self, name: String, initial_handle: Option<u32>) -> Result<u32> {
        let key = self.graph_key;
        self.actor
            .run(move |core| -> Result<u32> {
                let h = initial_handle.map(|h| HandleId::new(u64::from(h)));
                with_extra::<Graph, _>(key, move |g| {
                    let id = g.state(core, name, h).map_err(name_error_to_napi)?;
                    u32::try_from(id.raw())
                        .map_err(|_| NapiError::from_reason("node id exceeds u32"))
                })
                .expect("BenchGraph: graph entry missing")
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
        let key = self.graph_key;
        let binding = Arc::clone(&self.binding);
        self.actor
            .run(move |core| -> Result<u32> {
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
                    BuiltinFn::AddOne => {
                        Arc::new(
                            |deps: &[crate::core_bindings::BenchValue]| match deps.first() {
                                Some(crate::core_bindings::BenchValue::Int(n)) => {
                                    Some(crate::core_bindings::BenchValue::Int(n + 1))
                                }
                                _ => None,
                            },
                        )
                    }
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
                with_extra::<Graph, _>(key, move |g| {
                    let id = g
                        .derived(core, name, &deps, fn_id, EqualsMode::Identity)
                        .map_err(name_error_to_napi)?;
                    u32::try_from(id.raw())
                        .map_err(|_| NapiError::from_reason("node id exceeds u32"))
                })
                .expect("BenchGraph: graph entry missing")
            })
            .await?
    }

    /// Register an existing node (e.g., from `BenchOperators::register_map`)
    /// under `name` in this graph's namespace.
    ///
    /// **Async** at the napi boundary (QA fix 2026-05-20, Edge Case
    /// Hunter F4 / Blind Hunter M7): `Graph::add` fires
    /// `NamespaceChangeSink`s synchronously, and an active
    /// `observe_all_reactive` / `attach_snapshot_storage` handle's
    /// sink calls `core.subscribe(new_node, sink)`. Under the prior
    /// sync `run_sync` shape that subscribe would push the
    /// handshake's START to a TSFN-backed sink while libuv is blocked
    /// on the actor reply — at best a queue-full silent drop
    /// (`NonBlocking max_queue_size=8`), at worst a 3-way deadlock
    /// if any sink uses `bridge_sync`. `actor.run` keeps libuv free
    /// to pump TSFN microtasks during the await.
    #[napi]
    pub async fn add(&self, name: String, node_id: u32) -> Result<u32> {
        let key = self.graph_key;
        self.actor
            .run(move |core| -> Result<u32> {
                with_extra::<Graph, _>(key, move |g| {
                    let id = g
                        .add(core, NodeId::new(u64::from(node_id)), name)
                        .map_err(name_error_to_napi)?;
                    u32::try_from(id.raw())
                        .map_err(|_| NapiError::from_reason("node id exceeds u32"))
                })
                .expect("BenchGraph: graph entry missing")
            })
            .await?
    }

    // -------------------------------------------------------------------
    // Named-sugar lifecycle (canonical §3.2.1).
    // -------------------------------------------------------------------

    #[napi]
    pub async fn set_by_name(&self, name: String, handle: u32) -> Result<()> {
        let key = self.graph_key;
        self.actor
            .run(move |core| {
                with_extra::<Graph, _>(key, move |g| {
                    g.set(core, &name, HandleId::new(u64::from(handle)));
                })
                .expect("BenchGraph: graph entry missing");
            })
            .await
    }

    #[napi]
    pub async fn get_by_name(&self, name: String) -> Result<u32> {
        let key = self.graph_key;
        let h = self
            .actor
            .run(move |core| {
                with_extra::<Graph, _>(key, move |g| g.get(core, &name))
                    .expect("BenchGraph: graph entry missing")
            })
            .await?;
        u32::try_from(h.raw()).map_err(|_| NapiError::from_reason("handle exceeds u32"))
    }

    #[napi]
    pub async fn invalidate_by_name(&self, name: String) -> Result<()> {
        let key = self.graph_key;
        self.actor
            .run(move |core| {
                with_extra::<Graph, _>(key, move |g| {
                    g.invalidate_by_name(core, &name);
                })
                .expect("BenchGraph: graph entry missing");
            })
            .await
    }

    #[napi]
    pub async fn complete_by_name(&self, name: String) -> Result<()> {
        let key = self.graph_key;
        self.actor
            .run(move |core| {
                with_extra::<Graph, _>(key, move |g| {
                    g.complete_by_name(core, &name);
                })
                .expect("BenchGraph: graph entry missing");
            })
            .await
    }

    #[napi]
    pub async fn error_by_name(&self, name: String, error_handle: u32) -> Result<()> {
        let key = self.graph_key;
        self.actor
            .run(move |core| {
                with_extra::<Graph, _>(key, move |g| {
                    g.error_by_name(core, &name, HandleId::new(u64::from(error_handle)));
                })
                .expect("BenchGraph: graph entry missing");
            })
            .await
    }

    #[napi]
    pub async fn remove(&self, name: String) -> Result<RemoveAuditJs> {
        let key = self.graph_key;
        let audit = self
            .actor
            .run(move |core| {
                with_extra::<Graph, _>(key, move |g| g.remove(core, &name))
                    .expect("BenchGraph: graph entry missing")
            })
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
        let key = self.graph_key;
        self.actor
            .run(move |core| {
                with_extra::<Graph, _>(key, move |g| g.signal(core, SignalKind::Invalidate))
                    .expect("BenchGraph: graph entry missing");
            })
            .await
    }

    #[napi]
    pub async fn signal_pause(&self, lock_id: u32) -> Result<()> {
        let key = self.graph_key;
        self.actor
            .run(move |core| {
                with_extra::<Graph, _>(key, move |g| {
                    g.signal(core, SignalKind::Pause(LockId::new(u64::from(lock_id))));
                })
                .expect("BenchGraph: graph entry missing");
            })
            .await
    }

    #[napi]
    pub async fn signal_resume(&self, lock_id: u32) -> Result<()> {
        let key = self.graph_key;
        self.actor
            .run(move |core| {
                with_extra::<Graph, _>(key, move |g| {
                    g.signal(core, SignalKind::Resume(LockId::new(u64::from(lock_id))));
                })
                .expect("BenchGraph: graph entry missing");
            })
            .await
    }

    /// Atomic-ish signal broadcast over a flat (code, payload) batch
    /// (Phase E /qa F3 — D077 server-side input validation). Accepts
    /// the same `MSG_CODE_*` table as `batch_emit_handle_messages`,
    /// but rejects everything except INVALIDATE / PAUSE / RESUME — the
    /// only kinds canonical §3.7.1 supports for graph-wide broadcast.
    #[napi]
    pub async fn signal_batch(&self, encoded: Vec<u32>) -> Result<()> {
        if encoded.len() % 2 != 0 {
            return Err(NapiError::from_reason(format!(
                "signal_batch: encoded length must be even (alternating code, payload pairs); got {}",
                encoded.len()
            )));
        }
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

        let key = self.graph_key;
        self.actor
            .run(move |core| {
                with_extra::<Graph, _>(key, move |g| {
                    for chunk in encoded.chunks_exact(2) {
                        let code = chunk[0];
                        let payload = chunk[1];
                        match code {
                            MSG_CODE_INVALIDATE => g.signal(core, SignalKind::Invalidate),
                            MSG_CODE_PAUSE => {
                                g.signal(core, SignalKind::Pause(LockId::new(u64::from(payload))));
                            }
                            MSG_CODE_RESUME => {
                                g.signal(core, SignalKind::Resume(LockId::new(u64::from(payload))));
                            }
                            _ => unreachable!("validated up-front"),
                        }
                    }
                })
                .expect("BenchGraph: graph entry missing");
            })
            .await
    }

    // -------------------------------------------------------------------
    // Mount tree (canonical §3.4).
    // -------------------------------------------------------------------

    /// Mount a fresh empty subgraph under `name`; returns a new
    /// `BenchGraph` sharing this graph's actor + binding.
    ///
    /// Uses [`crate::core_actor::with_extra_try_install_child`] to
    /// read the parent + install the child Graph in a SINGLE
    /// `WORKER_EXTRAS` `borrow_mut` — avoids the F2-style nested-
    /// borrow panic the prior `with_extra` + `install_extra` shape
    /// triggered (Edge Case Hunter F2 / Blind Hunter M5, 2026-05-20).
    #[napi]
    pub async fn mount_new(&self, name: String) -> Result<BenchGraph> {
        let parent_key = self.graph_key;
        let child_key = alloc_worker_key();
        self.actor
            .run(move |core| -> Result<()> {
                with_extra_try_install_child::<Graph, Graph, (), _, _>(
                    parent_key,
                    child_key,
                    move |parent| -> std::result::Result<(Graph, ()), NapiError> {
                        let child = parent
                            .mount_new(core, name)
                            .map_err(|e| NapiError::from_reason(format!("{e}")))?;
                        Ok((child, ()))
                    },
                )
                .expect("BenchGraph: graph entry missing")
            })
            .await??;
        Ok(BenchGraph {
            actor: Arc::clone(&self.actor),
            binding: Arc::clone(&self.binding),
            graph_key: child_key,
        })
    }

    /// Detach a previously-mounted subgraph by name.
    #[napi]
    pub async fn unmount(&self, name: String) -> Result<RemoveAuditJs> {
        let key = self.graph_key;
        let audit = self
            .actor
            .run(move |core| {
                with_extra::<Graph, _>(key, move |g| g.unmount(core, &name))
                    .expect("BenchGraph: graph entry missing")
            })
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
        let key = self.graph_key;
        self.actor
            .run(move |core| {
                with_extra::<Graph, _>(key, move |g| g.destroy(core))
                    .expect("BenchGraph: graph entry missing");
            })
            .await
    }

    // -------------------------------------------------------------------
    // Static introspection.
    // -------------------------------------------------------------------

    /// Static edges snapshot — `Vec<(from_name, to_name)>` flattened to
    /// alternating `Vec<String>` for napi marshaling.
    ///
    /// **D267 — async** (was `run_sync`). The parity `Impl.edges`
    /// contract is widened to `T | Promise<T>` so the pure-ts arm
    /// returns sync values unchanged; rust arm returns a Promise.
    /// Eliminates the sink-callback deadlock class — see the
    /// "Namespace introspection" block above for the full rationale.
    #[napi]
    pub async fn edges(&self, recursive: bool) -> Result<Vec<String>> {
        let key = self.graph_key;
        self.actor
            .run(move |core| {
                with_extra::<Graph, _>(key, move |g| {
                    let edges = g.edges(core, recursive);
                    let mut flat: Vec<String> = Vec::with_capacity(edges.len() * 2);
                    for (from, to) in edges {
                        flat.push(from);
                        flat.push(to);
                    }
                    flat
                })
                .expect("BenchGraph: graph entry missing")
            })
            .await
    }

    /// JSON-serialized describe snapshot. JS adapter parses with
    /// `JSON.parse`. Static — for reactive describe, see
    /// `describe_reactive`.
    ///
    /// **D267 — async** (was `run_sync`). See `edges` above.
    #[napi]
    pub async fn describe_json(&self) -> Result<String> {
        let key = self.graph_key;
        let snapshot = self
            .actor
            .run(move |core| {
                with_extra::<Graph, _>(key, move |g| g.describe(core))
                    .expect("BenchGraph: graph entry missing")
            })
            .await?;
        serde_json::to_string(&snapshot)
            .map_err(|e| NapiError::from_reason(format!("describe serialization failed: {e}")))
    }

    // -------------------------------------------------------------------
    // Reactive surfaces (Slice X2 — Phase E2 BenchGraph reactive,
    // S6/D255 actor-routed).
    // -------------------------------------------------------------------

    /// Subscribe to live topology snapshots (canonical R3.6.1
    /// `describe({ reactive: true })`). Returns a
    /// [`BenchDescribeReactiveHandle`]; JS code MUST `await
    /// handle.dispose()` (D246 r3 — owner-invoked detach).
    #[napi]
    pub fn describe_reactive<'env>(
        &self,
        env: &'env Env,
        sink: Function<'_, String, ()>,
    ) -> Result<PromiseRaw<'env, BenchDescribeReactiveHandle>> {
        let tsfn = build_describe_tsfn(sink)?;
        let graph_key = self.graph_key;
        let handle_key = alloc_worker_key();
        let actor = Arc::clone(&self.actor);
        env.spawn_future(async move {
            let actor_for_handle = Arc::clone(&actor);
            actor
                .run(move |core| {
                    with_extra_install_child::<Graph, ReactiveDescribeHandle, _, _>(
                        graph_key,
                        handle_key,
                        move |g| {
                            let describe_sink: DescribeSink = std::rc::Rc::new(move |snapshot| {
                                if let Ok(json) = serde_json::to_string(snapshot) {
                                    let _status =
                                        tsfn.call(json, ThreadsafeFunctionCallMode::NonBlocking);
                                }
                            });
                            let rdh = g.describe_reactive(core, &describe_sink);
                            (rdh, ())
                        },
                    )
                    .expect("BenchGraph: graph entry missing");
                })
                .await?;
            Ok(BenchDescribeReactiveHandle {
                actor: actor_for_handle,
                handle_key,
            })
        })
    }

    /// Subscribe to all-named-nodes message stream with auto-subscribe
    /// on late-added nodes (canonical R3.6.2 `observe(undefined, {
    /// reactive: true })`).
    #[napi]
    pub fn observe_all_reactive<'env>(
        &self,
        env: &'env Env,
        sink: Function<'_, FnArgs<(String, Vec<u32>)>, ()>,
    ) -> Result<PromiseRaw<'env, BenchObserveReactiveHandle>> {
        let tsfn = build_observe_all_tsfn(sink)?;
        let graph_key = self.graph_key;
        let handle_key = alloc_worker_key();
        let actor = Arc::clone(&self.actor);
        env.spawn_future(async move {
            let actor_for_handle = Arc::clone(&actor);
            actor
                .run(move |core| {
                    with_extra_install_child::<Graph, ObserveReactiveInner, _, _>(
                        graph_key,
                        handle_key,
                        move |g| {
                            let mut handle = g.observe_all_reactive();
                            handle.subscribe(core, move |name: &str, msgs: &[Message]| {
                                let payload = encode_messages(msgs);
                                let _status = tsfn.call(
                                    FnArgs::from((name.to_string(), payload)),
                                    ThreadsafeFunctionCallMode::NonBlocking,
                                );
                            });
                            (ObserveReactiveInner::All(handle), ())
                        },
                    )
                    .expect("BenchGraph: graph entry missing");
                })
                .await?;
            Ok(BenchObserveReactiveHandle {
                actor: actor_for_handle,
                handle_key,
            })
        })
    }

    /// Sink-style observe of a single node (canonical R3.6.2 default
    /// mode `observe(path)`). When `path` is `None`, fan-out snapshot
    /// across all named nodes (no auto-subscribe).
    #[napi]
    pub fn observe_subscribe<'env>(
        &self,
        env: &'env Env,
        path: Option<String>,
        sink: Function<'_, FnArgs<(String, Vec<u32>)>, ()>,
    ) -> Result<PromiseRaw<'env, BenchObserveReactiveHandle>> {
        let tsfn = build_observe_all_tsfn(sink)?;
        let graph_key = self.graph_key;
        let handle_key = alloc_worker_key();
        let actor = Arc::clone(&self.actor);
        env.spawn_future(async move {
            let actor_for_handle = Arc::clone(&actor);
            actor
                .run(move |core| {
                    with_extra_install_child::<Graph, ObserveReactiveInner, _, _>(
                        graph_key,
                        handle_key,
                        move |g| {
                            let inner = if let Some(path) = path {
                                let one = g.observe(&path);
                                let path_owned = path;
                                let sink: Sink = std::rc::Rc::new(move |msgs: &[Message]| {
                                    let payload = encode_messages(msgs);
                                    let _status = tsfn.call(
                                        FnArgs::from((path_owned.clone(), payload)),
                                        ThreadsafeFunctionCallMode::NonBlocking,
                                    );
                                });
                                // R3.6.2 sink-style observe: resolve to
                                // a node id, subscribe via Core, retain
                                // the (NodeId, SubscriptionId) pair for
                                // detach in dispose / Drop.
                                let node_id = one.node_id();
                                let sub_id = core.subscribe(node_id, sink);
                                ObserveReactiveInner::OneSub(node_id, sub_id)
                            } else {
                                let mut all = g.observe_all();
                                all.subscribe(core, move |name: &str, msgs: &[Message]| {
                                    let payload = encode_messages(msgs);
                                    let _status = tsfn.call(
                                        FnArgs::from((name.to_string(), payload)),
                                        ThreadsafeFunctionCallMode::NonBlocking,
                                    );
                                });
                                ObserveReactiveInner::AllSubs(all)
                            };
                            (inner, ())
                        },
                    )
                    .expect("BenchGraph: graph entry missing");
                })
                .await?;
            Ok(BenchObserveReactiveHandle {
                actor: actor_for_handle,
                handle_key,
            })
        })
    }
}

impl Drop for BenchGraph {
    /// Post a fire-and-forget closure to the actor that removes this
    /// BenchGraph's Graph from `WORKER_EXTRAS`, dropping the `!Send`
    /// Graph on the worker stack. If the actor has already shut down
    /// (the broader BenchCore lifetime is over), the dispatch is a
    /// no-op — the worker thread already terminated and dropped its
    /// entire WORKER_EXTRAS map along with all Graph entries.
    ///
    /// **Caller obligation (QA fix 2026-05-20 F8):** JS callers MUST
    /// `await dispose()` on every derived handle (BenchDescribeReactiveHandle,
    /// BenchObserveReactiveHandle, BenchStorageHandle) BEFORE
    /// dropping the parent BenchGraph. This Drop only releases the
    /// Graph entry — derived handles live in `WORKER_EXTRAS` under
    /// their own keys and hold `Rc<RefCell<GraphInner>>` clones; they
    /// keep the GraphInner alive (and their ns_sinks/topology subs
    /// active) until each handle's own Drop fires later. Without an
    /// explicit dispose order, the substrate's `#[must_use]` detach
    /// contract is honored on a per-handle basis but the timing is
    /// non-deterministic relative to the parent Graph's intended
    /// lifetime.
    fn drop(&mut self) {
        let key = self.graph_key;
        let _ = self.actor.dispatch_detached(move |_core| {
            let _ = take_extra::<Graph>(key);
        });
    }
}

// ---------------------------------------------------------------------------
// Reactive describe handle (Slice X2 — S6/D255 actor-routed)
// ---------------------------------------------------------------------------

/// Handle for a reactive describe subscription. JS code MUST `await
/// dispose()` before letting it drop — D246 r3 owner-invoked detach
/// (no RAII below the binding). `Drop` is a defense-in-depth
/// fire-and-forget detach via `dispatch_detached`.
#[napi]
pub struct BenchDescribeReactiveHandle {
    actor: Arc<CoreActor>,
    handle_key: u64,
}

#[napi]
impl BenchDescribeReactiveHandle {
    /// Owner-invoked synchronous detach (D246 r3). Idempotent —
    /// subsequent calls find no entry in `WORKER_EXTRAS` and no-op.
    #[napi]
    pub async fn dispose(&self) -> Result<()> {
        let key = self.handle_key;
        self.actor
            .run(move |core| {
                if let Some(rdh) = take_extra::<ReactiveDescribeHandle>(key) {
                    rdh.detach(core);
                }
            })
            .await
    }
}

impl Drop for BenchDescribeReactiveHandle {
    fn drop(&mut self) {
        // Defense-in-depth: if `dispose()` wasn't called, post the
        // detach fire-and-forget. If the actor is already shut down
        // (e.g., BenchCore dropped), the dispatch is a no-op and the
        // worker thread already dropped WORKER_EXTRAS with everything
        // inside.
        let key = self.handle_key;
        let _ = self.actor.dispatch_detached(move |core| {
            if let Some(rdh) = take_extra::<ReactiveDescribeHandle>(key) {
                rdh.detach(core);
            }
        });
    }
}

// ---------------------------------------------------------------------------
// Reactive / sink-style observe handle (Slice X2 — S6/D255 actor-routed)
//
// Single napi class with an internal enum so the JS adapter can
// dispose any observe variant (one-sub / all-subs / all-reactive)
// uniformly.
// ---------------------------------------------------------------------------

#[allow(dead_code)]
enum ObserveReactiveInner {
    /// Single-node sink-style observe — pairs a NodeId with the
    /// SubscriptionId returned by `Core::subscribe`. Detach calls
    /// `core.unsubscribe(node_id, sub_id)`.
    OneSub(NodeId, SubscriptionId),
    /// All-nodes snapshot fan-out — `GraphObserveAll` (no auto-sub).
    AllSubs(GraphObserveAll),
    /// All-nodes reactive (auto-subscribe on late additions).
    All(GraphObserveAllReactive),
}

impl ObserveReactiveInner {
    /// Owner-invoked detach. All three variants carry Core
    /// subscriptions that the substrate's `#[must_use]` contract
    /// requires to be released via an explicit `detach(&Core)` call
    /// (D246 r3 — no RAII below the binding):
    ///
    /// - `OneSub(node_id, sub_id)` → `core.unsubscribe(node_id, sub_id)`.
    /// - `AllSubs(GraphObserveAll)` → `a.detach(core)` releases every
    ///   per-name fan-out sub it accumulated. Without this,
    ///   `Vec<(NodeId, SubscriptionId)>` leaks for the Core lifetime
    ///   (substrate doc: "you MUST call detach(core) or they leak"
    ///   at `crates/graphrefly-graph/src/observe.rs`).
    /// - `All(GraphObserveAllReactive)` → `r.detach(core)` releases
    ///   the topology sub, the namespace-change sink, AND every fan-
    ///   out sub the auto-subscribe path opened. Same `#[must_use]`
    ///   contract.
    ///
    /// QA fix 2026-05-20 (Edge Case Hunter F1 + Blind Hunter C1):
    /// the prior implementation dropped the `AllSubs`/`All` arms
    /// silently with a comment that misread the substrate contract.
    fn detach(self, core: &graphrefly_core::Core) {
        match self {
            Self::OneSub(node_id, sub_id) => core.unsubscribe(node_id, sub_id),
            Self::AllSubs(mut a) => a.detach(core),
            Self::All(mut r) => r.detach(core),
        }
    }
}

/// Handle for an observe subscription (single-node, all-nodes
/// snapshot, or all-nodes reactive auto-subscribe). JS code SHOULD
/// `await dispose()` — D246 r3 owner-invoked detach.
#[napi]
pub struct BenchObserveReactiveHandle {
    actor: Arc<CoreActor>,
    handle_key: u64,
}

#[napi]
impl BenchObserveReactiveHandle {
    #[napi]
    pub async fn dispose(&self) -> Result<()> {
        let key = self.handle_key;
        self.actor
            .run(move |core| {
                if let Some(inner) = take_extra::<ObserveReactiveInner>(key) {
                    inner.detach(core);
                }
            })
            .await
    }
}

impl Drop for BenchObserveReactiveHandle {
    fn drop(&mut self) {
        let key = self.handle_key;
        let _ = self.actor.dispatch_detached(move |core| {
            if let Some(inner) = take_extra::<ObserveReactiveInner>(key) {
                inner.detach(core);
            }
        });
    }
}

// ---------------------------------------------------------------------------
// TSFN builders for reactive surfaces (Slice X2)
// ---------------------------------------------------------------------------

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

fn build_describe_tsfn(callback: Function<'_, String, ()>) -> Result<Arc<DescribeTsfn>> {
    let tsfn = callback
        .build_threadsafe_function::<String>()
        .max_queue_size::<8>()
        .callee_handled::<false>()
        .build_callback(|ctx: ThreadsafeCallContext<String>| Ok(ctx.value))?;
    Ok(Arc::new(tsfn))
}

fn build_observe_all_tsfn(
    callback: Function<'_, FnArgs<(String, Vec<u32>)>, ()>,
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
