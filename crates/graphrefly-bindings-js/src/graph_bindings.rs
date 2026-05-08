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

use graphrefly_core::{BindingBoundary, Core, EqualsMode, FnId, HandleId, LockId, NodeId};
use graphrefly_graph::{Graph, NameError, RemoveError, SignalKind};
use napi::bindgen_prelude::*;
use napi::Error as NapiError;
use napi_derive::napi;

use crate::core_bindings::{
    run_blocking, BenchBinding, BenchCore, BuiltinFn, MSG_CODE_COMPLETE, MSG_CODE_DATA,
    MSG_CODE_DIRTY, MSG_CODE_ERROR, MSG_CODE_INVALIDATE, MSG_CODE_PAUSE, MSG_CODE_RESOLVED,
    MSG_CODE_RESUME, MSG_CODE_START, MSG_CODE_TEARDOWN,
};

/// JS-facing wrapper around a `graphrefly_graph::Graph`. Each
/// `BenchGraph` shares its `BenchCore`'s value registry + binding so
/// nodes registered through `BenchGraph::state(...)` are visible to
/// `BenchOperators::register_*(...)` and vice-versa.
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
