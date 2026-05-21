//! `BenchOperators` napi class — operator factory surface for JS callers.
//!
//! M3 napi-rs operator parity slice. Locked design in
//! `~/src/graphrefly-ts/archive/docs/SESSION-rust-port-napi-operators.md`
//! (D049–D053 + D062–D066, with D062/D063 superseded by Option E
//! [D070], and D072 supersedes the napi 2.x pin → clean napi 3.x).
//! Build profile: only enabled with the `operators` feature.
//!
//! # Layout
//!
//! 1. **Trait impls** — `OperatorBinding` / `HigherOrderBinding` /
//!    `ProducerBinding` for `BenchBinding`. Each `register_*` method
//!    inserts the boxed closure into one of `Registry`'s closure
//!    registries.
//!
//! 2. **TSFN bridge** — `bridge_sync<T, R>(tsfn, arg) -> R` blocks the
//!    *tokio blocking-pool thread* (NOT the JS thread) on
//!    `mpsc::sync_channel(1)` while the TSFN result-handler delivers
//!    the JS callback's return value via libuv. napi 3.x's cb signature
//!    is `FnOnce(Result<Return>, Env) -> Result<()>` — JS-callback
//!    throws are delivered as `Err` to our cb, which logs and sends a
//!    sentinel through the channel. Per D072, this is the native C1
//!    fix (no Rust-built JS wrapper needed unlike napi 2.x).
//!
//! 3. **`BenchOperators` napi class** — 22 `register_*` methods (one
//!    per operator). Methods that take a JS callback are `pub fn ...
//!    -> Result<PromiseRaw<'env, u32>>` and use `Env::spawn_future` to
//!    return a Promise. Methods without callbacks stay as `async fn`.
//!
//! # Re-entrance
//!
//! JS callbacks executing inside a TSFN tick MUST NOT call back into
//! `BenchCore` / `BenchOperators` synchronously — they should `await`
//! the nested call. With Promise-returning napi methods + tokio's
//! blocking pool, awaited re-entries are non-deadlocking.

use std::sync::mpsc::sync_channel;
use std::sync::{Arc, Weak};

use graphrefly_core::{DepBatch, EqualsMode, FnId, HandleId, NodeId};
use graphrefly_operators::{
    binding::OperatorBinding,
    buffer,
    combine::{self, PackerFn},
    control,
    error::OperatorFactoryError,
    flow,
    higher_order::{self, HigherOrderBinding, ProjectFn},
    ops_impl,
    producer::{ProducerBinding, ProducerBuildFn, ProducerStorage},
    source, stratify, temporal, transform,
};
use napi::bindgen_prelude::*;
use napi::threadsafe_function::{
    ThreadsafeCallContext, ThreadsafeFunction, ThreadsafeFunctionCallMode,
};
use napi::{Env, Error as NapiError};
use napi_derive::napi;

use crate::core_actor::CoreActor;
use crate::core_bindings::{BenchBinding, BenchCore};

// =====================================================================
// Trait impls — closure registration for the operators crate
// =====================================================================

impl OperatorBinding for BenchBinding {
    fn register_projector(&self, f: Box<dyn Fn(HandleId) -> HandleId + Send + Sync>) -> FnId {
        let mut reg = self.registry.lock();
        let fn_id = FnId::new(reg.next_fn_id);
        reg.next_fn_id += 1;
        reg.projectors.insert(fn_id, Arc::from(f));
        fn_id
    }

    fn register_predicate(&self, f: Box<dyn Fn(HandleId) -> bool + Send + Sync>) -> FnId {
        let mut reg = self.registry.lock();
        let fn_id = FnId::new(reg.next_fn_id);
        reg.next_fn_id += 1;
        reg.predicates.insert(fn_id, Arc::from(f));
        fn_id
    }

    fn register_folder(
        &self,
        f: Box<dyn Fn(HandleId, HandleId) -> HandleId + Send + Sync>,
    ) -> FnId {
        let mut reg = self.registry.lock();
        let fn_id = FnId::new(reg.next_fn_id);
        reg.next_fn_id += 1;
        reg.folders.insert(fn_id, Arc::from(f));
        fn_id
    }

    fn register_equals(&self, f: Box<dyn Fn(HandleId, HandleId) -> bool + Send + Sync>) -> FnId {
        let mut reg = self.registry.lock();
        let fn_id = FnId::new(reg.next_fn_id);
        reg.next_fn_id += 1;
        reg.equalses.insert(fn_id, Arc::from(f));
        fn_id
    }

    fn register_pairwise_packer(
        &self,
        f: Box<dyn Fn(HandleId, HandleId) -> HandleId + Send + Sync>,
    ) -> FnId {
        let mut reg = self.registry.lock();
        let fn_id = FnId::new(reg.next_fn_id);
        reg.next_fn_id += 1;
        reg.pairwise_packers.insert(fn_id, Arc::from(f));
        fn_id
    }

    fn register_packer(&self, f: PackerFn) -> FnId {
        let mut reg = self.registry.lock();
        let fn_id = FnId::new(reg.next_fn_id);
        reg.next_fn_id += 1;
        reg.packers.insert(fn_id, Arc::from(f));
        fn_id
    }

    fn register_stratify_classifier(
        &self,
        f: Box<dyn Fn(HandleId, HandleId) -> bool + Send + Sync>,
    ) -> FnId {
        let mut reg = self.registry.lock();
        let fn_id = FnId::new(reg.next_fn_id);
        reg.next_fn_id += 1;
        reg.stratify_classifiers.insert(fn_id, Arc::from(f));
        fn_id
    }
}

impl ProducerBinding for BenchBinding {
    fn register_producer_build(&self, build: ProducerBuildFn) -> FnId {
        let mut reg = self.registry.lock();
        let fn_id = FnId::new(reg.next_fn_id);
        reg.next_fn_id += 1;
        reg.producer_builds.insert(fn_id, Arc::from(build));
        fn_id
    }

    fn producer_storage(&self) -> &ProducerStorage {
        &self.producer_storage
    }
}

impl HigherOrderBinding for BenchBinding {
    fn register_project(&self, project: ProjectFn) -> FnId {
        let mut reg = self.registry.lock();
        let fn_id = FnId::new(reg.next_fn_id);
        reg.next_fn_id += 1;
        reg.higher_order_projectors
            .insert(fn_id, Arc::from(project));
        fn_id
    }

    fn invoke_project(&self, fn_id: FnId, value: HandleId) -> NodeId {
        let f = {
            let reg = self.registry.lock();
            reg.higher_order_projectors
                .get(&fn_id)
                .cloned()
                .unwrap_or_else(|| panic!("unknown higher-order project fn_id {fn_id:?}"))
        };
        f(value)
    }
}

// =====================================================================
// TSFN bridge — sync-return wrapper (napi 3.x cb signature)
// =====================================================================
//
// **napi 3.x C1 fix.** TSFN cb receives `Result<Return>` directly:
// JS-callback throws arrive as `Err(napi::Error)`, NOT as a fatal
// abort. Our cb logs the throw, sends a sentinel through the channel,
// and returns Ok(()) so napi doesn't treat it as a panic. The bridge
// then panics on its caller side via the channel collapse path —
// caught by Core's BatchGuard::drop wave-discard discipline (D061).
//
// `CalleeHandled = true` (default) — required so the cb sees the Err
// arm. `Weak = false` (we want the TSFN to keep the JS event loop
// alive while a wave is in flight). `MaxQueueSize = 1` (M9 — sync
// bridge invariant, at most one in-flight call per TSFN).

/// TSFN type alias — 7 const generics: `T = input`, `Return = output`,
/// `CallJsBackArgs = T` (pass-through), `ErrorStatus = Status`,
/// `CalleeHandled = false`, `Weak = false`, `MaxQueueSize = 1`.
///
/// **`CalleeHandled = false`** (Slice Y, 2026-05-08): napi-rs 3.x with
/// `callee_handled::<true>()` makes JS receive `(err, value)` Node-style
/// — even though the typed `Function<'_, u32, u32>` declares the logical
/// signature as `(u32) -> u32`. With `false`, JS receives `(value)`
/// directly, matching the typed signature and `makeProjector`-style
/// adapter shapes (which take `(h: number) => ...`). JS-throw delivery
/// via `Result<R>` to the result handler is preserved (the
/// `FnOnce(Result<Return>, Env) -> Result<()>` cb signature is the
/// same in both `CalleeHandled` impls per napi-rs 3.x source).
/// Pre-Slice-Y verified empirically: `(h) => h` actually got
/// `(null, 1)` as args because `callee_handled::<true>()` was the
/// wire shape; this silently broke the rustImpl parity arm
/// (makeProjector's `registry.get(h)` looked up `null`).
type Tsfn<T, R> = ThreadsafeFunction<T, R, T, Status, false, false, 1>;

/// Block the calling tokio blocking-pool thread until the TSFN's JS
/// callback returns a value of type `R`. JS-callback throws are
/// delivered to our cb as `Err`; the cb sends a per-shape sentinel
/// (caller-supplied) so the wave can discard cleanly via Core's
/// BatchGuard panic path. Status≠Ok also panics (TSFN aborted /
/// closed during shutdown).
fn bridge_sync<T, R>(tsfn: &Arc<Tsfn<T, R>>, arg: T) -> R
where
    T: 'static + Send + JsValuesTupleIntoVec,
    R: FromNapiValue + Send + 'static,
{
    let (tx, rx) = sync_channel::<Result<R>>(1);
    // CalleeHandled=false → call_with_return_value takes plain `T`
    // (no Result wrapping). JS-throw delivery via the cb's `Result<R>`
    // is preserved — the result handler's signature is identical
    // between the two CalleeHandled impls in napi-rs 3.x.
    let status = tsfn.call_with_return_value(
        arg,
        ThreadsafeFunctionCallMode::Blocking,
        move |result: Result<R>, _env: Env| -> Result<()> {
            // C1 fix (napi 3.x native): JS throw delivered as Err.
            // Forward through channel as Result; caller handles.
            // Return Ok(()) so napi doesn't treat us as panicked.
            let _ = tx.send(result);
            Ok(())
        },
    );
    if status != Status::Ok {
        panic!(
            "TSFN call_with_return_value failed with status {status:?} (likely TSFN closed during shutdown). \
             Wave panic-discarded via Core's BatchGuard drop discipline."
        );
    }
    match rx.recv() {
        Ok(Ok(val)) => val,
        Ok(Err(e)) => {
            eprintln!("[graphrefly] JS callback threw: {e}");
            panic!(
                "JS callback threw: {e}. Wave panic-discarded via Core's BatchGuard drop discipline."
            );
        }
        Err(_) => panic!(
            "TSFN result channel closed without delivering a value. \
             Wave panic-discarded via Core's BatchGuard drop discipline."
        ),
    }
}

// =====================================================================
// `BenchOperators` napi class
//
// **`async fn` vs `pub fn` shape (H-13 doc note):** napi methods that
// take a `Function<'_, Args, Return>` parameter are `pub fn ...
// -> Result<PromiseRaw<'_, u32>>` and use `env.spawn_future(async move
// { ... })` to return a Promise. Reason: `Function<'_, Args, Return>` is
// `!Send` (lifetime tied to env), so an `async fn` whose parameter
// list includes one produces a `!Send` future, which napi-rs's
// auto-Promise-wrap rejects (it requires `Future: Send`). Methods
// without `Function<'_, >` (e.g., `register_take` taking just a
// `count: u32`) stay as `async fn`. From JS, both shapes return
// Promises and are equivalent under `await`.
// =====================================================================

#[napi]
pub struct BenchOperators {
    pub(crate) actor: Arc<CoreActor>,
    pub(crate) binding: Arc<BenchBinding>,
}

#[napi]
impl BenchOperators {
    /// Build a `BenchOperators` companion that shares the supplied
    /// `BenchCore`'s value registry + Core. Registrations on either
    /// class are visible to the other.
    ///
    /// **F14 invariant (restored 2026-05-20 QA F9/m12, preserved by
    /// construction):** the `binding: Arc<BenchBinding>` field and
    /// the actor's Core's `BindingBoundary` upcast come from the
    /// SAME `Arc<BenchBinding>` — both `core.actor_arc()` and
    /// `core.binding_arc()` clone fields owned by the supplied
    /// `BenchCore`. `BenchCore::new` is the single construction path
    /// for that Arc; no public API mints a divergent binding.
    #[napi(factory)]
    #[must_use]
    pub fn from_core(core: &BenchCore) -> Self {
        Self {
            actor: core.actor_arc(),
            binding: core.binding_arc(),
        }
    }

    // -----------------------------------------------------------------
    // D263/D264 — generic user-derived TSFN registration (parity slice
    // for the `setDeps`/`addDep`/`removeDep` rewire surface). The JS
    // callback receives the per-dep wave handle arrays
    // (`(deps: number[][]) => number[]`) and returns a flat list of
    // output handles (each becomes one `FnEmission::Data` in this
    // wave). JS-side handles are minted via `core.allocExternalHandle`
    // (retain=1) so no retain-bump is needed Rust-side — the closure
    // forwards each returned handle straight to `FnEmission::Data` and
    // Core takes ownership of the share. Used by `wrapper.js`'s
    // `impl.map` reroute so `NativeNode.setDeps`/`addDep`/`removeDep`
    // can rebind the JS closure cell + call `Core::set_deps` without
    // a substrate widening (D264 P1).
    // -----------------------------------------------------------------

    /// `register_user_derived(deps, fn)` — register a derived node whose
    /// fn is a generic JS callback of shape
    /// `(deps: number[][]) => number[]`. Each input `deps[i]` is the
    /// array of DATA handles delivered for dep `i` this wave (may be
    /// empty for a dep that did not deliver). Each output handle becomes
    /// one `FnEmission::Data` in this wave. Output handles MUST be minted
    /// via `BenchCore.allocExternalHandle` so the JS-side adapter owns
    /// the share that Core takes ownership of.
    #[napi]
    pub fn register_user_derived<'env>(
        &self,
        env: &'env Env,
        dep_ids: Vec<u32>,
        callback: Function<'_, Vec<Vec<u32>>, Vec<u32>>,
    ) -> Result<PromiseRaw<'env, Vec<u32>>> {
        let tsfn = build_vec_vec_to_vec_tsfn(callback)?;
        let user_closure = closure_user_derived(tsfn);
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        env.spawn_future(async move {
            actor
                .run(move |core| -> Result<Vec<u32>> {
                    let fn_id = {
                        let mut reg = binding.registry.lock();
                        let id = FnId::new(reg.next_fn_id);
                        reg.next_fn_id += 1;
                        reg.user_derived_fns.insert(id, Arc::from(user_closure));
                        id
                    };
                    let deps: Vec<NodeId> = dep_ids
                        .into_iter()
                        .map(|d| NodeId::new(u64::from(d)))
                        .collect();
                    let node_id = core
                        .register_derived(&deps, fn_id, EqualsMode::Identity, false)
                        .map_err(|e| NapiError::from_reason(format!("{e}")))?;
                    let node_u32 = u32::try_from(node_id.raw())
                        .map_err(|_| NapiError::from_reason("node id exceeds u32"))?;
                    let fn_u32 = u32::try_from(fn_id.raw())
                        .map_err(|_| NapiError::from_reason("fn id exceeds u32"))?;
                    Ok(vec![node_u32, fn_u32])
                })
                .await?
        })
    }

    /// Rebind the TSFN closure backing an existing `register_user_derived`
    /// node so a subsequent `Core::set_deps` lands against the new fn.
    /// Used by `NativeNode.setDeps`/`addDep`/`removeDep` to atomically
    /// swap the operator fn alongside the dep-shape change (D264).
    ///
    /// Returns the same `fn_id` (passed back as the same `u32`) that the
    /// node was registered with — JS doesn't need it since the rewire
    /// surface threads the binding through the node-id ↔ fn-id mapping
    /// it keeps in the wrapper, but exposing it keeps the surface
    /// debug-friendly.
    ///
    /// # Errors
    ///
    /// Returns `Err` if `fn_id` is not a known user-derived fn.
    #[napi]
    pub fn rebind_user_derived<'env>(
        &self,
        env: &'env Env,
        fn_id: u32,
        callback: Function<'_, Vec<Vec<u32>>, Vec<u32>>,
    ) -> Result<PromiseRaw<'env, u32>> {
        let tsfn = build_vec_vec_to_vec_tsfn(callback)?;
        let user_closure = closure_user_derived(tsfn);
        let binding = Arc::clone(&self.binding);
        let fid = FnId::new(u64::from(fn_id));
        env.spawn_future(async move {
            let mut reg = binding.registry.lock();
            if !reg.user_derived_fns.contains_key(&fid) {
                return Err(NapiError::from_reason(format!(
                    "rebind_user_derived: unknown fn_id {fn_id}",
                )));
            }
            reg.user_derived_fns.insert(fid, Arc::from(user_closure));
            Ok(fn_id)
        })
    }

    // -----------------------------------------------------------------
    // Transform operators (Slice C-1)
    // -----------------------------------------------------------------

    /// `map(src, project)` — JS callback `(handle: number) => number`
    /// returns the new HandleId (typically built via
    /// `BenchCore::intern_int(...)` from inside the callback).
    #[napi]
    pub fn register_map<'env>(
        &self,
        env: &'env Env,
        src: u32,
        project: Function<'_, u32, u32>,
    ) -> Result<PromiseRaw<'env, u32>> {
        let tsfn = build_h_to_h_tsfn(project)?;
        let projector_closure = closure_h_to_h(tsfn, Arc::downgrade(&self.binding));
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let src_id = NodeId::new(u64::from(src));
        env.spawn_future(async move {
            actor
                .run(move |core| -> Result<u32> {
                    let op_binding: Arc<dyn OperatorBinding> = binding;
                    let registration = transform::map(core, &op_binding, src_id, projector_closure);
                    u32::try_from(registration.node.raw())
                        .map_err(|_| NapiError::from_reason("node id exceeds u32"))
                })
                .await?
        })
    }

    /// `filter(src, predicate)` — JS: `(handle: number) => boolean`.
    #[napi]
    pub fn register_filter<'env>(
        &self,
        env: &'env Env,
        src: u32,
        predicate: Function<'_, u32, bool>,
    ) -> Result<PromiseRaw<'env, u32>> {
        let tsfn = build_h_to_bool_tsfn(predicate)?;
        let predicate_closure = closure_h_to_bool(tsfn);
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let src_id = NodeId::new(u64::from(src));
        env.spawn_future(async move {
            actor
                .run(move |core| -> Result<u32> {
                    let op_binding: Arc<dyn OperatorBinding> = binding;
                    let registration =
                        transform::filter(core, &op_binding, src_id, predicate_closure);
                    u32::try_from(registration.node.raw())
                        .map_err(|_| NapiError::from_reason("node id exceeds u32"))
                })
                .await?
        })
    }

    /// `scan(src, seed, folder)` — JS folder: `(acc, value) => acc'`.
    #[napi]
    pub fn register_scan<'env>(
        &self,
        env: &'env Env,
        src: u32,
        seed: u32,
        folder: Function<'_, FnArgs<(u32, u32)>, u32>,
    ) -> Result<PromiseRaw<'env, u32>> {
        let tsfn = build_hh_to_h_tsfn(folder)?;
        let folder_closure = closure_hh_to_h(tsfn, Arc::downgrade(&self.binding));
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let src_id = NodeId::new(u64::from(src));
        let seed_h = HandleId::new(u64::from(seed));
        env.spawn_future(async move {
            actor
                .run(move |core| -> Result<u32> {
                    let op_binding: Arc<dyn OperatorBinding> = binding;
                    let registration =
                        transform::scan(core, &op_binding, src_id, folder_closure, seed_h);
                    u32::try_from(registration.node.raw())
                        .map_err(|_| NapiError::from_reason("node id exceeds u32"))
                })
                .await?
        })
    }

    /// `reduce(src, seed, folder)` — emit only on upstream COMPLETE.
    #[napi]
    pub fn register_reduce<'env>(
        &self,
        env: &'env Env,
        src: u32,
        seed: u32,
        folder: Function<'_, FnArgs<(u32, u32)>, u32>,
    ) -> Result<PromiseRaw<'env, u32>> {
        let tsfn = build_hh_to_h_tsfn(folder)?;
        let folder_closure = closure_hh_to_h(tsfn, Arc::downgrade(&self.binding));
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let src_id = NodeId::new(u64::from(src));
        let seed_h = HandleId::new(u64::from(seed));
        env.spawn_future(async move {
            actor
                .run(move |core| -> Result<u32> {
                    let op_binding: Arc<dyn OperatorBinding> = binding;
                    let registration =
                        transform::reduce(core, &op_binding, src_id, folder_closure, seed_h);
                    u32::try_from(registration.node.raw())
                        .map_err(|_| NapiError::from_reason("node id exceeds u32"))
                })
                .await?
        })
    }

    /// `distinctUntilChanged(src)` — identity equals.
    #[napi]
    pub async fn register_distinct_until_changed(&self, src: u32) -> Result<u32> {
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let src_id = NodeId::new(u64::from(src));
        let identity_closure: Box<dyn Fn(HandleId, HandleId) -> bool + Send + Sync> =
            Box::new(|a, b| a == b);
        actor
            .run(move |core| -> Result<u32> {
                let op_binding: Arc<dyn OperatorBinding> = binding;
                let registration =
                    transform::distinct_until_changed(core, &op_binding, src_id, identity_closure);
                u32::try_from(registration.node.raw())
                    .map_err(|_| NapiError::from_reason("node id exceeds u32"))
            })
            .await?
    }

    /// `distinctUntilChanged(src, equals)` — custom JS equals.
    #[napi]
    pub fn register_distinct_until_changed_with<'env>(
        &self,
        env: &'env Env,
        src: u32,
        equals: Function<'_, FnArgs<(u32, u32)>, bool>,
    ) -> Result<PromiseRaw<'env, u32>> {
        let tsfn = build_hh_to_bool_tsfn(equals)?;
        let equals_closure = closure_hh_to_bool(tsfn);
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let src_id = NodeId::new(u64::from(src));
        env.spawn_future(async move {
            actor
                .run(move |core| -> Result<u32> {
                    let op_binding: Arc<dyn OperatorBinding> = binding;
                    let registration = transform::distinct_until_changed(
                        core,
                        &op_binding,
                        src_id,
                        equals_closure,
                    );
                    u32::try_from(registration.node.raw())
                        .map_err(|_| NapiError::from_reason("node id exceeds u32"))
                })
                .await?
        })
    }

    /// `pairwise(src, packer)` — packer: `(prev, current) => HandleId`.
    #[napi]
    pub fn register_pairwise<'env>(
        &self,
        env: &'env Env,
        src: u32,
        packer: Function<'_, FnArgs<(u32, u32)>, u32>,
    ) -> Result<PromiseRaw<'env, u32>> {
        let tsfn = build_hh_to_h_tsfn(packer)?;
        let packer_closure = closure_hh_to_h(tsfn, Arc::downgrade(&self.binding));
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let src_id = NodeId::new(u64::from(src));
        env.spawn_future(async move {
            actor
                .run(move |core| -> Result<u32> {
                    let op_binding: Arc<dyn OperatorBinding> = binding;
                    let registration =
                        transform::pairwise(core, &op_binding, src_id, packer_closure);
                    u32::try_from(registration.node.raw())
                        .map_err(|_| NapiError::from_reason("node id exceeds u32"))
                })
                .await?
        })
    }

    // -----------------------------------------------------------------
    // Combine operators (Slice C-2)
    // -----------------------------------------------------------------

    /// `combine(srcs, packer)` — N-ary combine with all-deps-real gate.
    #[napi]
    pub fn register_combine<'env>(
        &self,
        env: &'env Env,
        srcs: Vec<u32>,
        packer: Function<'_, Vec<u32>, u32>,
    ) -> Result<PromiseRaw<'env, u32>> {
        let tsfn = build_vec_to_h_tsfn(packer)?;
        let packer_closure = closure_packer(tsfn, Arc::downgrade(&self.binding));
        let binding = Arc::clone(&self.binding);
        let src_ids: Vec<NodeId> = srcs
            .into_iter()
            .map(|s| NodeId::new(u64::from(s)))
            .collect();
        let actor = Arc::clone(&self.actor);
        env.spawn_future(async move {
            let result = actor
                .run(move |core| {
                    let op_binding: Arc<dyn OperatorBinding> = binding;
                    combine::combine(core, &op_binding, &src_ids, packer_closure)
                })
                .await?;
            result
                .map_err(operator_factory_error_to_napi)
                .and_then(|reg| {
                    u32::try_from(reg.node.raw())
                        .map_err(|_| NapiError::from_reason("node id exceeds u32"))
                })
        })
    }

    /// `withLatestFrom(primary, secondary, packer)` — fire-on-primary.
    #[napi]
    pub fn register_with_latest_from<'env>(
        &self,
        env: &'env Env,
        primary: u32,
        secondary: u32,
        packer: Function<'_, Vec<u32>, u32>,
    ) -> Result<PromiseRaw<'env, u32>> {
        let tsfn = build_vec_to_h_tsfn(packer)?;
        let packer_closure = closure_packer(tsfn, Arc::downgrade(&self.binding));
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let primary_id = NodeId::new(u64::from(primary));
        let secondary_id = NodeId::new(u64::from(secondary));
        env.spawn_future(async move {
            actor
                .run(move |core| -> Result<u32> {
                    let op_binding: Arc<dyn OperatorBinding> = binding;
                    let registration = combine::with_latest_from(
                        core,
                        &op_binding,
                        primary_id,
                        secondary_id,
                        packer_closure,
                    );
                    u32::try_from(registration.node.raw())
                        .map_err(|_| NapiError::from_reason("node id exceeds u32"))
                })
                .await?
        })
    }

    /// `merge(srcs)` — no callbacks; emit-as-arrives.
    #[napi]
    pub async fn register_merge(&self, srcs: Vec<u32>) -> Result<u32> {
        let actor = Arc::clone(&self.actor);
        let src_ids: Vec<NodeId> = srcs
            .into_iter()
            .map(|s| NodeId::new(u64::from(s)))
            .collect();
        let result = actor
            .run(move |core| combine::merge(core, &src_ids))
            .await?;
        result
            .map_err(operator_factory_error_to_napi)
            .and_then(|reg| {
                u32::try_from(reg.node.raw())
                    .map_err(|_| NapiError::from_reason("node id exceeds u32"))
            })
    }

    // -----------------------------------------------------------------
    // Flow operators (Slice C-3)
    // -----------------------------------------------------------------

    #[napi]
    pub async fn register_take(&self, src: u32, count: u32) -> Result<u32> {
        let actor = Arc::clone(&self.actor);
        let src_id = NodeId::new(u64::from(src));
        actor
            .run(move |core| -> Result<u32> {
                let registration = flow::take(core, src_id, count);
                u32::try_from(registration.node.raw())
                    .map_err(|_| NapiError::from_reason("node id exceeds u32"))
            })
            .await?
    }

    #[napi]
    pub async fn register_skip(&self, src: u32, count: u32) -> Result<u32> {
        let actor = Arc::clone(&self.actor);
        let src_id = NodeId::new(u64::from(src));
        actor
            .run(move |core| -> Result<u32> {
                let registration = flow::skip(core, src_id, count);
                u32::try_from(registration.node.raw())
                    .map_err(|_| NapiError::from_reason("node id exceeds u32"))
            })
            .await?
    }

    #[napi]
    pub fn register_take_while<'env>(
        &self,
        env: &'env Env,
        src: u32,
        predicate: Function<'_, u32, bool>,
    ) -> Result<PromiseRaw<'env, u32>> {
        let tsfn = build_h_to_bool_tsfn(predicate)?;
        let predicate_closure = closure_h_to_bool(tsfn);
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let src_id = NodeId::new(u64::from(src));
        env.spawn_future(async move {
            actor
                .run(move |core| -> Result<u32> {
                    let op_binding: Arc<dyn OperatorBinding> = binding;
                    let registration =
                        flow::take_while(core, &op_binding, src_id, predicate_closure);
                    u32::try_from(registration.node.raw())
                        .map_err(|_| NapiError::from_reason("node id exceeds u32"))
                })
                .await?
        })
    }

    #[napi]
    pub async fn register_last(&self, src: u32) -> Result<u32> {
        let actor = Arc::clone(&self.actor);
        let src_id = NodeId::new(u64::from(src));
        actor
            .run(move |core| -> Result<u32> {
                let registration = flow::last(core, src_id);
                u32::try_from(registration.node.raw())
                    .map_err(|_| NapiError::from_reason("node id exceeds u32"))
            })
            .await?
    }

    #[napi]
    pub async fn register_last_with_default(&self, src: u32, default_handle: u32) -> Result<u32> {
        let actor = Arc::clone(&self.actor);
        let src_id = NodeId::new(u64::from(src));
        let default_h = HandleId::new(u64::from(default_handle));
        let result = actor
            .run(move |core| flow::last_with_default(core, src_id, default_h))
            .await?;
        result
            .map_err(operator_factory_error_to_napi)
            .and_then(|reg| {
                u32::try_from(reg.node.raw())
                    .map_err(|_| NapiError::from_reason("node id exceeds u32"))
            })
    }

    #[napi]
    pub async fn register_first(&self, src: u32) -> Result<u32> {
        let actor = Arc::clone(&self.actor);
        let src_id = NodeId::new(u64::from(src));
        actor
            .run(move |core| -> Result<u32> {
                let registration = flow::first(core, src_id);
                u32::try_from(registration.node.raw())
                    .map_err(|_| NapiError::from_reason("node id exceeds u32"))
            })
            .await?
    }

    #[napi]
    pub fn register_find<'env>(
        &self,
        env: &'env Env,
        src: u32,
        predicate: Function<'_, u32, bool>,
    ) -> Result<PromiseRaw<'env, u32>> {
        let tsfn = build_h_to_bool_tsfn(predicate)?;
        let predicate_closure = closure_h_to_bool(tsfn);
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let src_id = NodeId::new(u64::from(src));
        env.spawn_future(async move {
            actor
                .run(move |core| -> Result<u32> {
                    let op_binding: Arc<dyn OperatorBinding> = binding;
                    let registration = flow::find(core, &op_binding, src_id, predicate_closure);
                    u32::try_from(registration.node.raw())
                        .map_err(|_| NapiError::from_reason("node id exceeds u32"))
                })
                .await?
        })
    }

    #[napi]
    pub async fn register_element_at(&self, src: u32, index: u32) -> Result<u32> {
        let actor = Arc::clone(&self.actor);
        let src_id = NodeId::new(u64::from(src));
        actor
            .run(move |core| -> Result<u32> {
                let registration = flow::element_at(core, src_id, index);
                u32::try_from(registration.node.raw())
                    .map_err(|_| NapiError::from_reason("node id exceeds u32"))
            })
            .await?
    }

    // -----------------------------------------------------------------
    // Subscription-managed combinators (Slice D-ops)
    // -----------------------------------------------------------------

    #[napi]
    pub fn register_zip<'env>(
        &self,
        env: &'env Env,
        srcs: Vec<u32>,
        packer: Function<'_, Vec<u32>, u32>,
    ) -> Result<PromiseRaw<'env, u32>> {
        let tsfn = build_vec_to_h_tsfn(packer)?;
        let packer_closure = closure_packer(tsfn, Arc::downgrade(&self.binding));
        let binding = Arc::clone(&self.binding);
        let src_ids: Vec<NodeId> = srcs
            .into_iter()
            .map(|s| NodeId::new(u64::from(s)))
            .collect();
        let actor = Arc::clone(&self.actor);
        env.spawn_future(async move {
            actor
                .run(move |core| -> Result<u32> {
                    let op_binding: Arc<dyn OperatorBinding> =
                        Arc::clone(&binding) as Arc<dyn OperatorBinding>;
                    let pack_fn_id = op_binding.register_packer(packer_closure);
                    let producer_binding: Arc<dyn ProducerBinding> = binding;
                    // R5.7.x — `ops_impl::zip` returns
                    // `Err(OperatorFactoryError::EmptySources)` when sources is
                    // empty. Translate to NapiError via the shared converter.
                    let node = ops_impl::zip(core, &producer_binding, src_ids, pack_fn_id)
                        .map_err(operator_factory_error_to_napi)?;
                    u32::try_from(node.raw())
                        .map_err(|_| NapiError::from_reason("node id exceeds u32"))
                })
                .await?
        })
    }

    #[napi]
    pub async fn register_concat(&self, first: u32, second: u32) -> Result<u32> {
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let first_id = NodeId::new(u64::from(first));
        let second_id = NodeId::new(u64::from(second));
        actor
            .run(move |core| -> Result<u32> {
                let producer_binding: Arc<dyn ProducerBinding> = binding;
                let node = ops_impl::concat(core, &producer_binding, first_id, second_id);
                u32::try_from(node.raw()).map_err(|_| NapiError::from_reason("node id exceeds u32"))
            })
            .await?
    }

    #[napi]
    pub async fn register_race(&self, srcs: Vec<u32>) -> Result<u32> {
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let src_ids: Vec<NodeId> = srcs
            .into_iter()
            .map(|s| NodeId::new(u64::from(s)))
            .collect();
        actor
            .run(move |core| -> Result<u32> {
                let producer_binding: Arc<dyn ProducerBinding> = binding;
                // R5.7.x — `ops_impl::race` returns
                // `Err(OperatorFactoryError::EmptySources)` when sources is
                // empty. Translate to NapiError via the shared converter.
                let node = ops_impl::race(core, &producer_binding, src_ids)
                    .map_err(operator_factory_error_to_napi)?;
                u32::try_from(node.raw()).map_err(|_| NapiError::from_reason("node id exceeds u32"))
            })
            .await?
    }

    #[napi]
    pub async fn register_take_until(&self, src: u32, notifier: u32) -> Result<u32> {
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let src_id = NodeId::new(u64::from(src));
        let notifier_id = NodeId::new(u64::from(notifier));
        actor
            .run(move |core| -> Result<u32> {
                let producer_binding: Arc<dyn ProducerBinding> = binding;
                let node = ops_impl::take_until(core, &producer_binding, src_id, notifier_id);
                u32::try_from(node.raw()).map_err(|_| NapiError::from_reason("node id exceeds u32"))
            })
            .await?
    }

    // -----------------------------------------------------------------
    // Higher-order operators (Slice E)
    // -----------------------------------------------------------------

    #[napi]
    pub fn register_switch_map<'env>(
        &self,
        env: &'env Env,
        outer: u32,
        project: Function<'_, u32, u32>,
    ) -> Result<PromiseRaw<'env, u32>> {
        let tsfn = build_h_to_h_tsfn(project)?;
        let project_closure = closure_h_to_nodeid(tsfn);
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let outer_id = NodeId::new(u64::from(outer));
        env.spawn_future(async move {
            actor
                .run(move |core| -> Result<u32> {
                    let higher_binding: Arc<dyn HigherOrderBinding> = binding;
                    let node =
                        higher_order::switch_map(core, &higher_binding, outer_id, project_closure);
                    u32::try_from(node.raw())
                        .map_err(|_| NapiError::from_reason("node id exceeds u32"))
                })
                .await?
        })
    }

    #[napi]
    pub fn register_exhaust_map<'env>(
        &self,
        env: &'env Env,
        outer: u32,
        project: Function<'_, u32, u32>,
    ) -> Result<PromiseRaw<'env, u32>> {
        let tsfn = build_h_to_h_tsfn(project)?;
        let project_closure = closure_h_to_nodeid(tsfn);
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let outer_id = NodeId::new(u64::from(outer));
        env.spawn_future(async move {
            actor
                .run(move |core| -> Result<u32> {
                    let higher_binding: Arc<dyn HigherOrderBinding> = binding;
                    let node =
                        higher_order::exhaust_map(core, &higher_binding, outer_id, project_closure);
                    u32::try_from(node.raw())
                        .map_err(|_| NapiError::from_reason("node id exceeds u32"))
                })
                .await?
        })
    }

    #[napi]
    pub fn register_concat_map<'env>(
        &self,
        env: &'env Env,
        outer: u32,
        project: Function<'_, u32, u32>,
    ) -> Result<PromiseRaw<'env, u32>> {
        let tsfn = build_h_to_h_tsfn(project)?;
        let project_closure = closure_h_to_nodeid(tsfn);
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let outer_id = NodeId::new(u64::from(outer));
        env.spawn_future(async move {
            actor
                .run(move |core| -> Result<u32> {
                    let higher_binding: Arc<dyn HigherOrderBinding> = binding;
                    let node =
                        higher_order::concat_map(core, &higher_binding, outer_id, project_closure);
                    u32::try_from(node.raw())
                        .map_err(|_| NapiError::from_reason("node id exceeds u32"))
                })
                .await?
        })
    }

    /// `merge_map(outer, project, concurrency?)` — `None` = unbounded.
    #[napi]
    pub fn register_merge_map<'env>(
        &self,
        env: &'env Env,
        outer: u32,
        project: Function<'_, u32, u32>,
        concurrency: Option<u32>,
    ) -> Result<PromiseRaw<'env, u32>> {
        let tsfn = build_h_to_h_tsfn(project)?;
        let project_closure = closure_h_to_nodeid(tsfn);
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let outer_id = NodeId::new(u64::from(outer));
        env.spawn_future(async move {
            actor
                .run(move |core| -> Result<u32> {
                    let higher_binding: Arc<dyn HigherOrderBinding> = binding;
                    let node = higher_order::merge_map_with_concurrency(
                        core,
                        &higher_binding,
                        outer_id,
                        project_closure,
                        concurrency,
                    );
                    u32::try_from(node.raw())
                        .map_err(|_| NapiError::from_reason("node id exceeds u32"))
                })
                .await?
        })
    }

    // -----------------------------------------------------------------
    // Control operators (Slice U napi parity)
    // -----------------------------------------------------------------

    /// `tap(src, fn)` — side-effect on each DATA. JS callback: `(handle: u32) => void`.
    #[napi]
    pub fn register_tap<'env>(
        &self,
        env: &'env Env,
        src: u32,
        callback: Function<'_, u32, ()>,
    ) -> Result<PromiseRaw<'env, u32>> {
        let tsfn = build_h_to_unit_tsfn(callback)?;
        let tap_closure = closure_h_to_unit(tsfn);
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let src_id = NodeId::new(u64::from(src));
        env.spawn_future(async move {
            actor
                .run(move |core| -> Result<u32> {
                    let fn_id = {
                        let mut reg = binding.registry.lock();
                        let fn_id = FnId::new(reg.next_fn_id);
                        reg.next_fn_id += 1;
                        reg.tap_fns.insert(fn_id, Arc::from(tap_closure));
                        fn_id
                    };
                    let producer_binding: Arc<dyn ProducerBinding> = binding;
                    let node = control::tap(core, &producer_binding, src_id, fn_id);
                    u32::try_from(node.raw())
                        .map_err(|_| NapiError::from_reason("node id exceeds u32"))
                })
                .await?
        })
    }

    /// `tap_observer(src, data_fn?, error_fn?, complete_fn?)` — lifecycle-aware taps.
    #[napi]
    pub fn register_tap_observer<'env>(
        &self,
        env: &'env Env,
        src: u32,
        data_callback: Option<Function<'_, u32, ()>>,
        error_callback: Option<Function<'_, u32, ()>>,
        complete_callback: Option<Function<'env, (), ()>>,
    ) -> Result<PromiseRaw<'env, u32>> {
        let data_tsfn = data_callback.map(build_h_to_unit_tsfn).transpose()?;
        let error_tsfn = error_callback.map(build_h_to_unit_tsfn).transpose()?;
        let complete_tsfn = complete_callback.map(build_unit_to_unit_tsfn).transpose()?;

        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let src_id = NodeId::new(u64::from(src));
        env.spawn_future(async move {
            actor
                .run(move |core| -> Result<u32> {
                    let mut reg = binding.registry.lock();
                    let data_fn_id = data_tsfn.map(|tsfn| {
                        let fn_id = FnId::new(reg.next_fn_id);
                        reg.next_fn_id += 1;
                        reg.tap_fns
                            .insert(fn_id, Arc::from(closure_h_to_unit(tsfn)));
                        fn_id
                    });
                    let error_fn_id = error_tsfn.map(|tsfn| {
                        let fn_id = FnId::new(reg.next_fn_id);
                        reg.next_fn_id += 1;
                        reg.tap_error_fns
                            .insert(fn_id, Arc::from(closure_h_to_unit(tsfn)));
                        fn_id
                    });
                    let complete_fn_id = complete_tsfn.map(|tsfn| {
                        let fn_id = FnId::new(reg.next_fn_id);
                        reg.next_fn_id += 1;
                        reg.tap_complete_fns
                            .insert(fn_id, Arc::from(closure_unit(tsfn)));
                        fn_id
                    });
                    drop(reg);
                    let producer_binding: Arc<dyn ProducerBinding> = binding;
                    let node = control::tap_observer(
                        core,
                        &producer_binding,
                        src_id,
                        data_fn_id,
                        error_fn_id,
                        complete_fn_id,
                    );
                    u32::try_from(node.raw())
                        .map_err(|_| NapiError::from_reason("node id exceeds u32"))
                })
                .await?
        })
    }

    /// `on_first_data(src, fn)` — one-shot side-effect on first DATA.
    #[napi]
    pub fn register_on_first_data<'env>(
        &self,
        env: &'env Env,
        src: u32,
        callback: Function<'_, u32, ()>,
    ) -> Result<PromiseRaw<'env, u32>> {
        let tsfn = build_h_to_unit_tsfn(callback)?;
        let tap_closure = closure_h_to_unit(tsfn);
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let src_id = NodeId::new(u64::from(src));
        env.spawn_future(async move {
            actor
                .run(move |core| -> Result<u32> {
                    let fn_id = {
                        let mut reg = binding.registry.lock();
                        let fn_id = FnId::new(reg.next_fn_id);
                        reg.next_fn_id += 1;
                        reg.tap_fns.insert(fn_id, Arc::from(tap_closure));
                        fn_id
                    };
                    let producer_binding: Arc<dyn ProducerBinding> = binding;
                    let node = control::on_first_data(core, &producer_binding, src_id, fn_id);
                    u32::try_from(node.raw())
                        .map_err(|_| NapiError::from_reason("node id exceeds u32"))
                })
                .await?
        })
    }

    /// `rescue(src, fn)` — error recovery. JS callback: `(handle: u32) => number | -1`.
    /// Returns a handle for the recovered value, or -1 to propagate the original error.
    #[napi]
    pub fn register_rescue<'env>(
        &self,
        env: &'env Env,
        src: u32,
        callback: Function<'_, u32, i32>,
    ) -> Result<PromiseRaw<'env, u32>> {
        let tsfn = build_h_to_i32_tsfn(callback)?;
        let rescue_closure = closure_rescue(tsfn, Arc::downgrade(&self.binding));
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let src_id = NodeId::new(u64::from(src));
        env.spawn_future(async move {
            actor
                .run(move |core| -> Result<u32> {
                    let fn_id = {
                        let mut reg = binding.registry.lock();
                        let fn_id = FnId::new(reg.next_fn_id);
                        reg.next_fn_id += 1;
                        reg.rescue_fns.insert(fn_id, Arc::from(rescue_closure));
                        fn_id
                    };
                    let producer_binding: Arc<dyn ProducerBinding> = binding;
                    let node = control::rescue(core, &producer_binding, src_id, fn_id);
                    u32::try_from(node.raw())
                        .map_err(|_| NapiError::from_reason("node id exceeds u32"))
                })
                .await?
        })
    }

    /// `valve(src, control, gate_fn)` — boolean-gated passthrough.
    /// JS gate callback: `(handle: u32) => boolean`.
    #[napi]
    pub fn register_valve<'env>(
        &self,
        env: &'env Env,
        src: u32,
        control_node: u32,
        gate: Function<'_, u32, bool>,
    ) -> Result<PromiseRaw<'env, u32>> {
        let tsfn = build_h_to_bool_tsfn(gate)?;
        let predicate_closure = closure_h_to_bool(tsfn);
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let src_id = NodeId::new(u64::from(src));
        let control_id = NodeId::new(u64::from(control_node));
        env.spawn_future(async move {
            actor
                .run(move |core| -> Result<u32> {
                    let op_binding: Arc<dyn OperatorBinding> =
                        Arc::clone(&binding) as Arc<dyn OperatorBinding>;
                    let gate_fn_id = op_binding.register_predicate(predicate_closure);
                    let producer_binding: Arc<dyn ProducerBinding> = binding;
                    let node = control::valve(
                        core,
                        &producer_binding,
                        src_id,
                        control_id,
                        gate_fn_id,
                        None, // no CancellationToken in parity surface
                    );
                    u32::try_from(node.raw())
                        .map_err(|_| NapiError::from_reason("node id exceeds u32"))
                })
                .await?
        })
    }

    /// `settle(src, quiet_waves, max_waves?)` — convergence detector.
    #[napi]
    #[must_use]
    pub async fn register_settle(
        &self,
        src: u32,
        quiet_waves: u32,
        max_waves: Option<u32>,
    ) -> Result<u32> {
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let src_id = NodeId::new(u64::from(src));
        actor
            .run(move |core| -> Result<u32> {
                let producer_binding: Arc<dyn ProducerBinding> = binding;
                let node = control::settle(core, &producer_binding, src_id, quiet_waves, max_waves);
                u32::try_from(node.raw()).map_err(|_| NapiError::from_reason("node id exceeds u32"))
            })
            .await?
    }

    /// `repeat(src, count)` — sequential resubscribe loop.
    #[napi]
    #[must_use]
    pub async fn register_repeat(&self, src: u32, count: u32) -> Result<u32> {
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let src_id = NodeId::new(u64::from(src));
        actor
            .run(move |core| -> Result<u32> {
                let producer_binding: Arc<dyn ProducerBinding> = binding;
                let node = control::repeat(core, &producer_binding, src_id, count);
                u32::try_from(node.raw()).map_err(|_| NapiError::from_reason("node id exceeds u32"))
            })
            .await?
    }

    // -----------------------------------------------------------------
    // Buffer operators (Slice U napi parity)
    // -----------------------------------------------------------------

    /// `buffer(src, notifier, packer)` — notifier-triggered flush.
    #[napi]
    pub fn register_buffer<'env>(
        &self,
        env: &'env Env,
        src: u32,
        notifier: u32,
        packer: Function<'_, Vec<u32>, u32>,
    ) -> Result<PromiseRaw<'env, u32>> {
        let tsfn = build_vec_to_h_tsfn(packer)?;
        let packer_closure = closure_packer(tsfn, Arc::downgrade(&self.binding));
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let src_id = NodeId::new(u64::from(src));
        let notifier_id = NodeId::new(u64::from(notifier));
        env.spawn_future(async move {
            actor
                .run(move |core| -> Result<u32> {
                    let op_binding: Arc<dyn OperatorBinding> =
                        Arc::clone(&binding) as Arc<dyn OperatorBinding>;
                    let pack_fn_id = op_binding.register_packer(packer_closure);
                    let producer_binding: Arc<dyn ProducerBinding> = binding;
                    let node =
                        buffer::buffer(core, &producer_binding, src_id, notifier_id, pack_fn_id);
                    u32::try_from(node.raw())
                        .map_err(|_| NapiError::from_reason("node id exceeds u32"))
                })
                .await?
        })
    }

    /// `buffer_count(src, count, packer)` — fixed-size buffer.
    #[napi]
    pub fn register_buffer_count<'env>(
        &self,
        env: &'env Env,
        src: u32,
        count: u32,
        packer: Function<'_, Vec<u32>, u32>,
    ) -> Result<PromiseRaw<'env, u32>> {
        let tsfn = build_vec_to_h_tsfn(packer)?;
        let packer_closure = closure_packer(tsfn, Arc::downgrade(&self.binding));
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let src_id = NodeId::new(u64::from(src));
        env.spawn_future(async move {
            actor
                .run(move |core| -> Result<u32> {
                    let op_binding: Arc<dyn OperatorBinding> =
                        Arc::clone(&binding) as Arc<dyn OperatorBinding>;
                    let pack_fn_id = op_binding.register_packer(packer_closure);
                    let producer_binding: Arc<dyn ProducerBinding> = binding;
                    let node = buffer::buffer_count(
                        core,
                        &producer_binding,
                        src_id,
                        count as usize,
                        pack_fn_id,
                    );
                    u32::try_from(node.raw())
                        .map_err(|_| NapiError::from_reason("node id exceeds u32"))
                })
                .await?
        })
    }

    /// `window(src, notifier)` — notifier-triggered sub-node splitting.
    #[napi]
    #[must_use]
    pub async fn register_window(&self, src: u32, notifier: u32) -> Result<u32> {
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let src_id = NodeId::new(u64::from(src));
        let notifier_id = NodeId::new(u64::from(notifier));
        actor
            .run(move |core| -> Result<u32> {
                let producer_binding: Arc<dyn ProducerBinding> = binding;
                let node = buffer::window(core, &producer_binding, src_id, notifier_id);
                u32::try_from(node.raw()).map_err(|_| NapiError::from_reason("node id exceeds u32"))
            })
            .await?
    }

    /// `window_count(src, count)` — count-based sub-node splitting.
    #[napi]
    #[must_use]
    pub async fn register_window_count(&self, src: u32, count: u32) -> Result<u32> {
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let src_id = NodeId::new(u64::from(src));
        actor
            .run(move |core| -> Result<u32> {
                let producer_binding: Arc<dyn ProducerBinding> = binding;
                let node = buffer::window_count(core, &producer_binding, src_id, count as usize);
                u32::try_from(node.raw()).map_err(|_| NapiError::from_reason("node id exceeds u32"))
            })
            .await?
    }

    // -----------------------------------------------------------------
    // Temporal operator extensions (Slice U napi parity)
    // -----------------------------------------------------------------

    /// `timeout(src, ms, error_handle)` — idle watchdog.
    #[napi]
    #[must_use]
    pub async fn register_timeout(&self, src: u32, ms: u32, error_handle: u32) -> Result<u32> {
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let src_id = NodeId::new(u64::from(src));
        let error_h = HandleId::new(u64::from(error_handle));
        actor
            .run(move |core| -> Result<u32> {
                let producer_binding: Arc<dyn ProducerBinding> = binding;
                let node =
                    temporal::timeout(core, &producer_binding, src_id, u64::from(ms), error_h);
                u32::try_from(node.raw()).map_err(|_| NapiError::from_reason("node id exceeds u32"))
            })
            .await?
    }

    /// `buffer_time(src, ms, packer)` — time-windowed buffer flush.
    #[napi]
    pub fn register_buffer_time<'env>(
        &self,
        env: &'env Env,
        src: u32,
        ms: u32,
        packer: Function<'_, Vec<u32>, u32>,
    ) -> Result<PromiseRaw<'env, u32>> {
        let tsfn = build_vec_to_h_tsfn(packer)?;
        let packer_closure = closure_packer(tsfn, Arc::downgrade(&self.binding));
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let src_id = NodeId::new(u64::from(src));
        env.spawn_future(async move {
            actor
                .run(move |core| -> Result<u32> {
                    let op_binding: Arc<dyn OperatorBinding> =
                        Arc::clone(&binding) as Arc<dyn OperatorBinding>;
                    let pack_fn_id = op_binding.register_packer(packer_closure);
                    let producer_binding: Arc<dyn ProducerBinding> = binding;
                    let node = temporal::buffer_time(
                        core,
                        &producer_binding,
                        src_id,
                        u64::from(ms),
                        pack_fn_id,
                    );
                    u32::try_from(node.raw())
                        .map_err(|_| NapiError::from_reason("node id exceeds u32"))
                })
                .await?
        })
    }

    /// `window_time(src, ms)` — time-windowed sub-node splitting.
    #[napi]
    #[must_use]
    pub async fn register_window_time(&self, src: u32, ms: u32) -> Result<u32> {
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let src_id = NodeId::new(u64::from(src));
        actor
            .run(move |core| -> Result<u32> {
                let producer_binding: Arc<dyn ProducerBinding> = binding;
                let node = temporal::window_time(core, &producer_binding, src_id, u64::from(ms));
                u32::try_from(node.raw()).map_err(|_| NapiError::from_reason("node id exceeds u32"))
            })
            .await?
    }

    // -----------------------------------------------------------------
    // Cold sources (Slice 3e/3f napi parity)
    // -----------------------------------------------------------------

    /// `from_iter(handles)` — emit each handle as DATA, then COMPLETE.
    #[napi]
    #[must_use]
    pub async fn register_from_iter(&self, handles: Vec<u32>) -> Result<u32> {
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        actor
            .run(move |core| -> Result<u32> {
                let handle_ids: Vec<HandleId> = handles
                    .into_iter()
                    .map(|h| HandleId::new(u64::from(h)))
                    .collect();
                let producer_binding: Arc<dyn ProducerBinding> = binding;
                let node = source::from_iter(core, &producer_binding, handle_ids);
                u32::try_from(node.raw()).map_err(|_| NapiError::from_reason("node id exceeds u32"))
            })
            .await?
    }

    /// `of(handles)` — convenience alias for `from_iter`.
    #[napi]
    #[must_use]
    pub async fn register_of(&self, handles: Vec<u32>) -> Result<u32> {
        self.register_from_iter(handles).await
    }

    /// `empty()` — COMPLETE immediately.
    #[napi]
    #[must_use]
    pub async fn register_empty(&self) -> Result<u32> {
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        actor
            .run(move |core| -> Result<u32> {
                let producer_binding: Arc<dyn ProducerBinding> = binding;
                let node = source::empty(core, &producer_binding);
                u32::try_from(node.raw()).map_err(|_| NapiError::from_reason("node id exceeds u32"))
            })
            .await?
    }

    /// `never()` — no-op, stays active.
    #[napi]
    #[must_use]
    pub async fn register_never(&self) -> Result<u32> {
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        actor
            .run(move |core| -> Result<u32> {
                let producer_binding: Arc<dyn ProducerBinding> = binding;
                let node = source::never(core, &producer_binding);
                u32::try_from(node.raw()).map_err(|_| NapiError::from_reason("node id exceeds u32"))
            })
            .await?
    }

    /// `throw_error(error_handle)` — emit ERROR immediately.
    #[napi]
    #[must_use]
    pub async fn register_throw_error(&self, error_handle: u32) -> Result<u32> {
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let error_h = HandleId::new(u64::from(error_handle));
        actor
            .run(move |core| -> Result<u32> {
                let producer_binding: Arc<dyn ProducerBinding> = binding;
                let node = source::throw_error(core, &producer_binding, error_h);
                u32::try_from(node.raw()).map_err(|_| NapiError::from_reason("node id exceeds u32"))
            })
            .await?
    }

    // -----------------------------------------------------------------
    // Stratify (D199 — Unit 5 Q9.2 of SESSION-rust-port-layer-boundary)
    // -----------------------------------------------------------------

    /// `stratify_branch(src, rules, classifier)` — single classifier-
    /// routing branch. JS classifier callback:
    /// `(rules_handle: u32, value_handle: u32) => boolean`. The TS
    /// `stratify(name, source, rules, opts) -> Graph` factory composes
    /// N instances of this one per rule.
    #[napi]
    pub fn register_stratify_branch<'env>(
        &self,
        env: &'env Env,
        src: u32,
        rules_node: u32,
        classifier: Function<'_, FnArgs<(u32, u32)>, bool>,
    ) -> Result<PromiseRaw<'env, u32>> {
        let tsfn = build_hh_to_bool_tsfn(classifier)?;
        let classifier_closure = closure_hh_to_bool(tsfn);
        let binding = Arc::clone(&self.binding);
        let actor = Arc::clone(&self.actor);
        let src_id = NodeId::new(u64::from(src));
        let rules_id = NodeId::new(u64::from(rules_node));
        env.spawn_future(async move {
            actor
                .run(move |core| -> Result<u32> {
                    let op_binding: Arc<dyn OperatorBinding> =
                        Arc::clone(&binding) as Arc<dyn OperatorBinding>;
                    let classifier_fn_id =
                        op_binding.register_stratify_classifier(classifier_closure);
                    let producer_binding: Arc<dyn ProducerBinding> = binding;
                    let node = stratify::stratify_branch(
                        core,
                        &producer_binding,
                        src_id,
                        rules_id,
                        classifier_fn_id,
                    );
                    u32::try_from(node.raw())
                        .map_err(|_| NapiError::from_reason("node id exceeds u32"))
                })
                .await?
        })
    }
}

// =====================================================================
// TSFN builder helpers (napi 3.x builder pattern)
//
// Each `build_*_tsfn` consumes a `Function<'_, Args, Return>` and returns
// an `Arc<Tsfn<T, R>>` with `max_queue_size = 1`, `callee_handled =
// true`. The `build_callback` closure converts the input T to the
// JsValuesTuple that JS sees (identity for u32 / Vec<u32>; tuple-pack
// for FnArgs<(u32, u32)>).
// =====================================================================

fn build_h_to_h_tsfn(callback: Function<'_, u32, u32>) -> Result<Arc<Tsfn<u32, u32>>> {
    let tsfn = callback
        .build_threadsafe_function::<u32>()
        .max_queue_size::<1>()
        .callee_handled::<false>()
        .build_callback(|ctx: ThreadsafeCallContext<u32>| Ok(ctx.value))?;
    Ok(Arc::new(tsfn))
}

fn build_h_to_bool_tsfn(callback: Function<'_, u32, bool>) -> Result<Arc<Tsfn<u32, bool>>> {
    let tsfn = callback
        .build_threadsafe_function::<u32>()
        .max_queue_size::<1>()
        .callee_handled::<false>()
        .build_callback(|ctx: ThreadsafeCallContext<u32>| Ok(ctx.value))?;
    Ok(Arc::new(tsfn))
}

fn build_hh_to_h_tsfn(
    callback: Function<'_, FnArgs<(u32, u32)>, u32>,
) -> Result<Arc<Tsfn<FnArgs<(u32, u32)>, u32>>> {
    let tsfn = callback
        .build_threadsafe_function::<FnArgs<(u32, u32)>>()
        .max_queue_size::<1>()
        .callee_handled::<false>()
        .build_callback(|ctx: ThreadsafeCallContext<FnArgs<(u32, u32)>>| Ok(ctx.value))?;
    Ok(Arc::new(tsfn))
}

fn build_hh_to_bool_tsfn(
    callback: Function<'_, FnArgs<(u32, u32)>, bool>,
) -> Result<Arc<Tsfn<FnArgs<(u32, u32)>, bool>>> {
    let tsfn = callback
        .build_threadsafe_function::<FnArgs<(u32, u32)>>()
        .max_queue_size::<1>()
        .callee_handled::<false>()
        .build_callback(|ctx: ThreadsafeCallContext<FnArgs<(u32, u32)>>| Ok(ctx.value))?;
    Ok(Arc::new(tsfn))
}

fn build_vec_to_h_tsfn(callback: Function<'_, Vec<u32>, u32>) -> Result<Arc<Tsfn<Vec<u32>, u32>>> {
    let tsfn = callback
        .build_threadsafe_function::<Vec<u32>>()
        .max_queue_size::<1>()
        .callee_handled::<false>()
        .build_callback(|ctx: ThreadsafeCallContext<Vec<u32>>| Ok(ctx.value))?;
    Ok(Arc::new(tsfn))
}

/// D263/D264 — generic user-derived TSFN. Input is `Vec<Vec<u32>>`
/// (per-dep wave handles), output is `Vec<u32>` (emit-as-DATA handles).
fn build_vec_vec_to_vec_tsfn(
    callback: Function<'_, Vec<Vec<u32>>, Vec<u32>>,
) -> Result<Arc<Tsfn<Vec<Vec<u32>>, Vec<u32>>>> {
    let tsfn = callback
        .build_threadsafe_function::<Vec<Vec<u32>>>()
        .max_queue_size::<1>()
        .callee_handled::<false>()
        .build_callback(|ctx: ThreadsafeCallContext<Vec<Vec<u32>>>| Ok(ctx.value))?;
    Ok(Arc::new(tsfn))
}

// Slice U napi parity — tap/rescue callback shapes.

fn build_h_to_unit_tsfn(callback: Function<'_, u32, ()>) -> Result<Arc<Tsfn<u32, ()>>> {
    let tsfn = callback
        .build_threadsafe_function::<u32>()
        .max_queue_size::<1>()
        .callee_handled::<false>()
        .build_callback(|ctx: ThreadsafeCallContext<u32>| Ok(ctx.value))?;
    Ok(Arc::new(tsfn))
}

fn build_unit_to_unit_tsfn(callback: Function<'_, (), ()>) -> Result<Arc<Tsfn<(), ()>>> {
    let tsfn = callback
        .build_threadsafe_function::<()>()
        .max_queue_size::<1>()
        .callee_handled::<false>()
        .build_callback(|ctx: ThreadsafeCallContext<()>| Ok(ctx.value))?;
    Ok(Arc::new(tsfn))
}

fn build_h_to_i32_tsfn(callback: Function<'_, u32, i32>) -> Result<Arc<Tsfn<u32, i32>>> {
    let tsfn = callback
        .build_threadsafe_function::<u32>()
        .max_queue_size::<1>()
        .callee_handled::<false>()
        .build_callback(|ctx: ThreadsafeCallContext<u32>| Ok(ctx.value))?;
    Ok(Arc::new(tsfn))
}

// =====================================================================
// Closure builders (TSFN → boxed closure of the right shape)
//
// **C2 / C3 retain-bump discipline:** every closure that returns a
// HandleId to Core (`project_each` / `pack_tuple` / `pairwise_pack` /
// `fold_each` outputs) MUST validate the handle exists then bump
// retain (D016 / D020 — Core takes ownership of one fresh share).
// `Registry::validate_and_retain` returns false on unknown HandleId
// → panic with diagnostic.
// =====================================================================

fn closure_h_to_h(
    tsfn: Arc<Tsfn<u32, u32>>,
    binding: Weak<BenchBinding>,
) -> Box<dyn Fn(HandleId) -> HandleId + Send + Sync> {
    // Captures `Weak<BenchBinding>` (Slice Y, 2026-05-08) — closures
    // stored in `binding.registry.projectors` would otherwise create
    // the same Arc cycle as producer-builds. Upgrade-on-fire.
    //
    // Slice X3 /qa F (2026-05-08): graceful no-op on dangling weak
    // (was `.expect("...")` which panicked across the napi boundary).
    // Closure storage lives in BenchBinding's own registry, so dangling
    // weak should be unreachable except during a partial Drop unwind —
    // skipping the retain-validate is consistent with the producer-build
    // factories' `else { return; }` pattern.
    Box::new(move |h: HandleId| -> HandleId {
        let arg = u32::try_from(h.raw()).expect("HandleId exceeds u32");
        let ret: u32 = bridge_sync(&tsfn, arg);
        let h_out = HandleId::new(u64::from(ret));
        let Some(binding) = binding.upgrade() else {
            // BenchBinding dropped — Drop-cascade race. Return the raw
            // out-handle without retain bump; the registry that would
            // validate is gone anyway.
            return h_out;
        };
        if !binding.registry.lock().validate_and_retain(h_out) {
            panic!(
                "JS callback returned unknown HandleId({}); registry has no value mapping. \
                 JS callbacks must return HandleIds obtained via BenchCore::intern_int(...).",
                h_out.raw()
            );
        }
        h_out
    })
}

fn closure_h_to_bool(tsfn: Arc<Tsfn<u32, bool>>) -> Box<dyn Fn(HandleId) -> bool + Send + Sync> {
    Box::new(move |h: HandleId| -> bool {
        let arg = u32::try_from(h.raw()).expect("HandleId exceeds u32");
        bridge_sync::<u32, bool>(&tsfn, arg)
    })
}

fn closure_hh_to_h(
    tsfn: Arc<Tsfn<FnArgs<(u32, u32)>, u32>>,
    binding: Weak<BenchBinding>,
) -> Box<dyn Fn(HandleId, HandleId) -> HandleId + Send + Sync> {
    // Weak<BenchBinding> (Slice Y) — see `closure_h_to_h` doc.
    // Slice X3 /qa F: graceful no-op on dangling weak.
    Box::new(move |a: HandleId, b: HandleId| -> HandleId {
        let a_u32 = u32::try_from(a.raw()).expect("HandleId exceeds u32");
        let b_u32 = u32::try_from(b.raw()).expect("HandleId exceeds u32");
        let ret: u32 = bridge_sync(&tsfn, FnArgs::from((a_u32, b_u32)));
        let h_out = HandleId::new(u64::from(ret));
        let Some(binding) = binding.upgrade() else {
            return h_out;
        };
        if !binding.registry.lock().validate_and_retain(h_out) {
            panic!(
                "JS callback returned unknown HandleId({}); registry has no value mapping. \
                 JS callbacks must return HandleIds obtained via BenchCore::intern_int(...).",
                h_out.raw()
            );
        }
        h_out
    })
}

fn closure_hh_to_bool(
    tsfn: Arc<Tsfn<FnArgs<(u32, u32)>, bool>>,
) -> Box<dyn Fn(HandleId, HandleId) -> bool + Send + Sync> {
    Box::new(move |a: HandleId, b: HandleId| -> bool {
        let a_u32 = u32::try_from(a.raw()).expect("HandleId exceeds u32");
        let b_u32 = u32::try_from(b.raw()).expect("HandleId exceeds u32");
        bridge_sync::<FnArgs<(u32, u32)>, bool>(&tsfn, FnArgs::from((a_u32, b_u32)))
    })
}

/// D263/D264 — generic user-derived closure. Builds the per-dep handle
/// arrays from `DepBatch` slices, invokes the TSFN, and returns the
/// output handles for `FnEmission::Data` dispatch. JS-side handles
/// are minted via `core.allocExternalHandle` (retain=1 at allocation
/// time), so we do NOT call `validate_and_retain` here — Core takes
/// ownership of the share by storing the handle in its emission queue.
fn closure_user_derived(
    tsfn: Arc<Tsfn<Vec<Vec<u32>>, Vec<u32>>>,
) -> Box<dyn Fn(&[DepBatch]) -> Vec<HandleId> + Send + Sync> {
    Box::new(move |deps: &[DepBatch]| -> Vec<HandleId> {
        // Pre-allocate per-dep handle arrays for this wave. Each dep's
        // `data` is the SmallVec of DATA handles delivered this wave
        // (empty when a dep was not involved or only RESOLVED/terminal).
        let arg: Vec<Vec<u32>> = deps
            .iter()
            .map(|db| {
                db.data
                    .iter()
                    .map(|h| u32::try_from(h.raw()).expect("HandleId exceeds u32"))
                    .collect::<Vec<u32>>()
            })
            .collect();
        let ret: Vec<u32> = bridge_sync(&tsfn, arg);
        ret.into_iter()
            .map(|h| HandleId::new(u64::from(h)))
            .collect()
    })
}

fn closure_packer(tsfn: Arc<Tsfn<Vec<u32>, u32>>, binding: Weak<BenchBinding>) -> PackerFn {
    // Weak<BenchBinding> (Slice Y) — see `closure_h_to_h` doc.
    // Slice X3 /qa F: graceful no-op on dangling weak.
    Box::new(move |handles: &[HandleId]| -> HandleId {
        let arg: Vec<u32> = handles
            .iter()
            .map(|h| u32::try_from(h.raw()).expect("HandleId exceeds u32"))
            .collect();
        let ret: u32 = bridge_sync(&tsfn, arg);
        let h_out = HandleId::new(u64::from(ret));
        let Some(binding) = binding.upgrade() else {
            return h_out;
        };
        if !binding.registry.lock().validate_and_retain(h_out) {
            panic!(
                "JS packer callback returned unknown HandleId({}); registry has no value mapping. \
                 JS packers must return HandleIds obtained via BenchCore::intern_int(...).",
                h_out.raw()
            );
        }
        h_out
    })
}

// Slice U napi parity — tap/rescue closure builders.

/// Tap side-effect: HandleId → (). No return value, no retain bump.
fn closure_h_to_unit(tsfn: Arc<Tsfn<u32, ()>>) -> Box<dyn Fn(HandleId) + Send + Sync> {
    Box::new(move |h: HandleId| {
        let arg = u32::try_from(h.raw()).expect("HandleId exceeds u32");
        bridge_sync::<u32, ()>(&tsfn, arg);
    })
}

/// Complete tap side-effect: () → (). No args, no return.
fn closure_unit(tsfn: Arc<Tsfn<(), ()>>) -> Box<dyn Fn() + Send + Sync> {
    Box::new(move || {
        bridge_sync::<(), ()>(&tsfn, ());
    })
}

/// Rescue closure: HandleId → Result<HandleId, ()>. JS callback returns
/// -1 for "recovery failed" (propagate original error), or a handle ID
/// for the recovered value (with retain-bump via validate_and_retain).
fn closure_rescue(
    tsfn: Arc<Tsfn<u32, i32>>,
    binding: Weak<BenchBinding>,
) -> Box<dyn Fn(HandleId) -> std::result::Result<HandleId, ()> + Send + Sync> {
    Box::new(move |h: HandleId| -> std::result::Result<HandleId, ()> {
        let arg = u32::try_from(h.raw()).expect("HandleId exceeds u32");
        let ret: i32 = bridge_sync(&tsfn, arg);
        if ret < 0 {
            return Err(());
        }
        let h_out = HandleId::new(u64::from(ret as u32));
        let Some(binding) = binding.upgrade() else {
            return Ok(h_out);
        };
        if !binding.registry.lock().validate_and_retain(h_out) {
            panic!(
                "rescue JS callback returned unknown HandleId({}); registry has no value mapping.",
                h_out.raw()
            );
        }
        Ok(h_out)
    })
}

/// Higher-order project: HandleId → NodeId. JS callback returns u32
/// (the inner-stream NodeId). No retain bump — NodeId isn't a handle.
/// H-08: validate `ret != 0` since NodeIds are issued from a counter
/// starting at 1; 0 means the JS callback returned a default or never
/// set a NodeId.
fn closure_h_to_nodeid(tsfn: Arc<Tsfn<u32, u32>>) -> Box<dyn Fn(HandleId) -> NodeId + Send + Sync> {
    Box::new(move |h: HandleId| -> NodeId {
        let arg = u32::try_from(h.raw()).expect("HandleId exceeds u32");
        let ret_u32: u32 = bridge_sync(&tsfn, arg);
        if ret_u32 == 0 {
            panic!(
                "higher-order project JS callback returned NodeId(0); valid NodeIds start at 1. \
                 JS callbacks must return a NodeId obtained from a register_state/derived/etc call."
            );
        }
        NodeId::new(u64::from(ret_u32))
    })
}

// =====================================================================
// Error converters
// =====================================================================

fn operator_factory_error_to_napi(e: OperatorFactoryError) -> NapiError {
    NapiError::from_reason(format!("{e}"))
}
