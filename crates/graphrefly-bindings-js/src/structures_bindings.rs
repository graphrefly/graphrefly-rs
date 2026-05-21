//! napi-rs reactive structure bindings for parity tests (Slice U structures).
//!
//! Wraps `graphrefly_structures` into napi classes so the Rust parity arm
//! can exercise `ReactiveLog`, `ReactiveList`, `ReactiveMap`, and
//! `ReactiveIndex` through the same `Impl` interface.
//!
//! All values cross the FFI boundary as opaque `HandleId`s (narrowed to
//! `u32`). Each structure takes a JS packer callback `(handles: u32[]) → u32`
//! at construction time — this becomes the `InternFn<Vec<HandleId>>` that
//! converts snapshots to a single packed handle for Core emission.
//!
//! # S6/D255 actor model
//!
//! Each `BenchReactive*` struct stores an `Arc<CoreActor>` (Send+Sync,
//! shared with the parent BenchCore + companion classes). The inner
//! `Arc<Mutex<ReactiveLog>>` etc. ARE Send+Sync. Every structure
//! method now takes `&Core` explicitly (D246/D231), so we route
//! through `actor.run(move |core| inner.lock().method(core, …))`.
//!
//! View / scan / attach handles (`LogView`, `ScanHandle`, `ReactiveSub`)
//! are themselves Send+Sync (post-D246 they hold only
//! `Vec<(NodeId, SubscriptionId)>` for owner-invoked detach). They live
//! on the JS-side `BenchLogView` / `BenchScanHandle` / `BenchLogSubscription`
//! napi classes, which post detach via `actor.dispatch_detached` in
//! `Drop` (D246 r3 — the binding is the `Drop` owner).

use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi::threadsafe_function::{
    ThreadsafeCallContext, ThreadsafeFunction, ThreadsafeFunctionCallMode,
};
use napi_derive::napi;

use graphrefly_core::handle::HandleId;
use graphrefly_core::NodeId;

use graphrefly_structures::{
    InternFn, LogView, ReactiveIndex, ReactiveIndexOptions, ReactiveList, ReactiveListOptions,
    ReactiveLog, ReactiveLogOptions, ReactiveMap, ReactiveMapOptions, ReactiveSub, ScanHandle,
    ViewSpec,
};

use crate::core_actor::CoreActor;
use crate::core_bindings::{BenchBinding, BenchCore};

// ── Shared TSFN infrastructure ──────────────────────────────────────────

type Tsfn<T, R> = ThreadsafeFunction<T, R, T, Status, false, false, 1>;
type PackTsfn = Arc<Tsfn<Vec<u32>, u32>>;

fn build_pack_tsfn(callback: Function<'_, Vec<u32>, u32>) -> Result<PackTsfn> {
    let tsfn = callback
        .build_threadsafe_function::<Vec<u32>>()
        .max_queue_size::<1>()
        .callee_handled::<false>()
        .build_callback(|ctx: ThreadsafeCallContext<Vec<u32>>| Ok(ctx.value))?;
    Ok(Arc::new(tsfn))
}

/// Same bridge_sync pattern as operator_bindings — blocks the calling
/// (worker) thread on a sync_channel while TSFN pumps through libuv.
fn bridge_sync<T, R>(tsfn: &Arc<Tsfn<T, R>>, arg: T) -> R
where
    T: 'static + Send + JsValuesTupleIntoVec,
    R: FromNapiValue + Send + 'static,
{
    let (tx, rx) = std::sync::mpsc::sync_channel::<Result<R>>(1);
    let status = tsfn.call_with_return_value(
        arg,
        ThreadsafeFunctionCallMode::Blocking,
        move |result: Result<R>, _env: Env| -> Result<()> {
            let _ = tx.send(result);
            Ok(())
        },
    );
    if status != Status::Ok {
        panic!("TSFN call_with_return_value failed with status {status:?} (structures_bindings)");
    }
    rx.recv()
        .expect("TSFN channel closed without reply")
        .expect("JS packer callback threw an exception")
}

/// Build an `InternFn<Vec<HandleId>>` from a pack TSFN + binding weak ref.
fn make_intern_fn(
    tsfn: PackTsfn,
    binding: std::sync::Weak<BenchBinding>,
) -> InternFn<Vec<HandleId>> {
    Arc::new(move |handles: Vec<HandleId>| -> HandleId {
        let arg: Vec<u32> = handles
            .iter()
            .map(|h| u32::try_from(h.raw()).expect("HandleId exceeds u32"))
            .collect();
        let ret: u32 = bridge_sync(&tsfn, arg);
        let h_out = HandleId::new(u64::from(ret));
        if let Some(b) = binding.upgrade() {
            if !b.registry.lock().validate_and_retain(h_out) {
                panic!(
                    "JS packer returned unknown HandleId({}); registry has no value mapping.",
                    h_out.raw()
                );
            }
        }
        h_out
    })
}

/// Identity intern for `scan` (TAcc = HandleId).
fn make_identity_intern_fn(binding: std::sync::Weak<BenchBinding>) -> InternFn<HandleId> {
    Arc::new(move |acc: HandleId| -> HandleId {
        if let Some(b) = binding.upgrade() {
            if !b.registry.lock().validate_and_retain(acc) {
                panic!(
                    "scan folder returned unknown HandleId({}); registry has no value mapping.",
                    acc.raw()
                );
            }
        }
        acc
    })
}

/// Build an `InternFn<Vec<(HandleId, HandleId)>>` for ReactiveMap snapshots.
fn make_map_intern_fn(
    tsfn: PackTsfn,
    binding: std::sync::Weak<BenchBinding>,
) -> InternFn<Vec<(HandleId, HandleId)>> {
    Arc::new(move |pairs: Vec<(HandleId, HandleId)>| -> HandleId {
        let arg: Vec<u32> = pairs
            .iter()
            .flat_map(|(k, v)| {
                [
                    u32::try_from(k.raw()).expect("HandleId exceeds u32"),
                    u32::try_from(v.raw()).expect("HandleId exceeds u32"),
                ]
            })
            .collect();
        let ret: u32 = bridge_sync(&tsfn, arg);
        let h_out = HandleId::new(u64::from(ret));
        if let Some(b) = binding.upgrade() {
            if !b.registry.lock().validate_and_retain(h_out) {
                panic!(
                    "JS packer returned unknown HandleId({}); registry has no value mapping.",
                    h_out.raw()
                );
            }
        }
        h_out
    })
}

/// Build an `InternFn<Vec<IndexRow<HandleId, HandleId>>>` for ReactiveIndex.
fn make_index_intern_fn(
    tsfn: PackTsfn,
    binding: std::sync::Weak<BenchBinding>,
) -> InternFn<Vec<graphrefly_structures::IndexRow<HandleId, HandleId>>> {
    Arc::new(
        move |rows: Vec<graphrefly_structures::IndexRow<HandleId, HandleId>>| -> HandleId {
            let arg: Vec<u32> = rows
                .iter()
                .flat_map(|row| {
                    [
                        u32::try_from(row.primary.raw()).expect("HandleId exceeds u32"),
                        u32::try_from(row.value.raw()).expect("HandleId exceeds u32"),
                    ]
                })
                .collect();
            let ret: u32 = bridge_sync(&tsfn, arg);
            let h_out = HandleId::new(u64::from(ret));
            if let Some(b) = binding.upgrade() {
                if !b.registry.lock().validate_and_retain(h_out) {
                    panic!(
                        "JS packer returned unknown HandleId({}); registry has no value mapping.",
                        h_out.raw()
                    );
                }
            }
            h_out
        },
    )
}

// ── BenchReactiveLog ────────────────────────────────────────────────────

#[napi]
pub struct BenchReactiveLog {
    inner: Arc<parking_lot::Mutex<ReactiveLog<HandleId>>>,
    actor: Arc<CoreActor>,
    node_id_raw: u32,
    binding_weak: std::sync::Weak<BenchBinding>,
}

#[napi]
impl BenchReactiveLog {
    /// Create from a BenchCore + packer callback. Constructor runs
    /// `ReactiveLog::new(&core, …)` on the actor's worker thread via
    /// `run_sync` (briefly blocks libuv on the one-shot factory call).
    ///
    /// **`run_sync` safety, lifecycle-precondition framing
    /// (QA 2026-05-20 F7):** `ReactiveLog::new` is a Core *topology
    /// mutation* (`core.register_state(...)`), not a read-only op.
    /// It is safe under `run_sync` because **no subscribers exist
    /// yet for this newly-created node** — no TSFN-backed sink can
    /// fire during construction → no `bridge_sync` libuv-busy
    /// hazard. The invariant is the *lifecycle precondition* (the
    /// node is freshly created, no `subscribe` could have been
    /// called yet), NOT the substrate operation being side-effect-
    /// free. A future change that wires construction-time
    /// subscribers would invalidate this analysis.
    #[napi(factory)]
    pub fn create(
        core: &BenchCore,
        packer: Function<'_, Vec<u32>, u32>,
        max_size: Option<u32>,
    ) -> Result<Self> {
        let tsfn = build_pack_tsfn(packer)?;
        let actor = core.actor_arc();
        let binding_weak = Arc::downgrade(&core.binding_arc());
        let intern = make_intern_fn(tsfn, binding_weak.clone());
        let opts = ReactiveLogOptions {
            max_size: max_size.map(|n| n as usize),
            ..Default::default()
        };
        let log = actor.run_sync(move |c| ReactiveLog::new(c, intern, opts))?;
        let node_id_raw = u32::try_from(log.node_id.raw())
            .map_err(|_| Error::from_reason("node id exceeds u32"))?;
        Ok(Self {
            inner: Arc::new(parking_lot::Mutex::new(log)),
            actor,
            node_id_raw,
            binding_weak,
        })
    }

    #[napi(getter)]
    pub fn node_id(&self) -> u32 {
        self.node_id_raw
    }

    #[napi(getter)]
    pub fn size(&self) -> u32 {
        self.inner.lock().size() as u32
    }

    #[napi]
    pub async fn append(&self, handle: u32) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        let h = HandleId::new(u64::from(handle));
        self.actor
            .run(move |core| {
                inner.lock().append(core, h);
            })
            .await
    }

    #[napi]
    pub async fn append_many(&self, handles: Vec<u32>) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        let hs: Vec<HandleId> = handles
            .into_iter()
            .map(|h| HandleId::new(u64::from(h)))
            .collect();
        self.actor
            .run(move |core| {
                inner.lock().append_many(core, hs);
            })
            .await
    }

    #[napi]
    pub async fn clear(&self) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        self.actor
            .run(move |core| {
                inner.lock().clear(core);
            })
            .await
    }

    #[napi]
    pub fn at(&self, index: i32) -> u32 {
        self.inner
            .lock()
            .at(i64::from(index))
            .map(|h| u32::try_from(h.raw()).unwrap_or(0))
            .unwrap_or(0)
    }

    #[napi]
    pub async fn trim_head(&self, n: u32) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        self.actor
            .run(move |core| {
                inner.lock().trim_head(core, n as usize);
            })
            .await
    }

    // ── F18: view / scan / attach ──────────────────────────────────────

    #[napi]
    pub fn view_tail<'env>(
        &self,
        env: &'env Env,
        packer: Function<'_, Vec<u32>, u32>,
        n: u32,
    ) -> Result<PromiseRaw<'env, BenchLogView>> {
        let tsfn = build_pack_tsfn(packer)?;
        self.spawn_view(env, tsfn, ViewSpec::Tail { n: n as usize })
    }

    #[napi]
    pub fn view_slice<'env>(
        &self,
        env: &'env Env,
        packer: Function<'_, Vec<u32>, u32>,
        start: u32,
        stop: Option<u32>,
    ) -> Result<PromiseRaw<'env, BenchLogView>> {
        let tsfn = build_pack_tsfn(packer)?;
        self.spawn_view(
            env,
            tsfn,
            ViewSpec::Slice {
                start: start as usize,
                stop: stop.map(|s| s as usize),
            },
        )
    }

    #[napi]
    pub fn view_from_cursor<'env>(
        &self,
        env: &'env Env,
        packer: Function<'_, Vec<u32>, u32>,
        cursor_node_id: u32,
        read_cursor: Function<'_, Vec<u32>, u32>,
    ) -> Result<PromiseRaw<'env, BenchLogView>> {
        let tsfn = build_pack_tsfn(packer)?;
        let cursor_tsfn = build_pack_tsfn(read_cursor)?;
        let read = Arc::new(move |h: HandleId| -> usize {
            let arg = vec![u32::try_from(h.raw()).expect("HandleId exceeds u32")];
            bridge_sync(&cursor_tsfn, arg) as usize
        });
        self.spawn_view(
            env,
            tsfn,
            ViewSpec::FromCursor {
                cursor_node: NodeId::new(u64::from(cursor_node_id)),
                read_cursor: read,
            },
        )
    }

    fn spawn_view<'env>(
        &self,
        env: &'env Env,
        tsfn: PackTsfn,
        spec: ViewSpec,
    ) -> Result<PromiseRaw<'env, BenchLogView>> {
        let intern = make_intern_fn(tsfn, self.binding_weak.clone());
        let inner = Arc::clone(&self.inner);
        let actor = self.actor.clone();
        let actor_for_handle = actor.clone();
        env.spawn_future(async move {
            let view = actor
                .run(move |core| inner.lock().view(core, spec, intern))
                .await?;
            let node_id_raw = u32::try_from(view.node_id.raw())
                .map_err(|_| Error::from_reason("view node id exceeds u32"))?;
            Ok(BenchLogView {
                view: Some(view),
                actor: actor_for_handle,
                node_id_raw,
            })
        })
    }

    #[napi]
    pub fn scan<'env>(
        &self,
        env: &'env Env,
        seed: u32,
        folder: Function<'_, Vec<u32>, u32>,
    ) -> Result<PromiseRaw<'env, BenchScanHandle>> {
        let folder_tsfn = build_pack_tsfn(folder)?;
        let step = Arc::new(move |acc: &HandleId, value: &HandleId| -> HandleId {
            let arg = vec![
                u32::try_from(acc.raw()).expect("HandleId exceeds u32"),
                u32::try_from(value.raw()).expect("HandleId exceeds u32"),
            ];
            HandleId::new(u64::from(bridge_sync(&folder_tsfn, arg)))
        });
        let intern = make_identity_intern_fn(self.binding_weak.clone());
        let seed_h = HandleId::new(u64::from(seed));
        let inner = Arc::clone(&self.inner);
        let actor = self.actor.clone();
        let actor_for_handle = actor.clone();
        env.spawn_future(async move {
            let scan = actor
                .run(move |core| inner.lock().scan(core, seed_h, step, intern))
                .await?;
            let node_id_raw = u32::try_from(scan.node_id.raw())
                .map_err(|_| Error::from_reason("scan node id exceeds u32"))?;
            Ok(BenchScanHandle {
                scan: Some(scan),
                actor: actor_for_handle,
                node_id_raw,
            })
        })
    }

    /// D270 — `skip_cached_replay` (memo:Re P2 parity). When true AND
    /// the upstream has a cached value, the subscribe-handshake DATA
    /// replay is suppressed; subsequent live emissions still land.
    /// Default `false` preserves push-on-subscribe semantics.
    #[napi]
    pub async fn attach(
        &self,
        upstream_node_id: u32,
        skip_cached_replay: Option<bool>,
    ) -> Result<BenchLogSubscription> {
        let binding = self.binding_weak.clone();
        let read_value = Arc::new(move |h: HandleId| -> HandleId {
            if let Some(b) = binding.upgrade() {
                b.registry.lock().validate_and_retain(h);
            }
            h
        });
        let inner = Arc::clone(&self.inner);
        let actor = self.actor.clone();
        let actor_for_sub = actor.clone();
        let opts = graphrefly_structures::AttachOptions {
            skip_cached_replay: skip_cached_replay.unwrap_or(false),
        };
        let sub = actor
            .run(move |core| {
                inner.lock().attach_with_options(
                    core,
                    NodeId::new(u64::from(upstream_node_id)),
                    read_value,
                    opts,
                )
            })
            .await?;
        Ok(BenchLogSubscription {
            sub: Arc::new(parking_lot::Mutex::new(Some(sub))),
            actor: actor_for_sub,
        })
    }
}

#[napi]
pub struct BenchLogView {
    view: Option<LogView>,
    actor: Arc<CoreActor>,
    node_id_raw: u32,
}

#[napi]
impl BenchLogView {
    #[napi(getter)]
    pub fn node_id(&self) -> u32 {
        self.node_id_raw
    }
}

impl Drop for BenchLogView {
    fn drop(&mut self) {
        if let Some(mut view) = self.view.take() {
            let _ = self.actor.dispatch_detached(move |core| view.detach(core));
        }
    }
}

#[napi]
pub struct BenchScanHandle {
    scan: Option<ScanHandle>,
    actor: Arc<CoreActor>,
    node_id_raw: u32,
}

#[napi]
impl BenchScanHandle {
    #[napi(getter)]
    pub fn node_id(&self) -> u32 {
        self.node_id_raw
    }
}

impl Drop for BenchScanHandle {
    fn drop(&mut self) {
        if let Some(mut scan) = self.scan.take() {
            let _ = self.actor.dispatch_detached(move |core| scan.detach(core));
        }
    }
}

#[napi]
pub struct BenchLogSubscription {
    sub: Arc<parking_lot::Mutex<Option<ReactiveSub>>>,
    actor: Arc<CoreActor>,
}

#[napi]
impl BenchLogSubscription {
    /// Owner-invoked detach (D246 r3). Idempotent — second call finds
    /// no inner ReactiveSub and no-ops. Async to keep the JS thread
    /// free during the actor round-trip; `&self` (not `&mut self`)
    /// because napi-rs 3 rejects `async fn(&mut self)` without
    /// explicit `unsafe`. Interior mutability via `Mutex<Option<…>>`.
    #[napi]
    pub async fn dispose(&self) -> Result<()> {
        let taken = self.sub.lock().take();
        if let Some(mut sub) = taken {
            self.actor.run(move |core| sub.detach(core)).await?;
        }
        Ok(())
    }
}

impl Drop for BenchLogSubscription {
    fn drop(&mut self) {
        let taken = self.sub.lock().take();
        if let Some(mut sub) = taken {
            let _ = self.actor.dispatch_detached(move |core| sub.detach(core));
        }
    }
}

// ── BenchReactiveList ───────────────────────────────────────────────────

#[napi]
pub struct BenchReactiveList {
    inner: Arc<parking_lot::Mutex<ReactiveList<HandleId>>>,
    actor: Arc<CoreActor>,
    node_id_raw: u32,
}

#[napi]
impl BenchReactiveList {
    /// **`run_sync` safety:** see [`BenchReactiveLog::create`]'s doc
    /// — `ReactiveList::new` is a topology mutation safe under
    /// `run_sync` *only* because no subscribers exist for the
    /// freshly-created node (lifecycle precondition).
    #[napi(factory)]
    pub fn create(core: &BenchCore, packer: Function<'_, Vec<u32>, u32>) -> Result<Self> {
        let tsfn = build_pack_tsfn(packer)?;
        let actor = core.actor_arc();
        let binding_weak = Arc::downgrade(&core.binding_arc());
        let intern = make_intern_fn(tsfn, binding_weak);
        let opts = ReactiveListOptions::default();
        let list = actor.run_sync(move |c| ReactiveList::new(c, intern, opts))?;
        let node_id_raw = u32::try_from(list.node_id.raw())
            .map_err(|_| Error::from_reason("node id exceeds u32"))?;
        Ok(Self {
            inner: Arc::new(parking_lot::Mutex::new(list)),
            actor,
            node_id_raw,
        })
    }

    #[napi(getter)]
    pub fn node_id(&self) -> u32 {
        self.node_id_raw
    }

    #[napi(getter)]
    pub fn size(&self) -> u32 {
        self.inner.lock().size() as u32
    }

    #[napi]
    pub async fn append(&self, handle: u32) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        let h = HandleId::new(u64::from(handle));
        self.actor
            .run(move |core| {
                inner.lock().append(core, h);
            })
            .await
    }

    #[napi]
    pub async fn append_many(&self, handles: Vec<u32>) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        let hs: Vec<HandleId> = handles
            .into_iter()
            .map(|h| HandleId::new(u64::from(h)))
            .collect();
        self.actor
            .run(move |core| {
                inner.lock().append_many(core, hs);
            })
            .await
    }

    #[napi]
    pub async fn insert(&self, index: u32, handle: u32) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        let h = HandleId::new(u64::from(handle));
        self.actor
            .run(move |core| -> Result<()> {
                inner
                    .lock()
                    .insert(core, index as usize, h)
                    .map_err(|e| Error::from_reason(format!("insert out of range: {e}")))?;
                Ok(())
            })
            .await?
    }

    #[napi]
    pub async fn pop(&self, index: Option<i32>) -> Result<u32> {
        let inner = Arc::clone(&self.inner);
        self.actor
            .run(move |core| -> Result<u32> {
                let h = inner
                    .lock()
                    .pop(core, i64::from(index.unwrap_or(-1)))
                    .ok_or_else(|| Error::from_reason("pop on empty list"))?;
                u32::try_from(h.raw()).map_err(|_| Error::from_reason("HandleId exceeds u32"))
            })
            .await?
    }

    #[napi]
    pub async fn clear(&self) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        self.actor
            .run(move |core| {
                inner.lock().clear(core);
            })
            .await
    }

    #[napi]
    pub fn at(&self, index: i32) -> u32 {
        self.inner
            .lock()
            .at(i64::from(index))
            .map(|h| u32::try_from(h.raw()).unwrap_or(0))
            .unwrap_or(0)
    }
}

// ── BenchReactiveMap ────────────────────────────────────────────────────

#[napi]
pub struct BenchReactiveMap {
    inner: Arc<parking_lot::Mutex<ReactiveMap<HandleId, HandleId>>>,
    actor: Arc<CoreActor>,
    node_id_raw: u32,
}

#[napi]
impl BenchReactiveMap {
    /// **`run_sync` safety:** see [`BenchReactiveLog::create`]'s doc
    /// — `ReactiveMap::new` is a topology mutation safe under
    /// `run_sync` *only* because no subscribers exist for the
    /// freshly-created node (lifecycle precondition).
    #[napi(factory)]
    pub fn create(
        core: &BenchCore,
        packer: Function<'_, Vec<u32>, u32>,
        max_size: Option<u32>,
        default_ttl: Option<f64>,
    ) -> Result<Self> {
        let tsfn = build_pack_tsfn(packer)?;
        let actor = core.actor_arc();
        let binding_weak = Arc::downgrade(&core.binding_arc());
        let intern = make_map_intern_fn(tsfn, binding_weak);
        let opts = ReactiveMapOptions {
            default_ttl,
            max_size: max_size.map(|n| n as usize),
            ..Default::default()
        };
        let map_result = actor.run_sync(move |c| ReactiveMap::new(c, intern, opts))?;
        let map = map_result
            .map_err(|e| Error::from_reason(format!("ReactiveMap::new failed: {e:?}")))?;
        let node_id_raw = u32::try_from(map.node_id.raw())
            .map_err(|_| Error::from_reason("node id exceeds u32"))?;
        Ok(Self {
            inner: Arc::new(parking_lot::Mutex::new(map)),
            actor,
            node_id_raw,
        })
    }

    #[napi(getter)]
    pub fn node_id(&self) -> u32 {
        self.node_id_raw
    }

    #[napi(getter)]
    pub fn size(&self) -> u32 {
        self.inner.lock().size() as u32
    }

    /// `async fn` at the napi boundary (QA fix 2026-05-20, Edge Case
    /// Hunter F3 / Blind Hunter C2): `ReactiveMap::has` is NOT
    /// read-only — when TTL is configured, an expired-key check
    /// triggers `prune_expired_inner` + `self.emitter.emit(core,
    /// snapshot)` (see `crates/graphrefly-structures/src/reactive.rs`
    /// `has` body). The emission fires subscribers; if any subscriber
    /// uses `bridge_sync` (Blocking TSFN call), libuv must be free to
    /// pump the TSFN microtask — under the prior sync `run_sync`
    /// shape libuv was blocked on the actor reply → 3-way deadlock.
    /// `actor.run` keeps libuv free during the await.
    ///
    /// **Public-API change:** `Impl.ImplReactiveMap.has(key)` is
    /// `Promise<boolean>` (was `boolean`); `parity-tests/impls/
    /// types.ts` widened correspondingly. See cross-track-ledger.md
    /// §1 row 2026-05-20 (F3).
    #[napi]
    pub async fn has(&self, key: u32) -> Result<bool> {
        let k = HandleId::new(u64::from(key));
        let inner = Arc::clone(&self.inner);
        self.actor.run(move |core| inner.lock().has(core, &k)).await
    }

    /// `async fn` at the napi boundary — same rationale as `has`
    /// (TTL-prune side-effect emits a snapshot, can deadlock libuv
    /// under sync `run_sync`).
    #[napi]
    pub async fn get(&self, key: u32) -> Result<u32> {
        let k = HandleId::new(u64::from(key));
        let inner = Arc::clone(&self.inner);
        self.actor
            .run(move |core| {
                inner
                    .lock()
                    .get(core, &k)
                    .map(|h| u32::try_from(h.raw()).unwrap_or(0))
                    .unwrap_or(0)
            })
            .await
    }

    #[napi]
    pub async fn set(&self, key: u32, value: u32, ttl: Option<f64>) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        let k = HandleId::new(u64::from(key));
        let v = HandleId::new(u64::from(value));
        self.actor
            .run(move |core| {
                inner.lock().set_with_ttl(core, k, v, ttl);
            })
            .await
    }

    #[napi]
    pub async fn delete(&self, key: u32) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        let k = HandleId::new(u64::from(key));
        self.actor
            .run(move |core| {
                inner.lock().delete(core, &k);
            })
            .await
    }

    #[napi]
    pub async fn clear(&self) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        self.actor
            .run(move |core| {
                inner.lock().clear(core);
            })
            .await
    }
}

// ── BenchReactiveIndex ──────────────────────────────────────────────────

#[napi]
pub struct BenchReactiveIndex {
    inner: Arc<parking_lot::Mutex<ReactiveIndex<HandleId, HandleId>>>,
    actor: Arc<CoreActor>,
    node_id_raw: u32,
    /// F20/D205: numeric-primary → value-handle mirror.
    keyed: Arc<parking_lot::Mutex<std::collections::BTreeMap<i64, u32>>>,
}

#[napi]
impl BenchReactiveIndex {
    /// **`run_sync` safety:** see [`BenchReactiveLog::create`]'s doc
    /// — `ReactiveIndex::new` is a topology mutation safe under
    /// `run_sync` *only* because no subscribers exist for the
    /// freshly-created node (lifecycle precondition).
    #[napi(factory)]
    pub fn create(core: &BenchCore, packer: Function<'_, Vec<u32>, u32>) -> Result<Self> {
        let tsfn = build_pack_tsfn(packer)?;
        let actor = core.actor_arc();
        let binding_weak = Arc::downgrade(&core.binding_arc());
        let intern = make_index_intern_fn(tsfn, binding_weak);
        let opts = ReactiveIndexOptions::default();
        let index = actor.run_sync(move |c| ReactiveIndex::new(c, intern, opts))?;
        let node_id_raw = u32::try_from(index.node_id.raw())
            .map_err(|_| Error::from_reason("node id exceeds u32"))?;
        Ok(Self {
            inner: Arc::new(parking_lot::Mutex::new(index)),
            actor,
            node_id_raw,
            keyed: Arc::new(parking_lot::Mutex::new(std::collections::BTreeMap::new())),
        })
    }

    #[napi(getter)]
    pub fn node_id(&self) -> u32 {
        self.node_id_raw
    }

    #[napi(getter)]
    pub fn size(&self) -> u32 {
        self.inner.lock().size() as u32
    }

    #[napi]
    pub fn has(&self, primary: u32) -> bool {
        let k = HandleId::new(u64::from(primary));
        self.inner.lock().has(&k)
    }

    #[napi]
    pub fn get(&self, primary: u32) -> u32 {
        let k = HandleId::new(u64::from(primary));
        self.inner
            .lock()
            .get(&k)
            .map(|h| u32::try_from(h.raw()).unwrap_or(0))
            .unwrap_or(0)
    }

    #[napi]
    pub async fn upsert(
        &self,
        primary: u32,
        secondary: String,
        value: u32,
        numeric_key: Option<i64>,
    ) -> Result<bool> {
        let inner = Arc::clone(&self.inner);
        let p = HandleId::new(u64::from(primary));
        let v = HandleId::new(u64::from(value));
        if let Some(nk) = numeric_key {
            self.keyed.lock().insert(nk, value);
        }
        self.actor
            .run(move |core| inner.lock().upsert(core, p, secondary, v))
            .await
    }

    #[napi]
    pub async fn delete(&self, primary: u32, numeric_key: Option<i64>) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        let k = HandleId::new(u64::from(primary));
        if let Some(nk) = numeric_key {
            self.keyed.lock().remove(&nk);
        }
        self.actor
            .run(move |core| {
                inner.lock().delete(core, &k);
            })
            .await
    }

    /// F20/D205: values whose numeric primary sorts within `[start, end)`.
    #[napi]
    pub fn range_by_primary(&self, start: i64, end: i64) -> Vec<u32> {
        if start >= end {
            return Vec::new();
        }
        self.keyed
            .lock()
            .range(start..end)
            .map(|(_, &v)| v)
            .collect()
    }

    #[napi]
    pub async fn clear(&self) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        self.keyed.lock().clear();
        self.actor
            .run(move |core| {
                inner.lock().clear(core);
            })
            .await
    }
}
