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
//! Threading model: structures wrap `parking_lot::Mutex` internally; napi
//! methods that mutate run on the tokio blocking pool via `run_blocking`
//! (same as `BenchCore` / `BenchGraph`). Factory methods are sync since
//! `ReactiveLog::new` etc. are lightweight Core mutex operations (same
//! risk profile as `BenchOperators::from_core`).

use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi::threadsafe_function::{
    ThreadsafeCallContext, ThreadsafeFunction, ThreadsafeFunctionCallMode,
};
use napi_derive::napi;

use graphrefly_core::handle::HandleId;
use graphrefly_core::{Core, NodeId, Subscription};

use graphrefly_structures::{
    InternFn, LogView, ReactiveIndex, ReactiveIndexOptions, ReactiveList, ReactiveListOptions,
    ReactiveLog, ReactiveLogOptions, ReactiveMap, ReactiveMapOptions, ScanHandle, ViewSpec,
};

use crate::core_bindings::{run_blocking, BenchBinding, BenchCore};

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

/// Same bridge_sync pattern as operator_bindings — blocks tokio thread
/// on sync_channel while TSFN pumps through libuv.
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

/// Identity intern for `scan` (TAcc = HandleId). The JS folder already
/// returns a registry-allocated handle; this validates + retains it so the
/// scan-node emission keeps it alive (mirrors `make_intern_fn`'s retain).
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
/// Flattens `[(k, v), ...]` into `[k, v, k, v, ...]` before passing to JS.
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
            // Flatten to [primary, value, primary, value, ...] — JS packer
            // decodes by stride-2. Secondary strings dropped (internal sort key).
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
    core: Core,
    node_id_raw: u32,
    binding_weak: std::sync::Weak<BenchBinding>,
}

#[napi]
impl BenchReactiveLog {
    /// Create from a BenchCore + packer callback. Sync factory — lightweight
    /// Core mutex operation (same risk profile as BenchOperators::from_core).
    #[napi(factory)]
    pub fn create(
        core: &BenchCore,
        packer: Function<'_, Vec<u32>, u32>,
        max_size: Option<u32>,
    ) -> Result<Self> {
        let tsfn = build_pack_tsfn(packer)?;
        let c = core.core_clone();
        let binding_weak = Arc::downgrade(&core.binding_arc());
        let intern = make_intern_fn(tsfn, binding_weak.clone());
        let opts = ReactiveLogOptions {
            max_size: max_size.map(|n| n as usize),
            ..Default::default()
        };
        let log = ReactiveLog::new(&c, intern, opts);
        let node_id_raw = u32::try_from(log.node_id.raw())
            .map_err(|_| Error::from_reason("node id exceeds u32"))?;
        Ok(Self {
            inner: Arc::new(parking_lot::Mutex::new(log)),
            core: c,
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
        let core = self.core.clone();
        let h = HandleId::new(u64::from(handle));
        run_blocking(core, move || {
            inner.lock().append(h);
            Ok(())
        })
        .await?
    }

    #[napi]
    pub async fn append_many(&self, handles: Vec<u32>) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        let core = self.core.clone();
        let hs: Vec<HandleId> = handles
            .into_iter()
            .map(|h| HandleId::new(u64::from(h)))
            .collect();
        run_blocking(core, move || {
            inner.lock().append_many(hs);
            Ok(())
        })
        .await?
    }

    #[napi]
    pub async fn clear(&self) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        let core = self.core.clone();
        run_blocking(core, move || {
            inner.lock().clear();
            Ok(())
        })
        .await?
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
        let core = self.core.clone();
        run_blocking(core, move || {
            inner.lock().trim_head(n as usize);
            Ok(())
        })
        .await?
    }

    // ── F18: view / scan / attach ──────────────────────────────────────
    //
    // These register a node + `core.subscribe`, and Rust `Core::subscribe`
    // fires the push-on-subscribe handshake sink SYNCHRONOUSLY. For a
    // view/scan that sink calls the snapshot `intern`, which `bridge_sync`s
    // back to a JS packer TSFN. So the subscribe MUST run off the libuv
    // thread (via `run_blocking` on the tokio blocking pool, like `append`)
    // — otherwise the napi thread blocks on a TSFN it must itself pump =
    // deadlock. TSFN construction (`Function` is not `Send`) stays on the
    // napi thread, before the `run_blocking` boundary.

    /// `view({ tail: n })` — last `n` entries. `packer` mirrors `create`'s
    /// snapshot packer: `(handles: u32[]) → u32`. Sync method + spawned
    /// Promise: `Function` is `!Send` and lifetime-bound to `env`, so it
    /// can't cross an `.await`; the TSFN is built sync (napi thread), the
    /// blocking subscribe runs off-thread (`env.spawn_future` →
    /// `run_blocking`). Mirrors `OperatorBindings::register_map`.
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

    /// `view({ slice: [start, stop] })` — `[start..stop)`; `stop` defaults
    /// to log length when `None`.
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

    /// `view({ fromCursor })` — entries from a cursor node's position
    /// onward. `read_cursor` is a JS callback `(cursorHandle: u32[1]) →
    /// u32` returning the cursor position; it fires inside Core waves on
    /// the blocking pool, so its `bridge_sync` is safe.
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
        let core = self.core.clone();
        env.spawn_future(async move {
            let view = run_blocking(core, move || inner.lock().view(spec, intern)).await?;
            let node_id_raw = u32::try_from(view.node_id.raw())
                .map_err(|_| Error::from_reason("view node id exceeds u32"))?;
            Ok(BenchLogView {
                _view: view,
                node_id_raw,
            })
        })
    }

    /// `scan(seed, folder)` — running aggregate. `seed` is a handle; the
    /// JS `folder` is `([acc, value]: u32[2]) → u32` (accumulator handle).
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
        let core = self.core.clone();
        env.spawn_future(async move {
            let scan = run_blocking(core, move || inner.lock().scan(seed_h, step, intern)).await?;
            let node_id_raw = u32::try_from(scan.node_id.raw())
                .map_err(|_| Error::from_reason("scan node id exceeds u32"))?;
            Ok(BenchScanHandle {
                _scan: scan,
                node_id_raw,
            })
        })
    }

    /// `attach(upstream)` — append every upstream DATA handle into this
    /// log. Handle-opaque: `read_value` retains so `at()` can't read a
    /// wave-released slot (release-on-trim is a known bounded leak — see
    /// porting-deferred.md, parallels the terminal-slot retain note).
    #[napi]
    pub async fn attach(&self, upstream_node_id: u32) -> Result<BenchLogSubscription> {
        let binding = self.binding_weak.clone();
        let read_value = Arc::new(move |h: HandleId| -> HandleId {
            if let Some(b) = binding.upgrade() {
                b.registry.lock().validate_and_retain(h);
            }
            h
        });
        let inner = Arc::clone(&self.inner);
        let core = self.core.clone();
        let sub = run_blocking(core, move || {
            inner
                .lock()
                .attach(NodeId::new(u64::from(upstream_node_id)), read_value)
        })
        .await?;
        Ok(BenchLogSubscription { sub: Some(sub) })
    }
}

/// RAII handle for `ReactiveLog::view`. Dropping disposes the view's
/// subscriptions. JS subscribes to `node_id` via `BenchCore.subscribeWithTsfn`.
#[napi]
pub struct BenchLogView {
    _view: LogView,
    node_id_raw: u32,
}

#[napi]
impl BenchLogView {
    #[napi(getter)]
    pub fn node_id(&self) -> u32 {
        self.node_id_raw
    }
}

/// RAII handle for `ReactiveLog::scan`. Dropping disposes the scan
/// subscription. JS subscribes to `node_id` for accumulator snapshots.
#[napi]
pub struct BenchScanHandle {
    _scan: ScanHandle,
    node_id_raw: u32,
}

#[napi]
impl BenchScanHandle {
    #[napi(getter)]
    pub fn node_id(&self) -> u32 {
        self.node_id_raw
    }
}

/// RAII handle for `ReactiveLog::attach`. `dispose()` stops the attachment
/// deterministically; dropping the object does the same (RAII fallback for
/// the non-deterministic-GC case).
#[napi]
pub struct BenchLogSubscription {
    sub: Option<Subscription>,
}

#[napi]
impl BenchLogSubscription {
    #[napi]
    pub fn dispose(&mut self) {
        self.sub = None;
    }
}

// ── BenchReactiveList ───────────────────────────────────────────────────

#[napi]
pub struct BenchReactiveList {
    inner: Arc<parking_lot::Mutex<ReactiveList<HandleId>>>,
    core: Core,
    node_id_raw: u32,
}

#[napi]
impl BenchReactiveList {
    #[napi(factory)]
    pub fn create(core: &BenchCore, packer: Function<'_, Vec<u32>, u32>) -> Result<Self> {
        let tsfn = build_pack_tsfn(packer)?;
        let c = core.core_clone();
        let binding_weak = Arc::downgrade(&core.binding_arc());
        let intern = make_intern_fn(tsfn, binding_weak);
        let opts = ReactiveListOptions::default();
        let list = ReactiveList::new(&c, intern, opts);
        let node_id_raw = u32::try_from(list.node_id.raw())
            .map_err(|_| Error::from_reason("node id exceeds u32"))?;
        Ok(Self {
            inner: Arc::new(parking_lot::Mutex::new(list)),
            core: c,
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
        let core = self.core.clone();
        let h = HandleId::new(u64::from(handle));
        run_blocking(core, move || {
            inner.lock().append(h);
            Ok(())
        })
        .await?
    }

    #[napi]
    pub async fn append_many(&self, handles: Vec<u32>) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        let core = self.core.clone();
        let hs: Vec<HandleId> = handles
            .into_iter()
            .map(|h| HandleId::new(u64::from(h)))
            .collect();
        run_blocking(core, move || {
            inner.lock().append_many(hs);
            Ok(())
        })
        .await?
    }

    #[napi]
    pub async fn insert(&self, index: u32, handle: u32) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        let core = self.core.clone();
        let h = HandleId::new(u64::from(handle));
        run_blocking(core, move || {
            inner
                .lock()
                .insert(index as usize, h)
                .map_err(|e| Error::from_reason(format!("insert out of range: {e}")))?;
            Ok(())
        })
        .await?
    }

    #[napi]
    pub async fn pop(&self, index: Option<i32>) -> Result<u32> {
        let inner = Arc::clone(&self.inner);
        let core = self.core.clone();
        run_blocking(core, move || {
            let h = inner
                .lock()
                .pop(i64::from(index.unwrap_or(-1)))
                .ok_or_else(|| Error::from_reason("pop on empty list"))?;
            u32::try_from(h.raw()).map_err(|_| Error::from_reason("HandleId exceeds u32"))
        })
        .await?
    }

    #[napi]
    pub async fn clear(&self) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        let core = self.core.clone();
        run_blocking(core, move || {
            inner.lock().clear();
            Ok(())
        })
        .await?
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
    core: Core,
    node_id_raw: u32,
}

#[napi]
impl BenchReactiveMap {
    #[napi(factory)]
    pub fn create(
        core: &BenchCore,
        packer: Function<'_, Vec<u32>, u32>,
        max_size: Option<u32>,
        default_ttl: Option<f64>,
    ) -> Result<Self> {
        let tsfn = build_pack_tsfn(packer)?;
        let c = core.core_clone();
        let binding_weak = Arc::downgrade(&core.binding_arc());
        let intern = make_map_intern_fn(tsfn, binding_weak);
        let opts = ReactiveMapOptions {
            default_ttl,
            max_size: max_size.map(|n| n as usize),
            ..Default::default()
        };
        let map = ReactiveMap::new(&c, intern, opts)
            .map_err(|e| Error::from_reason(format!("ReactiveMap::new failed: {e:?}")))?;
        let node_id_raw = u32::try_from(map.node_id.raw())
            .map_err(|_| Error::from_reason("node id exceeds u32"))?;
        Ok(Self {
            inner: Arc::new(parking_lot::Mutex::new(map)),
            core: c,
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
    pub fn has(&self, key: u32) -> bool {
        let k = HandleId::new(u64::from(key));
        self.inner.lock().has(&k)
    }

    #[napi]
    pub fn get(&self, key: u32) -> u32 {
        let k = HandleId::new(u64::from(key));
        self.inner
            .lock()
            .get(&k)
            .map(|h| u32::try_from(h.raw()).unwrap_or(0))
            .unwrap_or(0)
    }

    #[napi]
    pub async fn set(&self, key: u32, value: u32, ttl: Option<f64>) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        let core = self.core.clone();
        let k = HandleId::new(u64::from(key));
        let v = HandleId::new(u64::from(value));
        run_blocking(core, move || {
            inner.lock().set_with_ttl(k, v, ttl);
            Ok(())
        })
        .await?
    }

    #[napi]
    pub async fn delete(&self, key: u32) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        let core = self.core.clone();
        let k = HandleId::new(u64::from(key));
        run_blocking(core, move || {
            inner.lock().delete(&k);
            Ok(())
        })
        .await?
    }

    #[napi]
    pub async fn clear(&self) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        let core = self.core.clone();
        run_blocking(core, move || {
            inner.lock().clear();
            Ok(())
        })
        .await?
    }
}

// ── BenchReactiveIndex ──────────────────────────────────────────────────

#[napi]
pub struct BenchReactiveIndex {
    inner: Arc<parking_lot::Mutex<ReactiveIndex<HandleId, HandleId>>>,
    core: Core,
    node_id_raw: u32,
    /// F20/D205: numeric-primary → value-handle mirror. The core
    /// `ReactiveIndex::range_by_primary` is `K: Ord`-correct and covered by
    /// the `index_range_by_primary_d205` cargo test; the parity arm ranges
    /// over a user-meaningful `i64` key (never opaque `HandleId` order) per
    /// D205. `BTreeMap` range iteration is naturally ascending.
    keyed: Arc<parking_lot::Mutex<std::collections::BTreeMap<i64, u32>>>,
}

#[napi]
impl BenchReactiveIndex {
    #[napi(factory)]
    pub fn create(core: &BenchCore, packer: Function<'_, Vec<u32>, u32>) -> Result<Self> {
        let tsfn = build_pack_tsfn(packer)?;
        let c = core.core_clone();
        let binding_weak = Arc::downgrade(&core.binding_arc());
        let intern = make_index_intern_fn(tsfn, binding_weak);
        let opts = ReactiveIndexOptions::default();
        let index = ReactiveIndex::new(&c, intern, opts);
        let node_id_raw = u32::try_from(index.node_id.raw())
            .map_err(|_| Error::from_reason("node id exceeds u32"))?;
        Ok(Self {
            inner: Arc::new(parking_lot::Mutex::new(index)),
            core: c,
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

    /// `secondary` is a sort-key string (not a handle). `numeric_key`
    /// (F20/D205, optional trailing arg) records this row in the numeric
    /// range mirror so `range_by_primary` can query by user key.
    #[napi]
    pub async fn upsert(
        &self,
        primary: u32,
        secondary: String,
        value: u32,
        numeric_key: Option<i64>,
    ) -> Result<bool> {
        let inner = Arc::clone(&self.inner);
        let core = self.core.clone();
        let p = HandleId::new(u64::from(primary));
        let v = HandleId::new(u64::from(value));
        if let Some(nk) = numeric_key {
            self.keyed.lock().insert(nk, value);
        }
        run_blocking(core, move || Ok(inner.lock().upsert(p, secondary, v))).await?
    }

    #[napi]
    pub async fn delete(&self, primary: u32, numeric_key: Option<i64>) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        let core = self.core.clone();
        let k = HandleId::new(u64::from(primary));
        if let Some(nk) = numeric_key {
            self.keyed.lock().remove(&nk);
        }
        run_blocking(core, move || {
            inner.lock().delete(&k);
            Ok(())
        })
        .await?
    }

    /// F20/D205: values whose numeric primary sorts within `[start, end)`
    /// (inclusive start, exclusive end), ascending by primary key.
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
        let core = self.core.clone();
        self.keyed.lock().clear();
        run_blocking(core, move || {
            inner.lock().clear();
            Ok(())
        })
        .await?
    }
}
