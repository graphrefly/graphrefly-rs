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
use graphrefly_core::Core;

use graphrefly_structures::{
    InternFn, ReactiveIndex, ReactiveIndexOptions, ReactiveList, ReactiveListOptions, ReactiveLog,
    ReactiveLogOptions, ReactiveMap, ReactiveMapOptions,
};

use crate::core_bindings::{run_blocking, BenchBinding, BenchCore};

// ── Shared TSFN infrastructure ──────────────────────────────────────────

type Tsfn<T, R> = ThreadsafeFunction<T, R, T, Status, false, false, 1>;
type PackTsfn = Arc<Tsfn<Vec<u32>, u32>>;

fn build_pack_tsfn(callback: Function<Vec<u32>, u32>) -> Result<PackTsfn> {
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
}

#[napi]
impl BenchReactiveLog {
    /// Create from a BenchCore + packer callback. Sync factory — lightweight
    /// Core mutex operation (same risk profile as BenchOperators::from_core).
    #[napi(factory)]
    pub fn create(
        core: &BenchCore,
        packer: Function<Vec<u32>, u32>,
        max_size: Option<u32>,
    ) -> Result<Self> {
        let tsfn = build_pack_tsfn(packer)?;
        let c = core.core_clone();
        let binding_weak = Arc::downgrade(&core.binding_arc());
        let intern = make_intern_fn(tsfn, binding_weak);
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
    pub fn create(core: &BenchCore, packer: Function<Vec<u32>, u32>) -> Result<Self> {
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
        packer: Function<Vec<u32>, u32>,
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
}

#[napi]
impl BenchReactiveIndex {
    #[napi(factory)]
    pub fn create(core: &BenchCore, packer: Function<Vec<u32>, u32>) -> Result<Self> {
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

    /// `secondary` is a sort-key string (not a handle).
    #[napi]
    pub async fn upsert(&self, primary: u32, secondary: String, value: u32) -> Result<bool> {
        let inner = Arc::clone(&self.inner);
        let core = self.core.clone();
        let p = HandleId::new(u64::from(primary));
        let v = HandleId::new(u64::from(value));
        run_blocking(core, move || Ok(inner.lock().upsert(p, secondary, v))).await?
    }

    #[napi]
    pub async fn delete(&self, primary: u32) -> Result<()> {
        let inner = Arc::clone(&self.inner);
        let core = self.core.clone();
        let k = HandleId::new(u64::from(primary));
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
