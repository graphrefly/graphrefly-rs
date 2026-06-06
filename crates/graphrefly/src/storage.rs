//! Passive storage/read-through helpers for Rust product completeness (D123).
//!
//! This module is deliberately graph-agnostic: it creates no graph nodes, adds no
//! graph storage methods, and does not participate in hydration/restore or wave
//! protocol semantics.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_STORE_ID: AtomicU64 = AtomicU64::new(1);

pub type StorageResult<T> = Result<T, StorageError>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageError {
    Unsupported { label: String, capability: String },
    Backend(String),
}

impl StorageError {
    pub fn backend(message: impl Into<String>) -> Self {
        Self::Backend(message.into())
    }

    pub fn unsupported(label: impl Into<String>, capability: impl Into<String>) -> Self {
        Self::Unsupported {
            label: label.into(),
            capability: capability.into(),
        }
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported { label, capability } => {
                write!(f, "{label}: KV tier does not support {capability}")
            }
            Self::Backend(message) => f.write_str(message),
        }
    }
}

impl Error for StorageError {}

/// Opaque D108 per-key generation token for typed KV versioned reads.
#[derive(Clone)]
pub struct KvGeneration {
    store_id: u64,
    epoch: u64,
    key: String,
    version: u64,
}

impl KvGeneration {
    fn new(store_id: u64, epoch: u64, key: &str, version: u64) -> Self {
        Self {
            store_id,
            epoch,
            key: key.to_owned(),
            version,
        }
    }
}

impl fmt::Debug for KvGeneration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("KvGeneration(<opaque>)")
    }
}

#[derive(Clone, Debug)]
pub enum KvVersionedRead<T> {
    Hit { value: T, generation: KvGeneration },
    Miss { generation: KvGeneration },
}

impl<T> KvVersionedRead<T> {
    pub fn generation(&self) -> &KvGeneration {
        match self {
            Self::Hit { generation, .. } | Self::Miss { generation } => generation,
        }
    }
}

/// Typed string-key KV tier. Versioning is a narrow optional D108 capability.
pub trait KvStorageTier<T: Clone> {
    fn get(&self, key: &str) -> StorageResult<Option<T>>;
    fn set(&self, key: &str, value: T) -> StorageResult<()>;
    fn delete(&self, key: &str) -> StorageResult<()>;
    fn list(&self, prefix: &str) -> StorageResult<Vec<String>>;

    fn supports_versioned(&self) -> bool {
        false
    }

    fn get_versioned(&self, _key: &str) -> StorageResult<KvVersionedRead<T>> {
        Err(StorageError::unsupported(
            "kvStorage",
            "versioned get/set-if-match",
        ))
    }

    fn set_if_match(
        &self,
        _key: &str,
        _value: T,
        _generation: &KvGeneration,
    ) -> StorageResult<bool> {
        Err(StorageError::unsupported(
            "kvStorage",
            "versioned get/set-if-match",
        ))
    }
}

#[derive(Debug)]
struct MemoryEntry<T> {
    value: T,
    version: u64,
}

#[derive(Debug)]
struct MemoryKvInner<T> {
    store_id: u64,
    epoch: Cell<u64>,
    entries: RefCell<HashMap<String, MemoryEntry<T>>>,
    tombstones: RefCell<HashMap<String, u64>>,
    next_version: Cell<u64>,
}

/// In-memory typed KV tier with D108 opaque-generation support.
#[derive(Clone, Debug)]
pub struct MemoryKv<T: Clone> {
    inner: Rc<MemoryKvInner<T>>,
}

pub fn memory_kv<T: Clone>() -> MemoryKv<T> {
    MemoryKv {
        inner: Rc::new(MemoryKvInner {
            store_id: NEXT_STORE_ID.fetch_add(1, Ordering::Relaxed),
            epoch: Cell::new(0),
            entries: RefCell::new(HashMap::new()),
            tombstones: RefCell::new(HashMap::new()),
            next_version: Cell::new(1),
        }),
    }
}

pub fn dict_kv<T: Clone>(entries: impl IntoIterator<Item = (impl Into<String>, T)>) -> MemoryKv<T> {
    let kv = memory_kv();
    for (key, value) in entries {
        kv.set(&key.into(), value)
            .expect("memory_kv set is infallible");
    }
    kv
}

impl<T: Clone> MemoryKv<T> {
    fn bump_version(&self) -> u64 {
        let version = self.inner.next_version.get();
        self.inner.next_version.set(version + 1);
        version
    }

    fn current_version(&self, key: &str) -> u64 {
        if let Some(entry) = self.inner.entries.borrow().get(key) {
            entry.version
        } else {
            self.inner
                .tombstones
                .borrow()
                .get(key)
                .copied()
                .unwrap_or(0)
        }
    }

    fn generation_for(&self, key: &str) -> KvGeneration {
        KvGeneration::new(
            self.inner.store_id,
            self.inner.epoch.get(),
            key,
            self.current_version(key),
        )
    }

    pub fn clear(&self) {
        self.inner.entries.borrow_mut().clear();
        self.inner.tombstones.borrow_mut().clear();
        self.inner.epoch.set(self.inner.epoch.get() + 1);
        self.inner.next_version.set(1);
    }
}

impl<T: Clone> KvStorageTier<T> for MemoryKv<T> {
    fn get(&self, key: &str) -> StorageResult<Option<T>> {
        Ok(self
            .inner
            .entries
            .borrow()
            .get(key)
            .map(|entry| entry.value.clone()))
    }

    fn set(&self, key: &str, value: T) -> StorageResult<()> {
        let version = self.bump_version();
        self.inner
            .entries
            .borrow_mut()
            .insert(key.to_owned(), MemoryEntry { value, version });
        self.inner.tombstones.borrow_mut().remove(key);
        Ok(())
    }

    fn delete(&self, key: &str) -> StorageResult<()> {
        if self.inner.entries.borrow_mut().remove(key).is_some() {
            let version = self.bump_version();
            self.inner
                .tombstones
                .borrow_mut()
                .insert(key.to_owned(), version);
        }
        Ok(())
    }

    fn list(&self, prefix: &str) -> StorageResult<Vec<String>> {
        let mut keys = self
            .inner
            .entries
            .borrow()
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect::<Vec<_>>();
        keys.sort();
        Ok(keys)
    }

    fn supports_versioned(&self) -> bool {
        true
    }

    fn get_versioned(&self, key: &str) -> StorageResult<KvVersionedRead<T>> {
        if let Some(entry) = self.inner.entries.borrow().get(key) {
            return Ok(KvVersionedRead::Hit {
                value: entry.value.clone(),
                generation: KvGeneration::new(
                    self.inner.store_id,
                    self.inner.epoch.get(),
                    key,
                    entry.version,
                ),
            });
        }
        Ok(KvVersionedRead::Miss {
            generation: self.generation_for(key),
        })
    }

    fn set_if_match(&self, key: &str, value: T, generation: &KvGeneration) -> StorageResult<bool> {
        if generation.store_id != self.inner.store_id
            || generation.epoch != self.inner.epoch.get()
            || generation.key != key
            || generation.version != self.current_version(key)
        {
            return Ok(false);
        }
        self.set(key, value)?;
        Ok(true)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadThroughLookupTier {
    pub index: isize,
    pub name: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadThroughOutcome {
    Hit,
    Miss,
    Error,
}

#[derive(Clone, Debug)]
pub struct ReadThroughLookupFact<T> {
    pub outcome: ReadThroughOutcome,
    pub key: String,
    pub tier: ReadThroughLookupTier,
    pub value: Option<T>,
    pub generation: Option<KvGeneration>,
    pub error: Option<StorageError>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReadThroughPromotionFact {
    pub tier: ReadThroughLookupTier,
    pub ok: bool,
    pub error: Option<StorageError>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TieredReadThroughStatus {
    Hit,
    Miss,
    Error,
}

#[derive(Clone, Debug)]
pub struct TieredReadThroughResult<T> {
    pub status: TieredReadThroughStatus,
    pub key: String,
    pub value: Option<T>,
    pub hit_tier: Option<ReadThroughLookupTier>,
    pub facts: Vec<ReadThroughLookupFact<T>>,
    pub promotions: Vec<ReadThroughPromotionFact>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum PromotionPolicy {
    #[default]
    AllEarlier,
    Disabled,
    Indices(Vec<usize>),
}

pub struct ReadThroughMissContext {
    pub key: String,
    pub tier: ReadThroughLookupTier,
}

pub struct ReadThroughErrorContext {
    pub key: String,
    pub tier: ReadThroughLookupTier,
    pub stage: ReadThroughErrorStage,
    pub error: StorageError,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadThroughErrorStage {
    Lookup,
    Promotion,
}

pub type ReadThroughLoadFn<'a, T> = dyn Fn(&str) -> StorageResult<Option<T>> + 'a;
pub type ReadThroughMissFn<'a> = dyn Fn(ReadThroughMissContext) + 'a;
pub type ReadThroughErrorFn<'a> = dyn Fn(ReadThroughErrorContext) + 'a;

pub struct TieredReadThroughOptions<'a, T: Clone> {
    pub key: String,
    pub tiers: Vec<&'a dyn KvStorageTier<T>>,
    pub tier_names: Vec<String>,
    pub load: Option<Box<ReadThroughLoadFn<'a, T>>>,
    pub promote_to: PromotionPolicy,
    pub on_miss: Option<Box<ReadThroughMissFn<'a>>>,
    pub on_error: Option<Box<ReadThroughErrorFn<'a>>>,
}

impl<'a, T: Clone> TieredReadThroughOptions<'a, T> {
    pub fn new(key: impl Into<String>, tiers: Vec<&'a dyn KvStorageTier<T>>) -> Self {
        Self {
            key: key.into(),
            tiers,
            tier_names: Vec::new(),
            load: None,
            promote_to: PromotionPolicy::AllEarlier,
            on_miss: None,
            on_error: None,
        }
    }
}

pub fn read_through_kv<T: Clone>(
    opts: TieredReadThroughOptions<'_, T>,
) -> TieredReadThroughResult<T> {
    tiered_read_through(opts)
}

/// Graph-agnostic tiered lookup + optional promotion helper (D104/D123).
pub fn tiered_read_through<T: Clone>(
    opts: TieredReadThroughOptions<'_, T>,
) -> TieredReadThroughResult<T> {
    let TieredReadThroughOptions {
        key,
        tiers,
        tier_names,
        load,
        promote_to,
        on_miss,
        on_error,
    } = opts;

    let mut facts = Vec::new();
    let mut promotions = Vec::new();
    let mut hit_tier = None;
    let mut value = None;

    for (index, tier) in tiers.iter().enumerate() {
        let info = lookup_tier(index as isize, &tier_names);
        let read = if tier.supports_versioned() {
            match tier.get_versioned(&key) {
                Ok(KvVersionedRead::Hit { value, generation }) => {
                    Ok((Some(value), Some(generation)))
                }
                Ok(KvVersionedRead::Miss { generation }) => Ok((None, Some(generation))),
                Err(error) => Err(error),
            }
        } else {
            tier.get(&key).map(|found| (found, None))
        };

        match read {
            Ok((Some(found), generation)) => {
                facts.push(ReadThroughLookupFact {
                    outcome: ReadThroughOutcome::Hit,
                    key: key.clone(),
                    tier: info.clone(),
                    value: Some(found.clone()),
                    generation,
                    error: None,
                });
                hit_tier = Some(info);
                value = Some(found);
                break;
            }
            Ok((None, generation)) => {
                facts.push(ReadThroughLookupFact {
                    outcome: ReadThroughOutcome::Miss,
                    key: key.clone(),
                    tier: info.clone(),
                    value: None,
                    generation,
                    error: None,
                });
                call_on_miss(&on_miss, &key, info);
            }
            Err(error) => {
                facts.push(ReadThroughLookupFact {
                    outcome: ReadThroughOutcome::Error,
                    key: key.clone(),
                    tier: info.clone(),
                    value: None,
                    generation: None,
                    error: Some(error.clone()),
                });
                call_on_error(&on_error, &key, info, ReadThroughErrorStage::Lookup, error);
            }
        }
    }

    if hit_tier.is_none() {
        if let Some(load) = load {
            let loader_tier = ReadThroughLookupTier {
                index: -1,
                name: Some("load".to_owned()),
            };
            match load(&key) {
                Ok(Some(loaded)) => {
                    facts.push(ReadThroughLookupFact {
                        outcome: ReadThroughOutcome::Hit,
                        key: key.clone(),
                        tier: loader_tier.clone(),
                        value: Some(loaded.clone()),
                        generation: None,
                        error: None,
                    });
                    hit_tier = Some(loader_tier);
                    value = Some(loaded);
                }
                Ok(None) => {
                    facts.push(ReadThroughLookupFact {
                        outcome: ReadThroughOutcome::Miss,
                        key: key.clone(),
                        tier: loader_tier.clone(),
                        value: None,
                        generation: None,
                        error: None,
                    });
                    call_on_miss(&on_miss, &key, loader_tier);
                }
                Err(error) => {
                    facts.push(ReadThroughLookupFact {
                        outcome: ReadThroughOutcome::Error,
                        key: key.clone(),
                        tier: loader_tier.clone(),
                        value: None,
                        generation: None,
                        error: Some(error.clone()),
                    });
                    call_on_error(
                        &on_error,
                        &key,
                        loader_tier,
                        ReadThroughErrorStage::Lookup,
                        error,
                    );
                }
            }
        }
    }

    if let (Some(found), Some(source_tier)) = (value.as_ref(), hit_tier.as_ref()) {
        let source_index = if source_tier.index < 0 {
            tiers.len()
        } else {
            source_tier.index as usize
        };
        for index in build_promotion_targets(tiers.len(), source_index, &promote_to) {
            let tier = tiers[index];
            let info = lookup_tier(index as isize, &tier_names);
            let write = if tier.supports_versioned() {
                if let Some(generation) = generation_for_tier(&facts, index) {
                    tier.set_if_match(&key, found.clone(), generation)
                } else {
                    Err(StorageError::backend(
                        "tiered_read_through: versioned promotion target was not observed with a generation",
                    ))
                }
            } else {
                tier.set(&key, found.clone()).map(|()| true)
            };

            match write {
                Ok(ok) => promotions.push(ReadThroughPromotionFact {
                    tier: info,
                    ok,
                    error: None,
                }),
                Err(error) => {
                    promotions.push(ReadThroughPromotionFact {
                        tier: info.clone(),
                        ok: false,
                        error: Some(error.clone()),
                    });
                    call_on_error(
                        &on_error,
                        &key,
                        info,
                        ReadThroughErrorStage::Promotion,
                        error,
                    );
                }
            }
        }
    }

    let status = if hit_tier.is_some() && value.is_some() {
        TieredReadThroughStatus::Hit
    } else if facts
        .iter()
        .any(|fact| fact.outcome == ReadThroughOutcome::Error)
    {
        TieredReadThroughStatus::Error
    } else {
        TieredReadThroughStatus::Miss
    };

    TieredReadThroughResult {
        status,
        key,
        value,
        hit_tier,
        facts,
        promotions,
    }
}

fn lookup_tier(index: isize, tier_names: &[String]) -> ReadThroughLookupTier {
    ReadThroughLookupTier {
        index,
        name: tier_names.get(index.max(0) as usize).cloned(),
    }
}

fn generation_for_tier<T>(
    facts: &[ReadThroughLookupFact<T>],
    index: usize,
) -> Option<&KvGeneration> {
    facts
        .iter()
        .find(|fact| fact.tier.index == index as isize)
        .and_then(|fact| fact.generation.as_ref())
}

fn build_promotion_targets(
    tier_count: usize,
    hit_index: usize,
    promote_to: &PromotionPolicy,
) -> Vec<usize> {
    if tier_count == 0 {
        return Vec::new();
    }
    let max_promote = hit_index.min(tier_count);
    match promote_to {
        PromotionPolicy::Disabled => Vec::new(),
        PromotionPolicy::AllEarlier => (0..max_promote).collect(),
        PromotionPolicy::Indices(indices) => {
            let mut out = Vec::new();
            for &index in indices {
                if index < tier_count && index < max_promote && !out.contains(&index) {
                    out.push(index);
                }
            }
            out
        }
    }
}

fn call_on_miss(
    on_miss: &Option<Box<ReadThroughMissFn<'_>>>,
    key: &str,
    tier: ReadThroughLookupTier,
) {
    if let Some(on_miss) = on_miss {
        let _ = catch_unwind(AssertUnwindSafe(|| {
            on_miss(ReadThroughMissContext {
                key: key.to_owned(),
                tier,
            });
        }));
    }
}

fn call_on_error(
    on_error: &Option<Box<ReadThroughErrorFn<'_>>>,
    key: &str,
    tier: ReadThroughLookupTier,
    stage: ReadThroughErrorStage,
    error: StorageError,
) {
    if let Some(on_error) = on_error {
        let _ = catch_unwind(AssertUnwindSafe(|| {
            on_error(ReadThroughErrorContext {
                key: key.to_owned(),
                tier,
                stage,
                error,
            });
        }));
    }
}
