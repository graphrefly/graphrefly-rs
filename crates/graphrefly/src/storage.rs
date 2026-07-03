//! Passive storage/read-through helpers for Rust product completeness (D123).
//!
//! This module is deliberately graph-agnostic: it creates no graph nodes, adds no
//! graph storage methods, and does not participate in hydration/restore or wave
//! protocol semantics.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::data_structures::{IndexChange, IndexRow, ListChange, LogChange, MapChange};
use crate::json::{strict_canonical_json_bytes, strict_json_decode, Codec, JsonCodecError};

static NEXT_STORE_ID: AtomicU64 = AtomicU64::new(1);

pub type StorageResult<T> = Result<T, StorageError>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageError {
    Unsupported { label: String, capability: String },
    ContentAddressedMiss { key: String },
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
            Self::ContentAddressedMiss { key } => {
                write!(
                    f,
                    "content-addressed lookup miss in read-strict mode: {key}"
                )
            }
            Self::Backend(message) => f.write_str(message),
        }
    }
}

impl Error for StorageError {}

pub trait ByteStorageBackend {
    fn get(&self, key: &str) -> StorageResult<Option<Vec<u8>>>;
    fn put(&self, key: &str, value: &[u8]) -> StorageResult<()>;
    fn put_if_absent(&self, _key: &str, _value: &[u8]) -> StorageResult<bool> {
        Err(StorageError::unsupported("byteStorage", "put-if-absent"))
    }
    fn delete(&self, key: &str) -> StorageResult<()>;
    fn list(&self, prefix: &str) -> StorageResult<Vec<String>>;
}

const FILE_STEM_PREFIX: &str = "k-";
const DEFAULT_FILE_EXTENSION: &str = ".bin";
const STORAGE_NAMESPACE_PREFIX: &str = "storage-namespace";
const STORAGE_NAMESPACE_PREFIX_WITH_COLON: &str = "storage-namespace:";

fn storage_tuple_key(parts: &[&str]) -> String {
    serde_json::to_string(parts).expect("storage tuple key encoding cannot fail")
}

fn parse_storage_tuple_key(value: &str) -> Option<Vec<String>> {
    serde_json::from_str::<Vec<String>>(value).ok()
}

fn storage_physical_key(namespace: &str, logical_key: &str) -> String {
    format!(
        "{STORAGE_NAMESPACE_PREFIX}:{}",
        storage_tuple_key(&[namespace, logical_key])
    )
}

fn decode_storage_physical_key(
    namespace: &str,
    raw_key: &str,
    malformed_message: &'static str,
) -> StorageResult<Option<String>> {
    let Some(tuple_key) = raw_key.strip_prefix(STORAGE_NAMESPACE_PREFIX_WITH_COLON) else {
        return Ok(None);
    };
    let Some(tuple) = parse_storage_tuple_key(tuple_key) else {
        return Err(StorageError::backend(malformed_message));
    };
    if tuple.first().map(String::as_str) != Some(namespace) {
        return Ok(None);
    }
    if tuple.len() != 2 {
        return Err(StorageError::backend(malformed_message));
    }
    Ok(Some(tuple[1].clone()))
}

fn content_addressed_storage_key(prefix: &str, hash_hex: &str) -> String {
    format!("{prefix}:{}", storage_tuple_key(&[hash_hex]))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileBackendOptions {
    pub namespace: String,
    pub extension: String,
}

impl Default for FileBackendOptions {
    fn default() -> Self {
        Self {
            namespace: String::new(),
            extension: DEFAULT_FILE_EXTENSION.to_owned(),
        }
    }
}

impl FileBackendOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    pub fn with_extension(mut self, extension: impl Into<String>) -> Self {
        self.extension = extension.into();
        self
    }
}

#[derive(Clone, Debug)]
pub struct FileBackend {
    dir: PathBuf,
    namespace: String,
    extension: String,
}

pub fn file_backend(
    dir: impl Into<PathBuf>,
    opts: FileBackendOptions,
) -> StorageResult<FileBackend> {
    validate_namespace("fileBackend", &opts.namespace)?;
    validate_extension(&opts.extension)?;
    Ok(FileBackend {
        dir: dir.into(),
        namespace: opts.namespace,
        extension: opts.extension,
    })
}

impl FileBackend {
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn storage_key(&self, key: &str) -> StorageResult<String> {
        validate_logical_key("fileBackend", key)?;
        Ok(storage_physical_key(&self.namespace, key))
    }

    fn path_for(&self, key: &str) -> StorageResult<PathBuf> {
        let stem = key_to_file_stem(&self.storage_key(key)?);
        Ok(self
            .dir
            .join(format!("{FILE_STEM_PREFIX}{stem}{}", self.extension)))
    }

    fn key_from_filename(&self, filename: &str) -> StorageResult<Option<String>> {
        if filename.starts_with('.') || !filename.ends_with(&self.extension) {
            return Ok(None);
        }
        let stem = &filename[..filename.len() - self.extension.len()];
        let Some(raw_stem) = stem.strip_prefix(FILE_STEM_PREFIX) else {
            return Ok(None);
        };
        let Some(key) = file_stem_to_key(raw_stem) else {
            return Ok(None);
        };
        decode_storage_physical_key(&self.namespace, &key, "fileBackend: malformed stored key")
    }
}

impl ByteStorageBackend for FileBackend {
    fn get(&self, key: &str) -> StorageResult<Option<Vec<u8>>> {
        match fs::read(self.path_for(key)?) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(StorageError::backend(format!("fileBackend.get: {error}"))),
        }
    }

    fn put(&self, key: &str, value: &[u8]) -> StorageResult<()> {
        fs::create_dir_all(&self.dir)
            .map_err(|err| StorageError::backend(format!("fileBackend.put: {err}")))?;
        let file_path = self.path_for(key)?;
        let file_name = file_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| StorageError::backend("fileBackend.put: invalid file name"))?;
        let tmp = write_temp_file(&self.dir, file_name, value, "fileBackend.put")?;
        if let Err(error) = fs::rename(&tmp, &file_path) {
            let _ = fs::remove_file(&tmp);
            return Err(StorageError::backend(format!("fileBackend.put: {error}")));
        }
        Ok(())
    }

    fn put_if_absent(&self, key: &str, value: &[u8]) -> StorageResult<bool> {
        fs::create_dir_all(&self.dir)
            .map_err(|err| StorageError::backend(format!("fileBackend.put_if_absent: {err}")))?;
        let file_path = self.path_for(key)?;
        let file_name = file_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| StorageError::backend("fileBackend.put_if_absent: invalid file name"))?;
        let tmp = write_temp_file(&self.dir, file_name, value, "fileBackend.put_if_absent")?;
        match fs::hard_link(&tmp, &file_path) {
            Ok(()) => {
                let _ = fs::remove_file(&tmp);
                Ok(true)
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let _ = fs::remove_file(&tmp);
                Ok(false)
            }
            Err(error) => {
                let _ = fs::remove_file(&tmp);
                Err(StorageError::backend(format!(
                    "fileBackend.put_if_absent: {error}"
                )))
            }
        }
    }

    fn delete(&self, key: &str) -> StorageResult<()> {
        match fs::remove_file(self.path_for(key)?) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(StorageError::backend(format!(
                "fileBackend.delete: {error}"
            ))),
        }
    }

    fn list(&self, prefix: &str) -> StorageResult<Vec<String>> {
        validate_list_prefix("fileBackend", prefix)?;
        let entries = match fs::read_dir(&self.dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(StorageError::backend(format!("fileBackend.list: {error}"))),
        };
        let mut keys = Vec::new();
        for entry in entries {
            let entry =
                entry.map_err(|err| StorageError::backend(format!("fileBackend.list: {err}")))?;
            let Some(filename) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if let Some(key) = self.key_from_filename(&filename)? {
                if key.starts_with(prefix) {
                    keys.push(key);
                }
            }
        }
        keys.sort();
        Ok(keys)
    }
}

fn validate_namespace(label: &str, value: &str) -> StorageResult<()> {
    let _ = (label, value);
    Ok(())
}

fn validate_logical_key(label: &str, value: &str) -> StorageResult<()> {
    let _ = (label, value);
    Ok(())
}

fn validate_list_prefix(label: &str, value: &str) -> StorageResult<()> {
    let _ = (label, value);
    Ok(())
}

fn validate_extension(extension: &str) -> StorageResult<()> {
    let valid = extension.len() >= 2
        && extension.starts_with('.')
        && !extension.contains("..")
        && !extension.contains('/')
        && !extension.contains('\\')
        && !extension.contains('\0')
        && extension
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
    if valid {
        Ok(())
    } else {
        Err(StorageError::backend(
            "fileBackend: extension must be a simple suffix such as .bin",
        ))
    }
}

fn temp_file_path(dir: &Path, file_name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    dir.join(format!(
        ".{file_name}.{}.{}.{}.tmp",
        std::process::id(),
        nanos,
        NEXT_STORE_ID.fetch_add(1, Ordering::Relaxed)
    ))
}

fn write_temp_file(
    dir: &Path,
    file_name: &str,
    value: &[u8],
    label: &str,
) -> StorageResult<PathBuf> {
    for _ in 0..16 {
        let tmp = temp_file_path(dir, file_name);
        let mut file = match OpenOptions::new().write(true).create_new(true).open(&tmp) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(StorageError::backend(format!("{label}: {error}"))),
        };
        if let Err(error) = file.write_all(value).and_then(|_| file.sync_all()) {
            let _ = fs::remove_file(&tmp);
            return Err(StorageError::backend(format!("{label}: {error}")));
        }
        return Ok(tmp);
    }
    Err(StorageError::backend(format!(
        "{label}: could not allocate a unique temporary file"
    )))
}

fn key_to_file_stem(key: &str) -> String {
    let mut out = String::new();
    for byte in key.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-') {
            out.push(byte as char);
        } else {
            out.push('%');
            out.push(hex_nibble(byte >> 4));
            out.push(hex_nibble(byte & 0x0f));
        }
    }
    out
}

fn hex_nibble(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        _ => (b'a' + (value - 10)) as char,
    }
}

fn file_stem_to_key(stem: &str) -> Option<String> {
    let bytes = stem.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            let hi = (bytes[index + 1] as char).to_digit(16)?;
            let lo = (bytes[index + 2] as char).to_digit(16)?;
            out.push(((hi << 4) | lo) as u8);
            index += 3;
        } else if bytes[index].is_ascii() {
            out.push(bytes[index]);
            index += 1;
        } else {
            return None;
        }
    }
    let key = String::from_utf8(out).ok()?;
    if key_to_file_stem(&key) == stem {
        Some(key)
    } else {
        None
    }
}

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
    fn put_if_absent(&self, _key: &str, _value: T) -> StorageResult<bool> {
        Err(StorageError::unsupported("kvStorage", "put-if-absent"))
    }
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

    fn put_if_absent(&self, key: &str, value: T) -> StorageResult<bool> {
        if self.inner.entries.borrow().contains_key(key) {
            return Ok(false);
        }
        self.set(key, value)?;
        Ok(true)
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

#[derive(Clone, Debug)]
pub struct CodecKvStorage<B, C, T> {
    backend: B,
    codec: C,
    marker: std::marker::PhantomData<T>,
}

pub fn codec_kv_storage<B, C, T>(backend: B, codec: C) -> CodecKvStorage<B, C, T>
where
    B: ByteStorageBackend + Clone,
    C: Codec<T> + Clone,
    T: Clone,
{
    CodecKvStorage {
        backend,
        codec,
        marker: std::marker::PhantomData,
    }
}

impl<B, C, T> KvStorageTier<T> for CodecKvStorage<B, C, T>
where
    B: ByteStorageBackend + Clone,
    C: Codec<T> + Clone,
    T: Clone,
{
    fn get(&self, key: &str) -> StorageResult<Option<T>> {
        self.backend
            .get(key)?
            .map(|bytes| self.codec.decode(&bytes).map_err(storage_json_error))
            .transpose()
    }

    fn set(&self, key: &str, value: T) -> StorageResult<()> {
        let bytes = self.codec.encode(&value).map_err(storage_json_error)?;
        self.backend.put(key, &bytes)
    }

    fn put_if_absent(&self, key: &str, value: T) -> StorageResult<bool> {
        let bytes = self.codec.encode(&value).map_err(storage_json_error)?;
        self.backend.put_if_absent(key, &bytes)
    }

    fn delete(&self, key: &str) -> StorageResult<()> {
        self.backend.delete(key)
    }

    fn list(&self, prefix: &str) -> StorageResult<Vec<String>> {
        self.backend.list(prefix)
    }
}

pub type FileKv<T, C> = CodecKvStorage<FileBackend, C, T>;

pub fn file_kv<T, C>(
    dir: impl Into<PathBuf>,
    opts: FileBackendOptions,
    codec: C,
) -> StorageResult<FileKv<T, C>>
where
    C: Codec<T> + Clone,
    T: Clone,
{
    Ok(codec_kv_storage(file_backend(dir, opts)?, codec))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileAppendLogOptions {
    pub backend: FileBackendOptions,
    pub prefix: String,
}

impl Default for FileAppendLogOptions {
    fn default() -> Self {
        Self {
            backend: FileBackendOptions::default(),
            prefix: "event-log".to_owned(),
        }
    }
}

impl FileAppendLogOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_backend(mut self, backend: FileBackendOptions) -> Self {
        self.backend = backend;
        self
    }

    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }
}

pub fn file_append_log<T, C>(
    dir: impl Into<PathBuf>,
    opts: FileAppendLogOptions,
    codec: C,
) -> StorageResult<AppendLogStorage<T>>
where
    C: Codec<T> + Clone + 'static,
    T: Clone + 'static,
{
    let kv = file_kv(dir, opts.backend, codec)?;
    Ok(append_log_storage(Rc::new(kv), opts.prefix))
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ContentAddressedMode {
    Read,
    Write,
    #[default]
    ReadWrite,
    ReadStrict,
}

pub type ContentAddressedKeyContext<Ctx> = dyn Fn(&Ctx) -> Result<Value, JsonCodecError>;

pub struct ContentAddressedKvOptions<Ctx, V: Clone> {
    pub kv: Rc<dyn KvStorageTier<V>>,
    pub key_context: Rc<ContentAddressedKeyContext<Ctx>>,
    pub key_prefix: Option<String>,
    pub mode: ContentAddressedMode,
}

impl<V: Clone> ContentAddressedKvOptions<Value, V> {
    pub fn new(kv: Rc<dyn KvStorageTier<V>>) -> Self {
        Self {
            kv,
            key_context: Rc::new(|ctx| Ok(ctx.clone())),
            key_prefix: None,
            mode: ContentAddressedMode::ReadWrite,
        }
    }
}

impl<Ctx, V: Clone> ContentAddressedKvOptions<Ctx, V> {
    pub fn from_key_context(
        kv: Rc<dyn KvStorageTier<V>>,
        f: impl Fn(&Ctx) -> Result<Value, JsonCodecError> + 'static,
    ) -> Self {
        Self {
            kv,
            key_context: Rc::new(f),
            key_prefix: None,
            mode: ContentAddressedMode::ReadWrite,
        }
    }

    pub fn with_key_context(
        mut self,
        f: impl Fn(&Ctx) -> Result<Value, JsonCodecError> + 'static,
    ) -> Self {
        self.key_context = Rc::new(f);
        self
    }

    pub fn with_key_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.key_prefix = Some(prefix.into());
        self
    }

    pub fn with_mode(mut self, mode: ContentAddressedMode) -> Self {
        self.mode = mode;
        self
    }
}

#[derive(Clone)]
pub struct ContentAddressedKv<Ctx, V: Clone> {
    kv: Rc<dyn KvStorageTier<V>>,
    key_context: Rc<ContentAddressedKeyContext<Ctx>>,
    key_prefix: Option<String>,
    mode: ContentAddressedMode,
}

impl<Ctx, V: Clone> fmt::Debug for ContentAddressedKv<Ctx, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ContentAddressedKv")
            .field("key_prefix", &self.key_prefix)
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

pub type ContentAddressedStorage<Ctx, V> = ContentAddressedKv<Ctx, V>;
pub type ContentAddressedStorageOptions<Ctx, V> = ContentAddressedKvOptions<Ctx, V>;

pub fn content_addressed_kv<Ctx, V: Clone>(
    opts: ContentAddressedKvOptions<Ctx, V>,
) -> ContentAddressedKv<Ctx, V> {
    ContentAddressedKv {
        kv: opts.kv,
        key_context: opts.key_context,
        key_prefix: opts.key_prefix,
        mode: opts.mode,
    }
}

pub fn content_addressed_storage<Ctx, V: Clone>(
    opts: ContentAddressedStorageOptions<Ctx, V>,
) -> ContentAddressedStorage<Ctx, V> {
    content_addressed_kv(opts)
}

impl<Ctx, V: Clone> ContentAddressedKv<Ctx, V> {
    pub fn key_for(&self, ctx: &Ctx) -> StorageResult<String> {
        let context = (self.key_context)(ctx).map_err(storage_json_error)?;
        let bytes = strict_canonical_json_bytes(&context).map_err(storage_json_error)?;
        let hex = sha256_hex(&bytes);
        Ok(match &self.key_prefix {
            Some(prefix) => content_addressed_storage_key(prefix, &hex),
            None => hex,
        })
    }

    pub fn lookup(&self, ctx: &Ctx) -> StorageResult<Option<V>> {
        if self.mode == ContentAddressedMode::Write {
            return Ok(None);
        }
        let key = self.key_for(ctx)?;
        let value = self.kv.get(&key)?;
        if value.is_none() && self.mode == ContentAddressedMode::ReadStrict {
            return Err(StorageError::ContentAddressedMiss { key });
        }
        Ok(value)
    }

    pub fn store(&self, ctx: &Ctx, value: V) -> StorageResult<()> {
        if self.mode == ContentAddressedMode::Read {
            return Ok(());
        }
        let key = self.key_for(ctx)?;
        self.kv.set(&key, value)
    }

    pub fn forget(&self, ctx: &Ctx) -> StorageResult<()> {
        if matches!(
            self.mode,
            ContentAddressedMode::Read | ContentAddressedMode::Write
        ) {
            return Ok(());
        }
        let key = self.key_for(ctx)?;
        self.kv.delete(&key)
    }
}

fn storage_json_error(error: JsonCodecError) -> StorageError {
    StorageError::backend(error.to_string())
}

fn sha256_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChangeLifecycle {
    Spec,
    Data,
    Ownership,
}

pub const WAL_KEY_SEGMENT: &str = "wal";
pub const WAL_FRAME_SEQ_PAD: usize = 20;
pub const WAL_FORMAT_VERSION: u64 = 1;

pub type WalFrameTimestampNs = String;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WalFrameBody<T> {
    pub t: String,
    pub lifecycle: ChangeLifecycle,
    pub path: String,
    pub change: T,
    pub frame_seq: u64,
    pub frame_t_ns: WalFrameTimestampNs,
    pub format_version: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WalFrame<T> {
    pub t: String,
    pub lifecycle: ChangeLifecycle,
    pub path: String,
    pub change: T,
    pub frame_seq: u64,
    pub frame_t_ns: WalFrameTimestampNs,
    pub format_version: u64,
    pub checksum: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WalFrameOptions<T> {
    pub path: String,
    pub change: T,
    pub frame_seq: u64,
    pub lifecycle: ChangeLifecycle,
    pub frame_t_ns: Option<String>,
}

impl<T> WalFrameOptions<T> {
    pub fn new(path: impl Into<String>, change: T, frame_seq: u64) -> Self {
        Self {
            path: path.into(),
            change,
            frame_seq,
            lifecycle: ChangeLifecycle::Data,
            frame_t_ns: None,
        }
    }

    pub fn with_lifecycle(mut self, lifecycle: ChangeLifecycle) -> Self {
        self.lifecycle = lifecycle;
        self
    }

    pub fn with_frame_t_ns(mut self, frame_t_ns: impl Into<String>) -> Self {
        self.frame_t_ns = Some(frame_t_ns.into());
        self
    }
}

pub fn wal_frame_prefix(namespace: &str) -> String {
    if namespace.is_empty() {
        WAL_KEY_SEGMENT.to_owned()
    } else {
        format!("{namespace}/{WAL_KEY_SEGMENT}")
    }
}

pub fn wal_frame_key(prefix: &str, frame_seq: u64) -> String {
    format!("{prefix}/{frame_seq:0>width$}", width = WAL_FRAME_SEQ_PAD)
}

pub fn wal_frame_checksum<T: Serialize>(body: &WalFrameBody<T>) -> StorageResult<String> {
    assert_wal_frame_body(body).map_err(storage_json_error)?;
    let value = serde_json::to_value(body).map_err(|err| StorageError::backend(err.to_string()))?;
    assert_wal_frame_body_value(&value, "walFrameCodec").map_err(storage_json_error)?;
    let bytes = strict_canonical_json_bytes(&value).map_err(storage_json_error)?;
    Ok(sha256_hex(&bytes))
}

pub fn wal_frame<T: Serialize>(opts: WalFrameOptions<T>) -> StorageResult<WalFrame<T>> {
    let body = WalFrameBody {
        t: "c".to_owned(),
        lifecycle: opts.lifecycle,
        path: opts.path,
        change: opts.change,
        frame_seq: opts.frame_seq,
        frame_t_ns: match opts.frame_t_ns {
            Some(value) => crate::json::assert_non_negative_decimal_integer_string(
                value,
                "walFrameCodec: frame_t_ns",
            )
            .map_err(storage_json_error)?,
            None => now_ns(),
        },
        format_version: WAL_FORMAT_VERSION,
    };
    let checksum = wal_frame_checksum(&body)?;
    Ok(WalFrame {
        t: body.t,
        lifecycle: body.lifecycle,
        path: body.path,
        change: body.change,
        frame_seq: body.frame_seq,
        frame_t_ns: body.frame_t_ns,
        format_version: body.format_version,
        checksum,
    })
}

pub fn assert_wal_frame<T: Serialize>(frame: &WalFrame<T>) -> crate::json::JsonCodecResult<()> {
    assert_wal_frame_body(&WalFrameBody {
        t: frame.t.clone(),
        lifecycle: frame.lifecycle.clone(),
        path: frame.path.clone(),
        change: &frame.change,
        frame_seq: frame.frame_seq,
        frame_t_ns: frame.frame_t_ns.clone(),
        format_version: frame.format_version,
    })?;
    if !is_sha256_hex(&frame.checksum) {
        return Err(JsonCodecError::validation(
            "walFrameCodec: checksum must be a lowercase sha256 hex string",
        ));
    }
    Ok(())
}

pub fn verify_wal_frame_checksum<T: Serialize>(frame: &WalFrame<T>) -> StorageResult<bool> {
    assert_wal_frame(frame).map_err(storage_json_error)?;
    let body = WalFrameBody {
        t: frame.t.clone(),
        lifecycle: frame.lifecycle.clone(),
        path: frame.path.clone(),
        change: &frame.change,
        frame_seq: frame.frame_seq,
        frame_t_ns: frame.frame_t_ns.clone(),
        format_version: frame.format_version,
    };
    Ok(wal_frame_checksum(&body)? == frame.checksum)
}

#[derive(Clone, Debug, Default)]
pub struct WalFrameCodec<T> {
    marker: std::marker::PhantomData<T>,
}

pub fn wal_frame_codec<T>() -> WalFrameCodec<T> {
    WalFrameCodec {
        marker: std::marker::PhantomData,
    }
}

impl<T> Codec<WalFrame<T>> for WalFrameCodec<T>
where
    T: Serialize + DeserializeOwned,
{
    fn encode(&self, value: &WalFrame<T>) -> crate::json::JsonCodecResult<Vec<u8>> {
        assert_wal_frame(value)?;
        let value =
            serde_json::to_value(value).map_err(|err| JsonCodecError::encode(err.to_string()))?;
        assert_wal_frame_value(&value)?;
        strict_canonical_json_bytes(&value)
    }

    fn decode(&self, bytes: &[u8]) -> crate::json::JsonCodecResult<WalFrame<T>> {
        let value = strict_json_decode(bytes)?;
        assert_wal_frame_value(&value)?;
        serde_json::from_value(value).map_err(|err| JsonCodecError::decode(err.to_string()))
    }
}

fn assert_wal_frame_body<T: Serialize>(body: &WalFrameBody<T>) -> crate::json::JsonCodecResult<()> {
    if body.t != "c" {
        return Err(JsonCodecError::validation("walFrameCodec: t must be c"));
    }
    if body.path.is_empty() {
        return Err(JsonCodecError::validation(
            "walFrameCodec: path must be a non-empty string",
        ));
    }
    crate::json::assert_non_negative_decimal_integer_string(
        body.frame_t_ns.clone(),
        "walFrameCodec: frame_t_ns",
    )?;
    if body.format_version != WAL_FORMAT_VERSION {
        return Err(JsonCodecError::validation(format!(
            "walFrameCodec: format_version must be {WAL_FORMAT_VERSION}"
        )));
    }
    let value =
        serde_json::to_value(body).map_err(|err| JsonCodecError::encode(err.to_string()))?;
    assert_wal_frame_body_value(&value, "walFrameCodec")
}

fn assert_wal_frame_value(value: &Value) -> crate::json::JsonCodecResult<()> {
    assert_wal_frame_body_value(value, "walFrameCodec")?;
    let Some(record) = value.as_object() else {
        return Err(JsonCodecError::validation(
            "walFrameCodec: frame must be an object",
        ));
    };
    if !record.contains_key("checksum") {
        return Err(JsonCodecError::validation(
            "walFrameCodec: checksum is required",
        ));
    }
    let Some(checksum) = record.get("checksum").and_then(Value::as_str) else {
        return Err(JsonCodecError::validation(
            "walFrameCodec: checksum must be a lowercase sha256 hex string",
        ));
    };
    if !is_sha256_hex(checksum) {
        return Err(JsonCodecError::validation(
            "walFrameCodec: checksum must be a lowercase sha256 hex string",
        ));
    }
    Ok(())
}

fn assert_wal_frame_body_value(value: &Value, label: &str) -> crate::json::JsonCodecResult<()> {
    let Some(record) = value.as_object() else {
        return Err(JsonCodecError::validation(format!(
            "{label}: frame must be an object"
        )));
    };
    for key in record.keys() {
        match key.as_str() {
            "t" | "lifecycle" | "path" | "change" | "frame_seq" | "frame_t_ns"
            | "format_version" | "checksum" => {}
            _ => {
                return Err(JsonCodecError::validation(format!(
                    "walFrameCodec: unknown field {key}"
                )))
            }
        }
    }
    match record.get("t").and_then(Value::as_str) {
        Some("c") => {}
        _ => return Err(JsonCodecError::validation("walFrameCodec: t must be c")),
    }
    match record.get("lifecycle").and_then(Value::as_str) {
        Some("spec" | "data" | "ownership") => {}
        _ => {
            return Err(JsonCodecError::validation(
                "walFrameCodec: lifecycle must be spec, data, or ownership",
            ))
        }
    }
    match record.get("path").and_then(Value::as_str) {
        Some(path) if !path.is_empty() => {}
        _ => {
            return Err(JsonCodecError::validation(
                "walFrameCodec: path must be a non-empty string",
            ))
        }
    }
    if !record.contains_key("change") {
        return Err(JsonCodecError::validation(
            "walFrameCodec: change payload is required",
        ));
    }
    if record.get("frame_seq").and_then(Value::as_u64).is_none() {
        return Err(JsonCodecError::validation(
            "walFrameCodec: frame_seq must be a non-negative integer",
        ));
    }
    let Some(frame_t_ns) = record.get("frame_t_ns").and_then(Value::as_str) else {
        return Err(JsonCodecError::validation(
            "walFrameCodec: frame_t_ns must be a canonical non-negative decimal integer string",
        ));
    };
    crate::json::assert_non_negative_decimal_integer_string(
        frame_t_ns,
        "walFrameCodec: frame_t_ns",
    )?;
    if record.get("format_version").and_then(Value::as_u64) != Some(WAL_FORMAT_VERSION) {
        return Err(JsonCodecError::validation(format!(
            "walFrameCodec: format_version must be {WAL_FORMAT_VERSION}"
        )));
    }
    Ok(())
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ChangeEnvelope<T> {
    pub lifecycle: ChangeLifecycle,
    pub structure: String,
    pub version: Value,
    pub t_ns: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
    pub change: T,
}

#[derive(Clone, Debug)]
pub struct ChangeEnvelopeOptions {
    pub lifecycle: ChangeLifecycle,
    pub structure: String,
    pub version: Value,
    pub t_ns: Option<String>,
    pub seq: Option<u64>,
}

impl ChangeEnvelopeOptions {
    pub fn new(structure: impl Into<String>) -> Self {
        Self {
            lifecycle: ChangeLifecycle::Data,
            structure: structure.into(),
            version: Value::from(1),
            t_ns: None,
            seq: None,
        }
    }

    pub fn with_lifecycle(mut self, lifecycle: ChangeLifecycle) -> Self {
        self.lifecycle = lifecycle;
        self
    }

    pub fn with_version(mut self, version: impl Into<Value>) -> Self {
        self.version = version.into();
        self
    }

    pub fn with_t_ns(mut self, t_ns: impl Into<String>) -> Self {
        self.t_ns = Some(t_ns.into());
        self
    }

    pub fn with_seq(mut self, seq: u64) -> Self {
        self.seq = Some(seq);
        self
    }
}

pub fn now_ns() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos().to_string())
        .unwrap_or_else(|_| "0".to_owned())
}

pub fn envelope_change<T>(
    change: T,
    opts: ChangeEnvelopeOptions,
) -> StorageResult<ChangeEnvelope<T>> {
    if opts.structure.is_empty() {
        return Err(StorageError::backend(
            "changeEnvelopeCodec: structure must be a non-empty string",
        ));
    }
    let t_ns = match opts.t_ns {
        Some(value) => crate::json::assert_non_negative_decimal_integer_string(
            value,
            "changeEnvelopeCodec: t_ns",
        )
        .map_err(storage_json_error)?,
        None => now_ns(),
    };
    let envelope = ChangeEnvelope {
        lifecycle: opts.lifecycle,
        structure: opts.structure,
        version: opts.version,
        t_ns,
        seq: opts.seq,
        change,
    };
    assert_change_envelope(&envelope).map_err(storage_json_error)?;
    Ok(envelope)
}

#[derive(Clone, Debug, Default)]
pub struct ChangeEnvelopeCodec<T> {
    marker: std::marker::PhantomData<T>,
}

pub fn change_envelope_codec<T>() -> ChangeEnvelopeCodec<T> {
    ChangeEnvelopeCodec {
        marker: std::marker::PhantomData,
    }
}

impl<T> Codec<ChangeEnvelope<T>> for ChangeEnvelopeCodec<T>
where
    T: Serialize + DeserializeOwned,
{
    fn encode(&self, value: &ChangeEnvelope<T>) -> crate::json::JsonCodecResult<Vec<u8>> {
        assert_change_envelope(value)?;
        let value =
            serde_json::to_value(value).map_err(|err| JsonCodecError::encode(err.to_string()))?;
        strict_canonical_json_bytes(&value)
    }

    fn decode(&self, bytes: &[u8]) -> crate::json::JsonCodecResult<ChangeEnvelope<T>> {
        let value = strict_json_decode(bytes)?;
        assert_change_envelope_value(&value, "changeEnvelopeCodec")?;
        serde_json::from_value(value).map_err(|err| JsonCodecError::decode(err.to_string()))
    }
}

pub fn assert_change_envelope<T>(value: &ChangeEnvelope<T>) -> crate::json::JsonCodecResult<()> {
    if value.structure.is_empty() {
        return Err(JsonCodecError::validation(
            "changeEnvelopeCodec: structure must be a non-empty string",
        ));
    }
    crate::json::assert_non_negative_decimal_integer_string(
        value.t_ns.clone(),
        "changeEnvelopeCodec: t_ns",
    )?;
    match &value.version {
        Value::Number(_) | Value::String(_) => Ok(()),
        _ => Err(JsonCodecError::validation(
            "changeEnvelopeCodec: version must be a finite number or string",
        )),
    }
}

fn assert_change_envelope_value(value: &Value, label: &str) -> crate::json::JsonCodecResult<()> {
    let Some(record) = value.as_object() else {
        return Err(JsonCodecError::validation(format!(
            "{label}: frame must be an object"
        )));
    };
    match record.get("lifecycle").and_then(Value::as_str) {
        Some("spec" | "data" | "ownership") => {}
        _ => {
            return Err(JsonCodecError::validation(
                "changeEnvelopeCodec: lifecycle must be spec, data, or ownership",
            ))
        }
    }
    match record.get("structure").and_then(Value::as_str) {
        Some(structure) if !structure.is_empty() => {}
        _ => {
            return Err(JsonCodecError::validation(
                "changeEnvelopeCodec: structure must be a non-empty string",
            ))
        }
    }
    match record.get("version") {
        Some(Value::Number(_) | Value::String(_)) => {}
        _ => {
            return Err(JsonCodecError::validation(
                "changeEnvelopeCodec: version must be a finite number or string",
            ))
        }
    }
    let Some(t_ns) = record.get("t_ns").and_then(Value::as_str) else {
        return Err(JsonCodecError::validation(
            "changeEnvelopeCodec: t_ns must be a canonical non-negative decimal integer string",
        ));
    };
    crate::json::assert_non_negative_decimal_integer_string(t_ns, "changeEnvelopeCodec: t_ns")?;
    if record.get("seq").is_some_and(|seq| seq.as_u64().is_none()) {
        return Err(JsonCodecError::validation(
            "changeEnvelopeCodec: seq must be a non-negative integer when present",
        ));
    }
    if !record.contains_key("change") {
        return Err(JsonCodecError::validation(
            "changeEnvelopeCodec: change payload is required",
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ObserveEventFrame<T> {
    pub lifecycle: ChangeLifecycle,
    pub structure: String,
    pub version: Value,
    pub t_ns: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
    pub change: T,
    #[serde(rename = "observeSeq")]
    pub observe_seq: u64,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ObserveEventFrameOptions {
    pub stream: Option<String>,
}

impl ObserveEventFrameOptions {
    pub fn with_stream(mut self, stream: impl Into<String>) -> Self {
        self.stream = Some(stream.into());
        self
    }
}

pub fn observe_event_frame<T>(
    observe_seq: u64,
    path: impl Into<String>,
    change: T,
    opts: ObserveEventFrameOptions,
) -> StorageResult<ObserveEventFrame<T>> {
    let envelope = envelope_change(
        change,
        ChangeEnvelopeOptions::new("observe-event").with_seq(observe_seq),
    )?;
    Ok(ObserveEventFrame {
        lifecycle: envelope.lifecycle,
        structure: envelope.structure,
        version: envelope.version,
        t_ns: envelope.t_ns,
        seq: envelope.seq,
        change: envelope.change,
        observe_seq,
        path: path.into(),
        stream: opts.stream,
    })
}

pub fn assert_observe_event_frame<T>(
    value: &ObserveEventFrame<T>,
) -> crate::json::JsonCodecResult<()> {
    assert_change_envelope(&ChangeEnvelope {
        lifecycle: value.lifecycle.clone(),
        structure: value.structure.clone(),
        version: value.version.clone(),
        t_ns: value.t_ns.clone(),
        seq: value.seq,
        change: (),
    })?;
    if value.structure != "observe-event" {
        return Err(JsonCodecError::validation(
            "observeEventFrameCodec: structure must be observe-event",
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, Default)]
pub struct ObserveEventFrameCodec<T> {
    marker: std::marker::PhantomData<T>,
}

pub fn observe_event_frame_codec<T>() -> ObserveEventFrameCodec<T> {
    ObserveEventFrameCodec {
        marker: std::marker::PhantomData,
    }
}

impl<T> Codec<ObserveEventFrame<T>> for ObserveEventFrameCodec<T>
where
    T: Serialize + DeserializeOwned,
{
    fn encode(&self, value: &ObserveEventFrame<T>) -> crate::json::JsonCodecResult<Vec<u8>> {
        assert_observe_event_frame(value)?;
        let value =
            serde_json::to_value(value).map_err(|err| JsonCodecError::encode(err.to_string()))?;
        strict_canonical_json_bytes(&value)
    }

    fn decode(&self, bytes: &[u8]) -> crate::json::JsonCodecResult<ObserveEventFrame<T>> {
        let value = strict_json_decode(bytes)?;
        assert_observe_event_frame_value(&value)?;
        serde_json::from_value(value).map_err(|err| JsonCodecError::decode(err.to_string()))
    }
}

fn assert_observe_event_frame_value(value: &Value) -> crate::json::JsonCodecResult<()> {
    assert_change_envelope_value(value, "observeEventFrameCodec")?;
    let Some(record) = value.as_object() else {
        return Err(JsonCodecError::validation(
            "observeEventFrameCodec: frame must be an object",
        ));
    };
    if record.get("structure").and_then(Value::as_str) != Some("observe-event") {
        return Err(JsonCodecError::validation(
            "observeEventFrameCodec: structure must be observe-event",
        ));
    }
    if record.get("observeSeq").and_then(Value::as_u64).is_none() {
        return Err(JsonCodecError::validation(
            "observeEventFrameCodec: observeSeq must be a non-negative integer",
        ));
    }
    if record.get("path").and_then(Value::as_str).is_none() {
        return Err(JsonCodecError::validation(
            "observeEventFrameCodec: path must be a string",
        ));
    }
    if record
        .get("stream")
        .is_some_and(|stream| !stream.is_string())
    {
        return Err(JsonCodecError::validation(
            "observeEventFrameCodec: stream must be a string when present",
        ));
    }
    Ok(())
}

pub type ObserveEventLogPage<T> = AppendLogPage<ObserveEventFrame<T>>;

pub fn read_observe_event_log_page<T: Clone>(
    log: &dyn AppendLogStorageTier<ObserveEventFrame<T>>,
    opts: AppendLogReadOptions,
) -> StorageResult<ObserveEventLogPage<T>> {
    read_append_log_page(log, opts)
}

pub const APPEND_LOG_SEQ_PAD: usize = 20;

#[derive(Clone, Debug, PartialEq)]
pub struct AppendLogEntry<T> {
    pub key: String,
    pub seq: u64,
    pub value: T,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AppendLogReadOptions {
    pub after: Option<u64>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AppendLogPage<T> {
    pub entries: Vec<AppendLogEntry<T>>,
    pub next_after: Option<u64>,
    pub done: bool,
}

pub trait AppendLogStorageTier<T: Clone> {
    fn append(&self, value: T) -> StorageResult<AppendLogEntry<T>>;
    fn read(&self, opts: AppendLogReadOptions) -> StorageResult<Vec<AppendLogEntry<T>>>;
    fn truncate_after(&self, seq: u64) -> StorageResult<()>;
    fn size(&self) -> StorageResult<usize>;
}

#[derive(Clone)]
pub struct AppendLogStorage<T: Clone> {
    kv: Rc<dyn KvStorageTier<T>>,
    prefix: String,
}

impl<T: Clone> fmt::Debug for AppendLogStorage<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppendLogStorage")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct MultiWriterAppendLogStorage<T: Clone> {
    kv: Rc<dyn KvStorageTier<T>>,
    prefix: String,
    max_attempts: usize,
}

impl<T: Clone> fmt::Debug for MultiWriterAppendLogStorage<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiWriterAppendLogStorage")
            .field("prefix", &self.prefix)
            .field("max_attempts", &self.max_attempts)
            .finish_non_exhaustive()
    }
}

pub fn append_log_key(prefix: &str, seq: u64) -> String {
    format!("{prefix}/{seq:0APPEND_LOG_SEQ_PAD$}")
}

pub fn append_log_storage<T: Clone>(
    kv: Rc<dyn KvStorageTier<T>>,
    prefix: impl Into<String>,
) -> AppendLogStorage<T> {
    AppendLogStorage {
        kv,
        prefix: prefix.into(),
    }
}

pub fn memory_append_log<T: Clone + 'static>(prefix: impl Into<String>) -> AppendLogStorage<T> {
    append_log_storage(Rc::new(memory_kv()), prefix)
}

pub fn multi_writer_append_log_storage<T: Clone>(
    kv: Rc<dyn KvStorageTier<T>>,
    prefix: impl Into<String>,
    max_attempts: usize,
) -> StorageResult<MultiWriterAppendLogStorage<T>> {
    if max_attempts == 0 {
        return Err(StorageError::backend(
            "multi_writer_append_log_storage: max_attempts must be positive",
        ));
    }
    Ok(MultiWriterAppendLogStorage {
        kv,
        prefix: prefix.into(),
        max_attempts,
    })
}

pub fn memory_multi_writer_append_log<T: Clone + 'static>(
    prefix: impl Into<String>,
) -> MultiWriterAppendLogStorage<T> {
    multi_writer_append_log_storage(Rc::new(memory_kv()), prefix, 1024)
        .expect("memory_kv supports put-if-absent")
}

pub fn read_append_log_page<T: Clone>(
    log: &dyn AppendLogStorageTier<T>,
    opts: AppendLogReadOptions,
) -> StorageResult<AppendLogPage<T>> {
    let limit = opts.limit.unwrap_or(100);
    if limit == 0 {
        return Err(StorageError::backend(
            "read_append_log_page: limit must be positive",
        ));
    }
    if limit == usize::MAX {
        return Err(StorageError::backend(
            "read_append_log_page: limit must leave room for one lookahead entry",
        ));
    }
    let mut lookahead_opts = opts.clone();
    lookahead_opts.limit = Some(limit + 1);
    let mut entries = log.read(lookahead_opts)?;
    let done = entries.len() <= limit;
    if entries.len() > limit {
        entries.truncate(limit);
    }
    let next_after = entries.last().map(|entry| entry.seq).or(opts.after);
    Ok(AppendLogPage {
        entries,
        next_after,
        done,
    })
}

pub const REACTIVE_COLLECTION_SNAPSHOT_FORMAT: &str = "graphrefly.reactive-collection.snapshot.v1";
pub const REACTIVE_COLLECTION_CHANGE_FORMAT: &str = "graphrefly.reactive-collection.change.v1";
pub const REACTIVE_COLLECTION_FRAME_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ReactiveCollectionKind {
    #[serde(rename = "reactiveList")]
    ReactiveList,
    #[serde(rename = "reactiveLog")]
    ReactiveLog,
    #[serde(rename = "reactiveMap")]
    ReactiveMap,
    #[serde(rename = "reactiveIndex")]
    ReactiveIndex,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReactiveCollectionSnapshotFrame {
    pub format: String,
    pub version: u8,
    pub kind: ReactiveCollectionKind,
    #[serde(rename = "changeCursor")]
    pub change_cursor: i64,
    pub snapshot: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReactiveCollectionChangeFrame {
    pub format: String,
    pub version: u8,
    pub kind: ReactiveCollectionKind,
    pub change: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReactiveCollectionRestoreState<T> {
    pub kind: ReactiveCollectionKind,
    pub state: T,
    pub source: ReactiveCollectionRestoreSource,
    pub snapshot: ReactiveCollectionSnapshotRestoreMeta,
    pub changes: ReactiveCollectionChangesRestoreMeta,
    pub cursor: Option<u64>,
    pub snapshot_found: bool,
    pub changes_applied: usize,
}

pub type ReactiveListRestoreState<T> = ReactiveCollectionRestoreState<Vec<T>>;
pub type ReactiveLogRestoreState<T> = ReactiveCollectionRestoreState<Vec<T>>;
pub type ReactiveMapRestoreState<K, V> = ReactiveCollectionRestoreState<Vec<(K, V)>>;
pub type ReactiveIndexRestoreState<K, S, V> =
    ReactiveCollectionRestoreState<Vec<IndexRow<K, S, V>>>;

struct FoldedCollectionState<T> {
    state: T,
    cursor: Option<u64>,
    snapshot_cursor: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReactiveCollectionRestoreSource {
    Empty,
    Changes,
    Snapshot,
    SnapshotAndChanges,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReactiveCollectionSnapshotRestoreMeta {
    pub found: bool,
    pub change_cursor: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReactiveCollectionChangesRestoreMeta {
    pub applied: usize,
    pub cursor: i64,
}

#[derive(Clone, Default)]
pub struct LoadReactiveCollectionStateOptions<'a> {
    pub storage_prefix: Option<&'a str>,
    pub snapshot_key: Option<&'a str>,
    pub change_log: Option<&'a dyn AppendLogStorageTier<ReactiveCollectionChangeFrame>>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ReactiveCollectionSnapshotFrameCodec;

#[derive(Clone, Copy, Debug, Default)]
pub struct ReactiveCollectionChangeFrameCodec;

pub fn reactive_collection_snapshot_key(prefix: &str) -> StorageResult<String> {
    if prefix.is_empty() {
        return Err(StorageError::backend(
            "reactive_collection_snapshot_key: storage_prefix must be non-empty",
        ));
    }
    Ok(format!("{prefix}/snapshot"))
}

pub fn reactive_collection_snapshot_frame(
    kind: ReactiveCollectionKind,
    change_cursor: i64,
    snapshot: Value,
) -> StorageResult<ReactiveCollectionSnapshotFrame> {
    let frame = ReactiveCollectionSnapshotFrame {
        format: REACTIVE_COLLECTION_SNAPSHOT_FORMAT.to_owned(),
        version: REACTIVE_COLLECTION_FRAME_VERSION,
        kind,
        change_cursor,
        snapshot,
    };
    assert_reactive_collection_snapshot_frame(&frame)?;
    Ok(frame)
}

pub fn reactive_collection_change_frame(
    kind: ReactiveCollectionKind,
    change: Value,
) -> StorageResult<ReactiveCollectionChangeFrame> {
    let frame = ReactiveCollectionChangeFrame {
        format: REACTIVE_COLLECTION_CHANGE_FORMAT.to_owned(),
        version: REACTIVE_COLLECTION_FRAME_VERSION,
        kind,
        change,
    };
    assert_reactive_collection_change_frame(&frame)?;
    Ok(frame)
}

pub fn reactive_collection_snapshot_frame_codec() -> ReactiveCollectionSnapshotFrameCodec {
    ReactiveCollectionSnapshotFrameCodec
}

pub fn reactive_collection_change_frame_codec() -> ReactiveCollectionChangeFrameCodec {
    ReactiveCollectionChangeFrameCodec
}

pub fn assert_reactive_collection_snapshot_frame(
    frame: &ReactiveCollectionSnapshotFrame,
) -> StorageResult<()> {
    if frame.format != REACTIVE_COLLECTION_SNAPSHOT_FORMAT {
        return Err(StorageError::backend(format!(
            "reactiveCollection snapshot frame: unsupported format {}",
            frame.format
        )));
    }
    if frame.version != REACTIVE_COLLECTION_FRAME_VERSION {
        return Err(StorageError::backend(format!(
            "reactiveCollection snapshot frame: unsupported version {}",
            frame.version
        )));
    }
    if frame.change_cursor < -1 {
        return Err(StorageError::backend(
            "reactiveCollection snapshot frame: changeCursor must be -1 or a non-negative integer",
        ));
    }
    let value = serde_json::to_value(frame).map_err(storage_serde_json_error)?;
    strict_canonical_json_bytes(&value).map_err(storage_json_error)?;
    Ok(())
}

pub fn assert_reactive_collection_change_frame(
    frame: &ReactiveCollectionChangeFrame,
) -> StorageResult<()> {
    if frame.format != REACTIVE_COLLECTION_CHANGE_FORMAT {
        return Err(StorageError::backend(format!(
            "reactiveCollection change frame: unsupported format {}",
            frame.format
        )));
    }
    if frame.version != REACTIVE_COLLECTION_FRAME_VERSION {
        return Err(StorageError::backend(format!(
            "reactiveCollection change frame: unsupported version {}",
            frame.version
        )));
    }
    let value = serde_json::to_value(frame).map_err(storage_serde_json_error)?;
    strict_canonical_json_bytes(&value).map_err(storage_json_error)?;
    Ok(())
}

impl Codec<ReactiveCollectionSnapshotFrame> for ReactiveCollectionSnapshotFrameCodec {
    fn encode(
        &self,
        value: &ReactiveCollectionSnapshotFrame,
    ) -> crate::json::JsonCodecResult<Vec<u8>> {
        assert_reactive_collection_snapshot_frame(value).map_err(storage_error_to_json)?;
        let json =
            serde_json::to_value(value).map_err(|err| JsonCodecError::encode(err.to_string()))?;
        strict_canonical_json_bytes(&json)
    }

    fn decode(
        &self,
        bytes: &[u8],
    ) -> crate::json::JsonCodecResult<ReactiveCollectionSnapshotFrame> {
        let json = strict_json_decode(bytes)?;
        let frame =
            serde_json::from_value(json).map_err(|err| JsonCodecError::decode(err.to_string()))?;
        assert_reactive_collection_snapshot_frame(&frame).map_err(storage_error_to_json)?;
        Ok(frame)
    }
}

impl Codec<ReactiveCollectionChangeFrame> for ReactiveCollectionChangeFrameCodec {
    fn encode(
        &self,
        value: &ReactiveCollectionChangeFrame,
    ) -> crate::json::JsonCodecResult<Vec<u8>> {
        assert_reactive_collection_change_frame(value).map_err(storage_error_to_json)?;
        let json =
            serde_json::to_value(value).map_err(|err| JsonCodecError::encode(err.to_string()))?;
        strict_canonical_json_bytes(&json)
    }

    fn decode(&self, bytes: &[u8]) -> crate::json::JsonCodecResult<ReactiveCollectionChangeFrame> {
        let json = strict_json_decode(bytes)?;
        let frame =
            serde_json::from_value(json).map_err(|err| JsonCodecError::decode(err.to_string()))?;
        assert_reactive_collection_change_frame(&frame).map_err(storage_error_to_json)?;
        Ok(frame)
    }
}

pub fn load_reactive_list_state<T>(
    snapshot_store: &dyn KvStorageTier<ReactiveCollectionSnapshotFrame>,
    options: LoadReactiveCollectionStateOptions<'_>,
) -> StorageResult<ReactiveListRestoreState<T>>
where
    T: Clone + Serialize + DeserializeOwned,
{
    let (snapshot, cursor, snapshot_found) = load_collection_snapshot::<Vec<T>>(
        snapshot_store,
        &options,
        ReactiveCollectionKind::ReactiveList,
    )?;
    let mut state = snapshot.unwrap_or_default();
    let (cursor, changes_applied) = fold_collection_changes(
        state,
        cursor,
        options.change_log,
        ReactiveCollectionKind::ReactiveList,
        fold_list_change::<T>,
    )?;
    state = cursor.state;
    restore_state(
        ReactiveCollectionKind::ReactiveList,
        state,
        snapshot_found,
        cursor.snapshot_cursor,
        changes_applied,
        cursor.cursor,
    )
}

pub fn load_reactive_log_state<T>(
    snapshot_store: &dyn KvStorageTier<ReactiveCollectionSnapshotFrame>,
    options: LoadReactiveCollectionStateOptions<'_>,
) -> StorageResult<ReactiveLogRestoreState<T>>
where
    T: Clone + Serialize + DeserializeOwned,
{
    let (snapshot, cursor, snapshot_found) = load_collection_snapshot::<Vec<T>>(
        snapshot_store,
        &options,
        ReactiveCollectionKind::ReactiveLog,
    )?;
    let mut state = snapshot.unwrap_or_default();
    let (cursor, changes_applied) = fold_collection_changes(
        state,
        cursor,
        options.change_log,
        ReactiveCollectionKind::ReactiveLog,
        fold_log_change::<T>,
    )?;
    state = cursor.state;
    restore_state(
        ReactiveCollectionKind::ReactiveLog,
        state,
        snapshot_found,
        cursor.snapshot_cursor,
        changes_applied,
        cursor.cursor,
    )
}

pub fn load_reactive_map_state<K, V>(
    snapshot_store: &dyn KvStorageTier<ReactiveCollectionSnapshotFrame>,
    options: LoadReactiveCollectionStateOptions<'_>,
) -> StorageResult<ReactiveMapRestoreState<K, V>>
where
    K: Clone + Serialize + DeserializeOwned,
    V: Clone + Serialize + DeserializeOwned,
{
    let (snapshot, cursor, snapshot_found) = load_collection_snapshot::<Vec<(K, V)>>(
        snapshot_store,
        &options,
        ReactiveCollectionKind::ReactiveMap,
    )?;
    let mut state = snapshot.unwrap_or_default();
    assert_unique_map_keys(&state, "reactiveMap snapshot")?;
    let (cursor, changes_applied) = fold_collection_changes(
        state,
        cursor,
        options.change_log,
        ReactiveCollectionKind::ReactiveMap,
        fold_map_change::<K, V>,
    )?;
    state = cursor.state;
    assert_unique_map_keys(&state, "reactiveMap restore")?;
    restore_state(
        ReactiveCollectionKind::ReactiveMap,
        state,
        snapshot_found,
        cursor.snapshot_cursor,
        changes_applied,
        cursor.cursor,
    )
}

pub fn load_reactive_index_state<K, S, V>(
    snapshot_store: &dyn KvStorageTier<ReactiveCollectionSnapshotFrame>,
    options: LoadReactiveCollectionStateOptions<'_>,
) -> StorageResult<ReactiveIndexRestoreState<K, S, V>>
where
    K: Clone + Serialize + DeserializeOwned,
    S: Clone + Serialize + DeserializeOwned,
    V: Clone + Serialize + DeserializeOwned,
{
    let (snapshot, cursor, snapshot_found) = load_collection_snapshot::<Vec<IndexRow<K, S, V>>>(
        snapshot_store,
        &options,
        ReactiveCollectionKind::ReactiveIndex,
    )?;
    let mut state = snapshot.unwrap_or_default();
    assert_unique_index_primaries(&state, "reactiveIndex snapshot")?;
    let (cursor, changes_applied) = fold_collection_changes(
        state,
        cursor,
        options.change_log,
        ReactiveCollectionKind::ReactiveIndex,
        fold_index_change::<K, S, V>,
    )?;
    state = cursor.state;
    assert_unique_index_primaries(&state, "reactiveIndex restore")?;
    restore_state(
        ReactiveCollectionKind::ReactiveIndex,
        state,
        snapshot_found,
        cursor.snapshot_cursor,
        changes_applied,
        cursor.cursor,
    )
}

fn load_collection_snapshot<T>(
    snapshot_store: &dyn KvStorageTier<ReactiveCollectionSnapshotFrame>,
    options: &LoadReactiveCollectionStateOptions<'_>,
    kind: ReactiveCollectionKind,
) -> StorageResult<(Option<T>, Option<u64>, bool)>
where
    T: DeserializeOwned,
{
    let key = resolve_collection_snapshot_key(options)?;
    let Some(frame) = snapshot_store.get(&key)? else {
        return Ok((None, None, false));
    };
    assert_reactive_collection_snapshot_frame(&frame)?;
    if frame.kind != kind {
        return Err(StorageError::backend(format!(
            "reactiveCollection snapshot frame: expected {:?}, got {:?}",
            kind, frame.kind
        )));
    }
    let state = serde_json::from_value(frame.snapshot.clone()).map_err(storage_serde_json_error)?;
    let cursor = if frame.change_cursor < 0 {
        None
    } else {
        Some(frame.change_cursor as u64)
    };
    Ok((Some(state), cursor, true))
}

fn fold_collection_changes<T, F>(
    mut state: T,
    mut cursor: Option<u64>,
    change_log: Option<&dyn AppendLogStorageTier<ReactiveCollectionChangeFrame>>,
    kind: ReactiveCollectionKind,
    mut fold: F,
) -> StorageResult<(FoldedCollectionState<T>, usize)>
where
    F: FnMut(&mut T, ReactiveCollectionChangeFrame) -> StorageResult<()>,
{
    let snapshot_cursor = cursor
        .map(seq_to_snapshot_cursor)
        .transpose()?
        .unwrap_or(-1);
    let Some(log) = change_log else {
        return Ok((
            FoldedCollectionState {
                state,
                cursor,
                snapshot_cursor,
            },
            0,
        ));
    };
    let entries = log.read(AppendLogReadOptions {
        after: cursor,
        limit: None,
    })?;
    let mut expected = cursor.map_or(0, |seq| seq.saturating_add(1));
    for entry in entries.iter() {
        if entry.seq != expected {
            return Err(StorageError::backend(format!(
                "reactiveCollection load: non-contiguous change log sequence, expected {expected}, got {}",
                entry.seq
            )));
        }
        assert_reactive_collection_change_frame(&entry.value)?;
        if entry.value.kind != kind {
            return Err(StorageError::backend(format!(
                "reactiveCollection change frame: expected {:?}, got {:?}",
                kind, entry.value.kind
            )));
        }
        fold(&mut state, entry.value.clone())?;
        cursor = Some(entry.seq);
        expected = expected.checked_add(1).ok_or_else(|| {
            StorageError::backend("reactiveCollection load: change log sequence overflow")
        })?;
    }
    Ok((
        FoldedCollectionState {
            state,
            cursor,
            snapshot_cursor,
        },
        entries.len(),
    ))
}

fn restore_state<T>(
    kind: ReactiveCollectionKind,
    state: T,
    snapshot_found: bool,
    snapshot_cursor: i64,
    changes_applied: usize,
    cursor: Option<u64>,
) -> StorageResult<ReactiveCollectionRestoreState<T>> {
    let change_cursor = cursor
        .map(seq_to_snapshot_cursor)
        .transpose()?
        .unwrap_or(-1);
    let source = match (snapshot_found, changes_applied > 0) {
        (false, false) => ReactiveCollectionRestoreSource::Empty,
        (false, true) => ReactiveCollectionRestoreSource::Changes,
        (true, false) => ReactiveCollectionRestoreSource::Snapshot,
        (true, true) => ReactiveCollectionRestoreSource::SnapshotAndChanges,
    };
    Ok(ReactiveCollectionRestoreState {
        kind,
        state,
        source,
        snapshot: ReactiveCollectionSnapshotRestoreMeta {
            found: snapshot_found,
            change_cursor: snapshot_cursor,
        },
        changes: ReactiveCollectionChangesRestoreMeta {
            applied: changes_applied,
            cursor: change_cursor,
        },
        cursor,
        snapshot_found,
        changes_applied,
    })
}

fn fold_list_change<T>(
    state: &mut Vec<T>,
    frame: ReactiveCollectionChangeFrame,
) -> StorageResult<()>
where
    T: Clone + Serialize + DeserializeOwned,
{
    let change: ListChange<T> =
        serde_json::from_value(frame.change).map_err(storage_serde_json_error)?;
    match change {
        ListChange::Append { value } => state.push(value),
        ListChange::AppendMany { values } => state.extend(values),
        ListChange::Insert { index, value } => {
            if index > state.len() {
                return Err(StorageError::backend(format!(
                    "reactiveList fold: insert index {index} is out of bounds for len {}",
                    state.len()
                )));
            }
            state.insert(index, value);
        }
        ListChange::InsertMany { index, values } => {
            if index > state.len() {
                return Err(StorageError::backend(format!(
                    "reactiveList fold: insertMany index {index} is out of bounds for len {}",
                    state.len()
                )));
            }
            state.splice(index..index, values);
        }
        ListChange::Pop { index, value } => {
            if index >= state.len() {
                return Err(StorageError::backend(format!(
                    "reactiveList fold: pop index {index} is out of bounds for len {}",
                    state.len()
                )));
            }
            let actual = state.remove(index);
            if !strict_json_equal(&actual, &value)? {
                return Err(StorageError::backend(
                    "reactiveList fold: pop value does not match stored state",
                ));
            }
        }
        ListChange::TrimHead { n } => {
            if n > state.len() {
                return Err(StorageError::backend(format!(
                    "reactiveList fold: trimHead {n} exceeds len {}",
                    state.len()
                )));
            }
            state.drain(0..n);
        }
        ListChange::Clear { count } => {
            if count != state.len() {
                return Err(StorageError::backend(format!(
                    "reactiveList fold: clear count {count} does not match len {}",
                    state.len()
                )));
            }
            state.clear();
        }
    }
    Ok(())
}

fn fold_log_change<T>(state: &mut Vec<T>, frame: ReactiveCollectionChangeFrame) -> StorageResult<()>
where
    T: Clone + Serialize + DeserializeOwned,
{
    let change: LogChange<T> =
        serde_json::from_value(frame.change).map_err(storage_serde_json_error)?;
    match change {
        LogChange::Append { value } => state.push(value),
        LogChange::AppendMany { values } => state.extend(values),
        LogChange::TrimHead { n } => {
            if n > state.len() {
                return Err(StorageError::backend(format!(
                    "reactiveLog fold: trimHead {n} exceeds len {}",
                    state.len()
                )));
            }
            state.drain(0..n);
        }
        LogChange::Clear { count } => {
            if count != state.len() {
                return Err(StorageError::backend(format!(
                    "reactiveLog fold: clear count {count} does not match len {}",
                    state.len()
                )));
            }
            state.clear();
        }
    }
    Ok(())
}

fn fold_map_change<K, V>(
    state: &mut Vec<(K, V)>,
    frame: ReactiveCollectionChangeFrame,
) -> StorageResult<()>
where
    K: Clone + Serialize + DeserializeOwned,
    V: Clone + Serialize + DeserializeOwned,
{
    let change: MapChange<K, V> =
        serde_json::from_value(frame.change).map_err(storage_serde_json_error)?;
    match change {
        MapChange::Set { key, value } => match find_map_key(state, &key)? {
            Some(index) => state[index] = (key, value),
            None => state.push((key, value)),
        },
        MapChange::Delete { key, previous } => {
            let Some(index) = find_map_key(state, &key)? else {
                return Err(StorageError::backend(
                    "reactiveMap fold: delete key is missing",
                ));
            };
            let (_, actual) = state.remove(index);
            if !strict_json_equal(&actual, &previous)? {
                return Err(StorageError::backend(
                    "reactiveMap fold: delete previous value does not match stored state",
                ));
            }
        }
        MapChange::Clear { count } => {
            if count != state.len() {
                return Err(StorageError::backend(format!(
                    "reactiveMap fold: clear count {count} does not match len {}",
                    state.len()
                )));
            }
            state.clear();
        }
    }
    Ok(())
}

fn fold_index_change<K, S, V>(
    state: &mut Vec<IndexRow<K, S, V>>,
    frame: ReactiveCollectionChangeFrame,
) -> StorageResult<()>
where
    K: Clone + Serialize + DeserializeOwned,
    S: Clone + Serialize + DeserializeOwned,
    V: Clone + Serialize + DeserializeOwned,
{
    let change: IndexChange<K, S, V> =
        serde_json::from_value(frame.change).map_err(storage_serde_json_error)?;
    match change {
        IndexChange::Upsert {
            primary,
            secondary,
            value,
        } => {
            let row = IndexRow {
                primary,
                secondary,
                value,
            };
            match find_index_primary(state, &row.primary)? {
                Some(index) => state[index] = row,
                None => state.push(row),
            }
        }
        IndexChange::Delete { primary } => {
            let Some(index) = find_index_primary(state, &primary)? else {
                return Err(StorageError::backend(
                    "reactiveIndex fold: delete primary is missing",
                ));
            };
            state.remove(index);
        }
        IndexChange::DeleteMany { primaries } => {
            remove_index_primaries(state, &primaries, "reactiveIndex fold: deleteMany primary")?;
        }
        IndexChange::Clear { count } => {
            if count != state.len() {
                return Err(StorageError::backend(format!(
                    "reactiveIndex fold: clear count {count} does not match len {}",
                    state.len()
                )));
            }
            state.clear();
        }
    }
    Ok(())
}

fn assert_unique_map_keys<K, V>(entries: &[(K, V)], label: &str) -> StorageResult<()>
where
    K: Serialize,
{
    let mut seen = Vec::<Vec<u8>>::new();
    for (index, (key, _)) in entries.iter().enumerate() {
        let id = strict_json_identity(key)?;
        if seen.iter().any(|existing| existing == &id) {
            return Err(StorageError::backend(format!(
                "{label}: entry {index} duplicates an earlier key"
            )));
        }
        seen.push(id);
    }
    Ok(())
}

fn assert_unique_index_primaries<K, S, V>(
    rows: &[IndexRow<K, S, V>],
    label: &str,
) -> StorageResult<()>
where
    K: Serialize,
{
    let mut seen = Vec::<Vec<u8>>::new();
    for (index, row) in rows.iter().enumerate() {
        let id = strict_json_identity(&row.primary)?;
        if seen.iter().any(|existing| existing == &id) {
            return Err(StorageError::backend(format!(
                "{label}: row {index} duplicates an earlier primary"
            )));
        }
        seen.push(id);
    }
    Ok(())
}

fn find_map_key<K, V>(entries: &[(K, V)], key: &K) -> StorageResult<Option<usize>>
where
    K: Serialize,
{
    let target = strict_json_identity(key)?;
    for (index, (candidate, _)) in entries.iter().enumerate() {
        if strict_json_identity(candidate)? == target {
            return Ok(Some(index));
        }
    }
    Ok(None)
}

fn find_index_primary<K, S, V>(
    rows: &[IndexRow<K, S, V>],
    primary: &K,
) -> StorageResult<Option<usize>>
where
    K: Serialize,
{
    let target = strict_json_identity(primary)?;
    for (index, row) in rows.iter().enumerate() {
        if strict_json_identity(&row.primary)? == target {
            return Ok(Some(index));
        }
    }
    Ok(None)
}

fn remove_index_primaries<K, S, V>(
    rows: &mut Vec<IndexRow<K, S, V>>,
    primaries: &[K],
    label: &str,
) -> StorageResult<()>
where
    K: Serialize,
{
    let mut seen = Vec::<Vec<u8>>::new();
    let mut indexes = Vec::<usize>::new();
    for (index, primary) in primaries.iter().enumerate() {
        let id = strict_json_identity(primary)?;
        if seen.iter().any(|existing| existing == &id) {
            return Err(StorageError::backend(format!(
                "{label} {index} duplicates an earlier primary"
            )));
        }
        seen.push(id);
        let Some(row_index) = find_index_primary(rows, primary)? else {
            return Err(StorageError::backend(format!("{label} {index} is missing")));
        };
        indexes.push(row_index);
    }
    indexes.sort_unstable_by(|a, b| b.cmp(a));
    for index in indexes {
        rows.remove(index);
    }
    Ok(())
}

fn resolve_collection_snapshot_key(
    options: &LoadReactiveCollectionStateOptions<'_>,
) -> StorageResult<String> {
    if let Some(key) = options.snapshot_key {
        if key.is_empty() {
            return Err(StorageError::backend(
                "reactiveCollection load: snapshot_key must be non-empty",
            ));
        }
        return Ok(key.to_owned());
    }
    let prefix = options.storage_prefix.ok_or_else(|| {
        StorageError::backend("reactiveCollection load: storage_prefix or snapshot_key is required")
    })?;
    reactive_collection_snapshot_key(prefix)
}

fn strict_json_equal<T: Serialize>(left: &T, right: &T) -> StorageResult<bool> {
    let left = serde_json::to_value(left).map_err(storage_serde_json_error)?;
    let right = serde_json::to_value(right).map_err(storage_serde_json_error)?;
    Ok(
        strict_canonical_json_bytes(&left).map_err(storage_json_error)?
            == strict_canonical_json_bytes(&right).map_err(storage_json_error)?,
    )
}

fn strict_json_identity<T: Serialize>(value: &T) -> StorageResult<Vec<u8>> {
    let value = serde_json::to_value(value).map_err(storage_serde_json_error)?;
    strict_canonical_json_bytes(&value).map_err(storage_json_error)
}

fn seq_to_snapshot_cursor(seq: u64) -> StorageResult<i64> {
    i64::try_from(seq)
        .map_err(|_| StorageError::backend("reactiveCollection cursor exceeds i64 range"))
}

fn storage_error_to_json(error: StorageError) -> JsonCodecError {
    JsonCodecError::validation(error.to_string())
}

fn storage_serde_json_error(error: serde_json::Error) -> StorageError {
    StorageError::backend(format!("reactiveCollection JSON error: {error}"))
}

impl<T: Clone> AppendLogStorage<T> {
    fn next_seq(&self) -> StorageResult<u64> {
        next_seq_from_keys(&self.prefix, self.kv.list(&format!("{}/", self.prefix))?)
    }
}

impl<T: Clone> AppendLogStorageTier<T> for AppendLogStorage<T> {
    fn append(&self, value: T) -> StorageResult<AppendLogEntry<T>> {
        let seq = self.next_seq()?;
        let key = append_log_key(&self.prefix, seq);
        self.kv.set(&key, value.clone())?;
        Ok(AppendLogEntry { key, seq, value })
    }

    fn read(&self, opts: AppendLogReadOptions) -> StorageResult<Vec<AppendLogEntry<T>>> {
        read_append_log_entries(self.kv.as_ref(), &self.prefix, opts)
    }

    fn truncate_after(&self, seq: u64) -> StorageResult<()> {
        delete_append_log_entries_after(self.kv.as_ref(), &self.prefix, seq)
    }

    fn size(&self) -> StorageResult<usize> {
        size_from_keys(&self.prefix, &self.kv.list(&format!("{}/", self.prefix))?)
    }
}

impl<T: Clone> AppendLogStorageTier<T> for MultiWriterAppendLogStorage<T> {
    fn append(&self, value: T) -> StorageResult<AppendLogEntry<T>> {
        let mut seq =
            next_seq_from_keys(&self.prefix, self.kv.list(&format!("{}/", self.prefix))?)?;
        let mut attempts = 0;
        loop {
            if attempts >= self.max_attempts {
                let refreshed =
                    next_seq_from_keys(&self.prefix, self.kv.list(&format!("{}/", self.prefix))?)?;
                seq = seq.max(refreshed);
                attempts = 0;
            }
            attempts += 1;
            let key = append_log_key(&self.prefix, seq);
            if self.kv.put_if_absent(&key, value.clone())? {
                return Ok(AppendLogEntry { key, seq, value });
            }
            seq = seq.checked_add(1).ok_or_else(|| {
                StorageError::backend(format!(
                    "append log next sequence is outside the u64 range: {}",
                    self.prefix
                ))
            })?;
        }
    }

    fn read(&self, opts: AppendLogReadOptions) -> StorageResult<Vec<AppendLogEntry<T>>> {
        read_append_log_entries(self.kv.as_ref(), &self.prefix, opts)
    }

    fn truncate_after(&self, _seq: u64) -> StorageResult<()> {
        Err(StorageError::backend(
            "multi_writer_append_log_storage.truncate_after: unsupported without a stronger compaction capability",
        ))
    }

    fn size(&self) -> StorageResult<usize> {
        size_from_keys(&self.prefix, &self.kv.list(&format!("{}/", self.prefix))?)
    }
}

fn read_append_log_entries<T: Clone>(
    kv: &dyn KvStorageTier<T>,
    prefix: &str,
    opts: AppendLogReadOptions,
) -> StorageResult<Vec<AppendLogEntry<T>>> {
    let mut keys = kv
        .list(&format!("{prefix}/"))?
        .into_iter()
        .map(|key| seq_from_key(prefix, &key).map(|seq| (key, seq)))
        .collect::<StorageResult<Vec<_>>>()?;
    keys.retain(|(_, seq)| opts.after.is_none_or(|after| *seq > after));
    keys.sort_by_key(|(_, seq)| *seq);
    if let Some(limit) = opts.limit {
        keys.truncate(limit);
    }
    let mut entries = Vec::with_capacity(keys.len());
    for (key, seq) in keys {
        let value = kv.get(&key)?.ok_or_else(|| {
            StorageError::backend(format!("append log listed key is missing: {key}"))
        })?;
        entries.push(AppendLogEntry { key, seq, value });
    }
    Ok(entries)
}

fn delete_append_log_entries_after<T: Clone>(
    kv: &dyn KvStorageTier<T>,
    prefix: &str,
    seq: u64,
) -> StorageResult<()> {
    let keys = kv
        .list(&format!("{prefix}/"))?
        .into_iter()
        .map(|key| seq_from_key(prefix, &key).map(|parsed| (key, parsed)))
        .collect::<StorageResult<Vec<_>>>()?;
    for (key, parsed) in keys {
        if parsed > seq {
            kv.delete(&key)?;
        }
    }
    Ok(())
}

fn next_seq_from_keys(prefix: &str, keys: Vec<String>) -> StorageResult<u64> {
    let Some(max_seq) = keys
        .iter()
        .map(|key| seq_from_key(prefix, key))
        .collect::<StorageResult<Vec<_>>>()?
        .into_iter()
        .max()
    else {
        return Ok(0);
    };
    max_seq.checked_add(1).ok_or_else(|| {
        StorageError::backend(format!(
            "append log next sequence is outside the u64 range: {prefix}"
        ))
    })
}

fn size_from_keys(prefix: &str, keys: &[String]) -> StorageResult<usize> {
    for key in keys {
        seq_from_key(prefix, key)?;
    }
    Ok(keys.len())
}

fn seq_from_key(prefix: &str, key: &str) -> StorageResult<u64> {
    let head = format!("{prefix}/");
    let raw = key
        .strip_prefix(&head)
        .ok_or_else(|| StorageError::backend(format!("append log key outside prefix: {key}")))?;
    if !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(StorageError::backend(format!(
            "append log key has a non-numeric sequence: {key}"
        )));
    }
    if raw.len() != APPEND_LOG_SEQ_PAD {
        return Err(StorageError::backend(format!(
            "append log key sequence must be {APPEND_LOG_SEQ_PAD} padded digits: {key}"
        )));
    }
    raw.parse::<u64>().map_err(|_| {
        StorageError::backend(format!(
            "append log key sequence is outside the u64 range: {key}"
        ))
    })
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
