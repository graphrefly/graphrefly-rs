//! Filesystem-backed kv backend (M4.C — DS-14-storage Audit 4).
//!
//! [`FileBackend`] maps each key to a `.bin` file under a configured directory.
//! Keys are percent-encoded so any UTF-8 string can be stored safely:
//! `[a-zA-Z0-9_-]` pass through; everything else is UTF-8 encoded with each
//! byte formatted as lowercase `%xx`. The encoded filename for any given key
//! is byte-identical to the TS `fileBackend` impl
//! ([`packages/pure-ts/src/extra/storage/tiers-node.ts`](https://github.com/graphrefly/graphrefly-ts/blob/main/packages/pure-ts/src/extra/storage/tiers-node.ts) — D159) so a TS-written
//! file can be loaded by a Rust reader on the same directory.
//!
//! Writes are atomic via [`tempfile::NamedTempFile::persist`]: a tempfile is
//! created in the target directory, written in full, then renamed onto the
//! key path. A partially-written file is never visible at the final path,
//! even on process crash. The `NamedTempFile` Drop impl deletes any tempfile
//! that never made it through `persist` (covers panics between create and
//! commit).
//!
//! `flush()` is a no-op — durability is on per-write basis via the rename.
//! `read` / `delete` / `list` tolerate missing directory + missing key by
//! returning `Ok(None)` / `Ok(())` / `Ok(vec![])` respectively (D158).
//!
//! Cargo feature: gated behind `file` (default-on).

use std::fs;
use std::io;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{de::DeserializeOwned, Serialize};
use tempfile::NamedTempFile;

use crate::backend::StorageBackend;
use crate::codec::{Codec, JsonCodec};
use crate::error::StorageError;
use crate::memory::{
    append_log_storage, kv_storage, snapshot_storage, AppendLogStorage, AppendLogStorageOptions,
    KvStorage, KvStorageOptions, SnapshotStorage, SnapshotStorageOptions,
};

/// File extension applied to every key file. Inverse `decode_filename_to_key`
/// rejects entries that don't end in this suffix.
const FILE_SUFFIX: &str = ".bin";

/// Lowercase hex alphabet for `%xx` encoding. Lower case is required for
/// byte-equal cross-impl filenames; TS produces lowercase via
/// `Number.toString(16)`.
const HEX_LOWER: &[u8; 16] = b"0123456789abcdef";

/// Filesystem-backed [`StorageBackend`].
///
/// One file per key under `dir`. Concurrent writers are safe at the
/// per-key granularity (atomic rename via `tempfile`); concurrent writers
/// to the SAME key race in unspecified-but-atomic fashion (last commit wins).
///
/// # Filesystem portability
///
/// Key→filename encoding preserves ASCII case: `Foo` and `foo` encode to
/// `Foo.bin` and `foo.bin`. On case-insensitive filesystems (default macOS
/// APFS, default Windows NTFS) these collide. graphrefly-internal keys
/// (tier names, WAL frame paths) are case-consistent by construction, so
/// the collision is only reachable with adversarial user-supplied keys.
/// Lift documented in `porting-deferred.md` "M4.C `FileBackend`
/// case-insensitive-filesystem key collision".
///
/// # Example
///
/// ```ignore
/// use std::sync::Arc;
/// use graphrefly_storage::{file_backend, snapshot_storage, SnapshotStorageOptions};
///
/// let backend = file_backend("./checkpoints");
/// let tier = snapshot_storage(backend, SnapshotStorageOptions::<MyState, _>::default());
/// tier.save(state).unwrap();
/// ```
#[derive(Debug)]
pub struct FileBackend {
    dir: PathBuf,
    name: String,
    include_hidden: bool,
}

impl FileBackend {
    /// Construct a backend rooted at `dir`. The directory is created lazily on
    /// first `write()` — `read` / `list` / `delete` tolerate its absence.
    #[must_use]
    pub fn new(dir: impl AsRef<Path>) -> Self {
        let dir = dir.as_ref().to_path_buf();
        let name = format!("file:{}", dir.display());
        Self {
            dir,
            name,
            include_hidden: false,
        }
    }

    /// Override whether `list()` includes filenames beginning with `.` (D161).
    ///
    /// Default `false`: hidden filenames are skipped. This protects against
    /// in-flight `tempfile::NamedTempFile` temp files (which are created with
    /// a leading-`.` prefix) leaking into enumeration results during a
    /// concurrent flush.
    ///
    /// Pass `true` if your application intentionally writes keys whose
    /// percent-encoding produces a leading-`.` filename and you need them
    /// visible in `list()`.
    #[must_use]
    pub fn with_include_hidden(mut self, include: bool) -> Self {
        self.include_hidden = include;
        self
    }

    /// Backend root directory.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Whether `list()` includes dot-prefixed filenames.
    #[must_use]
    pub fn include_hidden(&self) -> bool {
        self.include_hidden
    }

    /// Per-key filesystem path (`<dir>/<encoded-key>.bin`).
    fn path_for(&self, key: &str) -> PathBuf {
        let mut filename = encode_key_to_filename(key);
        filename.push_str(FILE_SUFFIX);
        self.dir.join(filename)
    }
}

/// Convenience constructor returning an `Arc<FileBackend>`. Use this when
/// sharing a single backend across multiple tiers (the paired
/// `{ snapshot, wal }` pattern from DS-14-storage §a). For non-default
/// configuration use `Arc::new(FileBackend::new(dir).with_include_hidden(true))`.
#[must_use]
pub fn file_backend(dir: impl AsRef<Path>) -> Arc<FileBackend> {
    Arc::new(FileBackend::new(dir))
}

impl StorageBackend for FileBackend {
    fn name(&self) -> &str {
        &self.name
    }

    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        match fs::read(self.path_for(key)) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(io_error("read", &self.dir, e)),
        }
    }

    fn write(&self, key: &str, bytes: &[u8]) -> Result<(), StorageError> {
        fs::create_dir_all(&self.dir).map_err(|e| io_error("mkdir", &self.dir, e))?;
        let target = self.path_for(key);
        let mut tmp =
            NamedTempFile::new_in(&self.dir).map_err(|e| io_error("tempfile", &self.dir, e))?;
        tmp.write_all(bytes)
            .map_err(|e| io_error("write tmp", &self.dir, e))?;
        tmp.persist(&target)
            .map_err(|e| io_error("rename", &self.dir, e.error))?;
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<(), StorageError> {
        match fs::remove_file(self.path_for(key)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(io_error("delete", &self.dir, e)),
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        let entries = match fs::read_dir(&self.dir) {
            Ok(e) => e,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(io_error("list", &self.dir, e)),
        };
        let mut keys = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| io_error("list-entry", &self.dir, e))?;
            let raw = entry.file_name();
            let Some(name) = raw.to_str() else { continue };
            if !self.include_hidden && name.starts_with('.') {
                continue;
            }
            let Some(key) = decode_filename_to_key(name) else {
                continue;
            };
            if !prefix.is_empty() && !key.starts_with(prefix) {
                continue;
            }
            keys.push(key);
        }
        keys.sort();
        Ok(keys)
    }
}

fn io_error(op: &str, dir: &Path, source: io::Error) -> StorageError {
    StorageError::BackendError {
        message: format!("file backend {op} failed at {}: {source}", dir.display()),
        source: Some(Box::new(source)),
    }
}

/// Encode an arbitrary key to a safe filename stem.
///
/// `[a-zA-Z0-9_-]` pass through unencoded; everything else is UTF-8 encoded
/// and each byte is formatted as lowercase `%xx`. Cross-impl byte-identical
/// with TS [`pathFor`](https://github.com/graphrefly/graphrefly-ts/blob/main/packages/pure-ts/src/extra/storage/tiers-node.ts).
fn encode_key_to_filename(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    let mut buf = [0u8; 4];
    for ch in key.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
            out.push(ch);
            continue;
        }
        for &byte in ch.encode_utf8(&mut buf).as_bytes() {
            out.push('%');
            out.push(HEX_LOWER[(byte >> 4) as usize] as char);
            out.push(HEX_LOWER[(byte & 0x0F) as usize] as char);
        }
    }
    out
}

/// Inverse of [`encode_key_to_filename`].
///
/// Returns `None` when:
/// - the filename does not end in `.bin`
/// - the decoded byte sequence is not valid UTF-8
/// - the filename contains non-ASCII characters outside `%xx` escapes
///   (those can't have come from our encoder; matches TS behavior of treating
///   such filenames as un-decodable)
///
/// Truncated (`abc%5`) or invalid-hex (`abc%5z`) escapes fall through to
/// literal-byte semantics — matches the TS `keyFromFilename` regex-fallthrough
/// branch.
fn decode_filename_to_key(filename: &str) -> Option<String> {
    let stem = filename.strip_suffix(FILE_SUFFIX)?;
    let chars: Vec<char> = stem.chars().collect();
    let mut bytes: Vec<u8> = Vec::with_capacity(chars.len());
    let mut i = 0;
    while i < chars.len() {
        let ch = chars[i];
        if ch == '%' && i + 2 < chars.len() {
            if let (Some(hi), Some(lo)) = (nibble(chars[i + 1]), nibble(chars[i + 2])) {
                bytes.push((hi << 4) | lo);
                i += 3;
                continue;
            }
        }
        if !ch.is_ascii() {
            return None;
        }
        bytes.push(ch as u8);
        i += 1;
    }
    String::from_utf8(bytes).ok()
}

fn nibble(c: char) -> Option<u8> {
    c.to_digit(16).and_then(|d| u8::try_from(d).ok())
}

// ── Convenience tier wrappers ───────────────────────────────────────────────

/// Convenience: snapshot tier over a fresh file backend rooted at `dir`.
/// Mirror of [`crate::memory_snapshot`] for filesystem persistence.
#[must_use]
pub fn file_snapshot<T, C>(
    dir: impl AsRef<Path>,
    opts: SnapshotStorageOptions<T, C>,
) -> SnapshotStorage<FileBackend, T, C>
where
    T: Send + Sync + 'static,
    C: Codec<T>,
{
    snapshot_storage(Arc::new(FileBackend::new(dir)), opts)
}

/// Convenience: snapshot tier over a fresh file backend with
/// [`SnapshotStorageOptions::default`] + a `JsonCodec`.
#[must_use]
pub fn file_snapshot_default<T>(dir: impl AsRef<Path>) -> SnapshotStorage<FileBackend, T, JsonCodec>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    file_snapshot(dir, SnapshotStorageOptions::default())
}

/// Convenience: append-log tier over a fresh file backend rooted at `dir`.
#[must_use]
pub fn file_append_log<T, C>(
    dir: impl AsRef<Path>,
    opts: AppendLogStorageOptions<T, C>,
) -> AppendLogStorage<FileBackend, T, C>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    C: Codec<Vec<T>>,
{
    append_log_storage(Arc::new(FileBackend::new(dir)), opts)
}

/// Convenience: append-log tier over a fresh file backend with
/// [`AppendLogStorageOptions::default`] + a `JsonCodec`.
#[must_use]
pub fn file_append_log_default<T>(
    dir: impl AsRef<Path>,
) -> AppendLogStorage<FileBackend, T, JsonCodec>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    file_append_log(dir, AppendLogStorageOptions::default())
}

/// Convenience: kv tier over a fresh file backend rooted at `dir`.
#[must_use]
pub fn file_kv<T, C>(
    dir: impl AsRef<Path>,
    opts: KvStorageOptions<T, C>,
) -> KvStorage<FileBackend, T, C>
where
    T: Send + Sync + 'static,
    C: Codec<T>,
{
    kv_storage(Arc::new(FileBackend::new(dir)), opts)
}

/// Convenience: kv tier over a fresh file backend with
/// [`KvStorageOptions::default`] + a `JsonCodec`.
#[must_use]
pub fn file_kv_default<T>(dir: impl AsRef<Path>) -> KvStorage<FileBackend, T, JsonCodec>
where
    T: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    file_kv(dir, KvStorageOptions::default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_alphanumeric_passthrough() {
        assert_eq!(encode_key_to_filename("abcXYZ-_09"), "abcXYZ-_09");
    }

    #[test]
    fn encode_special_chars_percent_escape() {
        assert_eq!(
            encode_key_to_filename("app/with:slashes"),
            "app%2fwith%3aslashes"
        );
    }

    #[test]
    fn encode_non_ascii_two_byte_utf8() {
        // U+00E9 'é' = 0xC3 0xA9
        assert_eq!(encode_key_to_filename("café"), "caf%c3%a9");
    }

    #[test]
    fn encode_non_ascii_three_byte_utf8() {
        // U+20AC '€' = 0xE2 0x82 0xAC
        assert_eq!(encode_key_to_filename("€100"), "%e2%82%ac100");
    }

    #[test]
    fn encode_emoji_four_byte_utf8() {
        // U+1F44B 👋 = 0xF0 0x9F 0x91 0x8B
        assert_eq!(encode_key_to_filename("👋"), "%f0%9f%91%8b");
    }

    #[test]
    fn encode_empty_key() {
        assert_eq!(encode_key_to_filename(""), "");
    }

    #[test]
    fn decode_round_trip_covers_canonical_set() {
        for key in [
            "simple",
            "app/with:slashes",
            "café",
            "€100",
            "👋 hello",
            "a-b_c",
            "",
        ] {
            let filename = format!("{}.bin", encode_key_to_filename(key));
            assert_eq!(
                decode_filename_to_key(&filename).as_deref(),
                Some(key),
                "round-trip failed for {key:?}",
            );
        }
    }

    #[test]
    fn decode_rejects_non_bin_suffix() {
        assert!(decode_filename_to_key("foo.txt").is_none());
        assert!(decode_filename_to_key("foo").is_none());
        assert!(decode_filename_to_key(".bin").is_some()); // empty stem decodes to ""
    }

    #[test]
    fn decode_truncated_percent_escape_treated_literally() {
        // Matches TS keyFromFilename: incomplete `%x` at end falls through to
        // ASCII branch — `abc%5` decodes to `abc%5`.
        assert_eq!(
            decode_filename_to_key("abc%5.bin").as_deref(),
            Some("abc%5")
        );
    }

    #[test]
    fn decode_invalid_hex_treated_literally() {
        // `%5z` fails the hex check, falls through to per-char ASCII bytes.
        assert_eq!(
            decode_filename_to_key("abc%5z.bin").as_deref(),
            Some("abc%5z")
        );
    }

    #[test]
    fn decode_uppercase_hex_accepted() {
        // TS regex is /[0-9a-f]{2}$/i (case-insensitive); Rust mirrors via
        // char::to_digit which accepts both cases.
        assert_eq!(
            decode_filename_to_key("caf%C3%A9.bin").as_deref(),
            Some("café")
        );
    }

    #[test]
    fn decode_rejects_non_ascii_outside_escapes() {
        // A filename containing a literal non-ASCII char (not `%xx`) cannot
        // have come from our encoder; treat as un-decodable.
        assert!(decode_filename_to_key("café.bin").is_none());
    }

    #[test]
    fn nibble_validates_hex_set() {
        for c in ['0', '5', '9', 'a', 'f', 'A', 'F'] {
            assert!(nibble(c).is_some(), "{c} should be a hex digit");
        }
        for c in ['g', 'G', '/', '@', '\u{00e9}'] {
            assert!(nibble(c).is_none(), "{c} should not be a hex digit");
        }
    }
}
