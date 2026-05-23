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

use std::collections::HashMap;
use std::fs;
use std::io;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

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
/// # Filesystem portability (B2 — 2026-05-22, /porting-to-rs)
///
/// Key→filename encoding preserves ASCII case: `Foo` and `foo` encode to
/// `Foo.bin` and `foo.bin`. On case-insensitive filesystems (default macOS
/// APFS, default Windows NTFS) these collide silently — last `write` wins.
///
/// To surface this loudly rather than corrupting data, `FileBackend` probes
/// the filesystem on first `write()` and rejects subsequent writes whose
/// encoded filename differs from a previously-written key only in casing.
/// The probe is per-instance and runs at most once.
///
/// - **Case-sensitive filesystems** (Linux ext4/tmpfs, macOS APFS configured
///   case-sensitive at format time): no enforcement; both `Foo` and `foo`
///   succeed and resolve to distinct files.
/// - **Case-insensitive filesystems** (default macOS APFS, Windows NTFS):
///   second of `Foo` / `foo` fails with [`StorageError::BackendError`] whose
///   message names both the existing and would-collide keys for diagnosis.
/// - Read / list / delete paths are zero-overhead — the probe runs only on
///   `write`, since collisions are write-introduced.
///
/// Tests force the probe outcome via
/// [`FileBackend::with_case_insensitive`] so they're FS-independent.
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
    /// Case-sensitivity state, lazily initialized on first `write()`.
    /// `None` until probed; `Some(false)` = case-sensitive (zero enforcement);
    /// `Some(true)` = case-insensitive (track `seen_keys` and reject
    /// case-divergent collisions).
    case_state: OnceLock<CaseState>,
    /// Probe-outcome override. `None` = probe naturally on first write;
    /// `Some(b)` = skip probe and force `case_state` to `Some(b)`. Set via
    /// [`Self::with_case_insensitive`] for FS-independent tests.
    case_override: Option<bool>,
}

/// Resolved case-sensitivity classification + collision tracker.
#[derive(Debug)]
enum CaseState {
    /// Filesystem distinguishes `Foo` from `foo`; no enforcement needed.
    Sensitive,
    /// Filesystem treats `Foo` and `foo` as the same file. Track the
    /// canonical (lowercase) encoded filename → original encoded filename so
    /// each subsequent write can detect cross-case collisions.
    Insensitive {
        seen: Mutex<HashMap<String, String>>,
    },
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
            case_state: OnceLock::new(),
            case_override: None,
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

    /// Override the filesystem case-sensitivity probe outcome (B2,
    /// 2026-05-22). `Some(true)` forces case-insensitive enforcement;
    /// `Some(false)` forces case-sensitive (skips enforcement). The natural
    /// probe is bypassed when set.
    ///
    /// **Internal test hook only.** Gated behind `cfg(any(test,
    /// feature = "test-hooks"))` so production callers cannot construct
    /// a `FileBackend` with a misleading case-sensitivity classification
    /// (e.g., `with_case_insensitive(false)` on an APFS volume would
    /// re-introduce the silent-overwrite hazard B2 closes). The override
    /// exists so unit tests can exercise both branches independently of
    /// the host filesystem (macOS CI runners default to APFS case-
    /// insensitive; Linux CI runners default to ext4/tmpfs case-sensitive).
    ///
    /// /qa G2.4 (2026-05-22): the original `pub` form was a public-API
    /// expansion that escaped the porting-deferred close. Tightened to
    /// test-only visibility.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    #[must_use]
    pub fn with_case_insensitive(mut self, forced: bool) -> Self {
        self.case_override = Some(forced);
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

    /// Encoded filename (sans dir) for a key — used by the case-collision
    /// tracker for case-folded comparison.
    fn filename_for(key: &str) -> String {
        let mut filename = encode_key_to_filename(key);
        filename.push_str(FILE_SUFFIX);
        filename
    }

    /// Resolve `case_state`, running the filesystem probe lazily if needed.
    /// Called from `write()` only — read / list / delete paths skip this so
    /// they retain zero overhead. The probe runs at most once per
    /// `FileBackend` instance.
    fn ensure_case_state(&self) -> &CaseState {
        self.case_state.get_or_init(|| {
            // Respect the explicit override first (test-only hook).
            if let Some(forced) = self.case_override {
                return if forced {
                    CaseState::Insensitive {
                        seen: Mutex::new(HashMap::new()),
                    }
                } else {
                    CaseState::Sensitive
                };
            }
            match probe_case_sensitivity(&self.dir) {
                Some(true) => CaseState::Insensitive {
                    seen: Mutex::new(HashMap::new()),
                },
                Some(false) | None => CaseState::Sensitive,
            }
        })
    }

    /// On case-insensitive filesystems, ensure `key`'s encoded filename
    /// doesn't collide with a previously-written key that differs only in
    /// casing. Returns the encoded filename for atomic insertion by the
    /// caller post-success.
    ///
    /// On case-sensitive filesystems, no-op.
    fn check_case_collision(&self, key: &str) -> Result<(), StorageError> {
        let CaseState::Insensitive { seen } = self.ensure_case_state() else {
            return Ok(());
        };
        let filename = Self::filename_for(key);
        let folded = filename.to_ascii_lowercase();
        // Lock scope: short; the map is touched only on writes.
        let mut guard = seen.lock().expect("case-collision tracker poisoned");
        if let Some(existing) = guard.get(&folded) {
            if existing != &filename {
                return Err(StorageError::BackendError {
                    message: format!(
                        "case-insensitive filesystem collision: existing key \
                         file {existing:?} and new key file {filename:?} \
                         (encoded from {key:?}) map to the same on-disk path \
                         when case-folded; FileBackend rejects to prevent \
                         silent overwrite",
                    ),
                    source: None,
                });
            }
        } else {
            guard.insert(folded, filename);
        }
        Ok(())
    }

    /// Drop a key from the case-collision tracker (allows the casing to be
    /// reused after `delete`). No-op on case-sensitive filesystems.
    fn release_case_slot(&self, key: &str) {
        // Read-only access to `case_state` — DO NOT trigger the probe here.
        // `delete()` should not pay probe cost.
        let Some(CaseState::Insensitive { seen }) = self.case_state.get() else {
            return;
        };
        let filename = Self::filename_for(key);
        let folded = filename.to_ascii_lowercase();
        if let Ok(mut guard) = seen.lock() {
            // Only release if the slot holds our exact casing — avoids
            // accidentally clearing a slot held by another casing of the
            // same key (which would itself have failed `check_case_collision`).
            if guard.get(&folded) == Some(&filename) {
                guard.remove(&folded);
            }
        }
    }
}

/// Probe whether the directory's filesystem treats casing as significant.
///
/// Returns `Some(true)` for case-insensitive, `Some(false)` for case-sensitive.
/// Returns `None` if the probe cannot complete (directory not creatable,
/// permission errors, etc.) — caller defaults to case-sensitive (no
/// enforcement) so the probe failure mode is "lose protection," never
/// "spurious rejection."
///
/// Algorithm: write a uniquely-named probe file, attempt `fs::metadata` of
/// the same path uppercased, delete the probe file. The same-length match
/// indicates the upper-cased path resolved to the lower-cased probe file —
/// case-insensitivity.
/// /qa G2.2 (2026-05-22): process-wide monotonic nonce. Two
/// `FileBackend`s probing the same directory in the same nanosecond on
/// systems with a coarse `SystemTime` resolution would otherwise share
/// a probe filename and race each other's results. The nonce
/// guarantees a unique probe filename even on low-resolution clocks.
static PROBE_NONCE: AtomicU64 = AtomicU64::new(0);

/// /qa G2.2 (2026-05-22): sweep orphan probe files left behind by
/// SIGKILL'd or panicked prior runs. Probe files use the
/// `.gr-case-probe-*` pattern; the leading `.` keeps them invisible to
/// `list()` (D161 hidden filter), but they accumulate across crashes.
/// Sweep runs at most once per process via the [`SWEPT`] `OnceLock`;
/// any `.gr-case-probe-*` file is removed regardless of age — they are
/// always short-lived and any survivor is by definition orphan.
fn sweep_orphan_probe_files(dir: &Path) {
    use std::collections::HashSet;
    static SWEPT: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
    let swept = SWEPT.get_or_init(|| Mutex::new(HashSet::new()));
    let Ok(mut guard) = swept.lock() else {
        return; // poisoned — skip the sweep, not load-bearing
    };
    if guard.contains(dir) {
        return;
    }
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let Some(name_str) = name.to_str() else {
                continue;
            };
            if name_str.starts_with(".gr-case-probe-") || name_str.starts_with(".GR-CASE-PROBE-") {
                let _ = fs::remove_file(entry.path());
            }
        }
    }
    guard.insert(dir.to_path_buf());
}

fn probe_case_sensitivity(dir: &Path) -> Option<bool> {
    fs::create_dir_all(dir).ok()?;
    // /qa G2.2: sweep orphans first so a SIGKILL'd prior run can't leave
    // residue that pollutes a future `list()` on this directory.
    sweep_orphan_probe_files(dir);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_nanos();
    let pid = std::process::id();
    // /qa G2.2: process-wide monotonic nonce closes the
    // two-backends-same-nanosecond race vector.
    let nonce = PROBE_NONCE.fetch_add(1, Ordering::Relaxed);
    // Single canonical filename: lower-case stem. Probe via upper-case lookup.
    // Leading `.` keeps the probe file invisible to `list()` (D161 hidden filter).
    let lower_name = format!(".gr-case-probe-{pid}-{nanos}-{nonce}-a.bin");
    let upper_name = lower_name.to_ascii_uppercase();
    let lower_path = dir.join(&lower_name);
    let upper_path = dir.join(&upper_name);
    let _ = fs::write(&lower_path, b"probe");
    let result = fs::metadata(&upper_path).is_ok();
    let _ = fs::remove_file(&lower_path);
    // Best-effort: if the upper-case path was somehow created as a distinct
    // file (theoretically impossible on a case-sensitive FS since we only
    // wrote the lower-case path), clean it up too.
    let _ = fs::remove_file(&upper_path);
    Some(result)
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
        // B2 (2026-05-22): on case-insensitive filesystems, reject writes
        // whose encoded filename differs from a previously-written key only
        // in casing. Probe runs at most once per backend instance. Checked
        // BEFORE the atomic-rename write so a rejected write leaves no
        // tempfile residue.
        self.check_case_collision(key)?;
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
        // B2 + /qa G2.3 (2026-05-22): on a case-insensitive filesystem,
        // `path_for("Foo")` and `path_for("foo")` resolve to the SAME
        // on-disk file. Releasing the case-collision slot BEFORE
        // `fs::remove_file` opens a clobber race: thread A releases
        // "Foo", thread B writes "foo" (passes case-check, becomes the
        // canonical casing), thread A's `fs::remove_file` then removes
        // thread B's just-written data. Sequence the ops so the slot
        // release happens AFTER the on-disk delete succeeds.
        match fs::remove_file(self.path_for(key)) {
            Ok(()) => {
                self.release_case_slot(key);
                Ok(())
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                // File never existed — still safe to drop the slot, but
                // do it after the kind-check so a failing `remove_file`
                // doesn't strand the tracker entry.
                self.release_case_slot(key);
                Ok(())
            }
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

    // ── B2 (2026-05-22, /porting-to-rs storage-honest-error batch) ─────────
    //
    // Case-collision detection on case-insensitive filesystems.
    //
    // The tests use `FileBackend::with_case_insensitive(forced)` to bypass
    // the natural filesystem probe — keeps outcomes deterministic across CI
    // hosts (macOS APFS default = case-insensitive; Linux ext4 default =
    // case-sensitive).

    #[test]
    fn case_insensitive_rejects_case_divergent_second_write() {
        // Force case-insensitive enforcement regardless of the underlying
        // filesystem. Then write `Foo` followed by `foo` and expect the
        // second to fail with a clear diagnostic.
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = FileBackend::new(dir.path()).with_case_insensitive(true);
        backend
            .write("Foo", b"first")
            .expect("first write must succeed");
        let err = backend
            .write("foo", b"second")
            .expect_err("case-divergent second write must reject");
        let StorageError::BackendError { message, .. } = err else {
            panic!("expected StorageError::BackendError, got: {err:?}");
        };
        assert!(
            message.contains("case-insensitive filesystem collision"),
            "diagnostic must label the failure class, got: {message}"
        );
        assert!(
            message.contains("Foo.bin") && message.contains("foo.bin"),
            "diagnostic must name both colliding encoded filenames, got: {message}"
        );
    }

    #[test]
    fn case_insensitive_same_casing_overwrites() {
        // Writing the same key twice (same casing) is the normal overwrite
        // case — must not be flagged as a collision.
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = FileBackend::new(dir.path()).with_case_insensitive(true);
        backend.write("Foo", b"first").expect("first write");
        backend
            .write("Foo", b"second")
            .expect("same-casing overwrite must succeed");
        let read = backend.read("Foo").expect("read").expect("present");
        assert_eq!(read, b"second");
    }

    #[test]
    fn case_insensitive_delete_releases_slot() {
        // After deleting `Foo`, writing `foo` must succeed — the casing slot
        // was released by the delete.
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = FileBackend::new(dir.path()).with_case_insensitive(true);
        backend.write("Foo", b"first").expect("write Foo");
        backend.delete("Foo").expect("delete Foo");
        backend.write("foo", b"new").expect("post-delete write foo");
        let read = backend.read("foo").expect("read foo").expect("present");
        assert_eq!(read, b"new");
    }

    #[test]
    fn case_sensitive_allows_case_divergent_writes() {
        // On a forced-sensitive backend, `Foo` and `foo` must both succeed
        // and resolve to distinct files. We can't verify distinct on-disk
        // files on a case-insensitive host (the second write would clobber
        // the first), so we only assert the calls succeed and the
        // collision tracker doesn't fire.
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = FileBackend::new(dir.path()).with_case_insensitive(false);
        backend.write("Foo", b"first").expect("write Foo");
        backend
            .write("foo", b"second")
            .expect("forced-sensitive backend must not reject case-divergent keys");
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
