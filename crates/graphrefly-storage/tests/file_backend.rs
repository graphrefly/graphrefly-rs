//! File backend integration tests (M4.C 2026-05-10).
//!
//! Mirrors the TS file backend test surface
//! (`packages/pure-ts/src/__tests__/extra/storage.test.ts`) — atomic write,
//! key sanitization, UTF-8 round-trip, missing-directory tolerance —
//! plus Rust-specific tests: cross-restart durability, `include_hidden`
//! option, temp-file cleanup verification, tier round-trip via
//! `file_snapshot` / `file_kv` / `file_append_log`.

#![cfg(feature = "file")]

use std::fs;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tempfile::TempDir;

use graphrefly_storage::{
    file_append_log_default, file_backend, file_kv_default, file_snapshot_default, FileBackend,
    SnapshotStorageTier, StorageBackend,
};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
struct Snap {
    name: String,
    value: u32,
}

// ── Bytes-level backend ───────────────────────────────────────────────────

#[test]
fn read_write_round_trip() {
    let dir = TempDir::new().unwrap();
    let backend = FileBackend::new(dir.path());
    backend.write("hello", b"world").unwrap();
    assert_eq!(backend.read("hello").unwrap(), Some(b"world".to_vec()));
}

#[test]
fn read_miss_returns_none() {
    let dir = TempDir::new().unwrap();
    let backend = FileBackend::new(dir.path());
    assert_eq!(backend.read("nope").unwrap(), None);
}

#[test]
fn write_overwrites_atomically() {
    let dir = TempDir::new().unwrap();
    let backend = FileBackend::new(dir.path());
    backend.write("k", b"v1").unwrap();
    backend.write("k", b"v2").unwrap();
    assert_eq!(backend.read("k").unwrap(), Some(b"v2".to_vec()));
}

#[test]
fn write_leaves_no_temp_files_in_dir() {
    let dir = TempDir::new().unwrap();
    let backend = FileBackend::new(dir.path());
    backend.write("k", b"v").unwrap();
    // After successful write, the only .bin file is the target — no temp
    // files (tempfile::NamedTempFile creates `.tmpXXXXXX` and the persist
    // call renames it onto target).
    let entries: Vec<String> = fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    let bin_count = entries.iter().filter(|n| n.ends_with(".bin")).count();
    let tmp_count = entries.iter().filter(|n| n.starts_with(".tmp")).count();
    assert_eq!(bin_count, 1, "expected exactly one .bin file: {entries:?}");
    assert_eq!(tmp_count, 0, "stray temp files: {entries:?}");
}

#[test]
fn delete_removes_key() {
    let dir = TempDir::new().unwrap();
    let backend = FileBackend::new(dir.path());
    backend.write("k", b"v").unwrap();
    backend.delete("k").unwrap();
    assert_eq!(backend.read("k").unwrap(), None);
}

#[test]
fn delete_is_idempotent_on_missing_key() {
    let dir = TempDir::new().unwrap();
    let backend = FileBackend::new(dir.path());
    // No prior write — delete should not error.
    backend.delete("never-existed").unwrap();
}

#[test]
fn delete_tolerates_missing_directory() {
    let dir = TempDir::new().unwrap();
    let nested = dir.path().join("never-created");
    let backend = FileBackend::new(&nested);
    // `read` / `delete` / `list` all tolerate the missing dir.
    backend.delete("k").unwrap();
    assert!(!nested.exists(), "delete should not create the directory");
}

#[test]
fn list_returns_empty_when_directory_missing() {
    let dir = TempDir::new().unwrap();
    let nested = dir.path().join("never-created");
    let backend = FileBackend::new(&nested);
    assert_eq!(backend.list("").unwrap(), Vec::<String>::new());
    assert!(!nested.exists());
}

#[test]
fn list_lex_asc_with_prefix() {
    let dir = TempDir::new().unwrap();
    let backend = FileBackend::new(dir.path());
    backend.write("g/10", b"a").unwrap();
    backend.write("g/02", b"b").unwrap();
    backend.write("g/01", b"c").unwrap();
    backend.write("other", b"d").unwrap();
    assert_eq!(backend.list("g/").unwrap(), vec!["g/01", "g/02", "g/10"]);
    let mut all = backend.list("").unwrap();
    all.sort();
    assert_eq!(all, vec!["g/01", "g/02", "g/10", "other"]);
}

#[test]
fn list_skips_hidden_by_default() {
    let dir = TempDir::new().unwrap();
    let backend = FileBackend::new(dir.path());
    backend.write("visible", b"v").unwrap();
    // Drop a hidden file manually — should be invisible to list().
    fs::write(dir.path().join(".secret.bin"), b"x").unwrap();
    fs::write(dir.path().join(".tmpABCDEF"), b"x").unwrap();
    assert_eq!(backend.list("").unwrap(), vec!["visible"]);
}

#[test]
fn list_with_include_hidden_surfaces_dot_files() {
    let dir = TempDir::new().unwrap();
    let backend = FileBackend::new(dir.path()).with_include_hidden(true);
    backend.write("visible", b"v").unwrap();
    fs::write(dir.path().join(".secret.bin"), b"x").unwrap();
    // `.tmpABCDEF` has no .bin suffix so it's filtered regardless.
    fs::write(dir.path().join(".tmpABCDEF"), b"x").unwrap();
    let mut keys = backend.list("").unwrap();
    keys.sort();
    assert_eq!(keys, vec![".secret", "visible"]);
}

#[test]
fn write_creates_directory_lazily() {
    let dir = TempDir::new().unwrap();
    let nested = dir.path().join("nested").join("deeper");
    let backend = FileBackend::new(&nested);
    assert!(!nested.exists());
    backend.write("k", b"v").unwrap();
    assert!(nested.exists(), "write should mkdir -p");
    assert_eq!(backend.read("k").unwrap(), Some(b"v".to_vec()));
}

#[test]
fn key_with_unsafe_chars_round_trips() {
    let dir = TempDir::new().unwrap();
    let backend = FileBackend::new(dir.path());
    backend.write("app/with:slashes", b"v").unwrap();
    assert_eq!(
        backend.read("app/with:slashes").unwrap(),
        Some(b"v".to_vec())
    );
    // Encoded filename uses %xx — no literal slash on disk.
    let on_disk: Vec<String> = fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(on_disk, vec!["app%2fwith%3aslashes.bin"]);
}

#[test]
fn keys_with_non_ascii_round_trip_via_utf8() {
    let dir = TempDir::new().unwrap();
    let backend = FileBackend::new(dir.path());
    let keys = ["café", "€100", "👋 hello"];
    for k in keys {
        backend.write(k, k.as_bytes()).unwrap();
    }
    for k in keys {
        assert_eq!(backend.read(k).unwrap(), Some(k.as_bytes().to_vec()));
    }
    let mut listed = backend.list("").unwrap();
    listed.sort();
    let mut expected: Vec<String> = keys.iter().map(|s| s.to_string()).collect();
    expected.sort();
    assert_eq!(listed, expected);
    for k in keys {
        backend.delete(k).unwrap();
    }
    assert_eq!(backend.list("").unwrap(), Vec::<String>::new());
}

#[test]
fn name_carries_dir_path() {
    let dir = TempDir::new().unwrap();
    let backend = FileBackend::new(dir.path());
    let name = backend.name();
    assert!(name.starts_with("file:"), "name was {name:?}");
    assert!(name.contains(&dir.path().display().to_string()));
}

#[test]
fn file_backend_factory_returns_shared_arc() {
    let dir = TempDir::new().unwrap();
    let b: Arc<FileBackend> = file_backend(dir.path());
    let b2 = Arc::clone(&b);
    b.write("k", b"v").unwrap();
    assert_eq!(b2.read("k").unwrap(), Some(b"v".to_vec()));
}

#[test]
fn include_hidden_default_is_false_and_accessor_visible() {
    let dir = TempDir::new().unwrap();
    let b = FileBackend::new(dir.path());
    assert!(!b.include_hidden());
    let b2 = b.with_include_hidden(true);
    assert!(b2.include_hidden());
}

#[test]
fn dir_accessor_returns_input_path() {
    let dir = TempDir::new().unwrap();
    let b = FileBackend::new(dir.path());
    assert_eq!(b.dir(), dir.path());
}

// ── Tier-level wrappers ───────────────────────────────────────────────────

#[test]
fn file_snapshot_round_trip_and_overwrite() {
    let dir = TempDir::new().unwrap();
    let tier = file_snapshot_default::<Snap>(dir.path());
    let s1 = Snap {
        name: "g".into(),
        value: 1,
    };
    tier.save(s1.clone()).unwrap();
    assert_eq!(tier.load().unwrap(), Some(s1));
    let s2 = Snap {
        name: "g".into(),
        value: 2,
    };
    tier.save(s2.clone()).unwrap();
    assert_eq!(tier.load().unwrap(), Some(s2));
}

#[test]
fn file_snapshot_cross_restart_durability() {
    let dir = TempDir::new().unwrap();
    let snapshot = Snap {
        name: "persisted".into(),
        value: 42,
    };
    {
        let tier = file_snapshot_default::<Snap>(dir.path());
        tier.save(snapshot.clone()).unwrap();
        // Drop tier — verifies the rename is durable across tier lifetimes.
    }
    {
        let tier = file_snapshot_default::<Snap>(dir.path());
        assert_eq!(tier.load().unwrap(), Some(snapshot));
    }
}

#[test]
fn file_kv_round_trip_with_delete() {
    use graphrefly_storage::KvStorageTier;
    let dir = TempDir::new().unwrap();
    let tier = file_kv_default::<Snap>(dir.path());
    let v = Snap {
        name: "alpha".into(),
        value: 7,
    };
    tier.save("alpha", v.clone()).unwrap();
    assert_eq!(tier.load("alpha").unwrap(), Some(v));
    tier.delete("alpha").unwrap();
    assert_eq!(tier.load("alpha").unwrap(), None);
}

#[test]
fn file_append_log_accumulates_and_loads() {
    use graphrefly_storage::AppendLogStorageTier;
    let dir = TempDir::new().unwrap();
    let tier = file_append_log_default::<Snap>(dir.path());
    let entries = vec![
        Snap {
            name: "a".into(),
            value: 1,
        },
        Snap {
            name: "b".into(),
            value: 2,
        },
    ];
    tier.append_entries(&entries).unwrap();
    let loaded: Vec<Snap> = tier.load_entries_all(None).unwrap();
    assert_eq!(loaded, entries);
}

// ── /qa P1–P2: edge-case coverage ──────────────────────────────────────────

#[test]
fn read_tolerates_missing_directory() {
    let dir = TempDir::new().unwrap();
    let nested = dir.path().join("never-created");
    let backend = FileBackend::new(&nested);
    // read on a path under a non-existent dir returns Ok(None), same as
    // delete and list (which already have tests above).
    assert_eq!(backend.read("k").unwrap(), None);
    assert!(!nested.exists(), "read should not create the directory");
}

#[test]
fn empty_key_write_is_hidden_from_default_list() {
    let dir = TempDir::new().unwrap();
    let backend = FileBackend::new(dir.path());
    backend.write("", b"empty").unwrap();
    // "" encodes to "" → path_for("") = <dir>/.bin (dot-prefixed).
    // Default include_hidden=false filters it from list — TS-parity.
    assert_eq!(backend.list("").unwrap(), Vec::<String>::new());
    // But read still works — the file IS on disk.
    assert_eq!(backend.read("").unwrap(), Some(b"empty".to_vec()));
    // With include_hidden=true, the empty key is visible.
    let backend_h = FileBackend::new(dir.path()).with_include_hidden(true);
    assert!(backend_h.list("").unwrap().contains(&String::new()));
}
