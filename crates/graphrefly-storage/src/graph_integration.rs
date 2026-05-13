//! Graph-level storage integration (M4.E2 — D170–D174).
//!
//! Free functions (not `Graph` methods) because `graphrefly-graph` does not
//! depend on `graphrefly-storage` — the dependency flows in the other
//! direction. See D170.
//!
//! # Provided APIs
//!
//! - [`GraphCheckpointRecord`] — portable baseline type wrapping a
//!   [`GraphPersistSnapshot`] + seq metadata + `format_version` (F7 close).
//! - [`diff_snapshots`] / [`decompose_diff_to_frames`] — snapshot diff →
//!   WAL frame generation engine (D172).
//! - [`attach_snapshot_storage`] + [`StorageHandle`] — wire observe
//!   subscription → snapshot diff → WAL frame writes.
//! - [`restore_snapshot`] — three-phase replay (baseline → checksum verify
//!   → lifecycle-scoped batch).
//!
//! # Manifest (D173 — F4 close)
//!
//! F4 (cross-restart key recovery) is structurally closed by D174: at the
//! attach boundary, `key_of` is derived deterministically from
//! `graph.name`. On restore, the snapshot key is known without a separate
//! manifest entry. The checkpoint record's `seq` field serves as the WAL
//! high-water mark.

use std::sync::{Arc, Mutex};

use graphrefly_core::Message;
use graphrefly_graph::{Graph, GraphPersistSnapshot, NodeSlice};
use graphrefly_structures::{BaseChange, Lifecycle, Version};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{PhaseStat, RestoreError, RestoreResult, StorageError};
use crate::tier::{KvStorageTier, SnapshotStorageTier};
use crate::wal::{
    graph_wal_prefix, verify_wal_frame_checksum, wal_frame_checksum, wal_frame_key, WALFrame,
    WalTag, REPLAY_ORDER,
};

// ── Constants ──────────────────────────────────────────────────────────────

/// Current snapshot format version. Embedded in [`GraphCheckpointRecord`]
/// and in `BaseChange.version` within decomposed WAL frames (F7 close).
pub const SNAPSHOT_VERSION: u64 = 1;

// ── Types ──────────────────────────────────────────────────────────────────

/// Portable baseline record written by [`attach_snapshot_storage`] on
/// full-snapshot writes. Contains the full [`GraphPersistSnapshot`] plus
/// metadata for WAL cursor alignment.
///
/// The `format_version` field closes F7 (missing `format_version` on
/// `WALFrame` and checkpoint records).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphCheckpointRecord {
    /// Graph name (matches `snapshot.name`).
    pub name: String,
    /// Snapshot mode — `"full"` for baseline, `"diff"` reserved for future
    /// incremental baselines.
    pub mode: String,
    /// The complete graph state.
    pub snapshot: GraphPersistSnapshot,
    /// WAL-tier cursor at baseline write time. Frames with `frame_seq >
    /// seq` are the delta.
    pub seq: u64,
    /// Wall-clock timestamp at baseline write time.
    pub timestamp_ns: u64,
    /// Format version (F7 close).
    pub format_version: u64,
}

// ── Diff engine (D172) ─────────────────────────────────────────────────────

/// Structural diff between two [`GraphPersistSnapshot`]s.
#[derive(Debug, Clone)]
pub struct GraphSnapshotDiff {
    /// Node names present in `after` but not `before`.
    pub nodes_added: Vec<String>,
    /// Full slices for added nodes (parallel to `nodes_added`).
    pub nodes_added_slices: Vec<NodeSlice>,
    /// Node names present in `before` but not `after`.
    pub nodes_removed: Vec<String>,
    /// Nodes whose `value` field changed between snapshots.
    pub value_changes: Vec<ValueChange>,
    /// Subgraph mount names added.
    pub subgraphs_added: Vec<String>,
    /// Subgraph mount names removed.
    pub subgraphs_removed: Vec<String>,
}

impl GraphSnapshotDiff {
    /// True when no structural or value changes were detected.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.nodes_added.is_empty()
            && self.nodes_removed.is_empty()
            && self.value_changes.is_empty()
            && self.subgraphs_added.is_empty()
            && self.subgraphs_removed.is_empty()
    }
}

/// A single node value change detected by [`diff_snapshots`].
#[derive(Debug, Clone)]
pub struct ValueChange {
    /// Node path that changed.
    pub path: String,
    /// New value. `None` means the node transitioned to sentinel (INVALIDATE).
    pub to: Option<Value>,
}

/// Compare two snapshots and produce a structural diff.
///
/// Only examines the top-level namespace (not recursive into subgraphs —
/// subgraph diffs are handled by the attach wiring per-subgraph).
#[must_use]
pub fn diff_snapshots(
    before: &GraphPersistSnapshot,
    after: &GraphPersistSnapshot,
) -> GraphSnapshotDiff {
    let mut nodes_added = Vec::new();
    let mut nodes_added_slices = Vec::new();
    let mut nodes_removed = Vec::new();
    let mut value_changes = Vec::new();
    let mut subgraphs_added = Vec::new();
    let mut subgraphs_removed = Vec::new();

    // Nodes added or changed.
    for (name, after_slice) in &after.nodes {
        if let Some(before_slice) = before.nodes.get(name) {
            if before_slice.value != after_slice.value {
                value_changes.push(ValueChange {
                    path: name.clone(),
                    to: after_slice.value.clone(),
                });
            }
        } else {
            nodes_added.push(name.clone());
            nodes_added_slices.push(after_slice.clone());
        }
    }

    // Nodes removed.
    for name in before.nodes.keys() {
        if !after.nodes.contains_key(name) {
            nodes_removed.push(name.clone());
        }
    }

    // Subgraphs added/removed.
    for name in after.subgraphs.keys() {
        if !before.subgraphs.contains_key(name) {
            subgraphs_added.push(name.clone());
        }
    }
    for name in before.subgraphs.keys() {
        if !after.subgraphs.contains_key(name) {
            subgraphs_removed.push(name.clone());
        }
    }

    GraphSnapshotDiff {
        nodes_added,
        nodes_added_slices,
        nodes_removed,
        value_changes,
        subgraphs_added,
        subgraphs_removed,
    }
}

/// Intermediate frame before checksum stamping.
struct DecomposedFrame {
    lifecycle: Lifecycle,
    path: String,
    change: BaseChange<Value>,
}

/// Convert a [`GraphSnapshotDiff`] into WAL frames ready for persistence.
///
/// `timestamp_ns` is the wall-clock at diff time. `base_seq` is the WAL
/// cursor; returned frames have `frame_seq` values starting at
/// `base_seq + 1`.
///
/// Returns `(frames, next_seq)` where `next_seq` is the highest assigned
/// `frame_seq`.
pub fn decompose_diff_to_frames(
    diff: &GraphSnapshotDiff,
    timestamp_ns: u64,
    base_seq: u64,
) -> Result<(Vec<WALFrame<Value>>, u64), StorageError> {
    let mut decomposed = Vec::new();

    let wrap = |structure: &str, lifecycle: Lifecycle, payload: Value| -> BaseChange<Value> {
        BaseChange {
            structure: structure.to_owned(),
            version: Version::Counter(SNAPSHOT_VERSION),
            t_ns: timestamp_ns,
            seq: None,
            lifecycle,
            change: payload,
        }
    };

    // Spec lifecycle: node add/remove, subgraph mount/unmount.
    for (i, name) in diff.nodes_added.iter().enumerate() {
        let slice = &diff.nodes_added_slices[i];
        let payload = serde_json::json!({
            "kind": "graph.add",
            "nodeId": name,
            "slice": serde_json::to_value(slice).map_err(|e|
                StorageError::Codec(crate::codec::CodecError::Encode(e.to_string()))
            )?,
        });
        decomposed.push(DecomposedFrame {
            lifecycle: Lifecycle::Spec,
            path: name.clone(),
            change: wrap("graph.spec", Lifecycle::Spec, payload),
        });
    }

    for name in &diff.nodes_removed {
        let payload = serde_json::json!({
            "kind": "graph.remove",
            "nodeId": name,
        });
        decomposed.push(DecomposedFrame {
            lifecycle: Lifecycle::Spec,
            path: name.clone(),
            change: wrap("graph.spec", Lifecycle::Spec, payload),
        });
    }

    for name in &diff.subgraphs_added {
        let payload = serde_json::json!({
            "kind": "graph.mount",
            "path": name,
            "subgraphId": name,
        });
        decomposed.push(DecomposedFrame {
            lifecycle: Lifecycle::Spec,
            path: name.clone(),
            change: wrap("graph.spec", Lifecycle::Spec, payload),
        });
    }

    for name in &diff.subgraphs_removed {
        let payload = serde_json::json!({
            "kind": "graph.unmount",
            "path": name,
        });
        decomposed.push(DecomposedFrame {
            lifecycle: Lifecycle::Spec,
            path: name.clone(),
            change: wrap("graph.spec", Lifecycle::Spec, payload),
        });
    }

    // Data lifecycle: value changes.
    for vc in &diff.value_changes {
        let payload = if let Some(ref value) = vc.to {
            serde_json::json!({
                "kind": "node.set",
                "path": vc.path,
                "value": value,
            })
        } else {
            serde_json::json!({
                "kind": "node.invalidate",
                "path": vc.path,
            })
        };
        decomposed.push(DecomposedFrame {
            lifecycle: Lifecycle::Data,
            path: vc.path.clone(),
            change: wrap("graph.value", Lifecycle::Data, payload),
        });
    }

    // Assign frame_seq and compute checksums.
    let mut seq = base_seq;
    let mut frames = Vec::with_capacity(decomposed.len());
    for d in decomposed {
        seq += 1;
        let mut frame = WALFrame {
            t: WalTag,
            lifecycle: d.lifecycle,
            path: d.path,
            change: d.change,
            frame_seq: seq,
            frame_t_ns: timestamp_ns,
            checksum: String::new(),
            format_version: 1,
        };
        frame.checksum = wal_frame_checksum(&frame)?;
        frames.push(frame);
    }

    Ok((frames, seq))
}

// ── Attach (D170) ──────────────────────────────────────────────────────────

/// Per-tier state managed by the attach wiring.
struct TierState {
    /// The snapshot tier (writes full baselines).
    snapshot_tier: Box<dyn SnapshotStorageTier<GraphCheckpointRecord>>,
    /// Optional WAL tier (writes individual delta frames).
    wal_tier: Option<Box<dyn KvStorageTier<WALFrame<Value>>>>,
    /// WAL key prefix derived from `graph.name`.
    wal_prefix: String,
    /// Monotonic cursor.
    seq: u64,
    /// Flush counter for `compact_every` cadence.
    flush_count: u64,
    /// Configured compact-every cadence (0 = every flush writes full baseline).
    compact_every: u32,
    /// Last snapshot used for diff computation.
    last_snapshot: Option<GraphPersistSnapshot>,
    /// Disposed flag.
    disposed: bool,
}

/// Configuration for a single snapshot+WAL tier pair.
pub struct AttachTierPair {
    /// Snapshot tier for full baselines.
    pub snapshot: Box<dyn SnapshotStorageTier<GraphCheckpointRecord>>,
    /// Optional WAL tier for delta frames. When `None`, every flush
    /// writes a full baseline (no incremental WAL).
    pub wal: Option<Box<dyn KvStorageTier<WALFrame<Value>>>>,
}

/// Filter predicate for [`AttachOptions`].
pub type PathFilter = Box<dyn Fn(&str) -> bool + Send + Sync>;

/// Error callback for [`AttachOptions`].
pub type ErrorCallback = Box<dyn Fn(&StorageError) + Send + Sync>;

/// Options for [`attach_snapshot_storage`].
#[derive(Default)]
pub struct AttachOptions {
    /// Per-path filter. Return `true` to persist changes for this path.
    /// `None` means persist all paths.
    pub filter: Option<PathFilter>,
    /// Error callback invoked when a flush fails.
    pub on_error: Option<ErrorCallback>,
}

/// Handle returned by [`attach_snapshot_storage`]. Dropping this handle
/// unsubscribes the observe sink (RAII disposal).
pub struct StorageHandle {
    /// Shared state; inner `disposed` flag prevents late-fire callbacks.
    state: Arc<Mutex<Vec<TierState>>>,
    /// Graph reference (kept alive so the observe subscription stays valid).
    _graph: Graph,
    /// The observe handle — dropping it unsubscribes all sinks.
    _observe: graphrefly_graph::GraphObserveAllReactive,
}

impl StorageHandle {
    /// Explicitly dispose (equivalent to `Drop`, but callable).
    pub fn dispose(&self) {
        if let Ok(mut states) = self.state.lock() {
            for s in states.iter_mut() {
                s.disposed = true;
            }
        }
    }
}

impl Drop for StorageHandle {
    fn drop(&mut self) {
        self.dispose();
    }
}

/// Wire an observe subscription on `graph` that persists node changes
/// to the provided snapshot+WAL tier pairs.
///
/// # Debounce (D171)
///
/// Timer-based debounce is deferred — all tiers flush synchronously on
/// every qualifying event (`debounce_ms > 0` logs a warning and treats
/// as 0). See `porting-deferred.md` "M4.B tier-level setTimeout-equivalent
/// debounce" entry.
///
/// # `key_of` (D174, closes F8)
///
/// The snapshot tier's backend key is derived from `graph.name` via
/// the checkpoint record's `name` field. Cross-impl `key_of` divergence
/// disappears at this boundary.
pub fn attach_snapshot_storage(
    graph: &Graph,
    pairs: Vec<AttachTierPair>,
    options: AttachOptions,
) -> StorageHandle {
    let graph_name = graph.name();
    let wal_prefix = graph_wal_prefix(&graph_name);

    let mut states = Vec::with_capacity(pairs.len());
    for pair in pairs {
        // Warn on debounce > 0 (D171).
        if let Some(ms) = pair.snapshot.debounce_ms() {
            if ms > 0 {
                tracing::warn!(
                    graph = %graph_name,
                    debounce_ms = ms,
                    "debounce_ms > 0 not yet supported in Rust; treating as 0 (D171)"
                );
            }
        }

        // Bootstrap: enumerate existing WAL frames to find high-water seq.
        let mut high_seq: u64 = 0;
        if let Some(ref wal) = pair.wal {
            if let Ok(keys) = wal.list(&wal_prefix) {
                for key in keys {
                    if let Some(seg) = key.rsplit('/').next() {
                        if let Ok(s) = seg.parse::<u64>() {
                            high_seq = high_seq.max(s);
                        }
                    }
                }
            }
        }

        let compact_every = pair.snapshot.compact_every().unwrap_or(10);

        states.push(TierState {
            snapshot_tier: pair.snapshot,
            wal_tier: pair.wal,
            wal_prefix: wal_prefix.clone(),
            seq: high_seq,
            flush_count: 0,
            compact_every,
            last_snapshot: None,
            disposed: false,
        });
    }

    let shared_states = Arc::new(Mutex::new(states));
    let states_for_sink = shared_states.clone();
    let graph_clone = graph.clone();
    let filter = options.filter;
    let on_error = options.on_error;

    // Wire observe_all_reactive so late-added nodes are also covered.
    let mut observe = graph.observe_all_reactive();
    observe.subscribe(move |path: &str, messages: &[Message]| {
        // Filter: only tiers 3–5 (DATA/RESOLVED, INVALIDATE, COMPLETE/ERROR).
        let dominated_by_tier = messages.iter().any(|m| {
            let t = m.tier();
            (3..6).contains(&t)
        });
        if !dominated_by_tier {
            return;
        }

        // Optional path filter.
        if let Some(ref f) = filter {
            if !f(path) {
                return;
            }
        }

        // Take a snapshot once, shared across all sync tiers.
        let snapshot = graph_clone.snapshot();

        if let Ok(mut states) = states_for_sink.lock() {
            for s in states.iter_mut() {
                if s.disposed {
                    continue;
                }
                if let Err(e) = flush_tier(s, &snapshot) {
                    if let Some(ref cb) = on_error {
                        cb(&e);
                    }
                }
            }
        }
    });

    StorageHandle {
        state: shared_states,
        _graph: graph.clone(),
        _observe: observe,
    }
}

/// Flush a single tier: either full baseline or WAL delta.
fn flush_tier(s: &mut TierState, snapshot: &GraphPersistSnapshot) -> Result<(), StorageError> {
    s.flush_count += 1;

    let write_full = s.wal_tier.is_none()
        || s.last_snapshot.is_none()
        || (s.compact_every > 0 && s.flush_count.is_multiple_of(u64::from(s.compact_every)));

    if write_full {
        write_full_baseline(s, snapshot)?;
    } else {
        write_wal_delta(s, snapshot)?;
    }

    s.last_snapshot = Some(snapshot.clone());
    Ok(())
}

/// Write a full baseline snapshot to the snapshot tier.
fn write_full_baseline(
    s: &mut TierState,
    snapshot: &GraphPersistSnapshot,
) -> Result<(), StorageError> {
    let timestamp_ns = graphrefly_core::wall_clock_ns();
    let record = GraphCheckpointRecord {
        name: snapshot.name.clone(),
        mode: "full".to_owned(),
        snapshot: snapshot.clone(),
        seq: s.seq,
        timestamp_ns,
        format_version: SNAPSHOT_VERSION,
    };

    s.snapshot_tier.save(record)?;
    s.snapshot_tier.flush()?;
    Ok(())
}

/// Write WAL delta frames for the diff between `last_snapshot` and current.
fn write_wal_delta(s: &mut TierState, snapshot: &GraphPersistSnapshot) -> Result<(), StorageError> {
    let last = s
        .last_snapshot
        .as_ref()
        .expect("caller ensures last_snapshot is Some");
    let diff = diff_snapshots(last, snapshot);

    if diff.is_empty() {
        return Ok(());
    }

    let timestamp_ns = graphrefly_core::wall_clock_ns();
    let (frames, next_seq) = decompose_diff_to_frames(&diff, timestamp_ns, s.seq)?;

    if let Some(ref wal) = s.wal_tier {
        for frame in &frames {
            let key = wal_frame_key(&s.wal_prefix, frame.frame_seq);
            wal.save(&key, frame.clone())?;
        }
        wal.flush()?;
    }

    s.seq = next_seq;
    Ok(())
}

// ── Restore (D170) ─────────────────────────────────────────────────────────

/// Torn-write policy for mid-stream checksum failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TornWritePolicy {
    /// Drop the frame and continue (default for WAL tail).
    Skip,
    /// Abort the entire restore.
    Abort,
}

/// Callback for torn-write decisions. Receives the `frame_seq` and
/// reason; returns the desired policy.
pub type OnTornWrite = Box<dyn Fn(u64, &str) -> TornWritePolicy + Send + Sync>;

/// Options for [`restore_snapshot`].
pub struct RestoreOptions<'a> {
    /// Snapshot tier to load the baseline from.
    pub snapshot_tier: &'a dyn SnapshotStorageTier<GraphCheckpointRecord>,
    /// WAL tier to enumerate delta frames from.
    pub wal_tier: &'a dyn KvStorageTier<WALFrame<Value>>,
    /// Optional max `frame_seq` to replay up to. `None` = replay all.
    pub target_seq: Option<u64>,
    /// Torn-write callback. If `None`, defaults: tail = Skip, mid = Abort.
    pub on_torn_write: Option<OnTornWrite>,
}

/// Three-phase WAL replay: baseline load → checksum verify → lifecycle-
/// scoped batch.
///
/// # Phase 1: Baseline
///
/// Loads the `mode:"full"` baseline from the snapshot tier. The snapshot
/// key is derived from `graph.name` (D174).
///
/// # Phase 2: Checksum verification
///
/// Enumerates WAL frames with `frame_seq > baseline.seq`, verifies each
/// frame's SHA-256 checksum, applies torn-write policy on mismatch.
///
/// # Phase 3: Lifecycle-scoped batch replay
///
/// Groups verified frames by lifecycle. Replays in cross-scope order
/// (`Spec → Data → Ownership`). Each lifecycle runs in a `graph.batch()`
/// for atomic partial-restore semantics (Q2).
pub fn restore_snapshot(
    graph: &Graph,
    opts: &RestoreOptions<'_>,
) -> Result<RestoreResult, RestoreError> {
    // Phase 1: Load and apply baseline.
    let baseline = load_baseline(graph, opts)?;
    let baseline_seq = baseline.seq;

    // Phase 1b: Collect WAL frames post-baseline.
    let collected = collect_wal_frames(opts, &baseline.name, baseline_seq)?;

    // Phase 2: Checksum verification.
    let (verified, skipped) = verify_frames(collected, opts.on_torn_write.as_ref())?;

    // Phase 3: Lifecycle-scoped batch replay.
    Ok(replay_by_lifecycle(graph, &verified, baseline_seq, skipped))
}

/// Phase 1: Load baseline from snapshot tier + apply to graph.
fn load_baseline(
    graph: &Graph,
    opts: &RestoreOptions<'_>,
) -> Result<GraphCheckpointRecord, RestoreError> {
    let baseline = opts
        .snapshot_tier
        .load()
        .map_err(|e| RestoreError::PhaseFailed {
            lifecycle: Lifecycle::Spec,
            frame_seq: 0,
            message: format!("baseline load failed: {e}"),
        })?
        .ok_or(RestoreError::BaselineMissing)?;

    if baseline.mode != "full" {
        return Err(RestoreError::BaselineMissing);
    }

    graph
        .restore(&baseline.snapshot)
        .map_err(|e| RestoreError::PhaseFailed {
            lifecycle: Lifecycle::Spec,
            frame_seq: 0,
            message: format!("baseline restore failed: {e}"),
        })?;

    Ok(baseline)
}

/// Phase 1b: Enumerate + filter WAL frames.
fn collect_wal_frames(
    opts: &RestoreOptions<'_>,
    graph_name: &str,
    baseline_seq: u64,
) -> Result<Vec<WALFrame<Value>>, RestoreError> {
    let wal_prefix = graph_wal_prefix(graph_name);
    let keys = opts
        .wal_tier
        .list(&wal_prefix)
        .map_err(|e| RestoreError::PhaseFailed {
            lifecycle: Lifecycle::Spec,
            frame_seq: 0,
            message: format!("WAL frame enumeration failed: {e}"),
        })?;

    let mut collected: Vec<WALFrame<Value>> = Vec::new();
    for key in keys {
        let frame_seq = key
            .rsplit('/')
            .next()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        if frame_seq <= baseline_seq {
            continue;
        }
        if let Some(target) = opts.target_seq {
            if frame_seq > target {
                continue;
            }
        }

        if let Some(frame) = opts
            .wal_tier
            .load(&key)
            .map_err(|e| RestoreError::PhaseFailed {
                lifecycle: Lifecycle::Data,
                frame_seq,
                message: format!("WAL frame load failed: {e}"),
            })?
        {
            collected.push(frame);
        }
    }

    collected.sort_by_key(|f| f.frame_seq);
    Ok(collected)
}

/// Phase 2: Verify checksums, apply torn-write policy.
fn verify_frames(
    collected: Vec<WALFrame<Value>>,
    on_torn_write: Option<&OnTornWrite>,
) -> Result<(Vec<WALFrame<Value>>, u64), RestoreError> {
    let mut verified = Vec::new();
    let mut skipped: u64 = 0;
    let total = collected.len();

    for (i, frame) in collected.into_iter().enumerate() {
        if verify_wal_frame_checksum(&frame).unwrap_or(false) {
            verified.push(frame);
            continue;
        }

        let is_tail = i == total - 1;
        let policy = if let Some(cb) = on_torn_write {
            cb(frame.frame_seq, "checksum-mismatch")
        } else if is_tail {
            TornWritePolicy::Skip
        } else {
            TornWritePolicy::Abort
        };

        match policy {
            TornWritePolicy::Skip => skipped += 1,
            TornWritePolicy::Abort => {
                return Err(RestoreError::TornWriteMidStream {
                    frame_seq: frame.frame_seq,
                    reason: "checksum-mismatch".to_owned(),
                });
            }
        }
    }

    Ok((verified, skipped))
}

/// Phase 3: Group by lifecycle, replay in cross-scope order.
fn replay_by_lifecycle(
    graph: &Graph,
    verified: &[WALFrame<Value>],
    baseline_seq: u64,
    skipped: u64,
) -> RestoreResult {
    let mut grouped: [Vec<WALFrame<Value>>; 3] = [Vec::new(), Vec::new(), Vec::new()];
    for frame in verified {
        for (idx, lifecycle) in REPLAY_ORDER.iter().enumerate() {
            if frame.lifecycle == *lifecycle {
                grouped[idx].push(frame.clone());
                break;
            }
        }
    }

    let mut phases = Vec::new();
    let mut replayed: u64 = 0;
    let mut final_seq: u64 = baseline_seq;

    for (idx, lifecycle) in REPLAY_ORDER.iter().enumerate() {
        let life_frames = &grouped[idx];
        if life_frames.is_empty() {
            continue;
        }
        let frame_count = life_frames.len() as u64;
        let max_seq = life_frames.iter().map(|f| f.frame_seq).max().unwrap_or(0);

        let frames_for_batch = life_frames.clone();
        let graph_for_batch = graph.clone();
        graph.batch(move || {
            for frame in &frames_for_batch {
                apply_wal_frame(&graph_for_batch, frame);
            }
        });

        replayed += frame_count;
        final_seq = final_seq.max(max_seq);
        phases.push(PhaseStat {
            lifecycle: *lifecycle,
            frames: frame_count,
        });
    }

    RestoreResult {
        replayed_frames: replayed,
        skipped_frames: skipped,
        final_seq,
        phases,
    }
}

/// Apply a single WAL frame to a graph. Mirrors TS `applyWalFrame`.
fn apply_wal_frame(graph: &Graph, frame: &WALFrame<Value>) {
    let change = &frame.change.change;
    let kind = change.get("kind").and_then(Value::as_str).unwrap_or("");

    match frame.lifecycle {
        Lifecycle::Spec => match kind {
            "graph.add" => {
                let node_id_str = change.get("nodeId").and_then(Value::as_str).unwrap_or("");
                if node_id_str.is_empty() || graph.try_resolve(node_id_str).is_some() {
                    return; // already present or invalid
                }
                // Only auto-create state nodes (matches TS behavior).
                let slice = change.get("slice");
                let node_type = slice
                    .and_then(|s| s.get("type"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if node_type != "state" {
                    return;
                }
                let initial_value = slice.and_then(|s| s.get("value")).cloned();
                let handle = initial_value.map_or(graphrefly_core::NO_HANDLE, |v| {
                    graph.core().binding_ptr().deserialize_value(v)
                });
                let _ = graph.state(node_id_str, Some(handle));
            }
            "graph.remove" => {
                let node_id_str = change.get("nodeId").and_then(Value::as_str).unwrap_or("");
                if !node_id_str.is_empty() && graph.try_resolve(node_id_str).is_some() {
                    let _ = graph.remove(node_id_str);
                }
            }
            // graph.mount, graph.unmount — deferred (Phase 14.6+)
            _ => {}
        },
        Lifecycle::Data => match kind {
            "node.set" => {
                let path = change.get("path").and_then(Value::as_str).unwrap_or("");
                if let Some(value) = change.get("value") {
                    if !path.is_empty() && graph.try_resolve(path).is_some() {
                        let handle = graph.core().binding_ptr().deserialize_value(value.clone());
                        graph.set(path, handle);
                    }
                }
            }
            "node.invalidate" => {
                let path = change.get("path").and_then(Value::as_str).unwrap_or("");
                if !path.is_empty() {
                    if let Some(id) = graph.try_resolve(path) {
                        graph.invalidate(id);
                    }
                }
            }
            // node.versionBump — deferred (V0 versioning is internal)
            _ => {}
        },
        // Ownership lifecycle — deferred (Phase 13)
        Lifecycle::Ownership => {}
    }
}
