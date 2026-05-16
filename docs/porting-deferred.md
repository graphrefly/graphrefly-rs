---
title: Porting flags & deferred concerns
last_updated: 2026-05-14 (Layer-boundary slice: F24 + D171 + stratify + parity scenarios)
---

# Porting flags & deferred concerns

Running registry of design tensions, perf concerns, and known v1 limitations
that were **surfaced during the port** but **deliberately not fixed yet**.
Companion to [migration-status.md](migration-status.md) — that file tracks
*what landed*; this file tracks *what we know but punted on*.

When picking up a milestone, scan this file for items in scope. When closing a
milestone, move the resolved entries into the milestone's closing section in
`migration-status.md` and remove them here.

---

## Convention

Each entry has:

- **What** — the concrete observation (file:line where applicable)
- **Why deferred** — what would be lost by fixing it now (or what risk fixing it
  introduces) and what triggers a re-look
- **Source** — where it came from (audit, /qa pass, bench data, session doc)

Items are grouped by category, not chronologically. Add new items to the right
group; if a group grows past ~5 items, split it.

---

## Performance — §10 simplifications deferred until evidence-driven

The session doc Part 11 directive: "do not pre-optimize hot paths before
profiling; use criterion benchmarks to identify actual bottlenecks. The §10
analysis identifies *likely* wins — verify with data." Pass 5 of the v0 bench
(SmallVec + ChildEdge dep_idx caching, both regressed) already validated the
principle.

The following items are kept deferred as a single bucket. **Do not pick them
up without a criterion bench reproducer that makes them pay for themselves.**
Add a per-item entry only if a bench surfaces the cost.

| § | What | Current shape | Optimization | Status |
|---|---|---|---|---|
| 10.3 | Diamond resolution bookkeeping | `pending_fires: HashSet<NodeId>` + per-node `involved_this_wave` | `WaveTracker { settled: u64, all_deps_mask: u64 }` bitmask (O(1) is_complete vs O(n)). Pays only on fan-in beyond ~32 deps. | deferred |
| 10.4 | Per-dep indexing | position-based `deps.iter().position(...)` per child propagation | Stable `slot_index` per DepRecord; restructures `tracked: HashSet<usize>` + wave-mask bookkeeping. | dropped — `dep_node_to_index` HashMap is O(1) per lookup, good enough |
| 10.5 | Wave-end notification buffer | `pending_notify: IndexMap<NodeId, Vec<Message>>` (alloc per active node) | Single `BatchFrame { pending: Vec<(NodeId, ...)> }` arena per wave. | deferred |
| 10.6 | Sink iteration | `subscribers: HashMap<SubId, Sink>`, flush iterates under lock | Epoch iteration / snapshot-spread (TS pattern). Rust avoids snapshot cost because sink re-entrance discipline differs (see "Late subscriber + multi-emit-per-wave snapshot gap" below for the related correctness gap). | deferred |
| ~~10.13~~ | ~~First-run gate scan~~ | ~~`dep_records.iter().any(...)`~~ | ~~`received_mask: u64` + `all_deps_mask` bitmask~~ | **RESOLVED** — Slice U (2026-05-12). `received_mask: u64` field on `NodeRecord`; `has_sentinel_deps()` is O(1) bitmask compare. Bit set by `deliver_data_to_consumer`, cleared on deactivation/invalidation/resubscribable reset. |
| ~~—~~ | ~~`pick_next_fire` transitive upstream walk~~ | ~~O(N·V) BFS per pick~~ | ~~Per-node `unresolved_dep_count` counter~~ | **RESOLVED** — Slice U (2026-05-12). `topo_rank: u32` field on `NodeRecord`; `pick_next_fire` uses O(N) min-scan by `topo_rank` (computed at `set_deps` time via BFS from roots). Eliminates per-pick BFS entirely. |

**Source:** Slice A+B audit (2026-05-05); pick_next_fire entry from Slice C-1.5
(2026-05-05). Collapsed into a single bucket 2026-05-07 (Slice F doc cleanup)
per user direction — individual entries were prior-art noise relative to "do
not pre-optimize." §10.13 and pick_next_fire resolved in Slice U (2026-05-12).

### Slice U known limitations (2026-05-12)

- **`topo_rank` not propagated to consumers on `set_deps`.** When a node's deps change via `set_deps`, its own `topo_rank` is recomputed, but downstream consumers' ranks are not cascaded. Correct re-ranking would require a transitive walk of all reachable consumers. Acceptable for v1: `set_deps` is an experimental Phase 13.8 API, rarely called in practice, and the worst case is a temporarily suboptimal pick order (one extra no-op fire that settles on the next wave). Fix if `set_deps` becomes hot-path.
- **`valve` CancellationToken vs TS AbortController parity.** TS valve accepts an `AbortController` which valve can both observe (abort signal from outside) and trigger (valve closes → abort). Rust `CancellationToken` only supports the trigger direction (valve close → cancel). The observe direction (external cancel → close valve) would require spawning a tokio task to select on the token, which adds async machinery for an edge case. Deferred until a concrete use case surfaces.
- **Structure napi packer drops ReactiveIndex secondary keys.** `make_index_intern_fn` in `structures_bindings.rs` flattens `IndexRow<HandleId, HandleId>` to `[primary, value, ...]` — the `secondary: String` field is dropped because it's an internal sort key not surfaced in the TS public API. If a future TS consumer needs secondary key access, the packer callback shape needs widening.
- ~~**Structure napi `maxSize`/`defaultTtl` on Map are scaffolded but untested.**~~ **RESOLVED 2026-05-11 (M5.B) — confirmed 2026-05-13 (Slice W).** TTL/LRU now wired through `ReactiveMapOptions` to the Rust layer (`reactive.rs::default_ttl_ns`, `lru_max_size`, `prune_expired_inner`). Per-call `set_with_ttl` accepted via napi. **Parity-test coverage gap:** only `maxSize` is exercised by `scenarios/structures/reactive-map.test.ts` (line 101); TTL scenario is queued as a follow-up (see migration-status Slice W "Follow-ups queued for next slice"). See F19 entry below for full status.

---

## Profile-surfaced optimizations (2026-05-10) — candidates for a future cleanup batch

User direction (2026-05-10): "I'm sure there are some optimization items in
the deferred we can pick them up later in a batch." The three items below were
surfaced by the post-Q-beyond profile (`samply` + macOS `sample` on
`crates/graphrefly-core/examples/profile_disjoint_fn_fire.rs`, 4M emits across
2 disjoint partitions). They're individually small wins (1-3% each) but
collectively could deliver ~5-7% with much lower engineering risk than
sub-slice 4 (per-partition `nodes`/`children` shards). Group them into a
single "profile-driven cleanup" batch when picked up.

### ~~(1) Eliminate `mach_absolute_time` from the hot path~~ — RESOLVED 2026-05-10 (no action needed)

- **What:** profile shows `mach_absolute_time` at 2.44% self-time on the
  worker thread of the disjoint fn-fire workload.
- **Investigation (2026-05-10):** grep of all `monotonic_ns()` /
  `wall_clock_ns()` / `Instant::now()` / `clock::*` calls in
  `crates/graphrefly-core/src/` found **zero** time calls on the wave-engine
  hot path (commit_emission, queue_notify, fire_regular,
  deliver_data_to_consumer, begin_batch_for, flush_notifications). The only
  two active `monotonic_ns()` sites are in `PauseState::add_lock()` (cold —
  transition to paused) and `PauseState::overflow_diagnostic()` (cold —
  overflow reporting). The ~2% `mach_absolute_time` is from
  **`parking_lot`'s internal adaptive spinning** — it uses `Instant::now()`
  to decide spin-vs-park on contended mutex acquire. This is not addressable
  from graphrefly-core code; the mitigation is reducing lock acquisitions
  per emit (addressed by items (2) and (3) below).
- **Resolution:** no code change. Item closed as "not our code." The profile
  cost will decrease as items (2)/(3) reduce the number of mutex acquires
  per emit.

### ~~(2) Reduce allocation churn in `queue_notify` and `pending_notify`~~ — RESOLVED 2026-05-10

- **What:** profile showed ~3.5% of total time in allocator activity from
  per-batch `Vec<Sink>` clone-snapshot and `Vec<Message>` growth.
- **Resolution (2026-05-10):** converted `PendingBatch.sinks` from
  `Vec<Sink>` to `SmallVec<[Sink; 1]>` (inlines the common
  single-subscriber case) and `PendingBatch.messages` from `Vec<Message>`
  to `SmallVec<[Message; 3]>` (inlines the common DIRTY + DATA + optional
  RESOLVED set). Both changes eliminate heap allocation for the dominant
  hot-loop pattern (single subscriber, ≤3 messages per node per wave).
- **Measured improvement:** combined with item (3), the profile harness
  shows −12.6% wall-clock on disjoint fn-fire (928→811 ns/emit). The
  SmallVec contribution vs the partition cache is not individually isolable
  from the profile harness but the criterion bench delta for state-emit
  (which doesn't benefit from the partition cache as much) shows ~1%
  improvement attributable to reduced allocator churn.

### ~~(3) Cache or short-circuit `begin_batch_for`~~ — RESOLVED 2026-05-10

- **What:** every `Core::emit` invoked `compute_touched_partitions(seed)`
  BFS under state + registry locks, even for repeated emits on the same
  seed where the topology hadn't changed.
- **Resolution (2026-05-10):** per-thread `PARTITION_CACHE` thread-local
  caching `(core_id, seed, epoch) → partitions`. On repeated emits to the
  same seed (the dominant hot-loop pattern), the BFS + its state-lock +
  registry-lock + HashSet + SmallVec allocations are skipped entirely.
  Cache invalidated by registry epoch change (concurrent
  register/union/split); post-acquire epoch recheck validates staleness.
- **Measured improvement:** the dominant contributor to the −12.6%
  wall-clock improvement on disjoint fn-fire. Saves 2 lock acquisitions +
  BFS per emit in the common case (repeated same-seed emits).

### Suggested ordering when batched — N/A (all items resolved)

All three profile-surfaced items have been addressed in the 2026-05-10
profile-driven cleanup batch. Combined measurement: −12.6% wall-clock on
the `profile_disjoint_fn_fire` harness (928 → 811 ns/emit, Apple Silicon,
2 threads × 2M emits on disjoint partitions with ~450 ns calibrated spin
per fn-fire).

---

## v1 dispatcher limitations (intentional, with planned lift points)

### `release_handles` / `release_handle` called lock-held during `reset_for_fresh_lifecycle` (Phase 3 + 3b + 5) — established pattern, expanded by D-α

- **What:** `Core::reset_for_fresh_lifecycle` takes a `&mut CoreState` parameter (caller holds the state mutex). All three release passes — Phase 3 (`old_scratch.release_handles`), the new Phase 3b D-α queue drain, and Phase 5 (`for h in handles_to_release { binding.release_handle(h) }`) — run with the state lock held. Same applies to `Drop for CoreState`'s D-α queue drain. Pre-D-α the same shape existed for Phase 3 + Phase 5 (single old scratch + bounded wave-state vec); D-α expands the surface by adding a queue that can hold N scratches accumulated across non-terminal deactivate cycles (typically O(few) in practice; degenerate workloads could push higher per the `pending_scratch_release` growth-bound note on `CoreState`).
- **Why this is safe at v1:** the `BindingBoundary::release_handle` (and `OperatorScratch::release_handles`) trait contract documents the leaf-operation invariant — implementations MUST NOT re-enter Core during refcount calls. Production bindings (napi-rs, pyo3, wasm-bindgen) honor this. The lock-held pattern is consistent across Phase 3 / 3b / 5 / Drop, so D-α doesn't change the contract — just stretches the existing surface.
- **Why divergent from the broader Phase G pattern:** Phase G (in `Subscription::Drop`) releases handles lock-RELEASED (gather under lock, drop lock, then call release). That's the more conservative shape. `reset_for_fresh_lifecycle` runs lock-HELD by structural necessity — the caller already holds the lock (it's mid-subscribe handshake), and the function can't relinquish it without breaking the broader subscribe state-machine.
- **Lift point:** widen the `BindingBoundary` trait's `release_handle` rustdoc to make the leaf-operation requirement a HARD contract (currently it reads more like an implementation hint). Add a `release_handle_lock_held: bool` capability flag if a future binding genuinely needs Core re-entrance during refcount paths. Until then, defer.
- **Source:** /qa F2 / M1 (2026-05-10 A/B/D/E/F batch review). Pre-existing for Phase 3/5; expanded for Phase 3b + Drop drain via D-α.

### ~~`signal_invalidate` uses unbounded recursion (stack overflow risk on deep mount trees)~~ — RESOLVED 2026-05-12 (Slice 3e/3f)

- **Resolution:** `collect_signal_invalidate_ids` converted from recursive to iterative using an explicit `Vec<Graph>` worklist. Pop a graph, lock briefly, push children to worklist, push own ids to `out`. Mirrors the `destroy()` cascade pattern.
- **Where it landed:** `crates/graphrefly-graph/src/graph.rs` — `collect_signal_invalidate_ids`.
- **Source:** /qa Blind Hunter Major (2026-05-10 A/B/D/E/F batch review).

### ~~M4.B `flush()` pending data loss on encode / write failure~~ — RESOLVED 2026-05-11 (D165)

All three tier `flush()` impls restructured to restore pending state on encode/write failure. `SnapshotStorage::try_flush` returns `Err((T, StorageError))` — caller restores T to `pending`. KV and AppendLog flush loops restore remaining unprocessed entries on any mid-loop error. Two regression tests in `tests/redb_backend.rs` (`f1_snapshot_flush_failure_preserves_pending`, `f1_kv_flush_failure_preserves_pending`) verify the restore path using a `FailWrite` stub backend. Additionally, redb's per-write ACID transactions (D163) provide backend-level durability — a committed write is durable regardless of what happens to the tier's in-process state.

### M4.B `compact_every` modulo semantics — single-add equivalent to TS, batch saves use boundary-crossing

- **What:** /qa F2 surfaced that the pre-fix code used strict `is_multiple_of(N)` (matching TS `writeCount % compactEvery === 0`). For single-save APIs (`SnapshotStorage::save`, `KvStorage::save`) this is fine. For `AppendLogStorage::append_entries(&[batch])` where the batch jumps multiple compact_every boundaries, strict-modulo can miss the trigger entirely. **The fix moved both impls (Rust + TS) to boundary-crossing trigger logic** (`prev / N != new / N`) — a batch that crosses any boundary fires one flush. For single-add the two formulations are semantically equivalent.
- **Why now (not deferred):** the bug only surfaced with batch APIs which were on the table in M4.B (`append_entries`). Fixing it preserves user-visible cadence behavior across the impls.
- **Cross-language coordination:** Rust fix at `crates/graphrefly-storage/src/memory.rs` (all three `*Tier` impls); TS fix at `packages/pure-ts/src/extra/storage/tiers.ts` (all three factories). Both impls verified test-green post-fix.
- **Source:** /qa F2 (D138-followup, 2026-05-10).

### ~~M4.B `AppendLogStorage::flush` concurrent-write race (lift @ M4.D redb)~~ — RESOLVED 2026-05-11 (D163)

redb per-write ACID transactions (D163) serialize concurrent writers by MVCC design — `Database::begin_write` blocks until the previous write transaction commits. Two concurrent `flush()` calls on the same tier or on tiers sharing a backend are serialized at the database level; no per-tier `flush_lock` needed. Memory and file backends remain single-writer-documented (their rustdoc already notes "not thread-safe for concurrent writes"). The structural race is closed for the production backend (redb); non-production backends carry explicit documentation.

### ~~M4.B Snapshot `last_saved_key` is process-local — cross-restart `load()` with custom `key_of` (lift @ M4.E manifest)~~ — STRUCTURALLY CLOSED 2026-05-11 (D174)

- **What:** [crates/graphrefly-storage/src/memory.rs](../crates/graphrefly-storage/src/memory.rs) `SnapshotStorage::last_saved_key` tracks the most recent backend key written. On a fresh process attaching a tier to an existing backend, this field is `None`, so `load()` falls back to `self.name`. If the prior run wrote under a `key_of`-derived key that differs from `tier.name`, `load()` returns `None` even though a baseline exists.
- **Status (M4.E2 close):** D174 structurally closes this. `attach_snapshot_storage` derives the snapshot key deterministically from `graph.name` via `key_of` — no manifest needed. `restore_snapshot` uses the same `key_of` derivation. The tier-level `last_saved_key` gap is masked by the graph-level deterministic keying.
- **Source:** /qa F4 (2026-05-10); closed D174 / M4.E2 (2026-05-11).

### ~~M4.B Snapshot `key_of` default doesn't peek into `T::name` field — cross-impl divergence~~ — STRUCTURALLY CLOSED 2026-05-11 (D174)

- **What:** TS `snapshotStorage` default `key_of` at `packages/pure-ts/src/extra/storage/tiers.ts:346-348` is `(v) => (v as { name?: string }).name ?? opts.name ?? backend.name ?? "snapshot"` — it inspects the snapshot for a `name` field. Rust `snapshot_storage` default at `crates/graphrefly-storage/src/memory.rs:111-115` always uses `tier.name`. Cross-impl: a Rust writer using default `key_of` and `Snap { name: "alpha", value: 1 }` writes under `tier.name`; a TS writer with the same snapshot writes under `"alpha"`. WAL files written by one impl can't be loaded by the other.
- **Status (M4.E2 close):** D174 structurally closes this at the graph-integration layer. `attach_snapshot_storage` always passes `key_of = |snap| snap.name` (derived from `graph.name`), so both impls converge at the attach boundary. The tier-level default `key_of` divergence remains but is irrelevant when using the graph-level API.
- **Source:** /qa F8 (2026-05-10); closed D174 / M4.E2 (2026-05-11).

### ~~Cross-language `format_version` field missing from `WALFrame<T>` — spec §d gap~~ — RESOLVED (Rust) 2026-05-12 (Slice 3e/3f)

- **Resolution (Rust side):** Added `pub format_version: u32` to `WALFrame<T>` with `#[serde(default = "default_format_version")]` (defaults to `1`). Backward-compatible with existing M4.A frames. Excluded from `ChecksumBody` (metadata, not content) — parity fixtures unchanged.
- **TS side:** still missing. When adding to TS, the field MUST also be excluded from the checksum body (`canonicalFrameBody` at `wal.ts:141`) — otherwise cross-language parity breaks. Track in TS `docs/optimizations.md`.
- **Source:** /qa F7 (2026-05-10). Cross-language scope — tracked in TS `docs/optimizations.md` under "Active work items".

### ~~M4.B tier-level setTimeout-equivalent debounce~~ — RESOLVED 2026-05-14 (Layer-boundary slice, D171)

- **What landed:** [`attach_snapshot_storage`](../crates/graphrefly-storage/src/graph_integration.rs) now respects each tier's `debounce_ms`. The observe sink writes via `tier.save()` (which buffers per the `BaseStorageTier` contract) but skips inline `tier.flush()` when `debounce_ms > 0`. Callers drain via new `StorageHandle::flush_all() -> Result<(), StorageError>`.
- **Why this shape (Option B over the originally-deferred Option A "reactive subgraph"):** keeps `graphrefly-storage` sync + binding-agnostic per the canonical no-async-in-storage invariant. The reactive-timer wiring belongs in the layer that already owns the timer primitive (`graphrefly_operators::temporal::interval`, shipped in Slice T). Binding callers wire `interval(debounce_ms) → handle.flush_all()` themselves; the storage crate gains no new dependency on operators.
- **API delta:** `StorageHandle::flush_all()` added (returns the first error encountered, if any; successful tiers still flush). The `debounce_ms > 0` warn-log in `attach_snapshot_storage` removed (no longer relevant). Test added: `attach_respects_snapshot_debounce_ms_buffering` in `tests/graph_integration.rs`.
- **Decision provenance:** D171 was originally deferred in M4.E2 (2026-05-11). Lifted in Layer-boundary code-slice 2026-05-14 alongside the F24 + stratify + scenario-authoring bundle. The Option-A reactive-subgraph approach is documented but not adopted — Option B has lower coupling and the same user-visible behavior.

### M4.C `FileBackend` case-insensitive-filesystem key collision

- **What:** [crates/graphrefly-storage/src/file.rs](../crates/graphrefly-storage/src/file.rs) `encode_key_to_filename` preserves ASCII alphanumeric case (`Foo` encodes to `Foo`, `foo` to `foo`). On case-insensitive filesystems (default macOS HFS+ / APFS, default Windows NTFS) the files `Foo.bin` and `foo.bin` collide — last `write` wins, the second `read` returns the wrong value, `list` reports only one entry. Cross-impl parity is preserved (TS `fileBackend` has the same behavior).
- **Why acceptable at v1:** graphrefly-internal keys (tier names, `<graph>/wal/<seq>` paths) are case-consistent by construction — there's no internal pattern that produces case-divergent siblings. Adversarial user-supplied keys with case-only differences are a deliberate footgun on any filesystem; the inherent constraint is unavoidable without a filesystem-portability adapter.
- **Lift point:** at M4.E `Graph::attach_storage` or a focused follow-on slice, either (a) document the constraint loudly and trust the user; (b) detect the filesystem case-sensitivity at attach time (`File::create_new("Probe-X.bin")` + `File::open("probe-x.bin")` — collision indicates case-insensitive) and reject case-divergent keys with a clear diagnostic; (c) lowercase-fold all keys before encoding (lossy — breaks round-trip for users who genuinely want distinct casings). (a) and (b) are both viable; (c) is not acceptable pre-1.0.
- **Source:** M4.C slice 2026-05-10. Documented inline at `file.rs` `FileBackend` struct doc.

### M4.E1 `from_snapshot` auto-hydration doesn't handle cross-subgraph deps

- **What:** `Graph::from_snapshot` auto-hydration mode resolves deps by name within the same graph's `created` map. If a node in the snapshot depends on a node in a mounted subgraph (cross-mount dep), the dep name won't be found in the local `created` map, and the node will be reported as `UnresolvableDeps`. The TS impl has the same limitation — `from_snapshot` only resolves local deps.
- **Why acceptable at v1:** cross-mount deps are rare in practice (most deps are local to the graph they're declared in). The builder mode of `from_snapshot` handles cross-mount deps naturally because the user wires topology explicitly. Auto-hydration mode is primarily for state-heavy graphs with simple topologies.
- **Lift point:** when cross-mount dep resolution is needed, extend the `created` map to include subgraph nodes with qualified names (e.g. `"subgraph.nodeName"`), or pass a pre-populated dep resolver that can look up nodes across the mount tree.
- **Source:** D168 / M4.E1 slice 2026-05-11.

### M4.E2 ownership lifecycle replay in `apply_wal_frame` (mount/unmount)

- **What:** `apply_wal_frame` in `graph_integration.rs` handles `WalTag::SpecAdd` (graph.add) and `WalTag::SpecRemove` (graph.remove) for state nodes. It does NOT handle ownership lifecycle frames (mount/unmount subgraphs) because the WAL tag vocabulary doesn't include mount/unmount variants yet. Subgraph added/removed entries in `GraphSnapshotDiff` are computed by `diff_snapshots` but not decomposed to WAL frames by `decompose_diff_to_frames`.
- **Why acceptable at v1:** subgraph mount/unmount is a topology change that's best handled at the `from_snapshot` / `restore` level (which already supports recursive subgraphs). WAL-level replay of mount/unmount would require new `WalTag` variants and careful ordering (mount parent before children). Current restore path handles subgraph topology via baseline snapshot restore.
- **Lift point:** when the WAL tag vocabulary is extended to include mount/unmount lifecycle events, add corresponding arms to `apply_wal_frame` and `decompose_diff_to_frames`.
- **Source:** D172 / M4.E2 slice 2026-05-11.

### M4.E2 `attach_snapshot_storage` TEARDOWN tier excluded from persistence

- **What:** The `observe_all_reactive` subscription in `attach_snapshot_storage` filters to tiers 3–5 (`(3..6).contains(&t)`). Tier 6 (TEARDOWN) is excluded. This means a node's teardown event is NOT persisted to the WAL. On restore, torn-down nodes will appear as their last pre-teardown state (COMPLETE or ERROR).
- **Why acceptable at v1:** TEARDOWN is a terminal lifecycle event that triggers cleanup. Restoring a graph to a pre-teardown state is generally the desired behavior — teardown cleanup actions are not idempotent and should not be replayed. The TS implementation has the same behavior.
- **Lift point:** if a use case arises where teardown state needs to persist (e.g., audit log), add tier 6 to the filter and a corresponding `WalTag::Teardown` variant.
- **Source:** M4.E2 slice 2026-05-11.

### M4.E1 snapshot torn-read caveat (concurrent mutation)

- **What:** `Graph::snapshot()` takes state-at-call-time. If nodes are being mutated concurrently (from another thread or re-entrant wave), the snapshot may capture a mix of pre- and post-mutation values. The inner lock protects the namespace walk, but individual `core.cache_of` / `core.is_terminal` calls after the lock drop can observe mid-wave state.
- **Why acceptable at v1:** TS impl has the same semantics — `snapshot()` is a synchronous point-in-time capture. The doc comment explicitly notes: "use `Graph::batch` or `Graph::signal(Pause)` for consistency if needed." No user has requested snapshot-level isolation.
- **Lift point:** if needed, `snapshot()` could acquire the Core lock for the entire walk (snapshot under lock), or use a copy-on-write epoch to freeze state for the duration of serialization. Both add complexity for a scenario that hasn't been reported.
- **Source:** D167 / M4.E1 slice 2026-05-11.

### M4.A canonical-JSON parity caveats (WAL checksum)

- **What:** [crates/graphrefly-storage/src/wal.rs](../crates/graphrefly-storage/src/wal.rs) `canonical_json` routes typed values through `serde_json::to_value` → `serde_json::Map` (BTreeMap-backed by default — sorted iteration) → `serde_json::to_string`. This matches TS `stableJsonString` (recursive key-sort + `JSON.stringify(_, undefined, 0)`) byte-for-byte on the WAL frame schema (ASCII keys, integer numerics, no floats). Two known divergence vectors exist outside that schema:
  - **UTF-16 vs UTF-8 sort:** JS sorts keys by UTF-16 code-unit order; Rust `BTreeMap` sorts by UTF-8 byte order. For ASCII keys these are identical; for keys containing surrogate-pair code points (≥ U+10000) they diverge. The WAL frame's own keys are ASCII, so this can only bite if `path` or `change.structure` carries non-BMP code points — neither is expected for graph identifiers.
  - **Float subnormals:** JS `JSON.stringify` and Rust `serde_json` (via `ryu`) agree on finite f64 in the safe-int range, but may diverge on subnormals / denormalized floats. WAL frames typically carry integer payloads.
- **Why acceptable at v1:** the WAL frame schema is constrained — ASCII keys, integer counters, optional sub-payload that for the V0 envelope is always integer / string. No real consumer surfaces non-ASCII identifiers or float user-payloads at present. The parity-fixture test (`wal::tests::checksum_parity_fixture_minimal_frame`) pins byte-equivalence with TS on the canonical use case.
- **Lift point:** either (a) ban non-ASCII keys + non-finite floats at the WAL encode boundary (loud error on attempt — closes the divergence by construction); OR (b) write a custom canonical-JSON serializer that emits UTF-16-code-unit sort order + IEEE-754-shortest-round-trip number formatting matching JS exactly. (a) is ~10 LOC + clear failure mode; (b) is ~150 LOC with subtle parity drift risk.
- **Source:** D138 / M4.A slice 2026-05-10. Documented inline at `wal.rs` module-doc lines 14-36.

### `DescribeValue::Rendered(Value::Null)` is JSON-indistinguishable from `Option::None` (sentinel cache)

- **What:** [crates/graphrefly-graph/src/describe.rs](../crates/graphrefly-graph/src/describe.rs) `NodeDescribe.value: Option<DescribeValue>` serializes:
  - `None` (sentinel cache, `NO_HANDLE`) → `"value":null`
  - `Some(DescribeValue::Handle(h))` → `"value":<u64>`
  - `Some(DescribeValue::Rendered(Value::Null))` (binding rendered as JSON null) → `"value":null`
- A JSON consumer reading the serialized describe output cannot tell "node has no cache" from "node has cache but binding rendered as null".
- **Why acceptable at v1:** `serde_json::Value::Null` is a legitimate rendering target (for nullable user values); the ambiguity is on the JSON side, not the API side. Rust callers comparing `DescribeValue::Rendered(Value::Null)` vs `None` via `PartialEq` get the correct distinction.
- **Lift point:** either (a) add a top-level `value_mode: "handle" | "rendered"` field on `GraphDescribeOutput` so consumers know which interpretation applies; OR (b) document that bindings should return a non-Null discriminator (e.g. `{"opaque": true}`) for handles that resolve to null user values; OR (c) tag the serialized form (e.g. `{"value": {"rendered": null}}`) — would break JSON shape parity with TS canonical.
- **Source:** /qa m1/m28 (2026-05-10 A/B/D/E/F batch review).

### ~~Fn re-entrance via `BindingBoundary::invoke_fn` and `custom_equals` still forbidden~~ — RESOLVED in Slice A close

`Core::fire_fn` and `Core::commit_emission` were rewritten as
three-phase operations: snapshot inputs under the lock → drop lock →
call binding → reacquire lock → apply result. User fns may now
re-enter `Core::emit` / `pause` / `resume` / `invalidate` / `complete`
/ `error` / `teardown` from inside `invoke_fn` and run a nested wave;
custom equals oracles may re-enter Core during evaluation.

`run_wave` and `drain_and_flush` are now `&self`-only and manage
their own locking. `BatchGuard::drop` and `activate_derived` updated
to match. ~400-500 LOC of churn as estimated.

Verified by `tests/lock_released.rs` (2026-05-05) — 13 regression
tests covering fn re-entrance via emit/pause/resume/invalidate, custom
equals re-entrance, and a concurrent emit during invoke_fn deadlock
test. Closed 2026-05-05.

### ~~Late subscriber + multi-emit-per-wave snapshot gap (D2 — Rust-only dispatcher design)~~ — RESOLVED 2026-05-08 in Slice X4

`PendingPerNode` was reshaped from a single `(sinks, messages)` tuple
to a `SmallVec<[PendingBatch; 1]>` of subscriber-snapshot epochs.
`NodeRecord::subscribers_revision: u64` bumps on every mutation of
`subscribers` (subscribe install, `Subscription::Drop`, handshake-panic
eviction). `Core::queue_notify` consults the revision: if it matches
the open batch's `snapshot_revision` (common case, no mid-wave subscribe
at this node) the message appends to the open batch with no extra
allocation. If the revision advanced, a fresh `PendingBatch` opens with
a current sink snapshot — pre-subscribe batches retain their original
snapshot so earlier emits don't double-deliver via flush AND handshake.

Picked option 1 (re-snapshot every push) over options 2 (walk-and-append,
re-introduces duplicate-Data) and 3 (per-message tracking, heavier with
no win). Revision tracking gates the snapshot allocation on actual
subscriber-set change, keeping the common case allocation-free.

`flush_notifications` iterates batches in arrival order within each
phase × node loop; per-batch sink snapshots and per-batch message
filtering are independent. Refcount-release walks (`flush_notifications`,
`Drop for CoreState`, `BatchGuard::drop` panic-discard) all use the new
`PendingPerNode::iter_messages` flat-walk helper.

Verified by `crates/graphrefly-core/tests/sink_snapshot.rs` (4 tests):
multi-emit + late-subscribe (canonical D2), single-emit + late-subscribe
(preserves original snapshot fix's target), multi-emit + mid-wave
unsubscribe, multi-emit + no sub change (common case unchanged).
Cross-impl no-regression case in
`packages/parity-tests/scenarios/core/sink-snapshot.test.ts` (1 test ×
2 impls). Closed 2026-05-08.

The canonical D2 case (multi-emit + late-subscribe in one wave) is
**not** parity-tested cross-impl — the bug is structurally
unreachable in `pure-ts` (per-emit snapshot at delivery time, not
per-wave at flush time), and the napi `Impl` interface does not
expose `batch(closure)` for JS to interleave subscribe between emits
inside one Rust wave (`parking_lot::ReentrantMutex` thread-affinity
contract on `BatchGuard` doesn't span `spawn_blocking` boundaries).
Cargo regression tests are the source of truth for the canonical case.

### ~~Cross-thread emit blocks until in-flight wave completes (D3)~~ — RESOLVED 2026-05-09 in Slice Y1+Y2 (Phases B–L)

**Closure summary (2026-05-09):** the per-partition `wave_owner` substrate landed in Phase E, split-eager partition splitting in Phase F, criterion-bench validation in Phase J, comprehensive cross-partition tests in Phase H, TLA+ verification of all 5 Q6 invariants in Phase I (`docs/research/wave_protocol_partitioned.tla` + `_MC` — 133,611 states explored, 0 invariant violations), CLAUDE.md invariant 3 wording lifted to current state in Phase K, and this strikethrough lands in Phase L. **Result:** disjoint-partition fn-fire-heavy waves see 1.82× speedup at 2 threads (2.16× separation vs same-partition) per the Phase J calibrated bench. **Caveat:** tight-emit Regime A still serializes on the Core-global state mutex — wide-spectrum parallelism awaits the "Per-partition state-shard refactor (Q2 + Q3 + Q-beyond)" entry below. Decisions D089–D099 logged in `docs/rust-port-decisions.md`. Closing migration-status section: `Slice Y1+Y2 CLOSED (2026-05-08 → 2026-05-09)`.

The original entry body is preserved below for archival reference.

### Cross-thread emit blocks until in-flight wave completes (D3 — original entry, UPDATED 2026-05-07, RESOLVED 2026-05-09)

- **What:** with the wave-owner re-entrant mutex (Slice A close /qa,
  M1), cross-thread `Core::emit` calls block at
  `wave_owner.lock_arc()` until the in-flight wave's drain + flush +
  sink-fire completes. Same-thread re-entry passes through.
- **Why this design:** TS is single-threaded; PY is GIL-serialized.
  Rust is the first impl with parallel access. The wave-owner mutex
  is the chosen serialization point. Without it, the lock-released
  drain (Slice A close) would let a concurrent `emit` absorb into
  the in-flight wave's `pending_notify` and return BEFORE its
  subscribers fire — breaking the user-facing contract that `emit`
  returning means subscribers have observed.
- **Trade-offs accepted:**
  - Concurrent threads cannot drive parallel waves on the same Core.
  - A blocked thread waits for the owner thread's fn fires + sink
    fires to complete. If a fn or sink is slow, blockers wait.
- **Per-subgraph parallelism status (UPDATED 2026-05-07):** the original
  entry said "M2's mount/unmount work surfaces" per-subgraph
  parallelism. M2 closed 2026-05-06, but mount/unmount landed without
  per-subgraph mutex granularity — Core is still single-mutex per
  CLAUDE.md Rust invariant 3 ("planned"), not per Slice E+ implementation.
  **Real defer condition:** consumer pressure for cross-thread parallel
  waves on the same Core. Until then, the wave-owner mutex is the
  correct shape and not a "v1 limitation" — it's the v1 design.
- **Design LOCKED 2026-05-08 (Slice X4 follow-up):**
  [`SESSION-rust-port-d3-per-subgraph-parallelism.md`](https://github.com/graphrefly/graphrefly-ts/blob/main/archive/docs/SESSION-rust-port-d3-per-subgraph-parallelism.md)
  in graphrefly-ts. Top-level: Option B (single Core, per-partition
  state). **Q1 = (c-uf split-eager)** — union-find connectivity-based
  with reachability walk on edge removal, mirroring graphrefly-py's
  [`subgraph_locks.py`](https://github.com/graphrefly/graphrefly-py/blob/main/src/graphrefly/core/subgraph_locks.py)
  but adding split where py is monotonic-merge. Decisions logged at
  `docs/rust-port-decisions.md` D085 (split-into-design-doc + Option
  B recommendation), D086 (Q1 lock + union-find + split-eager).
- **Slice X5 LANDED 2026-05-08: substrate-only intermediate state.**
  `crates/graphrefly-core/src/subgraph.rs` (new module, ~370 LOC)
  ships `SubgraphId`, `SubgraphRegistry` (union-find with
  union-by-rank + path compression + cleanup re-rooting), and
  `SubgraphLockBox` (per-partition `wave_owner: Arc<ReentrantMutex<()>>`
  allocated but not yet authoritative). `Core::register` calls
  `registry.ensure_registered(new_node)` + `union_nodes(new_node, dep)`
  for each dep; `Core::set_deps` calls `union_nodes(n, added_dep)`
  for each new edge and `on_edge_removed(n, removed_dep)` (no-op in
  X5; Y1 split site) for each removed edge. Public `Core::partition_count()`
  + `Core::partition_of(node)` accessors expose connectivity for
  bench / inspection. **490 cargo tests pass (was 469 pre-X5; +10
  subgraph unit tests + 7 integration tests + 4 D2 sink-snapshot from
  Slice X4); 142 parity tests pass.** `cargo clippy --all-targets
  -D warnings` clean; `cargo fmt --check` clean for files touched;
  `#![forbid(unsafe_code)]` preserved.
- **Y1 carry-forward:** wave engine migration to per-partition
  `wave_owner`. Every `begin_batch()` / `run_wave()` call site +
  `Core::subscribe`'s `wave_owner.lock_arc()` + `BatchGuard`'s lock
  retention need to be reshaped around `lock_for(node)` with
  retry-validate semantics for held-Arc-vs-current-root divergence
  on union. Plus split-eager (reachability walk + state migration on
  edge removal) and `currently_firing` extension to reject mid-wave
  `set_deps` triggering migration. **Estimated ~2500 LOC**, touches
  ~80 call sites in `node.rs` + `batch.rs`. Substantial enough to
  warrant its own dedicated batch — splitting from X5 closes a
  meaningful intermediate state (substrate green) without risking
  the 490-cargo-test invariant during a multi-day wave-engine churn.
- **Y1 entry-condition #1 (Slice X5 /qa P12, 2026-05-08):
  `Core::register` and `Core::set_deps` register-then-union ordering
  vs lock acquisition.** The X5 substrate acquires the registry mutex
  AFTER dropping the state lock at both
  [`crates/graphrefly-core/src/node.rs`](https://github.com/graphrefly/graphrefly-rs/blob/main/crates/graphrefly-core/src/node.rs)
  `Core::register` (~line 1976) and `Core::set_deps` (~line 3866).
  This creates a window where a concurrent thread observes the new
  node in `s.nodes` / new edges in `s.children`, but the registry
  hasn't yet unioned the partition. Today benign (`Core::partition_of`
  is debug/inspection-only); under Y1 the wave engine consumes
  `lock_for(node)`, and the window means `lock_for` could resolve to
  a partition that's been topologically unioned in `s.children` but
  not yet in `registry`. **Y1 must either** (a) move registry mutation
  back inside the state-lock scope (acceptable — registry mutex is
  uncontended in X5 substrate), OR (b) document the eventual-consistency
  window and add lock-validation retry inside `Core::lock_for`.
  Recommend (a) for simplicity.
- **Y1 entry-condition #2 (Slice X5 /qa P13, 2026-05-08): mid-wave
  `set_deps` registry mutation must extend the `currently_firing`
  thread-local guard.** Q3=(a-strict) per the D3 design lock REJECTS
  mid-wave migrations. X5 monotonic-merge unions mid-wave silently
  ([`crates/graphrefly-core/src/node.rs`](https://github.com/graphrefly/graphrefly-rs/blob/main/crates/graphrefly-core/src/node.rs)
  `set_deps` at ~line 3865) — benign because the registry isn't
  authoritative yet. Under Y1 the wave engine holds partition A's
  `wave_owner`, fires `invoke_fn`, and a recursive `Core::set_deps`
  triggering union into partition B would mutate the lock-acquisition
  graph mid-wave. The Slice F A6 `currently_firing: Vec<NodeId>`
  thread-local must be extended: if `union_nodes` (or `split` under
  split-eager) would migrate any node currently on the firing stack,
  reject with a `SetDepsError::PartitionMigrationDuringFire` variant
  (or extend the existing `ReentrantOnFiringNode`).
- **Y1 entry-condition #3 (Slice X5 /qa noted, 2026-05-08): X4
  parity-test forcing function for D2 canonical case.** The
  cross-impl D2 parity scenario at
  [`packages/parity-tests/scenarios/core/sink-snapshot.test.ts`](https://github.com/graphrefly/graphrefly-ts/blob/main/packages/parity-tests/scenarios/core/sink-snapshot.test.ts)
  only covers the no-regression case; the canonical D2 (multi-emit +
  late-subscribe in one wave) is cargo-only because pure-TS dispatcher
  snapshots subscribers per delivery and the case is unreachable in
  pure-TS today. **If the pure-TS dispatcher is ever refactored** to
  per-wave-at-flush-time snapshotting (mirroring Rust), the parity
  scenario must be widened to cover the canonical case to lock the
  cross-impl contract.
- **Slice Y1+Y2 IN-FLIGHT 2026-05-08 → 2026-05-09 (Phase B-E
  landed; Phase F-L carry forward).** User chose Option 3 (single
  all-in-one D3 closure). Decisions D089–D094 logged. **Phase C
  lifted Y1 entry-condition #1** (registry mutation inside the
  state-lock scope, option (a) per D090; lock order `state →
  registry`). **Phase D lifted Y1 entry-condition #2** (new
  `SetDepsError::PartitionMigrationDuringFire` variant + check, option
  (b) per D091; Phase F split-eager widens the check). **Phase E
  delivered the canonical parallelism win**: legacy `Core::wave_owner`
  removed; wave engine routes through per-partition
  `partition_wave_owner_lock_arc(seed)` with retry-validate;
  `BatchGuard::_wave_guards: SmallVec<[WaveOwnerGuard; 4]>` holds
  acquired-in-ascending-`SubgraphId`-order partition guards;
  closure-form `Core::batch` acquires every current partition (Q7
  serialization point); `Core::begin_batch_for(seed)` walks
  `s.children` + `meta_companions` to compute touched partitions
  upfront; `Core::run_wave_for(seed, op)` migrated all 7 callers in
  `node.rs` (subscribe activation, emit, emit_terminal, teardown,
  invalidate, resume default-mode drain, set_deps push-on-subscribe);
  legacy `run_wave` removed. The acceptance test
  `concurrent_emit_blocks_until_in_flight_wave_completes` (asserting
  whole-Core serialization) was replaced with the inverse
  `concurrent_emit_on_disjoint_partitions_runs_truly_parallel` (parallel
  across disjoint partitions) + companion
  `concurrent_emit_on_same_partition_serializes` (same-partition still
  serializes). Both pass — the parallelism property is observable.
  **496 cargo + 142 parity green** (was 491 + 142; +4 P12+P13
  regressions, +1 net from inversion+companion). **`Core::wave_owner`
  field, legacy whole-Core ReentrantMutex retired** — `WeakCore`
  updated, `Core::new` simplified.

  **Phase E re-scope**: original plan included Q2 (cross_partition
  Mutex split — relocate `pending_pause_overflow` /
  `pending_auto_resolve` / `wave_cache_snapshots` /
  `deferred_handle_releases` out of `CoreState`) and Q3 (per-partition
  `tier3_emitted_this_wave`). Both are independent optimizations —
  the parallelism win landed without them. Deferred to Phase H or
  later (subject to bench evidence). Cross-thread waves on disjoint
  partitions still briefly contend on the single state mutex during
  pending_notify queueing / clear_wave_state, but no longer on a
  Core-global wave_owner. Phase J bench will quantify the residual
  contention.

  **Phase F (2026-05-09) delivered split-eager** per the locked
  Q1 = (c-uf split-eager) design. New `bfs_undirected_dep_graph`
  helper in `node.rs` walks `s.children` + `dep_records` from a
  seed (with optional one-edge skip for the P13 connectivity
  pre-check). New `SubgraphRegistry::split_partition` method
  resets every component node to a singleton, re-unions via
  post-removal dep edges, and assigns the original `Arc<SubgraphLockBox>`
  to the keep-side root while allocating a fresh box for the
  orphan-side root. `Core::set_deps`'s registry block now invokes
  this on each removed dep when the post-removal BFS shows
  disconnection. P13 widened to reject mid-fire splits via the
  same BFS with `skip_edge = Some((removed_dep, n))`. **498 cargo
  tests pass** (+2 net from Phase F's new split-eager scenarios).

  **Phase G (cleanup_node activation) DEFERRED** — requires a
  NodeRecord removal lifecycle (currently `terminate_node` only
  marks the node terminal; NodeRecords persist for the life of
  `CoreState`). No leak vector observed in practice; each
  terminal-only partition retains a single `Arc<ReentrantMutex<()>>`
  (negligible overhead) and registry HashMap entries drop together
  with `CoreState`. Lifts when graph-layer `unmount` (or a
  binding-side equivalent) surfaces a load-bearing need for explicit
  node removal. `cleanup_node` remains `#[allow(dead_code)]`-gated
  with an explanatory rustdoc.

  **Phase J (criterion bench, 2026-05-09) — quantified parallelism;
  earlier "OBSERVABLE" claim retracted as regime-dependent.** New
  bench [`crates/graphrefly-core/benches/per_subgraph_parallelism.rs`](https://github.com/graphrefly/graphrefly-rs/blob/main/crates/graphrefly-core/benches/per_subgraph_parallelism.rs)
  runs 5 + 3 scenarios spanning two regimes:
  - **Tight state-emit (no fn fire):** 2t-disjoint = 7.11 ms vs
    serial = 5.35 ms (8000 emits) — **0.75× = 33% SLOWER.**
    4t-disjoint = 14.84 ms vs serial 16K = 10.48 ms — **0.71× =
    42% SLOWER.** Per-partition `wave_owner` invisible because
    state mutex is held throughout `commit_emission` +
    `drain_and_flush` and is Core-global; cross-thread state-mutex
    contention dominates.
  - **Fn-fire-heavy (~4µs CPU per fire, lock-released invoke_fn):**
    2t-disjoint = 1.35 ms vs serial = 1.66 ms (2000 emits) —
    **1.23× speedup.** 2t-same-partition = 2.66 ms — **1.97×
    SLOWER than disjoint** (canonical Y1 property: same-partition
    serializes via partition `wave_owner`; disjoint-partition runs
    concurrent fn fires lock-released).

  **Implication for Phase E re-scope (Q2 / Q3):** the wave-engine
  migration is structurally correct; the user-facing parallelism
  gain is gated on workload mix. To extend the gain to tight-emit
  workloads, the Q2 cross-partition mutex split (move
  `pending_pause_overflow` / `pending_auto_resolve` /
  `wave_cache_snapshots` / `deferred_handle_releases` out of
  `CoreState`) and Q3 per-partition `tier3_emitted_this_wave`
  must land — they shrink the state-mutex critical section so
  cross-thread state acquisition doesn't serialize disjoint-
  partition emits. **Q2/Q3 is the next concrete step toward
  wide-spectrum parallelism.** Bench evidence justifies the work
  but the workload-specific benefit ceiling depends on real
  consumer mix.

  **Test note:** the existing
  `concurrent_emit_on_disjoint_partitions_runs_truly_parallel`
  acceptance test pins a NON-BLOCKING property (thread B's emit
  returns BEFORE tx.send releases thread A's blocked fn — QA-fix
  group 2 replaced the earlier 2s timing window with a deterministic
  atomic event-ordering check). The companion
  `concurrent_emit_on_same_partition_serializes` similarly uses
  entry/exit atomic flags instead of timing-window checks. Both
  pin correctness properties (non-blocking / serialization), not
  wall-clock-speedup properties — Phase J bench is the
  speedup-measurement source.

  **CORRECTION (2026-05-15, D047) — the disjoint-regime numbers above
  were inflated by an `in_tick` drain-skip bug.** `in_tick` (the wave
  ownership / drain gate) was Core-global. In concurrent emits on
  disjoint partitions of one Core, thread B observed thread A's
  `in_tick = true`, wrongly treated its independent disjoint wave as
  nested, and **no-op'd its `BatchGuard::drop` — B's wave never
  drained.** The Phase J "fn-fire-heavy 2t-disjoint = 1.35 ms, 1.23×
  speedup" measured a path that *skipped thread B's entire drain*. After
  the per-(Core, thread) re-keying (D047), a clean back-to-back A/B (low
  machine load) shows:
  - `fnfire_parallel_2t_disjoint` ≈ **4.63 ms** (was ≈1.09 ms buggy) —
    i.e. **≈2.8× SLOWER than serial**, not 1.23× faster.
  - `parallel_4t_disjoint` (state-emit) **+27%**; serial ±5%;
    same-partition / fn-fire-serial ≈ neutral.

  **The "Q2/Q3 is the next concrete step toward wide-spectrum
  parallelism" implication and all disjoint-regime speedup evidence in
  this Phase J block must be RE-VALIDATED against post-D047 numbers
  before being relied on. The per-subgraph-parallelism value proposition
  in the disjoint regime is materially weaker than this section claims**
  (flagged for spec-owner re-validation; not silently rewritten).
  **Follow-up C — CLOSED as investigated (2026-05-15), no code change.**
  A cheap-correct-drain fast-path is **not viable**: (1) every disjoint
  bench subscribes a sink, so `pending_notify`-drain + `fire_deferred`
  delivery is *mandatory correctness work* (the exact work the bug
  skipped) — an "all-empty ⇒ skip" predicate never fires on the benched
  workload; (2) `commit_emission` takes a wave-cache rollback snapshot
  whenever `in_tick && cache != NO_HANDLE` (≈ every real state emit), so
  the predicate is unreachable even with no subscribers; (3) the residual
  cost is the **Core-global `lock_state()`** in the post-drain block
  (`clear_wave_state` touches `CoreState`; `drain_deferred` needs
  `&mut s`) — under N-thread disjoint contention that single mutex per
  emit is the bottleneck, i.e. the **already-deferred Q2/Q3 per-partition
  state-shard refactor is the only real recovery lever, NOT a drain
  fast-path**. The fn-fire disjoint cost is irreducible (fn fires must
  drain). `wave_protocol_partitioned_MC` is unaffected (`in_tick` is a
  Rust drain-gate not in the TLA+ model; the modeled per-partition
  `wave_owner` relation is unchanged).

  **Open strategic question (escalated, NOT decided here):** the Phase J
  disjoint-parallel speedup was substantially a bug artifact, so the
  Rust port's *parallelism* selling point in this regime is weak/absent
  until Q2/Q3. This says nothing about correct-Rust-vs-`pure-ts`
  throughput/memory/FFI — **that cross-impl comparison is UNMEASURED**
  (the `packages/parity-tests` `rustImpl` arm only activates once
  `@graphrefly/native` publishes). Before the Rust port's performance
  value proposition is relied on, a proper **correct-Rust vs
  `@graphrefly/pure-ts`** benchmark is required; the "4×" figure is
  correct-vs-buggy *intra-Rust*, not Rust-vs-TS, and must not be read as
  the cross-impl gap. See `docs/rust-port-decisions.md` D047.

### ~~Cross-partition acquire-during-fire deadlock (Phase H+) — bundled into next batch (2026-05-09)~~ — FULLY CLOSED 2026-05-10 (LIMITED 2026-05-09 + STRICT D115 2026-05-10)

**Closure summary (limited variant, 2026-05-09):** Phase H+ option (d)
panicking variant landed for the **non-producer wave-active surface**.
The check fires when `held_partitions` is non-empty (this thread is
inside an active wave that's already acquired ≥1 partition) AND
`in_producer_build == 0` (we're not inside a producer-pattern operator's
build/project closure or sink callback) AND the new partition id is
`<= max-already-held`. Caught surfaces include:

- **User-fn fire** — derived / dynamic / state `invoke_fn` re-entering
  Core on a smaller-id partition mid-fire.
- **Sink callbacks** — non-producer sinks (test recorders, application
  observers) re-entering Core on a smaller-id partition while an outer
  wave's wave_owner is held (closed by /qa N1(a) widening).
- **Subscribe handshake / Subscription::Drop cleanup** — same shape:
  any nested re-entry path goes through the same gate.

Verified by `crates/graphrefly-core/tests/per_subgraph_parallelism.rs::user_fn_cross_partition_emit_during_fire_panics_with_h_plus_diagnostic`.
The TLA+ spec at `docs/research/wave_protocol_partitioned.tla`
verifies the safe protocol clean (207,305 states at MaxOps=7); the
impl now matches the spec for the non-producer surface (modulo the
producer-pattern carry-forward below).

**Producer-pattern operator carve-out (intentional):** `ProducerCtx::subscribe_to`
in `graphrefly-operators::producer` wraps every producer-internal
sink with a `ProducerSinkGuard` that bumps `IN_PRODUCER_BUILD` for
the duration of the sink call. This preserves the existing
producer-pattern operator architecture (`zip` / `concat` / `race` /
`take_until` / `switch_map` / `exhaust_map` / `concat_map` /
`merge_map` all do cross-partition `Core::subscribe` + `Core::emit`
from inside their inner-source sink callbacks) against the widened
H+ gate. Refactoring the operators to defer their sink-time inner
subscribes to wave-end is the broader Phase H+ STRICT variant scope
(option `b` defer-to-post-flush, ~1500+ LOC) — see "STRICT variant"
section below.

Verified by `crates/graphrefly-core/tests/per_subgraph_parallelism.rs::user_fn_cross_partition_emit_during_fire_panics_with_h_plus_diagnostic`.
The 3 existing `lock_released.rs` tests that exercised the
descending-cross-partition pattern were refactored via
`add_meta_companion` to declare cross-partition reachability upfront
so the wave acquires both partitions ascending; the inner re-entry
becomes a re-entrant acquire on a held partition (no panic).

**~~Carried forward — STRICT variant (still deferred)~~** — **LANDED
2026-05-10 (D115).** The typed-error variant (option `d` strict) was
implemented as the Phase H+ STRICT slice. Key changes:

1. `check_and_acquire` returns `Result<(), PartitionOrderViolation>` —
   no longer panics for producer paths.
2. Per-Core `deferred_producer_ops: Mutex<Vec<DeferredProducerOp>>` queue
   drained after wave_guards release in `BatchGuard::drop`.
3. `try_subscribe` / `emit_or_defer` / `complete_or_defer` / `error_or_defer`
   methods added — operators use these instead of raw emit/subscribe.
4. `IN_PRODUCER_BUILD` thread-local removed entirely.
5. `ProducerSinkGuard` wrapper removed; `FiringGuard::is_producer_build` removed.
6. All operator sink closures migrated to `*_or_defer` variants.

Internal-only change: public `subscribe`/`emit`/`complete`/`error`
signatures unchanged (they delegate to `try_*` and panic on violation
for non-producer paths — behavior unchanged). Binding-side error
mapping (napi-rs / pyo3 / wasm) NOT needed since public API didn't change.

Test coverage: `producer_cross_partition_emit_defers_and_succeeds`,
`producer_cross_partition_subscribe_defers_and_succeeds`,
`deferred_emit_ordering_preserved` (3 new tests). 505 cargo total.

The TLA+ spec at `docs/research/wave_protocol_partitioned.tla` remains
the correctness authority — the impl now matches the spec for the FULL
surface (both non-producer AND producer paths enforce ascending order;
the deferred-queue retries at wave-end when no partitions are held).

The original entry body is preserved below for archival reference.

### Cross-partition acquire-during-fire deadlock (Phase H+) — original entry, bundled into next batch (2026-05-09; LIMITED VARIANT landed same day)

- **What (generalized framing):** a thread holding partition `H`
  (max-id `m`) initiates a NEW partition acquire (via any path that
  eventually calls `partition_wave_owner_lock_arc`) targeting a
  partition `r` with `r < m`. Q7 ascending-order is enforced WITHIN
  a single `compute_touched_partitions`-driven batch acquire, but
  NOT ACROSS successive `begin_batch_for` calls on the same thread.
  The lower-id-after-higher-id acquire breaks ascending order
  globally for that thread; under cross-thread contention, AB/BA
  deadlock can form.
- **Concrete trigger surfaces (each is a manifestation of the same
  protocol gap):**
  1. **Producer-pattern subscribe-during-fire (the original framing).**
     A Producer-pattern node `P` in partition X executes its fn-fire
     (lock-released `invoke_fn`). The fn calls `Core::subscribe(source, sink)`
     where `source ∈ Y`. Subscribe acquires Y's `wave_owner` even
     though X is already held; if `Y < X`, ascending order is broken.
  2. **Lifecycle re-entry on a different partition mid-fire.** From
     inside `invoke_fn` on a node in X, the user fn re-enters
     `Core::complete` / `Core::error` / `Core::teardown` /
     `Core::invalidate` on a node in Y. Each goes through
     `run_wave_for(node)` → `begin_batch_for(node)` →
     `partition_wave_owner_lock_arc`. Same protocol gap.
  3. **`Subscription::Drop` from inside a fire** (Q5 scope item in
     the session-doc) when the cleanup walks upstream into a partition
     `< max-already-held`. Q5 acknowledged drop-cascade ascending-
     acquisition; the gap appears when the drop happens INSIDE another
     wave's fire (not just standalone drop).
- **Risk:** classic AB/BA deadlock. Thread A holds X, attempts to
  acquire Y (where Y < X). Concurrently thread B holds Y, attempts
  to acquire X (legitimate batch acquire `{X, Y}` ascending). Cycle:
  A waits on Y (held by B); B waits on X (held by A).
- **Why deferred:** the Producer-pattern surface is uncommon in v1
  use (mostly higher-order operators that subscribe to upstream
  sources, and those tend to defer the subscribe to sink-time for
  unrelated reasons). Lifecycle-during-fire is rare and already
  flagged as a re-entrance hazard. The fix is structural and shouldn't
  block D3 closure. v1 ships with a documented hazard rather than a
  structural fix.
- **TLA+ status (Phase I, 2026-05-09):**
  [`docs/research/wave_protocol_partitioned.tla`](https://github.com/graphrefly/graphrefly-ts/blob/main/docs/research/wave_protocol_partitioned.tla)
  REQUIRES the protective ascending-order guard on every acquire
  (encoded as `\A r \in newTargets : \A h \in threadHolds[t] : r > h`
  on `BeginBatchFor`, `BeginPending`, and
  `BeginSubscriptionDropCleanup`) for `NoWaitForCycle` to verify.
  When the guard is removed, the model checker produces the AB/BA
  counterexample within 5 actions. **The verified spec describes
  the SAFE protocol; the impl ships with the documented gap.**
- **Lift options:** the fix when surfaced is structural — either
  (a) widen the outer wave's `BatchGuard` to include all partitions
  the fn might re-enter (impossible without prescience over user
  binding closures); (b) defer the inner acquire to a post-flush
  callback queue (apply nested `subscribe` / `complete` / `error` /
  `teardown` at wave end against the outer drain); (c) document
  that re-entering Core on a DIFFERENT partition from inside fire
  is unsafe under cross-thread contention and require the user to
  schedule the operation outside the wave; or (d) detect the
  ascending-order violation at acquire time and reject with a
  typed error (caller chooses sequencing). Option (b) is the
  cleanest user-facing surface but adds wave-end queue machinery;
  option (d) is the smallest-blast-radius fix and matches the
  Q3=(a-strict) rejection precedent.
- **LOC re-estimate (post-/qa, 2026-05-09):** the original "~200–500
  LOC" estimate was scope-light. Realistic sizing per option:
  - **Option (d) panicking variant (~300 LOC):** thread-local
    `held_partitions: SmallVec<[SubgraphId; 4]>` (mirrors
    `currently_firing`), check at every `partition_wave_owner_lock_arc`
    site comparing new id against max held, panic on violation,
    drop-side bookkeeping. No public-API change. Smallest
    blast radius; UX trade-off is that the violation surfaces as a
    panic rather than a typed error.
  - **Option (d) typed-error variant (~700–900 LOC + public API
    break):** same thread-local + check, but every public `Core`
    method that may eventually call `partition_wave_owner_lock_arc`
    grows a `PartitionOrderViolation` error variant
    (subscribe / emit / complete / error / teardown / invalidate /
    set_deps push-on-subscribe — ≥7 public methods, each with
    binding-side and graph-layer call sites that need the new
    error plumbed through). Public API break — all binding wrappers
    (napi-rs / pyo3 / wasm-bindgen) need matching error mapping.
  - **Option (b) defer-to-post-flush (~1500+ LOC):** wave-end
    callback queue infrastructure shares machinery with parts of
    Q-beyond's per-partition state-shard work; sequence H+ option (b)
    AFTER Q-beyond if this option is chosen.
  - **Option (c) document-only (~50 LOC):** rustdoc warnings on
    every public method that calls `partition_wave_owner_lock_arc`
    + a `Subscription::Drop` panic-on-mid-fire-cross-partition
    debug-only check. Does NOT fix the hazard, just makes it
    discoverable. Lowest cost, lowest safety.
  - **Recommended path:** option (d) panicking variant first
    (~300 LOC, gives concrete safety property at low cost); raise
    to typed-error variant if user feedback requests it. Sequence
    independently of Q2+Q3+Q-beyond — H+ panicking option (d) is
    orthogonal to the state-shard layout and can land before or
    after.
- **Acceptance for the lift:** the `producer_subscribe_to_other_partition_during_fire_deadlock_hazard`
  test in [`crates/graphrefly-core/tests/per_subgraph_parallelism.rs`](https://github.com/graphrefly/graphrefly-rs/blob/main/crates/graphrefly-core/tests/per_subgraph_parallelism.rs)
  un-`#[ignore]`d with the body matching the chosen fix (b/c/d).
  Plus loom-checked tests for each of the three trigger surfaces
  above (subscribe / lifecycle re-entry / drop-during-fire).
- **Source:** QA-finding #6 (Edge Case Hunter, 2026-05-09) on
  Slice E + F + Phase J QA pass. Generalized framing 2026-05-09 from
  the Phase I TLA+ counterexample, which showed the deadlock arises
  from same-thread successive-acquire ordering, not specifically
  from the Producer pattern. Cross-references Q5 scope item in
  [`SESSION-rust-port-d3-per-subgraph-parallelism.md`](https://github.com/graphrefly/graphrefly-ts/blob/main/archive/docs/SESSION-rust-port-d3-per-subgraph-parallelism.md)
  — Q5 covered standalone `Subscription::Drop` (lock-released
  drop with per-upstream-partition acquisition, no deadlock); the
  generalized "any acquire-during-fire on a different partition"
  hazard was not explicitly covered.

### Phase I TLA+ modeling gaps (D1–D5, surfaced 2026-05-09 /qa)

The Phase I TLA+ harness (`docs/research/wave_protocol_partitioned*` in
graphrefly-ts) verifies the safe-protocol cross-partition lock-acquisition
properties under bounded model-checking. The /qa pass surfaced five
modeling gaps that are NOT bugs in the spec or impl, but documented
limitations of the abstraction. Each is a candidate for follow-up
spec enrichment if a future investigation needs the missing coverage.

- **D1 — `three_disjoint_partitions_emit_returns_concurrently_while_one_blocked`
  doesn't pin "T2 and T3 hold their wave_owners simultaneously".** The
  test verifies the negative property (T2/T3 don't block on T1's held
  P_1 wave_owner) but does NOT pin the positive ("T2 and T3 can
  hold their respective wave_owners at the same instant"). To pin
  simultaneous-hold would require a multi-channel orchestration test
  (~50+ LOC) where each thread signals "I'm inside the held window"
  and the test verifies overlap. **Source:** Blind Hunter #4. **Why
  deferred:** the test's primary property (per-partition lock
  acquisition isn't blocked by held disjoint-partition lock) is
  what the slice claims; simultaneous-hold is a stronger property
  not strictly necessary for D3 closure.

- **D2 — TLA+ `Union` / `Split` guard "no thread holds either
  partition" is more restrictive than the real impl.** The real
  `Core::set_deps` (`crates/graphrefly-core/src/node.rs:3954+`) only
  rejects when a CURRENTLY-FIRING node would migrate; it does NOT
  check whether some thread holds the partition's wave_owner. The
  spec abstracts the retry-validate loop in `partition_wave_owner_lock_arc`
  (registry-epoch-bump retry on lost race) by requiring no holds at
  all when Union/Split fires. **Source:** Edge Case Hunter #6 + Blind
  Hunter #7. **Lift:** add a separate `UnionDuringHeld` /
  `SplitDuringHeld` action in the spec that triggers retry-validate
  (epoch bump → drop guards → retry until epoch matches). Estimated
  ~80 LOC of TLA+. Worth doing if a future bug in the retry-validate
  path needs coverage.

- **D3 — `ReleaseAll` precondition `currentlyFiring[t] = {}` doesn't
  model panic-discard release.** `BatchGuard::drop` on a panicked
  wave (`crates/graphrefly-core/src/batch.rs:2509`) discards
  pending_fires and clears `in_tick` WITHOUT draining; the firing
  stack may or may not be empty depending on where the panic
  happened. Spec's `ReleaseAll` cannot fire when `currentlyFiring[t] # {}`,
  so panic-discard mid-fire is not exercised. **Source:** Edge Case
  Hunter #10. **Lift:** add `ReleasePanic(t)` action that releases
  guards + clears `currentlyFiring[t]` atomically (modeling
  panic-discard collapses all stack frames). ~30 LOC TLA+. Verify
  `MidFireMigrationRejected` still holds across this action — if
  not, the panic path needs a hard look.

- **D4 — `BeginBatchFor` atomic-acquire abstraction doesn't model
  partial-acquire-then-park.** The real `partition_wave_owner_lock_arc`
  loop acquires partitions one-by-one in a sequence; if mid-loop
  another thread mutates topology, the epoch-bump triggers retry. The
  spec abstracts this as one atomic action. Edge cases under topology
  churn during acquire (e.g., A acquires X, then between A's
  `compute_touched_partitions` of X+Y and A's actual `lock_arc(Y)`,
  B has unioned X-Y → A's snapshot is stale) are NOT directly
  modeled. **Source:** Blind Hunter #8 + Edge Case Hunter (analysis
  block). **Lift:** would require splitting `BeginBatchFor` into a
  state-machine of partial-acquire steps; substantial ~150 LOC of
  TLA+. Defer unless a real bug surfaces.

- **D5 — `TouchedFrom` CHOOSE-based fixed-point produces opaque
  counterexample traces.** Mathematically correct (least-set fixed
  point) but if a future bug surfaces, the trace shows CHOOSE
  selecting an opaque set. **Source:** Edge Case Hunter #8. **Lift:**
  replace with recursive bounded-fuel operator (~30 LOC TLA+); only
  prioritize if a future bug investigation actually needs to read
  MC traces.

### ~~Per-partition state-shard refactor (Q2 + Q3 + Q-beyond)~~ — Q-beyond CLOSED 2026-05-10 with bench-driven scope reduction; per-partition `nodes`/`children` shards (sub-slice 4) DEFERRED evidence-gated

**Closure summary (Q-beyond, 2026-05-10):** the original Q-beyond plan was "split CoreState into per-partition `SubgraphShard`s for everything." User pushback ("we have created a lot of locks, I'm not sure if they are hurting more on the performance or gaining more on the parallelism") triggered a first-principles audit + bench-driven decision (D108). The bench harness `crates/graphrefly-core/benches/lock_strategy.rs` (7 scenarios × 4-6 sync primitives) revealed: (a) uncontended same-thread mutex acquire is identical to thread_local borrow (~14 ns/op — parking_lot's adaptive spin makes the "mutex hop is slow" intuition wrong); (b) the real cost is cross-thread cache-line bouncing on the lock state (S3: shared mutex 35.9 ns/op vs per-partition mutex / thread_local 13.0-13.4 ns/op); (c) DashMap loses single-thread by 1.5-2× (S5); (d) RwLock is 82% slower than Mutex for read-heavy 2-thread (S5); (e) crossbeam-channel (Tokio-style) is 2× slower than mutex single-thread (S6).

**Architecture decision (D108):** hybrid — per-thread `WaveState` thread_local for wave-scoped state + per-partition `wave_owner` ReentrantMutex for cross-thread serialization (unchanged). NO DashMap, NO RwLock, NO Tokio. NO per-partition `nodes`/`children` shards (sub-slice 4 dropped — see below).

**12 wave-scoped fields migrated CoreState → per-thread WaveState** across sub-slices 1-3 (2026-05-09 → 2026-05-10):
- Sub-slice 1: `deferred_handle_releases`, `wave_cache_snapshots`, `pending_auto_resolve`, `pending_pause_overflow` (the 4 ex-CrossPartitionState fields). `Core::cross_partition`, `WeakCore::cross_partition`, `lock_cross_partition()`, `CrossPartitionState` struct ALL eliminated.
- Sub-slice 2: `pending_fires`, `pending_notify`.
- Sub-slice 3: `currently_firing`, `in_tick`, `deferred_flush_jobs`, `deferred_cleanup_hooks`, `pending_wipes`, `invalidate_hooks_fired_this_wave`.

**Sub-slice 4 (per-partition `nodes`/`children` shards) DROPPED per D110.** Subagent that started sub-slice 4 surfaced a structural blocker (`compute_touched_partitions` walks `s.children` to know which partitions to lock — chicken-and-egg with sharded children) AND the Phase J bench against the prior baseline showed sub-slices 1-3 alone delivered the perf goal. See "Per-partition `nodes`/`children` shards (Q-beyond Sub-slice 4) — DEFERRED evidence-gated" entry below for the carry-forward.

**Phase J bench post-Q-beyond (2026-05-10) vs prior Q3+Q2 baseline:** 2t-disjoint state-emit −17.9%, 2t-same-partition state-emit −26.8%, serial 4N −15.5%, fn-fire serial −20.9%, fn-fire 2t-disjoint −4.7%, fn-fire 2t-same −16.8%. **The Q3+Q2 regression was fully recovered + exceeded by the per-thread architecture alone.** Disjoint-vs-same separation 1.84× (was 2.11× — parallelism property preserved; ratio compresses because both regimes got faster).

**502 cargo + 142 parity green; 0 failed; 2 ignored.** clippy + fmt clean. `#![forbid(unsafe_code)]` preserved. Decisions D108–D112 logged in `docs/rust-port-decisions.md`. Closing migration-status section: "Current state (2026-05-10)".

**Original Q-beyond entry preserved below for the architecture audit trail.**

---

### Per-partition `nodes`/`children` shards (Q-beyond Sub-slice 4) — DEFERRED evidence-gated 2026-05-10

- **What:** split `CoreState`'s `nodes: HashMap<NodeId, NodeRecord>` and `children: HashMap<NodeId, HashSet<NodeId>>` per-partition onto `SubgraphLockBox.shard: parking_lot::Mutex<SubgraphShard>`. Cross-partition cascades acquire multiple shards in ascending `SubgraphId` order (Q7 pattern). `set_deps` split path acquires `wave_owner` of the affected partition before mutating to serialize against in-flight cross-thread waves on that partition (graphrefly-py-style serialization; same-thread re-entry passes through).

- **Why deferred:** TWO closure events. (i) D110 (2026-05-10) initially dropped sub-slice 4 because Phase J bench post-sub-slices-1-3 showed the wave-scoped migration alone delivered the perf goal (state-emit 2t-disjoint −17.9%, fn-fire serial −20.9%, etc. vs prior Q3+Q2 baseline). (ii) D113 (2026-05-10) re-explored sub-slice 4 with a more rigorous bench (S8 in `crates/graphrefly-core/benches/lock_strategy.rs`) and DEFERRED for the second time after HONEST bench data + engineering-blocker analysis. Combined evidence: sub-slice 4 would deliver ~9% additional gain on disjoint fn-fire IF the engineering risks are managed cleanly, but the engineering complexity is genuinely high.

- **HONEST bench reframe (D113, 2026-05-10):** the original S8 Variant B comparison was misleading — Variant B eliminated the state mutex entirely (only shard.lock acquired), but a structural-only sub-slice 4 keeps state.lock() because state still holds children, ID counters, topology_sinks, config. The `2t_disjoint_HONEST_state_plus_shard` variant added 2026-05-10 holds BOTH state.lock() AND shard.lock() per access (faithful to a structural migration) and shows **2.50 ms vs current 2.74 ms = ~9% improvement** (not the 19% the misleading variant suggested). Real dispatcher gain is likely smaller still — the bench abstracts away other sources of state mutex contention (`s.children` walks, queue_notify, deliver_data_to_consumer, etc.). Realistic estimate: 4-9% on disjoint fn-fire workloads.

- **Engineering blockers identified by subagent analysis (2026-05-10):**
  1. **Re-entrancy hazard.** parking_lot::Mutex is non-reentrant. Cascade-style code (`terminate_node`, `set_deps`, `invalidate`, `fire_op_*`) has patterns that hold parent NodeRecord borrow across child access. Post-migration, parent and child are SAME partition (union-find) → SAME shard → deadlock. Each cascade site needs careful audit + restructure (drop borrow before re-acquire, or use multi-shard helper).
  2. **`clear_wave_state` per-wave shard walks.** Today walks `self.nodes.values_mut()` once. Post-migration: lock every touched shard, walk. Per-wave overhead grows with partition count. Resolvable by narrowing to BatchGuard's touched-sids set (add `BatchGuard::touched_sids()` accessor).
  3. **`walk_undirected_dep_graph` is a free function on `&CoreState`.** Used in `set_deps` for P13 widening + post-removal split detection. Reads `s.nodes[cur].dep_records`. Post-migration: each visited node requires a shard lock for dep_records read. Resolvable but cascades signature changes.
  4. **`set_deps` split-migration is correctness-tricky.** Orphan-side NodeRecords must move shards atomically with `split_partition`. Wave_owner-acquire-on-split (graphrefly-py-style) is the right fix but is a behavior change vs today (set_deps was non-blocking against cross-thread waves) and needs at least one new test exercising mid-wave cross-thread set_deps with concurrent emit.

- **Lift trigger (D113-tightened):** evidence-gated. Re-engage IF AND ONLY IF:
  - A profiling trace on a realistic disjoint-partition workload identifies `parking_lot::Mutex::lock` on `Core::state` as a top-3 contributor (>20% of dispatcher time), OR
  - A user / parity-test report of measurable wall-clock degradation traceable to state-mutex contention on `nodes` reads, OR
  - A criterion bench scenario where state-emit Regime A shows poor scaling at N≥4 threads on disjoint partitions where ≥80% of dispatcher time is in `parking_lot::Mutex::lock`.

  The goal of tightening: prevent re-engaging based on speculative bench data alone (the sub-slice-4 exploration was triggered by a misleading bench projection; evidence-gated lift trigger prevents that recurring).

- **Resolutions for the structural blocker** (when work resumes):
  - **`compute_touched_partitions` recursion:** RESOLVED by minimum-viable scope — keep `children` on CoreState (don't migrate it to shards). `compute_touched_partitions` walks `s.children` from CoreState (no shard lookup needed).
  - **`terminate_node` cross-partition cascade:** pre-compute touched partitions via `compute_touched_partitions` (already done by BatchGuard upfront), then acquire all touched shards in ascending sid order via `with_shards_for(&[node_ids], |shards| ...)` helper. Same-partition parent+child accessed via the same `MutexGuard<SubgraphShard>` (no re-entry).
  - **`set_deps` split-migration race:** `partition_wave_owner_lock_arc(removed_dep)` before `split_partition`; same-thread re-entry passes through, cross-thread blocks until the wave drains.
  - **`clear_wave_state`:** add `BatchGuard::touched_sids() -> &[SubgraphId]` accessor; iterate only touched partitions.

- **Estimated scope (D113-revised):** ~2000-3000 LOC across `node.rs` + `batch.rs` + `subgraph.rs`. Lower than D110's original ~3000-5000 estimate because minimum-viable scope (nodes-only, leave children) sidesteps several blockers. ~100+ access sites for `nodes` + ~40 indirect via `require_node`/`require_node_mut` + lock-discipline restructuring at every callsite + new SubgraphShard struct + helpers + split-migration race fix + new tests for the behavior change.

- **Suggested approach (when work resumes):** option A from the agent's pre-halt report — multi-commit staging:
  1. Add `SubgraphShard` substrate (just `nodes` field; not children), helpers (`with_node`, `with_node_mut`, `with_shards_for`), `BatchGuard::touched_sids` accessor.
  2. Migrate `Core::register` to insert into the partition's shard.
  3. Migrate read-mostly sites first (`require_node`, lookups in `subscribe`, etc.), drop the `s.nodes` reads that were temporary mirrors.
  4. Migrate write sites (cascade walks, fire_regular Phase 3, etc.) — this is where re-entrancy hazards bite; audit each cascade site individually.
  5. Wire the set_deps wave_owner-acquire-on-split fix + add the new test.
  6. Update `Drop for CoreState` to walk shards.
  7. Bench: confirm ≥5% improvement on `fnfire_parallel_2t_disjoint` per `crates/graphrefly-core/benches/per_subgraph_parallelism.rs`. If <5%, REVERT — the engineering risk doesn't justify the gain.

  **NOTE (2026-05-15, D047): this ≥5% gate's baseline was bug-inflated.**
  The `fnfire_parallel_2t_disjoint` figure this step compares against
  (≈1.1–1.35 ms) was an artifact of the `in_tick` drain-skip bug — thread
  B's wave never drained (see the Phase J **CORRECTION** above). The
  honest post-D047 baseline is ≈4.6 ms. Re-base this gate against the
  post-D047 number before running the Sub-slice 4 shard refactor; the
  old "≥5% vs ≈1.1 ms" target is **void**.

- **Source:** Q-beyond batch close (2026-05-10) D110 first drop; D113 (2026-05-10) re-exploration + HONEST bench + final defer. Subagent transcripts in conversation history surfaced the structural blocker analysis.

---

### Per-partition state-shard refactor (Q2 + Q3 + Q-beyond) — Q2 + Slice G tier3 placement (D1 patch) + BTreeMap swap LANDED 2026-05-09; Q-beyond fully CLOSED 2026-05-10 (original entry below preserved for archive)

**Closure summary (Q3 v1 → D1 patch + Q2, 2026-05-09):** Q3 v1 placed
`tier3_emitted_this_wave` per-partition on `SubgraphLockBox::state:
parking_lot::Mutex<SubgraphState>`. The QA pass surfaced D1 (mid-wave
cross-thread `set_deps` partition-split desync → potential R1.3.3.a
violation when X migrates from a held partition to a fresh orphan-side
box mid-wave with empty `tier3_emitted_this_wave`). User picked D1 (b)
trace+fix; the trace confirmed the gap is reachable via thread B's
set_deps when thread A is mid-wave but between fn fires
(`currently_firing.is_empty()` → P13 short-circuits). **D1 fix landed
the same day:** moved Slice G tier3 tracking to a per-thread
`thread_local! { static TIER3_EMITTED_THIS_WAVE: RefCell<AHashSet<NodeId>> }`
in `crate::batch`. Wave scope = thread (cross-thread emits BLOCK on
the partition wave_owner so they always land in the OTHER thread's
wave context with the OTHER thread's thread-local). Cleared at the
outermost `BatchGuard` drop on both success + panic-discard paths,
plus a defensive wave-start clear at outermost owning entry against
cargo's thread-reuse propagating stale entries from a panicked-mid-wave
prior test. Q3 v1 substrate fully removed (`SubgraphState` struct +
`state` field on `SubgraphLockBox` + `WaveOwnerGuard.box_` field +
`Core::partition_box_of` helper). Q2 (cross_partition mutex split —
`Core::cross_partition: Arc<parking_lot::Mutex<CrossPartitionState>>`
holding the 4 wave-aggregation fields previously in CoreState) and the
BTreeMap → SmallVec swap on `held_partitions::HELD` landed alongside.
~700 LOC across `node.rs` / `batch.rs` / `subgraph.rs` / `Cargo.toml`.
**502 cargo tests + 142 parity green; 0 failed; 2 ignored** post-/qa.
clippy + fmt clean. `#![forbid(unsafe_code)]` preserved. See
[`migration-status.md`](migration-status.md) "Current state (2026-05-09)"
for the per-sub-slice + per-/qa-finding breakdown.

**Phase J bench post-Q3+Q2 (criterion, 2026-05-09):** ~25% regression
on fn-fire-disjoint (5.41 ms vs 4.32 ms pre-Q2/Q3); 21% regression on
fn-fire-same-partition; ~10-15% on Regime A. Disjoint-vs-same
separation 2.11× (was 2.18× — parallelism property preserved). The
regression matches the porting-deferred entry's framing: Q2/Q3 in
isolation are pattern-prep with bench cost; Q-beyond is the
perf-positive change. The pbox-cache optimization in `commit_emission`
recovered ~7% of the fn-fire regression (registry-mutex acquire
amortized to once per emission); further wins are gated on Q-beyond.

**What carries forward — Q-beyond:**

- **What:** split `CoreState` into per-partition
  `SubgraphShard`s holding `nodes` / `children` / `pending_notify` /
  `pending_fires` / wave bookkeeping. Reshape every wave-engine site
  to `shards[partition].lock()`. Cross-partition cascades acquire
  multiple shards in ascending `SubgraphId` order (same Q7 pattern as
  `wave_owner`). The shape that realizes wide-spectrum parallelism for
  tight state-emit loops (not just fn-fire-heavy workloads), and
  recovers + exceeds the Q3+Q2 bench regression by removing the global
  state mutex bottleneck entirely.
- **Estimated scope:** ~2000–3000 LOC across `node.rs` + `batch.rs`.
  Touches every read/write site (~150+), `BatchGuard`, drain, flush,
  sink fire, `Drop for CoreState`, `WeakCore`. The Q3+Q2
  prerequisites are now in place: per-partition state pattern via
  `SubgraphLockBox::state`, cross-partition aggregation pattern via
  `Core::cross_partition`. Q-beyond extends both.
- **Plan:** land in per-field sub-slices (move one CoreState field
  per-partition at a time) with cargo test green at each boundary.
  Per-partition shards likely take the form
  `SubgraphLockBox::shard: parking_lot::Mutex<SubgraphShard>` (sibling
  to `state` and `wave_owner`). Lock-discipline:
  `state-residual → cross_partition → shard[i] → ... → partition_state`.

**Original entry preserved below for the bench-evidence baseline,
sequencing rationale, and risks discussion.**

---

- **What (original framing):** unified follow-up batch covering session-doc Q2
  (cross_partition mutex split), Q3 (per-partition
  `tier3_emitted_this_wave`), and Q-beyond (split `CoreState` into
  per-partition `SubgraphShard`s holding `nodes` / `children` /
  `pending_notify` / `pending_fires` / wave bookkeeping). The full
  shape that realizes wide-spectrum parallelism for tight state-emit
  loops, not just fn-fire-heavy workloads.

- **Why this is one batch (not three):** Q2 alone trims ~4 fields
  out of ~15 in the state critical section — bench impact 0–5% in
  Regime A. Q3 alone is similar (one `AHashSet` reduction). Both
  are *prerequisite shape* for Q-beyond, which is the actual
  perf-positive change: per-partition `nodes` / `children` /
  `pending_notify` / `pending_fires` shards mean two threads on
  disjoint partitions hold disjoint mutexes (no algorithmic
  contention) and disjoint memory regions (no L1/L2 cache-line
  bouncing on the shared atomic). Doing Q2 / Q3 in isolation is
  busy-work; bundled with Q-beyond they're the natural shape-
  preparation prefix.

- **Bench evidence (the trigger):**
  [`crates/graphrefly-core/benches/per_subgraph_parallelism.rs`](https://github.com/graphrefly/graphrefly-rs/blob/main/crates/graphrefly-core/benches/per_subgraph_parallelism.rs)
  Phase J results (2026-05-09):
  | Regime | Serial | 2t-disjoint | 2t-same-partition | Notes |
  |---|---|---|---|---|
  | Tight state-emit (8K emits) | 5.35 ms | 7.11 ms (0.75×) | 7.37 ms (0.73×) | Per-partition `wave_owner` invisible — `state` mutex (cache-line) is the bottleneck |
  | Fn-fire-heavy (2K emits) | 1.66 ms | **1.35 ms (1.23×)** | 2.66 ms (0.62×) | `state` released around lock-released `invoke_fn`; **disjoint vs same = 1.97× separation** — Y1 property visible |

  Hypothesis for the post-batch numbers: 2t-disjoint state-emit
  approaches `serial / 2` (modulo coordination overhead) — i.e.
  ~1.7–1.9× speedup at 2 threads. 4t-disjoint approaches
  `serial / 4` on a 4-core machine. Same-partition stays at 1.0× /
  baseline (correct serialization preserved by per-partition
  `wave_owner`).

- **Estimated scope:** ~2000–3000 LOC across `node.rs` + `batch.rs`.
  Touches `CoreState` declaration, `Core::new`, every read/write
  site (~150+), `BatchGuard`, drain, flush, sink fire,
  `Drop for CoreState`, `WeakCore`, plus a `cross_partition`
  mutex for the 4 Q2 fields and per-`SubgraphLockBox`
  `state: Mutex<SubgraphState>` for Q3. Sequencing within the
  batch:
  1. Q3 first — smallest. Add `SubgraphState`, move `tier3`
     per-partition. Establishes the per-partition-data pattern.
  2. Q2 — split `cross_partition` mutex out, move 4 wave-scoped
     fields. Establishes the cross-partition aggregation pattern.
  3. Q-beyond — split `CoreState` into per-partition `SubgraphShard`s.
     Reshape every wave-engine site to `shards[partition].lock()`.
     Cross-partition cascades acquire multiple shards in ascending
     `SubgraphId` order (same Q7 pattern as `wave_owner`).
  4. Bench re-run — quantify the win. Update bench expectations.

- **Risks:** breaking the 498-test invariant during a multi-day
  refactor; cross-partition cascade-shard acquisition has subtle
  lock-order requirements; `BatchGuard` reshape interacts with the
  retry-validate loop. Plan to land in sub-slices (Q3, then Q2,
  then Q-beyond per-site) with cargo test green at each boundary.

- **Path-to-win:**
  1. **D3 closed 2026-05-09** via Phase H + I + K + L (correctness
     tests, TLA+ extension, CLAUDE.md update, closing docs).
     v1 shipped with the documented regime-dependent parallelism
     caveat.
  2. **This batch + Phase H+ are the planned next-batch follow-up**
     (post-1.0 if release happens before; or Slice Z+ within the
     pre-1.0 sequencer). User direction 2026-05-09: "put that
     Q2 + Q3 + Q-beyond refactor as the next batch after H/I/K/L";
     amended same-day to ALSO bundle Phase H+ (cross-partition
     acquire-during-fire deadlock fix — see entry above) into the
     same next batch. The two are independent slices that can be
     sequenced either way: H+ option (d) (detect-and-reject) is
     small (~200–500 LOC) and orthogonal to the state-shard layout;
     H+ option (b) (defer-acquire-to-post-flush) shares wave-end
     queue machinery with parts of Q-beyond — pick the order based
     on which H+ option is chosen.
  3. **Update CLAUDE.md Rust invariant 3** when this batch lands —
     the current wording (post-Phase-K) acknowledges the
     regime-dependent partial win; lift to "wide-spectrum" only
     when Regime A also shows true parallelism (Q-beyond) AND the
     Phase H+ ascending-order gap closes (so the safe protocol
     verified by `wave_protocol_partitioned` matches what the
     impl enforces).

- **Source:** Phase J bench evidence (2026-05-09). User direction
  2026-05-09 to bundle Q2 + Q3 + Q-beyond as one follow-up batch
  rather than landing Q2 / Q3 in isolation.
- **Y2 carry-forward:** TLA+ extension of `wave_protocol_rewire`
  covering partition lock-ordering + cross-partition deadlock-freedom
  + union/split discipline; multi-thread parallel-emit criterion
  bench validating sub-linear wall-clock scaling vs serialized;
  CLAUDE.md Rust invariant 3 wording update from "planned" to current.
  Mandatory before D3 fully closes per session-doc Q6/Q8 scope items.
- **Verified by:** `concurrent_emit_blocks_until_in_flight_wave_completes`
  in `tests/lock_released.rs`.
- **Source:** Slice A close /qa Blind Hunter + Edge Case Hunter findings
  (2026-05-05); user decision Q2=(b) wave-owner mutex; reframed
  2026-05-07 — "M2 unblocks this" was incorrect; per-subgraph mutex
  needs its own design slice.

### ~~Set_deps from inside firing node's fn corrupts Dynamic `tracked` indices (D1)~~ — RESOLVED 2026-05-07 (Slice F A6)

`set_deps(N, ...)` from inside `N`'s own `invoke_fn` now returns `SetDepsError::ReentrantOnFiringNode` via the thread-local "currently firing" stack (`CoreState::currently_firing` + `FiringGuard` RAII at `crates/graphrefly-core/src/batch.rs:99`). The corrupt-tracked-indices scenario is unreachable. Verified by `tests/slice_f_corrections.rs::a6_set_deps_from_firing_fn_rejected_with_reentrant_error`. Originally believed deferred at Slice A close (2026-05-05); fix actually shipped in Slice F (2026-05-07).

### Original D1 entry (kept for archive)

- **What:** lock-released `invoke_fn` (Slice A close, M1) means a user
  fn for a Dynamic node `n` can re-enter `Core::set_deps(n, &new_deps)`
  during its own fire. Phase 1 of `fire_fn` snapshotted dep_handles
  BEFORE invoke_fn. The fn returns `FnResult::Data { tracked: Some([2,
  3]) }` — those indices refer to the dep ordering the fn was invoked
  with. `set_deps` rewrote the deps to a new ordering. Phase 3 stores
  the returned `tracked` into `rec.tracked` against the NEW dep
  ordering — index 2/3 now points at different nodes.
- **Why deferred:** narrow scenario (recursive `set_deps` from the
  firing node's own fn). The existing "Mid-fn rewire re-entrancy
  hazard" Phase 13.8 carry-forward acknowledges related concerns. Real
  fix: reject `set_deps(n, ...)` with `SetDepsError::ReentrantOnFiringNode`
  via a thread-local "currently firing" stack, OR document that
  `tracked` indices are tied to the dep ordering the fn was invoked
  with and verify alignment in phase 3.
- **Source:** Slice A close /qa Edge Case Hunter finding (2026-05-05).

### ~~Per-tier handshake panic on tier-N leaves sink registered (D4)~~ — RESOLVED in Slice F A7 (2026-05-07)

The per-tier handshake fire in `Core::subscribe` is wrapped in
`catch_unwind`. On panic, the orphaned sink is removed from
`subscribers` (via the already-allocated `sub_id`) before the unwind
resumes, so subsequent waves' `flush_notifications` cannot re-fire the
panicking sink. Verified by
`tests/slice_f_corrections.rs::a7_handshake_panic_removes_sink_and_does_not_re_fire_on_next_wave`.
Slice F A7 shipped with the catch_unwind 2026-05-07; this entry was
preserved in the active backlog by oversight and struck through during
Slice X4 (2026-05-08) as a documentation-only cleanup.

### `commit_emission` cache-race documentation (D5 — adjunct to P3)

- **What:** P3 fixed the refcount-leak side of the cache-race window
  (phase 3 re-reads current cache and releases the actual displaced
  handle, not the snapshot). But `is_data` was computed in phase 2
  against `(old_handle, new_handle)`. If a same-thread nested
  `commit_emission` advanced the cache between phase 1 and phase 3,
  the equals decision is stale relative to the actual displaced cache.
- **Behavior:** for Identity equals, this means the user's emit may
  redundantly produce `Data(new_handle)` when `Resolved` would have
  sufficed (subscribers observe one extra `[Dirty, Data(new)]` in
  the rare race). Refcount math is correct; subscribers see a benign
  no-op duplicate. For Custom equals oracles racing the same-node
  commit, the equals decision is undefined — oracle was given
  `(old, new)` but the actual semantic question is `(current, new)`.
- **Why deferred:** the only sound fix is to re-evaluate `is_data` in
  phase 3 against the current cache, which requires another lock-
  released equals call (chicken-and-egg cycle for Custom). Scope:
  document the limitation; reject if it surfaces as a real consumer
  pain point.
- **Source:** Slice A close /qa Blind Hunter + Edge Case Hunter findings
  (2026-05-05).

### CI hardening — pin TLA fetch to release SHA (post-1.0)

- **What:** `.github/workflows/ci.yml`'s `tlc` job fetches MC harnesses
  from `graphrefly-ts/main` via raw GitHub URLs. Today this picks up
  spec amendments immediately, which is the desired tracking shape
  for pre-1.0. Once graphrefly-ts cuts versioned releases, the
  `TLA_REF` env var should pin to a release SHA / tag instead of
  `main` for supply-chain auditability.
- **Source:** Slice A close /qa Blind Hunter finding (2026-05-05).

### ~~Subscribe-time handshake fires lock-held; re-entrance from handshake panics~~ — RESOLVED 2026-05-07 (Slice E rework, D045)

`Core::subscribe` now acquires `wave_owner.lock_arc()` first
(re-entrant), takes the state lock briefly to install the sink +
snapshot handshake state, drops the state lock, then fires the
per-tier handshake LOCK-RELEASED. Sink callbacks may re-enter Core
freely (`emit` / `complete` / `error` / nested `subscribe`); same-
thread re-entry passes through `wave_owner` transparently. Cross-
thread emits block on `wave_owner` until the subscribe scope ends,
preserving R1.3.5.a happens-after ordering.

`IN_HANDSHAKE_FIRE` thread-local + `HandshakeFireGuard` struct
removed; `lock_state()` no longer asserts. Verified by
`handshake_sink_can_reenter_core_emit_on_other_node` in
`tests/lock_discipline.rs` (the previous panic-expecting test
inverted: now expects re-entrant emit to succeed).

The Slice E rework was prompted by user pushback on accumulating v1
limitations. The cached-inner subscribe pattern that previously
panicked (`switch_map(outer, |n| state(Some(n*10)))`) now works
transparently. ~50 LOC change, reuses existing `wave_owner`
infrastructure from Slice A close /qa Q2 — no per-sink
staging-buffer machinery required. Closed 2026-05-07.

### ~~Deactivation cleanup not yet modeled (cache-clear half)~~ — RESOLVED 2026-05-10 (D119/D120/D121, A sub-slice / Phase G cleanup_node)

Phase G (`Subscription::Drop` last-sub branch — `crates/graphrefly-core/src/node.rs`)
implements the cache-clear half of deactivation, mirroring TS `_deactivate`
(`packages/pure-ts/src/core/node.ts:2185-2297`):

1. (existing) user `cleanup_for(OnDeactivation)`
2. (existing) `producer_deactivate`
3. (existing) `wipe_ctx` (resubscribable + terminal)
4. **NEW Phase G:** Core cache-clear — releases compute `cached`
   (R2.2.7 / R2.2.8 ROM rule: state preserved, compute cleared per D119),
   per-dep `prev_data` + `data_batch` retains, per-dep `dep_terminals[i]`
   Error retains (D121 — closes the per-cascade-destination leak), pause
   buffer DATA, replay buffer; resets `has_fired_once`, `dirty`,
   `involved_this_wave`, `tracked` (dynamic). Drains pause lockset.
   Keeps per-node `terminal` slot intact (D121).

Lock-released release discipline: handles collected under state lock,
released after lock drop (mirrors `Core::resume` Phase 2). Re-entrance
into Core during `release_handle` is permitted. Verified by
`crates/graphrefly-core/tests/phase_g_cleanup_node.rs` (6 tests).

### ~~Cascade recursion can stack-overflow on deep chains~~ — RESOLVED in Slice A-bigger

`Core::terminate_node`, `Core::teardown_inner`, `Core::invalidate_inner`,
and `Core::activate_derived` now use `Vec`-based work queues instead of
recursing into the OS thread stack. Linear chains of 5000 nodes cascade
without overflow (verified by `tests/cascade_depth.rs`). Closed
2026-05-05.

### Wave-drain `pick_next_fire` cycle fallback can busy-loop

- **What:** [`Core::pick_next_fire`](../crates/graphrefly-core/src/node.rs)
  picks any pending fire when no node has all-deps-settled. If the chosen
  node's fn produces DATA that re-arms `pending_fires`, the wave never
  quiesces — only the 10k-iteration assert in `drain_and_flush` saves it
  by panicking the whole Core.
- **Why deferred:** registration-time cycle prevention is the proper fix
  (currently `register_computed` only validates dep existence; cycle
  detection is in `set_deps` only). Pre-existing in v0; Slice A+B did not
  introduce or worsen it.
- **Source:** Blind Hunter QA finding, 2026-05-05.

### `commit_emission` no-op equals propagates DIRTY+RESOLVED to all uninvolved children

- **What:** When a parent emits a same-value RESOLVED, the propagation path
  unconditionally queues DIRTY+RESOLVED on every child that hasn't been
  involved this wave (for diamond fan-in wave-mask completion). Subscribers
  downstream see DIRTY+RESOLVED noise even though no value flowed.
  Particularly visible for paused children (DIRTY immediate, RESOLVED
  buffered).
- **Why deferred:** pre-existing v0 design (diamond resolution wave-mask
  completion). Slice A+B did not change this behavior; flagged for
  evidence-driven review when subscriber semantics surface a real
  consumer-side issue.
- **Source:** Edge Case Hunter QA finding, 2026-05-05.

---

### ~~Subscribe handshake delivered as single sink call (R1.3.5.a divergence)~~ — RESOLVED in Slice A close

`Core::subscribe` now builds the handshake as per-tier slices and
fires each non-empty slice as a separate sink call:
`[Start]`, optionally `[Data(cache)]`, optionally `[Complete]` /
`[Error(h)]`, optionally `[Teardown]`. Cached state subscribes
produce 2 sink calls; torn-down state produces up to 4.

Verified by `handshake_tier_split_*` tests in
`tests/lock_released.rs` (2026-05-05). The
`inv7_handshake_first_message_is_start_*` proptests still hold —
they assert `Start` is the first message in the FIRST sink call, not
that the entire handshake is one call. Closed 2026-05-05.

## M2 Slice E+ /qa — closed audit fixes

The /qa pass on Slice E+ landed five patches: A2 (`NameError::Destroyed`
returned from `add` instead of `assert!` panic — symmetric with
`MountError::Destroyed`), B1 (mount/mount_new TOCTOU — parent inner
lock held across validation + insert), B2 (graph-layer meta filter
in `signal_invalidate` per R3.7.2 + new `Core::meta_companions_of`
accessor), B3 (`destroy()` reorder — namespace cleared AFTER teardown
cascade per R3.7.3), B4 (`describe()` `meta` field added per
canonical Appendix B JSON schema — always None in this slice; field
reserved). Plus auto-applicables: A1 (`assert!(matches!(...))` on
test variant checks), A3 (`#[must_use]` on `GraphObserveOne`), A4
(`signal_invalidate` short-circuits on destroyed graph), A5
(`status_of` doc-comment about reactive describe terminating
substate), A6 (regression test for parent.destroy propagating to
user-held child clones), B8 (`Graph::set_deps` rustdoc `# Hazards`
block referencing D1).

7 new regression tests added: `destroy_propagates_destroyed_flag_to_user_held_child_clone`,
`destroy_preserves_namespace_during_teardown_cascade`,
`signal_invalidate_skips_meta_companions`,
`signal_invalidate_on_destroyed_graph_is_noop`,
`describe_meta_field_omitted_when_none`,
`describe_meta_field_round_trips_via_json`,
`meta_companions_of_returns_registered_companions` (Core-side).
`add_after_destroy_panics` rewritten to
`add_after_destroy_returns_destroyed_error`.

Test count post-/qa: 227 (was 220 pre-/qa, 174 pre-Slice-E+).
`cargo clippy --all-targets -D warnings` clean. `cargo fmt --check`
clean. `#![forbid(unsafe_code)]` preserved.

## M2 Slice E+ — read-side introspection + composition divergences

### ~~`describe()` `value` field surfaces raw `HandleId`, not user-rendered `T`~~ — RESOLVED by D131 (F sub-slice, 2026-05-10)

- **Resolution (D131, A/B/D/E/F batch):** new optional `DebugBindingBoundary`
  extension trait in `crates/graphrefly-graph/src/debug.rs` (kept OUT of
  `graphrefly-core` to preserve serde-free dep footprint). `NodeDescribe.value`
  refactored to `Option<DescribeValue>` enum with `Handle(HandleId)` (raw,
  default for `Graph::describe()`) and `Rendered(serde_json::Value)` variants
  (binding-projected, from `Graph::describe_with_debug(debug)`). Serialized
  uniformly without an enum tag — consumers see either a number or whatever
  JSON shape the binding emits. Bindings impl `BindingBoundary` always and
  `DebugBindingBoundary` opt-in. 3 tests in `crates/graphrefly-graph/tests/describe.rs::debug_render`.

### ~~`observe_all()` snapshot-at-subscribe-time semantics~~ — RESOLVED in Slice F

`Graph::observe_all_reactive()` (Slice F, 2026-05-06) auto-subscribes
late-added named nodes via Graph-level namespace-change notifications.
The original `observe_all()` (snapshot-at-subscribe-time) remains
available as the non-reactive variant. Drop-order discipline prevents
a deadlock where the topology sink closure's `Arc<inner>` could be the
last reference and try to drop `Subscription`s under `CoreState` lock.
Closed 2026-05-06.

### ~~`signal_invalidate` does not snapshot the namespace under a single lock~~ — TIGHTENED by D130 (E sub-slice, 2026-05-10)

- **Resolution (D130, A/B/D/E/F batch):** `signal_invalidate` refactored to
  two-phase tree-wide gather. Phase 1 walks the entire mount tree under
  per-graph locks (each subgraph locked briefly to extract names + meta
  filter + children, then released), accumulating a flat `Vec<NodeId>`.
  Phase 2 runs `Core::invalidate` on the flat list — no Graph locks held
  during the Core cascade (preserves Slice F /qa D2 lock-ordering rule).
  DFS pre-order; destroyed subgraphs skipped via existing
  `inner.destroyed` short-circuit. Concurrent `add` / `mount_new` /
  `unmount` mid-walk only affect NOT-YET-snapshotted subgraphs — the
  invariant the original behavior aimed for is now structural rather
  than per-subgraph.
- **Where it landed:** `crates/graphrefly-graph/src/graph.rs` —
  `signal_invalidate` + new `collect_signal_invalidate_ids` recursive
  helper.
- **Tests:** 3 in `crates/graphrefly-graph/tests/mount.rs` —
  `signal_invalidate_traverses_deep_mount_tree`,
  `signal_invalidate_skips_destroyed_subtree`,
  `signal_invalidate_gather_does_not_hold_graph_lock_during_core_call`.
- **Source:** Slice E+ (2026-05-05); tightened by D130 (A/B/D/E/F batch, 2026-05-10).

### Anonymous Core nodes surface as `_anon_<NodeId>` strings in describe deps

- **What:** when a named node depends on a Core-registered node that
  has no namespace entry (e.g. registered via `Graph::core().register_state(...)`
  rather than `Graph::state(name, ...)`), `describe()` surfaces the
  dep name as `_anon_<NodeId.raw()>`. The anonymous node itself is
  NOT enumerated as a top-level node in the output.
- **Why divergent:** describe() is the named-graph view; anonymous
  Core-only nodes belong to the underlying dispatcher, not the
  namespace. Surfacing them as first-class nodes would dilute the
  namespace contract. Surfacing them only as edges keeps the output
  lossless without elevating them.
- **Lift point:** if a future user-facing demand surfaces, add an
  optional `describe({ include_anon: true })` mode that synthesizes
  `_anon_<id>` entries into `nodes`. v1 pattern: `Graph::add(id, "name")`
  to bring an anonymous node into the namespace.
- **Source:** Slice E+ (2026-05-05); per Q3 lock.

### ~~`try_resolve` does not support `..::sibling::node` cross-subgraph paths (R3.5.2)~~ — RESOLVED 2026-05-13 (Slice V3)

- **Resolution:** `try_resolve_checked` now supports `..` segments for parent traversal. `"..::sibling::node"` walks up to the parent graph via `inner.parent.upgrade()`, then recurses with the remaining path. Multiple `..` segments chain. `PathError::NoParent` is returned if `..` is used on a root graph. Recursive implementation handles all edge cases including mixed `..` and name segments.
- **Where it landed:** `crates/graphrefly-graph/src/graph.rs` — `try_resolve_checked` method.
- **Source:** Slice E+ /qa Edge Case Hunter F3 (2026-05-05); resolved Slice V3 (2026-05-13).

### ~~`try_resolve` returns `None` silently for malformed paths~~ — RESOLVED 2026-05-13 (Slice V3)

- **Resolution:** New `Graph::try_resolve_checked` returns `Result<Option<NodeId>, PathError>` with typed errors: `PathError::Empty`, `PathError::NoParent`, `PathError::ChildNotFound(String)`, `PathError::Destroyed`. Also supports `..` parent traversal (R3.5.2). `try_resolve` now delegates to `try_resolve_checked().ok().flatten()` preserving backward compat. `PathError` is publicly exported from `graphrefly_graph`.
- **Where it landed:** `crates/graphrefly-graph/src/graph.rs` — `try_resolve_checked`, `PathError` enum. `crates/graphrefly-graph/src/lib.rs` — re-export.
- **Source:** Slice E+ /qa Blind Hunter + Edge Case Hunter F9 (2026-05-05); resolved Slice V3 (2026-05-13).

### `audit_of` recursive node/mount counts are racy under concurrent mutation

- **What:** [crates/graphrefly-graph/src/mount.rs](../crates/graphrefly-graph/src/mount.rs)
  `audit_of` snapshots `inner.children.values()` then drops the lock
  before recursing. Concurrent `state(...)` / `mount_new(...)` on a
  child Graph during `unmount` produces inconsistent
  `RemoveAudit { node_count, mount_count }`.
- **Why deferred:** the unmount flow detaches the child from the
  parent BEFORE auditing, so the only writers racing the audit are
  threads that hold a `Graph` clone of the child. That is a narrow
  scenario and `RemoveAudit` is best-effort diagnostic data anyway.
  The proper fix (lock-ordering pass with multi-level locks across
  parent + descendants) is overkill for the current usage.
- **Source:** Slice E+ /qa Blind Hunter (2026-05-05).

### `Graph::set_deps` exposes the D1 (set_deps-from-firing-fn) hazard at a wider surface

- **What:** Pre-Slice-E+, hitting D1 (Dynamic `tracked` corruption
  from re-entrant `set_deps`) required reaching through `.core()`.
  `Graph::set_deps` is now public sugar at the namespace layer,
  widening the surface.
- **Why deferred:** the structural fix (thread-local "currently
  firing" stack rejecting `SetDepsError::ReentrantOnFiringNode`)
  belongs with the broader D1 work, not bolted on at the Graph
  layer. For now, `Graph::set_deps`'s rustdoc carries a `# Hazards`
  block cross-referencing D1.
- **Source:** Slice E+ /qa Edge Case Hunter F7 (2026-05-05).

### `GraphObserveOne::up()` decomposed into `pause` / `resume` / `invalidate` methods (R3.6.2 divergence)

- **What:** Canonical R3.6.2 specifies `up(messages: Messages)` as a
  unified upstream-injection API. Rust's `GraphObserveOne` exposes
  `pause(lock_id)` / `resume(lock_id)` / `invalidate()` as separate
  methods to avoid allocating a `Vec<Message>` on every upstream
  call.
- **Why divergent:** Rust ergonomics + non-allocating signature.
  Cross-binding wrappers may reassemble a unified `up(messages)` if
  needed.
- **Source:** Slice E+ /qa Edge Case Hunter F12 (2026-05-05).

### `GraphObserveAll::subscribe` accumulates Subscriptions into one shared vec

- **What:** Multiple `subscribe` calls on the same handle stash all
  per-node `Subscription`s into a single `subs` vec. There's no way
  to detach a single `subscribe` call's sinks independently — the
  user must drop the handle (which detaches everyone).
- **Why deferred:** v1 ergonomic decision. Per-call disposal would
  return `Vec<Subscription>` and surface the per-node subscription
  shape; current API hides that. Lifts when reactive observe
  changeset variants land (which need per-call accounting).
- **Source:** Slice E+ /qa Blind Hunter (2026-05-05).

### Unmount-vs-destroy not distinguishable on the reactive stream

- **What:** Observers see `[Complete, Teardown]` whether the user
  called `parent.unmount("p")` or `child.destroy()` directly. Only
  the unmount caller sees `RemoveAudit`; subscribers can't tell why
  their node terminated.
- **Why deferred:** add-on observability concern. Tracker
  retrospective patterns may need this distinction; lifts via either
  a new `Message::Unmount` variant (protocol expansion — needs spec
  amendment) or a "reason" field on `RemoveAudit` plumbed through
  the wave.
- **Source:** Slice E+ /qa Edge Case Hunter F6 (2026-05-05).

### ~~`_anon_<id>` deps could collide with a user-named node `_anon_<id>`~~ — RESOLVED 2026-05-13 (Slice V3)

- **Resolution:** `Graph::validate_name` now rejects names starting with `_anon_` via `NameError::ReservedPrefix`. `Graph::is_valid_name` also checks the prefix. The `_anon_<id>` format is reserved for anonymous-node describe output; user-named nodes cannot collide.
- **Where it landed:** `crates/graphrefly-graph/src/graph.rs` — `validate_name`, `is_valid_name`, `NameError::ReservedPrefix`. Mount layer `From<NameError> for MountError` updated to map `ReservedPrefix` → `InvalidName`.
- **Source:** Slice E+ /qa Blind Hunter (2026-05-05); resolved Slice V3 (2026-05-13).

### ~~`ancestors()` cycle insurance~~ — RESOLVED 2026-05-13 (Slice V3)

- **Resolution:** `ancestors()` now uses a `HashSet<usize>` visited set keyed by `Arc::as_ptr` address. If a cycle is encountered (future regression in mount validation), the walk breaks instead of looping forever.
- **Where it landed:** `crates/graphrefly-graph/src/mount.rs` — `ancestors` function.
- **Source:** Slice E+ /qa Blind Hunter (2026-05-05); resolved Slice V3 (2026-05-13).

### Port-coverage gaps in canonical §3 Graph surface

These canonical-spec methods are tracked for parity. Items marked
~~resolved~~ landed in M2 Slice F (2026-05-06).

- ~~**`graph.signal(messages)` general broadcast (R3.7.1)**~~ —
  **RESOLVED in Slice F.** `Graph::signal(SignalKind)` with
  `Invalidate` / `Pause(lock)` / `Resume(lock)` variants.
- ~~**Sugar wrappers (R3.2.1)**~~ — **RESOLVED in Slice F.**
  `set(name, h)` / `get(name)` / `invalidate_by_name(name)` /
  `complete_by_name(name)` / `error_by_name(name, h)`.
- ~~**`graph.remove(name)` (R3.2.3)**~~ — **RESOLVED in Slice F.**
  Handles both individual named nodes (TEARDOWN + namespace removal)
  and mounted subgraphs (delegates to `unmount`). Returns
  `GraphRemoveAudit`.
- ~~**`graph.edges(opts?)` (R3.3.1)**~~ — **RESOLVED in Slice F.**
  `Graph::edges(recursive: bool) -> Vec<(String, String)>` with
  recursive mount-walking and `_anon_<id>` for unnamed deps.
- **`graph.tagFactory(factory, factoryArgs)` (R3.1.2)** — provenance
  annotation for `describe()` / snapshot replay. Deferred.
- **`graph.resourceProfile()` (R3.6.3)** — runtime profile
  (subscriber counts, fan-in/out). Used by debugging utilities.
  Deferred.
- **`graph.setVersioning(level)` (R3.2.4)** — bulk versioning level
  apply. Waits on §7 Node Versioning port. Deferred.
- **napi-rs `BenchGraph` class** — Graph API bindings for JS
  consumers. Deferred until graph-layer API is consumer-driven.
- **Source:** Slice E+ /qa Edge Case Hunter port-coverage gap audit
  (2026-05-05); updated 2026-05-06 (Slice F close).

### Cross-Core (multi-binding) mount rejected with `MountError::CoreMismatch`

- **What:** [crates/graphrefly-graph/src/mount.rs](../crates/graphrefly-graph/src/mount.rs)
  `mount(name, child)` rejects when `parent.core` and `child.core`
  point to different dispatchers (verified via
  `Core::same_dispatcher` / `Arc::ptr_eq`). The user can `mount_new`
  a fresh subgraph sharing the parent's Core, or rebuild the child
  against the parent's Core.
- **Why deferred:** cross-Core mount requires the bindings layer
  to negotiate value-handle translation across registries. Open
  Question 1 in `archive/docs/SESSION-rust-port-architecture.md`
  Part 6 ("Cross-language handle-space composition") — explicit
  serialize bridge or process-boundary semantics. Post-M6 work.
- **Source:** Slice E+ (2026-05-05); reference Open Question 1.

---

## M3 Slice C-1 — operator deferrals

### ~~napi-rs operator binding parity not yet shipped~~ — RESOLVED 2026-05-07 (M3 napi-rs operator parity Phase A–C)

`BenchOperators` napi class shipped with 24 operators wired (6 transform + 3 combine + 7 flow + 4 producer-shape + 4 higher-order). `OperatorBinding` / `HigherOrderBinding` / `ProducerBinding` impls on `BenchBinding`. TSFN bridge (`bridge_sync<T, R>`) handles JS-callback sync return via worker-thread Core (D062). See `migration-status.md` row "M3 (napi-rs operator parity)" + session doc `archive/docs/SESSION-rust-port-napi-operators.md`.

### pyo3 operator binding deferred to M6

- **What:** `graphrefly-bindings-py` is scaffold-only. Operators surface lands with M6 closing the graphrefly-py G.6 parity gap.
- **Why deferred:** M6 is gated by M5 (structures); operators surface there is naturally part of the M6 milestone.
- **Source:** Slice C-1 close (2026-05-06).

### `OperatorOpts.equals` is currently a no-op for transform operators

- **What:** `Core::register_operator(deps, op, opts)` accepts `opts.equals: EqualsMode` and stores it on `NodeRecord.equals`. But all transform operators emit via `commit_emission_verbatim`, which skips equals substitution (R1.3.2.d / R1.3.3.c). `opts.equals` therefore has no observable effect for the current Slice C-1 operators.
- **Why deferred:** the field is reserved for future operators that explicitly want wire-level cache-vs-new equals dedup (e.g., a "distinctOutput" variant that combines distinctUntilChanged's prev-comparison with cache-substitution semantics). v1 ergonomics: keep the field in the struct so future callers don't break.
- **Lift point:** when a future operator opts in, add a `commit_emission` (with-substitution) path to its `fire_op_*` helper.
- **Source:** Slice C-1 close (2026-05-06).

### ~~`fire_operator` first-run gate uses linear-scan `has_sentinel_deps()`~~ — RESOLVED in Slice U

`has_sentinel_deps()` now uses the `received_mask: u64` bitmask (O(1) compare
against `full_mask`), resolving both this entry and §10.13 simultaneously.
Landed 2026-05-12 as part of the Slice U §10 perf batch.

- **Source:** Slice C-1 close (2026-05-06); resolved Slice U (2026-05-12).

### ~~Operator describe doesn't surface per-operator discriminant~~ — RESOLVED 2026-05-13 (Slice V5)

- **Resolution:** `NodeDescribe` gains `operator_kind: Option<String>` (serialized as `"operator"` in JSON, omitted for non-operator nodes). The `operator_op_name` helper maps each `OperatorOp` variant to its camelCase name (`"map"`, `"filter"`, `"scan"`, `"reduce"`, `"distinctUntilChanged"`, `"pairwise"`, `"combine"`, `"withLatestFrom"`, `"merge"`, `"take"`, `"skip"`, `"takeWhile"`, `"last"`, `"tap"`, `"tapFirst"`, `"valve"`, `"settle"`).
- **Where it landed:** `crates/graphrefly-graph/src/describe.rs` — `NodeDescribe.operator_kind` field + `operator_op_name` function.
- **Source:** Slice C-1 close (2026-05-06); resolved Slice V5 (2026-05-13).

## M3 Slice C-2 — combinator operator deferrals

### Merge first-error-terminates divergence (D022)

- **What:** TS `merge` terminates immediately on the first upstream `ERROR` (producer pattern with manual subscribe propagation). Rust `merge` uses Core's standard dep-terminal cascade (R1.3.4.b): error propagates only when ALL deps are terminal, and `ERROR` dominates over `COMPLETE`.
- **Why divergent:** Rust combinators register as standard operator nodes and inherit Core's uniform cascade behavior. TS merge uses a hand-wired producer pattern that intercepts errors eagerly. Unifying would require either a special "first-terminal" mode in Core (complexity tax on every node) or a producer-pattern escape hatch outside Core dispatch.
- **Lift point:** if a consumer needs first-error semantics, add `OperatorOpts::cascade: CascadeMode::FirstTerminal | AllTerminal` enum (new register_operator option). Deferred until evidence surfaces.
- **Source:** Slice C-2 close (2026-05-06); documented in combine.rs Rust test `merge_error_cascades_when_all_deps_terminal`.

### Combine custom tuple-equality deferred (D023)

- **What:** `combine` and `withLatestFrom` emit via `commit_emission_verbatim` (skips cache-vs-new equals check). A custom equals that compares tuple contents element-wise (avoiding downstream re-fire when only one dep changed to the same value) is not wired.
- **Why deferred:** the `EqualsMode::Custom(FnId)` machinery exists in Core but tuple-equality requires the binding to implement a cross-handle deep comparison that knows the internal structure of the packed tuple. No current consumer exercises this path; default Identity mode (always emit) is correct if slightly chatty.
- **Lift point:** when a consumer needs dedup on tuple outputs, wire `opts.equals: EqualsMode::Custom(eq_fn)` through `combine`/`with_latest_from` factory signatures and switch from `commit_emission_verbatim` to `commit_emission` (with-substitution path).
- **Source:** Slice C-2 close (2026-05-06).

### WithLatestFrom first-fire gate-release emits on any dep (semantic note)

- **What:** On the FIRST fire (first-run gate transitioning `has_fired_once` false→true), `fire_op_with_latest_from` unconditionally emits the `[primary, secondary]` pair regardless of which dep triggered the wave. This means if the gate release is triggered by a secondary-only delivery, it still emits (since the gate guarantees both deps have real values via `prev_data`).
- **Why this way:** TS uses `partial: false` which holds until all deps deliver, then fires the fn which sees both deps' data. The fn naturally emits regardless of which dep triggered (it has no concept of "primary fired this wave"). Rust's wave-end batch clearing means `primary_fired=false` on secondary-triggered gate release, so the first-fire special case is needed for parity with TS.
- **Source:** Slice C-2 close (2026-05-06); fix in `batch.rs::fire_op_with_latest_from`.

---

## M3 Slice C-3 — flow operator deferrals

### ~~Flow operator counters reset only on resubscribable terminal cycle (D028)~~ — FULLY CLOSED by D129 (D-α, 2026-05-10)

- **Resolution (D129, A/B/D/E/F batch):** Phase G on resubscribable + has-op now builds a FRESH scratch via `Core::make_op_scratch_with_binding` (lock-held — `binding.retain_handle` is a leaf op), installs it on `rec.op_scratch`, and pushes the OLD scratch to `CoreState::pending_scratch_release` for deferred release. The queue drains on the next `reset_for_fresh_lifecycle` Phase 3b (AFTER its Phase 2 fresh retain — preserves Slice C-3 /qa P1 seed-aliasing-acc invariant) or on `Drop for CoreState` (catch-all). All three rows of the original status matrix now correct:
  | Path | Status |
  |---|---|
  | Non-resubscribable + deactivate | released by Phase G ✓ (D124) |
  | Resubscribable + terminal + late-subscribe → reset | `reset_for_fresh_lifecycle` releases ✓ |
  | Resubscribable + non-terminal deactivate → reactivate | fresh scratch installed by Phase G; old shares deferred to queue ✓ (D129) |
- **Where it landed:** `crates/graphrefly-core/src/node.rs` — `Subscription::Drop` Phase G branch (lines ~858–1010) + `reset_for_fresh_lifecycle` Phase 3b (line ~3540) + `Drop for CoreState` D-α drain (lines ~5499+). Static `Core::make_op_scratch_with_binding(binding, op)` enables the Phase G call site (which has only `&dyn BindingBoundary`, not `&Core`).
- **Tests:** 5 in `crates/graphrefly-operators/tests/phase_g_op_scratch.rs` — `take_resubscribable_non_terminal_deactivate_resets_counter`, `scan_resubscribable_non_terminal_deactivate_resets_acc_to_seed`, `phase_g_resubscribable_seed_aliasing_acc_does_not_collapse_registry`, `pending_scratch_release_drains_on_core_drop`, `pending_scratch_release_drains_on_reset_for_fresh_lifecycle`.
- **Source:** Slice C-3 close (2026-05-06); D028. Partial close via /qa F8 (D124, 2026-05-10 — non-resubscribable case). Full close via D129 (A/B/D/E/F batch, 2026-05-10 — resubscribable case via defer queue).

### ~~`take_until(source, notifier)` not yet ported~~ — RESOLVED (Slice D / M3 producer-pattern slice); doc strike-through 2026-05-10 (B sub-slice cleanup batch)

`take_until` shipped in `crates/graphrefly-operators/src/ops_impl.rs:610` as a producer-pattern operator — subscribes to both `source` and `notifier`, forwards source DATA until notifier emits any DATA, then completes. Uses the H+ STRICT defer-to-wave-end machinery (`emit_or_defer` / `complete_or_defer` / `error_or_defer`). 4 cargo tests in `crates/graphrefly-operators/tests/subscription.rs:389+` (`take_until_forwards_source_until_notifier_emits`, `take_until_does_not_forward_notifier_value`, `take_until_source_complete_propagates`, `take_until_source_error_propagates`). The original deferred entry was preserved in the active backlog by oversight; doc strike-through landed during the 2026-05-10 B sub-slice cleanup batch (D122).

### ~~Operator describe doesn't surface flow-variant discriminant~~ — RESOLVED 2026-05-13 (Slice V5)

- **Resolution:** Inherited from Slice C-1 resolution. All flow-variant `OperatorOp` discriminants (`Take`, `Skip`, `TakeWhile`, `Last`) now surface in `NodeDescribe.operator_kind` as `"take"`, `"skip"`, `"takeWhile"`, `"last"` respectively.
- **Source:** Slice C-3 close (2026-05-06); resolved Slice V5 (2026-05-13).

### `predicate_each` length-mismatch silently truncates `take_while` in release builds (Slice C-3 /qa D1)

- **What:** [`fire_op_take_while`](../crates/graphrefly-core/src/batch.rs) loops over `inputs` and reads `pass.get(i).copied().unwrap_or(false)` (`batch.rs` ~1382). A `debug_assert!` catches `pass.len() != inputs.len()` in debug builds. In release builds, a binding-side bug returning a too-short `Vec<bool>` causes `unwrap_or(false)` to read missing entries as `false` → first-false short-circuit → `done` + `self.complete()`. Silent self-completion. The same defect class affects `fire_op_filter`, but there it just silently drops items without terminating — less catastrophic.
- **Why deferred:** binding contract violation; no production binding (napi-rs / pyo3 / wasm-bindgen) should violate the contract. Promoting `debug_assert!` to `assert!` would crash release builds on a misbehaving binding rather than silently truncate; that's a defensible tradeoff for v1 but not blocking. A unified `binding_contract_check_pass_len(pass, inputs)` helper that asserts in all builds is the cleaner long-term fix.
- **Lift point:** when bench evidence shows operator dispatch is hot enough to warrant pre-validation cost, OR when a real binding bug shows up in production. Until then, defensive `unwrap_or(false)` is acceptable.
- **Source:** Slice C-3 /qa Edge Case Hunter (2026-05-06).

### Future napi-rs `last(src, opts?)` adapter must accept `{ defaultValue: T }` shape (Slice C-3 /qa D2)

- **What:** TS legacy `last(source, options?: { defaultValue?: T })` accepts an optional opts object; `Object.hasOwn(options, "defaultValue")` distinguishes "no default" (key absent) from "explicit undefined default" (key present). Rust core splits this into two factories: [`last`](../crates/graphrefly-operators/src/flow.rs) (no default — emits only `Complete` on empty stream) and [`last_with_default`](../crates/graphrefly-operators/src/flow.rs) (real default handle required). The parity-test scenario [`packages/parity-tests/scenarios/operators/flow.test.ts`](https://github.com/graphrefly/graphrefly-ts/blob/main/packages/parity-tests/scenarios/operators/flow.test.ts) calls `impl.last(src, { defaultValue: 42 })` against `pureTsImpl`. When `rustImpl` activates, the napi-rs binding must dispatch `impl.last(src, { defaultValue: V })` to the Rust `last_with_default(src, intern(V))` factory and `impl.last(src)` (no opts) to `last(src)`.
- **Why deferred:** parity surface concern for the future binding slice; not a Rust core concern. The Rust dispatch is correct (the two factories cover the two semantic cases); only the JS-binding adapter layer needs to bridge.
- **Lift point:** when the napi-rs operator binding lands (M3 close-gate residual, alongside the TSFN custom-equals work). The binding's `last(src, opts?)` adapter inspects `opts` and `Object.hasOwn(opts, "defaultValue")`, then dispatches accordingly. The Rust-only edge of `defaultValue: undefined` (TS distinguishes "explicit undefined" from "absent") is acceptably collapsed (Rust has no `undefined`); document the divergence in the binding's adapter.
- **Source:** Slice C-3 /qa Edge Case Hunter (2026-05-06).

---

## M2 Slice F /qa — deferred concerns (D1–D6)

QA pass on Slice F surfaced these v1 limitations / divergences. Each
is acceptable for the M2 close but worth tracking for future slices.

### D1 — `edges()` cross-graph deps surface as `_anon_<id>` for sibling-graph nodes

- **What:** [crates/graphrefly-graph/src/graph.rs](../crates/graphrefly-graph/src/graph.rs)
  `edges_inner` builds `names_map` from the local namespace only.
  Two graphs sharing a Core (parent + mounted child OR siblings under
  a common ancestor) can have cross-graph dep edges (a node in Graph A
  depends on a node in Graph B). When `edges(false)` is called on A,
  the dep id is not in A's namespace → emitted as `_anon_<id>`,
  even though it IS named in B.
- **Why deferred:** v1 acceptable behavior for the namespace-scoped
  edges accessor. The `recursive: true` walk also misses sibling
  references (it only descends into mounted children of the calling
  graph). Same shape as the existing Slice E+ /qa "`try_resolve` no
  `..::sibling::node`" deferral.
- **Lift point:** if cross-graph edge resolution surfaces as a real
  consumer need (e.g., describe-the-whole-shared-Core view), add an
  `edges_in_core(opts)` accessor or a `Graph::root().edges(true)`
  walk that builds a unified id→qualified-name map across the entire
  shared-Core mount tree.
- **Source:** Slice F /qa Blind Hunter + Edge Case Hunter (2026-05-06).

### ~~D2 — `GraphObserveAllReactive::subscribed` HashSet grows unboundedly~~ — RESOLVED 2026-05-13 (Slice V3)

- **Resolution:** `GraphObserveAllReactive::subscribe` now wires a `Core::subscribe_topology` listener that prunes `subscribed` and `subs` on `TopologyEvent::NodeTornDown(id)`. The `_topo_sub: Option<TopologySubscription>` field keeps the listener alive; dropping the handle unsubscribes both namespace and topology listeners.
- **Where it landed:** `crates/graphrefly-graph/src/observe.rs` — `GraphObserveAllReactive` struct + `subscribe` method.
- **Source:** Slice F /qa Blind Hunter (2026-05-06); resolved Slice V3 (2026-05-13).

### ~~D3 — `signal()` `SignalKind` enum narrower than canonical's open `Messages`~~ — RESOLVED 2026-05-13 (Slice V3)

- **Resolution:** `SignalKind` extended with `Complete` and `Error(HandleId)` variants. `signal_complete` and `signal_error` implementations added with meta-companion filtering (same as `signal_invalidate`, per D4 resolution).
- **Where it landed:** `crates/graphrefly-graph/src/graph.rs` — `SignalKind` enum + `signal_complete` / `signal_error` methods.
- **Source:** Slice F /qa Edge Case Hunter F6 (2026-05-06); resolved Slice V3 (2026-05-13).

### ~~D4 — `signal(Pause/Resume)` does not filter meta companions~~ — RESOLVED 2026-05-13 (Slice V3)

- **Resolution:** `signal_pause`, `signal_resume`, `signal_complete`, and `signal_error` now all use `collect_signal_ids_with_meta_filter()` — the same iterative worklist + meta-companion HashSet filtering as `collect_signal_invalidate_ids`. Meta companions are excluded from all four signal broadcasts, matching the symmetry argument.
- **Where it landed:** `crates/graphrefly-graph/src/graph.rs` — new `collect_signal_ids_with_meta_filter` helper; `signal_pause`, `signal_resume`, `signal_complete`, `signal_error` refactored to use it.
- **Source:** Slice F /qa Edge Case Hunter F7 (2026-05-06); resolved Slice V3 (2026-05-13).

### ~~D5 — `describe_reactive` does not fire on `set_deps` topology changes~~ — RESOLVED 2026-05-13 (Slice V3)

- **Resolution:** `describe_reactive` now also subscribes to `Core::subscribe_topology` and fires a fresh `describe()` snapshot on `TopologyEvent::DepsChanged`. The topology subscription is held in `ReactiveDescribeHandle._topo_sub` and cleaned up on drop (dropped BEFORE unsubscribing namespace sink to avoid deadlock).
- **Where it landed:** `crates/graphrefly-graph/src/describe.rs` — `ReactiveDescribeHandle` struct + `describe_reactive` method.
- **Source:** Slice F /qa Edge Case Hunter F10 (2026-05-06); resolved Slice V3 (2026-05-13).

## M2 parity-tests fragility (post-/qa, 2026-05-06)

Surfaced during /qa of the B+C+D batch. The six new `scenarios/graph/`
scenarios pass against `pureTsImpl` but rest on assumptions that may not
generalize cleanly when `rustImpl` activates. Tracked rather than fixed
in-batch because (a) `rustImpl` is still `null` (no observable divergence
yet), and (b) tightening would require either spec-citation amendments or
shape-adapter design that's better addressed in a follow-up slice.

### MP1 — `describe-reactive` parity tests assume synchronous flush outside `batch()`

- **What:** [packages/parity-tests/scenarios/graph/describe-reactive.test.ts](https://github.com/graphrefly/graphrefly-ts/blob/main/packages/parity-tests/scenarios/graph/describe-reactive.test.ts)
  "emits a fresh snapshot when a node is added/removed" tests call
  `g.state(...)` / `g.remove(...)` outside an explicit `batch()` and
  immediately read `snaps.length` to detect the new snapshot. TS legacy's
  `_describeReactive` uses `registerBatchFlushHook` which fires
  synchronously when `bump()` is called outside a batch.
- **Why deferred:** if Rust's reactive describe coalesces namespace
  changes through a different mechanism (e.g., a wave-end notify), the
  test could become non-deterministic across impls.
- **Lift point:** wrap the mutation + assertion in `g.batch(() => {...})`
  to make the coalescing point explicit, OR add a spec-cited assertion
  that namespace-change → reactive-describe push is synchronous.
- **Source:** Slice B+C+D /qa Blind Hunter #8 + Edge Case Hunter #11
  (2026-05-06).

### MP2 — `sugar.test.ts` derived-fn data shape may not match across impls

- **What:** the "g.set chained through derived produces propagated DATA"
  test passes a `(data) => result[]` fn where `data[0]` is treated as
  `T[]` (a plain array of values). TS legacy's `GraphDerivedFn` exposes
  `data` as `readonly (readonly unknown[] | undefined)[]` — per-dep
  batches. The test happens to work because `batch0.map(...)` over a
  homogeneous batch returns a sensible result.
- **Why deferred:** Rust's binding may expose dep batches via a different
  shape (e.g., `DepBatch`-like struct, or a flat per-emit array). Without
  an `Impl.derivedFn` adapter or shape-locking interface, the test will
  silently break or pass-with-different-semantics when `rustImpl`
  activates.
- **Lift point:** add `Impl.derivedFn(deps, fn) -> Node` factory or
  document the canonical batch shape contract in scenario docstring.
- **Source:** Slice B+C+D /qa Blind Hunter #10 (2026-05-06).

### MP3 — `signal.test.ts` tier-3 rejection assumes synchronous throw

- **What:** `expect(() => g.signal([[impl.DATA, 99]])).toThrow();` requires
  the impl to validate synchronously inside the call. TS legacy's
  `signal()` validates in a `for...of` BEFORE `_signalDeliver`, so
  rejection IS synchronous.
- **Why deferred:** Rust port's `signal(SignalKind)` is a closed enum
  (no `Data` variant — see D3 above), so this test can't even map to
  the Rust binding's surface 1:1 today. The test asserts TS-impl-specific
  shape.
- **Lift point:** when rustImpl activates, either widen `SignalKind` to
  include a Data variant that rejects (matching the test), or update the
  test to use the impl's shape.
- **Source:** Slice B+C+D /qa Blind Hunter #15 + Edge Case Hunter #9
  (2026-05-06).

### MP4 — `observe-all-reactive.test.ts` unsubscribe test conflates deactivation with sink-list-clear

- **What:** the "unsubscribe stops further events" test calls `unsub()`
  then asserts no further events. TS legacy achieves this via single-
  subscriber deactivation (last sink unsubscribes → derived deactivates →
  `disposed = true` → `actions.emit` no-ops). Rust port's
  `GraphObserveAllReactive` clears its own sink list on unsub
  independently. The test pins one mechanism but the behavior is
  semantically the same.
- **Why deferred:** the divergence isn't observable in the current
  single-sink test scenario. It would surface in a 2-sink test where one
  unsubscribes and the other expects to keep receiving events.
- **Lift point:** add a parallel test where two sinks are subscribed,
  one unsubs, and assert the other still receives events. That pins the
  cross-impl contract precisely.
- **Source:** Slice B+C+D /qa Edge Case Hunter #10 (2026-05-06).

---

### ~~D6 — `packages/parity-tests/scenarios/graph/` not yet widened for the M2 surface~~ — RESOLVED 2026-05-06

Six scenario files landed under
`~/src/graphrefly-ts/packages/parity-tests/scenarios/graph/`: `sugar.test.ts`
(R3.2.1 named-sugar — set/invalidate/complete/error/derived-propagation),
`remove.test.ts` (R3.2.3 — node + mount remove), `edges.test.ts` (R3.3.1 —
local + recursive + `_anon_<id>` for unnamed), `signal.test.ts` (R3.7.1 —
INVALIDATE broadcast + recursion into mounts + tier-3 rejection),
`describe-reactive.test.ts` (R3.6.1 — initial snapshot + add + remove
events), `observe-all-reactive.test.ts` (R3.6.2 — unsubscribe-stops-events;
late-node auto-subscribe skipped).

The `Impl` interface widened with `Graph` + tier symbols (`INVALIDATE` /
`PAUSE` / `RESUME` / `COMPLETE` / `ERROR` / `TEARDOWN`). 18 scenarios
passing; 2 skipped tests (with comments) cover Rust-port-only enhancements
not yet backported to TS legacy: `R3.7.3 ordering` (TS clears namespace
before TEARDOWN — Rust port reordered in Slice F /qa P1) and
`observe({ reactive: true })` auto-subscribe-late-nodes (Rust port adds
this via namespace-change sinks — TS legacy snapshots at observe-time).
When @graphrefly/native publishes and rustImpl activates, those skips
become divergence assertions that fail loud.

Closed 2026-05-06.

### Slice U-napi parity widening (2026-05-12) — pure-ts passing, rustImpl deferred

- **What:** `Impl` interface widened with 13 new methods: `tap`, `tapObserver`, `onFirstData`, `rescue`, `valve`, `settle`, `repeat`, `buffer`, `bufferCount`, `fromIter`, `of`, `empty`, `throwError`. Three new scenario files: `control.test.ts` (7 scenarios), `buffer.test.ts` (2 scenarios), `sources.test.ts` (4 scenarios). All 13 pass against `pureTsImpl`. The `rustImpl` arm stubs are in place but fail (expected — native module not rebuilt with Slice U operators).
- **Why deferred (rustImpl):** Native module rebuild is a separate step gated by napi-build CI. The pure-ts scenarios validate spec semantics; rustImpl activation validates FFI parity.
- **Lift point:** Next napi rebuild (pre-M6). When `@graphrefly/native` publishes, all 13 scenarios should pass against both impls.
- **Source:** Slice U-napi S5 (2026-05-12).

---

## ~~R1.3.2.d / R1.3.3.a equals-substitution scope violation~~ — RESOLVED 2026-05-07 in Slice G

Closed by the per-wave equals coalescing fix in
[`crates/graphrefly-core/src/batch.rs`](../crates/graphrefly-core/src/batch.rs)
`Core::commit_emission`. The dispatcher now uses a wave-scoped
`tier3_emitted_this_wave: AHashSet<NodeId>` to detect "subsequent emit at
this node in the same wave." On a subsequent emit:

1. Skip equals substitution (would violate R1.3.2.d).
2. Queue Data verbatim.
3. Retroactively rewrite any prior `Resolved` entries (in pending_notify
   AND in the per-node pause buffer) to `Data(snapshot)` using the
   wave-start cache snapshot, taking fresh retains as needed.

Auto-resolve / Noop / empty-Batch / settle_dirty_resolved paths now also
guard against queueing Resolved into a wave that already has Data.

The F1 dev-mode `debug_assert` is re-added in `queue_notify` and locks the
R1.3.3.a invariant in dev / test builds. Regression tests:
`slice_g_batch_multi_same_value_emit_does_not_produce_multi_resolved`
(multi-emit batch produces multi-DATA, no multi-Resolved) and
`slice_g_single_emit_equals_match_still_produces_resolved` (R1.3.2.a
preserved for single-emit). All 411 tests pass with F1 active.

## QA-surfaced divergences (Slice F audit follow-on QA, 2026-05-07)

### `Core::up(INVALIDATE)` cascades through dep instead of R1.4.2 plain-forward

- **What:** Canonical R1.4.2 says upstream INVALIDATE is "plain forward —
  does not self-process INVALIDATE on intermediate or terminal nodes."
  Rust v1's [`Core::up`](../crates/graphrefly-core/src/node.rs)
  Tier-4 routing delegates to `self.invalidate(dep_id)` per dep, which
  DOES clear the dep's cache and cascade INVALIDATE downstream.
- **Why divergent:** consumer-friendly v1 simplification. Calling
  `core.up(node, Invalidate)` to "nudge deps to invalidate" actively
  wipes their caches; users expecting plain-forward semantics get
  surprise sibling-subgraph invalidation.
- **Lift point:** when a real consumer hits the divergence, lift to a
  plain-forward path (queue `Message::Invalidate` to dep's subscribers
  without `Core::invalidate`) OR add `up_with_options` for
  per-call control.
- **Source:** Slice F audit follow-on /qa Edge Case Hunter (2026-05-07);
  user decision D2=(a).

### `pending_pause_overflow` cleared on panic-unwind silently drops queued ERROR

- **What:** [`clear_wave_state`](../crates/graphrefly-core/src/node.rs)
  unconditionally clears `pending_pause_overflow` on every wave-end,
  including the panic-discard path in `BatchGuard::drop`. Scenario: a
  wave queues a pause-overflow `PendingPauseOverflow` event, then a
  different fn panics mid-wave. The panic-discard path skips the
  synthesis loop and clears the queue → ERROR silently lost.
- **Why divergent:** `BatchGuard` panic-discard atomicity says "the
  wave didn't happen." Re-queueing overflow events would partially
  break that invariant. The pause buffer's `dropped` counter IS
  preserved (consumers polling `ResumeReport.dropped` post-resume
  still see the count), but the diagnostic ERROR with
  `lockHeldDurationMs` etc. is lost for that specific wave.
- **Why deferred:** narrow scenario (pause overflow + same-wave panic
  + binding implements `synthesize_pause_overflow_error`). The fn
  panic itself is louder than the missing diagnostic.
- **Lift point:** if consumers report missing ERRORs in production
  panic-recovery scenarios, defer overflow events across waves with
  explicit ordering semantics.
- **Source:** Slice F audit follow-on /qa Edge Case Hunter
  (2026-05-07); user decision D4=(a).

### `Core::up` from inside fn-fire — composition note (false-positive defense)

- **What:** Initially flagged by /qa as a hazard ("nested run_wave
  resets wave-state mid-outer-wave"). Confirmed FALSE POSITIVE: nested
  `run_wave` calls observe `in_tick=true` and acquire a non-owning
  `BatchGuard` (line 2156: `if !self.owns_tick { return; }`). Drop
  returns immediately without `clear_wave_state`. Outer wave's Slice G
  / A3 / A6 state survives intact.
- **Status:** documentation-only entry to capture the verification.
  Same-thread re-entrant `actions.up(...)` calls compose cleanly with
  the outer wave; cross-thread emits block on `wave_owner` per the
  M1 design.
- **Source:** Slice F audit follow-on /qa Edge Case Hunter EC#3
  (2026-05-07) — flagged + verified false-positive; user decision
  D1=(c) doc-only.

### ~~D3 — `register` / `set_pausable_mode` typed-error refactor~~ — RESOLVED 2026-05-07 in Slice H

Slice H landed the typed-error promotion as scheduled. New types:

- `RegisterError::{UnknownDep, OperatorWithoutDeps,
  InitialOnlyForStateNodes, OperatorSeedSentinel, TerminalDep}` —
  every variant zero-side-effect on `Err` (no node added to graph,
  scratch handle retains released lock-released).
- `SetPausableModeError::{UnknownNode, WhilePaused}` — widened to
  also surface `UnknownNode` (previously panicked via
  `require_node_mut`), per user direction Q3=widen for surface
  consistency with `Core::up::UpError`.

`Core::register` restructured into three phases (lock-released
validation → scratch creation → state-lock validation with
refcount-discipline early-return). `make_op_scratch` returns `Result`
with seed-sentinel check BEFORE any `retain_handle` so `Err` from
that path takes no retains; `Err` from later phases releases scratch
via `OperatorScratch::release_handles` lock-released. `reset_for_fresh_lifecycle`
uses `.expect()` because seed validity is guaranteed at registration time.

Sweep applied across ~150 call sites: tests `.unwrap()`,
production-shape sites in `graphrefly-operators` / `graphrefly-graph`
/ `graphrefly-bindings-js` use `.expect("invariant: …")`. `Graph::derived` /
`Graph::dynamic` / `Graph::state` got `# Panics` doc sections — their
public `Result<NodeId, NameError>` return covers namespace conflicts
only; Core-layer `RegisterError` is treated as caller-contract
violation and surfaces as panic.

10 new regression tests in `tests/slice_h.rs` cover each error variant
plus refcount-leak verification. **427 tests pass workspace-wide**
(was 417 pre-Slice-H).

**Source:** Slice F audit follow-on /qa decision D3=(a) (2026-05-07);
implemented same day.

## Spec divergences acknowledged in v1

These are deliberate simplifications relative to the TS production
dispatcher (`~/src/graphrefly-ts/src/core/node.ts`). Each is a known
divergence; M1-close hardening or M2 work may revisit.

### ~~Pause-buffer overflow does not synthesize ERROR (TS Lock 6.A divergence)~~ — RESOLVED 2026-05-07 (Slice F A3); doc strike-through 2026-05-10 (B sub-slice cleanup batch)

The canonical fix landed in Slice F A3 (2026-05-07). `BindingBoundary::synthesize_pause_overflow_error(node_id, dropped_count, configured_max, lock_held_duration_ms) -> Option<HandleId>` is the binding-side hook. `Core::drain_and_flush` invokes the synth at wave-end (`crates/graphrefly-core/src/batch.rs:856`), routes the returned handle through `Core::error` to terminate the node and cascade. Default impl returns `None` (silent drop + `ResumeReport.dropped` fallback). Tests in `crates/graphrefly-core/tests/slice_f_corrections.rs::a3_pause_overflow_synthesizes_error_once_per_cycle` and `a3_overflow_silently_dropped_when_binding_returns_none`. The original deferred entry was preserved in the active backlog by oversight; doc strike-through landed during the 2026-05-10 B sub-slice cleanup batch (D122).

### ~~`alloc_lock_id` can collide with user-supplied `LockId::new(N)`~~ — RESOLVED 2026-05-07 (Slice F A4); doc strike-through 2026-05-10 (B sub-slice cleanup batch)

Resolved via option (b) of the lift options — high-range reservation. `next_lock_id` initialized at `1u64 << 31` (`crates/graphrefly-core/src/node.rs:1807`); `LockId` ranges documented at `crates/graphrefly-core/src/handle.rs:118-133`: `[0, 1<<32)` user range, `[1<<32, u64::MAX]` dispatcher range. User-supplied ids (typical max u32::MAX from JS marshalling) cannot collide with dispatcher allocations. The original deferred entry was preserved in the active backlog by oversight; doc strike-through landed during the 2026-05-10 B sub-slice cleanup batch (D116).

### ~~Cross-wave DIRTY-then-DATA under PAUSE (was "DIRTY-without-DATA across pause-resume")~~ — RESOLVED 2026-05-07 in Slice F audit follow-on

The canonical fix landed: `PausableMode::{Default, ResumeAll, Off}` enum
on `NodeOpts`, with `Default` (the canonical default per spec §2.6) now
suppressing fn-fire upstream while paused — so neither tier-1 DIRTY nor
tier-3 DATA propagates from the paused node during the pause window.
On final RESUME, fn fires once with consolidated dep state. Closes the
cross-wave DIRTY-then-DATA gap that prompted the original
"DIRTY-without-DATA" framing.

The cross-repo audit (2026-05-07) confirmed:

- **R1.3.1.b** is about intra-wave glitch-free ordering, NOT same-wave
  subscriber pairing across PAUSE.
- **§2.6** documents three modes (`true`/Default, `"resumeAll"`,
  `false`/Off); all three are canonical. The original Rust v1 choice
  to "canonicalize on resumeAll" was a regression — `Default` is the
  default for every untagged source.
- TS reference for `Default` mode: `pure-ts/src/core/node.ts:2556-2558`
  (`_pendingWave = true` gate) + `node.ts:3103-3106`
  (`_maybeRunFnOnSettlement()` on RESUME).

Rust impl in `crates/graphrefly-core/src/node.rs` `PauseState::Paused.pending_wave`
flag + `Core::set_pausable_mode` setter + `deliver_data_to_consumer`
suppression branch + `Core::resume` Phase-4 consolidated fn-fire schedule.
Regression: `item5_default_mode_consolidates_to_one_fn_fire_on_resume` in
`tests/slice_f_corrections.rs`. Closed 2026-05-07.

### ~~`set_deps` mid-wave full-removal leaves stuck DIRTY without RESOLVED~~ — RESOLVED 2026-05-10 (D117, B sub-slice)

When `set_deps(N, &[])` is called mid-wave AND `pending_notify[N]` contains
a tier-1 (DIRTY) message with no settle yet, `Core::set_deps` now pushes
`N` to `pending_auto_resolve` (the existing wave-end auto-resolve sweep
mechanism). The drain-end sweep at `crates/graphrefly-core/src/batch.rs:923+`
re-checks pending_notify and queues a paired Resolved via `queue_notify`
(handles paused-children pause-buffer routing automatically). Verified by
`crates/graphrefly-core/tests/setdeps.rs::set_deps_full_removal_mid_wave_pairs_dirty_with_resolved`.
Closes R1.3.1.b two-phase-push pairing for the empty-deps mid-wave case.

### `pending_notify` IndexMap insertion-order: meta companion ordering breaks for **mid-wave** teardown

- **What:** `pending_notify: IndexMap<NodeId, Vec<Message>>` flushes in
  insertion order, so meta-TEARDOWN ordering works for the standard case
  (teardown at wave start). But if a parent had earlier-wave traffic
  (DIRTY/RESOLVED queued under its key) BEFORE the teardown, the parent's
  entry is already in the map with an earlier insertion position than the
  meta's later teardown queue. The meta's TEARDOWN flushes after the
  parent's wave traffic plus its own teardown, breaking R1.3.9.d ordering.
- **Why deferred:** niche path (mid-wave teardown of a node that already
  emitted other traffic this wave). Tests cover the common case
  (teardown-between-waves). Real fix likely requires per-tier flush
  ordering in `flush_notifications` (companions flush before parents at
  tier-6) or a separate teardown queue.
- **Source:** Blind Hunter QA finding, 2026-05-05.

### ~~Non-resubscribable terminal Error handles leak via diamond cascade~~ — RESOLVED 2026-05-10 (D121, A sub-slice / Phase G cleanup_node)

Phase G (`Subscription::Drop` last-sub branch — `crates/graphrefly-core/src/node.rs`)
releases per-dep `dep_terminals[i]` Error retains on consumer deactivation
(D121). The leak was per-cascade-destination — each consumer node retained
one share of the upstream's error handle in its own `dep_terminals[i]`
slot. Phase G walks `rec.dep_records` releasing those retains alongside
`prev_data` + `data_batch` retains. The producer's own `rec.terminal` slot
is preserved (D121: needed for late-subscriber R2.2.7.a reset or R2.2.7.b
rejection per D118). Verified by
`crates/graphrefly-core/tests/phase_g_cleanup_node.rs::phase_g_releases_dep_terminal_error_handles_on_deactivation`.
Closes the cross-repo audit note about the discipline mirroring TS
`_deactivate` (per-dep settled-state release).

## Open questions from SESSION-rust-port-architecture.md Part 6

These were flagged in the session doc Open Questions section as not blocking
M1, but worth re-validating during the port.

1. **Cross-language handle-space composition.** TS-frontend mounting a
   Python-frontend subgraph: registries are disjoint. Probably "explicit
   serialize bridge" or "process boundary" rather than fusing registries.
   Out of scope until post-M6.
2. **Async fn boundary modeling in TLA+.** Current spec is sync-only; pyo3 async
   path will need an additional MC.
3. **Refcount soundness companion module.** Production-grade leak detection;
   prototype `extensions.test.ts` smoke-tests bounded handle counts. Add a
   `loom`-permuted MC if it surfaces as M3+ concern.
4. **Custom-equals oracle correctness beyond identity-extension.**
   `H2 IdentityEqualsIsPureCore` ASSUME catches misconfigured oracles
   structurally, but doesn't catch oracles violating symmetry/transitivity.
   Probably out of scope for the protocol spec; binding-layer test convention.
5. ~~Phase 14 missing `### Phase 14 — …` header in
   `~/src/graphrefly-ts/docs/implementation-plan.md`.~~ ✅ already fixed
   upstream (Phase 14 has a proper header at line 1436); the SESSION doc
   note referenced shifted line numbers from a snapshot-in-time. Verified
   2026-05-05 during Slice A+B.

---

## Phase 13.8 carry-forward follow-ups

Surfaced during the TS rewire impl + integration gap-finding. Inherited by
the Rust port — to re-validate when `set_deps` carries the full M1 feature
set (PAUSE/RESUME, INVALIDATE, COMPLETE/ERROR/TEARDOWN).

- ~~**Late-subscriber-to-terminal-node-delivers-nothing universal foot-gun.**~~
  RESOLVED 2026-05-10 (D118, B sub-slice) — canonical-spec amendment
  R2.2.7.a + R2.2.7.b in [`docs/implementation-plan-13.6-canonical-spec.md`](../docs/implementation-plan-13.6-canonical-spec.md)
  (and `~/src/graphrefly/GRAPHREFLY-SPEC.md` mirror) cleanly splits the
  policy by `resubscribable` flag, replacing the prior "replay terminal
  history as courtesy" with explicit semantics:
  - **Resubscribable + terminal** (any TEARDOWN state) → reset to fresh
    lifecycle on subscribe (R2.2.7.a). Drops the prior F3 audit guard
    (`!has_received_teardown`).
  - **Non-resubscribable + terminal** → reject — `Core::try_subscribe`
    returns `Err(SubscribeError::TornDown { node })`; public `subscribe`
    panics (R2.2.7.b).
  Operators (zip / concat / race / take_until / merge / switch_map / etc.)
  match on the `SubscribeError` variant and skip dead upstream sources.
  Verified by `crates/graphrefly-core/tests/resubscribable.rs` (5 new
  tests covering rejection × 4 + reset-after-teardown + reset-after-any-
  terminal). TS mirrored at `packages/pure-ts/src/core/node.ts:1281+`
  (3008/3008 tests pass + 142 parity tests pass).
- **TS operator audit for R2.2.7.b rejection handling (D127, /qa F3 carry-forward)** — PARTIALLY RESOLVED by D133 (A sub-slice, 2026-05-10) for `extra/operators/*`; `extra/sources/*` and patterns-layer sites remain unaudited.
  - **Resolved scope (D133, 2026-05-10):** new `subscribeOr(source, sink, onDead)` helper added to
    `packages/pure-ts/src/core/subscribe-error.ts` (also exported via `@graphrefly/graphrefly`).
    25 sites migrated across `packages/pure-ts/src/extra/operators/{buffer,take,control,combine,time,higher-order}.ts`.
    Per-op Dead semantics match Rust impl: buffer / time-window-style →
    flush + self-COMPLETE; takeUntil source → self-COMPLETE; takeUntil
    notifier → no-op; merge / concat / zip / race → per-source-Dead
    handling; higher-order inner → `on_inner_complete` via `forwardInner`.
    All 3008 pure-ts tests pass post-migration.
  - **Remaining scope (carry-forward):** `packages/pure-ts/src/extra/sources/{settled.ts, async.ts}` contains ~8 raw `source.subscribe(...)` calls in `firstWhere` / `firstValueFrom` / `subscribeAndAwaitDone` / async-iter conversion paths that still throw uncaught `TornDownError` on dead upstreams. Patterns-layer `compat/*` and `patterns/**/*.ts` not yet swept either. Lift: wrap each call site with `trySubscribeOrDead` or the new `subscribeOr` helper + per-op Dead-handling matching its consumer contract (e.g. `firstValueFrom(dead)` should reject the promise with `TornDownError`, not bubble it).
  - **Source:** /qa F3 (2026-05-10); partially resolved by D133 (operators only). Full close awaits consumer pressure on sources/patterns paths.

- **~~F2 immediate-Dead end-to-end operator tests (D126, /qa carry-forward)~~** — RESOLVED by D132 (B sub-slice, 2026-05-10).
  New `OpRuntime::with_all_partitions_held(f)` helper in
  `crates/graphrefly-operators/tests/common/mod.rs` wraps `f` in a
  `core.batch()` scope (which acquires every existing partition's
  `wave_owner` in ascending `SubgraphId` order via the retry-validate
  loop). Producer-pattern operator factories called inside the helper
  see partition-coherent state — `try_subscribe(dead_source)` returns
  `SubscribeError::TornDown` synchronously (not deferred), surfacing
  as `SubscribeOutcome::Dead` to per-op handlers. 8 e2e tests in
  `crates/graphrefly-operators/tests/dead_source_e2e.rs` covering
  zip / concat / race / take_until per-op Dead-source semantics.
  Source: /qa F2 (2026-05-10); resolved 2026-05-10 A/B/D/E/F batch.

- **Mid-fn rewire re-entrancy hazard.** TS impl rejects synchronous re-entry
  via `_inDepMutation`. Rust impl currently only rejects via the cycle check;
  needs explicit re-entrancy guard once `set_deps` is callable from inside an
  `invoke_fn` (today the lock-held discipline forbids it anyway).
- **Cycle-detection perf for high-frequency rewires.** `Core::path_from_to`
  is DFS — fine for typical cases, expensive on dense graphs. Memoize or
  switch to topological-numbering invariant when M3 operators introduce
  high-frequency rewire patterns.

---

## Audit fixes landed in Slice A+B (2026-05-05)

These were surfaced during the Slice A+B audit and resolved as part of the
parity work — recorded here for traceability.

### Dynamic-node rewire fn-fire — FIXED

- **What was broken:** `Core::set_deps` on a Dynamic node cleared `tracked` but
  preserved `has_fired_once`. The deliver gate
  `!has_fired_once || tracked.contains(&dep_idx)` then resolved to false for
  every new dep delivery, leaving the dynamic node on stale cache.
- **Fix:** reset `has_fired_once = false` for Dynamic in `set_deps`. The next
  dep delivery satisfies the first-fire branch unconditionally; fn re-runs and
  the returned `tracked` set repopulates. Regression tests added at
  [`crates/graphrefly-core/tests/setdeps.rs`](../crates/graphrefly-core/tests/setdeps.rs).

### `pending_notify` was `HashMap` — FIXED (now `IndexMap`)

- **What was broken:** `pending_notify: HashMap<NodeId, Vec<Message>>` had
  non-deterministic iteration order, which violated the R1.3.9.d meta-TEARDOWN
  ordering invariant when both the meta and parent had queued messages in the
  same wave (~50% of the time the parent's TEARDOWN flushed before its meta's).
- **Fix:** switched to `indexmap::IndexMap` so flush order = insertion order,
  and the cascade's recursion order naturally puts meta TEARDOWNs first. The
  workspace already depended on `indexmap` so no new dependency.

### `BindingBoundary::retain_handle` was missing — FIXED

- **What was broken:** the §10.2 pause buffer needs handles to outlive the
  cache slot that originally interned them. Without a way for Core to bump
  refcount, `commit_emission`'s `release_handle(old_cache)` would underflow
  the binding-side refcount when a buffered DATA still referenced it.
- **Fix:** added `retain_handle(handle)` to `BindingBoundary` with a
  `{}` default impl. Bindings that track refcounts (`TestBinding`,
  `BenchBinding`) override it. Core calls retain on buffer-push, release on
  drain or overflow-drop. Symmetric with `release_handle`.

### F1: `set_deps` refcount leak on removed `dep_terminals` Error slots — FIXED (QA pass)

- **What was broken:** when a dep was removed via `set_deps`, its
  `dep_terminals[idx] = Some(Error(h))` slot was dropped without releasing
  the per-slot `retain_handle` taken in `terminate_node`. Each rewire-away
  of a terminal-Error dep leaked one refcount share for the error handle.
- **Fix:** in `Core::set_deps`, collect Error handles from removed-dep
  slots before installing the new vecs, then call `release_handle` after
  the wave completes (lock dropped first, consistent with the resume-drain
  binding-cross-boundary pattern). Regression test verifies refcount
  decrease via the new `TestBinding::refcount_of` inspector.

### F2: rewire of terminal node + terminal-non-resubscribable dep — REJECTED via Phase 13.8 Q1 lock (QA pass)

- **What was broken:** `Core::set_deps` had no terminal-state check on `n`
  (the rewiring node) or on newly-added deps. Rewiring a terminal node
  was undefined behavior; rewiring to add a non-resubscribable terminal
  dep would silently inject a stuck-terminal slot.
- **Fix:** new `SetDepsError::TerminalNode { n }` rejected when `n.terminal.is_some()`.
  New `SetDepsError::TerminalDep { n, dep }` rejected when an added dep
  is terminal AND not resubscribable. Resubscribable terminal deps are
  ALLOWED — the subscribe path resets their lifecycle when activated.
  Already-present (kept) deps are not re-checked. Regression tests added.

### F3: resubscribable reset blocked after TEARDOWN — FIXED (QA pass)

- **What was broken:** `reset_for_fresh_lifecycle` cleared
  `has_received_teardown = false`, effectively resurrecting a torn-down
  resubscribable node when a late subscriber arrived. Per R2.6.4, TEARDOWN
  is permanent destruction; only COMPLETE/ERROR is recoverable for
  resubscribable nodes.
- **Fix:** `subscribe`'s reset condition extended to
  `rec.resubscribable && rec.terminal.is_some() && !rec.has_received_teardown`.
  Also extended the handshake to replay TEARDOWN if `torn_down` is true
  — late subscribers to torn-down nodes always see the full `[START,
  DATA?, COMPLETE|ERROR, TEARDOWN]` lifecycle. Regression tests added.

### F4: stale module doc — FIXED (QA pass)

- **What was broken:** `node.rs` module doc's "Out of scope for this slice"
  block listed INVALIDATE / COMPLETE/ERROR / TEARDOWN / Meta TEARDOWN /
  Resubscribable as future work, but Slice A+B landed all of them.
- **Fix:** rewrote the module doc to reflect the closed scope; moved the
  remaining out-of-scope items (deactivation, proptest, batch.rs,
  ReentrantMutex) to a clearly-labeled section. Added cross-links to
  `migration-status.md` and `porting-deferred.md`.

## M3 Slice D — subscription-managed combinator deferrals

Surfaced during /qa adversarial review (2026-05-07).

### D1: TEARDOWN propagation through producers

- **Context:** When a producer receives TEARDOWN from an upstream source, the
  current implementation ignores it (sinks only match Data/Complete/Error).
  TS legacy propagates TEARDOWN through the subscription chain.
- **Risk:** Low — producers manage their own subscription lifecycle via
  `producer_deactivate`. TEARDOWN from upstream is a signal that the upstream
  is permanently dead, but the producer's deactivation already drops
  subscriptions when the last downstream subscriber leaves.
- **Trigger:** Evidence of TEARDOWN-through-producer being observable in a
  parity scenario. Monitor when M4+ lifecycle scenarios land.

### ~~D2: Sink Arc cycle audit~~ — AUDITED 2026-05-07 (no fix needed)

Audit of all four producer ops (`zip` / `concat` / `race` /
`take_until`) and the Slice E higher-order ops (`switch_map` /
`exhaust_map` / `merge_map` / `concat_map`) identified one cycle path:
sink → Core (clone) → CoreState → nodes → upstream.subscribers → sink.

This cycle **breaks correctly via `Subscription::Drop`** — when the
last user-held Subscription on a producer drops, `producer_deactivate`
fires; the binding's `producer_storage[producer_id]` entry drops,
which transitively drops all internal Subscriptions, which removes
all sinks from upstream subscribers maps, which decrements all Core
clones the sinks held. Existing test
`producer_storage_cleared_on_deactivation` in
`tests/subscription.rs` confirms the chain.

The Recorder Weak-capture fix was for a DIFFERENT cycle shape (sink
↔ user-held RecorderInner). Producer-op sinks don't have the
"user-held inner" pattern. **No code change required.**

Practical leak only occurs if user leaks their public-facing
Subscription on the producer node — same shape as any sink that
captures Core. Documented in `producer.rs` module docs as a known
v1 pattern; v2 may switch to `Weak<Core>` upgrade-on-fire.

### D3: Predicate-based kind derivation as an `Option<OperatorOp>`

- **Context:** `NodeRecord::kind()` derives from 4 fields via if/else chain.
  An alternative is to store an `Option<NodeKind>` computed once at
  registration. Current approach is simpler and avoids one more field.
- **Risk:** None — purely an internal API ergonomic question.
- **Trigger:** If `kind()` becomes a hot path in profiling.

### ~~D4: concat phase-0 COMPLETE from second source~~ — RESOLVED 2026-05-07 (D041)

`ConcatState.second_completed: bool` flag tracks whether `second`
emitted Complete during phase 0. On `first` Complete, the phase
transition drains `pending`, then checks `second_completed` and
self-completes lock-released if so. Regression test
`concat_completes_when_second_completes_before_first_in_phase_zero`
in `tests/subscription.rs`. Parity scenario in
`packages/parity-tests/scenarios/operators/subscription.test.ts`
(skipped against TS legacy as a Rust-port-only fix; activates as a
divergence assertion when rustImpl publishes).

### ~~D5: Subscription::Drop race under concurrent unsubscribe~~ — VERIFIED 2026-05-07 (D042)

Loom model-check landed in `crates/graphrefly-core/tests/loom_subscription.rs`
(3 tests: concurrent_drop_of_last_two_subs_fires_deactivate_exactly_once,
concurrent_drop_with_one_remaining_sub_does_not_fire_deactivate,
drop_of_last_sub_on_non_producer_node_does_not_deactivate). Confirms
across all loom-permuted thread interleavings that
`producer_deactivate` fires exactly once when the last two subs are
dropped concurrently. Run via `RUSTFLAGS="--cfg loom" cargo test
--test loom_subscription`. Default-build skips the test (cfg-gated)
so no impact on CI's standard test loop.

The current production impl in `crates/graphrefly-core/src/node.rs`
`impl Drop for Subscription` is correct because the decrement +
"was-last?" check happen under the SAME `parking_lot::Mutex<CoreState>`
acquisition (no TOCTOU window). The loom test is insurance against a
future regression that splits the two operations.

### D6: Parity test coverage gaps

- **Context:** Current parity scenarios cover the happy path (DATA
  forwarding, buffering, phase transitions) but not edge cases:
  zip with 3+ sources, race pre-winner error, concat error during
  phase 0, takeUntil source error, empty zip/race.
- **Risk:** Low — Rust-side unit tests cover these, but cross-impl
  parity assertions would catch behavioral divergences.
- **Trigger:** Expand parity scenarios when rustImpl arm activates.
- **Update 2026-05-07:** Partially closed by D6 widening (Slice E-prep
  /qa cleanup) — added `zip` with 3 sources, `race` no-winner
  all-complete (skipped Rust-port-only), `race` pre-winner ERROR,
  `concat` phase-0 COMPLETE (skipped Rust-port-only D041), `concat`
  phase-0 ERROR, `takeUntil` source ERROR. Remaining gaps: empty
  `zip` / `race` (deferred).

---

## M3 Slice E — higher-order operator deferrals

### ~~Cached-inner handshake re-entry panics in v1~~ — RESOLVED 2026-05-07 (Slice E rework, D045)

Closed by the lock-released handshake rework above. `switch_map` /
`exhaust_map` / `concat_map` / `merge_map` /
`merge_map_with_concurrency` now accept cached inner state nodes
without restriction. Realistic user code (`switch_map(outer, |n|
state(Some(n*10)))`) works transparently.

`tests/higher_order.rs` updated to use the cached-inner pattern
(`state_int(Some(v))`); the previous "uncached workaround" module
doc removed. Closed 2026-05-07.

### DIRTY/RESOLVED forwarding from inner — Rust producer divergence

- **What:** TS legacy `forwardInner` in
  `extra/operators/higher-order.ts` forwards `DIRTY` / `RESOLVED` from
  the inner subscription to the outer output via `a.down([m])`. Rust
  port's `build_inner_sink` drops them silently and relies on Core's
  wave engine to emit DIRTY/RESOLVED on the producer's own emits.
- **Why divergent:** in TS, the outer's `a` is a NodeActions surface
  that lets the user explicitly control wave signals. In Rust, the
  producer pattern's only emission path is `Core::emit` (which itself
  generates DIRTY/Data/Resolved per Core's wave engine). Manual
  forwarding would generate duplicate DIRTY messages.
- **Observable behavior:** matches TS in the standard
  `DATA / COMPLETE / ERROR` cases. Diverges on the rare case where
  inner emits a no-data wave (DIRTY+RESOLVED with no DATA) — Rust's
  outer downstream sees no signal, while TS forwards DIRTY+RESOLVED.
  Practical impact: minimal, since "no-data wave through higher-order"
  is a rare pattern.
- **Lift point:** if a real consumer needs the wave-signal forwarding,
  add `Core::emit_signal(producer, signal)` lock-released method.
- **Source:** Slice E (2026-05-07); documented in `higher_order.rs`
  module doc.
- **Update 2026-05-07 (Slice E /qa, D046 P3):** scope NARROWED. Only
  `DIRTY` / `RESOLVED` / `PAUSE` / `RESUME` / `START` are dropped
  silently. `INVALIDATE` and `TEARDOWN` are now FORWARDED via
  `Core::invalidate(producer_id)` / `Core::teardown(producer_id)`
  respectively (closes the R1.2.7 INVALIDATE forwarding gap and
  propagates inner permanent-destruction via R2.6.4 TEARDOWN).

---

## Audit fixes landed in M3 Slice E /qa (2026-05-07)

These were surfaced by the Slice E /qa adversarial review and resolved
in the same slice (D046) — recorded here for traceability.

### P1 — switch_map `[Data, Error]` same-batch refcount underflow — FIXED

- **What was broken:** `switch_map`'s phase-1 `retain_handle(outer_h)`
  was gated on `!s.terminated`; if `Error` arrived later in the same
  batch (setting `terminated=true` mid-loop), the retain was skipped.
  But phase-2's `release_handle(outer_h)` was unconditional, causing a
  binding-side refcount underflow on the chosen handle.
- **Fix:** `Plan.latest_retained: bool` flag tracks whether phase-1
  performed the retain. Phase-2's project + release block is gated on
  `latest_retained`, so the [Data, Error] same-batch path correctly
  skips both the retain AND the release. Regression test:
  `switch_map_data_then_error_in_same_batch_does_not_underflow_handle`
  asserts the data handle's refcount is preserved across the batch.

### P2 — merge_map / concat_map completed-inner Subscriptions accumulating in producer_storage — FIXED

- **What was broken:** `spawn_inner`'s `on_complete` decremented `active`
  + dequeued next, but never removed the completed inner's
  `Subscription` from `producer_storage[producer_id].subs`. Long-running
  merge_map / concat_map → unbounded `subs.len()` growth.
- **Fix:** moved inner-sub tracking out of `producer_storage.subs`
  into `MergeMapState.inner_subs: HashMap<u64, Subscription>` keyed
  by per-op `next_inner_id`. Each `on_complete` removes its specific
  id (lock-released drop). `producer_storage.subs` now holds only
  the outer source sub (single entry, no positional concerns).
  Regression test:
  `merge_map_does_not_accumulate_completed_inner_subs_in_producer_storage`.

### Cached-outer-source positional ordering bug — FIXED (latent)

- **What was broken:** `ctx.subscribe_to(source, outer_sink)` calls
  `core.subscribe(source, outer_sink)` BEFORE pushing the outer
  Subscription. If `source` is cached, the synchronous handshake
  fires `outer_sink` → `outer_sink` invokes `project` + subscribes
  to inner → `push_inner_sub` lands at `subs[0]` → outer subscribe
  returns → outer sub lands at `subs[1]`. Now `subs = [inner, outer]`.
  Subsequent `drop_inner_subs` truncated to len 1, dropping the
  OUTER sub → operator stalls.
- **Fix:** same refactor as P2 — inner subs no longer live in
  `producer_storage.subs`, eliminating the positional ambiguity.
  Regression test: `switch_map_with_cached_outer_does_not_drop_outer_sub`.

### P3 — INVALIDATE / TEARDOWN forwarding from inner — FIXED

- **What was broken:** `build_inner_sink` only forwarded
  Data/Complete/Error; INVALIDATE / TEARDOWN / DIRTY / RESOLVED /
  PAUSE / RESUME hit `_ => {}`. Real concern: inner cache
  invalidation (R1.2.7) didn't surface to producer subscribers;
  inner permanent destruction (R2.6.4 TEARDOWN) didn't propagate.
- **Fix:** `build_inner_sink` now matches `Message::Invalidate` →
  `Core::invalidate(producer_id)` and `Message::Teardown` →
  `Core::teardown(producer_id)`. DIRTY/RESOLVED/PAUSE/RESUME/Start
  remain dropped (acknowledged divergence — see entry above).
  Regression test: `switch_map_forwards_inner_invalidate_to_producer`.

### Recursive `spawn_inner` stack-overflow risk — FIXED

- **What was broken:** `merge_map`'s `on_complete` recursively called
  `spawn_inner` to drain the next buffered DATA. For pathological
  pre-completed-inner buffers (synchronous Complete during the
  subscribe handshake), recursion depth grew with buffer size →
  stack overflow on 10k+ buffered items with all pre-completed
  inners.
- **Fix:** thread-local `MERGE_DRAIN_ACTIVE: Cell<bool>` guard
  breaks the recursion. The outermost drain owns the loop; nested
  `on_complete` invocations only decrement state and return. The
  outermost loop iterates until the buffer is empty or cap is
  reached. `pending_inner_ids: AHashSet<u64>` distinguishes
  "spawn started, on_complete not yet fired" from "on_complete
  fired during subscribe" — post-subscribe code skips inserting
  the dead sub if the id was already removed.

### Concat first_sink defensive per-iteration `terminated` check — FIXED

- **What was broken:** the outer `if s.terminated || s.phase != 0
  { return; }` guard at the top of the lock block didn't catch a
  `terminated` set BY an earlier message in the SAME batch (e.g.,
  defensively-emitted stale `[Data, Complete, Data]`).
- **Fix:** added per-iteration `if s.terminated || s.phase != 0
  { break; }` inside the `for m in msgs` loop. Defensive only —
  the realistic source contract precludes this.

### Send + Sync compile-time asserts on higher-order state structs — ADDED

- Added a `const _: fn() = || { ... }` block at the bottom of
  `higher_order.rs` that statically asserts `SwitchState`,
  `ExhaustState`, `MergeMapState`, and `ProjectFn` are
  `Send + Sync`. Cheap regression insurance against a future state
  field that accidentally introduces a `!Send` (e.g., `Cell` /
  `Rc<...>`).

### TS parity scenarios — `test.skip` → `test.runIf` for Rust-port-only assertions — FIXED

- **What was broken:** the two Rust-port-only divergence assertions
  (concat-phase-0 self-complete, race no-winner all-complete) used
  static `test.skip(...)` which would NEVER activate even when
  `rustImpl` publishes — the tests were dead until manually
  unskipped.
- **Fix:** replaced `test.skip(...)` with
  `test.runIf(impl.name !== "pure-ts")(...)`. The
  assertion now activates for any non-legacy impl (including
  rustImpl once it publishes), so divergences become loud failures
  instead of silent skips.

## QA-surfaced divergences (Slice X1+Y+X2+X3 /qa, 2026-05-08)

### `Graph::edges` doubles the walk via `collect_qualified_names` + `edges_inner`

- **What:** Slice Y's cross-graph edges fix introduced a global names_map pre-computation pass (`collect_qualified_names`) that walks the entire mount tree before `edges_inner` walks it again. For deep mount trees this is O(2N) lock acquisitions on the per-graph `inner: Mutex<GraphInner>`. Both walks acquire each graph's `inner` lock, do work, and drop. Single-thread-safe (parent → child ordering preserved); concurrent edges() calls on overlapping subtrees deadlock-free; but reactive describe under namespace-thrash can serialize.
- **Why deferred:** correctness is fine; performance regression on graphs with many mounts only. Pre-Slice-Y was single-pass but didn't resolve cross-graph deps. The fix prioritized correctness over perf.
- **Lift point:** restore single-pass via `&mut IndexMap` accumulator threaded through `edges_inner`. Build names_map AS we walk, propagate up via the accumulator. ~30 LOC refactor. Add a bench or fast-check property if reactive describe under namespace-thrash shows up in a profile.
- **Source:** Slice X1+Y+X2+X3 /qa (2026-05-08), Edge Case Hunter F9.

### `BenchGraph::derived` registry-then-Core lock-ordering hazard

- **What:** `BenchGraph::derived` does `binding.registry.lock() → core.register_derived(...)` which acquires `Core::state::lock()`. Concurrent paths that lock in the OPPOSITE order — e.g., a tokio thread mid-wave that holds Core state + calls back into `BindingBoundary::retain_handle` (which locks `binding.registry`) — deadlock by AB-BA lock ordering.
- **Why deferred:** parity-tests are single-threaded; not exercised today. Slice Y's harness/multi-agent use case explicitly drives concurrent registration but the parity-tests stop short of cross-thread register-vs-emit scenarios.
- **Lift point:** document `registry < core_state` as the canonical lock order; either restructure `BenchGraph::derived` to release `registry` lock before calling into Core, OR add a debug-build lock-order tracker. The single-LOC `Core::binding_ptr()` accessor (F14 lift-point) would also enable per-call invariant checks.
- **Source:** Slice X1+Y+X2+X3 /qa (2026-05-08), Edge Case Hunter F14.

### `WeakCore::upgrade` partial-drop window across 3 independent Arcs

- **What:** `WeakCore { state: Weak, binding: Weak, wave_owner: Weak }` upgrades each independently. `BenchCore::Drop` drops fields in declaration order (`subscriptions`, `binding`, `core`). Between dropping `binding` and `core`, a producer-build closure firing concurrently from another thread could see `state.upgrade().is_some()` and `binding.upgrade().is_none()` — `WeakCore::upgrade` returns `None` correctly here, so it's not a corruption risk. But the converse: a closure that already cloned `Core` from a prior successful upgrade holds 3 strong Arcs across an unwind path that drops one Arc independently could see misaligned references.
- **Why deferred:** theoretical race; the Drop ordering and clone-from-upgrade pattern make actual misalignment hard to construct. Bench / parity-tests don't trigger it.
- **Lift point:** restructure to `Arc<CoreInner>` (single allocation holding state + binding + wave_owner) and `Weak<CoreInner>` for one-step upgrade. Larger refactor (touches every `Core` clone site); requires `state`, `binding`, `wave_owner` field access through a single Arc. ~100 LOC.
- **Source:** Slice X1+Y+X2+X3 /qa (2026-05-08), Edge Case Hunter F6.

### F14 `Core::binding_ptr()` accessor for cross-crate invariant assert

- **What:** F14's deferred lift-point ("debug-build `Arc::ptr_eq` assert in `BenchGraph::from_core`") couldn't be expressed because `Core::binding` is `pub(crate)` in `graphrefly-core` (different crate from `graphrefly-bindings-js`). A single-LOC accessor `pub fn Core::binding_ptr() -> *const dyn BindingBoundary` (or `*const ()` thin pointer) would unblock the assert.
- **Why deferred:** cosmetic; the construction invariant (`BenchCore::new` is the single path that builds the binding) holds today by inspection. F14 is now documented in `graph_bindings.rs` doc comment.
- **Lift point:** add the accessor whenever a downstream refactor would benefit, then add `debug_assert_eq!` in `BenchGraph::from_core`. Same accessor would also enable the `BenchGraph::derived` lock-order debug-tracker (above entry).
- **Source:** Slice X1+Y+X2+X3 /qa (2026-05-08), follow-on to F14 close note.

## QA-surfaced divergences (Slice H /qa, 2026-05-07)

The /qa pass on Slice H surfaced three concerns that were NOT fixed in
the slice itself. Each captured here with what / why-deferred / source.

### F3 — `reset_for_fresh_lifecycle` calls `make_op_scratch` lock-held

- **What:** Slice H's `Core::register` was explicitly restructured to
  take operator-scratch handle retains LOCK-RELEASED ("Phase 2 —
  lock-released per Slice E (D045) handshake discipline" — comment in
  `node.rs`). The matching code path in `Core::reset_for_fresh_lifecycle`
  (resubscribable terminal cycle) calls `make_op_scratch(prev_op)`
  (which calls `binding.retain_handle(seed)`) AND
  `scratch.release_handles(...)` while holding the state-lock guard.
- **Why deferred:** pre-existing v1 lock-discipline divergence. Slice H
  touched the function signature only (added `.expect()` to the now-
  `Result`-returning `make_op_scratch`); it did not change the lock
  scope. Real fix: refactor `reset_for_fresh_lifecycle` to drop the
  state lock, call `make_op_scratch` lock-released, then re-acquire the
  lock to install + queue handle releases via the existing
  `deferred_handle_releases` queue (Slice F audit infra). Mirrors the
  three-phase pattern Slice H landed in `register`.
- **Lift point:** schedule as part of any future "lock-discipline
  audit" slice that walks all `binding.{retain_handle, release_handle,
  invoke_fn, custom_equals, ...}` callers and pushes lock-released
  invocation across the board. Existing porting-deferred entries
  ("Audit fixes landed in Slice A+B" §10.10 family) already cover most
  of the surface; this one slipped because it's behind a
  resubscribable-terminal-reset code path that few tests exercise.
- **Source:** Slice H /qa Edge Case Hunter F3 (2026-05-07).

### F11 — Phase 3 closure walks deps twice in `Core::register`

- **What:** Slice H's `Core::register` Phase 3 (now folded with
  insertion under a single `lock_state()` per /qa F1) walks `deps`
  twice — first to check `s.nodes.contains_key(&dep)`, then to check
  `dep_rec.terminal && !dep_rec.resubscribable` via `s.require_node(dep)`.
  `require_node` uses an internal panic on missing key, so the first
  loop's job is to pre-validate before the second loop calls into it.
- **Why deferred:** functionally correct but redundant work. A single
  loop using `s.nodes.get(&dep)` and matching on `Option<&NodeRecord>`
  would avoid the double iteration AND avoid `require_node`'s panic
  path (which is a code-smell for a validating function). Cleanup
  opportunity, not a correctness fix.
- **Lift point:** opportunistic — fold into any future register
  refactor or a small "code-cleanup" pass.
- **Source:** Slice H /qa Blind Hunter F11 (2026-05-07).

### F17 — Asymmetric `UnknownNode` typed-error surface

- **What:** Slice H widened `Core::set_pausable_mode` to surface
  `UnknownNode` as a typed error variant (per user direction Q3=widen).
  The other `require_node_mut` callers in `Core` (`set_resubscribable`,
  `complete`, `error`, `add_meta_companion`, `pause` / `resume`-via-
  `up`, etc.) still panic on unknown ids. Public surface is now
  asymmetric: a JS binding that catches `set_pausable_mode` errors
  gracefully will still abort the process on `complete(unknown_id)`.
- **Why deferred:** out of Slice H scope (the slice scope per
  porting-deferred D3 was specifically `register*` + `set_pausable_mode`).
  Extending to all `require_node_mut` callers needs its own design call:
  which methods get widened (e.g., `complete` / `error` are
  user-facing terminal-emit methods — strong case for typed errors;
  `add_meta_companion` is binding-internal), and what the unified
  error surface looks like. Could reuse a single `NodeRefError ::
  UnknownNode` shared across all the methods, or keep per-method enums.
- **Lift point:** schedule as "Slice H follow-on — widen `UnknownNode`
  typed-error surface to remaining `require_node_mut` callers" if and
  when binding-side error UX consistency becomes a real consumer
  concern.
- **Source:** Slice H /qa Blind Hunter F12 (2026-05-07).

## M3 napi-rs operator parity — carry-forward (Phases D + E)

Phases A (TSFN substrate + worker-thread Core), B (`BenchOperators` napi class + 22 register methods), C (custom-equals bundle) landed 2026-05-07. Phases D + E carry forward to follow-on slices.

### ~~Phase D — three-bench shape (D049) deferred~~ — RESOLVED 2026-05-08 (Slice X1)

Bench harness shipped at `~/src/graphrefly-ts/packages/pure-ts/src/__bench__/operators-via-tsfn.bench.ts`. Three bench groups: `builtin_fn` (pure-FFI baseline) + `tsfn_identity` (TSFN scheduling) + `tsfn_addone_js` (end-to-end Rust-via-TSFN-into-JS with deref+intern). All three use top-level await for setup (vitest bench mode does NOT run `beforeAll` — convention from `graphrefly.bench.ts:5`) and `await emitInt` for end-to-end measurement (the legacy `ffi-cost.bench.ts` fires-and-forgets emit, which only measures Promise scheduling). Subtraction interpretation: `(2)−(1) = TSFN scheduling cost`; `(3)−(2) = JS compute + 2× sync FFI`; `(3)` is the headline.

Cold-tokio warmup is per-`describe` (each block builds a fresh BenchCore); the first bench runs slightly cold relative to subsequent ones. Acceptable for the substrate-validation scope of Phase D; if the §10 perf-tier work needs tighter numbers, lift to shared-Core warmup.

### ~~TSFN err-first wire-vs-typed signature mismatch (Phase D finding, 2026-05-08)~~ — RESOLVED 2026-05-08 (Slice Y, same day)

User direction 2026-05-08: "Fix this in this batch because otherwise parity tests are broken. Do the ultimate fix." Bundled into Slice Y rather than deferring.

**What landed:** flipped `CalleeHandled = true` → `false` across all 7 TSFN sites:
- `operator_bindings.rs::Tsfn<T, R>` type alias (final const generic flipped).
- `operator_bindings.rs::build_h_to_h_tsfn` / `build_h_to_bool_tsfn` / `build_hh_to_h_tsfn` / `build_hh_to_bool_tsfn` / `build_vec_to_h_tsfn` (5 builders, all `.callee_handled::<false>()`).
- `core_bindings.rs::SinkTsfn` type alias + `build_sink_tsfn`.
- `core_bindings.rs::ReleaseTsfn` type alias + `release_callback` site at line ~456.

**Side-concern landed too:** `bridge_sync` and `bridge_sync_unit` updated to call `tsfn.call_with_return_value(arg, ...)` (plain `T`, not `Ok(arg)`) per napi-rs 3.x's `CalleeHandled=false` impl signature. The `FnOnce(Result<Return>, Env) -> Result<()>` cb signature is identical between the two `CalleeHandled` impls, so JS-throw delivery via `Result<R>` is preserved — `bridge_sync`'s panic-on-throw discipline (line 209-212) still works.

**Empirical validation:** the rustImpl parity arm activated for the first time (`pnpm --filter @graphrefly/native build` produced `graphrefly-native.darwin-arm64.node`; `pnpm --filter @graphrefly/parity-tests test` ran 138 tests). Result: **118 passed, 0 failed, 16 skipped, 4 todo.** All operator scenarios (transform 16, combine 12, flow 22, higher-order 18, subscription 32) pass against rustImpl AND pureTsImpl. The 4 originally-failing `g.derived(name, deps, fn)` sites are now `test.runIf(impl.name !== "rust-via-napi")` gated per the F9 carry-forward.

### Closure-builder Arc-cycle (binding-side production parallel of producer-build cycle)

Slice Y discovered (during the broader cycle audit prompted by user's "harness use case" question) that **3 closure builders in `operator_bindings.rs` had the same cycle pattern as producer-builds**, on top of the 7 producer-build sites already fixed:

- `closure_h_to_h(tsfn, binding)` — used by `register_map`. Stored long-term in `binding.registry.projectors`.
- `closure_hh_to_h(tsfn, binding)` — used by `register_scan` / `register_reduce` / `register_pairwise` / `register_distinct_until_changed_with`. Stored in `binding.registry.folders` / `pairwise_packers`.
- `closure_packer(tsfn, binding)` — used by `register_combine` / `register_with_latest_from` / `register_zip`. Stored in `binding.registry.packers`.

Each captured `binding: Arc<BenchBinding>` strong, creating cycle: `BenchBinding → registry.{projectors|folders|packers}[fn_id] → closure → strong Arc<BenchBinding>` — same shape as the producer-builds cycle.

**Fix:** change closure builder signatures from `binding: Arc<BenchBinding>` to `binding: Weak<BenchBinding>` and upgrade-on-fire (defensive `panic!` on dangling weak — unreachable in practice since closure storage lives inside the binding's own registry). All 7 call sites changed from `Arc::clone(&self.binding)` to `Arc::downgrade(&self.binding)`.

**Why it didn't surface in tests pre-Slice-Y:** the rustImpl parity arm wasn't activated, and the bench's `OpRuntime::Drop` happens at process exit so the leak doesn't accumulate across test runs.

`closure_h_to_bool` / `closure_hh_to_bool` / `closure_h_to_nodeid` don't capture binding — no cycle, no fix needed.

(Continued under main Slice Y close section.)

### Phase E — `parity-tests/impls/rust.ts` activation deferred

- **What:** D053 locks activate-and-triage rollout for `parity-tests/impls/rust.ts`. The napi binding ships with the operator surface, but the JS-side adapter that wraps `BenchCore` / `BenchOperators` into the high-level `Impl` shape (`impl.node([], { initial, name })`, `impl.map(src, fn)`, `node.subscribe(...)`, `node.down([[DATA, v]])`, etc.) is substantial wrapper code (~200–400 LOC) that hasn't shipped yet.
- **Why deferred:** the adapter is its own conceptually-independent block of work — it's a JS-side "rebuild the high-level reactive-graph API on top of a u32-NodeId / u32-HandleId interface." Doing it inline would push this slice past the typical close size. Better to land Phases A–C as a clean reviewable unit + activate the parity arm in a follow-on slice scoped specifically to the adapter shape.
- **Lift point:** follow-on slice "M3 napi-rs operator parity — Phase E (rustImpl activation + triage)". Steps: (1) build the napi binding via napi-cli into a publishable `@graphrefly/native` shape; (2) write `rust.ts` adapter mapping `Impl` methods to `BenchCore` / `BenchOperators`; (3) flip `rustImpl` non-null; (4) run `pnpm --filter @graphrefly/parity-tests test` against both arms; (5) triage Rust-port-only divergences with `test.runIf` markers + porting-deferred entries.
- **Source:** Phase A–C close (2026-05-07); session doc `archive/docs/SESSION-rust-port-napi-operators.md` §5 Phase E.

### ~~napi-rs 2.x → 3.x bump deferred~~ — RESOLVED 2026-05-07 (D072)

Bumped to clean napi 3.x (no compat-mode) with per-crate `forbid` → `deny` carve-out for `unsafe_code` in `bindings-js/src/lib.rs`. Native 3.x TSFN cb signature `FnOnce(Result<Return>, Env) -> Result<()>` delivers JS-callback throws as `Err` to our cb (no fatal abort) — C1 fixed by design. `Function<Args, Return>` typed callbacks, `Env::spawn_future` returning `PromiseRaw<'env, T>`, builder-pattern TSFN setup. Wrapper helpers from D071 deleted.

### v1 limitation: JS callbacks should `await` (not sync-call) re-entries into `BenchCore` / `BenchOperators`

- **What:** Per D070's Option E architecture, the tokio blocking-pool thread executing the wave is parked inside a TSFN-bridge `mpsc::sync_channel` `recv` while a JS callback (project / predicate / fold / pack / equals / higher-order project) executes on the JS thread. If the JS callback synchronously **awaits** a nested `BenchCore.emit(...)` / `BenchOperators.register_*(...)` call, the nested napi method runs on a DIFFERENT tokio blocking thread (tokio's pool defaults to 512 threads); JS yields to libuv during the await; libuv pumps the original TSFN's result-handler; original tokio thread continues. **No deadlock under await.** The deadlock vector is JS callbacks that try to call BenchCore methods **synchronously without await** — but napi-rs methods return Promises now, so JS can't synchronously call them anyway (calling a Promise-returning function without await just gets a pending Promise, no JS-side wait).
- **Why deferred:** v1 acceptable. The Option E architecture (D070) makes this a non-issue in practice — Promise-returning napi methods naturally compose with await semantics. Original D062's deadlock concern (sync mpsc on JS thread) is gone.
- **Lift point:** N/A — Option E fixes this by construction.
- **Source:** Phase A close (2026-05-07); QA-pass D070 supersession.

### ~~v1 limitation: `BenchCore::Drop` can deadlock if waves are in-flight~~ — RESOLVED 2026-05-08 (Slice Y)

`BenchCore::dispose() -> Promise<void>` napi method shipped in [`core_bindings.rs`](https://github.com/graphrefly/graphrefly-rs/blob/main/crates/graphrefly-bindings-js/src/core_bindings.rs) and declared in `index.d.ts`. JS code MUST `await core.dispose()` before letting BenchCore drop. Inside, the entire `subscriptions: Vec<Option<Subscription>>` is taken via `mem::take` on the calling thread (cheap pointer swap) and shipped into `run_blocking` for drop on a tokio blocking thread (which is allowed to block on Core's mutex while the JS thread stays free to pump libuv). Idempotent — subsequent calls are no-ops on the empty vec.

JS adapter rewiring (rust.ts `g.destroy()` chains through `dispose()`) lands in a follow-on slice — the Rust-side method is now in place.

### ~~v1 limitation: Arc-cycle leak per `BenchCore` instance~~ — RESOLVED 2026-05-08 (Slice Y)

Producer-build closure cycle structurally broken via `Weak<dyn ProducerBinding>` + `WeakCore` captures. New `Core::weak_handle() -> WeakCore` API in [`graphrefly-core::node`](https://github.com/graphrefly/graphrefly-rs/blob/main/crates/graphrefly-core/src/node.rs); `WeakCore::upgrade() -> Option<Core>` upgrades to a strong handle if the host hasn't dropped yet. Applied across 7 producer-pattern factory sites:

- `crates/graphrefly-operators/src/ops_impl.rs` — `zip` (lines ~83), `concat` (~262), `race` (~485), `take_until` (~618).
- `crates/graphrefly-operators/src/higher_order.rs` — `switch_map` (~222), `exhaust_map` (~470), `merge_map_with_concurrency` (~755). `concat_map` is sugar over `merge_map_with_concurrency(.., Some(1))` and inherits the fix.

Build closures capture weak refs and upgrade once at activation; if the host Core dropped between registration and build-time, `upgrade().is_none()` and the closure no-ops cleanly. Sub-closures (sinks) capture strong refs cloned from the upgraded weaks — short-lived, dropped via `producer_deactivate` lifecycle on last-subscriber unsubscribe.

Test fixture's `OpRuntime::make_packer` (`crates/graphrefly-operators/tests/common/mod.rs`) had a parallel cycle pattern (test-only; production napi `closure_packer` captures TSFN, not binding). Fixed in same slice via `Weak<InnerBinding>` capture for consistency.

Verified by 8 new regression tests in `crates/graphrefly-operators/tests/arc_cycle_break.rs` — each producer factory + a combined "many producers" scenario. Each test captures `Weak<InnerBinding>` before runtime drop and asserts `weak.upgrade().is_none()` after drop, proving the binding's strong count hits 0.

`producer.rs` doc comment "Reference-cycle note (v1 limitation)" rewritten to "Reference-cycle discipline (Slice Y, 2026-05-08)" describing the shipped weak-Arc pattern.

**Workspace test count:** 461 → 469 (+8 cycle-break tests). `cargo clippy --all-targets -D warnings` clean. `cargo fmt --check` clean. `#![forbid(unsafe_code)]` preserved.

### v1 limitation: BigInt HandleId / NodeId for unbounded handle space (D064 follow-up)

- **What:** Per D064, the napi binding narrows `HandleId(u64)` and `NodeId(u64)` to `u32` at the FFI boundary (matching existing `BenchCore::register_state_int → u32` convention). Long-running production processes that exhaust the 4-billion-handle space within a single `BenchCore` will overflow at `u32::try_from(...).expect("...")` sites.
- **Why deferred:** bench / parity-test workloads don't hit this. Production users that do can either (a) restart the BenchCore, or (b) wait for the BigInt migration.
- **Lift point:** swap `u32` napi types for `napi::JsBigInt` across the BenchCore + BenchOperators surface. Carries per-call BigInt boxing cost. Bench evidence justifies.
- **Source:** D064 close (2026-05-07).

### v1 limitation: per-fire JS-callback throw isolation (D065 follow-up)

- **What:** Per D065, JS-callback throws panic at the FFI boundary, propagating to the worker thread's wave engine which panic-discards the entire in-flight wave via `clear_wave_state`. Multiple operator callbacks firing in the same wave that throw individually all collapse the wave; there's no per-fire isolation.
- **Why deferred:** matches D060's cleanup-closure discipline (Core stays panic-naive about user code). Per-fire isolation requires either (a) `catch_unwind` per `project_each` / `predicate_each` / `fold_each` invocation in `BindingBoundary` impl (impacts perf — `UnwindSafe` bounds), or (b) restructuring Core's wave engine to checkpoint per-fire (substantial refactor).
- **Lift point:** when production workloads surface specific need for per-fire isolation. Until then, the wave-discard granularity is acceptable.
- **Source:** D065 close (2026-05-07).

## Audit fixes landed in Slice H /qa (2026-05-07)

These were surfaced by Slice H /qa adversarial review and resolved
in-slice rather than deferred:

### F1 + F2 — TOCTOU race + panic-unsafe scratch leak — FIXED

- **What was broken (F1):** the original Slice H restructured
  `Core::register` into three phases with a closure-scoped
  `lock_state()` for Phase 3 validation, then a fresh `lock_state()`
  for Phase 4 insertion. Pre-Slice-H code held the lock continuously
  across both phases. The split opened a TOCTOU window where a
  concurrent `Core::complete(dep)` on a non-resubscribable dep on
  another `Core` clone could slip in between the two lock acquisitions,
  recreating the wedge `RegisterError::TerminalDep` was designed to
  prevent.
- **What was broken (F2):** `OperatorScratch` impls have no `Drop` impl
  that calls `release_handles`. Any panic between `make_op_scratch`
  (Phase 2) and the explicit `if let Err(e)` cleanup branch (e.g.,
  `lock_state()` reentrance assert, OOM-as-panic on Vec growth in dep
  iteration) would drop the `Box<dyn OperatorScratch>` without
  releasing the seed/default retain.
- **Fix:** Introduced `ScratchReleaseGuard<'a>` RAII wrapper in
  `node.rs`. Wraps the scratch immediately after Phase 2; on early
  return / panic, the guard's `Drop` calls `release_handles` lock-
  released (variable destruction order: guard declared BEFORE
  `lock_state()`, so the `MutexGuard` drops first). On the success
  path, `scratch_guard.take()` disarms the guard and hands ownership
  to `NodeRecord.op_scratch`. Phase 3 + Phase 4 now share a SINGLE
  `lock_state()` acquisition — closes the TOCTOU window and gives the
  guard a single LIFO scope to land in.

### F4 — `Last { default }` early-return refcount discipline test — ADDED

- **What was broken:** the original Slice H tests covered Scan-seed
  refcount discipline (UnknownDep) and Reduce-seed (TerminalDep), but
  not Last-default. A regression that dropped Last from the early-
  release branch would not be caught.
- **Fix:** Added
  `register_operator_last_with_default_with_unknown_dep_does_not_leak_default`
  in `tests/slice_h.rs` mirroring the Scan/Reduce variants.

### F5 — `register_err_does_not_add_node_to_graph` tightened — FIXED

- **What was broken:** the test discarded the `Result` variant via
  `let _ = ...` and asserted only `node_count` unchanged. A regression
  that silently `Ok`'d would still pass the test.
- **Fix:** test now binds the result and asserts
  `Err(RegisterError::UnknownDep(bogus))` AND `node_count` unchanged.

### F6 — error-precedence test — ADDED

- **What was broken:** Phase 1 (`OperatorWithoutDeps`) vs Phase 2
  (`OperatorSeedSentinel`) ordering was implicit. A future refactor
  swapping the checks would silently change the variant returned for
  `register_operator(&[], OperatorOp::Scan { seed: NO_HANDLE, .. })`.
- **Fix:** added `op_without_deps_takes_precedence_over_seed_sentinel`
  in `tests/slice_h.rs` pinning Phase 1 → Phase 2 ordering.

### F7 — operator factory `assert!`s promoted to typed `OperatorFactoryError` — FIXED

- **What was broken:** factory-layer asserts in `combine.rs::combine`,
  `combine.rs::merge`, and `flow.rs::last_with_default_with` panicked
  on bad inputs (empty sources, NO_HANDLE default), creating a
  surface inconsistency: `Core::register*` returned typed errors,
  but the factories above panicked.
- **Fix:** new `crates/graphrefly-operators/src/error.rs` with
  `OperatorFactoryError::{EmptySources, ZeroDefault, Register(_)}`.
  `From<RegisterError>` derive makes `register_operator(...)?`
  propagation transparent. `combine` / `merge` / `merge_as_op` /
  `last_with_default` / `last_with_default_with` all return
  `Result<_, OperatorFactoryError>`. 9 new regression tests in
  `tests/slice_h_factory_errors.rs` cover both factory-layer variants
  AND `From<RegisterError>` propagation.

### F8 — Last-shaped `.expect` messages tightened — FIXED

- **What was broken:** `last_with` and `last_with_default_with` used a
  generic invariant message mentioning "seed" — but `Last` has
  `default`, not `seed`.
- **Fix:** wording now says "default" not "seed" for Last variants.

### F9 — `Eq` derive added to `RegisterError` / `SetPausableModeError` — FIXED

- **Fix:** zero-cost — both enums now derive `Eq` so consumers can use
  them as `HashSet`/`HashMap` keys.

### F10 — stray `FnResult` import in test file — FIXED

- **Fix:** dropped `FnResult` from `tests/slice_h.rs` imports + the
  unused-suppression sentinel.

### F12 — `# Errors` rustdoc cites phase numbers — FIXED

- **Fix:** `Core::register`'s `# Errors` block now annotates each
  variant with its phase (Phase 1: lock-released, Phase 2: scratch,
  Phase 3: state-lock validation) so the doc matches the runtime check
  order.

### F13 — `make_op_scratch` Box-before-retain ordering — FIXED

- **What was broken:** for Scan/Reduce/Last, `binding.retain_handle`
  ran BEFORE `Box::new`. If `Box::new` panicked (OOM-as-panic), the
  retain was taken with no Box to anchor it — leak.
- **Fix:** allocate the state struct first, then call
  `retain_handle`. Defense against alloc panics.

### F14 — `reset_for_fresh_lifecycle` `expect` message clarified — FIXED

- **Fix:** message no longer claims "seed was validated" (misleading
  for `Last { default: NO_HANDLE }` which has no seed); now reads
  "stored OperatorOp passed make_op_scratch validation at registration
  time."

### F15 — `RegisterError` rustdoc mentions Last default retain — FIXED

- **Fix:** top-level `RegisterError` docstring now explicitly notes
  that `Last { default }` retains its `default` handle on the same
  release path as Scan/Reduce seeds.

### F16 — `set_pausable_mode_on_unknown_node_errors` test pinned no-mutation — FIXED

- **Fix:** test now sets a known node's mode to `Off`, attempts to
  change a bogus id (gets `UnknownNode` as expected), then re-sets the
  known node's mode to `Off` (succeeds, proving no partial state
  mutation happened on the failed call).

---

## Phase E rustImpl activation — carry-forward divergences (D073–D077, 2026-05-07)

### ~~BenchGraph reactive methods (`describe_reactive` / `observe_all_reactive` / `edges` reactive)~~ — RESOLVED 2026-05-08 (Slice X2 / Phase E2)

`BenchGraph::describe_reactive` + `observe_all_reactive` + `observe_subscribe` (sink-style with optional path) shipped in [`crates/graphrefly-bindings-js/src/graph_bindings.rs`](https://github.com/graphrefly/graphrefly-rs/blob/main/crates/graphrefly-bindings-js/src/graph_bindings.rs). Two new RAII handle napi classes (`BenchDescribeReactiveHandle` + `BenchObserveReactiveHandle`) with async `dispose()` per the Slice Y dispose-on-tokio discipline. Both TSFNs use `callee_handled::<false>()` matching the Slice Y convention.

"Edges reactive" was actually subsumed by `describe_reactive` — each snapshot includes the edges, so consumers reading edges from successive describe snapshots get the reactive edge stream for free. No separate `edges_reactive` method needed.

JS adapter widened `Impl.Graph` with unified `observe(path?, opts?)` per canonical R3.6.2 + `describe(opts?)` for the reactive variant. **+11 parity tests** in `describe-reactive.test.ts` + `observe-all-reactive.test.ts`; 133 parity tests passing total (was 122).

### Original entry (kept for archive)

- **What:** `Graph::describe_reactive`, `Graph::observe_all_reactive`,
  and the reactive variant of `edges({reactive: true})` exist in
  `graphrefly-graph` (Slice F) but are NOT wrapped in `BenchGraph` for
  this slice. The corresponding parity scenarios under
  `packages/parity-tests/scenarios/graph/describe-reactive.test.ts`,
  `observe-all-reactive.test.ts`, and the reactive halves of
  `edges.test.ts` are `test.skip`'d for both impls.
- **Why deferred:** wrapping the reactive variants requires (a)
  exposing `Graph::describe_reactive`'s `ReactiveDescribeHandle` over
  napi (returns a dispose handle + a Node), (b) wiring Producer-style
  nodes that emit on graph mutations, (c) a TSFN-backed sink path on
  the JS adapter side. The static surface (`describeJson`,
  `edges(recursive)`) ships in this slice; the reactive surface is its
  own scope.
- **Lift point:** new follow-on slice "Phase E2 — BenchGraph reactive
  surface". Steps: (1) extend `BenchGraph` with `describe_reactive`
  returning a struct holding `(node_id: u32, dispose_fn_id: u64)`; (2)
  same for `observe_all_reactive` + `edges_reactive`; (3) JS adapter
  exposes `g.describe({reactive: true})` returning `{ node, dispose
  }` matching the legacy shape; (4) re-enable the skipped scenarios.
- **Source:** Phase E close (2026-05-07).

### `Graph.derived(name, deps, fn)` with arbitrary JS fn

- **What:** `BenchGraph::derived` only accepts the built-in `BuiltinFn`
  enum (`Identity` / `AddOne`). Tests calling `g.derived(name, deps,
  (data) => [...])` with arbitrary JS callbacks throw with a clear
  diagnostic from `rust.ts`. The workaround for tests: build the
  derived node via `BenchOperators` (e.g., `impl.map(src, fn)`) and
  attach via `g.add(name, node)`.
- **Why deferred:** wiring TSFN-backed derived fn into BenchGraph's
  Graph::derived path requires either widening `BenchGraph` with a
  TSFN-receiving variant OR routing through `Core::register_derived`
  with binding-side fn registration. Both are non-trivial. Most
  parity scenarios that use `g.derived` can be rewritten to use
  `g.add(name, impl.map(src, fn))` shape — that's the pattern the
  Rust port encourages anyway (operators are first-class; arbitrary
  derived fns are deemphasized).
- **Lift point:** Phase E2 follow-on; OR rewrite parity scenarios
  that currently use `g.derived(name, deps, fn)` to use the operator
  factory + `g.add` pattern.
- **Source:** Phase E close (2026-05-07).

### `Graph.mount(name, child)` with pre-built foreign child

- **What:** `BenchGraph::mount_new(name)` creates a child sharing the
  same Core. Mounting a pre-built foreign child (`g.mount(name,
  child)`) is rejected with a diagnostic from `rust.ts`.
- **Why deferred:** cross-Core mount has structural questions
  (handle-space disjointness across BenchCore instances) that aren't
  resolved in the Rust port. Single-Core mount via `mount_new` covers
  the common case and matches the M2 Slice E+ semantics already
  present in `graphrefly-graph`.
- **Lift point:** if a parity scenario specifically requires foreign-
  child mount, address case-by-case. Current scenarios that use
  `g.mount(name)` (no child) work fine.
- **Source:** Phase E close (2026-05-07).

### Error-payload identity for non-i32 values

- **What:** `BenchCore::error_int(node, err_code: i32)` is the only
  error-emit path on the napi binding. Tests that emit ERROR with
  arbitrary `T` payloads (e.g., `s.error("not found")`) lose payload
  identity through the rust adapter — the JS-side mirror has the
  value but the napi binding emits a sentinel `0` error code.
- **Why deferred:** adding `BenchCore::error_handle(node, handle)`
  taking a JS-allocated handle is straightforward (~10 LOC mirroring
  `emit_handle`). Tests that depend on error-payload identity through
  rustImpl will surface during activation triage.
- **Lift point:** add `BenchCore::error_handle` napi method when a
  parity scenario surfaces the gap. The `RustNode.error(value)` impl
  in `rust.ts` already pre-allocates the handle; only the binding
  call needs to be widened.
- **Source:** Phase E close (2026-05-07).

### `RustNode.allocLockId` semantics

- **What:** `RustNode.allocLockId()` calls `BenchCore::alloc_lock_id`
  which allocates from a high (`1<<32`+) range to avoid collision with
  user-supplied LockIds (per Slice F A4 fix). Legacy uses a process-
  local counter starting at 0/1. Tests that compare lock-id values
  numerically across impls will diverge.
- **Why deferred:** lock-id values are opaque per spec; tests should
  not assume specific numeric values. If a parity scenario does
  numerical comparison, address by widening the test to accept either
  range OR by aligning the legacy counter starting point.
- **Lift point:** triage individual scenarios as they surface.
- **Source:** Phase E close (2026-05-07).

### Cross-platform `.node` artifacts not built / published

- **What:** This slice ships the `@graphrefly/native` package shape,
  napi-cli config, and `package.json` declaring the per-platform
  `optionalDependencies` matrix. CI matrix to build artifacts per
  platform lands separately (Commit 5 of this slice). NPM publish flow
  is a release-engineering follow-on (D075).
- **Why deferred:** parity-test activation requires only host-platform
  build (`pnpm --filter @graphrefly/native build` from local toolchain).
  Cross-platform publish is downstream of 1.0 ship-readiness.
- **Lift point:** when 1.0 ship engineering picks up; coordinate with
  `@graphrefly/pure-ts` publish cadence + npm credentials in CI.
- **Source:** Phase E close (2026-05-07).

### Post-1.0 follow-on: BigInt migration for u32-narrowed napi types

- **What:** The napi binding currently narrows `HandleId(u64)`, `NodeId(u64)`, `LockId(u64)`, `FnId(u64)` to `u32` at the FFI boundary (D064 — matches existing `BenchCore::register_state_int → u32` convention). Each method that crosses the boundary does `u32::try_from(raw).expect(...)` or `.map_err(...)`, which silently caps the usable space at 4 billion handles per process.
- **Why deferred:** Bench / parity-test workloads don't approach 2^32. Phase E /qa F1 surfaced a concrete cost — `alloc_lock_id`'s seed had to drop from `1u64 << 32` to `1u64 << 31` to keep the napi round-trip working, halving the high-range ceiling. The same constraint applies to every other u64 type that crosses napi.
- **Lift point:** **after the Rust port closes** (post-M5 or post-1.0 — user direction 2026-05-08). At that point, sweep all `u32::try_from` sites in `crates/graphrefly-bindings-js/src/{core_bindings,operator_bindings,graph_bindings}.rs` and migrate to `napi::bindgen_prelude::BigInt` (n-api 9 supports `bigint64_t`). JS adapter coerces BigInt ↔ Number where safe (Number.MAX_SAFE_INTEGER = 2^53 covers most realistic counters); the BigInt → Number conversion is a JS-side concern. Concurrently, lift the `next_lock_id` seed back to `1u64 << 32+` (or higher).
- **Source:** Phase E /qa F1 close (2026-05-08). User pre-locked the design intent: u32 narrowing is acceptable for now, BigInt sweep happens after the port stabilizes.

---

## Phase E /qa deferred items (2026-05-08)

These were surfaced by the Phase E /qa adversarial review (Blind Hunter +
Edge Case Hunter subagents) but not fixed in /qa scope. F1–F8 from the
same review WERE fixed and are documented in `migration-status.md` under
the `Phase E /qa` row.

### ~~F9 — `edges.test.ts` (3 tests) + `sugar.test.ts` use `g.derived(name, deps, fn)` with arbitrary JS fn — not gated for rustImpl~~ — RESOLVED 2026-05-08 (Slice Y)

All 4 sites rewritten to use `g.add(name, await impl.map/combine(...))` — the documented workaround that exercises the same observable behavior against both impls. Initial pass used `test.runIf(impl.name !== "rust-via-napi")` gating; user pushback ("wth, is that how you make tests pass?") correctly flagged that as an anti-pattern. The proper rewrite passes against both arms.

The rewrite surfaced an additional rust impl bug: `graphrefly_graph::Graph::edges_inner` built a per-graph `names_map` and didn't share it across the mount tree, so cross-graph deps from a child node to a parent-graph name fell through to `_anon_<id>`. Fixed in same slice by pre-computing a global `names_map` via new `collect_qualified_names` helper at the top of `Graph::edges`, then passing it through `edges_inner` recursive calls. (`crates/graphrefly-graph/src/graph.rs:551-630`).

### F10 — `index.js` musl detection reads `/proc/self/maps` and produces unmapped platform keys

- **What:** `crates/graphrefly-bindings-js/index.js:32-43` reads `/proc/self/maps` (potentially MB on long-running processes) and substring-matches "musl" to detect Alpine. Two issues: (a) `/proc/self/maps` is unbounded (mapped libraries grow with process lifetime); (b) the `platforms` table on lines 21-28 has NO musl entries. On Alpine, `pickPlatformKey()` returns `"linux-x64-musl"` → `platforms[key]` is `undefined` → falls through to `graphrefly-native.linux-x64-musl.node` which doesn't exist → `require('@graphrefly/native-linux-x64-musl')` fails. Final error: confusing "no native artifact found for linux-x64-musl" without explaining musl isn't supported.
- **Why deferred:** musl support is downstream of the Cross-platform CI matrix (D075) which currently builds gnu / msvc / darwin only. Fixing the loader without adding musl artifacts would make Alpine users marginally better-error'd but still broken. Lift together when Alpine is an explicit target.
- **Lift point:** add `linux-x64-musl` / `linux-arm64-musl` to the CI matrix in `.github/workflows/ci.yml` `napi-build-matrix` (requires `cross` target setup for musl-libc); also add the keys to `platforms` in `index.js`. Replace the `/proc/self/maps` check with `process.report.getReport().header.glibcVersionRuntime === undefined` (faster, more standard).
- **Source:** Phase E /qa (2026-05-08), Blind Hunter F8 + Edge Case Hunter F9.

### ~~F11 — `getState()` singleton causes cross-test pollution within a single vitest file~~ — RESOLVED 2026-05-08 (Slice X3)

vitest `afterEach` hook added to `packages/parity-tests/impls/rust.ts` that nulls `cachedState` between tests (cheap option from the lift-point). Each test now gets a fresh `BenchCore` + `BenchOperators` + `JSValueRegistry`. 134 parity tests still passing post-fix; the shared-Core-across-operators-and-Graph invariant is preserved within each test, just not across tests.

### ~~F12 — `RustNode.down([[INVALIDATE]])` updated cache mirror BEFORE awaited Core call~~ — RESOLVED 2026-05-08 (in Slice Y's F2 batch refactor)

Cache-mirror update now happens AFTER `await batchEmitHandleMessages` resolves; a Core throw can't corrupt the mirror. Fixed inline as part of F2.

### Original F12 entry (kept for archive)

- **What was broken:** `_updateCache(undefined)` ran synchronously inside the for-loop iteration, but `core.invalidate(this.nodeId)` was queued in `buffered` and awaited later. If awaiting threw, the mirror was zeroed without Core's cache being touched.
- **Status:** RESOLVED inline as part of F2's batch refactor. The new `down()` shape moves the `_updateCache(undefined)` AFTER `await batchEmitHandleMessages` resolves, so a Core throw doesn't corrupt the mirror.
- **Source:** Phase E /qa (2026-05-08), Edge Case Hunter F12. Resolved in F2 fix (same /qa pass).

### F13 — `napi-cli --use-cross` flag verification

- **What:** `.github/workflows/ci.yml:188-194` (the `napi-build-matrix` job) uses `--use-cross` for the `aarch64-unknown-linux-gnu` row to invoke rustup `cross`. napi-rs CLI 3.x is also moving toward `--cross-compile` / `--use-napi-cross` (zigbuild-based). The flag may have been renamed.
- **Why deferred:** verifiable on first CI run only; can't reproduce locally without setting up GitHub Actions runners. Fast feedback once CI runs.
- **Lift point:** first push that triggers the CI matrix; if the linux-arm64-gnu row fails with "unknown flag --use-cross", switch to `--cross-compile` (3.0.0-alpha.62+) or invoke `cross build` directly.
- **Source:** Phase E /qa (2026-05-08), Blind Hunter F9.

### ~~F14 — `BenchGraph::binding` field is redundant with `core`'s embedded binding~~ — RESOLVED 2026-05-08 (Slice X3, partial)

The redundant `binding: Arc<BenchBinding>` field is intentionally kept — `BenchGraph::derived` needs the concrete `BenchBinding` type to access `binding.registry.lock()` for fn registration; the trait-erased `core.binding: Arc<dyn BindingBoundary>` can't be downcast back to `Arc<BenchBinding>` without `Any` bound. The lift-point's debug-build `Arc::ptr_eq` assert can't be expressed in `bindings-js` because `core.binding` is `pub(crate)` in `graphrefly-core` (different crate). Documented as a construction-invariant: `BenchCore::new` is the single path that builds the binding; both `core_clone()` and `binding_arc()` derive from the same `Arc<BenchBinding>`.

Field doc comment in `graph_bindings.rs::BenchGraph` calls out the redundancy + invariant explicitly so future readers don't try to "clean up" the field. If a downstream refactor wants the assert: widen `Core` with a public `binding_arc() -> &Arc<dyn BindingBoundary>` accessor.

### F15 — `LegacyNode.hasFiredOnce()` approximation diverges from rust's real `hasFiredOnce`

- **What:** Legacy approximates `hasFiredOnce` as "cache is non-undefined-non-null." Rust's real `has_fired_once` is a separate bookkeeping flag. After `node.invalidate()`, cache is undefined → legacy returns false; rust returns true (it DID fire once historically). Tests that distinguish "fired-and-invalidated" from "never-fired" will diverge.
- **Why deferred:** parity-only divergence, no current scenario exercises it. The legacy `has_fired_once` semantics aren't part of the public legacy API; expose-and-align is its own slice.
- **Lift point:** when a parity scenario lands that exercises this distinction, either widen legacy.Node to expose the bookkeeping flag OR gate the test with `runIf`.
- **Source:** Phase E /qa (2026-05-08), Blind Hunter F12.

### F16 — `RustGraph.set/invalidate(name)` mirror-update only fires when the named node is owned by this RustGraph

- **What:** `RustGraph.set(name, value)` does `this.nodesByName.get(name)?._updateCache(value)`. If the node was added via `add(name, externalNode)` from a different RustGraph (multi-graph composition), `nodesByName` lookup hits but the OTHER graph's `nodesByName` doesn't know about the rename. Cross-graph cache-mirror divergence.
- **Why deferred:** current parity scenarios don't exercise multi-RustGraph composition. Phase E2 / patterns work will.
- **Lift point:** move cache-mirror tracking from per-RustGraph map to a central `Map<NodeId, RustNode<unknown>>` keyed by node id. Any RustGraph that observes a Core operation refreshes the right node's mirror via that central map.
- **Source:** Phase E /qa (2026-05-08), Edge Case Hunter F11.

### ~~F17 — `release_callback` set_release_callback overwritable; second install drops the prior TSFN~~ — RESOLVED 2026-05-08 (Slice X3)

Regression test landed at `packages/parity-tests/scenarios/core/release-callback-reinstall.test.ts` (Rust-only — `pure-ts` doesn't have TSFN-backed release callbacks; this is a binding-implementation regression test, not cross-impl parity). The test installs callback A, registers a state node with a JS-allocated handle (refcount=1), re-installs callback B, then triggers a release by emitting a different handle on the state node. Asserts B receives the release of the first handle AND A receives nothing — confirming the prior TSFN is dropped (and thus deallocated) on re-install. Vitest passes; the underlying Rust impl `*self.binding.release_callback.lock() = Some(Arc::new(tsfn))` is correct by construction.

---

## M5 — Reactive data structures

### ~~F18 — ReactiveLog view/scan/attach napi~~ — RESOLVED 2026-05-15 (Slice 1 / D203 native-ship); `attach_storage` napi carried

- **Resolved (napi view/scan/attach):** `structures_bindings.rs::BenchReactiveLog` now exposes `view_tail` / `view_slice` / `view_from_cursor` / `scan` / `attach` + RAII wrappers `BenchLogView` / `BenchScanHandle` / `BenchLogSubscription` (the last with explicit `dispose()` for deterministic unsub). TSFN bridging reuses the existing `build_pack_tsfn` + `bridge_sync` infra. Wired through `Impl.reactiveLog` (`view`/`scan`/`attach`) in `types.ts` + `pure-ts.ts` + `rust.ts`; `scenarios/messaging/subscription.test.ts` authored in-slice (5 scenarios × 2 arms).
- **Still carried — `attach_storage` napi:** the multi-tier `AppendLogSink` abstraction is **not** bound. No consumer scenario exercises it; binding the JS `AppendLogSink` TSFN pair (`append_entries`/`load_entries`) speculatively contradicts D196's spirit even under the D203 exemption (the exemption's purpose — a consumable native package — is met by view/scan/attach + the scenario). Append-log persistence parity is separately covered cross-impl via `attachSnapshotStorage`. **Lift point:** a JS-side scenario needing append-log persistence tiers through the napi binding.
- **Accepted leak — grows with append count (new):** `attach`'s `read_value` retains every appended `HandleId` so `at()` cannot read a wave-released slot, but releases NONE — not on `trim_head`, `clear`, or `BenchLogSubscription::dispose`. The registry refcount/value maps grow monotonically with the number of appended entries until Core drop (QA-corrected wording: this is *not* "bounded" — it is unbounded in append count, bounded only by Core lifetime). Parallels the M1-parity terminal-slot retain note; a proper release-on-trim/clear/dispose pass is a future structures-refcount slice.
- **Intern-fn panic policy on the now-public surface (D1, deferred 2026-05-15):** `make_intern_fn` / `make_map_intern_fn` / `make_index_intern_fn` / `make_identity_intern_fn` all `panic!` when the JS packer/folder returns a `HandleId` absent from the registry. This is a long-standing pre-existing convention (M5 structures bindings), NOT introduced by F18 — but D203 flips `@graphrefly/native` to a public `0.1.0`, so a downstream consumer's buggy packer/folder now panics across the FFI boundary instead of throwing a catchable JS `Error`. Decision (D1, user-locked): leave as-is; the unregistered-handle panic is a **hard contract precondition** of the `InternFn` boundary. Lift point: a `0.x` hardening pass that threads `napi::Error` through the `InternFn` contract for all four intern fns (cross-cutting; own slice).
- **`spawn_view`/`scan` vs `append` ordering (latent, design-fragility note):** `view`/`scan` run their `core.subscribe` inside `run_blocking` on a separate `env.spawn_future`; `append` is an independent `run_blocking`. There is no in-binding ordering guarantee between a `view` subscribe completing and a concurrently-issued `append` — it is safe today only because every parity scenario `await`s sequentially. If a future scenario fires `view()` and `append()` without an intervening await, interleave is unspecified. Not a defect in the slice as written; documented so a future concurrent scenario doesn't assume ordering.
- **`native-npm-publish.yml` has no tag↔version assertion (process, optional):** the workflow publishes whatever `package.json` `version` is on a `native-v*` tag; a mismatched tag re-publishes the stale version (npm rejects a duplicate, failing the job — acceptable fail-safe). Optional future hardening: assert the tag matches `package.json` `version` before the umbrella publish.
- **Source:** M5.A D179 (core), Slice W 2026-05-13 (napi defer), D203/D204 2026-05-15 (native-ship resolution), D1 + QA 2026-05-15 (panic policy, leak-wording correction, ordering/workflow notes).

### Option C `@graphrefly/native` async surface — known limitations (deferred 2026-05-15, D206/D207)

The shipped wrapper (`crates/graphrefly-bindings-js/wrapper.js`) closes NEXT-BATCH item 8 as an async surface. Carried limitations (none block the slice; all are pre-existing `BenchGraph` shape constraints now surfaced on a *public* `0.1.0` surface):

- **`graph.derived(name, deps, fn)` / `graph.mount(name, childGraph)` throw.** `BenchGraph` only supports built-in derived fns and `mountNew`; JS-callback derived nodes must go through the operator factories (`impl.map`/`filter`/…) + `graph.add()`. The wrapper throws a clear error rather than silently mis-wiring. Pre-existing (was D074 carry-forward in the old private adapter); now a documented public-surface constraint. **Lift point:** a `BenchGraph.derivedWithFn` napi that takes a TSFN — gated by a direct consumer needing graph-attached JS-fn derived nodes (no scenario today).
- **`describeNode` returns a Core-projection shape, not the pure-ts `DescribeNodeOutput` shape.** Per D207 it reuses Core's read-side accessors → `{ type, status, deps:[NodeId u64…], valueHandle, sentinel }`. It does NOT include pure-ts's `meta`/`v`/named-dep fields (no Graph namespace for an unattached node; Core has no metadata-storage primitive yet — same gap as `graphrefly-graph::NodeDescribe.meta` always-`None`). Adequate for the inspection-helper contract (`Impl.describeNode` is typed `unknown`); a richer parity shape is a future slice if a consumer needs field-level parity. **N1 behavioral parity coverage (QA Finding 8, 2026-05-15):** `packages/parity-tests/scenarios/core/n1-infra.test.ts` now exercises the other 3 cross-impl-comparable N1 symbols (`sha256Hex`/`RingBuffer`/`ResettableTimer`) behaviorally on both arms — closing the "structural cast but no behavioral coverage" gap. `describeNode` stays deliberately excluded from that scenario's equality assertions because of THIS shape divergence (asserting field equality would be a false regression); when a consumer needs field-level `describeNode` parity, the richer shape AND its parity assertion land together as the same future slice.
- **Parity-harness per-test delegation is a `Proxy`.** `packages/parity-tests/impls/rust.ts` wraps `createNativeImpl()` in a `Proxy` that re-creates the underlying instance after each `afterEach` dispose (vitest within-file isolation). Direct `@graphrefly/native` consumers call `createNativeImpl()` once and hold the instance — no Proxy. The Proxy is harness-only; not part of the public surface. Documented so a future harness refactor doesn't mistake it for a wrapper concern.
- **Source:** D206 (`~/src/graphrefly-ts/docs/rust-port-decisions.md`) + `~/src/graphrefly-ts/archive/docs/SESSION-DS-native-substrate-contract.md` "Option C — committed follow-on slice plan"; D207 (`describeNode` reuse decision).

### ~~F19 — ReactiveMap TTL, LRU, retention policies~~ — RESOLVED M5.B (2026-05-11); Slice W (2026-05-13) confirmed napi-pass-through

- **What:** Rust `reactive.rs` ships `default_ttl_ns` + per-call TTL (`set_with_ttl`), `lru_max_size`, and `RetentionPolicy` (score-based with `archive_threshold` + `max_size`). `prune_expired_inner` for batch cleanup. napi `BenchReactiveMap::create` accepts `default_ttl: Option<f64>` + `max_size: Option<u32>`; `set` accepts per-call `ttl: Option<f64>`. All wired through `ReactiveMapOptions` to the Rust layer (`reactive.rs:1011-1015` reads `default_ttl`; `reactive.rs:880` `lru_max_size`; `reactive.rs:904` `prune_expired_inner`).
- **Source:** Slice M5.B landing (2026-05-11), confirmed-no-action in Slice W 2026-05-13 audit.

### ~~F20 — ReactiveIndex range query~~ — RESOLVED 2026-05-15 (Slice 1 / D205); was a NEW primitive, not a napi-only gap

- **Correction:** the prior "Rust core landed M5.B, only the napi binding deferred" framing was **wrong**. `range_by_primary`/`rangeByPrimary` existed in **neither** `crates/graphrefly-structures/src/reactive.rs` `ReactiveIndex` **nor** `packages/pure-ts/src/extra/data-structures/reactive-index.ts`. (ReactiveIndex custom equals — the other half of the original entry — did land M5.B and remains shipped; that part was conflated.) D205 reclassified F20 as a new primitive and locked semantics.
- **Resolved full-stack (D205):** Rust core `ReactiveIndex::range_by_primary(&K,&K)->Vec<V>` (`K: Ord` method-scoped, does not constrain the type) + `index_range_by_primary_d205` cargo test (PASSING). pure-ts `reactiveIndex().rangeByPrimary(start,end)`. napi `BenchReactiveIndex::range_by_primary(start:i64,end:i64)` backed by an `i64 → value-handle` `BTreeMap` mirror (the core method is HandleId-`Ord`-correct but a parity arm must range over a user-meaningful numeric key, never opaque handle order — D205); optional trailing `numeric_key` added to `upsert`/`delete`. Wired through `Impl.reactiveIndex.rangeByPrimary` (all 3 arms); `scenarios/structures/reactive-index-range.test.ts` authored in-slice (3 scenarios × 2 arms).
- **Semantics (locked D205):** `[start, end)` inclusive-start/exclusive-end, ascending by primary, secondary-axis-independent; `start >= end` → `[]`. No cursor variant.
- **Source:** M5.A D179, Slice W 2026-05-13 (napi defer), D203/D204/D205 2026-05-15 (reclassify + full-stack resolution).

### F21 — imbl-backed persistent backends

- **What:** Cargo.toml depends on `imbl` but default backends use `Vec`/`HashMap`. Persistent immutable collections give O(log n) snapshot-and-revert for op-log changesets.
- **Why deferred:** Vec/HashMap defaults are simpler and faster for small-to-medium collections. No current workload benefits from persistent-collection semantics. Backend trait abstraction means imbl can slot in later without API changes.
- **Lift point:** When bench evidence shows snapshot/revert cost justifies the overhead, or when Phase 14 op-log changesets require structural persistence.
- **Source:** D178 decision.

### F22 — Versioning integration (V0/V1 content-addressed versions)

- **What:** TS structures support `versioning` option (V0 counter, V1 CID). Rust M5.A uses monotonic counter only via `Version::Counter(u64)`.
- **Why deferred:** CID versioning requires content-addressing infrastructure (blake3/sha2 hashing of snapshots). Counter versioning covers all current use cases.
- **Lift point:** When Phase 14 op-log changesets or cross-replica sync need content-addressed versions.
- **Source:** M5.A scope decision D179.

### F23 — Graph-level sugar wrappers for reactive structures — TS SHIPPED 2026-05-12; Rust still deferred

- **What:** TS `Graph` now ships `graph.log(name, opts?)`, `graph.list(name, opts?)`, `graph.map(name, opts?)`, `graph.index(name, opts?)` sugar methods that create a structure bundle, register its primary backing node, AND auto-register companion nodes under sub-paths (`name/byPrimary` for index, `name/mutationLog` when `mutationLog` option is set). Rust M5.A structures remain Core-level only — Graph-level wrappers (`graph.reactive_log(name, opts)` etc.) not yet implemented.
- **Why deferred (Rust):** Core-level is more fundamental. Graph wrappers are thin convenience (register structure's node_id with Graph's namespace). Can be added without changing structure internals.
- **Lift point:** When Graph-level composition patterns are needed (M2 follow-up or M5.B).
- **Source:** D177 decision; TS-side shipped in Slice U-napi S3 (2026-05-12).

### ~~F24 — napi-rs structures binding: rust.ts adapter + parity tests~~ — RESOLVED 2026-05-14 (Layer-boundary slice)

- **What landed:** `pnpm --filter @graphrefly/native build` produced the host `.node` binary with `--features full` (includes `structures`). The `rust.ts` adapter already had `buildRustStructures()` ready behind a `typeof n.BenchReactiveLog?.create === "function"` feature-detect; the rebuild made the classes available at runtime, so `hasStructures()` returns `true` on the rust arm and the existing 26 scenarios (`scenarios/structures/{reactive-log,reactive-list,reactive-map,reactive-index}.test.ts`) flipped from pure-ts-only to cross-impl.
- **Test counts (post-rebuild):** 52 structures parity scenarios pass (26 × 2 arms). All other parity tests still pass: pure-ts 3011/3011; parity 315/316 + 1 skipped.
- **Source:** Slice U-napi S4 (2026-05-12, original deferral); Layer-boundary code-slice (2026-05-14, resolution).

### F25 — HandleId Display impl added for ReactiveIndex K bound

- **What:** `HandleId` gained `impl Display` (`HandleId(N)` format) in `graphrefly-core/src/handle.rs` to satisfy `ReactiveIndex<K,V>` requiring `K: ToString`. Previously only `Debug` was derived.
- **Why noted:** Not a deferral — this is a resolved concern. The Display output is a debug-quality string (`HandleId(42)`), not a user-facing representation. If a future surface needs human-readable handle rendering, the Display impl may need revisiting.
- **Source:** Slice U-napi S4, discovered while compiling structures_bindings.rs (2026-05-12).

### §7-A — `commit_emission` single-pass collapse — DEFERRED (evidence-based; flagged for user ratification)

- **What:** §7 item (1) bundles, alongside the union-find deletion, collapsing `commit_emission`'s defensive multi-phase re-lock (Phase-1 snapshot / Phase-3 re-lock / terminal re-checks) into a single pass. This slice (D208–D211) shipped the union-find deletion + lock-free `None` floor + `SerializationGroupId` contract but **did not** collapse `commit_emission`.
- **Why deferred:** `SESSION-rust-port-perf-value-investigation.md` §5b *measured* the lock-cycle collapse at **~0% single-thread benefit** (contended-only ~10%); the ~7.6× win is the union-find/machinery deletion (§4b/§4c), not the re-lock collapse. The collapse is also the single riskiest behavioral change in §7 (terminal/equals/DIRTY ordering under the multi-phase structure). Deferring it materially de-risked an already-maximal slice while still delivering §7's measured perf lever and the ~83 ns floor.
- **Status:** This is a deviation from the literal "one batch, both" wording of the user-locked §7. The AskUserQuestion to confirm failed (stream closed) so it was taken on the strength of §5b's own measurement and **flagged for explicit user ratification**. If the user wants it in-batch, it is a focused follow-on against the now-simplified (group-serialized) wave engine.
- **Lift point:** Bench evidence that the contended-regime ~10% matters for a real workload, OR user direction to complete §7 literally.
- **Source:** §7 item (1); §5b measurement; D209; this slice.

### §7-B — Cross-component dynamic re-entry into an unheld serialization group

- **What:** §7's wave acquisition collects the touched-group set via a bounded cascade walk (children + meta) from the seed and acquires it sorted upfront. A fn that, mid-fire, re-enters Core (`emit`/`complete`/…) on a node in a **different** component whose group was *not* in the seed's cascade will acquire that group's `ReentrantMutex` nested (not pre-sorted with the held set).
- **Why acceptable in v1:** §7 deletes the old union-find defer machinery and does not specify a replacement for this case; the strict all-`None`/all-`Some` component-consistency invariant + static groups + the sorted upfront acquire cover the common topology-reachable cascade. Recorded honestly rather than re-inventing machinery §7 says to delete.
- **Severity (QA F5, 2026-05-16 — strengthened from the original "missed serialization" wording):** the failure mode is **deadlock potential**, not merely a missed-serialization window. A fn holding group `G_hi` (in the seed cascade) that re-enters Core and emits into a *disjoint-component* node whose group `G_lo < G_hi` performs a **descending mid-wave acquisition**; concurrently another thread holding `G_lo` and re-entrantly acquiring `G_hi` forms a classic ABBA cycle. This is exactly what the deleted `held_partitions::check_and_acquire` ascending-order check + `PartitionOrderViolation` defer-to-wave-end existed to prevent. Reachability is still bounded: it requires cross-*component* (disjoint dep-graph) re-entrant emit under multi-thread grouped load — the producer-pattern common case keeps the target cascade-reachable (group already held → `ReentrantMutex` pass-through). But the consequence if hit is a hang, not just a torn update.
- **Lift point:** If a real consumer hits cross-component dynamic re-entry under multi-thread grouped load, add a minimal "acquire-or-defer-to-wave-end if it would descend" guard (a much smaller mechanism than the deleted union-find defer). Until then, prefer keeping co-emitting nodes in one component (so the target group is in the seed cascade and pre-acquired sorted).
- **Source:** §7 wave-acquisition spec; this slice.

### §7-C — Vestigial union-find surface retained for downstream zero-churn (cleanup follow-on)

- **What:** To honor D211 (keep `graphrefly-graph`/`-operators`/`-storage`/`-structures` + napi compiling with zero churn), these now-dead symbols were kept as **vestigial, never-constructed** surface: `PartitionOrderViolation` (fields → `u64`, never produced), `SubscribeError::PartitionOrderViolation`, `SetDepsError::PartitionMigrationDuringFire`, `DeferredProducerOp` (+ `push_deferred_producer_op`, now immediate-exec), the `*_or_defer` methods (now thin aliases to the non-deferred ops), `Core::drain_deferred_producer_ops` (empty inline no-op). The `graphrefly-operators` `Err(SubscribeError::PartitionOrderViolation(_))` defer arms are now dead (never taken).
- **Why deferred:** Removing them requires editing the matching `Err(_)` arms + call sites across `graphrefly-operators` (`producer.rs`, `higher_order.rs`) — a pure downstream-churn refactor disjoint from the Core rewrite. Pre-1.0, no compat constraint; it's a cleanup, not a correctness issue.
- **Lift point:** A standalone `graphrefly-operators` cleanup slice (delete the dead defer arms + the vestigial Core symbols together).
- **Source:** D211; this slice.

### §7-D — Throwaway perf-investigation scaffolding removed

- **What:** The inert `bench_naive` / `bench_state_collapse` Cargo features + their `#[cfg(feature = ...)]` code blocks in `node.rs`/`batch.rs` (and the matching `cfg_attr`s) referenced the very union-find/lock-cycle machinery this slice deletes; the dead `cfg(bench_naive)` paths were stripped as part of the rewrite. `benches/minimal_handle_core.rs` + `examples/profile_disjoint_state_emit.rs` remain (they are the floor-measurement harness and stay useful as the §7 floor regression bench). The `bench_naive`/`bench_state_collapse` feature entries in `Cargo.toml` are now unused and should be removed in the docs/cleanup pass.
- **Source:** `project_next_porting_batch.md` (throwaway artifacts, revert-before-release); §7 implementation.

### §7-E — `GroupLockRegistry` never prunes (unbounded growth over a long-lived churning Core)

- **What:** `groups::GroupLockRegistry::lock_arc` is get-or-create and the map only grows; no teardown path prunes a `SerializationGroupId` whose nodes were all unmounted/destroyed. A long-lived `Core<LockedCell>` that churns through many distinct group ids leaks one `Arc<ReentrantMutex<()>>` + HashMap entry per id permanently, and closure-form `begin_batch` (`acquire_all_group_guards` → `all_groups_sorted`) acquires every *ever-touched* group lock, so its cost grows monotonically.
- **Why deferred (QA F6, 2026-05-16):** groups are user-declared, static, and typically few + long-lived (the §7 model is "a handful of serialization domains," not per-node ids). The leak is bounded by *distinct group-id count over process lifetime*, not by node/wave count — negligible for the intended usage. Pruning needs a refcount of live nodes per group (or a sweep on unmount), which is extra machinery with no current consumer pressure.
- **Lift point:** When a consumer creates unboundedly-many distinct groups, add per-group live-node refcounting (decrement on unmount/destroy; drop the registry entry at zero) or a periodic sweep keyed off `partition_of` membership.
- **Source:** QA review (Blind Hunter §7 pass); this slice.

### §7-F — `*_or_defer` family carries `where C: Send + Sync` → unavailable on `Core<SingleThreadCell>`

- **What:** The vestigial `emit_or_defer`/`complete_or_defer`/… aliases (§7-C) retained a `where C: Send + Sync` bound. `SingleThreadCell` is `!Send + !Sync`, so producer-pattern operators (`stratify`/`control` and anything routing through `*_or_defer`) are not callable on the §7 floor substrate. Not a current break — `graphrefly-operators` is monomorphic over the default `Core<LockedCell>`, so the workspace compiles and all 825 tests pass.
- **Why deferred (QA F7, 2026-05-16):** "operators on `Core<SingleThreadCell>`" is a future milestone (the floor today is the bench/regression harness + leaf substrate use, not the operator layer). Removing the bound requires the §7-C cleanup slice to also re-check that the immediate-exec `*_or_defer` bodies don't themselves need `Send + Sync` (they shouldn't post-rewrite — no closure is queued/sent anymore).
- **Lift point:** Fold into the §7-C `graphrefly-operators` cleanup slice: drop the dead defer arms, drop the `Send + Sync` bound, make the operator layer generic over `C: StateCell`, add a `Core<SingleThreadCell>` + operators smoke test.
- **Source:** QA review (Blind Hunter §7 pass); this slice.

## Deferred infra — consolidate the 27 graphrefly-core integration-test binaries (D215)

- **What:** Each `crates/graphrefly-core/tests/*.rs` is compiled+linked as a
  separate test binary (27 of them). `cargo test` link cost scales with that
  count. Consolidating into one harness (`tests/integration/main.rs` +
  `#[path]`/`mod` includes, deduping the per-file `mod common;`) is the
  biggest compile/link-time win.
- **Why deferred (D215, 2026-05-16):** It mass-edits the same `tests/*.rs`
  files an active parallel session has uncommitted add/delete changes on
  (`serialization_groups.rs`, `group_parallelism.rs`, deleted
  `per_subgraph_parallelism.rs`/`subgraph_registry.rs`). Landing it now is a
  guaranteed merge collision. With nextest in place the run-phase
  parallelism is already solved (cross-binary global pool), so the residual
  value is compile-time only — not worth the conflict.
- **Lift point:** After the §7 serialization-group slice lands and the test
  file set is stable, do the consolidation as its own slice. Verify under an
  isolated `CARGO_TARGET_DIR` via `scripts/dev-test.sh`.
- **Source:** D215 test-loop speedup (this slice).
