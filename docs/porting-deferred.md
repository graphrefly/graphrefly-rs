---
title: Porting flags & deferred concerns
last_updated: 2026-05-07 (Slice H — register + set_pausable_mode typed-error promotion)
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

| § | What | Current shape | Optimization |
|---|---|---|---|
| 10.3 | Diamond resolution bookkeeping | `pending_fires: HashSet<NodeId>` + per-node `involved_this_wave` | `WaveTracker { settled: u64, all_deps_mask: u64 }` bitmask (O(1) is_complete vs O(n)). Pays only on fan-in beyond ~32 deps. |
| 10.4 | Per-dep indexing | position-based `deps.iter().position(...)` per child propagation | Stable `slot_index` per DepRecord; restructures `tracked: HashSet<usize>` + wave-mask bookkeeping. |
| 10.5 | Wave-end notification buffer | `pending_notify: IndexMap<NodeId, Vec<Message>>` (alloc per active node) | Single `BatchFrame { pending: Vec<(NodeId, ...)> }` arena per wave. |
| 10.6 | Sink iteration | `subscribers: HashMap<SubId, Sink>`, flush iterates under lock | Epoch iteration / snapshot-spread (TS pattern). Rust avoids snapshot cost because sink re-entrance discipline differs (see "Late subscriber + multi-emit-per-wave snapshot gap" below for the related correctness gap). |
| 10.13 | First-run gate scan | `dep_records.iter().any(|r| r.prev_data == NO_HANDLE && r.data_batch.is_empty())` | `received_mask: u64` + `all_deps_mask` bitmask. Largely redundant — wave engine only re-adds nodes to `pending_fires` after real DATA delivery. |
| —    | `pick_next_fire` transitive upstream walk | O(N·V) BFS per pick (correctness fix from Slice C-1.5, was O(n²) immediate-only and incorrect for >1-hop diamonds) | Per-node `unresolved_dep_count` counter (decrement on settle, fire on zero) — O(1) per pick. |

**Source:** Slice A+B audit (2026-05-05); pick_next_fire entry from Slice C-1.5
(2026-05-05). Collapsed into a single bucket 2026-05-07 (Slice F doc cleanup)
per user direction — individual entries were prior-art noise relative to "do
not pre-optimize."

---

## v1 dispatcher limitations (intentional, with planned lift points)

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

### Late subscriber + multi-emit-per-wave snapshot gap (D2 — Rust-only dispatcher design)

- **What:** the sinks-snapshot-on-first-touch fix (Slice A close, M1)
  freezes a node's per-wave subscriber list at the FIRST `queue_notify`
  push. If a subscriber is installed BETWEEN two emits to the same node
  in one wave (e.g. inside `batch(|| { emit(s, h1); subscribe(s, late);
  emit(s, h2); })`), `late` doesn't appear in the entry's snapshot —
  emit 2's `Dirty + Data(h2)` flushes only to existing subscribers.
  `late` sees `[Start, Data(h1)]` from its handshake and nothing more
  this wave; actual cache is `h2` after the wave settles.
- **Why divergent:** R1.3.5.a (subscriber sees consistent post-handshake
  view) is silently weakened in this niche scenario. The single-emit-
  per-wave + late subscribe case (the canonical race the snapshot fix
  targets) IS correct.
- **Why deferred (UPDATED 2026-05-07 audit):** This is a Rust-only
  dispatcher design concern — TS production has no equivalent because it
  snapshots `[...this._sinks]` AT DELIVERY TIME PER EMIT
  ([packages/legacy-pure-ts/src/core/node.ts:3583-3617](https://github.com/graphrefly/graphrefly-ts/blob/main/packages/legacy-pure-ts/src/core/node.ts)),
  not once per wave. TS's single-threaded synchronous dispatch makes the
  failure mode literally unreachable. The Rust port batched the snapshot
  to amortize cost across the lock-released drain; the gap is the
  trade-off. **Original "wait for DS-14" framing was a layer mismatch** —
  DS-14's `BaseChange<T>` per-emit metadata lives in
  `bundle.mutations` envelopes, not on dispatcher-level
  `pending_notify` entries. DS-14 will not produce a substrate that
  resolves this trade-off. Pick one of the three candidate fixes on its
  merits when M2+ surfaces a real consumer:
  - **Re-snapshot every push** — fixes correctness, allocates O(emits ×
    subscribers) per wave.
  - **Walk pending_notify on subscribe and append new sink** —
    re-introduces the duplicate-Data bug for the single-emit + late
    subscribe case (the snapshot fix's original target).
  - **Per-message subscriber tracking** — most expressive, heaviest.
- **Source:** Slice A close /qa Edge Case Hunter finding (2026-05-05);
  reframed 2026-05-07 per cross-repo audit confirming Rust-only nature.

### Cross-thread emit blocks until in-flight wave completes (D3 — UPDATED 2026-05-07)

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
  When/if pressure surfaces, scope a dedicated per-subgraph-mutex
  design slice (substantial refactor: lock-ordering protocol, cross-
  subgraph wave handoff, deadlock prevention).
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

### Per-tier handshake panic on tier-N leaves sink registered (D4)

- **What:** the per-tier handshake split (R1.3.5.a alignment, Slice A
  close) fires `[Start]`, `[Data(v)]`, `[Complete|Error]`, `[Teardown]`
  as separate sink calls. The sink is installed in `subscribers` BEFORE
  the handshake fires. If a user sink panics on tier N (e.g.
  `[Data(v)]`) during handshake, the panic unwinds out of `subscribe()`
  before the `Subscription` handle is returned. The sink stays
  registered in `subscribers`; subsequent waves' `flush_notifications`
  will fire it (its `Arc<dyn Fn>` clone in
  `pending_notify[node].sinks` is alive).
- **Why deferred:** still a real concern after Slice E's lock-released
  handshake rework — the sink installation happens before the
  handshake fires (so concurrent subsequent emits observe it). A
  panicking handshake-time sink leaves the sink in `subscribers`
  without returning a `Subscription`, so the user has no handle to
  drop. `Drop for CoreState` releases the sink Arc when the last
  `Core` drops, so it's not a memory leak, but a panicking sink
  keeps panicking on every subsequent wave's flush.
- **Lift point:** wrap the lock-released handshake fire in
  `catch_unwind`; on panic, remove the sink from subscribers and
  re-raise. Small change (~20 LOC); not blocking.
- **Source:** Slice A close /qa Blind Hunter finding (2026-05-05);
  partially superseded by Slice E rework (D045) but the panic-leaves-
  sink behavior remains.

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

### Deactivation cleanup not yet modeled (cache-clear half)

- **What:** when the last subscriber unsubscribes from a derived/dynamic node,
  TS clears its cache and releases handles. Rust currently no-ops the
  unsubscribe path's CACHE-CLEAR side. (As of Slice E2 — 2026-05-07 —
  the user-facing cleanup-hook side IS modeled: `BindingBoundary::cleanup_for(node, OnDeactivation)` fires lock-released
  from `Subscription::Drop` when the last sub leaves, gated on
  `has_fired_once`, before the existing `producer_deactivate` hook per
  D056. What's still deferred: Core's own cache release + handle
  drop on deactivation.)
- **Why deferred:** correct under "always subscribed" lifetimes (which all
  current tests assume). Lands when the graph layer (M2) introduces
  subgraph mount/unmount that makes deactivation observable.
- **Source:** [node.rs](../crates/graphrefly-core/src/node.rs) inline comment.
  Cross-ref Slice E2 wiring above for the cleanup-hook layer that already
  exists in v1.

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

### `describe()` `value` field surfaces raw `HandleId`, not user-rendered `T`

- **What:** [crates/graphrefly-graph/src/describe.rs](../crates/graphrefly-graph/src/describe.rs)
  `NodeDescribe.value: Option<HandleId>`. Canonical TS surfaces `value: T`
  directly — the binding-side registry resolves `HandleId → T` before
  serialization.
- **Why divergent:** Core operates on opaque `HandleId` integers per the
  handle-protocol cleaving plane. Producing `T` would require a Core-side
  binding callback, which conflicts with the cleaving-plane invariant.
- **Lift point:** binding crates (`graphrefly-bindings-js`, `-py`) provide
  a thin wrapper (e.g. `describe_with_values`) that walks the handle-id
  output and substitutes the registered user value before serializing for
  end-user JSON consumption.
- **Source:** Slice E+ (2026-05-05); documented in `describe.rs` module
  docs and §11 Implementation Deltas.

### ~~`observe_all()` snapshot-at-subscribe-time semantics~~ — RESOLVED in Slice F

`Graph::observe_all_reactive()` (Slice F, 2026-05-06) auto-subscribes
late-added named nodes via Graph-level namespace-change notifications.
The original `observe_all()` (snapshot-at-subscribe-time) remains
available as the non-reactive variant. Drop-order discipline prevents
a deadlock where the topology sink closure's `Arc<inner>` could be the
last reference and try to drop `Subscription`s under `CoreState` lock.
Closed 2026-05-06.

### `signal_invalidate` does not snapshot the namespace under a single lock

- **What:** [crates/graphrefly-graph/src/graph.rs](../crates/graphrefly-graph/src/graph.rs)
  `Graph::signal_invalidate` collects own-graph node ids + child Graphs
  under one `inner.lock()`, then drops the lock and walks them.
  Concurrent `add` / `mount_new` / `unmount` calls during the walk are
  visible (new nodes added after the snapshot are NOT invalidated;
  unmounted children may already be detached when the recursive call
  hits them).
- **Why divergent:** holding the namespace lock across the recursive
  invalidation walk would serialize all graph mutations against
  invalidate cascades — fine in practice (signal_invalidate is
  typically setup/teardown-time, not on the hot path), but the
  acquisition-order rule is "Graph → Core only", and the Core
  invalidate cascade re-enters the Graph layer would deadlock against
  a Graph-held lock. Snapshotting first preserves the rule.
- **Source:** Slice E+ (2026-05-05).

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

### `try_resolve` does not support `..::sibling::node` cross-subgraph paths (R3.5.2)

- **What:** [crates/graphrefly-graph/src/graph.rs](../crates/graphrefly-graph/src/graph.rs)
  `try_resolve` only supports root-relative descent. Canonical R3.5.2:
  *"Cross-subgraph references use relative paths from the shared
  parent."* The `..::sibling::node` form requires walking up via
  `inner.parent.upgrade()` per `..` segment.
- **Why deferred:** non-trivial path-parser change with edge cases
  (multiple `..`, traversal past the root, `..` after a name segment).
  Not blocking M2 — sibling resolution from inside a child graph is a
  Phase 4+ pattern need (harnessLoop sibling wiring), not yet in
  scope.
- **Workaround:** resolve from the shared parent: pass the parent
  `Graph` clone to fns that need cross-subgraph reach, or use
  `Graph::node` from the parent.
- **Source:** Slice E+ /qa Edge Case Hunter F3 (2026-05-05).

### `try_resolve` returns `None` silently for malformed paths

- **What:** Empty paths (`""`), leading separator (`"::foo"`),
  trailing separator (`"foo::"`), and double-separator (`"a::::b"`)
  all silently resolve to `None` rather than reporting a malformed
  path. Caller cannot distinguish "absent" from "syntactically
  invalid."
- **Why deferred:** changing the return type to `Result<Option<NodeId>,
  PathError>` is a public-API break for narrow benefit; in v1, all
  paths are constructed by graph code (sugar constructors validate at
  registration). Document the behavior explicitly.
- **Source:** Slice E+ /qa Blind Hunter + Edge Case Hunter F9
  (2026-05-05).

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

### `_anon_<id>` deps could collide with a user-named node `_anon_<id>`

- **What:** `describe()` surfaces unnamed Core deps as
  `_anon_<NodeId.raw()>` strings in `nodes[].deps` and `edges`.
  A user who names a node `_anon_5` AND has an anonymous Core
  node with `NodeId::raw() == 5` produces an ambiguous output —
  two distinct nodes share the same dep label.
- **Why deferred:** low-probability collision (raw NodeIds are not
  stable across builds). Format ambiguity is the issue, not
  correctness. Fix lifts when the `_anon_<id>` convention is
  replaced with a structured shape (e.g., a separate
  `anonymous: HashMap<u64, NodeDescribe>` map alongside the named
  `nodes` map).
- **Source:** Slice E+ /qa Blind Hunter (2026-05-05).

### `ancestors()` cycle insurance

- **What:** [crates/graphrefly-graph/src/mount.rs](../crates/graphrefly-graph/src/mount.rs)
  `ancestors` walks Weak parent pointers without a visited-set
  check. If a future regression lets `mount` accept a cycle (e.g.,
  through internal `with_core` escape hatches), `ancestors` would
  loop forever.
- **Why deferred:** current API surface prevents cycles
  (`mount.AlreadyMounted` rejects double-parent). `with_core` is
  `pub(crate)` only. Belt-and-braces cycle guard is cheap; defer
  until a cycle slips past mount validation.
- **Source:** Slice E+ /qa Blind Hunter (2026-05-05).

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

### `fire_operator` first-run gate uses linear-scan `has_sentinel_deps()`

- **What:** `fire_operator`'s pre-fire skip check calls `rec.has_sentinel_deps()` (O(n_deps) linear scan). For transform operators (single-dep, R5.7), this is O(1) in practice. But for future multi-dep operators (combine, withLatestFrom), it's O(n_deps) per fire.
- **Why deferred:** mirrors the §10.13 deferred concern for `fire_regular`; the `received_mask: u64` bitmask refactor would land alongside that work. Bench-driven re-look.
- **Source:** Slice C-1 close (2026-05-06).

### Operator describe doesn't surface per-operator discriminant

- **What:** `Graph::describe()` reports operator nodes as `type: "operator"` but doesn't include the `OperatorOp` discriminant (Map vs Filter vs Scan etc.) or the registered `FnId` references. Consumers see "this is an operator" but not "this is a map".
- **Why deferred:** belongs with the canonical describe extension that surfaces operator catalog metadata (Phase 4+ pattern). v1 acceptable — the namespace + edges are sufficient for current consumers.
- **Lift point:** add an `operator: { kind: "map" | "filter" | ... }` field to `NodeDescribe` when the operator catalog substrate lands.
- **Source:** Slice C-1 close (2026-05-06).

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

### Flow operator counters reset only on resubscribable terminal cycle (D028)

- **What:** TS legacy `take.ts` resets `taken` / `skipped` / `done` / `latest` on every deactivation (when last subscriber leaves) per Lock 6.D. Rust v1's [`crates/graphrefly-core/src/op_state.rs`](../crates/graphrefly-core/src/op_state.rs) state structs only reset via `reset_for_fresh_lifecycle` — the resubscribable terminal lifecycle reset path. Non-resubscribable nodes whose subscribers go to zero and then come back DON'T reset their flow-operator state.
- **Why divergent:** matches the broader "Deactivation cleanup not yet modeled" deferral. Rust v1 only models the terminal-resubscribable lifecycle reset; full deactivation/reactivation cleanup awaits M2-graph mount/unmount work that surfaces deactivation as observable.
- **Lift point:** when deactivation cleanup lands, call `OperatorScratch::release_handles` + reinstall fresh scratch on deactivation (mirrors `reset_for_fresh_lifecycle` shape — the substrate is already in place via the trait's `release_handles` method).
- **Source:** Slice C-3 close (2026-05-06); D028.

### `take_until(source, notifier)` not yet ported

- **What:** TS `take.ts` exports `takeUntil(source, notifier, opts?)` — forwards `source` until `notifier` matches `predicate`, then `Complete`. Implementation uses the producer pattern (manual subscribe to two sources). Not in the Slice C-3 scope.
- **Why deferred:** the Core-dispatch `OperatorOp` family (Take/Skip/TakeWhile/Last) handles single-dep / count / predicate gates cleanly. `takeUntil` is fundamentally a 2-dep subscription-managed operator (D020 category B) — different infrastructure. Lands with the future subscription-managed slice that also covers `zip` / `concat` / `race` / `switchMap` / `mergeMap` / `concatMap`.
- **Source:** Slice C-3 close (2026-05-06); D024 scope decision.

### Operator describe doesn't surface flow-variant discriminant

- **What:** Same as the existing Slice C-1 deferral ("Operator describe doesn't surface per-operator discriminant"). `Graph::describe()` reports the new `Take` / `Skip` / `TakeWhile` / `Last` variants as `type: "operator"` without indicating which flow op they are. Consumers see "this is an operator" but not "this is a take(3)".
- **Why deferred:** belongs with the canonical describe extension that surfaces operator catalog metadata (Phase 4+ pattern). v1 acceptable.
- **Source:** Slice C-3 close (2026-05-06); inherits from Slice C-1 deferral.

### `predicate_each` length-mismatch silently truncates `take_while` in release builds (Slice C-3 /qa D1)

- **What:** [`fire_op_take_while`](../crates/graphrefly-core/src/batch.rs) loops over `inputs` and reads `pass.get(i).copied().unwrap_or(false)` (`batch.rs` ~1382). A `debug_assert!` catches `pass.len() != inputs.len()` in debug builds. In release builds, a binding-side bug returning a too-short `Vec<bool>` causes `unwrap_or(false)` to read missing entries as `false` → first-false short-circuit → `done` + `self.complete()`. Silent self-completion. The same defect class affects `fire_op_filter`, but there it just silently drops items without terminating — less catastrophic.
- **Why deferred:** binding contract violation; no production binding (napi-rs / pyo3 / wasm-bindgen) should violate the contract. Promoting `debug_assert!` to `assert!` would crash release builds on a misbehaving binding rather than silently truncate; that's a defensible tradeoff for v1 but not blocking. A unified `binding_contract_check_pass_len(pass, inputs)` helper that asserts in all builds is the cleaner long-term fix.
- **Lift point:** when bench evidence shows operator dispatch is hot enough to warrant pre-validation cost, OR when a real binding bug shows up in production. Until then, defensive `unwrap_or(false)` is acceptable.
- **Source:** Slice C-3 /qa Edge Case Hunter (2026-05-06).

### Future napi-rs `last(src, opts?)` adapter must accept `{ defaultValue: T }` shape (Slice C-3 /qa D2)

- **What:** TS legacy `last(source, options?: { defaultValue?: T })` accepts an optional opts object; `Object.hasOwn(options, "defaultValue")` distinguishes "no default" (key absent) from "explicit undefined default" (key present). Rust core splits this into two factories: [`last`](../crates/graphrefly-operators/src/flow.rs) (no default — emits only `Complete` on empty stream) and [`last_with_default`](../crates/graphrefly-operators/src/flow.rs) (real default handle required). The parity-test scenario [`packages/parity-tests/scenarios/operators/flow.test.ts`](https://github.com/graphrefly/graphrefly-ts/blob/main/packages/parity-tests/scenarios/operators/flow.test.ts) calls `impl.last(src, { defaultValue: 42 })` against `legacyImpl`. When `rustImpl` activates, the napi-rs binding must dispatch `impl.last(src, { defaultValue: V })` to the Rust `last_with_default(src, intern(V))` factory and `impl.last(src)` (no opts) to `last(src)`.
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

### D2 — `GraphObserveAllReactive::subscribed` HashSet grows unboundedly

- **What:** [crates/graphrefly-graph/src/observe.rs](../crates/graphrefly-graph/src/observe.rs)
  `ObserveAllReactiveInner::subscribed` records every `NodeId` ever
  subscribed; it is never pruned for torn-down nodes. For long-running
  graphs with high node turnover, memory grows linearly with total
  nodes ever created (not currently live).
- **Why deferred:** v1 acceptable for short-lived graphs (the typical
  reactive-observe lifecycle is a UI session, not a server-uptime
  span). Pruning requires a `Core::subscribe_topology` listener for
  `NodeTornDown` events to remove ids from `subscribed` — adds a
  second subscription per reactive observer.
- **Lift point:** when M3+ surfaces a long-running consumer that
  exhibits the leak under bench, wire a topology subscription that
  prunes `subscribed` on `NodeTornDown(id)`.
- **Source:** Slice F /qa Blind Hunter (2026-05-06).

### D3 — `signal()` `SignalKind` enum narrower than canonical's open `Messages`

- **What:** [crates/graphrefly-graph/src/graph.rs](../crates/graphrefly-graph/src/graph.rs)
  `SignalKind` is closed at `Invalidate / Pause(LockId) / Resume(LockId)`.
  Canonical R3.7.1's `signal(messages: Messages, opts?)` is open-ended;
  `[[ERROR, h]]` and `[[COMPLETE]]` graph-wide broadcasts are
  representable in TS but rejected at the type level in Rust.
- **Why divergent:** v1 narrow surface. Adding `SignalKind::Complete` /
  `SignalKind::Error(HandleId)` requires deciding meta-filter semantics
  for terminal broadcast (R3.7.2 explicit only for INVALIDATE — see
  D4). Pre-1.0; closed enum is easier to extend than to retract later.
- **Lift point:** if a consumer needs graph-wide COMPLETE/ERROR
  (e.g., shutting a whole subgraph terminally without TEARDOWN
  cascade), extend the enum and decide R3.7.2's coverage scope.
- **Source:** Slice F /qa Edge Case Hunter F6 (2026-05-06).

### D4 — `signal(Pause/Resume)` does not filter meta companions

- **What:** Canonical R3.7.2 explicitly requires meta filtering for
  `INVALIDATE` (meta children's auto-derived caches must not be wiped).
  The spec is silent on PAUSE/RESUME meta filtering. Rust's
  `signal_pause` / `signal_resume` pause/resume every named node
  including any meta companions; `signal_invalidate` does filter
  metas.
- **Why deferred:** spec ambiguity. Symmetry argument suggests metas
  should NOT be paused independently of their parent (their lifecycle
  is bound to the parent's), but no canonical rule explicitly
  requires it. TS impl behavior should be checked; if TS skips meta
  on PAUSE/RESUME and Rust doesn't, that's a parity gap.
- **Lift point:** raise an issue against `~/src/graphrefly` to
  extend R3.7.2 to cover PAUSE/RESUME explicitly, OR verify against
  TS production behavior and align.
- **Source:** Slice F /qa Edge Case Hunter F7 (2026-05-06).

### D5 — `describe_reactive` does not fire on `set_deps` topology changes

- **What:** [crates/graphrefly-graph/src/describe.rs](../crates/graphrefly-graph/src/describe.rs)
  `describe_reactive` subscribes to Graph-level namespace-change sinks
  (fired from `add` / `remove` / `mount` / `unmount` / `destroy`).
  Core's `DepsChanged` topology event from `set_deps` does NOT trigger
  the namespace sink — but `describe()` output's edges DO change when
  deps change (edges are derived from `core.deps_of`). The reactive
  describe stream is therefore incomplete: it tracks namespace
  mutations but misses dep mutations even though dep mutations affect
  the describe payload.
- **Why divergent:** layering decision per D006 in the decision log.
  Core topology events are Core-internal; Graph-level reactivity
  tracks Graph-level mutations. Composing a unified stream requires
  either (a) Graph subscribes to Core topology and forwards
  `DepsChanged` as a namespace-change, (b) the user composes both
  subscriptions manually with dedup. The describe rustdoc explicitly
  documents the workaround.
- **Lift point:** when consumers actually need it, internalize the
  composition by having `describe_reactive` ALSO subscribe to Core's
  `subscribe_topology` and dedup against the namespace fire.
- **Source:** Slice F /qa Edge Case Hunter F10 (2026-05-06).

## M2 parity-tests fragility (post-/qa, 2026-05-06)

Surfaced during /qa of the B+C+D batch. The six new `scenarios/graph/`
scenarios pass against `legacyImpl` but rest on assumptions that may not
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

### Pause-buffer overflow does not synthesize ERROR (TS Lock 6.A divergence)

- **What:** When the per-node pause replay buffer hits the Core-global
  `pause_buffer_cap`, the oldest message is dropped and `dropped` increments.
  TS Lock 6.A also synthesizes a `Message::Error` carrying overflow detail
  (cap value, dropped count, lock-held duration in ms). The Rust port
  reports overflow only via `ResumeReport.dropped`.
- **Why divergent:** simpler v1 surface; callers are expected to inspect
  `ResumeReport.dropped > 0` themselves. The reserved
  `PauseState::Paused.started_at_ns` field is `#[allow(dead_code)]` for
  future use when ERROR synthesis lands.
- **Source:** Edge Case Hunter QA finding, 2026-05-05; TS reference at
  `~/src/graphrefly-ts/src/core/node.ts:3193-3225`.

### `alloc_lock_id` can collide with user-supplied `LockId::new(N)`

- **What:** [`Core::alloc_lock_id`](../crates/graphrefly-rs/crates/graphrefly-core/src/node.rs)
  uses a sequential `next_lock_id` starting at 1. Users who construct
  `LockId::new(N)` directly (the `pub` constructor exposes any `u64`) can
  collide with later alloc-allocated ids, causing
  cross-controller pause leak: caller A's `resume(node, lock_a)` would
  drain caller B's pause if both happened to use the same `LockId`.
- **Why deferred:** TS uses opaque `Symbol()` for guaranteed uniqueness;
  Rust's `LockId` is a u64 newtype. Production fix: either reserve a high
  range for `alloc_lock_id` (start at e.g. `u64::MAX / 2`) or make
  `LockId::new` `pub(crate)` and force users through `alloc_lock_id`. v1
  recommendation: pick one allocation scheme per consumer.
- **Source:** Edge Case Hunter QA finding, 2026-05-05.

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
- TS reference for `Default` mode: `legacy-pure-ts/src/core/node.ts:2556-2558`
  (`_pendingWave = true` gate) + `node.ts:3103-3106`
  (`_maybeRunFnOnSettlement()` on RESUME).

Rust impl in `crates/graphrefly-core/src/node.rs` `PauseState::Paused.pending_wave`
flag + `Core::set_pausable_mode` setter + `deliver_data_to_consumer`
suppression branch + `Core::resume` Phase-4 consolidated fn-fire schedule.
Regression: `item5_default_mode_consolidates_to_one_fn_fire_on_resume` in
`tests/slice_f_corrections.rs`. Closed 2026-05-07.

### `set_deps` mid-wave full-removal leaves stuck DIRTY without RESOLVED

- **What:** If `set_deps(N, &[])` is called while N is in dirty state (a
  prior dep delivered DATA and queued DIRTY this wave), the rewire removes
  all deps. The wave engine has nothing to fire (no fn deps) and no auto-
  RESOLVED is queued. Subscribers see DIRTY without a paired settle.
- **Why deferred:** complex semantics — auto-RESOLVED on wave-end after
  full removal would compete with downstream-cascade conventions. The
  inline comment at `set_deps` already acknowledges the gap. Real fix
  bundles with the M2 graph-layer "structural change → wave settle"
  protocol.
- **Source:** Edge Case Hunter QA finding, 2026-05-05.

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

### Non-resubscribable terminal Error handles leak via diamond cascade

- **What:** When a node terminates with `Error(h)`, every per-node
  `terminal` slot AND every consumer's `dep_terminals[idx]` slot retains
  `h` (refcount bump). For non-resubscribable nodes, those retains are
  never released (the resubscribable reset path is the only release site
  for these slots). Each terminal node leaks one retain per cascade
  destination.
- **Why deferred (UPDATED 2026-05-07 audit):** Cross-repo audit confirmed
  the lift point is **Rust M2 deactivation cleanup**, NOT TS Phase 14.
  The TS `_deactivate` impl
  ([packages/legacy-pure-ts/src/core/node.ts:2185-2294](https://github.com/graphrefly/graphrefly-ts/blob/main/packages/legacy-pure-ts/src/core/node.ts))
  clears all per-dep settled-state on last-sink-detach (calls
  `resetDepRecord(d)` for each dep, clears `_pauseBuffer`, `_replayBuffer`,
  `_pauseLocks`, and `_cached` for compute nodes). TS doesn't refcount
  handles, so the "leak per cascade destination" shape is structurally
  different — but the *discipline* is exactly what Rust needs to mirror:
  release per-dep `Handle` refs in `dep_terminals[idx]` + own `terminal`
  slot when deactivation lands. **No TS Phase 14 dep.** Cross-link to
  the "Deactivation cleanup not yet modeled" entry — both lift together
  at the M2 deactivation work.
- **Source:** Migration-status.md "Carried forward" section reinforced by
  Blind Hunter QA finding, 2026-05-05; cross-repo audit 2026-05-07
  confirmed Rust-M2 dependency (not TS).

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

- **Late-subscriber-to-terminal-node-delivers-nothing universal foot-gun.**
  When a node has emitted COMPLETE and a new subscriber arrives, the
  push-on-subscribe path must decide whether to replay the terminator.
  Resubscribable nodes already get a clean handshake (Slice A+B); the
  non-resubscribable case is the foot-gun.
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
  `test.runIf(impl.name !== "legacy-pure-ts")(...)`. The
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

Bench harness shipped at `~/src/graphrefly-ts/packages/legacy-pure-ts/src/__bench__/operators-via-tsfn.bench.ts`. Three bench groups: `builtin_fn` (pure-FFI baseline) + `tsfn_identity` (TSFN scheduling) + `tsfn_addone_js` (end-to-end Rust-via-TSFN-into-JS with deref+intern). All three use top-level await for setup (vitest bench mode does NOT run `beforeAll` — convention from `graphrefly.bench.ts:5`) and `await emitInt` for end-to-end measurement (the legacy `ffi-cost.bench.ts` fires-and-forgets emit, which only measures Promise scheduling). Subtraction interpretation: `(2)−(1) = TSFN scheduling cost`; `(3)−(2) = JS compute + 2× sync FFI`; `(3)` is the headline.

Cold-tokio warmup is per-`describe` (each block builds a fresh BenchCore); the first bench runs slightly cold relative to subsequent ones. Acceptable for the substrate-validation scope of Phase D; if the §10 perf-tier work needs tighter numbers, lift to shared-Core warmup.

### ~~TSFN err-first wire-vs-typed signature mismatch (Phase D finding, 2026-05-08)~~ — RESOLVED 2026-05-08 (Slice Y, same day)

User direction 2026-05-08: "Fix this in this batch because otherwise parity tests are broken. Do the ultimate fix." Bundled into Slice Y rather than deferring.

**What landed:** flipped `CalleeHandled = true` → `false` across all 7 TSFN sites:
- `operator_bindings.rs::Tsfn<T, R>` type alias (final const generic flipped).
- `operator_bindings.rs::build_h_to_h_tsfn` / `build_h_to_bool_tsfn` / `build_hh_to_h_tsfn` / `build_hh_to_bool_tsfn` / `build_vec_to_h_tsfn` (5 builders, all `.callee_handled::<false>()`).
- `core_bindings.rs::SinkTsfn` type alias + `build_sink_tsfn`.
- `core_bindings.rs::ReleaseTsfn` type alias + `release_callback` site at line ~456.

**Side-concern landed too:** `bridge_sync` and `bridge_sync_unit` updated to call `tsfn.call_with_return_value(arg, ...)` (plain `T`, not `Ok(arg)`) per napi-rs 3.x's `CalleeHandled=false` impl signature. The `FnOnce(Result<Return>, Env) -> Result<()>` cb signature is identical between the two `CalleeHandled` impls, so JS-throw delivery via `Result<R>` is preserved — `bridge_sync`'s panic-on-throw discipline (line 209-212) still works.

**Empirical validation:** the rustImpl parity arm activated for the first time (`pnpm --filter @graphrefly/native build` produced `graphrefly-native.darwin-arm64.node`; `pnpm --filter @graphrefly/parity-tests test` ran 138 tests). Result: **118 passed, 0 failed, 16 skipped, 4 todo.** All operator scenarios (transform 16, combine 12, flow 22, higher-order 18, subscription 32) pass against rustImpl AND legacyImpl. The 4 originally-failing `g.derived(name, deps, fn)` sites are now `test.runIf(impl.name !== "rust-via-napi")` gated per the F9 carry-forward.

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
  `@graphrefly/legacy-pure-ts` publish cadence + npm credentials in CI.
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

Regression test landed at `packages/parity-tests/scenarios/core/release-callback-reinstall.test.ts` (Rust-only — `legacy-pure-ts` doesn't have TSFN-backed release callbacks; this is a binding-implementation regression test, not cross-impl parity). The test installs callback A, registers a state node with a JS-allocated handle (refcount=1), re-installs callback B, then triggers a release by emitting a different handle on the state node. Asserts B receives the release of the first handle AND A receives nothing — confirming the prior TSFN is dropped (and thus deallocated) on re-install. Vitest passes; the underlying Rust impl `*self.binding.release_callback.lock() = Some(Arc::new(tsfn))` is correct by construction.
