---
title: Porting flags & deferred concerns
last_updated: 2026-05-06 (M3 Slice C-2 close)
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
analysis identifies *likely* wins — verify with data."

Pass 5 of the v0 bench (SmallVec + ChildEdge dep_idx caching, both regressed)
already validated the principle. The following §10 items are kept deferred
until a bench reproducer makes them pay for themselves.

### §10.3 — Diamond resolution bitmask

- **What:** [graphrefly-core/src/node.rs](../crates/graphrefly-core/src/node.rs) uses
  `pending_fires: HashSet<NodeId>` + per-node `involved_this_wave: bool` instead
  of the §10.3 `WaveTracker { settled: u64, all_deps_mask: u64 }` bitmask.
- **Why deferred:** correct as-is; bitmask is O(1) `is_complete()` vs current
  O(n) wave bookkeeping. Pays for itself only on hot fan-in paths beyond ~32
  deps. Re-look when bench surfaces such a path.
- **Source:** Slice A+B audit (2026-05-05).

### §10.4 — Stable `slot_index` per DepRecord

- **What:** position-based dep indexing throughout the dispatcher
  (`commit_emission` uses `deps.iter().position(|&d| d == node_id)` per
  child propagation, O(n_deps) per delivery). On `set_deps`, we rebuild a
  `Vec<HandleId>` aligned to the new dep order rather than using a stable
  slot id.
- **Why deferred:** correct as-is for current rewire scenarios (set_deps
  preserves dep_handles[i] alignment by node_id). The §10.4 stable slot
  refactor would also restructure `tracked: HashSet<usize>` and the wave-mask
  bookkeeping. Big change with no current pain point.
- **Source:** Slice A+B audit (2026-05-05).

### §10.5 — Single-arena BatchFrame

- **What:** wave-end notifications buffer through
  `pending_notify: HashMap<NodeId, Vec<Message>>` (one allocation per active
  node). §10.5 prescribes a single `BatchFrame { pending: Vec<(NodeId, ...)> }`
  per wave.
- **Why deferred:** v0 bench did not show buffer alloc as a bottleneck. The
  HashMap shape also makes diamond fan-in coalescing and per-node tier-3
  rewriting straightforward; the arena would push that complexity into the
  flush phase.
- **Source:** Slice A+B audit (2026-05-05).

### §10.6 — Sink list epoch iteration

- **What:** `subscribers: HashMap<SubscriptionId, Sink>`; flush iterates
  `rec.subscribers.values()` while the lock is held. TS spreads
  `[...this._sinks]` to handle mid-iteration unsubscribe.
- **Why deferred:** Rust avoids the snapshot-spread cost because sink
  re-entrance into Core is forbidden by the v1 lock discipline (sinks fire
  while `parking_lot::Mutex<CoreState>` is held; recursive unsubscribe would
  deadlock long before it could mutate the subscribers map). When the
  re-entrance limitation lifts (see "v1 limitations" below), revisit §10.6.
- **Source:** Slice A+B audit (2026-05-05).

### §10.13 — First-run gate bitmask

- **What:** `fire_fn` checks `if rec.dep_handles.contains(&NO_HANDLE) { return }`
  — O(n_deps) linear scan per fn-fire attempt.
- **Why deferred:** trivial bitmask refactor (`received_mask: u64` + `all_deps_mask`),
  but mostly redundant given that the wave engine only re-adds a node to
  `pending_fires` when a dep has actually delivered DATA. The hot path is
  already bounded by the wave's pending set. Bench-driven re-look.
- **Source:** Slice A+B audit (2026-05-05).

### `pick_next_fire` transitive upstream walk is O(N·V) per pick

- **What:** [batch.rs](../crates/graphrefly-core/src/batch.rs) `pick_next_fire`
  + `transitive_upstream_settled` — for each candidate in `pending_fires`,
  BFS-walks transitive ancestors via `deps` to verify none is currently
  pending. With pending-size N and graph size V, worst-case O(N·V) per
  pick (vs the prior immediate-only check's O(N·D) where D is direct deps).
- **Why deferred:** the immediate-only check (Slice A+B) was incorrect for
  graphs deeper than 1 hop: a downstream join-node could fire prematurely
  on stale upstream when a sibling branch fired first. Slice C-1.5
  switched to the transitive walk to fix the diamond glitch the strict
  tests surfaced. Performance is an acceptable trade vs correctness.
  Replace with a per-node `unresolved_dep_count` counter (decrement when an
  upstream settles, fire when count hits zero) when bench shows the cost
  — that's O(1) per pick.
- **Source:** Slice C-1.5 (2026-05-05). Original O(n²) flag was Slice A+B.

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

### Late subscriber + multi-emit-per-wave snapshot gap (D2 — needs rethink)

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
- **Why deferred (per user direction, 2026-05-05 /qa):** the three
  candidate fixes have non-trivial trade-offs:
  - **Re-snapshot every push** — fixes correctness but allocates
    O(emits × subscribers) per wave; penalizes the common case.
  - **Walk pending_notify on subscribe and append new sink** — would
    re-introduce the duplicate-Data bug for the single-emit + late
    subscribe case (the snapshot fix's original target).
  - **Per-message subscriber tracking** — most expressive but heavy.
  The user's direction: park as a documented v1 limitation and revisit
  when the design shape shifts (likely alongside DS-14 changesets,
  which already touches per-emit metadata).
- **Source:** Slice A close /qa Edge Case Hunter finding (2026-05-05);
  user decision Q1=(a) defer with rethink note.

### Cross-thread emit blocks until in-flight wave completes (D3)

- **What:** with the wave-owner re-entrant mutex (Slice A close /qa,
  M1), cross-thread `Core::emit` calls block at
  `wave_owner.lock_arc()` until the in-flight wave's drain + flush +
  sink-fire completes. Same-thread re-entry passes through.
- **Why divergent:** TS is single-threaded; PY is GIL-serialized.
  Rust is the first impl with parallel access. The wave-owner mutex
  is the chosen serialization point. Without it, the lock-released
  drain (Slice A close) would let a concurrent `emit` absorb into
  the in-flight wave's `pending_notify` and return BEFORE its
  subscribers fire — breaking the user-facing contract that `emit`
  returning means subscribers have observed.
- **Trade-offs accepted:**
  - Concurrent threads cannot drive parallel waves on the same Core.
    Per-subgraph parallelism (CLAUDE.md invariant 3) is the planned
    parallel-throughput axis; M2's mount/unmount work surfaces it.
  - A blocked thread waits for the owner thread's fn fires + sink
    fires to complete. If a fn or sink is slow, blockers wait.
- **Verified by:** `concurrent_emit_blocks_until_in_flight_wave_completes`
  in `tests/lock_released.rs`.
- **Source:** Slice A close /qa Blind Hunter + Edge Case Hunter findings
  (2026-05-05); user decision Q2=(b) wave-owner mutex.

### Set_deps from inside firing node's fn corrupts Dynamic `tracked` indices (D1)

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
  the handshake fires (race-fix discipline from Slice A-bigger). If a
  user sink panics on tier N (e.g. `[Data(v)]`) during handshake, the
  panic unwinds out of `subscribe()` before the `Subscription` handle
  is returned. The sink stays registered in `subscribers`; subsequent
  waves' `flush_notifications` will fire it (its `Arc<dyn Fn>` clone
  in `pending_notify[node].sinks` is alive).
- **Why deferred:** pre-existing pattern from Slice A-bigger (the
  subscribe-vs-emit race fix). The per-tier split multiplies the panic
  surface but doesn't introduce the leak. `Drop for CoreState` releases
  the sink Arc when the last `Core` drops, so it's not a memory leak,
  but a panicking sink could keep panicking on every subsequent wave's
  flush. Real fix lands with the staging-buffer machinery for the
  handshake re-entrance lift.
- **Source:** Slice A close /qa Blind Hunter finding (2026-05-05).

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

### Subscribe-time handshake fires lock-held; re-entrance from handshake panics

- **What:** `Core::subscribe` registers the new sink under the state
  lock and fires the per-tier handshake (`[Start]`, optionally
  `[Data(cache)]`, `[Complete]` / `[Error(h)]`, `[Teardown]`) to that
  sink while still holding the lock. A handshake-time sink callback
  that calls back into Core hits the **`IN_HANDSHAKE_FIRE`
  thread-local diagnostic** (Slice A-bigger /qa item A) and panics
  with a clear message instead of silently deadlocking. Verified by
  `handshake_sink_reentry_panics_with_diagnostic_not_deadlocks` in
  `tests/lock_discipline.rs`.
- **Why divergent:** the lock-held handshake is the trade-off for the
  subscribe-vs-emit race fix (Slice A-bigger). Atomic register-and-fire
  keeps Start strictly before any concurrent wave's tier-1+ messages.
  The alternative (register-then-drop-then-fire) re-introduces the
  ordering hazard: a concurrent flush could deliver wave messages to
  the new sink before our handshake fires.
- **Why still here after Slice A close:** Slice A close lifted fn
  re-entrance and custom_equals re-entrance, and split the handshake
  into per-tier sink calls (closing R1.3.5.a). But the handshake still
  fires lock-held — lifting it requires per-sink "handshake pending"
  staging-buffer machinery (queue concurrent wave messages destined
  for this sink until handshake completes, then drain). Larger
  surgery than the Slice A close refactor; tracked for M1-close+
  follow-up.
- **Source:** Slice A-bigger (2026-05-05); diagnostic added in Slice
  A-bigger /qa (2026-05-05); per-tier delivery landed in Slice A
  close (2026-05-05).

### Deactivation cleanup not yet modeled

- **What:** when the last subscriber unsubscribes from a derived/dynamic node,
  TS clears its cache and releases handles. Rust currently no-ops the
  unsubscribe path's deactivation.
- **Why deferred:** correct under "always subscribed" lifetimes (which all
  current tests assume). Lands when the graph layer (M2) introduces
  subgraph mount/unmount that makes deactivation observable.
- **Source:** [node.rs](../crates/graphrefly-core/src/node.rs) inline comment.

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

### napi-rs operator binding parity not yet shipped

- **What:** `BenchCore` ([`crates/graphrefly-bindings-js/src/core_bindings.rs`](../crates/graphrefly-bindings-js/src/core_bindings.rs)) does NOT expose `register_map` / `register_filter` / `register_scan` etc. The Slice C-1 substrate is reachable from JS only via direct `Core::register_operator` + a binding-side `OperatorBinding` impl that doesn't yet exist for napi.
- **Why deferred:** operator factories take `Box<dyn Fn(HandleId) -> HandleId + Send + Sync>` closures. Wiring JS callbacks across napi requires tsfn (thread-safe function) plumbing — same shape needed for the deferred TSFN-based JS-callback fns / TSFN custom-equals work (see "Slice B — napi-rs binding parity" closing section in `migration-status.md`). Bundling them lets one tsfn refactor cover both paths.
- **Lift point:** when a JS consumer needs operators OR when TSFN custom-equals lands, expose `register_map(deps, project_fn)` etc. on `BenchCore` (or a richer `BenchOperators` companion class). Wire `parity-tests/impls/rust.ts` to flip non-null at the same time so the existing transform parity scenarios activate against rustImpl.
- **Source:** Slice C-1 close (2026-05-06).

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

### DIRTY-without-DATA across pause-resume boundary

- **What:** Spec §1.3.1.b says "every DIRTY is followed by tier-3 in the
  same wave." When a node is paused mid-wave, DIRTY (tier 1) flushes
  immediately while DATA (tier 3) buffers. Subscribers see DIRTY without
  the matching DATA/RESOLVED until resume drains the buffer.
- **Why divergent:** matches TS production behavior — pause is explicitly
  out-of-wave for tier-3 ordering. The "same-wave" pairing invariant
  applies to non-paused waves only. Subscribers that pair DIRTY with
  expected tier-3 must tolerate cross-wave delivery under pause. Document;
  do not change.
- **Source:** Edge Case Hunter QA finding, 2026-05-05; TS reference at
  `~/src/graphrefly-ts/src/core/node.ts:3179-3192` (only `resumeAll` mode
  buffers; default mode in TS suppresses fn-fire instead). Rust v1 unifies
  on the buffer-and-replay shape.

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
- **Why deferred:** correct for v1 (terminal nodes are one-shot at this
  layer; no node deletion / unsubscribe-deactivation yet). Real fix lands
  with M2 graph-layer node-deletion and the deactivation cleanup path —
  both will release these slots when the node leaves the live graph.
- **Source:** Migration-status.md "Carried forward" section reinforced by
  Blind Hunter QA finding, 2026-05-05.

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
