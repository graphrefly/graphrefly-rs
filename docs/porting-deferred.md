---
title: Porting flags & deferred concerns
last_updated: 2026-05-05
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

### Fn re-entrance via `BindingBoundary::invoke_fn` and `custom_equals` still forbidden

- **What:** `flush_notifications` now drops the state lock before firing
  sinks (Slice A-bigger), so wave-end sinks can re-enter Core safely.
  But the lock is **still held across two binding callbacks**:
  - `BindingBoundary::invoke_fn` in `fire_fn` — fn callbacks that
    re-enter Core deadlock.
  - `BindingBoundary::custom_equals` in `commit_emission` →
    `handles_equal` — custom-equals callbacks that re-enter Core
    deadlock.
- **Why deferred:** lifting both requires `run_wave` / `drain_and_flush`
  / `fire_fn` / `commit_emission` to take ownership of the `MutexGuard`
  (so per-iteration drop-around-binding-call becomes possible) and
  refactoring all 5 `run_wave` callers + `activate_derived` +
  `BatchGuard::drop`. Estimated 400–500 LOC of churn — scoped out of
  Slice A-bigger /qa (2026-05-05) to keep the touch surface manageable;
  tracked here for a follow-up slice. Sink re-entrance — the more
  common pattern — is fully supported.
- **Source:** Slice A-bigger (2026-05-05); scope confirmed in Slice
  A-bigger /qa (2026-05-05) when item E option 2 was scoped out.

### Subscribe-time handshake fires lock-held; re-entrance from handshake panics

- **What:** `Core::subscribe` registers the new sink under the state
  lock and fires the handshake `[Start, Data(cache)?, ...]` to that sink
  while still holding the lock. A handshake-time sink callback that
  calls back into Core hits the **`IN_HANDSHAKE_FIRE` thread-local
  diagnostic** (Slice A-bigger /qa item A) and panics with a clear
  message instead of silently deadlocking. Verified by
  `handshake_sink_reentry_panics_with_diagnostic_not_deadlocks` in
  `tests/lock_discipline.rs`.
- **Why divergent:** the lock-held handshake is the trade-off for the
  subscribe-vs-emit race fix (Slice A-bigger). Atomic register-and-fire
  keeps Start strictly before any concurrent wave's tier-1+ messages.
  The alternative (register-then-drop-then-fire) re-introduces the
  ordering hazard: a concurrent flush could deliver wave messages to
  the new sink before our handshake fires. Lifts together with the
  pending `MutexGuard`-ownership refactor — that work also lets the
  handshake split into separate tier-grouped sink calls (closing the
  R1.3.5.a divergence).
- **Source:** Slice A-bigger (2026-05-05); diagnostic added in Slice
  A-bigger /qa (2026-05-05).

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

### Subscribe handshake delivered as single sink call (R1.3.5.a divergence)

- **What:** `Core::subscribe` synthesizes the START handshake (`[Start]`,
  optionally `[Start, Data(v)]` for cached state) under the state lock and
  fires it via a single `sink(&handshake)` call. TS's R1.3.5.a routes
  `Start` through the same `downWithBatch` path as other messages, so a
  cached source's handshake splits into two sink calls (tier 0 `[Start]`,
  then tier 3 `[Data(v)]`).
- **Why divergent:** v1 simplification — subscribe is out-of-wave, so the
  handshake doesn't need batch deferral. The single-call shape is observed
  by `inv7_handshake_first_message_is_start_*` (Slice C-2 proptest).
- **Why still here after Slice A-bigger:** the per-tier split would land
  naturally when fn re-entrance lifts (the same `MutexGuard`-ownership
  refactor lets the handshake route through `pending_notify` + the
  two-pass tier-then-node flush). Held back from Slice A-bigger because
  that refactor was scoped out alongside fn re-entrance.
- **Source:** Slice C-2 (2026-05-05); test expectation aligned to the
  current Rust shape pending the lock-discipline rework.

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
