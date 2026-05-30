# `graphrefly` — Rust clean-slate substrate (CSP-5)

> **Authority:** `~/src/graphrefly` (branch `clean-slate`). This crate implements
> the language-neutral protocol; that repo wins on any disagreement. Do not
> duplicate its content here — cite D#/R-IDs and point.

This is the **single self-contained crate** (`@graphrefly/rust`, D32) replacing the
retired port-model 8-crate workspace. The old crates (`graphrefly-core` /
`-graph` / `-operators` / `-storage` / `-structures` + 3 bindings) are **frozen as
read-only reference** (D41 analogue) — `exclude`d from the workspace, deleted once
this crate reaches intent-parity. They are source-as-reference during re-derivation,
not a build target.

## Scope — substrate only

CSP-5 builds the wave-protocol substrate: `protocol` + `node` + `dispatcher`
(LocalSync + LocalAsync pools) + `ctx` + `batch` + rewire. The graph layer / 8-verb
sugar / operators / sources / inspection are **later per-language phases** (the
CSP-2-rs equivalents, D6/D24 never in parity) and are NOT in this skeleton.

### Value representation (locked impl decision, 2026-05-29)

**Per-language impl choice — NOT a protocol change (no spec-amend, no D#)**, user-
approved via `/dev-dispatch` Phase 2: heterogeneous dep/message values cross the
substrate **erased as `AnyValue = Rc<dyn Any>`** (the Rust analogue of TS's
`unknown`). One DATA payload fans out to N sinks + cache + `prev_data` sharing one
allocation via refcount; single-thread (D22) ⇒ `Rc`+`dyn Any` (no `Send`/`Sync`);
the user fn downcasts (`ctx.data::<U>(i)`). A **typed `Node<T>` facade ships now**
(not deferred to the graph layer) and re-types the boundary (`cache()`/`set()`
downcast). **No `PartialEq` bound** — D49 removed substrate value-equality (dedup is
opt-in at the operator layer). Rejected: handle-protocol id-registry (retired
port-model tax; reserve for the wire bridge), `Box<dyn Any>` (forces a value-clone
per extra sink). **Borrow discipline** (the Rust-vs-TS divergence): the wave engine
never holds a `RefCell` borrow across a re-entrant call — clone the callable out,
drop the borrow, then call.

### Dispatcher pool = slotmap + widened Handle (locked impl decision, 2026-05-29)

**Per-language impl choice — NOT a protocol change (no spec-amend, no D#)**, user-
approved via `/dev-dispatch` Phase 2 (B32): the dispatcher pool is a **slotmap**
(`slots: Vec<Slot{f: Option<NodeFn>, generation}>` + a free-list), and a node's fn
slot is **freed on `NodeInner::Drop`** (`dispatcher.unregister`). Without this the
pool held every registered fn for the whole process, pinning the upstream `Core`s
the fn captured (the no-GC leak; `Node: Clone` makes capturing handles-into-fns the
idiomatic feedback pattern). The registering node isn't pinned by the pool (its fn
captures *upstreams*, not self), so its `Drop` fires and unregisters — releasing the
captures. A fn that captures its OWN node is a genuine `Rc` cycle this can't break
(use a `Weak` self-handle). **`Handle` is widened to `{pool_id, handle_id,
generation}`**: the `generation` is a per-language slotmap detail (D24), **NOT** part
of the protocol/IDL `Handle` (D7/DR-2 stays `(pool_id, handle_id)`) — it is
wire-meaningless and dropped at the wire boundary (a wired handle re-resolves against
the remote pool). `invoke` validates the generation → a stale handle (e.g. a future
async-pool deferred callback firing after the node dropped + slot recycled) is a
silent no-op, never a recycled-slot misfire. Rust-only; B15 is the TS sibling
(separate code). Rust rewire fn-swap unregister lands when Rust rewire (C-8) does.

### Slice status (CSP-5 — first kernel + control/terminal slice, LANDED 2026-05-29)

| Module | State |
|---|---|
| `protocol` | **concrete** — `Tier` (D34), `Message`/`Wave` (D8/D9, kind-only `Debug`), `Handle` (D7), `LockId` (D10), `GraphError` (D31), `AnyValue`. |
| `node` | **kernel + control/terminal impl** — `Core` (erased wave engine) + typed `Node<T>` (state/state_empty/producer/derived; `up`/`down` public per R-node-iface; `Clone` handle). Two-phase DIRTY→DATA, diamond pending-join (R-diamond/R-two-phase), first-run gate (R-first-run-gate), **every occurrence is DATA + substrate-synthesized undirty RESOLVED (R-resolved-undirty / D49)**, push-on-subscribe (R-push-subscribe), lazy activation, ROM/RAM (R-rom-ram), `Drop` cleanup (D1). **+INVALIDATE (idempotent `!has_data`, `dep_prev`→SENTINEL, dirty un-wedge, same-wave merge — C-3)**, **+PAUSE/RESUME lockset + default-mode coalesce (C-5)**, **+COMPLETE/ERROR terminal (terminal-is-forever, D17)**, **+the D30 catch boundary** = `catch_unwind` at the wave-owner converting a panic (Rust throw analogue) → `[[ERROR,e]]` on the blamed cycle node (C-6), with the **`WaveScope` touched-set** whole-cascade wave-flag recovery (**B25 closed**). **+Slice A (async):** `producer_async`/`derived_async`/`*_opts` constructors + `Pausable {True\|ResumeAll\|False}` + `pause_buffer` + `should_buffer_on_pause` (async COMPUTE buffers in `true`, leaf source immediate, `resumeAll` buffers own production) + `on_resume` drain/replay + `owned_down` (the DeferredCtx late-emit boundary) + the async-pool undirty-RESOLVED exemption (C-2/C-4/C-9/C-10). |
| `dispatcher` | **impl** — `Dispatcher` + LocalSync **and LocalAsync slotmap pools** (`pools: Vec<Pool>` by `pool_id`; `register`/`register_async`/`unregister`/`invoke`/`pool_kind`, clone-out-before-invoke, generation-checked); the async `invoke` is still **sync void** (R-sync-core) — async is a node-kind label. `Handle` widened with a local `generation` (B32, per-language); thread-local default (D26). |
| `ctx` | **impl** — `Ctx` (`down`/`emit`/typed `data`/`prev`/`state`/`on_deactivation`/**`on_invalidate`**/**`defer`**); cleanup hooks are **per-run** (cleared each fn invoke + re-registered, R-cleanup-hooks / C-14); `up` validates R-ctx-up + self-handles PAUSE/RESUME. **`defer()` → `DeferredCtx`** is the owned `'static` async late-emit handle (Slice A). |
| `batch` | contract stub — `TODO` (deferred slice). |

**28 unit + 15 conformance tests green** (clippy `-D warnings` clean, fmt clean,
`#![forbid(unsafe_code)]`). Conformance: **C-3** INVALIDATE×state×onInvalidate, **C-5**
PAUSE lockset multi-source, **C-6** sync feedback cycle→ERROR (+B25 recovery), **C-13**
INVALIDATE×PAUSE precedence, **C-14** per-run cleanup hooks (control/terminal slice),
**C-2 / C-4 / C-9 / C-10** (Slice A async), **C-7** up-at-source, **C-8** intra-graph
rewire, **C-11** higher-order inner rewire at the wave boundary (`ctx.rewire_next`),
**C-12** occurrences-stay-DATA (D49), **C-15** dep-terminal-settles-dirty (B35).
Unit tests add the Slice A `resumeAll` buffer+replay case + 12 rewire cases.
Teeth-verified: Slice A (async-compute buffer / async undirty exemption), C-7 (terminus
INVALIDATE), C-8 (idx-box reroute / atomic multi-add defer), QA-F1 (DepMutationGuard),
C-15 (`release_dep_dirty` + the settle-fallback), C-11 (deferral via `deferred_ok` + the
immediate-self-rewire→ERROR facet).

**/qa 2026-05-30 (3 review arms: blind / spec / borrow-unwind).** FIXED **QA-F1**
(critical, consensus): a user fn panicking mid-`rewire_apply` (an added dep's activation
fn, or a removed dep's `onDeactivation` hook) left the node permanently
`in_dep_mutation=true` (the wave-owner catch's `reset_wave_flags` doesn't cover it) →
every future recompute deferred + every future rewire rejected reentrant. Fix = a
`DepMutationGuard` RAII clearing the flag on unwind (mirrors the TS `finally`);
regression test teeth-verified (disable → the node stays wedged). DEFERRED: **B16** widened
to `lang:all` (the half-installed-subscription + half-applied-arrays leftover on a
rewire-internal panic — parity-consistent, only reachable via such a panic); **B37** new
(`DeferredCtx` holds a strong `Core` → over-retention if an async result is abandoned —
strong-vs-`Weak` decision deferred to the async runner, B1). Confirmed clean: idx-box
reroute identity, parallel-array rebuild, `pending` accounting, borrow discipline,
`Weak` upgrade, fn-swap GC, `with_wave_owner` nesting. Below-threshold notes: cycle DFS
is O(V²) vs spec O(V+E) (per-language perf nit); an async filter-reject has no escape
hatch (parity-consistent with the C-4 deferred≠rejected exemption).

**D30 catch + B25 (per-language impl, D24 — like the `AnyValue` value-rep decision;
no spec-amend / no D#):** Rust has no graph layer yet, so the catch lives at the
substrate **wave-owner** — the outermost public `subscribe`/`down`/`up`/`set` entry
installs a thread-local `WaveScope {owner, touched, blamed}` and runs the cascade
under `catch_unwind`. A `panic!` is the Rust analogue of a value-level `throw`
(R-reentrancy mandates an unwind, not a graceful return). On a caught panic the
touched-set resets every participant's transient wave-flags to baseline (leaving
persisted `cache`/`dep_prev`/`status`/`ctx.state` intact), then emits `[[ERROR,e]]`
from `blamed` (the innermost fn that ran = a node ON the cycle). This `WaveScope` is
the same committed wave boundary that **batch (D12)** and **`ctx.rewire_next` (D47)**
will reuse.

**Async slice (Slice A, LANDED 2026-05-30 — /dev-dispatch):** LocalAsync pool
(dispatcher `Vec<Pool>` + `register_async` + `pool_kind`; the async `invoke` is still
sync void — async is a node-kind label, the fn defers via a stashed `ctx.defer()`),
`DeferredCtx` (owned `'static` late-emit, the Rust analogue of the TS async fn
capturing `ctx`; `&Ctx` cannot outlive `invoke`), and the node pause modes (`Pausable
{True|ResumeAll|False}` + `pause_buffer` + `should_buffer_on_pause` + `on_resume`
drain) + the async-pool undirty-RESOLVED exemption (a deferred no-emit ≠ reject, C-4).
Drives **C-2 / C-4 / C-9 / C-10** green. `resumeAll` is at the TS partial (B9) level
(own-production buffer+replay; no pre-pause baseline-equals fidelity; node.rs unit
test, no conformance scenario). Both new mechanisms teeth-verified.

**Up-at-source + rewire slices (LANDED 2026-05-30 — /dev-dispatch):** C-7 (D38 —
`Core::up` terminus/forward) and C-8 (D42 — `setDeps`/`addDep`/`removeDep` + `Core::rewire`,
the per-dep dispatch refactored to a shared `Rc<Cell<i64>>` CURRENT-index box for the
surgical Option-C reroute/drain, fn-swap GC, atomic multi-add settle, Q6 auto-settle).

**Dep-terminal + rewire-deferred slice (LANDED 2026-05-30 — /dev-dispatch, all
Rust-actionable work this session):** **C-15** (R-terminal-settles-dirty / B35 +
R-deps-terminal) — `receive_from_dep` COMPLETE/ERROR arms `release_dep_dirty` (the dep's
in-wave DIRTY contribution, the exactly-one-settle invariant) + `settle_after_absorbed_terminal`
(sawData→`maybe_run` else an undirty RESOLVED, with the gate-holds fallback) +
`terminal_as_real_input`→`maybe_run` + `complete_when_deps_complete`/`error_when_deps_error`
auto-cascade; `NodeOpts` gains the three flags (manual `Default` = cascade-on), `DepTerminal
{Complete | Error(Rc<str>)}` (the error **message** — `Box<dyn Error>` is not `Clone`, D31) +
`DepRecord.terminal`, `all_deps_settled` terminal-as-input carve-out. **C-12** (occurrences-stay-DATA,
D49 — already in the substrate from CSP-5; take/distinctUntilChanged expressed as inline
`ctx.state` derived nodes, no operator library). **C-11** (`ctx.rewire_next` — a `DEFERRED_REWIRE`
thread-local FIFO + `DRAINING` re-entrancy guard, drained at the `with_wave_owner` committed
boundary, **reusing the B25 `WaveScope`** = the Rust analogue of the TS `boundary.ts`;
`request`/`apply_rewire_next` add/remove/set reuse the R-rewire surgical path (C-8),
terminal-discards-queue, a per-apply reject→ERROR via `owned_down`; `Ctx::rewire_next_add/remove/set`).
**Parity fix (Rust-only, pre-existing):** the `run_wave` undirty-RESOLVED synthesis now exempts a
TERMINAL wave (`+ !n.terminal`, mirroring the TS `_terminal === undefined` guard) — surfaced by
C-11 facet 5 (a fn emitting a bare COMPLETE). The B16 QA-F1 rewire-panic unit test was adapted
to construct its node `error_when_deps_error:false` (the new auto-error-cascade correctly
TERMINATES a consumer whose dep errors, so "d survives + recovers" is kept observable by
absorbing). **The Rust conformance arm is now FULLY GREEN C-2..C-15 — only C-1 remains
(wire-bridge B2, post-1.0); CSP-6 effectively complete.**

**Deferred to later slices** (per the agreed sequencing): TEARDOWN, batch. Still open (not
pre-committed): a real async-pool runner (callback/handle resolution) once a consumer needs
genuine wall-clock async beyond the deterministic deferred-emit boundary; the rewire-mid-batch /
rewire×async-late-ctx edges (TS backlog B17–B19) once batch lands; `ctx.rewire_next` on the
**async** `DeferredCtx` (for async-source *Map) — gated on the Rust graph layer; the higher-order
`*Map` SUGAR (a future Rust graph layer, CSP-2-rs).

## Floor (cite, never violate)

- **D22** — graph = single-thread causal/concurrency domain ⇒ this crate is
  `!Send + !Sync`, `Rc<RefCell<…>>` not `Arc<Mutex<…>>`; actor model dropped.
- **F-SYNC-CORE** — `dispatcher.invoke` sync `()`, async only in pools / wire bridge.
- **F-DISPATCH-ALL** — every node fn goes through the dispatcher.
- **D32** — self-contained package, no cross-language peer-deps.
- **D24** — parity = behavioral conformance via the language-neutral spec (only
  the substrate is in parity; operators/sugar/inspection are per-language).

## Conformance target map (CSP-6 — Rust arm)

`CSP-6` deps = `CSP-3` (TS arm, done/green) + `CSP-5`. Drive each scenario in
`~/src/graphrefly/spec/conformance.jsonl` to `runtimes.rust = pass`, in this order
(mirrors how the TS arm hardened the spec — core first, deferred-rewire last):

| Order | Scenario | Covers | Status / Notes |
|---|---|---|---|
| 1 | **C-3** | INVALIDATE × ctx.state × onInvalidate | ✅ **GREEN** 2026-05-29 (pure sync + lifecycle hooks) |
| 1 | **C-5** | PAUSE lockset multi-source | ✅ **GREEN** 2026-05-29 (default-mode lockset correctness) |
| 3 | **C-6** | sync feedback cycle → ERROR | ✅ **GREEN** 2026-05-29 (D37 reject + D30 wave-owner catch + B25 recovery) |
| 2 | **C-2** | async result at paused node | ✅ **GREEN** 2026-05-30 (Slice A — async COMPUTE node buffers, on_resume replays) |
| 2 | **C-4** | mixed sync/async diamond | ✅ **GREEN** 2026-05-30 (Slice A — deferred async leg, undirty-RESOLVED exemption) |
| 2 | **C-9** | `pausable:false` async source ignores PAUSE | ✅ **GREEN** 2026-05-30 (Slice A — D44 outer gate) |
| 2 | **C-10** | `true`-mode async leaf source delivers own production | ✅ **GREEN** 2026-05-30 (Slice A — D44 leaf carve-out) |
| 3 | **C-7** | upstream control at a depless source | ✅ **GREEN** 2026-05-30 (D38 — depless terminus honors INVALIDATE, drops DIRTY/TEARDOWN; intermediate forwards) |
| 4 | **C-8** | intra-graph runtime rewire | ✅ **GREEN** 2026-05-30 (D42 — setDeps/addDep/removeDep; Rc<Cell<i64>> idx-box reroute/drain; atomic multi-add settle) |
| 5 | **C-15** | a dep's terminal releases its in-wave DIRTY (no wedge) | ✅ **GREEN** 2026-05-30 (B35/R-deps-terminal — release + absorbed-terminal settle + terminal_as_real_input + auto-cascade; teeth-verified) |
| 6 | **C-12** | occurrences stay DATA; RESOLVED undirty-only | ✅ **GREEN** 2026-05-30 (D49 already in substrate; take/distinctUntilChanged as inline ctx.state derived nodes) |
| 7 | **C-11** | higher-order inner rewire at the wave boundary | ✅ **GREEN** 2026-05-30 (D47 `ctx.rewire_next` — DEFERRED_REWIRE FIFO drained at the WaveScope boundary; `R-rewire-deferred` active TS-side; Rust handle-GC B32 done) |
| last | **C-1** | cross-graph diamond coalesce | needs wire bridge (B2, post-1.0); in-process core only for now |

**Arm status:** C-2..C-15 all `rust:pass` — the Rust conformance arm is **fully green**;
only **C-1** remains (wire-bridge-blocked, B2/post-1.0). CSP-6 effectively complete.

## Cross-track sequencing

- **Python (CSP-7) deferred** until this Rust arm drives C-2..C-15 green (now done) — Rust's
  ownership/no-GC model is the high-signal second implementation that stress-tests
  the spec's language-neutrality; Py is GC + semantically close to TS, so opening
  it now adds low marginal hardening signal and triples redo risk against the still-
  draft C-11.
- **Convergence-policy bar** (substrate changes): G1 spec citation + G2 cross-arm +
  G3 edge-case design call before minting a D# — see
  `feedback_three_gate_substrate_convergence` (do not ratify accidents).

## Follow-ups (surfaced, not yet done)

- Rewrite the repo root `CLAUDE.md` (still describes the port-model) for clean-slate
  — awaiting approval.
