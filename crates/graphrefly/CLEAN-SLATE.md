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
| `node` | **kernel + control/terminal impl** — `Core` (erased wave engine) + typed `Node<T>` (state/state_empty/producer/derived; `up`/`down` public per R-node-iface; `Clone` handle). Two-phase DIRTY→DATA, diamond pending-join (R-diamond/R-two-phase), first-run gate (R-first-run-gate), **every occurrence is DATA + substrate-synthesized undirty RESOLVED (R-resolved-undirty / D49)**, push-on-subscribe (R-push-subscribe), lazy activation, ROM/RAM (R-rom-ram), `Drop` cleanup (D1). **+INVALIDATE (idempotent `!has_data`, `dep_prev`→SENTINEL, dirty un-wedge, same-wave merge — C-3)**, **+PAUSE/RESUME lockset + default-mode coalesce (C-5)**, **+COMPLETE/ERROR terminal (terminal-is-forever, D17)**, **+the D30 catch boundary** = `catch_unwind` at the wave-owner converting a panic (Rust throw analogue) → `[[ERROR,e]]` on the blamed cycle node (C-6), with the **`WaveScope` touched-set** whole-cascade wave-flag recovery (**B25 closed**). |
| `dispatcher` | **impl** — `Dispatcher` + LocalSync **slotmap pool** (`register`/`unregister`/`invoke`, clone-out-before-invoke, generation-checked); `Handle` widened with a local `generation` (B32, per-language); thread-local default (D26). |
| `ctx` | **impl** — `Ctx` (`down`/`emit`/typed `data`/`prev`/`state`/`on_deactivation`/**`on_invalidate`**); cleanup hooks are **per-run** (cleared each fn invoke + re-registered, R-cleanup-hooks / C-14); `up` validates R-ctx-up + self-handles PAUSE/RESUME. |
| `batch` | contract stub — `TODO` (deferred slice). |

**15 tests green** (clippy clean, fmt clean, `#![forbid(unsafe_code)]`): 12 unit
(push-on-subscribe, D49 occurrence-stays-DATA, D49 filter no-emit→undirty RESOLVED,
producer activation-exemption, first-run gate, diamond exactly-once two-phase,
ROM/RAM deactivation, D1 drop cleanup) + **3 conformance** in `tests/conformance.rs`
(**C-3** INVALIDATE×state×onInvalidate, **C-5** PAUSE lockset multi-source, **C-6**
sync feedback cycle→ERROR incl. the B25 recovery regression, teeth-verified).

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

**Deferred to later slices** (per the agreed sequencing): TEARDOWN, the
resumeAll/false pause modes + async-paused buffering (C-2/C-9/C-10, need the async
pool), up-at-source INVALIDATE terminus (C-7/D38), batch, LocalAsync/async, rewire
(C-8), `ctx.rewire_next`/C-11. Still open (not pre-committed): pool callback/handle
resolution for async, async-pool re-entry onto the single thread.

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
| 2 | **C-2** | async result at paused node | LocalAsync pool + buffer/replay |
| 2 | **C-4** | mixed sync/async diamond | async serializes onto the single thread (R-graph-domain) |
| 2 | **C-9** | `pausable:false` async source ignores PAUSE | D44 outer gate |
| 2 | **C-10** | `true`-mode async leaf source delivers own production | D44 carve-out |
| 3 | **C-7** | upstream control at a depless source | D38 terminus honor/drop |
| 4 | **C-8** | intra-graph runtime rewire | D42 substrate rewire (setDeps/addDep/removeDep) |
| 5 | **C-1** | cross-graph diamond coalesce | needs wire bridge (B2, post-1.0); in-process core only for now |
| **last** | **C-11** | higher-order inner rewire at the wave boundary | D47 `ctx.rewire_next`; spec `R-rewire-deferred` is DRAFT; B15 (fn-swap handle GC) hard prereq; do AFTER TS drives C-11 green |

## Cross-track sequencing

- **Python (CSP-7) deferred** until this Rust arm drives C-2..C-10 green — Rust's
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
