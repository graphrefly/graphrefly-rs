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

### Slice status (CSP-5 first cut — LANDED 2026-05-29, /qa hardened)

| Module | State |
|---|---|
| `protocol` | **concrete** — `Tier` (D34), `Message`/`Wave` (D8/D9, kind-only `Debug`), `Handle` (D7), `LockId` (D10), `GraphError` (D31), `AnyValue`. |
| `node` | **kernel impl** — `Core` (erased wave engine) + typed `Node<T>` (state/state_empty/producer/derived). Two-phase DIRTY→DATA, diamond pending-join (R-diamond/R-two-phase), first-run gate (R-first-run-gate), **every occurrence is DATA + substrate-synthesized undirty RESOLVED for a no-emit fn (R-resolved-undirty / D49, no equals-substitution)**, push-on-subscribe (R-push-subscribe), lazy activation, ROM/RAM (R-rom-ram), **`Drop` cleanup (D1: detach upstream subs + fire `on_deactivation`)**. |
| `dispatcher` | **impl** — `Dispatcher` + LocalSync pool, `Handle` register/invoke (clone-out-before-invoke); thread-local default (D26). |
| `ctx` | **impl** — `Ctx` (`down`/`emit`/typed `data`/`prev`/`state`/`on_deactivation`); `up` validates R-ctx-up. |
| `batch` | contract stub — `TODO` (deferred slice). |

**12 unit tests green** (clippy clean, fmt clean, `#![forbid(unsafe_code)]`):
push-on-subscribe, **D49 occurrence-stays-DATA**, **D49 filter no-emit→undirty
RESOLVED**, producer activation-exemption, first-run gate, **diamond exactly-once
two-phase**, ROM/RAM deactivation, **D1 drop cleanup**.

**Deferred to later slices** (per the agreed sequencing): INVALIDATE/onInvalidate,
COMPLETE/ERROR terminal, TEARDOWN, PAUSE/RESUME lockset, batch, LocalAsync/async,
rewire (C-8), `ctx.rewire_next`/C-11. The D37 re-entrancy guard currently `panic!`s
(stand-in); proper `[[ERROR,e]]` graph-layer conversion (D30) lands with the
terminal slice + C-6, which is ALSO where whole-cascade panic-recovery lands
(**backlog B25** — a panicking fn leaves source/intermediate wave-flags stale on the
unwind path; unobservable now as the sync core has no catch boundary). Still open
(not pre-committed): pool callback/handle resolution for async, async-pool re-entry
onto the single thread.

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

| Order | Scenario | Covers | Notes |
|---|---|---|---|
| 1 | **C-3** | INVALIDATE × ctx.state × onInvalidate | pure sync + lifecycle hooks |
| 1 | **C-5** | PAUSE lockset multi-source | pause lockset correctness |
| 2 | **C-2** | async result at paused node | LocalAsync pool + buffer/replay |
| 2 | **C-4** | mixed sync/async diamond | async serializes onto the single thread (R-graph-domain) |
| 2 | **C-9** | `pausable:false` async source ignores PAUSE | D44 outer gate |
| 2 | **C-10** | `true`-mode async leaf source delivers own production | D44 carve-out |
| 3 | **C-6** | sync feedback cycle → ERROR | D37 re-entrancy reject |
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
