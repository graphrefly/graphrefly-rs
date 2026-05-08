# Report 005 — M3 Slice F + audit follow-on + Slice G + Slice E1 + Slice H (current frontier)

**Slices covered:** Slice F (canonical-spec correctness pass), Slice F audit follow-on (tier-based dispatch refactor + `Core::up` + UpError), Slice F audit follow-on /qa (A1–A10 fixes + cat-B doc), Slice G (R1.3.2.d per-wave equals coalescing), Slice E1 (replay buffer / R2.6.5 / Lock 6.G), Slice H (`register*` + `set_pausable_mode` typed-error promotion + Slice H /qa). All landed 2026-05-07.

**Test count progression:** 388 (Slice E) → 405 (Slice F) → 411 (audit follow-on incl. F2 + tier refactor) → 411 (Slice G — same count; in-place rewrites tested separately) → (411 + Slice E1 within F) → 417 (Slice F audit follow-on /qa) → 427 (Slice H mid-/qa) → **438 (Slice H + /qa final)**.

---

## 1. Why these slices matter

The Slice E rework had unblocked higher-order ops, but a systematic walk through the canonical spec (post-Slice-E) surfaced **10+ documented divergences** that had accumulated through M1–M3 Slice E. Slice F is the **comprehensive canonical correctness pass** — fixing them in-slice rather than deferring to a "v1 limitations" pile.

Slice G is the in-place fix for the bug that Slice F's audit `debug_assert` exposed: per-emit equals substitution applying within a batch violated R1.3.2.d.

Slice E1 lands the replay buffer (R2.6.5 / Lock 6.G) so subscribe handshakes can deliver buffered DATA between Start + cache and any terminal slice.

Slice H promotes ~150 call sites of `register*` + `set_pausable_mode` from `assert!` panics to typed `Result<_, RegisterError>` / `Result<_, SetPausableModeError>`. The /qa pass surfaced and fixed a TOCTOU window via `ScratchReleaseGuard` RAII.

---

## 2. Behavioral traces

### Trace 1 — Pause-overflow ERROR synthesis (Slice F A3 / R1.3.8.c / Lock 6.A)

| Step | Event | Internal state change | Observable output |
|------|-------|----------------------|-------------------|
| 1 | `set_pause_buffer_cap(node, Some(2))` | NodeRecord.pause_buffer_cap = 2 | — |
| 2 | `pause(node, lockId=L1)` | enter PAUSED | — |
| 3 | `emit(node, h_a)` | pause_buffer = [h_a] (under cap) | — |
| 4 | `emit(node, h_b)` | pause_buffer = [h_a, h_b] (= cap) | — |
| 5 | `emit(node, h_c)` | pause_buffer.len > cap → `synthesize_pause_overflow_error(node, droppedCount=1, cap=2, lock_held_ms=…)` | — |
| 6 | error cascade | binding allocates `Box<PauseOverflowDiagnostic{...}>` → error_handle; `Core::error(node, error_handle)` | sink: `[Error(diagnostic_handle)]` (after RESUME) |

Pre-Slice-F this was a silent drop. Now structured ERROR with `{nodeId, droppedCount, configuredMax, lockHeldDurationMs}`.

**Diagrams:** [F11.1](#fc-11.1) Pause overflow ERROR.

### Trace 2 — `PausableMode { Default, ResumeAll, Off }` consolidation (Slice F Item-5)

| Step | Event | Internal state change | Observable output |
|------|-------|----------------------|-------------------|
| 1 | `set_pausable_mode(d, Default)` (where d is derived on s) | NodeRecord.pausable_mode = Default | — |
| 2 | `pause(d, L1)` | d enters PAUSED | — |
| 3 | `emit(s, h1)` | s wave fires: d would normally fire, but PAUSED Default → buffer dep delivery only (one slot per dep) | — |
| 4 | `emit(s, h2)` | replace dep delivery (consolidate) | — |
| 5 | `resume(d, L1)` | fire fn ONCE with `[h2]` (consolidated) | sink: `[Data(fn(h2))]` not `[Data(fn(h1)), Data(fn(h2))]` |

`ResumeAll` (pre-Slice-F default) replays each delivery. `Off` is a pause no-op.

**Diagrams:** [F11.2](#fc-11.2).

### Trace 3 — R1.3.2.d per-wave equals coalescing (Slice G)

| Step | Event | Internal state change | Observable output |
|------|-------|----------------------|-------------------|
| 1 | `batch(|| { ... })` opens batch | — | — |
| 2 | `emit(s, h1)` | first emit — `tier3_emitted_this_wave.insert(s)`; equals → Resolved (Identity); `pending_notify[s] = Resolved` | — |
| 3 | `emit(s, h2)` | already in `tier3_emitted_this_wave` → skip equals, queue Data verbatim. **Retroactive rewrite:** any prior Resolved for s in pending_notify is rewritten to `Data(snapshot)` using wave-start cache. | `pending_notify[s] = [Data(snapshot), Data(h2)]` |
| 4 | drain wave | sink sees `[Data(snapshot), Data(h2)]` not `[Resolved, Data(h2)]` | — |

Pre-fix `[Resolved, Data(h2)]` violated R1.3.3.a (Resolved must discharge once per wave). Slice F audit added the dev-mode `debug_assert`; Slice G fixed and re-enabled it.

**Diagrams:** [F11.3](#fc-11.3).

Auto-resolve, Noop, empty-Batch, `settle_dirty_resolved` paths now also gate on `!any tier-3 in pending_notify` to prevent the same R1.3.3.a violation door.

### Trace 4 — Replay buffer subscribe handshake (Slice E1)

| Step | Event | Internal state change | Observable output |
|------|-------|----------------------|-------------------|
| 1 | `register_state(initial=h0, opts.replay_buffer=Some(2))` | NodeRecord.replay_buffer_cap = 2 | — |
| 2 | `emit(s, h1)` | commit: `push_replay_buffer(s, h1)` → buffer=[h1] | — |
| 3 | `emit(s, h2)` | buffer=[h1, h2] | — |
| 4 | `emit(s, h3)` | buffer.len > cap → evict h1 (deferred_handle_releases.push(h1)); buffer=[h2, h3] | — |
| 5 | `subscribe(s, sink)` LATE | handshake plan: `[Start, Data(cache=h3), Data(replay[0]=h2), Data(replay[1]=h3)]` | sink sees `[Start, Data(h3), Data(h2), Data(h3)]` |

🟨 **Slice F /qa A1 dedupe:** if last replay entry equals cache slice (Identity), skip the duplicate. Diagram: [F11.4](#fc-11.4).

### Trace 5 — `Core::up(node, message)` + `UpError` tier-based dispatch (Slice F audit follow-on)

| Step | Event | Internal state change | Observable output |
|------|-------|----------------------|-------------------|
| 1 | `core.up(node_id=999, message=Pause(L1))` | UnknownNode check first (A10) → 999 not registered → `Err(UpError::UnknownNode)` | — |
| 2 | `core.up(node, Data(h))` (tier 3) | tier check after node check → `Err(UpError::UnsupportedTier)` (use `Core::emit`) | — |
| 3 | `core.up(node, Pause(L1))` (tier 2) | `Core::pause(node, L1)` | — |
| 4 | `core.up(node, Invalidate)` (tier 2) | `Core::invalidate(node)` (R1.4.2: cascades via dep-walk in Core::invalidate; documented divergence vs plain-forward) | — |
| 5 | `core.up(node, Complete)` (tier 4) | `Core::complete(node)` | terminal cascade |

Higher-order outer sinks + producer sinks (~26 sites) refactored from `match m { Data | Complete | Error }` → `match m.tier()` + `payload_handle()`. **Diagrams:** [F11.5](#fc-11.5).

### Trace 6 — `register_operator` typed error path (Slice H)

| Step | Event | Internal state change | Observable output |
|------|-------|----------------------|-------------------|
| 1 | `core.register_operator(deps=[bogus_id], OperatorOp::Map(fn_id), opts)` | Phase 1: lock-released validation walks `deps`; `bogus_id` not in nodes → `Err(RegisterError::UnknownDep(bogus_id))` (no scratch allocated, no panic) | — |
| 2 | `core.register_operator(deps=[s, t], OperatorOp::Last{ default: NO_HANDLE }, opts)` | Phase 1: validation passes → Phase 2: `make_op_scratch(Last{default: NO_HANDLE})` returns `Err(SeedSentinel)` (Slice H widened) | `Err(RegisterError::OperatorSeedSentinel)` |
| 3 | concurrent `Core::complete(s)` between Phase 1 and Phase 3 | Slice H /qa F1+F2 fix: single state-lock acquisition + `ScratchReleaseGuard` RAII → if Phase 3 re-validation finds `s` newly terminal, return Err and release scratch lock-released | — |

**Diagrams:** [F8.2](#fc-8.2) `register_operator` flow; [F12.1](#fc-12.1) RegisterError; [F12.2](#fc-12.2) ScratchReleaseGuard.

### Trace 7 — `set_pausable_mode` widened to `UnknownNode` (D048)

| Step | Event | Internal state change | Observable output |
|------|-------|----------------------|-------------------|
| 1 | `core.set_pausable_mode(999, Default)` (unknown) | pre-Slice-H: panic via `require_node_mut`. Post-Slice-H/D048: `Err(SetPausableModeError::UnknownNode(999))` | — |
| 2 | `core.set_pausable_mode(node, Off)` while node PAUSED | Returns `Err(SetPausableModeError::WhilePaused)` (no mutation; F16 pinning test) | — |

D048 widened `UnknownNode` for consistency with `Core::up::UpError::UnknownNode` (A10 fix).

**Diagrams:** [F12.1](#fc-12.1).

### Trace 8 — `OperatorFactoryError` typed errors (Slice H /qa F7)

| Step | Event | Internal state change | Observable output |
|------|-------|----------------------|-------------------|
| 1 | `operators::combine(&core, &binding, sources=[], packer)` | sources.is_empty() → `Err(OperatorFactoryError::EmptySources)` | — |
| 2 | `operators::last_with_default(&core, &binding, source, default=NO_HANDLE)` | `default == NO_HANDLE` → `Err(OperatorFactoryError::ZeroDefault)` | — |
| 3 | `operators::merge(&core, &binding, sources=[bogus])` | `core.register_operator` → `Err(RegisterError::UnknownDep)` → `Err(OperatorFactoryError::Register(re))` via `From` | — |

`crates/graphrefly-operators/src/error.rs` is new. 9 regression tests in `tests/slice_h_factory_errors.rs`. **Diagrams:** [F12.3](#fc-12.3).

---

## 3. Simplification delta

| # | TS pattern | Rust replacement | Simpler? | Notes |
|---|-----------|------------------|----------|-------|
| 1 | TS pause overflow: silent drop or implementation-defined | Rust: synthesized Error with structured diagnostic ([F11.1](#fc-11.1)) | Same | Slice F A3 — closes documented divergence |
| 2 | TS PausableMode (consolidates by default per canonical §2.6) | Rust `PausableMode { Default, ResumeAll, Off }` enum ([F11.2](#fc-11.2)) | Same | Rust port-led; TS port-back lives in Phase 13.6.B |
| 3 | TS `currentWaveEmits` Set captured in `_pump` closure | Rust `tier3_emitted_this_wave` AHashSet on CoreState (Slice G) | Same | Lifted to struct field for lock-acquired access |
| 4 | TS `_emit` per-emit equals via closure | Rust per-emit + retroactive rewrite (Slice G) ([F11.3](#fc-11.3)) | Rust harder | Justified: R1.3.2.d invariant + multiple emit paths (commit_emission, commit_emission_verbatim, settle_dirty_resolved) |
| 5 | TS `replayBuffer` Observable opt | Rust `NodeOpts::replay_buffer: Option<usize>` + per-node `VecDeque<HandleId>` ([F11.4](#fc-11.4)) | Same | Slice E1 substrate |
| 6 | TS `up()` decomposed (separate methods per tier) | Rust `Core::up(node, message) → Result<(), UpError>` ([F11.5](#fc-11.5)) | Same/cleaner | R1.4.1 single entry point; matches canonical |
| 7 | TS tier dispatch via `Message` discriminated union | Rust `match m.tier()` + `payload_handle()` (~26 sites refactored) | Same | "Use tier for signal routing" feedback memory closed |
| 8 | TS `assert(deps[i] !== undefined)` | Rust `RegisterError::{UnknownDep, OperatorWithoutDeps, ...}` ([F12.1](#fc-12.1)) | Same | Slice H D047 |
| 9 | TS `throw new Error(...)` | Rust typed `Result<NodeId, RegisterError>` | Same | Slice H |
| 10 | TS factory `throw` | Rust `OperatorFactoryError` enum + `From<RegisterError>` ([F12.3](#fc-12.3)) | Same | Slice H /qa F7 |
| 11 | TS no-TOCTOU (single-threaded JS) | Rust `ScratchReleaseGuard` RAII + single state-lock acquisition ([F12.2](#fc-12.2)) | Rust harder | Justified: multi-thread; the entire diagram has no TS analog |
| 12 | TS `tier3_emitted_this_wave` Set captured in closure | Rust `commit_emission_verbatim` populates `tier3_emitted_this_wave` (Slice F /qa A2) | Same | Closes Batch-then-standard R1.3.3.a violation door |

**Net assessment:** rows 4 and 11 add Rust complexity. Row 4 is justified by multi-path equals coverage; Row 11 is justified by multi-thread state lock. All other rows are equivalent or simpler.

---

## 4. Deferred gaps audit

**`#[ignore]` stubs:** zero across `slice_f_corrections.rs` (29 tests + 8 added in audit follow-on), `slice_h.rs` (12 tests post-/qa), `slice_h_factory_errors.rs` (9 tests).

**Open items linked to Slice F / G / E1 / H:**
- `pending_pause_overflow` cleared on panic-unwind silently drops queued ERROR (Slice F audit /qa D4) — open, acknowledged divergence.
- `Core::up(INVALIDATE)` cascades via dep instead of plain-forward per R1.4.2 (Slice F audit /qa D2) — open, acknowledged divergence.
- `Core::up` nested-call composition note (Slice F audit /qa D1) — documented as safe (false-positive defense).
- D3 `register/set_pausable_mode` typed-error refactor (RESOLVED via Slice H).
- D5 parity tests for new APIs deferred to napi-rs activation slice — open.

**Slice H /qa surfaced (open):**
- F3 `reset_for_fresh_lifecycle` calls `make_op_scratch` lock-held — open. Lock-discipline asymmetry; `make_op_scratch` is lock-released in `Core::register` but lock-held here. Tracked.
- F11 Phase 3 closure walks deps twice in `Core::register` — perf; open.
- F17 Asymmetric `UnknownNode` typed-error surface — open. `Graph::derived` gets `Result<NodeId, NameError>` for namespace conflicts; the underlying Core call gets `RegisterError` — but the wrapper layer doesn't surface `RegisterError::UnknownDep` distinctly. Tracked.

**Slice H /qa landed (closed):**
- F1+F2 TOCTOU race + panic-unsafe scratch leak. ✅
- F4 `Last{default}` early-return refcount test. ✅
- F5 `register_err_does_not_add_node_to_graph` tightened. ✅
- F6 error-precedence test. ✅
- F7 operator factory `assert!`→typed `OperatorFactoryError`. ✅
- F8 Last-shaped `.expect` tightened. ✅
- F9 `Eq` derive on `RegisterError`/`SetPausableModeError`. ✅
- F10 stray `FnResult` import. ✅
- F12 `# Errors` rustdoc cites phase numbers. ✅
- F13 `make_op_scratch` Box-before-retain ordering. ✅
- F14 `reset_for_fresh_lifecycle` expect message. ✅
- F15 `RegisterError` rustdoc Last default retain. ✅
- F16 `set_pausable_mode_on_unknown_node_errors` no-mutation pinned. ✅

**Potential correctness holes:** **none.** This is the cleanest /qa cycle in the port — every concern surfaced has either a fix or a deferred-with-reason entry.

---

## 5. Parity test coverage

Existing scenarios specifically for Slice F + audit follow-on + Slice G + Slice E1: none directly (Rust-port-led changes; TS port-back is in Phase 13.6.B scope).

**Suggested parity scenarios to add when napi binding ships:**
1. **Pause-overflow ERROR synthesis** (R1.3.8.c / Lock 6.A) — set cap, exceed, assert structured Error with `{droppedCount, configuredMax}`.
2. **PausableMode Default consolidation** (canonical §2.6) — assert single fn-fire on resume vs ResumeAll mode's N fires.
3. **R1.3.2.d per-wave equals coalescing** — emit twice in batch, assert `[Data(snap), Data(h2)]` not `[Resolved, Data(h2)]`.
4. **Replay buffer handshake** (R2.6.5) — set cap, emit N+1, late subscribe, assert handshake includes replay slices.
5. **Core::up + UpError** (R1.4.1) — assert UnknownNode, UnsupportedTier per tier.
6. **typed RegisterError** (Slice H) — assert UnknownDep / OperatorWithoutDeps / SeedSentinel / TerminalDep paths.
7. **typed SetPausableModeError** — assert UnknownNode + WhilePaused.
8. **OperatorFactoryError** (Slice H /qa F7) — assert EmptySources / ZeroDefault / Register(re).

---

## 6. Recommended actions

1. **Close F3** (lock-discipline asymmetry in `reset_for_fresh_lifecycle`) — small fix; aligns lock discipline.
2. **Close F11** (Phase 3 dup-iter in `Core::register`) — perf cleanup.
3. **Close F17** (asymmetric UnknownNode typed-error surface) — design pass needed; surface RegisterError variants through Graph wrapper.
4. **Schedule TS port-back** of `PausableMode` consolidation per Phase 13.6.B migration scope.
5. **Schedule Slice E2** (fn ctx + cleanup hooks per R2.4.5/R2.4.6) — D054/D055 design locked 2026-05-07; lift waiting on (a) `ctx` extends/replaces `FnCtx` decision and (b) binding-side storage shape.
6. **Add the 8 parity scenarios above** when napi operator binding lands.

---

## 7. Overall assessment

- **Spec-fidelity: very high.** The Slice F audit pass closed >10 documented divergences in a single slice; Slice G closed the bug it surfaced; Slice E1 added missing R2.6.5 substrate; Slice H promoted ~150 sites from panic to typed Result without breaking any existing test.
- **Over-engineering risk: low.** 2 Rust-adds-complexity rows in delta (Slice G retroactive rewrite, ScratchReleaseGuard RAII); both forced by Rust-specific constraints.
- **Slice H /qa is exemplary.** 17 distinct concerns surfaced; 13 closed in /qa, 3 deferred with explicit reason, 1 (F2) was the headline correctness win (TOCTOU).
- **No correctness holes flagged.** First slice cluster with zero open correctness gaps.
- **No HALT.**

**Deferred items: 17+ closed, 5 open** (all explicit, all tracked).
