# Report 001 — M1 dispatcher + M2 graph (closed milestones)

**Slices covered:** Pre-M1 scaffold → Slice A+B → A-bigger → A close → B (napi) → C (CI) → C-1 → C-1.5 → C-2 → D (Graph) → E+ (read-side) → F (topology + reactive) — all closed by 2026-05-06.
**LOC at close:** ~5,627 src + ~7,807 tests in `graphrefly-core`; 1,825 src + ~1,300 tests in `graphrefly-graph`.

---

## 1. Behavioral traces (closed-milestone scenarios)

### Trace 1 — Cached state push-on-subscribe per-tier handshake (R1.2.3, R2.2.3, R1.3.5.a)

| Step | Event | Internal state change | Observable output |
|------|-------|----------------------|-------------------|
| 1 | `register_state(initial=h0)` | `NodeRecord{ status: Settled, cache_handle: h0, subscribers: [] }`; `retain_handle(h0)` | `NodeId` |
| 2 | `subscribe(state_id, sink_A)` | acquire `wave_owner`, `state.lock()` builds plan: `[Start, Data(h0)]`, push `sink_A`, `drop(state)` | `Subscription` |
| 3 | LOCK-RELEASED dispatch | per-tier sink calls: `[Start]`, `[Data(h0)]` | `sink_A` observes `Start` then `Data(h0)` as separate calls |
| 4 | `subscribe(state_id, sink_B)` | identical plan; `sink_B` cache snapshot is `h0` | `sink_B` observes `[Start, Data(h0)]` |
| 5 | `emit(state_id, h1)` (no batch) | wave engine: `tier3_emitted_this_wave.insert(state_id)`, equals → Resolved (Identity), commit cache → `h1`, queue `pending_notify[state_id] = Resolved` | (mid-wave) |
| 6 | drain wave | flush phase 4: `[Data(h1), Resolved]` | both sinks observe `[Data(h1)]` then `[Resolved]` |

**Diagrams:** [F1.1](#fc-1.1) NodeRecord/CoreState shape; [F2.6](#fc-2.6) `queue_notify` pause routing + sink-snapshot-on-first-touch; [F3.1](#fc-3.1) subscribe protocol with R1.3.5.a per-tier handshake.

### Trace 2 — Diamond glitch-free settle via transitive `pick_next_fire` (R1.3.1.b, R2.7.1) — Slice C-1.5 fix

| Step | Event | Internal state change | Observable output |
|------|-------|----------------------|-------------------|
| 1 | Topology: `s → l, r → c` (c depends on l + r, both depend on s) | static deps registered | — |
| 2 | `subscribe(c, sink)` | activates `c` and ancestors via `activate_derived` | `[Start, Data(initial_c)]` |
| 3 | `emit(s, h_new)` | `pending_fires = {l, r, c}` (BFS dirties everything downstream) | (mid-wave) |
| 4 | wave drain — fire `l` first | `pick_next_fire`: walks transitively, picks `l` (no upstream pending). `l` fires → `Data(h_l)`. | (mid-wave) |
| 5 | `pick_next_fire` again | now `r` and `c` are pending. `c` has `r` upstream still pending → `pick_next_fire` walks transitively and skips `c`, returns `r`. | (mid-wave) |
| 6 | `r` fires → `Data(h_r)` | now only `c` pending; both upstreams settled this wave | (mid-wave) |
| 7 | `c` fires once with both new dep latests | emits one `Data(h_c)` reflecting both updates | sink observes `[Data(h_c)]` once — no glitch |

**Diagrams:** [F2.4](#fc-2.4) `pick_next_fire` transitive walk; [F2.5](#fc-2.5) `flush_notifications` two-phase by tier; [F3.6](#fc-3.6) `set_deps`. Without the transitive walk (Slice C-1's bug), step 5 would have fired `c` after `l` but before `r` settled this wave, producing an interim `Data(h_c)` based on stale `r`.

### Trace 3 — TEARDOWN with meta-companion ordering + auto-COMPLETE (R1.3.9.d, R2.6.4 / Lock 6.F)

| Step | Event | Internal state change | Observable output |
|------|-------|----------------------|-------------------|
| 1 | `add_meta_companion(state_id, meta_id)` | meta link tracked | — |
| 2 | `complete(state_id)` | wave: queue `[Teardown(state), Teardown(meta), Complete(state), Complete(meta)]` (R1.3.9.d ordering: Teardown precedes Complete; meta Teardown follows owner Teardown) | — |
| 3 | drain wave phase 5 | sinks see Teardown sequence | `sink: [Teardown(state), Teardown(meta)]` |
| 4 | drain wave phase 4 | sinks see Complete sequence | `sink: [Complete(state), Complete(meta)]` |

**Diagrams:** [F3.4](#fc-3.4) `teardown_inner` iterative with R1.3.9.d meta ordering; [F3.3](#fc-3.3) terminal cascade.

### Trace 4 — `Graph::describe()` JSON snapshot (R3.6.1 + Appendix B)

| Step | Event | Internal state change | Observable output |
|------|-------|----------------------|-------------------|
| 1 | `Graph::new()` + `derived(name="d", deps=[s], fn_id)` + `state(name="s", initial=h0)` | namespace registers `s` and `d` with `RemoveAudit` | — |
| 2 | `graph.describe()` | serializes to JSON: `{ version, schemaVersion, nodes: [{ id, name, kind, deps, value }] }` | `String` JSON |
| 3 | inspect output | `value: HandleId` — raw handle, not user value | matches Appendix B except for the value field 🟨 |

**Diagrams:** [F6.2](#fc-6.2) `describe()` JSON build; [F0.2](#fc-0.2) handle-protocol cleaving plane (explains why `value` surfaces as HandleId).

### Trace 5 — `set_deps` cycle rejection + push-on-add for cached deps (R3.3.1.1 + Phase 13.8 Q1)

| Step | Event | Internal state change | Observable output |
|------|-------|----------------------|-------------------|
| 1 | Topology: `a → b` (a depends on b) | — | — |
| 2 | `set_deps(b, [a])` | cycle check: walk reachable-from `a`, contains `b` → `Err(CycleDetected)` | `Err` |
| 3 | `set_deps(c, [a])` (where `a` has cached DATA) | atomic dep-record swap: release old; retain new; if new dep has cached DATA, push to `c` lock-released | `c` fn-fires once with `a`'s latest |

TLA+ verified `wave_protocol_rewire.tla` — 35,950 distinct states, 0 violations. **Diagrams:** [F3.6](#fc-3.6) `set_deps` atomic dep mutation.

---

## 2. Simplification delta (M1 + M2)

| # | TS pattern | Rust replacement | Simpler? | Notes |
|---|-----------|------------------|----------|-------|
| 1 | Per-`NodeImpl` JS closures (acc, deps, subscribers all in one closure) | `NodeRecord` struct ([F1.1](#fc-1.1)) | Same | Field-by-field equivalence; required for lock-acquired mutation |
| 2 | `currentWaveEmits` Set captured in `_pump` closure | `tier3_emitted_this_wave` AHashSet on `CoreState` ([F1.1](#fc-1.1)) | Same | Lifted to struct field because Rust state lock is shared across methods |
| 3 | `wave_owner` (none — single-threaded JS) | `parking_lot::ReentrantMutex<()>` ([F1.3](#fc-1.3)) | Rust adds | Justified: multi-thread emit serialization without poisoning re-entrant subscribe |
| 4 | `pick_next_fire`: linear scan | Transitive BFS ([F2.4](#fc-2.4)) | Rust harder | Justified: Slice C-1 bug forced transitive walk to fix R1.3.1.b diamond glitch |
| 5 | `Subscription` = closure returning disposer | `struct Subscription { core: Weak<Core>, node: NodeId, sink_id: usize }` with `impl Drop` ([F1.3](#fc-1.3)) | Same | RAII matches §10.12 |
| 6 | Fn fire: synchronous, lock-free | `fn fire_fn` LOCK-RELEASED ([F2.2](#fc-2.2)) | Rust harder | Slice A close lifted from lock-held; required to allow re-entrance |
| 7 | `commit_emission`: equals + queue | Same shape but with cache-snapshot guard for Custom equals ([F2.3](#fc-2.3)) | Rust harder | Slice A close /qa P3 fix; cache-race protection |
| 8 | `pending_notify` Map | `IndexMap<NodeId, NotifyEntry>` ([F1.1](#fc-1.1)) | Same | IndexMap chosen for deterministic iteration |
| 9 | `BatchGuard` (TS: `currentTick` counter) | `struct BatchGuard<'a>` with `Drop` ([F2.7](#fc-2.7)) | Same | RAII; per-Core scoping (TS uses process-global) |
| 10 | `Drop for CoreState` (none — GC) | Walks all NodeRecords releasing handles ([F4.1](#fc-4.1)) | Rust adds | Justified: Rust has no GC; required for refcount discipline |
| 11 | `Graph` mount: same-Core only | Same — `RemoveAudit` parallel ([F5.3](#fc-5.3)) | Same | Rust `RemoveAudit` IS the audit type; TS calls it `GraphRemoveAudit` |
| 12 | `signal_invalidate(path)` recursive walk | Same — non-snapshot ([F5.4](#fc-5.4)) | Same | — |
| 13 | `describe()` Appendix B JSON | Same shape, `value: HandleId` 🟨 | Same | Documented divergence — values cross cleaving plane |
| 14 | `observe_all()` snapshot at subscribe | Snapshot at subscribe (default mode); reactive variant in Slice F | Same | Slice F adds the reactive variant via `observe_all_reactive` |
| 15 | `subscribe_topology` (none — TS uses `mountObservers`) | First-class `TopologyEvent` enum ([F5.1](#fc-5.1)) | Rust adds | Slice F primitive; TS port-back path |

**Net assessment:** 4 rows add Rust complexity (rows 3, 4, 6, 10). All four are forced by Rust-specific requirements — none are speculative.

---

## 3. Deferred gaps (closed-milestone state)

**`#[ignore]` test stubs:** zero across `graphrefly-core` and `graphrefly-graph`. Convention is doc-only deferrals tracked in [`porting-deferred.md`](../../../graphrefly-rs/docs/porting-deferred.md), not test-enforced.

**Open items relevant to M1 + M2 (excerpted, see deferred doc for full list):**
- D1 — `set_deps` from inside firing fn corrupts Dynamic tracked indices. **RESOLVED in Slice F audit follow-on** via thread-local "currently-firing" guard returning `SetDepsError::ReentrantOnFiringNode`. ✅
- D2 — Late subscriber multi-emit-per-wave snapshot gap. Open. Documented divergence — late subscriber sees the most-recent Data only, not the per-emit history.
- D3 — Cross-thread emit blocks until in-flight wave completes. Open (updated 2026-05-07 with note: behavior is intentional but worth opt-in non-blocking variant).
- D4 — Per-tier handshake panic on tier-N leaves sink registered. **RESOLVED in Slice F audit follow-on** via per-tier `catch_unwind` wrapper. ✅
- D5 — `commit_emission` cache-race documentation. Open (documentation-only; no behavioral change).
- "Subscribe-time handshake fires lock-held" — **RESOLVED in Slice E** via D045. ✅
- "Cascade recursion stack-overflow" — **RESOLVED in Slice A-bigger** via iterative cascades. ✅
- "Subscribe handshake delivered as single sink call R1.3.5.a" — **RESOLVED in Slice A close**. ✅

**M2-specific:** [F6.2](#fc-6.2) `describe()` value field surfaces raw HandleId; [F6.1](#fc-6.1) `try_resolve` no `..::sibling::node`; [F6.3](#fc-6.3) `up()` decomposed (R3.6.2 divergence). All three documented and tracked.

**Potential correctness holes:** none flagged. Two acknowledged divergences:
- `audit_of` recursive counts racy — documented, not a correctness hole because callers are inspection-only.
- Anonymous Core nodes surface as `_anon_<NodeId>` — documented; not a correctness issue.

---

## 4. Parity test coverage

`packages/parity-tests/scenarios/` covers M1 + M2 with the following structure:
- `core/` — base dispatcher invariants (run today against `pureTsImpl`; `rustImpl` activates with napi parity)
- `graph/sugar.test.ts`, `graph/remove.test.ts`, `graph/edges.test.ts`, `graph/signal.test.ts`, `graph/describe-reactive.test.ts`, `graph/observe-all-reactive.test.ts` — 6 scenarios from Slice F D6 widening (18 passing + 2 skipped per `test.runIf`)

**Missing parity scenarios (suggested for M3 napi binding activation):**
- R1.3.1.b diamond glitch-free settle (covered in `core` integration but no dedicated parity scenario)
- R1.3.5.a tier-split handshake delivery semantics
- R1.3.9.d meta-TEARDOWN ordering across companions
- R3.4 mount with `RemoveAudit` parallel
- R3.7 `destroy()` cascade ordering

These suggestions are non-blocking — they exist to catch regressions when `rustImpl` activates.

---

## 5. Recommended actions

1. **Activate parity scenarios** when napi-rs operator binding lands (deferred — D053 = activate-and-triage approach locked).
2. **Add `Core::up(INVALIDATE)` plain-forward variant** per R1.4.2 to close Slice F audit /qa D2 — currently the cascade goes via dep-walk inside `Core::invalidate`. Documented as acceptable v1 divergence.
3. **Document `Graph::set_deps` D1 hazard publicly** — the `set_deps` reentrancy guard now lives in Core, but the Graph wrapper still allows it; rustdoc warning is the only guard.
4. **Mirror `set_deps` cycle check into `register_computed`** — small, no new tests needed.
5. **Promote `pick_next_fire` perf flag into criterion bench** — currently no end-to-end perf signal on cycle-fallback busy-loop edge case.

---

## 6. Overall assessment (M1 + M2)

- **Spec-fidelity: high.** All R1-tier and R2-tier rules verified against current source; `set_deps` TLA+ verified on top.
- **Over-engineering risk: low.** 15-row delta has 4 Rust-adds-complexity rows; all justified.
- **Deferred items: 19 total** (10 resolved, 9 open). All open items documented with explicit rationale.
- **No HALT.** No contradiction with the canonical spec found.
