# Decision-Consistency Audit — 2026-05-21

> **Historical port-era audit.** This file records a point-in-time audit of the
> retired port model. It is not current package API guidance and is not a shared
> docs authority. Current Rust package docs are governed by
> [`docs.jsonl`](docs.jsonl); language-neutral authority lives in
> `~/src/graphrefly`.

## Methodology

Applied 8 audit lenses (L1 vestigial-surface from earlier relaxations; L2 invariant-watch violations; L3 D196 misapplications; L4 stale rationale references; L5 cross-D contradictions; L6 scope-gap findings; L7 TRASH/ hygiene; L8 doc-vs-code drift) against ~200 D-numbered decisions D047–D271. Sources read: `~/src/graphrefly-ts/docs/rust-port-decisions.md` (2118 LOC, all D# headers), `~/src/graphrefly-rs/docs/porting-deferred.md` (4354 LOC, all section headers + §7 + D266-D270 follow-on + S7 closing), `~/src/graphrefly-rs/docs/migration-status.md` (lines 1–120 NEXT-BATCH + D271 + D266-D270 closure blocks), `~/src/graphrefly-ts/docs/cross-track-ledger.md` (entire), `~/src/graphrefly-ts/archive/optimizations/cross-language-notes.jsonl` (14 `divergence-*` ids enumerated), `~/src/graphrefly-ts/.claude/skills/decision-guard/SKILL.md` (entire). Live-code corroboration via grep across `~/src/graphrefly-rs/crates/**` for invariant-watch items #3–#10/#13, vestigial Send+Sync, deleted-machinery comment leakage, and §7-B/C/D/E/F status. `cargo metadata --no-deps` confirmed TRASH-dir exclusion. Scope: all locked D-numbers + the active porting-deferred + cross-track-ledger rows. Outside scope: implementation of fixes (audit is read-only).

## Summary

| Lens | Findings | High | Recommended triage mix |
|---|---|---|---|
| L1 | 2 | 1 | 1 CLEANUP, 1 CLOSE |
| L2 | 0 | 0 | (#1, #2, #11, #12, #13, #14 already clean — only comment hits remain; see closed-as-clean) |
| L3 | 1 | 0 | 1 AMEND-D |
| L4 | 4 | 2 | 4 CLEANUP |
| L5 | 1 | 0 | 1 AMEND-D |
| L6 | 1 | 0 | 1 DEFER (already-deferred; framing-amend) |
| L7 | 0 | 0 | (TRASH/ confirmed not compiled by cargo; clean) |
| L8 | 3 | 2 | 3 CLEANUP |
| **Total** | **12** | **5** | **6 CLEANUP, 2 AMEND-D, 1 DEFER, 1 CLOSE, 2 inline above** |

## Findings (sorted by severity, then by lens)

### HIGH [L4-001] `pub struct Core` rustdoc still describes pre-D221/D248 shared shape

- **Lens:** L4 (stale rationale)
- **Description:** The doc-comment immediately above `pub struct Core` says "Holds an `Arc` to the `BindingBoundary` and all dispatch state. Cheap to clone (the inner `Arc<Mutex<CoreState>>` is shared); pass `Core` by value to threads." This is the pre-D221 shape. Post-D221/D246/D248 `Core` is move-only, NOT `Clone`, NOT `Send + Sync`, and contains `RefCell` regions — the inner-doc 15 lines below correctly explains all this. The top-of-struct doc is directly contradictory and is the doc a downstream reader sees first.
- **Evidence:** `~/src/graphrefly-rs/crates/graphrefly-core/src/node.rs:1771-1776` ("Cheap to clone (the inner `Arc<Mutex<CoreState>>` is shared); pass `Core` by value to threads") vs `:1779-1817` (RefCell, "single-owner", "!Send + !Sync", "cross-`Core` parallelism = independent per-worker Cores"). Confirmed by grep — only one site uses the "Cheap to clone" phrasing; the rest of the file aligns with the post-D248 reality.
- **Triage:** CLEANUP
- **Rationale:** Pure-doc fix; the misleading sentence will cause readers to assume shared-Core ergonomics that D221/D246/D248 deleted. No decision change needed.

### HIGH [L4-002] `crates/graphrefly-graph/src/describe.rs` references deleted `GraphOps` trait

- **Lens:** L4 (stale rationale, doc-vs-code)
- **Description:** Two rustdoc cross-links — `[crate::GraphOps::describe]` and `[crate::GraphOps::describe_with_debug]` — point at a trait that was deleted by D247 ("`SubgraphRef/GraphOps/NamespaceHandle/'g/SnapshotOps DELETED`" per `project_next_porting_batch` memory + `graph.rs:7-8` confirms "no `SubgraphRef`/`GraphOps`/`NamespaceHandle`, no `'g` lifetime"). The doclinks point into the void.
- **Evidence:** `~/src/graphrefly-rs/crates/graphrefly-graph/src/describe.rs:80` ("Raw handle view (default for [`crate::GraphOps::describe`])") + `:82` ("Binding-rendered view (from [`crate::GraphOps::describe_with_debug`])"). `grep -rn "trait GraphOps\|pub trait GraphOps\|impl GraphOps\|use.*GraphOps" crates/graphrefly-graph/src` → 0 hits.
- **Triage:** CLEANUP
- **Rationale:** Broken doclinks; same pure-doc class as L4-001. Rustdoc-build emits warnings.

### HIGH [L8-001] §7-E porting-deferred entry references `GroupLockRegistry` that no longer exists

- **Lens:** L8 (doc-vs-code drift)
- **Description:** `~/src/graphrefly-rs/docs/porting-deferred.md:3176-3181` lists §7-E "`GroupLockRegistry` never prunes" as an open deferral with a "Lift point: When a consumer creates unboundedly-many distinct groups." But `GroupLockRegistry`, `groups.rs`, the `SerializationGroupId` group-locking layer, and the `*_or_defer` `where C: Send + Sync` cliff are all GONE post-D253 (S5, 2026-05-19) and D255 (S6 actor model). The §7-E "leak" is structurally impossible.
- **Evidence:** `grep -rn "GroupLockRegistry\|groups::" crates 2>&1 | head -10` → 0 hits in src; `crates/graphrefly-core/src/` has no `groups.rs` file (`ls`). Per D253 lock the entire `SchedulingGroupId` surface was deleted (confirmed by grep — all live references are in comments/test doc-headers/TRASH).
- **Triage:** CLOSE
- **Rationale:** Stale deferral; the lift-point condition cannot fire. Same applies to §7-F (`*_or_defer where C: Send + Sync` — the `C` generic was deleted with `StateCell`). Both should be moved to a "resolved by D253/D255" closing block; §7-B's "ABBA cycle on `ReentrantMutex` cross-component" also is structurally impossible under the actor model (one Core per OS thread, D252).

### HIGH [L5-001] D258 inline panic-message comment contradicts D262/P4 downgrade

- **Lens:** L5 (cross-D contradiction)
- **Description:** D258 (S6 fold-in) softened the `claim_in_tick` panic message as "useful for both in-tree Rust devs and JS `@graphrefly/native` consumers." D262 /qa P4 (2026-05-20) explicitly **downgraded** that framing: `core_actor.rs` M1 `catch_unwind(...)` swallows the panic value before it crosses napi → JS callers observe a sync_channel disconnect, NOT the message text. D258's value-add is "Rust-dev clarity only; binding-friendly framing is aspirational pending a panic→JS bridge slice (not committed)." The inline source comment at the panic site STILL carries the original D258 framing, contradicting the locked D262/P4 correction.
- **Evidence:** `~/src/graphrefly-rs/crates/graphrefly-core/src/batch.rs:3498-3506` ("message softened to be useful for both in-tree Rust devs and JS `@graphrefly/native` consumers...") vs `~/src/graphrefly-rs/docs/porting-deferred.md:184` D262 P4 (the polished message "reaches Rust's panic hook → stderr, but `core_actor.rs` M1 `catch_unwind` swallows the panic value before it crosses napi → JS users see a sync_channel disconnect, not the message"). Cross-confirmed by `rust-port-decisions.md` D262 P4 entry.
- **Triage:** AMEND-D
- **Rationale:** The inline comment is the canonical source-truth for future readers; it must reflect the D262 lock. Either amend the inline comment to match D262, or amend D258's "Affects" to record the inline-comment fix as part of the P4 patch (currently P4's affects-list only mentions `migration-status.md S7 LANDED block` + `porting-deferred.md`, omitting the source comment).

### HIGH [L8-002] D250-AMENDED `#[ignore]` stub deletion contradicts current doc references

- **Lens:** L8 (doc-vs-code drift)
- **Description:** D250-AMENDED (2026-05-20, S6 close) DELETED the 3 retired stubs (`fn_can_reenter_core_pause_resume_during_invoke_fn`, `sink_can_reenter_core_via_pause_and_resume`, `a6_set_deps_from_firing_fn_rejected_with_reentrant_error`) per `feedback_no_backward_compat`. But `~/src/graphrefly-rs/docs/porting-deferred.md` still contains stale audit-trail prose referencing them — line 3802 has a strikethrough-marked entry "— D250 retired stubs.~~" and line 4007 carries "[D250 — S4 re-entry-stub disposition LOCKED → S6 DELETED]" but reads as if the doc reader may not realize the post-amendment delete happened (the doc retains the original D250 framing for ~25 lines before the amendment is noted).
- **Evidence:** `~/src/graphrefly-rs/docs/porting-deferred.md:3801-3803` (strikethrough) + `:4007-4024` (still describes the disposition history). `~/src/graphrefly-ts/docs/rust-port-decisions.md` D250 entry at line 1908 explicitly says "AMENDED 2026-05-20 (S6 close): the 3 `#[ignore]` stubs were DELETED outright". Grep `~/src/graphrefly-rs/crates/graphrefly-core/tests/{lock_released,lock_discipline,slice_f_corrections}.rs` for the 3 fn names → 0 hits (confirms delete landed).
- **Triage:** CLEANUP
- **Rationale:** Pure-doc fix; collapse the multi-paragraph stub-disposition history into a one-line "RESOLVED 2026-05-20 by D250-AMENDED: stubs deleted; structural cover via live mailbox-reentry tests" reference.

### MEDIUM [L1-001] `MailboxOp::Defer` rustdoc still references `SchedulingGroupId` (deleted D253)

- **Lens:** L1 (vestigial-surface from earlier relaxation)
- **Description:** `~/src/graphrefly-rs/crates/graphrefly-core/src/mailbox.rs:140-157` rustdoc on the `runnable` `AtomicBool` describes M6 granularity in terms of `SchedulingGroupId` ("the worker's Core hosts its declared `SchedulingGroupId`(s); a finer per-`SchedulingGroupId` sub-bit has no consumer..."). D253 (2026-05-19) DELETED the `SchedulingGroupId` API surface entirely. The QA F12 note at `:153-157` doubles down: "if a future per-`SchedulingGroupId` sub-bit is ever added, it MUST be split in lockstep across BOTH this `CoreMailbox.runnable` AND `DeferQueue.runnable`". When M6 happens it will not use `SchedulingGroupId` (that's literally what D253's rationale said).
- **Evidence:** `crates/graphrefly-core/src/mailbox.rs:143,153` (live `SchedulingGroupId` references in rustdoc on a public field's documentation). `grep -rn "SchedulingGroupId" crates/*/src` shows ALL remaining live references are now in comments only — `node.rs:2489-2491,4505,4959-4970` + `mailbox.rs:143,153` — but mailbox.rs's are user-facing rustdoc on a `pub struct` field doc, while node.rs's are crate-internal comments. The mailbox doc is the externally-visible one.
- **Triage:** CLEANUP
- **Rationale:** Rustdoc on a `pub` field; affects external readers. Rewrite to reference "per-Core wake bit; M6 scheduling-grain TBD per the M6 design pass."

### MEDIUM [L8-003] Stale `StateCell` / `Core<C: StateCell>` references in `CoreShared` rustdoc

- **Lens:** L8 (doc-vs-code drift)
- **Description:** `crates/graphrefly-core/src/node.rs:1550-1558` describes `CoreShared` in terms of the deleted `StateCell` trait ("appears in the public `StateCell` trait surface (`from_parts` / `lock_shared`)") and `Step 2 reshapes `crate::state_cell` to hold one `CoreShared` + a per-`ShardKey` shard map`. Per the user memory's `project_next_porting_batch`, S2c-Step-2c (the `Mutex<GraphInner>→RefCell/&mut; delete groups.rs/LockedCell; collapse StateCell C generic — ~128+45 refs`) is "NONE STARTED" — but `grep -rn "trait StateCell\|struct LockedCell\|struct SingleThreadCell\|Core<\|impl<C" crates/graphrefly-core/src` returns ZERO hits, indicating the generic IS already collapsed (or never lived where the doc claims).
- **Evidence:** `node.rs:1555 ("appears in the public `StateCell` trait surface")`, `:1705-1706 ("the public `StateCell` trait surface (`Core<C: StateCell>` is public and `SingleThreadCell`/`LockedCell` are")`, `:1780,2134,2201` (more `StateCell` rustdoc references). `grep -rn "trait StateCell\|struct LockedCell\|struct SingleThreadCell" crates 2>&1` → only the 6 comment hits, zero definitions. `grep "pub struct Core" node.rs` → `pub struct Core {` (no generic).
- **Triage:** CLEANUP (or VERIFY first)
- **Rationale:** The doc claims `StateCell` is a "public trait surface" but no definition exists. Either (a) the doc is stale (the trait was collapsed pre-this-audit; rewrite the rustdoc to drop `StateCell` mentions) or (b) the trait exists in a not-yet-grepped location. If (a), CLEANUP; if (b), VERIFY. The user-memory's "NONE STARTED" for Step-2c suggests the cleanup is genuinely pending and the doc is correctly anticipating a future delete — but if so, the COMMENTS describing `StateCell` as currently-public are misleading when there is no `StateCell` to make public.

### MEDIUM [L1-002] §7-C vestigial union-find surface still live (D211-deferred, now D196-misframed)

- **Lens:** L1 (vestigial-surface from earlier relaxation)
- **Description:** §7-C lists `PartitionOrderViolation`, `SubscribeError::PartitionOrderViolation`, `SetDepsError::PartitionMigrationDuringFire`, `DeferredProducerOp`, `push_deferred_producer_op`, `drain_deferred_producer_ops`, the `*_or_defer` aliases. Verified all still live in source post-D246/D247/D248/D255 (Slice B "absorbed" never actually deleted them — the actor-model rewrite happened in a different shape than D216's "shard parallelism redesign"). These are now genuinely dead code from a deleted machinery, kept only by the original D211 "no churn outside Core" compile-time budget.
- **Evidence:** `grep -rn "PartitionOrderViolation\|PartitionMigrationDuringFire\|DeferredProducerOp\|push_deferred_producer_op\|drain_deferred_producer_ops" crates/*/src` → 25+ live hits in `crates/graphrefly-core/src/{node.rs,batch.rs}` + downstream `Err(_)` arms. `node.rs:1058-1060` confirms `#[error("vestigial PartitionOrderViolation (never constructed post-§7)")]` — the type knows it's vestigial.
- **Triage:** CLEANUP
- **Rationale:** The lift-condition stated in §7-C ("Slice B parallelism redesign rewrites the *exact* group/lock layer") materialized in shape D255 (actor model) which structurally eliminated cross-thread group locking — the union-find symbols' rationale is gone. A focused 1-slice CLEANUP removing all 6 symbols + their dead `Err(_)` arms is the right disposition. Importantly: this is **decision-consistency restoration** (vestigial-surface from D248/D253/D255 relaxation chain), NOT speculative substrate, so D196 does not apply here.

### LOW [L3-001] §7-C deferral framing implicitly invokes D196 spirit

- **Lens:** L3 (D196 misapplication)
- **Description:** §7-C's "Lift point: A standalone `graphrefly-operators` cleanup slice (delete the dead defer arms + the vestigial Core symbols together)" is currently deferred under a "no churn" rationale that, after D246/D248/D255, becomes effectively "no speculative cleanup" — which mimics D196 reasoning. But D196 governs ADDING new substrate surface in anticipation of a consumer; REMOVING vestigial code from a deleted machinery is the opposite — it is decision-consistency RESTORATION (clearing surface made dead by an earlier locked relaxation). Conflating the two postpones cleanup that has zero scope-expansion risk.
- **Evidence:** `~/src/graphrefly-rs/docs/porting-deferred.md:3166-3169` (§7-C lift-point) + L1-002 confirms the surface is dead code (`#[error("vestigial PartitionOrderViolation (never constructed post-§7)")]`). The D196 spirit invocation is implicit (no explicit D196 cite in §7-C), but the deferral logic ("not cheap as a standalone slice") IS the D196-pattern argument applied to cleanup that should be gated by `feedback_no_backward_compat`, not D196.
- **Triage:** AMEND-D
- **Rationale:** Re-frame the §7-C entry's "Why deferred" prose to acknowledge: (1) the original D211 compile-budget rationale is satisfied (D246/D255 already touched the surrounding layer); (2) the deferral now sits in "decision-consistency restoration backlog," not the D196 consumer-pressure queue; (3) `feedback_no_backward_compat` says delete dead pre-1.0 code aggressively. No new D-number; this is an AMEND to the porting-deferred entry's framing. (User caller may decide separately to lock a CLEANUP D-number; the audit only flags the framing.)

### LOW [L6-001] D267 "drop run_sync everywhere on bindings" wording broader than scope landed

- **Lens:** L6 (scope-gap)
- **Description:** D267 reads "Drop all sync `run_sync` napi methods on `BenchGraph` (and any other binding); make every Core-touching binding async." The landed change converted BenchGraph read methods to async but `run_sync` still appears in `core_actor.rs` (the implementation of `run_sync` itself, which is fine), `graph_bindings.rs:117` (constructor `from_core` — documented "one-shot factory call... blocking is acceptable"), and 4 sites in `structures_bindings.rs` (factory constructors for `BenchReactiveLog/List/Map/Index::create`, each with verbose "no subscribers exist at construction" safety justifications). Each surviving site is individually defensible, but D267's literal "drop run_sync everywhere... make every Core-touching binding async" is broader than what landed.
- **Evidence:** `~/src/graphrefly-ts/docs/rust-port-decisions.md` D267 ("Drop all sync `run_sync` napi methods on `BenchGraph` (and any other binding); make every Core-touching binding async"). `grep -n "actor.run_sync" crates/graphrefly-bindings-js/src/{structures_bindings,graph_bindings}.rs` → 5 surviving sites (1 in graph_bindings constructor, 4 in structures factory `create`s), each with rustdoc rationale at `structures_bindings.rs:200-210` (BenchReactiveLog), `:543-545` (BenchReactiveList), `:659-661` (BenchReactiveMap), `:790-792` (BenchReactiveIndex). The cross-track-ledger row at line 67 says "Constructor `from_core` + structures `create` factories keep `run_sync` (documented safe — no subscribers exist at construction)" — which IS decision-consistent but is recorded only in the ledger, not in D267's text.
- **Triage:** DEFER (already-deferred via the inline rationales; framing-amend optional)
- **Rationale:** Each surviving site is correctly justified by lifecycle precondition (no subscribers ⇒ no TSFN-callback re-entry hazard ⇒ no D070/D077 deadlock class). The gap is purely textual: D267's wording is more sweeping than the surgical fix that landed. A future binding addition that ALSO needs a sync constructor would have to re-derive the same "no subscribers at construction" rationale — small risk of someone misapplying D267 literally and breaking factory ergonomics, OR violating it by adding a sync factory without the lifecycle audit. AMEND-D D267 to scope precisely: "drop `run_sync` from BenchGraph read methods + Core read paths; factory constructors retain `run_sync` per lifecycle-precondition rationale (no subscribers at construction)."

## Closed-as-clean (negative results worth recording)

- **L2 #1** `pub async fn` in `graphrefly-core`/`-graph`/`-operators`/`-structures` — `grep -rn "pub async fn" crates/graphrefly-{core,graph,operators,structures}/src` returns ZERO hits. Clean.
- **L2 #2** `MailboxOp::AsyncDefer` variant — `MailboxOp` has only `Emit | Complete | Error | Defer(SendDeferFn)` per `crates/graphrefly-core/src/mailbox.rs:93-108`. No `AsyncDefer`. Clean.
- **L2 #11** `CURRENT_CORE` / `CoreThreadGuard` / `current_core()` — `grep` returns only historical "deleted" comments in `core_actor.rs`/`core_bindings.rs`/`owned.rs`/`operators/tests/common/mod.rs`. No live definitions. Clean.
- **L2 #12** `core.clone()` / `impl Clone for Core` — `grep -rn "core\.clone()\|Core::clone" crates/*/src crates/*/tests crates/*/benches | grep -v TRASH` → 0 hits outside TRASH (TRASH-confirmed not compiled, see L7). Clean.
- **L2 #13** `Core` by value in shared container — only `core: Core` in `crates/graphrefly-core/src/owned.rs:45` (OwnedCore is owner-thread-only by construction, NOT put in `Arc`/`OnceLock`) + one historical comment. Clean.
- **L2 #14** `SchedulingGroupId` speculative surface — all live grep hits are comments or test doc headers; no live definition (was deleted by D253). Clean.
- **L2 #3** (Sink/TopologySink async return types) — no async return types. Clean.
- **L2 #4** (`.await` in actor closure body) — `core_actor.rs` `run<F, R>` is bounded `F: FnOnce(&Core) -> R + Send + 'static` (compiler-enforced); no `.await` possible. Clean.
- **L2 #5–#8** (napi `.then` inside actor closure / `wrapper.js` Promise stashing / reactive primitive returning `Promise<Node<T>>` / sync escape hatch on binding reads) — D267 landed the structural fix for #8 (BenchGraph reads are async); #5–#7 are not currently violated based on the audit grep scope.
- **L2 #9** `BindingBoundary` widening without a ledger row — every BindingBoundary widening (D232/D243/D244/D245/D245-r5/D248/D249) has a corresponding cross-track-ledger row at `~/src/graphrefly-ts/docs/cross-track-ledger.md` (verified D243/D244/D245/D248 rows present). Clean.
- **L2 #10** new `Impl` method without parity scenario in same slice — D267 added 5 new `Impl` widenings; scenario at `scenarios/graph/remove.test.ts` is the forcing function (per D267 affects-list). Clean.
- **L7 TRASH/ hygiene** — `~/src/graphrefly-rs/crates/graphrefly-core/{tests,benches}/TRASH/` contain files with stale `SchedulingGroupId` + `core.clone()` references, but `cargo metadata --no-deps --format-version=1 | <jq for TRASH src_paths>` returns ZERO matches — cargo's auto-discovery only picks up `tests/*.rs` (not nested), and `benches/*.rs` (not nested). TRASH files are structurally excluded; no `[[test]]`/`[[bench]]` overrides re-include them. Clean.
- **L4** `β` path references — sampled D255/D256/D258 rationale, `β` correctly framed as "dead permanently (D070/D077)". Clean.
- **L5** D255 vs D257 (drop GLOBAL_CORE vs keep) — D257 explicitly reverses the S6 lock step 5 wording and the rationale is sound (the singleton works structurally post-actor since BenchCore is Send+Sync). Documented reversal, not contradiction. Clean.
- **L5** D269's `load_entries`/`load_entries_all` shape vs `feedback_no_backward_compat` — D269 explicitly cites the no-backward-compat rule and chose the split for clarity, not back-compat. Decision-consistent.

## Summary by triage class

- **CLEANUP (6):** L4-001 (Core rustdoc), L4-002 (GraphOps doclinks), L8-002 (D250 stub-deletion doc), L1-001 (MailboxOp::Defer rustdoc), L8-003 (StateCell references — VERIFY-then-CLEANUP), L1-002 (vestigial union-find surface)
- **AMEND-D (2):** L5-001 (D258 inline-comment vs D262/P4 — proposed semantic: "amend D262/P4 affects-list OR the inline comment at `batch.rs:3498-3506` to match the locked downgrade — JS callers do NOT see the message"), L3-001 (§7-C deferral framing — proposed semantic: "porting-deferred §7-C lift-condition re-categorized as decision-consistency restoration backlog, not D196 consumer-pressure queue")
- **DEFER (1):** L6-001 (D267 wording vs landed scope — proposed semantic: "scope D267 to BenchGraph read methods + Core read paths; factory constructors retain `run_sync` under lifecycle-precondition rationale")
- **CLOSE (1):** L8-001 (§7-E `GroupLockRegistry never prunes` — registry is gone; close as structurally resolved by D253/D255; same applies to §7-B and §7-F)
- **VERIFY (0):** none currently; L8-003 may convert to VERIFY if a `StateCell` definition exists outside the grepped scope.

## Notes for the caller

- The five HIGH-severity findings are all decision-consistency or doc-hygiene issues with no behavioral risk today, but each is the kind of "stale-premise propagation" anti-pattern §"Recurring anti-patterns" #3 in the decision-guard skill calls out. A focused doc-hygiene CLEANUP slice (L4-001, L4-002, L8-001, L8-002, L8-003) would close most of the HIGH band in one pass.
- L1-002 (vestigial union-find surface) is the only finding that may benefit from a NEW-D lock: the user explicitly named this audit-pattern ("when a decision RELAXES an invariant... downstream code keeps the stricter shape as 'vestigial surface' unless explicitly cleaned up"). D267 precedent for naming a CLEANUP slice exists (D266 mega-batch staging).
- L3-001 is the user-named framing-error pattern (deferring vestigial-surface cleanup under D196 when the right gate is "decision-consistency restoration"). Worth surfacing in the next /decision-guard session as a recurring-anti-pattern entry alongside the existing 10.
