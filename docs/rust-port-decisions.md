# Rust port decisions

Architectural and product-level decisions made during the port.
Each entry records context, options, and rationale for future reference.

---

### D001 — M2 Slice F scope: port-coverage gaps + reactive describe/observe + napi-rs Graph parity
- **Date:** 2026-05-05
- **Context:** M2 Slice E+ (read-side introspection + composition) landed. "Next batch" to close M2 formally before M3 opens.
- **Options:** A) Port-coverage gaps + napi-rs Graph only (mechanical). B) Reactive describe/observe + Core topology-change primitive (substrate). C) Both.
- **Decision:** C — both. Port-coverage gaps + napi-rs Graph parity AND reactive describe/observe with Core topology-change notification primitive.
- **Rationale:** reactive describe/observe is the substrate needed for dynamic graph visualization (founding vision). Port-coverage gaps are mechanical wrappers. Combined they close M2.
- **Affects:** graphrefly-core (topology sink), graphrefly-graph (gaps + reactive), graphrefly-bindings-js (BenchGraph), parity-tests.

### D002 — Rename RemoveAudit → GraphRemoveAudit
- **Date:** 2026-05-05
- **Context:** Canonical R3.2.3 calls the return type `GraphRemoveAudit`. Current Rust has `RemoveAudit` (mount.rs).
- **Options:** A) Keep `RemoveAudit`. B) Rename to `GraphRemoveAudit`.
- **Decision:** B — rename to `GraphRemoveAudit`. Pre-1.0, free to refactor.
- **Rationale:** Matches canonical spec. `remove(name)` and `unmount(name)` both return it.
- **Affects:** graphrefly-graph `mount.rs`, `lib.rs`, `graph.rs`.

### D003 — napi-rs Graph binding shape: class-per-Graph
- **Date:** 2026-05-05
- **Context:** Need to expose Graph API to JS consumers. BenchCore is a single-class-per-instance pattern.
- **Options:** A) Class-per-Graph (`BenchGraph`). B) Free-function namespace.
- **Decision:** A — class-per-Graph for ergonomic JS consumer.
- **Rationale:** Graph is stateful (holds namespace + mount tree); class shape mirrors JS usage patterns.
- **Affects:** graphrefly-bindings-js.

### D004 — Defer tagFactory / resourceProfile / setVersioning
- **Date:** 2026-05-05
- **Context:** Canonical §3 port-coverage gaps include R3.1.2 tagFactory, R3.6.3 resourceProfile, R3.2.4 setVersioning.
- **Options:** A) Include in Slice F. B) Defer.
- **Decision:** B — defer. Each has non-trivial sub-design (provenance annotation shape, runtime profile schema, §7 versioning port).
- **Rationale:** Low consumer demand; implementation depends on substrate not yet ported.
- **Affects:** porting-deferred.md (new deferral entries).

### D005 — Core topology-change notification architecture
- **Date:** 2026-05-05
- **Context:** Reactive describe/observe needs a substrate for topology change detection. Core must notify when nodes are registered/torn-down/deps-changed.
- **Options:** A) Core-level `subscribe_topology(sink)` with `TopologyEvent` enum. B) Per-registration-site ad-hoc callbacks. C) Node-based topology stream (circular — registering the observer IS a topology change).
- **Decision:** A — Core-level `subscribe_topology(sink)` returning `TopologySubscription`. Graph layer wraps with namespace/mount events + recomputes describe on change.
- **Rationale:** Clean separation. No circularity — topology sinks are NOT nodes. Graph-layer `describe_reactive` calls describe() on each topology event and invokes user callback.
- **Affects:** graphrefly-core (new subscribe_topology API), graphrefly-graph (reactive describe/observe wrappers).

### D006 — Graph-level namespace-change sinks (not Core topology events) for reactive describe/observe
- **Date:** 2026-05-06
- **Context:** Core fires `TopologyEvent::NodeRegistered` from `register_state` / `register_computed` BEFORE `Graph::add()` inserts the name into the namespace. A reactive describe sink that calls `graph.describe()` from a Core topology callback would see the node in Core but not yet in the namespace.
- **Options:** A) Use Core topology events and accept the ordering gap. B) Add Graph-level namespace-change sinks fired from `add()` / `remove()` / `destroy()`. C) Defer Core topology fire to after `add()` (would require restructuring `Graph::state()` to return the ID first, then fire topology events).
- **Decision:** B — Graph-level `NamespaceChangeSink` fired from `add()` / `remove()` / `destroy()` after the inner lock drops. Core topology events still exist for low-level consumers who need `DepsChanged` / `NodeTornDown` notifications.
- **Rationale:** Clean layering — Core topology events remain accurate for Core concerns (they fire when Core state changes), while Graph-level namespace sinks fire when Graph state changes. `set_deps` reactivity is available via Core topology subscription for callers who need it.
- **Affects:** graphrefly-graph `graph.rs` (new `subscribe_namespace_change` / `fire_namespace_change`), `describe.rs` (`describe_reactive` uses namespace sinks), `observe.rs` (`observe_all_reactive` uses namespace sinks).

### D007 — Drop-order discipline for reactive observe_all
- **Date:** 2026-05-06
- **Context:** `GraphObserveAllReactive` holds both a namespace sink subscription (via id) and an `Arc<Mutex<ObserveAllReactiveInner>>` containing `Vec<Subscription>`. The namespace sink closure captures another `Arc` to the same inner. If the namespace sink's `Arc` is the last reference when unsubscribing, dropping it tries to drop `Subscription`s, each of which locks `CoreState`. But unsubscribing already holds the Graph inner lock → deadlock.
- **Options:** A) Use `Weak<Mutex<...>>` in the closure. B) Ensure the struct's own `inner` Arc drops AFTER the closure's Arc (so the closure's Arc is never the last reference). C) Explicit manual `Drop` that clears subscriptions before unsubscribing.
- **Decision:** B+C hybrid — declare `ns_sink_id` field before `inner` field (Rust drops in declaration order), AND implement `Drop` that explicitly unsubscribes the namespace sink before `inner` drops.
- **Rationale:** Belt-and-suspenders approach. The explicit `Drop` is the primary safety mechanism; field ordering is defensive.
- **Affects:** graphrefly-graph `observe.rs` (`GraphObserveAllReactive` field order + Drop impl).

### D009 — M3 operator architecture: option (c) Core-dispatch with `OperatorOp` `NodeKind` variant
- **Date:** 2026-05-06
- **Context:** First operators slice (Slice C-1) needs an architecture lock before code lands. Three options: (a) operators-as-derived-factories (TS-canonical shape; sugar over `register_derived`), (b) pre-canned fn impls registered with the binding (operators crate ships `Box<dyn Fn(&[DepBatch])>` builders), (c) Core dispatches via new `NodeKind::Operator(OperatorOp)` variant; binding extends FFI surface with bulk projection methods.
- **Options:** A=derived-factory, B=pre-canned fn impls, C=Core dispatch.
- **Decision:** C — Core dispatch with `NodeKind::Operator(OperatorOp)` and bulk projection FFI methods (`project_each` / `predicate_each` / `fold_each`) on `BindingBoundary`. Operator-specific logic (filter wave-exclusivity, distinct-by-identity, pairwise prev-value) becomes Core-internal — zero FFI for operators that don't need user callbacks.
- **Rationale:** Maximum perf (one FFI per fire regardless of batch length, internal short-circuits for filter/distinct), and the substrate (`DepRecord` + `DepBatch` + `FnResult::Batch`) was already designed for this dispatch shape. The user explicitly requires that the operators crate not import the graph layer — operators must consume `Core` only, which option C satisfies via direct `Core::register_operator` calls.
- **Affects:** graphrefly-core (NodeKind extension, OperatorOp enum, fire_fn dispatch fork, new BindingBoundary methods), graphrefly-operators (factories), graphrefly-bindings-js (FFI surface widening).

### D010 — Operator user-callback boundary: option (b) helper trait + (c) builder per knob-heavy op
- **Date:** 2026-05-06
- **Context:** Operators with user callbacks (`map(project)`, `filter(predicate)`, `scan(reducer, seed)`) need to plumb Rust closures or JS-facing fn ids through to the FFI surface. Three options: (a) caller pre-registers FnId, (b) operators crate accepts `Box<dyn Fn>` and registers via a thin `OperatorBinding` helper trait, (c) builder struct per operator with chained config.
- **Options:** A=pre-registered FnId, B=Box<dyn Fn> via helper trait, C=builder per operator.
- **Decision:** B for ergonomic ops (map / filter / scan / distinctUntilChanged) + C for knob-heavy ops (throttle / retry / circuitBreaker etc.).
- **Rationale:** B hides the register-then-pass-FnId dance for the common case; C is justified when an operator has 3+ config knobs. A is too verbose at the call site for trivial wins.
- **Affects:** graphrefly-operators (OperatorBinding trait + operator factories).

### D011 — `partial: bool` register option lifted into Core
- **Date:** 2026-05-06
- **Context:** R5.4 partial-mode is operator-specific (map/filter/scan want partial-on; withLatestFrom/combine want partial-off). Core's R2.5.3 first-run gate is currently unconditional. Without a `partial` knob, operators can't opt out of the gate.
- **Options:** A=keep first-run gate hardcoded (operators that need partial-on can't ship), B=add `partial: bool` to `register_derived` / `register_dynamic` / `register_operator`.
- **Decision:** B — Core gains a `partial: bool` register option. Default `false` (existing behavior preserved). Operators set their own default per type.
- **Rationale:** R5.4 needs Core support for the partial-true mode; this is the right time to plumb it (alongside the operator dispatch extension).
- **Affects:** graphrefly-core (register_* signature extension; first-run gate conditional).

### D012 — Filter is silent-drop (no RESOLVED on rejected items)
- **Date:** 2026-05-06
- **Context:** R1.3.3.b filter wave-exclusivity. TS legacy emits RESOLVED when entire wave rejects; never per-dropped-item. Question: does Rust port mirror, or take a cleaner shape now that Slice B's R1.3.1.a fix removed spurious DIRTY?
- **Options:** A=mirror TS (RESOLVED on fully-rejected wave), B=silent drop (no DIRTY, no RESOLVED, no settle when predicate rejects).
- **Decision:** B — silent drop. Subscribers see no message for dropped values. Cleaner; falls out naturally of the R1.3.1.a fix.
- **Rationale:** With R1.3.1.a (DIRTY only queued when not-already-dirty), filter doesn't need to settle if it never dirtied. Edge case for upstream-broadcast DIRTY: Core suppresses Operator-Filter's downstream DIRTY when the predicate fully rejects, OR Operator-Filter queues a single RESOLVED to drain its self-DIRTY without forwarding. Implementation choice deferred to slice C-1.
- **Affects:** graphrefly-core (Operator dispatch path), graphrefly-operators (filter factory).

### D013 — M3 Slice A /qa + napi DepBatch parity + M2 parity-tests widening (B + C + D batch)
- **Date:** 2026-05-06
- **Context:** Three follow-up gaps surfaced post-M3 Slice A+B: deferred batch-accumulation tests for the new DepRecord substrate; deferred napi-rs DepBatch parity; M2 close-gate residual D6 (parity-tests not yet widened).
- **Options:** A=M3 operators (next milestone), B=parity-tests widening, C=Slice A /qa tests, D=napi-rs DepBatch parity.
- **Decision:** B + C + D combined batch (skip A operator slice for this batch — large architecture decisions need their own slice).
- **Rationale:** All three are smaller, lower-risk, close known gaps. C and D exercise the substrate end-to-end. B closes the M2-close-gate residual without touching graphrefly-rs.
- **Affects:** graphrefly-core (`tests/dep_batch.rs`), graphrefly-bindings-js (`BuiltinBatchFn` + `BatchEmissionJs` + `DepBatchJs` + `register_batch_derived` + `batch_emit_messages`), graphrefly-ts/packages/parity-tests (`scenarios/graph/*`).

### D014 — M3 Slice C-1 scope: bundle full transform module in one slice
- **Date:** 2026-05-06
- **Context:** First operators slice. Three viable cuts: A=one slice (substrate + 6 ops), B=three slices (substrate+map / filter+distinct / scan+reduce+pairwise), C=two slices (stateless / stateful).
- **Options:** A / B / C.
- **Decision:** A — bundle substrate + all 6 transform operators (map / filter / scan / reduce / distinctUntilChanged / pairwise) in one slice.
- **Rationale:** End-to-end value; substrate gets exercised by all operator shapes (stateless / stateful / terminal-aware). The pre-locked architecture (D009–D012) is sufficient to ship the full transform module without iteration on the FFI shape.
- **Affects:** graphrefly-core (NodeKind extension, OperatorOp/OperatorState enums, fire_fn dispatch fork, BindingBoundary extensions, partial flag), graphrefly-operators (OperatorBinding trait + factories), tests, parity-tests.

### D015 — `OperatorBinding` lives in graphrefly-operators
- **Date:** 2026-05-06
- **Context:** The closure-registration helper trait per D010 needs a home. Two options: graphrefly-core (alongside `BindingBoundary`) vs graphrefly-operators (alongside the factories that need it).
- **Options:** A=graphrefly-core, B=graphrefly-operators.
- **Decision:** B — graphrefly-operators.
- **Rationale:** Cleaner layering. `BindingBoundary` is the Core-callable FFI surface (lives in core); `OperatorBinding` is operator-factory ergonomics (lives where the factories are). Bindings impl both traits for the same struct.
- **Affects:** graphrefly-operators `binding.rs`.

### D016 — Projector signature is `Fn(HandleId) -> HandleId`
- **Date:** 2026-05-06
- **Context:** Operator user closures fundamentally take `T` and return `R`, but the FFI surface is opaque `HandleId`. Confirm: binding-side `register_projector` wraps `Fn(T) -> R` into `Fn(HandleId) -> HandleId` by deref+intern.
- **Options:** A=Core sees `T`, B=Core sees `HandleId` only with binding-side wrapping.
- **Decision:** B — `register_projector` accepts `Box<dyn Fn(HandleId) -> HandleId + Send + Sync>`. The binding-side helper that converts `Fn(T) -> R` to this shape is a binding concern (e.g., a `BoxedRustBinding::wrap_projector<T, R>(f)` constructor).
- **Rationale:** Preserves the handle-protocol cleaving plane invariant (Core sees opaque `HandleId` only). Binding-side wrapping is a convenience, not a Core concern.
- **Affects:** graphrefly-operators `binding.rs` (trait definition + sample wrapping helper for the test binding).

### D017 — Operator equals defaults to Identity, overridable via `OperatorOpts`
- **Date:** 2026-05-06
- **Context:** R5.7 transform-operator output equals discipline. Should operator output cache participate in equals substitution like a regular Derived?
- **Options:** A=hardcoded Identity (no override), B=Identity default with override.
- **Decision:** B — `OperatorOpts { equals: EqualsMode, partial: bool }` with `Default::default()` returning `Identity` + `false`.
- **Rationale:** Matches the Derived/Dynamic equals discipline; users who need custom output equals (e.g., distinct-via-derived shape) can opt in.
- **Affects:** graphrefly-core `register_operator` signature.

### D018 — Filter silent-drop emits self-RESOLVED on full-reject
- **Date:** 2026-05-06
- **Context:** Filter wave-exclusivity (D012). When predicate fully rejects a wave, two sub-options: (a) let upstream-broadcast DIRTY ride and queue a single RESOLVED to drain, (b) suppress Operator-Filter's downstream DIRTY broadcast entirely.
- **Options:** A=let DIRTY ride + RESOLVED on full-reject, B=suppress DIRTY entirely.
- **Decision:** A — let DIRTY ride; queue RESOLVED on full-reject.
- **Rationale:** Doesn't fight the cross-node two-phase tier-then-node flush. Aligns with the existing "no-op equals propagates DIRTY+RESOLVED" pattern (already deferred). Mixed-batch waves (some pass, some reject) emit DIRTY + per-pass DATA; full-reject emits DIRTY + RESOLVED. Per-rejected-item RESOLVED is suppressed (matches TS).
- **Affects:** graphrefly-core `fire_fn` filter dispatch path.

### D019 — `partial: bool` added to all three register methods
- **Date:** 2026-05-06
- **Context:** D011 lifts `partial` into Core. Question: do we plumb it through `register_state` / `register_derived` / `register_dynamic` (public-API break, mechanical), use a `RegisterOpts` struct (cleaner but bigger break), or use a separate `set_partial(node)` setter (no break but state mutation hazard)?
- **Options:** A=positional arg, B=RegisterOpts struct, C=setter.
- **Decision:** A — add `partial: bool` as a trailing positional arg to all three. State takes 2 args (initial, partial); Derived/Dynamic take 4 args (deps, fn_id, equals, partial). For State, partial is a no-op (state nodes don't fire fn) but kept for surface consistency.
- **Rationale:** Pre-1.0; breaking the registration surface is cheap. RegisterOpts struct could come later as a unifying refactor — for now, the positional add keeps call-site updates mechanical.
- **Affects:** graphrefly-core `Core::register_*` signatures + every call site (tests, benches, graph crate, bindings).

### D008 — Slice F /qa decisions (P1–P7 fix priorities)
- **Date:** 2026-05-06
- **Context:** Adversarial review of Slice F surfaced 7 priority correctness bugs (P1–P7). Each had clear fix shape but some had architecture-touching trade-offs (subscribe contract, push-on-subscribe semantics, Weak-capture refactor, parity-test scope).
- **Decisions:**
  - **P1, P2, P3, P4, P6:** apply correctness patches as proposed. P1 mirrors `destroy()` namespace-clear-after-cascade. P2 makes `teardown_inner` return `Vec<NodeId>` of cascaded ids; `Core::teardown` fires `NodeTornDown` per id. P3 wires `parent.fire_namespace_change()` in mount/mount_new/unmount. P4 installs the namespace listener BEFORE the initial snapshot in `GraphObserveAllReactive::subscribe`. P6 captures `Weak<Mutex<GraphInner>>` + `Core` in reactive sinks instead of strong `Graph` clones.
  - **P5(a):** `GraphObserveAllReactive::subscribe` panics on second call (subscribe-once v1 contract) instead of unsubscribe-then-rebind. Rebuild the handle to install another sink.
  - **P7(a):** `Graph::describe_reactive` pushes the initial snapshot synchronously before installing the namespace listener. Matches canonical R3.6.1 push-on-subscribe semantics; consumers always start from a known baseline.
  - **P8(b):** `packages/parity-tests/scenarios/graph/` widening deferred to a dedicated follow-up slice (D6 in `porting-deferred.md`). Slice F /qa scope kept tight at the Rust impl + tests.
- **Rationale:** P1–P4, P6 are pure correctness fixes that defeat slice goals if left in. P5(a) prefers explicit failure over silent leak; v1 callers can always rebuild the handle. P7(a) aligns with the dynamic graph visualization use case (UI subscribes, sees current state immediately, then deltas).
- **Affects:** `graphrefly-core/src/{node.rs,topology.rs}`, `graphrefly-graph/src/{graph.rs,mount.rs,describe.rs,observe.rs}`, regression tests across `tests/{topology.rs,gap_fills.rs,reactive.rs}`.

### D020 — M3 Slice C-2 scope: Combine + WithLatestFrom + Merge (Core-dispatch)
- **Date:** 2026-05-06
- **Context:** M3 Slice C-1 (transform operators) landed. Next operators slice targets multi-dep combinators. Operators split into three categories: (a) multi-dep Core-dispatch (combine, withLatestFrom, merge), (b) subscription-managed (zip, concat, race), (c) higher-order (switchMap, exhaustMap, concatMap, mergeMap). Category (a) fits the existing Core-dispatch pattern; (b) and (c) need new infrastructure.
- **Options:** A) All combinators in one slice. B) Category (a) only. C) (a) + (b).
- **Decision:** B — Combine + WithLatestFrom + Merge only. Multi-dep Core-dispatch operators that extend the existing `OperatorOp` + `fire_operator` pattern.
- **Rationale:** Coherent slice (~1000 lines). Subscription-managed and higher-order operators need different infrastructure (dynamic inner subscription management) — separate slice.
- **Affects:** graphrefly-core (OperatorOp variants, fire_op_* dispatch, BindingBoundary pack_tuple), graphrefly-operators (factories + OperatorBinding), parity-tests.

### D021 — WithLatestFrom semantics: match GraphReFly Phase 10.5 (partial: false gate)
- **Date:** 2026-05-06
- **Context:** Q1 — what semantics for withLatestFrom? RxJS/callbag silently drop primary emissions when secondary hasn't emitted. GraphReFly (Phase 10.5) uses `partial: false` first-run gate to hold until both deps deliver, then "fire on primary alone" post-warmup.
- **Options:** A) RxJS-style silent drop. B) GraphReFly Phase 10.5 gate. C) Redefine.
- **Decision:** B — match GraphReFly Phase 10.5. Gate holds until both deps deliver real DATA. Post-warmup: fire on primary alone, sample secondary from `dep_records[1].prev_data`. INVALIDATE guard: if `prev_data == NO_HANDLE` and batch empty post-warmup → settle with RESOLVED.
- **Rationale:** Strictly better than RxJS (handles cache invalidation). Already proven in production TS. The first-run gate is a Core primitive — no extra machinery needed.
- **Affects:** graphrefly-core `fire_op_with_latest_from`, graphrefly-operators `with_latest_from` factory.

### D022 — Merge is zero-FFI Core-dispatch
- **Date:** 2026-05-06
- **Context:** Merge forwards all dep DATA handles verbatim — no transformation. In TS, it uses producer pattern (manual subscribes). In Rust, can be an OperatorOp variant with N deps.
- **Options:** A) Producer-style (subscription-managed). B) Core-dispatch OperatorOp with zero FFI on fire.
- **Decision:** B — `OperatorOp::Merge` with no `fn_id`. `fire_op_merge` collects all dep batch handles, retains each, emits each verbatim. Zero binding calls on fire path.
- **Rationale:** Merge doesn't transform values — just forwards. Core already handles DIRTY/COMPLETE tracking. Zero FFI is maximally efficient.
- **Affects:** graphrefly-core `fire_op_merge`.

### D023 — Combine custom tuple-equality deferred
- **Date:** 2026-05-06
- **Context:** TS combine implements element-wise custom equals for tuple dedup. In Rust, each `pack_tuple` call produces a fresh HandleId → identity-equals never deduplicates.
- **Options:** A) Implement tuple custom equals now. B) Defer — identity-equals is v1 default.
- **Decision:** B — defer. Identity-equals means combine always emits (no dedup between waves). Acceptable for v1; users can add custom equals via `OperatorOpts` if needed.
- **Rationale:** No bench evidence that tuple dedup is load-bearing. Matches "no optimization without evidence" principle.
- **Affects:** porting-deferred.md (new deferral entry).

### D024 — M3 Slice C-3 scope: bundle flow operators (take/skip/takeWhile/last + sugar)
- **Date:** 2026-05-06
- **Context:** Next operators slice after C-2 (combinators). Flow operators are single-dep, count/predicate-based, fit existing `OperatorOp` + `fire_operator` pattern naturally. `takeUntil` requires producer/subscription-managed pattern (D020 category B) — separate slice.
- **Options:** A) Bundle take + skip + takeWhile + last + sugar (first/find/element_at). B) Split into stateless (take/skip/first) and stateful (takeWhile/last/find/element_at). C) Add `takeUntil` too via subscription-managed primitive.
- **Decision:** A — bundle take + skip + takeWhile + last (+ first/find/element_at sugar) in one slice. Defer `takeUntil` to a later subscription-managed slice.
- **Rationale:** Coherent slice (~900–1200 lines including the `op_scratch` migration per D026). `takeUntil`'s producer pattern is fundamentally different infrastructure; doesn't fit this slice's scope.
- **Affects:** graphrefly-core (4 new OperatorOp variants, fire_op_* helpers, NodeKind::skips_auto_cascade for Last), graphrefly-operators (flow.rs), parity-tests (operators/flow.test.ts).

### D025 — `last` factory shape: two factories (`last` + `last_with_default`)
- **Date:** 2026-05-06
- **Context:** TS `last(source, options?: { defaultValue?: T })` accepts an optional default. Rust factory shape options.
- **Options:** A) Two factories (`last(source)` errors-on-empty as silent no-op, `last_with_default(source, default)` always emits). B) Single `last(source, default: Option<HandleId>)`. C) `OperatorOpts`-style `last(source, opts: LastOpts { default: Option<HandleId> })`.
- **Decision:** A — two factories. `last(source)` emits only `Complete` on empty stream (subscriber sees `[Start, Complete]` — no Data); `last_with_default(source, default)` emits `[Start, Data(default), Complete]` on empty stream.
- **Rationale:** Mirrors existing scan/reduce + scan_with/reduce_with split for `OperatorOpts`. Cleanest call-site for the no-default common case.
- **Affects:** graphrefly-operators `flow.rs` factories.

### D026 — Per-node generic scratch slot: `op_scratch: Option<Box<dyn OperatorScratch>>` (replaces typed `operator_state: HandleId`)
- **Date:** 2026-05-06
- **Context:** Take/Skip need a `u32` counter that spans waves; the existing `operator_state: HandleId` slot can't carry primitive integers. Three architectural options for adding cross-wave operator state.
- **Options:** A) Add a parallel typed field `operator_count: u32` on `NodeRecord` for Take/Skip; keep `operator_state: HandleId` for handle-bearing ops. B) Replace `operator_state` with a generic `Box<dyn OperatorScratch>` slot; each operator defines its own state struct (e.g., `TakeState { count_emitted: u32 }`, `ScanState { acc: HandleId }`, `LastState { latest: HandleId }`). C) Per-OperatorOp variant carries inline state (conflates registration config with runtime state).
- **Decision:** B + migrate existing operators in this slice (Q6 path (i)). Add `OperatorScratch: Any + Send + Sync` trait with `release_handles(binding)` method; replace `operator_state: HandleId` field; refactor Scan/Reduce/Distinct/Pairwise's `fire_op_*` helpers to use `op_scratch_mut::<TheirState>(rec)`. New Flow operators use `op_scratch` natively.
- **Rationale:** User explicitly anticipates more state needs. Generic scratch scales to N operators with one `NodeRecord` field; release path consolidated through trait method (no per-operator Drop case in `Drop for CoreState`). Heap allocation per stateful operator instance is acceptable (per-node, not per-fire). Migrating in the same slice avoids dual-mechanism debt.
- **Affects:** graphrefly-core `node.rs` (new `OperatorScratch` trait, `op_scratch` field, removal of `operator_state` field), `batch.rs` (`op_scratch_mut` helper, refactor of all 4 existing `fire_op_*` helpers + 4 new ones), `Drop for CoreState` (walk op_scratch + call release_handles), `reset_for_fresh_lifecycle` (call release_handles + clear scratch).

### D027 — `take(0)` edge case: allow at factory; first fire emits zero items then self-completes
- **Date:** 2026-05-06
- **Context:** TS `take.ts:33` short-circuits at registration: returns a node that emits `[COMPLETE]` immediately on first fire. Rust port options.
- **Options:** A) Reject at factory (`assert!(count > 0)`). B) Allow `0`; `fire_op_take` immediately self-terminates on first dep delivery (no Data emit). C) Match TS exactly — emit Complete on registration (before any dep fire).
- **Decision:** B — allow `count = 0` at factory. First time `fire_op_take` runs (dep delivers DATA), we increment counter zero times in the per-input loop, find `count_emitted >= count` immediately, emit `Complete`, self-terminate via `Core::complete(node_id)`.
- **Rationale:** Simpler than C (no special registration-time path); doesn't reject a legitimate degenerate use case. Subscriber sees `[Start, Dirty, Complete]` on first fire — predictable.
- **Affects:** graphrefly-core `fire_op_take`, graphrefly-operators `take` factory.

### D028 — Flow operator deactivation parity: only resubscribable terminal lifecycle reset clears scratch
- **Date:** 2026-05-06
- **Context:** TS Lock 6.D resets Take's `taken` / Skip's `skipped` / TakeWhile's `done` / Last's `latest` on deactivation (when last subscriber leaves). Rust v1 has the broader "deactivation cleanup not yet modeled" deferral; only resubscribable terminal lifecycle reset (`reset_for_fresh_lifecycle`) is wired.
- **Options:** A) Match TS — wire deactivation hook for these operators (introduces partial deactivation cleanup). B) Match Rust v1's existing pattern — clear scratch only via `reset_for_fresh_lifecycle`.
- **Decision:** B — match Rust v1 pattern. Document divergence in `porting-deferred.md`. A take(n) re-subscribed mid-stream (without terminal cycle) won't reset its counter in Rust v1.
- **Rationale:** Matches the broader "deactivation cleanup not yet modeled" deferral; lifts together with M2-graph mount/unmount work. Avoids partial deactivation hook for one operator family.
- **Affects:** graphrefly-core `reset_for_fresh_lifecycle` (calls `release_handles` + clears scratch); porting-deferred.md (new entry).

### D029 — TakeWhile reuses `BindingBoundary::predicate_each` (no new FFI)
- **Date:** 2026-05-06
- **Context:** TakeWhile needs a `Fn(T) -> bool` predicate. Filter already exposes `predicate_each(fn_id, &[HandleId]) -> Vec<bool>`.
- **Options:** A) Add a new FFI surface for take-while (e.g., `predicate_each_until_false`). B) Reuse `predicate_each`; `fire_op_take_while` iterates results, emits up to first `false`, then self-completes.
- **Decision:** B — reuse `predicate_each`. Slightly wasteful when the predicate is expensive AND most-of-batch will be cut off, but the FFI surface stays narrow.
- **Rationale:** No bench evidence the wasted predicate calls matter. Adding a new FFI surface for a perf optimization without evidence violates Pass 5 directive.
- **Affects:** graphrefly-core `fire_op_take_while`, graphrefly-operators `take_while` factory.

### D030 — Drop `NodeKind` enum from `NodeRecord`; kind is derived metadata
- **Date:** 2026-05-06
- **Context:** Slice D substrate prep. Per the user direction "find a good way to unify them, not just internal dispatch; node in TS is not mere internal dispatching." The TS `NodeImpl` has no `_kind` field — kind is determined by `(deps.length, fn.is_some(), _isDynamic)`. Rust port had a `kind: NodeKind` field on `NodeRecord` that duplicated information already encoded in the field shape; adding new kinds (Producer, AutoTrack) required new enum variants + new register methods + new fire helpers.
- **Options:** A) Keep `kind` field as-is; add `Producer` variant; B) Drop `kind` field, derive on demand from `(deps.is_empty(), fn_id, op, is_dynamic)`; C) Tag-union refactor (NodeKindShape enum that owns deps + fn/op together).
- **Decision:** B — drop the `kind: NodeKind` field. `NodeRecord` now carries `fn_id: Option<FnId>` + `op: Option<OperatorOp>` + `is_dynamic: bool`. Helper methods (`is_state()`, `is_producer()`, `is_compute()`, `is_operator()`, `skips_auto_cascade()`, `kind()`) cover predicate needs. Public API `Core::kind_of(id) -> Option<NodeKind>` derives the enum on each call.
- **Rationale:** Mirrors TS data model exactly. Adding new shapes (Producer this slice; AutoTrack later) becomes additive — Producer is just `(deps.is_empty() && fn_id.is_some())`, no new enum variant or register method needed for the substrate. Eliminates the "kind must stay in sync with field shape" invariant. Single source of truth.
- **Affects:** graphrefly-core `NodeRecord` shape; ~15 internal match sites switched from `match rec.kind` to predicate methods or `match rec.op`; `kind_of` derives via `NodeRecord::kind()`. `NodeKind` enum stays as the public API surface (with new `Producer` variant added).

### D031 — Producer node kind via unified `register()` shape (D030 corollary)
- **Date:** 2026-05-06
- **Context:** Subscription-managed combinators (zip / concat / race / takeUntil) need a node shape that fires its fn once on first subscribe and uses the binding to manage its own subscriptions to upstream sources (the TS producer pattern in `extra/operators/combine.ts:332-379`). Pre-D030 this would have required a new `NodeKind::Producer` variant + new register method + new fire-helper.
- **Options:** A) Add `register_producer` as sugar over the unified `register()` (D030 substrate makes this a one-line wrapper); B) Keep producer registration entirely binding-side (binding registers a State node, then manually wires fn-firing).
- **Decision:** A — `Core::register_producer(fn_id) -> NodeId` sugar over `register()` with `deps: vec![], fn_or_op: Some(NodeFnOrOp::Fn(fn_id))`. Producer kind is derived from the field shape; no new enum variant in `NodeRecord`, no new fire helper. The wave engine fires the fn via the existing `fire_regular` path on first subscribe (queued by `activate_derived` walking the order vec).
- **Rationale:** D030 made this nearly free. Adds one new variant to the public `NodeKind` enum (for `kind_of` reporting) + one new sugar method + one new line in `activate_derived` (queue producer in `pending_fires`). Total substrate cost: ~30 LOC.
- **Affects:** graphrefly-core `NodeKind::Producer` variant, `Core::register_producer`, `activate_derived` producer queue branch.

### D032 — Producer fn signature unchanged: `Fn(&[DepBatch]) -> FnResult` with empty dep_data
- **Date:** 2026-05-06
- **Context:** Producer fn fires lock-released with no deps. The existing `BindingBoundary::invoke_fn(node_id, fn_id, dep_data)` signature works for producers if dep_data is just an empty slice — the binding's fn body uses captured state (Core ref + binding ref) to call `Core::subscribe` from inside.
- **Options:** A) Keep invoke_fn unchanged; producer fn body sees `dep_data: &[]` (empty); B) Add a new `invoke_producer_fn(node_id, fn_id, ctx: ProducerCtx)` method on BindingBoundary.
- **Decision:** A — invoke_fn stays. Empty dep_data is the natural representation for a no-deps node. The binding constructs its ergonomic ProducerCtx wrapper from inside its fn body using captured Arc refs (the same pattern FnCtx uses today — see D034).
- **Rationale:** Symmetric with how FnCtx-equivalent state is binding-side, not Core-side (D034). No new FFI surface. Bindings that don't ship producers don't pay any cost.
- **Affects:** None — invoke_fn signature unchanged.

### D033 — Producer state lives in the binding, not Core's `op_scratch`
- **Date:** 2026-05-06
- **Context:** Subscription-managed producers need to track upstream `Subscription` handles so they can be dropped on producer deactivation. Two storage options.
- **Options:** A) Store in `NodeRecord::op_scratch` (Core-side); B) Store in the binding's per-node state map (binding-side); C) Hybrid — Core knows about subs but binding owns state.
- **Decision:** B — binding owns producer state. Core invokes `BindingBoundary::producer_deactivate(node_id)` on last unsubscribe; the binding drops its per-node entry, which transitively drops the Subscription handles via their Drop impl (they reach back into Core::state to remove the sink).
- **Rationale:** (1) Symmetric with FnCtx — both are binding-side wrappers around Core primitives. (2) `Subscription::Drop` re-enters Core's state lock; if subs lived in `op_scratch` (which is accessed under the lock), the drop-path would deadlock. Binding ownership lets the deactivate hook fire lock-released so Subscription drops re-enter cleanly. (3) Multi-binding parity — each binding (napi-rs / pyo3 / wasm) ships its own idiomatic ergonomic wrapper.
- **Affects:** graphrefly-core `BindingBoundary::producer_deactivate(node_id)` lifecycle hook (default no-op); binding implementations own producer state maps.

### D034 — FnCtx is binding-side; ProducerCtx follows the same pattern
- **Date:** 2026-05-06
- **Context:** During the substrate design discussion, surfaced that the Rust port has NO Core-side FnCtx — `BindingBoundary::invoke_fn(node_id, fn_id, dep_data: &[DepBatch])` only passes opaque per-dep state. Each binding (napi-rs, etc.) constructs its own ergonomic FnCtx-shape from `dep_data` for the user fn. Same pattern applies to ProducerCtx.
- **Decision:** Document the symmetry. Core provides primitives (`subscribe`, `emit`, lifecycle hooks); the binding wraps them into ctx-shaped APIs for user closures. ProducerCtx lives in `graphrefly-operators::producer` (Commit 2) as the default Rust-side helper; bindings may use it directly OR replace with their own version.
- **Rationale:** Symmetric layering. Core stays minimal; binding ergonomics are binding-side.
- **Affects:** Architecture / design clarification; no code changes.

### D035 — `BindingBoundary::producer_deactivate(node_id)` lifecycle hook
- **Date:** 2026-05-06
- **Context:** Producer needs a way to signal the binding "drop your per-node state" when the last subscriber leaves. Hook fires from `Subscription::Drop` when the just-removed sub was the last one on a producer node.
- **Decision:** Add `BindingBoundary::producer_deactivate(_node_id: NodeId)` with default no-op. Bindings that ship producers override it. Fires lock-released (after `Subscription::Drop` releases the state lock) so the binding's deactivation impl may re-enter Core (`release_handle`, even `subscribe` for unusual scenarios).
- **Rationale:** Single new FFI method. Default no-op keeps existing bindings (TestBinding, BenchBinding, etc.) compatible without forced edits. Re-entrance allowed because lock-released.
- **Affects:** graphrefly-core `BindingBoundary::producer_deactivate` (new default-no-op method); `Subscription::Drop` (clones Arc<dyn BindingBoundary> out of state, calls `producer_deactivate` after lock drops).

### D036 — `ProducerBinding` super-trait + `ProducerCtx` in `graphrefly-operators::producer`
- **Date:** 2026-05-06
- **Context:** D-ops needed an ergonomic API for subscribing to upstream sources from inside a producer fn body (zip/concat/race/takeUntil pattern). Per D034 / D031, the producer-state surface lives binding-side (symmetric with FnCtx), not in Core.
- **Options:** A) Single trait method `register_producer_build` returning a FnId; binding stores closures + state map. B) Generic `Any`-backed scratch slot in Core's `op_scratch`; closure captures Core/binding refs. C) New Core-level method `Core::subscribe_for_producer(producer_node, source, sink)`.
- **Decision:** A — `ProducerBinding: BindingBoundary` super-trait with `register_producer_build(build: ProducerBuildFn) -> FnId` + `producer_storage() -> &ProducerStorage`. `ProducerCtx::subscribe_to(source, sink)` calls `Core::subscribe` and stuffs the resulting `Subscription` into the binding's `producer_storage[node_id]`. `default_producer_deactivate(storage, node_id)` is the helper bindings call from their `BindingBoundary::producer_deactivate` impl to drop the storage entry (which cascades into Subscription drops).
- **Rationale:** Pure binding-layer pattern; Core stays minimal. Symmetric with `OperatorBinding` (D015) — both extend `BindingBoundary` with closure-registration + storage. Multi-binding parity preserved (each binding implements the trait its own way; the operators crate ships a default helper).
- **Affects:** graphrefly-operators `producer` module (new); operator factories (`zip` / `concat` / `race` / `take_until`) accept `&Arc<dyn ProducerBinding>` at registration.

### D037 — Op-specific state captured in build-closure Arcs (not stored in `producer_storage.op_state`)
- **Date:** 2026-05-06
- **Context:** Each producer op needs per-instance mutable state (zip's per-source FIFOs, concat's phase flag, race's winner index, takeUntil's terminated flag). Two storage options surface in the design.
- **Options:** A) Store state in the build closure's captures via `Arc<Mutex<OpState>>`; sinks capture clones. B) Store state in `ProducerNodeState::op_state: Option<Box<dyn Any>>` (binding-side per-node slot); sinks resolve via `binding.producer_storage().lock().get(node_id).op_state.downcast_mut()`.
- **Decision:** A — closure captures. The build closure constructs `Arc<Mutex<OpState>>` on each activation; sinks capture clones. State lifetime tracks Subscription lifetime: when producer_deactivate fires and drops the storage entry → Subscriptions drop → sinks drop → state Arc count decrements → state drops with the last sink.
- **Rationale:** Mirrors TS impl exactly (each producer fn body creates fresh local state per activation via JS closure capture). Simpler than `Any`-downcast machinery in `op_state` slot. Each activation gets a fresh state instance — no cross-cycle pollution. The `op_state` field on `ProducerNodeState` stays available for future ops that want trait-object storage but is unused by the four core ops.
- **Affects:** graphrefly-operators `ops_impl.rs` (closure-capture pattern in each operator).

### D038 — Widen `Core::emit` to accept Producer nodes (not just State)
- **Date:** 2026-05-06
- **Context:** Producer ops' sink callbacks need to call `Core::emit(producer_node, h)` to drive emissions on the producer node. Pre-D038 `Core::emit` panicked on non-State nodes (Slice A close discipline: "emit() is for state nodes only; derived emits via fn").
- **Options:** A) Widen the assertion to accept State OR Producer. B) Add a separate `Core::emit_producer(node_id, h)` method. C) Have producers commit emissions via a different path (e.g., `Core::commit_emission_for_producer`).
- **Decision:** A — widen `Core::emit`'s assertion to `rec.is_state() || rec.is_producer()`. Producer is conceptually a sink-driven source: state-driven sources accept user `emit`; producer-driven sources accept sink-callback `emit`. Both bypass the fn-return-value emission path used by Derived/Dynamic/Operator.
- **Rationale:** State and Producer are both intrinsic-source kinds. Unifying their emission API matches the canonical-spec data model where producer is "node(fn)" and emits via `actions.emit` (which maps to `Core::emit` at the FFI plane). Adding a separate method would duplicate the same logic.
- **Affects:** graphrefly-core `Core::emit` (assertion widened, doc updated).

### D039 — M3 next-batch sequencing: Option 2 (D-ops /qa cleanup) before Option 1 (Slice E higher-order)
- **Date:** 2026-05-07
- **Context:** D-ops landed 2026-05-07 with /qa surfacing D1–D6 deferred concerns. D4 (concat phase-0 COMPLETE bug — concat hangs if `s2` completes before `s1` during phase-0) is a real correctness bug. Higher-order operators (switchMap/exhaustMap/concatMap/mergeMap) are the natural next operator category but stack on the producer substrate.
- **Options:** A) Land Slice E (higher-order) immediately; address D-ops /qa items as follow-ups. B) Land D-ops /qa cleanup first (D4 concat fix + D2 Arc cycle audit + D5 loom check + D6 parity widening), then Slice E. C) Bundle D-ops /qa front-loaded into Slice E as one larger slice.
- **Decision:** B — Option 2 (D-ops /qa cleanup) first as its own coherent slice, then Option 1 (Slice E) as a separate slice.
- **Rationale:** D4 is a real hang in a shipped operator; fixing before stacking new producer-pattern code avoids building on a known bug. D2 Arc cycle audit protects the higher-order operators (which reuse the same producer substrate). Splitting keeps each slice's blast radius contained.
- **Affects:** Sequencing only; no code shape impact.

### D040 — Idiomatic Rust shape convention: codebase already converged
- **Date:** 2026-05-07
- **Context:** Surveyed Rust port for TS-shape-mirroring patterns (TS `number | "unbounded"` style). Asked whether codebase needs broad refactor toward more idiomatic Rust shapes.
- **Findings:** Rust port is already ~95% idiomatic — newtype wrappers (`NodeId`/`HandleId`/`LockId`/`FnId`), `EqualsMode` enum vs TS strings, `Option<T>` for "absent/unbounded" instead of sentinel values, `Result<T, E>` with `thiserror` for fallible operations. The relevant precedent for "unbounded vs cap" is `pause_buffer_cap: Option<usize>` (None = unbounded; Some(n) = cap; Some(0) = degenerate-but-allowed).
- **Decision:** No codebase-wide refactor needed. Convention to apply for future "unbounded vs cap" shapes: `Option<u32>` (or `Option<usize>` where natural), matching the `pause_buffer_cap` precedent. Don't deviate to `NonZeroU32` since the existing precedent doesn't enforce non-zero either.
- **Rationale:** Consistency with established convention beats stricter typing on case-by-case basis. If a future slice needs non-zero enforcement, lift convention then; for now, preserve uniformity.
- **Affects:** mergeMap concurrency (D043 below), any future "cap" parameter.

### D041 — D4 fix: concat phase-0 COMPLETE handoff
- **Date:** 2026-05-07
- **Context:** Current concat impl ignores `second` source COMPLETE during phase 0 (before `first` completes). After phase transition (when `first` completes), concat drains `pending` then waits for new `second` data — but `second` already completed during phase 0, and Complete fires once. Result: concat hangs.
- **Options:** A) Track `second_completed_in_phase_0` flag in concat state; on phase transition, if flag is set AND `pending` is empty post-drain, self-complete immediately. B) Track per-source phase + completion separately; combine at transition. C) Re-subscribe second source post-transition (defeats the producer-pattern semantics).
- **Decision:** A — track `second_completed: bool` in the concat closure-captured state. On phase transition (after `first` Complete arrives), drain `pending`, then if `second_completed && pending.is_empty()`, call `Core::complete(node_id)` lock-released.
- **Rationale:** Simplest fix; matches TS legacy semantics. The flag is cheap (one bool added to ConcatState). Refcount discipline: pending entries are already tracked for retain/release on drain.
- **Affects:** `crates/graphrefly-operators/src/ops_impl.rs::concat` (ConcatState + sink behavior); regression test in `tests/subscription.rs`; parity scenario in `scenarios/operators/subscription.test.ts`.

### D042 — D5 loom verification: extract Subscription drop-path under loom-compat primitives
- **Date:** 2026-05-07
- **Context:** D-ops /qa flagged D5: `Subscription::Drop` race under concurrent unsubscribe could allow double-deactivate or missed-deactivate when two threads race to drop the last two subscriptions. User direction: formal loom model-check.
- **Options:** A) Inline conditional compilation (`#[cfg(loom)] use loom::sync::Mutex; #[cfg(not(loom))] use parking_lot::Mutex;`) across affected modules — broad blast radius, every Mutex/Arc swap. B) Extract minimal race-prone state into a separate `SubscriberRegistry` module with loom-compat primitives directly; `parking_lot::Mutex<SubscriberRegistry>` wraps for production; loom test instantiates with `loom::sync::Mutex` directly. C) Use `shuttle` instead (PCT scheduler, no instrumentation needed) — different tool, similar role.
- **Decision:** B — extract the race-prone subscriber-count + producer-deactivate decision into a focused testable shape. Add `loom` as a dev-dep gated on `cfg(loom)`. New test file `crates/graphrefly-core/tests/loom_subscription.rs` runs `RUSTFLAGS="--cfg loom" cargo test --test loom_subscription`.
- **Rationale:** Minimal blast radius; aligns with how loom is typically integrated in well-engineered Rust libraries (e.g., tokio). Avoids broad cfg-gating across the codebase. Decision logic to verify: "the thread that observes count==1 before its decrement is the sole one that fires producer_deactivate".
- **Affects:** New dev-dep `loom`; new test file; possible minor refactor to `Subscription::Drop` to make the decision logic locally-testable.

### D043 — mergeMap concurrency: `Option<u32>` (None = unbounded)
- **Date:** 2026-05-07
- **Context:** TS `MergeMapOptions` has `concurrency?: number` (default Infinity = unbounded). Rust port shape choice.
- **Options:** A) `concurrency: Option<u32>` (None = unbounded; Some(n) = cap). B) `concurrency: Option<NonZeroU32>` (compile-time rejection of 0). C) Custom enum `MergeConcurrency::Unbounded | Limit(u32)`.
- **Decision:** A — `Option<u32>`. Mirrors `pause_buffer_cap: Option<usize>` convention from D040.
- **Rationale:** Codebase consistency (D040). `concurrency: Some(0)` is degenerate (would queue everything indefinitely) but matches `pause_buffer_cap: Some(0)`'s degenerate behavior.
- **Affects:** `graphrefly-operators::higher_order::merge_map` factory signature.

### D044 — Higher-order operator slice scope: switchMap → exhaustMap → concatMap → mergeMap, all four in one slice
- **Date:** 2026-05-07
- **Context:** TS `extra/operators/higher-order.ts` exports four higher-order operators. Land all in one slice or split?
- **Decision:** All four in one slice (Slice E), in TS-source order: switchMap → exhaustMap → concatMap → mergeMap.
- **Rationale:** All four share the producer-pattern substrate (D-substrate) and reuse the projector closure shape `Fn(T) -> Node<R>`. Bundling preserves the slice cadence (~600–900 LOC including tests + parity, similar to Slice C-3 + D-ops scope). User explicitly approved.
- **Affects:** `graphrefly-operators::higher_order` module (new); 4 new factory functions; binding extension for projector registration; tests + parity scenarios.

### D046 — Slice E /qa: inner-sub tracking moves out of producer_storage; INVALIDATE/TEARDOWN forwarded
- **Date:** 2026-05-07
- **Context:** Adversarial review of Slice E surfaced three correlated issues: (P1) `switch_map`'s phase-1 retain on `outer_h` was conditional on `!terminated` while phase-2 release was unconditional, causing a refcount underflow on `[Data, Error]` same-batch waves. (P2) `merge_map` / `concat_map` accumulated completed-inner `Subscription`s in `producer_storage[producer_id].subs` indefinitely — O(N) memory leak per producer over inner-completion lifetime. (Cached-outer) `ctx.subscribe_to(source, outer_sink)` calls `core.subscribe` BEFORE pushing the outer sub; if `source` is cached, the synchronous handshake fires outer_sink → invokes project + subscribes to inner → `push_inner_sub` lands at `subs[0]` → outer subscribe returns → outer sub lands at `subs[1]`. `subs = [inner, outer]`. Subsequent `drop_inner_subs` truncates to len 1, dropping the OUTER sub. (P3) `build_inner_sink` only handled Data/Complete/Error; INVALIDATE/TEARDOWN from inner were silently dropped.
- **Decision:**
  - Move inner-sub tracking OUT of `producer_storage.subs` and into per-op state Mutex: `SwitchState.inner_sub: Option<Subscription>`, `ExhaustState.inner_sub: Option<Subscription>`, `MergeMapState.inner_subs: HashMap<u64, Subscription>` keyed by per-op `next_inner_id`. `producer_storage.subs` holds only the OUTER source sub (single entry, no positional invariants). Removes `drop_inner_subs` / `push_inner_sub` helpers.
  - `switch_map` phase-1: track `latest_retained: bool`; phase-2 release gated on `latest_retained` (P1 fix).
  - `build_inner_sink` forwards `Message::Invalidate` → `Core::invalidate(producer_id)` and `Message::Teardown` → `Core::teardown(producer_id)`. DIRTY/RESOLVED/PAUSE/RESUME/Start still dropped (acknowledged divergence). (P3 option A.)
  - `merge_map` drain converted from recursive `spawn_inner` (on_complete → spawn_inner → core.subscribe → on_complete → ...) to iterative loop guarded by thread-local `MERGE_DRAIN_ACTIVE`. Outermost drain owns the loop; nested `on_complete` invocations only decrement state and return. Prevents stack overflow on pathological pre-completed-inner buffer drains.
  - `pending_inner_ids: AHashSet<u64>` on MergeMapState distinguishes "spawn started, on_complete not yet fired" from "on_complete fired during subscribe". Post-subscribe code skips inserting the dead sub if `on_complete` already removed the id.
- **Rationale:** All four issues share a root cause — positional/lifecycle dependence on `producer_storage.subs`. The state-Mutex-per-op refactor cleans up the shape and prevents future positional bugs (e.g., adding a notifier sub for new combinators). INVALIDATE forwarding closes a real spec gap (R1.2.7); TEARDOWN forwarding propagates inner destruction correctly. Iterative drain via thread-local guard prevents stack overflow without complex trampolining.
- **Affects:** `crates/graphrefly-operators/src/higher_order.rs` (full refactor), `tests/higher_order.rs` (new regression tests: P1 [Data, Error] underflow, P2 storage-no-accumulation, cached-outer outer-sub-survives, INVALIDATE forwarding); `crates/graphrefly-operators/src/ops_impl.rs` (concat first_sink defensive per-iteration `terminated` check); `packages/parity-tests/scenarios/operators/subscription.test.ts` (`test.skip` → `test.runIf(impl.name !== "pure-ts")` so Rust-port-only assertions activate when rustImpl publishes).

### D045 — Subscribe handshake fires lock-released; remove IN_HANDSHAKE_FIRE diagnostic
- **Date:** 2026-05-07
- **Context:** Slice E surfaced a v1 limitation: cached-inner state nodes can't be subscribed from inside higher-order operator sinks because the handshake fires lock-held with the `IN_HANDSHAKE_FIRE` thread-local panic-diagnostic. User pushback: "How come there are so many v1 limitations. Can we not introduce that many limitation and fix them directly?"
- **Options:** A) Defer the limitation; document it as v1 (original Slice E plan). B) Per-sink staging-buffer machinery (queue concurrent wave messages destined for the new sink until handshake completes, then drain) — substantial refactor. C) Acquire `wave_owner` re-entrant mutex first, then drop state lock before firing handshake. Cross-thread emits block on `wave_owner`; same-thread re-entry from sinks passes through reentrantly.
- **Decision:** C — `Core::subscribe` acquires `wave_owner.lock_arc()` first, takes state lock briefly to install the sink + snapshot handshake state, drops state lock, fires per-tier handshake LOCK-RELEASED. `IN_HANDSHAKE_FIRE` thread-local + `HandshakeFireGuard` removed; `lock_state()` no longer asserts. Same-thread sink re-entry into Core (`emit` / `complete` / `error` / nested `subscribe`) is now safe. Cross-thread emits still preserve R1.3.5.a happens-after via `wave_owner` serialization (already established by Slice A close /qa Q2 wave-owner mutex).
- **Rationale:** ~50 LOC change (much less than the per-sink staging-buffer alternative). Reuses existing `wave_owner` infrastructure rather than adding new machinery. Unblocks the canonical Slice E user pattern (`switch_map(outer, |n| state(Some(n*10)))`) — cached inner state nodes work transparently. User pushback was correct: the limitation was unnecessary.
- **Affects:** `graphrefly-core::node.rs` (subscribe path; module doc; remove `IN_HANDSHAKE_FIRE` thread-local + `HandshakeFireGuard` struct + assertion in `lock_state`). Test rewrite: `tests/lock_discipline.rs::handshake_sink_reentry_panics_with_diagnostic_not_deadlocks` → `handshake_sink_can_reenter_core_emit_on_other_node` (negative-becomes-positive). `tests/higher_order.rs` updated to use realistic cached-inner pattern. Closes the porting-deferred entries: "Subscribe-time handshake fires lock-held; re-entrance from handshake panics" + "Cached-inner handshake re-entry panics in v1".

### D047 — `in_tick` re-keyed Core-global → per-(Core, thread); Phase J disjoint "speedup" was a drain-skip bug artifact
- **Date:** 2026-05-15 (user-locked)
- **Context:** CI `cargo test --all-targets` failed on a `wave_state_clear_outermost` debug-assert in the `per_subgraph_parallelism` bench. Root cause: `CoreState.in_tick` (the wave-ownership / drain gate) was Core-global. Under concurrent emits on **disjoint partitions of one Core** (the per-subgraph-parallelism regime — threads do NOT block each other), thread B observed thread A's `in_tick = true`, wrongly classified its independent disjoint wave as nested, and no-op'd its `BatchGuard::drop` — so B's wave never drained (leaked payload retains, undelivered sink batches; caught late by the assert). A pure thread-local `in_tick` instead broke cross-Core same-thread isolation (/qa F1: Core-A's flag leaks to Core-B on the same OS thread).
- **Options:** A) per-(Core, thread) keyed thread-local. B) derive ownership from the per-partition `wave_owner` the thread already holds (canonical D3/Q4 design — larger refactor, same drain cost). C) silence the bench under `cargo test` and ship the bug. D) revert per the Sub-slice-4 `:846` ≥5% gate (untenable: "revert" = keep a correctness bug to preserve a number that *was* the bug).
- **Decision:** A — `thread_local IN_TICK_OWNED: AHashSet<u64>` keyed by `Core::generation` (`crate::batch`), with `Core::{in_tick,claim_in_tick,clear_in_tick}`. Satisfies all three constraints jointly: per-Core key ⇒ cross-Core isolation (/qa F1); per-thread ⇒ disjoint-partition drain correctness; shared slot within one (Core, thread) ⇒ nested re-entry (/qa EC#3). `currently_firing` stays Core-global on `CoreState` (cross-thread P13 set_deps check, /qa F2) — unchanged. The `lock_state()` round-trip in `begin_batch_with_guards` is dropped (in_tick has no cross-thread read requirement).
- **Perf consequence (clean back-to-back criterion A/B, low machine load):** the fix correctly makes each disjoint thread own + drain its wave — work the buggy baseline silently skipped. The Phase J disjoint-regime figures were therefore substantially bug artifacts:
  - `fnfire_parallel_2t_disjoint`: baseline (buggy) 1.09 ms → fixed 4.63 ms. The documented Phase J "fn-fire-heavy 2t-disjoint = 1.35 ms, **1.23× speedup**" inverts to **≈2.8× SLOWER than serial** once thread B actually drains.
  - `parallel_4t_disjoint` (state-emit) +27%; serial ±5%; same-partition / fn-fire-serial ≈ neutral.
- **Rationale / acceptance:** correctness supersedes a bug-inflated speedup — the baseline was fast because broken. The disjoint "speedup" claims are *corrected in the docs*, not the fix reverted. **Follow-up C closed-as-investigated 2026-05-15 (no code):** a cheap-correct-drain fast-path is not viable — subscriber delivery is mandatory correctness work, wave-cache rollback snapshots are taken on ≈every real emit (predicate unreachable), and the residual cost is the Core-global `lock_state()` in the post-drain block; the only real recovery lever is the **already-deferred Q2/Q3 per-partition state-shard refactor**, not a fast-path (`porting-deferred.md` Phase J CORRECTION has the full reasoning). **Escalated strategic question (NOT decided here):** the Phase J disjoint-parallel advantage was substantially a bug artifact, so the Rust port's *parallelism* selling point in that regime is weak/absent pending Q2/Q3 — but **correct-Rust vs `@graphrefly/pure-ts` throughput/memory/FFI is UNMEASURED** (parity `rustImpl` arm activates only post-`@graphrefly/native`-publish). The "4×" is correct-vs-buggy *intra-Rust*, not the cross-impl gap, and must not be read as such. The Rust port's value proposition (per `CLAUDE.md`: compiler-enforced safety + future canonical impl, with perf a *secondary, workload-gated* claim per the pre-existing Phase J hedge) needs a proper correct-Rust-vs-pure-ts benchmark before its performance case is relied on — flagged for spec-owner, not silently rewritten.
- **TLA+/MC:** `wave_protocol_partitioned_MC` (graphrefly-ts, CI-gated) is **not re-validated and does not need to be** — `in_tick` is a Rust-impl drain-ownership gate that is **not modeled** in the TLA+ spec; the modeled per-partition `wave_owner` mutex relation is **unchanged** by this slice. (Recorded explicitly per the no-autonomous-decisions principle rather than left implicit.)
- **`/qa` hardening (applied in the same slice):** a fn/sink panic in the drain phase that escapes the per-call `catch_unwind` isolation previously skipped BOTH the WaveState drain (→ next owning wave trips `wave_state_clear_outermost`) and the `in_tick` clear — a **pre-existing** window (the old `s.in_tick = false` had the identical post-drain placement). Fix: wrap `drain_and_flush()` in `catch_unwind`; on a caught panic run the shared `discard_wave_cleanup()` (factored out of the closure-body-panic branch so both panic origins get identical `BatchGuard` atomicity) + `clear_in_tick()`, then `resume_unwind`. **A first attempt used an end-of-`drop` RAII releaser; `/qa` Phase 3 caught that it regressed `lock_discipline::sink_can_reenter_core_via_emit` / `…_complete_another_node_from_callback`** — sinks fire in `fire_deferred` *before* `drop` scope ends, so deferring the clear to drop-end left `in_tick` owned during `fire_deferred`, making a re-entrant sink emit a non-owning no-op (its data silently lost). Corrected to **explicit `clear_in_tick()` after drain+cleanup but BEFORE `fire_deferred`** on each of the three exit paths (closure-body-panic branch, drain-panic `catch_unwind` arm, success-path locked block) — the exact placement of the pre-D047 `s.in_tick = false`, now with the drain-panic window closed by the `catch_unwind`. New regression test `tests/batch.rs::panic_in_drain_phase_releases_wave_ownership_for_next_wave` pins the hardened path. `/qa` triage: 2 review subagents (Blind + Edge Case Hunter); BH-major / EC F-1 fixed (per user direction); EC F-2 (`#[must_use]` on `in_tick()`) + stale-slot doc accuracy applied; EC F-3 (`Generation` newtype, invariant #8) deferred to the broader newtype sweep (also touches `PARTITION_CACHE`); EC F-4 rejected (verified no lock-discipline defect).
- **Affects:** `graphrefly-core::batch.rs` (`IN_TICK_OWNED` thread_local + helpers; `begin_batch_with_guards`; 3 `commit_emission` reads; `BatchGuard::discard_wave_cleanup()` factored; `BatchGuard::drop` `catch_unwind`-around-drain + explicit `clear_in_tick` ×3 before `fire_deferred`; `#[must_use]` on `in_tick()`; doc comments), `graphrefly-core::node.rs` (`CoreState.in_tick` field + init removed; doc comments). New regression tests: `tests/batch.rs::cross_core_same_thread_batchguard_isolation` (pins /qa F1 under the new keying), `tests/batch.rs::panic_in_drain_phase_releases_wave_ownership_for_next_wave` (pins the /qa drain-panic hardening), `tests/per_subgraph_parallelism.rs::disjoint_partition_concurrent_emits_each_drain_and_deliver` (pins the bug fix: both disjoint threads deliver + no retain leak). Panic→clean-slot also covered by existing `tests/batch.rs::batch_panic_discards_pending_wave`. Docs: this entry; `docs/porting-deferred.md` Phase J CORRECTION + Sub-slice-4 `:846` NOTE; `CLAUDE.md` invariant #3 correction; `docs/migration-status.md`.

