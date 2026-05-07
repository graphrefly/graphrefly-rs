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
