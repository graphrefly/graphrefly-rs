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
