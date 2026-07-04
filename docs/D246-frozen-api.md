# D246 boundary-1 — FROZEN API contract (consumer-cascade spec)

> **Historical port-era document.** This file is retained as an audit trail for
> the retired Rust port model. It is not current package API guidance and is not
> a shared docs authority. Current Rust package docs are governed by
> [`docs.jsonl`](docs.jsonl); language-neutral authority lives in
> `~/src/graphrefly`.

The keystone landed in `graphrefly-core` + `graphrefly-graph` (both
compile). Every consumer must be rewritten to THIS surface. Do **not**
re-introduce `SubgraphRef`/`GraphOps`/`NamespaceHandle`/`'g` graph
lifetimes/`SnapshotOps`/core-RAII-`Drop`-unsubscribe. Pre-1.0 — no
back-compat shims (delete legacy, don't bridge).

## graphrefly_core::OwnedCore (NEW — the one Core-ownership keystone)

```rust
use graphrefly_core::OwnedCore;
let rt = OwnedCore::new(binding /* Arc<dyn BindingBoundary> */);     // owns Core
let core: &Core = rt.core();                                          // D231 owner-side &Core
rt.binding();                                                         // Arc<dyn BindingBoundary>
let sub  = rt.track_subscribe(node_id, sink);                         // -> SubscriptionId, tracked
let tsub = rt.track_subscribe_topology(topo_sink);                    // -> TopologySubscriptionId, tracked
rt.unsubscribe(node_id, sub);  rt.unsubscribe_topology(tsub);         // explicit early (idempotent)
// Drop(rt): owner-thread synchronous teardown of all tracked subs.
```
`OwnedCore<C: StateCell = LockedCell>`. Replaces `TestRuntime` /
`StructuresRuntime` / the operators harness runtime — those become thin
newtypes that **compose** `OwnedCore` + their `*TestBinding`/`Recorder`
(keep the Recorder/binding infra; only the Core-ownership+sub-track+Drop
keystone moves to `OwnedCore`). Pattern:

```rust
pub struct TestRuntime { pub binding: Arc<TestBinding>, rt: OwnedCore }
impl TestRuntime {
    pub fn new() -> Self { let b = TestBinding::new();
        Self { binding: b.clone(), rt: OwnedCore::new(b as Arc<dyn BindingBoundary>) } }
    pub fn core(&self) -> &Core { self.rt.core() }
    pub fn track_subscribe(&self, n, s) -> SubscriptionId { self.rt.track_subscribe(n,s) }
    pub fn unsubscribe(&self, n, s) { self.rt.unsubscribe(n,s) }
    // state()/derived()/dynamic()/subscribe_recorder()/cache_value() etc:
    // keep, but route Core access through self.core().
}
// delete the hand-rolled `subs: Mutex<Vec<..>>` + `impl Drop` — OwnedCore owns that now.
```
`StateHandle`/`Recorder` keep working: they call `self.core()` /
`rt.core()`. `Recorder` already holds no core RAII (just ids) — unchanged.

## graphrefly_core::CoreFull (the ONE facade — unchanged surface + `mailbox`)

`CoreFull` now also has `fn mailbox(&self) -> Arc<CoreMailbox>`. It is
the one object-safe facade (mutation+inspection+serialize+mailbox). Any
in-wave `MailboxOp::Defer(Box<dyn FnOnce(&dyn CoreFull)>)` closure may
use it. `Core: CoreFull`, so `&core` coerces to `&dyn CoreFull`.

## graphrefly_graph::Graph (ONE type — Core-free, `Clone`, `Send+Sync+'static`)

`Graph::new(name)` — **no binding/core arg**. A subgraph is a `Graph`
(cheap `Arc` clone). Deleted: `SubgraphRef`, `GraphOps`,
`NamespaceHandle`, `SnapshotOps`, `MountError::CoreMismatch`,
`Graph::view`, `Graph::with_existing_core`, `with_core`.

**Pure-namespace (NO `&Core`):** `name()`, `node(path)`,
`try_resolve(path)`, `try_resolve_checked(path)`, `name_of(id)`,
`node_count()`, `node_names()`, `child_names()`, `is_destroyed()`,
`is_valid_name(s)` (assoc), `subscribe_namespace_change(sink)`,
`unsubscribe_namespace_change(id)`, `observe(path)->GraphObserveOne`,
`observe_all()->GraphObserveAll`,
`observe_all_reactive()->GraphObserveAllReactive`,
`ancestors(include_self)->Vec<Graph>`, `inner_arc()` (pub(crate)).

**`&Core` first arg (D246 r2):** `state(core,name,Option<HandleId>)`,
`derived(core,name,&[NodeId],FnId,EqualsMode)`,
`dynamic(core,name,&[NodeId],FnId,EqualsMode)`, `add(core,id,name)`,
`set(core,name,h)`, `get(core,name)->HandleId`,
`invalidate_by_name(core,name)`, `complete_by_name(core,name)`,
`error_by_name(core,name,h)`, `remove(core,name)`,
`edges(core,recursive)`, `describe(core)`,
`describe_with_debug(core,&dyn DebugBindingBoundary)`,
`describe_reactive(core,DescribeSink)->ReactiveDescribeHandle`,
`fire_namespace_change(core)`, `subscribe(core,id,sink)->SubscriptionId`,
`unsubscribe(core,id,sub)`, `unsubscribe_topology(core,tsub)`,
`emit(core,id,h)`, `cache_of(core,id)`, `has_fired_once(core,id)`,
`complete(core,id)`, `error(core,id,h)`, `teardown(core,id)`,
`invalidate(core,id)`, `pause(core,id,lock)`, `resume(core,id,lock)`,
`alloc_lock_id(core)`, `set_deps(core,n,&[NodeId])`,
`set_resubscribable(core,id,bool)`, `add_meta_companion(core,p,c)`,
`batch(core,FnOnce())`, `signal(core,SignalKind)`,
`signal_invalidate(core)`, `destroy(core)`,
`mount(core,name,&Graph)->Graph`, `mount_new(core,name)->Graph`,
`mount_with(core,name,FnOnce(&Graph))->Graph`,
`unmount(core,name)->Result<GraphRemoveAudit,MountError>`,
`snapshot(core)->GraphPersistSnapshot`,
`restore(core,&snap)`,
`Graph::from_snapshot(core,&snap,Option<SnapshotBuilder>,Option<IndexMap<String,NodeFactory>>)->Result<Graph,_>`.

Observe/describe handles: **NO RAII `Drop`** (D246 r3). Teardown is
owner-invoked, synchronous:
- `GraphObserveOne::subscribe(core,sink)->ObserveSub`;
  `ObserveSub::detach(core)`, `.node_id()`, `.sub_id()`.
  `GraphObserveOne::{pause,resume,invalidate}(core,...)`.
- `GraphObserveAll::subscribe(&mut self,core,sink)->usize`;
  `.detach(&mut self,core)`.
- `GraphObserveAllReactive::subscribe(&mut self,core,sink)->usize`;
  `.detach(&mut self,core)`.
- `ReactiveDescribeHandle::detach(core)`.
- `SnapshotBuilder = Box<dyn FnOnce(&Core,&Graph)>`;
  `NodeFactory = Box<dyn Fn(&Core,&Graph,&str,&NodeSlice,&[NodeId])->Result<NodeId,SnapshotError>>`.

In tests, replace RAII-drop reliance: keep the handle bound and call
`.detach(rt.core())` at end-of-scope. **`detach(core)` is REQUIRED for
reactive describe/observe + storage handles** — their Core message/
topology subscriptions are opened via raw `core.subscribe*` and are
**not** `OwnedCore`-tracked, so `OwnedCore` drop does NOT collect them
(only subs opened via `OwnedCore::track_subscribe` are). `graph.destroy
(core)` collects the namespace-change sinks (QA-A1); the Core
topology/message subs still need explicit `detach(core)`.

## graphrefly_structures (Blind #3 — D246 rule 6: mutation = owner-sync)

`EmitHandle<S>`: **drop the `mailbox` field**; signature becomes
`fn emit(&self, core: &Core, snapshot: S) -> Version` doing
`core.emit(self.node_id, (self.intern)(snapshot))` synchronously
owner-side (bump `version` first, exactly as before). Constructors
(`ReactiveLog/List/Map/Index::new`) keep `core: &Core` arg but build
`EmitHandle { node_id, intern, version: AtomicU64::new(0) }` (no
`mailbox`).

**Every mutation method gains `core: &Core` as the first param** and
passes it to `self.emitter.emit(core, snapshot)`:
`ReactiveLog::{append,append_many,clear,trim_head}` (+ any other mut),
`ReactiveList::{append,append_many,insert,insert_many,pop,clear}`,
`ReactiveMap::{set,delete,clear}`, `ReactiveIndex::{...,delete,clear}`.
Preserve mutation_log version ordering (emit returns Version; record
after, same as today).

**In-wave sink emitters** (`view`/`scan` `view_emitter`/`scan_emitter`,
captured into long-lived `core.subscribe` sinks that fire IN-WAVE) keep
deferred re-entry — add a private
`struct SinkEmitter<S> { mailbox: Arc<CoreMailbox>, node_id, intern, version: AtomicU64 }`
with `fn emit(&self,snapshot)->Version { post_emit }`; build it with
`core.mailbox()` and capture it into the sink closures (replaces the
old `EmitHandle{mailbox,..}` used there). `attach`/`attach_storage`
already use raw `mailbox.post_emit` in-sink — keep as is (genuine
deferred sink re-entry, D246 r6).

`ReactiveSub<'c>` (RAII unsubscribe in `Drop`): **remove the `Drop`**,
keep ids, add `pub fn detach(&self, core: &Core)` (owner-invoked,
synchronous). It still carries `core: &'c Core` ONLY if needed for
ergonomic detach-on-scope-exit — prefer: drop the `'c`, store
`subs: Vec<(NodeId,SubscriptionId)>`, expose `detach(&self,&Core)`.
`LogView`/`ScanHandle`/`AttachStorageHandle` hold a `ReactiveSub`
(rename field accessible) + expose `detach(&self,&Core)` delegating.
Update structures tests to call `.detach(rt.core())` instead of relying
on drop.

## graphrefly_storage

`graph_integration.rs`: any `SubgraphRef`/`GraphOps`/`NamespaceHandle`
import → `Graph`. Core-touching graph calls take `rt.core()`/`&Core`.
The in-wave observe-sink snapshot path uses `&dyn CoreFull` +
`snapshot_of`-equivalent via the public `Graph::snapshot(core)` is
owner-side; the deferred in-wave path posts `MailboxOp::Defer(|cf| {
... cf ... })` and calls the free path (mirror existing intent — it
already used `CoreFull`). Keep behavior identical; only the API shape
changes.

## Test-cascade rule (all crates)

`Graph::new("g", binding)` → `let rt = OwnedCore::new(binding); let g = Graph::new("g");`
then thread `rt.core()` into every Core-touching `g.*`. Bind reactive
handles and `.detach(rt.core())` (or rely on `OwnedCore` Drop +
`g.destroy(rt.core())`). `bindings-js` is OUT of default-members /
boundary-1 (D245/§1e) — record-and-skip, do not touch.
