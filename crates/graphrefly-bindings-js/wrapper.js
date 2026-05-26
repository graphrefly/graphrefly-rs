// @ts-check
'use strict';

/**
 * `@graphrefly/native` — hand-written ergonomic ASYNC public surface.
 *
 * This is the Option C deliverable (graphrefly-ts
 * `archive/docs/SESSION-DS-native-substrate-contract.md` →
 * "Option C — committed follow-on slice plan"; decisions D206 / D207).
 *
 * It is a typed, async public API over the napi `Bench*` classes
 * (`index.js`) whose shape mirrors the parity `Impl` contract
 * (graphrefly-ts `packages/parity-tests/impls/types.ts`). Every
 * Core-touching call is a `Promise` because the napi Core runs on a
 * tokio blocking pool (D070 / D077) — a sync call into the TSFN
 * bridge deadlocks libuv.
 *
 * **It is NOT a `@graphrefly/pure-ts` sync drop-in** and
 * `@graphrefly/graphrefly` does NOT consume it (that stays Option B /
 * D080, deferred). It serves direct async-tolerant Node consumers.
 *
 * This module is the single source of truth for the Rust-arm adapter:
 * the parity harness (`packages/parity-tests/impls/rust.ts`) now
 * imports `createNativeImpl()` from here instead of carrying its own
 * private ~1800-LOC copy — eliminating the parity-vs-real divergence
 * the N1 follow-up flagged.
 *
 * No dependency on `@graphrefly/pure-ts`: this ships from
 * `@graphrefly/native` (Rust substrate) and owns its own protocol
 * tier symbols.
 */

// napi binding (graphrefly-native.<target>.node loader). The require is
// kept lazy-tolerant so a consumer on an unbuilt checkout gets a clear
// error from `createNativeImpl()` rather than a module-load crash.
let nativeBinding = null;
let nativeLoadError = null;
try {
  // eslint-disable-next-line global-require
  nativeBinding = require('./index.js');
} catch (e) {
  nativeLoadError = e;
}

// ---------------------------------------------------------------------------
// Protocol tier symbols — owned by this surface (no pure-ts dependency).
// Symbol identity is per-surface; parity scenarios reference `impl.DATA`
// etc., so cross-impl comparisons stay self-consistent.
// ---------------------------------------------------------------------------

const DATA = Symbol('graphrefly.DATA');
const RESOLVED = Symbol('graphrefly.RESOLVED');
const DIRTY = Symbol('graphrefly.DIRTY');
const INVALIDATE = Symbol('graphrefly.INVALIDATE');
const PAUSE = Symbol('graphrefly.PAUSE');
const RESUME = Symbol('graphrefly.RESUME');
const COMPLETE = Symbol('graphrefly.COMPLETE');
const ERROR = Symbol('graphrefly.ERROR');
const TEARDOWN = Symbol('graphrefly.TEARDOWN');

// Variant code table — must match `core_bindings.rs` MSG_CODE_*.
const MSG_CODE_START = 0;
const MSG_CODE_DIRTY = 1;
const MSG_CODE_RESOLVED = 2;
const MSG_CODE_DATA = 3;
const MSG_CODE_INVALIDATE = 4;
const MSG_CODE_PAUSE = 5;
const MSG_CODE_RESUME = 6;
const MSG_CODE_COMPLETE = 7;
const MSG_CODE_ERROR = 8;
const MSG_CODE_TEARDOWN = 9;

function tierForCode(code) {
  switch (code) {
    case MSG_CODE_DIRTY:
      return DIRTY;
    case MSG_CODE_RESOLVED:
      return RESOLVED;
    case MSG_CODE_DATA:
      return DATA;
    case MSG_CODE_INVALIDATE:
      return INVALIDATE;
    case MSG_CODE_PAUSE:
      return PAUSE;
    case MSG_CODE_RESUME:
      return RESUME;
    case MSG_CODE_COMPLETE:
      return COMPLETE;
    case MSG_CODE_ERROR:
      return ERROR;
    case MSG_CODE_TEARDOWN:
      return TEARDOWN;
    default:
      return Symbol(`unknown-tier-${code}`);
  }
}

function symbolToCode(sym) {
  if (sym === DATA) return MSG_CODE_DATA;
  if (sym === DIRTY) return MSG_CODE_DIRTY;
  if (sym === RESOLVED) return MSG_CODE_RESOLVED;
  if (sym === INVALIDATE) return MSG_CODE_INVALIDATE;
  if (sym === PAUSE) return MSG_CODE_PAUSE;
  if (sym === RESUME) return MSG_CODE_RESUME;
  if (sym === COMPLETE) return MSG_CODE_COMPLETE;
  if (sym === ERROR) return MSG_CODE_ERROR;
  if (sym === TEARDOWN) return MSG_CODE_TEARDOWN;
  throw new Error(`[graphrefly/native] unknown tier symbol: ${String(sym)}`);
}

/**
 * Decode a flat `[code, payload, ...]` array into `[tier, value?]`
 * tuples, resolving handle payloads via the JS-side registry. Skips
 * `Start` (user sinks don't observe the per-subscription handshake).
 * Throws on a registry miss for DATA/ERROR — a silent `undefined`
 * would be misread as the "never emitted" SENTINEL.
 */
function decodeMessages(flat, registry) {
  const messages = [];
  for (let i = 0; i < flat.length; i += 2) {
    const code = flat[i];
    const payload = flat[i + 1];
    if (code === MSG_CODE_START) continue;
    if (code === MSG_CODE_DATA || code === MSG_CODE_ERROR) {
      if (!registry.has(payload)) {
        throw new Error(
          `[graphrefly/native] decodeMessages: unknown HandleId(${payload}) for ` +
            `code=${code === MSG_CODE_DATA ? 'DATA' : 'ERROR'}; registry mirror missing. ` +
            'Possible Rust-side handle leak or release_callback miss.',
        );
      }
      messages.push([code === MSG_CODE_DATA ? DATA : ERROR, registry.get(payload)]);
    } else if (code === MSG_CODE_PAUSE || code === MSG_CODE_RESUME) {
      messages.push([tierForCode(code), payload]);
    } else {
      messages.push([tierForCode(code)]);
    }
  }
  return messages;
}

// ---------------------------------------------------------------------------
// JSValueRegistry — per-BenchCore Map<u32 handle, T> (handle-protocol
// cleaving plane: Core sees only u32 HandleIds; user values live here).
// ---------------------------------------------------------------------------

class JSValueRegistry {
  constructor() {
    this.store = new Map();
  }

  set(handle, value) {
    this.store.set(handle, value);
  }

  get(handle) {
    return this.store.get(handle);
  }

  has(handle) {
    return this.store.has(handle);
  }

  delete(handle) {
    this.store.delete(handle);
  }

  get size() {
    return this.store.size;
  }
}

// ---------------------------------------------------------------------------
// NativeNode<T> — async node wrapper over (BenchCore, NodeId, registry).
// ---------------------------------------------------------------------------

class NativeNode {
  constructor(core, nodeId, registry) {
    this.core = core;
    this.nodeId = nodeId;
    this.registry = registry;
    this.cacheValue = undefined;
    // D263/D264 — user-derived rewire surface. Populated only for
    // nodes built via `impl.map`'s `register_user_derived` reroute;
    // setDeps/addDep/removeDep throw on nodes without these fields.
    // - _operators: BenchOperators handle (needed for rebind_user_derived).
    // - _fnId: the FnId Core was told to dispatch when this node fires.
    // - _deps: the current ordered array of NativeNode deps; mirrors
    //   what `Core::set_deps` sees Rust-side. Tracked JS-side so
    //   addDep/removeDep can compute the new slice without re-querying.
    // - _userFnAdapter: the wrapper-side adapter factory that turns a
    //   user fn into the `(deps: number[][]) => number[]` shape Core
    //   wants. Captured so addDep/removeDep can rebuild the closure
    //   for the changed dep-shape.
    this._operators = null;
    this._fnId = null;
    this._deps = null;
    this._currentUserFn = null;
    // QA A2 — stashed rollback error from a prior failed rewire. When
    // set, any further setDeps/addDep/removeDep call throws via
    // `_requireRewireHooks` rather than dispatching against an
    // indeterminate closure-cell state.
    this._rewirePoisoned = null;
  }

  get inner() {
    return this.nodeId;
  }

  get cache() {
    return this.cacheValue;
  }

  _updateCache(value) {
    this.cacheValue = value;
  }

  async subscribe(cb) {
    const registry = this.registry;
    const self = this;
    const sink = (flat) => {
      const messages = [];
      for (let i = 0; i < flat.length; i += 2) {
        const code = flat[i];
        const payload = flat[i + 1];
        if (code === MSG_CODE_START) continue;
        if (code === MSG_CODE_DATA) {
          const value = registry.get(payload);
          self._updateCache(value);
          messages.push([DATA, value]);
        } else if (code === MSG_CODE_ERROR) {
          messages.push([ERROR, registry.get(payload)]);
        } else if (code === MSG_CODE_PAUSE || code === MSG_CODE_RESUME) {
          messages.push([tierForCode(code), payload]);
        } else if (code === MSG_CODE_INVALIDATE) {
          self._updateCache(undefined);
          messages.push([tierForCode(code)]);
        } else {
          messages.push([tierForCode(code)]);
        }
      }
      cb(messages);
    };
    const subIdx = await this.core.subscribeWithTsfn(this.nodeId, sink);
    return async () => {
      await this.core.unsubscribe(subIdx);
    };
  }

  async down(msgs) {
    const encoded = [];
    let invalidated = false;
    for (const msg of msgs) {
      const tier = msg[0];
      if (tier === DATA) {
        const h = this.core.allocExternalHandle();
        this.registry.set(h, msg[1]);
        encoded.push(MSG_CODE_DATA, h);
      } else if (tier === DIRTY) {
        encoded.push(MSG_CODE_DIRTY, 0);
      } else if (tier === INVALIDATE) {
        encoded.push(MSG_CODE_INVALIDATE, 0);
        invalidated = true;
      } else if (tier === PAUSE) {
        encoded.push(MSG_CODE_PAUSE, msg[1]);
      } else if (tier === RESUME) {
        encoded.push(MSG_CODE_RESUME, msg[1]);
      } else if (tier === COMPLETE) {
        encoded.push(MSG_CODE_COMPLETE, 0);
      } else if (tier === ERROR) {
        const h = this.core.allocExternalHandle();
        this.registry.set(h, msg[1]);
        encoded.push(MSG_CODE_ERROR, h);
      } else if (tier === TEARDOWN) {
        encoded.push(MSG_CODE_TEARDOWN, 0);
      } else {
        // Terminal else — an unmapped tier (foreign symbol, RESOLVED,
        // START, …) would otherwise be silently dropped from `encoded`,
        // turning a programmer error into an invisible no-op. Fail loud,
        // mirroring `symbolToCode`'s unknown-symbol throw.
        throw new Error(`[graphrefly/native] unmapped tier in down(): ${String(tier)}`);
      }
    }
    await this.core.batchEmitHandleMessages(this.nodeId, encoded);
    if (invalidated) {
      this._updateCache(undefined);
    }
  }

  async complete() {
    await this.core.complete(this.nodeId);
  }

  async error(value) {
    const h = this.core.allocExternalHandle();
    this.registry.set(h, value);
    await this.core.batchEmitHandleMessages(this.nodeId, [MSG_CODE_ERROR, h]);
  }

  async invalidate() {
    await this.core.invalidate(this.nodeId);
    this._updateCache(undefined);
  }

  async teardown() {
    await this.core.teardown(this.nodeId);
  }

  async pause(lockId) {
    await this.core.pause(this.nodeId, lockId);
  }

  async resume(lockId) {
    return this.core.resume(this.nodeId, lockId);
  }

  async allocLockId() {
    return this.core.allocLockId();
  }

  async setResubscribable(value) {
    await this.core.setResubscribable(this.nodeId, value);
  }

  async hasFiredOnce() {
    return this.core.hasFiredOnce(this.nodeId);
  }

  // -------------------------------------------------------------------
  // D263/D264 — `setDeps`/`addDep`/`removeDep` rewire trio. Available
  // only on nodes built via `impl.map`'s generic user-derived path
  // (`register_user_derived`); other operators keep their OperatorOp
  // dispatch and have no fn-rebind hook. Composed JS-side as
  // `rebind_user_derived(fnId, batchFn) + Core::set_deps(node, deps)`
  // per D264 P1 — no new `add_dep`/`remove_dep` napi on BenchCore.
  //
  // The user fn shape matches the parity `Impl` contract — multi-dep
  // batch: `(data: ReadonlyArray<ReadonlyArray<unknown>>) => ReadonlyArray<unknown>`.
  // -------------------------------------------------------------------

  _requireRewireHooks(method) {
    if (this._fnId === null || this._operators === null) {
      throw new Error(
        `[graphrefly/native] ${method}: node was not built via a user-derived ` +
          `path (impl.map). Only impl.map currently reroutes through ` +
          `register_user_derived; other operators keep their OperatorOp ` +
          `dispatch and have no fn-rebind hook. Per D264 P1 the rewire ` +
          `surface is confined to impl.map until a parity scenario forces ` +
          `widening to additional operators.`,
      );
    }
    // QA A2 (2026-05-21): a prior rewire rollback failed → node is in an
    // indeterminate state. Fail fast rather than dispatching against an
    // inconsistent closure.
    if (this._rewirePoisoned) {
      throw new Error(
        `[graphrefly/native] ${method}: node is poisoned from a prior ` +
          `rewire rollback failure: ${this._rewirePoisoned.message ?? this._rewirePoisoned}`,
      );
    }
  }

  // Rewire-atomicity discipline (matches pure-ts
  // `wave_protocol_rewire_MC`): setDeps/addDep/removeDep must look
  // atomic to subscribers — new fn + new dep shape land together,
  // OR neither lands if Core rejects (self-dep, cycle, terminal,
  // non-resub-terminal dep, mid-fire). Sequence:
  //   1. Rebind the JS closure cell FIRST so any handshake-triggered
  //      fire inside `Core::set_deps` (e.g. new dep delivers cached
  //      DATA via the subscribe-handshake) dispatches through the new
  //      fn.
  //   2. Call `Core::set_deps`. On Err — roll back the JS closure
  //      swap, then propagate.
  // The (briefly swapped, then rolled-back) window between (1) and a
  // failing (2) is safe: rewire happens outside any active fire (A6
  // reentrancy guard already rejects mid-fire set_deps), so no other
  // wave's dispatch can race the swap on the actor thread.
  async _swapFn(newFn) {
    const batchFn = makeUserDerivedAdapter(this.core, this.registry, newFn);
    await this._operators.rebindUserDerived(this._fnId, batchFn);
  }

  async setDeps(newDeps, fn) {
    this._requireRewireHooks('setDeps');
    const oldUserFn = this._currentUserFn;
    await this._swapFn(fn);
    this._currentUserFn = fn;
    try {
      await this.core.setDeps(this.nodeId, newDeps.map((n) => n.inner));
    } catch (e) {
      // Roll back the closure swap so the original fn still drives
      // dispatch (matches the parity contract: rejected setDeps leaves
      // both fn AND dep shape untouched). QA A2: rollback itself can
      // fail; `_rollbackSwapFn` stashes the rollback error on the node
      // so the next rewire call short-circuits via `_requireRewireHooks`.
      await this._rollbackSwapFn(oldUserFn);
      throw e;
    }
    this._deps = newDeps.slice();
  }

  async addDep(dep, fn) {
    this._requireRewireHooks('addDep');
    // QA D2 (2026-05-21): even on idempotent append (`dep` already in
    // `_deps`), still call `Core::set_deps` with the unchanged dep list
    // so Core re-runs its invariant gauntlet (self-dep, cycle, terminal
    // `this`, non-resubscribable terminal, mid-fire reentrancy). Pure-ts
    // re-validates each call; early-returning here was a parity hole.
    const existingIdx = this._deps.findIndex((d) => d.inner === dep.inner);
    const newDeps = existingIdx >= 0 ? this._deps.slice() : [...this._deps, dep];
    const oldUserFn = this._currentUserFn;
    await this._swapFn(fn);
    this._currentUserFn = fn;
    try {
      await this.core.setDeps(this.nodeId, newDeps.map((n) => n.inner));
    } catch (e) {
      await this._rollbackSwapFn(oldUserFn);
      throw e;
    }
    this._deps = newDeps;
    return existingIdx >= 0 ? existingIdx : newDeps.length - 1;
  }

  async removeDep(dep, fn) {
    this._requireRewireHooks('removeDep');
    const newDeps = this._deps.filter((d) => d.inner !== dep.inner);
    const oldUserFn = this._currentUserFn;
    await this._swapFn(fn);
    this._currentUserFn = fn;
    if (newDeps.length !== this._deps.length) {
      try {
        await this.core.setDeps(this.nodeId, newDeps.map((n) => n.inner));
      } catch (e) {
        await this._rollbackSwapFn(oldUserFn);
        throw e;
      }
      this._deps = newDeps;
    }
  }

  // QA A2 (2026-05-21): rollback path may itself fail (TSFN dispatch error,
  // actor unavailable, fn_id evicted). If rollback errors, the node is in an
  // indeterminate state — new fn potentially live with old deps. Surface
  // both errors via `AggregateError` and mark the node poisoned so a future
  // rewire fails fast instead of dispatching against an inconsistent closure.
  async _rollbackSwapFn(oldUserFn) {
    try {
      await this._swapFn(oldUserFn);
      this._currentUserFn = oldUserFn;
    } catch (rollbackErr) {
      this._rewirePoisoned = rollbackErr;
      // Re-throw within the outer catch's flow — outer catch already has
      // the original Core::set_deps error, so we stash this for surfacing.
      // The outer `throw e` will land first; subsequent rewires throw
      // poison.
    }
  }
}

// D263/D264 — turn a user fn of shape
//   `(data: ReadonlyArray<ReadonlyArray<unknown>>) => ReadonlyArray<unknown>`
// into the wire-shape Rust expects for `register_user_derived` /
// `rebind_user_derived`:
//   `(depsHandles: number[][]) => number[]`
// The closure dereferences each per-dep handle to a value, runs the
// user fn, then allocates JS-side handles (`allocExternalHandle` —
// retain=1) for each output. Core takes ownership of those shares as
// it stores them in the emission queue.
function makeUserDerivedAdapter(core, registry, userFn) {
  return (depsHandles) => {
    const data = new Array(depsHandles.length);
    for (let i = 0; i < depsHandles.length; i++) {
      const slot = depsHandles[i];
      const values = new Array(slot.length);
      for (let j = 0; j < slot.length; j++) {
        values[j] = registry.get(slot[j]);
      }
      data[i] = values;
    }
    const out = userFn(data) || [];
    const handles = new Array(out.length);
    for (let i = 0; i < out.length; i++) {
      const h = core.allocExternalHandle();
      registry.set(h, out[i]);
      handles[i] = h;
    }
    return handles;
  };
}

// ---------------------------------------------------------------------------
// NativeGraph — async Graph wrapper over BenchGraph.
// ---------------------------------------------------------------------------

class NativeGraph {
  constructor(bench, core, registry, nodesByName, operators) {
    this.bench = bench;
    this.core = core;
    this.registry = registry;
    this.nodesByName = nodesByName || new Map();
    // D267: reverse cache `nodeId -> name`. Lets `nameOf` return sync
    // when JS already knows the answer (the common case: the node was
    // created via this Graph's `state`/`add`). Preserves the R3.7.3
    // sync-observability of node names from inside a TEARDOWN sink
    // cross-arm — the substrate keeps the name in its namespace until
    // AFTER the cascade, and the JS-side cache mirrors that lifetime
    // (cleared in `remove`/`destroy` AFTER `bench.remove/destroy`
    // resolves, i.e. AFTER the cascade has settled). Cross-mount
    // resolution (`child::inner`) and nodes JS doesn't own fall
    // through to the async napi path — the Impl contract `T |
    // Promise<T>` lets tests `await` regardless.
    this.nodeIdToName = new Map();
    // D292 D.1: BenchOperators handle (needed for `derived(name, deps,
    // fn)` to reroute through `registerUserDerived`). Optional — child
    // graphs mounted via `mount(name)` inherit from the parent.
    this.operators = operators || null;
    // D292 D.1 + F3 (cross-cutting closure-cell registry teardown):
    // per-name map of arbitrary-fn derived nodes' closure cells. The
    // cell IS the node — eviction must fire on the same paths the
    // substrate tears the node down on. Today (D292): `remove(name)`,
    // `destroy()`, and `impl.close()` cascade. **R1 anti-pattern #5
    // watch:** if substrate teardown ever evolves outside these three
    // paths (e.g., a future GC-driven sweep, a `Core::trim_dead_nodes`
    // primitive), update eviction wiring HERE — silently leaving the
    // cell alive past the substrate node IS the foot-gun.
    this.closureCells = new Map();
    // Exposed for storage binding access (raw BenchGraph).
    this._bench = bench;
  }

  // D267 — `tryResolve`/`nameOf` may be sync (JS cache hit) or async
  // (cache miss / cross-mount path). The parity `Impl` contract is
  // widened to `T | Promise<T>` so the pure-ts arm keeps its sync
  // shape unchanged. Every parity scenario writes `await impl
  // .tryResolve(...)` regardless of arm; TS resolves non-promise
  // values immediately. The sync shape used to deadlock when a JS
  // sink callback (already blocking the actor worker via TSFN
  // Blocking) called these methods unconditionally — see
  // graph_bindings.rs "Namespace introspection" block. Returning
  // sync from the JS cache avoids re-entering the actor at all,
  // which both fixes the deadlock AND preserves the R3.7.3 invariant
  // ("name resolves during teardown cascade") cross-arm.
  tryResolve(path) {
    // Fast path: no cross-mount segment + JS cache hit.
    if (!path.includes('::')) {
      const cached = this.nodesByName.get(path);
      if (cached) return cached;
    }
    // Slow path (cross-mount or cache miss): await napi.
    return this.bench.tryResolve(path).then((id) => {
      if (id === 0) return undefined;
      const cached = !path.includes('::') ? this.nodesByName.get(path) : undefined;
      if (cached) return cached;
      return new NativeNode(this.core, id, this.registry);
    });
  }

  nameOf(node) {
    const id = node.inner;
    // Fast path: JS reverse cache hit.
    const cached = this.nodeIdToName.get(id);
    if (cached !== undefined) return cached;
    // Slow path: await napi (rare for tests; covers nodes JS doesn't own).
    return this.bench.nameOf(id).then((n) => n ?? undefined);
  }

  async state(name, initial) {
    let initialHandle = null;
    if (initial !== undefined) {
      initialHandle = this.core.allocExternalHandle();
      this.registry.set(initialHandle, initial);
    }
    const id = await this.bench.state(name, initialHandle);
    const node = new NativeNode(this.core, id, this.registry);
    if (initial !== undefined) {
      node._updateCache(initial);
    }
    this.nodesByName.set(name, node);
    this.nodeIdToName.set(id, name); // D267 reverse cache
    return node;
  }

  /**
   * D292 D.1 — arbitrary-fn derived node, async via D263's
   * `registerUserDerived` TSFN reroute + `bench.add`.
   *
   * Shape mirrors pure-ts's `Graph.derived(name, deps, fn)`:
   *   fn: (batches: ReadonlyArray<ReadonlyArray<unknown>>) => ReadonlyArray<unknown>
   * where `batches[i]` is the wave's accumulated DATA values for `deps[i]`.
   *
   * **2 napi crossings per call** (`registerUserDerived` + `bench.add`).
   * The fused single-crossing variant (D292 Option C) is deferred per
   * D196 consumer-pressure gate; non-breaking widening to add later.
   *
   * **CLOSURE-CELL LIFETIME (R1 anti-pattern #5 watch):** the user `fn`
   * is retained in `this.closureCells` keyed by `name`. The cell is
   * evicted ONLY when the substrate tears the node down, which today
   * happens via `graph.remove(name)` or `graph.destroy()` cascading
   * into `Core::teardown_node` (R2.6.4), AND on `impl.close()`'s
   * actor shutdown (F3 cross-cutting closure-cell registry teardown).
   * If substrate teardown ever evolves to fire outside those three
   * paths (e.g., a future GC-driven sweep, a `Core::trim_dead_nodes`
   * primitive, etc.), update the eviction wiring HERE — silently
   * leaving the closure cell alive past the substrate node IS the
   * foot-gun.
   *
   * **Trade-off vs OperatorOp paths:** arbitrary-fn derived nodes go
   * through `registerUserDerived` (TSFN-backed user closure) and lose
   * `OperatorOp::Map`/`Filter`/etc. optimizations. Same trade-off
   * `impl.map` already takes per D263; this method extends it to
   * `graph.derived(name, deps, fn)`.
   */
  async derived(name, deps, fn) {
    if (!this.operators) {
      throw new Error(
        '[graphrefly/native] graph.derived(name, deps, fn) requires an ' +
          'operators handle on this NativeGraph instance. Mounted child ' +
          'graphs inherit operators from the parent; if you see this on ' +
          'a parent Graph, the createNativeImpl() wiring is broken.',
      );
    }
    const adapter = makeUserDerivedAdapter(this.core, this.registry, fn);
    const [nodeId, fnId] = await this.operators.registerUserDerived(
      deps.map((d) => d.inner),
      adapter,
    );
    await this.bench.add(name, nodeId);
    const node = new NativeNode(this.core, nodeId, this.registry);
    node._operators = this.operators;
    node._fnId = fnId;
    node._deps = deps.slice();
    node._currentUserFn = fn;
    this.nodesByName.set(name, node);
    this.nodeIdToName.set(nodeId, name); // D267 reverse cache
    this.closureCells.set(name, { fnId, fn }); // D292 D.1 / F3
    return node;
  }

  async dynamic(name, deps, fn) {
    return this.derived(name, deps, fn);
  }

  async add(name, node) {
    const id = node.inner;
    // S6 QA fix 2026-05-20: bench.add() is now async (Promise<u32>) —
    // pre-S6 it was sync via `run_sync`, which deadlocked when an
    // active observe_all_reactive sink fired core.subscribe with a
    // TSFN-backed callback (libuv blocked on the actor reply). The
    // outer `async add` already returns a Promise; the await chains
    // the actor round-trip in.
    await this.bench.add(name, id);
    this.nodesByName.set(name, node);
    this.nodeIdToName.set(id, name); // D267 reverse cache
    return node;
  }

  async set(name, value) {
    const h = this.core.allocExternalHandle();
    this.registry.set(h, value);
    await this.bench.setByName(name, h);
    const n = this.nodesByName.get(name);
    if (n) n._updateCache(value);
  }

  async get(name) {
    const h = await this.bench.getByName(name);
    if (h === 0) return undefined;
    return this.registry.get(h);
  }

  async invalidate(name) {
    await this.bench.invalidateByName(name);
    const n = this.nodesByName.get(name);
    if (n) n._updateCache(undefined);
  }

  async complete(name) {
    await this.bench.completeByName(name);
  }

  async error(name, value) {
    const h = this.core.allocExternalHandle();
    this.registry.set(h, value);
    await this.bench.errorByName(name, h);
  }

  async remove(name) {
    // D267: capture id BEFORE bench.remove (which may invalidate the
    // node via TEARDOWN cascade) so the post-cascade JS cache clear
    // can also drop the reverse-map entry. Note: TEARDOWN sinks fire
    // INSIDE `bench.remove`'s wave — `nodeIdToName` still has the
    // entry at sink-fire time, preserving R3.7.3 sync nameOf
    // observability.
    const node = this.nodesByName.get(name);
    const audit = await this.bench.remove(name);
    this.nodesByName.delete(name);
    if (node) this.nodeIdToName.delete(node.inner);
    // D292 D.1 / F3: evict closure cell AFTER the substrate teardown
    // resolves. The TEARDOWN cascade has fully settled at this point
    // (mirrors `nodesByName` eviction timing — the sink callbacks above
    // could still observe the cell via `node._currentUserFn`, but the
    // cell is no longer load-bearing for any future fn fire).
    this.closureCells.delete(name);
    return audit;
  }

  async signal(messages) {
    const encoded = [];
    for (const msg of messages) {
      encoded.push(symbolToCode(msg[0]));
      encoded.push(typeof msg[1] === 'number' ? msg[1] : 0);
    }
    await this.bench.signalBatch(encoded);
  }

  async mount(name, child) {
    if (child) {
      throw new Error(
        '[graphrefly/native] graph.mount(name, child) with a pre-built child ' +
          'is not supported by the BenchGraph surface.',
      );
    }
    const childBench = await this.bench.mountNew(name);
    // D292 D.1: propagate operators handle to child graphs so
    // `child.derived(name, deps, fn)` reroutes through the same
    // `registerUserDerived` TSFN dispatch as the parent.
    return new NativeGraph(childBench, this.core, this.registry, undefined, this.operators);
  }

  async unmount(name) {
    return this.bench.unmount(name);
  }

  async destroy() {
    await this.bench.destroy();
    this.nodesByName.clear();
    this.nodeIdToName.clear(); // D267 reverse cache
    this.closureCells.clear(); // D292 D.1 / F3
  }

  async destroyAsync() {
    // BenchGraph.destroy() is already async-shaped; awaiting it is the
    // native-arm analogue of pure-ts's storage-disposer-awaiting variant.
    await this.bench.destroy();
    this.nodesByName.clear();
    this.nodeIdToName.clear(); // D267 reverse cache
  }

  // D267 — `edges`/`describe` are async on the native arm. See
  // `tryResolve`/`nameOf` above for the rationale.
  async edges(opts) {
    const flat = await this.bench.edges((opts && opts.recursive) ?? false);
    const result = [];
    for (let i = 0; i < flat.length; i += 2) {
      result.push([flat[i], flat[i + 1]]);
    }
    return result;
  }

  describe(opts) {
    if (opts && opts.reactive) {
      return this._describeReactive();
    }
    // Returns a Promise<unknown> on the native arm. Parity `Impl
    // .describe()` is widened to `T | Promise<T>`.
    return this.bench.describeJson().then((json) => JSON.parse(json));
  }

  async _describeReactive() {
    const sinks = new Set();
    let latest;
    const handle = await this.bench.describeReactive((json) => {
      const snapshot = JSON.parse(json);
      latest = snapshot;
      for (const sink of sinks) sink(snapshot);
    });
    return {
      subscribe: (sink) => {
        sinks.add(sink);
        if (latest !== undefined) sink(latest);
        return () => {
          sinks.delete(sink);
        };
      },
      dispose: async () => {
        sinks.clear();
        await handle.dispose();
      },
    };
  }

  async observe(path, opts) {
    const sinks = new Set();
    const registry = this.registry;
    const dispatchAll = (name, encoded) => {
      const decoded = decodeMessages(encoded, registry);
      for (const sink of sinks) sink(name, decoded);
    };
    let handle;
    if (opts && opts.reactive) {
      handle = await this.bench.observeAllReactive(dispatchAll);
    } else {
      handle = await this.bench.observeSubscribe(path ?? null, dispatchAll);
    }
    return {
      subscribe: (sink) => {
        sinks.add(sink);
        return () => {
          sinks.delete(sink);
        };
      },
      dispose: async () => {
        sinks.clear();
        await handle.dispose();
      },
    };
  }

  // D286 (cross-track-ledger §1 D283 native landing).
  //
  // R3.1.2 — tag the graph with factory provenance. `factoryArgs` is
  // JSON.stringify'd at the wrapper boundary because the napi shape
  // takes `Option<String>` (avoids a full JS→Rust value bridge for a
  // metadata-only field). `undefined` ⇒ `null` over the wire, which
  // the Rust side maps to `None` → clears stale args per the QA F8
  // invariant.
  async tagFactory(factory, factoryArgs) {
    const json = factoryArgs === undefined ? null : JSON.stringify(factoryArgs);
    await this.bench.tagFactory(factory, json);
  }

  // R3.6.3 — runtime profile. Returns a fresh snapshot per call (not
  // reactive). The napi side returns JSON-serialized
  // `GraphProfileResult` (D284-narrowed: no value-size fields); the
  // wrapper parses with JSON.parse to match the
  // `ImplGraphProfileResult` shape pure-ts callers expect.
  async resourceProfile(opts) {
    const topN = opts && typeof opts.topN === "number" ? opts.topN : null;
    const json = await this.bench.resourceProfile(topN);
    return JSON.parse(json);
  }
}

// ---------------------------------------------------------------------------
// N1 substrate-infra surface (5 symbols — D203 item 8 / D206-D207).
//
// `RingBuffer` / `ResettableTimer` / `sourceOpts` are thin pure-TS infra
// (NO Rust counterpart needed — types.ts: "thin TS over napi core, no
// Rust"). Hand-written here so the surface is self-contained.
// `describeNode` / `sha256Hex` route to the new napi fns
// (`BenchCore.describeNode` — D267 promoted to async to close the
// sink-callback deadlock class; D207 reuse of the describe projection;
// `BenchCore.sha256Hex` async at the boundary, sync hashing in
// graphrefly-core).
// ---------------------------------------------------------------------------

/** Fixed-capacity ring buffer — drop-oldest / FIFO eviction. */
class RingBuffer {
  constructor(capacity) {
    if (!Number.isInteger(capacity) || capacity <= 0) {
      throw new Error(`RingBuffer capacity must be a positive integer (got ${capacity})`);
    }
    this.capacity = capacity;
    this.buf = new Array(capacity);
    this.head = 0;
    this._size = 0;
  }

  get size() {
    return this._size;
  }

  get maxSize() {
    return this.capacity;
  }

  push(item) {
    const idx = (this.head + this._size) % this.capacity;
    this.buf[idx] = item;
    if (this._size < this.capacity) this._size += 1;
    else this.head = (this.head + 1) % this.capacity;
  }

  shift() {
    if (this._size === 0) return undefined;
    const item = this.buf[this.head];
    this.buf[this.head] = undefined;
    this.head = (this.head + 1) % this.capacity;
    this._size -= 1;
    return item;
  }

  at(i) {
    if (this._size === 0) return undefined;
    const n = i < 0 ? this._size + i : i;
    if (n < 0 || n >= this._size) return undefined;
    return this.buf[(this.head + n) % this.capacity];
  }

  toArray() {
    const result = new Array(this._size);
    for (let i = 0; i < this._size; i += 1) {
      result[i] = this.buf[(this.head + i) % this.capacity];
    }
    return result;
  }

  clear() {
    for (let i = 0; i < this._size; i += 1) {
      this.buf[(this.head + i) % this.capacity] = undefined;
    }
    this.head = 0;
    this._size = 0;
  }
}

/** Resettable deadline timer (spec §5.10 escape hatch). */
class ResettableTimer {
  constructor() {
    this._timer = undefined;
    this._gen = 0;
  }

  start(delayMs, callback) {
    this.cancel();
    this._gen += 1;
    const gen = this._gen;
    this._timer = setTimeout(() => {
      this._timer = undefined;
      if (gen !== this._gen) return;
      callback();
    }, delayMs);
  }

  cancel() {
    if (this._timer !== undefined) {
      clearTimeout(this._timer);
      this._timer = undefined;
    }
  }

  get pending() {
    return this._timer !== undefined;
  }
}

/** Producer-shaped source NodeOptions defaulter (thin TS infra). */
function sourceOpts(opts) {
  return { describeKind: 'producer', ...(opts || {}) };
}

// ---------------------------------------------------------------------------
// createNativeImpl() — the async public surface factory.
//
// Each call constructs a fresh BenchCore + JSValueRegistry + operators
// handle and returns an `Impl`-shaped object. Direct consumers call this
// once; the parity harness calls it per test (with disposal).
//
// D292 D.3 Item 5: options bag with `autoCloseOnBeforeExit` opt-in.
// See README "Closing a NativeImpl" → "When to opt in to
// `autoCloseOnBeforeExit`" for the 3-condition rubric (R5 refinement).
// ---------------------------------------------------------------------------

// D292 D.3 Item 1 — FinalizationRegistry-driven off-libuv async shutdown.
//
// When a `NativeImpl` is GC'd without an explicit `await impl.close()`,
// Node fires this finalizer ON THE LIBUV THREAD. If we called the
// underlying `BenchCore::close()` here (async napi → blocks libuv until
// the actor reply lands), we'd block the JS event loop on `handle.join()`
// — visually indistinguishable from the "process never exits" hang D293
// closed (the D292 WATCH (i) hazard).
//
// `BenchCore::finalize_async()` (new in D292) is a SYNC napi method that
// posts the shutdown work to tokio's blocking pool via
// `napi::bindgen_prelude::spawn_blocking`. The libuv thread returns
// immediately; the join completes off-thread on the tokio runtime.
//
// **Why a module-level singleton:** FinalizationRegistry instances are
// cheap, but sharing one across all `createNativeImpl()` calls means
// the GC only walks one registry, not N.
//
// **Why we don't `register` Graph/Subscription:** D.3 Item 3 (nested
// `Symbol.asyncDispose`) is deferred per D196 — same logic applies to
// nested finalizers. The top-level NativeImpl finalizer cascades into
// all sub-surfaces via the actor shutdown.
//
// **Held value:** the BenchCore napi handle (`state.core`). It survives
// past the JS `impl` object because the finalizer holds it. The
// FinalizationRegistry doesn't keep the GC target alive — it only
// holds the heldValue across the finalization boundary.
const _coreFinalizer = new FinalizationRegistry((coreHandle) => {
  // Best-effort: a coreHandle that's already shut down (explicit
  // close() ran before GC) treats this as a no-op via the
  // `actor.shutdown()` idempotency flag. No error to handle —
  // finalize_async is sync void.
  try {
    coreHandle.finalizeAsync();
  } catch (e) {
    // The handle should never throw — `finalize_async` is `pub fn`
    // returning unit. If it does (napi binding mismatch, dropped
    // .node, etc.), surface via console.error per F2's
    // unhandled-cleanup-path convention.
    console.error('[graphrefly/native] FinalizationRegistry cleanup failed:', e);
  }
});

function createNativeImpl(opts) {
  opts = opts || {};
  if (!nativeBinding) {
    throw new Error(
      `[graphrefly/native] napi binding not loaded — build it with ` +
        `\`pnpm --filter @graphrefly/native build\`. Cause: ${nativeLoadError}`,
    );
  }
  const native = nativeBinding;

  const core = new native.BenchCore();
  const operators = native.BenchOperators.fromCore(core);
  const registry = new JSValueRegistry();
  core.setReleaseCallback((handle) => {
    registry.delete(handle);
  });
  const state = { core, operators, registry };

  const unwrap = (n) => n.inner;
  const wrapNode = (id) => new NativeNode(state.core, id, state.registry);

  const makeProjector = (fn) => (h) => {
    const result = fn(state.registry.get(h));
    const out = state.core.allocExternalHandle();
    state.registry.set(out, result);
    return out;
  };
  const makePredicate = (predicate) => (h) => predicate(state.registry.get(h));
  const makeFolder = (fn) => (accH, valueH) => {
    const result = fn(state.registry.get(accH), state.registry.get(valueH));
    const out = state.core.allocExternalHandle();
    state.registry.set(out, result);
    return out;
  };
  const makeEquals = (equals) => (a, b) =>
    equals(state.registry.get(a), state.registry.get(b));
  const makeStratifyClassifier = (classifier) => (rulesH, valueH) => {
    try {
      return classifier(state.registry.get(rulesH), state.registry.get(valueH));
    } catch {
      return false;
    }
  };
  const makePackerArray = () => (handles) => {
    const values = handles.map((h) => state.registry.get(h));
    const out = state.core.allocExternalHandle();
    state.registry.set(out, values);
    return out;
  };
  const makePairwise = () => (prev, current) => {
    const out = state.core.allocExternalHandle();
    state.registry.set(out, [state.registry.get(prev), state.registry.get(current)]);
    return out;
  };

  const storage = buildStorage(native, state, wrapNode);
  const structures = buildStructures(native, state, wrapNode);

  const impl = {
    name: 'rust-via-napi',

    DATA,
    RESOLVED,
    DIRTY,
    INVALIDATE,
    PAUSE,
    RESUME,
    COMPLETE,
    ERROR,
    TEARDOWN,

    async node(_deps, opts) {
      let id;
      if (opts && opts.initial !== undefined) {
        const handle = state.core.allocExternalHandle();
        state.registry.set(handle, opts.initial);
        id = await state.core.registerStateWithHandle(handle);
      } else {
        id = await state.core.registerStateSentinel();
      }
      if (opts && opts.resubscribable) {
        await state.core.setResubscribable(id, true);
      }
      const node = new NativeNode(state.core, id, state.registry);
      if (opts && opts.initial !== undefined) {
        node._updateCache(opts.initial);
      }
      return node;
    },

    Graph: class extends NativeGraph {
      constructor(name) {
        const bench = native.BenchGraph.fromCore(state.core, name);
        // D292 D.1: pass operators handle so `graph.derived(name, deps, fn)`
        // can reroute through `registerUserDerived` (TSFN-backed user
        // closures). Without this the derived() method throws.
        super(bench, state.core, state.registry, undefined, state.operators);
      }
    },

    // D282 / D288 Path D / D289 / D290 — sync-handle batch with per-frame ctx.
    //
    // Opens a `BenchBatchContext` via `BenchCore::open_batch` (D289 napi);
    // builds a JS-side `ctx` that dispatches `ctx.down(node, msg)` by tier
    // into the corresponding `BenchBatchContext.down_*` method, reusing
    // the existing handle-encoding helpers (`allocExternalHandle` +
    // `registry.set`) so no encoding-logic dual-source-of-truth with
    // `NativeNode.down`. On normal return: `benchCtx.commit()` flushes
    // R4.3.5 drain + `fire_deferred`. On throw: `benchCtx.rollback()`
    // runs `discard_wave_cleanup` + `restore_wave_cache_snapshots`
    // (R4.3.2), then re-throws.
    //
    // **Per-frame lifetime (D288 Q3 / D290 QA F2 lock).** `closed` flips
    // in an OUTER `finally` so it fires AFTER substrate cleanup
    // (commit/rollback) regardless of throw path — mirrors pure-ts
    // adapter's `try { legacy.batch(...) } finally { closed = true; }`
    // shape. Every ctx method checks `closed` FIRST so a stashed-and-
    // fired-late `ctx.down(...)` throws cleanly even if `benchCtx.commit()`
    // itself threw. Pinned by Case 15a/15b `post-frame-ctx-throws`
    // regressions.
    async batch(fn) {
      const benchCtx = state.core.openBatch();
      let closed = false;
      const ctx = {
        down(node, msg) {
          if (closed) throw new Error('BatchCtx used after batch frame closed');
          const nodeId = node.inner;
          const tier = msg[0];
          if (tier === DATA) {
            const h = state.core.allocExternalHandle();
            state.registry.set(h, msg[1]);
            benchCtx.downHandle(nodeId, h);
          } else if (tier === COMPLETE) {
            benchCtx.downComplete(nodeId);
          } else if (tier === ERROR) {
            const payload = msg[1];
            if (typeof payload === 'number' && Number.isInteger(payload)) {
              benchCtx.downErrorInt(nodeId, payload);
            } else if (typeof payload === 'string') {
              benchCtx.downErrorStr(nodeId, payload);
            } else {
              throw new Error(
                `BatchCtx.down: ERROR payload must be i32 or string on native arm; ` +
                  `got ${typeof payload} (D289 BenchBatchContext exposes downErrorInt / ` +
                  `downErrorStr only — no downErrorHandle. Widen if a parity scenario needs it.)`,
              );
            }
          } else if (tier === INVALIDATE) {
            // QA F10 (D290): do NOT eager-update the JS-side cache
            // mirror here. Inside batch, substrate buffers the INVALIDATE
            // op until commit; on commit, the subscribe sink's TSFN
            // callback fires the `_updateCache(undefined)` on its own
            // (`wrapper.js:235-237`). On rollback, the substrate discards
            // the op; no sink fires; the JS-side cacheValue must stay at
            // the pre-batch value. Eagerly clearing it here broke rollback
            // cache restoration even for subscribed nodes.
            benchCtx.downInvalidate(nodeId);
          } else if (tier === TEARDOWN) {
            benchCtx.downTeardown(nodeId);
          } else if (tier === PAUSE) {
            benchCtx.downPause(nodeId, msg[1]);
          } else if (tier === RESUME) {
            benchCtx.downResume(nodeId, msg[1]);
          } else if (tier === DIRTY || tier === RESOLVED) {
            // Substrate-internal — DIRTY is queue plumbing, RESOLVED is
            // what derived fns emit via FnResult; neither has a public
            // emit API on BenchBatchContext (per D289 / index.d.ts).
            throw new Error(
              `BatchCtx.down: tier ${String(tier)} is substrate-internal on native arm; ` +
                `no BenchBatchContext.down_* method exists (wrapper.js batch() ctx.down).`,
            );
          } else {
            throw new Error(
              `BatchCtx.down: unmapped tier ${String(tier)} ` +
                `(wrapper.js batch() ctx.down — add a branch here if a new spec tier surfaces).`,
            );
          }
        },
      };
      // QA F2 (D290): close-flag flips in outer `finally` so it fires
      // AFTER substrate cleanup (commit/rollback) regardless of throw
      // path. Mirrors pure-ts adapter shape; commit-throw (D289 tripwire)
      // and rollback-throw both still set `closed=true` for stashed-ctx
      // safety.
      try {
        try {
          fn(ctx);
        } catch (e) {
          benchCtx.rollback();
          throw e;
        }
        benchCtx.commit();
      } finally {
        closed = true;
      }
    },

    // Transform.
    // D263/D264 — `impl.map` reroutes through the generic
    // `register_user_derived` path so the resulting node carries the
    // rewire hooks (`setDeps`/`addDep`/`removeDep`). Other operators
    // (filter/scan/combine/etc.) keep their OperatorOp paths — they
    // have no parity-scenario forcing function for setDeps yet (D264
    // P1: scope confined to map).
    async map(src, fn) {
      // The user's `(x: T) => U` is adapted to the batch shape that
      // `register_user_derived` expects: deps[0] is the wave's DATA
      // handles from the (single) source dep; we emit one output per
      // handle.
      const initialUserFn = (data) => {
        const out = [];
        const slot = data[0] || [];
        for (const v of slot) out.push(fn(v));
        return out;
      };
      const batchFn = makeUserDerivedAdapter(state.core, state.registry, initialUserFn);
      const [nodeId, fnId] = await state.operators.registerUserDerived(
        [unwrap(src)],
        batchFn,
      );
      const node = wrapNode(nodeId);
      node._operators = state.operators;
      node._fnId = fnId;
      node._deps = [src];
      node._currentUserFn = initialUserFn;
      return node;
    },
    async filter(src, predicate) {
      return wrapNode(
        await state.operators.registerFilter(unwrap(src), makePredicate(predicate)),
      );
    },
    async scan(src, fn, seed) {
      const seedH = state.core.allocExternalHandle();
      state.registry.set(seedH, seed);
      return wrapNode(
        await state.operators.registerScan(unwrap(src), seedH, makeFolder(fn)),
      );
    },
    async reduce(src, fn, seed) {
      const seedH = state.core.allocExternalHandle();
      state.registry.set(seedH, seed);
      return wrapNode(
        await state.operators.registerReduce(unwrap(src), seedH, makeFolder(fn)),
      );
    },
    async distinctUntilChanged(src, equals) {
      const id = equals
        ? await state.operators.registerDistinctUntilChangedWith(
            unwrap(src),
            makeEquals(equals),
          )
        : await state.operators.registerDistinctUntilChanged(unwrap(src));
      return wrapNode(id);
    },
    async pairwise(src) {
      return wrapNode(
        await state.operators.registerPairwise(unwrap(src), makePairwise()),
      );
    },

    // Combine.
    async combine(srcs) {
      return wrapNode(
        await state.operators.registerCombine(srcs.map(unwrap), makePackerArray()),
      );
    },
    async withLatestFrom(primary, secondary) {
      return wrapNode(
        await state.operators.registerWithLatestFrom(
          unwrap(primary),
          unwrap(secondary),
          makePackerArray(),
        ),
      );
    },
    async merge(srcs) {
      return wrapNode(await state.operators.registerMerge(srcs.map(unwrap)));
    },

    // Flow.
    async take(src, count) {
      return wrapNode(await state.operators.registerTake(unwrap(src), count));
    },
    async skip(src, count) {
      return wrapNode(await state.operators.registerSkip(unwrap(src), count));
    },
    async takeWhile(src, predicate) {
      return wrapNode(
        await state.operators.registerTakeWhile(unwrap(src), makePredicate(predicate)),
      );
    },
    async last(src, opts) {
      let id;
      if (opts && opts.defaultValue !== undefined) {
        const dh = state.core.allocExternalHandle();
        state.registry.set(dh, opts.defaultValue);
        id = await state.operators.registerLastWithDefault(unwrap(src), dh);
      } else {
        id = await state.operators.registerLast(unwrap(src));
      }
      return wrapNode(id);
    },
    async first(src) {
      return wrapNode(await state.operators.registerFirst(unwrap(src)));
    },
    async find(src, predicate) {
      return wrapNode(
        await state.operators.registerFind(unwrap(src), makePredicate(predicate)),
      );
    },
    async elementAt(src, index) {
      return wrapNode(await state.operators.registerElementAt(unwrap(src), index));
    },

    // Subscription-managed combinators.
    async zip(srcs) {
      return wrapNode(
        await state.operators.registerZip(srcs.map(unwrap), makePackerArray()),
      );
    },
    async concat(first, second) {
      return wrapNode(
        await state.operators.registerConcat(unwrap(first), unwrap(second)),
      );
    },
    async race(srcs) {
      return wrapNode(await state.operators.registerRace(srcs.map(unwrap)));
    },
    async takeUntil(src, notifier) {
      return wrapNode(
        await state.operators.registerTakeUntil(unwrap(src), unwrap(notifier)),
      );
    },

    // Higher-order.
    async switchMap(outer, project) {
      const projector = (h) => unwrap(project(state.registry.get(h)));
      return wrapNode(await state.operators.registerSwitchMap(unwrap(outer), projector));
    },
    async exhaustMap(outer, project) {
      const projector = (h) => unwrap(project(state.registry.get(h)));
      return wrapNode(
        await state.operators.registerExhaustMap(unwrap(outer), projector),
      );
    },
    async concatMap(outer, project) {
      const projector = (h) => unwrap(project(state.registry.get(h)));
      return wrapNode(
        await state.operators.registerConcatMap(unwrap(outer), projector),
      );
    },
    async mergeMap(outer, project, concurrency) {
      const projector = (h) => unwrap(project(state.registry.get(h)));
      return wrapNode(
        await state.operators.registerMergeMap(
          unwrap(outer),
          projector,
          concurrency ?? null,
        ),
      );
    },

    // Control operators.
    async tap(src, fn) {
      const callback = (h) => fn(state.registry.get(h));
      return wrapNode(await state.operators.registerTap(unwrap(src), callback));
    },
    async tapObserver(src, opts) {
      const dataCb = opts.data ? (h) => opts.data(state.registry.get(h)) : undefined;
      const errorCb = opts.error ? (h) => opts.error(state.registry.get(h)) : undefined;
      const completeCb = opts.complete ?? undefined;
      return wrapNode(
        await state.operators.registerTapObserver(
          unwrap(src),
          dataCb ?? null,
          errorCb ?? null,
          completeCb ?? null,
        ),
      );
    },
    async onFirstData(src, fn) {
      const callback = (h) => fn(state.registry.get(h));
      return wrapNode(await state.operators.registerOnFirstData(unwrap(src), callback));
    },
    async rescue(src, fn) {
      const callback = (errorHandle) => {
        const result = fn(state.registry.get(errorHandle));
        if (result === undefined) return -1;
        const outH = state.core.allocExternalHandle();
        state.registry.set(outH, result);
        return outH;
      };
      return wrapNode(await state.operators.registerRescue(unwrap(src), callback));
    },
    async valve(src, control, gate) {
      const gateCb = (h) => gate(state.registry.get(h));
      return wrapNode(
        await state.operators.registerValve(unwrap(src), unwrap(control), gateCb),
      );
    },
    async settle(src, quietWaves, maxWaves) {
      return wrapNode(
        await state.operators.registerSettle(unwrap(src), quietWaves, maxWaves ?? null),
      );
    },
    async repeat(src, count) {
      return wrapNode(await state.operators.registerRepeat(unwrap(src), count));
    },

    // Buffer operators.
    async buffer(src, notifier) {
      const packer = makePackerArray();
      return wrapNode(
        await state.operators.registerBuffer(unwrap(src), unwrap(notifier), packer),
      );
    },
    async bufferCount(src, count) {
      const packer = makePackerArray();
      return wrapNode(
        await state.operators.registerBufferCount(unwrap(src), count, packer),
      );
    },

    // Cold sources.
    async fromIter(values) {
      const handles = values.map((v) => {
        const h = state.core.allocExternalHandle();
        state.registry.set(h, v);
        return h;
      });
      return wrapNode(await state.operators.registerFromIter(handles));
    },
    async of(values) {
      const handles = values.map((v) => {
        const h = state.core.allocExternalHandle();
        state.registry.set(h, v);
        return h;
      });
      return wrapNode(await state.operators.registerOf(handles));
    },
    async empty() {
      return wrapNode(await state.operators.registerEmpty());
    },
    async throwError(error) {
      const h = state.core.allocExternalHandle();
      state.registry.set(h, error);
      return wrapNode(await state.operators.registerThrowError(h));
    },

    // Stratify substrate.
    async stratifyBranch(src, rules, classifier) {
      return wrapNode(
        await state.operators.registerStratifyBranch(
          unwrap(src),
          unwrap(rules),
          makeStratifyClassifier(classifier),
        ),
      );
    },

    storage,
    structures,

    // ── N1 substrate-infra surface (D203 item 8 / D206-D207) ──────────
    RingBuffer,
    ResettableTimer,
    async describeNode(node) {
      const json = await state.core.describeNode(node.inner);
      return JSON.parse(json);
    },
    async sha256Hex(input) {
      const bytes = typeof input === 'string' ? new TextEncoder().encode(input) : input;
      return state.core.sha256Hex(bytes);
    },
    sourceOpts,
  };

  // D293 (2026-05-25): public end-of-life surface for ALL napi
  // consumers — closes the "process never exits" hang for test
  // frameworks (vitest/jest/mocha), CLI scripts, serverless cold-start,
  // and AWS Lambda. After `await impl.close()` returns, the Rust
  // worker thread has exited and the Node process is free to exit
  // naturally. Subsequent method calls on this `impl` (or any sub-
  // surface — Graph, Subscription, etc.) reject with a clear
  // "worker thread dropped" Error. See README "Closing a NativeImpl".
  //
  // Modern (Node 22+): `await using impl = createNativeImpl();`
  // auto-calls close() at block exit via Symbol.asyncDispose.
  //
  // D292 D.3 Item 2 + F2 (cross-cutting panic-propagation contract):
  // reject on actor errors. Pre-D292 a `.catch(() => {})` swallowed
  // actor errors for backward-compat with the parity harness's prior
  // `_dispose` afterEach shape; with F4's `_dispose` removal, the
  // swallow has no justification. Users who want silent cleanup wrap
  // in `try { await impl.close(); } catch { /* swallow */ }`. Every
  // napi method that posts to the actor MUST surface actor panics as
  // rejected Promises with a payload-string error message (F2);
  // `wrapper.js` MUST NOT `.catch(() => {})` any actor error.
  //
  // D292 D.3 Item 4 (close-waits / drain): close awaits the actor
  // shutdown which internally takes the join lock. In-flight async
  // ops (commit/rollback, async describes, etc.) resolve before the
  // worker thread exits because the actor's op queue is FIFO — any
  // op posted before shutdown is processed before the worker breaks
  // out of its loop. **R4 caveat:** a stuck user closure inside an
  // in-flight op blocks the drain; wrap in `Promise.race([impl.close(),
  // timeoutMs])` if bounded close time matters.
  //
  // D292 D.3 Item 1: also unregister from the FinalizationRegistry
  // so a GC after explicit close doesn't trigger a second shutdown
  // attempt (which would be an idempotent no-op via actor.shutdown's
  // flag swap, but cleaner to unregister than rely on idempotency).
  impl.close = async () => {
    _coreFinalizer.unregister(impl);
    await state.core.close();
  };
  // Symbol.asyncDispose wiring (Node 22+; older Node silently
  // ignores Symbol-keyed properties on a plain object).
  impl[Symbol.asyncDispose] = impl.close;
  // D292 F4 (value #1, pre-1.0): `_dispose` parity-only alias dropped.
  // The parity harness now calls `close()` directly (see
  // `~/src/graphrefly-ts/packages/parity-tests/impls/rust.ts` afterEach).

  // D292 D.3 Item 1 — register with the FinalizationRegistry so a GC
  // of `impl` without explicit close() routes shutdown off-libuv
  // via `BenchCore::finalize_async`. The held value is the raw
  // `state.core` napi handle — it survives the JS impl's GC because
  // the finalizer captures it. The `unregisterToken` is `impl` itself
  // so an explicit close() can unregister (avoid double-fire).
  _coreFinalizer.register(impl, state.core, impl);

  // D292 D.3 Item 5: `autoCloseOnBeforeExit` opt-in safety net. Default
  // is OFF (respects user agency; pre-1.0 semantic stays unlocked;
  // opt-in is non-breaking to widen later if consumer pressure
  // surfaces). README's "Closing a NativeImpl" section documents the
  // 3-condition rubric (R5 refinement) for when to enable this.
  //
  // Detach the handler on explicit `close()` so `await impl.close()`
  // followed by process exit doesn't double-fire (idempotent shutdown
  // makes the double-fire harmless, but the listener detach is cleaner
  // — keeps the process listener-count audit clean for callers that
  // monitor process events).
  if (opts.autoCloseOnBeforeExit) {
    const userClose = impl.close;
    const handler = () => {
      // `beforeExit` is sync; the close Promise drains async on the
      // tokio runtime. Node holds the process open until in-flight
      // microtasks complete, so this Promise IS awaited even though
      // we don't return it from the handler.
      userClose().catch((e) => {
        // R5/F2 carve-out: beforeExit is a safety-net path, NOT a
        // user-initiated close. A rejection here MUST surface
        // (console.error) — silent swallow would defeat F2's whole
        // point — but it cannot reject the Promise contract because
        // beforeExit handlers don't have one. console.error is the
        // canonical surfacing for unhandled cleanup-path errors.
        console.error('[graphrefly/native] close() during beforeExit failed:', e);
      });
    };
    process.on('beforeExit', handler);
    impl.close = async () => {
      process.removeListener('beforeExit', handler);
      await userClose();
    };
    // Refresh the Symbol.asyncDispose binding so `await using` runs
    // the handler-detaching close, not the raw one.
    impl[Symbol.asyncDispose] = impl.close;
  }

  return impl;
}

// ---------------------------------------------------------------------------
// Storage sub-surface (M4.F).
// ---------------------------------------------------------------------------

function buildStorage(native, state) {
  const n = native;
  if (typeof n.BenchMemoryBackend !== 'function') return undefined;

  return {
    memoryBackend() {
      const b = new n.BenchMemoryBackend();
      return {
        _raw: b,
        readRaw(key) {
          return b.readRaw(key) ?? undefined;
        },
        list(prefix) {
          return b.list(prefix);
        },
      };
    },

    snapshotTier(backend, opts) {
      const b = backend._raw;
      const tier = n.BenchValueSnapshotTier.create(
        b,
        (opts && opts.name) ?? null,
        (opts && opts.compactEvery) ?? null,
        (opts && opts.debounceMs) ?? null,
      );
      return {
        get name() {
          return tier.name;
        },
        get debounceMs() {
          return tier.debounceMs ?? undefined;
        },
        get compactEvery() {
          return tier.compactEvery ?? undefined;
        },
        save(value) {
          tier.save(JSON.stringify(value));
        },
        load() {
          const json = tier.load();
          return json != null ? JSON.parse(json) : undefined;
        },
        flush() {
          tier.flush();
        },
        rollback() {
          tier.rollback();
        },
      };
    },

    kvTier(backend, opts) {
      const b = backend._raw;
      const tier = n.BenchValueKvTier.create(
        b,
        (opts && opts.name) ?? null,
        (opts && opts.compactEvery) ?? null,
        (opts && opts.debounceMs) ?? null,
      );
      return {
        get name() {
          return tier.name;
        },
        save(key, value) {
          tier.save(key, JSON.stringify(value));
        },
        load(key) {
          const json = tier.load(key);
          return json != null ? JSON.parse(json) : undefined;
        },
        delete(key) {
          tier.delete(key);
        },
        list(prefix) {
          return tier.list(prefix);
        },
        flush() {
          tier.flush();
        },
        rollback() {
          tier.rollback();
        },
      };
    },

    appendLogTier(backend, opts) {
      const b = backend._raw;
      const tier = n.BenchValueAppendLogTier.create(
        b,
        (opts && opts.name) ?? null,
        (opts && opts.compactEvery) ?? null,
        (opts && opts.debounceMs) ?? null,
        (opts && opts.mode) ?? null, // D269: 'append' (default) | 'overwrite'
      );
      return {
        get name() {
          return tier.name;
        },
        get mode() {
          // D269 — exposes the configured mode; delta-shipping consumers
          // (e.g. reactiveLog.attachStorage) MUST reject overwrite tiers.
          return tier.mode;
        },
        appendEntries(entries) {
          tier.appendEntries(JSON.stringify(entries));
        },
        // D269: back-compat shape — `loadEntries(keyFilter)` returns a
        // bare entries array. For windowed-cursor pagination, use
        // `loadEntriesPaged({ keyFilter, cursor, pageSize })`.
        async loadEntries(keyFilter) {
          return JSON.parse(tier.loadEntries(keyFilter ?? null, null, null));
        },
        async loadEntriesPaged(loadOpts) {
          const json = tier.loadEntries(
            (loadOpts && loadOpts.keyFilter) ?? null,
            (loadOpts && loadOpts.cursor && loadOpts.cursor.position) ?? null,
            (loadOpts && loadOpts.pageSize) ?? null,
          );
          const parsed = JSON.parse(json);
          // Paginated response is `{ entries, cursor }`.
          return {
            entries: parsed.entries,
            cursor: parsed.cursor
              ? { position: parsed.cursor.position, __brand: 'AppendCursor' }
              : undefined,
          };
        },
        flush() {
          tier.flush();
        },
        rollback() {
          tier.rollback();
        },
      };
    },

    checkpointSnapshotTier(backend, opts) {
      const b = backend._raw;
      const tier = n.BenchCheckpointSnapshotTier.create(
        b,
        (opts && opts.name) ?? null,
        (opts && opts.compactEvery) ?? null,
      );
      return {
        _rawTier: tier,
        get name() {
          return tier.name;
        },
        save(record) {
          tier.save(JSON.stringify(record));
        },
        load() {
          const json = tier.load();
          return json != null ? JSON.parse(json) : undefined;
        },
        flush() {
          tier.flush();
        },
        rollback() {
          tier.rollback();
        },
      };
    },

    walKvTier(backend, opts) {
      const b = backend._raw;
      const tier = n.BenchWalKvTier.create(
        b,
        (opts && opts.name) ?? null,
        (opts && opts.compactEvery) ?? null,
      );
      return {
        _rawTier: tier,
        get name() {
          return tier.name;
        },
        save(key, frame) {
          tier.save(key, JSON.stringify(frame));
        },
        load(key) {
          const json = tier.load(key);
          return json != null ? JSON.parse(json) : undefined;
        },
        delete(key) {
          tier.delete(key);
        },
        list(prefix) {
          return tier.list(prefix);
        },
        flush() {
          tier.flush();
        },
        rollback() {
          tier.rollback();
        },
      };
    },

    async attachSnapshotStorage(graph, snapshot, wal) {
      const bench = graph._bench;
      if (!bench) throw new Error('storage: could not access raw BenchGraph');
      const snapTier = snapshot._rawTier;
      const walTier = wal ? wal._rawTier : undefined;
      const handle = await n.benchAttachSnapshotStorage(bench, snapTier, walTier ?? null);
      return {
        async dispose() {
          await handle.dispose();
        },
      };
    },

    async restoreSnapshot(graph, snapshot, wal, opts) {
      const bench = graph._bench;
      if (!bench) throw new Error('storage: could not access raw BenchGraph');
      const json = await n.benchRestoreSnapshot(
        bench,
        snapshot._rawTier,
        wal._rawTier,
        (opts && opts.targetSeq) ?? null,
      );
      const result = JSON.parse(json);
      return {
        replayedFrames: result.replayed_frames,
        skippedFrames: result.skipped_frames,
        finalSeq: result.final_seq,
        phases: result.phases.map((p) => ({ lifecycle: p.lifecycle, frames: p.frames })),
      };
    },

    async graphSnapshot(graph) {
      const bench = graph._bench;
      if (!bench) throw new Error('storage: could not access raw BenchGraph');
      return JSON.parse(await n.benchGraphSnapshot(bench));
    },

    walFrameKey(prefix, frameSeq) {
      return n.benchWalFrameKey(prefix, frameSeq);
    },

    async walFrameChecksum(frame) {
      return n.benchWalFrameChecksum(JSON.stringify(frame));
    },

    async verifyWalFrameChecksum(frame) {
      return n.benchVerifyWalFrameChecksum(JSON.stringify(frame));
    },

    walReplayOrder() {
      return n.benchReplayOrder();
    },
  };
}

// ---------------------------------------------------------------------------
// Reactive structures sub-surface (M5).
// ---------------------------------------------------------------------------

function buildStructures(native, state, wrapNode) {
  const n = native;
  if (typeof n.BenchReactiveLog?.create !== 'function') return undefined;

  return {
    reactiveLog(opts) {
      const packer = (handles) => {
        const values = handles.map((h) => state.registry.get(h));
        const out = state.core.allocExternalHandle();
        state.registry.set(out, values);
        return out;
      };
      const log = n.BenchReactiveLog.create(
        state.core,
        packer,
        opts && opts.maxSize != null ? opts.maxSize : null,
      );
      const node = wrapNode(log.nodeId);
      const keepAlive = [];
      return {
        node,
        get size() {
          return log.size;
        },
        async append(value) {
          const h = state.core.allocExternalHandle();
          state.registry.set(h, value);
          await log.append(h);
        },
        async appendMany(values) {
          const handles = values.map((v) => {
            const h = state.core.allocExternalHandle();
            state.registry.set(h, v);
            return h;
          });
          await log.appendMany(handles);
        },
        async clear() {
          await log.clear();
        },
        async trimHead(nh) {
          await log.trimHead(nh);
        },
        at(index) {
          const h = log.at(index);
          if (h === 0) return undefined;
          return state.registry.get(h);
        },
        async view(spec) {
          let v;
          if (spec.kind === 'tail') {
            v = await log.viewTail(packer, spec.n);
          } else if (spec.kind === 'slice') {
            v = await log.viewSlice(packer, spec.start, spec.stop ?? null);
          } else {
            const cursorNodeId = spec.cursor.inner;
            const readCursor = (handles) => {
              const cv = state.registry.get(handles[0]);
              if (typeof cv !== 'number' || !Number.isInteger(cv) || cv < 0) {
                throw new Error(
                  `view(fromCursor): cursor must resolve to a non-negative integer position, got ${String(cv)}`,
                );
              }
              return cv;
            };
            v = await log.viewFromCursor(packer, cursorNodeId, readCursor);
          }
          keepAlive.push(v);
          return wrapNode(v.nodeId);
        },
        async scan(initial, step) {
          const seedH = state.core.allocExternalHandle();
          state.registry.set(seedH, initial);
          const folder = (handles) => {
            const next = step(state.registry.get(handles[0]), state.registry.get(handles[1]));
            const outH = state.core.allocExternalHandle();
            state.registry.set(outH, next);
            return outH;
          };
          const sc = await log.scan(seedH, folder);
          keepAlive.push(sc);
          return wrapNode(sc.nodeId);
        },
        async attach(upstream, opts) {
          const skipCachedReplay = opts && opts.skipCachedReplay === true;
          // D270 (memo:Re P2 parity): pass skipCachedReplay through to
          // the napi `attach`. When true, drops the subscribe-handshake
          // DATA replay if the upstream has a cached value.
          const sub = await log.attach(upstream.inner, skipCachedReplay);
          keepAlive.push(sub);
          return async () => {
            // S6 QA fix 2026-05-20: BenchLogSubscription.dispose is
            // now async (was sync pre-S6). Pre-S6 the sync `sub.dispose()`
            // returned undefined immediately; post-S6 it returns a
            // Promise — without await, the UnsubFn resolves before
            // the underlying ReactiveSub::detach actually completes
            // (parity contract: `UnsubFn = () => Promise<void>` from
            // `packages/parity-tests/impls/types.ts`).
            await sub.dispose();
          };
        },
      };
    },

    reactiveList() {
      const packer = (handles) => {
        const values = handles.map((h) => state.registry.get(h));
        const out = state.core.allocExternalHandle();
        state.registry.set(out, values);
        return out;
      };
      const list = n.BenchReactiveList.create(state.core, packer);
      const node = wrapNode(list.nodeId);
      return {
        node,
        get size() {
          return list.size;
        },
        async append(value) {
          const h = state.core.allocExternalHandle();
          state.registry.set(h, value);
          await list.append(h);
        },
        async appendMany(values) {
          const handles = values.map((v) => {
            const h = state.core.allocExternalHandle();
            state.registry.set(h, v);
            return h;
          });
          await list.appendMany(handles);
        },
        async insert(index, value) {
          const h = state.core.allocExternalHandle();
          state.registry.set(h, value);
          await list.insert(index, h);
        },
        async pop(index) {
          const h = await list.pop(index ?? null);
          return state.registry.get(h);
        },
        async clear() {
          await list.clear();
        },
        at(index) {
          const h = list.at(index);
          if (h === 0) return undefined;
          return state.registry.get(h);
        },
      };
    },

    reactiveMap(opts) {
      const packer = (handles) => {
        const pairs = [];
        for (let i = 0; i < handles.length; i += 2) {
          pairs.push([state.registry.get(handles[i]), state.registry.get(handles[i + 1])]);
        }
        const out = state.core.allocExternalHandle();
        state.registry.set(out, new Map(pairs));
        return out;
      };
      const map = n.BenchReactiveMap.create(
        state.core,
        packer,
        opts && opts.maxSize != null ? opts.maxSize : null,
        null,
      );
      const node = wrapNode(map.nodeId);
      const keyHandles = new Map();
      return {
        node,
        get size() {
          return map.size;
        },
        async set(key, value) {
          let kh = keyHandles.get(key);
          if (kh == null) {
            kh = state.core.allocExternalHandle();
            state.registry.set(kh, key);
            keyHandles.set(key, kh);
          }
          const vh = state.core.allocExternalHandle();
          state.registry.set(vh, value);
          await map.set(kh, vh, null);
        },
        // S6 QA fix 2026-05-20 (F3): map.get/has are now async at the
        // napi surface (TTL-prune emits a snapshot → libuv deadlock
        // vector under prior sync `run_sync` shape). Outer wrappers
        // become async to match the parity Impl contract.
        async get(key) {
          const kh = keyHandles.get(key);
          if (kh == null) return undefined;
          const vh = await map.get(kh);
          if (vh === 0) return undefined;
          return state.registry.get(vh);
        },
        async has(key) {
          const kh = keyHandles.get(key);
          if (kh == null) return false;
          return await map.has(kh);
        },
        async delete(key) {
          const kh = keyHandles.get(key);
          if (kh == null) return;
          await map.delete(kh);
        },
        async clear() {
          await map.clear();
          keyHandles.clear();
        },
      };
    },

    reactiveIndex() {
      const packer = (handles) => {
        const rows = [];
        for (let i = 0; i < handles.length; i += 2) {
          rows.push({
            primary: state.registry.get(handles[i]),
            value: state.registry.get(handles[i + 1]),
          });
        }
        const out = state.core.allocExternalHandle();
        state.registry.set(out, rows);
        return out;
      };
      const index = n.BenchReactiveIndex.create(state.core, packer);
      const node = wrapNode(index.nodeId);
      const primaryHandles = new Map();
      let sawNonNumericPrimary = false;
      return {
        node,
        get size() {
          return index.size;
        },
        async upsert(primary, secondary, value) {
          let ph = primaryHandles.get(primary);
          if (ph == null) {
            ph = state.core.allocExternalHandle();
            state.registry.set(ph, primary);
            primaryHandles.set(primary, ph);
          }
          const vh = state.core.allocExternalHandle();
          state.registry.set(vh, value);
          const numericKey = typeof primary === 'number' ? primary : null;
          if (numericKey === null) sawNonNumericPrimary = true;
          return index.upsert(ph, secondary, vh, numericKey);
        },
        async delete(primary) {
          const ph = primaryHandles.get(primary);
          if (ph == null) return;
          const numericKey = typeof primary === 'number' ? primary : null;
          await index.delete(ph, numericKey);
        },
        async clear() {
          await index.clear();
          primaryHandles.clear();
        },
        has(primary) {
          const ph = primaryHandles.get(primary);
          if (ph == null) return false;
          return index.has(ph);
        },
        get(primary) {
          const ph = primaryHandles.get(primary);
          if (ph == null) return undefined;
          const vh = index.get(ph);
          if (vh === 0) return undefined;
          return state.registry.get(vh);
        },
        rangeByPrimary(start, end) {
          if (sawNonNumericPrimary) {
            throw new Error(
              'rangeByPrimary requires numeric primary keys on the rust arm ' +
                '(D205: the i64 mirror cannot represent non-numeric primaries)',
            );
          }
          const handles = index.rangeByPrimary(start, end);
          return handles.map((h) => state.registry.get(h));
        },
      };
    },
  };
}

module.exports = {
  createNativeImpl,
  RingBuffer,
  ResettableTimer,
  sourceOpts,
  DATA,
  RESOLVED,
  DIRTY,
  INVALIDATE,
  PAUSE,
  RESUME,
  COMPLETE,
  ERROR,
  TEARDOWN,
};
