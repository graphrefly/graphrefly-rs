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
}

// ---------------------------------------------------------------------------
// NativeGraph — async Graph wrapper over BenchGraph.
// ---------------------------------------------------------------------------

class NativeGraph {
  constructor(bench, core, registry, nodesByName) {
    this.bench = bench;
    this.core = core;
    this.registry = registry;
    this.nodesByName = nodesByName || new Map();
    // Exposed for storage binding access (raw BenchGraph).
    this._bench = bench;
  }

  tryResolve(path) {
    const id = this.bench.tryResolve(path);
    if (id === 0) return undefined;
    const cached = this.nodesByName.get(path);
    if (cached) return cached;
    return new NativeNode(this.core, id, this.registry);
  }

  nameOf(node) {
    const id = node.inner;
    return this.bench.nameOf(id) ?? undefined;
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
    return node;
  }

  async derived(_name, _deps, _fn) {
    // `BenchGraph.derived` only supports built-in fns; JS-callback
    // derived nodes go through the operator factories + `add`.
    throw new Error(
      '[graphrefly/native] graph.derived(name, deps, fn) with arbitrary fn is ' +
        'not supported by the BenchGraph surface; use the operator factories ' +
        '(impl.map / impl.filter / ...) plus graph.add().',
    );
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
    const audit = await this.bench.remove(name);
    this.nodesByName.delete(name);
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
    return new NativeGraph(childBench, this.core, this.registry);
  }

  async unmount(name) {
    return this.bench.unmount(name);
  }

  async destroy() {
    await this.bench.destroy();
    this.nodesByName.clear();
  }

  async destroyAsync() {
    // BenchGraph.destroy() is already async-shaped; awaiting it is the
    // native-arm analogue of pure-ts's storage-disposer-awaiting variant.
    await this.bench.destroy();
    this.nodesByName.clear();
  }

  edges(opts) {
    const flat = this.bench.edges((opts && opts.recursive) ?? false);
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
    return JSON.parse(this.bench.describeJson());
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
}

// ---------------------------------------------------------------------------
// N1 substrate-infra surface (5 symbols — D203 item 8 / D206-D207).
//
// `RingBuffer` / `ResettableTimer` / `sourceOpts` are thin pure-TS infra
// (NO Rust counterpart needed — types.ts: "thin TS over napi core, no
// Rust"). Hand-written here so the surface is self-contained.
// `describeNode` / `sha256Hex` route to the new napi fns
// (`BenchCore.describeNode` sync — D207 reuse of the describe
// projection; `BenchCore.sha256Hex` async at the boundary, sync hashing
// in graphrefly-core).
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
// ---------------------------------------------------------------------------

function createNativeImpl() {
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
        super(bench, state.core, state.registry);
      }
    },

    // Transform.
    async map(src, fn) {
      return wrapNode(await state.operators.registerMap(unwrap(src), makeProjector(fn)));
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
    describeNode(node) {
      const json = state.core.describeNode(node.inner);
      return JSON.parse(json);
    },
    async sha256Hex(input) {
      const bytes = typeof input === 'string' ? new TextEncoder().encode(input) : input;
      return state.core.sha256Hex(bytes);
    },
    sourceOpts,
  };

  // Internal disposal hook for the parity harness (per-test fresh Core).
  impl._dispose = async () => {
    await state.core.dispose().catch(() => {});
  };

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
      );
      return {
        get name() {
          return tier.name;
        },
        appendEntries(entries) {
          tier.appendEntries(JSON.stringify(entries));
        },
        async loadEntries(keyFilter) {
          return JSON.parse(tier.loadEntries(keyFilter ?? null));
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
        async attach(upstream) {
          const sub = await log.attach(upstream.inner);
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
