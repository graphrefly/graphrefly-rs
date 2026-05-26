/**
 * `@graphrefly/native` — hand-written ergonomic ASYNC public surface.
 *
 * Option C deliverable (graphrefly-ts
 * `archive/docs/SESSION-DS-native-substrate-contract.md`; D206 / D207).
 * Async over the napi `Bench*` classes, shape-mirroring the parity
 * `Impl` contract (graphrefly-ts `packages/parity-tests/impls/types.ts`)
 * WITHOUT depending on `@graphrefly/pure-ts`. Every Core-touching call
 * is a `Promise` (Core runs on a tokio blocking pool — D070/D077).
 *
 * NOT a `@graphrefly/pure-ts` sync drop-in; `@graphrefly/graphrefly`
 * does not consume it (Option B / D080 — deferred).
 */

export type Tier = symbol;

export type Message<T = unknown> =
  | readonly [Tier]
  | readonly [Tier, T]
  | readonly [Tier, ...unknown[]];

export type UnsubFn = () => Promise<void>;
export type SinkFn<T> = (msgs: ReadonlyArray<Message<T>>) => void;

/**
 * Per-frame batch context surfaced to `NativeImpl.batch`'s `fn` argument
 * (D288 Q3 lock, D289 binding). Single-msg shape; do NOT stash — post-
 * frame `down(...)` throws.
 */
export interface NativeBatchCtx {
  down<T>(node: NativeNode<T>, msg: Message<T>): void;
}

export interface NativeNode<T> {
  subscribe(cb: SinkFn<T>): Promise<UnsubFn>;
  down(msgs: ReadonlyArray<Message<T>>): Promise<void>;
  readonly cache: T | undefined;
  complete(): Promise<void>;
  error(value: T): Promise<void>;
  invalidate(): Promise<void>;
  teardown(): Promise<void>;
  pause(lockId: number): Promise<void>;
  resume(lockId: number): Promise<{ replayed: number; dropped: number } | null>;
  allocLockId(): Promise<number>;
  setResubscribable(value: boolean): Promise<void>;
  hasFiredOnce(): Promise<boolean>;
  /** D263/D264 — atomically replace the full upstream dep set AND the
   * transform fn. Mirrors pure-ts `Node.setDeps`. Available only on
   * nodes built via `impl.map` (the only operator wired through the
   * generic `register_user_derived` path per D264 P1); calling on
   * other nodes throws. Self-dep and cycle rejection happens Core-side
   * via `Core::set_deps`. */
  setDeps(
    newDeps: ReadonlyArray<NativeNode<unknown>>,
    fn: (data: ReadonlyArray<ReadonlyArray<unknown>>) => ReadonlyArray<unknown>,
  ): Promise<void>;
  /** D263/D264 — append one dep, replacing the fn for the grown shape.
   * Returns the new dep's index (or the existing index if already
   * present). Same scope rules as {@link setDeps}. */
  addDep(
    dep: NativeNode<unknown>,
    fn: (data: ReadonlyArray<ReadonlyArray<unknown>>) => ReadonlyArray<unknown>,
  ): Promise<number>;
  /** D263/D264 — remove one dep, replacing the fn for the shrunk shape.
   * Idempotent (fn swap still applies when `dep` is absent). Same
   * scope rules as {@link setDeps}. */
  removeDep(
    dep: NativeNode<unknown>,
    fn: (data: ReadonlyArray<ReadonlyArray<unknown>>) => ReadonlyArray<unknown>,
  ): Promise<void>;
  readonly inner: unknown;
}

export interface NativeGraph {
  // D267 — `tryResolve`/`nameOf`/`edges`/`describe` are async on the
  // native arm (was sync; deadlocked when called from inside a TSFN
  // sink callback). The parity `Impl` contract is widened to `T |
  // Promise<T>` so the pure-ts arm keeps its sync shape unchanged.
  tryResolve(path: string): Promise<NativeNode<unknown> | undefined>;
  nameOf(node: NativeNode<unknown>): Promise<string | undefined>;
  state<T>(name: string, initial?: T): Promise<NativeNode<T>>;
  /**
   * **D292 D.1** — arbitrary-fn derived node via TSFN-backed
   * `registerUserDerived` reroute + `bench.add`. The user `fn` receives
   * the wave's per-dep batches and returns the wave's per-output values.
   *
   * **CLOSURE-CELL LIFETIME (R1 anti-pattern #5 watch):** the `fn` is
   * retained in the graph's per-name closure-cell map. Eviction is wired
   * to `graph.remove(name)`, `graph.destroy()`, and `impl.close()`
   * cascade (F3 cross-cutting closure-cell registry teardown). **If
   * substrate teardown ever evolves to fire outside those three paths**
   * (e.g., a future GC-driven sweep, a `Core::trim_dead_nodes` primitive,
   * etc.), update eviction wiring on the JS side — silently leaving the
   * closure cell alive past the substrate node IS the foot-gun.
   *
   * **Trade-off vs OperatorOp paths:** arbitrary-fn derived nodes go
   * through the generic TSFN dispatch and lose `OperatorOp::Map` /
   * `Filter` / etc. optimizations. Same trade-off `impl.map` already
   * takes per D263; this method extends it to `graph.derived(...)`.
   *
   * **2 napi crossings per call** (`registerUserDerived` + `bench.add`).
   * The fused single-crossing variant (D292 Option C) is deferred per
   * D196 consumer-pressure gate; non-breaking widening to add later.
   */
  derived<T>(
    name: string,
    deps: ReadonlyArray<NativeNode<unknown>>,
    fn: (data: ReadonlyArray<ReadonlyArray<unknown>>) => ReadonlyArray<T>,
  ): Promise<NativeNode<T>>;
  dynamic<T>(
    name: string,
    deps: ReadonlyArray<NativeNode<unknown>>,
    fn: (data: ReadonlyArray<ReadonlyArray<unknown>>) => ReadonlyArray<T>,
  ): Promise<NativeNode<T>>;
  add<T>(name: string, node: NativeNode<T>): Promise<NativeNode<T>>;
  set(name: string, value: unknown): Promise<void>;
  get(name: string): Promise<unknown>;
  invalidate(name: string): Promise<void>;
  complete(name: string): Promise<void>;
  error(name: string, value: unknown): Promise<void>;
  remove(name: string): Promise<{ nodeCount: number; mountCount: number }>;
  signal(messages: ReadonlyArray<Message>): Promise<void>;
  mount(name: string, child?: NativeGraph): Promise<NativeGraph>;
  unmount(name: string): Promise<{ nodeCount: number; mountCount: number }>;
  destroy(): Promise<void>;
  destroyAsync(): Promise<void>;
  edges(opts?: { recursive?: boolean }): Promise<Array<[string, string]>>;
  describe(): Promise<unknown>;
  describe(opts: { reactive: true }): Promise<ReactiveDescribeHandle>;
  observe(): Promise<ObserveSubscription>;
  observe(path: string): Promise<ObserveSubscription>;
  observe(path: string | undefined, opts: { reactive: true }): Promise<ObserveSubscription>;
  /**
   * R3.1.2 — annotate the graph with factory provenance for
   * `describe({ detail: "spec" })`, snapshot replay, debugging. D286.
   * Surfaces at the top of `describe()` output as `factory` +
   * `factoryArgs` keys. A second call with `factoryArgs === undefined`
   * MUST clear stale args (QA F8 invariant). Drops the spec's
   * `this`-chain return per the D267/D282 async-everywhere convention.
   */
  tagFactory(factory: string, factoryArgs?: unknown): Promise<void>;
  /**
   * R3.6.3 — snapshot-based runtime profile (per-node subscriber
   * counts + dep counts, top-N hotspots by subscriber/dep count,
   * orphan classification). D286.
   *
   * D284 amendment: the returned object does NOT carry
   * `valueSizeBytes` per node, `totalValueSizeBytes` aggregate, or
   * `hotspots.byValueSize` — these were pure-ts-inferred fields the
   * canonical spec doesn't mandate (see `wrapper.js` D286 comment).
   */
  resourceProfile(opts?: { topN?: number }): Promise<NativeGraphProfileResult>;
}

/** D284-narrowed per-node profile (matches `ImplNodeProfile`). */
export interface NativeNodeProfile {
  path: string;
  type: string;
  status: string;
  subscriberCount: number;
  depCount: number;
  isOrphanEffect: boolean;
  orphanKind: "orphan-effect" | "idle-derived" | "idle-producer" | null;
}

/** D284-narrowed aggregate profile (matches `ImplGraphProfileResult`). */
export interface NativeGraphProfileResult {
  nodeCount: number;
  edgeCount: number;
  subgraphCount: number;
  nodes: NativeNodeProfile[];
  hotspots: {
    bySubscriberCount: NativeNodeProfile[];
    byDepCount: NativeNodeProfile[];
  };
  orphans: NativeNodeProfile[];
  orphanEffects: NativeNodeProfile[];
}

export interface ReactiveDescribeHandle {
  subscribe(sink: (snapshot: unknown) => void): () => void;
  dispose(): Promise<void>;
}

export interface ObserveSubscription {
  subscribe(
    sink: (pathOrMsgs: string | ReadonlyArray<Message>, msgs?: ReadonlyArray<Message>) => void,
  ): () => void;
  dispose(): Promise<void>;
}

export interface NativeRingBuffer<T> {
  readonly size: number;
  readonly maxSize: number;
  push(item: T): void;
  shift(): T | undefined;
  at(i: number): T | undefined;
  toArray(): T[];
  clear(): void;
}

export interface NativeResettableTimer {
  start(delayMs: number, callback: () => void): void;
  cancel(): void;
  readonly pending: boolean;
}

/** Fixed-capacity ring buffer (drop-oldest / FIFO eviction). N1. */
export declare class RingBuffer<T> implements NativeRingBuffer<T> {
  constructor(capacity: number);
  readonly size: number;
  readonly maxSize: number;
  push(item: T): void;
  shift(): T | undefined;
  at(i: number): T | undefined;
  toArray(): T[];
  clear(): void;
}

/** Resettable deadline timer (spec §5.10 escape hatch). N1. */
export declare class ResettableTimer implements NativeResettableTimer {
  constructor();
  start(delayMs: number, callback: () => void): void;
  cancel(): void;
  readonly pending: boolean;
}

/** Producer-shaped source NodeOptions defaulter. N1. */
export declare function sourceOpts(
  opts?: Record<string, unknown>,
): Record<string, unknown>;

export declare const DATA: Tier;
export declare const RESOLVED: Tier;
export declare const DIRTY: Tier;
export declare const INVALIDATE: Tier;
export declare const PAUSE: Tier;
export declare const RESUME: Tier;
export declare const COMPLETE: Tier;
export declare const ERROR: Tier;
export declare const TEARDOWN: Tier;

export interface NativeStructures {
  reactiveLog<T>(opts?: { maxSize?: number }): unknown;
  reactiveList<T>(): unknown;
  reactiveMap<K, V>(opts?: { maxSize?: number }): unknown;
  reactiveIndex<K, V>(): unknown;
}

export interface NativeStorage {
  [k: string]: unknown;
}

/**
 * The async public substrate surface. Shape-mirrors the parity `Impl`
 * contract; consumers `import { createNativeImpl } from "@graphrefly/native"`.
 */
export interface NativeImpl {
  readonly name: string;

  readonly DATA: Tier;
  readonly RESOLVED: Tier;
  readonly DIRTY: Tier;
  readonly INVALIDATE: Tier;
  readonly PAUSE: Tier;
  readonly RESUME: Tier;
  readonly COMPLETE: Tier;
  readonly ERROR: Tier;
  readonly TEARDOWN: Tier;

  node<T>(
    deps: ReadonlyArray<NativeNode<unknown>>,
    opts?: { initial?: T; name?: string; resubscribable?: boolean },
  ): Promise<NativeNode<T>>;

  Graph: new (name: string) => NativeGraph;

  /**
   * Run `fn` inside an explicit batch frame (D282 / D288 Path D / D289).
   *
   * Opens a `BenchBatchContext` via `BenchCore::open_batch` and supplies
   * a per-frame {@link NativeBatchCtx}. Successful return → `commit()`
   * (R4.3.5 drain + `fire_deferred`); throw → `rollback()`
   * (R4.3.2 `discard_wave_cleanup` + `restore_wave_cache_snapshots`),
   * then re-throw.
   *
   * **Per-frame lifetime (D288 Q3 lock):** do NOT stash `ctx`. Post-frame
   * `ctx.down(...)` throws `"BatchCtx used after batch frame closed"`.
   */
  batch(fn: (ctx: NativeBatchCtx) => void): Promise<void>;

  map<T, U>(src: NativeNode<T>, fn: (x: T) => U): Promise<NativeNode<U>>;
  filter<T>(src: NativeNode<T>, predicate: (x: T) => boolean): Promise<NativeNode<T>>;
  scan<T, U>(src: NativeNode<T>, fn: (acc: U, x: T) => U, seed: U): Promise<NativeNode<U>>;
  reduce<T, U>(src: NativeNode<T>, fn: (acc: U, x: T) => U, seed: U): Promise<NativeNode<U>>;
  distinctUntilChanged<T>(
    src: NativeNode<T>,
    equals?: (a: T, b: T) => boolean,
  ): Promise<NativeNode<T>>;
  pairwise<T>(src: NativeNode<T>): Promise<NativeNode<[T, T]>>;
  combine<T>(srcs: ReadonlyArray<NativeNode<unknown>>): Promise<NativeNode<T[]>>;
  withLatestFrom<T, U>(
    primary: NativeNode<T>,
    secondary: NativeNode<U>,
  ): Promise<NativeNode<[T, U]>>;
  merge<T>(srcs: ReadonlyArray<NativeNode<T>>): Promise<NativeNode<T>>;
  take<T>(src: NativeNode<T>, count: number): Promise<NativeNode<T>>;
  skip<T>(src: NativeNode<T>, count: number): Promise<NativeNode<T>>;
  takeWhile<T>(src: NativeNode<T>, predicate: (x: T) => boolean): Promise<NativeNode<T>>;
  last<T>(src: NativeNode<T>, opts?: { defaultValue: T }): Promise<NativeNode<T>>;
  first<T>(src: NativeNode<T>): Promise<NativeNode<T>>;
  find<T>(src: NativeNode<T>, predicate: (x: T) => boolean): Promise<NativeNode<T>>;
  elementAt<T>(src: NativeNode<T>, index: number): Promise<NativeNode<T>>;
  zip<T>(srcs: ReadonlyArray<NativeNode<unknown>>): Promise<NativeNode<T[]>>;
  concat<T>(first: NativeNode<T>, second: NativeNode<T>): Promise<NativeNode<T>>;
  race<T>(srcs: ReadonlyArray<NativeNode<T>>): Promise<NativeNode<T>>;
  takeUntil<T>(src: NativeNode<T>, notifier: NativeNode<unknown>): Promise<NativeNode<T>>;
  switchMap<T, U>(
    outer: NativeNode<T>,
    project: (x: T) => NativeNode<U>,
  ): Promise<NativeNode<U>>;
  exhaustMap<T, U>(
    outer: NativeNode<T>,
    project: (x: T) => NativeNode<U>,
  ): Promise<NativeNode<U>>;
  concatMap<T, U>(
    outer: NativeNode<T>,
    project: (x: T) => NativeNode<U>,
  ): Promise<NativeNode<U>>;
  mergeMap<T, U>(
    outer: NativeNode<T>,
    project: (x: T) => NativeNode<U>,
    concurrency?: number,
  ): Promise<NativeNode<U>>;
  tap<T>(src: NativeNode<T>, fn: (x: T) => void): Promise<NativeNode<T>>;
  tapObserver<T>(
    src: NativeNode<T>,
    opts: { data?: (x: T) => void; error?: (e: unknown) => void; complete?: () => void },
  ): Promise<NativeNode<T>>;
  onFirstData<T>(src: NativeNode<T>, fn: (x: T) => void): Promise<NativeNode<T>>;
  rescue<T>(src: NativeNode<T>, fn: (err: unknown) => T | undefined): Promise<NativeNode<T>>;
  valve<T>(
    src: NativeNode<T>,
    control: NativeNode<unknown>,
    gate: (x: unknown) => boolean,
  ): Promise<NativeNode<T>>;
  settle<T>(src: NativeNode<T>, quietWaves: number, maxWaves?: number): Promise<NativeNode<T>>;
  repeat<T>(src: NativeNode<T>, count: number): Promise<NativeNode<T>>;
  buffer<T>(src: NativeNode<T>, notifier: NativeNode<unknown>): Promise<NativeNode<T[]>>;
  bufferCount<T>(src: NativeNode<T>, count: number): Promise<NativeNode<T[]>>;
  fromIter<T>(values: T[]): Promise<NativeNode<T>>;
  of<T>(values: T[]): Promise<NativeNode<T>>;
  empty<T>(): Promise<NativeNode<T>>;
  throwError<T>(error: unknown): Promise<NativeNode<T>>;
  stratifyBranch<T, R>(
    src: NativeNode<T>,
    rules: NativeNode<R>,
    classifier: (rules: R, value: T) => boolean,
  ): Promise<NativeNode<T>>;

  readonly storage?: NativeStorage;
  readonly structures?: NativeStructures;

  // ── N1 substrate-infra surface (D203 item 8 / D206-D207) ────────────
  RingBuffer: new <T>(capacity: number) => NativeRingBuffer<T>;
  ResettableTimer: new () => NativeResettableTimer;
  // D267 — `describeNode` async (was sync; sink-callback deadlock).
  describeNode(node: NativeNode<unknown>): Promise<unknown>;
  sha256Hex(input: string | Uint8Array): Promise<string>;
  sourceOpts(opts?: Record<string, unknown>): Record<string, unknown>;

  /**
   * **D293 (2026-05-25):** end-of-life surface — shuts down the Rust
   * worker thread + frees the Node process to exit naturally.
   *
   * After `await impl.close()` returns:
   * - The Rust worker thread has exited; `Core` has dropped.
   * - The Node process is free to exit (no non-daemon thread blocking).
   * - Subsequent method calls on this `impl` (or sub-surfaces — Graph,
   *   Subscription) reject with `Error: CoreActor#N: worker thread
   *   dropped before closure dispatch (actor is shut down or shutting
   *   down)`.
   *
   * Idempotent: subsequent `close()` calls are best-effort no-ops.
   *
   * **JS-side ergonomics:**
   * - Modern (Node 22+): `await using impl = createNativeImpl();`
   *   auto-calls `close()` at block exit via [Symbol.asyncDispose].
   * - Compat: `const impl = createNativeImpl(); try { ... } finally {
   *   await impl.close(); }`.
   *
   * **Why required:** the napi binding spawns one non-daemon Rust
   * worker thread per `BenchCore`; `std::thread::spawn` has no daemon
   * concept on POSIX, so the thread blocks Node's process exit
   * indefinitely without `close()` (test frameworks, CLI scripts,
   * serverless cold-start, Lambda all hit this).
   */
  close(): Promise<void>;
  /**
   * **D293 (2026-05-25):** ES2024 explicit-resource-management
   * (`await using`) wiring — Node 22+ auto-calls this at the end of
   * the `await using` block. Identical to {@link close}.
   */
  [Symbol.asyncDispose](): Promise<void>;
}

/**
 * Options bag for {@link createNativeImpl} (D292 D.3 Item 5).
 *
 * All fields optional. Default behavior matches the pre-D292 surface:
 * no `process.on('beforeExit')` safety net; users call `close()`
 * explicitly (or use `await using` on Node 22+).
 */
export interface CreateNativeImplOptions {
  /**
   * **D292 D.3 Item 5 (locked = B, default off):** when `true`,
   * register a `process.on('beforeExit', () => impl.close())` safety
   * net. Default `false`.
   *
   * **See README "Closing a NativeImpl" → "When to opt in to
   * `autoCloseOnBeforeExit`"** for the 3-condition rubric (R5
   * refinement). Opt in ONLY when ALL three apply:
   *
   * 1. Your runtime fires `beforeExit` reliably (NOT under jest worker
   *    pools with `isolate: false`, deno, browser/wasm, `process.exit()`
   *    paths, or long-lived servers).
   * 2. You can't sequence an explicit `await impl.close()` at the
   *    right place (e.g., module-level singleton with no natural
   *    teardown hook).
   * 3. You accept that close-drain (Item 4) may block process exit
   *    beyond expected timing.
   *
   * If any of the three doesn't hold, prefer `await using` (Node 22+)
   * or explicit `try/finally`.
   */
  autoCloseOnBeforeExit?: boolean;
}

/**
 * Construct a fresh async substrate surface (own BenchCore + registry).
 * Direct consumers call once; the parity harness calls per-test.
 *
 * @param opts See {@link CreateNativeImplOptions}.
 */
export declare function createNativeImpl(opts?: CreateNativeImplOptions): NativeImpl;
