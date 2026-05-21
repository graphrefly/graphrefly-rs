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
  tryResolve(path: string): NativeNode<unknown> | undefined;
  nameOf(node: NativeNode<unknown>): string | undefined;
  state<T>(name: string, initial?: T): Promise<NativeNode<T>>;
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
  edges(opts?: { recursive?: boolean }): Array<[string, string]>;
  describe(): unknown;
  describe(opts: { reactive: true }): Promise<ReactiveDescribeHandle>;
  observe(): Promise<ObserveSubscription>;
  observe(path: string): Promise<ObserveSubscription>;
  observe(path: string | undefined, opts: { reactive: true }): Promise<ObserveSubscription>;
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
  describeNode(node: NativeNode<unknown>): unknown;
  sha256Hex(input: string | Uint8Array): Promise<string>;
  sourceOpts(opts?: Record<string, unknown>): Record<string, unknown>;

  /** @internal Parity-harness disposal hook (per-test fresh Core). */
  _dispose?: () => Promise<void>;
}

/**
 * Construct a fresh async substrate surface (own BenchCore + registry).
 * Direct consumers call once; the parity harness calls per-test.
 */
export declare function createNativeImpl(): NativeImpl;
