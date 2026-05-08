/* eslint-disable */
/**
 * @graphrefly/native — napi-rs binding type declarations.
 *
 * Hand-written for the Phase E rustImpl activation slice (D074); napi-cli
 * regenerates this file when `pnpm build` runs. We commit a hand-written
 * version so TypeScript can type-check `rust.ts` even before the .node
 * artifact is built.
 *
 * Surface mirrors:
 * - `BenchCore` from `core_bindings.rs`
 * - `BenchOperators` from `operator_bindings.rs`
 * - `BenchGraph` from `graph_bindings.rs`
 */

export declare function version(): string;

/** Empty FFI round-trip — bench utility. */
export declare function noopCall(): void;
/** Single-i32-return FFI round-trip — bench utility. */
export declare function noopCallReturningInt(): number;

export declare const enum BuiltinFn {
  Identity = 'Identity',
  AddOne = 'AddOne',
}

export declare const enum BuiltinBatchFn {
  MapAddOneBatch = 'MapAddOneBatch',
  MulTenThenComplete = 'MulTenThenComplete',
}

export interface BatchEmissionJs {
  kind: string;
  value?: number | null;
}

export interface ResumeReportJs {
  replayed: number;
  dropped: number;
}

export interface RemoveAuditJs {
  nodeCount: number;
  mountCount: number;
}

export declare class BenchCore {
  constructor();

  // Sync handle / value registry helpers.
  internInt(value: number): number;
  derefInt(handle: number): number;
  allocExternalHandle(): number;

  // JS-side prune callback installation (D076).
  setReleaseCallback(callback: (handle: number) => void): void;

  // State / derived registration.
  registerStateInt(initial: number): Promise<number>;
  registerStateSentinel(): Promise<number>;
  registerStateWithHandle(initialHandle: number): Promise<number>;
  registerDerived(depIds: Array<number>, builtin: BuiltinFn): Promise<number>;
  registerBatchDerived(depIds: Array<number>, builtin: BuiltinBatchFn): Promise<number>;

  // Subscriptions.
  subscribeNoop(nodeId: number): Promise<number>;
  subscribeWithTsfn(nodeId: number, sinkCallback: (msgs: Array<number>) => void): Promise<number>;
  unsubscribe(idx: number): Promise<void>;

  // Emission.
  emitInt(nodeId: number, value: number): Promise<void>;
  emitHandle(nodeId: number, handle: number): Promise<void>;
  cacheInt(nodeId: number): Promise<number>;
  cacheHandle(nodeId: number): Promise<number>;
  rustEmitLoop(nodeId: number, n: number): Promise<void>;
  batchEmitInts(nodeId: number, values: Array<number>): Promise<void>;
  batchEmitMessages(nodeId: number, msgs: Array<BatchEmissionJs>): Promise<void>;
  /**
   * Atomic batch dispatch using flat (code, payload) u32 pairs. Phase E
   * /qa F2 — D077 "one call = one wave". See `core_bindings.rs` doc for
   * the MSG_CODE_* table.
   */
  batchEmitHandleMessages(nodeId: number, encoded: Array<number>): Promise<void>;

  // Lifecycle ops.
  allocLockId(): Promise<number>;
  setPauseBufferCap(cap: number | null): Promise<void>;
  pause(nodeId: number, lockId: number): Promise<void>;
  resume(nodeId: number, lockId: number): Promise<ResumeReportJs | null>;
  isPaused(nodeId: number): Promise<boolean>;
  pauseLockCount(nodeId: number): Promise<number>;
  holdsPauseLock(nodeId: number, lockId: number): Promise<boolean>;
  invalidate(nodeId: number): Promise<void>;
  complete(nodeId: number): Promise<void>;
  errorInt(nodeId: number, errCode: number): Promise<void>;
  teardown(nodeId: number): Promise<void>;
  addMetaCompanion(parent: number, companion: number): Promise<void>;
  setResubscribable(nodeId: number, resubscribable: boolean): Promise<void>;
  setDeps(nodeId: number, newDepIds: Array<number>): Promise<void>;
  hasFiredOnce(nodeId: number): Promise<boolean>;
}

export declare class BenchOperators {
  static fromCore(core: BenchCore): BenchOperators;

  // Transform.
  registerMap(src: number, project: (h: number) => number): Promise<number>;
  registerFilter(src: number, predicate: (h: number) => boolean): Promise<number>;
  registerScan(src: number, seed: number, folder: (acc: number, value: number) => number): Promise<number>;
  registerReduce(src: number, seed: number, folder: (acc: number, value: number) => number): Promise<number>;
  registerDistinctUntilChanged(src: number): Promise<number>;
  registerDistinctUntilChangedWith(src: number, equals: (a: number, b: number) => boolean): Promise<number>;
  registerPairwise(src: number, packer: (prev: number, current: number) => number): Promise<number>;

  // Combine.
  registerCombine(srcs: Array<number>, packer: (handles: Array<number>) => number): Promise<number>;
  registerWithLatestFrom(primary: number, secondary: number, packer: (handles: Array<number>) => number): Promise<number>;
  registerMerge(srcs: Array<number>): Promise<number>;

  // Flow.
  registerTake(src: number, count: number): Promise<number>;
  registerSkip(src: number, count: number): Promise<number>;
  registerTakeWhile(src: number, predicate: (h: number) => boolean): Promise<number>;
  registerLast(src: number): Promise<number>;
  registerLastWithDefault(src: number, defaultHandle: number): Promise<number>;
  registerFirst(src: number): Promise<number>;
  registerFind(src: number, predicate: (h: number) => boolean): Promise<number>;
  registerElementAt(src: number, index: number): Promise<number>;

  // Subscription-managed combinators.
  registerZip(srcs: Array<number>, packer: (handles: Array<number>) => number): Promise<number>;
  registerConcat(first: number, second: number): Promise<number>;
  registerRace(srcs: Array<number>): Promise<number>;
  registerTakeUntil(src: number, notifier: number): Promise<number>;

  // Higher-order.
  registerSwitchMap(outer: number, project: (h: number) => number): Promise<number>;
  registerExhaustMap(outer: number, project: (h: number) => number): Promise<number>;
  registerConcatMap(outer: number, project: (h: number) => number): Promise<number>;
  registerMergeMap(outer: number, project: (h: number) => number, concurrency?: number | null): Promise<number>;
}

export declare class BenchGraph {
  static fromCore(core: BenchCore, name: string): BenchGraph;

  // Sync introspection.
  name(): string;
  nodeCount(): number;
  nodeNames(): Array<string>;
  childNames(): Array<string>;
  isDestroyed(): boolean;
  tryResolve(path: string): number;
  nameOf(nodeId: number): string | null;

  // Sugar constructors.
  state(name: string, initialHandle?: number | null): Promise<number>;
  derived(name: string, depIds: Array<number>, builtin: BuiltinFn): Promise<number>;
  add(name: string, nodeId: number): number;

  // Named-sugar lifecycle.
  setByName(name: string, handle: number): Promise<void>;
  getByName(name: string): Promise<number>;
  invalidateByName(name: string): Promise<void>;
  completeByName(name: string): Promise<void>;
  errorByName(name: string, errorHandle: number): Promise<void>;
  remove(name: string): Promise<RemoveAuditJs>;

  // Signal broadcast.
  signalInvalidate(): Promise<void>;
  signalPause(lockId: number): Promise<void>;
  signalResume(lockId: number): Promise<void>;
  /**
   * Atomic-ish broadcast over a flat (code, payload) batch with
   * server-side input validation. Phase E /qa F3 — rejects tier-3
   * (DATA/RESOLVED) + COMPLETE/ERROR/TEARDOWN before any partial
   * broadcast fires (legacy parity).
   */
  signalBatch(encoded: Array<number>): Promise<void>;

  // Mount tree.
  mountNew(name: string): Promise<BenchGraph>;
  unmount(name: string): Promise<RemoveAuditJs>;

  // Lifecycle.
  destroy(): Promise<void>;

  // Static introspection.
  edges(recursive: boolean): Array<string>;
  describeJson(): string;
}

// Global singleton variants (legacy — kept for cross-Worker bench).
export declare function globalRegisterStateInt(initial: number): Promise<number>;
export declare function globalRegisterDerivedIdentity(depIds: Array<number>): Promise<number>;
export declare function globalSubscribeNoop(nodeId: number): Promise<number>;
export declare function globalEmitInt(nodeId: number, value: number): Promise<void>;
export declare function globalCacheInt(nodeId: number): Promise<number>;
export declare function globalRustEmitLoop(nodeId: number, n: number): Promise<void>;
