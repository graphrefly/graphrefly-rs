# `@graphrefly/native`

> **Historical port-era binding package.** This crate is excluded from the
> clean-slate workspace and retained as frozen reference material. It is not
> current Rust package API guidance. Current Rust package docs are governed by
> [`../../docs/docs.jsonl`](../../docs/docs.jsonl); shared public docs live in
> `~/src/graphrefly` under D563.

Rust-native preview substrate for the retired `@graphrefly/graphrefly` /
`@graphrefly/pure-ts` port-era stack, implemented in Rust via
[napi-rs](https://napi.rs). Treat references to the old parity harness as
historical behavior-test context, not structural `Impl` parity guidance.

This package is an **async preview** (D206/D207): every Core-touching call
returns a `Promise`. Direct sync consumers stay on `@graphrefly/pure-ts`
for the time being; this package existed for async-tolerant Node consumers
and as a historical participant in `packages/parity-tests`.

## Install

```sh
pnpm add @graphrefly/native
# or
npm install @graphrefly/native
```

The `.node` binary is selected at install time from a per-platform
sub-package (`@graphrefly/native-darwin-arm64`, `…linux-x64-gnu`, etc.).
napi-rs's loader (`index.js`) picks the right one automatically.

## Quick start

```js
import { createNativeImpl } from "@graphrefly/native";

const impl = createNativeImpl();

const state = await impl.node([], { name: "counter", initial: 0 });
await state.subscribe((msgs) => {
  for (const m of msgs) console.log(m);
});

await state.down([[impl.DATA, 42]]);

// CRITICAL — see "Closing a NativeImpl" below.
await impl.close();
```

## Closing a `NativeImpl`

**Every `NativeImpl` MUST be closed when you're done with it.**

`@graphrefly/native` spawns one Rust worker thread per `NativeImpl`
(via `std::thread::spawn`). Rust threads on POSIX have no daemon concept
— the thread blocks Node's process exit indefinitely until explicitly
joined. Without `await impl.close()`, your Node process will appear to
hang on exit (test frameworks, CLI scripts, serverless cold-start, and
AWS Lambda all hit this).

### Modern pattern (Node 22+) — `await using`

ES2024 explicit-resource-management. The `[Symbol.asyncDispose]` wired on
`NativeImpl` auto-calls `close()` at block exit.

```js
import { createNativeImpl } from "@graphrefly/native";

{
  await using impl = createNativeImpl();
  // ... your reactive logic ...
} // ← impl.close() auto-called here; Rust worker thread exits cleanly
```

### Compat pattern (any Node version) — `try` / `finally`

```js
import { createNativeImpl } from "@graphrefly/native";

const impl = createNativeImpl();
try {
  // ... your reactive logic ...
} finally {
  await impl.close();
}
```

### Test framework usage

Most test frameworks (vitest, jest, mocha) auto-detect hanging workers
and either hang forever or print a "workers did not exit" warning. The
fix is the same — `close()` per `NativeImpl` in your test teardown:

```js
import { afterEach } from "vitest";

let impl;
beforeEach(() => { impl = createNativeImpl(); });
afterEach(async () => { await impl.close(); });
```

Or with `await using` per test (Node 22+):

```js
test("my reactive scenario", async () => {
  await using impl = createNativeImpl();
  // ... test ...
}); // impl.close() auto-called at scope exit
```

### Behavior contract

- **Idempotent.** Subsequent `close()` calls are best-effort no-ops; no
  throw.
- **Synchronous wait.** `await impl.close()` returns only after the Rust
  worker thread has exited and `Core` has dropped on its stack.
- **Post-close method calls reject.** After `close()`, any method on
  `impl` (or any nested handle that shares the same actor — `Graph`,
  `Subscription`, etc.) rejects with `Error: "CoreActor#N: worker thread
  dropped before closure dispatch (actor is shut down or shutting
  down)"`. JavaScript code that awaits a method post-close gets a
  Promise rejection.
- **`Symbol.asyncDispose` is wired.** On Node 22+, `await using` works
  out of the box. On older Node, the Symbol-keyed property is silently
  ignored — use the explicit `await impl.close()` pattern.

### When to opt in to `autoCloseOnBeforeExit`

The default surface (`createNativeImpl()`) does NOT register a
`process.on('beforeExit', () => impl.close())` safety net — you call
`close()` explicitly (or use `await using` on Node 22+). Opt in to
`createNativeImpl({ autoCloseOnBeforeExit: true })` ONLY when ALL three
apply:

1. **Your runtime fires `beforeExit` reliably.** **NOT** under: jest
   worker pools with `isolate: false`; deno; browser (wasm);
   `process.exit()` paths; long-lived servers (`beforeExit` only fires
   when the event loop is genuinely empty).
2. **You can't sequence an explicit `await impl.close()` at the right
   place** (e.g., your code creates a NativeImpl as a module-level
   singleton with no natural teardown hook).
3. **You accept that `beforeExit` runs synchronously** — close-drain
   semantics (the close-waits-for-in-flight contract) may block process
   exit beyond expected timing.

If any of the three doesn't hold, prefer `await using` (Node 22+) or
explicit `try/finally`.

```js
// Opt in:
const impl = createNativeImpl({ autoCloseOnBeforeExit: true });
// ...use impl across module scope; no need to explicitly close.
// Node exits → beforeExit fires → impl.close() drains + shuts down.
```

If a `close()` failure happens inside the `beforeExit` handler (storage
tier timeout, async-commit panic propagation, etc.) it surfaces via
`console.error('[graphrefly/native] close() during beforeExit failed:',
err)` — the rejection cannot reject a Promise contract because
`beforeExit` handlers don't have one, so console-error is the canonical
surfacing path for unhandled cleanup-path errors.

### Escape hatch: `process.exit()`

If you want the Node process to terminate without explicit `close()` —
e.g., a short-lived CLI script where cleanup overhead doesn't matter —
`process.exit(0)` bypasses Node's wait-for-threads exit logic and kills
the process directly. Useful for scripts, NOT for long-running services
(it skips other cleanup paths like `process.on('exit')` handlers and
streaming flushes).

```js
const impl = createNativeImpl();
await doMyReactiveWork(impl);
process.exit(0);  // skip cleanup, kill process now
```

`close()` is the structured alternative — same outcome (process exits
cleanly), without bypassing other cleanup paths.

## What ships in v0.1.0 (D292)

- `BenchCore::close()` async napi (drains subs + shuts down actor) — D293.
- `Symbol.asyncDispose` on `NativeImpl` for ES2024 `await using` — D293.
- Post-close error message broadened to "(actor is shut down or shutting
  down)" so consumers can recognize shutdown-class failures uniformly — D293.
- **D292 D.1 — `BenchGraph.derived(name, deps, fn)`** arbitrary-fn
  widening via TSFN reroute (lifts the prior throw on JS-callback derived
  nodes; closure-cell eviction auto-wired to `graph.remove`/`destroy`).
- **D292 D.2 — Async `BenchBatchContext::commit` + `rollback`** via
  `tokio::task::spawn_blocking` so libuv stays free during the sink-fire
  drain; symmetric `catch_unwind` on both commit + rollback surfaces sink
  panics as rejected Promises (closes the BH15 sink-panic-hang class).
- **D292 D.3 Item 1 — `FinalizationRegistry`-driven async shutdown** off
  the libuv thread (GC of a `NativeImpl` no longer blocks the JS event
  loop while the worker joins).
- **D292 D.3 Item 2 — `impl.close()` rejects on actor errors** (no
  silent swallow; matches Promise contract).
- **D292 D.3 Item 5 — `autoCloseOnBeforeExit` opt-in safety net** (see
  "When to opt in to `autoCloseOnBeforeExit`" above for the 3-condition
  rubric).
- **F4 — `_dispose` parity-harness alias dropped** (pre-1.0 cleanup;
  callers use the public `close()`).

## Deferred to a future minor bump

- `Symbol.asyncDispose` on nested handles (`Graph`, `Subscription`,
  `BenchBatchContext`).
- Async commit/rollback on `BenchBatchContext` (closes a separate
  libuv-deadlock class for TSFN-backed sinks in `BatchGuard::Drop`).
- Async-shutdown-from-finalizer so GC of `NativeImpl` on the libuv
  thread doesn't block the JS event loop on the worker join.

## License

MIT — see [LICENSE](../../LICENSE).
