# `@graphrefly/native`

Rust-native substrate for [`@graphrefly/graphrefly`](https://github.com/clfhhc/graphrefly-ts)
— same `Impl` contract as `@graphrefly/pure-ts`, implemented in Rust via
[napi-rs](https://napi.rs).

This package is an **async preview** (D206/D207): every Core-touching call
returns a `Promise`. Direct sync consumers stay on `@graphrefly/pure-ts`
for the time being; this package exists for async-tolerant Node consumers
and is the parity-arm in `packages/parity-tests`.

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

## What ships in v0.0.8

- `BenchCore::close()` async napi (drains subs + shuts down actor).
- `Symbol.asyncDispose` on `NativeImpl` for ES2024 `await using`.
- `_dispose` parity-harness hook aliased to `close()` (semantic change
  pre-1.0: `_dispose` now also kills the actor — verified safe by grep
  for all `_dispose` callers).
- Post-close error message broadened to "(actor is shut down or shutting
  down)" so consumers can recognize shutdown-class failures uniformly.

## What's coming in v0.1.0 (D292)

- `FinalizationRegistry` GC fallback so missing `close()` is at most a
  delayed exit (not a hang).
- `process.on('beforeExit')` safety net for auto-cleanup.
- `Symbol.asyncDispose` on nested handles (`Graph`, `Subscription`,
  `BenchBatchContext`).
- Async commit/rollback on `BenchBatchContext` (closes a separate
  libuv-deadlock class for TSFN-backed sinks in `BatchGuard::Drop`).
- Async-shutdown-from-finalizer so GC of `NativeImpl` on the libuv
  thread doesn't block the JS event loop on the worker join.

## License

MIT — see [LICENSE](../../LICENSE).
