# graphrefly-rs — agent context (Rust implementation, clean-slate)

**GraphReFly** — reactive universal reduction layer (high fan-in/out → information
reduction → push; not LLM-limited, D1). This repo is the **Rust implementation**:
the Cargo package `graphrefly-rs`, lib crate `graphrefly`, a self-contained Rust
package (D32), and the native shared engine/reusable graph-infrastructure library
for Python and future non-TS host-language packages (D415). TypeScript remains
self-contained; Python/host packages own idiomatic bindings, value/lifetime
mapping, and ecosystem integration over this Rust layer. Cross-runtime graph
collaboration = a coarse wire bridge, never hidden distributed same-wave
semantics.

> **Clean-slate retired the port model.** The old port-era line (M1–M5 milestones,
> handle-protocol cleaving plane, `Arc<Mutex>` actor model, 8-crate workspace,
> 3-digit D193–D301, parity `Impl` interface) is **history**. This branch
> (`clean-slate`) rebuilds the substrate from the language-neutral spec. Do not
> reach for port-model docs/decisions.

> **This file points, it does not host.** The language-neutral authority — protocol
> spec, decisions, conformance, formal model — lives in `~/src/graphrefly`. When
> anything here disagrees with that repo, **that repo wins.**

## Authority — where the truth lives (`~/src/graphrefly`)

Read `~/src/graphrefly/CLAUDE.md` first — it is the single-source index for the design.

| Concern | Source of truth |
|---|---|
| **Decision locator / global resolver** | `~/src/graphrefly/authority/ledgers.jsonl` + `authority/federation.mjs` |
| **Root language-neutral / cross-project decisions** | `~/src/graphrefly/decisions/decisions.jsonl` |
| **Rust package-local decisions** | `decisions/decisions.jsonl` (`graphrefly-rs:<D#>`) |
| **Relocated root-origin Rust history** | `decisions/root-origin-history.jsonl` (`graphrefly:<D#>`, locator-owned; never for new records) |
| **Design narrative** — L0–L6 locks, F-* constraints, spec-amendment list | `~/src/graphrefly/sessions/active/SESSION-clean-slate-redesign.md` (DS-1) |
| **Protocol rules (宪法)** | `~/src/graphrefly/spec/rules.jsonl` (changed via `/spec-amend`) |
| **Conformance scenarios (parity)** | `~/src/graphrefly/spec/conformance.jsonl` (driven via `/conformance`) |
| **Formal model** | `~/src/graphrefly/formal/*.tla` (+ MC configs) |
| **Cross-project program / backlog / anti-patterns** | `~/src/graphrefly/plan/{phases,backlog,antipatterns}.jsonl` |
| **Rust implementation sequencer** | `plan/work.jsonl` (`graphrefly-rs:<work-id>`; currently empty, registered by root `authority/work-ledgers.jsonl`) |
| **Shared public docs / graphrefly.dev architecture** | `~/src/graphrefly/docs` + guide records (D563) |
| **Rendered authority view** (progress / structure / gaps) | `~/src/graphrefly/dashboard/` (`node dashboard/build.mjs`) |

Sibling packages: `@graphrefly/ts` (`~/src/graphrefly-ts`) is the self-contained
TypeScript package and lead spec-hardening impl; `@graphrefly/py` (`~/src/graphrefly-py`)
is the Python host package layered over the Rust native engine (D415).

## Rust package-local documentation boundary

`docs/docs.jsonl` is this repo's package-local docs policy. It exists to keep
D32 and D563 boundaries sharp:

- `~/src/graphrefly` owns shared public docs, guide records, graphrefly.dev
  website/blog architecture, protocol authority, and dashboard/control views.
- `~/src/graphrefly-rs` owns Rust rustdoc comments, crate README material,
  Rust examples, development docs, crate release notes, docs.rs output, and
  Rust package-local docs checks.
- This repo must not hand-maintain mirrors of shared public docs, TypeScript
  docs, Python docs, public blog posts, or the internal dashboard.

## Clean-slate floor (cite, never violate — full text in DS-1 / `rules.jsonl`)

- **Sacred (L0.7):** topology declarative/serializable/inspectable · wave protocol is
  a public spec · wave protocol impl is **sync** · all fn go through the dispatcher.
- **8 verbs, closed set (D4):** `node` `graph` `batch` `state` + `producer` `derived`
  `effect` `mount`. Operators are `node` sugar, per-language, never in parity (D6/D24).
- **`ctx.up` / `ctx.down(msgs)` (D8):** one `msgs` array = one wave; may mix tiers.
  `ctx.up` is **control-tier only** (DIRTY/PAUSE/RESUME/INVALIDATE/TEARDOWN); DATA/
  RESOLVED/COMPLETE/ERROR are down-only (R-ctx-up). Handle = pure data
  `(pool_id, handle_id)`, no methods (D7).
- **7-tier const table (D34):** 0 START / 1 PAUSE·RESUME / 2 DIRTY / 3 DATA·RESOLVED /
  4 INVALIDATE / 5 COMPLETE·ERROR / 6 TEARDOWN; immediate `<3`, batch-deferred `≥3`.
  Closed set; adding a tier is a constitutional change.
- **graph = single-thread causal/concurrency domain (D22):** parallelism via pool
  callback or multi-graph + wire bridge; rewire intra-graph only. **The actor model
  is dropped.**
- **parity = behavioral conformance (D24):** only the substrate is in parity (driven
  via `/conformance`); operators/sugar/sources/inspection are per-language, never parity.
- **config dissolved (D26):** clock is graph-local (no global singleton); `messageTier`
  is a compile-time const table; `onMessage`/`onSubscribe` are substrate-fixed (D19).
- **Host fatal boundary (D431):** native bindings may use `HostBoundaryAbort` only to
  tunnel host process-control exceptions back to the host; it is not DATA, not graph
  `ERROR`, and not a protocol rollback/TEARDOWN/COMPLETE mechanism.
- **Forced (F-*):** F-SYNC-CORE (`dispatcher.invoke` sync `()`; async only in pools /
  wire bridge) · F-DISPATCH-ALL (no inline-fn bypass) · F-NO-IMPL-DEFINED (spec-locked
  or explicitly undefined) · F-NO-WEDGE-CUT · F-NO-LLM-ONLY · F-GRAPH-FIRST-API · F-PERF.

Durable values (memory `feedback_*`): no backward compat (pre-1.0) · no imperative
triggers · single source of truth · **no autonomous decisions** (surface spec↔code
conflicts, don't silently pick) · no implement without explicit approval · verify
premise before greenfield.

## Personal project governance

Before decision or work admission, design review, dispatch, QA, long-running goal progression,
live/provider/spend authorization, retry, or stalled-work recovery, load and follow the personal
`$project-governance` skill at `~/.codex/skills/project-governance/SKILL.md`. It governs
cross-project record and permission classification; Rust-local and root GraphReFly authorities remain
canonical for their own concerns. The concrete GraphReFly family mapping is proposed as
`graphrefly:B137`; do not invent that schema or migrate history before its separate approval, and do
not mint a D# merely for an attempt, rerun, receipt, incident, provider/model change, or spend grant.

## Rust-specific floor (clean-slate)

1. **Single-thread = `Rc<RefCell<…>>`, NOT `Arc<Mutex<…>>` (D22).** A graph is one
   causal/concurrency domain; the substrate is `!Send + !Sync`. The actor model and
   per-partition `ReentrantMutex` machinery of the old port are **gone** — they were
   the ~9.5× concurrency tax (memory `project_rust_perf_value_investigation`). Lock the
   `!Send`/`!Sync` boundary with `static_assertions` once types stabilize.
2. **No async runtime in the core (F-SYNC-CORE).** `dispatcher.invoke` is sync. `tokio`
   enters only behind the `LocalAsync` pool (D20) and the wire bridge.
3. **No `unsafe`. Anywhere.** `#![forbid(unsafe_code)]` at the crate root. Find a safe
   abstraction; if you truly cannot, escalate to spec-level discussion first.
4. **Error = unknown, single value generic (D31).** `Node<T>` carries one value
   generic; the error channel is `Box<dyn Error>` (`GraphError`). No typed-error
   combinatorial explosion.
5. **No `unwrap()`/`expect()` on user-facing paths.** `thiserror` enums for domain
   errors; `unwrap` only in tests or genuinely-impossible-by-construction paths (with a
   comment).
6. **`#[must_use]` on value-returning public fns; `clippy::pedantic` warn-by-default.**
   Allow per-need with a comment, never silently.
7. **Public ids behind newtypes** (`LockId`, `Handle`, future `NodeId`) — never raw ints.

## CSP-5 scope + layout

This branch now carries the clean-slate Rust package beyond the initial CSP-5
substrate: `crates/graphrefly` includes the protocol core, graph-layer Rust API,
operators, sources, storage helpers, app-infra helpers, environment adapters, and
wire-bridge helpers. See **`crates/graphrefly/CLEAN-SLATE.md`** for the detailed
module status and conformance map.

```
crates/
└── graphrefly/                # THE native shared clean-slate engine/library (D415)
    └── src/{protocol,node,dispatcher,ctx,batch}.rs
└── graphrefly-bindings-py/    # Python native binding foundation

The old port-model crate directories and old JS/WASM binding directories have
been deleted from the active tree. Use git history for archaeology; do not
develop against or reintroduce them.
```

## Workflow rules

- **spec-first** (F-NO-IMPL-DEFINED): any protocol-behavior change → amend
  `~/src/graphrefly` `spec/rules.jsonl` + `formal/*.tla` + `spec/conformance.jsonl`
  **before** code (`/spec-amend`). The substrate must satisfy the conformance scenarios.
- **decision-first + owner-first**: protocol, cross-runtime and cross-project locks stay in
  `graphrefly`; Rust-only package/implementation locks go to `graphrefly-rs:decisions/decisions.jsonl`.
  Never duplicate a body between ledgers; qualify cross-repo refs.
- **consistency gate**: `node ~/src/graphrefly/dashboard/build.mjs --check` after
  touching any spec/decision/plan jsonl.
  After owner-ledger changes run `npm --prefix ~/src/graphrefly run authority:check:workspace`.

## Commands

```bash
# Toolchain is managed via `mise`; do NOT assume `cargo`/`rustc` is on PATH in agent shells.
# Preferred:
mise exec -- cargo test  -p graphrefly-rs     # the clean-slate package; lib crate name is `graphrefly`
mise exec -- cargo build -p graphrefly-rs
mise exec -- cargo clippy -p graphrefly-rs --all-targets
mise exec -- cargo fmt --all

# Fallback when `mise` is unavailable in a constrained shell:
~/.cargo/bin/cargo test  -p graphrefly-rs
~/.cargo/bin/cargo build -p graphrefly-rs
~/.cargo/bin/cargo clippy -p graphrefly-rs --all-targets
~/.cargo/bin/cargo fmt --all

# Long gates: as the crate grows, run via the sanctioned runner — NEVER `;`-chain
# background cargo (memory feedback_no_chained_background_cargo / the Slice B-2
# incident). scripts/run-logged.sh + the `<<<RUN-LOGGED:DONE>>>` sentinel survive
# from the port era; rewire them to the single crate before relying on them.
```

## Durable principles

Follow the durable project values named in the clean-slate skills and
`~/src/graphrefly` authority docs, especially `feedback_no_autonomous_decisions`,
`feedback_no_implement_without_approval`, `feedback_single_source_of_truth`,
`feedback_long_command_observation`, `project_clean_slate_pivot`,
`project_rust_clean_slate_kickoff`, and
`feedback_three_gate_substrate_convergence`. Do not treat any TypeScript memory
directory as a Rust documentation authority.

New Rust-local decisions must satisfy ~/src/graphrefly/authority/README.md. The
root-origin-history ledger is relocation-only.
