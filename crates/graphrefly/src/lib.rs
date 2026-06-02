//! # GraphReFly — Rust clean-slate substrate (`@graphrefly/rust`)
//!
//! Reactive **universal reduction layer**: high fan-in/fan-out → information
//! reduction → push. Not LLM-limited; performance first-class (D1).
//!
//! ## Authority — the truth lives in `~/src/graphrefly` (branch `clean-slate`)
//!
//! This crate is the Rust **implementation**. The language-neutral authority —
//! protocol spec, decisions, formal model, conformance scenarios — is in the
//! `graphrefly` design repo. On any disagreement, **that repo wins**.
//!
//! | Concern | Source of truth |
//! |---|---|
//! | Decisions (D#) | `~/src/graphrefly/decisions/decisions.jsonl` |
//! | Protocol rules (宪法) | `~/src/graphrefly/spec/rules.jsonl` |
//! | Conformance (parity) | `~/src/graphrefly/spec/conformance.jsonl` |
//! | Formal model | `~/src/graphrefly/formal/*.tla` |
//! | Phase plan | `~/src/graphrefly/plan/phases.jsonl` (this crate = CSP-5) |
//!
//! Sibling self-contained packages: `@graphrefly/ts` (`~/src/graphrefly-ts`),
//! `@graphrefly/py` (`~/src/graphrefly-py`). Cross-language = a coarse wire
//! bridge, never in-process (D32, no cross-language peer-deps).
//!
//! ## Floor (cite, never violate)
//!
//! - **D22** — a graph is a single-thread causal/concurrency domain. This crate
//!   is therefore `!Send + !Sync`: state lives behind `Rc<RefCell<…>>`, **not**
//!   `Arc<Mutex<…>>`. The actor model is dropped. Parallelism = pool callback or
//!   multi-graph + wire bridge.
//! - **F-SYNC-CORE** — the wave-protocol core is synchronous; `dispatcher.invoke`
//!   is `fn(&Ctx)` returning `()`. Async lives only in pools (LocalAsync) and the
//!   wire bridge.
//! - **F-DISPATCH-ALL** — every node fn goes through the dispatcher; no inline-fn
//!   bypass.
//! - **D4** — 8-verb closed set (node/graph/batch/state + producer/derived/effect/
//!   mount). Operators are `node` sugar, per-language, never in parity (D6/D24).
//! - **D8** — the fn boundary is `ctx.up(msgs)` / `ctx.down(msgs)`; one `msgs`
//!   array = one wave. `ctx.up` is control-tier only (R-ctx-up).
//!
//! ## Clean-slate scope (what this crate builds)
//!
//! A self-contained Rust package (D32): protocol + node + dispatcher (LocalSync +
//! LocalAsync pools) + ctx + batch + rewire, plus the B53 graph-layer MVP
//! (Graph/graph, graph-owned 8-verb sugar, find/describe/observe first cut).
//! Operators remain per-language graph-layer sugar (D6/D24) and are re-derived
//! after this MVP rather than shimmed from the retired port model.
//!
//! > **Status:** [`protocol`], [`node`], [`dispatcher`], [`ctx`] are implemented
//! > (kernel + control/terminal + async + rewire + dep-terminal + `ctx.rewire_next`
//! > slices, plus pull/routed-up, terminal/later-async catch-up, and batch). [`graph`]
//! > adds the first product-completeness layer (B53). The Rust conformance arm is green
//! > for **C-2..C-22 except C-1**; **C-1** remains wire-bridge-blocked. See
//! > `CLEAN-SLATE.md` for the per-module status + conformance target map.

#![forbid(unsafe_code)]

pub mod batch;
pub mod ctx;
pub mod dispatcher;
pub mod graph;
pub mod node;
pub mod protocol;

pub use batch::{batch, BatchCtx};
pub use ctx::{Ctx, DeferredCtx, DepRecord, DepTerminal};
pub use dispatcher::{default_dispatcher, Dispatcher, PoolKind};
pub use graph::{
    graph, graph_opts, DescribeEdge, DescribeNode, DescribeSnapshot, DescribeValue, Graph,
    GraphNode, GraphNodeOpts, GraphObserver, GraphOptions, ObserveEvent, ObserveMessage,
    ObserveStream, Values,
};
pub use node::{Core, Node, NodeOpts, Pausable, Status};
pub use protocol::{AnyValue, GraphError, Handle, LockId, Message, Tier, Wave};
