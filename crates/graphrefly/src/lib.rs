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
//! ## CSP-5 scope (what this crate builds)
//!
//! The **substrate only**: protocol + node + dispatcher (LocalSync + LocalAsync
//! pools) + ctx + batch + rewire. The graph layer / 8-verb sugar / operators /
//! inspection are later per-language phases (CSP-2-rs equivalents) and are NOT in
//! this skeleton. See `CLEAN-SLATE.md` for the conformance target map.
//!
//! > **Status:** [`protocol`], [`node`], [`dispatcher`], [`ctx`] are implemented
//! > (kernel + control/terminal slice — C-3/C-5/C-6 green); [`batch`] is still a
//! > contract stub, and LocalAsync/rewire are later slices. See `CLEAN-SLATE.md`
//! > for the per-module status + conformance target map.

#![forbid(unsafe_code)]

pub mod batch;
pub mod ctx;
pub mod dispatcher;
pub mod node;
pub mod protocol;

pub use ctx::{Ctx, DepRecord};
pub use dispatcher::{default_dispatcher, Dispatcher};
pub use node::{Core, Node, Status};
pub use protocol::{AnyValue, GraphError, Handle, LockId, Message, Tier, Wave};
