//! `GraphReFly` Graph container — sugar layer over `graphrefly_core::Core`
//! with named namespace, mount/unmount composition, `describe()` JSON
//! introspection, and `observe()` sink-style message tap.
//!
//! # Status (M2 Slice E+ — 2026-05-05)
//!
//! Read-side introspection + composition slice. Lands the canonical-spec
//! `Graph` surface for `state` / `derived` / `dynamic` named sugar
//! (§3.9), `add` / `node` / `name_of` / `try_resolve` namespace (§3.5),
//! `mount` / `mount_new` / `mount_with` / `unmount` / `ancestors`
//! composition (§3.4), `destroy` / `signal_invalidate` lifecycle (§3.7),
//! `describe()` JSON form (§3.6 + Appendix B), and `observe()` /
//! `observe_all()` default sink-style tap (§3.6.2).
//!
//! Hard-breaks the M2 starter (Slice D) sugar API: `state(name, initial)`
//! replaces `state(initial)`, etc. Pre-1.0 — no compat shims (per
//! `feedback_no_backward_compat.md`).
//!
//! Out of scope (subsequent slices):
//!
//! - **Reactive `describe()` / `observe()`** — `Node<GraphDescribeOutput>`
//!   and `Node<ObserveChangeset>` modes (§3.6.1 / §3.6.2 reactive
//!   variants). Need a Core-level topology-change notification primitive.
//! - **Async-iterable `observe()` modes** — `structured` / `timeline` /
//!   `causal` / `derived` opts.
//! - **`describe()` non-JSON formats** — pretty / mermaid / d2 / stage-log.
//! - **`describe({ explain })` / `describe({ reachable })`** — causal
//!   chain + reachability walks.
//! - **Snapshot / restore + content addressing** — V1 lazy CID / V2
//!   schema validation / V3 caps + cross-graph refs (§3.8 + Phase 6).
//! - **`mutate()` factory** — DS-14 RAII + audit append substrate.
//! - **Cross-Core (multi-binding) mount** — open question 1 in
//!   `archive/docs/SESSION-rust-port-architecture.md` Part 6; post-M6.
//! - **Meta annotations + versioning fields** in describe output —
//!   need metadata-storage primitive on Core.
//!
//! # Crate lints
//!
//! `#![forbid(unsafe_code)]` per workspace policy.

#![forbid(unsafe_code)]
#![warn(rust_2018_idioms, unreachable_pub)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::missing_errors_doc)]

mod describe;
mod graph;
mod mount;
mod observe;

pub use describe::{EdgeDescribe, GraphDescribeOutput, NodeDescribe, NodeStatus, NodeTypeStr};
pub use graph::{Graph, NameError};
pub use mount::{MountError, RemoveAudit};
pub use observe::{GraphObserveAll, GraphObserveOne};
