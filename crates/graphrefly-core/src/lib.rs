//! `GraphReFly` handle-protocol core dispatcher.
//!
//! This crate is the heart of the protocol: dispatcher, message tiers,
//! batch coalescing, wave engine, dep tracking, equals-substitution,
//! first-run gate, PAUSE/RESUME with lockIds, INVALIDATE broadcast,
//! versioning lifecycle, atomic dep mutation (`set_deps`).
//!
//! It operates entirely on opaque [`HandleId`](handle::HandleId) integers —
//! user values `T` never enter the core. Per-language bindings (napi-rs for
//! JavaScript, pyo3 for Python, wasm-bindgen for WASM) hold the
//! value-to-handle registry. Equals-substitution under
//! `EqualsMode::Identity` is a `u64` compare with zero FFI; user-fn
//! invocation is the only mandatory boundary crossing per fn fire.
//!
//! # Status (2026-05-03)
//!
//! M1 first slice — warm-up modules ([`message`], [`handle`], [`clock`],
//! [`boundary`]). The dispatcher itself, batch engine, PAUSE/RESUME, and
//! `set_deps` follow incrementally. See
//! `~/src/graphrefly-ts/docs/implementation-plan.md` Phase 13.7 for the
//! full slice plan; reference impl at
//! `~/src/graphrefly-ts/src/__experiments__/handle-core/`.
//!
//! # Safety
//!
//! No `unsafe` is permitted in this crate or anywhere in the workspace.
//! Enforced by `#![forbid(unsafe_code)]` at the root. See `CLAUDE.md`
//! Rust invariant 1.

#![forbid(unsafe_code)]
#![cfg_attr(not(feature = "std"), no_std)]
#![warn(rust_2018_idioms, unreachable_pub)]
#![warn(clippy::pedantic)]
// `clippy::pedantic` includes a few that are too noisy for this codebase.
#![allow(
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    // Some Core methods take `&self` for architectural consistency (they're
    // conceptually Core methods even if they don't read self state directly,
    // since the state lives in CoreState behind self.state.lock()). The lint
    // would force them into associated fns, breaking the consistent API.
    clippy::unused_self,
    // doc_markdown false-positives on protocol identifiers like `T`, `JS`, etc.
    clippy::doc_markdown,
)]

mod batch;
pub mod boundary;
pub mod clock;
pub mod handle;
pub mod hash;
pub mod mailbox;
pub mod message;
pub mod node;
pub(crate) mod op_state;
pub mod owned;
#[cfg(feature = "tokio")]
pub mod timer;
pub mod topology;

pub use batch::BatchGuard;

pub use boundary::{BindingBoundary, CleanupTrigger, DepBatch, FnEmission, FnResult};
pub use clock::{monotonic_ns, wall_clock_ns};
pub use handle::{FnId, HandleId, LockId, NodeId, SerializationGroupId, NO_HANDLE};
pub use hash::sha256_hex;
pub use mailbox::{CoreMailbox, DeferFn, DeferQueue, MailboxOp};
pub use message::{Message, Messages};
pub use node::{
    Core, CoreFull, DeferredProducerOp, EqualsMode, NodeFnOrOp, NodeKind, NodeOpts,
    NodeRegistration, OperatorOp, OperatorOpts, PausableMode, PauseError, RegisterError,
    ResumeReport, SetDepsError, SetGroupError, SetPausableModeError, Sink, SubscribeError,
    SubscriptionId, TerminalKind, UpError,
};
pub use owned::OwnedCore;
pub use topology::{TopologyEvent, TopologySink, TopologySubscriptionId};
