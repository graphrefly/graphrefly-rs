//! `GraphReFly` Graph container — sugar layer over `graphrefly_core::Core`
//! with named namespace, mount/unmount composition, `describe()` JSON
//! introspection, and `observe()` sink-style message tap.
//!
//! # Status
//!
//! M2 complete: `Graph` container, mount/unmount, namespace resolution,
//! `describe()` / `observe()`, snapshot round-trip (see migration-status
//! D276+). Deferred presentation-tier observe modes stay binding-side;
//! open substrate gaps are listed in `docs/porting-deferred.md`.
//!
//! Binding-side / post-1.0 (not this crate): render formats (pretty /
//! mermaid / d2), DS-14 `mutate()` factory, cross-Core mount.
//!
//! # Crate lints
//!
//! `#![forbid(unsafe_code)]` per workspace policy.

#![forbid(unsafe_code)]
#![warn(rust_2018_idioms, unreachable_pub)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::missing_errors_doc)]

mod debug;
mod describe;
mod graph;
mod mount;
mod observe;
mod profile;
mod snapshot;

pub use debug::DebugBindingBoundary;
pub use describe::{
    DescribeSink, DescribeValue, EdgeDescribe, GraphDescribeOutput, NodeDescribe, NodeStatus,
    NodeTypeStr, ReactiveDescribeHandle,
};
pub use graph::{Graph, GraphInner, NameError, PathError, RemoveError, SignalKind};
pub use mount::{GraphRemoveAudit, MountError};
pub use observe::{GraphObserveAll, GraphObserveAllReactive, GraphObserveOne, ObserveSub};
pub use profile::{GraphProfileOptions, GraphProfileResult, Hotspots, NodeProfile, OrphanKind};
pub use snapshot::{
    GraphPersistSnapshot, NodeFactory, NodeSlice, NodeSnapshotStatus, SnapshotBuilder,
    SnapshotError,
};
