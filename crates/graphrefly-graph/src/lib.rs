//! `GraphReFly` Graph container, describe/observe, content-addressed snapshots.
//!
//! Phase 6.1–6.3 (V1–V3 lazy CID, V2 schema validation, V3 caps + refs)
//! and Phase 14 (codec envelope evolution for delta-aware codecs) live in
//! this crate. Pairs with the IPLD ecosystem: dag-cbor + blake3 CIDs
//! make snapshots first-class IPLD documents — free interop with IPFS,
//! Iroh, Ceramic, and libp2p.
//!
//! # Status
//!
//! Scaffold. Implementation lands during Milestone 2 of the Rust port.
//!
//! # Module layout (planned)
//!
//! - `graph` — Graph container; mount, activate, dispatch coordinator
//! - `describe` — topology introspection (mermaid, d2, ascii, json)
//! - `observe` — message tap; one source of truth for live data flow
//! - `snapshot` — serialize / restore entry points
//! - `codec/dag_cbor` — canonical content-addressed codec
//! - `codec/cbor` — loose CBOR for snapshots that aren't content-addressed
//! - `codec/json` — debug / inspection format
//! - `content_id` — CID computation, V1 lazy CID, V3 refs

#![warn(rust_2018_idioms, unreachable_pub)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::missing_errors_doc)]

#[cfg(test)]
mod tests {
    #[test]
    fn scaffold_compiles() {}
}
