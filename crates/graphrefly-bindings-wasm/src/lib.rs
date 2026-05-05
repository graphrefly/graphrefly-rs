//! WebAssembly bindings for `GraphReFly`.
//!
//! Targets browsers + edge runtimes (Cloudflare Workers, Deno Deploy,
//! Bun edge). Built via `wasm-pack build`; published as
//! `@graphrefly/lite-wasm`, `@graphrefly/standard-wasm`.
//!
//! Trade-offs vs. the napi-rs distribution:
//!   - Smaller (~250 KB lite, ~900 KB standard) and runs in environments
//!     where native modules aren't loadable
//!   - Loses real OS thread parallelism (WASM threads exist but require
//!     COOP/COEP headers — clunky in practice)
//!   - No filesystem persistence (storage / structures crates are not
//!     exposed via this target)
//!
//! # Status
//!
//! Scaffold. Bindings land alongside napi-rs progression — same Rust
//! source, different compile target.

#![warn(rust_2018_idioms, unreachable_pub)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::missing_errors_doc)]

use wasm_bindgen::prelude::*;

/// Smoke export to verify the wasm-bindgen build chain works.
#[wasm_bindgen]
#[must_use]
pub fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}
