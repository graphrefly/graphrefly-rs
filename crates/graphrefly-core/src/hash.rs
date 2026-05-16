//! Synchronous SHA-256 hashing.
//!
//! GraphReFly's public substrate exposes `sha256Hex` (cache-key /
//! content-addressing helper used by versioning, snapshot paths, and the
//! presentation-layer adapters). The TS reference impl is async only
//! because `crypto.subtle.digest` returns a `Promise` — there is nothing
//! intrinsically async about SHA-256.
//!
//! Per the Rust-port invariant "No async runtime in Core" (CLAUDE.md
//! invariant 4 / D070 / D077), the hashing itself stays **fully
//! synchronous** here. The async-everywhere wrapping demanded by the
//! `Impl` parity contract is applied only at the napi boundary
//! (`graphrefly-bindings-js`), never in this crate.
//!
//! @module

use sha2::{Digest, Sha256};

/// Hex-encode the SHA-256 digest of `bytes`. Pure, synchronous, no
/// allocation beyond the 64-char output `String`. Mirrors the TS
/// `sha256Hex` byte semantics: the caller is responsible for encoding a
/// `string` input to UTF-8 bytes before calling (the napi boundary does
/// this).
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    hex::encode(digest)
}

#[cfg(test)]
mod tests {
    use super::sha256_hex;

    #[test]
    fn sha256_hex_known_vectors() {
        // RFC-known: SHA-256("") and SHA-256("abc").
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn sha256_hex_matches_ts_string_byte_semantics() {
        // The TS `sha256Hex("hello")` path UTF-8-encodes the string;
        // the Rust napi boundary does the same, so the digest must match
        // the well-known SHA-256("hello").
        assert_eq!(
            sha256_hex("hello".as_bytes()),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }
}
