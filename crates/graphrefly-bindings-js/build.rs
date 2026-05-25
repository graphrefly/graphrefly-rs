// napi-rs build script. Generates the platform-specific binding glue at
// compile time. See https://napi.rs/docs/build-process for details.

extern crate napi_build;

fn main() {
    napi_build::setup();

    // D289 / D288 — test-build link discipline for cargo nextest.
    //
    // `napi_build::setup()` adds `-Wl,-undefined,dynamic_lookup` on
    // macOS for the cdylib output (napi extern symbols like
    // `napi_call_threadsafe_function` are resolved at runtime by
    // Node.js when the `.node` is loaded; they have no definition at
    // link time). Cargo's TEST binary is a separate compilation unit
    // (not a cdylib) and the dynamic_lookup arg is NOT propagated to
    // it — so `cargo nextest run -p graphrefly-bindings-js` fails
    // with "Undefined symbols for architecture arm64" until we
    // mirror the link arg for the test profile too.
    //
    // Safe because the `batch_bindings` cargo tests never invoke the
    // TSFN / napi-callback code paths — they construct `BenchCore`
    // (CoreActor::spawn doesn't touch napi), subscribe substrate
    // sinks directly (no TSFN), and drive `BenchBatchContext` (the
    // BatchOp loop doesn't TSFN either). A test that DID hit those
    // paths would crash at runtime with a dynamic-link error — which
    // is the correct failure mode for an unintended napi-touching
    // test in a JS-free environment.
    //
    // The `cargo:rustc-cdylib-link-arg-bins=` form is the cdylib
    // arg napi_build sets; for test bins we need the plain
    // `cargo:rustc-link-arg-tests=` form (cargo since 1.56).
    // `napi_build::setup()` adds the link arg only for the cdylib
    // output (via `cargo:rustc-cdylib-link-arg=...`). Mirror it for
    // ALL other binaries cargo emits via the `rlib` crate-type (see
    // Cargo.toml) — test bins, benches, examples. The generic
    // `cargo:rustc-link-arg=...` directive applies to bins/tests/
    // benches/examples (cargo book: build scripts → "Outputs of the
    // Build Script"). On Linux/Android we use
    // `--unresolved-symbols=ignore-all` instead of dynamic_lookup
    // (GNU ld syntax differs from Apple ld64).
    let target = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    match target.as_str() {
        "macos" | "ios" => {
            println!("cargo:rustc-link-arg=-Wl,-undefined,dynamic_lookup");
        }
        "linux" | "android" => {
            println!("cargo:rustc-link-arg=-Wl,--unresolved-symbols=ignore-all");
        }
        _ => {}
    }
}
