// napi-rs build script. Generates the platform-specific binding glue at
// compile time. See https://napi.rs/docs/build-process for details.

extern crate napi_build;

fn main() {
    napi_build::setup();
}
