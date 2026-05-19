//! Optional binding-side handle → debug value rendering (F sub-slice,
//! 2026-05-10).
//!
//! [`Graph::describe`](crate::Graph::describe) surfaces node values as
//! `DescribeValue::Handle(HandleId)` — raw u64 view, no FFI. Consumers
//! who want a TS-style `value: T` view in the JSON output pass an
//! [`DebugBindingBoundary`] impl to
//! [`Graph::describe_with_debug`](crate::Graph::describe_with_debug);
//! that variant renders each handle via the trait before serializing.
//!
//! # Why a separate trait (not on `BindingBoundary`)
//!
//! Keeping the hot-path FFI trait ([`graphrefly_core::BindingBoundary`])
//! free of debug / introspection methods means:
//!
//! - Minimal bindings (test stubs, perf benches without describe
//!   needs) don't have to implement debug-rendering. They impl
//!   `BindingBoundary` only.
//! - `graphrefly-core` stays serde-free. The debug rendering returns
//!   `serde_json::Value`, which is a `graphrefly-graph` concern
//!   (where describe-output JSON already lives).
//! - Production bindings (napi-rs, pyo3, wasm-bindgen) implement both
//!   traits side-by-side. The describe-rendering path is opt-in via
//!   `describe_with_debug`; default `describe` callers pay no cost.
//!
//! The trait is intentionally minimal — one method, no default impl.
//! Bindings are expected to look up the registered `T` for the
//! handle and project it via `serde_json` (or a custom translator
//! for non-serde-friendly value types).

use graphrefly_core::HandleId;

/// Render handles into JSON-shaped debug values.
///
/// Bindings that want to support
/// [`Graph::describe_with_debug`](crate::Graph::describe_with_debug)
/// implement this trait alongside
/// [`graphrefly_core::BindingBoundary`]. The two traits cover the
/// two FFI surfaces:
///
/// - `BindingBoundary` is the hot-path trait — invoked per fn-fire,
///   per equals check, per handle refcount op. Bindings ALWAYS impl
///   this.
/// - `DebugBindingBoundary` is the cold-path trait — invoked once per
///   named node per `describe_with_debug` call. Bindings impl this
///   ONLY if they want their `Graph::describe()` output to surface
///   `value: T` shapes instead of raw `value: <u64>` handles.
///
/// # Implementation guidance
///
/// For a typical binding that stores `HashMap<HandleId, T>` where
/// `T: Serialize`, the implementation is one line:
///
/// ```ignore
/// impl DebugBindingBoundary for MyBinding {
///     fn handle_to_debug(&self, h: HandleId) -> serde_json::Value {
///         self.values.borrow_mut().get(&h)
///             .map(|v| serde_json::to_value(v).unwrap_or(serde_json::Value::Null))
///             .unwrap_or(serde_json::Value::Null)
///     }
/// }
/// ```
///
/// For value types that don't implement `serde::Serialize` (e.g.,
/// JS function handles in napi-rs), the binding can return a
/// descriptor object like `{ "type": "function", "fn_id": 42 }` or
/// fall back to `Value::String(format!("<opaque:{h:?}>"))`.
///
/// # Thread safety
///
/// `Send + Sync` per the same rationale as `BindingBoundary`: the
/// Core dispatcher may be cloned across threads, so any binding it
/// holds must also be thread-safe.
pub trait DebugBindingBoundary: Send + Sync {
    /// Render `handle` as a JSON value. Implementations typically
    /// look the handle up in the binding's value registry and
    /// serialize via `serde_json::to_value(&value)`. Return
    /// `serde_json::Value::Null` for unknown / dangling handles
    /// (the call may race with concurrent `release_handle`
    /// finalizers).
    fn handle_to_debug(&self, handle: HandleId) -> serde_json::Value;
}
