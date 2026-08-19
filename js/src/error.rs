//! The one place a Rust error (or a binding's own message) becomes the
//! `JsValue` thrown across the wasm boundary, so a given operation reports the
//! same message shape wherever it is reached from.

use std::fmt::Display;

use wasm_bindgen::JsValue;

/// A failure as the JS exception value: the message alone.
pub(crate) fn js_err(message: impl Display) -> JsValue {
    JsValue::from_str(&message.to_string())
}

/// [`js_err`] labelled with the operation that failed: `"{context}: {cause}"`.
/// Use it only where the cause alone would not say which step failed.
pub(crate) fn js_err_ctx(context: &str, cause: impl Display) -> JsValue {
    JsValue::from_str(&format!("{}: {}", context, cause))
}
