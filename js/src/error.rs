//! The one place a Rust error (or a binding's own message) becomes the
//! `JsValue` thrown across the wasm boundary, so a given operation reports the
//! same message shape wherever it is reached from.

use std::fmt::Display;

use wasm_bindgen::JsValue;

/// A failure as the JS exception value: a `js_sys::Error` carrying the
/// message alone.
pub(crate) fn js_err(message: impl Display) -> JsValue {
    js_sys::Error::new(&message.to_string()).into()
}

/// [`js_err`] labelled with the operation that failed: `"{context}: {cause}"`.
/// Use it only where the cause alone would not say which step failed.
pub(crate) fn js_err_ctx(context: &str, cause: impl Display) -> JsValue {
    js_err(format!("{}: {}", context, cause))
}

/// An argument of the wrong type or shape as the JS exception value: a
/// `js_sys::TypeError` carrying the message alone.
pub(crate) fn js_type_err(message: impl Display) -> JsValue {
    js_sys::TypeError::new(&message.to_string()).into()
}

/// A number outside the range its argument takes as the JS exception value:
/// a `js_sys::RangeError` carrying the message alone.
pub(crate) fn js_range_err(message: impl Display) -> JsValue {
    js_sys::RangeError::new(&message.to_string()).into()
}
