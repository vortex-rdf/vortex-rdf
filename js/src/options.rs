//! Build-time options: resolving the JS `BuildOptions` object into core
//! strategies.

use js_sys::Reflect;
use vortex_rdf_core::{IndexType, Indexes, LayoutStrategy};
use wasm_bindgen::prelude::*;

use crate::error::js_err;

/// Build-time configuration resolved from the JS `BuildOptions` object.
pub(crate) struct BuildConfig {
    pub(crate) layout: LayoutStrategy,
    pub(crate) indexes: Indexes,
}

impl Default for BuildConfig {
    fn default() -> Self {
        Self {
            // Dictionary is the default layout in every vortex-rdf frontend: the
            // most compact layout, and it backs the code-based read model.
            layout: LayoutStrategy::Dictionary,
            indexes: Vec::new(),
        }
    }
}

/// Resolve the optional JS build options. Accepts `undefined`/`null` (all
/// defaults) or a `BuildOptions` object. The strategy vocabularies live on
/// core's `FromStr` impls (the canonical kebab-case names shared by every
/// frontend); parse failures become JS exceptions.
pub(crate) fn parse_build_options(options: JsValue) -> Result<BuildConfig, JsValue> {
    if options.is_null() || options.is_undefined() {
        return Ok(BuildConfig::default());
    }

    let mut config = BuildConfig::default();
    if let Some(name) = get_string_option(&options, "layout")? {
        config.layout = name.parse().map_err(js_err)?;
    }
    let indexes = Reflect::get(&options, &"indexes".into())
        .map_err(|_| js_err("Could not read the 'indexes' option"))?;
    if !indexes.is_null() && !indexes.is_undefined() {
        if !js_sys::Array::is_array(&indexes) {
            return Err(js_err("Option 'indexes' must be an array"));
        }
        config.indexes = indexes
            .unchecked_ref::<js_sys::Array>()
            .iter()
            .map(|value| match value.as_string() {
                Some(name) => name.parse::<IndexType>().map_err(js_err),
                None => Err(js_err("Option 'indexes' must contain strings")),
            })
            .collect::<Result<Indexes, JsValue>>()?;
    }
    Ok(config)
}

/// Read an optional string field, erroring if present but not a string.
fn get_string_option(options: &JsValue, key: &str) -> Result<Option<String>, JsValue> {
    let value = Reflect::get(options, &key.into())
        .map_err(|_| js_err(format!("Could not read the '{}' option", key)))?;
    if value.is_null() || value.is_undefined() {
        return Ok(None);
    }
    match value.as_string() {
        Some(name) => Ok(Some(name)),
        None => Err(js_err(format!("Option '{}' must be a string", key))),
    }
}
