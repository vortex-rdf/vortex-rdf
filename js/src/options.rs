//! Build-time options: resolving the JS `BuildOptions` object into core
//! strategies, the RDF format-name table, and the single place the builder is
//! monomorphized.

use futures::Stream;
use js_sys::Reflect;
use oxrdfio::RdfFormat;
use vortex_rdf_core::common::formats::{format_from_name, supported_format_names};
use vortex_rdf_core::error::Result as CoreResult;
use vortex_rdf_core::store::{BuiltArray, RawQuad};
use vortex_rdf_core::{
    IndexType, Indexes, LayoutStrategy, SortedInMemoryBuilder, VortexArrayBuilder,
};
use wasm_bindgen::prelude::*;

use crate::error::{js_err, js_err_ctx};

pub(crate) fn parse_format(format_name: &str) -> Result<RdfFormat, JsValue> {
    format_from_name(format_name).ok_or_else(|| {
        // Quote core's own name list, so this message cannot understate what
        // `format_from_name` actually accepts.
        let supported = supported_format_names()
            .iter()
            .map(|name| format!("'{name}'"))
            .collect::<Vec<_>>()
            .join(", ");
        js_err(format!(
            "Unsupported format: {format_name}. Supported formats are {supported}."
        ))
    })
}

/// Build-time configuration resolved from the JS `BuildOptions` object.
pub(crate) struct BuildConfig {
    pub(crate) layout: LayoutStrategy,
    pub(crate) indexes: Indexes,
}

impl Default for BuildConfig {
    fn default() -> Self {
        Self {
            // Dictionary is the JS default: it is the most compact layout and
            // backs the zero-copy code-based read model (integer `.equals`).
            layout: LayoutStrategy::Dictionary,
            indexes: Vec::new(),
        }
    }
}

/// Run the quad stream through the builder.
///
/// Every entry point (`fromString`, `fromQuads`, `rdf_to_vortex`) builds
/// through here. WebAssembly has no filesystem for the out-of-core strategy's
/// spill runs, so the in-memory sort is the one builder compiled in.
pub(crate) async fn build_array(
    quads: impl Stream<Item = CoreResult<RawQuad>> + Unpin + Send + 'static,
    config: BuildConfig,
) -> Result<BuiltArray, JsValue> {
    let BuildConfig { layout, indexes } = config;
    SortedInMemoryBuilder::build_vortex_array(Box::new(quads), layout, indexes)
        .await
        .map_err(|e| js_err_ctx("Vortex build error", e))
}

/// Resolve the optional JS build options. Accepts `undefined`/`null` (all
/// defaults) or a `BuildOptions` object.
pub(crate) fn parse_build_options(options: JsValue) -> Result<BuildConfig, JsValue> {
    if options.is_null() || options.is_undefined() {
        return Ok(BuildConfig::default());
    }

    let mut config = BuildConfig::default();
    if let Some(name) = get_string_option(&options, "layout")? {
        config.layout = parse_layout(&name)?;
    }
    let indexes = Reflect::get(&options, &"indexes".into())
        .map_err(|_| js_err("Could not read the 'indexes' option"))?;
    if !indexes.is_null() && !indexes.is_undefined() {
        if !js_sys::Array::is_array(&indexes) {
            return Err(js_err("Option 'indexes' must be an array"));
        }
        config.indexes = js_sys::Array::from(&indexes)
            .iter()
            .map(|value| match value.as_string() {
                Some(name) => parse_index(&name),
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

// The strategy vocabularies live on core's `FromStr` impls — the canonical
// kebab-case names shared by every frontend, and nothing else. These wrappers
// only shape the failure into a JS exception.

fn parse_layout(name: &str) -> Result<LayoutStrategy, JsValue> {
    name.parse().map_err(js_err)
}

fn parse_index(name: &str) -> Result<IndexType, JsValue> {
    name.parse().map_err(js_err)
}
