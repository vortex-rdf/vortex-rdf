//! Build-time options: resolving the JS `BuildOptions` object into core
//! strategies, the RDF format-name table, and the single place the builder is
//! monomorphized.

use futures::Stream;
use js_sys::Reflect;
use oxrdfio::RdfFormat;
use vortex_rdf_core::common::formats::{format_from_name, supported_format_names};
use vortex_rdf_core::{
    BuiltArray, DictForm, IndexType, Indexes, LayoutStrategy, QuadColumn, RawQuad,
    Result as CoreResult, SortedInMemoryBuilder, TermEncoding, VortexArrayBuilder,
};
use wasm_bindgen::prelude::*;

use crate::error::{js_err, js_err_ctx};

pub(crate) fn parse_format(format_name: &str) -> Result<RdfFormat, JsValue> {
    format_from_name(format_name).ok_or_else(|| {
        js_err(format!(
            "unknown RDF format {format_name:?}; expected one of: {}",
            supported_format_names().join(", ")
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
            // Dictionary is the default layout in every vortex-rdf frontend: the
            // most compact layout, and it backs the code-based read model.
            layout: LayoutStrategy::Dictionary,
            indexes: Vec::new(),
        }
    }
}

/// Run the quad stream through the builder.
///
/// Every entry point (`fromString`, `fromQuads`, `serializeRdf`) builds
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
        config.indexes = js_sys::Array::from(&indexes)
            .iter()
            .map(|value| match value.as_string() {
                Some(name) => name.parse::<IndexType>().map_err(js_err),
                None => Err(js_err("Option 'indexes' must contain strings")),
            })
            .collect::<Result<Indexes, JsValue>>()?;
    }
    Ok(config)
}

/// The resident form of an adopted store's term dictionary when the caller
/// names none.
const DEFAULT_DICT_FORM: DictForm = DictForm::AsWritten;

/// Resolve the optional JS `OpenOptions` object behind `fromBytes`:
/// `dictionary` (a dictionary-form name — `as-written` keeps the file's
/// FSST chunks, `plaintext` decodes the column once into one canonical
/// form). Accepts `undefined`/`null` for the default; the vocabulary is
/// core's `FromStr`, so parse failures carry core's messages.
pub(crate) fn parse_open_options(options: JsValue) -> Result<DictForm, JsValue> {
    if options.is_null() || options.is_undefined() {
        return Ok(DEFAULT_DICT_FORM);
    }
    match get_string_option(&options, "dictionary")? {
        Some(name) => name.parse().map_err(js_err),
        None => Ok(DEFAULT_DICT_FORM),
    }
}

/// Resolve the optional JS `ArrowOptions` object behind `matchArrowIPC`:
/// `encoding` (a term-encoding name, default `codes`) and `projection` (quad
/// column names in the order to ship them, default all four). Accepts
/// `undefined`/`null` for the defaults; the vocabularies are core's
/// `FromStr` impls, so parse failures carry core's messages.
pub(crate) fn parse_arrow_options(
    options: JsValue,
) -> Result<(TermEncoding, Option<Vec<QuadColumn>>), JsValue> {
    if options.is_null() || options.is_undefined() {
        return Ok((TermEncoding::Codes, None));
    }
    let encoding = match get_string_option(&options, "encoding")? {
        Some(name) => name.parse().map_err(js_err)?,
        None => TermEncoding::Codes,
    };
    let projection = Reflect::get(&options, &"projection".into())
        .map_err(|_| js_err("Could not read the 'projection' option"))?;
    if projection.is_null() || projection.is_undefined() {
        return Ok((encoding, None));
    }
    if !js_sys::Array::is_array(&projection) {
        return Err(js_err("Option 'projection' must be an array"));
    }
    let columns = js_sys::Array::from(&projection)
        .iter()
        .map(|value| match value.as_string() {
            Some(name) => name.parse::<QuadColumn>().map_err(js_err),
            None => Err(js_err("Option 'projection' must contain strings")),
        })
        .collect::<Result<Vec<_>, JsValue>>()?;
    Ok((encoding, Some(columns)))
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
