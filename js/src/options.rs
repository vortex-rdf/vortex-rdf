//! Build-time options: resolving the JS `BuildOptions` object into core
//! strategies, the RDF format-name table, and the single place the builder is
//! monomorphized.

use futures::Stream;
use js_sys::Reflect;
use oxrdfio::RdfFormat;
use vortex_rdf_core::common::formats::{format_from_name, supported_format_names};
use vortex_rdf_core::{
    BuiltArray, DictForm, IndexType, Indexes, Keep, LayoutStrategy, QuadColumn, RawQuad,
    Result as CoreResult, SortedInMemoryBuilder, TermEncoding, VortexArrayBuilder,
};
use wasm_bindgen::JsCast;
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
const DEFAULT_DICT_FORM: DictForm = DictForm::Plaintext;

/// Resolve the optional JS `OpenOptions` object behind `fromBytes`:
/// `dictionary` (a dictionary-form name — `plaintext`, the default, decodes
/// the column once into one canonical form; `as-written` keeps the file's
/// FSST chunks). Accepts `undefined`/`null` for the default; the vocabulary
/// is core's `FromStr`, so parse failures carry core's messages.
pub(crate) fn parse_open_options(options: JsValue) -> Result<DictForm, JsValue> {
    if options.is_null() || options.is_undefined() {
        return Ok(DEFAULT_DICT_FORM);
    }
    match get_string_option(&options, "dictionary")? {
        Some(name) => name.parse().map_err(js_err),
        None => Ok(DEFAULT_DICT_FORM),
    }
}

/// The read behind an Arrow export, resolved from the JS `ArrowOptions`
/// object: the export shape (`encoding`, `projection`) and the narrowing the
/// store applies before any row is gathered (`keep` constraints in column
/// order, then the `offset`/`limit` row window).
pub(crate) struct ArrowRead {
    pub(crate) encoding: TermEncoding,
    pub(crate) projection: Option<Vec<QuadColumn>>,
    pub(crate) keep: Vec<(QuadColumn, Keep)>,
    pub(crate) limit: Option<usize>,
    pub(crate) offset: usize,
}

impl Default for ArrowRead {
    fn default() -> Self {
        Self {
            encoding: TermEncoding::Codes,
            projection: None,
            keep: Vec::new(),
            limit: None,
            offset: 0,
        }
    }
}

impl ArrowRead {
    pub(crate) fn windowed(&self) -> bool {
        self.limit.is_some() || self.offset > 0
    }
}

/// Resolve the optional JS `ArrowOptions` object behind `matchArrowFFI`:
/// `encoding` (a term-encoding name, default `codes`), `projection` (quad
/// column names in the order to ship them, default all four), `keep` (an
/// object keyed by quad column: a `Uint32Array` or array of codes for a code
/// set, `{lo, hi}` for a half-open code range), `limit` and `offset`
/// (non-negative integers; a `limit` past the address space means no limit).
/// Accepts `undefined`/`null` for the defaults; the vocabularies are core's
/// `FromStr` impls, so parse failures carry core's messages.
pub(crate) fn parse_arrow_options(options: JsValue) -> Result<ArrowRead, JsValue> {
    let mut read = ArrowRead::default();
    if options.is_null() || options.is_undefined() {
        return Ok(read);
    }
    if let Some(name) = get_string_option(&options, "encoding")? {
        read.encoding = name.parse().map_err(js_err)?;
    }
    if let Some(projection) = get_option(&options, "projection")? {
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
        read.projection = Some(columns);
    }
    if let Some(keep) = get_option(&options, "keep")? {
        read.keep = parse_keep(&keep)?;
    }
    if let Some(limit) = get_option(&options, "limit")? {
        read.limit = Some(parse_count(&limit, "limit")?);
    }
    if let Some(offset) = get_option(&options, "offset")? {
        read.offset = parse_count(&offset, "offset")?;
    }
    Ok(read)
}

/// The `keep` option — an object keyed by quad column name — as core
/// constraints, in column order. A `null`/`undefined` entry constrains nothing.
fn parse_keep(keep: &JsValue) -> Result<Vec<(QuadColumn, Keep)>, JsValue> {
    if !keep.is_object() || js_sys::Array::is_array(keep) {
        return Err(js_err(
            "Option 'keep' must be an object keyed by quad column",
        ));
    }
    let mut constraints = Vec::new();
    for key in js_sys::Object::keys(keep.unchecked_ref::<js_sys::Object>()).iter() {
        let name = key.as_string().unwrap_or_default();
        let column: QuadColumn = name.parse().map_err(js_err)?;
        let value = Reflect::get(keep, &key)
            .map_err(|_| js_err(format!("Could not read the keep entry for column {name:?}")))?;
        if value.is_null() || value.is_undefined() {
            continue;
        }
        constraints.push((column, parse_keep_value(&name, &value)?));
    }
    constraints.sort_by_key(|(column, _)| column.index());
    Ok(constraints)
}

/// One `keep` entry: a `Uint32Array` or an array of codes (a code set, any
/// order, duplicates allowed), or a `{lo, hi}` object (the half-open code
/// range `lo..hi`).
fn parse_keep_value(name: &str, value: &JsValue) -> Result<Keep, JsValue> {
    if let Some(codes) = value.dyn_ref::<js_sys::Uint32Array>() {
        return Ok(Keep::set(codes.to_vec()));
    }
    if js_sys::Array::is_array(value) {
        let codes = js_sys::Array::from(value)
            .iter()
            .map(|code| {
                code_of(&code).ok_or_else(|| {
                    js_err(format!(
                        "keep codes for column {name:?} must be integers in the u32 code space"
                    ))
                })
            })
            .collect::<Result<Vec<u32>, JsValue>>()?;
        return Ok(Keep::set(codes));
    }
    if value.is_object() {
        let bound = |key: &str| {
            Reflect::get(value, &key.into())
                .ok()
                .and_then(|v| code_of(&v))
        };
        if let (Some(lo), Some(hi)) = (bound("lo"), bound("hi")) {
            return Ok(Keep::range(lo, hi));
        }
    }
    Err(js_err(format!(
        "keep for column {name:?} must be a Uint32Array, an array of codes, or a {{lo, hi}} code range"
    )))
}

/// A JS number as a term code: an integer in the `u32` code space.
fn code_of(value: &JsValue) -> Option<u32> {
    let n = value.as_f64()?;
    (n >= 0.0 && n.fract() == 0.0 && n <= f64::from(u32::MAX)).then_some(n as u32)
}

/// A row count option (`limit`, `offset`): a non-negative integer, saturating
/// at the address space (so `Number.MAX_SAFE_INTEGER` reads as "no limit").
fn parse_count(value: &JsValue, key: &str) -> Result<usize, JsValue> {
    match value.as_f64() {
        Some(n) if n >= 0.0 && n.fract() == 0.0 => Ok(if n >= usize::MAX as f64 {
            usize::MAX
        } else {
            n as usize
        }),
        _ => Err(js_err(format!(
            "Option '{key}' must be a non-negative integer"
        ))),
    }
}

/// Read an optional field: `None` when absent, `null` or `undefined`.
fn get_option(options: &JsValue, key: &str) -> Result<Option<JsValue>, JsValue> {
    let value = Reflect::get(options, &key.into())
        .map_err(|_| js_err(format!("Could not read the '{key}' option")))?;
    Ok((!value.is_null() && !value.is_undefined()).then_some(value))
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
