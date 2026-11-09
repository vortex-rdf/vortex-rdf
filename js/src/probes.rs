//! Read narrowing and probes from JS values: the `keep`, `limit` and
//! `offset` options of `matchCodes`, `countQuads` and `matchCodeChunks`, the
//! chunk options of `matchCodeChunks` (`columns`, `batchRows`, `signal`), the
//! options of `matchCodesMany`, and the probe list of the `*Many` calls.
//! Everything is parsed before any evaluation, so a malformed option or
//! probe throws before the store is read: a value of the wrong type or shape
//! throws a `TypeError`, a number outside its range a `RangeError`. An
//! option that is `undefined` or `null` is absent, and so is an options
//! object (the bindings take it as an `Option`, which is `None` for both).

use js_sys::{Array, Float64Array, Object, Reflect};
use vortex_rdf_core::{Keep, Probe, QuadColumn, TermCode};
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;

use crate::codes::{code_from_js, safe_u64};
use crate::error::{js_range_err, js_type_err};
use crate::terms::pattern_probe;

/// The most rows a chunk of `matchCodeChunks` holds when its options name no
/// `batchRows`.
pub(crate) const DEFAULT_BATCH_ROWS: usize = 8192;

/// The options of `matchCodes` and `countQuads`.
const READ_OPTIONS: [&str; 3] = ["keep", "limit", "offset"];
/// The options of `matchCodeChunks`.
const CHUNK_OPTIONS: [&str; 6] = ["keep", "limit", "offset", "columns", "batchRows", "signal"];
/// The options of `matchCodesMany`.
const BATCH_OPTIONS: [&str; 1] = ["signal"];
/// The keys of a probe object.
const PROBE_KEYS: [&str; 7] = ["s", "p", "o", "g", "keep", "limit", "offset"];
/// The positions a `keep` names.
const POSITIONS: [&str; 4] = ["s", "p", "o", "g"];

/// How a chunked read is cut: the columns each chunk holds, in `(s, p, o, g)`
/// order, the most rows a chunk holds, and the signal whose abort ends the
/// iteration.
pub(crate) struct ChunkShape {
    pub(crate) columns: Vec<QuadColumn>,
    pub(crate) batch_rows: usize,
    pub(crate) signal: Signal,
}

/// A `signal` option: the `AbortSignal` (`undefined` for none), and whether
/// it had already aborted when the options were read.
pub(crate) struct Signal {
    pub(crate) value: JsValue,
    pub(crate) aborted: bool,
}

/// The probe of `matchCodes` and `countQuads`: the pattern `probe` narrowed
/// by the `keep`, `limit` and `offset` of `options`.
pub(crate) fn read_probe(probe: Probe, options: Option<&JsValue>) -> Result<Probe, JsValue> {
    let mut options = Fields::read(options, "options", "option", &READ_OPTIONS)?;
    narrowed(probe, &mut options)
}

/// The probe and the chunk shape of `matchCodeChunks`: the pattern `probe`
/// narrowed as for [`read_probe`], every column unless `columns` names some,
/// and [`DEFAULT_BATCH_ROWS`] unless `batchRows` is given.
pub(crate) fn chunk_probe(
    probe: Probe,
    options: Option<&JsValue>,
) -> Result<(Probe, ChunkShape), JsValue> {
    let mut options = Fields::read(options, "options", "option", &CHUNK_OPTIONS)?;
    let probe = narrowed(probe, &mut options)?;
    let columns = match options.take("columns") {
        Some(names) => chunk_columns(&names)?,
        None => QuadColumn::ALL.to_vec(),
    };
    let batch_rows = match options.take("batchRows") {
        Some(rows) => row_count(&rows, "batchRows", 1)?,
        None => DEFAULT_BATCH_ROWS,
    };
    let signal = signal(&mut options)?;
    Ok((
        probe,
        ChunkShape {
            columns,
            batch_rows,
            signal,
        },
    ))
}

/// The `signal` of `matchCodesMany`'s options.
pub(crate) fn batch_signal(options: Option<&JsValue>) -> Result<Signal, JsValue> {
    let mut options = Fields::read(options, "options", "option", &BATCH_OPTIONS)?;
    signal(&mut options)
}

/// Every probe of a `*Many` call, in input order, parsed before any runs.
pub(crate) fn parse_probes(probes: &JsValue) -> Result<Vec<Probe>, JsValue> {
    if !Array::is_array(probes) {
        return Err(js_type_err("probes must be an array of probes"));
    }
    let probes = probes.unchecked_ref::<Array>();
    let mut parsed = Vec::with_capacity(probes.length() as usize);
    for probe in probes.iter() {
        parsed.push(parse_probe(&probe)?);
    }
    Ok(parsed)
}

/// A probe: an array `[s, p, o, g]` of pattern terms, or an object with keys
/// `s`, `p`, `o`, `g`, `keep`, `limit` and `offset`, each optional. A pattern
/// term reads as in `match`: a wildcard when absent, a NamedNode IRI when a
/// bare string, else an RDF/JS term of its position's kind.
fn parse_probe(probe: &JsValue) -> Result<Probe, JsValue> {
    if Array::is_array(probe) {
        let slots = probe.unchecked_ref::<Array>();
        if slots.length() != 4 {
            return Err(js_type_err(format!(
                "a probe array has four positions [s, p, o, g], got {}",
                slots.length()
            )));
        }
        return pattern_probe(slots.get(0), slots.get(1), slots.get(2), slots.get(3));
    }
    if !probe.is_object() {
        return Err(js_type_err(
            "a probe is an array [s, p, o, g] of pattern terms or an object with keys s, p, o, \
             g, keep, limit and offset",
        ));
    }
    let mut fields = Fields::read(Some(probe), "a probe", "probe key", &PROBE_KEYS)?;
    let mut term = |key| fields.take(key).unwrap_or(JsValue::UNDEFINED);
    let probe = pattern_probe(term("s"), term("p"), term("o"), term("g"))?;
    narrowed(probe, &mut fields)
}

/// `probe` narrowed by the `keep`, `limit` and `offset` of `fields`.
fn narrowed<const N: usize>(mut probe: Probe, fields: &mut Fields<N>) -> Result<Probe, JsValue> {
    if let Some(keep) = fields.take("keep") {
        probe.keeps = parse_keeps(&keep)?;
    }
    if let Some(limit) = fields.take("limit") {
        probe.limit = Some(row_count(&limit, "limit", 0)?);
    }
    if let Some(offset) = fields.take("offset") {
        probe.offset = row_count(&offset, "offset", 0)?;
    }
    Ok(probe)
}

/// The keeps of a `keep` option: an object from position (`s`, `p`, `o`,
/// `g`) to a code set or range, applied in `(s, p, o, g)` order.
fn parse_keeps(keep: &JsValue) -> Result<Vec<(QuadColumn, Keep)>, JsValue> {
    if !keep.is_object() || Array::is_array(keep) || keep.is_instance_of::<Float64Array>() {
        return Err(js_type_err(
            "keep must be an object mapping a position (s, p, o or g) to the codes to keep there",
        ));
    }
    let mut positions = Fields::read(Some(keep), "keep", "keep position", &POSITIONS)?;
    let mut keeps = Vec::new();
    for column in QuadColumn::ALL {
        if let Some(spec) = positions.take(column.name()) {
            keeps.push((column, parse_keep(&spec)?));
        }
    }
    Ok(keeps)
}

/// One keep: an array of two codes `[lo, hi]` is the code range, the
/// half-open codes `lo <= code < hi`; a `Float64Array`, or an array of any
/// other length, is a code set.
fn parse_keep(spec: &JsValue) -> Result<Keep, JsValue> {
    if Array::is_array(spec) {
        let items = spec.unchecked_ref::<Array>();
        if items.length() == 2 {
            let lo = keep_code(&items.get(0))?;
            let hi = keep_code(&items.get(1))?;
            return Ok(Keep::range(lo..hi));
        }
        let codes = items
            .iter()
            .map(|item| keep_code(&item))
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(Keep::set(codes));
    }
    if let Some(codes) = spec.dyn_ref::<Float64Array>() {
        let codes = codes
            .to_vec()
            .into_iter()
            .map(|code| code_from_js(code).map_err(js_range_err))
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(Keep::set(codes));
    }
    Err(js_type_err(
        "a keep is a code set (a Float64Array, or an array of codes of any length but two) or a \
         code range (an array [lo, hi], the half-open codes lo <= code < hi)",
    ))
}

/// A code of a keep: a number that is an integer from 0 to 2^53 − 1.
fn keep_code(value: &JsValue) -> Result<TermCode, JsValue> {
    let number = value
        .as_f64()
        .ok_or_else(|| js_type_err(format!("a keep code is a number, got {}", type_of(value))))?;
    code_from_js(number).map_err(js_range_err)
}

/// A row count option (`limit`, `offset`, `batchRows`): an integer from
/// `least` to 2^53 − 1 ([`safe_u64`]). A wasm32 store holds fewer rows than a
/// `usize` counts, so a count past one saturates.
fn row_count(value: &JsValue, name: &str, least: u64) -> Result<usize, JsValue> {
    let number = value
        .as_f64()
        .ok_or_else(|| js_type_err(format!("{name} is a number, got {}", type_of(value))))?;
    let Some(count) = safe_u64(number).filter(|&count| count >= least) else {
        return Err(js_range_err(format!(
            "{name} must be an integer from {least} to 2^53 - 1, got {number}"
        )));
    };
    Ok(usize::try_from(count).unwrap_or(usize::MAX))
}

/// The columns a `columns` option names (`'s'`, `'p'`, `'o'`, `'g'`), in
/// `(s, p, o, g)` order: a chunk holds each column under its name, so a
/// repeated name adds nothing.
fn chunk_columns(names: &JsValue) -> Result<Vec<QuadColumn>, JsValue> {
    if !Array::is_array(names) {
        return Err(js_type_err(
            "columns must be an array of column names ('s', 'p', 'o', 'g')",
        ));
    }
    let names = names.unchecked_ref::<Array>();
    if names.length() == 0 {
        return Err(js_range_err(
            "columns names no column: name at least one of 's', 'p', 'o' or 'g'",
        ));
    }
    let mut wanted = [false; 4];
    for name in names.iter() {
        let Some(name) = name.as_string() else {
            return Err(js_type_err(format!(
                "a column name is a string, got {}",
                type_of(&name)
            )));
        };
        let column = QuadColumn::from_name(&name).ok_or_else(|| {
            js_type_err(format!(
                "unknown column {name:?}: expected 's', 'p', 'o' or 'g'"
            ))
        })?;
        wanted[column.index()] = true;
    }
    Ok(QuadColumn::ALL
        .into_iter()
        .filter(|column| wanted[column.index()])
        .collect())
}

/// The `signal` option: an `AbortSignal` (an object whose `aborted` is a
/// boolean), or `undefined` when there is none.
fn signal<const N: usize>(options: &mut Fields<N>) -> Result<Signal, JsValue> {
    let Some(value) = options.take("signal") else {
        return Ok(Signal {
            value: JsValue::UNDEFINED,
            aborted: false,
        });
    };
    let aborted = if value.is_object() {
        Reflect::get(&value, &JsValue::from_str("aborted"))?
    } else {
        JsValue::UNDEFINED
    };
    let Some(aborted) = aborted.as_bool() else {
        return Err(js_type_err("signal must be an AbortSignal"));
    };
    Ok(Signal { value, aborted })
}

/// The `typeof` of `value`, for an error message.
fn type_of(value: &JsValue) -> String {
    value.js_typeof().as_string().unwrap_or_default()
}

/// The values of an object's own enumerable keys, read in one pass: options,
/// a probe or a keep. Each key must be one of the keys the object takes; a
/// value that is `undefined` or `null` is absent, as is every value when
/// there is no object.
struct Fields<const N: usize> {
    keys: &'static [&'static str; N],
    values: [Option<JsValue>; N],
}

impl<const N: usize> Fields<N> {
    /// The fields of `value`, refusing a non-object and a key outside `keys`
    /// (`what` names the object and `noun` a key in the messages).
    fn read(
        value: Option<&JsValue>,
        what: &str,
        noun: &str,
        keys: &'static [&'static str; N],
    ) -> Result<Self, JsValue> {
        let mut values = std::array::from_fn(|_| None);
        if let Some(value) = value {
            if !value.is_object() {
                return Err(js_type_err(format!("{what} must be an object")));
            }
            for key in Object::keys(value.unchecked_ref::<Object>()).iter() {
                let name = key.as_string().unwrap_or_default();
                let Some(slot) = keys.iter().position(|known| *known == name) else {
                    return Err(js_type_err(format!(
                        "unknown {noun} {name:?}: expected {}",
                        one_of(keys)
                    )));
                };
                let field = Reflect::get(value, &key)?;
                if !field.is_null_or_undefined() {
                    values[slot] = Some(field);
                }
            }
        }
        Ok(Fields { keys, values })
    }

    /// The value at `key`, taken out; `None` when it is absent.
    fn take(&mut self, key: &str) -> Option<JsValue> {
        let slot = self.keys.iter().position(|known| *known == key)?;
        self.values[slot].take()
    }
}

/// `keys` as prose: `a`, `a or b`, `a, b or c`.
fn one_of(keys: &[&str]) -> String {
    match keys {
        [] => String::new(),
        [only] => (*only).to_owned(),
        [init @ .., last] => format!("{} or {last}", init.join(", ")),
    }
}
