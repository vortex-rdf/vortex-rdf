use std::sync::Arc;

use clap::ValueEnum;
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::arrays::{PrimitiveArray, VarBinViewArray};
use vortex_array::dtype::{DType, FieldNames};
use vortex_array::scalar::Scalar;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};

use crate::common::utils::{buf_as_str, graph_name_str};
use crate::error::{Result, VortexRdfError};
use crate::io::VORTEX_LIGHT_SESSION;
use crate::store::RawQuad;

pub mod default;
pub mod dictionary;
pub mod term_dictionary;
pub mod typed_object;

use self::term_dictionary::{DICT_FIELD, TermDictionary};

/// Determines the columnar schema used to store RDF quads in the Vortex StructArray.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
pub enum LayoutStrategy {
    /// ### `LayoutStrategy::Default` column schema
    ///
    /// All quad columns stored as opaque UTF-8 strings in N-Triples form.
    /// All four quad fields are stored as raw UTF-8 strings in N-Triples serialization form.
    /// Vortex applies `DictionaryLayout` internally to compress repeated values.
    ///
    /// | Column | Type              | Content                                                    |
    /// |--------|-------------------|------------------------------------------------------------|
    /// | `s`    | `VarBin<Utf8>`    | Subject: `<IRI>` or `_:blank`                              |
    /// | `p`    | `VarBin<Utf8>`    | Predicate: `<IRI>`                                         |
    /// | `o`    | `VarBin<Utf8>`    | Object: `<IRI>`, `_:blank`, `"lit"`, `"lit"@lang`, `"lit"^^<dt>` |
    /// | `g`    | `VarBin<Utf8>`    | Graph: `<IRI>`, `_:blank`, or `""` for DefaultGraph        |
    ///
    /// When `Indexes` contains `IndexType::SecondaryByReference`, four additional
    /// columns are appended: `_idx_o_val`, `_idx_o_rid`, `_idx_p_val`, `_idx_p_rid`.
    Default,

    /// ### `LayoutStrategy::TypedObject` column schema
    /// Object column decomposed into typed sub-columns (kind, value, datatype, lang).
    /// Same as `Default` for `s`, `p`, `g`. The `o` column is decomposed into typed fields
    /// so that Vortex can apply datatype-appropriate encodings (delta, RLE, dictionary).
    ///
    /// | Column       | Type                  | Content                                     |
    /// |--------------|-----------------------|---------------------------------------------|
    /// | `s`          | `VarBin<Utf8>`        | (same as Default)                           |
    /// | `p`          | `VarBin<Utf8>`        | (same as Default)                           |
    /// | `o_kind`     | `PrimitiveArray<u8>`  | 0=IRI, 1=BlankNode, 2=PlainLiteral, 3=LangLiteral, 4=TypedLiteral |
    /// | `o_value`    | `VarBin<Utf8>`        | IRI string, blank node ID, or literal value |
    /// | `o_datatype` | `VarBin<Utf8>` (nullable) | Datatype IRI — non-null when `o_kind = 4`  |
    /// | `o_lang`     | `VarBin<Utf8>` (nullable) | Language tag — non-null when `o_kind = 3`  |
    /// | `g`          | `VarBin<Utf8>`        | (same as Default)                           |
    ///
    /// When `Indexes` contains `IndexType::SecondaryByReference`, `_idx_o_val`
    /// sorts the full object terms in N-Triples form (same as the other layouts).
    TypedObject,

    /// ### `LayoutStrategy::Dictionary` column schema
    /// All four quad fields stored as u32 codes into a single global term
    /// dictionary. The dictionary is intrinsic to the layout: it lives in the
    /// `_dict_terms` column, always emitted alongside the code columns (see
    /// [`term_dictionary`]).
    ///
    /// | Column        | Type                  | Content                                             |
    /// |---------------|-----------------------|-----------------------------------------------------|
    /// | `s`,`p`,`o`,`g` | `PrimitiveArray<u32>` | code = position of the term in the sorted dictionary |
    /// | `_dict_terms` | `list<utf8>`          | row 0 = the sorted unique terms as one list; all other rows empty |
    ///
    /// Term IDs are lexicographic ranks, so code comparisons are
    /// order-isomorphic to string comparisons (sorted builders keep the
    /// subject binary-search fast path on the u32 column).
    ///
    /// When `Indexes` contains `IndexType::SecondaryByReference`, the
    /// `_idx_o_val`/`_idx_p_val` columns hold u32 codes instead of strings
    /// (see `IndexType::append_dictionary_columns`).
    Dictionary,
}

impl LayoutStrategy {
    /// Detect the column layout by inspecting the struct field names in the
    /// dtype, without materializing the array.
    pub(crate) fn from_dtype(dtype: &DType) -> LayoutStrategy {
        if let DType::Struct(fields, _) = dtype {
            // Presence of the intrinsic dictionary column means Dictionary layout.
            if fields.names().iter().any(|n| n.as_ref() == DICT_FIELD) {
                return LayoutStrategy::Dictionary;
            }
            // Presence of the typed-object kind column means TypedObject layout.
            if fields.names().iter().any(|n| n.as_ref() == "o_kind") {
                return LayoutStrategy::TypedObject;
            }
        }
        // Neither marker column found: plain string columns, Default layout.
        LayoutStrategy::Default
    }

    /// Field names of the primary (non-index) columns for this layout.
    pub(crate) fn field_names(self) -> Vec<Arc<str>> {
        match self {
            LayoutStrategy::Default => default::field_names(),
            LayoutStrategy::TypedObject => typed_object::field_names(),
            LayoutStrategy::Dictionary => dictionary::field_names(),
        }
    }

    /// Build the primary column arrays for this layout from raw quads.
    /// An empty slice yields empty columns with the correct dtypes.
    ///
    /// Not available for `Dictionary`: encoding needs the global
    /// [`TermDictionary`], so Dictionary chunks are built by the dedicated
    /// [`dictionary::build_chunk`] pipeline instead.
    ///
    /// [`TermDictionary`]: crate::store::layouts::term_dictionary::TermDictionary
    pub(crate) fn build_columns(self, quads: &[RawQuad]) -> Result<Vec<ArrayRef>> {
        match self {
            LayoutStrategy::Default => Ok(default::build_columns(quads)),
            LayoutStrategy::TypedObject => typed_object::build_columns(quads),
            LayoutStrategy::Dictionary => Err(crate::error::VortexRdfError::Serialization(
                "Dictionary layout chunks are built via the dictionary pipeline, \
                 not the generic column path"
                    .to_string(),
            )),
        }
    }
}

/// Query-time layout: the build-time [`LayoutStrategy`] resolved against a
/// constructed array, carrying any state intrinsic to the layout — for the
/// Dictionary layout, the global term dictionary. Holding the state in the
/// variant makes "Dictionary layout without a dictionary" unrepresentable.
#[derive(Clone)]
pub(crate) enum ResolvedLayout {
    Default,
    TypedObject,
    Dictionary(Arc<TermDictionary>),
}

/// Column equality constraints a quad pattern compiles to under a given
/// layout: the single source of truth consumed by both the in-memory mask
/// scan and the pushed-down file filter in `match_pattern`.
pub(crate) enum Constraints {
    /// A bound term cannot match any quad (e.g. absent from the dictionary).
    AlwaysFalse,
    /// Conjunction of per-column equalities; empty means unconstrained.
    Eq(Vec<(&'static str, Scalar)>),
}

impl ResolvedLayout {
    /// The build-time strategy tag this layout was resolved from.
    pub(crate) fn strategy(&self) -> LayoutStrategy {
        match self {
            ResolvedLayout::Default => LayoutStrategy::Default,
            ResolvedLayout::TypedObject => LayoutStrategy::TypedObject,
            ResolvedLayout::Dictionary(_) => LayoutStrategy::Dictionary,
        }
    }

    /// Field names of the primary (non-index) columns.
    pub(crate) fn primary_column_names(&self) -> Vec<&'static str> {
        match self {
            ResolvedLayout::Default | ResolvedLayout::Dictionary(_) => vec!["s", "p", "o", "g"],
            ResolvedLayout::TypedObject => {
                vec!["s", "p", "o_kind", "o_value", "o_datatype", "o_lang", "g"]
            }
        }
    }

    /// Project an array down to this layout's primary (non-index) columns only.
    pub(crate) fn project_primary(&self, arr: &ArrayRef) -> Result<ArrayRef> {
        let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
        let struct_arr = arr
            .clone()
            .execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;

        let primary = self.primary_column_names();
        let names: FieldNames = primary.iter().copied().collect();
        let arrays: Vec<ArrayRef> = primary
            .iter()
            .map(|n| {
                struct_arr
                    .unmasked_field_by_name(n)
                    .cloned()
                    .map_err(VortexRdfError::Vortex)
            })
            .collect::<Result<_>>()?;

        let len = arrays.first().map(|a| a.len()).unwrap_or(0);
        StructArray::try_new(names, arrays, len, Validity::NonNullable)
            .map_err(VortexRdfError::Vortex)
            .map(|a| a.into_array())
    }

    /// Decode a StructArray chunk into quads. Dictionary chunks are decoded
    /// through the layout's own dictionary (their `_dict_terms` payload may
    /// have been lost to slicing/filtering/file re-blocking).
    pub(crate) fn decode_chunk(&self, chunk: &ArrayRef) -> Vec<Result<Quad>> {
        match self {
            ResolvedLayout::Default => default::decode_chunk(chunk),
            ResolvedLayout::TypedObject => typed_object::decode_chunk(chunk),
            ResolvedLayout::Dictionary(dict) => dictionary::decode_chunk(chunk, dict),
        }
    }

    /// Write whatever state this layout holds intrinsically back into `array`,
    /// so that it can be serialized and read back without this layout's help.
    ///
    /// This is the counterpart to the caching done when a layout is resolved:
    /// state that lives in the array is hoisted into the variant at
    /// construction, and derived arrays may no longer carry it. Only the
    /// Dictionary layout has such state (its term dictionary); the other
    /// layouts encode everything in the columns themselves and pass `array`
    /// straight through.
    pub(crate) fn attach_intrinsic_state(&self, array: ArrayRef) -> Result<ArrayRef> {
        match self {
            ResolvedLayout::Default | ResolvedLayout::TypedObject => Ok(array),
            ResolvedLayout::Dictionary(dict) => dictionary::attach_payload(array, dict),
        }
    }

    /// Scalar for probing a sorted term column — the primary `s` column or a
    /// secondary index's `_idx_*_val` column. Under the Dictionary layout the
    /// term is translated to its u32 code (sorted-dictionary codes preserve
    /// lexicographic order); `None` means the term is absent from the
    /// dictionary and matches nothing.
    pub(crate) fn probe_scalar(&self, term_str: &str) -> Option<Scalar> {
        match self {
            ResolvedLayout::Dictionary(dict) => dict.get_id(term_str).map(Scalar::from),
            _ => Some(Scalar::from(term_str)),
        }
    }

    /// Decode an array of this layout's rows back into [`RawQuad`]s — each
    /// term in its N-Triples string form, without an oxrdf parse round-trip.
    ///
    /// The inverse of the build-time column encoding, for the operations that
    /// rebuild a store from its quads (compaction, and reads that must merge a
    /// string tail into a Dictionary-encoded base): Default reads its four
    /// string columns verbatim, TypedObject recomposes the object term from
    /// its typed sub-columns, and Dictionary resolves each u32 code through
    /// this layout's term dictionary.
    pub(crate) fn raw_quads(&self, rows: &ArrayRef) -> Result<Vec<RawQuad>> {
        let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
        let struct_arr = rows
            .clone()
            .execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        let (s, p, o, g) = match self {
            ResolvedLayout::Default => (
                read_string_column(&struct_arr, "s")?,
                read_string_column(&struct_arr, "p")?,
                read_string_column(&struct_arr, "o")?,
                read_string_column(&struct_arr, "g")?,
            ),
            ResolvedLayout::TypedObject => (
                read_string_column(&struct_arr, "s")?,
                read_string_column(&struct_arr, "p")?,
                typed_object::object_terms(&struct_arr)?,
                read_string_column(&struct_arr, "g")?,
            ),
            ResolvedLayout::Dictionary(dict) => {
                let term = |codes: Vec<u32>| -> Result<Vec<String>> {
                    codes
                        .into_iter()
                        .map(|code| {
                            if (code as usize) >= dict.len() {
                                return Err(VortexRdfError::Deserialization(format!(
                                    "Term code {} out of dictionary bounds ({})",
                                    code,
                                    dict.len()
                                )));
                            }
                            let bytes = dict.view().bytes_at(code as usize);
                            buf_as_str(bytes.as_ref()).map(str::to_string)
                        })
                        .collect()
                };
                (
                    term(read_u32_column(&struct_arr, "s")?)?,
                    term(read_u32_column(&struct_arr, "p")?)?,
                    term(read_u32_column(&struct_arr, "o")?)?,
                    term(read_u32_column(&struct_arr, "g")?)?,
                )
            }
        };
        Ok(s.into_iter()
            .zip(p)
            .zip(o)
            .zip(g)
            .map(|(((s, p), o), g)| RawQuad { s, p, o, g })
            .collect())
    }

    /// Compile a quad pattern into per-column equality constraints: the
    /// layout-specific term → (column, scalar) mapping.
    pub(crate) fn constraints(
        &self,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Constraints {
        let mut eqs: Vec<(&'static str, Scalar)> = Vec::new();
        match self {
            ResolvedLayout::Default => {
                if let Some(s) = subject {
                    eqs.push(("s", Scalar::from(s.to_string().as_str())));
                }
                if let Some(p) = predicate {
                    eqs.push(("p", Scalar::from(p.to_string().as_str())));
                }
                if let Some(o) = object {
                    eqs.push(("o", Scalar::from(o.to_string().as_str())));
                }
                if let Some(g) = graph {
                    eqs.push(("g", Scalar::from(graph_name_str(g).as_str())));
                }
            }
            ResolvedLayout::TypedObject => {
                if let Some(s) = subject {
                    eqs.push(("s", Scalar::from(s.to_string().as_str())));
                }
                if let Some(p) = predicate {
                    eqs.push(("p", Scalar::from(p.to_string().as_str())));
                }
                if let Some(o) = object {
                    let (kind, value, dt, lang) = typed_object::decompose_object(o);
                    eqs.push(("o_kind", Scalar::from(kind)));
                    eqs.push(("o_value", Scalar::from(value.as_str())));
                    if let Some(dt_str) = dt {
                        eqs.push(("o_datatype", Scalar::from(dt_str.as_str())));
                    }
                    if let Some(lang_str) = lang {
                        eqs.push(("o_lang", Scalar::from(lang_str.as_str())));
                    }
                }
                if let Some(g) = graph {
                    eqs.push(("g", Scalar::from(graph_name_str(g).as_str())));
                }
            }
            ResolvedLayout::Dictionary(dict) => {
                // Resolve every bound term to its code: a term absent from
                // the dictionary cannot match any quad.
                let bound = [
                    ("s", subject.map(|s| s.to_string())),
                    ("p", predicate.map(|p| p.to_string())),
                    ("o", object.map(|o| o.to_string())),
                    ("g", graph.map(graph_name_str)),
                ];
                for (field, term) in bound {
                    if let Some(term_str) = term {
                        match dict.get_id(&term_str) {
                            Some(id) => eqs.push((field, Scalar::from(id))),
                            None => return Constraints::AlwaysFalse,
                        }
                    }
                }
            }
        }
        Constraints::Eq(eqs)
    }
}

/// Read a UTF-8 string column into owned term strings, one per row.
fn read_string_column(struct_arr: &StructArray, name: &str) -> Result<Vec<String>> {
    let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
    let col = struct_arr
        .unmasked_field_by_name(name)
        .map_err(VortexRdfError::Vortex)?
        .clone()
        .execute::<VarBinViewArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    (0..col.len())
        .map(|i| {
            let buf = col.bytes_at(i);
            buf_as_str(buf.as_ref()).map(str::to_string)
        })
        .collect()
}

/// Read a u32 code column into owned codes, one per row.
fn read_u32_column(struct_arr: &StructArray, name: &str) -> Result<Vec<u32>> {
    let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
    let col = struct_arr
        .unmasked_field_by_name(name)
        .map_err(VortexRdfError::Vortex)?
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    Ok(col.as_slice::<u32>().to_vec())
}
