//! How quads become columns, and how a pattern becomes column constraints:
//! the [`LayoutStrategy`] a build requests, the `ResolvedLayout` a store
//! reads through (the strategy plus the state it cannot carry alone, a
//! Dictionary layout's term access), and the dispatch that turns them into
//! per-layout column building. The `QuadPattern`/`PatternCodes` pattern form
//! and its lowering into column constraints are `pattern`. Chunk decoding,
//! one dispatch over where a layout keeps its terms for every
//! representation, is `decode` (the string layouts' reads in `strings`).
//!
//! A leaf (`default`, `typed_object`, `dictionary`) owns exactly one layout's
//! physical schema, its column names and encode/decode loops, and never names
//! another's. The Dictionary leaf is a folder module carrying the whole
//! term-dictionary subsystem (storage, ingest, residency) beside its
//! encode/decode paths.
//!
//! Secondary indexes are *not* part of a layout: they are built as their own
//! children beside a layout's quad rows, in the index's encoding for the
//! layout (term strings, or the Dictionary layout's term codes), and the
//! index modules own those columns and their names (see
//! [`IndexType`](crate::store::indexes::IndexType)).

use std::sync::Arc;

use vortex_array::ArrayRef;
use vortex_array::dtype::DType;

use crate::error::{Result, VortexRdfError};
use crate::store::RawQuad;

mod decode;
pub(crate) mod default;
pub(crate) mod dictionary;
mod pattern;
mod strings;
pub(crate) mod typed_object;

pub(crate) use self::decode::ChunkDecode;
pub(crate) use self::dictionary::DictAccess;
pub(crate) use self::pattern::{
    Constraints, OwnedQuadPattern, PatternCodes, Prepared, QuadPattern, TermRef,
};
pub(crate) use self::strings::StringLayout;
use crate::store::schema::{CODE_PTYPE, COL_O_KIND, COL_S, PRIMARY_COLUMNS};

/// The columnar schema a store's quads are stored in.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
pub enum LayoutStrategy {
    /// All four quad fields as opaque N-Triples strings.
    ///
    /// ### `LayoutStrategy::Default` column schema
    ///
    /// All four quad fields stored as opaque UTF-8 strings in N-Triples
    /// serialization form. Vortex applies its own `DictLayout` internally to
    /// compress repeated values.
    ///
    /// | Column | Type              | Content                                                    |
    /// |--------|-------------------|------------------------------------------------------------|
    /// | `s`    | `VarBin<Utf8>`    | Subject: `<IRI>` or `_:blank`                              |
    /// | `p`    | `VarBin<Utf8>`    | Predicate: `<IRI>`                                         |
    /// | `o`    | `VarBin<Utf8>`    | Object: `<IRI>`, `_:blank`, `"lit"`, `"lit"@lang`, `"lit"^^<dt>` |
    /// | `g`    | `VarBin<Utf8>`    | Graph: `<IRI>`, `_:blank`, or `""` for DefaultGraph        |
    ///
    /// Each requested [`IndexType`] adds its own children beside these
    /// columns; see that enum's variant docs for their column tables (they
    /// hold term strings here, as under `TypedObject`).
    ///
    /// [`IndexType`]: crate::store::indexes::IndexType
    Default,

    /// Object column split into typed sub-columns (kind, value, datatype, lang).
    ///
    /// ### `LayoutStrategy::TypedObject` column schema
    ///
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
    /// Index children are unaffected by the object split: every requested
    /// [`IndexType`] builds the same term-string children it would under
    /// `Default`, sorting whole object terms in N-Triples form.
    ///
    /// [`IndexType`]: crate::store::indexes::IndexType
    TypedObject,

    /// All four quad fields as term codes into one shared term dictionary.
    ///
    /// ### `LayoutStrategy::Dictionary` column schema
    ///
    /// All four quad fields stored as [`TermCode`]s (`u64`) into a single
    /// global term dictionary. In memory the dictionary lives beside the
    /// columns (see `dictionary::term_dict`); a serialized file carries it as
    /// the native container's `dictionary` child (see `crate::io::container`),
    /// so the quad columns stay bare.
    ///
    /// | Column        | Type                  | Content                                             |
    /// |---------------|-----------------------|-----------------------------------------------------|
    /// | `s`,`p`,`o`,`g` | `PrimitiveArray<u64>` | code = position of the term in the sorted dictionary |
    ///
    /// Term codes are lexicographic ranks, so code comparisons are
    /// order-isomorphic to string comparisons (sorted builders keep the
    /// subject binary-search fast path on the code column).
    ///
    /// Every requested [`IndexType`] builds its usual children, except that
    /// their term-valued columns hold codes instead of strings; the row-id
    /// columns are `u64` ([`RowId`](crate::store::RowId)) under every layout.
    ///
    /// [`IndexType`]: crate::store::indexes::IndexType
    Dictionary,
}

/// The canonical strategy name: kebab-case (`"default"`, `"typed-object"`,
/// `"dictionary"`), the spelling the `clap` derive exposes on the CLI, so
/// every frontend reports one vocabulary and a value one prints another
/// parses.
impl std::fmt::Display for LayoutStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            LayoutStrategy::Default => "default",
            LayoutStrategy::TypedObject => "typed-object",
            LayoutStrategy::Dictionary => "dictionary",
        })
    }
}

/// Accepts exactly the canonical kebab-case names
/// [`Display`](std::fmt::Display) emits (`"default"`, `"typed-object"`,
/// `"dictionary"`), the one vocabulary every frontend shares. Any other name
/// is an [`InvalidInput`](VortexRdfError::InvalidInput) error.
impl std::str::FromStr for LayoutStrategy {
    type Err = VortexRdfError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "default" => Ok(LayoutStrategy::Default),
            "typed-object" => Ok(LayoutStrategy::TypedObject),
            "dictionary" => Ok(LayoutStrategy::Dictionary),
            _ => Err(VortexRdfError::InvalidInput(format!(
                "unknown layout strategy {s:?}; expected \"default\", \"typed-object\" or \
                 \"dictionary\""
            ))),
        }
    }
}

impl LayoutStrategy {
    /// The layout a struct dtype's schema names, read off its columns without
    /// materializing anything: an `s` column of codes is Dictionary, an
    /// `o_kind` column TypedObject, anything else Default.
    pub(crate) fn from_dtype(dtype: &DType) -> LayoutStrategy {
        if let DType::Struct(fields, _) = dtype {
            // The dictionary itself rides outside the schema, as the native
            // container's dictionary child. Another integer width is no layout
            // of this crate's: the open refuses it (`check_id_columns`) before
            // anything reads it.
            if matches!(fields.field(COL_S), Some(DType::Primitive(ptype, _)) if ptype == CODE_PTYPE)
            {
                return LayoutStrategy::Dictionary;
            }
            if fields.names().iter().any(|n| n.as_ref() == COL_O_KIND) {
                return LayoutStrategy::TypedObject;
            }
        }
        LayoutStrategy::Default
    }

    /// Names of the primary (non-index) columns for this layout, in schema
    /// order.
    pub(crate) fn primary_column_names(self) -> &'static [&'static str] {
        match self {
            LayoutStrategy::Default => default::COLUMNS,
            LayoutStrategy::TypedObject => typed_object::COLUMNS,
            LayoutStrategy::Dictionary => &PRIMARY_COLUMNS,
        }
    }

    /// This layout's primary column arrays for `quads`; an empty slice yields
    /// empty columns of the right dtypes.
    ///
    /// A `Serialization` error for `Dictionary`, whose encoding needs the
    /// global [`TermDictionary`]: its chunks are built from codes by
    /// [`dictionary::build_code_chunk`] instead.
    ///
    /// [`TermDictionary`]: dictionary::TermDictionary
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
/// constructed array, carrying any state intrinsic to the layout (for the
/// Dictionary layout, access to the global term dictionary). Holding the
/// state in the variant makes "Dictionary layout without a dictionary"
/// unrepresentable; [`DictAccess`] carries *how* the dictionary is reached
/// (resident, or left in its file).
#[derive(Clone)]
pub(crate) enum ResolvedLayout {
    Default,
    TypedObject,
    Dictionary(DictAccess),
}

impl ResolvedLayout {
    /// The term dictionary, when it is in memory: `None` for the string
    /// layouts and for a dictionary left in its file.
    pub(crate) fn resident_dictionary(&self) -> Option<&Arc<dictionary::TermDictionary>> {
        match self {
            ResolvedLayout::Dictionary(access) => access.resident(),
            _ => None,
        }
    }

    /// The build-time strategy tag this layout was resolved from.
    pub(crate) fn strategy(&self) -> LayoutStrategy {
        match self {
            ResolvedLayout::Default => LayoutStrategy::Default,
            ResolvedLayout::TypedObject => LayoutStrategy::TypedObject,
            ResolvedLayout::Dictionary(_) => LayoutStrategy::Dictionary,
        }
    }
}
