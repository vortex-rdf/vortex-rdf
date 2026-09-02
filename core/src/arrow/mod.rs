//! The Arrow face of a store: the record-batch schema of a matched view.
//!
//! Everything Arrow-consumer-facing speaks through here: the schema a quad
//! batch carries ([`quad_schema`]), the way term columns are encoded in it
//! ([`TermEncoding`]), and the column-selection currency ([`QuadColumn`]).
//! The batches themselves are produced by the store's export methods, which
//! convert each Vortex chunk through the session's `ArrowSession`
//! (registered in [`crate::session`]).
//!
//! The column names and their order are the store's own serialized contract
//! ([`schema::PRIMARY_COLUMNS`](crate::store::schema)); the Arrow schema
//! restates them, it does not define them.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::error::{Result, VortexRdfError};
use crate::store::LayoutStrategy;
use crate::store::schema::PRIMARY_COLUMNS;

/// Schema-metadata key holding the store's [`LayoutStrategy`] (canonical
/// kebab-case name).
pub const META_LAYOUT: &str = "vortex_rdf.layout";
/// Schema-metadata key holding the [`TermEncoding`] (canonical name).
pub const META_TERM_ENCODING: &str = "vortex_rdf.term_encoding";
/// Schema-metadata key holding the producing crate version.
pub const META_VERSION: &str = "vortex_rdf.version";
/// Schema-metadata key holding the spelling of the default graph in the `g`
/// column: the empty string.
pub const META_DEFAULT_GRAPH: &str = "vortex_rdf.default_graph";

/// How term columns are encoded in an exported record batch.
///
/// All three encodings carry the same four non-nullable columns (`s`, `p`,
/// `o`, `g`); they differ in the Arrow type of every cell.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum TermEncoding {
    /// `UInt32` term codes — the Dictionary layout's native currency.
    ///
    /// Codes are lexicographic ranks over the store's term dictionary, so
    /// equality and ordering on codes mirror the N-Triples strings. Cheapest
    /// to export (buffer-sharing) and to join on; resolve codes to strings
    /// through the dictionary export.
    Codes,
    /// `Dictionary(UInt32, Utf8View)` — the same codes as [`Self::Codes`],
    /// with the term dictionary attached as the Arrow dictionary values.
    ///
    /// Every batch of a stream shares one values array, so consumers see
    /// strings while the data stays code-sized.
    Terms,
    /// `Utf8View` N-Triples strings — the layout-independent form.
    Strings,
}

/// The canonical encoding name: `"codes"`, `"terms"`, `"strings"` — the one
/// spelling every frontend shares (schema metadata, bindings arguments).
impl std::fmt::Display for TermEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            TermEncoding::Codes => "codes",
            TermEncoding::Terms => "terms",
            TermEncoding::Strings => "strings",
        })
    }
}

/// Accepts exactly the canonical names [`Display`](std::fmt::Display) emits.
impl std::str::FromStr for TermEncoding {
    type Err = VortexRdfError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "codes" => Ok(TermEncoding::Codes),
            "terms" => Ok(TermEncoding::Terms),
            "strings" => Ok(TermEncoding::Strings),
            _ => Err(VortexRdfError::InvalidOperation(format!(
                "unknown term encoding {s:?}; expected \"codes\", \"terms\" or \"strings\""
            ))),
        }
    }
}

/// One of the four primary quad columns, in emission order — the currency of
/// column projections on the batch export.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum QuadColumn {
    /// The subject column `s`.
    S,
    /// The predicate column `p`.
    P,
    /// The object column `o`.
    O,
    /// The graph-name column `g` (empty string = default graph).
    G,
}

impl QuadColumn {
    /// All four columns in emission order — the identity projection.
    pub const ALL: [QuadColumn; 4] = [QuadColumn::S, QuadColumn::P, QuadColumn::O, QuadColumn::G];

    /// The column's serialized name (`"s"`, `"p"`, `"o"`, `"g"`).
    pub fn name(self) -> &'static str {
        PRIMARY_COLUMNS[self.index()]
    }

    /// The column's position in the unprojected schema.
    pub fn index(self) -> usize {
        match self {
            QuadColumn::S => 0,
            QuadColumn::P => 1,
            QuadColumn::O => 2,
            QuadColumn::G => 3,
        }
    }
}

/// The Arrow schema of a quad record batch under `layout` × `encoding`:
/// the four non-nullable primary columns (`s`, `p`, `o`, `g`), each typed per
/// [`TermEncoding`], plus the `vortex_rdf.*` metadata entries.
///
/// # Errors
///
/// The combination must be servable: [`TermEncoding::Codes`] and
/// [`TermEncoding::Terms`] exist only under [`LayoutStrategy::Dictionary`],
/// and the TypedObject layout has no Arrow export.
pub fn quad_schema(layout: LayoutStrategy, encoding: TermEncoding) -> Result<SchemaRef> {
    let cell_type = match (layout, encoding) {
        (LayoutStrategy::TypedObject, _) => {
            return Err(VortexRdfError::InvalidOperation(
                "the typed-object layout has no Arrow export".to_string(),
            ));
        }
        (LayoutStrategy::Dictionary, TermEncoding::Codes) => DataType::UInt32,
        (LayoutStrategy::Dictionary, TermEncoding::Terms) => {
            DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8View))
        }
        (LayoutStrategy::Dictionary | LayoutStrategy::Default, TermEncoding::Strings) => {
            DataType::Utf8View
        }
        (LayoutStrategy::Default, TermEncoding::Codes | TermEncoding::Terms) => {
            return Err(VortexRdfError::InvalidOperation(format!(
                "term encoding \"{encoding}\" needs the dictionary layout; \
                 this store uses \"{layout}\""
            )));
        }
    };
    let fields: Vec<Field> = PRIMARY_COLUMNS
        .iter()
        .map(|name| Field::new(*name, cell_type.clone(), false))
        .collect();
    let metadata = HashMap::from([
        (META_LAYOUT.to_string(), layout.to_string()),
        (META_TERM_ENCODING.to_string(), encoding.to_string()),
        (META_VERSION.to_string(), env!("CARGO_PKG_VERSION").to_string()),
        (META_DEFAULT_GRAPH.to_string(), String::new()),
    ]);
    Ok(Arc::new(Schema::new_with_metadata(fields, metadata)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dictionary_codes_schema() {
        let schema = quad_schema(LayoutStrategy::Dictionary, TermEncoding::Codes).unwrap();
        assert_eq!(schema.fields().len(), 4);
        for (field, name) in schema.fields().iter().zip(PRIMARY_COLUMNS) {
            assert_eq!(field.name(), name);
            assert_eq!(field.data_type(), &DataType::UInt32);
            assert!(!field.is_nullable());
        }
        assert_eq!(schema.metadata()[META_LAYOUT], "dictionary");
        assert_eq!(schema.metadata()[META_TERM_ENCODING], "codes");
        assert_eq!(schema.metadata()[META_DEFAULT_GRAPH], "");
        assert_eq!(schema.metadata()[META_VERSION], env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn dictionary_terms_schema() {
        let schema = quad_schema(LayoutStrategy::Dictionary, TermEncoding::Terms).unwrap();
        let expected =
            DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8View));
        for field in schema.fields() {
            assert_eq!(field.data_type(), &expected);
        }
    }

    #[test]
    fn strings_schema_on_both_string_capable_layouts() {
        for layout in [LayoutStrategy::Dictionary, LayoutStrategy::Default] {
            let schema = quad_schema(layout, TermEncoding::Strings).unwrap();
            for field in schema.fields() {
                assert_eq!(field.data_type(), &DataType::Utf8View);
            }
            assert_eq!(schema.metadata()[META_LAYOUT], layout.to_string());
        }
    }

    #[test]
    fn unservable_combinations_error() {
        for encoding in [TermEncoding::Codes, TermEncoding::Terms, TermEncoding::Strings] {
            assert!(quad_schema(LayoutStrategy::TypedObject, encoding).is_err());
        }
        for encoding in [TermEncoding::Codes, TermEncoding::Terms] {
            assert!(quad_schema(LayoutStrategy::Default, encoding).is_err());
        }
    }

    #[test]
    fn canonical_names_round_trip() {
        for encoding in [TermEncoding::Codes, TermEncoding::Terms, TermEncoding::Strings] {
            assert_eq!(encoding.to_string().parse::<TermEncoding>().unwrap(), encoding);
        }
        assert!("Codes".parse::<TermEncoding>().is_err());
    }

    #[test]
    fn quad_column_names_follow_the_serialized_contract() {
        assert_eq!(QuadColumn::ALL.map(QuadColumn::name), PRIMARY_COLUMNS);
        for (i, column) in QuadColumn::ALL.into_iter().enumerate() {
            assert_eq!(column.index(), i);
        }
    }
}
