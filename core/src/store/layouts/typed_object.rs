//! Column-building and decoding logic for [`LayoutStrategy::TypedObject`]:
//! the object column is decomposed into typed sub-columns
//! (`o_kind`, `o_value`, `o_datatype`, `o_lang`).
//!
//! [`LayoutStrategy::TypedObject`]: super::LayoutStrategy::TypedObject

use oxrdf::{BlankNode, Literal, NamedNode, Quad, Term};
use vortex_array::arrays::struct_::StructArray;
use vortex_array::arrays::{PrimitiveArray, VarBinViewArray};
use vortex_array::{ArrayRef, ExecutionCtx, IntoArray, VortexSessionExecute};

use crate::common::terms::{parse_graph_name, parse_named_node, parse_subject, parse_term};
use crate::common::vocab::XSD_STRING;
use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::RawQuad;
use crate::store::array::{StrColReader, field_as, make_nullable_string_array, make_string_array};
use crate::store::schema::{COL_G, COL_P, COL_S};

/// The term kind tag — its presence in a schema is what marks the layout.
pub(crate) const COL_O_KIND: &str = "o_kind";
/// The object's lexical value: IRI string, blank node id, or literal value.
pub(crate) const COL_O_VALUE: &str = "o_value";
/// The literal datatype IRI — null unless the object is a typed literal.
pub(crate) const COL_O_DATATYPE: &str = "o_datatype";
/// The literal language tag — null unless the object is a language literal.
pub(crate) const COL_O_LANG: &str = "o_lang";

/// The primary columns:
/// `s`, `p`, `o_kind`, `o_value`, `o_datatype`, `o_lang`, `g`.
pub(crate) const COLUMNS: &[&str] = &[
    COL_S,
    COL_P,
    COL_O_KIND,
    COL_O_VALUE,
    COL_O_DATATYPE,
    COL_O_LANG,
    COL_G,
];

/// Build the primary column arrays from raw quads, decomposing each object
/// term into its typed sub-columns. An empty slice yields empty columns with
/// the correct dtypes.
pub(crate) fn build_columns(quads: &[RawQuad]) -> Result<Vec<ArrayRef>> {
    let n = quads.len();
    let mut kinds = Vec::with_capacity(n);
    let mut values = Vec::with_capacity(n);
    let mut datatypes: Vec<Option<String>> = Vec::with_capacity(n);
    let mut langs: Vec<Option<String>> = Vec::with_capacity(n);

    for q in quads {
        let term = parse_term(&q.o).ok_or_else(|| {
            VortexRdfError::Deserialization(format!("Cannot parse object string: {}", q.o))
        })?;
        let (kind, value, dt, lang) = decompose_object(&term);
        kinds.push(kind);
        values.push(value);
        datatypes.push(dt);
        langs.push(lang);
    }

    Ok(vec![
        make_string_array(quads.iter().map(|q| q.s.as_str())),
        make_string_array(quads.iter().map(|q| q.p.as_str())),
        PrimitiveArray::from_iter(kinds).into_array(),
        make_string_array(values.iter().map(String::as_str)),
        make_nullable_string_array(datatypes),
        make_nullable_string_array(langs),
        make_string_array(quads.iter().map(|q| q.g.as_str())),
    ])
}

/// Decompose an RDF object Term into typed sub-columns.
///
/// Returns `(kind, value, datatype, language)` where:
/// - 0=IRI, 1=BlankNode, 2=PlainLiteral (xsd:string), 3=LangLiteral, 4=TypedLiteral
pub(crate) fn decompose_object(term: &Term) -> (u8, String, Option<String>, Option<String>) {
    match term {
        Term::NamedNode(n) => (0, n.as_str().to_string(), None, None),
        Term::BlankNode(b) => (1, b.as_str().to_string(), None, None),
        Term::Literal(l) => {
            if let Some(lang) = l.language() {
                (3, l.value().to_string(), None, Some(lang.to_string()))
            } else {
                let dt = l.datatype().as_str();
                if dt == XSD_STRING {
                    (2, l.value().to_string(), None, None)
                } else {
                    (4, l.value().to_string(), Some(dt.to_string()), None)
                }
            }
        }
    }
}

/// Recompose a Term from decomposed typed sub-columns.
fn compose_object(
    kind: u8,
    value: &str,
    datatype: Option<&str>,
    lang: Option<&str>,
) -> Result<Term> {
    // Trusted-input decode path — see `parse_named_node`: the sub-columns
    // were decomposed from already-validated terms at build time, so the
    // checked constructors' re-validation (a full `oxiri::Iri::parse` per
    // IRI) is skipped, matching `parse_term`.
    match kind {
        0 => Ok(Term::NamedNode(NamedNode::new_unchecked(value))),
        1 => Ok(Term::BlankNode(BlankNode::new_unchecked(value))),
        2 => Ok(Term::Literal(Literal::new_simple_literal(value))),
        3 => Ok(Term::Literal(
            Literal::new_language_tagged_literal_unchecked(value, lang.unwrap_or("")),
        )),
        4 => {
            let dt_str = datatype.unwrap_or(XSD_STRING);
            Ok(Term::Literal(Literal::new_typed_literal(
                value,
                NamedNode::new_unchecked(dt_str),
            )))
        }
        _ => Err(VortexRdfError::Deserialization(format!(
            "Unknown object kind: {}",
            kind
        ))),
    }
}

/// A nullable string column, or `None` when it cannot be read as one — every
/// row then reads as null.
fn nullable_str_col(
    struct_arr: &StructArray,
    name: &str,
    ctx: &mut ExecutionCtx,
) -> Option<VarBinViewArray> {
    field_as::<VarBinViewArray>(struct_arr, name, ctx).ok()
}

/// The four object sub-columns of a chunk, executed to their canonical
/// arrays.
struct ObjectColumns {
    kind: PrimitiveArray,
    value: VarBinViewArray,
    datatype: Option<VarBinViewArray>,
    lang: Option<VarBinViewArray>,
}

impl ObjectColumns {
    /// Load the sub-columns of `struct_arr`. `o_kind` and `o_value` are
    /// required; the nullable `o_datatype`/`o_lang` fall back to all-null.
    fn load(struct_arr: &StructArray, ctx: &mut ExecutionCtx) -> Result<Self> {
        Ok(Self {
            kind: field_as::<PrimitiveArray>(struct_arr, COL_O_KIND, ctx)?,
            value: field_as::<VarBinViewArray>(struct_arr, COL_O_VALUE, ctx)?,
            datatype: nullable_str_col(struct_arr, COL_O_DATATYPE, ctx),
            lang: nullable_str_col(struct_arr, COL_O_LANG, ctx),
        })
    }

    /// Row-level readers over the loaded columns.
    fn reader(&self) -> ObjectReader<'_> {
        ObjectReader {
            kinds: self.kind.as_slice::<u8>(),
            values: StrColReader::new(&self.value),
            datatypes: self.datatype.as_ref().map(StrColReader::new),
            langs: self.lang.as_ref().map(StrColReader::new),
        }
    }
}

/// Per-row access to an [`ObjectColumns`], recomposing each row's object
/// term.
struct ObjectReader<'a> {
    kinds: &'a [u8],
    values: StrColReader<'a>,
    datatypes: Option<StrColReader<'a>>,
    langs: Option<StrColReader<'a>>,
}

impl ObjectReader<'_> {
    /// The object term at row `i`.
    fn term_at(&self, i: usize) -> Result<Term> {
        compose_object(
            self.kinds[i],
            self.values.str_at(i)?,
            nullable_str_at(self.datatypes.as_ref(), i)?,
            nullable_str_at(self.langs.as_ref(), i)?,
        )
    }
}

/// Row `i` of a nullable string column: `None` for a missing column or an
/// empty value.
fn nullable_str_at<'a>(col: Option<&StrColReader<'a>>, i: usize) -> Result<Option<&'a str>> {
    match col {
        Some(c) => {
            let s = c.str_at(i)?;
            Ok(if s.is_empty() { None } else { Some(s) })
        }
        None => Ok(None),
    }
}

/// The object terms of every row in N-Triples form, recomposed from
/// `o_kind`/`o_value`/`o_datatype`/`o_lang` — the inverse of
/// [`build_columns`]' decomposition, used by
/// [`ResolvedLayout::raw_quads`](super::ResolvedLayout::raw_quads) wherever
/// rows are rebuilt from their string form.
pub(crate) fn object_terms(struct_arr: &StructArray) -> Result<Vec<String>> {
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let columns = ObjectColumns::load(struct_arr, &mut ctx)?;
    let objects = columns.reader();
    (0..struct_arr.len())
        .map(|i| Ok(objects.term_at(i)?.to_string()))
        .collect()
}

/// A chunk with typed object sub-columns as quads; the outer `Err` is a
/// chunk-level failure, an inner `Err` a row whose terms fail to parse.
pub(crate) fn decode_chunk(chunk: &ArrayRef) -> Result<Vec<Result<Quad>>> {
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let struct_arr = chunk
        .clone()
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let n = struct_arr.len();
    let s_col = field_as::<VarBinViewArray>(&struct_arr, COL_S, &mut ctx)?;
    let p_col = field_as::<VarBinViewArray>(&struct_arr, COL_P, &mut ctx)?;
    let o_cols = ObjectColumns::load(&struct_arr, &mut ctx)?;
    let g_col = field_as::<VarBinViewArray>(&struct_arr, COL_G, &mut ctx)?;

    let subjects = StrColReader::new(&s_col);
    let predicates = StrColReader::new(&p_col);
    let objects = o_cols.reader();
    let graphs = StrColReader::new(&g_col);

    Ok((0..n)
        .map(|i| {
            let subject = parse_subject(subjects.str_at(i)?)?;
            let predicate = parse_named_node(predicates.str_at(i)?)?;
            let object = objects.term_at(i)?;
            let graph_name = parse_graph_name(graphs.str_at(i)?)?;
            Ok(Quad::new(subject, predicate, object, graph_name))
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compose_object_rejects_unknown_kind() {
        let err = compose_object(5, "x", None, None).unwrap_err();
        assert!(
            matches!(&err, VortexRdfError::Deserialization(msg) if msg.contains("Unknown object kind: 5")),
            "{err:?}"
        );
    }
}
