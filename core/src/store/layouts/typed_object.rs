//! Column building and decoding for `LayoutStrategy::TypedObject`: the
//! object column decomposed into `o_kind`, `o_value`, `o_datatype` and
//! `o_lang`.

use oxrdf::{BlankNode, Literal, NamedNode, Quad, Term};
use vortex_array::arrays::struct_::StructArray;
use vortex_array::arrays::{PrimitiveArray, VarBinViewArray};
use vortex_array::{ArrayRef, ExecutionCtx, IntoArray, VortexSessionExecute};

use crate::common::terms::{Trust, graph_name, named_node, subject, term};
use crate::common::vocab::XSD_STRING;
use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::RawQuad;
use crate::store::array::{StrColReader, field_as, make_string_array};
use crate::store::schema::{COL_G, COL_P, COL_S};
pub(crate) use crate::store::schema::{COL_O_DATATYPE, COL_O_KIND, COL_O_LANG, COL_O_VALUE};

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

/// The primary column arrays of `quads`, each object decomposed into its
/// typed sub-columns; an empty slice yields empty columns of the right
/// dtypes.
pub(crate) fn build_columns(quads: &[RawQuad]) -> Result<Vec<ArrayRef>> {
    let n = quads.len();
    let mut kinds = Vec::with_capacity(n);
    let mut values = Vec::with_capacity(n);
    let mut datatypes: Vec<Option<String>> = Vec::with_capacity(n);
    let mut langs: Vec<Option<String>> = Vec::with_capacity(n);

    for q in quads {
        let (kind, value, dt, lang) = decompose_object(&term(&q.o, Trust::Stored)?);
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
        VarBinViewArray::from_iter_nullable_str(datatypes).into_array(),
        VarBinViewArray::from_iter_nullable_str(langs).into_array(),
        make_string_array(quads.iter().map(|q| q.g.as_str())),
    ])
}

/// An object term as `(kind, value, datatype, language)`: kind 0 = IRI,
/// 1 = blank node, 2 = plain literal (`xsd:string`), 3 = language-tagged
/// literal, 4 = typed literal.
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

/// The inverse of [`decompose_object`], through the unchecked constructors
/// (the sub-columns hold terms validated at build time).
fn compose_object(
    kind: u8,
    value: &str,
    datatype: Option<&str>,
    lang: Option<&str>,
) -> Result<Term> {
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

/// The four object sub-columns of a chunk as canonical arrays; a missing or
/// unreadable `o_datatype`/`o_lang` reads as all-null.
struct ObjectColumns {
    kind: PrimitiveArray,
    value: VarBinViewArray,
    datatype: Option<VarBinViewArray>,
    lang: Option<VarBinViewArray>,
}

impl ObjectColumns {
    fn load(struct_arr: &StructArray, ctx: &mut ExecutionCtx) -> Result<Self> {
        Ok(Self {
            kind: field_as::<PrimitiveArray>(struct_arr, COL_O_KIND, ctx)?,
            value: field_as::<VarBinViewArray>(struct_arr, COL_O_VALUE, ctx)?,
            datatype: field_as::<VarBinViewArray>(struct_arr, COL_O_DATATYPE, ctx).ok(),
            lang: field_as::<VarBinViewArray>(struct_arr, COL_O_LANG, ctx).ok(),
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

/// Per-row access to an [`ObjectColumns`].
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

/// Every row's object in N-Triples form, recomposed from the sub-columns.
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
    let struct_arr = chunk.clone().execute::<StructArray>(&mut ctx)?;
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
            Ok(Quad::new(
                subject(subjects.str_at(i)?, Trust::Stored)?,
                named_node(predicates.str_at(i)?, Trust::Stored)?,
                objects.term_at(i)?,
                graph_name(graphs.str_at(i)?, Trust::Stored)?,
            ))
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
