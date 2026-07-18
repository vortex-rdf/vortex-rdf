//! Column-building and decoding logic for [`LayoutStrategy::TypedObject`]:
//! the object column is decomposed into typed sub-columns
//! (`o_kind`, `o_value`, `o_datatype`, `o_lang`).
//!
//! [`LayoutStrategy::TypedObject`]: super::LayoutStrategy::TypedObject

use std::sync::Arc;

use oxrdf::{BlankNode, Literal, NamedNode, Quad, Term};
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::arrays::{PrimitiveArray, VarBinViewArray};

use crate::common::utils::{
    buf_as_str, get_as_term, make_nullable_string_array, make_string_array, parse_graph_name,
    parse_named_node, parse_subject,
};
use crate::error::{Result, VortexRdfError};
use crate::io::VORTEX_LIGHT_SESSION;
use crate::store::RawQuad;

/// Field names of the primary columns:
/// `s`, `p`, `o_kind`, `o_value`, `o_datatype`, `o_lang`, `g`.
pub(crate) fn field_names() -> Vec<Arc<str>> {
    vec![
        "s".into(), "p".into(),
        "o_kind".into(), "o_value".into(), "o_datatype".into(), "o_lang".into(),
        "g".into(),
    ]
}

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
        let term = get_as_term(&q.o)
            .ok_or_else(|| VortexRdfError::Deserialization(
                format!("Cannot parse object string: {}", q.o)
            ))?;
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
                if dt == "http://www.w3.org/2001/XMLSchema#string" {
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
    match kind {
        0 => NamedNode::new(value)
            .map(Term::NamedNode)
            .map_err(|e| VortexRdfError::Deserialization(e.to_string())),
        1 => Ok(Term::BlankNode(BlankNode::new_unchecked(value))),
        2 => Ok(Term::Literal(Literal::new_simple_literal(value))),
        3 => Literal::new_language_tagged_literal(value, lang.unwrap_or(""))
            .map(Term::Literal)
            .map_err(|e| VortexRdfError::Deserialization(e.to_string())),
        4 => {
            let dt_str = datatype.unwrap_or("http://www.w3.org/2001/XMLSchema#string");
            let dt = NamedNode::new(dt_str)
                .map_err(|e| VortexRdfError::Deserialization(e.to_string()))?;
            Ok(Term::Literal(Literal::new_typed_literal(value, dt)))
        }
        _ => Err(VortexRdfError::Deserialization(format!("Unknown object kind: {}", kind))),
    }
}

/// Reconstruct the object terms in N-Triples form — the representation the
/// secondary index stores — from the typed sub-columns, one per row.
///
/// The counterpart to [`build_columns`]' object decomposition, used when
/// rebuilding indexes during compaction: the index's `_idx_o_val` column
/// holds the full object term, which under this layout has to be recomposed
/// from `o_kind`/`o_value`/`o_datatype`/`o_lang`.
pub(crate) fn object_terms(struct_arr: &StructArray) -> Result<Vec<String>> {
    let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();

    let kind_col = struct_arr
        .unmasked_field_by_name("o_kind")
        .map_err(VortexRdfError::Vortex)?
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let val_col = struct_arr
        .unmasked_field_by_name("o_value")
        .map_err(VortexRdfError::Vortex)?
        .clone()
        .execute::<VarBinViewArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    // Nullable columns — try VarBinViewArray; fall back to all-None on error.
    let dt_col: Option<VarBinViewArray> = struct_arr
        .unmasked_field_by_name("o_datatype")
        .ok()
        .and_then(|c| c.clone().execute::<VarBinViewArray>(&mut ctx).ok());
    let lang_col: Option<VarBinViewArray> = struct_arr
        .unmasked_field_by_name("o_lang")
        .ok()
        .and_then(|c| c.clone().execute::<VarBinViewArray>(&mut ctx).ok());

    let kinds = kind_col.as_slice::<u8>();

    (0..struct_arr.len())
        .map(|i| {
            let val_buf = val_col.bytes_at(i);
            let dt_buf = dt_col.as_ref().map(|c| c.bytes_at(i));
            let lang_buf = lang_col.as_ref().map(|c| c.bytes_at(i));

            let dt = match dt_buf.as_ref() {
                Some(b) => {
                    let s = buf_as_str(b.as_ref())?;
                    if s.is_empty() { None } else { Some(s) }
                }
                None => None,
            };
            let lang = match lang_buf.as_ref() {
                Some(b) => {
                    let s = buf_as_str(b.as_ref())?;
                    if s.is_empty() { None } else { Some(s) }
                }
                None => None,
            };

            let object = compose_object(kinds[i], buf_as_str(val_buf.as_ref())?, dt, lang)?;
            Ok(object.to_string())
        })
        .collect()
}

/// Decode a StructArray chunk with typed object sub-columns into Quads.
pub(crate) fn decode_chunk(chunk: &ArrayRef) -> Vec<Result<Quad>> {
    let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();

    let struct_arr = match chunk.clone().execute::<StructArray>(&mut ctx) {
        Ok(a) => a,
        Err(e) => return vec![Err(VortexRdfError::Vortex(e))],
    };

    let n = struct_arr.len();

    macro_rules! get_str_col {
        ($name:expr) => {
            match struct_arr
                .unmasked_field_by_name($name)
                .map_err(VortexRdfError::Vortex)
                .and_then(|c| c.clone().execute::<VarBinViewArray>(&mut ctx).map_err(VortexRdfError::Vortex))
            {
                Ok(arr) => arr,
                Err(e) => return vec![Err(e)],
            }
        };
    }

    let s_col = get_str_col!("s");
    let p_col = get_str_col!("p");
    let kind_col = match struct_arr
        .unmasked_field_by_name("o_kind")
        .map_err(VortexRdfError::Vortex)
        .and_then(|c| c.clone().execute::<PrimitiveArray>(&mut ctx).map_err(VortexRdfError::Vortex))
    {
        Ok(a) => a,
        Err(e) => return vec![Err(e)],
    };
    let val_col = get_str_col!("o_value");
    let g_col = get_str_col!("g");

    // Nullable columns — try VarBinViewArray; fall back to all-None on error.
    let dt_col: Option<VarBinViewArray> = struct_arr
        .unmasked_field_by_name("o_datatype")
        .ok()
        .and_then(|c| c.clone().execute::<VarBinViewArray>(&mut ctx).ok());
    let lang_col: Option<VarBinViewArray> = struct_arr
        .unmasked_field_by_name("o_lang")
        .ok()
        .and_then(|c| c.clone().execute::<VarBinViewArray>(&mut ctx).ok());

    let kinds = kind_col.as_slice::<u8>();

    (0..n)
        .map(|i| {
            // Borrow &str views over the column buffers (zero-copy);
            // the oxrdf constructors make the single owned copy.
            let s_buf = s_col.bytes_at(i);
            let p_buf = p_col.bytes_at(i);
            let kind = kinds[i];
            let val_buf = val_col.bytes_at(i);
            let g_buf = g_col.bytes_at(i);
            let dt_buf = dt_col.as_ref().map(|c| c.bytes_at(i));
            let lang_buf = lang_col.as_ref().map(|c| c.bytes_at(i));

            let dt = match dt_buf.as_ref() {
                Some(b) => {
                    let s = buf_as_str(b.as_ref())?;
                    if s.is_empty() { None } else { Some(s) }
                }
                None => None,
            };
            let lang = match lang_buf.as_ref() {
                Some(b) => {
                    let s = buf_as_str(b.as_ref())?;
                    if s.is_empty() { None } else { Some(s) }
                }
                None => None,
            };

            let subject = parse_subject(buf_as_str(s_buf.as_ref())?)?;
            let predicate = parse_named_node(buf_as_str(p_buf.as_ref())?)?;
            let object = compose_object(kind, buf_as_str(val_buf.as_ref())?, dt, lang)?;
            let graph_name = parse_graph_name(buf_as_str(g_buf.as_ref())?)?;
            Ok(Quad::new(subject, predicate, object, graph_name))
        })
        .collect()
}
