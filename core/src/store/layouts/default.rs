//! Column-building and decoding logic for [`LayoutStrategy::Default`]:
//! all four quad fields stored as opaque UTF-8 strings in N-Triples form.
//!
//! [`LayoutStrategy::Default`]: super::LayoutStrategy::Default

use oxrdf::Quad;
use vortex_array::arrays::VarBinViewArray;
use vortex_array::arrays::struct_::StructArray;
use vortex_array::{ArrayRef, VortexSessionExecute};

use crate::common::terms::quad_from_terms;
use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::RawQuad;
use crate::store::array::{StrColReader, field_as, make_string_array};
use crate::store::schema::{COL_G, COL_O, COL_P, COL_S, PRIMARY_COLUMNS};

/// The primary columns: `s`, `p`, `o`, `g`.
pub(crate) const COLUMNS: &[&str] = &PRIMARY_COLUMNS;

/// Build the primary column arrays from raw quads. An empty slice yields
/// empty columns with the correct dtypes.
pub(crate) fn build_columns(quads: &[RawQuad]) -> Vec<ArrayRef> {
    vec![
        make_string_array(quads.iter().map(|q| q.s.as_str())),
        make_string_array(quads.iter().map(|q| q.p.as_str())),
        make_string_array(quads.iter().map(|q| q.o.as_str())),
        make_string_array(quads.iter().map(|q| q.g.as_str())),
    ]
}

/// Decode a StructArray chunk with `s`/`p`/`o`/`g` string columns into Quads.
pub(crate) fn decode_chunk(chunk: &ArrayRef) -> Vec<Result<Quad>> {
    let mut ctx = VORTEX_SESSION.create_execution_ctx();

    let struct_arr = match chunk.clone().execute::<StructArray>(&mut ctx) {
        Ok(a) => a,
        Err(e) => return vec![Err(VortexRdfError::Vortex(e))],
    };

    let n = struct_arr.len();

    let mut column = |name| field_as::<VarBinViewArray>(&struct_arr, name, &mut ctx);
    let columns = (|| {
        Ok((
            column(COL_S)?,
            column(COL_P)?,
            column(COL_O)?,
            column(COL_G)?,
        ))
    })();
    let (s_col, p_col, o_col, g_col) = match columns {
        Ok(columns) => columns,
        Err(e) => return vec![Err(e)],
    };

    let s = StrColReader::new(&s_col);
    let p = StrColReader::new(&p_col);
    let o = StrColReader::new(&o_col);
    let g = StrColReader::new(&g_col);

    (0..n)
        .map(|i| quad_from_terms(s.str_at(i), p.str_at(i), o.str_at(i), g.str_at(i)))
        .collect()
}
