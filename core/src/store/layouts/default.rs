//! Column building and decoding for `LayoutStrategy::Default`: the four quad
//! fields as UTF-8 strings in N-Triples form.

use oxrdf::Quad;
use vortex_array::arrays::VarBinViewArray;
use vortex_array::arrays::struct_::StructArray;
use vortex_array::{ArrayRef, VortexSessionExecute};

use crate::common::terms::quad_from_terms;
use crate::error::Result;
use crate::session::VORTEX_SESSION;
use crate::store::RawQuad;
use crate::store::array::{StrColReader, field_as, make_string_array};
use crate::store::schema::{COL_G, COL_O, COL_P, COL_S, PRIMARY_COLUMNS};

/// The primary columns: `s`, `p`, `o`, `g`.
pub(crate) const COLUMNS: &[&str] = &PRIMARY_COLUMNS;

/// The primary column arrays of `quads`; an empty slice yields empty columns
/// of the right dtypes.
pub(crate) fn build_columns(quads: &[RawQuad]) -> Vec<ArrayRef> {
    vec![
        make_string_array(quads.iter().map(|q| q.s.as_str())),
        make_string_array(quads.iter().map(|q| q.p.as_str())),
        make_string_array(quads.iter().map(|q| q.o.as_str())),
        make_string_array(quads.iter().map(|q| q.g.as_str())),
    ]
}

/// A chunk with `s`/`p`/`o`/`g` string columns as quads; the outer `Err` is
/// a chunk-level failure, an inner `Err` a row whose terms fail to parse.
pub(crate) fn decode_chunk(chunk: &ArrayRef) -> Result<Vec<Result<Quad>>> {
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let struct_arr = chunk.clone().execute::<StructArray>(&mut ctx)?;
    let n = struct_arr.len();
    let mut column = |name| field_as::<VarBinViewArray>(&struct_arr, name, &mut ctx);
    let (s_col, p_col, o_col, g_col) = (
        column(COL_S)?,
        column(COL_P)?,
        column(COL_O)?,
        column(COL_G)?,
    );

    let s = StrColReader::new(&s_col);
    let p = StrColReader::new(&p_col);
    let o = StrColReader::new(&o_col);
    let g = StrColReader::new(&g_col);

    Ok((0..n)
        .map(|i| quad_from_terms(s.str_at(i)?, p.str_at(i)?, o.str_at(i)?, g.str_at(i)?))
        .collect())
}
