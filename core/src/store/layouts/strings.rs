//! The string layouts' rows read back ([`StringLayout`]): Default's four
//! N-Triples columns, and TypedObject's, whose object is spelled from its
//! typed sub-columns. Each representation a chunk decodes into reads a
//! string layout's rows here ([`ChunkDecode::from_strings`]); the Dictionary
//! layout's codes decode in its own leaf.
//!
//! [`ChunkDecode::from_strings`]: super::ChunkDecode::from_strings

use std::sync::Arc;

use oxrdf::Quad;
use vortex_array::arrays::VarBinViewArray;
use vortex_array::arrays::struct_::StructArray;
use vortex_array::{ArrayRef, VortexSessionExecute};

use super::{ResolvedLayout, default, typed_object};
use crate::common::quad::{NQuadLines, SharedQuad};
use crate::error::Result;
use crate::session::VORTEX_SESSION;
use crate::store::array::{StrColReader, field_as};
use crate::store::schema::{COL_G, COL_O, COL_P, COL_S};
use crate::store::{LayoutStrategy, RawQuad};

/// A layout that stores its terms as N-Triples strings, read in place.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StringLayout {
    /// Four string columns.
    Default,
    /// Three string columns, the object in typed sub-columns.
    TypedObject,
}

impl From<StringLayout> for ResolvedLayout {
    fn from(layout: StringLayout) -> Self {
        match layout {
            StringLayout::Default => ResolvedLayout::Default,
            StringLayout::TypedObject => ResolvedLayout::TypedObject,
        }
    }
}

impl StringLayout {
    /// The build-time strategy tag of this layout.
    pub(crate) fn strategy(self) -> LayoutStrategy {
        match self {
            StringLayout::Default => LayoutStrategy::Default,
            StringLayout::TypedObject => LayoutStrategy::TypedObject,
        }
    }

    /// `chunk`'s rows as oxrdf quads, each term parsed from its column.
    pub(super) fn quads(self, chunk: &ArrayRef) -> Vec<Result<Quad>> {
        match self {
            StringLayout::Default => default::decode_chunk(chunk),
            StringLayout::TypedObject => typed_object::decode_chunk(chunk),
        }
    }

    /// `rows` as [`RawQuad`]s, each term in its N-Triples string form: the
    /// string columns verbatim, a TypedObject object recomposed from its
    /// sub-columns.
    pub(crate) fn raw_quads(self, rows: &ArrayRef) -> Result<Vec<RawQuad>> {
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let struct_arr = rows.clone().execute::<StructArray>(&mut ctx)?;
        let objects = match self {
            StringLayout::Default => read_string_column(&struct_arr, COL_O)?,
            StringLayout::TypedObject => typed_object::object_terms(&struct_arr)?,
        };
        Ok(read_string_column(&struct_arr, COL_S)?
            .into_iter()
            .zip(read_string_column(&struct_arr, COL_P)?)
            .zip(objects)
            .zip(read_string_column(&struct_arr, COL_G)?)
            .map(|(((s, p), o), g)| RawQuad { s, p, o, g })
            .collect())
    }

    /// `rows` as [`SharedQuad`]s. Each term is read in place (a TypedObject
    /// object spelled from its sub-columns into one buffer) and copied into a
    /// fresh `Arc<str>` only where it differs from the same role's term in
    /// the row before, so a run of rows repeating a term, as rows sorted by
    /// subject repeat their subject, predicate and graph, shares one
    /// allocation.
    pub(super) fn shared_quads(self, rows: &ArrayRef) -> Result<Vec<SharedQuad>> {
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let struct_arr = rows.clone().execute::<StructArray>(&mut ctx)?;
        let n = struct_arr.len();
        let s = field_as::<VarBinViewArray>(&struct_arr, COL_S, &mut ctx)?;
        let p = field_as::<VarBinViewArray>(&struct_arr, COL_P, &mut ctx)?;
        let g = field_as::<VarBinViewArray>(&struct_arr, COL_G, &mut ctx)?;
        let objects: Vec<Arc<str>> = match self {
            StringLayout::Default => {
                let o = field_as::<VarBinViewArray>(&struct_arr, COL_O, &mut ctx)?;
                shared_column(&o)
            }
            StringLayout::TypedObject => {
                let columns = typed_object::ObjectColumns::load(&struct_arr, &mut ctx)?;
                let reader = columns.reader();
                let mut spelling = String::new();
                let mut run = None;
                (0..n)
                    .map(|i| {
                        spelling.clear();
                        reader.push_spelling(i, &mut spelling)?;
                        Ok(shared_term(&mut run, &spelling))
                    })
                    .collect::<Result<_>>()?
            }
        };
        Ok(shared_column(&s)
            .into_iter()
            .zip(shared_column(&p))
            .zip(objects)
            .zip(shared_column(&g))
            .map(|(((s, p), o), g)| SharedQuad { s, p, o, g })
            .collect())
    }

    /// `rows` as N-Triples/N-Quads lines, each term read in place (a
    /// TypedObject object spelled from its sub-columns straight into the
    /// lines).
    pub(super) fn nquad_lines(self, rows: &ArrayRef) -> Result<NQuadLines> {
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let struct_arr = rows.clone().execute::<StructArray>(&mut ctx)?;
        let n = struct_arr.len();
        let s = field_as::<VarBinViewArray>(&struct_arr, COL_S, &mut ctx)?;
        let p = field_as::<VarBinViewArray>(&struct_arr, COL_P, &mut ctx)?;
        let g = field_as::<VarBinViewArray>(&struct_arr, COL_G, &mut ctx)?;
        let (s, p, g) = (
            StrColReader::new(&s),
            StrColReader::new(&p),
            StrColReader::new(&g),
        );
        let mut lines = NQuadLines::with_capacity(n);
        match self {
            StringLayout::Default => {
                let o = field_as::<VarBinViewArray>(&struct_arr, COL_O, &mut ctx)?;
                let o = StrColReader::new(&o);
                for i in 0..n {
                    lines.push([s.str_at(i), p.str_at(i), o.str_at(i), g.str_at(i)]);
                }
            }
            StringLayout::TypedObject => {
                let columns = typed_object::ObjectColumns::load(&struct_arr, &mut ctx)?;
                let objects = columns.reader();
                for i in 0..n {
                    lines.term(s.str_at(i));
                    lines.term(p.str_at(i));
                    objects.push_spelling(i, &mut lines.text)?;
                    lines.text.push(' ');
                    lines.end(g.str_at(i));
                }
            }
        }
        Ok(lines)
    }
}

/// A UTF-8 string column read into owned term strings, one per row. The bytes
/// are taken as UTF-8 unchecked (`StrColReader`): they must come from a Utf8
/// column of a store this crate wrote or the caller trusts.
fn read_string_column(struct_arr: &StructArray, name: &str) -> Result<Vec<String>> {
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let col = field_as::<VarBinViewArray>(struct_arr, name, &mut ctx)?;
    let reader = StrColReader::new(&col);
    Ok((0..col.len())
        .map(|i| reader.str_at(i).to_string())
        .collect())
}

/// `term` as a shared string: `run`'s, the term of the row before, when the
/// two are equal, else a fresh copy, which becomes `run`.
fn shared_term(run: &mut Option<Arc<str>>, term: &str) -> Arc<str> {
    if let Some(held) = run
        && **held == *term
    {
        return Arc::clone(held);
    }
    let fresh: Arc<str> = Arc::from(term);
    *run = Some(Arc::clone(&fresh));
    fresh
}

/// A UTF-8 string column's terms as shared strings ([`shared_term`]), each
/// run of equal terms one allocation. Unchecked UTF-8, as
/// [`read_string_column`] reads.
fn shared_column(col: &VarBinViewArray) -> Vec<Arc<str>> {
    let reader = StrColReader::new(col);
    let mut run = None;
    (0..col.len())
        .map(|i| shared_term(&mut run, reader.str_at(i)))
        .collect()
}
