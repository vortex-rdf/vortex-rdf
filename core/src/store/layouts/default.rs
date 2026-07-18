//! Column-building and decoding logic for [`LayoutStrategy::Default`]:
//! all four quad fields stored as opaque UTF-8 strings in N-Triples form.
//!
//! [`LayoutStrategy::Default`]: super::LayoutStrategy::Default

use std::fmt::Write as _;
use std::sync::Arc;

use oxrdf::{GraphName, Quad};
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::arrays::VarBinViewArray;
use vortex_array::builders::VarBinViewBuilder;
use vortex_array::dtype::{DType, Nullability};
use vortex_array::validity::Validity;

use crate::common::utils::{
    buf_as_str, get_as_term, make_string_array, parse_graph_name, parse_named_node, parse_subject,
};
use crate::error::{Result, VortexRdfError};
use crate::io::VORTEX_LIGHT_SESSION;
use crate::store::RawQuad;

/// Field names of the primary columns: `s`, `p`, `o`, `g`.
pub(crate) fn field_names() -> Vec<Arc<str>> {
    vec!["s".into(), "p".into(), "o".into(), "g".into()]
}

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
    let o_col = get_str_col!("o");
    let g_col = get_str_col!("g");

    (0..n)
        .map(|i| {
            // Borrow &str views over the column buffers (zero-copy);
            // the oxrdf constructors make the single owned copy.
            let s_buf = s_col.bytes_at(i);
            let p_buf = p_col.bytes_at(i);
            let o_buf = o_col.bytes_at(i);
            let g_buf = g_col.bytes_at(i);
            decode_spog(
                buf_as_str(s_buf.as_ref())?,
                buf_as_str(p_buf.as_ref())?,
                buf_as_str(o_buf.as_ref())?,
                buf_as_str(g_buf.as_ref())?,
            )
        })
        .collect()
}

pub(crate) fn decode_spog(s: &str, p: &str, o: &str, g: &str) -> Result<Quad> {
    let subject = parse_subject(s)?;
    let predicate = parse_named_node(p)?;
    let object = get_as_term(o)
        .ok_or_else(|| VortexRdfError::Deserialization(format!("Invalid object: {}", o)))?;
    let graph_name = parse_graph_name(g)?;
    Ok(Quad::new(subject, predicate, object, graph_name))
}

/// Column builders for the Default layout, filled directly from quads.
///
/// Terms are formatted (via `Display`, the same N-Triples form `RawQuad` uses)
/// into a single reused `String` buffer and appended into the column builders,
/// so steady-state ingestion performs no per-quad heap allocations.
pub(crate) struct DirectChunkBuilder {
    s: VarBinViewBuilder,
    p: VarBinViewBuilder,
    o: VarBinViewBuilder,
    g: VarBinViewBuilder,
    len: usize,
    fmt_buf: String,
}

impl DirectChunkBuilder {
    pub(crate) fn new(capacity: usize) -> Self {
        let col = || VarBinViewBuilder::with_capacity(DType::Utf8(Nullability::NonNullable), capacity);
        Self {
            s: col(),
            p: col(),
            o: col(),
            g: col(),
            len: 0,
            fmt_buf: String::with_capacity(256),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(crate) fn push(&mut self, q: &Quad) {
        self.fmt_buf.clear();
        write!(self.fmt_buf, "{}", q.subject).expect("write to String");
        self.s.append_value(&self.fmt_buf);

        self.fmt_buf.clear();
        write!(self.fmt_buf, "{}", q.predicate).expect("write to String");
        self.p.append_value(&self.fmt_buf);

        self.fmt_buf.clear();
        write!(self.fmt_buf, "{}", q.object).expect("write to String");
        self.o.append_value(&self.fmt_buf);

        match &q.graph_name {
            GraphName::DefaultGraph => self.g.append_value(""),
            other => {
                self.fmt_buf.clear();
                write!(self.fmt_buf, "{}", other).expect("write to String");
                self.g.append_value(&self.fmt_buf);
            }
        }

        self.len += 1;
    }

    pub(crate) fn finish(mut self) -> Result<ArrayRef> {
        StructArray::try_new(
            field_names().into(),
            vec![
                self.s.finish_into_varbinview().into_array(),
                self.p.finish_into_varbinview().into_array(),
                self.o.finish_into_varbinview().into_array(),
                self.g.finish_into_varbinview().into_array(),
            ],
            self.len,
            Validity::NonNullable,
        )
        .map_err(VortexRdfError::Vortex)
        .map(|a| a.into_array())
    }
}
