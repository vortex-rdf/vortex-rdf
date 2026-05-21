use crate::common::{indexes, utils};
use crate::error::{Result, VortexRdfError};
use crate::index::RdfDictionary;
use crate::store::layout::{RdfQuadLayout, RdfQuadLayoutBuilder};
use futures::{Stream, StreamExt, stream};
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
use std::sync::Arc;
use vortex_array::arrays::bool::BoolArrayExt;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::arrays::{BoolArray, ConstantArray, PrimitiveArray, StructArray};
use vortex_array::builtins::ArrayBuiltins;
use vortex_array::scalar::Scalar;
use vortex_array::scalar_fn::fns::operators::Operator;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex::VortexSessionDefault;
use vortex::session::VortexSession;

#[derive(Clone, Debug)]
pub struct FlatLayout;

impl<Dict> RdfQuadLayout<Dict> for FlatLayout
where
    Dict: RdfDictionary,
{
    const STORAGE_LAYOUT: &'static str = "flat-spog";

    fn empty_quads() -> Result<ArrayRef> {
        build_spog_struct_array(vec![], vec![], vec![], vec![])
    }

    fn extract_quads(root: &ArrayRef) -> Result<ArrayRef> {
        let session = VortexSession::default();
        let mut ctx = session.create_execution_ctx();
        let vortex_struct = root
            .clone()
            .execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;

        utils::extract_vortex_struct_field(&vortex_struct, "quads")
    }

    async fn build_vortex_index(
        quad_stream: impl Stream<Item = Result<Quad>> + Unpin + Send + 'static,
    ) -> Result<ArrayRef> {
        let mut dictionary = Dict::new();

        let quads: Vec<Quad> = quad_stream
            .collect::<Vec<Result<Quad>>>()
            .await
            .into_iter()
            .collect::<Result<Vec<Quad>>>()?;

        let mut s_ids = Vec::with_capacity(quads.len());
        let mut p_ids = Vec::with_capacity(quads.len());
        let mut o_ids = Vec::with_capacity(quads.len());
        let mut g_ids = Vec::with_capacity(quads.len());
        let mut term_strings = Vec::with_capacity(quads.len() * 4);

        for quad in &quads {
            term_strings.push(quad.subject.to_string());
            term_strings.push(quad.predicate.to_string());
            term_strings.push(quad.object.to_string());
            term_strings.push(quad.graph_name.to_string());
        }

        let all_ids = dictionary
            .get_or_insert_bulk(&term_strings.iter().map(String::as_str).collect::<Vec<_>>());

        for i in 0..quads.len() {
            s_ids.push(all_ids[i * 4]);
            p_ids.push(all_ids[i * 4 + 1]);
            o_ids.push(all_ids[i * 4 + 2]);
            g_ids.push(all_ids[i * 4 + 3]);
        }

        let quads_flat = StructArray::from_fields(&[
            ("s", PrimitiveArray::from_iter(s_ids).into_array()),
            ("p", PrimitiveArray::from_iter(p_ids).into_array()),
            ("o", PrimitiveArray::from_iter(o_ids).into_array()),
            ("g", PrimitiveArray::from_iter(g_ids).into_array()),
        ])
        .map_err(VortexRdfError::Vortex)?
        .into_array();

        let dict_fields_raw = dictionary.to_vortex_array()?;

        let mut field_names: Vec<Arc<str>> = Vec::new();
        let mut field_arrays: Vec<ArrayRef> = Vec::new();

        field_names.push("store_type".into());
        field_arrays.push(ConstantArray::new(Dict::store_type(), 1).into_array());

        for (name, arr) in dict_fields_raw {
            field_names.push(name.into());
            field_arrays.push(indexes::wrap_array_in_list(arr)?);
        }

        field_names.push("quads".into());
        field_arrays.push(indexes::wrap_array_in_list(quads_flat)?);

        let vortex_array =
            StructArray::try_new(field_names.into(), field_arrays, 1, Validity::NonNullable)
                .map_err(VortexRdfError::Vortex)?
                .into_array();

        Ok(vortex_array)
    }

    fn quads<'a>(
        dictionary: &'a Dict,
        quads: &'a ArrayRef,
    ) -> Result<Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + 'a>> {
        let session = VortexSession::default();
        let mut ctx = session.create_execution_ctx();
        let quads_struct = quads
            .clone()
            .execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        let fields = quads_struct.unmasked_fields();

        let s_ids = fields
            .get(0)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing S IDs".to_string()))?
            .clone()
            .execute::<PrimitiveArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        let p_ids = fields
            .get(1)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing P IDs".to_string()))?
            .clone()
            .execute::<PrimitiveArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        let o_ids = fields
            .get(2)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing O IDs".to_string()))?
            .clone()
            .execute::<PrimitiveArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        let g_ids = fields
            .get(3)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing G IDs".to_string()))?
            .clone()
            .execute::<PrimitiveArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;

        let len = s_ids.len();

        let iter = (0..len).map(move |i| {
            let s_id = s_ids.as_slice::<u32>()[i];
            let p_id = p_ids.as_slice::<u32>()[i];
            let o_id = o_ids.as_slice::<u32>()[i];
            let g_id = g_ids.as_slice::<u32>()[i];

            let s_term = dictionary.get_term(s_id).ok_or_else(|| {
                VortexRdfError::Deserialization(format!("S ID {} not in dictionary", s_id))
            })?;
            let p_term = dictionary.get_term(p_id).ok_or_else(|| {
                VortexRdfError::Deserialization(format!("P ID {} not in dictionary", p_id))
            })?;
            let o_term = dictionary.get_term(o_id).ok_or_else(|| {
                VortexRdfError::Deserialization(format!("O ID {} not in dictionary", o_id))
            })?;
            let g_name = dictionary.get_graph_name(g_id).ok_or_else(|| {
                VortexRdfError::Deserialization(format!("G ID {} not in dictionary", g_id))
            })?;

            let subject = match s_term {
                Term::NamedNode(n) => NamedOrBlankNode::NamedNode(n),
                Term::BlankNode(b) => NamedOrBlankNode::BlankNode(b),
                _ => {
                    return Err(VortexRdfError::Deserialization(
                        "Invalid subject type".to_string(),
                    ));
                }
            };
            let predicate = match p_term {
                Term::NamedNode(n) => n,
                _ => {
                    return Err(VortexRdfError::Deserialization(
                        "Invalid predicate type".to_string(),
                    ));
                }
            };

            Ok(Quad::new(subject, predicate, o_term, g_name))
        });

        Ok(Box::new(stream::iter(iter)))
    }

    fn find_mask(
        dictionary: &Dict,
        quads: &ArrayRef,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Option<ArrayRef>> {
        let session = VortexSession::default();
        let mut ctx = session.create_execution_ctx();
        let quads_struct = quads
            .clone()
            .execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        let fields = quads_struct.unmasked_fields();

        let mut mask: Option<ArrayRef> = None;

        let mut combine_mask = |new_mask: ArrayRef| -> Result<()> {
            if let Some(m) = mask.take() {
                mask = Some(
                    m.binary(new_mask, Operator::And)
                        .map_err(VortexRdfError::Vortex)?,
                );
            } else {
                mask = Some(new_mask);
            }
            Ok(())
        };

        let patterns = [
            (subject.map(|s| s.to_string()), 0, "Subject"),
            (predicate.map(|p| p.to_string()), 1, "Predicate"),
            (object.map(|o| o.to_string()), 2, "Object"),
            (graph.map(|g| g.to_string()), 3, "Graph"),
        ];

        for (term_opt, col_idx, _label) in patterns {
            if let Some(term_str) = term_opt {
                if let Some(id) = dictionary.get_id(&term_str) {
                    let col = fields.get(col_idx).unwrap();
                    let scalar = Scalar::from(id)
                        .cast(col.dtype())
                        .map_err(VortexRdfError::Vortex)?;
                    let column_mask = col
                        .binary(
                            ConstantArray::new(scalar, col.len()).into_array(),
                            Operator::Eq,
                        )
                        .map_err(VortexRdfError::Vortex)?;
                    combine_mask(column_mask)?;
                } else {
                    return Ok(Some(ConstantArray::new(false, quads.len()).into_array()));
                }
            }
        }

        Ok(mask)
    }

    fn add_quad(dictionary: &mut Dict, quads: &ArrayRef, quad: Quad) -> Result<ArrayRef> {
        let (mut s_ids, mut p_ids, mut o_ids, mut g_ids) = extract_id_columns(quads)?;

        s_ids.push(dictionary.get_or_insert(&quad.subject.to_string()));
        p_ids.push(dictionary.get_or_insert(&quad.predicate.to_string()));
        o_ids.push(dictionary.get_or_insert(&quad.object.to_string()));
        g_ids.push(dictionary.get_or_insert(&quad.graph_name.to_string()));

        build_spog_struct_array(s_ids, p_ids, o_ids, g_ids)
    }

    fn delete_quad(dictionary: &Dict, quads: &ArrayRef, quad: &Quad) -> Result<ArrayRef> {
        let mask = Self::find_mask(
            dictionary,
            quads,
            Some(&quad.subject),
            Some(&quad.predicate),
            Some(&quad.object),
            Some(&quad.graph_name),
        )?;

        if let Some(m) = mask {
            let inverse_mask = m.not().map_err(VortexRdfError::Vortex)?;
            let session = VortexSession::default();
            let mut ctx = session.create_execution_ctx();
            let bool_arr = inverse_mask
                .execute::<BoolArray>(&mut ctx)
                .map_err(VortexRdfError::Vortex)?;
            let canonical_mask = bool_arr.to_mask_fill_null_false(&mut ctx);
            let filtered = quads
                .filter(canonical_mask)
                .map_err(VortexRdfError::Vortex)?;
            Ok(filtered)
        } else {
            Ok(quads.clone())
        }
    }
}

pub struct FlatLayoutBuilder {
    s_ids: Vec<u32>,
    p_ids: Vec<u32>,
    o_ids: Vec<u32>,
    g_ids: Vec<u32>,
}

impl FlatLayoutBuilder {
    pub fn new() -> Self {
        Self {
            s_ids: Vec::new(),
            p_ids: Vec::new(),
            o_ids: Vec::new(),
            g_ids: Vec::new(),
        }
    }
}

impl<Dict> RdfQuadLayoutBuilder<Dict> for FlatLayoutBuilder
where
    Dict: RdfDictionary,
{
    fn ingest(&mut self, quad: &Quad, dictionary: &mut Dict) -> Result<()> {
        self.s_ids
            .push(dictionary.get_or_insert(&quad.subject.to_string()));
        self.p_ids
            .push(dictionary.get_or_insert(&quad.predicate.to_string()));
        self.o_ids
            .push(dictionary.get_or_insert(&quad.object.to_string()));
        self.g_ids
            .push(dictionary.get_or_insert(&quad.graph_name.to_string()));
        Ok(())
    }

    fn finalize(&mut self, _dictionary: &mut Dict) -> Result<()> {
        Ok(())
    }

    fn build_quads(&self) -> Result<ArrayRef> {
        let arr = StructArray::from_fields(&[
            (
                "s",
                PrimitiveArray::from_iter(self.s_ids.clone()).into_array(),
            ),
            (
                "p",
                PrimitiveArray::from_iter(self.p_ids.clone()).into_array(),
            ),
            (
                "o",
                PrimitiveArray::from_iter(self.o_ids.clone()).into_array(),
            ),
            (
                "g",
                PrimitiveArray::from_iter(self.g_ids.clone()).into_array(),
            ),
        ])
        .map_err(VortexRdfError::Vortex)?
        .into_array();

        Ok(arr)
    }
}

fn extract_id_columns(quads: &ArrayRef) -> Result<(Vec<u32>, Vec<u32>, Vec<u32>, Vec<u32>)> {
    let session = VortexSession::default();
    let mut ctx = session.create_execution_ctx();

    let quads_struct = quads
        .clone()
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;

    let fields = quads_struct.unmasked_fields();

    let extract = |idx: usize, name: &str, ctx: &mut _| -> Result<Vec<u32>> {
        let arr = fields
            .get(idx)
            .ok_or_else(|| VortexRdfError::Deserialization(format!("Missing {} column", name)))?
            .clone()
            .execute::<PrimitiveArray>(ctx)
            .map_err(VortexRdfError::Vortex)?;

        Ok(arr.as_slice::<u32>().to_vec())
    };

    let s_ids = extract(0, "s", &mut ctx)?;
    let p_ids = extract(1, "p", &mut ctx)?;
    let o_ids = extract(2, "o", &mut ctx)?;
    let g_ids = extract(3, "g", &mut ctx)?;

    Ok((s_ids, p_ids, o_ids, g_ids))
}

fn build_spog_struct_array(
    s_ids: Vec<u32>,
    p_ids: Vec<u32>,
    o_ids: Vec<u32>,
    g_ids: Vec<u32>,
) -> Result<ArrayRef> {
    StructArray::from_fields(&[
        ("s", PrimitiveArray::from_iter(s_ids).into_array()),
        ("p", PrimitiveArray::from_iter(p_ids).into_array()),
        ("o", PrimitiveArray::from_iter(o_ids).into_array()),
        ("g", PrimitiveArray::from_iter(g_ids).into_array()),
    ])
    .map_err(VortexRdfError::Vortex)
    .map(|arr| arr.into_array())
}
