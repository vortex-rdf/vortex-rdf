use crate::common::{indexes, utils};
use crate::error::{Result, VortexRdfError};
use crate::index::RdfDictionary;
use crate::io::de;

use futures::{Stream, StreamExt, stream};
use oxrdf::{GraphName, NamedNode, Quad, Subject, Term};
use std::time::Instant;

use vortex::compute::{Operator, and, compare, filter};
use vortex_array::arrays::{ChunkedArray, ConstantArray, PrimitiveArray, StructArray};
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, Canonical, IntoArray, ToCanonical};
use vortex_mask::Mask;
use vortex_scalar::Scalar;

use crate::store::VortexRdfStoreLike;

impl<Dict: RdfDictionary> VortexRdfStoreLike<Dict> for VortexRdfStore<Dict> {
    fn dictionary(&self) -> &Dict {
        &self.dictionary
    }

    fn quads_array(&self) -> &ArrayRef {
        &self.quads
    }

    fn quads_stream(&self) -> Result<Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + '_>> {
        self.quads() // reuse existing method
    }
}

use std::sync::Arc;

pub trait LayoutStrategy<Dict: RdfDictionary>: Send {
    fn ingest(&mut self, quad: &Quad, dictionary: &mut Dict) -> Result<()>;

    fn finalize(&mut self, dictionary: &mut Dict) -> Result<()>;

    fn build_quads(&self) -> Result<ArrayRef>;

    /// These fields must already be valid root-level fields of length 1.
    ///
    /// Examples:
    /// - `storage_layout`: ConstantArray length 1
    /// - `zone_maps`: ListArray length 1
    /// - `file_metadata`: ListArray length 1
    fn build_extra_root_fields(&self) -> Result<Vec<(Arc<str>, ArrayRef)>> {
        Ok(Vec::new())
    }
}

pub struct IndexBuilder;

impl IndexBuilder {
    pub async fn build<Dict, Strategy>(
        mut quad_stream: impl Stream<Item = Result<Quad>> + Unpin + Send,
        mut strategy: Strategy,
    ) -> Result<ArrayRef>
    where
        Dict: RdfDictionary,
        Strategy: LayoutStrategy<Dict>,
    {
        let mut dictionary = Dict::new();

        while let Some(result) = quad_stream.next().await {
            let quad = result?;
            strategy.ingest(&quad, &mut dictionary)?;
        }

        strategy.finalize(&mut dictionary)?;

        let quads = strategy.build_quads()?;
        let quads_list = indexes::wrap_array_in_list(quads)?;

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
        field_arrays.push(quads_list);

        for (name, arr) in strategy.build_extra_root_fields()? {
            field_names.push(name);
            field_arrays.push(arr);
        }

        let root = StructArray::try_new(field_names.into(), field_arrays, 1, Validity::NonNullable)
            .map_err(VortexRdfError::Vortex)?
            .into_array();

        Ok(root)
    }
}

/// Unified VortexRdfStore that works with any RdfDictionary implementation
pub struct VortexRdfStore<Dict: RdfDictionary> {
    pub dictionary: Dict,
    pub quads: ArrayRef,
}

pub struct PlainLayout {
    s_ids: Vec<u32>,
    p_ids: Vec<u32>,
    o_ids: Vec<u32>,
    g_ids: Vec<u32>,
}

impl PlainLayout {
    pub fn new() -> Self {
        Self {
            s_ids: Vec::new(),
            p_ids: Vec::new(),
            o_ids: Vec::new(),
            g_ids: Vec::new(),
        }
    }
}

impl<Dict: RdfDictionary> LayoutStrategy<Dict> for PlainLayout {
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
        let quads = StructArray::from_fields(&[
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

        Ok(quads)
    }
}

impl<Dict: RdfDictionary> VortexRdfStore<Dict> {
    /// Create a new store from a Vortex array
    pub fn new(vortex_array: ArrayRef) -> Result<Self> {
        // First field is store_type
        // Dictionary fields are flattened in the middle
        // Last field is quads
        let vortex_struct = vortex_array.to_struct();

        // We pass the whole struct to Dict::from_vortex_array.
        // Implementations must assume the struct contains their fields mingled with others
        // and know the fixed position of the fields.
        let dictionary = Dict::from_vortex_array(&vortex_array)?;

        // Quads field
        let quads = utils::extract_vortex_struct_field(&vortex_struct, "quads")?;

        Ok(Self { dictionary, quads })
    }

    /// Create an empty store
    pub fn empty() -> Self {
        let dictionary = Dict::new();
        let quads = StructArray::from_fields(&[
            (
                "s",
                PrimitiveArray::from_iter(Vec::<u32>::new()).into_array(),
            ),
            (
                "p",
                PrimitiveArray::from_iter(Vec::<u32>::new()).into_array(),
            ),
            (
                "o",
                PrimitiveArray::from_iter(Vec::<u32>::new()).into_array(),
            ),
            (
                "g",
                PrimitiveArray::from_iter(Vec::<u32>::new()).into_array(),
            ),
        ])
        .unwrap()
        .into_array();

        Self { dictionary, quads }
    }

    /// Load from file (requires file-io feature)
    #[cfg(feature = "file-io")]
    pub async fn from_file<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let vortex_array = de::load_vortex_file_ref(path.as_ref().to_path_buf()).await?;
        Self::new(vortex_array)
    }

    /// Load from bytes
    pub async fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let cursor = std::io::Cursor::new(bytes);
        let vortex_array = de::array_from_reader(cursor)?;
        Self::new(vortex_array)
    }

    /// Build a Vortex index from a stream of quads
    pub async fn build_vortex_index(
        quad_stream: impl Stream<Item = Result<Quad>> + Unpin + Send + 'static,
    ) -> Result<ArrayRef> {
        IndexBuilder::build::<Dict, _>(quad_stream, PlainLayout::new()).await
    }

    /// Get the quads array
    pub fn get_quads_array(&self) -> Result<ArrayRef> {
        Ok(self.quads.clone())
    }

    /// Get the number of quads in the store
    pub fn size(&self) -> usize {
        self.quads.len()
    }

    /// Internal helper to find row indices matching a pattern
    fn find_mask(
        &self,
        subject: Option<&Subject>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Option<ArrayRef>> {
        let quads_struct = self.quads.to_struct();
        let fields = quads_struct.fields();

        let mut mask: Option<ArrayRef> = None;

        let mut combine_mask = |new_mask: ArrayRef| -> Result<()> {
            if let Some(m) = mask.take() {
                mask = Some(and(&m, &new_mask).map_err(VortexRdfError::Vortex)?);
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

        for (term_opt, col_idx, label) in patterns {
            if let Some(term_str) = term_opt {
                let start = Instant::now();
                if let Some(id) = self.dictionary.get_id(&term_str) {
                    let col = fields.get(col_idx).unwrap();
                    let scalar = Scalar::from(id)
                        .cast(col.dtype())
                        .map_err(VortexRdfError::Vortex)?;

                    let column_mask = self.compare_with_pruning(col, &scalar)?;
                    log::debug!(
                        "[VortexRdfStore::find_mask] {} comparison took {:?}",
                        label,
                        start.elapsed()
                    );
                    combine_mask(column_mask)?;
                } else {
                    // ID not in dictionary means no possible match
                    return Ok(Some(
                        ConstantArray::new(false, self.quads.len()).into_array(),
                    ));
                }
            }
        }

        Ok(mask)
    }

    fn compare_with_pruning(&self, col: &ArrayRef, scalar: &Scalar) -> Result<ArrayRef> {
        compare(
            col,
            &ConstantArray::new(scalar.clone(), col.len()).into_array(),
            Operator::Eq,
        )
        .map_err(VortexRdfError::Vortex)
    }

    fn with_quads(&self, quads: ArrayRef) -> Result<Self> {
        Ok(Self {
            dictionary: self.dictionary.clone(),
            quads,
        })
    }

    /// Match a quad pattern
    pub async fn match_pattern(
        &self,
        subject: Option<&Subject>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Self> {
        let start = Instant::now();
        let mask = self.find_mask(subject, predicate, object, graph)?;

        if let Some(m) = mask {
            let mask_start = Instant::now();
            let quads_array_ref = self.get_quads_array()?;
            let canonical: Canonical = (&*m).to_canonical();
            let canonical_mask = match canonical {
                Canonical::Bool(b) => Mask::from(b.bit_buffer().clone()),
                _ => {
                    return Err(VortexRdfError::Deserialization(
                        "Mask must be boolean".to_string(),
                    ));
                }
            };
            log::debug!(
                "[VortexRdfStore::match_pattern] Mask creation took {:?}",
                mask_start.elapsed()
            );
            let filter_start = Instant::now();
            let filtered_quads =
                filter(&quads_array_ref, &canonical_mask).map_err(VortexRdfError::Vortex)?;
            log::debug!(
                "[VortexRdfStore::match_pattern] Filtering compute operation took {:?}",
                filter_start.elapsed()
            );
            let _self = self.with_quads(filtered_quads);
            log::debug!(
                "[VortexRdfStore::match_pattern] Pattern matching took overall {:?}",
                start.elapsed()
            );
            _self
        } else {
            Ok(Self {
                dictionary: self.dictionary.clone(),
                quads: self.quads.clone(),
            })
        }
    }

    /// Add a quad to the store
    pub async fn add_quad(&self, quad: Quad) -> Result<Self> {
        let mut new_dict = self.dictionary.clone();

        let s_id = new_dict.get_or_insert(&quad.subject.to_string());
        let p_id = new_dict.get_or_insert(&quad.predicate.to_string());
        let o_id = new_dict.get_or_insert(&quad.object.to_string());
        let g_id = new_dict.get_or_insert(&quad.graph_name.to_string());

        // Encode quad IDs into a StructArray
        let s_arr = PrimitiveArray::from_iter(vec![s_id]).into_array();
        let p_arr = PrimitiveArray::from_iter(vec![p_id]).into_array();
        let o_arr = PrimitiveArray::from_iter(vec![o_id]).into_array();
        let g_arr = PrimitiveArray::from_iter(vec![g_id]).into_array();

        let new_row =
            StructArray::from_fields(&[("s", s_arr), ("p", p_arr), ("o", o_arr), ("g", g_arr)])
                .map_err(VortexRdfError::Vortex)?
                .into_array();
        let old_quads = self.get_quads_array()?;

        // Use vortex ChunkedArray for efficient zero-copy concatenation
        let combined_quads = ChunkedArray::try_new(
            vec![old_quads, new_row],
            self.get_quads_array()?.dtype().clone(),
        )
        .map_err(VortexRdfError::Vortex)?
        .into_array();

        Ok(Self {
            dictionary: new_dict,
            quads: combined_quads,
        })
    }

    /// Delete a quad from the store
    pub async fn delete_quad(&self, quad: &Quad) -> Result<Self> {
        let mask = self.find_mask(
            Some(&quad.subject),
            Some(&quad.predicate),
            Some(&quad.object),
            Some(&quad.graph_name),
        )?;

        if let Some(m) = mask {
            // We want rows where mask is FALSE
            let inverse_mask_array = compare(
                &m,
                &IntoArray::into_array(ConstantArray::new(Scalar::from(true), m.len())),
                Operator::NotEq,
            )
            .map_err(VortexRdfError::Vortex)?;

            let canonical = inverse_mask_array.to_canonical();
            let canonical_mask = match canonical {
                Canonical::Bool(b) => Mask::from(b.bit_buffer().clone()),
                _ => {
                    return Err(VortexRdfError::Deserialization(
                        "Inverse mask must be boolean".to_string(),
                    ));
                }
            };

            let quads_array_ref = self.get_quads_array()?;
            let filtered_quads =
                filter(&quads_array_ref, &canonical_mask).map_err(VortexRdfError::Vortex)?;
            self.with_quads(filtered_quads)
        } else {
            Ok(Self {
                dictionary: self.dictionary.clone(),
                quads: self.quads.clone(),
            })
        }
    }

    /// Get all quads as a stream
    pub fn quads(&self) -> Result<Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + '_>> {
        let quads_start = Instant::now();
        let quads_struct = self.quads.to_struct();
        let fields = quads_struct.fields();

        let s_ids = fields
            .get(0)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing S IDs".to_string()))?
            .clone()
            .to_primitive();
        let p_ids = fields
            .get(1)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing P IDs".to_string()))?
            .clone()
            .to_primitive();
        let o_ids = fields
            .get(2)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing O IDs".to_string()))?
            .clone()
            .to_primitive();
        let g_ids = fields
            .get(3)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing G IDs".to_string()))?
            .clone()
            .to_primitive();
        log::debug!(
            "[VortexRdfStore::quads] Quads struct extraction took {:?}",
            quads_start.elapsed()
        );

        let len = s_ids.len();

        let iter = (0..len).map(move |i| {
            let s_id = s_ids.as_slice::<u32>()[i];
            let p_id = p_ids.as_slice::<u32>()[i];
            let o_id = o_ids.as_slice::<u32>()[i];
            let g_id = g_ids.as_slice::<u32>()[i];

            let s_term = self.dictionary.get_term(s_id).ok_or_else(|| {
                VortexRdfError::Deserialization(format!("S ID {} not in dictionary", s_id))
            })?;
            let p_term = self.dictionary.get_term(p_id).ok_or_else(|| {
                VortexRdfError::Deserialization(format!("P ID {} not in dictionary", p_id))
            })?;
            let o_term = self.dictionary.get_term(o_id).ok_or_else(|| {
                VortexRdfError::Deserialization(format!("O ID {} not in dictionary", o_id))
            })?;
            let g_name = self.dictionary.get_graph_name(g_id).ok_or_else(|| {
                VortexRdfError::Deserialization(format!("G ID {} not in dictionary", g_id))
            })?;

            let subject = match s_term {
                Term::NamedNode(n) => Subject::NamedNode(n),
                Term::BlankNode(b) => Subject::BlankNode(b),
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
}

// Implement QuadStore trait for VortexRdfStore
impl<D: RdfDictionary> crate::store::QuadStore for VortexRdfStore<D> {
    fn quads(&self) -> Result<Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + '_>> {
        self.quads()
    }
}
