pub mod dictionary;

use crate::error::{Result, VortexRdfError};
use crate::io::{de, ser};
use dictionary::Dictionary;
use oxrdf::{GraphName, NamedNode, Quad, Subject, Term};
use vortex::compute::{and, compare, filter, Operator};
use vortex_array::arrays::{ChunkedArray, ConstantArray, ListArray, PrimitiveArray, StructArray};
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, Canonical, IntoArray, ToCanonical};
use vortex_mask::Mask;
use std::time::Instant;
use vortex::buffer::Buffer;
use crate::utils;

use futures::Stream;

pub struct VortexRdfStore {
    pub vortex_array: ArrayRef,
    pub dictionary: Dictionary,
}

impl VortexRdfStore {
    pub fn new(vortex_array: ArrayRef) -> Result<Self> {
        let dictionary = Dictionary::from_vortex_array(&vortex_array)?;
        Ok(Self {
            vortex_array,
            dictionary,
        })
    }

    pub async fn empty() -> Result<Self> {
        let array = ser::encode_quads(futures::stream::empty()).await?;
        Self::new(array)
    }

    pub async fn from_file<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        // VortexFile::open handles opening the file from path
        let vortex_array = de::read_array_from_vortex(path.as_ref().to_path_buf()).await?;
        Self::new(vortex_array)
    }

    pub async fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let buffer = Buffer::from(bytes.to_vec());
        let vortex_array = de::read_array_from_vortex(buffer).await?;
        Self::new(vortex_array)
    }

    pub fn get_quads_array(&self) -> Result<ArrayRef> {
        let quads_array = utils::quads_array_from_vortex_array(self.vortex_array.clone())?;
        Ok(quads_array)
    }

    pub fn size(&self) -> usize {
        self.get_quads_array().map(|a| a.len()).unwrap_or(0)
    }

    pub fn quads(&self) -> Result<impl Stream<Item = Result<Quad>>> {
        de::decode_quads_stream(self.vortex_array.clone())
    }

    /// Internal helper to find row indices matching a pattern
    fn find_mask(
        &self,
        subject: Option<&Subject>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Option<ArrayRef>> {
        let quads_array_ref = self.get_quads_array()?;
        let quads_struct = quads_array_ref.to_struct();
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

        if let Some(s) = subject {
            let start_subject = Instant::now();
            let id = self.dictionary.get_id(&s.to_string());
            if let Some(sid) = id {
                let col = fields.get(0).unwrap();
                let scalar = vortex_scalar::Scalar::from(sid)
                    .cast(col.dtype())
                    .map_err(VortexRdfError::Vortex)?;
                let col_mask = compare(
                    col,
                    &ConstantArray::new(scalar, col.len()).into_array(),
                    Operator::Eq,
                )
                .map_err(VortexRdfError::Vortex)?;
                log::debug!("[VortexStore::find_mask] Subject comparison took {:?}", start_subject.elapsed());
                combine_mask(col_mask)?;
            } else {
                // If term not in dict, mask is all false
                let col = fields.get(0).unwrap();
                return Ok(Some(
                    ConstantArray::new(false, col.len()).into_array(),
                ));
            }
        }

        if let Some(p) = predicate {
            let start_predicate = Instant::now();
            let id = self.dictionary.get_id(&p.to_string());
            if let Some(pid) = id {
                let col = fields.get(1).unwrap();
                let scalar = vortex_scalar::Scalar::from(pid)
                    .cast(col.dtype())
                    .map_err(VortexRdfError::Vortex)?;
                let col_mask = compare(
                    col,
                    &ConstantArray::new(scalar, col.len()).into_array(),
                    Operator::Eq,
                )
                .map_err(VortexRdfError::Vortex)?;
                log::debug!("[VortexStore::find_mask] Predicate comparison took {:?}", start_predicate.elapsed());
                combine_mask(col_mask)?;
            } else {
                let col = fields.get(1).unwrap();
                return Ok(Some(
                    ConstantArray::new(false, col.len()).into_array(),
                ));
            }
        }

        if let Some(o) = object {
            let start_object = Instant::now();
            let id = self.dictionary.get_id(&o.to_string());
            if let Some(oid) = id {
                let col = fields.get(2).unwrap();
                let scalar = vortex_scalar::Scalar::from(oid)
                    .cast(col.dtype())
                    .map_err(VortexRdfError::Vortex)?;
                let col_mask = compare(
                    col,
                    &ConstantArray::new(scalar, col.len()).into_array(),
                    Operator::Eq,
                )
                .map_err(VortexRdfError::Vortex)?;
                log::debug!("[VortexStore::find_mask] Object comparison took {:?}", start_object.elapsed());
                combine_mask(col_mask)?;
            } else {
                let col = fields.get(2).unwrap();
                return Ok(Some(
                    ConstantArray::new(false, col.len()).into_array(),
                ));
            }
        }

        if let Some(g) = graph {
            let start_graph = Instant::now();
            let id = self.dictionary.get_id(&g.to_string());
            if let Some(gid) = id {
                let col = fields.get(3).unwrap();
                let scalar = vortex_scalar::Scalar::from(gid)
                    .cast(col.dtype())
                    .map_err(VortexRdfError::Vortex)?;
                let col_mask = compare(
                    col,
                    &ConstantArray::new(scalar, col.len()).into_array(),
                    Operator::Eq,
                )
                .map_err(VortexRdfError::Vortex)?;
                log::debug!("[VortexStore::find_mask] Graph comparison took {:?}", start_graph.elapsed());
                combine_mask(col_mask)?;
            } else {
                let col = fields.get(3).unwrap();
                return Ok(Some(
                    ConstantArray::new(false, col.len()).into_array(),
                ));
            }
        }

        Ok(mask)
    }

    pub async fn match_pattern(
        &self,
        subject: Option<&Subject>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Self> {
        let start = Instant::now();
        // TODO: check if can use vortex-scan for better performance on large datasets.
        let mask = self.find_mask(subject, predicate, object, graph)?;

        if let Some(m) = mask {
            let mask_start = Instant::now();
            let quads_array_ref = self.get_quads_array()?;
            let canonical = m.to_canonical();
            let canonical_mask = match canonical {
                Canonical::Bool(b) => Mask::from(b.bit_buffer().clone()),
                _ => {
                    return Err(VortexRdfError::Deserialization(
                        "Mask must be boolean".to_string(),
                    ))
                }
            };
            log::debug!("[VortexStore::match_pattern] Mask creation took {:?}", mask_start.elapsed());
            let filter_start = Instant::now();
            let filtered_quads = filter(&quads_array_ref, &canonical_mask)
                .map_err(VortexRdfError::Vortex)?;
            log::debug!("[VortexStore::match_pattern] Filtering compute operation took {:?}", filter_start.elapsed());
            let _self = self.with_quads(filtered_quads);
            log::debug!("[VortexStore::match_pattern] Pattern matching took overall {:?}", start.elapsed());
            _self
        } else {
            Ok(Self {
                vortex_array: self.vortex_array.clone(),
                dictionary: self.dictionary.clone(),
            })
        }
    }

    pub async fn add_quad(&self, quad: Quad) -> Result<Self> {
        let mut new_dict = self.dictionary.clone();
        let s_id = new_dict.get_or_insert(&quad.subject.to_string());
        let p_id = new_dict.get_or_insert(&quad.predicate.to_string());
        let o_id = new_dict.get_or_insert(&quad.object.to_string());
        let g_id = new_dict.get_or_insert(&quad.graph_name.to_string());

        let new_row = ser::encode_quad_ids(vec![s_id], vec![p_id], vec![o_id], vec![g_id])?;
        let old_quads = self.get_quads_array()?;

        // Use vortex ChunkedArray for efficient zero-copy concatenation
        let combined_quads = ChunkedArray::try_new(
            vec![old_quads, new_row],
            self.get_quads_array()?.dtype().clone(),
        )
        .map_err(VortexRdfError::Vortex)?
        .into_array();

        // Re-encode dictionary
        let dict_arr = ser::encode_dictionary(&new_dict)?;

        Ok(Self {
            vortex_array: self.with_dict_and_quads(dict_arr, combined_quads)?,
            dictionary: new_dict,
        })
    }

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
                &ConstantArray::new(true, m.len()).into_array(),
                Operator::NotEq,
            )
            .map_err(VortexRdfError::Vortex)?;

            let canonical = inverse_mask_array.to_canonical();
            let canonical_mask = match canonical {
                Canonical::Bool(b) => Mask::from(b.bit_buffer().clone()),
                _ => {
                    return Err(VortexRdfError::Deserialization(
                        "Inverse mask must be boolean".to_string(),
                    ))
                }
            };

            let quads_array_ref = self.get_quads_array()?;
            let filtered_quads = filter(&quads_array_ref, &canonical_mask)
            .map_err(VortexRdfError::Vortex)?;
            self.with_quads(filtered_quads)
        } else {
            Ok(Self {
                vortex_array: self.vortex_array.clone(),
                dictionary: self.dictionary.clone(),
            })
        }
    }

    fn with_quads(&self, quads: ArrayRef) -> Result<Self> {
        Ok(Self {
            vortex_array: self.with_quads_array(quads)?,
            dictionary: self.dictionary.clone(),
        })
    }

    fn with_quads_array(&self, quads: ArrayRef) -> Result<ArrayRef> {
        let vortex_struct = self.vortex_array.to_struct();
        let dict_list = vortex_struct.fields().get(0).unwrap().clone();
        self.with_dict_and_quads_array(dict_list, quads)
    }

    fn with_dict_and_quads(&self, dict: ArrayRef, quads: ArrayRef) -> Result<ArrayRef> {
        let dict_offsets =
            PrimitiveArray::from_iter(vec![0i32, dict.len() as i32])
                .into_array();
        let dict_list = ListArray::try_new(
            dict,
            dict_offsets,
            Validity::NonNullable,
        )
        .map_err(VortexRdfError::Vortex)?
        .into_array();

        self.with_dict_and_quads_array(dict_list, quads)
    }

    fn with_dict_and_quads_array(&self, dict_list: ArrayRef, quads: ArrayRef) -> Result<ArrayRef> {
        let quads_offsets = PrimitiveArray::from_iter(vec![0i32, quads.len() as i32])
            .into_array();
        let quads_list = ListArray::try_new(
                quads,
                quads_offsets,
                Validity::NonNullable,
            )
            .map_err(VortexRdfError::Vortex)?
            .into_array();

        let new_dict_struct = StructArray::from_fields(&[
            ("dictionary", dict_list),
            ("quads", quads_list),
        ])
        .map_err(VortexRdfError::Vortex)?
        .into_array();
        Ok(new_dict_struct)
    }
}
