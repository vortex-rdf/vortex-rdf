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
use vortex::buffer::Buffer;
use vortex_dtype::{DType, Nullability, PType};

use futures::Stream;

#[allow(dead_code)]
pub struct VortexRdfStore {
    pub root: ArrayRef,
    pub dictionary: Dictionary,
    #[allow(dead_code)]
    mmap: Option<memmap2::Mmap>,
}

impl VortexRdfStore {
    pub fn new(root: ArrayRef) -> Result<Self> {
        let dictionary = Dictionary::from_root(&root)?;
        Ok(Self {
            root,
            dictionary,
            mmap: None,
        })
    }

    pub fn empty() -> Result<Self> {
        let array = ser::encode_quads(std::iter::empty())?;
        Self::new(array)
    }

    pub async fn from_file<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        // VortexFile::open handles opening the file from path
        let array = de::read_array_from_vortex(path.as_ref().to_path_buf()).await?;
        Self::new(array)
    }

    pub async fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let buffer = Buffer::from(bytes.to_vec());
        let array = de::read_array_from_vortex(buffer).await?;
        Self::new(array)
    }

    pub fn get_quads_array(&self) -> Result<ArrayRef> {
        let root_struct = self.root.to_struct();
        let quads_list_ref = root_struct
            .fields()
            .get(1)
            .ok_or_else(|| VortexRdfError::Deserialization("Missing quads field".to_string()))?
            .clone();

        let quads_list = quads_list_ref.to_listview();
        let quads_offset = quads_list.offsets().scalar_at(0).cast(&DType::Primitive(PType::I32, Nullability::NonNullable)).map_err(VortexRdfError::Vortex)?.as_primitive().typed_value::<i32>().ok_or_else(|| VortexRdfError::Deserialization("Missing quads offset".to_string()))? as usize;
        let quads_size = quads_list.sizes().scalar_at(0).cast(&DType::Primitive(PType::I32, Nullability::NonNullable)).map_err(VortexRdfError::Vortex)?.as_primitive().typed_value::<i32>().ok_or_else(|| VortexRdfError::Deserialization("Missing quads size".to_string()))? as usize;
        Ok(quads_list
            .elements()
            .slice(quads_offset..quads_offset + quads_size))
    }

    pub fn size(&self) -> usize {
        self.get_quads_array().map(|a| a.len()).unwrap_or(0)
    }

    pub fn quads(&self) -> Result<impl Stream<Item = Result<Quad>>> {
        de::decode_quads_stream(self.root.clone())
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
                combine_mask(col_mask)?;
            } else {
                let col = fields.get(1).unwrap();
                return Ok(Some(
                    ConstantArray::new(false, col.len()).into_array(),
                ));
            }
        }

        if let Some(o) = object {
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
                combine_mask(col_mask)?;
            } else {
                let col = fields.get(2).unwrap();
                return Ok(Some(
                    ConstantArray::new(false, col.len()).into_array(),
                ));
            }
        }

        if let Some(g) = graph {
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
        // For now, we still use the mask-based approach but we can eventually refactor
        // to use vortex-scan and vortex-expr for better performance on large datasets.
        let mask = self.find_mask(subject, predicate, object, graph)?;

        if let Some(m) = mask {
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
            let filtered_quads =
                filter(&quads_array_ref, &canonical_mask).map_err(VortexRdfError::Vortex)?;
            self.with_quads(filtered_quads)
        } else {
            Ok(Self {
                root: self.root.clone(),
                dictionary: self.dictionary.clone(),
                mmap: None,
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
            root: self.with_dict_and_quads(dict_arr, combined_quads)?,
            dictionary: new_dict,
            mmap: None,
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
            let filtered_quads =
                filter(&quads_array_ref, &canonical_mask).map_err(VortexRdfError::Vortex)?;
            self.with_quads(filtered_quads)
        } else {
            Ok(Self {
                root: self.root.clone(),
                dictionary: self.dictionary.clone(),
                mmap: None,
            })
        }
    }

    fn with_quads(&self, quads: ArrayRef) -> Result<Self> {
        Ok(Self {
            root: self.with_quads_root(quads)?,
            dictionary: self.dictionary.clone(),
            mmap: None,
        })
    }

    fn with_quads_root(&self, quads: ArrayRef) -> Result<ArrayRef> {
        let root_struct = self.root.to_struct();
        let dict_list = root_struct.fields().get(0).unwrap().clone();
        self.with_dict_and_quads_root(dict_list, quads)
    }

    fn with_dict_and_quads(&self, dict: ArrayRef, quads: ArrayRef) -> Result<ArrayRef> {
        let dict_offsets =
            PrimitiveArray::from_iter(vec![0i32, dict.len() as i32])
                .into_array();
        let dict_list = ListArray::try_new(dict, dict_offsets, Validity::NonNullable)?.into_array();

        self.with_dict_and_quads_root(dict_list, quads)
    }

    fn with_dict_and_quads_root(&self, dict_list: ArrayRef, quads: ArrayRef) -> Result<ArrayRef> {
        let quads_offsets =
            PrimitiveArray::from_iter(vec![0i32, quads.len() as i32])
                .into_array();
        let quads_list =
            ListArray::try_new(quads, quads_offsets, Validity::NonNullable)?.into_array();

        let new_root = StructArray::from_fields(&[
            ("dictionary", dict_list),
            ("quads", quads_list),
        ])?
        .into_array();
        Ok(new_root)
    }
}
